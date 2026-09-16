use super::*;
use crate::polymarket_market_metadata_v6_layout::PolymarketMarketMetadataV6Row;

pub(super) const MARKET_WS_ENDPOINT: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
pub(super) const DEFAULT_DISCOVERY_WAIT_SECS: u64 = 90;
pub(super) const MARKET_NEXT_DISCOVERY_INTERVAL_SECS: u64 = 10;
pub(super) const GAMMA_REQUEST_TIMEOUT_SECS: u64 = 20;
pub(super) const POLYMARKET_RESOLUTION_FIRST_SLOT_DELAY_MS: i64 = 3 * 60_000;
pub(super) const POLYMARKET_RESOLUTION_SLOT_INTERVAL_MS: i64 = 60_000;
pub(super) const POLYMARKET_RESOLUTION_ATTEMPT_COUNT: u8 = 6;
pub(super) const POLYMARKET_PENDING_MAX_MARKETS: usize = 2;
pub(super) const POLYMARKET_PENDING_STATE_MAX_BYTES: usize = 1024 * 1024;
pub(super) const POLYMARKET_DECODED_QUEUE_CAPACITY: usize = 25_000;
pub(super) const POLYMARKET_DECODED_MAX_BYTES: usize = 67_108_864;
pub(super) const POLYMARKET_DRAIN_CONTROL_SLOTS: usize = 2;
pub(super) const POLYMARKET_CONTROL_CHANNEL_CAPACITY: usize = 4;

pub(super) enum PolymarketSourceControl {
    Fence {
        stop_after_fence: bool,
        day_boundary_utc_ms: Option<i64>,
        resume_after_barrier: Option<oneshot::Receiver<()>>,
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
}

pub(super) fn polymarket_source_control_is_ordinary(control: &PolymarketSourceControl) -> bool {
    matches!(
        control,
        PolymarketSourceControl::Fence {
            stop_after_fence: false,
            day_boundary_utc_ms: None,
            resume_after_barrier: None,
            ..
        }
    )
}

pub(super) struct BudgetedPolymarketMessage {
    pub(super) message: WsMessage,
    pub(super) recv_time_utc_ms: i64,
    pub(super) raw_day_ticket: RawDayAcceptedTicket,
    pub(super) _global_reservation: InflightReservation,
    pub(super) _decoded_reservation: InflightReservation,
}

#[allow(clippy::large_enum_variant)]
pub(super) enum PolymarketDrainEvent {
    Message(BudgetedPolymarketMessage),
    Fence { id: u64, stop_after_fence: bool },
}

#[derive(Debug)]
pub(super) enum PolymarketDrainTerminal {
    Sdk(SdkStreamError),
    Capacity {
        reason: String,
        recv_time_utc_ms: i64,
        error_kind: io::ErrorKind,
    },
    Admission {
        reason: String,
        recv_time_utc_ms: i64,
    },
    QueueClosed,
}

pub(super) enum PolymarketPumpControl {
    Fence {
        id: u64,
        stop_after_fence: bool,
        resume_after_fence: Option<oneshot::Receiver<()>>,
    },
}

#[allow(clippy::large_enum_variant)]
pub(super) enum PolymarketPumpItem {
    Message(BudgetedPolymarketMessage),
    Fence { id: u64, stop_after_fence: bool },
    Terminal(PolymarketDrainTerminal),
}

pub(super) enum PolymarketFenceRequest {
    Queued(u64),
    Paused {
        id: u64,
        resume: oneshot::Sender<()>,
    },
    PumpClosed,
}

pub(super) struct PolymarketDrainRuntime {
    pub(super) sdk: MarketSdkRuntime,
    pub(super) events: mpsc::Receiver<PolymarketPumpItem>,
    pub(super) controls: mpsc::Sender<PolymarketPumpControl>,
    pub(super) pump: Option<tokio::task::JoinHandle<()>>,
    pub(super) next_fence_id: u64,
    #[cfg(test)]
    #[allow(dead_code)]
    pub(super) decoded_budget: InflightBudget,
}

impl PolymarketDrainRuntime {
    pub(super) fn new(
        endpoint: String,
        config: WsConfig,
        global_budget: InflightBudget,
        raw_day_gate: impl Into<SourceAdmissionGate>,
    ) -> Result<Self> {
        Self::new_with_recv_clock(
            endpoint,
            config,
            global_budget,
            raw_day_gate.into(),
            Arc::new(now_ms),
        )
    }

    pub(super) fn new_with_recv_clock(
        endpoint: String,
        config: WsConfig,
        global_budget: InflightBudget,
        raw_day_gate: impl Into<SourceAdmissionGate>,
        recv_utc_now: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<Self> {
        Self::from_market_sdk(
            MarketSdkRuntime::new(endpoint, config)?,
            global_budget,
            raw_day_gate.into(),
            recv_utc_now,
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_with_before_ack_pause(
        endpoint: String,
        config: WsConfig,
        global_budget: InflightBudget,
        raw_day_gate: impl Into<SourceAdmissionGate>,
        generation: u64,
        revision: u64,
        reached: oneshot::Sender<()>,
        resume: oneshot::Receiver<()>,
    ) -> Result<Self> {
        Self::from_market_sdk(
            MarketSdkRuntime::new_with_before_ack_pause(
                endpoint, config, generation, revision, reached, resume,
            )?,
            global_budget,
            raw_day_gate.into(),
            Arc::new(now_ms),
        )
    }

    fn from_market_sdk(
        mut sdk: MarketSdkRuntime,
        global_budget: InflightBudget,
        raw_day_gate: SourceAdmissionGate,
        recv_utc_now: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<Self> {
        let receiver = sdk.take_message_receiver()?;
        let decoded_budget = InflightBudget::with_limits(
            POLYMARKET_DECODED_QUEUE_CAPACITY,
            POLYMARKET_DECODED_MAX_BYTES,
        );
        let (event_tx, event_rx) = mpsc::channel::<PolymarketPumpItem>(
            POLYMARKET_DECODED_QUEUE_CAPACITY + POLYMARKET_DRAIN_CONTROL_SLOTS,
        );
        let (control_tx, control_rx) =
            mpsc::channel::<PolymarketPumpControl>(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
        let pump_decoded_budget = decoded_budget.clone();
        let pump = tokio::spawn(run_polymarket_fast_drain(
            receiver,
            event_tx,
            control_rx,
            global_budget,
            pump_decoded_budget,
            raw_day_gate,
            recv_utc_now,
        ));
        Ok(Self {
            sdk,
            events: event_rx,
            controls: control_tx,
            pump: Some(pump),
            next_fence_id: 0,
            #[cfg(test)]
            decoded_budget,
        })
    }

    pub(super) async fn request_fence(
        &mut self,
        stop_after_fence: bool,
    ) -> Result<PolymarketFenceRequest> {
        self.request_fence_inner(stop_after_fence, None).await
    }

    pub(super) async fn request_paused_fence(&mut self) -> Result<PolymarketFenceRequest> {
        let (resume, resume_after_fence) = oneshot::channel();
        match self
            .request_fence_inner(false, Some(resume_after_fence))
            .await?
        {
            PolymarketFenceRequest::Queued(id) => Ok(PolymarketFenceRequest::Paused { id, resume }),
            PolymarketFenceRequest::PumpClosed => Ok(PolymarketFenceRequest::PumpClosed),
            PolymarketFenceRequest::Paused { .. } => {
                Err(invalid_data("nested Polymarket paused fence request").into())
            }
        }
    }

    async fn request_fence_inner(
        &mut self,
        stop_after_fence: bool,
        resume_after_fence: Option<oneshot::Receiver<()>>,
    ) -> Result<PolymarketFenceRequest> {
        self.next_fence_id = self
            .next_fence_id
            .checked_add(1)
            .ok_or_else(|| invalid_data("Polymarket source fence revision overflow"))?;
        let id = self.next_fence_id;
        match timeout(
            BUFFER_ACQUIRE_TIMEOUT,
            self.controls.send(PolymarketPumpControl::Fence {
                id,
                stop_after_fence,
                resume_after_fence,
            }),
        )
        .await
        {
            Err(_) => Err(invalid_data("Polymarket fast-drain control queue timeout").into()),
            Ok(Ok(())) => Ok(PolymarketFenceRequest::Queued(id)),
            Ok(Err(_)) => Ok(PolymarketFenceRequest::PumpClosed),
        }
    }

    pub(super) async fn recv(
        &mut self,
        timeout_duration: Duration,
    ) -> std::result::Result<Option<PolymarketDrainEvent>, PolymarketDrainTerminal> {
        match timeout(timeout_duration, self.events.recv()).await {
            Err(_) => Ok(None),
            Ok(Some(PolymarketPumpItem::Message(message))) => {
                Ok(Some(PolymarketDrainEvent::Message(message)))
            }
            Ok(Some(PolymarketPumpItem::Fence {
                id,
                stop_after_fence,
            })) => Ok(Some(PolymarketDrainEvent::Fence {
                id,
                stop_after_fence,
            })),
            Ok(Some(PolymarketPumpItem::Terminal(error))) => Err(error),
            Ok(None) => Err(PolymarketDrainTerminal::QueueClosed),
        }
    }

    pub(super) async fn wait_pump_stopped(&mut self) -> Result<()> {
        let Some(mut pump) = self.pump.take() else {
            return Ok(());
        };
        match timeout(BUFFER_ACQUIRE_TIMEOUT, &mut pump).await {
            Ok(result) => {
                result.map_err(|error| {
                    invalid_data(format!("Polymarket fast-drain task failed:{error}"))
                })?;
                Ok(())
            }
            Err(_) => {
                pump.abort();
                let _ = pump.await;
                Err(invalid_data("Polymarket fast-drain task stop timeout").into())
            }
        }
    }

    pub(super) async fn abort_pump_and_wait(&mut self) {
        if let Some(pump) = self.pump.take() {
            pump.abort();
            let _ = pump.await;
        }
    }
}

impl Drop for PolymarketDrainRuntime {
    fn drop(&mut self) {
        if let Some(pump) = &self.pump {
            pump.abort();
        }
    }
}

pub(super) async fn run_polymarket_fast_drain(
    mut receiver: MarketSdkMessageReceiver,
    events: mpsc::Sender<PolymarketPumpItem>,
    mut controls: mpsc::Receiver<PolymarketPumpControl>,
    global_budget: InflightBudget,
    decoded_budget: InflightBudget,
    raw_day_gate: SourceAdmissionGate,
    recv_utc_now: Arc<dyn Fn() -> i64 + Send + Sync>,
) {
    loop {
        tokio::select! {
            biased;
            control = controls.recv() => {
                let Some(PolymarketPumpControl::Fence {
                    id,
                    stop_after_fence,
                    resume_after_fence,
                }) = control else {
                    break;
                };
                let terminal_before_snapshot = receiver.terminal_snapshot();
                let pending_at_fence = receiver.pending_len();
                let terminal_at_fence = terminal_before_snapshot.or_else(|| receiver.terminal_snapshot());
                for _ in 0..pending_at_fence {
                    match receiver.try_recv() {
                        Ok(Some(message)) => {
                            if !enqueue_polymarket_decoded_message(
                                message,
                                &events,
                                &global_budget,
                                &decoded_budget,
                                &raw_day_gate,
                                recv_utc_now(),
                            )
                            .await
                            {
                                return;
                            }
                        }
                        Ok(None) => break,
                        Err(error) => {
                            send_polymarket_terminal(
                                &events,
                                &raw_day_gate,
                                PolymarketDrainTerminal::Sdk(error),
                            )
                            .await;
                            return;
                        }
                    }
                }
                if let Some(error) = terminal_at_fence {
                    send_polymarket_terminal(
                        &events,
                        &raw_day_gate,
                        PolymarketDrainTerminal::Sdk(error),
                    )
                    .await;
                    return;
                }
                if events.send(PolymarketPumpItem::Fence { id, stop_after_fence }).await.is_err() {
                    break;
                }
                if stop_after_fence {
                    break;
                }
                if let Some(mut resume) = resume_after_fence {
                    let mut terminal_sent = false;
                    loop {
                        if terminal_sent {
                            tokio::select! {
                                _ = &mut resume => {}
                                _ = events.closed() => {}
                            }
                            return;
                        }
                        tokio::select! {
                            biased;
                            resumed = &mut resume => {
                                if resumed.is_err() {
                                    return;
                                }
                                break;
                            }
                            received = receiver.recv(Duration::from_millis(250)) => {
                                let message = match received {
                                    Ok(Some(message)) => message,
                                    Ok(None) => continue,
                                    Err(error) => {
                                        send_polymarket_terminal(
                                            &events,
                                            &raw_day_gate,
                                            PolymarketDrainTerminal::Sdk(error),
                                        )
                                        .await;
                                        terminal_sent = true;
                                        continue;
                                    }
                                };
                                if !enqueue_polymarket_decoded_message(
                                    message,
                                    &events,
                                    &global_budget,
                                    &decoded_budget,
                                    &raw_day_gate,
                                    recv_utc_now(),
                                )
                                .await
                                {
                                    terminal_sent = true;
                                }
                            }
                        }
                    }
                }
            }
            received = receiver.recv(Duration::from_millis(250)) => {
                let message = match received {
                    Ok(Some(message)) => message,
                    Ok(None) => continue,
                    Err(error) => {
                        send_polymarket_terminal(
                            &events,
                            &raw_day_gate,
                            PolymarketDrainTerminal::Sdk(error),
                        )
                        .await;
                        break;
                    }
                };
                if !enqueue_polymarket_decoded_message(
                    message,
                    &events,
                    &global_budget,
                    &decoded_budget,
                    &raw_day_gate,
                    recv_utc_now(),
                )
                .await
                {
                    break;
                }
            }
        }
    }
}

pub(super) async fn enqueue_polymarket_decoded_message(
    message: WsMessage,
    events: &mpsc::Sender<PolymarketPumpItem>,
    global_budget: &InflightBudget,
    decoded_budget: &InflightBudget,
    raw_day_gate: &SourceAdmissionGate,
    recv_time_utc_ms: i64,
) -> bool {
    if !message.is_market() {
        return true;
    }
    let owned_bytes = owned_polymarket_message_bytes(&message);
    if owned_bytes > MAX_TYPED_ROW_BYTES {
        let reason = format!(
            "operator_intervention_required:Polymarket decoded message exceeds {MAX_TYPED_ROW_BYTES} bytes:bytes={owned_bytes}"
        );
        send_polymarket_terminal(
            events,
            raw_day_gate,
            PolymarketDrainTerminal::Capacity {
                reason,
                recv_time_utc_ms,
                error_kind: io::ErrorKind::InvalidData,
            },
        )
        .await;
        return false;
    }
    let acquire_deadline = Instant::now() + BUFFER_ACQUIRE_TIMEOUT;
    let global_reservation =
        match acquire_polymarket_fast_path(global_budget, 1, owned_bytes, BUFFER_ACQUIRE_TIMEOUT)
            .await
        {
            Ok(reservation) => reservation,
            Err(error) => {
                let error_kind = error.kind();
                let reason = format!("polymarket_decoded_global_budget_failed:{error}");
                send_polymarket_terminal(
                    events,
                    raw_day_gate,
                    PolymarketDrainTerminal::Capacity {
                        reason,
                        recv_time_utc_ms,
                        error_kind,
                    },
                )
                .await;
                return false;
            }
        };
    let remaining = acquire_deadline.saturating_duration_since(Instant::now());
    let decoded_reservation =
        match acquire_polymarket_fast_path(decoded_budget, 1, owned_bytes, remaining).await {
            Ok(reservation) => reservation,
            Err(error) => {
                drop(global_reservation);
                let error_kind = error.kind();
                let reason = format!("polymarket_decoded_budget_failed:{error}");
                send_polymarket_terminal(
                    events,
                    raw_day_gate,
                    PolymarketDrainTerminal::Capacity {
                        reason,
                        recv_time_utc_ms,
                        error_kind,
                    },
                )
                .await;
                return false;
            }
        };
    let raw_day_ticket = match raw_day_gate
        .accept_unadmitted_event_ticket("polymarket", recv_time_utc_ms)
        .await
    {
        Ok(Some(ticket)) => ticket,
        Ok(None) => return true,
        Err(error) => {
            send_polymarket_terminal(
                events,
                raw_day_gate,
                PolymarketDrainTerminal::Admission {
                    reason: format!("polymarket_raw_day_ticket_failed:{error}"),
                    recv_time_utc_ms,
                },
            )
            .await;
            return false;
        }
    };
    let item = PolymarketPumpItem::Message(BudgetedPolymarketMessage {
        message,
        recv_time_utc_ms,
        raw_day_ticket,
        _global_reservation: global_reservation,
        _decoded_reservation: decoded_reservation,
    });
    match events.try_send(item) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(item)) => {
            match timeout(BUFFER_ACQUIRE_TIMEOUT, events.send(item)).await {
                Ok(Ok(())) => true,
                Ok(Err(_)) => false,
                Err(_) => {
                    let reason = "polymarket_decoded_queue_backpressure_timeout".to_owned();
                    send_polymarket_terminal(
                        events,
                        raw_day_gate,
                        PolymarketDrainTerminal::Capacity {
                            reason,
                            recv_time_utc_ms,
                            error_kind: io::ErrorKind::TimedOut,
                        },
                    )
                    .await;
                    false
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    }
}

pub(super) async fn send_polymarket_terminal(
    events: &mpsc::Sender<PolymarketPumpItem>,
    raw_day_gate: &SourceAdmissionGate,
    terminal: PolymarketDrainTerminal,
) {
    if polymarket_drain_terminal_is_fatal(&terminal) {
        raw_day_gate.observe_source_failure(RuntimeCloseCause::SourceInnerReturn {
            source_kind: "polymarket_market",
        });
    }
    let _ = events.send(PolymarketPumpItem::Terminal(terminal)).await;
}

fn polymarket_drain_terminal_is_fatal(terminal: &PolymarketDrainTerminal) -> bool {
    match terminal {
        PolymarketDrainTerminal::Sdk(error) => !polymarket_sdk_error_is_retryable(error),
        PolymarketDrainTerminal::Capacity { .. }
        | PolymarketDrainTerminal::Admission { .. }
        | PolymarketDrainTerminal::QueueClosed => true,
    }
}

async fn acquire_polymarket_fast_path(
    budget: &InflightBudget,
    rows: usize,
    bytes: usize,
    wait: Duration,
) -> io::Result<InflightReservation> {
    match budget.try_acquire_available(rows, bytes) {
        Ok(reservation) => Ok(reservation),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
            budget.acquire(rows, bytes, wait).await
        }
        Err(error) => Err(error),
    }
}

pub(super) async fn finalize_polymarket_outer_ready_pending<N>(
    timeline_now: &N,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
) -> Result<()>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    if !context.pending_requires_fence() {
        return Ok(());
    }
    let fatal_integrity = finalize_polymarket_ready_pending_while_paused(
        timeline_now,
        context,
        sink,
        collector_identity,
        run_started,
    )
    .await?;
    if let Some(reason) = fatal_integrity {
        return Err(invalid_data(reason).into());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn wait_for_polymarket_outer_recovery<N>(
    recovery: &mut SourceRecoverySupervisor,
    sink: &SourceEventSink,
    gamma: &GammaClient,
    deadline: Option<Instant>,
    control_rx: &mut mpsc::Receiver<PolymarketSourceControl>,
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    context: &mut PolymarketProcessingContext,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    timeline_now: Arc<N>,
) -> Result<bool>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync + 'static,
{
    match wait_for_polymarket_outer_recovery_inner(
        recovery,
        sink,
        gamma,
        deadline,
        control_rx,
        resolution_poll_tasks,
        context,
        collector_identity,
        run_started,
        timeline_now,
    )
    .await
    {
        Err(error) => Err(drain_polymarket_anchors_after_processing_error(
            error,
            context,
            sink,
            collector_identity,
            run_started,
            recovery,
        )
        .await),
        result => result,
    }
}

#[allow(clippy::too_many_arguments)]
async fn wait_for_polymarket_outer_recovery_inner<N>(
    recovery: &mut SourceRecoverySupervisor,
    sink: &SourceEventSink,
    gamma: &GammaClient,
    deadline: Option<Instant>,
    control_rx: &mut mpsc::Receiver<PolymarketSourceControl>,
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    context: &mut PolymarketProcessingContext,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    timeline_now: Arc<N>,
) -> Result<bool>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync + 'static,
{
    let wait = recovery.next_wait();
    let delay = deadline_remaining(deadline)
        .map(|remaining| remaining.min(wait.delay))
        .unwrap_or(wait.delay);
    if delay.is_zero() {
        quiesce_polymarket_resolution_actor(
            Vec::new(),
            resolution_poll_tasks,
            &*timeline_now,
            context,
            sink,
            collector_identity,
            run_started,
            recovery,
        )
        .await?;
        return Ok(false);
    }
    sink.source_queue_backpressure
        .record_reconnect(recovery.source_kind, timeline_now().utc_ms);
    sink.source_queue_backpressure
        .set_recovery_active(recovery.source_kind, true);
    let retry_at = TokioInstant::now() + delay;
    loop {
        let now = timeline_now();
        context.actor_now_utc_ms = now.utc_ms;
        let discovery_authorities_complete =
            polymarket_context_discovery_authorities_complete(context);
        if discovery_authorities_complete {
            advance_polymarket_boundaries(
                now.utc_ms,
                context,
                sink,
                collector_identity,
                run_started,
            )
            .await?;
        }
        finalize_polymarket_outer_ready_pending(
            &*timeline_now,
            context,
            sink,
            collector_identity,
            run_started,
        )
        .await?;
        spawn_due_gamma_resolution_polls(
            gamma,
            resolution_poll_tasks,
            &timeline_now,
            context,
            now,
            sink,
        )?;
        drain_ready_market_anchor_requests(context, sink, run_started).await?;
        if discovery_authorities_complete && control_rx.is_empty() {
            spawn_due_market_anchor_requests(
                gamma,
                context,
                sink,
                collector_identity,
                &timeline_now,
            )?;
        }
        let next_actor_at =
            next_polymarket_outer_actor_at(&*timeline_now, context, discovery_authorities_complete)
                .into_iter()
                .chain(
                    discovery_authorities_complete
                        .then(|| next_market_anchor_request_at(context, now))
                        .flatten(),
                )
                .min();
        let has_next_actor_wake = next_actor_at.is_some();
        let next_actor_at = next_actor_at.unwrap_or(retry_at);
        tokio::select! {
            biased;
            control = control_rx.recv() => {
                let Some(PolymarketSourceControl::Fence {
                    stop_after_fence,
                    day_boundary_utc_ms,
                    resume_after_barrier,
                    reply,
                }) = control else {
                    quiesce_polymarket_resolution_actor(
                        Vec::new(),
                        resolution_poll_tasks,
                        &*timeline_now,
                        context,
                        sink,
                        collector_identity,
                        run_started,
                        recovery,
                    )
                    .await?;
                    return Ok(false);
                };
                if stop_after_fence {
                    if day_boundary_utc_ms.is_some() {
                        let error =
                            invalid_data("Polymarket outer stop fence carried a UTC day boundary");
                        let _ = reply.send(Err(error.to_string()));
                        return Err(error.into());
                    }
                    let result = quiesce_polymarket_resolution_actor(
                        Vec::new(),
                        resolution_poll_tasks,
                        &*timeline_now,
                        context,
                        sink,
                        collector_identity,
                        run_started,
                        recovery,
                    )
                    .await;
                    match result {
                        Ok(()) => {
                            let _ = reply.send(Ok(()));
                            return Ok(false);
                        }
                        Err(error) => {
                            let _ = reply.send(Err(error.to_string()));
                            return Err(error);
                        }
                    }
                }
                if day_boundary_utc_ms.is_some() != resume_after_barrier.is_some() {
                    let error = invalid_data(
                        "Polymarket outer day boundary and resume signal must be paired",
                    );
                    let _ = reply.send(Err(error.to_string()));
                    return Err(error.into());
                }
                abort_ws_claimed_gamma_resolution_polls(context);
                let finalize_result: Result<()> = async {
                    let mut anchors = Vec::new();
                    collect_all_market_anchor_items(context, &mut anchors).await?;
                    process_polymarket_boundary_drain_items(anchors, day_boundary_utc_ms, &*timeline_now, context, sink, collector_identity, run_started, recovery).await?;
                    drain_all_gamma_resolution_polls(
                        resolution_poll_tasks,
                        &*timeline_now,
                        context,
                        sink,
                        collector_identity,
                        run_started,
                    )
                    .await?;
                    if let Some(day_boundary_utc_ms) = day_boundary_utc_ms {
                        let boundary_now = timeline_now();
                        context.actor_now_utc_ms = boundary_now.utc_ms;
                        if boundary_now.utc_ms < day_boundary_utc_ms {
                            return Err(invalid_data(format!(
                                "Polymarket outer day fence preceded its UTC boundary:now={}:boundary={day_boundary_utc_ms}",
                                boundary_now.utc_ms
                            ))
                            .into());
                        }
                        preflight_polymarket_early_ws_claims_for_current_boundary(
                            boundary_now.utc_ms,
                            context,
                        )?;
                        admit_missing_polymarket_context_metadata(
                            context,
                            sink,
                            collector_identity,
                            run_started,
                            Some(day_boundary_utc_ms),
                        )
                        .await?;
                        if polymarket_context_discovery_authorities_complete(context) {
                            advance_polymarket_boundaries(
                                boundary_now.utc_ms,
                                context,
                                sink,
                                collector_identity,
                                run_started,
                            )
                            .await?;
                        }
                    }
                    let fatal_integrity =
                        finalize_polymarket_ready_pending_while_paused(
                            &*timeline_now,
                            context,
                            sink,
                            collector_identity,
                            run_started,
                        )
                        .await?;
                    if let Some(reason) = fatal_integrity {
                        return Err(invalid_data(reason).into());
                    }
                    Ok(())
                }
                .await;
                if let Err(error) = finalize_result {
                    let _ = reply.send(Err(error.to_string()));
                    return Err(error);
                }
                let _ = reply.send(Ok(()));
                if let Some(resume) = resume_after_barrier
                    && resume.await.is_err()
                {
                    return Err(invalid_data(
                        "Polymarket outer recovery day-barrier resume dropped",
                    )
                    .into());
                }
                return Ok(!deadline_reached(deadline));
            }
            result = context.market_anchor.tasks.join_next(), if !context.market_anchor.tasks.is_empty() => {
                if let Some(result) = result {
                    record_market_anchor_completion(result.map_err(|error| invalid_data(format!("market anchor request task failed:{error}")))?, context, sink, run_started).await?;
                }
            }
            result = resolution_poll_tasks.join_next(), if !resolution_poll_tasks.is_empty() => {
                let Some(result) = result else {
                    continue;
                };
                record_gamma_resolution_join_result(
                    result,
                    &*timeline_now,
                    context,
                    sink,
                    collector_identity,
                    run_started,
                )
                .await?;
                abort_ws_claimed_gamma_resolution_polls(context);
                finalize_polymarket_outer_ready_pending(
                    &*timeline_now,
                    context,
                    sink,
                    collector_identity,
                    run_started,
                )
                .await?;
            }
            () = tokio::time::sleep_until(next_actor_at), if has_next_actor_wake => {}
            () = tokio::time::sleep_until(retry_at) => {
                if deadline_reached(deadline) {
                    quiesce_polymarket_resolution_actor(
                        Vec::new(),
                        resolution_poll_tasks,
                        &*timeline_now,
                        context,
                        sink,
                        collector_identity,
                        run_started,
                        recovery,
                    )
                    .await?;
                    return Ok(false);
                }
                return Ok(true);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_polymarket_source_with_spec(
    spec: PolymarketSourceSpec,
    _dataset_root: PathBuf,
    sink: SourceEventSink,
    collector_identity: CollectorIdentity,
    deadline_ms: Option<i64>,
    run_started: Instant,
    mut control_rx: mpsc::Receiver<PolymarketSourceControl>,
    startup_recovery: PolymarketRestartRecovery,
) -> Result<()> {
    let gamma = GammaClient::new(&spec.gamma_endpoint)?;
    let source_sequence_epoch_id = polymarket_source_sequence_epoch_id(&collector_identity)?;
    let deadline = deadline_ms.map(|deadline_ms| {
        Instant::now()
            + Duration::from_millis(
                u64::try_from(deadline_ms.saturating_sub(now_ms())).unwrap_or(0),
            )
    });
    let mut recovery = SourceRecoverySupervisor::new(
        "polymarket_market",
        u64::try_from(now_ms()).unwrap_or_default(),
    );
    #[cfg(test)]
    recovery.set_wait_override(spec.recovery_wait_override);
    let mut startup_recovery = startup_recovery;
    let mut restart_fixed_identities = std::mem::take(&mut startup_recovery.fixed_identities);
    let mut context = PolymarketProcessingContext::from_recovery_with_source_epoch(
        startup_recovery,
        source_sequence_epoch_id.clone(),
    )?;
    let mut resolution_poll_tasks = JoinSet::<GammaResolutionPollCompletion>::new();
    while deadline_ms.is_none_or(|deadline_ms| now_ms() < deadline_ms) {
        let discovery = if context.is_none() {
            let discovery = match discover_initial_polymarket_context(
                &gamma,
                spec.discovery_wait,
                spec.discovery_retry_delay,
                &mut control_rx,
            )
            .await
            {
                Ok(Some(discovery)) if gamma_discovery_is_current(&discovery, now_ms()) => {
                    discovery
                }
                Ok(Some(_)) => continue,
                Ok(None) => return Ok(()),
                Err(PolymarketDiscoveryError::Unavailable(_)) => {
                    if !wait_for_source_recovery(&mut recovery, &sink, deadline).await {
                        break;
                    }
                    continue;
                }
                Err(error) => {
                    let error = polymarket_restart_discovery_integrity_error(
                        &restart_fixed_identities,
                        &error,
                    );
                    record_gap(
                        &sink.gaps,
                        format!("polymarket_discovery_integrity:{error}"),
                    );
                    return Err(error.into());
                }
            };
            context = Some(PolymarketProcessingContext::new_with_source_epoch(
                discovery.clone(),
                source_sequence_epoch_id.clone(),
            ));
            Some(discovery)
        } else {
            None
        };
        if let Some(recovered) = context.as_mut() {
            recovered.reclassify_recovered_pending(now_ms())?;
        }
        if clear_empty_polymarket_context(
            &mut context,
            &sink,
            &collector_identity,
            run_started,
            &mut recovery,
        )
        .await?
        {
            continue;
        }
        let context = context
            .as_mut()
            .ok_or_else(|| invalid_data("Polymarket processing context was not initialized"))?;
        let raw_day_gate = sink
            .source_queue_backpressure
            .admission_gate()
            .ok_or_else(|| invalid_data("raw-day gate missing before Polymarket runtime start"))?;
        let mut market_runtime = PolymarketDrainRuntime::new(
            spec.market_ws_endpoint.clone(),
            spec.ws_config.clone(),
            sink.inflight_budget.clone(),
            raw_day_gate,
        )?;
        let collected = collect_polymarket_single_receiver(
            discovery,
            context,
            &mut resolution_poll_tasks,
            &sink,
            &collector_identity,
            deadline_ms,
            run_started,
            &mut market_runtime,
            &gamma,
            &mut recovery,
            &mut control_rx,
            spec.discovery_wait,
            spec.discovery_retry_delay,
            &mut restart_fixed_identities,
            production_polymarket_timeline_now,
        )
        .await;
        let collected = match collected {
            Ok(()) if market_runtime.pump.is_some() => {
                fence_polymarket_processor(
                    &mut market_runtime,
                    true,
                    context,
                    &sink,
                    &collector_identity,
                    run_started,
                    &mut recovery,
                )
                .await
            }
            Err(error) if market_runtime.pump.is_some() => {
                stop_polymarket_after_processing_error(
                    error,
                    &mut market_runtime,
                    context,
                    &sink,
                    &collector_identity,
                    run_started,
                    &mut recovery,
                )
                .await
            }
            result => result,
        };
        drop(market_runtime);
        match collected {
            Ok(()) => return Ok(()),
            Err(error) if market_source_error_is_retryable(error.as_ref()) => {
                if !wait_for_polymarket_outer_recovery(
                    &mut recovery,
                    &sink,
                    &gamma,
                    deadline,
                    &mut control_rx,
                    &mut resolution_poll_tasks,
                    context,
                    &collector_identity,
                    run_started,
                    Arc::new(production_polymarket_timeline_now),
                )
                .await?
                {
                    break;
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

pub(super) async fn clear_empty_polymarket_context(
    context: &mut Option<PolymarketProcessingContext>,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<bool> {
    let Some(retired) = context
        .as_mut()
        .filter(|context| context.current.is_none() && context.pending.is_empty())
    else {
        return Ok(false);
    };
    let mut anchors = Vec::new();
    collect_all_market_anchor_items(retired, &mut anchors).await?;
    process_polymarket_boundary_drain_items(
        anchors,
        None,
        &production_polymarket_timeline_now,
        retired,
        sink,
        collector_identity,
        run_started,
        recovery,
    )
    .await?;
    *context = None;
    Ok(true)
}

pub(super) fn market_source_error_is_retryable(error: &(dyn Error + 'static)) -> bool {
    error
        .downcast_ref::<SdkStreamError>()
        .is_some_and(polymarket_sdk_error_is_retryable)
        || matches!(
            error.downcast_ref::<PolymarketDiscoveryError>(),
            Some(PolymarketDiscoveryError::Unavailable(_))
        )
}

fn polymarket_sdk_error_is_retryable(error: &SdkStreamError) -> bool {
    matches!(
        error,
        SdkStreamError::Connection(_)
            | SdkStreamError::Subscription(_)
            | SdkStreamError::TerminalDisconnected
            | SdkStreamError::Timeout(_)
    )
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum PolymarketDiscoveryError {
    Unavailable(String),
    Integrity(String),
    FixedIdentitySelection {
        expected_slug: String,
        detail: String,
    },
}

impl PolymarketDiscoveryError {
    #[cfg(test)]
    pub(super) fn is_integrity(&self) -> bool {
        !matches!(self, Self::Unavailable(_))
    }

    fn detail(&self) -> &str {
        match self {
            Self::Unavailable(detail)
            | Self::Integrity(detail)
            | Self::FixedIdentitySelection { detail, .. } => detail,
        }
    }

    fn fixed_identity_expected_slug(&self) -> Option<&str> {
        match self {
            Self::FixedIdentitySelection { expected_slug, .. } => Some(expected_slug),
            Self::Unavailable(_) | Self::Integrity(_) => None,
        }
    }
}

impl std::fmt::Display for PolymarketDiscoveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(message) => write!(formatter, "Gamma unavailable: {message}"),
            Self::Integrity(message)
            | Self::FixedIdentitySelection {
                detail: message, ..
            } => write!(formatter, "Gamma integrity failure: {message}"),
        }
    }
}

impl Error for PolymarketDiscoveryError {}

async fn discover_initial_polymarket_context(
    gamma: &GammaClient,
    discovery_wait: Duration,
    discovery_retry_delay: Duration,
    control_rx: &mut mpsc::Receiver<PolymarketSourceControl>,
) -> std::result::Result<Option<BtcUpdownDiscovery>, PolymarketDiscoveryError> {
    select_initial_polymarket_context(
        discover_markets_with_wait(gamma, discovery_wait, discovery_retry_delay),
        control_rx,
    )
    .await
}

pub(super) async fn select_initial_polymarket_context<D>(
    discovery: D,
    control_rx: &mut mpsc::Receiver<PolymarketSourceControl>,
) -> std::result::Result<Option<BtcUpdownDiscovery>, PolymarketDiscoveryError>
where
    D: std::future::Future<
            Output = std::result::Result<BtcUpdownDiscovery, PolymarketDiscoveryError>,
        >,
{
    tokio::pin!(discovery);
    loop {
        tokio::select! {
            biased;
            control = control_rx.recv() => {
                let Some(PolymarketSourceControl::Fence {
                    stop_after_fence,
                    day_boundary_utc_ms: _,
                    resume_after_barrier,
                    reply,
                }) = control else {
                    return Ok(None);
                };
                let _ = reply.send(Ok(()));
                if stop_after_fence {
                    return Ok(None);
                }
                if let Some(resume) = resume_after_barrier {
                    resume.await.map_err(|_| PolymarketDiscoveryError::Integrity(
                        "Polymarket initial discovery barrier resume dropped".to_owned()
                    ))?;
                }
            }
            result = &mut discovery => return result.map(Some),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct PolymarketWsTerminalEvidence {
    pub(super) source_sequence_epoch_id: String,
    pub(super) collector_admission_sequence: u64,
    pub(super) source_received_at_utc_ms: i64,
    pub(super) validation_completed_at_utc_ms: i64,
    pub(super) source_payload_json: String,
    pub(super) source_payload_sha256: String,
    pub(super) platform_timestamp_raw: String,
    pub(super) platform_timestamp_utc_ms: i64,
    pub(super) winning_asset_id: String,
    pub(super) winning_outcome: String,
    pub(super) raw_day_ticket: RawDayAcceptedTicket,
}

impl PolymarketWsTerminalEvidence {
    fn dynamic_owned_bytes(&self) -> usize {
        self.source_sequence_epoch_id
            .capacity()
            .saturating_add(self.source_payload_json.capacity())
            .saturating_add(self.source_payload_sha256.capacity())
            .saturating_add(self.platform_timestamp_raw.capacity())
            .saturating_add(self.winning_asset_id.capacity())
            .saturating_add(self.winning_outcome.capacity())
            .saturating_add(self.raw_day_ticket.dynamic_owned_bytes())
    }
}

#[derive(Clone, Debug)]
pub(super) struct AcceptedPolymarketMetadataRow {
    pub(super) row: PolymarketMarketMetadataRow,
    pub(super) raw_day_ticket: RawDayAcceptedTicket,
    pub(super) ws_claimant_source_sequence_epoch_id: Option<String>,
    pub(super) ws_claimant_collector_admission_sequence: Option<u64>,
}

impl AcceptedPolymarketMetadataRow {
    fn dynamic_owned_bytes(&self) -> usize {
        self.row
            .collector_host_id
            .capacity()
            .saturating_add(self.row.collector_region.capacity())
            .saturating_add(self.row.schema_version.capacity())
            .saturating_add(self.row.slug.capacity())
            .saturating_add(self.row.condition_id.capacity())
            .saturating_add(self.row.clob_market_id.capacity())
            .saturating_add(self.row.asset_ids.capacity())
            .saturating_add(
                self.row
                    .source_endpoint
                    .as_ref()
                    .map_or(0, String::capacity),
            )
            .saturating_add(
                self.row
                    .source_evidence_payload_json
                    .as_ref()
                    .map_or(0, String::capacity),
            )
            .saturating_add(
                self.row
                    .source_evidence_raw_sha256
                    .as_ref()
                    .map_or(0, String::capacity),
            )
            .saturating_add(
                self.row
                    .closed_time_raw
                    .as_ref()
                    .map_or(0, String::capacity),
            )
            .saturating_add(
                self.row
                    .uma_resolution_status
                    .as_ref()
                    .map_or(0, String::capacity),
            )
            .saturating_add(self.row.outcomes_raw.as_ref().map_or(0, String::capacity))
            .saturating_add(
                self.row
                    .outcome_prices_raw
                    .as_ref()
                    .map_or(0, String::capacity),
            )
            .saturating_add(
                self.row
                    .ws_platform_timestamp_raw
                    .as_ref()
                    .map_or(0, String::capacity),
            )
            .saturating_add(
                self.row
                    .winning_asset_id
                    .as_ref()
                    .map_or(0, String::capacity),
            )
            .saturating_add(
                self.row
                    .winning_outcome
                    .as_ref()
                    .map_or(0, String::capacity),
            )
            .saturating_add(self.raw_day_ticket.dynamic_owned_bytes())
            .saturating_add(
                self.ws_claimant_source_sequence_epoch_id
                    .as_ref()
                    .map_or(0, String::capacity),
            )
    }
}

#[cfg(test)]
impl std::ops::Deref for AcceptedPolymarketMetadataRow {
    type Target = PolymarketMarketMetadataRow;

    fn deref(&self) -> &Self::Target {
        &self.row
    }
}

#[derive(Clone, Debug)]
pub(super) enum PolymarketTerminalClaim {
    MarketResolvedWs(Box<PolymarketWsTerminalEvidence>),
    GammaBySlug(Box<AcceptedPolymarketMetadataRow>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PendingPolymarketIdentity {
    pub(super) slug: String,
    pub(super) clob_market_id: String,
    pub(super) condition_id: String,
    pub(super) up_asset_id: String,
    pub(super) down_asset_id: String,
    pub(super) market_start_utc_ms: i64,
    pub(super) market_end_utc_ms: i64,
    pub(super) fixed_identity_verified_at_utc_ms: i64,
    pub(super) discovery_source_received_at_utc_ms: i64,
    pub(super) discovery_source_raw_sha256: String,
}

impl PendingPolymarketIdentity {
    pub(super) fn from_discovered(market: &DiscoveredBtcUpdownMarket) -> Result<Self> {
        Ok(Self {
            slug: market.slug.clone(),
            clob_market_id: market.id.clone(),
            condition_id: market.condition_id.clone(),
            up_asset_id: market.up_asset_id.clone(),
            down_asset_id: market.down_asset_id.clone(),
            market_start_utc_ms: market.start_ts_s.saturating_mul(1_000),
            market_end_utc_ms: market.end_ts_s.saturating_mul(1_000),
            fixed_identity_verified_at_utc_ms: market.fixed_identity_verified_at_utc_ms,
            discovery_source_received_at_utc_ms: market.metadata_received_utc_ms.ok_or_else(
                || invalid_data("pending Polymarket identity is missing discovery receive time"),
            )?,
            discovery_source_raw_sha256: market
                .raw_json_sha256
                .clone()
                .filter(|digest| !digest.trim().is_empty())
                .ok_or_else(|| {
                    invalid_data("pending Polymarket identity is missing discovery digest")
                })?,
        })
    }

    fn from_recovered(recovered: &RecoveredPolymarketPending) -> Self {
        Self {
            slug: recovered.slug.clone(),
            clob_market_id: recovered.clob_market_id.clone(),
            condition_id: recovered.condition_id.clone(),
            up_asset_id: recovered.up_asset_id.clone(),
            down_asset_id: recovered.down_asset_id.clone(),
            market_start_utc_ms: recovered.market_start_utc_ms,
            market_end_utc_ms: recovered.market_end_utc_ms,
            fixed_identity_verified_at_utc_ms: recovered.fixed_identity_verified_at_utc_ms,
            discovery_source_received_at_utc_ms: recovered.discovery_source_received_at_utc_ms,
            discovery_source_raw_sha256: recovered.discovery_source_raw_sha256.clone(),
        }
    }

    fn dynamic_owned_bytes(&self) -> usize {
        self.slug
            .capacity()
            .saturating_add(self.clob_market_id.capacity())
            .saturating_add(self.condition_id.capacity())
            .saturating_add(self.up_asset_id.capacity())
            .saturating_add(self.down_asset_id.capacity())
            .saturating_add(self.discovery_source_raw_sha256.capacity())
    }

    pub(super) fn gamma_terminal_identity(&self) -> GammaTerminalFixedIdentity {
        GammaTerminalFixedIdentity {
            slug: self.slug.clone(),
            clob_market_id: self.clob_market_id.clone(),
            condition_id: self.condition_id.clone(),
            up_asset_id: self.up_asset_id.clone(),
            down_asset_id: self.down_asset_id.clone(),
            market_end_utc_ms: self.market_end_utc_ms,
        }
    }

    fn identity_authority(&self) -> VerifiedPolymarketMarketIdentity {
        VerifiedPolymarketMarketIdentity {
            market: PolymarketMarketIdentity {
                slug: self.slug.clone(),
                condition_id: self.condition_id.clone(),
                clob_market_id: self.clob_market_id.clone(),
                ordered_up_down_asset_ids_json: serde_json::to_string(&[
                    self.up_asset_id.as_str(),
                    self.down_asset_id.as_str(),
                ])
                .unwrap_or_default(),
                market_start_utc_ms: self.market_start_utc_ms,
                market_end_utc_ms: self.market_end_utc_ms,
            },
            first_verified_at_utc_ms: self.fixed_identity_verified_at_utc_ms,
        }
    }
}

#[derive(Debug)]
pub(super) struct PendingPolymarketResolution {
    pub(super) identity: PendingPolymarketIdentity,
    pub(super) next_attempt_ordinal: u8,
    pub(super) in_flight_ordinal: Option<u8>,
    pub(super) poll_abort_handle: Option<tokio::task::AbortHandle>,
    pub(super) attempts_recorded: u32,
    pub(super) valid_response_count: u32,
    pub(super) transport_unavailable_count: u32,
    pub(super) missed_slot_count: u32,
    pub(super) last_valid_metadata_received_at_utc_ms: Option<i64>,
    pub(super) claim: Option<PolymarketTerminalClaim>,
    pub(super) exhaustion_ticket: Option<RawDayAcceptedTicket>,
    pub(super) fatal_integrity_error: bool,
    pub(super) buffered_attempt_rows: Vec<AcceptedPolymarketMetadataRow>,
    pub(super) recovery_missed_ordinals: VecDeque<u8>,
    pub(super) recovery_reclassification_pending: bool,
}

impl PendingPolymarketResolution {
    pub(super) fn new(
        identity: PendingPolymarketIdentity,
        early_claim: Option<PolymarketWsTerminalEvidence>,
    ) -> Self {
        Self {
            identity,
            next_attempt_ordinal: 1,
            in_flight_ordinal: None,
            poll_abort_handle: None,
            attempts_recorded: 0,
            valid_response_count: 0,
            transport_unavailable_count: 0,
            missed_slot_count: 0,
            last_valid_metadata_received_at_utc_ms: None,
            claim: early_claim
                .map(|claim| PolymarketTerminalClaim::MarketResolvedWs(Box::new(claim))),
            exhaustion_ticket: None,
            fatal_integrity_error: false,
            buffered_attempt_rows: Vec::new(),
            recovery_missed_ordinals: VecDeque::new(),
            recovery_reclassification_pending: false,
        }
    }

    fn from_recovered(recovered: RecoveredPolymarketPending) -> Self {
        Self {
            identity: PendingPolymarketIdentity::from_recovered(&recovered),
            next_attempt_ordinal: recovered.next_attempt_ordinal,
            in_flight_ordinal: None,
            poll_abort_handle: None,
            attempts_recorded: recovered.attempts_recorded,
            valid_response_count: recovered.valid_response_count,
            transport_unavailable_count: recovered.transport_unavailable_count,
            missed_slot_count: recovered.missed_slot_count,
            last_valid_metadata_received_at_utc_ms: recovered
                .last_valid_metadata_received_at_utc_ms,
            claim: None,
            exhaustion_ticket: None,
            fatal_integrity_error: false,
            buffered_attempt_rows: Vec::new(),
            recovery_missed_ordinals: recovered.missed_attempt_ordinals.into(),
            recovery_reclassification_pending: true,
        }
    }

    pub(super) fn budget_horizon_utc_ms(&self) -> i64 {
        self.identity.market_end_utc_ms.saturating_add(8 * 60_000)
    }

    pub(super) fn slot_utc_ms(&self, ordinal: u8) -> i64 {
        self.identity
            .market_end_utc_ms
            .saturating_add(POLYMARKET_RESOLUTION_FIRST_SLOT_DELAY_MS)
            .saturating_add(
                i64::from(ordinal.saturating_sub(1))
                    .saturating_mul(POLYMARKET_RESOLUTION_SLOT_INTERVAL_MS),
            )
    }

    pub(super) fn requires_fence(&self) -> bool {
        (self.claim.is_some() || self.exhaustion_ticket.is_some())
            && self.in_flight_ordinal.is_none()
            && self.poll_abort_handle.is_none()
    }

    fn owned_bytes(&self) -> usize {
        let claim_bytes = match &self.claim {
            Some(PolymarketTerminalClaim::MarketResolvedWs(claim)) => {
                std::mem::size_of::<PolymarketWsTerminalEvidence>()
                    .saturating_add(claim.dynamic_owned_bytes())
            }
            Some(PolymarketTerminalClaim::GammaBySlug(row)) => {
                std::mem::size_of::<AcceptedPolymarketMetadataRow>()
                    .saturating_add(row.dynamic_owned_bytes())
            }
            None => 0,
        };
        let buffered_bytes = self.buffered_attempt_rows.iter().fold(
            self.buffered_attempt_rows
                .capacity()
                .saturating_mul(std::mem::size_of::<AcceptedPolymarketMetadataRow>()),
            |total, accepted| total.saturating_add(accepted.dynamic_owned_bytes()),
        );
        std::mem::size_of::<Self>()
            .saturating_add(self.identity.dynamic_owned_bytes())
            .saturating_add(claim_bytes)
            .saturating_add(buffered_bytes)
            .saturating_add(
                self.exhaustion_ticket
                    .as_ref()
                    .map_or(0, RawDayAcceptedTicket::dynamic_owned_bytes),
            )
            .saturating_add(
                self.recovery_missed_ordinals
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u8>()),
            )
    }
}

#[derive(Debug)]
pub(super) enum GammaResolutionPollFailure {
    Transport {
        class: ResolutionTransportErrorClass,
        http_status: Option<u16>,
    },
    SdkDecodeOrSchema,
}

#[derive(Debug)]
pub(super) enum RawDayTicketAcceptance {
    Accepted(RawDayAcceptedTicket),
    Cutoff,
    Failed(String),
}

#[derive(Debug)]
pub(super) struct GammaResolutionPollCompletion {
    pub(super) slug: String,
    pub(super) ordinal: u8,
    pub(super) scheduled_at_utc_ms: i64,
    pub(super) fetch_start_utc_ms: i64,
    pub(super) completed_at_utc_ms: i64,
    pub(super) result: std::result::Result<SdkGammaMarket, GammaResolutionPollFailure>,
    pub(super) raw_day_ticket: RawDayTicketAcceptance,
}

pub(super) struct PolymarketProcessingContext {
    pub(super) market_anchor: MarketAnchorRuntime,
    pub(super) source_sequence_epoch_id: String,
    pub(super) current: Option<DiscoveredBtcUpdownMarket>,
    pub(super) next: Option<DiscoveredBtcUpdownMarket>,
    pub(super) pending: BTreeMap<String, PendingPolymarketResolution>,
    pub(super) early_ws_claims: BTreeMap<String, PolymarketWsTerminalEvidence>,
    pub(super) source_authorities: Vec<SourceAuthority>,
    pub(super) active_asset_to_slug: BTreeMap<String, String>,
    pub(super) asset_to_slug: BTreeMap<String, String>,
    pub(super) actor_now_utc_ms: i64,
    pub(super) actor_utc_now: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct PolymarketTimelineNow {
    pub(super) utc_ms: i64,
    pub(super) monotonic: TokioInstant,
}

pub(super) fn production_polymarket_timeline_now() -> PolymarketTimelineNow {
    PolymarketTimelineNow {
        utc_ms: now_ms(),
        monotonic: TokioInstant::now(),
    }
}

impl PolymarketProcessingContext {
    #[cfg(test)]
    pub(super) fn new(discovery: BtcUpdownDiscovery) -> Self {
        Self::new_with_source_epoch(discovery, "test-polymarket-source-epoch".to_owned())
    }

    pub(super) fn new_with_source_epoch(
        discovery: BtcUpdownDiscovery,
        source_sequence_epoch_id: String,
    ) -> Self {
        let mut context = Self {
            market_anchor: MarketAnchorRuntime::default(),
            source_sequence_epoch_id,
            current: Some(discovery.current),
            next: discovery.next,
            pending: BTreeMap::new(),
            early_ws_claims: BTreeMap::new(),
            source_authorities: Vec::new(),
            active_asset_to_slug: BTreeMap::new(),
            asset_to_slug: BTreeMap::new(),
            actor_now_utc_ms: 0,
            actor_utc_now: None,
        };
        context.rebuild_asset_to_slug();
        context
    }

    #[cfg(test)]
    pub(super) fn from_recovery(recovery: PolymarketRestartRecovery) -> Result<Option<Self>> {
        Self::from_recovery_with_source_epoch(recovery, "test-polymarket-source-epoch".to_owned())
    }

    pub(super) fn from_recovery_with_source_epoch(
        recovery: PolymarketRestartRecovery,
        source_sequence_epoch_id: String,
    ) -> Result<Option<Self>> {
        if recovery.pending.is_empty() {
            return Ok(None);
        }
        let mut context = Self {
            market_anchor: MarketAnchorRuntime::default(),
            source_sequence_epoch_id,
            current: None,
            next: None,
            pending: recovery
                .pending
                .into_iter()
                .map(|recovered| {
                    (
                        recovered.slug.clone(),
                        PendingPolymarketResolution::from_recovered(recovered),
                    )
                })
                .collect(),
            early_ws_claims: BTreeMap::new(),
            source_authorities: Vec::new(),
            active_asset_to_slug: BTreeMap::new(),
            asset_to_slug: BTreeMap::new(),
            actor_now_utc_ms: 0,
            actor_utc_now: None,
        };
        context.rebuild_asset_to_slug();
        context.ensure_pending_bounds()?;
        Ok(Some(context))
    }

    pub(super) fn reclassify_recovered_pending(&mut self, observed_at_utc_ms: i64) -> Result<()> {
        let mut no_future_slot = Vec::new();
        for (slug, pending) in &mut self.pending {
            if !pending.recovery_reclassification_pending {
                continue;
            }
            while pending.next_attempt_ordinal <= POLYMARKET_RESOLUTION_ATTEMPT_COUNT
                && pending.slot_utc_ms(pending.next_attempt_ordinal) <= observed_at_utc_ms
            {
                pending
                    .recovery_missed_ordinals
                    .push_back(pending.next_attempt_ordinal);
                pending.next_attempt_ordinal = pending.next_attempt_ordinal.saturating_add(1);
            }
            pending.recovery_reclassification_pending = false;
            if pending.next_attempt_ordinal > POLYMARKET_RESOLUTION_ATTEMPT_COUNT {
                no_future_slot.push(slug.clone());
            }
        }
        for slug in no_future_slot {
            self.pending.remove(&slug);
        }
        self.rebuild_asset_to_slug();
        self.ensure_pending_bounds()
    }

    pub(super) fn replace_discovery(&mut self, discovery: BtcUpdownDiscovery) {
        self.current = Some(discovery.current);
        self.next = discovery.next;
        self.rebuild_asset_to_slug();
    }

    pub(super) fn discovery_snapshot(&self) -> Option<BtcUpdownDiscovery> {
        self.current.clone().map(|current| BtcUpdownDiscovery {
            current,
            next: self.next.clone(),
        })
    }

    pub(super) fn rebuild_asset_to_slug(&mut self) {
        self.active_asset_to_slug.clear();
        self.asset_to_slug.clear();
        for market in self.current.iter().chain(self.next.iter()) {
            self.active_asset_to_slug
                .insert(market.up_asset_id.clone(), market.slug.clone());
            self.active_asset_to_slug
                .insert(market.down_asset_id.clone(), market.slug.clone());
            self.asset_to_slug
                .insert(market.up_asset_id.clone(), market.slug.clone());
            self.asset_to_slug
                .insert(market.down_asset_id.clone(), market.slug.clone());
        }
        for pending in self.pending.values() {
            self.asset_to_slug.insert(
                pending.identity.up_asset_id.clone(),
                pending.identity.slug.clone(),
            );
            self.asset_to_slug.insert(
                pending.identity.down_asset_id.clone(),
                pending.identity.slug.clone(),
            );
        }
        let owned_slugs = self
            .current
            .iter()
            .chain(self.next.iter())
            .map(|market| market.slug.as_str())
            .chain(self.pending.keys().map(String::as_str))
            .collect::<BTreeSet<_>>();
        self.early_ws_claims
            .retain(|slug, _| owned_slugs.contains(slug.as_str()));
        self.source_authorities.retain(|authority| {
            self.current
                .iter()
                .chain(self.next.iter())
                .any(|market| polymarket_authority_matches_discovered(authority, market))
                || self.pending.values().any(|pending| {
                    polymarket_authority_matches_identity(authority, &pending.identity)
                })
        });
    }

    pub(super) fn desired_assets(&self) -> Result<BTreeSet<U256>> {
        self.asset_to_slug
            .keys()
            .map(|asset| U256::from_str(asset).map_err(Into::into))
            .collect()
    }

    fn matched_resolved_slug(&self, event: &MarketResolved) -> Option<String> {
        let condition_id = event.market.to_string();
        let event_assets = event
            .asset_ids
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        let winner = event.winning_asset_id.as_ref().map(ToString::to_string);
        if let Some(market) = self.current.iter().chain(self.next.iter()).find(|market| {
            market.id == event.id
                || market.condition_id == condition_id
                || event.slug.as_deref() == Some(market.slug.as_str())
                || event_assets.contains(&market.up_asset_id)
                || event_assets.contains(&market.down_asset_id)
                || winner.as_deref() == Some(market.up_asset_id.as_str())
                || winner.as_deref() == Some(market.down_asset_id.as_str())
        }) {
            return Some(market.slug.clone());
        }
        self.pending.values().find_map(|pending| {
            let identity = &pending.identity;
            (identity.clob_market_id == event.id
                || identity.condition_id == condition_id
                || event.slug.as_deref() == Some(identity.slug.as_str())
                || event_assets.contains(&identity.up_asset_id)
                || event_assets.contains(&identity.down_asset_id)
                || winner.as_deref() == Some(identity.up_asset_id.as_str())
                || winner.as_deref() == Some(identity.down_asset_id.as_str()))
            .then(|| identity.slug.clone())
        })
    }

    fn pending_requires_fence(&self) -> bool {
        self.pending
            .values()
            .any(PendingPolymarketResolution::requires_fence)
    }

    pub(super) fn pending_owned_bytes(&self) -> usize {
        let pending = self.pending.iter().fold(0_usize, |total, (slug, pending)| {
            total
                .saturating_add(std::mem::size_of::<String>())
                .saturating_add(slug.capacity())
                .saturating_add(pending.owned_bytes())
        });
        let early_claims = self
            .early_ws_claims
            .iter()
            .fold(pending, |total, (slug, evidence)| {
                total
                    .saturating_add(std::mem::size_of::<String>())
                    .saturating_add(slug.capacity())
                    .saturating_add(std::mem::size_of::<PolymarketWsTerminalEvidence>())
                    .saturating_add(evidence.dynamic_owned_bytes())
            });
        self.source_authorities.iter().fold(
            early_claims.saturating_add(
                self.source_authorities
                    .capacity()
                    .saturating_mul(std::mem::size_of::<SourceAuthority>()),
            ),
            |total, authority| {
                total.saturating_add(polymarket_authority_dynamic_owned_bytes(authority))
            },
        )
    }

    pub(super) fn ensure_pending_bounds(&self) -> Result<()> {
        if self.pending.len() > POLYMARKET_PENDING_MAX_MARKETS {
            return Err(invalid_data(format!(
                "Polymarket pending market bound exceeded:count={}:limit={POLYMARKET_PENDING_MAX_MARKETS}",
                self.pending.len()
            ))
            .into());
        }
        if self.source_authorities.len() > POLYMARKET_PENDING_MAX_MARKETS.saturating_add(2) {
            return Err(invalid_data(format!(
                "Polymarket source authority bound exceeded:count={}:limit={}",
                self.source_authorities.len(),
                POLYMARKET_PENDING_MAX_MARKETS.saturating_add(2)
            ))
            .into());
        }
        let owned_bytes = self.pending_owned_bytes();
        if owned_bytes >= POLYMARKET_PENDING_STATE_MAX_BYTES {
            return Err(invalid_data(format!(
                "Polymarket pending state byte bound exceeded:bytes={owned_bytes}:limit={POLYMARKET_PENDING_STATE_MAX_BYTES}"
            ))
            .into());
        }
        Ok(())
    }

    fn validation_now_utc_ms(&self) -> i64 {
        self.actor_utc_now
            .as_ref()
            .map_or(self.actor_now_utc_ms, |now| now())
    }

    pub(super) fn register_source_authority(&mut self, authority: SourceAuthority) -> Result<()> {
        let same_identity = |candidate: &SourceAuthority| {
            same_identity_authority(&candidate.identity, &authority.identity)
        };
        let same_complete_tuple = |candidate: &SourceAuthority| {
            same_identity(candidate)
                && candidate.origin == authority.origin
                && candidate.evidence == authority.evidence
        };
        let duplicate = self.source_authorities.iter().any(same_complete_tuple);
        let mut updated = self.source_authorities.clone();
        if !duplicate {
            updated.retain(|candidate| !same_identity(candidate));
        }
        updated.push(authority);
        if updated.len() > POLYMARKET_PENDING_MAX_MARKETS.saturating_add(2) {
            return Err(invalid_data(format!(
                "Polymarket source authority bound exceeded:count={}:limit={}",
                updated.len(),
                POLYMARKET_PENDING_MAX_MARKETS.saturating_add(2)
            ))
            .into());
        }
        let previous = std::mem::replace(&mut self.source_authorities, updated);
        if let Err(error) = self.ensure_pending_bounds() {
            self.source_authorities = previous;
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn validate_local_transition_authority(
        &self,
        row: &PolymarketMarketMetadataRow,
    ) -> Result<()> {
        let Some(reference) = source_reference_from_metadata_row(row)? else {
            return Ok(());
        };
        validate_polymarket_source_references(&self.source_authorities, &[reference]).map_err(
            |error| {
                invalid_data(format!(
                    "polymarket_metadata_source_reference_integrity:{error}"
                ))
                .into()
            },
        )
    }
}

fn discovered_identity_authority(
    market: &DiscoveredBtcUpdownMarket,
) -> VerifiedPolymarketMarketIdentity {
    VerifiedPolymarketMarketIdentity {
        market: discovered_market_identity(market),
        first_verified_at_utc_ms: market.fixed_identity_verified_at_utc_ms,
    }
}

fn discovered_market_identity(market: &DiscoveredBtcUpdownMarket) -> PolymarketMarketIdentity {
    PolymarketMarketIdentity {
        slug: market.slug.clone(),
        condition_id: market.condition_id.clone(),
        clob_market_id: market.id.clone(),
        ordered_up_down_asset_ids_json: serde_json::to_string(&[
            market.up_asset_id.as_str(),
            market.down_asset_id.as_str(),
        ])
        .unwrap_or_default(),
        market_start_utc_ms: market.start_ts_s.saturating_mul(1_000),
        market_end_utc_ms: market.end_ts_s.saturating_mul(1_000),
    }
}

fn polymarket_authority_matches_discovered(
    authority: &SourceAuthority,
    market: &DiscoveredBtcUpdownMarket,
) -> bool {
    same_identity_authority(&authority.identity, &discovered_identity_authority(market))
}

pub(super) fn polymarket_context_discovery_authorities_complete(
    context: &PolymarketProcessingContext,
) -> bool {
    context
        .current
        .iter()
        .chain(context.next.iter())
        .all(|market| {
            context
                .source_authorities
                .iter()
                .any(|authority| polymarket_authority_matches_discovered(authority, market))
        })
}

fn polymarket_authority_matches_identity(
    authority: &SourceAuthority,
    identity: &PendingPolymarketIdentity,
) -> bool {
    same_identity_authority(&authority.identity, &identity.identity_authority())
}

fn polymarket_authority_dynamic_owned_bytes(authority: &SourceAuthority) -> usize {
    authority
        .identity
        .market
        .slug
        .capacity()
        .saturating_add(authority.identity.market.condition_id.capacity())
        .saturating_add(authority.identity.market.clob_market_id.capacity())
        .saturating_add(
            authority
                .identity
                .market
                .ordered_up_down_asset_ids_json
                .capacity(),
        )
        .saturating_add(authority.origin.capacity())
        .saturating_add(authority.evidence.raw_sha256.capacity())
}

pub(super) fn polymarket_drain_terminal_error(
    terminal: PolymarketDrainTerminal,
    sink: &SourceEventSink,
) -> Box<dyn Error + Send + Sync> {
    match terminal {
        PolymarketDrainTerminal::Sdk(error) => {
            if let SdkStreamError::Lagged(count) = &error {
                sink.source_queue_backpressure
                    .record_lag("polymarket_market", now_ms(), *count);
            }
            if !matches!(
                error,
                SdkStreamError::Connection(_)
                    | SdkStreamError::Subscription(_)
                    | SdkStreamError::TerminalDisconnected
                    | SdkStreamError::Timeout(_)
            ) {
                record_gap(&sink.gaps, format!("polymarket_sdk_stream_fatal:{error}"));
            }
            error.into()
        }
        PolymarketDrainTerminal::Capacity {
            reason,
            recv_time_utc_ms,
            error_kind,
        } => {
            if error_kind == io::ErrorKind::TimedOut {
                sink.source_queue_backpressure.record_timeout();
            }
            let reason = format!(
                "{reason}:decoded_messages_dropped=1:normalized_rows=unknown:recv_time_utc_ms={recv_time_utc_ms}"
            );
            record_gap_at(&sink.gaps, recv_time_utc_ms, reason.clone());
            io::Error::new(error_kind, reason).into()
        }
        PolymarketDrainTerminal::Admission {
            reason,
            recv_time_utc_ms,
        } => {
            record_gap_at(&sink.gaps, recv_time_utc_ms, reason.clone());
            invalid_data(reason).into()
        }
        PolymarketDrainTerminal::QueueClosed => {
            let reason = "polymarket_fast_drain_queue_closed".to_owned();
            record_gap(&sink.gaps, reason.clone());
            io::Error::new(io::ErrorKind::BrokenPipe, reason).into()
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn process_polymarket_budgeted_message(
    budgeted: BudgetedPolymarketMessage,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    _recovery: &mut SourceRecoverySupervisor,
) -> Result<()> {
    let BudgetedPolymarketMessage {
        message,
        recv_time_utc_ms,
        raw_day_ticket,
        _global_reservation: message_global_reservation,
        _decoded_reservation: message_decoded_reservation,
    } = budgeted;
    let _message_global_reservation = message_global_reservation;
    let _message_decoded_reservation = message_decoded_reservation;
    if let WsMessage::MarketResolved(event) = &message {
        return process_polymarket_market_resolved(
            event,
            recv_time_utc_ms,
            context,
            sink,
            collector_identity,
            run_started,
            raw_day_ticket,
        )
        .await;
    }
    let post_end_asset_to_slug = context
        .current
        .as_ref()
        .filter(|current| recv_time_utc_ms >= current.end_ts_s.saturating_mul(1_000))
        .map(|current| {
            let mut routes = context.active_asset_to_slug.clone();
            routes.remove(&current.up_asset_id);
            routes.remove(&current.down_asset_id);
            routes
        });
    let routed_asset_to_slug = post_end_asset_to_slug
        .as_ref()
        .unwrap_or(&context.active_asset_to_slug);
    let Some(discovery) = context.discovery_snapshot() else {
        return Ok(());
    };
    let Some(admitted) =
        target_admitted_polymarket_message(&message, &discovery, routed_asset_to_slug)?
    else {
        return Ok(());
    };
    let source_sequence_epoch_id = context.source_sequence_epoch_id.clone();
    let _ = enqueue_polymarket_orderbook_v3_with_ticket(
        sink,
        run_started,
        recv_time_utc_ms,
        &raw_day_ticket,
        collector_identity,
        &source_sequence_epoch_id,
        || admitted.polymarket_v3_projection(),
    )
    .await?;
    Ok(())
}

pub(super) fn validate_polymarket_market_resolved_identity(
    identity: &PendingPolymarketIdentity,
    event: &MarketResolved,
) -> Result<()> {
    if event.id != identity.clob_market_id {
        return Err(invalid_data(format!(
            "Polymarket market_resolved market id mismatch:slug={}",
            identity.slug
        ))
        .into());
    }
    if event.market.to_string() != identity.condition_id {
        return Err(invalid_data(format!(
            "Polymarket market_resolved condition id mismatch:slug={}",
            identity.slug
        ))
        .into());
    }
    if event
        .slug
        .as_deref()
        .is_some_and(|slug| slug != identity.slug)
    {
        return Err(invalid_data(format!(
            "Polymarket market_resolved slug mismatch:slug={}",
            identity.slug
        ))
        .into());
    }
    let observed_assets = event
        .asset_ids
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    let expected_assets =
        BTreeSet::from([identity.up_asset_id.clone(), identity.down_asset_id.clone()]);
    if event.asset_ids.len() != 2
        || observed_assets.len() != 2
        || observed_assets != expected_assets
    {
        return Err(invalid_data(format!(
            "Polymarket market_resolved asset mapping mismatch:slug={}",
            identity.slug
        ))
        .into());
    }
    if !event.outcomes.is_empty()
        && (event.outcomes.len() != 2
            || event
                .asset_ids
                .iter()
                .zip(&event.outcomes)
                .any(
                    |(asset, outcome)| match outcome.trim().to_ascii_lowercase().as_str() {
                        "up" => asset.to_string() != identity.up_asset_id,
                        "down" => asset.to_string() != identity.down_asset_id,
                        _ => true,
                    },
                ))
    {
        return Err(invalid_data(format!(
            "Polymarket market_resolved outcome mapping mismatch:slug={}",
            identity.slug
        ))
        .into());
    }
    let winning_asset_id = event
        .winning_asset_id
        .as_ref()
        .ok_or_else(|| {
            invalid_data(format!(
                "Polymarket market_resolved winning asset missing:slug={}",
                identity.slug
            ))
        })?
        .to_string();
    let expected_winning_outcome = if winning_asset_id == identity.up_asset_id {
        "up"
    } else if winning_asset_id == identity.down_asset_id {
        "down"
    } else {
        return Err(invalid_data(format!(
            "Polymarket market_resolved winner outside fixed assets:slug={}",
            identity.slug
        ))
        .into());
    };
    let winning_outcome = event.winning_outcome.as_deref().ok_or_else(|| {
        invalid_data(format!(
            "Polymarket market_resolved winning outcome missing:slug={}",
            identity.slug
        ))
    })?;
    if !same_outcome_label(winning_outcome, expected_winning_outcome) {
        return Err(invalid_data(format!(
            "Polymarket market_resolved winning outcome mismatch:slug={}",
            identity.slug
        ))
        .into());
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn target_admitted_market_resolved_v3_projection(
    event: &MarketResolved,
    context: &PolymarketProcessingContext,
) -> Result<Option<PolymarketOrderbookV3Projection>> {
    let Some(slug) = context.matched_resolved_slug(event) else {
        return Ok(None);
    };
    let identity = if let Some(pending) = context.pending.get(&slug) {
        pending.identity.clone()
    } else {
        let market = context
            .current
            .iter()
            .chain(context.next.iter())
            .find(|market| market.slug == slug)
            .ok_or_else(|| invalid_data("matched market_resolved identity disappeared"))?;
        PendingPolymarketIdentity::from_discovered(market)?
    };
    validate_polymarket_market_resolved_identity(&identity, event)?;
    let projection = PolymarketOrderbookV3Projection::from_target_admitted_sdk_message(
        &WsMessage::MarketResolved(event.clone()),
    )?
    .map(|projection| projection.with_slug(Some(identity.slug)))
    .ok_or_else(|| {
        invalid_data("target-admitted market_resolved did not produce a v3 projection")
    })?;
    Ok(Some(projection))
}

pub(super) async fn process_polymarket_market_resolved(
    event: &MarketResolved,
    recv_time_utc_ms: i64,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    raw_day_ticket: RawDayAcceptedTicket,
) -> Result<()> {
    let Some(slug) = context.matched_resolved_slug(event) else {
        return Ok(());
    };
    let identity = if let Some(pending) = context.pending.get(&slug) {
        pending.identity.clone()
    } else {
        let market = context
            .current
            .iter()
            .chain(context.next.iter())
            .find(|market| market.slug == slug)
            .ok_or_else(|| invalid_data("matched market_resolved identity disappeared"))?;
        PendingPolymarketIdentity::from_discovered(market)?
    };
    validate_polymarket_market_resolved_identity(&identity, event)?;
    let validation_completed_at_utc_ms = context.validation_now_utc_ms();
    if validation_completed_at_utc_ms < recv_time_utc_ms {
        return Err(invalid_data(format!(
            "Polymarket market_resolved validation clock regressed:received={recv_time_utc_ms}:validation_completed={validation_completed_at_utc_ms}"
        ))
        .into());
    }
    let winning_asset_id = event
        .winning_asset_id
        .as_ref()
        .expect("validated market_resolved has a winner")
        .to_string();
    let winning_outcome = event
        .winning_outcome
        .as_deref()
        .expect("validated market_resolved has an outcome")
        .to_owned();
    let claim_agrees = |claim: &PolymarketTerminalClaim| -> bool {
        let (claimed_asset, claimed_outcome) = match claim {
            PolymarketTerminalClaim::MarketResolvedWs(evidence) => (
                evidence.winning_asset_id.as_str(),
                evidence.winning_outcome.as_str(),
            ),
            PolymarketTerminalClaim::GammaBySlug(accepted) => (
                accepted.row.winning_asset_id.as_deref().unwrap_or_default(),
                accepted.row.winning_outcome.as_deref().unwrap_or_default(),
            ),
        };
        claimed_asset == winning_asset_id && same_outcome_label(claimed_outcome, &winning_outcome)
    };
    if let Some(pending) = context.pending.get(&slug) {
        if recv_time_utc_ms > pending.budget_horizon_utc_ms() {
            return Ok(());
        }
        if let Some(claim) = &pending.claim
            && !claim_agrees(claim)
        {
            return Err(invalid_data(format!(
                "Polymarket market_resolved contradicts the existing terminal claim:slug={slug}"
            ))
            .into());
        }
    } else if let Some(claim) = context.early_ws_claims.get(&slug)
        && (claim.winning_asset_id != winning_asset_id
            || !same_outcome_label(&claim.winning_outcome, &winning_outcome))
    {
        return Err(invalid_data(format!(
            "Polymarket market_resolved contradicts the existing early terminal claim:slug={slug}"
        ))
        .into());
    }
    let projection_event = event.clone();
    let projection_slug = slug.clone();
    let source_sequence_epoch_id = context.source_sequence_epoch_id.clone();
    let receipt = enqueue_polymarket_orderbook_v3_with_ticket(
        sink,
        run_started,
        recv_time_utc_ms,
        &raw_day_ticket,
        collector_identity,
        &source_sequence_epoch_id,
        move || {
            PolymarketOrderbookV3Projection::from_target_admitted_sdk_message(
                &WsMessage::MarketResolved(projection_event),
            )?
            .map(|projection| projection.with_slug(Some(projection_slug)))
            .ok_or_else(|| {
                invalid_data("validated market_resolved did not produce a v3 projection").into()
            })
        },
    )
    .await?;
    let resolved = receipt.market_resolved.ok_or_else(|| {
        invalid_data("admitted market_resolved is missing typed terminal evidence")
    })?;
    let evidence = PolymarketWsTerminalEvidence {
        source_sequence_epoch_id: receipt.source_sequence_epoch_id,
        collector_admission_sequence: receipt.collector_admission_sequence,
        source_received_at_utc_ms: recv_time_utc_ms,
        validation_completed_at_utc_ms,
        source_payload_json: resolved.source_evidence_payload_json,
        source_payload_sha256: resolved.source_evidence_raw_sha256,
        platform_timestamp_raw: resolved.platform_timestamp_utc_ms.to_string(),
        platform_timestamp_utc_ms: resolved.platform_timestamp_utc_ms,
        winning_asset_id: resolved.winning_asset_id,
        winning_outcome: resolved.winning_outcome,
        raw_day_ticket,
    };
    if let Some(pending) = context.pending.get_mut(&slug) {
        if pending.claim.is_none() {
            pending.claim = Some(PolymarketTerminalClaim::MarketResolvedWs(Box::new(
                evidence,
            )));
        }
        context.ensure_pending_bounds()?;
        return Ok(());
    }
    if context.early_ws_claims.len() >= POLYMARKET_PENDING_MAX_MARKETS
        && !context.early_ws_claims.contains_key(&slug)
    {
        return Err(invalid_data("Polymarket early WS terminal claim bound exceeded").into());
    }
    context.early_ws_claims.entry(slug).or_insert(evidence);
    context.ensure_pending_bounds()?;
    Ok(())
}

fn polymarket_processing_sink_unusable(
    error: &(dyn Error + 'static),
    sink: &SourceEventSink,
) -> bool {
    if sink.tx.is_closed() {
        return true;
    }
    error.downcast_ref::<io::Error>().is_some_and(|error| {
        matches!(
            error.kind(),
            io::ErrorKind::BrokenPipe | io::ErrorKind::TimedOut
        )
    })
}

fn replace_retryable_polymarket_primary(
    primary_error: &mut Box<dyn Error + Send + Sync>,
    later_error: Box<dyn Error + Send + Sync>,
) {
    if market_source_error_is_retryable(primary_error.as_ref())
        && !market_source_error_is_retryable(later_error.as_ref())
    {
        *primary_error = later_error;
    }
}

fn polymarket_context_install_metadata_error(
    subscription_error: &mut Option<Box<dyn Error + Send + Sync>>,
    metadata_error: Box<dyn Error + Send + Sync>,
    gaps: &SharedSourceGaps,
) -> Box<dyn Error + Send + Sync> {
    let Some(mut primary) = subscription_error.take() else {
        return metadata_error;
    };
    let metadata_becomes_primary = market_source_error_is_retryable(primary.as_ref())
        && !market_source_error_is_retryable(metadata_error.as_ref());
    if !metadata_becomes_primary {
        record_gap(
            gaps,
            format!("polymarket_context_install_secondary_metadata_error:{metadata_error}"),
        );
    }
    replace_retryable_polymarket_primary(&mut primary, metadata_error);
    primary
}

fn record_abandoned_polymarket_messages(
    gaps: &SharedSourceGaps,
    abandoned_by_day: BTreeMap<String, (usize, i64, i64)>,
) {
    for (utc_day, (messages, first_recv, last_recv)) in abandoned_by_day {
        record_gap_for_day(
            gaps,
            &utc_day,
            format!(
                "polymarket_accepted_decoded_messages_discarded:messages={messages}:first_recv_time_utc_ms={first_recv}:last_recv_time_utc_ms={last_recv}",
            ),
        );
    }
}

fn note_abandoned_polymarket_message(
    abandoned_by_day: &mut BTreeMap<String, (usize, i64, i64)>,
    recv_time_utc_ms: i64,
) {
    let utc_day = utc_day_string_from_ms(recv_time_utc_ms).unwrap_or_else(|| "unknown".to_owned());
    let entry = abandoned_by_day
        .entry(utc_day)
        .or_insert((0, recv_time_utc_ms, recv_time_utc_ms));
    entry.0 = entry.0.saturating_add(1);
    entry.1 = entry.1.min(recv_time_utc_ms);
    entry.2 = entry.2.max(recv_time_utc_ms);
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn stop_polymarket_after_processing_error(
    first_error: Box<dyn Error + Send + Sync>,
    market_runtime: &mut PolymarketDrainRuntime,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<()> {
    let mut primary_error = first_error;
    let mut can_process = !polymarket_processing_sink_unusable(primary_error.as_ref(), sink);
    let request = match market_runtime.request_fence(true).await {
        Ok(request) => Some(request),
        Err(error) => {
            record_gap(
                &sink.gaps,
                format!("polymarket_processing_error_stop_request_failed:{error}"),
            );
            market_runtime.abort_pump_and_wait().await;
            None
        }
    };
    let expected_stop_id = match request {
        Some(PolymarketFenceRequest::Queued(id)) => Some(id),
        Some(PolymarketFenceRequest::Paused { id, .. }) => Some(id),
        Some(PolymarketFenceRequest::PumpClosed) | None => None,
    };
    let mut abandoned_by_day = BTreeMap::<String, (usize, i64, i64)>::new();
    loop {
        match market_runtime.recv(Duration::from_secs(1)).await {
            Ok(Some(PolymarketDrainEvent::Message(message))) => {
                if can_process {
                    let recv_time_utc_ms = message.recv_time_utc_ms;
                    if let Err(error) = process_polymarket_budgeted_message(
                        message,
                        context,
                        sink,
                        collector_identity,
                        run_started,
                        recovery,
                    )
                    .await
                    {
                        let sink_unusable =
                            polymarket_processing_sink_unusable(error.as_ref(), sink);
                        record_gap_at(
                            &sink.gaps,
                            recv_time_utc_ms,
                            format!("polymarket_processing_error_during_drain:{error}"),
                        );
                        replace_retryable_polymarket_primary(&mut primary_error, error);
                        if sink_unusable {
                            can_process = false;
                        }
                    }
                } else {
                    note_abandoned_polymarket_message(
                        &mut abandoned_by_day,
                        message.recv_time_utc_ms,
                    );
                }
            }
            Ok(Some(PolymarketDrainEvent::Fence {
                id,
                stop_after_fence,
            })) => {
                if stop_after_fence && expected_stop_id.is_none_or(|expected| expected == id) {
                    break;
                }
            }
            Ok(None) => {
                if market_runtime.pump.is_none() {
                    break;
                }
            }
            Err(PolymarketDrainTerminal::QueueClosed) => break,
            Err(terminal) => {
                let cleanup_error = polymarket_drain_terminal_error(terminal, sink);
                if !market_source_error_is_retryable(cleanup_error.as_ref()) {
                    record_gap(
                        &sink.gaps,
                        format!(
                            "polymarket_processing_error_terminal_during_drain:{cleanup_error}"
                        ),
                    );
                }
                replace_retryable_polymarket_primary(&mut primary_error, cleanup_error);
                break;
            }
        }
    }
    if let Err(error) = market_runtime.wait_pump_stopped().await {
        record_gap(
            &sink.gaps,
            format!("polymarket_processing_error_pump_join_failed:{error}"),
        );
        replace_retryable_polymarket_primary(&mut primary_error, error);
    }
    record_abandoned_polymarket_messages(&sink.gaps, abandoned_by_day);
    if !market_source_error_is_retryable(primary_error.as_ref()) {
        primary_error = drain_polymarket_anchors_after_processing_error(
            primary_error,
            context,
            sink,
            collector_identity,
            run_started,
            recovery,
        )
        .await;
    }
    Err(primary_error)
}

async fn drain_polymarket_anchors_after_processing_error(
    mut primary_error: Box<dyn Error + Send + Sync>,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Box<dyn Error + Send + Sync> {
    let mut anchors = Vec::new();
    if let Err(error) = collect_all_market_anchor_items(context, &mut anchors).await {
        record_gap(
            &sink.gaps,
            format!("polymarket_anchor_error_drain_failed:{error}"),
        );
        replace_retryable_polymarket_primary(&mut primary_error, error);
    }
    let observed = context.validation_now_utc_ms();
    process_polymarket_subscription_drain_items(
        anchors,
        Some(primary_error),
        &|| PolymarketTimelineNow {
            utc_ms: observed,
            monotonic: TokioInstant::now(),
        },
        context,
        sink,
        collector_identity,
        run_started,
        recovery,
    )
    .await
    .expect("primary processing error is retained")
}

#[allow(clippy::too_many_arguments)]
async fn drain_polymarket_fence(
    market_runtime: &mut PolymarketDrainRuntime,
    request: PolymarketFenceRequest,
    expected_stop: bool,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<Option<oneshot::Sender<()>>> {
    let (expected_id, resume) = match request {
        PolymarketFenceRequest::Queued(id) => (Some(id), None),
        PolymarketFenceRequest::Paused { id, resume } => (Some(id), Some(resume)),
        PolymarketFenceRequest::PumpClosed => (None, None),
    };
    loop {
        match market_runtime.recv(Duration::from_secs(1)).await {
            Ok(Some(PolymarketDrainEvent::Message(message))) => {
                if let Err(error) = process_polymarket_budgeted_message(
                    message,
                    context,
                    sink,
                    collector_identity,
                    run_started,
                    recovery,
                )
                .await
                {
                    drop(resume);
                    return stop_polymarket_after_processing_error(
                        error,
                        market_runtime,
                        context,
                        sink,
                        collector_identity,
                        run_started,
                        recovery,
                    )
                    .await
                    .map(|()| None);
                }
            }
            Ok(Some(PolymarketDrainEvent::Fence {
                id,
                stop_after_fence,
            })) => {
                if expected_id == Some(id) && stop_after_fence == expected_stop {
                    return Ok(resume);
                }
                return Err(invalid_data(format!(
                    "unexpected Polymarket fast-drain fence:id={id}:stop={stop_after_fence}"
                ))
                .into());
            }
            Ok(None) => {}
            Err(terminal) => return Err(polymarket_drain_terminal_error(terminal, sink)),
        }
    }
}

async fn collect_polymarket_fence_messages(
    market_runtime: &mut PolymarketDrainRuntime,
    request: PolymarketFenceRequest,
    expected_stop: bool,
    sink: &SourceEventSink,
) -> Result<(Option<oneshot::Sender<()>>, Vec<BudgetedPolymarketMessage>)> {
    let (expected_id, resume) = match request {
        PolymarketFenceRequest::Queued(id) => (Some(id), None),
        PolymarketFenceRequest::Paused { id, resume } => (Some(id), Some(resume)),
        PolymarketFenceRequest::PumpClosed => (None, None),
    };
    let mut messages = Vec::new();
    loop {
        match market_runtime.recv(Duration::from_secs(1)).await {
            Ok(Some(PolymarketDrainEvent::Message(message))) => messages.push(message),
            Ok(Some(PolymarketDrainEvent::Fence {
                id,
                stop_after_fence,
            })) => {
                if expected_id == Some(id) && stop_after_fence == expected_stop {
                    return Ok((resume, messages));
                }
                return Err(invalid_data(format!(
                    "unexpected Polymarket arbitration fence:id={id}:stop={stop_after_fence}"
                ))
                .into());
            }
            Ok(None) => {}
            Err(terminal) => return Err(polymarket_drain_terminal_error(terminal, sink)),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn fence_polymarket_processor(
    market_runtime: &mut PolymarketDrainRuntime,
    stop_after_fence: bool,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<()> {
    let request = market_runtime.request_fence(stop_after_fence).await?;
    let resume = drain_polymarket_fence(
        market_runtime,
        request,
        stop_after_fence,
        context,
        sink,
        collector_identity,
        run_started,
        recovery,
    )
    .await?;
    if resume.is_some() {
        return Err(invalid_data("ordinary Polymarket fence returned a pause handle").into());
    }
    if stop_after_fence {
        market_runtime.wait_pump_stopped().await?;
        let mut anchors = Vec::new();
        collect_all_market_anchor_items(context, &mut anchors).await?;
        let observed = context.validation_now_utc_ms();
        process_polymarket_boundary_drain_items(
            anchors,
            None,
            &|| PolymarketTimelineNow {
                utc_ms: observed,
                monotonic: TokioInstant::now(),
            },
            context,
            sink,
            collector_identity,
            run_started,
            recovery,
        )
        .await?;
    }
    Ok(())
}

pub(super) enum PolymarketBoundaryDrainItem {
    Market(Box<BudgetedPolymarketMessage>),
    Gamma(Box<GammaResolutionPollCompletion>),
    Anchor(Box<MarketAnchorCompletion>),
}

impl PolymarketBoundaryDrainItem {
    fn received_at_utc_ms(&self) -> i64 {
        match self {
            Self::Market(message) => message.recv_time_utc_ms,
            Self::Gamma(completion) => completion.completed_at_utc_ms,
            Self::Anchor(completion) => completion.row.timestamp_received,
        }
    }

    fn source_order(&self) -> u8 {
        match self {
            Self::Gamma(_) => 0,
            Self::Market(_) => 1,
            Self::Anchor(_) => 2,
        }
    }
}

fn sort_polymarket_boundary_drain_items(
    items: &mut [PolymarketBoundaryDrainItem],
    day_boundary_utc_ms: Option<i64>,
) {
    items.sort_by_key(|item| {
        (
            day_boundary_utc_ms.map_or(0, |boundary| {
                u8::from(item.received_at_utc_ms() >= boundary)
            }),
            item.received_at_utc_ms(),
            item.source_order(),
        )
    });
}

#[allow(clippy::too_many_arguments)]
async fn process_polymarket_boundary_drain_item<N>(
    item: PolymarketBoundaryDrainItem,
    timeline_now: &N,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<()>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    match item {
        PolymarketBoundaryDrainItem::Anchor(completion) => {
            record_market_anchor_completion(*completion, context, sink, run_started).await
        }
        PolymarketBoundaryDrainItem::Market(message) => {
            process_polymarket_budgeted_message(
                *message,
                context,
                sink,
                collector_identity,
                run_started,
                recovery,
            )
            .await
        }
        PolymarketBoundaryDrainItem::Gamma(completion) => {
            record_gamma_resolution_completion(
                *completion,
                &|| timeline_now().utc_ms,
                context,
                sink,
                collector_identity,
                run_started,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn process_polymarket_boundary_drain_items<N>(
    mut items: Vec<PolymarketBoundaryDrainItem>,
    day_boundary_utc_ms: Option<i64>,
    timeline_now: &N,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<()>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    sort_polymarket_boundary_drain_items(&mut items, day_boundary_utc_ms);
    match process_polymarket_subscription_drain_items(
        items,
        None,
        timeline_now,
        context,
        sink,
        collector_identity,
        run_started,
        recovery,
    )
    .await
    {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_polymarket_subscription_drain_items<N>(
    items: Vec<PolymarketBoundaryDrainItem>,
    mut primary_error: Option<Box<dyn Error + Send + Sync>>,
    timeline_now: &N,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Option<Box<dyn Error + Send + Sync>>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    let mut can_process = primary_error
        .as_ref()
        .is_none_or(|error| !polymarket_processing_sink_unusable(error.as_ref(), sink));
    let mut abandoned_by_day = BTreeMap::<String, (usize, i64, i64)>::new();
    for item in items {
        let received_at_utc_ms = item.received_at_utc_ms();
        if !can_process {
            match item {
                PolymarketBoundaryDrainItem::Anchor(completion) => {
                    record_gap_at(
                        &sink.gaps,
                        received_at_utc_ms,
                        format!(
                            "polymarket_anchor_completion_discarded_after_processing_error:slug={}:ordinal={}",
                            completion.row.identity.slug, completion.row.anchor_query_ordinal,
                        ),
                    );
                }
                PolymarketBoundaryDrainItem::Market(_) => {
                    note_abandoned_polymarket_message(&mut abandoned_by_day, received_at_utc_ms);
                }
                PolymarketBoundaryDrainItem::Gamma(completion) => {
                    record_gap_at(
                        &sink.gaps,
                        received_at_utc_ms,
                        format!(
                            "polymarket_gamma_completion_discarded_after_processing_error:slug={}:ordinal={}",
                            completion.slug, completion.ordinal
                        ),
                    );
                }
            }
            continue;
        }
        if let Err(error) = process_polymarket_boundary_drain_item(
            item,
            timeline_now,
            context,
            sink,
            collector_identity,
            run_started,
            recovery,
        )
        .await
        {
            let sink_unusable = polymarket_processing_sink_unusable(error.as_ref(), sink);
            if let Some(primary) = primary_error.as_mut() {
                record_gap_at(
                    &sink.gaps,
                    received_at_utc_ms,
                    format!("polymarket_processing_error_during_subscription_change_drain:{error}"),
                );
                replace_retryable_polymarket_primary(primary, error);
            } else {
                primary_error = Some(error);
            }
            if sink_unusable {
                can_process = false;
            }
        }
    }
    record_abandoned_polymarket_messages(&sink.gaps, abandoned_by_day);
    primary_error
}

fn preflight_polymarket_early_ws_claims_for_current_boundary(
    now_utc_ms: i64,
    context: &PolymarketProcessingContext,
) -> Result<()> {
    if context.early_ws_claims.is_empty() {
        return Ok(());
    }
    let claim_slugs = context.early_ws_claims.keys().collect::<Vec<_>>();
    let current = context.current.as_ref().ok_or_else(|| {
        invalid_data(format!(
            "accepted early WS claims have no current owner:claims={claim_slugs:?}"
        ))
    })?;
    let current_end_utc_ms = current.end_ts_s.saturating_mul(1_000);
    if now_utc_ms < current_end_utc_ms {
        return Err(invalid_data(format!(
            "accepted early WS claims cannot be materialized before current E:current={}:now={now_utc_ms}:end={current_end_utc_ms}:claims={claim_slugs:?}",
            current.slug
        ))
        .into());
    }
    if context.early_ws_claims.len() != 1 || !context.early_ws_claims.contains_key(&current.slug) {
        return Err(invalid_data(format!(
            "accepted early WS claims include a non-current owner:current={}:claims={claim_slugs:?}",
            current.slug
        ))
        .into());
    }
    Ok(())
}

async fn materialize_polymarket_accepted_early_ws_at_quiesce(
    now_utc_ms: i64,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
) -> Result<bool> {
    preflight_polymarket_early_ws_claims_for_current_boundary(now_utc_ms, context)?;
    let Some(current) = context.current.as_ref() else {
        return Ok(false);
    };
    let Some(parent_ticket) = context
        .early_ws_claims
        .get(&current.slug)
        .map(|claim| claim.raw_day_ticket.clone())
    else {
        return Ok(false);
    };

    let pending_identity = PendingPolymarketIdentity::from_discovered(current)?;
    if context.pending.contains_key(&pending_identity.slug) {
        return Err(invalid_data(format!(
            "duplicate Polymarket pending owner during accepted stop drain:slug={}",
            pending_identity.slug
        ))
        .into());
    }
    let pending_row = polymarket_market_state_transition_row(
        &pending_identity,
        MarketRuntimeRoleTransition::CurrentToResolutionPending,
        now_utc_ms,
        collector_identity,
    )?;
    context.validate_local_transition_authority(&pending_row)?;
    let gate = sink
        .source_queue_backpressure
        .admission_gate()
        .ok_or_else(|| {
            invalid_data("raw-day gate missing while accepted early WS is materialized at quiesce")
        })?;
    let pending_row_ticket = gate
        .accept_derived_polymarket_event_ticket(&parent_ticket, pending_row.timestamp_received)
        .await?;

    let expired_current = context
        .current
        .take()
        .ok_or_else(|| invalid_data("Polymarket current disappeared during accepted stop drain"))?;
    debug_assert_eq!(expired_current.slug, pending_identity.slug);
    let early_claim = context
        .early_ws_claims
        .remove(&pending_identity.slug)
        .ok_or_else(|| {
            invalid_data("accepted early WS claim disappeared during stop materialization")
        })?;
    context.pending.insert(
        pending_identity.slug.clone(),
        PendingPolymarketResolution::new(pending_identity, Some(early_claim)),
    );
    context.next.take();
    context.rebuild_asset_to_slug();
    context.ensure_pending_bounds()?;
    send_polymarket_metadata_row_with_ticket(
        sink,
        run_started,
        AcceptedPolymarketMetadataRow {
            row: pending_row,
            raw_day_ticket: pending_row_ticket,
            ws_claimant_source_sequence_epoch_id: None,
            ws_claimant_collector_admission_sequence: None,
        },
    )
    .await?;
    Ok(true)
}

fn collect_polymarket_gamma_join_item(
    result: std::result::Result<GammaResolutionPollCompletion, tokio::task::JoinError>,
    context: &mut PolymarketProcessingContext,
    items: &mut Vec<PolymarketBoundaryDrainItem>,
    clean_exit: bool,
) -> Result<()> {
    match result {
        Ok(completion) => {
            items.push(PolymarketBoundaryDrainItem::Gamma(Box::new(completion)));
            Ok(())
        }
        Err(error) if error.is_cancelled() && clean_exit => {
            clear_cancelled_gamma_resolution_poll_owner(error.id(), context)
        }
        Err(error) if error.is_cancelled() => {
            record_cancelled_gamma_resolution_poll(error.id(), context)
        }
        Err(error) => {
            Err(invalid_data(format!("Gamma resolution poll task failed:{error}")).into())
        }
    }
}

fn collect_ready_gamma_resolution_items(
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    context: &mut PolymarketProcessingContext,
    items: &mut Vec<PolymarketBoundaryDrainItem>,
    clean_exit: bool,
) -> Result<()> {
    while let Some(result) = resolution_poll_tasks.try_join_next() {
        collect_polymarket_gamma_join_item(result, context, items, clean_exit)?;
    }
    collect_ready_market_anchor_items(context, items)
}

async fn abort_and_collect_gamma_resolution_items(
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    context: &mut PolymarketProcessingContext,
    items: &mut Vec<PolymarketBoundaryDrainItem>,
) -> Result<()> {
    resolution_poll_tasks.abort_all();
    timeout(BUFFER_ACQUIRE_TIMEOUT, async {
        while let Some(result) = resolution_poll_tasks.join_next().await {
            collect_polymarket_gamma_join_item(result, context, items, true)?;
        }
        Ok(())
    })
    .await
    .map_err(|_| invalid_data("Gamma resolution poll abort join timeout"))?
}

#[allow(clippy::too_many_arguments)]
async fn arbitrate_polymarket_ready_items<N>(
    messages: Vec<BudgetedPolymarketMessage>,
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    day_boundary_utc_ms: Option<i64>,
    timeline_now: &N,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<()>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    let mut items = messages
        .into_iter()
        .map(|message| PolymarketBoundaryDrainItem::Market(Box::new(message)))
        .collect::<Vec<_>>();
    collect_ready_gamma_resolution_items(resolution_poll_tasks, context, &mut items, false)?;
    process_polymarket_boundary_drain_items(
        items,
        day_boundary_utc_ms,
        timeline_now,
        context,
        sink,
        collector_identity,
        run_started,
        recovery,
    )
    .await?;
    ensure_polymarket_resolution_task_bound(resolution_poll_tasks, context)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn arbitrate_polymarket_fence_with_ready_gamma<N>(
    market_runtime: &mut PolymarketDrainRuntime,
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    timeline_now: &N,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<()>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    let request = market_runtime.request_fence(false).await?;
    let (resume, messages) =
        collect_polymarket_fence_messages(market_runtime, request, false, sink).await?;
    if resume.is_some() {
        return Err(invalid_data("ordinary Polymarket fence returned a pause handle").into());
    }
    arbitrate_polymarket_ready_items(
        messages,
        resolution_poll_tasks,
        None,
        timeline_now,
        context,
        sink,
        collector_identity,
        run_started,
        recovery,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn quiesce_polymarket_resolution_actor<N>(
    messages: Vec<BudgetedPolymarketMessage>,
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    timeline_now: &N,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<()>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    let mut items = messages
        .into_iter()
        .map(|message| PolymarketBoundaryDrainItem::Market(Box::new(message)))
        .collect::<Vec<_>>();
    collect_ready_gamma_resolution_items(resolution_poll_tasks, context, &mut items, true)?;
    abort_and_collect_gamma_resolution_items(resolution_poll_tasks, context, &mut items).await?;
    collect_all_market_anchor_items(context, &mut items).await?;
    process_polymarket_boundary_drain_items(
        items,
        None,
        timeline_now,
        context,
        sink,
        collector_identity,
        run_started,
        recovery,
    )
    .await?;
    let now = timeline_now();
    context.actor_now_utc_ms = now.utc_ms;
    materialize_polymarket_accepted_early_ws_at_quiesce(
        now.utc_ms,
        context,
        sink,
        collector_identity,
        run_started,
    )
    .await?;
    ensure_polymarket_resolution_task_bound(resolution_poll_tasks, context)?;
    finalize_polymarket_outer_ready_pending(
        timeline_now,
        context,
        sink,
        collector_identity,
        run_started,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn quiesce_connected_polymarket_source<N>(
    market_runtime: &mut PolymarketDrainRuntime,
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    timeline_now: &N,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<()>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    let request = market_runtime.request_fence(true).await?;
    let (resume, messages) =
        collect_polymarket_fence_messages(market_runtime, request, true, sink).await?;
    if resume.is_some() {
        return Err(invalid_data("stopping Polymarket fence returned a pause handle").into());
    }
    market_runtime.wait_pump_stopped().await?;
    quiesce_polymarket_resolution_actor(
        messages,
        resolution_poll_tasks,
        timeline_now,
        context,
        sink,
        collector_identity,
        run_started,
        recovery,
    )
    .await
}

async fn pause_polymarket_processor_for_day_boundary(
    market_runtime: &mut PolymarketDrainRuntime,
    sink: &SourceEventSink,
) -> Result<(oneshot::Sender<()>, Vec<BudgetedPolymarketMessage>)> {
    let request = market_runtime.request_paused_fence().await?;
    let (expected_id, resume) = match request {
        PolymarketFenceRequest::Paused { id, resume } => (id, resume),
        PolymarketFenceRequest::PumpClosed => {
            return Err(invalid_data("Polymarket pump closed before day-boundary fence").into());
        }
        PolymarketFenceRequest::Queued(_) => {
            return Err(
                invalid_data("Polymarket day-boundary fence did not pause the pump").into(),
            );
        }
    };
    let mut messages = Vec::new();
    loop {
        match market_runtime.recv(Duration::from_secs(1)).await {
            Ok(Some(PolymarketDrainEvent::Message(message))) => messages.push(message),
            Ok(Some(PolymarketDrainEvent::Fence {
                id,
                stop_after_fence,
            })) if id == expected_id && !stop_after_fence => {
                return Ok((resume, messages));
            }
            Ok(Some(PolymarketDrainEvent::Fence {
                id,
                stop_after_fence,
            })) => {
                return Err(invalid_data(format!(
                    "unexpected Polymarket day-boundary fence:id={id}:stop={stop_after_fence}"
                ))
                .into());
            }
            Ok(None) => {}
            Err(terminal) => return Err(polymarket_drain_terminal_error(terminal, sink)),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_polymarket_source_control<N>(
    control: PolymarketSourceControl,
    market_runtime: &mut PolymarketDrainRuntime,
    context: &mut PolymarketProcessingContext,
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    timeline_now: &N,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<bool>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    let PolymarketSourceControl::Fence {
        stop_after_fence,
        day_boundary_utc_ms,
        resume_after_barrier,
        reply,
    } = control;
    if stop_after_fence {
        if day_boundary_utc_ms.is_some() {
            let error = invalid_data("Polymarket stop fence carried a UTC day boundary");
            let _ = reply.send(Err(error.to_string()));
            return Err(error.into());
        }
        let result = quiesce_connected_polymarket_source(
            market_runtime,
            resolution_poll_tasks,
            timeline_now,
            context,
            sink,
            collector_identity,
            run_started,
            recovery,
        )
        .await;
        match result {
            Ok(()) => {
                let _ = reply.send(Ok(()));
            }
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
                return Err(error);
            }
        }
        return Ok(true);
    }

    if let Some(resume_after_barrier) = resume_after_barrier {
        let Some(day_boundary_utc_ms) = day_boundary_utc_ms else {
            let error = invalid_data("Polymarket day fence is missing its UTC boundary");
            let _ = reply.send(Err(error.to_string()));
            return Err(error.into());
        };
        let (paused, queued_messages) =
            match pause_polymarket_processor_for_day_boundary(market_runtime, sink).await {
                Ok(paused) => paused,
                Err(error) => {
                    let _ = reply.send(Err(error.to_string()));
                    return Err(error);
                }
            };
        abort_ws_claimed_gamma_resolution_polls(context);
        let finalize_result: Result<()> = async {
            let mut drained = queued_messages
                .into_iter()
                .map(|message| PolymarketBoundaryDrainItem::Market(Box::new(message)))
                .collect::<Vec<_>>();
            while let Some(result) = resolution_poll_tasks.join_next().await {
                match result {
                    Ok(completion) => {
                        drained.push(PolymarketBoundaryDrainItem::Gamma(Box::new(completion)));
                    }
                    Err(error) if error.is_cancelled() => {
                        record_cancelled_gamma_resolution_poll(error.id(), context)?;
                    }
                    Err(error) => {
                        return Err(invalid_data(format!(
                            "Gamma resolution poll task failed:{error}"
                        ))
                        .into());
                    }
                }
            }
            collect_all_market_anchor_items(context, &mut drained).await?;
            process_polymarket_boundary_drain_items(
                drained,
                Some(day_boundary_utc_ms),
                timeline_now,
                context,
                sink,
                collector_identity,
                run_started,
                recovery,
            )
            .await?;
            let boundary_now = timeline_now();
            context.actor_now_utc_ms = boundary_now.utc_ms;
            if boundary_now.utc_ms < day_boundary_utc_ms {
                return Err(invalid_data(format!(
                    "Polymarket day fence preceded its UTC boundary:now={}:boundary={day_boundary_utc_ms}",
                    boundary_now.utc_ms
                ))
                .into());
            }
            preflight_polymarket_early_ws_claims_for_current_boundary(
                boundary_now.utc_ms,
                context,
            )?;
            advance_polymarket_boundaries(
                boundary_now.utc_ms,
                context,
                sink,
                collector_identity,
                run_started,
            )
            .await?;
            ensure_polymarket_resolution_task_bound(resolution_poll_tasks, context)?;
            let fatal_integrity =
                finalize_polymarket_ready_pending_while_paused(
                    timeline_now,
                    context,
                    sink,
                    collector_identity,
                    run_started,
                )
                .await?;
            if let Some(reason) = fatal_integrity {
                return Err(invalid_data(reason).into());
            }
            Ok(())
        }
        .await;
        if let Err(error) = finalize_result {
            let _ = reply.send(Err(error.to_string()));
            return Err(error);
        }
        let _ = reply.send(Ok(()));
        if resume_after_barrier.await.is_err() {
            return Err(invalid_data("Polymarket day-barrier resume dropped").into());
        }
        paused
            .send(())
            .map_err(|_| invalid_data("Polymarket paused fence resume receiver dropped"))?;
        apply_polymarket_subscription_while_draining(
            market_runtime,
            context.desired_assets()?,
            context,
            resolution_poll_tasks,
            sink,
            collector_identity,
            run_started,
            recovery,
        )
        .await?;
        return Ok(false);
    }

    if day_boundary_utc_ms.is_some() {
        let error = invalid_data("ordinary Polymarket fence carried a UTC day boundary");
        let _ = reply.send(Err(error.to_string()));
        return Err(error.into());
    }
    let result = arbitrate_polymarket_fence_with_ready_gamma(
        market_runtime,
        resolution_poll_tasks,
        timeline_now,
        context,
        sink,
        collector_identity,
        run_started,
        recovery,
    )
    .await;
    match result {
        Ok(()) => {
            let _ = reply.send(Ok(()));
        }
        Err(error) => {
            let _ = reply.send(Err(error.to_string()));
            return Err(error);
        }
    }
    Ok(false)
}

enum PolymarketSubscriptionProgress {
    Applied,
    SdkError(SdkStreamError),
    ProcessingError(Box<dyn Error + Send + Sync>),
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn apply_polymarket_subscription_while_draining(
    market_runtime: &mut PolymarketDrainRuntime,
    target_assets: BTreeSet<U256>,
    context: &mut PolymarketProcessingContext,
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<()> {
    let progress = {
        let sdk = &mut market_runtime.sdk;
        let events = &mut market_runtime.events;
        let operation = sdk.replace_and_wait(target_assets, Duration::from_secs(20));
        tokio::pin!(operation);
        loop {
            let mut operation_result = None;
            let mut drained = Vec::<PolymarketBoundaryDrainItem>::new();
            let mut processing_error = None;
            let mut fifo_terminal_error = None;
            tokio::select! {
                biased;
                result = &mut operation => {
                    operation_result = Some(result);
                }
                result = resolution_poll_tasks.join_next(),
                    if !resolution_poll_tasks.is_empty() =>
                {
                    if let Some(result) = result
                        && let Err(error) = collect_polymarket_gamma_join_item(
                            result,
                            context,
                            &mut drained,
                            false,
                        )
                    {
                        processing_error = Some(error);
                    }
                }
                item = events.recv() => {
                    match item {
                        Some(PolymarketPumpItem::Message(message)) => {
                            drained.push(PolymarketBoundaryDrainItem::Market(Box::new(message)));
                        }
                        Some(PolymarketPumpItem::Terminal(terminal)) => {
                            fifo_terminal_error =
                                Some(polymarket_drain_terminal_error(terminal, sink));
                        }
                        Some(PolymarketPumpItem::Fence { id, stop_after_fence }) => {
                            fifo_terminal_error = Some(
                                invalid_data(format!(
                                    "unexpected Polymarket fence during subscription change:id={id}:stop={stop_after_fence}"
                                ))
                                .into(),
                            );
                        }
                        None => {
                            fifo_terminal_error = Some(
                                polymarket_drain_terminal_error(
                                    PolymarketDrainTerminal::QueueClosed,
                                    sink,
                                ),
                            );
                        }
                    }
                }
            }

            while processing_error.is_none() && fifo_terminal_error.is_none() {
                match events.try_recv() {
                    Ok(PolymarketPumpItem::Message(message)) => {
                        drained.push(PolymarketBoundaryDrainItem::Market(Box::new(message)));
                    }
                    Ok(PolymarketPumpItem::Terminal(terminal)) => {
                        fifo_terminal_error = Some(polymarket_drain_terminal_error(terminal, sink));
                    }
                    Ok(PolymarketPumpItem::Fence {
                        id,
                        stop_after_fence,
                    }) => {
                        fifo_terminal_error = Some(
                            invalid_data(format!(
                                "unexpected Polymarket fence during subscription change:id={id}:stop={stop_after_fence}"
                            ))
                            .into(),
                        );
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        fifo_terminal_error = Some(polymarket_drain_terminal_error(
                            PolymarketDrainTerminal::QueueClosed,
                            sink,
                        ));
                    }
                }
            }
            if processing_error.is_none()
                && fifo_terminal_error.is_none()
                && let Err(error) = collect_ready_gamma_resolution_items(
                    resolution_poll_tasks,
                    context,
                    &mut drained,
                    false,
                )
            {
                processing_error = Some(error);
            }
            if !drained.is_empty() {
                let validation_now_utc_ms = context.validation_now_utc_ms();
                processing_error = process_polymarket_subscription_drain_items(
                    drained,
                    processing_error,
                    &|| PolymarketTimelineNow {
                        utc_ms: validation_now_utc_ms,
                        monotonic: TokioInstant::now(),
                    },
                    context,
                    sink,
                    collector_identity,
                    run_started,
                    recovery,
                )
                .await;
            }
            if let Some(terminal_error) = fifo_terminal_error {
                if let Some(primary) = processing_error.as_mut() {
                    record_gap(
                        &sink.gaps,
                        format!(
                            "polymarket_processing_error_terminal_during_subscription_change:{terminal_error}"
                        ),
                    );
                    replace_retryable_polymarket_primary(primary, terminal_error);
                } else {
                    processing_error = Some(terminal_error);
                }
            }
            if let Some(error) = processing_error {
                break PolymarketSubscriptionProgress::ProcessingError(error);
            }
            abort_ws_claimed_gamma_resolution_polls(context);
            if let Some(result) = operation_result {
                break match result {
                    Ok(()) => PolymarketSubscriptionProgress::Applied,
                    Err(error) => PolymarketSubscriptionProgress::SdkError(error),
                };
            }
        }
    };
    match progress {
        PolymarketSubscriptionProgress::Applied => Ok(()),
        PolymarketSubscriptionProgress::ProcessingError(error) => {
            stop_polymarket_after_processing_error(
                error,
                market_runtime,
                context,
                sink,
                collector_identity,
                run_started,
                recovery,
            )
            .await
        }
        PolymarketSubscriptionProgress::SdkError(error) => {
            stop_polymarket_after_processing_error(
                error.into(),
                market_runtime,
                context,
                sink,
                collector_identity,
                run_started,
                recovery,
            )
            .await
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PolymarketCurrentMetadataRoute {
    ExistingCurrent,
    CurrentSnapshot,
    CurrentCandidate,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn install_polymarket_context(
    discovery: BtcUpdownDiscovery,
    evaluation_ms: i64,
    current_metadata_route: PolymarketCurrentMetadataRoute,
    commit_metadata_after_subscription_error: bool,
    market_runtime: &mut PolymarketDrainRuntime,
    context: &mut PolymarketProcessingContext,
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<()> {
    let previous_current = context.current.clone();
    let previous_next = context.next.clone();
    let previous_early_ws_claims = context.early_ws_claims.clone();
    let previous_source_authorities = context.source_authorities.clone();
    context.replace_discovery(discovery);
    let target_assets = match context.desired_assets() {
        Ok(target_assets) => target_assets,
        Err(error) => {
            context.current = previous_current;
            context.next = previous_next;
            context.early_ws_claims = previous_early_ws_claims;
            context.source_authorities = previous_source_authorities;
            context.rebuild_asset_to_slug();
            return Err(error);
        }
    };
    let mut subscription_error = apply_polymarket_subscription_while_draining(
        market_runtime,
        target_assets,
        context,
        resolution_poll_tasks,
        sink,
        collector_identity,
        run_started,
        recovery,
    )
    .await
    .err();
    if subscription_error.is_some() && !commit_metadata_after_subscription_error {
        return Err(subscription_error.take().unwrap());
    }
    let current_authority = match current_metadata_route {
        PolymarketCurrentMetadataRoute::ExistingCurrent => None,
        PolymarketCurrentMetadataRoute::CurrentSnapshot => Some(
            async {
                let current = context.current.as_ref().ok_or_else(|| {
                    invalid_data("installed Polymarket context is missing current")
                })?;
                send_polymarket_metadata_event(
                    sink,
                    run_started,
                    current,
                    "current",
                    collector_identity,
                )
                .await
            }
            .await,
        ),
        PolymarketCurrentMetadataRoute::CurrentCandidate => Some(
            async {
                let current = context.current.as_ref().ok_or_else(|| {
                    invalid_data("installed Polymarket context is missing current")
                })?;
                let row =
                    polymarket_candidate_to_active_row(current, evaluation_ms, collector_identity)?;
                send_polymarket_metadata_source_authority_row(sink, run_started, row).await
            }
            .await,
        ),
    };
    if let Some(current_authority) = current_authority {
        let authority = match current_authority {
            Ok(authority) => authority,
            Err(error) => {
                return Err(polymarket_context_install_metadata_error(
                    &mut subscription_error,
                    error,
                    &sink.gaps,
                ));
            }
        };
        if let Err(error) = context.register_source_authority(authority) {
            return Err(polymarket_context_install_metadata_error(
                &mut subscription_error,
                error,
                &sink.gaps,
            ));
        }
    }
    if let Some(next) = &context.next {
        let authority = match send_polymarket_metadata_event(
            sink,
            run_started,
            next,
            "next",
            collector_identity,
        )
        .await
        {
            Ok(authority) => authority,
            Err(error) => {
                return Err(polymarket_context_install_metadata_error(
                    &mut subscription_error,
                    error,
                    &sink.gaps,
                ));
            }
        };
        if let Err(error) = context.register_source_authority(authority) {
            return Err(polymarket_context_install_metadata_error(
                &mut subscription_error,
                error,
                &sink.gaps,
            ));
        }
    }
    if let Some(error) = subscription_error {
        return Err(error);
    }
    recovery.record_business_recovery();
    sink.source_queue_backpressure
        .set_recovery_active("polymarket_market", false);
    Ok(())
}

async fn admit_missing_polymarket_context_metadata(
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    source_received_before_utc_ms: Option<i64>,
) -> Result<()> {
    let missing = context
        .current
        .iter()
        .map(|market| (market, "current"))
        .chain(context.next.iter().map(|market| (market, "next")))
        .filter(|(market, _)| {
            !context
                .source_authorities
                .iter()
                .any(|authority| polymarket_authority_matches_discovered(authority, market))
        })
        .map(|(market, role)| (market.clone(), role))
        .collect::<Vec<_>>();
    for (market, role) in missing {
        if source_received_before_utc_ms.is_some_and(|boundary| {
            market
                .metadata_received_utc_ms
                .is_some_and(|received| received >= boundary)
        }) {
            continue;
        }
        let authority =
            send_polymarket_metadata_event(sink, run_started, &market, role, collector_identity)
                .await?;
        context.register_source_authority(authority)?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PolymarketContextInstallOutcome {
    Applied,
    Expired,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn install_polymarket_context_if_current(
    discovery: BtcUpdownDiscovery,
    evaluation_ms: i64,
    current_metadata_route: PolymarketCurrentMetadataRoute,
    commit_metadata_after_subscription_error: bool,
    market_runtime: &mut PolymarketDrainRuntime,
    context: &mut PolymarketProcessingContext,
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<PolymarketContextInstallOutcome> {
    if !gamma_discovery_is_current(&discovery, evaluation_ms) {
        return Ok(PolymarketContextInstallOutcome::Expired);
    }
    install_polymarket_context(
        discovery,
        evaluation_ms,
        current_metadata_route,
        commit_metadata_after_subscription_error,
        market_runtime,
        context,
        resolution_poll_tasks,
        sink,
        collector_identity,
        run_started,
        recovery,
    )
    .await?;
    Ok(PolymarketContextInstallOutcome::Applied)
}

pub(super) fn validate_polymarket_context_switch(
    current: &BtcUpdownDiscovery,
    replacement: &BtcUpdownDiscovery,
) -> Result<()> {
    if current.current.slug == replacement.current.slug {
        return Ok(());
    }
    let Some(prefetched) = current
        .next
        .as_ref()
        .filter(|market| market.slug == replacement.current.slug)
    else {
        return Ok(());
    };
    if !same_market_identity(
        &discovered_market_identity(prefetched),
        &discovered_market_identity(&replacement.current),
    ) {
        return Err(invalid_data(format!(
            "Polymarket prefetched market identity changed before promotion:slug={}",
            prefetched.slug
        ))
        .into());
    }
    Ok(())
}

pub(super) fn validate_polymarket_discovery_identity_continuity(
    previous: &BtcUpdownDiscovery,
    current: &BtcUpdownDiscovery,
) -> Result<()> {
    for previous_market in std::iter::once(&previous.current).chain(previous.next.as_ref()) {
        let Some(current_market) = std::iter::once(&current.current)
            .chain(current.next.as_ref())
            .find(|market| market.slug == previous_market.slug)
        else {
            continue;
        };
        if !same_market_identity(
            &discovered_market_identity(previous_market),
            &discovered_market_identity(current_market),
        ) {
            return Err(invalid_data(format!(
                "Polymarket market identity changed across transport recovery:slug={}",
                previous_market.slug
            ))
            .into());
        }
    }
    Ok(())
}

pub(super) fn preserve_polymarket_fixed_identity_authority(
    previous: &BtcUpdownDiscovery,
    replacement: &mut BtcUpdownDiscovery,
) -> Result<()> {
    validate_polymarket_discovery_identity_continuity(previous, replacement)?;
    for replacement_market in
        std::iter::once(&mut replacement.current).chain(replacement.next.as_mut())
    {
        let Some(previous_market) = std::iter::once(&previous.current)
            .chain(previous.next.as_ref())
            .find(|market| market.slug == replacement_market.slug)
        else {
            continue;
        };
        replacement_market.fixed_identity_verified_at_utc_ms =
            previous_market.fixed_identity_verified_at_utc_ms;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct RecoveredPolymarketFixedIdentityMatch {
    pub(super) current: bool,
    pub(super) next: bool,
    pub(super) fresh_current: bool,
}

impl RecoveredPolymarketFixedIdentityMatch {
    pub(super) fn consume(self, recovered: &mut RecoveredPolymarketFixedIdentities) {
        if self.current {
            recovered.current = None;
        }
        if self.next {
            recovered.next = None;
        }
    }
}

pub(super) fn preserve_recovered_polymarket_fixed_identity_authority(
    recovered: &RecoveredPolymarketFixedIdentities,
    discovery: &mut BtcUpdownDiscovery,
) -> Result<RecoveredPolymarketFixedIdentityMatch> {
    let mut matched = RecoveredPolymarketFixedIdentityMatch::default();
    for (fresh_current, market) in std::iter::once((true, &mut discovery.current))
        .chain(discovery.next.as_mut().map(|market| (false, market)))
    {
        let current_matches = recovered
            .current
            .as_ref()
            .is_some_and(|identity| identity.slug == market.slug);
        let next_matches = recovered
            .next
            .as_ref()
            .is_some_and(|identity| identity.slug == market.slug);
        if current_matches && next_matches {
            return Err(invalid_data(format!(
                "operator_intervention_required:Polymarket restart has multiple fixed identity candidates:slug={}",
                market.slug
            ))
            .into());
        }
        let (identity, matched_current) = if current_matches {
            (recovered.current.as_ref(), true)
        } else if next_matches {
            (recovered.next.as_ref(), false)
        } else {
            (None, false)
        };
        let Some(identity) = identity else {
            continue;
        };
        if !recovered_polymarket_fixed_identity_matches(identity, market) {
            return Err(invalid_data(format!(
                "operator_intervention_required:Polymarket restart fixed identity conflict:slug={}",
                market.slug
            ))
            .into());
        }
        market.fixed_identity_verified_at_utc_ms = identity.fixed_identity_verified_at_utc_ms;
        if matched_current {
            matched.current = true;
        } else {
            matched.next = true;
        }
        if fresh_current {
            matched.fresh_current = true;
        }
    }
    Ok(matched)
}

pub(super) fn recovered_polymarket_current_metadata_route(
    recovered_match: RecoveredPolymarketFixedIdentityMatch,
) -> PolymarketCurrentMetadataRoute {
    if recovered_match.fresh_current {
        PolymarketCurrentMetadataRoute::CurrentSnapshot
    } else {
        PolymarketCurrentMetadataRoute::CurrentCandidate
    }
}

pub(super) fn recovered_polymarket_fixed_identity_matches(
    recovered: &RecoveredPolymarketFixedIdentity,
    fresh: &DiscoveredBtcUpdownMarket,
) -> bool {
    same_market_identity(
        &PolymarketMarketIdentity {
            slug: recovered.slug.clone(),
            condition_id: recovered.condition_id.clone(),
            clob_market_id: recovered.clob_market_id.clone(),
            ordered_up_down_asset_ids_json: serde_json::to_string(&[
                recovered.up_asset_id.as_str(),
                recovered.down_asset_id.as_str(),
            ])
            .unwrap_or_default(),
            market_start_utc_ms: recovered.market_start_utc_ms,
            market_end_utc_ms: recovered.market_end_utc_ms,
        },
        &discovered_market_identity(fresh),
    )
}

pub(super) fn polymarket_restart_discovery_integrity_error(
    recovered: &RecoveredPolymarketFixedIdentities,
    error: &PolymarketDiscoveryError,
) -> io::Error {
    let expected_slug = error.fixed_identity_expected_slug();
    let has_recovered_authority = expected_slug.is_some_and(|expected_slug| {
        recovered
            .current
            .as_ref()
            .is_some_and(|identity| identity.slug == expected_slug)
            || recovered
                .next
                .as_ref()
                .is_some_and(|identity| identity.slug == expected_slug)
    });
    if has_recovered_authority {
        let expected_slug = expected_slug.unwrap_or_default();
        invalid_data(format!(
            "operator_intervention_required:Polymarket restart discovery identity conflict:slug={expected_slug}:error={}",
            error.detail()
        ))
    } else {
        invalid_data(error.detail())
    }
}

fn stamp_polymarket_discovery_validation(
    market: &mut DiscoveredBtcUpdownMarket,
    validation_completed_at_utc_ms: i64,
) -> Result<()> {
    if market
        .metadata_received_utc_ms
        .is_some_and(|received| received > validation_completed_at_utc_ms)
    {
        return Err(invalid_data(format!(
            "Polymarket discovery validation clock preceded source receive:slug={}:received={}:validation_completed={validation_completed_at_utc_ms}",
            market.slug,
            market.metadata_received_utc_ms.unwrap_or_default()
        ))
        .into());
    }
    market.fixed_identity_verified_at_utc_ms = validation_completed_at_utc_ms;
    market.metadata_validation_completed_at_utc_ms = validation_completed_at_utc_ms;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn switch_polymarket_context<N>(
    mut discovery: BtcUpdownDiscovery,
    timeline_now: &N,
    current_metadata_route: PolymarketCurrentMetadataRoute,
    market_runtime: &mut PolymarketDrainRuntime,
    context: &mut PolymarketProcessingContext,
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<PolymarketContextInstallOutcome>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    if let Some(current) = context.discovery_snapshot()
        && let Err(error) = validate_polymarket_context_switch(&current, &discovery)
            .and_then(|()| preserve_polymarket_fixed_identity_authority(&current, &mut discovery))
    {
        record_gap(
            &sink.gaps,
            format!("polymarket_rollover_identity_integrity:{error}"),
        );
        return Err(error);
    }
    arbitrate_polymarket_fence_with_ready_gamma(
        market_runtime,
        resolution_poll_tasks,
        timeline_now,
        context,
        sink,
        collector_identity,
        run_started,
        recovery,
    )
    .await?;
    install_polymarket_context_if_current(
        discovery,
        timeline_now().utc_ms,
        current_metadata_route,
        true,
        market_runtime,
        context,
        resolution_poll_tasks,
        sink,
        collector_identity,
        run_started,
        recovery,
    )
    .await
}

fn schedule_polymarket_discovery_retry<N>(
    recovery: &mut SourceRecoverySupervisor,
    sink: &SourceEventSink,
    timeline_now: &N,
) -> TokioInstant
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    let wait = recovery.next_wait();
    sink.source_queue_backpressure
        .record_reconnect("polymarket_market", timeline_now().utc_ms);
    sink.source_queue_backpressure
        .set_recovery_active("polymarket_market", true);
    timeline_now().monotonic + wait.delay
}

pub(super) async fn advance_polymarket_boundaries(
    now_utc_ms: i64,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
) -> Result<bool> {
    let Some(current) = context.current.as_ref() else {
        return Ok(false);
    };
    if now_utc_ms < current.end_ts_s.saturating_mul(1_000) {
        return Ok(false);
    }

    let pending_identity = PendingPolymarketIdentity::from_discovered(current)?;
    if context.pending.contains_key(&pending_identity.slug) {
        return Err(invalid_data(format!(
            "duplicate Polymarket pending owner:slug={}",
            pending_identity.slug
        ))
        .into());
    }
    let pending_row = polymarket_market_state_transition_row(
        &pending_identity,
        MarketRuntimeRoleTransition::CurrentToResolutionPending,
        now_utc_ms,
        collector_identity,
    )?;
    let promoted = context
        .next
        .as_ref()
        .filter(|next| {
            let start = next.start_ts_s.saturating_mul(1_000);
            let end = next.end_ts_s.saturating_mul(1_000);
            start <= now_utc_ms && now_utc_ms < end
        })
        .cloned();
    let promotion_row = promoted
        .as_ref()
        .map(|market| {
            let identity = PendingPolymarketIdentity::from_discovered(market)?;
            polymarket_market_state_transition_row(
                &identity,
                MarketRuntimeRoleTransition::NextToCurrent,
                now_utc_ms,
                collector_identity,
            )
        })
        .transpose()?;
    context.validate_local_transition_authority(&pending_row)?;
    if let Some(row) = &promotion_row {
        context.validate_local_transition_authority(row)?;
    }

    let expired_current = context
        .current
        .take()
        .ok_or_else(|| invalid_data("Polymarket current disappeared at boundary"))?;
    debug_assert_eq!(expired_current.slug, pending_identity.slug);
    let early_claim = context.early_ws_claims.remove(&pending_identity.slug);
    context.pending.insert(
        pending_identity.slug.clone(),
        PendingPolymarketResolution::new(pending_identity, early_claim),
    );
    context.next.take();
    context.current = promoted;
    context.rebuild_asset_to_slug();
    context.ensure_pending_bounds()?;

    send_polymarket_metadata_row(sink, run_started, pending_row).await?;
    if let Some(row) = promotion_row {
        send_polymarket_metadata_row(sink, run_started, row).await?;
    }
    Ok(true)
}

pub(super) async fn record_gamma_resolution_completion<N>(
    completion: GammaResolutionPollCompletion,
    validation_now_utc_ms: &N,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
) -> Result<()>
where
    N: Fn() -> i64 + Send + Sync,
{
    let raw_day_ticket = match &completion.raw_day_ticket {
        RawDayTicketAcceptance::Accepted(ticket) => ticket.clone(),
        RawDayTicketAcceptance::Cutoff => {
            let pending = context.pending.get_mut(&completion.slug).ok_or_else(|| {
                invalid_data(format!(
                    "cutoff Gamma completion has no pending owner:slug={}:ordinal={}",
                    completion.slug, completion.ordinal
                ))
            })?;
            if pending.in_flight_ordinal != Some(completion.ordinal) {
                return Err(invalid_data(format!(
                    "cutoff Gamma completion ownership mismatch:slug={}:expected={:?}:actual={}",
                    completion.slug, pending.in_flight_ordinal, completion.ordinal
                ))
                .into());
            }
            pending.in_flight_ordinal = None;
            pending.poll_abort_handle = None;
            return Ok(());
        }
        RawDayTicketAcceptance::Failed(error) => {
            return Err(invalid_data(format!(
                "Gamma completion raw-day ticket failed:slug={}:ordinal={}:error={error}",
                completion.slug, completion.ordinal
            ))
            .into());
        }
    };
    if completion.fetch_start_utc_ms < completion.scheduled_at_utc_ms
        || completion.completed_at_utc_ms < completion.fetch_start_utc_ms
    {
        return Err(invalid_data(format!(
            "Gamma resolution request clock regressed without decoded evidence:slug={}:ordinal={}:scheduled={}:fetch_start={}:completed={}",
            completion.slug,
            completion.ordinal,
            completion.scheduled_at_utc_ms,
            completion.fetch_start_utc_ms,
            completion.completed_at_utc_ms
        ))
        .into());
    }
    let pending = context.pending.get(&completion.slug).ok_or_else(|| {
        invalid_data(format!(
            "Gamma completion has no pending owner:slug={}:ordinal={}",
            completion.slug, completion.ordinal
        ))
    })?;
    let ws_claimed = matches!(
        pending.claim,
        Some(PolymarketTerminalClaim::MarketResolvedWs(_))
    );
    let row = polymarket_resolution_attempt_row(
        pending,
        &completion,
        validation_now_utc_ms,
        ws_claimed,
        collector_identity,
    )?;
    let result = row
        .resolution_poll_result
        .ok_or_else(|| invalid_data("Gamma completion row is missing result"))?;
    let accepted_row = AcceptedPolymarketMetadataRow {
        row,
        raw_day_ticket: raw_day_ticket.clone(),
        ws_claimant_source_sequence_epoch_id: None,
        ws_claimant_collector_admission_sequence: None,
    };

    let mut emit_now = None;
    let mut fail_after_emit = false;
    {
        let pending = context
            .pending
            .get_mut(&completion.slug)
            .ok_or_else(|| invalid_data("Gamma pending owner disappeared during completion"))?;
        pending.in_flight_ordinal = None;
        pending.poll_abort_handle = None;
        pending.attempts_recorded = pending.attempts_recorded.saturating_add(1);
        pending.next_attempt_ordinal = completion.ordinal.saturating_add(1);
        match result {
            ResolutionPollResult::Terminal => {
                if pending.claim.is_none() {
                    pending.claim =
                        Some(PolymarketTerminalClaim::GammaBySlug(Box::new(accepted_row)));
                } else {
                    return Err(invalid_data(format!(
                        "Gamma terminal attempted a second claim:slug={}",
                        completion.slug
                    ))
                    .into());
                }
            }
            ResolutionPollResult::ValidNonterminal => {
                pending.valid_response_count = pending.valid_response_count.saturating_add(1);
                pending.last_valid_metadata_received_at_utc_ms = Some(
                    pending
                        .last_valid_metadata_received_at_utc_ms
                        .map_or(completion.completed_at_utc_ms, |previous| {
                            previous.max(completion.completed_at_utc_ms)
                        }),
                );
                emit_now = Some(accepted_row);
            }
            ResolutionPollResult::TransportUnavailable => {
                pending.transport_unavailable_count =
                    pending.transport_unavailable_count.saturating_add(1);
                if ws_claimed {
                    pending.buffered_attempt_rows.push(accepted_row);
                } else {
                    emit_now = Some(accepted_row);
                }
            }
            ResolutionPollResult::PostTerminalClaimObservation => {
                pending.buffered_attempt_rows.push(accepted_row);
            }
            ResolutionPollResult::IntegrityError => {
                pending.fatal_integrity_error = true;
                if ws_claimed {
                    pending.buffered_attempt_rows.push(accepted_row);
                } else {
                    emit_now = Some(accepted_row);
                    fail_after_emit = true;
                }
            }
            ResolutionPollResult::MissedDueToDowntime => {
                return Err(
                    invalid_data("live Gamma completion cannot be missed_due_to_downtime").into(),
                );
            }
        }
        if completion.ordinal == POLYMARKET_RESOLUTION_ATTEMPT_COUNT
            && pending.claim.is_none()
            && !pending.fatal_integrity_error
        {
            pending.exhaustion_ticket = Some(raw_day_ticket);
        }
    }
    context.ensure_pending_bounds()?;
    if let Some(row) = emit_now {
        send_polymarket_metadata_row_with_ticket(sink, run_started, row).await?;
    }
    if fail_after_emit {
        return Err(invalid_data(format!(
            "Gamma resolution integrity failure:slug={}:ordinal={}",
            completion.slug, completion.ordinal
        ))
        .into());
    }
    Ok(())
}

pub(super) fn record_cancelled_gamma_resolution_poll(
    task_id: tokio::task::Id,
    context: &mut PolymarketProcessingContext,
) -> Result<()> {
    let pending = context
        .pending
        .values_mut()
        .find(|pending| {
            pending
                .poll_abort_handle
                .as_ref()
                .is_some_and(|handle| handle.id() == task_id)
        })
        .ok_or_else(|| {
            invalid_data(format!(
                "cancelled Gamma resolution poll has no pending owner:task_id={task_id}"
            ))
        })?;
    if !matches!(
        pending.claim,
        Some(PolymarketTerminalClaim::MarketResolvedWs(_))
    ) {
        return Err(invalid_data(format!(
            "Gamma resolution poll was cancelled without a WS terminal claim:task_id={task_id}"
        ))
        .into());
    }
    pending.in_flight_ordinal = None;
    pending.poll_abort_handle = None;
    Ok(())
}

fn clear_cancelled_gamma_resolution_poll_owner(
    task_id: tokio::task::Id,
    context: &mut PolymarketProcessingContext,
) -> Result<()> {
    let pending = context
        .pending
        .values_mut()
        .find(|pending| {
            pending
                .poll_abort_handle
                .as_ref()
                .is_some_and(|handle| handle.id() == task_id)
        })
        .ok_or_else(|| {
            invalid_data(format!(
                "cancelled Gamma resolution poll has no pending owner during clean exit:task_id={task_id}"
            ))
        })?;
    pending.in_flight_ordinal = None;
    pending.poll_abort_handle = None;
    Ok(())
}

fn abort_ws_claimed_gamma_resolution_polls(context: &mut PolymarketProcessingContext) {
    for pending in context.pending.values_mut() {
        if matches!(
            pending.claim,
            Some(PolymarketTerminalClaim::MarketResolvedWs(_))
        ) && let Some(abort) = pending.poll_abort_handle.as_ref()
        {
            abort.abort();
        }
    }
}

async fn record_gamma_resolution_join_result<N>(
    result: std::result::Result<GammaResolutionPollCompletion, tokio::task::JoinError>,
    timeline_now: &N,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
) -> Result<()>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    match result {
        Ok(completion) => {
            record_gamma_resolution_completion(
                completion,
                &|| timeline_now().utc_ms,
                context,
                sink,
                collector_identity,
                run_started,
            )
            .await
        }
        Err(error) if error.is_cancelled() => {
            record_cancelled_gamma_resolution_poll(error.id(), context)
        }
        Err(error) => {
            Err(invalid_data(format!("Gamma resolution poll task failed:{error}")).into())
        }
    }
}

async fn drain_ready_gamma_resolution_polls<N>(
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    timeline_now: &N,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
) -> Result<()>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    while let Some(result) = resolution_poll_tasks.try_join_next() {
        record_gamma_resolution_join_result(
            result,
            timeline_now,
            context,
            sink,
            collector_identity,
            run_started,
        )
        .await?;
    }
    ensure_polymarket_resolution_task_bound(resolution_poll_tasks, context)
}

async fn drain_all_gamma_resolution_polls<N>(
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    timeline_now: &N,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
) -> Result<()>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    while let Some(result) = resolution_poll_tasks.join_next().await {
        record_gamma_resolution_join_result(
            result,
            timeline_now,
            context,
            sink,
            collector_identity,
            run_started,
        )
        .await?;
    }
    ensure_polymarket_resolution_task_bound(resolution_poll_tasks, context)
}

pub(super) fn ensure_polymarket_resolution_task_bound(
    resolution_poll_tasks: &JoinSet<GammaResolutionPollCompletion>,
    context: &PolymarketProcessingContext,
) -> Result<()> {
    let in_flight = context
        .pending
        .values()
        .filter(|pending| pending.in_flight_ordinal.is_some())
        .count();
    let abort_handles = context
        .pending
        .values()
        .filter(|pending| pending.poll_abort_handle.is_some())
        .count();
    let tasks = resolution_poll_tasks.len();
    if tasks > POLYMARKET_PENDING_MAX_MARKETS
        || in_flight > POLYMARKET_PENDING_MAX_MARKETS
        || abort_handles > POLYMARKET_PENDING_MAX_MARKETS
        || tasks != in_flight
        || tasks != abort_handles
    {
        return Err(invalid_data(format!(
            "Polymarket resolution task ownership invariant failed:tasks={tasks}:in_flight={in_flight}:abort_handles={abort_handles}:limit={POLYMARKET_PENDING_MAX_MARKETS}"
        ))
        .into());
    }
    Ok(())
}

fn spawn_due_gamma_resolution_polls<N>(
    gamma: &GammaClient,
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    timeline_now: &Arc<N>,
    context: &mut PolymarketProcessingContext,
    now: PolymarketTimelineNow,
    sink: &SourceEventSink,
) -> Result<()>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync + 'static,
{
    let raw_day_gate = sink
        .source_queue_backpressure
        .admission_gate()
        .ok_or_else(|| invalid_data("raw-day gate missing before Gamma resolution poll spawn"))?;
    let due_polls = context
        .pending
        .iter()
        .filter(|(_, pending)| {
            let ordinal = pending.next_attempt_ordinal;
            pending.claim.is_none()
                && !pending.fatal_integrity_error
                && pending.in_flight_ordinal.is_none()
                && ordinal <= POLYMARKET_RESOLUTION_ATTEMPT_COUNT
                && now.utc_ms >= pending.slot_utc_ms(ordinal)
        })
        .map(|(slug, pending)| {
            let ordinal = pending.next_attempt_ordinal;
            (slug.clone(), ordinal, pending.slot_utc_ms(ordinal))
        })
        .collect::<Vec<_>>();
    for (slug, ordinal, scheduled_at_utc_ms) in due_polls {
        let request_ticket = raw_day_gate.hot_request_ticket();
        if matches!(raw_day_gate, SourceAdmissionGate::Hot(_)) && request_ticket.is_none() {
            continue;
        }
        let gamma = gamma.clone();
        let task_timeline = Arc::clone(timeline_now);
        let task_raw_day_gate = raw_day_gate.clone();
        let task_slug = slug.clone();
        let handle = resolution_poll_tasks.spawn(async move {
            let fetch_start_utc_ms = task_timeline().utc_ms;
            let request = MarketBySlugRequest::builder()
                .slug(task_slug.clone())
                .build();
            let result = match timeout(
                Duration::from_secs(GAMMA_REQUEST_TIMEOUT_SECS),
                gamma.market_by_slug(&request),
            )
            .await
            {
                Ok(Ok(market)) => Ok(market),
                Ok(Err(error)) => match classify_gamma_market_by_slug_transport_error(&error) {
                    Some(transport) => Err(GammaResolutionPollFailure::Transport {
                        class: transport.class,
                        http_status: transport.http_status,
                    }),
                    None => Err(GammaResolutionPollFailure::SdkDecodeOrSchema),
                },
                Err(_) => Err(GammaResolutionPollFailure::Transport {
                    class: ResolutionTransportErrorClass::Timeout,
                    http_status: None,
                }),
            };
            let completed_at_utc_ms = task_timeline().utc_ms;
            let raw_day_ticket = match task_raw_day_gate
                .accept_response_ticket(request_ticket.as_ref(), completed_at_utc_ms)
                .await
            {
                Ok(Some(ticket)) => RawDayTicketAcceptance::Accepted(ticket),
                Ok(None) => RawDayTicketAcceptance::Cutoff,
                Err(error) => RawDayTicketAcceptance::Failed(error.to_string()),
            };
            GammaResolutionPollCompletion {
                slug: task_slug,
                ordinal,
                scheduled_at_utc_ms,
                fetch_start_utc_ms,
                completed_at_utc_ms,
                result,
                raw_day_ticket,
            }
        });
        let pending = context.pending.get_mut(&slug).ok_or_else(|| {
            invalid_data("Polymarket pending owner disappeared before poll spawn")
        })?;
        pending.in_flight_ordinal = Some(ordinal);
        pending.poll_abort_handle = Some(handle);
    }
    ensure_polymarket_resolution_task_bound(resolution_poll_tasks, context)
}

fn next_polymarket_outer_actor_at<N>(
    timeline_now: &N,
    context: &PolymarketProcessingContext,
    include_current_boundary: bool,
) -> Option<TokioInstant>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    let now = timeline_now();
    let current_boundary_utc_ms = if include_current_boundary {
        context
            .current
            .as_ref()
            .map(|current| current.end_ts_s.saturating_mul(1_000))
    } else {
        None
    };
    let next_slot_utc_ms = context
        .pending
        .values()
        .filter(|pending| {
            pending.claim.is_none()
                && !pending.fatal_integrity_error
                && pending.in_flight_ordinal.is_none()
                && pending.next_attempt_ordinal <= POLYMARKET_RESOLUTION_ATTEMPT_COUNT
        })
        .map(|pending| pending.slot_utc_ms(pending.next_attempt_ordinal))
        .min();
    current_boundary_utc_ms
        .into_iter()
        .chain(next_slot_utc_ms)
        .min()
        .map(|wake_utc_ms| {
            now.monotonic
                + Duration::from_millis(
                    u64::try_from(wake_utc_ms.saturating_sub(now.utc_ms)).unwrap_or(0),
                )
        })
}

pub(super) async fn admit_recovered_polymarket_missed_slots(
    observed_at_utc_ms: i64,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
) -> Result<()> {
    let missed = context
        .pending
        .iter()
        .flat_map(|(slug, pending)| {
            pending
                .recovery_missed_ordinals
                .iter()
                .copied()
                .map(|ordinal| (slug.clone(), ordinal))
        })
        .collect::<Vec<_>>();
    if missed.is_empty() {
        return Ok(());
    }
    let rows = missed
        .iter()
        .map(|(slug, ordinal)| {
            let pending = context.pending.get(slug).ok_or_else(|| {
                invalid_data("recovered Polymarket pending disappeared before missed admission")
            })?;
            polymarket_resolution_missed_row(
                pending,
                *ordinal,
                observed_at_utc_ms,
                collector_identity,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    match send_polymarket_metadata_rows_atomically(sink, run_started, rows).await? {
        EventAdmissionOutcome::Admitted => {}
        EventAdmissionOutcome::Deferred => {
            return Err(invalid_data(
                "recovered Polymarket missed rows crossed a UTC admission boundary",
            )
            .into());
        }
        EventAdmissionOutcome::Cutoff => {
            return Err(invalid_data(
                "recovered Polymarket missed rows reached a controlled runtime cutoff",
            )
            .into());
        }
    }
    for (slug, ordinal) in missed {
        let pending = context.pending.get_mut(&slug).ok_or_else(|| {
            invalid_data("recovered Polymarket pending disappeared after missed admission")
        })?;
        if pending.recovery_missed_ordinals.pop_front() != Some(ordinal) {
            return Err(invalid_data(format!(
                "recovered Polymarket missed ordinal order changed:slug={slug}:ordinal={ordinal}"
            ))
            .into());
        }
        pending.attempts_recorded = pending.attempts_recorded.saturating_add(1);
        pending.missed_slot_count = pending.missed_slot_count.saturating_add(1);
    }
    context.ensure_pending_bounds()
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn finalize_polymarket_pending_after_fence<N>(
    timeline_now: &N,
    market_runtime: &mut PolymarketDrainRuntime,
    context: &mut PolymarketProcessingContext,
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    recovery: &mut SourceRecoverySupervisor,
) -> Result<()>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    let ready_slugs = context
        .pending
        .iter()
        .filter(|(_, pending)| pending.requires_fence())
        .map(|(slug, _)| slug.clone())
        .collect::<Vec<_>>();
    if ready_slugs.is_empty() {
        return Ok(());
    }

    let request = market_runtime.request_paused_fence().await?;
    let (resume, messages) =
        collect_polymarket_fence_messages(market_runtime, request, false, sink).await?;
    let resume =
        resume.ok_or_else(|| invalid_data("Polymarket pump closed before paused fence"))?;
    arbitrate_polymarket_ready_items(
        messages,
        resolution_poll_tasks,
        None,
        timeline_now,
        context,
        sink,
        collector_identity,
        run_started,
        recovery,
    )
    .await?;
    abort_ws_claimed_gamma_resolution_polls(context);

    let fatal_integrity = finalize_polymarket_ready_pending_while_paused(
        timeline_now,
        context,
        sink,
        collector_identity,
        run_started,
    )
    .await?;
    resume
        .send(())
        .map_err(|_| invalid_data("Polymarket paused fence resume receiver dropped"))?;
    apply_polymarket_subscription_while_draining(
        market_runtime,
        context.desired_assets()?,
        context,
        resolution_poll_tasks,
        sink,
        collector_identity,
        run_started,
        recovery,
    )
    .await?;
    if let Some(reason) = fatal_integrity {
        return Err(invalid_data(reason).into());
    }
    Ok(())
}

pub(super) async fn finalize_polymarket_ready_pending_while_paused<N>(
    timeline_now: &N,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
) -> Result<Option<String>>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync,
{
    enum PendingFinalization {
        Terminal {
            rows: Vec<AcceptedPolymarketMetadataRow>,
            fatal_integrity: bool,
        },
        Exhausted {
            row: Box<AcceptedPolymarketMetadataRow>,
        },
        NotReady,
    }

    let ready_slugs = context
        .pending
        .iter()
        .filter(|(_, pending)| pending.requires_fence())
        .map(|(slug, _)| slug.clone())
        .collect::<Vec<_>>();
    let mut fatal_integrity = None;
    for slug in ready_slugs {
        let applied_at_utc_ms = timeline_now().utc_ms;
        context.actor_now_utc_ms = applied_at_utc_ms;
        let Some(pending) = context.pending.get(&slug) else {
            continue;
        };
        let finalization = if let Some(claim) = &pending.claim {
            let terminal_row = match claim {
                PolymarketTerminalClaim::MarketResolvedWs(evidence) => {
                    if applied_at_utc_ms < evidence.validation_completed_at_utc_ms {
                        return Err(invalid_data(format!(
                            "Polymarket WS terminal applied clock regressed:slug={slug}:validation_completed={}:applied={applied_at_utc_ms}",
                            evidence.validation_completed_at_utc_ms
                        ))
                        .into());
                    }
                    AcceptedPolymarketMetadataRow {
                        row: polymarket_resolution_terminal_ws_row(
                            pending,
                            evidence,
                            applied_at_utc_ms,
                            collector_identity,
                        )?,
                        raw_day_ticket: evidence.raw_day_ticket.clone(),
                        ws_claimant_source_sequence_epoch_id: Some(
                            evidence.source_sequence_epoch_id.clone(),
                        ),
                        ws_claimant_collector_admission_sequence: Some(
                            evidence.collector_admission_sequence,
                        ),
                    }
                }
                PolymarketTerminalClaim::GammaBySlug(row) => {
                    let validation_completed = row
                        .row
                        .row_evidence_validation_completed_at_utc
                        .unwrap_or(applied_at_utc_ms);
                    if applied_at_utc_ms < validation_completed {
                        return Err(invalid_data(format!(
                            "Polymarket Gamma terminal applied clock regressed:slug={slug}:validation_completed={validation_completed}:applied={applied_at_utc_ms}"
                        ))
                        .into());
                    }
                    let mut accepted = (**row).clone();
                    complete_polymarket_terminal_transition(&mut accepted.row, applied_at_utc_ms);
                    accepted
                }
            };
            let mut terminal_bundle =
                Vec::with_capacity(pending.buffered_attempt_rows.len().saturating_add(1));
            terminal_bundle.push(terminal_row);
            terminal_bundle.extend(pending.buffered_attempt_rows.iter().cloned());
            PendingFinalization::Terminal {
                rows: terminal_bundle,
                fatal_integrity: pending.fatal_integrity_error,
            }
        } else if let Some(raw_day_ticket) = pending.exhaustion_ticket.clone() {
            if applied_at_utc_ms < pending.budget_horizon_utc_ms() {
                return Err(invalid_data(format!(
                    "Polymarket exhausted role-exit preceded budget horizon:slug={slug}:horizon={}:applied={applied_at_utc_ms}",
                    pending.budget_horizon_utc_ms()
                ))
                .into());
            }
            let row = polymarket_resolution_followup_summary_row(
                pending,
                applied_at_utc_ms,
                collector_identity,
            )?;
            let gate = sink
                .source_queue_backpressure
                .admission_gate()
                .ok_or_else(|| {
                    invalid_data(
                        "raw-day gate missing while exhausted Polymarket summary is finalized",
                    )
                })?;
            let raw_day_ticket = gate
                .accept_derived_polymarket_event_ticket(&raw_day_ticket, row.timestamp_received)
                .await?;
            PendingFinalization::Exhausted {
                row: Box::new(AcceptedPolymarketMetadataRow {
                    row,
                    raw_day_ticket,
                    ws_claimant_source_sequence_epoch_id: None,
                    ws_claimant_collector_admission_sequence: None,
                }),
            }
        } else {
            PendingFinalization::NotReady
        };
        match finalization {
            PendingFinalization::Terminal {
                rows,
                fatal_integrity: terminal_fatal_integrity,
            } => {
                send_polymarket_terminal_prefix_ordered(sink, run_started, rows).await?;
                if context.pending.remove(&slug).is_none() {
                    return Err(invalid_data(format!(
                        "Polymarket terminal owner disappeared after successful enqueue:slug={slug}"
                    ))
                    .into());
                }
                if terminal_fatal_integrity {
                    fatal_integrity = Some(format!(
                        "Gamma resolution integrity failure after WS terminal:slug={slug}"
                    ));
                }
            }
            PendingFinalization::Exhausted { row } => {
                send_polymarket_metadata_row_with_ticket(sink, run_started, *row).await?;
                if context.pending.remove(&slug).is_none() {
                    return Err(invalid_data(format!(
                        "Polymarket exhausted owner disappeared after successful enqueue:slug={slug}"
                    ))
                    .into());
                }
            }
            PendingFinalization::NotReady => {}
        }
    }
    context.rebuild_asset_to_slug();
    context.ensure_pending_bounds()?;
    Ok(fatal_integrity)
}

pub(super) async fn abort_polymarket_discovery_tasks<A, B>(
    next_discovery_tasks: &mut JoinSet<A>,
    rollover_discovery_tasks: &mut JoinSet<B>,
) where
    A: Send + 'static,
    B: Send + 'static,
{
    next_discovery_tasks.abort_all();
    rollover_discovery_tasks.abort_all();
    while next_discovery_tasks.join_next().await.is_some() {}
    while rollover_discovery_tasks.join_next().await.is_some() {}
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn collect_polymarket_single_receiver<N>(
    discovery: Option<BtcUpdownDiscovery>,
    context: &mut PolymarketProcessingContext,
    resolution_poll_tasks: &mut JoinSet<GammaResolutionPollCompletion>,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    deadline_ms: Option<i64>,
    run_started: Instant,
    market_runtime: &mut PolymarketDrainRuntime,
    gamma: &GammaClient,
    recovery: &mut SourceRecoverySupervisor,
    control_rx: &mut mpsc::Receiver<PolymarketSourceControl>,
    discovery_wait: Duration,
    discovery_retry_delay: Duration,
    restart_fixed_identities: &mut RecoveredPolymarketFixedIdentities,
    timeline_now: N,
) -> Result<()>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync + 'static,
{
    let timeline_now = Arc::new(timeline_now);
    let validation_timeline = Arc::clone(&timeline_now);
    context.actor_utc_now = Some(Arc::new(move || validation_timeline().utc_ms));
    let initial_now = timeline_now();
    context.actor_now_utc_ms = initial_now.utc_ms;
    admit_recovered_polymarket_missed_slots(
        initial_now.utc_ms,
        context,
        sink,
        collector_identity,
        run_started,
    )
    .await?;
    if let Some(mut discovery) = discovery {
        let recovered_match = match preserve_recovered_polymarket_fixed_identity_authority(
            restart_fixed_identities,
            &mut discovery,
        ) {
            Ok(recovered_match) => recovered_match,
            Err(error) => {
                record_gap(
                    &sink.gaps,
                    format!("polymarket_restart_identity_integrity:{error}"),
                );
                return Err(error);
            }
        };
        let installed = install_polymarket_context_if_current(
            discovery,
            initial_now.utc_ms,
            PolymarketCurrentMetadataRoute::CurrentSnapshot,
            false,
            market_runtime,
            context,
            resolution_poll_tasks,
            sink,
            collector_identity,
            run_started,
            recovery,
        )
        .await?;
        if installed == PolymarketContextInstallOutcome::Expired {
            context.current = None;
            context.next = None;
            context.rebuild_asset_to_slug();
            return Err(PolymarketDiscoveryError::Unavailable(
                "Gamma discovery expired before initial context installation".to_owned(),
            )
            .into());
        }
        recovered_match.consume(restart_fixed_identities);
    } else {
        if context.current.is_none() && context.pending.is_empty() {
            return Err(invalid_data(
                "Polymarket transport recovery has no current or pending context",
            )
            .into());
        }
        let recovered_match = if let Some(mut discovery) = context.discovery_snapshot() {
            let recovered_match = match preserve_recovered_polymarket_fixed_identity_authority(
                restart_fixed_identities,
                &mut discovery,
            ) {
                Ok(recovered_match) => recovered_match,
                Err(error) => {
                    record_gap(
                        &sink.gaps,
                        format!("polymarket_restart_identity_integrity:{error}"),
                    );
                    return Err(error);
                }
            };
            context.replace_discovery(discovery);
            recovered_match
        } else {
            RecoveredPolymarketFixedIdentityMatch::default()
        };
        apply_polymarket_subscription_while_draining(
            market_runtime,
            context.desired_assets()?,
            context,
            resolution_poll_tasks,
            sink,
            collector_identity,
            run_started,
            recovery,
        )
        .await?;
        admit_missing_polymarket_context_metadata(
            context,
            sink,
            collector_identity,
            run_started,
            None,
        )
        .await?;
        recovered_match.consume(restart_fixed_identities);
        recovery.record_business_recovery();
        sink.source_queue_backpressure
            .set_recovery_active("polymarket_market", false);
    }
    let installed_now = timeline_now();
    let mut next_discovery_at = if context.next.is_none() {
        installed_now.monotonic
    } else {
        installed_now.monotonic + Duration::from_secs(MARKET_NEXT_DISCOVERY_INTERVAL_SECS)
    };
    let mut next_discovery_tasks = JoinSet::new();
    let mut rollover_discovery_tasks = JoinSet::new();
    let mut rollover_retry_at = context.current.is_none().then_some(installed_now.monotonic);
    let mut pending_ordinary_control = None;
    loop {
        if pending_ordinary_control.is_none() {
            match control_rx.try_recv() {
                Ok(control) if polymarket_source_control_is_ordinary(&control) => {
                    pending_ordinary_control = Some(control);
                }
                Ok(control) => {
                    abort_polymarket_discovery_tasks(
                        &mut next_discovery_tasks,
                        &mut rollover_discovery_tasks,
                    )
                    .await;
                    if handle_polymarket_source_control(
                        control,
                        market_runtime,
                        context,
                        resolution_poll_tasks,
                        &*timeline_now,
                        sink,
                        collector_identity,
                        run_started,
                        recovery,
                    )
                    .await?
                    {
                        return Ok(());
                    }
                    let resumed_now = timeline_now();
                    if context.next.is_none() {
                        next_discovery_at = resumed_now.monotonic;
                    }
                    if context.current.is_none() {
                        rollover_retry_at = Some(resumed_now.monotonic);
                    }
                    continue;
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    abort_polymarket_discovery_tasks(
                        &mut next_discovery_tasks,
                        &mut rollover_discovery_tasks,
                    )
                    .await;
                    quiesce_connected_polymarket_source(
                        market_runtime,
                        resolution_poll_tasks,
                        &*timeline_now,
                        context,
                        sink,
                        collector_identity,
                        run_started,
                        recovery,
                    )
                    .await?;
                    return Ok(());
                }
            }
        }
        let loop_now = timeline_now();
        context.actor_now_utc_ms = loop_now.utc_ms;

        let boundary_now = if context
            .current
            .as_ref()
            .is_some_and(|current| loop_now.utc_ms >= current.end_ts_s.saturating_mul(1_000))
        {
            arbitrate_polymarket_fence_with_ready_gamma(
                market_runtime,
                resolution_poll_tasks,
                &*timeline_now,
                context,
                sink,
                collector_identity,
                run_started,
                recovery,
            )
            .await?;
            let boundary_now = timeline_now();
            context.actor_now_utc_ms = boundary_now.utc_ms;
            boundary_now
        } else {
            loop_now
        };
        let boundary_advanced = advance_polymarket_boundaries(
            boundary_now.utc_ms,
            context,
            sink,
            collector_identity,
            run_started,
        )
        .await?;
        if boundary_advanced {
            next_discovery_tasks.abort_all();
            next_discovery_at = boundary_now.monotonic;
            if context.current.is_none() {
                rollover_retry_at = Some(boundary_now.monotonic);
            }
        }

        drain_ready_gamma_resolution_polls(
            resolution_poll_tasks,
            &*timeline_now,
            context,
            sink,
            collector_identity,
            run_started,
        )
        .await?;
        drain_ready_market_anchor_requests(context, sink, run_started).await?;
        abort_ws_claimed_gamma_resolution_polls(context);
        if context.pending_requires_fence() {
            finalize_polymarket_pending_after_fence(
                &*timeline_now,
                market_runtime,
                context,
                resolution_poll_tasks,
                sink,
                collector_identity,
                run_started,
                recovery,
            )
            .await?;
            continue;
        }

        if deadline_ms.is_some_and(|deadline_ms| timeline_now().utc_ms >= deadline_ms) {
            abort_polymarket_discovery_tasks(
                &mut next_discovery_tasks,
                &mut rollover_discovery_tasks,
            )
            .await;
            quiesce_connected_polymarket_source(
                market_runtime,
                resolution_poll_tasks,
                &*timeline_now,
                context,
                sink,
                collector_identity,
                run_started,
                recovery,
            )
            .await?;
            return Ok(());
        }

        if let Some(control) = pending_ordinary_control.take() {
            abort_polymarket_discovery_tasks(
                &mut next_discovery_tasks,
                &mut rollover_discovery_tasks,
            )
            .await;
            if handle_polymarket_source_control(
                control,
                market_runtime,
                context,
                resolution_poll_tasks,
                &*timeline_now,
                sink,
                collector_identity,
                run_started,
                recovery,
            )
            .await?
            {
                return Ok(());
            }
            let resumed_now = timeline_now();
            if context.next.is_none() {
                next_discovery_at = resumed_now.monotonic;
            }
            if context.current.is_none() {
                rollover_retry_at = Some(resumed_now.monotonic);
            }
            continue;
        }

        let now = timeline_now();
        spawn_due_market_anchor_requests(gamma, context, sink, collector_identity, &timeline_now)?;
        admit_recovered_polymarket_missed_slots(
            now.utc_ms,
            context,
            sink,
            collector_identity,
            run_started,
        )
        .await?;
        spawn_due_gamma_resolution_polls(
            gamma,
            resolution_poll_tasks,
            &timeline_now,
            context,
            now,
            sink,
        )?;

        if let Some(current) = context.current.clone()
            && context.next.is_none()
            && next_discovery_tasks.is_empty()
            && now.monotonic >= next_discovery_at
            && now.utc_ms < current.end_ts_s * 1000
        {
            next_discovery_at =
                now.monotonic + Duration::from_secs(MARKET_NEXT_DISCOVERY_INTERVAL_SECS);
            let next_slug = discovery_windows(current.start_ts_s * 1000).next.slug;
            let gamma = gamma.clone();
            let discovery_timeline = Arc::clone(&timeline_now);
            next_discovery_tasks.spawn(async move {
                fetch_exact_slug_candidates_with_clock(&gamma, &next_slug, || {
                    discovery_timeline().utc_ms
                })
                .await
            });
        }

        if let Some(result) = next_discovery_tasks.try_join_next() {
            match result {
                Ok(Ok(batch)) if gamma_batch_is_current_next(&batch, timeline_now().utc_ms) => {
                    let Some(mut updated) = context.discovery_snapshot() else {
                        continue;
                    };
                    let selection_started_at_utc_ms = timeline_now().utc_ms;
                    match updated.attach_next(&batch.candidates, selection_started_at_utc_ms) {
                        Ok(true) => {
                            let validation_completed_at_utc_ms = timeline_now().utc_ms;
                            let next = updated.next.as_mut().ok_or_else(|| {
                                invalid_data(
                                    "Polymarket next discovery attached without a selected market",
                                )
                            })?;
                            stamp_polymarket_discovery_validation(
                                next,
                                validation_completed_at_utc_ms,
                            )?;
                            let recovered_match =
                                match preserve_recovered_polymarket_fixed_identity_authority(
                                    restart_fixed_identities,
                                    &mut updated,
                                ) {
                                    Ok(recovered_match) => recovered_match,
                                    Err(error) => {
                                        record_gap(
                                            &sink.gaps,
                                            format!(
                                                "polymarket_restart_identity_integrity:{error}"
                                            ),
                                        );
                                        return Err(error);
                                    }
                                };
                            let installed = switch_polymarket_context(
                                updated,
                                &*timeline_now,
                                PolymarketCurrentMetadataRoute::ExistingCurrent,
                                market_runtime,
                                context,
                                resolution_poll_tasks,
                                sink,
                                collector_identity,
                                run_started,
                                recovery,
                            )
                            .await?;
                            match installed {
                                PolymarketContextInstallOutcome::Applied => {
                                    recovered_match.consume(restart_fixed_identities);
                                }
                                PolymarketContextInstallOutcome::Expired => {
                                    next_discovery_at = timeline_now().monotonic;
                                }
                            }
                        }
                        Ok(false) => {}
                        Err(error) => match classify_next_discovery_selection_error(
                            error,
                            &batch.requested_slug,
                        ) {
                            PolymarketDiscoveryError::Unavailable(_) => {}
                            error => {
                                let error = polymarket_restart_discovery_integrity_error(
                                    restart_fixed_identities,
                                    &error,
                                );
                                record_gap(
                                    &sink.gaps,
                                    format!("polymarket_next_discovery_integrity:{error}"),
                                );
                                return Err(error.into());
                            }
                        },
                    }
                }
                Ok(Ok(_)) | Ok(Err(PolymarketDiscoveryError::Unavailable(_))) => {}
                Ok(Err(error)) => {
                    let error = polymarket_restart_discovery_integrity_error(
                        restart_fixed_identities,
                        &error,
                    );
                    record_gap(
                        &sink.gaps,
                        format!("polymarket_next_discovery_integrity:{error}"),
                    );
                    return Err(error.into());
                }
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    let reason = format!("polymarket_next_discovery_task_failed:{error}");
                    record_gap(&sink.gaps, reason.clone());
                    return Err(invalid_data(reason).into());
                }
            }
        }

        if context.current.is_none()
            && rollover_discovery_tasks.is_empty()
            && rollover_retry_at.is_some_and(|retry_at| timeline_now().monotonic >= retry_at)
        {
            rollover_retry_at = None;
            let gamma = gamma.clone();
            let discovery_timeline = Arc::clone(&timeline_now);
            rollover_discovery_tasks.spawn(async move {
                discover_markets_with_wait_clock(
                    &gamma,
                    discovery_wait,
                    discovery_retry_delay,
                    || discovery_timeline().utc_ms,
                )
                .await
            });
        }
        if let Some(result) = rollover_discovery_tasks.try_join_next() {
            match result {
                Ok(Ok(mut discovery))
                    if gamma_discovery_is_current(&discovery, timeline_now().utc_ms) =>
                {
                    let recovered_match =
                        match preserve_recovered_polymarket_fixed_identity_authority(
                            restart_fixed_identities,
                            &mut discovery,
                        ) {
                            Ok(recovered_match) => recovered_match,
                            Err(error) => {
                                record_gap(
                                    &sink.gaps,
                                    format!("polymarket_restart_identity_integrity:{error}"),
                                );
                                return Err(error);
                            }
                        };
                    let current_metadata_route =
                        recovered_polymarket_current_metadata_route(recovered_match);
                    let installed = switch_polymarket_context(
                        discovery,
                        &*timeline_now,
                        current_metadata_route,
                        market_runtime,
                        context,
                        resolution_poll_tasks,
                        sink,
                        collector_identity,
                        run_started,
                        recovery,
                    )
                    .await?;
                    match installed {
                        PolymarketContextInstallOutcome::Applied => {}
                        PolymarketContextInstallOutcome::Expired => {
                            rollover_retry_at = Some(timeline_now().monotonic);
                            continue;
                        }
                    }
                    let installed_now = timeline_now();
                    recovered_match.consume(restart_fixed_identities);
                    next_discovery_at = if context.next.is_none() {
                        installed_now.monotonic
                    } else {
                        installed_now.monotonic
                            + Duration::from_secs(MARKET_NEXT_DISCOVERY_INTERVAL_SECS)
                    };
                    rollover_retry_at = None;
                }
                Ok(Ok(_)) => {
                    rollover_retry_at = Some(timeline_now().monotonic);
                }
                Ok(Err(PolymarketDiscoveryError::Unavailable(_))) => {
                    rollover_retry_at = Some(schedule_polymarket_discovery_retry(
                        recovery,
                        sink,
                        &*timeline_now,
                    ));
                }
                Ok(Err(error)) => {
                    let error = polymarket_restart_discovery_integrity_error(
                        restart_fixed_identities,
                        &error,
                    );
                    record_gap(
                        &sink.gaps,
                        format!("polymarket_rollover_discovery_integrity:{error}"),
                    );
                    return Err(error.into());
                }
                Err(error) => {
                    let reason = format!("polymarket_rollover_discovery_task_failed:{error}");
                    record_gap(&sink.gaps, reason.clone());
                    return Err(invalid_data(reason).into());
                }
            }
        }

        tokio::select! {
            biased;
            control = control_rx.recv() => {
                let Some(control) = control else {
                    abort_polymarket_discovery_tasks(
                        &mut next_discovery_tasks,
                        &mut rollover_discovery_tasks,
                    )
                    .await;
                    quiesce_connected_polymarket_source(
                        market_runtime,
                        resolution_poll_tasks,
                        &*timeline_now,
                        context,
                        sink,
                        collector_identity,
                        run_started,
                        recovery,
                    )
                    .await?;
                    return Ok(());
                };
                if polymarket_source_control_is_ordinary(&control) {
                    pending_ordinary_control = Some(control);
                    continue;
                }
                abort_polymarket_discovery_tasks(
                    &mut next_discovery_tasks,
                    &mut rollover_discovery_tasks,
                )
                .await;
                if handle_polymarket_source_control(
                    control,
                    market_runtime,
                    context,
                    resolution_poll_tasks,
                    &*timeline_now,
                    sink,
                    collector_identity,
                    run_started,
                    recovery,
                )
                .await?
                {
                    return Ok(());
                }
                let resumed_now = timeline_now();
                if context.next.is_none() {
                    next_discovery_at = resumed_now.monotonic;
                }
                if context.current.is_none() {
                    rollover_retry_at = Some(resumed_now.monotonic);
                }
            }
            received = market_runtime.recv(Duration::from_millis(250)) => match received {
                Ok(Some(PolymarketDrainEvent::Message(message))) => {
                    context.actor_now_utc_ms = timeline_now().utc_ms;
                    if let Err(error) = process_polymarket_budgeted_message(
                        message,
                        context,
                        sink,
                        collector_identity,
                        run_started,
                        recovery,
                    )
                    .await
                    {
                        return stop_polymarket_after_processing_error(
                            error,
                            market_runtime,
                            context,
                            sink,
                            collector_identity,
                            run_started,
                            recovery,
                        )
                        .await;
                    }
                }
                Ok(None) => {}
                Ok(Some(PolymarketDrainEvent::Fence {
                    id,
                    stop_after_fence,
                })) => {
                    return Err(invalid_data(format!(
                        "unexpected unclaimed Polymarket fence:id={id}:stop={stop_after_fence}"
                    ))
                    .into());
                }
                Err(terminal) => return Err(polymarket_drain_terminal_error(terminal, sink)),
            }
        }
    }
}

async fn discover_markets_with_wait(
    gamma: &GammaClient,
    wait: Duration,
    retry_delay: Duration,
) -> std::result::Result<BtcUpdownDiscovery, PolymarketDiscoveryError> {
    discover_markets_with_wait_clock(gamma, wait, retry_delay, now_ms).await
}

pub(super) async fn discover_markets_with_wait_clock<N>(
    gamma: &GammaClient,
    wait: Duration,
    retry_delay: Duration,
    timeline_now_utc_ms: N,
) -> std::result::Result<BtcUpdownDiscovery, PolymarketDiscoveryError>
where
    N: Fn() -> i64,
{
    let started = Instant::now();
    let final_error = loop {
        let query_ms = timeline_now_utc_ms();
        let windows = discovery_windows(query_ms);
        let last_error = match fetch_exact_slug_candidates_with_clock(
            gamma,
            &windows.current.slug,
            &timeline_now_utc_ms,
        )
        .await
        {
            Ok(batch) => {
                let evaluation_ms = timeline_now_utc_ms();
                if !gamma_batch_is_current(&batch, evaluation_ms) {
                    format!(
                        "requested current slug {} expired before processing",
                        batch.requested_slug
                    )
                } else {
                    match select_btc_updown_markets(evaluation_ms, &batch.candidates, &[]) {
                        Ok(mut discovery) => {
                            let validation_completed_at_utc_ms = timeline_now_utc_ms();
                            stamp_polymarket_discovery_validation(
                                &mut discovery.current,
                                validation_completed_at_utc_ms,
                            )
                            .map_err(|error| {
                                PolymarketDiscoveryError::Integrity(error.to_string())
                            })?;
                            return Ok(discovery);
                        }
                        Err(error) => match classify_discovery_selection_error(error) {
                            PolymarketDiscoveryError::Unavailable(message) => message,
                            integrity => return Err(integrity),
                        },
                    }
                }
            }
            Err(PolymarketDiscoveryError::Unavailable(message)) => message,
            Err(integrity) => return Err(integrity),
        };
        if started.elapsed() >= wait {
            break last_error;
        }
        let delay = retry_delay.min(wait.saturating_sub(started.elapsed()));
        if delay.is_zero() {
            break last_error;
        }
        sleep(delay).await;
    };
    Err(PolymarketDiscoveryError::Unavailable(format!(
        "btc-updown-5m current discovery failed after wait_secs={}: {final_error}",
        wait.as_secs()
    )))
}

#[derive(Debug)]
pub(super) struct GammaCandidateBatch {
    pub(super) requested_slug: String,
    pub(super) candidates: Vec<BtcUpdownMarketCandidate>,
}

pub(super) fn classify_discovery_selection_error(
    error: DiscoverySelectionError,
) -> PolymarketDiscoveryError {
    let fixed_identity_expected_slug = match &error {
        DiscoverySelectionError::MultipleCurrentCandidates { expected_slug, .. }
        | DiscoverySelectionError::MultipleNextCandidates { expected_slug, .. } => {
            Some(expected_slug.clone())
        }
        DiscoverySelectionError::InvalidCurrent {
            expected_slug,
            reason,
            ..
        }
        | DiscoverySelectionError::InvalidNext {
            expected_slug,
            reason,
            ..
        } if matches!(
            reason,
            CandidateRejectReason::SlugMismatch
                | CandidateRejectReason::MissingConditionId
                | CandidateRejectReason::InvalidConditionId
                | CandidateRejectReason::InvalidTokenCount
                | CandidateRejectReason::UntrustedOutcomeMapping
                | CandidateRejectReason::WindowBoundsMismatch
        ) =>
        {
            Some(expected_slug.clone())
        }
        _ => None,
    };
    match error {
        DiscoverySelectionError::MissingCurrentCandidate { .. }
        | DiscoverySelectionError::MissingNextCandidate { .. } => {
            PolymarketDiscoveryError::Unavailable(format!("{error:?}"))
        }
        error => {
            let detail = format!("{error:?}");
            if let Some(expected_slug) = fixed_identity_expected_slug {
                PolymarketDiscoveryError::FixedIdentitySelection {
                    expected_slug,
                    detail,
                }
            } else {
                PolymarketDiscoveryError::Integrity(detail)
            }
        }
    }
}

pub(super) fn classify_next_discovery_selection_error(
    error: DiscoverySelectionError,
    expected_slug: &str,
) -> PolymarketDiscoveryError {
    if error == DiscoverySelectionError::PairRelationInvalid {
        return PolymarketDiscoveryError::FixedIdentitySelection {
            expected_slug: expected_slug.to_owned(),
            detail: format!("{error:?}"),
        };
    }
    classify_discovery_selection_error(error)
}

fn classify_gamma_sdk_error(
    error: polymarket_client_sdk_v2::error::Error,
) -> PolymarketDiscoveryError {
    if error.kind() == PolymarketSdkErrorKind::Status {
        return PolymarketDiscoveryError::Unavailable(error.to_string());
    }
    if let Some(reqwest_error) = error.downcast_ref::<reqwest::Error>() {
        if reqwest_error.is_builder() || reqwest_error.is_decode() {
            return PolymarketDiscoveryError::Integrity(error.to_string());
        }
        return PolymarketDiscoveryError::Unavailable(error.to_string());
    }
    if error.downcast_ref::<serde_json::Error>().is_some() {
        return PolymarketDiscoveryError::Integrity(error.to_string());
    }
    PolymarketDiscoveryError::Integrity(error.to_string())
}

pub(super) fn gamma_batch_is_current_next(batch: &GammaCandidateBatch, evaluation_ms: i64) -> bool {
    discovery_windows(evaluation_ms).next.slug == batch.requested_slug
}

pub(super) fn gamma_batch_is_current(batch: &GammaCandidateBatch, evaluation_ms: i64) -> bool {
    discovery_windows(evaluation_ms).current.slug == batch.requested_slug
}

pub(super) fn gamma_discovery_is_current(
    discovery: &BtcUpdownDiscovery,
    evaluation_ms: i64,
) -> bool {
    discovery_windows(evaluation_ms).current.slug == discovery.current.slug
}

#[cfg(test)]
pub(super) fn late_polymarket_identity_can_activate(
    source_received_at_utc_ms: i64,
    verified_at_utc_ms: i64,
    applied_at_utc_ms: i64,
    market_start_utc_ms: i64,
    market_end_utc_ms: i64,
) -> bool {
    source_received_at_utc_ms <= verified_at_utc_ms
        && verified_at_utc_ms <= applied_at_utc_ms
        && applied_at_utc_ms >= market_start_utc_ms
        && applied_at_utc_ms < market_end_utc_ms
}

#[cfg(test)]
pub(super) async fn fetch_exact_slug_candidates(
    gamma: &GammaClient,
    slug: &str,
) -> std::result::Result<GammaCandidateBatch, PolymarketDiscoveryError> {
    fetch_exact_slug_candidates_with_clock(gamma, slug, now_ms).await
}

async fn fetch_exact_slug_candidates_with_clock<N>(
    gamma: &GammaClient,
    slug: &str,
    timeline_now_utc_ms: N,
) -> std::result::Result<GammaCandidateBatch, PolymarketDiscoveryError>
where
    N: Fn() -> i64,
{
    let mut request = MarketsRequest::default();
    request.slug = vec![slug.to_owned()];
    request.closed = Some(false);
    request.limit = Some(10);
    let metadata_fetch_start_utc_ms = timeline_now_utc_ms();
    let markets = timeout(
        Duration::from_secs(GAMMA_REQUEST_TIMEOUT_SECS),
        gamma.markets(&request),
    )
    .await
    .map_err(|_| {
        PolymarketDiscoveryError::Unavailable(format!(
            "Gamma request for {slug} exceeded {GAMMA_REQUEST_TIMEOUT_SECS}s"
        ))
    })?
    .map_err(classify_gamma_sdk_error)?;
    let metadata_received_utc_ms = timeline_now_utc_ms();
    let candidates = markets
        .iter()
        .map(|market| {
            let mut candidate = BtcUpdownMarketCandidate::from_sdk_market(market);
            candidate.metadata_fetch_start_utc_ms = Some(metadata_fetch_start_utc_ms);
            candidate.metadata_received_utc_ms = Some(metadata_received_utc_ms);
            candidate
        })
        .collect();
    Ok(GammaCandidateBatch {
        requested_slug: slug.to_owned(),
        candidates,
    })
}

pub(super) fn bounded_ws_config() -> WsConfig {
    let mut config = WsConfig::default();
    config.connect_timeout = Some(Duration::from_secs(WS_CONNECT_TIMEOUT_SECS));
    config.reconnect.max_attempts = Some(3);
    config.reconnect.max_backoff = Duration::from_secs(5);
    config.heartbeat_interval = Duration::from_secs(10);
    config.require_text_pong = true;
    config.heartbeat_timeout = Duration::from_secs(20);
    config
}

#[derive(Clone)]
pub(super) struct PolymarketSourceSpec {
    pub(super) gamma_endpoint: String,
    pub(super) market_ws_endpoint: String,
    pub(super) ws_config: WsConfig,
    pub(super) discovery_wait: Duration,
    pub(super) discovery_retry_delay: Duration,
    #[cfg(test)]
    pub(super) recovery_wait_override: Option<Duration>,
}

impl PolymarketSourceSpec {
    pub(super) fn production() -> Self {
        Self {
            gamma_endpoint: "https://gamma-api.polymarket.com".to_owned(),
            market_ws_endpoint: MARKET_WS_ENDPOINT.to_owned(),
            ws_config: bounded_ws_config(),
            discovery_wait: Duration::from_secs(DEFAULT_DISCOVERY_WAIT_SECS),
            discovery_retry_delay: Duration::from_secs(10),
            #[cfg(test)]
            recovery_wait_override: None,
        }
    }
}

static POLYMARKET_SOURCE_EPOCH_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PolymarketOrderbookV3AdmissionReceipt {
    pub(super) source_sequence_epoch_id: String,
    pub(super) collector_admission_sequence: u64,
    pub(super) collector_admission_time_utc_ms: i64,
    pub(super) market_resolved: Option<PolymarketMarketResolvedAdmissionEvidence>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PolymarketMarketResolvedAdmissionEvidence {
    pub(super) source_evidence_payload_json: String,
    pub(super) source_evidence_raw_sha256: String,
    pub(super) platform_timestamp_utc_ms: i64,
    pub(super) winning_asset_id: String,
    pub(super) winning_outcome: String,
}

fn polymarket_market_resolved_admission_evidence(
    message: &PolymarketOrderbookV3Message,
) -> Result<Option<PolymarketMarketResolvedAdmissionEvidence>> {
    let PolymarketOrderbookV3Event::MarketResolved(resolved) = &message.event else {
        return Ok(None);
    };
    let platform_timestamp_utc_ms = message.envelope.timestamp.ok_or_else(|| {
        invalid_data("admitted Polymarket market_resolved is missing its platform timestamp")
    })?;
    let event_message = resolved.event_message.as_ref().map(|event_message| {
        json!({
            "id": event_message.id,
            "ticker": event_message.ticker,
            "slug": event_message.slug,
            "title": event_message.title,
            "description": event_message.description,
        })
    });
    let source_evidence_payload_json = serde_json::to_string(&json!({
        "event_type": "market_resolved",
        "market": message.envelope.market,
        "slug": message.envelope.slug,
        "clob_market_id": resolved.clob_market_id,
        "question": resolved.question,
        "description": resolved.description,
        "asset_ids": resolved.asset_ids,
        "outcomes": resolved.outcomes,
        "event_message": event_message,
        "winning_asset_id": resolved.winning_asset_id,
        "winning_outcome": resolved.winning_outcome,
        "timestamp": platform_timestamp_utc_ms,
    }))?;
    let source_evidence_raw_sha256 = Sha256::digest(source_evidence_payload_json.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(Some(PolymarketMarketResolvedAdmissionEvidence {
        source_evidence_payload_json,
        source_evidence_raw_sha256,
        platform_timestamp_utc_ms,
        winning_asset_id: resolved.winning_asset_id.clone(),
        winning_outcome: resolved.winning_outcome.clone(),
    }))
}

fn record_polymarket_fanout_drop(
    sink: &SourceEventSink,
    recv_time_utc_ms: i64,
    market_rows: usize,
    metadata_rows: usize,
) {
    let total = market_rows.saturating_add(metadata_rows) as u64;
    sink.source_queue_backpressure.record_drop(total);
    if market_rows > 0 {
        sink.source_queue_backpressure.record_operational_drop(
            "polymarket_market",
            recv_time_utc_ms,
            market_rows as u64,
        );
    }
    if metadata_rows > 0 {
        sink.source_queue_backpressure.record_operational_drop(
            "polymarket_market_metadata",
            recv_time_utc_ms,
            metadata_rows as u64,
        );
    }
}

enum PolymarketRawDayAuthority<'a> {
    Unadmitted,
    Accepted(&'a RawDayAcceptedTicket),
}

pub(super) async fn send_polymarket_events_atomically<F>(
    sink: &SourceEventSink,
    recv_time_utc_ms: i64,
    events: Vec<InflightRawEvent>,
    commit: F,
) -> Result<EventAdmissionOutcome>
where
    F: FnOnce() -> Result<()>,
{
    send_polymarket_events_atomically_inner(
        sink,
        recv_time_utc_ms,
        events,
        PolymarketRawDayAuthority::Unadmitted,
        commit,
    )
    .await
}

async fn send_polymarket_events_atomically_with_ticket<F>(
    sink: &SourceEventSink,
    recv_time_utc_ms: i64,
    events: Vec<InflightRawEvent>,
    ticket: &RawDayAcceptedTicket,
    commit: F,
) -> Result<EventAdmissionOutcome>
where
    F: FnOnce() -> Result<()>,
{
    send_polymarket_events_atomically_inner(
        sink,
        recv_time_utc_ms,
        events,
        PolymarketRawDayAuthority::Accepted(ticket),
        commit,
    )
    .await
}

async fn send_polymarket_events_atomically_inner<F>(
    sink: &SourceEventSink,
    recv_time_utc_ms: i64,
    mut events: Vec<InflightRawEvent>,
    authority: PolymarketRawDayAuthority<'_>,
    commit: F,
) -> Result<EventAdmissionOutcome>
where
    F: FnOnce() -> Result<()>,
{
    if events.is_empty() {
        if let Err(error) = commit() {
            record_gap_at(
                &sink.gaps,
                recv_time_utc_ms,
                format!("polymarket_atomic_fanout_commit_failed:{error}"),
            );
            return Err(error);
        }
        return Ok(EventAdmissionOutcome::Admitted);
    }
    let market_rows = events
        .iter()
        .filter(|event| event.event.source_kind == "polymarket_market")
        .count();
    let metadata_rows = events.len().saturating_sub(market_rows);
    let batch_utc_day = utc_day_string_from_ms(recv_time_utc_ms)
        .ok_or_else(|| invalid_data("Polymarket atomic batch UTC day is unavailable"))?;
    if events.iter().any(|event| {
        utc_day_string_from_ms(event.event.recv_time_utc_ms).as_deref()
            != Some(batch_utc_day.as_str())
    }) {
        let reason = "Polymarket atomic batch spans UTC days".to_owned();
        record_polymarket_fanout_drop(sink, recv_time_utc_ms, market_rows, metadata_rows);
        record_gap_at(&sink.gaps, recv_time_utc_ms, reason.clone());
        return Err(invalid_data(reason).into());
    }
    let gate = sink
        .source_queue_backpressure
        .admission_gate()
        .ok_or_else(|| {
            invalid_data("raw-day gate missing from production Polymarket enqueue path")
        })?;
    let (raw_enqueue, accepted_ticket) = match authority {
        PolymarketRawDayAuthority::Unadmitted => {
            let Some(raw_enqueue) = gate
                .begin_unadmitted_event_enqueue("polymarket", recv_time_utc_ms)
                .await?
            else {
                return Ok(EventAdmissionOutcome::Cutoff);
            };
            (raw_enqueue, None)
        }
        PolymarketRawDayAuthority::Accepted(ticket) => (
            gate.begin_accepted_ticket_enqueue("polymarket", recv_time_utc_ms, ticket)
                .await?,
            Some(ticket.clone()),
        ),
    };
    let raw_day_permit = match &raw_enqueue {
        SourceEnqueueDecision::Permit(enqueue) => enqueue.raw_day_permit.clone(),
        SourceEnqueueDecision::Defer(_) => None,
    };
    for event in &mut events {
        event.raw_day_permit = raw_day_permit.clone();
    }
    let count = events.len();
    if count > sink.tx.max_capacity() {
        record_polymarket_fanout_drop(sink, recv_time_utc_ms, market_rows, metadata_rows);
        let reason = format!(
            "operator_intervention_required:Polymarket fan-out exceeds ingress capacity:rows={count}:capacity={}",
            sink.tx.max_capacity()
        );
        record_gap_at(&sink.gaps, recv_time_utc_ms, reason.clone());
        return Err(invalid_data(reason).into());
    }
    if let SourceEnqueueDecision::Defer(epoch) = &raw_enqueue {
        if let Err(error) = commit() {
            record_polymarket_fanout_drop(sink, recv_time_utc_ms, market_rows, metadata_rows);
            record_gap_at(
                &sink.gaps,
                recv_time_utc_ms,
                format!("polymarket_atomic_fanout_commit_failed:{error}"),
            );
            return Err(error);
        }
        if let Some(ticket) = accepted_ticket {
            sink.source_queue_backpressure.defer_accepted_raw_batch(
                "polymarket",
                epoch.clone(),
                events,
                ticket,
            )?;
        } else {
            sink.source_queue_backpressure
                .defer_raw_batch("polymarket", epoch.clone(), events)?;
        }
        return Ok(EventAdmissionOutcome::Deferred);
    }
    let _enqueue_guard = sink.enqueue_lock.lock().await;
    let permits = match sink.tx.try_reserve_many(count) {
        Ok(permits) => permits,
        Err(mpsc::error::TrySendError::Full(())) => {
            sink.source_queue_backpressure.record_wait();
            match timeout(BUFFER_ACQUIRE_TIMEOUT, sink.tx.reserve_many(count)).await {
                Ok(Ok(permits)) => permits,
                Ok(Err(_)) => {
                    record_polymarket_fanout_drop(
                        sink,
                        recv_time_utc_ms,
                        market_rows,
                        metadata_rows,
                    );
                    let reason = "bounded_queue_send_closed:polymarket_atomic_fanout".to_owned();
                    record_gap_at(&sink.gaps, recv_time_utc_ms, reason.clone());
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, reason).into());
                }
                Err(_) => {
                    sink.source_queue_backpressure.record_timeout();
                    record_polymarket_fanout_drop(
                        sink,
                        recv_time_utc_ms,
                        market_rows,
                        metadata_rows,
                    );
                    let reason =
                        "bounded_queue_backpressure_timeout:polymarket_atomic_fanout".to_owned();
                    record_gap_at(&sink.gaps, recv_time_utc_ms, reason.clone());
                    return Err(io::Error::new(io::ErrorKind::TimedOut, reason).into());
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(())) => {
            record_polymarket_fanout_drop(sink, recv_time_utc_ms, market_rows, metadata_rows);
            let reason = "bounded_queue_send_closed:polymarket_atomic_fanout".to_owned();
            record_gap_at(&sink.gaps, recv_time_utc_ms, reason.clone());
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, reason).into());
        }
    };
    if let Err(error) = stamp_source_admissions_atomically(&mut events, sink) {
        record_polymarket_fanout_drop(sink, recv_time_utc_ms, market_rows, metadata_rows);
        return Err(error);
    }
    if let Err(error) = commit() {
        record_polymarket_fanout_drop(sink, recv_time_utc_ms, market_rows, metadata_rows);
        record_gap_at(
            &sink.gaps,
            recv_time_utc_ms,
            format!("polymarket_atomic_fanout_commit_failed:{error}"),
        );
        return Err(error);
    }
    for (permit, event) in permits.zip(events) {
        permit.send(SourceIngressPayload::RawEvent(event));
    }
    Ok(EventAdmissionOutcome::Admitted)
}

pub(super) async fn send_polymarket_metadata_event(
    sink: &SourceEventSink,
    run_started: Instant,
    market: &crate::market_discovery::DiscoveredBtcUpdownMarket,
    discovery_slot: &str,
    collector_identity: &CollectorIdentity,
) -> Result<SourceAuthority> {
    let role = match discovery_slot {
        "current" => MarketRuntimeRole::Current,
        "next" => MarketRuntimeRole::Next,
        other => {
            return Err(invalid_data(format!(
                "unsupported Polymarket metadata discovery slot:{other}"
            ))
            .into());
        }
    };
    let row = polymarket_metadata_snapshot_row(market, role, collector_identity)?;
    send_polymarket_metadata_source_authority_row(sink, run_started, row).await
}

async fn send_polymarket_metadata_row(
    sink: &SourceEventSink,
    run_started: Instant,
    row: PolymarketMarketMetadataRow,
) -> Result<()> {
    let event = polymarket_metadata_event(row, None, None)?;
    send_event(sink, run_started, event).await
}

fn polymarket_metadata_event(
    row: PolymarketMarketMetadataRow,
    ws_claimant_source_sequence_epoch_id: Option<String>,
    ws_claimant_collector_admission_sequence: Option<u64>,
) -> Result<RawEvent> {
    let row = PolymarketMarketMetadataV5Row::from_common(
        row,
        ws_claimant_source_sequence_epoch_id,
        ws_claimant_collector_admission_sequence,
    );
    validate_polymarket_metadata_draft(&row)?;
    polymarket_metadata_v6_raw_event(PolymarketMarketMetadataV6Row::Existing(row))
}

fn validate_polymarket_metadata_draft(row: &PolymarketMarketMetadataV5Row) -> Result<()> {
    let validation = row.validate_draft();
    if !validation.is_complete() {
        return Err(invalid_data(format!(
            "Polymarket metadata v5 validation failed before admission:{}",
            validation.violations.join(",")
        ))
        .into());
    }
    Ok(())
}

async fn send_polymarket_metadata_row_with_ticket(
    sink: &SourceEventSink,
    run_started: Instant,
    accepted: AcceptedPolymarketMetadataRow,
) -> Result<()> {
    let event = polymarket_metadata_event(
        accepted.row,
        accepted.ws_claimant_source_sequence_epoch_id,
        accepted.ws_claimant_collector_admission_sequence,
    )?;
    send_event_with_ticket(sink, run_started, event, &accepted.raw_day_ticket).await
}

pub(super) async fn send_polymarket_metadata_source_authority_row(
    sink: &SourceEventSink,
    run_started: Instant,
    row: PolymarketMarketMetadataRow,
) -> Result<SourceAuthority> {
    let authority = discovery_source_authority_from_row(&row)?;
    let event = polymarket_metadata_event(row, None, None)?;
    match send_event_with_admission_outcome(sink, run_started, event).await? {
        EventAdmissionOutcome::Admitted => Ok(authority),
        EventAdmissionOutcome::Deferred => Err(invalid_data(
            "polymarket_metadata_source_authority_not_admitted:utc_deferred",
        )
        .into()),
        EventAdmissionOutcome::Cutoff => Err(invalid_data(
            "polymarket_metadata_source_authority_not_admitted:controlled_runtime_cutoff",
        )
        .into()),
    }
}

pub(super) fn discovery_source_authority_from_row(
    row: &PolymarketMarketMetadataRow,
) -> Result<SourceAuthority> {
    if !matches!(
        row.metadata_row_kind,
        PolymarketMetadataRowKind::MetadataSnapshot
            | PolymarketMetadataRowKind::MarketStateTransition
    ) || row.source_evidence_origin != Some(SourceEvidenceOrigin::GammaMarketsDiscovery)
    {
        return Err(invalid_data(format!(
            "Polymarket source authority is not a complete discovery evidence row:slug={}",
            row.slug
        ))
        .into());
    }
    source_authority_from_metadata_row(row).ok_or_else(|| {
        invalid_data(format!(
            "Polymarket source authority is not a complete discovery evidence row:slug={}",
            row.slug
        ))
        .into()
    })
}

async fn send_polymarket_metadata_rows_atomically(
    sink: &SourceEventSink,
    run_started: Instant,
    rows: Vec<PolymarketMarketMetadataRow>,
) -> Result<EventAdmissionOutcome> {
    let rows = rows
        .into_iter()
        .map(|row| PolymarketMarketMetadataV5Row::from_common(row, None, None))
        .collect();
    send_polymarket_metadata_rows_atomically_with_authority(sink, run_started, rows, None).await
}

pub(super) async fn send_polymarket_metadata_rows_atomically_with_tickets(
    sink: &SourceEventSink,
    run_started: Instant,
    accepted_rows: Vec<AcceptedPolymarketMetadataRow>,
) -> Result<EventAdmissionOutcome> {
    let Some(first) = accepted_rows.first() else {
        return Ok(EventAdmissionOutcome::Admitted);
    };
    let raw_day_ticket = first.raw_day_ticket.clone();
    if accepted_rows
        .iter()
        .any(|accepted| accepted.raw_day_ticket != raw_day_ticket)
    {
        let recv_time_utc_ms = first.row.timestamp_received;
        let reason =
            "Polymarket terminal atomic group contains inconsistent raw-day tickets".to_owned();
        record_polymarket_fanout_drop(sink, recv_time_utc_ms, 0, accepted_rows.len());
        record_gap_at(&sink.gaps, recv_time_utc_ms, reason.clone());
        return Err(invalid_data(reason).into());
    }
    let rows = accepted_rows
        .into_iter()
        .map(|accepted| {
            PolymarketMarketMetadataV5Row::from_common(
                accepted.row,
                accepted.ws_claimant_source_sequence_epoch_id,
                accepted.ws_claimant_collector_admission_sequence,
            )
        })
        .collect();
    send_polymarket_metadata_rows_atomically_with_authority(
        sink,
        run_started,
        rows,
        Some(&raw_day_ticket),
    )
    .await
}

async fn send_polymarket_metadata_rows_atomically_with_authority(
    sink: &SourceEventSink,
    run_started: Instant,
    rows: Vec<PolymarketMarketMetadataV5Row>,
    raw_day_ticket: Option<&RawDayAcceptedTicket>,
) -> Result<EventAdmissionOutcome> {
    for row in &rows {
        validate_polymarket_metadata_draft(row)?;
    }
    if rows.len() <= 1 {
        if let Some(row) = rows.into_iter().next() {
            let event =
                polymarket_metadata_v6_raw_event(PolymarketMarketMetadataV6Row::Existing(row))?;
            return match raw_day_ticket {
                Some(ticket) => {
                    send_event_with_ticket_outcome(sink, run_started, event, ticket).await
                }
                None => send_event_with_admission_outcome(sink, run_started, event).await,
            };
        }
        return Ok(EventAdmissionOutcome::Admitted);
    }

    let events = rows
        .into_iter()
        .map(|row| polymarket_metadata_v6_raw_event(PolymarketMarketMetadataV6Row::Existing(row)))
        .collect::<Result<Vec<_>>>()?;
    let recv_time_utc_ms = events[0].recv_time_utc_ms;
    let partition = utc_hour_string_from_ms(recv_time_utc_ms)
        .ok_or_else(|| invalid_data("Polymarket terminal bundle UTC partition is unavailable"))?;
    if events.iter().any(|event| {
        utc_hour_string_from_ms(event.recv_time_utc_ms).as_deref() != Some(partition.as_str())
    }) {
        let reason = "Polymarket terminal bundle spans UTC partitions".to_owned();
        record_polymarket_fanout_drop(sink, recv_time_utc_ms, 0, events.len());
        record_gap_at(&sink.gaps, recv_time_utc_ms, reason.clone());
        return Err(invalid_data(reason).into());
    }

    let mut total_bytes = 0_usize;
    let mut measured_bytes = Vec::with_capacity(events.len());
    for event in &events {
        let bytes = owned_event_bytes(event);
        if bytes > MAX_TYPED_ROW_BYTES {
            let reason = format!(
                "operator_intervention_required:max_typed_row_bytes_exceeded:polymarket_market_metadata:bytes={bytes}:limit={MAX_TYPED_ROW_BYTES}"
            );
            record_polymarket_fanout_drop(sink, recv_time_utc_ms, 0, events.len());
            record_gap_at(&sink.gaps, recv_time_utc_ms, reason.clone());
            return Err(invalid_data(reason).into());
        }
        total_bytes = total_bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid_data("Polymarket terminal bundle byte count overflow"))?;
        measured_bytes.push(bytes);
    }
    let mut reservation = match sink
        .inflight_budget
        .acquire(events.len(), total_bytes, BUFFER_ACQUIRE_TIMEOUT)
        .await
    {
        Ok(reservation) => reservation,
        Err(error) => {
            sink.source_queue_backpressure.record_timeout();
            record_polymarket_fanout_drop(sink, recv_time_utc_ms, 0, events.len());
            record_gap_at(
                &sink.gaps,
                recv_time_utc_ms,
                format!("polymarket_terminal_bundle_budget_failed:{error}"),
            );
            return Err(error.into());
        }
    };
    let mut prepared = Vec::with_capacity(events.len());
    for (event, bytes) in events.into_iter().zip(measured_bytes) {
        let row_reservation = reservation.split(1, bytes)?;
        prepared.push(InflightRawEvent {
            event,
            control: None,
            admission_gaps: Vec::new(),
            raw_day_permit: None,
            _source_recv_monotonic_ms: monotonic_ms(run_started),
            _reservation: Some(row_reservation),
        });
    }
    drop(reservation);
    match raw_day_ticket {
        Some(ticket) => {
            send_polymarket_events_atomically_with_ticket(
                sink,
                recv_time_utc_ms,
                prepared,
                ticket,
                || Ok(()),
            )
            .await
        }
        None => {
            send_polymarket_events_atomically(sink, recv_time_utc_ms, prepared, || Ok(())).await
        }
    }
}

pub(super) async fn send_polymarket_terminal_prefix_ordered(
    sink: &SourceEventSink,
    run_started: Instant,
    rows: Vec<AcceptedPolymarketMetadataRow>,
) -> Result<()> {
    if rows.len() <= 1 {
        match send_polymarket_metadata_rows_atomically_with_tickets(sink, run_started, rows).await?
        {
            EventAdmissionOutcome::Admitted | EventAdmissionOutcome::Deferred => {}
            EventAdmissionOutcome::Cutoff => return Ok(()),
        }
        return Ok(());
    }
    let first = rows
        .first()
        .ok_or_else(|| invalid_data("Polymarket terminal prefix is empty"))?;
    if first.row.metadata_row_kind != PolymarketMetadataRowKind::ResolutionTerminalWs {
        return Err(invalid_data(
            "cross-partition Polymarket terminal prefix requires a WS terminal first",
        )
        .into());
    }
    if rows.iter().skip(1).any(|accepted| {
        let row = &accepted.row;
        row.metadata_row_kind != PolymarketMetadataRowKind::ResolutionPollAttempt
            || !matches!(
                row.resolution_poll_result,
                Some(
                    ResolutionPollResult::PostTerminalClaimObservation
                        | ResolutionPollResult::TransportUnavailable
                        | ResolutionPollResult::IntegrityError
                )
            )
            || (row.resolution_poll_result
                == Some(ResolutionPollResult::PostTerminalClaimObservation)
                && row.terminal_claim_owner != Some(TerminalClaimOwner::MarketResolvedWs))
    }) {
        return Err(invalid_data(
            "cross-partition Polymarket terminal prefix contains an invalid buffered attempt",
        )
        .into());
    }

    let mut groups = Vec::<Vec<AcceptedPolymarketMetadataRow>>::new();
    let mut previous_partition = None::<String>;
    for accepted in rows {
        let partition =
            utc_hour_string_from_ms(accepted.row.timestamp_received).ok_or_else(|| {
                invalid_data("Polymarket terminal prefix UTC partition is unavailable")
            })?;
        if previous_partition
            .as_ref()
            .is_some_and(|previous| partition < *previous)
        {
            return Err(invalid_data("Polymarket terminal prefix UTC partition regressed").into());
        }
        if previous_partition.as_deref() != Some(partition.as_str()) {
            groups.push(Vec::new());
            previous_partition = Some(partition);
        }
        groups
            .last_mut()
            .expect("terminal prefix has a current partition")
            .push(accepted);
    }

    for group in groups {
        match send_polymarket_metadata_rows_atomically_with_tickets(sink, run_started, group)
            .await?
        {
            EventAdmissionOutcome::Admitted | EventAdmissionOutcome::Deferred => {}
            EventAdmissionOutcome::Cutoff => continue,
        }
    }
    Ok(())
}

fn record_polymarket_v3_admission_failure(
    sink: &SourceEventSink,
    recv_time_utc_ms: i64,
    reason: &str,
) {
    sink.source_queue_backpressure.record_drop(1);
    sink.source_queue_backpressure.record_operational_drop(
        "polymarket_market",
        recv_time_utc_ms,
        1,
    );
    record_gap_at(
        &sink.gaps,
        recv_time_utc_ms,
        format!("polymarket_orderbook_v3_admission_failed:{reason}"),
    );
}

pub(super) async fn enqueue_polymarket_orderbook_v3_with_ticket<F>(
    sink: &SourceEventSink,
    run_started: Instant,
    recv_time_utc_ms: i64,
    ticket: &RawDayAcceptedTicket,
    collector_identity: &CollectorIdentity,
    source_sequence_epoch_id: &str,
    build_projection: F,
) -> Result<PolymarketOrderbookV3AdmissionReceipt>
where
    F: FnOnce() -> Result<PolymarketOrderbookV3Projection>,
{
    let gate = sink
        .source_queue_backpressure
        .admission_gate()
        .ok_or_else(|| invalid_data("raw-day gate missing from Polymarket v3 admission path"))?;
    let raw_enqueue = gate
        .begin_accepted_ticket_enqueue("polymarket", recv_time_utc_ms, ticket)
        .await?;

    let budget_deadline = TokioInstant::now() + BUFFER_ACQUIRE_TIMEOUT;
    let mut byte_reservation = match sink
        .inflight_budget
        .acquire(0, MAX_TYPED_ROW_BYTES, BUFFER_ACQUIRE_TIMEOUT)
        .await
    {
        Ok(reservation) => reservation,
        Err(error) => {
            sink.source_queue_backpressure.record_timeout();
            record_polymarket_v3_admission_failure(
                sink,
                recv_time_utc_ms,
                &format!("global_inflight_budget_failed:{error}"),
            );
            return Err(error.into());
        }
    };
    let projection = build_projection().inspect_err(|error| {
        record_polymarket_v3_admission_failure(
            sink,
            recv_time_utc_ms,
            &format!("canonical_projection_failed:{error}"),
        );
    })?;
    let pending = projection
        .prepare_admission(
            recv_time_utc_ms,
            collector_identity.host_id.clone(),
            collector_identity.region.clone(),
            MARKET_WS_ENDPOINT.to_owned(),
            source_sequence_epoch_id.to_owned(),
        )
        .inspect_err(|error| {
            record_polymarket_v3_admission_failure(
                sink,
                recv_time_utc_ms,
                &format!("canonical_projection_failed:{error}"),
            );
        })?;
    let owned_bytes = pending.owned_bytes();
    if owned_bytes > MAX_TYPED_ROW_BYTES {
        let reason = format!(
            "operator_intervention_required:max_typed_row_bytes_exceeded:bytes={owned_bytes}:limit={MAX_TYPED_ROW_BYTES}"
        );
        record_polymarket_v3_admission_failure(sink, recv_time_utc_ms, &reason);
        return Err(invalid_data(reason).into());
    }
    byte_reservation
        .shrink_to(0, owned_bytes)
        .inspect_err(|error| {
            record_polymarket_v3_admission_failure(
                sink,
                recv_time_utc_ms,
                &format!("inflight_byte_reservation_shrink_failed:{error}"),
            );
        })?;
    let row_reservation = match sink
        .inflight_budget
        .acquire(
            1,
            0,
            budget_deadline.saturating_duration_since(TokioInstant::now()),
        )
        .await
    {
        Ok(reservation) => reservation,
        Err(error) => {
            sink.source_queue_backpressure.record_timeout();
            record_polymarket_v3_admission_failure(
                sink,
                recv_time_utc_ms,
                &format!("global_inflight_row_budget_failed:{error}"),
            );
            return Err(error.into());
        }
    };

    match raw_enqueue {
        SourceEnqueueDecision::Permit(raw_enqueue) => {
            let _enqueue_guard = sink.enqueue_lock.lock().await;
            let queue_permit = match sink.tx.try_reserve() {
                Ok(permit) => permit,
                Err(mpsc::error::TrySendError::Full(())) => {
                    sink.source_queue_backpressure.record_wait();
                    match timeout(BUFFER_ACQUIRE_TIMEOUT, sink.tx.reserve()).await {
                        Ok(Ok(permit)) => permit,
                        Ok(Err(_)) => {
                            let reason = "bounded_queue_send_closed";
                            record_polymarket_v3_admission_failure(sink, recv_time_utc_ms, reason);
                            return Err(io::Error::new(io::ErrorKind::BrokenPipe, reason).into());
                        }
                        Err(_) => {
                            sink.source_queue_backpressure.record_timeout();
                            let reason = "bounded_queue_backpressure_timeout";
                            record_polymarket_v3_admission_failure(sink, recv_time_utc_ms, reason);
                            return Err(io::Error::new(io::ErrorKind::TimedOut, reason).into());
                        }
                    }
                }
                Err(mpsc::error::TrySendError::Closed(())) => {
                    let reason = "bounded_queue_send_closed";
                    record_polymarket_v3_admission_failure(sink, recv_time_utc_ms, reason);
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, reason).into());
                }
            };
            let (message, receipt) = admit_polymarket_orderbook_v3_pending(
                sink,
                recv_time_utc_ms,
                source_sequence_epoch_id,
                pending,
                owned_bytes,
            )?;
            queue_permit.send(SourceIngressPayload::PolymarketOrderbookV3(
                InflightPolymarketOrderbookV3 {
                    message,
                    raw_day_permit: raw_enqueue.raw_day_permit,
                    _source_recv_monotonic_ms: monotonic_ms(run_started),
                    _row_reservation: row_reservation,
                    _byte_reservation: byte_reservation,
                },
            ));
            Ok(receipt)
        }
        SourceEnqueueDecision::Defer(epoch) => {
            let mut deferred = sink
                .source_queue_backpressure
                .deferred_ingress_batches
                .lock()
                .expect("deferred ingress mutex poisoned");
            let (message, receipt) = admit_polymarket_orderbook_v3_pending(
                sink,
                recv_time_utc_ms,
                source_sequence_epoch_id,
                pending,
                owned_bytes,
            )?;
            deferred
                .entry("polymarket".to_owned())
                .or_default()
                .push_back(DeferredIngressBatch::PolymarketOrderbookV3(Box::new(
                    DeferredPolymarketOrderbookV3 {
                        epoch,
                        accepted_ticket: ticket.clone(),
                        message,
                        _source_recv_monotonic_ms: monotonic_ms(run_started),
                        _row_reservation: row_reservation,
                        _byte_reservation: byte_reservation,
                    },
                )));
            Ok(receipt)
        }
    }
}

fn admit_polymarket_orderbook_v3_pending(
    sink: &SourceEventSink,
    recv_time_utc_ms: i64,
    source_sequence_epoch_id: &str,
    pending: PendingPolymarketOrderbookV3Admission,
    owned_bytes: usize,
) -> Result<(
    PolymarketOrderbookV3Message,
    PolymarketOrderbookV3AdmissionReceipt,
)> {
    let collector_admission_time_utc_ms = now_ms();
    if collector_admission_time_utc_ms <= 0 || recv_time_utc_ms > collector_admission_time_utc_ms {
        let reason = format!(
            "invalid_real_admission_clock:received={recv_time_utc_ms}:admission={collector_admission_time_utc_ms}"
        );
        record_polymarket_v3_admission_failure(sink, recv_time_utc_ms, &reason);
        return Err(invalid_data(reason).into());
    }
    let collector_admission_sequence = sink
        .source_queue_backpressure
        .reserve_admission_sequences("polymarket_market", 1)
        .inspect_err(|error| {
            record_polymarket_v3_admission_failure(
                sink,
                recv_time_utc_ms,
                &format!("sequence_allocation_failed:{error}"),
            );
        })?;
    let message = pending
        .admit(
            collector_admission_sequence,
            collector_admission_time_utc_ms,
        )
        .inspect_err(|error| {
            record_polymarket_v3_admission_failure(
                sink,
                recv_time_utc_ms,
                &format!("canonical_admission_failed:{error}"),
            );
        })?;
    debug_assert_eq!(message.owned_bytes(), owned_bytes);
    let market_resolved = polymarket_market_resolved_admission_evidence(&message)?;
    let receipt = PolymarketOrderbookV3AdmissionReceipt {
        source_sequence_epoch_id: source_sequence_epoch_id.to_owned(),
        collector_admission_sequence,
        collector_admission_time_utc_ms,
        market_resolved,
    };
    Ok((message, receipt))
}

pub(super) async fn send_event_with_ticket(
    sink: &SourceEventSink,
    run_started: Instant,
    event: RawEvent,
    ticket: &RawDayAcceptedTicket,
) -> Result<()> {
    match send_event_with_ticket_outcome(sink, run_started, event, ticket).await? {
        EventAdmissionOutcome::Admitted | EventAdmissionOutcome::Deferred => Ok(()),
        EventAdmissionOutcome::Cutoff => Err(invalid_data(
            "accepted Polymarket ticket unexpectedly reached a controlled runtime cutoff",
        )
        .into()),
    }
}

async fn send_event_with_ticket_outcome(
    sink: &SourceEventSink,
    run_started: Instant,
    event: RawEvent,
    ticket: &RawDayAcceptedTicket,
) -> Result<EventAdmissionOutcome> {
    let producer = logical_producer_for_source_kind(&event.source_kind)?;
    if producer != "polymarket" {
        return Err(invalid_data(format!(
            "accepted Polymarket ticket cannot authorize producer {producer}"
        ))
        .into());
    }
    let recv_time_utc_ms = event.recv_time_utc_ms;
    let owned_bytes = owned_event_bytes(&event);
    if owned_bytes > MAX_TYPED_ROW_BYTES {
        let reason = format!(
            "operator_intervention_required:max_typed_row_bytes_exceeded:{}:bytes={owned_bytes}:limit={MAX_TYPED_ROW_BYTES}",
            event.source_kind
        );
        sink.source_queue_backpressure.record_operational_drop(
            &event.source_kind,
            recv_time_utc_ms,
            1,
        );
        record_gap_at(&sink.gaps, recv_time_utc_ms, reason.clone());
        return Err(invalid_data(reason).into());
    }
    let reservation = match sink
        .inflight_budget
        .acquire(1, owned_bytes, BUFFER_ACQUIRE_TIMEOUT)
        .await
    {
        Ok(reservation) => reservation,
        Err(error) => {
            sink.source_queue_backpressure.record_timeout();
            sink.source_queue_backpressure.record_drop(1);
            sink.source_queue_backpressure.record_operational_drop(
                &event.source_kind,
                recv_time_utc_ms,
                1,
            );
            record_gap_at(
                &sink.gaps,
                recv_time_utc_ms,
                format!(
                    "global_inflight_budget_failed:{}:{error}",
                    event.source_kind
                ),
            );
            return Err(error.into());
        }
    };
    let prepared = vec![InflightRawEvent {
        event,
        control: None,
        admission_gaps: Vec::new(),
        raw_day_permit: None,
        _source_recv_monotonic_ms: monotonic_ms(run_started),
        _reservation: Some(reservation),
    }];
    send_polymarket_events_atomically_with_ticket(sink, recv_time_utc_ms, prepared, ticket, || {
        Ok(())
    })
    .await
}

pub(super) fn polymarket_source_sequence_epoch_id(identity: &CollectorIdentity) -> Result<String> {
    let task_sequence = POLYMARKET_SOURCE_EPOCH_COUNTER
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            current.checked_add(1)
        })
        .map(|previous| previous + 1)
        .map_err(|_| invalid_data("Polymarket market source epoch sequence overflow"))?;
    let started_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| invalid_data(format!("system clock precedes Unix epoch: {error}")))?
        .as_nanos();
    let mut hasher = Sha256::new();
    hasher.update(b"polymarket_market_source_sequence_epoch_v1\0");
    hasher.update(identity.host_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(identity.region.as_bytes());
    hasher.update(b"\0");
    hasher.update(std::process::id().to_le_bytes());
    hasher.update(started_ns.to_le_bytes());
    hasher.update(task_sequence.to_le_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}
