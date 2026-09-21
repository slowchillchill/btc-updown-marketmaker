use super::*;

pub(super) const BINANCE_DEPTH_WS: &str = "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms";

static DEPTH_SOURCE_EPOCH_COUNTER: AtomicU64 = AtomicU64::new(0);
pub(super) const BINANCE_DEPTH_SNAPSHOT_MAX_ATTEMPTS: u32 = 3;
const BINANCE_DEPTH_PENDING_MAX_ROWS: usize = 25_000;
const BINANCE_DEPTH_PENDING_MAX_BYTES: usize = 67_108_864;
const BINANCE_HTTP_CONNECT_TIMEOUT_SECS: u64 = 5;
const BINANCE_HTTP_TOTAL_TIMEOUT_SECS: u64 = 15;

pub(super) fn binance_ws_payload(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

pub(super) enum DepthSourceControl {
    Fence {
        kind: DepthFenceKind,
        stop_after_fence: bool,
        resume_after_barrier: Option<oneshot::Receiver<()>>,
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DepthFenceKind {
    HourFinalize,
    ResetContinuity,
}

#[cfg(test)]
pub(super) async fn run_binance_depth_source_with_spec(
    spec: BinanceDepthSpec,
    sink: SourceEventSink,
    collector_identity: CollectorIdentity,
    deadline: Option<Instant>,
    run_started: Instant,
) -> Result<()> {
    run_binance_depth_source_with_spec_and_control(
        spec,
        sink,
        collector_identity,
        deadline,
        run_started,
        None,
    )
    .await
}

pub(super) async fn run_binance_depth_source_with_spec_and_control(
    spec: BinanceDepthSpec,
    sink: SourceEventSink,
    collector_identity: CollectorIdentity,
    deadline: Option<Instant>,
    run_started: Instant,
    mut control_rx: Option<mpsc::Receiver<DepthSourceControl>>,
) -> Result<()> {
    let http_client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(BINANCE_HTTP_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(BINANCE_HTTP_TOTAL_TIMEOUT_SECS))
        .build()?;
    let source_sequence_epoch_id = depth_source_sequence_epoch_id(&collector_identity)?;
    let ingress_sequence = DepthIngressSequenceAllocator::default();
    let snapshot_not_before = DepthSnapshotNotBefore::default();
    let mut connection_generation = 0_u64;
    let mut snapshot_trigger_reason = "bootstrap".to_owned();
    let mut recovery = SourceRecoverySupervisor::new(
        "binance_depth_update",
        u64::try_from(now_ms()).unwrap_or_default(),
    );
    #[cfg(test)]
    recovery.set_wait_override(spec.recovery_wait_override);
    'connections: while !deadline_reached(deadline) {
        if let Some(receiver) = control_rx.as_mut() {
            loop {
                match receiver.try_recv() {
                    Ok(control) => {
                        if handle_depth_source_control(control).await? {
                            break 'connections;
                        }
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        return Err(invalid_data("depth source control channel closed").into());
                    }
                }
            }
        }
        let (depth_stream, _) = match timeout(
            Duration::from_secs(WS_CONNECT_TIMEOUT_SECS),
            connect_async(&spec.websocket_endpoint),
        )
        .await
        {
            Ok(Ok(connected)) => connected,
            Ok(Err(err)) => {
                let reason = if snapshot_trigger_reason == "bootstrap" {
                    format!("connect_error:{err}")
                } else {
                    format!("reconnect_error:{err}")
                };
                if !handle_binance_depth_ws_connect_failure(
                    &sink,
                    &mut recovery,
                    &mut snapshot_trigger_reason,
                    reason,
                    deadline,
                )
                .await
                {
                    break;
                }
                continue;
            }
            Err(_) => {
                let reason = if snapshot_trigger_reason == "bootstrap" {
                    "connect_timeout".to_owned()
                } else {
                    "reconnect_timeout".to_owned()
                };
                if !handle_binance_depth_ws_connect_failure(
                    &sink,
                    &mut recovery,
                    &mut snapshot_trigger_reason,
                    reason,
                    deadline,
                )
                .await
                {
                    break;
                }
                continue;
            }
        };
        connection_generation = connection_generation
            .checked_add(1)
            .ok_or_else(|| invalid_data("Binance depth connection generation overflow"))?;
        let update_exit = run_binance_depth_update_ws_source_with_control(
            depth_stream,
            BinanceDepthUpdateContext {
                http_client: http_client.clone(),
                sink: sink.clone(),
                collector_identity: collector_identity.clone(),
                deadline,
                run_started,
                spec: spec.clone(),
                source_sequence_epoch_id: source_sequence_epoch_id.clone(),
                connection_generation,
                ingress_sequence: ingress_sequence.clone(),
                snapshot_not_before: snapshot_not_before.clone(),
                initial_snapshot_trigger_reason: snapshot_trigger_reason.clone(),
                #[cfg(test)]
                pending_limits: None,
            },
            control_rx.as_mut(),
        )
        .await?;
        match update_exit {
            BinanceDepthUpdateExit::DeadlineReached => break,
            BinanceDepthUpdateExit::Stopped => break,
            BinanceDepthUpdateExit::StreamEnded {
                reason,
                saw_transport,
            } => {
                if saw_transport {
                    recovery.record_business_recovery();
                    sink.source_queue_backpressure
                        .set_recovery_active("binance_depth_update", false);
                }
                if !prepare_binance_depth_reconnect(
                    &sink,
                    &mut recovery,
                    &mut snapshot_trigger_reason,
                    reason,
                    deadline,
                )
                .await
                {
                    break;
                }
            }
        }
    }
    Ok(())
}

pub(super) async fn handle_binance_depth_ws_connect_failure(
    sink: &SourceEventSink,
    recovery: &mut SourceRecoverySupervisor,
    snapshot_trigger_reason: &mut String,
    reason: String,
    deadline: Option<Instant>,
) -> bool {
    prepare_binance_depth_reconnect(sink, recovery, snapshot_trigger_reason, reason, deadline).await
}

pub(super) async fn prepare_binance_depth_reconnect(
    sink: &SourceEventSink,
    recovery: &mut SourceRecoverySupervisor,
    snapshot_trigger_reason: &mut String,
    reason: String,
    deadline: Option<Instant>,
) -> bool {
    *snapshot_trigger_reason = format!("reconnect:{reason}");
    wait_for_source_recovery(recovery, sink, deadline).await
}

#[allow(clippy::too_many_arguments)]
async fn fetch_binance_depth_snapshot_cycle(
    http_client: reqwest::Client,
    collector_identity: CollectorIdentity,
    snapshot_endpoint: String,
    trigger_reason: String,
    source_sequence_epoch_id: String,
    connection_generation: u64,
    ingress_sequence: DepthIngressSequenceAllocator,
    snapshot_not_before: DepthSnapshotNotBefore,
) -> Result<DepthSnapshotCycleOutcome> {
    let mut last_unavailable_reason = "snapshot_attempts_exhausted".to_owned();
    let snapshot_jitter_seed =
        depth_snapshot_jitter_seed(&source_sequence_epoch_id, connection_generation);
    for attempt in 1..=BINANCE_DEPTH_SNAPSHOT_MAX_ATTEMPTS {
        snapshot_not_before.wait().await;
        let snapshot_fetch_start_utc = now_ms();
        let fetched = async {
            let mut response = http_client
                .get(&snapshot_endpoint)
                .send()
                .await
                .map_err(|error| format!("request:{error}"))?;
            let status = response.status();
            let retry_after = binance_snapshot_retry_after(response.headers(), status);
            if let Some(delay) = retry_after {
                snapshot_not_before.defer_for(delay);
            }
            if !status.is_success() {
                let _ = ingress_sequence.allocate()?;
                if matches!(status.as_u16(), 408 | 418 | 429) || status.is_server_error() {
                    return Ok::<_, String>(DepthSnapshotAttempt::Unavailable {
                        reason: format!("http_status:{status}"),
                        retry_after,
                    });
                }
                return Err(format!("http_status_terminal:{status}"));
            }
            let body_limit = u64::try_from(MAX_TYPED_ROW_BYTES)
                .map_err(|_| "snapshot_body_limit_conversion".to_owned())?;
            if response
                .content_length()
                .is_some_and(|length| length > body_limit)
            {
                return Err(format!(
                    "schema:response_body_too_large:limit={MAX_TYPED_ROW_BYTES}"
                ));
            }
            let mut body = Vec::with_capacity(
                response
                    .content_length()
                    .and_then(|length| usize::try_from(length).ok())
                    .unwrap_or_default()
                    .min(MAX_TYPED_ROW_BYTES),
            );
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|error| format!("body:{error}"))?
            {
                let next_length = body
                    .len()
                    .checked_add(chunk.len())
                    .ok_or_else(|| "schema:response_body_length_overflow".to_owned())?;
                if next_length > MAX_TYPED_ROW_BYTES {
                    return Err(format!(
                        "schema:response_body_too_large:limit={MAX_TYPED_ROW_BYTES}"
                    ));
                }
                body.extend_from_slice(&chunk);
            }
            let timestamp_received = now_ms();
            let depth_ingress_sequence = ingress_sequence.allocate()?;
            if body.is_empty() {
                return Err("empty_body".to_owned());
            }
            let mut snapshot_value =
                serde_json::from_slice::<Value>(&body).map_err(|error| format!("json:{error}"))?;
            let snapshot_last_update_id = snapshot_value
                .get("lastUpdateId")
                .and_then(Value::as_u64)
                .filter(|id| *id > 0)
                .ok_or_else(|| "last_update_id_missing".to_owned())?;
            let object = snapshot_value
                .as_object_mut()
                .ok_or_else(|| "snapshot_not_object".to_owned())?;
            object.insert("snapshot_trigger_reason".to_owned(), json!(trigger_reason));
            object.insert(
                "snapshot_fetch_start_utc".to_owned(),
                json!(snapshot_fetch_start_utc),
            );
            object.insert(
                "source_sequence_epoch_id".to_owned(),
                json!(source_sequence_epoch_id),
            );
            object.insert(
                "connection_generation".to_owned(),
                json!(connection_generation),
            );
            object.insert(
                "depth_ingress_sequence".to_owned(),
                json!(depth_ingress_sequence),
            );
            let mut event = raw_event(
                "binance_depth_snapshot",
                "depth_snapshot",
                "BTCUSDT",
                None,
                &snapshot_endpoint,
                &collector_identity,
                snapshot_value,
            );
            event.recv_time_utc_ms = timestamp_received;
            let parsed = parse_depth_snapshot_raw(
                event_envelope(&event, SourceKind::BinanceDepthSnapshot),
                &event.raw_json,
                snapshot_fetch_start_utc,
                timestamp_received,
                snapshot_endpoint.clone(),
            )
            .map_err(|error| format!("schema:{error}"))?;
            let validation = parsed.validate_contract();
            if !validation.is_complete() {
                return Err(format!(
                    "schema:invalid_snapshot_contract:{}",
                    validation.missing_fields.join(",")
                ));
            }
            let event_bytes = owned_event_bytes(&event);
            if event_bytes > MAX_TYPED_ROW_BYTES {
                return Err(format!(
                    "schema:enriched_snapshot_row_too_large:bytes={event_bytes}:limit={MAX_TYPED_ROW_BYTES}"
                ));
            }
            Ok(DepthSnapshotAttempt::Available(Box::new(
                DepthSnapshotRecord {
                    snapshot_last_update_id,
                    snapshot_utc_day: utc_day_string_from_ms(timestamp_received)
                        .unwrap_or_else(|| "unknown".to_owned()),
                    event,
                },
            )))
        }
        .await;
        let mut server_retry_after = None;
        match fetched {
            Ok(DepthSnapshotAttempt::Available(record)) => {
                return Ok(DepthSnapshotCycleOutcome::Available(record));
            }
            Ok(DepthSnapshotAttempt::Unavailable {
                reason,
                retry_after,
            }) => {
                last_unavailable_reason = reason;
                server_retry_after = retry_after;
            }
            Err(error) if error.starts_with("request:") || error.starts_with("body:") => {
                last_unavailable_reason = error;
            }
            Err(error) if depth_snapshot_schema_error(&error) => {
                return Err(io::Error::other(format!(
                    "binance_depth_snapshot_schema_fatal:{error}"
                ))
                .into());
            }
            Err(error) => {
                return Err(io::Error::other(format!(
                    "binance_depth_snapshot_integrity_fatal:{error}"
                ))
                .into());
            }
        }
        if attempt == BINANCE_DEPTH_SNAPSHOT_MAX_ATTEMPTS {
            return Ok(DepthSnapshotCycleOutcome::Unavailable {
                reason: last_unavailable_reason,
                retry_after: server_retry_after,
            });
        }
        let base_retry = Some(retry_delay(
            "binance_depth_snapshot",
            attempt,
            250,
            BINANCE_WS_RECONNECT_MAX_MS,
            snapshot_jitter_seed,
        ));
        if let Some(delay) = server_retry_after.into_iter().chain(base_retry).max() {
            wait_depth_snapshot_delay(delay).await;
        }
    }
    Ok(DepthSnapshotCycleOutcome::Unavailable {
        reason: last_unavailable_reason,
        retry_after: None,
    })
}

pub(super) fn binance_snapshot_retry_after(
    headers: &reqwest::header::HeaderMap,
    status: reqwest::StatusCode,
) -> Option<Duration> {
    if !matches!(status.as_u16(), 418 | 429) {
        return None;
    }
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
}

pub(super) fn start_binance_depth_snapshot_cycle(
    context: &BinanceDepthUpdateContext,
    trigger_reason: impl Into<String>,
) -> DepthSnapshotFuture {
    Box::pin(fetch_binance_depth_snapshot_cycle(
        context.http_client.clone(),
        context.collector_identity.clone(),
        context.spec.snapshot_endpoint.clone(),
        trigger_reason.into(),
        context.source_sequence_epoch_id.clone(),
        context.connection_generation,
        context.ingress_sequence.clone(),
        context.snapshot_not_before.clone(),
    ))
}

#[cfg(test)]
pub(super) async fn fetch_binance_depth_snapshot(
    context: &BinanceDepthUpdateContext,
    trigger_reason: &str,
) -> Result<DepthSnapshotCycleOutcome> {
    start_binance_depth_snapshot_cycle(context, trigger_reason).await
}

fn depth_snapshot_schema_error(error: &str) -> bool {
    error.starts_with("json:")
        || error.starts_with("schema:")
        || error == "empty_body"
        || error == "last_update_id_missing"
        || error == "snapshot_not_object"
}

pub(super) async fn run_binance_depth_update_ws_source_with_control(
    mut stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    context: BinanceDepthUpdateContext,
    mut control_rx: Option<&mut mpsc::Receiver<DepthSourceControl>>,
) -> Result<BinanceDepthUpdateExit> {
    let mut continuity = DepthContinuityState::AwaitingSnapshot;
    #[cfg(test)]
    let mut pending = context
        .pending_limits
        .map(|(max_rows, max_bytes)| DepthPendingBuffer::with_limits(max_rows, max_bytes))
        .unwrap_or_default();
    #[cfg(not(test))]
    let mut pending = DepthPendingBuffer::default();
    let mut snapshot_future = Some(start_binance_depth_snapshot_cycle(
        &context,
        context.initial_snapshot_trigger_reason.clone(),
    ));
    let mut snapshot_unavailable_cycles = 0_u32;
    let mut saw_business = false;
    let mut saw_transport = false;
    let mut frame_inactivity = Box::pin(sleep(depth_frame_inactivity_wait(
        context.deadline,
        context.spec.frame_inactivity_timeout,
    )));
    while !deadline_reached(context.deadline) {
        tokio::select! {
            biased;
            control = async {
                match control_rx.as_deref_mut() {
                    Some(receiver) => receiver.recv().await,
                    None => std::future::pending::<Option<DepthSourceControl>>().await,
                }
            } => {
                let Some(control) = control else {
                    return fail_depth_session_after_pending_release(
                        &context,
                        &mut pending,
                        &mut saw_business,
                        invalid_data("depth source control channel closed").into(),
                    )
                    .await;
                };
                if handle_active_depth_source_control(
                    control,
                    &context,
                    &mut snapshot_future,
                    &mut pending,
                    &mut continuity,
                    &mut saw_business,
                )
                .await?
                {
                    return Ok(BinanceDepthUpdateExit::Stopped);
                }
                reset_depth_frame_inactivity(
                    &mut frame_inactivity,
                    context.deadline,
                    context.spec.frame_inactivity_timeout,
                );
                if matches!(continuity, DepthContinuityState::AwaitingSnapshot)
                    && snapshot_future.is_none()
                {
                    snapshot_future = Some(start_binance_depth_snapshot_cycle(
                        &context,
                        "utc_day_boundary",
                    ));
                }
            }
            snapshot = async {
                match snapshot_future.as_mut() {
                    Some(future) => future.await,
                    None => std::future::pending::<Result<DepthSnapshotCycleOutcome>>().await,
                }
            } => {
                snapshot_future = None;
                let snapshot = match snapshot {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        return fail_depth_session_after_pending_release(
                            &context,
                            &mut pending,
                            &mut saw_business,
                            error,
                        )
                        .await;
                    }
                };
                match snapshot {
                    DepthSnapshotCycleOutcome::Available(record) => {
                        snapshot_unavailable_cycles = 0;
                        let trigger = match admit_depth_snapshot_and_drain(
                            &context,
                            *record,
                            &mut pending,
                            &mut continuity,
                            &mut saw_business,
                        )
                        .await
                        {
                            Ok(trigger) => trigger,
                            Err(error) => {
                                return fail_depth_session_after_pending_release(
                                    &context,
                                    &mut pending,
                                    &mut saw_business,
                                    error,
                                )
                                .await;
                            }
                        };
                        if let Some(trigger) = trigger {
                            snapshot_future = Some(start_binance_depth_snapshot_cycle(
                                &context,
                                trigger,
                            ));
                        }
                    }
                    DepthSnapshotCycleOutcome::Unavailable {
                        reason,
                        retry_after,
                    } => {
                        neutral_release_depth_pending(
                            &context,
                            &mut pending,
                            &mut saw_business,
                        )
                        .await?;
                        continuity = DepthContinuityState::AwaitingSnapshot;
                        snapshot_unavailable_cycles = snapshot_unavailable_cycles.saturating_add(1);
                        let internal_delay = depth_snapshot_retry_delay(
                            snapshot_unavailable_cycles,
                            depth_snapshot_jitter_seed(
                                &context.source_sequence_epoch_id,
                                context.connection_generation,
                            ),
                        );
                        let delay = retry_after
                            .into_iter()
                            .chain(Some(internal_delay))
                            .max()
                            .unwrap_or(internal_delay);
                        snapshot_future = Some(start_binance_depth_snapshot_cycle_after(
                            &context,
                            format!("availability_retry:{reason}"),
                            delay,
                        ));
                    }
                }
            }
            _ = &mut frame_inactivity => {
                if deadline_reached(context.deadline) {
                    if depth_controlled_cutoff_is_active(&context) {
                        pending.discard_all();
                    } else {
                        neutral_release_depth_pending(
                            &context,
                            &mut pending,
                            &mut saw_business,
                        )
                        .await?;
                    }
                    return Ok(BinanceDepthUpdateExit::DeadlineReached);
                }
                neutral_release_depth_pending(&context, &mut pending, &mut saw_business).await?;
                return Ok(BinanceDepthUpdateExit::StreamEnded {
                    reason: "full_frame_inactivity".to_owned(),
                    saw_transport,
                });
            }
            message = stream.next() => match message {
            Some(Ok(Message::Text(text))) => {
                reset_depth_frame_inactivity(
                    &mut frame_inactivity,
                    context.deadline,
                    context.spec.frame_inactivity_timeout,
                );
                let raw_json = text.to_string();
                let recv_time_utc_ms = now_ms();
                let depth_ingress_sequence = match context.ingress_sequence.allocate() {
                    Ok(sequence) => sequence,
                    Err(error) => {
                        return fail_depth_session_after_pending_release(
                            &context,
                            &mut pending,
                            &mut saw_business,
                            invalid_data(format!("binance_depth_ingress_integrity:{error}")).into(),
                        )
                        .await;
                    }
                };
                let value = match serde_json::from_str::<Value>(&raw_json) {
                    Ok(value) => value,
                    Err(error) => {
                        return fail_depth_session_after_pending_release(
                            &context,
                            &mut pending,
                            &mut saw_business,
                            invalid_data(format!(
                                "binance_depth_update_parse_error:parse_depth_update_json:{error}"
                            ))
                            .into(),
                        )
                        .await;
                    }
                };
                let payload = binance_ws_payload(&value);
                let Some(event_type) = payload.get("e").and_then(Value::as_str) else {
                    return fail_depth_session_after_pending_release(
                        &context,
                        &mut pending,
                        &mut saw_business,
                        invalid_data("binance_depth_update_parse_error:missing_string_field:e")
                            .into(),
                    )
                    .await;
                };
                if event_type == "serverShutdown" {
                    neutral_release_depth_pending(
                        &context,
                        &mut pending,
                        &mut saw_business,
                    )
                    .await?;
                    return Ok(BinanceDepthUpdateExit::StreamEnded {
                        reason: "server_shutdown".to_owned(),
                        saw_transport,
                    });
                }
                if event_type != "depthUpdate" {
                    return fail_depth_session_after_pending_release(
                        &context,
                        &mut pending,
                        &mut saw_business,
                        invalid_data(format!(
                            "binance_depth_update_parse_error:unexpected_depth_event_type:{event_type}"
                        ))
                        .into(),
                    )
                    .await;
                }
                let base_event = RawEvent {
                    source_kind: "binance_depth_update".to_owned(),
                    source_event_type: "depth_update".to_owned(),
                    source_identifier: "BTCUSDT".to_owned(),
                    source_ts: payload
                        .get("E")
                        .and_then(Value::as_i64)
                        .map(|ts| ts.to_string()),
                    recv_time_utc_ms,
                    collector_host_id: context.collector_identity.host_id.clone(),
                    collector_region: context.collector_identity.region.clone(),
                    source_endpoint: context.spec.websocket_endpoint.clone(),
                    raw_json: raw_json.clone(),
                    collector_admission: None,
                };
                let parsed = match parse_depth_update_raw(
                    event_envelope(&base_event, SourceKind::BinanceDepthUpdate),
                    &raw_json,
                ) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        return fail_depth_session_after_pending_release(
                            &context,
                            &mut pending,
                            &mut saw_business,
                            invalid_data(format!("binance_depth_update_parse_error:{error}"))
                                .into(),
                        )
                        .await;
                    }
                };
                let validation = parsed.validate_contract();
                if !validation.is_complete() {
                    return fail_depth_session_after_pending_release(
                        &context,
                        &mut pending,
                        &mut saw_business,
                        invalid_data(format!(
                            "binance_depth_update_contract_error:{}",
                            validation.missing_fields.join(",")
                        ))
                        .into(),
                    )
                    .await;
                }
                saw_transport = true;
                let recv_utc_day = utc_day_string_from_ms(recv_time_utc_ms)
                    .unwrap_or_else(|| "unknown".to_owned());
                if matches!(
                    &continuity,
                    DepthContinuityState::Ready { snapshot_utc_day, .. }
                        if snapshot_utc_day != &recv_utc_day
                ) {
                    continuity = DepthContinuityState::AwaitingSnapshot;
                    if snapshot_future.is_none() {
                        snapshot_future = Some(start_binance_depth_snapshot_cycle(
                            &context,
                            "utc_day_boundary",
                        ));
                    }
                }
                match continuity {
                    DepthContinuityState::AwaitingSnapshot => {
                        let owned_bytes = owned_event_bytes(&base_event);
                        let buffered = BufferedDepthUpdate {
                            event: base_event,
                            depth_ingress_sequence,
                            owned_bytes,
                        };
                        if pending.would_exceed(owned_bytes) {
                            neutral_release_depth_pending(
                                &context,
                                &mut pending,
                                &mut saw_business,
                            )
                            .await?;
                            neutral_release_one_depth_update(
                                &context,
                                buffered,
                                &mut saw_business,
                            )
                            .await?;
                            continuity = DepthContinuityState::AwaitingSnapshot;
                            if snapshot_future.is_none() {
                                snapshot_future = Some(start_binance_depth_snapshot_cycle_after(
                                    &context,
                                    "pending_buffer_capacity",
                                    depth_snapshot_retry_delay(
                                        1,
                                        depth_snapshot_jitter_seed(
                                            &context.source_sequence_epoch_id,
                                            context.connection_generation,
                                        ),
                                    ),
                                ));
                            }
                        } else {
                            pending.push(buffered)?;
                        }
                    }
                    DepthContinuityState::Ready { .. } => {
                        let assessment = admit_depth_update(
                            &context,
                            base_event,
                            depth_ingress_sequence,
                            depth_continuity_coordinates(&continuity)?,
                        )
                        .await?;
                        if let Some(assessment) = assessment {
                            saw_business = true;
                            if let Some(trigger) =
                                apply_depth_assessment(&mut continuity, &assessment)?
                            {
                                snapshot_future = Some(start_binance_depth_snapshot_cycle(
                                    &context,
                                    trigger,
                                ));
                            }
                        }
                    }
                }
            }
            Some(Ok(Message::Ping(payload))) => {
                reset_depth_frame_inactivity(
                    &mut frame_inactivity,
                    context.deadline,
                    context.spec.frame_inactivity_timeout,
                );
                if !matches!(timeout(
                    Duration::from_secs(WS_WRITE_TIMEOUT_SECS),
                    stream.send(Message::Pong(payload)),
                )
                .await, Ok(Ok(())))
                {
                    neutral_release_depth_pending(
                        &context,
                        &mut pending,
                        &mut saw_business,
                    )
                    .await?;
                    return Ok(BinanceDepthUpdateExit::StreamEnded {
                        reason: "pong_write_failed".to_owned(),
                        saw_transport,
                    });
                }
                saw_transport = true;
            }
            Some(Ok(Message::Close(frame))) => {
                reset_depth_frame_inactivity(
                    &mut frame_inactivity,
                    context.deadline,
                    context.spec.frame_inactivity_timeout,
                );
                neutral_release_depth_pending(&context, &mut pending, &mut saw_business).await?;
                return Ok(BinanceDepthUpdateExit::StreamEnded {
                    reason: format!("ws_closed:{frame:?}"),
                    saw_transport,
                });
            }
            Some(Ok(Message::Pong(_))) => {
                reset_depth_frame_inactivity(
                    &mut frame_inactivity,
                    context.deadline,
                    context.spec.frame_inactivity_timeout,
                );
                saw_transport = true;
            }
            Some(Ok(Message::Binary(_))) => {
                reset_depth_frame_inactivity(
                    &mut frame_inactivity,
                    context.deadline,
                    context.spec.frame_inactivity_timeout,
                );
                return fail_depth_session_after_pending_release(
                    &context,
                    &mut pending,
                    &mut saw_business,
                    invalid_data("binance_depth_update_binary_payload_integrity_fatal").into(),
                )
                .await;
            }
            Some(Ok(_)) => {
                reset_depth_frame_inactivity(
                    &mut frame_inactivity,
                    context.deadline,
                    context.spec.frame_inactivity_timeout,
                );
            }
            Some(Err(err)) => {
                neutral_release_depth_pending(&context, &mut pending, &mut saw_business).await?;
                return Ok(BinanceDepthUpdateExit::StreamEnded {
                    reason: format!("ws_error:{err}"),
                    saw_transport,
                });
            }
            None => {
                neutral_release_depth_pending(&context, &mut pending, &mut saw_business).await?;
                return Ok(BinanceDepthUpdateExit::StreamEnded {
                    reason: "ws_ended".to_owned(),
                    saw_transport,
                });
            }
        },
        }
    }
    if depth_controlled_cutoff_is_active(&context) {
        pending.discard_all();
    } else {
        neutral_release_depth_pending(&context, &mut pending, &mut saw_business).await?;
    }
    Ok(BinanceDepthUpdateExit::DeadlineReached)
}

fn depth_frame_inactivity_wait(deadline: Option<Instant>, frame_window: Duration) -> Duration {
    deadline_remaining(deadline)
        .map(|remaining| remaining.min(frame_window))
        .unwrap_or(frame_window)
}

fn reset_depth_frame_inactivity(
    timer: &mut Pin<Box<tokio::time::Sleep>>,
    deadline: Option<Instant>,
    frame_window: Duration,
) {
    timer
        .as_mut()
        .reset(tokio::time::Instant::now() + depth_frame_inactivity_wait(deadline, frame_window));
}

pub(super) fn depth_snapshot_retry_delay(cycle: u32, jitter_seed: u64) -> Duration {
    retry_delay(
        "binance_depth_snapshot",
        cycle.min(32),
        BINANCE_WS_RECONNECT_BASE_MS,
        BINANCE_WS_RECONNECT_MAX_MS,
        jitter_seed,
    )
}

fn depth_snapshot_jitter_seed(source_sequence_epoch_id: &str, connection_generation: u64) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(b"binance_depth_snapshot_jitter_v1\0");
    hasher.update(source_sequence_epoch_id.as_bytes());
    hasher.update(connection_generation.to_le_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_le_bytes(bytes)
}

fn start_binance_depth_snapshot_cycle_after(
    context: &BinanceDepthUpdateContext,
    trigger_reason: impl Into<String>,
    delay: Duration,
) -> DepthSnapshotFuture {
    let future = start_binance_depth_snapshot_cycle(context, trigger_reason);
    Box::pin(async move {
        if !delay.is_zero() {
            wait_depth_snapshot_delay(delay).await;
        }
        future.await
    })
}

fn depth_continuity_coordinates(
    continuity: &DepthContinuityState,
) -> Result<(Option<u64>, Option<u64>)> {
    match continuity {
        DepthContinuityState::AwaitingSnapshot => Ok((None, None)),
        DepthContinuityState::Ready {
            snapshot_last_update_id,
            local_u,
            ..
        } => Ok((Some(*snapshot_last_update_id), *local_u)),
    }
}

pub(super) async fn admit_depth_update(
    context: &BinanceDepthUpdateContext,
    mut event: RawEvent,
    depth_ingress_sequence: u64,
    (snapshot_last_update_id, local_u_before): (Option<u64>, Option<u64>),
) -> Result<Option<DepthUpdateAssessment>> {
    let parsed = parse_depth_update_raw(
        event_envelope(&event, SourceKind::BinanceDepthUpdate),
        &event.raw_json,
    )
    .map_err(|error| invalid_data(format!("binance_depth_update_parse_error:{error}")))?;
    let assessment = assess_depth_update(snapshot_last_update_id, local_u_before, &parsed)
        .map_err(invalid_data)?;
    let mut enriched = serde_json::from_str::<Value>(&event.raw_json)?;
    let object = enriched
        .as_object_mut()
        .ok_or_else(|| invalid_data("Binance depth update payload is not an object"))?;
    object.insert(
        "source_sequence_epoch_id".to_owned(),
        json!(context.source_sequence_epoch_id),
    );
    object.insert(
        "connection_generation".to_owned(),
        json!(context.connection_generation),
    );
    object.insert(
        "depth_ingress_sequence".to_owned(),
        json!(depth_ingress_sequence),
    );
    object.insert(
        "snapshot_last_update_id".to_owned(),
        json!(assessment.snapshot_last_update_id),
    );
    object.insert(
        "local_u_before".to_owned(),
        json!(assessment.local_u_before),
    );
    object.insert(
        "stitch_verdict".to_owned(),
        json!(assessment.verdict.as_str()),
    );
    object.insert(
        "stitch_evidence".to_owned(),
        json!(assessment.stitch_evidence),
    );
    object.insert("local_u_after".to_owned(), json!(assessment.local_u_after));
    object.insert("stitch_gap_reason".to_owned(), json!(assessment.gap_reason));
    event.raw_json = enriched.to_string();
    let admission_gaps = match assessment.verdict {
        DepthUpdateVerdict::NotAppliedConfirmedHole => vec![format!(
            "binance_depth_update_stitch_gap:{}",
            assessment
                .gap_reason
                .as_deref()
                .ok_or_else(|| invalid_data("confirmed depth hole omitted gap reason"))?
        )],
        _ => Vec::new(),
    };
    if matches!(
        send_event_with_reservation_outcome(
            &context.sink,
            context.run_started,
            event,
            None,
            admission_gaps,
        )
        .await?,
        EventAdmissionOutcome::Cutoff
    ) {
        return Ok(None);
    }
    Ok(Some(assessment))
}

pub(super) fn apply_depth_assessment(
    continuity: &mut DepthContinuityState,
    assessment: &DepthUpdateAssessment,
) -> Result<Option<String>> {
    match assessment.verdict {
        DepthUpdateVerdict::Applied => {
            let next_local_u = assessment
                .local_u_after
                .ok_or_else(|| invalid_data("applied depth update omitted local_u_after"))?;
            let DepthContinuityState::Ready { local_u, .. } = continuity else {
                return Err(invalid_data("applied depth update without ready snapshot").into());
            };
            *local_u = Some(next_local_u);
            Ok(None)
        }
        DepthUpdateVerdict::NotAppliedStaleDuplicate => Ok(None),
        DepthUpdateVerdict::NotAppliedConfirmedHole => {
            let reason = assessment
                .gap_reason
                .as_deref()
                .ok_or_else(|| invalid_data("confirmed depth hole omitted gap reason"))?;
            *continuity = DepthContinuityState::AwaitingSnapshot;
            Ok(Some(format!("stitch_gap:{reason}")))
        }
        DepthUpdateVerdict::NotAppliedSnapshotUnavailable => {
            *continuity = DepthContinuityState::AwaitingSnapshot;
            Ok(None)
        }
    }
}

async fn neutral_release_one_depth_update(
    context: &BinanceDepthUpdateContext,
    update: BufferedDepthUpdate,
    saw_business: &mut bool,
) -> Result<()> {
    let Some(assessment) = admit_depth_update(
        context,
        update.event,
        update.depth_ingress_sequence,
        (None, None),
    )
    .await?
    else {
        return Ok(());
    };
    if assessment.verdict != DepthUpdateVerdict::NotAppliedSnapshotUnavailable {
        return Err(invalid_data("neutral depth release produced a non-neutral verdict").into());
    }
    *saw_business = true;
    Ok(())
}

async fn neutral_release_depth_pending(
    context: &BinanceDepthUpdateContext,
    pending: &mut DepthPendingBuffer,
    saw_business: &mut bool,
) -> Result<()> {
    while let Some(update) = pending.pop_front()? {
        neutral_release_one_depth_update(context, update, saw_business).await?;
    }
    Ok(())
}

async fn fail_depth_session_after_pending_release(
    context: &BinanceDepthUpdateContext,
    pending: &mut DepthPendingBuffer,
    saw_business: &mut bool,
    terminal_error: Box<dyn Error + Send + Sync>,
) -> Result<BinanceDepthUpdateExit> {
    if let Err(release_error) = neutral_release_depth_pending(context, pending, saw_business).await
    {
        return Err(io::Error::other(format!(
            "binance_depth_terminal_pending_release_failed:terminal={terminal_error}:release={release_error}"
        ))
        .into());
    }
    Err(terminal_error)
}

async fn drain_depth_pending(
    context: &BinanceDepthUpdateContext,
    pending: &mut DepthPendingBuffer,
    continuity: &mut DepthContinuityState,
    saw_business: &mut bool,
) -> Result<Option<String>> {
    while matches!(continuity, DepthContinuityState::Ready { .. }) {
        let Some(update) = pending.pop_front()? else {
            break;
        };
        let Some(assessment) = admit_depth_update(
            context,
            update.event,
            update.depth_ingress_sequence,
            depth_continuity_coordinates(continuity)?,
        )
        .await?
        else {
            pending.discard_all();
            break;
        };
        *saw_business = true;
        if let Some(trigger) = apply_depth_assessment(continuity, &assessment)? {
            return Ok(Some(trigger));
        }
    }
    Ok(None)
}

async fn admit_depth_snapshot_and_drain(
    context: &BinanceDepthUpdateContext,
    record: DepthSnapshotRecord,
    pending: &mut DepthPendingBuffer,
    continuity: &mut DepthContinuityState,
    saw_business: &mut bool,
) -> Result<Option<String>> {
    match send_event_with_admission_outcome(&context.sink, context.run_started, record.event)
        .await?
    {
        EventAdmissionOutcome::Admitted | EventAdmissionOutcome::Deferred => {}
        EventAdmissionOutcome::Cutoff => {
            pending.discard_all();
            *continuity = DepthContinuityState::AwaitingSnapshot;
            return Ok(None);
        }
    }
    *continuity = DepthContinuityState::Ready {
        snapshot_last_update_id: record.snapshot_last_update_id,
        local_u: None,
        snapshot_utc_day: record.snapshot_utc_day,
    };
    drain_depth_pending(context, pending, continuity, saw_business).await
}

async fn handle_active_depth_source_control(
    control: DepthSourceControl,
    context: &BinanceDepthUpdateContext,
    snapshot_future: &mut Option<DepthSnapshotFuture>,
    pending: &mut DepthPendingBuffer,
    continuity: &mut DepthContinuityState,
    saw_business: &mut bool,
) -> Result<bool> {
    let DepthSourceControl::Fence {
        kind,
        stop_after_fence,
        resume_after_barrier,
        reply,
    } = control;
    match kind {
        DepthFenceKind::HourFinalize => {
            if matches!(continuity, DepthContinuityState::Ready { .. }) && !pending.is_empty() {
                return Err(invalid_data(
                    "ready Binance depth continuity retained a pending update at hour fence",
                )
                .into());
            }
        }
        DepthFenceKind::ResetContinuity => {
            drop(snapshot_future.take());
            if depth_controlled_cutoff_is_active(context) {
                pending.discard_all();
            } else {
                neutral_release_depth_pending(context, pending, saw_business).await?;
            }
            *continuity = DepthContinuityState::AwaitingSnapshot;
        }
    }
    let _ = reply.send(Ok(()));
    if let Some(resume) = resume_after_barrier
        && resume.await.is_err()
    {
        drop(snapshot_future.take());
        neutral_release_depth_pending(context, pending, saw_business).await?;
        *continuity = DepthContinuityState::AwaitingSnapshot;
        return Err(invalid_data("depth barrier owner dropped source resume signal").into());
    }
    Ok(stop_after_fence)
}

fn depth_controlled_cutoff_is_active(context: &BinanceDepthUpdateContext) -> bool {
    context
        .sink
        .source_queue_backpressure
        .admission_gate()
        .is_some_and(|gate| gate.unadmitted_runtime_cutoff_is_active())
}

pub(super) async fn handle_depth_source_control(control: DepthSourceControl) -> Result<bool> {
    let DepthSourceControl::Fence {
        kind: _,
        stop_after_fence,
        resume_after_barrier,
        reply,
    } = control;
    let _ = reply.send(Ok(()));
    if let Some(resume) = resume_after_barrier {
        resume
            .await
            .map_err(|_| invalid_data("depth barrier owner dropped source resume signal"))?;
    }
    Ok(stop_after_fence)
}

fn depth_source_sequence_epoch_id(identity: &CollectorIdentity) -> Result<String> {
    let task_sequence = DEPTH_SOURCE_EPOCH_COUNTER
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            current.checked_add(1)
        })
        .map(|previous| previous + 1)
        .map_err(|_| invalid_data("Binance depth source epoch sequence overflow"))?;
    let started_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| invalid_data(format!("system clock precedes Unix epoch: {error}")))?
        .as_nanos();
    let mut hasher = Sha256::new();
    hasher.update(b"binance_depth_source_sequence_epoch_v1\0");
    hasher.update(identity.host_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(identity.region.as_bytes());
    hasher.update(b"\0");
    hasher.update(std::process::id().to_le_bytes());
    hasher.update(started_ns.to_le_bytes());
    hasher.update(task_sequence.to_le_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}

#[derive(Clone)]
pub(super) struct BinanceDepthSpec {
    pub(super) websocket_endpoint: String,
    pub(super) snapshot_endpoint: String,
    pub(super) frame_inactivity_timeout: Duration,
    #[cfg(test)]
    pub(super) recovery_wait_override: Option<Duration>,
}

impl BinanceDepthSpec {
    pub(super) fn production() -> Self {
        Self {
            websocket_endpoint: BINANCE_DEPTH_WS.to_owned(),
            snapshot_endpoint: BINANCE_DEPTH_SNAPSHOT_ENDPOINT.to_owned(),
            frame_inactivity_timeout: Duration::from_secs(30),
            #[cfg(test)]
            recovery_wait_override: None,
        }
    }
}

#[derive(Debug)]
pub(super) enum BinanceDepthUpdateExit {
    DeadlineReached,
    Stopped,
    StreamEnded { reason: String, saw_transport: bool },
}

pub(super) struct BinanceDepthUpdateContext {
    pub(super) http_client: reqwest::Client,
    pub(super) sink: SourceEventSink,
    pub(super) collector_identity: CollectorIdentity,
    pub(super) deadline: Option<Instant>,
    pub(super) run_started: Instant,
    pub(super) spec: BinanceDepthSpec,
    pub(super) source_sequence_epoch_id: String,
    pub(super) connection_generation: u64,
    pub(super) ingress_sequence: DepthIngressSequenceAllocator,
    pub(super) snapshot_not_before: DepthSnapshotNotBefore,
    pub(super) initial_snapshot_trigger_reason: String,
    #[cfg(test)]
    pub(super) pending_limits: Option<(usize, usize)>,
}

// 分段只限制单次定时器跨度，完整 Retry-After 仍按单调时钟累计等待。
async fn wait_depth_snapshot_delay(delay: Duration) {
    let started = Instant::now();
    while let Some(remaining) = delay
        .checked_sub(started.elapsed())
        .filter(|d| !d.is_zero())
    {
        sleep(remaining.min(Duration::from_secs(24 * 60 * 60))).await;
    }
}

#[derive(Clone, Default)]
pub(super) struct DepthSnapshotNotBefore {
    pub(super) deadline: Arc<Mutex<Option<(Instant, Duration)>>>,
}

impl DepthSnapshotNotBefore {
    fn defer_for(&self, delay: Duration) {
        let mut deadline = self
            .deadline
            .lock()
            .expect("depth snapshot not-before mutex poisoned");
        let now = Instant::now();
        if deadline.is_none_or(|(started, duration)| {
            delay > duration.saturating_sub(now.duration_since(started))
        }) {
            *deadline = Some((now, delay));
        }
    }

    async fn wait(&self) {
        loop {
            let deadline = *self
                .deadline
                .lock()
                .expect("depth snapshot not-before mutex poisoned");
            let Some(remaining) = deadline.and_then(|(started, duration)| {
                duration
                    .checked_sub(started.elapsed())
                    .filter(|d| !d.is_zero())
            }) else {
                return;
            };
            wait_depth_snapshot_delay(remaining).await;
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct DepthIngressSequenceAllocator {
    pub(super) last: Arc<AtomicU64>,
}

impl DepthIngressSequenceAllocator {
    pub(super) fn allocate(&self) -> std::result::Result<u64, String> {
        self.last
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map(|previous| previous + 1)
            .map_err(|_| "binance_depth_ingress_sequence_overflow".to_owned())
    }
}

pub(super) struct DepthSnapshotRecord {
    pub(super) snapshot_last_update_id: u64,
    snapshot_utc_day: String,
    pub(super) event: RawEvent,
}

enum DepthSnapshotAttempt {
    Available(Box<DepthSnapshotRecord>),
    Unavailable {
        reason: String,
        retry_after: Option<Duration>,
    },
}

pub(super) enum DepthSnapshotCycleOutcome {
    Available(Box<DepthSnapshotRecord>),
    Unavailable {
        reason: String,
        retry_after: Option<Duration>,
    },
}

type DepthSnapshotFuture = Pin<Box<dyn Future<Output = Result<DepthSnapshotCycleOutcome>> + Send>>;

pub(super) enum DepthContinuityState {
    AwaitingSnapshot,
    Ready {
        snapshot_last_update_id: u64,
        local_u: Option<u64>,
        snapshot_utc_day: String,
    },
}

struct BufferedDepthUpdate {
    event: RawEvent,
    depth_ingress_sequence: u64,
    owned_bytes: usize,
}

pub(super) struct DepthPendingBuffer {
    updates: VecDeque<BufferedDepthUpdate>,
    reserved_bytes: usize,
    max_rows: usize,
    max_bytes: usize,
}

impl Default for DepthPendingBuffer {
    fn default() -> Self {
        Self {
            updates: VecDeque::new(),
            reserved_bytes: 0,
            max_rows: BINANCE_DEPTH_PENDING_MAX_ROWS,
            max_bytes: BINANCE_DEPTH_PENDING_MAX_BYTES,
        }
    }
}

impl DepthPendingBuffer {
    #[cfg(test)]
    pub(super) fn with_limits(max_rows: usize, max_bytes: usize) -> Self {
        Self {
            max_rows,
            max_bytes,
            ..Self::default()
        }
    }

    pub(super) fn would_exceed(&self, additional_bytes: usize) -> bool {
        self.updates.len() >= self.max_rows
            || self
                .reserved_bytes
                .checked_add(additional_bytes)
                .is_none_or(|total| total > self.max_bytes)
    }

    fn push(&mut self, update: BufferedDepthUpdate) -> Result<()> {
        if self.would_exceed(update.owned_bytes) {
            return Err(invalid_data("Binance depth pending buffer cap exceeded").into());
        }
        self.reserved_bytes = self
            .reserved_bytes
            .checked_add(update.owned_bytes)
            .ok_or_else(|| invalid_data("Binance depth pending byte count overflow"))?;
        self.updates.push_back(update);
        Ok(())
    }

    fn pop_front(&mut self) -> Result<Option<BufferedDepthUpdate>> {
        let Some(update) = self.updates.pop_front() else {
            return Ok(None);
        };
        self.reserved_bytes = self
            .reserved_bytes
            .checked_sub(update.owned_bytes)
            .ok_or_else(|| invalid_data("Binance depth pending byte ownership underflow"))?;
        Ok(Some(update))
    }

    fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }

    fn discard_all(&mut self) {
        self.updates.clear();
        self.reserved_bytes = 0;
    }
}

#[cfg(test)]
mod delay_tests {
    use super::*;

    #[tokio::test]
    async fn binance_depth_large_retry_waits_remain_pending_and_cancellable() {
        for seconds in [4 * 24 * 60 * 60, u64::MAX] {
            let delay = Duration::from_secs(seconds);
            let gate = DepthSnapshotNotBefore::default();
            gate.defer_for(delay);
            gate.defer_for(Duration::from_secs(1));
            assert_eq!(gate.deadline.lock().unwrap().unwrap().1, delay);
            assert!(
                timeout(Duration::from_millis(10), gate.wait())
                    .await
                    .is_err()
            );
            assert!(
                timeout(Duration::from_millis(10), wait_depth_snapshot_delay(delay))
                    .await
                    .is_err()
            );
        }
    }
}
