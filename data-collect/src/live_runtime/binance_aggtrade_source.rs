use super::*;

pub(super) const BINANCE_AGGTRADE_WS: &str = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
static AGGTRADE_SOURCE_EPOCH_COUNTER: AtomicU64 = AtomicU64::new(0);
const BINANCE_AGGTRADE_JSON_VALUE_STAGING_MULTIPLIER: usize = 128;
const BINANCE_AGGTRADE_INVALID_SERIALIZATION_STAGING_MAX_BYTES: usize = 4 * MAX_TYPED_ROW_BYTES;

pub(super) enum AggTradeSourceControl {
    Fence {
        stop_after_fence: bool,
        resume_after_barrier: Option<oneshot::Receiver<()>>,
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
}

pub(super) struct BinanceAggTradeRecovery {
    pub(super) websocket_dedupe: BinanceRecentIdWindow,
}

pub(super) async fn run_binance_ws_source(
    recovery: BinanceAggTradeRecovery,
    spec: BinanceWsSpec,
    sink: SourceEventSink,
    collector_identity: CollectorIdentity,
    deadline: Option<Instant>,
    run_started: Instant,
    mut control_rx: mpsc::Receiver<AggTradeSourceControl>,
) -> Result<()> {
    let source_sequence_epoch_id = aggtrade_source_sequence_epoch_id(&collector_identity)?;
    let ingress_sequences = BinanceAggTradeIngressSequenceAllocator::default();
    let mut websocket_dedupe = recovery.websocket_dedupe;
    let mut connection_generation = 0_u64;
    let mut recovery = SourceRecoverySupervisor::new(
        "binance_aggtrade",
        u64::try_from(now_ms()).unwrap_or_default(),
    );
    #[cfg(test)]
    recovery.set_wait_override(spec.recovery_wait_override);

    'connections: while !deadline_reached(deadline) {
        while let Ok(control) = control_rx.try_recv() {
            if handle_aggtrade_source_control(control).await? {
                return Ok(());
            }
        }

        let connect_timeout = deadline_remaining(deadline)
            .map(|remaining| remaining.min(Duration::from_secs(WS_CONNECT_TIMEOUT_SECS)))
            .unwrap_or_else(|| Duration::from_secs(WS_CONNECT_TIMEOUT_SECS));
        if connect_timeout.is_zero() {
            break;
        }
        let websocket_config = WebSocketConfig::default()
            .max_message_size(Some(BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES))
            .max_frame_size(Some(BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES));
        let (mut stream, _) = match timeout(
            connect_timeout,
            connect_async_with_config(&spec.url, Some(websocket_config), false),
        )
        .await
        {
            Ok(Ok(connected)) => connected,
            Ok(Err(_)) => {
                if !wait_for_source_recovery(&mut recovery, &sink, deadline).await {
                    break;
                }
                continue;
            }
            Err(_) => {
                if deadline_reached(deadline)
                    || !wait_for_source_recovery(&mut recovery, &sink, deadline).await
                {
                    break;
                }
                continue;
            }
        };
        connection_generation = connection_generation.checked_add(1).ok_or_else(|| {
            sink.source_queue_backpressure
                .record_operational_drop(&spec.source_kind, now_ms(), 1);
            record_gap(
                &sink.gaps,
                "binance_aggtrade_connection_generation_terminal:overflow".to_owned(),
            );
            invalid_data("binance aggTrade connection generation overflow")
        })?;
        let current_connection_generation = connection_generation;

        let frame_inactivity_window = Duration::from_secs(spec.frame_inactivity_timeout_secs);
        let mut frame_inactivity_deadline = Instant::now() + frame_inactivity_window;
        let _reconnect_reason = loop {
            if deadline_reached(deadline) {
                break 'connections;
            }
            let frame_inactivity_remaining =
                frame_inactivity_deadline.saturating_duration_since(Instant::now());
            if frame_inactivity_remaining.is_zero() {
                break "frame_inactivity_timeout".to_owned();
            }
            let read_timeout = deadline_remaining(deadline)
                .map(|remaining| remaining.min(frame_inactivity_remaining))
                .unwrap_or(frame_inactivity_remaining);
            let input = tokio::select! {
                biased;
                control = control_rx.recv() => {
                    match control {
                        Some(control) => {
                            if handle_aggtrade_source_control(control).await? {
                                return Ok(());
                            }
                            frame_inactivity_deadline = Instant::now() + frame_inactivity_window;
                            continue;
                        }
                        None => {
                            return Err(invalid_data(
                                "aggTrade source control channel closed",
                            ).into());
                        }
                    }
                }
                message = timeout(read_timeout, stream.next()) => message,
            };

            if matches!(&input, Ok(Some(Ok(_)))) {
                frame_inactivity_deadline = Instant::now() + frame_inactivity_window;
                recovery.record_business_recovery();
                sink.source_queue_backpressure
                    .set_recovery_active(&spec.source_kind, false);
            }

            match input {
                Ok(Some(Ok(Message::Text(text)))) => {
                    let recv_time_utc_ms = now_ms();
                    let ingress_coordinate = allocate_aggtrade_websocket_ingress(
                        &ingress_sequences,
                        &sink,
                        recv_time_utc_ms,
                    )?;
                    let websocket_staging_reservation = sink
                        .inflight_budget
                        .acquire(
                            1,
                            aggtrade_websocket_staging_bytes(text.len(), true)?,
                            BUFFER_ACQUIRE_TIMEOUT,
                        )
                        .await?;
                    let raw_bytes = text.as_bytes().to_vec();
                    let value = match serde_json::from_str::<Value>(text.as_str()) {
                        Ok(value) => value,
                        Err(error) => {
                            drop(text);
                            emit_aggtrade_websocket_invalid(
                                raw_bytes,
                                recv_time_utc_ms,
                                ingress_coordinate,
                                BinanceAggTradeInvalidReason::InvalidJson,
                                error.to_string(),
                                &sink,
                                &collector_identity,
                                &spec,
                                &source_sequence_epoch_id,
                                current_connection_generation,
                                run_started,
                                websocket_staging_reservation,
                            )
                            .await?;
                            continue;
                        }
                    };
                    drop(text);
                    let (payload, stream_name) = match (value.get("stream"), value.get("data")) {
                        (None, None) => (&value, "btcusdt@aggTrade"),
                        (Some(Value::String(stream_name)), Some(payload)) => {
                            (payload, stream_name.as_str())
                        }
                        _ => {
                            drop(value);
                            emit_aggtrade_websocket_invalid(
                                raw_bytes,
                                recv_time_utc_ms,
                                ingress_coordinate,
                                BinanceAggTradeInvalidReason::InvalidPayload,
                                "combined_stream_requires_string_stream_and_data",
                                &sink,
                                &collector_identity,
                                &spec,
                                &source_sequence_epoch_id,
                                current_connection_generation,
                                run_started,
                                websocket_staging_reservation,
                            )
                            .await?;
                            continue;
                        }
                    };
                    let Some(event_type) = payload.get("e").and_then(Value::as_str) else {
                        drop(value);
                        emit_aggtrade_websocket_invalid(
                            raw_bytes,
                            recv_time_utc_ms,
                            ingress_coordinate,
                            BinanceAggTradeInvalidReason::InvalidPayload,
                            "missing_event_type",
                            &sink,
                            &collector_identity,
                            &spec,
                            &source_sequence_epoch_id,
                            current_connection_generation,
                            run_started,
                            websocket_staging_reservation,
                        )
                        .await?;
                        continue;
                    };
                    let event_type = event_type.to_owned();
                    if event_type == "serverShutdown" {
                        break "server_shutdown".to_owned();
                    }
                    if event_type != spec.default_event_type {
                        drop(value);
                        emit_aggtrade_websocket_invalid(
                            raw_bytes,
                            recv_time_utc_ms,
                            ingress_coordinate,
                            BinanceAggTradeInvalidReason::InvalidPayload,
                            format!("unexpected_event_type:{event_type}"),
                            &sink,
                            &collector_identity,
                            &spec,
                            &source_sequence_epoch_id,
                            current_connection_generation,
                            run_started,
                            websocket_staging_reservation,
                        )
                        .await?;
                        continue;
                    }
                    let websocket = match BinanceWsAggTrade::from_wire(
                        payload,
                        stream_name,
                        recv_time_utc_ms,
                        ingress_coordinate.clone(),
                    ) {
                        Ok(websocket) => websocket,
                        Err(error) => {
                            drop(value);
                            emit_aggtrade_websocket_invalid(
                                raw_bytes,
                                recv_time_utc_ms,
                                ingress_coordinate,
                                BinanceAggTradeInvalidReason::InvalidPayload,
                                error,
                                &sink,
                                &collector_identity,
                                &spec,
                                &source_sequence_epoch_id,
                                current_connection_generation,
                                run_started,
                                websocket_staging_reservation,
                            )
                            .await?;
                            continue;
                        }
                    };
                    drop(value);
                    drop(raw_bytes);
                    let native_id = websocket.shared.aggregate_trade_id;
                    let payload_conflict = match websocket_dedupe
                        .observe(native_id, websocket.source_native_payload_json.as_bytes())
                    {
                        BinanceIdDecision::Duplicate => {
                            sink.source_queue_backpressure.record_structural_dedupe(
                                &spec.source_kind,
                                websocket.timestamp_received,
                                1,
                            );
                            continue;
                        }
                        BinanceIdDecision::Conflict => true,
                        BinanceIdDecision::OutsideWindow | BinanceIdDecision::Unique => false,
                    };
                    let output = BinanceAggTradeOutput::Websocket(websocket);
                    let event = binance_aggtrade_raw_event(
                        &output,
                        &collector_identity,
                        &spec,
                        &source_sequence_epoch_id,
                        current_connection_generation,
                    )?;
                    let bytes = owned_event_bytes(&event);
                    if bytes > MAX_TYPED_ROW_BYTES {
                        sink.source_queue_backpressure.record_operational_drop(
                            &spec.source_kind,
                            recv_time_utc_ms,
                            1,
                        );
                        return Err(invalid_data(format!(
                            "operator_intervention_required:binance_aggtrade_output_row_too_large:bytes={bytes}:limit={MAX_TYPED_ROW_BYTES}"
                        ))
                        .into());
                    }
                    let mut reservation = websocket_staging_reservation;
                    reservation.shrink_to(1, bytes)?;
                    let admission_gaps = payload_conflict
                        .then(|| format!("binance_aggtrade_payload_conflict:id={native_id}"))
                        .into_iter()
                        .collect();
                    send_event_with_reservation_outcome(
                        &sink,
                        run_started,
                        event,
                        Some(reservation),
                        admission_gaps,
                    )
                    .await?;
                }
                Ok(Some(Ok(Message::Binary(payload)))) => {
                    let recv_time_utc_ms = now_ms();
                    let coordinate = allocate_aggtrade_websocket_ingress(
                        &ingress_sequences,
                        &sink,
                        recv_time_utc_ms,
                    )?;
                    let websocket_staging_reservation = sink
                        .inflight_budget
                        .acquire(
                            1,
                            aggtrade_websocket_staging_bytes(payload.len(), false)?,
                            BUFFER_ACQUIRE_TIMEOUT,
                        )
                        .await?;
                    emit_aggtrade_websocket_invalid(
                        payload.to_vec(),
                        recv_time_utc_ms,
                        coordinate,
                        BinanceAggTradeInvalidReason::InvalidPayload,
                        "unexpected_binary_application_message",
                        &sink,
                        &collector_identity,
                        &spec,
                        &source_sequence_epoch_id,
                        current_connection_generation,
                        run_started,
                        websocket_staging_reservation,
                    )
                    .await?;
                }
                Ok(Some(Ok(Message::Ping(payload)))) => {
                    let write_timeout = deadline_remaining(deadline)
                        .map(|remaining| remaining.min(Duration::from_secs(WS_WRITE_TIMEOUT_SECS)))
                        .unwrap_or_else(|| Duration::from_secs(WS_WRITE_TIMEOUT_SECS));
                    if write_timeout.is_zero() {
                        break "source_deadline_before_pong".to_owned();
                    }
                    match timeout(write_timeout, stream.send(Message::Pong(payload))).await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => break format!("pong_send_error:{error}"),
                        Err(_) => break "pong_send_timeout".to_owned(),
                    }
                }
                Ok(Some(Ok(Message::Close(frame)))) => break format!("closed:{frame:?}"),
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(TungsteniteError::Capacity(CapacityError::MessageTooLong {
                    size,
                    max_size,
                })))) => {
                    let recv_time_utc_ms = now_ms();
                    let coordinate = allocate_aggtrade_websocket_ingress(
                        &ingress_sequences,
                        &sink,
                        recv_time_utc_ms,
                    )?;
                    let mut oversize_reservation = sink
                        .inflight_budget
                        .acquire(1, MAX_TYPED_ROW_BYTES, BUFFER_ACQUIRE_TIMEOUT)
                        .await?;
                    let observed_bytes_at_least = u64::try_from(size)
                        .map_err(|_| invalid_data("aggTrade oversize size overflow"))?;
                    let capture = BinanceAggTradeRawCapture::oversize_not_captured(
                        observed_bytes_at_least,
                        None,
                    )
                    .map_err(invalid_data)?;
                    let invalid = BinanceAggTradeInvalidInput::new(
                        BinanceAggTradeInvalidTransport::Websocket,
                        coordinate,
                        recv_time_utc_ms,
                        spec.url.clone(),
                        BinanceAggTradeInvalidReason::BodyTooLarge,
                        format!("observed={size}:configured_max={max_size}"),
                        capture,
                    )
                    .map_err(invalid_data)?;
                    let event = binance_aggtrade_invalid_raw_event(
                        &invalid,
                        &collector_identity,
                        &spec,
                        &source_sequence_epoch_id,
                        current_connection_generation,
                    )?;
                    drop(invalid);
                    let bytes = owned_event_bytes(&event);
                    oversize_reservation.shrink_to(1, bytes)?;
                    send_event_with_reservation(
                        &sink,
                        run_started,
                        event,
                        Some(oversize_reservation),
                    )
                    .await?;
                    break "oversize_application_message".to_owned();
                }
                Ok(Some(Err(error))) => break format!("error:{error}"),
                Ok(None) => break "stream_ended".to_owned(),
                Err(_) if deadline_reached(deadline) => break 'connections,
                Err(_) => break "frame_inactivity_timeout".to_owned(),
            }
        };

        if !wait_for_source_recovery(&mut recovery, &sink, deadline).await {
            break;
        }
    }
    Ok(())
}

async fn handle_aggtrade_source_control(control: AggTradeSourceControl) -> Result<bool> {
    let AggTradeSourceControl::Fence {
        stop_after_fence,
        resume_after_barrier,
        reply,
    } = control;
    let _ = reply.send(Ok(()));
    if let Some(resume) = resume_after_barrier {
        resume
            .await
            .map_err(|_| invalid_data("aggTrade barrier owner dropped source resume signal"))?;
    }
    Ok(stop_after_fence)
}
pub(super) fn binance_aggtrade_raw_event(
    output: &BinanceAggTradeOutput,
    collector_identity: &CollectorIdentity,
    spec: &BinanceWsSpec,
    source_sequence_epoch_id: &str,
    connection_generation: u64,
) -> Result<RawEvent> {
    if connection_generation == 0 {
        return Err(invalid_data("aggTrade connection generation is zero").into());
    }
    let shared = output.shared();
    let BinanceAggTradeOutput::Websocket(websocket) = output;
    let websocket_ingress_sequence = websocket_ingress_sequence(&websocket.ingress_coordinate)?;
    let source_native_payload_json = output.source_native_payload_json().map_err(invalid_data)?;
    Ok(RawEvent {
        source_kind: spec.source_kind.clone(),
        source_event_type: spec.default_event_type.clone(),
        source_identifier: websocket.symbol.clone(),
        source_ts: Some(shared.trade_time_ms.to_string()),
        recv_time_utc_ms: output.timestamp_received(),
        collector_host_id: collector_identity.host_id.clone(),
        collector_region: collector_identity.region.clone(),
        source_endpoint: spec.url.clone(),
        raw_json: json!({
            "record_kind": "trade",
            "timestamp_received": output.timestamp_received(),
            "rest_backfill_timestamp_received": Value::Null,
            "ingest_source": output.ingest_source().as_str(),
            "event_type": websocket.event_type,
            "event_time": websocket.event_time_ms,
            "trade_time": shared.trade_time_ms,
            "symbol": websocket.symbol,
            "aggregate_trade_id": shared.aggregate_trade_id,
            "price": shared.price,
            "quantity": shared.quantity,
            "first_trade_id": shared.first_trade_id,
            "last_trade_id": shared.last_trade_id,
            "buyer_is_maker": shared.buyer_is_maker,
            "stream": websocket.stream,
            "ws_ignore": websocket.ws_ignore,
            "rest_was_best_price_match": Value::Null,
            "source_native_payload_json": source_native_payload_json,
            "source_sequence_epoch_id": source_sequence_epoch_id,
            "connection_generation": connection_generation,
            "websocket_ingress_sequence": websocket_ingress_sequence,
            "rest_response_ingress_sequence": Value::Null,
            "rest_response_item_index": Value::Null,
        })
        .to_string(),
        collector_admission: None,
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: spec.source_kind.clone(),
    })
}

fn websocket_ingress_sequence(coordinate: &BinanceAggTradeIngressCoordinate) -> Result<u64> {
    match coordinate {
        BinanceAggTradeIngressCoordinate::Websocket { sequence } if *sequence != 0 => Ok(*sequence),
        _ => Err(invalid_data("aggTrade websocket ingress coordinate is invalid").into()),
    }
}

#[derive(Serialize)]
struct BinanceAggTradeInvalidEvidenceEnvelope<'a> {
    capture_limit_bytes: u64,
    declared_content_length: Option<u64>,
    endpoint: &'a str,
    evidence_schema: &'static str,
    invalid_reason_code: &'a str,
    observed_bytes_at_least: u64,
    payload_base64: Option<&'a str>,
    payload_capture_status: &'a str,
    payload_encoding: Option<&'a str>,
    payload_length_bytes: Option<u64>,
    payload_sha256: Option<&'a str>,
    receive_time_utc_ms: i64,
    transport: &'a str,
}

#[derive(Serialize)]
struct BinanceAggTradeInvalidRawPayload<'a> {
    connection_generation: u64,
    ingest_source: &'a str,
    invalid_reason_code: &'a str,
    invalid_reason_detail: Option<&'a str>,
    payload_capture_status: &'a str,
    payload_encoding: Option<&'a str>,
    payload_length_bytes: Option<u64>,
    payload_sha256: Option<&'a str>,
    record_kind: &'static str,
    rest_response_ingress_sequence: Option<u64>,
    rest_response_item_index: Option<u64>,
    source_native_payload_json: &'a str,
    source_sequence_epoch_id: &'a str,
    timestamp_received: i64,
    websocket_ingress_sequence: Option<u64>,
}

pub(super) fn binance_aggtrade_invalid_raw_event(
    invalid: &BinanceAggTradeInvalidInput,
    collector_identity: &CollectorIdentity,
    spec: &BinanceWsSpec,
    source_sequence_epoch_id: &str,
    connection_generation: u64,
) -> Result<RawEvent> {
    if connection_generation == 0 {
        return Err(invalid_data("aggTrade invalid connection generation is zero").into());
    }
    let websocket_ingress_sequence = Some(websocket_ingress_sequence(&invalid.ingress_coordinate)?);
    let (payload_encoding, payload_length_bytes, payload_sha256, payload_base64) = match invalid
        .raw_capture
        .status
    {
        crate::binance_aggtrade::BinanceAggTradeRawCaptureStatus::Complete => {
            let length = u64::try_from(invalid.raw_capture.bytes.len())
                .map_err(|_| invalid_data("aggTrade invalid payload length overflow"))?;
            let digest = format!("{:x}", Sha256::digest(&invalid.raw_capture.bytes));
            (
                Some("base64"),
                Some(length),
                Some(digest),
                Some(base64::engine::general_purpose::STANDARD.encode(&invalid.raw_capture.bytes)),
            )
        }
        crate::binance_aggtrade::BinanceAggTradeRawCaptureStatus::OversizeNotCaptured => (
            None,
            invalid.raw_capture.declared_content_length,
            None,
            None,
        ),
    };
    let capture_status = invalid.raw_capture.status.as_str();
    let source_native_payload_json =
        serde_json::to_string(&BinanceAggTradeInvalidEvidenceEnvelope {
            capture_limit_bytes: invalid.raw_capture.limit_bytes,
            declared_content_length: invalid.raw_capture.declared_content_length,
            endpoint: &invalid.endpoint,
            evidence_schema: "binance_aggtrade_invalid_evidence_v1",
            invalid_reason_code: invalid.reason.as_str(),
            observed_bytes_at_least: invalid.raw_capture.observed_bytes_at_least,
            payload_base64: payload_base64.as_deref(),
            payload_capture_status: capture_status,
            payload_encoding,
            payload_length_bytes,
            payload_sha256: payload_sha256.as_deref(),
            receive_time_utc_ms: invalid.timestamp_received,
            transport: invalid.transport.as_str(),
        })
        .map_err(|error| invalid_data(format!("serialize aggTrade invalid evidence:{error}")))?;
    drop(payload_base64);
    let invalid_reason_detail =
        (!invalid.reason_detail.is_empty()).then_some(invalid.reason_detail.as_str());
    let raw_json = serde_json::to_string(&BinanceAggTradeInvalidRawPayload {
        connection_generation,
        ingest_source: invalid.transport.as_str(),
        invalid_reason_code: invalid.reason.as_str(),
        invalid_reason_detail,
        payload_capture_status: capture_status,
        payload_encoding,
        payload_length_bytes,
        payload_sha256: payload_sha256.as_deref(),
        record_kind: "invalid_input",
        rest_response_ingress_sequence: None,
        rest_response_item_index: None,
        source_native_payload_json: &source_native_payload_json,
        source_sequence_epoch_id,
        timestamp_received: invalid.timestamp_received,
        websocket_ingress_sequence,
    })
    .map_err(|error| invalid_data(format!("serialize aggTrade invalid row:{error}")))?
    .into_boxed_str()
    .into_string();
    Ok(RawEvent {
        source_kind: spec.source_kind.clone(),
        source_event_type: "invalid_input".to_owned(),
        source_identifier: format!("{}:{}", invalid.transport.as_str(), invalid.reason.as_str()),
        source_ts: None,
        recv_time_utc_ms: invalid.timestamp_received,
        collector_host_id: collector_identity.host_id.clone(),
        collector_region: collector_identity.region.clone(),
        source_endpoint: invalid.endpoint.clone(),
        raw_json,
        collector_admission: None,
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: spec.source_kind.clone(),
    })
}

pub(super) fn allocate_aggtrade_websocket_ingress(
    allocator: &BinanceAggTradeIngressSequenceAllocator,
    sink: &SourceEventSink,
    recv_time_utc_ms: i64,
) -> Result<BinanceAggTradeIngressCoordinate> {
    allocator.allocate_websocket().map_err(|error| {
        sink.source_queue_backpressure.record_operational_drop(
            "binance_aggtrade",
            recv_time_utc_ms,
            1,
        );
        record_gap(
            &sink.gaps,
            format!("binance_aggtrade_ingress_sequence_terminal:{error}"),
        );
        invalid_data(error.to_string()).into()
    })
}

pub(super) fn binance_aggtrade_websocket_invalid_input(
    raw_bytes: Vec<u8>,
    recv_time_utc_ms: i64,
    endpoint: &str,
    ingress_coordinate: BinanceAggTradeIngressCoordinate,
    reason: BinanceAggTradeInvalidReason,
    reason_detail: impl Into<String>,
) -> Result<BinanceAggTradeInvalidInput> {
    let capture = BinanceAggTradeRawCapture::complete(raw_bytes, None).map_err(invalid_data)?;
    BinanceAggTradeInvalidInput::new(
        BinanceAggTradeInvalidTransport::Websocket,
        ingress_coordinate,
        recv_time_utc_ms,
        endpoint.to_owned(),
        reason,
        reason_detail,
        capture,
    )
    .map_err(|error| invalid_data(error).into())
}

pub(super) fn aggtrade_websocket_staging_bytes(
    payload_bytes: usize,
    parse_json: bool,
) -> Result<usize> {
    let input_and_capture = payload_bytes
        .checked_mul(2)
        .ok_or_else(|| invalid_data("aggTrade websocket input staging size overflow"))?;
    let json_value = if parse_json {
        payload_bytes
            .checked_mul(BINANCE_AGGTRADE_JSON_VALUE_STAGING_MULTIPLIER)
            .ok_or_else(|| invalid_data("aggTrade websocket JSON staging size overflow"))?
    } else {
        0
    };
    input_and_capture
        .checked_add(json_value)
        .and_then(|bytes| {
            bytes.checked_add(BINANCE_AGGTRADE_INVALID_SERIALIZATION_STAGING_MAX_BYTES)
        })
        .ok_or_else(|| invalid_data("aggTrade websocket total staging size overflow").into())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn emit_aggtrade_websocket_invalid(
    raw_bytes: Vec<u8>,
    recv_time_utc_ms: i64,
    ingress_coordinate: BinanceAggTradeIngressCoordinate,
    reason: BinanceAggTradeInvalidReason,
    reason_detail: impl Into<String>,
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    spec: &BinanceWsSpec,
    source_sequence_epoch_id: &str,
    connection_generation: u64,
    run_started: Instant,
    mut staging_reservation: InflightReservation,
) -> Result<()> {
    let invalid = binance_aggtrade_websocket_invalid_input(
        raw_bytes,
        recv_time_utc_ms,
        &spec.url,
        ingress_coordinate,
        reason,
        reason_detail,
    )?;
    let event = binance_aggtrade_invalid_raw_event(
        &invalid,
        collector_identity,
        spec,
        source_sequence_epoch_id,
        connection_generation,
    )?;
    drop(invalid);
    let bytes = owned_event_bytes(&event);
    if bytes > MAX_TYPED_ROW_BYTES {
        return Err(invalid_data(format!(
            "operator_intervention_required:binance_aggtrade_invalid_row_too_large:bytes={bytes}:limit={MAX_TYPED_ROW_BYTES}"
        ))
        .into());
    }
    staging_reservation.shrink_to(1, bytes)?;
    send_event_with_reservation(sink, run_started, event, Some(staging_reservation)).await
}

fn aggtrade_source_sequence_epoch_id(identity: &CollectorIdentity) -> Result<String> {
    let task_sequence = AGGTRADE_SOURCE_EPOCH_COUNTER
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            current.checked_add(1)
        })
        .map(|previous| previous + 1)
        .map_err(|_| invalid_data("binance aggTrade source epoch sequence overflow"))?;
    let started_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| invalid_data(format!("system clock precedes Unix epoch: {error}")))?
        .as_nanos();
    let mut hasher = Sha256::new();
    hasher.update(b"binance_aggtrade_source_sequence_epoch_v1\0");
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
pub(super) struct BinanceWsSpec {
    pub(super) source_kind: String,
    pub(super) default_event_type: String,
    pub(super) url: String,
    pub(super) frame_inactivity_timeout_secs: u64,
    #[cfg(test)]
    pub(super) recovery_wait_override: Option<Duration>,
}
