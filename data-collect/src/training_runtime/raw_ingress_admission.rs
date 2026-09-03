use super::*;

fn source_admission_authority(event: &RawEvent) -> Option<&'static str> {
    match event.source_kind.as_str() {
        "binance_aggtrade" => "binance_aggtrade",
        "binance_depth_snapshot" | "binance_depth_update" => "binance_depth",
        "polymarket_market_metadata" => "polymarket_market_metadata",
        _ => return None,
    }
    .into()
}

fn record_source_admission_integrity(
    event: &RawEvent,
    sink: &SourceEventSink,
    admission_source: &str,
    error: &str,
) {
    sink.source_queue_backpressure.record_operational_drop(
        &event.source_kind,
        event.recv_time_utc_ms,
        1,
    );
    record_gap(
        &sink.gaps,
        format!("{admission_source}_admission_integrity:{error}"),
    );
}

fn validate_source_admission_at(
    event: &RawEvent,
    sink: &SourceEventSink,
    time_utc_ms: i64,
) -> Result<Option<&'static str>> {
    let Some(admission_source) = source_admission_authority(event) else {
        return Ok(None);
    };
    if event.collector_admission.is_some() {
        let error = format!("{admission_source} event admitted more than once");
        record_source_admission_integrity(event, sink, admission_source, &error);
        return Err(invalid_data(error).into());
    }
    if time_utc_ms <= 0 {
        let error = format!("{admission_source} admission time is not positive");
        record_source_admission_integrity(event, sink, admission_source, &error);
        return Err(invalid_data(error).into());
    }
    if admission_source == "polymarket_market_metadata" {
        let row = serde_json::from_str::<Value>(&event.raw_json).map_err(|error| {
            let error =
                format!("Polymarket metadata draft JSON is invalid before admission:{error}");
            record_source_admission_integrity(event, sink, admission_source, &error);
            invalid_data(error)
        })?;
        let future_causal_time = [
            ("recv_time_utc_ms", Some(event.recv_time_utc_ms)),
            (
                "timestamp_received",
                row.get("timestamp_received").and_then(Value::as_i64),
            ),
            (
                "fixed_identity_verified_at_utc",
                row.get("fixed_identity_verified_at_utc")
                    .and_then(Value::as_i64),
            ),
            (
                "source_evidence_received_at_utc",
                row.get("source_evidence_received_at_utc")
                    .and_then(Value::as_i64),
            ),
            (
                "row_evidence_validation_completed_at_utc",
                row.get("row_evidence_validation_completed_at_utc")
                    .and_then(Value::as_i64),
            ),
            (
                "metadata_fetch_start_utc",
                row.get("metadata_fetch_start_utc").and_then(Value::as_i64),
            ),
            (
                "lifecycle_applied_at_utc",
                row.get("lifecycle_applied_at_utc").and_then(Value::as_i64),
            ),
            (
                "runtime_role_applied_at_utc",
                row.get("runtime_role_applied_at_utc")
                    .and_then(Value::as_i64),
            ),
        ]
        .into_iter()
        .find_map(|(field, value)| {
            value
                .filter(|value| *value > time_utc_ms)
                .map(|value| (field, value))
        });
        if let Some((field, value)) = future_causal_time {
            let error = format!(
                "Polymarket metadata causal time is after real admission clock:field={field}:value={value}:admission={time_utc_ms}"
            );
            record_source_admission_integrity(event, sink, admission_source, &error);
            return Err(invalid_data(error).into());
        }
    }
    Ok(Some(admission_source))
}

pub(super) fn stamp_source_admission(event: &mut RawEvent, sink: &SourceEventSink) -> Result<()> {
    let time_utc_ms = now_ms();
    let Some(admission_source) = validate_source_admission_at(event, sink, time_utc_ms)? else {
        return Ok(());
    };
    let sequence = sink
        .source_queue_backpressure
        .reserve_admission_sequences(admission_source, 1)
        .inspect_err(|error| {
            record_source_admission_integrity(event, sink, admission_source, &error.to_string());
        })?;
    event.collector_admission = Some(RawEventAdmission {
        sequence,
        time_utc_ms,
    });
    Ok(())
}

pub(super) fn stamp_source_admissions_atomically(
    events: &mut [InflightRawEvent],
    sink: &SourceEventSink,
) -> Result<()> {
    let time_utc_ms = now_ms();
    let mut admission_source = None;
    let mut admission_count = 0_usize;
    for event in events.iter() {
        let Some(source) = validate_source_admission_at(&event.event, sink, time_utc_ms)? else {
            continue;
        };
        if admission_source.is_some_and(|expected| expected != source) {
            let error = format!(
                "atomic ingress batch mixed admission authorities:{}:{source}",
                admission_source.unwrap_or_default()
            );
            record_source_admission_integrity(&event.event, sink, source, &error);
            return Err(invalid_data(error).into());
        }
        admission_source = Some(source);
        admission_count = admission_count.saturating_add(1);
    }
    let Some(admission_source) = admission_source else {
        return Ok(());
    };
    let first_sequence =
        sink.source_queue_backpressure
            .reserve_admission_sequences(admission_source, admission_count)
            .inspect_err(|error| {
                for event in events.iter().filter(|event| {
                    source_admission_authority(&event.event) == Some(admission_source)
                }) {
                    record_source_admission_integrity(
                        &event.event,
                        sink,
                        admission_source,
                        &error.to_string(),
                    );
                }
            })?;
    for (offset, event) in events
        .iter_mut()
        .filter(|event| source_admission_authority(&event.event) == Some(admission_source))
        .enumerate()
    {
        event.event.collector_admission = Some(RawEventAdmission {
            sequence: first_sequence + offset as u64,
            time_utc_ms,
        });
    }
    Ok(())
}

pub(super) async fn send_event(
    sink: &SourceEventSink,
    run_started: Instant,
    event: RawEvent,
) -> Result<()> {
    match send_event_with_admission_outcome(sink, run_started, event).await? {
        EventAdmissionOutcome::Admitted | EventAdmissionOutcome::Deferred => Ok(()),
        EventAdmissionOutcome::Cutoff => {
            Err(invalid_data("raw event reached a controlled runtime cutoff").into())
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum EventAdmissionOutcome {
    Admitted,
    Deferred,
    Cutoff,
}

pub(super) async fn send_event_with_admission_outcome(
    sink: &SourceEventSink,
    run_started: Instant,
    event: RawEvent,
) -> Result<EventAdmissionOutcome> {
    enqueue_event_with_reservation_outcome(sink, run_started, event, None, Vec::new()).await
}

pub(super) async fn send_event_with_outcome(
    sink: &SourceEventSink,
    run_started: Instant,
    event: RawEvent,
) -> Result<bool> {
    send_event_with_admission_outcome(sink, run_started, event)
        .await
        .map(|outcome| outcome != EventAdmissionOutcome::Cutoff)
}

pub(super) async fn send_event_with_reservation(
    sink: &SourceEventSink,
    run_started: Instant,
    event: RawEvent,
    existing_reservation: Option<InflightReservation>,
) -> Result<()> {
    send_event_with_reservation_outcome(sink, run_started, event, existing_reservation, Vec::new())
        .await
        .map(|_| ())
}

pub(super) async fn send_event_with_reservation_outcome(
    sink: &SourceEventSink,
    run_started: Instant,
    event: RawEvent,
    existing_reservation: Option<InflightReservation>,
    admission_gaps: Vec<String>,
) -> Result<bool> {
    enqueue_event_with_reservation_outcome(
        sink,
        run_started,
        event,
        existing_reservation,
        admission_gaps,
    )
    .await
    .map(|outcome| outcome != EventAdmissionOutcome::Cutoff)
}

async fn enqueue_event_with_reservation_outcome(
    sink: &SourceEventSink,
    run_started: Instant,
    event: RawEvent,
    existing_reservation: Option<InflightReservation>,
    admission_gaps: Vec<String>,
) -> Result<EventAdmissionOutcome> {
    let source_kind = event.source_kind.clone();
    let recv_time_utc_ms = event.recv_time_utc_ms;
    let gate = sink
        .source_queue_backpressure
        .raw_day_gate()
        .ok_or_else(|| {
            invalid_data(format!(
                "raw-day gate missing from production enqueue path: source={source_kind}"
            ))
        })?;
    let producer = logical_producer_for_source_kind(&source_kind)?;
    let Some(raw_enqueue) = gate
        .begin_unadmitted_event_enqueue(producer, recv_time_utc_ms)
        .await?
    else {
        return Ok(EventAdmissionOutcome::Cutoff);
    };
    let owned_bytes = owned_event_bytes(&event);
    if owned_bytes > MAX_TYPED_ROW_BYTES {
        let gap = format!(
            "operator_intervention_required:max_typed_row_bytes_exceeded:{source_kind}:bytes={owned_bytes}:limit={MAX_TYPED_ROW_BYTES}"
        );
        sink.source_queue_backpressure
            .record_operational_drop(&source_kind, recv_time_utc_ms, 1);
        record_gap(&sink.gaps, gap.clone());
        return Err(invalid_data(gap).into());
    }
    let reservation = match existing_reservation {
        Some(reservation) => reservation,
        None => match sink
            .inflight_budget
            .acquire(1, owned_bytes, BUFFER_ACQUIRE_TIMEOUT)
            .await
        {
            Ok(reservation) => reservation,
            Err(error) => {
                sink.source_queue_backpressure.record_timeout();
                sink.source_queue_backpressure.record_drop(1);
                sink.source_queue_backpressure.record_operational_drop(
                    &source_kind,
                    recv_time_utc_ms,
                    1,
                );
                let gap = format!("global_inflight_budget_failed:{source_kind}:{error}");
                record_gap(&sink.gaps, gap);
                return Err(error.into());
            }
        },
    };
    let mut inflight_event = InflightRawEvent {
        event,
        control: None,
        admission_gaps,
        raw_day_permit: match &raw_enqueue {
            RawDayEnqueueDecision::Permit(enqueue) => Some(enqueue.permit.clone()),
            RawDayEnqueueDecision::Defer(_) => None,
        },
        _source_recv_monotonic_ms: monotonic_ms(run_started),
        _reservation: Some(reservation),
    };
    if let RawDayEnqueueDecision::Defer(epoch) = &raw_enqueue {
        sink.source_queue_backpressure.defer_raw_batch(
            producer,
            epoch.clone(),
            vec![inflight_event],
        )?;
        return Ok(EventAdmissionOutcome::Deferred);
    }
    let _enqueue_guard = sink.enqueue_lock.lock().await;
    let permit = match sink.tx.try_reserve() {
        Ok(permit) => permit,
        Err(mpsc::error::TrySendError::Full(())) => {
            sink.source_queue_backpressure.record_wait();
            match timeout(BUFFER_ACQUIRE_TIMEOUT, sink.tx.reserve()).await {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => {
                    sink.source_queue_backpressure.record_drop(1);
                    sink.source_queue_backpressure.record_operational_drop(
                        &source_kind,
                        recv_time_utc_ms,
                        1,
                    );
                    let gap = format!("bounded_queue_send_failed:{source_kind}");
                    record_gap(&sink.gaps, gap.clone());
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, gap).into());
                }
                Err(_) => {
                    sink.source_queue_backpressure.record_timeout();
                    sink.source_queue_backpressure.record_drop(1);
                    sink.source_queue_backpressure.record_operational_drop(
                        &source_kind,
                        recv_time_utc_ms,
                        1,
                    );
                    let gap = format!("bounded_queue_backpressure_timeout:{source_kind}");
                    record_gap(&sink.gaps, gap.clone());
                    return Err(io::Error::new(io::ErrorKind::TimedOut, gap).into());
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(())) => {
            sink.source_queue_backpressure.record_drop(1);
            sink.source_queue_backpressure.record_operational_drop(
                &source_kind,
                recv_time_utc_ms,
                1,
            );
            let gap = format!("bounded_queue_send_closed:{source_kind}");
            record_gap(&sink.gaps, gap.clone());
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, gap).into());
        }
    };
    stamp_source_admission(&mut inflight_event.event, sink)?;
    let admission_gaps = std::mem::take(&mut inflight_event.admission_gaps);
    permit.send(SourceIngressPayload::RawEvent(inflight_event));
    for gap in admission_gaps {
        record_gap_at(&sink.gaps, recv_time_utc_ms, gap);
    }
    Ok(EventAdmissionOutcome::Admitted)
}
