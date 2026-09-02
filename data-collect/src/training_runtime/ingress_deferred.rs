use super::*;

fn record_deferred_raw_batch_drop(sink: &SourceEventSink, events: &[InflightRawEvent]) {
    sink.source_queue_backpressure
        .record_drop(events.len() as u64);
    for event in events {
        sink.source_queue_backpressure.record_operational_drop(
            &event.event.source_kind,
            event.event.recv_time_utc_ms,
            1,
        );
    }
}

fn record_deferred_ingress_batch_drop(sink: &SourceEventSink, batch: &DeferredIngressBatch) {
    match batch {
        DeferredIngressBatch::LegacyRaw(batch) => {
            record_deferred_raw_batch_drop(sink, &batch.events);
        }
        DeferredIngressBatch::PolymarketOrderbookV3(event) => {
            sink.source_queue_backpressure.record_drop(1);
            sink.source_queue_backpressure.record_operational_drop(
                "polymarket_market",
                event.message.envelope.timestamp_received,
                1,
            );
        }
    }
}

fn record_deferred_polymarket_v3_drop(
    sink: &SourceEventSink,
    event: &DeferredPolymarketOrderbookV3,
) {
    sink.source_queue_backpressure.record_drop(1);
    sink.source_queue_backpressure.record_operational_drop(
        "polymarket_market",
        event.message.envelope.timestamp_received,
        1,
    );
}

fn record_deferred_polymarket_v3_flush_drop(
    sink: &SourceEventSink,
    current: &DeferredPolymarketOrderbookV3,
    remaining: &VecDeque<DeferredIngressBatch>,
) {
    record_deferred_polymarket_v3_drop(sink, current);
    for batch in remaining {
        record_deferred_ingress_batch_drop(sink, batch);
    }
}

pub(super) async fn flush_accepted_deferred_raw_events_for_stop(
    sink: &SourceEventSink,
    producer: &str,
    close: &RawDayClose,
) -> Result<()> {
    let batches = sink
        .source_queue_backpressure
        .take_deferred_ingress_batches(producer);
    let controlled_cutoff = sink
        .source_queue_backpressure
        .raw_day_gate()
        .is_some_and(|gate| gate.unadmitted_runtime_cutoff_is_active());
    let mut accepted = VecDeque::new();
    for batch in batches {
        let is_accepted = match &batch {
            DeferredIngressBatch::LegacyRaw(batch) => batch.accepted_ticket.is_some(),
            DeferredIngressBatch::PolymarketOrderbookV3(_) => true,
        };
        if is_accepted {
            accepted.push_back(batch);
        } else if !controlled_cutoff {
            record_deferred_ingress_batch_drop(sink, &batch);
        }
    }
    flush_deferred_ingress_batches(sink, producer, close, accepted).await
}

pub(super) async fn flush_deferred_raw_events(
    sink: &SourceEventSink,
    producer: &str,
    close: &RawDayClose,
) -> Result<()> {
    let batches = sink
        .source_queue_backpressure
        .take_deferred_ingress_batches(producer);
    flush_deferred_ingress_batches(sink, producer, close, batches).await
}

async fn flush_deferred_ingress_batches(
    sink: &SourceEventSink,
    producer: &str,
    close: &RawDayClose,
    mut batches: VecDeque<DeferredIngressBatch>,
) -> Result<()> {
    while let Some(batch) = batches.pop_front() {
        match batch {
            DeferredIngressBatch::LegacyRaw(batch) => {
                flush_deferred_raw_batch(sink, producer, close, batch, &batches).await?;
            }
            DeferredIngressBatch::PolymarketOrderbookV3(event) => {
                flush_deferred_polymarket_orderbook_v3(sink, producer, close, *event, &batches)
                    .await?;
            }
        }
    }
    Ok(())
}

async fn flush_deferred_raw_batch(
    sink: &SourceEventSink,
    producer: &str,
    close: &RawDayClose,
    batch: DeferredRawBatch,
    remaining: &VecDeque<DeferredIngressBatch>,
) -> Result<()> {
    let epoch = batch.epoch;
    let mut events = batch.events;
    let accepted_ticket = batch.accepted_ticket;
    if epoch.utc_day != close.next_utc_day
        || epoch.old_generation != close.generation
        || epoch.expected_generation != close.next_generation
    {
        record_deferred_raw_batch_drop(sink, &events);
        for batch in remaining {
            record_deferred_ingress_batch_drop(sink, batch);
        }
        return Err(invalid_data(format!(
                "deferred raw-event batch close identity mismatch: producer={producer} expected_close={close:?} actual_epoch={epoch:?}"
            ))
            .into());
    }
    let first = events
        .first()
        .ok_or_else(|| invalid_data("deferred raw-event batch became empty"))?;
    if events.iter().any(|event| {
        logical_producer_for_source_kind(&event.event.source_kind).ok() != Some(producer)
    }) {
        record_deferred_raw_batch_drop(sink, &events);
        for batch in remaining {
            record_deferred_ingress_batch_drop(sink, batch);
        }
        return Err(invalid_data(format!(
            "deferred raw-event producer mismatch while flushing: expected={producer}"
        ))
        .into());
    }
    if events
        .iter()
        .any(|event| event.raw_day_permit.is_some() || event.event.collector_admission.is_some())
    {
        record_deferred_raw_batch_drop(sink, &events);
        for batch in remaining {
            record_deferred_ingress_batch_drop(sink, batch);
        }
        return Err(invalid_data(format!(
            "deferred raw-event staging already owned final admission: producer={producer}"
        ))
        .into());
    }
    let gate = sink
        .source_queue_backpressure
        .raw_day_gate()
        .ok_or_else(|| invalid_data("raw-day gate missing while deferred events flushed"))?;
    let enqueue = match accepted_ticket.as_ref() {
        Some(ticket) => match gate.current_state() {
            RawDayGateState::Closing(ref actual) if actual == close => {
                gate.begin_accepted_close_bound_deferred_flush(
                    producer,
                    close,
                    first.event.recv_time_utc_ms,
                    ticket,
                )
                .await?
            }
            _ => match gate
                .begin_accepted_ticket_enqueue(producer, first.event.recv_time_utc_ms, ticket)
                .await?
            {
                RawDayEnqueueDecision::Permit(enqueue) => enqueue,
                RawDayEnqueueDecision::Defer(actual_epoch) => {
                    record_deferred_raw_batch_drop(sink, &events);
                    for batch in remaining {
                        record_deferred_ingress_batch_drop(sink, batch);
                    }
                    return Err(invalid_data(format!(
                            "accepted deferred raw-event batch remained ahead of active day: producer={producer} expected={epoch:?} actual={actual_epoch:?} recv_time_utc_ms={}",
                            first.event.recv_time_utc_ms,
                        ))
                        .into());
                }
            },
        },
        None => match gate.current_state() {
            RawDayGateState::Closing(ref actual) if actual == close => {
                let Some(enqueue) = gate
                    .begin_close_bound_deferred_flush(producer, close, first.event.recv_time_utc_ms)
                    .await?
                else {
                    return Ok(());
                };
                enqueue
            }
            _ => {
                let Some(decision) = gate
                    .begin_unadmitted_event_enqueue(producer, first.event.recv_time_utc_ms)
                    .await?
                else {
                    return Ok(());
                };
                match decision {
                    RawDayEnqueueDecision::Permit(enqueue) => enqueue,
                    RawDayEnqueueDecision::Defer(actual_epoch) => {
                        record_deferred_raw_batch_drop(sink, &events);
                        for batch in remaining {
                            record_deferred_ingress_batch_drop(sink, batch);
                        }
                        return Err(invalid_data(format!(
                                "deferred raw-event batch remained ahead of active day: producer={producer} expected={epoch:?} actual={actual_epoch:?} recv_time_utc_ms={}",
                                first.event.recv_time_utc_ms,
                            ))
                            .into());
                    }
                }
            }
        },
    };
    let permit = enqueue.permit.clone();
    if permit.utc_day != epoch.utc_day
        || permit.generation != epoch.expected_generation
        || permit.generation != epoch.old_generation.saturating_add(1)
        || events.iter().any(|event| {
            utc_day_string_from_ms(event.event.recv_time_utc_ms).as_deref()
                != Some(permit.utc_day.as_str())
        })
    {
        record_deferred_raw_batch_drop(sink, &events);
        for batch in remaining {
            record_deferred_ingress_batch_drop(sink, batch);
        }
        return Err(invalid_data(format!(
                "deferred raw-event batch epoch mismatch: producer={producer} expected={epoch:?} active={permit:?}"
            ))
            .into());
    }
    for event in &mut events {
        event.raw_day_permit = Some(permit.clone());
    }
    let count = events.len();
    let _enqueue_guard = sink.enqueue_lock.lock().await;
    if count > sink.tx.max_capacity() {
        record_deferred_raw_batch_drop(sink, &events);
        for batch in remaining {
            record_deferred_ingress_batch_drop(sink, batch);
        }
        return Err(invalid_data(format!(
                "deferred raw-event batch exceeds ingress capacity: producer={producer} rows={count} capacity={}",
                sink.tx.max_capacity()
            ))
            .into());
    }
    let permits = match sink.tx.try_reserve_many(count) {
        Ok(permits) => permits,
        Err(mpsc::error::TrySendError::Full(())) => {
            sink.source_queue_backpressure.record_wait();
            match timeout(BUFFER_ACQUIRE_TIMEOUT, sink.tx.reserve_many(count)).await {
                Ok(Ok(permits)) => permits,
                Ok(Err(_)) => {
                    record_deferred_raw_batch_drop(sink, &events);
                    for batch in remaining {
                        record_deferred_ingress_batch_drop(sink, batch);
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        format!("deferred raw-event ingress channel closed: producer={producer}"),
                    )
                    .into());
                }
                Err(_) => {
                    sink.source_queue_backpressure.record_timeout();
                    record_deferred_raw_batch_drop(sink, &events);
                    for batch in remaining {
                        record_deferred_ingress_batch_drop(sink, batch);
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("deferred raw-event ingress timeout: producer={producer}"),
                    )
                    .into());
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(())) => {
            record_deferred_raw_batch_drop(sink, &events);
            for batch in remaining {
                record_deferred_ingress_batch_drop(sink, batch);
            }
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("deferred raw-event ingress channel closed: producer={producer}"),
            )
            .into());
        }
    };
    if let Err(error) = stamp_source_admissions_atomically(&mut events, sink) {
        record_deferred_raw_batch_drop(sink, &events);
        for batch in remaining {
            record_deferred_ingress_batch_drop(sink, batch);
        }
        return Err(error);
    }
    for (permit, mut event) in permits.zip(events) {
        let gap_time_utc_ms = event.event.recv_time_utc_ms;
        let admission_gaps = std::mem::take(&mut event.admission_gaps);
        permit.send(SourceIngressPayload::LegacyRaw(event));
        for gap in admission_gaps {
            record_gap_at(&sink.gaps, gap_time_utc_ms, gap);
        }
    }
    Ok(())
}

async fn flush_deferred_polymarket_orderbook_v3(
    sink: &SourceEventSink,
    producer: &str,
    close: &RawDayClose,
    event: DeferredPolymarketOrderbookV3,
    remaining: &VecDeque<DeferredIngressBatch>,
) -> Result<()> {
    let recv_time_utc_ms = event.message.envelope.timestamp_received;
    if producer != "polymarket"
        || event.epoch.utc_day != close.next_utc_day
        || event.epoch.old_generation != close.generation
        || event.epoch.expected_generation != close.next_generation
        || event.accepted_ticket != RawDayAcceptedTicket::Defer(event.epoch.clone())
    {
        record_deferred_polymarket_v3_flush_drop(sink, &event, remaining);
        return Err(invalid_data(format!(
            "deferred Polymarket v3 close identity mismatch:producer={producer}:close={close:?}:epoch={:?}:ticket={:?}",
            event.epoch, event.accepted_ticket
        ))
        .into());
    }
    event
        .message
        .validate()
        .inspect_err(|_| record_deferred_polymarket_v3_flush_drop(sink, &event, remaining))?;
    let gate = sink
        .source_queue_backpressure
        .raw_day_gate()
        .ok_or_else(|| invalid_data("raw-day gate missing while deferred v3 event flushed"))?;
    let enqueue = match gate.current_state() {
        RawDayGateState::Closing(ref actual) if actual == close => {
            gate.begin_accepted_close_bound_deferred_flush(
                producer,
                close,
                recv_time_utc_ms,
                &event.accepted_ticket,
            )
            .await?
        }
        _ => match gate
            .begin_accepted_ticket_enqueue(producer, recv_time_utc_ms, &event.accepted_ticket)
            .await?
        {
            RawDayEnqueueDecision::Permit(enqueue) => enqueue,
            RawDayEnqueueDecision::Defer(actual_epoch) => {
                record_deferred_polymarket_v3_flush_drop(sink, &event, remaining);
                return Err(invalid_data(format!(
                    "accepted deferred Polymarket v3 event remained ahead of active day:expected={:?}:actual={actual_epoch:?}:recv_time_utc_ms={recv_time_utc_ms}",
                    event.epoch
                ))
                .into());
            }
        },
    };
    let raw_day_permit = enqueue.permit.clone();
    if raw_day_permit.utc_day != event.epoch.utc_day
        || raw_day_permit.generation != event.epoch.expected_generation
        || raw_day_permit.generation != event.epoch.old_generation.saturating_add(1)
        || utc_day_string_from_ms(recv_time_utc_ms).as_deref()
            != Some(raw_day_permit.utc_day.as_str())
    {
        record_deferred_polymarket_v3_flush_drop(sink, &event, remaining);
        return Err(invalid_data(format!(
            "deferred Polymarket v3 epoch mismatch:expected={:?}:active={raw_day_permit:?}",
            event.epoch
        ))
        .into());
    }
    let _enqueue_guard = sink.enqueue_lock.lock().await;
    let permit = match sink.tx.try_reserve() {
        Ok(permit) => permit,
        Err(mpsc::error::TrySendError::Full(())) => {
            sink.source_queue_backpressure.record_wait();
            match timeout(BUFFER_ACQUIRE_TIMEOUT, sink.tx.reserve()).await {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => {
                    record_deferred_polymarket_v3_flush_drop(sink, &event, remaining);
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "deferred Polymarket v3 ingress channel closed",
                    )
                    .into());
                }
                Err(_) => {
                    sink.source_queue_backpressure.record_timeout();
                    record_deferred_polymarket_v3_flush_drop(sink, &event, remaining);
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "deferred Polymarket v3 ingress timeout",
                    )
                    .into());
                }
            }
        }
        Err(mpsc::error::TrySendError::Closed(())) => {
            record_deferred_polymarket_v3_flush_drop(sink, &event, remaining);
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "deferred Polymarket v3 ingress channel closed",
            )
            .into());
        }
    };
    permit.send(SourceIngressPayload::PolymarketOrderbookV3(
        InflightPolymarketOrderbookV3 {
            message: event.message,
            raw_day_permit,
            _source_recv_monotonic_ms: event._source_recv_monotonic_ms,
            _row_reservation: event._row_reservation,
            _byte_reservation: event._byte_reservation,
        },
    ));
    Ok(())
}
