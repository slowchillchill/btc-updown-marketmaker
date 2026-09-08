use super::*;

#[derive(Default)]
pub(super) struct PendingIngressBatches {
    pub(super) raw_events: BTreeMap<String, Vec<InflightRawEvent>>,
    pub(super) polymarket_orderbook_v3: Vec<InflightPolymarketOrderbookV3>,
}

impl PendingIngressBatches {
    pub(super) fn for_training_source_kinds() -> Self {
        Self {
            raw_events: training_source_kinds()
                .iter()
                .map(|source| {
                    (
                        (*source).to_owned(),
                        Vec::<InflightRawEvent>::with_capacity(DEFAULT_FLUSH_ROWS),
                    )
                })
                .collect(),
            polymarket_orderbook_v3: Vec::with_capacity(DEFAULT_FLUSH_ROWS),
        }
    }
}

pub(super) async fn flush_all_batches(
    writers: &mut BTreeMap<String, SourceWriterHandle>,
    batches: &mut PendingIngressBatches,
    gaps: &SharedSourceGaps,
    runtime_stats: &SourceQueueBackpressureStats,
    writer_queue_tracker: &mut QueueDurationTracker,
    run_started: Instant,
) -> Result<()> {
    let sources = batches.raw_events.keys().cloned().collect::<Vec<_>>();
    for source_kind in sources {
        let Some(batch) = batches.raw_events.get_mut(&source_kind) else {
            continue;
        };
        flush_source_batch(
            writers,
            &source_kind,
            batch,
            gaps,
            runtime_stats,
            writer_queue_tracker,
            run_started,
        )
        .await?;
    }
    flush_polymarket_orderbook_v3_batch(
        writers,
        &mut batches.polymarket_orderbook_v3,
        gaps,
        runtime_stats,
        writer_queue_tracker,
        run_started,
    )
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_ingress_control(
    control: IngressControl,
    writers: &mut BTreeMap<String, SourceWriterHandle>,
    batches: &mut PendingIngressBatches,
    gaps: &SharedSourceGaps,
    runtime_stats: &SourceQueueBackpressureStats,
    writer_queue_tracker: &mut QueueDurationTracker,
    run_started: Instant,
    manifest_tx: &mpsc::Sender<ManifestCommand>,
    rollover_coordinator: &mut RolloverCoordinatorState,
) -> Result<()> {
    match control {
        IngressControl::FinalizeSourceHour {
            source_kind,
            utc_hour,
        } => {
            let batch = batches.raw_events.get_mut(&source_kind).ok_or_else(|| {
                invalid_data(format!(
                    "missing source batch for hour finalize: {source_kind}"
                ))
            })?;
            flush_source_batch(
                writers,
                &source_kind,
                batch,
                gaps,
                runtime_stats,
                writer_queue_tracker,
                run_started,
            )
            .await?;
            if source_kind == "polymarket_market" {
                flush_polymarket_orderbook_v3_batch(
                    writers,
                    &mut batches.polymarket_orderbook_v3,
                    gaps,
                    runtime_stats,
                    writer_queue_tracker,
                    run_started,
                )
                .await?;
            }
            send_writer_hour_finalize(writers, &source_kind, &utc_hour).await?;
        }
        IngressControl::DayBarrier {
            producer,
            close,
            reply,
        } => {
            rollover_coordinator.register_producer_barrier(
                producer.clone(),
                close.clone(),
                reply,
            )?;
            flush_all_batches(
                writers,
                batches,
                gaps,
                runtime_stats,
                writer_queue_tracker,
                run_started,
            )
            .await?;
            send_writer_day_finalize(writers, &producer, &close).await?;
            let gap_snapshot = gaps
                .lock()
                .expect("source gap mutex poisoned")
                .reasons_for_day(&close.utc_day);
            let operational_counters = runtime_stats.drain_operational_day(&close.utc_day);
            manifest_tx
                .send(ManifestCommand::ProducerBarrier {
                    close,
                    producer,
                    gaps: gap_snapshot,
                    operational_counters,
                })
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "daily manifest owner channel closed during producer barrier",
                    )
                })?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn process_ingress_event(
    inflight: SourceIngressPayload,
    writers: &mut BTreeMap<String, SourceWriterHandle>,
    batches: &mut PendingIngressBatches,
    coverage: &mut BTreeMap<String, CoverageStats>,
    received_rows: &mut u64,
    average_event_bytes: &mut u64,
    gaps: &SharedSourceGaps,
    runtime_stats: &SourceQueueBackpressureStats,
    writer_queue_tracker: &mut QueueDurationTracker,
    run_started: Instant,
    manifest_tx: &mpsc::Sender<ManifestCommand>,
    rollover_coordinator: &mut RolloverCoordinatorState,
) -> Result<()> {
    let mut inflight_event = match inflight {
        SourceIngressPayload::RawEvent(event) => {
            if event.event.source_kind == "polymarket_market"
                && TypedParquetSegmentWriter::current_schema_version("polymarket_market")?
                    == POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION
            {
                return Err(invalid_data(format!(
                    "raw Polymarket ingress is forbidden after v3 activation:event_type={}",
                    event.event.source_event_type
                ))
                .into());
            }
            event
        }
        SourceIngressPayload::PolymarketOrderbookV3(event) => {
            let raw_day_permit = event.raw_day_permit.as_ref().ok_or_else(|| {
                invalid_data("production raw event missing day/generation permit")
            })?;
            let Some(writer) = writers.get("polymarket_market") else {
                return Err(invalid_data(
                    "Polymarket orderbook v3 ingress is missing the polymarket_market writer",
                )
                .into());
            };
            if writer.schema_version != POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION {
                return Err(invalid_data(format!(
                    "Polymarket orderbook v3 ingress reached a non-v3 writer:event_type={}:utc_day={}:generation={}",
                    event.message.event_type(),
                    raw_day_permit.utc_day,
                    raw_day_permit.generation,
                ))
                .into());
            }
            let recv_time_utc_ms = event.message.envelope.timestamp_received;
            let event_day = utc_day_string_from_ms(recv_time_utc_ms).ok_or_else(|| {
                invalid_data("UTC day unavailable for inflight Polymarket v3 message")
            })?;
            if event_day != raw_day_permit.utc_day || raw_day_permit.generation == 0 {
                return Err(invalid_data(format!(
                    "inflight Polymarket v3 raw-day permit mismatch: event_day={event_day} permit={:?}",
                    event.raw_day_permit
                ))
                .into());
            }
            *received_rows = received_rows.saturating_add(1);
            update_polymarket_v3_coverage(coverage, &event.message);
            *average_event_bytes = rolling_average_bytes(
                *average_event_bytes,
                u64::try_from(event.message.owned_bytes())
                    .map_err(|_| invalid_data("Polymarket v3 owned bytes do not fit in u64"))?,
                *received_rows,
            );
            batches.polymarket_orderbook_v3.push(event);
            if batches.polymarket_orderbook_v3.len() >= DEFAULT_FLUSH_ROWS {
                flush_polymarket_orderbook_v3_batch(
                    writers,
                    &mut batches.polymarket_orderbook_v3,
                    gaps,
                    runtime_stats,
                    writer_queue_tracker,
                    run_started,
                )
                .await?;
            }
            return Ok(());
        }
    };
    if let Some(control) = inflight_event.control.take() {
        drop(inflight_event);
        return handle_ingress_control(
            control,
            writers,
            batches,
            gaps,
            runtime_stats,
            writer_queue_tracker,
            run_started,
            manifest_tx,
            rollover_coordinator,
        )
        .await;
    }
    if !inflight_event.admission_gaps.is_empty() {
        return Err(invalid_data("production raw event retained pre-admission gap effects").into());
    }
    let permit = inflight_event
        .raw_day_permit
        .as_ref()
        .ok_or_else(|| invalid_data("production raw event missing day/generation permit"))?;
    let event_day = utc_day_string_from_ms(inflight_event.event.recv_time_utc_ms)
        .ok_or_else(|| invalid_data("UTC day unavailable for inflight raw event"))?;
    if event_day != permit.utc_day || permit.generation == 0 {
        return Err(invalid_data(format!(
            "inflight raw-day permit mismatch: event_day={event_day} permit={permit:?}"
        ))
        .into());
    }
    *received_rows = received_rows.saturating_add(1);
    let event = &inflight_event.event;
    update_coverage(coverage, event);
    *average_event_bytes = rolling_average_bytes(
        *average_event_bytes,
        estimated_event_bytes(event),
        *received_rows,
    );
    let source_kind = event.source_kind.clone();
    let batch = batches.raw_events.entry(source_kind.clone()).or_default();
    batch.push(inflight_event);
    if batch.len() >= DEFAULT_FLUSH_ROWS {
        flush_source_batch(
            writers,
            &source_kind,
            batch,
            gaps,
            runtime_stats,
            writer_queue_tracker,
            run_started,
        )
        .await?;
    }
    Ok(())
}

fn rolling_average_bytes(previous_average: u64, current_value: u64, observed_rows: u64) -> u64 {
    if observed_rows <= 1 {
        return current_value.max(1);
    }
    let prior_weight = observed_rows.saturating_sub(1);
    previous_average
        .saturating_mul(prior_weight)
        .saturating_add(current_value)
        / observed_rows
}
