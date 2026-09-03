use super::*;

pub(super) fn raw_event(
    source_kind: &str,
    source_event_type: &str,
    source_identifier: &str,
    source_ts: Option<String>,
    source_endpoint: &str,
    collector_identity: &CollectorIdentity,
    raw: Value,
) -> RawEvent {
    raw_event_with_recv_time(
        source_kind,
        source_event_type,
        source_identifier,
        source_ts,
        now_ms(),
        source_endpoint,
        collector_identity,
        raw,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn raw_event_with_recv_time(
    source_kind: &str,
    source_event_type: &str,
    source_identifier: &str,
    source_ts: Option<String>,
    recv_time_utc_ms: i64,
    source_endpoint: &str,
    collector_identity: &CollectorIdentity,
    raw: Value,
) -> RawEvent {
    RawEvent {
        source_kind: source_kind.to_owned(),
        source_event_type: source_event_type.to_owned(),
        source_identifier: source_identifier.to_owned(),
        source_ts,
        recv_time_utc_ms,
        collector_host_id: collector_identity.host_id.clone(),
        collector_region: collector_identity.region.clone(),
        source_endpoint: source_endpoint.to_owned(),
        raw_json: raw.to_string(),
        collector_admission: None,
    }
}

pub(super) fn estimated_event_bytes(event: &RawEvent) -> u64 {
    event.source_kind.len() as u64
        + event.source_event_type.len() as u64
        + event.source_identifier.len() as u64
        + event.source_ts.as_deref().unwrap_or_default().len() as u64
        + event.collector_host_id.len() as u64
        + event.collector_region.len() as u64
        + event.source_endpoint.len() as u64
        + event.raw_json.len() as u64
}

pub(super) fn event_envelope(event: &RawEvent, source_kind: SourceKind) -> EventEnvelope {
    EventEnvelope {
        source_kind,
        source_event_type: event.source_event_type.clone(),
        source_identifier: event.source_identifier.clone(),
        source_timestamp: event
            .source_ts
            .clone()
            .map(|value| SourceTimestamp::Timestamp { value })
            .unwrap_or_else(|| SourceTimestamp::NoSourceTimestamp {
                reason: "source_event_has_no_timestamp".to_owned(),
                snapshot_boundary: event.recv_time_utc_ms.to_string(),
            }),
        recv_time_utc_ms: Some(event.recv_time_utc_ms),
        collector_host_id: event.collector_host_id.clone(),
        collector_region: event.collector_region.clone(),
        source_endpoint: event.source_endpoint.clone(),
        host_clock_sync_status: HostClockSyncStatus::Unavailable {
            reason: "not_checked_in_training_runtime".to_owned(),
        },
        raw_evidence: RawEvidence::Json(event.raw_json.clone()),
    }
}

pub(super) struct DeferredRawBatch {
    pub(super) epoch: RawDayDeferredEpoch,
    pub(super) events: Vec<InflightRawEvent>,
    pub(super) accepted_ticket: Option<RawDayAcceptedTicket>,
}

pub(super) enum DeferredIngressBatch {
    RawEvent(DeferredRawBatch),
    PolymarketOrderbookV3(Box<DeferredPolymarketOrderbookV3>),
}

#[derive(Default)]
pub(super) struct SourceQueueBackpressureStats {
    pub(super) wait_count: AtomicU64,
    pub(super) timeout_count: AtomicU64,
    pub(super) dropped_rows: AtomicU64,
    pub(super) aggtrade_admission_sequence: AtomicU64,
    pub(super) depth_admission_sequence: AtomicU64,
    pub(super) polymarket_market_admission_sequence: AtomicU64,
    pub(super) polymarket_metadata_admission_sequence: AtomicU64,
    operational_counters: Mutex<BTreeMap<String, BTreeMap<String, SourceOperationalCounterDelta>>>,
    pub(super) active_recovery_sources: Mutex<BTreeSet<String>>,
    pub(super) deferred_ingress_batches: Mutex<BTreeMap<String, VecDeque<DeferredIngressBatch>>>,
    raw_day_gate: OnceLock<Arc<RawDayGate>>,
}

impl SourceQueueBackpressureStats {
    pub(super) fn reserve_admission_sequences(&self, source: &str, count: usize) -> Result<u64> {
        let increment = u64::try_from(count)
            .map_err(|_| invalid_data(format!("{source} admission batch size overflow")))?;
        if increment == 0 {
            return Err(invalid_data(format!("{source} admission batch is empty")).into());
        }
        let (counter, label) = match source {
            "binance_aggtrade" => (
                &self.aggtrade_admission_sequence,
                "binance aggTrade admission sequence",
            ),
            "binance_depth" => (
                &self.depth_admission_sequence,
                "Binance depth admission sequence",
            ),
            "polymarket_market" => (
                &self.polymarket_market_admission_sequence,
                "Polymarket market admission sequence",
            ),
            "polymarket_market_metadata" => (
                &self.polymarket_metadata_admission_sequence,
                "Polymarket metadata admission sequence",
            ),
            _ => {
                return Err(invalid_data(format!(
                    "unsupported admission sequence source:{source}"
                ))
                .into());
            }
        };
        counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(increment)
            })
            .map(|previous| previous + 1)
            .map_err(|_| invalid_data(format!("{label} overflow")).into())
    }

    pub(super) fn install_raw_day_gate(&self, gate: Arc<RawDayGate>) -> Result<()> {
        self.raw_day_gate
            .set(gate)
            .map_err(|_| invalid_data("raw-day gate already installed").into())
    }

    pub(super) fn raw_day_gate(&self) -> Option<Arc<RawDayGate>> {
        self.raw_day_gate.get().cloned()
    }

    pub(super) fn defer_raw_batch(
        &self,
        producer: &str,
        epoch: RawDayDeferredEpoch,
        events: Vec<InflightRawEvent>,
    ) -> Result<()> {
        self.defer_raw_batch_inner(producer, epoch, events, None)
    }

    pub(super) fn defer_accepted_raw_batch(
        &self,
        producer: &str,
        epoch: RawDayDeferredEpoch,
        events: Vec<InflightRawEvent>,
        ticket: RawDayAcceptedTicket,
    ) -> Result<()> {
        self.defer_raw_batch_inner(producer, epoch, events, Some(ticket))
    }

    fn defer_raw_batch_inner(
        &self,
        producer: &str,
        epoch: RawDayDeferredEpoch,
        events: Vec<InflightRawEvent>,
        accepted_ticket: Option<RawDayAcceptedTicket>,
    ) -> Result<()> {
        if events.is_empty() {
            return Err(invalid_data("cannot defer an empty raw-event batch").into());
        }
        if !LOGICAL_PRODUCERS.contains(&producer) {
            return Err(invalid_data(format!(
                "cannot defer raw events for unknown logical producer: {producer}"
            ))
            .into());
        }
        self.deferred_ingress_batches
            .lock()
            .expect("deferred raw-event mutex poisoned")
            .entry(producer.to_owned())
            .or_default()
            .push_back(DeferredIngressBatch::RawEvent(DeferredRawBatch {
                epoch,
                events,
                accepted_ticket,
            }));
        Ok(())
    }

    pub(super) fn take_deferred_ingress_batches(
        &self,
        producer: &str,
    ) -> VecDeque<DeferredIngressBatch> {
        self.deferred_ingress_batches
            .lock()
            .expect("deferred ingress mutex poisoned")
            .remove(producer)
            .unwrap_or_default()
    }

    pub(super) fn record_wait(&self) {
        self.wait_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_timeout(&self) {
        self.timeout_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_drop(&self, rows: u64) {
        self.dropped_rows.fetch_add(rows, Ordering::Relaxed);
    }

    pub(super) fn record_structural_dedupe(
        &self,
        source_kind: &str,
        recv_time_utc_ms: i64,
        events: u64,
    ) {
        self.record_operational(source_kind, recv_time_utc_ms, |counter| {
            counter.structural_dedupe_dropped_events = counter
                .structural_dedupe_dropped_events
                .saturating_add(events);
        });
    }

    pub(super) fn record_lag(&self, source_kind: &str, recv_time_utc_ms: i64, events: u64) {
        self.record_operational(source_kind, recv_time_utc_ms, |counter| {
            counter.lag_events = counter.lag_events.saturating_add(events);
        });
    }

    pub(super) fn record_reconnect(&self, source_kind: &str, recv_time_utc_ms: i64) {
        self.record_operational(source_kind, recv_time_utc_ms, |counter| {
            counter.reconnect_events = counter.reconnect_events.saturating_add(1);
        });
    }

    pub(super) fn record_operational_drop(
        &self,
        source_kind: &str,
        recv_time_utc_ms: i64,
        rows: u64,
    ) {
        self.record_operational(source_kind, recv_time_utc_ms, |counter| {
            counter.dropped_rows = counter.dropped_rows.saturating_add(rows);
        });
    }

    fn record_operational(
        &self,
        source_kind: &str,
        recv_time_utc_ms: i64,
        update: impl FnOnce(&mut SourceOperationalCounterDelta),
    ) {
        let Some(utc_day) = utc_day_string_from_ms(recv_time_utc_ms) else {
            return;
        };
        let mut counters = self
            .operational_counters
            .lock()
            .expect("operational counter mutex poisoned");
        let counter = counters
            .entry(utc_day)
            .or_default()
            .entry(source_kind.to_owned())
            .or_default();
        update(counter);
    }

    pub(super) fn drain_operational_day(
        &self,
        utc_day: &str,
    ) -> BTreeMap<String, SourceOperationalCounterDelta> {
        self.operational_counters
            .lock()
            .expect("operational counter mutex poisoned")
            .remove(utc_day)
            .unwrap_or_default()
    }

    pub(super) fn drain_all_operational(
        &self,
    ) -> BTreeMap<String, BTreeMap<String, SourceOperationalCounterDelta>> {
        std::mem::take(
            &mut *self
                .operational_counters
                .lock()
                .expect("operational counter mutex poisoned"),
        )
    }

    pub(super) fn set_recovery_active(&self, source_kind: &str, active: bool) {
        let mut sources = self
            .active_recovery_sources
            .lock()
            .expect("active recovery mutex poisoned");
        if active {
            sources.insert(source_kind.to_owned());
        } else {
            sources.remove(source_kind);
        }
    }

    pub(super) fn apply_to_tracker(&self, tracker: &mut QueueDurationTracker) {
        let wait_count = self.wait_count.swap(0, Ordering::Relaxed);
        for _ in 0..wait_count {
            tracker.record_backpressure_wait();
        }
        let timeout_count = self.timeout_count.swap(0, Ordering::Relaxed);
        for _ in 0..timeout_count {
            tracker.record_backpressure_timeout();
        }
        let dropped_rows = self.dropped_rows.swap(0, Ordering::Relaxed);
        if dropped_rows > 0 {
            tracker.record_drop(dropped_rows);
        }
    }
}

#[derive(Clone)]
pub(super) struct SourceEventSink {
    pub(super) tx: mpsc::Sender<SourceIngressPayload>,
    pub(super) gaps: SharedSourceGaps,
    pub(super) source_queue_backpressure: Arc<SourceQueueBackpressureStats>,
    pub(super) inflight_budget: InflightBudget,
    pub(super) enqueue_lock: Arc<AsyncMutex<()>>,
}

impl SourceEventSink {
    pub(super) fn fork_logical_producer(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            gaps: Arc::clone(&self.gaps),
            source_queue_backpressure: Arc::clone(&self.source_queue_backpressure),
            inflight_budget: self.inflight_budget.clone(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        }
    }
}

pub(super) struct InflightRawEvent {
    pub(super) event: RawEvent,
    pub(super) control: Option<IngressControl>,
    pub(super) admission_gaps: Vec<String>,
    pub(super) raw_day_permit: Option<RawDayPermit>,
    pub(super) _source_recv_monotonic_ms: u64,
    pub(super) _reservation: Option<InflightReservation>,
}

pub(super) struct InflightPolymarketOrderbookV3 {
    pub(super) message: PolymarketOrderbookV3Message,
    pub(super) raw_day_permit: RawDayPermit,
    pub(super) _source_recv_monotonic_ms: u64,
    pub(super) _row_reservation: InflightReservation,
    pub(super) _byte_reservation: InflightReservation,
}

pub(super) struct DeferredPolymarketOrderbookV3 {
    pub(super) epoch: RawDayDeferredEpoch,
    pub(super) accepted_ticket: RawDayAcceptedTicket,
    pub(super) message: PolymarketOrderbookV3Message,
    pub(super) _source_recv_monotonic_ms: u64,
    pub(super) _row_reservation: InflightReservation,
    pub(super) _byte_reservation: InflightReservation,
}

#[allow(clippy::large_enum_variant)]
pub(super) enum SourceIngressPayload {
    RawEvent(InflightRawEvent),
    PolymarketOrderbookV3(InflightPolymarketOrderbookV3),
}

impl SourceIngressPayload {
    #[cfg(test)]
    pub(super) fn into_raw(self) -> InflightRawEvent {
        match self {
            Self::RawEvent(event) => event,
            Self::PolymarketOrderbookV3(_) => panic!("expected a raw event ingress payload"),
        }
    }

    #[cfg(test)]
    pub(super) fn into_event(self) -> RawEvent {
        self.into_raw().event
    }

    #[cfg(test)]
    pub(super) fn into_polymarket_orderbook_v3(self) -> InflightPolymarketOrderbookV3 {
        match self {
            Self::PolymarketOrderbookV3(event) => event,
            Self::RawEvent(_) => panic!("expected a Polymarket v3 ingress payload"),
        }
    }
}

#[cfg(test)]
impl std::ops::Deref for SourceIngressPayload {
    type Target = InflightRawEvent;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::RawEvent(event) => event,
            Self::PolymarketOrderbookV3(_) => panic!("expected a raw event ingress payload"),
        }
    }
}

#[cfg(test)]
impl std::ops::DerefMut for SourceIngressPayload {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::RawEvent(event) => event,
            Self::PolymarketOrderbookV3(_) => panic!("expected a raw event ingress payload"),
        }
    }
}

impl InflightRawEvent {
    #[cfg(test)]
    pub(super) fn for_test(mut event: RawEvent) -> Self {
        if matches!(
            event.source_kind.as_str(),
            "binance_aggtrade" | "binance_depth_snapshot" | "binance_depth_update"
        ) && event.collector_admission.is_none()
        {
            event.collector_admission = Some(RawEventAdmission {
                sequence: u64::try_from(event.recv_time_utc_ms).unwrap_or(1).max(1),
                time_utc_ms: event.recv_time_utc_ms.max(1),
            });
        }
        let budget = InflightBudget::new();
        let bytes = owned_event_bytes(&event);
        Self {
            event,
            control: None,
            admission_gaps: Vec::new(),
            raw_day_permit: None,
            _source_recv_monotonic_ms: 0,
            _reservation: Some(budget.try_acquire(1, bytes)),
        }
    }
}
