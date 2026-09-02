use super::*;

pub(super) const RTDS_WS_ENDPOINT: &str = "wss://ws-live-data.polymarket.com";
const RTDS_FRESH_MIN_AGE_MS: i64 = -5_000;
const RTDS_FRESH_MAX_AGE_MS: i64 = 30_000;
pub(super) const RTDS_DEDUPE_WINDOW_MS: i64 = 60 * 60 * 1000;
const RTDS_DEDUPE_MAX_KEYS: usize = 100_000;
const RTDS_STEADY_SILENCE_THRESHOLD: Duration = Duration::from_secs(300);
const RTDS_FIRST_FRESH_OBSERVATION_WINDOW: Duration = Duration::from_secs(60);
const RTDS_OUTER_BACKOFF_INITIAL_MS: u64 = 1_000;
const RTDS_OUTER_BACKOFF_MAX_MS: u64 = 300_000;

fn operator_dedupe_error(message: String) -> io::Error {
    invalid_data(format!(
        "operator_intervention_required:dedupe_invariant:{message}"
    ))
}

#[derive(Clone, Debug)]
pub(super) struct ClassifiedRtdsMessage {
    payload: ChainlinkPrice,
    age_ms: Option<i64>,
    pub(super) freshness_status: &'static str,
}

pub(super) fn classify_rtds_message(
    message: &polymarket_client_sdk_v2::rtds::RtdsMessage,
    recv_time_utc_ms: i64,
) -> Result<Option<ClassifiedRtdsMessage>> {
    let payload = match serde_json::from_value::<ChainlinkPrice>(message.payload.clone()) {
        Ok(payload) => payload,
        Err(_) if message.topic != "crypto_prices_chainlink" || message.msg_type != "update" => {
            return Ok(None);
        }
        Err(error) => {
            return Err(invalid_data(format!("RTDS Chainlink payload invalid: {error}")).into());
        }
    };
    let age_ms = recv_time_utc_ms.checked_sub(payload.timestamp);
    let freshness_status = if !plausible_unix_timestamp_ms(message.timestamp)
        || !plausible_unix_timestamp_ms(payload.timestamp)
        || age_ms.is_none()
    {
        "invalid_timestamp"
    } else if message.topic != "crypto_prices_chainlink" {
        "invalid_topic"
    } else if message.msg_type != "update" {
        "invalid_type"
    } else if payload.symbol != "btc/usd" {
        "invalid_symbol"
    } else if age_ms
        .is_some_and(|age| (RTDS_FRESH_MIN_AGE_MS..=RTDS_FRESH_MAX_AGE_MS).contains(&age))
    {
        "fresh"
    } else {
        "stale"
    };
    Ok(Some(ClassifiedRtdsMessage {
        payload,
        age_ms,
        freshness_status,
    }))
}

pub(super) fn bounded_rtds_ws_config() -> WsConfig {
    let mut config = WsConfig::default();
    config.connect_timeout = Some(Duration::from_secs(WS_CONNECT_TIMEOUT_SECS));
    config.reconnect.max_attempts = None;
    config.reconnect.max_backoff = Duration::from_secs(5);
    config.heartbeat_interval = Duration::from_secs(5);
    config.require_text_pong = false;
    config.heartbeat_timeout = Duration::from_secs(15);
    config
}

#[derive(Clone)]
pub(super) struct RtdsSourceSpec {
    pub(super) endpoint: String,
    pub(super) ws_config: WsConfig,
    pub(super) business_timing: RtdsBusinessTiming,
}

impl RtdsSourceSpec {
    pub(super) fn production() -> Self {
        Self {
            endpoint: RTDS_WS_ENDPOINT.to_owned(),
            ws_config: bounded_rtds_ws_config(),
            business_timing: RtdsBusinessTiming::production(),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct RtdsBusinessTiming {
    pub(super) steady_silence_threshold: Duration,
    pub(super) first_fresh_observation_window: Duration,
    pub(super) outer_backoff_initial_ms: u64,
    pub(super) outer_backoff_max_ms: u64,
}

impl RtdsBusinessTiming {
    const fn production() -> Self {
        Self {
            steady_silence_threshold: RTDS_STEADY_SILENCE_THRESHOLD,
            first_fresh_observation_window: RTDS_FIRST_FRESH_OBSERVATION_WINDOW,
            outer_backoff_initial_ms: RTDS_OUTER_BACKOFF_INITIAL_MS,
            outer_backoff_max_ms: RTDS_OUTER_BACKOFF_MAX_MS,
        }
    }
}

#[derive(Clone, Copy)]
enum RtdsBusinessDeadlineKind {
    Steady,
    FirstFresh,
    Backoff,
}

pub(super) struct RtdsBusinessRecovery {
    timing: RtdsBusinessTiming,
    jitter_seed: u64,
    generation: u64,
    connected_at: Option<tokio::time::Instant>,
    applied_at: Option<tokio::time::Instant>,
    deadline: Option<(tokio::time::Instant, RtdsBusinessDeadlineKind)>,
    ever_received_fresh: bool,
    episode_trigger: Option<&'static str>,
    replacement_attempts: u64,
    next_backoff_level: u32,
}

impl RtdsBusinessRecovery {
    pub(super) fn new(timing: RtdsBusinessTiming, jitter_seed: u64, generation: u64) -> Self {
        Self {
            timing,
            jitter_seed,
            generation,
            connected_at: None,
            applied_at: None,
            deadline: None,
            ever_received_fresh: false,
            episode_trigger: None,
            replacement_attempts: 0,
            next_backoff_level: 1,
        }
    }

    pub(super) fn observe_state(
        &mut self,
        state: ConnectionState,
        generation: u64,
        observed_at: tokio::time::Instant,
    ) {
        if generation != self.generation {
            self.start_generation(generation);
        }
        if state.is_connected() {
            self.connected_at.get_or_insert(observed_at);
            self.arm_first_fresh_deadline();
        } else {
            self.clear_generation_observations();
        }
    }

    pub(super) fn observe_applied(&mut self, generation: u64, observed_at: tokio::time::Instant) {
        if generation != self.generation {
            self.start_generation(generation);
        }
        self.applied_at.get_or_insert(observed_at);
        self.arm_first_fresh_deadline();
    }

    pub(super) fn reconcile_current(
        &mut self,
        state: ConnectionState,
        generation: u64,
        subscription_applied: bool,
        observed_at: tokio::time::Instant,
    ) {
        if generation != self.generation {
            self.start_generation(generation);
        }
        if !state.is_connected() {
            self.clear_generation_observations();
            return;
        }
        self.connected_at.get_or_insert(observed_at);
        if subscription_applied {
            self.applied_at.get_or_insert(observed_at);
            self.arm_first_fresh_deadline();
        } else {
            self.applied_at = None;
            self.deadline = None;
        }
    }

    pub(super) fn observe_fresh(&mut self, generation: u64, observed_at: tokio::time::Instant) {
        if generation != self.generation {
            return;
        }
        self.ever_received_fresh = true;
        self.deadline = Some((
            observed_at + self.timing.steady_silence_threshold,
            RtdsBusinessDeadlineKind::Steady,
        ));
        self.episode_trigger = None;
        self.replacement_attempts = 0;
        self.next_backoff_level = 1;
    }

    pub(super) fn next_deadline(&self) -> Option<tokio::time::Instant> {
        self.deadline.map(|(deadline, _)| deadline)
    }

    pub(super) fn replacement_due(&mut self, now: tokio::time::Instant) -> bool {
        let Some((deadline, kind)) = self.deadline else {
            return false;
        };
        if now < deadline {
            return false;
        }
        match kind {
            RtdsBusinessDeadlineKind::Steady => {
                self.episode_trigger.get_or_insert("steady_silence");
                true
            }
            RtdsBusinessDeadlineKind::Backoff => true,
            RtdsBusinessDeadlineKind::FirstFresh => {
                self.episode_trigger
                    .get_or_insert("first_fresh_window_timeout");
                let delay = retry_delay(
                    "polymarket_rtds_chainlink_btc_usd",
                    self.next_backoff_level,
                    self.timing.outer_backoff_initial_ms,
                    self.timing.outer_backoff_max_ms,
                    self.jitter_seed,
                );
                self.deadline = Some((now + delay, RtdsBusinessDeadlineKind::Backoff));
                false
            }
        }
    }

    pub(super) fn commit_replacement(&mut self) -> (&'static str, u64) {
        let trigger = self
            .episode_trigger
            .expect("replacement is always part of a recovery episode");
        self.replacement_attempts = self.replacement_attempts.saturating_add(1);
        if self
            .deadline
            .is_some_and(|(_, kind)| matches!(kind, RtdsBusinessDeadlineKind::Backoff))
        {
            self.next_backoff_level = self.next_backoff_level.saturating_add(1);
        }
        self.clear_generation_observations();
        (trigger, self.replacement_attempts)
    }

    pub(super) fn start_runtime(&mut self, generation: u64) {
        self.start_generation(generation);
    }

    fn start_generation(&mut self, generation: u64) {
        self.generation = generation;
        self.clear_generation_observations();
    }

    fn arm_first_fresh_deadline(&mut self) {
        if self.deadline.is_some() || !self.ever_received_fresh {
            return;
        }
        if let (Some(connected_at), Some(applied_at)) = (self.connected_at, self.applied_at) {
            self.deadline = Some((
                connected_at.max(applied_at) + self.timing.first_fresh_observation_window,
                RtdsBusinessDeadlineKind::FirstFresh,
            ));
        }
    }

    fn clear_generation_observations(&mut self) {
        self.connected_at = None;
        self.applied_at = None;
        self.deadline = None;
    }
}

pub(super) fn rtds_subscription_evidence_json() -> String {
    json!({
        "action": "subscribe",
        "subscriptions": [{
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": "{\"symbol\":\"btc/usd\"}"
        }]
    })
    .to_string()
}

pub(super) fn rtds_source_native_payload_json(
    message: &polymarket_client_sdk_v2::rtds::RtdsMessage,
) -> Result<String> {
    let canonical = serde_json::to_string(&json!({
        "source": "polymarket_rtds_chainlink_btc_usd",
        "topic": message.topic,
        "type": message.msg_type,
        "timestamp": message.timestamp,
        "payload": message.payload,
    }))?;
    if canonical.len() > MAX_TYPED_ROW_BYTES {
        return Err(io::Error::other(format!(
            "operator_intervention_required:RTDS source-native payload exceeds {MAX_TYPED_ROW_BYTES} bytes"
        ))
        .into());
    }
    Ok(canonical)
}

pub(super) fn restore_rtds_dedupe(
    dataset_root: &Path,
    restore_time_ms: i64,
) -> Result<TimedFingerprintWindow> {
    let mut window =
        TimedFingerprintWindow::with_limits(RTDS_DEDUPE_WINDOW_MS, RTDS_DEDUPE_MAX_KEYS);
    let mut paths = Vec::new();
    let days = BTreeSet::from([
        utc_day_string_from_ms(restore_time_ms.saturating_sub(RTDS_DEDUPE_WINDOW_MS))
            .ok_or_else(|| io::Error::other("RTDS previous UTC day is out of range"))?,
        utc_day_string_from_ms(restore_time_ms)
            .ok_or_else(|| io::Error::other("RTDS current UTC day is out of range"))?,
    ]);
    for day in days {
        paths.extend(finalized_parquet_files(
            &dataset_root
                .join("training")
                .join("typed_raw")
                .join(format!("utc_day={day}"))
                .join("source_kind=polymarket_rtds_chainlink_btc_usd"),
        )?);
    }
    paths.sort_by_key(|path| finalized_parquet_order_key(path));
    let window_start_ms = restore_time_ms.saturating_sub(RTDS_DEDUPE_WINDOW_MS);
    for path in paths {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
        if builder.schema().index_of("timestamp_received").is_err()
            || builder
                .schema()
                .index_of("source_native_payload_json")
                .is_err()
        {
            continue;
        }
        if builder.metadata().file_metadata().num_rows() == 0 {
            continue;
        }
        let projection = ProjectionMask::columns(
            builder.parquet_schema(),
            ["timestamp_received", "source_native_payload_json"],
        );
        let mut reader = builder
            .with_projection(projection)
            .with_batch_size(16)
            .build()?;
        for batch in &mut reader {
            let batch = batch?;
            let received_index = batch.schema().index_of("timestamp_received")?;
            let payload_index = batch.schema().index_of("source_native_payload_json")?;
            let received = batch
                .column(received_index)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| io::Error::other("RTDS restore receive time is not timestamp-ms"))?;
            let payloads = batch
                .column(payload_index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| io::Error::other("RTDS restore payload is not UTF8"))?;
            for row in 0..batch.num_rows() {
                if received.is_null(row) || payloads.is_null(row) {
                    return Err(io::Error::other(format!(
                        "RTDS restore key missing in {}",
                        path.display()
                    ))
                    .into());
                }
                let received_ms = received.value(row);
                if received_ms < window_start_ms {
                    continue;
                }
                let payload = payloads.value(row);
                if payload.len() > MAX_TYPED_ROW_BYTES {
                    return Err(io::Error::other(format!(
                        "operator_intervention_required:RTDS restore payload exceeds {MAX_TYPED_ROW_BYTES} bytes in {}",
                        path.display()
                    ))
                    .into());
                }
                let _ = window
                    .observe(payload.as_bytes(), received_ms)
                    .map_err(operator_dedupe_error)?;
            }
        }
    }
    Ok(window)
}

fn create_rtds_runtime(
    spec: &RtdsSourceSpec,
    subscription: &Subscription,
) -> Result<RtdsSdkRuntime> {
    let mut runtime = RtdsSdkRuntime::new(spec.endpoint.clone(), spec.ws_config.clone())?;
    runtime.replace_subscription(subscription.clone())?;
    Ok(runtime)
}

pub(super) async fn run_rtds_source_with_spec(
    spec: RtdsSourceSpec,
    dataset_root: PathBuf,
    sink: SourceEventSink,
    collector_identity: CollectorIdentity,
    deadline: Option<Instant>,
    run_started: Instant,
) -> Result<()> {
    let restore_time_ms = now_ms();
    let mut dedupe =
        tokio::task::spawn_blocking(move || restore_rtds_dedupe(&dataset_root, restore_time_ms))
            .await
            .map_err(|error| {
                invalid_data(format!(
                    "operator_intervention_required:RTDS dedupe restore join failed:{error}"
                ))
            })?
            .map_err(|error| {
                invalid_data(format!(
                    "operator_intervention_required:RTDS dedupe restore failed:{error}"
                ))
            })?;
    let subscription = Subscription::chainlink_prices(Some("btc/usd".to_owned()));
    let mut rtds_runtime_epoch = 0_u64;
    let mut runtime = create_rtds_runtime(&spec, &subscription)?;
    let mut recorded_generation = runtime.connection_generation();
    let jitter_seed = u64::try_from(now_ms()).unwrap_or_default();
    let mut business_recovery =
        RtdsBusinessRecovery::new(spec.business_timing, jitter_seed, recorded_generation);

    loop {
        let business_deadline = business_recovery.next_deadline();
        let runtime_result = tokio::select! {
            biased;
            result = runtime.recv_event_until(deadline) => Some(result),
            () = async {
                if let Some(deadline) = business_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => None,
        };
        if let Some(result) = runtime_result {
            match result {
                Ok(Some(RtdsRuntimeEvent::State { state, generation })) => {
                    record_rtds_generation_advance(&mut recorded_generation, generation, &sink)?;
                    business_recovery.observe_state(state, generation, tokio::time::Instant::now());
                }
                Ok(Some(RtdsRuntimeEvent::Applied(applied_event))) => {
                    record_rtds_generation_advance(
                        &mut recorded_generation,
                        applied_event.generation,
                        &sink,
                    )?;
                    business_recovery
                        .observe_applied(applied_event.generation, tokio::time::Instant::now());
                }
                Ok(Some(RtdsRuntimeEvent::Message(message))) => {
                    let observed_at = tokio::time::Instant::now();
                    let freshness_generation = message.freshness_generation;
                    let observed_generation = freshness_generation.unwrap_or(recorded_generation);
                    record_rtds_generation_advance(
                        &mut recorded_generation,
                        observed_generation,
                        &sink,
                    )?;
                    let is_fresh = emit_rtds_message(
                        &sink,
                        &collector_identity,
                        run_started,
                        &mut dedupe,
                        message.message,
                    )
                    .await?;
                    if is_fresh && let Some(generation) = freshness_generation {
                        business_recovery.observe_fresh(generation, observed_at);
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    if let SdkStreamError::Lagged(count) = &error {
                        sink.source_queue_backpressure.record_lag(
                            "polymarket_rtds_chainlink_btc_usd",
                            now_ms(),
                            *count,
                        );
                    }
                    record_gap(&sink.gaps, format!("rtds_sdk_stream_fatal:{error}"));
                    return Err(error.into());
                }
            }
        }

        let now = tokio::time::Instant::now();
        if business_recovery
            .next_deadline()
            .is_none_or(|deadline| deadline > now)
        {
            continue;
        }
        let state = runtime.connection_state();
        let generation = runtime.connection_generation();
        let desired_revision = runtime.subscription_revision();
        let subscription_applied = runtime.applied_subscription().is_some_and(|applied| {
            applied.generation == generation && applied.revision >= desired_revision
        });
        business_recovery.reconcile_current(state, generation, subscription_applied, now);
        if !business_recovery.replacement_due(now) {
            continue;
        }

        let (trigger, attempt_index) = business_recovery.commit_replacement();
        eprintln!(
            "rtds_business_replacement_start trigger_kind={trigger} rtds_runtime_epoch={rtds_runtime_epoch} sdk_connection_generation={generation} attempt_index={attempt_index}"
        );
        runtime.shutdown_and_wait().await;

        rtds_runtime_epoch += 1;
        runtime = create_rtds_runtime(&spec, &subscription)?;
        recorded_generation = runtime.connection_generation();
        business_recovery.start_runtime(recorded_generation);
        eprintln!(
            "rtds_business_runtime_created trigger_kind={trigger} rtds_runtime_epoch={rtds_runtime_epoch} sdk_connection_generation={recorded_generation} attempt_index={attempt_index}"
        );
    }
    Ok(())
}

fn record_rtds_generation_advance(
    recorded_generation: &mut u64,
    generation: u64,
    sink: &SourceEventSink,
) -> Result<()> {
    let newly_observed = generation
        .checked_sub(*recorded_generation)
        .ok_or_else(|| invalid_data("RTDS recorded connection generation regressed"))?;
    if newly_observed == 0 {
        return Ok(());
    }
    for _ in 0..newly_observed {
        sink.source_queue_backpressure
            .record_reconnect("polymarket_rtds_chainlink_btc_usd", now_ms());
    }
    *recorded_generation = generation;
    Ok(())
}

pub(super) async fn emit_rtds_message(
    sink: &SourceEventSink,
    collector_identity: &CollectorIdentity,
    run_started: Instant,
    dedupe: &mut TimedFingerprintWindow,
    message: polymarket_client_sdk_v2::rtds::RtdsMessage,
) -> Result<bool> {
    let recv_time_utc_ms = now_ms();
    let classified = match classify_rtds_message(&message, recv_time_utc_ms) {
        Ok(Some(classified)) => classified,
        Ok(None) => return Ok(false),
        Err(error) => {
            record_gap(
                &sink.gaps,
                format!("rtds_unparseable_chainlink_payload:{error}"),
            );
            return Err(error);
        }
    };
    let source_native_payload_json = rtds_source_native_payload_json(&message)?;
    if dedupe
        .observe(source_native_payload_json.as_bytes(), recv_time_utc_ms)
        .map_err(operator_dedupe_error)?
        == FingerprintDecision::Duplicate
    {
        sink.source_queue_backpressure.record_structural_dedupe(
            "polymarket_rtds_chainlink_btc_usd",
            recv_time_utc_ms,
            1,
        );
        return Ok(false);
    }
    let source_ts = classified.payload.timestamp;
    let source_identifier = classified.payload.symbol.clone();
    let value = classified.payload.value.to_string();
    let is_fresh = classified.freshness_status == "fresh";
    send_event(
        sink,
        run_started,
        RawEvent {
            source_kind: "polymarket_rtds_chainlink_btc_usd".to_owned(),
            source_event_type: message.msg_type.clone(),
            source_identifier,
            source_ts: Some(source_ts.to_string()),
            recv_time_utc_ms,
            collector_host_id: collector_identity.host_id.clone(),
            collector_region: collector_identity.region.clone(),
            source_endpoint: RTDS_WS_ENDPOINT.to_owned(),
            raw_json: json!({
                "topic": message.topic,
                "type": message.msg_type,
                "message_timestamp": message.timestamp,
                "payload_timestamp": classified.payload.timestamp,
                "symbol": classified.payload.symbol,
                "value": value,
                "age_ms": classified.age_ms,
                "freshness_status": classified.freshness_status,
                "subscription_raw_json": rtds_subscription_evidence_json(),
                "payload": message.payload,
                "source_native_payload_json": source_native_payload_json,
            })
            .to_string(),
            collector_admission: None,
        },
    )
    .await?;
    Ok(is_fresh)
}
