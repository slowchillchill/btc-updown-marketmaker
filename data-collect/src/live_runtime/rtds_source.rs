use super::*;

pub(super) const RTDS_WS_ENDPOINT: &str = "wss://ws-live-data.polymarket.com";
const RTDS_FRESH_MIN_AGE_MS: i64 = -5_000;
const RTDS_FRESH_MAX_AGE_MS: i64 = 30_000;
pub(super) const RTDS_DEDUPE_WINDOW_MS: i64 = 60 * 60 * 1000;
const RTDS_DEDUPE_MAX_KEYS: usize = 100_000;

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
}

impl RtdsSourceSpec {
    pub(super) fn production() -> Self {
        Self {
            endpoint: RTDS_WS_ENDPOINT.to_owned(),
            ws_config: bounded_rtds_ws_config(),
        }
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
    let mut runtime = RtdsSdkRuntime::new(spec.endpoint.clone(), spec.ws_config.clone())?;
    runtime.replace_subscription(subscription)?;
    let mut recorded_generation = runtime.connection_generation();

    loop {
        match runtime.recv_event_until(deadline).await {
            Ok(Some(RtdsRuntimeEvent::Applied(applied))) => {
                record_rtds_generation_advance(
                    &mut recorded_generation,
                    applied.generation,
                    &sink,
                )?;
            }
            Ok(Some(RtdsRuntimeEvent::Message(message))) => {
                let observed_generation = message
                    .freshness_generation
                    .unwrap_or_else(|| runtime.connection_generation());
                record_rtds_generation_advance(
                    &mut recorded_generation,
                    observed_generation,
                    &sink,
                )?;
                emit_rtds_message(
                    &sink,
                    &collector_identity,
                    run_started,
                    &mut dedupe,
                    message.message,
                )
                .await?;
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
            parquet_raw_layout_version: 1,
            parquet_raw_source_kind: "polymarket_rtds_chainlink_btc_usd".to_owned(),
        },
    )
    .await?;
    Ok(is_fresh)
}
