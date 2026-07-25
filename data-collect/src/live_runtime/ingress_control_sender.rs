use super::*;

pub(super) async fn send_ingress_control(
    sink: &SourceEventSink,
    run_started: Instant,
    control: IngressControl,
    label: &str,
) -> Result<()> {
    let _enqueue_guard = sink.enqueue_lock.lock().await;
    let message = InflightRawEvent {
        event: RawEvent {
            source_kind: "__ingress_control".to_owned(),
            source_event_type: label.to_owned(),
            source_identifier: label.to_owned(),
            source_ts: None,
            recv_time_utc_ms: now_ms(),
            collector_host_id: String::new(),
            collector_region: String::new(),
            source_endpoint: String::new(),
            raw_json: "{}".to_owned(),
            collector_admission: None,
            parquet_raw_layout_version: 0,
            parquet_raw_source_kind: "__ingress_control".to_owned(),
        },
        control: Some(control),
        admission_gaps: Vec::new(),
        raw_day_permit: None,
        _source_recv_monotonic_ms: monotonic_ms(run_started),
        _reservation: None,
    };
    let message = SourceIngressPayload::LegacyRaw(message);
    match sink.tx.try_send(message) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(message)) => {
            match timeout(BUFFER_ACQUIRE_TIMEOUT, sink.tx.send(message)).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) => Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("ingress control queue closed: {label}"),
                )
                .into()),
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("ingress control queue timeout: {label}"),
                )
                .into()),
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            format!("ingress control queue closed: {label}"),
        )
        .into()),
    }
}

pub(super) async fn send_day_barrier(
    sink: &SourceEventSink,
    run_started: Instant,
    producer: &str,
    close: RawDayClose,
) -> Result<DayBarrierRelease> {
    let (reply_tx, reply_rx) = oneshot::channel();
    send_ingress_control(
        sink,
        run_started,
        IngressControl::DayBarrier {
            producer: producer.to_owned(),
            close,
            reply: reply_tx,
        },
        "utc_day_barrier",
    )
    .await?;
    match reply_rx.await {
        Ok(Ok(release)) => Ok(release),
        Ok(Err(error)) => Err(invalid_data(format!(
            "producer day barrier failed before coordinated release: producer={producer} error={error}"
        ))
        .into()),
        Err(_) => Err(invalid_data(format!(
            "producer day barrier reply dropped: producer={producer}"
        ))
        .into()),
    }
}

pub(super) async fn send_source_hour_finalize(
    sink: &SourceEventSink,
    run_started: Instant,
    source_kind: &str,
    utc_hour: String,
) -> Result<()> {
    send_ingress_control(
        sink,
        run_started,
        IngressControl::FinalizeSourceHour {
            source_kind: source_kind.to_owned(),
            utc_hour,
        },
        "utc_hour_finalize",
    )
    .await
}
