use super::*;
use crate::deployment::CollectorStartupReceipt;
use crate::handoff::{receive_confirmation, write_handoff_body, write_handoff_v2};
use data_pipeline_core::public_input::DataCollectHandoffV2;
use tokio::net::UnixStream;

pub(crate) async fn run_hot_sources(
    stream: UnixStream,
    receipt: CollectorStartupReceipt,
    run_started: Instant,
    deadline: Option<Instant>,
    deadline_ms: Option<i64>,
) -> Result<()> {
    run_hot_sources_with_specs(
        stream,
        receipt,
        run_started,
        deadline,
        deadline_ms,
        TrainingSourceSpecs::production(),
    )
    .await
}

pub(super) async fn run_hot_sources_with_specs(
    stream: UnixStream,
    receipt: CollectorStartupReceipt,
    run_started: Instant,
    deadline: Option<Instant>,
    deadline_ms: Option<i64>,
    specs: TrainingSourceSpecs,
) -> Result<()> {
    let session_gate = HotSessionGate::new(deadline);
    #[cfg(test)]
    let session_gate = {
        let mut gate = session_gate;
        gate.comparison_root = std::env::var_os("DATA_COLLECT_HOT_COMPARISON_ROOT")
            .map(PathBuf::from)
            .map(|root| root.join(&receipt.run_id));
        gate
    };
    let gate = Arc::new(session_gate);
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    stats.install_hot_session(Arc::clone(&gate))?;
    let (tx, rx) = bounded_event_queue::<SourceIngressPayload>(DEFAULT_QUEUE_CAPACITY)?;
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: stats,
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    let identity = CollectorIdentity {
        host_id: receipt.collector_host_id,
        region: receipt.collector_region,
    };
    let (pm_tx, pm_rx) = mpsc::channel(POLYMARKET_CONTROL_CHANNEL_CAPACITY);
    let (depth_tx, depth_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let (agg_tx, agg_rx) = mpsc::channel(AGGTRADE_CONTROL_CHANNEL_CAPACITY);
    let mut sources = JoinSet::new();
    sources.spawn(observe_hot_source_return(
        "polymarket_market",
        Box::pin(run_polymarket_source_with_spec(
            specs.polymarket,
            receipt.dataset_root_canonical,
            sink.fork_logical_producer(),
            identity.clone(),
            deadline_ms,
            run_started,
            pm_rx,
            PolymarketRestartRecovery {
                owners: Vec::new(),
                ws_claims: BTreeMap::new(),
                pending: Vec::new(),
                fixed_identities: RecoveredPolymarketFixedIdentities::default(),
            },
        )),
        Arc::clone(&gate),
    ));
    sources.spawn(observe_hot_source_return(
        "polymarket_rtds_chainlink_btc_usd",
        run_rtds_source_with_dedupe(
            specs.rtds,
            TimedFingerprintWindow::with_limits(RTDS_DEDUPE_WINDOW_MS, RTDS_DEDUPE_MAX_KEYS),
            sink.fork_logical_producer(),
            identity.clone(),
            deadline,
            run_started,
        ),
        Arc::clone(&gate),
    ));
    sources.spawn(observe_hot_source_return(
        "binance_depth_update",
        run_binance_depth_source_with_spec_and_control(
            specs.depth,
            sink.fork_logical_producer(),
            identity.clone(),
            deadline,
            run_started,
            Some(depth_rx),
        ),
        Arc::clone(&gate),
    ));
    sources.spawn(observe_hot_source_return(
        "binance_aggtrade",
        run_binance_ws_source(
            BinanceAggTradeRecovery {
                websocket_dedupe: BinanceRecentIdWindow::with_limit(BINANCE_AGGTRADE_RECOVERY_IDS),
            },
            specs.aggtrade,
            sink.fork_logical_producer(),
            identity,
            deadline,
            run_started,
            agg_rx,
        ),
        Arc::clone(&gate),
    ));
    drop(sink);

    // 截止/故障监督独立于下游确认等待，仍使用原来源 Fence 排空。
    let stop_gate = Arc::clone(&gate);
    let controller = tokio::spawn(async move {
        let admission = SourceAdmissionGate::Hot(Arc::clone(&stop_gate));
        tokio::select! {
            () = shutdown_signal() => stop_gate.stop(RuntimeCloseCause::OperatorStop),
            () = async {
                if let Some(deadline) = deadline {
                    tokio::time::sleep_until(deadline.into()).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => stop_gate.stop(RuntimeCloseCause::RunDeadline),
            () = admission.wait_for_hot_cutoff() => {},
        }
        let (reply, _) = oneshot::channel();
        let _ = pm_tx
            .send(PolymarketSourceControl::Fence {
                stop_after_fence: true,
                day_boundary_utc_ms: None,
                resume_after_barrier: None,
                reply,
            })
            .await;
        let (reply, _) = oneshot::channel();
        let _ = depth_tx
            .send(DepthSourceControl::Fence {
                kind: DepthFenceKind::ResetContinuity,
                stop_after_fence: true,
                resume_after_barrier: None,
                reply,
            })
            .await;
        let (reply, _) = oneshot::channel();
        let _ = agg_tx
            .send(AggTradeSourceControl::Fence {
                stop_after_fence: true,
                resume_after_barrier: None,
                reply,
            })
            .await;
    });

    drain_hot_sources(stream, rx, sources, controller, gate).await
}

pub(super) async fn drain_hot_sources(
    mut stream: UnixStream,
    mut rx: mpsc::Receiver<SourceIngressPayload>,
    mut sources: JoinSet<SourceTaskTermination>,
    controller: tokio::task::JoinHandle<()>,
    gate: Arc<HotSessionGate>,
) -> Result<()> {
    let mut last_confirmed_sequence = 0;
    let mut failure = None;
    let mut connection_usable = true;
    let mut input_open = true;
    while input_open || !sources.is_empty() {
        tokio::select! {
            report = sources.join_next(), if !sources.is_empty() => {
                match report {
                    Some(Ok(report)) => if let Some(error) = report.error { append_runtime_failure(&mut failure, format!("{}:{error}", report.source_kind)); },
                    Some(Err(error)) => {
                        let reason = format!("hot source join failed:{error}");
                        gate.stop(RuntimeCloseCause::CoordinatorFailure { stage: "hot_source_join" });
                        gate.fail(reason.clone());
                        append_runtime_failure(&mut failure, reason);
                    },
                    None => {},
                }
            }
            payload = rx.recv(), if input_open => {
                let Some(payload) = payload else { input_open = false; continue; };
                if !connection_usable { drop(payload); continue; }
                let result = send_public_payload(&mut stream, payload, last_confirmed_sequence + 1, &gate, &mut connection_usable).await;
                match result {
                    Ok(true) => last_confirmed_sequence += 1,
                    Ok(false) => {},
                    Err(error) => {
                        let reason = error.to_string();
                        gate.stop(RuntimeCloseCause::CoordinatorFailure { stage: "hot_handoff" });
                        gate.fail(reason.clone());
                        append_runtime_failure(&mut failure, reason);
                    }
                }
            }
        }
    }
    if let Err(error) = controller.await {
        append_runtime_failure(&mut failure, error.to_string());
    }
    if let Some(error) = gate.failure() {
        append_runtime_failure(&mut failure, error);
    }
    if let Some(reason) = failure {
        if connection_usable {
            let _ = write_handoff_v2(
                &mut stream,
                &DataCollectHandoffV2::Aborted {
                    last_confirmed_sequence,
                    reason: reason.clone(),
                },
            )
            .await;
        }
        return Err(invalid_data(format!(
            "hot_handoff_aborted:last_confirmed_sequence={last_confirmed_sequence}:{reason}"
        ))
        .into());
    }
    write_handoff_v2(
        &mut stream,
        &DataCollectHandoffV2::End {
            last_confirmed_sequence,
        },
    )
    .await
}

pub(super) async fn send_public_payload(
    stream: &mut UnixStream,
    payload: SourceIngressPayload,
    transport_sequence: u64,
    _gate: &HotSessionGate,
    connection_usable: &mut bool,
) -> Result<bool> {
    #[cfg(test)]
    if let Some(root) = &_gate.comparison_root {
        crate::training_runtime::tests::hot_runtime::record_same_input_cold_parquet(
            root,
            transport_sequence,
            &payload,
        )?;
    }
    let (row, reservation, row_reservation, byte_reservation) = match payload {
        SourceIngressPayload::RawEvent(value) => {
            let row = raw_event_to_public_input(&value.event)?;
            (row, value._reservation, None, None)
        }
        SourceIngressPayload::PolymarketOrderbookV3(value) => (
            Some(polymarket_public_input(value.message)?),
            None,
            Some(value._row_reservation),
            Some(value._byte_reservation),
        ),
    };
    let Some(row) = row else {
        return Ok(false);
    };
    #[cfg(test)]
    if let Some(root) = &_gate.comparison_root {
        fs::write(
            root.join(format!("{transport_sequence:08}.public.json")),
            serde_json::to_vec(&row)?,
        )?;
    }
    let frame = DataCollectHandoffV2::Message {
        transport_sequence,
        row,
    };
    let body = serde_json::to_vec(&frame)?;
    #[cfg(test)]
    if let Some(measurements) = _gate.send_measurements.lock().unwrap().as_mut() {
        let DataCollectHandoffV2::Message { row, .. } = &frame else {
            unreachable!()
        };
        measurements.push((
            body.len(),
            body.capacity(),
            crate::training_runtime::tests::hot_runtime::measured_new_market_public_owned_bytes(
                row,
            ),
        ));
    }
    drop(frame);
    // 此后失败可能留下半帧；只有完成对应确认才恢复可用状态。
    *connection_usable = false;
    let sent = write_handoff_body(stream, &body).await;
    drop(body);
    drop((reservation, row_reservation, byte_reservation));
    sent?;
    receive_confirmation(stream, transport_sequence).await?;
    *connection_usable = true;
    Ok(true)
}
