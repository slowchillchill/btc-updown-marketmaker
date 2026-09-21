use super::*;
use crate::deployment::CollectorBinaryRole;
use crate::handoff::{read_handoff_v2, write_handoff_v2};
use data_pipeline_core::public_input::{
    DataCollectHandoffV2, PolymarketMarketMetadataV6Input, TypedRawRowV2,
};
use data_pipeline_core::typed_raw::TypedRawRow;

async fn shutdown_controlled_hot_fixture(fixture: ComposedLoopbackFixture) {
    for server in fixture.servers {
        server.abort();
        if let Err(error) = server.await {
            assert!(
                error.is_cancelled(),
                "controlled source server panicked: {error}"
            );
        }
    }
}

// 只在显式测试取证时保存同一准入对象的冷物理行；合成清单不是生产日关闭证据。
pub(in crate::training_runtime) fn record_same_input_cold_parquet(
    root: &Path,
    sequence: u64,
    payload: &SourceIngressPayload,
) -> Result<()> {
    let (source, received) = match payload {
        SourceIngressPayload::RawEvent(value) => (
            value.event.source_kind.as_str(),
            value.event.recv_time_utc_ms,
        ),
        SourceIngressPayload::PolymarketOrderbookV3(value) => (
            "polymarket_market",
            value.message.envelope.timestamp_received,
        ),
    };
    let day = utc_day_string_from_ms(received).unwrap();
    let utc_hour = utc_hour_string_from_ms(received).unwrap();
    let day_root = root
        .join(format!("{sequence:08}"))
        .join("typed_raw")
        .join(format!("utc_day={day}"));
    let directory = day_root.join(format!("source_kind={source}"));
    let mut writer = TypedParquetSegmentWriter::create_for_current_source(
        &directory,
        &format!("{source}_{}", &utc_hour[11..]),
        source,
    )?;
    let rows = match payload {
        SourceIngressPayload::RawEvent(value) => write_typed_event(&mut writer, &value.event)?,
        SourceIngressPayload::PolymarketOrderbookV3(value) => {
            writer.write_polymarket_orderbook_v3_messages(
                std::slice::from_ref(&value.message),
                &mut PolymarketOrderbookV3WriteSequenceState::default(),
            )?;
            1
        }
    };
    writer.append_key_value_metadata(DAILY_FILE_RECOVERY_METADATA_KEY, json!({
        "source_kind":source,"schema_version":writer.schema_version(),"utc_hour":utc_hour,"part":0,"rows":rows,
    }).to_string())?;
    let segment = writer.finalize()?;
    let day_start = received.div_euclid(UTC_DAY_MS) * UTC_DAY_MS;
    let manifest = json!({
        "test_evidence_kind":"synthetic_same_admitted_input_comparison",
        "schema_version":"data_collect_daily_manifest_v3","state":"closed","utc_day":day,
        "closure_reason":"utc_day_barrier","collection_day_coverage":{
            "schema_version":"data_collect_collection_day_coverage_v1","state":"ended_known",
            "first_started_utc_ms":day_start,"last_started_utc_ms":day_start,
            "ended_utc_ms":day_start+UTC_DAY_MS,"reactivated_within_day":false
        },
        "producer_barriers":["binance_aggtrade","binance_depth","polymarket","polymarket_rtds"],
        "writer_finalize_acks":training_source_kinds(),
        "files":[{"path":segment.final_path.strip_prefix(&day_root).unwrap(),"source_kind":source,
            "schema_version":segment.schema_version,"utc_hour":utc_hour,"part":0,"rows":segment.row_count,
            "bytes":fs::metadata(&segment.final_path)?.len(),"sha256":segment.sha256}]
    });
    fs::write(
        day_root.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_hot_roles_wait_for_session_then_deliver_all_seven_source_types() {
    exercise_real_hot_roles(Some(3)).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_hot_roles_without_deadline_deliver_until_operator_stop() {
    const CHILD: &str = "DATA_COLLECT_UNBOUNDED_HOT_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        // SIGTERM 只发给独立测试进程，不影响并行测试或宿主服务。
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "training_runtime::tests::hot_runtime::real_hot_roles_without_deadline_deliver_until_operator_stop", "--nocapture"])
            .env(CHILD, "1").output().unwrap();
        assert!(
            output.status.success(),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    exercise_real_hot_roles(None).await;
}

async fn exercise_real_hot_roles(max_runtime_secs: Option<u64>) {
    for (role, environment) in [
        (CollectorBinaryRole::PaperHot, "paper"),
        (CollectorBinaryRole::LiveHot, "live"),
    ] {
        let fixture = ComposedLoopbackFixture::spawn(ComposedLoopbackMode::healthy()).await;
        let specs = fixture.specs.clone();
        let root = tempdir().unwrap();
        // 即使原根已有损坏训练材料，热来源也不读取或恢复它。
        fs::create_dir_all(root.path().join("training/typed_raw/utc_day=2026-08-08")).unwrap();
        let old_manifest = root
            .path()
            .join("training/typed_raw/utc_day=2026-08-08/manifest.json");
        fs::write(&old_manifest, b"existing cold evidence is untouched").unwrap();
        let mut args = vec![
            "--dataset-root".into(),
            root.path().as_os_str().to_owned(),
            "--run-id".into(),
            format!("{environment}-controlled").into(),
            "--collector-region".into(),
            "fixture".into(),
        ];
        if let Some(seconds) = max_runtime_secs {
            args.extend(["--max-runtime-secs".into(), seconds.to_string().into()]);
        }
        let runtime = tokio::spawn(crate::runtime::run_hot_collector_with_runner(
            role,
            args,
            move |stream, receipt, started, deadline, deadline_ms| {
                assert_eq!(
                    deadline,
                    max_runtime_secs.map(|seconds| started + Duration::from_secs(seconds))
                );
                assert_eq!(deadline_ms.is_some(), max_runtime_secs.is_some());
                Box::pin(
                    crate::training_runtime::hot_runtime::run_hot_sources_with_specs(
                        stream,
                        receipt,
                        started,
                        deadline,
                        deadline_ms,
                        specs,
                    ),
                )
            },
        ));
        let socket = root
            .path()
            .join(environment)
            .join("data_collect_handoff/hot.sock");
        let mut stream = timeout(Duration::from_secs(30), async {
            loop {
                if let Ok(stream) = tokio::net::UnixStream::connect(&socket).await {
                    break stream;
                }
                assert!(
                    !runtime.is_finished(),
                    "role exited before socket publication"
                );
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let DataCollectHandoffV2::Session(session) = read_handoff_v2(&mut stream).await.unwrap()
        else {
            panic!("Session must be first");
        };
        assert_eq!(session.environment, environment);
        assert_eq!(session.run_id, format!("{environment}-controlled"));
        sleep(Duration::from_millis(50)).await;
        assert_eq!(fixture.counters.gamma_requests.load(Ordering::Relaxed), 0);
        assert_eq!(fixture.counters.clob_connections.load(Ordering::Relaxed), 0);
        assert_eq!(fixture.counters.rtds_connections.load(Ordering::Relaxed), 0);
        assert_eq!(
            fixture.counters.depth_connections.load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            fixture
                .counters
                .aggtrade_connections
                .load(Ordering::Relaxed),
            0
        );
        write_handoff_v2(
            &mut stream,
            &DataCollectHandoffV2::Received {
                transport_sequence: 0,
            },
        )
        .await
        .unwrap();
        let mut sources = BTreeSet::new();
        let mut windows = BTreeSet::new();
        let mut last = 0;
        let mut anchor_seen = false;
        let mut stop_sent = false;
        timeout(Duration::from_secs(15), async {
            loop {
                match read_handoff_v2(&mut stream).await.unwrap() {
                    DataCollectHandoffV2::Message {
                        transport_sequence,
                        row,
                    } => {
                        assert_eq!(transport_sequence, last + 1);
                        data_pipeline_core::public_reference::source_evidence_v3_from_typed_raw_v2(
                            &row,
                        )
                        .unwrap();
                        match &row {
                            TypedRawRowV2::Existing(row) => {
                                sources.insert(match row {
                                    TypedRawRow::PolymarketOrderbook(_) => "polymarket_market",
                                    TypedRawRow::PolymarketMarketMetadata(_) => {
                                        "polymarket_market_metadata"
                                    }
                                    TypedRawRow::PolymarketRtdsChainlinkBtcUsd(_) => {
                                        "polymarket_rtds_chainlink_btc_usd"
                                    }
                                    TypedRawRow::BinanceDepthSnapshot(_) => {
                                        "binance_depth_snapshot"
                                    }
                                    TypedRawRow::BinanceDepthUpdate(_) => "binance_depth_update",
                                    TypedRawRow::BinanceAggTrade(_) => "binance_aggtrade",
                                });
                            }
                            TypedRawRowV2::PolymarketMarketMetadataV6(row) => {
                                sources.insert("polymarket_market_metadata");
                                if let PolymarketMarketMetadataV6Input::AnchorRule(row) =
                                    row.as_ref()
                                {
                                    assert!(row.collector_admission_sequence > 0);
                                    assert!(
                                        row.collector_admission_time_utc_ms
                                            >= row.timestamp_received
                                    );
                                    if let Some(price) = &row.price_to_beat_decimal {
                                        assert_eq!(price, "64950.01421851144");
                                        assert_eq!(
                                            row.price_to_beat_source_field.as_deref(),
                                            Some("eventMetadata.priceToBeat")
                                        );
                                        assert!(
                                            row.http_body
                                                .as_ref()
                                                .unwrap()
                                                .contains("64950.01421851144")
                                        );
                                        anchor_seen = true;
                                    }
                                }
                            }
                            TypedRawRowV2::PolymarketRtdsChainlinkTwap(row) => {
                                sources.insert("polymarket_rtds_chainlink_twap");
                                windows.insert(row.window_s);
                                assert_eq!(row.full_accuracy_value, "65000100000000000000001");
                                assert!(row.collector_admission_sequence > 0);
                            }
                            TypedRawRowV2::HistoricalPriceAnchorRepairBatch(_) => {
                                panic!("hot session cannot synthesize history")
                            }
                        }
                        drop(row);
                        write_handoff_v2(
                            &mut stream,
                            &DataCollectHandoffV2::Received { transport_sequence },
                        )
                        .await
                        .unwrap();
                        last = transport_sequence;
                        if max_runtime_secs.is_none()
                            && !stop_sent
                            && sources.len() == 7
                            && anchor_seen
                        {
                            assert!(!runtime.is_finished());
                            assert!(
                                std::process::Command::new("kill")
                                    .args(["-TERM", &std::process::id().to_string()])
                                    .status()
                                    .unwrap()
                                    .success()
                            );
                            stop_sent = true;
                        }
                    }
                    DataCollectHandoffV2::End {
                        last_confirmed_sequence,
                    } => {
                        assert_eq!(last_confirmed_sequence, last);
                        break;
                    }
                    other => panic!("healthy sources must terminate normally: {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        runtime.await.unwrap().unwrap();
        assert_eq!(stop_sent, max_runtime_secs.is_none());
        assert_eq!(sources, training_source_kinds().iter().copied().collect());
        assert_eq!(windows, BTreeSet::from([60]));
        assert!(anchor_seen);
        assert_eq!(
            fs::read(old_manifest).unwrap(),
            b"existing cold evidence is untouched"
        );
        assert_eq!(utc_day_directories(root.path()).unwrap().len(), 1);
        shutdown_controlled_hot_fixture(fixture).await;
    }
}

#[tokio::test]
async fn accepted_hot_tickets_survive_midnight_cutoff_and_keep_http_and_terminal_derivatives() {
    let gate = Arc::new(HotSessionGate::new(Some(
        Instant::now() + Duration::from_secs(60),
    )));
    let admission = SourceAdmissionGate::Hot(Arc::clone(&gate));
    let midnight = 1_783_209_600_000_i64;
    let ws = admission
        .accept_unadmitted_event_ticket("polymarket", midnight - 1)
        .await
        .unwrap()
        .unwrap();
    let http = admission.hot_request_ticket().unwrap();
    assert!(
        admission
            .begin_unadmitted_event_enqueue("rtds", midnight + 1)
            .await
            .unwrap()
            .is_some()
    );
    gate.stop(RuntimeCloseCause::RunDeadline);
    assert!(
        admission
            .accept_unadmitted_event_ticket("polymarket", midnight - 1)
            .await
            .unwrap()
            .is_none()
    );
    assert!(admission.hot_request_ticket().is_none());
    // 受理时刻在截止之前，队列准入与真实 HTTP 接收可在截止之后。
    let completed = admission
        .accept_response_ticket(Some(&http), midnight + 5)
        .await
        .unwrap()
        .unwrap();
    for ticket in [&ws, &completed] {
        let SourceEnqueueDecision::Permit(permit) = admission
            .begin_accepted_ticket_enqueue("polymarket", midnight + 5, ticket)
            .await
            .unwrap()
        else {
            panic!("hot does not defer into a training day");
        };
        assert!(permit.raw_day_permit.is_none());
    }
    let terminal_metadata = admission
        .accept_derived_polymarket_event_ticket(&ws, midnight + 6)
        .await
        .unwrap();
    assert!(matches!(
        admission
            .begin_accepted_ticket_enqueue("polymarket", midnight + 6, &terminal_metadata)
            .await
            .unwrap(),
        SourceEnqueueDecision::Permit(_)
    ));
}

fn admitted_pm_message(question: &str, sequence: u64) -> PolymarketOrderbookV3Message {
    let received = 1_783_392_404_600;
    let sdk: WsMessage = serde_json::from_value(json!({
        "id":"market-current","question":question,"event_type":"new_market",
        "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
        "assets_ids":["101","102"],"outcomes":["Up","Down"],"timestamp":"1783392404599"
    }))
    .unwrap();
    PolymarketOrderbookV3Projection::from_target_admitted_sdk_message(&sdk)
        .unwrap()
        .unwrap()
        .admit(PolymarketOrderbookV3Admission {
            timestamp_received: received,
            collector_host_id: "test-host".to_owned(),
            collector_region: "test-region".to_owned(),
            source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
            source_sequence_epoch_id: "hot-test-epoch".to_owned(),
            collector_admission_sequence: sequence,
            collector_admission_time_utc_ms: received + 1,
        })
        .unwrap()
}

#[tokio::test]
async fn hot_price_change_keeps_all_members_in_one_socket_message() {
    let received = now_ms();
    let sdk: WsMessage = serde_json::from_value(json!({
        "event_type":"price_change", "market":"0x0000000000000000000000000000000000000000000000000000000000000002", "timestamp":"1783392404599",
        "price_changes":[
            {"asset_id":"101","price":"0.41","size":"120.5","side":"BUY","best_bid":"0.41","best_ask":"0.42"},
            {"asset_id":"102","price":"0.59","size":"0","side":"SELL","best_bid":"0.58","best_ask":"0.59"}
        ]
    })).unwrap();
    let message = PolymarketOrderbookV3Projection::from_target_admitted_sdk_message(&sdk)
        .unwrap()
        .unwrap()
        .admit(PolymarketOrderbookV3Admission {
            timestamp_received: received,
            collector_host_id: "test-host".to_owned(),
            collector_region: "test-region".to_owned(),
            source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
            source_sequence_epoch_id: "test-epoch".to_owned(),
            collector_admission_sequence: 1,
            collector_admission_time_utc_ms: received + 1,
        })
        .unwrap();
    let budget = InflightBudget::new();
    let owned = message.owned_bytes();
    let payload = SourceIngressPayload::PolymarketOrderbookV3(InflightPolymarketOrderbookV3 {
        message,
        raw_day_permit: None,
        _source_recv_monotonic_ms: 0,
        _row_reservation: budget.try_acquire(1, 0),
        _byte_reservation: budget.try_acquire(0, owned),
    });
    let mut gate = HotSessionGate::new(Some(Instant::now() + Duration::from_secs(60)));
    gate.comparison_root = std::env::var_os("DATA_COLLECT_HOT_COMPARISON_ROOT")
        .map(PathBuf::from)
        .map(|path| path.join("price-change-members-controlled"));
    let (mut sender, mut reader) = tokio::net::UnixStream::pair().unwrap();
    let task = tokio::spawn(async move {
        crate::training_runtime::hot_runtime::send_public_payload(
            &mut sender,
            payload,
            1,
            &gate,
            &mut true,
        )
        .await
    });
    let DataCollectHandoffV2::Message {
        transport_sequence: 1,
        row: TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)),
    } = read_handoff_v2(&mut reader).await.unwrap()
    else {
        panic!("one whole price_change message");
    };
    let data_pipeline_core::typed_raw::PolymarketOrderbookEvent::PriceChange { price_changes } =
        &row.event
    else {
        panic!("price_change");
    };
    assert_eq!(price_changes.len(), 2);
    assert_eq!(price_changes[0].asset_id, "101");
    assert_eq!(price_changes[0].price, "0.41");
    assert_eq!(price_changes[0].size.as_deref(), Some("120.5"));
    assert_eq!(price_changes[1].asset_id, "102");
    assert_eq!(price_changes[1].size.as_deref(), Some("0"));
    assert_eq!(price_changes[1].best_ask.as_deref(), Some("0.59"));
    drop(row);
    write_handoff_v2(
        &mut reader,
        &DataCollectHandoffV2::Received {
            transport_sequence: 1,
        },
    )
    .await
    .unwrap();
    assert!(task.await.unwrap().unwrap());
}

// 本资源夹具的 NewMarket 所有动态字段逐项计量；其他类型明确没有该指标。
pub(in crate::training_runtime) fn measured_new_market_public_owned_bytes(
    row: &TypedRawRowV2,
) -> Option<usize> {
    let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)) = row else {
        return None;
    };
    let data_pipeline_core::typed_raw::PolymarketOrderbookEvent::NewMarket {
        clob_market_id,
        question,
        description,
        asset_ids,
        outcomes,
        event_message,
    } = &row.event
    else {
        return None;
    };
    let strings = |values: &Vec<String>| {
        values.capacity() * std::mem::size_of::<String>()
            + values.iter().map(String::capacity).sum::<usize>()
    };
    let optional = |value: &Option<String>| value.as_ref().map_or(0, String::capacity);
    Some(
        std::mem::size_of::<TypedRawRowV2>()
            + row.market.capacity()
            + optional(&row.slug)
            + row.collector_host_id.capacity()
            + row.collector_region.capacity()
            + row.source_endpoint.capacity()
            + row.schema_version.capacity()
            + row.source_sequence_epoch_id.capacity()
            + clob_market_id.capacity()
            + optional(question)
            + optional(description)
            + strings(asset_ids)
            + strings(outcomes)
            + event_message.as_ref().map_or(0, |event| {
                event.id.capacity()
                    + optional(&event.ticker)
                    + optional(&event.slug)
                    + optional(&event.title)
                    + optional(&event.description)
            }),
    )
}

#[tokio::test]
async fn hot_handoff_resource_prefixes_measure_source_encoding_and_receive_ownership() {
    const N: u64 = 64;
    let budget = InflightBudget::new();
    let gate = Arc::new(HotSessionGate::new(Some(
        Instant::now() + Duration::from_secs(60),
    )));
    *gate.send_measurements.lock().unwrap() = Some(Vec::new());
    let (tx, mut rx) = bounded_event_queue(DEFAULT_QUEUE_CAPACITY).unwrap();
    for sequence in 1..=N {
        let message = admitted_pm_message(&"\u{0001}".repeat(200_000), sequence);
        let owned = message.owned_bytes();
        tx.send(SourceIngressPayload::PolymarketOrderbookV3(
            InflightPolymarketOrderbookV3 {
                message,
                raw_day_permit: None,
                _source_recv_monotonic_ms: 0,
                _row_reservation: budget.try_acquire(1, 0),
                _byte_reservation: budget.try_acquire(0, owned),
            },
        ))
        .await
        .unwrap();
    }
    drop(tx);
    let (mut writer, mut reader) = tokio::net::UnixStream::pair().unwrap();
    let sender_gate = Arc::clone(&gate);
    let sender = tokio::spawn(async move {
        let mut sequence = 0;
        let mut usable = true;
        while let Some(payload) = rx.recv().await {
            crate::training_runtime::hot_runtime::send_public_payload(
                &mut writer,
                payload,
                sequence + 1,
                &sender_gate,
                &mut usable,
            )
            .await
            .unwrap();
            sequence += 1;
        }
        assert!(usable);
        assert_eq!(sequence, N);
    });
    for sequence in 1..=N {
        let body = crate::handoff::read_handoff_body(&mut reader)
            .await
            .unwrap();
        let frame: DataCollectHandoffV2 = serde_json::from_slice(&body).unwrap();
        let DataCollectHandoffV2::Message {
            transport_sequence,
            row,
        } = frame
        else {
            panic!("business frame");
        };
        assert_eq!(transport_sequence, sequence);
        let receive_capacity = body.capacity();
        let decoded_owned = measured_new_market_public_owned_bytes(&row).unwrap();
        drop(body);
        drop(row);
        tokio::task::yield_now().await;
        if [N / 4, N / 2, N].contains(&sequence) {
            let (length, capacity, public_owned) =
                gate.send_measurements.lock().unwrap().as_ref().unwrap()[sequence as usize - 1];
            let (available_rows, available_bytes) = budget.available();
            assert_eq!(
                available_rows,
                GLOBAL_INFLIGHT_MAX_ROWS - (N - sequence) as usize
            );
            eprintln!(
                "{}",
                json!({
                    "test":"hot_handoff_resource_prefixes","prefix":sequence,"N":N,
                    "source_held_rows":GLOBAL_INFLIGHT_MAX_ROWS-available_rows,
                    "source_held_bytes":GLOBAL_INFLIGHT_MAX_BYTES-available_bytes,
                    "public_owned_bytes_before_encode_release":public_owned,
                    "send_body_length":length,"send_body_capacity":capacity,
                    "receive_body_capacity":receive_capacity,"decoded_owned_bytes":decoded_owned,
                    "encoding_overlap_objects":{"public":1,"body":1},
                    "decode_overlap_objects":{"body":1,"decoded":1},
                    "awaiting_received_source_objects":0,"awaiting_received_send_bodies":0,
                    "scope":"PM NewMarket 有限夹具；来源额度不代表进程堆峰值"
                })
            );
            assert!(!sender.is_finished(), "prefix remains unconfirmed");
        }
        write_handoff_v2(
            &mut reader,
            &DataCollectHandoffV2::Received {
                transport_sequence: sequence,
            },
        )
        .await
        .unwrap();
    }
    sender.await.unwrap();
    assert_eq!(
        budget.available(),
        (GLOBAL_INFLIGHT_MAX_ROWS, GLOBAL_INFLIGHT_MAX_BYTES)
    );
}

#[tokio::test]
async fn hot_one_mib_source_rule_is_not_a_frame_cap_or_a_second_budget_acquire() {
    let message = admitted_pm_message(&"\u{0001}".repeat(200_000), 1);
    let owned = message.owned_bytes();
    assert!(owned < MAX_TYPED_ROW_BYTES);
    let budget = InflightBudget::new();
    let payload = SourceIngressPayload::PolymarketOrderbookV3(InflightPolymarketOrderbookV3 {
        message,
        raw_day_permit: None,
        _source_recv_monotonic_ms: 0,
        _row_reservation: budget.try_acquire(1, 0),
        _byte_reservation: budget.try_acquire(0, owned),
    });
    let remaining = budget.try_acquire(
        GLOBAL_INFLIGHT_MAX_ROWS - 1,
        GLOBAL_INFLIGHT_MAX_BYTES - owned,
    );
    assert_eq!(budget.available(), (0, 0));
    let (mut writer, mut reader) = tokio::net::UnixStream::pair().unwrap();
    let sender = tokio::spawn(async move {
        let gate = HotSessionGate::new(Some(Instant::now() + Duration::from_secs(60)));
        let mut usable = true;
        crate::training_runtime::hot_runtime::send_public_payload(
            &mut writer,
            payload,
            1,
            &gate,
            &mut usable,
        )
        .await
        .unwrap();
        assert!(usable);
    });
    sleep(Duration::from_millis(20)).await;
    assert!(!sender.is_finished());
    assert_eq!(
        budget.available(),
        (0, 0),
        "source permit stays held while write_all is blocked"
    );
    let DataCollectHandoffV2::Message {
        transport_sequence,
        row,
    } = timeout(Duration::from_secs(1), read_handoff_v2(&mut reader))
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("complete business object");
    };
    let body_len = serde_json::to_vec(&DataCollectHandoffV2::Message {
        transport_sequence,
        row: row.clone(),
    })
    .unwrap()
    .len();
    assert!(
        body_len > MAX_TYPED_ROW_BYTES,
        "valid escaped frame exceeds the source-object limit"
    );
    let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)) = row else {
        panic!("PM row");
    };
    let data_pipeline_core::typed_raw::PolymarketOrderbookEvent::NewMarket { question, .. } =
        row.event
    else {
        panic!("complete new_market");
    };
    assert_eq!(question.as_ref().unwrap().as_bytes(), vec![1; 200_000]);
    drop(question);
    tokio::task::yield_now().await;
    assert_eq!(
        budget.available(),
        (1, owned),
        "sending body and source reservation are released before Received"
    );
    assert!(
        !sender.is_finished(),
        "write_all completion alone is not a confirmation"
    );
    write_handoff_v2(
        &mut reader,
        &DataCollectHandoffV2::Received {
            transport_sequence: 1,
        },
    )
    .await
    .unwrap();
    sender.await.unwrap();
    drop(remaining);
    assert_eq!(
        budget.available(),
        (GLOBAL_INFLIGHT_MAX_ROWS, GLOBAL_INFLIGHT_MAX_BYTES)
    );
}

#[tokio::test]
async fn hot_source_failure_keeps_a_healthy_unconfirmed_message_deliverable() {
    let message = admitted_pm_message("source failure", 1);
    let budget = InflightBudget::new();
    let owned = message.owned_bytes();
    let payload = SourceIngressPayload::PolymarketOrderbookV3(InflightPolymarketOrderbookV3 {
        message,
        raw_day_permit: None,
        _source_recv_monotonic_ms: 0,
        _row_reservation: budget.try_acquire(1, 0),
        _byte_reservation: budget.try_acquire(0, owned),
    });
    let gate = Arc::new(HotSessionGate::new(Some(
        Instant::now() + Duration::from_secs(60),
    )));
    let sender_gate = Arc::clone(&gate);
    let (mut writer, mut reader) = tokio::net::UnixStream::pair().unwrap();
    let sender = tokio::spawn(async move {
        let mut usable = true;
        let result = crate::training_runtime::hot_runtime::send_public_payload(
            &mut writer,
            payload,
            1,
            &sender_gate,
            &mut usable,
        )
        .await;
        assert!(usable);
        result
    });
    let received = read_handoff_v2(&mut reader).await.unwrap();
    assert!(matches!(
        received,
        DataCollectHandoffV2::Message {
            transport_sequence: 1,
            ..
        }
    ));
    drop(received);
    let report = observe_hot_source_return(
        "polymarket_market",
        async { Err(invalid_data("actual fixture source failure").into()) },
        Arc::clone(&gate),
    )
    .await;
    assert!(
        report
            .error
            .unwrap()
            .contains("actual fixture source failure")
    );
    tokio::task::yield_now().await;
    assert!(
        !sender.is_finished(),
        "source failure does not cancel a healthy consumer's pending confirmation"
    );
    write_handoff_v2(
        &mut reader,
        &DataCollectHandoffV2::Received {
            transport_sequence: 1,
        },
    )
    .await
    .unwrap();
    assert!(sender.await.unwrap().unwrap());
    assert!(
        gate.failure()
            .unwrap()
            .contains("actual fixture source failure")
    );
    assert_eq!(
        budget.available(),
        (GLOBAL_INFLIGHT_MAX_ROWS, GLOBAL_INFLIGHT_MAX_BYTES)
    );
}

#[tokio::test]
async fn hot_raw_enqueue_already_waiting_for_capacity_survives_cutoff() {
    let gate = Arc::new(HotSessionGate::new(Some(
        Instant::now() + Duration::from_secs(60),
    )));
    let stats = Arc::new(SourceQueueBackpressureStats::default());
    stats.install_hot_session(Arc::clone(&gate)).unwrap();
    let (tx, mut rx) = mpsc::channel(1);
    let sink = SourceEventSink {
        tx,
        gaps: Arc::new(Mutex::new(SourceGapStore::default())),
        source_queue_backpressure: Arc::clone(&stats),
        inflight_budget: InflightBudget::new(),
        enqueue_lock: Arc::new(AsyncMutex::new(())),
    };
    send_event(
        &sink,
        Instant::now(),
        test_raw_event("binance_aggtrade", 1_783_392_404_599),
    )
    .await
    .unwrap();
    let later_sink = sink.clone();
    let waiting = tokio::spawn(async move {
        send_event(
            &later_sink,
            Instant::now(),
            test_raw_event("binance_aggtrade", 1_783_392_404_600),
        )
        .await
    });
    timeout(Duration::from_secs(1), async {
        while stats.wait_count.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    gate.stop(RuntimeCloseCause::RunDeadline);
    assert!(!waiting.is_finished());
    drop(rx.recv().await.unwrap());
    waiting.await.unwrap().unwrap();
    let accepted = rx.recv().await.unwrap().into_raw();
    assert!(accepted.raw_day_permit.is_none());
    assert_eq!(accepted.event.collector_admission.unwrap().sequence, 2);
    let outcome = send_event_with_admission_outcome(
        &sink,
        Instant::now(),
        test_raw_event("binance_aggtrade", 1_783_392_404_601),
    )
    .await
    .unwrap();
    assert_eq!(outcome, EventAdmissionOutcome::Cutoff);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_role_drains_accepted_anchor_after_real_source_failure_before_aborted() {
    let fixture = ComposedLoopbackFixture::spawn(ComposedLoopbackMode::healthy()).await;
    let mut specs = fixture.specs.clone();
    let gamma_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    specs.polymarket.gamma_endpoint = format!("http://{}", gamma_listener.local_addr().unwrap());
    let (anchor_started_tx, anchor_started_rx) = oneshot::channel();
    let (anchor_release_tx, anchor_release_rx) = oneshot::channel();
    let gamma_server = tokio::spawn(async move {
        let mut anchor_started = Some(anchor_started_tx);
        let mut release = Some(anchor_release_rx);
        loop {
            let (mut stream, _) = gamma_listener.accept().await.unwrap();
            let request = read_loopback_http_request(&mut stream).await.unwrap();
            let windows = discovery_windows(now_ms());
            if request.starts_with("GET /events/slug/") {
                anchor_started.take().unwrap().send(()).unwrap();
                release.take().unwrap().await.unwrap();
                let markets = gamma_market_response_for_window(&windows.current);
                let body = format!(
                    r#"{{"id":"delayed-anchor","slug":"{}","eventMetadata":{{"priceToBeat":6.49500142185114400000001e4}},"markets":{markets}}}"#,
                    windows.current.slug
                );
                write_loopback_http_response(&mut stream, "200 OK", &body).await;
                break;
            }
            let body = if request.contains(&windows.next.slug) {
                "[]".to_owned()
            } else {
                gamma_market_response_for_window(&windows.current)
            };
            write_loopback_http_response(&mut stream, "200 OK", &body).await;
        }
    });
    let depth_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    specs.depth.snapshot_endpoint = format!(
        "http://{}/api/v3/depth",
        depth_listener.local_addr().unwrap()
    );
    let (depth_requested_tx, depth_requested_rx) = oneshot::channel();
    let (fail_depth_tx, fail_depth_rx) = oneshot::channel();
    let depth_server = tokio::spawn(async move {
        let (mut stream, _) = depth_listener.accept().await.unwrap();
        read_loopback_http_request(&mut stream).await.unwrap();
        depth_requested_tx.send(()).unwrap();
        fail_depth_rx.await.unwrap();
        // 已有“200 但缺少 lastUpdateId”致命解析边界；不添加来源健康规则。
        write_loopback_http_response(&mut stream, "200 OK", "{}").await;
    });
    let root = tempdir().unwrap();
    let args = vec![
        "--dataset-root".into(),
        root.path().as_os_str().to_owned(),
        "--run-id".into(),
        "drain-on-failure".into(),
        "--collector-region".into(),
        "fixture".into(),
        "--max-runtime-secs".into(),
        "10".into(),
    ];
    let runtime = tokio::spawn(crate::runtime::run_hot_collector_with_runner(
        CollectorBinaryRole::PaperHot,
        args,
        move |stream, receipt, started, deadline, deadline_ms| {
            Box::pin(
                crate::training_runtime::hot_runtime::run_hot_sources_with_specs(
                    stream,
                    receipt,
                    started,
                    deadline,
                    deadline_ms,
                    specs,
                ),
            )
        },
    ));
    let socket = root.path().join("paper/data_collect_handoff/hot.sock");
    let mut stream = timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(stream) = tokio::net::UnixStream::connect(&socket).await {
                break stream;
            }
            assert!(!runtime.is_finished());
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        read_handoff_v2(&mut stream).await.unwrap(),
        DataCollectHandoffV2::Session(_)
    ));
    write_handoff_v2(
        &mut stream,
        &DataCollectHandoffV2::Received {
            transport_sequence: 0,
        },
    )
    .await
    .unwrap();
    let first = read_handoff_v2(&mut stream).await.unwrap();
    assert!(matches!(
        first,
        DataCollectHandoffV2::Message {
            transport_sequence: 1,
            ..
        }
    ));
    drop(first);
    anchor_started_rx.await.unwrap();
    depth_requested_rx.await.unwrap();
    fail_depth_tx.send(()).unwrap();
    depth_server.await.unwrap();
    // 当前业务未确认时触发真实来源错误；在途 HTTP 仍未完成。
    sleep(Duration::from_millis(100)).await;
    anchor_release_tx.send(()).unwrap();
    write_handoff_v2(
        &mut stream,
        &DataCollectHandoffV2::Received {
            transport_sequence: 1,
        },
    )
    .await
    .unwrap();
    let mut last = 1;
    let mut anchor_seen = false;
    let reason = timeout(Duration::from_secs(10), async {
        loop {
            match read_handoff_v2(&mut stream).await.unwrap() {
                DataCollectHandoffV2::Message {
                    transport_sequence,
                    row,
                } => {
                    assert_eq!(transport_sequence, last + 1);
                    if let TypedRawRowV2::PolymarketMarketMetadataV6(row) = &row
                        && let PolymarketMarketMetadataV6Input::AnchorRule(row) = row.as_ref()
                        && row.price_to_beat_decimal.as_deref()
                            == Some("6.49500142185114400000001e4")
                    {
                        anchor_seen = true;
                        assert_eq!(row.timestamp_received, row.http_received_at_utc.unwrap());
                    }
                    drop(row);
                    write_handoff_v2(
                        &mut stream,
                        &DataCollectHandoffV2::Received { transport_sequence },
                    )
                    .await
                    .unwrap();
                    last = transport_sequence;
                }
                DataCollectHandoffV2::Aborted {
                    last_confirmed_sequence,
                    reason,
                } => {
                    assert_eq!(last_confirmed_sequence, last);
                    break reason;
                }
                other => panic!("source failure must drain and then Aborted: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert!(
        anchor_seen,
        "accepted HTTP anchor cannot be dropped because another source failed"
    );
    assert!(reason.contains("binance_depth_update"));
    let error = runtime.await.unwrap().unwrap_err().to_string();
    assert!(error.contains("hot_handoff_aborted"));
    assert!(error.contains(&format!("last_confirmed_sequence={last}")));
    gamma_server.await.unwrap();
    shutdown_controlled_hot_fixture(fixture).await;
}
