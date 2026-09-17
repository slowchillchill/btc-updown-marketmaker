use super::*;
use crate::{public_recording::tests as fixture, public_replay::PublicReplayV1};
use data_collect::handoff::{read_handoff_v2, write_handoff_v2};
use data_pipeline_core::typed_raw::*;
use std::{
    sync::{
        Condvar,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::net::UnixStream;

fn config(interval: u64, capacity: usize) -> PublicOnlineConfigV1 {
    PublicOnlineConfigV1 {
        header: fixture::core().header().clone(),
        recording: fixture::config(),
        recording_in_flight_inputs: capacity.try_into().unwrap(),
        stabilize_every_inputs: interval.try_into().unwrap(),
        read_page_rows: 1.try_into().unwrap(),
        max_read_batch_bytes: (1 << 20).try_into().unwrap(),
    }
}
fn session(role: &str) -> DataCollectHandoffV2 {
    DataCollectHandoffV2::Session(DataCollectHandoffSessionV2 {
        frame_schema_version: DATA_COLLECT_HANDOFF_SCHEMA_V2.into(),
        environment: role.into(),
        run_id: "real-socket-test".into(),
        git_commit: "test".into(),
        binary_sha256: "test".into(),
        config_digest: "test".into(),
        collector_host_id: "host".into(),
        collector_region: "hk".into(),
    })
}
async fn start(server: &mut UnixStream, role: &str) {
    write_handoff_v2(server, &session(role)).await.unwrap();
    assert_eq!(
        read_handoff_v2(server).await.unwrap(),
        DataCollectHandoffV2::Received {
            transport_sequence: 0
        }
    );
}
async fn message(server: &mut UnixStream, seq: u64, input: RecordedIngressV1) {
    let DptIngressV1::Message { row, .. } = input.ingress else {
        panic!()
    };
    write_handoff_v2(
        server,
        &DataCollectHandoffV2::Message {
            transport_sequence: seq,
            row,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        read_handoff_v2(server).await.unwrap(),
        DataCollectHandoffV2::Received {
            transport_sequence: seq
        }
    );
}
async fn send_rows(mut server: UnixStream, rows: Vec<RecordedIngressV1>, role: &str) {
    start(&mut server, role).await;
    let n = rows.len() as u64;
    for (i, row) in rows.into_iter().enumerate() {
        message(&mut server, i as u64 + 1, row).await;
    }
    write_handoff_v2(
        &mut server,
        &DataCollectHandoffV2::End {
            last_confirmed_sequence: n,
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn socket_roles_preserve_complete_groups_and_reference_tail_stable_pages() {
    // 同一原序：不适用30秒、适用a、后到b、身份、规则。覆盖跨页与真实接收回退。
    let rows = vec![
        fixture::twap_record(0, 30, 21001, "64000000000000000000000"),
        fixture::twap_record(1, 60, 21002, "65000000000000000000000"),
        fixture::twap_record(2, 60, 21003, "66000000000000000000000"),
        fixture::meta(3),
        fixture::rule_record(4, 60, None),
        fixture::pm(5, 1, 22000, fixture::book("100")),
        fixture::pm(
            6,
            2,
            22001,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![fixture::change(Some("80")), fixture::change(Some("100"))],
            },
        ),
        fixture::pm(7, 3, 22002, fixture::book("100")),
    ];
    let mut baseline = None;
    for (role, interval) in [("paper", 100), ("live", 1), ("paper", 2)] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("record");
        let (client, server) = UnixStream::pair().unwrap();
        let mut observed = Vec::new();
        let (result, ()) = tokio::join!(
            consume_public_online_v1(client, &root, config(interval, 1), |a| {
                observed.push(a.clone());
                Ok(())
            }),
            send_rows(server, rows.clone(), role)
        );
        let result = result.unwrap();
        assert!(result.committed_prefix.normal_end);
        assert_eq!(result.measurements.applied_inputs, 9);
        assert_eq!(result.last_received_transport_sequence, 8);
        let group = &observed[4].closed_groups[0];
        let reference = group.observations[0].temporary_reference.as_ref().unwrap();
        assert_eq!(reference.selected_position.closed_at_input_position, 1);
        assert!(
            observed[3].closed_groups[0].observations[0]
                .temporary_reference
                .is_none()
        );
        assert_eq!(observed[6].closed_groups.len(), 1);
        assert_eq!(observed[6].closed_groups[0].member_input_positions, vec![6]);
        assert_eq!(observed[6].closed_groups[0].records.len(), 1);
        assert_eq!(observed[7].closed_groups.len(), 1); // 同值异时仍是完整组。
        let mut replay = PublicReplayV1::open(&root, 1, 1 << 20).unwrap();
        let mut replayed = Vec::new();
        while let Some((input, applied)) = replay.next_applied().unwrap() {
            if input.input_position == 4 {
                assert_eq!(input.dependency_record_refs[0].input_position, 1);
            }
            replayed.push(applied.applied);
        }
        assert_eq!(observed, replayed);
        if let Some(before) = &baseline {
            assert_eq!(&observed, before)
        } else {
            baseline = Some(observed)
        }
    }
}

#[tokio::test]
async fn current_twap_after_rule_is_available_without_waiting_for_recording() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("record");
    let (client, server) = UnixStream::pair().unwrap();
    let rows = vec![
        fixture::meta(0),
        fixture::rule_record(1, 60, None),
        fixture::twap_record(2, 60, 23000, "65000000000000000000000"),
    ];
    let mut selected = None;
    let (out, ()) = tokio::join!(
        consume_public_online_v1(client, &root, config(100, 1), |a| {
            if a.applied_prefix.next_input_position == 3 {
                selected = a.closed_groups[0].observations[0]
                    .temporary_reference
                    .clone()
            }
            Ok(())
        }),
        send_rows(server, rows, "paper")
    );
    out.unwrap();
    assert_eq!(
        selected.unwrap().selected_position.closed_at_input_position,
        2
    );
}

fn block_writer() -> (Arc<(Mutex<bool>, Condvar)>, Arc<AtomicBool>) {
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let entered = Arc::new(AtomicBool::new(false));
    let (g, e) = (gate.clone(), entered.clone());
    test_control::set(Arc::new(move || {
        e.store(true, Ordering::SeqCst);
        let mut open = g.0.lock().unwrap();
        while !*open {
            open = g.1.wait(open).unwrap();
        }
    }));
    (gate, entered)
}
fn release(gate: &(Mutex<bool>, Condvar)) {
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
}
#[tokio::test]
async fn blocked_writer_consumes_before_stability_and_resumes_without_duplicates() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("record");
    let (client, server) = UnixStream::pair().unwrap();
    let (gate, entered) = block_writer();
    let sender = tokio::spawn(send_rows(
        server,
        vec![
            fixture::meta(0),
            fixture::pm(1, 1, 22000, fixture::book("100")),
        ],
        "paper",
    ));
    let count = std::cell::Cell::new(0);
    let mut run = Box::pin(consume_public_online_v1(
        client,
        &root,
        config(1, 1),
        |_| {
            count.set(count.get() + 1);
            Ok(())
        },
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut run)
            .await
            .is_err()
    );
    assert!(entered.load(Ordering::SeqCst));
    assert_eq!(count.get(), 1);
    assert!(
        PublicRecordingReaderV1::recover(&root, 1, 1 << 20)
            .unwrap()
            .prefix
            .is_none()
    );
    release(&gate);
    let completion = run.await.unwrap();
    sender.await.unwrap();
    assert_eq!(completion.measurements.applied_inputs, 3);
    assert_eq!(count.get(), 3);
    assert_eq!(completion.measurements.peak_recording_in_flight_inputs, 1);
    let mut replay = PublicReplayV1::open(&root, 1, 1 << 20).unwrap();
    let mut n = 0;
    while replay.next_applied().unwrap().is_some() {
        n += 1;
    }
    assert_eq!(n, 3);
}

#[tokio::test]
async fn real_background_io_failure_wakes_silent_peer_and_preserves_applied_count() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("record");
    let (client, mut server) = UnixStream::pair().unwrap();
    let (gate, entered) = block_writer();
    let (sent, received) = oneshot::channel();
    let sender = tokio::spawn(async move {
        start(&mut server, "paper").await;
        message(&mut server, 1, fixture::meta(0)).await;
        sent.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    let mut run = Box::pin(consume_public_online_v1(
        client,
        &root,
        config(100, 2),
        |_| Ok(()),
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut run)
            .await
            .is_err()
    );
    received.await.unwrap();
    assert!(entered.load(Ordering::SeqCst));
    // 实际目标路径成为普通文件，唯一写入器创建pending目录返回真实I/O错误。
    std::fs::write(
        root.join("pending-00000000000000000000"),
        b"not a directory",
    )
    .unwrap();
    release(&gate);
    let error = tokio::time::timeout(Duration::from_secs(2), run)
        .await
        .unwrap()
        .unwrap_err();
    sender.abort();
    let error = error.downcast_ref::<PublicOnlineFailureV1>().unwrap();
    assert_eq!(error.measurements.applied_inputs, 1);
    assert!(error.measurements.stable_prefix.is_none());
    assert!(error.reason.contains("公共记录后台失败"));
    assert!(
        PublicRecordingReaderV1::recover(&root, 1, 1 << 20)
            .unwrap()
            .prefix
            .is_none()
    );
}

#[tokio::test]
async fn truncated_malformed_aborted_and_cancelled_are_not_normal_end() {
    use tokio::io::AsyncWriteExt;
    for mode in ["partial", "malformed", "aborted", "cancel"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("record");
        let (client, mut server) = UnixStream::pair().unwrap();
        let sender = tokio::spawn(async move {
            start(&mut server, "live").await;
            match mode {
                "partial" => {
                    server.write_u64(u64::MAX).await.unwrap();
                    server.write_all(b"{").await.unwrap();
                }
                "malformed" => {
                    server.write_u64(1).await.unwrap();
                    server.write_all(b"{").await.unwrap();
                }
                "aborted" => {
                    write_handoff_v2(
                        &mut server,
                        &DataCollectHandoffV2::Aborted {
                            last_confirmed_sequence: 0,
                            reason: "source failure".into(),
                        },
                    )
                    .await
                    .unwrap();
                }
                _ => {
                    std::future::pending::<()>().await;
                }
            }
        });
        let mut run = Box::pin(consume_public_online_v1(
            client,
            &root,
            config(100, 1),
            |_| Ok(()),
        ));
        if mode == "cancel" {
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut run)
                    .await
                    .is_err()
            );
            drop(run);
            sender.abort();
        } else {
            let error = run.await.unwrap_err();
            assert_eq!(
                error
                    .downcast_ref::<PublicOnlineFailureV1>()
                    .unwrap()
                    .measurements
                    .applied_inputs,
                0
            );
            sender.await.unwrap();
        }
        assert!(
            PublicRecordingReaderV1::recover(&root, 1, 1 << 20)
                .unwrap()
                .prefix
                .is_none()
        );
    }
}

#[tokio::test]
async fn seven_source_objects_and_new_pm_epoch_keep_existing_business_semantics() {
    let mut spot = fixture::twap_input(60).common;
    spot.schema_version = POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into();
    spot.topic = "crypto_prices_chainlink".into();
    let trade=BinanceAggTradeRow { timestamp_received:22001, rest_backfill_timestamp_received:None, ingest_source:"websocket".into(), rest_was_best_price_match:None, source_native_payload_json:r#"{"e":"aggTrade","E":22000,"s":"BTCUSDT","a":1,"p":"64000","q":"1","f":1,"l":1,"T":22000,"m":false,"M":true}"#.into(), source_sequence_epoch_id:"agg-epoch".into(),connection_generation:1,websocket_ingress_sequence:1,rest_response_ingress_sequence:None,rest_response_item_index:None,collector_admission_sequence:1,collector_admission_time_utc_ms:22002,collector_host_id:"host".into(),collector_region:"hk".into(),source_endpoint:"wss://stream.binance.com:9443/ws/btcusdt@aggTrade".into(),schema_version:BINANCE_AGGTRADE_SCHEMA.into(),record:BinanceAggTradeRecord::Trade{event_type:"aggTrade".into(),event_time:22000,trade_time:22000,symbol:"BTCUSDT".into(),aggregate_trade_id:1,price:"64000".into(),quantity:"1".into(),first_trade_id:1,last_trade_id:1,buyer_is_maker:false,stream:"btcusdt@aggTrade".into(),ws_ignore:true} };
    let mut new_epoch = fixture::pm(7, 1, 23000, fixture::book("50"));
    if let DptIngressV1::Message {
        row: TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)),
        source_evidence,
    } = &mut new_epoch.ingress
    {
        row.source_sequence_epoch_id = "epoch-b".into();
        *source_evidence = source_evidence_v3_from_typed_raw_v2(&TypedRawRowV2::Existing(
            TypedRawRow::PolymarketOrderbook(row.clone()),
        ))
        .unwrap();
    }
    let rows = vec![
        fixture::meta(0),
        fixture::record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(fixture::snapshot(
                "depth", 10, 1,
            ))),
        ),
        fixture::record(
            2,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(fixture::update(
                "depth",
                11,
                11,
                2,
                Some(10),
                None,
                BinanceDepthStitchVerdict::Applied,
                vec![DepthLevel {
                    price: "64000".into(),
                    quantity: "0.6".into(),
                }],
            ))),
        ),
        fixture::record(
            3,
            TypedRawRowV2::Existing(TypedRawRow::BinanceAggTrade(trade)),
        ),
        fixture::record(
            4,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(spot)),
        ),
        fixture::twap_record(5, 60, 22002, "65000000000000000000000"),
        fixture::pm(6, 1, 22000, fixture::book("100")),
        new_epoch,
    ];
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("record");
    let (client, server) = UnixStream::pair().unwrap();
    let mut observed = Vec::new();
    let sender = tokio::spawn(send_rows(server, rows, "live"));
    let result = consume_public_online_v1(client, &root, config(3, 2), |a| {
        observed.push(a.clone());
        Ok(())
    })
    .await;
    assert!(result.is_ok(), "{:?}", result);
    sender.await.unwrap();
    let mut replay = PublicReplayV1::open(&root, 1, 1 << 20).unwrap();
    let mut values = Vec::new();
    while let Some((_, a)) = replay.next_applied().unwrap() {
        values.push(a.applied)
    }
    assert_eq!(observed, values);
    assert_eq!(observed[0].applied_prefix.next_input_position, 1);
    assert!(
        observed[0].closed_groups[0].observations[0]
            .chainlink_spot
            .value
            .is_none()
    );
    assert!(
        observed[4].closed_groups[0].observations[0]
            .chainlink_spot
            .value
            .is_some()
    );
}

#[tokio::test]
async fn unpaired_terminal_end_preserves_original_core_error_and_no_normal_prefix() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("record");
    let (client, server) = UnixStream::pair().unwrap();
    let terminal = fixture::pm(
        1,
        1,
        30000,
        PolymarketOrderbookEvent::MarketResolved {
            clob_market_id: "3195576".into(),
            question: None,
            description: None,
            asset_ids: vec!["20".into(), "3".into()],
            outcomes: vec![],
            event_message: None,
            winning_asset_id: "20".into(),
            winning_outcome: "UP".into(),
        },
    );
    let (result, ()) = tokio::join!(
        consume_public_online_v1(client, &root, config(1, 1), |_| Ok(())),
        send_rows(server, vec![fixture::meta(0), terminal], "paper")
    );
    let error = result.unwrap_err();
    let failure = error.downcast_ref::<PublicOnlineFailureV1>().unwrap();
    assert_eq!(failure.measurements.applied_inputs, 2);
    assert!(
        failure.reason.contains("Unpaired") || failure.reason.contains("未配对"),
        "{}",
        failure.reason
    );
    assert!(
        !failure
            .measurements
            .stable_prefix
            .as_ref()
            .unwrap()
            .normal_end
    );
}

#[tokio::test]
async fn explicit_host_cancel_reports_applied_prefix_without_creating_end() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("record");
    let (client, mut server) = UnixStream::pair().unwrap();
    let (cancel, cancelled) = oneshot::channel();
    let sender = tokio::spawn(async move {
        start(&mut server, "paper").await;
        message(&mut server, 1, fixture::meta(0)).await;
        cancel.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    let error = consume_public_online_until_v1(client, &root, config(100, 1), |_| Ok(()), async {
        cancelled.await.unwrap();
        Ok(())
    })
    .await
    .unwrap_err();
    sender.abort();
    let failure = error.downcast_ref::<PublicOnlineFailureV1>().unwrap();
    assert_eq!(failure.measurements.applied_inputs, 1);
    assert!(failure.reason.contains("宿主取消"));
    assert!(failure.measurements.stable_prefix.is_none());
}
