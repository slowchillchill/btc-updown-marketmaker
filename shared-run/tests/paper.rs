#[path = "common/fixture_input.rs"]
mod fixture_input;
#[path = "common/fixture_policy.rs"]
mod fixture_policy;
#[path = "common/fixture_recording.rs"]
mod fixture_recording;
#[path = "common/fixture_reference.rs"]
mod fixture_reference;
#[path = "common/fixture_reference_inputs.rs"]
mod fixture_reference_inputs;
mod common {
    pub use super::fixture_input::*;
    pub use super::fixture_policy::*;
    pub use super::fixture_recording::*;
    pub use super::fixture_reference_inputs::*;
}

use common::*;
use contracts::*;
use data_collect::handoff::{read_handoff_v2, write_handoff_v2};
use data_pipeline_core::{public_input::*, typed_raw::*};
use data_pipeline_training::{public_online::*, public_recording::*};
use shared_run::paper::*;
use sim_system::{execution::RequestContextV2, latency_profile::*, platform_rules::*};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncWriteExt,
    net::{UnixListener, UnixStream},
};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
fn context(id: &str) -> RequestContextV2 {
    RequestContextV2 {
        global_request_id: id.into(),
        caller_correlation_id: None,
    }
}
fn setup(root: &Path) -> (PaperConfigV1, Arc<SealedProcessRuleSnapshotV3>) {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json");
    let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: fixture,
        expected_file_sha256: Sha256Hex(
            "d63c0e7b09edc4b2c4c3f0295f806b25f6f5fd03663cdf60ff062117e53e4e54".into(),
        ),
        replay_causal_start_utc_ms: i64::MAX,
    })
    .unwrap();
    let finality = root.join("finality.json");
    std::fs::write(&finality, SUCCESS_POLICY).unwrap();
    // 原采样器选定可短时覆盖的测试 seed；不替换生产时钟/延迟 profile。
    let latency = load_embedded_proxy_latency_profile().unwrap();
    // 固定新版完整32字节种子；保留原出站200..300ms及短响应的真实窗口。
    let seed = "28158a348a1d6c7c487c652331cc7457d85a359488a13aaf18e487127a33e45f".to_owned();
    let mut sampler = sim_system::latency_profile::SessionDelaySamplerV2::from_hex(&seed).unwrap();
    assert_eq!(
        sampler.sample(&latency, DelaySampleDomainV1::RequestOutbound),
        250
    );
    assert_eq!(
        sampler.sample(&latency, DelaySampleDomainV1::DirectResponseInbound),
        10
    );
    assert_eq!(
        sampler.sample(&latency, DelaySampleDomainV1::RequestOutbound),
        10
    );
    assert_eq!(
        sampler.sample(&latency, DelaySampleDomainV1::DirectResponseInbound),
        11
    );
    let config = PaperConfigV1 {
        socket_path: root.join("hot.sock"),
        dpt_root: root.join("dpt"),
        sim_log: root.join("sim.jsonl"),
        noe_root: root.join("noe"),
        ledger_root: root.join("ledger"),
        public_online: PublicOnlineConfigV1 {
            header: core().header().clone(),
            recording: config(),
            recording_in_flight_inputs: 1.try_into().unwrap(),
            stabilize_every_inputs: 1.try_into().unwrap(),
            read_page_rows: 1.try_into().unwrap(),
            max_read_batch_bytes: (1 << 20).try_into().unwrap(),
        },
        reference_market: ReferenceMarketSpecV1 {
            condition_id: ConditionId(CONDITION_ID.into()),
            clob_market_id: ClobMarketId("3195576".into()),
            slug: "btc-updown-5m-20".into(),
            outcomes: vec![
                ReferenceOutcomeSpecV1 {
                    token_id: TokenId(UP_TOKEN_ID.into()),
                    outcome: "UP".into(),
                },
                ReferenceOutcomeSpecV1 {
                    token_id: TokenId(DOWN_TOKEN_ID.into()),
                    outcome: "DOWN".into(),
                },
            ],
        },
        rule_snapshot_path: root.join("rules.json"),
        implementation_identity: ImplementationIdentityV1 {
            git_commit: "a".repeat(40),
            source_identity_sha256: Sha256Hex("b".repeat(64)),
            cargo_lock_sha256: Sha256Hex("c".repeat(64)),
        },
        finality_policy_path: finality,
        session: PaperSessionV1 {
            run_id: "paper-test".into(),
            session_id: "paper-session".into(),
            account_id: "paper-account".into(),
            caller_instance_id: "paper-host".into(),
            creation_request_id: "paper-create".into(),
            response_bridge_binding_id: "paper-bridge".into(),
            initial_account_state: InitialAccountStateV2 {
                available_pusd: ExactDecimal("100".into()),
            },
            session_seed: Sha256Hex(seed),
        },
        activation_id: "paper-active".into(),
        consumption_sequence_ref: "paper-inputs".into(),
        noe_implementation_identity: "paper-test".into(),
        admission_policy: ledger::AccountAdmissionPolicyV1 {
            per_market_all_in_commitment_limit_pusd: ExactDecimal("100".into()),
        },
        max_group_bytes: 2 << 20,
        finite_in_flight_groups: 1,
        script: PaperScriptConfigV1 {
            condition_id: ConditionId(CONDITION_ID.into()),
            token_id: TokenId(UP_TOKEN_ID.into()),
            limit_price_pusd_per_share: rust_decimal::Decimal::new(2, 1),
            size_shares: rust_decimal::Decimal::from(5),
            post_only: true,
            submit_context: context("paper-buy"),
            cancel_context: context("paper-cancel"),
        },
    };
    (config, rules)
}
async fn accept(listener: &UnixListener) -> UnixStream {
    let (mut stream, _) = listener.accept().await.unwrap();
    write_handoff_v2(
        &mut stream,
        &DataCollectHandoffV2::Session(DataCollectHandoffSessionV2 {
            frame_schema_version: DATA_COLLECT_HANDOFF_SCHEMA_V2.into(),
            environment: "paper".into(),
            run_id: "peer".into(),
            git_commit: "test".into(),
            binary_sha256: "test".into(),
            config_digest: "test".into(),
            collector_host_id: "test".into(),
            collector_region: "test".into(),
        }),
    )
    .await
    .unwrap();
    confirmed(&mut stream, 0).await;
    stream
}
async fn confirmed(stream: &mut UnixStream, sequence: u64) {
    assert_eq!(
        read_handoff_v2(stream).await.unwrap(),
        DataCollectHandoffV2::Received {
            transport_sequence: sequence
        }
    );
}
async fn send(stream: &mut UnixStream, sequence: u64, row: TypedRawRowV2) {
    write_handoff_v2(
        stream,
        &DataCollectHandoffV2::Message {
            transport_sequence: sequence,
            row,
        },
    )
    .await
    .unwrap();
    confirmed(stream, sequence).await;
}
fn raw(input: RecordedIngressV1) -> TypedRawRowV2 {
    let DptIngressV1::Message { row, .. } = input.ingress else {
        panic!()
    };
    row
}
fn metadata() -> TypedRawRowV2 {
    let mut row = raw(meta(0));
    let TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(m)) = &mut row else {
        panic!()
    };
    let at = now();
    let start = (at / 1000 - 60) * 1000;
    m.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    m.market_runtime_role = MetadataMarketRuntimeRole::Current;
    m.timestamp_received = at;
    m.collector_admission_time_utc_ms = at + 1;
    m.slug = format!("btc-updown-5m-{}", start / 1000);
    m.market_start_utc = start;
    m.market_end_utc = start + 300_000;
    m.fixed_identity_verified_at_utc = at - 1;
    m.source_evidence_received_at_utc = Some(at);
    m.row_evidence_validation_completed_at_utc = Some(at + 1);
    m.metadata_fetch_start_utc = Some(at - 10);
    row
}
fn book_row(seq: u64) -> TypedRawRowV2 {
    let at = now();
    let mut row = raw(pm(
        0,
        seq,
        at,
        PolymarketOrderbookEvent::Book {
            asset_id: UP_TOKEN_ID.into(),
            bids: vec![PolymarketPriceLevel {
                price: "0.5".into(),
                size: "100".into(),
            }],
            asks: vec![PolymarketPriceLevel {
                price: "0.6".into(),
                size: "80".into(),
            }],
            last_trade_price: Some("0.55".into()),
        },
    ));
    let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)) = &mut row else {
        panic!()
    };
    r.timestamp_received = at;
    r.collector_admission_time_utc_ms = at + 1;
    r.slug = None;
    row
}
fn inputs(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|line| line["kind"] == "ACCEPTED_INPUTS")
        .flat_map(|line| line["body"].as_array().unwrap().clone())
        .collect()
}
fn requests(path: &Path) -> Vec<serde_json::Value> {
    inputs(path)
        .into_iter()
        .filter(|v| v["input"] == "PUBLIC_REQUEST")
        .map(|v| v["body"]["request"].clone())
        .collect()
}
#[tokio::test(flavor = "current_thread")]
async fn production_host_real_order_cancel_and_same_future_half_frame() {
    let tmp = tempfile::tempdir().unwrap();
    let (config, rules) = setup(tmp.path());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let peer = async {
        let mut stream = accept(&listener).await;
        send(
            &mut stream,
            1,
            raw(reference_then_market_inputs().remove(0)),
        )
        .await;
        // 开户先于任何市场，参考输入本身不产生请求。
        assert!(requests(&config.sim_log).is_empty());
        send(&mut stream, 2, metadata()).await;
        assert!(
            requests(&config.sim_log).is_empty(),
            "首可用组 NoTrade 零请求"
        );
        send(&mut stream, 3, book_row(1)).await;
        let mut down = book_row(2);
        if let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)) = &mut down {
            if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut r.event {
                *asset_id = DOWN_TOKEN_ID.into();
            }
        }
        send(&mut stream, 4, down).await;
        let frame = DataCollectHandoffV2::Message {
            transport_sequence: 5,
            row: book_row(3),
        };
        let bytes = serde_json::to_vec(&frame).unwrap();
        let half = bytes.len() / 2;
        stream.write_u64(bytes.len() as u64).await.unwrap();
        stream.write_all(&bytes[..half]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(5), read_handoff_v2(&mut stream))
                .await
                .is_err(),
            "半帧不能确认"
        );
        stream.write_all(&bytes[half..]).await.unwrap();
        confirmed(&mut stream, 5).await;
        for seq in 6..=10 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            send(&mut stream, seq, book_row(seq - 2)).await;
        }
        write_handoff_v2(
            &mut stream,
            &DataCollectHandoffV2::End {
                last_confirmed_sequence: 10,
            },
        )
        .await
        .unwrap();
    };
    let (run, ()) = tokio::join!(run_paper_v1(&config, rules, std::future::pending()), peer);
    let report = run.unwrap();
    assert_eq!(
        report.script,
        PaperScriptStepV1::CancellationFeedback(
            contracts::execution_normalization::SourceNeutralCancellationOutcomeV1::Canceled
        )
    );
    assert!(report.sim_noe_ledger_recording_drained);
    assert_eq!(report.collector_exit, None);
    let PublicOnlineOutcomeV1::End(end) = report.dpt else {
        panic!()
    };
    assert_eq!(end.last_received_transport_sequence, 10);
    assert_eq!(end.measurements.applied_inputs, 11);
    assert_eq!(end.committed_prefix.next_input_position, 11);
    let ledger = report.ledger_consumed.unwrap();
    assert!(ledger.platform_source_prefix > 1);
    assert_eq!(ledger.platform_confirmed_projection.cash_pusd.0, "100");
    assert_eq!(ledger.platform_confirmed_projection.open_order_count, 0);
    let req = requests(&config.sim_log);
    assert_eq!(req.len(), 2, "原真实 Bridge 请求各一次");
    assert_eq!(
        req[0]["request"]["context"]["global_request_id"],
        "paper-buy"
    );
    assert_eq!(
        req[1]["request"]["context"]["global_request_id"],
        "paper-cancel"
    );
    let accepted = inputs(&config.sim_log);
    for kind in ["CREATE_SESSION", "ACTIVATE_SESSION", "ENROLL_MARKET"] {
        assert_eq!(
            accepted.iter().filter(|v| v["input"] == kind).count(),
            1,
            "开户及首次登记恰一次"
        );
    }
    let group_at = |position| {
        accepted
            .iter()
            .find(|v| {
                v["input"] == "PUBLIC_GROUP" && v["body"]["reference"]["group_position"] == position
            })
            .unwrap()["body"]["execution_time"]["logical_time_utc_ms"]
            .as_i64()
            .unwrap()
    };
    let before_partial = group_at(3);
    let after_partial = group_at(4);
    assert!(
        accepted
            .iter()
            .any(|v| v["input"] == "PUBLIC_CLOSE_TIME" && {
                let logical = v["body"]["execution_time"]["logical_time_utc_ms"]
                    .as_i64()
                    .unwrap();
                let actual = v["body"]["closure"]["ExecutionClockAdvancedTo"]
                    .as_i64()
                    .unwrap();
                logical > before_partial && actual < after_partial
            }),
        "半帧期间真实 timer 已推进较后义务；不是收到完整帧后才补推进"
    );
    let text = std::fs::read_to_string(&config.sim_log).unwrap();
    assert!(text.contains("ExecutionClockAdvancedTo"));
    assert!(!text.contains("RunSeal"));
    let mut reader = PublicRecordingReaderV1::open_committed(
        &config.dpt_root,
        Some(end.committed_prefix),
        1,
        1 << 20,
    )
    .unwrap();
    let mut positions = vec![];
    while let Some(row) = reader.next_row().unwrap() {
        if let PublicStoredRowV1::Input(input) = row {
            positions.push(input.input_position)
        }
    }
    assert_eq!(positions, (0..11).collect::<Vec<_>>());
}
#[tokio::test(flavor = "current_thread")]
async fn normal_end_missing_order_is_drained_but_not_covered() {
    let tmp = tempfile::tempdir().unwrap();
    let (config, rules) = setup(tmp.path());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let peer = async {
        let mut stream = accept(&listener).await;
        send(&mut stream, 1, metadata()).await;
        write_handoff_v2(
            &mut stream,
            &DataCollectHandoffV2::End {
                last_confirmed_sequence: 1,
            },
        )
        .await
        .unwrap();
    };
    let (run, ()) = tokio::join!(run_paper_v1(&config, rules, std::future::pending()), peer);
    let report = run.unwrap();
    assert!(matches!(report.dpt, PublicOnlineOutcomeV1::End(_)));
    assert_eq!(report.script, PaperScriptStepV1::AwaitingTargetGroup);
    assert!(requests(&config.sim_log).is_empty());
    assert!(
        !std::fs::read_to_string(&config.sim_log)
            .unwrap()
            .contains("RunSeal")
    );
}
#[tokio::test(flavor = "current_thread")]
async fn planned_stop_aborted_and_disconnect_keep_distinct_outcomes() {
    for mode in ["stop", "aborted", "disconnect"] {
        let tmp = tempfile::tempdir().unwrap();
        let (config, rules) = setup(tmp.path());
        let listener = UnixListener::bind(&config.socket_path).unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let peer = async {
            let mut stream = accept(&listener).await;
            send(&mut stream, 1, metadata()).await;
            match mode {
                "stop" => {
                    stream.write_u64(100).await.unwrap();
                    stream.write_all(b"{").await.unwrap();
                    tx.send(()).unwrap();
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                "aborted" => {
                    write_handoff_v2(
                        &mut stream,
                        &DataCollectHandoffV2::Aborted {
                            last_confirmed_sequence: 1,
                            reason: "source failed".into(),
                        },
                    )
                    .await
                    .unwrap();
                }
                _ => {}
            }
        };
        let stop = async {
            if mode == "stop" {
                rx.await.unwrap()
            } else {
                std::future::pending().await
            }
        };
        let (run, ()) = tokio::join!(run_paper_v1(&config, rules, stop), peer);
        if mode == "stop" {
            let report = run.unwrap();
            let PublicOnlineOutcomeV1::Stopped(stopped) = report.dpt else {
                panic!()
            };
            assert_eq!(stopped.measurements.applied_inputs, 1);
            assert_eq!(stopped.last_received_transport_sequence, 1);
            assert_eq!(
                stopped
                    .measurements
                    .stable_prefix
                    .unwrap()
                    .next_input_position,
                1
            );
        } else {
            let error = run.unwrap_err();
            let failure = error.downcast_ref::<PaperFailureV1>().unwrap();
            assert_eq!(failure.measurements.applied_inputs, 1);
            assert_eq!(failure.last_received_transport_sequence, 1);
            assert!(failure.dpt.is_none());
            if mode == "aborted" {
                assert!(failure.reason.contains("source failed"));
            }
        }
        assert!(
            !std::fs::read_to_string(&config.sim_log)
                .unwrap()
                .contains("RunSeal")
        );
    }
}
#[test]
fn binary_requires_explicit_configuration_without_querying_network() {
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_paper"))
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("paper <"));
}

#[tokio::test(flavor = "current_thread")]
async fn one_sided_market_keeps_original_unknown_and_never_invents_cancel() {
    let tmp = tempfile::tempdir().unwrap();
    let (config, rules) = setup(tmp.path());
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let peer = async {
        let mut stream = accept(&listener).await;
        send(&mut stream, 1, metadata()).await;
        send(&mut stream, 2, book_row(1)).await;
        tokio::time::sleep(Duration::from_millis(350)).await;
        send(&mut stream, 3, book_row(2)).await;
        send(&mut stream, 4, book_row(3)).await;
        write_handoff_v2(
            &mut stream,
            &DataCollectHandoffV2::End {
                last_confirmed_sequence: 4,
            },
        )
        .await
        .unwrap();
    };
    let (run, ()) = tokio::join!(run_paper_v1(&config, rules, std::future::pending()), peer);
    let report = run.unwrap();
    assert!(report.sim_noe_ledger_recording_drained);
    assert_eq!(report.script, PaperScriptStepV1::AwaitingCancellableOrder);
    assert_eq!(requests(&config.sim_log).len(), 1);
    let mut unknown = false;
    for context in std::fs::read_dir(&config.noe_root).unwrap() {
        let metadata: serde_json::Value = serde_json::from_slice(
            &std::fs::read(context.unwrap().path().join("context.json")).unwrap(),
        )
        .unwrap();
        let reference =
            serde_json::from_value(metadata["payload"]["context"]["context_reference"].clone())
                .unwrap();
        let store =
            ::execution_normalization::DurableContextStoreV3::open(&config.noe_root, &reference)
                .unwrap();
        for commit in store.commits() {
            for record in &commit.records {
                let record = serde_json::to_value(record).unwrap();
                if record["Evidence"]["evidence"]["Control"]["result"]["Submission"]["outcome"]
                    == "EvidenceUnknownNoEffect"
                {
                    unknown = true;
                }
            }
        }
    }

    assert!(unknown, "双侧基线未齐时保留原未知反馈，不重试/跳步");
}

#[tokio::test(flavor = "current_thread")]
async fn original_background_io_failure_is_not_reported_as_drained() {
    let tmp = tempfile::tempdir().unwrap();
    let (config, rules) = setup(tmp.path());
    // 原 Ledger 后台首次打开目录时遇到真实文件/目录错误。
    std::fs::write(&config.ledger_root, b"not a directory").unwrap();
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let peer = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        write_handoff_v2(
            &mut stream,
            &DataCollectHandoffV2::Session(DataCollectHandoffSessionV2 {
                frame_schema_version: DATA_COLLECT_HANDOFF_SCHEMA_V2.into(),
                environment: "paper".into(),
                run_id: "peer".into(),
                git_commit: "test".into(),
                binary_sha256: "test".into(),
                config_digest: "test".into(),
                collector_host_id: "test".into(),
                collector_region: "test".into(),
            }),
        )
        .await
        .unwrap();
        let _ = read_handoff_v2(&mut stream).await;
        let _ = write_handoff_v2(
            &mut stream,
            &DataCollectHandoffV2::End {
                last_confirmed_sequence: 0,
            },
        )
        .await;
    };
    let run = run_paper_v1(&config, rules, std::future::pending());
    tokio::pin!(run);
    let result = tokio::select! { result = &mut run => result, () = peer => run.await };
    let error = result.unwrap_err();
    assert!(
        error.to_string().contains("directory") || error.to_string().contains("exists"),
        "{error}"
    );
    assert!(
        !std::fs::read_to_string(&config.sim_log)
            .unwrap()
            .contains("RunSeal")
    );
}
