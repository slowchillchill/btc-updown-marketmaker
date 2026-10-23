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
use sim_system::{
    execution::{BusinessProjectionVersionV1, RequestContextV2},
    latency_profile::*,
    platform_rules::*,
};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncWriteExt,
    net::{UnixListener, UnixStream},
};

#[path = "common/fixture_terminal.rs"]
mod fixture_terminal;

#[path = "common/fixture_paper.rs"]
mod fixture_paper;
use fixture_paper::*;

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
    assert!(!text.contains("\"RUN_SEAL\""));
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
            .contains("\"RUN_SEAL\"")
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
                .contains("\"RUN_SEAL\"")
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
            ::execution_normalization::DurableContextStoreV4::open(&config.noe_root, &reference)
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
            .contains("\"RUN_SEAL\"")
    );
}

fn paired_script() -> PaperScriptConfigV1 {
    PaperScriptConfigV1::PairedBuy {
        condition_id: ConditionId(CONDITION_ID.into()),
        orders: [
            PaperGtcLegV1 {
                token_id: TokenId(UP_TOKEN_ID.into()),
                limit_price_pusd_per_share: rust_decimal::Decimal::new(2, 1),
                size_shares: rust_decimal::Decimal::from(5),
                post_only: true,
            },
            PaperGtcLegV1 {
                token_id: TokenId(DOWN_TOKEN_ID.into()),
                limit_price_pusd_per_share: rust_decimal::Decimal::new(3, 1),
                size_shares: rust_decimal::Decimal::from(7),
                post_only: true,
            },
        ],
        submit_context: context("paper-pair"),
    }
}
fn down_book(seq: u64) -> TypedRawRowV2 {
    let mut row = book_row(seq);
    let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)) = &mut row else {
        unreachable!()
    };
    let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut r.event else {
        unreachable!()
    };
    *asset_id = DOWN_TOKEN_ID.into();
    row
}
fn evidence_path(tmp: &tempfile::TempDir, name: &str) -> std::path::PathBuf {
    if let Some(root) = std::env::var_os("PAPER_TEST_ARTIFACT_ROOT") {
        let root = std::path::PathBuf::from(root).join(name);
        std::fs::create_dir_all(root.parent().unwrap()).unwrap();
        std::fs::create_dir(&root).unwrap();
        root
    } else {
        tmp.path().to_owned()
    }
}
fn retained_report(config: &PaperConfigV1, report: &PaperReportV1) {
    if std::env::var_os("PAPER_TEST_ARTIFACT_ROOT").is_some() {
        let root = config.sim_log.parent().unwrap();
        std::fs::write(
            root.join("effective-config.json"),
            serde_json::to_vec_pretty(config).unwrap(),
        )
        .unwrap();
        std::fs::write(
            root.join("report.json"),
            serde_json::to_vec_pretty(report).unwrap(),
        )
        .unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn production_paired_buy_reserves_whole_exact_action_once_or_sends_strictly_zero() {
    for (cash, large_quantity) in [("100", false), ("3", false), ("100", true)] {
        let tmp = tempfile::tempdir().unwrap();
        let root = evidence_path(&tmp, &format!("paired-cash-{cash}-large-{large_quantity}"));
        let (mut config, rules) = setup(&root);
        config.script = paired_script();
        if large_quantity && let PaperScriptConfigV1::PairedBuy { orders, .. } = &mut config.script
        {
            // SDK合法大数量；中间百分之一份/微本金超过Decimal，仍须按实际现金正常拒绝。
            orders[0].size_shares = "1000000000000000000000000000".parse().unwrap();
        }
        config.session.initial_account_state.available_pusd = ExactDecimal(cash.into());
        let listener = UnixListener::bind(&config.socket_path).unwrap();
        let peer = async {
            let mut stream = accept(&listener).await;
            send(&mut stream, 1, metadata()).await;
            assert!(requests(&config.sim_log).is_empty(), "NoTrade零请求");
            send(&mut stream, 2, book_row(1)).await;
            send(&mut stream, 3, down_book(2)).await;
            tokio::time::sleep(Duration::from_millis(350)).await;
            for seq in 4..=6 {
                send(&mut stream, seq, book_row(seq)).await;
            }
            write_handoff_v2(
                &mut stream,
                &DataCollectHandoffV2::End {
                    last_confirmed_sequence: 6,
                },
            )
            .await
            .unwrap();
        };
        let (run, ()) = tokio::join!(
            run_paper_v1(&config, rules.clone(), std::future::pending()),
            peer
        );
        let report = run.unwrap();
        retained_report(&config, &report);
        assert!(report.sim_noe_ledger_recording_drained);
        let ledger = report.ledger_consumed.as_ref().unwrap();
        let local = &ledger.local_pending_reservation_projection;
        assert_eq!(
            local
                .principal_pusd
                .0
                .parse::<rust_decimal::Decimal>()
                .unwrap(),
            rust_decimal::Decimal::ZERO
        );
        assert_eq!(
            local
                .immediate_fee_pusd
                .0
                .parse::<rust_decimal::Decimal>()
                .unwrap(),
            rust_decimal::Decimal::ZERO
        );
        let req = requests(&config.sim_log);
        if cash == "100" && !large_quantity {
            assert_eq!(report.script, PaperScriptStepV1::SubmissionSent);
            assert_eq!(req.len(), 1, "一个整动作一次提交，不拆腿不重试");
            assert_eq!(req[0]["method"], "POST_ORDERS");
            assert_eq!(
                req[0]["request"]["context"]["global_request_id"],
                "paper-pair"
            );
            let items = req[0]["request"]["items"].as_array().unwrap();
            assert_eq!(items.len(), 2);
            for (ordinal, (token, price, size)) in
                [(UP_TOKEN_ID, "0.2", "5"), (DOWN_TOKEN_ID, "0.3", "7")]
                    .iter()
                    .enumerate()
            {
                assert_eq!(items[ordinal]["batch_item_ordinal"], ordinal);
                let order = &items[ordinal]["item"]["order"];
                assert_eq!(order["token_id"], *token);
                assert_eq!(
                    order["kind"]["GtcLimit"]["limit_price_pusd_per_share"],
                    *price
                );
                assert_eq!(order["kind"]["GtcLimit"]["size_shares"], *size);
                assert_eq!(
                    items[ordinal]["item"]["order_identity_mode"],
                    "PublicRequestV1"
                );
            }
            assert_eq!(ledger.platform_confirmed_projection.open_order_count, 2);
            assert_eq!(
                ledger
                    .platform_confirmed_projection
                    .principal_encumbrance_pusd
                    .0,
                "3.1"
            );
            assert_eq!(
                ledger.platform_confirmed_projection.available_pusd.0, "96.9",
                "已预留与平台占资只计一次"
            );
        } else {
            // 每腿分别1和2.1都不超过3，整动作3.1不足；不得部分发出或改值。
            assert_eq!(
                report.script,
                PaperScriptStepV1::LocalFundsRejected {
                    cash_available: cash.parse().unwrap()
                }
            );
            assert!(req.is_empty(), "严格零发送");
            assert_eq!(ledger.platform_confirmed_projection.open_order_count, 0);
            assert_eq!(ledger.platform_confirmed_projection.available_pusd.0, cash);
            assert_eq!(
                ledger
                    .platform_confirmed_projection
                    .principal_encumbrance_pusd
                    .0,
                "0"
            );
        }
        let summary = sim_system::execution::visit_sim_requests_v1(
            &config.sim_log,
            config.max_group_bytes,
            |_, _, _| Ok(()),
        )
        .unwrap();
        assert_eq!(
            summary.opened.projection_version,
            BusinessProjectionVersionV1::V4
        );
        assert_eq!(
            summary.opened.platform_rule_snapshot_file_sha256,
            *rules.file_sha256()
        );
        assert!(!summary.saw_seal);
        assert!(!summary.incomplete_tail);
        println!(
            "paired cash={cash} large={large_quantity} inputs={} groups={} report={}",
            summary.inputs,
            summary.groups,
            serde_json::to_string(&report).unwrap()
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn production_v4_zero_target_cancel_completes_request_without_inventing_target_result() {
    let tmp = tempfile::tempdir().unwrap();
    let root = evidence_path(&tmp, "zero-target-cancel");
    let (mut config, rules) = setup(&root);
    config.script = PaperScriptConfigV1::CancelMarket {
        condition_id: ConditionId(CONDITION_ID.into()),
        cancel_context: context("paper-empty-cancel"),
    };
    let listener = UnixListener::bind(&config.socket_path).unwrap();
    let peer = async {
        let mut stream = accept(&listener).await;
        send(&mut stream, 1, metadata()).await;
        send(&mut stream, 2, book_row(1)).await;
        send(&mut stream, 3, down_book(2)).await;
        // 仍使用正式经验延迟；随后真实组领取反馈，不以登记回执替代完成。
        tokio::time::sleep(Duration::from_millis(350)).await;
        send(&mut stream, 4, book_row(3)).await;
        send(&mut stream, 5, book_row(4)).await;
        write_handoff_v2(
            &mut stream,
            &DataCollectHandoffV2::End {
                last_confirmed_sequence: 5,
            },
        )
        .await
        .unwrap();
    };
    let (run, ()) = tokio::join!(run_paper_v1(&config, rules, std::future::pending()), peer);
    let report = run.unwrap();
    retained_report(&config, &report);
    assert_eq!(
        report.script,
        PaperScriptStepV1::CancellationRequestCompleted(
            contracts::execution_normalization::CancellationRequestCompletedV1 {
                canceled_count: 0,
                not_canceled_no_effect_count: 0,
                evidence_unknown_no_effect_count: 0,
            }
        )
    );
    let req = requests(&config.sim_log);
    assert_eq!(req.len(), 1);
    assert_eq!(req[0]["method"], "CANCEL_MARKET_ORDERS");
    assert_eq!(
        req[0]["request"]["context"]["global_request_id"],
        "paper-empty-cancel"
    );
    assert_eq!(
        report
            .ledger_consumed
            .as_ref()
            .unwrap()
            .platform_confirmed_projection
            .available_pusd
            .0,
        "100"
    );
    let mut request_complete = 0;
    let mut target_feedback = 0;
    for context in std::fs::read_dir(&config.noe_root).unwrap() {
        let metadata: serde_json::Value = serde_json::from_slice(
            &std::fs::read(context.unwrap().path().join("context.json")).unwrap(),
        )
        .unwrap();
        let reference =
            serde_json::from_value(metadata["payload"]["context"]["context_reference"].clone())
                .unwrap();
        let store =
            ::execution_normalization::DurableContextStoreV4::open(&config.noe_root, &reference)
                .unwrap();
        for commit in store.commits() {
            for record in &commit.records {
                if let ::execution_normalization::NormalizationRecord::Evidence {
                    evidence:
                        ::execution_normalization::SourceNeutralEvidenceKind::Control { result },
                    ..
                } = record
                {
                    match result {
                        contracts::execution_normalization::SourceNeutralControlResult::CancellationRequestCompleted { .. } => request_complete += 1,
                        contracts::execution_normalization::SourceNeutralControlResult::Cancellation { .. } => target_feedback += 1,
                        _ => {}
                    }
                }
            }
        }
    }
    assert_eq!(request_complete, 1);
    assert_eq!(target_feedback, 0);
    println!(
        "zero-target request_complete={request_complete} targets={target_feedback} report={}",
        serde_json::to_string(&report).unwrap()
    );
}

// 新宿主的受控策略只检验公共接口，不提供业务策略或生产阈值。
struct DeadlineSchedule {
    silence_ms: i64,
    scope: SourceStreamScopeV1,
}
impl shared_run::StrategyPublicHealthScheduleV1 for DeadlineSchedule {
    fn next_evaluation_at(&self, markets: &[&MarketBusinessObservationV1], at: i64) -> Option<i64> {
        markets
            .iter()
            .filter_map(|o| {
                o.source_progress
                    .get_scope(&self.scope)
                    .and_then(|p| p.last_valid_business_message.as_ref())
                    .map(|e| e.received_at_utc_ms + self.silence_ms)
            })
            .filter(|due| *due > at)
            .min()
    }
}
struct ControlledOnlineStrategy {
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    release: Option<std::sync::mpsc::Receiver<()>>,
    frozen_bid: Arc<std::sync::Mutex<Vec<Option<Option<ExactDecimal>>>>>,
    completed: bool,
    silence_ms: i64,
    scope: SourceStreamScopeV1,
}
impl shared_run::StrategyV1 for ControlledOnlineStrategy {
    type Error = std::convert::Infallible;
    type Diagnostics = ();
    fn decide(
        &mut self,
        _: &shared_run::StrategyInputV1,
        _: i64,
    ) -> Result<shared_run::StrategyOutputV1, Self::Error> {
        panic!("在线宿主必须调用必要冻结端口，不能退回完整账户复制")
    }
    fn uses_source_health(&self) -> bool {
        true
    }
    fn public_health_schedule(
        &self,
    ) -> Option<Box<dyn shared_run::StrategyPublicHealthScheduleV1>> {
        Some(Box::new(DeadlineSchedule {
            silence_ms: self.silence_ms,
            scope: self.scope.clone(),
        }))
    }
    fn next_public_health_evaluation_at(
        &self,
        markets: &[&MarketBusinessObservationV1],
        _: Option<&shared_run::StrategyCalculationV1>,
        at: i64,
    ) -> Option<i64> {
        shared_run::StrategyPublicHealthScheduleV1::next_evaluation_at(
            &DeadlineSchedule {
                silence_ms: self.silence_ms,
                scope: self.scope.clone(),
            },
            markets,
            at,
        )
    }
    fn qualify_intent_public(
        &self,
        group: &ClosedBusinessGroupV1,
        _: &shared_run::StrategyCalculationV1,
        _: usize,
        at: i64,
    ) -> Result<shared_run::IntentQualificationV1, Self::Error> {
        let received = group
            .observations
            .get_market(CONDITION_ID)
            .and_then(|o| o.source_progress.get_scope(&self.scope))
            .and_then(|p| p.last_valid_business_message.as_ref())
            .map(|e| e.received_at_utc_ms);
        Ok(
            if received.is_some_and(|received| at < received + self.silence_ms) {
                shared_run::IntentQualificationV1::Qualified
            } else {
                shared_run::IntentQualificationV1::Invalidated(
                    shared_run::IntentInvalidationReasonV1 {
                        reason_code: "controlled_source_silence".into(),
                        scopes: vec![self.scope.clone()],
                    },
                )
            },
        )
    }
}
impl shared_run::DetachedStrategyV1 for ControlledOnlineStrategy {
    fn decide_detached(
        &mut self,
        input: &shared_run::DetachedStrategyInputV1,
        _: i64,
    ) -> Result<shared_run::StrategyOutputV1, Self::Error> {
        let valid = input
            .markets
            .iter()
            .find(|f| f.observation.market.condition_id.0 == CONDITION_ID)
            .filter(|f| {
                f.observation
                    .source_progress
                    .get_scope(&self.scope)
                    .and_then(|p| p.last_valid_business_message.as_ref())
                    .is_some()
            });
        let Some(market) = valid else {
            return Ok(shared_run::StrategyOutputV1::Continue {
                intents: vec![],
                next_evaluation_at_utc_ms: None,
                diagnostics: (),
            });
        };
        assert!(market.statistics.is_some(), "原统计由实际在线应用生成");
        assert_eq!(
            input.action_before.cash_available(),
            rust_decimal::Decimal::from(100)
        );
        if self.completed {
            return Ok(shared_run::StrategyOutputV1::Exit { diagnostics: () });
        }
        // 原context按token数值排序；资产位置不代表UP/DOWN身份。
        let asset = market
            .observation
            .assets
            .iter()
            .find(|asset| asset.asset.token_id.0 == self.scope.asset_or_symbol)
            .expect("合法metadata必须包含scope原资产身份");
        let bid = asset.valuation_best_bid.value.clone();
        self.frozen_bid.lock().unwrap().push(bid.clone());
        self.entered.take().unwrap().send(()).unwrap();
        self.release.take().unwrap().recv().unwrap();
        assert_eq!(
            asset.valuation_best_bid.value, bid,
            "后台计算输入不受后继行情改变"
        );
        self.completed = true;
        Ok(shared_run::StrategyOutputV1::Continue {
            intents: vec![shared_run::ReservedIntentV1 {
                context: context("controlled-expired-cancel"),
                intent: Arc::new(ModelIntentV1::CancelMarketOrders {
                    condition_id: ConditionId(CONDITION_ID.into()),
                }),
                reserves: vec![],
            }],
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        })
    }
}
#[tokio::test(flavor = "current_thread")]
async fn strategy_host_retains_silent_expiry_through_recovery_and_recalculates_without_new_market()
{
    use shared_run::paper_strategy::*;
    let evidence = PaperHostEvidenceCase::new("silent-expiry-recovery");
    let (base, rules) = setup(&evidence.path);
    let config = PaperStrategyConfigV1 {
        statistics_input_set_identity: Sha256Hex("d".repeat(64)),
        statistics: PublicStatisticsConfigV1 {
            momentum_windows_ms: vec![50],
            spot_variance_windows_ms: vec![50],
            terminal_integral_windows_ms: vec![50],
            blb_lookback_windows_ms: vec![],
        },
        runtime: base.runtime,
    };
    std::fs::write(
        evidence.path.join("config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
    let listener = UnixListener::bind(&config.runtime.socket_path).unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let frozen_bid = Arc::new(std::sync::Mutex::new(vec![]));
    let strategy = ControlledOnlineStrategy {
        entered: Some(entered_tx),
        release: Some(release_rx),
        frozen_bid: frozen_bid.clone(),
        completed: false,
        silence_ms: 25,
        scope: SourceStreamScopeV1 {
            source: SourceStreamKindV1::PolymarketBook,
            condition_id: Some(ConditionId(CONDITION_ID.into())),
            asset_or_symbol: UP_TOKEN_ID.into(),
        },
    };
    let peer = async {
        let mut stream = accept(&listener).await;
        send(&mut stream, 1, metadata()).await;
        send(&mut stream, 2, book_row(1)).await;
        entered_rx.await.unwrap();
        // 明确跨过该夹具25ms数学期限；该等待不替代SIM成交或反馈推进。
        tokio::time::sleep(Duration::from_millis(75)).await;
        let mut recovered = book_row(2);
        if let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)) = &mut recovered {
            if let PolymarketOrderbookEvent::Book { bids, .. } = &mut row.event {
                bids[0].price = "0.4".into();
            }
        }
        send(&mut stream, 3, recovered).await;
        release_tx.send(()).unwrap();
        // 保持连接无新行情。宿主必须领取计算结果并自行最早合法重算/Exit。
        let mut tail = [0u8; 1];
        use tokio::io::AsyncReadExt;
        let _ = stream.read(&mut tail).await;
    };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            run_paper_strategy_v1(&config, rules, strategy, std::future::pending()),
            peer
        )
    })
    .await
    .expect("有限受控宿主必须返回");
    let report = run.unwrap();
    std::fs::write(
        evidence.path.join("report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    assert!(matches!(report.outcome, PaperStrategyEndV1::StrategyExit));
    assert!(!report.sim_noe_ledger_recording_drained);
    assert!(report.tail_stability_unknown);
    assert_eq!(
        report.measurements.actual_sent_intents, 0,
        "恢复不能复活已过期原动作"
    );
    assert_eq!(report.measurements.calculations_invalidated, 1);
    assert!(report.measurements.calculations_completed >= 2);
    assert!(report.measurements.peak_retained_public_groups >= 2);
    assert!(
        report
            .measurements
            .last_mathematical_invalid_at_utc_ms
            .unwrap()
            < report.measurements.last_invalid_detected_at_utc_ms.unwrap()
    );
    assert_eq!(
        frozen_bid.lock().unwrap().as_slice(),
        &[Some(Some(ExactDecimal("0.5".into())))]
    );
}

struct ExitOnlyOnlineStrategy;
impl shared_run::StrategyV1 for ExitOnlyOnlineStrategy {
    type Error = std::convert::Infallible;
    type Diagnostics = ();
    fn decide(
        &mut self,
        _: &shared_run::StrategyInputV1,
        _: i64,
    ) -> Result<shared_run::StrategyOutputV1, Self::Error> {
        panic!("只使用必要冻结入口")
    }
}
impl shared_run::DetachedStrategyV1 for ExitOnlyOnlineStrategy {
    fn decide_detached(
        &mut self,
        _: &shared_run::DetachedStrategyInputV1,
        _: i64,
    ) -> Result<shared_run::StrategyOutputV1, Self::Error> {
        Ok(shared_run::StrategyOutputV1::Exit { diagnostics: () })
    }
}
#[tokio::test(flavor = "current_thread")]
async fn strategy_exit_ends_actual_process_without_recording_drain_or_run_seal() {
    use shared_run::paper_strategy::*;
    if let Some(config_path) = std::env::var_os("PAPER_EXIT_CHILD_CONFIG") {
        let config: PaperStrategyConfigV1 =
            serde_json::from_slice(&std::fs::read(config_path).unwrap()).unwrap();
        let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
            snapshot_path: std::env::var_os("PAPER_EXIT_RULE_SNAPSHOT").unwrap().into(),
            expected_file_sha256: Sha256Hex(
                "d63c0e7b09edc4b2c4c3f0295f806b25f6f5fd03663cdf60ff062117e53e4e54".into(),
            ),
            replay_causal_start_utc_ms: i64::MAX,
        })
        .unwrap();
        let report = run_paper_strategy_v1(
            &config,
            rules,
            ExitOnlyOnlineStrategy,
            std::future::pending(),
        )
        .await
        .unwrap();
        std::fs::write(
            config
                .runtime
                .ledger_root
                .parent()
                .unwrap()
                .join("child-report.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
        assert!(matches!(report.outcome, PaperStrategyEndV1::StrategyExit));
        return;
    }
    let evidence = PaperHostEvidenceCase::new("actual-process-exit");
    let (base, _) = setup(&evidence.path);
    let config = PaperStrategyConfigV1 {
        runtime: base.runtime,
        statistics_input_set_identity: Sha256Hex("d".repeat(64)),
        statistics: PublicStatisticsConfigV1 {
            momentum_windows_ms: vec![],
            spot_variance_windows_ms: vec![],
            terminal_integral_windows_ms: vec![],
            blb_lookback_windows_ms: vec![],
        },
    };
    let config_path = evidence.path.join("child-config.json");
    std::fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
    let listener = UnixListener::bind(&config.runtime.socket_path).unwrap();
    let output = std::fs::File::create(evidence.path.join("child-stdout.log")).unwrap();
    let errors = std::fs::File::create(evidence.path.join("child-stderr.log")).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "strategy_exit_ends_actual_process_without_recording_drain_or_run_seal",
            "--nocapture",
        ])
        .env("PAPER_EXIT_CHILD_CONFIG", &config_path)
        .env(
            "PAPER_EXIT_RULE_SNAPSHOT",
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json"),
        )
        .stdout(output)
        .stderr(errors)
        .spawn()
        .unwrap();
    let pid = child.id();
    let wait = async {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    let peer = async {
        use tokio::io::AsyncReadExt;
        let mut stream = accept(&listener).await;
        send(&mut stream, 1, metadata()).await;
        // 不发送End，不请求撤单、终局或赎回；只有宿主Exit能关闭此连接。
        let mut tail = vec![];
        stream.read_to_end(&mut tail).await.unwrap();
    };
    let (status, ()) =
        tokio::time::timeout(Duration::from_secs(10), async { tokio::join!(wait, peer) })
            .await
            .expect("实际子进程应正常退出；超时只报告，不强杀或伪造成功");
    assert!(status.success());
    assert!(
        !Path::new(&format!("/proc/{pid}")).exists(),
        "原程序及其线程已退出"
    );
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(evidence.path.join("child-report.json")).unwrap())
            .unwrap();
    assert_eq!(report["outcome"], "StrategyExit");
    assert_eq!(report["sim_noe_ledger_recording_drained"], false);
    assert_eq!(report["tail_stability_unknown"], true);
    assert!(
        !std::fs::read_to_string(&config.runtime.sim_log)
            .unwrap()
            .contains("RUN_SEAL")
    );
    assert!(
        !std::fs::read_dir(&config.runtime.ledger_root)
            .unwrap()
            .any(|entry| entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("ledger-sealed-v1--")),
        "原stop_and_drain会封存已开户Ledger段；Exit未调用此排空路径"
    );
}

struct ObserveNoopStrategy {
    reads: Arc<std::sync::Mutex<Vec<Option<i64>>>>,
    wake_scheduled: bool,
}
impl shared_run::StrategyV1 for ObserveNoopStrategy {
    type Error = std::convert::Infallible;
    type Diagnostics = ();
    fn requires_source_progress(&self) -> bool {
        true
    }
    fn decide(
        &mut self,
        _: &shared_run::StrategyInputV1,
        _: i64,
    ) -> Result<shared_run::StrategyOutputV1, Self::Error> {
        panic!("必要冻结入口")
    }
}
impl shared_run::DetachedStrategyV1 for ObserveNoopStrategy {
    fn decide_detached(
        &mut self,
        input: &shared_run::DetachedStrategyInputV1,
        at: i64,
    ) -> Result<shared_run::StrategyOutputV1, Self::Error> {
        let scope = SourceStreamScopeV1 {
            source: SourceStreamKindV1::PolymarketBook,
            condition_id: Some(ConditionId(CONDITION_ID.into())),
            asset_or_symbol: UP_TOKEN_ID.into(),
        };
        let received = input
            .markets
            .iter()
            .find(|m| m.observation.market.condition_id.0 == CONDITION_ID)
            .and_then(|m| m.observation.source_progress.get_scope(&scope))
            .and_then(|p| p.last_valid_business_message.as_ref())
            .map(|e| e.received_at_utc_ms);
        self.reads.lock().unwrap().push(received);
        assert_eq!(
            input.action_before.cash_available(),
            rust_decimal::Decimal::from(100)
        );
        assert_eq!(
            input
                .action_before
                .account_economic_numbers()
                .confirmed_net_pnl,
            contracts::economics::EconomicRational::zero()
        );
        let next = if received.is_some() && !self.wake_scheduled {
            self.wake_scheduled = true;
            Some(at + 25)
        } else {
            None
        };
        Ok(shared_run::StrategyOutputV1::Continue {
            intents: vec![],
            next_evaluation_at_utc_ms: next,
            diagnostics: (),
        })
    }
}
#[tokio::test(flavor = "current_thread")]
async fn strategy_host_end_stopped_duplicate_and_quiet_query_preserve_real_source_progress() {
    use shared_run::paper_strategy::*;
    for stopped in [false, true] {
        let evidence = PaperHostEvidenceCase::new(if stopped {
            "strategy-stopped-source-progress"
        } else {
            "strategy-end-source-progress"
        });
        let (base, rules) = setup(&evidence.path);
        let config = PaperStrategyConfigV1 {
            runtime: base.runtime,
            statistics_input_set_identity: Sha256Hex("d".repeat(64)),
            statistics: PublicStatisticsConfigV1 {
                momentum_windows_ms: vec![],
                spot_variance_windows_ms: vec![],
                terminal_integral_windows_ms: vec![],
                blb_lookback_windows_ms: vec![],
            },
        };
        std::fs::write(
            evidence.path.join("config.json"),
            serde_json::to_vec_pretty(&config).unwrap(),
        )
        .unwrap();
        let listener = UnixListener::bind(&config.runtime.socket_path).unwrap();
        let reads = Arc::new(std::sync::Mutex::new(vec![]));
        let strategy = ObserveNoopStrategy {
            reads: reads.clone(),
            wake_scheduled: false,
        };
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let stop = async {
            if stopped {
                let _ = stop_rx.await;
            } else {
                std::future::pending().await
            }
        };
        let expected_received = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let peer = async {
            let mut stream = accept(&listener).await;
            send(&mut stream, 1, metadata()).await;
            let row = book_row(1);
            let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(book)) = &row else {
                panic!()
            };
            expected_received.store(book.timestamp_received, std::sync::atomic::Ordering::SeqCst);
            send(&mut stream, 2, row.clone()).await;
            send(&mut stream, 3, row).await;
            tokio::time::sleep(Duration::from_millis(90)).await;
            if stopped {
                stop_tx.send(()).unwrap();
            } else {
                write_handoff_v2(
                    &mut stream,
                    &DataCollectHandoffV2::End {
                        last_confirmed_sequence: 3,
                    },
                )
                .await
                .unwrap();
            }
            use tokio::io::AsyncReadExt;
            let mut tail = vec![];
            let _ = stream.read_to_end(&mut tail).await;
        };
        let phases = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observer = |p: PaperStrategyProgressV1<'_>| {
            assert!(p.total_inputs.is_none());
            phases.lock().unwrap().push((
                p.phase.to_owned(),
                p.measurements.applied_inputs,
                p.measurements.applied_groups,
            ));
        };
        let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(
                run_paper_strategy_with_progress_v1(&config, rules, strategy, stop, observer),
                peer
            )
        })
        .await
        .unwrap();
        let report = run.unwrap();
        assert!(report.sim_noe_ledger_recording_drained);
        assert!(!report.tail_stability_unknown);
        match report.outcome {
            PaperStrategyEndV1::Input(PublicOnlineOutcomeV1::End(ref end)) if !stopped => {
                assert!(end.committed_prefix.normal_end)
            }
            PaperStrategyEndV1::Input(PublicOnlineOutcomeV1::Stopped(_)) if stopped => {}
            _ => panic!("End/Stopped必须保留实际身份"),
        }
        let expected = expected_received.load(std::sync::atomic::Ordering::SeqCst);
        let received: Vec<_> = reads.lock().unwrap().iter().flatten().copied().collect();
        assert!(received.len() >= 2, "静默25ms策略唤醒产生真实新读取");
        assert!(
            received.iter().all(|at| *at == expected),
            "重复输入及纯查询不刷新正式业务进展"
        );
        assert!(requests(&config.runtime.sim_log).is_empty());
        assert!(
            !std::fs::read_to_string(&config.runtime.sim_log)
                .unwrap()
                .contains("RUN_SEAL")
        );
        std::fs::write(
            evidence.path.join("report.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
        std::fs::write(
            evidence.path.join("source-progress-reads.json"),
            serde_json::to_vec_pretty(&*reads.lock().unwrap()).unwrap(),
        )
        .unwrap();
        std::fs::write(
            evidence.path.join("progress.json"),
            serde_json::to_vec_pretty(&*phases.lock().unwrap()).unwrap(),
        )
        .unwrap();
    }
}

#[derive(Default)]
struct PrefixBoundaryFacts {
    qualifications: Vec<(usize, i64, i64)>,
    receipts: Vec<shared_run::SentIntentReceiptV1>,
    invalidated_at: Option<(usize, i64, i64)>,
    final_cash: Option<rust_decimal::Decimal>,
}
struct PrefixBoundaryStrategy {
    finished: bool,
    invalidated: Option<tokio::sync::oneshot::Sender<()>>,
    facts: Arc<std::sync::Mutex<PrefixBoundaryFacts>>,
}
impl shared_run::StrategyV1 for PrefixBoundaryStrategy {
    type Error = std::convert::Infallible;
    type Diagnostics = ();
    fn uses_borrowed_input(&self) -> bool {
        true
    }
    fn decide(
        &mut self,
        _: &shared_run::StrategyInputV1,
        _: i64,
    ) -> Result<shared_run::StrategyOutputV1, Self::Error> {
        panic!("必要冻结端口")
    }
    fn qualify_intent_borrowed(
        &mut self,
        read: &shared_run::BorrowedStrategyCurrentReadV1<'_>,
        _: &shared_run::StrategyCalculationV1,
        index: usize,
        at: i64,
    ) -> Result<shared_run::IntentQualificationV1, Self::Error> {
        self.facts.lock().unwrap().qualifications.push((
            index,
            at,
            read.action_before
                .cutoff()
                .decision_time()
                .logical_time_utc_ms(),
        ));
        // 有限受控计算负载使下一项时刻确实不同；不是生产延迟或额外发送条件。
        if index == 0 {
            std::thread::sleep(Duration::from_millis(15));
            return Ok(shared_run::IntentQualificationV1::Qualified);
        }
        Ok(shared_run::IntentQualificationV1::Invalidated(
            shared_run::IntentInvalidationReasonV1 {
                reason_code: "controlled_suffix_expired".into(),
                scopes: vec![],
            },
        ))
    }
    fn invalidated(
        &mut self,
        _: &shared_run::StrategyCalculationV1,
        result: &shared_run::InvalidatedBatchV1,
    ) {
        assert!(result.release_errors.is_empty());
        let mut facts = self.facts.lock().unwrap();
        facts.receipts = result.sent.clone();
        facts.invalidated_at = Some((
            result.invalidated_intent_index,
            result.detected_at_utc_ms,
            result.observed_cutoff.decision_time().logical_time_utc_ms(),
        ));
        self.finished = true;
        self.invalidated.take().unwrap().send(()).unwrap();
    }
}
impl shared_run::DetachedStrategyV1 for PrefixBoundaryStrategy {
    fn decide_detached(
        &mut self,
        input: &shared_run::DetachedStrategyInputV1,
        _: i64,
    ) -> Result<shared_run::StrategyOutputV1, Self::Error> {
        let ready = input.markets.iter().any(|m| {
            m.observation
                .assets
                .iter()
                .any(|a| a.asset.token_id.0 == UP_TOKEN_ID && a.bids.value.is_some())
        });
        if self.finished {
            self.facts.lock().unwrap().final_cash = Some(input.action_before.cash_available());
            return Ok(shared_run::StrategyOutputV1::Continue {
                intents: vec![],
                next_evaluation_at_utc_ms: None,
                diagnostics: (),
            });
        }
        if !ready {
            return Ok(shared_run::StrategyOutputV1::Continue {
                intents: vec![],
                next_evaluation_at_utc_ms: None,
                diagnostics: (),
            });
        }
        let cancel = |id| shared_run::ReservedIntentV1 {
            context: context(id),
            intent: Arc::new(ModelIntentV1::CancelMarketOrders {
                condition_id: ConditionId(CONDITION_ID.into()),
            }),
            reserves: vec![],
        };
        Ok(shared_run::StrategyOutputV1::Continue {
            intents: vec![
                cancel("prefix-cancel"),
                shared_run::ReservedIntentV1 {
                    context: context("expired-buy"),
                    intent: Arc::new(ModelIntentV1::SubmitSingleBuy {
                        condition_id: ConditionId(CONDITION_ID.into()),
                        order: BuyLegIntentV1 {
                            token_id: TokenId(UP_TOKEN_ID.into()),
                            order: BuyOrderIntentV1::GtcLimit {
                                limit_price_pusd_per_share: rust_decimal::Decimal::new(2, 1),
                                size_shares: rust_decimal::Decimal::from(5),
                                post_only: true,
                            },
                        },
                    }),
                    reserves: vec![shared_run::IntentReserveV1 {
                        batch_item_ordinal: 0,
                        reserve: ledger::ReserveIntentV1 {
                            request_identity: ledger::LedgerRequestIdentityV1(
                                "expired-buy-reserve".into(),
                            ),
                            condition_id: ConditionId(CONDITION_ID.into()),
                            requested_principal_pusd: ExactDecimal("1".into()),
                            immediate_fee_reserve_pusd: ExactDecimal("0".into()),
                        },
                    }],
                },
                cancel("suffix-cancel-must-not-send"),
            ],
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        })
    }
}
#[tokio::test(flavor = "current_thread")]
async fn strategy_each_actual_item_boundary_keeps_sent_prefix_and_releases_expired_suffix_at_zero_recording_capacity()
 {
    use shared_run::paper_strategy::*;
    let evidence = PaperHostEvidenceCase::new("zero-capacity-actual-items");
    let (base, rules) = setup(&evidence.path);
    let mut runtime = base.runtime;
    runtime.finite_in_flight_groups = 0; // 原合法rendezvous容量，无新增最小容量。
    let config = PaperStrategyConfigV1 {
        runtime,
        statistics_input_set_identity: Sha256Hex("d".repeat(64)),
        statistics: PublicStatisticsConfigV1 {
            momentum_windows_ms: vec![],
            spot_variance_windows_ms: vec![],
            terminal_integral_windows_ms: vec![],
            blb_lookback_windows_ms: vec![],
        },
    };
    std::fs::write(
        evidence.path.join("config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
    let listener = UnixListener::bind(&config.runtime.socket_path).unwrap();
    let facts = Arc::new(std::sync::Mutex::new(PrefixBoundaryFacts::default()));
    let (invalid_tx, invalid_rx) = tokio::sync::oneshot::channel();
    let strategy = PrefixBoundaryStrategy {
        finished: false,
        invalidated: Some(invalid_tx),
        facts: facts.clone(),
    };
    let peer = async {
        let mut stream = accept(&listener).await;
        send(&mut stream, 1, metadata()).await;
        send(&mut stream, 2, book_row(1)).await;
        invalid_rx.await.unwrap();
        // 下一必要冻结由宿主自行调度；后继一个真实输入只作为停止前缀的因果边界。
        send(&mut stream, 3, book_row(2)).await;
        write_handoff_v2(
            &mut stream,
            &DataCollectHandoffV2::End {
                last_confirmed_sequence: 3,
            },
        )
        .await
        .unwrap();
    };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            run_paper_strategy_v1(&config, rules, strategy, std::future::pending()),
            peer
        )
    })
    .await
    .unwrap();
    let report = run.unwrap();
    assert!(report.sim_noe_ledger_recording_drained);
    let facts = facts.lock().unwrap();
    assert_eq!(
        facts.qualifications.len(),
        2,
        "失效后缀包含cancel且不再资格/发送"
    );
    assert_eq!(facts.receipts.len(), 1);
    assert!(facts.receipts[0].newly_registered);
    assert_eq!(facts.receipts[0].sent_at_utc_ms, facts.qualifications[0].1);
    assert_eq!(facts.qualifications[0].1, facts.qualifications[0].2);
    assert_eq!(facts.qualifications[1].1, facts.qualifications[1].2);
    assert!(
        facts.qualifications[1].1 > facts.qualifications[0].1,
        "第二项必须取自己实际时点"
    );
    assert_eq!(
        facts.invalidated_at,
        Some((1, facts.qualifications[1].1, facts.qualifications[1].1))
    );
    assert_eq!(
        facts.final_cash,
        Some(rust_decimal::Decimal::from(100)),
        "原整动作预留的未发送部分准确释放"
    );
    let req = requests(&config.runtime.sim_log);
    assert_eq!(req.len(), 1);
    assert_eq!(
        report.measurements.actual_sent_intents, 1,
        "失效后缀前的真实新登记前缀仍须计入"
    );
    assert_eq!(
        req[0]["request"]["context"]["global_request_id"],
        "prefix-cancel"
    );
    assert!((2..=3).contains(&report.measurements.peak_request_recording_groups));
    std::fs::write(
        evidence.path.join("report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    std::fs::write(
        evidence.path.join("qualifications.json"),
        serde_json::to_vec_pretty(&facts.qualifications).unwrap(),
    )
    .unwrap();
    std::fs::write(
        evidence.path.join("sent.json"),
        serde_json::to_vec_pretty(&facts.receipts).unwrap(),
    )
    .unwrap();
}

struct SettlementSocketStrategy {
    submitted: bool,
    accepted: Option<tokio::sync::oneshot::Sender<()>>,
    settled: Option<tokio::sync::oneshot::Sender<()>>,
    prefix_read: Option<(u64, tokio::sync::oneshot::Sender<(String, bool)>)>,
    reads: Arc<std::sync::Mutex<Vec<(i64, String, Vec<String>)>>>,
}
impl shared_run::StrategyV1 for SettlementSocketStrategy {
    type Error = std::convert::Infallible;
    type Diagnostics = ();
    fn decide(
        &mut self,
        _: &shared_run::StrategyInputV1,
        _: i64,
    ) -> Result<shared_run::StrategyOutputV1, Self::Error> {
        panic!("正式必要冻结端口")
    }
    fn qualify_intent_borrowed(
        &mut self,
        _: &shared_run::BorrowedStrategyCurrentReadV1<'_>,
        _: &shared_run::StrategyCalculationV1,
        _: usize,
        _: i64,
    ) -> Result<shared_run::IntentQualificationV1, Self::Error> {
        Ok(shared_run::IntentQualificationV1::Qualified)
    }
}
impl shared_run::DetachedStrategyV1 for SettlementSocketStrategy {
    fn decide_detached(
        &mut self,
        input: &shared_run::DetachedStrategyInputV1,
        at: i64,
    ) -> Result<shared_run::StrategyOutputV1, Self::Error> {
        let cash = input.action_before.cash_available();
        let positions = input
            .action_before
            .market_economics(&ConditionId(CONDITION_ID.into()))
            .map(|m| m.success_shares.iter().map(ToString::to_string).collect())
            .unwrap_or_default();
        self.reads
            .lock()
            .unwrap()
            .push((at, cash.to_string(), positions));
        if input
            .action_before
            .orders()
            .any(|o| o.cancellable == ExecutionReadValueV1::Known(true))
        {
            if let Some(tx) = self.accepted.take() {
                let _ = tx.send(());
            }
        }
        if self.prefix_read.as_ref().is_some_and(|(prefix, _)| {
            input
                .action_before
                .cutoff()
                .receipt()
                .group()
                .applied_prefix
                .next_input_position
                >= *prefix
        }) {
            let (_, tx) = self.prefix_read.take().unwrap();
            let pending = input
                .action_before
                .obligation_trades()
                .iter()
                .any(|t| !t.success_applied());
            let _ = tx.send((cash.to_string(), pending));
        }
        if cash == rust_decimal::Decimal::from(104) {
            if let Some(tx) = self.settled.take() {
                let _ = tx.send(());
            }
        }
        let ready = input.markets.iter().any(|m| {
            m.observation
                .assets
                .iter()
                .filter(|a| a.bids.value.is_some() && a.asks.value.is_some())
                .count()
                == 2
        });
        let mut intents = vec![];
        if ready && !self.submitted {
            self.submitted = true;
            intents.push(shared_run::ReservedIntentV1 {
                context: context("settlement-maker"),
                intent: Arc::new(ModelIntentV1::SubmitSingleBuy {
                    condition_id: ConditionId(CONDITION_ID.into()),
                    order: BuyLegIntentV1 {
                        token_id: TokenId(UP_TOKEN_ID.into()),
                        order: BuyOrderIntentV1::GtcLimit {
                            limit_price_pusd_per_share: rust_decimal::Decimal::new(2, 1),
                            size_shares: rust_decimal::Decimal::from(5),
                            post_only: true,
                        },
                    },
                }),
                reserves: vec![shared_run::IntentReserveV1 {
                    batch_item_ordinal: 0,
                    reserve: ledger::ReserveIntentV1 {
                        request_identity: ledger::LedgerRequestIdentityV1(
                            "settlement-maker:0".into(),
                        ),
                        condition_id: ConditionId(CONDITION_ID.into()),
                        requested_principal_pusd: ExactDecimal("1".into()),
                        immediate_fee_reserve_pusd: ExactDecimal("0".into()),
                    },
                }],
            });
        }
        Ok(shared_run::StrategyOutputV1::Continue {
            intents,
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn source_time_ws_terminal_pays_actual_old_l2_maker_once_through_production_host() {
    for mode in ["ws", "missing-ws", "gamma-unknown", "ws-pending"] {
        let case = source_time_settlement_case(mode);
        println!(
            "settlement {mode} future_bytes={}",
            std::mem::size_of_val(&case)
        );
        Box::pin(case).await;
    }
}
async fn source_time_settlement_case(mode: &str) {
    use shared_run::paper_strategy::*;
    let evidence = PaperHostEvidenceCase::new(&format!("settle-{mode}"));
    let (base, rules) = setup(&evidence.path);
    let config = PaperStrategyConfigV1 {
        runtime: base.runtime,
        statistics_input_set_identity: Sha256Hex("e".repeat(64)),
        statistics: PublicStatisticsConfigV1 {
            momentum_windows_ms: vec![],
            spot_variance_windows_ms: vec![],
            terminal_integral_windows_ms: vec![],
            blb_lookback_windows_ms: vec![],
        },
    };
    if mode == "ws-pending" {
        // 显式工程政策输入，让真实终态先于原成交成功；不改生产默认、时钟或延迟。
        std::fs::write(&config.runtime.finality_policy_path, br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":4000},{"state":"CONFIRMED","offset_ms":5000}],"economic_application_ordinal":1},"failure_overrides":[]}"#).unwrap();
    }
    let listener = UnixListener::bind(&config.runtime.socket_path).unwrap();
    let (prefix_tx, prefix_rx) = tokio::sync::oneshot::channel();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let (settled_tx, settled_rx) = tokio::sync::oneshot::channel();
    let reads = Arc::new(std::sync::Mutex::new(vec![]));
    let strategy = SettlementSocketStrategy {
        submitted: false,
        accepted: Some(accepted_tx),
        settled: Some(settled_tx),
        prefix_read: (mode != "ws").then_some((7, prefix_tx)),
        reads: reads.clone(),
    };
    let mut meta = metadata();
    let TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(m)) = &mut meta else {
        panic!()
    };
    // 合法五分钟市场临近末端；实际UTC等待端点，不修改生产时钟或延迟。
    let end = (now() / 1000 + 3) * 1000;
    m.market_start_utc = end - 300_000;
    m.market_end_utc = end;
    m.slug = format!("btc-updown-5m-{}", m.market_start_utc / 1000);
    let market = MarketIdentityV2 {
        condition_id: ConditionId(CONDITION_ID.into()),
        clob_market_id: ClobMarketId(m.clob_market_id.clone()),
        window: MarketWindowV2 {
            start_at_utc_ms: m.market_start_utc,
            end_at_utc_ms: end,
        },
    };
    let slug = m.slug.clone();
    let peer = async {
        let mut stream = accept(&listener).await;
        send(&mut stream, 1, meta).await;
        send(&mut stream, 2, book_row(1)).await;
        let mut down = book_row(2);
        if let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)) = &mut down {
            if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut r.event {
                *asset_id = DOWN_TOKEN_ID.into();
            }
        }
        send(&mut stream, 3, down).await;
        accepted_rx.await.unwrap();
        assert!(
            now() < end,
            "真实Accepted必须仍在市场内，不能用本地登记代替"
        );
        for (seq, size) in [(3, "5"), (4, "0")] {
            let mut row = book_row(seq);
            let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)) = &mut row else {
                panic!()
            };
            r.event = PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![PolymarketPriceChangeEntry {
                    asset_id: UP_TOKEN_ID.into(),
                    price: "0.2".into(),
                    size: Some(size.into()),
                    side: PolymarketSide::Buy,
                    best_bid: None,
                    best_ask: None,
                }],
            };
            send(&mut stream, seq + 1, row).await;
        }
        tokio::time::sleep(Duration::from_millis((end + 1 - now()).max(0) as u64)).await;
        let source = end + 1;
        let received = now().max(source);
        let mut terminal_meta = fixture_terminal::terminal_metadata_row(&market);
        terminal_meta.slug = slug.clone();
        terminal_meta.fixed_identity_verified_at_utc = end - 300_001;
        terminal_meta.timestamp_received = received;
        terminal_meta.collector_admission_time_utc_ms = received + 2;
        terminal_meta.source_evidence_received_at_utc = Some(received);
        terminal_meta.row_evidence_validation_completed_at_utc = Some(received + 1);
        terminal_meta.lifecycle_applied_at_utc = Some(received + 2);
        terminal_meta.runtime_role_applied_at_utc = Some(received + 2);
        terminal_meta.ws_platform_timestamp_raw = Some(source.to_string());
        terminal_meta.ws_platform_timestamp_utc = Some(source);
        let mut ws = fixture_terminal::terminal_orderbook_row(&market);
        ws.slug = Some(slug);
        ws.timestamp = Some(source);
        ws.timestamp_received = received;
        ws.collector_admission_time_utc_ms = received + 2;
        if mode == "gamma-unknown" {
            terminal_meta.metadata_row_kind = PolymarketMetadataRowKind::ResolutionPollAttempt;
            terminal_meta.metadata_origin = PolymarketMetadataOrigin::GammaBySlug;
            terminal_meta.source_endpoint =
                Some("https://gamma-api.polymarket.com/markets/slug".into());
            terminal_meta.source_evidence_origin = Some(MetadataSourceEvidenceOrigin::GammaBySlug);
            terminal_meta.source_evidence_event_type =
                Some(MetadataSourceEvidenceEventType::GammaMarketBySlugResponse);
            terminal_meta.source_evidence_channel = None;
            terminal_meta.metadata_fetch_start_utc = Some(received - 20);
            terminal_meta.closed = Some(true);
            terminal_meta.closed_time_raw = None;
            // collector闭市事实不是Gamma的平台源事件时刻；保留原WS/source time None。
            terminal_meta.closed_time_utc = Some(source);
            terminal_meta.closed_time_check_failure_reason = None;
            terminal_meta.automatically_resolved = Some(true);
            terminal_meta.uma_resolution_status = Some("resolved".into());
            terminal_meta.outcomes_raw = Some(r#"["UP","DOWN"]"#.into());
            terminal_meta.outcome_prices_raw = Some(r#"["1","0"]"#.into());
            terminal_meta.active = Some(true);
            terminal_meta.active_unavailable_reason = None;
            terminal_meta.accepting_orders = Some(false);
            terminal_meta.ws_platform_timestamp_raw = None;
            terminal_meta.ws_platform_timestamp_utc = None;
            terminal_meta.resolution_evidence_source =
                Some(MetadataResolutionEvidenceSource::GammaBySlug);
            terminal_meta.terminal_claim_owner = Some(MetadataTerminalClaimOwner::GammaBySlug);
            terminal_meta.resolution_poll_attempt_ordinal = Some(4);
            terminal_meta.resolution_poll_scheduled_utc = Some(received - 30);
            terminal_meta.resolution_poll_result = Some(MetadataResolutionPollResult::Terminal);
            terminal_meta.resolution_followup_status =
                Some(MetadataResolutionFollowupStatus::TerminalGammaFallback);
            terminal_meta.ws_claimant_source_sequence_epoch_id = None;
            terminal_meta.ws_claimant_collector_admission_sequence = None;
        }
        send(
            &mut stream,
            6,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
                terminal_meta,
            ))),
        )
        .await;
        let valid_ws = mode == "ws" || mode == "ws-pending";
        if valid_ws {
            send(
                &mut stream,
                7,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(ws.clone())),
            )
            .await;
        } else {
            send(&mut stream, 7, book_row(5)).await;
        }
        if mode != "ws" {
            let (cash, pending) = prefix_rx.await.unwrap();
            assert_eq!(
                cash.parse::<rust_decimal::Decimal>().unwrap(),
                rust_decimal::Decimal::from(99),
                "证据缺失或经济前驱未闭时不得回款"
            );
            if mode == "ws-pending" {
                assert!(pending, "终态已消费时仍保留原真实未成功成交");
            }
        }
        if valid_ws {
            settled_rx.await.unwrap();
        }
        // 相同正式来源身份重复输入不能第二次兑付；后继真实组触发账户重读。
        if valid_ws {
            send(
                &mut stream,
                8,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(ws)),
            )
            .await;
        } else {
            send(&mut stream, 8, book_row(6)).await;
        }
        write_handoff_v2(
            &mut stream,
            &DataCollectHandoffV2::End {
                last_confirmed_sequence: 8,
            },
        )
        .await
        .unwrap();
    };
    let run = Box::pin(run_paper_strategy_v1(
        &config,
        rules,
        strategy,
        std::future::pending(),
    ));
    println!(
        "settlement {mode} boxed_host_future_bytes={}",
        std::mem::size_of_val(&run)
    );
    let (report, ()) =
        tokio::time::timeout(Duration::from_secs(45), async { tokio::join!(run, peer) })
            .await
            .expect("有限正式链未完成，保留现场而不造成功");
    let report_value = if mode == "missing-ws" {
        let error = report.unwrap_err();
        assert!(
            error.to_string().contains("UnpairedWsTerminalMetadata"),
            "缺WS的原DPT自然结束必须如实拒绝，不伪造完整End：{error}"
        );
        serde_json::json!({"error": error.to_string(), "tail_stability_unknown": true})
    } else {
        let report = report.unwrap();
        assert!(matches!(
            report.outcome,
            PaperStrategyEndV1::Input(PublicOnlineOutcomeV1::End(_))
        ));
        assert_eq!(report.measurements.actual_sent_intents, 1);
        serde_json::to_value(report).unwrap()
    };
    assert_eq!(requests(&config.runtime.sim_log).len(), 1);
    let reads = reads.lock().unwrap();
    if mode == "ws" || mode == "ws-pending" {
        assert!(reads.iter().any(|(_, cash, _)| cash == "104"));
    } else {
        assert!(!reads.iter().any(|(_, cash, _)| cash == "104"));
    }
    assert!(reads.iter().all(
        |(_, cash, _)| cash.parse::<rust_decimal::Decimal>().unwrap()
            <= rust_decimal::Decimal::from(104)
    ));
    std::fs::write(
        evidence.path.join("account-reads.json"),
        serde_json::to_vec_pretty(&*reads).unwrap(),
    )
    .unwrap();
    std::fs::write(
        evidence.path.join("report.json"),
        serde_json::to_vec_pretty(&report_value).unwrap(),
    )
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn finite_input_during_background_calculation_measures_retained_groups_and_release() {
    use shared_run::paper_strategy::*;
    let evidence = PaperHostEvidenceCase::new("background-128-public-groups");
    let (base, rules) = setup(&evidence.path);
    let config = PaperStrategyConfigV1 {
        statistics_input_set_identity: Sha256Hex("d".repeat(64)),
        statistics: PublicStatisticsConfigV1 {
            momentum_windows_ms: vec![50],
            spot_variance_windows_ms: vec![50],
            terminal_integral_windows_ms: vec![50],
            blb_lookback_windows_ms: vec![],
        },
        runtime: base.runtime,
    };
    std::fs::write(
        evidence.path.join("config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
    let listener = UnixListener::bind(&config.runtime.socket_path).unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let frozen_bid = Arc::new(std::sync::Mutex::new(vec![]));
    let strategy = ControlledOnlineStrategy {
        entered: Some(entered_tx),
        release: Some(release_rx),
        frozen_bid: frozen_bid.clone(),
        completed: false,
        silence_ms: 25,
        scope: SourceStreamScopeV1 {
            source: SourceStreamKindV1::PolymarketBook,
            condition_id: Some(ConditionId(CONDITION_ID.into())),
            asset_or_symbol: UP_TOKEN_ID.into(),
        },
    };
    let peer = async {
        let mut stream = accept(&listener).await;
        send(&mut stream, 1, metadata()).await;
        send(&mut stream, 2, book_row(1)).await;
        entered_rx.await.unwrap();
        // 明确跨过该夹具25ms数学期限；该等待不替代SIM成交或反馈推进。
        tokio::time::sleep(Duration::from_millis(75)).await;
        for ordinal in 0..128u64 {
            let mut recovered = book_row(ordinal + 2);
            if let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)) = &mut recovered {
                if let PolymarketOrderbookEvent::Book { bids, .. } = &mut row.event {
                    bids[0].size = (101 + ordinal).to_string();
                }
            }
            send(&mut stream, ordinal + 3, recovered).await;
        }
        release_tx.send(()).unwrap();
        // 保持连接无新行情。宿主必须领取计算结果并自行最早合法重算/Exit。
        let mut tail = [0u8; 1];
        use tokio::io::AsyncReadExt;
        let _ = stream.read(&mut tail).await;
    };
    let (run, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            run_paper_strategy_v1(&config, rules, strategy, std::future::pending()),
            peer
        )
    })
    .await
    .expect("有限受控宿主必须返回");
    let report = run.unwrap();
    std::fs::write(
        evidence.path.join("report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    assert!(matches!(report.outcome, PaperStrategyEndV1::StrategyExit));
    assert!(!report.sim_noe_ledger_recording_drained);
    assert!(report.tail_stability_unknown);
    assert_eq!(
        report.measurements.actual_sent_intents, 0,
        "恢复不能复活已过期原动作"
    );
    assert_eq!(report.measurements.calculations_invalidated, 1);
    assert!(report.measurements.calculations_completed >= 2);
    assert_eq!(report.measurements.peak_retained_public_groups, 129);
    assert_eq!(report.measurements.retained_public_groups_at_end, 0);
    assert_eq!(report.measurements.applied_inputs, 130);
    println!("background128 {}", serde_json::to_string(&report).unwrap());
    assert!(
        report
            .measurements
            .last_mathematical_invalid_at_utc_ms
            .unwrap()
            < report.measurements.last_invalid_detected_at_utc_ms.unwrap()
    );
    assert_eq!(
        frozen_bid.lock().unwrap().as_slice(),
        &[Some(Some(ExactDecimal("0.5".into())))]
    );
}
