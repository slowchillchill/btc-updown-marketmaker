#[path = "common/fixture_registry.rs"]
mod fixture_registry;
#[cfg(feature = "stage-timing")]
#[path = "common/fixture_market_identity.rs"]
mod fixture_market_identity;
#[cfg(feature = "stage-timing")]
#[path = "common/fixture_multi_market.rs"]
mod fixture_multi_market;
#[cfg(feature = "stage-timing")]
#[path = "support/cancellation_scaling.rs"]
mod cancellation_scaling;
#[path = "common/fixture_clock.rs"]
mod fixture_clock;
#[path = "common/fixture_execution.rs"]
mod fixture_execution;
#[path = "common/fixture_execution_v4.rs"]
mod fixture_execution_v4;
#[path = "support/historical.rs"]
mod fixture_historical;
#[path = "common/fixture_input.rs"]
mod fixture_input;
#[path = "common/fixture_pair.rs"]
mod fixture_pair;
#[path = "common/fixture_policy.rs"]
mod fixture_policy;
#[path = "common/fixture_recording.rs"]
mod fixture_recording;
#[path = "common/fixture_reference.rs"]
mod fixture_reference;
#[path = "common/fixture_terminal.rs"]
mod fixture_terminal;
#[path = "common/fixture_trading.rs"]
mod fixture_trading;
mod common {
    pub use super::fixture_execution_v4::*;
    pub use super::fixture_historical::*;
    pub use super::fixture_input::*;
    pub use super::fixture_pair::*;
    pub use super::fixture_policy::*;
    pub use super::fixture_recording::*;
    pub use super::fixture_trading::*;
}

use common::*;
use contracts::*;
use ledger::*;
use shared_run::*;
use sim_system::{execution::*, public_consumption::*};
use std::sync::Arc;

fn request(id: &str) -> RequestContextV2 {
    RequestContextV2 {
        global_request_id: id.into(),
        caller_correlation_id: None,
    }
}
fn restore_client(
    shared: SharedExecutionV2,
    observation: MemoryNoeObservationV4,
    driver: &mut PublicConsumptionStateV1,
    ledger_root: &std::path::Path,
) -> SharedExecutionV2 {
    // 沿原完整点顺序先交接后台材料，再保存客户端并替换记录接收器。
    let mut execution = Vec::new();
    driver.write_checkpoint(&mut execution).unwrap();
    let mut ledger = Vec::new();
    let mut account = Vec::new();
    shared.ledger().write_checkpoint(&mut ledger).unwrap();
    shared.write_checkpoint(&mut account).unwrap();
    drop(shared);
    let mut ledger = LedgerWriterV1::restore_checkpoint(ledger.as_slice()).unwrap();
    driver
        .reconnect_ledger_recording(&mut ledger, ledger_root)
        .unwrap();
    SharedExecutionV2::restore_checkpoint(ledger, observation, account.as_slice()).unwrap()
}
fn field<'a>(value: &'a ciborium::Value, name: &str) -> &'a ciborium::Value {
    &value
        .as_map()
        .unwrap()
        .iter()
        .find(|(key, _)| key.as_text() == Some(name))
        .unwrap()
        .1
}
fn contains_text(value: &ciborium::Value, text: &str) -> bool {
    match value {
        ciborium::Value::Text(s) => s == text,
        ciborium::Value::Array(values) => values.iter().any(|v| contains_text(v, text)),
        ciborium::Value::Map(values) => values
            .iter()
            .any(|(k, v)| contains_text(k, text) || contains_text(v, text)),
        _ => false,
    }
}
fn balance(shared: &SharedExecutionV2) {
    let read = shared.ledger().economic_read().unwrap();
    assert_eq!(read.cash().actual_available_pusd.0, "99.8711");
    assert_eq!(read.cash().actual_reserved_pusd.0, "0.1289");
}
#[tokio::test(flavor = "current_thread")]
async fn historical_prepared_identity_converts_early_economics_once_and_survives_both_client_restore_cuts()
 {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_trading_window(&root, "100", None, 60_000, true);
    let (mut execution, observation, _) =
        execution_historical_v4(&temp.path().join("sim.jsonl"), &core, format!("{:064x}", 1));
    let reader = data_pipeline_training::public_recording::PublicRecordingReaderV1::recover(
        &root,
        1,
        1 << 20,
    )
    .unwrap();
    let prepared_hidden = data_pipeline_training::hidden_preparation::prepare_hidden_v1(
        core,
        reader,
        &temp.path().join("hidden"),
        &config(),
        "historical-prepared-cut".into(),
    )
    .unwrap();
    let mut hidden =
        RecordedHiddenBucketSourceV1::open(&prepared_hidden, &root, 1, 1 << 20).unwrap();
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    let ledger_root = temp.path().join("ledger");
    execution
        .bind_ledger_recording_v2(&mut ledger, &ledger_root)
        .unwrap();
    let mut shared = SharedExecutionV2::from_opening(ledger, observation.clone())
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "prepared-cut".into());
    let mut public_groups = 0;
    while driver
        .next_group_at()
        .unwrap()
        .is_some_and(|at| at <= 22_000)
    {
        driver.next_group().unwrap().unwrap();
        public_groups += 1;
    }
    let mut hidden_buckets = 0;
    let next_hidden = loop {
        let bucket = hidden.next_bucket().unwrap().unwrap();
        let at = bucket.reference().bucket_time_utc_ms();
        if at > 22_000 {
            break at;
        }
        driver.hidden_bucket(bucket).unwrap();
        hidden_buckets += 1;
    };
    eprintln!(
        "历史构造：发送22000之前 public_groups={public_groups} hidden_buckets={hidden_buckets}, next_public={:?}, next_hidden={next_hidden}",
        driver.next_group_at().unwrap()
    );
    shared.action_before(&mut driver).unwrap().unwrap();
    let intent = Arc::new(ModelIntentV1::SubmitSingleBuy {
        condition_id: ConditionId(CONDITION_ID.into()),
        order: BuyLegIntentV1 {
            token_id: TokenId(UP_TOKEN_ID.into()),
            order: BuyOrderIntentV1::GtcLimit {
                limit_price_pusd_per_share: rust_decimal::Decimal::new(1, 2),
                size_shares: rust_decimal::Decimal::new(1289, 2),
                post_only: true,
            },
        },
    });
    let reserve = ReserveIntentV1 {
        request_identity: LedgerRequestIdentityV1("prepared-funds".into()),
        condition_id: ConditionId(CONDITION_ID.into()),
        requested_principal_pusd: ExactDecimal("0.1289".into()),
        immediate_fee_reserve_pusd: ExactDecimal("0".into()),
    };
    let receipt = shared
        .execute_reserved(
            &mut driver.decision().unwrap(),
            request("prepared-early"),
            Some(intent),
            &[reserve.clone()],
        )
        .await
        .unwrap()
        .unwrap();
    balance(&shared);
    assert_eq!(
        shared
            .ledger()
            .projection_v2()
            .unwrap()
            .account_instance_key
            .caller_role,
        SimCallerRoleV1::HistoricalBacktest
    );
    // 原SIM检查点确实保存了尚未处理请求的身份模式，未持有可外部指定的订单ID。
    let mut sim_bytes = Vec::new();
    driver.write_checkpoint(&mut sim_bytes).unwrap();
    let sim_saved: ciborium::Value = ciborium::de::from_reader(sim_bytes.as_slice()).unwrap();
    let pending = field(
        field(
            field(field(&sim_saved, "state"), "session"),
            "mutation_requests",
        ),
        "pending_actions",
    );
    assert_eq!(pending.as_map().unwrap().len(), 1);
    assert!(contains_text(pending, "PublicRequestV1"));
    assert!(!contains_text(pending, "prepared_order_id"));
    assert_eq!(
        shared
            .ledger()
            .funds_read_v1()
            .requests()
            .next()
            .unwrap()
            .reservation_state,
        ReservationStatusV1::SubmittedUnknown
    );
    // 初始关联既未受理、也未打开或应用经济事实。
    let mut snapshot_bytes = Vec::new();
    shared
        .ledger()
        .write_checkpoint(&mut snapshot_bytes)
        .unwrap();
    let snapshot: ciborium::Value = ciborium::de::from_reader(snapshot_bytes.as_slice()).unwrap();
    let orders = field(field(&snapshot, "lifecycle"), "orders")
        .as_array()
        .unwrap();
    assert_eq!(orders.len(), 1);
    for order in orders {
        let value = &order.as_array().unwrap()[1];
        assert_eq!(field(value, "open").as_bool(), Some(false));
        assert_eq!(field(value, "economic_applied").as_bool(), Some(false));
        assert_eq!(
            field(value, "accepted_principal_counted").as_bool(),
            Some(false)
        );
    }
    shared = restore_client(shared, observation.clone(), &mut driver, &ledger_root);
    balance(&shared);
    // 固定新版seed1：平台22006、生命周期22016、响应22022。
    // next_required_logical_drive不枚举反馈交付，必须在现有公开observe_at取截点。
    assert_eq!(receipt.platform_arrived_at_utc_ms(), 22_006);
    driver.observe_at(22_017).unwrap().unwrap();
    shared.apply_closed().unwrap();
    let early_status = shared
        .ledger()
        .funds_read_v1()
        .requests()
        .next()
        .unwrap()
        .reservation_state;
    let early_response = shared
        .registry()
        .orders()
        .any(|o| o.latest_direct_feedback.is_some());
    eprintln!(
        "实际观察 at=22017 arrival={} reservation={early_status:?} direct_response={early_response}",
        receipt.platform_arrived_at_utc_ms()
    );
    assert_eq!(early_status, ReservationStatusV1::Converted);
    assert!(!early_response, "必须实际观察经济先到、响应未到的截点");
    balance(&shared);
    shared = restore_client(shared, observation.clone(), &mut driver, &ledger_root);
    balance(&shared);
    driver.observe_at(22_023).unwrap().unwrap();
    shared.apply_closed().unwrap();
    assert!(
        shared
            .registry()
            .orders()
            .any(|o| o.latest_direct_feedback.is_some()),
        "随后必须实际收到提交响应"
    );
    let late_status = shared
        .ledger()
        .funds_read_v1()
        .requests()
        .next()
        .unwrap()
        .reservation_state;
    eprintln!("实际观察 at=22023 reservation={late_status:?} direct_response=true");
    assert_eq!(late_status, ReservationStatusV1::Converted);
    // 重复领取闭合前缀不再变更占用。
    shared.apply_closed().unwrap();
    balance(&shared);
    let before = shared.ledger().projection_v2();
    driver.stop_and_drain().unwrap();
    let recovered = LedgerWriterV1::open_durable(&ledger_root).unwrap();
    assert_eq!(recovered.projection_v2(), before);
}

/// 有界检查点成本观察：8/80个真实订单历史；不作为回测吞吐或收益门。
#[tokio::test(flavor = "current_thread")]
#[ignore = "#185 bounded registry checkpoint history cost observation"]
async fn registry_checkpoint_cost_keeps_real_eight_and_eighty_order_histories() {
    use rust_decimal::Decimal;
    for orders in [8, 80] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let core = recorded_memory_complete(&root);
        let (mut execution, observation, _) = execution_with_rules_v4(
            &temp.path().join("sim.jsonl"),
            &core,
            SUCCESS_POLICY,
            None,
            "e".repeat(64),
            "1000",
        );
        let mut ledger = LedgerWriterV1::new_with_admission_v3(
            AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
        );
        execution
            .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
            .unwrap();
        let mut shared = SharedExecutionV2::from_opening(ledger, observation.clone())
            .unwrap()
            .unwrap();
        let source =
            RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "registry-cost".into());
        for _ in 0..2 {
            driver.next_group().unwrap().unwrap();
        }
        for index in 0..orders {
            shared
                .execute(
                    &mut driver.decision().unwrap(),
                    request(&format!("registry-cost-{index}")),
                    Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                        condition_id: ConditionId(CONDITION_ID.into()),
                        order: BuyLegIntentV1 {
                            token_id: TokenId(UP_TOKEN_ID.into()),
                            order: BuyOrderIntentV1::GtcLimit {
                                limit_price_pusd_per_share: Decimal::new(1, 2),
                                size_shares: Decimal::from(5),
                                post_only: true,
                            },
                        },
                    })),
                )
                .await
                .unwrap();
        }
        while driver.next_group().unwrap().is_some() {
            shared.apply_closed().unwrap();
        }
        driver.stop_and_drain().unwrap();
        shared.apply_closed().unwrap();
        assert_eq!(shared.registry().orders().count(), orders);
        let started = std::time::Instant::now();
        let mut bytes = Vec::new();
        for _ in 0..32 {
            bytes.clear();
            shared.write_checkpoint(&mut bytes).unwrap();
        }
        let elapsed = started.elapsed();
        let mut ledger_bytes = Vec::new();
        shared.ledger().write_checkpoint(&mut ledger_bytes).unwrap();
        let restored = SharedExecutionV2::restore_checkpoint(
            LedgerWriterV1::restore_checkpoint(ledger_bytes.as_slice()).unwrap(),
            observation,
            bytes.as_slice(),
        )
        .unwrap();
        assert_eq!(restored.registry().orders().count(), orders);
        assert_eq!(
            restored.ledger().economic_read().unwrap().cash(),
            shared.ledger().economic_read().unwrap().cash()
        );
        println!(
            "registry_checkpoint_cost={}",
            serde_json::json!({"orders":orders,"captures":32,"encode_in_memory_ns":elapsed.as_nanos(),"bytes":bytes.len(),"restored_orders":restored.registry().orders().count()})
        );
    }
}

/// #392 保持同一终态公开前缀、现金和零活跃责任，只增加真实已终结订单历史。
#[cfg(feature = "stage-timing")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit #392 bounded historical-order visit measurement"]
async fn ended_order_history_cost_at_identical_current_responsibility() {
    use rust_decimal::Decimal;
    let mut expected = None;
    for orders in [0, 128] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let core = recorded_memory_complete(&root);
        let (mut execution, observation, _) = execution_with_rules_v4(
            &temp.path().join("sim.jsonl"),
            &core,
            SUCCESS_POLICY,
            None,
            "e".repeat(64),
            "1000",
        );
        let mut ledger = LedgerWriterV1::new_with_admission_v3(
            AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
        );
        execution
            .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
            .unwrap();
        let mut shared = SharedExecutionV2::from_opening(ledger, observation.clone())
            .unwrap()
            .unwrap();
        let source =
            RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "history-cost".into());
        driver.enable_finite_recording_wait().unwrap();
        for _ in 0..2 {
            driver.next_group().unwrap().unwrap();
        }
        for index in 0..orders {
            driver.wait_for_recording_capacity().unwrap();
            shared
                .execute(
                    &mut driver.decision().unwrap(),
                    request(&format!("history-{index}")),
                    Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                        condition_id: ConditionId(CONDITION_ID.into()),
                        order: BuyLegIntentV1 {
                            token_id: TokenId(UP_TOKEN_ID.into()),
                            order: BuyOrderIntentV1::GtcLimit {
                                limit_price_pusd_per_share: Decimal::new(1, 2),
                                size_shares: Decimal::from(5),
                                post_only: true,
                            },
                        },
                    })),
                )
                .await
                .unwrap()
                .unwrap();
        }
        let mut canceled = false;
        while driver.next_group().unwrap().is_some() {
            shared.apply_closed().unwrap();
            fixture_registry::assert_cancel_objects_reference(shared.registry());
            if !canceled
                && driver
                    .observation_cutoff()
                    .unwrap()
                    .decision_time()
                    .logical_time_utc_ms()
                    >= 26_000
            {
                shared
                    .execute(
                        &mut driver.decision().unwrap(),
                        request("history-cancel"),
                        Some(Arc::new(ModelIntentV1::CancelMarketOrders {
                            condition_id: ConditionId(CONDITION_ID.into()),
                        })),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                canceled = true;
            }
        }
        assert!(canceled);
        let cutoff = serde_json::to_value(driver.observation_cutoff()).unwrap();
        assert!(cutoff.is_object());
        driver.stop_and_drain().unwrap();
        shared.apply_closed().unwrap();
        assert_eq!(shared.registry().orders().count(), orders);
        let cash = shared.ledger().economic_read().unwrap().cash().to_owned();
        let before = account_history_visits_v1();
        let started = std::time::Instant::now();
        for _ in 0..32 {
            let ExecutionReadValueV1::Known(objects) = shared.registry().cancel_objects() else {
                panic!("known terminal objects")
            };
            assert!(objects.is_empty(), "真实终态历史没有可撤责任");
        }
        let elapsed = started.elapsed().as_nanos();
        let after = account_history_visits_v1();
        let original_started = std::time::Instant::now();
        let original_before = account_history_visits_v1();
        for _ in 0..32 {
            let ExecutionReadValueV1::Known(objects) = shared
                .registry()
                .cancel_objects_full_history_for_measurement()
            else {
                panic!("known original objects")
            };
            assert!(objects.is_empty());
        }
        let original_elapsed = original_started.elapsed().as_nanos();
        let original_after = account_history_visits_v1();
        // 兼容真实旧检查点缺少可重建索引的表示，事实和引用池保持原样。
        let mut checkpoint = Vec::new();
        shared.write_checkpoint(&mut checkpoint).unwrap();
        let mut value: ciborium::value::Value =
            ciborium::de::from_reader(checkpoint.as_slice()).unwrap();
        let ciborium::value::Value::Map(fields) = &mut value else {
            panic!("checkpoint map")
        };
        let (_, ciborium::value::Value::Map(registry)) = fields
            .iter_mut()
            .find(|(k, _)| k.as_text() == Some("registry"))
            .unwrap()
        else {
            panic!("registry map")
        };
        registry.retain(|(k, _)| k.as_text() != Some("action_population"));
        let mut old_checkpoint = Vec::new();
        ciborium::ser::into_writer(&value, &mut old_checkpoint).unwrap();
        let mut ledger_bytes = Vec::new();
        shared.ledger().write_checkpoint(&mut ledger_bytes).unwrap();
        let restored = SharedExecutionV2::restore_checkpoint(
            LedgerWriterV1::restore_checkpoint(ledger_bytes.as_slice()).unwrap(),
            observation.clone(),
            old_checkpoint.as_slice(),
        )
        .unwrap();
        fixture_registry::assert_cancel_objects_reference(restored.registry());
        let restored_before = account_history_visits_v1();
        let _ = restored.registry().cancel_objects();
        let restored_after = account_history_visits_v1();
        assert_eq!(
            restored_after.cancel_order_visits - restored_before.cancel_order_visits,
            0,
            "旧索引在恢复时重建，闭合历史无逐帧扫描"
        );
        assert_eq!(restored.registry().orders().count(), orders);

        let current = serde_json::json!({"cash":cash,"public_group":cutoff["receipt"]["group"],
            "visible_at":cutoff["decision_time"]["logical_time_utc_ms"],"active_cancel_objects":0});
        if let Some(expected) = &expected {
            assert_eq!(&current, expected);
        } else {
            expected = Some(current);
        }
        println!(
            "ended_history_cost={}",
            serde_json::json!({"ended_orders":orders,"frames":32,"wall_ns":elapsed,
            "cancel_order_visits":after.cancel_order_visits-before.cancel_order_visits,
            "paired_intent_visits":after.paired_intent_visits-before.paired_intent_visits,
            "original_wall_ns":original_elapsed,"original_cancel_order_visits":original_after.cancel_order_visits-original_before.cancel_order_visits})
        );
    }
}

/// #392 已完成撤单不再进入在途统计；最新响应按原完成交付序查询。
#[cfg(feature = "stage-timing")]
#[tokio::test(flavor = "current_thread")]
async fn cancellation_history_index_preserves_real_feedback_and_legacy_checkpoint() {
    let mut responsibility = None;
    for history in [0, 128] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let core = recorded_memory_complete(&root);
        let (mut execution, observation, _) = execution_with_rules_v4(
            &temp.path().join("sim.jsonl"),
            &core,
            SUCCESS_POLICY,
            None,
            "e".repeat(64),
            "1000",
        );
        let mut ledger = LedgerWriterV1::new_with_admission_v3(
            AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
        );
        execution
            .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
            .unwrap();
        let mut shared = SharedExecutionV2::from_opening(ledger, observation.clone())
            .unwrap()
            .unwrap();
        let source =
            RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "cancel-history".into());
        driver.enable_finite_recording_wait().unwrap();
        for _ in 0..2 {
            driver.next_group().unwrap().unwrap();
        }
        let condition = ConditionId(CONDITION_ID.into());
        let other = ConditionId(format!("0x{}", "2".repeat(64)));
        let market = CancellationScopeV1::Market(&condition);
        let no_match = CancellationScopeV1::Market(&other);
        let unknown_order = CancellationScopeV1::Orders(vec!["not-yet-located"]);
        for index in 0..=history {
            let id = if index == history {
                "latest".into()
            } else {
                format!("history-{index:03}")
            };
            driver.wait_for_recording_capacity().unwrap();
            shared
                .execute(
                    &mut driver.decision().unwrap(),
                    request(&id),
                    Some(Arc::new(ModelIntentV1::CancelMarketOrders {
                        condition_id: condition.clone(),
                    })),
                )
                .await
                .unwrap()
                .unwrap();
        }
        for scope in [None, Some(&market), Some(&no_match), Some(&unknown_order)] {
            fixture_registry::assert_cancellation_requests_reference(shared.registry(), scope);
        }
        assert_eq!(
            shared.registry().pending_cancellation_count(None),
            ExecutionReadValueV1::Known(history + 1)
        );
        while driver.next_group().unwrap().is_some() {
            shared.apply_closed().unwrap();
            for scope in [None, Some(&market), Some(&no_match), Some(&unknown_order)] {
                fixture_registry::assert_cancellation_requests_reference(shared.registry(), scope);
            }
        }
        let cutoff = serde_json::to_value(driver.observation_cutoff()).unwrap();
        driver.stop_and_drain().unwrap();
        shared.apply_closed().unwrap();
        let read = shared.registry().cancellation_requests(None);
        assert_eq!(read.pending_count, ExecutionReadValueV1::Known(0));
        assert_eq!(
            read.response_counts,
            [const { ExecutionReadValueV1::Known(0) }; 3]
        );
        assert!(matches!(
            read.latest_response,
            ExecutionReadValueV1::Known(Some(_))
        ));
        assert!(matches!(
            shared
                .registry()
                .cancellation_requests(Some(&unknown_order))
                .latest_response,
            ExecutionReadValueV1::Unavailable
        ));
        let current = serde_json::json!({"cash":shared.ledger().economic_read().unwrap().cash(),
            "group":cutoff["receipt"]["group"],"visible_at":cutoff["decision_time"]["logical_time_utc_ms"],
            "orders":shared.registry().orders().count(),"pending":read.pending_count,"counts":read.response_counts});
        if let Some(expected) = &responsibility {
            assert_eq!(&current, expected);
        } else {
            responsibility = Some(current);
        }
        for (name, scope) in [("latest", None), ("no_related_completion", Some(&no_match))] {
            let before = account_history_visits_v1();
            let started = std::time::Instant::now();
            for _ in 0..32 {
                let _ = shared.registry().cancellation_requests(scope);
            }
            let elapsed = started.elapsed().as_nanos();
            let after = account_history_visits_v1();
            let original_before = account_history_visits_v1();
            let original_started = std::time::Instant::now();
            for _ in 0..32 {
                let _ = shared
                    .registry()
                    .cancellation_requests_full_history_for_measurement(scope);
            }
            let original_elapsed = original_started.elapsed().as_nanos();
            let original_after = account_history_visits_v1();
            assert_eq!(
                after.latest_cancel_request_visits - before.latest_cancel_request_visits,
                32 * if name == "latest" { 1 } else { 0 }
            );
            println!(
                "cancel_history_cost={}",
                serde_json::json!({"history":history,"completed_requests":history+1,"case":name,"frames":32,
                "wall_ns":elapsed,"source_visits":after.cancellation_source_visits-before.cancellation_source_visits,
                "latest_visits":after.latest_cancel_request_visits-before.latest_cancel_request_visits,
                "original_wall_ns":original_elapsed,"original_source_visits":original_after.cancellation_source_visits-original_before.cancellation_source_visits,
                "original_latest_visits":original_after.latest_cancel_request_visits-original_before.latest_cancel_request_visits})
            );
        }
        let mut bytes = Vec::new();
        shared.write_checkpoint(&mut bytes).unwrap();
        let mut value: ciborium::Value = ciborium::de::from_reader(bytes.as_slice()).unwrap();
        let ciborium::Value::Map(fields) = &mut value else {
            panic!("checkpoint")
        };
        let (_, ciborium::Value::Map(registry)) = fields
            .iter_mut()
            .find(|(k, _)| k.as_text() == Some("registry"))
            .unwrap()
        else {
            panic!("registry")
        };
        let (_, ciborium::Value::Map(population)) = registry
            .iter_mut()
            .find(|(k, _)| k.as_text() == Some("action_population"))
            .unwrap()
        else {
            panic!("population")
        };
        population.retain(|(k, _)| k.as_text() != Some("cancellation_reads"));
        let mut old = Vec::new();
        ciborium::ser::into_writer(&value, &mut old).unwrap();
        let mut ledger_bytes = Vec::new();
        shared.ledger().write_checkpoint(&mut ledger_bytes).unwrap();
        let restored = SharedExecutionV2::restore_checkpoint(
            LedgerWriterV1::restore_checkpoint(ledger_bytes.as_slice()).unwrap(),
            observation.clone(),
            old.as_slice(),
        )
        .unwrap();
        for scope in [None, Some(&market), Some(&no_match), Some(&unknown_order)] {
            fixture_registry::assert_cancellation_requests_reference(restored.registry(), scope);
        }
        let before = account_history_visits_v1();
        let _ = restored.registry().cancellation_requests(None);
        let after = account_history_visits_v1();
        assert_eq!(
            after.latest_cancel_request_visits - before.latest_cancel_request_visits,
            1,
            "旧活动索引缺少新字段时恢复一次补建"
        );
    }
}
