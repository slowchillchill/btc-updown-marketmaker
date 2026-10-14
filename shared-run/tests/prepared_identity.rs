mod common;
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
