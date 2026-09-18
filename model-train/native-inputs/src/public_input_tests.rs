#[allow(dead_code)]
#[path = "../../../shared-run/tests/common/mod.rs"]
pub(crate) mod common;
use crate::*;
use common::*;
use contracts::{economics::*, *};
use ledger::*;
use rust_decimal::Decimal;
use shared_run::*;
use sim_system::{execution::*, public_consumption::*};
use std::sync::Arc;
fn context(id: &str) -> RequestContextV2 {
    RequestContextV2 {
        global_request_id: id.into(),
        caller_correlation_id: None,
    }
}
fn leg(token: &str, size: i64) -> BuyLegIntentV1 {
    BuyLegIntentV1 {
        token_id: TokenId(token.into()),
        order: BuyOrderIntentV1::GtcLimit {
            limit_price_pusd_per_share: Decimal::new(2, 1),
            size_shares: Decimal::from(size),
            post_only: true,
        },
    }
}
fn projection(n: &str) -> f32 {
    crate::exact::signed_log_f32(&contracts::economics::EconomicRational::from_decimal(
        n.parse().unwrap(),
    ))
}
#[tokio::test(flavor = "current_thread")]
async fn real_public_chain_freezes_cash_encodes_original_legs_and_cold_root() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_memory_complete(&root);
    let (mut execution, observation) =
        execution(&temp.path().join("sim.jsonl"), &core, SUCCESS_POLICY);
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let account = shared
        .ledger()
        .projection_v2()
        .unwrap()
        .account_instance_key;
    let mut native = NativeInputRoot::new("trajectory".into(), account.clone(), -10_000);
    let mut cold = NativeInputRoot::new("cold".into(), account, 0);
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "native".into());
    let mut exported = Vec::new();
    let mut frozen = None;
    for index in 0..2 {
        driver.next_group().unwrap().unwrap();
        let group = native.consume(&mut shared, &mut driver).unwrap().unwrap();
        assert!(
            cold.consume(&mut shared, &mut driver)
                .unwrap()
                .unwrap()
                .rows
                .is_empty()
        );
        if index == 0 {
            assert!(matches!(
                group.pre_frame_hidden_ops[0],
                HiddenOp::ResetRootHidden { .. }
            ));
            assert_eq!(group.rows[0].role, InputRole::ContextOnly);
            assert_eq!(&group.rows[0].slow[0..3], &[0., 0., 0.]);
        } else {
            assert_eq!(group.rows[0].role, InputRole::DecisionRequired);
            assert_eq!(group.rows[0].slow[110], projection("100"));
            assert!(group.pre_frame_hidden_ops.is_empty());
        }
        exported.push(exported_group(&group));
        if index == 1 {
            frozen = Some(group);
        }
    }
    let frozen = frozen.unwrap();
    shared
        .execute(
            &mut driver.decision().unwrap(),
            context("paired"),
            Some(Arc::new(ModelIntentV1::SubmitPairedBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                orders: [leg(UP_TOKEN_ID, 50), leg(DOWN_TOKEN_ID, 50)],
            })),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frozen.rows[0].slow[110], projection("100"));
    assert_eq!(
        frozen
            .action_before
            .as_ref()
            .unwrap()
            .economics()
            .cash
            .actual_available_pusd
            .0,
        "100"
    );
    let mut saw_orders = false;
    let mut retired = 0;
    while driver.next_group().unwrap().is_some() {
        let group = native.consume(&mut shared, &mut driver).unwrap().unwrap();
        retired += group.post_frame_hidden_ops.len();
        if group
            .rows
            .iter()
            .any(|r| r.role == InputRole::DecisionRequired)
            && group.cancel.len() == 4
        {
            if !saw_orders {
                // 既有 late registry 正式入口，真实引用缺源组件读取，不是合法组。
                let read = group.action_before.as_ref().unwrap();
                let orders = read.orders().collect::<Vec<_>>();
                let mut late = SharedExecutionRegistryV1::new(Arc::new(
                    orders[0].accepted.unwrap().context().clone(),
                ));
                for order in &orders {
                    let origin = order.origin.as_ref().unwrap();
                    late.freeze(
                        origin.intent.global_request_id().into(),
                        Arc::new(origin.intent.intent().clone()),
                    );
                    late.observe(order.accepted.unwrap());
                }
                late.observe(orders[0].formed.unwrap());
                let partial = late.orders().collect::<Vec<_>>();
                assert_eq!(partial.len(), 2);
                assert!(
                    crate::account::order_ages(&partial, 30_000)
                        .iter()
                        .all(|v| matches!(v, ExecutionReadValueV1::Unavailable))
                );
                late.observe(orders[1].formed.unwrap());
                let source_times = orders
                    .iter()
                    .map(|o| crate::values::times(o.formed).unwrap().0)
                    .collect::<Vec<_>>();
                let ages = crate::account::order_ages(&late.orders().collect::<Vec<_>>(), 30_000);
                for (value, time) in ages.iter().zip([
                    source_times.iter().min().unwrap(),
                    source_times.iter().max().unwrap(),
                ]) {
                    assert_eq!(
                        *value,
                        ExecutionReadValueV1::Known(crate::exact::ratio(30_000 - *time, 1000))
                    );
                }
            }
            saw_orders = true;
            assert_eq!(group.rows[0].slow[110], projection("80"));
            assert_eq!(group.cancel[0][5], 1.);
            assert_eq!(group.cancel[1][6], 1.);
            assert_eq!(group.cancel[2][5], 1.);
            assert_eq!(group.cancel[2][56], 1.);
            assert_eq!(group.cancel[0][34], projection("10"));
            assert_eq!(group.cancel[3][105], projection("1"));
            assert_eq!(group.cancel[3][108], projection("1"));
        }
        exported.push(exported_group(&group));
    }
    assert!(saw_orders);
    assert_eq!(retired, 1);
    if let Some(path) = std::env::var_os("NATIVE_INPUT_FIXTURE_OUTPUT") {
        std::fs::write(path, serde_json::to_vec(&exported).unwrap()).unwrap();
    }
}

#[test]
fn overlap_context_reads_current_reserve_and_old_local_only_survives_switch() {
    use data_pipeline_core::{public_input::*, typed_raw::*};
    use data_pipeline_training::public_recording::*;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let mut inputs = Vec::new();
    for (position, other, at, active) in [
        (0, false, 10_000, false),
        (1, false, 20_000, true),
        (2, true, 290_000, false),
        (3, true, 320_000, true),
    ] {
        let DptIngressV1::Message {
            row: TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(mut m)),
            ..
        } = meta(0).ingress
        else {
            unreachable!()
        };
        m.market_end_utc = if other { 620_000 } else { 320_000 };
        m.timestamp_received = at;
        m.collector_admission_sequence = position + 1;
        m.collector_admission_time_utc_ms = at + 1;
        m.metadata_fetch_start_utc = Some(at - 10);
        m.source_evidence_received_at_utc = Some(at);
        m.row_evidence_validation_completed_at_utc = Some(at + 1);
        if other {
            m.market_start_utc = 320_000;
            m.condition_id = OTHER_CONDITION_ID.into();
            m.clob_market_id = "3195577".into();
            m.asset_ids = "[\"21\",\"4\"]".into();
            m.fixed_identity_verified_at_utc = at - 1;
        }
        if active {
            m.market_lifecycle_state = MetadataMarketLifecycleState::Active;
            m.market_runtime_role = MetadataMarketRuntimeRole::Current;
        }
        inputs.push(record(
            position,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(m)),
        ));
    }
    inputs.push(RecordedIngressV1 {
        input_stream_id: "core-test".into(),
        input_position: 4,
        ingress: DptIngressV1::EndOfInput,
        dependency_record_refs: vec![],
    });
    for input in inputs {
        let prepared = prepare_public_ingress_v1(&core, &input, &Default::default(), &[]).unwrap();
        let applied = core.push(&prepared).unwrap().into_owned();
        writer.record(input, applied).unwrap();
    }
    writer.stabilize().unwrap();
    let (mut execution, observation) =
        execution(&temp.path().join("sim.jsonl"), &core, SUCCESS_POLICY);
    let mut ledger =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let account = shared
        .ledger()
        .projection_v2()
        .unwrap()
        .account_instance_key;
    let mut native = NativeInputRoot::new("overlap".into(), account, -10_000);
    let source = RecordedPublicGroupSourceV1::open(
        &root,
        1,
        1 << 20,
        TradingTimes(vec![10_000, 20_000, 290_000, 320_000]),
    )
    .unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "overlap".into());
    let mut exported = Vec::new();
    for index in 0..4 {
        driver.next_group().unwrap().unwrap();
        let group = native.consume(&mut shared, &mut driver).unwrap().unwrap();
        exported.push(exported_group(&group));
        if index == 1 {
            assert_eq!(
                shared
                    .reserve_action_v2(&[ReserveIntentV1 {
                        request_identity: LedgerRequestIdentityV1("local-only".into()),
                        condition_id: ConditionId(CONDITION_ID.into()),
                        requested_principal_pusd: ExactDecimal("20".into()),
                        immediate_fee_reserve_pusd: ExactDecimal("0".into())
                    }])
                    .unwrap()
                    .len(),
                1
            );
        }
        if index == 2 {
            assert_eq!(group.current, Some(ConditionId(CONDITION_ID.into())));
            assert!(group.old.is_empty());
            let context = group
                .rows
                .iter()
                .find(|r| r.hidden_state_key.condition_id.0 == OTHER_CONDITION_ID)
                .unwrap();
            assert_eq!(context.role, InputRole::ContextOnly);
            assert_eq!(context.slow[0], projection("20"));
            assert!(group.rows.iter().all(|r| r.slow[110] == projection("80")));
        }
        if index == 3 {
            assert_eq!(group.current, Some(ConditionId(OTHER_CONDITION_ID.into())));
            assert_eq!(group.old, vec![ConditionId(CONDITION_ID.into())]);
            assert!(group.pre_frame_hidden_ops.is_empty(), "切换保持预热hidden");
            assert_eq!(group.post_frame_hidden_ops.len(), 1);
            assert_eq!(group.rows.len(), 1);
            assert_eq!(group.rows[0].slow[27], projection("20"));
            assert_eq!(&group.rows[0].slow[0..3], &[0., 1., 1.]);
        }
    }
    if let Some(path) = std::env::var_os("NATIVE_OVERLAP_FIXTURE_OUTPUT") {
        std::fs::write(path, serde_json::to_vec(&exported).unwrap()).unwrap();
    }
}

fn r(n: &str, d: &str) -> ExactRationalV1 {
    ExactRationalV1 {
        numerator: n.into(),
        denominator: d.into(),
    }
}
#[tokio::test(flavor = "current_thread")]
async fn native_partial_makers_project_average_cost_and_nonterminating_fraction() {
    for fractional in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let (core, times) = recorded_average_pool(&root, fractional);
        let (mut execution, observation) =
            execution(&temp.path().join("sim.jsonl"), &core, SUCCESS_POLICY);
        let mut ledger =
            LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
        execution
            .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
            .unwrap();
        let mut shared = SharedExecutionV1::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "average".into());
        let account = shared
            .ledger()
            .projection_v2()
            .unwrap()
            .account_instance_key;
        let mut native = NativeInputRoot::new("average".into(), account, -10_000);
        for _ in 0..4 {
            driver.next_group().unwrap().unwrap();
            native.consume(&mut shared, &mut driver).unwrap().unwrap();
        }
        async fn buy<I: Iterator<Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>>>(
            shared: &mut SharedExecutionV1,
            driver: &mut FinitePublicConsumptionV1<I>,
            id: &str,
            token: &str,
            price: Decimal,
        ) {
            shared
                .execute(
                    &mut driver.decision().unwrap(),
                    RequestContextV2 {
                        global_request_id: id.into(),
                        caller_correlation_id: None,
                    },
                    Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                        condition_id: ConditionId(CONDITION_ID.into()),
                        order: BuyLegIntentV1 {
                            token_id: TokenId(token.into()),
                            order: BuyOrderIntentV1::GtcLimit {
                                limit_price_pusd_per_share: price,
                                size_shares: Decimal::from(5),
                                post_only: true,
                            },
                        },
                    })),
                )
                .await
                .unwrap();
        }
        buy(
            &mut shared,
            &mut driver,
            "a-first",
            UP_TOKEN_ID,
            Decimal::new(1, 1),
        )
        .await;
        let mut second = false;
        let mut opposite = false;
        while driver.next_group().unwrap().is_some() {
            if !shared.apply_closed().unwrap() {
                continue;
            }
            let at = driver
                .decision()
                .unwrap()
                .cutoff()
                .decision_time()
                .logical_time_utc_ms();
            // 每个截点先读取，再实施原脚本动作。46k是明确诊断中间读取，
            // 不把该组或从它继续的测试探针宣称完整合法模型序列。
            let read = shared.action_before(&mut driver).unwrap().unwrap();
            if at == 46_000 {
                assert!(matches!(
                    read.cancel_objects(),
                    ExecutionReadValueV1::Unavailable
                ));
                assert_eq!(read.orders().count(), 2);
                assert!(
                    read.orders()
                        .any(|o| o.origin.is_none() && o.formed.is_some())
                );
                let bindings = driver
                    .frames()
                    .iter()
                    .map(|f| {
                        (
                            f.observation.market.condition_id.0.clone(),
                            crate::root::MarketBinding {
                                market: f.observation.market.clone(),
                                assets: f
                                    .observation
                                    .assets
                                    .iter()
                                    .map(|a| a.asset.clone())
                                    .collect(),
                            },
                        )
                    })
                    .collect();
                let input = crate::account::encode(
                    &read,
                    Some(&ConditionId(CONDITION_ID.into())),
                    &[],
                    &bindings,
                    at,
                    InputRole::DecisionRequired,
                )
                .unwrap();
                assert_eq!(&input[406..409], &[projection("2"), 1., 1.]);
                assert_eq!(&input[388..391], &[0., 0., 1.]);
                assert!(matches!(
                    native.consume(&mut shared, &mut driver),
                    Err(InputError::MissingAssociation)
                ));
                continue;
            }
            let group = native.consume(&mut shared, &mut driver).unwrap().unwrap();
            if at >= 28_000 && !second {
                second = true;
                let old = shared
                    .registry()
                    .orders()
                    .next()
                    .unwrap()
                    .order_identity
                    .clone();
                shared
                    .execute(
                        &mut driver.decision().unwrap(),
                        RequestContextV2 {
                            global_request_id: "cancel-old".into(),
                            caller_correlation_id: None,
                        },
                        Some(Arc::new(ModelIntentV1::CancelOrder {
                            platform_order_id: match old.key {
                                ::execution_normalization::MemoryIdentityKeyV1::Entity(
                                    ::execution_normalization::SourceNeutralEntityKeyV1::Order {
                                        source_order_id,
                                    },
                                ) => source_order_id,
                                _ => unreachable!(),
                            },
                        })),
                    )
                    .await
                    .unwrap();
                buy(
                    &mut shared,
                    &mut driver,
                    "a-second",
                    UP_TOKEN_ID,
                    if fractional {
                        Decimal::new(45, 2)
                    } else {
                        Decimal::new(9, 1)
                    },
                )
                .await;
            }
            if at >= 38_000 && !opposite {
                opposite = true;
                buy(
                    &mut shared,
                    &mut driver,
                    "b-first",
                    DOWN_TOKEN_ID,
                    Decimal::new(2, 1),
                )
                .await;
            }
            if at >= 56_000 {
                let slow = &group.rows[0].slow;
                let expected_pnl = if fractional {
                    r("7", "15")
                } else {
                    r("3", "10")
                };
                let expected_orphan = if fractional { r("2", "3") } else { r("1", "2") };
                assert_eq!(
                    slow[332],
                    crate::exact::signed_log_f32(
                        &contracts::economics::EconomicRational::try_from(&expected_pnl).unwrap()
                    )
                );
                assert_eq!(
                    slow[341],
                    crate::exact::signed_log_f32(
                        &contracts::economics::EconomicRational::try_from(&expected_orphan)
                            .unwrap()
                    )
                );
                break;
            }
        }
        assert!(shared.apply_closed().unwrap());
        let read = shared.ledger().economic_read().unwrap();
        let allocations: Vec<_> = read.paired_allocations().collect();
        assert_eq!(allocations.len(), 1);
        assert_eq!(allocations[0].quantity_shares.0, "1");
        let m = read.market_totals().next().unwrap();
        let up = m
            .assets
            .as_ref()
            .unwrap()
            .iter()
            .position(|a| a.token_id.0 == UP_TOKEN_ID)
            .unwrap();
        assert_eq!(
            allocations[0].principal_cost_pusd[up],
            if fractional { r("1", "3") } else { r("1", "2") }
        );
        assert_eq!(allocations[0].principal_cost_pusd[1 - up], r("1", "5"));
        assert_eq!(
            m.confirmed_net_pnl_pusd,
            if fractional {
                r("7", "15")
            } else {
                r("3", "10")
            }
        );
        assert_eq!(m.orphan_shares[up].0, if fractional { "2" } else { "1" });
        driver.stop_and_drain().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn complete_real_object_population_crosses_block_boundary() {
    let mut exports = Vec::new();
    for count in [1, 255, 256, 258] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let (core, times) = recorded_trading_window(&root, "100", None, 320_000, true);
        let (mut execution, observation) = execution_with_seed_and_cash(
            &temp.path().join("sim.jsonl"),
            &core,
            SUCCESS_POLICY,
            None,
            "e".repeat(64),
            "1000",
        );
        let mut ledger =
            LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
        execution
            .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
            .unwrap();
        let mut shared = SharedExecutionV1::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let account = shared
            .ledger()
            .projection_v2()
            .unwrap()
            .account_instance_key;
        let mut native = NativeInputRoot::new(format!("objects-{count}"), account, -10_000);
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "population".into());
        for _ in 0..4 {
            driver.next_group().unwrap().unwrap();
            native.consume(&mut shared, &mut driver).unwrap().unwrap();
        }
        for index in 0..count {
            driver.wait_for_recording_capacity().unwrap();
            shared
                .execute(
                    &mut driver.decision().unwrap(),
                    context(&format!("single-{index}")),
                    Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                        condition_id: ConditionId(CONDITION_ID.into()),
                        order: leg(UP_TOKEN_ID, 5),
                    })),
                )
                .await
                .unwrap()
                .unwrap();
        }
        let mut verified = false;
        while driver.next_group().unwrap().is_some() {
            let Some(read) = shared.action_before(&mut driver).unwrap() else {
                continue;
            };
            let at = read.cutoff().decision_time().logical_time_utc_ms();
            if at >= 51_000 {
                let group = native.consume(&mut shared, &mut driver).unwrap().unwrap();
                assert_eq!(group.cancel.len(), count + 1);
                assert_eq!(
                    &group.rows[0].slow[406..409],
                    &[projection(&count.to_string()), 1., 1.]
                );
                assert_eq!(
                    group.cancel.last().unwrap()[105],
                    projection(&count.to_string())
                );
                exports.push(serde_json::to_value(&group).unwrap());
                verified = true;
                break;
            }
        }
        assert!(verified);
        driver.stop_and_drain().unwrap();
    }
    if let Some(path) = std::env::var_os("NATIVE_OBJECT_FIXTURE_OUTPUT") {
        std::fs::write(path, serde_json::to_vec(&exports).unwrap()).unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn complete_population_keeps_uncancellable_pending_and_quote_base_na() {
    for (quote, compensated) in [(false, false), (true, false), (false, true)] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let (core, times) = recorded_trading(&root, "5", false);
        let policy=br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10000},{"state":"CONFIRMED","offset_ms":20000}],"economic_application_ordinal":1},"failure_overrides":[]}"#;
        let mut policy: serde_json::Value = serde_json::from_slice(policy).unwrap();
        if compensated {
            let mut path = policy["normal_path"].clone();
            path["steps"][2]["state"] = serde_json::json!("FAILED");
            policy["failure_overrides"] = serde_json::json!([{"selector":{"selector_domain":"REQUEST","account_instance_key":AccountInstanceKeyV1{run_id:"shared-combination".into(),caller_role:SimCallerRoleV1::Paper,caller_instance_id:"shared-caller".into(),account_id:"shared-account".into(),platform_source:PlatformSourceV1::Simulated},"global_request_id":"take"},"path":path}]);
        }
        let policy = serde_json::to_vec(&policy).unwrap();
        let (mut execution, observation) =
            execution(&temp.path().join("sim.jsonl"), &core, &policy);
        let mut ledger =
            LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
        execution
            .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
            .unwrap();
        let mut shared = SharedExecutionV1::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let account = shared
            .ledger()
            .projection_v2()
            .unwrap()
            .account_instance_key;
        let mut native = NativeInputRoot::new("pending".into(), account, -10_000);
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "pending".into());
        for _ in 0..4 {
            driver.next_group().unwrap().unwrap();
            native.consume(&mut shared, &mut driver).unwrap().unwrap();
        }
        let order = if quote {
            BuyOrderIntentV1::FakMarket {
                spend_pusd: Decimal::from(3),
                worst_price_pusd_per_share: Decimal::new(6, 1),
            }
        } else {
            BuyOrderIntentV1::GtcLimit {
                limit_price_pusd_per_share: Decimal::new(6, 1),
                size_shares: Decimal::from(5),
                post_only: false,
            }
        };
        shared
            .execute(
                &mut driver.decision().unwrap(),
                context("take"),
                Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                    condition_id: ConditionId(CONDITION_ID.into()),
                    order: BuyLegIntentV1 {
                        token_id: TokenId(UP_TOKEN_ID.into()),
                        order,
                    },
                })),
            )
            .await
            .unwrap()
            .unwrap();
        let mut pending = false;
        let mut success = false;
        while driver.next_group().unwrap().is_some() {
            let Some(read) = shared.action_before(&mut driver).unwrap() else {
                continue;
            };
            let at = read.cutoff().decision_time().logical_time_utc_ms();
            if [30_000, 36_000, 46_000, 56_000].contains(&at) {
                let group = native.consume(&mut shared, &mut driver).unwrap().unwrap();
                let slow = &group.rows[0].slow;
                eprintln!(
                    "quote={quote} at={at} population={} balance={} success={} active={} flags={:?} age={:?}",
                    read.orders().count(),
                    slow[54],
                    slow[122],
                    slow[388],
                    &slow[389..391],
                    &slow[382..388]
                );
                if at == 36_000 {
                    assert_eq!(read.orders().count(), 1);
                    let order = read.orders().next().unwrap();
                    assert_eq!(order.cancellable, ExecutionReadValueV1::Known(false));
                    if quote {
                        assert_eq!(
                            order.active_unfilled_shares,
                            ExecutionReadValueV1::NotApplicable
                        );
                    }
                    assert_eq!(&slow[388..391], &[0., 1., 1.]);
                    assert_eq!(&slow[406..409], &[projection("1"), 1., 1.]);
                    assert_eq!(slow[122], 0.);
                    pending = true;
                } else if at == 46_000 {
                    assert_eq!(slow[54], projection("5"));
                    assert_eq!(slow[122], 0.);
                } else if at == 56_000 {
                    assert_eq!(slow[54], if compensated { 0. } else { projection("5") });
                    assert_eq!(slow[122], if compensated { 0. } else { projection("5") });
                    assert_eq!(slow[185], projection("5"));
                    success = true;
                }
            }
        }
        assert!(pending && success);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_rebates_project_exact_contribution_ratios_and_old_sum() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_rebate_contributions(&root);
    let (mut execution, observation) = execution_with_seed_and_cash(
        &temp.path().join("sim.jsonl"),
        &core,
        SUCCESS_POLICY,
        None,
        "e".repeat(64),
        "2000",
    );
    let mut ledger =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "rebate".into());
    for _ in 0..8 {
        driver.next_group().unwrap().unwrap();
    }
    for (condition, token, id) in [
        (CONDITION_ID, UP_TOKEN_ID, "maker-low"),
        (OTHER_CONDITION_ID, "21", "maker-high"),
    ] {
        shared
            .execute(
                &mut driver.decision().unwrap(),
                RequestContextV2 {
                    global_request_id: id.into(),
                    caller_correlation_id: None,
                },
                Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                    condition_id: ConditionId(condition.into()),
                    order: BuyLegIntentV1 {
                        token_id: TokenId(token.into()),
                        order: BuyOrderIntentV1::GtcLimit {
                            limit_price_pusd_per_share: Decimal::new(5, 1),
                            size_shares: Decimal::from(143),
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
        if driver
            .decision()
            .unwrap()
            .cutoff()
            .decision_time()
            .logical_time_utc_ms()
            >= 89_111_000
        {
            break;
        }
    }
    assert!(shared.apply_closed().unwrap());
    let read = shared.ledger().economic_read().unwrap();
    let day = read
        .rebate_days()
        .find(|d| d.kind == RebateKindV1::Maker)
        .unwrap();
    let mut contributions: Vec<_> = day
        .contributions
        .iter()
        .map(|c| c.contribution_pusd.0.as_str())
        .collect();
    contributions.sort();
    assert_eq!(contributions, ["0.50002", "0.50004"]);
    assert_eq!(day.amounts.paid_pusd, r("1", "1"));
    assert_eq!(day.amounts.locked_remainder_pusd, r("3", "50000"));
    for market in read.market_totals() {
        let expected = if market.condition_id.0 == CONDITION_ID {
            r("25001", "50003")
        } else {
            r("25002", "50003")
        };
        assert_eq!(market.rebates[0].paid_pusd, expected);
        let economics = AccountEconomicsReadV1::from_ledger(&read);
        let mut slots = [0.; 18];
        crate::account::encode_rebates(&mut slots, 0, &economics, &[&market.condition_id], 0, true);
        assert_eq!(
            slots[12].to_bits(),
            if market.condition_id.0 == CONDITION_ID {
                0x3ecf9840
            } else {
                0x3ecf99ff
            }
        );
        assert_eq!(&slots[13..15], &[1., 1.]);

        assert_eq!(market.confirmed_net_pnl_pusd, expected);
        assert_eq!(market.rebates[0].confirmed_unpaid_pusd, r("0", "1"));
    }
    let economics = AccountEconomicsReadV1::from_ledger(&read);
    let mut slots = [0.; 18];
    crate::account::encode_rebates(
        &mut slots,
        0,
        &economics,
        &economics
            .markets
            .iter()
            .map(|m| &m.condition_id)
            .collect::<Vec<_>>(),
        0,
        true,
    );
    assert_eq!(
        slots[12].to_bits(),
        0x3f317218,
        "旧集合先精确求和为1再取ln2"
    );
    driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn late_success_updates_old_account_without_reviving_hidden() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_settlement(&root);
    let policy=br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10000},{"state":"CONFIRMED","offset_ms":50000}],"economic_application_ordinal":1},"failure_overrides":[]}"#;
    let (mut execution, observation) = execution(&temp.path().join("sim.jsonl"), &core, policy);
    let mut ledger =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let account = shared
        .ledger()
        .projection_v2()
        .unwrap()
        .account_instance_key;
    let mut native = NativeInputRoot::new("late".into(), account, -10_000);
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "late".into());
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
        native.consume(&mut shared, &mut driver).unwrap().unwrap();
    }
    shared
        .execute(
            &mut driver.decision().unwrap(),
            context("late-buy"),
            Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                order: BuyLegIntentV1 {
                    token_id: TokenId(UP_TOKEN_ID.into()),
                    order: BuyOrderIntentV1::GtcLimit {
                        limit_price_pusd_per_share: Decimal::new(6, 1),
                        size_shares: Decimal::from(5),
                        post_only: false,
                    },
                },
            })),
        )
        .await
        .unwrap()
        .unwrap();
    let mut retires = 0;
    let mut saw_pending_old = false;
    let mut saw_success_old = false;
    while driver.next_group().unwrap().is_some() {
        let Some(group) = native.consume(&mut shared, &mut driver).unwrap() else {
            continue;
        };
        retires += group.post_frame_hidden_ops.len();
        if group.cutoff.decision_time().logical_time_utc_ms() >= 61_003 {
            assert!(group.current.is_none());
            assert_eq!(group.old, vec![ConditionId(CONDITION_ID.into())]);
            assert!(group.rows.is_empty());
            assert!(group.pre_frame_hidden_ops.is_empty());
            let read = shared.ledger().economic_read().unwrap();
            let m = read.market_totals().next().unwrap();
            let quantity = &m.success_shares[m
                .assets
                .as_ref()
                .unwrap()
                .iter()
                .position(|a| a.token_id.0 == UP_TOKEN_ID)
                .unwrap()]
            .0;
            if quantity == "0" {
                saw_pending_old = true;
            } else {
                assert_eq!(quantity, "5");
                saw_success_old = true;
                break;
            }
        }
    }
    assert!(saw_pending_old && saw_success_old);
    assert_eq!(retires, 1);
    driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn old_pairing_is_market_local_before_sum() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_rebate_contributions(&root);
    let (mut execution, observation) =
        execution(&temp.path().join("sim.jsonl"), &core, SUCCESS_POLICY);
    let mut ledger =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "old-pair".into());
    for _ in 0..8 {
        driver.next_group().unwrap().unwrap();
    }
    for (condition, token, id) in [
        (CONDITION_ID, UP_TOKEN_ID, "old-a"),
        (OTHER_CONDITION_ID, "4", "old-b"),
    ] {
        shared
            .execute(
                &mut driver.decision().unwrap(),
                context(id),
                Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                    condition_id: ConditionId(condition.into()),
                    order: BuyLegIntentV1 {
                        token_id: TokenId(token.into()),
                        order: BuyOrderIntentV1::GtcLimit {
                            limit_price_pusd_per_share: Decimal::new(99, 2),
                            size_shares: Decimal::from(5),
                            post_only: false,
                        },
                    },
                })),
            )
            .await
            .unwrap()
            .unwrap();
    }
    while driver.next_group().unwrap().is_some() {
        shared.apply_closed().unwrap();
        if driver
            .decision()
            .unwrap()
            .cutoff()
            .decision_time()
            .logical_time_utc_ms()
            >= 89_111_000
        {
            break;
        }
    }
    assert!(shared.apply_closed().unwrap());
    let read = AccountEconomicsReadV1::from_ledger(&shared.ledger().economic_read().unwrap());
    assert_eq!(read.markets.len(), 2);
    for m in &read.markets {
        let side = if m.condition_id.0 == CONDITION_ID {
            0
        } else {
            1
        };
        let index = m
            .assets
            .as_ref()
            .unwrap()
            .iter()
            .position(|a| a.outcome.index == side)
            .unwrap();
        assert_eq!(m.success_shares[index].0, "5");
        assert_eq!(m.success_shares[1 - index].0, "0");
    }
    let ids = read
        .markets
        .iter()
        .map(|m| &m.condition_id)
        .collect::<Vec<_>>();
    let mut slots = [0.; 3];
    crate::values::slot(&mut slots, 0, crate::account::paired_shares(&read, &ids));
    assert_eq!(slots, [0., 1., 1.], "跨市场A5/B5不能变成pair5");
    driver.stop_and_drain().unwrap();
}

fn exported_group(group: &NativeInputGroup) -> serde_json::Value {
    use crate::prefix::*;
    use num_bigint::BigUint;
    let mut output = serde_json::to_value(group).unwrap();
    for (row, json) in group
        .rows
        .iter()
        .zip(output["rows"].as_array_mut().unwrap())
    {
        assert_eq!(row.fast.len(), 836);
        assert_eq!(&row.fast[806..], &row.rule);
        let Some(action_context) = row.action_context.as_ref() else {
            assert_eq!(row.role, InputRole::ContextOnly);
            continue;
        };
        let mut prefixes = vec![
            action_context
                .prefix_features(&PrefixState::default())
                .to_vec(),
        ];
        // 固定 LIMIT 属性前缀，只验证编码，不作为完整 BUY 支持或费用求解器。
        let mut state = PrefixState {
            family: Some(Family::SubmitSingleBuy),
            stage: Stage::QuantityBranch,
            single_leg: Some(Leg::B),
            interval: Some(QuantityInterval {
                upper: BigUint::from(1000u32),
                lo: BigUint::from(500u32),
                hi: BigUint::from(1000u32),
            }),
            ..Default::default()
        };
        let interval = action_context.prefix_features(&state);
        assert_eq!(interval[16], 0.);
        assert_eq!(interval[18], 0.5);
        prefixes.push(interval.to_vec());
        state.select_quantity(BigUint::from(500u32));
        state.stage = Stage::Variant(Leg::B);
        let leaf = action_context.prefix_features(&state);
        assert_eq!(&leaf[17..21], &[0.; 4]);
        assert_eq!(leaf[23], 1.);
        assert_eq!(&leaf[24..42], &[0.; 18]);
        prefixes.push(leaf.to_vec());
        state.legs[1].variant = Some(Variant::GtcLimit);
        state.stage = Stage::PostOnly(Leg::B);
        prefixes.push(action_context.prefix_features(&state).to_vec());
        state.legs[1].post_only = Some(false);
        state.stage = Stage::Price(Leg::B);
        let selected = action_context.prefix_features(&state);
        assert_eq!(selected[42], 1.);
        assert_eq!(selected[48], 1.);
        assert_eq!(selected[51], 1.);
        prefixes.push(selected.to_vec());
        // 精确已选0.5由同帧tick确定；仅用于投影核对，不制造完成后图调用。
        state.legs[1].price_index = Some(BigUint::from(50u32));
        let completed = action_context.prefix_features(&state);
        assert_eq!(completed[57], 0.);
        assert_eq!(completed[59], 1.);
        json["fixture_prefixes"] = serde_json::to_value(prefixes).unwrap();
    }
    output
}
