use super::*;
#[path = "../../../shared-run/tests/common/fixture_clock.rs"]
mod fixture_clock;
#[path = "../../../shared-run/tests/common/fixture_execution.rs"]
mod fixture_execution;
#[path = "../../../shared-run/tests/common/fixture_input.rs"]
mod fixture_input;
#[path = "../../../shared-run/tests/common/fixture_policy.rs"]
mod fixture_policy;
#[path = "../../../shared-run/tests/common/fixture_recording.rs"]
mod fixture_recording;
#[path = "repair_fixture.rs"]
mod fixture_repair;
#[path = "../../../shared-run/tests/common/fixture_terminal.rs"]
mod fixture_terminal;
mod common {
    pub use super::fixture_input::*;
    pub use super::fixture_policy::*;
    pub use super::fixture_repair::*;
}

use common::*;
use sim_system::{execution::*, public_consumption::*};

fn reserved(id: &str, token: &str, principal: &str, worst: Decimal) -> ReservedIntentV1 {
    ReservedIntentV1 {
        context: RequestContextV2 {
            global_request_id: id.into(),
            caller_correlation_id: None,
        },
        intent: Arc::new(ModelIntentV1::SubmitSingleBuy {
            condition_id: ConditionId(CONDITION_ID.into()),
            order: BuyLegIntentV1 {
                token_id: TokenId(token.into()),
                order: BuyOrderIntentV1::FakMarket {
                    spend_pusd: principal.parse().unwrap(),
                    worst_price_pusd_per_share: worst,
                },
            },
        }),
        reserves: vec![IntentReserveV1 {
            batch_item_ordinal: 0,
            reserve: ReserveIntentV1 {
                request_identity: LedgerRequestIdentityV1(format!("{id}-reserve")),
                condition_id: ConditionId(CONDITION_ID.into()),
                requested_principal_pusd: ExactDecimal(principal.into()),
                immediate_fee_reserve_pusd: ExactDecimal("0.1".into()),
            },
        }],
    }
}
#[tokio::test(flavor = "current_thread")]
async fn mined_partial_success_and_compensation_keep_principal_support_and_charge_fees_once() {
    use contracts::execution_normalization::SourceNeutralTradeEconomicDispositionV1 as D;
    for (fee_rate, expected_fee) in [
        ("0.452898550724637681", "1"),
        ("0.022644927536231884", "0.05"),
    ] {
        for fails in [false, true] {
            let temp = if let Some(parent) = std::env::var_os("ISSUE360_FORENSIC_ROOT") {
                tempfile::Builder::new()
                    .prefix("fee-owner-")
                    .tempdir_in(parent)
                    .unwrap()
            } else {
                tempfile::tempdir().unwrap()
            };
            let root = temp.path().to_owned();
            let _temp = if std::env::var_os("ISSUE360_FORENSIC_ROOT").is_some() {
                temp.keep();
                None
            } else {
                Some(temp)
            };
            println!("fee_material_root {}", root.display());
            let dpt = root.join("dpt");
            let (core, times) = recorded_repair_lifecycle(&dpt);
            let mut policy: serde_json::Value = serde_json::from_slice(SUCCESS_POLICY).unwrap();
            policy["normal_path"]["steps"][2]["offset_ms"] = serde_json::json!(1000);
            if fails {
                policy["failure_overrides"] = serde_json::json!([{"selector":{"selector_domain":"FILL","account_instance_key":{"run_id":"shared-combination","caller_role":SimCallerRoleV1::Paper,"caller_instance_id":"shared-caller","account_id":"shared-account","platform_source":PlatformSourceV1::Simulated},"fill_sequence":3},"path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10},{"state":"FAILED","offset_ms":2000}],"economic_application_ordinal":1}}]);
            }
            std::fs::write(
                root.join("finality.json"),
                serde_json::to_vec_pretty(&policy).unwrap(),
            )
            .unwrap();
            std::fs::write(root.join("fee-config.json"), serde_json::to_vec_pretty(&serde_json::json!({"fee_rate":fee_rate,"expected_fee":expected_fee,"fails":fails})).unwrap()).unwrap();
            let (mut execution, observation, _) = execution_with_fee_policy_v4(
                &root.join("sim.jsonl"),
                &core,
                &serde_json::to_vec(&policy).unwrap(),
                fee_rate,
            );
            let mut ledger = LedgerWriterV1::new_with_admission_v3(
                AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
            );
            execution
                .bind_ledger_recording_v2(&mut ledger, &root.join("ledger"))
                .unwrap();
            let mut shared = SharedExecutionV2::from_opening(ledger, observation)
                .unwrap()
                .unwrap();
            let source = RecordedPublicGroupSourceV1::open(&dpt, 1, 1 << 20, times).unwrap();
            let mut driver =
                FinitePublicConsumptionV1::new(execution, source, "profit-lifecycle".into());
            for _ in 0..2 {
                driver.next_group().unwrap().unwrap();
            }
            shared
                .execute_batch_reserved(
                    &mut driver.decision().unwrap(),
                    &[reserved("old", UP_TOKEN_ID, "5", Decimal::new(5, 1))],
                )
                .await
                .unwrap();
            loop {
                driver.next_group().unwrap().unwrap();
                shared.apply_closed().unwrap();
                if shared
                    .ledger()
                    .economic_read()
                    .unwrap()
                    .trades()
                    .any(|t| t.success_applied)
                {
                    break;
                }
            }
            let initial = shared.strategy_input(&mut driver).unwrap().unwrap();
            let before_p = initial.action_before.profit_and_equity().0;
            let mut a = ProfitAllocationV2 {
                global_request_id: "repair".into(),
                reserve_identity: LedgerRequestIdentityV1("repair-reserve".into()),
                condition_id: ConditionId(CONDITION_ID.into()),
                token_id: TokenId(DOWN_TOKEN_ID.into()),
                estimated_principal_pusd: r(Decimal::new(66, 1)).public(),
                estimated_fee_pusd: r(Decimal::new(1, 1)).public(),
                estimated_shares: r(Decimal::from(10)).public(),
                original_orphan_shares: r(Decimal::from(10)).public(),
                original_orphan_principal_per_share: r(Decimal::new(5, 1)).public(),
                estimate_cutoff: initial.action_before.cutoff().clone(),
                allocated_profit_pusd: r(Decimal::new(17, 1)).public(),
            };
            shared
                .execute_batch_reserved(
                    &mut driver.decision().unwrap(),
                    &[reserved(
                        "repair",
                        DOWN_TOKEN_ID,
                        "6.6",
                        Decimal::new(72, 2),
                    )],
                )
                .await
                .unwrap();
            let mut saw_before = false;
            let mut saw_mined = false;
            let mut saw_partial = false;
            let mut saw_end = false;
            loop {
                driver.stabilize_recording().unwrap();
                let input = shared
                    .strategy_input_with_trade_details(&mut driver, false)
                    .unwrap()
                    .unwrap();
                assert!(input.trade_economics.is_empty(), "不得开启全历史投影");
                let read = &input.action_before;
                let ExecutionReadValueV1::Known(trades) = read.request_trade_economics("repair")
                else {
                    panic!("原成交应可读")
                };
                let lifecycle = shared.ledger().economic_read().unwrap();
                let actual: Vec<_> = lifecycle
                    .trades()
                    .filter(|t| t.fact.token_id.0 == DOWN_TOKEN_ID)
                    .collect();
                let active: Vec<_> = actual
                    .iter()
                    .filter(|t| {
                        t.economic_applied
                            && !t.success_applied
                            && !matches!(t.fact.economic_disposition, D::FinalFailureCompensated)
                    })
                    .collect();
                let successful: Vec<_> = actual.iter().filter(|t| t.success_applied).collect();
                let occupied = ComplementaryPairV1::profit_occupancy_read(read, &a)
                    .unwrap()
                    .unwrap_or_else(R::zero);
                if actual.is_empty() {
                    assert_eq!(occupied, r(Decimal::new(17, 1)));
                    saw_before = true;
                } else if active.len() == 2 {
                    assert_eq!(
                        occupied,
                        r(Decimal::new(16, 1)),
                        "MINED不得按零本金预留释放"
                    );
                    assert_eq!(
                        read.profit_and_equity().0,
                        before_p.sub(&r(expected_fee.parse().unwrap()))
                    );
                    let inventory =
                        crate::strategies::common::remaining_inventory_estimate(read, &a)
                            .unwrap()
                            .unwrap();
                    assert_eq!(
                        inventory.loss,
                        r(Decimal::new(16, 1)),
                        "D-0378当前平均池未决损失不再次加入已在P中的原H费用或本腿MINED费用"
                    );
                    assert!(!inventory.unknown);
                    println!(
                        "inventory_fee_read {}",
                        serde_json::json!({"fee_rate":fee_rate,"fails":fails,
                        "p":read.profit_and_equity().0.public(),"loss":inventory.loss.public(),"original_h_fee":format!("{:?}",read.market_economics(&a.condition_id).unwrap().orphan_fee)})
                    );
                    let remaining = ComplementaryPairV1::remaining_estimate(read, &a)
                        .unwrap()
                        .unwrap();
                    assert_eq!(remaining.principal, r(Decimal::new(66, 1)));
                    assert_eq!(remaining.shares, r(Decimal::from(10)));
                    assert_eq!(
                        read.market_economics(&a.condition_id)
                            .unwrap()
                            .successful_investment,
                        initial
                            .action_before
                            .market_economics(&a.condition_id)
                            .unwrap()
                            .successful_investment,
                        "MINED尚未计入成功投入，不能重复计成功池"
                    );
                    a.allocated_profit_pusd = r(Decimal::new(5, 1)).public();
                    assert_eq!(
                        ComplementaryPairV1::profit_occupancy_read(read, &a).unwrap(),
                        Some(r(Decimal::new(5, 1)))
                    );
                    a.allocated_profit_pusd = r(Decimal::new(17, 1)).public();
                    saw_mined = true;
                } else if fails && active.len() == 1 && successful.len() == 1 {
                    assert_eq!(trades.len(), 2, "部分成功时仍保留同请求的两笔原成交");
                    let confirmed = trades
                        .iter()
                        .filter(|t| t.success_applied)
                        .fold(Decimal::ZERO, |sum, t| {
                            sum + decimal(&t.fact.base_quantity_shares).unwrap()
                        });
                    assert_eq!(confirmed, Decimal::from(5));
                    assert_eq!(
                        read.profit_and_equity().0,
                        before_p
                            .sub(&r(expected_fee.parse().unwrap()))
                            .sub(&r(Decimal::new(5, 1))),
                        "首笔5份@.6本金配平亏损.5只进入P一次"
                    );
                    assert_eq!(
                        occupied,
                        r(Decimal::new(11, 1)),
                        "剩余5份@.72：2.5+3.6-5=1.1"
                    );
                    saw_partial = true;
                } else if actual.len() == 2
                    && active.is_empty()
                    && (successful.len() == 2
                        || (fails
                            && successful.len() == 1
                            && actual.iter().any(|t| {
                                t.fact.economic_disposition == D::FinalFailureCompensated
                            })))
                {
                    assert_eq!(occupied, R::zero());
                    assert!(trades.is_empty(), "原请求全部终局后不再冻结已闭合成交");
                    let necessary = shared.ledger().necessary_read_v2().unwrap();
                    for t in &actual {
                        assert!(
                            !necessary
                                .unclosed_trade_identities()
                                .any(|id| id == t.trade_identity)
                        );
                        if t.fact.economic_disposition == D::FinalFailureCompensated {
                            let terminal = necessary.trade_economics(t.trade_identity).unwrap();
                            assert!(!terminal.economic_applied);
                            assert!(!terminal.success_applied);
                            assert_eq!(
                                terminal.actual_net_fee_pusd,
                                ExactDecimal("0".into()),
                                "失败补偿恢复本笔原净费，不复刻现金补偿矩阵"
                            );
                        }
                    }
                    saw_end = true;
                }
                // 拥有式和借用式沿同一原身份集合，包含多个fill且不重计状态更新。
                let borrowed = shared
                    .strategy_borrowed_observation_input(&mut driver, false)
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    ComplementaryPairV1::profit_occupancy_read(&borrowed.action_before, &a)
                        .unwrap(),
                    ComplementaryPairV1::profit_occupancy_read(read, &a).unwrap()
                );
                if driver.next_group().unwrap().is_none() {
                    break;
                }
            }
            assert!(
                saw_before && saw_mined && saw_end && (!fails || saw_partial),
                "before={saw_before} mined={saw_mined} partial={saw_partial} end={saw_end}"
            );
            driver.stop_and_drain().unwrap();
        }
    }
}

/// 原continuous乱序合同：只重排真实SIM/NOE引用，不改来源字段或经济事实。
#[tokio::test(flavor = "current_thread")]
async fn terminal_order_keeps_successful_sibling_until_last_economics_closes() {
    use ::execution_normalization::{
        NoeConsumedRecordRef, NormalizationRecord as N, SourceNeutralEvidenceKind as E,
    };
    use contracts::execution_normalization::{
        CancellationRequestCompletedV1, SourceNeutralAccountTransitionKindV1 as T1,
        SourceNeutralEconomicDispositionV1 as D1, SourceNeutralTradeEconomicDispositionV1 as D,
    };
    let temp = tempfile::tempdir().unwrap();
    let dpt = temp.path().join("dpt");
    let (core, times) =
        recorded_repair_lifecycle_prices(&dpt, "0.9", [("0.19", "8"), ("0.21", "8")], true);
    let (mut execution, observation, _) = execution_with_fee_policy_v4(
        &temp.path().join("sim.jsonl"),
        &core,
        SUCCESS_POLICY,
        "0.07",
    );
    let opening = observation.opening_record().unwrap();
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut producer = SharedExecutionV2::from_opening(ledger, observation.clone())
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&dpt, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "sibling-lifecycle".into());
    loop {
        driver.next_group().unwrap().unwrap();
        if driver
            .observation_cutoff()
            .unwrap()
            .decision_time()
            .logical_time_utc_ms()
            >= 22_001
        {
            break;
        }
    }
    let mut records = Vec::new();
    eprintln!("sibling original context: {:#?}", opening.context());
    for (id, token, principal, worst) in [
        ("old", UP_TOKEN_ID, "9", Decimal::new(9, 1)),
        ("repair", DOWN_TOKEN_ID, "3.2", Decimal::new(32, 2)),
    ] {
        let sent = driver
            .observation_cutoff()
            .unwrap()
            .decision_time()
            .logical_time_utc_ms();
        let target = sent + 59_665;
        let request = reserved(id, token, principal, worst);
        producer
            .execute(
                &mut driver.decision().unwrap(),
                request.context,
                Some(request.intent),
            )
            .await
            .unwrap()
            .unwrap();
        loop {
            driver.next_group().unwrap().unwrap();
            if driver
                .observation_cutoff()
                .unwrap()
                .decision_time()
                .logical_time_utc_ms()
                >= target
            {
                break;
            }
        }
        let observed = driver
            .observation_cutoff()
            .unwrap()
            .decision_time()
            .logical_time_utc_ms();
        assert_eq!(sent, if id == "old" { 22_001 } else { 81_666 });
        assert_eq!(observed, target);
        assert!(observed < 149_331, "完整反馈观察仍在市场窗口内");
        eprintln!("sibling {id}: sent={sent}, feedback_target={target}, observed={observed}");
        let received = observation.take_closed_records();
        for record in &received {
            eprintln!("sibling original record: {:#?}", record.record());
        }
        records.extend(received);
        if id == "old" {
            assert!(records.iter().any(|r| matches!(r.record(), N::Evidence {
                evidence: E::TradeLifecycleV3 { result }, ..
            } if result.result.token_id.0 == UP_TOKEN_ID && result.result.economic_disposition == D::FinalSuccess)), "旧腿终局实到后才发送repair");
        }
    }
    let mut registry = producer.registry().clone();
    let second = records
        .iter()
        .find_map(|record| match record.record() {
            N::Evidence {
                evidence: E::TradeLifecycleV3 { result },
                ..
            } if result.result.token_id.0 == DOWN_TOKEN_ID
                && result.result.source_price_pusd_per_share.0 == "0.21"
                && result.result.economic_disposition == D::FinalSuccess =>
            {
                Some(result.result.trade_identity.clone())
            }
            _ => None,
        })
        .expect("真实第二档终局成交");
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    ledger.apply_noe_memory_v2(&opening).unwrap();
    let mut delayed = Vec::new();
    let mut waiting = Vec::new();
    for record in &records {
        registry.observe(record);
        if matches!(record.record(), N::EconomicFact { fact, .. }
            if matches!(&fact.disposition, D1::AccountTransition {
                transition: T1::FillFinalityEconomicApplied { trade_identity, .. }, ..
            } if trade_identity == &second))
        {
            delayed.push(record.clone());
        } else {
            waiting.push(record.clone());
        }
    }
    assert_eq!(delayed.len(), 1, "只延迟第二笔原经济引用");
    let apply_ready =
        |ledger: &mut LedgerWriterV1,
         mut waiting: Vec<NoeConsumedRecordRef<CancellationRequestCompletedV1>>| {
            loop {
                let previous = waiting.len();
                waiting.retain(|record| {
                    matches!(
                        ledger.apply_noe_memory_v2(record).unwrap(),
                        LedgerMemoryApplyOutcomeV1::Applied(
                            LedgerApplyOutcomeV1::MissingPlatformPredecessor { .. }
                        ) | LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation
                    )
                });
                if waiting.len() == previous || waiting.is_empty() {
                    return waiting;
                }
            }
        };
    waiting = apply_ready(&mut ledger, waiting);
    let a = ProfitAllocationV2 {
        global_request_id: "repair".into(),
        reserve_identity: LedgerRequestIdentityV1("repair-reserve".into()),
        condition_id: ConditionId(CONDITION_ID.into()),
        token_id: TokenId(DOWN_TOKEN_ID.into()),
        estimated_principal_pusd: r(Decimal::new(32, 1)).public(),
        estimated_fee_pusd: r(Decimal::new(15232, 5)).public(),
        estimated_shares: r(Decimal::from(10)).public(),
        original_orphan_shares: r(Decimal::from(10)).public(),
        original_orphan_principal_per_share: r(Decimal::new(9, 1)).public(),
        estimate_cutoff: driver.observation_cutoff().unwrap(),
        allocated_profit_pusd: r(Decimal::new(22, 1)).public(),
    };
    // 复用原legacy checkpoint边界交接实际owner状态和待前驱引用；不加测试专用生产入口。
    #[derive(serde::Serialize)]
    struct Saved<'a> {
        registry: &'a SharedExecutionRegistryV2,
        pending: &'a [NoeConsumedRecordRef<CancellationRequestCompletedV1>],
    }
    let mut check = |ledger: &LedgerWriterV1,
                     pending: &[NoeConsumedRecordRef<CancellationRequestCompletedV1>],
                     closed: bool| {
        let mut ledger_bytes = Vec::new();
        ledger.write_checkpoint(&mut ledger_bytes).unwrap();
        let mut shared_bytes = Vec::new();
        ciborium::ser::into_writer(
            &Saved {
                registry: &registry,
                pending,
            },
            &mut shared_bytes,
        )
        .unwrap();
        let restored = LedgerWriterV1::restore_checkpoint(ledger_bytes.as_slice()).unwrap();
        let mut shared = SharedExecutionV2::restore_checkpoint(
            restored,
            observation.clone(),
            shared_bytes.as_slice(),
        )
        .unwrap();
        let input = shared
            .strategy_observation_input_with_trade_details(&mut driver, false)
            .unwrap()
            .unwrap();
        assert!(input.trade_economics.is_empty());
        let read = &input.action_before;
        assert_eq!(
            read.orders().count(),
            0,
            "两笔FinalSuccess均可见，订单已退出活动集合"
        );
        let ExecutionReadValueV1::Known(trades) = read.request_trade_economics("repair") else {
            panic!("原owner成交应可读")
        };
        if !closed {
            assert_eq!(trades.len(), 2, "保留同一终局订单已成功的兄弟成交");
            let succeeded: Decimal = trades
                .iter()
                .filter(|t| t.success_applied)
                .map(|t| decimal(&t.fact.base_quantity_shares).unwrap())
                .sum();
            assert_eq!(succeeded, Decimal::from(8));
            let late = trades.iter().find(|t| t.trade_identity == &second).unwrap();
            assert_eq!(late.fact.economic_disposition, D::FinalSuccess);
            assert!(!late.economic_applied && !late.success_applied);
            let first = trades.iter().find(|t| t.success_applied).unwrap();
            assert_eq!(
                decimal(&first.fact.immediate_taker_fee_pusd).unwrap(),
                Decimal::new(8619, 5)
            );
            assert_eq!(
                decimal(&late.fact.immediate_taker_fee_pusd).unwrap(),
                Decimal::new(9290, 5)
            );
            let remaining = ComplementaryPairV1::remaining_estimate(read, &a)
                .unwrap()
                .unwrap();
            assert_eq!(
                remaining.fee,
                r(Decimal::new(9290, 5)),
                "未应用实际费仍在未决估计中"
            );
            assert_eq!(
                remaining.loss.sub(&remaining.fee),
                r(Decimal::new(22, 2)),
                "本金分项只剩2份"
            );
            let economics = shared.ledger().economic_read().unwrap();
            let old_trade = economics
                .trades()
                .find(|t| t.fact.token_id.0 == UP_TOKEN_ID)
                .unwrap();
            assert_eq!(
                decimal(&old_trade.actual_net_fee_pusd).unwrap(),
                Decimal::new(63, 3)
            );
            assert_eq!(
                read.profit_and_equity().0,
                r(Decimal::new(-86919, 5)),
                "初始P为0：旧费.063、首笔费.08619和本金配平亏损.72只计一次"
            );
            assert_eq!(
                ComplementaryPairV1::profit_occupancy_read(read, &a).unwrap(),
                Some(r(Decimal::new(31290, 5))),
                "仅余2份本金损失.22加未应用费.09290，不按8份占用.97290"
            );
        } else {
            assert!(trades.is_empty());
            assert_eq!(
                shared
                    .ledger()
                    .necessary_read_v2()
                    .unwrap()
                    .unclosed_trade_identities()
                    .count(),
                0
            );
            assert_eq!(read.profit_and_equity().0, r(Decimal::new(-118209, 5)));
            assert_eq!(
                ComplementaryPairV1::profit_occupancy_read(read, &a).unwrap(),
                None
            );
        }
        let borrowed = shared
            .strategy_borrowed_observation_input(&mut driver, false)
            .unwrap()
            .unwrap();
        assert!(borrowed.trade_economics.is_empty());
        assert_eq!(
            ComplementaryPairV1::profit_occupancy_read(&borrowed.action_before, &a).unwrap(),
            ComplementaryPairV1::profit_occupancy_read(read, &a).unwrap()
        );
        let ExecutionReadValueV1::Known(borrowed_trades) =
            borrowed.action_before.request_trade_economics("repair")
        else {
            panic!("借用读取应可用")
        };
        assert_eq!(borrowed_trades.len(), trades.len());
    };
    check(&ledger, &waiting, false);
    waiting.extend(delayed);
    waiting = apply_ready(&mut ledger, waiting);
    assert!(waiting.is_empty(), "原经济引用补齐后全部应用");
    check(&ledger, &waiting, true);
    // 原有限来源仍按正式终局走完；没有提前break掩盖输入不完整。
    while driver.next_group().unwrap().is_some() {}
    driver.stop_and_drain().unwrap();
}
