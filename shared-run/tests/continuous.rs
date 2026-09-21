mod common;
use common::*;
use contracts::*;
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
#[tokio::test(flavor = "current_thread")]
async fn real_group_bridge_registry_ledger_freeze_and_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_memory_complete(&root);
    let (mut execution, observation) =
        execution(&temp.path().join("sim.jsonl"), &core, SUCCESS_POLICY);
    let opening = observation.opening_record().unwrap();
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    let ledger_root = temp.path().join("ledger");
    execution
        .bind_ledger_recording_v2(&mut ledger, &ledger_root)
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation.clone())
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "shared".into());
    driver.next_group().unwrap().unwrap();
    driver.next_group().unwrap().unwrap();
    let before = shared.action_before(&mut driver).unwrap().unwrap();
    assert_eq!(
        before.ledger().platform_confirmed_projection.cash_pusd.0,
        "100"
    );
    assert_eq!(before.ledger().available_through_utc_ms, None);
    let intent = Arc::new(ModelIntentV1::SubmitPairedBuy {
        condition_id: ConditionId(CONDITION_ID.into()),
        orders: [leg(UP_TOKEN_ID, 5), leg(DOWN_TOKEN_ID, 5)],
    });
    shared
        .execute(
            &mut driver.decision().unwrap(),
            context("paired"),
            Some(intent.clone()),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        shared.registry().orders().count(),
        0,
        "登记回执不能伪造平台Accepted"
    );
    assert_eq!(
        shared
            .registry()
            .commitments(
                &ConditionId(CONDITION_ID.into()),
                &TokenId(UP_TOKEN_ID.into())
            )
            .count(),
        1
    );
    assert!(
        shared
            .execute(&mut driver.decision().unwrap(), context("no-trade"), None)
            .await
            .unwrap()
            .is_none()
    );
    for _ in 2..=6 {
        driver.next_group().unwrap().unwrap();
    }
    let snapshot = shared.action_before(&mut driver).unwrap().unwrap();
    assert!(snapshot.cutoff().private_feedback_sequence() > 0);
    let orders: Vec<_> = snapshot.orders().collect();
    assert_eq!(orders.len(), 2);
    for order in &orders {
        assert!(order.accepted.is_some());
        assert!(order.formed.is_some());
        assert!(std::ptr::eq(
            order.origin.as_ref().unwrap().intent.intent(),
            intent.as_ref()
        ));
    }
    let mut incomplete = SharedExecutionRegistryV1::new(Arc::new(opening.context().clone()));
    incomplete.observe(orders[0].formed.unwrap());
    assert!(
        matches!(incomplete.cancel_objects(), ExecutionReadValueV1::Known(objects)
            if objects.len() == 1 && matches!(&objects[0], CancelObjectReadV1::Single { order }
                if order.cancellable == ExecutionReadValueV1::Unavailable)),
        "真实ID可寻址，数量未知保留为Unknown Single"
    );
    let ExecutionReadValueV1::Known(objects) = snapshot.cancel_objects() else {
        panic!("完整形成序可排序")
    };
    assert_eq!(objects.len(), 4, "两Single、一原ordinal Paired、完整Market");
    assert_eq!(
        snapshot
            .order_funds()
            .map(|r| r.principal_encumbrance_pusd)
            .collect::<Vec<_>>()
            .len(),
        2
    );
    assert_eq!(
        before
            .ledger()
            .platform_confirmed_projection
            .principal_encumbrance_pusd
            .0,
        "0",
        "动作后资金不可倒灌冻结组"
    );
    shared
        .execute(
            &mut driver.decision().unwrap(),
            context("market-cancel"),
            Some(Arc::new(ModelIntentV1::CancelMarketOrders {
                condition_id: ConditionId(CONDITION_ID.into()),
            })),
        )
        .await
        .unwrap();
    while driver.next_group().unwrap().is_some() {
        shared.action_before(&mut driver).unwrap();
    }
    assert!(shared.apply_closed().unwrap());
    let expected = shared.ledger().projection_v2().unwrap();
    driver.complete(&mut Materials { root }).unwrap();
    let mut recovered = LedgerWriterV1::open_durable(&ledger_root).unwrap();
    assert_eq!(recovered.projection_v2().unwrap(), expected);
    assert!(matches!(
        recovered.apply_noe_memory_v1(&opening).unwrap(),
        LedgerMemoryApplyOutcomeV1::Applied(LedgerApplyOutcomeV1::AlreadyApplied { .. })
    ));
}
#[tokio::test(flavor = "current_thread")]
async fn real_initial_taker_and_later_maker_keep_base_and_pending_commitment() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_trading(&root, "2", true);
    let policy=br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10000},{"state":"CONFIRMED","offset_ms":15000}],"economic_application_ordinal":1},"failure_overrides":[]}"#;
    let (mut execution, observation) = execution(&temp.path().join("sim.jsonl"), &core, policy);
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "maker".into());
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    shared
        .execute(
            &mut driver.decision().unwrap(),
            context("gtc10"),
            Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                order: BuyLegIntentV1 {
                    token_id: TokenId(UP_TOKEN_ID.into()),
                    order: BuyOrderIntentV1::GtcLimit {
                        limit_price_pusd_per_share: Decimal::new(6, 1),
                        size_shares: Decimal::from(10),
                        post_only: false,
                    },
                },
            })),
        )
        .await
        .unwrap();
    let mut seen = false;
    let mut saved = Vec::new();
    let mut order_id = None;
    while driver.next_group().unwrap().is_some() {
        let Some(view) = shared.action_before(&mut driver).unwrap() else {
            continue;
        };
        let at = view.cutoff().decision_time().logical_time_utc_ms();
        let orders = view.orders().collect::<Vec<_>>();
        if at == 36_000 {
            assert_eq!(orders.len(), 1);
            assert_eq!(
                orders[0].matched_shares,
                ExecutionReadValueV1::Known(ExactDecimal("5".into()))
            );
            assert_eq!(
                orders[0].active_unfilled_shares,
                ExecutionReadValueV1::Known(ExactDecimal("5".into()))
            );
            assert_eq!(
                shared.registry().committed_target_shares(
                    &ConditionId(CONDITION_ID.into()),
                    &TokenId(UP_TOKEN_ID.into())
                ),
                ExecutionReadValueV1::Known(ExactDecimal("10".into()))
            );
            seen = true;
            order_id = Some(orders[0].order_identity.clone());
            saved.push(orders[0].accepted.unwrap().clone());
            saved.push(orders[0].formed.unwrap().clone());
            saved.extend(
                shared
                    .registry()
                    .order_trades(orders[0].order_identity)
                    .cloned(),
            );
            shared
                .execute(
                    &mut driver.decision().unwrap(),
                    context("close-maker"),
                    Some(Arc::new(ModelIntentV1::CancelOrder {
                        platform_order_id: platform_id(orders[0].order_identity),
                    })),
                )
                .await
                .unwrap();
        }
        if at == 56_000 {
            let id = order_id.as_ref().unwrap();
            let final_order = shared.registry().order(id).unwrap();
            let mut late = SharedExecutionRegistryV1::new(Arc::new(saved[0].context().clone()));
            late.observe(final_order.latest_lifecycle.unwrap());
            for r in saved.iter().rev() {
                late.observe(r);
            }
            for r in shared.registry().order_trades(id) {
                late.observe(r);
                late.observe(r);
            }
            let read = late.order(id).unwrap();
            assert_eq!(
                read.matched_shares,
                ExecutionReadValueV1::Known(ExactDecimal("5".into()))
            );
            assert_eq!(
                read.active_unfilled_shares,
                ExecutionReadValueV1::Known(ExactDecimal("0".into()))
            );
            assert_eq!(read.cancellable, ExecutionReadValueV1::Known(false));
        }
    }
    assert!(seen);
}
fn delayed_policy(outcome: &str) -> Vec<u8> {
    let normal = serde_json::json!({"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10000},{"state":"CONFIRMED","offset_ms":15000}],"economic_application_ordinal":1});
    let overrides = if outcome == "success" {
        vec![]
    } else {
        let path = if outcome == "unapplied" {
            serde_json::json!({"steps":[{"state":"MATCHED","offset_ms":0},{"state":"FAILED","offset_ms":15000}],"economic_application_ordinal":null})
        } else {
            serde_json::json!({"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10000},{"state":"FAILED","offset_ms":15000}],"economic_application_ordinal":1})
        };
        vec![
            serde_json::json!({"selector":{"selector_domain":"REQUEST","account_instance_key":AccountInstanceKeyV1{run_id:"shared-combination".into(),caller_role:SimCallerRoleV1::Paper,caller_instance_id:"shared-caller".into(),account_id:"shared-account".into(),platform_source:PlatformSourceV1::Simulated},"global_request_id":"pending4"},"path":path}),
        ]
    };
    serde_json::to_vec(&serde_json::json!({"schema_version":"trade_finality_policy_v2","normal_path":normal,"failure_overrides":overrides})).unwrap()
}
#[tokio::test(flavor = "current_thread")]
async fn actual_pending_four_keeps_ten_then_finality_and_cancel_close_separate_responsibility() {
    for outcome in ["success", "unapplied", "compensated"] {
        for cancel in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("dpt");
            let (core, times) = recorded_trading(&root, "4", false);
            let policy = delayed_policy(outcome);
            let (mut execution, observation) =
                execution(&temp.path().join("sim.jsonl"), &core, &policy);
            let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
                per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
            });
            execution
                .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
                .unwrap();
            let mut shared = SharedExecutionV1::from_opening(ledger, observation)
                .unwrap()
                .unwrap();
            let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
            let mut driver = FinitePublicConsumptionV1::new(execution, source, "pending".into());
            for _ in 0..4 {
                driver.next_group().unwrap().unwrap();
            }
            shared
                .execute(
                    &mut driver.decision().unwrap(),
                    context("pending4"),
                    Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                        condition_id: ConditionId(CONDITION_ID.into()),
                        order: BuyLegIntentV1 {
                            token_id: TokenId(UP_TOKEN_ID.into()),
                            order: BuyOrderIntentV1::GtcLimit {
                                limit_price_pusd_per_share: Decimal::new(6, 1),
                                size_shares: Decimal::from(10),
                                post_only: false,
                            },
                        },
                    })),
                )
                .await
                .unwrap();
            let mut observed = Vec::new();
            while driver.next_group().unwrap().is_some() {
                let Some(view) = shared.action_before(&mut driver).unwrap() else {
                    continue;
                };
                let at = view.cutoff().decision_time().logical_time_utc_ms();
                let commitment = shared.registry().committed_target_shares(
                    &ConditionId(CONDITION_ID.into()),
                    &TokenId(UP_TOKEN_ID.into()),
                );
                if at == 28_000 {
                    assert_eq!(
                        commitment,
                        ExecutionReadValueV1::Known(ExactDecimal("10".into()))
                    );
                    let order = view.orders().next().unwrap();
                    let formed = order.formed.unwrap();
                    let mut partial =
                        SharedExecutionRegistryV1::new(Arc::new(formed.context().clone()));
                    partial.observe(formed);
                    assert!(matches!(formed.record(),
                        ::execution_normalization::NormalizationRecord::Evidence {
                            evidence: ::execution_normalization::SourceNeutralEvidenceKind::OrderLifecycle {
                                result: contracts::execution_normalization::SourceNeutralOrderLifecycleResultV1 {
                                    event: contracts::execution_normalization::SourceNeutralOrderLifecycleEventV1::Accepted {
                                        state: contracts::execution_normalization::SourceNeutralOrderStateV1::Open,
                                    }, ..
                                }
                            }, ..
                        }
                    ));
                    let read = partial.order(order.order_identity).unwrap();
                    assert_eq!(read.cancellable, ExecutionReadValueV1::Unavailable);
                    assert_eq!(
                        read.active_unfilled_shares,
                        ExecutionReadValueV1::Unavailable
                    );
                    assert!(matches!(
                        partial.cancel_objects(),
                        ExecutionReadValueV1::Known(objects) if objects.len() == 1 && matches!(&objects[0], CancelObjectReadV1::Single { order } if order.cancellable == ExecutionReadValueV1::Unavailable)
                    ));
                    if cancel {
                        shared
                            .execute(
                                &mut driver.decision().unwrap(),
                                context("cancel6"),
                                Some(Arc::new(ModelIntentV1::CancelMarketOrders {
                                    condition_id: ConditionId(CONDITION_ID.into()),
                                })),
                            )
                            .await
                            .unwrap();
                    }
                    observed.push(at);
                }
                if at == 36_000 {
                    assert_eq!(
                        commitment,
                        ExecutionReadValueV1::Known(ExactDecimal(
                            if cancel { "4" } else { "10" }.into()
                        )),
                        "{outcome}"
                    );
                    // 36k 仍是原未终局成交；终局反馈 received=37022，
                    // 已证空档闭合后在下一真实 46k 组可见。

                    let trade = shared.registry().orders().next().unwrap();
                    let records: Vec<_> = shared
                        .registry()
                        .order_trades(trade.order_identity)
                        .collect();
                    assert_eq!(records.len(), 1);
                    let ::execution_normalization::NormalizationRecord::Evidence {
                        evidence:
                            ::execution_normalization::SourceNeutralEvidenceKind::TradeLifecycleV3 {
                                result,
                            },
                        ..
                    } = records[0].record()
                    else {
                        panic!("原成交引用")
                    };
                    let result = &result.result;
                    use contracts::execution_normalization::SourceNeutralTradeEconomicDispositionV1::*;
                    assert_eq!(
                        result.economic_disposition,
                        if outcome == "unapplied" {
                            PendingUnapplied
                        } else {
                            PendingApplied
                        }
                    );
                    observed.push(at);
                }
                if at == 46_000 || at == 56_000 {
                    assert_eq!(
                        commitment,
                        ExecutionReadValueV1::Known(ExactDecimal(
                            if cancel { "0" } else { "6" }.into()
                        )),
                        "{outcome}"
                    );
                    observed.push(at);
                }
            }
            assert_eq!(observed, vec![28_000, 36_000, 46_000, 56_000]);
        }
    }
}
#[tokio::test(flavor = "current_thread")]
async fn actual_quote_orders_keep_original_target_until_complete_initial_set_is_final() {
    for fok in [true, false] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let (core, times) = recorded_trading(&root, if fok { "20" } else { "4" }, false);
        let policy = delayed_policy("success");
        let (mut execution, observation) =
            execution(&temp.path().join("sim.jsonl"), &core, &policy);
        let opening = observation.opening_record().unwrap();
        let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
            per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
        });
        execution
            .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
            .unwrap();
        let mut shared = SharedExecutionV1::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "quote".into());
        for _ in 0..4 {
            driver.next_group().unwrap().unwrap();
        }
        let order = if fok {
            BuyOrderIntentV1::FokMarket {
                spend_pusd: Decimal::from(7),
                worst_price_pusd_per_share: Decimal::new(7, 1),
            }
        } else {
            BuyOrderIntentV1::FakMarket {
                spend_pusd: Decimal::from(6),
                worst_price_pusd_per_share: Decimal::new(6, 1),
            }
        };
        let intent = Arc::new(ModelIntentV1::SubmitSingleBuy {
            condition_id: ConditionId(CONDITION_ID.into()),
            order: BuyLegIntentV1 {
                token_id: TokenId(UP_TOKEN_ID.into()),
                order,
            },
        });
        let receipt = shared
            .execute(
                &mut driver.decision().unwrap(),
                context("quote10"),
                Some(intent.clone()),
            )
            .await
            .unwrap()
            .unwrap();
        let mut incomplete = SharedExecutionRegistryV1::new(Arc::new(opening.context().clone()));
        let origin = incomplete.freeze("quote10".into(), intent);
        incomplete.record_sim_receipt(&origin, &receipt);
        let mut seen = 0;
        while driver.next_group().unwrap().is_some() {
            let Some(view) = shared.action_before(&mut driver).unwrap() else {
                continue;
            };
            let at = view.cutoff().decision_time().logical_time_utc_ms();
            if at == 28_000 {
                let order = view.orders().next().unwrap();
                assert_eq!(
                    order.active_unfilled_shares,
                    ExecutionReadValueV1::NotApplicable
                );
                assert_eq!(order.cancellable, ExecutionReadValueV1::Known(false));
                assert_eq!(
                    shared.registry().committed_target_shares(
                        &ConditionId(CONDITION_ID.into()),
                        &TokenId(UP_TOKEN_ID.into())
                    ),
                    ExecutionReadValueV1::Known(ExactDecimal("10".into()))
                );
                // 只消费真实反馈引用，先让接受生命周期可见、提交回执尚不可见。
                incomplete.observe(order.formed.unwrap());
                incomplete.observe(order.latest_lifecycle.unwrap());
                let partial = incomplete.order(order.order_identity).unwrap();
                assert!(partial.origin.is_none());
                assert!(partial.accepted.is_none());
                assert_eq!(partial.cancellable, ExecutionReadValueV1::Known(false));
                assert_eq!(
                    partial.active_unfilled_shares,
                    ExecutionReadValueV1::Unavailable
                );
                assert_eq!(partial.matched_shares, ExecutionReadValueV1::Unavailable);
                if !fok {
                    assert!(matches!(
                        partial.latest_lifecycle.unwrap().record(),
                        ::execution_normalization::NormalizationRecord::Evidence {
                            evidence: ::execution_normalization::SourceNeutralEvidenceKind::OrderLifecycle {
                                result: contracts::execution_normalization::SourceNeutralOrderLifecycleResultV1 {
                                    event: contracts::execution_normalization::SourceNeutralOrderLifecycleEventV1::FakUnspentReleased { .. },
                                    ..
                                }
                            }, ..
                        }
                    ));
                }
                assert_eq!(
                    incomplete
                        .all_commitments()
                        .next()
                        .unwrap()
                        .remaining_target_shares,
                    ExecutionReadValueV1::Known(ExactDecimal("10".into()))
                );
                incomplete.observe(order.accepted.unwrap());
                seen += 1;
            }
            if at == 56_000 {
                assert_eq!(
                    shared.registry().committed_target_shares(
                        &ConditionId(CONDITION_ID.into()),
                        &TokenId(UP_TOKEN_ID.into())
                    ),
                    ExecutionReadValueV1::Known(ExactDecimal("0".into()))
                );
                assert_eq!(
                    incomplete.committed_target_shares(
                        &ConditionId(CONDITION_ID.into()),
                        &TokenId(UP_TOKEN_ID.into())
                    ),
                    ExecutionReadValueV1::Known(ExactDecimal("10".into())),
                    "Accepted可见但实际初始成交集合未覆盖，不能提前闭合"
                );
                let full = shared.registry().orders().next().unwrap();
                for trade in shared.registry().order_trades(full.order_identity) {
                    incomplete.observe(trade);
                }
                assert_eq!(
                    incomplete.committed_target_shares(
                        &ConditionId(CONDITION_ID.into()),
                        &TokenId(UP_TOKEN_ID.into())
                    ),
                    ExecutionReadValueV1::Known(ExactDecimal("0".into()))
                );
                seen += 1;
            }
        }
        assert_eq!(seen, 2);
    }
}
#[tokio::test(flavor = "current_thread")]
async fn all_parameter_readers_keep_cash100_while_later_real_maker_application_is_cash80() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_trading_with_maker(&root, "1", Some(("0.2", "100")));
    let policy = delayed_policy("success");
    let (mut execution, observation) = execution(&temp.path().join("sim.jsonl"), &core, &policy);
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "cash-freeze".into());
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    let before = shared.action_before(&mut driver).unwrap().unwrap();
    shared
        .execute(
            &mut driver.decision().unwrap(),
            context("maker100"),
            Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                order: leg(UP_TOKEN_ID, 100),
            })),
        )
        .await
        .unwrap();
    let mut seen = false;
    while driver.next_group().unwrap().is_some() {
        let Some(after) = shared.action_before(&mut driver).unwrap() else {
            continue;
        };
        if after.cutoff().decision_time().logical_time_utc_ms() == 56_000 {
            assert_eq!(
                after.ledger().platform_confirmed_projection.cash_pusd.0,
                "80"
            );
            assert_eq!(
                before.ledger().platform_confirmed_projection.cash_pusd.0,
                "100"
            );
            for frame in driver.decision().unwrap().frames() {
                assert_eq!(&frame.decision_cutoff, after.cutoff());
            }
            seen = true;
        }
    }
    assert!(seen);
}
#[tokio::test(flavor = "current_thread")]
async fn actual_partial_pair_reads_six_zero_and_converted_order_funds_without_averaging() {
    use ::execution_normalization::{NormalizationRecord, SourceNeutralEvidenceKind};
    use contracts::execution_normalization::*;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_pair(&root, true);
    let (mut execution, observation) =
        execution(&temp.path().join("sim.jsonl"), &core, SUCCESS_POLICY);
    let opening = observation.opening_record().unwrap();
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    ledger.apply_noe_memory_v1(&opening).unwrap();
    let ids = [
        LedgerRequestIdentityV1("reserve0".into()),
        LedgerRequestIdentityV1("reserve1".into()),
    ];
    for (ordinal, amount) in ["6", "9"].into_iter().enumerate() {
        ledger
            .try_reserve_intent(
                &ids[ordinal],
                &ConditionId(CONDITION_ID.into()),
                &ExactDecimal(amount.into()),
                &ExactDecimal("0".into()),
            )
            .unwrap();
        ledger
            .record_submission_started(
                &LedgerLocalEventIdentityV1(format!("sent{ordinal}")),
                &ids[ordinal],
                &LedgerSubmissionItemIdentityV1 {
                    global_request_id: "pair-partial".into(),
                    batch_item_ordinal: ordinal as u16,
                },
            )
            .unwrap();
    }
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "partial".into());
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    let mut second = leg(DOWN_TOKEN_ID, 30);
    if let BuyOrderIntentV1::GtcLimit {
        limit_price_pusd_per_share,
        ..
    } = &mut second.order
    {
        *limit_price_pusd_per_share = Decimal::new(3, 1);
    }
    intent_bridge::IntentBridge::bind_sim_group(&mut driver.decision().unwrap())
        .execute(
            context("pair-partial"),
            &ModelIntentV1::SubmitPairedBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                orders: [leg(UP_TOKEN_ID, 30), second],
            },
        )
        .await
        .unwrap();
    for _ in 4..=6 {
        driver.next_group().unwrap().unwrap();
    }
    let records = observation.take_closed_records();
    for r in &records {
        if matches!(
            r.record(),
            NormalizationRecord::Evidence {
                evidence: SourceNeutralEvidenceKind::Control { .. },
                ..
            }
        ) {
            ledger.apply_noe_memory_v1(r).unwrap();
        }
    }
    assert_eq!(
        ledger
            .funds_read_v1()
            .request(&ids[0])
            .unwrap()
            .principal_pusd
            .0,
        "6"
    );
    assert_eq!(
        ledger
            .funds_read_v1()
            .request(&ids[1])
            .unwrap()
            .principal_pusd
            .0,
        "0"
    );
    assert_eq!(
        ledger
            .funds_read_v1()
            .request(&ids[1])
            .unwrap()
            .reservation_state,
        ReservationStatusV1::Released
    );
    for r in &records {
        ledger.apply_noe_memory_v1(r).unwrap();
    }
    let accepted: Vec<_> = records
        .iter()
        .filter_map(|r| match r.record() {
            NormalizationRecord::Evidence {
                evidence:
                    SourceNeutralEvidenceKind::Control {
                        result:
                            SourceNeutralControlResult::Submission {
                                outcome: SourceNeutralSubmissionOutcomeV2::Accepted { order, .. },
                            },
                    },
                ..
            } => Some(&order.order_identity),
            _ => None,
        })
        .collect();
    assert_eq!(accepted.len(), 1);
    assert_eq!(
        ledger
            .funds_read_v1()
            .request(&ids[0])
            .unwrap()
            .reservation_state,
        ReservationStatusV1::Converted
    );
    assert_eq!(
        ledger
            .funds_read_v1()
            .request(&ids[0])
            .unwrap()
            .principal_pusd
            .0,
        "0"
    );
    assert_eq!(
        ledger
            .funds_read_v1()
            .order(accepted[0])
            .unwrap()
            .principal_encumbrance_pusd,
        ExecutionReadValueV1::Known(ExactDecimal("6".into()))
    );
}
fn platform_id(id: &::execution_normalization::MemoryLogicalIdentityV1) -> String {
    let ::execution_normalization::MemoryIdentityKeyV1::Entity(
        ::execution_normalization::SourceNeutralEntityKeyV1::Order { source_order_id },
    ) = &id.key
    else {
        panic!("NOE原订单实体")
    };
    source_order_id.clone()
}
#[tokio::test(flavor = "current_thread")]
async fn sealed_group_five_methods_are_once_and_construction_failure_and_no_trade_are_zero() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_memory_complete(&root);
    let path = temp.path().join("sim.jsonl");
    let (mut execution, observation) = execution(&path, &core, SUCCESS_POLICY);
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "five".into());
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    let condition = ConditionId(CONDITION_ID.into());
    let mut receipts = Vec::new();
    for (id, intent) in [
        (
            "single",
            ModelIntentV1::SubmitSingleBuy {
                condition_id: condition.clone(),
                order: leg(UP_TOKEN_ID, 5),
            },
        ),
        (
            "pair",
            ModelIntentV1::SubmitPairedBuy {
                condition_id: condition.clone(),
                orders: [leg(UP_TOKEN_ID, 5), leg(DOWN_TOKEN_ID, 5)],
            },
        ),
    ] {
        receipts.push(
            shared
                .execute(
                    &mut driver.decision().unwrap(),
                    context(id),
                    Some(Arc::new(intent)),
                )
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert!(
        shared
            .execute(&mut driver.decision().unwrap(), context("no-trade"), None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        shared
            .execute(
                &mut driver.decision().unwrap(),
                context("invalid-empty"),
                Some(Arc::new(ModelIntentV1::CancelOrders {
                    platform_order_ids: vec![]
                }))
            )
            .await,
        Err(SimRunActorErrorV2::InvalidBusinessRequest(_))
    ));
    for _ in 4..=6 {
        driver.next_group().unwrap().unwrap();
    }
    shared.action_before(&mut driver).unwrap();
    let ids: Vec<_> = shared
        .registry()
        .orders()
        .map(|o| platform_id(o.order_identity))
        .collect();
    assert_eq!(ids.len(), 3);
    for (id, intent) in [
        (
            "cancel-one",
            ModelIntentV1::CancelOrder {
                platform_order_id: ids[0].clone(),
            },
        ),
        (
            "cancel-batch",
            ModelIntentV1::CancelOrders {
                platform_order_ids: vec![ids[0].clone(), ids[1].clone()],
            },
        ),
        (
            "cancel-market",
            ModelIntentV1::CancelMarketOrders {
                condition_id: condition,
            },
        ),
    ] {
        receipts.push(
            shared
                .execute(
                    &mut driver.decision().unwrap(),
                    context(id),
                    Some(Arc::new(intent)),
                )
                .await
                .unwrap()
                .unwrap(),
        );
    }
    while driver.next_group().unwrap().is_some() {
        shared.action_before(&mut driver).unwrap();
    }
    shared.apply_closed().unwrap();
    driver.complete(&mut Materials { root }).unwrap();
    let text = std::fs::read_to_string(path).unwrap();
    let values: Vec<serde_json::Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let mut requests = Vec::new();
    fn visit(v: &serde_json::Value, out: &mut Vec<serde_json::Value>) {
        match v {
            serde_json::Value::Object(m) => {
                if m.get("input").and_then(|v| v.as_str()) == Some("PUBLIC_REQUEST") {
                    out.push(m["body"]["request"].clone());
                }
                for v in m.values() {
                    visit(v, out)
                }
            }
            serde_json::Value::Array(a) => {
                for v in a {
                    visit(v, out)
                }
            }
            _ => {}
        }
    }
    for value in &values {
        visit(value, &mut requests)
    }
    assert_eq!(requests.len(), 5);
    let methods: Vec<_> = requests
        .iter()
        .map(|r| r["method"].as_str().unwrap())
        .collect();
    assert_eq!(
        methods,
        vec![
            "POST_ORDER",
            "POST_ORDERS",
            "CANCEL_ORDER",
            "CANCEL_ORDERS",
            "CANCEL_MARKET_ORDERS"
        ]
    );
    assert_eq!(receipts.len(), 5);
    let unique: std::collections::BTreeSet<_> =
        receipts.iter().map(|r| r.input_sequence()).collect();
    assert_eq!(unique.len(), 5);
}
#[tokio::test(flavor = "current_thread")]
async fn ledger_background_io_failure_is_original_error_and_never_clean_run() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_memory_complete(&root);
    let path = temp.path().join("sim.jsonl");
    let (mut execution, observation) = execution(&path, &core, SUCCESS_POLICY);
    let blocked = temp.path().join("ledger-file");
    std::fs::write(&blocked, b"retained").unwrap();
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    execution
        .bind_ledger_recording_v2(&mut ledger, &blocked)
        .unwrap();
    let _shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "io-failure".into());
    let mut failure = None;
    loop {
        match driver.next_group() {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(e) => {
                failure = Some(e);
                break;
            }
        }
    }
    let error = if let Some(error) = failure {
        error
    } else {
        match driver.complete(&mut Materials { root }) {
            Err(e) => e,
            Ok(_) => panic!("I/O失败不能正常完成"),
        }
    };
    assert!(matches!(
        error,
        SimRunActorErrorV2::Log(SimEventLogErrorV2::Ledger(LedgerDurableErrorV1::Io { .. }))
    ));
    assert_eq!(std::fs::read(blocked).unwrap(), b"retained");
    assert!(!std::fs::read_to_string(path).unwrap().contains("RUN_SEAL"));
}
#[tokio::test(flavor = "current_thread")]
async fn real_same_arrival_delayed_batch_forms_r1_s0_r0_without_reordering_original_legs() {
    use contracts::execution_normalization::*;
    use sim_system::latency_profile::{DelaySampleDomainV1, load_embedded_proxy_latency_profile};
    let latency = load_embedded_proxy_latency_profile().unwrap();
    // 从冻结经验延迟选择真实 seq1/seq2 接收反序的同源变体，不改事件时间。
    let seed = (0u64..100)
        .map(|n| format!("{n:064x}"))
        .find(|seed| {
            latency.sample_delay_ms(
                seed.as_bytes(),
                "1",
                DelaySampleDomainV1::LifecycleFeedbackInbound,
            ) > latency.sample_delay_ms(
                seed.as_bytes(),
                "2",
                DelaySampleDomainV1::LifecycleFeedbackInbound,
            )
        })
        .unwrap();
    let mut seen = std::collections::BTreeMap::new();
    let (r, s) = (0..10_000)
        .find_map(|i| {
            let id = format!("same-arrival-{i}");
            let delay =
                latency.sample_delay_ms(seed.as_bytes(), &id, DelaySampleDomainV1::RequestOutbound);
            seen.insert(delay, id.clone()).map(|prior| (prior, id))
        })
        .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_memory_complete(&root);
    let (mut execution, observation) = execution_with_seed(
        &temp.path().join("sim.jsonl"),
        &core,
        SUCCESS_POLICY,
        Some(150),
        seed.clone(),
    );
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "formation".into());
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    assert_eq!(
        driver.decision().unwrap().frames()[0]
            .rule_context
            .taker_delay_ms,
        150
    );
    let r_receipt = shared
        .execute(
            &mut driver.decision().unwrap(),
            context(&r),
            Some(Arc::new(ModelIntentV1::SubmitPairedBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                orders: [
                    BuyLegIntentV1 {
                        token_id: TokenId(UP_TOKEN_ID.into()),
                        order: BuyOrderIntentV1::FokMarket {
                            spend_pusd: Decimal::from(6),
                            worst_price_pusd_per_share: Decimal::new(6, 1),
                        },
                    },
                    leg(DOWN_TOKEN_ID, 10),
                ],
            })),
        )
        .await
        .unwrap()
        .unwrap();
    let s_receipt = shared
        .execute(
            &mut driver.decision().unwrap(),
            context(&s),
            Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                order: leg(UP_TOKEN_ID, 5),
            })),
        )
        .await
        .unwrap()
        .unwrap();
    // 原冻结意图的重复提交必须返回首次 receipt，不改变跨请求处理键。
    let original = shared
        .registry()
        .all_commitments()
        .find(|c| c.origin.intent.global_request_id() == r)
        .unwrap()
        .origin
        .intent
        .intent()
        .clone();
    let repeated = shared
        .execute(
            &mut driver.decision().unwrap(),
            context(&r),
            Some(Arc::new(original)),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repeated, r_receipt);
    assert!(r_receipt.input_sequence() < s_receipt.input_sequence());
    assert_eq!(
        r_receipt.platform_arrived_at_utc_ms(),
        s_receipt.platform_arrived_at_utc_ms()
    );
    for _ in 4..=6 {
        driver.next_group().unwrap().unwrap();
    }
    shared.action_before(&mut driver).unwrap();
    let source_seq = |record: &::execution_normalization::NoeConsumedRecordRefV1| match record
        .record()
        .source_reference()
        .record
    {
        SimV2SourceRecordReferenceV1::OrderLifecycle {
            private_feedback_sequence,
            ..
        } => private_feedback_sequence,
        _ => panic!("formed引用必须为原OrderAccepted"),
    };
    let mut formed: Vec<_> = shared
        .registry()
        .orders()
        .map(|o| {
            let origin = o.origin.unwrap();
            (
                source_seq(o.formed.unwrap()),
                origin.intent.global_request_id().to_owned(),
                origin.batch_item_ordinal,
            )
        })
        .collect();
    let mut delivered: Vec<_> = shared
        .registry()
        .orders()
        .map(|o| {
            let formed = o.formed.unwrap();
            let ::execution_normalization::NoeConsumptionVisibilityV1::Feedback {
                delivery_sequence,
                ..
            } = formed.visibility()
            else {
                panic!("形成事件为真实反馈")
            };
            (*delivery_sequence, source_seq(formed))
        })
        .collect();
    delivered.sort();
    let source_in_delivery: Vec<_> = delivered.iter().map(|(_, source)| *source).collect();
    assert!(
        source_in_delivery.windows(2).any(|pair| pair[0] > pair[1]),
        "夹具须真实包含接收反序: {delivered:?}"
    );
    let mut processed = shared
        .registry()
        .orders()
        .map(|order| {
            let origin = order.origin.unwrap();
            let ::execution_normalization::NormalizationRecord::Evidence {
                evidence:
                    ::execution_normalization::SourceNeutralEvidenceKind::Control {
                        result:
                            SourceNeutralControlResult::Submission {
                                outcome:
                                    SourceNeutralSubmissionOutcomeV2::Accepted {
                                        effective_processing_at_utc_ms,
                                        ..
                                    },
                            },
                    },
                ..
            } = order.accepted.unwrap().record()
            else {
                panic!("原Accepted逐项处理时间")
            };
            let receipt = if origin.intent.global_request_id() == r {
                r_receipt
            } else {
                s_receipt
            };
            (
                (
                    *effective_processing_at_utc_ms,
                    receipt.input_sequence(),
                    origin.batch_item_ordinal,
                ),
                origin.intent.global_request_id().to_owned(),
                origin.batch_item_ordinal,
            )
        })
        .collect::<Vec<_>>();
    processed.sort();
    formed.sort();
    assert_eq!(
        formed
            .iter()
            .map(|(_, id, ordinal)| (id.as_str(), *ordinal))
            .collect::<Vec<_>>(),
        vec![(r.as_str(), 1), (s.as_str(), 0), (r.as_str(), 0)]
    );
    assert_eq!(
        processed
            .iter()
            .map(|(_, id, ordinal)| (id, ordinal))
            .collect::<Vec<_>>(),
        formed
            .iter()
            .map(|(_, id, ordinal)| (id, ordinal))
            .collect::<Vec<_>>()
    );
}
#[tokio::test(flavor = "current_thread")]
async fn real_late_query_cannot_reopen_closed_leg_and_paired_cancel_disappears() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_trading(&root, "4", false);
    let (mut execution, observation) =
        execution(&temp.path().join("sim.jsonl"), &core, SUCCESS_POLICY);
    let opening = observation.opening_record().unwrap();
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "late-query".into());
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    let intent = Arc::new(ModelIntentV1::SubmitPairedBuy {
        condition_id: ConditionId(CONDITION_ID.into()),
        orders: [leg(UP_TOKEN_ID, 10), leg(DOWN_TOKEN_ID, 10)],
    });
    let receipt = shared
        .execute(
            &mut driver.decision().unwrap(),
            context("late-pair"),
            Some(intent.clone()),
        )
        .await
        .unwrap()
        .unwrap();
    let mut late = SharedExecutionRegistryV1::new(Arc::new(opening.context().clone()));
    let origin = late.freeze("late-pair".into(), intent);
    late.record_sim_receipt(&origin, &receipt);
    let mut selected = None;
    let mut query = None;
    let mut milestones = 0;
    while driver.next_group().unwrap().is_some() {
        let Some(view) = shared.action_before(&mut driver).unwrap() else {
            continue;
        };
        let at = view.cutoff().decision_time().logical_time_utc_ms();
        if at == 28_000 {
            let ExecutionReadValueV1::Known(objects) = view.cancel_objects() else {
                panic!("原实际paired可撤")
            };
            assert!(
                objects
                    .iter()
                    .any(|o| matches!(o, CancelObjectReadV1::Paired { .. }))
            );
            let order = view
                .orders()
                .find(|o| o.origin.as_ref().unwrap().batch_item_ordinal == 1)
                .unwrap();
            let id = order.order_identity.clone();
            late.observe(order.accepted.unwrap());
            late.observe(order.formed.unwrap());
            driver
                .decision()
                .unwrap()
                .request(BusinessRequestV2::Order(OrderRequestV2 {
                    context: context("old-query"),
                    platform_order_id: platform_id(&id),
                }))
                .unwrap();
            selected = Some(id);
            milestones += 1;
        }
        if at == 36_000 {
            let id = selected.as_ref().unwrap();
            let order = shared.registry().order(id).unwrap();
            query = Some(order.latest_direct_feedback.unwrap().clone());
            assert!(matches!(query.as_ref().unwrap().record().source_reference().record,contracts::execution_normalization::SimV2SourceRecordReferenceV1::DirectResponse{association:contracts::execution_normalization::SimV2DirectResponseAssociationV1::Request{..},..}));
            shared
                .execute(
                    &mut driver.decision().unwrap(),
                    context("close-leg1"),
                    Some(Arc::new(ModelIntentV1::CancelOrder {
                        platform_order_id: platform_id(id),
                    })),
                )
                .await
                .unwrap();
            milestones += 1;
        }
        if at == 56_000 {
            let id = selected.as_ref().unwrap();
            let closed = shared.registry().order(id).unwrap();
            late.observe(closed.latest_lifecycle.unwrap());
            late.observe(query.as_ref().unwrap());
            let read = late.order(id).unwrap();
            assert_eq!(
                read.active_unfilled_shares,
                ExecutionReadValueV1::Known(ExactDecimal("0".into()))
            );
            assert_eq!(read.cancellable, ExecutionReadValueV1::Known(false));
            let ExecutionReadValueV1::Known(objects) = view.cancel_objects() else {
                panic!("完整剩余成员")
            };
            assert_eq!(objects.len(), 2);
            assert!(
                !objects
                    .iter()
                    .any(|o| matches!(o, CancelObjectReadV1::Paired { .. }))
            );
            let CancelObjectReadV1::Market { orders, .. } = &objects[1] else {
                panic!("市场成员")
            };
            assert_eq!(orders.len(), 1);
            milestones += 1;
        }
    }
    assert_eq!(milestones, 3);
}
#[tokio::test(flavor = "current_thread")]
async fn actual_trade_association_late_only_attributes_existing_entries() {
    use ::execution_normalization::{NormalizationRecord, SourceNeutralEvidenceKind};
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_trading(&root, "4", false);
    let (execution, observation) = execution(&temp.path().join("sim.jsonl"), &core, SUCCESS_POLICY);
    let opening = observation.opening_record().unwrap();
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    ledger.apply_noe_memory_v1(&opening).unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "late-attribution".into());
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    intent_bridge::IntentBridge::bind_sim_group(&mut driver.decision().unwrap())
        .execute(
            context("attribution"),
            &ModelIntentV1::SubmitSingleBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                order: BuyLegIntentV1 {
                    token_id: TokenId(UP_TOKEN_ID.into()),
                    order: BuyOrderIntentV1::GtcLimit {
                        limit_price_pusd_per_share: Decimal::new(6, 1),
                        size_shares: Decimal::from(10),
                        post_only: false,
                    },
                },
            },
        )
        .await
        .unwrap();
    let mut records = Vec::new();
    while driver.next_group().unwrap().is_some() {
        records.extend(observation.take_closed_records());
    }
    records.extend(observation.take_closed_records());
    let mut association = Vec::new();
    let mut pending = Vec::new();
    let mut order = None;
    for record in records {
        match record.record() {
            NormalizationRecord::Evidence {
                evidence: SourceNeutralEvidenceKind::TradeLifecycleV3 { result },
                ..
            } => {
                order = Some(result.result.order_identity.clone());
                association.push(record);
            }
            _ => pending.push(record),
        }
    }
    while !pending.is_empty() {
        let mut waiting = Vec::new();
        let prior = pending.len();
        for r in pending {
            if matches!(
                ledger.apply_noe_memory_v1(&r).unwrap(),
                LedgerMemoryApplyOutcomeV1::Applied(
                    LedgerApplyOutcomeV1::MissingPlatformPredecessor { .. }
                )
            ) {
                waiting.push(r);
            }
        }
        assert!(waiting.len() < prior);
        pending = waiting;
    }
    let id = order.unwrap();
    let before = ledger.projection_v2().unwrap();
    assert_eq!(
        ledger
            .funds_read_v1()
            .order(&id)
            .unwrap()
            .principal_encumbrance_pusd,
        ExecutionReadValueV1::Unavailable
    );
    // 真实接收顺序是 MATCHED、CONFIRMED、MINED；原 Ledger 生命周期随这三条输入更新。
    // 此处验证资金归属不重复经济入账，不把生命周期计数误当经济金额。
    assert_eq!(association.len(), 3);
    for (record, pending_count) in association.iter().zip([1, 0, 1]) {
        ledger.apply_noe_memory_v1(record).unwrap();
        let current = ledger.projection_v2().unwrap();
        assert_eq!(
            current.platform_confirmed_projection.pending_fill_count,
            pending_count
        );
        assert_eq!(
            current.platform_source_prefix,
            before.platform_source_prefix
        );
        let mut economic = current.platform_confirmed_projection;
        economic.pending_fill_count = before.platform_confirmed_projection.pending_fill_count;
        assert_eq!(economic, before.platform_confirmed_projection);
    }
    let after = ledger.projection_v2().unwrap();
    assert_eq!(after.ledger_entry_seq, before.ledger_entry_seq + 3);
    assert_eq!(
        ledger
            .funds_read_v1()
            .order(&id)
            .unwrap()
            .principal_encumbrance_pusd,
        ExecutionReadValueV1::Known(ExactDecimal("3.6".into()))
    );
    for record in &association {
        assert!(matches!(
            ledger.apply_noe_memory_v1(record).unwrap(),
            LedgerMemoryApplyOutcomeV1::Applied(LedgerApplyOutcomeV1::AlreadyApplied { .. })
        ));
    }
    assert_eq!(ledger.projection_v2().unwrap(), after);
}

#[tokio::test(flavor = "current_thread")]
async fn real_expiry_dust_and_market_terminal_close_without_reopening() {
    use ::execution_normalization::{NormalizationRecord, SourceNeutralEvidenceKind};
    use contracts::execution_normalization::*;
    for variant in ["expiry", "dust", "market"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let (core, times) = recorded_trading_window(
            &root,
            if variant == "dust" { "4.999" } else { "2" },
            if variant == "dust" {
                Some(("0.6", "3"))
            } else {
                None
            },
            400_000,
            true,
        );
        let (execution, observation) =
            execution(&temp.path().join("sim.jsonl"), &core, SUCCESS_POLICY);
        let ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
            per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
        });
        let mut shared = SharedExecutionV1::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, variant.into());
        for _ in 0..4 {
            driver.next_group().unwrap().unwrap();
        }
        let order = if variant == "expiry" {
            BuyOrderIntentV1::GtdLimit {
                limit_price_pusd_per_share: Decimal::new(6, 1),
                size_shares: Decimal::from(5),
                post_only: false,
                expires_at_utc_sec: 300,
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
                context(variant),
                Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                    condition_id: ConditionId(CONDITION_ID.into()),
                    order: BuyLegIntentV1 {
                        token_id: TokenId(UP_TOKEN_ID.into()),
                        order,
                    },
                })),
            )
            .await
            .unwrap();
        while driver.next_group().unwrap().is_some() {
            shared.action_before(&mut driver).unwrap();
        }
        assert!(shared.apply_closed().unwrap());
        let order = shared.registry().orders().next().expect("真实Accepted订单");
        let NormalizationRecord::Evidence {
            evidence: SourceNeutralEvidenceKind::OrderLifecycle { result },
            ..
        } = order.latest_lifecycle.unwrap().record()
        else {
            panic!("真实终态")
        };
        assert!(
            match variant {
                "expiry" => matches!(result.event, SourceNeutralOrderLifecycleEventV1::GtdExpired),
                "dust" => matches!(
                    result.event,
                    SourceNeutralOrderLifecycleEventV1::VenueDustTerminal
                ),
                _ => matches!(
                    result.event,
                    SourceNeutralOrderLifecycleEventV1::MarketResolvedCanceled { .. }
                ),
            },
            "{variant}: {:?}",
            result.event
        );
        assert_eq!(
            order.active_unfilled_shares,
            ExecutionReadValueV1::Known(ExactDecimal("0".into()))
        );
        assert_eq!(order.cancellable, ExecutionReadValueV1::Known(false));
        let mut late =
            SharedExecutionRegistryV1::new(Arc::new(order.accepted.unwrap().context().clone()));
        late.observe(order.latest_lifecycle.unwrap());
        late.observe(order.accepted.unwrap());
        late.observe(order.formed.unwrap());
        for trade in shared.registry().order_trades(order.order_identity) {
            late.observe(trade);
        }
        let late = late.order(order.order_identity).unwrap();
        assert_eq!(late.matched_shares, order.matched_shares);
        assert_eq!(
            late.active_unfilled_shares,
            ExecutionReadValueV1::Known(ExactDecimal("0".into()))
        );
        assert_eq!(late.cancellable, ExecutionReadValueV1::Known(false));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn pairs_follow_real_formation_not_request_name() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_memory_complete(&root);
    let (execution, observation) = execution(&temp.path().join("sim.jsonl"), &core, SUCCESS_POLICY);
    let ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, MemoryRecordedTimes).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "review-paired".into());
    driver.next_group().unwrap().unwrap();
    driver.next_group().unwrap().unwrap();
    shared.action_before(&mut driver).unwrap().unwrap();
    for name in ["z-first", "a-second"] {
        shared
            .execute(
                &mut driver.decision().unwrap(),
                RequestContextV2 {
                    global_request_id: name.into(),
                    caller_correlation_id: None,
                },
                Some(Arc::new(ModelIntentV1::SubmitPairedBuy {
                    condition_id: ConditionId(CONDITION_ID.into()),
                    orders: [leg(UP_TOKEN_ID, 5), leg(DOWN_TOKEN_ID, 5)],
                })),
            )
            .await
            .unwrap()
            .unwrap();
    }
    for _ in 2..=6 {
        driver.next_group().unwrap().unwrap();
    }
    let snapshot = shared.action_before(&mut driver).unwrap().unwrap();
    let ExecutionReadValueV1::Known(objects) = snapshot.cancel_objects() else {
        panic!("missing real objects")
    };
    let singles: Vec<_> = objects
        .iter()
        .filter_map(|o| match o {
            CancelObjectReadV1::Single { order } => {
                Some(order.origin.as_ref().unwrap().intent.global_request_id())
            }
            _ => None,
        })
        .collect();
    let pairs: Vec<_> = objects
        .iter()
        .filter_map(|o| match o {
            CancelObjectReadV1::Paired { origin, .. } => Some(origin.global_request_id()),
            _ => None,
        })
        .collect();
    for object in &objects {
        if let CancelObjectReadV1::Paired { orders, .. } = object {
            assert_eq!(orders[0].origin.as_ref().unwrap().batch_item_ordinal, 0);
            assert_eq!(orders[1].origin.as_ref().unwrap().batch_item_ordinal, 1);
        }
    }
    println!("Singles ordered by real formation: {singles:?}; Paired: {pairs:?}");
    assert_eq!(singles, vec!["z-first", "z-first", "a-second", "a-second"]);
    assert_eq!(
        pairs,
        vec!["z-first", "a-second"],
        "paired must retain formation order independently of request id"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn equal_start_markets_sort_condition_before_order_formation() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_equal_start_markets(&root);
    use sim_system::latency_profile::{DelaySampleDomainV1, load_embedded_proxy_latency_profile};
    let latency = load_embedded_proxy_latency_profile().unwrap();
    let seed = (0u64..100)
        .map(|n| format!("{n:064x}"))
        .find(|seed| {
            latency.sample_delay_ms(
                seed.as_bytes(),
                "higher-first",
                DelaySampleDomainV1::RequestOutbound,
            ) < latency.sample_delay_ms(
                seed.as_bytes(),
                "lower-second",
                DelaySampleDomainV1::RequestOutbound,
            )
        })
        .unwrap();
    let (execution, observation) = execution_with_seed(
        &temp.path().join("sim.jsonl"),
        &core,
        SUCCESS_POLICY,
        None,
        seed,
    );
    let ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "condition-order".into());
    for _ in 0..8 {
        driver.next_group().unwrap().unwrap();
    }
    for (condition, tokens, name) in [
        (OTHER_CONDITION_ID, ["21", "4"], "higher-first"),
        (CONDITION_ID, [UP_TOKEN_ID, DOWN_TOKEN_ID], "lower-second"),
    ] {
        shared
            .execute(
                &mut driver.decision().unwrap(),
                context(name),
                Some(Arc::new(ModelIntentV1::SubmitPairedBuy {
                    condition_id: ConditionId(condition.into()),
                    orders: [leg(tokens[0], 5), leg(tokens[1], 5)],
                })),
            )
            .await
            .unwrap();
    }
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    let view = shared.action_before(&mut driver).unwrap().unwrap();
    let first_source = |name: &str| {
        shared.registry().orders().filter(|order|order.origin.as_ref().unwrap().intent.global_request_id()==name).map(|order|match order.formed.unwrap().record().source_reference().record {contracts::execution_normalization::SimV2SourceRecordReferenceV1::OrderLifecycle{private_feedback_sequence,..}=>private_feedback_sequence,_=>panic!("原形成事件")}).min().unwrap()
    };
    assert!(
        first_source("higher-first") < first_source("lower-second"),
        "真实形成序须与condition顺序相反"
    );
    let ExecutionReadValueV1::Known(objects) = view.cancel_objects() else {
        panic!("完整实际对象")
    };
    let single: Vec<_> = objects
        .iter()
        .filter_map(|o| {
            if let CancelObjectReadV1::Single { order } = o {
                Some(order.origin.as_ref().unwrap().intent.global_request_id())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        single,
        vec![
            "lower-second",
            "lower-second",
            "higher-first",
            "higher-first"
        ]
    );
    let pairs: Vec<_> = objects
        .iter()
        .filter_map(|o| {
            if let CancelObjectReadV1::Paired { origin, orders } = o {
                assert_eq!(orders[0].origin.as_ref().unwrap().batch_item_ordinal, 0);
                assert_eq!(orders[1].origin.as_ref().unwrap().batch_item_ordinal, 1);
                Some(origin.global_request_id())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(pairs, vec!["lower-second", "higher-first"]);
    let markets: Vec<_> = objects
        .iter()
        .filter_map(|o| {
            if let CancelObjectReadV1::Market { condition_id, .. } = o {
                Some(condition_id.0.as_str())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(markets, vec![CONDITION_ID, OTHER_CONDITION_ID]);
}

#[tokio::test(flavor = "current_thread")]
async fn planned_stop_preserves_open_orders_and_does_not_read_next_group() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_trading(&root, "2", true);
    let path = temp.path().join("sim.jsonl");
    let (mut execution, observation) = execution(&path, &core, SUCCESS_POLICY);
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    let ledger_root = temp.path().join("ledger");
    execution
        .bind_ledger_recording_v2(&mut ledger, &ledger_root)
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation.clone())
        .unwrap()
        .unwrap();
    let reads = std::rc::Rc::new(std::cell::Cell::new(0));
    let counter = reads.clone();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times)
        .unwrap()
        .inspect(move |_| counter.set(counter.get() + 1));
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "stop-open".into());
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    shared.action_before(&mut driver).unwrap().unwrap();
    shared
        .execute(
            &mut driver.decision().unwrap(),
            context("open-with-position"),
            Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                order: BuyLegIntentV1 {
                    token_id: TokenId(UP_TOKEN_ID.into()),
                    order: BuyOrderIntentV1::GtcLimit {
                        limit_price_pusd_per_share: Decimal::new(6, 1),
                        size_shares: Decimal::from(10),
                        post_only: false,
                    },
                },
            })),
        )
        .await
        .unwrap();
    loop {
        driver.next_group().unwrap().unwrap();
        let action = shared.action_before(&mut driver).unwrap().unwrap();
        if action.cutoff().decision_time().logical_time_utc_ms() == 36_000 {
            let orders: Vec<_> = action.orders().collect();
            assert_eq!(orders.len(), 1);
            assert_eq!(
                orders[0].active_unfilled_shares,
                ExecutionReadValueV1::Known(ExactDecimal("5".into()))
            );
            break;
        }
    }
    let expected = shared.ledger().projection_v2().unwrap();
    assert_ne!(
        expected
            .platform_confirmed_projection
            .principal_encumbrance_pusd
            .0,
        "0"
    );
    assert!(
        expected
            .platform_confirmed_projection
            .outcome_balances
            .iter()
            .any(|b| b.token_id.0 == UP_TOKEN_ID && b.balance_shares.0 == "5")
    );
    let count = reads.get();
    let delivered = observation.take_closed_records();
    assert!(delivered.is_empty());
    driver.stop_and_drain().unwrap();
    assert_eq!(reads.get(), count, "停止不能调用来源 next 或处理未来终态");
    assert!(observation.take_closed_records().is_empty());
    assert_eq!(shared.ledger().projection_v2().unwrap(), expected);
    assert_eq!(
        LedgerWriterV1::open_durable(&ledger_root)
            .unwrap()
            .projection_v2()
            .unwrap(),
        expected
    );
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(!text.contains("RunSeal"));
    assert!(!text.contains("CancelOrder"));
    assert!(!text.contains("CancelAllOrders"));
}

#[tokio::test(flavor = "current_thread")]
async fn empty_initial_markets_consume_reference_before_enrollment_without_reopening_account() {
    use data_pipeline_core::public_observation_core::PublicObservationCoreV1;
    use sim_system::scenario::PlatformMarketV1;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_reference_then_market(&root);
    let empty = PublicObservationCoreV1::new(core.header().clone()).unwrap();
    assert_eq!(empty.fixed_markets().count(), 0);
    let path = temp.path().join("sim.jsonl");
    let (mut execution, observation) = execution(&path, &empty, SUCCESS_POLICY);
    let opening = observation.opening_record().unwrap();
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    let ledger_root = temp.path().join("ledger");
    execution
        .bind_ledger_recording_v2(&mut ledger, &ledger_root)
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation.clone())
        .unwrap()
        .unwrap();
    let initial = shared.ledger().projection_v2().unwrap();
    let mut source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times)
        .unwrap()
        .peekable();
    let reference = source.peek().unwrap().as_ref().unwrap();
    assert!(reference.prepared.group.observations.is_empty());
    assert!(
        reference
            .prepared
            .group
            .records
            .iter()
            .any(|r| matches!(r.payload, PublicBusinessPayloadV1::ReferenceFact(_)))
    );
    let mut driver =
        FinitePublicConsumptionV1::new(execution, source, "empty-first-reference".into());
    let (first, _) = driver.next_group().unwrap().unwrap();
    assert_eq!(first.group().group_position, 0);
    assert!(driver.frames().is_empty());
    assert!(shared.action_before(&mut driver).unwrap().is_none());
    assert_eq!(shared.ledger().projection_v2().unwrap(), initial);
    // 参考组已被真实消费，随后才将原 DPT 身份交给原登记入口。
    let context = core.pm_context(CONDITION_ID).unwrap();
    let market = PlatformMarketV1::try_new(
        context.market,
        context
            .assets
            .into_iter()
            .map(|a| a.asset)
            .collect::<Vec<_>>()
            .try_into()
            .unwrap(),
    )
    .unwrap();
    let enrollment = driver
        .enroll_market(MarketEnrollmentRequestV1 {
            enrollment_request_id: "first-arrived-market".into(),
            market,
        })
        .unwrap();
    let (second, _) = driver.next_group().unwrap().unwrap();
    assert_eq!(second.group().group_position, 1);
    assert!(second.actor_input_sequence() > first.actor_input_sequence());
    let frames = driver.frames();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].rule_context.market, enrollment.market);
    assert_eq!(
        frames[0].rule_context.snapshot_file_sha256.0,
        "d63c0e7b09edc4b2c4c3f0295f806b25f6f5fd03663cdf60ff062117e53e4e54"
    );
    assert!(shared.action_before(&mut driver).unwrap().is_some());
    let enrolled = shared.ledger().projection_v2().unwrap();
    // 新经济索引保留实际市场/腿映射，登记自身不重开户或改变现金、UTC。
    assert_eq!(enrolled.ledger_entry_seq, initial.ledger_entry_seq + 1);
    let mut financial_only = enrolled.clone();
    financial_only.ledger_entry_seq = initial.ledger_entry_seq;
    assert_eq!(financial_only, initial);
    assert_eq!(
        shared
            .ledger()
            .economic_read()
            .unwrap()
            .market_totals()
            .count(),
        1
    );
    assert_eq!(
        observation.opening_record().unwrap().position(),
        opening.position()
    );
    driver.stop_and_drain().unwrap();
    assert_eq!(
        LedgerWriterV1::open_durable(&ledger_root)
            .unwrap()
            .projection_v2()
            .unwrap(),
        enrolled
    );
    assert!(!std::fs::read_to_string(path).unwrap().contains("RunSeal"));
}

#[tokio::test(flavor = "current_thread")]
async fn continuous_empty_reference_enrollment_uses_same_shared_chain_without_reopening() {
    use data_pipeline_core::public_observation_core::PublicObservationCoreV1;
    use sim_system::scenario::PlatformMarketV1;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_reference_then_market(&root);
    let empty = PublicObservationCoreV1::new(core.header().clone()).unwrap();
    assert_eq!(empty.fixed_markets().count(), 0);
    let path = temp.path().join("sim.jsonl");
    let (mut execution, observation) = execution(&path, &empty, SUCCESS_POLICY);
    let opening = observation.opening_record().unwrap();
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    let ledger_root = temp.path().join("ledger");
    execution
        .bind_ledger_recording_v2(&mut ledger, &ledger_root)
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation.clone())
        .unwrap()
        .unwrap();
    let initial = shared.ledger().projection_v2().unwrap();
    let mut source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times)
        .unwrap()
        .peekable();
    let reference = source.peek().unwrap().as_ref().unwrap();
    assert!(reference.prepared.group.observations.is_empty());
    assert!(
        reference
            .prepared
            .group
            .records
            .iter()
            .any(|r| matches!(r.payload, PublicBusinessPayloadV1::ReferenceFact(_)))
    );
    let mut driver = ContinuousPublicConsumptionV1::new(execution, "online-first-reference".into());
    let (first, _) = driver
        .apply_group(source.next().unwrap().unwrap().prepared)
        .unwrap();
    assert_eq!(first.group().group_position, 0);
    assert!(driver.frames().is_empty());
    assert!(shared.action_before(&mut driver).unwrap().is_none());
    assert_eq!(shared.ledger().projection_v2().unwrap(), initial);
    // 参考组已被真实消费，随后才将原 DPT 身份交给原登记入口。
    let context = core.pm_context(CONDITION_ID).unwrap();
    let market = PlatformMarketV1::try_new(
        context.market,
        context
            .assets
            .into_iter()
            .map(|a| a.asset)
            .collect::<Vec<_>>()
            .try_into()
            .unwrap(),
    )
    .unwrap();
    let enrollment = driver
        .enroll_market(MarketEnrollmentRequestV1 {
            enrollment_request_id: "first-arrived-market".into(),
            market,
        })
        .unwrap();
    let before_late_market_close = shared.ledger().projection_v2();
    let (second, _) = driver
        .apply_group(source.next().unwrap().unwrap().prepared)
        .unwrap();
    assert_eq!(
        LedgerWriterV1::open_durable(&ledger_root)
            .unwrap()
            .projection_v2(),
        before_late_market_close,
        "PAPER迟登记已结束市场在接纳后的首个真实关闭前缀稳定此前材料，不回溯或等待Finish"
    );
    assert_eq!(second.group().group_position, 1);
    assert!(second.actor_input_sequence() > first.actor_input_sequence());
    let frames = driver.frames();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].rule_context.market, enrollment.market);
    assert_eq!(
        frames[0].rule_context.snapshot_file_sha256.0,
        "d63c0e7b09edc4b2c4c3f0295f806b25f6f5fd03663cdf60ff062117e53e4e54"
    );
    let before = shared.action_before(&mut driver).unwrap().unwrap();
    let after = shared.ledger().projection_v2().unwrap();
    // 当前执行 UTC 已越过历史夹具窗口；原返佣义务可形成零金额事实。
    // 账户不重建及资金不变，不要求真实外生前缀仍停在开户记录。
    assert_eq!(after.account_instance_key, initial.account_instance_key);
    assert_eq!(
        after.platform_confirmed_projection,
        initial.platform_confirmed_projection
    );
    assert_eq!(
        after.local_pending_reservation_projection,
        initial.local_pending_reservation_projection
    );
    assert_eq!(after.market_projections, initial.market_projections);
    assert_eq!(before.ledger(), &after);
    assert!(
        shared
            .execute(
                &mut driver.decision().unwrap(),
                RequestContextV2 {
                    global_request_id: "online-no-trade".into(),
                    caller_correlation_id: None
                },
                None
            )
            .await
            .unwrap()
            .is_none()
    );
    let request = shared
        .execute(
            &mut driver.decision().unwrap(),
            RequestContextV2 {
                global_request_id: "online-cancel-market".into(),
                caller_correlation_id: None,
            },
            Some(Arc::new(ModelIntentV1::CancelMarketOrders {
                condition_id: ConditionId(CONDITION_ID.into()),
            })),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request.scheduled_action_count(), 1);
    assert_eq!(shared.ledger().projection_v2().unwrap(), after);
    assert_eq!(before.ledger(), &after);
    assert_eq!(
        observation.opening_record().unwrap().position(),
        opening.position()
    );
    driver.stop_and_drain().unwrap();
    assert_eq!(
        LedgerWriterV1::open_durable(&ledger_root)
            .unwrap()
            .projection_v2()
            .unwrap(),
        after
    );
    assert!(!std::fs::read_to_string(path).unwrap().contains("RunSeal"));
}

#[tokio::test(flavor = "current_thread")]
async fn online_same_input_handoff_serves_timers_then_enrolls_and_executes_once() {
    use data_collect::handoff::{read_handoff_v2, write_handoff_v2};
    use data_pipeline_core::{public_input::*, public_observation_core::PublicObservationCoreV1};
    use data_pipeline_training::public_online::*;
    use sim_system::scenario::PlatformMarketV1;
    use tokio::io::AsyncWriteExt;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let config = online_config();
    let core = PublicObservationCoreV1::new(config.header.clone()).unwrap();
    let path = temp.path().join("sim.jsonl");
    let (mut execution, observation) = execution(&path, &core, SUCCESS_POLICY);
    let opening = observation.opening_record().unwrap();
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation.clone())
        .unwrap()
        .unwrap();
    let initial = shared.ledger().projection_v2().unwrap();
    let mut driver = ContinuousPublicConsumptionV1::new(execution, "socket-online".into());
    let (client, mut server) = tokio::net::UnixStream::pair().unwrap();
    let (partial_sent, partial_received) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        write_handoff_v2(
            &mut server,
            &DataCollectHandoffV2::Session(DataCollectHandoffSessionV2 {
                frame_schema_version: DATA_COLLECT_HANDOFF_SCHEMA_V2.into(),
                environment: "paper".into(),
                run_id: "socket-online".into(),
                git_commit: "test".into(),
                binary_sha256: "test".into(),
                config_digest: "test".into(),
                collector_host_id: "test".into(),
                collector_region: "test".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            read_handoff_v2(&mut server).await.unwrap(),
            DataCollectHandoffV2::Received {
                transport_sequence: 0
            }
        );
        let mut partial_sent = Some(partial_sent);
        let mut released = Some(released);
        for (index, input) in reference_then_market_inputs().into_iter().enumerate() {
            let DptIngressV1::Message { row, .. } = input.ingress else {
                panic!()
            };
            let frame = DataCollectHandoffV2::Message {
                row,
                transport_sequence: index as u64 + 1,
            };
            if index == 1 {
                let bytes = serde_json::to_vec(&frame).unwrap();
                let split = bytes.len() / 2;
                server.write_u64(bytes.len() as u64).await.unwrap();
                server.write_all(&bytes[..split]).await.unwrap();
                partial_sent.take().unwrap().send(()).unwrap();
                released.take().unwrap().await.unwrap();
                server.write_all(&bytes[split..]).await.unwrap();
            } else {
                write_handoff_v2(&mut server, &frame).await.unwrap();
            }
            assert_eq!(
                read_handoff_v2(&mut server).await.unwrap(),
                DataCollectHandoffV2::Received {
                    transport_sequence: index as u64 + 1
                }
            );
        }
        write_handoff_v2(
            &mut server,
            &DataCollectHandoffV2::End {
                last_confirmed_sequence: 3,
            },
        )
        .await
        .unwrap();
    });
    let mut progress = PublicOnlineProgressV1::default();
    let mut session = PublicOnlineSessionV1::start(client, &root, config, &mut progress)
        .await
        .unwrap();
    let mut release = Some(release);
    let mut partial_received = Some(partial_received);
    let mut group_count = 0;
    let mut request_count = 0;
    let mut pm_positions = Vec::new();
    for input_index in 0..4 {
        let mut next = Box::pin(session.next_applied(std::future::pending()));
        if input_index == 1 {
            partial_received.take().unwrap().await.unwrap();
            for _ in 0..3 {
                assert!(
                    tokio::time::timeout(std::time::Duration::from_millis(2), &mut next)
                        .await
                        .is_err()
                );
                driver.advance_due().unwrap(); // 真实 timer 消费入口，保留原半帧 future。
                assert!(driver.frames().is_empty());
                assert!(shared.action_before(&mut driver).unwrap().is_none());
            }
            release.take().unwrap().send(()).unwrap();
        }
        let PublicOnlineNextV1::Applied(mut pending) = next.await.unwrap() else {
            panic!()
        };
        assert_eq!(pending.input().input_position, input_index);
        let groups = PreparedPublicGroupV1::from_online(&mut pending);
        for (index, group) in groups.into_iter().enumerate() {
            assert!(Arc::ptr_eq(
                &group.group,
                &pending.applied().closed_groups[index]
            ));
            for (position, _) in &group.pm_inputs {
                pm_positions.push(*position);
            }
            if group_count == 1 {
                assert!(driver.frames().is_empty());
                let context = &group.pm_contexts[0];
                let market = PlatformMarketV1::try_new(
                    context.market.clone(),
                    context
                        .assets
                        .iter()
                        .map(|a| a.asset.clone())
                        .collect::<Vec<_>>()
                        .try_into()
                        .unwrap(),
                )
                .unwrap();
                driver
                    .enroll_market(MarketEnrollmentRequestV1 {
                        enrollment_request_id: "first-online-market".into(),
                        market,
                    })
                    .unwrap();
            }
            let (receipt, _) = driver.apply_group(group).unwrap();
            assert_eq!(receipt.group().group_position, group_count);
            group_count += 1;
            if input_index == 0 {
                assert!(driver.frames().is_empty());
                assert!(shared.action_before(&mut driver).unwrap().is_none());
                assert_eq!(shared.ledger().projection_v2().unwrap(), initial);
            } else {
                let before = shared.action_before(&mut driver).unwrap().unwrap();
                assert_eq!(
                    before.ledger().account_instance_key,
                    initial.account_instance_key
                );
                assert_eq!(
                    before.ledger().platform_confirmed_projection,
                    initial.platform_confirmed_projection
                );
                if input_index == 1 {
                    let request = shared
                        .execute(
                            &mut driver.decision().unwrap(),
                            context("socket-cancel-market"),
                            Some(Arc::new(ModelIntentV1::CancelMarketOrders {
                                condition_id: ConditionId(CONDITION_ID.into()),
                            })),
                        )
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(request.scheduled_action_count(), 1);
                    request_count += 1;
                }
            }
        }
        match pending.complete().await.unwrap() {
            PublicOnlineCompletedInputV1::Continue(next) => session = next,
            PublicOnlineCompletedInputV1::End(end) => {
                assert_eq!(input_index, 3);
                assert_eq!(end.last_received_transport_sequence, 3);
                assert!(end.committed_prefix.normal_end);
                break;
            }
        }
    }
    peer.await.unwrap();
    assert_eq!(group_count, 3);
    assert_eq!(request_count, 1);
    assert_eq!(pm_positions, vec![6]); // 一条参考事实 + metadata 的五条事实。
    assert_eq!(
        observation.opening_record().unwrap().position(),
        opening.position()
    );
    driver.stop_and_drain().unwrap();
    assert!(!std::fs::read_to_string(path).unwrap().contains("RunSeal"));
    // 原 reader 可直接读取同次在线记录，无新持久形状。
    let mut replay =
        data_pipeline_training::public_replay::PublicReplayV1::open(&root, 1, 1 << 20).unwrap();
    let mut count = 0;
    while replay.next_applied().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 4);
}

fn online_config() -> data_pipeline_training::public_online::PublicOnlineConfigV1 {
    data_pipeline_training::public_online::PublicOnlineConfigV1 {
        header: core().header().clone(),
        recording: config(),
        recording_in_flight_inputs: 1.try_into().unwrap(),
        stabilize_every_inputs: 100.try_into().unwrap(),
        read_page_rows: 1.try_into().unwrap(),
        max_read_batch_bytes: (1 << 20).try_into().unwrap(),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn online_multi_group_and_zero_step_pm_keep_canonical_positions_and_original_format() {
    use data_collect::handoff::{read_handoff_v2, write_handoff_v2};
    use data_pipeline_core::{public_input::*, typed_raw::*};
    use data_pipeline_training::{public_online::*, public_recording::*};
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let zero = pm(
        4,
        2,
        31_001,
        PolymarketOrderbookEvent::NewMarket {
            clob_market_id: market().clob_market_id.0,
            question: None,
            description: None,
            asset_ids: vec![UP_TOKEN_ID.into(), DOWN_TOKEN_ID.into()],
            outcomes: vec!["UP".into(), "DOWN".into()],
            event_message: None,
        },
    );
    let notice = pm(
        3,
        1,
        31_000,
        PolymarketOrderbookEvent::NewMarket {
            clob_market_id: market().clob_market_id.0,
            question: None,
            description: None,
            asset_ids: vec![UP_TOKEN_ID.into(), DOWN_TOKEN_ID.into()],
            outcomes: vec!["UP".into(), "DOWN".into()],
            event_message: None,
        },
    );
    let rows = vec![
        record(
            0,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(terminal_orderbook_row(
                &market(),
            ))),
        ),
        record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
                terminal_metadata_row(&market()),
            ))),
        ),
        meta(2),
        notice,
        zero.clone(),
        RecordedIngressV1 {
            input_position: 5,
            ..zero
        },
    ];
    let (client, mut server) = tokio::net::UnixStream::pair().unwrap();
    let peer = tokio::spawn(async move {
        write_handoff_v2(
            &mut server,
            &DataCollectHandoffV2::Session(DataCollectHandoffSessionV2 {
                frame_schema_version: DATA_COLLECT_HANDOFF_SCHEMA_V2.into(),
                environment: "paper".into(),
                run_id: "online-multi".into(),
                git_commit: "test".into(),
                binary_sha256: "test".into(),
                config_digest: "test".into(),
                collector_host_id: "test".into(),
                collector_region: "test".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            read_handoff_v2(&mut server).await.unwrap(),
            DataCollectHandoffV2::Received {
                transport_sequence: 0
            }
        );
        for (i, input) in rows.into_iter().enumerate() {
            let DptIngressV1::Message { row, .. } = input.ingress else {
                panic!()
            };
            write_handoff_v2(
                &mut server,
                &DataCollectHandoffV2::Message {
                    row,
                    transport_sequence: i as u64 + 1,
                },
            )
            .await
            .unwrap();
            assert_eq!(
                read_handoff_v2(&mut server).await.unwrap(),
                DataCollectHandoffV2::Received {
                    transport_sequence: i as u64 + 1
                }
            );
        }
        write_handoff_v2(
            &mut server,
            &DataCollectHandoffV2::End {
                last_confirmed_sequence: 6,
            },
        )
        .await
        .unwrap();
    });
    let mut progress = PublicOnlineProgressV1::default();
    let mut session = PublicOnlineSessionV1::start(client, &root, online_config(), &mut progress)
        .await
        .unwrap();
    let mut observed = Vec::new();
    let mut pm_positions = Vec::new();
    loop {
        let PublicOnlineNextV1::Applied(mut pending) =
            session.next_applied(std::future::pending()).await.unwrap()
        else {
            panic!()
        };
        let index = pending.input().input_position;
        observed.push(pending.applied().clone());
        let groups = PreparedPublicGroupV1::from_online(&mut pending);
        if index == 2 {
            assert_eq!(groups.len(), 2); // 身份组及原先等待配对的终态组，同一次 push 释放。
            assert_eq!(groups[0].group.member_input_positions, vec![2]);
            assert_eq!(groups[1].group.member_input_positions, vec![0, 1, 2]);
            assert!(groups.iter().all(|g| g.pm_inputs.is_empty()));
        } else if index == 3 || index == 4 {
            assert_eq!(groups.len(), 1);
            assert_eq!(groups[0].pm_inputs.len(), 1);
            pm_positions.push(groups[0].pm_inputs[0].0);
            assert!(matches!(
                groups[0].pm_inputs[0].1.message().payload,
                CanonicalPmPayloadV1::NewMarket { .. }
            ));
        } else {
            assert!(groups.is_empty());
        }
        for (i, group) in groups.iter().enumerate() {
            assert!(Arc::ptr_eq(
                &group.group,
                &pending.applied().closed_groups[i]
            ));
            assert_eq!(
                group.reference.applied_prefix.next_input_position,
                index + 1
            );
        }
        match pending.complete().await.unwrap() {
            PublicOnlineCompletedInputV1::Continue(next) => session = next,
            PublicOnlineCompletedInputV1::End(end) => {
                assert_eq!(end.last_received_transport_sequence, 6);
                break;
            }
        }
    }
    peer.await.unwrap();
    let mut reader = PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap();
    let mut canonical_count = 0;
    let mut reports = Vec::new();
    let mut duplicate = false;
    while let Some(row) = reader.next_row().unwrap() {
        match row {
            PublicStoredRowV1::Canonical {
                canonical_position,
                record,
                ..
            } => {
                assert_eq!(canonical_position, canonical_count);
                canonical_count += 1;
                if matches!(record, CanonicalEventRecordV2::PolymarketMessage(_)) {
                    assert!(pm_positions.contains(&canonical_position));
                }
            }
            PublicStoredRowV1::PmStepBinding(binding) => {
                assert_ne!(
                    Some(&binding.position.canonical_record_position),
                    pm_positions.last(),
                    "同代际第二个通知仅报告"
                );
            }
            PublicStoredRowV1::PmReport {
                canonical_position,
                report,
                ..
            } if Some(&canonical_position) == pm_positions.last() => {
                reports.push(report.reason_code);
            }
            PublicStoredRowV1::Report { report, .. } => {
                duplicate |= report.report.reason_code == "POLYMARKET_IDENTICAL_MESSAGE_RETRY";
            }
            _ => {}
        }
    }
    assert_eq!(pm_positions.len(), 2);
    assert_eq!(pm_positions[1], pm_positions[0] + 1);
    assert_eq!(reports, vec!["POLYMARKET_NEW_MARKET_IDENTITY_CONFIRMED"]);
    assert!(duplicate);
    let mut replay =
        data_pipeline_training::public_replay::PublicReplayV1::open(&root, 1, 1 << 20).unwrap();
    let mut replayed = Vec::new();
    while let Some((_, applied)) = replay.next_applied().unwrap() {
        replayed.push(applied.applied);
    }
    assert_eq!(observed, replayed);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_partial_fak_final_success_before_economics_drives_gap_without_success_pool() {
    use ::execution_normalization::{NormalizationRecord, SourceNeutralEvidenceKind};
    use contracts::execution_normalization::SourceNeutralTradeEconomicDispositionV1 as D;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_trading(&root, "4", false);
    let policy = delayed_policy("success");
    let (execution, observation) = execution(&temp.path().join("sim.jsonl"), &core, &policy);
    let opening = observation.opening_record().unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "visible-success".into());
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    let intent = Arc::new(ModelIntentV1::SubmitSingleBuy {
        condition_id: ConditionId(CONDITION_ID.into()),
        order: BuyLegIntentV1 {
            token_id: TokenId(UP_TOKEN_ID.into()),
            order: BuyOrderIntentV1::FakMarket {
                spend_pusd: Decimal::from(6),
                worst_price_pusd_per_share: Decimal::new(6, 1),
            },
        },
    });
    let receipt = intent_bridge::IntentBridge::bind_sim_group(&mut driver.decision().unwrap())
        .execute(context("quote10"), &intent)
        .await
        .unwrap();
    let mut records = Vec::new();
    while driver.next_group().unwrap().is_some() {
        records.extend(observation.take_closed_records());
    }
    records.extend(observation.take_closed_records());
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    ledger.apply_noe_memory_v1(&opening).unwrap();
    let mut registry = SharedExecutionRegistryV1::new(Arc::new(opening.context().clone()));
    let origin = registry.freeze("quote10".into(), intent);
    registry.record_sim_receipt(&origin, &receipt);
    let mut terminal = Vec::new();
    let mut pending = Vec::new();
    for record in &records {
        match record.record() {
            NormalizationRecord::Evidence {
                evidence:
                    SourceNeutralEvidenceKind::InitialMarket { .. }
                    | SourceNeutralEvidenceKind::MarketEnrollment { .. },
                ..
            } => {
                ledger.apply_noe_memory_v1(record).unwrap();
            }
            NormalizationRecord::Evidence {
                evidence: SourceNeutralEvidenceKind::TradeLifecycleV3 { result },
                ..
            } => {
                assert_eq!(result.result.base_quantity_shares, ExactDecimal("4".into()));
                if result.result.economic_disposition == D::FinalSuccess {
                    terminal.push(record);
                } else {
                    pending.push(record);
                }
            }
            _ => {
                registry.observe(record);
            }
        }
    }
    assert_eq!(
        registry.committed_target_shares(
            &ConditionId(CONDITION_ID.into()),
            &TokenId(UP_TOKEN_ID.into())
        ),
        ExecutionReadValueV1::Known(ExactDecimal("10".into()))
    );
    assert!(!terminal.is_empty());
    assert!(!pending.is_empty());
    for record in &terminal {
        ledger.apply_noe_memory_v1(record).unwrap();
        registry.observe(record);
    }
    let assert_visible = |ledger: &LedgerWriterV1, registry: &SharedExecutionRegistryV1| {
        let read = ledger.economic_read().unwrap();
        let totals = read
            .market_totals()
            .find(|m| m.condition_id.0 == CONDITION_ID)
            .unwrap();
        let filled = totals
            .assets
            .as_ref()
            .unwrap()
            .iter()
            .position(|a| a.token_id.0 == UP_TOKEN_ID)
            .unwrap();
        assert_eq!(totals.repair_gap_shares[filled], ExactDecimal("0".into()));
        assert_eq!(
            totals.repair_gap_shares[1 - filled],
            ExactDecimal("4".into())
        );
        assert_eq!(
            totals.success_shares,
            [ExactDecimal("0".into()), ExactDecimal("0".into())]
        );
        assert_eq!(totals.paired_shares, ExactDecimal("0".into()));
        assert_eq!(read.successful_trades().count(), 0);
        assert_eq!(
            registry.committed_target_shares(
                &ConditionId(CONDITION_ID.into()),
                &TokenId(UP_TOKEN_ID.into())
            ),
            ExecutionReadValueV1::Known(ExactDecimal("0".into()))
        );
    };
    assert_visible(&ledger, &registry);
    let before = ledger.projection_v2().unwrap();
    for record in &terminal {
        ledger.apply_noe_memory_v1(record).unwrap();
        registry.observe(record);
    }
    assert_eq!(ledger.projection_v2().unwrap(), before);
    for record in &pending {
        ledger.apply_noe_memory_v1(record).unwrap();
        registry.observe(record);
    }
    assert_visible(&ledger, &registry);
    // 最后才应用原经济引用；不改变来源时间，不伪造前驱或成功。
    let mut waiting = records.iter().collect::<Vec<_>>();
    while !waiting.is_empty() {
        let prior = waiting.len();
        waiting.retain(|record| {
            matches!(
                ledger.apply_noe_memory_v1(record).unwrap(),
                LedgerMemoryApplyOutcomeV1::Applied(
                    LedgerApplyOutcomeV1::MissingPlatformPredecessor { .. }
                ) | LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation
            )
        });
        assert!(waiting.len() < prior);
    }
    let read = ledger.economic_read().unwrap();
    let totals = read
        .market_totals()
        .find(|m| m.condition_id.0 == CONDITION_ID)
        .unwrap();
    let filled = totals
        .assets
        .as_ref()
        .unwrap()
        .iter()
        .position(|a| a.token_id.0 == UP_TOKEN_ID)
        .unwrap();
    assert_eq!(totals.success_shares[filled], ExactDecimal("4".into()));
    assert_eq!(totals.success_shares[1 - filled], ExactDecimal("0".into()));
    assert_eq!(totals.repair_gap_shares[filled], ExactDecimal("0".into()));
    assert_eq!(
        totals.repair_gap_shares[1 - filled],
        ExactDecimal("4".into())
    );
}

#[tokio::test(flavor = "current_thread")]
async fn v4_real_partial_cancel_and_zero_target_completion_preserve_economics() {
    use contracts::ExecutionReadValueV1::Known;
    for family in ["single", "paired", "market"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let (core, times) = recorded_trading_window(&root, "100", None, 60_000, true);
        let path = temp.path().join("sim.jsonl");
        let (mut execution, observation, _) =
            execution_with_rules_v4(&path, &core, SUCCESS_POLICY, None, "e".repeat(64), "100");
        execution
            .configure_cancel_failure_v1(CancelFailureScenarioV1 {
                targets: vec![
                    CancelFailureTargetV1 {
                        global_request_id: "finite-failure".into(),
                        platform_order_id: "sim-1".into(),
                    },
                    CancelFailureTargetV1 {
                        global_request_id: "post-close".into(),
                        platform_order_id: "sim-1".into(),
                    },
                ],
            })
            .unwrap();
        let mut ledger =
            LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
        execution
            .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
            .unwrap();
        let opening = observation.opening_record().unwrap();
        let mut shared = SharedExecutionV2::from_opening(ledger, observation.clone())
            .unwrap()
            .unwrap();
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "cancel-v4".into());
        driver.next_group().unwrap().unwrap();
        driver.next_group().unwrap().unwrap();
        shared.action_before(&mut driver).unwrap().unwrap();
        shared
            .execute(
                &mut driver.decision().unwrap(),
                context("empty-market"),
                Some(Arc::new(ModelIntentV1::CancelMarketOrders {
                    condition_id: ConditionId(CONDITION_ID.into()),
                })),
            )
            .await
            .unwrap()
            .unwrap();
        let pending = shared.action_before(&mut driver).unwrap().unwrap();
        assert_eq!(pending.cancellation_requests(None).pending_count, Known(1));
        for _ in 2..5 {
            driver.next_group().unwrap().unwrap();
        }
        let empty = shared.action_before(&mut driver).unwrap().unwrap();
        let response = empty.cancellation_requests(None);
        assert_eq!(response.pending_count, Known(0));
        assert_eq!(response.response_counts, [Known(0), Known(0), Known(0)]);
        assert!(matches!(response.latest_response, Known(Some(_))));
        let Known(Some(empty_completion)) = response.latest_response else {
            unreachable!()
        };
        shared
            .execute(
                &mut driver.decision().unwrap(),
                context("pair-after-empty"),
                Some(Arc::new(ModelIntentV1::SubmitPairedBuy {
                    condition_id: ConditionId(CONDITION_ID.into()),
                    orders: [leg(UP_TOKEN_ID, 5), leg(DOWN_TOKEN_ID, 5)],
                })),
            )
            .await
            .unwrap()
            .unwrap();
        for _ in 5..=6 {
            driver.next_group().unwrap().unwrap();
        }
        let before = shared.action_before(&mut driver).unwrap().unwrap();
        let orders: Vec<_> = before.orders().collect();
        assert_eq!(orders.len(), 2);
        assert_eq!(platform_id(orders[0].order_identity), "sim-1");
        assert!(orders.iter().all(|o| o.cancellable == Known(true)));
        let first = orders[0].order_identity.clone();
        let intent = match family {
            "single" => ModelIntentV1::CancelOrder {
                platform_order_id: "sim-1".into(),
            },
            "paired" => ModelIntentV1::CancelOrders {
                platform_order_ids: vec!["sim-1".into(), "sim-2".into()],
            },
            _ => ModelIntentV1::CancelMarketOrders {
                condition_id: ConditionId(CONDITION_ID.into()),
            },
        };
        let intent = Arc::new(intent);
        let receipt = shared
            .execute(
                &mut driver.decision().unwrap(),
                context("finite-failure"),
                Some(intent.clone()),
            )
            .await
            .unwrap()
            .unwrap();
        let duplicate = shared
            .execute(
                &mut driver.decision().unwrap(),
                context("finite-failure"),
                Some(intent),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt, duplicate);
        let pending = shared.action_before(&mut driver).unwrap().unwrap();
        assert_eq!(pending.cancellation_requests(None).pending_count, Known(1));
        let mut checked = false;
        while driver.next_group().unwrap().is_some() {
            let view = shared.action_before(&mut driver).unwrap().unwrap();
            let requests = view.cancellation_requests(None);
            if requests.response_counts[1] != Known(1) {
                continue;
            }
            assert_eq!(requests.pending_count, Known(0));
            assert_eq!(
                requests.response_counts,
                [Known(u64::from(family != "single")), Known(1), Known(0)]
            );
            let current = shared.registry().order(&first).unwrap();
            assert_eq!(current.cancellable, Known(true));
            assert_eq!(
                current.active_unfilled_shares,
                Known(ExactDecimal("5".into()))
            );
            assert_eq!(current.matched_shares, Known(ExactDecimal("0".into())));
            assert_eq!(
                before
                    .orders()
                    .filter(|o| o.cancellable == Known(true))
                    .count(),
                2,
                "旧快照不被撤单改写"
            );
            let funds = view
                .order_funds()
                .find(|f| f.order_identity == &first)
                .unwrap();
            let original_funds = before
                .order_funds()
                .find(|f| f.order_identity == &first)
                .unwrap();
            assert_eq!(
                funds.principal_encumbrance_pusd,
                original_funds.principal_encumbrance_pusd
            );
            assert_eq!(
                funds.immediate_fee_encumbrance_pusd,
                original_funds.immediate_fee_encumbrance_pusd
            );
            for request in ["explicit-again-a", "explicit-again-b"] {
                shared
                    .execute(
                        &mut driver.decision().unwrap(),
                        context(request),
                        Some(Arc::new(ModelIntentV1::CancelOrder {
                            platform_order_id: "sim-1".into(),
                        })),
                    )
                    .await
                    .unwrap()
                    .unwrap();
            }
            let overlapping = shared.action_before(&mut driver).unwrap().unwrap();
            assert_eq!(
                overlapping.cancellation_requests(None).pending_count,
                Known(2)
            );
            assert_eq!(
                overlapping
                    .cancellation_requests(Some(&CancellationScopeV1::Orders(vec!["sim-1"])))
                    .pending_count,
                Known(2)
            );
            assert_eq!(
                overlapping
                    .cancellation_requests(Some(&CancellationScopeV1::Orders(vec!["sim-2"])))
                    .pending_count,
                Known(0),
                "同市场不同ID不相交"
            );
            checked = true;
            break;
        }
        assert!(checked, "{family} 必须实际收到原响应");
        let mut closed = false;
        let mut completed = false;
        while driver.next_group().unwrap().is_some() {
            let view = shared.action_before(&mut driver).unwrap().unwrap();
            let current = shared.registry().order(&first).unwrap();
            if current.cancellable == Known(false) {
                assert_eq!(
                    current.active_unfilled_shares,
                    Known(ExactDecimal("0".into()))
                );
                if !closed {
                    shared
                        .execute(
                            &mut driver.decision().unwrap(),
                            context("post-close"),
                            Some(Arc::new(ModelIntentV1::CancelOrder {
                                platform_order_id: "sim-1".into(),
                            })),
                        )
                        .await
                        .unwrap()
                        .unwrap();
                    closed = true;
                } else if view.cancellation_requests(None).pending_count == Known(0) {
                    assert_eq!(
                        view.cancellation_requests(None).response_counts,
                        [Known(0), Known(1), Known(0)]
                    );
                    completed = true;
                }
            } else {
                assert!(!closed, "后到普通未撤成不能反转明确撤成");
            }
        }
        assert!(closed && completed);

        let before_reopen = shared.ledger().projection_v2();
        let cash_before_reopen = shared.ledger().economic_read().unwrap().cash().clone();
        driver.complete(&mut Materials { root }).unwrap();
        let mut restored = LedgerWriterV1::open_durable(temp.path().join("ledger")).unwrap();
        assert_eq!(restored.projection_v2(), before_reopen);
        assert_eq!(
            restored.economic_read().unwrap().cash(),
            &cash_before_reopen
        );
        assert!(matches!(
            restored.apply_noe_memory_v2(&opening).unwrap(),
            LedgerMemoryApplyOutcomeV1::Applied(LedgerApplyOutcomeV1::AlreadyApplied { .. })
        ));
        assert_eq!(
            restored.projection_v2(),
            before_reopen,
            "原引用重交不改变余额或经济序"
        );
        let reference = empty_completion.durable_reference().unwrap();
        assert!(observation.result_references().contains(reference.result()));
        assert_eq!(
            ::execution_normalization::read_durable_noe_record_position(reference.result())
                .unwrap(),
            empty_completion.position(),
        );
        let durable = ::execution_normalization::read_noe_result_v4(reference.result()).unwrap();
        let ::execution_normalization::NormalizationRecord::Evidence {
            evidence: ::execution_normalization::SourceNeutralEvidenceKind::Control {
                result: contracts::execution_normalization::SourceNeutralControlResult::CancellationRequestCompleted { result: persisted },
            }, ..
        } = durable.record else { panic!("零目标完成必须持久为原控制事实") };
        let ::execution_normalization::NormalizationRecord::Evidence {
            evidence: ::execution_normalization::SourceNeutralEvidenceKind::Control {
                result: contracts::execution_normalization::SourceNeutralControlResult::CancellationRequestCompleted { result: memory },
            }, ..
        } = empty_completion.record() else { unreachable!() };
        assert_eq!(&persisted, memory, "真实零目标响应持久与内存同一事实");
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let hits: Vec<_> = lines
            .iter()
            .filter_map(|line| line.pointer("/body/cancel_scenario_hits"))
            .flat_map(|v| v.as_array().unwrap())
            .collect();
        assert_eq!(hits.len(), 1, "只有实际失败分支记录一次命中：{text}");
        assert_eq!(hits[0]["platform_order_id"], "sim-1");
        assert_eq!(hits[0]["global_request_id"], "finite-failure");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn real_terminal_query_then_late_open_precede_lifecycle() {
    use ::execution_normalization::{NormalizationRecord, SourceNeutralEvidenceKind};
    use ExecutionReadValueV1::Known;
    use contracts::execution_normalization::*;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_trading_with_probes(
        &root,
        "2",
        None,
        400_000,
        2,
        "100",
        &[298_970, 299_006, 299_007, 299_008, 299_020],
    );
    let (execution, observation) = execution(&temp.path().join("sim.jsonl"), &core, SUCCESS_POLICY);
    let ledger =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "terminal-query".into());
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    let intent = Arc::new(ModelIntentV1::SubmitPairedBuy {
        condition_id: ConditionId(CONDITION_ID.into()),
        orders: [
            BuyLegIntentV1 {
                token_id: TokenId(UP_TOKEN_ID.into()),
                order: BuyOrderIntentV1::GtdLimit {
                    limit_price_pusd_per_share: Decimal::new(6, 1),
                    size_shares: Decimal::from(5),
                    post_only: false,
                    expires_at_utc_sec: 300,
                },
            },
            leg(DOWN_TOKEN_ID, 5),
        ],
    });
    shared
        .execute(
            &mut driver.decision().unwrap(),
            context("terminal-pair"),
            Some(intent),
        )
        .await
        .unwrap()
        .unwrap();
    let mut id = None;
    let mut terminal = None;
    let mut initial_funds = None;
    let mut checked = 0;
    while driver.next_group().unwrap().is_some() {
        let Some(view) = shared.action_before(&mut driver).unwrap() else {
            continue;
        };
        let at = view.cutoff().decision_time().logical_time_utc_ms();
        if at == 28_000 {
            id = Some(
                view.orders()
                    .find(|o| o.origin.as_ref().unwrap().batch_item_ordinal == 0)
                    .unwrap()
                    .order_identity
                    .clone(),
            );
        }
        if at == 299_970 {
            let Known(objects) = view.cancel_objects() else {
                panic!("完整成员")
            };
            assert!(
                objects
                    .iter()
                    .any(|o| matches!(o, CancelObjectReadV1::Paired { .. }))
            );
            initial_funds = Some(
                view.order_funds()
                    .find(|o| o.order_identity == id.as_ref().unwrap())
                    .unwrap()
                    .principal_encumbrance_pusd
                    .clone(),
            );
            assert_eq!(initial_funds, Some(Known(ExactDecimal("1.8".into()))));
            // 同一原固定种子/经验时延：Open 出站29/入站8，Expired 出站30/入站6。
            // 处理299999/300000，收到300007/300006；生命周期独立延迟，未改原图或采样器。
            for request in ["open-query-3645", "terminal-query-248140"] {
                driver
                    .decision()
                    .unwrap()
                    .request(BusinessRequestV2::Order(OrderRequestV2 {
                        context: context(request),
                        platform_order_id: platform_id(id.as_ref().unwrap()),
                    }))
                    .unwrap();
            }
        }
        if at == 300_007 || at == 300_008 {
            let id = id.as_ref().unwrap();
            let order = shared.registry().order(id).unwrap();
            let direct = order.latest_direct_feedback.unwrap();
            let NormalizationRecord::Evidence {
                evidence:
                    SourceNeutralEvidenceKind::Control {
                        result:
                            SourceNeutralControlResult::OrderQuery {
                                result: SourceNeutralOrderQueryResult::Found { order: snapshot },
                            },
                    },
                ..
            } = direct.record()
            else {
                panic!("真实 Found 查询: {:?}", direct.record())
            };
            assert_eq!(
                snapshot.state,
                if at == 300_007 {
                    SourceNeutralOrderStateV1::Expired
                } else {
                    SourceNeutralOrderStateV1::Open
                }
            );
            let SimV2SourceRecordReferenceV1::DirectResponse {
                platform_responded_at_utc_ms,
                response_received_at_utc_ms,
                ..
            } = direct.record().source_reference().record
            else {
                panic!("查询原直接响应来源")
            };
            assert_eq!(
                (platform_responded_at_utc_ms, response_received_at_utc_ms),
                if at == 300_007 {
                    (300_000, 300_006)
                } else {
                    (299_999, 300_007)
                }
            );
            assert!(
                !matches!(order.latest_lifecycle.unwrap().record(), NormalizationRecord::Evidence { evidence: SourceNeutralEvidenceKind::OrderLifecycle { result }, .. } if matches!(result.event, SourceNeutralOrderLifecycleEventV1::GtdExpired)),
                "终态生命周期尚未收到"
            );
            if at == 300_007 {
                terminal = Some(direct.clone());
            } else {
                assert!(direct.position() != terminal.as_ref().unwrap().position());
                let seq =
                    |r: &::execution_normalization::NoeConsumedRecordRefV1| match r.visibility() {
                        ::execution_normalization::NoeConsumptionVisibilityV1::Feedback {
                            delivery_sequence,
                            ..
                        } => *delivery_sequence,
                        _ => 0,
                    };
                assert!(
                    seq(direct) > seq(terminal.as_ref().unwrap()),
                    "晚Open实际具有更晚交付序号"
                );
            }
            assert_eq!(order.cancellable, Known(false));
            assert_eq!(
                order.active_unfilled_shares,
                Known(ExactDecimal("0".into()))
            );
            assert_eq!(
                view.orders()
                    .filter(|o| o.cancellable == Known(true))
                    .count(),
                1
            );
            let Known(objects) = view.cancel_objects() else {
                panic!("完整成员")
            };
            assert!(
                !objects
                    .iter()
                    .any(|o| matches!(o, CancelObjectReadV1::Paired { .. }))
            );
            assert_eq!(
                shared
                    .ledger()
                    .funds_read_v1()
                    .order(id)
                    .unwrap()
                    .principal_encumbrance_pusd,
                *initial_funds.as_ref().unwrap(),
                "查询不释放真实资金占用"
            );
            checked += 1;
        }
    }
    assert_eq!(checked, 2);
    let id = id.unwrap();
    // 仍用真实成交引用检验终态后关联接收与重复接收；不声称改写原成交接收时刻。
    let mut late =
        SharedExecutionRegistryV1::new(Arc::new(terminal.as_ref().unwrap().context().clone()));
    late.observe(&terminal.unwrap());
    let order = shared.registry().order(&id).unwrap();
    late.observe(order.accepted.unwrap());
    let trades: Vec<_> = shared.registry().order_trades(&id).cloned().collect();
    assert!(!trades.is_empty());
    for _ in 0..2 {
        for trade in &trades {
            late.observe(trade);
        }
    }
    assert_eq!(late.order(&id).unwrap().cancellable, Known(false));
    assert_eq!(
        late.order(&id).unwrap().matched_shares,
        order.matched_shares
    );
    assert_eq!(late.order_trades(&id).count(), trades.len());
}

#[tokio::test(flavor = "current_thread")]
async fn real_closed_market_prefix_stabilizes_zero_activity_before_finish() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_staggered_market_ends(&root);
    let path = temp.path().join("sim.jsonl");
    let (mut execution, observation) = execution(&path, &core, SUCCESS_POLICY);
    let opening = observation.opening_record().unwrap();
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    let ledger_root = temp.path().join("ledger");
    execution
        .bind_ledger_recording_v2(&mut ledger, &ledger_root)
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation.clone())
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "market-stability".into());
    let mut crossed_first = false;
    let mut crossed_second = false;
    let mut prior = shared.ledger().projection_v2();
    while driver.next_group().unwrap().is_some() {
        let Some(view) = shared.action_before(&mut driver).unwrap() else {
            continue;
        };
        let at = view.cutoff().decision_time().logical_time_utc_ms();
        if at == 32_000 || at == 42_000 {
            let recovered = LedgerWriterV1::open_durable(&ledger_root).unwrap();
            assert_eq!(
                recovered.projection_v2(),
                prior,
                "30/40秒原关闭前缀各自自动稳定此前Ledger；随后公共帧不倒灌"
            );
            assert!(opening.durable_reference().is_some());
            assert!(!std::fs::read_to_string(&path).unwrap().contains("RUN_SEAL"));
            if at == 32_000 {
                crossed_first = true;
            } else {
                crossed_second = true;
                break;
            }
        }
        prior = shared.ledger().projection_v2();
    }
    assert!(crossed_first && crossed_second);
    assert!(shared.registry().orders().next().is_none());
    let mut restored = LedgerWriterV1::open_durable(&ledger_root).unwrap();
    assert!(matches!(
        restored.apply_noe_memory_v1(&opening).unwrap(),
        LedgerMemoryApplyOutcomeV1::Applied(LedgerApplyOutcomeV1::AlreadyApplied { .. })
    ));
    driver.stop_and_drain().unwrap();
    assert!(!ledger_root.join("ledger-active-v1.jsonl").exists());
    assert!(!std::fs::read_to_string(path).unwrap().contains("RUN_SEAL"));
}

#[test]
fn v3_real_opening_stable_ledger_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let core = recorded_memory_complete(&temp.path().join("dpt"));
    let (mut execution, observation, _) = execution_with_rules(
        &temp.path().join("sim.jsonl"),
        &core,
        SUCCESS_POLICY,
        None,
        "e".repeat(64),
        "100",
    );
    let opening = observation.opening_record().unwrap();
    let mut ledger =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    let ledger_root = temp.path().join("ledger");
    execution
        .bind_ledger_recording_v2(&mut ledger, &ledger_root)
        .unwrap();
    let shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let before = shared.ledger().projection_v2();
    execution.stabilize_recording().unwrap();
    execution.stop_and_drain().unwrap();
    let reference = opening.durable_reference().unwrap();
    let view = ::execution_normalization::read_durable_noe_record_v1(reference.result()).unwrap();
    assert_eq!(view.position(), opening.position());
    assert_eq!(
        ::execution_normalization::read_durable_noe_record_position(reference.result()).unwrap(),
        opening.position(),
    );
    let mut restored = LedgerWriterV1::open_durable(&ledger_root).unwrap();
    assert_eq!(restored.projection_v2(), before);
    assert!(matches!(
        restored.apply_noe_memory_v1(&opening).unwrap(),
        LedgerMemoryApplyOutcomeV1::Applied(LedgerApplyOutcomeV1::AlreadyApplied { .. })
    ));
    assert_eq!(restored.projection_v2(), before);
}

#[test]
fn v4_real_opening_stable_ledger_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let core = recorded_memory_complete(&temp.path().join("dpt"));
    let (mut execution, observation, _) = execution_with_rules_v4(
        &temp.path().join("sim.jsonl"),
        &core,
        SUCCESS_POLICY,
        None,
        "e".repeat(64),
        "100",
    );
    let opening = observation.opening_record().unwrap();
    let mut ledger =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    let ledger_root = temp.path().join("ledger");
    execution
        .bind_ledger_recording_v2(&mut ledger, &ledger_root)
        .unwrap();
    let shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let before = shared.ledger().projection_v2();
    execution.stabilize_recording().unwrap();
    execution.stop_and_drain().unwrap();
    let reference = opening.durable_reference().unwrap();
    let view = ::execution_normalization::read_durable_noe_record_v2(reference.result()).unwrap();
    assert_eq!(view.position(), opening.position());
    assert_eq!(
        ::execution_normalization::read_durable_noe_record_position(reference.result()).unwrap(),
        opening.position(),
    );
    let mut restored = LedgerWriterV1::open_durable(&ledger_root).unwrap();
    assert_eq!(restored.projection_v2(), before);
    assert!(matches!(
        restored.apply_noe_memory_v2(&opening).unwrap(),
        LedgerMemoryApplyOutcomeV1::Applied(LedgerApplyOutcomeV1::AlreadyApplied { .. })
    ));
    assert_eq!(restored.projection_v2(), before);
}
