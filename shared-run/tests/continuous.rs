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
        matches!(
            incomplete.cancel_objects(),
            ExecutionReadValueV1::Unavailable
        ),
        "缺真实数量基点不能报告完整空集"
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
                    observed.push(at);
                }
                if at == 46_000 {
                    assert_eq!(
                        commitment,
                        ExecutionReadValueV1::Known(ExactDecimal(
                            if cancel { "4" } else { "10" }.into()
                        ))
                    );
                    let trade = shared.registry().orders().next().unwrap();
                    let records: Vec<_> = shared
                        .registry()
                        .order_trades(trade.order_identity)
                        .collect();
                    assert_eq!(records.len(), 1);
                    let ::execution_normalization::NormalizationRecord::Evidence {
                        evidence:
                            ::execution_normalization::SourceNeutralEvidenceKind::TradeLifecycle {
                                result,
                            },
                        ..
                    } = records[0].record()
                    else {
                        panic!("原成交引用")
                    };
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
                if at == 56_000 {
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
    formed.sort();
    assert_eq!(
        formed
            .iter()
            .map(|(_, id, ordinal)| (id.as_str(), *ordinal))
            .collect::<Vec<_>>(),
        vec![(r.as_str(), 1), (s.as_str(), 0), (r.as_str(), 0)]
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
                evidence: SourceNeutralEvidenceKind::TradeLifecycle { result },
                ..
            } => {
                order = Some(result.order_identity.clone());
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
