mod common;
use common::*;
use contracts::{economics::*, *};
use ledger::*;
use rust_decimal::Decimal;
use shared_run::*;
use sim_system::{execution::*, public_consumption::*, *};
use std::sync::Arc;

fn r(n: &str, d: &str) -> ExactRationalV1 {
    ExactRationalV1 {
        numerator: n.into(),
        denominator: d.into(),
    }
}
// 原代理延迟档最大值 29697ms，亦大于这些场景的成交/结算固定延迟。
// 显式有限脚本 v1：Δ=max(1ms, 所用延迟最大值)，顺序步骤相隔 2Δ。
const SETTLEMENT_DELTA_MS: u64 = 29_697;
fn script(case: usize) -> ResolutionRedeemScenarioV1 {
    use ResolutionRedeemScenarioEventV1::*;
    use SettlementQueryReplyV1 as Q;
    use SettlementSubmissionReceiptV1 as A;
    use SettlementTerminalV1 as T;
    let events = match case {
        0 | 5 | 6 | 8 => vec![
            SubmissionReceipt(A::Registered),
            Terminal(T::Success),
            RepeatPriorReceipt {
                prior_step_index: 1,
            },
        ],
        1 => vec![
            SubmissionReceipt(A::Unknown),
            ObservationTimeout,
            QueryReply(Q::AcceptedPending),
            Terminal(T::Success),
            RepeatPriorReceipt {
                prior_step_index: 0,
            },
        ],
        2 => vec![
            SubmissionReceipt(A::Unknown),
            ObservationTimeout,
            QueryReply(Q::FinalSuccess),
            Terminal(T::Success),
        ],
        3 => vec![
            SubmissionReceipt(A::Unknown),
            ObservationTimeout,
            QueryReply(Q::ConfirmedNoAcceptedOrSuccessfulTransaction),
            RetryAuthorized,
            SubmissionReceipt(A::Registered),
            Terminal(T::Success),
        ],
        4 => vec![
            SubmissionReceipt(A::Registered),
            Terminal(T::FailureNoEffect),
            RepeatPriorReceipt {
                prior_step_index: 1,
            },
        ],
        7 => vec![
            SubmissionReceipt(A::Unknown),
            ObservationTimeout,
            QueryReply(Q::Unknown),
            RetryAuthorized,
            Terminal(T::Success),
        ],
        _ => unreachable!(),
    };
    // 首个明确接纳：S1 的查询、S2 的查询成功、S3 的重试接纳。
    // Unknown 对照只在终态成功首次证明接纳。
    let accepted_offset = match case {
        1 | 2 => 4,
        3 | 7 => 8,
        _ => 0,
    } * SETTLEMENT_DELTA_MS;
    let mut scenario = ResolutionRedeemScenarioV1 {
        steps: events
            .into_iter()
            .enumerate()
            .map(|(i, event)| ResolutionRedeemScenarioStepV1 {
                offset_ms: i as u64 * 2 * SETTLEMENT_DELTA_MS,
                event,
            })
            .collect(),
        costs: if case == 4 || case == 5 {
            vec![]
        } else {
            vec![ScenarioSettlementCostV1 {
                correction_of: None,
                cost_identity: "gas-1".into(),
                recognition_offset_ms: accepted_offset,
                payment_offset_ms: Some(
                    accepted_offset
                        + if case == 6 {
                            0
                        } else {
                            2 * SETTLEMENT_DELTA_MS
                        },
                ),
                amount_pusd: ExactDecimal("0.1".into()),
            }]
        },
    };
    if case == 8 {
        scenario.costs.push(ScenarioSettlementCostV1 {
            correction_of: Some("gas-1".into()),
            cost_identity: "refund-1".into(),
            recognition_offset_ms: 6 * SETTLEMENT_DELTA_MS,
            payment_offset_ms: Some(8 * SETTLEMENT_DELTA_MS),
            amount_pusd: ExactDecimal("-0.1".into()),
        });
    }
    scenario
}
fn assert_first_visible_acceptance(
    case: usize,
    at: i64,
    read: &LedgerEconomicReadV2<'_>,
    first_accepted_at: &mut Option<i64>,
) {
    if case == 4 || case == 5 || first_accepted_at.is_some() {
        return;
    }
    let accepted = read
        .resolutions()
        .next()
        .and_then(|r| r.request)
        .is_some_and(|r| {
            matches!(
                r.observation,
                SettlementObservationV1::Registered
                    | SettlementObservationV1::QueryAcceptedPending
                    | SettlementObservationV1::QueryFinalSuccess
                    | SettlementObservationV1::TerminalSuccess
            )
        });
    let costs: Vec<_> = read.settlement_costs().collect();
    if !accepted {
        assert!(
            costs.is_empty(),
            "case {case}, t={at}: 首次接纳可见前不应确认费用"
        );
        return;
    }
    assert_eq!(
        costs.len(),
        1,
        "case {case}, t={at}: 首次接纳可见截点即有费用事实"
    );
    assert_eq!(costs[0].fact.recognized_pusd.0, "0.1");
    assert_eq!(
        costs[0].fact.paid_pusd.0,
        if case == 6 { "0.1" } else { "0" }
    );
    assert_eq!(
        read.market_totals()
            .next()
            .unwrap()
            .unpaid_settlement_fee_pusd
            .0,
        if case == 6 { "0" } else { "0.1" }
    );
    *first_accepted_at = Some(at);
    eprintln!(
        "case {case}: 首次接纳与费用同一公开截点 t={at}, recognized=0.1, paid={}",
        costs[0].fact.paid_pusd.0
    );
}
// 截点预期仅由已冻结脚本、原买入本金3与独立费用0.084计算；不由待测余额反推。
fn assert_settlement_cut(
    case: usize,
    elapsed: u64,
    read: &LedgerEconomicReadV2<'_>,
    seen: &mut std::collections::BTreeSet<u64>,
) {
    use ResolutionRedeemScenarioEventV1 as E;
    let scenario = script(case);
    let mut cuts: Vec<_> = scenario
        .steps
        .iter()
        .map(|s| s.offset_ms)
        .chain(scenario.costs.iter().flat_map(|c| {
            [Some(c.recognition_offset_ms), c.payment_offset_ms]
                .into_iter()
                .flatten()
        }))
        .collect();
    cuts.sort_unstable();
    cuts.dedup();
    let Some(&cut) = cuts
        .iter()
        .rev()
        .find(|&&t| t + SETTLEMENT_DELTA_MS <= elapsed)
    else {
        return;
    };
    if cuts.iter().any(|&t| t > cut && t <= elapsed) || !seen.insert(cut) {
        return;
    }
    let step = scenario
        .steps
        .iter()
        .rev()
        .find(|s| s.offset_ms <= cut)
        .unwrap();
    let expected_observation = match &step.event {
        E::SubmissionReceipt(SettlementSubmissionReceiptV1::Registered) => {
            SettlementObservationV1::Registered
        }
        E::SubmissionReceipt(SettlementSubmissionReceiptV1::Unknown) => {
            SettlementObservationV1::SubmissionUnknown
        }
        E::ObservationTimeout => SettlementObservationV1::ObservationTimeout,
        E::QueryReply(SettlementQueryReplyV1::AcceptedPending) => {
            SettlementObservationV1::QueryAcceptedPending
        }
        E::QueryReply(SettlementQueryReplyV1::FinalSuccess) => {
            SettlementObservationV1::QueryFinalSuccess
        }
        E::QueryReply(SettlementQueryReplyV1::ConfirmedNoAcceptedOrSuccessfulTransaction) => {
            SettlementObservationV1::QueryConfirmedNoTransaction
        }
        E::QueryReply(SettlementQueryReplyV1::Unknown) => SettlementObservationV1::QueryUnknown,
        E::RetryAuthorized if case == 3 => SettlementObservationV1::RetryAuthorized,
        E::RetryAuthorized => SettlementObservationV1::RetryNotAuthorized,
        E::Terminal(SettlementTerminalV1::Success) if case == 2 => {
            SettlementObservationV1::QueryFinalSuccess
        }
        E::Terminal(SettlementTerminalV1::Success) => SettlementObservationV1::TerminalSuccess,
        E::Terminal(SettlementTerminalV1::FailureNoEffect) => {
            SettlementObservationV1::TerminalFailureNoEffect
        }
        // 旧回执不回退已终局状态。
        E::RepeatPriorReceipt { .. } if case == 4 => {
            SettlementObservationV1::TerminalFailureNoEffect
        }
        E::RepeatPriorReceipt { .. } => SettlementObservationV1::TerminalSuccess,
    };
    let resolution = read.resolutions().next().unwrap();
    assert_eq!(
        resolution.request.unwrap().observation,
        expected_observation,
        "case {case}, cut {cut}"
    );
    assert_eq!(
        resolution.settlement_cost_attribution,
        SettlementCostAttributionV1::Directional
    );
    let success = scenario.steps.iter().any(|s| {
        s.offset_ms <= cut
            && matches!(
                s.event,
                E::Terminal(SettlementTerminalV1::Success)
                    | E::QueryReply(SettlementQueryReplyV1::FinalSuccess)
            )
    });
    let mut recognized = Decimal::ZERO;
    let mut paid = Decimal::ZERO;
    for cost in &scenario.costs {
        let amount = Decimal::from_str_exact(&cost.amount_pusd.0).unwrap();
        if cost.recognition_offset_ms <= cut {
            recognized += amount;
        }
        if cost.payment_offset_ms.is_some_and(|t| t <= cut) {
            paid += amount;
        }
    }
    let market = read.market_totals().next().unwrap();
    assert_eq!(
        Decimal::from_str_exact(&market.unpaid_settlement_fee_pusd.0).unwrap(),
        recognized - paid,
        "case {case}, cut {cut}"
    );
    assert_eq!(
        Decimal::from_str_exact(&read.cash().actual_available_pusd.0).unwrap(),
        Decimal::new(96916, 3)
            + if success {
                Decimal::from(5)
            } else {
                Decimal::ZERO
            }
            - paid,
        "case {case}, cut {cut}"
    );
    assert_eq!(read.cash().actual_reserved_pusd.0, "0");
    assert_eq!(
        market.winner_orphan_receivable_pusd.0,
        if success { "0" } else { "5" }
    );
    let nav = UnmarkedAccountEconomicsReadV3::from_ledger(read);
    // 正式判定/赎回的当前V2不叠加旧孤腿标记，费用和返佣沿同一原账本。
    let current = AccountEconomicsReadV3::from_ledger_and_observations(read, std::iter::empty());
    assert_eq!(
        current.conservative_nav_pusd,
        ExecutionReadValueV1::Known(nav.conservative_nav_pusd.clone())
    );
    assert_eq!(
        current.directional_residual_pusd,
        ExecutionReadValueV1::Known(nav.directional_residual_pusd.clone())
    );
    assert_eq!(
        current.worst_case_maker_nav_pusd,
        nav.worst_case_maker_nav_pusd
    );
    assert_eq!(nav.worst_case_maker_nav_pusd, r("24229", "250"));
    assert_eq!(
        nav.directional_residual_pusd,
        EconomicRational::from_decimal(Decimal::from(5) - recognized).public()
    );
    assert_eq!(nav.other_account_residual_pusd, r("0", "1"));
    assert_eq!(
        nav.conservative_nav_pusd,
        EconomicRational::from_decimal(Decimal::new(101916, 3) - recognized).public()
    );
    assert_eq!(
        EconomicRational::try_from(&nav.conservative_nav_pusd).unwrap(),
        EconomicRational::try_from(&nav.worst_case_maker_nav_pusd)
            .unwrap()
            .add(&EconomicRational::try_from(&nav.directional_residual_pusd).unwrap())
            .add(&EconomicRational::try_from(&nav.other_account_residual_pusd).unwrap())
    );
}
fn assert_all_settlement_cuts(case: usize, seen: &std::collections::BTreeSet<u64>) {
    let scenario = script(case);
    let expected: std::collections::BTreeSet<_> = scenario
        .steps
        .iter()
        .map(|s| s.offset_ms)
        .chain(scenario.costs.iter().flat_map(|c| {
            [Some(c.recognition_offset_ms), c.payment_offset_ms]
                .into_iter()
                .flatten()
        }))
        .collect();
    assert_eq!(
        seen, &expected,
        "case {case}: 每个独立等待/费用/终态截点均实际观察"
    );
}
#[tokio::test(flavor = "current_thread")]
async fn actual_chain_requested_settlement_s0_to_s4_and_stable_ledger() {
    for case in 0..=8 {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let (core, times) = recorded_settlement(&root);
        let (mut execution, observation) =
            execution(&temp.path().join("sim.jsonl"), &core, SUCCESS_POLICY);
        execution
            .configure_resolution_redeem_v1(script(case))
            .unwrap();
        let mut ledger = LedgerWriterV1::new_with_admission_v3(
            AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
        );
        let ledger_root = temp.path().join("ledger");
        execution
            .bind_ledger_recording_v2(&mut ledger, &ledger_root)
            .unwrap();
        let mut shared = SharedExecutionV1::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "economics".into());
        for _ in 0..4 {
            driver.next_group().unwrap().unwrap();
        }
        shared
            .execute(
                &mut driver.decision().unwrap(),
                RequestContextV2 {
                    global_request_id: "buy-five".into(),
                    caller_correlation_id: None,
                },
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
            .unwrap();
        let mut coordinator = SettlementCoordinatorV1::default();
        let mut observed_resolution = false;
        let mut requested_at = None;
        let mut seen_cuts = std::collections::BTreeSet::new();
        let mut first_accepted_at = None;
        while driver.next_group().unwrap().is_some() {
            if !shared
                .advance_settlement(&mut coordinator, &mut driver)
                .unwrap()
            {
                eprintln!(
                    "case {case}: no complete prefix at {:?}",
                    driver
                        .decision()
                        .map(|d| d.cutoff().decision_time().logical_time_utc_ms())
                );
                continue;
            }
            let at = driver
                .decision()
                .unwrap()
                .cutoff()
                .decision_time()
                .logical_time_utc_ms();
            if requested_at.is_none()
                && driver
                    .settlement_simulation_read(&ConditionId(CONDITION_ID.into()))
                    .is_some()
            {
                requested_at = Some(at);
            }
            let read = shared.ledger().economic_read().unwrap();
            assert_first_visible_acceptance(case, at, &read, &mut first_accepted_at);
            if let Some(start) = requested_at {
                assert_settlement_cut(case, (at - start) as u64, &read, &mut seen_cuts);
            }
            for resolution in read.resolutions() {
                observed_resolution = true;
                assert_eq!(
                    resolution.settlement_cost_attribution,
                    SettlementCostAttributionV1::Directional
                );
            }
        }
        assert!(shared.apply_closed().unwrap());
        assert_all_settlement_cuts(case, &seen_cuts);
        assert_eq!(first_accepted_at.is_some(), case != 4 && case != 5);
        assert!(
            observed_resolution,
            "case {case}: true qualified resolution consumed"
        );
        let read = shared.ledger().economic_read().unwrap();
        let market = read
            .market_totals()
            .find(|m| m.condition_id.0 == CONDITION_ID)
            .unwrap();
        // 原费用核的独立数值：5 * .07 * .6 * .4 = .084。
        assert_eq!(market.executed[1].actual_fee_pusd.0, "0.084", "case {case}");
        assert_eq!(
            market.confirmed_net_pnl_pusd,
            if case == 4 || case == 5 || case == 8 {
                r("479", "250")
            } else {
                r("227", "125")
            },
            "case {case}"
        );
        assert_eq!(
            read.cash().actual_available_pusd.0,
            if case == 4 {
                "96.916"
            } else if case == 5 || case == 8 {
                "101.916"
            } else {
                "101.816"
            },
            "case {case}"
        );
        assert_eq!(
            market.winner_orphan_receivable_pusd.0,
            if case == 4 { "5" } else { "0" },
            "case {case}"
        );
        let stats = driver
            .settlement_simulation_read(&ConditionId(CONDITION_ID.into()))
            .unwrap();
        assert_eq!(
            stats.sends,
            if case == 3 { 2 } else { 1 },
            "case {case}: physical sends"
        );
        assert_eq!(
            stats.queries,
            if (1..=3).contains(&case) || case == 7 {
                1
            } else {
                0
            },
            "case {case}: queries"
        );
        assert_eq!(stats.final_success, case != 4);
        let nav = UnmarkedAccountEconomicsReadV3::from_ledger(&read);
        assert_eq!(nav.worst_case_maker_nav_pusd, r("24229", "250"));
        assert_eq!(
            nav.directional_residual_pusd,
            if case == 4 || case == 5 || case == 8 {
                r("5", "1")
            } else {
                r("49", "10")
            }
        );
        assert_eq!(
            nav.conservative_nav_pusd,
            if case == 4 || case == 5 || case == 8 {
                r("25479", "250")
            } else {
                r("12727", "125")
            }
        );
        assert_eq!(
            EconomicRational::try_from(&nav.conservative_nav_pusd).unwrap(),
            EconomicRational::try_from(&nav.worst_case_maker_nav_pusd)
                .unwrap()
                .add(&EconomicRational::try_from(&nav.directional_residual_pusd).unwrap())
                .add(&EconomicRational::try_from(&nav.other_account_residual_pusd).unwrap())
        );
        let expected_cash = read.cash().clone();
        let expected_market = market;
        drop(read);
        if case == 4 {
            driver.stop_and_drain().unwrap();
        } else {
            driver.complete().unwrap();
        }
        let recovered = LedgerWriterV1::open_durable(&ledger_root).unwrap();
        let recovered = recovered.economic_read().unwrap();
        assert_eq!(recovered.cash(), &expected_cash);
        assert_eq!(
            recovered
                .market_totals()
                .find(|m| m.condition_id.0 == CONDITION_ID)
                .unwrap(),
            expected_market
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn real_partial_makers_fix_average_cost_and_nonterminating_fraction() {
    for fractional in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let (core, times) = recorded_average_pool(&root, fractional);
        let (mut execution, observation) =
            execution(&temp.path().join("sim.jsonl"), &core, SUCCESS_POLICY);
        let mut ledger = LedgerWriterV1::new_with_admission_v3(
            AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
        );
        execution
            .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
            .unwrap();
        let mut shared = SharedExecutionV1::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "average".into());
        for _ in 0..4 {
            driver.next_group().unwrap().unwrap();
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
                break;
            }
        }
        assert!(shared.apply_closed().unwrap());
        let read = shared.ledger().economic_read().unwrap();
        let m = read.market_totals().next().unwrap();
        let up = m
            .assets
            .as_ref()
            .unwrap()
            .iter()
            .position(|a| a.token_id.0 == UP_TOKEN_ID)
            .unwrap();
        // 由原Maker成交与公共池配平产生贡献，不在测试重建配平器。
        let mut unpaired = EconomicRational::zero();
        for trade in read.trades().filter(|t| t.fact.token_id.0 == UP_TOKEN_ID) {
            assert!(trade.economic_applied && trade.success_applied);
            let ExecutionReadValueV1::Known(shares) = trade.unpaired_contribution_shares else {
                panic!("新事务归属应已可读")
            };
            let shares = EconomicRational::try_from(&shares).unwrap();
            unpaired = unpaired.add(&shares);
            let original =
                EconomicRational::from_decimal(trade.fact.base_quantity_shares.0.parse().unwrap());
            assert!(shares < original, "两个Up原成交都沿同一池比例配平");
        }
        assert_eq!(
            unpaired.public(),
            r(if fractional { "2" } else { "1" }, "1")
        );
        assert_eq!(m.paired_shares.0, "1");
        assert_eq!(m.success_principal_cost_pusd[up], r("1", "1"));
        assert_eq!(m.success_principal_cost_pusd[1 - up], r("1", "5"));
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

#[test]
fn actual_opening_dual_cash_atomic_sum_and_original_sticky_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let dpt_root = temp.path().join("dpt");
    let core = recorded_memory_complete(&dpt_root);
    let (mut execution, observation) = execution_with_seed_and_cash(
        &temp.path().join("sim.jsonl"),
        &core,
        SUCCESS_POLICY,
        None,
        "e".repeat(64),
        "2000",
    );
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    let ledger_root = temp.path().join("ledger");
    execution
        .bind_ledger_recording_v2(&mut ledger, &ledger_root)
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let before = shared.ledger().projection_v2().unwrap().ledger_entry_seq;
    let intent = |id: &str, principal: &str| ReserveIntentV1 {
        request_identity: LedgerRequestIdentityV1(id.into()),
        condition_id: ConditionId(CONDITION_ID.into()),
        requested_principal_pusd: ExactDecimal(principal.into()),
        immediate_fee_reserve_pusd: ExactDecimal("0".into()),
    };
    let rejected = [intent("sum-a", "1200"), intent("sum-b", "900")];
    assert_eq!(
        shared.reserve_action_v2(&rejected).unwrap(),
        vec![LedgerReservationOutcomeV1::InsufficientGlobalAvailableFunds; 2]
    );
    assert_eq!(
        shared.ledger().projection_v2().unwrap().ledger_entry_seq,
        before
    );
    assert_eq!(
        shared
            .ledger()
            .economic_read()
            .unwrap()
            .cash()
            .actual_available_pusd
            .0,
        "2000"
    );
    assert!(matches!(
        shared
            .reserve_action_v2(&[intent("above-old-limit", "1200")])
            .unwrap()[0],
        LedgerReservationOutcomeV1::Reserved { .. }
    ));
    let cash = shared.ledger().economic_read().unwrap().cash().clone();
    assert_eq!(cash.actual_available_pusd.0, "800");
    assert_eq!(cash.actual_reserved_pusd.0, "1200");
    execution.stop_and_drain().unwrap();
    let mut restored = LedgerWriterV1::open_durable(&ledger_root).unwrap();
    assert_eq!(restored.economic_read().unwrap().cash(), &cash);
    assert_eq!(
        restored.try_reserve_action_v2(&rejected).unwrap(),
        vec![LedgerReservationOutcomeV1::InsufficientGlobalAvailableFunds; 2]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn actual_two_market_maker_day_allocates_exact_contribution_ratios() {
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
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let initial = shared.ledger().necessary_read_v2().unwrap();
    assert_eq!(initial.account().confirmed_net_pnl.public(), r("0", "1"));
    assert_eq!(
        initial.account().confirmed_unpaid_rebate.public(),
        r("0", "1")
    );
    assert_eq!(initial.funds_markets().count(), 0);
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
        // 原完整读取与增量读取必须沿同一经济前缀；初始化在成交前，覆盖日期更新及支付。
        let totals = shared.ledger().necessary_read_v2().unwrap();
        let read = shared.ledger().economic_read().unwrap();
        let mut pnl = EconomicRational::zero();
        let mut unpaid = EconomicRational::zero();
        for m in read.market_totals() {
            pnl = pnl.add(&EconomicRational::try_from(&m.confirmed_net_pnl_pusd).unwrap());
            for b in m.rebates {
                unpaid = unpaid.add(&EconomicRational::try_from(&b.confirmed_unpaid_pusd).unwrap());
            }
        }
        assert_eq!(totals.account().confirmed_net_pnl, pnl);
        assert_eq!(totals.account().confirmed_unpaid_rebate, unpaid);
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
        assert_eq!(market.confirmed_net_pnl_pusd, expected);
        assert_eq!(market.rebates[0].confirmed_unpaid_pusd, r("0", "1"));
    }
    let totals = shared.ledger().necessary_read_v2().unwrap();
    assert_eq!(totals.account().confirmed_net_pnl.public(), r("1", "1"));
    assert_eq!(
        totals.account().confirmed_unpaid_rebate.public(),
        r("0", "1")
    );
    let expected_totals = totals.account().clone();
    let expected_markets: Vec<_> = read.market_totals().collect();
    driver.stop_and_drain().unwrap();
    let restored = LedgerWriterV1::open_durable(temp.path().join("ledger")).unwrap();
    assert_eq!(
        restored.necessary_read_v2().unwrap().account(),
        &expected_totals
    );
    let mut checkpoint = Vec::new();
    restored.write_checkpoint(&mut checkpoint).unwrap();
    let restored_checkpoint = LedgerWriterV1::restore_checkpoint(checkpoint.as_slice()).unwrap();
    assert_eq!(
        restored_checkpoint.necessary_read_v2().unwrap().account(),
        &expected_totals
    );
    let read = restored.economic_read().unwrap();
    assert_eq!(read.market_totals().collect::<Vec<_>>(), expected_markets);
    let day = read
        .rebate_days()
        .find(|day| day.kind == RebateKindV1::Maker)
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
    assert_old_day_snapshots_accept_new_payment(
        &temp.path().join("ledger"),
        &temp.path().join("legacy-ledger"),
    );
}

// 从本场景真实落盘的交易生成旧 days 完整快照布局，保留实际来源与经济字段。
// 第一次支付及其后续记录继续使用当前增量布局，验证旧前缀可继续读入新记录。
fn assert_old_day_snapshots_accept_new_payment(
    current_root: &std::path::Path,
    legacy_root: &std::path::Path,
) {
    use std::io::Write;
    let mut manifests: Vec<_> = std::fs::read_dir(current_root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .ends_with(".manifest.json")
        })
        .collect();
    manifests.sort();
    let mut days = std::collections::BTreeMap::<String, serde_json::Value>::new();
    let mut old_prefix = Vec::new();
    let mut new_tail = Vec::new();
    let mut snapshots = 0;
    let mut payment_seen = false;
    for manifest in manifests {
        let reference: LedgerSealedSegmentReferenceV1 =
            serde_json::from_slice(&std::fs::read(manifest).unwrap()).unwrap();
        ::execution_normalization::record_file::visit(
            std::path::Path::new(&reference.location), "ledger_arrow_parquet_v1", |(seq, kind, bytes)| {
                let mut body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                if let Some(patch) = body.get_mut("economics").and_then(|v| v.as_object_mut()) {
                    let updates = patch["day_updates"].as_array().unwrap();
                    payment_seen |= updates.iter().any(|entry| entry[1].get("Paid").is_some());
                    if !payment_seen {
                        let updates = patch.remove("day_updates").unwrap();
                        let mut full_days = Vec::new();
                        for entry in updates.as_array().unwrap() {
                            let key = &entry[0];
                            let update = &entry[1];
                            let day = days.entry(key.to_string()).or_insert_with(|| serde_json::json!({
                                "kind": key[0], "day": key[1], "contributions": [], "result": null, "paid": "0"
                            }));
                            if let Some(additions) = update.get("Contributions").and_then(|v| v.get("additions")) {
                                day["contributions"].as_array_mut().unwrap().extend(additions.as_array().unwrap().iter().cloned());
                            }
                            if let Some(result) = update.get("Finalized").and_then(|v| v.get("result")) { day["result"] = result.clone(); }
                            full_days.push(serde_json::json!([key, day]));
                            snapshots += 1;
                        }
                        patch.insert("days".into(), full_days.into());
                        patch.insert("allocations".into(), serde_json::json!([]));
                        for market in patch["markets"].as_array_mut().unwrap() {
                            for pool in market[1]["pools"].as_array_mut().unwrap() {
                                pool.as_object_mut().unwrap().insert("contributors".into(), serde_json::json!([]));
                            }
                        }
                    }
                }
                let mut line = serde_json::to_vec(&serde_json::json!({
                    "version": 2, "encoding": "ledger_transaction_segment_v2",
                    "durable_record_seq": seq, "payload": {"kind": kind, "body": body}
                })).unwrap();
                line.push(b'\n');
                if payment_seen { new_tail.extend(line); } else { old_prefix.extend(line); }
                Ok(())
            }
        ).unwrap();
    }
    assert!(snapshots >= 2);
    assert!(payment_seen);
    std::fs::create_dir(legacy_root).unwrap();
    let active = legacy_root.join("ledger-active-v1.jsonl");
    // 旧完整快照缺实际贡献材料时，不能被误解为只有 paid 的新操作。
    let broken_root = legacy_root.with_extension("missing-contributions");
    std::fs::create_dir(&broken_root).unwrap();
    let mut broken = Vec::new();
    let mut removed = false;
    for line in old_prefix
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
    {
        let mut row: serde_json::Value = serde_json::from_slice(line).unwrap();
        if !removed {
            if let Some(days) = row["payload"]["body"]["economics"]["days"].as_array_mut() {
                if let Some(day) = days.first_mut() {
                    day[1].as_object_mut().unwrap().remove("contributions");
                    removed = true;
                }
            }
        }
        serde_json::to_writer(&mut broken, &row).unwrap();
        broken.push(b'\n');
    }
    assert!(removed);
    std::fs::write(broken_root.join("ledger-active-v1.jsonl"), broken).unwrap();
    assert!(LedgerWriterV1::open_durable(&broken_root).is_err());
    std::fs::write(&active, old_prefix).unwrap();
    let before = LedgerWriterV1::open_durable(legacy_root).unwrap();
    let read = before.economic_read().unwrap();
    let day = read
        .rebate_days()
        .find(|day| day.kind == RebateKindV1::Maker)
        .unwrap();
    assert_eq!(day.contributions.len(), 2);
    assert_eq!(day.amounts.confirmed_unpaid_pusd, r("1", "1"));
    assert_eq!(day.amounts.paid_pusd, r("0", "1"));
    drop(before);
    let mut append = std::fs::OpenOptions::new()
        .append(true)
        .open(active)
        .unwrap();
    append.write_all(&new_tail).unwrap();
    append.sync_all().unwrap();
    drop(append);
    let after = LedgerWriterV1::open_durable(legacy_root).unwrap();
    let read = after.economic_read().unwrap();
    let day = read
        .rebate_days()
        .find(|day| day.kind == RebateKindV1::Maker)
        .unwrap();
    assert_eq!(day.contributions.len(), 2);
    assert_eq!(day.amounts.confirmed_unpaid_pusd, r("0", "1"));
    assert_eq!(day.amounts.paid_pusd, r("1", "1"));
    assert_eq!(day.amounts.locked_remainder_pusd, r("3", "50000"));
}

#[tokio::test(flavor = "current_thread")]
async fn original_external_cash_identity_correction_and_no_frame_nav() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_settlement(&root);
    let (mut execution, observation) =
        execution(&temp.path().join("sim.jsonl"), &core, SUCCESS_POLICY);
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    let ledger_root = temp.path().join("ledger");
    execution
        .bind_ledger_recording_v2(&mut ledger, &ledger_root)
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "external".into());
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    let facts = [
        (
            "unknown-credit",
            "7",
            ExternalCashAttributionV1::UnknownActivity,
            None,
        ),
        (
            "unknown-debit",
            "-3",
            ExternalCashAttributionV1::UnknownActivity,
            None,
        ),
        (
            "reverse-credit",
            "-7",
            ExternalCashAttributionV1::UnknownActivity,
            Some("unknown-credit"),
        ),
        ("deposit", "2", ExternalCashAttributionV1::Deposit, None),
    ];
    for (id, amount, attribution, correction) in facts {
        let fact = ExternalCashFactV1 {
            identity: id.into(),
            signed_delta_pusd: ExactDecimal(amount.into()),
            attribution,
            correction_of: correction.map(str::to_owned),
        };
        driver
            .decision()
            .unwrap()
            .apply_external_cash_fact_v1(fact.clone())
            .unwrap();
        driver
            .decision()
            .unwrap()
            .apply_external_cash_fact_v1(fact)
            .unwrap();
    }
    while driver.next_group().unwrap().is_some() {
        shared.apply_closed().unwrap();
    }
    assert!(shared.apply_closed().unwrap());
    let nav =
        UnmarkedAccountEconomicsReadV3::from_ledger(&shared.ledger().economic_read().unwrap());
    assert_eq!(nav.conservative_nav_pusd, r("99", "1"));
    assert_eq!(nav.worst_case_maker_nav_pusd, r("97", "1"));
    assert_eq!(nav.other_account_residual_pusd, r("2", "1"));
    driver.complete().unwrap();
    let restored = LedgerWriterV1::open_durable(&ledger_root).unwrap();
    assert_eq!(
        UnmarkedAccountEconomicsReadV3::from_ledger(&restored.economic_read().unwrap()),
        nav
    );
}

#[tokio::test(flavor = "current_thread")]
async fn resolution_before_late_success_fixes_fee_bucket_and_updates_pi_without_frames() {
    for late in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let (core, times) = recorded_settlement(&root);
        let policy = br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10000},{"state":"CONFIRMED","offset_ms":20000}],"economic_application_ordinal":1},"failure_overrides":[]}"#;
        let (mut execution, observation) = execution_with_seed(
            &temp.path().join("sim.jsonl"),
            &core,
            policy,
            None,
            "00000000000000000000000000000000000000000000000000000000000023c8".into(),
        );
        execution.configure_resolution_redeem_v1(script(0)).unwrap();
        let mut ledger = LedgerWriterV1::new_with_admission_v3(
            AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
        );
        let ledger_root = temp.path().join("ledger");
        execution
            .bind_ledger_recording_v2(&mut ledger, &ledger_root)
            .unwrap();
        let mut shared = SharedExecutionV1::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "late".into());
        for _ in 0..4 {
            driver.next_group().unwrap().unwrap();
        }
        let mut sent_b = false;
        let mut coordinator = SettlementCoordinatorV1::default();
        let mut saw_pending_resolution = false;
        let mut saw_pair = false;
        let mut saw_unseen_trade = false;
        let mut saw_unpaid_fee = false;
        loop {
            let at = driver
                .decision()
                .unwrap()
                .cutoff()
                .decision_time()
                .logical_time_utc_ms();
            for (token, id) in [(UP_TOKEN_ID, "first-a"), (DOWN_TOKEN_ID, "later-b")] {
                if (token == UP_TOKEN_ID && at != 22000)
                    || (token == DOWN_TOKEN_ID && (sent_b || (late && at < 56000)))
                {
                    continue;
                }
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
                                    limit_price_pusd_per_share: Decimal::new(6, 1),
                                    size_shares: Decimal::from(5),
                                    post_only: false,
                                },
                            },
                        })),
                    )
                    .await
                    .unwrap();
                if token == DOWN_TOKEN_ID {
                    sent_b = true;
                }
            }
            // 原 Accepted received=56031；首成交 received=66832、MINED=66022。
            // 66000 组仍在经济反馈之前，71000 已能领取 PendingApplied。
            if late && at == 66_000 {
                assert!(shared.apply_closed().unwrap());
                let read = shared.ledger().economic_read().unwrap();
                let resolution = read.resolutions().next().unwrap();
                assert_eq!(
                    resolution.economic_predecessors_closed,
                    ExecutionReadValueV1::Known(true)
                );
                assert_eq!(resolution.winner_orphan_receivable_pusd.0, "5");
                assert!(resolution.request.is_none());
                let commitment = shared
                    .registry()
                    .all_commitments()
                    .find(|c| c.origin.intent.global_request_id() == "later-b")
                    .unwrap();
                assert_eq!(
                    commitment.remaining_target_shares,
                    ExecutionReadValueV1::Unavailable
                );
                let order = shared
                    .registry()
                    .orders()
                    .find(|o| {
                        o.origin
                            .as_ref()
                            .is_some_and(|o| o.intent.global_request_id() == "later-b")
                    })
                    .unwrap();
                assert!(order.accepted.is_some());
                // Accepted 已说明匹配 5，但首个成交事实尚未公开，不能据此闭合承诺。
                assert_eq!(
                    order.matched_shares,
                    ExecutionReadValueV1::Known(ExactDecimal("5".into()))
                );
                assert_eq!(
                    read.market_totals().next().unwrap().executed[1].shares.0,
                    "5"
                );
                assert!(
                    driver
                        .settlement_simulation_read(&ConditionId(CONDITION_ID.into()))
                        .is_none()
                );
                saw_unseen_trade = true;
            }
            if shared
                .advance_settlement(&mut coordinator, &mut driver)
                .unwrap()
            {
                let read = shared.ledger().economic_read().unwrap();
                for resolution in read.resolutions() {
                    assert_eq!(
                        resolution.settlement_cost_attribution,
                        if late {
                            SettlementCostAttributionV1::Directional
                        } else {
                            SettlementCostAttributionV1::Maker
                        }
                    );
                    if late && resolution.winner_orphan_receivable_pusd.0 == "5" {
                        assert_eq!(resolution.paired_receivable_pusd.0, "0");
                        if at == 66_000 {
                            assert!(
                                driver
                                    .settlement_simulation_read(&ConditionId(CONDITION_ID.into()))
                                    .is_none()
                            );
                        }
                        assert!(resolution.request.is_none());

                        saw_pending_resolution = true;
                    }
                    if resolution.paired_receivable_pusd.0 == "5" {
                        saw_pair = true;
                    }
                }
                let nav = UnmarkedAccountEconomicsReadV3::from_ledger(&read);
                if read
                    .market_totals()
                    .next()
                    .unwrap()
                    .unpaid_settlement_fee_pusd
                    .0
                    == "0.1"
                {
                    // 两腿本金6、Taker费0.168，配对应收5，结算费用0.1尚未付款。
                    assert_eq!(read.cash().actual_available_pusd.0, "93.832");
                    assert_eq!(nav.conservative_nav_pusd, r("24683", "250"));
                    assert_eq!(
                        nav.worst_case_maker_nav_pusd,
                        if late {
                            r("12354", "125")
                        } else {
                            r("24683", "250")
                        }
                    );
                    assert_eq!(
                        nav.directional_residual_pusd,
                        if late { r("-1", "10") } else { r("0", "1") }
                    );
                    saw_unpaid_fee = true;
                }
                assert_eq!(
                    EconomicRational::try_from(&nav.conservative_nav_pusd).unwrap(),
                    EconomicRational::try_from(&nav.worst_case_maker_nav_pusd)
                        .unwrap()
                        .add(&EconomicRational::try_from(&nav.directional_residual_pusd).unwrap())
                        .add(
                            &EconomicRational::try_from(&nav.other_account_residual_pusd).unwrap()
                        )
                );
            }
            if driver.next_group().unwrap().is_none() {
                break;
            }
        }
        assert!(shared.apply_closed().unwrap());
        assert_eq!(saw_pending_resolution, late);
        assert_eq!(saw_unseen_trade, late);
        assert!(saw_unpaid_fee);
        let stats = driver
            .settlement_simulation_read(&ConditionId(CONDITION_ID.into()))
            .unwrap();
        assert_eq!(stats.sends, 1);
        assert!(stats.final_success);
        assert!(saw_pair);
        let read = shared.ledger().economic_read().unwrap();
        let nav = UnmarkedAccountEconomicsReadV3::from_ledger(&read);
        assert_eq!(nav.conservative_nav_pusd, r("24683", "250"));
        assert_eq!(
            nav.worst_case_maker_nav_pusd,
            if late {
                r("12354", "125")
            } else {
                r("24683", "250")
            }
        );
        assert_eq!(
            nav.directional_residual_pusd,
            if late { r("-1", "10") } else { r("0", "1") }
        );
        let market = read.market_totals().next().unwrap();
        assert_eq!(market.confirmed_net_pnl_pusd, r("-317", "250"));
        assert_eq!(market.maximum_confirmed_net_loss_pusd, r("317", "250"));
        driver.complete().unwrap();
        let restored = LedgerWriterV1::open_durable(&ledger_root).unwrap();
        assert_eq!(
            UnmarkedAccountEconomicsReadV3::from_ledger(&restored.economic_read().unwrap()),
            nav
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn stable_request_material_queries_pending_success_and_controlled_retry() {
    use ::execution_normalization::{MemoryIdentityKeyV1, SourceNeutralEntityKeyV1};
    for case in [1, 2, 3] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let (core, times) = recorded_settlement(&root);
        let (mut execution, observation) =
            execution(&temp.path().join("sim.jsonl"), &core, SUCCESS_POLICY);
        execution
            .configure_resolution_redeem_v1(script(case))
            .unwrap();
        let mut ledger = LedgerWriterV1::new_with_admission_v3(
            AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
        );
        let ledger_root = temp.path().join("ledger");
        execution
            .bind_ledger_recording_v2(&mut ledger, &ledger_root)
            .unwrap();
        let mut shared = SharedExecutionV1::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "stable-query".into());
        for _ in 0..4 {
            driver.next_group().unwrap().unwrap();
        }
        shared
            .execute(
                &mut driver.decision().unwrap(),
                RequestContextV2 {
                    global_request_id: "buy-five".into(),
                    caller_correlation_id: None,
                },
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
            .unwrap();
        let mut coordinator = SettlementCoordinatorV1::default();
        let mut restored_request = None;
        let mut retried = false;
        let mut requested_at = None;
        let mut seen_cuts = std::collections::BTreeSet::new();
        let mut first_accepted_at = None;
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
            assert_first_visible_acceptance(
                case,
                at,
                &shared.ledger().economic_read().unwrap(),
                &mut first_accepted_at,
            );
            if let Some(start) = requested_at {
                assert_settlement_cut(
                    case,
                    (at - start) as u64,
                    &shared.ledger().economic_read().unwrap(),
                    &mut seen_cuts,
                );
            }
            if restored_request.is_none() {
                let has_request = shared
                    .ledger()
                    .economic_read()
                    .unwrap()
                    .resolutions()
                    .any(|r| r.request.is_some());
                if !has_request {
                    shared
                        .advance_settlement(&mut coordinator, &mut driver)
                        .unwrap();
                    if requested_at.is_none()
                        && driver
                            .settlement_simulation_read(&ConditionId(CONDITION_ID.into()))
                            .is_some()
                    {
                        requested_at = Some(at);
                    }
                    continue;
                }
                // 只排空原记录材料；模拟后端继续保有原状态，没有恢复训练账户。
                driver.stabilize_recording().unwrap();
                let restored = LedgerWriterV1::open_durable(&ledger_root).unwrap();
                let read = restored.economic_read().unwrap();
                let resolution = read.resolutions().next().unwrap();
                let fact = resolution.request.unwrap();
                assert_eq!(
                    resolution.settlement_cost_attribution,
                    SettlementCostAttributionV1::Directional
                );
                let MemoryIdentityKeyV1::Entity(SourceNeutralEntityKeyV1::Resolution {
                    source_resolution_id,
                }) = &fact.settlement_request_id.resolution_identity.key
                else {
                    panic!("原判定身份")
                };
                let request = SettlementRequestV1 {
                    identity: SettlementRequestIdV1 {
                        account: fact.settlement_request_id.account.clone(),
                        condition_id: fact.settlement_request_id.condition_id.clone(),
                        resolution_identity: source_resolution_id.clone(),
                        policy_version: fact.settlement_request_id.policy_version.clone(),
                    },
                    index_sets: fact.index_sets,
                };
                driver
                    .decision()
                    .unwrap()
                    .query_resolution_redeem_v1(request.identity.clone())
                    .unwrap();
                restored_request = Some(request);
            } else {
                let read = shared.ledger().economic_read().unwrap();
                let observation = read
                    .resolutions()
                    .next()
                    .unwrap()
                    .request
                    .unwrap()
                    .observation;
                if observation == SettlementObservationV1::RetryAuthorized && !retried {
                    driver
                        .decision()
                        .unwrap()
                        .request_resolution_redeem_v1(restored_request.clone().unwrap())
                        .unwrap();
                    retried = true;
                }
            }
        }
        assert_all_settlement_cuts(case, &seen_cuts);
        assert_eq!(first_accepted_at.is_some(), case != 4 && case != 5);
        assert!(shared.apply_closed().unwrap());
        let nav =
            UnmarkedAccountEconomicsReadV3::from_ledger(&shared.ledger().economic_read().unwrap());
        let stats = driver
            .settlement_simulation_read(&ConditionId(CONDITION_ID.into()))
            .unwrap();
        assert_eq!(stats.queries, 1);
        assert_eq!(stats.sends, if case == 3 { 2 } else { 1 });
        assert_eq!(retried, case == 3);
        assert!(stats.final_success);
        assert_eq!(nav.conservative_nav_pusd, r("12727", "125"));
        driver.complete().unwrap();
        let restored = LedgerWriterV1::open_durable(&ledger_root).unwrap();
        assert_eq!(
            UnmarkedAccountEconomicsReadV3::from_ledger(&restored.economic_read().unwrap()),
            nav
        );
        let restored_again = LedgerWriterV1::open_durable(&ledger_root).unwrap();
        assert_eq!(
            UnmarkedAccountEconomicsReadV3::from_ledger(&restored_again.economic_read().unwrap()),
            nav
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn pure_loser_closes_without_settlement_request_after_original_barriers() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_settlement(&root);
    let (mut execution, observation) =
        execution(&temp.path().join("sim.jsonl"), &core, SUCCESS_POLICY);
    execution.configure_resolution_redeem_v1(script(0)).unwrap();
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "loser".into());
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    shared
        .execute(
            &mut driver.decision().unwrap(),
            RequestContextV2 {
                global_request_id: "loser-five".into(),
                caller_correlation_id: None,
            },
            Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                order: BuyLegIntentV1 {
                    token_id: TokenId(DOWN_TOKEN_ID.into()),
                    order: BuyOrderIntentV1::GtcLimit {
                        limit_price_pusd_per_share: Decimal::new(6, 1),
                        size_shares: Decimal::from(5),
                        post_only: false,
                    },
                },
            })),
        )
        .await
        .unwrap();
    let mut coordinator = SettlementCoordinatorV1::default();
    while driver.next_group().unwrap().is_some() {
        shared
            .advance_settlement(&mut coordinator, &mut driver)
            .unwrap();
    }
    assert!(shared.apply_closed().unwrap());
    let read = shared.ledger().economic_read().unwrap();
    let resolution = read.resolutions().next().unwrap();
    assert_eq!(
        resolution.economic_predecessors_closed,
        ExecutionReadValueV1::Known(true)
    );
    assert!(resolution.request.is_none());
    assert_eq!(resolution.paired_receivable_pusd.0, "0");
    assert_eq!(resolution.winner_orphan_receivable_pusd.0, "0");
    assert!(
        driver
            .settlement_simulation_read(&ConditionId(CONDITION_ID.into()))
            .is_none()
    );
    let nav = UnmarkedAccountEconomicsReadV3::from_ledger(&read);
    assert_eq!(nav.conservative_nav_pusd, r("24229", "250"));
    assert_eq!(
        read.market_totals().next().unwrap().confirmed_net_pnl_pusd,
        r("-771", "250")
    );
    driver.complete().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn paired_receivable_precedes_loser_failure_and_compensation_barrier() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_settlement_pending_loser(&root);
    let account = AccountInstanceKeyV1 {
        run_id: "shared-combination".into(),
        caller_role: SimCallerRoleV1::Paper,
        caller_instance_id: "shared-caller".into(),
        account_id: "shared-account".into(),
        platform_source: PlatformSourceV1::Simulated,
    };
    let policy = serde_json::to_vec(&serde_json::json!({"schema_version":"trade_finality_policy_v2",
        "normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10},{"state":"CONFIRMED","offset_ms":20}],"economic_application_ordinal":1},
        "failure_overrides":[{"selector":{"selector_domain":"REQUEST","account_instance_key":account,"global_request_id":"pending-one"},
            "path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10},{"state":"FAILED","offset_ms":20000}],"economic_application_ordinal":1}}]})).unwrap();
    let (mut execution, observation) = execution(&temp.path().join("sim.jsonl"), &core, &policy);
    execution.configure_resolution_redeem_v1(script(0)).unwrap();
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    let ledger_root = temp.path().join("ledger");
    execution
        .bind_ledger_recording_v2(&mut ledger, &ledger_root)
        .unwrap();
    let mut shared = SharedExecutionV1::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "pending-loser".into());
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    let mut first = true;
    let mut pending_sent = false;
    let mut saw_blocked = false;
    let mut coordinator = SettlementCoordinatorV1::default();
    loop {
        let at = driver
            .decision()
            .unwrap()
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        let orders = if first {
            vec![(UP_TOKEN_ID, "six-a", 6), (DOWN_TOKEN_ID, "five-b", 5)]
        } else if at >= 56000 && !pending_sent {
            pending_sent = true;
            vec![(DOWN_TOKEN_ID, "pending-one", 5)]
        } else {
            vec![]
        };
        first = false;
        for (token, id, q) in orders {
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
                                limit_price_pusd_per_share: Decimal::new(6, 1),
                                size_shares: Decimal::from(q),
                                post_only: false,
                            },
                        },
                    })),
                )
                .await
                .unwrap();
        }
        if shared
            .advance_settlement(&mut coordinator, &mut driver)
            .unwrap()
        {
            let read = shared.ledger().economic_read().unwrap();
            for resolution in read.resolutions() {
                assert_eq!(
                    resolution.settlement_cost_attribution,
                    SettlementCostAttributionV1::Maker
                );
                if resolution.economic_predecessors_closed == ExecutionReadValueV1::Known(false) {
                    assert_eq!(resolution.paired_receivable_pusd.0, "5");
                    assert_eq!(resolution.winner_orphan_receivable_pusd.0, "1");
                    assert!(resolution.request.is_none());
                    saw_blocked = true;
                }
            }
        }
        if driver.next_group().unwrap().is_none() {
            break;
        }
    }
    assert!(saw_blocked);
    assert!(shared.apply_closed().unwrap());
    let read = shared.ledger().economic_read().unwrap();
    assert_eq!(read.cash().actual_available_pusd.0, "99.1152");
    let market = read.market_totals().next().unwrap();
    assert_eq!(market.executed[1].actual_fee_pusd.0, "0.1848");
    assert_eq!(market.confirmed_net_pnl_pusd, r("-553", "625"));
    assert_eq!(
        driver
            .settlement_simulation_read(&ConditionId(CONDITION_ID.into()))
            .unwrap()
            .sends,
        1
    );
    driver.complete().unwrap();
    let restored = LedgerWriterV1::open_durable(&ledger_root).unwrap();
    assert_eq!(
        restored
            .economic_read()
            .unwrap()
            .market_totals()
            .next()
            .unwrap(),
        market
    );
}

#[tokio::test(flavor = "current_thread")]
async fn pending_settlement_and_unclaimed_feedback_restore_with_original_modules() {
    use sim_system::{
        finality::TradeFinalityPolicyV2, latency_profile::load_embedded_proxy_latency_profile,
        scenario::*,
    };
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, TradingTimes(mut delivered)) = recorded_settlement(&root);
    // 公共交付可合法落后平台接收；29,697ms来自此夹具加载的原代理延迟档，
    // 不是平台上限。空档覆盖该档，不改脚本、原随机序或反馈。
    for at in &mut delivered {
        if *at >= 66_000 {
            *at += SETTLEMENT_DELTA_MS as i64;
        }
    }
    let times = TradingTimes(delivered);
    let (mut execution, observation, rules) = execution_with_rules_v4(
        &temp.path().join("sim.jsonl"),
        &core,
        SUCCESS_POLICY,
        None,
        "e".repeat(64),
        "100",
    );
    execution.configure_resolution_redeem_v1(script(1)).unwrap();
    let ledger_root = temp.path().join("ledger");
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &ledger_root)
        .unwrap();
    let mut shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let groups = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let consumed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = consumed.clone();
    let source = groups
        .clone()
        .into_iter()
        .inspect(move |_| {
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        })
        .map(Ok);
    let mut driver =
        FinitePublicConsumptionV1::new(execution, source, "settlement-checkpoint".into());
    driver.enable_finite_recording_wait().unwrap();
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    shared
        .execute(
            &mut driver.decision().unwrap(),
            RequestContextV2 {
                global_request_id: "checkpoint-buy-five".into(),
                caller_correlation_id: None,
            },
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
        .unwrap();
    let mut coordinator = SettlementCoordinatorV1::default();
    loop {
        assert!(driver.next_group().unwrap().is_some());
        shared
            .advance_settlement(&mut coordinator, &mut driver)
            .unwrap();
        if driver
            .settlement_simulation_read(&ConditionId(CONDITION_ID.into()))
            .is_some_and(|r| !r.final_success)
        {
            break;
        }
    }
    let next_public = driver.next_group_at().unwrap().unwrap();
    driver.advance_before(next_public).unwrap();
    assert!(
        driver
            .settlement_simulation_read(&ConditionId(CONDITION_ID.into()))
            .is_some_and(|r| !r.final_success)
    );
    let mut sim = Vec::new();
    driver.write_checkpoint(&mut sim).unwrap();
    let prefetched = driver.prefetched_group().unwrap();
    let cursor = consumed.load(std::sync::atomic::Ordering::Relaxed);
    let mut ledger = Vec::new();
    shared.ledger().write_checkpoint(&mut ledger).unwrap();
    let mut registry = Vec::new();
    shared.write_checkpoint(&mut registry).unwrap();
    let mut settlement = Vec::new();
    ciborium::ser::into_writer(&coordinator, &mut settlement).unwrap();
    #[derive(serde::Deserialize)]
    struct Driver {
        noe: Vec<u8>,
    }
    #[derive(serde::Deserialize)]
    struct Observation {
        consumption: ::execution_normalization::NoeConsumptionSourceV2,
        closed_receive_time: Option<i64>,
        public_visible_time: Option<i64>,
    }
    #[derive(serde::Deserialize)]
    struct Port {
        observation: Observation,
    }
    let saved: Driver = ciborium::de::from_reader(sim.as_slice()).unwrap();
    let mut saved: Port = ciborium::de::from_reader(saved.noe.as_slice()).unwrap();
    let closed = saved.observation.closed_receive_time.unwrap();
    let visible = saved.observation.public_visible_time.unwrap();
    assert!(
        saved
            .observation
            .consumption
            .take_closed(closed, visible)
            .is_empty(),
        "耐久保存不能提前领取尚不可见反馈"
    );
    let future = saved
        .observation
        .consumption
        .take_closed(i64::MAX, i64::MAX);
    assert!(!future.is_empty(), "完整状态须实际含尚未领的反馈");
    assert!(
        future.iter().all(|r| r.durable_reference().is_some()),
        "恢复消费引用指向原已稳定NOE材料"
    );
    while driver.next_group().unwrap().is_some() {
        shared
            .advance_settlement(&mut coordinator, &mut driver)
            .unwrap();
    }
    shared.apply_closed().unwrap();
    let expected = serde_json::to_value(AccountEconomicsReadV3::from_ledger_and_observations(
        &shared.ledger().economic_read().unwrap(),
        std::iter::empty(),
    ))
    .unwrap();
    let expected_projection = shared.ledger().projection_v2();
    driver.stop_and_drain().unwrap();
    drop(shared);
    let scenario = PlatformScenarioV1::try_new(
        core.fixed_markets()
            .map(|market| {
                let c = core.pm_context(&market.condition_id.0).unwrap();
                PlatformMarketV1::try_new(
                    c.market,
                    c.assets
                        .into_iter()
                        .map(|a| a.asset)
                        .collect::<Vec<_>>()
                        .try_into()
                        .unwrap(),
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let scenario = Arc::new(
        SealedPlatformScenarioV1::decode_and_seal(
            &serde_json::to_vec(&SimulationScenarioArtifactV1 {
                schema_version: PLATFORM_SCENARIO_SCHEMA_V1.into(),
                markets: scenario.market_set().markets().to_vec(),
            })
            .unwrap(),
        )
        .unwrap(),
    );
    let (mut state, observation) = PublicConsumptionStateV1::restore_checkpoint(
        sim.as_slice(),
        rules,
        Arc::new(TradeFinalityPolicyV2::decode_and_seal(SUCCESS_POLICY).unwrap()),
        scenario,
        load_embedded_proxy_latency_profile().unwrap(),
        2 << 20,
        128,
    )
    .unwrap();
    let mut ledger = LedgerWriterV1::restore_checkpoint(ledger.as_slice()).unwrap();
    state
        .reconnect_ledger_recording(&mut ledger, &ledger_root)
        .unwrap();
    let mut shared =
        SharedExecutionV2::restore_checkpoint(ledger, observation, registry.as_slice()).unwrap();
    let source = prefetched
        .into_iter()
        .chain(groups.into_iter().skip(cursor))
        .map(Ok);
    let mut driver = FinitePublicConsumptionV1::from_checkpoint_state(state, source);
    driver.enable_finite_recording_wait().unwrap();
    let mut coordinator: SettlementCoordinatorV1 =
        ciborium::de::from_reader(settlement.as_slice()).unwrap();
    assert!(
        driver
            .settlement_simulation_read(&ConditionId(CONDITION_ID.into()))
            .is_some_and(|r| !r.final_success)
    );
    while driver.next_group().unwrap().is_some() {
        shared
            .advance_settlement(&mut coordinator, &mut driver)
            .unwrap();
    }
    shared.apply_closed().unwrap();
    assert_eq!(shared.ledger().projection_v2(), expected_projection);
    assert_eq!(
        serde_json::to_value(AccountEconomicsReadV3::from_ledger_and_observations(
            &shared.ledger().economic_read().unwrap(),
            std::iter::empty()
        ))
        .unwrap(),
        expected
    );
    driver.stop_and_drain().unwrap();
}
