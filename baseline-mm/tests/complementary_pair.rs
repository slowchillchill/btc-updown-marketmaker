#[allow(dead_code)]
#[path = "../../shared-run/tests/common/mod.rs"]
mod common;
use baseline_mm::*;
use common::*;
use contracts::*;
use ledger::*;
use rust_decimal::Decimal;
use shared_run::*;
use sim_system::{
    execution::*, platform_rules::SealedProcessRuleSnapshotV3, public_consumption::*,
};
use std::{path::Path, sync::Arc};

type Driver = FinitePublicConsumptionV1<RecordedPublicGroupSourceV1<MemoryRecordedTimes>>;
fn config() -> ComplementaryPairConfigV1 {
    ComplementaryPairConfigV1::parse_toml(include_str!(
        "../configs/strategies/complementary_pair/synthetic-engineering.toml"
    ))
    .unwrap()
}
fn setup(root: &Path, cash: &str) -> (SharedExecutionV2, Driver, Arc<SealedProcessRuleSnapshotV3>) {
    let dpt = root.join("dpt");
    let core = recorded_memory_complete(&dpt);
    let (mut execution, observation, rules) = execution_with_rules_v4(
        &root.join("sim.jsonl"),
        &core,
        SUCCESS_POLICY,
        None,
        "e".repeat(64),
        cash,
    );
    let mut ledger =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &root.join("ledger"))
        .unwrap();
    let shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&dpt, 1, 1 << 20, MemoryRecordedTimes).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "candidate-test".into());
    for _ in 0..2 {
        driver.next_group().unwrap().unwrap();
    }
    (shared, driver, rules)
}
fn strategy(
    c: ComplementaryPairConfigV1,
    rules: Arc<SealedProcessRuleSnapshotV3>,
) -> ComplementaryPairV1 {
    ComplementaryPairV1::new(
        c,
        rules,
        "candidate".into(),
        StrategyContinuityV1::new_account(),
    )
    .unwrap()
}
fn fact<T>(v: T) -> ObservedFactV1<T> {
    ObservedFactV1 {
        value: Some(v),
        quality: MarketDataQuality::Fresh,
        ..Default::default()
    }
}
/// 公开观测叶子通过正式类型构造；账户、持仓、订单、费用全部来自原SIM/NOE/Ledger。
fn estimated(mut input: StrategyInputV1, price: &str) -> StrategyInputV1 {
    for m in &mut input.markets {
        m.observation.settlement_reference = fact(BtcSettlementReferenceV1::ChainlinkSpot);
        let anchor = MarketPriceAnchorV1 {
            price_usd_per_btc: ExactDecimal("100000".into()),
            source: MarketAnchorSourceV1::OfficialCryptoPriceOpenPrice,
            application: PriceAnchorApplicationV1::Observed,
        };
        m.observation.effective_reference =
            EffectiveMarketReferenceV1::OfficialAnchor(fact(anchor));
        m.observation.chainlink_spot = fact(ChainlinkBtcUsdReferenceV1 {
            symbol: "btc/usd".into(),
            price_usd_per_btc: ExactDecimal(price.into()),
            message_timestamp_utc_ms: 21_000,
        });
    }
    input
}
fn diagnostic(
    o: &StrategyOutputV1<ComplementaryPairDiagnosticV1>,
) -> &ComplementaryPairDiagnosticV1 {
    match o {
        StrategyOutputV1::Continue { diagnostics, .. } | StrategyOutputV1::Exit { diagnostics } => {
            diagnostics
        }
    }
}
fn intents(o: &StrategyOutputV1<ComplementaryPairDiagnosticV1>) -> &[ReservedIntentV1] {
    match o {
        StrategyOutputV1::Continue { intents, .. } => intents,
        _ => &[],
    }
}
fn buy(
    rules: &SealedProcessRuleSnapshotV3,
    id: &str,
    side: &str,
    q: Decimal,
    p: Decimal,
    post_only: bool,
) -> ReservedIntentV1 {
    let n = num_bigint::BigUint::from((q * Decimal::from(100)).to_string().parse::<u64>().unwrap());
    let index = (p * Decimal::from(100))
        .normalize()
        .to_string()
        .parse::<u16>()
        .unwrap();
    let cost = rules
        .limit_buy_cost_v1(&n, index, &ExactDecimal("0.01".into()), post_only)
        .unwrap();
    ReservedIntentV1 {
        context: RequestContextV2 {
            global_request_id: id.into(),
            caller_correlation_id: None,
        },
        intent: Arc::new(ModelIntentV1::SubmitSingleBuy {
            condition_id: ConditionId(CONDITION_ID.into()),
            order: BuyLegIntentV1 {
                token_id: TokenId(side.into()),
                order: BuyOrderIntentV1::GtcLimit {
                    limit_price_pusd_per_share: p,
                    size_shares: q,
                    post_only,
                },
            },
        }),
        reserves: vec![IntentReserveV1 {
            batch_item_ordinal: 0,
            reserve: ReserveIntentV1 {
                request_identity: LedgerRequestIdentityV1(format!("{id}-funds")),
                condition_id: ConditionId(CONDITION_ID.into()),
                requested_principal_pusd: ExactDecimal(
                    (Decimal::from_str_exact(&cost.principal_micro.to_string()).unwrap()
                        / Decimal::from(1_000_000))
                    .to_string(),
                ),
                immediate_fee_reserve_pusd: ExactDecimal(
                    (Decimal::from_str_exact(&cost.fee_1e5_units.to_string()).unwrap()
                        / Decimal::from(100_000))
                    .to_string(),
                ),
            },
        }],
    }
}
async fn fill_up(
    shared: &mut SharedExecutionV2,
    driver: &mut Driver,
    rules: &SealedProcessRuleSnapshotV3,
) -> StrategyInputV1 {
    shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[buy(
                rules,
                "up-fill",
                UP_TOKEN_ID,
                Decimal::from(10),
                Decimal::new(6, 1),
                false,
            )],
        )
        .await
        .unwrap();
    while driver.next_group().unwrap().is_some() {
        let input = shared.strategy_input(driver).unwrap().unwrap();
        if input
            .trade_economics
            .iter()
            .any(|t| t.success_applied && t.economic_applied)
        {
            return input;
        }
    }
    panic!("原经济应用应产生真实孤腿");
}

#[tokio::test(flavor = "current_thread")]
async fn construction_config_and_frozen_pair_diagnostics_use_public_dispatch() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let mut s = strategy(config(), rules);
    let input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    let calculation = input.calculate(&mut s, 22_000).unwrap();
    let frozen = diagnostic(&calculation.output).clone();
    assert_eq!(frozen.rule_version, "candidate_v1");
    assert!(
        matches!(intents(&calculation.output)[0].intent.as_ref(),ModelIntentV1::SubmitPairedBuy{orders,..} if matches!((&orders[0].order,&orders[1].order),(BuyOrderIntentV1::GtcLimit{size_shares:a,post_only:true,..},BuyOrderIntentV1::GtcLimit{size_shares:b,post_only:true,..}) if a==b))
    );
    let result = shared
        .dispatch_strategy(
            &mut driver.decision().unwrap(),
            &mut s,
            calculation.complete(22_000),
        )
        .await
        .unwrap();
    let StrategyDispatchV1::Continue {
        calculation,
        receipts,
        ..
    } = result
    else {
        panic!()
    };
    assert_eq!(receipts.len(), 1);
    assert_eq!(diagnostic(&calculation.calculation.output), &frozen);
    driver.next_group().unwrap().unwrap();
    let changed = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100030",
    );
    let _ = changed
        .calculate(
            &mut s,
            changed
                .action_before
                .cutoff()
                .decision_time()
                .logical_time_utc_ms(),
        )
        .unwrap();
    assert_eq!(
        diagnostic(&calculation.calculation.output),
        &frozen,
        "后一轮估计不能污染旧冻结诊断"
    );
    assert!(
        ComplementaryPairConfigV1::parse_toml("[repair]\nenable_active_repair=false").is_err(),
        "缺失完整风险配置不能伪装为正式默认"
    );
}
#[tokio::test(flavor = "current_thread")]
async fn temporary_reference_missing_inputs_zero_variance_and_nav_are_distinct() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let mut c = config();
    c.probability.initial_sigma_usd_per_sqrt_second = 0.0;
    c.probability.estimation_error_usd = 0.0;
    let mut s = strategy(c, rules);
    let mut input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    let raw = reference_after(21_000, 1);
    let parsed = data_pipeline_core::public_reference::source_evidence_v3_from_typed_raw_v2(
        &data_pipeline_core::public_input::TypedRawRowV2::PolymarketRtdsChainlinkTwap(raw),
    );
    assert!(parsed.is_ok());
    // 临时参考使用公共选择的原结构；正式锚点未到本身不禁估计。
    let a = DirectObservationAnchorV1 {
        field_path: "temporary".into(),
        position: PublicFactPositionV1 {
            input_stream_id: "test".into(),
            closed_at_input_position: 0,
            fact_ordinal: 0,
        },
        group_id: Sha256Hex("1".repeat(64)),
        source_message_id: Sha256Hex("2".repeat(64)),
        source_event_at_utc_ms: Some(20_999),
        received_at_utc_ms: 21_000,
    };
    let t = TemporaryTwapReferenceV1 {
        value: ChainlinkBtcUsdTwapReferenceV1 {
            symbol: "btc/usd".into(),
            window_s: 30,
            price_usd_per_btc: ExactDecimal("100000".into()),
            payload_timestamp_utc_ms: 20_999,
            message_timestamp_utc_ms: 21_000,
        },
        selected_position: a.position.clone(),
        direct_anchor: a,
        dependency_frontier: input.markets[0].observation.visible_prefix.clone(),
    };
    input.markets[0].observation.effective_reference = EffectiveMarketReferenceV1::TemporaryTwap(t);
    let out = s.decide(&input, 22_000).unwrap();
    assert!(matches!(
        diagnostic(&out).markets[0].probability,
        ProbabilityEstimateV1::Known {
            up: 1.0,
            temporary_reference: true,
            uncertainty_usd: 0.0,
            ..
        }
    ));
    input.markets[0].observation.chainlink_spot = Default::default();
    let out = s.decide(&input, 21_001).unwrap();
    assert!(matches!(
        diagnostic(&out).markets[0].probability,
        ProbabilityEstimateV1::Unavailable(EstimateUnavailableV1::MissingSettlementPrice)
    ));
    assert!(
        matches!(
            diagnostic(&out).risk_equity_pusd,
            ExecutionReadValueV1::Known(_)
        ),
        "估计缺失与NAV未知各自表达"
    );
    assert!(intents(&out).is_empty());
}
#[tokio::test(flavor = "current_thread")]
async fn actual_orphan_cost_and_fak_bound_do_not_require_profit_or_a_premium_gate() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let input = fill_up(&mut shared, &mut driver, &rules).await;
    let now = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let mut input = estimated(input, "100000");
    input.markets[0]
        .observation
        .assets
        .iter_mut()
        .find(|a| a.asset.token_id.0 == DOWN_TOKEN_ID)
        .unwrap()
        .asks = fact(vec![PriceLevelV1 {
        price_pusd_per_share: ExactDecimal("0.4".into()),
        quantity_shares: ExactDecimal("10".into()),
    }]);
    let mut c = config();
    c.quote.base_half_spread = Decimal::new(1, 2);
    c.repair.base_loss_allowance_per_share = Decimal::new(2, 1);
    c.repair.residual_risk_weight = Decimal::from(10);
    c.quote.base_pair_shares = Decimal::from(10);
    let mut s = strategy(c, rules);
    let out = s.decide(&input, now).unwrap();
    let d = diagnostic(&out);
    assert!(
        contracts::economics::EconomicRational::try_from(&d.qualified_net_pnl_pusd).unwrap()
            <= contracts::economics::EconomicRational::zero()
    );
    assert!(
        d.markets[0]
            .unavailable
            .contains(&ActionUnavailableV1::AmountOverfillBound),
        "不能用盘口创造FAK最低价保证: {d:?}"
    );
    assert!(
        intents(&out).iter().any(|i| matches!(
            i.intent.as_ref(),
            ModelIntentV1::SubmitSingleBuy {
                order: BuyLegIntentV1 {
                    order: BuyOrderIntentV1::GtcLimit {
                        post_only: true,
                        ..
                    },
                    ..
                },
                ..
            }
        )),
        "零利润仍可在独立小亏容忍内Maker修复"
    );
    let cost = input
        .trade_economics
        .iter()
        .find(|t| t.success_applied)
        .unwrap();
    assert_eq!(cost.fact.quote_amount_pusd, ExactDecimal("6".into()));
    assert_eq!(
        d.markets[0]
            .comparison
            .as_ref()
            .unwrap()
            .paired_net_per_share,
        contracts::economics::EconomicRational::from_decimal(Decimal::new(-68, 4)).public(),
        "已确认费按剩余贡献归属一次，不再次从P扣费"
    );
}
#[tokio::test(flavor = "current_thread")]
async fn high_water_window_exit_is_immediate_and_no_opportunity_keeps_evaluating() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let input = fill_up(&mut shared, &mut driver, &rules).await;
    let now = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let mut continuity = StrategyContinuityV1::new_account();
    continuity.confirmed_high_water_pusd =
        contracts::economics::EconomicRational::from_decimal(Decimal::from(3000)).public();
    let mut s =
        ComplementaryPairV1::new(config(), rules, "termination".into(), continuity).unwrap();
    let out = s.decide(&input, now).unwrap();
    assert!(diagnostic(&out).terminating);
    assert!(
        matches!(out, StrategyOutputV1::Continue { .. }),
        "当前无估计/机会不构成立即退出"
    );
    let end = input.markets[0].observation.market.window.end_at_utc_ms;
    let calculation = input.calculate(&mut s, end).unwrap();
    assert!(matches!(calculation.output, StrategyOutputV1::Exit { .. }));
    let before = shared.ledger().projection_v2().unwrap().ledger_entry_seq;
    assert!(matches!(
        shared
            .dispatch_strategy(
                &mut driver.decision().unwrap(),
                &mut s,
                calculation.complete(end)
            )
            .await
            .unwrap(),
        StrategyDispatchV1::Exit { .. }
    ));
    assert_eq!(
        shared.ledger().projection_v2().unwrap().ledger_entry_seq,
        before
    );
    assert_eq!(
        s.continuity().confirmed_high_water_pusd,
        contracts::economics::EconomicRational::from_decimal(Decimal::from(3000)).public()
    );
}

type PoolDriver = FinitePublicConsumptionV1<RecordedPublicGroupSourceV1<TradingTimes>>;
async fn profitable_pool(
    root: &Path,
) -> (
    SharedExecutionV2,
    PoolDriver,
    Arc<SealedProcessRuleSnapshotV3>,
    StrategyInputV1,
) {
    let dpt = root.join("dpt");
    let (core, times) = recorded_scaled_average_pool(&dpt, false, 10);
    let (mut execution, observation, rules) = execution_with_fee_v4(
        &root.join("sim.jsonl"),
        &core,
        "2000",
        "0.0000000000000000000000000001",
    );
    let mut ledger =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &root.join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&dpt, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "pool-candidate".into());
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[buy(
                &rules,
                "pool-up-first",
                UP_TOKEN_ID,
                Decimal::from(10),
                Decimal::new(1, 1),
                true,
            )],
        )
        .await
        .unwrap();
    let mut second = false;
    let mut opposite = false;
    loop {
        driver.next_group().unwrap().unwrap();
        let input = shared.strategy_input(&mut driver).unwrap().unwrap();
        let at = input
            .action_before
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        if at >= 28_000 && !second {
            second = true;
            shared
                .execute_batch_reserved(
                    &mut driver.decision().unwrap(),
                    &[buy(
                        &rules,
                        "pool-up-second",
                        UP_TOKEN_ID,
                        Decimal::from(10),
                        Decimal::new(9, 1),
                        true,
                    )],
                )
                .await
                .unwrap();
        }
        if at >= 38_000 && !opposite {
            opposite = true;
            shared
                .execute_batch_reserved(
                    &mut driver.decision().unwrap(),
                    &[buy(
                        &rules,
                        "pool-down",
                        DOWN_TOKEN_ID,
                        Decimal::from(10),
                        Decimal::new(2, 1),
                        true,
                    )],
                )
                .await
                .unwrap();
        }
        if input
            .action_before
            .economics()
            .markets
            .iter()
            .any(|m| Decimal::from_str_exact(&m.paired_shares.0).unwrap() == Decimal::from(10))
        {
            return (shared, driver, rules, input);
        }
    }
}
#[tokio::test(flavor = "current_thread")]
async fn positive_premium_is_compared_and_profit_occupation_tracks_real_dispatch_and_cancel() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules, input) = profitable_pool(temp.path()).await;
    let now = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let mut c = config();
    c.quote.inventory_shift_per_pusd = Decimal::new(6, 2);
    c.repair.residual_risk_weight = Decimal::from(100);
    c.repair.enable_active_repair = false;
    let mut s = strategy(c.clone(), rules.clone());
    let input = estimated(input, "100000");
    let calculation = input.calculate(&mut s, now).unwrap();
    let d = diagnostic(&calculation.output);
    assert_eq!(
        d.qualified_net_pnl_pusd,
        contracts::economics::EconomicRational::from_decimal(Decimal::from(3)).public()
    );
    assert_eq!(d.markets[0].choice, MarketChoiceV1::MakerRepair);
    assert!(
        contracts::economics::EconomicRational::try_from(
            d.markets[0]
                .comparison
                .as_ref()
                .unwrap()
                .premium_per_share
                .as_ref()
                .unwrap()
        )
        .unwrap()
            > contracts::economics::EconomicRational::zero()
    );
    assert!(
        s.continuity().unresolved_profit_allocations.is_empty(),
        "候选计算尚未占用跨轮资源"
    );
    shared
        .dispatch_strategy(
            &mut driver.decision().unwrap(),
            &mut s,
            calculation.complete(now),
        )
        .await
        .unwrap();
    assert_eq!(s.continuity().unresolved_profit_allocations.len(), 1);
    driver.next_group().unwrap().unwrap();
    let input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    let now = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let out = s.decide(&input, now).unwrap();
    assert_eq!(diagnostic(&out).markets[0].choice, MarketChoiceV1::Maintain);
    assert!(
        diagnostic(&out).profit_allocations.is_empty(),
        "原缺口未决时不重复占用"
    );
    assert!(
        contracts::economics::EconomicRational::try_from(&diagnostic(&out).available_profit_pusd)
            .unwrap()
            < contracts::economics::EconomicRational::from_decimal(Decimal::from(3))
    );
    let mut restored =
        ComplementaryPairV1::new(c, rules, "candidate".into(), s.continuity()).unwrap();
    let restored_out = restored.decide(&input, now).unwrap();
    assert_eq!(
        diagnostic(&out).available_profit_pusd,
        diagnostic(&restored_out).available_profit_pusd
    );
    let ids: Vec<_> = input
        .action_before
        .orders()
        .filter_map(|o| match &o.order_identity.key {
            ::execution_normalization::MemoryIdentityKeyV1::Entity(
                ::execution_normalization::SourceNeutralEntityKeyV1::Order { source_order_id },
            ) => Some(source_order_id.clone()),
            _ => None,
        })
        .collect();
    shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[ReservedIntentV1 {
                context: RequestContextV2 {
                    global_request_id: "close-maker".into(),
                    caller_correlation_id: None,
                },
                intent: Arc::new(ModelIntentV1::CancelOrders {
                    platform_order_ids: ids,
                }),
                reserves: vec![],
            }],
        )
        .await
        .unwrap();
    driver.next_group().unwrap().unwrap();
    let input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    let now = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let _ = s.decide(&input, now).unwrap();
    assert!(
        s.continuity().unresolved_profit_allocations.is_empty(),
        "真实无效果余量反馈释放原占用"
    );
}
#[tokio::test(flavor = "current_thread")]
async fn active_repair_cancels_same_side_then_recalculates_and_does_not_resend_unknown() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules, _) = profitable_pool(temp.path()).await;
    shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[buy(
                &rules,
                "old-down",
                DOWN_TOKEN_ID,
                Decimal::from(5),
                Decimal::new(3, 1),
                true,
            )],
        )
        .await
        .unwrap();
    driver.next_group().unwrap().unwrap();
    let input = shared.strategy_input(&mut driver).unwrap().unwrap();
    let now = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let adjust = |input| {
        let mut input = estimated(input, "100000");
        input.markets[0]
            .observation
            .assets
            .iter_mut()
            .find(|a| a.asset.token_id.0 == DOWN_TOKEN_ID)
            .unwrap()
            .asks = fact(vec![PriceLevelV1 {
            price_pusd_per_share: ExactDecimal("0.4".into()),
            quantity_shares: ExactDecimal("2.5".into()),
        }]);
        input
    };
    let input = adjust(input);
    let mut c = config();
    // 全域界巨大：仅以显式极宽合成参数验证可达分支，不是经营风险默认。
    c.risk.per_market_orphan_limit_pusd = Decimal::MAX;
    c.risk.account_orphan_limit_pusd = Decimal::MAX;
    c.repair.maximum_overfill_pusd = Decimal::MAX;
    c.repair.maximum_overfill_shares = Decimal::MAX;
    c.repair.residual_risk_weight = Decimal::from(10);
    c.repair.base_loss_allowance_per_share = Decimal::new(2, 1);
    let mut s = strategy(c, rules);
    let calculation = input.calculate(&mut s, now).unwrap();
    assert_eq!(
        diagnostic(&calculation.output).markets[0].choice,
        MarketChoiceV1::CancelBeforeActiveRepair
    );
    assert!(
        intents(&calculation.output)
            .iter()
            .all(|i| matches!(i.intent.as_ref(), ModelIntentV1::CancelOrders { .. })),
        "旧快照仅撤单"
    );
    shared
        .dispatch_strategy(
            &mut driver.decision().unwrap(),
            &mut s,
            calculation.complete(now),
        )
        .await
        .unwrap();
    let pending = adjust(shared.strategy_input(&mut driver).unwrap().unwrap());
    let pending_out = s.decide(&pending, now).unwrap();
    assert!(!intents(&pending_out).iter().any(|i| matches!(
        i.intent.as_ref(),
        ModelIntentV1::SubmitSingleBuy {
            order: BuyLegIntentV1 {
                order: BuyOrderIntentV1::FakMarket { .. },
                ..
            },
            ..
        }
    )));
    driver.next_group().unwrap().unwrap();
    let input = adjust(shared.strategy_input(&mut driver).unwrap().unwrap());
    let now = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let calculation = input.calculate(&mut s, now).unwrap();
    assert_eq!(
        diagnostic(&calculation.output).markets[0].choice,
        MarketChoiceV1::ActiveRepair
    );
    shared
        .dispatch_strategy(
            &mut driver.decision().unwrap(),
            &mut s,
            calculation.complete(now),
        )
        .await
        .unwrap();
    let input = adjust(shared.strategy_input(&mut driver).unwrap().unwrap());
    let out = s.decide(&input, now).unwrap();
    assert!(
        !intents(&out).iter().any(|i| matches!(
            i.intent.as_ref(),
            ModelIntentV1::SubmitSingleBuy {
                order: BuyLegIntentV1 {
                    order: BuyOrderIntentV1::FakMarket { .. },
                    ..
                },
                ..
            }
        )),
        "未决主动补腿不按原缺口重复发"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn twap_uses_causal_integral_and_binance_corrections_can_reverse_repeatedly() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let mut input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "99900",
    );
    input.markets[0].observation.market.window.end_at_utc_ms = 120_000;
    input.markets[0].observation.settlement_reference =
        fact(BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds);
    let mut s = strategy(config(), rules.clone());
    let first = s.decide(&input, 60_000).unwrap();
    assert!(matches!(
        diagnostic(&first).markets[0].probability,
        ProbabilityEstimateV1::Known {
            approximate_window_integral: false,
            ..
        }
    ));
    input.markets[0].observation.chainlink_spot = fact(ChainlinkBtcUsdReferenceV1 {
        symbol: "btc/usd".into(),
        price_usd_per_btc: ExactDecimal("100500".into()),
        message_timestamp_utc_ms: 110_000,
    });
    let out = s.decide(&input, 110_000).unwrap();
    let ProbabilityEstimateV1::Known {
        mean_usd,
        up,
        approximate_window_integral,
        ..
    } = &diagnostic(&out).markets[0].probability
    else {
        panic!()
    };
    assert!((*mean_usd - 100000.0).abs() < 1e-8);
    assert!((*up - 0.5).abs() < 1e-8);
    assert!(*approximate_window_integral);
    let mut missing_history = strategy(config(), rules.clone());
    assert!(matches!(
        diagnostic(&missing_history.decide(&input, 110_000).unwrap()).markets[0].probability,
        ProbabilityEstimateV1::Unavailable(EstimateUnavailableV1::MissingWindowHistory)
    ));
    input.markets[0].observation.settlement_reference =
        fact(BtcSettlementReferenceV1::ChainlinkSpot);
    input.markets[0].observation.chainlink_spot = fact(ChainlinkBtcUsdReferenceV1 {
        symbol: "btc/usd".into(),
        price_usd_per_btc: ExactDecimal("100000".into()),
        message_timestamp_utc_ms: 60_000,
    });
    let mut s = strategy(config(), rules);
    let mut innovations = Vec::new();
    for (n, price) in ["100100", "100200", "100000", "100250"].iter().enumerate() {
        input.markets[0].observation.binance_trade = fact(BinanceAggregateTradeReferenceV1 {
            symbol: "BTCUSDT".into(),
            aggregate_trade_id: n as u64,
            first_trade_id: n as u64,
            last_trade_id: n as u64,
            price_usdt_per_btc: ExactDecimal((*price).into()),
            quantity_btc: ExactDecimal("1".into()),
            buyer_is_maker: false,
            event_time_utc_ms: 60000 + n as i64,
        });
        let out = s.decide(&input, 60000 + n as i64).unwrap();
        let ProbabilityEstimateV1::Known {
            binance_innovation_usd,
            ..
        } = diagnostic(&out).markets[0].probability
        else {
            panic!()
        };
        innovations.push(binance_innovation_usd);
    }
    assert_eq!(innovations[0], 0.0, "首对仅建立USDT/USD因果基差");
    assert!(
        innovations[1] > 0.0 && innovations[2] < 0.0 && innovations[3] > 0.0,
        "每次反转重新计算，不固定首次方向"
    );
}
#[tokio::test(flavor = "current_thread")]
async fn ordinary_repricing_counts_old_and_new_investment_and_uses_equal_targets() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let mut c = config();
    c.quote.base_pair_shares = Decimal::from(10_000);
    c.risk.per_market_orphan_limit_pusd = Decimal::from(3000);
    c.risk.account_orphan_limit_pusd = Decimal::from(3000);
    let mut s = strategy(c.clone(), rules.clone());
    let input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    let at = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let calculation = input.calculate(&mut s, at).unwrap();
    let first_cost: Decimal = intents(&calculation.output)
        .iter()
        .flat_map(|i| &i.reserves)
        .map(|r| {
            Decimal::from_str_exact(&r.reserve.requested_principal_pusd.0).unwrap()
                + Decimal::from_str_exact(&r.reserve.immediate_fee_reserve_pusd.0).unwrap()
        })
        .sum();
    assert!(first_cost > Decimal::from(500) && first_cost <= Decimal::from(1000));
    shared
        .dispatch_strategy(
            &mut driver.decision().unwrap(),
            &mut s,
            calculation.complete(at),
        )
        .await
        .unwrap();
    let input = loop {
        driver.next_group().unwrap().unwrap();
        let input = estimated(
            shared.strategy_input(&mut driver).unwrap().unwrap(),
            "100000",
        );
        if input.action_before.orders().count() >= 2 {
            break input;
        }
    };
    let at = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    c.quote.base_half_spread = Decimal::new(2, 1);
    let mut s = ComplementaryPairV1::new(c, rules, "candidate".into(), s.continuity()).unwrap();
    let out = s.decide(&input, at).unwrap();
    let new_cost: Decimal = intents(&out)
        .iter()
        .flat_map(|i| &i.reserves)
        .map(|r| {
            Decimal::from_str_exact(&r.reserve.requested_principal_pusd.0).unwrap()
                + Decimal::from_str_exact(&r.reserve.immediate_fee_reserve_pusd.0).unwrap()
        })
        .sum();
    assert!(
        first_cost + new_cost <= Decimal::from(1000),
        "允许重叠，但不释放尚未反馈旧单的投入"
    );
    assert!(
        new_cost
            <= Decimal::from_str_exact(
                &input.action_before.economics().cash.actual_available_pusd.0
            )
            .unwrap()
    );
    for item in intents(&out) {
        if let ModelIntentV1::SubmitPairedBuy { orders, .. } = item.intent.as_ref() {
            let sizes: Vec<_> = orders
                .iter()
                .map(|leg| match leg.order {
                    BuyOrderIntentV1::GtcLimit {
                        size_shares,
                        post_only: true,
                        ..
                    } => size_shares,
                    _ => panic!(),
                })
                .collect();
            assert_eq!(sizes[0], sizes[1]);
        }
    }
    assert!(
        intents(&out)
            .iter()
            .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::CancelOrders { .. })),
        "旧高价单必须申请撤换"
    );
}
#[tokio::test(flavor = "current_thread")]
async fn unknown_public_nav_stops_new_pairs_but_independent_repair_remains_possible() {
    let temp = tempfile::tempdir().unwrap();
    let dpt = temp.path().join("dpt");
    let (core, times) = recorded_nav_marks_with_book_quantity(&dpt, "10");
    let (mut execution, observation, rules) = execution_with_rules_v4(
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
    let mut shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&dpt, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "unknown-nav".into());
    for _ in 0..8 {
        driver.next_group().unwrap().unwrap();
    }
    let mut other = buy(
        &rules,
        "other-up",
        "21",
        Decimal::from(10),
        Decimal::new(6, 1),
        false,
    );
    if let ModelIntentV1::SubmitSingleBuy { condition_id, .. } = Arc::make_mut(&mut other.intent) {
        *condition_id = ConditionId(OTHER_CONDITION_ID.into());
    }
    other.reserves[0].reserve.condition_id = ConditionId(OTHER_CONDITION_ID.into());
    shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[
                buy(
                    &rules,
                    "nav-up",
                    UP_TOKEN_ID,
                    Decimal::from(10),
                    Decimal::new(6, 1),
                    false,
                ),
                other,
            ],
        )
        .await
        .unwrap();
    let input = loop {
        driver.next_group().unwrap().unwrap();
        let input = shared.strategy_input(&mut driver).unwrap().unwrap();
        if input
            .action_before
            .cutoff()
            .decision_time()
            .logical_time_utc_ms()
            >= 71_000
        {
            break input;
        }
    };
    assert!(matches!(
        input.action_before.economics().conservative_nav_pusd,
        ExecutionReadValueV1::Unavailable
    ));
    let at = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let input = estimated(input, "100000");
    let mut c = config();
    c.repair.base_loss_allowance_per_share = Decimal::new(2, 1);
    c.repair.residual_risk_weight = Decimal::from(100);
    let mut s = strategy(c, rules);
    let out = s.decide(&input, at).unwrap();
    assert!(matches!(
        diagnostic(&out).risk_equity_pusd,
        ExecutionReadValueV1::Unavailable
    ));
    assert!(
        !intents(&out)
            .iter()
            .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::SubmitPairedBuy { .. }))
    );
    assert!(intents(&out).iter().any(|i|matches!(i.intent.as_ref(),ModelIntentV1::SubmitSingleBuy{condition_id,..} if condition_id.0==OTHER_CONDITION_ID)),"NAV未知不是所有已有缺口修复的统一否决门: {:?}",diagnostic(&out));
}
#[tokio::test(flavor = "current_thread")]
async fn maker_overlap_bounds_use_expensive_old_order_price_not_cheap_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules, _) = profitable_pool(temp.path()).await;
    shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[buy(
                &rules,
                "expensive-old-down",
                DOWN_TOKEN_ID,
                Decimal::from(5),
                Decimal::new(7, 1),
                true,
            )],
        )
        .await
        .unwrap();
    let input = loop {
        driver.next_group().unwrap().unwrap();
        let input = estimated(
            shared.strategy_input(&mut driver).unwrap().unwrap(),
            "100000",
        );
        if input.action_before.orders().count() > 0 {
            break input;
        }
    };
    let at = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let mut c = config();
    c.repair.enable_active_repair = false;
    c.repair.residual_risk_weight = Decimal::from(100);
    c.repair.maximum_overfill_pusd = Decimal::from(2);
    let mut s = strategy(c, rules);
    let out = s.decide(&input, at).unwrap();
    let new_q = intents(&out)
        .iter()
        .find_map(|i| match i.intent.as_ref() {
            ModelIntentV1::SubmitSingleBuy {
                order:
                    BuyLegIntentV1 {
                        order: BuyOrderIntentV1::GtcLimit { size_shares, .. },
                        ..
                    },
                ..
            } => Some(*size_shares),
            _ => None,
        })
        .unwrap();
    assert!(
        (Decimal::from(5) + new_q - Decimal::from(10)).max(Decimal::ZERO) * Decimal::new(7, 1)
            <= Decimal::from(2),
        "旧高价单与低价新单任意先成交的反向孤腿都要受限"
    );
    assert!(new_q >= Decimal::from(5), "有限小批修复仍可行");
    assert!(
        intents(&out)
            .iter()
            .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::CancelOrders { .. }))
    );
}
