#[path = "../../shared-run/tests/common/fixture_average_pool.rs"]
mod fixture_average_pool;
#[path = "../../shared-run/tests/common/fixture_clock.rs"]
mod fixture_clock;
#[path = "../../shared-run/tests/common/fixture_execution.rs"]
mod fixture_execution;
#[path = "../../shared-run/tests/common/fixture_execution_v4.rs"]
mod fixture_execution_v4;
#[path = "../../shared-run/tests/common/fixture_input.rs"]
mod fixture_input;
#[path = "../../shared-run/tests/common/fixture_market_identity.rs"]
mod fixture_market_identity;
#[path = "../../shared-run/tests/common/fixture_multi_market.rs"]
mod fixture_multi_market;
#[path = "../../shared-run/tests/common/fixture_nav.rs"]
mod fixture_nav;
#[path = "../../shared-run/tests/common/fixture_pair.rs"]
mod fixture_pair;
#[path = "../../shared-run/tests/common/fixture_policy.rs"]
mod fixture_policy;
#[path = "support/profit_fixture.rs"]
mod fixture_profit;
#[path = "../../shared-run/tests/common/fixture_recording.rs"]
mod fixture_recording;
#[path = "../../shared-run/tests/common/fixture_reference.rs"]
mod fixture_reference;
#[path = "../../shared-run/tests/common/fixture_terminal.rs"]
mod fixture_terminal;
mod common {
    pub use super::fixture_average_pool::*;
    pub use super::fixture_clock::*;
    pub use super::fixture_execution_v4::*;
    pub use super::fixture_input::*;
    pub use super::fixture_market_identity::*;
    pub use super::fixture_multi_market::*;
    pub use super::fixture_nav::*;
    pub use super::fixture_pair::*;
    pub use super::fixture_policy::*;
    pub use super::fixture_profit::*;
    pub use super::fixture_reference::*;
}

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
fn config() -> ComplementaryPairConfigV3 {
    let mut c = ComplementaryPairConfigV3::parse_toml(include_str!(
        "../configs/strategies/complementary_pair/synthetic-engineering-v3.toml"
    ))
    .unwrap();
    // 旧经营反例使用长窗口显式工程阈值；健康到期另由专项反例验证。
    c.health = SourceHealthConfigV1 {
        polymarket_silence_normal_ms: 0,
        polymarket_silence_ms: 1_000_000,
        polymarket_event_delay_normal_ms: 0,
        polymarket_event_delay_ms: 1_000_000,
        chainlink_silence_normal_ms: 0,
        chainlink_silence_ms: 1_000_000,
        chainlink_event_delay_normal_ms: 0,
        chainlink_event_delay_ms: 1_000_000,
        binance_trade_silence_normal_ms: 0,
        binance_trade_silence_ms: 1_000_000,
        binance_trade_event_delay_normal_ms: 0,
        binance_trade_event_delay_ms: 1_000_000,
        binance_depth_silence_normal_ms: 0,
        binance_depth_silence_ms: 1_000_000,
        binance_depth_event_delay_normal_ms: 0,
        binance_depth_event_delay_ms: 1_000_000,
    };
    c
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
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &root.join("ledger"))
        .unwrap();
    let shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let mut source =
        RecordedPublicGroupSourceV1::open(&dpt, 1, 1 << 20, MemoryRecordedTimes).unwrap();
    source.enable_source_progress();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "candidate-test".into());
    for _ in 0..2 {
        driver.next_group().unwrap().unwrap();
    }
    (shared, driver, rules)
}
fn strategy(
    c: ComplementaryPairConfigV3,
    rules: Arc<SealedProcessRuleSnapshotV3>,
) -> ComplementaryPairV1 {
    ComplementaryPairV1::new(
        c,
        rules,
        "candidate".into(),
        StrategyContinuityV2::new_account(),
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
        // 与本函数的显式合成估值叶子同时建立有效来源证据。
        let mut progress = m.observation.source_progress.to_vec();
        for (source, kind, symbol) in [
            (
                SourceStreamKindV1::ChainlinkTwapThirty,
                CanonicalSourceKindV2::PolymarketRtdsChainlinkTwap,
                "btc/usd",
            ),
            (
                SourceStreamKindV1::ChainlinkTwapSixty,
                CanonicalSourceKindV2::PolymarketRtdsChainlinkTwap,
                "btc/usd",
            ),
            (
                SourceStreamKindV1::ChainlinkSpot,
                CanonicalSourceKindV2::PolymarketRtdsChainlinkBtcUsd,
                "btc/usd",
            ),
            (
                SourceStreamKindV1::BinanceAggregateTrade,
                CanonicalSourceKindV2::BinanceAggtrade,
                "BTCUSDT",
            ),
            (
                SourceStreamKindV1::BinanceDepth,
                CanonicalSourceKindV2::BinanceDepthSnapshot,
                "BTCUSDT",
            ),
        ] {
            let evidence = SourceEvidenceRefV3 {
                source_kind: kind,
                source_message_id: Sha256Hex("a".repeat(64)),
                source_native_identity: None,
                source_event_at_utc_ms: Some(21_000),
                received_at_utc_ms: 21_000,
                collector_admission: None,
            };
            progress.push(SourceStreamProgressV1 {
                scope: SourceStreamScopeV1 {
                    source,
                    condition_id: None,
                    asset_or_symbol: symbol.into(),
                },
                last_valid_business_message: Some(evidence.clone()),
                last_observed_message: evidence,
                connection_generation: None,
                synchronization: SourceSynchronizationV1::Unknown,
                quality: EventQualityV2 {
                    availability: MarketDataQuality::Fresh,
                    resync: ResyncQuality::NotRequired,
                },
            });
        }
        for asset in &m.observation.assets {
            if !progress.iter().any(|p| {
                p.scope.source == SourceStreamKindV1::PolymarketBook
                    && p.scope.asset_or_symbol == asset.asset.token_id.0
            }) {
                let evidence = SourceEvidenceRefV3 {
                    source_kind: CanonicalSourceKindV2::PolymarketOrderbook,
                    source_message_id: Sha256Hex("b".repeat(64)),
                    source_native_identity: None,
                    source_event_at_utc_ms: Some(21_000),
                    received_at_utc_ms: 21_000,
                    collector_admission: None,
                };
                progress.push(SourceStreamProgressV1 {
                    scope: SourceStreamScopeV1 {
                        source: SourceStreamKindV1::PolymarketBook,
                        condition_id: Some(m.observation.market.condition_id.clone()),
                        asset_or_symbol: asset.asset.token_id.0.clone(),
                    },
                    last_valid_business_message: Some(evidence.clone()),
                    last_observed_message: evidence,
                    connection_generation: None,
                    synchronization: SourceSynchronizationV1::Unknown,
                    quality: EventQualityV2 {
                        availability: MarketDataQuality::Fresh,
                        resync: ResyncQuality::NotRequired,
                    },
                });
            }
        }
        m.observation.source_progress = progress.into();
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
    assert_eq!(frozen.rule_version, "candidate_v3");
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
        ComplementaryPairConfigV3::parse_toml("[repair]\nenable_active_repair=false").is_err(),
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
async fn actual_orphan_cost_and_fak_estimate_do_not_require_profit_or_a_premium_gate() {
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
    }])
    .into();
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
    assert_eq!(
        d.markets[0].choice,
        MarketChoiceV1::ActiveRepair,
        "同一公开场景可形成FAK，估计不要求未来结果保证: {d:?}"
    );
    assert!(intents(&out).iter().any(|i| matches!(
        i.intent.as_ref(),
        ModelIntentV1::SubmitSingleBuy {
            order: BuyLegIntentV1 {
                order: BuyOrderIntentV1::FakMarket { .. },
                ..
            },
            ..
        }
    )));
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
    let mut continuity = StrategyContinuityV2::new_account();
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
    profitable_pool_shape(root, 10, 10).await
}
async fn profitable_pool_shape(
    root: &Path,
    scale: u32,
    down_shares: u32,
) -> (
    SharedExecutionV2,
    PoolDriver,
    Arc<SealedProcessRuleSnapshotV3>,
    StrategyInputV1,
) {
    let dpt = root.join("dpt");
    let (core, times) = recorded_scaled_average_pool(&dpt, false, scale);
    let (mut execution, observation, rules) = execution_with_fee_v4(
        &root.join("sim.jsonl"),
        &core,
        "2000",
        "0.0000000000000000000000000001",
    );
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
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
                        Decimal::from(down_shares),
                        Decimal::new(2, 1),
                        true,
                    )],
                )
                .await
                .unwrap();
        }
        if input.action_before.economics().markets.iter().any(|m| {
            Decimal::from_str_exact(&m.paired_shares.0).unwrap() == Decimal::from(scale.min(20))
        }) {
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
    let current_action = shared
        .strategy_input(&mut driver)
        .unwrap()
        .unwrap()
        .action_before;
    assert_eq!(
        s.qualify_intent(&health_read(&input, current_action), &calculation, 0, now)
            .unwrap(),
        IntentQualificationV1::Qualified,
        "原正利润池支持通过真实当前读取复核"
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
        }])
        .into();
        input
    };
    let input = adjust(input);
    let mut c = config();
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
    let mut s =
        ComplementaryPairV1::new(c.clone(), rules.clone(), "candidate".into(), s.continuity())
            .unwrap();
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
    let start = input.markets[0].observation.market.window.start_at_utc_ms;
    let mut restricted = ComplementaryPairV1::new(c, rules, "excluded".into(), s.continuity())
        .unwrap()
        .with_entry_market_range(Some(EntryMarketRangeV1 {
            start_at_utc_ms: start + 1,
            end_at_utc_ms: start + 2,
        }));
    let excluded = restricted.decide(&input, at).unwrap();
    assert!(
        intents(&excluded)
            .iter()
            .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::CancelOrders { .. })),
        "被排除市场的真实旧单仍撤换"
    );
    assert!(
        !intents(&excluded)
            .iter()
            .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::SubmitPairedBuy { .. }))
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
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
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
async fn maker_overlap_risk_uses_expensive_old_order_price_not_cheap_replacement() {
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
    c.risk.per_market_orphan_limit_pusd = Decimal::from(2);
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
            <= Decimal::new(35, 1),
        "旧高价承诺3.5已高于2额度，允许不增加既有暴露，不恢复额外2pUSD过补门"
    );
    assert!(new_q >= Decimal::from(5), "有限小批修复仍可行");
    assert!(
        intents(&out)
            .iter()
            .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::CancelOrders { .. }))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn rolling_twap_fallback_cannot_supply_the_terminal_spot_integral() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let mut input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "99900",
    );
    let o = &mut input.markets[0].observation;
    o.market.window.end_at_utc_ms = 120_000;
    o.settlement_reference = fact(BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds);
    o.chainlink_spot = ObservedFactV1::default();
    o.chainlink_twap_sixty = fact(ChainlinkBtcUsdTwapReferenceV1 {
        symbol: "btc/usd".into(),
        window_s: 60,
        price_usd_per_btc: ExactDecimal("99900".into()),
        payload_timestamp_utc_ms: 60_000,
        message_timestamp_utc_ms: 60_000,
    });
    let mut s = strategy(config(), rules);
    let before = input.calculate(&mut s, 60_000).unwrap();
    assert!(
        matches!(
            diagnostic(&before.output).markets[0].probability,
            ProbabilityEstimateV1::Known {
                mean_usd: 99900.0,
                approximate_window_integral: false,
                ..
            }
        ),
        "窗前TWAP仍可作为未来水平近似"
    );
    input.markets[0].observation.chainlink_twap_sixty = fact(ChainlinkBtcUsdTwapReferenceV1 {
        symbol: "btc/usd".into(),
        window_s: 60,
        price_usd_per_btc: ExactDecimal("100500".into()),
        payload_timestamp_utc_ms: 110_000,
        message_timestamp_utc_ms: 110_000,
    });
    let inside = input.calculate(&mut s, 110_000).unwrap();
    assert!(
        matches!(
            diagnostic(&inside.output).markets[0].probability,
            ProbabilityEstimateV1::Unavailable(EstimateUnavailableV1::MissingWindowHistory)
        ),
        "rolling均值不是60–110秒真实spot路径"
    );
    assert!(
        !intents(&inside.output).iter().any(|i| matches!(
            i.intent.as_ref(),
            ModelIntentV1::SubmitPairedBuy { .. } | ModelIntentV1::SubmitSingleBuy { .. }
        )),
        "缺少该概率的真实依赖，不能形成依赖它的新报价"
    );
}
#[tokio::test(flavor = "current_thread")]
async fn fak_minimum_principal_is_independent_of_maker_minimum_shares() {
    for target in [5, 3] {
        let temp = tempfile::tempdir().unwrap();
        let (mut shared, mut driver, rules, input) = profitable_pool(temp.path()).await;
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
        }])
        .into();
        let now = input
            .action_before
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        let mut c = config();
        c.quote.base_pair_shares = Decimal::from(target);
        // 与原FAK分支夹具相同的显式合成额度，不是经营配置。
        c.repair.residual_risk_weight = Decimal::from(10);
        c.repair.base_loss_allowance_per_share = Decimal::new(2, 1);
        let mut continuity = StrategyContinuityV2::new_account();
        continuity.terminating = true;
        let mut s =
            ComplementaryPairV1::new(c, rules, "amount-minimum".into(), continuity).unwrap();
        let calculation = input.calculate(&mut s, now).unwrap();
        assert_eq!(
            diagnostic(&calculation.output).markets[0].choice,
            MarketChoiceV1::ActiveRepair,
            "LIMIT最小5份不能截断金额合格的FAK: target={target}"
        );
        assert!(matches!(intents(&calculation.output)[0].intent.as_ref(),
            ModelIntentV1::SubmitSingleBuy { order: BuyLegIntentV1 { order: BuyOrderIntentV1::FakMarket { spend_pusd, worst_price_pusd_per_share }, .. }, .. }
            if *spend_pusd == Decimal::ONE && *worst_price_pusd_per_share == Decimal::new(4,1)));
        if target == 3 {
            assert!(
                diagnostic(&calculation.output).markets[0]
                    .comparison
                    .as_ref()
                    .unwrap()
                    .maker_value_per_share
                    .is_none()
            );
        }
        let action = shared
            .strategy_input(&mut driver)
            .unwrap()
            .unwrap()
            .action_before;
        assert_eq!(
            s.qualify_intent(&health_read(&input, action), &calculation, 0, now)
                .unwrap(),
            IntentQualificationV1::Qualified,
            "原金额FAK当前成本资格通过"
        );
        let result = shared
            .dispatch_strategy(
                &mut driver.decision().unwrap(),
                &mut s,
                calculation.complete(now),
            )
            .await
            .unwrap();
        assert!(
            matches!(result, StrategyDispatchV1::Continue { receipts, .. } if receipts.len() == 1)
        );
    }
}

fn health_read<'a>(
    input: &'a StrategyInputV1,
    action_before: ActionBeforeReadV2,
) -> StrategyCurrentReadV1<'a> {
    let cutoff = action_before.cutoff().clone();
    StrategyCurrentReadV1 {
        trade_economics: input.trade_economics.clone(),
        markets: input
            .markets
            .iter()
            .map(|f| PublicObservationFrameV1 {
                statistics: None,
                observation: &f.observation,
                rule_context: f.rule_context.clone(),
                receipt: cutoff.receipt().clone(),
                decision_cutoff: cutoff.clone(),
            })
            .collect(),
        action_before,
    }
}
#[tokio::test(flavor = "current_thread")]
async fn health_dependencies_follow_enabled_algorithm_and_asset_scope() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let mut input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    let mut c = config();
    c.probability.binance_innovation_weight = 0.0;
    c.probability.binance_pressure_weight_usd = 0.0;
    c.probability.binance_trade_pressure_weight_usd = 1.0;
    input.markets[0].observation.source_progress = input.markets[0]
        .observation
        .source_progress
        .iter()
        .filter(|p| p.scope.source != SourceStreamKindV1::BinanceDepth)
        .cloned()
        .collect::<Vec<_>>()
        .into();
    let mut s = strategy(c.clone(), rules.clone());
    assert!(s.observation_requirements().binance_depth_quantities);
    let output = s.decide(&input, 22_000).unwrap();
    assert!(
        diagnostic(&output).markets[0]
            .source_health
            .iter()
            .any(|p| p.scope.source == SourceStreamKindV1::BinanceDepth)
    );
    assert!(
        intents(&output).is_empty(),
        "成交压力的深度分母未启用独立深度系数仍是必要依赖"
    );
    c.probability.binance_trade_pressure_weight_usd = 0.0;
    let mut disabled = strategy(c, rules);
    assert!(!disabled.observation_requirements().binance_depth_quantities);
    let output = disabled.decide(&input, 22_000).unwrap();
    assert!(diagnostic(&output).markets[0].source_health.is_empty());
    assert!(!intents(&output).is_empty());
    let mut progress = input.markets[0].observation.source_progress.to_vec();
    let own = progress
        .iter_mut()
        .find(|p| p.scope.source == SourceStreamKindV1::PolymarketBook)
        .unwrap();
    own.scope.asset_or_symbol = "unrelated-token".into();
    input.markets[0].observation.source_progress = progress.into();
    let output = disabled.decide(&input, 22_000).unwrap();
    assert!(
        diagnostic(&output).markets[0]
            .source_health
            .iter()
            .any(|i| i.failure == SourceHealthFailureV1::UnknownBusinessProgress)
    );
    assert!(
        intents(&output)
            .iter()
            .all(|i| matches!(i.intent.as_ref(), ModelIntentV1::CancelOrders { .. }))
    );
}
#[tokio::test(flavor = "current_thread")]
async fn frozen_action_invalidates_when_real_commitment_changes_without_rewriting_diagnostics() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    let mut s = strategy(config(), rules.clone());
    let calculation = input.calculate(&mut s, 22_000).unwrap();
    let frozen = diagnostic(&calculation.output).clone();
    let action = shared
        .strategy_input(&mut driver)
        .unwrap()
        .unwrap()
        .action_before;
    assert_eq!(
        s.qualify_intent(&health_read(&input, action), &calculation, 0, 22_000)
            .unwrap(),
        IntentQualificationV1::Qualified
    );
    shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[buy(
                &rules,
                "competing-real-request",
                UP_TOKEN_ID,
                Decimal::from(5),
                Decimal::new(4, 1),
                true,
            )],
        )
        .await
        .unwrap();
    let current = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    let action = shared
        .strategy_input(&mut driver)
        .unwrap()
        .unwrap()
        .action_before;
    assert!(
        matches!(s.qualify_intent(&health_read(&current, action), &calculation, 0, 22_000).unwrap(), IntentQualificationV1::Invalidated(reason) if reason.reason_code == "action_business_facts_changed")
    );
    assert_eq!(diagnostic(&calculation.output), &frozen);
}
#[tokio::test(flavor = "current_thread")]
async fn independent_repair_qualifies_healthy_held_mark_and_rejects_missing_mark() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let raw = fill_up(&mut shared, &mut driver, &rules).await;
    let at = raw
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let mut input = estimated(raw, "100000");
    input.markets[0].observation.source_progress = input.markets[0]
        .observation
        .source_progress
        .iter()
        .filter(|p| p.scope.source != SourceStreamKindV1::ChainlinkSpot)
        .cloned()
        .collect::<Vec<_>>()
        .into();
    let mut c = config();
    c.repair.enable_active_repair = false;
    c.repair.residual_risk_weight = Decimal::from(10);
    c.repair.base_loss_allowance_per_share = Decimal::new(2, 1);
    let mut s = strategy(c, rules);
    let calculation = input.calculate(&mut s, at).unwrap();
    let index = intents(&calculation.output)
        .iter()
        .position(|i| matches!(i.intent.as_ref(), ModelIntentV1::SubmitSingleBuy { .. }))
        .expect("失效辅助源不阻塞独立Maker修复");
    let action = shared
        .strategy_input(&mut driver)
        .unwrap()
        .unwrap()
        .action_before;
    assert_eq!(
        s.qualify_intent(&health_read(&input, action), &calculation, index, at)
            .unwrap(),
        IntentQualificationV1::Qualified
    );
    let held = diagnostic(&calculation.output)
        .action_health_dependencies
        .iter()
        .find_map(|d| d.independent_held_token.as_ref())
        .unwrap()
        .clone();
    input.markets[0]
        .observation
        .assets
        .iter_mut()
        .find(|a| a.asset.token_id == held)
        .unwrap()
        .valuation_best_bid = Default::default();
    let action = shared
        .strategy_input(&mut driver)
        .unwrap()
        .unwrap()
        .action_before;
    assert!(
        matches!(s.qualify_intent(&health_read(&input, action), &calculation, index, at).unwrap(), IntentQualificationV1::Invalidated(r) if r.reason_code == "repair_held_mark_unavailable")
    );
}
#[tokio::test(flavor = "current_thread")]
async fn invalidated_batch_registers_profit_only_for_actual_sent_prefix() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules, input) = profitable_pool(temp.path()).await;
    let at = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let mut c = config();
    c.quote.inventory_shift_per_pusd = Decimal::new(6, 2);
    c.repair.residual_risk_weight = Decimal::from(100);
    c.repair.enable_active_repair = false;
    let mut s = strategy(c, rules);
    let input = estimated(input, "100000");
    let mut calculation = input.calculate(&mut s, at).unwrap();
    let StrategyOutputV1::Continue {
        diagnostics,
        intents,
        ..
    } = &mut calculation.output
    else {
        panic!()
    };
    let mut unattempted = diagnostics.profit_allocations[0].clone();
    unattempted.global_request_id = "unattempted-suffix".into();
    unattempted.reserve_identity = LedgerRequestIdentityV1("unattempted-funds".into());
    diagnostics.profit_allocations.push(unattempted);
    let sent = shared
        .execute_batch_reserved(&mut driver.decision().unwrap(), &intents[..1])
        .await
        .unwrap();
    let result = InvalidatedBatchV1 {
        sent,
        invalidated_intent_index: 1,
        reason: IntentInvalidationReasonV1 {
            reason_code: "test-health-expired".into(),
            scopes: vec![],
        },
        observed_cutoff: input.action_before.cutoff().clone(),
        detected_at_utc_ms: at,
        release_errors: vec![],
    };
    s.invalidated(&calculation, &result);
    let allocations = s.continuity().unresolved_profit_allocations;
    assert_eq!(allocations.len(), 1);
    assert!(!allocations.contains_key("unattempted-suffix"));
}

#[tokio::test(flavor = "current_thread")]
async fn real_pairing_changes_orphan_fee_contribution_and_invalidates_frozen_repair() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let raw = fill_up(&mut shared, &mut driver, &rules).await;
    let at = raw
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let input = estimated(raw, "100000");
    let old_trades = input.trade_economics.clone();
    let mut c = config();
    c.repair.enable_active_repair = false;
    c.repair.residual_risk_weight = Decimal::from(10);
    c.repair.base_loss_allowance_per_share = Decimal::new(2, 1);
    let mut s = strategy(c, rules.clone());
    let calculation = input.calculate(&mut s, at).unwrap();
    let index = intents(&calculation.output)
        .iter()
        .position(|i| matches!(i.intent.as_ref(), ModelIntentV1::SubmitSingleBuy { .. }))
        .unwrap();
    let original_fee = diagnostic(&calculation.output).action_health_dependencies[0]
        .business
        .orphan_fee
        .clone()
        .unwrap();
    let frozen = diagnostic(&calculation.output).clone();
    shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[buy(
                &rules,
                "real-pairing-feedback",
                DOWN_TOKEN_ID,
                Decimal::from(5),
                Decimal::new(9, 1),
                false,
            )],
        )
        .await
        .unwrap();
    let mut verified = false;
    while driver.next_group().unwrap().is_some() {
        let fresh = estimated(
            shared.strategy_input(&mut driver).unwrap().unwrap(),
            "100000",
        );
        let changed = old_trades.iter().any(|old| {
            fresh.trade_economics.iter().any(|new| {
                new.trade_identity == old.trade_identity
                    && new.unpaired_contribution_shares != old.unpaired_contribution_shares
            })
        });
        if !changed {
            continue;
        }
        let current = shared.strategy_current_read(&driver).unwrap().unwrap();
        assert_eq!(current.trade_economics, fresh.trade_economics);
        let action = shared
            .strategy_input(&mut driver)
            .unwrap()
            .unwrap()
            .action_before;
        assert!(
            matches!(s.qualify_intent(&health_read(&fresh, action), &calculation,index,at).unwrap(),IntentQualificationV1::Invalidated(r) if r.reason_code == "action_business_facts_changed")
        );
        assert!(
            contracts::economics::EconomicRational::try_from(&original_fee)
                .unwrap()
                .is_positive(),
            "原真实Taker孤腿具有正费用贡献"
        );
        println!(
            "real contribution changed; frozen_orphan_fee={original_fee:?}; current costs remain original Ledger facts"
        );
        verified = true;
        break;
    }
    assert!(verified, "原执行/NOE/Ledger产生可达配平贡献变化");
    assert_eq!(input.trade_economics, old_trades);
    assert_eq!(diagnostic(&calculation.output), &frozen);
}

#[tokio::test(flavor = "current_thread")]
async fn unrelated_market_commitment_does_not_invalidate_frozen_repair() {
    let temp = tempfile::tempdir().unwrap();
    let dpt = temp.path().join("dpt");
    let (core, times) = recorded_equal_start_markets(&dpt);
    let (mut execution, observation, rules) = execution_with_rules_v4(
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
    let mut shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&dpt, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "two-market-health".into());
    for _ in 0..8 {
        driver.next_group().unwrap().unwrap();
    }
    shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[buy(
                &rules,
                "real-first-market-orphan",
                UP_TOKEN_ID,
                Decimal::from(10),
                Decimal::new(6, 1),
                false,
            )],
        )
        .await
        .unwrap();
    let input = loop {
        assert!(driver.next_group().unwrap().is_some());
        let input = shared.strategy_input(&mut driver).unwrap().unwrap();
        if input
            .trade_economics
            .iter()
            .any(|t| t.economic_applied && t.success_applied)
        {
            break input;
        }
    };
    let at = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let mut c = config();
    c.repair.base_loss_allowance_per_share = Decimal::new(2, 1);
    c.repair.residual_risk_weight = Decimal::from(100);
    c.repair.enable_active_repair = false;
    let mut s = strategy(c, rules.clone());
    let input = estimated(input, "100000");
    let calculation = input.calculate(&mut s, at).unwrap();
    let dependency = &diagnostic(&calculation.output).action_health_dependencies[0];
    let other = input
        .markets
        .iter()
        .find(|f| f.observation.market.condition_id != dependency.condition_id)
        .expect("原双市场费用池夹具");
    let mut item = buy(
        &rules,
        "unrelated-market-request",
        UP_TOKEN_ID,
        Decimal::from(5),
        Decimal::new(2, 1),
        true,
    );
    item.intent = Arc::new(ModelIntentV1::SubmitSingleBuy {
        condition_id: other.observation.market.condition_id.clone(),
        order: BuyLegIntentV1 {
            token_id: other.observation.assets[0].asset.token_id.clone(),
            order: BuyOrderIntentV1::GtcLimit {
                limit_price_pusd_per_share: Decimal::new(2, 1),
                size_shares: Decimal::from(5),
                post_only: true,
            },
        },
    });
    item.reserves[0].reserve.condition_id = other.observation.market.condition_id.clone();
    shared
        .execute_batch_reserved(&mut driver.decision().unwrap(), &[item])
        .await
        .unwrap();
    let current = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    let action = shared
        .strategy_input(&mut driver)
        .unwrap()
        .unwrap()
        .action_before;
    assert_eq!(
        s.qualify_intent(&health_read(&current, action), &calculation, 0, at)
            .unwrap(),
        IntentQualificationV1::Qualified
    );
}

#[tokio::test(flavor = "current_thread")]
async fn pending_twap_deadline_survives_spot_arrival() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let mut input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    let o = &mut input.markets[0].observation;
    o.market.window.end_at_utc_ms = 120_000;
    o.settlement_reference = fact(BtcSettlementReferenceV1::ChainlinkTwapThirtySeconds);
    o.chainlink_spot = Default::default();
    o.chainlink_twap_thirty = fact(ChainlinkBtcUsdTwapReferenceV1 {
        symbol: "btc/usd".into(),
        window_s: 30,
        price_usd_per_btc: ExactDecimal("100000".into()),
        payload_timestamp_utc_ms: 21000,
        message_timestamp_utc_ms: 21000,
    });
    let mut c = config();
    c.health.chainlink_silence_ms = 5000;
    c.health.chainlink_event_delay_ms = 5000;
    c.probability.binance_innovation_weight = 0.0;
    let mut s = strategy(c, rules);
    let calculation = input.calculate(&mut s, 22000).unwrap();
    let frozen_diagnostic = serde_json::to_value(diagnostic(&calculation.output)).unwrap();
    let dep = &diagnostic(&calculation.output).action_health_dependencies[0];
    assert!(
        dep.sources
            .iter()
            .any(|p| p.source == SourceStreamKindV1::ChainlinkTwapThirty)
    );
    assert!(!intents(&calculation.output).is_empty());
    let o = &mut input.markets[0].observation;
    o.chainlink_spot = fact(ChainlinkBtcUsdReferenceV1 {
        symbol: "btc/usd".into(),
        price_usd_per_btc: ExactDecimal("100000".into()),
        message_timestamp_utc_ms: 23000,
    });
    let mut progress = o.source_progress.to_vec();
    let spot = progress
        .iter_mut()
        .find(|p| p.scope.source == SourceStreamKindV1::ChainlinkSpot)
        .unwrap();
    let e = spot.last_valid_business_message.as_mut().unwrap();
    e.received_at_utc_ms = 23000;
    e.source_event_at_utc_ms = Some(23000);
    spot.last_observed_message = e.clone();
    o.source_progress = progress.into();
    let action = shared
        .strategy_input(&mut driver)
        .unwrap()
        .unwrap()
        .action_before;
    let current = health_read(&input, action);
    assert_eq!(
        s.qualify_intent(&current, &calculation, 0, 23000).unwrap(),
        IntentQualificationV1::Qualified
    );
    let next = s.next_health_evaluation_at(&current.markets, Some(&calculation), 23000);
    assert_eq!(
        s.next_health_evaluation_at(&current.markets, None, 23000),
        Some(28000),
        "没有待发决定时仍按当前spot经营依赖排期限"
    );
    let at_due = s.qualify_intent(&current, &calculation, 0, 26000).unwrap();
    println!(
        "frozen_twap_received=21000 spot_received=23000 frozen_deadline=26000 next={next:?} qualification_at_frozen_deadline={at_due:?}"
    );
    assert!(matches!(at_due, IntentQualificationV1::Invalidated(_)));
    assert_eq!(
        next,
        Some(26000),
        "冻结动作依赖到期必须唤醒，不能被当前新选spot掩盖"
    );
    assert_eq!(
        serde_json::to_value(diagnostic(&calculation.output)).unwrap(),
        frozen_diagnostic
    );
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_chainlink_history_survives_binance_failure_at_original_decide() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let mut input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    let mut c = config();
    c.probability.binance_innovation_weight = 1.0;
    c.probability.binance_pressure_weight_usd = 0.0;
    c.probability.binance_trade_pressure_weight_usd = 0.0;
    c.probability.volatility_window_ms = 1000;
    let condition = input.markets[0].observation.market.condition_id.0.clone();
    let mut strategy = strategy(c, rules);
    let reference = input.markets[0].observation.effective_reference.clone();
    input.markets[0].observation.effective_reference = EffectiveMarketReferenceV1::Missing;
    for (at, price) in [(21000, "100000"), (22000, "100002"), (23000, "100001")] {
        let o = &mut input.markets[0].observation;
        let mut progress = o.source_progress.to_vec();
        progress.retain(|p| p.scope.source != SourceStreamKindV1::BinanceAggregateTrade);
        let spot = progress
            .iter_mut()
            .find(|p| p.scope.source == SourceStreamKindV1::ChainlinkSpot)
            .unwrap();
        let evidence = spot.last_valid_business_message.as_mut().unwrap();
        evidence.received_at_utc_ms = at;
        evidence.source_event_at_utc_ms = Some(at);
        o.chainlink_spot = fact(ChainlinkBtcUsdReferenceV1 {
            symbol: "btc/usd".into(),
            price_usd_per_btc: ExactDecimal(price.into()),
            message_timestamp_utc_ms: at,
        });
        o.chainlink_spot.direct_anchors = vec![DirectObservationAnchorV1 {
            field_path: "chainlink_spot".into(),
            position: PublicFactPositionV1 {
                input_stream_id: "test".into(),
                closed_at_input_position: 0,
                fact_ordinal: 0,
            },
            group_id: Sha256Hex("1".repeat(64)),
            source_message_id: evidence.source_message_id.clone(),
            source_event_at_utc_ms: Some(at),
            received_at_utc_ms: at,
        }];
        o.source_progress = progress.into();
        let output = strategy.decide(&input, at).unwrap();
        assert!(matches!(
            diagnostic(&output).markets[0].probability,
            ProbabilityEstimateV1::Unavailable(EstimateUnavailableV1::UnhealthySource)
        ));
        assert!(intents(&output).is_empty(), "综合健康失败仍限制新增风险");
        input.markets[0].observation.effective_reference = reference.clone();
    }
    let saved = serde_json::to_value(strategy.checkpoint()).unwrap();
    assert_eq!(
        saved["signals"][&condition]["chainlink"],
        serde_json::json!([[21000, 100000.0], [22000, 100002.0], [23000, 100001.0]])
    );
    assert!(
        saved["signals"][&condition]["binance_baseline"].is_null(),
        "历史维护不提早建立创新基差"
    );
    // 没有新spot（同接收毫秒）仍按窗口清理，并保留原左前驱。
    strategy.decide(&input, 25000).unwrap();
    let saved = serde_json::to_value(strategy.checkpoint()).unwrap();
    assert_eq!(
        saved["signals"][&condition]["chainlink"],
        serde_json::json!([[22000, 100002.0], [23000, 100001.0]])
    );
}

#[tokio::test(flavor = "current_thread")]
async fn actual_health_calculation_borrows_required_scope_among_unrelated_history() {
    use data_pipeline_core::{public_input::*, typed_raw::*};
    // 通过原 metadata/PM typed_raw 准备和公共归约建立来源，而非改写既有进展。
    let historical_progress = |count: u64| {
        let mut source = common::core();
        source.enable_source_progress();
        let mut progress = Vec::new();
        for i in 0..count {
            let condition = format!("0x{:064x}", i + 256);
            let token = (1000 + 2 * i).to_string();
            let DptIngressV1::Message {
                row: TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(mut metadata)),
                ..
            } = common::meta(0).ingress
            else {
                panic!("原 metadata 夹具应提供正式行")
            };
            metadata.condition_id = condition.clone();
            metadata.clob_market_id = (4_000_000 + i).to_string();
            metadata.asset_ids = format!("[\"{token}\",\"{}\"]", 1001 + 2 * i);
            metadata.collector_admission_sequence = i + 1;
            let input = common::record(
                2 * i,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(metadata)),
            );
            let prepared =
                prepare_public_ingress_v1(&source, &input, &Default::default(), &[]).unwrap();
            source.push(&prepared).unwrap();
            let event = PolymarketOrderbookEvent::Book {
                asset_id: token.clone(),
                bids: vec![PolymarketPriceLevel {
                    price: "0.5".into(),
                    size: "100".into(),
                }],
                asks: vec![PolymarketPriceLevel {
                    price: "0.6".into(),
                    size: "80".into(),
                }],
                last_trade_price: Some("0.55".into()),
            };
            let DptIngressV1::Message {
                row: TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(mut book)),
                ..
            } = common::pm(0, i + 1, 21_000, event).ingress
            else {
                panic!("原 PM 夹具应提供正式行")
            };
            book.market = condition.clone();
            let input = common::record(
                2 * i + 1,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(book)),
            );
            let prepared =
                prepare_public_ingress_v1(&source, &input, &Default::default(), &[]).unwrap();
            source.push(&prepared).unwrap();
            let scope = SourceStreamScopeV1 {
                source: SourceStreamKindV1::PolymarketBook,
                condition_id: Some(ConditionId(condition.clone())),
                asset_or_symbol: token.clone(),
            };
            let context = source.prepared_pm_context(&condition).unwrap();
            assert_eq!(context.market.condition_id.0, condition);
            assert!(
                context
                    .assets
                    .iter()
                    .any(|asset| asset.asset.token_id.0 == token)
            );
            let observed = source.current_source_progress(&scope).unwrap();
            assert_eq!(
                observed.last_observed_message.source_kind,
                CanonicalSourceKindV2::PolymarketOrderbook
            );
            assert_eq!(
                observed
                    .last_valid_business_message
                    .as_ref()
                    .unwrap()
                    .source_kind,
                CanonicalSourceKindV2::PolymarketOrderbook
            );
            progress.push(observed.clone());
        }
        assert_eq!(progress.len(), count as usize);
        progress
    };
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let original = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    let reference = original
        .calculate(&mut strategy(config(), rules.clone()), 22_000)
        .unwrap();
    assert!(!diagnostic(&reference.output).markets.is_empty());
    for historical_sources in [8, 80] {
        let history = historical_progress(historical_sources);
        let mut input = estimated(
            shared.strategy_input(&mut driver).unwrap().unwrap(),
            "100000",
        );
        for market in &mut input.markets {
            let original_progress: Vec<_> =
                market.observation.source_progress.iter().cloned().collect();
            let mut values = SourceProgressSnapshotsV1::new();
            for p in &original_progress {
                values.insert(p.scope.clone(), Arc::new(p.clone()));
            }
            for p in &history {
                values.insert(p.scope.clone(), Arc::new(p.clone()));
            }
            market.observation.source_progress = SharedSourceProgressV1::new(values);
        }
        let calculation = input
            .calculate(&mut strategy(config(), rules.clone()), 22_000)
            .unwrap();
        assert_eq!(
            diagnostic(&calculation.output).markets,
            diagnostic(&reference.output).markets
        );
        // 合法旧数组可以无序；同一健康调用仍读取原来源。
        let mut legacy = input;
        for market in &mut legacy.markets {
            let mut progress: Vec<_> = market.observation.source_progress.iter().cloned().collect();
            progress.reverse();
            market.observation.source_progress = progress.into();
        }
        let old = legacy
            .calculate(&mut strategy(config(), rules.clone()), 22_000)
            .unwrap();
        assert_eq!(
            diagnostic(&old.output).markets,
            diagnostic(&reference.output).markets
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn active_repair_uses_public_level_principals_and_one_total_fee() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let input = fill_up(&mut shared, &mut driver, &rules).await;
    let now = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let mut input = estimated(input, "100000");
    let asset = input.markets[0]
        .observation
        .assets
        .iter_mut()
        .find(|a| a.asset.token_id.0 == DOWN_TOKEN_ID)
        .unwrap();
    // 故意乱序；真实策略必须先吃便宜档，不能以10*.4生成4本金。
    asset.asks = fact(vec![
        PriceLevelV1 {
            price_pusd_per_share: ExactDecimal("0.4".into()),
            quantity_shares: ExactDecimal("6".into()),
        },
        PriceLevelV1 {
            price_pusd_per_share: ExactDecimal("0.3".into()),
            quantity_shares: ExactDecimal("4".into()),
        },
    ])
    .into();
    let mut c = config();
    c.repair.base_loss_allowance_per_share = Decimal::new(2, 1);
    c.repair.residual_risk_weight = Decimal::from(100);
    let mut s = strategy(c, rules);
    let calculation = input.calculate(&mut s, now).unwrap();
    assert_eq!(
        diagnostic(&calculation.output).markets[0].choice,
        MarketChoiceV1::ActiveRepair
    );
    let item = &intents(&calculation.output)[0];
    assert!(
        matches!(item.intent.as_ref(),ModelIntentV1::SubmitSingleBuy {order:BuyLegIntentV1 {order:BuyOrderIntentV1::FakMarket {spend_pusd,worst_price_pusd_per_share},..},..} if *spend_pusd==Decimal::new(36,1) && *worst_price_pusd_per_share==Decimal::new(4,1))
    );
    assert_eq!(
        item.reserves[0].reserve.immediate_fee_reserve_pusd,
        ExactDecimal("0.1596".into())
    );
    assert_eq!(
        diagnostic(&calculation.output).profit_allocations[0].estimate_cutoff,
        *input.action_before.cutoff()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn amount_pending_keeps_original_estimate_and_only_incremental_loss_until_real_feedback() {
    use contracts::economics::EconomicRational as R;
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules, input) = profitable_pool(temp.path()).await;
    let mut input = estimated(input, "100000");
    let now = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let asset = input.markets[0]
        .observation
        .assets
        .iter_mut()
        .find(|a| a.asset.token_id.0 == DOWN_TOKEN_ID)
        .unwrap();
    asset.asks = fact(vec![
        PriceLevelV1 {
            price_pusd_per_share: ExactDecimal("0.6".into()),
            quantity_shares: ExactDecimal("4".into()),
        },
        PriceLevelV1 {
            price_pusd_per_share: ExactDecimal("0.7".into()),
            quantity_shares: ExactDecimal("6".into()),
        },
    ])
    .into();
    let mut c = config();
    c.repair.base_loss_allowance_per_share = Decimal::new(5, 1);
    c.repair.residual_risk_weight = Decimal::from(100);
    let mut s = strategy(c, rules);
    let calculation = input.calculate(&mut s, now).unwrap();
    assert_eq!(
        diagnostic(&calculation.output).markets[0].choice,
        MarketChoiceV1::ActiveRepair
    );
    let original = diagnostic(&calculation.output).profit_allocations[0].clone();
    assert_eq!(
        original.estimated_principal_pusd,
        R::from_decimal(Decimal::new(66, 1)).public()
    );
    assert_eq!(
        original.allocated_profit_pusd,
        R::from_decimal(Decimal::new(16, 1)).public(),
        "原本金5+新本金6.6-配对10=新增亏损1.6，不是占6.6"
    );
    shared
        .dispatch_strategy(
            &mut driver.decision().unwrap(),
            &mut s,
            calculation.complete(now),
        )
        .await
        .unwrap();
    for asks in [
        vec![],
        vec![PriceLevelV1 {
            price_pusd_per_share: ExactDecimal("0.1".into()),
            quantity_shares: ExactDecimal("100".into()),
        }],
    ] {
        let action = shared
            .strategy_input(&mut driver)
            .unwrap()
            .unwrap()
            .action_before;
        let mut next = StrategyInputV1 {
            action_before: action,
            markets: input.markets.clone(),
            trade_economics: input.trade_economics.clone(),
        };
        next.markets[0]
            .observation
            .assets
            .iter_mut()
            .find(|a| a.asset.token_id.0 == DOWN_TOKEN_ID)
            .unwrap()
            .asks = fact(asks).into();
        let out = s.decide(&next, now).unwrap();
        assert_eq!(
            s.continuity().unresolved_profit_allocations[&original.global_request_id],
            original,
            "盘口消失/变便宜不释放旧场景"
        );
        assert_eq!(
            diagnostic(&out).available_profit_pusd,
            R::from_decimal(Decimal::new(14, 1)).public()
        );
        assert!(
            next.action_before
                .request_funds()
                .any(|f| f.request_identity == &original.reserve_identity
                    && f.principal_pusd == ExactDecimal("6.6".into()))
        );
    }
    driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn v2_existing_maker_quantity_beyond_gap_can_be_maintained_with_real_old_commitment() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules, _) = profitable_pool(temp.path()).await;
    shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[buy(
                &rules,
                "old-beyond-gap",
                DOWN_TOKEN_ID,
                Decimal::from(18),
                Decimal::new(4, 1),
                true,
            )],
        )
        .await
        .unwrap();
    let input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    let now = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    assert!(
        input
            .action_before
            .commitments()
            .any(|c| c.origin.intent.global_request_id() == "old-beyond-gap")
    );
    let mut c = config();
    c.repair.enable_active_repair = false;
    c.repair.residual_risk_weight = Decimal::from(100);
    let mut s = strategy(c, rules);
    let out = s.decide(&input, now).unwrap();
    assert_eq!(
        diagnostic(&out).markets[0].choice,
        MarketChoiceV1::Maintain,
        "18份旧承诺超过10份缺口，但不能用隐含零过补门否决: {:?}",
        diagnostic(&out)
    );
    assert!(intents(&out).is_empty());
    assert!(
        input
            .action_before
            .market_funds(&ConditionId(CONDITION_ID.into()))
            .total()
            >= contracts::economics::EconomicRational::from_decimal(Decimal::new(72, 1)),
        "旧承诺沿公共本地/平台资金归属保持，不要求仍在本地预留表"
    );
    driver.stop_and_drain().unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn v3_continuity_rejects_missing_or_explicit_old_rule_without_resetting_it() {
    let mut old = serde_json::to_value(StrategyContinuityV2::new_account()).unwrap();
    old.as_object_mut().unwrap().remove("rule_version");
    assert!(serde_json::from_value::<StrategyContinuityV2>(old).is_err());
    let temp = tempfile::tempdir().unwrap();
    let (_, driver, rules) = setup(temp.path(), "2000");
    for version in ["candidate_v1", "candidate_v2"] {
        let mut old = StrategyContinuityV2::new_account();
        old.rule_version = version.into();
        let result =
            ComplementaryPairV1::new(config(), rules.clone(), "no-silent-upgrade".into(), old);
        assert!(
            matches!(result,Err(StrategyErrorV1::Configuration(message)) if message.contains(version) && message.contains("candidate_v3"))
        );
    }
    driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn startup_scope_only_blocks_new_pairs_and_keeps_real_probability_sampling() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    let at = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let start = input.markets[0].observation.market.window.start_at_utc_ms;
    let mut unrestricted = strategy(config(), rules.clone());
    let original = unrestricted.decide(&input, at).unwrap();
    assert!(
        intents(&original)
            .iter()
            .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::SubmitPairedBuy { .. }))
    );
    let mut excluded =
        strategy(config(), rules.clone()).with_entry_market_range(Some(EntryMarketRangeV1 {
            start_at_utc_ms: start + 1,
            end_at_utc_ms: start + 9999,
        }));
    let blocked = excluded.decide(&input, at).unwrap();
    assert!(
        !intents(&blocked)
            .iter()
            .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::SubmitPairedBuy { .. }))
    );
    let original_diag = diagnostic(&original);
    let blocked_diag = diagnostic(&blocked);
    assert_eq!(
        original_diag.markets[0].probability,
        blocked_diag.markets[0].probability
    );
    assert!(blocked_diag.markets[0].pair_entry.evaluated);
    assert!(!blocked_diag.markets[0].pair_entry.market_eligible);
    assert!(
        blocked_diag.markets[0]
            .pair_entry
            .business_result_per_share
            .is_some()
    );
    assert_eq!(
        excluded.continuity().next_request_sequence,
        0,
        "禁止时不先生成请求再过滤"
    );
    for (lower, upper, allowed) in [(start, start + 1, true), (start - 1, start, false)] {
        let mut s =
            strategy(config(), rules.clone()).with_entry_market_range(Some(EntryMarketRangeV1 {
                start_at_utc_ms: lower,
                end_at_utc_ms: upper,
            }));
        let out = s.decide(&input, at).unwrap();
        assert_eq!(
            intents(&out)
                .iter()
                .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::SubmitPairedBuy { .. })),
            allowed
        );
    }
}

#[path = "support/initial_best_bid.rs"]
mod initial_best_bid;
