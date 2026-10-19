use super::*;

fn bb_config() -> ComplementaryPairConfigV3 {
    let mut c = config();
    c.quote.initial_pair_price_reference = InitialPairPriceReferenceV1::BestBid;
    c.probability.binance_innovation_weight = 0.0;
    c.quote.base_half_spread = Decimal::ZERO;
    c.quote.uncertainty_spread_weight = Decimal::ZERO;
    c.quote.signal_spread_weight = Decimal::ZERO;
    c.repair.enable_active_repair = false;
    c
}
fn levels(rows: &[(&str, &str)]) -> Arc<ObservedFactV1<Vec<PriceLevelV1>>> {
    fact(
        rows.iter()
            .map(|(p, q)| PriceLevelV1 {
                price_pusd_per_share: ExactDecimal((*p).into()),
                quantity_shares: ExactDecimal((*q).into()),
            })
            .collect(),
    )
    .into()
}
fn asset(input: &mut StrategyInputV1, side: usize) -> &mut AssetBusinessObservationV1 {
    let token = [UP_TOKEN_ID, DOWN_TOKEN_ID][side];
    input.markets[0]
        .observation
        .assets
        .iter_mut()
        .find(|a| a.asset.token_id.0 == token)
        .unwrap()
}
fn books(input: &mut StrategyInputV1, bids: [&[(&str, &str)]; 2], asks: [&[(&str, &str)]; 2]) {
    for side in 0..2 {
        let a = asset(input, side);
        a.bids = levels(bids[side]);
        a.asks = levels(asks[side]);
        a.tick = fact(ExactDecimal("0.01".into()));
    }
}
fn prices(out: &StrategyOutputV1<ComplementaryPairDiagnosticV1>) -> Vec<(Decimal, Decimal)> {
    intents(out)
        .iter()
        .flat_map(|i| match i.intent.as_ref() {
            ModelIntentV1::SubmitPairedBuy { orders, .. } => orders
                .iter()
                .map(|leg| match leg.order {
                    BuyOrderIntentV1::GtcLimit {
                        limit_price_pusd_per_share,
                        size_shares,
                        post_only: true,
                    } => (limit_price_pusd_per_share, size_shares),
                    _ => panic!("初始配对必须是Maker"),
                })
                .collect(),
            _ => vec![],
        })
        .collect()
}
fn cancelled(out: &StrategyOutputV1<ComplementaryPairDiagnosticV1>) -> Vec<String> {
    intents(out)
        .iter()
        .flat_map(|i| match i.intent.as_ref() {
            ModelIntentV1::CancelOrders { platform_order_ids } => platform_order_ids.clone(),
            _ => vec![],
        })
        .collect()
}
fn targets(out: &StrategyOutputV1<ComplementaryPairDiagnosticV1>) -> [Option<Decimal>; 2] {
    diagnostic(out).markets[0]
        .pair_entry
        .target_prices
        .as_ref()
        .unwrap()
        .clone()
        .map(|p| p.map(|p| Decimal::from_str_exact(&p.0).unwrap()))
}
#[test]
fn v3_requires_explicit_42nd_field_and_preserves_legacy_material() {
    let old =
        include_str!("../../configs/strategies/complementary_pair/synthetic-engineering.toml");
    let err = ComplementaryPairConfigV3::parse_toml(old)
        .unwrap_err()
        .to_string();
    assert!(err.contains("initial_pair_price_reference"));
    let mut c = serde_json::to_value(bb_config()).unwrap();
    let count: usize = c
        .as_object()
        .unwrap()
        .values()
        .map(|v| v.as_object().unwrap().len())
        .sum();
    assert_eq!(count, 42);
    c["quote"]["initial_pair_price_reference"] = "bid_plus_tick".into();
    assert!(serde_json::from_value::<ComplementaryPairConfigV3>(c).is_err());
}
#[tokio::test(flavor = "current_thread")]
async fn v3_hand_prices_clipping_empty_ask_and_positive_bid_selection() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let mut input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    // 报价只读同一冻结观察；概率仍由原概率入口生成。
    let mut c = bb_config();
    c.probability.initial_sigma_usd_per_sqrt_second = 0.0;
    c.probability.estimation_error_usd = 0.0;
    for (up, down, ask_up, ask_down, expect) in [
        ("0.65", "0.30", Some("0.66"), Some("0.31"), ["0.65", "0.30"]),
        ("0.657", "0.309", None, None, ["0.65", "0.30"]),
        ("0.99", "0.30", Some("0.63"), None, ["0.62", "0.30"]),
        ("1", "0.30", None, None, ["0.99", "0.30"]),
    ] {
        let a = ask_up.map(|p| vec![(p, "10")]).unwrap_or_default();
        let b = ask_down.map(|p| vec![(p, "10")]).unwrap_or_default();
        books(
            &mut input,
            [&[(up, "10"), ("0.999", "0")], &[(down, "10")]],
            [&a, &b],
        );
        let out = strategy(c.clone(), rules.clone())
            .decide(&input, 22000)
            .unwrap();
        assert_eq!(
            targets(&out),
            expect.map(|p| Some(Decimal::from_str_exact(p).unwrap()))
        );
        assert_eq!(
            diagnostic(&out).markets[0].pair_entry.price_reference,
            Some(InitialPairPriceReferenceV1::BestBid)
        );
    }
    // 概率由合成现价形成0.60档公平目标；同盘口买一目标仍为0.65。
    input = estimated(input, "100007.3");
    c.probability.initial_sigma_usd_per_sqrt_second = 10.0;
    c.quote.initial_pair_price_reference = InitialPairPriceReferenceV1::FairValue;
    books(
        &mut input,
        [&[("0.65", "10")], &[("0.30", "10")]],
        [&[("0.66", "10")], &[("0.31", "10")]],
    );
    let fv = strategy(c, rules).decide(&input, 22000).unwrap();
    assert_eq!(
        targets(&fv),
        [Some(Decimal::new(6, 1)), Some(Decimal::new(3, 1))]
    );
    driver.stop_and_drain().unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn v3_missing_bid_is_per_side_and_valid_bids_still_require_probability() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let mut input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    for missing in 0..3 {
        books(
            &mut input,
            [&[("0.65", "10")], &[("0.30", "10")]],
            [&[], &[]],
        );
        asset(&mut input, 0).bids = match missing {
            0 => Default::default(),
            1 => levels(&[]),
            _ => levels(&[("0.65", "0")]),
        };
        let out = strategy(bb_config(), rules.clone())
            .decide(&input, 22000)
            .unwrap();
        assert_eq!(targets(&out), [None, Some(Decimal::new(3, 1))]);
        assert!(prices(&out).is_empty());
        assert_eq!(
            diagnostic(&out).markets[0].pair_entry.target_unavailable,
            Some([
                Some(InitialPairPriceUnavailableV1::MissingPositiveBid),
                None
            ])
        );
    }
    books(
        &mut input,
        [&[("0.65", "10")], &[("0.30", "10")]],
        [&[], &[]],
    );
    input.markets[0].observation.effective_reference = EffectiveMarketReferenceV1::Missing;
    let out = strategy(bb_config(), rules).decide(&input, 22000).unwrap();
    assert!(prices(&out).is_empty());
    assert!(
        diagnostic(&out).markets[0]
            .unavailable
            .contains(&ActionUnavailableV1::EstimateRequired)
    );
    driver.stop_and_drain().unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn v3_real_pending_low_price_reuse_no_chase_and_per_side_maintenance() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[
                buy(
                    &rules,
                    "old-up",
                    UP_TOKEN_ID,
                    Decimal::from(10),
                    Decimal::new(4, 1),
                    true,
                ),
                buy(
                    &rules,
                    "old-down",
                    DOWN_TOKEN_ID,
                    Decimal::from(10),
                    Decimal::new(2, 1),
                    true,
                ),
            ],
        )
        .await
        .unwrap();
    let mut input = loop {
        driver.next_group().unwrap().unwrap();
        let input = estimated(
            shared.strategy_input(&mut driver).unwrap().unwrap(),
            "100000",
        );
        if input.action_before.orders().count() >= 2 {
            break input;
        }
    };
    let now = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    books(
        &mut input,
        [&[("0.65", "10")], &[("0.30", "10")]],
        [&[("0.66", "10")], &[("0.31", "10")]],
    );
    let out = strategy(bb_config(), rules.clone())
        .decide(&input, now)
        .unwrap();
    assert!(
        cancelled(&out).is_empty(),
        "{:?} {:?}",
        diagnostic(&out),
        cancelled(&out)
    );
    assert!(prices(&out).is_empty(), "两侧已有数量，不追价/重复下单");
    assert_eq!(
        diagnostic(&out).markets[0]
            .pair_entry
            .reused_prices
            .as_ref()
            .unwrap()
            .clone()
            .map(|p| Decimal::from_str_exact(&p.0).unwrap()),
        [Decimal::new(4, 1), Decimal::new(2, 1)]
    );
    // 只有Up缺目标时，一般维护只撤Up。禁开范围不阻断旧义务维护。
    asset(&mut input, 0).bids = levels(&[]);
    let start = input.markets[0].observation.market.window.start_at_utc_ms;
    let out = strategy(bb_config(), rules.clone())
        .with_entry_market_range(Some(EntryMarketRangeV1 {
            start_at_utc_ms: start + 1,
            end_at_utc_ms: start + 2,
        }))
        .decide(&input, now)
        .unwrap();
    assert_eq!(cancelled(&out).len(), 1, "另一侧不能因Up缺目标撤销");
    // 买一下移仍撤高价旧单；抬高经营门会阻止new_pair内部撤单，一般维护仍应撤。
    books(
        &mut input,
        [&[("0.30", "10")], &[("0.30", "10")]],
        [&[], &[]],
    );
    let mut c = bb_config();
    c.quote.minimum_business_result_per_share = Decimal::from(100);
    let out = strategy(c, rules).decide(&input, now).unwrap();
    assert_eq!(cancelled(&out).len(), 1);
    assert_eq!(
        diagnostic(&out).markets[0].pair_entry.gate_passed,
        Some(false)
    );
    driver.stop_and_drain().unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn v3_final_reused_prices_drive_business_cash_investment_and_risk() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[buy(
                &rules,
                "reuse-up",
                UP_TOKEN_ID,
                Decimal::from(5),
                Decimal::new(4, 1),
                true,
            )],
        )
        .await
        .unwrap();
    let mut input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    books(
        &mut input,
        [&[("0.65", "10")], &[("0.30", "10")]],
        [&[], &[]],
    );
    let mut c = bb_config();
    c.quote.joint_fill_scenario_weights = [1.0, 0.0, 0.0, 0.0];
    c.quote.minimum_business_result_per_share = Decimal::new(2, 1);
    c.quote.base_pair_shares = Decimal::from(10000);
    c.risk.per_market_orphan_limit_pusd = Decimal::from(10);
    c.risk.account_orphan_limit_pusd = Decimal::from(10);
    let out = strategy(c.clone(), rules.clone())
        .decide(&input, 22000)
        .unwrap();
    let d = &diagnostic(&out).markets[0].pair_entry;
    assert!(
        (d.business_result_per_share.unwrap() - 0.3).abs() < 1e-12,
        "必须用最终0.4+0.3，不能用目标0.65+0.3"
    );
    assert_eq!(d.gate_passed, Some(true));
    let ps = prices(&out);
    assert_eq!(ps.len(), 2);
    assert_eq!(ps[0].0, Decimal::new(4, 1));
    // 旧Up2 pUSD承诺仍占风险；新增双腿不得突破10。
    assert!(ps[0].1 * Decimal::new(4, 1) + Decimal::from(2) <= Decimal::from(10));
    let total: Decimal = intents(&out)
        .iter()
        .flat_map(|i| &i.reserves)
        .map(|r| {
            Decimal::from_str_exact(&r.reserve.requested_principal_pusd.0).unwrap()
                + Decimal::from_str_exact(&r.reserve.immediate_fee_reserve_pusd.0).unwrap()
        })
        .sum();
    assert!(total + Decimal::from(2) <= Decimal::from(1000));
    assert!(
        total
            <= Decimal::from_str_exact(
                &input.action_before.economics().cash.actual_available_pusd.0
            )
            .unwrap()
    );
    c.quote.minimum_business_result_per_share = Decimal::new(31, 2);
    let blocked = strategy(c, rules).decide(&input, 22000).unwrap();
    assert!(prices(&blocked).is_empty());
    driver.stop_and_drain().unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn v3_repair_keeps_fair_formula_and_local_qualified_exception() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules, _) = profitable_pool(temp.path()).await;
    shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[buy(
                &rules,
                "old-repair",
                DOWN_TOKEN_ID,
                Decimal::from(18),
                Decimal::new(4, 1),
                true,
            )],
        )
        .await
        .unwrap();
    let mut input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    books(
        &mut input,
        [&[("0.1", "10")], &[]],
        [&[("0.99", "10")], &[("0.99", "10")]],
    );
    let now = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let mut c = bb_config();
    c.repair.residual_risk_weight = Decimal::from(100);
    let bb = strategy(c.clone(), rules.clone())
        .decide(&input, now)
        .unwrap();
    c.quote.initial_pair_price_reference = InitialPairPriceReferenceV1::FairValue;
    let fv = strategy(c, rules).decide(&input, now).unwrap();
    assert_eq!(diagnostic(&bb).markets[0].choice, MarketChoiceV1::Maintain);
    assert_eq!(
        serde_json::to_value(&diagnostic(&fv).markets[0].comparison).unwrap(),
        serde_json::to_value(&diagnostic(&bb).markets[0].comparison).unwrap()
    );
    assert!(
        !diagnostic(&bb).markets[0].pair_entry.evaluated,
        "repair Maintain优先阻止new_pair"
    );
    assert!(
        intents(&bb).is_empty(),
        "缺买一但现行局部qualified仍保留修复单"
    );
    driver.stop_and_drain().unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn v3_historical_single_partial_fill_loses_repair_use_and_is_cancelled_by_new_pair() {
    let temp = tempfile::tempdir().unwrap();
    // 实际两批Up各10份；单张Down28份只成交20，8份余单仍沿原公共链存在。
    let (mut shared, mut driver, rules, raw) = profitable_pool_shape(temp.path(), 20, 28).await;
    let mut input = estimated(raw, "100000");
    assert!(
        input
            .action_before
            .economics()
            .markets
            .iter()
            .any(|m| m.paired_shares.0 == "20")
    );
    assert!(
        input
            .action_before
            .commitments()
            .any(|c| c.origin.intent.global_request_id() == "pool-down")
    );
    books(
        &mut input,
        [&[("0.4", "10")], &[("0.1", "10")]],
        [&[("0.99", "10")], &[("0.99", "10")]],
    );
    let now = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let bb = strategy(bb_config(), rules.clone())
        .decide(&input, now)
        .unwrap();
    assert!(diagnostic(&bb).markets[0].pair_entry.evaluated);
    assert_eq!(
        Decimal::from_str_exact(
            &diagnostic(&bb).markets[0]
                .pair_entry
                .reused_prices
                .as_ref()
                .unwrap()[1]
                .0
        )
        .unwrap(),
        Decimal::new(1, 1)
    );
    assert!(!cancelled(&bb).is_empty(), "旧single来源不豁免新目标撤单");
    let mut fv = bb_config();
    fv.quote.initial_pair_price_reference = InitialPairPriceReferenceV1::FairValue;
    let old = strategy(fv, rules).decide(&input, now).unwrap();
    assert!(cancelled(&old).is_empty(), "0.2低于旧公平目标，应能复用");
    // 公共原commitment保留，不因发出撤单就释放。
    assert!(
        shared
            .strategy_input(&mut driver)
            .unwrap()
            .unwrap()
            .action_before
            .commitments()
            .any(|c| c.origin.intent.global_request_id() == "pool-down")
    );
    driver.stop_and_drain().unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn v3_retain_orphan_still_allows_new_pair_without_global_qualified_exemption() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules, _) = profitable_pool(temp.path()).await;
    shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[buy(
                &rules,
                "old-repair-low",
                DOWN_TOKEN_ID,
                Decimal::from(5),
                Decimal::new(4, 1),
                true,
            )],
        )
        .await
        .unwrap();
    let mut input = loop {
        driver.next_group().unwrap().unwrap();
        let input = estimated(
            shared.strategy_input(&mut driver).unwrap().unwrap(),
            "100000",
        );
        if input.action_before.orders().any(|o| {
            o.origin
                .as_ref()
                .is_some_and(|o| o.intent.global_request_id() == "old-repair-low")
        }) {
            break input;
        }
    };
    books(
        &mut input,
        [&[("0.1", "10")], &[("0.1", "10")]],
        [&[("0.99", "10")], &[("0.99", "10")]],
    );
    let now = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let mut c = bb_config();
    c.repair.maker_path_ms = 1_000_000;
    c.repair.residual_risk_weight = Decimal::ZERO;
    let out = strategy(c, rules).decide(&input, now).unwrap();
    assert!(
        diagnostic(&out).markets[0].pair_entry.evaluated,
        "无repair动作/Maintain不统一禁止孤腿新开"
    );
    assert!(!prices(&out).is_empty());
    assert!(
        !cancelled(&out).is_empty(),
        "new_pair不继承一般维护的qualified局部豁免"
    );
    // 本地未决请求仍有真实义务；没有来源状态复制或提前释放。
    assert!(
        input
            .action_before
            .commitments()
            .any(|c| c.origin.intent.global_request_id() == "old-repair-low")
    );
    driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn v3_does_not_add_a_pair_price_sum_gate() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules) = setup(temp.path(), "2000");
    let mut input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    books(
        &mut input,
        [&[("0.65", "10")], &[("0.65", "10")]],
        [&[], &[]],
    );
    let mut c = bb_config();
    c.quote.minimum_business_result_per_share = Decimal::new(-4, 1);
    c.quote.joint_fill_scenario_weights = [1.0, 0.0, 0.0, 0.0];
    let out = strategy(c, rules).decide(&input, 22000).unwrap();
    let ps = prices(&out);
    assert_eq!(ps.len(), 2, "原经营门允许该显式负值时，不另加sum<=1门");
    assert!(ps[0].0 + ps[1].0 > Decimal::ONE);
    driver.stop_and_drain().unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn v3_repair_new_maker_uses_same_fair_formula_in_both_modes() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules, _) = profitable_pool(temp.path()).await;
    let mut input = estimated(
        shared.strategy_input(&mut driver).unwrap().unwrap(),
        "100000",
    );
    books(
        &mut input,
        [&[("0.1", "10")], &[("0.1", "10")]],
        [&[("0.99", "10")], &[("0.99", "10")]],
    );
    let now = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let mut c = bb_config();
    c.repair.residual_risk_weight = Decimal::from(100);
    let bb = strategy(c.clone(), rules.clone())
        .decide(&input, now)
        .unwrap();
    c.quote.initial_pair_price_reference = InitialPairPriceReferenceV1::FairValue;
    let fv = strategy(c, rules).decide(&input, now).unwrap();
    assert_eq!(
        diagnostic(&bb).markets[0].choice,
        MarketChoiceV1::MakerRepair
    );
    assert!(!diagnostic(&bb).markets[0].pair_entry.evaluated);
    let submitted = |out: &StrategyOutputV1<ComplementaryPairDiagnosticV1>| {
        intents(out)
            .iter()
            .map(|i| serde_json::to_value(i.intent.as_ref()).unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(submitted(&bb), submitted(&fv));
    assert!(intents(&bb).iter().any(|i|matches!(i.intent.as_ref(),ModelIntentV1::SubmitSingleBuy{order:BuyLegIntentV1{order:BuyOrderIntentV1::GtcLimit{limit_price_pusd_per_share,..},..},..} if *limit_price_pusd_per_share>Decimal::new(1,1))));
    driver.stop_and_drain().unwrap();
}
