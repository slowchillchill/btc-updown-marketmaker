//! #384：同一具名组只执行一次；策略状态与显式计算均有限。
use super::*;
use ContinuousInventoryOperatingModeV1::{Legacy, PairCompletion, PairCompletionCost};
use fixture_input::{DOWN_TOKEN_ID as DOWN, UP_TOKEN_ID as UP};

fn config(mode: ContinuousInventoryOperatingModeV1) -> ContinuousInventoryConfigV1 {
    let mut c = engineering_config();
    c.operating_mode = mode;
    c.extra_quote_space_ticks = 1;
    c.inventory_skew_ticks_per_capacity = Decimal::ZERO;
    c
}
fn marker() -> (i64, PolymarketOrderbookEvent) {
    (28_000, quoted_book(UP, "0.5", "0.6"))
}
fn advance_to(f: &mut InventoryFixture, time: i64) {
    let at = BASE + time;
    loop {
        let next = [
            f.driver.next_group_at().unwrap(),
            f.next_hidden
                .as_ref()
                .map(|b| b.reference().bucket_time_utc_ms()),
            f.driver
                .next_required_logical_drive()
                .map(|r| r.required_at_utc_ms),
        ]
        .into_iter()
        .flatten()
        .min();
        if next.is_none_or(|t| t > at) {
            break;
        }
        next_combined(&mut f.driver, &mut f.hidden, &mut f.next_hidden);
        f.shared.apply_closed().unwrap();
    }
    let now = f
        .driver
        .observation_cutoff()
        .unwrap()
        .decision_time()
        .logical_time_utc_ms();
    assert!(now <= at);
    if now < at {
        f.driver.advance_before(at).unwrap();
        f.driver.observe_at(at).unwrap();
    }
    f.shared.apply_closed().unwrap();
}
async fn submit(f: &mut InventoryFixture, items: &[ReservedIntentV1]) {
    let cutoff = f.driver.observation_cutoff().unwrap();
    let at = cutoff.decision_time().logical_time_utc_ms();
    let receipts = f
        .shared
        .execute_batch_reserved(&mut f.driver.send_decision_at(cutoff, at).unwrap(), items)
        .await
        .unwrap();
    println!(
        "pair_completion_setup_receipts {}",
        serde_json::to_string(&receipts).unwrap()
    );
}
fn calculate(
    f: &mut InventoryFixture,
    label: &str,
    calls: &mut usize,
) -> StrategyCalculationV1<ContinuousInventoryDiagnosticV1> {
    *calls += 1;
    assert!(*calls <= 8, "每状态显式计算上限");
    let c = f.calculation();
    println!(
        "pair_completion_calculation {label} {}",
        serde_json::to_string(&c).unwrap()
    );
    c
}
fn finish(f: InventoryFixture, label: &str, calls: usize) {
    let ledger = f.shared.ledger().economic_read().unwrap();
    println!(
        "pair_completion_economics {label} {}",
        serde_json::to_string(&ledger.market_totals().collect::<Vec<_>>()).unwrap()
    );
    println!(
        "pair_completion_cash {label} {:?}",
        f.shared.ledger().necessary_read_v2().unwrap().cash()
    );
    let cutoff = f.driver.observation_cutoff().unwrap();
    let feedback = cutoff.private_feedback_sequence();
    let drained = f.driver.stop_and_drain();
    println!(
        "pair_completion_finish {label} calls={calls} delivered_feedback_sequence={feedback} drain={drained:?}"
    );
    drained.unwrap();
}
fn quantities(c: &StrategyCalculationV1<ContinuousInventoryDiagnosticV1>) -> [Decimal; 2] {
    details(c).1.markets[0]
        .quantities
        .clone()
        .map(|q| dec(&q.0))
}
fn seed_events(up: &str, down: Option<&str>) -> Vec<(i64, PolymarketOrderbookEvent)> {
    let mut events = vec![(
        23_000,
        trade(UP, "0.45", &(dec(up) + dec("100")).to_string()),
    )];
    if let Some(down) = down {
        events.push((
            23_001,
            trade(DOWN, "0.45", &(dec(down) + dec("100")).to_string()),
        ));
    }
    events.push(marker());
    events
}
async fn seed(f: &mut InventoryFixture, up: &str, down: Option<&str>) {
    let mut orders = vec![limit("seed-up", UP, up, "0.45")];
    if let Some(down) = down {
        orders.push(limit("seed-down", DOWN, down, "0.45"));
    }
    submit(f, &orders).await;
    advance_to(f, 25_000);
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "#384 场景1—7已由04执行，保留原证据；workspace不得重复构造"]
async fn pair_completion_formal_scenarios_01_through_07() {
    let mut states = 0;
    let mut computations = 0;
    // 1: 旧A/B在同正式输入保持原bool插点和精确价coverage。
    for enabled in [false, true] {
        let mut cfg = config(Legacy);
        cfg.enable_ordinary_active_replenishment = enabled;
        let mut f = InventoryFixture::new(vec![marker()], cfg);
        states += 1;
        let mut n = 0;
        let initial = calculate(&mut f, "1-legacy-initial", &mut n);
        assert_eq!(quantities(&initial), [dec("50"); 2]);
        submit(&mut f, &[limit("old-up", UP, "30", "0.44")]).await;
        let c = calculate(&mut f, "1-legacy-active", &mut n);
        assert_eq!(
            quantities(&c),
            if enabled {
                [dec("20"), dec("50")]
            } else {
                [Decimal::ZERO; 2]
            }
        );
        assert_eq!(
            details(&c).1.rule_version,
            if enabled {
                CONTINUOUS_INVENTORY_QUOTE_SPACE_RULE_VERSION
            } else {
                CONTINUOUS_INVENTORY_NO_ACTIVE_REPLENISHMENT_RULE_VERSION
            }
        );
        computations += n;
        finish(f, "1", n);
    }
    // 2: C/D初始仍共同50；真实登记后全部旧责任阻止重复。
    for mode in [PairCompletion, PairCompletionCost] {
        let mut f = InventoryFixture::new(vec![marker()], config(mode));
        states += 1;
        let mut n = 0;
        let c = calculate(&mut f, "2-initial", &mut n);
        assert_eq!(quantities(&c), [dec("50"); 2]);
        f.dispatch(c).await;
        let repeated = calculate(&mut f, "2-registered", &mut n);
        assert_eq!(quantities(&repeated), [Decimal::ZERO; 2]);
        computations += n;
        finish(f, "2", n);
    }
    // 3: H0/R>0不是旧initial；全价30/20共同余额20，并保entry约束。
    for mode in [PairCompletion, PairCompletionCost] {
        let mut f = InventoryFixture::new(vec![marker()], config(mode));
        states += 1;
        let mut n = 0;
        submit(
            &mut f,
            &[
                limit("old-up", UP, "30", "0.45"),
                limit("old-down", DOWN, "20", "0.43"),
            ],
        )
        .await;
        let c = calculate(&mut f, "3-old-price", &mut n);
        assert!(!details(&c).1.markets[0].initial);
        assert_eq!(quantities(&c), [dec("20"); 2]);
        assert_eq!(
            details(&c).1.markets[0].coverage,
            std::array::from_fn(|_| ExactDecimal("0".into()))
        );
        f.strategy = f.strategy.with_entry_market_range(Some(EntryMarketRangeV1 {
            start_at_utc_ms: BASE + 40_000,
            end_at_utc_ms: BASE + 50_000,
        }));
        let excluded = calculate(&mut f, "3-entry-excluded", &mut n);
        assert_eq!(quantities(&excluded), [Decimal::ZERO; 2]);
        computations += n;
        finish(f, "3", n);
    }
    // 4: H20已有缺侧20，撤单申请不能立即再获20；迟到成功沿原池配对。
    for mode in [PairCompletion, PairCompletionCost] {
        let events = vec![
            (23_000, trade(UP, "0.45", "120")),
            (26_000, trade(DOWN, "0.45", "120")),
            marker(),
        ];
        let mut f = InventoryFixture::new(events, config(mode));
        states += 1;
        let mut n = 0;
        seed(&mut f, "20", None).await;
        submit(&mut f, &[limit("old-missing", DOWN, "20", "0.45")]).await;
        let c = calculate(&mut f, "4-covered", &mut n);
        assert_eq!(
            details(&c).1.markets[0].h,
            [ExactDecimal("20".into()), ExactDecimal("0".into())]
        );
        assert_eq!(quantities(&c), [Decimal::ZERO; 2]);
        advance_to(&mut f, 26_000);
        submit(
            &mut f,
            &[ReservedIntentV1 {
                context: RequestContextV2 {
                    global_request_id: "cancel-after-match".into(),
                    caller_correlation_id: None,
                },
                intent: std::sync::Arc::new(ModelIntentV1::CancelMarketOrders {
                    condition_id: ConditionId(fixture_input::CONDITION_ID.into()),
                }),
                reserves: vec![],
            }],
        )
        .await;
        let pending = calculate(&mut f, "4-cancel-pending", &mut n);
        assert_eq!(quantities(&pending), [Decimal::ZERO; 2]);
        advance_to(&mut f, 28_500);
        let applied = calculate(&mut f, "4-late-success", &mut n);
        assert_eq!(
            details(&applied).1.markets[0].h,
            std::array::from_fn(|_| ExactDecimal("0".into()))
        );
        let totals = f
            .shared
            .ledger()
            .economic_read()
            .unwrap()
            .market_totals()
            .next()
            .unwrap();
        assert_eq!(totals.cumulative_paired_shares.0, "20");
        computations += n;
        finish(f, "4", n);
    }
    // 5: 正式NOE待应用时仍是责任；不通过计算轮询反馈。
    {
        let mut f = InventoryFixture::new(seed_events("20", None), config(PairCompletionCost));
        states += 1;
        let mut n = 0;
        submit(&mut f, &[limit("seed-up", UP, "20", "0.45")]).await;
        loop {
            next_combined(&mut f.driver, &mut f.hidden, &mut f.next_hidden);
            let read = f
                .shared
                .strategy_observation_input(&mut f.driver)
                .unwrap()
                .unwrap();
            let pending = read
                .action_before
                .obligation_trades()
                .iter()
                .any(|t| !t.success_applied());
            if pending {
                break;
            }
            assert!(
                read.action_before
                    .cutoff()
                    .decision_time()
                    .logical_time_utc_ms()
                    < BASE + 28_000
            );
        }
        let pending = calculate(&mut f, "5-observed-not-applied", &mut n);
        assert_eq!(
            details(&pending).1.markets[0].h,
            std::array::from_fn(|_| ExactDecimal("0".into()))
        );
        assert_eq!(
            details(&pending).1.markets[0].t[0],
            ExactDecimal("20".into())
        );
        advance_to(&mut f, 25_000);
        let applied = calculate(&mut f, "5-success-applied", &mut n);
        assert_eq!(
            details(&applied).1.markets[0].h[0],
            ExactDecimal("20".into())
        );
        assert_eq!(quantities(&applied), [Decimal::ZERO, dec("20")]);
        computations += n;
        finish(f, "5", n);
    }
    // 6: 不同请求的正式平均池自然配成5，剩余H5不是等待原请求两腿。
    {
        let mut f = InventoryFixture::new(seed_events("10", Some("5")), config(PairCompletionCost));
        states += 1;
        let mut n = 0;
        seed(&mut f, "10", Some("5")).await;
        let c = calculate(&mut f, "6-cross-request-pool", &mut n);
        assert_eq!(
            details(&c).1.markets[0].h,
            [ExactDecimal("5".into()), ExactDecimal("0".into())]
        );
        assert_eq!(quantities(&c), [Decimal::ZERO, dec("5")]);
        let totals = f
            .shared
            .ledger()
            .economic_read()
            .unwrap()
            .market_totals()
            .next()
            .unwrap();
        assert_eq!(totals.cumulative_paired_shares.0, "5");
        computations += n;
        finish(f, "6", n);
    }
    // 7: 配成但本金仍占投入，不能提前释放现金/1000投资限额。
    for mode in [PairCompletion, PairCompletionCost] {
        let mut f = InventoryFixture::new(seed_events("2000", Some("2000")), config(mode));
        states += 1;
        let mut n = 0;
        seed(&mut f, "2000", Some("2000")).await;
        let c = calculate(&mut f, "7-paired-capital", &mut n);
        assert_eq!(
            details(&c).1.markets[0].h,
            std::array::from_fn(|_| ExactDecimal("0".into()))
        );
        assert_eq!(
            details(&c).1.markets[0].investment,
            contracts::economics::EconomicRational::from_decimal(dec("1800")).public()
        );
        assert_eq!(quantities(&c), [Decimal::ZERO; 2]);
        assert_eq!(
            f.shared.ledger().necessary_read_v2().unwrap().cash().0,
            dec("200")
        );
        computations += n;
        finish(f, "7", n);
    }
    assert_eq!(states, 12);
    assert!(computations <= 152);
    println!(
        "pair_completion_workload scenarios=1-7 states={states} calculations={computations} explicit_injected_economic_events=0 real_runs=0"
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "#384 04后仅场景8—12一次纠偏执行；workspace不得重复构造"]
async fn pair_completion_formal_scenarios_08_through_12() {
    let mut states = 0;
    let mut computations = 0;
    // 8: 仅setup使用正式Taker，策略自身FAK关闭；真实历史费进入H池一次。
    {
        let mut f = InventoryFixture::new(vec![marker()], config(PairCompletionCost));
        states += 1;
        let mut n = 0;
        let mut item = limit("fee-history", UP, "20", "0.6");
        item.intent = std::sync::Arc::new(ModelIntentV1::SubmitSingleBuy {
            condition_id: ConditionId(fixture_input::CONDITION_ID.into()),
            order: BuyLegIntentV1 {
                token_id: TokenId(UP.into()),
                order: BuyOrderIntentV1::FakMarket {
                    spend_pusd: dec("12"),
                    worst_price_pusd_per_share: dec("0.6"),
                },
            },
        });
        item.reserves[0].reserve.immediate_fee_reserve_pusd = ExactDecimal("1".into());
        submit(&mut f, &[item]).await;
        advance_to(&mut f, 25_000);
        let c = calculate(&mut f, "8-real-historical-fee", &mut n);
        let market = f
            .shared
            .ledger()
            .necessary_read_v2()
            .unwrap()
            .market(&ConditionId(fixture_input::CONDITION_ID.into()))
            .unwrap();
        let ExecutionReadValueV1::Known(fee) = market.orphan_fee else {
            panic!("原Taker费必须已应用");
        };
        let r = contracts::economics::EconomicRational::from_decimal;
        // 正式quote微单位向下取整允许该12本金成交20.000001；不是名义12/.6。
        let expected_h = dec("20.000001");
        let asset = market
            .assets
            .as_ref()
            .unwrap()
            .iter()
            .position(|a| a.token_id == TokenId(UP.into()))
            .unwrap();
        assert_eq!(market.orphan_shares[asset], expected_h);
        assert_eq!(market.orphan_principal, r(dec("12")));
        assert_eq!(fee, r(dec("0.336")));
        assert_eq!(
            details(&c).1.markets[0].h[0],
            ExactDecimal("20.000001".into())
        );
        {
            let economic = f.shared.ledger().economic_read().unwrap();
            let mut trades = economic.trades();
            let original = trades.next().unwrap();
            assert!(original.economic_applied && original.success_applied);
            assert_eq!(original.fact.token_id, TokenId(UP.into()));
            assert_eq!(dec(&original.fact.base_quantity_shares.0), expected_h);
            assert_eq!(dec(&original.fact.quote_amount_pusd.0), dec("12"));
            assert_eq!(dec(&original.fact.immediate_taker_fee_pusd.0), dec("0.336"));
            assert_eq!(dec(&original.actual_net_fee_pusd.0), dec("0.336"));
            assert!(trades.next().is_none());
        }
        let k = market
            .orphan_principal
            .add(&fee)
            .divided_by(&r(market.orphan_shares[asset]));
        let cap = contracts::economics::EconomicRational::from_decimal(dec("0.98")).sub(&k);
        let p = dec(&details(&c).1.markets[0].prices[1].as_ref().unwrap().0);
        assert!(contracts::economics::EconomicRational::from_decimal(p) <= cap);
        assert!(contracts::economics::EconomicRational::from_decimal(p + dec("0.01")) > cap);
        let quantity = dec(&details(&c).1.markets[0].quantities[1].0);
        let exact_new = f
            .rules
            .limit_buy_cost_v1(
                &(quantity * Decimal::from(100))
                    .to_string()
                    .parse::<num_bigint::BigUint>()
                    .unwrap(),
                (p / dec("0.01")).to_string().parse::<u16>().unwrap(),
                &ExactDecimal("0.01".into()),
                true,
            )
            .unwrap();
        let new_cost = r(exact_new
            .principal_micro
            .to_string()
            .parse::<Decimal>()
            .unwrap())
        .divided_by(&r(dec("1000000")))
        .add(
            &r(exact_new
                .fee_1e5_units
                .to_string()
                .parse::<Decimal>()
                .unwrap())
            .divided_by(&r(dec("100000"))),
        );
        assert!(new_cost <= cap.mul(&r(quantity)));
        assert!(!f.strategy.config().repair.enable_active_repair);
        let mut bytes = vec![];
        f.shared.ledger().write_checkpoint(&mut bytes).unwrap();
        let mut old: ciborium::Value = ciborium::from_reader(bytes.as_slice()).unwrap();
        fn remove_fee(v: &mut ciborium::Value) -> usize {
            match v {
                ciborium::Value::Map(fields) => {
                    let before = fields.len();
                    fields.retain(|(k, _)| !matches!(k, ciborium::Value::Text(k) if k == "orphan_fee" || k == "pool_contribution_shares"));
                    before - fields.len()
                        + fields.iter_mut().map(|(_, v)| remove_fee(v)).sum::<usize>()
                }
                ciborium::Value::Array(v) => v.iter_mut().map(remove_fee).sum(),
                _ => 0,
            }
        }
        assert!(remove_fee(&mut old) > 0);
        let mut saved = vec![];
        ciborium::into_writer(&old, &mut saved).unwrap();
        let restored = LedgerWriterV1::restore_checkpoint(saved.as_slice()).unwrap();
        assert!(matches!(
            restored
                .necessary_read_v2()
                .unwrap()
                .market(&ConditionId(fixture_input::CONDITION_ID.into()))
                .unwrap()
                .orphan_fee,
            ExecutionReadValueV1::Unavailable
        ));
        f.shared = SharedExecutionV2::from_opening(restored, f.observation.clone())
            .unwrap()
            .unwrap();
        let unavailable = calculate(&mut f, "8-old-checkpoint-unknown", &mut n);
        assert_eq!(quantities(&unavailable), [Decimal::ZERO; 2]);
        assert!(
            details(&unavailable).1.markets[0]
                .unavailable
                .iter()
                .any(|s| s == "account_obligation_unknown")
        );
        computations += n;
        finish(f, "8", n);
    }
    // 9: 真实已配本金1989+未配9留下现金2；最高成本价买不起最小5，.40仍可行。
    {
        let mut cfg = config(PairCompletionCost);
        cfg.market_investment_limit_pusd = dec("2000");
        let mut f = InventoryFixture::new(seed_events("2230", Some("2210")), cfg);
        states += 1;
        let mut n = 0;
        seed(&mut f, "2230", Some("2210")).await;
        let c = calculate(&mut f, "9-price-quantity-cash", &mut n);
        assert_eq!(details(&c).1.markets[0].h[0], ExactDecimal("20".into()));
        assert_eq!(
            f.shared.ledger().necessary_read_v2().unwrap().cash().0,
            dec("2")
        );
        assert_eq!(
            dec(&details(&c).1.markets[0].prices[1].as_ref().unwrap().0),
            dec("0.40")
        );
        assert_eq!(quantities(&c), [Decimal::ZERO, dec("5")]);
        computations += n;
        finish(f, "9", n);
    }
    // 10: 较多侧tick也是D普通成本依赖；冻结原价原量只能失效，不能替代。
    {
        let mut events = seed_events("20", None);
        events.insert(
            1,
            (
                26_000,
                PolymarketOrderbookEvent::TickSizeChange {
                    asset_id: UP.into(),
                    old_tick_size: Some("0.01".into()),
                    new_tick_size: "0.02".into(),
                },
            ),
        );
        let mut f = InventoryFixture::new(events, config(PairCompletionCost));
        states += 1;
        let mut n = 0;
        seed(&mut f, "20", None).await;
        let c = calculate(&mut f, "10-frozen", &mut n);
        let frozen = serde_json::to_value(&c).unwrap();
        advance_to(&mut f, 27_000);
        let read = f.shared.strategy_current_read(&f.driver).unwrap().unwrap();
        let at = read
            .action_before
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        let q = f.strategy.qualify_intent(&read, &c, 0, at).unwrap();
        assert!(
            matches!(q, IntentQualificationV1::Invalidated(IntentInvalidationReasonV1 { ref reason_code, .. }) if reason_code == "pair_completion_ticks_changed")
        );
        assert_eq!(serde_json::to_value(&c).unwrap(), frozen);
        computations += n;
        finish(f, "10", n);
    }
    // 11: 普通仅缺侧；原受限实际H仍走独立held修复，不受新普通价目标封锁。
    for restricted in [false, true] {
        let mut cfg = config(PairCompletionCost);
        if restricted {
            cfg.per_market_orphan_limit_pusd = dec("1");
        }
        let mut f = InventoryFixture::new(seed_events("20", None), cfg);
        states += 1;
        let mut n = 0;
        seed(&mut f, "20", None).await;
        let c = calculate(&mut f, "11-normal-held", &mut n);
        assert_eq!(quantities(&c), [Decimal::ZERO, dec("20")]);
        assert_eq!(
            details(&c).1.action_dependencies[0].independent_held_token,
            restricted.then(|| TokenId(UP.into()))
        );
        assert_eq!(
            dec(&details(&c).1.markets[0].prices[1].as_ref().unwrap().0),
            if restricted { dec("0.44") } else { dec("0.53") }
        );
        computations += n;
        finish(f, "11", n);
    }
    // 12: entry外H0/R余额不绕准入；无新量仍维护旧单。另状态沿原实际回撤终止/held。
    {
        let empty = PolymarketOrderbookEvent::Book {
            asset_id: UP.into(),
            bids: vec![],
            asks: vec![],
            last_trade_price: None,
        };
        let mut f = InventoryFixture::new(vec![(23_000, empty), marker()], config(PairCompletion));
        states += 1;
        let mut n = 0;
        submit(&mut f, &[limit("window-old", UP, "20", "0.45")]).await;
        f.strategy = f.strategy.with_entry_market_range(Some(EntryMarketRangeV1 {
            start_at_utc_ms: BASE + 40_000,
            end_at_utc_ms: BASE + 50_000,
        }));
        let c = calculate(&mut f, "12-entry", &mut n);
        assert_eq!(quantities(&c), [Decimal::ZERO; 2]);
        advance_to(&mut f, 24_000);
        let c = calculate(&mut f, "12-maintenance", &mut n);
        assert_eq!(quantities(&c), [Decimal::ZERO; 2]);
        assert!(
            details(&c)
                .0
                .iter()
                .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::CancelOrders { .. }))
        );
        computations += n;
        finish(f, "12-entry", n);
    }
    {
        let mut f = InventoryFixture::new(
            vec![
                (23_000, quoted_book(UP, "0.70", "0.90")),
                (26_000, trade(UP, "0.80", "1000")),
                (28_000, quoted_book(UP, "0.01", "0.03")),
            ],
            config(PairCompletionCost),
        );
        states += 1;
        let mut n = 0;
        advance_to(&mut f, 24_000);
        submit(&mut f, &[limit("drawdown", UP, "1000", "0.80")]).await;
        advance_to(&mut f, 29_000);
        let c = calculate(&mut f, "12-termination", &mut n);
        assert!(details(&c).1.terminating);
        assert_eq!(quantities(&c)[0], Decimal::ZERO);
        assert!(quantities(&c)[1] > Decimal::ZERO);
        computations += n;
        finish(f, "12-termination", n);
    }
    assert_eq!(states, 7);
    assert!(computations <= 152);
    println!(
        "pair_completion_workload states={states} calculations={computations} explicit_injected_economic_events=1 checkpoint_opening_idempotent_reapplication=1 real_runs=0"
    );
}
