use super::*;
fn config_v5(enabled: bool) -> ComplementaryPairConfigV5 {
    let mut c: ComplementaryPairConfigV5 = serde_json::from_str(include_str!(
        "../../configs/strategies/complementary_pair/public-low-price-off.json"
    ))
    .unwrap();
    c.initial_pair_low_price_veto = enabled;
    c
}
fn v5(rules: Arc<SealedProcessRuleSnapshotV3>, enabled: bool) -> ComplementaryPairV1 {
    ComplementaryPairV1::new_low_price(
        config_v5(enabled),
        rules,
        "low-price-test".into(),
        StrategyContinuityV2::new_low_price_account(),
    )
    .unwrap()
}
fn advance(driver: &mut PublicDriver, m: &Material, at: i64) {
    let last = m
        .groups
        .iter()
        .rev()
        .find(|(_, t)| *t == at)
        .unwrap()
        .0
        .group_position;
    while driver
        .next_group()
        .unwrap()
        .unwrap()
        .0
        .group()
        .group_position
        < last
    {}
}
async fn old_orders(
    shared: &mut SharedExecutionV2,
    driver: &mut PublicDriver,
    rules: &SealedProcessRuleSnapshotV3,
    orders: &[(&str, Decimal, bool)],
) {
    let batch: Vec<_> = orders
        .iter()
        .enumerate()
        .map(|(i, (token, price, post))| {
            buy(
                rules,
                &format!("old-{i}"),
                token,
                Decimal::from(10),
                *price,
                *post,
            )
        })
        .collect();
    if !batch.is_empty() {
        shared
            .execute_batch_reserved(&mut driver.decision().unwrap(), &batch)
            .await
            .unwrap();
    }
}
fn pairs(out: &StrategyOutputV1<ComplementaryPairDiagnosticV1>) -> usize {
    intents(out)
        .iter()
        .filter(|i| matches!(i.intent.as_ref(), ModelIntentV1::SubmitPairedBuy { .. }))
        .count()
}
#[tokio::test(flavor = "current_thread")]
async fn public_v5_off_matches_v4_c0_requests_cancels_and_cash() {
    let mut results = vec![];
    for new_version in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (mut shared, mut driver, rules, m) = setup_public(temp.path());
        advance(&mut driver, &m, T + 202);
        old_orders(
            &mut shared,
            &mut driver,
            &rules,
            &[
                (UP_TOKEN_ID, Decimal::new(4, 1), true),
                (DOWN_TOKEN_ID, Decimal::new(5, 1), true),
            ],
        )
        .await;
        advance(&mut driver, &m, T + 4002);
        let input = shared.strategy_input(&mut driver).unwrap().unwrap();
        let mut strategy = if new_version {
            v5(rules.clone(), false)
        } else {
            ComplementaryPairV1::new_public(
                ComplementaryPairConfigV4 {
                    parameters: config_v5(false).parameters,
                    blb_decrease_veto: false,
                },
                rules.clone(),
                "low-price-test".into(),
                StrategyContinuityV2::new_public_account(),
            )
            .unwrap()
        };
        let out = strategy.decide(&input, T + 4017).unwrap();
        let d = &diagnostic(&out).markets[0];
        assert_eq!(pairs(&out), 1);
        assert!(
            intents(&out)
                .iter()
                .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::CancelOrders { .. }))
        );
        if new_version {
            assert_eq!(
                d.pair_entry.low_price_entry.unwrap(),
                InitialPairLowPriceEvaluationV5::Disabled
            );
        } else {
            assert!(d.pair_entry.low_price_entry.is_none());
        }
        let requests = serde_json::to_value(intents(&out)).unwrap();
        shared
            .execute_batch_reserved(&mut driver.decision().unwrap(), intents(&out))
            .await
            .unwrap();
        let after = shared.strategy_input(&mut driver).unwrap().unwrap();
        results.push((
            requests,
            serde_json::to_value(&after.action_before.economics().cash).unwrap(),
        ));
        driver.stop_and_drain().unwrap();
    }
    assert_eq!(results[0], results[1]);
}
#[tokio::test(flavor = "current_thread")]
async fn public_v5_each_low_leg_vetoes_whole_pair_after_reuse_and_keeps_tail_cancel() {
    for low in [UP_TOKEN_ID, DOWN_TOKEN_ID] {
        let temp = tempfile::tempdir().unwrap();
        let (mut shared, mut driver, rules, m) = setup_public(temp.path());
        advance(&mut driver, &m, T + 202);
        let other = if low == UP_TOKEN_ID {
            DOWN_TOKEN_ID
        } else {
            UP_TOKEN_ID
        };
        old_orders(
            &mut shared,
            &mut driver,
            &rules,
            &[
                (low, Decimal::new(4, 1), true),
                (other, Decimal::new(5, 1), true),
            ],
        )
        .await;
        advance(&mut driver, &m, T + 4002);
        let input = shared.strategy_input(&mut driver).unwrap().unwrap();
        let out = v5(rules, true).decide(&input, T + 4017).unwrap();
        let d = &diagnostic(&out).markets[0];
        let entry = d.pair_entry.low_price_entry.as_ref().unwrap();
        assert_eq!(*entry, InitialPairLowPriceEvaluationV5::VetoBelowBestBid);
        assert_eq!(pairs(&out), 0);
        // 策略价格/诊断固定[Up,Down]，原始F0资产按token数值排序。
        let side = if low == UP_TOKEN_ID { 0 } else { 1 };
        assert_eq!(
            d.pair_entry.public_entry.as_ref().unwrap().pm_quotes[side][0],
            Some(Decimal::new(45, 2))
        );
        assert_eq!(
            d.pair_entry.target_prices.as_ref().unwrap()[side],
            Some(ExactDecimal("0.45".into()))
        );
        assert_eq!(
            d.pair_entry.reused_prices.as_ref().unwrap()[side],
            ExactDecimal("0.4".into())
        );
        assert!(
            d.pair_entry
                .public_entry
                .as_ref()
                .unwrap()
                .new_pair_request_id
                .is_none()
        );
        let cancels: Vec<_> = intents(&out)
            .iter()
            .filter_map(|i| match i.intent.as_ref() {
                ModelIntentV1::CancelOrders { platform_order_ids } => Some(platform_order_ids),
                _ => None,
            })
            .collect();
        assert_eq!(cancels.len(), 1);
        assert_eq!(cancels[0].len(), 1);
        shared
            .execute_batch_reserved(&mut driver.decision().unwrap(), intents(&out))
            .await
            .unwrap();
        driver.stop_and_drain().unwrap();
    }
}
#[tokio::test(flavor = "current_thread")]
async fn public_v5_equal_quotes_pass_and_link_real_request_and_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules, m) = setup_public(temp.path());
    advance(&mut driver, &m, T + 202);
    let input = shared.strategy_input(&mut driver).unwrap().unwrap();
    let mut strategy = v5(rules.clone(), true);
    let out = strategy.decide(&input, T + 217).unwrap();
    let d = &diagnostic(&out).markets[0];
    assert_eq!(
        d.pair_entry.low_price_entry.unwrap(),
        InitialPairLowPriceEvaluationV5::NotBelowBestBid
    );

    assert_eq!(pairs(&out), 1);
    let id = &intents(&out)[0].context.global_request_id;
    assert_eq!(
        d.pair_entry
            .public_entry
            .as_ref()
            .unwrap()
            .new_pair_request_id
            .as_ref(),
        Some(id)
    );
    let receipts = shared
        .execute_batch_reserved(&mut driver.decision().unwrap(), intents(&out))
        .await
        .unwrap();
    assert!(!receipts.is_empty());
    // 仅核对现有策略checkpoint适配；不建立日入口续跑。
    let mut bytes = vec![];
    strategy.write_strategy_checkpoint(&mut bytes).unwrap();
    let mut restored = v5(rules, true);
    restored
        .restore_strategy_checkpoint(&mut bytes.as_slice())
        .unwrap();
    assert_eq!(
        serde_json::to_value(strategy.checkpoint()).unwrap(),
        serde_json::to_value(restored.checkpoint()).unwrap()
    );
    assert_eq!(
        serde_json::to_value(strategy.decide(&input, T + 217).unwrap()).unwrap(),
        serde_json::to_value(restored.decide(&input, T + 217).unwrap()).unwrap()
    );
    driver.stop_and_drain().unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn public_v5_maintenance_and_repair_priority_do_not_evaluate_new_condition() {
    for repair in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (mut shared, mut driver, rules, m) = setup_public(temp.path());
        advance(&mut driver, &m, T + 202);
        let orders = if repair {
            vec![(UP_TOKEN_ID, Decimal::new(6, 1), false)]
        } else {
            vec![
                (UP_TOKEN_ID, Decimal::new(4, 1), true),
                (DOWN_TOKEN_ID, Decimal::new(4, 1), true),
            ]
        };
        old_orders(&mut shared, &mut driver, &rules, &orders).await;
        advance(&mut driver, &m, T + 4002);
        let input = shared.strategy_input(&mut driver).unwrap().unwrap();
        let out = v5(rules, true).decide(&input, T + 4017).unwrap();
        let d = &diagnostic(&out).markets[0];
        assert!(d.pair_entry.low_price_entry.is_none(), "{d:?}");
        assert_eq!(pairs(&out), 0);
        if repair {
            assert!(!d.pair_entry.evaluated, "{d:?}");
        } else {
            assert_eq!(d.pair_entry.no_new_needed, Some(true));
        }
        driver.stop_and_drain().unwrap();
    }
}
#[tokio::test(flavor = "current_thread")]
async fn public_v5_frozen_true_book_beats_later_market_and_missing_book_stays_c0_failure() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules, m) = setup_public_custom(temp.path(), Some("0.35"), None);
    advance(&mut driver, &m, T + 202);
    old_orders(
        &mut shared,
        &mut driver,
        &rules,
        &[(UP_TOKEN_ID, Decimal::new(4, 1), true)],
    )
    .await;
    advance(&mut driver, &m, T + 3802);
    let frozen = shared.strategy_input(&mut driver).unwrap().unwrap();
    advance(&mut driver, &m, T + 4002);
    let later = shared.strategy_input(&mut driver).unwrap().unwrap();
    let out = v5(rules.clone(), true).decide(&frozen, T + 3817).unwrap();
    let d = &diagnostic(&out).markets[0];
    assert_eq!(
        d.pair_entry.low_price_entry.unwrap(),
        InitialPairLowPriceEvaluationV5::VetoBelowBestBid
    );
    assert_eq!(
        d.pair_entry.public_entry.as_ref().unwrap().pm_quotes[0][0],
        Some(Decimal::new(45, 2))
    );
    let later_out = v5(rules, true).decide(&later, T + 4017).unwrap();
    assert_ne!(
        diagnostic(&later_out).markets[0]
            .pair_entry
            .public_entry
            .as_ref()
            .unwrap()
            .pm_quotes[0][0],
        d.pair_entry.public_entry.as_ref().unwrap().pm_quotes[0][0]
    );
    driver.stop_and_drain().unwrap();
    // 合法Core空簿；给定公平价路径能到C0，不能借仍有值的估值/BBA补真簿。
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, rules, m) =
        setup_public_custom(temp.path(), None, Some(UP_TOKEN_ID));
    advance(&mut driver, &m, T + 202);
    let input = shared.strategy_input(&mut driver).unwrap().unwrap();
    assert!(
        input.markets[0]
            .observation
            .assets
            .iter()
            .find(|a| a.asset.token_id.0 == UP_TOKEN_ID)
            .unwrap()
            .bids
            .value
            .as_ref()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        input.markets[0]
            .observation
            .assets
            .iter()
            .find(|a| a.asset.token_id.0 == UP_TOKEN_ID)
            .unwrap()
            .valuation_best_bid
            .value
            .as_ref(),
        Some(&Some(ExactDecimal("0.45".into())))
    );
    let mut c = config_v5(true);
    c.parameters.quote.initial_pair_price_reference = InitialPairPriceReferenceV1::FairValue;
    let mut strategy = ComplementaryPairV1::new_low_price(
        c,
        rules,
        "missing-book".into(),
        StrategyContinuityV2::new_low_price_account(),
    )
    .unwrap();
    let out = strategy.decide(&input, T + 217).unwrap();
    let d = &diagnostic(&out).markets[0];
    assert_eq!(pairs(&out), 0);
    assert!(d.pair_entry.low_price_entry.is_none());
    if let Some(c0) = &d.pair_entry.public_entry {
        assert!(!c0.c0_passed);
    }
    driver.stop_and_drain().unwrap();
}
