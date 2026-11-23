use super::*;
fn check(
    read: &LedgerEconomicReadV2<'_>,
    driver: &PublicConsumptionStateV1,
) -> UnmarkedAccountEconomicsReadV3 {
    let cached = UnmarkedAccountEconomicsReadV3::from_ledger(read);
    assert_eq!(
        cached,
        UnmarkedAccountEconomicsReadV3::from_ledger_uncached_for_measurement(read)
    );
    assert_eq!(
        AccountEconomicsReadV3::from_ledger_and_observations(
            read,
            driver.observations_at_current_prefix_iter()
        ),
        AccountEconomicsReadV3::from_ledger_and_observations_uncached_for_measurement(
            read,
            driver.observations_at_current_prefix_iter()
        )
    );
    cached
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit #392 bounded 20/70-market unmarked economics cold/hot and actual feedback measurement"]
async fn economic_market_cache_cold_hot_and_single_market_feedback() {
    let root =
        PathBuf::from(std::env::var_os("BACKTEST_COST_TEST_OUTPUT_ROOT").expect("独立成本输出"));
    std::fs::create_dir(&root).unwrap();
    for seen in [20, 70] {
        let cell = root.join(format!("markets-{seen}"));
        std::fs::create_dir(&cell).unwrap();
        let material = fixture_nav::recorded_observation_scaling(&cell.join("dpt"), seen);
        let mut lane = lane(&cell.join("lane"), &material, seen, statistics()).await;
        let mut bytes = Vec::new();
        lane.shared.ledger().write_checkpoint(&mut bytes).unwrap();
        std::fs::write(cell.join("cold-ledger.cbor"), &bytes).unwrap();
        let reference_restored = LedgerWriterV1::restore_checkpoint(bytes.as_slice()).unwrap();
        let reference_read = reference_restored.economic_read().unwrap();
        let reference_started = Instant::now();
        let reference_cold =
            UnmarkedAccountEconomicsReadV3::from_ledger_uncached_for_measurement(&reference_read);
        let reference_cold_ns = reference_started.elapsed().as_nanos();
        let restored = LedgerWriterV1::restore_checkpoint(bytes.as_slice()).unwrap();
        let read = restored.economic_read().unwrap();
        let before = economic_read_cache_work_v1();
        let started = Instant::now();
        let frozen = UnmarkedAccountEconomicsReadV3::from_ledger(&read);
        let cold_ns = started.elapsed().as_nanos();
        let after = economic_read_cache_work_v1();
        assert_eq!(
            after.market_detail_builds - before.market_detail_builds,
            seen as u64,
            "恢复后派生缓存是冷的"
        );
        assert_eq!(
            after.account_contribution_replacements - before.account_contribution_replacements,
            seen as u64
        );
        assert_eq!(frozen, reference_cold);
        assert_eq!(
            frozen,
            UnmarkedAccountEconomicsReadV3::from_ledger_uncached_for_measurement(&read)
        );
        let mut costs = vec![
            serde_json::json!({"phase":"cold_after_restore","markets":seen,"wall_ns":cold_ns,"reference_cold_ns":reference_cold_ns,"builds":after.market_detail_builds-before.market_detail_builds,"account_contributions":after.account_contribution_replacements-before.account_contribution_replacements}),
        ];
        for trial in 0..3 {
            for original in if trial % 2 == 0 {
                [true, false]
            } else {
                [false, true]
            } {
                let before = economic_read_cache_work_v1();
                let started = Instant::now();
                for _ in 0..128 {
                    let value = if original {
                        UnmarkedAccountEconomicsReadV3::from_ledger_uncached_for_measurement(&read)
                    } else {
                        UnmarkedAccountEconomicsReadV3::from_ledger(&read)
                    };
                    std::hint::black_box(value);
                }
                let elapsed = started.elapsed().as_nanos();
                let after = economic_read_cache_work_v1();
                if !original {
                    assert_eq!(after.market_detail_builds - before.market_detail_builds, 0);
                    assert_eq!(
                        after.market_detail_cache_hits - before.market_detail_cache_hits,
                        128 * seen as u64
                    );
                }
                let cost = serde_json::json!({"phase":"hot","trial":trial,"reference":original,"markets":seen,"calls":128,"wall_ns":elapsed,
                    "builds":after.market_detail_builds-before.market_detail_builds,"hits":after.market_detail_cache_hits-before.market_detail_cache_hits,"original_builds":after.reference_market_detail_builds-before.reference_market_detail_builds});
                eprintln!("economic_cache_cost={cost}");
                costs.push(cost);
            }
        }
        assert_eq!(
            frozen,
            UnmarkedAccountEconomicsReadV3::from_ledger(&read),
            "旧冻结值不随其他读取变化"
        );
        // 原owner沿真实新公开前缀保持同一经济状态，盯市值每次从当前可见观察读取。
        let before_owned = check(&lane.shared.ledger().economic_read().unwrap(), &lane.driver);
        for _ in 0..4 {
            lane.driver.next_group().unwrap().unwrap();
            lane.shared.apply_closed().unwrap();
            check(&lane.shared.ledger().economic_read().unwrap(), &lane.driver);
        }
        let before_trade = check(&lane.shared.ledger().economic_read().unwrap(), &lane.driver);
        let cash_before = lane.shared.ledger().economic_read().unwrap().cash().clone();
        let work_before = economic_read_cache_work_v1();
        lane.shared
            .execute(
                &mut lane.driver.decision().unwrap(),
                RequestContextV2 {
                    global_request_id: "single-current-market".into(),
                    caller_correlation_id: None,
                },
                Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                    condition_id: ConditionId(material.current_market.clone()),
                    order: BuyLegIntentV1 {
                        token_id: TokenId("21".into()),
                        order: BuyOrderIntentV1::GtcLimit {
                            limit_price_pusd_per_share: Decimal::new(6, 1),
                            size_shares: 5.into(),
                            post_only: false,
                        },
                    },
                })),
            )
            .await
            .unwrap()
            .unwrap();
        let mut success_visible = false;
        // 原缩放夹具没有完整市场终局，不能消费其最终EndOfInput。只推进已声明更新前缀。
        for _ in 4..material.update_groups.len() {
            lane.driver.next_group().unwrap().unwrap();
            lane.shared.apply_closed().unwrap();
            let current = check(&lane.shared.ledger().economic_read().unwrap(), &lane.driver);
            if current.markets.iter().any(|m| {
                m.condition_id.0 == material.current_market
                    && m.assets
                        .as_ref()
                        .and_then(|assets| assets.iter().position(|asset| asset.token_id.0 == "21"))
                        .is_some_and(|side| m.success_shares[side].0 == "5")
            }) {
                success_visible = true;
                break;
            }
        }
        assert!(success_visible, "有限原更新前缀必须实际收到新成交成功反馈");
        let after_trade = check(&lane.shared.ledger().economic_read().unwrap(), &lane.driver);
        let work_after = economic_read_cache_work_v1();
        // 使用真实账户前缀及其返回的资产身份，分别读取当前、陈旧和空买价。
        let read = lane.shared.ledger().economic_read().unwrap();
        let mut observations = lane
            .driver
            .observations_at_current_prefix_iter()
            .cloned()
            .collect::<Vec<_>>();
        let mut marks = Vec::new();
        for (quality, value) in [
            (
                MarketDataQuality::Fresh,
                Some(Some(ExactDecimal("0.25".into()))),
            ),
            (
                MarketDataQuality::Fresh,
                Some(Some(ExactDecimal("0.35".into()))),
            ),
            (
                MarketDataQuality::Stale,
                Some(Some(ExactDecimal("0.35".into()))),
            ),
            (MarketDataQuality::Fresh, None),
            (MarketDataQuality::Fresh, Some(None)),
        ] {
            for observation in &mut observations {
                for asset in &mut observation.assets {
                    let quote = Arc::make_mut(&mut asset.valuation_best_bid);
                    quote.quality = quality;
                    quote.value = value.clone();
                }
            }
            let actual = AccountEconomicsReadV3::from_ledger_and_observations(&read, &observations);
            let reference =
                AccountEconomicsReadV3::from_ledger_and_observations_uncached_for_measurement(
                    &read,
                    &observations,
                );
            assert_eq!(actual, reference);
            assert_eq!(actual.cash, after_trade.cash);
            marks.push(actual.conservative_nav_pusd);
        }
        assert_ne!(marks[0], marks[1], "当前买价变化必须改变有孤腿的盯市值");
        assert!(matches!(marks[2], ExecutionReadValueV1::Unavailable));
        assert!(matches!(marks[3], ExecutionReadValueV1::Unavailable));
        assert!(
            matches!(marks[4], ExecutionReadValueV1::Known(_)),
            "已知空bid沿原合同按零计价"
        );
        let changed = before_trade
            .markets
            .iter()
            .zip(&after_trade.markets)
            .filter(|(a, b)| a != b)
            .map(|(_, m)| m.condition_id.0.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            changed,
            vec![material.current_market.clone()],
            "一个真实市场成交只改变对应完整经济明细"
        );
        assert_ne!(
            lane.shared.ledger().economic_read().unwrap().cash(),
            &cash_before,
            "现金/预留不能被派生缓存冻结"
        );
        assert!(
            work_after.market_detail_builds - work_before.market_detail_builds < seen as u64,
            "单市场反馈不使全部历史市场重算"
        );
        assert_eq!(before_owned, frozen, "恢复前后同一前缀、旧快照仍相同");
        let mut checkpoint = Vec::new();
        lane.shared
            .ledger()
            .write_checkpoint(&mut checkpoint)
            .unwrap();
        let recovered = LedgerWriterV1::restore_checkpoint(checkpoint.as_slice()).unwrap();
        assert_eq!(
            check(&recovered.economic_read().unwrap(), &lane.driver),
            after_trade
        );
        let result = serde_json::json!({"markets":seen,"cold_and_hot":costs,"single_market_changed":changed,"single_market_detail_builds":work_after.market_detail_builds-work_before.market_detail_builds,
            "single_market_invalidations":work_after.market_detail_invalidations-work_before.market_detail_invalidations,"single_market_account_replacements":work_after.account_contribution_replacements-work_before.account_contribution_replacements,
            "cash_before":cash_before,"cash_after":lane.shared.ledger().economic_read().unwrap().cash(),"old_frozen_unchanged":before_owned==frozen});
        eprintln!("economic_cache_feedback={result}");
        std::fs::write(
            cell.join("results.json"),
            serde_json::to_vec_pretty(&result).unwrap(),
        )
        .unwrap();
        lane.driver.stop_and_drain().unwrap();
    }
}
