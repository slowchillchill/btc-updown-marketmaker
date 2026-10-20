#[path = "common/day.rs"]
mod fixture_day;
#[path = "common/default.rs"]
mod fixture_default;
#[path = "common/initialization.rs"]
mod fixture_initialization;
#[path = "common/fixture_material.rs"]
mod fixture_material;
mod common {
    pub(crate) use super::fixture_day::*;
    pub(crate) use super::fixture_default::*;
    pub(crate) use super::fixture_initialization::*;
    pub(crate) use super::fixture_material::*;
}

use backtest::*;
use common::*;
use contracts::*;
use ledger::*;
use rust_decimal::Decimal;
use shared_run::*;
use sim_system::{execution::*, platform_rules::*};
use std::sync::Arc;
#[derive(Default)]
struct Probe {
    no_trade_only: bool,
    exit_immediately: bool,
    source_health: bool,
    trade_details: Option<bool>,
    quantity_demand: Option<bool>,
    expected_quantity_projection: Option<bool>,
    statistics_expected: bool,
    saw_depth: Option<Arc<std::sync::atomic::AtomicBool>>,
    rules: Option<Arc<SealedProcessRuleSnapshotV3>>,
    cancel_first: bool,
    canceled: bool,
    saw_cancel_in_flight_after_one_leg_fill: bool,
    saw_later_down_quote: bool,
    frozen_down_price: Option<Decimal>,
    bought_first: bool,
    bought_second: bool,
    exit: bool,
    fail: bool,
    bad_batch: bool,
    observed: Vec<(i64, usize, Option<String>, usize)>,
    last_economics: Option<AccountEconomicsReadV3>,
    first_economics: Option<AccountEconomicsReadV3>,
    dispatched: usize,
    failed_callback_sent: Option<usize>,
    first_next_evaluation: Option<i64>,
    calculation_budget: Option<usize>,
}
// 小夹具读取所选真实布局，保留帧/接纳组边界；不把省略材料伪装为旧完整日志。
fn sim_log_values(path: &std::path::Path) -> Vec<serde_json::Value> {
    let bytes = std::fs::read(path).unwrap();
    if !bytes.starts_with(b"SIMDAY01") {
        return std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    }
    let mut offset = 8;
    let mut frames = Vec::new();
    while offset < bytes.len() {
        let length = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;
        frames.push(ciborium::de::from_reader(&bytes[offset..offset + length]).unwrap());
        offset += length;
    }
    frames
}

impl StrategyV1 for Probe {
    type Error = &'static str;
    type Diagnostics = ();
    fn uses_source_health(&self) -> bool {
        self.source_health
    }
    fn requires_trade_economics_details(&self) -> bool {
        self.trade_details.unwrap_or(true)
    }
    fn observation_requirements(&self) -> contracts::PublicObservationRequirementsV1 {
        contracts::PublicObservationRequirementsV1 {
            binance_depth_quantities: self.quantity_demand.unwrap_or(true),
        }
    }
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1, Self::Error> {
        if let Some(full) = self.expected_quantity_projection {
            for market in &input.markets {
                if let Some(depth) = &market.observation.binance_depth.value {
                    assert_eq!(depth.full().is_some(), full, "共享日采用全部候选需求并集");
                    if self.statistics_expected {
                        let stats = market
                            .statistics
                            .as_ref()
                            .expect("共同统计已在真实day入口启用");
                        assert_eq!(
                            stats.depth.best_bid_usdt_per_btc.value,
                            Some(Decimal::from(64100))
                        );
                        assert_eq!(
                            stats.depth.best_ask_usdt_per_btc.value,
                            Some(Decimal::from(64200))
                        );
                        assert_eq!(
                            stats.depth.midpoint_usdt_per_btc.value,
                            Some(Decimal::from(64150))
                        );
                        assert_eq!(
                            stats.depth.spread_usdt_per_btc.value,
                            Some(Decimal::from(100))
                        );
                        if at < BASE + 2601 {
                            // 快照两档bid .7+.5=1.2，ask .4，压力(1.2-.4)/(1.2+.4)=.5。
                            assert_eq!(
                                stats.depth.bid_quantity_btc.value,
                                Some(Decimal::new(12, 1))
                            );
                            assert_eq!(
                                stats.depth.ask_quantity_btc.value,
                                Some(Decimal::new(4, 1))
                            );
                            assert_eq!(stats.depth.imbalance.value, Some(Decimal::new(5, 1)));
                        }
                    }
                    self.saw_depth
                        .as_ref()
                        .unwrap()
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
        // 仅测试的有界失败探针：旧实现重复即时计算时直接失败，避免挂住回归。
        if self
            .calculation_budget
            .is_some_and(|limit| self.observed.len() >= limit)
        {
            return Err("test-repeated-calculation-without-progress");
        }
        let first = input
            .markets
            .iter()
            .find(|m| m.observation.market.condition_id.0 == FIRST);
        let ask = first
            .and_then(|m| {
                m.observation
                    .assets
                    .iter()
                    .find(|a| a.asset.token_id.0 == "20")
            })
            .and_then(|a| a.asks.value.as_ref())
            .and_then(|levels| levels.first())
            .map(|l| l.price_pusd_per_share.0.clone());
        self.observed.push((
            at,
            input.markets.len(),
            ask.clone(),
            input.trade_economics.len(),
        ));
        self.first_economics
            .get_or_insert_with(|| input.action_before.economics().clone());
        self.last_economics = Some(input.action_before.economics().clone());
        if self.exit_immediately {
            return Ok(StrategyOutputV1::Exit { diagnostics: () });
        }
        if self.no_trade_only {
            return Ok(StrategyOutputV1::Continue {
                intents: vec![],
                next_evaluation_at_utc_ms: None,
                diagnostics: (),
            });
        }
        if ask.is_some() {
            if self.fail {
                return Err("original-strategy-failure");
            }
            if self.exit {
                return Ok(StrategyOutputV1::Exit { diagnostics: () });
            }
            if self.bad_batch {
                let cancel = || ReservedIntentV1 {
                    context: RequestContextV2 {
                        global_request_id: "duplicate".into(),
                        caller_correlation_id: None,
                    },
                    intent: Arc::new(ModelIntentV1::CancelMarketOrders {
                        condition_id: ConditionId(FIRST.into()),
                    }),
                    reserves: vec![],
                };
                return Ok(StrategyOutputV1::Continue {
                    intents: vec![cancel(), cancel()],
                    next_evaluation_at_utc_ms: None,
                    diagnostics: (),
                });
            }
        }
        self.saw_later_down_quote |= first.is_some_and(|m| {
            m.observation
                .assets
                .iter()
                .find(|a| a.asset.token_id.0 == "3")
                .and_then(|a| a.asks.value.as_ref())
                .and_then(|levels| levels.first())
                .is_some_and(|l| l.price_pusd_per_share.0 == "0.65")
        });
        self.saw_cancel_in_flight_after_one_leg_fill |= matches!(input.action_before.cancellation_requests(None).pending_count, ExecutionReadValueV1::Known(n) if n > 0)
            && input
                .trade_economics
                .iter()
                .any(|trade| trade.economic_applied)
            && input.action_before.commitments().next().is_some();
        let mut next_evaluation_at_utc_ms = None;
        let mut intents = vec![];
        if self.cancel_first
            && self.bought_first
            && !self.canceled
            && input
                .trade_economics
                .iter()
                .any(|trade| trade.economic_applied)
        {
            self.canceled = true;
            intents.push(ReservedIntentV1 {
                context: RequestContextV2 {
                    global_request_id: "cancel-first-after-one-leg".into(),
                    caller_correlation_id: None,
                },
                intent: Arc::new(ModelIntentV1::CancelMarketOrders {
                    condition_id: ConditionId(FIRST.into()),
                }),
                reserves: vec![],
            });
            next_evaluation_at_utc_ms = Some(at + 401);
        }

        if ask.is_some() && !self.bought_first {
            // 同毫秒双完整公开组必须全部已应用。
            assert!(
                first
                    .unwrap()
                    .observation
                    .assets
                    .iter()
                    .all(|a| a.asks.value.is_some())
            );
            self.bought_first = true;
            next_evaluation_at_utc_ms = self.first_next_evaluation;
            intents.push(buy(
                self.rules.as_ref().unwrap(),
                FIRST,
                "20",
                "first-buy",
                "0.6",
                false,
            ));
            let down_ask = first
                .unwrap()
                .observation
                .assets
                .iter()
                .find(|asset| asset.asset.token_id.0 == "3")
                .unwrap()
                .asks
                .value
                .as_ref()
                .unwrap()[0]
                .price_pusd_per_share
                .0
                .clone();
            let down_price = Decimal::from_str_exact(&down_ask).unwrap() - Decimal::new(15, 2);
            intents.push(buy(
                self.rules.as_ref().unwrap(),
                FIRST,
                "3",
                "first-down",
                &down_price.to_string(),
                true,
            ));
            if self.cancel_first {
                next_evaluation_at_utc_ms = Some(BASE + 1900);
            }
        }
        if !self.bought_second
            && input.markets.iter().any(|m| {
                m.observation.market.condition_id.0 == SECOND
                    && m.observation.assets.iter().all(|a| a.asks.value.is_some())
            })
        {
            self.bought_second = true;
            assert!(
                input
                    .trade_economics
                    .iter()
                    .any(|trade| trade.economic_applied),
                "跨市场后仍有第一个市场的真实成交"
            );
            intents.push(buy(
                self.rules.as_ref().unwrap(),
                SECOND,
                "21",
                "second-buy",
                "0.45",
                true,
            ));
        }
        Ok(StrategyOutputV1::Continue {
            intents,
            next_evaluation_at_utc_ms,
            diagnostics: (),
        })
    }
    fn dispatched(
        &mut self,
        calculation: &StrategyCalculationV1,
        result: &Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1>,
    ) {
        if let StrategyOutputV1::Continue { intents, .. } = &calculation.output {
            for intent in intents {
                if intent.context.global_request_id == "first-down"
                    && let ModelIntentV1::SubmitSingleBuy { order, .. } = intent.intent.as_ref()
                    && let BuyOrderIntentV1::GtcLimit {
                        limit_price_pusd_per_share,
                        ..
                    } = order.order
                {
                    self.frozen_down_price = Some(limit_price_pusd_per_share);
                }
            }
        }
        self.dispatched += 1;
        if let Err(error) = result {
            self.failed_callback_sent = Some(error.sent.len())
        }
    }
}
fn buy(
    rules: &SealedProcessRuleSnapshotV3,
    condition: &str,
    token: &str,
    id: &str,
    price: &str,
    post_only: bool,
) -> ReservedIntentV1 {
    let price_decimal = Decimal::from_str_exact(price).unwrap();
    let index = (price_decimal * Decimal::from(100))
        .normalize()
        .to_string()
        .parse()
        .unwrap();
    let cost = rules
        .limit_buy_cost_v1(
            &num_bigint::BigUint::from(1000_u32),
            index,
            &ExactDecimal("0.01".into()),
            post_only,
        )
        .unwrap();
    let principal = Decimal::from_str_exact(&cost.principal_micro.to_string()).unwrap()
        / Decimal::from(1_000_000);
    let fee =
        Decimal::from_str_exact(&cost.fee_1e5_units.to_string()).unwrap() / Decimal::from(100_000);

    ReservedIntentV1 {
        context: RequestContextV2 {
            global_request_id: id.into(),
            caller_correlation_id: None,
        },
        intent: Arc::new(ModelIntentV1::SubmitSingleBuy {
            condition_id: ConditionId(condition.into()),
            order: BuyLegIntentV1 {
                token_id: TokenId(token.into()),
                order: BuyOrderIntentV1::GtcLimit {
                    limit_price_pusd_per_share: Decimal::from_str_exact(price).unwrap(),
                    size_shares: Decimal::from(10),
                    post_only,
                },
            },
        }),
        reserves: vec![IntentReserveV1 {
            batch_item_ordinal: 0,
            reserve: ReserveIntentV1 {
                request_identity: LedgerRequestIdentityV1(format!("{id}-reserve")),
                condition_id: ConditionId(condition.into()),
                requested_principal_pusd: ExactDecimal(principal.to_string()),
                immediate_fee_reserve_pusd: ExactDecimal(fee.to_string()),
            },
        }],
    }
}
#[tokio::test(flavor = "current_thread")]
async fn physical_entry_freezes_delayed_decisions_uses_hidden_fills_and_keeps_one_account() {
    let temp = tempfile::tempdir().unwrap();
    let (config, rules) = setup(temp.path());
    // 兼容输入的旧标签不选择算法；新材料和有效配置绑定同一实际排序版本。
    assert_eq!(
        config.preparation.header.ordering_version,
        "historical-source-head-v1"
    );
    let mut probe = Probe {
        rules: Some(rules.clone()),
        cancel_first: true,
        ..Default::default()
    };
    let report = run_historical_v1(&config, rules, &mut probe).await.unwrap();
    let stream: serde_json::Value = serde_json::from_slice(
        &std::fs::read(config.preparation.work_root.join("public/stream.json")).unwrap(),
    )
    .unwrap();
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&config.metadata_path).unwrap()).unwrap();
    let actual_version =
        data_pipeline_training::public_replay::HISTORICAL_SOURCE_HEAD_ORDERING_VERSION;
    assert_eq!(stream["ordering_version"], actual_version);
    assert_eq!(
        metadata["config"]["preparation"]["header"]["ordering_version"],
        actual_version
    );
    assert_eq!(report.end_reason, HistoricalEndReasonV1::RangeEnd);
    assert!(report.recording_drained);
    assert!(probe.bought_first && probe.bought_second);
    assert!(probe.saw_later_down_quote);
    assert_eq!(probe.frozen_down_price, Some(Decimal::new(45, 2)));
    assert!(probe.canceled && probe.saw_cancel_in_flight_after_one_leg_fill);
    assert!(report.unclosed_commitments > 0);
    assert!(
        probe
            .observed
            .iter()
            .any(|(at, _, ask, trades)| *at < BASE + 2301
                && ask.as_deref() == Some("0.6")
                && *trades > 0),
        "隐藏成交反馈可见但迟到公开盘口不得偷看"
    );
    assert!(
        report
            .last_dispatched_calculation_timing
            .as_ref()
            .unwrap()
            .scheduled_send_at_utc_ms
            > report
                .last_dispatched_calculation_timing
                .as_ref()
                .unwrap()
                .observation_at_utc_ms
    );
    let wire = std::fs::read_to_string(&config.sim_log).unwrap();
    assert!(wire.contains("FINITE_HISTORICAL"));
    assert!(wire.contains("HISTORICAL_BACKTEST"));
    assert!(!wire.contains("RunSeal"));
    let ledger = report.ledger_consumed_memory.unwrap();
    assert_eq!(
        ledger.account_instance_key.caller_role,
        SimCallerRoleV1::HistoricalBacktest
    );
}
#[tokio::test(flavor = "current_thread")]
async fn range_tail_does_not_complete_or_advance_a_pending_calculation() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, rules) = setup(temp.path());
    config.timing.calculation_duration_ms = DelayScheduleV1::Fixed(100000);
    let report = run_historical_v1(&config, rules, &mut Probe::default())
        .await
        .unwrap();
    assert!(report.pending_calculation);
    assert_eq!(report.calculation_count, 1);
    assert!(report.last_dispatched_calculation_timing.is_none());
    assert_eq!(report.end_reason, HistoricalEndReasonV1::RangeEnd);
}
#[tokio::test(flavor = "current_thread")]
async fn range_tail_reports_final_public_economics_after_the_last_strategy_callback() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, rules) = setup(temp.path());
    config.preparation.scope.received_end_utc_ms = BASE + 1900;
    let mut probe = Probe {
        rules: Some(rules.clone()),
        ..Default::default()
    };
    let report = run_historical_v1(&config, rules, &mut probe).await.unwrap();
    let (last_at, _, _, last_trades) = probe.observed.last().unwrap();
    assert_eq!(*last_at, BASE + 1601);
    assert_eq!(*last_trades, 0);
    assert!(probe.last_economics.unwrap().markets.iter().all(|market| {
        market.executed.iter().all(|role| {
            role.shares == ExactDecimal("0".into())
                && role.actual_fee_pusd == ExactDecimal("0".into())
        })
    }));

    assert_eq!(report.end_reason, HistoricalEndReasonV1::RangeEnd);
    assert!(report.recording_drained);
    let cutoff = report.observation_cutoff.as_ref().unwrap();
    assert_eq!(cutoff.decision_time().logical_time_utc_ms(), BASE + 1899);
    assert_eq!(report.last_observed_at_utc_ms, Some(BASE + 1899));
    let economics = report.account_economics_memory.as_ref().unwrap();
    let market = economics
        .markets
        .iter()
        .find(|market| market.condition_id.0 == FIRST)
        .unwrap();
    let side = market
        .assets
        .as_ref()
        .unwrap()
        .iter()
        .position(|asset| asset.token_id.0 == "20")
        .unwrap();
    assert_eq!(market.orphan_shares[side], ExactDecimal("10".into()));
    assert_eq!(market.executed[1].shares, ExactDecimal("10".into()));
    assert_eq!(
        market.executed[1].actual_fee_pusd,
        ExactDecimal("0.168".into())
    );
    let ledger = report.ledger_consumed_memory.as_ref().unwrap();
    let projection = ledger
        .market_projections
        .iter()
        .find(|market| market.condition_id.0 == FIRST)
        .unwrap();
    assert_eq!(projection.fees_pusd, market.executed[1].actual_fee_pusd);
    assert_eq!(
        projection
            .outcome_balances
            .iter()
            .find(|balance| balance.token_id.0 == "20")
            .unwrap()
            .balance_shares,
        market.orphan_shares[side]
    );
    assert_eq!(
        projection.taker_rebate_receivable_pusd,
        ExactDecimal("0.01344".into())
    );
    assert_eq!(
        market.rebates[1].accrued_unmatured_pusd,
        contracts::economics::EconomicRational::from_decimal(Decimal::new(1344, 5)).public()
    );
    assert_eq!(
        report.conservative_nav_pusd.as_ref(),
        Some(&economics.conservative_nav_pusd)
    );
    assert_eq!(
        report.worst_case_maker_nav_pusd.as_ref(),
        Some(&economics.worst_case_maker_nav_pusd)
    );
    assert_range_tail_capital_observation_times(&config);
    assert_range_tail_day_economics(&report, 1900);
    // 沿既有正常结束产物保留同一快照和原截止，不把精确金额变成JSON浮点。
    let saved: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&config.result_path).unwrap()).unwrap();
    assert_eq!(saved, serde_json::to_value(&report).unwrap());
    let saved_market = saved["account_economics_memory"]["markets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|market| market["condition_id"] == FIRST)
        .unwrap();
    assert_eq!(saved_market["executed"][1]["actual_fee_pusd"], "0.168");
    assert_eq!(saved_market["assets"][side]["token_id"], "20");
    assert_eq!(saved_market["orphan_shares"][side], "10");
    assert_eq!(
        saved["ledger_consumed_memory"]["ledger_entry_seq"],
        ledger.ledger_entry_seq
    );
}

fn assert_range_tail_capital_observation_times(config: &HistoricalRunConfigV1) {
    // 原SIM执行日志独立确定积分分界，不能用被测统计值生成期望。
    let wire = std::fs::read_to_string(&config.sim_log).unwrap();
    let observations = wire
        .lines()
        .flat_map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            value["body"]["inputs"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|input| input["input"] == "PUBLIC_OBSERVE_TIME")
                .map(|input| input["body"]["logical_time_utc_ms"].as_i64().unwrap())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    for offset in [1632, 1642, 1899] {
        assert!(observations.contains(&(BASE + offset)));
    }
}

fn assert_range_tail_day_economics(report: &HistoricalRunReportV1, duration_ms: u64) {
    use contracts::economics::EconomicRational as R;
    let amount = |text: &str| R::from_decimal(Decimal::from_str_exact(text).unwrap());
    let statistics = &report.day_economics;
    assert_eq!(statistics.measured_duration_ms, duration_ms);
    assert_eq!(statistics.unknown_capital_duration_ms, 0);
    assert_eq!(statistics.unknown_nav_samples, 0);
    // 新固定序列：MINED经济事实先到，CONFIRMED份额后到。
    // 1642读取已扣本金6与费用0.168，但成功份额尚未入池；保守NAV临时少6.168。
    // 1899尾观察成功份额才按公开买价0.4估值4，期末损失2.168。未成熟返佣不计NAV。
    assert_eq!(
        statistics.maximum_nav_drawdown_pusd,
        ExecutionReadValueV1::Known(amount("6.168").public())
    );
    assert_eq!(
        statistics.maximum_nav_drawdown_fraction,
        ExecutionReadValueV1::Known(amount("6.168").divided_by(&amount("2000")).public())
    );
    // 固定规则rate=.07、exponent=1：非post-only买单按最坏价.5预留费，
    // 10*.07*.5*(1-.5)=.175；实际.6成交费为10*.07*.6*(1-.6)=.168。
    let principal = amount("10").mul(&amount("0.6"));
    let down = amount("10").mul(&amount("0.45"));
    let reserved_fee = amount("10")
        .mul(&amount("0.07"))
        .mul(&amount("0.5"))
        .mul(&amount("0.5"));
    let submitted = principal.add(&reserved_fee).add(&down);
    // seed=e×64：Up/Down出站21/10ms；Up MINED平台时刻1632，入站9ms。
    // 原逻辑在下一义务1642读取经济事实；此前的1632直接响应不改变10.675占用。
    // 1642以后本金6+Down预留4.5=10.5；已付费0.168不再属于占用资本。
    // NOE真实来源和SIM观察时刻独立支持这两个积分段，不从统计值生成期望。
    let tail = down.add(&principal);
    let integral = submitted.mul(&amount("41")).add(&tail.mul(&amount("258")));
    assert_eq!(
        statistics.maximum_committed_capital_pusd,
        ExecutionReadValueV1::Known(submitted.public())
    );
    assert_eq!(
        statistics.committed_capital_pusd_milliseconds,
        ExecutionReadValueV1::Known(integral.public())
    );
    assert_eq!(
        statistics.average_committed_capital_pusd,
        ExecutionReadValueV1::Known(
            integral
                .divided_by(&R::from_decimal(duration_ms.into()))
                .public()
        )
    );
}

#[tokio::test(flavor = "current_thread")]
async fn initialization_before_operating_range_does_not_extend_day_economics() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, rules) = setup(temp.path());
    config.preparation.scope.received_end_utc_ms = BASE + 1900;
    initialize_at(&mut config, BASE + 1000, &[FIRST]);
    let mut probe = Probe {
        rules: Some(rules.clone()),
        ..Default::default()
    };
    let report = run_historical_v1(&config, rules, &mut probe).await.unwrap();
    // 真实原元数据接收于BASE，初始化交付早于经营开始；策略仍从原经营输入开始。
    assert_eq!(probe.observed.first().unwrap().0, BASE + 1201);
    assert_eq!(
        report
            .delivery_summary
            .as_ref()
            .unwrap()
            .before_operating_start
            .groups,
        1
    );
    assert_range_tail_capital_observation_times(&config);
    assert_range_tail_day_economics(&report, 900);
}
#[tokio::test(flavor = "current_thread")]
async fn exit_returns_without_drain_result_write_or_future_public_observation() {
    let temp = tempfile::tempdir().unwrap();
    let (config, rules) = setup(temp.path());
    let mut probe = Probe {
        exit: true,
        ..Default::default()
    };
    let report = run_historical_v1(&config, rules, &mut probe).await.unwrap();
    assert_eq!(report.end_reason, HistoricalEndReasonV1::StrategyExit);
    assert!(!report.recording_drained);
    assert!(!config.result_path.exists());
    assert!(report.account_economics_memory.is_some());
    assert!(
        report
            .observation_cutoff
            .as_ref()
            .unwrap()
            .decision_time()
            .logical_time_utc_ms()
            < BASE + 1700
    );
    assert!(probe.observed.iter().all(|(at, _, _, _)| *at < BASE + 1700));
    assert_eq!(probe.dispatched, 1);
}
#[tokio::test(flavor = "current_thread")]
async fn strategy_and_batch_failures_preserve_original_errors_and_stop_without_retry() {
    for (bad_batch, source_health) in [(false, false), (true, false), (true, true)] {
        let temp = tempfile::tempdir().unwrap();
        let (config, rules) = setup(temp.path());
        let mut probe = Probe {
            fail: !bad_batch,
            bad_batch,
            source_health,
            ..Default::default()
        };
        let failure = run_historical_v1(&config, rules, &mut probe)
            .await
            .unwrap_err();
        if bad_batch {
            let report = failure.report.as_ref().unwrap();
            let batch = report.failed_batch.as_ref().unwrap();
            assert_eq!(batch.intents.len(), 2);
            assert_eq!(batch.intents[0].context.global_request_id, "duplicate");
            assert_eq!(batch.dispatch_effects.as_ref().unwrap().sent.len(), 1);
            assert!(batch.calculation_cash.is_some());
            assert!(batch.admission_cash.is_some());
            assert!(
                report
                    .pending_calculation_timing
                    .as_ref()
                    .unwrap()
                    .actual_sent_at_utc_ms
                    .is_some()
            );
            match failure.cause {
                HistoricalFailureCauseV1::Dispatch(error) => {
                    assert_eq!(error.sent.len(), 1);
                    assert!(matches!(
                        error.error,
                        SharedActionErrorV1::Sim(SimRunActorErrorV2::InvalidBusinessRequest(
                            "同批意图必须使用独立 global_request_id"
                        ))
                    ));
                }
                other => panic!("{other:?}"),
            };
            assert_eq!(probe.failed_callback_sent, Some(1));
        } else {
            assert!(matches!(
                failure.cause,
                HistoricalFailureCauseV1::Strategy("original-strategy-failure")
            ));
        }
        let report = failure.report.unwrap();
        assert_eq!(report.end_reason, HistoricalEndReasonV1::Failure);
        assert!(!report.recording_drained);
        assert!(report.account_economics_memory.is_some());
        assert!(report.observation_cutoff.is_some());
        assert!(!config.result_path.exists());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_zero_compute_and_dispatch_delays_consume_original_obligations() {
    for public_delay in [0, 100] {
        let temp = tempfile::tempdir().unwrap();
        let (mut config, rules) = setup(temp.path());
        config.timing.public_delivery_delay_ms = DelayScheduleV1::Fixed(public_delay);
        config.timing.calculation_duration_ms = DelayScheduleV1::Fixed(0);
        config.timing.dispatch_delay_ms = DelayScheduleV1::Fixed(0);
        let mut probe = Probe {
            rules: Some(rules.clone()),
            calculation_budget: Some(40),
            cancel_first: true,
            ..Default::default()
        };
        let report = run_historical_v1(&config, rules, &mut probe).await.unwrap();
        assert_eq!(report.end_reason, HistoricalEndReasonV1::RangeEnd);
        assert!(probe.bought_first && probe.bought_second);
        assert!(probe.canceled, "原反馈驱动后继续提交新请求及消费新义务");
        assert!(probe.observed.iter().any(|(_, _, _, trades)| *trades > 0));
        let timing = report.last_dispatched_calculation_timing.unwrap();
        assert_eq!(
            timing.calculation_started_at_utc_ms,
            timing.calculation_completed_at_utc_ms
        );
        assert_eq!(
            timing.calculation_completed_at_utc_ms,
            timing.scheduled_send_at_utc_ms
        );
        assert_eq!(timing.actual_sent_at_utc_ms, None);
        assert!(
            probe
                .observed
                .iter()
                .any(|(at, markets, _, _)| *at >= BASE + 10100 && *markets == 2)
        );
    }
}

fn material_bytes(
    root: &std::path::Path,
) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn visit(
        root: &std::path::Path,
        current: &std::path::Path,
        output: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
    ) {
        for item in std::fs::read_dir(current).unwrap() {
            let path = item.unwrap().path();
            if path.is_dir() {
                visit(root, &path, output);
            } else {
                output.insert(
                    path.strip_prefix(root).unwrap().into(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut output = Default::default();
    visit(root, root, &mut output);
    output
}
fn reuse_outputs(
    c: &HistoricalRunConfigV1,
    root: &std::path::Path,
) -> HistoricalPreparedRunConfigV1 {
    std::fs::create_dir(root).unwrap();
    let mut config = HistoricalPreparedRunConfigV1::from(c);
    config.sim_log = root.join("sim.jsonl");
    config.noe_root = root.join("noe");
    config.ledger_root = root.join("ledger");
    config.metadata_path = root.join("run.json");
    config.result_path = root.join("result.json");
    config
}
// 只归一明列的运行输出根；不删除业务字段或改来源/组/账户身份。
fn normalize_output_paths(value: &mut serde_json::Value, root: &std::path::Path) {
    match value {
        serde_json::Value::String(s) => {
            if let Some(rest) = s.strip_prefix(root.to_str().unwrap()) {
                *s = format!("RUN_OUTPUT{rest}");
            }
        }
        serde_json::Value::Array(a) => {
            for v in a {
                normalize_output_paths(v, root);
            }
        }
        serde_json::Value::Object(o) => {
            for v in o.values_mut() {
                normalize_output_paths(v, root);
            }
        }
        _ => {}
    }
}
fn add_binance_depth_to_reuse_material(config: &HistoricalRunConfigV1) {
    use data_collect::{source_hourly_layout::*, writer::TypedParquetSegmentWriter};
    let manifest_path = &config.preparation.scope.manifest_paths[0];
    let day = manifest_path.parent().unwrap();
    let mut files = Vec::new();
    let mut snapshot_writer = TypedParquetSegmentWriter::create_for_current_source(
        day,
        "depth-snapshot",
        "binance_depth_snapshot",
    )
    .unwrap();
    snapshot_writer
        .write_binance_depth_snapshot_rows(&[BinanceDepthSnapshotHourlyRow {
            timestamp_received: BASE + 1000,
            snapshot_fetch_start_utc: BASE + 990,
            last_update_id: 100,
            bids: r#"[["64100","0.7"],["64000","0.5"]]"#.into(),
            asks: r#"[["64200","0.4"]]"#.into(),
            symbol: "BTCUSDT".into(),
            source_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
                .into(),
            snapshot_status: "complete".into(),
            source_sequence_epoch_id: Some("depth-reuse".into()),
            connection_generation: Some(1),
            depth_ingress_sequence: Some(1),
            collector_admission_sequence: Some(1),
            collector_admission_time_utc_ms: Some(BASE + 1001),
            collector_host_id: "synthetic-collector".into(),
            collector_region: "test".into(),
            schema_version: BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION.into(),
        }])
        .unwrap();
    snapshot_writer.append_key_value_metadata("data_collect_daily_file_fact_v1",serde_json::json!({"source_kind":"binance_depth_snapshot","schema_version":BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,"utc_hour":"2026-08-24T00","part":0,"rows":1}).to_string()).unwrap();
    files.push((
        snapshot_writer.finalize().unwrap(),
        "binance_depth_snapshot",
        BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
    ));
    let mut update_writer = TypedParquetSegmentWriter::create_for_current_source(
        day,
        "depth-updates",
        "binance_depth_update",
    )
    .unwrap();
    let updates = [2_u64, 3].map(|seq| BinanceDepthUpdateHourlyRow {
        timestamp_received: BASE + 2300 + seq as i64 * 100,
        event_type: "depthUpdate".into(),
        event_time: BASE + 1900 - seq as i64 * 100,
        symbol: "BTCUSDT".into(),
        first_update_id: 99 + seq,
        final_update_id: 99 + seq,
        bids: format!(r#"[["64100","0.{seq}"]]"#),
        asks: "[]".into(),
        stream: "btcusdt@depth".into(),
        snapshot_last_update_id: Some(100),
        local_u_before: if seq == 2 { None } else { Some(101) },
        stitch_verdict: BINANCE_DEPTH_VERDICT_APPLIED.into(),
        stitch_evidence: data_collect::sources::binance::assess_depth_sequence(
            Some(100),
            if seq == 2 { None } else { Some(101) },
            99 + seq,
            99 + seq,
        )
        .unwrap()
        .stitch_evidence,
        stitch_gap_reason: None,
        source_sequence_epoch_id: Some("depth-reuse".into()),
        connection_generation: Some(1),
        depth_ingress_sequence: Some(seq),
        collector_admission_sequence: Some(seq),
        collector_admission_time_utc_ms: Some(BASE + 2301 + seq as i64 * 100),
        collector_host_id: "synthetic-collector".into(),
        collector_region: "test".into(),
        source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@depth".into(),
        schema_version: BINANCE_DEPTH_UPDATE_SCHEMA_VERSION.into(),
    });
    update_writer
        .write_binance_depth_update_rows(&updates)
        .unwrap();
    update_writer.append_key_value_metadata("data_collect_daily_file_fact_v1",serde_json::json!({"source_kind":"binance_depth_update","schema_version":BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,"utc_hour":"2026-08-24T00","part":0,"rows":2}).to_string()).unwrap();
    files.push((
        update_writer.finalize().unwrap(),
        "binance_depth_update",
        BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
    ));
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(manifest_path).unwrap()).unwrap();
    for (file, source, version) in files {
        manifest["files"].as_array_mut().unwrap().push(serde_json::json!({"path":file.final_path.file_name().unwrap().to_str().unwrap(),"source_kind":source,"schema_version":version,"utc_hour":"2026-08-24T00","part":0,"rows":file.row_count,"bytes":std::fs::metadata(&file.final_path).unwrap().len(),"sha256":file.sha256}));
    }
    std::fs::write(manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
}
// 将同一小夹具的物理行分为两片；不修改已发布原件或业务内容。
fn split_material_fixture(
    source: &std::path::Path,
    destination: &std::path::Path,
) -> std::path::PathBuf {
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(source).unwrap()).unwrap();
    let schema = builder.schema().clone();
    let rows = builder.metadata().file_metadata().num_rows() as usize;
    assert!(rows >= 2);
    let batches: Vec<_> = builder
        .with_batch_size(rows)
        .build()
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let paths = [
        destination.to_path_buf(),
        destination.with_file_name(format!(
            "{}-part-00001.parquet",
            destination.file_stem().unwrap().to_str().unwrap()
        )),
    ];
    let mut writers: Vec<_> = paths
        .iter()
        .map(|path| {
            ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema.clone(), None)
                .unwrap()
        })
        .collect();
    let mut position = 0;
    for batch in batches {
        let first = (rows / 2).saturating_sub(position).min(batch.num_rows());
        if first > 0 {
            writers[0].write(&batch.slice(0, first)).unwrap();
        }
        if first < batch.num_rows() {
            writers[1]
                .write(&batch.slice(first, batch.num_rows() - first))
                .unwrap();
        }
        position += batch.num_rows();
    }
    for writer in writers {
        writer.close().unwrap();
    }
    let description = destination.with_extension("parts.json");
    let relative = data_pipeline_training::SortedRun {
        parts: paths
            .iter()
            .map(|path| path.file_name().unwrap().into())
            .collect(),
    };
    std::fs::write(&description, serde_json::to_vec(&relative).unwrap()).unwrap();
    description
}
#[tokio::test(flavor = "current_thread")]
async fn published_material_reuses_twice_without_raw_and_preserves_complete_run_and_fresh_state() {
    let temp = tempfile::tempdir().unwrap();
    let (mut legacy, rules) = setup(temp.path());
    add_binance_depth_to_reuse_material(&legacy);
    initialize_at(&mut legacy, BASE + 1000, &[FIRST]);
    let mut old_probe = Probe {
        rules: Some(rules.clone()),
        cancel_first: true,
        ..Default::default()
    };
    let old_report = run_historical_v1(&legacy, rules.clone(), &mut old_probe)
        .await
        .unwrap();
    assert!(old_probe.bought_first && old_probe.bought_second && old_probe.canceled);
    let mut config = legacy.preparation.clone();
    config.work_root = temp.path().join("published");
    let description = prepare_and_publish_historical_run_v1(&config, None).unwrap();
    let material = PreparedHistoricalMaterialV1::read(&description).unwrap();
    assert!(material.public_prefix.normal_end);
    assert!(!material.initialization.input_positions.is_empty());
    use data_pipeline_training::{public_recording::*, public_replay::PublicReplayV1};
    let mut reader =
        PublicRecordingReaderV1::recover(&config.work_root.join("public"), 1, 1 << 20).unwrap();
    let mut compact_rows = 0;
    let mut source_bindings = Vec::new();
    while let Some(row) = reader.next_row().unwrap() {
        match row {
            PublicStoredRowV1::BinanceDepthReference { .. } => compact_rows += 1,
            PublicStoredRowV1::SourceBinding(binding) => source_bindings.push(binding),
            PublicStoredRowV1::Canonical {
                record: CanonicalEventRecordV2::ReferenceEvent(event),
                ..
            } if matches!(
                event.payload,
                CanonicalReferenceEventPayloadV2::BinanceDepthState(_)
            ) =>
            {
                panic!("新历史材料仍保存完整派生深度")
            }
            _ => {}
        }
    }
    assert_eq!(compact_rows, 3);
    let mut replay = PublicReplayV1::open(&config.work_root.join("public"), 1, 1 << 20).unwrap();
    // 同一扩充Input经原重放和默认writer形成旧完整表示；仅用于本小型兼容夹具。
    let full_root = config.work_root.join("full-public");
    let mut full_writer =
        PublicRecordingWriterV1::create(&full_root, replay.core.header(), config.recording.clone())
            .unwrap();
    let mut last_depth = None;
    while let Some((input, applied)) = replay.next_applied().unwrap() {
        if let data_pipeline_core::public_input::DptIngressV1::Message {
            source_evidence, ..
        } = &input.ingress
        {
            source_bindings.retain(|binding| {
                if binding.source_message_id == source_evidence.source_message_id {
                    full_writer.record_source_binding(binding.clone()).unwrap();
                    false
                } else {
                    true
                }
            });
        }
        for group in &applied.applied.closed_groups {
            for observation in &group.observations {
                if let Some(depth) = &observation.binance_depth.value {
                    last_depth = Some(depth.clone());
                }
            }
        }
        full_writer.record(input, applied).unwrap();
    }
    assert!(source_bindings.is_empty());
    assert_eq!(
        full_writer.stabilize().unwrap().unwrap(),
        material.public_prefix
    );
    let mut full_material = material.clone();
    full_material.public_relative_root = "full-public".into();
    let full_description = config.work_root.join("full-format-test.json");
    std::fs::write(
        &full_description,
        serde_json::to_vec(&full_material).unwrap(),
    )
    .unwrap();
    let depth = last_depth.unwrap();
    let depth = depth.full().unwrap();
    assert_eq!(depth.last_update_id, 102);
    assert!(
        depth
            .bids
            .iter()
            .any(|l| l.price_usdt_per_btc.0 == "64100" && l.quantity_btc.0 == "0.3")
    );
    // 两次复用读取等价多片；旧完整表示仍用于独立经济对照。
    let parts_root = config.work_root.join("parts-public");
    std::fs::create_dir(&parts_root).unwrap();
    for file in ["stream.json", "historical-initialization.json"] {
        let source = config.work_root.join("public").join(file);
        if source.exists() {
            std::fs::copy(source, parts_root.join(file)).unwrap();
        }
    }
    for entry in std::fs::read_dir(config.work_root.join("public")).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name().to_str().unwrap().starts_with("stable-") {
            let target = parts_root.join(entry.file_name());
            std::fs::create_dir(&target).unwrap();
            std::fs::copy(entry.path().join("prefix.json"), target.join("prefix.json")).unwrap();
            split_material_fixture(
                &entry.path().join("captured.parquet"),
                &target.join("captured.parquet"),
            );
        }
    }
    let mut parts_material = material.clone();
    parts_material.public_relative_root = "parts-public".into();
    for relative in [
        &mut parts_material.hidden_derived_relative_path,
        &mut parts_material.hidden_index_relative_path,
    ] {
        let source = config.work_root.join(&*relative);
        let destination = source.with_file_name(format!(
            "{}-fixture.parquet",
            source.file_stem().unwrap().to_str().unwrap()
        ));
        *relative = split_material_fixture(&source, &destination)
            .strip_prefix(&config.work_root)
            .unwrap()
            .to_path_buf();
    }
    let parts_description = config.work_root.join("parts-format-test.json");
    std::fs::write(
        &parts_description,
        serde_json::to_vec(&parts_material).unwrap(),
    )
    .unwrap();
    let original = material_bytes(&config.work_root);
    std::fs::rename(
        temp.path().join("inputs"),
        temp.path().join("raw-moved-away"),
    )
    .unwrap();
    // 损坏的更新目录不属于已发布凭据，复用不得recover到“最新”目录。
    let later = config.work_root.join("public/stable-99999999999999999999");
    std::fs::create_dir(&later).unwrap();
    std::fs::write(later.join("prefix.json"), b"broken-later-prefix").unwrap();
    let before = material_bytes(&config.work_root);
    let mut expected = serde_json::to_value(&old_report).unwrap();
    normalize_output_paths(&mut expected, temp.path());
    for suffix in ["reuse-full", "reuse-a", "reuse-b"] {
        let root = temp.path().join(suffix);
        let run = reuse_outputs(&legacy, &root);
        let mut probe = Probe {
            rules: Some(rules.clone()),
            cancel_first: true,
            ..Default::default()
        };
        assert!(!probe.bought_first && probe.dispatched == 0);
        let selected_description = if suffix == "reuse-full" {
            &full_description
        } else {
            &parts_description
        };
        let report =
            run_prepared_historical_v1(selected_description, &run, rules.clone(), &mut probe)
                .await
                .unwrap();
        let first = probe.first_economics.as_ref().unwrap();
        assert_eq!(
            serde_json::to_value(first).unwrap(),
            serde_json::to_value(old_probe.first_economics.as_ref().unwrap()).unwrap()
        );
        assert_eq!(first.cash.actual_available_pusd.0, "2000");
        assert!(probe.bought_first && probe.bought_second && probe.canceled);
        let economics = report.account_economics_memory.as_ref().unwrap();
        let first_market = economics
            .markets
            .iter()
            .find(|m| m.condition_id.0 == FIRST)
            .unwrap();
        eprintln!(
            "{suffix}: first-market taker economics {:?}",
            first_market.executed[1]
        );
        assert_eq!(first_market.executed[1].shares, ExactDecimal("10".into()));
        assert_eq!(
            first_market.executed[1].principal_pusd,
            ExactDecimal("5".into())
        );
        // 快照新增合法计算；首次发送与1701隐藏价0.5更新同毫秒，成交10份×0.5。
        // 冻结规则rate=0.07、exponent=1；按公式独立计算，不沿用无深度场景的0.168。
        let price = Decimal::new(5, 1);
        let expected_fee = Decimal::from(10) * Decimal::new(7, 2) * price * (Decimal::ONE - price);
        assert_eq!(
            Decimal::from_str_exact(&first_market.executed[1].actual_fee_pusd.0).unwrap(),
            expected_fee
        );
        let ledger = report.ledger_consumed_memory.as_ref().unwrap();
        assert_eq!(
            ledger
                .market_projections
                .iter()
                .find(|m| m.condition_id.0 == FIRST)
                .unwrap()
                .fees_pusd,
            first_market.executed[1].actual_fee_pusd
        );
        assert_eq!(probe.observed, old_probe.observed);
        let mut actual = serde_json::to_value(report).unwrap();
        normalize_output_paths(&mut actual, &root);
        assert_eq!(actual, expected, "完整报告仅归一各自输出根");
        let metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&run.metadata_path).unwrap()).unwrap();
        assert!(metadata["config"].get("preparation").is_none());
        assert_eq!(
            metadata["prepared_material"],
            serde_json::to_value(if suffix == "reuse-full" {
                &full_material
            } else {
                &parts_material
            })
            .unwrap()
        );
        assert!(run.result_path.exists());
    }
    assert_eq!(material_bytes(&config.work_root), before);
    // 发布产生的每个原文件仍逐字保持，不计测试额外添加的更新目录。
    for (path, bytes) in original {
        assert_eq!(std::fs::read(config.work_root.join(path)).unwrap(), bytes);
    }
}
#[tokio::test(flavor = "current_thread")]
async fn prepared_description_rejects_version_prefix_missing_market_and_hidden_mix_at_real_boundaries()
 {
    let temp = tempfile::tempdir().unwrap();
    let (config, rules) = setup(temp.path());
    let path = prepare_and_publish_historical_run_v1(&config.preparation, None).unwrap();
    let original = PreparedHistoricalMaterialV1::read(&path).unwrap();
    for (name, mut material) in [
        ("version", original.clone()),
        ("prefix", original.clone()),
        ("market", original.clone()),
        ("hidden", original.clone()),
    ] {
        match name {
            "version" => material.schema_version = "unsupported".into(),
            "prefix" => material.public_prefix.next_input_position += 1,
            "market" => {
                material.markets.remove(FIRST);
            }
            "hidden" => {
                material.hidden_consumer_stream_id = "foreign-consumer".into();
                material.preparation.hidden_stream_id = "foreign-consumer".into();
            }
            _ => unreachable!(),
        }
        // 不改正式描述：另一个不可信描述指向同样的原材料。
        let bad = path.parent().unwrap().join(format!("{name}.json"));
        std::fs::write(&bad, serde_json::to_vec(&material).unwrap()).unwrap();
        let run = reuse_outputs(&config, &temp.path().join(name));
        let failure = run_prepared_historical_v1(&bad, &run, rules.clone(), &mut Probe::default())
            .await
            .unwrap_err();
        let HistoricalFailureCauseV1::Material(error) = failure.cause else {
            panic!("应返回材料错误：{failure:?}");
        };
        let expected = match name {
            "version" => "不支持",
            "prefix" => "凭据",
            "market" => "缺少公开市场",
            "hidden" => "完整引用或consumer",
            _ => unreachable!(),
        };
        assert!(error.to_string().contains(expected), "{name}: {error}");
        assert!(!run.result_path.exists());
    }
}
#[test]
fn failed_prepare_never_publishes_success_description() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, _) = setup(temp.path());
    config.preparation.recording.max_capture_output_bytes = 1;
    let error = prepare_and_publish_historical_run_v1(&config.preparation, None).unwrap_err();
    assert!(
        error.to_string().contains("捕获产物超过冻结的总字节上限"),
        "{error}"
    );
    assert!(
        !config
            .preparation
            .work_root
            .join(PREPARED_HISTORICAL_MATERIAL_FILE_V1)
            .exists()
    );
}

#[derive(Clone, Debug)]
struct HealthDiagnostics {
    expiry: Option<i64>,
    frozen_quote: Option<String>,
}
struct HealthProbe {
    enabled: bool,
    expiry_delay: i64,
    rules: Arc<SealedProcessRuleSnapshotV3>,
    proposed: bool,
    detected: Vec<(i64, i64, i64, String)>,
    recalculated: Vec<i64>,
    sent_repairs: Vec<(i64, i64)>,
    checks: usize,
    last_qualification_at: i64,
    event_lag_limit: Option<i64>,
}
impl StrategyV1 for HealthProbe {
    type Error = &'static str;
    type Diagnostics = HealthDiagnostics;
    fn uses_source_health(&self) -> bool {
        self.enabled
    }
    fn next_health_evaluation_at(
        &self,
        _: &[sim_system::public_consumption::PublicObservationFrameV1<'_>],
        calculation: Option<&StrategyCalculationV1<Self::Diagnostics>>,
        at: i64,
    ) -> Option<i64> {
        let StrategyOutputV1::Continue { diagnostics, .. } = &calculation?.output else {
            return None;
        };
        diagnostics.expiry.filter(|expiry| *expiry > at)
    }
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1<Self::Diagnostics>, Self::Error> {
        let up = input
            .markets
            .iter()
            .find(|m| m.observation.market.condition_id.0 == FIRST)
            .and_then(|m| {
                m.observation
                    .assets
                    .iter()
                    .find(|a| a.asset.token_id.0 == "20")
            });
        let quote = up
            .and_then(|a| a.asks.value.as_ref())
            .and_then(|l| l.first())
            .map(|l| l.price_pusd_per_share.0.clone());
        let mut intents = vec![];
        let mut expiry = None;
        if quote.is_some() && !self.proposed {
            self.proposed = true;
            intents.push(buy(
                &self.rules,
                FIRST,
                "20",
                "health-original-buy",
                "0.4",
                true,
            ));
            expiry = Some(at + self.expiry_delay);
        } else if !self.detected.is_empty() && self.sent_repairs.is_empty() {
            self.recalculated.push(at);
            intents.push(ReservedIntentV1 {
                context: RequestContextV2 {
                    global_request_id: format!("new-repair-{at}"),
                    caller_correlation_id: None,
                },
                intent: Arc::new(ModelIntentV1::CancelMarketOrders {
                    condition_id: ConditionId(FIRST.into()),
                }),
                reserves: vec![],
            });
        }
        Ok(StrategyOutputV1::Continue {
            intents,
            next_evaluation_at_utc_ms: None,
            diagnostics: HealthDiagnostics {
                expiry,
                frozen_quote: quote,
            },
        })
    }
    fn qualify_intent(
        &mut self,
        input: &StrategyCurrentReadV1<'_>,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
        _: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        self.checks += 1;
        self.last_qualification_at = at;
        assert_eq!(
            input
                .action_before
                .cutoff()
                .decision_time()
                .logical_time_utc_ms(),
            at,
            "新值必须配当前合法截止"
        );
        let StrategyOutputV1::Continue { diagnostics, .. } = &calculation.output else {
            panic!()
        };
        let event_lagged = diagnostics.expiry.is_some()
            && self.event_lag_limit.is_some_and(|limit| {
                input
                    .markets
                    .iter()
                    .find(|m| m.observation.market.condition_id.0 == FIRST)
                    .and_then(|m| {
                        m.observation.source_progress.iter().find(|p| {
                            p.scope.source == SourceStreamKindV1::PolymarketBook
                                && p.scope.asset_or_symbol == "20"
                        })
                    })
                    .and_then(|p| p.last_valid_business_message.as_ref())
                    .is_some_and(|m| {
                        m.source_event_at_utc_ms
                            .is_some_and(|event| m.received_at_utc_ms - event > limit)
                    })
            });
        Ok(
            if diagnostics.expiry.is_some_and(|expiry| at >= expiry) || event_lagged {
                IntentQualificationV1::Invalidated(IntentInvalidationReasonV1 {
                    reason_code: "synthetic-up-stream-silence".into(),
                    scopes: vec![SourceStreamScopeV1 {
                        source: SourceStreamKindV1::PolymarketBook,
                        condition_id: Some(ConditionId(FIRST.into())),
                        asset_or_symbol: "20".into(),
                    }],
                })
            } else {
                IntentQualificationV1::Qualified
            },
        )
    }
    fn invalidated(
        &mut self,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
        result: &InvalidatedBatchV1,
    ) {
        let StrategyOutputV1::Continue { diagnostics, .. } = &calculation.output else {
            panic!()
        };
        assert!(result.sent.is_empty());
        assert_eq!(result.invalidated_intent_index, 0);
        self.detected.push((
            result.detected_at_utc_ms,
            calculation.calculation_started_at_utc_ms,
            result.observed_cutoff.decision_time().logical_time_utc_ms(),
            diagnostics.frozen_quote.clone().unwrap(),
        ));
    }
    fn dispatched(
        &mut self,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
        result: &Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1>,
    ) {
        let StrategyOutputV1::Continue { intents, .. } = &calculation.output else {
            return;
        };
        if intents
            .first()
            .is_some_and(|i| matches!(i.intent.as_ref(), ModelIntentV1::CancelMarketOrders { .. }))
        {
            assert_eq!(result.as_ref().unwrap().len(), 1);
            self.sent_repairs.push((
                calculation.calculation_started_at_utc_ms,
                self.last_qualification_at,
            ));
        }
    }
}
#[tokio::test(flavor = "current_thread")]
async fn health_deadline_during_compute_or_wait_invalidates_and_recomputes_without_new_up_message()
{
    for expiry_delay in [100, 300] {
        let temp = tempfile::tempdir().unwrap();
        let (config, rules) = setup(temp.path());
        let mut probe = HealthProbe {
            enabled: true,
            expiry_delay,
            rules: rules.clone(),
            proposed: false,
            detected: vec![],
            recalculated: vec![],
            sent_repairs: vec![],
            checks: 0,
            last_qualification_at: 0,
            event_lag_limit: None,
        };
        let report = run_historical_v1(&config, rules, &mut probe).await.unwrap();
        assert_eq!(probe.detected.len(), 1);
        let (detected, started, cutoff, quote) = &probe.detected[0];
        assert_eq!(*detected, *started + expiry_delay);
        assert_eq!(cutoff, detected);
        assert_eq!(quote, "0.6", "原诊断冻结");
        assert_eq!(
            probe.recalculated[0],
            (*started + 200).max(*detected),
            "保留原计算完成边界，等待发送可立刻重算"
        );
        assert_eq!(
            probe.sent_repairs[0].1,
            probe.sent_repairs[0].0 + 400,
            "新决定仍支付配置的计算和发送等待"
        );
        let health = report.source_health.unwrap();
        assert_eq!(health.invalidated_batches, 1);
        assert!(health.deadline_wakeups >= 1);
        let invalidated = health.last_invalidated_calculation_timing.unwrap();
        assert_eq!(
            invalidated.calculation_completed_at_utc_ms - invalidated.calculation_started_at_utc_ms,
            200
        );
        assert!(invalidated.actual_sent_at_utc_ms.is_none());
        let wire = std::fs::read_to_string(&config.sim_log).unwrap();
        assert!(
            !wire.contains("health-original-buy"),
            "尚未尝试时没有请求效果"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn fresh_arrival_with_lagged_event_invalidates_own_dependency_but_independent_cancel_executes()
 {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, rules) = setup(temp.path());
    config.timing.calculation_duration_ms = DelayScheduleV1::Fixed(800);
    config.timing.dispatch_delay_ms = DelayScheduleV1::Fixed(800);
    let mut probe = HealthProbe {
        enabled: true,
        expiry_delay: 20000,
        rules: rules.clone(),
        proposed: false,
        detected: vec![],
        recalculated: vec![],
        sent_repairs: vec![],
        checks: 0,
        last_qualification_at: 0,
        event_lag_limit: Some(200),
    };
    let report = run_historical_v1(&config, rules, &mut probe).await.unwrap();
    assert_eq!(probe.detected.len(), 1);
    assert_eq!(
        probe.detected[0].0,
        BASE + 2301,
        "收到本源新消息但其来源事件已滞后"
    );
    assert!(
        probe.sent_repairs[0].1 > probe.detected[0].0,
        "不依赖滞后估计的显式取消仍能发送"
    );
    assert_eq!(report.source_health.unwrap().invalidated_batches, 1);
}

struct QuietDeadlineProbe {
    deadline: i64,
    observed: Vec<i64>,
}
impl StrategyV1 for QuietDeadlineProbe {
    type Error = &'static str;
    type Diagnostics = ();
    fn uses_source_health(&self) -> bool {
        true
    }
    fn next_health_evaluation_at(
        &self,
        _: &[sim_system::public_consumption::PublicObservationFrameV1<'_>],
        _: Option<&StrategyCalculationV1>,
        at: i64,
    ) -> Option<i64> {
        (at < self.deadline).then_some(self.deadline)
    }
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1, Self::Error> {
        assert_eq!(
            input
                .action_before
                .cutoff()
                .decision_time()
                .logical_time_utc_ms(),
            at
        );
        self.observed.push(at);
        Ok(StrategyOutputV1::Continue {
            intents: vec![],
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn no_trade_pending_health_deadline_retains_followup_with_original_calculation_delays() {
    for (deadline, start_delay, followup) in [(2400, 0, 2701), (2500, 0, 2701), (2500, 50, 2801)] {
        let temp = tempfile::tempdir().unwrap();
        let (mut config, rules) = setup(temp.path());
        config.timing.calculation_start_delay_ms = DelayScheduleV1::Fixed(start_delay);
        let mut probe = QuietDeadlineProbe {
            deadline: BASE + deadline,
            observed: vec![],
        };
        let report = run_historical_v1(&config, rules, &mut probe).await.unwrap();
        let preceding = BASE + 2301 + start_delay as i64;
        let next = probe
            .observed
            .iter()
            .copied()
            .find(|at| *at > preceding)
            .unwrap();
        assert_eq!(
            next,
            BASE + followup,
            "空批不失效也须在原发送结束并支付启动延迟后重算，不等新行情"
        );
        let health = report.source_health.unwrap();
        assert_eq!(health.deadline_wakeups, 1);
        assert_eq!(health.invalidated_batches, 0);
        assert_eq!(health.qualification_calls, 0);
    }
}

struct MeasurementProbe {
    enabled: bool,
    decisions: usize,
    dispatches: usize,
}
impl StrategyV1 for MeasurementProbe {
    type Error = &'static str;
    type Diagnostics = ();
    fn uses_source_health(&self) -> bool {
        self.enabled
    }
    fn next_health_evaluation_at(
        &self,
        _: &[sim_system::public_consumption::PublicObservationFrameV1<'_>],
        _: Option<&StrategyCalculationV1>,
        at: i64,
    ) -> Option<i64> {
        [BASE + 2500, BASE + 3500]
            .into_iter()
            .find(|deadline| *deadline > at)
    }
    fn decide(&mut self, _: &StrategyInputV1, _: i64) -> Result<StrategyOutputV1, Self::Error> {
        self.decisions += 1;
        Ok(StrategyOutputV1::Continue {
            intents: vec![],
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        })
    }
    fn dispatched(
        &mut self,
        _: &StrategyCalculationV1,
        result: &Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1>,
    ) {
        assert!(result.as_ref().unwrap().is_empty());
        self.dispatches += 1;
    }
}
/// 有限合成机制开销测量；每个选项以独立进程运行，外部 GNU time 记录 CPU 与峰值内存。
#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn source_health_finite_overhead_measurement() {
    let enabled = std::env::var("SOURCE_HEALTH_MEASUREMENT_ENABLED").unwrap() == "1";
    let evidence_root =
        std::path::PathBuf::from(std::env::var("SOURCE_HEALTH_MEASUREMENT_ROOT").unwrap());
    let root = evidence_root.join(if enabled { "selected" } else { "unselected" });
    std::fs::create_dir(&root).unwrap();
    let (config, rules) = setup(&root);
    let manifest = &config.preparation.scope.manifest_paths[0];
    let input_identity = {
        use sha2::{Digest, Sha256};
        format!("{:x}", Sha256::digest(std::fs::read(manifest).unwrap()))
    };
    let mut probe = MeasurementProbe {
        enabled,
        decisions: 0,
        dispatches: 0,
    };
    let started = std::time::Instant::now();
    let report = run_historical_v1(&config, rules, &mut probe).await.unwrap();
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let timing = report.last_dispatched_calculation_timing.as_ref().unwrap();
    println!(
        "{}",
        serde_json::json!({ "selected": enabled, "input_manifest_sha256": input_identity, "elapsed_ms": elapsed_ms,
        "decision_count": probe.decisions, "dispatch_count": probe.dispatches, "qualification_calls": report.source_health.as_ref().map_or(0, |s| s.qualification_calls),
        "health_deadline_wakeups": report.source_health.as_ref().map_or(0, |s| s.deadline_wakeups),
        "last_decision_to_scheduled_dispatch_ms": timing.scheduled_send_at_utc_ms - timing.calculation_started_at_utc_ms,
        "purpose": "合成工程开销；不是盈利、历史静默事故或实际在线性能证据" })
    );
}

#[test]
fn day_received_range_does_not_include_late_received_hidden_source_fact() {
    let mut bucket_times = vec![];
    for end in [BASE + 2000, BASE + 2500] {
        let temp = tempfile::tempdir().unwrap();
        let (mut config, rules) = setup(temp.path());
        config.preparation.scope.received_end_utc_ms = end;
        let day = HistoricalDayConfigV1 {
            preparation: config.preparation.clone(),
            candidates: vec![HistoricalPreparedRunConfigV1::from(&config)],
            progress_interval_seconds: 0,
            producer_lead_groups: Some(1),
        };
        let result = run_historical_days_v1(&[day], rules, &|_| {
            Ok(Probe {
                no_trade_only: true,
                ..Default::default()
            })
        })
        .unwrap();
        assert_eq!(result[0].candidates[0].report["end_reason"], "RangeEnd");
        let mut times = vec![];
        for frame in sim_log_values(&config.sim_log) {
            if let Some(inputs) = frame
                .as_array()
                .and_then(|frame| frame.first())
                .and_then(|inputs| inputs.as_array())
            {
                for input in inputs {
                    // 日布局原隐藏桶帧：[4, 原前缀位置, 源时间, 原索引范围, 封口依据]。
                    if input[0] == 4 {
                        times.push(input[2].as_i64().unwrap());
                    }
                }
            }
        }
        bucket_times.push(times);
    }
    // 原夹具该行的平台源时间为1701，接收时间为2200；不是两种范围共享的隐藏前缀。
    assert!(!bucket_times[0].contains(&(BASE + 1701)));
    assert!(bucket_times[1].contains(&(BASE + 1701)));
}

#[test]
fn original_day_shares_public_core_and_keeps_delayed_candidates_independent() {
    let tmp = tempfile::tempdir().unwrap();
    let (config, rules) = setup(tmp.path());
    let mut first = HistoricalPreparedRunConfigV1::from(&config);
    // 原零容量通道迫使实际日入口逐次等交接；不改变生产默认或业务时间。
    first.finite_in_flight_groups = 0;
    let mut second = first.clone();
    first.session.run_id = "day-fast".into();
    second.session.run_id = "day-slow".into();
    second.session.account_id = "independent-slow-account".into();
    second.timing.public_delivery_delay_ms = DelayScheduleV1::Fixed(300);
    second.sim_log = tmp.path().join("slow-sim.jsonl");
    second.noe_root = tmp.path().join("slow-noe");
    second.ledger_root = tmp.path().join("slow-ledger");
    second.metadata_path = tmp.path().join("slow-metadata.json");
    second.result_path = tmp.path().join("slow-result.json");
    let day = HistoricalDayConfigV1 {
        preparation: config.preparation.clone(),
        candidates: vec![first, second],
        progress_interval_seconds: 0,
        producer_lead_groups: Some(1),
    };
    let results = run_historical_days_v1(&[day.clone()], rules.clone(), &|_| {
        Ok(Probe {
            rules: Some(rules.clone()),
            ..Default::default()
        })
    })
    .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].public_applied_inputs, 9);
    assert_eq!(results[0].candidates.len(), 2);
    for candidate in &results[0].candidates {
        assert_eq!(candidate.report["end_reason"], "RangeEnd");
        assert!(candidate.report["recording_drained"].as_bool().unwrap());
        assert!(candidate.report["calculation_count"].as_u64().unwrap() > 0);
    }
    fn assert_no_resume_files(root: &std::path::Path) {
        for entry in std::fs::read_dir(root).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            assert!(
                name != "source-order.cbor"
                    && name != "checkpoint.json"
                    && !name.starts_with("checkpoint-"),
                "回测不得保存恢复材料: {}",
                entry.path().display()
            );
            if entry.file_type().unwrap().is_dir() {
                assert_no_resume_files(&entry.path());
            }
        }
    }
    assert_no_resume_files(tmp.path());
    assert!(
        !config.preparation.work_root.exists(),
        "日入口不得生成完整宽录制"
    );
    let fast = serde_json::to_string(&sim_log_values(&config.sim_log)).unwrap();
    let slow = serde_json::to_string(&sim_log_values(&tmp.path().join("slow-sim.jsonl"))).unwrap();
    assert!(fast.contains("day-fast"));
    assert!(!fast.contains("day-slow"));
    assert!(slow.contains("day-slow"));
    assert!(slow.contains("independent-slow-account"));
    assert_ne!(
        results[0].candidates[0].report["last_dispatched_calculation_timing"],
        results[0].candidates[1].report["last_dispatched_calculation_timing"]
    );
}

#[test]
fn candidate_unwind_publishes_failure_and_wakes_daily_waiters() {
    let tmp = tempfile::tempdir().unwrap();
    let (config, rules) = setup(tmp.path());
    let day = HistoricalDayConfigV1 {
        preparation: config.preparation.clone(),
        candidates: vec![HistoricalPreparedRunConfigV1::from(&config)],
        progress_interval_seconds: 0,
        producer_lead_groups: Some(1),
    };
    let error = run_historical_days_v1(&[day], rules, &|_| -> Result<Probe, MaterialError> {
        panic!("fixture candidate unwind")
    })
    .unwrap_err();
    assert!(error.to_string().contains("候选线程异常退出"));
    assert!(!config.result_path.exists());
}

#[test]
fn two_original_dates_keep_two_thousand_and_same_day_cross_market_continuity() {
    let tmp = tempfile::tempdir().unwrap();
    let mut days = Vec::new();
    let mut shared_rules = None;
    for (index, date) in ["2026-08-24", "2026-08-25"].iter().enumerate() {
        let root = tmp.path().join(date);
        std::fs::create_dir(&root).unwrap();
        let (config, rules) = setup_day(&root, BASE + index as i64 * 86_400_000, date);
        shared_rules = Some(rules);
        let candidate = HistoricalPreparedRunConfigV1::from(&config);
        days.push(HistoricalDayConfigV1 {
            preparation: config.preparation,
            candidates: vec![candidate],
            progress_interval_seconds: 0,
            producer_lead_groups: Some(1),
        });
    }
    let rules = shared_rules.unwrap();
    let reports = run_historical_days_with_parallelism_v1(
        &days,
        rules.clone(),
        &|_| {
            Ok(Probe {
                rules: Some(rules.clone()),
                ..Default::default()
            })
        },
        2,
    )
    .unwrap();
    assert_eq!(reports.len(), 2);
    for (day, result) in days.iter().zip(&reports) {
        let candidate = &result.candidates[0];
        assert_eq!(
            candidate.initial_available_pusd,
            ExactDecimal("2000".into())
        );
        let frames = sim_log_values(&day.candidates[0].sim_log);
        assert_eq!(
            frames
                .iter()
                .filter(|frame| frame[0]["run_id"].is_string())
                .count(),
            1
        );
        assert_eq!(frames[0][0]["run_id"], day.candidates[0].session.run_id);
        assert!(
            candidate.report["account_economics_memory"]["markets"]
                .as_array()
                .unwrap()
                .len()
                >= 2,
            "同日原账户连续跨两个市场"
        );
    }
    let summaries = summarize_independent_historical_days_v1(&reports).unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].days, 2);
    assert_eq!(
        summaries[0].initial_capital_pusd,
        contracts::economics::EconomicRational::from_decimal(Decimal::from(4000)).public()
    );
    let pnl = match &summaries[0].total_confirmed_net_pnl_pusd {
        ExecutionReadValueV1::Known(pnl) => {
            contracts::economics::EconomicRational::try_from(pnl).unwrap()
        }
        _ => panic!("有效日夹具净结果应已知"),
    };
    assert_eq!(
        summaries[0].capital_weighted_average_day_confirmed_return,
        ExecutionReadValueV1::Known(
            pnl.divided_by(&contracts::economics::EconomicRational::from_decimal(
                Decimal::from(4000)
            ))
            .public()
        )
    );
}

#[test]
fn original_day_quantity_demand_union_preserves_complete_and_quantity_free_candidates() {
    use std::sync::atomic::{AtomicBool, Ordering};
    for (any_full, statistics_enabled) in [(false, false), (true, false), (false, true)] {
        let tmp = tempfile::tempdir().unwrap();
        let (config, rules) = setup(tmp.path());
        add_binance_depth_to_reuse_material(&config);
        let mut first = HistoricalPreparedRunConfigV1::from(&config);
        let mut second = first.clone();
        if statistics_enabled {
            first.public_statistics = Some(PublicStatisticsConfigV1 {
                momentum_windows_ms: vec![1000],
                spot_variance_windows_ms: vec![1000],
                terminal_integral_windows_ms: vec![1000],
            });
        }
        second.session.run_id = "quantity-second".into();
        second.session.account_id = "quantity-second-account".into();
        second.sim_log = tmp.path().join("second-sim.jsonl");
        second.noe_root = tmp.path().join("second-noe");
        second.ledger_root = tmp.path().join("second-ledger");
        second.metadata_path = tmp.path().join("second-metadata.json");
        second.result_path = tmp.path().join("second-result.json");
        second.timing.public_delivery_delay_ms = DelayScheduleV1::Fixed(300);
        let day = HistoricalDayConfigV1 {
            preparation: config.preparation.clone(),
            candidates: vec![first, second],
            progress_interval_seconds: 0,
            producer_lead_groups: Some(1),
        };
        let seen = [
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        ];
        let results = run_historical_days_v1(&[day], rules.clone(), &|candidate| {
            let index = usize::from(candidate.session.run_id == "quantity-second");
            Ok(Probe {
                quantity_demand: Some(any_full && index == 0),
                expected_quantity_projection: Some(any_full || statistics_enabled),
                statistics_expected: statistics_enabled && index == 0,
                saw_depth: Some(seen[index].clone()),
                rules: Some(rules.clone()),
                ..Default::default()
            })
        })
        .unwrap();
        assert_eq!(results[0].candidates.len(), 2);
        assert!(
            seen.iter().all(|seen| seen.load(Ordering::Relaxed)),
            "两种独立交付前沿均实际读到深度描述"
        );
        for candidate in &results[0].candidates {
            assert_eq!(candidate.report["end_reason"], "RangeEnd");
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn source_health_host_honors_trade_detail_demand_without_changing_observation_or_economics() {
    let mut outputs = Vec::new();
    for details in [true, false] {
        let temp = tempfile::tempdir().unwrap();
        let (mut config, rules) = setup(temp.path());
        // 原首市场序列足以生成真实成交；不进入依赖完整明细的第二市场探针。
        config.preparation.scope.received_end_utc_ms = BASE + 4000;
        let mut probe = Probe {
            source_health: true,
            trade_details: Some(details),
            rules: Some(rules.clone()),
            ..Default::default()
        };
        let report = run_historical_v1(&config, rules, &mut probe).await.unwrap();
        assert!(report.source_health.is_some(), "实际宿主须走健康观察入口");
        assert!(probe.bought_first);
        if details {
            assert!(
                probe.observed.iter().any(|(_, _, _, trades)| *trades > 0),
                "完整消费者确实见到原成交明细，不能以空历史自证省略"
            );
        } else {
            assert!(probe.observed.iter().all(|(_, _, _, trades)| *trades == 0));
        }
        outputs.push((
            probe
                .observed
                .iter()
                .map(|(at, count, ask, _)| (*at, *count, ask.clone()))
                .collect::<Vec<_>>(),
            serde_json::to_value(&report.account_economics_memory).unwrap(),
            serde_json::to_value(&report.day_economics).unwrap(),
            report.calculation_count,
            probe.dispatched,
            serde_json::to_value(&report.source_health).unwrap(),
        ));
    }
    assert_eq!(
        outputs[0], outputs[1],
        "健康入口同前沿/观察、真实账户及资金积分保持"
    );
}

fn research_day(config: &HistoricalRunConfigV1, _root: &std::path::Path) -> HistoricalDayConfigV1 {
    HistoricalDayConfigV1 {
        preparation: config.preparation.clone(),
        candidates: vec![HistoricalPreparedRunConfigV1::from(config)],
        progress_interval_seconds: 0,
        producer_lead_groups: Some(1),
    }
}

#[test]
fn research_hours_exclude_same_millisecond_boundary_and_keep_empty_hours() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut config, rules) = setup(tmp.path());
    config.preparation.scope.received_end_utc_ms = BASE + 3 * 3_600_000;
    config.timing.public_delivery_delay_ms = DelayScheduleV1::Fixed(3_598_900);
    let mut day = research_day(&config, &tmp.path().join("state"));
    let mut second = reuse_outputs(&config, &tmp.path().join("second"));
    second.session.run_id = "independent-research".into();
    second.session.account_id = "other-account".into();
    day.candidates.push(second);
    let results = run_historical_days_with_options_v1(
        &[day],
        rules.clone(),
        &|_| {
            Ok(Probe {
                no_trade_only: true,
                rules: Some(rules.clone()),
                ..Default::default()
            })
        },
        1,
        &HistoricalDayRunOptionsV1 {
            research_summary: true,
            ..Default::default()
        },
    )
    .unwrap();
    for result in &results[0].candidates {
        let r = &result.report;
        let research = &r["research"];
        let hours = research["hours"].as_array().unwrap();
        assert_eq!(hours.len(), 3);
        assert_eq!(research["decisions"]["successful"], r["calculation_count"]);
        assert_eq!(research["decisions"]["no_trade"], r["calculation_count"]);
        assert_eq!(research["decisions"]["actions"], 0);
        let cutoff = &hours[0]["cumulative"]["observation_cutoff"];
        assert!(
            cutoff["decision_time"]["logical_time_utc_ms"]
                .as_i64()
                .unwrap()
                < BASE + 3_600_000
        );
        assert!(hours[1]["decision_changes"]["successful"].as_u64().unwrap() > 0);
        assert_eq!(hours[2]["decision_changes"]["successful"], 0);
        assert_eq!(
            hours[1]["cumulative"]["decisions"],
            hours[2]["cumulative"]["decisions"]
        );
        assert_eq!(
            hours[1]["cumulative"]["day_economics"]["maximum_nav_drawdown_pusd"],
            hours[2]["cumulative"]["day_economics"]["maximum_nav_drawdown_pusd"]
        );
    }
    let a = &results[0].candidates[0].report["research"]["final_snapshot"];
    let b = &results[0].candidates[1].report["research"]["final_snapshot"];
    for field in ["metrics", "counters", "decisions", "day_economics"] {
        assert_eq!(a[field], b[field]);
    }
    assert_ne!(
        a["observation_cutoff"]["decision_time"]["run_id"],
        b["observation_cutoff"]["decision_time"]["run_id"]
    );
}

#[test]
fn research_budget_returns_partial_results_and_fresh_run_keeps_business_results() {
    let tmp = tempfile::tempdir().unwrap();
    let (config, rules) = setup(tmp.path());
    let day = research_day(&config, &tmp.path().join("state"));
    let mut stopped_day = day.clone();
    stopped_day.candidates = vec![reuse_outputs(&config, &tmp.path().join("stopped"))];
    let factory = |_: &HistoricalPreparedRunConfigV1| {
        Ok(Probe {
            rules: Some(rules.clone()),
            ..Default::default()
        })
    };
    let stopped = run_historical_days_with_options_v1(
        &[stopped_day],
        rules.clone(),
        &factory,
        1,
        &HistoricalDayRunOptionsV1 {
            research_summary: true,
            budget_stop_after: Some(std::time::Duration::ZERO),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(stopped[0].candidates[0].report["end_reason"], "BudgetStop");
    assert!(
        stopped[0].candidates[0].report["research"]["hours"]
            .as_array()
            .unwrap()
            .is_empty(),
        "预算前缀尚未抵达整点，不能补未完成小时"
    );
    assert!(
        !stopped[0].candidates[0].report["recording_drained"]
            .as_bool()
            .unwrap()
    );
    assert!(stopped[0].candidates[0].report["pending_calculation_timing"].is_object());
    assert!(!tmp.path().join("state").exists());
    // 使用独立的运行产物；保留已停止账户的证据，不从它恢复。
    let fresh = run_historical_days_with_options_v1(
        &[day.clone()],
        rules.clone(),
        &factory,
        1,
        &HistoricalDayRunOptionsV1 {
            research_summary: true,
            ..Default::default()
        },
    )
    .unwrap();
    let expected = fresh[0].candidates[0].report.clone();
    let sim = std::fs::read(&config.sim_log).unwrap();
    // 同一小夹具同一路径重新运行原默认入口，比较原业务报告及逐帧输出。
    for root in [&config.noe_root, &config.ledger_root] {
        std::fs::remove_dir_all(root).unwrap();
    }
    for path in [&config.sim_log, &config.metadata_path, &config.result_path] {
        std::fs::remove_file(path).unwrap();
    }
    let original = run_historical_days_v1(&[day], rules.clone(), &factory).unwrap();
    assert!(original[0].candidates[0].report.get("research").is_none());
    let mut business = expected.clone();
    business.as_object_mut().unwrap().remove("research");
    assert_eq!(business, original[0].candidates[0].report);
    assert_eq!(sim, std::fs::read(&config.sim_log).unwrap());
    assert_eq!(
        expected["research"]["decisions"]["successful"],
        expected["calculation_count"]
    );
    assert!(
        expected["research"]["decisions"]["actions"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        expected["research"]["decisions"]["no_trade"]
            .as_u64()
            .unwrap()
            > 0
    );
}

#[test]
fn research_mixed_candidates_keep_independent_exit_and_hour_boundaries() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut config, rules) = setup(tmp.path());
    config.preparation.scope.received_end_utc_ms = BASE + 3 * 3_600_000;
    config.timing.public_delivery_delay_ms = DelayScheduleV1::Fixed(3_598_900);
    let mut day = research_day(&config, &tmp.path().join("state"));
    let mut second = reuse_outputs(&config, &tmp.path().join("second"));
    second.session.run_id = "continue-research".into();
    second.session.account_id = "continue-account".into();
    second.timing.public_delivery_delay_ms = DelayScheduleV1::Fixed(3_599_200);
    day.candidates.push(second);
    let complete = run_historical_days_with_options_v1(
        &[day],
        rules.clone(),
        &|run: &HistoricalPreparedRunConfigV1| {
            Ok(Probe {
                no_trade_only: run.session.run_id == "continue-research",
                exit: run.session.run_id != "continue-research",
                rules: Some(rules.clone()),
                ..Default::default()
            })
        },
        1,
        &HistoricalDayRunOptionsV1 {
            research_summary: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(complete[0].candidates.len(), 2);
    let exited = &complete[0].candidates[0].report;
    assert_eq!(exited["end_reason"], "StrategyExit");
    assert_eq!(exited["research"]["decisions"]["exits"], 1);
    assert_eq!(exited["research"]["hours"].as_array().unwrap().len(), 1);
    assert!(exited["last_observed_at_utc_ms"].as_i64().unwrap() < BASE + 2 * 3_600_000);
    let continued = &complete[0].candidates[1].report;
    assert_eq!(continued["end_reason"], "RangeEnd");
    assert_eq!(continued["research"]["decisions"]["exits"], 0);
    assert_eq!(continued["research"]["hours"].as_array().unwrap().len(), 3);
    assert!(
        continued["last_observed_at_utc_ms"].as_i64().unwrap()
            > exited["last_observed_at_utc_ms"].as_i64().unwrap()
    );
}

#[test]
fn research_zero_duration_exit_keeps_original_terminal_boundary_on_budget_stop() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut config, rules) = setup(tmp.path());
    config.timing.calculation_duration_ms = DelayScheduleV1::Fixed(0);
    let day = research_day(&config, &tmp.path().join("state"));
    let factory = |_: &HistoricalPreparedRunConfigV1| {
        Ok(Probe {
            exit_immediately: true,
            rules: Some(rules.clone()),
            ..Default::default()
        })
    };
    let result = run_historical_days_with_options_v1(
        &[day.clone()],
        rules.clone(),
        &factory,
        1,
        &HistoricalDayRunOptionsV1 {
            research_summary: true,
            budget_stop_after: Some(std::time::Duration::ZERO),
            ..Default::default()
        },
    )
    .unwrap();
    let report = &result[0].candidates[0].report;
    assert_eq!(
        report["end_reason"], "StrategyExit",
        "已到期Exit不能被预算分类遮断"
    );
    assert_eq!(report["research"]["decisions"]["exits"], 1);
    assert_eq!(report["research"]["decisions"]["successful"], 1);
    assert_eq!(
        report["research"]["final_snapshot"]["boundary_utc_ms"],
        report["pending_calculation_timing"]["calculation_completed_at_utc_ms"]
    );
    assert!(report["research"]["hours"].as_array().unwrap().is_empty());
    assert!(!tmp.path().join("state").exists());
}

#[tokio::test(flavor = "current_thread")]
async fn gamma_policy_is_explicit_in_the_actual_historical_entry_and_old_config() {
    use sim_system::public_consumption::GammaResolutionPolicyV1;
    for policy in [
        GammaResolutionPolicyV1::SourceTimeOnly,
        GammaResolutionPolicyV1::QualifiedPublicConsumption,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let (mut config, rules) = setup(temp.path());
        config.gamma_resolution_policy = policy;
        let wire = serde_json::to_value(&config).unwrap();
        assert_eq!(
            wire.get("gamma_resolution_policy").is_some(),
            !policy.is_source_time_only()
        );
        let restored: HistoricalRunConfigV1 = serde_json::from_value(wire).unwrap();
        assert_eq!(restored.gamma_resolution_policy, policy);
        assert_eq!(
            HistoricalPreparedRunConfigV1::from(&restored).gamma_resolution_policy,
            policy
        );
        let mut probe = Probe {
            no_trade_only: true,
            ..Default::default()
        };
        let report = run_historical_v1(&restored, rules, &mut probe)
            .await
            .unwrap();
        assert!(report.recording_drained);
        assert_eq!(report.end_reason, HistoricalEndReasonV1::RangeEnd);
        assert_eq!(
            report
                .last_dispatched_calculation_timing
                .as_ref()
                .unwrap()
                .actual_sent_at_utc_ms,
            None
        );
        assert!(report.failed_batch.is_none());
        let inputs = sim_log_values(&restored.sim_log);
        let policies = inputs
            .iter()
            .filter_map(|frame| match frame["kind"].as_str() {
                Some("ACCEPTED_INPUTS") => frame["body"].as_array(),
                Some("ACCEPTED_INPUTS_V3") => frame["body"]["inputs"].as_array(),
                _ => None,
            })
            .flatten()
            .filter(|input| input["input"] == "GAMMA_RESOLUTION_POLICY")
            .collect::<Vec<_>>();
        assert_eq!(policies.len(), usize::from(!policy.is_source_time_only()));
        if let Some(input) = policies.first() {
            assert_eq!(input["body"], "qualified_public_consumption");
        }
    }
}

#[test]
fn actual_day_initialization_statistics_keep_late_candidate_prefixes() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, rules) = setup(temp.path());
    config.preparation.scope.received_end_utc_ms = BASE + 15000;
    initialize_at(&mut config, BASE + 2000, &[FIRST]);
    let mut reference_config = config.preparation.clone();
    reference_config.work_root = temp.path().join("reference-prepared");
    let prepared = prepare_historical_run_v1(&reference_config).unwrap();
    let mut replay = data_pipeline_training::public_replay::PublicReplayV1::open(
        &prepared.public_root,
        1,
        1 << 20,
    )
    .unwrap();
    let (mut expected_groups, mut expected_initial, mut expected_dependencies) = (0, 0, 0);
    while let Some((_, applied)) = replay.next_applied().unwrap() {
        for group in applied.applied.closed_groups {
            expected_groups += 1;
            let initial = prepared
                .initialization
                .group_is_initialization(&group.member_input_positions);
            expected_initial += u64::from(initial);
            let dependency = prepared.initialization.group_has_initialization_dependency(
                &group.member_input_positions,
                group.closed_at_input_position,
            ) || group.observations.iter().any(|o| {
                prepared
                    .initialization
                    .observation_has_initialization_dependency(o)
            });
            expected_dependencies += u64::from(!initial && dependency);
        }
    }
    let mut fast = HistoricalPreparedRunConfigV1::from(&config);
    fast.session.run_id = "initial-fast".into();
    let mut late = fast.clone();
    late.session.run_id = "initial-late".into();
    late.session.account_id = "initial-late-account".into();
    late.timing.public_delivery_delay_ms = DelayScheduleV1::Fixed(2100);
    late.sim_log = temp.path().join("late-sim.jsonl");
    late.noe_root = temp.path().join("late-noe");
    late.ledger_root = temp.path().join("late-ledger");
    late.metadata_path = temp.path().join("late-metadata.json");
    late.result_path = temp.path().join("late-result.json");
    let mut candidates = vec![fast.clone(), late];
    for index in 2..7 {
        let mut candidate = fast.clone();
        candidate.session.run_id = format!("initial-late-{index}");
        candidate.session.account_id = format!("initial-account-{index}");
        candidate.timing.public_delivery_delay_ms = DelayScheduleV1::Fixed(2000 + index * 10);
        candidate.sim_log = temp.path().join(format!("sim-{index}"));
        candidate.noe_root = temp.path().join(format!("noe-{index}"));
        candidate.ledger_root = temp.path().join(format!("ledger-{index}"));
        candidate.metadata_path = temp.path().join(format!("metadata-{index}.json"));
        candidate.result_path = temp.path().join(format!("result-{index}.json"));
        candidates.push(candidate);
    }
    let day = HistoricalDayConfigV1 {
        preparation: config.preparation.clone(),
        candidates,
        progress_interval_seconds: 0,
        producer_lead_groups: Some(1),
    };
    let result = run_historical_days_v1(&[day], rules, &|_| {
        Ok(Probe {
            no_trade_only: true,
            ..Default::default()
        })
    })
    .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].candidates.len(), 7);
    for candidate in &result[0].candidates {
        let summary = &candidate.report["delivery_summary"];
        let before = &summary["before_operating_start"];
        let within = &summary["within_operating_range"];
        assert_eq!(
            before["groups"].as_u64().unwrap() + within["groups"].as_u64().unwrap(),
            expected_groups
        );
        assert_eq!(
            before["initialization_source_groups"].as_u64().unwrap()
                + within["initialization_source_groups"].as_u64().unwrap(),
            expected_initial
        );
        assert_eq!(
            before["research_groups_with_initialization_dependencies"]
                .as_u64()
                .unwrap()
                + within["research_groups_with_initialization_dependencies"]
                    .as_u64()
                    .unwrap(),
            expected_dependencies
        );
        assert!(
            within["research_groups_with_initialization_dependencies"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(candidate.report["end_reason"], "RangeEnd");
    }
    assert_eq!(
        result[0].candidates[0].report["delivery_summary"]["before_operating_start"]["groups"],
        1
    );
    assert_eq!(
        result[0].candidates[1].report["delivery_summary"]["before_operating_start"]["groups"],
        0
    );
}

#[test]
fn later_candidate_original_failure_survives_earlier_candidate_collateral_stop() {
    let tmp = tempfile::tempdir().unwrap();
    let (config, rules) = setup(tmp.path());
    let first = HistoricalPreparedRunConfigV1::from(&config);
    let mut second = first.clone();
    second.session.run_id = "later-root".into();
    second.session.account_id = "later-account".into();
    second.sim_log = tmp.path().join("root-sim.jsonl");
    second.noe_root = tmp.path().join("root-noe");
    second.ledger_root = tmp.path().join("root-ledger");
    second.metadata_path = tmp.path().join("root-metadata.json");
    second.result_path = tmp.path().join("root-result.json");
    let day = HistoricalDayConfigV1 {
        preparation: config.preparation.clone(),
        candidates: vec![first, second],
        progress_interval_seconds: 0,
        producer_lead_groups: Some(1),
    };
    let error = run_historical_days_v1(&[day], rules.clone(), &|c| {
        Ok(Probe {
            rules: Some(rules.clone()),
            fail: c.session.run_id == "later-root",
            no_trade_only: c.session.run_id != "later-root",
            ..Default::default()
        })
    })
    .unwrap_err();
    let failure = error.downcast::<HistoricalDaysFailureV1>().unwrap();
    assert!(
        matches!(&failure.source,DayFailureSourceV1::Candidate {index:1,run_id} if run_id=="later-root")
    );
    assert!(
        failure
            .cause
            .downcast_ref::<HistoricalRunFailureV1<&str>>()
            .is_some()
    );
    let candidates = &failure.days[0].candidates;
    assert_eq!(candidates.len(), 2);
    assert_eq!(candidates[1].outcome, CandidateOutcomeV1::OriginalFailure);
    assert_eq!(candidates[1].report["end_reason"], "Failure");
    assert!(matches!(
        candidates[0].outcome,
        CandidateOutcomeV1::CollateralStop | CandidateOutcomeV1::Complete
    ));
    assert!(candidates[0].report.get("observation_cutoff").is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn zero_send_cash_refusal_keeps_unsent_timing_and_continues_on_later_observations() {
    for source_health in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (mut config, rules) = setup(temp.path());
        config.session.initial_account_state.available_pusd = ExactDecimal("0".into());
        config.research_evidence_path = Some(temp.path().join("evidence.jsonl"));
        // 首组拒绝后先到未来重评，不让新公开输入形成另一决定替换旧调度。
        config.timing.calculation_duration_ms = DelayScheduleV1::Fixed(0);
        config.timing.dispatch_delay_ms = DelayScheduleV1::Fixed(0);
        let mut probe = Probe {
            rules: Some(rules.clone()),
            source_health,
            // 本探针只验证第一批资金拒绝，不假定第二市场已存在成功成交。
            bought_second: true,
            first_next_evaluation: Some(BASE + 1250),
            calculation_budget: Some(40),
            ..Default::default()
        };
        let report = run_historical_v1(&config, rules, &mut probe).await.unwrap();
        assert_eq!(report.end_reason, HistoricalEndReasonV1::RangeEnd);
        assert_eq!(report.local_funds_rejected_batches, 1);
        let rows = std::fs::read_to_string(config.research_evidence_path.as_ref().unwrap())
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            rows.iter()
                .filter(|r| r["record_type"] == "local_funds_zero_submission")
                .count(),
            1
        );
        let calculation = rows
            .iter()
            .find(|r| {
                r["record_type"] == "calculation"
                    && r["intents"].as_array().is_some_and(|i| !i.is_empty())
            })
            .unwrap();
        assert!(calculation["intents"][0]["context"]["global_request_id"].is_string());
        assert_eq!(
            probe.failed_callback_sent,
            Some(0),
            "原dispatched通知保留零发送结果"
        );
        assert!(probe.observed.len() > 2);
        assert!(
            probe.observed.iter().any(|(at, ..)| *at == BASE + 1250),
            "无新公开行情时仍执行原严格未来重评，不重发已拒绝意图"
        );
        assert!(report.failed_batch.is_none());
        assert_eq!(report.unclosed_commitments, 0);
        assert!(report.recording_drained);
        assert!(
            report
                .last_dispatched_calculation_timing
                .is_some_and(|t| t.actual_sent_at_utc_ms.is_none())
        );
    }
}

#[test]
fn completed_business_report_survives_result_file_failure_without_completion_reuse() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, rules) = setup(temp.path());
    config.preparation.scope.received_end_utc_ms = BASE + 3_600_000;
    std::fs::create_dir(&config.result_path).unwrap();
    let state = temp.path().join("failed-output-state");
    let day = HistoricalDayConfigV1 {
        preparation: config.preparation.clone(),
        candidates: vec![HistoricalPreparedRunConfigV1::from(&config)],
        progress_interval_seconds: 0,
        producer_lead_groups: Some(1),
    };
    let error = run_historical_days_with_options_v1(
        &[day],
        rules,
        &|_| {
            Ok(Probe {
                no_trade_only: true,
                ..Default::default()
            })
        },
        1,
        &HistoricalDayRunOptionsV1 {
            research_summary: true,
            ..Default::default()
        },
    )
    .unwrap_err();
    let failure = error.downcast_ref::<HistoricalDaysFailureV1>().unwrap();
    let original = failure
        .cause
        .downcast_ref::<HistoricalRunFailureV1<&'static str>>()
        .unwrap();
    assert!(
        matches!(&original.cause, HistoricalFailureCauseV1::Material(error) if error.downcast_ref::<std::io::Error>().is_some_and(|error| error.kind() == std::io::ErrorKind::IsADirectory))
    );
    let candidate = &failure.days[0].candidates[0];
    assert_eq!(candidate.outcome, CandidateOutcomeV1::OriginalFailure);
    let report = &candidate.report;
    assert_eq!(report["end_reason"], "RangeEnd");
    assert_eq!(report["recording_drained"], true);
    assert_eq!(report["last_observed_at_utc_ms"], BASE + 3_600_000 - 1);
    assert!(report["account_economics_memory"].is_object());
    assert!(report["research"]["final_snapshot"].is_object());
    assert_eq!(report["research"]["hours"].as_array().unwrap().len(), 1);
    assert!(config.result_path.is_dir());
    assert!(!state.join("checkpoint.json").exists());
}

#[derive(Debug)]
struct FailingResearchObserver;
impl HistoricalPublicObserverV1 for FailingResearchObserver {
    fn requirements(&self) -> PublicObservationRequirementsV1 {
        PublicObservationRequirementsV1::FULL
    }
    fn observe(
        &self,
        _: &PublicGroupReferenceV1,
        _: &ClosedBusinessGroupV1,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError> {
        Err("fixture required public output failure".into())
    }
}
#[test]
fn required_public_output_failure_returns_day_and_candidate_partial_status() {
    let temp = tempfile::tempdir().unwrap();
    let (config, rules) = setup(temp.path());
    let day = HistoricalDayConfigV1 {
        preparation: config.preparation.clone(),
        candidates: vec![HistoricalPreparedRunConfigV1::from(&config)],
        progress_interval_seconds: 0,
        producer_lead_groups: None,
    };
    let result = run_historical_days_with_options_v1(
        &[day],
        rules.clone(),
        &|_| {
            Ok(Probe {
                rules: Some(rules.clone()),
                ..Default::default()
            })
        },
        1,
        &HistoricalDayRunOptionsV1 {
            public_observer: Some(Arc::new(FailingResearchObserver)),
            ..Default::default()
        },
    );
    let failure = result.unwrap_err();
    let failure = failure.downcast_ref::<HistoricalDaysFailureV1>().unwrap();
    assert_eq!(failure.source, DayFailureSourceV1::Producer);
    assert_eq!(failure.days.len(), 1);
    assert_eq!(
        failure.days[0].candidates[0].outcome,
        CandidateOutcomeV1::CollateralStop
    );
    assert_eq!(
        failure.days[0].candidates[0].report["end_reason"],
        "Failure"
    );
    assert!(
        failure
            .cause
            .to_string()
            .contains("required public output failure")
    );
}

#[derive(Debug, Default)]
struct EarlyExitFinishFailure {
    finish_calls: std::sync::atomic::AtomicUsize,
    saw_later_market: std::sync::atomic::AtomicBool,
}
impl HistoricalPublicObserverV1 for EarlyExitFinishFailure {
    fn requirements(&self) -> PublicObservationRequirementsV1 {
        PublicObservationRequirementsV1::FULL
    }
    fn observe(
        &self,
        _: &PublicGroupReferenceV1,
        group: &ClosedBusinessGroupV1,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError> {
        if group
            .observations
            .iter()
            .any(|o| o.market.condition_id.0 == SECOND)
        {
            self.saw_later_market
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(Arc::new(HistoricalPublicEvidenceV1 {
            scalars: serde_json::Value::Null,
            request_evidence: Default::default(),
        }))
    }
    fn finish(&self) -> Result<(), MaterialError> {
        self.finish_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err("fixture required early-exit public finish failure".into())
    }
}
#[test]
fn early_strategy_exit_before_eof_propagates_required_public_finish_failure() {
    let temp = tempfile::tempdir().unwrap();
    let (config, rules) = setup(temp.path());
    let observer = Arc::new(EarlyExitFinishFailure::default());
    let day = HistoricalDayConfigV1 {
        preparation: config.preparation.clone(),
        candidates: vec![HistoricalPreparedRunConfigV1::from(&config)],
        progress_interval_seconds: 0,
        producer_lead_groups: Some(0),
    };
    let failure = run_historical_days_with_options_v1(
        &[day],
        rules.clone(),
        &|_| {
            Ok(Probe {
                exit_immediately: true,
                rules: Some(rules.clone()),
                ..Default::default()
            })
        },
        1,
        &HistoricalDayRunOptionsV1 {
            public_observer: Some(observer.clone()),
            ..Default::default()
        },
    )
    .unwrap_err();
    let failure = failure.downcast_ref::<HistoricalDaysFailureV1>().unwrap();
    assert_eq!(failure.source, DayFailureSourceV1::Producer);
    assert!(
        failure
            .cause
            .to_string()
            .contains("early-exit public finish failure")
    );
    assert_eq!(
        observer
            .finish_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert!(
        !observer
            .saw_later_market
            .load(std::sync::atomic::Ordering::SeqCst),
        "不得为flush继续读取后续市场"
    );
    // 此夹具包含2条metadata和6条book，后续市场仍未消费，未到EOF。
    assert!(failure.days[0].public_applied_inputs < 8);
    let report = &failure.days[0].candidates[0].report;
    assert_eq!(report["end_reason"], "StrategyExit");
    assert!(report["last_observed_at_utc_ms"].as_i64().unwrap() < BASE + 6000);
}

#[test]
fn explicit_finite_host_consumes_complete_tail_groups_but_excludes_late_delivery() {
    struct TailProbe(Arc<std::sync::Mutex<Vec<(i64, Vec<bool>)>>>);
    impl StrategyV1 for TailProbe {
        type Error = &'static str;
        type Diagnostics = ();
        fn decide(
            &mut self,
            input: &StrategyInputV1,
            at: i64,
        ) -> Result<StrategyOutputV1, Self::Error> {
            for m in &input.markets {
                self.0.lock().unwrap().push((
                    at,
                    m.observation
                        .assets
                        .iter()
                        .map(|a| a.asks.value.is_some())
                        .collect(),
                ));
            }
            Ok(StrategyOutputV1::Continue {
                intents: vec![],
                next_evaluation_at_utc_ms: None,
                diagnostics: (),
            })
        }
    }
    for (end_offset, visible) in [(1201, false), (1202, true)] {
        let temp = tempfile::tempdir().unwrap();
        let (mut config, rules) = setup(temp.path());
        // 正式两条book都于1100接收、1101接纳，100ms交付后同为1201。
        // cutoff=1201排除二者；cutoff=1202必须完整消费二者，不能读半侧。
        config.preparation.scope.received_end_utc_ms = BASE + end_offset;
        config.session.caller_role = SimCallerRoleV1::ModelTrain;
        config.session.input_lifecycle_policy =
            sim_system::public_consumption::InputLifecyclePolicyV1::FiniteInput;
        config.timing.calculation_duration_ms = DelayScheduleV1::Fixed(0);
        config.timing.dispatch_delay_ms = DelayScheduleV1::Fixed(0);
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let day = research_day(&config, temp.path());
        let result =
            run_historical_days_v1(&[day], rules, &|_| Ok(TailProbe(seen.clone()))).unwrap();
        assert_eq!(result[0].candidates[0].report["end_reason"], "RangeEnd");
        let seen = seen.lock().unwrap();
        assert!(seen.iter().all(|(at, _)| *at < BASE + end_offset));
        assert_eq!(
            seen.iter().any(|(_, sides)| sides.iter().any(|v| *v)),
            visible
        );
        assert!(
            seen.iter()
                .all(|(_, sides)| !sides.iter().any(|v| *v) || sides.iter().all(|v| *v)),
            "同毫秒尾部不可读半侧"
        );
    }
}

fn fifo_model_train_setup(
    root: &std::path::Path,
) -> (HistoricalRunConfigV1, Arc<SealedProcessRuleSnapshotV3>) {
    let (mut config, original) = setup(root);
    config.public_statistics = Some(PublicStatisticsConfigV1 {
        momentum_windows_ms: vec![100, 1000],
        spot_variance_windows_ms: vec![1000],
        terminal_integral_windows_ms: vec![1000],
    });
    config.session.caller_role = SimCallerRoleV1::ModelTrain;
    config.session.input_lifecycle_policy =
        sim_system::public_consumption::InputLifecyclePolicyV1::FiniteInput;
    let rules = derive_public_maker_snapshot_v3(
        HistoricalProcessSnapshotRequestV1 {
            snapshot_path: original.path().to_path_buf(),
            expected_file_sha256: original.file_sha256().clone(),
            replay_causal_start_utc_ms: i64::MAX,
        },
        root.join("fifo-rules.json"),
        config.implementation_identity.clone(),
        &[SimCallerRoleV1::ModelTrain],
    )
    .unwrap();
    (config, rules)
}
#[derive(Debug)]
struct FifoRead {
    market_at: i64,
    earliest_start: i64,
    started_at: i64,
    up_ask: Option<String>,
    applied_trades: usize,
    available: ExactDecimal,
    commitments: usize,
}
struct FifoProbe {
    inner: Probe,
    calls: Vec<FifoRead>,
}
impl StrategyV1 for FifoProbe {
    type Error = &'static str;
    type Diagnostics = ();
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1, Self::Error> {
        let frame = input.market_frame_timing.as_ref().expect("实际FIFO帧");
        let market_at = frame.market_cutoff.decision_time().logical_time_utc_ms();
        assert_eq!(
            input
                .action_before
                .cutoff()
                .decision_time()
                .logical_time_utc_ms(),
            at
        );
        assert!(at >= frame.earliest_start_at_utc_ms);
        for market in &input.markets {
            assert!(
                market.statistics.is_some(),
                "实际新宿主显式接入共同统计，不能以None通过"
            );
            if let Some(stats) = &market.statistics {
                assert_eq!(stats.definition_version, PUBLIC_STATISTICS_VERSION_V1);
                assert_eq!(&stats.group, frame.market_cutoff.receipt().group());
                assert_eq!(stats.config.momentum_windows_ms, vec![100, 1000]);
                assert_eq!(stats.momentum.len(), 2);
                assert_eq!(stats.spot_variance.len(), 1);
                assert_eq!(stats.spot_integrals.len(), 1);
                assert_eq!(stats.source_applied_at_utc_ms.len(), 5);
                // 本合法材料无Binance/RTDS；F1—F8缺失不能在网格上补零或造消息。
                assert!(stats.depth.best_bid_usdt_per_btc.value.is_none());
                assert!(stats.latest_trade_pressure.value.is_none());
                assert!(
                    stats
                        .momentum
                        .iter()
                        .all(|m| m.change_usdt_per_btc.value.is_none())
                );
                if market_at == BASE + 1300 && market.observation.market.condition_id.0 == FIRST {
                    assert!(
                        stats
                            .pm_quantities
                            .iter()
                            .any(|q| q.asset.token_id.0 == "20"
                                && q.ask_shares.value == Some(Decimal::from(100)))
                    );
                }
                assert_eq!(
                    stats.observed_at_utc_ms, market_at,
                    "静默网格读取原共同统计，非新行情采样"
                );
            }
        }
        self.calls.push(FifoRead {
            market_at,
            earliest_start: frame.earliest_start_at_utc_ms,
            started_at: at,
            up_ask: input
                .markets
                .iter()
                .find(|m| m.observation.market.condition_id.0 == FIRST)
                .and_then(|m| {
                    m.observation
                        .assets
                        .iter()
                        .find(|a| a.asset.token_id.0 == "20")
                })
                .and_then(|a| a.asks.value.as_ref())
                .and_then(|v| v.first())
                .map(|l| l.price_pusd_per_share.0.clone()),
            applied_trades: input
                .trade_economics
                .iter()
                .filter(|t| t.economic_applied)
                .count(),
            available: input
                .action_before
                .economics()
                .cash
                .actual_available_pusd
                .clone(),
            commitments: input.action_before.commitments().count(),
        });
        self.inner.decide(input, at)
    }
    fn dispatched(
        &mut self,
        c: &StrategyCalculationV1,
        r: &Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1>,
    ) {
        self.inner.dispatched(c, r)
    }
}
#[tokio::test(flavor = "current_thread")]
async fn fifo_grid_keeps_silent_frames_without_extra_memory_calls_or_bridge_requests() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, rules) = fifo_model_train_setup(temp.path());
    config.preparation.scope.received_end_utc_ms = BASE + 3000;
    config.timing.decision_mode = HistoricalDecisionModeV1::Fixed100msFifo;
    config.timing.calculation_duration_ms = DelayScheduleV1::Fixed(0);
    config.timing.dispatch_delay_ms = DelayScheduleV1::Fixed(0);
    let mut probe = FifoProbe {
        inner: Probe {
            no_trade_only: true,
            ..Default::default()
        },
        calls: vec![],
    };
    let report = run_historical_v1(&config, rules, &mut probe).await.unwrap();
    // metadata 在1ms合法接纳，加100ms交付，首个合法网格为200ms。
    let expected = (200..3000)
        .step_by(100)
        .map(|t| BASE + t)
        .collect::<Vec<_>>();
    assert_eq!(
        probe.calls.iter().map(|c| c.market_at).collect::<Vec<_>>(),
        expected
    );
    assert!(
        probe
            .calls
            .iter()
            .all(|c| c.started_at == c.market_at && c.earliest_start == c.market_at)
    );
    let fifo = report.fifo.unwrap();
    assert_eq!(fifo.grid_opportunities, 30);
    assert_eq!(fifo.grid_without_market_frame, 2);
    assert_eq!(fifo.generated_market_frames, 28);
    assert_eq!(fifo.started_market_frames, 28);
    assert_eq!(fifo.queued_market_frames, 0);
    assert_eq!(
        probe.inner.dispatched, 28,
        "只有显式空动作才回调，空档没有模型调用"
    );
    assert_eq!(report.calculation_count, 28);
    assert_eq!(report.unclosed_commitments, 0);
    assert!(!report.pending_calculation);
}
#[tokio::test(flavor = "current_thread")]
async fn fifo_burst_preserves_old_market_and_reads_actual_sim_account_before_catching_up() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, rules) = fifo_model_train_setup(temp.path());
    config.preparation.scope.received_end_utc_ms = BASE + 6000;
    config.timing.decision_mode = HistoricalDecisionModeV1::Fixed100msFifo;
    // 首帧200ms；第13帧1400ms占用原槽到1400+900+100=2400ms。
    let mut durations = vec![0; 60];
    durations[12] = 900;
    let mut dispatches = vec![0; 60];
    dispatches[12] = 100;
    config.timing.calculation_duration_ms = DelayScheduleV1::Sequence(durations);
    config.timing.dispatch_delay_ms = DelayScheduleV1::Sequence(dispatches);
    let mut probe = FifoProbe {
        inner: Probe {
            rules: Some(rules.clone()),
            ..Default::default()
        },
        calls: vec![],
    };
    let report = run_historical_v1(&config, rules, &mut probe).await.unwrap();
    let expected = (200..6000)
        .step_by(100)
        .map(|t| BASE + t)
        .collect::<Vec<_>>();
    assert_eq!(
        probe.calls.iter().map(|c| c.market_at).collect::<Vec<_>>(),
        expected
    );
    let delayed = probe
        .calls
        .iter()
        .find(|c| c.market_at == BASE + 1500)
        .unwrap();
    assert_eq!(delayed.started_at, BASE + 2400);
    assert_eq!(
        delayed.up_ask.as_deref(),
        Some("0.6"),
        "实际2301ms已交付0.5，旧帧仍是0.6"
    );
    assert_eq!(delayed.earliest_start, BASE + 1500);
    assert!(
        delayed.applied_trades > 0,
        "等待期间原单成交已合法交付，不能被冻结在1500ms"
    );
    assert!(Decimal::from_str_exact(&delayed.available.0).unwrap() < Decimal::from(2000));
    assert!(delayed.commitments > 0, "原旧单/在途义务继续存在");
    assert_eq!(
        probe.inner.frozen_down_price,
        Some(Decimal::new(45, 2)),
        "原完整动作不按后来0.65改价"
    );
    let fifo = report.fifo.unwrap();
    assert_eq!(fifo.generated_market_frames, 58);
    assert_eq!(fifo.started_market_frames, 58);
    assert_eq!(fifo.peak_queued_market_frames, 10);
    assert_eq!(fifo.queued_market_frames, 0);
    assert_eq!(report.calculation_count, probe.calls.len());
}
#[tokio::test(flavor = "current_thread")]
async fn fifo_finite_tail_reports_waiting_frames_and_pending_action_without_draining() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, rules) = fifo_model_train_setup(temp.path());
    config.preparation.scope.received_end_utc_ms = BASE + 3000;
    config.timing.decision_mode = HistoricalDecisionModeV1::Fixed100msFifo;
    config.timing.calculation_start_delay_ms = DelayScheduleV1::Fixed(50);
    config.timing.calculation_duration_ms = DelayScheduleV1::Fixed(5000);
    config.timing.dispatch_delay_ms = DelayScheduleV1::Fixed(0);
    let mut probe = FifoProbe {
        inner: Probe {
            no_trade_only: true,
            ..Default::default()
        },
        calls: vec![],
    };
    let report = run_historical_v1(&config, rules, &mut probe).await.unwrap();
    assert_eq!(probe.calls.len(), 1);
    assert_eq!(probe.calls[0].market_at, BASE + 200);
    assert_eq!(probe.calls[0].earliest_start, BASE + 250);
    assert_eq!(probe.calls[0].started_at, BASE + 250);
    assert_eq!(probe.inner.dispatched, 0, "未完成计算不能当显式NoTrade派发");
    let fifo = report.fifo.unwrap();
    assert_eq!(fifo.generated_market_frames, 28);
    assert_eq!(fifo.started_market_frames, 1);
    assert_eq!(fifo.queued_market_frames, 27);
    assert_eq!(fifo.peak_queued_market_frames, 27);
    assert_eq!(
        fifo.queued_frame_timings[0]
            .market_cutoff
            .decision_time()
            .logical_time_utc_ms(),
        BASE + 300
    );
    assert!(report.pending_calculation);
    assert_eq!(
        report
            .pending_calculation_timing
            .unwrap()
            .calculation_completed_at_utc_ms,
        BASE + 5250
    );
    assert_eq!(report.last_observed_at_utc_ms, Some(BASE + 2999));
    assert_eq!(report.end_reason, HistoricalEndReasonV1::RangeEnd);
}

struct FifoSharedStatisticsProbe {
    captured: bool,
    shared: Arc<std::sync::Mutex<Vec<(Arc<PublicMarketStatisticsV1>, ExactDecimal)>>>,
}
impl StrategyV1 for FifoSharedStatisticsProbe {
    type Error = &'static str;
    type Diagnostics = ();
    fn decide(&mut self, input: &StrategyInputV1, _: i64) -> Result<StrategyOutputV1, Self::Error> {
        if !self.captured {
            self.captured = true;
            self.shared.lock().unwrap().push((
                input.markets[0].statistics.as_ref().unwrap().clone(),
                input
                    .action_before
                    .economics()
                    .cash
                    .actual_available_pusd
                    .clone(),
            ));
        }
        Ok(StrategyOutputV1::Continue {
            intents: vec![],
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        })
    }
}
#[test]
fn fifo_day_shares_original_statistics_history_with_independent_accounts_and_processing_delays() {
    let temp = tempfile::tempdir().unwrap();
    let (config, rules) = fifo_model_train_setup(temp.path());
    let mut first = HistoricalPreparedRunConfigV1::from(&config);
    first.timing.decision_mode = HistoricalDecisionModeV1::Fixed100msFifo;
    first.timing.calculation_duration_ms = DelayScheduleV1::Fixed(0);
    first.timing.dispatch_delay_ms = DelayScheduleV1::Fixed(0);
    first.finite_in_flight_groups = 0;
    let mut second = first.clone();
    second.session.run_id = "fifo-independent".into();
    second.session.account_id = "fifo-independent-account".into();
    second.session.initial_account_state.available_pusd = ExactDecimal("3000".into());
    second.timing.calculation_duration_ms = DelayScheduleV1::Fixed(200);
    second.sim_log = temp.path().join("second-sim");
    second.noe_root = temp.path().join("second-noe");
    second.ledger_root = temp.path().join("second-ledger");
    second.metadata_path = temp.path().join("second-metadata.json");
    second.result_path = temp.path().join("second-result.json");
    let shared = Arc::new(std::sync::Mutex::new(Vec::new()));
    let day = HistoricalDayConfigV1 {
        preparation: config.preparation,
        candidates: vec![first, second],
        progress_interval_seconds: 0,
        producer_lead_groups: Some(1),
    };
    let result = run_historical_days_v1(&[day], rules, &|_| {
        Ok(FifoSharedStatisticsProbe {
            captured: false,
            shared: shared.clone(),
        })
    })
    .unwrap();
    let observations = shared.lock().unwrap();
    assert_eq!(observations.len(), 2);
    assert!(
        Arc::ptr_eq(&observations[0].0.identity, &observations[1].0.identity)
            && Arc::ptr_eq(&observations[0].0.config, &observations[1].0.config),
        "同输入/交付/参数使用同一个原统计流，不按账户另建历史"
    );
    assert_eq!(
        observations[0].0, observations[1].0,
        "同一帧统计值相同；不要求有界末值缓存永久持有所有旧查询结果"
    );
    assert_ne!(
        observations[0].1, observations[1].1,
        "公共相同，账户真实可用现金独立"
    );
    assert_eq!(
        result[0].candidates[0].report["fifo"]["queued_market_frames"],
        0
    );
    assert!(
        result[0].candidates[1].report["fifo"]["queued_market_frames"]
            .as_u64()
            .unwrap()
            > 0
    );
}

// E/H is a model consumer's declared cutoff; legacy strategies keep their original end.
struct ActiveWindowProbe {
    inner: FifoProbe,
    end: i64,
}
impl StrategyV1 for ActiveWindowProbe {
    type Error = &'static str;
    type Diagnostics = ();
    fn active_until_utc_ms(&self) -> Option<i64> {
        Some(self.end)
    }
    fn requires_learning_economics(&self) -> bool {
        true
    }
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1, Self::Error> {
        assert!(at < self.end);
        self.inner.decide(input, at)
    }
    fn dispatched(
        &mut self,
        c: &StrategyCalculationV1,
        r: &Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1>,
    ) {
        self.inner.dispatched(c, r)
    }
}
#[tokio::test(flavor = "current_thread")]
async fn learning_eh_preserves_cross_e_calculation_without_tail_actor_or_dispatch() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, rules) = fifo_model_train_setup(temp.path());
    config.preparation.scope.received_end_utc_ms = BASE + 3000;
    config.timing.decision_mode = HistoricalDecisionModeV1::Fixed100msFifo;
    config.timing.calculation_start_delay_ms = DelayScheduleV1::Fixed(50);
    config.timing.calculation_duration_ms = DelayScheduleV1::Fixed(1000);
    config.timing.dispatch_delay_ms = DelayScheduleV1::Fixed(0);
    let mut p = ActiveWindowProbe {
        inner: FifoProbe {
            inner: Probe {
                no_trade_only: true,
                ..Default::default()
            },
            calls: vec![],
        },
        end: BASE + 500,
    };
    let r = run_historical_v1(&config, rules, &mut p).await.unwrap();
    assert_eq!(p.inner.calls.len(), 1);
    assert_eq!(p.inner.inner.dispatched, 0);
    assert!(r.pending_calculation);
    assert_eq!(
        r.pending_calculation_timing
            .unwrap()
            .calculation_completed_at_utc_ms,
        BASE + 1250
    );
    assert_eq!(r.fifo.as_ref().unwrap().generated_market_frames, 3);
    assert_eq!(r.fifo.as_ref().unwrap().queued_market_frames, 2);
    assert_eq!(r.last_observed_at_utc_ms, Some(BASE + 2999));
    assert!(r.learning_account_start.is_some());
    assert!(r.learning_account_memory.is_some());
}
#[tokio::test(flavor = "current_thread")]
async fn learning_e_later_than_h_is_an_error_and_never_silently_shortened() {
    let temp = tempfile::tempdir().unwrap();
    let (config, rules) = fifo_model_train_setup(temp.path());
    let end = config.preparation.scope.received_end_utc_ms + 1;
    let mut p = ActiveWindowProbe {
        inner: FifoProbe {
            inner: Probe::default(),
            calls: vec![],
        },
        end,
    };
    assert!(run_historical_v1(&config, rules, &mut p).await.is_err());
    assert!(p.inner.calls.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn learning_fixture_schedule_has_eight_calls_and_a_frozen_e_crossing() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, rules) = fifo_model_train_setup(temp.path());
    config.preparation.scope.received_end_utc_ms = BASE + 3000;
    config.timing.decision_mode = HistoricalDecisionModeV1::Fixed100msFifo;
    config.timing.calculation_start_delay_ms = DelayScheduleV1::Fixed(10);
    config.timing.calculation_duration_ms =
        DelayScheduleV1::Sequence(vec![200, 20, 20, 20, 20, 20, 20, 400]);
    config.timing.dispatch_delay_ms = DelayScheduleV1::Fixed(30);
    let mut p = ActiveWindowProbe {
        inner: FifoProbe {
            inner: Probe {
                no_trade_only: true,
                ..Default::default()
            },
            calls: vec![],
        },
        end: BASE + 1000,
    };
    let r = run_historical_v1(&config, rules, &mut p).await.unwrap();
    assert_eq!(
        p.inner.calls.len(),
        8,
        "已批准模型验证工作量由原宿主时序决定"
    );
    assert!(r.pending_calculation);
    assert!(
        r.pending_calculation_timing
            .unwrap()
            .scheduled_send_at_utc_ms
            >= BASE + 1000
    );
    assert!(p.inner.calls.iter().all(|c| c.started_at < BASE + 1000));
}
