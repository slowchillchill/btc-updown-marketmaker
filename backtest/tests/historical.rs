mod common;
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
    dispatched: usize,
    failed_callback_sent: Option<usize>,
}
impl StrategyV1 for Probe {
    type Error = &'static str;
    type Diagnostics = ();
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1, Self::Error> {
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
    let mut probe = Probe {
        rules: Some(rules.clone()),
        cancel_first: true,
        ..Default::default()
    };
    let report = run_historical_v1(&config, rules, &mut probe).await.unwrap();
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
            .actual_sent_at_utc_ms
            .unwrap()
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
    assert!(probe.observed.iter().all(|(at, _, _, _)| *at < BASE + 1700));
    assert_eq!(probe.dispatched, 1);
}
#[tokio::test(flavor = "current_thread")]
async fn strategy_and_batch_failures_preserve_original_errors_and_stop_without_retry() {
    for bad_batch in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (config, rules) = setup(temp.path());
        let mut probe = Probe {
            fail: !bad_batch,
            bad_batch,
            ..Default::default()
        };
        let failure = run_historical_v1(&config, rules, &mut probe)
            .await
            .unwrap_err();
        if bad_batch {
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
        assert_eq!(
            failure.report.unwrap().end_reason,
            HistoricalEndReasonV1::Failure
        );
        assert!(!config.result_path.exists());
    }
}
