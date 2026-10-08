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
    last_economics: Option<AccountEconomicsReadV2>,
    first_economics: Option<AccountEconomicsReadV2>,
    dispatched: usize,
    failed_callback_sent: Option<usize>,
    calculation_budget: Option<usize>,
}
impl StrategyV1 for Probe {
    type Error = &'static str;
    type Diagnostics = ();
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1, Self::Error> {
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
            timing.actual_sent_at_utc_ms.unwrap()
        );
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
    assert_eq!(depth.last_update_id, 102);
    assert!(
        depth
            .bids
            .iter()
            .any(|l| l.price_usdt_per_btc.0 == "64100" && l.quantity_btc.0 == "0.3")
    );
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
            &description
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
                &material
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
        _: &StrategyCurrentReadV1<'_>,
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
        _: &StrategyCurrentReadV1<'_>,
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
        _: &StrategyCurrentReadV1<'_>,
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
        "last_decision_to_dispatch_ms": timing.actual_sent_at_utc_ms.unwrap() - timing.calculation_started_at_utc_ms,
        "purpose": "合成工程开销；不是盈利、历史静默事故或实际在线性能证据" })
    );
}
