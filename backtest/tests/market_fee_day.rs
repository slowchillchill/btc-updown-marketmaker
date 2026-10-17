#[allow(dead_code)]
mod common;
use backtest::*;
use common::*;
use contracts::*;
use data_collect::{
    polymarket_orderbook_v3::{POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION, PolymarketOrderbookV3Event},
    writer::TypedParquetSegmentWriter,
};
use ledger::*;
use rust_decimal::Decimal;
use shared_run::*;
use sim_system::execution::*;
use std::sync::{Arc, Mutex};

#[derive(Default, Debug)]
struct Trace {
    first_sent: bool,
    second_proposed: bool,
    second_calculation_cash: Option<Decimal>,
    first_calculation_at: Option<i64>,
    first_platform_arrived_at: Option<i64>,
    second_calculation_at: Option<i64>,
    rejected: usize,
    observations_after_rejection: usize,
    sent: usize,
}
struct FeeProbe {
    health: bool,
    trace: Arc<Mutex<Trace>>,
}
impl StrategyV1 for FeeProbe {
    type Error = &'static str;
    type Diagnostics = ();
    fn uses_source_health(&self) -> bool {
        self.health
    }
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1, Self::Error> {
        let mut t = self.trace.lock().unwrap();
        if t.rejected > 0 {
            t.observations_after_rejection += 1;
        }
        let has_book = input.markets.iter().any(|m| {
            m.observation.market.condition_id.0 == FIRST
                && m.observation.assets.iter().any(|a| {
                    a.asset.token_id.0 == "20"
                        && a.asks.value.as_ref().is_some_and(|l| !l.is_empty())
                })
        });
        let item = if has_book && !t.first_sent {
            t.first_calculation_at = Some(at);
            Some(item(
                "fee-market",
                "90",
                "0.63",
                BuyOrderIntentV1::FakMarket {
                    spend_pusd: Decimal::from(90),
                    worst_price_pusd_per_share: Decimal::new(9, 1),
                },
            ))
        } else if t.first_sent
            && !t.second_proposed
            && input.action_before.cash_available() >= Decimal::from(9)
        {
            t.second_proposed = true;
            t.second_calculation_cash = Some(input.action_before.cash_available());
            t.second_calculation_at = Some(at);
            Some(item(
                "later-cash",
                "9",
                "0",
                BuyOrderIntentV1::GtcLimit {
                    limit_price_pusd_per_share: Decimal::new(3, 1),
                    size_shares: Decimal::from(30),
                    post_only: true,
                },
            ))
        } else {
            None
        };
        Ok(StrategyOutputV1::Continue {
            intents: item.into_iter().collect(),
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        })
    }
    fn dispatched(
        &mut self,
        _: &StrategyCalculationV1,
        result: &Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1>,
    ) {
        let mut t = self.trace.lock().unwrap();
        match result {
            Ok(sent) => {
                if let Some(first) = sent
                    .iter()
                    .find(|r| r.context.global_request_id == "fee-market")
                {
                    t.first_platform_arrived_at = Some(first.receipt.platform_arrived_at_utc_ms());
                }
                t.sent += sent.len();
                t.first_sent |= sent
                    .iter()
                    .any(|r| r.context.global_request_id == "fee-market");
            }
            Err(e) => {
                assert!(
                    e.is_local_funds_rejection(),
                    "unexpected dispatch failure: {e:?}"
                );
                t.rejected += 1;
            }
        }
    }
}
fn item(id: &str, principal: &str, fee: &str, order: BuyOrderIntentV1) -> ReservedIntentV1 {
    ReservedIntentV1 {
        context: RequestContextV2 {
            global_request_id: id.into(),
            caller_correlation_id: None,
        },
        intent: Arc::new(ModelIntentV1::SubmitSingleBuy {
            condition_id: ConditionId(FIRST.into()),
            order: BuyLegIntentV1 {
                token_id: TokenId("20".into()),
                order,
            },
        }),
        reserves: vec![IntentReserveV1 {
            batch_item_ordinal: 0,
            reserve: ReserveIntentV1 {
                request_identity: LedgerRequestIdentityV1(format!("{id}-reserve")),
                condition_id: ConditionId(FIRST.into()),
                requested_principal_pusd: ExactDecimal(principal.into()),
                immediate_fee_reserve_pusd: ExactDecimal(fee.into()),
            },
        }],
    }
}
fn fee_books(config: &HistoricalRunConfigV1) {
    let mut books = vec![
        book(FIRST, "20", BASE + 1100, BASE + 1100, 1, "0.9"),
        book(FIRST, "3", BASE + 1100, BASE + 1100, 2, "0.9"),
        book(FIRST, "20", BASE + 1300, BASE + 1300, 3, "0.1"),
        book(FIRST, "20", BASE + 2200, BASE + 2200, 4, "0.1"),
        // 独立公开轮先领取已到达的首单费用反馈；不与第二批发送同毫秒。
        book(FIRST, "20", BASE + 2700, BASE + 2700, 5, "0.1"),
    ];
    for b in &mut books {
        if let PolymarketOrderbookV3Event::Book(b) = &mut b.event {
            b.bids[0].price = "0.05".into();
            b.asks[0].size = "1000".into();
        }
    }
    let path = &config.preparation.scope.manifest_paths[0];
    let day = path.parent().unwrap();
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        day,
        "fee-books",
        POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
        "polymarket_market",
    )
    .unwrap();
    writer
        .write_polymarket_orderbook_v3_messages(&books, &mut Default::default())
        .unwrap();
    writer.append_key_value_metadata("data_collect_daily_file_fact_v1",serde_json::json!({"source_kind":"polymarket_market","schema_version":POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,"utc_hour":"2026-08-24T00","part":0,"rows":books.len()}).to_string()).unwrap();
    let f = writer.finalize().unwrap();
    let files = manifest["files"].as_array_mut().unwrap();
    files.retain(|f| f["source_kind"] != "polymarket_market");
    files.push(serde_json::json!({"path":f.final_path.file_name().unwrap().to_str().unwrap(),"source_kind":"polymarket_market","schema_version":POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,"utc_hour":"2026-08-24T00","part":0,"rows":f.row_count,"bytes":std::fs::metadata(&f.final_path).unwrap().len(),"sha256":f.sha256}));
    std::fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
}
#[test]
fn day_v2_actual_fee_changes_cash_then_local_funds_stop_only_batch_with_or_without_health() {
    for health in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (mut config, rules) = setup(temp.path());
        config.session.initial_account_state.available_pusd = ExactDecimal("100".into());
        config.timing.public_delivery_delay_ms = DelayScheduleV1::Fixed(0);
        config.timing.calculation_duration_ms = DelayScheduleV1::Fixed(1000);
        config.timing.dispatch_delay_ms = DelayScheduleV1::Fixed(0);
        fee_books(&config);
        let day = HistoricalDayConfigV1 {
            preparation: config.preparation.clone(),
            candidates: vec![HistoricalPreparedRunConfigV1::from(&config)],
            progress_interval_seconds: 0,
            producer_lead_groups: Some(1),
        };
        let trace = Arc::new(Mutex::new(Trace::default()));
        let result = run_historical_days_v1(&[day], rules, &|_| {
            Ok(FeeProbe {
                health,
                trace: trace.clone(),
            })
        })
        .unwrap();
        let report = &result[0].candidates[0].report;
        assert_eq!(report["end_reason"], "RangeEnd");
        assert_eq!(
            report["local_funds_rejected_batches"],
            1,
            "report={report:#} trace={:?}",
            trace.lock().unwrap()
        );
        assert_eq!(report["recording_drained"], true);
        let t = trace.lock().unwrap();
        // 原固定种子首请求outbound=21ms、taker=150ms、MINED=10ms，
        // 首八条生命周期反馈延迟最大61ms。2701是真实collector admission时刻。
        let arrival = t.first_platform_arrived_at.unwrap();
        let second_send = t.second_calculation_at.unwrap() + 1000;
        assert_eq!(arrival, t.first_calculation_at.unwrap() + 1000 + 21);
        assert!(arrival + 150 + 20 + 61 < BASE + 2701, "{t:?}");
        assert!(BASE + 2701 < second_send, "{t:?}");
        assert_eq!(t.sent, 1, "第二批零发送，不缩单重发");
        assert_eq!(t.second_calculation_cash, Some(Decimal::new(937, 2)));
        assert_eq!(t.rejected, 1);
        assert!(t.observations_after_rejection > 0, "拒绝后继续观察");
        let economics = &report["account_economics_memory"];
        assert_eq!(economics["cash"]["actual_available_pusd"], "4.33");
        let market = economics["markets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["condition_id"] == FIRST)
            .unwrap();
        let asset_index = market["assets"]
            .as_array()
            .unwrap()
            .iter()
            .position(|asset| asset["token_id"] == "20")
            .unwrap();
        // executed按Maker/Taker角色分组；success_shares才按assets分组。
        let executed = &market["executed"][1];
        let shares: Decimal = executed["shares"].as_str().unwrap().parse().unwrap();
        assert!(shares > Decimal::from(105), "实际成交超过原目标100份加5份");
        assert_eq!(shares, Decimal::new(900000009, 6));
        assert_eq!(executed["actual_fee_pusd"], "5.67");
        assert_eq!(market["success_shares"][asset_index], "900.000009");
        assert_eq!(market["executed"][0]["shares"], "0");
    }
}
