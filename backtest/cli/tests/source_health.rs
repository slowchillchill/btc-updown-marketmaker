#[allow(dead_code)]
#[path = "../../tests/common/mod.rs"]
mod common;
use backtest::*;
use baseline_mm::*;
use common::*;
use contracts::*;
use data_collect::{
    polymarket_market_metadata_v6_layout::*,
    polymarket_orderbook_v3::{
        POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION, PolymarketBookV3, PolymarketOrderbookV3Envelope,
        PolymarketOrderbookV3Event, PolymarketOrderbookV3Message, PolymarketPriceChangeEntryV3,
        PolymarketPriceChangeV3, PolymarketPriceLevelV3,
    },
    source_hourly_layout::*,
    writer::TypedParquetSegmentWriter,
};
use rust_decimal::Decimal;
use serde_json::json;
use sha2::{Digest, Sha256};
use shared_run::*;
use std::{path::Path, sync::Arc, time::Instant};

// 只观测真实策略，所有业务决定、期限、资格和通知仍委托同一实现。
struct Trace {
    strategy: ComplementaryPairV1,
    decisions: Vec<(i64, ComplementaryPairDiagnosticV1, Vec<String>)>,
    sent: Vec<(i64, String)>,
    invalidated: usize,
    qualify_ns: u128,
    deadline_ns: std::cell::Cell<u128>,
    qualified_at: std::collections::BTreeMap<String, i64>,
}
impl StrategyV1 for Trace {
    type Error = StrategyErrorV1;
    type Diagnostics = ComplementaryPairDiagnosticV1;
    fn uses_source_health(&self) -> bool {
        self.strategy.uses_source_health()
    }
    fn next_health_evaluation_at(
        &self,
        input: &StrategyCurrentReadV1<'_>,
        c: Option<&StrategyCalculationV1<Self::Diagnostics>>,
        at: i64,
    ) -> Option<i64> {
        let started = Instant::now();
        let result = self.strategy.next_health_evaluation_at(input, c, at);
        self.deadline_ns
            .set(self.deadline_ns.get() + started.elapsed().as_nanos());
        result
    }
    fn qualify_intent(
        &mut self,
        input: &StrategyCurrentReadV1<'_>,
        c: &StrategyCalculationV1<Self::Diagnostics>,
        index: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        let start = Instant::now();
        let result = self.strategy.qualify_intent(input, c, index, at);
        self.qualify_ns += start.elapsed().as_nanos();
        if matches!(result, Ok(IntentQualificationV1::Qualified))
            && let StrategyOutputV1::Continue { intents, .. } = &c.output
        {
            self.qualified_at
                .insert(intents[index].context.global_request_id.clone(), at);
        }
        result
    }
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1<Self::Diagnostics>, Self::Error> {
        let result = self.strategy.decide(input, at)?;
        if let StrategyOutputV1::Continue {
            diagnostics,
            intents,
            ..
        } = &result
        {
            if !intents.is_empty() {
                println!(
                    "decision_at={} intents={:?}",
                    at - BASE,
                    intents
                        .iter()
                        .map(|i| i.intent.as_ref())
                        .collect::<Vec<_>>()
                );
            }
            self.decisions.push((
                at,
                diagnostics.clone(),
                intents
                    .iter()
                    .map(|i| {
                        match i.intent.as_ref() {
                            ModelIntentV1::SubmitPairedBuy { .. } => "pair",
                            ModelIntentV1::SubmitSingleBuy {
                                order:
                                    BuyLegIntentV1 {
                                        order: BuyOrderIntentV1::FakMarket { .. },
                                        ..
                                    },
                                ..
                            } => "fak",
                            ModelIntentV1::SubmitSingleBuy { .. } => "maker",
                            ModelIntentV1::CancelOrders { .. } => "cancel",
                            _ => "other",
                        }
                        .into()
                    })
                    .collect(),
            ));
        }
        Ok(result)
    }
    fn dispatched(
        &mut self,
        c: &StrategyCalculationV1<Self::Diagnostics>,
        result: &Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1>,
    ) {
        self.strategy.dispatched(c, result);
        let StrategyOutputV1::Continue { intents, .. } = &c.output else {
            return;
        };
        let sent = match result {
            Ok(r) => r,
            Err(e) => &e.sent,
        };
        for receipt in sent {
            let item = intents
                .iter()
                .find(|i| i.context.global_request_id == receipt.context.global_request_id)
                .unwrap();
            self.sent.push((
                *self
                    .qualified_at
                    .get(&receipt.context.global_request_id)
                    .expect("已发请求经过原提交前资格边界"),
                match item.intent.as_ref() {
                    ModelIntentV1::CancelOrders { .. } => "cancel",
                    ModelIntentV1::SubmitPairedBuy { .. } => "pair",
                    ModelIntentV1::SubmitSingleBuy {
                        order:
                            BuyLegIntentV1 {
                                order: BuyOrderIntentV1::FakMarket { .. },
                                ..
                            },
                        ..
                    } => "fak",
                    _ => "maker",
                }
                .into(),
            ));
        }
    }
    fn invalidated(
        &mut self,
        c: &StrategyCalculationV1<Self::Diagnostics>,
        result: &InvalidatedBatchV1,
    ) {
        println!(
            "invalidated calculation_start={} detected={} sent_prefix={} index={} reason={}",
            c.calculation_started_at_utc_ms - BASE,
            result.detected_at_utc_ms - BASE,
            result.sent.len(),
            result.invalidated_intent_index,
            result.reason.reason_code
        );
        self.invalidated += 1;
        self.strategy.invalidated(c, result);
    }
}
fn strategy_config() -> ComplementaryPairConfigV1 {
    let mut c = ComplementaryPairConfigV1::parse_toml(include_str!(
        "../../../baseline-mm/configs/strategies/complementary_pair/synthetic-engineering.toml"
    ))
    .unwrap();
    c.probability.binance_innovation_weight = 0.0;
    c.quote.base_half_spread = Decimal::new(1, 2);
    c
}
fn finish(
    writer: TypedParquetSegmentWriter,
    source: &str,
    schema: &str,
    rows: u64,
) -> serde_json::Value {
    let mut writer = writer;
    writer.append_key_value_metadata("data_collect_daily_file_fact_v1", json!({"source_kind":source,"schema_version":schema,"utc_hour":"2026-08-24T00","part":0,"rows":rows}).to_string()).unwrap();
    let file = writer.finalize().unwrap();
    json!({"path":file.final_path.file_name().unwrap().to_str().unwrap(),"source_kind":source,"schema_version":schema,"utc_hour":"2026-08-24T00","part":0,"rows":file.row_count,"bytes":std::fs::metadata(&file.final_path).unwrap().len(),"sha256":file.sha256})
}
/// 原V3盘口与原历史装配；只增正式V6锚点/规则与RTDS合成来源。
fn fixture(
    root: &Path,
    price: &str,
    spot_times: &[i64],
) -> (
    HistoricalRunConfigV1,
    Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
) {
    fixture_with_events(
        root,
        price,
        &spot_times.iter().map(|at| (*at, *at)).collect::<Vec<_>>(),
    )
}
fn fixture_with_events(
    root: &Path,
    price: &str,
    spot_times: &[(i64, i64)],
) -> (
    HistoricalRunConfigV1,
    Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
) {
    let (mut config, rules) = setup(root);
    // 本文件有限组合的完整 SIM 记录不足1024项；容纳完整积压，不依赖writer调度。
    // 原公共夹具与生产回压语义保持；原128预算的实际失败保留在验证证据。
    config.finite_in_flight_groups = 1024;
    let manifest_path = &config.preparation.scope.manifest_paths[0];
    let day = manifest_path.parent().unwrap();
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(manifest_path).unwrap()).unwrap();
    let body = r#"{"eventMetadata":{"priceToBeat":100000},"twapEnabled":false}"#;
    let mut anchor: MarketAnchorRuleUpdateRow = serde_json::from_value(json!({
        "timestamp_received":BASE+50,"metadata_origin":"gamma_event_by_slug","collector_host_id":"synthetic-collector","collector_region":"test",
        "slug":"btc-updown-5m-engineering","condition_id":FIRST,"clob_market_id":"3195576","asset_ids":"[\"20\",\"3\"]",
        "market_start_utc":BASE+1000,"market_end_utc":BASE+10000,"fixed_identity_verified_at_utc":BASE-1,
        "market_lifecycle_state":"prefetched","market_runtime_role":"next","metadata_fetch_start_utc":BASE+40,
        "anchor_query_source_epoch_id":"synthetic-query","anchor_query_ordinal":1,"anchor_query_result":"observed",
        "price_to_beat_decimal":"100000","price_to_beat_source_field":"eventMetadata.priceToBeat","twap_enabled":false,
        "http_received_at_utc":BASE+50,"http_request_url":"https://gamma-api.polymarket.com/events/slug/btc-updown-5m-engineering",
        "http_body_encoding":"utf8","http_body":body,"http_body_sha256":format!("{:x}",Sha256::digest(body.as_bytes()))
    })).unwrap();
    anchor.collector_admission_sequence = 2;
    anchor.collector_admission_time_utc_ms = BASE + 51;
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        day,
        "health-metadata",
        POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION,
        "polymarket_market_metadata",
    )
    .unwrap();
    writer
        .write_polymarket_market_metadata_v6_rows(&[
            PolymarketMarketMetadataV6Row::Existing(metadata(
                FIRST,
                "3195576",
                ["20", "3"],
                BASE,
                BASE + 1000,
                BASE + 10000,
                1,
            )),
            PolymarketMarketMetadataV6Row::AnchorRule(anchor),
            PolymarketMarketMetadataV6Row::Existing(metadata(
                SECOND,
                "3195577",
                ["21", "4"],
                BASE + 6000,
                BASE + 10000,
                BASE + 20000,
                3,
            )),
        ])
        .unwrap();
    let metadata_file = finish(
        writer,
        "polymarket_market_metadata",
        POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION,
        3,
    );
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        day,
        "health-spot",
        "polymarket_rtds_chainlink_btc_usd_v4",
        "polymarket_rtds_chainlink_btc_usd",
    )
    .unwrap();
    writer.write_rtds_chainlink_btc_usd_rows(&spot_times.iter().map(|(at,event)| RtdsChainlinkBtcUsdHourlyRow {
        timestamp_received:BASE+at, message_timestamp:BASE+event, payload_timestamp:BASE+event, age_ms:Some(at-event),
        symbol:"btc/usd".into(),value:price.into(),topic:"crypto_prices_chainlink".into(),message_type:"update".into(),freshness_status:"fresh".into(),subscription_filter:"{}".into(),
        source_native_payload_json:json!({"topic":"crypto_prices_chainlink","type":"update","timestamp":BASE+event,"payload":{"symbol":"btc/usd","timestamp":BASE+event,"value":price}}).to_string(),
        collector_host_id:"synthetic-collector".into(),collector_region:"test".into(),source_endpoint:"wss://ws-live-data.polymarket.com".into(),schema_version:"polymarket_rtds_chainlink_btc_usd_v4".into(),
    }).collect::<Vec<_>>()).unwrap();
    let spot_file = finish(
        writer,
        "polymarket_rtds_chainlink_btc_usd",
        "polymarket_rtds_chainlink_btc_usd_v4",
        spot_times.len() as u64,
    );
    let files = manifest["files"].as_array_mut().unwrap();
    files.retain(|file| file["source_kind"] != "polymarket_market_metadata");
    files.extend([metadata_file, spot_file]);
    manifest["writer_finalize_acks"]
        .as_array_mut()
        .unwrap()
        .push(json!("polymarket_rtds_chainlink_twap"));
    std::fs::write(manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    config.strategy = StrategyDescriptorV1 {
        strategy_id: STRATEGY_ID.into(),
        strategy_rule_version: RULE_VERSION.into(),
        effective_parameters: serde_json::to_value(strategy_config()).unwrap(),
    };
    (config, rules)
}
async fn run(
    config: &HistoricalRunConfigV1,
    rules: Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
    c: ComplementaryPairConfigV1,
) -> (HistoricalRunReportV1, Trace) {
    let mut config = config.clone();
    config.strategy.effective_parameters = serde_json::to_value(&c).unwrap();
    let mut trace = Trace {
        strategy: ComplementaryPairV1::new(
            c,
            rules.clone(),
            "health-history".into(),
            StrategyContinuityV1::new_account(),
        )
        .unwrap(),
        decisions: vec![],
        sent: vec![],
        invalidated: 0,
        qualify_ns: 0,
        deadline_ns: std::cell::Cell::new(0),
        qualified_at: std::collections::BTreeMap::new(),
    };
    let report = run_historical_v1(&config, rules, &mut trace).await.unwrap();
    let sim_records = std::fs::read_to_string(&config.sim_log)
        .unwrap()
        .lines()
        .count();
    assert!(sim_records < config.finite_in_flight_groups);
    println!(
        "finite_sim_record_total={sim_records} budget={} (完整总数是积压上界，非采样队列峰值)",
        config.finite_in_flight_groups
    );
    (report, trace)
}
fn book(
    token: &str,
    received: i64,
    event: i64,
    sequence: u64,
    epoch: &str,
    ask: &str,
) -> PolymarketOrderbookV3Message {
    PolymarketOrderbookV3Message {
        envelope: PolymarketOrderbookV3Envelope {
            timestamp_received: BASE + received,
            timestamp: Some(BASE + event),
            market: FIRST.into(),
            slug: Some("btc-updown-5m-engineering".into()),
            collector_host_id: "synthetic-collector".into(),
            collector_region: "test".into(),
            source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
            schema_version: POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION.into(),
            source_sequence_epoch_id: epoch.into(),
            collector_admission_sequence: sequence,
            collector_admission_time_utc_ms: BASE + received + 1,
        },
        event: PolymarketOrderbookV3Event::Book(PolymarketBookV3 {
            asset_id: token.into(),
            bids: vec![PolymarketPriceLevelV3 {
                price: "0.4".into(),
                size: "100".into(),
            }],
            asks: vec![PolymarketPriceLevelV3 {
                price: ask.into(),
                size: "100".into(),
            }],
            last_trade_price: None,
        }),
    }
}
fn up_bid_change(
    received: i64,
    event: i64,
    sequence: u64,
    size: &str,
) -> PolymarketOrderbookV3Message {
    let mut message = book("20", received, event, sequence, "epoch-a", "0.7");
    message.event = PolymarketOrderbookV3Event::PriceChange(PolymarketPriceChangeV3 {
        price_changes: vec![PolymarketPriceChangeEntryV3 {
            asset_id: "20".into(),
            price: "0.62".into(),
            size: Some(size.into()),
            side: "BUY".into(),
            best_bid: None,
            best_ask: None,
        }],
    });
    message
}
fn replace_books(config: &HistoricalRunConfigV1, books: &[PolymarketOrderbookV3Message]) {
    let path = &config.preparation.scope.manifest_paths[0];
    let day = path.parent().unwrap();
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        day,
        "health-books",
        POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
        "polymarket_market",
    )
    .unwrap();
    writer
        .write_polymarket_orderbook_v3_messages(books, &mut Default::default())
        .unwrap();
    let fact = finish(
        writer,
        "polymarket_market",
        POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
        books.len() as u64,
    );
    let files = manifest["files"].as_array_mut().unwrap();
    files.retain(|f| f["source_kind"] != "polymarket_market");
    files.push(fact);
    std::fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn real_strategy_auxiliary_degradation_keeps_independent_repair_but_bad_books_stop_it() {
    for bad_books in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (config, rules) = fixture(temp.path(), "100010", &[800]);
        replace_books(
            &config,
            &[
                book("20", 1100, 1100, 1, "epoch-a", "0.7"),
                book("3", 1100, 1100, 2, "epoch-a", "0.6"),
                book("3", 1300, 1300, 3, "epoch-a", "0.4"),
                up_bid_change(2200, 2200, 4, "100"),
                up_bid_change(3000, 2400, 5, "95"),
            ],
        );
        let mut c = strategy_config();
        c.health.chainlink_silence_ms = 1500;
        c.repair.residual_risk_weight = Decimal::from(10);
        c.repair.maximum_overfill_pusd = Decimal::from(1_000_000_000u64);
        c.repair.maximum_overfill_shares = Decimal::from(1_000_000_000u64);
        c.risk.per_market_orphan_limit_pusd = Decimal::from(1_000_000_000u64);
        c.risk.account_orphan_limit_pusd = Decimal::from(1_000_000_000u64);
        if bad_books {
            c.health.polymarket_silence_ms = 1100;
        }
        let (report, trace) = run(&config, rules, c).await;
        println!(
            "bad_books={bad_books} actual_submit_boundaries={:?} qualification_calls={} invalidated_batches={}",
            trace
                .sent
                .iter()
                .map(|(at, k)| (at - BASE, k))
                .collect::<Vec<_>>(),
            report.source_health.as_ref().unwrap().qualification_calls,
            report.source_health.as_ref().unwrap().invalidated_batches
        );
        assert!(
            report
                .account_economics_memory
                .as_ref()
                .unwrap()
                .markets
                .iter()
                .any(|m| m.success_shares.iter().any(|q| q.0 != "0")),
            "原隐藏数量消耗已形成真实持仓"
        );
        if !bad_books {
            assert!(trace.decisions.iter().any(|(at, d, _)| *at >= BASE + 2300
                && d.markets.iter().any(|m| matches!(
                    m.probability,
                    ProbabilityEstimateV1::Unavailable(EstimateUnavailableV1::UnhealthySource)
                ) && matches!(
                    m.choice,
                    MarketChoiceV1::ActiveRepair
                        | MarketChoiceV1::MakerRepair
                        | MarketChoiceV1::Maintain
                ))));
            assert!(
                trace
                    .sent
                    .iter()
                    .any(|(at, k)| *at >= BASE + 2300 && (k == "fak" || k == "maker")),
                "独立修复必须真正提交"
            );
        } else {
            assert!(
                trace
                    .decisions
                    .iter()
                    .filter(|(at, _, _)| *at >= BASE + 2400)
                    .all(|(_, _, intents)| intents.iter().all(|k| k == "cancel"))
            );
        }
        assert!(report.source_health.unwrap().qualification_calls > 0);
    }
}
#[tokio::test(flavor = "current_thread")]
async fn real_strategy_epoch_requires_both_assets_before_reopening() {
    let temp = tempfile::tempdir().unwrap();
    let (config, rules) = fixture(temp.path(), "100000", &[800]);
    replace_books(
        &config,
        &[
            book("20", 1100, 1100, 1, "epoch-a", "0.7"),
            book("3", 1100, 1100, 2, "epoch-a", "0.6"),
            book("20", 3000, 3000, 1, "epoch-b", "0.6"),
            book("3", 4500, 4500, 2, "epoch-b", "0.6"),
        ],
    );
    let (report, trace) = run(&config, rules, strategy_config()).await;
    assert!(trace.decisions.iter().any(|(at, d, _)| *at > BASE + 3000
        && *at < BASE + 4500
        && d.markets.iter().any(|m| {
            m.source_health
                .iter()
                .any(|i| i.failure == SourceHealthFailureV1::SynchronizationRequired)
        })));
    assert!(
        trace
            .decisions
            .iter()
            .filter(|(at, _, _)| *at > BASE + 3000 && *at < BASE + 4500)
            .all(|(_, _, intents)| !intents.iter().any(|k| k == "pair"))
    );
    assert!(trace.decisions.iter().any(
        |(at, d, _)| *at > BASE + 4500 && d.markets.iter().any(|m| m.source_health.is_empty())
    ));
    assert!(report.source_health.unwrap().qualification_calls > 0);
}
#[tokio::test(flavor = "current_thread")]
async fn real_strategy_all_silent_deadline_sends_cancel_and_recovery_recalculates() {
    let temp = tempfile::tempdir().unwrap();
    let (config, rules) = fixture(temp.path(), "100000", &[800, 5000]);
    let mut c = strategy_config();
    c.health.chainlink_silence_ms = 1500;
    let (report, trace) = run(&config, rules, c).await;
    println!(
        "silence_recovery_actual_submit_boundaries={:?} calculations={}",
        trace
            .sent
            .iter()
            .map(|(at, k)| (at - BASE, k))
            .collect::<Vec<_>>(),
        trace.decisions.len()
    );
    assert!(trace.sent.iter().any(|(_, kind)| kind == "pair"));
    assert!(
        trace
            .sent
            .iter()
            .any(|(at, kind)| *at >= BASE + 2300 && *at < BASE + 5000 && kind == "cancel"),
        "没有新消息的期限必须通过真实策略发撤单"
    );
    assert!(
        trace.decisions.iter().any(|(at, d, i)| *at > BASE + 5000
            && d.markets.iter().any(|m| m.source_health.is_empty())
            && i.iter().any(|kind| kind == "pair")),
        "新有效同值消息恢复后重算新配对"
    );
    assert!(report.source_health.unwrap().qualification_calls > 0);
}
#[tokio::test(flavor = "current_thread")]
async fn real_strategy_health_expires_during_compute_or_dispatch_wait() {
    for silence in [600, 800] {
        let temp = tempfile::tempdir().unwrap();
        let (config, rules) = fixture(temp.path(), "100000", &[800]);
        let mut c = strategy_config();
        c.health.chainlink_silence_ms = silence;
        let (report, trace) = run(&config, rules, c).await;
        assert!(trace.sent.iter().all(|(_, kind)| kind != "pair"));
        assert!(report.source_health.unwrap().invalidated_batches > 0);
        assert!(trace.invalidated > 0);
    }
}

/// 真实请求的有限重复测量；分别报告期限/资格回调实耗，不将整轮耗时差称为纯检查开销。
#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn real_strategy_finite_health_callback_measurement() {
    let root = std::path::PathBuf::from(std::env::var("STRATEGY_HEALTH_MEASUREMENT_ROOT").unwrap());
    std::fs::create_dir(&root).unwrap();
    let (mut config, rules) = fixture(&root, "100010", &[800]);
    replace_books(
        &config,
        &[
            book("20", 1100, 1100, 1, "epoch-a", "0.7"),
            book("3", 1100, 1100, 2, "epoch-a", "0.6"),
            book("3", 1300, 1300, 3, "epoch-a", "0.4"),
            up_bid_change(2200, 2200, 4, "100"),
            up_bid_change(3000, 2400, 5, "95"),
        ],
    );
    let mut c = strategy_config();
    c.health.chainlink_silence_ms = 1500;
    c.repair.residual_risk_weight = Decimal::from(10);
    c.repair.maximum_overfill_pusd = Decimal::from(1_000_000_000u64);
    c.repair.maximum_overfill_shares = Decimal::from(1_000_000_000u64);
    c.risk.per_market_orphan_limit_pusd = Decimal::from(1_000_000_000u64);
    c.risk.account_orphan_limit_pusd = Decimal::from(1_000_000_000u64);
    config.strategy.effective_parameters = serde_json::to_value(&c).unwrap();
    std::fs::write(
        root.join("input-config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
    std::fs::write(
        root.join("strategy-config.json"),
        serde_json::to_vec_pretty(&c).unwrap(),
    )
    .unwrap();
    let identity = format!(
        "{:x}",
        Sha256::digest(std::fs::read(&config.preparation.scope.manifest_paths[0]).unwrap())
    );
    let started = Instant::now();
    let (report, trace) = run(&config, rules, c).await;
    let elapsed_ns = started.elapsed().as_nanos();
    let health = report.source_health.as_ref().unwrap();
    assert!(health.qualification_calls > 0 && trace.sent.iter().any(|(_, kind)| kind == "maker"));
    let current_trade_count = report
        .account_economics_memory
        .as_ref()
        .unwrap()
        .markets
        .iter()
        .filter(|m| m.success_shares.iter().any(|q| q.0 != "0"))
        .count();
    assert!(
        current_trade_count > 0,
        "新增当前费用读取实际消费非空原交易事实"
    );
    let measurement = json!({"purpose":"有限合成工程实耗，不是盈利或在线性能结论", "input_manifest_sha256":identity,
        "whole_run_elapsed_ns":elapsed_ns, "qualification_callback_ns":trace.qualify_ns,
        "deadline_callback_ns":trace.deadline_ns.get(), "qualification_calls":health.qualification_calls,
        "deadline_wakeups":health.deadline_wakeups, "invalidated_batches":health.invalidated_batches,
        "decisions":trace.decisions.len(), "markets_with_actual_trades":current_trade_count,"actual_requests":trace.sent,
        "timing_note":"actual_requests取提交前最后一次资格回调的合法at；decisions取计算开始；两类回调时间不包含当前读取构造或decide内部健康判断；整轮包含新增逐交易读取构造但无法单独归因，整轮时间不是纯健康开销"});
    std::fs::write(
        root.join("measurement.json"),
        serde_json::to_vec_pretty(&measurement).unwrap(),
    )
    .unwrap();
    std::fs::write(
        root.join("report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    println!("{measurement}");
}

#[tokio::test(flavor = "current_thread")]
async fn real_strategy_event_lag_is_not_repaired_by_same_value_receive_but_valid_event_recovers() {
    let temp = tempfile::tempdir().unwrap();
    let (config, rules) = fixture_with_events(
        temp.path(),
        "100000",
        &[(800, 800), (3000, 800), (5000, 5000)],
    );
    let mut c = strategy_config();
    c.health.chainlink_event_delay_ms = 1500;
    let (report, trace) = run(&config, rules, c).await;
    assert!(trace.decisions.iter().any(|(at, d, _)| *at >= BASE + 3100
        && *at < BASE + 5000
        && d.markets.iter().any(|m| {
            m.source_health
                .iter()
                .any(|h| h.failure == SourceHealthFailureV1::EventDelay)
        })));
    assert!(
        !trace
            .sent
            .iter()
            .any(|(at, k)| *at >= BASE + 2300 && *at < BASE + 5000 && k == "pair")
    );
    assert!(
        trace
            .sent
            .iter()
            .any(|(at, k)| *at >= BASE + 5000 && k == "pair")
    );
    assert!(report.source_health.unwrap().qualification_calls > 0);
}
#[tokio::test(flavor = "current_thread")]
async fn real_strategy_health_callbacks_do_not_reset_high_water_or_delay_exit() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, rules) = fixture(temp.path(), "100000", &[800]);
    config.session.initial_account_state.available_pusd = ExactDecimal("1300".into());
    let (report, trace) = run(&config, rules, strategy_config()).await;
    assert_eq!(report.end_reason, HistoricalEndReasonV1::StrategyExit);
    assert!(trace.sent.is_empty());
    assert!(trace.strategy.continuity().terminating);
    assert_eq!(
        trace.strategy.continuity().confirmed_high_water_pusd,
        contracts::economics::EconomicRational::from_decimal(Decimal::from(2000)).public()
    );
    assert!(!config.result_path.exists());
}

fn add_binance(config: &HistoricalRunConfigV1) {
    use data_collect::{event::*, source_format::BinanceAggTradeRaw};
    let path = &config.preparation.scope.manifest_paths[0];
    let day = path.parent().unwrap();
    let mut files = Vec::new();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        day,
        "health-depth-snapshot",
        BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
        "binance_depth_snapshot",
    )
    .unwrap();
    writer
        .write_binance_depth_snapshot_rows(&[BinanceDepthSnapshotHourlyRow {
            timestamp_received: BASE + 700,
            snapshot_fetch_start_utc: BASE + 690,
            last_update_id: 100,
            bids: r#"[["99999","10"]]"#.into(),
            asks: r#"[["100001","10"]]"#.into(),
            symbol: "BTCUSDT".into(),
            source_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
                .into(),
            snapshot_status: "complete".into(),
            source_sequence_epoch_id: Some("health-depth".into()),
            connection_generation: Some(1),
            depth_ingress_sequence: Some(1),
            collector_admission_sequence: Some(1),
            collector_admission_time_utc_ms: Some(BASE + 701),
            collector_host_id: "synthetic-collector".into(),
            collector_region: "test".into(),
            schema_version: BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION.into(),
        }])
        .unwrap();
    files.push(finish(
        writer,
        "binance_depth_snapshot",
        BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
        1,
    ));
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        day,
        "health-depth-update",
        BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
        "binance_depth_update",
    )
    .unwrap();
    writer
        .write_binance_depth_update_rows(&[BinanceDepthUpdateHourlyRow {
            timestamp_received: BASE + 750,
            event_type: "depthUpdate".into(),
            event_time: BASE + 750,
            symbol: "BTCUSDT".into(),
            first_update_id: 101,
            final_update_id: 101,
            bids: r#"[["99999","10"]]"#.into(),
            asks: r#"[["100001","10"]]"#.into(),
            stream: "btcusdt@depth".into(),
            snapshot_last_update_id: Some(100),
            local_u_before: None,
            stitch_verdict: BINANCE_DEPTH_VERDICT_APPLIED.into(),
            stitch_evidence: Some("U <= lastUpdateId + 1 <= u".into()),
            stitch_gap_reason: None,
            source_sequence_epoch_id: Some("health-depth".into()),
            connection_generation: Some(1),
            depth_ingress_sequence: Some(2),
            collector_admission_sequence: Some(2),
            collector_admission_time_utc_ms: Some(BASE + 751),
            collector_host_id: "synthetic-collector".into(),
            collector_region: "test".into(),
            source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@depth".into(),
            schema_version: BINANCE_DEPTH_UPDATE_SCHEMA_VERSION.into(),
        }])
        .unwrap();
    files.push(finish(
        writer,
        "binance_depth_update",
        BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
        1,
    ));
    let at = BASE + 800;
    let raw = BinanceAggTradeRaw {
        envelope: EventEnvelope {
            source_kind: SourceKind::BinanceAggTrade,
            source_event_type: "aggTrade".into(),
            source_identifier: "BTCUSDT".into(),
            source_timestamp: SourceTimestamp::Timestamp {
                value: at.to_string(),
            },
            recv_time_utc_ms: Some(at),
            collector_host_id: "synthetic-collector".into(),
            collector_region: "test".into(),
            source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@aggTrade".into(),
            host_clock_sync_status: HostClockSyncStatus::Synchronized {
                source: "synthetic".into(),
            },
            raw_evidence: RawEvidence::Json("{}".into()),
        },
        event_type: "aggTrade".into(),
        event_time_ms: at,
        symbol: "BTCUSDT".into(),
        aggregate_trade_id: 1,
        price: "100000".into(),
        quantity: "1".into(),
        first_trade_id: 1,
        last_trade_id: 1,
        trade_time_ms: at,
        buyer_is_maker: false,
        ws_ignore: false,
        stream_name: "btcusdt@aggTrade".into(),
    };
    let row = BinanceAggTradeHourlyRow::from_raw(
        &raw,
        BinanceAggTradeOrdering {
            source_sequence_epoch_id: "health-trade".into(),
            connection_generation: 1,
            websocket_ingress_sequence: 1,
            collector_admission_sequence: 1,
            collector_admission_time_utc_ms: at + 1,
        },
    )
    .unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        day,
        "health-trade",
        &row.schema_version,
        "binance_aggtrade",
    )
    .unwrap();
    writer
        .write_binance_aggtrade_rows(std::slice::from_ref(&row))
        .unwrap();
    files.push(finish(writer, "binance_aggtrade", &row.schema_version, 1));
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    manifest["files"].as_array_mut().unwrap().extend(files);
    std::fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn real_strategy_enabled_depth_trade_and_all_stream_silence_cancel_actual_risk_orders() {
    for all in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (config, rules) = fixture(temp.path(), "100000", &[800]);
        add_binance(&config);
        let mut c = strategy_config();
        c.probability.binance_trade_pressure_weight_usd = 1.0;
        c.health.binance_depth_silence_ms = 1500;
        if all {
            c.health.binance_trade_silence_ms = 1500;
            c.health.chainlink_silence_ms = 1500;
            c.health.polymarket_silence_ms = 1500;
        }
        let (report, trace) = run(&config, rules, c).await;
        assert!(trace.sent.iter().any(|(_, k)| k == "pair"));
        assert!(
            trace
                .sent
                .iter()
                .any(|(at, k)| *at >= BASE + 2250 && k == "cancel")
        );
        assert!(trace.decisions.iter().any(|(at, d, _)| *at >= BASE + 2250
            && d.markets.iter().any(|m| {
                m.source_health.iter().any(|h| {
                    h.scope.source == SourceStreamKindV1::BinanceDepth
                        && h.failure == SourceHealthFailureV1::ReceiveSilence
                })
            })));
        if all {
            assert!(
                trace
                    .decisions
                    .iter()
                    .any(|(at, d, _)| *at >= BASE + 4000 && d.markets[0].source_health.len() == 5)
            );
        }
        assert!(report.source_health.unwrap().qualification_calls > 0);
    }
}
