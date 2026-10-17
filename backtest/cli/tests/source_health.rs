#[path = "../../tests/common/default.rs"]
mod fixture_default;
#[path = "../../tests/common/fixture_material.rs"]
mod fixture_material;
mod common {
    pub(crate) use super::fixture_default::*;
    pub(crate) use super::fixture_material::*;
}

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

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
struct CallbackSamples {
    count: u64,
    total_ns: u128,
    max_ns: u128,
}
impl CallbackSamples {
    fn record(&mut self, elapsed: std::time::Duration) {
        self.count += 1;
        let ns = elapsed.as_nanos();
        self.total_ns += ns;
        self.max_ns = self.max_ns.max(ns);
    }
}
#[derive(serde::Serialize)]
struct DecideSample {
    started_at_utc_ms: i64,
    elapsed_ns: u128,
    outcome: &'static str,
}
#[derive(serde::Serialize)]
struct TraceSummary {
    decisions: Vec<(i64, ComplementaryPairDiagnosticV1, Vec<String>)>,
    sent: Vec<(i64, String)>,
    invalidated: usize,
    invalidated_at: Vec<(i64, i64)>,
    pure_decide: CallbackSamples,
    decide_samples: Vec<DecideSample>,
    public_qualification: CallbackSamples,
    full_qualification: CallbackSamples,
    next_health_evaluation: CallbackSamples,
}
// 只观测真实策略，所有业务决定、期限、资格和通知仍委托同一实现。
struct Trace {
    strategy: ComplementaryPairV1,
    decisions: Vec<(i64, ComplementaryPairDiagnosticV1, Vec<String>)>,
    sent: Vec<(i64, String)>,
    invalidated: usize,
    pure_decide: CallbackSamples,
    decide_samples: Vec<DecideSample>,
    public_qualification: std::cell::Cell<CallbackSamples>,
    full_qualification: CallbackSamples,
    next_health_evaluation: std::cell::Cell<CallbackSamples>,
    invalidated_at: Vec<(i64, i64)>,
    summary_sink: Option<Arc<std::sync::Mutex<Option<TraceSummary>>>>,
    qualified_at: std::collections::BTreeMap<String, i64>,
    qualification_invalidations: Vec<(i64, i64)>,
}
impl StrategyV1 for Trace {
    type Error = StrategyErrorV1;
    type Diagnostics = ComplementaryPairDiagnosticV1;
    fn requires_trade_economics_details(&self) -> bool {
        self.strategy.requires_trade_economics_details()
    }
    fn observation_requirements(&self) -> PublicObservationRequirementsV1 {
        self.strategy.observation_requirements()
    }
    fn uses_source_health(&self) -> bool {
        self.strategy.uses_source_health()
    }
    fn next_health_evaluation_at(
        &self,
        input: &[sim_system::public_consumption::PublicObservationFrameV1<'_>],
        c: Option<&StrategyCalculationV1<Self::Diagnostics>>,
        at: i64,
    ) -> Option<i64> {
        let started = Instant::now();
        let result = self.strategy.next_health_evaluation_at(input, c, at);
        let mut samples = self.next_health_evaluation.get();
        samples.record(started.elapsed());
        self.next_health_evaluation.set(samples);
        result
    }
    fn qualify_intent_public(
        &self,
        group: &ClosedBusinessGroupV1,
        c: &StrategyCalculationV1<Self::Diagnostics>,
        index: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        let started = Instant::now();
        let result = self.strategy.qualify_intent_public(group, c, index, at);
        let mut samples = self.public_qualification.get();
        samples.record(started.elapsed());
        self.public_qualification.set(samples);
        result
    }
    fn uses_borrowed_input(&self) -> bool {
        self.strategy.uses_borrowed_input()
    }
    fn selects_relevant_markets(&self) -> bool {
        self.strategy.selects_relevant_markets()
    }
    fn historical_market_requirements(&self) -> Vec<ConditionId> {
        self.strategy.historical_market_requirements()
    }
    fn calculation_market_requirements(
        &self,
        c: &StrategyCalculationV1<Self::Diagnostics>,
    ) -> Vec<ConditionId> {
        self.strategy.calculation_market_requirements(c)
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
        self.record_qualification(result, c, index, at, start.elapsed())
    }
    fn qualify_intent_borrowed(
        &mut self,
        input: &BorrowedStrategyCurrentReadV1<'_>,
        c: &StrategyCalculationV1<Self::Diagnostics>,
        index: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        let start = Instant::now();
        let result = self.strategy.qualify_intent_borrowed(input, c, index, at);
        self.record_qualification(result, c, index, at, start.elapsed())
    }
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1<Self::Diagnostics>, Self::Error> {
        let start = Instant::now();
        let result = self.strategy.decide(input, at);
        self.record_decision(result, at, start.elapsed())
    }
    fn decide_borrowed(
        &mut self,
        input: &BorrowedStrategyInputV1<'_>,
        at: i64,
    ) -> Result<StrategyOutputV1<Self::Diagnostics>, Self::Error> {
        let start = Instant::now();
        let result = self.strategy.decide_borrowed(input, at);
        self.record_decision(result, at, start.elapsed())
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
        self.invalidated_at
            .push((c.calculation_started_at_utc_ms, result.detected_at_utc_ms));
        self.strategy.invalidated(c, result);
    }
}
impl Drop for Trace {
    fn drop(&mut self) {
        if let Some(sink) = &self.summary_sink {
            *sink.lock().unwrap() = Some(TraceSummary {
                decisions: std::mem::take(&mut self.decisions),
                sent: std::mem::take(&mut self.sent),
                invalidated: self.invalidated,
                invalidated_at: std::mem::take(&mut self.invalidated_at),
                pure_decide: self.pure_decide,
                decide_samples: std::mem::take(&mut self.decide_samples),
                public_qualification: self.public_qualification.get(),
                full_qualification: self.full_qualification,
                next_health_evaluation: self.next_health_evaluation.get(),
            });
        }
    }
}
// 两个有限代表入口都走生产 day 链。仅收集包装器观测，不改变业务返回值。
fn run_day_trace(
    config: &HistoricalRunConfigV1,
    rules: Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
    c: ComplementaryPairConfigV2,
    root: &Path,
) -> (serde_json::Value, TraceSummary) {
    run_day_trace_with_research(config, rules, c, root, false)
}
fn run_day_trace_with_research(
    config: &HistoricalRunConfigV1,
    rules: Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
    c: ComplementaryPairConfigV2,
    _root: &Path,
    research: bool,
) -> (serde_json::Value, TraceSummary) {
    let mut candidate = HistoricalPreparedRunConfigV1::from(config);
    candidate.strategy.effective_parameters = serde_json::to_value(&c).unwrap();
    candidate.strategy_request_namespace = Some("health-history".into());
    let day = HistoricalDayConfigV1 {
        preparation: config.preparation.clone(),
        candidates: vec![candidate],
        progress_interval_seconds: 0,
        producer_lead_groups: None,
    };
    let sink = Arc::new(std::sync::Mutex::new(None));
    let factory = |_: &HistoricalPreparedRunConfigV1| -> Result<Trace, MaterialError> {
        let trace = Trace {
            strategy: ComplementaryPairV1::new(
                c.clone(),
                rules.clone(),
                "health-history".into(),
                StrategyContinuityV2::new_account(),
            )?,
            decisions: vec![],
            sent: vec![],
            invalidated: 0,
            pure_decide: Default::default(),
            decide_samples: Vec::new(),
            public_qualification: Default::default(),
            full_qualification: Default::default(),
            next_health_evaluation: Default::default(),
            invalidated_at: vec![],
            summary_sink: Some(sink.clone()),
            qualified_at: Default::default(),
            qualification_invalidations: vec![],
        };
        assert_eq!(
            trace.requires_trade_economics_details(),
            trace.strategy.requires_trade_economics_details()
        );
        assert!(!trace.requires_trade_economics_details());
        Ok(trace)
    };
    let results = run_historical_days_with_options_v1(
        &[day],
        rules.clone(),
        &factory,
        1,
        &HistoricalDayRunOptionsV1 {
            research_summary: research,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(results[0].public_applied_inputs > 0);
    let report = results[0].candidates[0].report.clone();
    let trace = sink.lock().unwrap().take().unwrap();
    assert!(
        trace.pure_decide.count > 0
            && trace.public_qualification.count > 0
            && trace.full_qualification.count > 0
            && trace.next_health_evaluation.count > 0
    );
    (report, trace)
}
fn strategy_config() -> ComplementaryPairConfigV2 {
    let mut c = ComplementaryPairConfigV2::parse_toml(include_str!(
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
    fixture_with_reference(root, price, spot_times, false)
}
fn fixture_with_reference(
    root: &Path,
    price: &str,
    spot_times: &[(i64, i64)],
    twap_enabled: bool,
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
    let body = if twap_enabled {
        r#"{"eventMetadata":{"priceToBeat":100000},"twapEnabled":true,"twapLookbackSeconds":30}"#
    } else {
        r#"{"eventMetadata":{"priceToBeat":100000},"twapEnabled":false}"#
    };
    // TWAP反例在终局积分窗口前形成候选，不用滚动TWAP伪造已发生积分。
    let first_end = if twap_enabled {
        BASE + 40000
    } else {
        BASE + 10000
    };
    let mut anchor: MarketAnchorRuleUpdateRow = serde_json::from_value(json!({
        "timestamp_received":BASE+50,"metadata_origin":"gamma_event_by_slug","collector_host_id":"synthetic-collector","collector_region":"test",
        "slug":"btc-updown-5m-engineering","condition_id":FIRST,"clob_market_id":"3195576","asset_ids":"[\"20\",\"3\"]",
        "market_start_utc":BASE+1000,"market_end_utc":first_end,"fixed_identity_verified_at_utc":BASE-1,
        "market_lifecycle_state":"prefetched","market_runtime_role":"next","metadata_fetch_start_utc":BASE+40,
        "anchor_query_source_epoch_id":"synthetic-query","anchor_query_ordinal":1,"anchor_query_result":"observed",
        "price_to_beat_decimal":"100000","price_to_beat_source_field":"eventMetadata.priceToBeat","twap_enabled":twap_enabled,
        "twap_lookback_seconds":twap_enabled.then_some(30),
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
                first_end,
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
    c: ComplementaryPairConfigV2,
) -> (HistoricalRunReportV1, Trace) {
    let mut config = config.clone();
    config.strategy.effective_parameters = serde_json::to_value(&c).unwrap();
    let mut trace = Trace {
        strategy: ComplementaryPairV1::new(
            c,
            rules.clone(),
            "health-history".into(),
            StrategyContinuityV2::new_account(),
        )
        .unwrap(),
        decisions: vec![],
        sent: vec![],
        invalidated: 0,
        pure_decide: Default::default(),
        decide_samples: Vec::new(),
        public_qualification: Default::default(),
        full_qualification: Default::default(),
        next_health_evaluation: Default::default(),
        invalidated_at: vec![],
        summary_sink: None,
        qualified_at: std::collections::BTreeMap::new(),
        qualification_invalidations: vec![],
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

#[tokio::test(flavor = "current_thread")]
async fn real_strategy_pending_twap_expiry_recalculates_after_spot_arrival() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, rules) = fixture_with_reference(temp.path(), "100000", &[(2300, 2300)], true);
    let mut dispatch_delays = vec![200; 32];
    dispatch_delays[3] = 1000;
    config.timing.dispatch_delay_ms = DelayScheduleV1::Sequence(dispatch_delays);
    replace_books(
        &config,
        &[
            book("20", 1100, 1100, 1, "epoch-a", "0.7"),
            book("3", 1100, 1100, 2, "epoch-a", "0.4"),
        ],
    );
    let manifest_path = &config.preparation.scope.manifest_paths[0];
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(manifest_path).unwrap()).unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        manifest_path.parent().unwrap(),
        "health-twap",
        RTDS_CHAINLINK_TWAP_SCHEMA_VERSION,
        "polymarket_rtds_chainlink_twap",
    )
    .unwrap();
    writer
        .write_rtds_chainlink_twap_rows_with_logical_bytes(
            &[RtdsChainlinkTwapHourlyRow {
                common: RtdsChainlinkBtcUsdHourlyRow {
                    timestamp_received: BASE + 2100,
                    message_timestamp: BASE + 2100,
                    payload_timestamp: BASE + 2100,
                    age_ms: Some(0),
                    symbol: "btc/usd".into(),
                    value: "100000".into(),
                    topic: "crypto_prices_twap_thirty".into(),
                    message_type: "update".into(),
                    freshness_status: "fresh".into(),
                    subscription_filter: "{}".into(),
                    source_native_payload_json: json!({
                        "topic":"crypto_prices_twap_thirty","type":"update","timestamp":BASE+2100,
                        "payload":{"symbol":"btc/usd","timestamp":BASE+2100,"value":"100000",
                            "full_accuracy_value":"100000000000000000000000","window_s":30}
                    })
                    .to_string(),
                    collector_host_id: "synthetic-collector".into(),
                    collector_region: "test".into(),
                    source_endpoint: "wss://ws-live-data.polymarket.com".into(),
                    schema_version: RTDS_CHAINLINK_TWAP_SCHEMA_VERSION.into(),
                },
                full_accuracy_value: "100000000000000000000000".into(),
                window_s: 30,
                source_sequence_epoch_id: "synthetic-twap-epoch".into(),
                collector_admission_sequence: 1,
                collector_admission_time_utc_ms: BASE + 2101,
            }],
            1024,
        )
        .unwrap();
    let twap_file = finish(
        writer,
        "polymarket_rtds_chainlink_twap",
        RTDS_CHAINLINK_TWAP_SCHEMA_VERSION,
        1,
    );
    manifest["files"].as_array_mut().unwrap().push(twap_file);
    std::fs::write(manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let mut c = strategy_config();
    c.health.chainlink_silence_ms = 500;
    c.health.chainlink_event_delay_ms = 500;
    let (report, trace) = run(&config, rules, c).await;
    let frozen_at = BASE + 2201;
    let expiry_at = BASE + 2600;
    println!(
        "twap_switch_calculations={:?}",
        trace
            .decisions
            .iter()
            .map(|(at, d, i)| (
                at - BASE,
                i,
                d.action_health_dependencies
                    .iter()
                    .flat_map(|a| a.sources.iter().map(|s| s.source))
                    .collect::<Vec<_>>()
            ))
            .collect::<Vec<_>>()
    );
    assert!(trace.decisions.iter().any(|(at, d, intents)| {
        *at == frozen_at
            && intents.iter().any(|kind| kind == "pair")
            && d.action_health_dependencies.iter().any(|a| {
                a.sources
                    .iter()
                    .any(|s| s.source == SourceStreamKindV1::ChainlinkTwapThirty)
            })
    }));
    println!(
        "twap_switch_qualification_invalidations={:?} recalculation_starts={:?}",
        trace
            .qualification_invalidations
            .iter()
            .map(|(start, at)| (start - BASE, at - BASE))
            .collect::<Vec<_>>(),
        trace
            .decisions
            .iter()
            .map(|(at, _, _)| at - BASE)
            .collect::<Vec<_>>()
    );
    assert!(
        trace
            .qualification_invalidations
            .contains(&(frozen_at, expiry_at)),
        "冻结TWAP期限必须在没有新消息时发现，而不是等spot到期或最终提交"
    );
    assert!(
        trace
            .decisions
            .iter()
            .any(|(at, d, intents)| *at == expiry_at
                && intents.iter().any(|kind| kind == "pair")
                && d.action_health_dependencies.iter().any(|a| {
                    a.sources
                        .iter()
                        .any(|s| s.source == SourceStreamKindV1::ChainlinkSpot)
                        && !a
                            .sources
                            .iter()
                            .any(|s| s.source == SourceStreamKindV1::ChainlinkTwapThirty)
                })),
        "原宿主在失效发现后最早合法时刻重算，不等新消息"
    );
    assert!(report.source_health.unwrap().deadline_wakeups > 0);
}

/// 真实请求的有限重复测量；分别报告期限/资格回调实耗，不将整轮耗时差称为纯检查开销。
#[test]
#[ignore]
fn real_strategy_finite_health_callback_measurement() {
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
    let (report, trace) = run_day_trace(&config, rules, c, &root);
    let elapsed_ns = started.elapsed().as_nanos();
    let health = &report["source_health"];
    assert!(
        health["qualification_calls"].as_u64().unwrap() > 0
            && trace.sent.iter().any(|(_, kind)| kind == "maker")
    );
    let current_trade_count = report["account_economics_memory"]["markets"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| {
            m["success_shares"]
                .as_array()
                .unwrap()
                .iter()
                .any(|q| q.as_str().unwrap() != "0")
        })
        .count();
    assert!(
        current_trade_count > 0,
        "新增当前费用读取实际消费非空原交易事实"
    );
    let measurement = json!({"purpose":"有限合成工程实耗，不是盈利或在线性能结论", "input_manifest_sha256":identity,
        "whole_run_elapsed_ns":elapsed_ns, "decide_samples":trace.decide_samples, "callbacks":{"pure_decide":trace.pure_decide,"public_qualification":trace.public_qualification,"full_qualification":trace.full_qualification,"next_health_evaluation":trace.next_health_evaluation}, "qualification_calls":health["qualification_calls"],
        "deadline_wakeups":health["deadline_wakeups"], "invalidated_batches":health["invalidated_batches"],
        "decisions":trace.decisions.len(), "markets_with_actual_trades":current_trade_count,"actual_requests":trace.sent,
        "timing_note":"生产day有限合成负载；actual_requests为发送前资格的at，decisions为计算开始；回调仅计原委托调用，嵌套/并行阶段不相加；不是旧小时调用分布、在线尾延迟或线上积压证据"});
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

#[tokio::test(flavor = "current_thread")]
async fn real_strategy_normal_boundary_keeps_risk_orders_and_max_boundary_cancels() {
    for (normal, max) in [(500, 1500), (1500, 1500), (0, 0)] {
        let temp = tempfile::tempdir().unwrap();
        let (config, rules) = fixture(temp.path(), "100000", &[800, 5000]);
        let mut c = strategy_config();
        c.health.chainlink_silence_normal_ms = normal;
        c.health.chainlink_silence_ms = max;
        c.health.chainlink_event_delay_normal_ms = normal;
        c.health.chainlink_event_delay_ms = max;
        if max == 0 {
            c.health.polymarket_silence_ms = 0;
            c.health.polymarket_event_delay_ms = 0;
            c.health.binance_trade_silence_ms = 0;
            c.health.binance_trade_event_delay_ms = 0;
            c.health.binance_depth_silence_ms = 0;
            c.health.binance_depth_event_delay_ms = 0;
        }
        let (_report, trace) = run(&config, rules, c).await;
        if normal < max {
            assert!(
                trace
                    .decisions
                    .iter()
                    .any(
                        |(_, d, _)| d.markets.iter().any(|m| m.source_health.is_empty()
                            && m.source_health_states
                                .iter()
                                .any(|s| s.scope.source == SourceStreamKindV1::ChainlinkSpot
                                    && s.state == SourceHealthStateV1::Elevated))
                    )
            );
            assert!(
                !trace
                    .sent
                    .iter()
                    .any(|(at, k)| *at < BASE + 2300 && k == "cancel")
            );
            assert!(
                trace
                    .sent
                    .iter()
                    .any(|(at, k)| *at >= BASE + 2300 && k == "cancel")
            );
            assert!(trace.sent.iter().any(|(_, k)| k == "pair"));
        } else {
            assert!(
                trace
                    .decisions
                    .iter()
                    .all(|(_, d, _)| d.markets.iter().all(|m| m
                        .source_health_states
                        .iter()
                        .all(|s| s.scope.source != SourceStreamKindV1::ChainlinkSpot
                            || s.state != SourceHealthStateV1::Elevated)))
            );
        }
        assert!(!trace.decisions.is_empty());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn same_millisecond_epoch_failure_then_recovery_invalidates_due_frozen_pair_once() {
    let temp = tempfile::tempdir().unwrap();
    let (config, rules) = same_millisecond_fixture(temp.path());
    let (report, trace) = run(&config, rules, strategy_config()).await;
    println!(
        "scenario_decisions={:?}",
        trace
            .decisions
            .iter()
            .map(|(at, d, intents)| (
                at - BASE,
                d.markets
                    .iter()
                    .map(|m| (&m.probability, &m.choice, &m.source_health))
                    .collect::<Vec<_>>(),
                intents
            ))
            .collect::<Vec<_>>()
    );
    let health = report.source_health.unwrap();
    assert_eq!(trace.invalidated, 1);
    assert_eq!(health.invalidated_batches, 1);
    let invalidated = health.last_invalidated_calculation_timing.unwrap();
    assert_eq!(invalidated.scheduled_send_at_utc_ms, BASE + 1701);
    assert!(invalidated.actual_sent_at_utc_ms.is_none());
    assert!(
        !trace.sent.iter().any(|(at, _)| *at == BASE + 1701),
        "旧冻结请求不发送"
    );
    assert!(
        trace
            .decisions
            .iter()
            .any(|(at, d, intents)| *at == BASE + 1701
                && !intents.is_empty()
                && d.markets.iter().all(|m| m.source_health.is_empty())),
        "消费同毫秒恢复组后在最早合法时刻重算"
    );
    assert!(
        trace
            .sent
            .iter()
            .any(|(at, kind)| *at == BASE + 2101 && kind == "pair"),
        "新计算继续支付原200ms计算和200ms发送等待"
    );
}

fn same_millisecond_fixture(
    root: &Path,
) -> (
    HistoricalRunConfigV1,
    Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
) {
    let (mut config, rules) = fixture(root, "100000", &[800]);
    // 初始双腿asks均0.6满足原经营比较；健康配对在1301计算、1701发送到期。
    // 新epoch的两条独立消息均公开于1701，先要求另一腿快照，后恢复。
    config.preparation.scope.received_end_utc_ms = BASE + 2200;
    replace_books(
        &config,
        &[
            book("20", 1100, 1100, 1, "epoch-a", "0.6"),
            book("3", 1100, 1100, 2, "epoch-a", "0.6"),
            book("20", 1600, 1600, 1, "epoch-b", "0.6"),
            book("3", 1600, 1600, 2, "epoch-b", "0.6"),
        ],
    );
    (config, rules)
}
#[test]
#[ignore]
fn same_millisecond_day_stage_measurement() {
    let root = std::path::PathBuf::from(std::env::var("STRATEGY_HEALTH_MEASUREMENT_ROOT").unwrap());
    std::fs::create_dir(&root).unwrap();
    let (config, rules) = same_millisecond_fixture(&root);
    let started = Instant::now();
    let (report, trace) = run_day_trace(&config, rules, strategy_config(), &root);
    let elapsed = started.elapsed().as_nanos();
    let health = &report["source_health"];
    assert_eq!(trace.invalidated_at, vec![(BASE + 1301, BASE + 1701)]);
    assert_eq!(health["invalidated_batches"], 1);
    assert!(!trace.sent.iter().any(|(at, _)| *at == BASE + 1701));
    assert!(
        trace
            .decisions
            .iter()
            .any(|(at, d, intents)| *at == BASE + 1701
                && !intents.is_empty()
                && d.markets.iter().all(|m| m.source_health.is_empty()))
    );
    assert!(
        trace
            .sent
            .iter()
            .any(|(at, kind)| *at == BASE + 2101 && kind == "pair")
    );
    let measurement = json!({"purpose":"生产day有限同毫秒失效夹具，不是旧小时分布或线上尾延迟", "whole_run_elapsed_ns":elapsed,"callbacks":{"pure_decide":trace.pure_decide,"public_qualification":trace.public_qualification,"full_qualification":trace.full_qualification,"next_health_evaluation":trace.next_health_evaluation},"invalidated_at":trace.invalidated_at,"actual_requests":trace.sent,"decisions":trace.decisions.len()});
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

#[test]
fn research_counts_successful_decisions_even_when_frozen_batch_is_invalidated() {
    let temp = tempfile::tempdir().unwrap();
    let (config, rules) = same_millisecond_fixture(temp.path());
    let (report, trace) =
        run_day_trace_with_research(&config, rules, strategy_config(), temp.path(), true);
    assert_eq!(trace.invalidated, 1);
    let counts = &report["research"]["decisions"];
    let no_trade = trace
        .decisions
        .iter()
        .filter(|(_, _, intents)| intents.is_empty())
        .count();
    assert_eq!(counts["successful"], trace.decisions.len());
    assert_eq!(counts["no_trade"], no_trade);
    assert_eq!(counts["actions"], trace.decisions.len() - no_trade);
    assert_eq!(counts["exits"], 0);
    assert_eq!(counts["successful"], report["calculation_count"]);
}

impl Trace {
    fn record_qualification(
        &mut self,
        result: Result<IntentQualificationV1, StrategyErrorV1>,
        c: &StrategyCalculationV1<ComplementaryPairDiagnosticV1>,
        index: usize,
        at: i64,
        elapsed: std::time::Duration,
    ) -> Result<IntentQualificationV1, StrategyErrorV1> {
        self.full_qualification.record(elapsed);
        if matches!(result, Ok(IntentQualificationV1::Invalidated(_))) {
            self.qualification_invalidations
                .push((c.calculation_started_at_utc_ms, at));
        }
        if matches!(result, Ok(IntentQualificationV1::Qualified))
            && let StrategyOutputV1::Continue { intents, .. } = &c.output
        {
            self.qualified_at
                .insert(intents[index].context.global_request_id.clone(), at);
        }
        result
    }
    fn record_decision(
        &mut self,
        result: Result<StrategyOutputV1<ComplementaryPairDiagnosticV1>, StrategyErrorV1>,
        at: i64,
        elapsed: std::time::Duration,
    ) -> Result<StrategyOutputV1<ComplementaryPairDiagnosticV1>, StrategyErrorV1> {
        self.pure_decide.record(elapsed);
        let outcome = match &result {
            Ok(StrategyOutputV1::Continue { intents, .. }) if intents.is_empty() => "NoTrade",
            Ok(StrategyOutputV1::Continue { .. }) => "Actions",
            Ok(StrategyOutputV1::Exit { .. }) => "Exit",
            Err(_) => "Error",
        };
        self.decide_samples.push(DecideSample {
            started_at_utc_ms: at,
            elapsed_ns: elapsed.as_nanos(),
            outcome,
        });
        let result = result?;
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
}
