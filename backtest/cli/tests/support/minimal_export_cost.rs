//! 显式、有界的真实日入口成本夹具。不同模式由调用方顺序启动独立进程。
use super::*;
use std::collections::BTreeMap;

#[derive(Debug)]
struct FullNoop;
impl HistoricalPublicObserverV1 for FullNoop {
    fn requirements(&self) -> PublicObservationRequirementsV1 {
        PublicObservationRequirementsV1::FULL
    }
    fn observe(
        &self,
        _: &PublicGroupReferenceV1,
        _: &ClosedBusinessGroupV1,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError> {
        Ok(Arc::new(HistoricalPublicEvidenceV1 {
            scalars: Default::default(),
            request_evidence: Default::default(),
        }))
    }
}
fn files(root: &Path) -> Vec<std::path::PathBuf> {
    let mut out = vec![];
    for e in std::fs::read_dir(root).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            out.extend(files(&p));
        } else {
            out.push(p);
        }
    }
    out.sort();
    out
}
fn candidate(
    base: &HistoricalRunConfigV1,
    root: &Path,
    stream: usize,
    index: usize,
    c: &ComplementaryPairConfigV3,
    export: bool,
) -> HistoricalPreparedRunConfigV1 {
    std::fs::create_dir_all(root).unwrap();
    let mut run = HistoricalPreparedRunConfigV1::from(base);
    run.session.run_id = format!("cost-{stream}-{index}");
    run.session.session_id = format!("session-{stream}-{index}");
    run.session.account_id = format!("account-{stream}-{index}");
    run.strategy_request_namespace = Some(format!("requests-{stream}-{index}"));
    let mut c = c.clone();
    if index == 1 {
        c.quote.base_half_spread = Decimal::ZERO;
        c.quote.uncertainty_spread_weight = Decimal::ZERO;
    }
    run.strategy.effective_parameters = serde_json::to_value(c).unwrap();
    run.timing = HistoricalTimingV1 {
        decision_mode: backtest::HistoricalDecisionModeV1::EventDriven,
        public_delivery_delay_ms: DelayScheduleV1::Fixed(0),
        calculation_start_delay_ms: DelayScheduleV1::Fixed(0),
        calculation_duration_ms: DelayScheduleV1::Fixed(15),
        dispatch_delay_ms: DelayScheduleV1::Fixed(0),
    };
    run.sim_log = root.join("sim.jsonl");
    run.noe_root = root.join("noe");
    run.ledger_root = root.join("ledger");
    run.metadata_path = root.join("metadata.json");
    run.result_path = root.join("result.json");
    run.research_evidence_path = export.then(|| root.join("candidate.jsonl"));
    run
}
fn prepare(
    root: &Path,
    scale: usize,
) -> (
    HistoricalRunConfigV1,
    Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
) {
    let times = (0..96 * scale)
        .map(|i| 3400 + (i as i64) * 30 / (scale as i64))
        .collect::<Vec<_>>();
    let mut times = times;
    times.insert(0, 800);
    let (config, rules) = fixture(root, "100010", &times);
    add_binance(&config);
    let n = 128 * scale;
    let mut up = book("20", 1100, 1100, 1, "epoch-a", "0.7");
    let mut down = book("3", 1100, 1100, 2, "epoch-a", "0.6");
    for msg in [&mut up, &mut down] {
        let PolymarketOrderbookV3Event::Book(b) = &mut msg.event else {
            unreachable!()
        };
        b.bids.extend((0..n).map(|i| PolymarketPriceLevelV3 {
            price: format!("0.{:06}", 100000 + i),
            size: "10".into(),
        }));
        b.asks.extend((0..n).map(|i| PolymarketPriceLevelV3 {
            price: format!("0.{:06}", 800000 + i),
            size: "10".into(),
        }));
    }
    let mut books = vec![up, down, book("3", 1300, 1300, 3, "epoch-a", "0.4")];
    books.push(up_bid_change(2200, 2200, 4, "100"));
    books.push(up_bid_change(3000, 2250, 5, "95"));
    for i in 0..96 * scale {
        // 多个价格的来源持续更新；最优报价与策略诊断通常不变。
        let at = 3400 + (i as i64) * 30 / (scale as i64);
        let mut msg = up_bid_change(at, at, i as u64 + 6, "11");
        let PolymarketOrderbookV3Event::PriceChange(c) = &mut msg.event else {
            unreachable!()
        };
        c.price_changes[0].price = format!("0.{:06}", 100000 + (i % n));
        books.push(msg);
    }
    books.push(book("20", 6000, 6000, 20000, "epoch-a", "0.7"));
    replace_books(&config, &books);
    (config, rules)
}
pub(super) fn trace(
    strategy: ComplementaryPairV1,
    sink: Arc<std::sync::Mutex<Option<TraceSummary>>>,
) -> Trace {
    Trace {
        strategy,
        decisions: vec![],
        sent: vec![],
        exact_decisions: vec![],
        exact_receipts: vec![],
        invalidated: 0,
        pure_decide: Default::default(),
        decide_samples: vec![],
        public_qualification: Default::default(),
        full_qualification: Default::default(),
        next_health_evaluation: Default::default(),
        invalidated_at: vec![],
        summary_sink: Some(sink),
        qualified_at: Default::default(),
        qualification_invalidations: vec![],
    }
}
#[test]
#[ignore = "需显式指定独立工程产物目录和成本模式；只运行有界合成数据"]
fn bounded_dual_stream_dual_candidate_cost() {
    let root = std::path::PathBuf::from(
        std::env::var_os("RESEARCH_COST_ROOT").expect("RESEARCH_COST_ROOT"),
    );
    let mode = std::env::var("RESEARCH_COST_MODE").unwrap();
    assert!(["off", "full", "new", "old"].contains(&mode.as_str()));
    let scale = std::env::var("RESEARCH_COST_SCALE")
        .unwrap_or("1".into())
        .parse::<usize>()
        .unwrap();
    assert!((1..=2).contains(&scale));
    std::fs::create_dir_all(&root).unwrap();
    let inputs = root.join("inputs");
    std::fs::create_dir_all(&inputs).unwrap();
    let config_path = root.join("synthetic-config.json");
    let (config, rules) = if config_path.exists() {
        let config: HistoricalRunConfigV1 =
            serde_json::from_slice(&std::fs::read(&config_path).unwrap()).unwrap();
        let rule_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json");
        let sha = Sha256Hex(format!(
            "{:x}",
            Sha256::digest(std::fs::read(&rule_path).unwrap())
        ));
        let rules = sim_system::platform_rules::load_historical_process_snapshot_v3(
            sim_system::platform_rules::HistoricalProcessSnapshotRequestV1 {
                snapshot_path: rule_path,
                expected_file_sha256: sha,
                replay_causal_start_utc_ms: BASE,
            },
        )
        .unwrap();
        (config, rules)
    } else {
        let (config, rules) = prepare(&inputs, scale);
        std::fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();
        (config, rules)
    };

    let output = root.join(&mode);
    std::fs::create_dir(&output).unwrap();
    let export = mode == "new" || mode == "old";
    let observer = if export {
        Some(Arc::new(
            streaming_research::StreamingResearchV1::open(&output.join("public")).unwrap(),
        ) as Arc<dyn HistoricalPublicObserverV1>)
    } else if mode == "full" {
        Some(Arc::new(FullNoop) as Arc<dyn HistoricalPublicObserverV1>)
    } else {
        None
    };
    let sinks = Arc::new(std::sync::Mutex::new(BTreeMap::new()));
    let mut c = strategy_config();
    if let Ok(reference) = std::env::var("INITIAL_PAIR_COST_REFERENCE") {
        c.quote.initial_pair_price_reference = serde_json::from_value(json!(reference)).unwrap();
    }
    c.health.chainlink_silence_ms = 1500;
    c.repair.enable_active_repair = false;
    c.repair.residual_risk_weight = Decimal::from(10);
    c.risk.per_market_orphan_limit_pusd = Decimal::from(1_000_000_000u64);
    c.risk.account_orphan_limit_pusd = Decimal::from(1_000_000_000u64);
    let days = (0..2)
        .map(|stream| {
            let mut preparation = config.preparation.clone();
            preparation.header.input_stream_id = format!("cost-stream-{stream}");
            preparation.work_root = output.join(format!("work-{stream}"));
            HistoricalDayConfigV1 {
                preparation,
                candidates: (0..2)
                    .map(|i| {
                        candidate(
                            &config,
                            &output.join(format!("candidate-{stream}-{i}")),
                            stream,
                            i,
                            &c,
                            export,
                        )
                    })
                    .collect(),
                progress_interval_seconds: 0,
                producer_lead_groups: Some(2048),
            }
        })
        .collect::<Vec<_>>();
    let factory = |run: &HistoricalPreparedRunConfigV1| -> Result<Trace, MaterialError> {
        let sink = Arc::new(std::sync::Mutex::new(None));
        sinks
            .lock()
            .unwrap()
            .insert(run.session.run_id.clone(), sink.clone());
        Ok(trace(
            ComplementaryPairV1::new(
                serde_json::from_value(run.strategy.effective_parameters.clone())?,
                rules.clone(),
                run.strategy_request_namespace.clone().unwrap(),
                StrategyContinuityV2::new_account(),
            )?,
            sink,
        ))
    };
    let before = ProcessCountersV1::read().ok();
    let hz = process_clock_ticks_per_second_v1();
    let started = Instant::now();
    let results = run_historical_days_with_options_v1(
        &days,
        rules.clone(),
        &factory,
        2,
        &HistoricalDayRunOptionsV1 {
            research_summary: true,
            public_observer: observer,
            ..Default::default()
        },
    )
    .unwrap();
    let wall = started.elapsed().as_secs_f64();
    let after = ProcessCountersV1::read().ok();
    let cost = process_measurement_v1(before.as_ref(), after.as_ref(), hz, wall);
    let mut business = BTreeMap::new();
    let mut evaluations = 0;
    for (id, sink) in sinks.lock().unwrap().iter() {
        let trace = sink.lock().unwrap().take().unwrap();
        evaluations += trace.exact_decisions.len();
        business.insert(
            id.clone(),
            json!({"evaluations":trace.exact_decisions,"receipts":trace.exact_receipts}),
        );
    }
    // 全报告比较，包含精确账本/费用与账户原事实，不以NAV单值代替。
    let reports = results
        .iter()
        .map(|d| {
            d.candidates
                .iter()
                .map(|c| json!({"run_id":c.run_id,"report":c.report,"outcome":c.outcome}))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    std::fs::write(
        output.join("precheck.json"),
        serde_json::to_vec(&json!({"business":business,"reports":reports})).unwrap(),
    )
    .unwrap();
    assert!(
        c.quote.initial_pair_price_reference == InitialPairPriceReferenceV1::BestBid
            || results.iter().flat_map(|d| &d.candidates).any(|c| {
                c.report["account_economics_memory"]["markets"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|m| {
                        m["success_shares"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|q| q.as_str().unwrap() != "0")
                    })
            }),
        "公平价兼容夹具必须存在可独立核对的实际成交；买一模式按真实结果记录零成交"
    );
    // SIM 原记录精确比较；NOE/账本耐久记录也保留，比较时排除元数据中的运行路径。
    let mut durable = BTreeMap::new();
    for path in files(&output)
        .into_iter()
        .filter(|p| p.file_name().unwrap() == "sim.jsonl")
    {
        let raw = std::fs::read(&path).unwrap();
        assert_eq!(&raw[..8], b"SIMDAY01");
        let mut position = 8;
        let mut values = vec![];
        while position < raw.len() {
            let length =
                u64::from_le_bytes(raw[position..position + 8].try_into().unwrap()) as usize;
            position += 8;
            let value: serde_json::Value =
                ciborium::from_reader(&raw[position..position + length]).unwrap();
            values.push(value);
            position += length;
        }
        durable.insert(
            path.strip_prefix(&output)
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            values,
        );
    }
    let mut public_bytes = 0;
    let mut candidate_bytes = 0;
    for p in files(&output) {
        let n = std::fs::metadata(&p).unwrap().len();
        if p.parent()
            .is_some_and(|p| p.file_name().is_some_and(|n| n == "public"))
        {
            public_bytes += n;
        } else if p.file_name().unwrap() == "candidate.jsonl" {
            candidate_bytes += n;
        }
    }
    std::fs::write(
        output.join("business.json"),
        serde_json::to_vec(&json!({"business":business,"reports":reports,"sim":durable})).unwrap(),
    )
    .unwrap();
    let summary = json!({"mode":mode,"price_reference":c.quote.initial_pair_price_reference,"scale":scale,"streams":2,"candidates_per_stream":2,"concurrent_worker_units":6,"input_count_per_stream":results.iter().map(|r|r.public_applied_inputs).collect::<Vec<_>>(),"pm_levels_per_side":n_levels(scale),"pm_source_changes_per_stream":96*scale,"evaluations":evaluations,"public_bytes":public_bytes,"candidate_bytes":candidate_bytes,"research_bytes":public_bytes+candidate_bytes,"cost":cost});
    std::fs::write(
        output.join("cost.json"),
        serde_json::to_vec_pretty(&summary).unwrap(),
    )
    .unwrap();
    println!("cost={summary}");
}
fn n_levels(scale: usize) -> usize {
    128 * scale + 1
}
