//! 新候选沿正式 DPT / Bridge / SIM / NOE / Ledger 的有限场景。
#[path = "../../shared-run/tests/common/fixture_clock.rs"]
mod fixture_clock;
#[path = "../../shared-run/tests/common/fixture_execution.rs"]
mod fixture_execution;
#[path = "../../shared-run/tests/common/fixture_execution_v4.rs"]
mod fixture_execution_v4;
#[path = "../../shared-run/tests/common/fixture_input.rs"]
mod fixture_input;
#[path = "../../shared-run/tests/common/fixture_pair.rs"]
mod fixture_pair;
#[path = "../../shared-run/tests/common/fixture_policy.rs"]
mod fixture_policy;
#[path = "../../shared-run/tests/common/fixture_recording.rs"]
mod fixture_recording;
#[path = "../../shared-run/tests/common/fixture_reference.rs"]
mod fixture_reference;
#[path = "../../shared-run/tests/common/fixture_terminal.rs"]
mod fixture_terminal;

#[path = "support/inventory_execution.rs"]
mod inventory_execution;
use baseline_mm::*;
use contracts::*;
use data_pipeline_core::{public_input::*, public_observation_core::*, typed_raw::*};
use data_pipeline_training::public_recording::PublicRecordingWriterV1;
use fixture_clock::TradingTimes;
use ledger::*;
use rust_decimal::Decimal;
use shared_run::*;
use sim_system::platform_rules::*;
use sim_system::{execution::*, public_consumption::*};
fn engineering_config() -> ContinuousInventoryConfigV1 {
    let c = ContinuousInventoryConfigV1::parse_toml(include_str!(
        "../configs/strategies/continuous_inventory/adopted-v1.toml"
    ))
    .unwrap();
    let mut value = serde_json::to_value(c).unwrap();
    for (key, n) in value["health"].as_object_mut().unwrap() {
        *n = serde_json::json!(if key.contains("normal") { 0 } else { 1_000_000 });
    }
    value["probability_reference"]["binance_innovation_weight"] = serde_json::json!(0.0);
    serde_json::from_value(value).unwrap()
}
const BASE: i64 = 1_787_443_200_000;
fn recorded_inventory(root: &std::path::Path) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_inventory_events(root, vec![])
}
fn recorded_inventory_events(
    root: &std::path::Path,
    events: Vec<(i64, PolymarketOrderbookEvent)>,
) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_inventory_tail(root, events, false)
}
fn recorded_inventory_tail(
    root: &std::path::Path,
    events: Vec<(i64, PolymarketOrderbookEvent)>,
    terminal: bool,
) -> (PublicObservationCoreV1, TradingTimes) {
    use fixture_input::*;
    let mut core = core();
    let mut writer =
        PublicRecordingWriterV1::create(root, core.header(), fixture_recording::config()).unwrap();
    let mut snapshot = metadata_snapshot_row();
    snapshot.timestamp_received += BASE;
    snapshot.collector_admission_time_utc_ms += BASE;
    snapshot.market_start_utc += BASE;
    snapshot.market_end_utc += BASE;
    snapshot.fixed_identity_verified_at_utc += BASE;
    snapshot.source_evidence_received_at_utc =
        snapshot.source_evidence_received_at_utc.map(|t| t + BASE);
    snapshot.row_evidence_validation_completed_at_utc = snapshot
        .row_evidence_validation_completed_at_utc
        .map(|t| t + BASE);
    snapshot.metadata_fetch_start_utc = snapshot.metadata_fetch_start_utc.map(|t| t + BASE);
    let initial = record(
        0,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
            snapshot.clone(),
        ))),
    );
    let mut active = snapshot;
    active.timestamp_received = BASE + 20_000;
    active.collector_admission_sequence = 2;
    active.collector_admission_time_utc_ms = BASE + 20_001;
    active.metadata_fetch_start_utc = Some(BASE + 19_990);
    active.source_evidence_received_at_utc = Some(BASE + 20_000);
    active.row_evidence_validation_completed_at_utc = Some(BASE + 20_001);
    active.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    active.market_runtime_role = MetadataMarketRuntimeRole::Current;
    let mut down = book("100");
    if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut down {
        *asset_id = DOWN_TOKEN_ID.into();
    }
    let mut spot = fixture_reference::reference_after(BASE + 21_000, 1).common;
    spot.schema_version = POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into();
    spot.topic = "crypto_prices_chainlink".into();
    spot.source_native_payload_json=serde_json::json!({"topic":"crypto_prices_chainlink","type":"update","timestamp":BASE+20_999,"payload":{"symbol":"btc/usd","timestamp":BASE+20_998,"value":"64950.01"}}).to_string();
    let mut inputs = vec![
        initial,
        record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(active))),
        ),
        record(
            2,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                1,
                BASE + 21_000,
                BASE + 22_000,
                book("100"),
            ))),
        ),
        record(
            3,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                2,
                BASE + 21_000,
                BASE + 22_000,
                down,
            ))),
        ),
        record(
            4,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(spot)),
        ),
    ];
    let mut visible = vec![
        BASE + 10_000,
        BASE + 22_000,
        BASE + 22_000,
        BASE + 22_000,
        BASE + 22_000,
    ];
    for (at, event) in events {
        let position = inputs.len() as u64;
        inputs.push(record(
            position,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                position - 2,
                BASE + at,
                BASE + at + 1,
                event,
            ))),
        ));
        visible.push(BASE + at + 1_000);
    }
    if terminal {
        let market = fixture_input::market();
        let mut metadata = fixture_terminal::terminal_metadata_row(&market);
        fn shift(v: &mut serde_json::Value) {
            if let Some(fields) = v.as_object_mut() {
                for (key, value) in fields {
                    if (key.ends_with("_utc")
                        || key.ends_with("_utc_ms")
                        || key == "timestamp_received")
                        && value.is_i64()
                    {
                        *value = serde_json::json!(value.as_i64().unwrap() + BASE);
                    }
                }
            }
        }
        let mut value = serde_json::to_value(&metadata).unwrap();
        shift(&mut value);
        value["ws_platform_timestamp_raw"] = serde_json::json!((BASE + 31_000).to_string());
        metadata = serde_json::from_value(value).unwrap();
        let position = inputs.len() as u64;
        inputs.push(record(
            position,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata))),
        ));
        visible.push(BASE + 31_003);
        let mut ws = fixture_terminal::terminal_orderbook_row(&market);
        ws.timestamp = Some(BASE + 31_000);
        ws.timestamp_received += BASE;
        ws.collector_admission_time_utc_ms += BASE;
        inputs.push(record(
            position + 1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(ws)),
        ));
        visible.push(BASE + 31_003);
    }
    visible.push(*visible.last().unwrap());
    inputs.push(fixture_recording::end(inputs.len() as u64));
    let mut times = Vec::new();
    for input in inputs {
        let result = fixture_recording::apply(&mut core, &input);
        for group in &result.applied.closed_groups {
            times.push(visible[group.closed_at_input_position as usize]);
        }
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    (core, TradingTimes(times))
}
fn v2_rules(temp: &std::path::Path) -> std::sync::Arc<SealedProcessRuleSnapshotV3> {
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json");
    derive_historical_maker_v2_snapshot_v3(
        HistoricalProcessSnapshotRequestV1 {
            snapshot_path: source,
            expected_file_sha256: Sha256Hex(
                "d63c0e7b09edc4b2c4c3f0295f806b25f6f5fd03663cdf60ff062117e53e4e54".into(),
            ),
            replay_causal_start_utc_ms: BASE,
        },
        temp.join("rules-v2.json"),
        ImplementationIdentityV1 {
            git_commit: "a".repeat(40),
            source_identity_sha256: Sha256Hex("b".repeat(64)),
            cargo_lock_sha256: Sha256Hex("c".repeat(64)),
        },
    )
    .unwrap()
}
#[tokio::test(flavor = "current_thread")]
async fn no_probability_gate_initial_pair_and_no_feedback_no_duplicate() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = recorded_inventory(&root);
    let rules = v2_rules(temp.path());
    assert_eq!(
        rules.maker_model_version_for(SimCallerRoleV1::HistoricalBacktest),
        "PUBLIC_TRADE_CONSERVATIVE_FIFO_V2"
    );
    let (mut execution, observation, rules) = inventory_execution::execution(
        &temp.path().join("sim.jsonl"),
        &core,
        fixture_policy::SUCCESS_POLICY,
        rules,
        "2000",
        "inventory-fixture",
    );
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let mut source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    source.enable_source_progress();
    let mut driver =
        FinitePublicConsumptionV1::new(execution, source, "continuous-inventory".into());
    loop {
        driver
            .next_group()
            .unwrap()
            .expect("必要合成来源到达前不能EOF");
        if driver.frames_at_observation().iter().any(|frame| {
            frame.observation.chainlink_spot.value.is_some()
                && frame.observation.assets.len() == 2
                && frame
                    .observation
                    .assets
                    .iter()
                    .all(|a| a.bids.value.is_some() && a.asks.value.is_some())
        }) {
            break;
        }
    }
    let config = engineering_config();
    let mut strategy = ContinuousInventoryV1::new(
        config,
        rules,
        "inventory".into(),
        ContinuousInventoryContinuityV1::new_account(Decimal::from(2000)),
    )
    .unwrap();
    let input = shared.strategy_input(&mut driver).unwrap().unwrap();
    assert!(
        input.markets.iter().all(|m| m.statistics.is_none()),
        "不伪造C0或公平概率"
    );
    let at = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let calculation = input.calculate(&mut strategy, at).unwrap();
    let StrategyOutputV1::Continue {
        intents,
        diagnostics,
        ..
    } = &calculation.output
    else {
        panic!("新账户不终止")
    };
    assert_eq!(intents.len(), 1, "{diagnostics:?}");
    let ModelIntentV1::SubmitPairedBuy { orders, .. } = intents[0].intent.as_ref() else {
        panic!("首次等量")
    };
    for leg in orders {
        assert!(
            matches!(leg.order,BuyOrderIntentV1::GtcLimit {size_shares,post_only:true,..} if size_shares==Decimal::from(50))
        );
    }
    let sent = shared
        .dispatch_strategy_qualified(
            &mut driver.decision().unwrap(),
            &mut strategy,
            calculation.complete(at),
        )
        .await
        .unwrap();
    let QualifiedStrategyDispatchV1::Dispatched {
        dispatch: StrategyDispatchV1::Continue { receipts, .. },
        qualification_calls,
    } = sent
    else {
        panic!("合法冻结应通过发送前资格")
    };
    assert_eq!(receipts.len(), 1);
    assert_eq!(qualification_calls, 1);
    // 无平台接纳/成交反馈，原本地预留及receipt承诺已进入公共读取。
    assert_eq!(shared.registry().orders().count(), 0);
    let repeated = shared.strategy_input(&mut driver).unwrap().unwrap();
    let StrategyOutputV1::Continue {
        intents,
        diagnostics,
        ..
    } = strategy.decide(&repeated, at).unwrap()
    else {
        panic!("不终止")
    };
    assert!(intents.is_empty(), "{diagnostics:?}");
    assert_eq!(
        diagnostics.markets[0].coverage,
        [ExactDecimal("50".into()), ExactDecimal("50".into())]
    );
    assert_eq!(
        diagnostics.markets[0].deficit,
        [ExactDecimal("0".into()), ExactDecimal("0".into())]
    );
    let first_projection = strategy.research_compact_diagnostics(&diagnostics);
    assert_eq!(
        first_projection["obligation_changes"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let unchanged = strategy.research_compact_diagnostics(&diagnostics);
    assert!(unchanged["definition"].is_null());
    assert!(
        unchanged["obligation_changes"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        unchanged["no_longer_unclosed"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        first_projection["reservation_changes"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(
        unchanged["reservation_changes"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        strategy.research_request_markets(&diagnostics).is_empty(),
        "无BLB请求消费者"
    );
    assert_eq!(repeated.action_before.cash_available(), Decimal::from(1955));
    assert_eq!(
        diagnostics.markets[0].investment,
        contracts::economics::EconomicRational::from_decimal(Decimal::from(45)).public()
    );
    driver.stop_and_drain().unwrap();
}

fn dec(s: &str) -> Decimal {
    s.parse().unwrap()
}
fn limit(id: &str, token: &str, quantity: &str, price: &str) -> ReservedIntentV1 {
    let q = dec(quantity);
    let p = dec(price);
    ReservedIntentV1 {
        context: RequestContextV2 {
            global_request_id: id.into(),
            caller_correlation_id: None,
        },
        intent: std::sync::Arc::new(ModelIntentV1::SubmitSingleBuy {
            condition_id: ConditionId(fixture_input::CONDITION_ID.into()),
            order: BuyLegIntentV1 {
                token_id: TokenId(token.into()),
                order: BuyOrderIntentV1::GtcLimit {
                    limit_price_pusd_per_share: p,
                    size_shares: q,
                    post_only: true,
                },
            },
        }),
        reserves: vec![IntentReserveV1 {
            batch_item_ordinal: 0,
            reserve: ReserveIntentV1 {
                request_identity: LedgerRequestIdentityV1(format!("{id}-funds")),
                condition_id: ConditionId(fixture_input::CONDITION_ID.into()),
                requested_principal_pusd: ExactDecimal((q * p).to_string()),
                immediate_fee_reserve_pusd: ExactDecimal("0".into()),
            },
        }],
    }
}
fn quoted_book(token: &str, bid: &str, ask: &str) -> PolymarketOrderbookEvent {
    PolymarketOrderbookEvent::Book {
        asset_id: token.into(),
        bids: vec![PolymarketPriceLevel {
            price: bid.into(),
            size: "100".into(),
        }],
        asks: vec![PolymarketPriceLevel {
            price: ask.into(),
            size: "100".into(),
        }],
        last_trade_price: None,
    }
}
fn inventory_forensic_root() -> Option<std::ffi::OsString> {
    std::env::var_os("PAPER_HOST_TEST_OUTPUT_ROOT")
        .or_else(|| std::env::var_os("ISSUE360_FORENSIC_ROOT"))
}
type Driver = FinitePublicConsumptionV1<RecordedPublicGroupSourceV1<TradingTimes>>;
struct InventoryFixture {
    _temp: Option<tempfile::TempDir>,
    shared: SharedExecutionV2,
    driver: Driver,
    strategy: ContinuousInventoryV1,
    hidden: RecordedHiddenBucketSourceV1,
    next_hidden: Option<AcceptedHiddenStepBucketV1>,
}
impl InventoryFixture {
    fn new(
        events: Vec<(i64, PolymarketOrderbookEvent)>,
        config: ContinuousInventoryConfigV1,
    ) -> Self {
        Self::with_policy(events, config, fixture_policy::SUCCESS_POLICY)
    }
    fn with_policy(
        events: Vec<(i64, PolymarketOrderbookEvent)>,
        config: ContinuousInventoryConfigV1,
        policy: &[u8],
    ) -> Self {
        Self::with_tail(events, config, policy, false)
    }
    fn with_tail(
        events: Vec<(i64, PolymarketOrderbookEvent)>,
        config: ContinuousInventoryConfigV1,
        policy: &[u8],
        terminal: bool,
    ) -> Self {
        let forensic_root = inventory_forensic_root();
        if let Some(root) = &forensic_root {
            std::fs::create_dir_all(root).unwrap();
        }
        let temp = if let Some(root) = &forensic_root {
            tempfile::Builder::new()
                .prefix("same-maker-example-")
                .tempdir_in(root)
                .unwrap()
        } else {
            tempfile::tempdir().unwrap()
        };
        let root = temp.path().join("dpt");
        if forensic_root.is_some() {
            println!("forensic_material_root {}", temp.path().display());
            std::fs::write(temp.path().join("finality.json"), policy).unwrap();
            std::fs::write(
                temp.path().join("effective-config.json"),
                serde_json::to_vec_pretty(&config).unwrap(),
            )
            .unwrap();
            println!("forensic_seed {}", "e".repeat(64));
        }
        let (core, times) = recorded_inventory_tail(&root, events, terminal);
        let rules = v2_rules(temp.path());
        let (mut execution, observation, _) = inventory_execution::execution(
            &temp.path().join("sim.jsonl"),
            &core,
            policy,
            rules.clone(),
            "2000",
            "inventory-fixture",
        );
        let mut ledger = LedgerWriterV1::new_with_admission_v3(
            AccountAdmissionPolicyV3::ActualCashNoMarketLimit,
        );
        execution
            .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
            .unwrap();
        let shared = SharedExecutionV2::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let prepared = data_pipeline_training::hidden_preparation::prepare_hidden_v1(
            core,
            data_pipeline_training::public_recording::PublicRecordingReaderV1::recover(
                &root,
                1,
                1 << 20,
            )
            .unwrap(),
            &temp.path().join("hidden"),
            &fixture_recording::config(),
            "inventory-hidden".into(),
        )
        .unwrap();
        let mut hidden = RecordedHiddenBucketSourceV1::open(&prepared, &root, 1, 1 << 20).unwrap();
        let mut next_hidden = hidden.next_bucket().unwrap();
        let mut source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
        source.enable_source_progress();
        let mut driver =
            FinitePublicConsumptionV1::new(execution, source, "inventory-fixture".into());
        loop {
            next_combined(&mut driver, &mut hidden, &mut next_hidden);
            if driver.frames_at_observation().iter().any(|f| {
                f.observation.chainlink_spot.value.is_some()
                    && f.observation.assets.len() == 2
                    && f.observation
                        .assets
                        .iter()
                        .all(|a| a.bids.value.is_some() && a.asks.value.is_some())
            }) {
                break;
            }
        }
        let strategy = ContinuousInventoryV1::new(
            config,
            rules,
            "inventory".into(),
            ContinuousInventoryContinuityV1::new_account(Decimal::from(2000)),
        )
        .unwrap();
        let temp = if forensic_root.is_some() {
            let _ = temp.keep();
            None
        } else {
            Some(temp)
        };
        Self {
            _temp: temp,
            shared,
            driver,
            strategy,
            hidden,
            next_hidden,
        }
    }
    fn detached_calculation(&mut self) -> StrategyCalculationV1<ContinuousInventoryDiagnosticV1> {
        let input = self
            .shared
            .strategy_borrowed_observation_input(&mut self.driver, false)
            .unwrap()
            .unwrap()
            .freeze_for_detached();
        let at = input
            .action_before
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        let output = self.strategy.decide_detached(&input, at).unwrap();
        StrategyCalculationV1 {
            market_frame_timing: input.market_frame_timing,
            input_cutoff: input.action_before.cutoff().clone(),
            calculation_started_at_utc_ms: at,
            output,
        }
    }
    fn calculation(&mut self) -> StrategyCalculationV1<ContinuousInventoryDiagnosticV1> {
        let input = self
            .shared
            .strategy_observation_input(&mut self.driver)
            .unwrap()
            .unwrap();
        let at = input
            .action_before
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        input.calculate(&mut self.strategy, at).unwrap()
    }
    async fn dispatch(&mut self, c: StrategyCalculationV1<ContinuousInventoryDiagnosticV1>) {
        let at = c.input_cutoff.decision_time().logical_time_utc_ms();
        let result = self
            .shared
            .dispatch_strategy_qualified(
                &mut self.driver.decision().unwrap(),
                &mut self.strategy,
                c.complete(at),
            )
            .await
            .unwrap();
        assert!(
            matches!(result, QualifiedStrategyDispatchV1::Dispatched { .. }),
            "合法计算应通过资格"
        );
    }
    fn advance(&mut self, at: i64) {
        loop {
            next_combined(&mut self.driver, &mut self.hidden, &mut self.next_hidden);
            let input = self
                .shared
                .strategy_observation_input(&mut self.driver)
                .unwrap()
                .unwrap();
            if input
                .action_before
                .cutoff()
                .decision_time()
                .logical_time_utc_ms()
                >= BASE + at
            {
                break;
            }
        }
    }
}
// 沿正式有限宿主选择公共、隐藏和原义务三者最早时点；本毫秒事件先供完再闭合。
fn next_combined(
    driver: &mut Driver,
    hidden: &mut RecordedHiddenBucketSourceV1,
    next: &mut Option<AcceptedHiddenStepBucketV1>,
) {
    let public = driver.next_group_at().unwrap();
    let hidden_at = next.as_ref().map(|b| b.reference().bucket_time_utc_ms());
    let required = driver
        .next_required_logical_drive()
        .map(|r| r.required_at_utc_ms);
    let at = [public, hidden_at, required]
        .into_iter()
        .flatten()
        .min()
        .expect("声明的有限事件必须到达");
    driver.advance_before(at).unwrap();
    while driver.next_group_at().unwrap() == Some(at) {
        driver.next_group().unwrap().unwrap();
    }
    if hidden_at == Some(at) {
        let bucket = next.take().unwrap();
        if inventory_forensic_root().is_some() {
            println!(
                "forensic_hidden {}",
                serde_json::to_string(&bucket).unwrap()
            );
        }
        driver.hidden_bucket(bucket).unwrap();
        *next = hidden.next_bucket().unwrap();
    }
    driver.observe_at(at).unwrap();
    if required == Some(at) {
        driver.advance_before(at.checked_add(1).unwrap()).unwrap();
    }
    if inventory_forensic_root().is_some() {
        println!(
            "forensic_step at={at} public={public:?} hidden={hidden_at:?} required={required:?} next_required={:?}",
            driver.next_required_logical_drive()
        );
    }
}
fn details(
    c: &StrategyCalculationV1<ContinuousInventoryDiagnosticV1>,
) -> (&[ReservedIntentV1], &ContinuousInventoryDiagnosticV1) {
    match &c.output {
        StrategyOutputV1::Continue {
            intents,
            diagnostics,
            ..
        } => (intents, diagnostics),
        _ => panic!("有限市场不应提前退出"),
    }
}
#[tokio::test(flavor = "current_thread")]
async fn original_price_coverage_unequal_pair_return_and_over_capacity_are_preserved() {
    use fixture_input::{DOWN_TOKEN_ID as DOWN, UP_TOKEN_ID as UP};
    let events = vec![
        (23_000, quoted_book(UP, "0.5", "0.62")),
        (23_001, quoted_book(DOWN, "0.5", "0.62")),
        (25_000, quoted_book(UP, "0.5", "0.6")),
        (25_001, quoted_book(DOWN, "0.5", "0.6")),
    ];
    let mut f = InventoryFixture::new(events, engineering_config());
    f.shared
        .execute_batch_reserved(
            &mut f.driver.decision().unwrap(),
            &[limit("old-up", UP, "30", "0.45")],
        )
        .await
        .unwrap();
    let c = f.calculation();
    let (intents, d) = details(&c);
    assert_eq!(
        d.markets[0].coverage,
        [ExactDecimal("30".into()), ExactDecimal("0".into())]
    );
    assert_eq!(
        d.markets[0].quantities,
        [ExactDecimal("20".into()), ExactDecimal("50".into())]
    );
    let ModelIntentV1::SubmitPairedBuy { orders, .. } = intents[0].intent.as_ref() else {
        panic!("正式不等量双腿")
    };
    assert!(
        matches!(orders[0].order,BuyOrderIntentV1::GtcLimit {size_shares,..} if size_shares==dec("20"))
    );
    assert!(
        matches!(orders[1].order,BuyOrderIntentV1::GtcLimit {size_shares,..} if size_shares==dec("50"))
    );
    f.dispatch(c).await;
    assert!(details(&f.calculation()).0.is_empty(), "没有回执不能重补");
    f.advance(24_001);
    let changed = f.calculation();
    let (intents, d) = details(&changed);
    assert_eq!(
        d.markets[0].prices,
        [
            Some(ExactDecimal("0.44".into())),
            Some(ExactDecimal("0.44".into()))
        ]
    );
    assert_eq!(
        d.markets[0].coverage,
        [ExactDecimal("0".into()), ExactDecimal("0".into())]
    );
    assert_eq!(intents.len(), 1, "异价旧单不普通撤销");
    f.dispatch(changed).await;
    f.advance(26_001);
    let returned = f.calculation();
    let (intents, d) = details(&returned);
    assert!(intents.is_empty(), "价位返回必须复用旧价容量: {d:?}");
    assert_eq!(
        d.markets[0].coverage,
        [ExactDecimal("50".into()), ExactDecimal("50".into())]
    );
    f.shared
        .execute_batch_reserved(
            &mut f.driver.decision().unwrap(),
            &[limit("extra-up", UP, "30", "0.45")],
        )
        .await
        .unwrap();
    let over = f.calculation();
    let (intents, d) = details(&over);
    assert_eq!(d.markets[0].coverage[0], ExactDecimal("80".into()));
    assert!(intents.is_empty(), "超容量旧单普通保留: {d:?}");
    f.driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn full_side_risk_keeps_other_side_and_true_excess_cannot_add_risk() {
    use fixture_input::{DOWN_TOKEN_ID as DOWN, UP_TOKEN_ID as UP};
    for (old_price, expect_down) in [("0.50", true), ("0.51", false)] {
        let mut f = InventoryFixture::new(vec![], engineering_config());
        f.shared
            .execute_batch_reserved(
                &mut f.driver.decision().unwrap(),
                &[limit("full-up", UP, "200", old_price)],
            )
            .await
            .unwrap();
        let c = f.calculation();
        let (intents, d) = details(&c);
        if expect_down {
            assert_eq!(
                d.markets[0].quantities,
                [ExactDecimal("0".into()), ExactDecimal("50".into())]
            );
            assert_eq!(intents.len(), 1);
            let ModelIntentV1::SubmitSingleBuy { order, .. } = intents[0].intent.as_ref() else {
                panic!("不能因Up满额清除Down")
            };
            assert_eq!(order.token_id, TokenId(DOWN.into()));
            f.dispatch(c).await;
            assert!(details(&f.calculation()).0.is_empty());
        } else {
            assert!(intents.is_empty(), "没有H的真超限不得借独立修复放行: {d:?}");
        }
        f.driver.stop_and_drain().unwrap();
    }
}
fn trade(token: &str, price: &str, size: &str) -> PolymarketOrderbookEvent {
    PolymarketOrderbookEvent::LastTradePrice {
        asset_id: token.into(),
        price: price.into(),
        size: Some(size.into()),
        side: Some(PolymarketSide::Sell),
        fee_rate_bps: None,
    }
}
#[tokio::test(flavor = "current_thread")]
async fn real_v2_maker_trade_moves_pending_to_successful_inventory_without_double_counting() {
    use fixture_input::UP_TOKEN_ID as UP;
    let events = vec![
        (24_000, trade(UP, "0.45", "130")),
        (
            26_000,
            PolymarketOrderbookEvent::LastTradePrice {
                asset_id: UP.into(),
                price: "0.45".into(),
                size: None,
                side: None,
                fee_rate_bps: None,
            },
        ),
    ];
    let mut f = InventoryFixture::new(events, engineering_config());
    let receipts = f
        .shared
        .execute_batch_reserved(
            &mut f.driver.decision().unwrap(),
            &[limit("maker-up", UP, "30", "0.45")],
        )
        .await
        .unwrap();
    println!(
        "maker_receipts {}",
        serde_json::to_string(&receipts).unwrap()
    );
    let mut pending = None;
    let applied = loop {
        next_combined(&mut f.driver, &mut f.hidden, &mut f.next_hidden);
        let c = f.detached_calculation();
        let d = details(&c).1;
        println!("maker_legal_freeze {}", serde_json::to_string(&c).unwrap());
        if d.markets[0].t[0] == ExactDecimal("30".into()) && pending.is_none() {
            pending = Some(c);
        } else if d.markets[0].h[0] == ExactDecimal("30".into()) {
            break c;
        }
        assert!(
            f.driver
                .observation_cutoff()
                .unwrap()
                .decision_time()
                .logical_time_utc_ms()
                < BASE + 30_000,
            "自然市场结束前必须完成此有限成功路径，不能伪造延后经营"
        );
    };
    let pending = pending.expect("真实30份必须经过已观察未成功应用阶段");
    let read = f
        .shared
        .strategy_observation_input(&mut f.driver)
        .unwrap()
        .unwrap();
    let cash = read.action_before.cash_available();
    let unclosed = read.action_before.obligation_trades().len();
    f.driver.stop_and_drain().unwrap();
    let (_, d) = details(&pending);
    assert_eq!(
        d.markets[0].h,
        [ExactDecimal("0".into()), ExactDecimal("0".into())]
    );
    assert_eq!(
        d.markets[0].t,
        [ExactDecimal("30".into()), ExactDecimal("0".into())],
        "正式跨价预算先扣更优External100，再成交Account30: {d:?}"
    );
    assert_eq!(d.markets[0].difference, ExactDecimal("30".into()));
    let (_, d) = details(&applied);
    assert_eq!(
        d.markets[0].h,
        [ExactDecimal("30".into()), ExactDecimal("0".into())]
    );
    assert_eq!(
        d.markets[0].t,
        [ExactDecimal("0".into()), ExactDecimal("0".into())]
    );
    assert_eq!(d.markets[0].difference, ExactDecimal("30".into()));
    assert_eq!(
        d.markets[0].investment,
        contracts::economics::EconomicRational::from_decimal(dec("13.5")).public()
    );
    assert_eq!(cash, dec("1986.5"));
    assert_eq!(unclosed, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn health_deadline_invalidates_unsent_and_empty_book_keeps_cancellation_responsibility() {
    use fixture_input::{DOWN_TOKEN_ID as DOWN, UP_TOKEN_ID as UP};
    let mut config = engineering_config();
    config.health.polymarket_silence_ms = 500;
    config.health.polymarket_event_delay_ms = 1500;
    let mut empty = quoted_book(DOWN, "0.5", "0.6");
    if let PolymarketOrderbookEvent::Book { bids, asks, .. } = &mut empty {
        bids.clear();
        asks.clear();
    }
    let mut f = InventoryFixture::new(vec![(23_000, empty)], config);
    let original = f.calculation();
    let at = BASE + 22_600;
    f.driver.observe_at(at).unwrap();
    let current = f.shared.strategy_current_read(&f.driver).unwrap().unwrap();
    let q = f
        .strategy
        .qualify_intent(&current, &original, 0, at)
        .unwrap();
    assert!(
        matches!(q,IntentQualificationV1::Invalidated(IntentInvalidationReasonV1 {ref reason_code,..}) if reason_code=="source_health")
    );
    drop(current);
    // 原合法22秒冻结的明确未发意图没有造成任何预留，保留原Bridge行为。
    assert_eq!(
        f.shared.ledger().necessary_read_v2().unwrap().cash().0,
        dec("2000")
    );
    // 在原22秒冻结发送能力上不回退时钟；发送选择仍需合法当前依赖，使用显式setup原单。
    let cutoff = f.driver.observation_cutoff().unwrap();
    f.shared
        .execute_batch_reserved(
            &mut f.driver.send_decision_at(cutoff, at).unwrap(),
            &[limit("old-health", UP, "30", "0.45")],
        )
        .await
        .unwrap();
    f.advance(24_000);
    let c = f.calculation();
    let (intents, d) = details(&c);
    assert!(d.markets[0].books.is_none(), "真正有效空簿不能以BBA补齐");
    assert!(
        d.markets[0]
            .unavailable
            .iter()
            .any(|s| s == "source_health")
    );
    assert!(
        intents
            .iter()
            .any(|i| matches!(i.intent.as_ref(), ModelIntentV1::CancelOrders { .. })),
        "无报价仍有旧单取消责任: {d:?}"
    );
    assert!(!intents.iter().any(|i| matches!(
        i.intent.as_ref(),
        ModelIntentV1::SubmitPairedBuy { .. } | ModelIntentV1::SubmitSingleBuy { .. }
    )));
    f.driver.stop_and_drain().unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn partial_match_and_cancel_keep_original_obligations_disjoint() {
    use fixture_input::UP_TOKEN_ID as UP;
    let mut f = InventoryFixture::new(
        vec![(24_000, trade(UP, "0.45", "110"))],
        engineering_config(),
    );
    f.shared
        .execute_batch_reserved(
            &mut f.driver.decision().unwrap(),
            &[limit("partial-up", UP, "30", "0.45")],
        )
        .await
        .unwrap();
    let mut saw_pending = false;
    loop {
        next_combined(&mut f.driver, &mut f.hidden, &mut f.next_hidden);
        let c = f.calculation();
        let (_, d) = details(&c);
        let m = &d.markets[0];
        assert_eq!(
            m.investment,
            contracts::economics::EconomicRational::from_decimal(dec("13.5")).public()
        );
        if m.t[0] == ExactDecimal("10".into()) {
            saw_pending = true;
            let obligations = &d.account_dependencies[0].obligations;
            let quantities = obligations
                .iter()
                .map(|o| match &o.quantity {
                    ExecutionReadValueV1::Known(q) => dec(&q.0),
                    _ => panic!("正式已知原量"),
                })
                .sum::<Decimal>();
            assert_eq!(quantities, dec("30"), "20活动余量与10成交互斥相加");
        }
        if m.h[0] == ExactDecimal("10".into()) {
            break;
        }
        assert!(
            f.driver
                .observation_cutoff()
                .unwrap()
                .decision_time()
                .logical_time_utc_ms()
                < BASE + 30_000
        );
    }
    assert!(saw_pending);
    let input = f
        .shared
        .strategy_observation_input(&mut f.driver)
        .unwrap()
        .unwrap();
    let ids = input
        .action_before
        .orders()
        .map(|o| match &o.order_identity.key {
            ::execution_normalization::MemoryIdentityKeyV1::Entity(
                ::execution_normalization::SourceNeutralEntityKeyV1::Order { source_order_id },
            ) => source_order_id.clone(),
            _ => panic!("原SIM订单身份"),
        })
        .collect::<Vec<_>>();
    assert_eq!(ids.len(), 1);
    let cutoff = f.driver.observation_cutoff().unwrap();
    let at = cutoff.decision_time().logical_time_utc_ms();
    f.shared
        .execute_batch_reserved(
            &mut f.driver.send_decision_at(cutoff, at).unwrap(),
            &[ReservedIntentV1 {
                context: RequestContextV2 {
                    global_request_id: "cancel-partial".into(),
                    caller_correlation_id: None,
                },
                intent: std::sync::Arc::new(ModelIntentV1::CancelOrders {
                    platform_order_ids: ids,
                }),
                reserves: vec![],
            }],
        )
        .await
        .unwrap();
    loop {
        next_combined(&mut f.driver, &mut f.hidden, &mut f.next_hidden);
        let input = f
            .shared
            .strategy_observation_input(&mut f.driver)
            .unwrap()
            .unwrap();
        if input.action_before.orders().count() == 0 {
            break;
        }
        assert!(
            f.driver
                .observation_cutoff()
                .unwrap()
                .decision_time()
                .logical_time_utc_ms()
                < BASE + 30_000
        );
    }
    let c = f.calculation();
    let (_, d) = details(&c);
    assert_eq!(
        d.markets[0].h,
        [ExactDecimal("10".into()), ExactDecimal("0".into())]
    );
    assert_eq!(
        d.markets[0].t,
        [ExactDecimal("0".into()), ExactDecimal("0".into())]
    );
    assert_eq!(d.markets[0].obligation_count, 0);
    assert_eq!(
        d.markets[0].investment,
        contracts::economics::EconomicRational::from_decimal(dec("4.5")).public()
    );
    assert_eq!(d.markets[0].cash_before, ExactDecimal("1995.5".into()));
    f.driver.stop_and_drain().unwrap();
}
fn failure_policy(applied: bool) -> Vec<u8> {
    let normal: serde_json::Value = serde_json::from_slice(fixture_policy::SUCCESS_POLICY).unwrap();
    let path = if applied {
        serde_json::json!({"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10},{"state":"FAILED","offset_ms":20}],"economic_application_ordinal":1})
    } else {
        serde_json::json!({"steps":[{"state":"MATCHED","offset_ms":0},{"state":"FAILED","offset_ms":20}],"economic_application_ordinal":null})
    };
    serde_json::to_vec(&serde_json::json!({"schema_version":"trade_finality_policy_v2","normal_path":normal["normal_path"],"failure_overrides":[{
        "selector":{"selector_domain":"REQUEST","account_instance_key":AccountInstanceKeyV1 {run_id:"inventory-fixture".into(),caller_role:SimCallerRoleV1::HistoricalBacktest,
            caller_instance_id:"shared-caller".into(),account_id:"shared-account".into(),platform_source:PlatformSourceV1::Simulated},"global_request_id":"failed-up"},"path":path}]})).unwrap()
}
#[tokio::test(flavor = "current_thread")]
async fn final_failure_releases_pending_and_returns_to_initial_without_sticky_history() {
    use fixture_input::UP_TOKEN_ID as UP;
    for applied in [false, true] {
        let mut f = InventoryFixture::with_policy(
            vec![(24_000, trade(UP, "0.45", "130"))],
            engineering_config(),
            &failure_policy(applied),
        );
        f.shared
            .execute_batch_reserved(
                &mut f.driver.decision().unwrap(),
                &[limit("failed-up", UP, "30", "0.45")],
            )
            .await
            .unwrap();
        let mut saw_pending = false;
        loop {
            next_combined(&mut f.driver, &mut f.hidden, &mut f.next_hidden);
            let c = f.calculation();
            let (_, d) = details(&c);
            let m = &d.markets[0];
            if m.t[0] == ExactDecimal("30".into()) {
                saw_pending = true;
            }
            assert_eq!(m.h, [ExactDecimal("0".into()), ExactDecimal("0".into())]);
            if saw_pending && m.obligation_count == 0 {
                assert_eq!(m.t, [ExactDecimal("0".into()), ExactDecimal("0".into())]);
                assert_eq!(
                    m.investment,
                    contracts::economics::EconomicRational::zero().public()
                );
                assert_eq!(m.cash_before, ExactDecimal("2000".into()));
                assert!(m.initial, "失败后没有H/T/承诺时恢复首次，不设曾交易sticky");
                assert_eq!(
                    m.quantities,
                    [ExactDecimal("50".into()), ExactDecimal("50".into())]
                );
                break;
            }
            assert!(
                f.driver
                    .observation_cutoff()
                    .unwrap()
                    .decision_time()
                    .logical_time_utc_ms()
                    < BASE + 30_000
            );
        }
        f.driver.stop_and_drain().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_profit_fixed_fifty_price_cap_final_coverage_and_support_occupancy() {
    use fixture_input::{DOWN_TOKEN_ID as DOWN, UP_TOKEN_ID as UP};
    let events = vec![
        (23_000, trade(UP, "0.20", "110")),
        (23_001, trade(DOWN, "0.20", "110")),
        (24_000, quoted_book(UP, "0.70", "0.90")),
        (26_000, trade(UP, "0.80", "10")),
        (27_000, quoted_book(UP, "0.50", "0.60")),
    ];
    let mut f = InventoryFixture::new(events, engineering_config());
    f.shared
        .execute_batch_reserved(
            &mut f.driver.decision().unwrap(),
            &[
                limit("cheap-up", UP, "10", "0.20"),
                limit("cheap-down", DOWN, "10", "0.20"),
            ],
        )
        .await
        .unwrap();
    f.advance(25_000);
    let read = f
        .shared
        .strategy_observation_input(&mut f.driver)
        .unwrap()
        .unwrap();
    assert_eq!(
        read.action_before.profit_and_equity().0,
        contracts::economics::EconomicRational::from_decimal(dec("6")),
        "原Ledger跨批成功配对10份、成本4，实际已确认配对毛额6；未当作现金6到账"
    );
    assert_eq!(read.action_before.cash_available(), dec("1996"));
    let cutoff = f.driver.observation_cutoff().unwrap();
    let at = cutoff.decision_time().logical_time_utc_ms();
    f.shared
        .execute_batch_reserved(
            &mut f.driver.send_decision_at(cutoff, at).unwrap(),
            &[
                limit("costly-up", UP, "10", "0.80"),
                limit("old-down", DOWN, "40", "0.32"),
            ],
        )
        .await
        .unwrap();
    f.advance(28_000);
    let c = f.calculation();
    let (_, d) = details(&c);
    let m = &d.markets[0];
    assert_eq!(m.h, [ExactDecimal("10".into()), ExactDecimal("0".into())]);
    assert_eq!(
        d.available_profit,
        contracts::economics::EconomicRational::from_decimal(dec("6")).public()
    );
    // 原10秒有限窗口此刻允许小亏<0.01。1+allowance-.8+6/50在[.32,.33)，最终价.32。
    // 若错误地以本次缺口10为分母，则不会从原.45下压到.32。
    assert_eq!(m.prices[1], Some(ExactDecimal("0.32".into())));
    assert_eq!(
        m.coverage[1],
        ExactDecimal("40".into()),
        "按成本下压后的最终价重算C"
    );
    assert_eq!(m.deficit[1], ExactDecimal("10".into()));
    assert_eq!(m.quantities[1], ExactDecimal("10".into()));
    assert_eq!(d.profit_allocations.len(), 1);
    let original = f
        .shared
        .ledger()
        .economic_read()
        .unwrap()
        .market_totals()
        .next()
        .unwrap();
    assert_eq!(original.cumulative_paired_shares.0, "10");
    assert_eq!(
        original.cumulative_paired_actual_trade_fee_pusd,
        ExecutionReadValueV1::Known(contracts::economics::EconomicRational::zero().public())
    );
    assert_eq!(
        original.cumulative_paired_spread_after_trade_fee_pusd,
        ExecutionReadValueV1::Known(
            contracts::economics::EconomicRational::from_decimal(dec("6")).public()
        )
    );
    // 真实归约生成的原checkpoint，按正式serde(default)旧版本缺字段边界恢复；不构造矛盾经济状态。
    let mut saved = Vec::new();
    f.shared.ledger().write_checkpoint(&mut saved).unwrap();
    let mut value: ciborium::Value = ciborium::from_reader(saved.as_slice()).unwrap();
    fn remove_old_fields(v: &mut ciborium::Value) -> usize {
        match v {
            ciborium::Value::Map(fields) => {
                let before = fields.len();
                fields.retain(|(key,_)|!matches!(key,ciborium::Value::Text(key) if key=="orphan_fee"||key=="pool_contribution_shares"));
                before - fields.len()
                    + fields
                        .iter_mut()
                        .map(|(_, v)| remove_old_fields(v))
                        .sum::<usize>()
            }
            ciborium::Value::Array(values) => values.iter_mut().map(remove_old_fields).sum(),
            _ => 0,
        }
    }
    assert!(remove_old_fields(&mut value) > 0);
    let mut legacy = Vec::new();
    ciborium::into_writer(&value, &mut legacy).unwrap();
    let restored = LedgerWriterV1::restore_checkpoint(legacy.as_slice()).unwrap();
    let old = restored
        .economic_read()
        .unwrap()
        .market_totals()
        .next()
        .unwrap();
    assert_eq!(
        old.cumulative_paired_shares,
        original.cumulative_paired_shares
    );
    assert_eq!(
        old.cumulative_paired_principal_cost_pusd,
        original.cumulative_paired_principal_cost_pusd
    );
    assert_eq!(
        old.cumulative_paired_actual_trade_fee_pusd,
        ExecutionReadValueV1::Unavailable
    );
    assert_eq!(
        old.cumulative_paired_spread_after_trade_fee_pusd,
        ExecutionReadValueV1::Unavailable
    );
    assert_eq!(
        d.profit_allocations[0].allocated_profit_pusd,
        contracts::economics::EconomicRational::from_decimal(dec("1.2")).public(),
        "只有新增10份真实H可配部分产生本金增量1.2；既有未成交单不能免除支持占用"
    );
    f.dispatch(c).await;
    let repeated = f.calculation();
    let (_, d) = details(&repeated);
    assert_eq!(
        d.available_profit,
        contracts::economics::EconomicRational::from_decimal(dec("4.8")).public(),
        "已发未回执占用不能重复借"
    );
    assert!(
        d.markets[0].prices[1]
            .as_ref()
            .is_some_and(|p| dec(&p.0) <= dec("0.30")),
        "缩量不抬价；固定50分母继续使用剩余支持"
    );
    f.driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn end_does_not_release_open_orders_and_resolution_uses_original_cancellation_facts() {
    use fixture_input::{CONDITION_ID, UP_TOKEN_ID as UP};
    let mut f = InventoryFixture::with_tail(
        vec![(24_000, trade(UP, "0.45", "130"))],
        engineering_config(),
        fixture_policy::SUCCESS_POLICY,
        true,
    );
    let c = f.calculation();
    f.dispatch(c).await;
    f.advance(30_000);
    let c = f.calculation();
    let (intents, _) = details(&c);
    assert!(
        !intents.iter().any(|i| matches!(
            i.intent.as_ref(),
            ModelIntentV1::SubmitSingleBuy { .. } | ModelIntentV1::SubmitPairedBuy { .. }
        )),
        "end不新BUY"
    );
    let read = f
        .shared
        .strategy_observation_input(&mut f.driver)
        .unwrap()
        .unwrap();
    assert_eq!(
        read.action_before.orders().count(),
        2,
        "end本身不是SIM自动取消或释放"
    );
    assert_eq!(read.action_before.cash_available(), dec("1955"));
    assert_eq!(
        read.action_before
            .market_economics(&ConditionId(CONDITION_ID.into()))
            .unwrap()
            .orphan_shares
            .iter()
            .sum::<Decimal>(),
        dec("30")
    );
    f.advance(31_003);
    // 公共终态可见不等于取消反馈已领取；沿原实际义务推进至该反馈应用。
    loop {
        let read = f
            .shared
            .strategy_observation_input(&mut f.driver)
            .unwrap()
            .unwrap();
        if read.action_before.orders().count() == 0 {
            break;
        }
        assert!(
            f.driver.next_required_logical_drive().is_some(),
            "原取消反馈尚有正式到期义务"
        );
        next_combined(&mut f.driver, &mut f.hidden, &mut f.next_hidden);
    }
    let read = f
        .shared
        .strategy_observation_input(&mut f.driver)
        .unwrap()
        .unwrap();
    assert_eq!(
        read.action_before.orders().count(),
        0,
        "合格resolution原MarketResolvedCanceled释放open"
    );
    assert_eq!(
        read.action_before.cash_available(),
        dec("2016.5"),
        "1955加原取消释放31.5，再加原AutoRedeemApplied赢家回款30；不提前或重复计现金"
    );
    let m = f
        .shared
        .ledger()
        .economic_read()
        .unwrap()
        .market_totals()
        .next()
        .unwrap();
    assert_eq!(m.cumulative_paired_shares.0, "0");
    assert_eq!(m.winner_orphan_receivable_pusd.0, "0");
    assert_eq!(m.settlement_state, EconomicSettlementStateV1::Redeemed);
    assert!(
        f.shared
            .ledger()
            .economic_read()
            .unwrap()
            .resolutions()
            .next()
            .unwrap()
            .request
            .is_none(),
        "D-0328历史交易侧不主动redeem"
    );
    f.driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn confirmed_drawdown_keeps_actual_h_repair_qualified_without_new_exposed_side() {
    use fixture_input::{DOWN_TOKEN_ID as DOWN, UP_TOKEN_ID as UP};
    let mut f = InventoryFixture::new(
        vec![
            (23_000, quoted_book(UP, "0.70", "0.90")),
            (26_000, trade(UP, "0.80", "1000")),
            (28_000, quoted_book(UP, "0.01", "0.03")),
        ],
        engineering_config(),
    );
    f.advance(24_000);
    let cutoff = f.driver.observation_cutoff().unwrap();
    let at = cutoff.decision_time().logical_time_utc_ms();
    f.shared
        .execute_batch_reserved(
            &mut f.driver.send_decision_at(cutoff, at).unwrap(),
            &[limit("drawdown-original-up", UP, "1000", "0.80")],
        )
        .await
        .unwrap();
    f.advance(29_000);
    let c = f.calculation();
    let (intents, d) = details(&c);
    assert!(
        d.terminating,
        "实际成功本金800、当前真簿标记10使权益1210，触及原30%阈值"
    );
    assert_eq!(
        d.markets[0].h,
        [ExactDecimal("1000".into()), ExactDecimal("0".into())]
    );
    assert_eq!(
        d.markets[0].investment,
        contracts::economics::EconomicRational::from_decimal(dec("800")).public()
    );
    let buy = intents
        .iter()
        .filter(|i| {
            matches!(
                i.intent.as_ref(),
                ModelIntentV1::SubmitSingleBuy { .. } | ModelIntentV1::SubmitPairedBuy { .. }
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(buy.len(), 1, "{d:?}");
    let ModelIntentV1::SubmitSingleBuy { order, .. } = buy[0].intent.as_ref() else {
        panic!("终止只独立补实际H")
    };
    assert_eq!(order.token_id, TokenId(DOWN.into()), "不增加已有Up侧新风险");
    let BuyOrderIntentV1::GtcLimit { size_shares, .. } = order.order else {
        panic!("原post_only LIMIT")
    };
    assert!(size_shares > Decimal::ZERO && size_shares <= dec("1000"));
    assert_eq!(
        d.action_dependencies[0].independent_held_token,
        Some(TokenId(UP.into()))
    );
    let id = buy[0].context.global_request_id.clone();
    let spend = buy[0]
        .reserves
        .iter()
        .map(|r| dec(&r.reserve.requested_principal_pusd.0))
        .sum::<Decimal>();
    f.dispatch(c).await;
    let read = f
        .shared
        .strategy_observation_input(&mut f.driver)
        .unwrap()
        .unwrap();
    assert!(
        read.action_before
            .commitments()
            .any(|x| x.origin.intent.global_request_id() == id)
    );
    assert_eq!(
        read.action_before.cash_available(),
        dec("1200") - spend,
        "原reserve/Bridge真实发送，预留仅扣一次"
    );
    f.driver.stop_and_drain().unwrap();
}

// D-0378：原利润池和双BUY混合反馈均来自正式持久合成链。
async fn mixed_profit_case(shared_h: bool) {
    use fixture_input::{DOWN_TOKEN_ID as DOWN, UP_TOKEN_ID as UP};
    let mut events = vec![
        (23_000, trade(UP, "0.20", "110")),
        (23_001, trade(DOWN, "0.20", "110")),
        (23_010, quoted_book(UP, "0.70", "0.90")),
        (24_100, trade(UP, "0.80", "10")),
        (24_200, quoted_book(UP, "0.65", "0.67")),
        (24_201, quoted_book(DOWN, "0.32", "0.34")),
        (26_000, trade(UP, "0.65", "110")),
    ];
    if shared_h {
        events.extend([
            (26_200, quoted_book(UP, "0.66", "0.68")),
            (26_201, quoted_book(DOWN, "0.31", "0.33")),
        ]);
    }
    events.push((27_500, trade(DOWN, "0.32", "105")));
    let mut f = InventoryFixture::new(events, engineering_config());
    f.shared
        .execute_batch_reserved(
            &mut f.driver.decision().unwrap(),
            &[
                limit("profit-seed-up", UP, "10", "0.20"),
                limit("profit-seed-down", DOWN, "10", "0.20"),
            ],
        )
        .await
        .unwrap();
    f.advance(24_010);
    f.driver.stabilize_recording().unwrap();
    let input = f
        .shared
        .strategy_observation_input(&mut f.driver)
        .unwrap()
        .unwrap();
    assert_eq!(
        input.action_before.profit_and_equity().0,
        contracts::economics::EconomicRational::from_decimal(dec("6"))
    );
    let cutoff = f.driver.observation_cutoff().unwrap();
    let at = cutoff.decision_time().logical_time_utc_ms();
    f.shared
        .execute_batch_reserved(
            &mut f.driver.send_decision_at(cutoff, at).unwrap(),
            &[limit("profit-held-up", UP, "10", "0.80")],
        )
        .await
        .unwrap();
    f.advance(25_201);
    f.driver.stabilize_recording().unwrap();
    let c = f.calculation();
    let (intents, d) = details(&c);
    assert_eq!(
        d.markets[0].h,
        [ExactDecimal("10".into()), ExactDecimal("0".into())]
    );
    assert_eq!(d.profit_allocations.len(), 1);
    let first = d.profit_allocations[0].clone();
    assert_eq!(
        first.allocated_profit_pusd,
        contracts::economics::EconomicRational::from_decimal(dec("1.2")).public()
    );
    let item = intents
        .iter()
        .find(|i| i.context.global_request_id == first.global_request_id)
        .unwrap();
    let ModelIntentV1::SubmitPairedBuy { orders, .. } = item.intent.as_ref() else {
        panic!("真实混合双BUY")
    };
    for (i, (token, price)) in [(UP, "0.65"), (DOWN, "0.32")].iter().enumerate() {
        assert_eq!(orders[i].token_id, TokenId((*token).into()));
        assert!(
            matches!(orders[i].order,BuyOrderIntentV1::GtcLimit {limit_price_pusd_per_share,size_shares,..} if limit_price_pusd_per_share==dec(price)&&size_shares==dec("50"))
        );
    }
    println!(
        "profit_frozen {}",
        serde_json::json!({"shared_h":shared_h,"diagnostics":d,"intents":intents,"owner":f.shared.ledger().economic_read().unwrap().market_totals().collect::<Vec<_>>()})
    );
    f.dispatch(c).await;
    f.driver.stabilize_recording().unwrap();
    f.advance(27_000);
    f.driver.stabilize_recording().unwrap();
    let c = f.calculation();
    let (_, d) = details(&c);
    assert_eq!(
        d.available_profit,
        contracts::economics::EconomicRational::from_decimal(dec("4.8")).public(),
        "另一Up10成功不能释放Down1.2"
    );
    assert_eq!(
        d.markets[0].h,
        [ExactDecimal("20".into()), ExactDecimal("0".into())]
    );
    assert_eq!(
        d.account_dependencies[0].orphan_cost,
        contracts::economics::EconomicRational::from_decimal(dec("14.5")).public()
    );
    println!(
        "profit_other_success {}",
        serde_json::json!({"shared_h":shared_h,"diagnostics":d,"owner":f.shared.ledger().economic_read().unwrap().market_totals().collect::<Vec<_>>()})
    );
    if shared_h {
        f.advance(27_201);
        f.driver.stabilize_recording().unwrap();
        let second = f.calculation();
        let (_, d) = details(&second);
        assert_eq!(d.profit_allocations.len(), 1);
        assert_eq!(
            d.profit_allocations[0].allocated_profit_pusd,
            contracts::economics::EconomicRational::from_decimal(dec("0.7")).public()
        );
        assert_eq!(
            d.profit_allocations[0].original_orphan_shares,
            contracts::economics::EconomicRational::from_decimal(dec("20")).public(),
            "两个请求共享当前H场景，不分虚拟独占配额"
        );
        println!(
            "profit_shared_second {}",
            serde_json::json!({"diagnostics":d,"allocations":f.strategy.continuity().unresolved_profit_allocations})
        );
        f.dispatch(second).await;
        f.driver.stabilize_recording().unwrap();
        let repeated = f.calculation();
        let (_, d) = details(&repeated);
        assert_eq!(
            d.available_profit,
            contracts::economics::EconomicRational::from_decimal(dec("4.1")).public()
        );
    }
    f.advance(28_500);
    f.driver.stabilize_recording().unwrap();
    let c = f.calculation();
    let (_, d) = details(&c);
    let pnl = f
        .shared
        .strategy_observation_input(&mut f.driver)
        .unwrap()
        .unwrap()
        .action_before
        .profit_and_equity()
        .0;
    assert_eq!(
        pnl,
        contracts::economics::EconomicRational::from_decimal(dec("5.775"))
    );
    assert_eq!(
        d.markets[0].h,
        [ExactDecimal("15".into()), ExactDecimal("0".into())]
    );
    assert_eq!(
        d.account_dependencies[0].orphan_cost,
        contracts::economics::EconomicRational::from_decimal(dec("10.875")).public()
    );
    assert_eq!(
        d.available_profit,
        contracts::economics::EconomicRational::from_decimal(dec(if shared_h {
            "4.4"
        } else {
            "5.1"
        }))
        .public(),
        "所属success_applied已进P，仅保留未决max责任，不重扣.225"
    );
    assert!(
        !d.markets[0]
            .cancellations
            .iter()
            .any(|(_, reason)| reason == "successful_inventory_hard_cost"),
        "支持按实际可配H分母，不能用剩余45份误撤"
    );
    println!(
        "profit_own_success {}",
        serde_json::json!({"shared_h":shared_h,"diagnostics":d,"owner":f.shared.ledger().economic_read().unwrap().market_totals().collect::<Vec<_>>()})
    );
    let read = f
        .shared
        .strategy_observation_input(&mut f.driver)
        .unwrap()
        .unwrap();
    let ids = read
        .action_before
        .orders()
        .filter(|o| o.token_id == Some(&TokenId(DOWN.into())))
        .map(|o| match &o.order_identity.key {
            ::execution_normalization::MemoryIdentityKeyV1::Entity(
                ::execution_normalization::SourceNeutralEntityKeyV1::Order { source_order_id },
            ) => source_order_id.clone(),
            _ => panic!("正式订单"),
        })
        .collect::<Vec<_>>();
    let cutoff = f.driver.observation_cutoff().unwrap();
    let at = cutoff.decision_time().logical_time_utc_ms();
    f.shared
        .execute_batch_reserved(
            &mut f.driver.send_decision_at(cutoff, at).unwrap(),
            &[ReservedIntentV1 {
                context: RequestContextV2 {
                    global_request_id: "close-profit-down".into(),
                    caller_correlation_id: None,
                },
                intent: std::sync::Arc::new(ModelIntentV1::CancelOrders {
                    platform_order_ids: ids,
                }),
                reserves: vec![],
            }],
        )
        .await
        .unwrap();
    let pending = f.calculation();
    let (_, d) = details(&pending);
    assert_eq!(
        d.available_profit,
        contracts::economics::EconomicRational::from_decimal(dec(if shared_h {
            "4.4"
        } else {
            "5.1"
        }))
        .public(),
        "取消请求不能提前释放"
    );
    loop {
        next_combined(&mut f.driver, &mut f.hidden, &mut f.next_hidden);
        let read = f
            .shared
            .strategy_observation_input(&mut f.driver)
            .unwrap()
            .unwrap();
        if !read
            .action_before
            .orders()
            .any(|o| o.token_id == Some(&TokenId(DOWN.into())))
        {
            break;
        }
        assert!(
            read.action_before
                .cutoff()
                .decision_time()
                .logical_time_utc_ms()
                < BASE + 30_000
        );
    }
    let closed = f.calculation();
    let (_, d) = details(&closed);
    assert_eq!(
        d.available_profit,
        contracts::economics::EconomicRational::from_decimal(dec("5.775")).public(),
        "只关闭所属Down即可释放，另一Up余量不能继续借作Down义务"
    );
    assert!(
        f.strategy
            .continuity()
            .unresolved_profit_allocations
            .is_empty()
    );
    println!(
        "profit_closed {}",
        serde_json::json!({"shared_h":shared_h,"diagnostics":d})
    );
    f.driver.stop_and_drain().unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn mixed_buy_profit_support_uses_own_leg_and_current_average_pool() {
    mixed_profit_case(false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn shared_h_profit_requests_keep_separate_capped_overlapping_scenarios() {
    mixed_profit_case(true).await;
}
