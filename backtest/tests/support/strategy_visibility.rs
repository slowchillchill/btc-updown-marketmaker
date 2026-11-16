//! 同一公开前缀下改变隐藏准备，策略输入必须不可区分；宿主审计保留差异。
use super::*;
use backtest::offline_scene_source::*;
use serde_json::{Value, json};
use sim_system::offline_scene::*;
use std::{borrow::Borrow, sync::Mutex};

#[derive(Debug, PartialEq)]
struct PublicSample {
    cutoff: StrategyObservationCutoffV1,
    value: Value,
    debug_cutoff: String,
    trade_economics: Vec<FrozenTradeEconomicsV1>,
}

struct IsolationProbe {
    borrowed: bool,
    learning: bool,
    proposed: bool,
    samples: Arc<Mutex<Vec<PublicSample>>>,
    borrowed_frames: Arc<Mutex<Vec<Value>>>,
    dispatched: Arc<Mutex<Vec<Value>>>,
    callbacks: Arc<Mutex<Vec<Value>>>,
}

impl IsolationProbe {
    fn observe<G, M, A>(
        &mut self,
        input: &StrategyInputV1<G, M, A>,
        at: i64,
    ) -> Result<StrategyOutputV1, &'static str>
    where
        G: Borrow<SharedExecutionRegistryV2>,
        M: StrategyMarketReadV1,
        A: ActionAccountReadV2,
    {
        let cutoff = input.action_before.cutoff().clone();
        let cutoff_json = serde_json::to_value(&cutoff).unwrap();
        assert_no_host_coordinates(&cutoff_json);
        assert_eq!(
            input.action_before.learning_account().is_some(),
            self.learning
        );
        let value = json!({
            "at": at,
            "cutoff": cutoff_json,
            "market_frame_timing": input.market_frame_timing,
            "markets": input.markets.iter().map(|market| json!({
                "observation": market.observation(),
                "statistics": market.statistics(),
                "rule_context": market.rule_context(),
            })).collect::<Vec<_>>(),
            "economics": input.action_before.economics(),
            "ledger": input.action_before.ledger(),
            "learning": input.action_before.learning_account(),
            "learning_debug": format!("{:?}", input.action_before.learning_account()),
            "pending_noe_records": input.action_before.pending_noe_record_count(),
            "trade_economics": format!("{:?}", input.trade_economics),
        });
        assert_no_host_coordinates(&value);
        // There are no own orders in this fixture. Thus hidden market changes
        // cannot legitimately create different already-delivered own feedback.
        assert_eq!(input.action_before.orders().count(), 0);
        assert!(input.action_before.obligation_trades().is_empty());
        self.samples.lock().unwrap().push(PublicSample {
            debug_cutoff: format!("{cutoff:?}"),
            trade_economics: input.trade_economics.clone(),
            cutoff,
            value,
        });
        let intents = if !self.proposed && at >= BASE + 500 {
            self.proposed = true;
            vec![ReservedIntentV1 {
                context: RequestContextV2 {
                    global_request_id: "visibility-qualification".into(),
                    caller_correlation_id: None,
                },
                intent: Arc::new(ModelIntentV1::CancelMarketOrders {
                    condition_id: ConditionId(FIRST.into()),
                }),
                reserves: vec![],
            }]
        } else {
            vec![]
        };
        Ok(StrategyOutputV1::Continue {
            intents,
            next_evaluation_at_utc_ms: Some(at + 250),
            diagnostics: (),
        })
    }

    fn qualify<G, A>(
        &self,
        input: &StrategyCurrentReadV1<'_, G, A>,
        calculation: &StrategyCalculationReadV1<'_>,
        index: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, &'static str>
    where
        G: Borrow<SharedExecutionRegistryV2>,
        A: ActionAccountReadV2,
    {
        let value = json!({
            "kind": "qualify",
            "at": at,
            "index": index,
            "calculation": calculation,
            "cutoff": input.action_before.cutoff(),
            "markets": input.markets,
            "markets_debug": format!("{:?}", input.markets),
            "economics": input.action_before.economics(),
            "ledger": input.action_before.ledger(),
            "learning": input.action_before.learning_account(),
            "learning_debug": format!("{:?}", input.action_before.learning_account()),
            "trade_economics": format!("{:?}", input.trade_economics),
        });
        assert_no_host_coordinates(&value);
        self.callbacks.lock().unwrap().push(value);
        // An actual qualified dispatch reaches the current-view and invalidated
        // callbacks, while this deterministic rejection creates no own feedback.
        Ok(IntentQualificationV1::Invalidated(
            IntentInvalidationReasonV1 {
                reason_code: "visibility-fixture".into(),
                scopes: vec![],
            },
        ))
    }
}

impl StrategyV1 for IsolationProbe {
    type Error = &'static str;
    type Diagnostics = ();

    fn uses_borrowed_input(&self) -> bool {
        self.borrowed
    }

    fn uses_source_health(&self) -> bool {
        true
    }

    fn requires_learning_economics(&self) -> bool {
        self.learning
    }

    fn decide(
        &mut self,
        input: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1, Self::Error> {
        self.observe(input, at)
    }

    fn decide_borrowed(
        &mut self,
        input: &BorrowedStrategyInputV1<'_>,
        at: i64,
    ) -> Result<StrategyOutputV1, Self::Error> {
        let frames = serde_json::to_value(&input.markets).unwrap();
        // Probe the complete borrowed frame, not only the convenience trait.
        assert_no_host_coordinates(&frames);
        self.borrowed_frames.lock().unwrap().push(frames);
        let detached = input.freeze_for_detached();
        let value = json!({
            "kind": "detached",
            "cutoff": detached.action_before.cutoff(),
            "timing": detached.market_frame_timing,
            "markets": detached.markets.iter().map(|market| json!({
                "observation": market.observation,
                "statistics": market.statistics,
                "rule_context": market.rule_context,
            })).collect::<Vec<_>>(),
            "cash_available": detached.action_before.cash_available(),
            "profit_and_equity": format!("{:?}", detached.action_before.profit_and_equity()),
            "risk_market_ids": detached.action_before.risk_market_ids(),
            "trade_economics": format!("{:?}", detached.trade_economics),
        });
        assert_no_host_coordinates(&value);
        assert_eq!(
            detached.action_before.cutoff(),
            input.action_before.cutoff()
        );
        self.callbacks.lock().unwrap().push(value);
        self.observe(input, at)
    }

    fn next_health_evaluation_at(
        &self,
        markets: &[StrategyMarketReadFrameV1<'_>],
        calculation: Option<&StrategyCalculationReadV1<'_>>,
        at: i64,
    ) -> Option<i64> {
        let value = json!({
            "kind": "health",
            "at": at,
            "markets": markets,
            "markets_debug": format!("{markets:?}"),
            "calculation": calculation,
        });
        assert_no_host_coordinates(&value);
        self.callbacks.lock().unwrap().push(value);
        None
    }

    fn qualify_intent(
        &mut self,
        input: &StrategyCurrentReadV1<'_>,
        calculation: &StrategyCalculationReadV1<'_>,
        index: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        self.qualify(input, calculation, index, at)
    }

    fn qualify_intent_borrowed(
        &mut self,
        input: &BorrowedStrategyCurrentReadV1<'_>,
        calculation: &StrategyCalculationReadV1<'_>,
        index: usize,
        at: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        self.qualify(input, calculation, index, at)
    }

    fn invalidated(
        &mut self,
        calculation: &StrategyCalculationReadV1<'_>,
        result: &StrategyInvalidatedBatchV1,
    ) {
        assert!(result.sent.is_empty());
        let value = json!({
            "kind": "invalidated",
            "calculation": calculation,
            "result_debug": format!("{result:?}"),
            "cutoff": result.observed_cutoff,
            "sent": result.sent,
        });
        assert_no_host_coordinates(&value);
        self.callbacks.lock().unwrap().push(value);
    }

    fn dispatched(
        &mut self,
        calculation: &StrategyCalculationReadV1<'_>,
        result: &Result<Vec<StrategySentIntentReceiptV1>, StrategyBatchExecutionErrorV1>,
    ) {
        let calculation = serde_json::to_value(calculation).unwrap();
        assert_no_host_coordinates(&calculation);
        let sent = result.as_ref().expect("NoTrade does not call the bridge");
        assert!(sent.is_empty());
        self.dispatched.lock().unwrap().push(calculation);
    }
}

fn assert_no_host_coordinates(value: &Value) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                assert!(
                    !matches!(
                        key.as_str(),
                        "actor_input_sequence" | "drive_position" | "qualified_rebates"
                    ),
                    "host-only field {key} reached a strategy view: {value}"
                );
                assert_no_host_coordinates(value);
            }
        }
        Value::Array(values) => values.iter().for_each(assert_no_host_coordinates),
        Value::String(value) => {
            assert!(!value.contains("actor_input_sequence"));
            assert!(!value.contains("drive_position"));
            assert!(!value.contains("qualified_rebates"));
        }
        _ => {}
    }
}

struct IsolationRun {
    samples: Vec<PublicSample>,
    borrowed_frames: Vec<Value>,
    dispatched: Vec<Value>,
    callbacks: Vec<Value>,
    accepted_inputs: usize,
    preloads: usize,
    public_inputs: u64,
    host_boundary_at: i64,
}

fn isolation_run(extra_hidden_market: bool, borrowed: bool, learning: bool) -> IsolationRun {
    let root = tempfile::tempdir().unwrap();
    let (mut config, original_rules) = setup(root.path());
    if learning {
        // The actual learning entry attaches a frozen learning read to FIFO
        // inputs. Event-driven owned/borrowed inputs intentionally omit it.
        config.timing.decision_mode = HistoricalDecisionModeV1::Fixed100msFifo;
    }
    let rules = derive_offline_probabilistic_snapshot_v3(
        HistoricalProcessSnapshotRequestV1 {
            snapshot_path: original_rules.path().into(),
            expected_file_sha256: original_rules.file_sha256().clone(),
            replay_causal_start_utc_ms: BASE,
        },
        root.path().join("isolation-rules.json"),
        config.implementation_identity.clone(),
    )
    .unwrap();
    let (_, markets, market_assets, manifests) = collect_scene_facts_v1(
        &config.preparation,
        BASE - 60_000,
        config.preparation.scope.received_end_utc_ms,
        0,
    )
    .unwrap();
    let scene_path = root.path().join("isolation.scene");
    let mut writer = SceneStreamWriterV2::create(&scene_path).unwrap();
    for (sequence, (condition, token)) in [(FIRST, "20"), (SECOND, "21")]
        .into_iter()
        .take(if extra_hidden_market { 2 } else { 1 })
        .enumerate()
    {
        writer
            .push(Event {
                at_ms: BASE + 500,
                source_sequence: sequence as u64,
                level: Level {
                    condition: condition.into(),
                    buy_token: token.into(),
                    price_micro: 400_000,
                },
                kind: EventKind::Add {
                    segment: sequence as u64 + 1,
                    quantity: 10_000_000,
                    implicit: false,
                },
            })
            .unwrap();
    }
    writer
        .finish(OfflineSceneMaterialV1 {
            scene: PreparedScene {
                model_version: MODEL_VERSION.into(),
                parameters: Parameters {
                    association_ms: 250,
                    allocation: Allocation::Nearest,
                    cancellation: Cancellation::SegmentProportionalV2,
                    internal_time: InternalTime::SourceWhenPresent,
                    seed: 7,
                    association_trade_shift_ms: 0,
                },
                events: vec![],
                report: PreparationReport::default(),
            },
            markets,
            market_assets,
            source_manifests: manifests,
        })
        .unwrap();
    let candidate = HistoricalPreparedRunConfigV1::from(&config);
    let day = HistoricalDayConfigV1 {
        preparation: config.preparation,
        candidates: vec![candidate.clone()],
        progress_interval_seconds: 0,
        producer_lead_groups: Some(1),
        offline_scenes: std::collections::BTreeMap::from([(
            candidate.session.run_id.clone(),
            scene_path,
        )]),
    };
    let samples = Arc::new(Mutex::new(Vec::new()));
    let borrowed_frames = Arc::new(Mutex::new(Vec::new()));
    let dispatched = Arc::new(Mutex::new(Vec::new()));
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    let result = run_historical_days_v1(&[day], rules, &|_| {
        Ok(IsolationProbe {
            borrowed,
            learning,
            proposed: false,
            samples: samples.clone(),
            borrowed_frames: borrowed_frames.clone(),
            dispatched: dispatched.clone(),
            callbacks: callbacks.clone(),
        })
    })
    .unwrap();
    assert_eq!(result[0].candidates[0].report["end_reason"], "RangeEnd");
    let log = sim_log_values(&candidate.sim_log);
    let day_layout = std::fs::read(&candidate.sim_log)
        .unwrap()
        .starts_with(b"SIMDAY01");
    let accepted: Vec<&Value> = if day_layout {
        log.iter()
            .skip(1)
            .filter_map(|frame| frame.as_array()?.first()?.as_array())
            .flatten()
            .collect()
    } else {
        log.iter()
            .filter_map(|frame| match frame["kind"].as_str()? {
                "ACCEPTED_INPUTS" => frame["body"].as_array(),
                "ACCEPTED_INPUTS_V3" => frame["body"]["inputs"].as_array(),
                _ => None,
            })
            .flatten()
            .collect()
    };
    // By the end both markets have become public and both runs have preloaded
    // them. Compare the accepted host prefix at the first shared public group
    // after the hidden scene, before SECOND's public identity at BASE + 6000.
    let public_at = |input: &Value| {
        if day_layout {
            (input[0].as_u64() == Some(3))
                .then(|| input[2].as_i64())
                .flatten()
        } else {
            (input["input"] == "PUBLIC_GROUP")
                .then(|| input["body"]["execution_time"]["logical_time_utc_ms"].as_i64())
                .flatten()
        }
    };
    let boundary = accepted
        .iter()
        .position(|input| {
            public_at(input).is_some_and(|at| (BASE + 1000..BASE + 6000).contains(&at))
        })
        .expect("a common public group must follow hidden preload and precede SECOND identity");
    let host_boundary_at = public_at(accepted[boundary]).unwrap();
    let accepted_inputs = boundary + 1;
    let preloads = accepted[..=boundary]
        .iter()
        .filter(|input| {
            serde_json::to_string(input)
                .unwrap()
                .contains("PRELOAD_MARKET")
        })
        .count();
    IsolationRun {
        samples: Arc::try_unwrap(samples).unwrap().into_inner().unwrap(),
        borrowed_frames: Arc::try_unwrap(borrowed_frames)
            .unwrap()
            .into_inner()
            .unwrap(),
        dispatched: Arc::try_unwrap(dispatched).unwrap().into_inner().unwrap(),
        callbacks: Arc::try_unwrap(callbacks).unwrap().into_inner().unwrap(),
        accepted_inputs,
        preloads,
        host_boundary_at,
        public_inputs: result[0].public_applied_inputs,
    }
}

#[test]
fn hidden_preload_cannot_change_owned_or_borrowed_strategy_input() {
    for (borrowed, learning) in [(false, false), (true, false), (false, true)] {
        let ordinary = isolation_run(false, borrowed, learning);
        let hidden = isolation_run(true, borrowed, learning);
        assert!(!ordinary.samples.is_empty());
        assert!(!ordinary.dispatched.is_empty());
        assert!(ordinary.samples.iter().any(|sample| {
            let at = sample.value["at"].as_i64().unwrap();
            (BASE + 500..BASE + 6000).contains(&at)
        }));
        assert_eq!(ordinary.public_inputs, hidden.public_inputs);
        assert_eq!(ordinary.samples, hidden.samples);
        assert_eq!(ordinary.borrowed_frames, hidden.borrowed_frames);
        assert_eq!(ordinary.dispatched, hidden.dispatched);
        assert_eq!(ordinary.callbacks, hidden.callbacks);
        for kind in ["health", "qualify", "invalidated"] {
            assert!(
                ordinary
                    .callbacks
                    .iter()
                    .any(|sample| sample["kind"] == kind),
                "the fixture must execute the real {kind} callback"
            );
        }
        if borrowed {
            assert!(
                ordinary
                    .callbacks
                    .iter()
                    .any(|sample| sample["kind"] == "detached")
            );
        }
        assert_eq!(ordinary.host_boundary_at, hidden.host_boundary_at);
        assert_eq!(hidden.preloads, ordinary.preloads + 1);
        assert_eq!(
            hidden.accepted_inputs,
            ordinary.accepted_inputs + 1,
            "the shared public boundary retains one extra accepted hidden host input"
        );
        eprintln!(
            "strategy-isolation borrowed={borrowed} learning={learning} public_inputs={} decisions={} other_views={} dispatched={} host_prefix_inputs={}/{} preloads={}/{}",
            ordinary.public_inputs,
            ordinary.samples.len(),
            ordinary.callbacks.len(),
            ordinary.dispatched.len(),
            ordinary.accepted_inputs,
            hidden.accepted_inputs,
            ordinary.preloads,
            hidden.preloads,
        );
    }
}
