//! 无模型：真实采集请求与生产学习公共再生接线，而不是两次visitor互证。
use super::*;
use crate::{
    ConsumerRunConfig, ModelConsumer, ModelDiagnostics, encoding::market_projection,
    worker::ModelWorker,
};
use backtest::*;
use shared_run::{StrategyInputV1, StrategyOutputV1, StrategyV1};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

struct Capture {
    inner: ModelConsumer,
    progress: bool,
    observed: Arc<Mutex<Vec<(i64, Value)>>>,
}
impl StrategyV1 for Capture {
    type Error = String;
    type Diagnostics = ModelDiagnostics;
    fn active_until_utc_ms(&self) -> Option<i64> {
        self.inner.active_until_utc_ms()
    }
    fn requires_learning_economics(&self) -> bool {
        self.inner.requires_learning_economics()
    }
    fn requires_trade_economics_details(&self) -> bool {
        false
    }
    fn observation_requirements(&self) -> PublicObservationRequirementsV1 {
        self.inner.observation_requirements()
    }
    fn requires_source_progress(&self) -> bool {
        self.progress || self.inner.requires_source_progress()
    }
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1<Self::Diagnostics>, String> {
        self.observed.lock().unwrap().push((
            at,
            json!(
                input
                    .markets
                    .iter()
                    .map(|m| market_projection(m, at))
                    .collect::<Vec<_>>()
            ),
        ));
        self.inner.decide(input, at)
    }
}
pub(crate) fn collect(
    root: &std::path::Path,
    progress: bool,
) -> (
    ConsumerRunConfig,
    Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
    Vec<Value>,
    Vec<Value>,
) {
    let (rules, mut day) = super::learning_tests::material(root);
    crate::learning_test_fixture::append_reference_inputs(&root.join("input"));
    let end = day.preparation.scope.received_end_utc_ms;
    let c = &mut day.candidates[0];
    c.session.caller_role = SimCallerRoleV1::ModelTrain;
    c.session.input_lifecycle_policy =
        sim_system::public_consumption::InputLifecyclePolicyV1::FiniteInput;
    c.gamma_resolution_policy =
        sim_system::public_consumption::GammaResolutionPolicyV1::QualifiedPublicConsumption;
    c.timing.decision_mode = HistoricalDecisionModeV1::Fixed100msFifo;
    c.entry_market_scope = Some(EntryMarketScopeV1::default());
    c.strategy.effective_parameters["model_consumer"]["learning"] = json!({"group_id":"input-assembly-v2","active_end_utc_ms":end,"evidence_end_utc_ms":end,"trajectory_path":root.join("trajectory.jsonl"),"collection_fault_after_call":null});
    let config: ConsumerRunConfig = serde_json::from_value(json!({"rules":rules,"days":[day],"parallel_dates":1,"worker":{"python":"python3","script":std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/public-input-worker.py"),"model":root.join("requests.jsonl"),"device":"cpu","torch_threads":1,"max_batch":1,"encoding":{"field_buckets":4096,"category_buckets":4096}}})).unwrap();
    let config = crate::public_input::bind(&config);
    let opened = crate::open_consumer_rules(&config).unwrap();
    let worker = ModelWorker::start(config.worker.clone()).unwrap();
    let observed = Arc::new(Mutex::new(vec![]));
    run_historical_days_with_parallelism_v1(
        &config.days,
        opened.rules.clone(),
        &|c| {
            Ok(Capture {
                inner: ModelConsumer::new(
                    serde_json::from_value(
                        c.strategy.effective_parameters["model_consumer"].clone(),
                    )?,
                    worker.clone(),
                    opened.rules.clone(),
                    c.effective_entry_market_scope.clone().unwrap(),
                    &opened.binding,
                )?,
                progress,
                observed: observed.clone(),
            })
        },
        1,
    )
    .unwrap();
    drop(worker);
    let observations = observed
        .lock()
        .unwrap()
        .iter()
        .map(|(t, v)| json!({"at":t,"public":v}))
        .collect();
    let rows = std::fs::read_to_string(root.join("trajectory.jsonl"))
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str::<Value>(s).unwrap())
        .filter(|v| v["call"].is_number())
        .collect::<Vec<_>>();
    eprintln!(
        "合成FIFO必要前缀：{:?}",
        rows.iter()
            .map(|r| (
                r["market_frame"]["market_cutoff"]["decision_time"]["logical_time_utc_ms"]
                    .as_i64()
                    .unwrap()
                    - crate::learning_test_fixture::common::BASE,
                r["processing_start_utc_ms"].as_i64().unwrap()
                    - crate::learning_test_fixture::common::BASE
            ))
            .collect::<Vec<_>>()
    );
    (config, opened.rules, rows, observations)
}
#[test]
fn actual_collection_and_production_replay_keep_same_f9_and_tokens_without_source_progress() {
    let root = tempfile::tempdir().unwrap();
    let (config, rules, rows, observed) = collect(root.path(), false);
    assert!(!rows.is_empty());
    let requests = std::fs::read_to_string(root.path().join("requests.jsonl"))
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str::<Value>(s).unwrap())
        .filter(|r| r["phase"] == "begin")
        .collect::<Vec<_>>();
    assert_eq!(requests.len(), rows.len());
    let queries: BTreeMap<_, _> = [(
        "a".into(),
        rows.iter()
            .map(|r| {
                r["market_frame"]["market_cutoff"]["decision_time"]["logical_time_utc_ms"]
                    .as_i64()
                    .unwrap()
            })
            .collect(),
    )]
    .into_iter()
    .collect();
    let mut matched = 0;
    let streams = visit_day_frames(
        &config
            .rules
            .frozen()
            .map(|b| b.public_rules())
            .unwrap_or_else(|| match &config.rules {
                crate::ConsumerRulesV1::Historical(r) => r.clone(),
                _ => panic!("fixture"),
            }),
        Some(rules.clone()),
        &config.days[0],
        &queries,
        |_, at, g, frames| {
            for (i, row) in rows.iter().enumerate().filter(|(_, r)| {
                r["market_frame"]["market_cutoff"]["decision_time"]["logical_time_utc_ms"] == at
            }) {
                assert_eq!(
                    serde_json::to_value(g)?,
                    row["market_frame"]["market_cutoff"]["receipt"]["group"]
                );
                let (frame, projection) =
                    compose_trajectory_frame(row, frames, &config.worker.encoding, true)?;
                assert_eq!(
                    token_rows(&json!([frame["global"]])),
                    token_rows(&json!([requests[i]["global"]]))
                );
                assert_eq!(
                    token_rows(&frame["markets"]),
                    token_rows(&requests[i]["markets"])
                );
                assert_eq!(
                    json!(
                        projection["markets"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|p| &p["public"])
                            .collect::<Vec<_>>()
                    ),
                    observed[i]["public"]
                );
                assert_eq!(row["version"], crate::public_input::TRAJECTORY_VERSION);
                matched += 1;
            }
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(streams, 1);
    assert_eq!(matched, rows.len());
    // 保留旧off/on失败条件：同一合法组，在来源进展非空后，投影和实际token不同。
    let day = &config.days[0];
    let c = &day.candidates[0];
    let public = ContinuousPublicExportConfigV1 {
        rules: PublicExportRulesV1 {
            snapshot_path: rules.path().into(),
            expected_file_sha256: rules.file_sha256().clone(),
        },
        preparation: day.preparation.clone(),
        markets: PublicObservationMarketsV1::All,
        statistics: c.public_statistics.clone().unwrap(),
        public_delivery_delay_ms: c.timing.public_delivery_delay_ms.clone(),
        observe_after_groups: false,
        query_times_utc_ms: queries["a"].iter().copied().collect(),
        export_new_trades: false,
    };
    let mut difference = false;
    visit_continuous_public_frames_with_requirements_v1(
        &public,
        rules,
        crate::public_input::OBSERVATION,
        true,
        |at, g, frames| {
            for (i, row) in rows.iter().enumerate().filter(|(_, r)| {
                r["market_frame"]["market_cutoff"]["decision_time"]["logical_time_utc_ms"] == at
            }) {
                assert_eq!(
                    serde_json::to_value(g)?,
                    row["market_frame"]["market_cutoff"]["receipt"]["group"]
                );
                let (frame, projection) =
                    compose_trajectory_frame(row, &frames, &config.worker.encoding, true)?;
                if frames
                    .iter()
                    .any(|f| !f.observation.source_progress.is_empty())
                {
                    assert_ne!(
                        json!(
                            projection["markets"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .map(|p| &p["public"])
                                .collect::<Vec<_>>()
                        ),
                        observed[i]["public"]
                    );
                    assert_ne!(
                        token_rows(&frame["markets"]),
                        token_rows(&requests[i]["markets"])
                    );
                    difference = true;
                }
            }
            Ok(())
        },
        |_| {},
        || false,
    )
    .unwrap();
    assert!(difference);
    let text = serde_json::to_string(&observed).unwrap();
    for name in ["quality", "missing", "coverage", "source_applied_at_utc_ms"] {
        assert!(text.contains(name), "必要F9仍在: {name}");
    }
}
#[test]
fn explicit_source_evidence_merges_with_full_statistics_without_health_policy() {
    let root = tempfile::tempdir().unwrap();
    let (_, _, _, observed) = collect(root.path(), true);
    eprintln!(
        "独立来源需求：帧数={}，每帧来源数={:?}",
        observed.len(),
        observed
            .iter()
            .map(|v| v["public"]
                .as_array()
                .unwrap()
                .iter()
                .map(|m| m["f0"]["source_progress"].as_array().map(Vec::len))
                .collect::<Vec<_>>())
            .collect::<Vec<_>>()
    );
    assert!(
        observed
            .iter()
            .any(|v| v["public"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m["f0"]["source_progress"]
                    .as_array()
                    .is_some_and(|p| !p.is_empty())))
    );
}
#[test]
fn legacy_checkpoint_and_consumed_group_are_not_silently_upgraded() {
    let output = std::process::Command::new("python3")
        .arg(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/checkpoint-version-check.py"),
        )
        .arg(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("python/learning_engine.py"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

// 实际Policy.encode把数值token转换为f32；JSON的f64尾差不是新精度政策。
fn token_rows(value: &Value) -> Vec<Vec<(u32, u32, f32, u8)>> {
    serde_json::from_value(value.clone()).unwrap()
}
#[test]
fn old_trajectory_is_read_only_and_existing_effective_binding_records_new_input_semantics() {
    let root = tempfile::tempdir().unwrap();
    let (config, _) = super::learning_tests::collection(root.path(), true);
    let current = crate::public_input::bind(&config.consumer);
    assert_ne!(
        serde_json::to_value(&current).unwrap(),
        serde_json::to_value(&config.consumer).unwrap()
    );
    assert_eq!(
        serde_json::to_value(crate::public_input::bind(&current)).unwrap(),
        serde_json::to_value(&current).unwrap()
    );
    let path = root.path().join("trajectory-1.jsonl");
    let mut frame: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    frame["version"] = json!("learning-trajectory-v1");
    std::fs::write(&path, format!("{frame}\n")).unwrap();
    assert_eq!(
        Trajectory::open(&path, 4, 2).err().unwrap().to_string(),
        "未知轨迹版本"
    );
}
