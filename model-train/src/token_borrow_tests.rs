//! #368固定真实路径比较。所有公共/账户输入均来自有限合成夹具，无神经库。
use super::*;
use backtest::run_historical_days_with_parallelism_v1;
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
fn digest(b: &[u8]) -> String {
    format!("{:x}", Sha256::digest(b))
}
fn ticks() -> u64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap();
    let f = s[s.rfind(')').unwrap() + 2..]
        .split_whitespace()
        .collect::<Vec<_>>();
    f[11].parse::<u64>().unwrap() + f[12].parse::<u64>().unwrap()
}
fn memory() -> String {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .filter(|s| s.starts_with("VmRSS:") || s.starts_with("VmHWM:"))
        .collect::<Vec<_>>()
        .join("; ")
}
fn root() -> PathBuf {
    std::env::var_os("TOKEN_BORROW_ROOT")
        .expect("显式夹具根")
        .into()
}
fn worker(out: &Path) -> ModelWorker {
    std::fs::create_dir_all(out).unwrap();
    let control = out.join("worker.json");
    std::fs::write(&control, serde_json::to_vec(&json!({"out":out})).unwrap()).unwrap();
    ModelWorker::start(crate::worker::WorkerConfig {
        performance: None,
        python: "python3".into(),
        script: Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/token-borrow-worker.py"),
        model: control,
        device: "cpu".into(),
        torch_threads: 1,
        max_batch: 1,
        encoding: EncodingConfig {
            field_buckets: 4096,
            category_buckets: 4096,
        },
    })
    .unwrap()
}
fn private(input: &StrategyInputV1, m: &FrozenMarketReadV1, n: usize) -> Value {
    let account = repair_eligibility(input, m).unwrap().1;
    let feedback=(0..n).map(|i| json!({"token_id":m.observation.assets[0].asset.token_id,"matched_shares":format!("{}.{:02}",i%5,i%100),"active_unfilled_shares":"2.00","accepted":true,"formed":true,"latest_lifecycle":null,"latest_direct_feedback":null,"cancellable":false})).collect::<Vec<_>>();
    json!({"account":account,"order_feedback":feedback,"funds":{"principal":"0","fee":"0","local_principal":"0","local_fee":"0","platform_pending_principal":"0","platform_pending_fee":"0","pending_count":0}})
}
struct Capture {
    inner: Option<ModelConsumer>,
    worker: Option<ModelWorker>,
    out: PathBuf,
    case: Option<(usize, bool)>,
    result: Arc<Mutex<Option<Value>>>,
    preparation_started: Instant,
    preparation_cpu: u64,
}
impl StrategyV1 for Capture {
    type Error = String;
    type Diagnostics = Value;
    fn requires_source_progress(&self) -> bool {
        false
    }
    fn observation_requirements(&self) -> PublicObservationRequirementsV1 {
        crate::public_input::OBSERVATION
    }
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1<Value>, String> {
        assert!(!input.markets.is_empty());
        let public = input
            .markets
            .iter()
            .map(|m| market_projection(m, at))
            .collect::<Vec<_>>();
        let snapshot = json!({"at":at,"public":public,"time":time_projection(input,at),"market_ids":input.markets.iter().map(|m|&m.observation.market.condition_id).collect::<Vec<_>>()});
        if self.case.is_none() {
            let mut cases = Vec::new();
            for n in [4usize, 1024] {
                let values = input
                    .markets
                    .iter()
                    .map(|m| private(input, m, n))
                    .collect::<Vec<_>>();
                let bytes = serde_json::to_vec(&values).unwrap();
                cases.push(json!({"feedback_entries_per_market":n,"market_private":values,"private_json_bytes":bytes.len(),"private_sha256":digest(&bytes)}));
            }
            let frozen = json!({"snapshot":snapshot,"cases":cases,"iterations_per_path_size":64,"total_per_version":256,"label":{"advantage":1.25,"critic_target":-2,"old_joint_logp":-0.5,"unknown":{"necessary":["Unavailable"],"value":null}}});
            std::fs::write(
                root().join("fixtures/frozen.json"),
                serde_json::to_vec_pretty(&frozen).unwrap(),
            )
            .unwrap();
            *self.result.lock().unwrap() = Some(json!({"prepare_only":true,"snapshot":snapshot}));
        } else {
            let frozen: Value = serde_json::from_slice(
                &std::fs::read(root().join("fixtures/frozen.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(
                snapshot, frozen["snapshot"],
                "公共/时刻/身份必须在计时前严格匹配冻结输入"
            );
            let (size, learn) = self.case.unwrap();
            let values: Vec<Value> =
                serde_json::from_value(frozen["cases"][size]["market_private"].clone()).unwrap();
            let prep = self.preparation_started;
            let pcpu = self.preparation_cpu;
            let mut row = crate::strategy::ipc_reduction_tests::row(&input.markets, 0);
            row["market_private"] = json!(values);
            row["processing_start_utc_ms"] = at.into();
            let label = frozen["label"].clone();
            if !learn {
                self.inner.as_mut().unwrap().test_market_private =
                    (0..64).flat_map(|_| values.clone()).collect();
            }
            let prep_wall = prep.elapsed().as_secs_f64();
            let prep_ticks = ticks() - pcpu;
            let mem = memory();
            let begin = Instant::now();
            let cpu = ticks();
            let mut injection_counts = Vec::new();
            for _ in 0..64 {
                if learn {
                    let w = self.worker.as_ref().unwrap();
                    let frame =
                        crate::learning::ipc_cost_frame(&row, &input.markets, &w.config.encoding)
                            .unwrap();
                    w.request(json!({"phase":"learn_step","frame":frame,"label":label}))
                        .unwrap();
                } else {
                    let before = self.inner.as_ref().unwrap().test_market_private.len();
                    self.inner.as_mut().unwrap().decide(input, at)?;
                    let after = self.inner.as_ref().unwrap().test_market_private.len();
                    assert_eq!(before - after, input.markets.len());
                    injection_counts
                        .push(json!({"market_count":input.markets.len(),"injected":before-after}));
                }
            }
            let wall = begin.elapsed().as_secs_f64();
            let cpu = ticks() - cpu;
            if !learn {
                assert!(self.inner.as_ref().unwrap().test_market_private.is_empty());
            }
            let record = json!({"path":if learn{"ipc_cost_frame->compose_learn_step_frame->trajectory_tokens->worker"}else{"Capture::decide->ModelConsumer::decide->worker"},"size":size,"iterations":64,"wall_seconds":wall,"rust_cpu_ticks":cpu,"ticks_per_second":100,"prepare_wall_seconds":prep_wall,"prepare_cpu_ticks":prep_ticks,"memory_before":mem,"memory_after":memory(),"private_json_bytes":frozen["cases"][size]["private_json_bytes"],"private_sha256":frozen["cases"][size]["private_sha256"],"market_count":input.markets.len(),"decide_per_call_injection":injection_counts,"snapshot":snapshot,"whole_rpc":row["rpc"],"whole_label":label});
            std::fs::write(
                self.out.join("cost.json"),
                serde_json::to_vec_pretty(&record).unwrap(),
            )
            .unwrap();
            *self.result.lock().unwrap() = Some(record);
        }
        // 只需要首个有效合成输入；不产生额外decide/sample/warmup调用。
        Ok(StrategyOutputV1::Exit {
            diagnostics: Value::Null,
        })
    }
}
fn run_capture(
    config: &crate::ConsumerRunConfig,
    out: &Path,
    case: Option<(usize, bool)>,
) -> Value {
    let preparation_started = Instant::now();
    let preparation_cpu = ticks();
    std::fs::create_dir_all(out).unwrap();
    let mut value = serde_json::to_value(config).unwrap();
    value["days"][0]["preparation"]["work_root"] = json!(out.join("prepared"));
    for (key, name) in [
        ("research_evidence_path", "decisions.jsonl"),
        ("sim_log", "sim.jsonl"),
        ("noe_root", "noe"),
        ("ledger_root", "ledger"),
        ("metadata_path", "metadata.json"),
        ("result_path", "result.json"),
    ] {
        value["days"][0]["candidates"][0][key] = json!(out.join(name));
    }
    let cfg: crate::ConsumerRunConfig = serde_json::from_value(value).unwrap();
    let opened = crate::open_consumer_rules(&cfg).unwrap();
    let prepared = Instant::now();
    let pcpu = ticks();
    let fake = case.map(|_| worker(&out.join("worker")));
    let startup = json!({"wall_seconds":prepared.elapsed().as_secs_f64(),"cpu_ticks":ticks()-pcpu});
    let result = Arc::new(Mutex::new(None));
    run_historical_days_with_parallelism_v1(
        &cfg.days,
        opened.rules.clone(),
        &|c| {
            let inner = fake.as_ref().map(|w| {
                ModelConsumer::new(
                    serde_json::from_value(
                        c.strategy.effective_parameters["model_consumer"].clone(),
                    )
                    .unwrap(),
                    w.clone(),
                    opened.rules.clone(),
                    c.effective_entry_market_scope.clone().unwrap(),
                    &opened.binding,
                )
                .unwrap()
            });
            Ok(Capture {
                inner,
                worker: fake.clone(),
                out: out.into(),
                case,
                result: result.clone(),
                preparation_started,
                preparation_cpu,
            })
        },
        1,
    )
    .unwrap();
    std::fs::write(
        out.join("worker-startup.json"),
        serde_json::to_vec_pretty(&startup).unwrap(),
    )
    .unwrap();
    let result = result.lock().unwrap().take().expect("收到一个有效输入");
    result
}
#[test]
#[ignore = "#368显式预备：不执行比较路径或模型"]
fn freeze_token_borrow_inputs() {
    let r = root();
    let material = r.join("fixtures/material");
    std::fs::create_dir(&material).unwrap();
    let (rules, mut day) = crate::learning::learning_tests::material(&material);
    let c = &mut day.candidates[0];
    c.session.caller_role = SimCallerRoleV1::ModelTrain;
    c.session.input_lifecycle_policy =
        sim_system::public_consumption::InputLifecyclePolicyV1::FiniteInput;
    c.gamma_resolution_policy =
        sim_system::public_consumption::GammaResolutionPolicyV1::QualifiedPublicConsumption;
    c.timing.decision_mode = backtest::HistoricalDecisionModeV1::Fixed100msFifo;
    c.entry_market_scope = Some(backtest::EntryMarketScopeV1::default());
    let cfg = crate::ConsumerRunConfig {
        rules: rules.into(),
        days: vec![day],
        parallel_dates: 1,
        worker: crate::worker::WorkerConfig {
            performance: None,
            python: "unused".into(),
            script: "unused".into(),
            model: "unused".into(),
            device: "cpu".into(),
            torch_threads: 1,
            max_batch: 1,
            encoding: EncodingConfig {
                field_buckets: 4096,
                category_buckets: 4096,
            },
        },
    };
    let cfg = crate::public_input::bind(&cfg);
    std::fs::write(
        r.join("fixtures/run.json"),
        serde_json::to_vec_pretty(&cfg).unwrap(),
    )
    .unwrap();
    run_capture(&cfg, &r.join("fixtures/prepare"), None);
}
#[test]
#[ignore = "#368固定每版256次，仅授权时显式一次"]
fn token_borrow_cost_comparison() {
    let r = root();
    let out = PathBuf::from(std::env::var_os("TOKEN_BORROW_OUTPUT").expect("显式唯一输出"));
    std::fs::create_dir(&out).unwrap();
    let cfg: crate::ConsumerRunConfig =
        serde_json::from_slice(&std::fs::read(r.join("fixtures/run.json")).unwrap()).unwrap();
    let mut costs = Vec::new();
    for size in 0..2 {
        for learn in [false, true] {
            eprintln!(
                "#368 {:?} size={size},64次",
                if learn { "trajectory" } else { "decide" }
            );
            costs.push(run_capture(
                &cfg,
                &out.join(format!("{}-{size}", if learn { "learn" } else { "decide" })),
                Some((size, learn)),
            ));
        }
    }
    std::fs::write(
        out.join("costs.json"),
        serde_json::to_vec_pretty(&costs).unwrap(),
    )
    .unwrap();
}
