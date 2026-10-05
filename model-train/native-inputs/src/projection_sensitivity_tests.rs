//! 专用单测试进程：真实公共组、共同外生BUY历史和末端投影回填。
use super::*;
use crate::exact::projection_diagnostic;
use pyo3::prelude::*;
use std::path::PathBuf;

fn common_actor(group: Arc<NativeInputGroup>) -> Option<NativeActor> {
    let row = group
        .rows
        .iter()
        .position(|row| matches!(row.role, InputRole::DecisionRequired))?;
    let mut actor = NativeActor::new(group, row).unwrap();
    let support = actor.support().unwrap();
    if support[2] {
        actor.choose(2).unwrap();
        let quantity = BigUint::from(500u32);
        assert!(actor.state.interval.as_ref().unwrap().hi >= quantity);
        while actor.state.stage == Stage::QuantityBranch {
            actor
                .choose(segment_for(
                    actor.state.interval.as_ref().unwrap(),
                    &quantity,
                ))
                .unwrap();
        }
        actor.choose(0).unwrap();
        actor.choose(0).unwrap();
        // 固定同一精确5股GTC双腿；可用价格由原生支持集选择最小合法坐标。
        while !actor.is_complete() {
            let choice = actor
                .support()
                .unwrap()
                .iter()
                .position(|legal| *legal)
                .unwrap();
            actor.choose(choice).unwrap();
        }
    } else {
        assert!(support[0]);
        actor.choose(0).unwrap();
    }
    Some(actor)
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "显式单测试：捕获真实末端投影，或回填实际CUDA结果；固定未训练模型，无训练数据"]
async fn actual_component_projection_sensitivity() {
    let output = std::env::var("PROJECTION_SENSITIVITY_OUTPUT").expect("必须指定诊断报告路径");
    let policy =
        std::env::var("PROJECTION_SENSITIVITY_POLICY").unwrap_or_else(|_| "exact_cpu".into());
    let table: Option<serde_json::Value> = std::env::var("PROJECTION_SENSITIVITY_REPLACEMENTS")
        .ok()
        .map(|path| serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap());
    assert_eq!(
        policy == "exact_cpu",
        table.is_none(),
        "候选必须显式提供CUDA替换表"
    );
    let _session = projection_diagnostic::begin(&policy, table.as_ref());
    let mut h = fixture_with_midpoints(
        "2000",
        ["0.01", "0.01"],
        24_999,
        &[35_000, 45_000, 55_000],
        ["100", "100"],
        "5",
        &[
            (22_001, "0.1", "0.3"),
            (22_002, "0.3", "0.5"),
            (22_003, "0.2", "0.4"),
        ],
    );
    let mut groups = Vec::new();
    let mut paths = Vec::new();
    let mut external_intent = None;
    loop {
        projection_diagnostic::context(format!("public_group_{}", groups.len()));
        let Some(group) = h.next() else { break };
        let at = group.cutoff.decision_time().logical_time_utc_ms();
        let group = Arc::new(group);
        projection_diagnostic::context(format!("common_actor_{}", groups.len()));
        let actor = common_actor(group.clone());
        paths.push(actor.as_ref().map(|actor| actor.path.clone()));
        groups.push(group);
        if external_intent.is_none() && at >= 24_999 {
            let actor = actor.unwrap();
            let intent = actor.intent().unwrap().unwrap();
            assert!(matches!(intent, ModelIntentV1::SubmitPairedBuy { .. }));
            external_intent = Some(format!("{intent:?}"));
            h.shared
                .execute(
                    &mut h.driver.decision().unwrap(),
                    context("projection-common-buy"),
                    Some(Arc::new(intent)),
                )
                .await
                .unwrap()
                .unwrap();
        }
        // 既有BUY夹具是稳定有限前缀，没有EndOfInput；仅消费其已给定的最后一个公开组。
        if at == 56_000 {
            break;
        }
    }
    assert!(external_intent.is_some());
    projection_diagnostic::context("continuous_model_and_actor");
    let component_result: Result<String, String> = Python::attach(|py| {
        let native = groups
            .into_iter()
            .map(|group| Py::new(py, crate::python::PythonGroup { group }))
            .collect::<PyResult<Vec<_>>>()?;
        let module = py.import("model_train.projection_sensitivity")?;
        if let Ok(stream) = std::env::var("PROJECTION_SENSITIVITY_FIXTURE_STREAM_OUTPUT") {
            assert_eq!(policy, "exact_cpu", "流式兼容检查仅运行原精确有限夹具");
            module
                .getattr("verify_streaming_fixture")?
                .call1((native, paths, stream, 742001u64))?
                .extract::<String>()
        } else {
            module
                .getattr("run")?
                .call1((native, paths, 742001u64))?
                .extract::<String>()
        }
    })
    .map_err(|error: PyErr| error.to_string());
    let captured = projection_diagnostic::snapshot();
    assert!(
        captured["rows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| { row["operation"] == "sqrt_log_f32" && row["input"]["numerator"] != "0" }),
        "真实BBA序列必须产生非零RMS末端输入"
    );
    let (components, error) = match component_result {
        Ok(value) => (
            serde_json::from_str::<serde_json::Value>(&value).unwrap(),
            None,
        ),
        Err(error) => (serde_json::Value::Null, Some(error)),
    };
    let status = if error.is_some() {
        "failed"
    } else if captured["missing_replacements"] != 0 {
        "discovery_only"
    } else {
        "complete"
    };
    let report = serde_json::json!({
        "schema":"projection_sensitivity_run_v1", "policy":policy, "status":status,
        "scope":"公共短BUY/BBA夹具、固定MAIN-1x256连续状态与Actor敏感性；无训练、收益或GPU常驻性能结论",
        "external_intent":external_intent, "capture":captured, "components":components, "error":error,
    });
    std::fs::write(output, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    h.driver.stop_and_drain().unwrap();
    assert!(error.is_none(), "Python组件诊断失败：{error:?}");
}

const REAL_START: i64 = 1_789_445_700_000; // 2026-09-15 04:15 UTC
const REAL_END: i64 = REAL_START + 300_000;
const REAL_WARMUP: i64 = REAL_START - 30_000;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedProjectionConfig {
    prepared: PathBuf,
    prepared_sha256: String,
    run: backtest::HistoricalPreparedRunConfigV1,
    rules: HistoricalRulesConfig,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoricalRulesConfig {
    snapshot_path: PathBuf,
    expected_file_sha256: Sha256Hex,
    replay_causal_start_utc_ms: i64,
}

fn read_artifact(path: &std::path::Path) -> (serde_json::Value, serde_json::Value) {
    use sha2::{Digest, Sha256};
    let bytes = std::fs::read(path).unwrap();
    (
        serde_json::from_slice(&bytes).unwrap(),
        serde_json::json!({"path":path,"sha256":format!("{:x}",Sha256::digest(&bytes)),"bytes":bytes.len()}),
    )
}

struct NoOpHistory;
impl shared_run::StrategyV1 for NoOpHistory {
    type Error = std::convert::Infallible;
    type Diagnostics = ();
    fn decide(
        &mut self,
        _: &shared_run::StrategyInputV1,
        _: i64,
    ) -> Result<shared_run::StrategyOutputV1, Self::Error> {
        Ok(shared_run::StrategyOutputV1::Continue {
            intents: Vec::new(),
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        })
    }
}

struct ProjectionObserver {
    native: Option<NativeInputRoot>,
    snapshot: Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
    python: Py<PyAny>,
    public_groups: usize,
    native_groups: usize,
}
impl backtest::HistoricalPublicGroupObserverV1 for ProjectionObserver {
    fn observe_public_group(
        &mut self,
        shared: &mut SharedExecutionV2,
        driver: &mut PublicConsumptionStateV1,
    ) -> Result<(), backtest::MaterialError> {
        let at = driver
            .decision()
            .ok_or("公共observer缺少真实decision")?
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        self.public_groups += 1;
        if at < REAL_WARMUP {
            return Ok(());
        }
        if at >= REAL_END {
            return Err("公共observer越过已批准诊断窗口".into());
        }
        if self.native.is_none() {
            let account = shared
                .ledger()
                .projection_v2()
                .ok_or("缺少实际开户账户")?
                .account_instance_key;
            self.native = Some(NativeInputRoot::new(
                "projection-real-20260915-0415".into(),
                account,
                REAL_WARMUP,
                self.snapshot.clone(),
            ));
        }
        projection_diagnostic::context(if at < REAL_START {
            "real_public_warmup"
        } else {
            "real_public_diagnostic"
        });
        let group = self
            .native
            .as_mut()
            .unwrap()
            .consume(shared, driver)
            .map_err(|error| format!("NativeInputRoot::consume: {error:?}"))?;
        if let Some(group) = group {
            projection_diagnostic::context(if at < REAL_START {
                "real_model_warmup"
            } else {
                "real_model_diagnostic"
            });
            Python::attach(|py| {
                let group = Py::new(
                    py,
                    crate::python::PythonGroup {
                        group: Arc::new(group),
                    },
                )?;
                self.python.call_method1(py, "step", (group,))?;
                Ok::<(), PyErr>(())
            })?;
            self.native_groups += 1;
        }
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "显式真实prepared诊断；固定训练内部五分钟、共同NoOp、前32组局部梯度；不训练"]
async fn prepared_component_projection_sensitivity() {
    use sim_system::platform_rules::{
        HistoricalProcessSnapshotRequestV1, load_historical_process_snapshot_v3,
    };
    let config_path = PathBuf::from(
        std::env::var("PROJECTION_SENSITIVITY_PREPARED_CONFIG")
            .expect("必须指定已核来源与运行配置"),
    );
    let (config_value, config_evidence) = read_artifact(&config_path);
    let config: PreparedProjectionConfig = serde_json::from_value(config_value).unwrap();
    let (_, prepared_evidence) = read_artifact(&config.prepared);
    assert_eq!(
        prepared_evidence["sha256"], config.prepared_sha256,
        "准备材料摘要不符"
    );
    let material = backtest::PreparedHistoricalMaterialV1::read(&config.prepared).unwrap();
    let scope = &material.preparation.scope;
    assert!(
        scope.received_start_utc_ms >= 1_788_912_000_000
            && scope.received_start_utc_ms <= REAL_WARMUP,
        "来源前史必须位于已授权训练范围内并覆盖T−30"
    );
    assert_eq!(scope.received_end_utc_ms, REAL_END);
    assert!(
        config.rules.replay_causal_start_utc_ms <= scope.received_start_utc_ms,
        "规则历史核验不得晚于公共前史开始"
    );
    assert_eq!(
        config.run.session.initial_account_state.available_pusd.0,
        "2000"
    );
    let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: config.rules.snapshot_path.clone(),
        expected_file_sha256: config.rules.expected_file_sha256.clone(),
        replay_causal_start_utc_ms: config.rules.replay_causal_start_utc_ms,
    })
    .unwrap();
    let (_, finality_evidence) = read_artifact(&config.run.finality_policy_path);
    let mut runtime_semantics = serde_json::to_value(&config.run).unwrap();
    let runtime = runtime_semantics.as_object_mut().unwrap();
    for field in [
        "sim_log",
        "noe_root",
        "ledger_root",
        "metadata_path",
        "result_path",
        "finality_policy_path",
    ] {
        runtime.remove(field);
    }
    runtime.insert(
        "finality_policy_sha256".into(),
        finality_evidence["sha256"].clone(),
    );
    runtime.insert(
        "rule_snapshot_sha256".into(),
        serde_json::to_value(&config.rules.expected_file_sha256).unwrap(),
    );
    runtime.insert(
        "rules_replay_causal_start_utc_ms".into(),
        config.rules.replay_causal_start_utc_ms.into(),
    );
    use sha2::{Digest, Sha256};
    let runtime_sha256 = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&runtime_semantics).unwrap())
    );
    let output =
        PathBuf::from(std::env::var("PROJECTION_SENSITIVITY_OUTPUT").expect("必须指定新报告路径"));
    assert!(!output.exists(), "保留已有诊断证据");
    let stream = output.with_extension("groups.jsonl.gz");
    let policy =
        std::env::var("PROJECTION_SENSITIVITY_POLICY").unwrap_or_else(|_| "exact_cpu".into());
    let replacements = std::env::var("PROJECTION_SENSITIVITY_REPLACEMENTS")
        .ok()
        .map(|path| read_artifact(std::path::Path::new(&path)));
    assert_eq!(
        policy == "exact_cpu",
        replacements.is_none(),
        "候选必须显式提供CUDA表"
    );
    let common = std::env::var("PROJECTION_SENSITIVITY_COMMON_PATHS")
        .ok()
        .map(|path| read_artifact(std::path::Path::new(&path)));
    let common_json = common.as_ref().map(|(value, _)| {
        let paths = if value["schema"] == "projection_common_paths_v1" {
            value
        } else {
            assert_eq!(
                value["status"], "complete",
                "共同路径必须来自完成的exact基准"
            );
            assert_eq!(value["policy"], "exact_cpu");
            &value["components"]["common_paths"]
        };
        serde_json::to_string(paths).unwrap()
    });
    assert!(
        policy == "exact_cpu" || common_json.is_some(),
        "候选必须绑定exact共同路径"
    );
    let _session =
        projection_diagnostic::begin(&policy, replacements.as_ref().map(|(value, _)| value));
    let python = Python::attach(|py| {
        py.import("model_train.projection_sensitivity")?
            .getattr("StreamingSensitivity")?
            .call1((stream.to_str().unwrap(), &policy, common_json, 742001u64))
            .map(|value| value.unbind())
    })
    .unwrap();
    let mut observer = ProjectionObserver {
        native: None,
        snapshot: rules.clone(),
        python,
        public_groups: 0,
        native_groups: 0,
    };
    let result = backtest::run_prepared_historical_with_observer_v1(
        &config.prepared,
        &config.run,
        rules,
        &mut NoOpHistory,
        &mut observer,
    )
    .await;
    let (historical, error) = match result {
        Ok(report) => (serde_json::to_value(report).unwrap(), None),
        Err(failure) => (
            serde_json::to_value(failure.report).unwrap(),
            Some(format!("{:?}", failure.cause)),
        ),
    };
    let finished = Python::attach(|py| {
        if error.is_some() {
            observer.python.call_method0(py, "close")?;
            Ok(None)
        } else {
            observer
                .python
                .call_method0(py, "finish")?
                .extract::<String>(py)
                .map(Some)
        }
    });
    let (components, error) = match finished {
        Ok(Some(value)) => (
            serde_json::from_str::<serde_json::Value>(&value).unwrap(),
            error,
        ),
        Ok(None) => (serde_json::Value::Null, error),
        Err(cause) => {
            Python::attach(|py| {
                let _ = observer.python.call_method0(py, "close");
            });
            (serde_json::Value::Null, Some(cause.to_string()))
        }
    };
    let captured = projection_diagnostic::snapshot();
    let coverage = components["gradient_target_reached"] == true;
    let status = if error.is_some() || !coverage {
        "failed"
    } else if captured["missing_replacements"] != 0 {
        "discovery_only"
    } else {
        "complete"
    };
    let report = serde_json::json!({
        "schema":"projection_sensitivity_stream_run_v1", "policy":policy,"status":status,
        "scope":"固定2026-09-15 04:15—04:20 UTC训练内部工程诊断；T−30连续数值暖态；前32个决定组局部state+Actor梯度；非正式训练或收益验收",
        "external_intent":null,"execution_history":"NoOp","operating_start_utc_ms":REAL_START,
        "operating_end_utc_ms":REAL_END,"native_root_started_at_utc_ms":REAL_WARMUP,
        "configuration":config_evidence,"prepared_material":prepared_evidence,"source_scope":scope,
        "runtime_semantics":runtime_semantics,"runtime_semantics_sha256":runtime_sha256,
        "replacement_table":replacements.as_ref().map(|(_,evidence)|evidence),
        "common_paths_source":common.as_ref().map(|(_,evidence)|evidence),
        "source_commit":std::env::var("PROJECTION_SENSITIVITY_SOURCE_COMMIT").ok(),
        "public_groups":observer.public_groups,"native_groups":observer.native_groups,
        "historical":historical,"capture":captured,"components":components,"error":error,
    });
    use std::io::Write;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .unwrap()
        .write_all(&serde_json::to_vec_pretty(&report).unwrap())
        .unwrap();
    assert!(error.is_none(), "真实公共诊断失败：{error:?}");
    assert!(
        coverage,
        "真实有限范围没有完成前32个决定组检查；保留实际报告"
    );
}
