//! 专用单测试进程：真实公共组、共同外生BUY历史和末端投影回填。
use super::*;
use crate::exact::projection_diagnostic;
use pyo3::prelude::*;

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
        py.import("model_train.projection_sensitivity")?
            .getattr("run")?
            .call1((native, paths, 742001u64))?
            .extract::<String>()
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
