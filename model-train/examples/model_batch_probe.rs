//! 6次明确计数的真实GRU：两个既有逻辑键的新合成初始化，不续跑EOF经济状态。
use model_train::worker::{ModelWorker, WorkerConfig};
use serde_json::{Value, json};
use std::{path::Path, time::Instant};
fn request(account: usize, call: u64) -> Value {
    let value = if account == 0 && call == 2 { 2.0 } else { 1.0 };
    json!({"phase":"begin","account":format!("synthetic-0-{account}"),"call":call,"seed":23001+account,"global":[[1,0,value,0]],"markets":[[[2,0,1.0,0]]],"options":[{"kind":"NoTrade","market":null,"side":null}],"diagnostic_hidden":true})
}
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 3 {
        return Err("用法: model_batch_probe <worker配置.json> <新输出.json>".into());
    }
    let config: WorkerConfig = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let started = Instant::now();
    let mut batched = Vec::new();
    let batch_metrics;
    let before_other;
    let after_other;
    {
        let worker = ModelWorker::start(config.clone())?;
        batched.extend(worker.request_batch(vec![request(0, 1), request(1, 1)])?);
        before_other =
            worker.request(json!({"phase":"account_snapshot","account":"synthetic-0-1"}))?;
        batched.push(worker.request(request(0, 2))?);
        after_other =
            worker.request(json!({"phase":"account_snapshot","account":"synthetic-0-1"}))?;
        batch_metrics = worker.request(json!({"phase":"metrics"}))?;
    }
    let mut independent = Vec::new();
    let independent_metrics;
    {
        let worker = ModelWorker::start(config)?;
        for (account, call) in [(0, 1), (1, 1), (0, 2)] {
            independent.push(worker.request(request(account, call))?);
        }
        independent_metrics = worker.request(json!({"phase":"metrics"}))?;
    }
    let mut maximum_error = 0f64;
    for (b, s) in batched.iter().zip(&independent) {
        if let (Some(b), Some(s)) = (
            b["hidden_after_f32"].as_array(),
            s["hidden_after_f32"].as_array(),
        ) {
            for (b, s) in b.iter().zip(s) {
                maximum_error =
                    maximum_error.max((b.as_f64().unwrap() - s.as_f64().unwrap()).abs());
            }
        }
    }
    let isolated = before_other == after_other;
    let result = json!({"purpose":"两个既有逻辑键的新合成初始化；只读快照不推进模型或经济状态", "forwards":6,
        "maximum_full_hidden_error":maximum_error,"original_1e_minus5_numeric_check_passed":maximum_error<1e-5,
        "account1_unchanged_while_account0_advances":isolated,"account1_before":before_other,"account1_after":after_other,
        "batch":batch_metrics,"independent":independent_metrics,"batched":batched,"sequential":independent,
        "elapsed_seconds":started.elapsed().as_secs_f64()});
    // 在任何断言前保存完整原向量/身份/指标；数值差不自动等于身份串线。
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(Path::new(&args[2]))?;
    serde_json::to_writer_pretty(file, &result)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    assert!(isolated, "推进账户0改变了账户1的完整hidden/calls/RNG");
    assert_eq!(batch_metrics["forwards"], 3);
    assert_eq!(independent_metrics["forwards"], 3);
    assert_eq!(batch_metrics["maximum_forward_batch"], 2);
    for response in batched.iter().take(2) {
        assert_eq!(response["forward"]["hidden_before_norm"], 0.0);
    }
    Ok(())
}
