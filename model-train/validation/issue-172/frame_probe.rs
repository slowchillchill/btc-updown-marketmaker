//! 空人口、跨块非空人口和多步合法动作的独立业务夹具；不混入真实市场延迟分布。
#[path = "frame_runtime_probe.rs"]
mod runtime;
use crate::{InputRole, action_tests::Harness};
use sim_system::execution::RequestContextV2;
use std::{io::Write, path::PathBuf, sync::Arc, time::Instant};

#[tokio::test(flavor = "current_thread")]
#[ignore = "#172 纯Rust ORT实际业务动作夹具；需原图、原预热输入和独立输出文件"]
async fn directed_frame_action_costs() {
    let model = PathBuf::from(std::env::var("MODEL_GRAPH_DIR").unwrap());
    let threads = std::env::var("MODEL_THREADS").unwrap().parse().unwrap();
    let mut output = std::io::BufWriter::new(
        std::fs::File::create(std::env::var("DIRECTED_OUTPUT").unwrap()).unwrap(),
    );
    // 每个模型/轮次/人口/家族均独立账户及执行器，原组来自consume，交易中不恢复快照。
    for round in 0..2 {
        let mut cases = vec![(0, 0), (0, 2), (1, 3), (86, 3), (86, 4), (86, 5)];
        if round == 1 {
            cases.reverse();
        }
        for (pairs, family) in cases {
            let preparation = Instant::now();
            let mut runtime = runtime::Runtime::new(&model, threads);
            let empty = runtime.empty_tensor_probe();
            let mut h = Harness::named(pairs, false, false, "issue-172-directed", "100");
            for _ in 0..4 {
                h.driver.next_group().unwrap().unwrap();
                let group = h
                    .native
                    .consume(&mut h.shared, &mut h.driver)
                    .unwrap()
                    .unwrap();
                runtime.advance(Arc::new(group), false);
            }
            h.pairs(pairs).await;
            let mut found = false;
            while h.driver.next_group().unwrap().is_some() {
                let Some(group) = h.native.consume(&mut h.shared, &mut h.driver).unwrap() else {
                    continue;
                };
                let at = group.cutoff.decision_time().logical_time_utc_ms();
                let group = Arc::new(group);
                let (_, state, states) = runtime.advance(group.clone(), false);
                if at != 28000 {
                    continue;
                }
                assert_eq!(group.cancel.len(), 3 * pairs + 1);
                found = true;
                let row = group
                    .rows
                    .iter()
                    .position(|r| r.role == InputRole::DecisionRequired)
                    .unwrap();
                let preparation_seconds = preparation.elapsed().as_secs_f64();
                // 同一冻结对象允许先检查支持集，再作一次真实执行；没有反馈前重算原模型状态。
                let before_frames = runtime.frames;
                let (actor, mut result) = runtime.directed(group, row, &states[row], family);
                assert_eq!(runtime.frames, before_frames);
                h.driver.wait_for_recording_capacity().unwrap();
                let began = Instant::now();
                let receipt = actor
                    .execute(
                        &mut h.shared,
                        &mut h.driver.decision().unwrap(),
                        RequestContextV2 {
                            global_request_id: "issue-172-directed-action".into(),
                            caller_correlation_id: None,
                        },
                    )
                    .await
                    .unwrap();
                result["execute_ns"] = serde_json::json!(began.elapsed().as_nanos());
                result["round"] = round.into();
                result["pairs"] = pairs.into();
                result["family"] = family.into();
                result["ort_threads"] = threads.into();
                result["at"] = at.into();
                result["preparation_seconds"] = preparation_seconds.into();
                result["preceding_state_timing"] = serde_json::to_value(state).unwrap();
                result["empty_tensor_only_samples_ns"] = serde_json::to_value(empty).unwrap();
                result["receipt"] = format!("{receipt:?}").into();
                result["parameter_gru_calls"] = 0.into();
                serde_json::to_writer(&mut output, &result).unwrap();
                writeln!(output).unwrap();
                output.flush().unwrap();
                break;
            }
            assert!(found, "真实业务夹具必须到达指定组");
            h.driver.stop_and_drain().unwrap();
        }
    }
}
