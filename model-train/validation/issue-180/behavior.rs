// 注入原 continuous_tests 的短真实组合场景；两个版本使用同一录制和模型。
#[tokio::test(flavor = "current_thread")]
#[ignore = "步骤8精确双版本三端对照，需显式图与证据目录"]
async fn issue180_behavior_preservation() {
    let directory = std::env::var("MODEL_GRAPH_DIR").unwrap();
    let mut runtime = crate::continuous_runtime::ThreeRuntime::new(&directory, 1);
    let parent = std::env::var("BEHAVIOR_OUTPUT").unwrap();
    let temp = tempfile::tempdir_in(&parent).unwrap();
    let recording = temp.path().join("dpt");
    let (core, times) = recorded_continuous(&recording);
    let mut root = ContinuousRoot::new(
        &temp.path().join("root"), &recording, &core, times,
        Scenario::Trade, 0, "issue180-behavior",
    );
    let mut ordinal = 0;
    let mut retired = 0;
    let mut initialized = 0;
    let mut same_ms = 0;
    let mut last = None;
    let mut submissions = 0;
    loop {
        next_recorded_group(&mut root.driver);
        let now = root.driver.decision().unwrap().cutoff().decision_time().logical_time_utc_ms();
        same_ms += usize::from(last == Some(now));
        last = Some(now);
        let group = issue180_consume(&mut root);
        if let Some(group) = group {
            initialized += group.pre_frame_hidden_ops.iter().filter(|op| matches!(op, HiddenOp::InitializeZero { .. })).count();
            retired += group.post_frame_hidden_ops.len();
            crate::continuous_runtime::issue180_trace(serde_json::json!({
                "kind":"input", "ordinal":ordinal, "group":group,
                "ledger":root.shared.ledger().projection_v2().unwrap(),
                "registry":root.source_sample(),
                "history_lengths":root.native.issue180_history_lengths(),
            }));
            let group = std::sync::Arc::new(group);
            for (_, actor) in runtime.advance(&[group.clone()], true) {
                let intent = actor.intent().unwrap();
                crate::continuous_runtime::issue180_trace(serde_json::json!({
                    "kind":"selection", "ordinal":ordinal,"path":actor.path,"intent":format!("{intent:?}"),
                }));
                submissions += usize::from(intent.is_some());
                issue180_execute(&actor, &mut root, ordinal).await;
            }
            // 模型若未提交，补一次支持集内原值动作；不称为自主模型决定。
            if now == FIRST + 1000 && submissions == 0 {
                let row = group.rows.iter().position(|r| r.role == InputRole::DecisionRequired).unwrap();
                let mut actor = NativeActor::new(group, row).unwrap();
                let support = actor.support().unwrap();
                let family = [1, 2].into_iter().find(|index| support[*index]).expect("真实支持的提交族");
                actor.choose(family).unwrap();
                while !actor.is_complete() {
                    let choice = actor.support().unwrap().iter().position(|legal| *legal).unwrap();
                    actor.choose(choice).unwrap();
                }
                crate::continuous_runtime::issue180_trace(serde_json::json!({"kind":"controlled-selection","path":actor.path,"intent":format!("{:?}",actor.intent().unwrap())}));
                issue180_execute(&actor, &mut root, ordinal).await;
                submissions += 1;
            }
        }
        ordinal += 1;
        if now == FIRST + 301000 { break; }
    }
    let final_sources = root.source_sample();
    root.driver.stop_and_drain().unwrap();
    std::fs::copy(temp.path().join("root/sim.jsonl"), std::path::Path::new(&parent).join("sim.jsonl")).unwrap();
    assert!(same_ms > 0 && initialized >= 2 && retired >= 1 && submissions > 0);
    crate::continuous_runtime::issue180_trace(serde_json::json!({
        "kind":"completion","groups":ordinal,"frames":runtime.frames,
        "same_ms":same_ms,"initializations":initialized,"retirements":retired,"submissions":submissions,
        "ledger":root.shared.ledger().projection_v2().unwrap(),"registry":final_sources,
    }));
    runtime.release_observation();
}
