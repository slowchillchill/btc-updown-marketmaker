use std::sync::Arc;
// 仅诊断消费者：原 DPT -> Shared/SIM -> Native -> 三端连续 Runtime -> 原执行。
fn issue180_accept_before(root: &mut ContinuousRoot, at: i64) {
    while root
        .driver
        .next_group_at()
        .unwrap()
        .is_some_and(|next| next <= at)
    {
        next_recorded_group(&mut root.driver);
        root.shared.apply_closed().unwrap();
        let frames = root.driver.frames();
        root.shared.observe_group(&frames);
        root.native.observe_sources(&frames);
    }
}
fn issue180_observe(root: &mut ContinuousRoot, at: i64) -> Arc<NativeInputGroup> {
    root.driver.observe_at(at).unwrap().unwrap();
    root.shared.apply_closed().unwrap();
    let frames = root.driver.frames_at_observation();
    let before = root
        .shared
        .freeze_action_before(root.driver.observation_cutoff().unwrap())
        .unwrap();
    let group = Arc::new(root.native.encode(&frames, before).unwrap());
    crate::continuous_runtime::issue180_trace(
        serde_json::json!({"kind":"observation","at":at,"group":group,"history_lengths":root.native.issue180_history_lengths()}),
    );
    group
}
fn issue180_controlled_pair(group: Arc<NativeInputGroup>) -> NativeActor {
    let row = group
        .rows
        .iter()
        .position(|row| row.role == InputRole::DecisionRequired)
        .unwrap();
    let mut actor = NativeActor::new(group, row).unwrap();
    assert!(actor.support().unwrap()[2]);
    actor.choose(2).unwrap();
    let quantity = num_bigint::BigUint::from(500u32);
    while actor.state.stage == crate::prefix::Stage::QuantityBranch {
        let interval = actor.state.interval.as_ref().unwrap();
        let selected = (0..16)
            .find(|index| {
                quantity_segment(interval, *index)
                    .is_some_and(|(lo, hi)| lo <= quantity && quantity <= hi)
            })
            .unwrap();
        actor.choose(selected).unwrap();
    }
    while !actor.is_complete() {
        let choice = actor
            .support()
            .unwrap()
            .iter()
            .position(|legal| *legal)
            .unwrap();
        actor.choose(choice).unwrap();
    }
    actor
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "步骤10真实三端有限时钟消费者，需显式图与产物目录"]
async fn issue180_time_host() {
    let directory = std::env::var("MODEL_GRAPH_DIR").unwrap();
    let parent = std::env::var("BEHAVIOR_OUTPUT").unwrap();
    let temp = tempfile::tempdir_in(&parent).unwrap();
    let recording = temp.path().join("dpt");
    let (core, times) = recorded_continuous(&recording);
    let mut root = ContinuousRoot::new(
        &temp.path().join("root"),
        &recording,
        &core,
        times,
        Scenario::Trade,
        0,
        "issue180-time",
    );
    let (jobs, receive_jobs) = std::sync::mpsc::channel::<(usize, Arc<NativeInputGroup>)>();
    let (ready, receive_ready) = std::sync::mpsc::channel();
    let (started, receive_started) = std::sync::mpsc::channel();
    let (completed, receive_completed) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut runtime = crate::continuous_runtime::ThreeRuntime::new(&directory, 1);
        ready.send(()).unwrap();
        for (ordinal, group) in receive_jobs {
            started.send(ordinal).unwrap();
            let began = std::time::Instant::now();
            let actors = runtime
                .advance(&[group], true)
                .into_iter()
                .map(|(_, actor)| actor)
                .collect::<Vec<_>>();
            completed
                .send((ordinal, actors, began.elapsed().as_nanos()))
                .unwrap();
        }
        let frames = runtime.frames;
        runtime.release_observation();
        frames
    });
    receive_ready.recv().unwrap();
    issue180_accept_before(&mut root, FIRST - 30000);
    let warm = issue180_observe(&mut root, FIRST - 30000);
    assert!(
        warm.rows
            .iter()
            .all(|row| row.role == InputRole::ContextOnly)
    );
    jobs.send((0, warm)).unwrap();
    assert_eq!(receive_started.recv().unwrap(), 0);
    let (ordinal, actors, _) = receive_completed.recv().unwrap();
    assert_eq!(ordinal, 0);
    assert!(actors.is_empty());
    issue180_accept_before(&mut root, FIRST);
    let original = issue180_observe(&mut root, FIRST);
    let frozen = serde_json::to_value(&original).unwrap();
    jobs.send((1, Arc::clone(&original))).unwrap();
    assert_eq!(receive_started.recv().unwrap(), 1);
    issue180_accept_before(&mut root, FIRST + 20);
    assert_eq!(
        root.driver
            .observation_cutoff()
            .unwrap()
            .receipt()
            .execution_time()
            .logical_time_utc_ms(),
        FIRST + 20
    );
    assert!(
        matches!(
            receive_completed.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ),
        "20ms事实必须在模型完成前已应用"
    );
    // 主线程实际应用源事实，模型线程只持不可变组；不持 driver 或 Shared。
    assert_eq!(serde_json::to_value(&original).unwrap(), frozen);
    crate::continuous_runtime::issue180_trace(
        serde_json::json!({"kind":"facts-during-compute","at":FIRST+20,"cutoff":root.driver.observation_cutoff(),"frozen_unchanged":true,"model_inflight":true}),
    );
    let (ordinal, actors, wall) = receive_completed.recv().unwrap();
    assert_eq!(ordinal, 1);
    root.driver.advance_before(FIRST + 50).unwrap();
    for actor in actors {
        let intent = format!("{:?}", actor.intent().unwrap());
        let receipt = actor
            .execute(
                &mut root.shared,
                &mut root
                    .driver
                    .send_decision_at(original.cutoff.clone(), FIRST + 50)
                    .unwrap(),
                RequestContextV2 {
                    global_request_id: "time-model-0".into(),
                    caller_correlation_id: None,
                },
            )
            .await
            .unwrap();
        crate::continuous_runtime::issue180_trace(
            serde_json::json!({"kind":"completion-send","observation":FIRST,"completion_at":FIRST+50,"send":receipt.as_ref().map(|_|FIRST+50),"compute_start":FIRST,"queue_wait_ms":0,"economic_delay_ms":50,"compute_wall_ns":wall,"intent":intent,"receipt":format!("{receipt:?}")}),
        );
    }
    let controlled = issue180_controlled_pair(Arc::clone(&original));
    let chosen = format!("{:?}", controlled.intent().unwrap());
    let receipt = controlled
        .execute(
            &mut root.shared,
            &mut root
                .driver
                .send_decision_at(original.cutoff.clone(), FIRST + 50)
                .unwrap(),
            RequestContextV2 {
                global_request_id: "time-controlled-buy".into(),
                caller_correlation_id: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
    crate::continuous_runtime::issue180_trace(
        serde_json::json!({"kind":"controlled-send","observation":FIRST,"send":FIRST+50,"intent":chosen,"arrival":receipt.platform_arrived_at_utc_ms(),"scheduled_actions":receipt.scheduled_action_count(),"registry":root.source_sample()}),
    );
    // 100ms周期的两次静默观察均接纳；250ms预声明完成延迟不能覆盖或跳过旧tick。
    let hundred = issue180_observe(&mut root, FIRST + 100);
    jobs.send((2, Arc::clone(&hundred))).unwrap();
    assert_eq!(receive_started.recv().unwrap(), 2);
    let two_hundred = issue180_observe(&mut root, FIRST + 200);
    assert_eq!(hundred.cutoff.receipt(), two_hundred.cutoff.receipt());
    let stale_buy = issue180_controlled_pair(Arc::clone(&two_hundred));
    let stale_intent = format!("{:?}", stale_buy.intent().unwrap());
    let frozen_cash = stale_buy
        .group
        .action_before
        .as_ref()
        .unwrap()
        .economics()
        .cash
        .clone();
    let mut controlled_cancel = NativeActor::new(
        Arc::clone(&hundred),
        hundred
            .rows
            .iter()
            .position(|row| row.role == InputRole::DecisionRequired)
            .unwrap(),
    )
    .unwrap();
    assert!(
        controlled_cancel.support().unwrap()[3],
        "已有真实未闭合订单须能沿原支持集撤单"
    );
    controlled_cancel.choose(3).unwrap();
    while !controlled_cancel.is_complete() {
        let choice = controlled_cancel
            .support()
            .unwrap()
            .iter()
            .position(|legal| *legal)
            .unwrap();
        controlled_cancel.choose(choice).unwrap();
    }
    crate::continuous_runtime::issue180_trace(
        serde_json::json!({"kind":"queued-tick","observation":FIRST+200,"compute_start":FIRST+350,"planned_completion":FIRST+600,"queue_wait_ms":150}),
    );
    crate::continuous_runtime::issue180_trace(
        serde_json::json!({"kind":"observation-window-closed","at":FIRST+200,"accepted_ticks":[FIRST,FIRST+100,FIRST+200],"drain_until":FIRST+600}),
    );
    for (ordinal, at, group) in [(2, FIRST + 350, hundred), (3, FIRST + 600, two_hundred)] {
        if ordinal == 3 {
            jobs.send((3, Arc::clone(&group))).unwrap();
            assert_eq!(receive_started.recv().unwrap(), 3);
        }
        let (actual, actors, wall) = receive_completed.recv().unwrap();
        assert_eq!(actual, ordinal);
        root.driver.advance_before(at).unwrap();
        for actor in actors {
            let receipt = actor
                .execute(
                    &mut root.shared,
                    &mut root
                        .driver
                        .send_decision_at(
                            group.action_before.as_ref().unwrap().cutoff().clone(),
                            at,
                        )
                        .unwrap(),
                    RequestContextV2 {
                        global_request_id: format!("time-model-{ordinal}"),
                        caller_correlation_id: None,
                    },
                )
                .await
                .unwrap();
            crate::continuous_runtime::issue180_trace(
                serde_json::json!({"kind":"completion-send","ordinal":ordinal,"completion_at":at,"send":receipt.as_ref().map(|_|at),"observation":if ordinal==2 {FIRST+100} else {FIRST+200},"compute_start":if ordinal==2 {FIRST+100} else {FIRST+350},"queue_wait_ms":if ordinal==2 {0} else {150},"economic_delay_ms":250,"compute_wall_ns":wall,"receipt":format!("{receipt:?}")}),
            );
        }
        if ordinal == 2 {
            let intent = format!("{:?}", controlled_cancel.intent().unwrap());
            let receipt = controlled_cancel
                .execute(
                    &mut root.shared,
                    &mut root
                        .driver
                        .send_decision_at(group.cutoff.clone(), at)
                        .unwrap(),
                    RequestContextV2 {
                        global_request_id: "time-controlled-cancel".into(),
                        caller_correlation_id: None,
                    },
                )
                .await
                .unwrap()
                .unwrap();
            crate::continuous_runtime::issue180_trace(
                serde_json::json!({"kind":"controlled-cancel","observation":FIRST+100,"send":at,"intent":intent,"arrival":receipt.platform_arrived_at_utc_ms(),"scheduled_actions":receipt.scheduled_action_count()}),
            );
        }
    }
    // 已接纳冻结选择的补充反例：发送前现金事实改变，原整动作预留拒绝，不改意图。
    root.driver.observe_at(FIRST + 650).unwrap();
    let cash_cutoff = root.driver.observation_cutoff().unwrap();
    root.driver
        .send_decision_at(cash_cutoff, FIRST + 650)
        .unwrap()
        .apply_external_cash_fact_v1(contracts::economics::ExternalCashFactV1 {
            identity: "time-cash-debit".into(),
            signed_delta_pusd: contracts::ExactDecimal("-1999.95".into()),
            attribution: contracts::economics::ExternalCashAttributionV1::UnknownActivity,
            correction_of: None,
        })
        .unwrap();
    root.driver.observe_at(FIRST + 651).unwrap();
    root.shared.apply_closed().unwrap();
    root.driver.observe_at(FIRST + 700).unwrap();
    root.shared.apply_closed().unwrap();
    let cash = root.shared.ledger().economic_read().unwrap().cash().clone();
    crate::continuous_runtime::issue180_trace(
        serde_json::json!({"kind":"before-send-cash","at":FIRST+700,"actual_available":cash.actual_available_pusd,"actual_reserved":cash.actual_reserved_pusd,"maker_available":cash.maker_available_pusd,"maker_reserved":cash.maker_reserved_pusd,"ledger_prefix":root.shared.ledger().projection_v2().unwrap().platform_source_prefix}),
    );
    let rejected = stale_buy
        .execute(
            &mut root.shared,
            &mut root
                .driver
                .send_decision_at(stale_buy.group.cutoff.clone(), FIRST + 700)
                .unwrap(),
            RequestContextV2 {
                global_request_id: "time-cash-rejected".into(),
                caller_correlation_id: None,
            },
        )
        .await;
    crate::continuous_runtime::issue180_trace(
        serde_json::json!({"kind":"cash-attempt-result","result":format!("{rejected:?}")}),
    );
    assert!(
        matches!(rejected, Err(crate::ActionError::Reservation(_))),
        "原整动作结果={rejected:?}"
    );
    assert_eq!(format!("{:?}", stale_buy.intent().unwrap()), stale_intent);
    assert_eq!(
        stale_buy
            .group
            .action_before
            .as_ref()
            .unwrap()
            .economics()
            .cash,
        frozen_cash
    );
    crate::continuous_runtime::issue180_trace(
        serde_json::json!({"kind":"cash-changed-reservation","observation":FIRST+200,"send_attempt":FIRST+700,"intent_unchanged":true,"result":format!("{rejected:?}")}),
    );
    // 原5分钟身份的合法来源跳转，非加速容量回放。
    for (ordinal, at) in [
        (4, FIRST + 270000),
        (5, FIRST + 300000),
        (6, FIRST + 301000),
    ] {
        issue180_accept_before(&mut root, at);
        let group = issue180_observe(&mut root, at);
        jobs.send((ordinal, Arc::clone(&group))).unwrap();
        assert_eq!(receive_started.recv().unwrap(), ordinal);
        let (actual, actors, wall) = receive_completed.recv().unwrap();
        assert_eq!(actual, ordinal);
        for actor in actors {
            let intent = format!("{:?}", actor.intent().unwrap());
            let receipt = actor
                .execute(
                    &mut root.shared,
                    &mut root
                        .driver
                        .send_decision_at(group.cutoff.clone(), at)
                        .unwrap(),
                    RequestContextV2 {
                        global_request_id: format!("time-identity-{ordinal}"),
                        caller_correlation_id: None,
                    },
                )
                .await
                .unwrap();
            crate::continuous_runtime::issue180_trace(
                serde_json::json!({"kind":"identity-completion","observation":at,"completion_at":at,"send":receipt.as_ref().map(|_|at),"compute_wall_ns":wall,"intent":intent,"receipt":format!("{receipt:?}")}),
            );
        }
    }
    drop(jobs);
    let frames = worker.join().unwrap();
    crate::continuous_runtime::issue180_trace(serde_json::json!({
        "kind":"accepted-work-stop-state",
        "requests":root.shared.registry().issue180_request_results(),
        "next_required":root.driver.next_required_logical_drive().map(|required|serde_json::json!({"path":format!("{:?}",required.path),"at":required.required_at_utc_ms})),
        "local_pending":root.shared.ledger().projection_v2().unwrap().local_pending_reservation_projection,
    }));
    root.driver.stop_and_drain().unwrap();
    std::fs::copy(
        temp.path().join("root/sim.jsonl"),
        std::path::Path::new(&parent).join("time-sim.jsonl"),
    )
    .unwrap();
    crate::continuous_runtime::issue180_trace(
        serde_json::json!({"kind":"time-host-completion","ticks":7,"frames":frames,"stopped_without_future":true}),
    );
}
