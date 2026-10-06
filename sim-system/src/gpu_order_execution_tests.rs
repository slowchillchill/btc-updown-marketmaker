// 原外层请求登记与合法接单生成只读基线；设备自行生成撮合/计量/账户候选。
#[tokio::test(flavor = "current_thread")]
#[ignore = "AutoDL同提交合法接单→真实行情maker→撤单设备纵向验证"]
async fn gpu_legal_gtc_request_taker_maker_cancel_matches_original_execution() {
    let (_directory, mut actor) =
        active_market_actor(SimCallerRoleV1::ModelTrain, "run-gpu-legal-gtc").await;
    let initial = decoded_maker_initial_events();
    let condition = initial[0].market.condition_id.clone();
    let token = TokenId("1".to_owned());
    let (mut state, _) = reduce_market_event_slice_v2(
        actor.state.take().unwrap(), MarketEventPathV1::HiddenPlatform, 1_100, &initial,
        actor._rule_snapshot.snapshot(), actor._finality_policy.as_ref(),
    ).unwrap_or_else(|_| panic!("真实解码双腿初始化失败"));
    let request = BusinessRequestV2::PostOrders(PostOrdersRequestV2 {
        context: RequestContextV2 { global_request_id: "gpu-legal-gtc-post".into(), caller_correlation_id: None },
        items: vec![PostOrderBatchItemV2 { batch_item_ordinal: 0, item: post_item(
            "gpu-legal-gtc-item", &condition, &token, SimBuyOrderKindV1::GtcLimit {
                limit_price_pusd_per_share: "0.3".parse().unwrap(),
                size_shares: "5".parse().unwrap(), post_only: false,
            },
        ) }],
    });
    let (next, receipt) = register_request_candidate(
        &actor.opened, actor.scenario.initial_market_directory(), state, &request_cutoff(1_200),
        &request, 11, actor._latency_profile.as_ref(), actor._rule_snapshot.snapshot(),
    );
    state = next;
    let processing_at = receipt.platform_arrived_at_utc_ms;
    assert!(processing_at + 150 < initial[0].market.window.end_at_utc_ms);
    let (next, accepted) = advance_mutation_candidate_with_outputs(
        &actor.opened, actor.scenario.initial_market_directory(), state, processing_at,
        actor._rule_snapshot.snapshot(), actor._finality_policy.as_ref(),
    );
    state = next;
    let accepted = accepted.expect("到达时刻必须实际处理合法GTC");
    let MutationActionBusinessEffectV3::Post(posts) = &accepted.business else { panic!("应返回提交结果"); };
    let PostOrderItemOutcomeV2::Accepted(post) = &posts[0].outcome else { panic!("真实5份订单未接受: {:?}", posts[0].outcome); };
    assert_eq!(post.platform_order_id, "sim-1");
    let snapshot = |state: &BusinessReducerStateV2| state.session.as_ref().unwrap()
        .execution_state.gpu_order_checkpoint_for_tests("sim-1");
    let mut checkpoints = vec![serde_json::json!({"stage":"accepted", "state":snapshot(&state)})];
    // 逐条原最终性时点，不把接受或MATCHED当成经济应用。
    while let Some(at) = state.session.as_ref().unwrap().execution_state.next_scheduled_finality_at_utc_ms() {
        let (next, _) = advance_finality_to_candidate(state, at, actor._rule_snapshot.snapshot(), actor._finality_policy.as_ref());
        state = next;
        checkpoints.push(serde_json::json!({"stage":"taker_finality", "at":at, "state":snapshot(&state)}));
    }
    let maker_at = processing_at + 50;
    let mut events = vec![decoded_maker_level_event(BookSide::Ask, "0.2", "1.000001", "0", maker_at, maker_at+1, 10)];
    for cycle in 0..3_u64 {
        events.push(decoded_maker_level_event(BookSide::Bid, "0.3", "0", "0.000001", maker_at, maker_at+2+cycle as i64*2, 11+cycle*2));
        events.push(decoded_maker_level_event(BookSide::Bid, "0.3", "0.000001", "0", maker_at, maker_at+3+cycle as i64*2, 12+cycle*2));
    }
    for (ordinal, event) in events.iter().enumerate() {
        let (next, _) = reduce_market_event_slice_v2(state, MarketEventPathV1::HiddenPlatform, maker_at,
            std::slice::from_ref(event), actor._rule_snapshot.snapshot(), actor._finality_policy.as_ref())
            .unwrap_or_else(|_| panic!("真实maker步骤失败"));
        state = next;
        checkpoints.push(serde_json::json!({"stage":"hidden_step", "ordinal":ordinal, "state":snapshot(&state)}));
    }
    assert_eq!(state.session.as_ref().unwrap().execution_state.fill_shape_for_tests().0, 2);
    while let Some(at) = state.session.as_ref().unwrap().execution_state.next_scheduled_finality_at_utc_ms() {
        let (next, _) = advance_finality_to_candidate(state, at, actor._rule_snapshot.snapshot(), actor._finality_policy.as_ref());
        state = next;
        checkpoints.push(serde_json::json!({"stage":"maker_finality", "at":at, "state":snapshot(&state)}));
    }
    let cancel = BusinessRequestV2::CancelOrders(CancelOrdersRequestV2 {
        context: RequestContextV2 { global_request_id: "gpu-legal-gtc-cancel".into(), caller_correlation_id: None },
        items: vec![CancelOrderBatchItemV2 { batch_item_ordinal: 0, item: CancelOrderItemV1 {
            item_id: "gpu-legal-gtc-cancel-item".into(), platform_order_id: "sim-1".into(),
        } }],
    });
    let (next, cancel_receipt) = register_request_candidate(&actor.opened, actor.scenario.initial_market_directory(),
        state, &request_cutoff(maker_at+30), &cancel, 12, actor._latency_profile.as_ref(), actor._rule_snapshot.snapshot());
    state = next;
    let (state, canceled) = advance_mutation_candidate_with_outputs(&actor.opened, actor.scenario.initial_market_directory(),
        state, cancel_receipt.platform_arrived_at_utc_ms, actor._rule_snapshot.snapshot(), actor._finality_policy.as_ref());
    assert!(matches!(canceled.unwrap().business, MutationActionBusinessEffectV3::Cancel(_)));
    checkpoints.push(serde_json::json!({"stage":"canceled", "state":snapshot(&state)}));
    let rules = &actor._rule_snapshot.snapshot().synchronized_platform_rules;
    let input = serde_json::json!({"checkpoints":checkpoints, "initial":initial, "events":events,
        "processing_at":processing_at, "maker_at":maker_at,
        "finality_path":actor._finality_policy.normal_path(),
        "fee_rate":rules.fee_schedule.rate.value().to_string(), "fee_exponent":rules.fee_schedule.exponent,
        "post":post, "receipt":receipt, "cancel_receipt":cancel_receipt});
    let mut child = std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(std::env::var("B_GPU_ORDER_VALIDATOR").unwrap()).stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(), &input).unwrap();
    let result = child.wait_with_output().unwrap();
    assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
    eprintln!("gpu_order_execution={}", String::from_utf8(result.stdout).unwrap());
}
