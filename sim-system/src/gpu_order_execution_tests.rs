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
    let PostOrderItemOutcomeV2::Accepted(post) = &posts.outcome else { panic!("真实5份订单未接受: {:?}", posts.outcome); };
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
    let mut confirmed_path = serde_json::to_value(actor._finality_policy.normal_path()).unwrap();
    confirmed_path["economic_application_ordinal"] = serde_json::json!(2);
    let confirmed_policy = TradeFinalityPolicyV2::decode_and_seal(&serde_json::to_vec(&serde_json::json!({
        "schema_version":"trade_finality_policy_v2", "normal_path":confirmed_path,"failure_overrides":[],
    })).unwrap()).expect("确认时应用经济的原密封路径");
    let input = serde_json::json!({"checkpoints":checkpoints, "initial":initial, "events":events,
        "processing_at":processing_at, "maker_at":maker_at,
        "finality_path":actor._finality_policy.normal_path(),
        "confirmed_economic_path":confirmed_policy.normal_path(),
        "fee_rate":rules.fee_schedule.rate.value().to_string(), "fee_exponent":rules.fee_schedule.exponent,
        "taker_rebate_rate":actor._rule_snapshot.snapshot().project_account_rules.rebate_settlement.value.fixed_taker_rebate_rate.value().to_string(),
        "post":post, "cancel_arrived_at":cancel_receipt.platform_arrived_at_utc_ms});
    let mut child = std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(std::env::var("B_GPU_ORDER_VALIDATOR").unwrap()).stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(), &input).unwrap();
    let result = child.wait_with_output().unwrap();
    assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
    eprintln!("gpu_order_execution={}", String::from_utf8(result.stdout).unwrap());
}

// 经实际会话策略密封及外层5份请求建立失败分支，不绕过接单约束。
#[tokio::test(flavor = "current_thread")]
#[ignore = "AutoDL原密封失败路径/首次经济seq/设备补偿验证"]
async fn gpu_sealed_failure_paths_preserve_original_economic_sequence_and_compensate_once() {
    let mut cases = Vec::new();
    for (index, economic_ordinal) in [None, Some(0_u32), Some(1_u32)].into_iter().enumerate() {
        let run_id = format!("run-gpu-failure-{index}");
        let original_spec = spec(&run_id, "gpu-failure-create");
        let account_key = contracts::AccountInstanceKeyV1 {
            run_id: original_spec.run_id.clone(), caller_instance_id: original_spec.caller_instance_id.clone(),
            account_id: original_spec.account_id.clone(),caller_role:SimCallerRoleV1::ModelTrain,
            platform_source:contracts::PlatformSourceV1::Simulated,
        };
        let path = serde_json::json!({"steps":[
            {"state":"MATCHED","offset_ms":0},
            {"state":"MINED","offset_ms":10},
            {"state":"FAILED","offset_ms":20}],"economic_application_ordinal":economic_ordinal});
        let policy = Arc::new(TradeFinalityPolicyV2::decode_and_seal(&serde_json::to_vec(&serde_json::json!({
            "schema_version":"trade_finality_policy_v2", "normal_path":successful_policy_v2_for_tests().normal_path(),
            "failure_overrides":[{"selector":{"selector_domain":"REQUEST","account_instance_key":account_key,
                "global_request_id":"gpu-failure-post"},"path":path}],
        })).unwrap()).expect("原失败策略密封"));
        let (_directory, mut actor) = active_market_actor_with_policy(SimCallerRoleV1::ModelTrain,&run_id,policy.clone()).await;
        let initial = decoded_maker_initial_events();
        let condition = initial[0].market.condition_id.clone();
        let token = TokenId("1".into());
        let (state, _) = reduce_market_event_slice_v2(actor.state.take().unwrap(),MarketEventPathV1::HiddenPlatform,
            1_100,&initial,actor._rule_snapshot.snapshot(),policy.as_ref()).unwrap_or_else(|_| panic!("原初始化"));
        assert_eq!(state.session.as_ref().unwrap().execution_state.account_instance_key(), &account_key);
        let request = BusinessRequestV2::PostOrders(PostOrdersRequestV2 {
            context:RequestContextV2 {global_request_id:"gpu-failure-post".into(),caller_correlation_id:None},
            items:vec![PostOrderBatchItemV2 {batch_item_ordinal:0,item:post_item("gpu-failure-item",&condition,&token,
                SimBuyOrderKindV1::GtcLimit {limit_price_pusd_per_share:"0.3".parse().unwrap(),
                    size_shares:"5".parse().unwrap(),post_only:false})}],
        });
        let (state,receipt) = register_request_candidate(&actor.opened,actor.scenario.initial_market_directory(),state,
            &request_cutoff(1_200),&request,11,actor._latency_profile.as_ref(),actor._rule_snapshot.snapshot());
        let (mut state,accepted) = advance_mutation_candidate_with_outputs(&actor.opened,
            actor.scenario.initial_market_directory(),state,receipt.platform_arrived_at_utc_ms,
            actor._rule_snapshot.snapshot(),policy.as_ref());
        let MutationActionBusinessEffectV3::Post(post) = accepted.unwrap().business else {panic!("原提交结果");};
        assert!(matches!(&post.outcome,PostOrderItemOutcomeV2::Accepted(_)));
        let checkpoint = |state:&BusinessReducerStateV2| state.session.as_ref().unwrap().execution_state.gpu_order_checkpoint_for_tests("sim-1");
        let mut checkpoints = vec![serde_json::json!({"at":receipt.platform_arrived_at_utc_ms,"state":checkpoint(&state)})];
        while let Some(at) = state.session.as_ref().unwrap().execution_state.next_scheduled_finality_at_utc_ms() {
            let (next,_) = advance_finality_to_candidate(state,at,actor._rule_snapshot.snapshot(),policy.as_ref());
            state=next;
            checkpoints.push(serde_json::json!({"at":at,"state":checkpoint(&state)}));
        }
        assert_eq!(checkpoints.len(),3);
        let rules = &actor._rule_snapshot.snapshot().synchronized_platform_rules;
        cases.push(serde_json::json!({"path":path,"initial":initial,"processing_at":receipt.platform_arrived_at_utc_ms,
            "checkpoints":checkpoints,"fee_rate":rules.fee_schedule.rate.value().to_string(),"fee_exponent":rules.fee_schedule.exponent,
            "taker_rebate_rate":actor._rule_snapshot.snapshot().project_account_rules.rebate_settlement.value.fixed_taker_rebate_rate.value().to_string()}));
    }
    let mut child = std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(std::env::var("B_GPU_FINALITY_VALIDATOR").unwrap()).stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(),&cases).unwrap();
    let output=child.wait_with_output().unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    eprintln!("gpu_failure_finality={}",String::from_utf8(output.stdout).unwrap());
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "AutoDL原合法多ask逐fill及同时间最终性顺序"]
async fn gpu_legal_multiask_preserves_each_fill_and_same_time_finality_order() {
    let (_directory,mut actor) = active_market_actor(SimCallerRoleV1::ModelTrain,"run-gpu-multiask").await;
    let mut initial = decoded_maker_initial_events();
    let ExecutionMarketEventPayloadV3::InitialL2Snapshot(book) = &mut initial[0].payload else {unreachable!()};
    book.asks.push(contracts::PriceLevelV1 {price_pusd_per_share:ExactDecimal("0.25".into()),quantity_shares:ExactDecimal("1".into())});
    initial[0].event_id = data_pipeline_core::derive_execution_market_event_v3_id(&initial[0]).unwrap();
    initial[0] = data_pipeline_core::decode_execution_market_event_v3(&data_pipeline_core::encode_execution_market_event_v3(&initial[0]).unwrap()).unwrap();
    let condition = initial[0].market.condition_id.clone();
    let token = TokenId("1".into());
    let (state,_) = reduce_market_event_slice_v2(actor.state.take().unwrap(),MarketEventPathV1::HiddenPlatform,1_100,
        &initial,actor._rule_snapshot.snapshot(),actor._finality_policy.as_ref()).unwrap_or_else(|_|panic!("原双腿初始化"));
    // 同价正增量必须追加另一个E段，不能与原B/Q合并。
    let addition = decoded_maker_level_event(BookSide::Ask,"0.2","1.000001","2.000001",1_150,1_151,10);
    let (state,_) = reduce_market_event_slice_v2(state,MarketEventPathV1::HiddenPlatform,1_150,
        std::slice::from_ref(&addition),actor._rule_snapshot.snapshot(),actor._finality_policy.as_ref())
        .unwrap_or_else(|_|panic!("原同价正增量"));
    let request = BusinessRequestV2::PostOrders(PostOrdersRequestV2 {
        context:RequestContextV2 {global_request_id:"gpu-multiask-post".into(),caller_correlation_id:None},
        items:vec![PostOrderBatchItemV2 {batch_item_ordinal:0,item:post_item("gpu-multiask-item",&condition,&token,
            SimBuyOrderKindV1::GtcLimit {limit_price_pusd_per_share:"0.3".parse().unwrap(),size_shares:"5".parse().unwrap(),post_only:false})}],
    });
    let (state,receipt) = register_request_candidate(&actor.opened,actor.scenario.initial_market_directory(),state,
        &request_cutoff(1_200),&request,11,actor._latency_profile.as_ref(),actor._rule_snapshot.snapshot());
    let (state,accepted) = advance_mutation_candidate_with_outputs(&actor.opened,actor.scenario.initial_market_directory(),state,
        receipt.platform_arrived_at_utc_ms,actor._rule_snapshot.snapshot(),actor._finality_policy.as_ref());
    let MutationActionBusinessEffectV3::Post(post) = accepted.unwrap().business else {panic!("原提交结果");};
    assert!(matches!(&post.outcome,PostOrderItemOutcomeV2::Accepted(_)));
    let mut execution = state.session.unwrap().execution_state;
    assert_eq!(execution.fill_shape_for_tests(),(3,4));
    let mut checkpoints = vec![serde_json::json!({"at":receipt.platform_arrived_at_utc_ms,
        "state":execution.gpu_order_checkpoint_for_tests("sim-1")})];
    while let Some(at) = execution.next_scheduled_finality_at_utc_ms() {
        let (next,effect) = execution.advance_next_scheduled_finality_at(at,actor._finality_policy.as_ref(),actor._rule_snapshot.snapshot()).unwrap();
        execution=next;
        let transition = effect.transition.expect("逐原义务消费");
        checkpoints.push(serde_json::json!({"at":at,"fill_sequence":transition.trade_finality.fill_sequence,
            "state":execution.gpu_order_checkpoint_for_tests("sim-1")}));
    }
    assert_eq!(checkpoints.len(),7);
    let rules = &actor._rule_snapshot.snapshot().synchronized_platform_rules;
    let input = serde_json::json!({"initial":initial,"addition":addition,"checkpoints":checkpoints,"processing_at":receipt.platform_arrived_at_utc_ms,
        "finality_path":actor._finality_policy.normal_path(),"fee_rate":rules.fee_schedule.rate.value().to_string(),
        "fee_exponent":rules.fee_schedule.exponent,"taker_rebate_rate":actor._rule_snapshot.snapshot().project_account_rules.rebate_settlement.value.fixed_taker_rebate_rate.value().to_string()});
    let mut child = std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(std::env::var("B_GPU_MULTIASK_VALIDATOR").unwrap()).stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(),&input).unwrap();
    let output=child.wait_with_output().unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    eprintln!("gpu_multiask={}",String::from_utf8(output.stdout).unwrap());
}
