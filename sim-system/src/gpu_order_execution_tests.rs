// 原外层请求登记与合法接单生成只读基线；设备自行生成撮合/计量/账户候选。
#[tokio::test(flavor = "current_thread")]
#[ignore = "AutoDL原请求登记、完整u64 pending顺序及首次回执设备对照"]
async fn gpu_mutation_registration_preserves_original_pending_and_first_receipt() {
    let (_directory, mut actor) = active_market_actor(SimCallerRoleV1::ModelTrain, "run-gpu-mutation-register").await;
    let initial = decoded_initial_market_events("10");
    let condition = initial[0].market.condition_id.clone();
    let token = TokenId("1".into());
    let (state, _) = reduce_market_event_slice_v2(actor.state.take().unwrap(),
        MarketEventPathV1::HiddenPlatform, 1_100, &initial, actor._rule_snapshot.snapshot(),
        actor._finality_policy.as_ref()).unwrap_or_else(|_| panic!("原请求初始化失败"));
    let seed = state.session.as_ref().unwrap().session_seed.0.as_bytes().to_vec();
    let request_id = "gpu-mutation-pair";
    let request = BusinessRequestV2::PostOrders(PostOrdersRequestV2 {
        context: RequestContextV2 { global_request_id: request_id.into(), caller_correlation_id: None },
        items: vec![
            PostOrderBatchItemV2 { batch_item_ordinal: 7, item: post_item("gtc", &condition, &token,
                SimBuyOrderKindV1::GtcLimit { limit_price_pusd_per_share: "0.3".parse().unwrap(),
                    size_shares: "5".parse().unwrap(), post_only: false }) },
            PostOrderBatchItemV2 { batch_item_ordinal: 1, item: post_item("fok", &condition, &token,
                SimBuyOrderKindV1::FokMarket { spend_pusd: "3.6".parse().unwrap(),
                    worst_price_pusd_per_share: "0.6".parse().unwrap() }) },
        ],
    });
    let (mut state, receipt) = register_request_candidate(&actor.opened,
        actor.scenario.initial_market_directory(), state, &request_cutoff(1_200), &request, 11,
        actor._latency_profile.as_ref(), actor._rule_snapshot.snapshot());
    let keys = state.session.as_ref().unwrap().mutation_requests.pending_actions.keys()
        .map(|key| serde_json::json!({"effective_at":key.effective_processing_at_utc_ms,
            "input_sequence":key.input_sequence,"ordinal":key.batch_item_ordinal})).collect::<Vec<_>>();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0]["ordinal"], 7);
    let mut response_times = None;
    for key in &keys {
        let (next, effects) = advance_mutation_candidate_with_outputs(&actor.opened,
            actor.scenario.initial_market_directory(), state, key["effective_at"].as_i64().unwrap(),
            actor._rule_snapshot.snapshot(), actor._finality_policy.as_ref());
        state = next;
        if let Some(response) = effects.unwrap().completed_response {
            let StoredDirectResponseV3::Draft(response) = response.as_ref() else { panic!("原候选响应尚未编号"); };
            response_times = Some(serde_json::json!({"responded_at":response.platform_responded_at_utc_ms,
                "received_at":response.response_received_at_utc_ms}));
        }
    }
    let altered = BusinessRequestV2::PostOrders(PostOrdersRequestV2 {
        context: RequestContextV2 { global_request_id: request_id.into(), caller_correlation_id: Some("different".into()) },
        items: vec![],
    });
    assert_eq!(existing_mutation_request_receipt_v2(&state, &altered.context().global_request_id), Some(receipt));
    let rules = actor._rule_snapshot.snapshot();
    let source = serde_json::json!({"profile_sha256":actor._latency_profile.file_sha256(),
        "seed":seed,"identity":request_id,"sent_at":1200,"input_sequence":11,
        "taker_delay_ms":if rules.synchronized_platform_rules.taker_order_delay_enabled {
            rules.documented_platform_rules.taker_delay.value.delay_ms } else { 0 },
        "receipt":{"input_sequence":receipt.input_sequence(),
            "arrived_at":receipt.platform_arrived_at_utc_ms(),"scheduled_count":receipt.scheduled_action_count()},
        "pending":keys,"response":response_times.unwrap(),"original_changed_body_retry_keeps_receipt":true});
    let mut child = std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(std::env::var("B_GPU_MUTATION_VALIDATOR").unwrap())
        .stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(), &source).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    println!("gpu_mutation_registration={}", String::from_utf8(output.stdout).unwrap());
}

#[cfg(feature = "shared-execution")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "AutoDL同提交合法接单→真实行情maker→撤单设备纵向验证"]
async fn gpu_legal_gtc_request_taker_maker_cancel_matches_original_execution() {
    let saved = Rc::new(RefCell::new(None));
    let capture = Rc::clone(&saved);
    let normalization = Rc::new(RefCell::new(Vec::new()));
    let normalization_capture = Rc::clone(&normalization);
    let (_directory, mut actor) = active_market_actor_with_projection(
        SimCallerRoleV1::ModelTrain, "run-gpu-legal-gtc", successful_policy_v2_for_tests(),
        move |path, opened| {
            opened.projection_version = crate::runtime_v2::BusinessProjectionVersionV1::V4;
            let reference = contracts::execution_normalization::DurableSourceReferenceV1 {
                source: contracts::execution_normalization::ImmutableSourceReferenceV1::Location(path.display().to_string()),
            };
            let memory = noe_projection::MemoryNoeProjectionPortV4::new(path.parent().unwrap().join("noe-memory"),
                opened.clone(), reference.clone(), reference.clone(), "create-run-gpu-legal-gtc".into(), "gpu-cancel-source".into());
            *capture.borrow_mut() = Some(memory.observation());
            let durable = noe_projection::SimV2NoeProjectionPortV4::new(path.parent().unwrap().join("noe-durable"),
                opened.clone(), reference.clone(), reference, "create-run-gpu-legal-gtc".into(), "gpu-cancel-source".into());
            Box::new(GpuNoeReferencePort { memory, durable, normalization_reference: normalization_capture, normalization_core: None })
        },
    ).await;
    let observation = saved.borrow_mut().take().unwrap();
    let mut ledger = ledger::LedgerWriterV1::new_with_admission_v2(ledger::AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    ledger.apply_noe_memory_v2(&observation.opening_record().unwrap()).unwrap();
    for record in observation.take_closed_records() { ledger.apply_noe_memory_v2(&record).unwrap(); }
    let opening_ledger = gpu_ledger_reservation_checkpoint(&ledger, "opening");
    let mut prior_batches = Vec::new();
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
    let post = post.clone();
    state = gpu_accept_projection_reference(&mut actor, state,
        BusinessReducerOutputV2::MutationAction(Some(accepted)), &mut prior_batches);
    let snapshot = |state: &BusinessReducerStateV2| state.session.as_ref().unwrap()
        .execution_state.gpu_order_checkpoint_for_tests("sim-1");
    let mut checkpoints = vec![serde_json::json!({"stage":"accepted", "state":snapshot(&state)})];
    // 逐条原最终性时点，不把接受或MATCHED当成经济应用。
    while let Some(at) = state.session.as_ref().unwrap().execution_state.next_scheduled_finality_at_utc_ms() {
        let (next, effect) = advance_finality_to_candidate(state, at, actor._rule_snapshot.snapshot(), actor._finality_policy.as_ref());
        state = gpu_accept_projection_reference(&mut actor, next, BusinessReducerOutputV2::LogicalDrive(
            LogicalDriveOutputV2 { lifecycle: effect.into_lifecycle_feedback().unwrap(), ..Default::default() }), &mut prior_batches);
        checkpoints.push(serde_json::json!({"stage":"taker_finality", "at":at, "state":snapshot(&state)}));
    }
    let maker_at = processing_at + 50;
    let mut events = vec![decoded_maker_level_event(BookSide::Ask, "0.2", "1.000001", "0", maker_at, maker_at+1, 10)];
    for cycle in 0..3_u64 {
        events.push(decoded_maker_level_event(BookSide::Bid, "0.3", "0", "0.000001", maker_at, maker_at+2+cycle as i64*2, 11+cycle*2));
        events.push(decoded_maker_level_event(BookSide::Bid, "0.3", "0.000001", "0", maker_at, maker_at+3+cycle as i64*2, 12+cycle*2));
    }
    for (ordinal, event) in events.iter().enumerate() {
        let (next, effect) = reduce_market_event_slice_v2(state, MarketEventPathV1::HiddenPlatform, maker_at,
            std::slice::from_ref(event), actor._rule_snapshot.snapshot(), actor._finality_policy.as_ref())
            .unwrap_or_else(|_| panic!("真实maker步骤失败"));
        state = next;
        let lifecycle = effect.execution.into_iter().flat_map(|value| value.into_lifecycle_feedback().unwrap()).collect::<Vec<_>>();
        if !lifecycle.is_empty() {
            state = gpu_accept_projection_reference(&mut actor, state, BusinessReducerOutputV2::LogicalDrive(
                LogicalDriveOutputV2 { lifecycle, ..Default::default() }), &mut prior_batches);
        }
        checkpoints.push(serde_json::json!({"stage":"hidden_step", "ordinal":ordinal, "state":snapshot(&state)}));
    }
    assert_eq!(state.session.as_ref().unwrap().execution_state.fill_shape_for_tests().0, 2);
    while let Some(at) = state.session.as_ref().unwrap().execution_state.next_scheduled_finality_at_utc_ms() {
        let (next, effect) = advance_finality_to_candidate(state, at, actor._rule_snapshot.snapshot(), actor._finality_policy.as_ref());
        state = gpu_accept_projection_reference(&mut actor, next, BusinessReducerOutputV2::LogicalDrive(
            LogicalDriveOutputV2 { lifecycle: effect.into_lifecycle_feedback().unwrap(), ..Default::default() }), &mut prior_batches);
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
    // 取消前公开账本来自真实V4唯一领取；不以SIM内部余额补写Ledger前缀。
    let prefix_at = prior_batches.iter().flat_map(|batch| {
        let order = batch["orders"].as_array().unwrap().iter().map(|value| value["metadata"]["feedback_received_at_utc_ms"].as_i64().unwrap());
        let trade = batch["trades"].as_array().unwrap().iter().map(|value| value["metadata"]["feedback_received_at_utc_ms"].as_i64().unwrap());
        let settlement = batch["settlements"].as_array().unwrap().iter().map(|value| value["metadata"]["feedback_received_at_utc_ms"].as_i64().unwrap());
        let direct = batch["direct"].as_array().unwrap().iter().map(|value| value["metadata"]["response_received_at_utc_ms"].as_i64().unwrap());
        order.chain(trade).chain(settlement).chain(direct)
    }).max().unwrap();
    actor.projection_port.close_receive_prefix(prefix_at);
    actor.projection_port.observe_public_time(&crate::public_consumption::TrustedActorExecutionTimeV1(
        crate::public_consumption::ActorTimeRecordV1 { run_id: actor.opened.run_id.clone(), drive_position: 1, logical_time_utc_ms: prefix_at }));
    let mut previous = observation.take_closed_records();
    let prior_records = previous.iter().map(|record| serde_json::json!({
        "record":record.record(), "position":record.position(), "visibility":record.visibility(),
    })).collect::<Vec<_>>();
    while !previous.is_empty() {
        let count = previous.len();
        let mut waiting = Vec::new();
        for record in previous {
            match ledger.apply_noe_memory_v2(&record).unwrap() {
                ledger::LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation |
                ledger::LedgerMemoryApplyOutcomeV1::Applied(ledger::LedgerApplyOutcomeV1::MissingPlatformPredecessor {..}) => waiting.push(record),
                _ => (),
            }
        }
        assert!(waiting.len() < count, "真实取消前账本可以闭合");
        previous = waiting;
    }
    let cancel_ledger_before = gpu_ledger_reservation_checkpoint(&ledger, "cancel_before");
    let next_private = state.session.as_ref().unwrap().next_private_feedback_sequence;
    let next_batch = state.session.as_ref().unwrap().next_projection_batch_sequence;
    let (state, canceled) = advance_mutation_candidate_with_outputs(&actor.opened, actor.scenario.initial_market_directory(),
        state, cancel_receipt.platform_arrived_at_utc_ms, actor._rule_snapshot.snapshot(), actor._finality_policy.as_ref());
    let canceled = canceled.expect("真实取消请求处理");
    assert!(matches!(canceled.business, MutationActionBusinessEffectV3::Cancel(_)));
    let mut cancel_batches = Vec::new();
    let state = gpu_accept_projection_reference(&mut actor, state, BusinessReducerOutputV2::MutationAction(Some(canceled)), &mut cancel_batches);
    let closed_at = cancel_receipt.platform_arrived_at_utc_ms + 10_000;
    actor.projection_port.close_receive_prefix(closed_at);
    actor.projection_port.observe_public_time(&crate::public_consumption::TrustedActorExecutionTimeV1(
        crate::public_consumption::ActorTimeRecordV1 { run_id: actor.opened.run_id.clone(), drive_position: 1, logical_time_utc_ms: closed_at }));
    let received = observation.take_closed_records();
    let cancel_records = received.iter().map(|record| serde_json::json!({
        "record":record.record(), "position":record.position(), "visibility":record.visibility(),
    })).collect::<Vec<_>>();
    let mut cancel_ledger_attempts = Vec::new();
    for record in &received {
        let before = gpu_ledger_reservation_checkpoint(&ledger, "before");
        let result = ledger.apply_noe_memory_v2(record).unwrap();
        assert!(!matches!(result, ledger::LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation |
            ledger::LedgerMemoryApplyOutcomeV1::Applied(ledger::LedgerApplyOutcomeV1::MissingPlatformPredecessor {..})));
        cancel_ledger_attempts.push(serde_json::json!({"position":record.position(), "before":before,
            "after":gpu_ledger_reservation_checkpoint(&ledger, "after")}));
    }
    let cancel_ledger_after = gpu_ledger_reservation_checkpoint(&ledger, "cancel_after");
    for record in &received {
        assert!(matches!(ledger.apply_noe_memory_v2(record).unwrap(), ledger::LedgerMemoryApplyOutcomeV1::Applied(ledger::LedgerApplyOutcomeV1::AlreadyApplied {..})));
    }
    assert_eq!(gpu_ledger_reservation_checkpoint(&ledger, "cancel_after"), cancel_ledger_after);
    assert_eq!(cancel_records.len(), 4, "CallerCanceled、真实释放、Target与Completed");
    assert!(observation.take_closed_records().is_empty());
    checkpoints.push(serde_json::json!({"stage":"canceled", "state":snapshot(&state)}));
    let rules = &actor._rule_snapshot.snapshot().synchronized_platform_rules;
    let mut confirmed_path = serde_json::to_value(actor._finality_policy.normal_path()).unwrap();
    confirmed_path["economic_application_ordinal"] = serde_json::json!(2);
    let confirmed_policy = TradeFinalityPolicyV2::decode_and_seal(&serde_json::to_vec(&serde_json::json!({
        "schema_version":"trade_finality_policy_v2", "normal_path":confirmed_path,"failure_overrides":[],
    })).unwrap()).expect("确认时应用经济的原密封路径");
    // 可选有限验证只从原源值重编码typedraw；新DPT身份不冒充旧V3构造身份。
    let compact_source = std::env::var_os("B_GPU_COMPACT_BASE_SOURCE").map(|_| {
        public_consumption_tests::gpu_compact_hidden_for_order_fixture(&initial, &events)
    });
    let input = serde_json::json!({"hidden_material":compact_source.as_ref().map(|(_, value)| value),
        "checkpoints":checkpoints, "initial":initial, "events":events,
        "processing_at":processing_at, "maker_at":maker_at,
        "finality_path":actor._finality_policy.normal_path(),
        "confirmed_economic_path":confirmed_policy.normal_path(),
        "fee_rate":rules.fee_schedule.rate.value().to_string(), "fee_exponent":rules.fee_schedule.exponent,
        "taker_rebate_rate":actor._rule_snapshot.snapshot().project_account_rules.rebate_settlement.value.fixed_taker_rebate_rate.value().to_string(),
        "maker_rebate_rate":actor._rule_snapshot.snapshot().project_account_rules.rebate_settlement.value.fixed_maker_rebate_rate.value().to_string(),
        "market_end":initial[0].market.window.end_at_utc_ms,
        "post":post,"post_request_key":"gpu-legal-gtc-post","post_input_sequence":receipt.input_sequence(),
        "post_sent_at":1200,"post_terms":match &request {
            BusinessRequestV2::PostOrders(request) => serde_json::json!({
                "ordinal":request.items[0].batch_item_ordinal,
                "item_id":request.items[0].item.item_id,"order":request.items[0].item.order}),
            _ => unreachable!(),
        },"proxy_latency_sha256":actor._latency_profile.file_sha256(),
        "cancel_arrived_at":cancel_receipt.platform_arrived_at_utc_ms,
        "cancel_request_key":"gpu-legal-gtc-cancel","cancel_input_sequence":cancel_receipt.input_sequence(),
        "cancel_sent_at":maker_at+30,
        "cancel_records":cancel_records,"cancel_batches":cancel_batches,"cancel_closed_at":closed_at,
        "prior_records":prior_records,"prior_batches":prior_batches,"prior_closed_at":prefix_at,
        "opening_ledger":opening_ledger,
        "cancel_ledger_before":cancel_ledger_before,"cancel_ledger_after":cancel_ledger_after,"cancel_ledger_attempts":cancel_ledger_attempts,
        "next_private":next_private,"next_batch":next_batch,"session_seed":state.session.as_ref().unwrap().session_seed.0});
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

#[cfg(feature = "shared-execution")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "AutoDL真实失败最终性V4反馈与Ledger成本补偿关联"]
async fn gpu_real_failed_fill_reverses_original_applied_cost_and_fee_once() {
    for (index, economic_ordinal) in [None, Some(0_u32), Some(1_u32)].into_iter().enumerate() {
        let run_id = format!("run-gpu-ledger-failure-{index}");
        let original_spec = spec(&run_id, "gpu-ledger-failure-create");
        let account_key = contracts::AccountInstanceKeyV1 {
            run_id: original_spec.run_id,
            caller_instance_id: original_spec.caller_instance_id,
            account_id: original_spec.account_id,
            caller_role: SimCallerRoleV1::ModelTrain,
            platform_source: contracts::PlatformSourceV1::Simulated,
        };
        let path = serde_json::json!({"steps":[
            {"state":"MATCHED","offset_ms":0},
            {"state":"MINED","offset_ms":10},
            {"state":"FAILED","offset_ms":20}],"economic_application_ordinal":economic_ordinal});
        let policy = Arc::new(TradeFinalityPolicyV2::decode_and_seal(
            &serde_json::to_vec(&serde_json::json!({
                "schema_version":"trade_finality_policy_v2",
                "normal_path":successful_policy_v2_for_tests().normal_path(),
                "failure_overrides":[{"selector":{"selector_domain":"REQUEST",
                    "account_instance_key":account_key,"global_request_id":"gpu-ledger-failure-post"},
                    "path":path}],
            })).unwrap(),
        ).unwrap());
        let saved = Rc::new(RefCell::new(None));
        let capture = Rc::clone(&saved);
        let normalization = Rc::new(RefCell::new(Vec::new()));
        let normalization_capture = Rc::clone(&normalization);
        let create_request_id = format!("create-{run_id}");
        let (_directory, mut actor) = active_market_actor_with_projection(
            SimCallerRoleV1::ModelTrain, &run_id, policy.clone(), move |log_path, opened| {
                opened.projection_version = crate::runtime_v2::BusinessProjectionVersionV1::V4;
                let reference = contracts::execution_normalization::DurableSourceReferenceV1 {
                    source: contracts::execution_normalization::ImmutableSourceReferenceV1::Location(
                        log_path.display().to_string(),
                    ),
                };
                let memory = noe_projection::MemoryNoeProjectionPortV4::new(
                    log_path.parent().unwrap().join("noe-memory"), opened.clone(),
                    reference.clone(), reference.clone(), create_request_id.clone(), "failure-source".into(),
                );
                *capture.borrow_mut() = Some(memory.observation());
                let durable = noe_projection::SimV2NoeProjectionPortV4::new(
                    log_path.parent().unwrap().join("noe-durable"), opened.clone(),
                    reference.clone(), reference, create_request_id.clone(), "failure-source".into(),
                );
                Box::new(GpuNoeReferencePort {
                    memory, durable, normalization_reference: normalization_capture, normalization_core: None,
                })
            },
        ).await;
        let observation = saved.borrow_mut().take().unwrap();
        let mut ledger = ledger::LedgerWriterV1::new_with_admission_v2(
            ledger::AccountAdmissionPolicyV2::DualCashNoMarketLimit,
        );
        ledger.apply_noe_memory_v2(&observation.opening_record().unwrap()).unwrap();
        for record in observation.take_closed_records() {
            ledger.apply_noe_memory_v2(&record).unwrap();
        }
        let mut initial = decoded_maker_initial_events();
        let ExecutionMarketEventPayloadV3::InitialL2Snapshot(book) = &mut initial[0].payload else { unreachable!() };
        book.asks = vec![contracts::PriceLevelV1 {
            price_pusd_per_share: ExactDecimal("0.5".into()), quantity_shares: ExactDecimal("5".into()),
        }];
        initial[0].event_id = data_pipeline_core::derive_execution_market_event_v3_id(&initial[0]).unwrap();
        initial[0] = data_pipeline_core::decode_execution_market_event_v3(
            &data_pipeline_core::encode_execution_market_event_v3(&initial[0]).unwrap(),
        ).unwrap();
        let condition = initial[0].market.condition_id.clone();
        let (state, _) = reduce_market_event_slice_v2(
            actor.state.take().unwrap(), MarketEventPathV1::HiddenPlatform, 1_100, &initial,
            actor._rule_snapshot.snapshot(), policy.as_ref(),
        ).unwrap_or_else(|_| panic!("原双腿初始化失败"));
        let request = BusinessRequestV2::PostOrders(PostOrdersRequestV2 {
            context: RequestContextV2 { global_request_id: "gpu-ledger-failure-post".into(), caller_correlation_id: None },
            items: vec![PostOrderBatchItemV2 { batch_item_ordinal: 0, item: post_item(
                "gpu-ledger-failure-item", &condition, &TokenId("1".into()),
                SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: "0.6".parse().unwrap(),
                    size_shares: "10".parse().unwrap(), post_only: false,
                },
            ) }],
        });
        let (state, receipt) = register_request_candidate(
            &actor.opened, actor.scenario.initial_market_directory(), state, &request_cutoff(1_200),
            &request, 11, actor._latency_profile.as_ref(), actor._rule_snapshot.snapshot(),
        );
        let (state, accepted) = advance_mutation_candidate_with_outputs(
            &actor.opened, actor.scenario.initial_market_directory(), state,
            receipt.platform_arrived_at_utc_ms, actor._rule_snapshot.snapshot(), policy.as_ref(),
        );
        let accepted = accepted.unwrap();
        let MutationActionBusinessEffectV3::Post(post) = &accepted.business else { panic!("原提交结果"); };
        assert!(matches!(post.outcome, PostOrderItemOutcomeV2::Accepted(_)));
        let mut batches = Vec::new();
        let mut state = gpu_accept_projection_reference(
            &mut actor, state, BusinessReducerOutputV2::MutationAction(Some(accepted)), &mut batches,
        );
        while let Some(at) = state.session.as_ref().unwrap().execution_state.next_scheduled_finality_at_utc_ms() {
            let (next, effects) = advance_finality_to_candidate(
                state, at, actor._rule_snapshot.snapshot(), policy.as_ref(),
            );
            state = gpu_accept_projection_reference(
                &mut actor, next, BusinessReducerOutputV2::LogicalDrive(LogicalDriveOutputV2 {
                    lifecycle: effects.into_lifecycle_feedback().unwrap(), ..Default::default()
                }), &mut batches,
            );
        }
        // 只关闭原已生成反馈的工程观察边界；不重新编号或伪造经济记录。
        actor.projection_port.close_receive_prefix(1_000_000);
        actor.projection_port.observe_public_time(&crate::public_consumption::TrustedActorExecutionTimeV1(
            crate::public_consumption::ActorTimeRecordV1 {
                run_id: actor.opened.run_id.clone(), drive_position: 1, logical_time_utc_ms: 1_000_000,
            },
        ));
        let records = observation.take_closed_records();
        assert!(!records.is_empty());
        let mut pending = records.clone();
        let mut checkpoints = Vec::new();
        while !pending.is_empty() {
            let previous = pending.len();
            let mut waiting = Vec::new();
            for record in pending {
                let outcome = ledger.apply_noe_memory_v2(&record).unwrap();
                checkpoints.push(serde_json::json!({
                    "record":record.record(),"position":record.position(),"visibility":record.visibility(),
                    "after":gpu_ledger_reservation_checkpoint(&ledger,"failure-noe"),
                }));
                if matches!(outcome, ledger::LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation
                    | ledger::LedgerMemoryApplyOutcomeV1::Applied(ledger::LedgerApplyOutcomeV1::MissingPlatformPredecessor {..})) {
                    waiting.push(record);
                }
            }
            assert!(waiting.len() < previous, "真实失败反馈前驱无法闭合");
            pending = waiting;
        }
        let before_repeat = ledger.projection_v2().unwrap();
        for record in &records { ledger.apply_noe_memory_v2(record).unwrap(); }
        assert_eq!(ledger.projection_v2().unwrap(), before_repeat, "重复原位置不能二次补偿");
        let artifact = std::path::PathBuf::from(std::env::var("B_GPU_ORDER_ARTIFACT_DIR").unwrap());
        std::fs::create_dir_all(&artifact).unwrap();
        std::fs::write(artifact.join(format!("original-failed-fill-{index}.json")),
            serde_json::to_vec_pretty(&serde_json::json!({"path":path,"batches":batches,
                "checkpoints":checkpoints,"final":before_repeat})).unwrap()).unwrap();
        let platform = &before_repeat.platform_confirmed_projection;
        eprintln!("failure_cost_case={index} application_ordinal={economic_ordinal:?} cost={} fee={}",
            platform.position_cost_basis_pusd.0, platform.fees_pusd.0);
        let zero = |value: &ExactDecimal| value.0.parse::<rust_decimal::Decimal>().unwrap().is_zero();
        assert!(zero(&platform.position_cost_basis_pusd), "失败应撤回此前实际应用成本");
        assert!(zero(&platform.fees_pusd), "失败应撤回此前实际应用费用");
        assert!(platform.outcome_balances.iter().all(|outcome|
            zero(&outcome.balance_shares) && zero(&outcome.position_cost_basis_pusd)));
        assert!(before_repeat.market_projections.iter().all(|market|
            zero(&market.position_cost_basis_pusd) && zero(&market.fees_pusd)));
        let output = std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
            .arg(std::env::var("B_GPU_POSITION_VALIDATOR").unwrap())
            .arg(artifact.join(format!("original-failed-fill-{index}.json")))
            .output().unwrap();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        std::fs::write(artifact.join(format!("failed-position-report-{index}.json")), &output.stdout).unwrap();
        eprintln!("gpu_failed_position={}", String::from_utf8_lossy(&output.stdout));
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "AutoDL原合法多ask逐fill及同时间最终性顺序"]
async fn gpu_legal_multiask_preserves_each_fill_and_same_time_finality_order() {
    let saved_observation=Rc::new(RefCell::new(None));
    let capture=Rc::clone(&saved_observation);
    let normalization_reference=Rc::new(RefCell::new(Vec::new()));
    let normalization_capture=Rc::clone(&normalization_reference);
    let (_directory,mut actor) = active_market_actor_with_projection(
        SimCallerRoleV1::ModelTrain,"run-gpu-multiask",successful_policy_v2_for_tests(),
        move |path,opened| {
            opened.projection_version=crate::runtime_v2::BusinessProjectionVersionV1::V4;
            let reference=contracts::execution_normalization::DurableSourceReferenceV1 {
                source:contracts::execution_normalization::ImmutableSourceReferenceV1::Location(path.display().to_string()),
            };
            let memory=noe_projection::MemoryNoeProjectionPortV4::new(path.parent().unwrap().join("noe-memory"),
                opened.clone(),reference.clone(),reference.clone(),"create-run-gpu-multiask".into(),"gpu-source-reference".into());
            *capture.borrow_mut()=Some(memory.observation());
            let durable=noe_projection::SimV2NoeProjectionPortV4::new(path.parent().unwrap().join("noe-durable"),
                opened.clone(),reference.clone(),reference,"create-run-gpu-multiask".into(),"gpu-source-reference".into());
            Box::new(GpuNoeReferencePort { memory,durable,
                normalization_reference:normalization_capture, normalization_core:None })
        }).await;
    let observation=saved_observation.borrow_mut().take().unwrap();
    assert!(observation.opening_record().is_some(),"开户必须先进入实际NOE上下文");
    let mut feedback_batches=Vec::new();
    // 原合法训练目录登记是非空投影，但没有private反馈草稿。
    let mut state=actor.state.take().unwrap();
    let enrollment=state.session.as_mut().unwrap().enroll_market(&MarketEnrollmentRequestV1 {
        enrollment_request_id:"gpu-before-business-enrollment".into(),market:market('2',"456","3","4"),
    }).unwrap();
    actor.state=Some(gpu_accept_projection_reference(&mut actor,state,
        BusinessReducerOutputV2::MarketEnrollment(enrollment),&mut feedback_batches));
    let initial_records=observation.take_closed_records();
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
    #[cfg(feature="shared-execution")]
    let mut reservation_checkpoints=Vec::new();
    #[cfg(feature="shared-execution")]
    let source_ledger={
        let mut ledger=ledger::LedgerWriterV1::new_with_admission_v2(ledger::AccountAdmissionPolicyV2::DualCashNoMarketLimit);
        ledger.apply_noe_memory_v2(&observation.opening_record().unwrap()).unwrap();
        for record in &initial_records { ledger.apply_noe_memory_v2(record).unwrap(); }
        reservation_checkpoints.push(gpu_ledger_reservation_checkpoint(&ledger,"opening"));
        let cost=actor._rule_snapshot.limit_buy_cost_v1(&num_bigint::BigUint::from(500u32),30,
            &ExactDecimal("0.01".into()),false).unwrap();
        let principal:rust_decimal::Decimal=cost.principal_micro.to_string().parse().unwrap();
        let fee:rust_decimal::Decimal=cost.fee_1e5_units.to_string().parse().unwrap();
        let intent=ledger::ReserveIntentV1 {
            request_identity:ledger::LedgerRequestIdentityV1("gpu-reservation".into()),condition_id:condition.clone(),
            requested_principal_pusd:ExactDecimal((principal/rust_decimal::Decimal::from(1_000_000)).normalize().to_string()),
            immediate_fee_reserve_pusd:ExactDecimal((fee/rust_decimal::Decimal::from(100_000)).normalize().to_string()),
        };
        let decision=ledger.try_reserve_action_v2(std::slice::from_ref(&intent)).unwrap();
        assert!(matches!(decision[0],ledger::LedgerReservationOutcomeV1::Reserved {..}));
        reservation_checkpoints.push(gpu_ledger_reservation_checkpoint(&ledger,"reserved"));
        let repeated=ledger.try_reserve_action_v2(std::slice::from_ref(&intent)).unwrap();
        assert_eq!(repeated,decision,"同请求预留结果粘滞，不二次占用");
        ledger.record_submission_started(&ledger::LedgerLocalEventIdentityV1("gpu-send-started".into()),
            &intent.request_identity,&ledger::LedgerSubmissionItemIdentityV1 {
                global_request_id:"gpu-multiask-post".into(),batch_item_ordinal:0,
            }).unwrap();
        reservation_checkpoints.push(gpu_ledger_reservation_checkpoint(&ledger,"submitted"));
        assert_eq!(ledger.try_reserve_action_v2(std::slice::from_ref(&intent)).unwrap(),decision,
            "发送后原预留决定仍保留最初Reserved序");
        ledger
    };
    let (state,receipt) = register_request_candidate(&actor.opened,actor.scenario.initial_market_directory(),state,
        &request_cutoff(1_200),&request,11,actor._latency_profile.as_ref(),actor._rule_snapshot.snapshot());
    let (state,accepted) = advance_mutation_candidate_with_outputs(&actor.opened,actor.scenario.initial_market_directory(),state,
        receipt.platform_arrived_at_utc_ms,actor._rule_snapshot.snapshot(),actor._finality_policy.as_ref());
    let accepted=accepted.unwrap();
    let MutationActionBusinessEffectV3::Post(post) = &accepted.business else {panic!("原提交结果");};
    assert!(matches!(&post.outcome,PostOrderItemOutcomeV2::Accepted(_)));
    let mut state=gpu_accept_projection_reference(&mut actor,state,
        BusinessReducerOutputV2::MutationAction(Some(accepted)),&mut feedback_batches);
    assert_eq!(state.session.as_ref().unwrap().execution_state.fill_shape_for_tests(),(3,4));
    let mut checkpoints = vec![serde_json::json!({"at":receipt.platform_arrived_at_utc_ms,
        "state":state.session.as_ref().unwrap().execution_state.gpu_order_checkpoint_for_tests("sim-1")})];
    while let Some(at) = state.session.as_ref().unwrap().execution_state.next_scheduled_finality_at_utc_ms() {
        let mut session=state.session.take().unwrap();
        let (next,effect) = session.execution_state.advance_next_scheduled_finality_at(at,
            actor._finality_policy.as_ref(),actor._rule_snapshot.snapshot()).unwrap();
        session.execution_state=next;
        let fill_sequence=effect.transition.as_ref().expect("逐原义务消费").trade_finality.fill_sequence;
        state.session=Some(session);
        state=gpu_accept_projection_reference(&mut actor,state,BusinessReducerOutputV2::LogicalDrive(LogicalDriveOutputV2 {
            lifecycle:effect.into_lifecycle_feedback().unwrap(),..Default::default()
        }),&mut feedback_batches);
        checkpoints.push(serde_json::json!({"at":at,"fill_sequence":fill_sequence,
            "state":state.session.as_ref().unwrap().execution_state.gpu_order_checkpoint_for_tests("sim-1")}));
    }
    assert_eq!(checkpoints.len(),7);
    assert!(observation.take_closed_records().is_empty(),"没有双时间闭合时不能交付动态反馈");
    actor.projection_port.close_receive_prefix(10_000);
    let time=|at| crate::public_consumption::TrustedActorExecutionTimeV1(crate::public_consumption::ActorTimeRecordV1 {
        run_id:actor.opened.run_id.clone(),drive_position:1,logical_time_utc_ms:at,
    });
    actor.projection_port.observe_public_time(&time(1_200));
    assert!(observation.take_closed_records().is_empty(),"hidden领先不得抬高public可见时间");
    actor.projection_port.observe_public_time(&time(10_000));
    let noe=observation.take_closed_records();
    assert!(!noe.is_empty());
    assert!(observation.take_closed_records().is_empty(),"实际领取位置不重复消费");
    #[cfg(feature="shared-execution")]
    let ledger_projection={
    let mut ledger=source_ledger;
    let mut pending=noe.clone();
    while !pending.is_empty() {
        let previous=pending.len();
        let mut remaining=Vec::new();
        for record in pending {
            let outcome=ledger.apply_noe_memory_v2(&record).unwrap();
            reservation_checkpoints.push(gpu_ledger_reservation_checkpoint(&ledger,"noe"));
            match outcome {
                ledger::LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation |
                ledger::LedgerMemoryApplyOutcomeV1::Applied(ledger::LedgerApplyOutcomeV1::MissingPlatformPredecessor {..})
                    => remaining.push(record),
                _ => (),
            }
        }
        assert!(remaining.len()<previous,"真实反馈原前驱未能闭合");
        pending=remaining;
    }
    let ledger_projection=ledger.projection_v2().unwrap();
    let original=&reservation_checkpoints[1];
    let repeated=ledger.try_reserve_action_v2(&[ledger::ReserveIntentV1 {
        request_identity:ledger::LedgerRequestIdentityV1("gpu-reservation".into()),condition_id:condition.clone(),
        requested_principal_pusd:ExactDecimal("1.5".into()),immediate_fee_reserve_pusd:ExactDecimal("0".into()),
    }]).unwrap();
    assert_eq!(repeated,vec![ledger::LedgerReservationOutcomeV1::Reserved {
        ledger_entry_seq:original["request"]["last_ledger_entry_seq"].as_u64().unwrap(),
    }],"转换后即使新参数不同，仍返回原预留决定");
    for record in &noe { ledger.apply_noe_memory_v2(record).unwrap(); }
    assert_eq!(ledger.projection_v2().unwrap(),ledger_projection,"原NOE位置重读不重复写账本");
    serde_json::to_value(ledger_projection).unwrap()
    };
    #[cfg(not(feature="shared-execution"))]
    let ledger_projection=serde_json::Value::Null;
    #[cfg(not(feature="shared-execution"))]
    let reservation_checkpoints=Vec::<serde_json::Value>::new();
    #[cfg(feature="shared-execution")]
    let economic_first_reference={
        let mut ledger=ledger::LedgerWriterV1::new_with_admission_v2(ledger::AccountAdmissionPolicyV2::DualCashNoMarketLimit);
        ledger.apply_noe_memory_v2(&observation.opening_record().unwrap()).unwrap();
        for record in &initial_records { ledger.apply_noe_memory_v2(record).unwrap(); }
        let intent=|id:&str,principal:&str|ledger::ReserveIntentV1 {
            request_identity:ledger::LedgerRequestIdentityV1(id.into()),condition_id:condition.clone(),
            requested_principal_pusd:ExactDecimal(principal.into()),immediate_fee_reserve_pusd:ExactDecimal("0".into()),
        };
        ledger.try_reserve_action_v2(&[intent("gpu-reservation","1.5")]).unwrap();
        ledger.record_submission_started(&ledger::LedgerLocalEventIdentityV1("gpu-send-started".into()),
            &ledger::LedgerRequestIdentityV1("gpu-reservation".into()),&ledger::LedgerSubmissionItemIdentityV1 {
                global_request_id:"gpu-multiask-post".into(),batch_item_ordinal:0,
            }).unwrap();
        ledger.try_reserve_action_v2(&[intent("gpu-unsent-reservation","0.25")]).unwrap();
        let economic=noe.iter().find(|record|match record.record() {
            execution_normalization::NormalizationRecord::EconomicFactV2 {fact,..}=>matches!(fact.economic_identity,
                contracts::execution_normalization::SourceNeutralEconomicIdentityV1::AccountTransition {account_transition_seq:1,..}),
            execution_normalization::NormalizationRecord::EconomicFact {fact,..}=>matches!(fact.economic_identity,
                contracts::execution_normalization::SourceNeutralEconomicIdentityV1::AccountTransition {account_transition_seq:1,..}),
            _=>false,
        }).unwrap();
        ledger.apply_noe_memory_v2(economic).unwrap();
        let unsent=|ledger:&ledger::LedgerWriterV1| {
            let funds=ledger.funds_read_v1();
            let value=funds.request(&ledger::LedgerRequestIdentityV1("gpu-unsent-reservation".into())).unwrap();
            serde_json::json!({"status":value.reservation_state,"last_ledger_entry_seq":value.last_ledger_entry_seq})
        };
        let before=gpu_ledger_reservation_checkpoint(&ledger,"economic_before_submission");
        let unsent_before=unsent(&ledger);
        assert_eq!(before["request"]["status"],"SubmittedUnknown");
        assert_eq!(unsent_before["status"],"ReservedBeforeSend");
        let submission=noe.iter().find(|record|match record.record() {
            execution_normalization::NormalizationRecord::Evidence {evidence,..}=>matches!(evidence,
                execution_normalization::SourceNeutralEvidenceKind::Control {result:contracts::execution_normalization::SourceNeutralControlResult::Submission {..}}),
            _=>false,
        }).unwrap();
        ledger.apply_noe_memory_v2(submission).unwrap();
        let after=gpu_ledger_reservation_checkpoint(&ledger,"submission_after_economic");
        assert_eq!(after["request"]["status"],"Converted");
        assert_eq!(unsent(&ledger),unsent_before);
        let fill_economic=noe.iter().find(|record|match record.record() {
            execution_normalization::NormalizationRecord::EconomicFactV2 {fact,..}=>matches!(fact.economic_identity,
                contracts::execution_normalization::SourceNeutralEconomicIdentityV1::AccountTransition {account_transition_seq:2,..}),
            execution_normalization::NormalizationRecord::EconomicFact {fact,..}=>matches!(fact.economic_identity,
                contracts::execution_normalization::SourceNeutralEconomicIdentityV1::AccountTransition {account_transition_seq:2,..}),
            _=>false,
        }).unwrap();
        ledger.apply_noe_memory_v2(fill_economic).unwrap();
        let economic_before_trade=gpu_ledger_reservation_checkpoint(&ledger,"fill_economic_before_trade");
        let pending_trade=noe.iter().find(|record|match record.record() {
            execution_normalization::NormalizationRecord::Evidence {evidence:execution_normalization::SourceNeutralEvidenceKind::TradeLifecycleV3 {result},..}
                => result.result.fill_sequence==1 && result.result.state==contracts::execution_normalization::SourceNeutralTradeStateV1::Matched,
            _=>false,
        }).unwrap();
        ledger.apply_noe_memory_v2(pending_trade).unwrap();
        let pending_after_economic=gpu_ledger_reservation_checkpoint(&ledger,"pending_trade_after_economic");
        assert_eq!(pending_after_economic["projection"]["platform_confirmed_projection"]["pending_fill_count"],1);
        serde_json::json!({"before":before,"after":after,"unsent":unsent_before,
            "economic_before_trade":economic_before_trade,"pending_after_economic":pending_after_economic})
    };
    #[cfg(not(feature="shared-execution"))]
    let economic_first_reference=serde_json::Value::Null;
    #[cfg(feature="shared-execution")]
    let contribution_first_reference={
        let mut ledger=ledger::LedgerWriterV1::new_with_admission_v2(ledger::AccountAdmissionPolicyV2::DualCashNoMarketLimit);
        ledger.apply_noe_memory_v2(&observation.opening_record().unwrap()).unwrap();
        for record in &initial_records { ledger.apply_noe_memory_v2(record).unwrap(); }
        let contribution=noe.iter().find(|record|matches!(record.record(),
            execution_normalization::NormalizationRecord::Evidence {
                evidence:execution_normalization::SourceNeutralEvidenceKind::SettlementLifecycleV2 {
                    result:contracts::economics::SourceNeutralSettlementLifecycleResultV2::Contribution(_),
                },..
            })).unwrap();
        ledger.apply_noe_memory_v2(contribution).unwrap();
        assert_eq!(ledger.economic_read().unwrap().trades().count(),0,"Taker贡献不等待成交详情");
        let checkpoint=gpu_ledger_reservation_checkpoint(&ledger,"contribution_before_trade");
        serde_json::json!({"record":contribution.record(),"checkpoint":checkpoint})
    };
    #[cfg(not(feature="shared-execution"))]
    let contribution_first_reference=serde_json::Value::Null;
    let records=noe.iter().map(|record|serde_json::json!({"record":record.record(),
        "position":record.position(),"visibility":record.visibility()})).collect::<Vec<_>>();
    let rules = &actor._rule_snapshot.snapshot().synchronized_platform_rules;
    let input = serde_json::json!({"initial":initial,"addition":addition,"checkpoints":checkpoints,"processing_at":receipt.platform_arrived_at_utc_ms,
        "finality_path":actor._finality_policy.normal_path(),"fee_rate":rules.fee_schedule.rate.value().to_string(),
        "fee_exponent":rules.fee_schedule.exponent,"taker_rebate_rate":actor._rule_snapshot.snapshot().project_account_rules.rebate_settlement.value.fixed_taker_rebate_rate.value().to_string(),
        "feedback_batches":feedback_batches,"noe_records":records,"initial_noe_records":initial_records.len(),
        "economic_first_reference":economic_first_reference,"contribution_first_reference":contribution_first_reference,
        "normalization_reference":normalization_reference.borrow().clone(),
        "ledger_projection":ledger_projection,"reservation_checkpoints":reservation_checkpoints,
        "opening_available_pusd":state.session.as_ref().unwrap().opening_fact.initial_account_state.available_pusd,
        "session_seed":state.session.as_ref().unwrap().session_seed.0});
    let mut child = std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(std::env::var("B_GPU_MULTIASK_VALIDATOR").unwrap()).stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(),&input).unwrap();
    let output=child.wait_with_output().unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    eprintln!("gpu_multiask={}",String::from_utf8(output.stdout).unwrap());
}

#[cfg(feature="shared-execution")]
#[tokio::test(flavor="current_thread")]
#[ignore="AutoDL真实外层FOK成熟日NOE与连续Ledger消费对照"]
async fn gpu_actual_fok_mature_day_preserves_noe_batch_and_continuous_ledger() {
    let run_id="run-gpu-fok-mature-day";
    let policy=Arc::new(TradeFinalityPolicyV2::decode_and_seal(
        br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10},{"state":"CONFIRMED","offset_ms":87000000}],"economic_application_ordinal":1},"failure_overrides":[]}"#
    ).unwrap());
    let saved=Rc::new(RefCell::new(None));
    let capture=Rc::clone(&saved);
    let normalization=Rc::new(RefCell::new(Vec::new()));
    let normalization_capture=Rc::clone(&normalization);
    let (_directory,mut actor)=active_market_actor_with_projection_and_cash(
        SimCallerRoleV1::ModelTrain,run_id,policy.clone(),move |path,opened| {
            opened.projection_version=crate::runtime_v2::BusinessProjectionVersionV1::V4;
            let reference=contracts::execution_normalization::DurableSourceReferenceV1 {
                source:contracts::execution_normalization::ImmutableSourceReferenceV1::Location(path.display().to_string()),
            };
            let memory=noe_projection::MemoryNoeProjectionPortV4::new(path.parent().unwrap().join("noe-memory"),
                opened.clone(),reference.clone(),reference.clone(),"create-run-gpu-fok-mature-day".into(),"gpu-day-source".into());
            *capture.borrow_mut()=Some(memory.observation());
            let durable=noe_projection::SimV2NoeProjectionPortV4::new(path.parent().unwrap().join("noe-durable"),
                opened.clone(),reference.clone(),reference,"create-run-gpu-fok-mature-day".into(),"gpu-day-source".into());
            Box::new(GpuNoeReferencePort {memory,durable,normalization_reference:normalization_capture,normalization_core:None})
        },"2000").await;
    let observation=saved.borrow_mut().take().unwrap();
    let mut ledger=ledger::LedgerWriterV1::new_with_admission_v2(ledger::AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    ledger.apply_noe_memory_v2(&observation.opening_record().unwrap()).unwrap();
    for record in observation.take_closed_records() { ledger.apply_noe_memory_v2(&record).unwrap(); }
    let mut initial=decoded_maker_initial_events();
    let ExecutionMarketEventPayloadV3::InitialL2Snapshot(book)=&mut initial[0].payload else {unreachable!()};
    book.asks=vec![contracts::PriceLevelV1 {price_pusd_per_share:ExactDecimal("0.5".into()),
        quantity_shares:ExactDecimal("1000.001".into())}];
    initial[0].event_id=data_pipeline_core::derive_execution_market_event_v3_id(&initial[0]).unwrap();
    initial[0]=data_pipeline_core::decode_execution_market_event_v3(&data_pipeline_core::encode_execution_market_event_v3(&initial[0]).unwrap()).unwrap();
    let condition=initial[0].market.condition_id.clone();
    let (state,_)=reduce_market_event_slice_v2(actor.state.take().unwrap(),MarketEventPathV1::HiddenPlatform,1100,
        &initial,actor._rule_snapshot.snapshot(),policy.as_ref()).unwrap_or_else(|_|panic!("真实双腿初始化"));
    let request=BusinessRequestV2::PostOrders(PostOrdersRequestV2 {
        context:RequestContextV2 {global_request_id:"gpu-fok-mature-post".into(),caller_correlation_id:None},
        items:vec![PostOrderBatchItemV2 {batch_item_ordinal:0,item:post_item("gpu-fok-mature-item",&condition,&TokenId("1".into()),
            SimBuyOrderKindV1::FokMarket {spend_pusd:"500.0005".parse().unwrap(),worst_price_pusd_per_share:"0.5".parse().unwrap()})}],
    });
    let (state,receipt)=register_request_candidate(&actor.opened,actor.scenario.initial_market_directory(),state,
        &request_cutoff(1200),&request,11,actor._latency_profile.as_ref(),actor._rule_snapshot.snapshot());
    let delay=i64::try_from(actor._rule_snapshot.snapshot().documented_platform_rules.taker_delay.value.delay_ms).unwrap();
    let processing=receipt.platform_arrived_at_utc_ms+delay;
    let (state,accepted)=advance_mutation_candidate_with_outputs(&actor.opened,actor.scenario.initial_market_directory(),state,
        processing,actor._rule_snapshot.snapshot(),policy.as_ref());
    let accepted=accepted.expect("合法FOK实际处理");
    let MutationActionBusinessEffectV3::Post(post)=&accepted.business else {panic!("真实FOK响应");};
    assert!(matches!(post.outcome,PostOrderItemOutcomeV2::Accepted(_)),"{:?}",post.outcome);
    let mut batches=Vec::new();
    let state=gpu_accept_projection_reference(&mut actor,state,BusinessReducerOutputV2::MutationAction(Some(accepted)),&mut batches);
    let (state,effect)=advance_finality_to_candidate(state,processing+10,actor._rule_snapshot.snapshot(),policy.as_ref());
    let mut state=gpu_accept_projection_reference(&mut actor,state,BusinessReducerOutputV2::LogicalDrive(LogicalDriveOutputV2 {
        lifecycle:effect.into_lifecycle_feedback().unwrap(),..Default::default()
    }),&mut batches);
    while let Some(at)=state.session.as_ref().unwrap().execution_state.next_rebate_clock_at_utc_ms() {
        if at>87_000_000 {break;}
        let (next,effect)=advance_rebate_clock_to_candidate(state,at);
        state=next;
        let lifecycle=effect.into_lifecycle_feedback().unwrap();
        if !lifecycle.is_empty() {
            state=gpu_accept_projection_reference(&mut actor,state,BusinessReducerOutputV2::LogicalDrive(LogicalDriveOutputV2 {
                lifecycle,..Default::default()
            }),&mut batches);
        }
    }
    let public_time=|at|crate::public_consumption::TrustedActorExecutionTimeV1(crate::public_consumption::ActorTimeRecordV1 {
        run_id:run_id.into(),drive_position:1,logical_time_utc_ms:at,
    });
    actor.projection_port.close_receive_prefix(87_000_000);
    actor.projection_port.observe_public_time(&public_time(87_000_000));
    let mut previous=observation.take_closed_records();
    while !previous.is_empty() {
        let count=previous.len();
        let mut waiting=Vec::new();
        for record in previous {
            match ledger.apply_noe_memory_v2(&record).unwrap() {
                ledger::LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation |
                ledger::LedgerMemoryApplyOutcomeV1::Applied(ledger::LedgerApplyOutcomeV1::MissingPlatformPredecessor {..})=>waiting.push(record),
                _=>(),
            }
        }
        assert!(waiting.len()<count,"真实初始前缀可闭合");
        previous=waiting;
    }
    let before=gpu_ledger_reservation_checkpoint(&ledger,"before_confirmed");
    let next_private=state.session.as_ref().unwrap().next_private_feedback_sequence;
    let next_batch=state.session.as_ref().unwrap().next_projection_batch_sequence;
    let account_before=state.session.as_ref().unwrap().execution_state.gpu_order_checkpoint_for_tests("sim-1");
    let (state,effect)=advance_finality_to_candidate(state,processing+87_000_000,
        actor._rule_snapshot.snapshot(),policy.as_ref());
    let state=gpu_accept_projection_reference(&mut actor,state,BusinessReducerOutputV2::LogicalDrive(LogicalDriveOutputV2 {
        lifecycle:effect.into_lifecycle_feedback().unwrap(),..Default::default()
    }),&mut batches);
    let final_at=processing+87_000_000;
    let closed_at=final_at+10_000;
    actor.projection_port.close_receive_prefix(closed_at);
    actor.projection_port.observe_public_time(&public_time(closed_at));
    let received=observation.take_closed_records();
    assert_eq!(received.len(),7,"真实终态Trade/Accrual/Day/Payment与3经济事实");
    let mut pending=received.clone();
    let mut attempts=Vec::new();
    while !pending.is_empty() {
        let count=pending.len();
        let mut waiting=Vec::new();
        for record in pending {
            let prior=gpu_ledger_reservation_checkpoint(&ledger,"before");
            let outcome=ledger.apply_noe_memory_v2(&record).unwrap();
            let deferred=matches!(outcome,ledger::LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation |
                ledger::LedgerMemoryApplyOutcomeV1::Applied(ledger::LedgerApplyOutcomeV1::MissingPlatformPredecessor {..}));
            attempts.push(serde_json::json!({"record":record.record(),"position":record.position(),"visibility":record.visibility(),
                "deferred":deferred,"before":prior,"after":gpu_ledger_reservation_checkpoint(&ledger,"after")}));
            if deferred {waiting.push(record);}
        }
        assert!(waiting.len()<count,"日业务前驱等待能按原位置重试闭合");
        pending=waiting;
    }
    let records=received.iter().map(|record|serde_json::json!({"record":record.record(),"position":record.position(),
        "visibility":record.visibility()})).collect::<Vec<_>>();
    let source=serde_json::json!({"source_contract":"original_outer_FOK_SIM_NOE_V4_Ledger",
        "before":before,"account_before":account_before,"after":gpu_ledger_reservation_checkpoint(&ledger,"after_all"),
        "records":records,"attempts":attempts,"next_private":next_private,"next_batch":next_batch,
        "processing_at":processing,"confirmed_at":final_at,"closed_at":closed_at,
        "finality_path":policy.normal_path(),"session_seed":state.session.as_ref().unwrap().session_seed.0,
        "taker_rebate_rate":actor._rule_snapshot.snapshot().project_account_rules.rebate_settlement.value.fixed_taker_rebate_rate.value().to_string()});
    let artifact=std::path::PathBuf::from(std::env::var("B_GPU_ORDER_ARTIFACT_DIR").unwrap());
    std::fs::create_dir_all(&artifact).unwrap();
    std::fs::write(artifact.join("original-outer-fok-mature-day.json"),serde_json::to_vec_pretty(&source).unwrap()).unwrap();
    let mut child=std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(std::env::var("B_GPU_FOK_DAY_VALIDATOR").unwrap()).stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(),&source).unwrap();
    let output=child.wait_with_output().unwrap();
    std::fs::write(artifact.join("outer-fok-day-report.json"),&output.stdout).unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    eprintln!("gpu_outer_fok_day={}",String::from_utf8_lossy(&output.stdout));
}

#[cfg(feature="shared-execution")]
#[tokio::test(flavor="current_thread")]
#[ignore="AutoDL真实外层Maker成熟日NOE与连续Ledger消费对照"]
async fn gpu_actual_maker_mature_day_preserves_noe_batch_and_continuous_ledger() {
    let reference_started=std::time::Instant::now();
    let run_id="run-gpu-maker-mature-day";
    let policy=Arc::new(TradeFinalityPolicyV2::decode_and_seal(
        br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10},{"state":"CONFIRMED","offset_ms":20}],"economic_application_ordinal":2},"failure_overrides":[]}"#
    ).unwrap());
    let saved=Rc::new(RefCell::new(None));
    let capture=Rc::clone(&saved);
    let normalization=Rc::new(RefCell::new(Vec::new()));
    let normalization_capture=Rc::clone(&normalization);
    let (_directory,mut actor)=active_market_actor_with_projection_and_cash(
        SimCallerRoleV1::ModelTrain,run_id,policy.clone(),move |path,opened| {
            opened.projection_version=crate::runtime_v2::BusinessProjectionVersionV1::V4;
            let reference=contracts::execution_normalization::DurableSourceReferenceV1 {
                source:contracts::execution_normalization::ImmutableSourceReferenceV1::Location(path.display().to_string()),
            };
            let memory=noe_projection::MemoryNoeProjectionPortV4::new(path.parent().unwrap().join("noe-memory"),
                opened.clone(),reference.clone(),reference.clone(),"create-run-gpu-maker-mature-day".into(),"gpu-day-source".into());
            *capture.borrow_mut()=Some(memory.observation());
            let durable=noe_projection::SimV2NoeProjectionPortV4::new(path.parent().unwrap().join("noe-durable"),
                opened.clone(),reference.clone(),reference,"create-run-gpu-maker-mature-day".into(),"gpu-day-source".into());
            Box::new(GpuNoeReferencePort {memory,durable,normalization_reference:normalization_capture,normalization_core:None})
        },"639.50052").await;
    let observation=saved.borrow_mut().take().unwrap();
    let mut ledger=ledger::LedgerWriterV1::new_with_admission_v2(ledger::AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    ledger.apply_noe_memory_v2(&observation.opening_record().unwrap()).unwrap();
    for record in observation.take_closed_records() { ledger.apply_noe_memory_v2(&record).unwrap(); }
    let mut initial=decoded_maker_initial_events();
    // 通过原编码/解码边界建立已初始化空盘口，不能用tick代替L2初始化。
    for event in &mut initial {
        let ExecutionMarketEventPayloadV3::InitialL2Snapshot(book)=&mut event.payload else {unreachable!()};
        book.bids.clear();
        book.asks.clear();
        event.event_id=data_pipeline_core::derive_execution_market_event_v3_id(event).unwrap();
        *event=data_pipeline_core::decode_execution_market_event_v3(&data_pipeline_core::encode_execution_market_event_v3(event).unwrap()).unwrap();
    }
    let market_end=initial[0].market.window.end_at_utc_ms;
    let condition=initial[0].market.condition_id.clone();
    let (state,_)=reduce_market_event_slice_v2(actor.state.take().unwrap(),MarketEventPathV1::HiddenPlatform,1100,
        &initial,actor._rule_snapshot.snapshot(),policy.as_ref()).unwrap_or_else(|_|panic!("真实双腿初始化"));
    let request=BusinessRequestV2::PostOrders(PostOrdersRequestV2 {
        context:RequestContextV2 {global_request_id:"gpu-maker-mature-post".into(),caller_correlation_id:None},
        items:vec![PostOrderBatchItemV2 {batch_item_ordinal:0,item:post_item("gpu-maker-mature-item",&condition,&TokenId("1".into()),
            SimBuyOrderKindV1::GtcLimit {limit_price_pusd_per_share:"0.4".parse().unwrap(),size_shares:"305".parse().unwrap(),post_only:false})}],
    });
    let (state,receipt)=register_request_candidate(&actor.opened,actor.scenario.initial_market_directory(),state,
        &request_cutoff(1200),&request,11,actor._latency_profile.as_ref(),actor._rule_snapshot.snapshot());
    let processing=receipt.platform_arrived_at_utc_ms;
    let (state,accepted)=advance_mutation_candidate_with_outputs(&actor.opened,actor.scenario.initial_market_directory(),state,
        processing,actor._rule_snapshot.snapshot(),policy.as_ref());
    let accepted=accepted.expect("合法Maker GTC实际处理");
    let MutationActionBusinessEffectV3::Post(post)=&accepted.business else {panic!("真实Maker GTC响应");};
    assert!(matches!(post.outcome,PostOrderItemOutcomeV2::Accepted(_)),"{:?}",post.outcome);
    let mut batches=Vec::new();
    let state=gpu_accept_projection_reference(&mut actor,state,BusinessReducerOutputV2::MutationAction(Some(accepted)),&mut batches);
    let public_time=|at|crate::public_consumption::TrustedActorExecutionTimeV1(crate::public_consumption::ActorTimeRecordV1 {
        run_id:run_id.into(),drive_position:1,logical_time_utc_ms:at,
    });
    let prefix_at=batches[0]["orders"].as_array().unwrap().iter()
        .map(|value|value["metadata"]["feedback_received_at_utc_ms"].as_i64().unwrap())
        .chain(batches[0]["direct"].as_array().unwrap().iter().map(|value|
            value["metadata"]["response_received_at_utc_ms"].as_i64().unwrap())).max().unwrap();
    actor.projection_port.close_receive_prefix(prefix_at);
    actor.projection_port.observe_public_time(&public_time(prefix_at));
    let mut previous=observation.take_closed_records();
    while !previous.is_empty() {
        let count=previous.len();
        let mut waiting=Vec::new();
        for record in previous {
            match ledger.apply_noe_memory_v2(&record).unwrap() {
                ledger::LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation |
                ledger::LedgerMemoryApplyOutcomeV1::Applied(ledger::LedgerApplyOutcomeV1::MissingPlatformPredecessor {..})=>waiting.push(record),
                _=>(),
            }
        }
        assert!(waiting.len()<count,"真实初始前缀可闭合");
        previous=waiting;
    }
    let before=gpu_ledger_reservation_checkpoint(&ledger,"accepted_prefix");
    let next_private=state.session.as_ref().unwrap().next_private_feedback_sequence;
    let next_batch=state.session.as_ref().unwrap().next_projection_batch_sequence;
    let account_before=state.session.as_ref().unwrap().execution_state.gpu_order_checkpoint_for_tests("sim-1");
    let maker_at=prefix_at+100;
    assert!(maker_at+30<market_end,"工程原请求必须在市场结束前处理");
    let mut state=state;
    let mut events=Vec::new();
    for event in [
        decoded_maker_level_event(BookSide::Bid,"0.4","0","300",maker_at,maker_at+1,10),
        decoded_maker_level_event(BookSide::Bid,"0.4","300","0",maker_at+10,maker_at+11,11),
    ] {
        let at=event.time.source_event_at_utc_ms.unwrap();
        let (next,effect)=reduce_market_event_slice_v2(state,MarketEventPathV1::HiddenPlatform,at,
            std::slice::from_ref(&event),actor._rule_snapshot.snapshot(),policy.as_ref()).unwrap_or_else(|_|panic!("实际hidden Maker行情"));
        state=next;
        let lifecycle=effect.execution.into_iter().flat_map(|value|value.into_lifecycle_feedback().unwrap()).collect::<Vec<_>>();
        if !lifecycle.is_empty() {
            state=gpu_accept_projection_reference(&mut actor,state,BusinessReducerOutputV2::LogicalDrive(
                LogicalDriveOutputV2 {lifecycle,..Default::default()}),&mut batches);
        }
        events.push(event);
    }
    while let Some(at)=state.session.as_ref().unwrap().execution_state.next_scheduled_finality_at_utc_ms() {
        let (next,effect)=advance_finality_to_candidate(state,at,actor._rule_snapshot.snapshot(),policy.as_ref());
        state=gpu_accept_projection_reference(&mut actor,next,BusinessReducerOutputV2::LogicalDrive(
            LogicalDriveOutputV2 {lifecycle:effect.into_lifecycle_feedback().unwrap(),..Default::default()}),&mut batches);
    }
    while let Some(at)=state.session.as_ref().unwrap().execution_state.next_rebate_clock_at_utc_ms() {
        if at>89_100_000 {break;}
        let (next,effect)=advance_rebate_clock_to_candidate(state,at);
        state=next;
        let lifecycle=effect.into_lifecycle_feedback().unwrap();
        if !lifecycle.is_empty() {
            state=gpu_accept_projection_reference(&mut actor,state,BusinessReducerOutputV2::LogicalDrive(
                LogicalDriveOutputV2 {lifecycle,..Default::default()}),&mut batches);
        }
    }
    let final_at=89_100_000;
    let closed_at=final_at+10_000;
    actor.projection_port.close_receive_prefix(closed_at);
    actor.projection_port.observe_public_time(&public_time(closed_at));
    let received=observation.take_closed_records();
    assert_eq!(received.len(),9,"真实3Trade、Maker市场/日结/付款与3经济事实");
    let original_producer_seconds=reference_started.elapsed().as_secs_f64();
    let original_ledger_started=std::time::Instant::now();
    let mut pending=received.clone();
    let mut attempts=Vec::new();
    while !pending.is_empty() {
        let count=pending.len();
        let mut waiting=Vec::new();
        for record in pending {
            let prior=gpu_ledger_reservation_checkpoint(&ledger,"before");
            let outcome=ledger.apply_noe_memory_v2(&record).unwrap();
            let deferred=matches!(outcome,ledger::LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation |
                ledger::LedgerMemoryApplyOutcomeV1::Applied(ledger::LedgerApplyOutcomeV1::MissingPlatformPredecessor {..}));
            attempts.push(serde_json::json!({"record":record.record(),"position":record.position(),"visibility":record.visibility(),
                "deferred":deferred,"before":prior,"after":gpu_ledger_reservation_checkpoint(&ledger,"after")}));
            if deferred {waiting.push(record);}
        }
        assert!(waiting.len()<count,"日业务前驱等待能按原位置重试闭合");
        pending=waiting;
    }
    let original_ledger_seconds=original_ledger_started.elapsed().as_secs_f64();
    let records=received.iter().map(|record|serde_json::json!({"record":record.record(),"position":record.position(),
        "visibility":record.visibility()})).collect::<Vec<_>>();
    let source=serde_json::json!({"source_contract":"original_outer_GTC_hidden_Maker_SIM_NOE_V4_Ledger",
        "before":before,"account_before":account_before,"after":gpu_ledger_reservation_checkpoint(&ledger,"after_all"),
        "records":records,"attempts":attempts,"next_private":next_private,"next_batch":next_batch,
        "processing_at":processing,"maker_at":maker_at+10,"market_end":market_end,"day_at":final_at,"events":events,"batches":batches,"closed_at":closed_at,
        "finality_path":policy.normal_path(),"session_seed":state.session.as_ref().unwrap().session_seed.0,
        "maker_rate":actor._rule_snapshot.snapshot().project_account_rules.rebate_settlement.value.fixed_maker_rebate_rate.value().to_string(),
        "fee_rate":actor._rule_snapshot.snapshot().synchronized_platform_rules.fee_schedule.rate.value().to_string(),
        "fee_exponent":actor._rule_snapshot.snapshot().synchronized_platform_rules.fee_schedule.exponent});
    eprintln!("maker_original_reference_stages={}",serde_json::json!({
        "SIM_NOE_opening_and_prefix_reference_seconds":original_producer_seconds,
        "Ledger_apply_and_checkpoint_reference_seconds":original_ledger_seconds,
        "original_attempts":attempts.len(),"original_records":received.len(),
    }));
    let artifact=std::path::PathBuf::from(std::env::var("B_GPU_ORDER_ARTIFACT_DIR").unwrap());
    std::fs::create_dir_all(&artifact).unwrap();
    std::fs::write(artifact.join("original-outer-maker-mature-day.json"),serde_json::to_vec_pretty(&source).unwrap()).unwrap();
    let mut child=std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(std::env::var("B_GPU_OUTER_MAKER_VALIDATOR").unwrap()).stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::inherit()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(),&source).unwrap();
    let output=child.wait_with_output().unwrap();
    std::fs::write(artifact.join("outer-maker-day-report.json"),&output.stdout).unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    eprintln!("gpu_outer_maker_day={}",String::from_utf8_lossy(&output.stdout));
}

fn gpu_accept_projection_reference(actor:&mut SimRunActorV2,state:BusinessReducerStateV2,
    output:BusinessReducerOutputV2,batches:&mut Vec<serde_json::Value>) -> BusinessReducerStateV2 {
    let (state,_,batch)=finalize_projection_batch_v2(&actor.opened.run_id,state,output,
        actor._latency_profile.as_ref()).unwrap();
    let batch=batch.expect("真实接单/最终性应产生非空原投影");
    let direct=batch.direct_responses().map(|response| match response {
        BusinessDirectResponse::PostOrders(result)=>serde_json::json!({"metadata":result.metadata}),
        BusinessDirectResponse::CancelOrder(result)=>serde_json::json!({"metadata":result.metadata}),
        BusinessDirectResponse::CancelOrders(result)=>serde_json::json!({"metadata":result.metadata}),
        BusinessDirectResponse::CancelMarketOrders(result)=>serde_json::json!({"metadata":result.metadata}),
        _=>panic!("此有限工程入口尚未映射该直接响应"),
    }).collect::<Vec<_>>();
    batches.push(serde_json::json!({"batch_sequence":batch.projection_batch_sequence,
        "has_control_admission":batch.account_opening.is_some() || batch.market_enrollment.is_some(),
        "orders":batch.order_lifecycle,"trades":batch.trade_lifecycle,
        "settlements":batch.settlement_lifecycle,"direct":direct}));
    assert!(actor.projection_port.try_accept_payload(Arc::new(BusinessProjectionBatchPayloadV1::V4(Arc::new(batch)))),
        "原NOE实际接纳完整反馈");
    state
}

// 原Actor同步持久发布与原Memory领取同时观察相同来源批；不新增生产端口。
struct GpuNoeReferencePort {
    memory:noe_projection::MemoryNoeProjectionPortV4,
    durable:noe_projection::SimV2NoeProjectionPortV4,
    normalization_reference:Rc<RefCell<Vec<serde_json::Value>>>,
    normalization_core:Option<execution_normalization::NormalizationCoreV4>,
}
impl BusinessProjectionPortV2 for GpuNoeReferencePort {
    fn try_accept(&mut self,_:Arc<BusinessProjectionBatchV2>) -> bool { false }
    fn try_accept_payload(&mut self,payload:Arc<BusinessProjectionBatchPayloadV1>) -> bool {
        if !self.durable.try_accept_payload(Arc::clone(&payload)) { return false; }
        let context=self.durable.store_for_resource_observation().context().clone();
        let core=self.normalization_core.get_or_insert_with(||
            execution_normalization::NormalizationCoreV4::new(context.clone()));
        self.normalization_reference.borrow_mut().extend(
            noe_projection::gpu_normalization_reference_v4(&context,core,payload.as_ref()));
        self.memory.accept_for_background(payload).expect("原MemoryNOE接纳同一实际批");
        true
    }
    fn durable_through(&self,run_id:&str) -> u64 { self.durable.durable_through(run_id) }
    fn close_receive_prefix(&mut self,at:i64) { self.memory.close_receive_prefix(at); }
    fn observe_public_time(&mut self,time:&crate::public_consumption::TrustedActorExecutionTimeV1) {
        self.memory.observe_public_time(time);
    }
}

#[cfg(feature="shared-execution")]
fn gpu_ledger_reservation_checkpoint(ledger:&ledger::LedgerWriterV1,stage:&str) -> serde_json::Value {
    let request=ledger.funds_read_v1().request(&ledger::LedgerRequestIdentityV1("gpu-reservation".into()))
        .map(|value|serde_json::json!({"status":value.reservation_state,"principal":value.principal_pusd,
            "fee":value.immediate_fee_pusd,"last_ledger_entry_seq":value.last_ledger_entry_seq}));
    let read=ledger.economic_read().unwrap();
    let trades=read.trades().map(|trade|serde_json::json!({"identity":trade.trade_identity,"fact":trade.fact,
        "economic_applied":trade.economic_applied,"success_applied":trade.success_applied,
        "net_fee":trade.actual_net_fee_pusd,"unpaired_contribution_shares":trade.unpaired_contribution_shares})).collect::<Vec<_>>();
    let rebate_days=read.rebate_days().map(|day|serde_json::json!({"kind":day.kind,
        "attribution_day_start_utc_ms":day.attribution_day_start_utc_ms,
        "contributions":day.contributions,"amounts":day.amounts})).collect::<Vec<_>>();
    let order_funds = ledger.funds_read_v1().orders().map(|order| serde_json::json!({
        "identity":order.order_identity,"principal":order.principal_encumbrance_pusd,
        "fee":order.immediate_fee_encumbrance_pusd,"last_ledger_entry_seq":order.last_ledger_entry_seq,
    })).collect::<Vec<_>>();
    let order_lifecycle = ledger.funds_read_v1().orders().filter_map(|order| {
        let funds = ledger.funds_read_v1();
        let life = funds.order_lifecycle(order.order_identity)?;
        Some(serde_json::json!({"identity":order.order_identity, "condition":life.condition_id,
            "open":life.open, "last_ledger_entry_seq":life.last_ledger_entry_seq}))
    }).collect::<Vec<_>>();
    serde_json::json!({"stage":stage,"projection":ledger.projection_v2().unwrap(),
        "cash":read.cash(),"request":request,"markets":read.market_totals().collect::<Vec<_>>(),"trades":trades,"rebate_days":rebate_days,"order_lifecycle":order_lifecycle,"order_funds":order_funds})
}

#[test]
#[ignore = "AutoDL D-0295 GPU精确源尾差与任意精度归属对照"]
fn gpu_finite_attribution_matches_original_exact_source_contract() {
    use contracts::economics::EconomicRational as R;
    use contracts::finite_attribution::*;
    let ratio=|n:i64,d:i64|R::ratio(n.into(),d.into());
    let mut splits=Vec::new();
    let huge=R::ratio((num_bigint::BigInt::from(1u8)<<180)+num_bigint::BigInt::from(3u8),num_bigint::BigInt::from(1_000_000_000_000_000u64));
    for (total,part,whole) in [
        (ratio(1,1),ratio(1,1),ratio(3,1)),
        (ratio(5,1_000_000_000_000),ratio(1,1),ratio(2,1)),
        (ratio(7,1_000_000_000_000),ratio(1,1),ratio(2,1)),
        (ratio(6,10_000_000_000_000),ratio(9,1),ratio(10,1)),
        (ratio(7,1_000_000_000_000_000),ratio(0,1),ratio(1,1)),
        (ratio(7,1_000_000_000_000_000),ratio(1,1),ratio(1,1)),
        (huge.clone(),ratio(77,1),ratio(211,1)),
    ] {
        let result=split_scale12_rne_v1(&total,&part,&whole).unwrap();
        splits.push(serde_json::json!({"total":total.public(),"part":part.public(),"whole":whole.public(),
            "allocated":result.allocated.public(),"remaining":result.remaining.public()}));
    }
    let mut allocations=Vec::new();
    for (total,capacities) in [
        (ratio(16,10_000_000_000_000),vec![ratio(10,10_000_000_000_000);3]),
        (ratio(9,10_000_000_000_000),vec![ratio(6,10_000_000_000_000);2]),
        (ratio(1,1),vec![ratio(0,1),ratio(2,1),ratio(0,1)]),
        (ratio(0,1),vec![ratio(0,1);2]),
        (huge.clone(),vec![huge.clone(),huge]),
    ] {
        let result=allocate_capacity_scale12_candidate_v1(&total,&capacities).unwrap();
        allocations.push(serde_json::json!({"total":total.public(),"capacities":capacities.iter().map(R::public).collect::<Vec<_>>(),
            "allocated":result.iter().map(R::public).collect::<Vec<_>>()}));
    }
    let mut child=std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(std::env::var("B_GPU_FINITE_VALIDATOR").unwrap()).stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(),&serde_json::json!({"splits":splits,"allocations":allocations})).unwrap();
    let output=child.wait_with_output().unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    eprintln!("gpu_finite_attribution={}",String::from_utf8(output.stdout).unwrap());
}
