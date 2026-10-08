// 原CPU请求登记前状态/处理时SDK/完整响应与NOE是独立oracle，不提供GPU编码terms。
#[cfg(feature="shared-execution")]
async fn pending_post_original_sources() -> Vec<serde_json::Value> {
    let mut cases=Vec::new();
    for name in ["fok_success","fok_incomplete","fok_fee_funds","mixed_reject_last",
        "mixed_accept_last","mixed_reject_first","processing_tick"] {
        let cash=if name=="fok_fee_funds" {"92"} else {"2000"};
        let saved=Rc::new(RefCell::new(None));let capture=Rc::clone(&saved);
        let normalization=Rc::new(RefCell::new(Vec::new()));let normalized=Rc::clone(&normalization);
        let policy=successful_policy_v2_for_tests();
        let run_id=format!("gpu-pending-{name}");
        let (_directory,mut actor)=active_market_actor_with_projection_and_cash(
            SimCallerRoleV1::ModelTrain,&run_id,policy.clone(),move |path,opened| {
                opened.projection_version=crate::runtime_v2::BusinessProjectionVersionV1::V4;
                let reference=contracts::execution_normalization::DurableSourceReferenceV1 {
                    source:contracts::execution_normalization::ImmutableSourceReferenceV1::Location(path.display().to_string())};
                let memory=noe_projection::MemoryNoeProjectionPortV4::new(path.parent().unwrap().join("noe-memory"),
                    opened.clone(),reference.clone(),reference.clone(),"gpu-pending-create".into(),"gpu-pending-source".into());
                *capture.borrow_mut()=Some(memory.observation());
                let durable=noe_projection::SimV2NoeProjectionPortV4::new(path.parent().unwrap().join("noe-durable"),
                    opened.clone(),reference.clone(),reference,"gpu-pending-create".into(),"gpu-pending-source".into());
                Box::new(GpuNoeReferencePort {memory,durable,normalization_reference:normalized,normalization_core:None})
            },cash).await;
        let observation=saved.borrow_mut().take().unwrap();
        let mut ledger=ledger::LedgerWriterV1::new_with_admission_v2(ledger::AccountAdmissionPolicyV2::DualCashNoMarketLimit);
        ledger.apply_noe_memory_v2(&observation.opening_record().unwrap()).unwrap();
        for record in observation.take_closed_records() {ledger.apply_noe_memory_v2(&record).unwrap();}
        let opening=gpu_ledger_reservation_checkpoint(&ledger,"before_request");
        let mut initial=decoded_maker_initial_events();
        let (price,quantity)=match name {
            "fok_success"=>("0.5","1000.001"),"fok_incomplete"=>("0.5","10"),
            "fok_fee_funds"=>("0.9","100"),"mixed_reject_last"=>("0.2","1.000001"),
            "mixed_accept_last"=>("0.2","10"),_ =>("0.5","10")};
        let ExecutionMarketEventPayloadV3::InitialL2Snapshot(book)=&mut initial[0].payload else {unreachable!()};
        book.asks=vec![contracts::PriceLevelV1 {price_pusd_per_share:ExactDecimal(price.into()),quantity_shares:ExactDecimal(quantity.into())}];
        if name=="processing_tick" {book.initial_tick_size_pusd_per_share=ExactDecimal("0.001".into());}
        initial[0].event_id=data_pipeline_core::derive_execution_market_event_v3_id(&initial[0]).unwrap();
        initial[0]=data_pipeline_core::decode_execution_market_event_v3(&data_pipeline_core::encode_execution_market_event_v3(&initial[0]).unwrap()).unwrap();
        let condition=initial[0].market.condition_id.clone();let token=TokenId("1".into());
        let (state,_)=reduce_market_event_slice_v2(actor.state.take().unwrap(),MarketEventPathV1::HiddenPlatform,1100,
            &initial,actor._rule_snapshot.snapshot(),policy.as_ref()).unwrap_or_else(|_|panic!("原初始化 {name}"));
        let fok=|spend:&str,worst:&str|SimBuyOrderKindV1::FokMarket {
            spend_pusd:spend.parse().unwrap(),worst_price_pusd_per_share:worst.parse().unwrap()};
        let gtc=|price:&str,size:&str|SimBuyOrderKindV1::GtcLimit {
            limit_price_pusd_per_share:price.parse().unwrap(),size_shares:size.parse().unwrap(),post_only:false};
        let kinds=match name {
            "fok_success"=>vec![(0,fok("500.0005","0.5"))],
            "fok_incomplete"=>vec![(0,fok("6","0.6"))],
            "fok_fee_funds"=>vec![(0,fok("90","0.9"))],
            "mixed_reject_last"=>vec![(7,gtc("0.3","5")),(1,fok("3.6","0.6"))],
            "mixed_accept_last"=>vec![(7,gtc("0.3","5")),(1,fok("1","0.6"))],
            "mixed_reject_first"=>vec![(7,gtc("0.5","1")),(1,fok("2","0.5"))],
            _=>vec![(0,gtc("0.123","10"))]};
        let posts=kinds.into_iter().enumerate().map(|(i,(ordinal,kind))|PostOrderBatchItemV2 {
            batch_item_ordinal:ordinal,item:post_item(&format!("pending-{name}-{i}"),&condition,&token,kind)}).collect::<Vec<_>>();
        let request_id=format!("pending-post-{name}");
        let reserve_intents=posts.iter().map(|post| {
            let identity=ledger::LedgerSubmissionItemIdentityV1 {global_request_id:request_id.clone(),batch_item_ordinal:post.batch_item_ordinal};
            let (principal,fee)=match &post.item.order.kind {
                SimBuyOrderKindV1::GtcLimit {limit_price_pusd_per_share,size_shares,post_only}=> {
                    let tick=match &initial[0].payload {ExecutionMarketEventPayloadV3::InitialL2Snapshot(book)=>&book.initial_tick_size_pusd_per_share,_=>unreachable!()};
                    let tick_decimal:rust_decimal::Decimal=tick.0.parse().unwrap();
                    let n=(*size_shares*rust_decimal::Decimal::from(100)).to_string().parse::<num_bigint::BigUint>().unwrap();
                    let index=(*limit_price_pusd_per_share/tick_decimal).to_string().parse::<u16>().unwrap();
                    let cost=actor._rule_snapshot.limit_buy_cost_v1(&n,index,tick,*post_only).unwrap();
                    (crate::matching::decimal_from_micro_units_v1(cost.principal_micro.to_string().parse().unwrap()).unwrap(),
                     rust_decimal::Decimal::from_str_exact(&cost.fee_1e5_units.to_string()).unwrap()/rust_decimal::Decimal::from(100_000))
                },
                SimBuyOrderKindV1::FokMarket {spend_pusd,..}=>(*spend_pusd,rust_decimal::Decimal::ZERO),
                _=>unreachable!(),
            };
            ledger::ReserveIntentV1 {request_identity:ledger::LedgerRequestIdentityV1(serde_json::to_string(&identity).unwrap()),
                condition_id:condition.clone(),requested_principal_pusd:ExactDecimal(principal.to_string()),immediate_fee_reserve_pusd:ExactDecimal(fee.to_string())}
        }).collect::<Vec<_>>();
        ledger.try_reserve_action_v2(&reserve_intents).unwrap();
        for (post,reserve) in posts.iter().zip(&reserve_intents) {
            let identity=ledger::LedgerSubmissionItemIdentityV1 {global_request_id:request_id.clone(),batch_item_ordinal:post.batch_item_ordinal};
            let event=ledger::LedgerLocalEventIdentityV1(serde_json::to_string(&("submission-started",&identity)).unwrap());
            ledger.record_submission_started(&event,&reserve.request_identity,&identity).unwrap();
        }
        let sent_ledger=gpu_ledger_reservation_checkpoint(&ledger,"actual_send_facts");
        let request=BusinessRequestV2::PostOrders(PostOrdersRequestV2 {
            context:RequestContextV2 {global_request_id:request_id.clone(),caller_correlation_id:None},items:posts.clone()});
        let (mut state,receipt)=register_request_candidate(&actor.opened,actor.scenario.initial_market_directory(),state,
            &request_cutoff(1200),&request,11,actor._latency_profile.as_ref(),actor._rule_snapshot.snapshot());
        let pending=state.session.as_ref().unwrap().mutation_requests.pending_actions.keys().copied().map(|k|
            serde_json::json!({"at":k.effective_processing_at_utc_ms,"input_sequence":k.input_sequence,"ordinal":k.batch_item_ordinal})).collect::<Vec<_>>();
        let first_at=pending[0]["at"].as_i64().unwrap();
        let mut changed=Vec::new();
        if name=="fok_fee_funds" {
            changed.push(decoded_maker_level_event(BookSide::Ask,"0.9","100","0",first_at-1,first_at-1,10));
            changed.push(decoded_maker_level_event(BookSide::Ask,"0.1","0","900",first_at-1,first_at-1,11));
        }
        if name=="processing_tick" {
            let event=crate::task21_tests::sealed_event(
                ExecutionMarketEventPayloadV3::TickSizeChange(contracts::TickSizeChangeV2 {
                    asset:crate::task21_tests::asset("1",0,"Up"),old_tick_size_pusd_per_share:Some(ExactDecimal("0.001".into())),
                    new_tick_size_pusd_per_share:ExactDecimal("0.01".into())}),
                crate::task21_tests::orderbook_evidence(first_at-1,first_at-1,12),crate::task21_tests::fresh_quality());
            changed.push(data_pipeline_core::decode_execution_market_event_v3(&data_pipeline_core::encode_execution_market_event_v3(&event).unwrap()).unwrap());
        }
        if !changed.is_empty() {
            let (next,_)=reduce_market_event_slice_v2(state,MarketEventPathV1::HiddenPlatform,first_at-1,
                &changed,actor._rule_snapshot.snapshot(),policy.as_ref()).unwrap_or_else(|_|panic!("原处理前行情 {name}"));state=next;
        }
        let mut effects=Vec::new();let mut batches=Vec::new();let mut drive=Vec::new();
        let first_private=state.session.as_ref().unwrap().next_private_feedback_sequence;
        let first_batch=state.session.as_ref().unwrap().next_projection_batch_sequence;
        for key in &pending {
            let at=key["at"].as_i64().unwrap();
            while let Some(finality_at)=state.session.as_ref().unwrap().execution_state.next_scheduled_finality_at_utc_ms() {
                if finality_at>at {break;}
                let (next,frame)=advance_finality_to_candidate(state,finality_at,actor._rule_snapshot.snapshot(),policy.as_ref());
                drive.push(serde_json::json!({"phase":"finality","at":finality_at}));
                state=gpu_accept_projection_reference(&mut actor,next,BusinessReducerOutputV2::LogicalDrive(
                    LogicalDriveOutputV2 {lifecycle:frame.into_lifecycle_feedback().unwrap(),..Default::default()}),&mut batches);
            }
            drive.push(serde_json::json!({"phase":"post","at":at}));
            let (next,effect)=advance_mutation_candidate_with_outputs(&actor.opened,actor.scenario.initial_market_directory(),state,
                at,actor._rule_snapshot.snapshot(),policy.as_ref());state=next;
            let effect=effect.unwrap();let MutationActionBusinessEffectV3::Post(post)=&effect.business else {panic!("原Post处理");};
            let outcome=match &post.outcome {
                PostOrderItemOutcomeV2::Accepted(body)=>serde_json::json!({"Accepted":body}),
                PostOrderItemOutcomeV2::RejectedNoEffect(reason)=>serde_json::json!({"RejectedNoEffect":reason}),
                PostOrderItemOutcomeV2::EvidenceUnknownNoEffect(reason)=>serde_json::json!({"EvidenceUnknownNoEffect":format!("{reason:?}")}),
            };
            let checkpoint=state.session.as_ref().unwrap().execution_state.gpu_order_checkpoint_for_tests("sim-1");
            effects.push(serde_json::json!({"key":key,"outcome":outcome,"checkpoint":checkpoint}));
            if effect.lifecycle.is_empty() && effect.completed_response.is_none() {
                // 原首腿拒绝尚未封口没有公开反馈；它仍保留自己的实际Post结果。
                let (next,_,batch)=finalize_projection_batch_v2(&actor.opened.run_id,state,
                    BusinessReducerOutputV2::MutationAction(Some(effect)),actor._latency_profile.as_ref()).unwrap();
                assert!(batch.is_none(),"原空draft不分配反馈批 {name}");state=next;
            } else {
                state=gpu_accept_projection_reference(&mut actor,state,BusinessReducerOutputV2::MutationAction(Some(effect)),&mut batches);
            }
        }
        while let Some(at)=state.session.as_ref().unwrap().execution_state.next_scheduled_finality_at_utc_ms() {
            let (next,frame)=advance_finality_to_candidate(state,at,actor._rule_snapshot.snapshot(),policy.as_ref());
            drive.push(serde_json::json!({"phase":"finality","at":at}));
            state=gpu_accept_projection_reference(&mut actor,next,BusinessReducerOutputV2::LogicalDrive(
                LogicalDriveOutputV2 {lifecycle:frame.into_lifecycle_feedback().unwrap(),..Default::default()}),&mut batches);
        }
        let closed_at=drive.last().unwrap()["at"].as_i64().unwrap()+10_000;
        actor.projection_port.close_receive_prefix(closed_at);
        actor.projection_port.observe_public_time(&crate::public_consumption::TrustedActorExecutionTimeV1(crate::public_consumption::ActorTimeRecordV1 {
            run_id:run_id.clone(),drive_position:1,logical_time_utc_ms:closed_at}));
        let records=observation.take_closed_records();
        let original_records=records.iter().map(|record|serde_json::json!({"record":record.record(),"position":record.position(),"visibility":record.visibility()})).collect::<Vec<_>>();
        let mut waiting=records;
        while !waiting.is_empty() {
            let count=waiting.len();let mut next=Vec::new();
            for record in waiting {
                let applied=ledger.apply_noe_memory_v2(&record).unwrap();
                if matches!(applied,ledger::LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation|ledger::LedgerMemoryApplyOutcomeV1::Applied(ledger::LedgerApplyOutcomeV1::MissingPlatformPredecessor {..})) {next.push(record);}
            }
            assert!(next.len()<count,"原公开NOE前驱闭合 {name}");waiting=next;
        }
        let received_ledger=gpu_ledger_reservation_checkpoint(&ledger,"after_received_actual_noe");
        let rules=actor._rule_snapshot.snapshot();
        let source=serde_json::json!({"name":name,"opening_ledger":opening,"cash":cash,"initial":initial,"changed":changed,
            "posts":posts,"reserve_intents":reserve_intents,"sent_ledger":sent_ledger,"received_ledger":received_ledger,
            "records":original_records,"closed_at":closed_at,"request_id":request_id,"sent_at":1200,"input_sequence":11,"pending":pending,
            "receipt":{"arrived_at":receipt.platform_arrived_at_utc_ms,"count":receipt.scheduled_action_count},
            "effects":effects,"drive":drive,"batches":batches,"first_private":first_private,"first_batch":first_batch,
            "market_end":initial[0].market.window.end_at_utc_ms,
            "maker_rebate_rate":actor._rule_snapshot.snapshot().project_account_rules.rebate_settlement.value.fixed_maker_rebate_rate.value().to_string(),"rule":actor._rule_snapshot.public_rule_context(initial[0].market.clone()),
            "profile_sha256":actor._latency_profile.file_sha256(),"session_seed":state.session.as_ref().unwrap().session_seed.0,
            "finality_path":policy.normal_path(),"taker_rebate_rate":rules.project_account_rules.rebate_settlement.value.fixed_taker_rebate_rate.value().to_string(),
            "next_private":state.session.as_ref().unwrap().next_private_feedback_sequence,
            "next_batch":state.session.as_ref().unwrap().next_projection_batch_sequence});
        cases.push(source);
    }
    cases
}

#[cfg(feature="shared-execution")]
#[tokio::test(flavor="current_thread")]
async fn pending_fok_and_mixed_post_preserve_original_results() {
    let cases=pending_post_original_sources().await;
    assert_eq!(cases.len(),7);
    if let Ok(directory)=std::env::var("B_GPU_ORDER_ARTIFACT_DIR") {
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(std::path::Path::new(&directory).join("original-pending-post-sources.json"),serde_json::to_vec_pretty(&cases).unwrap()).unwrap();
    }
    for case in &cases {
        let outcomes=case["effects"].as_array().unwrap().iter().map(|e|&e["outcome"]).collect::<Vec<_>>();
        let accepted=outcomes.iter().filter(|o|o.get("Accepted").is_some()).count();
        match case["name"].as_str().unwrap() {
            "fok_success"=>assert_eq!(accepted,1),
            "fok_incomplete"=>assert_eq!(outcomes[0]["RejectedNoEffect"],"FokNotFullyFilled"),
            "fok_fee_funds"=>assert_eq!(outcomes[0]["RejectedNoEffect"],"InsufficientAvailablePusd"),
            "mixed_reject_last"|"mixed_reject_first"=>assert_eq!(accepted,1),
            "mixed_accept_last"=>assert_eq!(accepted,2),
            "processing_tick"=>assert_eq!(outcomes[0]["RejectedNoEffect"],"OrderEncodingRejected"),
            _=>unreachable!(),
        }
        assert_eq!(case["effects"].as_array().unwrap().len(),case["posts"].as_array().unwrap().len());
    }
}

#[cfg(feature="shared-execution")]
#[tokio::test(flavor="current_thread")]
#[ignore="AutoDL原pending FOK/Mixed处理时SDK、完整Post及NOE/Ledger设备对照"]
async fn gpu_pending_fok_and_mixed_post_match_original_submission() {
    let cases=pending_post_original_sources().await;
    let artifact=std::path::PathBuf::from(std::env::var("B_GPU_ORDER_ARTIFACT_DIR").unwrap());
    std::fs::create_dir_all(&artifact).unwrap();
    std::fs::write(artifact.join("original-pending-post-sources.json"),serde_json::to_vec_pretty(&cases).unwrap()).unwrap();
    let mut child=std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(std::env::var("B_GPU_PENDING_POST_VALIDATOR").unwrap()).stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(),&cases).unwrap();
    let output=child.wait_with_output().unwrap();
    std::fs::write(artifact.join("pending-post-report.json"),&output.stdout).unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    eprintln!("pending_post={}",String::from_utf8_lossy(&output.stdout));
}

#[cfg(feature="shared-execution")]
#[tokio::test(flavor="current_thread")]
#[ignore="locked GPU; request market governs pending asset membership and rejection precedence"]
async fn gpu_pending_post_uses_request_market_for_asset_admission() {
    let mut cases=Vec::new();
    for (name,token) in [("unknown_token","999"),("cross_market_token","1"),("legal_market_token","301")] {
        let (_directory,mut actor)=active_market_actor(SimCallerRoleV1::ModelTrain,&format!("market-binding-{name}")).await;
        let first=actor.scenario.initial_market_directory().markets().next().unwrap().clone();
        let second=market_with_window('e',"456","301","302",1_000,3_000);
        actor.enroll_market(MarketEnrollmentRequestV1 {enrollment_request_id:format!("enroll-{name}"),market:second.clone()}).unwrap();
        let mut initial=decoded_initial_events_for_market_with_quantity(&first,1_100,1_110,1,"5");
        initial.extend(decoded_initial_events_for_market_with_quantity(&second,1_100,1_110,3,"5"));
        let (state,_)=drive_business_input_at_v2(actor.state.take().unwrap(),BusinessReducerInputV2::ClosedMarketBucket {
            bucket:AcceptedClosedMarketBucketV2 {reference:crate::runtime_v2::DurableMarketBucketReferenceV2 {
                path:MarketEventPathV1::HiddenPlatform,bucket_at_utc_ms:1_100,start_index_position:0,next_index_position:4,
                closure:crate::market::MillisecondBucketClosureV1::NextEventAt(4_000)},events:Arc::from(initial.clone())},
            rules:actor._rule_snapshot.snapshot(),finality_policy:actor._finality_policy.as_ref()}).unwrap();
        let (state,_)=drive_business_input_at_v2(state,BusinessReducerInputV2::PlatformPrefixAdvance {
            advance:PlatformPrefixAdvanceV2 {path:MarketEventPathV1::HiddenPlatform,closed_through_utc_ms:2_000,next_index_position:4},
            rules:actor._rule_snapshot.snapshot(),finality_policy:actor._finality_policy.as_ref()}).unwrap();
        let before=state.session.as_ref().unwrap().execution_state.gpu_order_checkpoint_for_tests("sim-1");
        let item=post_item(&format!("item-{name}"),&second.market().condition_id,&TokenId(token.into()),SimBuyOrderKindV1::FokMarket {
            spend_pusd:"3".parse().unwrap(),worst_price_pusd_per_share:"0.6".parse().unwrap()});
        let id=format!("request-{name}");
        let request=BusinessRequestV2::PostOrders(PostOrdersRequestV2 {context:RequestContextV2 {global_request_id:id.clone(),caller_correlation_id:None},
            items:vec![PostOrderBatchItemV2 {batch_item_ordinal:0,item:item.clone()}]});
        let (state,receipt)=register_request_candidate(&actor.opened,actor.scenario.initial_market_directory(),state,&request_cutoff(2_100),
            &request,11,actor._latency_profile.as_ref(),actor._rule_snapshot.snapshot());
        let processing=state.session.as_ref().unwrap().mutation_requests.pending_actions.keys().next().unwrap().effective_processing_at_utc_ms;
        assert!(processing<3_000);
        let seed=state.session.as_ref().unwrap().session_seed.0.clone();
        let (next,effect)=advance_mutation_candidate_with_outputs(&actor.opened,actor.scenario.initial_market_directory(),state,processing,
            actor._rule_snapshot.snapshot(),actor._finality_policy.as_ref());
        let MutationActionBusinessEffectV3::Post(post)=effect.unwrap().business else {panic!("原Post");};
        let outcome=match &post.outcome {
            PostOrderItemOutcomeV2::Accepted(body)=>serde_json::json!({"Accepted":body}),
            PostOrderItemOutcomeV2::RejectedNoEffect(reason)=>serde_json::json!({"RejectedNoEffect":reason}),
            other=>panic!("原准入结果 {other:?}"),
        };
        let after=next.session.as_ref().unwrap().execution_state.gpu_order_checkpoint_for_tests("sim-1");
        if name!="legal_market_token" {assert_eq!(outcome["RejectedNoEffect"],"AssetNotInMarket");assert_eq!(before,after);}
        else {assert!(outcome.get("Accepted").is_some());}
        cases.push(serde_json::json!({"name":name,"item":item,"initial":initial,"markets":[first,second],"before":before,"after":after,
            "outcome":outcome,"request_id":id,"sent_at":2_100,"processing":processing,"input_sequence":11,"session_seed":seed,
            "profile_sha256":actor._latency_profile.file_sha256(),"rule":actor._rule_snapshot.public_rule_context(initial[2].market.clone()),
            "finality_path":actor._finality_policy.normal_path(),"arrived_at":receipt.platform_arrived_at_utc_ms}));
    }
    let path=std::path::PathBuf::from(std::env::var("B_GPU_MARKET_ADMISSION_DIR").unwrap());std::fs::create_dir_all(&path).unwrap();
    std::fs::write(path.join("original-request-market-admission.json"),serde_json::to_vec_pretty(&cases).unwrap()).unwrap();
    let mut child=std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap()).arg(std::env::var("B_GPU_MARKET_ADMISSION_VALIDATOR").unwrap())
        .stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(),&cases).unwrap();let output=child.wait_with_output().unwrap();
    std::fs::write(path.join("request-market-admission-report.json"),&output.stdout).unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));eprintln!("request_market_admission={}",String::from_utf8_lossy(&output.stdout));
}
