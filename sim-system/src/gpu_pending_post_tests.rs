// 原CPU请求登记前状态/处理时SDK/完整响应与NOE是独立oracle，不提供GPU编码terms。
#[cfg(feature="shared-execution")]
async fn pending_post_original_sources() -> Vec<serde_json::Value> {
    pending_post_original_sources_for(&["fok_success","fok_incomplete","fok_fee_funds","mixed_reject_last",
        "mixed_accept_last","mixed_reject_first","processing_tick"],false).await
}

#[cfg(feature="shared-execution")]
async fn pending_post_original_sources_for(names:&[&str],include_cancels:bool) -> Vec<serde_json::Value> {
    pending_post_original_sources_at(names,include_cancels,None).await
}

#[cfg(feature="shared-execution")]
async fn pending_post_original_sources_at(names:&[&str],include_cancels:bool,receive_checkpoint:Option<i64>) -> Vec<serde_json::Value> {
    pending_post_original_sources_at_continuation(names,include_cancels,receive_checkpoint,None).await
}

#[cfg(feature="shared-execution")]
async fn pending_post_original_sources_at_continuation(names:&[&str],include_cancels:bool,receive_checkpoint:Option<i64>,
    continuation:Option<&serde_json::Value>) -> Vec<serde_json::Value> {
    let mut cases=Vec::new();
    for &name in names {
        let cash=if name=="fok_fee_funds" {"92"} else {"2000"};
        let saved=Rc::new(RefCell::new(None));let capture=Rc::clone(&saved);
        let normalization=Rc::new(RefCell::new(Vec::new()));let normalized=Rc::clone(&normalization);
        let policy=successful_policy_v2_for_tests();
        let run_id=format!("gpu-pending-{name}");
        let scenario=if include_cancels {sealed_scenario_for_markets(vec![market_with_window('1',"123","1","2",1000,300000)])} else {sealed_scenario()};
        let (_directory,mut actor)=active_market_actor_with_projection_cash_and_scenario(
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
            },cash,scenario).await;
        if include_cancels {
            actor.enroll_market(MarketEnrollmentRequestV1 {enrollment_request_id:"cancel-other-market".into(),
                market:market_with_window('e',"456","301","302",1_000,300_000)}).unwrap();
        }
        let observation=saved.borrow_mut().take().unwrap();
        let mut ledger=ledger::LedgerWriterV1::new_with_admission_v2(ledger::AccountAdmissionPolicyV2::DualCashNoMarketLimit);
        ledger.apply_noe_memory_v2(&observation.opening_record().unwrap()).unwrap();
        for record in observation.take_closed_records() {ledger.apply_noe_memory_v2(&record).unwrap();}
        let opening=gpu_ledger_reservation_checkpoint(&ledger,"before_request");
        let mut initial=decoded_maker_initial_events();
        if include_cancels {
            for event in &mut initial {
                event.market=actor.scenario.initial_market_directory().markets().next().unwrap().market().clone();
                event.event_id=data_pipeline_core::derive_execution_market_event_v3_id(event).unwrap();
                *event=data_pipeline_core::decode_execution_market_event_v3(&data_pipeline_core::encode_execution_market_event_v3(event).unwrap()).unwrap();
            }
        }
        if include_cancels {
            let market=market_with_window('e',"456","301","302",1_000,300_000);
            initial.extend(decoded_initial_events_for_market_with_quantity(&market,1100,1110,3,"10"));
        }
        let (price,quantity)=match name {
            "fok_success"=>("0.5","1000.001"),"fok_incomplete"=>("0.5","10"),
            "fok_fee_funds"=>("0.9","100"),"mixed_reject_last"=>("0.2","1.000001"),
            "mixed_accept_last"=>("0.2","10"),_ =>("0.5","10")};
        let ExecutionMarketEventPayloadV3::InitialL2Snapshot(book)=&mut initial[0].payload else {unreachable!()};
        book.asks=vec![contracts::PriceLevelV1 {price_pusd_per_share:ExactDecimal(price.into()),quantity_shares:ExactDecimal(quantity.into())}];
        if name=="processing_tick" {book.initial_tick_size_pusd_per_share=ExactDecimal("0.001".into());}
        initial[0].event_id=data_pipeline_core::derive_execution_market_event_v3_id(&initial[0]).unwrap();
        initial[0]=data_pipeline_core::decode_execution_market_event_v3(&data_pipeline_core::encode_execution_market_event_v3(&initial[0]).unwrap()).unwrap();
        if include_cancels {
            let ExecutionMarketEventPayloadV3::InitialL2Snapshot(book)=&mut initial[1].payload else {unreachable!()};
            book.asks=vec![contracts::PriceLevelV1 {price_pusd_per_share:ExactDecimal("0.2".into()),quantity_shares:ExactDecimal("10".into())}];
            initial[1].event_id=data_pipeline_core::derive_execution_market_event_v3_id(&initial[1]).unwrap();
            initial[1]=data_pipeline_core::decode_execution_market_event_v3(&data_pipeline_core::encode_execution_market_event_v3(&initial[1]).unwrap()).unwrap();
        }
        let condition=initial[0].market.condition_id.clone();let token=TokenId("1".into());let other_token=TokenId("2".into());
        let state=actor.state.take().unwrap();
        let state=if name=="preloading" {state} else {
            reduce_market_event_slice_v2(state,MarketEventPathV1::HiddenPlatform,1100,
                &initial,actor._rule_snapshot.snapshot(),policy.as_ref()).unwrap_or_else(|_|panic!("原初始化 {name}")).0
        };
        let fok=|spend:&str,worst:&str|SimBuyOrderKindV1::FokMarket {
            spend_pusd:spend.parse().unwrap(),worst_price_pusd_per_share:worst.parse().unwrap()};
        let gtc=|price:&str,size:&str|SimBuyOrderKindV1::GtcLimit {
            limit_price_pusd_per_share:price.parse().unwrap(),size_shares:size.parse().unwrap(),post_only:false};
        let kinds=match name {
            "fok_success"=>vec![(0,fok("500.0005","0.5"))],
            "fok_incomplete"=>vec![(0,fok("6","0.6"))],
            "fok_fee_funds"=>vec![(0,fok("90","0.9"))],
            "mixed_reject_last"=>vec![(7,gtc("0.3","5")),(1,fok("3.6","0.6"))],
            "mixed_accept_last"=>vec![(7,gtc(if include_cancels {"0.1"} else {"0.3"},"5")),(1,fok("1","0.6"))],
            "mixed_reject_first"=>vec![(7,gtc("0.5","1")),(1,fok("2","0.5"))],
            "preloading"=>vec![(0,gtc("0.12","10"))],
            _=>vec![(0,gtc("0.123","10"))]};
        let posts=kinds.into_iter().enumerate().map(|(i,(ordinal,kind))|PostOrderBatchItemV2 {
            batch_item_ordinal:if include_cancels {i as u16} else {ordinal},
            item:post_item(&format!("pending-{name}-{i}"),&condition,
                if include_cancels && i==1 {&other_token} else {&token},kind)}).collect::<Vec<_>>();
        let request_id=format!("pending-post-{name}");
        let reserve_intents=posts.iter().map(|post| {
            let identity=ledger::LedgerSubmissionItemIdentityV1 {global_request_id:request_id.clone(),batch_item_ordinal:post.batch_item_ordinal};
            let (principal,fee)=match &post.item.order.kind {
                SimBuyOrderKindV1::GtcLimit {limit_price_pusd_per_share,size_shares,post_only}=> {
                    let tick=match &initial[0].payload {ExecutionMarketEventPayloadV3::InitialL2Snapshot(book)=>&book.initial_tick_size_pusd_per_share,_=>unreachable!()};
                    let tick_decimal:rust_decimal::Decimal=tick.0.parse().unwrap();
                    let n=(*size_shares*rust_decimal::Decimal::from(100)).normalize().to_string().parse::<num_bigint::BigUint>().unwrap();
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
        let final_closed_at=continuation.map(|items|items["requests"][0]["sent_at"].as_i64().unwrap())
            .unwrap_or_else(||drive.last().unwrap()["at"].as_i64().unwrap()+10_000);
        let closed_at=receive_checkpoint.unwrap_or(final_closed_at);
        actor.projection_port.close_receive_prefix(closed_at);
        actor.projection_port.observe_public_time(&crate::public_consumption::TrustedActorExecutionTimeV1(crate::public_consumption::ActorTimeRecordV1 {
            run_id:run_id.clone(),drive_position:1,logical_time_utc_ms:closed_at}));
        let records=observation.take_closed_records();
        let mut original_records=records.iter().map(|record|serde_json::json!({"record":record.record(),"position":record.position(),"visibility":record.visibility()})).collect::<Vec<_>>();
        let mut waiting=records;
        while !waiting.is_empty() {
            let count=waiting.len();let mut next=Vec::new();
            for record in waiting {
                let applied=ledger.apply_noe_memory_v2(&record).unwrap();
                if matches!(applied,ledger::LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation|ledger::LedgerMemoryApplyOutcomeV1::Applied(ledger::LedgerApplyOutcomeV1::MissingPlatformPredecessor {..})) {next.push(record);}
            }
            assert!(next.len()<count,"原公开NOE前驱闭合 {name}");waiting=next;
        }
        let bounded_pre_send_ledger=receive_checkpoint.map(|_|gpu_ledger_reservation_checkpoint(&ledger,"actual_received_before_execute"));
        if receive_checkpoint.is_some() {
            actor.projection_port.close_receive_prefix(final_closed_at);
            actor.projection_port.observe_public_time(&crate::public_consumption::TrustedActorExecutionTimeV1(crate::public_consumption::ActorTimeRecordV1 {
                run_id:run_id.clone(),drive_position:2,logical_time_utc_ms:final_closed_at}));
            let records=observation.take_closed_records();
            original_records.extend(records.iter().map(|r|serde_json::json!({"record":r.record(),"position":r.position(),"visibility":r.visibility()})));
            let mut waiting=records;
            while !waiting.is_empty() {
                let count=waiting.len();let mut next=Vec::new();
                for record in waiting {
                    let applied=ledger.apply_noe_memory_v2(&record).unwrap();
                    if matches!(applied,ledger::LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation|ledger::LedgerMemoryApplyOutcomeV1::Applied(ledger::LedgerApplyOutcomeV1::MissingPlatformPredecessor {..})) {next.push(record);}
                }
                assert!(next.len()<count,"原后继公开NOE前驱闭合 {name}");waiting=next;
            }
        }
        let received_ledger=gpu_ledger_reservation_checkpoint(&ledger,"after_received_actual_noe");
        let rules=actor._rule_snapshot.snapshot();
        let mut source=serde_json::json!({"name":name,"preloaded":name!="preloading","opening_ledger":opening,"cash":cash,"initial":initial,"changed":changed,
            "run_id":actor.opened.run_id,"session_id":state.session.as_ref().unwrap().opening_fact.session_id,
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
        if let Some(checkpoint)=bounded_pre_send_ledger {source["bounded_pre_send_ledger"]=checkpoint;}
        if include_cancels {
            source["cancellation"]=pending_cancel_original_extension(&mut actor,state,&observation,&mut ledger,closed_at);
        } else if let Some(requests)=continuation {
            source["continuation"]=pending_continuous_original_extension(&mut actor,state,&observation,&mut ledger,&initial,requests);
        }
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
#[ignore="locked GPU; original Preloading unknown request completes without creating a price queue"]
async fn gpu_pending_preloading_preserves_unknown_no_effect() {
    let cases=pending_post_original_sources_for(&["preloading"],false).await;
    assert!(cases[0]["effects"][0]["outcome"].get("EvidenceUnknownNoEffect").is_some());
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
    eprintln!("pending_preloading={}",String::from_utf8_lossy(&output.stdout));
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

// 本来源只有原GTC/FOK及未结束市场，不装造GTD/FAK/结算义务。
#[cfg(feature="shared-execution")]
async fn pending_actor_send_original_source() -> serde_json::Value {
    pending_actor_send_original_source_with_history(false,false).await
}

#[cfg(feature="shared-execution")]
async fn pending_actor_send_original_source_with_history(nonzero_history:bool,two_markets:bool) -> serde_json::Value {
    let mut sources=pending_post_original_sources_at(&["fok_success"],false,Some(1410)).await;
    let source=&mut sources[0];
    let mut initial:Vec<ExecutionMarketEventV3>=serde_json::from_value(source["initial"].clone()).unwrap();
    // 两条正式原消息：公开领取界=1414；下一hidden桶恰为1414，仅同刻项增加原ask数量。
    let offset=if two_markets {299000}else{0};
    let mut events=vec![decoded_maker_level_event(BookSide::Ask,"0.5","1000.001","1000.001",1413+offset,1413+offset,10),
        decoded_maker_level_event(BookSide::Ask,"0.5","1000.001","1000.002",1414+offset,1414+offset,11)];
    if nonzero_history {
        for (sequence,at,bid) in [(20,1250,"0.4"),(21,1350,"0.42"),(22,1450,"0.44")] {
            let mut event=crate::task21_tests::sealed_event(
                ExecutionMarketEventPayloadV3::BestBidAsk(contracts::BestBidAskV2 {
                    asset:crate::task21_tests::asset("1",0,"Up"),
                    best_bid_pusd_per_share:Some(ExactDecimal(bid.into())),
                    best_ask_pusd_per_share:Some(ExactDecimal("0.5".into()))}),
                crate::task21_tests::orderbook_evidence(at+offset,at+offset,sequence),crate::task21_tests::fresh_quality());
            event.market=initial[0].market.clone();
            event.event_id=data_pipeline_core::derive_execution_market_event_v3_id(&event).unwrap();
            events.push(data_pipeline_core::decode_execution_market_event_v3(&data_pipeline_core::encode_execution_market_event_v3(&event).unwrap()).unwrap());
        }
        events.sort_by_key(|event|event.time.source_event_at_utc_ms);
        // row()实际collector admission=received+1；仅有限源的独立history期望。
        source["nonzero_history_samples"]=serde_json::json!([["0.45",1251],["0.46",1351],["0.47",1451]]);
    }
    if two_markets {
        // 本有限材料采用相邻5min窗口；原FOK经济来源/同账户仍由上面的真实接口给出。
        for event in &mut initial {
            event.market.window.end_at_utc_ms=301000;
            event.event_id=data_pipeline_core::derive_execution_market_event_v3_id(event).unwrap();
        }
        for event in &mut events {
            event.market=initial[0].market.clone();
            event.event_id=data_pipeline_core::derive_execution_market_event_v3_id(event).unwrap();
        }
        let mut next=initial.clone();
        for (index,event) in next.iter_mut().enumerate() {
            event.market.condition_id=ConditionId(format!("0x{}","2".repeat(64)));
            event.market.clob_market_id=ClobMarketId("124".into());
            event.market.window.start_at_utc_ms=301000;event.market.window.end_at_utc_ms=601000;
            let ExecutionMarketEventPayloadV3::InitialL2Snapshot(book)=&mut event.payload else {unreachable!()};
            book.asset.token_id=TokenId((index+3).to_string());
            book.bids[0].price_pusd_per_share=ExactDecimal("0.2".into());
            book.bids[0].quantity_shares=ExactDecimal("4".into());
            event.event_id=data_pipeline_core::derive_execution_market_event_v3_id(event).unwrap();
        }
        for (sequence,at,bid) in [(30,1250,"0.2"),(31,1350,"0.24"),(32,1450,"0.28")] {
            let mut event=crate::task21_tests::sealed_event(ExecutionMarketEventPayloadV3::BestBidAsk(contracts::BestBidAskV2 {
                asset:crate::task21_tests::asset("3",0,"Up"),best_bid_pusd_per_share:Some(ExactDecimal(bid.into())),
                best_ask_pusd_per_share:Some(ExactDecimal("0.5".into()))}),
                crate::task21_tests::orderbook_evidence(at+299000,at+299000,sequence),crate::task21_tests::fresh_quality());
            event.market=next[0].market.clone();event.event_id=data_pipeline_core::derive_execution_market_event_v3_id(&event).unwrap();
            events.push(data_pipeline_core::decode_execution_market_event_v3(&data_pipeline_core::encode_execution_market_event_v3(&event).unwrap()).unwrap());
        }
        for (sequence,at,token,bid) in [(40,1990,"1","0.46"),(41,1990,"3","0.3"),
            (42,1991,"3","0.32"),(43,1992,"3","0.34"),(44,1993,"3","0.36")] {
            let mut event=crate::task21_tests::sealed_event(ExecutionMarketEventPayloadV3::BestBidAsk(contracts::BestBidAskV2 {
                asset:crate::task21_tests::asset(token,0,"Up"),best_bid_pusd_per_share:Some(ExactDecimal(bid.into())),
                best_ask_pusd_per_share:Some(ExactDecimal("0.5".into()))}),
                crate::task21_tests::orderbook_evidence(at+offset,at+offset,sequence),crate::task21_tests::fresh_quality());
            event.market=if token=="1" {initial[0].market.clone()}else{next[0].market.clone()};
            event.event_id=data_pipeline_core::derive_execution_market_event_v3_id(&event).unwrap();
            events.push(data_pipeline_core::decode_execution_market_event_v3(&data_pipeline_core::encode_execution_market_event_v3(&event).unwrap()).unwrap());
        }
        initial.extend(next);events.sort_by_key(|event|event.time.source_event_at_utc_ms);
        source["initial"]=serde_json::json!(initial);source["two_market_history_samples"]=serde_json::json!([
            [["0.45",300251],["0.46",300351],["0.47",300451],["0.48",300991]],
            [["0.35",300251],["0.37",300351],["0.39",300451],["0.40",300991],["0.41",300992],["0.42",300993],["0.43",300994]] ]);
        source["continuous_two_markets"]=serde_json::json!(true);source["continuous_observation_offset"]=serde_json::json!(299000);source["market_end"]=serde_json::json!(301000);
    }
    let (directory,mut hidden)=public_consumption_tests::gpu_compact_hidden_for_order_fixture(&initial,&events,false);
    let artifact=std::path::PathBuf::from(std::env::var("B_GPU_ORDER_ARTIFACT_DIR").unwrap());
    std::fs::create_dir_all(&artifact).unwrap();
    std::fs::rename(directory.path().join("public"),artifact.join("public-compact")).unwrap();
    std::fs::rename(directory.path().join("hidden"),artifact.join("hidden-compact")).unwrap();
    hidden["source_binding"]["public"]=serde_json::json!(artifact.join("public-compact"));
    hidden["source_binding"]["hidden"]=serde_json::json!(artifact.join("hidden-compact"));
    source["bounded_public_events"]=serde_json::json!(events);
    source["bounded_hidden"]=hidden;
    source["bounded_timing"]=serde_json::json!({"observation_at":1300,"calculation_duration":114,"dispatch_delay":0,
        "provenance":"gpu_frozen_actor_sends_from_real_bounded_source finite engineering fixture"});
    std::fs::write(artifact.join("original-actor-send.json"),serde_json::to_vec_pretty(source).unwrap()).unwrap();
    source.clone()
}

#[cfg(feature="shared-execution")]
#[tokio::test(flavor="current_thread")]
#[ignore="locked GPU initial sorting; original bounded source and actual receive checkpoint"]
async fn pending_actor_send_original_public_checkpoint() {
    let source=pending_actor_send_original_source().await;
    let pre=&source["bounded_pre_send_ledger"];
    assert_ne!(pre["cash"],source["sent_ledger"]["cash"],"原已领取Ledger资金确实改变");
    let records=source["records"].as_array().unwrap();
    assert!(records.iter().any(|r|r["visibility"]["Feedback"]["available_at_utc_ms"]==1410));
    assert_eq!(source["bounded_timing"]["calculation_duration"],114);
    assert!(source["bounded_hidden"]["buckets"].as_array().unwrap().iter().any(|b|b["bucket"]["bucket_time_utc_ms"]==1414),"真实同刻hidden桶必须存在");
}

#[cfg(feature="shared-execution")]
#[tokio::test(flavor="current_thread")]
#[ignore="locked GPU; real compact prefix and strict Execute boundary to one Actor send"]
async fn gpu_frozen_actor_sends_from_real_bounded_source() {bounded_gpu_actor_check(None).await;}

#[cfg(feature="shared-execution")]
#[tokio::test(flavor="current_thread")]
#[ignore="真实两Buy按实际发送时刻倒序，保持共同正文人口与稳定引用"]
async fn gpu_continuous_out_of_order_buy_population() {bounded_gpu_actor_check(Some("--out-of-order-buy")).await;}

#[cfg(feature="shared-execution")]
#[tokio::test(flavor="current_thread")]
#[ignore="真实非零BBA经ContinuousActorInputs逐窗冻结、静默及容量续接"]
async fn gpu_continuous_nonzero_history_preserves_windows() {bounded_gpu_actor_check(Some("--nonzero-history")).await;}

#[cfg(feature="shared-execution")]
#[tokio::test(flavor="current_thread")]
#[ignore="同账户相邻5min市场T−30热身/完整GRU/积压跨开市"]
async fn gpu_continuous_two_markets_share_account_and_keep_market_memory() {
    bounded_gpu_actor_check(Some("--two-markets")).await;
}

#[cfg(feature="shared-execution")]
#[tokio::test(flavor="current_thread")]
#[ignore="locked GPU; real NoTrade at the bounded send boundary"]
async fn gpu_frozen_actor_no_trade_at_real_send_boundary() {bounded_gpu_actor_check(Some("--no-trade")).await;}

#[cfg(feature="shared-execution")]
async fn bounded_gpu_actor_check(mode:Option<&str>) {
    let source=pending_actor_send_original_source_with_history(mode==Some("--nonzero-history")||mode==Some("--two-markets"),mode==Some("--two-markets")).await;
    let artifact=std::path::PathBuf::from(std::env::var("B_GPU_ORDER_ARTIFACT_DIR").unwrap());
    let mut command=std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap());
    command.arg(std::env::var("B_GPU_ACTOR_SEND_VALIDATOR").unwrap());
    if let Some(mode)=mode {command.arg(mode);}
    let mut child=command.stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(),&source).unwrap();
    let output=child.wait_with_output().unwrap();
    std::fs::write(artifact.join("actor-send-report.json"),&output.stdout).unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    eprintln!("actor_send={}",String::from_utf8_lossy(&output.stdout));
}


// 仅补本次实际已发正文的独立原CPU后验；不提供GPU调度或结果输入。
#[cfg(feature="shared-execution")]
fn pending_continuous_original_extension(actor:&mut SimRunActorV2,mut state:BusinessReducerStateV2,
    observation:&noe_projection::MemoryNoeObservationV4,ledger:&mut ledger::LedgerWriterV1,
    initial:&[ExecutionMarketEventV3],requests:&serde_json::Value) -> serde_json::Value {
    let mut stages=Vec::new();let mut all_records=Vec::new();
    let events:Vec<ExecutionMarketEventV3>=serde_json::from_value(requests["public_events"].clone()).unwrap();
    for event in &events {
        let at=match &event.payload {
            ExecutionMarketEventPayloadV3::LevelExecutionTransition(level)=>level.platform_effective_at_utc_ms,
            ExecutionMarketEventPayloadV3::BestBidAsk(_)=>event.time.source_event_at_utc_ms.expect("原BBA有源时刻"),
            _=>panic!("本原短源仅档位与显式BBA消息"),
        };
        let (next,_)=reduce_market_event_slice_v2(state,MarketEventPathV1::HiddenPlatform,at,
            std::slice::from_ref(event),actor._rule_snapshot.snapshot(),actor._finality_policy.as_ref())
            .unwrap_or_else(|_|panic!("原连续档位消息无法归约"));state=next;
    }
    for (index,value) in requests["requests"].as_array().unwrap().iter().enumerate() {
        let sent=value["sent_at"].as_i64().unwrap();
        let request:BusinessRequestV2=serde_json::from_value(value["body"].clone()).unwrap();
        let sequence=value["input_sequence"].as_u64().unwrap();
        if let BusinessRequestV2::PostOrders(body)=&request {
            let reserves=body.items.iter().map(|post| {
                let (principal,fee)=match &post.item.order.kind {
                    SimBuyOrderKindV1::GtcLimit {limit_price_pusd_per_share,size_shares,post_only}=> {
                        let event=initial.iter().find(|event|match &event.payload {
                            ExecutionMarketEventPayloadV3::InitialL2Snapshot(book)=>book.asset.token_id==post.item.order.token_id,
                            _=>false}).unwrap();
                        let ExecutionMarketEventPayloadV3::InitialL2Snapshot(book)=&event.payload else {unreachable!()};
                        let tick=&book.initial_tick_size_pusd_per_share;
                        let n=(*size_shares*rust_decimal::Decimal::from(100)).normalize().to_string().parse::<num_bigint::BigUint>().unwrap();
                        let price=(*limit_price_pusd_per_share/tick.0.parse::<rust_decimal::Decimal>().unwrap()).normalize().to_string().parse::<u16>().unwrap();
                        let cost=actor._rule_snapshot.limit_buy_cost_v1(&n,price,tick,*post_only).unwrap();
                        (crate::matching::decimal_from_micro_units_v1(cost.principal_micro.to_string().parse().unwrap()).unwrap(),
                            rust_decimal::Decimal::from_str_exact(&cost.fee_1e5_units.to_string()).unwrap()/rust_decimal::Decimal::from(100_000))
                    },
                    SimBuyOrderKindV1::FokMarket {spend_pusd,..}=>(*spend_pusd,rust_decimal::Decimal::ZERO),
                    _=>unreachable!("本原Actor代表只有GTC/FOK"),
                };
                let identity=ledger::LedgerSubmissionItemIdentityV1 {global_request_id:body.context.global_request_id.clone(),batch_item_ordinal:post.batch_item_ordinal};
                ledger::ReserveIntentV1 {request_identity:ledger::LedgerRequestIdentityV1(serde_json::to_string(&identity).unwrap()),
                    condition_id:post.item.order.condition_id.clone(),requested_principal_pusd:ExactDecimal(principal.to_string()),immediate_fee_reserve_pusd:ExactDecimal(fee.to_string())}
            }).collect::<Vec<_>>();
            ledger.try_reserve_action_v2(&reserves).unwrap();
            for (post,reserve) in body.items.iter().zip(&reserves) {
                let identity=ledger::LedgerSubmissionItemIdentityV1 {global_request_id:body.context.global_request_id.clone(),batch_item_ordinal:post.batch_item_ordinal};
                let event=ledger::LedgerLocalEventIdentityV1(serde_json::to_string(&("submission-started",&identity)).unwrap());
                ledger.record_submission_started(&event,&reserve.request_identity,&identity).unwrap();
            }
        }
        let (next,receipt)=register_request_candidate(&actor.opened,actor.scenario.initial_market_directory(),state,
            &request_cutoff(sent),&request,sequence,actor._latency_profile.as_ref(),actor._rule_snapshot.snapshot());state=next;
        let mut effects=Vec::new();let mut batches=Vec::new();
        while let Some(key)=state.session.as_ref().unwrap().mutation_requests.pending_actions.keys().next().copied() {
            let (next,effect)=advance_mutation_candidate_with_outputs(&actor.opened,actor.scenario.initial_market_directory(),state,
                key.effective_processing_at_utc_ms,actor._rule_snapshot.snapshot(),actor._finality_policy.as_ref());state=next;
            let effect=effect.unwrap();
            let body=match &effect.business {
                MutationActionBusinessEffectV3::Post(post)=>serde_json::json!({"Post":match &post.outcome {
                    PostOrderItemOutcomeV2::Accepted(body)=>serde_json::json!({"Accepted":body}),
                    PostOrderItemOutcomeV2::RejectedNoEffect(reason)=>serde_json::json!({"RejectedNoEffect":reason}),
                    PostOrderItemOutcomeV2::EvidenceUnknownNoEffect(reason)=>serde_json::json!({"EvidenceUnknownNoEffect":format!("{reason:?}")}),
                }}),
                MutationActionBusinessEffectV3::Cancel(targets)=>serde_json::json!({"Cancel":targets}),
                _=>unreachable!(),
            };
            effects.push(serde_json::json!({"at":key.effective_processing_at_utc_ms,"ordinal":key.batch_item_ordinal,"body":body}));
            state=gpu_accept_projection_reference(actor,state,BusinessReducerOutputV2::MutationAction(Some(effect)),&mut batches);
        }
        let mut received=sent;
        for batch in &batches {
            for family in ["orders","trades","direct"] {
                for member in batch[family].as_array().unwrap() {
                    for key in ["feedback_received_at_utc_ms","response_received_at_utc_ms"] {
                        if let Some(at)=member["metadata"][key].as_i64() {received=received.max(at);}
                    }
                }
            }
        }
        let visible=received+1;
        assert!(visible<initial[0].market.window.end_at_utc_ms,"本有限代表不越过市场终态");
        actor.projection_port.close_receive_prefix(visible);
        actor.projection_port.observe_public_time(&crate::public_consumption::TrustedActorExecutionTimeV1(crate::public_consumption::ActorTimeRecordV1 {
            run_id:actor.opened.run_id.clone(),drive_position:3+index as u64,logical_time_utc_ms:visible}));
        let records=observation.take_closed_records();
        all_records.extend(records.iter().map(|record|serde_json::json!({"record":record.record(),"position":record.position(),"visibility":record.visibility()})));
        let mut waiting=records;
        while !waiting.is_empty() {
            let count=waiting.len();let mut next=Vec::new();
            for record in waiting {
                let applied=ledger.apply_noe_memory_v2(&record).unwrap();
                if matches!(applied,ledger::LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation|ledger::LedgerMemoryApplyOutcomeV1::Applied(ledger::LedgerApplyOutcomeV1::MissingPlatformPredecessor {..})) {next.push(record);}
            }
            assert!(next.len()<count,"原连续NOE前驱未闭合");waiting=next;
        }
        stages.push(serde_json::json!({"sent_at":sent,"receipt":{"arrived_at":receipt.platform_arrived_at_utc_ms,"count":receipt.scheduled_action_count},
            "effects":effects,"batches":batches,"received_before":visible,"ledger":gpu_ledger_reservation_checkpoint(ledger,"continuous_original_receive")}));
    }
    serde_json::json!({"stages":stages,"records":all_records})
}

#[cfg(feature="shared-execution")]
#[tokio::test(flavor="current_thread")]
#[ignore="实际已发GPU正文的原CPU结果/NOE/Ledger后验"]
async fn continuous_actor_original_request_reference() {
    let path=std::path::PathBuf::from(std::env::var("B_GPU_ORDER_ARTIFACT_DIR").unwrap());
    let requests:serde_json::Value=serde_json::from_slice(&std::fs::read(path.join("actual-continuous-requests.json")).unwrap()).unwrap();
    let original:serde_json::Value=serde_json::from_slice(&std::fs::read(path.join("original-actor-send.json")).unwrap()).unwrap();
    let continuation=serde_json::json!({"requests":requests,"public_events":original["bounded_public_events"]});
    let source=pending_post_original_sources_at_continuation(&["fok_success"],false,Some(1410),Some(&continuation)).await;
    std::fs::write(path.join("original-continuous-reference.json"),serde_json::to_vec_pretty(&source[0]).unwrap()).unwrap();
}
