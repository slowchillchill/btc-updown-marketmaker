// 真实混合请求形成后，原取消请求/处理前状态与NOE是独立设备对照来源。
#[cfg(feature="shared-execution")]
fn pending_cancel_original_extension(actor:&mut SimRunActorV2,mut state:BusinessReducerStateV2,
    observation:&noe_projection::MemoryNoeObservationV4,ledger:&mut ledger::LedgerWriterV1,
    mut cutoff:i64) -> serde_json::Value {
    use crate::runtime_v2::{CancelFailureScenarioV1,CancelFailureTargetV1,CancelScenarioOperationV1};
    let market_id=(0..100).map(|i|format!("market-population-{i}")).max_by_key(|id|actor._latency_profile.sample_delay_ms(
        state.session.as_ref().unwrap().session_seed.0.as_bytes(),id,crate::latency_profile::DelaySampleDomainV1::RequestOutbound)).unwrap();
    let scenario=CancelFailureScenarioV1 {targets:vec![CancelFailureTargetV1 {
        global_request_id:"cancel-single-failure".into(),platform_order_id:"sim-1".into()},CancelFailureTargetV1 {
        global_request_id:market_id.clone(),platform_order_id:"sim-4".into()}]};
    let input=BusinessInputV2::CancelScenario(CancelScenarioOperationV1::Configure(scenario.clone()));
    state=reduce_actor_input_v2(&actor.opened,actor.scenario.initial_market_directory(),state,&input,20,
        actor._latency_profile.as_ref(),actor._rule_snapshot.snapshot(),actor._finality_policy.as_ref())
        .unwrap_or_else(|_|panic!("原取消配置")).0;
    let item=|id:&str,order:&str|crate::execution::CancelOrderItemV1 {item_id:id.into(),platform_order_id:order.into()};
    let mut stages=Vec::new();let mut all_records=Vec::new();
    for index in 0..5 {
        let (id,request)=match index {
            0=>("cancel-single-failure".to_string(),BusinessRequestV2::CancelOrder(CancelOrderRequestV2 {
                context:context_for_gpu_cancel("cancel-single-failure"),item:item("cancel-one","sim-1")})),
            1=>("cancel-pair-dedup".to_string(),BusinessRequestV2::CancelOrders(CancelOrdersRequestV2 {
                context:context_for_gpu_cancel("cancel-pair-dedup"),items:vec![
                    crate::runtime_v2::CancelOrderBatchItemV2 {batch_item_ordinal:7,item:item("cancel-r0","sim-1")},
                    crate::runtime_v2::CancelOrderBatchItemV2 {batch_item_ordinal:1,item:item("cancel-r1","sim-2")},
                    crate::runtime_v2::CancelOrderBatchItemV2 {batch_item_ordinal:3,item:item("cancel-r0-again","sim-1")},]})),
            3=>("cancel-later-single".into(),BusinessRequestV2::CancelOrder(CancelOrderRequestV2 {
                context:context_for_gpu_cancel("cancel-later-single"),item:item("later-single","sim-4")})),
            _=> {
                let id=if index==2 {market_id.clone()} else {"cancel-market-empty".into()};
                let condition=actor.scenario.initial_market_directory().markets().next().unwrap().market().condition_id.clone();
                (id.clone(),BusinessRequestV2::CancelMarketOrders(crate::runtime_v2::CancelMarketOrdersRequestV2 {
                    context:context_for_gpu_cancel(&id),condition_id:condition}))
            },
        };
        let sent=cutoff+1;let sequence=21+index as u64*2;
        let before=state.session.as_ref().unwrap().execution_state.gpu_order_checkpoint_for_tests("sim-1");
        let (next,receipt)=register_request_candidate(&actor.opened,actor.scenario.initial_market_directory(),state,
            &request_cutoff(sent),&request,sequence,actor._latency_profile.as_ref(),actor._rule_snapshot.snapshot());state=next;
        let duplicate=existing_mutation_request_receipt_v2(&state,&id).unwrap();
        assert_eq!(duplicate,receipt,"原重复取消保首次receipt");
        let mut late=None;
        if index==2 {
            let late_id=(0..100).map(|i|format!("late-cancel-population-{i}")).min_by_key(|id|actor._latency_profile.sample_delay_ms(
                state.session.as_ref().unwrap().session_seed.0.as_bytes(),id,crate::latency_profile::DelaySampleDomainV1::RequestOutbound)).unwrap();
            let first=actor.scenario.initial_market_directory().markets().next().unwrap().market().condition_id.clone();
            let other=market_with_window('e',"456","301","302",1_000,300_000).market().condition_id.clone();
            let posts=(0..9).map(|i|PostOrderBatchItemV2 {batch_item_ordinal:i,item:post_item(&format!("late-cancel-{i}"),
                if i==8 {&other} else {&first},&TokenId(if i==8 {"301"} else {"1"}.into()),
                SimBuyOrderKindV1::GtcLimit {limit_price_pusd_per_share:"0.1".parse().unwrap(),
                    size_shares:"5".parse().unwrap(),post_only:false})}).collect::<Vec<_>>();
            let cost=actor._rule_snapshot.limit_buy_cost_v1(&num_bigint::BigUint::from(500u32),10,
                &ExactDecimal("0.01".into()),false).unwrap();
            let principal=crate::matching::decimal_from_micro_units_v1(cost.principal_micro.to_string().parse().unwrap()).unwrap();
            let fee=rust_decimal::Decimal::from_str_exact(&cost.fee_1e5_units.to_string()).unwrap()/rust_decimal::Decimal::from(100_000);
            let reserve=posts.iter().map(|post|ledger::ReserveIntentV1 {
                request_identity:ledger::LedgerRequestIdentityV1(serde_json::to_string(&ledger::LedgerSubmissionItemIdentityV1 {
                    global_request_id:late_id.clone(),batch_item_ordinal:post.batch_item_ordinal}).unwrap()),
                condition_id:post.item.order.condition_id.clone(),requested_principal_pusd:ExactDecimal(principal.to_string()),
                immediate_fee_reserve_pusd:ExactDecimal(fee.to_string())}).collect::<Vec<_>>();
            ledger.try_reserve_action_v2(&reserve).unwrap();
            for (post,reserve) in posts.iter().zip(&reserve) {
                let identity=ledger::LedgerSubmissionItemIdentityV1 {global_request_id:late_id.clone(),batch_item_ordinal:post.batch_item_ordinal};
                let event=ledger::LedgerLocalEventIdentityV1(serde_json::to_string(&("submission-started",&identity)).unwrap());
                ledger.record_submission_started(&event,&reserve.request_identity,&identity).unwrap();
            }
            let request=BusinessRequestV2::PostOrders(PostOrdersRequestV2 {context:context_for_gpu_cancel(&late_id),items:posts.clone()});
            let (next,late_receipt)=register_request_candidate(&actor.opened,actor.scenario.initial_market_directory(),state,
                &request_cutoff(sent+1),&request,sequence+1,actor._latency_profile.as_ref(),actor._rule_snapshot.snapshot());state=next;
            assert!(late_receipt.platform_arrived_at_utc_ms<receipt.platform_arrived_at_utc_ms,"原发送后人口到达先于市场取消处理");
            late=Some(serde_json::json!({"request":request,"sent_at":sent+1,"input_sequence":sequence+1,
                "receipt":{"arrived_at":late_receipt.platform_arrived_at_utc_ms,"count":late_receipt.scheduled_action_count},
                "reserve_intents":reserve,"sent_ledger":gpu_ledger_reservation_checkpoint(ledger,"late_real_submission")}));
        }
        let mut effects=Vec::new();let mut batches=Vec::new();
        while let Some(key)=state.session.as_ref().unwrap().mutation_requests.pending_actions.keys().next().copied() {
            let (next,effect)=advance_mutation_candidate_with_outputs(&actor.opened,actor.scenario.initial_market_directory(),state,
                key.effective_processing_at_utc_ms,actor._rule_snapshot.snapshot(),actor._finality_policy.as_ref());state=next;
            let effect=effect.unwrap();
            let body=match &effect.business {
                MutationActionBusinessEffectV3::Cancel(targets)=>serde_json::json!({"cancel":targets}),
                MutationActionBusinessEffectV3::Post(post)=>serde_json::json!({"post":{"item_id":post.item_id,"ordinal":post.batch_item_ordinal,
                    "outcome":match &post.outcome {PostOrderItemOutcomeV2::Accepted(body)=>serde_json::json!({"Accepted":body}),
                        PostOrderItemOutcomeV2::RejectedNoEffect(reason)=>serde_json::json!({"RejectedNoEffect":reason}),_=>unreachable!()}}}),
                _=>unreachable!(),
            };
            effects.push(serde_json::json!({"at":key.effective_processing_at_utc_ms,"input_sequence":key.input_sequence,
                "ordinal":key.batch_item_ordinal,"body":body,"account":state.session.as_ref().unwrap().execution_state.gpu_order_checkpoint_for_tests("sim-1")}));
            if effect.lifecycle.is_empty() && effect.completed_response.is_none() {
                let (next,_,batch)=finalize_projection_batch_v2(&actor.opened.run_id,state,
                    BusinessReducerOutputV2::MutationAction(Some(effect)),actor._latency_profile.as_ref()).unwrap();
                assert!(batch.is_none(),"原中途无效果取消没有公开反馈");state=next;
            } else {state=gpu_accept_projection_reference(actor,state,BusinessReducerOutputV2::MutationAction(Some(effect)),&mut batches);}
        }
        cutoff=effects.last().unwrap()["at"].as_i64().unwrap()+10_000;
        actor.projection_port.close_receive_prefix(cutoff);
        actor.projection_port.observe_public_time(&crate::public_consumption::TrustedActorExecutionTimeV1(crate::public_consumption::ActorTimeRecordV1 {
            run_id:actor.opened.run_id.clone(),drive_position:2+index as u64,logical_time_utc_ms:cutoff}));
        let records=observation.take_closed_records();
        let originals=records.iter().map(|r|serde_json::json!({"record":r.record(),"position":r.position(),"visibility":r.visibility()})).collect::<Vec<_>>();
        let mut waiting=records;
        while !waiting.is_empty() {
            let n=waiting.len();let mut next=Vec::new();
            for record in waiting {
                let applied=ledger.apply_noe_memory_v2(&record).unwrap();
                if matches!(applied,ledger::LedgerMemoryApplyOutcomeV1::MissingEconomicAssociation|ledger::LedgerMemoryApplyOutcomeV1::Applied(ledger::LedgerApplyOutcomeV1::MissingPlatformPredecessor {..})) {next.push(record);}
            }
            assert!(next.len()<n,"原取消NOE前驱闭合");waiting=next;
        }
        all_records.extend(originals.clone());
        stages.push(serde_json::json!({"request_id":id,"request":request,"sent_at":sent,"input_sequence":sequence,
            "receipt":{"arrived_at":receipt.platform_arrived_at_utc_ms,"count":receipt.scheduled_action_count},
            "before":before,"late":late,"effects":effects,"records":originals,"closed_at":cutoff,
            "ledger":gpu_ledger_reservation_checkpoint(ledger,"after_original_cancellation")}));
    }
    serde_json::json!({"scenario":scenario,"scenario_input_sequence":20,"stages":stages,"records":all_records})
}

#[cfg(feature="shared-execution")]
fn context_for_gpu_cancel(id:&str)->RequestContextV2 {
    RequestContextV2 {global_request_id:id.into(),caller_correlation_id:None}
}

#[cfg(feature="shared-execution")]
#[tokio::test(flavor="current_thread")]
async fn pending_cancel_complete_original_sources() {
    let cases=pending_post_original_sources_for(&["mixed_accept_last"],true).await;
    let artifact=std::path::PathBuf::from(std::env::var("B_GPU_ORDER_ARTIFACT_DIR").unwrap());
    std::fs::create_dir_all(&artifact).unwrap();
    std::fs::write(artifact.join("original-pending-cancel.json"),serde_json::to_vec_pretty(&cases).unwrap()).unwrap();
    let stages=cases[0]["cancellation"]["stages"].as_array().unwrap();
    assert_eq!(stages.len(),5);
    assert_eq!(stages[0]["effects"][0]["body"]["cancel"][0]["outcome"]["NotCanceledNoEffect"],"TemporaryFailureNoEffect");
    let market=stages[2]["effects"].as_array().unwrap().last().unwrap()["body"]["cancel"].as_array().unwrap();
    assert_eq!(market.iter().map(|t|t["platform_order_id"].as_str().unwrap()).collect::<Vec<_>>(),
        vec!["sim-10","sim-3","sim-4","sim-5","sim-6","sim-7","sim-8","sim-9"]);
    assert_eq!(market[2]["outcome"]["NotCanceledNoEffect"],"TemporaryFailureNoEffect");
    assert!(stages[4]["effects"][0]["body"]["cancel"].as_array().unwrap().is_empty());
}

