#[test]
#[ignore="AutoDL紧凑hidden真实恢复→原SIM行情/FIFO→设备资产状态"]
fn gpu_compact_hidden_drives_exact_asset_state_and_queues() {
    use data_pipeline_training::{compact_public::PublicCompactWriterV1,
        compact_hidden_material::{prepare_compact_hidden_material_v1,CompactHiddenMaterialReaderV1},
        compact_hidden_gpu::next_compact_hidden_gpu_bucket_v1,gpu_initial_sort::GpuInitialRunOrderV1};
    use crate::market::{MarketStateStoreV2,MarketEventHeaderV1};
    use crate::matching::gpu_queue_tests::HiddenQueueReference;
    use data_pipeline_core::pm_reduction::PmStepChangeV1;
    let temp=tempfile::tempdir().unwrap();
    let source=temp.path().join("public");let hidden=temp.path().join("hidden");
    let mut public=core();
    let mut writer=PublicCompactWriterV1::create(&source,public.header().clone()).unwrap();
    let mut down=book("0");
    if let PolymarketOrderbookEvent::Book {asset_id,bids,asks,..}=&mut down {
        *asset_id=DOWN_TOKEN_ID.into();bids.clear();asks.clear();
    }
    let recovery = record(5, TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(
        row("epoch-b", 1, 21005, 22000, book("15")),
    )));
    let depletion = record(6, TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(
        row("epoch-b", 2, 21006, 22000,
            PolymarketOrderbookEvent::PriceChange {price_changes:vec![change(Some("13"))]}),
    )));
    let at_end = record(7, TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(
        row("epoch-b", 3, MARKET_END_AT, MARKET_END_AT + 1000,
            PolymarketOrderbookEvent::PriceChange {price_changes:vec![change(Some("77"))]}),
    )));
    for input in [meta(0),pm(1,1,21000,book("10.0000007")),pm(2,2,21001,down),
        pm(3,3,21002,PolymarketOrderbookEvent::PriceChange {price_changes:vec![change(Some("20.0000007"))]}),
        pm(4,4,21003,PolymarketOrderbookEvent::PriceChange {price_changes:vec![change(Some("20.0000008"))]}),
        recovery,depletion,at_end,end(8)] {
        let prepared=prepare_public_ingress_v1(&public,&input,&BTreeMap::new(),&[]).unwrap();
        let applied=public.push_facts(&prepared).unwrap();
        writer.push_prepared(&input,&prepared,&applied).unwrap();
    }
    writer.publish().unwrap();
    let context=public.pm_context(CONDITION_ID).unwrap();
    let assets:Vec<_>=context.assets.iter().map(|v|v.asset.clone()).collect();
    let scenario=sealed_scenario_for_markets(vec![crate::scenario::PlatformMarketV1::try_new(
        context.market.clone(),assets.clone().try_into().unwrap()).unwrap()]);
    let mut original=MarketStateStoreV2::new(scenario.initial_market_directory());
    let mut queues=HiddenQueueReference::new(scenario.initial_market_directory(),CONDITION_ID);
    let mut sort=config().sort;sort.merge_workers=8;
    let order=Arc::new(GpuInitialRunOrderV1::start(Path::new(&std::env::var("B_GPU_PYTHON").unwrap()),
        Path::new(&std::env::var("B_GPU_SORT_WORKER").unwrap())).unwrap());
    prepare_compact_hidden_material_v1(&source,&hidden,1<<20,&sort,order,"gpu-hidden-asset-test".into(),None,|_,_,_,_| {}).unwrap();
    let mut reader=CompactHiddenMaterialReaderV1::open(&hidden,&source,1,1<<20,1<<20,|_,_,_,_| {}).unwrap();
    let slots:Vec<_>=assets.iter().flat_map(|a|[(a.token_id.0.clone(),BookSide::Bid,"0.5".parse().unwrap()),
        (a.token_id.0.clone(),BookSide::Ask,"0.6".parse().unwrap())]).collect();
    let mut buckets=Vec::new();let mut expected=Vec::new();let mut insertion_index=None;
    while let Some(bucket)=next_compact_hidden_gpu_bucket_v1(&mut reader).unwrap() {
        for (market,step) in &bucket.steps {
            let mut maker = Vec::new();
            if let Some(normalized)=crate::market::normalize_pm_step_v1(step).unwrap() {
                let header=MarketEventHeaderV1 {identity:&step.derived_id.0,market,time:&step.dependency_time,quality:&step.quality};
                original.apply_admitted_header(&header,&normalized,bucket.bucket.bucket_time_utc_ms,MARKET_END_AT).unwrap();
                // 原ExecutionQueueStateV2::end_matching清除全部E/A，不复制结束逻辑。
                maker = queues.apply(&normalized,bucket.bucket.bucket_time_utc_ms,MARKET_END_AT);
            }
            if matches!(&step.change,PmStepChangeV1::InitialSnapshot {asset,..} if asset.token_id.0==UP_TOKEN_ID) {
                // 原有效GTC余量的生产插入点；当前测试不冒充整订单接纳或经济反馈。
                queues.insert_account(UP_TOKEN_ID,"0.5".parse().unwrap());
                insertion_index = Some(expected.len());
            }
            let states:Vec<_>=assets.iter().map(|asset|{
                let (initialized,tick,quality)=original.asset_state_for_gpu_tests(CONDITION_ID,&asset.token_id.0);
                serde_json::json!({"initialized":initialized,"tick":tick.map(|v|v.to_string()),"quality":quality})
            }).collect();
            let shares:Vec<_>=slots.iter().map(|(token,side,price)|original.external_quantity_for_tests(CONDITION_ID,token,*side,*price).unwrap().to_string()).collect();
            expected.push(serde_json::json!({"assets":states,"shares":shares,"queues":queues.describe(&slots),"maker":maker}));
        }
        buckets.push(bucket);
    }
    assert!(buckets.iter().flat_map(|b|&b.steps).any(|(_,s)|matches!(s.change,PmStepChangeV1::InitialSnapshot {ref bids,ref asks,..} if bids.is_empty()&&asks.is_empty())));
    let input=serde_json::json!({"assets":assets,"slots":slots.iter().map(|(token,side,p)|serde_json::json!([token,side,p.to_string()])).collect::<Vec<_>>(),
        "buckets":buckets,"expected":expected,"insertion_index":insertion_index,
        "source_binding":{"binary":std::env::var("B_GPU_HIDDEN_CLI").unwrap(),"hidden":hidden,"public":source,
            "rows":1,"bytes":1048576,"max_batch_bytes":1048576}});
    let mut child=std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(std::env::var("B_GPU_HIDDEN_ASSET_VALIDATOR").unwrap()).stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(),&input).unwrap();
    let output=child.wait_with_output().unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    eprintln!("gpu_hidden_asset_reference={}",String::from_utf8(output.stdout).unwrap());
}
#[cfg(feature="shared-execution")]
#[tokio::test(flavor="current_thread")]
#[ignore="AutoDL真实紧凑TWAP/空Book/Terminal→一次原Resolved与后继Lifecycle"]
async fn gpu_compact_hidden_reference_population_closes_without_fabricating_market_effects() {
    gpu_hidden_terminal_source(false,false).await;
}

#[cfg(feature="shared-execution")]
#[tokio::test(flavor="current_thread")]
#[ignore="AutoDL真实空Book→外层GTC挂单→Terminal市场关闭/释放→同NOE/Ledger"]
async fn gpu_compact_hidden_terminal_closes_real_resting_gtc_and_releases_once() {
    gpu_hidden_terminal_source(true,false).await;
}

#[cfg(feature="shared-execution")]
#[tokio::test(flavor="current_thread")]
#[ignore="AutoDL真实Maker→Terminal→实际最终性关闭请求前驱"]
async fn gpu_compact_hidden_terminal_keeps_actual_maker_pending_until_terminal() {
    gpu_hidden_terminal_source(true,true).await;
}

#[cfg(feature="shared-execution")]
async fn gpu_hidden_terminal_source(with_order: bool, with_pending: bool) {
    use data_pipeline_training::{compact_public::{PublicCompactWriterV1, CompactFactPayloadV1},
        compact_hidden_material::{prepare_compact_hidden_material_v1, CompactHiddenMaterialReaderV1},
        compact_hidden_gpu::next_compact_hidden_gpu_bucket_v1, gpu_initial_sort::GpuInitialRunOrderV1};
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("public");
    let hidden = temp.path().join("hidden");
    let mut public = core();
    let mut market_facts = BTreeMap::new();
    let mut writer = PublicCompactWriterV1::create(&source, public.header().clone()).unwrap();
    let reference = |window, sequence| {
        let mut input = twap_input(window);
        input.common.timestamp_received = 21001 + sequence as i64;
        input.common.message_timestamp = 21000;
        input.common.payload_timestamp = 21000;
        input.common.age_ms = Some(1 + sequence as i64);
        input.collector_admission_sequence = sequence;
        input.collector_admission_time_utc_ms = 21002 + sequence as i64;
        input
    };
    let mut missing = reference(60, 3);
    missing.common.freshness_status = RtdsFreshnessStatus::InvalidTimestamp;
    let empty = |token: &str| PolymarketOrderbookEvent::Book {
        asset_id: token.into(), last_trade_price: None, bids: Vec::new(), asks: Vec::new(),
    };
    let mut inputs=vec![meta(0), record(1, TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference(30, 1))),
        record(2, TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference(60, 2))),
        record(3, TypedRawRowV2::PolymarketRtdsChainlinkTwap(missing)),
        pm(4, 1, 22000, empty(UP_TOKEN_ID)), pm(5, 2, 22000, empty(DOWN_TOKEN_ID)),
        record(if with_pending {8} else {6}, TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(terminal_metadata_row(&market()))))),
        record(if with_pending {9} else {7}, TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(terminal_orderbook_row(&market()))))];
    if with_pending {
        let delta=|size:&str|PolymarketOrderbookEvent::PriceChange {price_changes:vec![PolymarketPriceChangeEntry {
            asset_id:UP_TOKEN_ID.into(),price:"0.4".into(),size:Some(size.into()),side:PolymarketSide::Buy,best_bid:None,best_ask:None}]};
        inputs.insert(6,pm(6,3,26000,delta("3")));
        inputs.insert(7,pm(7,4,27000,delta("0")));
    }
    inputs.push(end(if with_pending {10} else {8}));
    for input in inputs {
        let prepared = prepare_public_ingress_v1(&public, &input, &BTreeMap::new(), &[]).unwrap();
        let applied = public.push_facts(&prepared).unwrap();
        writer.push_prepared(&input, &prepared, &applied).unwrap();
        // 只复用原纯身份物化，不重跑归约；reference header继续用真实canonical ID。
        for reduction in applied.reductions {
            let materialized = data_pipeline_core::public_reduction::materialize_public_reduction_v1(reduction).unwrap();
            for (binding, event) in materialized.bindings.into_iter().zip(materialized.records) {
                if let CanonicalEventRecordV2::MarketFact(fact) = event {
                    market_facts.insert(binding.position, fact);
                }
            }
        }
    }
    writer.publish().unwrap();
    let assets = public.pm_context(CONDITION_ID).unwrap().assets.into_iter().map(|a| a.asset).collect::<Vec<_>>();
    let mut sort = config().sort;
    sort.merge_workers = 8;
    let order = Arc::new(GpuInitialRunOrderV1::start(Path::new(&std::env::var("B_GPU_PYTHON").unwrap()),
        Path::new(&std::env::var("B_GPU_SORT_WORKER").unwrap())).unwrap());
    prepare_compact_hidden_material_v1(&source, &hidden, 1<<20, &sort, order,
        "gpu-hidden-reference-test".into(), None, |_,_,_,_| {}).unwrap();
    let mut reader = CompactHiddenMaterialReaderV1::open(&hidden, &source, 1, 1<<20, 1<<20, |_,_,_,_| {}).unwrap();
    let mut buckets = Vec::new();
    let mut canonical_market_facts_checked = 0;
    while let Some(bucket) = next_compact_hidden_gpu_bucket_v1(&mut reader).unwrap() {
        for ((position, reference), event_id) in bucket.bucket.reference_positions.iter()
            .zip(&bucket.references).zip(&bucket.reference_market_event_ids) {
            let restored = reader.reference_market_fact(*position).unwrap();
            match &reference.payload {
                CompactFactPayloadV1::Inline(PublicBusinessPayloadV1::MarketFact {..}) => {
                    let expected = &market_facts[&reference.position];
                    assert_eq!(restored.as_ref(), Some(expected));
                    assert_eq!(event_id.as_ref(), Some(&expected.event_id));
                    canonical_market_facts_checked += 1;
                }
                _ => {assert!(restored.is_none()); assert!(event_id.is_none());}
            }
        }
        buckets.push(bucket);
    }
    assert_eq!(buckets.iter().map(|b| b.bucket.bucket_time_utc_ms).collect::<Vec<_>>(), if with_pending {vec![21000,22000,26000,27000,31000]} else {vec![21000,22000,31000]});
    assert_eq!(buckets[0].references.len(), 2);
    assert!(buckets[0].steps.is_empty());
    assert!(buckets[0].references.iter().all(|r| matches!(r.payload,
        CompactFactPayloadV1::Inline(PublicBusinessPayloadV1::ReferenceFact(CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(_))))));
    assert!(buckets.last().unwrap().references.iter().any(|r| matches!(r.payload,
        CompactFactPayloadV1::Inline(PublicBusinessPayloadV1::MarketFact { .. }))));
    let scenario = sealed_scenario_for_markets(vec![crate::scenario::PlatformMarketV1::try_new(
        market(), assets.clone().try_into().unwrap()).unwrap()]);
    // 有限工程profile，仅将真实Maker终态放在Terminal之后，不是训练默认。
    let finality_path=serde_json::json!({"steps":[{"state":"MATCHED","offset_ms":0},
        {"state":"MINED","offset_ms":5000},{"state":"CONFIRMED","offset_ms":6000}],"economic_application_ordinal":1});
    let finality=with_pending.then(||Arc::new(TradeFinalityPolicyV2::decode_and_seal(
        &serde_json::to_vec(&serde_json::json!({"schema_version":"trade_finality_policy_v2",
        "normal_path":finality_path,"failure_overrides":[]})).unwrap()).unwrap()));
    let saved=Rc::new(RefCell::new(None));
    let capture=Rc::clone(&saved);
    let normalization=Rc::new(RefCell::new(Vec::new()));
    let normalization_capture=Rc::clone(&normalization);
    let (mut execution,_)=public_execution_with_scenario_capacity_and_projection(
        &temp.path().join("sim.jsonl"),scenario.clone(),SimCallerRoleV1::ModelTrain,32,None,
        Some(Box::new(move |path,opened| {
            opened.projection_version=crate::runtime_v2::BusinessProjectionVersionV1::V4;
            let reference=contracts::execution_normalization::DurableSourceReferenceV1 {
                source:contracts::execution_normalization::ImmutableSourceReferenceV1::Location(path.display().to_string())};
            let memory=noe_projection::MemoryNoeProjectionPortV4::new(path.parent().unwrap().join("noe-memory"),
                opened.clone(),reference.clone(),reference.clone(),"create-public".into(),"gpu-hidden-terminal".into());
            *capture.borrow_mut()=Some(memory.observation());
            let durable=noe_projection::SimV2NoeProjectionPortV4::new(path.parent().unwrap().join("noe-durable"),
                opened.clone(),reference.clone(),reference,"create-public".into(),"gpu-hidden-terminal".into());
            Box::new(GpuNoeReferencePort {memory,durable,normalization_reference:normalization_capture,normalization_core:None})
        })),finality).await;
    // 此无订单/零持仓工程例不执行请求脚本；显式安装已定请求式模式，禁止自动兑付。
    execution.configure_resolution_redeem_v1(crate::ResolutionRedeemScenarioV1 {
        steps:Vec::new(),costs:Vec::new()}).unwrap();
    let observation=saved.borrow_mut().take().unwrap();
    // 开户/初始市场已由同一原端口接纳；唯一消费者先领取初始化人口。
    let mut ledger=ledger::LedgerWriterV1::new_with_admission_v2(ledger::AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    ledger.apply_noe_memory_v2(&observation.opening_record().unwrap()).unwrap();
    for record in observation.take_closed_records() {ledger.apply_noe_memory_v2(&record).unwrap();}
    let ledger_before=gpu_ledger_reservation_checkpoint(&ledger,"before_Resolved");
    let mut state=execution.actor.state.take().unwrap();
    let next_private=state.session.as_ref().unwrap().next_private_feedback_sequence;
    let next_batch=state.session.as_ref().unwrap().next_projection_batch_sequence;
    let session_seed=state.session.as_ref().unwrap().session_seed.0.clone();
    let mut original = crate::market::MarketStateStoreV2::new(scenario.initial_market_directory());
    let mut controls = Vec::new();
    let mut qualified_sources=Vec::new();
    let mut batches=Vec::new();
    let mut lifecycle_count=0;
    let mut post_reference=None;
    let mut accepted_records=Vec::new();
    let mut ledger_accepted=None;
    let mut maker_checkpoints=Vec::new();
    let mut reference_index = 0;
    let mut terminal_index = None;
    for bucket in &buckets {
        if with_pending && bucket.bucket.bucket_time_utc_ms==31000 {
            let (next,effects)=advance_rebate_clock_to_candidate(state,MARKET_END_AT);
            state=next;
            assert!(effects.into_lifecycle_feedback().unwrap().is_empty(),"Maker end仍有真实pending fill，不提前市场结果");
        }
        for reference in &bucket.references {
            if let CompactFactPayloadV1::Inline(PublicBusinessPayloadV1::MarketFact { payload, .. }) = &reference.payload {
                if matches!(payload, CanonicalMarketFactPayloadV1::Terminal(_)) {
                    terminal_index.get_or_insert(reference_index);
                }
                let fact = &market_facts[&reference.position];
                if let Some(normalized) = crate::market::normalize_public_market_fact_v1(payload, &fact.quality).unwrap() {
                    let header = crate::market::MarketEventHeaderV1 {identity:&fact.event_id.0, market:&fact.market,
                        time:&fact.dependency_time, quality:&fact.quality};
                    let applied=original.apply_admitted_header(&header,&normalized,bucket.bucket.bucket_time_utc_ms,MARKET_END_AT).unwrap();
                    if let Some(qualified)=applied.qualified_resolution.as_ref() {
                        qualified_sources.push(serde_json::json!({"reference_index":reference_index,
                            "event_id":qualified.resolution_event_id,"source_at":qualified.resolution_source_event_at_utc_ms,
                            "terminal":qualified.terminal,"parsed_payouts":qualified.parsed_payouts.map(|p|p.to_string())}));
                    }
                    let (next,effects)=reduce_normalized_market_slice_v1(state,MarketEventPathV1::HiddenPlatform,
                        bucket.bucket.bucket_time_utc_ms,&[header],&[normalized],execution.actor._rule_snapshot.snapshot(),
                        execution.actor._finality_policy.as_ref()).unwrap_or_else(|_|panic!("真实canonical市场参考归约失败"));
                    state=next;
                    let mut lifecycle=Vec::new();
                    for effect in effects.execution {lifecycle.extend(effect.into_lifecycle_feedback().unwrap());}
                    if !lifecycle.is_empty() {
                        lifecycle_count+=lifecycle.len();
                        state=gpu_accept_projection_reference(&mut execution.actor,state,
                            BusinessReducerOutputV2::LogicalDrive(LogicalDriveOutputV2 {lifecycle,..Default::default()}),&mut batches);
                    }
                }
                if matches!(payload,CanonicalMarketFactPayloadV1::Terminal(_)) {
                    reference_index+=1;
                    continue;
                }
                let (mapped, trading, quality) = original.control_state_for_gpu_tests(CONDITION_ID);
                controls.push(serde_json::json!({"reference_index":reference_index, "mapped":mapped,
                    "trading":trading, "quality":quality}));
            }
            reference_index += 1;
        }
        for (market, step) in &bucket.steps {
            if let Some(normalized) = crate::market::normalize_pm_step_v1(step).unwrap() {
                let header = crate::market::MarketEventHeaderV1 {identity:&step.derived_id.0, market,
                    time:&step.dependency_time, quality:&step.quality};
                original.apply_admitted_header(&header, &normalized, bucket.bucket.bucket_time_utc_ms, MARKET_END_AT).unwrap();
                let (next,effects)=reduce_normalized_market_slice_v1(state,MarketEventPathV1::HiddenPlatform,
                    bucket.bucket.bucket_time_utc_ms,&[header],&[normalized],execution.actor._rule_snapshot.snapshot(),
                    execution.actor._finality_policy.as_ref()).unwrap_or_else(|_|panic!("真实空Book隐藏归约失败"));
                state=next;
                let mut lifecycle=Vec::new();
                for effect in effects.execution {lifecycle.extend(effect.into_lifecycle_feedback().unwrap());}
                if with_pending && !lifecycle.is_empty() {
                    maker_checkpoints.push(serde_json::json!({"at":bucket.bucket.bucket_time_utc_ms,
                        "order":state.session.as_ref().unwrap().execution_state.gpu_order_checkpoint_for_tests("sim-1")}));
                    state=gpu_accept_projection_reference(&mut execution.actor,state,
                        BusinessReducerOutputV2::LogicalDrive(LogicalDriveOutputV2 {lifecycle,..Default::default()}),&mut batches);
                } else {assert!(lifecycle.is_empty(),"原空Book不产生账户或成交反馈");}
            }
        }
        if with_order && bucket.bucket.bucket_time_utc_ms==22000 {
            let request=BusinessRequestV2::PostOrders(PostOrdersRequestV2 {
                context:RequestContextV2 {global_request_id:"gpu-terminal-resting-post".into(),caller_correlation_id:None},
                items:vec![PostOrderBatchItemV2 {batch_item_ordinal:0,item:post_item("gpu-terminal-resting-post:0",
                    &ConditionId(CONDITION_ID.into()),&TokenId(UP_TOKEN_ID.into()),SimBuyOrderKindV1::GtcLimit {
                        limit_price_pusd_per_share:"0.4".parse().unwrap(),size_shares:"5".parse().unwrap(),post_only:false})}],
            });
            let (next,receipt)=register_request_candidate(&execution.actor.opened,scenario.initial_market_directory(),state,
                &request_cutoff(22100),&request,11,execution.actor._latency_profile.as_ref(),execution.actor._rule_snapshot.snapshot());
            state=next;
            let processing=receipt.platform_arrived_at_utc_ms();
            assert!(processing<MARKET_END_AT,"原GTC到达须在市场end前，不缩改真实时延");
            let (next,accepted)=advance_mutation_candidate_with_outputs(&execution.actor.opened,scenario.initial_market_directory(),
                state,processing,execution.actor._rule_snapshot.snapshot(),execution.actor._finality_policy.as_ref());
            state=next;
            let accepted=accepted.expect("真实外层GTC在到达时处理");
            let MutationActionBusinessEffectV3::Post(post)=&accepted.business else {panic!("原Post结果")};
            assert!(matches!(&post.outcome,PostOrderItemOutcomeV2::Accepted(_)),"原合法空Book GTC必须接受: {:?}",post.outcome);
            state=gpu_accept_projection_reference(&mut execution.actor,state,BusinessReducerOutputV2::MutationAction(Some(accepted)),&mut batches);
            post_reference=Some(serde_json::json!({"request":request,"sent_at":22100,"input_sequence":receipt.input_sequence(),
                "processing_at":processing,"accepted":state.session.as_ref().unwrap().execution_state.gpu_order_checkpoint_for_tests("sim-1")}));
            execution.actor.projection_port.close_receive_prefix(29999);
            execution.actor.projection_port.observe_public_time(&crate::public_consumption::TrustedActorExecutionTimeV1(
                crate::public_consumption::ActorTimeRecordV1 {run_id:execution.actor.opened.run_id.clone(),drive_position:1,logical_time_utc_ms:29999}));
            let first=observation.take_closed_records();
            accepted_records=first.iter().map(|r|serde_json::json!({"record":r.record(),"position":r.position(),"visibility":r.visibility()})).collect();
            assert_eq!(first.len(),3,"实际GTC接纳：Submission、OrderLifecycle和占款Economic");
            for record in &first {ledger.apply_noe_memory_v2(record).unwrap();}
            ledger_accepted=Some(gpu_ledger_reservation_checkpoint(&ledger,"after_real_GTC"));
        }
    }
    assert!(terminal_index.is_some());
    // 非WS元数据无source time，不进hidden；真实终态组先Terminal再Lifecycle。
    assert_eq!(controls.len(),1,"只有Terminal之后的真实Lifecycle");
    assert_eq!(qualified_sources.len(),2,"Terminal与后继Lifecycle均给同一原qualified源");
    assert_eq!(lifecycle_count,if with_order {2} else {1},"每订单关闭加首次Resolved，后继qualified不重复，不自动兑付");
    assert_eq!(qualified_sources[0]["event_id"],qualified_sources[1]["event_id"]);
    assert!(canonical_market_facts_checked > 0);
    assert!(matches!(buckets.last().unwrap().references[0].payload,
        CompactFactPayloadV1::Inline(PublicBusinessPayloadV1::MarketFact {payload:CanonicalMarketFactPayloadV1::Terminal(_),..})));
    assert!(matches!(buckets.last().unwrap().references[1].payload,
        CompactFactPayloadV1::Inline(PublicBusinessPayloadV1::MarketFact {payload:CanonicalMarketFactPayloadV1::LifecycleTransition(_),..})));
    let close_at=41000;
    execution.actor.projection_port.close_receive_prefix(close_at);
    execution.actor.projection_port.observe_public_time(&crate::public_consumption::TrustedActorExecutionTimeV1(
        crate::public_consumption::ActorTimeRecordV1 {run_id:execution.actor.opened.run_id.clone(),drive_position:1,logical_time_utc_ms:close_at}));
    let closed_records=observation.take_closed_records();
    let records=closed_records.iter().map(|r|serde_json::json!({"record":r.record(),
        "position":r.position(),"visibility":r.visibility()})).collect::<Vec<_>>();
    let mut terminal_attempts=Vec::new();
    for record in &closed_records {
        ledger.apply_noe_memory_v2(record).unwrap();
        terminal_attempts.push(serde_json::json!({"position":record.position(),"after":gpu_ledger_reservation_checkpoint(&ledger,"Terminal_prefix")}));
    }
    let ledger_after=gpu_ledger_reservation_checkpoint(&ledger,"after_Resolved");
    let resolutions=ledger.economic_read().unwrap().resolutions().map(|r|serde_json::json!({
        "resolved":r.resolved,"request":r.request,"category":r.settlement_cost_attribution,
        "economic_predecessors_closed":r.economic_predecessors_closed,
        "paired_receivable":r.paired_receivable_pusd,"orphan_receivable":r.winner_orphan_receivable_pusd,
    })).collect::<Vec<_>>();
    assert_eq!(resolutions.len(),1,"同一原NOE进入Ledger只保存一个判定");
    let before_repeat=ledger.projection_v2().unwrap();
    for record in &closed_records {ledger.apply_noe_memory_v2(record).unwrap();}
    assert_eq!(ledger.projection_v2().unwrap(),before_repeat,
        "重读完整原position不产生Ledger条目");
    assert_eq!(records.len(),if with_pending {4} else if with_order {3} else {1},"Terminal订单关闭/经济释放/Resolved，零单仅一条Resolved");
    assert!(observation.take_closed_records().is_empty());
    let mut finality_reference=Vec::new();
    if with_pending {
        assert_eq!(maker_checkpoints.len(),1,"一条真实Maker fill");
        assert_eq!(resolutions[0]["economic_predecessors_closed"],serde_json::json!({"Known":false}));
        for at in [32000,33000] {
            let (next,effects)=advance_finality_to_candidate(state,at,execution.actor._rule_snapshot.snapshot(),execution.actor._finality_policy.as_ref());
            state=next;
            let lifecycle=effects.into_lifecycle_feedback().unwrap();
            state=gpu_accept_projection_reference(&mut execution.actor,state,
                BusinessReducerOutputV2::LogicalDrive(LogicalDriveOutputV2 {lifecycle,..Default::default()}),&mut batches);
            execution.actor.projection_port.close_receive_prefix(50000);
            execution.actor.projection_port.observe_public_time(&crate::public_consumption::TrustedActorExecutionTimeV1(
                crate::public_consumption::ActorTimeRecordV1 {run_id:execution.actor.opened.run_id.clone(),drive_position:1,logical_time_utc_ms:50000}));
            let received=observation.take_closed_records();
            let rows=received.iter().map(|r|serde_json::json!({"record":r.record(),"position":r.position(),"visibility":r.visibility()})).collect::<Vec<_>>();
            for record in &received {ledger.apply_noe_memory_v2(record).unwrap();}
            finality_reference.push(serde_json::json!({"at":at,"records":rows,
                "order":state.session.as_ref().unwrap().execution_state.gpu_order_checkpoint_for_tests("sim-1"),
                "ledger":gpu_ledger_reservation_checkpoint(&ledger,"actual_pending_finality"),
                "resolutions":ledger.economic_read().unwrap().resolutions().map(|r|serde_json::json!({"economic_predecessors_closed":r.economic_predecessors_closed,
                    "paired_receivable":r.paired_receivable_pusd,"orphan_receivable":r.winner_orphan_receivable_pusd})).collect::<Vec<_>>()}));
        }
    }
    let input = serde_json::json!({"assets":assets,"buckets":buckets,"market_end":MARKET_END_AT,"with_pending":with_pending,
        "engineering_finality_path":finality_path,"maker_checkpoints":maker_checkpoints,"finality_reference":finality_reference,
        "original_controls":controls,"qualified_sources":qualified_sources,"resolved_records":records,
        "ledger_before":ledger_before,"ledger_after":ledger_after,"ledger_resolutions":resolutions,
        "next_private":next_private,"next_batch":next_batch,"session_seed":session_seed,"resolved_close_at":close_at,
        "original_batches":batches,"terminal_reference_index":terminal_index.unwrap(),
        "canonical_market_facts_checked":canonical_market_facts_checked,
        "accepted_records":accepted_records,"ledger_accepted":ledger_accepted,"terminal_attempts":terminal_attempts,"post_reference":post_reference,"original_order_after":if with_order {Some(state.session.as_ref().unwrap().execution_state.gpu_order_checkpoint_for_tests("sim-1"))} else {None},
        "source_binding":{"binary":std::env::var("B_GPU_HIDDEN_CLI").unwrap(),"hidden":hidden,"public":source,
            "rows":1,"bytes":1048576,"max_batch_bytes":1048576}});
    let mut child = std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(std::env::var("B_GPU_HIDDEN_REFERENCE_VALIDATOR").unwrap()).stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(), &input).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    eprintln!("gpu_hidden_reference={}", String::from_utf8(output.stdout).unwrap());
}
