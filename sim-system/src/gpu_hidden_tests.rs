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
#[test]
#[ignore="AutoDL真实紧凑TWAP引用→设备循环；市场事实保持等待"]
fn gpu_compact_hidden_reference_population_closes_without_fabricating_market_effects() {
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
    for input in [meta(0), record(1, TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference(30, 1))),
        record(2, TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference(60, 2))),
        record(3, TypedRawRowV2::PolymarketRtdsChainlinkTwap(missing)),
        pm(4, 1, 22000, empty(UP_TOKEN_ID)), pm(5, 2, 22000, empty(DOWN_TOKEN_ID)),
        record(6, TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(terminal_metadata_row(&market()))))),
        record(7, TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(terminal_orderbook_row(&market())))), end(8)] {
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
    assert_eq!(buckets.iter().map(|b| b.bucket.bucket_time_utc_ms).collect::<Vec<_>>(), vec![21000,22000,31000]);
    assert_eq!(buckets[0].references.len(), 2);
    assert!(buckets[0].steps.is_empty());
    assert!(buckets[0].references.iter().all(|r| matches!(r.payload,
        CompactFactPayloadV1::Inline(PublicBusinessPayloadV1::ReferenceFact(CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(_))))));
    assert!(buckets[2].references.iter().any(|r| matches!(r.payload,
        CompactFactPayloadV1::Inline(PublicBusinessPayloadV1::MarketFact { .. }))));
    let scenario = sealed_scenario_for_markets(vec![crate::scenario::PlatformMarketV1::try_new(
        market(), assets.clone().try_into().unwrap()).unwrap()]);
    let mut original = crate::market::MarketStateStoreV2::new(scenario.initial_market_directory());
    let mut controls = Vec::new();
    let mut reference_index = 0;
    let mut terminal_index = None;
    for bucket in &buckets {
        for reference in &bucket.references {
            if let CompactFactPayloadV1::Inline(PublicBusinessPayloadV1::MarketFact { payload, .. }) = &reference.payload {
                if matches!(payload, CanonicalMarketFactPayloadV1::Terminal(_)) {
                    terminal_index = Some(reference_index);
                    break;
                }
                let fact = &market_facts[&reference.position];
                if let Some(normalized) = crate::market::normalize_public_market_fact_v1(payload, &fact.quality).unwrap() {
                    let header = crate::market::MarketEventHeaderV1 {identity:&fact.event_id.0, market:&fact.market,
                        time:&fact.dependency_time, quality:&fact.quality};
                    original.apply_admitted_header(&header, &normalized, bucket.bucket.bucket_time_utc_ms, MARKET_END_AT).unwrap();
                }
                let (mapped, trading, quality) = original.control_state_for_gpu_tests(CONDITION_ID);
                controls.push(serde_json::json!({"reference_index":reference_index, "mapped":mapped,
                    "trading":trading, "quality":quality}));
            }
            reference_index += 1;
        }
        if terminal_index.is_some() { break; }
        for (market, step) in &bucket.steps {
            if let Some(normalized) = crate::market::normalize_pm_step_v1(step).unwrap() {
                let header = crate::market::MarketEventHeaderV1 {identity:&step.derived_id.0, market,
                    time:&step.dependency_time, quality:&step.quality};
                original.apply_admitted_header(&header, &normalized, bucket.bucket.bucket_time_utc_ms, MARKET_END_AT).unwrap();
            }
        }
    }
    assert!(terminal_index.is_some());
    // 非WS元数据无source time，不进hidden；真实终态组先Terminal再Lifecycle。
    assert!(controls.is_empty());
    assert!(canonical_market_facts_checked > 0);
    assert!(matches!(buckets[2].references[0].payload,
        CompactFactPayloadV1::Inline(PublicBusinessPayloadV1::MarketFact {payload:CanonicalMarketFactPayloadV1::Terminal(_),..})));
    assert!(matches!(buckets[2].references[1].payload,
        CompactFactPayloadV1::Inline(PublicBusinessPayloadV1::MarketFact {payload:CanonicalMarketFactPayloadV1::LifecycleTransition(_),..})));
    let input = serde_json::json!({"assets":assets,"buckets":buckets,"market_end":MARKET_END_AT,
        "original_controls":controls,"terminal_reference_index":terminal_index.unwrap(),
        "canonical_market_facts_checked":canonical_market_facts_checked,
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
