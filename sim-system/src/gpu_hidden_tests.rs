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
    for input in [meta(0),pm(1,1,21000,book("10.0000007")),pm(2,2,21001,down),
        pm(3,3,21002,PolymarketOrderbookEvent::PriceChange {price_changes:vec![change(Some("20.0000007"))]}),
        pm(4,4,21003,PolymarketOrderbookEvent::PriceChange {price_changes:vec![change(Some("20.0000008"))]}),
        recovery,end(6)] {
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
    let mut queues=HiddenQueueReference::new();
    let mut sort=config().sort;sort.merge_workers=8;
    let order=Arc::new(GpuInitialRunOrderV1::start(Path::new(&std::env::var("B_GPU_PYTHON").unwrap()),
        Path::new(&std::env::var("B_GPU_SORT_WORKER").unwrap())).unwrap());
    prepare_compact_hidden_material_v1(&source,&hidden,1<<20,&sort,order,"gpu-hidden-asset-test".into(),None,|_,_,_,_| {}).unwrap();
    let mut reader=CompactHiddenMaterialReaderV1::open(&hidden,&source,1,1<<20,1<<20,|_,_,_,_| {}).unwrap();
    let slots:Vec<_>=assets.iter().flat_map(|a|[(a.token_id.0.clone(),BookSide::Bid,"0.5".parse().unwrap()),
        (a.token_id.0.clone(),BookSide::Ask,"0.6".parse().unwrap())]).collect();
    let mut buckets=Vec::new();let mut expected=Vec::new();
    while let Some(bucket)=next_compact_hidden_gpu_bucket_v1(&mut reader).unwrap() {
        for (market,step) in &bucket.steps {
            if let Some(normalized)=crate::market::normalize_pm_step_v1(step).unwrap() {
                let header=MarketEventHeaderV1 {identity:&step.derived_id.0,market,time:&step.dependency_time,quality:&step.quality};
                original.apply_admitted_header(&header,&normalized,bucket.bucket.bucket_time_utc_ms,MARKET_END_AT).unwrap();
                queues.apply(&normalized);
            }
            let states:Vec<_>=assets.iter().map(|asset|{
                let (initialized,tick,quality)=original.asset_state_for_gpu_tests(CONDITION_ID,&asset.token_id.0);
                serde_json::json!({"initialized":initialized,"tick":tick.map(|v|v.to_string()),"quality":quality})
            }).collect();
            let shares:Vec<_>=slots.iter().map(|(token,side,price)|original.external_quantity_for_tests(CONDITION_ID,token,*side,*price).unwrap().to_string()).collect();
            expected.push(serde_json::json!({"assets":states,"shares":shares,"queues":queues.describe(&slots)}));
        }
        buckets.push(bucket);
    }
    assert!(buckets.iter().flat_map(|b|&b.steps).any(|(_,s)|matches!(s.change,PmStepChangeV1::InitialSnapshot {ref bids,ref asks,..} if bids.is_empty()&&asks.is_empty())));
    let input=serde_json::json!({"assets":assets,"slots":slots.iter().map(|(token,side,p)|serde_json::json!([token,side,p.to_string()])).collect::<Vec<_>>(),
        "buckets":buckets,"expected":expected});
    let mut child=std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(std::env::var("B_GPU_HIDDEN_ASSET_VALIDATOR").unwrap()).stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
    serde_json::to_writer(child.stdin.take().unwrap(),&input).unwrap();
    let output=child.wait_with_output().unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    eprintln!("gpu_hidden_asset_reference={}",String::from_utf8(output.stdout).unwrap());
}
