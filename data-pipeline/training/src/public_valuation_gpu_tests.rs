//! 有界真实PM来源→原observer→compact→设备估值；不运行训练根或全窗重放。
use super::*;
use std::collections::BTreeMap;
use crate::public_recording::tests::{metadata_snapshot_row,row,book};
use data_pipeline_core::{public_input::prepare_public_ingress_v1,
    public_reference::source_evidence_v3_from_typed_raw_v2,
    typed_raw::{TypedRawRow,PolymarketOrderbookEvent as Event,
        PolymarketPriceChangeEntry,PolymarketPriceLevel,PolymarketSide}};

#[test]
#[ignore = "只在已同步准确提交的AutoDL CUDA环境显式运行"]
fn gpu_public_valuation_versions_match_original_core() {
    let artifact_root=PathBuf::from(std::env::var("B_GPU_VALUATION_ARTIFACT_ROOT")
        .expect("指定AutoDL持久估值证据根"));
    assert!(artifact_root.starts_with("/root/autodl-tmp"),"估值证据须位于AutoDL数据盘");
    fs::create_dir(&artifact_root).expect("证据根须不存在，不覆盖原现场");
    let quote=|bid:Option<&str>|Event::BestBidAsk {asset_id:"20".into(),
        best_bid:bid.map(str::to_owned),best_ask:Some("0.6".into()),spread:None};
    let change=|price:&str,size:&str|Event::PriceChange {price_changes:vec![PolymarketPriceChangeEntry {
        asset_id:"20".into(),side:PolymarketSide::Buy,price:price.into(),size:Some(size.into()),
        best_bid:None,best_ask:None}]};
    let source=|epoch:&str,sequence:u64,at:i64,event|TypedRawRowV2::Existing(
        TypedRawRow::PolymarketOrderbook(row(epoch,sequence,at,40_000+sequence.min(100) as i64,event)));
    let mut full=book("1");
    if let Event::Book {bids,..}=&mut full {
        bids.push(PolymarketPriceLevel {price:"0.3".into(),size:"1".into()});
    }
    let mut empty=book("1");
    if let Event::Book {bids,..}=&mut empty {bids.clear();}
    let mut scenarios=vec![
        ("version_empty_epoch",vec![source("epoch-a",1,21_000,book("1")),
            source("epoch-a",2,23_000,quote(Some("0.4"))),source("epoch-a",3,22_000,book("2")),
            source("epoch-a",4,24_000,change("0.1","3")),source("epoch-a",5,25_000,quote(None)),
            source("epoch-a",6,24_500,quote(Some("0.5"))),source("epoch-b",1,20_000,quote(Some("0.4"))),
            source("epoch-b",2,26_000,empty),source("epoch-b",3,27_000,quote(Some("0.4"))),
            source("epoch-b",4,28_000,change("0.1","0")),
            source("epoch-c",1,29_000,Event::LastTradePrice {asset_id:"20".into(),price:"0.5".into(),
                size:None,side:None,fee_rate_bps:None})]),
        ("unrelated_delete_and_real_quote_delete",vec![source("epoch-a",1,21_000,full.clone()),
            source("epoch-a",2,23_000,quote(Some("0.4"))),source("epoch-a",3,24_000,change("0.9","0")),
            source("epoch-a",4,25_000,change("0.5","0")),source("epoch-a",5,26_000,change("0.4","2")),
            source("epoch-a",6,27_000,change("0.4","0")),source("epoch-a",7,28_000,change("0.55","1"))]),
        ("low_quantity_does_not_refresh_direct",vec![source("epoch-a",1,21_000,full),
            source("epoch-a",2,23_000,quote(Some("0.4"))),source("epoch-a",3,24_000,change("0.4","0")),
            source("epoch-a",4,25_000,change("0.5","2")),source("epoch-a",5,26_000,change("0.4","1"))]),
    ];
    // 原合法PM准入跨u32边界及同消息member来源；不缩小u64或合并None/Some。
    let rows=&mut scenarios[0].1;
    rows.push(source("epoch-c",u64::from(u32::MAX),30_000,quote(Some("0.45"))));
    rows.push(source("epoch-c",u64::from(u32::MAX)+1,30_000,quote(Some("0.46"))));
    rows.push(source("epoch-c",u64::from(u32::MAX)+2,30_000,Event::PriceChange {price_changes:vec![
        PolymarketPriceChangeEntry {asset_id:"20".into(),side:PolymarketSide::Buy,price:"0.46".into(),
            size:Some("1".into()),best_bid:Some("0.46".into()),best_ask:Some("0.6".into())},
        PolymarketPriceChangeEntry {asset_id:"20".into(),side:PolymarketSide::Buy,price:"0.47".into(),
            size:Some("1".into()),best_bid:Some("0.47".into()),best_ask:Some("0.6".into())}]}));
    for (name,mut sources) in scenarios {
        let scenario_root=artifact_root.join(name);
        fs::create_dir(&scenario_root).unwrap();
        let target=scenario_root.join("valuation-compact");
        let header=PublicInputStreamHeaderV1 {input_stream_id:format!("gpu-valuation-{name}"),
            canonical_rule_version:"rule-v1".into(),observation_version:"public-observation-v1".into(),
            ordering_version:"original-input-v1".into(),input_scope:"constructed-short-valuation".into(),
            max_pending_ws_terminals:2,initial_state:PublicInitialStateV1::Empty};
        sources.insert(0,TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata_snapshot_row()))));
        let mut core=PublicObservationCoreV1::new(header.clone()).unwrap();
        let mut observer=PublicObservationCoreV1::new(header.clone()).unwrap();
        let mut writer=PublicCompactWriterV1::create(&target,header.clone()).unwrap();
        let mut references=vec![];let mut prefix=0_u64;
        for (position,source) in sources.into_iter().map(Some).chain(std::iter::once(None)).enumerate() {
            let ingress=source.map_or(DptIngressV1::EndOfInput,|row|DptIngressV1::Message {
                source_evidence:source_evidence_v3_from_typed_raw_v2(&row).unwrap(),row});
            let input=RecordedIngressV1 {input_stream_id:header.input_stream_id.clone(),input_position:position as u64,
                ingress,dependency_record_refs:vec![]};
            let ids=BTreeMap::new();
            let prepared=prepare_public_ingress_v1(&core,&input,&ids,&[]).unwrap();
            let applied=core.push_facts(&prepared).unwrap();writer.push(&input,&applied).unwrap();
            let prepared=prepare_public_ingress_v1(&observer,&input,&ids,&[]).unwrap();
            let observed=observer.push(&prepared).unwrap();
            for group in &observed.applied.closed_groups {
                for observation in &group.observations {
                    let valuation_presence:Vec<_>=observation.assets.iter()
                        .map(|asset|asset.valuation_best_bid.value.is_some()).collect();
                    references.push(serde_json::json!({"phase":format!("{name}/input-{position}"),
                        "prefix":prefix,"observation":observation,"valuation_presence":valuation_presence}));
                }
                prefix+=1;
            }
        }
        writer.publish().unwrap();
        let reference=scenario_root.join("valuation-reference.json");
        serde_json::to_writer(File::create(&reference).unwrap(),&references).unwrap();
        let status=std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
            .arg(std::env::var("B_GPU_STATE_VALIDATOR").unwrap())
            .args(["--binary",&std::env::var("B_GPU_COLUMNS_BINARY").unwrap()])
            .arg("--material").arg(&target).arg("--valuation-reference").arg(reference)
            .arg("--device-page").arg(scenario_root.join("valuation-page"))
            .arg("--output").arg(scenario_root.join("valuation-report.json")).status().unwrap();
        assert!(status.success(),"{name}");
    }
}
