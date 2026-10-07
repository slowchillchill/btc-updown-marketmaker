//! 原两个Native账户场景的有限冷读面对照；不把CPU业务调度作为GPU宿主。
use super::*;
use std::{collections::BTreeMap, fs, path::{Path,PathBuf}};
use data_pipeline_training::{public_recording::{PublicRecordingReaderV1,PublicStoredRowV1},
    compact_public::PublicCompactWriterV1};
use data_pipeline_core::{public_input::prepare_public_ingress_v1,
    public_observation_core::PublicObservationCoreV1};
pub(crate) type Materials=Arc<std::sync::Mutex<Vec<LedgerRecordMaterialV2>>>;

pub(crate) fn bind(execution:&mut PublicSimExecutionV1,ledger:&mut LedgerWriterV1,path:&Path)->Option<Materials> {
    if std::env::var_os("B_GPU_NAV_VALIDATOR").is_none() && std::env::var_os("B_GPU_ACTOR_CANCEL_REFERENCE").is_none() {
        execution.bind_ledger_recording_v2(ledger,path).unwrap();return None;
    }
    let materials=Arc::new(std::sync::Mutex::new(Vec::new()));
    let output=materials.clone();
    ledger.bind_record_sink_v2(Box::new(move |m|output.lock().unwrap().push(m)));
    Some(materials)
}

pub(crate) fn checkpoint(shared:&SharedExecutionV2,before:&ActionBeforeReadV2,
    group:Option<&NativeInputGroup>,materials:&Option<Materials>)->serde_json::Value {
    let read=shared.ledger().economic_read().unwrap();
    assert_eq!(read.base(),before.ledger(),"导出必须属于原ActionBefore同一次Ledger前缀");
    let records=materials.as_ref().unwrap().lock().unwrap().iter().filter_map(|m|{
        let record=m.consumed_record_v2()?;
        let sequence=m.ledger_entry_seq().unwrap();
        (sequence<=before.ledger().ledger_entry_seq).then(||serde_json::json!({
            "ledger_entry_seq":sequence,"position":record.position(),"context":record.context(),
            "visibility":record.visibility(),"record":record.record()}))
    }).collect::<Vec<_>>();
    serde_json::json!({"cutoff":before.cutoff().decision_time().logical_time_utc_ms(),
        "projection":read.base(),"cash":read.cash(),"residual":{
            "directional_cash_pusd":read.residual_cash().directional_cash_pusd,
            "other_cash_pusd":read.residual_cash().other_cash_pusd},
        "successful_trades":read.successful_trades().map(|v|serde_json::json!({
            "trade_identity":v.trade_identity,"fact":v.terminal})).collect::<Vec<_>>(),
        "costs":read.settlement_costs().map(|v|serde_json::json!({
            "fact":v.fact,"attribution":v.attribution})).collect::<Vec<_>>(),
        "resolutions":read.resolutions().map(|v|serde_json::json!({
            "resolved":v.resolved,"request":v.request,"attribution":v.settlement_cost_attribution})).collect::<Vec<_>>(),
        "rebate_days":read.rebate_days().map(|v|serde_json::json!({"kind":v.kind,
            "attribution_day_start_utc_ms":v.attribution_day_start_utc_ms,
            "contributions":v.contributions,"amounts":v.amounts})).collect::<Vec<_>>(),
        "economics":before.economics(),"records":records,
        "native":group.map(|g|serde_json::to_value(g).unwrap())})
}

pub(super) fn validate(name:&str,root:&Path,checkpoints:Vec<serde_json::Value>) {
    let Ok(validator)=std::env::var("B_GPU_NAV_VALIDATOR") else{return};
    let artifact=PathBuf::from(std::env::var("B_GPU_NAV_ARTIFACT_ROOT").unwrap()).join(name);
    assert!(artifact.starts_with("/root/autodl-tmp"));
    fs::create_dir(&artifact).expect("保留原证据，不覆盖旧目录");
    let compact=artifact.join("public-compact");
    build_compact(root,&compact);
    let source=serde_json::json!({"name":name,"compact":compact,"checkpoints":checkpoints});
    let reference=artifact.join("original-account-reference.json");
    fs::write(&reference,serde_json::to_vec_pretty(&source).unwrap()).unwrap();
    let status=std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(validator).arg(&reference).arg(&artifact)
        .stdout(fs::File::create(artifact.join("GPU.stdout")).unwrap())
        .stderr(fs::File::create(artifact.join("GPU.stderr")).unwrap()).status().unwrap();
    fs::write(artifact.join("GPU.exit"),status.code().unwrap_or(-1).to_string()).unwrap();
    assert!(status.success(),"{}",fs::read_to_string(artifact.join("GPU.stderr")).unwrap());
    eprintln!("gpu_nav_report={}",artifact.join("NAV-report.json").display());
}

pub(crate) fn build_compact(root:&Path,compact:&Path) {
    let mut reader=PublicRecordingReaderV1::recover(root,16,1<<20).unwrap();
    let mut core=PublicObservationCoreV1::new(reader.header.clone()).unwrap();
    let mut writer=PublicCompactWriterV1::create(&compact,reader.header.clone()).unwrap();
    while let Some(row)=reader.next_row().unwrap() {
        match row {
            PublicStoredRowV1::Input(input)=>{
                let ids=BTreeMap::new();
                let prepared=prepare_public_ingress_v1(&core,&input,&ids,&[]).unwrap();
                let applied=core.push_facts(&prepared).unwrap();
                writer.push_prepared(&input,&prepared,&applied).unwrap();
            },
            PublicStoredRowV1::SourceBinding(binding)=>writer.source_binding(&binding).unwrap(),
            _=>{},
        }
    }
    writer.publish().unwrap();
}
