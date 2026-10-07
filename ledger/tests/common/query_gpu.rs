//! 原stage3实际Ledger/NOE序列的有限设备消费者对照；不驱动SIM查询。
use super::*;
use std::{fs,path::PathBuf};

pub(super) struct Export {
    label: &'static str,
    initial: serde_json::Value,
    steps: Vec<serde_json::Value>,
}

impl Export {
    pub(super) fn begin(label: &'static str,writer:&LedgerWriterV1)->Option<Self> {
        std::env::var_os("B_GPU_ORDER_QUERY_VALIDATOR").map(|_|Self {
            label,initial:serde_json::to_value(writer.projection().unwrap()).unwrap(),steps:Vec::new()})
    }

    pub(super) fn local(export:&mut Option<Self>,writer:&LedgerWriterV1,action:serde_json::Value) {
        if let Some(export)=export {
            export.steps.push(serde_json::json!({"action":action,"expected":writer.projection().unwrap()}));
        }
    }

    pub(super) fn record(export:&mut Option<Self>,writer:&LedgerWriterV1,reference:&NoeResultReferenceV1) {
        if let Some(export)=export {
            let source=execution_normalization::read_noe_result_v1(reference).unwrap();
            // 原V1读取已返回真实提交边界；恢复位置接口不支持这个V1 context。
            let position=execution_normalization::NoeRecordPositionV1 {
                context_reference:source.boundary.context_reference.clone(),
                source_watermark:source.boundary.source_watermark.clone(),
                record_ordinal:reference.record_ordinal,
            };
            export.steps.push(serde_json::json!({"action":{"Noe":{
                "position":position,"context":source.context,"record":source.record,
                "durable_available_at":source.boundary.durable_available_at_utc_ms}},
                "expected":writer.projection().unwrap()}));
        }
    }

    pub(super) fn finish(export:Option<Self>) {
        let Some(export)=export else{return};
        let root=PathBuf::from(std::env::var("B_GPU_ORDER_QUERY_ARTIFACT_ROOT").unwrap());
        assert!(root.starts_with("/root/autodl-tmp"));
        let artifact=root.join(export.label);
        fs::create_dir(&artifact).expect("保留已有证据，不覆盖");
        let source=artifact.join("original-Ledger-NOE.json");
        fs::write(&source,serde_json::to_vec_pretty(&serde_json::json!({
            "label":export.label,"initial":export.initial,"steps":export.steps})).unwrap()).unwrap();
        let status=std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
            .arg(std::env::var("B_GPU_ORDER_QUERY_VALIDATOR").unwrap()).arg(source).arg(&artifact)
            .stdout(fs::File::create(artifact.join("GPU.stdout")).unwrap())
            .stderr(fs::File::create(artifact.join("GPU.stderr")).unwrap()).status().unwrap();
        fs::write(artifact.join("GPU.exit"),status.code().unwrap_or(-1).to_string()).unwrap();
        assert!(status.success(),"{}",fs::read_to_string(artifact.join("GPU.stderr")).unwrap());
    }
}
