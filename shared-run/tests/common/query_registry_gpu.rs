//! 原连续SIM规范入口的普通Query公开引用与独立Registry读面。
use super::*;

pub(super) fn frame(
    frames: &mut Vec<serde_json::Value>, stage: &str, at: i64,
    registry: &SharedExecutionRegistryV1,
) {
    if std::env::var_os("B_GPU_QUERY_REGISTRY_VALIDATOR").is_none() { return; }
    fn source(r: &::execution_normalization::NoeConsumedRecordRefV1) -> serde_json::Value {
        serde_json::json!({"record":r.record(),"position":r.position(),"context":r.context(),"visibility":r.visibility()})
    }
    let orders=registry.orders().map(|o| {
        let target=o.origin.as_ref().map(|origin| {
            let intent=origin.intent.intent();
            let leg=match intent {
                ModelIntentV1::SubmitSingleBuy {order,..}=>order,
                ModelIntentV1::SubmitPairedBuy {orders,..}=>&orders[origin.batch_item_ordinal as usize],
                _=>panic!("原Query夹具订单来源"),
            };
            match &leg.order {
                BuyOrderIntentV1::GtcLimit {size_shares,..}|BuyOrderIntentV1::GtdLimit {size_shares,..}=>*size_shares,
                _=>panic!("当前工程场景仅Base查询"),
            }
        });
        serde_json::json!({
        "identity":o.order_identity,"condition":o.condition_id,"token":o.token_id,
        "base_applicable":o.base_quantity_applicability,"matched":o.matched_shares,
        "active":o.active_unfilled_shares,"cancellable":o.cancellable,
        "origin_known":o.origin.is_some(),"original_target":target,
        "accepted":o.accepted.map(source),"formed":o.formed.map(source),
        "latest_lifecycle":o.latest_lifecycle.map(source),"latest_direct":o.latest_direct_feedback.map(source),
        "latest_cancel":o.latest_cancel_feedback.map(source),
        "trades":registry.order_trades(o.order_identity).map(source).collect::<Vec<_>>(),
    })}).collect::<Vec<_>>();
    frames.push(serde_json::json!({"stage":stage,"cutoff":at,"orders":orders}));
}

pub(super) fn finish(frames: Vec<serde_json::Value>) {
    let Ok(validator)=std::env::var("B_GPU_QUERY_REGISTRY_VALIDATOR") else {return};
    let artifact=std::path::PathBuf::from(std::env::var("B_GPU_QUERY_REGISTRY_ARTIFACT").unwrap());
    assert!(artifact.starts_with("/root/autodl-tmp"));
    std::fs::create_dir(&artifact).expect("新证据目录，不覆盖原失败");
    let source=artifact.join("original-query-registry.json");
    std::fs::write(&source,serde_json::to_vec_pretty(&frames).unwrap()).unwrap();
    let status=std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
        .arg(validator).arg(source).arg(&artifact)
        .stdout(std::fs::File::create(artifact.join("GPU.stdout")).unwrap())
        .stderr(std::fs::File::create(artifact.join("GPU.stderr")).unwrap())
        .status().unwrap();
    std::fs::write(artifact.join("GPU.exit"),status.code().unwrap_or(-1).to_string()).unwrap();
    assert!(status.success(),"{}",std::fs::read_to_string(artifact.join("GPU.stderr")).unwrap());
}
