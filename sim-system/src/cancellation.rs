//! 原取消请求登记的纯控制面叶；CPU和设备薄桥共用去重与原引用。

pub fn cancel_targets_batch_v2(input: &str) -> Result<String, String> {
    use crate::runtime_v2::BusinessRequestV2;
    let requests: Vec<BusinessRequestV2> = serde_json::from_str(input).map_err(|e| e.to_string())?;
    let targets = requests.iter().map(|request| match request {
        BusinessRequestV2::CancelOrder(value) => crate::run_v2::cancel_single_registration_v2(value),
        BusinessRequestV2::CancelOrders(value) => crate::run_v2::cancel_targets_registration_v2(value),
        BusinessRequestV2::CancelMarketOrders(_) => Ok(Vec::new()),
        _ => Err(crate::run_v2::SimRunActorErrorV2::InvalidBusinessRequest("取消薄桥只消费原取消请求")),
    }).collect::<Result<Vec<_>, _>>().map_err(|e| format!("{e:?}"))?;
    serde_json::to_string(&targets).map_err(|e| e.to_string())
}
