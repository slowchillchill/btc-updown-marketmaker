use crate::MaterialError;
use contracts::{ClosedBusinessGroupV1, PublicGroupReferenceV1, PublicObservationRequirementsV1};
use std::{collections::BTreeMap, sync::Arc};
/// 原市场键到必要研究标量的持久映射；冻结组共享未变化节点。
pub type HistoricalPublicScalarsV1 = imbl::OrdMap<String, serde_json::Value>;
/// 标量随原公开组运输；逐档明细按市场共享，仅真实请求消费。
#[derive(Debug)]
pub struct HistoricalPublicEvidenceV1 {
    pub scalars: HistoricalPublicScalarsV1,
    pub request_evidence: BTreeMap<String, Arc<dyn HistoricalRequestEvidenceV1>>,
}
/// 显式研究消费者；与策略需求求并集，不改变其权重或业务状态。
pub trait HistoricalPublicObserverV1: Send + Sync + std::fmt::Debug {
    /// 在日期装配阶段绑定一次；默认观察器本身已经绑定，不增加逐记录路由。
    fn for_stream(
        &self,
        _stream: &str,
    ) -> Result<Option<Arc<dyn HistoricalPublicObserverV1>>, MaterialError> {
        Ok(None)
    }
    fn requirements(&self) -> PublicObservationRequirementsV1;
    fn observe(
        &self,
        reference: &PublicGroupReferenceV1,
        group: &ClosedBusinessGroupV1,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError>;
    /// 实际组可见时钟由原生产入口交付，来源接收时钟不能代替它。
    fn observe_at(
        &self,
        reference: &PublicGroupReferenceV1,
        group: &ClosedBusinessGroupV1,
        _visible_at_utc_ms: i64,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError> {
        self.observe(reference, group)
    }
    fn record_request_evidence(
        &self,
        requests: &[HistoricalOriginalRequestV1],
    ) -> Result<serde_json::Value, MaterialError> {
        Ok(serde_json::Value::Array(
            requests
                .iter()
                .map(|r| r.value())
                .collect::<Result<_, _>>()?,
        ))
    }
    /// 原生产 EOF 只结束公共输入覆盖，候选仍可写冻结请求；这里不关闭输出。
    fn production_finished(&self) -> Result<(), MaterialError> {
        Ok(())
    }
    fn finish(&self) -> Result<(), MaterialError> {
        Ok(())
    }
}

/// 仅真实初始请求消费逐档正文，冻结对象随原公共组持有。
pub trait HistoricalRequestEvidenceV1: Send + Sync + std::fmt::Debug {
    fn value(&self) -> Result<serde_json::Value, MaterialError>;
    fn recorded(&self) -> &std::sync::atomic::AtomicBool;
}

pub struct HistoricalOriginalRequestV1 {
    pub group: PublicGroupReferenceV1,
    pub condition_id: contracts::ConditionId,
    pub blb: Arc<dyn HistoricalRequestEvidenceV1>,
}
impl HistoricalOriginalRequestV1 {
    pub fn value(&self) -> Result<serde_json::Value, MaterialError> {
        Ok(
            serde_json::json!({"group":self.group,"condition_id":self.condition_id,"blb":self.blb.value()?}),
        )
    }
}
