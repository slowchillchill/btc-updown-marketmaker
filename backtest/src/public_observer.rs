use crate::MaterialError;
use contracts::{ClosedBusinessGroupV1, PublicGroupReferenceV1, PublicObservationRequirementsV1};
use std::{collections::BTreeMap, sync::Arc};
/// 标量随原公开组运输；逐档明细按市场共享，仅真实请求消费。
#[derive(Debug)]
pub struct HistoricalPublicEvidenceV1 {
    pub scalars: serde_json::Value,
    pub request_evidence: BTreeMap<String, Arc<serde_json::Value>>,
}
/// 显式研究消费者；与策略需求求并集，不改变其权重或业务状态。
pub trait HistoricalPublicObserverV1: Send + Sync + std::fmt::Debug {
    fn requirements(&self) -> PublicObservationRequirementsV1;
    fn observe(
        &self,
        reference: &PublicGroupReferenceV1,
        group: &ClosedBusinessGroupV1,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError>;
    fn record_request_evidence(
        &self,
        evidence: &serde_json::Value,
    ) -> Result<serde_json::Value, MaterialError> {
        Ok(evidence.clone())
    }
    fn finish(&self) -> Result<(), MaterialError> {
        Ok(())
    }
}
