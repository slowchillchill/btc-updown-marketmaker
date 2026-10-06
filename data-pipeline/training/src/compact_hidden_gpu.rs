//! 完整hidden桶的原始材料批次接口；保持原引用/步骤/参考顺序，SIM拥有设备状态。
//! 此接口供冷转换，不承诺JSON证据树就是最终共享二进制列或活动根恢复。
use crate::{compact_hidden_material::{ClosedCompactHiddenBucketV1, CompactHiddenMaterialReaderV1},
    compact_public::CompactFactV1, public_recording::RecordingResult};
use contracts::{MarketIdentityV2, SourceEvidenceRefV3};
use data_pipeline_core::pm_reduction::PmReductionStepV1;
use serde::Serialize;

#[derive(Serialize)]
pub struct CompactHiddenGpuBucketV1 {
    pub version: &'static str,
    pub bucket: ClosedCompactHiddenBucketV1,
    /// 原hidden归约步骤，不得使用public PM归约步骤替代。
    pub steps: Vec<(MarketIdentityV2, PmReductionStepV1<'static, SourceEvidenceRefV3>)>,
    /// 桶内参考的原完整事实；同时间先于步骤，不能因暂时不驱动撮合而删除。
    pub references: Vec<CompactFactV1>,
}

pub fn next_compact_hidden_gpu_bucket_v1(reader: &mut CompactHiddenMaterialReaderV1)
    -> RecordingResult<Option<CompactHiddenGpuBucketV1>> {
    let Some(bucket) = reader.next_bucket()? else {return Ok(None);};
    let mut references = Vec::with_capacity(bucket.reference_positions.len());
    for position in &bucket.reference_positions {references.push(reader.reference_fact(*position)?);}
    let mut steps = Vec::with_capacity(bucket.steps.len());
    for reference in &bucket.steps {steps.push(reader.restore_step_with_market(reference)?);}
    Ok(Some(CompactHiddenGpuBucketV1 {version:"compact_hidden_gpu_bucket_v1",bucket,steps,references}))
}
