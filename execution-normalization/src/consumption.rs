//! NOE 所属来源交付能力。耐久读取只有正文，不能生成业务领取引用。
use crate::*;
use contracts::execution_normalization::*;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};

pub type NoeMemoryRecordV3 = NormalizationRecord<
    SourceNeutralTradeLifecycleResultV2<MemoryLogicalIdentityV1>,
    SourceNeutralControlResult<
        SourceNeutralSubmissionOutcomeV2<MemoryLogicalIdentityV1>,
        SourceNeutralOrderSnapshotV2<MemoryLogicalIdentityV1>,
        MemoryLogicalIdentityV1,
    >,
    MemoryLogicalIdentityV1,
    MemoryEconomicFactReferenceV1,
>;
pub type NoeMemoryBatchV3 = NormalizationBatch<
    SourceNeutralTradeLifecycleResultV2<MemoryLogicalIdentityV1>,
    SourceNeutralControlResult<
        SourceNeutralSubmissionOutcomeV2<MemoryLogicalIdentityV1>,
        SourceNeutralOrderSnapshotV2<MemoryLogicalIdentityV1>,
        MemoryLogicalIdentityV1,
    >,
    MemoryLogicalIdentityV1,
    MemoryEconomicFactReferenceV1,
>;
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NoeRecordPositionV1 {
    pub context_reference: ExecutionNormalizationContextReferenceV1,
    pub source_watermark: SimV2SourceWatermarkV1,
    pub record_ordinal: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NoeConsumptionVisibilityV1 {
    Initialization,
    Feedback {
        delivery_sequence: u64,
        available_at_utc_ms: i64,
    },
}
#[derive(Clone, Debug)]
pub struct NoeBatchDurabilityV1(Arc<OnceLock<NoePublishedContainerReferenceV1>>);
impl NoeBatchDurabilityV1 {
    /// 原后台发布者交付实际提交结果；这里不分配提交号。
    pub fn publish(&self, container: &NoePublishedContainerReferenceV1) {
        let _ = self.0.set(container.clone());
    }
}
#[derive(Clone, Debug)]
struct Batch {
    context: Arc<ExecutionNormalizationContextV1>,
    batch: Arc<NoeMemoryBatchV3>,
    durable: NoeBatchDurabilityV1,
}
#[derive(Clone, Debug)]
pub struct NoeConsumedRecordRefV1 {
    batch: Arc<Batch>,
    ordinal: usize,
    visibility: NoeConsumptionVisibilityV1,
}
impl NoeConsumedRecordRefV1 {
    pub fn position(&self) -> NoeRecordPositionV1 {
        NoeRecordPositionV1 {
            context_reference: self.context().context_reference.clone(),
            source_watermark: self.batch.batch.source_watermark.clone(),
            record_ordinal: self.ordinal as u64,
        }
    }
    pub fn context(&self) -> &ExecutionNormalizationContextV1 {
        &self.batch.context
    }
    pub fn record(&self) -> &NoeMemoryRecordV3 {
        &self.batch.batch.records[self.ordinal]
    }
    pub fn visibility(&self) -> &NoeConsumptionVisibilityV1 {
        &self.visibility
    }
    pub fn durable_reference(&self) -> Option<NoeConsumedRecordReferenceV1> {
        self.batch
            .durable
            .0
            .get()
            .map(|container| NoeConsumedRecordReferenceV1 {
                result: NoeResultReferenceV1 {
                    commit_container: container.clone(),
                    record_ordinal: self.ordinal as u64,
                },
                visibility: self.visibility.clone(),
            })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoeConsumedRecordReferenceV1 {
    result: NoeResultReferenceV1,
    visibility: NoeConsumptionVisibilityV1,
}
impl NoeConsumedRecordReferenceV1 {
    pub fn result(&self) -> &NoeResultReferenceV1 {
        &self.result
    }
    pub fn recorded_visibility(&self) -> &NoeConsumptionVisibilityV1 {
        &self.visibility
    }
}
/// 只读耐久视图，刻意没有业务可见性或可应用引用转换。
#[derive(Debug)]
pub struct NoeDurableRecordViewV1 {
    position: NoeRecordPositionV1,
    context: ExecutionNormalizationContextV1,
    record: NoeMemoryRecordV3,
}
impl NoeDurableRecordViewV1 {
    pub fn position(&self) -> NoeRecordPositionV1 {
        self.position.clone()
    }
    pub fn context(&self) -> &ExecutionNormalizationContextV1 {
        &self.context
    }
    pub fn record(&self) -> &NoeMemoryRecordV3 {
        &self.record
    }
}
pub fn read_durable_noe_record_v1(
    reference: &NoeResultReferenceV1,
) -> Result<NoeDurableRecordViewV1, DurableErrorV1> {
    let resolved = read_noe_result_v3(reference)?;
    // 冷读取沿原身份编解码，既不规范化，也不创建领取能力。
    let record = serde_json::to_value(&resolved.record)
        .and_then(serde_json::from_value)
        .map_err(|e| DurableErrorV1::ContainerDecode {
            path: reference.commit_container.location.clone().into(),
            message: e.to_string(),
        })?;
    Ok(NoeDurableRecordViewV1 {
        position: NoeRecordPositionV1 {
            context_reference: resolved.context.context_reference.clone(),
            source_watermark: resolved.boundary.source_watermark,
            record_ordinal: reference.record_ordinal,
        },
        context: resolved.context,
        record,
    })
}

/// 来源适配器持有真实已接纳批；消费侧只获得 observation 返回的单记录能力。
/// 此适配器由运行源拥有，业务消费者不持有，也没有按整数任选记录的接口。
#[derive(Debug, Default)]
pub struct NoeConsumptionSourceV1 {
    opening: Option<NoeConsumedRecordRefV1>,
    pending: Vec<(Arc<Batch>, Vec<usize>)>,
    delivered: u64,
}
impl NoeConsumptionSourceV1 {
    pub fn accept(
        &mut self,
        context: Arc<ExecutionNormalizationContextV1>,
        batch: Arc<NoeMemoryBatchV3>,
    ) -> NoeBatchDurabilityV1 {
        let durable = NoeBatchDurabilityV1(Arc::new(OnceLock::new()));
        let batch = Arc::new(Batch {
            context,
            batch,
            durable: durable.clone(),
        });
        let mut ordinals = Vec::new();
        for (ordinal, record) in batch.batch.records.iter().enumerate() {
            if matches!(record, NormalizationRecord::EconomicFact { fact, .. } if matches!(fact.disposition, SourceNeutralEconomicDispositionV1::AccountOpening { .. }))
            {
                self.opening = Some(NoeConsumedRecordRefV1 {
                    batch: batch.clone(),
                    ordinal,
                    visibility: NoeConsumptionVisibilityV1::Initialization,
                });
            }
            if feedback_coordinate(record).is_some() {
                ordinals.push(ordinal);
            }
        }
        if !ordinals.is_empty() {
            self.pending.push((batch, ordinals));
        }
        durable
    }
    pub fn opening_record(&self) -> Option<NoeConsumedRecordRefV1> {
        self.opening.clone()
    }
    pub fn delivered_through(&self) -> u64 {
        self.delivered
    }
    /// 仅由原受信来源驱动器传递其已闭合接收/公开逻辑时点。
    pub fn take_closed(&mut self, closed: i64, visible: i64) -> Vec<NoeConsumedRecordRefV1> {
        let mut ready = Vec::new();
        for (batch, ordinals) in &mut self.pending {
            ordinals.retain(|ordinal| {
                let record = &batch.batch.records[*ordinal];
                let (received, sequence) =
                    feedback_coordinate(record).expect("来源反馈队列仅含反馈");
                if received <= closed && received <= visible {
                    ready.push((
                        received,
                        sequence,
                        record.source_order().ordinal,
                        batch.clone(),
                        *ordinal,
                    ));
                    false
                } else {
                    true
                }
            });
        }
        self.pending.retain(|(_, ordinals)| !ordinals.is_empty());
        ready.sort_by_key(|(received, sequence, ordinal, _, _)| (*received, *sequence, *ordinal));
        let mut source = None;
        ready
            .into_iter()
            .map(|(received, sequence, _, batch, ordinal)| {
                if source != Some((received, sequence)) {
                    self.delivered = self.delivered.checked_add(1).expect("原交付序不溢出");
                    source = Some((received, sequence));
                }
                NoeConsumedRecordRefV1 {
                    batch,
                    ordinal,
                    visibility: NoeConsumptionVisibilityV1::Feedback {
                        delivery_sequence: self.delivered,
                        available_at_utc_ms: visible,
                    },
                }
            })
            .collect()
    }
}
fn feedback_coordinate(record: &NoeMemoryRecordV3) -> Option<(i64, u64)> {
    match record.source_reference().record {
        SimV2SourceRecordReferenceV1::DirectResponse {
            private_feedback_sequence,
            response_received_at_utc_ms,
            ..
        } => Some((response_received_at_utc_ms, private_feedback_sequence)),
        SimV2SourceRecordReferenceV1::OrderLifecycle {
            private_feedback_sequence,
            feedback_received_at_utc_ms,
            ..
        }
        | SimV2SourceRecordReferenceV1::TradeLifecycle {
            private_feedback_sequence,
            feedback_received_at_utc_ms,
            ..
        }
        | SimV2SourceRecordReferenceV1::SettlementLifecycle {
            private_feedback_sequence,
            feedback_received_at_utc_ms,
            ..
        } => Some((feedback_received_at_utc_ms, private_feedback_sequence)),
        _ => None,
    }
}
