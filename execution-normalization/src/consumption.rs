//! NOE 所属消费引用及受信宿主接线。耐久视图与消费引用保持直接类型区分。
//! 公开生产者接口不提供不可伪造来源保证；宿主须传真实批及驱动时点，不能重建耐久数据伪造交付。
mod mapping;

use crate::*;
use contracts::execution_normalization::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

pub type NoeMemoryRecord<R> = NormalizationRecord<
    SourceNeutralTradeLifecycleResultV2<MemoryLogicalIdentityV1>,
    SourceNeutralControlResult<
        SourceNeutralSubmissionOutcomeV2<MemoryLogicalIdentityV1>,
        SourceNeutralOrderSnapshotV2<MemoryLogicalIdentityV1>,
        MemoryLogicalIdentityV1,
        R,
    >,
    MemoryLogicalIdentityV1,
    MemoryEconomicFactReferenceV1,
>;
pub type NoeMemoryBatch<R> = NormalizationBatch<
    SourceNeutralTradeLifecycleResultV2<MemoryLogicalIdentityV1>,
    SourceNeutralControlResult<
        SourceNeutralSubmissionOutcomeV2<MemoryLogicalIdentityV1>,
        SourceNeutralOrderSnapshotV2<MemoryLogicalIdentityV1>,
        MemoryLogicalIdentityV1,
        R,
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
    ControlAdmission,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Batch<R> {
    context: Arc<ExecutionNormalizationContextV1>,
    batch: Arc<NoeMemoryBatch<R>>,
    durable: NoeBatchDurabilityV1,
}
/// 原批及已建立可见性的借用；私有字段不等于任意公开生产者调用的来源认证。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NoeConsumedRecordRef<R> {
    batch: Arc<Batch<R>>,
    ordinal: usize,
    visibility: NoeConsumptionVisibilityV1,
}
impl<R: Clone + std::fmt::Debug> NoeConsumedRecordRef<R> {
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
    pub fn record(&self) -> &NoeMemoryRecord<R> {
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
pub struct NoeDurableRecordView<R> {
    position: NoeRecordPositionV1,
    context: ExecutionNormalizationContextV1,
    record: NoeMemoryRecord<R>,
}
impl<R: Clone + std::fmt::Debug> NoeDurableRecordView<R> {
    pub fn position(&self) -> NoeRecordPositionV1 {
        self.position.clone()
    }
    pub fn context(&self) -> &ExecutionNormalizationContextV1 {
        &self.context
    }
    pub fn record(&self) -> &NoeMemoryRecord<R> {
        &self.record
    }
}
pub fn read_durable_noe_record_v1(
    reference: &NoeResultReferenceV1,
) -> Result<NoeDurableRecordViewV1, DurableErrorV1> {
    let resolved = read_noe_result_v3(reference)?;
    durable_record_view(reference, resolved)
}
pub fn read_durable_noe_record_v2(
    reference: &NoeResultReferenceV1,
) -> Result<NoeDurableRecordViewV2, DurableErrorV1> {
    durable_record_view(reference, read_noe_result_v4(reference)?)
}
fn durable_record_view<R>(
    reference: &NoeResultReferenceV1,
    resolved: ResolvedNoeResult<
        SourceNeutralTradeLifecycleResultV2,
        SourceNeutralControlResult<
            SourceNeutralSubmissionOutcomeV2,
            SourceNeutralOrderSnapshotV2,
            StableLogicalIdentityV1,
            R,
        >,
    >,
) -> Result<NoeDurableRecordView<R>, DurableErrorV1> {
    let record = mapping::map_normalization_record(resolved.record).map_err(|e| {
        DurableErrorV1::ContainerDecode {
            path: reference.commit_container.location.clone().into(),
            message: e.to_string(),
        }
    })?;
    Ok(NoeDurableRecordView {
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
/// 正式接线由运行源拥有；只接受本运行的实际规范化批和真实驱动时点。
/// 这些公开方法用于受信进程内协作：调用方仍可手工重建批并自填时点取得引用，
/// 因此来源真实性由宿主接线负责，不以该类型、私有字段或直接类型编译反例证明。
#[derive(Debug, Serialize, Deserialize)]
pub struct NoeConsumptionSource<R> {
    opening: Option<NoeConsumedRecordRef<R>>,
    admitted: Vec<NoeConsumedRecordRef<R>>,
    #[serde(with = "ordered_feedback")]
    pending: BTreeMap<(i64, u64, u32), Vec<NoeConsumedRecordRef<R>>>,
    delivered: u64,
}
impl<R: Clone + std::fmt::Debug + PartialEq> NoeConsumptionSource<R> {
    /// 开户事实经真实归约及原 NOE 规范化产生即为 Initialization，不等待后台交接或耐久。
    pub fn accept(
        &mut self,
        context: Arc<ExecutionNormalizationContextV1>,
        batch: Arc<NoeMemoryBatch<R>>,
    ) -> NoeBatchDurabilityV1 {
        let durable = NoeBatchDurabilityV1(Arc::new(OnceLock::new()));
        let batch = Arc::new(Batch {
            context,
            batch,
            durable: durable.clone(),
        });
        for (ordinal, record) in batch.batch.records.iter().enumerate() {
            if matches!(record, NormalizationRecord::EconomicFact { fact, .. } if matches!(fact.disposition, SourceNeutralEconomicDispositionV1::AccountOpening { .. }))
            {
                self.opening = Some(NoeConsumedRecordRef {
                    batch: batch.clone(),
                    ordinal,
                    visibility: NoeConsumptionVisibilityV1::Initialization,
                });
            }
            if matches!(
                record,
                NormalizationRecord::Evidence {
                    evidence: crate::SourceNeutralEvidenceKind::InitialMarket { .. }
                        | crate::SourceNeutralEvidenceKind::MarketEnrollment { .. },
                    ..
                }
            ) {
                self.admitted.push(NoeConsumedRecordRef {
                    batch: batch.clone(),
                    ordinal,
                    visibility: NoeConsumptionVisibilityV1::ControlAdmission,
                });
            }
            if let Some((received, sequence)) = feedback_coordinate(record) {
                self.pending
                    .entry((received, sequence, record.source_order().ordinal))
                    .or_default()
                    .push(NoeConsumedRecordRef {
                        batch: batch.clone(),
                        ordinal,
                        visibility: NoeConsumptionVisibilityV1::Initialization,
                    });
            }
        }
        durable
    }
    /// 真实同步接纳的目录事实；不编造接收反馈坐标或初始化 UTC。
    pub fn take_admitted_records(&mut self) -> Vec<NoeConsumedRecordRef<R>> {
        std::mem::take(&mut self.admitted)
    }
    pub fn opening_record(&self) -> Option<NoeConsumedRecordRef<R>> {
        self.opening.clone()
    }
    pub fn delivered_through(&self) -> u64 {
        self.delivered
    }
    /// 仅由原受信来源驱动器传递其已闭合接收/公开逻辑时点。
    pub fn take_closed(&mut self, closed: i64, visible: i64) -> Vec<NoeConsumedRecordRef<R>> {
        let cutoff = closed.min(visible);
        let mut ready = Vec::new();
        while self
            .pending
            .first_key_value()
            .is_some_and(|(key, _)| key.0 <= cutoff)
        {
            let (key, records) = self.pending.pop_first().expect("存在可领取前缀");
            ready.extend(records.into_iter().map(|r| (key, r)));
        }
        let mut source = None;
        ready
            .into_iter()
            .map(|((received, sequence, _), mut record)| {
                if source != Some((received, sequence)) {
                    self.delivered = self.delivered.checked_add(1).expect("原交付序不溢出");
                    source = Some((received, sequence));
                }
                record.visibility = NoeConsumptionVisibilityV1::Feedback {
                    delivery_sequence: self.delivered,
                    available_at_utc_ms: visible,
                };
                record
            })
            .collect()
    }
}

// 私有恢复队列只编码一次原批；有序坐标引用批内 ordinal，不复制经济事实。
mod ordered_feedback {
    use super::*;
    type Coordinate = (i64, u64, u32);
    type Entry = (Coordinate, Vec<(usize, usize, NoeConsumptionVisibilityV1)>);
    #[derive(Serialize, Deserialize)]
    struct Stored<R> {
        batches: Vec<Arc<Batch<R>>>,
        entries: Vec<Entry>,
    }
    pub fn serialize<S: serde::Serializer, R: Serialize>(
        queue: &BTreeMap<Coordinate, Vec<NoeConsumedRecordRef<R>>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut ids = BTreeMap::new();
        let mut batches = Vec::new();
        let entries = queue
            .iter()
            .map(|(coordinate, records)| {
                let refs = records
                    .iter()
                    .map(|r| {
                        let pointer = Arc::as_ptr(&r.batch) as usize;
                        let index = *ids.entry(pointer).or_insert_with(|| {
                            let i = batches.len();
                            batches.push(r.batch.clone());
                            i
                        });
                        (index, r.ordinal, r.visibility.clone())
                    })
                    .collect();
                (*coordinate, refs)
            })
            .collect();
        Stored { batches, entries }.serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>, R: Deserialize<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<Coordinate, Vec<NoeConsumedRecordRef<R>>>, D::Error> {
        let stored = Stored::<R>::deserialize(deserializer)?;
        stored
            .entries
            .into_iter()
            .map(|(coordinate, entries)| {
                let refs = entries
                    .into_iter()
                    .map(|(index, ordinal, visibility)| {
                        let batch = stored
                            .batches
                            .get(index)
                            .filter(|b| ordinal < b.batch.records.len())
                            .ok_or_else(|| serde::de::Error::custom("反馈队列原批引用越界"))?;
                        Ok(NoeConsumedRecordRef {
                            batch: batch.clone(),
                            ordinal,
                            visibility,
                        })
                    })
                    .collect::<Result<Vec<_>, D::Error>>()?;
                Ok((coordinate, refs))
            })
            .collect()
    }
}
fn feedback_coordinate<R: Clone + PartialEq>(record: &NoeMemoryRecord<R>) -> Option<(i64, u64)> {
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

impl<R> Default for NoeConsumptionSource<R> {
    fn default() -> Self {
        Self {
            opening: None,
            admitted: Vec::new(),
            pending: BTreeMap::new(),
            delivered: 0,
        }
    }
}
pub type NoeMemoryRecordV3 = NoeMemoryRecord<NoCancellationRequestCompletionV0>;
pub type NoeMemoryRecordV4 = NoeMemoryRecord<CancellationRequestCompletedV1>;
pub type NoeMemoryBatchV3 = NoeMemoryBatch<NoCancellationRequestCompletionV0>;
pub type NoeMemoryBatchV4 = NoeMemoryBatch<CancellationRequestCompletedV1>;
pub type NoeConsumedRecordRefV1 = NoeConsumedRecordRef<NoCancellationRequestCompletionV0>;
pub type NoeConsumedRecordRefV2 = NoeConsumedRecordRef<CancellationRequestCompletedV1>;
pub type NoeDurableRecordViewV1 = NoeDurableRecordView<NoCancellationRequestCompletionV0>;
pub type NoeDurableRecordViewV2 = NoeDurableRecordView<CancellationRequestCompletedV1>;
pub type NoeConsumptionSourceV1 = NoeConsumptionSource<NoCancellationRequestCompletionV0>;
pub type NoeConsumptionSourceV2 = NoeConsumptionSource<CancellationRequestCompletedV1>;

// 恢复保留原发布结果，不分配新提交号，也不把未领取耐久记录签发为反馈。
impl Serialize for NoeBatchDurabilityV1 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.get().serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for NoeBatchDurabilityV1 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let published = Option::<NoePublishedContainerReferenceV1>::deserialize(deserializer)?;
        let value = Self(Arc::new(OnceLock::new()));
        if let Some(container) = published {
            value.publish(&container);
        }
        Ok(value)
    }
}
