//! 同一公共生产链的隐藏流式分支。私有数量仅由来源排序后的 PM 消息归约。
use crate::{hidden_preparation::*, public_recording::RecordingResult};
use contracts::*;
use data_pipeline_core::{pm_reduction::*, public_observation_core::PmOwnedStateV1};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// 同刻参考在 PM 派生之前；PM 位置不依赖尚未知的全期参考数量。
pub const STREAMING_HIDDEN_ORDERING_VERSION: &str = "hidden-reference-before-pm-source-v1";
type PmOrder = (u64, i64, u64, u64);
type BucketOrder = (i64, u8, u64, i64, u64, u64, u32);
#[derive(Serialize, Deserialize)]
pub struct StreamingHiddenStateV1 {
    stream: String,
    source_identity: Sha256Hex,
    epochs: BTreeMap<String, u64>,
    states: BTreeMap<String, PmOwnedStateV1<PreparedPmEvidenceV1<SourceEvidenceRefV3>>>,
    pending_pm: BTreeMap<PmOrder, PreparedPmMessageV1>,
    pending_records: BTreeMap<BucketOrder, ResolvedHiddenRecordV1>,
    next_pm_position: u64,
    next_index_position: u64,
}
#[derive(Serialize, Deserialize)]
pub struct StreamingHiddenBucketV1 {
    pub reference: ClosedStreamingHiddenStepBucketV1,
    pub records: Vec<ResolvedHiddenRecordV1>,
}
impl StreamingHiddenStateV1 {
    pub fn new(stream: String, source_identity: Sha256Hex, epochs: BTreeMap<String, u64>) -> Self {
        Self {
            stream,
            source_identity,
            epochs,
            states: BTreeMap::new(),
            pending_pm: BTreeMap::new(),
            pending_records: BTreeMap::new(),
            next_pm_position: 0,
            next_index_position: 0,
        }
    }
    pub fn enroll(&mut self, context: PmMarketContextV1) -> RecordingResult<()> {
        if self.states.contains_key(&context.market.condition_id.0) {
            return Ok(());
        }
        let context = PmMarketContextV1 {
            market: context.market,
            assets: context.assets,
            identity_source_evidence: context
                .identity_source_evidence
                .into_iter()
                .map(|e| PreparedPmEvidenceV1::prepare(e, None))
                .collect::<Result<_, _>>()?,
            canonical_rule_version: context.canonical_rule_version,
        };
        self.states.insert(
            context.market.condition_id.0.clone(),
            PmOwnedStateV1::new(context),
        );
        Ok(())
    }
    pub fn push(
        &mut self,
        canonical_position: u64,
        record: CanonicalEventRecordV2,
    ) -> RecordingResult<()> {
        match record {
            CanonicalEventRecordV2::PolymarketMessage(message) => {
                self.push_prepared_pm(canonical_position, PreparedPmMessageV1::prepare(message)?);
            }
            record => {
                let at = match &record {
                    CanonicalEventRecordV2::ReferenceEvent(event) => {
                        event.time.source_event_at_utc_ms
                    }
                    CanonicalEventRecordV2::MarketFact(event) => {
                        event.dependency_time.source_event_at_utc_ms
                    }
                    _ => unreachable!(),
                };
                if let Some(at) = at {
                    self.pending_records.insert(
                        (at, 0, 0, 0, 0, canonical_position, 0),
                        ResolvedHiddenRecordV1::Reference {
                            canonical_position,
                            record,
                        },
                    );
                }
            }
        }
        Ok(())
    }
    pub fn push_prepared_pm(&mut self, canonical_position: u64, prepared: PreparedPmMessageV1) {
        let source = &prepared.message().lineage.source_evidence[0];
        let coordinate = source.collector_admission.as_ref().unwrap();
        let key = (
            self.epochs[&coordinate.source_sequence_epoch_id],
            source.source_event_at_utc_ms.unwrap(),
            coordinate.collector_admission_sequence,
            canonical_position,
        );
        self.pending_pm.insert(key, prepared);
    }
    pub fn push_depth_reference(
        &mut self,
        canonical_position: u64,
        binding: PublicFactBindingV1,
        time: EventTimeV2,
    ) {
        if let Some(at) = time.source_event_at_utc_ms {
            self.pending_records.insert(
                (at, 0, 0, 0, 0, canonical_position, 0),
                ResolvedHiddenRecordV1::BinanceDepthReference {
                    canonical_position,
                    binding,
                    time,
                },
            );
        }
    }
    /// pm_lower覆盖全部未读PM；None表示PM来源已完整结束。
    pub fn advance_pm(&mut self, pm_lower: Option<PmOrder>) -> RecordingResult<()> {
        use rayon::prelude::*;
        let mut markets: BTreeMap<String, Vec<(PmOrder, PreparedPmMessageV1)>> = BTreeMap::new();
        while self
            .pending_pm
            .first_key_value()
            .is_some_and(|(key, _)| pm_lower.is_none_or(|lower| *key < lower))
        {
            let (key, message) = self.pending_pm.pop_first().unwrap();
            markets
                .entry(message.message().market.condition_id.0.clone())
                .or_default()
                .push((key, message));
        }
        let work = markets
            .into_iter()
            .map(|(id, messages)| {
                let state = self
                    .states
                    .remove(&id)
                    .ok_or("流式隐藏 PM 缺少原固定市场上下文")?;
                Ok((id, state, messages))
            })
            .collect::<RecordingResult<Vec<_>>>()?;
        let stream = &self.stream;
        let completed = work
            .into_par_iter()
            .map(|(id, mut state, messages)| -> RecordingResult<_> {
                let mut output = Vec::new();
                for (key, prepared) in messages {
                    let result = compute_pm_message_steps_v1(
                        state.context(),
                        &state,
                        &prepared,
                        stream,
                        key.3,
                        false,
                    )?;
                    state.apply(&result);
                    let material = materialize_pm_step_identity_v1(result)?;
                    let steps = material
                        .reduction
                        .steps
                        .into_iter()
                        .zip(material.bindings)
                        .map(|(step, binding)| (step.into_source_step(), binding))
                        .collect::<Vec<_>>();
                    output.push((key, state.context().market.clone(), steps));
                }
                Ok((id, state, output))
            })
            .collect::<Vec<_>>();
        let mut output = Vec::new();
        for completed in completed {
            let (id, state, steps) = completed?;
            self.states.insert(id, state);
            output.extend(steps);
        }
        // 独立市场可以并行，原 PM 派生先后仍按原全局键交接。
        output.sort_by_key(|(key, _, _)| *key);
        for (key, market, steps) in output {
            for (step, binding) in steps {
                let reference = HiddenDerivedStepRefV1 {
                    canonical_fact_id: step.canonical_fact_id.clone(),
                    canonical_record_position: key.3,
                    member_index: step.member_index,
                    step_index: step.step_index,
                    derivation_position: self.next_pm_position,
                    position: binding.position,
                    derived_id: binding.derived_id,
                    dependency_time: step.dependency_time.clone(),
                };
                let at = step.dependency_time.source_event_at_utc_ms.unwrap();
                self.pending_records.insert(
                    (at, 1, key.0, key.1, key.2, key.3, step.step_index),
                    ResolvedHiddenRecordV1::Step {
                        stored: HiddenStoredStepV1 { reference, step },
                        market: market.clone(),
                    },
                );
                self.next_pm_position += 1;
            }
        }
        Ok(())
    }
    /// 所有原始来源、待配对终局和未归约PM共同给出来源时刻下界。
    pub fn next_bucket(
        &mut self,
        unread_lower: Option<i64>,
        applied_prefix: AppliedIngressPrefixV1,
        ended: bool,
    ) -> Option<StreamingHiddenBucketV1> {
        let at = self.pending_records.first_key_value()?.0.0;
        // epoch 内已经按来源时刻排序；只读取每个尚未结束 epoch 的首项。
        let mut pm_at = None;
        let mut remaining = self.pending_pm.range(..);
        while let Some((key, _)) = remaining.next() {
            pm_at = pm_at.into_iter().chain(Some(key.1)).min();
            remaining = self.pending_pm.range((
                std::ops::Bound::Excluded((key.0, i64::MAX, u64::MAX, u64::MAX)),
                std::ops::Bound::Unbounded,
            ));
        }
        let lower = unread_lower.into_iter().chain(pm_at).min();
        if !ended && lower.is_none_or(|lower| lower <= at) {
            return None;
        }
        if ended && lower.is_some_and(|lower| lower <= at) {
            return None;
        }
        let next = self
            .pending_records
            .range((
                std::ops::Bound::Excluded((
                    at,
                    u8::MAX,
                    u64::MAX,
                    i64::MAX,
                    u64::MAX,
                    u64::MAX,
                    u32::MAX,
                )),
                std::ops::Bound::Unbounded,
            ))
            .map(|(key, _)| key)
            .next()
            .map(|key| key.0);
        // NextGlobalEventAt 必须指向真实下一项，不能把 Footer 下界伪装成事件。
        let closure = match next {
            Some(next) if lower.is_none_or(|lower| lower >= next) => {
                ReplayIndexClosureV1::NextGlobalEventAt(next)
            }
            Some(_) => return None,
            None if ended && lower.is_none() => ReplayIndexClosureV1::EndOfInput,
            None => return None,
        };
        let start = self.next_index_position;
        let mut records = Vec::new();
        while self
            .pending_records
            .first_key_value()
            .is_some_and(|(key, _)| key.0 == at)
        {
            records.push(self.pending_records.pop_first().unwrap().1);
            self.next_index_position += 1;
        }
        Some(StreamingHiddenBucketV1 {
            reference: ClosedStreamingHiddenStepBucketV1 {
                source: StreamingHiddenSourceBindingV1 {
                    input_stream_id: applied_prefix.input_stream_id.clone(),
                    input_set_identity_sha256: self.source_identity.clone(),
                    applied_prefix,
                    ordering_version: STREAMING_HIDDEN_ORDERING_VERSION.into(),
                },
                bucket_time_utc_ms: at,
                index_start: start,
                index_end: self.next_index_position,
                closure,
            },
            records,
        })
    }
}
