//! 单日唯一公共 Core 与隐藏私有状态；只交付本次应用结果。
use crate::{
    ValidatedDirectSourceParquetFileV3, day_stream::*, hidden_stream::*,
    public_recording::RecordingResult,
};
use contracts::*;
use data_pipeline_core::{
    pm_reduction::*, public_input::*, public_observation_core::*, public_reduction::*,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub struct DayProducerV1<'a> {
    pub core: PublicObservationCoreV1,
    pub ingress: DayIngressV1<'a>,
    pub hidden: StreamingHiddenStateV1,
    ended: bool,
}
pub struct DayAppliedV1 {
    pub input: RecordedIngressV1,
    pub applied: AppliedIngressV1,
    pub first_canonical_position: u64,
    pub prepared_pm: Option<PreparedPmMessageV1>,
    pub contexts: BTreeMap<String, PmMarketContextV1<PreparedPmEvidenceV1<SourceEvidenceRefV3>>>,
    pub initialization_binding: Option<SourceArtifactBindingV1>,
    pub hidden: Vec<StreamingHiddenBucketV1>,
}
#[derive(Serialize, Deserialize)]
pub struct DayProducerCheckpointV1 {
    ingress: DayIngressCheckpointV1,
    hidden: StreamingHiddenStateV1,
    ended: bool,
}
impl<'a> DayProducerV1<'a> {
    pub fn new(
        ingress: DayIngressV1<'a>,
        header: PublicInputStreamHeaderV1,
        input_identity: Sha256Hex,
        hidden_stream: String,
    ) -> RecordingResult<Self> {
        let epochs = ingress
            .epochs()
            .iter()
            .filter(|((source, _), _)| *source == 1)
            .map(|((_, epoch), rank)| (epoch.clone(), *rank))
            .collect();
        Ok(Self {
            core: PublicObservationCoreV1::new(header)?,
            ingress,
            hidden: StreamingHiddenStateV1::new(hidden_stream, input_identity, epochs),
            ended: false,
        })
    }
    pub fn next_applied(&mut self) -> RecordingResult<Option<DayAppliedV1>> {
        let Some(mut input) = self.ingress.next_input()? else {
            return Ok(None);
        };
        let initialization_binding = self.ingress.initialization_binding().cloned();
        let first_canonical_position = self.core.next_canonical_position();
        let prepared = self.ingress.prepare_input(&self.core, &mut input)?;
        let applied = self.core.push(&prepared)?.into_owned();
        let dependencies = prepared.recorded.dependency_record_refs.clone();
        let prepared_pm = match prepared.current {
            PreparedCanonicalInputV1::PolymarketMessage(pm) => Some(pm),
            _ => None,
        };
        input.dependency_record_refs = dependencies;
        self.ended = matches!(input.ingress, DptIngressV1::EndOfInput);
        let mut contexts = BTreeMap::new();
        let mut materials = BTreeMap::new();
        for batch in applied.reductions {
            for material in materialize_compact_public_reduction_v1(batch)? {
                let position = match &material {
                    CompactCanonicalReferenceV1::Record { binding, .. }
                    | CompactCanonicalReferenceV1::BinanceDepth { binding, .. } => {
                        binding.position.clone()
                    }
                };
                materials.insert(position, material);
            }
        }
        let mut canonical = first_canonical_position;
        for group in &applied.applied.closed_groups {
            for fact in group.records.iter() {
                let market = match &fact.payload {
                    PublicBusinessPayloadV1::PolymarketMessage(pm) => Some(&pm.market),
                    PublicBusinessPayloadV1::MarketFact { market, .. } => Some(market),
                    _ => None,
                };
                if let Some(market) = market {
                    if let Some(context) = self.core.prepared_pm_context(&market.condition_id.0) {
                        contexts.insert(market.condition_id.0.clone(), context);
                    }
                    if let Some(context) = self.core.pm_context(&market.condition_id.0) {
                        self.hidden.enroll(context)?;
                    }
                }
                match &fact.payload {
                    PublicBusinessPayloadV1::PolymarketMessage(pm) => self.hidden.push(
                        canonical,
                        CanonicalEventRecordV2::PolymarketMessage(pm.clone()),
                    )?,
                    _ => match materials
                        .remove(&fact.position)
                        .expect("同次归约对应原关闭事实")
                    {
                        CompactCanonicalReferenceV1::Record { record, .. } => {
                            self.hidden.push(canonical, record)?
                        }
                        CompactCanonicalReferenceV1::BinanceDepth { binding, time } => {
                            self.hidden.push_depth_reference(canonical, binding, time)
                        }
                    },
                }
                canonical += 1;
            }
        }
        self.hidden
            .advance_pm(self.ingress.unread_pm_lower_bound())?;
        let lower = self
            .ingress
            .unread_source_lower_bound()?
            .into_iter()
            .chain(self.core.pending_terminal_source_lower_bound())
            .min();
        let mut hidden = Vec::new();
        while let Some(bucket) =
            self.hidden
                .next_bucket(lower, applied.applied.applied_prefix.clone(), self.ended)
        {
            hidden.push(bucket);
        }
        Ok(Some(DayAppliedV1 {
            input,
            applied: applied.applied,
            first_canonical_position,
            prepared_pm,
            contexts,
            initialization_binding,
            hidden,
        }))
    }
    pub fn write_checkpoint(&self, writer: impl std::io::Write) -> RecordingResult<()> {
        #[derive(Serialize)]
        struct Saved<'a> {
            ingress: DayIngressCheckpointV1,
            hidden: &'a StreamingHiddenStateV1,
            ended: bool,
        }
        ciborium::ser::into_writer(
            &Saved {
                ingress: self.ingress.checkpoint(),
                hidden: &self.hidden,
                ended: self.ended,
            },
            writer,
        )?;
        Ok(())
    }
    pub fn restore(
        core: PublicObservationCoreV1,
        files: &[&'a ValidatedDirectSourceParquetFileV3],
        reader: impl std::io::Read,
        rows: usize,
        bytes: usize,
        range: std::ops::Range<i64>,
    ) -> RecordingResult<Self> {
        let saved: DayProducerCheckpointV1 = ciborium::de::from_reader(reader)?;
        Ok(Self {
            core,
            ingress: DayIngressV1::restore(files, saved.ingress, rows, bytes, range)?,
            hidden: saved.hidden,
            ended: saved.ended,
        })
    }
}
