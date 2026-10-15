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
    #[cfg(feature = "stage-timing")]
    timings: DayProducerStageTimingsV1,
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
            #[cfg(feature = "stage-timing")]
            timings: Default::default(),
        })
    }
    pub fn next_applied(&mut self) -> RecordingResult<Option<DayAppliedV1>> {
        #[cfg(feature = "stage-timing")]
        let input_started = std::time::Instant::now();
        let Some(mut input) = self.ingress.next_input()? else {
            return Ok(None);
        };
        #[cfg(feature = "stage-timing")]
        let timing_source = match &input.ingress {
            DptIngressV1::Message {
                source_evidence, ..
            } => match source_evidence.source_kind {
                CanonicalSourceKindV2::BinanceDepthSnapshot => 2,
                CanonicalSourceKindV2::BinanceDepthUpdate => 3,
                CanonicalSourceKindV2::BinanceAggtrade => 1,
                CanonicalSourceKindV2::PolymarketMarketMetadata => 7,
                CanonicalSourceKindV2::PolymarketOrderbook => 8,
                CanonicalSourceKindV2::PolymarketRtdsChainlinkBtcUsd => 4,
                CanonicalSourceKindV2::PolymarketRtdsChainlinkTwap => 5,
            },
            DptIngressV1::EndOfInput => 6,
            DptIngressV1::Aborted { .. } => 0,
        };
        let initialization_binding = self.ingress.initialization_binding().cloned();
        let first_canonical_position = self.core.next_canonical_position();
        #[cfg(feature = "stage-timing")]
        {
            self.timings.inputs += 1;
            self.timings.by_source[timing_source].inputs += 1;
            let elapsed = input_started.elapsed().as_nanos();
            self.timings.decode_ns += elapsed;
            self.timings.by_source[timing_source].stage_ns[0] += elapsed;
        }
        #[cfg(feature = "stage-timing")]
        let prepared_started = std::time::Instant::now();
        let prepared = self.ingress.prepare_input(&self.core, &mut input)?;
        #[cfg(feature = "stage-timing")]
        {
            let elapsed = prepared_started.elapsed().as_nanos();
            self.timings.prepare_ns += elapsed;
            self.timings.by_source[timing_source].stage_ns[1] += elapsed;
        }
        #[cfg(feature = "stage-timing")]
        let core_started = std::time::Instant::now();
        let PublicAppliedIngressV1 {
            applied,
            reductions,
            pm_reductions,
        } = self.core.push_day(&prepared)?;
        // 日链无人消费借用 PM reductions；直接释放，不复制快照档位。
        drop(pm_reductions);
        #[cfg(feature = "stage-timing")]
        {
            let elapsed = core_started.elapsed().as_nanos();
            self.timings.core_ns += elapsed;
            self.timings.by_source[timing_source].stage_ns[2] += elapsed;
        }
        #[cfg(feature = "stage-timing")]
        let material_started = std::time::Instant::now();
        let dependencies = prepared.recorded.dependency_record_refs.clone();
        let prepared_pm = match prepared.current {
            PreparedCanonicalInputV1::PolymarketMessage(pm) => Some(pm),
            _ => None,
        };
        input.dependency_record_refs = dependencies;
        self.ended = matches!(input.ingress, DptIngressV1::EndOfInput);
        let mut contexts = BTreeMap::new();
        let mut materials = BTreeMap::new();
        for batch in reductions {
            for material in materialize_compact_public_reduction_v1(batch)? {
                let position = match &material {
                    CompactCanonicalReferenceV1::Record { binding, .. }
                    | CompactCanonicalReferenceV1::BinanceDepth { binding, .. } => {
                        binding.position.clone()
                    }
                    CompactCanonicalReferenceV1::BinanceDepthPosition { position, .. } => {
                        position.clone()
                    }
                };
                materials.insert(position, material);
            }
        }
        #[cfg(feature = "stage-timing")]
        {
            let elapsed = material_started.elapsed().as_nanos();
            self.timings.material_ns += elapsed;
            self.timings.by_source[timing_source].stage_ns[3] += elapsed;
        }
        #[cfg(feature = "stage-timing")]
        let hidden_started = std::time::Instant::now();
        let mut canonical = first_canonical_position;
        for group in &applied.closed_groups {
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
                    PublicBusinessPayloadV1::PolymarketMessage(_) => self.hidden.push_prepared_pm(
                        canonical,
                        prepared_pm.as_ref().expect("同次公共 PM 准备").clone(),
                    ),
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
                        CompactCanonicalReferenceV1::BinanceDepthPosition { position, time } => {
                            self.hidden.push_depth_position(canonical, position, time)
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
                .next_bucket(lower, applied.applied_prefix.clone(), self.ended)
        {
            hidden.push(bucket);
        }
        #[cfg(feature = "stage-timing")]
        {
            let elapsed = hidden_started.elapsed().as_nanos();
            self.timings.hidden_ns += elapsed;
            self.timings.by_source[timing_source].stage_ns[4] += elapsed;
        }
        Ok(Some(DayAppliedV1 {
            input,
            applied,
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
            #[cfg(feature = "stage-timing")]
            timings: Default::default(),
        })
    }
}

#[cfg(feature = "stage-timing")]
#[derive(Default, serde::Serialize)]
pub struct DayProducerStageTimingsV1 {
    pub inputs: u64,
    pub decode_ns: u128,
    pub prepare_ns: u128,
    pub core_ns: u128,
    pub material_ns: u128,
    pub hidden_ns: u128,
    #[serde(serialize_with = "serialize_source_timings")]
    pub by_source: [DayProducerSourceTimingsV1; 9],
}
#[cfg(feature = "stage-timing")]
#[derive(Default, serde::Serialize)]
pub struct DayProducerSourceTimingsV1 {
    pub inputs: u64,
    /// 顺序与既有五阶段一致：decode、prepare、core、material、hidden。
    pub stage_ns: [u128; 5],
}
#[cfg(feature = "stage-timing")]
impl DayProducerV1<'_> {
    pub fn stage_timings(&self) -> &DayProducerStageTimingsV1 {
        &self.timings
    }
}

#[cfg(feature = "stage-timing")]
fn serialize_source_timings<S: serde::Serializer>(
    samples: &[DayProducerSourceTimingsV1; 9],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeMap;
    const NAMES: [&str; 9] = [
        "aborted",
        "binance_aggtrade",
        "binance_depth_snapshot",
        "binance_depth_update",
        "chainlink_spot",
        "chainlink_twap",
        "end_of_input",
        "polymarket_market_metadata",
        "polymarket_orderbook",
    ];
    let mut map = serializer.serialize_map(None)?;
    for (name, sample) in NAMES
        .into_iter()
        .zip(samples)
        .filter(|(_, sample)| sample.inputs > 0)
    {
        map.serialize_entry(name, sample)?;
    }
    map.end()
}
