//! 单日唯一公共 Core；公共专用消费跳过隐藏模拟材料，只交付本次应用结果。
use crate::{day_stream::*, hidden_stream::*, public_recording::RecordingResult};
use contracts::*;
use data_pipeline_core::{
    pm_reduction::*, public_input::*, public_observation_core::*, public_reduction::*,
};
use std::collections::{BTreeMap, VecDeque};

pub struct DayProducerV1<'a> {
    pub core: PublicObservationCoreV1,
    pub ingress: DayIngressV1<'a>,
    pub hidden: StreamingHiddenStateV1,
    ended: bool,
    public_only: bool,
    queued: VecDeque<PrefetchedInputV1>,
    queued_error: Option<String>,
    #[cfg(feature = "stage-timing")]
    timings: DayProducerStageTimingsV1,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct DayPublicTradeV1 {
    pub market: MarketIdentityV2,
    pub position: PublicFactPositionV1,
    pub member_index: Option<u32>,
    pub step_index: u32,
    pub canonical_fact_id: CanonicalEventId,
    pub trade: PublicTradeV2,
    pub source: SourceEvidenceRefV3,
    pub direct_time: EventTimeV2,
    pub quality: EventQualityV2,
}
pub struct DayAppliedV1 {
    /// 仅公共消费模式交付正式接纳的新成交；不是最近成交快照。
    pub new_public_trades: Vec<DayPublicTradeV1>,
    pub input: RecordedIngressV1,
    pub applied: AppliedIngressV1,
    pub first_canonical_position: u64,
    pub prepared_pm: Option<PreparedPmMessageV1>,
    pub contexts: BTreeMap<String, PmMarketContextV1<PreparedPmEvidenceV1<SourceEvidenceRefV3>>>,
    pub initialization_binding: Option<SourceArtifactBindingV1>,
    pub hidden: Vec<StreamingHiddenBucketV1>,
}
// 只预读有限普通 PM 前缀。下界属于每项的原读取位置，不使用批末下界。
struct PrefetchedInputV1 {
    input: RecordedIngressV1,
    initialization_binding: Option<SourceArtifactBindingV1>,
    source_lower: Result<Option<i64>, String>,
    pm_lower: Option<(u64, i64, u64, u64)>,
    prepared_pm: Option<Result<PreparedPmMessageV1, data_pipeline_core::ContractError>>,
    #[cfg(feature = "stage-timing")]
    decode_ns: u128,
    #[cfg(feature = "stage-timing")]
    prepare_ns: u128,
}
fn ordinary_pm(input: &RecordedIngressV1) -> bool {
    matches!(&input.ingress, DptIngressV1::Message {row:TypedRawRowV2::Existing(data_pipeline_core::typed_raw::TypedRawRow::PolymarketOrderbook(row)),..} if !matches!(row.event,data_pipeline_core::typed_raw::PolymarketOrderbookEvent::MarketResolved {..}))
}
// 批次限制统计原消息实际载荷字符及容器；准备材料随本批消费释放。
fn pm_payload_bytes(input: &RecordedIngressV1) -> usize {
    use data_pipeline_core::typed_raw::{PolymarketOrderbookEvent as Event, TypedRawRow};
    let DptIngressV1::Message {
        row: TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)),
        ..
    } = &input.ingress
    else {
        return 0;
    };
    let option = |s: &Option<String>| s.as_ref().map_or(0, String::len);
    let payload = match &row.event {
        Event::Book {
            asset_id,
            bids,
            asks,
            last_trade_price,
        } => {
            asset_id.len()
                + option(last_trade_price)
                + bids
                    .iter()
                    .chain(asks)
                    .map(|l| std::mem::size_of_val(l) + l.price.len() + l.size.len())
                    .sum::<usize>()
        }
        Event::PriceChange { price_changes } => price_changes
            .iter()
            .map(|p| {
                std::mem::size_of_val(p)
                    + p.asset_id.len()
                    + p.price.len()
                    + option(&p.size)
                    + option(&p.best_bid)
                    + option(&p.best_ask)
            })
            .sum(),
        Event::TickSizeChange {
            asset_id,
            old_tick_size,
            new_tick_size,
        } => asset_id.len() + option(old_tick_size) + new_tick_size.len(),
        Event::LastTradePrice {
            asset_id,
            price,
            size,
            fee_rate_bps,
            ..
        } => asset_id.len() + price.len() + option(size) + option(fee_rate_bps),
        Event::BestBidAsk {
            asset_id,
            best_bid,
            best_ask,
            spread,
        } => asset_id.len() + option(best_bid) + option(best_ask) + option(spread),
        Event::NewMarket {
            clob_market_id,
            question,
            description,
            asset_ids,
            outcomes,
            event_message,
        }
        | Event::MarketResolved {
            clob_market_id,
            question,
            description,
            asset_ids,
            outcomes,
            event_message,
            ..
        } => {
            clob_market_id.len()
                + option(question)
                + option(description)
                + asset_ids
                    .iter()
                    .chain(outcomes)
                    .map(String::len)
                    .sum::<usize>()
                + event_message.as_ref().map_or(0, |e| {
                    e.id.len()
                        + option(&e.ticker)
                        + option(&e.slug)
                        + option(&e.title)
                        + option(&e.description)
                })
        }
    };
    std::mem::size_of_val(input)
        + std::mem::size_of_val(row)
        + row.market.len()
        + option(&row.slug)
        + row.collector_host_id.len()
        + row.collector_region.len()
        + row.source_endpoint.len()
        + row.schema_version.len()
        + row.source_sequence_epoch_id.len()
        + payload
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
            public_only: false,
            queued: VecDeque::new(),
            queued_error: None,
            #[cfg(feature = "stage-timing")]
            timings: Default::default(),
        })
    }
    /// 无账户宿主只需要公共归约，不准备隐藏模拟材料。
    pub fn new_public_only(
        ingress: DayIngressV1<'a>,
        header: PublicInputStreamHeaderV1,
        input_identity: Sha256Hex,
    ) -> RecordingResult<Self> {
        let mut producer = Self::new(ingress, header, input_identity, String::new())?;
        producer.public_only = true;
        Ok(producer)
    }
    fn fill_queue(&mut self) -> RecordingResult<()> {
        use rayon::prelude::*;
        if !self.queued.is_empty() {
            return Ok(());
        }
        if let Some(error) = self.queued_error.take() {
            return Err(error.into());
        }
        let (rows, bytes) = self.ingress.read_batch_limits();
        let mut retained = 0;
        loop {
            #[cfg(feature = "stage-timing")]
            let started = std::time::Instant::now();
            let input = match self.ingress.next_input() {
                Ok(Some(input)) => input,
                Ok(None) => break,
                Err(error) if !self.queued.is_empty() => {
                    self.queued_error = Some(error.to_string());
                    break;
                }
                Err(error) => return Err(error),
            };
            #[cfg(feature = "stage-timing")]
            let decode_ns = started.elapsed().as_nanos();
            let pm = ordinary_pm(&input);
            retained += pm_payload_bytes(&input);
            // 原路径在当前 Core 应用后才消费下界错误，预读不能提前报告它。
            let source_lower = self
                .ingress
                .unread_source_lower_bound()
                .map_err(|e| e.to_string());
            self.queued.push_back(PrefetchedInputV1 {
                input,
                initialization_binding: self.ingress.initialization_binding().cloned(),
                source_lower,
                pm_lower: self.ingress.unread_pm_lower_bound(),
                prepared_pm: None,
                #[cfg(feature = "stage-timing")]
                decode_ns,
                #[cfg(feature = "stage-timing")]
                prepare_ns: 0,
            });
            // 多读一项才能识别非PM边界；它保持未应用，且不跨越其固定身份变化准备。
            if !pm || self.queued.len() >= rows.max(1) || retained >= bytes {
                break;
            }
        }
        #[cfg(feature = "stage-timing")]
        let batch_started = std::time::Instant::now();
        let core = &self.core;
        let prepare = |entry: &mut PrefetchedInputV1| {
            if !ordinary_pm(&entry.input) {
                return;
            }
            #[cfg(feature = "stage-timing")]
            let started = std::time::Instant::now();
            entry.prepared_pm = Some(
                prepare_public_ingress_v1(core, &entry.input, &BTreeMap::new(), &[]).map(
                    |p| match p.current {
                        PreparedCanonicalInputV1::PolymarketMessage(pm) => pm,
                        _ => unreachable!("普通PM消息"),
                    },
                ),
            );
            #[cfg(feature = "stage-timing")]
            {
                entry.prepare_ns = started.elapsed().as_nanos();
            }
        };
        let entries = self.queued.make_contiguous();
        let pm_items = entries
            .iter()
            .filter(|entry| ordinary_pm(&entry.input))
            .count();
        if pm_items > 1 {
            entries.par_iter_mut().for_each(prepare);
        } else {
            entries.iter_mut().for_each(prepare);
        }
        #[cfg(feature = "stage-timing")]
        {
            if pm_items > 0 {
                self.timings.pm_preparation_batches += 1;
                self.timings.parallel_pm_preparation_batches += u64::from(pm_items > 1);
                self.timings.pm_preparation_items += pm_items as u64;
                self.timings.pm_preparation_max_batch =
                    self.timings.pm_preparation_max_batch.max(pm_items as u64);
                self.timings.pm_preparation_batch_wall_ns += batch_started.elapsed().as_nanos();
            }
        }
        Ok(())
    }
    pub fn next_applied(&mut self) -> RecordingResult<Option<DayAppliedV1>> {
        self.fill_queue()?;
        let Some(entry) = self.queued.pop_front() else {
            return Ok(None);
        };
        let PrefetchedInputV1 {
            mut input,
            initialization_binding,
            source_lower,
            pm_lower,
            prepared_pm,
            #[cfg(feature = "stage-timing")]
            decode_ns,
            #[cfg(feature = "stage-timing")]
            prepare_ns,
        } = entry;
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
        let first_canonical_position = self.core.next_canonical_position();
        #[cfg(feature = "stage-timing")]
        {
            self.timings.inputs += 1;
            self.timings.by_source[timing_source].inputs += 1;
            let elapsed = decode_ns;
            self.timings.decode_ns += elapsed;
            self.timings.by_source[timing_source].stage_ns[0] += elapsed;
        }
        #[cfg(feature = "stage-timing")]
        let prepared_started = std::time::Instant::now();
        let prepared = if let Some(pm) = prepared_pm {
            let pm = pm?;
            PreparedIngressV1 {
                prepared_group_id: Some(pm.message().group_id.clone()),
                recorded: RecordedIngressViewV1 {
                    input_stream_id: input.input_stream_id.clone(),
                    input_position: input.input_position,
                    ingress: &input.ingress,
                    dependency_record_refs: input.dependency_record_refs.clone(),
                },
                current: PreparedCanonicalInputV1::PolymarketMessage(pm),
                dependencies: std::borrow::Cow::Borrowed(&[]),
                reference_selection: None,
                reference_selection_market: None,
            }
        } else {
            self.ingress.prepare_input(&self.core, &mut input)?
        };
        #[cfg(feature = "stage-timing")]
        {
            let elapsed = prepare_ns + prepared_started.elapsed().as_nanos();
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
        #[cfg(feature = "stage-timing")]
        {
            let elapsed = core_started.elapsed().as_nanos();
            self.timings.core_ns += elapsed;
            self.timings.by_source[timing_source].stage_ns[2] += elapsed;
        }
        let mut new_public_trades = Vec::new();
        if self.public_only {
            for (position, reduction) in pm_reductions {
                for step in reduction.steps {
                    if let PmStepChangeV1::PublicTrade { trade } = step.change {
                        let market = applied
                            .closed_groups
                            .iter()
                            .flat_map(|g| g.records.iter())
                            .find_map(|fact| {
                                if fact.position != position {
                                    return None;
                                }
                                match &fact.payload {
                                    PublicBusinessPayloadV1::PolymarketMessage(pm) => {
                                        Some(pm.market.clone())
                                    }
                                    _ => None,
                                }
                            })
                            .expect("正式PM归约对应已关闭公共事实");
                        new_public_trades.push(DayPublicTradeV1 {
                            market,
                            position: position.clone(),
                            member_index: step.member_index,
                            step_index: step.step_index,
                            canonical_fact_id: step.canonical_fact_id,
                            trade,
                            source: step.direct.source().clone(),
                            direct_time: step.direct_time,
                            quality: step.quality,
                        });
                    }
                }
            }
            return Ok(Some(DayAppliedV1 {
                input,
                applied,
                first_canonical_position,
                new_public_trades,
                prepared_pm: None,
                contexts: BTreeMap::new(),
                initialization_binding,
                hidden: Vec::new(),
            }));
        }
        drop(pm_reductions);
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
        self.hidden.advance_pm(pm_lower)?;
        let lower = source_lower
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?
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
            new_public_trades,
            input,
            applied,
            first_canonical_position,
            prepared_pm,
            contexts,
            initialization_binding,
            hidden,
        }))
    }
}

#[cfg(feature = "stage-timing")]
#[derive(Default, serde::Serialize)]
pub struct DayProducerStageTimingsV1 {
    pub pm_preparation_batches: u64,
    pub parallel_pm_preparation_batches: u64,
    pub pm_preparation_items: u64,
    pub pm_preparation_max_batch: u64,
    pub pm_preparation_batch_wall_ns: u128,
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
