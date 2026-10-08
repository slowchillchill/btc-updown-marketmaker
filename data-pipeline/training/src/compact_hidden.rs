//! compact原事实直接供给独立hidden准备；不要求旧public recording或旧prepare产物。
use crate::{
    compact_public::{CompactFactPayloadV1, CompactFactV1, CompactPmMessageHeaderV1, PublicCompactReaderV1},
    hidden_preparation::HiddenOrderedReducerV1,
    historical_preparation_progress::{
        HistoricalPreparationNoticeV1 as Notice, HistoricalPreparationNotifierV1,
        HistoricalPreparationStageV1 as Stage, notify,
    },
    public_recording::RecordingResult,
    ExternalSorter, InitialRunOrder, PartCursor, SortConfig, SortMetrics, SortRow,
};
use contracts::*;
use data_pipeline_core::{
    pm_reduction::{MaterializedPmReductionV1, PmMarketContextV1, PreparedPmMessageV1},
    public_input::{PreparedCanonicalInputV1, prepare_ingress_v1},
    reduction::PolymarketAssetContextV1,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs, io, path::Path, sync::Arc};

/// 冷准备sink写材料/设备列，不推进账户或执行。失败时不能发布部分产物。
/// reference位置先于全部PM步骤；message的derivation_start沿原hidden规则分配。
pub trait CompactHiddenSinkV1 {
    fn reference(&mut self, canonical_position: u64, source_at_utc_ms: i64,
        derivation_position: u64, fact: &CompactFactV1) -> RecordingResult<()>;
    fn message(&mut self, canonical_position: u64, derivation_start: u64,
        prepared: &PreparedPmMessageV1, material: MaterializedPmReductionV1<'_>) -> RecordingResult<()>;
}

#[derive(Serialize, Deserialize)]
pub struct CompactHiddenSummaryV1 {
    /// 自身来源是compact完整验收前缀，不冒充旧recording的CommittedPrefix。
    pub source_prefix: AppliedIngressPrefixV1,
    pub consumer_stream_id: String,
    pub canonical_facts: u64,
    pub messages: u64,
    pub references: u64,
    pub steps: u64,
    pub reports: u64,
    pub message_sort: SortMetrics,
}

struct InitialContext {
    market: MarketIdentityV2,
    assets: Vec<(AssetIdentityV1, Option<ExactDecimal>)>,
    identity_sources: Vec<SourceEvidenceRefV3>,
}
impl InitialContext {
    fn prepared_context(&self, version: &str) -> RecordingResult<PmMarketContextV1> {
        let assets = self.assets.iter().map(|(asset, tick)| {
            Ok(PolymarketAssetContextV1 {
                asset: asset.clone(),
                initial_tick_size_pusd_per_share: tick.clone()
                    .ok_or_else(|| io::Error::other("原固定身份缺首次InitialTick"))?,
            })
        }).collect::<RecordingResult<Vec<_>>>()?;
        Ok(PmMarketContextV1 {
            market: self.market.clone(), assets,
            identity_source_evidence: self.identity_sources.clone(),
            canonical_rule_version: version.into(),
        })
    }
}

fn message_header(message: &CanonicalPmMessageV1) -> CompactPmMessageHeaderV1 {
    CompactPmMessageHeaderV1 {
        schema_version: message.schema_version.clone(), event_id: message.event_id.clone(),
        market: message.market.clone(), group_id: message.group_id.clone(), lineage: message.lineage.clone(),
        time: message.time.clone(), input_record: message.input_record.clone(),
    }
}

/// 调用方显式配置共享CPU外归并配额，初始run排序使用已绑定GPU worker。
/// 嵌套原类型解析及有状态公共算法仍由原Rust入口执行；不重放模型或观察全盘口。
pub fn reduce_compact_hidden_v1(
    reader: &mut PublicCompactReaderV1,
    work_root: &Path,
    config: &SortConfig,
    initial_order: Arc<dyn InitialRunOrder>,
    consumer_stream_id: String,
    sink: &mut impl CompactHiddenSinkV1,
    progress: Option<&HistoricalPreparationNotifierV1<'_>>,
) -> RecordingResult<CompactHiddenSummaryV1> {
    notify(progress, Notice::Stage(Stage::HiddenScanAndSupply));
    fs::create_dir(work_root)?;
    let messages_root = work_root.join("messages");
    fs::create_dir(&messages_root)?;
    let mut messages = ExternalSorter::with_initial_order(&messages_root, config, Some(initial_order))?;
    let version = reader.manifest.header.canonical_rule_version.clone();
    let mut contexts = BTreeMap::<String, InitialContext>::new();
    let mut epochs = BTreeMap::new();
    let mut canonical_position = 0_u64;
    let mut reference_order = 0_u64;
    let mut supplied = 0_u64;
    while let Some(packet) = reader.next_packet()? {
        notify(progress, Notice::HiddenScanRow);
        for fact in packet.groups.iter().flat_map(|group| &group.records) {
            if let CompactFactPayloadV1::Inline(PublicBusinessPayloadV1::MarketFact { market, payload }) = &fact.payload {
                match payload {
                    CanonicalMarketFactPayloadV1::FixedIdentity { assets } => {
                        contexts.entry(market.condition_id.0.clone()).or_insert_with(|| InitialContext {
                            market: market.clone(), assets: assets.iter().cloned().map(|asset| (asset, None)).collect(),
                            identity_sources: fact.lineage.source_evidence.clone(),
                        });
                    }
                    CanonicalMarketFactPayloadV1::InitialTick { asset, tick_size_pusd_per_share } => {
                        let context = contexts.get_mut(&market.condition_id.0)
                            .ok_or_else(|| io::Error::other("InitialTick缺原FixedIdentity"))?;
                        let (_, tick) = context.assets.iter_mut().find(|(identity, _)| identity == asset)
                            .ok_or_else(|| io::Error::other("InitialTick资产不属于原FixedIdentity"))?;
                        if tick.is_none() { *tick = Some(tick_size_pusd_per_share.clone()); }
                    }
                    _ => {}
                }
            }
            match &fact.payload {
                CompactFactPayloadV1::PmFromInput(header) => {
                    let context = contexts.get(&header.market.condition_id.0)
                        .ok_or_else(|| io::Error::other("hidden PM事实缺初始身份"))?
                        .prepared_context(&version)?;
                    // 当前input直接复用；关闭组引用较早输入时按文件offset读取，不缓存输入全集。
                    let previous;
                    let input = if header.input_record.input_position == packet.input.input_position {
                        &packet.input
                    } else {
                        previous = reader.input_record(&header.input_record)?;
                        &previous
                    };
                    let prepared = prepare_ingress_v1(input, Some(&context), &BTreeMap::new(), &[], &version)?;
                    let PreparedCanonicalInputV1::PolymarketMessage(message) = prepared.current else {
                        return Err(io::Error::other("PM事实引用不是原PM消息").into());
                    };
                    if message_header(message.message()) != *header {
                        return Err(io::Error::other("原PM输入重建身份/双时间与compact事实不符").into());
                    }
                    let source = message.message().lineage.source_evidence.first()
                        .ok_or_else(|| io::Error::other("原PM消息缺直接来源"))?;
                    let admission = source.collector_admission.as_ref()
                        .ok_or_else(|| io::Error::other("原PM消息缺准入坐标"))?;
                    let source_at = source.source_event_at_utc_ms
                        .ok_or_else(|| io::Error::other("原PM消息缺真实来源时间"))?;
                    let next_rank = u64::try_from(epochs.len())?;
                    let epoch_rank = *epochs.entry(admission.source_sequence_epoch_id.clone()).or_insert(next_rank);
                    let mut key = epoch_rank.to_be_bytes().to_vec();
                    key.extend(((source_at as u64) ^ (1 << 63)).to_be_bytes());
                    key.extend(admission.collector_admission_sequence.to_be_bytes());
                    key.extend(canonical_position.to_be_bytes());
                    messages.push(SortRow::from_sort_key(key,
                        serde_json::to_vec(&(canonical_position, message.message()))?))?;
                    supplied += 1;
                    notify(progress, Notice::HiddenMessageSupplied);
                }
                CompactFactPayloadV1::Inline(_) | CompactFactPayloadV1::BinanceDepthFromPrefix { .. } => {
                    if let Some(at) = fact.dependency_time.source_event_at_utc_ms {
                        sink.reference(canonical_position, at, reference_order, fact)?;
                        reference_order = reference_order.checked_add(1)
                            .ok_or_else(|| io::Error::other("hidden参考派生位置溢出"))?;
                        notify(progress, Notice::HiddenReferenceIndexed);
                    }
                }
            }
            canonical_position = canonical_position.checked_add(1)
                .ok_or_else(|| io::Error::other("hidden规范事实位置溢出"))?;
        }
        if (packet.input.input_position + 1).is_multiple_of(config.batch_rows as u64) {
            notify(progress, Notice::Checkpoint);
        }
    }
    if canonical_position != reader.manifest.facts {
        return Err(io::Error::other("hidden准备必须消费完整compact起点和末端").into());
    }
    notify(progress, Notice::Stage(Stage::HiddenMessagesFinish));
    let (sorted_path, message_sort) = messages.finish_with_progress(&mut |value| {
        notify(progress,Notice::SourceMergeProgress {index:0,progress:value});
    })?;
    notify(progress, Notice::HiddenMessagesSorted(message_sort.clone()));
    let contexts = contexts.into_values().map(|context| context.prepared_context(&version))
        .collect::<RecordingResult<Vec<_>>>()?;
    let mut reducer = HiddenOrderedReducerV1::new(contexts, consumer_stream_id.clone())?;
    let mut cursor = PartCursor::open(&sorted_path, config.batch_rows, config.max_run_bytes)?;
    let mut steps = 0_u64;
    let mut reports = 0_u64;
    notify(progress, Notice::Stage(Stage::HiddenReduceAndWrite));
    while let Some(row) = cursor.next_row()? {
        let (position, message): (u64, CanonicalPmMessageV1) = serde_json::from_slice(row.payload())?;
        let prepared = PreparedPmMessageV1::prepare(message)?;
        let material = reducer.reduce(&prepared, position)?;
        let count = u64::try_from(material.reduction.steps.len())?;
        reports = reports.checked_add(u64::try_from(material.reduction.reports.len())?)
            .ok_or_else(|| io::Error::other("hidden报告数量溢出"))?;
        let derivation_start = reference_order.checked_add(steps)
            .ok_or_else(|| io::Error::other("hidden派生位置溢出"))?;
        sink.message(position, derivation_start, &prepared, material)?;
        steps = steps.checked_add(count).ok_or_else(|| io::Error::other("hidden步骤数量溢出"))?;
        notify(progress, Notice::HiddenMessageReduced);
        if steps.is_multiple_of(config.batch_rows as u64) { notify(progress, Notice::Checkpoint); }
    }
    Ok(CompactHiddenSummaryV1 {
        source_prefix: reader.manifest.applied_prefix.clone(), consumer_stream_id,
        canonical_facts: canonical_position, messages: supplied, references: reference_order,
        steps, reports, message_sort,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compact_public::PublicCompactWriterV1,
        gpu_initial_sort::GpuInitialRunOrderV1,
        hidden_preparation::{HiddenIndexEntryV1, HiddenIndexReaderV1, HiddenStepReaderV1,
            HiddenStoredRowV1, HiddenStoredStepV1, prepare_hidden_v1},
        public_recording::{PublicRecordingReaderV1, PublicRecordingWriterV1},
        public_recording::tests::{apply, book, change, config, core, end, meta, pm, record, row, twap_input, twap_record},
    };
    use data_pipeline_core::{public_input::{TypedRawRowV2, prepare_public_ingress_v1},
        typed_raw::{PolymarketOrderbookEvent, RtdsFreshnessStatus, TypedRawRow}};

    #[derive(Default)]
    struct Collected {
        references: BTreeMap<u64, serde_json::Value>,
        rows: Vec<serde_json::Value>,
    }
    impl CompactHiddenSinkV1 for Collected {
        fn reference(&mut self, canonical_position: u64, at: i64, derivation_position: u64,
            _: &CompactFactV1) -> RecordingResult<()> {
            self.references.insert(derivation_position, serde_json::to_value(HiddenIndexEntryV1::Reference {
                canonical_position, source_at_utc_ms: at, derivation_position,
            })?);
            Ok(())
        }
        fn message(&mut self, canonical_position: u64, derivation_start: u64,
            _: &PreparedPmMessageV1, material: MaterializedPmReductionV1<'_>) -> RecordingResult<()> {
            for (offset, (step, binding)) in material.reduction.steps.into_iter().zip(material.bindings).enumerate() {
                let reference = HiddenDerivedStepRefV1 {
                    canonical_fact_id: step.canonical_fact_id.clone(), canonical_record_position: canonical_position,
                    member_index: step.member_index, step_index: step.step_index,
                    derivation_position: derivation_start + offset as u64,
                    position: binding.position, derived_id: binding.derived_id,
                    dependency_time: step.dependency_time.clone(),
                };
                let step = serde_json::from_value(serde_json::to_value(step)?)?;
                self.rows.push(serde_json::to_value(HiddenStoredRowV1::Step(HiddenStoredStepV1 {reference, step}))?);
            }
            for (ordinal, report) in material.reduction.reports.into_iter().enumerate() {
                let report = serde_json::from_value(serde_json::to_value(report)?)?;
                self.rows.push(serde_json::to_value(HiddenStoredRowV1::Report {
                    canonical_position, report_ordinal: ordinal as u32, report,
                })?);
            }
            Ok(())
        }
    }

    #[test]
    #[ignore = "AutoDL准确提交GPU初始排序与原hidden独立对照"]
    fn gpu_compact_hidden_matches_original_epoch_order_real_prestate_and_references() {
        let temp = tempfile::tempdir().unwrap();
        let mut old_core = core();
        let mut compact_core = core();
        let recording_root = temp.path().join("recording");
        let compact_root = temp.path().join("compact");
        let mut writer = PublicRecordingWriterV1::create(&recording_root, old_core.header(), config()).unwrap();
        let mut compact = PublicCompactWriterV1::create(&compact_root, compact_core.header().clone()).unwrap();
        let mut anomaly = twap_input(60);
        anomaly.common.freshness_status = RtdsFreshnessStatus::InvalidTimestamp;
        for input in [
            meta(0), pm(1, 1, 20050, book("10")),
            pm(2, 2, 20200, PolymarketOrderbookEvent::PriceChange {price_changes: vec![change(Some("20"))]}),
            record(3, TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-b", 1, 20100, 23000, book("15"))))),
            twap_record(4, 30, 25002, "65000"),
            record(5, TypedRawRowV2::PolymarketRtdsChainlinkTwap(anomaly)), end(6),
        ] {
            let prepared = prepare_public_ingress_v1(&compact_core, &input, &BTreeMap::new(), &[]).unwrap();
            let facts = compact_core.push_facts(&prepared).unwrap();
            compact.push_prepared(&input, &prepared, &facts).unwrap();
            let result = apply(&mut old_core, &input);
            let position = input.input_position;
            writer.record(input, result).unwrap();
            if [0, 2].contains(&position) { writer.split_part_for_test().unwrap(); }
        }
        compact.publish().unwrap();
        writer.stabilize().unwrap();
        let mut output_config = config();
        output_config.sort.merge_workers = 8;
        let recording = PublicRecordingReaderV1::recover(&recording_root, 1, 1<<20).unwrap();
        assert_eq!(recording.file_paths().len(), 3);
        let mut old = prepare_hidden_v1(old_core,
            recording,
            &temp.path().join("old-hidden"), &output_config, "same-hidden-consumer".into()).unwrap();
        // 仅改变原材料的物理位置；原epoch、步骤、引用及桶边界不变。
        for (label, path) in [("split-index", &mut old.index_path), ("split-derived", &mut old.derived_path)] {
            let mut cursor = PartCursor::open(&crate::SortedRun::read_parts(path).unwrap(), 1, 1<<20).unwrap();
            let mut capture = crate::gate_a::SequentialCaptureWriter::create_segment(label,
                &temp.path().join(label), std::sync::Arc::new(std::sync::Mutex::new(0)), &output_config).unwrap();
            let mut sequence = 0;
            while let Some(row) = cursor.next_row().unwrap() {
                capture.push(sequence, row.sort_key_bytes().to_vec(), row.payload()).unwrap();
                if sequence == 0 { capture.split_part_for_test().unwrap(); }
                sequence += 1;
            }
            *path = capture.close_segment().unwrap();
            assert_eq!(crate::SortedRun::read_parts(path).unwrap().parts.len(), 2);
        }
        let mut expected = Vec::new();
        let mut steps = HiddenStepReaderV1::open(&old, 1, 1<<20).unwrap();
        while let Some(row) = steps.next_row().unwrap() { expected.push(serde_json::to_value(row).unwrap()); }
        let mut references = BTreeMap::new();
        let mut index = HiddenIndexReaderV1::open(&old, 1, 1<<20).unwrap();
        while let Some((_, entry)) = index.next_entry().unwrap() {
            if let HiddenIndexEntryV1::Reference {derivation_position, ..} = &entry {
                references.insert(*derivation_position, serde_json::to_value(entry).unwrap());
            }
        }
        let order = Arc::new(GpuInitialRunOrderV1::start(
            Path::new(&std::env::var("B_GPU_PYTHON").unwrap()),
            Path::new(&std::env::var("B_GPU_SORT_WORKER").unwrap())).unwrap());
        let sort = output_config.sort;
        let mut collected = Collected::default();
        let mut reader = PublicCompactReaderV1::open(&compact_root, 1<<20).unwrap();
        let summary = reduce_compact_hidden_v1(&mut reader, &temp.path().join("compact-hidden"), &sort,
            order, "same-hidden-consumer".into(), &mut collected, None).unwrap();
        assert_eq!(collected.rows, expected);
        assert_eq!(collected.references, references);
        assert_eq!(summary.messages, 3);
        assert_eq!(summary.references, 1);
        assert_eq!(summary.canonical_facts, reader.manifest.facts);
        assert_eq!(summary.source_prefix, reader.manifest.applied_prefix);
        // 引用读取不推进完整读光标，不读取任何旧recording。
        let input = reader.input_record(&CanonicalInputRecordRefV1 {
            input_stream_id: reader.manifest.header.input_stream_id.clone(), input_position: 1,
        }).unwrap();
        assert_eq!(input.input_position, 1);
        assert!(reader.next_packet().unwrap().is_none());
        eprintln!("compact_hidden_original_reference={}", serde_json::to_string(&summary).unwrap());
    }
}
