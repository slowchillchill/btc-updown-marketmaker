//! 正常有限前缀的隐藏派生；先释放公开数量状态，再使用同一业务算法。
use crate::historical_preparation_progress::{
    HistoricalPreparationNoticeV1 as Notice, HistoricalPreparationNotifierV1,
    HistoricalPreparationStageV1 as Stage, notify,
};
use crate::public_recording::*;
use crate::{
    ExternalSorter, RunCursor, SortRow,
    gate_a::{GateAOutputConfig, SequentialCaptureWriter},
};
use contracts::*;
use data_pipeline_core::{
    pm_reduction::*,
    public_observation_core::{PmOwnedStateV1, PublicObservationCoreV1},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HiddenIndexEntryV1 {
    Step(HiddenDerivedStepRefV1),
    /// 原规范参考或市场事实；只保存原位置，缺真实来源时间的不进入此索引。
    Reference {
        canonical_position: u64,
        source_at_utc_ms: i64,
        derivation_position: u64,
    },
}
impl HiddenIndexEntryV1 {
    fn at(&self) -> i64 {
        match self {
            Self::Step(s) => s.dependency_time.source_event_at_utc_ms.unwrap(),
            Self::Reference {
                source_at_utc_ms, ..
            } => *source_at_utc_ms,
        }
    }
}
#[derive(Serialize, Deserialize)]
pub struct HiddenStoredStepV1 {
    pub reference: HiddenDerivedStepRefV1,
    /// 原步骤的完整类型化序列化，包含真实前量；属于可重建派生产物。
    pub step: PmReductionStepV1<'static, SourceEvidenceRefV3>,
}
#[derive(Serialize, Deserialize)]
pub enum HiddenStoredRowV1 {
    Step(HiddenStoredStepV1),
    Report {
        canonical_position: u64,
        report_ordinal: u32,
        report: PmReductionReportV2<SourceEvidenceRefV3>,
    },
}
// 写入端借用同一 wire 形状；读取端继续拥有实际反序列化的来源。
#[derive(Serialize)]
enum HiddenStoredRowRef<'s, 'm, E> {
    Step {
        reference: &'s HiddenDerivedStepRefV1,
        step: &'s PmReductionStepV1<'m, E>,
    },
    Report {
        canonical_position: u64,
        report_ordinal: u32,
        report: &'s PmReductionReportV2<E>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HiddenPreparationV1 {
    pub prefix: CommittedIngressPrefixV1,
    pub consumer_stream_id: String,
    pub derived_path: PathBuf,
    pub index_path: PathBuf,
}
/// 独立hidden状态，共享原归约与物化算法；不依赖旧recording的耐久前缀。
/// 调用方先按原epoch rank/source time/admission/canonical总序供给消息。
pub struct HiddenOrderedReducerV1 {
    states: BTreeMap<String, PmOwnedStateV1<PreparedPmEvidenceV1<SourceEvidenceRefV3>>>,
    consumer_stream_id: String,
    failed: bool,
}
impl HiddenOrderedReducerV1 {
    pub fn new(contexts: Vec<PmMarketContextV1>, consumer_stream_id: String) -> RecordingResult<Self> {
        let mut states = BTreeMap::new();
        for context in contexts {
            let condition = context.market.condition_id.0.clone();
            states.insert(
                condition,
                PmOwnedStateV1::new(PmMarketContextV1 {
                    market: context.market,
                    assets: context.assets,
                    identity_source_evidence: context
                        .identity_source_evidence
                        .into_iter()
                        .map(|e| PreparedPmEvidenceV1::prepare(e, None))
                        .collect::<Result<_, _>>()?,
                    canonical_rule_version: context.canonical_rule_version,
                }),
            );
        }
        Ok(Self { states, consumer_stream_id, failed: false })
    }

    /// 失败可能发生在状态应用后的物化阶段；不得从部分归约继续消费。
    pub fn reduce<'m>(
        &mut self,
        prepared: &'m PreparedPmMessageV1,
        canonical_position: u64,
    ) -> RecordingResult<MaterializedPmReductionV1<'m>> {
        if self.failed {
            return Err(io::Error::other("失败的hidden归约器不能继续消费").into());
        }
        self.failed = true;
        let state = self.states
            .get_mut(&prepared.message().market.condition_id.0)
            .ok_or_else(|| io::Error::other("hidden消息缺原固定身份及初始tick上下文"))?;
        let result = compute_pm_message_steps_v1(
            state.context(), state, prepared, &self.consumer_stream_id, canonical_position, false,
        )?;
        state.apply(&result);
        let material = materialize_pm_step_identity_v1(result)?;
        self.failed = false;
        Ok(material)
    }
}

/// 参数消费公开核心，不与隐藏数量状态并存；上下文仅保留原固定身份及初始 tick。
pub fn prepare_hidden_v1(
    public: PublicObservationCoreV1,
    reader: PublicRecordingReaderV1,
    root: &Path,
    config: &GateAOutputConfig,
    consumer_stream_id: String,
) -> RecordingResult<HiddenPreparationV1> {
    prepare_hidden_with_progress_v1(public, reader, root, config, consumer_stream_id, None)
}
pub fn prepare_hidden_with_progress_v1(
    public: PublicObservationCoreV1,
    mut reader: PublicRecordingReaderV1,
    root: &Path,
    config: &GateAOutputConfig,
    consumer_stream_id: String,
    progress: Option<&HistoricalPreparationNotifierV1<'_>>,
) -> RecordingResult<HiddenPreparationV1> {
    notify(progress, Notice::Stage(Stage::HiddenScanAndSupply));
    let prefix = reader
        .prefix
        .clone()
        .filter(|p| p.normal_end)
        .ok_or_else(|| io::Error::other("隐藏准备只消费正常结束的稳定有限前缀"))?;
    let contexts: Vec<_> = public
        .fixed_markets()
        .map(|m| public.pm_context(&m.condition_id.0).unwrap())
        .collect();
    drop(public);
    fs::create_dir(root)?;
    fs::create_dir(root.join("messages"))?;
    fs::create_dir(root.join("index"))?;
    let mut messages = ExternalSorter::new(&root.join("messages"), &config.sort)?;
    let mut index = ExternalSorter::new(&root.join("index"), &config.sort)?;
    let mut epochs = BTreeMap::new();
    let mut reference_order = 0;
    let mut scan_rows = 0_u64;
    while let Some(row) = reader.next_physical_row()? {
        notify(progress, Notice::HiddenScanRow);
        if progress.is_some() {
            scan_rows += 1;
            if scan_rows.is_multiple_of(config.sort.batch_rows as u64) {
                notify(progress, Notice::Checkpoint);
            }
        }
        match row {
            PublicStoredRowV1::Canonical {
                canonical_position,
                record,
                ..
            } => match record {
                CanonicalEventRecordV2::PolymarketMessage(message) => {
                    let source = &message.lineage.source_evidence[0];
                    let admission = source.collector_admission.as_ref().unwrap();
                    let next_rank = epochs.len() as u64;
                    let rank = *epochs
                        .entry(admission.source_sequence_epoch_id.clone())
                        .or_insert(next_rank);
                    let mut key = rank.to_be_bytes().to_vec();
                    key.extend(signed_key(source.source_event_at_utc_ms.unwrap()));
                    key.extend(admission.collector_admission_sequence.to_be_bytes());
                    key.extend(canonical_position.to_be_bytes());
                    messages.push(SortRow::from_sort_key(
                        key,
                        serde_json::to_vec(&(canonical_position, message))?,
                    ))?;
                    notify(progress, Notice::HiddenMessageSupplied);
                }
                CanonicalEventRecordV2::ReferenceEvent(event) => {
                    if let Some(at) = event.time.source_event_at_utc_ms {
                        let value = HiddenIndexEntryV1::Reference {
                            canonical_position,
                            source_at_utc_ms: at,
                            derivation_position: reference_order,
                        };
                        index.push(index_row(at, reference_order, &value)?)?;
                        reference_order += 1;
                        notify(progress, Notice::HiddenReferenceIndexed);
                    }
                }
                CanonicalEventRecordV2::MarketFact(event) => {
                    if let Some(at) = event.dependency_time.source_event_at_utc_ms {
                        let value = HiddenIndexEntryV1::Reference {
                            canonical_position,
                            source_at_utc_ms: at,
                            derivation_position: reference_order,
                        };
                        index.push(index_row(at, reference_order, &value)?)?;
                        reference_order += 1;
                        notify(progress, Notice::HiddenReferenceIndexed);
                    }
                }
            },
            PublicStoredRowV1::BinanceDepthReference {
                canonical_position,
                time,
                ..
            } => {
                if let Some(at) = time.source_event_at_utc_ms {
                    let value = HiddenIndexEntryV1::Reference {
                        canonical_position,
                        source_at_utc_ms: at,
                        derivation_position: reference_order,
                    };
                    index.push(index_row(at, reference_order, &value)?)?;
                    reference_order += 1;
                    notify(progress, Notice::HiddenReferenceIndexed);
                }
            }
            _ => {}
        }
    }
    notify(progress, Notice::Stage(Stage::HiddenMessagesFinish));
    let (sorted_path, message_metrics) = messages.finish(None)?;
    notify(progress, Notice::HiddenMessagesSorted(message_metrics));
    notify(progress, Notice::Stage(Stage::HiddenReduceAndWrite));
    let mut cursor = crate::PartCursor::open(
        &sorted_path,
        config.sort.batch_rows,
        config.sort.max_run_bytes,
    )?;
    let mut reducer = HiddenOrderedReducerV1::new(contexts, consumer_stream_id.clone())?;
    let mut derived = SequentialCaptureWriter::create(
        "hidden_derived_v1",
        &root.join("derived"),
        Arc::new(Mutex::new(0)),
        config,
    )?;
    derived.defer_batch_sync();
    let mut row_position = 0;
    let mut derivation_position = reference_order;
    let mut reduced_messages = 0_u64;
    while let Some(row) = cursor.next_row()? {
        let (canonical_position, message): (u64, CanonicalPmMessageV1) =
            serde_json::from_slice(row.payload())?;
        let prepared = PreparedPmMessageV1::prepare(message)?;
        let material = reducer.reduce(&prepared, canonical_position)?;
        notify(progress, Notice::HiddenMessageReduced);
        for (step, binding) in material.reduction.steps.into_iter().zip(material.bindings) {
            let reference = HiddenDerivedStepRefV1 {
                canonical_fact_id: step.canonical_fact_id.clone(),
                canonical_record_position: canonical_position,
                member_index: step.member_index,
                step_index: step.step_index,
                derivation_position,
                position: binding.position,
                derived_id: binding.derived_id,
                dependency_time: step.dependency_time.clone(),
            };
            if let Some(at) = reference.dependency_time.source_event_at_utc_ms {
                index.push(index_row(
                    at,
                    derivation_position,
                    &HiddenIndexEntryV1::Step(reference.clone()),
                )?)?;
                notify(progress, Notice::HiddenStepIndexed);
            }
            derived.push(
                row_position,
                row_position.to_be_bytes().to_vec(),
                &serde_json::to_vec(&HiddenStoredRowRef::Step {
                    reference: &reference,
                    step: &step,
                })?,
            )?;
            notify(progress, Notice::HiddenStepWritten);
            row_position += 1;
            derivation_position += 1;
        }
        for (ordinal, report) in material.reduction.reports.into_iter().enumerate() {
            derived.push(
                row_position,
                row_position.to_be_bytes().to_vec(),
                &serde_json::to_vec(&HiddenStoredRowRef::Report {
                    canonical_position,
                    report_ordinal: ordinal as u32,
                    report: &report,
                })?,
            )?;
            notify(progress, Notice::HiddenReportWritten);
            row_position += 1;
        }
        if progress.is_some() {
            reduced_messages += 1;
            if reduced_messages.is_multiple_of(config.sort.batch_rows as u64) {
                notify(progress, Notice::Checkpoint);
            }
        }
    }
    drop(reducer);
    notify(progress, Notice::Stage(Stage::HiddenFinalize));
    let derived_path = derived.close_segment()?;
    let (index_path, index_metrics) = index.finish_single_file(None)?;
    notify(progress, Notice::HiddenIndexSorted(index_metrics));
    crate::gate_a_publication::sync_tree(root)?;
    Ok(HiddenPreparationV1 {
        prefix,
        consumer_stream_id,
        derived_path,
        index_path,
    })
}
fn signed_key(value: i64) -> [u8; 8] {
    ((value as u64) ^ (1 << 63)).to_be_bytes()
}
fn index_row(at: i64, position: u64, value: &HiddenIndexEntryV1) -> RecordingResult<SortRow> {
    let mut key = signed_key(at).to_vec();
    key.extend(position.to_be_bytes());
    Ok(SortRow::from_sort_key(key, serde_json::to_vec(value)?))
}
/// 光标跨 Parquet 页读取至真正下一时点，页末不会产生闭桶。
pub struct HiddenBucketReaderV1 {
    cursor: RunCursor,
    prefix: CommittedIngressPrefixV1,
    next: Option<HiddenIndexEntryV1>,
    position: u64,
}
impl HiddenBucketReaderV1 {
    pub fn open(
        prepared: &HiddenPreparationV1,
        rows: usize,
        bytes: usize,
    ) -> RecordingResult<Self> {
        Ok(Self {
            cursor: RunCursor::open(&prepared.index_path, rows, bytes)?,
            prefix: prepared.prefix.clone(),
            next: None,
            position: 0,
        })
    }
    fn read(&mut self) -> RecordingResult<Option<HiddenIndexEntryV1>> {
        Ok(self
            .cursor
            .next_row()?
            .map(|r| serde_json::from_slice(r.payload()))
            .transpose()?)
    }
    pub fn next_bucket(&mut self) -> RecordingResult<Option<ClosedHiddenStepBucketV1>> {
        let first = match self.next.take() {
            Some(v) => v,
            None => match self.read()? {
                Some(v) => v,
                None => return Ok(None),
            },
        };
        let at = first.at();
        let start = self.position;
        let mut steps = Vec::new();
        let mut reference_positions = Vec::new();
        let mut current = first;
        let closure = loop {
            match current {
                HiddenIndexEntryV1::Step(step) => steps.push(step),
                HiddenIndexEntryV1::Reference { .. } => reference_positions.push(self.position),
            }
            self.position += 1;
            match self.read()? {
                None => break ReplayIndexClosureV1::EndOfInput,
                Some(v) if v.at() != at => {
                    let next_at = v.at();
                    self.next = Some(v);
                    break ReplayIndexClosureV1::NextGlobalEventAt(next_at);
                }
                Some(v) => current = v,
            }
        };
        Ok(Some(ClosedHiddenStepBucketV1 {
            committed_prefix: self.prefix.clone(),
            bucket_time_utc_ms: at,
            index_start: start,
            index_end: self.position,
            steps,
            reference_positions,
            closure,
        }))
    }
}

/// 按派生文件原序读取强类型消费材料；不重解码原始行情或重算业务。
pub struct HiddenStepReaderV1 {
    cursor: RunCursor,
}
impl HiddenStepReaderV1 {
    pub fn open(
        prepared: &HiddenPreparationV1,
        rows: usize,
        bytes: usize,
    ) -> RecordingResult<Self> {
        Ok(Self {
            cursor: RunCursor::open(&prepared.derived_path, rows, bytes)?,
        })
    }
    pub fn next_row(&mut self) -> RecordingResult<Option<HiddenStoredRowV1>> {
        Ok(self
            .cursor
            .next_row()?
            .map(|r| serde_json::from_slice(r.payload()))
            .transpose()?)
    }
}

/// 完整外排索引的原位置读取，供桶中的参考位置定位唯一规范记录。
pub struct HiddenIndexReaderV1 {
    cursor: RunCursor,
    next_position: u64,
}
impl HiddenIndexReaderV1 {
    pub fn open(
        prepared: &HiddenPreparationV1,
        rows: usize,
        bytes: usize,
    ) -> RecordingResult<Self> {
        Ok(Self {
            cursor: RunCursor::open(&prepared.index_path, rows, bytes)?,
            next_position: 0,
        })
    }
    pub fn next_entry(&mut self) -> RecordingResult<Option<(u64, HiddenIndexEntryV1)>> {
        let Some(row) = self.cursor.next_row()? else {
            return Ok(None);
        };
        let entry = serde_json::from_slice(row.payload())?;
        let position = self.next_position;
        self.next_position += 1;
        Ok(Some((position, entry)))
    }
}

/// 当前有界索引页的强类型材料，位置仍是唯一原索引位置。
pub enum ResolvedHiddenRecordV1 {
    Step {
        stored: HiddenStoredStepV1,
        market: MarketIdentityV2,
    },
    Reference {
        canonical_position: u64,
        record: CanonicalEventRecordV2,
    },
    /// 只推进原隐藏参考时序，不提供撮合正文。
    BinanceDepthReference {
        canonical_position: u64,
        binding: PublicFactBindingV1,
        time: EventTimeV2,
    },
}
/// 实际解码扫描计数；不等于磁盘物理读取字节或操作系统缓存未命中。
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct HiddenMaterialReadMetricsV1 {
    pub pages: u64,
    pub index_entries: u64,
    pub decoded_derived_rows: u64,
    /// 原录制的物理行数；观察区间只计一条，不包含兼容展开的逻辑行。
    pub decoded_recording_rows: u64,
    pub peak_page_entries: usize,
}
/// 按索引页定位原文件；只持有当前页，派生文件物理顺序不参与消费排序。
pub struct HiddenMaterialReaderV1 {
    prepared: HiddenPreparationV1,
    recording_root: PathBuf,
    index: HiddenIndexReaderV1,
    rows: usize,
    bytes: usize,
    metrics: HiddenMaterialReadMetricsV1,
    derived: Option<HiddenStepReaderV1>,
    recording: Option<PublicRecordingReaderV1>,
    last_derivation_position: Option<u64>,
    last_canonical_position: Option<u64>,
    last_market: Option<MarketIdentityV2>,
}
impl HiddenMaterialReaderV1 {
    pub fn open(
        prepared: &HiddenPreparationV1,
        recording_root: &Path,
        rows: usize,
        bytes: usize,
    ) -> RecordingResult<Self> {
        Ok(Self {
            prepared: prepared.clone(),
            recording_root: recording_root.to_owned(),
            index: HiddenIndexReaderV1::open(prepared, rows, bytes)?,
            rows,
            bytes,
            metrics: HiddenMaterialReadMetricsV1::default(),
            derived: None,
            recording: None,
            last_derivation_position: None,
            last_canonical_position: None,
            last_market: None,
        })
    }
    pub fn metrics(&self) -> &HiddenMaterialReadMetricsV1 {
        &self.metrics
    }
    pub fn next_page(&mut self) -> RecordingResult<Vec<(u64, ResolvedHiddenRecordV1)>> {
        let mut entries = Vec::new();
        for _ in 0..self.rows {
            let Some(entry) = self.index.next_entry()? else {
                break;
            };
            entries.push(entry);
        }
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        self.metrics.pages += 1;
        self.metrics.index_entries += entries.len() as u64;
        self.metrics.peak_page_entries = self.metrics.peak_page_entries.max(entries.len());
        let mut step_slots = BTreeMap::new();
        let mut fact_slots = BTreeMap::new();
        let mut step_market_slots: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
        for (slot, (_, entry)) in entries.iter().enumerate() {
            match entry {
                HiddenIndexEntryV1::Step(reference) => {
                    step_slots.insert(reference.derivation_position, slot);
                    step_market_slots
                        .entry(reference.canonical_record_position)
                        .or_default()
                        .push(slot);
                }
                HiddenIndexEntryV1::Reference {
                    canonical_position, ..
                } => {
                    fact_slots.insert(*canonical_position, slot);
                }
            }
        }
        let mut stored_steps: Vec<Option<HiddenStoredStepV1>> =
            (0..entries.len()).map(|_| None).collect();
        let mut resolved: Vec<Option<ResolvedHiddenRecordV1>> =
            (0..entries.len()).map(|_| None).collect();
        if !step_slots.is_empty() {
            // 索引可真实倒退；只在所需步骤已经越过时重开，不能把索引序当文件序。
            if self
                .last_derivation_position
                .is_some_and(|last| *step_slots.first_key_value().unwrap().0 <= last)
            {
                self.derived = None;
                self.last_derivation_position = None;
            }
            if self.derived.is_none() {
                self.derived = Some(HiddenStepReaderV1::open(
                    &self.prepared,
                    self.rows,
                    self.bytes,
                )?);
            }
            while let Some(row) = self.derived.as_mut().unwrap().next_row()? {
                self.metrics.decoded_derived_rows += 1;
                if let HiddenStoredRowV1::Step(step) = row {
                    self.last_derivation_position = Some(step.reference.derivation_position);
                    if let Some(slot) = step_slots.remove(&step.reference.derivation_position) {
                        let HiddenIndexEntryV1::Step(expected) = &entries[slot].1 else {
                            unreachable!()
                        };
                        if step.reference != *expected
                            || step.reference.position.consumer_stream_id
                                != self.prepared.consumer_stream_id
                        {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "隐藏步骤完整引用或consumer身份与索引不符",
                            )
                            .into());
                        }
                        stored_steps[slot] = Some(step);
                    }
                    if step_slots.is_empty() {
                        break;
                    }
                }
            }
        }
        // 同一消息的步骤可能跨页；仅复用其市场身份，不缓存规范载荷。
        if let (Some(position), Some(market)) = (self.last_canonical_position, &self.last_market) {
            if let Some(slots) = step_market_slots.remove(&position) {
                for slot in slots {
                    if let Some(stored) = stored_steps[slot].take() {
                        resolved[slot] = Some(ResolvedHiddenRecordV1::Step {
                            stored,
                            market: market.clone(),
                        });
                    }
                }
            }
        }
        if !fact_slots.is_empty() || !step_market_slots.is_empty() {
            let first = fact_slots
                .keys()
                .chain(step_market_slots.keys())
                .min()
                .copied()
                .unwrap();
            // 相等的真正 Reference 仍需原载荷，不能用上一市场身份代替。
            if self
                .last_canonical_position
                .is_some_and(|last| first <= last)
            {
                self.recording = None;
                self.last_canonical_position = None;
                self.last_market = None;
            }
            if self.recording.is_none() {
                self.recording = Some(PublicRecordingReaderV1::open_committed(
                    &self.recording_root,
                    Some(self.prepared.prefix.clone()),
                    self.rows,
                    self.bytes,
                )?);
            }
            while let Some(row) = self.recording.as_mut().unwrap().next_physical_row()? {
                self.metrics.decoded_recording_rows += 1;
                match row {
                    PublicStoredRowV1::BinanceDepthReference {
                        canonical_position,
                        binding,
                        time,
                    } => {
                        self.last_canonical_position = Some(canonical_position);
                        self.last_market = None;
                        if let Some(slot) = fact_slots.remove(&canonical_position) {
                            resolved[slot] = Some(ResolvedHiddenRecordV1::BinanceDepthReference {
                                canonical_position,
                                binding,
                                time,
                            });
                        }
                    }
                    PublicStoredRowV1::Canonical {
                        canonical_position,
                        record,
                        ..
                    } => {
                        self.last_canonical_position = Some(canonical_position);
                        self.last_market = match &record {
                            CanonicalEventRecordV2::PolymarketMessage(message) => {
                                Some(message.market.clone())
                            }
                            _ => None,
                        };
                        if let Some(market) = &self.last_market {
                            if let Some(slots) = step_market_slots.remove(&canonical_position) {
                                for slot in slots {
                                    if let Some(stored) = stored_steps[slot].take() {
                                        resolved[slot] = Some(ResolvedHiddenRecordV1::Step {
                                            stored,
                                            market: market.clone(),
                                        });
                                    }
                                }
                            }
                        }
                        if let Some(slot) = fact_slots.remove(&canonical_position) {
                            resolved[slot] = Some(ResolvedHiddenRecordV1::Reference {
                                canonical_position,
                                record,
                            });
                        }
                    }
                    _ => {}
                }
                if fact_slots.is_empty() && step_market_slots.is_empty() {
                    break;
                }
            }
        }
        entries
            .into_iter()
            .zip(resolved)
            .map(|((position, _), record)| {
                Ok((
                    position,
                    record.ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "隐藏索引引用的原记录缺失")
                    })?,
                ))
            })
            .collect()
    }
}
