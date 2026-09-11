//! 正常有限前缀的隐藏派生；先释放公开数量状态，再使用同一业务算法。
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HiddenPreparationV1 {
    pub prefix: CommittedIngressPrefixV1,
    pub consumer_stream_id: String,
    pub derived_path: PathBuf,
    pub index_path: PathBuf,
}
/// 参数消费公开核心，不与隐藏数量状态并存；上下文仅保留原固定身份及初始 tick。
pub fn prepare_hidden_v1(
    public: PublicObservationCoreV1,
    mut reader: PublicRecordingReaderV1,
    root: &Path,
    config: &GateAOutputConfig,
    consumer_stream_id: String,
) -> RecordingResult<HiddenPreparationV1> {
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
    while let Some(row) = reader.next_row()? {
        if let PublicStoredRowV1::Canonical {
            canonical_position,
            record,
            ..
        } = row
        {
            match record {
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
                    }
                }
            }
        }
    }
    let sorted = messages.finish()?;
    let mut cursor = RunCursor::open(
        &sorted.output_path,
        config.sort.batch_rows,
        config.sort.max_run_bytes,
    )?;
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
    let mut derived = SequentialCaptureWriter::create(
        "hidden_derived_v1",
        &root.join("derived"),
        Arc::new(Mutex::new(0)),
        config,
    )?;
    derived.defer_batch_sync();
    let mut row_position = 0;
    let mut derivation_position = reference_order;
    while let Some(row) = cursor.next_row()? {
        let (canonical_position, message): (u64, CanonicalPmMessageV1) =
            serde_json::from_slice(row.payload())?;
        let prepared = PreparedPmMessageV1::prepare(message)?;
        let state = states
            .get_mut(&prepared.message().market.condition_id.0)
            .unwrap();
        let result = compute_pm_message_steps_v1(
            state.context(),
            state,
            &prepared,
            &consumer_stream_id,
            canonical_position,
            false,
        )?;
        state.apply(&result);
        let material = materialize_pm_step_identity_v1(result)?;
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
            }
            derived.push(
                row_position,
                row_position.to_be_bytes().to_vec(),
                &serde_json::to_vec(&HiddenStoredRowV1::Step(HiddenStoredStepV1 {
                    reference,
                    step: serde_json::from_value(serde_json::to_value(step)?)?,
                }))?,
            )?;
            row_position += 1;
            derivation_position += 1;
        }
        for (ordinal, report) in material.reduction.reports.into_iter().enumerate() {
            let report = serde_json::from_value(serde_json::to_value(report)?)?;
            derived.push(
                row_position,
                row_position.to_be_bytes().to_vec(),
                &serde_json::to_vec(&HiddenStoredRowV1::Report {
                    canonical_position,
                    report_ordinal: ordinal as u32,
                    report,
                })?,
            )?;
            row_position += 1;
        }
    }
    drop(states);
    let derived_path = derived.close_segment()?;
    let sorted_index = index.finish()?;
    crate::gate_a_publication::sync_tree(root)?;
    Ok(HiddenPreparationV1 {
        prefix,
        consumer_stream_id,
        derived_path,
        index_path: sorted_index.output_path,
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
}
/// 实际解码扫描计数；不等于磁盘物理读取字节或操作系统缓存未命中。
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct HiddenMaterialReadMetricsV1 {
    pub pages: u64,
    pub index_entries: u64,
    pub decoded_derived_rows: u64,
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
            let mut reader = HiddenStepReaderV1::open(&self.prepared, self.rows, self.bytes)?;
            while let Some(row) = reader.next_row()? {
                self.metrics.decoded_derived_rows += 1;
                if let HiddenStoredRowV1::Step(step) = row {
                    if let Some(slot) = step_slots.remove(&step.reference.derivation_position) {
                        stored_steps[slot] = Some(step);
                    }
                    if step_slots.is_empty() {
                        break;
                    }
                }
            }
        }
        if !fact_slots.is_empty() || !step_market_slots.is_empty() {
            let mut reader = PublicRecordingReaderV1::open_committed(
                &self.recording_root,
                Some(self.prepared.prefix.clone()),
                self.rows,
                self.bytes,
            )?;
            while let Some(row) = reader.next_row()? {
                self.metrics.decoded_recording_rows += 1;
                if let PublicStoredRowV1::Canonical {
                    canonical_position,
                    record,
                    ..
                } = row
                {
                    if let CanonicalEventRecordV2::PolymarketMessage(message) = &record {
                        if let Some(slots) = step_market_slots.remove(&canonical_position) {
                            for slot in slots {
                                if let Some(stored) = stored_steps[slot].take() {
                                    resolved[slot] = Some(ResolvedHiddenRecordV1::Step {
                                        stored,
                                        market: message.market.clone(),
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
                    if fact_slots.is_empty() && step_market_slots.is_empty() {
                        break;
                    }
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
