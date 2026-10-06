//! 自身完成凭据的compact hidden派生产物；消息快照引用原输入，索引按原真实时间全局排序。
use crate::{
    compact_hidden::{CompactHiddenSinkV1, CompactHiddenSummaryV1, reduce_compact_hidden_v1},
    compact_public::{CompactFileV1, CompactPmChangeV1, CompactPmMessageHeaderV1, CompactPmStepV1,
        PublicCompactReaderV1, file_sha256_with_progress},
    hidden_preparation::HiddenIndexEntryV1,
    historical_preparation_progress::HistoricalPreparationNotifierV1,
    public_recording::RecordingResult,
    ExternalSorter, InitialRunOrder, RunCursor, SortConfig, SortRow,
};
use contracts::*;
use data_pipeline_core::pm_reduction::{MaterializedPmReductionV1, PmReductionStepV1,
    PmReductionReportV2, PmStepChangeV1, PreparedPmMessageV1};
use serde::{Deserialize, Serialize};
use std::{collections::{BTreeMap, BTreeSet}, fs::{self, File},
    io::{self, BufWriter, Read, Seek, SeekFrom, Write}, path::Path,
    sync::Arc, time::Duration};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactHiddenPreparationConfigV1 {
    pub max_packet_bytes: usize,
    pub sort: SortConfig,
    pub consumer_stream_id: String,
}

const VERSION: &str = "compact_hidden_source_steps_v1";
const MEMBERS: [&str; 4] = ["hidden_messages.bin", "message_offsets.u64le", "message_positions.u64le", "hidden_index.parquet"];

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactHiddenMessageV1 {
    pub canonical_position: u64,
    pub message: CompactPmMessageHeaderV1,
    pub steps: Vec<(HiddenDerivedStepRefV1, CompactPmStepV1<CanonicalEventId>)>,
    pub reports: Vec<PmReductionReportV2<SourceEvidenceRefV3>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactHiddenManifestV1 {
    pub version: String,
    pub source_manifest_sha256: String,
    pub summary: CompactHiddenSummaryV1,
    pub indexed_steps: u64,
    pub normal_end: bool,
    pub files: Vec<CompactFileV1>,
}

struct MaterialSink {
    payload: BufWriter<File>, offsets: BufWriter<File>, positions: BufWriter<File>,
    payload_bytes: u64,
    max_packet_bytes: usize,
    index: ExternalSorter,
    indexed_steps: u64,
}
impl MaterialSink {
    fn index_entry(&mut self, at: i64, derivation: u64, value: &HiddenIndexEntryV1) -> RecordingResult<()> {
        let mut key = ((at as u64) ^ (1<<63)).to_be_bytes().to_vec();
        key.extend(derivation.to_be_bytes());
        self.index.push(SortRow::from_sort_key(key, serde_json::to_vec(value)?))?;
        Ok(())
    }
}
impl CompactHiddenSinkV1 for MaterialSink {
    fn reference(&mut self, canonical_position: u64, at: i64, derivation: u64,
        _: &crate::compact_public::CompactFactV1) -> RecordingResult<()> {
        self.index_entry(at, derivation, &HiddenIndexEntryV1::Reference {
            canonical_position, source_at_utc_ms: at, derivation_position: derivation,
        })
    }
    fn message(&mut self, canonical_position: u64, derivation_start: u64,
        prepared: &PreparedPmMessageV1, material: MaterializedPmReductionV1<'_>) -> RecordingResult<()> {
        let source = prepared.message();
        let header = CompactPmMessageHeaderV1 {
            schema_version: source.schema_version.clone(), event_id: source.event_id.clone(),
            market: source.market.clone(), group_id: source.group_id.clone(), lineage: source.lineage.clone(),
            time: source.time.clone(), input_record: source.input_record.clone(),
        };
        let mut steps = Vec::with_capacity(material.reduction.steps.len());
        for (offset, (step, binding)) in material.reduction.steps.into_iter().zip(material.bindings).enumerate() {
            // 原Prepared证据只去除编码缓存，wire仍是原SourceEvidenceRefV3。
            let step: PmReductionStepV1<'static, SourceEvidenceRefV3> =
                serde_json::from_value(serde_json::to_value(step)?)?;
            let reference = HiddenDerivedStepRefV1 {
                canonical_fact_id: step.canonical_fact_id.clone(), canonical_record_position: canonical_position,
                member_index: step.member_index, step_index: step.step_index,
                derivation_position: derivation_start.checked_add(u64::try_from(offset)?)
                    .ok_or_else(|| io::Error::other("hidden派生位置溢出"))?,
                position: binding.position, derived_id: binding.derived_id,
                dependency_time: step.dependency_time.clone(),
            };
            if let Some(at) = reference.dependency_time.source_event_at_utc_ms {
                self.index_entry(at, reference.derivation_position, &HiddenIndexEntryV1::Step(reference.clone()))?;
                self.indexed_steps += 1;
            }
            let change = match step.change {
                PmStepChangeV1::InitialSnapshot { asset, initial_tick_size_pusd_per_share,
                    source_sequence_epoch_id, source_cursor, book_digest_sha256, evidence, .. } => {
                    CompactPmChangeV1::InitialSnapshotFromInput {
                        input_record: source.input_record.clone(), asset, initial_tick_size_pusd_per_share,
                        source_sequence_epoch_id, source_cursor, book_digest_sha256, evidence,
                    }
                }
                change => CompactPmChangeV1::Inline(change),
            };
            steps.push((reference, CompactPmStepV1 {
                canonical_fact_id: step.canonical_fact_id, member_index: step.member_index, step_index: step.step_index,
                derived_id: step.derived_id, change, direct: step.direct, prestate: step.prestate, absence: step.absence,
                dependencies: step.dependencies, direct_time: step.direct_time,
                dependency_time: step.dependency_time, quality: step.quality,
            }));
        }
        let reports = material.reduction.reports.into_iter().map(|report|
            serde_json::from_value(serde_json::to_value(report)?)).collect::<Result<_, _>>()?;
        let count = u64::try_from(steps.len())?;
        let packet = CompactHiddenMessageV1 {canonical_position, message: header, steps, reports};
        let encoded = serde_json::to_vec(&packet)?;
        if encoded.len() > self.max_packet_bytes { return Err(io::Error::other("hidden消息超过本次准备包容量").into()); }
        let bytes = zstd::bulk::compress(&encoded, 3)?;
        if bytes.len() > self.max_packet_bytes { return Err(io::Error::other("hidden压缩包超过本次准备包容量").into()); }
        self.payload.write_all(&bytes)?;
        self.payload_bytes = self.payload_bytes.checked_add(u64::try_from(bytes.len())?)
            .ok_or_else(|| io::Error::other("hidden载荷长度溢出"))?;
        self.offsets.write_all(&self.payload_bytes.to_le_bytes())?;
        for value in [canonical_position, derivation_start, count] {
            self.positions.write_all(&value.to_le_bytes())?;
        }
        Ok(())
    }
}

/// 所有大文件/临时run由调用方放在数据盘；失败保留专属pending，不覆盖既有产物。
pub fn prepare_compact_hidden_material_v1(
    source_root: &Path, target: &Path, max_packet_bytes: usize, config: &SortConfig,
    initial_order: Arc<dyn InitialRunOrder>, consumer_stream_id: String,
    progress: Option<&HistoricalPreparationNotifierV1<'_>>,
    mut publication_progress: impl FnMut(&str, &str, u64, Option<u64>),
) -> RecordingResult<CompactHiddenManifestV1> {
    let interval = Duration::from_secs(10);
    let mut reader = PublicCompactReaderV1::open_with_progress(source_root, max_packet_bytes, interval, |notice| {
        publication_progress(notice.stage, notice.member, notice.completed_bytes, notice.total_bytes);
    })?;
    let source_manifest_sha256 = file_sha256_with_progress(&source_root.join("manifest.json"), interval, |done, total| {
        publication_progress("source_receipt_sha256", "manifest.json", done, Some(total));
    })?;
    let parent = target.parent().ok_or_else(|| io::Error::other("hidden产物必须有父目录"))?;
    fs::create_dir_all(parent)?;
    let name = target.file_name().ok_or_else(|| io::Error::other("hidden产物缺文件名"))?.to_string_lossy();
    let pending = parent.join(format!(".{name}.pending"));
    fs::create_dir(&pending)?;
    fs::create_dir(pending.join("index"))?;
    let mut offsets = BufWriter::new(File::create_new(pending.join(MEMBERS[1]))?);
    offsets.write_all(&0_u64.to_le_bytes())?;
    let mut sink = MaterialSink {
        payload: BufWriter::new(File::create_new(pending.join(MEMBERS[0]))?), offsets,
        positions: BufWriter::new(File::create_new(pending.join(MEMBERS[2]))?), payload_bytes: 0, max_packet_bytes,
        index: ExternalSorter::with_initial_order(&pending.join("index"), config, Some(initial_order.clone()))?,
        indexed_steps: 0,
    };
    let summary = reduce_compact_hidden_v1(&mut reader, &pending.join("supply"), config,
        initial_order, consumer_stream_id, &mut sink, progress)?;
    // 仅移除本构建成功创建的临时排序目录；发布成员不复制原快照载荷。
    fs::remove_dir_all(pending.join("supply"))?;
    let MaterialSink {mut payload, mut offsets, mut positions, index, indexed_steps, ..} = sink;
    for file in [&mut payload, &mut offsets, &mut positions] { file.flush()?; file.get_ref().sync_all()?; }
    let (index_path, index_metrics) = index.finish_single_file_with_progress(&mut |notice| {
        crate::historical_preparation_progress::notify(progress,
            crate::historical_preparation_progress::HistoricalPreparationNoticeV1::SourceMergeProgress {index: 0, progress: notice});
    })?;
    crate::historical_preparation_progress::notify(progress,
        crate::historical_preparation_progress::HistoricalPreparationNoticeV1::HiddenIndexSorted(index_metrics));
    fs::rename(index_path, pending.join(MEMBERS[3]))?;
    fs::remove_dir_all(pending.join("index"))?;
    File::open(pending.join(MEMBERS[3]))?.sync_all()?;
    let mut files = Vec::new();
    for name in MEMBERS {
        let path = pending.join(name);
        files.push(CompactFileV1 {
            name: name.into(), bytes: path.metadata()?.len(),
            sha256: file_sha256_with_progress(&path, interval, |done, total| {
                publication_progress("hidden_member_sha256", name, done, Some(total));
            })?,
        });
    }
    let manifest = CompactHiddenManifestV1 {version: VERSION.into(), source_manifest_sha256,
        summary, indexed_steps, normal_end: true, files};
    let mut receipt = File::create_new(pending.join("manifest.json"))?;
    serde_json::to_writer(&mut receipt, &manifest)?;
    receipt.sync_all()?;
    File::open(&pending)?.sync_all()?;
    rustix::fs::renameat_with(rustix::fs::CWD, &pending, rustix::fs::CWD, target, rustix::fs::RenameFlags::NOREPLACE)?;
    File::open(parent)?.sync_all()?;
    publication_progress("hidden_published", "manifest.json", 0, None);
    Ok(manifest)
}

/// 全局hidden时间桶使用本产物自身完成凭据；没有旧CommittedIngressPrefix。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosedCompactHiddenBucketV1 {
    pub source_prefix: AppliedIngressPrefixV1,
    pub material_manifest_sha256: String,
    pub consumer_stream_id: String,
    pub bucket_time_utc_ms: i64,
    pub index_start: u64,
    pub index_end: u64,
    pub steps: Vec<HiddenDerivedStepRefV1>,
    pub reference_positions: Vec<u64>,
    pub closure: ReplayIndexClosureV1,
}

pub struct CompactHiddenMaterialReaderV1 {
    pub manifest: CompactHiddenManifestV1,
    pub material_manifest_sha256: String,
    payload: File,
    offsets: Vec<u64>,
    /// canonical_position -> (packet_index,first_derivation,steps)。只读块内小固定列。
    messages: BTreeMap<u64, (usize, u64, u64, bool)>,
    source: PublicCompactReaderV1,
    cached_message: Option<CompactHiddenMessageV1>,
    index: RunCursor,
    next: Option<HiddenIndexEntryV1>,
    position: u64,
    read_steps: u64,
    read_references: u64,
    read_reports: u64,
    expected_indexed_steps: u64,
    expected_references: BTreeMap<u64,(i64,u64)>,
    reference_facts: BTreeMap<u64,u64>,
    failed: bool,
    last_key: Option<(i64,u64)>,
    max_packet_bytes: usize,
}
fn entry_key(entry: &HiddenIndexEntryV1) -> RecordingResult<(i64,u64)> {
    Ok(match entry {
        HiddenIndexEntryV1::Reference {source_at_utc_ms, derivation_position, ..} => (*source_at_utc_ms, *derivation_position),
        HiddenIndexEntryV1::Step(step) => (step.dependency_time.source_event_at_utc_ms
            .ok_or_else(|| io::Error::other("hidden索引步骤缺真实来源时间"))?, step.derivation_position),
    })
}
impl CompactHiddenMaterialReaderV1 {
    pub fn open(root: &Path, source_root: &Path, rows: usize, bytes: usize, max_packet_bytes: usize,
        mut progress: impl FnMut(&str, &str, u64, Option<u64>)) -> RecordingResult<Self> {
        let interval = Duration::from_secs(10);
        let manifest: CompactHiddenManifestV1 = serde_json::from_reader(File::open(root.join("manifest.json"))?)?;
        if manifest.version != VERSION || !manifest.normal_end || manifest.files.len() != MEMBERS.len() {
            return Err(io::Error::other("hidden材料完成凭据不合法").into());
        }
        if manifest.indexed_steps > manifest.summary.steps {
            return Err(io::Error::other("hidden索引步骤超出实际步骤数量").into());
        }
        let mut source = PublicCompactReaderV1::open_with_progress(source_root, max_packet_bytes, interval, |notice| {
            progress(notice.stage, notice.member, notice.completed_bytes, notice.total_bytes);
        })?;
        let source_sha = file_sha256_with_progress(&source_root.join("manifest.json"), interval, |done,total| {
            progress("source_receipt_sha256", "manifest.json", done, Some(total));
        })?;
        if source_sha != manifest.source_manifest_sha256
            || source.manifest.applied_prefix != manifest.summary.source_prefix
            || source.manifest.facts != manifest.summary.canonical_facts {
            return Err(io::Error::other("hidden来源完成凭据不匹配").into());
        }
        // 自身计数不能替代来源人口。只读原事实，不重跑任何业务归约。
        let mut expected_messages = BTreeSet::new();
        let mut expected_references = BTreeMap::new();
        let mut canonical = 0_u64;
        let mut last_notice = std::time::Instant::now();
        while let Some(packet) = source.next_packet()? {
            for fact in packet.groups.iter().flat_map(|group| &group.records) {
                match &fact.payload {
                    crate::compact_public::CompactFactPayloadV1::PmFromInput(_) => {expected_messages.insert(canonical);}
                    _=>{if let Some(at) = fact.dependency_time.source_event_at_utc_ms {
                        expected_references.insert(canonical,(at,u64::try_from(expected_references.len())?));
                    }}
                }
                canonical = canonical.checked_add(1).ok_or_else(|| io::Error::other("hidden来源事实位置溢出"))?;
            }
            if last_notice.elapsed()>=interval {
                progress("hidden_source_census","inputs",packet.input.input_position+1,Some(source.manifest.inputs));
                last_notice = std::time::Instant::now();
            }
        }
        progress("hidden_source_census_complete","inputs",source.manifest.inputs,Some(source.manifest.inputs));
        if canonical != manifest.summary.canonical_facts || expected_messages.len() as u64 != manifest.summary.messages
            || expected_references.len() as u64 != manifest.summary.references {
            return Err(io::Error::other("hidden消息/参考人口与原完整来源不闭合").into());
        }
        for (file,name) in manifest.files.iter().zip(MEMBERS) {
            if file.name != name || fs::metadata(root.join(name))?.len() != file.bytes {
                return Err(io::Error::other("hidden成员名称/大小不匹配").into());
            }
            let hash = file_sha256_with_progress(&root.join(name), interval, |done,total| {
                progress("hidden_read_member_sha256", name, done, Some(total));
            })?;
            if hash != file.sha256 { return Err(io::Error::other("hidden成员SHA不匹配").into()); }
        }
        let message_count = manifest.summary.messages;
        let offset_bytes = message_count.checked_add(1).and_then(|n| n.checked_mul(8))
            .ok_or_else(|| io::Error::other("hidden消息偏移列长度溢出"))?;
        let position_bytes = message_count.checked_mul(24)
            .ok_or_else(|| io::Error::other("hidden消息位置列长度溢出"))?;
        if manifest.files[1].bytes != offset_bytes || manifest.files[2].bytes != position_bytes {
            return Err(io::Error::other("hidden消息固定列长度不闭合").into());
        }
        let mut offsets_file = File::open(root.join(MEMBERS[1]))?;
        let mut offsets = Vec::new();
        for _ in 0..=message_count {
            let mut word = [0_u8;8]; offsets_file.read_exact(&mut word)?;
            offsets.push(u64::from_le_bytes(word));
        }
        if offsets.first()!=Some(&0) || offsets.last()!=Some(&manifest.files[0].bytes)
            || offsets.windows(2).any(|pair| pair[0]>=pair[1]) {
            return Err(io::Error::other("hidden消息载荷没有完整连续覆盖").into());
        }
        let mut positions = File::open(root.join(MEMBERS[2]))?;
        let mut messages = BTreeMap::new();
        let mut next_derivation = manifest.summary.references;
        for index in 0..message_count {
            let mut words = [0_u8;24]; positions.read_exact(&mut words)?;
            let canonical = u64::from_le_bytes(words[..8].try_into()?);
            let first = u64::from_le_bytes(words[8..16].try_into()?);
            let count = u64::from_le_bytes(words[16..].try_into()?);
            if canonical >= manifest.summary.canonical_facts || first != next_derivation
                || messages.insert(canonical,(usize::try_from(index)?,first,count,false)).is_some() {
                return Err(io::Error::other("hidden消息身份/派生固定列不闭合").into());
            }
            next_derivation = next_derivation.checked_add(count).ok_or_else(|| io::Error::other("hidden派生位置溢出"))?;
        }
        if !messages.keys().copied().eq(expected_messages.into_iter()) {
            return Err(io::Error::other("hidden实际消息位置未覆盖原PM事实人口").into());
        }
        if next_derivation.checked_sub(manifest.summary.references) != Some(manifest.summary.steps) {
            return Err(io::Error::other("hidden步骤末端计数不闭合").into());
        }
        let material_manifest_sha256 = file_sha256_with_progress(&root.join("manifest.json"), interval, |done,total| {
            progress("hidden_receipt_sha256", "manifest.json", done, Some(total));
        })?;
        Ok(Self {manifest, material_manifest_sha256, payload:File::open(root.join(MEMBERS[0]))?, offsets, messages,
            source, cached_message:None, read_reports:0, expected_indexed_steps:0, expected_references, reference_facts:BTreeMap::new(), failed:false,
            index:RunCursor::open(&root.join(MEMBERS[3]), rows, bytes)?, next:None, position:0,
            read_steps:0, read_references:0, last_key:None, max_packet_bytes})
    }

    pub fn message(&mut self, canonical_position: u64) -> RecordingResult<&CompactHiddenMessageV1> {
        if self.failed { return Err(io::Error::other("失败的hidden材料读取不能继续").into()); }
        self.failed = true;
        self.load_message(canonical_position)?;
        self.failed = false;
        Ok(self.cached_message.as_ref().unwrap())
    }

    fn load_message(&mut self, canonical_position: u64) -> RecordingResult<()> {
        if self.cached_message.as_ref().is_some_and(|packet| packet.canonical_position==canonical_position) {
            return Ok(());
        }
        let &(index,first,count,validated) = self.messages.get(&canonical_position)
            .ok_or_else(|| io::Error::other("hidden步骤缺实际消息关联"))?;
        let start = self.offsets[index]; let length = self.offsets[index+1]-start;
        if length > self.max_packet_bytes as u64 { return Err(io::Error::other("hidden包超过本次读取容量").into()); }
        let mut bytes = vec![0;usize::try_from(length)?];
        self.payload.seek(SeekFrom::Start(start))?; self.payload.read_exact(&mut bytes)?;
        let size = zstd::zstd_safe::get_frame_content_size(&bytes)
            .map_err(|error| io::Error::other(format!("hidden帧头错误: {error:?}")))?
            .map(usize::try_from).transpose()?.unwrap_or(self.max_packet_bytes);
        if size > self.max_packet_bytes { return Err(io::Error::other("hidden解压包超过本次读取容量").into()); }
        let packet: CompactHiddenMessageV1 = serde_json::from_slice(&zstd::bulk::decompress(&bytes,size)?)?;
        if packet.canonical_position != canonical_position || packet.steps.len() as u64 != count
            || packet.message.input_record.input_stream_id != self.manifest.summary.source_prefix.input_stream_id
            || packet.message.input_record.input_position >= self.manifest.summary.source_prefix.next_input_position {
            return Err(io::Error::other("hidden消息载荷/固定列或来源引用不闭合").into());
        }
        let original = self.source.canonical_fact(canonical_position)?;
        if !matches!(original.payload, crate::compact_public::CompactFactPayloadV1::PmFromInput(ref header) if header==&packet.message) {
            return Err(io::Error::other("hidden消息不是来源实际规范PM事实").into());
        }
        for (offset,(reference,step)) in packet.steps.iter().enumerate() {
            if reference.canonical_record_position != canonical_position
                || reference.derivation_position != first + offset as u64
                || reference.position.consumer_stream_id != self.manifest.summary.consumer_stream_id
                || reference.position.canonical_record_position != canonical_position
                || reference.canonical_fact_id != packet.message.event_id
                || step.canonical_fact_id != packet.message.event_id
                || reference.member_index != step.member_index || reference.step_index != step.step_index
                || reference.position.member_index != step.member_index
                || reference.position.step_index != step.step_index
                || reference.derived_id.0.0 != step.derived_id.0
                || reference.dependency_time != step.dependency_time {
                return Err(io::Error::other("hidden派生步骤身份/时间不闭合").into());
            }
            if let CompactPmChangeV1::InitialSnapshotFromInput {input_record, ..} = &step.change {
                if input_record != &packet.message.input_record { return Err(io::Error::other("hidden快照不是本消息原输入引用").into()); }
            }
            if matches!(step.change, CompactPmChangeV1::Inline(PmStepChangeV1::InitialSnapshot {..})) {
                return Err(io::Error::other("compact hidden快照必须引用原输入").into());
            }
            self.restore_materialized_step(&packet.message, step.clone())?;
        }
        for report in &packet.reports {
            if report.canonical_fact_id != packet.message.event_id {
                return Err(io::Error::other("hidden报告不属于实际原消息").into());
            }
            if let data_pipeline_core::pm_reduction::PmReductionDispositionV2::Produced {derived_id} = &report.disposition {
                if !packet.steps.iter().any(|(_, step)| &step.derived_id == derived_id) {
                    return Err(io::Error::other("hiddenProduced报告缺实际派生步骤").into());
                }
            }
        }
        if !validated {
            self.expected_indexed_steps = self.expected_indexed_steps.checked_add(u64::try_from(packet.steps.iter()
                .filter(|(reference,_)| reference.dependency_time.source_event_at_utc_ms.is_some()).count())?)
                .ok_or_else(|| io::Error::other("hidden可索引步骤人口溢出"))?;
            self.read_reports = self.read_reports.checked_add(u64::try_from(packet.reports.len())?)
                .ok_or_else(|| io::Error::other("hidden报告计数溢出"))?;
            self.messages.get_mut(&canonical_position).unwrap().3 = true;
        }
        self.cached_message = Some(packet);
        Ok(())
    }

    /// 冷块消费者复原原强类型步骤；快照仅从自身来源原消息取回，不归约public步骤。
    pub fn restore_step(&mut self, reference: &HiddenDerivedStepRefV1) -> RecordingResult<PmReductionStepV1<'static, SourceEvidenceRefV3>> {
        if self.failed { return Err(io::Error::other("失败的hidden材料读取不能继续").into()); }
        self.failed = true;
        let result = self.restore_step_inner(reference);
        if result.is_ok() { self.failed = false; }
        result
    }

    fn restore_step_inner(&mut self, reference: &HiddenDerivedStepRefV1) -> RecordingResult<PmReductionStepV1<'static, SourceEvidenceRefV3>> {
        self.load_message(reference.canonical_record_position)?;
        let packet = self.cached_message.as_ref().unwrap();
        let (_, step) = packet.steps.iter().find(|(stored, _)| stored==reference)
            .ok_or_else(|| io::Error::other("恢复步骤不是本材料实际引用"))?;
        let step = step.clone();
        let header = packet.message.clone();
        self.restore_materialized_step(&header, step)
    }

    fn restore_materialized_step(&mut self, header: &CompactPmMessageHeaderV1,
        step: CompactPmStepV1<CanonicalEventId>) -> RecordingResult<PmReductionStepV1<'static, SourceEvidenceRefV3>> {
        let change = match step.change {
            CompactPmChangeV1::Inline(change) => change,
            CompactPmChangeV1::InitialSnapshotFromInput {input_record, asset, initial_tick_size_pusd_per_share,
                source_sequence_epoch_id, source_cursor, book_digest_sha256, evidence} => {
                let input = self.source.cold_input_record(&input_record)?;
                let context = data_pipeline_core::pm_reduction::PmMarketContextV1 {
                    market: header.market.clone(), assets: vec![data_pipeline_core::reduction::PolymarketAssetContextV1 {
                        asset: asset.clone(), initial_tick_size_pusd_per_share: initial_tick_size_pusd_per_share.clone(),
                    }], identity_source_evidence: vec![], canonical_rule_version: header.lineage.canonical_rule_version.clone(),
                };
                let prepared = data_pipeline_core::public_input::prepare_ingress_v1(&input, Some(&context), &BTreeMap::new(), &[],
                    &header.lineage.canonical_rule_version)?;
                let data_pipeline_core::public_input::PreparedCanonicalInputV1::PolymarketMessage(message) = prepared.current else {
                    return Err(io::Error::other("hidden快照来源不是原PM消息").into());
                };
                let original = message.message();
                if original.event_id != header.event_id || original.group_id != header.group_id
                    || original.market != header.market || original.lineage != header.lineage
                    || original.time != header.time || original.input_record != header.input_record {
                    return Err(io::Error::other("hidden快照原消息身份不一致").into());
                }
                let CanonicalPmPayloadV1::Book {fact: CanonicalBookFactV1::FullBook {asset: source_asset,bids,asks,..}} = &original.payload else {
                    return Err(io::Error::other("hidden初始快照不引用原FullBook").into());
                };
                if source_asset != &asset { return Err(io::Error::other("hidden初始快照资产不匹配").into()); }
                let source = original.lineage.source_evidence.first()
                    .ok_or_else(|| io::Error::other("hidden原消息缺来源证据"))?;
                let admission = source.collector_admission.as_ref()
                    .ok_or_else(|| io::Error::other("hidden原消息缺准入身份"))?;
                if admission.source_sequence_epoch_id!=source_sequence_epoch_id || admission.collector_admission_sequence!=source_cursor {
                    return Err(io::Error::other("hidden初始快照代际/原游标不匹配").into());
                }
                use data_pipeline_core::pm_reduction::PmEvidenceV1;
                let bids_map = bids.iter().map(|level|
                    Ok((data_pipeline_core::parse_canonical_decimal(&level.price_pusd_per_share,"snapshot.price")?,level)))
                    .collect::<Result<BTreeMap<_,_>,data_pipeline_core::ContractError>>()?;
                let asks_map = asks.iter().map(|level|
                    Ok((data_pipeline_core::parse_canonical_decimal(&level.price_pusd_per_share,"snapshot.price")?,level)))
                    .collect::<Result<BTreeMap<_,_>,data_pipeline_core::ContractError>>()?;
                if message.evidence().prepared_book_digest(&asset,&bids_map,&asks_map)? != book_digest_sha256 {
                    return Err(io::Error::other("hidden快照digest不匹配原源值").into());
                }
                PmStepChangeV1::InitialSnapshot {asset,initial_tick_size_pusd_per_share,source_sequence_epoch_id,source_cursor,
                    book_digest_sha256,bids:std::borrow::Cow::Owned(bids.clone()),asks:std::borrow::Cow::Owned(asks.clone()),evidence}
            }
        };
        let restored = PmReductionStepV1 {canonical_fact_id:step.canonical_fact_id,member_index:step.member_index,step_index:step.step_index,
            derived_id:step.derived_id,change,direct:step.direct,prestate:step.prestate,absence:step.absence,dependencies:step.dependencies,
            direct_time:step.direct_time,dependency_time:step.dependency_time,quality:step.quality};
        if data_pipeline_core::pm_reduction::materialized_pm_step_id_v1(&restored,&header.market,
            &header.lineage.canonical_rule_version)? != restored.derived_id {
            return Err(io::Error::other("hidden步骤恢复内容与原物化身份不一致").into());
        }
        Ok(restored)
    }

    fn read_index(&mut self) -> RecordingResult<Option<HiddenIndexEntryV1>> {
        let Some(row) = self.index.next_row()? else {
            // 仅报告/无索引的消息也必须完整校验；不让漏报告绕过完成凭据。
            let unseen: Vec<_> = self.messages.iter().filter_map(|(position, value)| (!value.3).then_some(*position)).collect();
            for position in unseen { self.load_message(position)?; }
            if self.read_steps != self.manifest.indexed_steps || self.read_steps != self.expected_indexed_steps
                || self.read_references != self.manifest.summary.references {
                return Err(io::Error::other("hidden时间索引末端计数不闭合").into());
            }
            if self.read_reports != self.manifest.summary.reports {
                return Err(io::Error::other("hidden报告末端计数不闭合").into());
            }
            if self.reference_facts.values().enumerate().any(|(ordinal, position)| *position != ordinal as u64) {
                return Err(io::Error::other("hidden参考派生位置不沿原规范事实顺序连续").into());
            }
            return Ok(None);
        };
        let entry: HiddenIndexEntryV1 = serde_json::from_slice(row.payload())?;
        let key = entry_key(&entry)?;
        let mut encoded = ((key.0 as u64) ^ (1<<63)).to_be_bytes().to_vec();
        encoded.extend(key.1.to_be_bytes());
        if row.sort_key_bytes()!=encoded || self.last_key.is_some_and(|last| key<=last) {
            return Err(io::Error::other("hidden时间键/派生次序不闭合").into());
        }
        match &entry {
            HiddenIndexEntryV1::Step(reference) => {
                self.load_message(reference.canonical_record_position)?;
                let packet = self.cached_message.as_ref().unwrap();
                if !packet.steps.iter().any(|(stored,_)| stored==reference) {
                    return Err(io::Error::other("hidden时间索引不指向实际步骤").into());
                }
                self.read_steps += 1;
            }
            HiddenIndexEntryV1::Reference {canonical_position,derivation_position,source_at_utc_ms} => {
                if *canonical_position>=self.manifest.summary.canonical_facts
                    || *derivation_position>=self.manifest.summary.references
                    || self.reference_facts.insert(*canonical_position,*derivation_position).is_some() {
                    return Err(io::Error::other("hidden参考位置越过自身来源范围").into());
                }
                if self.expected_references.get(canonical_position) != Some(&(*source_at_utc_ms,*derivation_position)) {
                    return Err(io::Error::other("hidden参考不指向原真实来源时间的非PM事实").into());
                }
                self.read_references += 1;
            }
        }
        self.last_key = Some(key);
        Ok(Some(entry))
    }

    /// 看见真实下一全局时间或完整EOF才关闭桶；Parquet页末不关闭时间。
    pub fn next_bucket(&mut self) -> RecordingResult<Option<ClosedCompactHiddenBucketV1>> {
        if self.failed { return Err(io::Error::other("失败的hidden材料读取不能继续").into()); }
        self.failed = true;
        let result = self.next_bucket_inner();
        if result.is_ok() { self.failed = false; }
        result
    }

    fn next_bucket_inner(&mut self) -> RecordingResult<Option<ClosedCompactHiddenBucketV1>> {
        let first = match self.next.take() {Some(value)=>value, None=>match self.read_index()? {
            Some(value)=>value, None=>return Ok(None),
        }};
        let at = entry_key(&first)?.0; let start = self.position;
        let mut current = first; let mut steps = Vec::new(); let mut references = Vec::new();
        let closure = loop {
            match current {HiddenIndexEntryV1::Step(step)=>steps.push(step),
                HiddenIndexEntryV1::Reference {..}=>references.push(self.position)};
            self.position += 1;
            match self.read_index()? {
                None=>break ReplayIndexClosureV1::EndOfInput,
                Some(value) if entry_key(&value)?.0 != at=>{
                    let next_at = entry_key(&value)?.0; self.next = Some(value);
                    break ReplayIndexClosureV1::NextGlobalEventAt(next_at);
                }
                Some(value)=>current=value,
            }
        };
        Ok(Some(ClosedCompactHiddenBucketV1 {
            source_prefix:self.manifest.summary.source_prefix.clone(), material_manifest_sha256:self.material_manifest_sha256.clone(),
            consumer_stream_id:self.manifest.summary.consumer_stream_id.clone(), bucket_time_utc_ms:at,
            index_start:start,index_end:self.position,steps,reference_positions:references,closure,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{compact_public::PublicCompactWriterV1, gpu_initial_sort::GpuInitialRunOrderV1,
        public_recording::tests::{book, change, config, core, end, meta, pm, record, row, twap_input, twap_record}};
    use data_pipeline_core::{public_input::{TypedRawRowV2, prepare_public_ingress_v1},
        typed_raw::{PolymarketOrderbookEvent, RtdsFreshnessStatus, TypedRawRow}};

    fn read_all(root: &Path, source: &Path, rows: usize) -> Vec<ClosedCompactHiddenBucketV1> {
        let mut reader = CompactHiddenMaterialReaderV1::open(root, source, rows, 1<<20, 1<<20, |_,_,_,_| {}).unwrap();
        let mut result = Vec::new();
        let mut snapshots = Vec::new();
        let mut transitions = Vec::new();
        while let Some(bucket) = reader.next_bucket().unwrap() {
            for reference in &bucket.steps {
                let step = reader.restore_step(reference).unwrap();
                match step.change {
                    PmStepChangeV1::InitialSnapshot {bids, ..} => {
                        assert_eq!(bids.len(),1);
                        snapshots.push(bids[0].quantity_shares.0.clone());
                    }
                    PmStepChangeV1::LevelChange {transition, ..} if transition.side==BookSide::Bid => {
                        transitions.push((transition.quantity_before_shares.0,transition.quantity_after_shares.0));
                    }
                    _=>{}
                }
            }
            result.push(bucket);
        }
        assert_eq!(snapshots, vec!["10"]);
        // 已初始化资产的epoch恢复沿原Reset+重校准转移，不伪造第二次InitialSnapshot。
        // 消息归约按epoch；发布索引按原真实时间，此反例的第二代20100先于前代20200。
        assert_eq!(transitions,vec![("0".into(),"15".into()),("10".into(),"20".into())]);
        assert_eq!(result.iter().map(|bucket| bucket.steps.len()).sum::<usize>(),15);
        assert_eq!(result.iter().map(|bucket| bucket.reference_positions.len()).sum::<usize>(),1);
        assert!(result.iter().any(|bucket| bucket.steps.len()>1)); // rows=1仍不可提前关闭同毫秒桶。
        assert!(result.iter().any(|bucket| bucket.steps.is_empty() && !bucket.reference_positions.is_empty()));
        assert!(matches!(result.last().unwrap().closure,ReplayIndexClosureV1::EndOfInput));
        for adjacent in result.windows(2) {
            assert_eq!(adjacent[0].closure,ReplayIndexClosureV1::NextGlobalEventAt(adjacent[1].bucket_time_utc_ms));
            assert_eq!(adjacent[0].index_end,adjacent[1].index_start);
        }
        result
    }

    #[test]
    #[ignore = "AutoDL准确提交GPU排序、自身发布、原快照恢复和完整hidden桶"]
    fn gpu_compact_hidden_material_roundtrip_closed_buckets_and_rejects_bad_count() {
        let temp = tempfile::tempdir().unwrap();
        let source_root = temp.path().join("public");
        let target = temp.path().join("hidden");
        let mut public = core();
        let mut writer = PublicCompactWriterV1::create(&source_root,public.header().clone()).unwrap();
        let mut anomaly = twap_input(60);
        anomaly.common.freshness_status = RtdsFreshnessStatus::InvalidTimestamp;
        for input in [meta(0), pm(1,1,20050,book("10")),
            pm(2,2,20200,PolymarketOrderbookEvent::PriceChange {price_changes:vec![change(Some("20"))]}),
            record(3,TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-b",1,20100,23000,book("15"))))),
            twap_record(4,30,25002,"65000"), record(5,TypedRawRowV2::PolymarketRtdsChainlinkTwap(anomaly)),end(6)] {
            let prepared = prepare_public_ingress_v1(&public,&input,&BTreeMap::new(),&[]).unwrap();
            let applied = public.push_facts(&prepared).unwrap();
            writer.push_prepared(&input,&prepared,&applied).unwrap();
        }
        writer.publish().unwrap();
        let order = Arc::new(GpuInitialRunOrderV1::start(Path::new(&std::env::var("B_GPU_PYTHON").unwrap()),
            Path::new(&std::env::var("B_GPU_SORT_WORKER").unwrap())).unwrap());
        let mut sort = config().sort; sort.merge_workers = 8;
        let manifest = prepare_compact_hidden_material_v1(&source_root,&target,1<<20,&sort,order,
            "original-hidden-consumer".into(),None,|_,_,_,_| {}).unwrap();
        assert_eq!(manifest.summary.messages,3);
        assert_eq!(manifest.summary.steps,15);
        assert_eq!(manifest.summary.reports,6);
        assert_eq!(fs::read_dir(&target).unwrap().count(),MEMBERS.len()+1);
        // 原输入才持久保存盘口；hidden快照步骤只有输入引用，没有再编码整侧。
        let mut reader = CompactHiddenMaterialReaderV1::open(&target,&source_root,1,1<<20,1<<20,|_,_,_,_| {}).unwrap();
        let positions:Vec<_> = reader.messages.keys().copied().collect();
        let mut snapshot_refs = 0;
        for position in positions {
            let packet = reader.message(position).unwrap();
            for (_,step) in &packet.steps {
                if matches!(step.change,CompactPmChangeV1::InitialSnapshotFromInput {..}) { snapshot_refs += 1; }
            }
        }
        assert_eq!(snapshot_refs,1);
        assert_eq!(read_all(&target,&source_root,1),read_all(&target,&source_root,64));
        // 合法精确tick替换、重新压缩并更新成员hash，仍必须绑定原物化身份。
        let original_payload = fs::read(target.join(MEMBERS[0])).unwrap();
        let original_offsets = fs::read(target.join(MEMBERS[1])).unwrap();
        let offsets:Vec<_> = original_offsets.chunks_exact(8).map(|word| u64::from_le_bytes(word.try_into().unwrap()) as usize).collect();
        let mut bad_payload = Vec::new(); let mut bad_offsets = 0_u64.to_le_bytes().to_vec();
        let mut modified = false; let mut modified_position = 0;
        for pair in offsets.windows(2) {
            let mut packet:CompactHiddenMessageV1 = serde_json::from_slice(&zstd::bulk::decompress(
                &original_payload[pair[0]..pair[1]],1<<20).unwrap()).unwrap();
            for (_,step) in &mut packet.steps {
                if let CompactPmChangeV1::InitialSnapshotFromInput {initial_tick_size_pusd_per_share,..} = &mut step.change {
                    *initial_tick_size_pusd_per_share = ExactDecimal("0.001".into());
                    modified = true;modified_position = packet.canonical_position;
                }
            }
            bad_payload.extend(zstd::bulk::compress(&serde_json::to_vec(&packet).unwrap(),3).unwrap());
            bad_offsets.extend((bad_payload.len() as u64).to_le_bytes());
        }
        assert!(modified);
        fs::write(target.join(MEMBERS[0]),&bad_payload).unwrap();
        fs::write(target.join(MEMBERS[1]),&bad_offsets).unwrap();
        let mut bad_tick = serde_json::to_value(&manifest).unwrap();
        for index in 0..2 {
            let path = target.join(MEMBERS[index]);
            bad_tick["files"][index]["bytes"] = serde_json::json!(fs::metadata(&path).unwrap().len());
            bad_tick["files"][index]["sha256"] = serde_json::json!(file_sha256_with_progress(&path,Duration::from_secs(10),|_,_| {}).unwrap());
        }
        serde_json::to_writer(File::create(target.join("manifest.json")).unwrap(),&bad_tick).unwrap();
        let mut broken_tick = CompactHiddenMaterialReaderV1::open(&target,&source_root,1,1<<20,1<<20,|_,_,_,_| {}).unwrap();
        assert!(broken_tick.message(modified_position).is_err());
        assert!(broken_tick.next_bucket().is_err());
        fs::write(target.join(MEMBERS[0]),original_payload).unwrap();
        fs::write(target.join(MEMBERS[1]),original_offsets).unwrap();
        serde_json::to_writer(File::create(target.join("manifest.json")).unwrap(),&manifest).unwrap();
        // 成员SHA不变仍不能通过被修改的自身计数；不重新归约业务。
        let mut bad:serde_json::Value = serde_json::from_reader(File::open(target.join("manifest.json")).unwrap()).unwrap();
        bad["summary"]["steps"] = serde_json::json!(16);
        serde_json::to_writer(File::create(target.join("manifest.json")).unwrap(),&bad).unwrap();
        assert!(CompactHiddenMaterialReaderV1::open(&target,&source_root,1,1<<20,1<<20,|_,_,_,_| {}).is_err());
        serde_json::to_writer(File::create(target.join("manifest.json")).unwrap(),&manifest).unwrap();
        let mut original = RunCursor::open(&target.join(MEMBERS[3]),1,1<<20).unwrap();
        let mut replacement = ExternalSorter::new(&temp.path().join("tamper-index"),&sort).unwrap();
        let mut removed = false;
        while let Some(row) = original.next_row().unwrap() {
            let entry:HiddenIndexEntryV1 = serde_json::from_slice(row.payload()).unwrap();
            if !removed && matches!(entry,HiddenIndexEntryV1::Step(_)) {removed = true;continue;}
            replacement.push(row).unwrap();
        }
        assert!(removed);
        let (index,_) = replacement.finish_single_file(None).unwrap();
        fs::rename(index,target.join(MEMBERS[3])).unwrap();
        let mut tampered:serde_json::Value = serde_json::to_value(&manifest).unwrap();
        tampered["indexed_steps"] = serde_json::json!(manifest.indexed_steps-1);
        tampered["files"][3]["bytes"] = serde_json::json!(fs::metadata(target.join(MEMBERS[3])).unwrap().len());
        tampered["files"][3]["sha256"] = serde_json::json!(file_sha256_with_progress(&target.join(MEMBERS[3]),
            Duration::from_secs(10),|_,_| {}).unwrap());
        serde_json::to_writer(File::create(target.join("manifest.json")).unwrap(),&tampered).unwrap();
        let mut broken = CompactHiddenMaterialReaderV1::open(&target,&source_root,1,1<<20,1<<20,|_,_,_,_| {}).unwrap();
        loop {
            match broken.next_bucket() {
                Ok(Some(_))=>{},
                Ok(None)=>panic!("删掉有真实时间的原步骤不能通过完整EOF"),
                Err(_)=>break,
            }
        }
        assert!(broken.next_bucket().is_err());
        eprintln!("compact_hidden_material_roundtrip={}",serde_json::to_string(&manifest).unwrap());
    }
}
