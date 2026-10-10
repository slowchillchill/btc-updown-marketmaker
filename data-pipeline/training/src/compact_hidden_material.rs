//! 自身完成凭据的compact hidden派生产物；消息快照引用原输入，索引按原真实时间全局排序。
use crate::{
    compact_hidden::{CompactHiddenSinkV1, CompactHiddenSummaryV1, reduce_compact_hidden_v1},
    compact_public::{CompactFileV1, CompactPmChangeV1, CompactPmMessageHeaderV1, CompactPmStepV1,
        PublicCompactReaderV1, file_sha256_with_progress},
    hidden_preparation::HiddenIndexEntryV1,
    historical_preparation_progress::HistoricalPreparationNotifierV1,
    public_recording::RecordingResult,
    ExternalSorter, InitialRunOrder, PartCursor, SortedRun, SortConfig, SortRow,
};
use contracts::*;
use data_pipeline_core::pm_reduction::{MaterializedPmReductionV1, PmReductionStepV1,
    PmReductionReportV2, PmStepChangeV1, PreparedPmMessageV1};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write}, path::Path,
    sync::Arc, time::Duration};
use std::os::unix::fs::FileExt;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactHiddenPreparationConfigV1 {
    pub max_packet_bytes: usize,
    pub sort: SortConfig,
    pub consumer_stream_id: String,
}

const VERSION_V1: &str = "compact_hidden_source_steps_v1";
const VERSION: &str = "compact_hidden_source_steps_v2";
const MEMBERS: [&str; 4] = ["hidden_messages.bin", "message_offsets.u64le", "message_positions.u64le", "hidden_index.parquet"];

fn index_member(part: usize) -> String {
    if part == 0 { MEMBERS[3].into() }
    else { crate::part_path(Path::new(MEMBERS[3]), part).file_name().unwrap().to_string_lossy().into_owned() }
}

// 同一最终索引移交用于生产发布；只保存已有有序片，不生成第二份清单。
pub(super) fn publish_index_parts(run: SortedRun, pending: &Path, interval: Duration,
    progress: &mut impl FnMut(&str, &str, u64, Option<u64>)) -> RecordingResult<Vec<CompactFileV1>> {
    let mut files = Vec::with_capacity(run.parts.len());
    for (part, source) in run.parts.into_iter().enumerate() {
        let name = index_member(part);
        let path = pending.join(&name);
        fs::rename(source, &path)?;
        File::open(&path)?.sync_all()?;
        files.push(CompactFileV1 {name: name.clone(), bytes: path.metadata()?.len(),
            sha256: file_sha256_with_progress(&path, interval, |done,total| {
                progress("hidden_member_sha256", &name, done, Some(total));
            })?});
    }
    Ok(files)
}

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
    publication_progress: impl FnMut(&str, &str, u64, Option<u64>),
) -> RecordingResult<CompactHiddenManifestV1> {
    prepare_compact_hidden_material_with_interval_v1(source_root,target,max_packet_bytes,config,
        initial_order,consumer_stream_id,progress,Duration::from_secs(10),publication_progress)
}

pub fn prepare_compact_hidden_material_with_interval_v1(
    source_root: &Path, target: &Path, max_packet_bytes: usize, config: &SortConfig,
    initial_order: Arc<dyn InitialRunOrder>, consumer_stream_id: String,
    progress: Option<&HistoricalPreparationNotifierV1<'_>>, interval: Duration,
    mut publication_progress: impl FnMut(&str, &str, u64, Option<u64>),
) -> RecordingResult<CompactHiddenManifestV1> {
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
    crate::historical_preparation_progress::notify(progress,
        crate::historical_preparation_progress::HistoricalPreparationNoticeV1::Stage(
            crate::historical_preparation_progress::HistoricalPreparationStageV1::HiddenFinalize));
    let (index_run, index_metrics) = index.finish_with_progress(&mut |notice| {
        crate::historical_preparation_progress::notify(progress,
            crate::historical_preparation_progress::HistoricalPreparationNoticeV1::SourceMergeProgress {index: 0, progress: notice});
    })?;
    crate::historical_preparation_progress::notify(progress,
        crate::historical_preparation_progress::HistoricalPreparationNoticeV1::HiddenIndexSorted(index_metrics));
    let index_files = publish_index_parts(index_run, &pending, interval, &mut publication_progress)?;
    fs::remove_dir_all(pending.join("index"))?;
    let mut files = Vec::new();
    for name in &MEMBERS[..3] {
        let path = pending.join(name);
        files.push(CompactFileV1 {
            name: (*name).into(), bytes: path.metadata()?.len(),
            sha256: file_sha256_with_progress(&path, interval, |done, total| {
                publication_progress("hidden_member_sha256", name, done, Some(total));
            })?,
        });
    }
    files.extend(index_files);
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
    /// 原首次FixedIdentity，只作SIM冷目录身份；不代表公开运行前缀已接纳。
    pub source_scene: BTreeMap<String, (MarketIdentityV2, Vec<AssetIdentityV1>)>,
    payload: File,
    offsets: File,
    message_positions: File,
    /// 原完整校验扫描顺写canonical -> 消息行+1；0非PM，MAX为尚未绑定PM。
    /// 临时列位于调用方TMPDIR，同reader复用；不发布为第二份材料。
    message_rows: File,
    message_validated: File,
    source: PublicCompactReaderV1,
    cached_message: Option<CompactHiddenMessageV1>,
    /// 仅当前消息的已验收恢复结果；切换消息即替换，不缓存整段历史。
    cached_steps: Vec<PmReductionStepV1<'static, SourceEvidenceRefV3>>,
    #[cfg(test)]
    message_loads: u64,
    index: PartCursor,
    next: Option<HiddenIndexEntryV1>,
    position: u64,
    read_steps: u64,
    read_references: u64,
    read_reports: u64,
    expected_indexed_steps: u64,
    /// 按原canonical参考派生序顺写(canonical,time)，逐项核对而非只比总数。
    expected_references: File,
    /// 已验收索引行 -> canonical参考；步骤行用MAX，不限制旧位置随机读取。
    reference_locations: BufWriter<File>,
    failed: bool,
    last_key: Option<(i64,u64)>,
    max_packet_bytes: usize,
}
fn read_u64_column(file: &File, row: u64) -> RecordingResult<u64> {
    let mut bytes = [0_u8;8];
    let offset = row.checked_mul(8).ok_or_else(|| io::Error::other("hidden定位列偏移溢出"))?;
    file.read_exact_at(&mut bytes,offset)?;
    Ok(u64::from_le_bytes(bytes))
}
fn write_u64_column(file: &File, row: u64, value: u64) -> RecordingResult<()> {
    let offset = row.checked_mul(8).ok_or_else(|| io::Error::other("hidden定位列偏移溢出"))?;
    file.write_all_at(&value.to_le_bytes(),offset)?;
    Ok(())
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
        progress: impl FnMut(&str, &str, u64, Option<u64>)) -> RecordingResult<Self> {
        Self::open_with_progress_interval(root,source_root,rows,bytes,max_packet_bytes,Duration::from_secs(10),progress)
    }

    pub fn open_with_progress_interval(root: &Path, source_root: &Path, rows: usize, bytes: usize,
        max_packet_bytes: usize, interval: Duration,
        mut progress: impl FnMut(&str, &str, u64, Option<u64>)) -> RecordingResult<Self> {
        let manifest: CompactHiddenManifestV1 = serde_json::from_reader(File::open(root.join("manifest.json"))?)?;
        let valid_members = match manifest.version.as_str() {
            VERSION_V1 => manifest.files.len() == MEMBERS.len(),
            VERSION => manifest.files.len() >= MEMBERS.len(),
            _ => false,
        };
        if !valid_members || !manifest.normal_end {
            return Err(io::Error::other("hidden材料完成凭据不合法").into());
        }
        for (position, file) in manifest.files.iter().enumerate() {
            let expected = if position < 3 { MEMBERS[position].into() } else { index_member(position-3) };
            if file.name != expected { return Err(io::Error::other("hidden成员名称/顺序不匹配").into()); }
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
        let mut message_rows = BufWriter::new(tempfile::tempfile()?);
        let mut expected_references = BufWriter::new(tempfile::tempfile()?);
        let mut message_count = 0_u64; let mut reference_count = 0_u64;
        let mut source_scene = BTreeMap::new();
        let mut canonical = 0_u64;
        let mut last_notice = std::time::Instant::now();
        while let Some(packet) = source.next_packet()? {
            for fact in packet.groups.iter().flat_map(|group| &group.records) {
                if let crate::compact_public::CompactFactPayloadV1::Inline(PublicBusinessPayloadV1::MarketFact {
                    market, payload:CanonicalMarketFactPayloadV1::FixedIdentity {assets},
                }) = &fact.payload {
                    source_scene.entry(market.condition_id.0.clone()).or_insert_with(||(market.clone(),assets.clone()));
                }
                let message_row = match &fact.payload {
                    crate::compact_public::CompactFactPayloadV1::PmFromInput(_) => {
                        message_count += 1; u64::MAX
                    },
                    _=>{
                        if let Some(at) = fact.dependency_time.source_event_at_utc_ms {
                            expected_references.write_all(&canonical.to_le_bytes())?;
                            expected_references.write_all(&at.to_le_bytes())?;
                            reference_count += 1;
                        }
                        0
                    },
                };
                // PublicCompactReader逐组证明next_fact连续、固定列与payload相等。
                message_rows.write_all(&message_row.to_le_bytes())?;
                canonical = canonical.checked_add(1).ok_or_else(|| io::Error::other("hidden来源事实位置溢出"))?;
            }
            if !interval.is_zero() && last_notice.elapsed()>=interval {
                progress("hidden_source_census","inputs",packet.input.input_position+1,Some(source.manifest.inputs));
                last_notice = std::time::Instant::now();
            }
        }
        progress("hidden_source_census_complete","inputs",source.manifest.inputs,Some(source.manifest.inputs));
        if canonical != manifest.summary.canonical_facts || message_count != manifest.summary.messages
            || reference_count != manifest.summary.references {
            return Err(io::Error::other("hidden消息/参考人口与原完整来源不闭合").into());
        }
        let message_rows = message_rows.into_inner()?;
        let expected_references = expected_references.into_inner()?;
        for file in &manifest.files {
            let name = file.name.as_str();
            if fs::metadata(root.join(name))?.len() != file.bytes {
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
        let offsets = File::open(root.join(MEMBERS[1]))?;
        let mut offset_cursor = BufReader::new(&offsets);
        let mut word = [0_u8;8]; offset_cursor.read_exact(&mut word)?;
        let mut previous = u64::from_le_bytes(word);
        if previous != 0 { return Err(io::Error::other("hidden消息载荷没有完整连续覆盖").into()); }
        for _ in 0..message_count {
            offset_cursor.read_exact(&mut word)?;
            let next = u64::from_le_bytes(word);
            if previous >= next { return Err(io::Error::other("hidden消息载荷没有完整连续覆盖").into()); }
            previous = next;
        }
        if previous != manifest.files[0].bytes {
            return Err(io::Error::other("hidden消息载荷没有完整连续覆盖").into());
        }
        let message_positions = File::open(root.join(MEMBERS[2]))?;
        let mut position_cursor = BufReader::new(&message_positions);
        let mut next_derivation = manifest.summary.references;
        for index in 0..message_count {
            let mut words = [0_u8;24]; position_cursor.read_exact(&mut words)?;
            let canonical = u64::from_le_bytes(words[..8].try_into()?);
            let first = u64::from_le_bytes(words[8..16].try_into()?);
            let count = u64::from_le_bytes(words[16..].try_into()?);
            if canonical >= manifest.summary.canonical_facts || first != next_derivation
                || read_u64_column(&message_rows,canonical)? != u64::MAX {
                return Err(io::Error::other("hidden消息身份/派生固定列不闭合").into());
            }
            write_u64_column(&message_rows,canonical,index.checked_add(1)
                .ok_or_else(|| io::Error::other("hidden消息行定位溢出"))?)?;
            next_derivation = next_derivation.checked_add(count).ok_or_else(|| io::Error::other("hidden派生位置溢出"))?;
        }
        // 原来源PM人口与唯一绑定消息行数相同，故不存在未绑定的PM或多余消息。
        let message_validated = tempfile::tempfile()?; message_validated.set_len(message_count)?;
        let reference_locations = BufWriter::new(tempfile::tempfile()?);
        if next_derivation.checked_sub(manifest.summary.references) != Some(manifest.summary.steps) {
            return Err(io::Error::other("hidden步骤末端计数不闭合").into());
        }
        let material_manifest_sha256 = file_sha256_with_progress(&root.join("manifest.json"), interval, |done,total| {
            progress("hidden_receipt_sha256", "manifest.json", done, Some(total));
        })?;
        let index = PartCursor::open(&SortedRun {parts: manifest.files[3..].iter().map(|file| root.join(&file.name)).collect()}, rows, bytes)?;
        Ok(Self {manifest, material_manifest_sha256, source_scene, payload:File::open(root.join(MEMBERS[0]))?, offsets, message_positions,
            message_rows, message_validated,
            source, cached_message:None, cached_steps:Vec::new(),
            #[cfg(test)] message_loads:0, read_reports:0, expected_indexed_steps:0, expected_references, reference_locations, failed:false,
            index, next:None, position:0,
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
        #[cfg(test)] { self.message_loads += 1; }
        if canonical_position >= self.manifest.summary.canonical_facts {
            return Err(io::Error::other("hidden步骤缺实际消息关联").into());
        }
        let row = read_u64_column(&self.message_rows,canonical_position)?;
        if row==0 || row==u64::MAX { return Err(io::Error::other("hidden步骤缺实际消息关联").into()); }
        let index = row-1;
        let (_,first,count) = self.message_position(index)?;
        let mut flag = [0_u8;1]; self.message_validated.read_exact_at(&mut flag,index)?;
        let mut offsets = [0_u8;16];
        self.offsets.read_exact_at(&mut offsets,index.checked_mul(8)
            .ok_or_else(|| io::Error::other("hidden消息偏移溢出"))?)?;
        let start = u64::from_le_bytes(offsets[..8].try_into()?);
        let length = u64::from_le_bytes(offsets[8..].try_into()?).checked_sub(start)
            .ok_or_else(|| io::Error::other("hidden消息载荷没有完整连续覆盖"))?;
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
        let mut restored_steps = Vec::with_capacity(packet.steps.len());
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
            restored_steps.push(self.restore_materialized_step(&packet.message, step.clone())?);
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
        if flag[0]==0 {
            self.expected_indexed_steps = self.expected_indexed_steps.checked_add(u64::try_from(packet.steps.iter()
                .filter(|(reference,_)| reference.dependency_time.source_event_at_utc_ms.is_some()).count())?)
                .ok_or_else(|| io::Error::other("hidden可索引步骤人口溢出"))?;
            self.read_reports = self.read_reports.checked_add(u64::try_from(packet.reports.len())?)
                .ok_or_else(|| io::Error::other("hidden报告计数溢出"))?;
            self.message_validated.write_all_at(&[1],index)?;
        }
        self.cached_steps = restored_steps;
        self.cached_message = Some(packet);
        Ok(())
    }

    /// 只恢复已验证hidden索引的真实参考，不生成公开可见性或旧稳定前缀。
    pub fn reference_fact(&mut self, index_position: u64) -> RecordingResult<crate::compact_public::CompactFactV1> {
        if self.failed { return Err(io::Error::other("失败的hidden材料读取不能继续").into()); }
        self.failed = true;
        let canonical = self.reference_canonical(index_position)?;
        let fact = self.source.canonical_fact(canonical)?;
        self.failed = false;
        Ok(fact)
    }

    /// 一次读取已验证引用的完整事实及原canonical市场身份，保持原组和来源顺序。
    pub fn reference_fact_with_market(&mut self, index_position: u64)
        -> RecordingResult<(crate::compact_public::CompactFactV1, Option<CanonicalMarketFactV1>)> {
        if self.failed {return Err(io::Error::other("失败的hidden材料读取不能继续").into());}
        self.failed = true;
        let canonical = self.reference_canonical(index_position)?;
        let fact = self.source.canonical_fact_with_market(canonical)?;
        self.failed = false;
        Ok(fact)
    }

    /// 冷块消费者复原原强类型步骤；快照仅从自身来源原消息取回，不归约public步骤。
    pub fn restore_step(&mut self, reference: &HiddenDerivedStepRefV1) -> RecordingResult<PmReductionStepV1<'static, SourceEvidenceRefV3>> {
        if self.failed { return Err(io::Error::other("失败的hidden材料读取不能继续").into()); }
        self.failed = true;
        let result = self.restore_step_inner(reference);
        if result.is_ok() { self.failed = false; }
        result
    }

    /// 同一已验证原消息的市场身份与步骤；供所属SIM批次转换，不复制整消息载荷。
    /// 读取/恢复只提供材料，不推进hidden运行前沿或公开反馈可见性。
    pub fn restore_step_with_market(&mut self, reference: &HiddenDerivedStepRefV1)
        -> RecordingResult<(MarketIdentityV2, PmReductionStepV1<'static, SourceEvidenceRefV3>)> {
        let step = self.restore_step(reference)?;
        let market = self.cached_message.as_ref().unwrap().message.market.clone();
        Ok((market, step))
    }

    fn restore_step_inner(&mut self, reference: &HiddenDerivedStepRefV1) -> RecordingResult<PmReductionStepV1<'static, SourceEvidenceRefV3>> {
        self.load_message(reference.canonical_record_position)?;
        let index = self.cached_step_index(reference)?;
        Ok(self.cached_steps[index].clone())
    }

    fn cached_step_index(&self, reference: &HiddenDerivedStepRefV1) -> RecordingResult<usize> {
        let packet = self.cached_message.as_ref().unwrap();
        let first = packet.steps.first().ok_or_else(|| io::Error::other("恢复步骤不是本材料实际引用"))?.0.derivation_position;
        let index = usize::try_from(reference.derivation_position.checked_sub(first)
            .ok_or_else(|| io::Error::other("恢复步骤不是本材料实际引用"))?)?;
        if packet.steps.get(index).map(|(stored,_)| stored) != Some(reference) {
            return Err(io::Error::other("恢复步骤不是本材料实际引用").into());
        }
        Ok(index)
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

    fn message_position(&self, index: u64) -> RecordingResult<(u64,u64,u64)> {
        let mut words = [0_u8;24];
        let offset = index.checked_mul(24).ok_or_else(|| io::Error::other("hidden消息行偏移溢出"))?;
        self.message_positions.read_exact_at(&mut words,offset)?;
        Ok((u64::from_le_bytes(words[..8].try_into()?),u64::from_le_bytes(words[8..16].try_into()?),
            u64::from_le_bytes(words[16..].try_into()?)))
    }

    fn reference_canonical(&mut self, index_position: u64) -> RecordingResult<u64> {
        if index_position >= self.read_steps.checked_add(self.read_references)
            .ok_or_else(|| io::Error::other("hidden索引位置溢出"))? {
            return Err(io::Error::other("参考不属于已验证的hidden索引位置").into());
        }
        self.reference_locations.flush()?;
        let canonical = read_u64_column(self.reference_locations.get_ref(),index_position)?;
        if canonical==u64::MAX { return Err(io::Error::other("参考不属于已验证的hidden索引位置").into()); }
        Ok(canonical)
    }

    fn read_index(&mut self) -> RecordingResult<Option<HiddenIndexEntryV1>> {
        let Some(row) = self.index.next_row()? else {
            // 仅报告/无索引的消息也必须完整校验；不让漏报告绕过完成凭据。
            let mut flags = BufReader::new(self.message_validated.try_clone()?);
            flags.seek(SeekFrom::Start(0))?;
            for index in 0..self.manifest.summary.messages {
                let mut flag = [0_u8;1]; flags.read_exact(&mut flag)?;
                if flag[0]==0 { self.load_message(self.message_position(index)?.0)?; }
            }
            self.reference_locations.flush()?;
            if self.read_steps != self.manifest.indexed_steps || self.read_steps != self.expected_indexed_steps
                || self.read_references != self.manifest.summary.references {
                return Err(io::Error::other("hidden时间索引末端计数不闭合").into());
            }
            if self.read_reports != self.manifest.summary.reports {
                return Err(io::Error::other("hidden报告末端计数不闭合").into());
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
                self.cached_step_index(reference)?;
                self.read_steps += 1;
            }
            HiddenIndexEntryV1::Reference {canonical_position,derivation_position,source_at_utc_ms} => {
                if *canonical_position>=self.manifest.summary.canonical_facts
                    || *derivation_position>=self.manifest.summary.references {
                    return Err(io::Error::other("hidden参考位置越过自身来源范围").into());
                }
                let expected_position = derivation_position.checked_mul(2)
                    .ok_or_else(|| io::Error::other("hidden参考定位溢出"))?;
                if read_u64_column(&self.expected_references,expected_position)? != *canonical_position
                    || read_u64_column(&self.expected_references,expected_position+1)? as i64 != *source_at_utc_ms {
                    return Err(io::Error::other("hidden参考不指向原真实来源时间的非PM事实").into());
                }
                // 期望ordinal唯一绑定canonical/time；重复ordinal必重复完整时间键，
                // 已由上面的严格递增键检查拒绝。无需第二份seen人口。
                self.read_references += 1;
            }
        }
        self.reference_locations.write_all(&match &entry {
            HiddenIndexEntryV1::Reference {canonical_position,..}=>*canonical_position,
            HiddenIndexEntryV1::Step(_)=>u64::MAX,
        }.to_le_bytes())?;
        self.last_key = Some(key);
        Ok(Some(entry))
    }

    /// 看见真实下一全局时间或完整EOF才关闭桶；Parquet页末不关闭时间。
    pub fn next_bucket(&mut self) -> RecordingResult<Option<ClosedCompactHiddenBucketV1>> {
        if self.failed { return Err(io::Error::other("失败的hidden材料读取不能继续").into()); }
        self.failed = true;
        let result = self.next_bucket_inner(None);
        if result.is_ok() { self.failed = false; }
        result
    }

    /// 在严格遍历当前完整桶时交付已物化步骤；peek不覆盖已收集的结果。
    /// 仅GPU冷构建需要此输出；原只读桶入口及恢复接口保持。
    pub fn next_bucket_with_market_steps(&mut self) -> RecordingResult<Option<(ClosedCompactHiddenBucketV1,
        Vec<(MarketIdentityV2, PmReductionStepV1<'static, SourceEvidenceRefV3>)>)>> {
        if self.failed { return Err(io::Error::other("失败的hidden材料读取不能继续").into()); }
        self.failed = true;
        let mut steps = Vec::new();
        let result = self.next_bucket_inner(Some(&mut steps));
        if result.is_ok() { self.failed = false; }
        Ok(result?.map(|bucket| (bucket, steps)))
    }

    fn next_bucket_inner(&mut self, mut materialized: Option<&mut Vec<(MarketIdentityV2,
        PmReductionStepV1<'static, SourceEvidenceRefV3>)>>) -> RecordingResult<Option<ClosedCompactHiddenBucketV1>> {
        let first = match self.next.take() {Some(value)=>value, None=>match self.read_index()? {
            Some(value)=>value, None=>return Ok(None),
        }};
        let at = entry_key(&first)?.0; let start = self.position;
        let mut current = first; let mut steps = Vec::new(); let mut references = Vec::new();
        let closure = loop {
            match current {HiddenIndexEntryV1::Step(step)=>{
                if let Some(output) = materialized.as_mut() {
                    let restored = self.restore_step_inner(&step)?;
                    output.push((self.cached_message.as_ref().unwrap().message.market.clone(), restored));
                }
                steps.push(step);
            },
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
    use std::collections::BTreeSet;
    use crate::{compact_public::PublicCompactWriterV1, gpu_initial_sort::GpuInitialRunOrderV1,
        public_recording::tests::{book, change, config, core, end, meta, pm, record, row, twap_input, twap_record}};
    use data_pipeline_core::{public_input::{TypedRawRowV2, prepare_public_ingress_v1},
        typed_raw::{PolymarketOrderbookEvent, RtdsFreshnessStatus, TypedRawRow}};

    fn prepare_reader_case(directory: &Path, inputs: impl IntoIterator<Item=data_pipeline_core::public_input::RecordedIngressV1>,
        sort: &SortConfig, order: Arc<dyn InitialRunOrder>) -> (std::path::PathBuf,std::path::PathBuf) {
        let source = directory.join("public"); let hidden = directory.join("hidden");
        let mut public = core();
        let mut writer = PublicCompactWriterV1::create(&source,public.header().clone()).unwrap();
        let started=std::time::Instant::now(); let mut last=std::time::Instant::now();
        for input in inputs {
            let prepared = prepare_public_ingress_v1(&public,&input,&BTreeMap::new(),&[]).unwrap();
            let applied = public.push_facts(&prepared).unwrap();
            writer.push_prepared(&input,&prepared,&applied).unwrap();
            if last.elapsed()>=Duration::from_secs(2) {
                println!("{}",serde_json::json!({"stage":"reader_case_public_preparation",
                    "input_position":input.input_position,"elapsed_seconds":started.elapsed().as_secs_f64()}));
                last=std::time::Instant::now();
            }
        }
        writer.publish().unwrap();
        prepare_compact_hidden_material_v1(&source,&hidden,1<<20,sort,order,
            "reader-case-consumer".into(),None,|_,_,_,_| {}).unwrap();
        (source,hidden)
    }

    #[test]
    #[ignore = "AutoDL真实canonical不同序、报告消息、旧位置恢复及定位损坏"]
    fn gpu_hidden_disk_locations_keep_old_reads_and_unindexed_reports() {
        let temp = tempfile::tempdir().unwrap();
        let order = Arc::new(GpuInitialRunOrderV1::start(Path::new(&std::env::var("B_GPU_PYTHON").unwrap()),
            Path::new(&std::env::var("B_GPU_SORT_WORKER").unwrap())).unwrap());
        let mut sort = config().sort; sort.merge_workers=8;
        let (source,hidden) = prepare_reader_case(temp.path(),[meta(0),pm(1,1,20050,book("10")),
            pm(2,2,20200,PolymarketOrderbookEvent::PriceChange {price_changes:vec![change(Some("20"))]}),
            pm(3,3,20100,PolymarketOrderbookEvent::PriceChange {price_changes:vec![change(Some("30"))]}),
            pm(4,4,20400,PolymarketOrderbookEvent::LastTradePrice {asset_id:"20".into(),price:"0.5".into(),
                size:None,side:None,fee_rate_bps:None}),twap_record(5,30,25002,"65000"),end(6)],&sort,order);
        let mut reader = CompactHiddenMaterialReaderV1::open(&hidden,&source,1,1<<20,1<<20,|_,_,_,_| {}).unwrap();
        let positions:Vec<_>=(0..reader.manifest.summary.messages).map(|index|reader.message_position(index).unwrap()).collect();
        assert_eq!(positions.len(),4);
        assert!(positions.windows(2).any(|pair|pair[0].0>pair[1].0),"物理消息列不能假设canonical升序");
        let report_only:Vec<_>=positions.iter().filter(|row|row.2==0).map(|row|row.0).collect();
        assert!(!report_only.is_empty());
        let mut first_step=None; let mut first_reference=None;
        while let Some(bucket)=reader.next_bucket().unwrap() {
            for reference in &bucket.steps {reader.restore_step(reference).unwrap();}
            for position in &bucket.reference_positions {reader.reference_fact(*position).unwrap();}
            first_step=first_step.or_else(||bucket.steps.first().cloned());
            first_reference=first_reference.or_else(||bucket.reference_positions.first().copied());
        }
        assert_eq!(reader.read_reports,reader.manifest.summary.reports);
        for index in 0..reader.manifest.summary.messages {
            let mut flag=[0]; reader.message_validated.read_exact_at(&mut flag,index).unwrap();
            assert_eq!(flag,[1],"EOF也须验证未索引消息");
        }
        for canonical in report_only {assert!(!reader.message(canonical).unwrap().reports.is_empty());}
        reader.restore_step(&first_step.unwrap()).unwrap();
        let old=reader.reference_fact(first_reference.unwrap()).unwrap();
        assert_eq!(old.dependency_time.source_event_at_utc_ms,Some(25000));
        println!("disk_reader_physical_message_positions={positions:?} old_reference_after_eof=true reports={}",reader.read_reports);
        drop(reader);
        // 原源人口不变、成员hash同步；重复物理消息关联仍不能只凭总数通过。
        let path=hidden.join(MEMBERS[2]); let original=fs::read(&path).unwrap();
        let mut duplicate=original.clone(); duplicate[24..32].copy_from_slice(&original[..8]);
        fs::write(&path,duplicate).unwrap();
        let manifest_path=hidden.join("manifest.json"); let receipt=fs::read(&manifest_path).unwrap();
        let mut manifest:CompactHiddenManifestV1=serde_json::from_slice(&receipt).unwrap();
        manifest.files[2].sha256=file_sha256_with_progress(&path,Duration::ZERO,|_,_| {}).unwrap();
        serde_json::to_writer(File::create(&manifest_path).unwrap(),&manifest).unwrap();
        assert!(CompactHiddenMaterialReaderV1::open(&hidden,&source,1,1<<20,1<<20,|_,_,_,_| {}).is_err());
    }

    #[test]
    #[ignore = "由递增材料代表在独立子进程调用；只记录真实reader资源，不是正式15日容量"]
    fn hidden_disk_reader_scale_child() {
        use std::os::unix::fs::MetadataExt;
        let directory=std::path::PathBuf::from(std::env::var("B_GPU_READER_SCALE_CASE").unwrap());
        let started=std::time::Instant::now();
        let mut reader=CompactHiddenMaterialReaderV1::open(&directory.join("hidden"),&directory.join("public"),
            1024,4<<20,1<<20,|_,_,_,_| {}).unwrap();
        let open_seconds=started.elapsed().as_secs_f64();
        let mut buckets=0_u64; let mut last=std::time::Instant::now();
        while let Some(bucket)=reader.next_bucket().unwrap() {
            for reference in &bucket.steps {reader.restore_step(reference).unwrap();}
            for position in &bucket.reference_positions {reader.reference_fact(*position).unwrap();}
            buckets+=1;
            if last.elapsed()>=Duration::from_secs(2) {
                println!("{}",serde_json::json!({"stage":"reader_scale_progress","buckets":buckets,
                    "at":bucket.bucket_time_utc_ms,"indexed_rows":reader.read_steps+reader.read_references,
                    "total_indexed_rows":reader.manifest.indexed_steps+reader.manifest.summary.references,
                    "elapsed_seconds":started.elapsed().as_secs_f64()})); last=std::time::Instant::now();
            }
        }
        reader.reference_locations.flush().unwrap();
        let files=[&reader.message_rows,&reader.message_validated,&reader.expected_references,reader.reference_locations.get_ref()];
        let logical:u64=files.iter().map(|file|file.metadata().unwrap().len()).sum();
        let allocated:u64=files.iter().map(|file|file.metadata().unwrap().blocks()*512).sum();
        let memory=fs::read_to_string("/proc/self/status").unwrap().lines()
            .filter(|line|line.starts_with("VmRSS:")||line.starts_with("VmHWM:")).map(str::to_owned).collect::<Vec<_>>();
        println!("{}",serde_json::json!({"stage":"reader_scale_complete","messages":reader.manifest.summary.messages,
            "facts":reader.manifest.summary.canonical_facts,"buckets":buckets,"batch_rows":1024,"batch_bytes":4<<20,
            "scratch_logical_bytes":logical,"scratch_allocated_bytes":allocated,"memory":memory,
            "open_seconds":open_seconds,"total_seconds":started.elapsed().as_secs_f64()}));
    }

    #[test]
    #[ignore = "AutoDL原typed输入固定批量递增512/4096/16384消息，独立reader进程量资源"]
    fn gpu_hidden_disk_reader_scales_without_resident_population_trees() {
        let directory=std::path::PathBuf::from(std::env::var("B_GPU_READER_SCALE_ARTIFACT_DIR").unwrap());
        fs::create_dir(&directory).unwrap();
        let order=Arc::new(GpuInitialRunOrderV1::start(Path::new(&std::env::var("B_GPU_PYTHON").unwrap()),
            Path::new(&std::env::var("B_GPU_SORT_WORKER").unwrap())).unwrap());
        let sort=SortConfig {max_run_rows:1024,max_run_bytes:16<<20,batch_rows:1024,merge_workers:8,
            merge_fan_in:8,max_open_files:96,max_temp_run_files:128,max_temp_bytes:1<<30};
        for count in [512_u64,4096,16384] {
            let case=directory.join(count.to_string());
            let started=std::time::Instant::now();
            let inputs=std::iter::once(meta(0)).chain((1..=count).map(|position| {
                let event=if position==1 {book("10")} else if position%16==0 {
                    PolymarketOrderbookEvent::LastTradePrice {asset_id:"20".into(),price:"0.5".into(),
                        size:None,side:None,fee_rate_bps:None}
                } else {PolymarketOrderbookEvent::PriceChange {price_changes:vec![change(Some(if position%2==0 {"20"} else {"21"}))]}};
                record(position,TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(
                    row("epoch-a",position,20050+position as i64,20150+position as i64,event))))
            })).chain(std::iter::once(end(count+1)));
            prepare_reader_case(&case,inputs,&sort,order.clone());
            println!("{}",serde_json::json!({"stage":"reader_scale_material_prepared","messages":count,
                "elapsed_seconds":started.elapsed().as_secs_f64(),"sort":sort}));
            let output=std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact","compact_hidden_material::tests::hidden_disk_reader_scale_child","--ignored","--nocapture","--test-threads=1"])
                .env("B_GPU_READER_SCALE_CASE",&case).output().unwrap();
            fs::write(case.join("reader.stdout"),&output.stdout).unwrap();
            fs::write(case.join("reader.stderr"),&output.stderr).unwrap();
            fs::write(case.join("reader.exit"),format!("{}\n",output.status)).unwrap();
            assert!(output.status.success(),"reader scale child {}: {}",output.status,String::from_utf8_lossy(&output.stderr));
            print!("{}",String::from_utf8_lossy(&output.stdout));
        }
    }

    fn read_all(root: &Path, source: &Path, rows: usize) -> Vec<ClosedCompactHiddenBucketV1> {
        let mut reader = CompactHiddenMaterialReaderV1::open(root, source, rows, 1<<20, 1<<20, |_,_,_,_| {}).unwrap();
        let expected_receipt = file_sha256_with_progress(&root.join("manifest.json"), Duration::ZERO, |_,_| {}).unwrap();
        assert_eq!(reader.material_manifest_sha256, expected_receipt);
        let mut result = Vec::new();
        let mut snapshots = Vec::new();
        let mut transitions = Vec::new();
        while let Some(bucket) = reader.next_bucket().unwrap() {
            assert_eq!(bucket.material_manifest_sha256, expected_receipt);
            for position in &bucket.reference_positions {
                let fact = reader.reference_fact(*position).unwrap();
                assert_eq!(fact.dependency_time.source_event_at_utc_ms,Some(bucket.bucket_time_utc_ms));
                assert!(!matches!(fact.payload,crate::compact_public::CompactFactPayloadV1::PmFromInput(_)));
            }
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

    fn split_index_for_test(root: &Path, config: &crate::gate_a::GateAOutputConfig) -> CompactHiddenManifestV1 {
        let mut manifest: CompactHiddenManifestV1 = serde_json::from_reader(File::open(root.join("manifest.json")).unwrap()).unwrap();
        let run = SortedRun {parts: manifest.files[3..].iter().map(|file| root.join(&file.name)).collect()};
        let work = root.join("split-test");
        let mut cursor = PartCursor::open(&run, 1, 1<<20).unwrap();
        let mut writer = crate::gate_a::SequentialCaptureWriter::create_segment("split-index", &work,
            std::sync::Arc::new(std::sync::Mutex::new(0)), config).unwrap();
        let mut sequence = 0;
        while let Some(row) = cursor.next_row().unwrap() {
            writer.push(sequence, row.sort_key_bytes().to_vec(), row.payload()).unwrap();
            if sequence == 0 { writer.split_part_for_test().unwrap(); }
            sequence += 1;
        }
        let locator = writer.close_segment().unwrap();
        let split = SortedRun::read_parts(&locator).unwrap();
        assert_eq!(split.parts.len(), 2);
        drop(cursor);
        for file in &manifest.files[3..] { fs::remove_file(root.join(&file.name)).unwrap(); }
        manifest.files.truncate(3);
        manifest.files.extend(publish_index_parts(split, root, Duration::ZERO, &mut |_,_,_,_| {}).unwrap());
        fs::remove_dir_all(work).unwrap();
        serde_json::to_writer(File::create(root.join("manifest.json")).unwrap(), &manifest).unwrap();
        manifest
    }

    fn logical_buckets(mut buckets: Vec<ClosedCompactHiddenBucketV1>) -> Vec<ClosedCompactHiddenBucketV1> {
        // 内容可比较，凭据必须各自绑定，不能改绑缓存或要求两版相同摘要。
        for bucket in &mut buckets { bucket.material_manifest_sha256.clear(); }
        buckets
    }

    #[test]
    #[ignore = "AutoDL同桶多消息及下一桶预读，直接交物化步骤不重复读取来源"]
    fn gpu_bucket_source_materializes_same_time_messages_before_peek_once() {
        let temp = tempfile::tempdir().unwrap();
        let source_root = temp.path().join("public");
        let target = temp.path().join("hidden");
        let mut public = core();
        let mut writer = PublicCompactWriterV1::create(&source_root,public.header().clone()).unwrap();
        for input in [meta(0),pm(1,1,20050,book("10")),
            pm(2,2,20050,PolymarketOrderbookEvent::PriceChange {price_changes:vec![change(Some("20"))]}),
            pm(3,3,20200,PolymarketOrderbookEvent::PriceChange {price_changes:vec![change(Some("30"))]}),end(4)] {
            let prepared = prepare_public_ingress_v1(&public,&input,&BTreeMap::new(),&[]).unwrap();
            let applied = public.push_facts(&prepared).unwrap();
            writer.push_prepared(&input,&prepared,&applied).unwrap();
        }
        writer.publish().unwrap();
        let order = Arc::new(GpuInitialRunOrderV1::start(Path::new(&std::env::var("B_GPU_PYTHON").unwrap()),
            Path::new(&std::env::var("B_GPU_SORT_WORKER").unwrap())).unwrap());
        let mut sort = config().sort; sort.merge_workers = 8;
        prepare_compact_hidden_material_v1(&source_root,&target,1<<20,&sort,order,
            "same-time-gpu-consumer".into(),None,|_,_,_,_| {}).unwrap();
        split_index_for_test(&target, &config());
        let mut reader = CompactHiddenMaterialReaderV1::open(&target,&source_root,1,1<<20,1<<20,|_,_,_,_| {}).unwrap();
        let first = crate::compact_hidden_gpu::next_compact_hidden_gpu_bucket_v1(&mut reader).unwrap().unwrap();
        assert_eq!(first.bucket.bucket_time_utc_ms,20050);
        assert_eq!(first.bucket.closure,ReplayIndexClosureV1::NextGlobalEventAt(20200));
        assert_eq!(first.bucket.steps.iter().map(|step|step.canonical_record_position).collect::<BTreeSet<_>>().len(),2);
        assert_eq!(reader.message_loads,3,"两条当前消息和下一桶预读消息各读取一次");
        assert!(first.steps.iter().any(|(_,step)| matches!(&step.change,
            PmStepChangeV1::InitialSnapshot {bids,..} if bids[0].quantity_shares.0=="10")));
        let changes = |bucket: &crate::compact_hidden_gpu::CompactHiddenGpuBucketV1| bucket.steps.iter().filter_map(|(_,step)| {
            match &step.change {
                PmStepChangeV1::LevelChange {transition,..} if transition.side==BookSide::Bid =>
                    Some((transition.quantity_before_shares.0.clone(),transition.quantity_after_shares.0.clone())),
                _=>None,
            }
        }).collect::<Vec<_>>();
        assert_eq!(changes(&first),vec![("10".into(),"20".into())]);
        let second = crate::compact_hidden_gpu::next_compact_hidden_gpu_bucket_v1(&mut reader).unwrap().unwrap();
        assert_eq!(second.bucket.bucket_time_utc_ms,20200);
        assert_eq!(changes(&second),vec![("20".into(),"30".into())]);
        assert!(crate::compact_hidden_gpu::next_compact_hidden_gpu_bucket_v1(&mut reader).unwrap().is_none());
        assert_eq!(reader.message_loads,3,"消费两桶不得重读被peek覆盖的消息");
        println!("same_time_messages=2 peek_messages=1 actual_message_loads={}",reader.message_loads);
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
        let mut sort = config().sort; sort.merge_workers = 8;sort.max_run_rows = 1;
        let mut message_merge_seen = false;let mut index_merge_seen = false;let mut finalizing = false;
        let mut progress = |notice| {
            use crate::historical_preparation_progress::{HistoricalPreparationNoticeV1 as Notice,HistoricalPreparationStageV1 as Stage};
            match notice {
                Notice::Stage(Stage::HiddenFinalize)=>finalizing=true,
                Notice::SourceMergeProgress {..} if finalizing=>index_merge_seen=true,
                Notice::SourceMergeProgress {..}=>message_merge_seen=true,
                _=>{},
            }
        };
        let notifier = HistoricalPreparationNotifierV1::new(&mut progress);
        let manifest = prepare_compact_hidden_material_v1(&source_root,&target,1<<20,&sort,order,
            "original-hidden-consumer".into(),Some(&notifier),|_,_,_,_| {}).unwrap();
        assert!(message_merge_seen && index_merge_seen);
        assert_eq!(manifest.summary.messages,3);
        assert_eq!(manifest.summary.steps,15);
        assert_eq!(manifest.summary.reports,6);
        assert_eq!(manifest.version, VERSION);
        assert_eq!(manifest.files.len(), 4);
        assert_eq!(fs::read_dir(&target).unwrap().count(),manifest.files.len()+1);
        let single = read_all(&target,&source_root,1);
        let single_sha = single[0].material_manifest_sha256.clone();
        let legacy = temp.path().join("legacy-v1"); fs::create_dir(&legacy).unwrap();
        for file in &manifest.files { fs::copy(target.join(&file.name), legacy.join(&file.name)).unwrap(); }
        let mut old = serde_json::to_value(&manifest).unwrap(); old["version"] = serde_json::json!(VERSION_V1);
        serde_json::to_writer(File::create(legacy.join("manifest.json")).unwrap(), &old).unwrap();
        let old_buckets = read_all(&legacy,&source_root,1);
        assert_ne!(old_buckets[0].material_manifest_sha256, single_sha);
        assert_eq!(logical_buckets(old_buckets), logical_buckets(single.clone()));
        let manifest = split_index_for_test(&target, &config());
        assert_eq!(manifest.files.len(), 5);
        assert_eq!(fs::read_dir(&target).unwrap().count(),manifest.files.len()+1);
        let multiple = read_all(&target,&source_root,1);
        assert_ne!(multiple[0].material_manifest_sha256, single_sha);
        assert_eq!(logical_buckets(single), logical_buckets(multiple));
        let subsequent = target.join(&manifest.files[4].name);
        let missing = temp.path().join("missing-piece.parquet");fs::rename(&subsequent, &missing).unwrap();
        assert!(CompactHiddenMaterialReaderV1::open(&target,&source_root,1,1<<20,1<<20,|_,_,_,_| {}).is_err());
        fs::rename(missing, subsequent).unwrap();
        // 原输入才持久保存盘口；hidden快照步骤只有输入引用，没有再编码整侧。
        let mut reader = CompactHiddenMaterialReaderV1::open(&target,&source_root,1,1<<20,1<<20,|_,_,_,_| {}).unwrap();
        let positions:Vec<_> = (0..reader.manifest.summary.messages)
            .map(|index| reader.message_position(index).unwrap().0).collect();
        let mut snapshot_refs = 0;
        for position in positions {
            let packet = reader.message(position).unwrap();
            for (_,step) in &packet.steps {
                if matches!(step.change,CompactPmChangeV1::InitialSnapshotFromInput {..}) { snapshot_refs += 1; }
            }
        }
        assert_eq!(snapshot_refs,1);
        assert_eq!(read_all(&target,&source_root,1),read_all(&target,&source_root,64));
        if let Ok(binary) = std::env::var("B_GPU_HIDDEN_CLI") {
            let output = std::process::Command::new(binary).args(["--progress-interval-seconds","0","--inspect"])
                .arg(&target).arg(&source_root).args(["1","1048576","1048576"]).output().unwrap();
            assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
            assert!(output.stderr.is_empty());
            let report:serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(report["steps"],15);assert_eq!(report["references"],1);
        }
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
        let mut original = PartCursor::open(&SortedRun {parts:manifest.files[3..].iter().map(|file|target.join(&file.name)).collect()},1,1<<20).unwrap();
        let replacement_root = temp.path().join("tamper-index");
        fs::create_dir(&replacement_root).unwrap();
        let mut replacement = ExternalSorter::new(&replacement_root,&sort).unwrap();
        let mut removed = false;
        while let Some(row) = original.next_row().unwrap() {
            let entry:HiddenIndexEntryV1 = serde_json::from_slice(row.payload()).unwrap();
            if !removed && matches!(entry,HiddenIndexEntryV1::Step(_)) {removed = true;continue;}
            replacement.push(row).unwrap();
        }
        assert!(removed);
        let (index,_) = replacement.finish(None).unwrap();
        drop(original);
        for file in &manifest.files[3..] {fs::remove_file(target.join(&file.name)).unwrap();}
        let replacement_files = publish_index_parts(index, &target, Duration::ZERO, &mut |_,_,_,_| {}).unwrap();
        let mut tampered:serde_json::Value = serde_json::to_value(&manifest).unwrap();
        tampered["indexed_steps"] = serde_json::json!(manifest.indexed_steps-1);
        tampered["files"] = serde_json::to_value(manifest.files[..3].iter().chain(&replacement_files).collect::<Vec<_>>()).unwrap();
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
