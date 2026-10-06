//! 公共完整事实的紧凑冷材料。输入/组/PM步骤各保存一次，不写模型观察或逐事件全盘口。
//! 当前wire侧车复用原强类型JSON编码；固定列供批量GPU转换，hidden由独立排序归约产生。
use std::{
    fs::{self, File},
    io::{self, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
use contracts::*;
use data_pipeline_core::{
    public_input::{DptIngressV1, RecordedIngressV1},
    pm_reduction::{PmEvidencePairV1, PmReductionReportV2, PmReductionStepV1, PmStepChangeV1},
    public_observation_core::{PublicFactAppliedIngressV1, PublicFactGroupV1, PublicObservationCoreV1},
    public_reduction::PublicReductionReportV1,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use crate::public_recording::RecordingResult;
use crate::public_replay::HistoricalMergeV1;

const SCHEMA: &str = "public_compact_facts_v2";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactFileV1 {
    pub name: String,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicCompactManifestV1 {
    pub schema_version: String,
    pub header: PublicInputStreamHeaderV1,
    pub applied_prefix: AppliedIngressPrefixV1,
    pub inputs: u64,
    pub groups: u64,
    pub facts: u64,
    pub pm_packets: u64,
    pub pm_steps: u64,
    pub reports: u64,
    pub normal_end: bool,
    pub files: Vec<CompactFileV1>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactPmMessageHeaderV1 {
    pub schema_version: String,
    pub event_id: CanonicalEventId,
    pub market: MarketIdentityV2,
    pub group_id: Sha256Hex,
    pub lineage: EventLineageV3,
    pub time: EventTimeV2,
    pub input_record: CanonicalInputRecordRefV1,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactFactPayloadV1 {
    /// 沿input_record读取同一原消息，以原公共规范化配方重建payload，不复制盘口。
    PmFromInput(CompactPmMessageHeaderV1),
    /// 消费完整合法前缀的源快照/增量/代际/接缝裁决重建，不按收到时间盲目覆盖。
    BinanceDepthFromPrefix { symbol: String, last_update_id: u64, through_input_position: u64 },
    Inline(PublicBusinessPayloadV1),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactFactV1 {
    pub position: PublicFactPositionV1,
    pub payload: CompactFactPayloadV1,
    pub source_refs: Vec<CanonicalInputRecordRefV1>,
    pub lineage: EventLineageV3,
    pub direct_time: EventTimeV2,
    pub dependency_time: EventTimeV2,
    pub quality: EventQualityV2,
    pub direct_sources: Vec<SourceEvidenceRefV3>,
}
impl From<&PublicBusinessFactV1> for CompactFactV1 {
    fn from(f: &PublicBusinessFactV1) -> Self {
        let payload = match &f.payload {
            PublicBusinessPayloadV1::PolymarketMessage(m) => CompactFactPayloadV1::PmFromInput(CompactPmMessageHeaderV1 {
                schema_version: m.schema_version.clone(), event_id: m.event_id.clone(), market: m.market.clone(),
                group_id: m.group_id.clone(), lineage: m.lineage.clone(), time: m.time.clone(), input_record: m.input_record.clone(),
            }),
            PublicBusinessPayloadV1::ReferenceFact(CanonicalReferenceEventPayloadV2::BinanceDepthState(d)) =>
                CompactFactPayloadV1::BinanceDepthFromPrefix { symbol: d.symbol.clone(), last_update_id: d.last_update_id,
                    through_input_position: f.position.closed_at_input_position },
            p => CompactFactPayloadV1::Inline(p.clone()),
        };
        Self { position: f.position.clone(), payload, source_refs: f.source_refs.clone(), lineage: f.lineage.clone(),
            direct_time: f.direct_time.clone(), dependency_time: f.dependency_time.clone(), quality: f.quality.clone(),
            direct_sources: f.direct_sources.clone() }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactGroupV1 {
    pub group_id: Sha256Hex,
    pub group_position: u64,
    pub member_input_positions: Vec<u64>,
    pub closed_at_input_position: u64,
    pub visible_at_utc_ms: i64,
    pub records: Vec<CompactFactV1>,
    pub source_progress_changes: Vec<SourceStreamProgressV1>,
}
impl From<&PublicFactGroupV1> for CompactGroupV1 {
    fn from(g: &PublicFactGroupV1) -> Self {
        Self { group_id: g.group_id.clone(), group_position: g.group_position,
            member_input_positions: g.member_input_positions.clone(), closed_at_input_position: g.closed_at_input_position,
            visible_at_utc_ms: g.visible_at_utc_ms, records: g.records.iter().map(CompactFactV1::from).collect(),
            source_progress_changes: g.source_progress_changes.clone() }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactPmChangeV1 {
    Inline(PmStepChangeV1<'static, SourceEvidenceRefV3>),
    InitialSnapshotFromInput {
        input_record: CanonicalInputRecordRefV1, asset: AssetIdentityV1,
        initial_tick_size_pusd_per_share: ExactDecimal, source_sequence_epoch_id: String,
        source_cursor: u64, book_digest_sha256: Sha256Hex, evidence: PmEvidencePairV1<SourceEvidenceRefV3>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactPmStepV1 {
    pub canonical_fact_id: CanonicalEventId,
    pub member_index: Option<u32>, pub step_index: u32, pub derived_id: PmDerivedStepPositionV1,
    pub change: CompactPmChangeV1, pub direct: SourceEvidenceRefV3,
    pub prestate: Option<PmEvidencePairV1<SourceEvidenceRefV3>>,
    pub absence: Option<PmEvidencePairV1<SourceEvidenceRefV3>>, pub dependencies: Vec<SourceEvidenceRefV3>,
    pub direct_time: EventTimeV2, pub dependency_time: EventTimeV2, pub quality: EventQualityV2,
}
/// Prepared证据的wire形状就是实际SourceEvidenceRefV3，不保存其编码缓存。
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactPmPacketV1 {
    pub position: PublicFactPositionV1,
    pub steps: Vec<CompactPmStepV1>,
    pub reports: Vec<PmReductionReportV2<SourceEvidenceRefV3, PmDerivedStepPositionV1>>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicCompactPacketV1 {
    pub input: RecordedIngressV1,
    pub groups: Vec<CompactGroupV1>,
    pub pm: Vec<CompactPmPacketV1>,
    pub reports: Vec<PublicReductionReportV1>,
}
impl PublicCompactPacketV1 {
    fn build(input: &RecordedIngressV1, applied: &PublicFactAppliedIngressV1<'_>) -> RecordingResult<Self> {
        let groups: Vec<_> = applied.groups.iter().map(CompactGroupV1::from).collect();
        let mut pm = Vec::new();
        for (position, reduction) in &applied.pm_reductions {
            let mut steps = Vec::new();
            for step in &reduction.steps {
                let s: PmReductionStepV1<'static, SourceEvidenceRefV3, PmDerivedStepPositionV1> =
                    serde_json::from_value(serde_json::to_value(step)?)?;
                let change = match s.change {
                    PmStepChangeV1::InitialSnapshot { asset, initial_tick_size_pusd_per_share,
                        source_sequence_epoch_id, source_cursor, book_digest_sha256, evidence, .. } => {
                        let input_record = groups.iter().flat_map(|g| &g.records).find_map(|f| match &f.payload {
                            CompactFactPayloadV1::PmFromInput(m) if m.event_id == s.canonical_fact_id => Some(m.input_record.clone()),
                            _ => None,
                        }).ok_or_else(|| io::Error::other("快照步骤缺少原输入关联"))?;
                        CompactPmChangeV1::InitialSnapshotFromInput { input_record, asset, initial_tick_size_pusd_per_share,
                            source_sequence_epoch_id, source_cursor, book_digest_sha256, evidence }
                    }
                    change => CompactPmChangeV1::Inline(change),
                };
                steps.push(CompactPmStepV1 { canonical_fact_id: s.canonical_fact_id, member_index: s.member_index,
                    step_index: s.step_index, derived_id: s.derived_id, change, direct: s.direct, prestate: s.prestate,
                    absence: s.absence, dependencies: s.dependencies, direct_time: s.direct_time,
                    dependency_time: s.dependency_time, quality: s.quality });
            }
            pm.push(CompactPmPacketV1 { position: position.clone(), steps,
                reports: serde_json::from_value(serde_json::to_value(&reduction.reports)?)? });
        }
        Ok(Self { input: input.clone(), groups, pm,
            reports: applied.reductions.iter().flat_map(|r| r.reports.iter().cloned()).collect() })
    }
}

pub struct PublicCompactWriterV1 {
    target: PathBuf,
    pending: PathBuf,
    payload: BufWriter<File>,
    offsets: BufWriter<File>,
    columns: BufWriter<File>,
    sources: BufWriter<File>,
    payload_bytes: u64,
    manifest: PublicCompactManifestV1,
    failed: bool,
}
fn new_file(root: &Path, name: &str) -> io::Result<BufWriter<File>> {
    Ok(BufWriter::new(File::create_new(root.join(name))?))
}
fn file_sha256(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 { break; }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}
impl PublicCompactWriterV1 {
    pub fn create(target: &Path, header: PublicInputStreamHeaderV1) -> RecordingResult<Self> {
        let parent = target.parent().ok_or_else(|| io::Error::other("材料必须有父目录"))?;
        fs::create_dir_all(parent)?;
        if target.exists() { return Err(io::Error::other("材料目标已存在，保留现场").into()); }
        let name = target.file_name().ok_or_else(|| io::Error::other("缺少材料名"))?.to_string_lossy();
        let pending = parent.join(format!(".{name}.pending"));
        fs::create_dir(&pending)?;
        let mut offsets = new_file(&pending, "packet_offsets.u64le")?;
        offsets.write_all(&0_u64.to_le_bytes())?;
        Ok(Self {
            target: target.into(), payload: new_file(&pending, "packet_payload.bin")?,
            offsets, columns: new_file(&pending, "group_columns.i64le")?,
            sources: new_file(&pending, "source_bindings.jsonl")?, pending,
            payload_bytes: 0,
            manifest: PublicCompactManifestV1 {
                schema_version: SCHEMA.into(),
                applied_prefix: AppliedIngressPrefixV1 {
                    input_stream_id: header.input_stream_id.clone(), next_input_position: 0,
            }, header, inputs: 0, groups: 0, facts: 0, pm_packets: 0, pm_steps: 0,
            reports: 0, normal_end: false, files: vec![],
            }, failed: false,
        })
    }
    pub fn source_binding(&mut self, source: &SourceArtifactBindingV1) -> RecordingResult<()> {
        // 此材料属于输入身份交接，不能把它充当新数据验收。
        if self.failed { return Err(io::Error::other("材料写入已失败").into()); }
        self.failed = true;
        serde_json::to_writer(&mut self.sources, source)?;
        self.sources.write_all(b"\n")?;
        self.failed = false;
        Ok(())
    }
    pub fn push(&mut self, input: &RecordedIngressV1,
                applied: &PublicFactAppliedIngressV1<'_>) -> RecordingResult<()> {
        if self.failed || self.manifest.normal_end { return Err(io::Error::other("失败或结束材料不能续写").into()); }
        self.failed = true;
        if input.input_stream_id != self.manifest.header.input_stream_id
            || input.input_position != self.manifest.inputs
            || applied.applied_prefix.input_stream_id != input.input_stream_id
            || applied.applied_prefix.next_input_position != input.input_position + 1 {
            return Err(io::Error::other("材料输入与应用前缀不连续").into());
        }
        let packet = PublicCompactPacketV1::build(input, applied)?;
        self.manifest.pm_packets += packet.pm.len() as u64;
        self.manifest.pm_steps += packet.pm.iter().map(|p| p.steps.len() as u64).sum::<u64>();
        self.manifest.reports += packet.reports.len() as u64 + packet.pm.iter().map(|p| p.reports.len() as u64).sum::<u64>();
        let bytes = serde_json::to_vec(&packet)?;
        self.payload.write_all(&bytes)?;
        self.payload_bytes = self.payload_bytes.checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("材料字节位置溢出"))?;
        self.offsets.write_all(&self.payload_bytes.to_le_bytes())?;
        for group in &applied.groups {
            if group.group_position != self.manifest.groups || group.closed_at_input_position != input.input_position {
                return Err(io::Error::other("材料组与关闭位置不连续").into());
            }
            // 每行5列：组序号/关闭输入/合法可见时刻/事实起点/事实末端。
            let end = self.manifest.facts.checked_add(group.records.len() as u64)
                .ok_or_else(|| io::Error::other("事实位置溢出"))?;
            for value in [group.group_position, group.closed_at_input_position,
                          group.visible_at_utc_ms as u64, self.manifest.facts, end] {
                self.columns.write_all(&value.to_le_bytes())?;
            }
            self.manifest.groups += 1;
            self.manifest.facts = end;
        }
        self.manifest.inputs += 1;
        self.manifest.applied_prefix = applied.applied_prefix.clone();
        self.manifest.normal_end = matches!(input.ingress, DptIngressV1::EndOfInput);
        self.failed = false;
        Ok(())
    }
    pub fn publish(mut self) -> RecordingResult<PublicCompactManifestV1> {
        if self.failed || !self.manifest.normal_end { return Err(io::Error::other("仅完整正常结束材料可发布").into()); }
        for file in [&mut self.payload, &mut self.offsets, &mut self.columns, &mut self.sources] {
            file.flush()?;
            file.get_ref().sync_all()?;
        }
        for name in ["packet_payload.bin", "packet_offsets.u64le", "group_columns.i64le", "source_bindings.jsonl"] {
            let path = self.pending.join(name);
            self.manifest.files.push(CompactFileV1 {
                name: name.into(), bytes: fs::metadata(&path)?.len(), sha256: file_sha256(&path)?,
            });
        }
        let mut manifest = File::create_new(self.pending.join("manifest.json"))?;
        serde_json::to_writer(&mut manifest, &self.manifest)?;
        manifest.sync_all()?;
        File::open(&self.pending)?.sync_all()?;
        if self.target.exists() { return Err(io::Error::other("材料目标已存在，保留pending").into()); }
        rustix::fs::renameat_with(rustix::fs::CWD, &self.pending,
            rustix::fs::CWD, &self.target, rustix::fs::RenameFlags::NOREPLACE)?;
        File::open(self.target.parent().unwrap())?.sync_all()?;
        Ok(self.manifest)
    }
}

/// CPU外排适配入口，显式借用调用者已选择的merge；不能称为GPU排序准备。
/// 新factsink直接交可失败writer，不经过旧public recording及其稳定前缀。
pub fn consume_historical_merge_facts_v1(
    merge: &mut HistoricalMergeV1, header: PublicInputStreamHeaderV1,
    writer: &mut PublicCompactWriterV1,
    mut progress: impl FnMut(u64) -> RecordingResult<()>,
) -> RecordingResult<PublicObservationCoreV1> {
    let mut core = PublicObservationCoreV1::new(header.clone())?;
    while let Some(mut input) = merge.next_input()? {
        if let Some(binding) = merge.initialization_binding() { writer.source_binding(binding)?; }
        let prepared = merge.prepare_input(&core, &mut input)?;
        let applied = core.push_facts(&prepared)?;
        let recorded = RecordedIngressV1 {
            input_stream_id: prepared.recorded.input_stream_id.clone(),
            input_position: prepared.recorded.input_position,
            ingress: prepared.recorded.ingress.clone(),
            dependency_record_refs: prepared.recorded.dependency_record_refs.clone(),
        };
        writer.push(&recorded, &applied)?;
        progress(input.input_position + 1)?;
    }
    let mut end = RecordedIngressV1 {
        input_stream_id: header.input_stream_id,
        input_position: core.applied_prefix().next_input_position,
        ingress: DptIngressV1::EndOfInput, dependency_record_refs: vec![],
    };
    let prepared = merge.prepare_input(&core, &mut end)?;
    let applied = core.push_facts(&prepared)?;
    let recorded = RecordedIngressV1 {
        input_stream_id: prepared.recorded.input_stream_id.clone(),
        input_position: prepared.recorded.input_position,
        ingress: prepared.recorded.ingress.clone(),
        dependency_record_refs: prepared.recorded.dependency_record_refs.clone(),
    };
    writer.push(&recorded, &applied)?;
    progress(end.input_position + 1)?;
    Ok(core)
}

pub struct PublicCompactReaderV1 {
    pub manifest: PublicCompactManifestV1,
    payload: File,
    offsets: File,
    columns: File,
    next: u64,
    next_group: u64,
    next_fact: u64,
    next_offset: u64,
    next_pm: u64,
    next_step: u64,
    next_report: u64,
    max_packet_bytes: usize,
}
impl PublicCompactReaderV1 {
    /// SHA核对完整文件；上限只限制本次读取分配，不改变业务或签发数据验收。
    pub fn open(root: &Path, max_packet_bytes: usize) -> RecordingResult<Self> {
        let manifest: PublicCompactManifestV1 = serde_json::from_reader(File::open(root.join("manifest.json"))?)?;
        if manifest.schema_version != SCHEMA || !manifest.normal_end || manifest.inputs == 0
            || manifest.applied_prefix.next_input_position != manifest.inputs
            || manifest.applied_prefix.input_stream_id != manifest.header.input_stream_id {
            return Err(io::Error::other("紧凑材料完成凭据不合法").into());
        }
        let expected = ["packet_payload.bin", "packet_offsets.u64le", "group_columns.i64le", "source_bindings.jsonl"];
        if manifest.files.len() != expected.len() { return Err(io::Error::other("材料文件集合不完整").into()); }
        for (item, name) in manifest.files.iter().zip(expected) {
            if item.name != name { return Err(io::Error::other("材料文件名或顺序错误").into()); }
            let file = File::open(root.join(name))?;
            if file.metadata()?.len() != item.bytes { return Err(io::Error::other("材料大小错误").into()); }
            if file_sha256(&root.join(name))? != item.sha256 { return Err(io::Error::other("材料SHA错误").into()); }
        }
        if manifest.files[1].bytes != manifest.inputs.checked_add(1).and_then(|n| n.checked_mul(8)).unwrap_or(0)
            || manifest.files[2].bytes != manifest.groups.checked_mul(40).unwrap_or(0) {
            return Err(io::Error::other("材料固定列长度错误").into());
        }
        Ok(Self { payload: File::open(root.join(expected[0]))?, offsets: File::open(root.join(expected[1]))?,
                  columns: File::open(root.join(expected[2]))?,
                  manifest, next: 0, next_group: 0, next_fact: 0, next_offset: 0,
                  next_pm: 0, next_step: 0, next_report: 0, max_packet_bytes })
    }
    pub fn next_packet(&mut self) -> RecordingResult<Option<PublicCompactPacketV1>> {
        if self.next == self.manifest.inputs { return Ok(None); }
        let mut value = [0_u8; 8];
        self.offsets.seek(SeekFrom::Start(self.next * 8))?;
        self.offsets.read_exact(&mut value)?;
        let start = u64::from_le_bytes(value);
        self.offsets.read_exact(&mut value)?;
        let end = u64::from_le_bytes(value);
        if start != self.next_offset || start >= end || end > self.manifest.files[0].bytes || end - start > self.max_packet_bytes as u64 {
            return Err(io::Error::other("材料payload偏移或本次资源容量错误").into());
        }
        let mut bytes = vec![0; (end - start) as usize];
        self.payload.seek(SeekFrom::Start(start))?;
        self.payload.read_exact(&mut bytes)?;
        let packet: PublicCompactPacketV1 = serde_json::from_slice(&bytes)?;
        if packet.input.input_stream_id != self.manifest.header.input_stream_id || packet.input.input_position != self.next {
            return Err(io::Error::other("材料packet输入位置错误").into());
        }
        let final_packet = self.next + 1 == self.manifest.inputs;
        if final_packet != matches!(packet.input.ingress, DptIngressV1::EndOfInput)
            || matches!(packet.input.ingress, DptIngressV1::Aborted { .. }) {
            return Err(io::Error::other("材料末端没有正常结束输入").into());
        }
        let mut associations = std::collections::BTreeMap::new();
        let mut ordinal = 0_u32;
        for group in &packet.groups {
            if group.group_position != self.next_group || group.closed_at_input_position != self.next
                || group.member_input_positions.iter().any(|p| *p > self.next) {
                return Err(io::Error::other("材料组位置或关闭前缀错误").into());
            }
            let fact_end = self.next_fact.checked_add(group.records.len() as u64)
                .ok_or_else(|| io::Error::other("材料事实数量溢出"))?;
            for expected in [group.group_position, group.closed_at_input_position,
                             group.visible_at_utc_ms as u64, self.next_fact, fact_end] {
                self.columns.read_exact(&mut value)?;
                if u64::from_le_bytes(value) != expected { return Err(io::Error::other("材料固定列/payload不一致").into()); }
            }
            for (index, fact) in group.records.iter().enumerate() {
                if fact.position.input_stream_id != self.manifest.header.input_stream_id
                    || fact.position.closed_at_input_position != self.next || fact.position.fact_ordinal != ordinal
                    || fact.source_refs.iter().any(|r| r.input_stream_id != self.manifest.header.input_stream_id || r.input_position > self.next) {
                    return Err(io::Error::other("材料事实位置或输入关联错误").into());
                }
                ordinal = ordinal.checked_add(1).ok_or_else(|| io::Error::other("事实序位溢出"))?;
                if let CompactFactPayloadV1::PmFromInput(message) = &fact.payload {
                    if message.input_record.input_stream_id != self.manifest.header.input_stream_id
                        || message.input_record.input_position > self.next {
                        return Err(io::Error::other("PM原消息输入引用越过合法前缀").into());
                    }
                    associations.insert(fact.position.clone(), (message, self.next_fact + index as u64));
                }
                if let CompactFactPayloadV1::BinanceDepthFromPrefix { through_input_position, .. } = &fact.payload {
                    if *through_input_position != self.next { return Err(io::Error::other("深度状态前缀不一致").into()); }
                }
                if matches!(&fact.payload, CompactFactPayloadV1::Inline(PublicBusinessPayloadV1::PolymarketMessage(_)
                    | PublicBusinessPayloadV1::ReferenceFact(CanonicalReferenceEventPayloadV2::BinanceDepthState(_)))) {
                    return Err(io::Error::other("紧凑事实不得内联重复盘口载荷").into());
                }
            }
            self.next_group += 1;
            self.next_fact = fact_end;
        }
        let mut seen_pm = std::collections::BTreeSet::new();
        for pm in &packet.pm {
            if !seen_pm.insert(&pm.position) { return Err(io::Error::other("PM材料重复事实关联").into()); }
            let (message, canonical_position) = associations.get(&pm.position)
                .ok_or_else(|| io::Error::other("PM步骤没有对应事实"))?;
            for (index, step) in pm.steps.iter().enumerate() {
                if step.canonical_fact_id != message.event_id || step.step_index != index as u32
                    || step.derived_id.canonical_record_position != *canonical_position
                    || step.derived_id.step_index != step.step_index || step.derived_id.member_index != step.member_index {
                    return Err(io::Error::other("PM步骤身份或序位错误").into());
                }
                if let CompactPmChangeV1::InitialSnapshotFromInput { input_record, .. } = &step.change {
                    if input_record != &message.input_record { return Err(io::Error::other("PM快照输入关联不一致").into()); }
                }
                if matches!(&step.change, CompactPmChangeV1::Inline(PmStepChangeV1::InitialSnapshot { .. })) {
                    return Err(io::Error::other("紧凑步骤不得内联重复快照").into());
                }
            }
            if pm.reports.iter().any(|r|r.canonical_fact_id != message.event_id) {
                return Err(io::Error::other("PM报告与事实关联错误").into());
            }
        }
        if associations.len() != seen_pm.len() { return Err(io::Error::other("PM事实缺少完整材料关联").into()); }
        self.next_pm += packet.pm.len() as u64;
        self.next_step += packet.pm.iter().map(|p|p.steps.len() as u64).sum::<u64>();
        self.next_report += packet.reports.len() as u64 + packet.pm.iter().map(|p|p.reports.len() as u64).sum::<u64>();
        if final_packet && (self.next_group != self.manifest.groups || self.next_fact != self.manifest.facts
                            || end != self.manifest.files[0].bytes || self.next_pm != self.manifest.pm_packets
                            || self.next_step != self.manifest.pm_steps || self.next_report != self.manifest.reports) {
            return Err(io::Error::other("材料末端计数或offset覆盖不闭合").into());
        }
        self.next_offset = end;
        self.next += 1;
        Ok(Some(packet))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use data_pipeline_core::{
        public_input::{prepare_public_ingress_v1, TypedRawRowV2},
        public_reference::source_evidence_v3_from_typed_raw_v2,
        typed_raw::{TypedRawRow, BinanceDepthStitchVerdict, DepthLevel},
    };
    use crate::public_recording::tests::{metadata_snapshot_row, row, book, snapshot, update};

    #[test]
    fn compact_atomic_publication_roundtrips_source_and_rejects_changed_payload() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("compact");
        let header = PublicInputStreamHeaderV1 {
            input_stream_id: "compact-boundary".into(), canonical_rule_version: "rule-v1".into(),
            observation_version: "public-observation-v1".into(), ordering_version: "original-input-v1".into(),
            input_scope: "constructed-finite".into(), max_pending_ws_terminals: 2,
            initial_state: PublicInitialStateV1::Empty,
        };
        let sources = vec![
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot("depth-a",100,1))),
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(update("depth-a",101,101,2,
                Some(100),None,BinanceDepthStitchVerdict::Applied,
                vec![DepthLevel { price:"63900".into(),quantity:"1.234567890123456789".into() }]))),
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata_snapshot_row()))),
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-a",1,21_000,22_000,
                book("12.000000000000000000000000001")))),
        ];
        let mut core = PublicObservationCoreV1::new(header.clone()).unwrap();
        let mut writer = PublicCompactWriterV1::create(&target, header.clone()).unwrap();
        let mut original_inputs = vec![];
        let mut complete_groups = vec![];
        for (index, row) in sources.into_iter().enumerate() {
            let input = RecordedIngressV1 {
                input_stream_id: header.input_stream_id.clone(),input_position:index as u64,
                ingress: DptIngressV1::Message { source_evidence:source_evidence_v3_from_typed_raw_v2(&row).unwrap(),row },
                dependency_record_refs:vec![],
            };
            let ids = BTreeMap::new();
            let prepared = prepare_public_ingress_v1(&core,&input,&ids,&[]).unwrap();
            let applied = core.push_facts(&prepared).unwrap();
            complete_groups.extend(applied.groups.clone());
            original_inputs.push(input.clone());
            writer.push(&input,&applied).unwrap();
        }
        let input = RecordedIngressV1 {
            input_stream_id:header.input_stream_id.clone(),input_position:original_inputs.len() as u64,
            ingress:DptIngressV1::EndOfInput,dependency_record_refs:vec![],
        };
        let ids = BTreeMap::new();
        let prepared = prepare_public_ingress_v1(&core,&input,&ids,&[]).unwrap();
        let applied = core.push_facts(&prepared).unwrap();
        complete_groups.extend(applied.groups.clone());
        writer.push(&input,&applied).unwrap();
        original_inputs.push(input);
        assert!(!target.exists());
        let receipt = writer.publish().unwrap();
        assert!(receipt.normal_end && receipt.facts > 0 && receipt.groups > 0);
        let mut reader = PublicCompactReaderV1::open(&target,1024*1024).unwrap();
        let mut rebuilt = PublicObservationCoreV1::new(header).unwrap();
        let mut rebuilt_groups = vec![];
        let mut index = 0;
        let mut pm_snapshot_refs = 0;
        while let Some(packet) = reader.next_packet().unwrap() {
            assert_eq!(serde_json::to_value(&packet.input).unwrap(),serde_json::to_value(&original_inputs[index]).unwrap());
            let encoded = serde_json::to_value(&packet).unwrap();
            // 原快照/增量各只有输入内的一组bids/asks；派生事实/步骤不能重复保存整盘口。
            fn bid_arrays(value:&serde_json::Value)->usize {
                match value {
                    serde_json::Value::Object(fields)=>fields.iter().map(|(k,v)|usize::from(k=="bids")+bid_arrays(v)).sum(),
                    serde_json::Value::Array(values)=>values.iter().map(bid_arrays).sum(),_=>0,
                }
            }
            assert_eq!(bid_arrays(&encoded),usize::from(matches!(index,0|1|3)));
            pm_snapshot_refs += packet.pm.iter().flat_map(|p|&p.steps)
                .filter(|s|matches!(s.change,CompactPmChangeV1::InitialSnapshotFromInput{..})).count();
            let ids = BTreeMap::new();
            let prepared = prepare_public_ingress_v1(&rebuilt,&packet.input,&ids,&[]).unwrap();
            let applied = rebuilt.push_facts(&prepared).unwrap();
            assert_eq!(packet.groups,applied.groups.iter().map(CompactGroupV1::from).collect::<Vec<_>>());
            rebuilt_groups.extend(applied.groups);
            index += 1;
        }
        assert_eq!(index,original_inputs.len());
        assert!(pm_snapshot_refs>0);
        assert_eq!(rebuilt_groups,complete_groups);
        let depths:Vec<_> = complete_groups.iter().flat_map(|g|g.records.iter()).filter_map(|f|match &f.payload {
            PublicBusinessPayloadV1::ReferenceFact(CanonicalReferenceEventPayloadV2::BinanceDepthState(d))=>Some(d),_=>None,
        }).collect();
        assert_eq!(depths.len(),2);
        assert_eq!(depths[0].bids.len(),2);
        assert_eq!(depths[1].bids.len(),3);
        assert!(depths[1].bids.iter().any(|v|v.quantity_btc.0=="1.234567890123456789"));
        // 实际审查反例：成员SHA均不变，仅末端facts计数篡改也必须拒绝。
        let manifest_path = target.join("manifest.json");
        let mut manifest:PublicCompactManifestV1=serde_json::from_reader(File::open(&manifest_path).unwrap()).unwrap();
        manifest.facts+=1;
        serde_json::to_writer(File::create(manifest_path).unwrap(),&manifest).unwrap();
        let mut damaged=PublicCompactReaderV1::open(&target,1024*1024).unwrap();
        let mut rejected=false;
        loop {
            match damaged.next_packet() { Ok(Some(_))=>{},Ok(None)=>break,Err(_)=>{rejected=true;break;} }
        }
        assert!(rejected);
    }
}
