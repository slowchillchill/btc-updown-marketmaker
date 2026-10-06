//! 公共完整事实的紧凑冷材料。输入/组/PM步骤各保存一次，不写模型观察或逐事件全盘口。
//! wire侧车复用原强类型JSON并逐packet独立Zstd压缩；固定列供批量GPU转换，hidden独立排序归约。
use std::{
    fs::{self, File},
    io::{self, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use contracts::*;
use data_pipeline_core::{
    public_input::{DptIngressV1, RecordedIngressV1, PreparedIngressV1, TypedRawRowV2},
    pm_reduction::{PmEvidencePairV1, PmReductionDispositionV2, PmReductionReportV2, PmReductionStepV1, PmStepChangeV1},
    public_observation_core::{PublicFactAppliedIngressV1, PublicFactGroupV1, PublicObservationCoreV1},
    public_reduction::PublicReductionReportV1,
    reduction::ReductionDispositionV1,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use crate::public_recording::RecordingResult;
use crate::public_replay::HistoricalMergeV1;

/// 紧凑准备只保留实际使用的冷入口参数，不要求旧Gate-A recording配置。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactPublicPreparationConfigV1 {
    pub files: Vec<crate::public_input::PublicFiniteFileV1>,
    pub received_start_utc_ms: i64,
    pub received_end_utc_ms: i64,
    pub work_root: PathBuf,
    pub header: PublicInputStreamHeaderV1,
    pub canonical: crate::canonical_run::CanonicalRunConfig,
    pub initialization: Option<crate::historical_initialization::MetadataInitializationConfigV1>,
}

const SCHEMA: &str = "public_compact_facts_v4_reference_selection_zstd";
const ZSTD_V3_SCHEMA: &str = "public_compact_facts_v3_zstd";
const LEGACY_SCHEMA: &str = "public_compact_facts_v2";

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
pub struct CompactPmStepV1<I = PmDerivedStepPositionV1> {
    pub canonical_fact_id: CanonicalEventId,
    pub member_index: Option<u32>, pub step_index: u32, pub derived_id: I,
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
pub struct CompactReferencePreparationV1 {
    pub market: Option<MarketIdentityV2>,
    pub selection: Option<PreparedReferenceSelectionV1>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicCompactPacketV1 {
    pub input: RecordedIngressV1,
    pub groups: Vec<CompactGroupV1>,
    pub pm: Vec<CompactPmPacketV1>,
    pub reports: Vec<PublicReductionReportV1>,
    /// None是旧材料或未使用原准备器，不能解释成已选择“无参考”。
    /// Some(None,None)表示原准备器已执行，本输入没有市场选择操作。
    pub reference_preparation: Option<CompactReferencePreparationV1>,
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
        Ok(Self { input: input.clone(), groups, pm, reference_preparation:None,
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
#[derive(Serialize)]
pub struct CompactPublicationProgressV1<'a> {
    pub stage: &'static str,
    pub member: &'a str,
    pub completed_bytes: u64,
    pub total_bytes: Option<u64>,
}
#[cfg(test)]
fn file_sha256(path: &Path) -> io::Result<String> {
    file_sha256_with_progress(path, Duration::from_secs(10), |_, _| {})
}
pub(crate) fn file_sha256_with_progress(
    path: &Path, interval: Duration, mut progress: impl FnMut(u64, u64),
) -> io::Result<String> {
    let mut file = File::open(path)?;
    let total = file.metadata()?.len();
    let mut completed = 0_u64;
    let mut last_notice = Instant::now();
    progress(completed, total);
    let mut digest = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 { break; }
        digest.update(&buffer[..count]);
        completed += count as u64;
        if last_notice.elapsed() >= interval {
            progress(completed, total);
            last_notice = Instant::now();
        }
    }
    progress(completed, total);
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
        self.push_reference(input,applied,None)
    }
    pub fn push_prepared(&mut self, input: &RecordedIngressV1, prepared: &PreparedIngressV1<'_>,
                         applied: &PublicFactAppliedIngressV1<'_>) -> RecordingResult<()> {
        if input.input_position!=prepared.recorded.input_position
            || input.input_stream_id!=prepared.recorded.input_stream_id
            || &input.ingress!=prepared.recorded.ingress
            || input.dependency_record_refs!=prepared.recorded.dependency_record_refs {
            return Err(io::Error::other("参考准备与实际入流记录不一致").into());
        }
        self.push_reference(input,applied,Some(CompactReferencePreparationV1 {
            market:prepared.reference_selection_market.clone(),selection:prepared.reference_selection.clone(),
        }))
    }
    fn push_reference(&mut self, input: &RecordedIngressV1,
                      applied: &PublicFactAppliedIngressV1<'_>,
                      preparation: Option<CompactReferencePreparationV1>) -> RecordingResult<()> {
        if self.failed || self.manifest.normal_end { return Err(io::Error::other("失败或结束材料不能续写").into()); }
        self.failed = true;
        if input.input_stream_id != self.manifest.header.input_stream_id
            || input.input_position != self.manifest.inputs
            || applied.applied_prefix.input_stream_id != input.input_stream_id
            || applied.applied_prefix.next_input_position != input.input_position + 1 {
            return Err(io::Error::other("材料输入与应用前缀不连续").into());
        }
        let mut packet = PublicCompactPacketV1::build(input, applied)?;
        packet.reference_preparation = preparation;
        self.write_packet(packet)
    }
    fn write_packet(&mut self, packet: PublicCompactPacketV1) -> RecordingResult<()> {
        self.failed = true;
        self.manifest.pm_packets += packet.pm.len() as u64;
        self.manifest.pm_steps += packet.pm.iter().map(|p| p.steps.len() as u64).sum::<u64>();
        self.manifest.reports += packet.reports.len() as u64 + packet.pm.iter().map(|p| p.reports.len() as u64).sum::<u64>();
        let bytes = zstd::bulk::compress(&serde_json::to_vec(&packet)?, 3)?;
        self.payload.write_all(&bytes)?;
        self.payload_bytes = self.payload_bytes.checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("材料字节位置溢出"))?;
        self.offsets.write_all(&self.payload_bytes.to_le_bytes())?;
        for group in &packet.groups {
            if group.group_position != self.manifest.groups || group.closed_at_input_position != packet.input.input_position {
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
        self.manifest.applied_prefix = AppliedIngressPrefixV1 {
            input_stream_id: packet.input.input_stream_id.clone(), next_input_position: packet.input.input_position + 1,
        };
        self.manifest.normal_end = matches!(packet.input.ingress, DptIngressV1::EndOfInput);
        self.failed = false;
        Ok(())
    }
    pub fn publish(self) -> RecordingResult<PublicCompactManifestV1> {
        self.publish_with_progress(Duration::from_secs(10), |_| {})
    }
    pub fn publish_with_progress(
        mut self, interval: Duration, mut progress: impl FnMut(CompactPublicationProgressV1<'_>),
    ) -> RecordingResult<PublicCompactManifestV1> {
        if self.failed || !self.manifest.normal_end { return Err(io::Error::other("仅完整正常结束材料可发布").into()); }
        for (name, file) in [("packet_payload.bin", &mut self.payload), ("packet_offsets.u64le", &mut self.offsets),
                             ("group_columns.i64le", &mut self.columns), ("source_bindings.jsonl", &mut self.sources)] {
            progress(CompactPublicationProgressV1 { stage:"flush_and_sync", member:name, completed_bytes:0,total_bytes:None });
            file.flush()?;
            file.get_ref().sync_all()?;
            let bytes = file.get_ref().metadata()?.len();
            progress(CompactPublicationProgressV1 { stage:"flush_and_sync_done", member:name, completed_bytes:bytes,total_bytes:Some(bytes) });
        }
        for name in ["packet_payload.bin", "packet_offsets.u64le", "group_columns.i64le", "source_bindings.jsonl"] {
            let path = self.pending.join(name);
            self.manifest.files.push(CompactFileV1 {
                name: name.into(), bytes: fs::metadata(&path)?.len(),
                sha256: file_sha256_with_progress(&path, interval, |completed,total| {
                    progress(CompactPublicationProgressV1 { stage:"member_sha256",member:name,completed_bytes:completed,total_bytes:Some(total) });
                })?,
            });
        }
        progress(CompactPublicationProgressV1 { stage:"atomic_manifest_publish",member:"manifest.json",completed_bytes:0,total_bytes:None });
        let mut manifest = File::create_new(self.pending.join("manifest.json"))?;
        serde_json::to_writer(&mut manifest, &self.manifest)?;
        manifest.sync_all()?;
        File::open(&self.pending)?.sync_all()?;
        if self.target.exists() { return Err(io::Error::other("材料目标已存在，保留pending").into()); }
        rustix::fs::renameat_with(rustix::fs::CWD, &self.pending,
            rustix::fs::CWD, &self.target, rustix::fs::RenameFlags::NOREPLACE)?;
        File::open(self.target.parent().unwrap())?.sync_all()?;
        progress(CompactPublicationProgressV1 { stage:"published",member:"manifest.json",completed_bytes:0,total_bytes:None });
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
        writer.push_prepared(&recorded,&prepared, &applied)?;
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
    writer.push_prepared(&recorded,&prepared, &applied)?;
    progress(end.input_position + 1)?;
    Ok(core)
}

pub struct PublicCompactReaderV1 {
    pub manifest: PublicCompactManifestV1,
    payload: File,
    offsets: File,
    columns: File,
    sources: File,
    next: u64,
    next_group: u64,
    next_fact: u64,
    next_offset: u64,
    next_pm: u64,
    next_step: u64,
    next_report: u64,
    max_packet_bytes: usize,
    reference_sources: std::collections::BTreeMap<u64,(ChainlinkBtcUsdTwapReferenceV1,SourceEvidenceRefV3)>,
    reference_markets: std::collections::BTreeMap<String,MarketIdentityV2>,
}
impl PublicCompactReaderV1 {
    /// 冷派生材料校验原规范位置；只读完整文件，不推进或签发运行时可见前缀。
    pub(crate) fn canonical_fact(&mut self, position: u64) -> RecordingResult<CompactFactV1> {
        let cursor = self.columns.stream_position()?;
        let result = self.cold_canonical_fact(position);
        self.columns.seek(SeekFrom::Start(cursor))?;
        result
    }

    fn cold_canonical_fact(&mut self, position: u64) -> RecordingResult<CompactFactV1> {
        if position >= self.manifest.facts { return Err(io::Error::other("原规范事实位置越界").into()); }
        let mut low = 0_u64; let mut high = self.manifest.groups;
        while low < high {
            let mid = low + (high-low)/2;
            self.columns.seek(SeekFrom::Start(mid.checked_mul(40).ok_or_else(|| io::Error::other("组列索引溢出"))?))?;
            let mut words = [0_u8;40]; self.columns.read_exact(&mut words)?;
            let end = u64::from_le_bytes(words[32..40].try_into()?);
            if end <= position { low = mid+1; } else { high = mid; }
        }
        self.columns.seek(SeekFrom::Start(low.checked_mul(40).ok_or_else(|| io::Error::other("组列索引溢出"))?))?;
        let mut words = [0_u8;40]; self.columns.read_exact(&mut words)?;
        let group_position = u64::from_le_bytes(words[..8].try_into()?);
        let input_position = u64::from_le_bytes(words[8..16].try_into()?);
        let first = u64::from_le_bytes(words[24..32].try_into()?);
        let end = u64::from_le_bytes(words[32..40].try_into()?);
        if !(first<=position && position<end) { return Err(io::Error::other("原事实位置与组列不闭合").into()); }
        let (packet, _, _) = self.decode_packet_at(input_position)?;
        let group = packet.groups.iter().find(|group| group.group_position==group_position)
            .ok_or_else(|| io::Error::other("原事实组不存在"))?;
        if group.records.len() as u64 != end-first { return Err(io::Error::other("原事实组载荷/列不闭合").into()); }
        Ok(group.records[usize::try_from(position-first)?].clone())
    }

    /// 已验收前缀内的原输入引用；读取不会推进组/事实光标。
    /// 供独立hidden准备恢复原CanonicalPmMessage，不复制输入全集到内存。
    pub fn input_record(&mut self, reference: &CanonicalInputRecordRefV1) -> RecordingResult<RecordedIngressV1> {
        if reference.input_stream_id != self.manifest.header.input_stream_id
            || reference.input_position >= self.next {
            return Err(io::Error::other("原输入引用未属于已读取的合法前缀").into());
        }
        self.cold_input_record(reference)
    }

    /// 派生材料冷解码只读原完整文件；不得用此位置推进模型/公开运行时前沿。
    pub(crate) fn cold_input_record(&mut self, reference: &CanonicalInputRecordRefV1) -> RecordingResult<RecordedIngressV1> {
        if reference.input_stream_id != self.manifest.header.input_stream_id
            || reference.input_position >= self.manifest.inputs {
            return Err(io::Error::other("冷原输入引用越过完整来源范围").into());
        }
        let (packet, _, _) = self.decode_packet_at(reference.input_position)?;
        if packet.input.input_stream_id != reference.input_stream_id
            || packet.input.input_position != reference.input_position {
            return Err(io::Error::other("原输入引用与实际packet不一致").into());
        }
        Ok(packet.input)
    }

    /// SHA核对完整文件；上限只限制本次读取分配，不改变业务或签发数据验收。
    pub fn open(root: &Path, max_packet_bytes: usize) -> RecordingResult<Self> {
        Self::open_with_progress(root, max_packet_bytes, Duration::from_secs(10), |_| {})
    }
    pub fn open_with_progress(
        root: &Path, max_packet_bytes: usize, interval: Duration,
        mut progress: impl FnMut(CompactPublicationProgressV1<'_>),
    ) -> RecordingResult<Self> {
        progress(CompactPublicationProgressV1 {stage:"read_manifest",member:"manifest.json",completed_bytes:0,total_bytes:None});
        let manifest: PublicCompactManifestV1 = serde_json::from_reader(File::open(root.join("manifest.json"))?)?;
        if !matches!(manifest.schema_version.as_str(), SCHEMA | ZSTD_V3_SCHEMA | LEGACY_SCHEMA) || !manifest.normal_end || manifest.inputs == 0
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
            let sha256 = file_sha256_with_progress(&root.join(name), interval, |completed, total| {
                progress(CompactPublicationProgressV1 {stage:"read_member_sha256",member:name,
                    completed_bytes:completed,total_bytes:Some(total)});
            })?;
            if sha256 != item.sha256 { return Err(io::Error::other("材料SHA错误").into()); }
        }
        if manifest.files[1].bytes != manifest.inputs.checked_add(1).and_then(|n| n.checked_mul(8)).unwrap_or(0)
            || manifest.files[2].bytes != manifest.groups.checked_mul(40).unwrap_or(0) {
            return Err(io::Error::other("材料固定列长度错误").into());
        }
        Ok(Self { payload: File::open(root.join(expected[0]))?, offsets: File::open(root.join(expected[1]))?,
                  columns: File::open(root.join(expected[2]))?, sources: File::open(root.join(expected[3]))?,
                  manifest, next: 0, next_group: 0, next_fact: 0, next_offset: 0,
                  next_pm: 0, next_step: 0, next_report: 0, max_packet_bytes,
                  reference_sources:Default::default(),reference_markets:Default::default() })
    }
    fn decode_packet_at(&mut self, position: u64) -> RecordingResult<(PublicCompactPacketV1, u64, u64)> {
        let offset = position.checked_mul(8)
            .ok_or_else(|| io::Error::other("材料输入索引溢出"))?;
        let mut value = [0_u8; 8];
        self.offsets.seek(SeekFrom::Start(offset))?;
        self.offsets.read_exact(&mut value)?;
        let start = u64::from_le_bytes(value);
        self.offsets.read_exact(&mut value)?;
        let end = u64::from_le_bytes(value);
        if start >= end || end > self.manifest.files[0].bytes || end - start > self.max_packet_bytes as u64 {
            return Err(io::Error::other("材料payload偏移或本次资源容量错误").into());
        }
        let mut bytes = vec![0; usize::try_from(end - start)?];
        self.payload.seek(SeekFrom::Start(start))?;
        self.payload.read_exact(&mut bytes)?;
        let decoded = if self.manifest.schema_version != LEGACY_SCHEMA {
            let frame_bytes = zstd::zstd_safe::get_frame_content_size(&bytes)
                .map_err(|error| io::Error::other(format!("材料压缩帧头错误: {error:?}")))?
                .map(usize::try_from).transpose()?.unwrap_or(self.max_packet_bytes);
            if frame_bytes > self.max_packet_bytes {
                return Err(io::Error::other("材料解压帧超过本次读取容量").into());
            }
            zstd::bulk::decompress(&bytes, frame_bytes)?
        } else { bytes };
        Ok((serde_json::from_slice(&decoded)?, start, end))
    }

    pub fn next_packet(&mut self) -> RecordingResult<Option<PublicCompactPacketV1>> {
        if self.next == self.manifest.inputs { return Ok(None); }
        let (packet, start, end) = self.decode_packet_at(self.next)?;
        if start != self.next_offset {
            return Err(io::Error::other("材料payload没有连续完整覆盖").into());
        }
        let mut value = [0_u8; 8];
        if packet.input.input_stream_id != self.manifest.header.input_stream_id || packet.input.input_position != self.next {
            return Err(io::Error::other("材料packet输入位置错误").into());
        }
        if self.manifest.schema_version != SCHEMA && packet.reference_preparation.is_some() {
            return Err(io::Error::other("旧schema不能声明新参考选择能力").into());
        }
        if let DptIngressV1::Message {row:TypedRawRowV2::PolymarketRtdsChainlinkTwap(row),source_evidence} = &packet.input.ingress {
            let event = data_pipeline_core::public_reference::canonicalize_chainlink_twap_v1(
                row,source_evidence,&self.manifest.header.canonical_rule_version)?;
            if let CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(value) = event.payload {
                self.reference_sources.insert(self.next,(value,source_evidence.clone()));
            }
        }
        for fact in packet.groups.iter().flat_map(|g|&g.records) {
            if let CompactFactPayloadV1::Inline(PublicBusinessPayloadV1::MarketFact {
                market,payload:CanonicalMarketFactPayloadV1::FixedIdentity {..},
            }) = &fact.payload {
                self.reference_markets.insert(market.condition_id.0.clone(),market.clone());
            }
        }
        if let Some(preparation) = &packet.reference_preparation {
            match (&preparation.market,&preparation.selection) {
                (None,None)=>{},
                (Some(market),Some(selection))=>{
                    if self.reference_markets.get(&market.condition_id.0)!=Some(market)
                        || selection.dependency_input_position!=self.next {
                        return Err(io::Error::other("参考选择市场/依赖位置错误").into());
                    }
                    match (&selection.selected_record_ref,&selection.selected_value,&selection.direct_anchor) {
                        (None,None,None)=>{},
                        (Some(record),Some(value),Some(anchor))=>{
                            let expected = self.reference_sources.get(&record.input_position)
                                .ok_or_else(||io::Error::other("参考选择没有实际TWAP源记录"))?;
                            if record.input_stream_id!=packet.input.input_stream_id || record.input_position>self.next
                                || (record.input_position<self.next && !packet.input.dependency_record_refs.contains(record))
                                || value!=&expected.0 || anchor.position.input_stream_id!=record.input_stream_id
                                || anchor.position.closed_at_input_position!=record.input_position || anchor.position.fact_ordinal!=0
                                || anchor.group_id!=expected.1.source_message_id || anchor.source_message_id!=expected.1.source_message_id
                                || anchor.source_event_at_utc_ms!=expected.1.source_event_at_utc_ms
                                || anchor.received_at_utc_ms!=expected.1.received_at_utc_ms
                                || anchor.field_path!=if value.window_s==30 {"chainlink_twap_thirty"} else {"chainlink_twap_sixty"} {
                                return Err(io::Error::other("参考选择精确值/源引用/直接锚点不闭合").into());
                            }
                        },
                        _=>return Err(io::Error::other("参考选择字段不完整").into()),
                    }
                },
                _=>return Err(io::Error::other("参考准备市场与选择不配对").into()),
            }
        }
        let final_packet = self.next + 1 == self.manifest.inputs;
        if final_packet != matches!(packet.input.ingress, DptIngressV1::EndOfInput)
            || matches!(packet.input.ingress, DptIngressV1::Aborted { .. }) {
            return Err(io::Error::other("材料末端没有正常结束输入").into());
        }
        let mut associations = std::collections::BTreeMap::new();
        let mut ordinal = 0_u32;
        let mut fact_positions = std::collections::BTreeSet::new();
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
                fact_positions.insert(&fact.position);
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
                    || step.derived_id.consumer_stream_id != self.manifest.header.input_stream_id
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
            for report in &pm.reports {
                if report.canonical_fact_id != message.event_id {
                    return Err(io::Error::other("PM报告与事实关联错误").into());
                }
                if let PmReductionDispositionV2::Produced { derived_id } = &report.disposition {
                    if !pm.steps.iter().any(|step| &step.derived_id == derived_id) {
                        return Err(io::Error::other("PM Produced报告没有同流实际步骤").into());
                    }
                }
            }
        }
        for (index, report) in packet.reports.iter().enumerate() {
            if report.position.input_stream_id != self.manifest.header.input_stream_id
                || report.position.closed_at_input_position != self.next
                || report.position.report_ordinal as usize != index {
                return Err(io::Error::other("公共报告流或关闭位置序位错误").into());
            }
            if let ReductionDispositionV1::Produced { result_event_id } = &report.report.disposition {
                if !fact_positions.contains(result_event_id) {
                    return Err(io::Error::other("公共Produced报告没有实际同批事实").into());
                }
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

/// 重用已完成/结构合法的旧冷材料，只转换物理编码，不重跑业务归约或原源解码。
pub fn repack_public_compact_v1(
    reader: &mut PublicCompactReaderV1, writer: &mut PublicCompactWriterV1,
    mut progress: impl FnMut(u64),
) -> RecordingResult<()> {
    if reader.next != 0 || writer.manifest.inputs != 0 || writer.failed
        || serde_json::to_value(&reader.manifest.header)? != serde_json::to_value(&writer.manifest.header)? {
        return Err(io::Error::other("重编码须从同一材料头的完整起点开始").into());
    }
    for source in serde_json::Deserializer::from_reader(reader.sources.try_clone()?)
        .into_iter::<SourceArtifactBindingV1>() {
        writer.source_binding(&source?)?;
    }
    while let Some(packet) = reader.next_packet()? {
        let processed = packet.input.input_position + 1;
        writer.write_packet(packet)?;
        progress(processed);
    }
    Ok(())
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

    fn prepare_reference<'a>(core:&PublicObservationCoreV1,input:&'a mut RecordedIngressV1,
        candidates:&[data_pipeline_core::public_reference::TemporaryReferenceCandidateV1])
        -> data_pipeline_core::public_input::PreparedIngressV1<'a> {
        if let Some(context) = core.fixed_markets().next() {
            data_pipeline_core::public_input::prepare_public_ingress_with_reference_v1(
                core,input,&BTreeMap::new(),&[],&context,|cursor| {
                    Ok((candidates.iter().filter(|c|c.record_ref.input_position>=cursor.next_input_position)
                        .cloned().collect(),cursor,true))
                }).unwrap()
        } else {
            prepare_public_ingress_v1(core,input,&BTreeMap::new(),&[]).unwrap()
        }
    }

    #[test]
    #[ignore = "明确在AutoDL同提交GPU环境运行；包含独立原core语义对照"]
    fn gpu_public_zero_selection_matches_original_core() {
        use data_pipeline_core::{public_observation_core::PmOwnedStateV1,
            pm_reduction::{PmStateReadV1, PmStepChangeV1}, typed_raw::{PolymarketOrderbookEvent, PolymarketPriceLevel}};
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("compact");
        let header = PublicInputStreamHeaderV1 {
            input_stream_id:"gpu-zero-real-construction".into(),canonical_rule_version:"rule-v1".into(),
            observation_version:"public-observation-v1".into(),ordering_version:"original-input-v1".into(),
            input_scope:"constructed-short-zero-boundary".into(),max_pending_ws_terminals:2,initial_state:PublicInitialStateV1::Empty,
        };
        let mut next_book = book("10");
        if let PolymarketOrderbookEvent::Book {bids,..} = &mut next_book {
            bids.push(PolymarketPriceLevel {price:"0.4".into(),size:"0".into()});
            bids.push(PolymarketPriceLevel {price:"0.51".into(),size:"3".into()});
        }
        // 终态必须经原metadata+WS配对入口；单边WS只能保持pending。
        let terminal_metadata = |sequence:u64,source:i64,received:i64| {
            use data_pipeline_core::typed_raw::*;
            let mut m = metadata_snapshot_row();
            m.metadata_row_kind = PolymarketMetadataRowKind::ResolutionTerminalWs;
            m.metadata_origin = PolymarketMetadataOrigin::MarketResolvedWs;
            m.timestamp_received = received;
            m.collector_admission_sequence = sequence;
            m.collector_admission_time_utc_ms = received+1;
            m.market_lifecycle_state = MetadataMarketLifecycleState::Retired;
            m.market_runtime_role = MetadataMarketRuntimeRole::None;
            m.source_endpoint = Some("wss://ws-subscriptions-clob.polymarket.com/ws/market".into());
            m.source_evidence_origin = Some(MetadataSourceEvidenceOrigin::MarketResolvedWs);
            m.source_evidence_event_type = Some(MetadataSourceEvidenceEventType::MarketResolved);
            m.source_evidence_channel = Some(MetadataSourceEvidenceChannel::Market);
            m.source_evidence_received_at_utc = Some(received);
            m.row_evidence_validation_completed_at_utc = Some(received+1);
            m.source_evidence_payload_json = None;
            m.source_evidence_raw_sha256 = Some("a".repeat(64));
            m.market_lifecycle_transition = Some(MetadataMarketLifecycleTransition::ActiveToRetired);
            m.lifecycle_applied_at_utc = Some(received+2);
            m.market_runtime_role_transition = Some(MetadataMarketRuntimeRoleTransition::ResolutionPendingToNone);
            m.runtime_role_applied_at_utc = Some(received+2);
            m.metadata_fetch_start_utc = None;
            m.closed = None; m.active = None; m.accepting_orders = None;
            m.active_unavailable_reason = Some(MetadataActiveUnavailableReason::NotApplicableForRowKind);
            m.outcomes_raw = None; m.outcome_prices_raw = None;
            m.ws_platform_timestamp_raw = Some(source.to_string());
            m.ws_platform_timestamp_utc = Some(source);
            m.winning_asset_id = Some("20".into()); m.winning_outcome = Some("UP".into());
            m.resolution_evidence_source = Some(MetadataResolutionEvidenceSource::MarketResolvedWs);
            m.terminal_claim_owner = Some(MetadataTerminalClaimOwner::MarketResolvedWs);
            m.resolution_budget_horizon_utc = Some(m.market_end_utc+8*60_000);
            m.resolution_followup_status = Some(MetadataResolutionFollowupStatus::TerminalWs);
            m.ws_claimant_source_sequence_epoch_id = Some("epoch-a".into());
            m.ws_claimant_collector_admission_sequence = Some(sequence);
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(m)))
        };
        let reference_source = |at:i64| {
            let row: data_pipeline_core::typed_raw::PolymarketRtdsChainlinkBtcUsdRow = serde_json::from_value(serde_json::json!({
                "timestamp_received":at,"message_timestamp":at-1,"payload_timestamp":at-3,"age_ms":3,
                "symbol":"btc/usd","value":"64000","topic":"crypto_prices_chainlink","message_type":"update",
                "freshness_status":"Fresh","subscription_filter":"{\"symbol\":\"btc/usd\"}",
                "source_native_payload_json":serde_json::json!({"topic":"crypto_prices_chainlink","type":"update",
                    "timestamp":at-1,"payload":{"symbol":"btc/usd","timestamp":at-3,"value":"64000"}}).to_string(),
                "collector_host_id":"collector-1","collector_region":"eu-west-1",
                "source_endpoint":"wss://ws-live-data.polymarket.com",
                "schema_version":data_pipeline_core::typed_raw::POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA,
            })).unwrap();
            row.validate().unwrap();
            TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row))
        };
        let aggregate_trade = |sequence:u64,price:&str,quantity:&str,maker:bool| {
            use data_pipeline_core::typed_raw::*;
            let at = 38000+sequence as i64;
            let native = serde_json::json!({"e":"aggTrade","E":at,"s":"BTCUSDT","a":sequence,
                "p":price,"q":quantity,"f":sequence,"l":sequence,"T":at,"m":maker,"M":true});
            let trade = BinanceAggTradeRow {
                timestamp_received:at+100,rest_backfill_timestamp_received:None,ingest_source:"websocket".into(),
                rest_was_best_price_match:None,source_native_payload_json:native.to_string(),
                source_sequence_epoch_id:"agg-epoch".into(),connection_generation:1,websocket_ingress_sequence:sequence,
                rest_response_ingress_sequence:None,rest_response_item_index:None,collector_admission_sequence:sequence,
                collector_admission_time_utc_ms:at+101,collector_host_id:"collector-1".into(),collector_region:"eu-west-1".into(),
                source_endpoint:"wss://stream.binance.com:9443/ws/btcusdt@aggTrade".into(),schema_version:BINANCE_AGGTRADE_SCHEMA.into(),
                record:BinanceAggTradeRecord::Trade {event_type:"aggTrade".into(),event_time:at,trade_time:at,
                    symbol:"BTCUSDT".into(),aggregate_trade_id:sequence,price:price.into(),quantity:quantity.into(),
                    first_trade_id:sequence,last_trade_id:sequence,buyer_is_maker:maker,stream:"btcusdt@aggTrade".into(),ws_ignore:true},
            };
            trade.validate().unwrap();
            TypedRawRowV2::Existing(TypedRawRow::BinanceAggTrade(trade))
        };
        let mut sources = vec![
            reference_source(200),reference_source(100),
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata_snapshot_row()))),
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-a",1,21000,22000,book("12")))),
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-a",2,23000,24000,next_book))),
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-a",3,25000,26000,
                PolymarketOrderbookEvent::BestBidAsk {asset_id:"20".into(),best_bid:Some("0.45".into()),
                    best_ask:Some("0.65".into()),spread:Some("0.20".into())}))),
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-a",4,27000,28000,
                PolymarketOrderbookEvent::BestBidAsk {asset_id:"20".into(),best_bid:Some("0.45".into()),
                    best_ask:Some("0.65".into()),spread:Some("0.20".into())}))),
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-a",5,29000,30000,
                PolymarketOrderbookEvent::TickSizeChange {asset_id:"20".into(),old_tick_size:Some("0.01".into()),
                    new_tick_size:"0.001".into()}))),
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-a",6,31000,32000,
                PolymarketOrderbookEvent::TickSizeChange {asset_id:"20".into(),old_tick_size:Some("0.001".into()),
                    new_tick_size:"0.001".into()}))),
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-a",7,33000,34000,
                PolymarketOrderbookEvent::LastTradePrice {asset_id:"20".into(),price:"0.55".into(),size:Some("2.5".into()),
                    side:Some(data_pipeline_core::typed_raw::PolymarketSide::Buy),fee_rate_bps:Some("100".into())}))),
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-a",8,35000,36000,
                PolymarketOrderbookEvent::LastTradePrice {asset_id:"20".into(),price:"0.57".into(),size:None,
                    side:None,fee_rate_bps:None}))),
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-a",9,37000,38000,
                PolymarketOrderbookEvent::LastTradePrice {asset_id:"20".into(),price:"0.53".into(),size:Some("1.25".into()),
                    side:Some(data_pipeline_core::typed_raw::PolymarketSide::Unknown),fee_rate_bps:None}))),
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot("depth-a",100,1))),
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(update("depth-a",101,101,2,Some(100),None,
                BinanceDepthStitchVerdict::Applied,vec![DepthLevel {price:"64000".into(),quantity:"0".into()},
                    DepthLevel {price:"63900".into(),quantity:"1.234".into()}]))),
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(update("depth-a",104,104,3,Some(100),Some(101),
                BinanceDepthStitchVerdict::NotAppliedConfirmedHole,vec![DepthLevel {price:"64100".into(),quantity:"0.9".into()}]))),
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot("depth-b",200,1))),
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(update("depth-c",201,201,1,None,None,
                BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable,vec![DepthLevel {price:"64100".into(),quantity:"0.99".into()}]))),
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot("depth-c",300,2))),
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(update("depth-c",301,301,3,Some(300),None,
                BinanceDepthStitchVerdict::Applied,vec![DepthLevel {price:"64000".into(),quantity:"0".into()},
                    DepthLevel {price:"63900".into(),quantity:"1.234".into()}]))),
            match crate::public_recording::tests::twap_record(12,30,23002,"65000000000000000000000").ingress {
                DptIngressV1::Message {row,..}=>row,_=>unreachable!(),
            },
            match crate::public_recording::tests::twap_record(13,30,24002,"66000000000000000000000").ingress {
                DptIngressV1::Message {row,..}=>row,_=>unreachable!(),
            },
            match crate::public_recording::tests::rule_record(14,30,None).ingress {
                DptIngressV1::Message {row,..}=>row,_=>unreachable!(),
            },
            aggregate_trade(1,"64000","1",false),aggregate_trade(2,"64001","2",true),
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-a",10,38500,38600,
                PolymarketOrderbookEvent::Book {asset_id:"3".into(),bids:vec![],asks:vec![],last_trade_price:None}))),
            terminal_metadata(11,39000,40000),
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-a",11,39000,40000,
                PolymarketOrderbookEvent::MarketResolved {clob_market_id:"3195576".into(),question:None,
                    description:None,asset_ids:vec!["20".into(),"3".into()],outcomes:vec!["UP".into(),"DOWN".into()],
                    event_message:None,winning_asset_id:"20".into(),winning_outcome:"UP".into()}))),
            terminal_metadata(12,41000,42000),
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-a",12,41000,42000,
                PolymarketOrderbookEvent::MarketResolved {clob_market_id:"3195576".into(),question:None,
                    description:None,asset_ids:vec!["20".into(),"3".into()],outcomes:vec!["UP".into(),"DOWN".into()],
                    event_message:None,winning_asset_id:"3".into(),winning_outcome:"DOWN".into()}))),
        ];
        // 同值新身份及超过16条的窗内路径，均从原合法BBA输入产生。
        for source in &mut sources {
            match source {
                TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)) if row.collector_admission_sequence>=4 => {
                    row.collector_admission_sequence += 20;
                },
                TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(row)) => {
                    if let Some(sequence) = row.ws_claimant_collector_admission_sequence.as_mut() { *sequence += 20; }
                },
                _ => {},
            }
        }
        sources.splice(6..6,(0..20).map(|index| {
            let bid = if index%2==0 {"0.45"} else {"0.50"};
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-a",4+index,26010+index as i64,
                26011+index as i64,PolymarketOrderbookEvent::BestBidAsk {asset_id:"20".into(),
                    best_bid:Some(bid.into()),best_ask:Some("0.65".into()),spread:None})))
        }));
        let mut core = PublicObservationCoreV1::new(header.clone()).unwrap();
        let mut writer = PublicCompactWriterV1::create(&target,header.clone()).unwrap();
        let mut observation_core = PublicObservationCoreV1::new(header.clone()).unwrap();
        let mut public_assets = Vec::new();
        let mut public_events = Vec::new();
        let mut public_visible_times = Vec::new();
        let mut public_market = serde_json::Value::Null;
        let mut terminal_observations = Vec::new();
        let mut first_sides = serde_json::Value::Null;
        let mut first_binance = serde_json::Value::Null;
        let mut binance_observations = Vec::new();
        let mut source_observations = Vec::new();
        let mut observed_groups = 0_u64;
        let mut reference_candidates = Vec::new();
        let mut original = None;
        let mut zero_evidence_without_change = false;
        let mut token = String::new();
        for (position, source) in sources.into_iter().map(Some).chain(std::iter::once(None)).enumerate() {
            let ingress = source.map_or(DptIngressV1::EndOfInput,|row|DptIngressV1::Message {
                source_evidence:source_evidence_v3_from_typed_raw_v2(&row).unwrap(),row,
            });
            let mut input = RecordedIngressV1 {input_stream_id:header.input_stream_id.clone(),input_position:position as u64,
                ingress,dependency_record_refs:vec![]};
            if let DptIngressV1::Message {row:TypedRawRowV2::PolymarketRtdsChainlinkTwap(row),source_evidence} = &input.ingress {
                let canonical_fact = data_pipeline_core::public_reference::canonicalize_chainlink_twap_v1(
                    row,source_evidence,&header.canonical_rule_version).unwrap();
                reference_candidates.push(data_pipeline_core::public_reference::TemporaryReferenceCandidateV1 {
                    canonical_fact,record_ref:CanonicalInputRecordRefV1 {input_stream_id:header.input_stream_id.clone(),input_position:position as u64},
                    anchor:DirectObservationAnchorV1 {field_path:"chainlink_twap_thirty".into(),
                        position:PublicFactPositionV1 {input_stream_id:header.input_stream_id.clone(),closed_at_input_position:position as u64,fact_ordinal:0},
                        group_id:source_evidence.source_message_id.clone(),source_message_id:source_evidence.source_message_id.clone(),
                        source_event_at_utc_ms:source_evidence.source_event_at_utc_ms,received_at_utc_ms:source_evidence.received_at_utc_ms},
                });
            }
            let mut observed_input = input.clone();
            let prepared = prepare_reference(&core,&mut input,&reference_candidates);
            let applied = core.push_facts(&prepared).unwrap();
            // 有界真实构造链的独立旧公共观察入口；不回放历史全窗。
            let observed_prepared = prepare_reference(&observation_core,&mut observed_input,&reference_candidates);
            let observed = observation_core.push(&observed_prepared).unwrap();
            for group in &observed.applied.closed_groups {
                public_events.extend(group.records.iter().cloned());
                public_visible_times.push(data_pipeline_core::public_observation_core::public_group_visible_at_utc_ms(
                    prepared.recorded.ingress,&group.records));
                for observation in &group.observations {
                    source_observations.push(serde_json::json!({"prefix":observed_groups,"observation":observation}));
                    if position == 3 {
                        first_sides = serde_json::json!({"prefix":observed_groups,"assets":observation.assets});
                    }
                    if first_binance.is_null() && observation.binance_depth.value.is_some() {
                        first_binance = serde_json::json!({"prefix":observed_groups,"depth":observation.binance_depth});
                    }
                    if matches!(prepared.recorded.ingress,DptIngressV1::Message {row:TypedRawRowV2::Existing(
                        TypedRawRow::BinanceDepthSnapshot(_) | TypedRawRow::BinanceDepthUpdate(_)),..}) {
                        binance_observations.push(serde_json::json!({"prefix":observed_groups,"depth":observation.binance_depth}));
                    }
                    public_assets = observation.assets.clone();
                    public_market = serde_json::json!({"market":observation.market,"lifecycle":observation.lifecycle,
                        "active":observation.active,"closed":observation.closed,"accepting_orders":observation.accepting_orders,
                        "terminal":observation.terminal,"settlement_reference":observation.settlement_reference,
                        "temporary_reference":observation.temporary_reference,"effective_reference":observation.effective_reference,
                        "chainlink_spot":observation.chainlink_spot,"chainlink_twap_thirty":observation.chainlink_twap_thirty,
                        "chainlink_twap_sixty":observation.chainlink_twap_sixty,"binance_depth":observation.binance_depth,
                        "binance_trade":observation.binance_trade});
                    if observation.terminal.value.is_some()
                        || observation.terminal.quality == MarketDataQuality::Invalid {
                        terminal_observations.push(serde_json::json!({"prefix":observed_groups,
                            "terminal":observation.terminal}));
                    }
                }
                observed_groups += 1;
            }
            for (_, reduction) in &applied.pm_reductions {
                if original.is_none() {
                    let context = core.fixed_markets().next().unwrap();
                    original = Some(PmOwnedStateV1::new(core.prepared_pm_context(&context.condition_id.0).unwrap()));
                }
                original.as_mut().unwrap().apply(reduction);
                let zero = |price:&ExactDecimal| price.0 == "0.4";
                let has_evidence = reduction.steps.iter().any(|step|matches!(&step.change,
                    PmStepChangeV1::LevelEvidenceOnly{asset,price,..} if zero(price) && {
                        token = asset.token_id.0.clone(); true
                    }));
                let has_change = reduction.steps.iter().any(|step|matches!(&step.change,
                    PmStepChangeV1::LevelChange{transition,..} if zero(&transition.price_pusd_per_share)));
                zero_evidence_without_change |= has_evidence && !has_change;
            }
            let recorded = RecordedIngressV1 {input_stream_id:prepared.recorded.input_stream_id.clone(),
                input_position:prepared.recorded.input_position,ingress:prepared.recorded.ingress.clone(),
                dependency_record_refs:prepared.recorded.dependency_record_refs.clone()};
            writer.push_prepared(&recorded,&prepared,&applied).unwrap();
        }
        writer.publish().unwrap();
        assert!(zero_evidence_without_change);
        let resolved = terminal_observations.iter().find(|v|!v["terminal"]["value"].is_null()).unwrap();
        assert_eq!(resolved["terminal"]["value"]["winning_asset_id"],"20");
        assert_eq!(resolved["terminal"]["value"]["payout_vector"].as_array().unwrap().len(),2);
        assert_eq!(public_market["terminal"]["quality"],"invalid");
        assert!(public_market["terminal"]["value"].is_null());
        assert_eq!(public_market["temporary_reference"]["value"]["price_usd_per_btc"],"65000");
        assert_eq!(public_market["chainlink_twap_thirty"]["value"]["price_usd_per_btc"],"66000");
        let original = original.unwrap();
        let price = data_pipeline_core::parse_canonical_decimal(&ExactDecimal("0.4".into()),"zero_price").unwrap();
        assert!(original.selected_level(&token,BookSide::Bid,price).unwrap().present);
        let asset = original.asset_state(&token).unwrap();
        let reference = serde_json::json!({"token":token,"price":"0.4",
            "quantity":original.quantity(&token,BookSide::Bid,price).normalize().to_string(),
            "present":true,"initialized":asset.has_initial_snapshot,"ready":asset.book_ready,
            "public_assets":public_assets,"public_events":public_events,"public_visible_times":public_visible_times,
            "public_market":public_market,"terminal_observations":terminal_observations,
            "first_sides":first_sides,"first_binance":first_binance,"binance_observations":binance_observations,"source_observations":source_observations,"observation_ms":44000});
        let reference_path = temp.path().join("original-reference.json");
        serde_json::to_writer(File::create(&reference_path).unwrap(),&reference).unwrap();
        eprintln!("original_core_zero_reference={reference}");
        let status = std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
            .arg(std::env::var("B_GPU_STATE_VALIDATOR").unwrap())
            .args(["--binary",&std::env::var("B_GPU_COLUMNS_BINARY").unwrap()])
            .arg("--material").arg(&target).arg("--original-reference").arg(reference_path)
            .arg("--output").arg(temp.path().join("report.json")).status().unwrap();
        assert!(status.success());
        // 新选择引用属于持久结构边界；重算文件hash也不能把第一条引用改成第二条。
        let mut reader = PublicCompactReaderV1::open(&target,1<<20).unwrap();
        let changed = temp.path().join("bad-reference");
        let mut writer = PublicCompactWriterV1::create(&changed,header).unwrap();
        let mut damaged = false;
        while let Some(mut packet) = reader.next_packet().unwrap() {
            if !damaged {
                if let Some(selection) = packet.reference_preparation.as_mut().and_then(|p|p.selection.as_mut()) {
                    if let Some(record) = selection.selected_record_ref.as_mut() {
                        record.input_position += 1; damaged = true;
                    }
                }
            }
            writer.write_packet(packet).unwrap();
        }
        assert!(damaged);
        writer.publish().unwrap();
        let mut reader = PublicCompactReaderV1::open(&changed,1<<20).unwrap();
        let mut rejected = false;
        loop {
            match reader.next_packet() {Ok(Some(_))=>{},Ok(None)=>break,Err(_)=>{rejected=true;break;}}
        }
        assert!(rejected);
    }

    #[test]
    #[ignore = "只在已同步准确提交的AutoDL CUDA环境显式运行"]
    fn gpu_public_missing_book_keeps_metadata_identity_and_empty_coordinate_fault() {
        for metadata in [true, false] {
            let temp = tempfile::tempdir().unwrap();
            let target = temp.path().join("compact");
            let header = PublicInputStreamHeaderV1 {
                input_stream_id:"gpu-no-pm-real-construction".into(),canonical_rule_version:"rule-v1".into(),
                observation_version:"public-observation-v1".into(),ordering_version:"original-input-v1".into(),
                input_scope:"constructed-no-pm-boundary".into(),max_pending_ws_terminals:2,
                initial_state:PublicInitialStateV1::Empty,
            };
            let mut core = PublicObservationCoreV1::new(header.clone()).unwrap();
            let mut writer = PublicCompactWriterV1::create(&target,header.clone()).unwrap();
            let sources = if metadata {
                vec![TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata_snapshot_row())))]
            } else { vec![] };
            for (position, source) in sources.into_iter().map(Some).chain(std::iter::once(None)).enumerate() {
                let ingress = source.map_or(DptIngressV1::EndOfInput,|row|DptIngressV1::Message {
                    source_evidence:source_evidence_v3_from_typed_raw_v2(&row).unwrap(),row,
                });
                let input = RecordedIngressV1 {input_stream_id:header.input_stream_id.clone(),input_position:position as u64,
                    ingress,dependency_record_refs:vec![]};
                let ids = BTreeMap::new();
                let prepared = prepare_public_ingress_v1(&core,&input,&ids,&[]).unwrap();
                let applied = core.push_facts(&prepared).unwrap();
                writer.push(&input,&applied).unwrap();
            }
            writer.publish().unwrap();
            let mut reader = PublicCompactReaderV1::open(&target,1024*1024).unwrap();
            let columns = crate::public_gpu_columns::public_pm_device_columns_v1(&mut reader, |_|{}).unwrap();
            assert_eq!(columns.assets.len(),if metadata {2} else {0});
            assert_eq!(columns.original_pm_steps,0);
            assert!(columns.slots.is_empty());
            let status = std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
                .arg(std::env::var("B_GPU_STATE_VALIDATOR").unwrap())
                .args(["--binary",&std::env::var("B_GPU_COLUMNS_BINARY").unwrap()])
                .arg("--material").arg(&target).arg("--output").arg(temp.path().join("report.json"))
                .status().unwrap();
            assert!(status.success());
        }
    }

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
        // 身份引用损坏与成员摘要一致仍须被结构层拒绝，不重跑业务归约。
        let payload_path = target.join("packet_payload.bin");
        let original_payload = fs::read(&payload_path).unwrap();
        let offsets = fs::read(target.join("packet_offsets.u64le")).unwrap();
        let mut damaged_payload = Vec::new();
        let mut damaged_offsets = 0_u64.to_le_bytes().to_vec();
        let mut changed = false;
        for pair in offsets.chunks_exact(8).collect::<Vec<_>>().windows(2) {
            let start = u64::from_le_bytes(pair[0].try_into().unwrap()) as usize;
            let end = u64::from_le_bytes(pair[1].try_into().unwrap()) as usize;
            let decoded = zstd::bulk::decompress(&original_payload[start..end],1024*1024).unwrap();
            let mut value: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
            let mut replacement = None;
            if !changed {
                if let Some(step) = value["pm"].as_array_mut().unwrap().iter_mut()
                    .find_map(|pm| pm["steps"].as_array_mut().unwrap().first_mut()) {
                    step["derived_id"]["consumer_stream_id"] = "compact-boundarx".into();
                    replacement = Some(zstd::bulk::compress(&serde_json::to_vec(&value).unwrap(),3).unwrap());
                    changed = true;
                }
            }
            damaged_payload.extend(replacement.as_deref().unwrap_or(&original_payload[start..end]));
            damaged_offsets.extend((damaged_payload.len() as u64).to_le_bytes());
        }
        assert!(changed);
        fs::write(&payload_path, &damaged_payload).unwrap();
        fs::write(target.join("packet_offsets.u64le"), &damaged_offsets).unwrap();
        let manifest_path = target.join("manifest.json");
        let original_manifest = fs::read(&manifest_path).unwrap();
        let mut manifest: PublicCompactManifestV1 = serde_json::from_slice(&original_manifest).unwrap();
        manifest.files[0].bytes = damaged_payload.len() as u64;
        manifest.files[0].sha256 = file_sha256(&payload_path).unwrap();
        manifest.files[1].sha256 = file_sha256(&target.join("packet_offsets.u64le")).unwrap();
        serde_json::to_writer(File::create(&manifest_path).unwrap(), &manifest).unwrap();
        let mut damaged = PublicCompactReaderV1::open(&target,1024*1024).unwrap();
        let mut rejected = false;
        loop {
            match damaged.next_packet() { Ok(Some(_))=>{}, Ok(None)=>break, Err(_)=>{rejected=true;break;} }
        }
        assert!(rejected);
        fs::write(target.join("packet_offsets.u64le"), offsets).unwrap();
        fs::write(&payload_path, original_payload).unwrap();
        fs::write(&manifest_path, original_manifest).unwrap();
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
    #[test]
    #[ignore = "只在已同步准确提交的AutoDL CUDA环境显式运行"]
    fn gpu_original_source_keys_reach_compact_fact_sink() {
        use crate::{canonical_run::CanonicalRunConfig, gpu_initial_sort::GpuInitialRunOrderV1,
            public_replay::{prepare_historical_rows_with_initial_order_v1,HISTORICAL_SOURCE_HEAD_ORDERING_VERSION}, SortConfig};
        use std::sync::Arc;
        let python = std::env::var("B_GPU_PYTHON").unwrap();
        let script = std::env::var("B_GPU_SORT_WORKER").unwrap();
        let order = Arc::new(GpuInitialRunOrderV1::start(Path::new(&python),Path::new(&script)).unwrap());
        let temp = tempfile::tempdir().unwrap();
        let sources = vec![
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row("epoch-a",1,21_000,22_000,book("12.000000000000000000000000001")))),
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(update("depth-a",101,101,2,Some(100),None,
                BinanceDepthStitchVerdict::Applied,vec![DepthLevel {price:"63900".into(),quantity:"1.234567890123456789".into()}]))),
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata_snapshot_row()))),
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot("depth-a",100,1))),
        ];
        let config = CanonicalRunConfig {
            decode_batch_rows:4096,max_decode_batch_bytes:64*1024*1024,max_temporary_row_bytes:64*1024*1024,
            max_epoch_count_per_source:1024,max_pending_ws_terminals:1024,max_retained_reducer_entries:65536,
            canonical_rule_version:"rule-v1".into(),
            sort:SortConfig {max_run_rows:1024,max_run_bytes:64*1024*1024,max_temp_run_files:64,
                merge_fan_in:2,merge_workers:8,max_open_files:24,batch_rows:1024,max_temp_bytes:256*1024*1024},
        };
        let header = PublicInputStreamHeaderV1 {
            input_stream_id:"gpu-compact".into(),canonical_rule_version:"rule-v1".into(),
            observation_version:"public-observation-v1".into(),ordering_version:HISTORICAL_SOURCE_HEAD_ORDERING_VERSION.into(),
            input_scope:"engineering-constructed-short".into(),max_pending_ws_terminals:1024,initial_state:PublicInitialStateV1::Empty,
        };
        let mut merge = prepare_historical_rows_with_initial_order_v1(&temp.path().join("merge"),&config,
            header.input_stream_id.clone(),||Ok(sources.clone().into_iter().map(Ok)),order).unwrap();
        let target = temp.path().join("compact");
        let mut writer = PublicCompactWriterV1::create(&target,header).unwrap();
        let core = consume_historical_merge_facts_v1(&mut merge,writer.manifest.header.clone(),&mut writer,|_|Ok(())).unwrap();
        assert_eq!(core.applied_prefix().next_input_position,5);
        let receipt = writer.publish().unwrap();
        assert!(receipt.normal_end && receipt.facts>0);
        let mut reader = PublicCompactReaderV1::open(&target,64*1024*1024).unwrap();
        let mut source_positions = vec![];
        let mut depth_refs = 0;
        while let Some(packet) = reader.next_packet().unwrap() {
            source_positions.push(packet.input.input_position);
            depth_refs += packet.groups.iter().flat_map(|g|&g.records)
                .filter(|f|matches!(f.payload,CompactFactPayloadV1::BinanceDepthFromPrefix{..})).count();
        }
        assert_eq!(source_positions,vec![0,1,2,3,4]);
        assert_eq!(depth_refs,2);
        eprintln!("gpu_compact_receipt={}",serde_json::to_string(&receipt).unwrap());
    }

}
