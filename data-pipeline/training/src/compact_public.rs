//! 公共完整事实的紧凑冷材料。输入/组/PM步骤各保存一次，不写模型观察或逐事件全盘口。
//! 当前wire侧车复用原强类型JSON编码；固定列供批量GPU转换，hidden由独立排序归约产生。
use std::{
    fs::{self, File},
    io::{self, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
use contracts::*;
use data_pipeline_core::{
    pm_reduction::{PmReductionReportV2, PmReductionStepV1},
    public_observation_core::{PublicFactAppliedIngressV1, PublicFactGroupV1, PublicObservationCoreV1},
    public_reduction::PublicReductionReportV1,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use crate::public_recording::RecordingResult;
use crate::public_replay::HistoricalMergeV1;

const SCHEMA: &str = "public_compact_facts_v1";

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
    pub normal_end: bool,
    pub files: Vec<CompactFileV1>,
}
#[derive(Serialize)]
struct PacketRef<'s, 'm> {
    input: &'s RecordedIngressV1,
    groups: &'s [PublicFactGroupV1],
    pm: Vec<PmPacketRef<'s, 'm>>,
    reports: Vec<&'s PublicReductionReportV1>,
}
#[derive(Serialize)]
struct PmPacketRef<'s, 'm> {
    position: &'s PublicFactPositionV1,
    steps: &'s [PmReductionStepV1<'m, data_pipeline_core::public_reduction::PreparedPublicEvidenceV1, PmDerivedStepPositionV1>],
    reports: &'s [PmReductionReportV2<data_pipeline_core::public_reduction::PreparedPublicEvidenceV1, PmDerivedStepPositionV1>],
}
/// Prepared证据的wire形状就是实际SourceEvidenceRefV3，不保存其编码缓存。
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactPmPacketV1 {
    pub position: PublicFactPositionV1,
    pub steps: Vec<PmReductionStepV1<'static, SourceEvidenceRefV3, PmDerivedStepPositionV1>>,
    pub reports: Vec<PmReductionReportV2<SourceEvidenceRefV3, PmDerivedStepPositionV1>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicCompactPacketV1 {
    pub input: RecordedIngressV1,
    pub groups: Vec<PublicFactGroupV1>,
    pub pm: Vec<CompactPmPacketV1>,
    pub reports: Vec<PublicReductionReportV1>,
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
                }, header, inputs: 0, groups: 0, facts: 0, normal_end: false, files: vec![],
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
        let packet = PacketRef {
            input, groups: &applied.groups,
            pm: applied.pm_reductions.iter().map(|(position, r)| PmPacketRef {
                position, steps: &r.steps, reports: &r.reports,
            }).collect(),
            reports: applied.reductions.iter().flat_map(|r| &r.reports).collect(),
        };
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
        writer.push(&input, &applied)?;
        progress(input.input_position + 1)?;
    }
    let mut end = RecordedIngressV1 {
        input_stream_id: header.input_stream_id,
        input_position: core.applied_prefix().next_input_position,
        ingress: DptIngressV1::EndOfInput, dependency_record_refs: vec![],
    };
    let prepared = merge.prepare_input(&core, &mut end)?;
    let applied = core.push_facts(&prepared)?;
    writer.push(&end, &applied)?;
    progress(end.input_position + 1)?;
    Ok(core)
}

pub struct PublicCompactReaderV1 {
    pub manifest: PublicCompactManifestV1,
    payload: File,
    offsets: File,
    next: u64,
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
            let mut file = File::open(root.join(name))?;
            if file.metadata()?.len() != item.bytes { return Err(io::Error::other("材料大小错误").into()); }
            if file_sha256(&root.join(name))? != item.sha256 { return Err(io::Error::other("材料SHA错误").into()); }
        }
        if manifest.files[1].bytes != manifest.inputs.checked_add(1).and_then(|n| n.checked_mul(8)).unwrap_or(0)
            || manifest.files[2].bytes != manifest.groups.checked_mul(40).unwrap_or(0) {
            return Err(io::Error::other("材料固定列长度错误").into());
        }
        Ok(Self { payload: File::open(root.join(expected[0]))?, offsets: File::open(root.join(expected[1]))?,
                  manifest, next: 0, max_packet_bytes })
    }
    pub fn next_packet(&mut self) -> RecordingResult<Option<PublicCompactPacketV1>> {
        if self.next == self.manifest.inputs { return Ok(None); }
        let mut value = [0_u8; 8];
        self.offsets.seek(SeekFrom::Start(self.next * 8))?;
        self.offsets.read_exact(&mut value)?;
        let start = u64::from_le_bytes(value);
        self.offsets.read_exact(&mut value)?;
        let end = u64::from_le_bytes(value);
        if start > end || end > self.manifest.files[0].bytes || end - start > self.max_packet_bytes as u64 {
            return Err(io::Error::other("材料payload偏移或本次资源容量错误").into());
        }
        let mut bytes = vec![0; (end - start) as usize];
        self.payload.seek(SeekFrom::Start(start))?;
        self.payload.read_exact(&mut bytes)?;
        let packet: PublicCompactPacketV1 = serde_json::from_slice(&bytes)?;
        if packet.input.input_stream_id != self.manifest.header.input_stream_id || packet.input.input_position != self.next {
            return Err(io::Error::other("材料packet输入位置错误").into());
        }
        if self.next + 1 == self.manifest.inputs && !matches!(packet.input.ingress, DptIngressV1::EndOfInput) {
            return Err(io::Error::other("材料末端没有正常结束输入").into());
        }
        self.next += 1;
        Ok(Some(packet))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use data_pipeline_core::public_input::prepare_public_ingress_v1;

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
        let mut core = PublicObservationCoreV1::new(header.clone()).unwrap();
        let input = RecordedIngressV1 {
            input_stream_id: header.input_stream_id.clone(), input_position: 0,
            ingress: DptIngressV1::EndOfInput, dependency_record_refs: vec![],
        };
        let ids = BTreeMap::new();
        let prepared = prepare_public_ingress_v1(&core, &input, &ids, &[]).unwrap();
        let applied = core.push_facts(&prepared).unwrap();
        let mut writer = PublicCompactWriterV1::create(&target, header).unwrap();
        writer.push(&input, &applied).unwrap();
        assert!(!target.exists());
        let receipt = writer.publish().unwrap();
        assert!(receipt.normal_end);
        assert_eq!(receipt.applied_prefix, applied.applied_prefix);
        let mut reader = PublicCompactReaderV1::open(&target, 1024 * 1024).unwrap();
        let packet = reader.next_packet().unwrap().unwrap();
        assert_eq!(serde_json::to_value(packet.input).unwrap(), serde_json::to_value(input).unwrap());
        assert!(packet.groups.is_empty());
        assert!(reader.next_packet().unwrap().is_none());
        // 代表真实持久边界：既存payload改变不能借manifest自称原材料。
        let mut payload = fs::read(target.join("packet_payload.bin")).unwrap();
        payload[0] ^= 1;
        fs::write(target.join("packet_payload.bin"), payload).unwrap();
        assert!(PublicCompactReaderV1::open(&target, 1024 * 1024).is_err());
    }
}
