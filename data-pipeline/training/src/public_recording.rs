//! 唯一后台记录流。应用调用不持有写入器；稳定由所属运行显式请求。
use crate::{
    RunCursor,
    gate_a::{GateAOutputConfig, SequentialCaptureWriter},
    gate_a_publication::{commit_public_segment, sync_tree},
};
use contracts::*;
use data_pipeline_core::{
    pm_reduction::*, public_input::*, public_observation_core::PublicAppliedIngressV1,
    public_reduction::*,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub type RecordingResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// 单一文件流内的不同记录；组和观测只保存原位置，不重复规范载荷。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PublicStoredRowV1 {
    Input(RecordedIngressV1),
    Canonical {
        canonical_position: u64,
        binding: PublicFactBindingV1,
        record: CanonicalEventRecordV2,
    },
    Report {
        input_position: u64,
        report: PublicReductionReportV1<CanonicalEventId>,
    },
    PmReport {
        input_position: u64,
        canonical_position: u64,
        report_ordinal: u32,
        report: PmReductionReportV2<SourceEvidenceRefV3>,
    },
    PmStepBinding(PmDerivedStepBindingV1),
    Group(PublicGroupIndexV1),
    Observation(PublicObservationIndexV1),
    SourceBinding(SourceArtifactBindingV1),
}

pub struct PublicRecordingWriterV1 {
    root: PathBuf,
    config: GateAOutputConfig,
    total_bytes: Arc<Mutex<u64>>,
    writer: Option<SequentialCaptureWriter>,
    segment: u64,
    segment_rows: u64,
    pending: CommittedIngressPrefixV1,
    committed: Option<CommittedIngressPrefixV1>,
    failed: bool,
}
impl PublicRecordingWriterV1 {
    /// 容量来自调用方当前运行配置，不在此选择训练/在线策略或稳定周期。
    pub fn create(
        root: &Path,
        header: &PublicInputStreamHeaderV1,
        config: GateAOutputConfig,
    ) -> RecordingResult<Self> {
        fs::create_dir(root)?;
        let header_path = root.join("stream.json");
        serde_json::to_writer(File::create(&header_path)?, header)?;
        File::open(&header_path)?.sync_all()?;
        File::open(root)?.sync_all()?;
        Ok(Self {
            root: root.into(),
            config,
            total_bytes: Arc::new(Mutex::new(0)),
            writer: None,
            segment: 0,
            segment_rows: 0,
            committed: None,
            failed: false,
            pending: CommittedIngressPrefixV1 {
                input_stream_id: header.input_stream_id.clone(),
                next_input_position: 0,
                next_canonical_position: 0,
                next_group_position: 0,
                next_observation_position: 0,
                next_public_index_position: 0,
                stable_storage_ref: String::new(),
                normal_end: false,
            },
        })
    }
    pub fn committed_prefix(&self) -> Option<&CommittedIngressPrefixV1> {
        self.committed.as_ref()
    }
    fn write(&mut self, row: PublicStoredRowV1) -> RecordingResult<()> {
        if self.writer.is_none() {
            let mut writer = SequentialCaptureWriter::create(
                "public_recording_v1",
                &self.root.join(format!("pending-{:020}", self.segment)),
                self.total_bytes.clone(),
                &self.config,
            )?;
            writer.defer_batch_sync();
            self.writer = Some(writer);
        }
        self.writer.as_mut().unwrap().push(
            self.segment_rows,
            self.segment_rows.to_be_bytes().to_vec(),
            &serde_json::to_vec(&row)?,
        )?;
        self.segment_rows += 1;
        Ok(())
    }
    /// 后台消费同次应用材料；此调用可能写盘，必须在业务调用之外。
    pub fn record(
        &mut self,
        input: RecordedIngressV1,
        result: PublicAppliedIngressV1<'static>,
    ) -> RecordingResult<()> {
        if self.failed {
            return Err(io::Error::other("记录尾部已失败，不能继续签发").into());
        }
        self.failed = true;
        let outcome = self.record_inner(input, result);
        if outcome.is_ok() {
            self.failed = false;
        }
        outcome
    }
    fn record_inner(
        &mut self,
        input: RecordedIngressV1,
        result: PublicAppliedIngressV1<'static>,
    ) -> RecordingResult<()> {
        let input_position = input.input_position;
        let normal_end = matches!(input.ingress, DptIngressV1::EndOfInput);
        self.write(PublicStoredRowV1::Input(input))?;
        #[cfg(test)]
        faults::at("after_input")?;
        let mut materials = result.reductions.into_iter();
        // 非 PM 的批次可能仅含报告，故先全部后台物化；每批只保存一次规范记录。
        let mut batches = std::collections::BTreeMap::new();
        for batch in materials.by_ref() {
            let material = materialize_public_reduction_v1(batch)?;
            for report in material.reports {
                self.write(PublicStoredRowV1::Report {
                    input_position,
                    report,
                })?;
            }
            for (record, binding) in material.records.into_iter().zip(material.bindings) {
                batches.insert(binding.position.clone(), (binding, record));
            }
        }
        for (position, reduction) in result.pm_reductions {
            let canonical_position = reduction
                .steps
                .first()
                .map(|s| s.derived_id.canonical_record_position);
            let material = materialize_pm_step_identity_v1(reduction)?;
            for binding in material.bindings {
                self.write(PublicStoredRowV1::PmStepBinding(binding))?;
            }
            // 即便消息只有无输出报告，规范位置仍从当组原消息位置确定。
            let canonical_position = canonical_position.unwrap_or_else(|| {
                self.pending.next_canonical_position
                    + result
                        .applied
                        .closed_groups
                        .iter()
                        .flat_map(|g| &g.records)
                        .position(|f| f.position == position)
                        .unwrap() as u64
            });
            for (report_ordinal, report) in material.reduction.reports.into_iter().enumerate() {
                // 原 Prepared 证据的序列化恰为实际 V3 证据；准备缓存不进入持久报告。
                let report = serde_json::from_value(serde_json::to_value(report)?)?;
                self.write(PublicStoredRowV1::PmReport {
                    input_position,
                    canonical_position,
                    report_ordinal: report_ordinal as u32,
                    report,
                })?;
            }
        }
        for group in result.applied.closed_groups {
            let refs = group.records.iter().map(|f| f.position.clone()).collect();
            for fact in group.records {
                let (binding, record) = match fact.payload {
                    PublicBusinessPayloadV1::PolymarketMessage(message) => (
                        PublicFactBindingV1 {
                            position: fact.position,
                            event_id: message.event_id.clone(),
                        },
                        CanonicalEventRecordV2::PolymarketMessage(message),
                    ),
                    _ => batches
                        .remove(&fact.position)
                        .expect("同次归约材料对应当组事实"),
                };
                self.write(PublicStoredRowV1::Canonical {
                    canonical_position: self.pending.next_canonical_position,
                    binding,
                    record,
                })?;
                self.pending.next_canonical_position += 1;
            }
            let mut observation_positions = Vec::new();
            for observation in group.observations {
                let index = PublicObservationIndexV1 {
                    observation_position: self.pending.next_observation_position,
                    market: observation.market,
                    group_position: observation.group_position,
                    visible_prefix: observation.visible_prefix,
                    group_events: observation
                        .group_events
                        .into_iter()
                        .map(|f| f.position)
                        .collect(),
                };
                observation_positions.push(index.observation_position);
                self.write(PublicStoredRowV1::Observation(index))?;
                self.pending.next_observation_position += 1;
            }
            self.write(PublicStoredRowV1::Group(PublicGroupIndexV1 {
                group_id: group.group_id,
                group_position: group.group_position,
                member_input_positions: group.member_input_positions,
                closed_at_input_position: group.closed_at_input_position,
                records: refs,
                observation_positions,
            }))?;
            self.pending.next_group_position += 1;
            self.pending.next_public_index_position += 1;
        }
        self.pending.next_input_position = result.applied.applied_prefix.next_input_position;
        self.pending.normal_end = normal_end;
        Ok(())
    }
    pub fn record_source_binding(
        &mut self,
        binding: SourceArtifactBindingV1,
    ) -> RecordingResult<()> {
        if self.failed {
            return Err(io::Error::other("记录尾部已失败").into());
        }
        self.failed = true;
        self.write(PublicStoredRowV1::SourceBinding(binding))?;
        self.failed = false;
        Ok(())
    }
    /// 异常终止保留于未完成尾部，不提升已应用或耐久前缀。
    pub fn record_aborted(&mut self, input: RecordedIngressV1) -> RecordingResult<()> {
        self.failed = true;
        self.write(PublicStoredRowV1::Input(input))
    }
    /// 合法检查点/市场边界/正常结束由所属运行显式调用；空闲不会进入此入口。
    pub fn stabilize(&mut self) -> RecordingResult<Option<CommittedIngressPrefixV1>> {
        if self.failed {
            return Err(io::Error::other("记录尾部未完成，不能发布稳定前缀").into());
        }
        let Some(writer) = self.writer.take() else {
            return Ok(self.committed.clone());
        };
        self.failed = true;
        writer.close_segment()?;
        let stage = self.root.join(format!("pending-{:020}", self.segment));
        let name = format!("stable-{:020}", self.segment);
        self.pending.stable_storage_ref = name.clone();
        serde_json::to_writer(File::create(stage.join("prefix.json"))?, &self.pending)?;
        sync_tree(&stage)?;
        #[cfg(test)]
        faults::at("before_publish")?;
        commit_public_segment(stage, &name)?;
        self.committed = Some(self.pending.clone());
        self.failed = false;
        self.segment += 1;
        self.segment_rows = 0;
        Ok(self.committed.clone())
    }
}

/// 按显式耐久凭据读取；冷恢复只在原写入方已经停止后扫描目录。
pub struct PublicRecordingReaderV1 {
    pub header: PublicInputStreamHeaderV1,
    pub prefix: Option<CommittedIngressPrefixV1>,
    root: PathBuf,
    segments: std::vec::IntoIter<PathBuf>,
    cursor: Option<RunCursor>,
    batch_rows: usize,
    max_batch_bytes: usize,
}
impl PublicRecordingReaderV1 {
    /// 冷恢复入口；调用方先结束原写入方，不能用于并发发现尚未返回的发布。
    pub fn recover(
        root: &Path,
        batch_rows: usize,
        max_batch_bytes: usize,
    ) -> RecordingResult<Self> {
        let header = serde_json::from_reader(File::open(root.join("stream.json"))?)?;
        let mut segments: Vec<_> = fs::read_dir(root)?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("stable-")
            })
            .collect();
        segments.sort();
        let prefix = segments
            .last()
            .map(|p| -> RecordingResult<_> {
                Ok(serde_json::from_reader(File::open(p.join("prefix.json"))?)?)
            })
            .transpose()?;
        Ok(Self {
            header,
            prefix,
            root: root.into(),
            segments: segments.into_iter(),
            cursor: None,
            batch_rows,
            max_batch_bytes,
        })
    }
    /// 持续读取只使用写入方已经返回的凭据；重命名但尚未确认的更晚目录不进入视图。
    pub fn open_committed(
        root: &Path,
        prefix: Option<CommittedIngressPrefixV1>,
        batch_rows: usize,
        max_batch_bytes: usize,
    ) -> RecordingResult<Self> {
        let header = serde_json::from_reader(File::open(root.join("stream.json"))?)?;
        let mut segments = Vec::new();
        if let Some(bound) = &prefix {
            for entry in fs::read_dir(root)? {
                let entry = entry?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("stable-") && name.as_ref() <= bound.stable_storage_ref.as_str()
                {
                    segments.push(entry.path());
                }
            }
        }
        segments.sort();
        Ok(Self {
            header,
            prefix,
            root: root.into(),
            segments: segments.into_iter(),
            cursor: None,
            batch_rows,
            max_batch_bytes,
        })
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn next_row(&mut self) -> RecordingResult<Option<PublicStoredRowV1>> {
        loop {
            if let Some(cursor) = &mut self.cursor {
                if let Some(row) = cursor.next_row()? {
                    return Ok(Some(serde_json::from_slice(row.payload())?));
                }
            }
            let Some(path) = self.segments.next() else {
                return Ok(None);
            };
            self.cursor = Some(RunCursor::open(
                &path.join("captured.parquet"),
                self.batch_rows,
                self.max_batch_bytes,
            )?);
        }
    }
    pub fn next_page(&mut self, rows: usize) -> RecordingResult<Vec<PublicStoredRowV1>> {
        let mut output = Vec::new();
        for _ in 0..rows {
            match self.next_row()? {
                Some(row) => output.push(row),
                None => break,
            }
        }
        Ok(output)
    }
}

/// 复用原离线准入已经确认的文件身份，不重算在线语义身份。
pub fn source_artifact_binding_v1(
    input: &crate::ValidatedDirectSourceDailyInputV3,
    file: &crate::ValidatedDirectSourceParquetFileV3,
    source_message_id: Sha256Hex,
) -> SourceArtifactBindingV1 {
    SourceArtifactBindingV1 {
        source_message_id,
        manifest_sha256: Sha256Hex(input.manifest_sha256().into()),
        file_sha256: Sha256Hex(file.sha256().into()),
        relative_path: file.manifest_relative_path().to_string_lossy().into_owned(),
    }
}
#[cfg(test)]
pub(crate) mod faults {
    thread_local! { static POINT: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) }; }
    pub fn set(point: &'static str) {
        POINT.set(Some(point));
    }
    pub fn at(point: &str) -> std::io::Result<()> {
        if POINT.get() == Some(point) {
            POINT.set(None);
            return Err(std::io::Error::other("注入记录发布故障"));
        }
        Ok(())
    }
}
#[cfg(test)]
#[path = "public_recording_tests.rs"]
mod tests;
