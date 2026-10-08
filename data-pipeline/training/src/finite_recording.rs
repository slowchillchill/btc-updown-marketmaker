//! 原有限准备和唯一公共录制循环；不生成隐藏材料或账户描述。
use crate::{
    canonical_run::CanonicalRunConfig,
    gate_a::GateAOutputConfig,
    historical_preparation_progress::{
        HistoricalPreparationNoticeV1 as Notice, HistoricalPreparationNotifierV1,
        HistoricalPreparationStageV1 as Stage, notify,
    },
    public_input::PublicFiniteFileV1,
    public_recording::*,
    public_replay::*,
};
use contracts::*;
use data_pipeline_core::{
    public_input::*,
    public_observation_core::{
        PublicAppliedIngressV1, PublicObservationCoreV1, PublicRecordingAppliedIngressV1,
    },
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FinitePublicRecordingConfigV1 {
    pub files: Vec<PublicFiniteFileV1>,
    pub received_start_utc_ms: i64,
    pub received_end_utc_ms: i64,
    pub work_root: PathBuf,
    pub header: PublicInputStreamHeaderV1,
    pub canonical: CanonicalRunConfig,
    pub recording: GateAOutputConfig,
}
/// 完整观察消费者继续使用原结果；录制线程只接收已转为 owned 的材料。
pub fn record_historical_merge_v1(
    merge: &mut HistoricalMergeV1,
    header: PublicInputStreamHeaderV1,
    public_root: &Path,
    recording: GateAOutputConfig,
    mut applied_callback: impl FnMut(
        &RecordedIngressV1,
        Option<&PublicAppliedIngressV1<'_>>,
        Option<&SourceArtifactBindingV1>,
    ),
    before_stabilize: impl FnMut(),
) -> RecordingResult<(PublicObservationCoreV1, Option<CommittedIngressPrefixV1>)> {
    record_merge(
        merge,
        header,
        public_root,
        recording,
        false,
        |input, applied, binding| {
            applied_callback(
                input,
                applied.map(|a| match a {
                    RecordingApplication::Full(full) => full,
                    RecordingApplication::Counts(_) => unreachable!("完整观察入口"),
                }),
                binding,
            )
        },
        before_stabilize,
    )
}

/// 无观察消费者的历史录制入口：保留真实数量与全部归约，不构造完整观察。
pub fn record_historical_merge_counts_v1(
    merge: &mut HistoricalMergeV1,
    header: PublicInputStreamHeaderV1,
    public_root: &Path,
    recording: GateAOutputConfig,
    mut applied_callback: impl FnMut(
        &RecordedIngressV1,
        Option<&PublicRecordingAppliedIngressV1<'_>>,
        Option<&SourceArtifactBindingV1>,
    ),
    before_stabilize: impl FnMut(),
) -> RecordingResult<(PublicObservationCoreV1, Option<CommittedIngressPrefixV1>)> {
    record_merge(
        merge,
        header,
        public_root,
        recording,
        true,
        |input, applied, binding| {
            applied_callback(
                input,
                applied.map(|a| match a {
                    RecordingApplication::Counts(counts) => counts,
                    RecordingApplication::Full(_) => unreachable!("观察计数入口"),
                }),
                binding,
            )
        },
        before_stabilize,
    )
}

enum RecordingApplication {
    Full(PublicAppliedIngressV1<'static>),
    Counts(PublicRecordingAppliedIngressV1<'static>),
}
struct RecordingCommand {
    // None 是按序结束指令；通道断开仍是异常结束。
    record: Option<(
        RecordedIngressV1,
        RecordingApplication,
        Option<SourceArtifactBindingV1>,
    )>,
}

// 单个 writer 与单个 Core；零容量 FIFO 在交接后允许 writer 保存上一条、Core
// 推进下一条。绑定随同一条输入交接；通道关闭不代表正常 End，也不发布材料。
fn record_merge(
    merge: &mut HistoricalMergeV1,
    header: PublicInputStreamHeaderV1,
    public_root: &Path,
    recording: GateAOutputConfig,
    counts_only: bool,
    mut applied_callback: impl FnMut(
        &RecordedIngressV1,
        Option<&RecordingApplication>,
        Option<&SourceArtifactBindingV1>,
    ),
    mut before_stabilize: impl FnMut(),
) -> RecordingResult<(PublicObservationCoreV1, Option<CommittedIngressPrefixV1>)> {
    let mut core = PublicObservationCoreV1::new(header.clone())?;
    let mut writer = PublicRecordingWriterV1::create_historical(public_root, &header, recording)?;
    let (sender, receiver) = std::sync::mpsc::sync_channel::<RecordingCommand>(0);
    let worker = std::thread::Builder::new()
        .name("historical-public-recording".into())
        .spawn(
            move || -> RecordingResult<Option<CommittedIngressPrefixV1>> {
                while let Ok(command) = receiver.recv() {
                    match command.record {
                        Some((input, applied, binding)) => {
                            if let Some(binding) = binding {
                                writer.record_source_binding(binding)?;
                            }
                            match applied {
                                RecordingApplication::Full(applied) => {
                                    writer.record(input, applied)?
                                }
                                RecordingApplication::Counts(applied) => {
                                    writer.record_counts(input, applied)?
                                }
                            }
                        }
                        None => return writer.stabilize(),
                    }
                }
                Ok(None)
            },
        )?;
    let apply_result = (|| -> RecordingResult<()> {
        while let Some(mut input) = merge.next_input()? {
            let binding = merge.initialization_binding().cloned();
            if let Some(binding) = &binding {
                applied_callback(&input, None, Some(binding));
            }
            let prepared = merge.prepare_input(&core, &mut input)?;
            let applied = if counts_only {
                RecordingApplication::Counts(core.push_recording(&prepared)?.into_owned())
            } else {
                RecordingApplication::Full(core.push(&prepared)?.into_owned())
            };
            applied_callback(&input, Some(&applied), None);
            sender.send(RecordingCommand {
                record: Some((input, applied, binding)),
            })?;
        }
        let mut end = RecordedIngressV1 {
            input_stream_id: header.input_stream_id,
            input_position: core.applied_prefix().next_input_position,
            ingress: DptIngressV1::EndOfInput,
            dependency_record_refs: vec![],
        };
        let prepared = merge.prepare_input(&core, &mut end)?;
        let applied = if counts_only {
            RecordingApplication::Counts(core.push_recording(&prepared)?.into_owned())
        } else {
            RecordingApplication::Full(core.push(&prepared)?.into_owned())
        };
        applied_callback(&end, Some(&applied), None);
        sender.send(RecordingCommand {
            record: Some((end, applied, None)),
        })?;
        before_stabilize();
        sender.send(RecordingCommand { record: None })?;
        Ok(())
    })();
    drop(sender);
    // 后台原错误优先于通道断开的表象；任何主线程错误都不发送结束指令。
    let prefix = worker
        .join()
        .map_err(|_| std::io::Error::other("历史公共录制线程 panic"))??;
    apply_result?;
    Ok((core, prefix))
}
pub fn prepare_finite_public_recording_v1(
    config: &FinitePublicRecordingConfigV1,
    progress: Option<&HistoricalPreparationNotifierV1<'_>>,
    applied_callback: impl FnMut(
        &RecordedIngressV1,
        Option<&data_pipeline_core::public_observation_core::PublicAppliedIngressV1<'_>>,
        Option<&SourceArtifactBindingV1>,
    ),
) -> RecordingResult<Option<CommittedIngressPrefixV1>> {
    std::fs::create_dir(&config.work_root)?;
    let mut merge = prepare_historical_finite_files_v1(
        &config.work_root.join("merge"),
        &config.files,
        &config.canonical,
        config.header.input_stream_id.clone(),
        config.received_start_utc_ms..config.received_end_utc_ms,
        progress,
    )?;
    let mut header = config.header.clone();
    header.ordering_version = HISTORICAL_SOURCE_HEAD_ORDERING_VERSION.into();
    notify(
        progress,
        Notice::Stage(Stage::PublicApplicationStatisticsRecording),
    );
    let (_, prefix) = record_historical_merge_v1(
        &mut merge,
        header,
        &config.work_root.join("public"),
        config.recording.clone(),
        applied_callback,
        || notify(progress, Notice::Stage(Stage::PublicStabilizeAndCatalog)),
    )?;
    Ok(prefix)
}
