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
use data_pipeline_core::{public_input::*, public_observation_core::PublicObservationCoreV1};
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
/// 两个消费者共用的原有限循环；回调仅借用当次结果及初始化绑定。
/// 仍在原writer稳定成功之后交付core及稳定前缀；原错误立即传播。
pub fn record_historical_merge_v1(
    merge: &mut HistoricalMergeV1,
    header: PublicInputStreamHeaderV1,
    public_root: &Path,
    recording: GateAOutputConfig,
    mut applied_callback: impl FnMut(
        &RecordedIngressV1,
        Option<&data_pipeline_core::public_observation_core::PublicAppliedIngressV1<'_>>,
        Option<&SourceArtifactBindingV1>,
    ),
    mut before_stabilize: impl FnMut(),
) -> RecordingResult<(PublicObservationCoreV1, Option<CommittedIngressPrefixV1>)> {
    let mut core = PublicObservationCoreV1::new(header.clone())?;
    let mut writer = PublicRecordingWriterV1::create_historical(public_root, &header, recording)?;
    while let Some(mut input) = merge.next_input()? {
        let binding = merge.initialization_binding();
        if let Some(binding) = binding {
            applied_callback(&input, None, Some(binding));
            writer.record_source_binding(binding.clone())?;
        }
        let prepared = merge.prepare_input(&core, &mut input)?;
        let applied = core.push(&prepared)?.into_owned();
        applied_callback(&input, Some(&applied), None);
        writer.record(input, applied)?;
    }
    let mut end = RecordedIngressV1 {
        input_stream_id: header.input_stream_id,
        input_position: core.applied_prefix().next_input_position,
        ingress: DptIngressV1::EndOfInput,
        dependency_record_refs: vec![],
    };
    let prepared = merge.prepare_input(&core, &mut end)?;
    let applied = core.push(&prepared)?.into_owned();
    applied_callback(&end, Some(&applied), None);
    writer.record(end, applied)?;
    before_stabilize();
    let prefix = writer.stabilize()?;
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
