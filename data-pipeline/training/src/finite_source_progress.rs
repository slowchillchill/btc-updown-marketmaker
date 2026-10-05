//! 有限研究消费：原文件解码/两遍代际/外排/输入准备，紧凑归约不写完整录制。
use crate::{
    canonical_run::CanonicalRunConfig,
    historical_initialization::{
        MetadataInitializationConfigV1, MetadataInitializationSelectionV1,
    },
    historical_preparation_progress::{
        HistoricalPreparationNoticeV1 as Notice, HistoricalPreparationNotifierV1,
        HistoricalPreparationStageV1 as Stage, notify,
    },
    public_input::PublicFiniteFileV1,
    public_recording::RecordingResult,
    public_replay::{
        HISTORICAL_SOURCE_HEAD_ORDERING_VERSION, HistoricalMergeV1,
        prepare_historical_finite_files_with_initialization_v1,
    },
};
use contracts::*;
use data_pipeline_core::{public_input::*, public_observation_core::*};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FiniteSourceProgressConfigV1 {
    pub files: Vec<PublicFiniteFileV1>,
    pub received_start_utc_ms: i64,
    pub received_end_utc_ms: i64,
    pub work_root: PathBuf,
    pub header: PublicInputStreamHeaderV1,
    pub canonical: CanonicalRunConfig,
    pub metadata_initialization: Option<MetadataInitializationConfigV1>,
}
/// 薄接原merge；一条连续core跨文件/UTC日消费，错误原样传播。
/// 回调在完整组归约后执行，不签发recording稳定前缀。
pub fn consume_historical_source_progress_v1(
    merge: &mut HistoricalMergeV1,
    header: PublicInputStreamHeaderV1,
    mut applied_callback: impl FnMut(
        &RecordedIngressV1,
        &PublicSourceProgressAppliedV1,
        Option<&SourceArtifactBindingV1>,
    ) -> RecordingResult<()>,
) -> RecordingResult<PublicObservationCoreV1> {
    let mut core = PublicObservationCoreV1::new(header.clone())?;
    while let Some(mut input) = merge.next_input()? {
        let prepared = merge.prepare_input(&core, &mut input)?;
        let applied = core.push_source_progress(&prepared)?;
        applied_callback(&input, &applied, merge.initialization_binding())?;
    }
    let mut end = RecordedIngressV1 {
        input_stream_id: header.input_stream_id,
        input_position: core.applied_prefix().next_input_position,
        ingress: DptIngressV1::EndOfInput,
        dependency_record_refs: vec![],
    };
    let prepared = merge.prepare_input(&core, &mut end)?;
    let applied = core.push_source_progress(&prepared)?;
    applied_callback(&end, &applied, None)?;
    Ok(core)
}
pub fn prepare_finite_source_progress_v1(
    config: &FiniteSourceProgressConfigV1,
    progress: Option<&HistoricalPreparationNotifierV1<'_>>,
    applied_callback: impl FnMut(
        &RecordedIngressV1,
        &PublicSourceProgressAppliedV1,
        Option<&SourceArtifactBindingV1>,
    ) -> RecordingResult<()>,
) -> RecordingResult<PublicObservationCoreV1> {
    std::fs::create_dir(&config.work_root)?;
    let initialization = config
        .metadata_initialization
        .as_ref()
        .map(|c| {
            MetadataInitializationSelectionV1::open_with_progress(
                c,
                config.canonical.decode_batch_rows,
                config.canonical.max_decode_batch_bytes,
                progress,
            )
        })
        .transpose()?;
    let mut merge = prepare_historical_finite_files_with_initialization_v1(
        &config.work_root.join("merge"),
        &config.files,
        &config.canonical,
        config.header.input_stream_id.clone(),
        config.received_start_utc_ms..config.received_end_utc_ms,
        initialization.as_ref(),
        progress,
    )?;
    let mut header = config.header.clone();
    header.ordering_version = HISTORICAL_SOURCE_HEAD_ORDERING_VERSION.into();
    notify(progress, Notice::Stage(Stage::SourceProgressApplication));
    consume_historical_source_progress_v1(&mut merge, header, applied_callback)
}
