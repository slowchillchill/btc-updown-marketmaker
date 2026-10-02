use crate::MaterialError;
use contracts::*;
use data_pipeline_core::{public_input::*, public_observation_core::PublicObservationCoreV1};
use data_pipeline_training::{
    canonical_run::CanonicalRunConfig, gate_a::GateAOutputConfig, hidden_preparation::*,
    public_recording::*, public_replay::*,
};
use serde::{Deserialize, Serialize};
use sim_system::scenario::PlatformMarketV1;
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoricalScopeV1 {
    pub manifest_paths: Vec<PathBuf>,
    pub received_start_utc_ms: i64,
    pub received_end_utc_ms: i64,
    /// 引用调用方已批准的研究/开发用途；不是独立留出集授权。
    pub purpose: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoricalPreparationConfigV1 {
    pub scope: HistoricalScopeV1,
    pub work_root: PathBuf,
    /// 调用方提供流身份与业务配置；ordering_version 由实际历史排序实现确定，
    /// 此处的兼容输入值不选择排序算法，也不作为新产物版本。
    pub header: PublicInputStreamHeaderV1,
    pub canonical: CanonicalRunConfig,
    pub recording: GateAOutputConfig,
    pub read_page_rows: usize,
    pub max_read_batch_bytes: usize,
    pub hidden_stream_id: String,
}
pub struct PreparedHistoricalRunV1 {
    pub public_root: PathBuf,
    pub hidden: HiddenPreparationV1,
    pub markets: BTreeMap<String, PlatformMarketV1>,
    pub source_manifests: Vec<(String, String)>,
    pub input_set_identity_sha256: Sha256Hex,
}
impl HistoricalPreparationConfigV1 {
    pub(crate) fn effective_header(&self) -> PublicInputStreamHeaderV1 {
        let mut header = self.header.clone();
        header.ordering_version = HISTORICAL_SOURCE_HEAD_ORDERING_VERSION.into();
        header
    }
}
/// 显式清单准入、真实文件外排、公共核心正常有限封口、原序重放和隐藏准备。
pub fn prepare_historical_run_v1(
    config: &HistoricalPreparationConfigV1,
) -> Result<PreparedHistoricalRunV1, MaterialError> {
    let header = config.effective_header();
    let input_set =
        data_pipeline_training::freeze_public_source_input_set_v1(&config.scope.manifest_paths)?;
    let admitted = input_set.admitted_daily_inputs();
    let files = admitted
        .iter()
        .flat_map(|day| day.files())
        .collect::<Vec<_>>();
    std::fs::create_dir(&config.work_root)?;
    let mut merge = prepare_historical_files_in_range_v1(
        &config.work_root.join("merge"),
        &files,
        &config.canonical,
        config.header.input_stream_id.clone(),
        Some(config.scope.received_start_utc_ms..config.scope.received_end_utc_ms),
    )?;
    let public_root = config.work_root.join("public");
    let mut core = PublicObservationCoreV1::new(header.clone())?;
    let mut writer =
        PublicRecordingWriterV1::create(&public_root, &header, config.recording.clone())?;
    while let Some(mut input) = merge.next_input()? {
        let prepared = merge.prepare_input(&core, &mut input)?;
        let applied = core.push(&prepared)?.into_owned();
        writer.record(input, applied)?;
    }
    let mut end = RecordedIngressV1 {
        input_stream_id: config.header.input_stream_id.clone(),
        input_position: core.applied_prefix().next_input_position,
        ingress: DptIngressV1::EndOfInput,
        dependency_record_refs: vec![],
    };
    let prepared = merge.prepare_input(&core, &mut end)?;
    let applied = core.push(&prepared)?.into_owned();
    writer.record(end, applied)?;
    writer.stabilize()?;
    drop(writer);
    drop(core);
    // 隐藏准备消费真实正常前缀对应的重放核心，不直接借用制备时的内存状态。
    let mut replay = PublicReplayV1::open(
        &public_root,
        config.read_page_rows,
        config.max_read_batch_bytes,
    )?;
    while replay.next_applied()?.is_some() {}
    let core = replay.into_core();
    let markets = core
        .fixed_markets()
        .map(|market| {
            let context = core
                .pm_context(&market.condition_id.0)
                .expect("原登记市场具有 PM 身份");
            let assets = context
                .assets
                .into_iter()
                .map(|asset| asset.asset)
                .collect::<Vec<_>>()
                .try_into()
                .map_err(|_| "原登记市场不是二元市场")?;
            Ok((
                market.condition_id.0.clone(),
                PlatformMarketV1::try_new(market.clone(), assets)?,
            ))
        })
        .collect::<Result<_, MaterialError>>()?;
    let reader = PublicRecordingReaderV1::recover(
        &public_root,
        config.read_page_rows,
        config.max_read_batch_bytes,
    )?;
    let hidden = prepare_hidden_v1(
        core,
        reader,
        &config.work_root.join("hidden"),
        &config.recording,
        config.hidden_stream_id.clone(),
    )?;
    Ok(PreparedHistoricalRunV1 {
        public_root,
        hidden,
        markets,
        input_set_identity_sha256: Sha256Hex(input_set.input_set_identity_sha256().into()),
        source_manifests: admitted
            .iter()
            .map(|day| (day.utc_day().into(), day.manifest_sha256().into()))
            .collect(),
    })
}
