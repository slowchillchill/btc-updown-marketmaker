use crate::statistics::HistoricalStatisticsCollectorV1;
use crate::{HistoricalPreparationProgressV1, HistoricalStatisticsV1, MaterialError};
use contracts::*;
use data_pipeline_core::{public_input::*, public_observation_core::PublicObservationCoreV1};
use data_pipeline_training::historical_preparation_progress::{
    HistoricalPreparationNotifierV1, HistoricalPreparationStageV1 as Stage,
};
use data_pipeline_training::{
    canonical_run::CanonicalRunConfig, gate_a::GateAOutputConfig, hidden_preparation::*,
    historical_initialization::*, public_recording::*, public_replay::*,
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
    #[serde(default)]
    pub metadata_initialization: Option<MetadataInitializationConfigV1>,
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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HistoricalInitializationProvenanceV1 {
    pub source_identity_version: Option<String>,
    pub input_positions: std::collections::BTreeSet<u64>,
    pub source_message_ids: std::collections::BTreeSet<String>,
    pub files: Vec<MetadataInitializationFileFactsV1>,
}
impl HistoricalInitializationProvenanceV1 {
    pub fn group_is_initialization(&self, members: &[u64]) -> bool {
        !members.is_empty()
            && members
                .iter()
                .all(|position| self.input_positions.contains(position))
    }
    pub fn group_has_initialization_dependency(&self, members: &[u64], closed_at: u64) -> bool {
        members
            .iter()
            .any(|position| self.input_positions.contains(position))
            || self.input_positions.contains(&closed_at)
    }
    pub fn observation_has_initialization_dependency(
        &self,
        observation: &MarketBusinessObservationV1,
    ) -> bool {
        let depends = |anchors: &[DirectObservationAnchorV1]| {
            anchors.iter().any(|anchor| {
                self.source_message_ids
                    .contains(&anchor.source_message_id.0)
            })
        };
        macro_rules! fields { ($($field:ident),*) => { false $(|| depends(&observation.$field.direct_anchors))* }; }
        fields!(
            lifecycle,
            active,
            closed,
            accepting_orders,
            terminal,
            official_anchor,
            settlement_reference,
            chainlink_spot,
            chainlink_twap_thirty,
            chainlink_twap_sixty,
            binance_depth,
            binance_trade
        ) || observation.assets.iter().any(|asset| {
            depends(&asset.bids.direct_anchors)
                || depends(&asset.asks.direct_anchors)
                || depends(&asset.tick.direct_anchors)
                || depends(&asset.public_trade.direct_anchors)
                || depends(&asset.best_bid_ask.direct_anchors)
                || depends(&asset.valuation_best_bid.direct_anchors)
        })
    }
}
pub struct PreparedHistoricalRunV1 {
    pub public_root: PathBuf,
    pub hidden: HiddenPreparationV1,
    pub markets: BTreeMap<String, PlatformMarketV1>,
    pub source_manifests: Vec<(String, String)>,
    pub input_set_identity_sha256: Sha256Hex,
    pub source_identity_sha256: Sha256Hex,
    pub initialization: HistoricalInitializationProvenanceV1,
}
impl HistoricalPreparationConfigV1 {
    /// 新历史准备及其调用方报告共用的实际流头；不改写旧记录。
    pub fn effective_header(&self) -> PublicInputStreamHeaderV1 {
        let mut header = self.header.clone();
        header.ordering_version = HISTORICAL_SOURCE_HEAD_ORDERING_VERSION.into();
        header
    }
}
/// 显式清单准入、真实文件外排、公共核心正常有限封口后直接移交原隐藏准备。
pub fn prepare_historical_run_v1(
    config: &HistoricalPreparationConfigV1,
) -> Result<PreparedHistoricalRunV1, MaterialError> {
    prepare_internal(config, None, None).map(|(prepared, _)| prepared)
}
/// 同一次生产准备借用原完整组累计统计；全部持久与hidden责任成功后交付。
pub struct PreparedHistoricalRunWithStatisticsV1 {
    pub prepared: PreparedHistoricalRunV1,
    pub statistics: HistoricalStatisticsV1,
}
pub fn prepare_historical_run_with_statistics_v1(
    config: &HistoricalPreparationConfigV1,
    progress: Option<HistoricalPreparationProgressV1>,
) -> Result<PreparedHistoricalRunWithStatisticsV1, MaterialError> {
    prepare_internal(
        config,
        Some(HistoricalStatisticsCollectorV1::default()),
        progress,
    )
    .map(
        |(prepared, statistics)| PreparedHistoricalRunWithStatisticsV1 {
            prepared,
            statistics: statistics.expect("本入口创建原统计collector"),
        },
    )
}
fn prepare_internal(
    config: &HistoricalPreparationConfigV1,
    mut statistics: Option<HistoricalStatisticsCollectorV1>,
    mut progress: Option<HistoricalPreparationProgressV1>,
) -> Result<(PreparedHistoricalRunV1, Option<HistoricalStatisticsV1>), MaterialError> {
    if let Some(p) = &mut progress {
        p.stage(Stage::Admission);
    }
    let result = (|| {
        let header = config.effective_header();
        let input_set = with_training_notifications(&mut progress, |notifications| {
            data_pipeline_training::freeze_public_source_input_set_with_progress_v1(
                &config.scope.manifest_paths,
                notifications,
            )
        })?;
        if let Some(p) = &mut progress {
            p.stage(Stage::InitializationSelection);
        }
        let initialization_selection = config
            .metadata_initialization
            .as_ref()
            .map(|initial| {
                if initial.received_start_utc_ms != config.scope.received_start_utc_ms
                    || initial.received_end_utc_ms != config.scope.received_end_utc_ms
                {
                    return Err::<_, MaterialError>("初始化用途的研究范围与准备范围不一致".into());
                }
                with_training_notifications(&mut progress, |notifications| {
                    MetadataInitializationSelectionV1::open_with_progress(
                        initial,
                        config.canonical.decode_batch_rows,
                        config.canonical.max_decode_batch_bytes,
                        notifications,
                    )
                })
            })
            .transpose()?;
        let source_identity_sha256 =
            match (&initialization_selection, &config.metadata_initialization) {
                (Some(selection), Some(initial)) => {
                    selection.source_identity(input_set.input_set_identity_sha256(), initial)?
                }
                _ => Sha256Hex(input_set.input_set_identity_sha256().into()),
            };
        let mut initialization = HistoricalInitializationProvenanceV1 {
            source_identity_version: initialization_selection
                .as_ref()
                .map(|_| HISTORICAL_INITIALIZATION_SOURCE_IDENTITY_VERSION.into()),
            input_positions: Default::default(),
            source_message_ids: Default::default(),
            files: initialization_selection
                .as_ref()
                .map(|selection| selection.files.clone())
                .unwrap_or_default(),
        };
        let admitted = input_set.admitted_daily_inputs();
        let files = admitted
            .iter()
            .flat_map(|day| day.files())
            .collect::<Vec<_>>();
        std::fs::create_dir(&config.work_root)?;
        let mut merge = with_training_notifications(&mut progress, |notifications| {
            prepare_historical_files_with_initialization_and_progress_v1(
                &config.work_root.join("merge"),
                &files,
                &config.canonical,
                config.header.input_stream_id.clone(),
                Some(config.scope.received_start_utc_ms..config.scope.received_end_utc_ms),
                initialization_selection.as_ref(),
                notifications,
            )
        })?;
        if let Some(p) = &mut progress {
            p.stage(Stage::PublicApplicationStatisticsRecording);
        }
        let public_root = config.work_root.join("public");
        let mut core = PublicObservationCoreV1::new(header.clone())?;
        let mut writer =
            PublicRecordingWriterV1::create(&public_root, &header, config.recording.clone())?;
        while let Some(mut input) = merge.next_input()? {
            if let Some(binding) = merge.initialization_binding() {
                initialization.input_positions.insert(input.input_position);
                initialization
                    .source_message_ids
                    .insert(binding.source_message_id.0.clone());
                writer.record_source_binding(binding.clone())?;
            }
            let prepared = merge.prepare_input(&core, &mut input)?;
            let applied = core.push(&prepared)?.into_owned();
            if let Some(s) = &mut statistics {
                for group in &applied.applied.closed_groups {
                    s.observe(&input, group, &initialization);
                }
            }
            if let Some(p) = &mut progress {
                p.applied(&applied.applied);
            }
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
        if let Some(s) = &mut statistics {
            for group in &applied.applied.closed_groups {
                s.observe(&end, group, &initialization);
            }
        }
        if let Some(p) = &mut progress {
            p.applied(&applied.applied);
        }
        writer.record(end, applied)?;
        if let Some(p) = &mut progress {
            p.stage(Stage::PublicStabilizeAndCatalog);
        }
        writer.stabilize()?;
        drop(writer);
        if config.metadata_initialization.is_some() {
            serde_json::to_writer(
                std::fs::File::create(public_root.join("historical-initialization.json"))?,
                &initialization,
            )?;
        }
        // 正常End已应用且原writer已成功稳定。隐藏入口只提取固定上下文后释放
        // 此生产core，再完整读取真实规范记录并独立归约隐藏数量；不重复空重放。
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
        if let Some(p) = &mut progress {
            p.stage(Stage::HiddenScanAndSupply);
        }
        let reader = PublicRecordingReaderV1::recover(
            &public_root,
            config.read_page_rows,
            config.max_read_batch_bytes,
        )?;
        let hidden = with_training_notifications(&mut progress, |notifications| {
            prepare_hidden_with_progress_v1(
                core,
                reader,
                &config.work_root.join("hidden"),
                &config.recording,
                config.hidden_stream_id.clone(),
                notifications,
            )
        })?;
        Ok((
            PreparedHistoricalRunV1 {
                public_root,
                hidden,
                source_identity_sha256,
                initialization,
                markets,
                input_set_identity_sha256: Sha256Hex(input_set.input_set_identity_sha256().into()),
                source_manifests: admitted
                    .iter()
                    .map(|day| (day.utc_day().into(), day.manifest_sha256().into()))
                    .collect(),
            },
            statistics
                .take()
                .map(HistoricalStatisticsCollectorV1::finish),
        ))
    })();
    if let Some(p) = &mut progress {
        p.finish(result.is_ok());
    }
    result
}

// 只桥接本次历史准备的专用通知；未启用时不给training通知，也不采进程。
fn with_training_notifications<T>(
    progress: &mut Option<HistoricalPreparationProgressV1>,
    run: impl FnOnce(Option<&HistoricalPreparationNotifierV1<'_>>) -> T,
) -> T {
    if let Some(progress) = progress.as_mut().filter(|p| p.enabled()) {
        let mut callback = |notice| progress.notice(notice);
        let notifications = HistoricalPreparationNotifierV1::new(&mut callback);
        run(Some(&notifications))
    } else {
        run(None)
    }
}
