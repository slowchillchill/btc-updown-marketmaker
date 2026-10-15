use crate::statistics::HistoricalStatisticsCollectorV1;
use crate::{HistoricalPreparationProgressV1, HistoricalStatisticsV1, MaterialError};
use contracts::*;

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
    /// 录制及一次性读取保留原无缓存入口；候选连续消费使用下方增量分类器。
    pub fn observations_have_initialization_dependency(
        &self,
        observations: &MarketObservationSetV1,
    ) -> bool {
        if self.source_message_ids.is_empty() || observations.is_empty() {
            return false;
        }
        let mut work = InitializationDependencyWork::default();
        let mut probe = InitializationDependencyProbe {
            sources: &self.source_message_ids,
            work: &mut work,
        };
        if let Some((markets, group)) = observations.shared_parts() {
            probe.group(group) || markets.values().any(|market| probe.market(market))
        } else {
            observations
                .iter()
                .any(|observation| probe.observation(observation))
        }
    }
    pub fn observation_has_initialization_dependency(
        &self,
        observation: &MarketBusinessObservationV1,
    ) -> bool {
        let mut work = InitializationDependencyWork::default();
        InitializationDependencyProbe {
            sources: &self.source_message_ids,
            work: &mut work,
        }
        .observation(observation)
    }
}
/// 按已知初始化来源增量分类；日作业共享发布边界和旧录制读取复用同一实现。
pub(crate) struct InitializationDependencyClassifier {
    diagnostics_enabled: bool,
    previous: Option<MarketObservationSnapshotsV1>,
    sources: std::collections::BTreeSet<String>,
    dependent_markets: std::collections::BTreeSet<String>,
}
impl Default for InitializationDependencyClassifier {
    fn default() -> Self {
        Self {
            previous: None,
            sources: Default::default(),
            dependent_markets: Default::default(),
            diagnostics_enabled: cfg!(test)
                || (cfg!(feature = "stage-timing")
                    && std::env::var_os("BACKTEST_TRANSPORT_DIAGNOSTICS")
                        .is_some_and(|v| v == "1")),
        }
    }
}
/// 实际分类访问量；不包含 OrdMap 为定位差分而比较的树节点。
#[derive(Default)]
pub(crate) struct InitializationDependencyWork {
    enabled: bool,
    pub markets: u64,
    pub source_ids: u64,
}
struct InitializationDependencyProbe<'a> {
    sources: &'a std::collections::BTreeSet<String>,
    work: &'a mut InitializationDependencyWork,
}
impl InitializationDependencyProbe<'_> {
    fn anchors(&mut self, anchors: &[DirectObservationAnchorV1]) -> bool {
        anchors.iter().any(|anchor| {
            if self.work.enabled {
                self.work.source_ids += 1;
            }
            self.sources.contains(&anchor.source_message_id.0)
        })
    }
    fn asset(&mut self, asset: &AssetBusinessObservationV1) -> bool {
        self.anchors(&asset.bids.direct_anchors)
            || self.anchors(&asset.asks.direct_anchors)
            || self.anchors(&asset.tick.direct_anchors)
            || self.anchors(&asset.public_trade.direct_anchors)
            || self.anchors(&asset.best_bid_ask.direct_anchors)
            || self.anchors(&asset.valuation_best_bid.direct_anchors)
    }
    fn market(&mut self, market: &MarketObservationSnapshotV1) -> bool {
        if self.work.enabled {
            self.work.markets += 1;
        }
        self.anchors(&market.lifecycle.direct_anchors)
            || self.anchors(&market.active.direct_anchors)
            || self.anchors(&market.closed.direct_anchors)
            || self.anchors(&market.accepting_orders.direct_anchors)
            || self.anchors(&market.terminal.direct_anchors)
            || self.anchors(&market.official_anchor.direct_anchors)
            || self.anchors(&market.settlement_reference.direct_anchors)
            || market.assets.iter().any(|asset| self.asset(asset))
    }
    // 前一市场已证实不依赖初始化时，未变 Arc 侧仍为假，无需重扫全部价位来源。
    fn changed_market(
        &mut self,
        market: &MarketObservationSnapshotV1,
        known_independent: Option<&MarketObservationSnapshotV1>,
    ) -> bool {
        let Some(old) = known_independent else {
            return self.market(market);
        };
        if self.work.enabled {
            self.work.markets += 1;
        }
        self.anchors(&market.lifecycle.direct_anchors)
            || self.anchors(&market.active.direct_anchors)
            || self.anchors(&market.closed.direct_anchors)
            || self.anchors(&market.accepting_orders.direct_anchors)
            || self.anchors(&market.terminal.direct_anchors)
            || self.anchors(&market.official_anchor.direct_anchors)
            || self.anchors(&market.settlement_reference.direct_anchors)
            || market.assets.iter().any(|asset| {
                let Some(previous) = old.assets.iter().find(|a| a.asset == asset.asset) else {
                    return self.asset(asset);
                };
                (!std::sync::Arc::ptr_eq(&asset.bids, &previous.bids)
                    && self.anchors(&asset.bids.direct_anchors))
                    || (!std::sync::Arc::ptr_eq(&asset.asks, &previous.asks)
                        && self.anchors(&asset.asks.direct_anchors))
                    || (!std::sync::Arc::ptr_eq(
                        &asset.valuation_best_bid,
                        &previous.valuation_best_bid,
                    ) && self.anchors(&asset.valuation_best_bid.direct_anchors))
                    || self.anchors(&asset.tick.direct_anchors)
                    || self.anchors(&asset.public_trade.direct_anchors)
                    || self.anchors(&asset.best_bid_ask.direct_anchors)
            })
    }
    fn observation(&mut self, observation: &MarketBusinessObservationV1) -> bool {
        if self.work.enabled {
            self.work.markets += 1;
        }
        macro_rules! fields { ($($field:ident),*) => {
            false $(|| self.anchors(&observation.$field.direct_anchors))*
        }; }
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
        ) || observation.assets.iter().any(|asset| self.asset(asset))
    }
    fn group(&mut self, group: &MarketObservationGroupV1) -> bool {
        self.anchors(&group.chainlink_spot.direct_anchors)
            || self.anchors(&group.chainlink_twap_thirty.direct_anchors)
            || self.anchors(&group.chainlink_twap_sixty.direct_anchors)
            || self.anchors(&group.binance_depth.direct_anchors)
            || self.anchors(&group.binance_trade.direct_anchors)
    }
}
impl InitializationDependencyClassifier {
    pub(crate) fn classify(
        &mut self,
        provenance: &HistoricalInitializationProvenanceV1,
        observations: &MarketObservationSetV1,
    ) -> (bool, InitializationDependencyWork) {
        let mut work = InitializationDependencyWork {
            enabled: self.diagnostics_enabled,
            ..Default::default()
        };
        if provenance.source_message_ids.is_empty() {
            self.previous = None;
            self.sources.clear();
            self.dependent_markets.clear();
            return (false, work);
        }
        let Some((markets, group)) = observations.shared_parts() else {
            // 旧数组没有共享树；不把旧共享快照冒充这个组。
            self.previous = None;
            let mut probe = InitializationDependencyProbe {
                sources: &provenance.source_message_ids,
                work: &mut work,
            };
            let dependent = observations
                .iter()
                .any(|observation| probe.observation(observation));
            return (dependent, work);
        };
        let mut probe = InitializationDependencyProbe {
            sources: &provenance.source_message_ids,
            work: &mut work,
        };
        if let Some(previous) = &self.previous
            && self.sources == provenance.source_message_ids
        {
            for change in previous.diff(markets) {
                use imbl::ordmap::DiffItem;
                match change {
                    DiffItem::Add(id, new) => {
                        if probe.market(new) {
                            self.dependent_markets.insert(id.clone());
                        }
                    }
                    DiffItem::Remove(id, _) => {
                        self.dependent_markets.remove(id);
                    }
                    DiffItem::Update {
                        old: (_, old),
                        new: (id, new),
                    } => {
                        let dependent = self.dependent_markets.remove(id);
                        if probe.changed_market(new, (!dependent).then_some(old)) {
                            self.dependent_markets.insert(id.clone());
                        }
                    }
                }
            }
        } else {
            self.dependent_markets = markets
                .iter()
                .filter(|(_, market)| probe.market(market))
                .map(|(id, _)| id.clone())
                .collect();
            self.sources.clone_from(&provenance.source_message_ids);
        }
        // 即使本组全局字段已经命中，也更新市场缓存，防止下一组使用旧汇总。
        self.previous = Some(markets.clone());
        let dependent =
            !markets.is_empty() && (!self.dependent_markets.is_empty() || probe.group(group));
        (dependent, work)
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
/// 唯一正式发布入口；准备完整成功才原子发布固定描述，不创建策略或账户。
pub fn prepare_and_publish_historical_run_v1(
    config: &HistoricalPreparationConfigV1,
    progress: Option<HistoricalPreparationProgressV1>,
) -> Result<PathBuf, MaterialError> {
    let (prepared, _) = prepare_internal(config, None, progress)?;
    crate::PreparedHistoricalMaterialV1::from_prepared(config, &prepared)?
        .publish(&config.work_root)
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
        let progress_cell = std::cell::RefCell::new(&mut progress);
        let (core, _) = if statistics.is_some() {
            data_pipeline_training::finite_recording::record_historical_merge_v1(
                &mut merge,
                header,
                &public_root,
                config.recording.clone(),
                |input, applied, binding| {
                    if let Some(binding) = binding {
                        initialization.input_positions.insert(input.input_position);
                        initialization
                            .source_message_ids
                            .insert(binding.source_message_id.0.clone());
                    }
                    if let Some(applied) = applied {
                        if let Some(s) = &mut statistics {
                            for group in &applied.applied.closed_groups {
                                s.observe(input, group, &initialization);
                            }
                        }
                        if let Some(p) = &mut **progress_cell.borrow_mut() {
                            p.applied(
                                applied.applied.applied_prefix.next_input_position,
                                applied.applied.closed_groups.len() as u64,
                                applied
                                    .applied
                                    .closed_groups
                                    .iter()
                                    .map(|g| g.observations.len() as u64)
                                    .sum(),
                            );
                        }
                    }
                },
                || {
                    if let Some(p) = &mut **progress_cell.borrow_mut() {
                        p.stage(Stage::PublicStabilizeAndCatalog);
                    }
                },
            )?
        } else {
            data_pipeline_training::finite_recording::record_historical_merge_counts_v1(
                &mut merge,
                header,
                &public_root,
                config.recording.clone(),
                |input, applied, binding| {
                    if let Some(binding) = binding {
                        initialization.input_positions.insert(input.input_position);
                        initialization
                            .source_message_ids
                            .insert(binding.source_message_id.0.clone());
                    }
                    if let Some(applied) = applied
                        && let Some(p) = &mut **progress_cell.borrow_mut()
                    {
                        p.applied(
                            applied.applied_prefix.next_input_position,
                            applied.closed_groups.len() as u64,
                            applied
                                .closed_groups
                                .iter()
                                .map(|g| g.observation_count)
                                .sum(),
                        );
                    }
                },
                || {
                    if let Some(p) = &mut **progress_cell.borrow_mut() {
                        p.stage(Stage::PublicStabilizeAndCatalog);
                    }
                },
            )?
        };
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
