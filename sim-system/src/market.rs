use std::collections::BTreeMap;
use std::fmt;

use contracts::{
    AssetIdentityV1, BestBidAskV2, BookSide, ExecutionMarketEventPayloadV3, ExecutionMarketEventV3,
    LevelExecutionBasis, MappedMarketLifecycleStateV1, MarketDataQuality,
    MarketResolutionTerminalV3, MarketTradingStateV2, QualityScopeV1, ResyncQuality,
    SimCallerRoleV1,
};
use data_pipeline_core::parse_canonical_decimal;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::market_directory::SessionMarketDirectoryV1;
use crate::matching::MatchingStateErrorV1;
use crate::scenario::PlatformMarketV1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketEventPathV1 {
    HiddenPlatform,
    PublicVisibility,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MillisecondBucketClosureV1 {
    NextEventAt(i64),
    EndOfInput,
}

pub(crate) fn market_path_drives_execution_for(
    caller_role: SimCallerRoleV1,
    path: MarketEventPathV1,
) -> bool {
    matches!(
        (caller_role, path),
        (
            SimCallerRoleV1::ModelTrain
                | SimCallerRoleV1::OfflineEvaluation
                | SimCallerRoleV1::HistoricalBacktest,
            MarketEventPathV1::HiddenPlatform
        ) | (SimCallerRoleV1::Paper, MarketEventPathV1::PublicVisibility)
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MarketEventErrorV1 {
    InvalidBucket(&'static str),
    Decode { index: usize, message: String },
    DuplicateEventId(String),
    EventOutsideBucket { index: usize },
    UnknownMarket(String),
    MarketStateAlreadyExists(String),
    MarketIdentityConflict(String),
    UnknownAsset(String),
    InitialSnapshotAlreadyLoaded(String),
    AssetNotInitialized(String),
    LifecyclePredecessorConflict,
    TickPredecessorConflict(String),
    LevelPredecessorConflict(String),
    QualityPredecessorConflict(String),
    UnexpectedLevelBasis(String),
    CausalGroupConflict(String),
    IncompleteInitialLoad(Vec<String>),
    MissingQualifiedMarketTerminal(Vec<String>),
    ResolutionBeforeEnd,
    ResolutionAssetConflict,
    ResolutionConflict(String),
    MarketEvictionConflict(String),
    BucketIdentityConflict,
    InputPathAlreadySealed,
    BucketStartPositionConflict { expected: u64, actual: u64 },
    ExecutionState(String),
}

impl fmt::Display for MarketEventErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBucket(message) => write!(formatter, "已封口市场事件桶非法：{message}"),
            Self::Decode { index, message } => {
                write!(formatter, "市场事件桶第 {index} 项解码失败：{message}")
            }
            Self::DuplicateEventId(id) => write!(formatter, "市场事件桶包含重复 event_id：{id}"),
            Self::EventOutsideBucket { index } => {
                write!(formatter, "市场事件桶第 {index} 项不属于声明的毫秒桶")
            }
            Self::UnknownMarket(id) => write!(formatter, "市场事件不属于会话市场目录：{id}"),
            Self::MarketStateAlreadyExists(id) => {
                write!(formatter, "市场状态已经存在：{id}")
            }
            Self::MarketIdentityConflict(id) => {
                write!(formatter, "市场完整身份与会话市场目录冲突：{id}")
            }
            Self::UnknownAsset(id) => write!(formatter, "市场事件资产不属于会话市场目录：{id}"),
            Self::InitialSnapshotAlreadyLoaded(id) => {
                write!(formatter, "资产已经完成初始 L2 装载：{id}")
            }
            Self::AssetNotInitialized(id) => write!(formatter, "资产尚未完成初始 L2 装载：{id}"),
            Self::LifecyclePredecessorConflict => {
                write!(formatter, "市场生命周期前驱与当前状态冲突")
            }
            Self::TickPredecessorConflict(id) => {
                write!(formatter, "资产 tick 前驱与当前状态冲突：{id}")
            }
            Self::LevelPredecessorConflict(id) => {
                write!(formatter, "外部盘口档位前驱数量与当前状态冲突：{id}")
            }
            Self::QualityPredecessorConflict(id) => {
                write!(formatter, "市场数据质量前驱与当前状态冲突：{id}")
            }
            Self::UnexpectedLevelBasis(id) => {
                write!(formatter, "缺口状态只接受完整 book 重校准档位：{id}")
            }
            Self::CausalGroupConflict(id) => write!(formatter, "桶内因果组身份冲突：{id}"),
            Self::IncompleteInitialLoad(markets) => {
                write!(
                    formatter,
                    "输入结束时市场初始装载仍不完整：{}",
                    markets.join(",")
                )
            }
            Self::MissingQualifiedMarketTerminal(markets) => write!(
                formatter,
                "输入结束时市场仍缺少合格终态：{}",
                markets.join(",")
            ),
            Self::ResolutionBeforeEnd => write!(formatter, "合格市场判定早于市场 end_at"),
            Self::ResolutionAssetConflict => write!(formatter, "市场判定资产与场景冲突"),
            Self::ResolutionConflict(id) => write!(formatter, "市场判定与已保留终态冲突：{id}"),
            Self::MarketEvictionConflict(message) => {
                write!(formatter, "市场热状态驱逐前驱冲突：{message}")
            }
            Self::BucketIdentityConflict => write!(formatter, "同一闭合水位绑定了不同市场事件桶"),
            Self::InputPathAlreadySealed => write!(formatter, "市场事件输入路径已经耗尽并封存"),
            Self::BucketStartPositionConflict { expected, actual } => write!(
                formatter,
                "市场事件桶起始索引位置不连续：期望 {expected}，实际 {actual}"
            ),
            Self::ExecutionState(message) => write!(formatter, "执行队列状态非法：{message}"),
        }
    }
}

impl std::error::Error for MarketEventErrorV1 {}

impl From<MatchingStateErrorV1> for MarketEventErrorV1 {
    fn from(value: MatchingStateErrorV1) -> Self {
        match value {
            MatchingStateErrorV1::ResolutionConflict(condition_id) => {
                Self::ResolutionConflict(condition_id)
            }
            other => Self::ExecutionState(format!("{other:?}")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum MarketLifecyclePhaseV1 {
    Preloading,
    Active,
    PostEndPendingResolution,
    ResolutionTerminal,
    Evicted,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MarketStateStoreV2 {
    markets: BTreeMap<String, MarketStateV2>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MarketBuyRejectionV2 {
    MarketNotInScenario,
    AssetNotInMarket,
    EndAtReached,
    ExplicitTradingStateVeto,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MarketBuyEvidenceUnknownV2 {
    Preloading,
    TargetAssetNotFresh(MarketDataQuality),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MarketBuyAdmissionV2 {
    Ready {
        current_tick_size_pusd_per_share: Decimal,
    },
    RejectedNoEffect(MarketBuyRejectionV2),
    EvidenceUnknownNoEffect(MarketBuyEvidenceUnknownV2),
    MissingCurrentTick,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NormalizedPriceLevelV2 {
    pub(crate) price_pusd_per_share: Decimal,
    pub(crate) quantity_shares: Decimal,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NormalizedMarketEventV2<'a> {
    MarketLifecycleTransition(&'a contracts::MarketLifecycleTransitionV2),
    MarketTradingState(&'a MarketTradingStateV2),
    MarketLifecycleSnapshot(&'a contracts::MappedMarketLifecycleStateV1),
    InitialTick {
        asset: &'a contracts::AssetIdentityV1,
        tick: Decimal,
    },
    InitialL2Snapshot {
        asset: &'a contracts::AssetIdentityV1,
        initial_tick_size_pusd_per_share: Decimal,
        bids: Vec<NormalizedPriceLevelV2>,
        asks: Vec<NormalizedPriceLevelV2>,
    },
    BestBidAsk {
        best: &'a BestBidAskV2,
    },
    TickSizeChange {
        change: &'a contracts::TickSizeChangeV2,
        old_tick_size_pusd_per_share: Option<Decimal>,
        new_tick_size_pusd_per_share: Decimal,
    },
    PublicTrade(&'a contracts::PublicTradeV2),
    LevelExecutionTransition {
        transition: &'a contracts::LevelExecutionTransitionV3,
        price_pusd_per_share: Decimal,
        quantity_before_shares: Decimal,
        quantity_after_shares: Decimal,
    },
    QualityBoundary {
        boundary: &'a contracts::QualityBoundaryV2,
        resync: ResyncQuality,
    },
    MarketResolutionTerminal {
        terminal: &'a MarketResolutionTerminalV3,
        parsed_payouts: [Decimal; 2],
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct QualifiedMarketResolutionV2 {
    pub(crate) resolution_event_id: String,
    pub(crate) resolution_source_event_at_utc_ms: i64,
    pub(crate) terminal: MarketResolutionTerminalV3,
    pub(crate) parsed_payouts: [Decimal; 2],
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct MarketStateApplicationV2 {
    pub(crate) qualified_resolution: Option<QualifiedMarketResolutionV2>,
    pub(crate) resolution_invalidation_reason: Option<String>,
}

pub(crate) fn normalize_market_event_v2<I, E>(
    event: &ExecutionMarketEventV3<I, E>,
) -> Result<NormalizedMarketEventV2<'_>, MarketEventErrorV1> {
    Ok(match &event.payload {
        ExecutionMarketEventPayloadV3::MarketLifecycleTransition(transition) => {
            NormalizedMarketEventV2::MarketLifecycleTransition(transition)
        }
        ExecutionMarketEventPayloadV3::MarketTradingState(state) => {
            NormalizedMarketEventV2::MarketTradingState(state)
        }
        ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) => {
            NormalizedMarketEventV2::InitialL2Snapshot {
                asset: &snapshot.asset,
                initial_tick_size_pusd_per_share: parse_decimal(
                    &snapshot.initial_tick_size_pusd_per_share,
                )?,
                bids: normalize_price_levels_v2(&snapshot.bids)?,
                asks: normalize_price_levels_v2(&snapshot.asks)?,
            }
        }
        ExecutionMarketEventPayloadV3::BestBidAsk(best) => {
            NormalizedMarketEventV2::BestBidAsk { best }
        }
        ExecutionMarketEventPayloadV3::TickSizeChange(change) => {
            NormalizedMarketEventV2::TickSizeChange {
                change,
                old_tick_size_pusd_per_share: change
                    .old_tick_size_pusd_per_share
                    .as_ref()
                    .map(parse_decimal)
                    .transpose()?,
                new_tick_size_pusd_per_share: parse_decimal(&change.new_tick_size_pusd_per_share)?,
            }
        }
        ExecutionMarketEventPayloadV3::PublicTrade(trade) => {
            NormalizedMarketEventV2::PublicTrade(trade)
        }
        ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) => {
            NormalizedMarketEventV2::LevelExecutionTransition {
                transition,
                price_pusd_per_share: parse_decimal(&transition.price_pusd_per_share)?,
                quantity_before_shares: parse_decimal(&transition.quantity_before_shares)?,
                quantity_after_shares: parse_decimal(&transition.quantity_after_shares)?,
            }
        }
        ExecutionMarketEventPayloadV3::QualityBoundary(boundary) => {
            NormalizedMarketEventV2::QualityBoundary {
                boundary,
                resync: event.quality.resync,
            }
        }
        ExecutionMarketEventPayloadV3::MarketResolutionTerminal(terminal) => {
            normalize_terminal_v2(terminal)?
        }
    })
}

fn normalize_price_levels_v2(
    levels: &[contracts::PriceLevelV1],
) -> Result<Vec<NormalizedPriceLevelV2>, MarketEventErrorV1> {
    levels
        .iter()
        .map(|level| {
            Ok(NormalizedPriceLevelV2 {
                price_pusd_per_share: parse_decimal(&level.price_pusd_per_share)?,
                quantity_shares: parse_decimal(&level.quantity_shares)?,
            })
        })
        .collect()
}

impl MarketStateStoreV2 {
    pub(crate) fn pm_quantity(
        &self,
        condition: &str,
        token: &str,
        side: contracts::BookSide,
        price: Decimal,
    ) -> Decimal {
        self.markets
            .get(condition)
            .and_then(|market| market.assets.get(token))
            .map_or(Decimal::ZERO, |asset| {
                asset.external_book.quantity(side, price)
            })
    }
    pub(crate) fn applies_pm_l2(&self, condition: &str, at: i64, end: i64) -> bool {
        at < end
            && self.markets.get(condition).is_some_and(|market| {
                !matches!(
                    market.lifecycle_phase,
                    MarketLifecyclePhaseV1::PostEndPendingResolution
                        | MarketLifecyclePhaseV1::ResolutionTerminal
                )
            })
    }

    pub(crate) fn new(directory: &SessionMarketDirectoryV1) -> Self {
        let markets = directory
            .markets()
            .map(|market| {
                (
                    market.market().condition_id.0.clone(),
                    MarketStateV2::new(market),
                )
            })
            .collect();
        Self { markets }
    }

    pub(crate) fn commit_admitted_market(&mut self, market: &PlatformMarketV1) {
        self.markets.insert(
            market.market().condition_id.0.clone(),
            MarketStateV2::new(market),
        );
    }

    pub(crate) fn market_count(&self) -> usize {
        self.markets.len()
    }

    pub(crate) fn contains_market(&self, condition_id: &str) -> bool {
        self.markets.contains_key(condition_id)
    }

    pub(crate) fn buy_admission(
        &self,
        condition_id: &str,
        token_id: &str,
        effective_processing_at_utc_ms: i64,
        end_at_utc_ms: i64,
    ) -> MarketBuyAdmissionV2 {
        let Some(market) = self.markets.get(condition_id) else {
            return MarketBuyAdmissionV2::RejectedNoEffect(
                MarketBuyRejectionV2::MarketNotInScenario,
            );
        };
        if effective_processing_at_utc_ms >= end_at_utc_ms
            || matches!(
                market.lifecycle_phase,
                MarketLifecyclePhaseV1::PostEndPendingResolution
                    | MarketLifecyclePhaseV1::ResolutionTerminal
            )
        {
            return MarketBuyAdmissionV2::RejectedNoEffect(MarketBuyRejectionV2::EndAtReached);
        }
        if market.trading_state.active == Some(false)
            || market.trading_state.closed == Some(true)
            || market.trading_state.accepting_orders == Some(false)
        {
            return MarketBuyAdmissionV2::RejectedNoEffect(
                MarketBuyRejectionV2::ExplicitTradingStateVeto,
            );
        }
        let Some(asset) = market.assets.get(token_id) else {
            return MarketBuyAdmissionV2::RejectedNoEffect(MarketBuyRejectionV2::AssetNotInMarket);
        };
        if market.lifecycle_phase == MarketLifecyclePhaseV1::Preloading {
            return MarketBuyAdmissionV2::EvidenceUnknownNoEffect(
                MarketBuyEvidenceUnknownV2::Preloading,
            );
        }
        if asset.quality != MarketDataQuality::Fresh {
            return MarketBuyAdmissionV2::EvidenceUnknownNoEffect(
                MarketBuyEvidenceUnknownV2::TargetAssetNotFresh(asset.quality),
            );
        }
        asset.current_tick_size_pusd_per_share.map_or(
            MarketBuyAdmissionV2::MissingCurrentTick,
            |current_tick_size_pusd_per_share| MarketBuyAdmissionV2::Ready {
                current_tick_size_pusd_per_share,
            },
        )
    }

    #[cfg(test)]
    fn apply_admitted_event(
        &mut self,
        event: &ExecutionMarketEventV3,
        normalized: &NormalizedMarketEventV2<'_>,
        at: i64,
        end: i64,
    ) -> Result<MarketStateApplicationV2, MarketEventErrorV1> {
        let header = MarketEventHeaderV1 {
            identity: &event.event_id.0,
            market: &event.market,
            time: &event.time,
            quality: &event.quality,
        };
        self.apply_admitted_header(&header, normalized, at, end)
    }

    pub(crate) fn apply_admitted_header(
        &mut self,
        event: &MarketEventHeaderV1<'_>,
        normalized: &NormalizedMarketEventV2<'_>,
        platform_effective_at_utc_ms: i64,
        end_at_utc_ms: i64,
    ) -> Result<MarketStateApplicationV2, MarketEventErrorV1> {
        self.markets
            .get_mut(&event.market.condition_id.0)
            .expect("目录已经准入的 V2 市场必须存在于角色市场状态")
            .apply_event(
                event,
                normalized,
                platform_effective_at_utc_ms,
                end_at_utc_ms,
            )
    }

    pub(crate) fn advance_time(
        &mut self,
        directory: &crate::market_directory::SessionMarketDirectoryV1,
        platform_effective_at_utc_ms: i64,
    ) -> Result<(), MarketEventErrorV1> {
        for admitted in directory.markets() {
            let condition_id = &admitted.market().condition_id.0;
            let market = self
                .markets
                .get_mut(condition_id)
                .ok_or_else(|| MarketEventErrorV1::UnknownMarket(condition_id.clone()))?;
            market.advance_time(
                platform_effective_at_utc_ms,
                admitted.market().window.end_at_utc_ms,
            );
        }
        Ok(())
    }

    pub(crate) fn validate_input_complete(&self) -> Result<(), MarketEventErrorV1> {
        let incomplete = self
            .markets
            .iter()
            .filter(|(_, market)| !market.assets.values().all(|asset| asset.initialized))
            .map(|(condition_id, _)| condition_id.clone())
            .collect::<Vec<_>>();
        if !incomplete.is_empty() {
            return Err(MarketEventErrorV1::IncompleteInitialLoad(incomplete));
        }
        let missing_terminal = self
            .markets
            .iter()
            .filter(|(_, market)| market.source_qualified_resolution.is_none())
            .map(|(condition_id, _)| condition_id.clone())
            .collect::<Vec<_>>();
        if !missing_terminal.is_empty() {
            return Err(MarketEventErrorV1::MissingQualifiedMarketTerminal(
                missing_terminal,
            ));
        }
        Ok(())
    }

    pub(crate) fn all_qualified_resolutions_terminal(&self) -> bool {
        self.markets.values().all(|market| {
            market.source_qualified_resolution.is_some()
                && market.lifecycle_phase == MarketLifecyclePhaseV1::ResolutionTerminal
        })
    }

    pub(crate) fn mark_resolution_terminal_if_qualified(
        &mut self,
        condition_id: &str,
        resolution: &MarketResolutionTerminalV3,
    ) -> bool {
        self.markets
            .get_mut(condition_id)
            .is_some_and(|market| market.mark_resolution_terminal_if_qualified(resolution))
    }

    #[cfg(test)]
    pub(crate) fn lifecycle_phase_for_tests(
        &self,
        condition_id: &str,
    ) -> Option<MarketLifecyclePhaseV1> {
        self.markets
            .get(condition_id)
            .map(|market| market.lifecycle_phase)
    }

    #[cfg(test)]
    pub(crate) fn external_quantity_for_tests(
        &self,
        condition_id: &str,
        token_id: &str,
        side: BookSide,
        price: Decimal,
    ) -> Option<Decimal> {
        self.markets
            .get(condition_id)?
            .assets
            .get(token_id)
            .map(|asset| asset.external_book.quantity(side, price))
    }

    #[cfg(test)]
    pub(crate) fn external_level_count_for_tests(&self, condition_id: &str) -> usize {
        self.markets.get(condition_id).map_or(0, |market| {
            market
                .assets
                .values()
                .map(|asset| asset.external_book.level_count())
                .sum()
        })
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
struct MarketStateV2 {
    lifecycle_phase: MarketLifecyclePhaseV1,
    mapped_lifecycle_state: Option<MappedMarketLifecycleStateV1>,
    trading_state: MarketTradingStateV2,
    market_quality: MarketDataQuality,
    assets: BTreeMap<String, AssetMarketStateV2>,
    source_qualified_resolution: Option<SourceQualifiedResolutionV2>,
}

#[derive(Debug, PartialEq, Eq)]
struct SourceQualifiedResolutionV2 {
    resolution_event_id: String,
    resolution_source_event_at_utc_ms: i64,
    terminal: MarketResolutionTerminalV3,
    parsed_payouts: [Decimal; 2],
}

impl MarketStateV2 {
    fn new(market: &PlatformMarketV1) -> Self {
        Self {
            lifecycle_phase: MarketLifecyclePhaseV1::Preloading,
            mapped_lifecycle_state: None,
            trading_state: MarketTradingStateV2 {
                active: None,
                closed: None,
                accepting_orders: None,
            },
            market_quality: MarketDataQuality::UnknownNotObserved,
            assets: market
                .assets()
                .iter()
                .map(|asset| (asset.token_id.0.clone(), AssetMarketStateV2::default()))
                .collect(),
            source_qualified_resolution: None,
        }
    }

    fn apply_event(
        &mut self,
        event: &MarketEventHeaderV1<'_>,
        normalized: &NormalizedMarketEventV2<'_>,
        platform_effective_at_utc_ms: i64,
        end_at_utc_ms: i64,
    ) -> Result<MarketStateApplicationV2, MarketEventErrorV1> {
        let mut resolution_invalidation_reason = None;
        self.advance_time(platform_effective_at_utc_ms, end_at_utc_ms);
        match normalized {
            NormalizedMarketEventV2::InitialL2Snapshot { .. }
            | NormalizedMarketEventV2::LevelExecutionTransition { .. }
                if matches!(
                    self.lifecycle_phase,
                    MarketLifecyclePhaseV1::PostEndPendingResolution
                        | MarketLifecyclePhaseV1::ResolutionTerminal
                ) => {}
            NormalizedMarketEventV2::MarketLifecycleTransition(transition) => {
                if self.mapped_lifecycle_state.is_some()
                    && transition.previous_state != self.mapped_lifecycle_state
                {
                    return Err(MarketEventErrorV1::LifecyclePredecessorConflict);
                }

                self.mapped_lifecycle_state = Some(transition.current_state.clone());
            }
            NormalizedMarketEventV2::MarketLifecycleSnapshot(current) => {
                self.mapped_lifecycle_state = Some((*current).clone());
            }
            NormalizedMarketEventV2::InitialTick { asset, tick } => {
                self.admitted_asset_mut(asset)
                    .current_tick_size_pusd_per_share = Some(*tick);
            }
            NormalizedMarketEventV2::MarketTradingState(state) => {
                self.trading_state = (*state).clone();
            }
            NormalizedMarketEventV2::InitialL2Snapshot {
                asset: snapshot_asset,
                initial_tick_size_pusd_per_share,
                bids,
                asks,
            } => {
                let asset = self.admitted_asset_mut(snapshot_asset);
                if asset.initialized {
                    return Err(MarketEventErrorV1::InitialSnapshotAlreadyLoaded(
                        snapshot_asset.token_id.0.clone(),
                    ));
                }
                asset.initialized = true;
                asset.current_tick_size_pusd_per_share = Some(*initial_tick_size_pusd_per_share);
                asset.quality = event.quality.availability;
                asset.external_book = ExactExternalBookV2::from_levels(bids, asks);

                if self.assets.values().all(|asset| asset.initialized) {
                    self.lifecycle_phase = MarketLifecyclePhaseV1::Active;
                }
            }
            NormalizedMarketEventV2::BestBidAsk { .. } => {}
            NormalizedMarketEventV2::TickSizeChange {
                change,
                old_tick_size_pusd_per_share,
                new_tick_size_pusd_per_share,
            } => {
                let asset = self.initialized_admitted_asset_mut(&change.asset)?;
                if old_tick_size_pusd_per_share.is_some()
                    && asset.current_tick_size_pusd_per_share != *old_tick_size_pusd_per_share
                {
                    return Err(MarketEventErrorV1::TickPredecessorConflict(
                        change.asset.token_id.0.clone(),
                    ));
                }
                asset.current_tick_size_pusd_per_share = Some(*new_tick_size_pusd_per_share);
            }
            NormalizedMarketEventV2::PublicTrade(_) => {}
            NormalizedMarketEventV2::LevelExecutionTransition {
                transition,
                price_pusd_per_share,
                quantity_before_shares,
                quantity_after_shares,
            } => {
                let asset = self.initialized_admitted_asset_mut(&transition.asset)?;
                if asset.quality != MarketDataQuality::Fresh
                    && transition.basis != LevelExecutionBasis::BookResyncDeltaInference
                {
                    return Err(MarketEventErrorV1::UnexpectedLevelBasis(
                        transition.asset.token_id.0.clone(),
                    ));
                }
                if asset
                    .external_book
                    .quantity(transition.side, *price_pusd_per_share)
                    != *quantity_before_shares
                {
                    return Err(MarketEventErrorV1::LevelPredecessorConflict(format!(
                        "{}:{:?}:{price_pusd_per_share}",
                        transition.asset.token_id.0, transition.side
                    )));
                }
                asset.external_book.set_quantity(
                    transition.side,
                    *price_pusd_per_share,
                    *quantity_after_shares,
                );
            }
            NormalizedMarketEventV2::QualityBoundary { boundary, resync } => {
                match &boundary.scope {
                    QualityScopeV1::Market => {
                        if self.market_quality != boundary.previous_quality {
                            return Err(MarketEventErrorV1::QualityPredecessorConflict(
                                event.market.condition_id.0.clone(),
                            ));
                        }
                        self.market_quality = boundary.current_quality;
                        if boundary.current_quality == MarketDataQuality::Invalid
                            && self.source_qualified_resolution.is_some()
                            && self.lifecycle_phase != MarketLifecyclePhaseV1::ResolutionTerminal
                        {
                            resolution_invalidation_reason = Some(boundary.reason_code.clone());
                        }
                    }
                    QualityScopeV1::Asset(identity) => {
                        let asset = self.admitted_asset_mut(identity);
                        if asset.quality != boundary.previous_quality {
                            return Err(MarketEventErrorV1::QualityPredecessorConflict(
                                identity.token_id.0.clone(),
                            ));
                        }
                        if *resync == ResyncQuality::Required {
                            asset.external_book.clear();
                        }
                        asset.quality = boundary.current_quality;
                    }
                }
            }
            NormalizedMarketEventV2::MarketResolutionTerminal {
                terminal,
                parsed_payouts,
            } => {
                if !self.assets.values().all(|asset| asset.initialized) {
                    return Err(MarketEventErrorV1::AssetNotInitialized(
                        event.market.condition_id.0.clone(),
                    ));
                }
                self.market_quality = event.quality.availability;
                if let Some(source_event_at_utc_ms) = event.time.source_event_at_utc_ms {
                    if source_event_at_utc_ms < end_at_utc_ms {
                        return Err(MarketEventErrorV1::ResolutionBeforeEnd);
                    }
                    if self
                        .source_qualified_resolution
                        .as_ref()
                        .is_some_and(|existing| existing.terminal != **terminal)
                    {
                        return Err(MarketEventErrorV1::ResolutionConflict(
                            event.market.condition_id.0.clone(),
                        ));
                    }
                    if self.source_qualified_resolution.is_none() {
                        let evidence = SourceQualifiedResolutionV2 {
                            resolution_event_id: event.identity.to_owned(),
                            resolution_source_event_at_utc_ms: source_event_at_utc_ms,
                            terminal: (*terminal).clone(),
                            parsed_payouts: *parsed_payouts,
                        };

                        self.source_qualified_resolution = Some(evidence);
                    }
                    if self.lifecycle_phase != MarketLifecyclePhaseV1::ResolutionTerminal {
                        self.lifecycle_phase = MarketLifecyclePhaseV1::PostEndPendingResolution;
                    }
                }
            }
        }
        self.advance_time(platform_effective_at_utc_ms, end_at_utc_ms);
        Ok(MarketStateApplicationV2 {
            qualified_resolution: self.qualified_resolution(),
            resolution_invalidation_reason,
        })
    }

    fn advance_time(&mut self, platform_effective_at_utc_ms: i64, end_at_utc_ms: i64) {
        if platform_effective_at_utc_ms >= end_at_utc_ms
            && self.lifecycle_phase == MarketLifecyclePhaseV1::Active
        {
            self.lifecycle_phase = MarketLifecyclePhaseV1::PostEndPendingResolution;
            for asset in self.assets.values_mut() {
                asset.external_book.clear();
            }
        }
    }

    fn qualified_resolution(&self) -> Option<QualifiedMarketResolutionV2> {
        (self.market_quality == MarketDataQuality::Fresh
            && self.lifecycle_phase != MarketLifecyclePhaseV1::ResolutionTerminal)
            .then(|| {
                self.source_qualified_resolution.as_ref().map(|resolution| {
                    QualifiedMarketResolutionV2 {
                        resolution_event_id: resolution.resolution_event_id.clone(),
                        resolution_source_event_at_utc_ms: resolution
                            .resolution_source_event_at_utc_ms,
                        terminal: resolution.terminal.clone(),
                        parsed_payouts: resolution.parsed_payouts,
                    }
                })
            })
            .flatten()
    }

    fn mark_resolution_terminal_if_qualified(
        &mut self,
        resolution: &MarketResolutionTerminalV3,
    ) -> bool {
        if self.market_quality == MarketDataQuality::Fresh
            && self
                .source_qualified_resolution
                .as_ref()
                .is_some_and(|existing| existing.terminal == *resolution)
        {
            self.lifecycle_phase = MarketLifecyclePhaseV1::ResolutionTerminal;
            true
        } else {
            false
        }
    }

    fn admitted_asset_mut(&mut self, identity: &AssetIdentityV1) -> &mut AssetMarketStateV2 {
        self.assets
            .get_mut(&identity.token_id.0)
            .expect("目录已经准入的 V2 资产必须存在于市场状态")
    }

    fn initialized_admitted_asset_mut(
        &mut self,
        identity: &AssetIdentityV1,
    ) -> Result<&mut AssetMarketStateV2, MarketEventErrorV1> {
        let asset = self.admitted_asset_mut(identity);
        if !asset.initialized {
            return Err(MarketEventErrorV1::AssetNotInitialized(
                identity.token_id.0.clone(),
            ));
        }
        Ok(asset)
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
struct AssetMarketStateV2 {
    initialized: bool,
    current_tick_size_pusd_per_share: Option<Decimal>,
    quality: MarketDataQuality,
    external_book: ExactExternalBookV2,
}

impl Default for AssetMarketStateV2 {
    fn default() -> Self {
        Self {
            initialized: false,
            current_tick_size_pusd_per_share: None,
            quality: MarketDataQuality::UnknownNotObserved,
            external_book: ExactExternalBookV2::default(),
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ExactExternalBookV2 {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl ExactExternalBookV2 {
    fn from_levels(bids: &[NormalizedPriceLevelV2], asks: &[NormalizedPriceLevelV2]) -> Self {
        let mut book = Self::default();
        for (side, levels) in [(BookSide::Bid, bids), (BookSide::Ask, asks)] {
            for level in levels {
                book.set_quantity(side, level.price_pusd_per_share, level.quantity_shares);
            }
        }
        book
    }

    fn levels(&self, side: BookSide) -> &BTreeMap<Decimal, Decimal> {
        match side {
            BookSide::Bid => &self.bids,
            BookSide::Ask => &self.asks,
        }
    }

    fn levels_mut(&mut self, side: BookSide) -> &mut BTreeMap<Decimal, Decimal> {
        match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        }
    }

    fn quantity(&self, side: BookSide, price: Decimal) -> Decimal {
        self.levels(side).get(&price).copied().unwrap_or_default()
    }

    fn level_count(&self) -> usize {
        self.bids.len() + self.asks.len()
    }

    fn set_quantity(&mut self, side: BookSide, price: Decimal, quantity: Decimal) {
        if quantity.is_zero() {
            self.levels_mut(side).remove(&price);
        } else {
            self.levels_mut(side).insert(price, quantity);
        }
    }

    fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }
}

#[cfg(feature = "measurement")]
fn mapped_lifecycle_owned_bytes(state: Option<&MappedMarketLifecycleStateV1>) -> u64 {
    match state {
        Some(MappedMarketLifecycleStateV1::UnknownUnmodeled { raw_value }) => {
            raw_value.len() as u64
        }
        Some(MappedMarketLifecycleStateV1::Known { .. }) | None => 0,
    }
}

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct AccountBookOverlayV1 {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

#[cfg(test)]
std::thread_local! {
    static ACCOUNT_BOOK_OVERLAY_CLONE_COUNT_FOR_TESTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

impl Clone for AccountBookOverlayV1 {
    fn clone(&self) -> Self {
        #[cfg(test)]
        ACCOUNT_BOOK_OVERLAY_CLONE_COUNT_FOR_TESTS.with(|count| count.set(count.get() + 1));
        Self {
            bids: self.bids.clone(),
            asks: self.asks.clone(),
        }
    }
}

#[cfg(test)]
fn reset_account_book_overlay_clone_count_for_tests() {
    ACCOUNT_BOOK_OVERLAY_CLONE_COUNT_FOR_TESTS.with(|count| count.set(0));
}

#[cfg(test)]
fn account_book_overlay_clone_count_for_tests() -> usize {
    ACCOUNT_BOOK_OVERLAY_CLONE_COUNT_FOR_TESTS.with(std::cell::Cell::get)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AccountBookOverlayStoreV1 {
    markets: BTreeMap<String, BTreeMap<String, AccountBookOverlayV1>>,
}

pub(crate) struct PreparedAccountBookOverlayLevelChangeV2<'a> {
    overlay: &'a mut AccountBookOverlayV1,
    side: BookSide,
    price: Decimal,
    candidate_quantity: Decimal,
}

impl AccountBookOverlayStoreV1 {
    pub(crate) fn new(directory: &SessionMarketDirectoryV1) -> Self {
        let markets = directory
            .markets()
            .map(|market| {
                let assets = market
                    .assets()
                    .iter()
                    .map(|asset| (asset.token_id.0.clone(), AccountBookOverlayV1::default()))
                    .collect();
                (market.market().condition_id.0.clone(), assets)
            })
            .collect();
        Self { markets }
    }

    pub(crate) fn insert_market(
        &mut self,
        market: &PlatformMarketV1,
    ) -> Result<(), MatchingStateErrorV1> {
        let condition_id = &market.market().condition_id.0;
        if self.markets.contains_key(condition_id) {
            return Err(MatchingStateErrorV1::MarketAlreadyInitialized(
                condition_id.clone(),
            ));
        }
        let assets = market
            .assets()
            .iter()
            .map(|asset| (asset.token_id.0.clone(), AccountBookOverlayV1::default()))
            .collect();
        self.markets.insert(condition_id.clone(), assets);
        Ok(())
    }

    pub(crate) fn commit_admitted_market(&mut self, market: &PlatformMarketV1) {
        let assets = market
            .assets()
            .iter()
            .map(|asset| (asset.token_id.0.clone(), AccountBookOverlayV1::default()))
            .collect();
        self.markets
            .insert(market.market().condition_id.0.clone(), assets);
    }

    pub(crate) fn contains_market(&self, condition_id: &str) -> bool {
        self.markets.contains_key(condition_id)
    }

    pub(crate) fn has_exact_directory_shape(&self, directory: &SessionMarketDirectoryV1) -> bool {
        self.markets.len() == directory.markets().len()
            && directory.markets().all(|market| {
                self.markets
                    .get(&market.market().condition_id.0)
                    .is_some_and(|assets| {
                        assets.len() == market.assets().len()
                            && market
                                .assets()
                                .iter()
                                .all(|asset| assets.contains_key(&asset.token_id.0))
                    })
            })
    }

    #[cfg(test)]
    pub(crate) fn market_and_asset_count_for_tests(&self) -> (usize, usize) {
        (
            self.markets.len(),
            self.markets.values().map(BTreeMap::len).sum(),
        )
    }

    pub(crate) fn market_is_empty(&self, condition_id: &str) -> Option<bool> {
        self.markets.get(condition_id).map(|assets| {
            assets
                .values()
                .all(|overlay| overlay.bids.is_empty() && overlay.asks.is_empty())
        })
    }

    pub(crate) fn quantity(
        &self,
        condition_id: &str,
        token_id: &str,
        side: BookSide,
        price: Decimal,
    ) -> Decimal {
        self.markets
            .get(condition_id)
            .and_then(|assets| assets.get(token_id))
            .map_or(Decimal::ZERO, |overlay| overlay.quantity(side, price))
    }

    pub(crate) fn best_price(
        &self,
        condition_id: &str,
        token_id: &str,
        side: BookSide,
    ) -> Option<Decimal> {
        self.markets
            .get(condition_id)
            .and_then(|assets| assets.get(token_id))
            .and_then(|overlay| overlay.best_price(side))
    }

    pub(crate) fn prepare_level_change_v2(
        &mut self,
        condition_id: &str,
        token_id: &str,
        side: BookSide,
        price: Decimal,
    ) -> Option<PreparedAccountBookOverlayLevelChangeV2<'_>> {
        let overlay = self.markets.get_mut(condition_id)?.get_mut(token_id)?;
        let candidate_quantity = overlay.quantity(side, price);
        Some(PreparedAccountBookOverlayLevelChangeV2 {
            overlay,
            side,
            price,
            candidate_quantity,
        })
    }

    pub(crate) fn add_quantity(
        &mut self,
        condition_id: &str,
        token_id: &str,
        side: BookSide,
        price: Decimal,
        quantity: Decimal,
    ) -> Option<()> {
        let overlay = self.markets.get_mut(condition_id)?.get_mut(token_id)?;
        let next =
            AccountBookOverlayV1::checked_add_quantity(overlay.quantity(side, price), quantity)?;
        overlay.set_quantity(side, price, next);
        Some(())
    }

    pub(crate) fn subtract_quantity(
        &mut self,
        condition_id: &str,
        token_id: &str,
        side: BookSide,
        price: Decimal,
        quantity: Decimal,
    ) -> Option<()> {
        let overlay = self.markets.get_mut(condition_id)?.get_mut(token_id)?;
        let next = AccountBookOverlayV1::checked_subtract_quantity(
            overlay.quantity(side, price),
            quantity,
        )?;
        overlay.set_quantity(side, price, next);
        Some(())
    }

    pub(crate) fn set_quantity(
        &mut self,
        condition_id: &str,
        token_id: &str,
        side: BookSide,
        price: Decimal,
        quantity: Decimal,
    ) {
        self.markets
            .get_mut(condition_id)
            .and_then(|assets| assets.get_mut(token_id))
            .expect("test overlay target must belong to scenario")
            .set_quantity(side, price, quantity);
    }
}

impl PreparedAccountBookOverlayLevelChangeV2<'_> {
    pub(crate) fn quantity(&self) -> Decimal {
        self.candidate_quantity
    }

    pub(crate) fn add_quantity(&mut self, quantity: Decimal) -> Option<()> {
        self.candidate_quantity =
            AccountBookOverlayV1::checked_add_quantity(self.candidate_quantity, quantity)?;
        Some(())
    }

    pub(crate) fn subtract_quantity(&mut self, quantity: Decimal) -> Option<()> {
        self.candidate_quantity =
            AccountBookOverlayV1::checked_subtract_quantity(self.candidate_quantity, quantity)?;
        Some(())
    }

    pub(crate) fn commit(self) {
        self.overlay
            .set_quantity(self.side, self.price, self.candidate_quantity);
    }
}

impl AccountBookOverlayV1 {
    fn levels(&self, side: BookSide) -> &BTreeMap<Decimal, Decimal> {
        match side {
            BookSide::Bid => &self.bids,
            BookSide::Ask => &self.asks,
        }
    }

    fn levels_mut(&mut self, side: BookSide) -> &mut BTreeMap<Decimal, Decimal> {
        match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        }
    }

    fn quantity(&self, side: BookSide, price: Decimal) -> Decimal {
        self.levels(side).get(&price).copied().unwrap_or_default()
    }

    fn checked_add_quantity(current: Decimal, quantity: Decimal) -> Option<Decimal> {
        current.checked_add(quantity)
    }

    fn checked_subtract_quantity(current: Decimal, quantity: Decimal) -> Option<Decimal> {
        let next = current.checked_sub(quantity)?;
        (!next.is_sign_negative()).then_some(next)
    }

    fn best_price(&self, side: BookSide) -> Option<Decimal> {
        match side {
            BookSide::Bid => self.levels(side).last_key_value(),
            BookSide::Ask => self.levels(side).first_key_value(),
        }
        .map(|(price, _)| *price)
    }

    fn set_quantity(&mut self, side: BookSide, price: Decimal, quantity: Decimal) {
        if quantity.is_zero() {
            self.levels_mut(side).remove(&price);
        } else {
            self.levels_mut(side).insert(price, quantity);
        }
    }
}

fn parse_decimal(value: &contracts::ExactDecimal) -> Result<Decimal, MarketEventErrorV1> {
    parse_canonical_decimal(value, "sim-system market state").map_err(|error| {
        MarketEventErrorV1::Decode {
            index: 0,
            message: error.to_string(),
        }
    })
}

#[cfg(feature = "measurement")]
fn source_resolution_owned_bytes_v2(resolution: &SourceQualifiedResolutionV2) -> u64 {
    let MarketResolutionTerminalV3::Resolved {
        winning_asset_id,
        winning_outcome,
        payout_vector,
        mapping_rule_version,
    } = &resolution.terminal;
    resolution.resolution_event_id.len() as u64
        + winning_asset_id.len() as u64
        + winning_outcome.len() as u64
        + mapping_rule_version.len() as u64
        + payout_vector
            .iter()
            .map(|entry| {
                (entry.asset.token_id.0.len()
                    + entry.asset.outcome.label.len()
                    + entry.payout_pusd_per_share.0.len()) as u64
            })
            .sum::<u64>()
}

fn resolution_matches_assets<'a>(
    terminal: &MarketResolutionTerminalV3,
    expected: impl Iterator<Item = &'a AssetIdentityV1>,
) -> bool {
    let MarketResolutionTerminalV3::Resolved { payout_vector, .. } = terminal;
    payout_vector.iter().map(|entry| &entry.asset).eq(expected)
}

#[cfg(feature = "measurement")]
impl MarketStateStoreV2 {
    pub(crate) fn measurement_resources(&self) -> crate::resource::ResourceQuantityV1 {
        let mut total = crate::resource::ResourceQuantityV1::default();
        for (condition, market) in &self.markets {
            total.add_record(
                condition.len() as u64
                    + mapped_lifecycle_owned_bytes(market.mapped_lifecycle_state.as_ref()),
            );
            for (token, asset) in &market.assets {
                total.add_record(token.len() as u64);
                total.add_records(asset.external_book.level_count() as u64);
            }
            if let Some(resolution) = &market.source_qualified_resolution {
                total.add_record(source_resolution_owned_bytes_v2(resolution));
            }
        }
        total
    }
}
#[cfg(feature = "measurement")]
impl AccountBookOverlayStoreV1 {
    pub(crate) fn measurement_resources(&self) -> crate::resource::ResourceQuantityV1 {
        let mut total = crate::resource::ResourceQuantityV1::default();
        for (condition, assets) in &self.markets {
            total.add_record(condition.len() as u64);
            for (token, book) in assets {
                total.add_record(token.len() as u64);
                total.add_records((book.bids.len() + book.asks.len()) as u64);
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::scenario_for_sim_tests;
    use crate::task21_tests::{
        asset, fresh_quality, initial_snapshot_event, orderbook_evidence, sealed_event,
    };

    fn account_overlay_store_for_tests() -> (AccountBookOverlayStoreV1, String, String) {
        let scenario = scenario_for_sim_tests();
        let directory = SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("测试目录");
        let market = scenario.market_set().markets().first().expect("测试市场");
        (
            AccountBookOverlayStoreV1::new(&directory),
            market.market().condition_id.0.clone(),
            market
                .assets()
                .first()
                .expect("测试资产")
                .token_id
                .0
                .clone(),
        )
    }

    fn decode_v2_event_for_tests(event: &ExecutionMarketEventV3) -> ExecutionMarketEventV3 {
        let bytes = data_pipeline_core::encode_execution_market_event_v3(event)
            .expect("编码 V2 市场状态测试事件");
        data_pipeline_core::decode_execution_market_event_v3(&bytes)
            .expect("从真实边界解码 V2 市场状态测试事件")
    }

    #[test]
    fn market_state_v2_parses_once_and_keeps_exact_level_endpoints() {
        let scenario = scenario_for_sim_tests();
        let directory = SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("测试目录");
        let market = scenario.market_set().markets().first().expect("测试市场");
        let condition_id = &market.market().condition_id.0;
        let mut store = MarketStateStoreV2::new(&directory);
        let initial = [
            initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "0.0000009"),
            initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "10"),
        ]
        .map(|event| decode_v2_event_for_tests(&event));
        for event in &initial {
            let normalized = normalize_market_event_v2(event).expect("一次规范化初始快照");
            store
                .apply_admitted_event(
                    event,
                    &normalized,
                    1_100,
                    market.market().window.end_at_utc_ms,
                )
                .expect("应用规范初始快照");
        }

        let transition = decode_v2_event_for_tests(&sealed_event(
            ExecutionMarketEventPayloadV3::LevelExecutionTransition(
                contracts::LevelExecutionTransitionV3 {
                    causal_group_id: contracts::Sha256Hex("9".repeat(64)),
                    market: market.market().clone(),
                    asset: asset("1", 0, "Up"),
                    side: BookSide::Bid,
                    price_pusd_per_share: contracts::ExactDecimal("0.4".to_owned()),
                    quantity_before_shares: contracts::ExactDecimal("0.0000009".to_owned()),
                    quantity_after_shares: contracts::ExactDecimal("0.0000011".to_owned()),
                    attributed_trade_quantity_shares: contracts::ExactDecimal("0".to_owned()),
                    already_covered_quantity_shares: contracts::ExactDecimal("0".to_owned()),
                    net_added_quantity_shares: contracts::ExactDecimal("0.0000002".to_owned()),
                    net_reduced_quantity_shares: contracts::ExactDecimal("0".to_owned()),
                    queue_consumption_quantity_shares: contracts::ExactDecimal("0".to_owned()),
                    basis: LevelExecutionBasis::LevelDeltaInference,
                    platform_effective_at_utc_ms: 1_300,
                    visible_at_utc_ms: 1_400,
                    source_attribution_quality: contracts::SourceAttributionQuality::Unattributed,
                    queue_result_quality: contracts::QueueResultQuality::DeterministicInferred,
                },
            ),
            orderbook_evidence(1_300, 1_400, 3),
            fresh_quality(),
        ));
        let normalized = normalize_market_event_v2(&transition).expect("一次规范化档位端点");
        store
            .apply_admitted_event(
                &transition,
                &normalized,
                1_300,
                market.market().window.end_at_utc_ms,
            )
            .expect("应用精确档位端点");

        assert_eq!(
            store
                .external_quantity_for_tests(condition_id, "1", BookSide::Bid, Decimal::new(4, 1),),
            Some(Decimal::new(11, 7)),
        );
    }

    #[test]
    fn market_buy_admission_uses_frozen_priority_and_only_target_asset_quality() {
        let scenario = scenario_for_sim_tests();
        let directory = SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("测试目录");
        let market = scenario.market_set().markets().first().expect("测试市场");
        let condition_id = &market.market().condition_id.0;
        let end_at_utc_ms = market.market().window.end_at_utc_ms;

        let preloading = MarketStateStoreV2::new(&directory);
        assert_eq!(
            preloading.buy_admission(condition_id, "unknown-token", end_at_utc_ms, end_at_utc_ms,),
            MarketBuyAdmissionV2::RejectedNoEffect(MarketBuyRejectionV2::EndAtReached),
            "end_at 必须先于未知资产裁决"
        );
        assert_eq!(
            preloading.buy_admission(condition_id, "1", 1_500, end_at_utc_ms),
            MarketBuyAdmissionV2::EvidenceUnknownNoEffect(MarketBuyEvidenceUnknownV2::Preloading,)
        );

        let mut vetoed = MarketStateStoreV2::new(&directory);
        let veto = decode_v2_event_for_tests(&sealed_event(
            ExecutionMarketEventPayloadV3::MarketTradingState(MarketTradingStateV2 {
                active: Some(false),
                closed: None,
                accepting_orders: None,
            }),
            orderbook_evidence(1_300, 1_301, 10),
            fresh_quality(),
        ));
        let normalized = normalize_market_event_v2(&veto).expect("一次规范化显式交易否决");
        vetoed
            .apply_admitted_event(&veto, &normalized, 1_300, end_at_utc_ms)
            .expect("应用显式交易否决");
        assert_eq!(
            vetoed.buy_admission(condition_id, "unknown-token", 1_500, end_at_utc_ms),
            MarketBuyAdmissionV2::RejectedNoEffect(MarketBuyRejectionV2::ExplicitTradingStateVeto,),
            "显式否决必须先于未知资产裁决"
        );

        let mut active = MarketStateStoreV2::new(&directory);
        for event in [
            initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10"),
            initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "10"),
        ]
        .map(|event| decode_v2_event_for_tests(&event))
        {
            let normalized = normalize_market_event_v2(&event).expect("一次规范化初始快照");
            active
                .apply_admitted_event(&event, &normalized, 1_100, end_at_utc_ms)
                .expect("应用规范初始快照");
        }
        let other_asset_stale = decode_v2_event_for_tests(&sealed_event(
            ExecutionMarketEventPayloadV3::QualityBoundary(contracts::QualityBoundaryV2 {
                scope: QualityScopeV1::Asset(asset("2", 1, "Down")),
                previous_quality: MarketDataQuality::Fresh,
                current_quality: MarketDataQuality::Stale,
                reason_code: "TEST_OTHER_ASSET_STALE".to_owned(),
            }),
            orderbook_evidence(1_300, 1_301, 11),
            contracts::EventQualityV2 {
                availability: MarketDataQuality::Stale,
                resync: ResyncQuality::NotRequired,
            },
        ));
        let normalized =
            normalize_market_event_v2(&other_asset_stale).expect("一次规范化另一资产质量边界");
        active
            .apply_admitted_event(&other_asset_stale, &normalized, 1_300, end_at_utc_ms)
            .expect("应用另一资产质量边界");
        assert_eq!(
            active.buy_admission(condition_id, "1", 1_500, end_at_utc_ms),
            MarketBuyAdmissionV2::Ready {
                current_tick_size_pusd_per_share: Decimal::new(1, 2),
            },
            "另一资产质量不得跨资产阻断目标资产"
        );

        let target_stale = decode_v2_event_for_tests(&sealed_event(
            ExecutionMarketEventPayloadV3::QualityBoundary(contracts::QualityBoundaryV2 {
                scope: QualityScopeV1::Asset(asset("1", 0, "Up")),
                previous_quality: MarketDataQuality::Fresh,
                current_quality: MarketDataQuality::Stale,
                reason_code: "TEST_TARGET_ASSET_STALE".to_owned(),
            }),
            orderbook_evidence(1_310, 1_311, 12),
            contracts::EventQualityV2 {
                availability: MarketDataQuality::Stale,
                resync: ResyncQuality::NotRequired,
            },
        ));
        let normalized =
            normalize_market_event_v2(&target_stale).expect("一次规范化目标资产质量边界");
        active
            .apply_admitted_event(&target_stale, &normalized, 1_310, end_at_utc_ms)
            .expect("应用目标资产质量边界");
        assert_eq!(
            active.buy_admission(condition_id, "1", 1_500, end_at_utc_ms),
            MarketBuyAdmissionV2::EvidenceUnknownNoEffect(
                MarketBuyEvidenceUnknownV2::TargetAssetNotFresh(MarketDataQuality::Stale),
            )
        );
    }

    #[test]
    fn account_book_overlay_level_change_reads_its_candidate_and_drop_keeps_live_state() {
        let (mut store, condition_id, token_id) = account_overlay_store_for_tests();
        let price = Decimal::new(45, 2);
        store.set_quantity(
            &condition_id,
            &token_id,
            BookSide::Bid,
            price,
            Decimal::new(10, 0),
        );
        let before = store.clone();

        let mut prepared = store
            .prepare_level_change_v2(&condition_id, &token_id, BookSide::Bid, price)
            .expect("目标账户盘口必须存在");
        assert_eq!(prepared.quantity(), Decimal::new(10, 0));
        prepared
            .add_quantity(Decimal::new(5, 0))
            .expect("首次增加候选数量");
        prepared
            .subtract_quantity(Decimal::new(3, 0))
            .expect("后续扣减必须读到候选数量");
        prepared
            .add_quantity(Decimal::new(2, 0))
            .expect("再次增加必须读到同一候选数量");
        assert_eq!(prepared.quantity(), Decimal::new(14, 0));
        drop(prepared);

        assert_eq!(store, before, "丢弃候选不得改动 live 账户盘口");
    }

    #[test]
    fn account_book_overlay_level_change_commit_matches_the_v1_in_place_sequence() {
        let (mut actual, condition_id, token_id) = account_overlay_store_for_tests();
        let mut expected = actual.clone();
        let bid_price = Decimal::new(40, 2);

        for store in [&mut actual, &mut expected] {
            store.set_quantity(
                &condition_id,
                &token_id,
                BookSide::Bid,
                bid_price,
                Decimal::new(7, 0),
            );
        }
        expected
            .add_quantity(
                &condition_id,
                &token_id,
                BookSide::Bid,
                bid_price,
                Decimal::new(5, 0),
            )
            .expect("V1 增加数量");
        expected
            .subtract_quantity(
                &condition_id,
                &token_id,
                BookSide::Bid,
                bid_price,
                Decimal::new(12, 0),
            )
            .expect("V1 扣至零");
        let mut prepared = actual
            .prepare_level_change_v2(&condition_id, &token_id, BookSide::Bid, bid_price)
            .expect("目标账户盘口必须存在");
        prepared
            .add_quantity(Decimal::new(5, 0))
            .expect("V2 增加数量");
        prepared
            .subtract_quantity(Decimal::new(12, 0))
            .expect("V2 扣至零");
        prepared.commit();

        assert_eq!(actual, expected);
    }

    #[test]
    fn account_book_overlay_level_change_errors_and_unknown_target_write_nothing() {
        let (mut store, condition_id, token_id) = account_overlay_store_for_tests();
        let insufficient_price = Decimal::new(41, 2);
        let overflow_price = Decimal::new(42, 2);
        store.set_quantity(
            &condition_id,
            &token_id,
            BookSide::Bid,
            insufficient_price,
            Decimal::new(2, 0),
        );
        store.set_quantity(
            &condition_id,
            &token_id,
            BookSide::Ask,
            overflow_price,
            Decimal::MAX,
        );
        let before = store.clone();

        assert!(
            store
                .prepare_level_change_v2(
                    "unknown-condition",
                    &token_id,
                    BookSide::Bid,
                    insufficient_price,
                )
                .is_none()
        );
        let mut insufficient = store
            .prepare_level_change_v2(&condition_id, &token_id, BookSide::Bid, insufficient_price)
            .expect("目标账户盘口必须存在");
        assert_eq!(insufficient.subtract_quantity(Decimal::new(3, 0)), None);
        assert_eq!(insufficient.quantity(), Decimal::new(2, 0));
        drop(insufficient);

        let mut overflow = store
            .prepare_level_change_v2(&condition_id, &token_id, BookSide::Ask, overflow_price)
            .expect("目标账户盘口必须存在");
        assert_eq!(overflow.add_quantity(Decimal::ONE), None);
        assert_eq!(overflow.quantity(), Decimal::MAX);
        drop(overflow);

        assert_eq!(store, before);
    }

    #[test]
    fn account_book_overlay_level_change_ignores_512_unrelated_targets() {
        reset_account_book_overlay_clone_count_for_tests();
        let mut markets = BTreeMap::new();
        for index in 0_i64..512 {
            let mut overlay = AccountBookOverlayV1::default();
            overlay.set_quantity(
                BookSide::Bid,
                Decimal::new(index + 1, 3),
                Decimal::new(index + 1, 0),
            );
            markets.insert(
                format!("unrelated-condition-{index:03}"),
                BTreeMap::from([(format!("unrelated-token-{index:03}"), overlay)]),
            );
        }
        markets.insert(
            "target-condition".to_owned(),
            BTreeMap::from([("target-token".to_owned(), AccountBookOverlayV1::default())]),
        );
        let mut actual = AccountBookOverlayStoreV1 { markets };
        let mut expected = actual.clone();
        assert_eq!(account_book_overlay_clone_count_for_tests(), 513);
        reset_account_book_overlay_clone_count_for_tests();
        let bid_price = Decimal::new(45, 2);

        {
            let mut prepared = actual
                .prepare_level_change_v2(
                    "target-condition",
                    "target-token",
                    BookSide::Bid,
                    bid_price,
                )
                .expect("目标账户盘口必须存在");
            prepared
                .add_quantity(Decimal::new(4, 0))
                .expect("增加目标 bid");
            prepared
                .add_quantity(Decimal::new(3, 0))
                .expect("同一目标 bid 继续增加");
            prepared
                .subtract_quantity(Decimal::new(2, 0))
                .expect("同一目标 bid 继续扣减");
            assert_eq!(account_book_overlay_clone_count_for_tests(), 0);
        }
        assert_eq!(actual, expected, "丢弃候选不得触及任何 live 目标");
        assert_eq!(account_book_overlay_clone_count_for_tests(), 0);

        let mut prepared = actual
            .prepare_level_change_v2("target-condition", "target-token", BookSide::Bid, bid_price)
            .expect("目标账户盘口必须存在");
        prepared
            .add_quantity(Decimal::new(4, 0))
            .expect("增加目标 bid");
        prepared
            .add_quantity(Decimal::new(3, 0))
            .expect("同一目标 bid 继续增加");
        prepared
            .subtract_quantity(Decimal::new(2, 0))
            .expect("同一目标 bid 继续扣减");
        assert_eq!(account_book_overlay_clone_count_for_tests(), 0);
        prepared.commit();
        assert_eq!(account_book_overlay_clone_count_for_tests(), 0);

        expected
            .add_quantity(
                "target-condition",
                "target-token",
                BookSide::Bid,
                bid_price,
                Decimal::new(5, 0),
            )
            .expect("V1 目标 bid");
        assert_eq!(actual, expected, "512 个无关目标及其价格不得被触及");
    }
}

/// 撮合只需原事件引用；新位置不伪装成尚未物化的摘要。
pub(crate) trait MarketEventIdentityV1 {
    fn identity_key(&self) -> String;
}
impl MarketEventIdentityV1 for contracts::CanonicalEventId {
    fn identity_key(&self) -> String {
        self.0.clone()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct MarketEventHeaderV1<'a> {
    pub identity: &'a str,
    pub market: &'a contracts::MarketIdentityV2,
    pub time: &'a contracts::EventTimeV2,
    pub quality: &'a contracts::EventQualityV2,
}

/// 直接借用当次 PM 步骤；只做撮合已有的精确数值归一化，不复制原盘口数组。
pub(crate) fn normalize_pm_step_v1<'a, E, I>(
    step: &'a data_pipeline_core::pm_reduction::PmReductionStepV1<'_, E, I>,
) -> Result<Option<NormalizedMarketEventV2<'a>>, MarketEventErrorV1> {
    use data_pipeline_core::pm_reduction::PmStepChangeV1;
    Ok(Some(match &step.change {
        PmStepChangeV1::InitialSnapshot {
            asset,
            initial_tick_size_pusd_per_share,
            bids,
            asks,
            ..
        } => NormalizedMarketEventV2::InitialL2Snapshot {
            asset,
            initial_tick_size_pusd_per_share: parse_decimal(initial_tick_size_pusd_per_share)?,
            bids: normalize_price_levels_v2(bids)?,
            asks: normalize_price_levels_v2(asks)?,
        },
        PmStepChangeV1::LevelChange { transition, .. } => {
            NormalizedMarketEventV2::LevelExecutionTransition {
                transition,
                price_pusd_per_share: parse_decimal(&transition.price_pusd_per_share)?,
                quantity_before_shares: parse_decimal(&transition.quantity_before_shares)?,
                quantity_after_shares: parse_decimal(&transition.quantity_after_shares)?,
            }
        }
        PmStepChangeV1::BestBidAsk { quote, .. } => {
            NormalizedMarketEventV2::BestBidAsk { best: quote }
        }
        PmStepChangeV1::Tick { tick, .. } => NormalizedMarketEventV2::TickSizeChange {
            change: tick,
            old_tick_size_pusd_per_share: tick
                .old_tick_size_pusd_per_share
                .as_ref()
                .map(parse_decimal)
                .transpose()?,
            new_tick_size_pusd_per_share: parse_decimal(&tick.new_tick_size_pusd_per_share)?,
        },
        PmStepChangeV1::PublicTrade { trade } => NormalizedMarketEventV2::PublicTrade(trade),
        PmStepChangeV1::Quality { boundary } => NormalizedMarketEventV2::QualityBoundary {
            boundary,
            resync: step.quality.resync,
        },
        _ => return Ok(None),
    }))
}

pub(crate) fn normalize_public_market_fact_v1<'a>(
    fact: &'a contracts::CanonicalMarketFactPayloadV1,
    quality: &contracts::EventQualityV2,
) -> Result<Option<NormalizedMarketEventV2<'a>>, MarketEventErrorV1> {
    use contracts::CanonicalMarketFactPayloadV1 as P;
    Ok(Some(match fact {
        P::FixedIdentity { .. } => return Ok(None),
        P::InitialTick {
            asset,
            tick_size_pusd_per_share,
        } => NormalizedMarketEventV2::InitialTick {
            asset,
            tick: parse_decimal(tick_size_pusd_per_share)?,
        },
        P::LifecycleTransition(v) => NormalizedMarketEventV2::MarketLifecycleTransition(v),
        P::MarketLifecycleSnapshot { current_state, .. } => {
            NormalizedMarketEventV2::MarketLifecycleSnapshot(current_state)
        }
        P::TradingState(v) => NormalizedMarketEventV2::MarketTradingState(v),
        P::QualityBoundary(v) => NormalizedMarketEventV2::QualityBoundary {
            boundary: v,
            resync: quality.resync,
        },
        P::Terminal(terminal) => normalize_terminal_v2(terminal)?,
    }))
}

fn normalize_terminal_v2(
    terminal: &MarketResolutionTerminalV3,
) -> Result<NormalizedMarketEventV2<'_>, MarketEventErrorV1> {
    let MarketResolutionTerminalV3::Resolved { payout_vector, .. } = terminal;
    let parsed_payouts = payout_vector
        .iter()
        .map(|entry| parse_decimal(&entry.payout_pusd_per_share))
        .collect::<Result<Vec<_>, _>>()?
        .try_into()
        .map_err(|_| MarketEventErrorV1::ResolutionAssetConflict)?;
    Ok(NormalizedMarketEventV2::MarketResolutionTerminal {
        terminal,
        parsed_payouts,
    })
}
