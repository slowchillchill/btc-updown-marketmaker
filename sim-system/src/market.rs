use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use contracts::{
    AssetIdentityV1, BestBidAskV2, BookSide, CanonicalEventId, ExecutionMarketEventPayloadV3,
    ExecutionMarketEventV3, LevelExecutionBasis, MappedMarketLifecycleStateV1, MarketDataQuality,
    MarketIdentityV2, MarketResolutionTerminalV3, MarketTradingStateV2, PublicTradeIdSourceV1,
    QualityScopeV1, ResyncQuality, Sha256Hex,
};
use data_pipeline_core::{decode_execution_market_event_v3, parse_canonical_decimal};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::market_directory::SessionMarketDirectoryV1;
use crate::matching::{AuthorizedMarketEventEffectsV1, MatchingStateErrorV1};
use crate::scenario::PlatformMarketV1;
use crate::session::{InputPathSealOutcomeV1, SessionError, SimCallerRoleV1, SimSession};

const CLOSED_BUCKET_HASH_DOMAIN_V1: &[u8] = b"SIM_CLOSED_MARKET_EVENT_BUCKET_V1\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum MarketEventPathV1 {
    HiddenPlatform,
    PublicVisibility,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum MillisecondBucketClosureV1 {
    NextEventAt(i64),
    EndOfInput,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClosedMarketEventBucketV1 {
    path: MarketEventPathV1,
    bucket_at_utc_ms: i64,
    start_index_position: u64,
    next_index_position: u64,
    closure: MillisecondBucketClosureV1,
    encoded_events: Arc<[Vec<u8>]>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DurableMarketBucketReferenceV1 {
    pub(crate) path: MarketEventPathV1,
    pub(crate) bucket_at_utc_ms: i64,
    pub(crate) start_index_position: u64,
    pub(crate) next_index_position: u64,
    pub(crate) closure: MillisecondBucketClosureV1,
    pub(crate) bucket_sha256: Sha256Hex,
}

impl ClosedMarketEventBucketV1 {
    pub(crate) fn drives_execution_for(&self, caller_role: SimCallerRoleV1) -> bool {
        matches!(
            (caller_role, self.path),
            (
                SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation,
                MarketEventPathV1::HiddenPlatform
            ) | (SimCallerRoleV1::Paper, MarketEventPathV1::PublicVisibility)
        )
    }

    pub(crate) fn bucket_at_utc_ms(&self) -> i64 {
        self.bucket_at_utc_ms
    }

    pub(crate) fn lifecycle_work_identity(&self) -> String {
        bucket_sha256(self).0
    }

    pub(crate) fn durable_reference(&self) -> DurableMarketBucketReferenceV1 {
        DurableMarketBucketReferenceV1 {
            path: self.path,
            bucket_at_utc_ms: self.bucket_at_utc_ms,
            start_index_position: self.start_index_position,
            next_index_position: self.next_index_position,
            closure: self.closure,
            bucket_sha256: bucket_sha256(self),
        }
    }

    pub(crate) fn validated_durable_reference_for_recovery(
        &self,
    ) -> Result<DurableMarketBucketReferenceV1, MarketEventErrorV1> {
        decode_bucket(self)?;
        Ok(self.durable_reference())
    }

    pub(crate) fn hidden(
        bucket_at_utc_ms: i64,
        start_index_position: u64,
        next_index_position: u64,
        closure: MillisecondBucketClosureV1,
        encoded_events: impl Into<Arc<[Vec<u8>]>>,
    ) -> Self {
        Self {
            path: MarketEventPathV1::HiddenPlatform,
            bucket_at_utc_ms,
            start_index_position,
            next_index_position,
            closure,
            encoded_events: encoded_events.into(),
        }
    }

    pub(crate) fn public(
        bucket_at_utc_ms: i64,
        start_index_position: u64,
        next_index_position: u64,
        closure: MillisecondBucketClosureV1,
        encoded_events: impl Into<Arc<[Vec<u8>]>>,
    ) -> Self {
        Self {
            path: MarketEventPathV1::PublicVisibility,
            bucket_at_utc_ms,
            start_index_position,
            next_index_position,
            closure,
            encoded_events: encoded_events.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MarketBucketApplyDispositionV1 {
    Applied,
    PhysicalRetry,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MarketEventErrorV1 {
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
    ResolutionBeforeEnd,
    ResolutionAssetConflict,
    ResolutionConflict(String),
    MarketEvictionConflict(String),
    BucketIdentityConflict,
    InputPathAlreadySealed,
    BucketStartPositionConflict { expected: u64, actual: u64 },
    ExecutionState(String),
    Session(SessionError),
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
            Self::Session(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for MarketEventErrorV1 {}

impl From<SessionError> for MarketEventErrorV1 {
    fn from(value: SessionError) -> Self {
        Self::Session(value)
    }
}

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct AppliedBucketIdentityV1 {
    bucket_at_utc_ms: i64,
    start_index_position: u64,
    next_index_position: u64,
    sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvictedAssetContinuationV1 {
    identity: AssetIdentityV1,
    current_tick_size_pusd_per_share: Option<Decimal>,
    quality: MarketDataQuality,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvictedMarketMarkerV1 {
    market: MarketIdentityV2,
    expected_assets: [AssetIdentityV1; 2],
    qualified_resolution: MarketResolutionTerminalV3,
    mapped_lifecycle_state: Option<MappedMarketLifecycleStateV1>,
    market_quality: MarketDataQuality,
    assets: BTreeMap<String, EvictedAssetContinuationV1>,
}

impl EvictedMarketMarkerV1 {
    fn from_terminal_state(state: &MarketStateV1) -> Result<Self, MarketEventErrorV1> {
        let condition_id = state.market.condition_id.0.clone();
        if state.lifecycle_phase != MarketLifecyclePhaseV1::ResolutionTerminal {
            return Err(MarketEventErrorV1::MarketEvictionConflict(format!(
                "市场尚未进入 ResolutionTerminal：{condition_id}"
            )));
        }
        let qualified_resolution = state.qualified_resolution.clone().ok_or_else(|| {
            MarketEventErrorV1::MarketEvictionConflict(format!(
                "终态市场缺少合格判定：{condition_id}"
            ))
        })?;
        let assets = state
            .assets
            .iter()
            .map(|(token_id, asset)| {
                (
                    token_id.clone(),
                    EvictedAssetContinuationV1 {
                        identity: asset.identity.clone(),
                        current_tick_size_pusd_per_share: asset.current_tick_size_pusd_per_share,
                        quality: asset.quality,
                    },
                )
            })
            .collect();
        Ok(Self {
            market: state.market.clone(),
            expected_assets: state.expected_assets.clone(),
            qualified_resolution,
            mapped_lifecycle_state: state.mapped_lifecycle_state.clone(),
            market_quality: state.market_quality,
            assets,
        })
    }

    fn matches_directory_member(&self, member: &PlatformMarketV1) -> bool {
        self.market == *member.market()
            && self.expected_assets == *member.assets()
            && self.assets.len() == member.assets().len()
            && member.assets().iter().all(|asset| {
                self.assets
                    .get(&asset.token_id.0)
                    .is_some_and(|state| state.identity == *asset)
            })
            && resolution_matches_assets(&self.qualified_resolution, member.assets().iter())
    }

    fn apply_no_effect_event(
        &mut self,
        event: &ExecutionMarketEventV3,
    ) -> Result<(), MarketEventErrorV1> {
        if event.market != self.market {
            return Err(MarketEventErrorV1::MarketIdentityConflict(
                event.market.condition_id.0.clone(),
            ));
        }
        match &event.payload {
            ExecutionMarketEventPayloadV3::MarketLifecycleTransition(transition) => {
                if transition.assets.as_slice() != self.expected_assets.as_slice() {
                    return Err(MarketEventErrorV1::ResolutionAssetConflict);
                }
                if self.mapped_lifecycle_state.is_some()
                    && transition.previous_state != self.mapped_lifecycle_state
                {
                    return Err(MarketEventErrorV1::LifecyclePredecessorConflict);
                }
                self.mapped_lifecycle_state = Some(transition.current_state.clone());
            }
            ExecutionMarketEventPayloadV3::MarketTradingState(_) => {}
            ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) => {
                self.validate_asset(&snapshot.asset)?;
            }
            ExecutionMarketEventPayloadV3::BestBidAsk(best) => {
                self.validate_asset(&best.asset)?;
            }
            ExecutionMarketEventPayloadV3::TickSizeChange(change) => {
                let asset = self.asset_mut(&change.asset)?;
                if let Some(old) = &change.old_tick_size_pusd_per_share {
                    let old = parse_decimal(old)?;
                    if asset.current_tick_size_pusd_per_share != Some(old) {
                        return Err(MarketEventErrorV1::TickPredecessorConflict(
                            change.asset.token_id.0.clone(),
                        ));
                    }
                }
                asset.current_tick_size_pusd_per_share =
                    Some(parse_decimal(&change.new_tick_size_pusd_per_share)?);
            }
            ExecutionMarketEventPayloadV3::PublicTrade(trade) => {
                self.validate_asset(&trade.asset)?;
            }
            ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) => {
                self.validate_asset(&transition.asset)?;
            }
            ExecutionMarketEventPayloadV3::QualityBoundary(boundary) => match &boundary.scope {
                QualityScopeV1::Market => {
                    if self.market_quality != boundary.previous_quality {
                        return Err(MarketEventErrorV1::QualityPredecessorConflict(
                            self.market.condition_id.0.clone(),
                        ));
                    }
                    self.market_quality = boundary.current_quality;
                }
                QualityScopeV1::Asset(identity) => {
                    let asset = self.asset_mut(identity)?;
                    if asset.quality != boundary.previous_quality {
                        return Err(MarketEventErrorV1::QualityPredecessorConflict(
                            identity.token_id.0.clone(),
                        ));
                    }
                    asset.quality = boundary.current_quality;
                }
            },
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(resolution) => {
                if !resolution_matches_assets(resolution, self.expected_assets.iter()) {
                    return Err(MarketEventErrorV1::ResolutionAssetConflict);
                }
                self.market_quality = event.quality.availability;
                let Some(source_at) = event.time.source_event_at_utc_ms else {
                    return Ok(());
                };
                if source_at < self.market.window.end_at_utc_ms {
                    return Err(MarketEventErrorV1::ResolutionBeforeEnd);
                }
                if resolution != &self.qualified_resolution {
                    return Err(MarketEventErrorV1::ResolutionConflict(
                        self.market.condition_id.0.clone(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_asset(&self, asset: &AssetIdentityV1) -> Result<(), MarketEventErrorV1> {
        if self.expected_assets.contains(asset) {
            Ok(())
        } else {
            Err(MarketEventErrorV1::UnknownAsset(asset.token_id.0.clone()))
        }
    }

    fn asset_mut(
        &mut self,
        identity: &AssetIdentityV1,
    ) -> Result<&mut EvictedAssetContinuationV1, MarketEventErrorV1> {
        self.assets
            .get_mut(&identity.token_id.0)
            .filter(|state| state.identity == *identity)
            .ok_or_else(|| MarketEventErrorV1::UnknownAsset(identity.token_id.0.clone()))
    }

    fn copy_continuation_to_terminal_state(
        &self,
        state: &mut MarketStateV1,
    ) -> Result<(), MarketEventErrorV1> {
        state.mapped_lifecycle_state = self.mapped_lifecycle_state.clone();
        state.market_quality = self.market_quality;
        for (token_id, continuation) in &self.assets {
            let asset = state
                .assets
                .get_mut(token_id)
                .filter(|asset| asset.identity == continuation.identity)
                .ok_or_else(|| MarketEventErrorV1::UnknownAsset(token_id.clone()))?;
            asset.current_tick_size_pusd_per_share = continuation.current_tick_size_pusd_per_share;
            asset.quality = continuation.quality;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
struct PreparedMarketEventsV1 {
    hot_markets: BTreeMap<String, MarketStateV1>,
    evicted_markets: BTreeMap<String, EvictedMarketMarkerV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MarketStateStoreV1 {
    markets: BTreeMap<String, MarketStateV1>,
    #[serde(default)]
    evicted_markets: BTreeMap<String, EvictedMarketMarkerV1>,
    last_applied_bucket: Option<AppliedBucketIdentityV1>,
}

impl MarketStateStoreV1 {
    pub(crate) fn lifecycle_phase_recovery_states(
        &self,
    ) -> BTreeMap<String, MarketLifecyclePhaseV1> {
        let mut phases = self
            .markets
            .iter()
            .map(|(condition_id, market)| (condition_id.clone(), market.lifecycle_phase))
            .collect::<BTreeMap<_, _>>();
        phases.extend(
            self.evicted_markets
                .keys()
                .map(|condition_id| (condition_id.clone(), MarketLifecyclePhaseV1::Evicted)),
        );
        phases
    }

    pub(crate) fn has_last_applied_bucket(&self) -> bool {
        self.last_applied_bucket.is_some()
    }

    pub(crate) fn expected_input_seal_outcome(&self) -> InputPathSealOutcomeV1 {
        self.input_seal_outcome(&PreparedMarketEventsV1::default())
    }

    pub(crate) fn last_applied_bucket_matches_durable_reference(
        &self,
        reference: &DurableMarketBucketReferenceV1,
    ) -> bool {
        self.last_applied_bucket.as_ref().is_some_and(|identity| {
            identity.bucket_at_utc_ms == reference.bucket_at_utc_ms
                && identity.start_index_position == reference.start_index_position
                && identity.next_index_position == reference.next_index_position
                && identity.sha256 == reference.bucket_sha256
        })
    }

    pub(crate) fn new(directory: &SessionMarketDirectoryV1) -> Self {
        let markets = directory
            .markets()
            .map(|member| {
                (
                    member.market().condition_id.0.clone(),
                    MarketStateV1::new(member),
                )
            })
            .collect();
        Self {
            markets,
            evicted_markets: BTreeMap::new(),
            last_applied_bucket: None,
        }
    }

    pub(crate) fn insert_market(
        &mut self,
        market: &PlatformMarketV1,
    ) -> Result<(), MarketEventErrorV1> {
        let condition_id = &market.market().condition_id.0;
        if self.markets.contains_key(condition_id)
            || self.evicted_markets.contains_key(condition_id)
        {
            return Err(MarketEventErrorV1::MarketStateAlreadyExists(
                condition_id.clone(),
            ));
        }
        self.markets
            .insert(condition_id.clone(), MarketStateV1::new(market));
        Ok(())
    }

    pub(crate) fn market(&self, condition_id: &str) -> Option<&MarketStateV1> {
        self.markets.get(condition_id)
    }

    pub(crate) fn is_evicted(&self, condition_id: &str) -> bool {
        self.evicted_markets.contains_key(condition_id)
    }

    pub(crate) fn evicted_condition_ids(&self) -> impl Iterator<Item = &str> {
        self.evicted_markets.keys().map(String::as_str)
    }

    pub(crate) fn evict_terminal_market(
        &mut self,
        condition_id: &str,
    ) -> Result<(), MarketEventErrorV1> {
        if self.evicted_markets.contains_key(condition_id) {
            return Err(MarketEventErrorV1::MarketEvictionConflict(format!(
                "市场已经驱逐：{condition_id}"
            )));
        }
        let state = self
            .markets
            .get(condition_id)
            .ok_or_else(|| MarketEventErrorV1::UnknownMarket(condition_id.to_owned()))?;
        let marker = EvictedMarketMarkerV1::from_terminal_state(state)?;
        if marker.market.condition_id.0 != condition_id {
            return Err(MarketEventErrorV1::MarketEvictionConflict(format!(
                "市场映射键与完整状态身份不一致：{condition_id}"
            )));
        }
        self.markets
            .remove(condition_id)
            .expect("已验证热市场必须仍存在");
        let previous = self.evicted_markets.insert(condition_id.to_owned(), marker);
        debug_assert!(previous.is_none());
        Ok(())
    }

    pub(crate) fn validate_recovered(
        &self,
        directory: &SessionMarketDirectoryV1,
    ) -> Result<(), MarketEventErrorV1> {
        if self
            .markets
            .keys()
            .any(|condition_id| self.evicted_markets.contains_key(condition_id))
            || self.markets.len().checked_add(self.evicted_markets.len())
                != Some(directory.markets().len())
        {
            return Err(MarketEventErrorV1::MarketIdentityConflict(
                "恢复市场状态与会话目录成员不一致".to_owned(),
            ));
        }
        for member in directory.markets() {
            let condition_id = &member.market().condition_id.0;
            let valid = if let Some(state) = self.markets.get(condition_id) {
                state.lifecycle_phase != MarketLifecyclePhaseV1::Evicted
                    && &state.market == member.market()
                    && &state.expected_assets == member.assets()
                    && state.assets.len() == member.assets().len()
                    && member.assets().iter().all(|asset| {
                        state
                            .assets
                            .get(&asset.token_id.0)
                            .is_some_and(|state| state.identity == *asset)
                    })
            } else {
                self.evicted_markets
                    .get(condition_id)
                    .is_some_and(|marker| marker.matches_directory_member(member))
            };
            if !valid {
                return Err(MarketEventErrorV1::MarketIdentityConflict(
                    condition_id.clone(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn qualified_resolution_recovery_states(
        &self,
    ) -> BTreeMap<String, (MarketResolutionTerminalV3, MarketLifecyclePhaseV1)> {
        let mut qualified = self
            .markets
            .iter()
            .filter_map(|(condition_id, state)| {
                state.qualified_resolution.as_ref().map(|resolution| {
                    (
                        condition_id.clone(),
                        (resolution.clone(), state.lifecycle_phase),
                    )
                })
            })
            .collect::<BTreeMap<_, _>>();
        qualified.extend(self.evicted_markets.iter().map(|(condition_id, marker)| {
            (
                condition_id.clone(),
                (
                    marker.qualified_resolution.clone(),
                    MarketLifecyclePhaseV1::Evicted,
                ),
            )
        }));
        qualified
    }

    fn mark_resolution_terminal_if_qualified(
        &mut self,
        condition_id: &str,
        resolution: &MarketResolutionTerminalV3,
    ) -> bool {
        if let Some(marker) = self.evicted_markets.get(condition_id) {
            return marker.qualified_resolution == *resolution;
        }
        self.markets
            .get_mut(condition_id)
            .is_some_and(|market| market.mark_resolution_terminal_if_qualified(resolution))
    }

    fn advance_time_through(&mut self, closed_through_utc_ms: i64) {
        for market in self.markets.values_mut() {
            market.advance_time_through(closed_through_utc_ms);
        }
    }

    fn prepare_events(
        &self,
        events: &[ExecutionMarketEventV3],
        path: MarketEventPathV1,
        bucket_at_utc_ms: i64,
    ) -> Result<PreparedMarketEventsV1, MarketEventErrorV1> {
        let mut prepared = PreparedMarketEventsV1::default();
        for event in events {
            let condition_id = event.market.condition_id.0.as_str();
            if let Some(marker) = self.evicted_markets.get(condition_id) {
                if !prepared.evicted_markets.contains_key(condition_id) {
                    prepared
                        .evicted_markets
                        .insert(condition_id.to_owned(), marker.clone());
                }
                prepared
                    .evicted_markets
                    .get_mut(condition_id)
                    .expect("affected evicted market was inserted above")
                    .apply_no_effect_event(event)?;
                continue;
            }
            if !prepared.hot_markets.contains_key(condition_id) {
                let current = self
                    .markets
                    .get(condition_id)
                    .ok_or_else(|| MarketEventErrorV1::UnknownMarket(condition_id.to_owned()))?;
                prepared
                    .hot_markets
                    .insert(condition_id.to_owned(), current.clone());
            }
            let market = prepared
                .hot_markets
                .get_mut(condition_id)
                .expect("affected market was inserted above");
            market.advance_time_through(bucket_at_utc_ms);
            market.apply_event(event, path)?;
        }
        Ok(prepared)
    }

    fn commit_prepared_events(
        &mut self,
        prepared: PreparedMarketEventsV1,
        bucket_at_utc_ms: i64,
        identity: AppliedBucketIdentityV1,
    ) {
        for (condition_id, market) in prepared.hot_markets {
            self.markets.insert(condition_id, market);
        }
        for (condition_id, marker) in prepared.evicted_markets {
            self.evicted_markets.insert(condition_id, marker);
        }
        self.advance_time_through(bucket_at_utc_ms);
        self.last_applied_bucket = Some(identity);
    }

    fn validate_initial_load_complete(
        &self,
        prepared: &PreparedMarketEventsV1,
    ) -> Result<(), MarketEventErrorV1> {
        let incomplete = self
            .markets
            .iter()
            .filter_map(|(condition_id, current)| {
                let candidate = prepared.hot_markets.get(condition_id).unwrap_or(current);
                (!candidate.initial_load_complete()).then(|| condition_id.clone())
            })
            .collect::<Vec<_>>();
        if incomplete.is_empty() {
            Ok(())
        } else {
            Err(MarketEventErrorV1::IncompleteInitialLoad(incomplete))
        }
    }

    fn missing_qualified_terminal_condition_ids(
        &self,
        prepared: &PreparedMarketEventsV1,
    ) -> Vec<String> {
        self.markets
            .iter()
            .filter_map(|(condition_id, current)| {
                let candidate = prepared.hot_markets.get(condition_id).unwrap_or(current);
                (candidate.qualified_resolution().is_none()
                    && candidate.lifecycle_phase() != MarketLifecyclePhaseV1::ResolutionTerminal)
                    .then(|| condition_id.clone())
            })
            .collect()
    }

    fn input_seal_outcome(&self, prepared: &PreparedMarketEventsV1) -> InputPathSealOutcomeV1 {
        let condition_ids = self.missing_qualified_terminal_condition_ids(prepared);
        if condition_ids.is_empty() {
            InputPathSealOutcomeV1::Exhausted
        } else {
            InputPathSealOutcomeV1::FailedMissingQualifiedMarketTerminal { condition_ids }
        }
    }

    fn last_applied_bucket_is(&self, identity: &AppliedBucketIdentityV1) -> bool {
        self.last_applied_bucket.as_ref() == Some(identity)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MarketStateV1 {
    market: MarketIdentityV2,
    expected_assets: [AssetIdentityV1; 2],
    lifecycle_phase: MarketLifecyclePhaseV1,
    mapped_lifecycle_state: Option<MappedMarketLifecycleStateV1>,
    trading_state: MarketTradingStateV2,
    market_quality: MarketDataQuality,
    assets: BTreeMap<String, AssetMarketStateV1>,
    external_state_released: bool,
    qualified_resolution: Option<MarketResolutionTerminalV3>,
    receive_only_resolution_event_id: Option<CanonicalEventId>,
}

impl MarketStateV1 {
    fn new(member: &PlatformMarketV1) -> Self {
        let assets = member
            .assets()
            .iter()
            .map(|asset| {
                (
                    asset.token_id.0.clone(),
                    AssetMarketStateV1::new(asset.clone()),
                )
            })
            .collect();
        Self {
            market: member.market().clone(),
            expected_assets: member.assets().clone(),
            lifecycle_phase: MarketLifecyclePhaseV1::Preloading,
            mapped_lifecycle_state: None,
            trading_state: MarketTradingStateV2 {
                active: None,
                closed: None,
                accepting_orders: None,
            },
            market_quality: MarketDataQuality::UnknownNotObserved,
            assets,
            external_state_released: false,
            qualified_resolution: None,
            receive_only_resolution_event_id: None,
        }
    }

    pub(crate) fn lifecycle_phase(&self) -> MarketLifecyclePhaseV1 {
        self.lifecycle_phase
    }

    pub(crate) fn condition_id(&self) -> &str {
        &self.market.condition_id.0
    }

    pub(crate) fn trading_state(&self) -> &MarketTradingStateV2 {
        &self.trading_state
    }

    pub(crate) fn market_quality(&self) -> MarketDataQuality {
        self.market_quality
    }

    pub(crate) fn end_at_utc_ms(&self) -> i64 {
        self.market.window.end_at_utc_ms
    }

    pub(crate) fn mapped_lifecycle_state(&self) -> Option<&MappedMarketLifecycleStateV1> {
        self.mapped_lifecycle_state.as_ref()
    }

    pub(crate) fn asset(&self, token_id: &str) -> Option<&AssetMarketStateV1> {
        self.assets.get(token_id)
    }

    #[cfg(test)]
    pub(crate) fn external_state_released(&self) -> bool {
        self.external_state_released
    }

    #[cfg(test)]
    pub(crate) fn external_price_level_count(&self) -> usize {
        self.assets
            .values()
            .map(|asset| asset.external_book.bids.len() + asset.external_book.asks.len())
            .sum()
    }

    pub(crate) fn qualified_resolution(&self) -> Option<&MarketResolutionTerminalV3> {
        (self.market_quality == MarketDataQuality::Fresh)
            .then_some(self.qualified_resolution.as_ref())
            .flatten()
    }

    pub(crate) fn receive_only_resolution_event_id(&self) -> Option<&CanonicalEventId> {
        self.receive_only_resolution_event_id.as_ref()
    }

    fn mark_resolution_terminal_if_qualified(
        &mut self,
        resolution: &MarketResolutionTerminalV3,
    ) -> bool {
        if self.qualified_resolution() == Some(resolution) {
            self.lifecycle_phase = MarketLifecyclePhaseV1::ResolutionTerminal;
            true
        } else {
            false
        }
    }

    fn initial_load_complete(&self) -> bool {
        self.assets.values().all(AssetMarketStateV1::is_initialized)
    }

    fn advance_time_through(&mut self, closed_through_utc_ms: i64) {
        if closed_through_utc_ms >= self.market.window.end_at_utc_ms
            && self.lifecycle_phase != MarketLifecyclePhaseV1::Preloading
            && self.initial_load_complete()
        {
            if self.lifecycle_phase == MarketLifecyclePhaseV1::Active {
                self.lifecycle_phase = MarketLifecyclePhaseV1::PostEndPendingResolution;
            }
            self.release_external_state();
        }
    }

    fn release_external_state(&mut self) {
        if self.external_state_released {
            return;
        }
        for asset in self.assets.values_mut() {
            asset.release_external_state();
        }
        self.external_state_released = true;
    }

    fn apply_event(
        &mut self,
        event: &ExecutionMarketEventV3,
        _path: MarketEventPathV1,
    ) -> Result<(), MarketEventErrorV1> {
        if event.market != self.market {
            return Err(MarketEventErrorV1::MarketIdentityConflict(
                event.market.condition_id.0.clone(),
            ));
        }
        if self.lifecycle_phase == MarketLifecyclePhaseV1::ResolutionTerminal {
            return self.apply_terminal_event(event);
        }
        if self.external_state_released {
            match &event.payload {
                ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) => {
                    self.asset_mut(&snapshot.asset)?;
                    return Ok(());
                }
                ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) => {
                    self.asset_mut(&transition.asset)?;
                    return Ok(());
                }
                _ => {}
            }
        }
        match &event.payload {
            ExecutionMarketEventPayloadV3::MarketLifecycleTransition(transition) => {
                if transition.assets.as_slice() != self.expected_assets.as_slice() {
                    return Err(MarketEventErrorV1::ResolutionAssetConflict);
                }
                if self.mapped_lifecycle_state.is_some()
                    && transition.previous_state != self.mapped_lifecycle_state
                {
                    return Err(MarketEventErrorV1::LifecyclePredecessorConflict);
                }
                self.mapped_lifecycle_state = Some(transition.current_state.clone());
            }
            ExecutionMarketEventPayloadV3::MarketTradingState(state) => {
                self.trading_state = state.clone();
            }
            ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) => {
                let asset = self.asset_mut(&snapshot.asset)?;
                if asset.is_initialized() {
                    return Err(MarketEventErrorV1::InitialSnapshotAlreadyLoaded(
                        snapshot.asset.token_id.0.clone(),
                    ));
                }
                asset.load_initial_snapshot(event, snapshot)?;
                if self.initial_load_complete() {
                    self.lifecycle_phase = MarketLifecyclePhaseV1::Active;
                }
            }
            ExecutionMarketEventPayloadV3::BestBidAsk(best) => {
                let asset = self.initialized_asset_mut(&best.asset)?;
                asset.reported_best_bid_ask = Some(best.clone());
            }
            ExecutionMarketEventPayloadV3::TickSizeChange(change) => {
                let asset = self.initialized_asset_mut(&change.asset)?;
                if let Some(old) = &change.old_tick_size_pusd_per_share {
                    let old = parse_decimal(old)?;
                    if asset.current_tick_size_pusd_per_share != Some(old) {
                        return Err(MarketEventErrorV1::TickPredecessorConflict(
                            change.asset.token_id.0.clone(),
                        ));
                    }
                }
                asset.current_tick_size_pusd_per_share =
                    Some(parse_decimal(&change.new_tick_size_pusd_per_share)?);
            }
            ExecutionMarketEventPayloadV3::PublicTrade(trade) => {
                let asset = self.initialized_asset_mut(&trade.asset)?;
                asset.last_public_trade = Some(PublicTradeAuditV1 {
                    trade_id: trade.trade_id.clone(),
                    trade_id_source: trade.trade_id_source,
                });
            }
            ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) => {
                let asset = self.initialized_asset_mut(&transition.asset)?;
                asset.apply_level_transition(transition)?;
            }
            ExecutionMarketEventPayloadV3::QualityBoundary(boundary) => match &boundary.scope {
                QualityScopeV1::Market => {
                    if self.market_quality != boundary.previous_quality {
                        return Err(MarketEventErrorV1::QualityPredecessorConflict(
                            self.market.condition_id.0.clone(),
                        ));
                    }
                    self.market_quality = boundary.current_quality;
                }
                QualityScopeV1::Asset(identity) => {
                    let asset = self.asset_mut(identity)?;
                    if asset.quality != boundary.previous_quality {
                        return Err(MarketEventErrorV1::QualityPredecessorConflict(
                            identity.token_id.0.clone(),
                        ));
                    }
                    if event.quality.resync == ResyncQuality::Required {
                        asset.external_book.clear();
                        asset.reported_best_bid_ask = None;
                    }
                    asset.quality = boundary.current_quality;
                }
            },
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(terminal) => {
                if !self.initial_load_complete() {
                    return Err(MarketEventErrorV1::AssetNotInitialized(
                        self.market.condition_id.0.clone(),
                    ));
                }
                if !resolution_matches_assets(terminal, self.expected_assets.iter()) {
                    return Err(MarketEventErrorV1::ResolutionAssetConflict);
                }
                self.market_quality = event.quality.availability;
                let Some(source_at) = event.time.source_event_at_utc_ms else {
                    self.receive_only_resolution_event_id = Some(event.event_id.clone());
                    return Ok(());
                };
                if source_at < self.market.window.end_at_utc_ms {
                    return Err(MarketEventErrorV1::ResolutionBeforeEnd);
                }
                if self
                    .qualified_resolution
                    .as_ref()
                    .is_some_and(|existing| existing != terminal)
                {
                    return Err(MarketEventErrorV1::ResolutionConflict(
                        self.market.condition_id.0.clone(),
                    ));
                }
                self.qualified_resolution
                    .get_or_insert_with(|| terminal.clone());
                if self.lifecycle_phase != MarketLifecyclePhaseV1::ResolutionTerminal {
                    self.lifecycle_phase = MarketLifecyclePhaseV1::PostEndPendingResolution;
                }
            }
        }
        Ok(())
    }

    fn apply_terminal_event(
        &mut self,
        event: &ExecutionMarketEventV3,
    ) -> Result<(), MarketEventErrorV1> {
        let mut marker = EvictedMarketMarkerV1::from_terminal_state(self)?;
        marker.apply_no_effect_event(event)?;
        marker.copy_continuation_to_terminal_state(self)?;
        match &event.payload {
            ExecutionMarketEventPayloadV3::MarketTradingState(state) => {
                self.trading_state = state.clone();
            }
            ExecutionMarketEventPayloadV3::BestBidAsk(best) => {
                self.initialized_asset_mut(&best.asset)?
                    .reported_best_bid_ask = Some(best.clone());
            }
            ExecutionMarketEventPayloadV3::PublicTrade(trade) => {
                self.initialized_asset_mut(&trade.asset)?.last_public_trade =
                    Some(PublicTradeAuditV1 {
                        trade_id: trade.trade_id.clone(),
                        trade_id_source: trade.trade_id_source,
                    });
            }
            ExecutionMarketEventPayloadV3::QualityBoundary(boundary) => {
                if event.quality.resync == ResyncQuality::Required
                    && let QualityScopeV1::Asset(identity) = &boundary.scope
                {
                    let asset = self.asset_mut(identity)?;
                    asset.external_book.clear();
                    asset.reported_best_bid_ask = None;
                }
            }
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(_)
                if event.time.source_event_at_utc_ms.is_none() =>
            {
                self.receive_only_resolution_event_id = Some(event.event_id.clone());
            }
            ExecutionMarketEventPayloadV3::MarketLifecycleTransition(_)
            | ExecutionMarketEventPayloadV3::InitialL2Snapshot(_)
            | ExecutionMarketEventPayloadV3::TickSizeChange(_)
            | ExecutionMarketEventPayloadV3::LevelExecutionTransition(_)
            | ExecutionMarketEventPayloadV3::MarketResolutionTerminal(_) => {}
        }
        Ok(())
    }

    fn asset_mut(
        &mut self,
        identity: &AssetIdentityV1,
    ) -> Result<&mut AssetMarketStateV1, MarketEventErrorV1> {
        self.assets
            .get_mut(&identity.token_id.0)
            .filter(|state| state.identity == *identity)
            .ok_or_else(|| MarketEventErrorV1::UnknownAsset(identity.token_id.0.clone()))
    }

    fn initialized_asset_mut(
        &mut self,
        identity: &AssetIdentityV1,
    ) -> Result<&mut AssetMarketStateV1, MarketEventErrorV1> {
        let state = self.asset_mut(identity)?;
        if !state.is_initialized() {
            return Err(MarketEventErrorV1::AssetNotInitialized(
                identity.token_id.0.clone(),
            ));
        }
        Ok(state)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AssetMarketStateV1 {
    identity: AssetIdentityV1,
    initial_snapshot_event_id: Option<CanonicalEventId>,
    initial_tick_size_pusd_per_share: Option<Decimal>,
    current_tick_size_pusd_per_share: Option<Decimal>,
    source_sequence_epoch_id: Option<String>,
    source_cursor: Option<u64>,
    book_digest_sha256: Option<Sha256Hex>,
    quality: MarketDataQuality,
    external_book: ExternalBookV1,
    reported_best_bid_ask: Option<BestBidAskV2>,
    last_public_trade: Option<PublicTradeAuditV1>,
}

impl AssetMarketStateV1 {
    fn new(identity: AssetIdentityV1) -> Self {
        Self {
            identity,
            initial_snapshot_event_id: None,
            initial_tick_size_pusd_per_share: None,
            current_tick_size_pusd_per_share: None,
            source_sequence_epoch_id: None,
            source_cursor: None,
            book_digest_sha256: None,
            quality: MarketDataQuality::UnknownNotObserved,
            external_book: ExternalBookV1::default(),
            reported_best_bid_ask: None,
            last_public_trade: None,
        }
    }

    fn is_initialized(&self) -> bool {
        self.initial_snapshot_event_id.is_some()
    }

    fn release_external_state(&mut self) {
        self.external_book.clear();
    }

    pub(crate) fn current_tick_size_pusd_per_share(&self) -> Option<Decimal> {
        self.current_tick_size_pusd_per_share
    }

    pub(crate) fn quality(&self) -> MarketDataQuality {
        self.quality
    }

    pub(crate) fn external_quantity(&self, side: BookSide, price: Decimal) -> Decimal {
        self.external_book.quantity(side, price)
    }

    pub(crate) fn best_external_price(&self, side: BookSide) -> Option<Decimal> {
        self.external_book.best_price(side)
    }

    fn load_initial_snapshot(
        &mut self,
        event: &ExecutionMarketEventV3,
        snapshot: &contracts::InitialL2SnapshotV2,
    ) -> Result<(), MarketEventErrorV1> {
        let tick = parse_decimal(&snapshot.initial_tick_size_pusd_per_share)?;
        self.external_book = ExternalBookV1::from_snapshot(snapshot)?;
        self.initial_snapshot_event_id = Some(event.event_id.clone());
        self.initial_tick_size_pusd_per_share = Some(tick);
        self.current_tick_size_pusd_per_share = Some(tick);
        self.source_sequence_epoch_id = Some(snapshot.source_sequence_epoch_id.clone());
        self.source_cursor = Some(snapshot.source_cursor);
        self.book_digest_sha256 = Some(snapshot.book_digest_sha256.clone());
        self.quality = event.quality.availability;
        Ok(())
    }

    fn apply_level_transition(
        &mut self,
        transition: &contracts::LevelExecutionTransitionV3,
    ) -> Result<(), MarketEventErrorV1> {
        if self.quality != MarketDataQuality::Fresh
            && transition.basis != LevelExecutionBasis::BookResyncDeltaInference
        {
            return Err(MarketEventErrorV1::UnexpectedLevelBasis(
                transition.asset.token_id.0.clone(),
            ));
        }
        let price = parse_decimal(&transition.price_pusd_per_share)?;
        let before = parse_decimal(&transition.quantity_before_shares)?;
        let after = parse_decimal(&transition.quantity_after_shares)?;
        if self.external_book.quantity(transition.side, price) != before {
            return Err(MarketEventErrorV1::LevelPredecessorConflict(format!(
                "{}:{:?}:{price}",
                transition.asset.token_id.0, transition.side
            )));
        }
        self.external_book.set_quantity(
            transition.side,
            price,
            after,
            Some(transition.causal_group_id.clone()),
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct ExternalBookV1 {
    bids: BTreeMap<Decimal, ExternalLevelV1>,
    asks: BTreeMap<Decimal, ExternalLevelV1>,
}

impl ExternalBookV1 {
    fn from_snapshot(
        snapshot: &contracts::InitialL2SnapshotV2,
    ) -> Result<Self, MarketEventErrorV1> {
        let mut book = Self::default();
        for level in &snapshot.bids {
            book.set_quantity(
                BookSide::Bid,
                parse_decimal(&level.price_pusd_per_share)?,
                parse_decimal(&level.quantity_shares)?,
                None,
            );
        }
        for level in &snapshot.asks {
            book.set_quantity(
                BookSide::Ask,
                parse_decimal(&level.price_pusd_per_share)?,
                parse_decimal(&level.quantity_shares)?,
                None,
            );
        }
        Ok(book)
    }

    fn levels(&self, side: BookSide) -> &BTreeMap<Decimal, ExternalLevelV1> {
        match side {
            BookSide::Bid => &self.bids,
            BookSide::Ask => &self.asks,
        }
    }

    fn levels_mut(&mut self, side: BookSide) -> &mut BTreeMap<Decimal, ExternalLevelV1> {
        match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        }
    }

    fn quantity(&self, side: BookSide, price: Decimal) -> Decimal {
        self.levels(side)
            .get(&price)
            .map_or(Decimal::ZERO, |level| level.quantity_shares)
    }

    fn best_price(&self, side: BookSide) -> Option<Decimal> {
        match side {
            BookSide::Bid => self.levels(side).last_key_value(),
            BookSide::Ask => self.levels(side).first_key_value(),
        }
        .map(|(price, _)| *price)
    }

    fn set_quantity(
        &mut self,
        side: BookSide,
        price: Decimal,
        quantity: Decimal,
        causal_group_id: Option<Sha256Hex>,
    ) {
        if quantity.is_zero() {
            self.levels_mut(side).remove(&price);
        } else {
            self.levels_mut(side).insert(
                price,
                ExternalLevelV1 {
                    quantity_shares: quantity,
                    causal_group_id,
                },
            );
        }
    }

    fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ExternalLevelV1 {
    quantity_shares: Decimal,
    causal_group_id: Option<Sha256Hex>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct AccountBookOverlayV1 {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AccountBookOverlayStoreV1 {
    markets: BTreeMap<String, BTreeMap<String, AccountBookOverlayV1>>,
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

    pub(crate) fn add_quantity(
        &mut self,
        condition_id: &str,
        token_id: &str,
        side: BookSide,
        price: Decimal,
        quantity: Decimal,
    ) -> Option<()> {
        let overlay = self.markets.get_mut(condition_id)?.get_mut(token_id)?;
        let next = overlay.quantity(side, price).checked_add(quantity)?;
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
        let next = overlay.quantity(side, price).checked_sub(quantity)?;
        if next.is_sign_negative() {
            return None;
        }
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PublicTradeAuditV1 {
    trade_id: String,
    trade_id_source: PublicTradeIdSourceV1,
}

fn parse_decimal(value: &contracts::ExactDecimal) -> Result<Decimal, MarketEventErrorV1> {
    parse_canonical_decimal(value, "sim-system market state").map_err(|error| {
        MarketEventErrorV1::Decode {
            index: 0,
            message: error.to_string(),
        }
    })
}

fn resolution_matches_assets<'a>(
    terminal: &MarketResolutionTerminalV3,
    expected: impl Iterator<Item = &'a AssetIdentityV1>,
) -> bool {
    let MarketResolutionTerminalV3::Resolved { payout_vector, .. } = terminal;
    payout_vector.iter().map(|entry| &entry.asset).eq(expected)
}

fn decode_bucket(
    bucket: &ClosedMarketEventBucketV1,
) -> Result<(Vec<ExecutionMarketEventV3>, AppliedBucketIdentityV1), MarketEventErrorV1> {
    if bucket.bucket_at_utc_ms <= 0 {
        return Err(MarketEventErrorV1::InvalidBucket("桶时点必须为正"));
    }
    if let MillisecondBucketClosureV1::NextEventAt(next_at) = bucket.closure
        && next_at <= bucket.bucket_at_utc_ms
    {
        return Err(MarketEventErrorV1::InvalidBucket(
            "下一事件时点必须严格晚于桶时点",
        ));
    }
    let event_count = u64::try_from(bucket.encoded_events.len())
        .map_err(|_| MarketEventErrorV1::InvalidBucket("桶内市场事件数量超过索引容量"))?;
    let index_span = bucket
        .next_index_position
        .checked_sub(bucket.start_index_position)
        .ok_or(MarketEventErrorV1::InvalidBucket(
            "下一索引位置早于起始位置",
        ))?;
    if index_span < event_count {
        return Err(MarketEventErrorV1::InvalidBucket(
            "索引位置必须覆盖全部桶内市场事件",
        ));
    }
    let mut event_ids = BTreeSet::new();
    let mut causal_groups = BTreeMap::new();
    let mut events = Vec::with_capacity(bucket.encoded_events.len());
    for (index, bytes) in bucket.encoded_events.iter().enumerate() {
        let event = decode_execution_market_event_v3(bytes).map_err(|error| {
            MarketEventErrorV1::Decode {
                index,
                message: error.to_string(),
            }
        })?;
        let event_at = match bucket.path {
            MarketEventPathV1::HiddenPlatform => event.time.source_event_at_utc_ms,
            MarketEventPathV1::PublicVisibility => Some(event.time.received_at_utc_ms),
        };
        if event_at != Some(bucket.bucket_at_utc_ms) {
            return Err(MarketEventErrorV1::EventOutsideBucket { index });
        }
        if !event_ids.insert(event.event_id.0.clone()) {
            return Err(MarketEventErrorV1::DuplicateEventId(event.event_id.0));
        }
        if let ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) = &event.payload
        {
            let group_facts = (
                event.market.clone(),
                event.time.source_event_at_utc_ms,
                event.time.received_at_utc_ms,
            );
            if let Some(existing) =
                causal_groups.insert(transition.causal_group_id.0.clone(), group_facts.clone())
                && existing != group_facts
            {
                return Err(MarketEventErrorV1::CausalGroupConflict(
                    transition.causal_group_id.0.clone(),
                ));
            }
        }
        events.push(event);
    }
    let sha256 = bucket_sha256(bucket);
    Ok((
        events,
        AppliedBucketIdentityV1 {
            bucket_at_utc_ms: bucket.bucket_at_utc_ms,
            start_index_position: bucket.start_index_position,
            next_index_position: bucket.next_index_position,
            sha256,
        },
    ))
}

fn bucket_sha256(bucket: &ClosedMarketEventBucketV1) -> Sha256Hex {
    let mut hash = Sha256::new();
    hash.update(CLOSED_BUCKET_HASH_DOMAIN_V1);
    hash.update([match bucket.path {
        MarketEventPathV1::HiddenPlatform => 0,
        MarketEventPathV1::PublicVisibility => 1,
    }]);
    hash.update(bucket.bucket_at_utc_ms.to_be_bytes());
    hash.update(bucket.start_index_position.to_be_bytes());
    hash.update(bucket.next_index_position.to_be_bytes());
    for bytes in bucket.encoded_events.iter() {
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
    let mut encoded = String::with_capacity(64);
    for byte in hash.finalize() {
        use fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("写入 String 不会失败");
    }
    Sha256Hex(encoded)
}

impl SimSession {
    pub(crate) fn preview_failed_input_seal(
        &self,
        bucket: &ClosedMarketEventBucketV1,
    ) -> Result<Option<InputPathSealOutcomeV1>, MarketEventErrorV1> {
        if bucket.closure != MillisecondBucketClosureV1::EndOfInput {
            return Ok(None);
        }
        if bucket.path == MarketEventPathV1::HiddenPlatform
            && self.session_spec().caller_role == SimCallerRoleV1::Paper
        {
            return Err(SessionError::HiddenPlatformReplayForbidden.into());
        }
        let (events, identity) = decode_bucket(bucket)?;
        let current_store = match bucket.path {
            MarketEventPathV1::HiddenPlatform => self
                .hidden_platform_markets()
                .expect("非模拟盘会话必须持有隐藏平台状态"),
            MarketEventPathV1::PublicVisibility => self.public_markets(),
        };
        if let Some((failed_path, _)) = self.failed_input_seal()
            && (failed_path != bucket.path || !current_store.last_applied_bucket_is(&identity))
        {
            return Err(MarketEventErrorV1::InputPathAlreadySealed);
        }
        if let Some(outcome) = self.input_path_seal(bucket.path) {
            if !current_store.last_applied_bucket_is(&identity) {
                return Err(MarketEventErrorV1::InputPathAlreadySealed);
            }
            return Ok(matches!(
                outcome,
                InputPathSealOutcomeV1::FailedMissingQualifiedMarketTerminal { .. }
            )
            .then(|| outcome.clone()));
        }
        let prepared = if let Some(previous) = &current_store.last_applied_bucket
            && previous.bucket_at_utc_ms == identity.bucket_at_utc_ms
            && previous.next_index_position == identity.next_index_position
        {
            if previous.start_index_position != identity.start_index_position
                || previous.sha256 != identity.sha256
            {
                return Err(MarketEventErrorV1::BucketIdentityConflict);
            }
            PreparedMarketEventsV1::default()
        } else {
            let expected_start = match bucket.path {
                MarketEventPathV1::HiddenPlatform => self
                    .platform_prefix_cursor()
                    .next_event_time_index_position(),
                MarketEventPathV1::PublicVisibility => self
                    .public_visibility_cursor()
                    .next_visibility_index_position(),
            };
            if bucket.start_index_position != expected_start {
                return Err(MarketEventErrorV1::BucketStartPositionConflict {
                    expected: expected_start,
                    actual: bucket.start_index_position,
                });
            }
            current_store.prepare_events(&events, bucket.path, bucket.bucket_at_utc_ms)?
        };
        current_store.validate_initial_load_complete(&prepared)?;
        let condition_ids = current_store.missing_qualified_terminal_condition_ids(&prepared);
        Ok((!condition_ids.is_empty()).then_some(
            InputPathSealOutcomeV1::FailedMissingQualifiedMarketTerminal { condition_ids },
        ))
    }

    pub(crate) fn mark_redeemed_resolution_terminal(
        &mut self,
        condition_id: &str,
        resolution: &MarketResolutionTerminalV3,
    ) {
        let mut terminalized = false;
        if let Some(hidden_platform_markets) = self.hidden_platform_markets_mut() {
            terminalized |= hidden_platform_markets
                .mark_resolution_terminal_if_qualified(condition_id, resolution);
        }
        terminalized |= self
            .public_markets_mut()
            .mark_resolution_terminal_if_qualified(condition_id, resolution);
        if terminalized {
            self.record_terminal_projection_pending(condition_id);
        }
    }

    pub(crate) fn last_applied_bucket_coordinates_match(
        &self,
        bucket: &ClosedMarketEventBucketV1,
    ) -> bool {
        let current_store = match bucket.path {
            MarketEventPathV1::HiddenPlatform => {
                let Some(store) = self.hidden_platform_markets() else {
                    return false;
                };
                store
            }
            MarketEventPathV1::PublicVisibility => self.public_markets(),
        };
        current_store
            .last_applied_bucket
            .as_ref()
            .is_some_and(|previous| {
                previous.bucket_at_utc_ms == bucket.bucket_at_utc_ms
                    && previous.next_index_position == bucket.next_index_position
            })
    }

    #[cfg(test)]
    pub(crate) fn apply_closed_market_event_bucket(
        &mut self,
        bucket: ClosedMarketEventBucketV1,
    ) -> Result<MarketBucketApplyDispositionV1, MarketEventErrorV1> {
        self.apply_closed_market_event_bucket_with_lifecycle_effects(bucket)
            .map(|(disposition, _)| disposition)
    }

    pub(crate) fn apply_closed_market_event_bucket_with_lifecycle_effects(
        &mut self,
        bucket: ClosedMarketEventBucketV1,
    ) -> Result<
        (
            MarketBucketApplyDispositionV1,
            AuthorizedMarketEventEffectsV1,
        ),
        MarketEventErrorV1,
    > {
        if bucket.path == MarketEventPathV1::HiddenPlatform
            && self.session_spec().caller_role == SimCallerRoleV1::Paper
        {
            return Err(SessionError::HiddenPlatformReplayForbidden.into());
        }
        let (events, identity) = decode_bucket(&bucket)?;
        let current_store = match bucket.path {
            MarketEventPathV1::HiddenPlatform => self
                .hidden_platform_markets()
                .expect("非模拟盘会话必须持有隐藏平台状态"),
            MarketEventPathV1::PublicVisibility => self.public_markets(),
        };
        if let Some((failed_path, _)) = self.failed_input_seal()
            && (failed_path != bucket.path
                || bucket.closure != MillisecondBucketClosureV1::EndOfInput
                || !current_store.last_applied_bucket_is(&identity))
        {
            return Err(MarketEventErrorV1::InputPathAlreadySealed);
        }
        if self.input_path_seal(bucket.path).is_some()
            && (bucket.closure != MillisecondBucketClosureV1::EndOfInput
                || !current_store.last_applied_bucket_is(&identity))
        {
            return Err(MarketEventErrorV1::InputPathAlreadySealed);
        }
        if let Some(previous) = &current_store.last_applied_bucket
            && previous.bucket_at_utc_ms == identity.bucket_at_utc_ms
            && previous.next_index_position == identity.next_index_position
        {
            if previous.start_index_position == identity.start_index_position
                && previous.sha256 == identity.sha256
            {
                if bucket.closure == MillisecondBucketClosureV1::EndOfInput {
                    current_store
                        .validate_initial_load_complete(&PreparedMarketEventsV1::default())?;
                    if self.input_path_seal(bucket.path).is_none() {
                        let outcome = current_store.expected_input_seal_outcome();
                        self.commit_input_path_seal(bucket.path, outcome);
                    }
                }
                return Ok((
                    MarketBucketApplyDispositionV1::PhysicalRetry,
                    AuthorizedMarketEventEffectsV1::default(),
                ));
            }
            return Err(MarketEventErrorV1::BucketIdentityConflict);
        }

        let expected_start = match bucket.path {
            MarketEventPathV1::HiddenPlatform => self
                .platform_prefix_cursor()
                .next_event_time_index_position(),
            MarketEventPathV1::PublicVisibility => self
                .public_visibility_cursor()
                .next_visibility_index_position(),
        };
        if bucket.start_index_position != expected_start {
            return Err(MarketEventErrorV1::BucketStartPositionConflict {
                expected: expected_start,
                actual: bucket.start_index_position,
            });
        }

        let mut prepared =
            current_store.prepare_events(&events, bucket.path, bucket.bucket_at_utc_ms)?;
        if bucket.closure == MillisecondBucketClosureV1::EndOfInput {
            current_store.validate_initial_load_complete(&prepared)?;
        }
        let prepared_input_seal = (bucket.closure == MillisecondBucketClosureV1::EndOfInput)
            .then(|| current_store.input_seal_outcome(&prepared));
        let authorized_events = events
            .iter()
            .filter(|event| !current_store.is_evicted(event.market.condition_id.0.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let qualified_resolution_event_ids = authorized_events
            .iter()
            .filter_map(|event| {
                let ExecutionMarketEventPayloadV3::MarketResolutionTerminal(resolution) =
                    &event.payload
                else {
                    return None;
                };
                let source_at_utc_ms = event.time.source_event_at_utc_ms?;
                let market = prepared.hot_markets.get(&event.market.condition_id.0)?;
                (source_at_utc_ms >= market.end_at_utc_ms()
                    && market.qualified_resolution() == Some(resolution))
                .then(|| event.event_id.0.clone())
            })
            .collect::<BTreeSet<_>>();
        let prepared_input_failed = prepared_input_seal.as_ref().is_some_and(|outcome| {
            matches!(
                outcome,
                InputPathSealOutcomeV1::FailedMissingQualifiedMarketTerminal { .. }
            )
        });
        let authorized_execution_path =
            bucket.drives_execution_for(self.session_spec().caller_role);
        let prepared_execution = if authorized_execution_path && !prepared_input_failed {
            let mut state = self.execution_state().clone();
            let lifecycle_effects = state.apply_authorized_market_events(
                self.market_directory(),
                bucket.bucket_at_utc_ms,
                &authorized_events,
                self.finality_policy(),
                &self
                    .rule_snapshot()
                    .snapshot()
                    .project_account_rules
                    .lifecycle_account_behavior
                    .value,
                &qualified_resolution_event_ids,
            )?;
            let query_projection_changed =
                state.query_projection_changed_from(self.execution_state());
            Some((state, query_projection_changed, lifecycle_effects))
        } else if authorized_execution_path {
            let mut state = self.execution_state().clone();
            state.release_initialized_matching_buffers_through(
                self.market_directory(),
                bucket.bucket_at_utc_ms,
            )?;
            Some((state, false, AuthorizedMarketEventEffectsV1::default()))
        } else {
            None
        };
        let prepared_query_projection_revision = match &prepared_execution {
            Some((_, true, _)) => Some(self.prepare_next_platform_state_revision()?),
            Some((_, false, _)) | None => None,
        };
        let execution_with_current_bucket = prepared_execution
            .as_ref()
            .map(|(state, _, _)| state)
            .unwrap_or_else(|| self.execution_state());
        let mut observed_redeemed_resolutions = Vec::new();
        if !prepared_input_failed {
            for (condition_id, market) in &mut prepared.hot_markets {
                let Some(resolution) =
                    execution_with_current_bucket.redeemed_market_resolution(condition_id)
                else {
                    continue;
                };
                if market.mark_resolution_terminal_if_qualified(resolution) {
                    observed_redeemed_resolutions.push((condition_id.clone(), resolution.clone()));
                }
            }
        }
        match bucket.path {
            MarketEventPathV1::HiddenPlatform => {
                self.validate_platform_prefix_closure(
                    bucket.bucket_at_utc_ms,
                    bucket.next_index_position,
                )?;
                self.hidden_platform_markets_mut()
                    .expect("非模拟盘会话必须持有隐藏平台状态")
                    .commit_prepared_events(prepared, bucket.bucket_at_utc_ms, identity);
                self.commit_platform_prefix_closure(
                    bucket.bucket_at_utc_ms,
                    bucket.next_index_position,
                );
            }
            MarketEventPathV1::PublicVisibility => {
                self.validate_public_visibility_closure(
                    bucket.bucket_at_utc_ms,
                    bucket.next_index_position,
                )?;
                self.public_markets_mut().commit_prepared_events(
                    prepared,
                    bucket.bucket_at_utc_ms,
                    identity,
                );
                self.commit_public_visibility_closure(
                    bucket.bucket_at_utc_ms,
                    bucket.next_index_position,
                );
            }
        }
        let lifecycle_effects = if let Some((state, _, lifecycle_effects)) = prepared_execution {
            if let Some(next_revision) = prepared_query_projection_revision {
                self.commit_prepared_platform_state_revision(next_revision);
            }
            self.replace_execution_state(state);
            lifecycle_effects
        } else {
            AuthorizedMarketEventEffectsV1::default()
        };
        for (condition_id, resolution) in observed_redeemed_resolutions {
            self.mark_redeemed_resolution_terminal(&condition_id, &resolution);
        }
        if let Some(outcome) = prepared_input_seal {
            self.commit_input_path_seal(bucket.path, outcome);
        }
        self.record_successful_market_input();
        Ok((MarketBucketApplyDispositionV1::Applied, lifecycle_effects))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::scenario_for_sim_tests;
    use crate::task21_tests::{
        asset, fresh_quality, hidden_bucket, initial_snapshot_event, orderbook_evidence,
        sealed_event,
    };

    fn prepare_canonical_hidden_events(
        store: &MarketStateStoreV1,
        at_utc_ms: i64,
        next_index_position: u64,
        events: &[ExecutionMarketEventV3],
    ) -> Result<(PreparedMarketEventsV1, AppliedBucketIdentityV1), MarketEventErrorV1> {
        let bucket = hidden_bucket(at_utc_ms, next_index_position, events);
        let (decoded, identity) = decode_bucket(&bucket)?;
        let prepared =
            store.prepare_events(&decoded, MarketEventPathV1::HiddenPlatform, at_utc_ms)?;
        Ok((prepared, identity))
    }

    fn resolved_terminal(
        member: &PlatformMarketV1,
        winning_asset_index: usize,
    ) -> MarketResolutionTerminalV3 {
        let winner = &member.assets()[winning_asset_index];
        MarketResolutionTerminalV3::Resolved {
            winning_asset_id: winner.token_id.0.clone(),
            winning_outcome: winner.outcome.label.clone(),
            payout_vector: member
                .assets()
                .iter()
                .enumerate()
                .map(|(index, asset)| contracts::PayoutEntryV1 {
                    asset: asset.clone(),
                    payout_pusd_per_share: contracts::ExactDecimal(
                        if index == winning_asset_index {
                            "1".to_owned()
                        } else {
                            "0".to_owned()
                        },
                    ),
                })
                .collect(),
            mapping_rule_version: "market-hot-eviction-test-v1".to_owned(),
        }
    }

    fn drive_market_to_resolution_terminal(
        store: &mut MarketStateStoreV1,
        member: &PlatformMarketV1,
    ) -> MarketResolutionTerminalV3 {
        let initial = [
            initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10"),
            initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "10"),
        ];
        let (prepared, identity) =
            prepare_canonical_hidden_events(store, 1_100, 2, &initial).expect("规范初始事件桶");
        store.commit_prepared_events(prepared, 1_100, identity);

        let resolution = resolved_terminal(member, 0);
        let terminal = sealed_event(
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(resolution.clone()),
            orderbook_evidence(2_001, 2_100, 3),
            fresh_quality(),
        );
        let (prepared, identity) =
            prepare_canonical_hidden_events(store, 2_001, 3, &[terminal]).expect("规范终态事件桶");
        store.commit_prepared_events(prepared, 2_001, identity);
        assert!(
            store.mark_resolution_terminal_if_qualified(
                &member.market().condition_id.0,
                &resolution
            )
        );
        resolution
    }

    #[test]
    fn market_state_store_json_round_trip_preserves_every_field() {
        let scenario = scenario_for_sim_tests();
        let directory = SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("测试目录");
        let mut state = MarketStateStoreV1::new(&directory);
        let condition_id = scenario.market_set().markets()[0]
            .market()
            .condition_id
            .0
            .clone();
        let market = state.markets.get_mut(&condition_id).expect("测试市场");
        market.external_state_released = true;
        let asset = market.assets.values_mut().next().expect("测试资产");
        asset.initial_tick_size_pusd_per_share = Some(Decimal::new(1, 2));
        asset.current_tick_size_pusd_per_share = Some(Decimal::new(2, 2));
        asset.external_book.set_quantity(
            BookSide::Bid,
            Decimal::new(4, 1),
            Decimal::new(3, 0),
            Some(Sha256Hex("b".repeat(64))),
        );
        state.last_applied_bucket = Some(AppliedBucketIdentityV1 {
            bucket_at_utc_ms: 1_500,
            start_index_position: 2,
            next_index_position: 4,
            sha256: Sha256Hex("a".repeat(64)),
        });

        let encoded = serde_json::to_vec(&state).expect("市场状态 JSON 编码");
        let decoded: MarketStateStoreV1 =
            serde_json::from_slice(&encoded).expect("市场状态 JSON 解码");

        assert_eq!(decoded, state);
        decoded
            .validate_recovered(&directory)
            .expect("往返市场状态必须重新绑定原目录");

        let mut tampered = serde_json::to_value(&state).expect("市场状态 JSON 值");
        let markets = tampered
            .get_mut("markets")
            .and_then(serde_json::Value::as_object_mut)
            .expect("市场状态映射");
        let extra = markets.values().next().expect("至少一个测试市场").clone();
        markets.insert("extra-condition".to_owned(), extra);
        let tampered: MarketStateStoreV1 =
            serde_json::from_value(tampered).expect("篡改市场状态仍可完成 JSON 解码");
        assert!(matches!(
            tampered.validate_recovered(&directory),
            Err(MarketEventErrorV1::MarketIdentityConflict(_))
        ));
    }

    #[test]
    fn crossing_end_at_releases_market_external_books_and_keeps_identity_shells() {
        let scenario = scenario_for_sim_tests();
        let member = &scenario.market_set().markets()[0];
        let mut market = MarketStateV1::new(member);
        market.lifecycle_phase = MarketLifecyclePhaseV1::Active;
        for (ordinal, asset) in market.assets.values_mut().enumerate() {
            asset.initial_snapshot_event_id = Some(CanonicalEventId(format!(
                "task41-initial-snapshot-{ordinal}"
            )));
            asset
                .external_book
                .set_quantity(BookSide::Bid, Decimal::new(4, 1), Decimal::ONE, None);
            asset
                .external_book
                .set_quantity(BookSide::Ask, Decimal::new(6, 1), Decimal::ONE, None);
        }

        market.advance_time_through(member.market().window.end_at_utc_ms - 1);
        assert!(!market.external_state_released());
        assert_eq!(market.external_price_level_count(), 4);

        market.advance_time_through(member.market().window.end_at_utc_ms);
        assert!(market.external_state_released());
        assert_eq!(market.external_price_level_count(), 0);
        assert_eq!(
            market.lifecycle_phase(),
            MarketLifecyclePhaseV1::PostEndPendingResolution
        );
        assert_eq!(market.assets.len(), 2, "盘后释放必须保留资产身份壳");
    }

    #[test]
    fn end_at_does_not_release_between_lifecycle_and_initial_snapshots() {
        let scenario = scenario_for_sim_tests();
        let member = &scenario.market_set().markets()[0];
        let mut market = MarketStateV1::new(member);
        market.lifecycle_phase = MarketLifecyclePhaseV1::Active;

        market.advance_time_through(member.market().window.end_at_utc_ms);

        assert!(!market.external_state_released());
        assert_eq!(market.lifecycle_phase(), MarketLifecyclePhaseV1::Active);
        assert!(!market.initial_load_complete());
    }

    #[test]
    fn terminal_market_eviction_rejects_nonterminal_and_recovers_from_compact_marker() {
        let scenario = scenario_for_sim_tests();
        let directory = SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("测试目录");
        let member = scenario.market_set().markets().first().expect("测试市场");
        let condition_id = member.market().condition_id.0.clone();
        let mut store = MarketStateStoreV1::new(&directory);

        assert!(matches!(
            store.evict_terminal_market(&condition_id),
            Err(MarketEventErrorV1::MarketEvictionConflict(message))
                if message.contains("ResolutionTerminal")
        ));

        let resolution = drive_market_to_resolution_terminal(&mut store, member);

        store
            .evict_terminal_market(&condition_id)
            .expect("合格终态市场可以驱逐");
        assert!(store.market(&condition_id).is_none());
        assert!(store.is_evicted(&condition_id));
        let marker = store.evicted_markets.get(&condition_id).expect("驱逐标记");
        assert_eq!(&marker.market, member.market());
        assert_eq!(&marker.expected_assets, member.assets());
        assert_eq!(
            store.evicted_condition_ids().collect::<Vec<_>>(),
            [&condition_id]
        );
        assert_eq!(
            store.lifecycle_phase_recovery_states().get(&condition_id),
            Some(&MarketLifecyclePhaseV1::Evicted)
        );
        assert_eq!(
            store
                .qualified_resolution_recovery_states()
                .get(&condition_id),
            Some(&(resolution, MarketLifecyclePhaseV1::Evicted))
        );
        store
            .validate_recovered(&directory)
            .expect("热状态与驱逐标记并集必须精确覆盖目录");

        let encoded = serde_json::to_vec(&store).expect("紧凑市场状态编码");
        let recovered: MarketStateStoreV1 =
            serde_json::from_slice(&encoded).expect("紧凑市场状态解码");
        assert_eq!(recovered, store);
        recovered
            .validate_recovered(&directory)
            .expect("恢复必须仅依赖紧凑驱逐标记");
    }

    #[test]
    fn evicted_market_consumes_valid_late_nonempty_bucket_without_recreating_hot_state() {
        let scenario = scenario_for_sim_tests();
        let directory = SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("测试目录");
        let member = scenario.market_set().markets().first().expect("测试市场");
        let condition_id = member.market().condition_id.0.clone();
        let mut store = MarketStateStoreV1::new(&directory);
        let resolution = drive_market_to_resolution_terminal(&mut store, member);
        store
            .evict_terminal_market(&condition_id)
            .expect("合格终态市场可以驱逐");
        let marker_before = store
            .evicted_markets
            .get(&condition_id)
            .expect("驱逐标记")
            .clone();

        let late_events = [
            sealed_event(
                ExecutionMarketEventPayloadV3::BestBidAsk(BestBidAskV2 {
                    asset: member.assets()[0].clone(),
                    best_bid_pusd_per_share: Some(contracts::ExactDecimal("0.45".to_owned())),
                    best_ask_pusd_per_share: Some(contracts::ExactDecimal("0.55".to_owned())),
                }),
                orderbook_evidence(2_100, 2_200, 4),
                fresh_quality(),
            ),
            sealed_event(
                ExecutionMarketEventPayloadV3::MarketResolutionTerminal(resolution.clone()),
                orderbook_evidence(2_100, 2_200, 5),
                fresh_quality(),
            ),
        ];
        let (prepared, identity) = prepare_canonical_hidden_events(&store, 2_100, 5, &late_events)
            .expect("同身份终态后事件必须可确定性消费");
        assert!(prepared.hot_markets.is_empty(), "驱逐市场不得重建热状态");
        assert_eq!(
            prepared.evicted_markets.keys().collect::<Vec<_>>(),
            [&condition_id]
        );
        let expected_identity = identity.clone();
        store.commit_prepared_events(prepared, 2_100, identity);

        assert!(store.market(&condition_id).is_none());
        assert_eq!(
            store.evicted_markets.get(&condition_id),
            Some(&marker_before)
        );
        assert_eq!(store.last_applied_bucket, Some(expected_identity));
        assert_eq!(
            store
                .qualified_resolution_recovery_states()
                .get(&condition_id),
            Some(&(resolution, MarketLifecyclePhaseV1::Evicted))
        );
    }

    #[test]
    fn terminal_and_evicted_markets_share_late_tick_predecessor_semantics() {
        let scenario = scenario_for_sim_tests();
        let directory = SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("测试目录");
        let member = scenario.market_set().markets().first().expect("测试市场");
        let condition_id = member.market().condition_id.0.clone();
        let mut hot = MarketStateStoreV1::new(&directory);
        drive_market_to_resolution_terminal(&mut hot, member);
        let mut evicted = hot.clone();
        evicted
            .evict_terminal_market(&condition_id)
            .expect("合格终态市场可以驱逐");

        let tick = |source_at_utc_ms: i64, sequence: u64, old: &str, new: &str| {
            sealed_event(
                ExecutionMarketEventPayloadV3::TickSizeChange(contracts::TickSizeChangeV2 {
                    asset: member.assets()[0].clone(),
                    old_tick_size_pusd_per_share: Some(contracts::ExactDecimal(old.to_owned())),
                    new_tick_size_pusd_per_share: contracts::ExactDecimal(new.to_owned()),
                }),
                orderbook_evidence(source_at_utc_ms, source_at_utc_ms + 100, sequence),
                fresh_quality(),
            )
        };

        let invalid = tick(2_050, 4, "0.02", "0.001");
        for store in [&hot, &evicted] {
            assert!(matches!(
                prepare_canonical_hidden_events(store, 2_050, 4, std::slice::from_ref(&invalid)),
                Err(MarketEventErrorV1::TickPredecessorConflict(id)) if id == "1"
            ));
        }

        for (at_utc_ms, sequence, old, new) in
            [(2_100, 4, "0.01", "0.001"), (2_200, 5, "0.001", "0.0001")]
        {
            let event = tick(at_utc_ms, sequence, old, new);
            for store in [&mut hot, &mut evicted] {
                let (prepared, identity) = prepare_canonical_hidden_events(
                    store,
                    at_utc_ms,
                    sequence,
                    std::slice::from_ref(&event),
                )
                .expect("合法终态后 tick 链必须在驱逐前后同样推进");
                store.commit_prepared_events(prepared, at_utc_ms, identity);
            }
        }

        let hot_tick = hot
            .market(&condition_id)
            .and_then(|market| market.asset("1"))
            .and_then(AssetMarketStateV1::current_tick_size_pusd_per_share);
        let evicted_tick = evicted
            .evicted_markets
            .get(&condition_id)
            .and_then(|marker| marker.assets.get("1"))
            .and_then(|asset| asset.current_tick_size_pusd_per_share);
        assert_eq!(hot_tick, Some(Decimal::new(1, 4)));
        assert_eq!(evicted_tick, hot_tick);
        assert!(evicted.market(&condition_id).is_none());
    }

    #[test]
    fn evicted_market_rejects_late_market_asset_and_resolution_conflicts() {
        let scenario = scenario_for_sim_tests();
        let directory = SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("测试目录");
        let member = scenario.market_set().markets().first().expect("测试市场");
        let condition_id = member.market().condition_id.0.clone();
        let mut store = MarketStateStoreV1::new(&directory);
        drive_market_to_resolution_terminal(&mut store, member);
        store
            .evict_terminal_market(&condition_id)
            .expect("合格终态市场可以驱逐");

        let mut wrong_market = sealed_event(
            ExecutionMarketEventPayloadV3::BestBidAsk(BestBidAskV2 {
                asset: member.assets()[0].clone(),
                best_bid_pusd_per_share: Some(contracts::ExactDecimal("0.45".to_owned())),
                best_ask_pusd_per_share: Some(contracts::ExactDecimal("0.55".to_owned())),
            }),
            orderbook_evidence(2_100, 2_200, 4),
            fresh_quality(),
        );
        wrong_market.market.clob_market_id = contracts::ClobMarketId("999".to_owned());
        wrong_market.event_id =
            data_pipeline_core::derive_execution_market_event_v3_id(&wrong_market)
                .expect("冲突市场事件仍须具有规范身份");
        assert!(matches!(
            prepare_canonical_hidden_events(&store, 2_100, 4, &[wrong_market]),
            Err(MarketEventErrorV1::MarketIdentityConflict(_))
        ));

        let wrong_asset = sealed_event(
            ExecutionMarketEventPayloadV3::BestBidAsk(BestBidAskV2 {
                asset: asset("1", 9, "Other"),
                best_bid_pusd_per_share: Some(contracts::ExactDecimal("0.45".to_owned())),
                best_ask_pusd_per_share: Some(contracts::ExactDecimal("0.55".to_owned())),
            }),
            orderbook_evidence(2_100, 2_200, 5),
            fresh_quality(),
        );
        assert!(matches!(
            prepare_canonical_hidden_events(&store, 2_100, 4, &[wrong_asset]),
            Err(MarketEventErrorV1::UnknownAsset(id)) if id == "1"
        ));

        let conflicting_resolution = resolved_terminal(member, 1);
        let conflicting_terminal = sealed_event(
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(conflicting_resolution),
            orderbook_evidence(2_100, 2_200, 6),
            fresh_quality(),
        );
        assert!(matches!(
            prepare_canonical_hidden_events(&store, 2_100, 4, &[conflicting_terminal]),
            Err(MarketEventErrorV1::ResolutionConflict(id)) if id == condition_id
        ));
        assert!(store.market(&condition_id).is_none());
    }
}
