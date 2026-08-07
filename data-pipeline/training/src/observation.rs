//! 从模型可见索引顺序生成门 A 市场侧 Observation。
//!
//! 本模块只维护九个已冻结组件的当前公开状态。同一接收毫秒的事件全部应用后才输出，
//! 不把内部确定性顺序暴露成模型可见的毫秒内中间点。

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use contracts::{
    AssetIdentityV1, BinanceAggregateTradeReferenceV1, BinanceDepthStateReferenceV1, BookSide,
    CANONICAL_EVENT_RECORD_SCHEMA_V1, CANONICAL_REFERENCE_EVENT_SCHEMA_V1, CanonicalEventId,
    CanonicalEventRecordV1, CanonicalReferenceEventPayloadV1, ChainlinkBtcUsdReferenceV1,
    EXECUTION_MARKET_EVENT_SCHEMA_V3, ExactDecimal, ExecutionMarketEventPayloadV3,
    ExecutionMarketEventV3, InitialL2SnapshotV2, MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V3,
    MappedMarketLifecycleStateV1, MarketDataQuality, MarketIdentityV2,
    MarketObservationCausalShellV3, MarketResolutionTerminalV3, MarketTradingStateV2,
    ObservationComponentId, ObservationComponentStateV2, ObservationLineageV3,
    ObservationVisibilityCutoffV2, PriceLevelV1, PublicTradeV2, QualityScopeV1, Sha256Hex,
    TickSizeChangeV2, VisibilityIndexEntryV2,
};
use data_pipeline_core::{
    ContractError, derive_observation_v3_hash, derive_observation_v3_id, parse_canonical_decimal,
    validate_market_observation_causal_shell_v3,
    validate_market_observation_v3_visibility_index_entries,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

const COMPONENT_DIGEST_DOMAIN_V1: &[u8] =
    b"data-pipeline/training/observation-component-state/v1\0";
const MARKET_DIGEST_DOMAIN_V1: &[u8] = b"data-pipeline/training/observation-market-state/v1\0";
const COMPONENT_DIGEST_SCHEMA_V1: &str = "observation_component_state_digest_v1";
const MARKET_DIGEST_SCHEMA_V1: &str = "observation_market_state_digest_v1";

#[derive(Debug)]
pub enum ObservationBuildError {
    Contract(ContractError),
    Json(serde_json::Error),
    UnexpectedMarket {
        expected_condition_id: String,
        actual_condition_id: String,
    },
    ConflictingMarketIdentity,
    AssetBeforeLifecycle(String),
    AssetOutsideMarket(String),
    ConflictingLifecycleAssets,
    UnsupportedQualityBoundary {
        scope: &'static str,
        reason_code: String,
    },
    VisibilityOrder {
        previous_received_at_utc_ms: i64,
        current_received_at_utc_ms: i64,
    },
    RetainedStateLimit {
        observed: usize,
        limit: usize,
    },
}

impl fmt::Display for ObservationBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(error) => write!(formatter, "Observation 合同错误：{error}"),
            Self::Json(error) => write!(formatter, "Observation JSON 错误：{error}"),
            Self::UnexpectedMarket {
                expected_condition_id,
                actual_condition_id,
            } => write!(
                formatter,
                "规范事件市场超出显式覆盖范围：预期 {expected_condition_id}，实际 {actual_condition_id}"
            ),
            Self::ConflictingMarketIdentity => {
                write!(formatter, "同一 condition_id 出现冲突市场身份")
            }
            Self::AssetBeforeLifecycle(token_id) => {
                write!(formatter, "资产事件早于固定生命周期资产集合：{token_id}")
            }
            Self::AssetOutsideMarket(token_id) => {
                write!(formatter, "资产不属于覆盖市场固定资产集合：{token_id}")
            }
            Self::ConflictingLifecycleAssets => {
                write!(formatter, "生命周期事件携带冲突的固定资产集合")
            }
            Self::UnsupportedQualityBoundary { scope, reason_code } => write!(
                formatter,
                "质量边界无法唯一归属 Observation 组件：scope={scope}, reason_code={reason_code}"
            ),
            Self::VisibilityOrder {
                previous_received_at_utc_ms,
                current_received_at_utc_ms,
            } => write!(
                formatter,
                "模型可见输入接收时间倒退：{previous_received_at_utc_ms} -> {current_received_at_utc_ms}"
            ),
            Self::RetainedStateLimit { observed, limit } => write!(
                formatter,
                "Observation 保留的 L2 价位数 {observed} 超过上限 {limit}"
            ),
        }
    }
}

impl std::error::Error for ObservationBuildError {}

impl From<ContractError> for ObservationBuildError {
    fn from(error: ContractError) -> Self {
        Self::Contract(error)
    }
}

impl From<serde_json::Error> for ObservationBuildError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "state_type", content = "state", rename_all = "snake_case")]
enum ComponentDomainState {
    PolymarketL2(Vec<AssetBookState>),
    BestBidAsk(Vec<AssetBestBidAskState>),
    CurrentTick(Vec<AssetTickState>),
    PublicTrade(PublicTradeV2),
    MarketLifecycle(MarketLifecycleState),
    MarketResolution(MarketResolutionTerminalV3),
    ChainlinkBtcUsd(ChainlinkBtcUsdReferenceV1),
    BinanceBtcUsdtDepth(BinanceDepthStateReferenceV1),
    BinanceBtcUsdtTrade(BinanceAggregateTradeReferenceV1),
}

#[derive(Clone, Debug, Serialize)]
struct AssetBookState {
    asset: AssetIdentityV1,
    bids: Vec<PriceLevelV1>,
    asks: Vec<PriceLevelV1>,
}

#[derive(Clone, Debug, Serialize)]
struct AssetBestBidAskState {
    asset: AssetIdentityV1,
    best_bid_pusd_per_share: Option<ExactDecimal>,
    best_ask_pusd_per_share: Option<ExactDecimal>,
}

#[derive(Clone, Debug, Serialize)]
struct AssetTickState {
    asset: AssetIdentityV1,
    tick_size_pusd_per_share: ExactDecimal,
}

#[derive(Clone, Debug, Default, Serialize)]
struct MarketLifecycleState {
    current_state: Option<MappedMarketLifecycleStateV1>,
    assets: Vec<AssetIdentityV1>,
    trading_state: Option<MarketTradingStateV2>,
}

#[derive(Clone, Debug)]
struct EventAnchor {
    event_id: CanonicalEventId,
    source_event_at_utc_ms: Option<i64>,
    received_at_utc_ms: i64,
}

#[derive(Clone, Debug)]
struct ComponentSlot {
    component: ObservationComponentId,
    quality: MarketDataQuality,
    state: Option<ComponentDomainState>,
    anchor: Option<EventAnchor>,
    state_digest_sha256: Option<Sha256Hex>,
}

impl ComponentSlot {
    fn unknown(component: ObservationComponentId) -> Self {
        Self {
            component,
            quality: MarketDataQuality::UnknownNotObserved,
            state: None,
            anchor: None,
            state_digest_sha256: None,
        }
    }

    fn update(
        &mut self,
        quality: MarketDataQuality,
        state: Option<ComponentDomainState>,
        anchor: &EventAnchor,
    ) -> Result<(), ObservationBuildError> {
        self.quality = quality;
        if is_visible(quality) {
            let state = state.ok_or_else(|| ContractError::InvalidField {
                field: format!("component.{:?}", self.component),
                reason: "可见组件缺少当前领域状态".to_owned(),
            })?;
            self.state = Some(state);
            self.anchor = Some(anchor.clone());
            self.state_digest_sha256 = None;
        } else {
            self.state = state;
            self.anchor = None;
            self.state_digest_sha256 = None;
        }
        Ok(())
    }

    fn seal(&mut self) -> Result<(), ObservationBuildError> {
        if !is_visible(self.quality) {
            self.state_digest_sha256 = None;
            return Ok(());
        }
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| ContractError::InvalidField {
                field: format!("component.{:?}", self.component),
                reason: "可见组件缺少当前领域状态".to_owned(),
            })?;
        let anchor = self
            .anchor
            .as_ref()
            .ok_or_else(|| ContractError::InvalidField {
                field: format!("component.{:?}", self.component),
                reason: "可见组件缺少当前事件锚点".to_owned(),
            })?;
        self.state_digest_sha256 = Some(component_state_digest(
            self.component,
            state,
            self.quality,
            &anchor.event_id,
        )?);
        Ok(())
    }

    fn public_state(&self, cutoff: i64) -> ObservationComponentStateV2 {
        let Some(anchor) = self.anchor.as_ref() else {
            return ObservationComponentStateV2 {
                component: self.component,
                quality: self.quality,
                source_event_at_utc_ms: None,
                received_at_utc_ms: None,
                information_age_ms: None,
                observed_transport_delay_ms: None,
                last_visible_event_id: None,
                state_digest_sha256: None,
            };
        };
        let information_age_ms = anchor
            .source_event_at_utc_ms
            .and_then(|source| cutoff.checked_sub(source))
            .and_then(|delta| u64::try_from(delta).ok());
        let observed_transport_delay_ms = anchor
            .source_event_at_utc_ms
            .and_then(|source| anchor.received_at_utc_ms.checked_sub(source))
            .and_then(|delta| u64::try_from(delta).ok());
        ObservationComponentStateV2 {
            component: self.component,
            quality: self.quality,
            source_event_at_utc_ms: anchor.source_event_at_utc_ms,
            received_at_utc_ms: Some(anchor.received_at_utc_ms),
            information_age_ms,
            observed_transport_delay_ms,
            last_visible_event_id: Some(anchor.event_id.clone()),
            state_digest_sha256: self.state_digest_sha256.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct AssetComponent<T> {
    value: T,
    quality: MarketDataQuality,
}

#[derive(Clone, Debug)]
struct MarketObservationState {
    market: MarketIdentityV2,
    expected_assets: Option<Vec<AssetIdentityV1>>,
    books: BTreeMap<String, AssetComponent<AssetBookState>>,
    bba: BTreeMap<String, AssetComponent<AssetBestBidAskState>>,
    ticks: BTreeMap<String, AssetComponent<AssetTickState>>,
    lifecycle: MarketLifecycleState,
    components: BTreeMap<ObservationComponentId, ComponentSlot>,
    dirty_components: BTreeMap<ObservationComponentId, EventAnchor>,
    max_retained_l2_levels: usize,
    peak_retained_l2_levels: usize,
}

impl MarketObservationState {
    fn new(market: MarketIdentityV2, max_retained_l2_levels: usize) -> Self {
        let components = data_pipeline_core::REQUIRED_OBSERVATION_COMPONENTS
            .into_iter()
            .map(|component| (component, ComponentSlot::unknown(component)))
            .collect();
        Self {
            market,
            expected_assets: None,
            books: BTreeMap::new(),
            bba: BTreeMap::new(),
            ticks: BTreeMap::new(),
            lifecycle: MarketLifecycleState::default(),
            components,
            dirty_components: BTreeMap::new(),
            max_retained_l2_levels,
            peak_retained_l2_levels: 0,
        }
    }

    fn apply_event(&mut self, event: &ExecutionMarketEventV3) -> Result<(), ObservationBuildError> {
        let anchor = EventAnchor {
            event_id: event.event_id.clone(),
            source_event_at_utc_ms: event.time.source_event_at_utc_ms,
            received_at_utc_ms: event.time.received_at_utc_ms,
        };
        match &event.payload {
            ExecutionMarketEventPayloadV3::MarketLifecycleTransition(transition) => {
                self.set_expected_assets(&transition.assets)?;
                self.lifecycle.current_state = Some(transition.current_state.clone());
                self.lifecycle.assets = transition.assets.clone();
                self.update_slot(
                    ObservationComponentId::MarketLifecycle,
                    event.quality.availability,
                    Some(ComponentDomainState::MarketLifecycle(
                        self.lifecycle.clone(),
                    )),
                    &anchor,
                )?;
            }
            ExecutionMarketEventPayloadV3::MarketTradingState(trading) => {
                self.lifecycle.trading_state = Some(trading.clone());
                self.update_slot(
                    ObservationComponentId::MarketLifecycle,
                    event.quality.availability,
                    Some(ComponentDomainState::MarketLifecycle(
                        self.lifecycle.clone(),
                    )),
                    &anchor,
                )?;
            }
            ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) => {
                self.require_asset(&snapshot.asset)?;
                self.apply_initial_snapshot(snapshot, event.quality.availability);
                self.check_retained_l2_levels()?;
                self.mark_dirty(ObservationComponentId::PolymarketL2, &anchor);
                self.mark_dirty(ObservationComponentId::CurrentTick, &anchor);
            }
            ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) => {
                self.require_asset(&transition.asset)?;
                self.apply_level_transition(transition)?;
                self.check_retained_l2_levels()?;
                self.mark_dirty(ObservationComponentId::PolymarketL2, &anchor);
            }
            ExecutionMarketEventPayloadV3::BestBidAsk(best) => {
                self.require_asset(&best.asset)?;
                self.bba.insert(
                    best.asset.token_id.0.clone(),
                    AssetComponent {
                        value: AssetBestBidAskState {
                            asset: best.asset.clone(),
                            best_bid_pusd_per_share: best.best_bid_pusd_per_share.clone(),
                            best_ask_pusd_per_share: best.best_ask_pusd_per_share.clone(),
                        },
                        quality: event.quality.availability,
                    },
                );
                self.mark_dirty(ObservationComponentId::BestBidAsk, &anchor);
            }
            ExecutionMarketEventPayloadV3::TickSizeChange(change) => {
                self.require_asset(&change.asset)?;
                self.apply_tick_change(change, event.quality.availability);
                self.mark_dirty(ObservationComponentId::CurrentTick, &anchor);
            }
            ExecutionMarketEventPayloadV3::PublicTrade(trade) => {
                self.require_asset(&trade.asset)?;
                self.update_slot(
                    ObservationComponentId::PublicTrade,
                    event.quality.availability,
                    Some(ComponentDomainState::PublicTrade(trade.clone())),
                    &anchor,
                )?;
            }
            ExecutionMarketEventPayloadV3::QualityBoundary(boundary) => match &boundary.scope {
                QualityScopeV1::Asset(asset)
                    if valid_l2_quality_boundary(event, boundary.reason_code.as_str()) =>
                {
                    self.require_asset(asset)?;
                    let book = self.books.get_mut(&asset.token_id.0).ok_or_else(|| {
                        ObservationBuildError::AssetBeforeLifecycle(asset.token_id.0.clone())
                    })?;
                    if boundary.reason_code == "POLYMARKET_L2_EPOCH_BASELINE_REQUIRED" {
                        book.value.bids.clear();
                        book.value.asks.clear();
                    }
                    book.quality = boundary.current_quality;
                    self.check_retained_l2_levels()?;
                    self.mark_dirty(ObservationComponentId::PolymarketL2, &anchor);
                }
                QualityScopeV1::Market
                    if boundary.current_quality == MarketDataQuality::Invalid
                        && is_terminal_conflict_reason(&boundary.reason_code) =>
                {
                    let current = self
                        .components
                        .get(&ObservationComponentId::MarketResolution)
                        .and_then(|slot| slot.state.clone());
                    self.update_slot(
                        ObservationComponentId::MarketResolution,
                        boundary.current_quality,
                        current,
                        &anchor,
                    )?;
                }
                QualityScopeV1::Asset(_) => {
                    return Err(ObservationBuildError::UnsupportedQualityBoundary {
                        scope: "asset",
                        reason_code: boundary.reason_code.clone(),
                    });
                }
                QualityScopeV1::Market => {
                    return Err(ObservationBuildError::UnsupportedQualityBoundary {
                        scope: "market",
                        reason_code: boundary.reason_code.clone(),
                    });
                }
            },
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(terminal) => {
                self.update_slot(
                    ObservationComponentId::MarketResolution,
                    event.quality.availability,
                    Some(ComponentDomainState::MarketResolution(terminal.clone())),
                    &anchor,
                )?;
            }
        }
        Ok(())
    }

    fn set_expected_assets(
        &mut self,
        assets: &[AssetIdentityV1],
    ) -> Result<(), ObservationBuildError> {
        match &self.expected_assets {
            Some(expected) if expected != assets => {
                return Err(ObservationBuildError::ConflictingLifecycleAssets);
            }
            Some(_) => {}
            None => self.expected_assets = Some(assets.to_vec()),
        }
        Ok(())
    }

    fn require_asset(&self, asset: &AssetIdentityV1) -> Result<(), ObservationBuildError> {
        let expected = self
            .expected_assets
            .as_ref()
            .ok_or_else(|| ObservationBuildError::AssetBeforeLifecycle(asset.token_id.0.clone()))?;
        if expected.iter().any(|candidate| candidate == asset) {
            Ok(())
        } else {
            Err(ObservationBuildError::AssetOutsideMarket(
                asset.token_id.0.clone(),
            ))
        }
    }

    fn apply_initial_snapshot(
        &mut self,
        snapshot: &InitialL2SnapshotV2,
        quality: MarketDataQuality,
    ) {
        self.books.insert(
            snapshot.asset.token_id.0.clone(),
            AssetComponent {
                value: AssetBookState {
                    asset: snapshot.asset.clone(),
                    bids: snapshot.bids.clone(),
                    asks: snapshot.asks.clone(),
                },
                quality,
            },
        );
        self.ticks.insert(
            snapshot.asset.token_id.0.clone(),
            AssetComponent {
                value: AssetTickState {
                    asset: snapshot.asset.clone(),
                    tick_size_pusd_per_share: snapshot.initial_tick_size_pusd_per_share.clone(),
                },
                quality,
            },
        );
    }

    fn apply_level_transition(
        &mut self,
        transition: &contracts::LevelExecutionTransitionV3,
    ) -> Result<(), ObservationBuildError> {
        let book = self
            .books
            .get_mut(&transition.asset.token_id.0)
            .ok_or_else(|| {
                ObservationBuildError::AssetBeforeLifecycle(transition.asset.token_id.0.clone())
            })?;
        let levels = match transition.side {
            BookSide::Bid => &mut book.value.bids,
            BookSide::Ask => &mut book.value.asks,
        };
        replace_level(
            levels,
            &transition.price_pusd_per_share,
            &transition.quantity_after_shares,
            transition.side,
        )?;
        Ok(())
    }

    fn apply_tick_change(&mut self, change: &TickSizeChangeV2, quality: MarketDataQuality) {
        self.ticks.insert(
            change.asset.token_id.0.clone(),
            AssetComponent {
                value: AssetTickState {
                    asset: change.asset.clone(),
                    tick_size_pusd_per_share: change.new_tick_size_pusd_per_share.clone(),
                },
                quality,
            },
        );
    }

    fn refresh_books(&mut self, anchor: &EventAnchor) -> Result<(), ObservationBuildError> {
        let (quality, complete) = aggregate_asset_quality(&self.expected_assets, &self.books);
        let state = complete.then(|| {
            ComponentDomainState::PolymarketL2(
                ordered_values(&self.expected_assets, &self.books)
                    .into_iter()
                    .map(|entry| entry.value.clone())
                    .collect(),
            )
        });
        self.update_slot_untracked(ObservationComponentId::PolymarketL2, quality, state, anchor)
    }

    fn refresh_bba(&mut self, anchor: &EventAnchor) -> Result<(), ObservationBuildError> {
        let (quality, complete) = aggregate_asset_quality(&self.expected_assets, &self.bba);
        let state = complete.then(|| {
            ComponentDomainState::BestBidAsk(
                ordered_values(&self.expected_assets, &self.bba)
                    .into_iter()
                    .map(|entry| entry.value.clone())
                    .collect(),
            )
        });
        self.update_slot_untracked(ObservationComponentId::BestBidAsk, quality, state, anchor)
    }

    fn refresh_ticks(&mut self, anchor: &EventAnchor) -> Result<(), ObservationBuildError> {
        let (quality, complete) = aggregate_asset_quality(&self.expected_assets, &self.ticks);
        let state = complete.then(|| {
            ComponentDomainState::CurrentTick(
                ordered_values(&self.expected_assets, &self.ticks)
                    .into_iter()
                    .map(|entry| entry.value.clone())
                    .collect(),
            )
        });
        self.update_slot_untracked(ObservationComponentId::CurrentTick, quality, state, anchor)
    }

    fn update_slot(
        &mut self,
        component: ObservationComponentId,
        quality: MarketDataQuality,
        state: Option<ComponentDomainState>,
        anchor: &EventAnchor,
    ) -> Result<(), ObservationBuildError> {
        self.update_slot_untracked(component, quality, state, anchor)?;
        self.mark_dirty(component, anchor);
        Ok(())
    }

    fn update_slot_untracked(
        &mut self,
        component: ObservationComponentId,
        quality: MarketDataQuality,
        state: Option<ComponentDomainState>,
        anchor: &EventAnchor,
    ) -> Result<(), ObservationBuildError> {
        self.components
            .get_mut(&component)
            .expect("九组件初始化完整")
            .update(quality, state, anchor)
    }

    fn mark_dirty(&mut self, component: ObservationComponentId, anchor: &EventAnchor) {
        self.dirty_components.insert(component, anchor.clone());
    }

    fn materialize_dirty_components(&mut self) -> Result<(), ObservationBuildError> {
        let dirty = std::mem::take(&mut self.dirty_components);
        for (component, anchor) in dirty {
            match component {
                ObservationComponentId::PolymarketL2 => self.refresh_books(&anchor)?,
                ObservationComponentId::BestBidAsk => self.refresh_bba(&anchor)?,
                ObservationComponentId::CurrentTick => self.refresh_ticks(&anchor)?,
                _ => {}
            }
            self.components
                .get_mut(&component)
                .expect("九组件初始化完整")
                .seal()?;
        }
        Ok(())
    }

    fn check_retained_l2_levels(&mut self) -> Result<(), ObservationBuildError> {
        let retained = self.books.values().try_fold(0usize, |total, book| {
            total
                .checked_add(book.value.bids.len())
                .and_then(|value| value.checked_add(book.value.asks.len()))
        });
        let retained = retained.unwrap_or(usize::MAX);
        self.peak_retained_l2_levels = self.peak_retained_l2_levels.max(retained);
        if retained > self.max_retained_l2_levels {
            return Err(ObservationBuildError::RetainedStateLimit {
                observed: retained,
                limit: self.max_retained_l2_levels,
            });
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct ProjectedObservation {
    pub observation: MarketObservationCausalShellV3,
}

/// 单个显式覆盖市场的有状态 Observation 投影器。
pub(crate) struct ObservationProjector {
    condition_id: String,
    input_set_identity_sha256: Sha256Hex,
    visibility_index_manifest_sha256: Sha256Hex,
    current_received_at_utc_ms: Option<i64>,
    changed_in_current_millisecond: bool,
    market_state: Option<MarketObservationState>,
    global_components: BTreeMap<ObservationComponentId, ComponentSlot>,
    dirty_global_components: BTreeSet<ObservationComponentId>,
    max_retained_l2_levels: usize,
}

impl ObservationProjector {
    pub(crate) fn new(
        condition_id: impl Into<String>,
        input_set_identity_sha256: Sha256Hex,
        visibility_index_manifest_sha256: Sha256Hex,
        max_retained_l2_levels: usize,
    ) -> Self {
        let global_components = [
            ObservationComponentId::ChainlinkBtcUsd,
            ObservationComponentId::BinanceBtcUsdtDepth,
            ObservationComponentId::BinanceBtcUsdtTrade,
        ]
        .into_iter()
        .map(|component| (component, ComponentSlot::unknown(component)))
        .collect();
        Self {
            condition_id: condition_id.into(),
            input_set_identity_sha256,
            visibility_index_manifest_sha256,
            current_received_at_utc_ms: None,
            changed_in_current_millisecond: false,
            market_state: None,
            global_components,
            dirty_global_components: BTreeSet::new(),
            max_retained_l2_levels,
        }
    }

    pub(crate) fn push(
        &mut self,
        record: &CanonicalEventRecordV1,
    ) -> Result<Option<ProjectedObservation>, ObservationBuildError> {
        let received_at = record_received_at(record);
        let completed = match self.current_received_at_utc_ms {
            Some(previous) if received_at < previous => {
                return Err(ObservationBuildError::VisibilityOrder {
                    previous_received_at_utc_ms: previous,
                    current_received_at_utc_ms: received_at,
                });
            }
            Some(previous) if received_at > previous => {
                let observation = self.finish_millisecond(previous)?;
                self.current_received_at_utc_ms = Some(received_at);
                observation
            }
            Some(_) => None,
            None => {
                self.current_received_at_utc_ms = Some(received_at);
                None
            }
        };

        self.apply_record(record)?;
        self.changed_in_current_millisecond = true;
        Ok(completed)
    }

    pub(crate) fn finish(&mut self) -> Result<Option<ProjectedObservation>, ObservationBuildError> {
        let Some(cutoff) = self.current_received_at_utc_ms else {
            return Ok(None);
        };
        self.finish_millisecond(cutoff)
    }

    fn apply_record(
        &mut self,
        record: &CanonicalEventRecordV1,
    ) -> Result<(), ObservationBuildError> {
        match record {
            CanonicalEventRecordV1::ExecutionMarketEvent(event) => {
                let actual = &event.market.condition_id.0;
                if actual != &self.condition_id {
                    return Err(ObservationBuildError::UnexpectedMarket {
                        expected_condition_id: self.condition_id.clone(),
                        actual_condition_id: actual.clone(),
                    });
                }
                match &mut self.market_state {
                    Some(state) if state.market != event.market => {
                        return Err(ObservationBuildError::ConflictingMarketIdentity);
                    }
                    Some(state) => state.apply_event(event)?,
                    None => {
                        let mut state = MarketObservationState::new(
                            event.market.clone(),
                            self.max_retained_l2_levels,
                        );
                        state.apply_event(event)?;
                        self.market_state = Some(state);
                    }
                }
            }
            CanonicalEventRecordV1::ReferenceEvent(event) => {
                let (component, state) = match &event.payload {
                    CanonicalReferenceEventPayloadV1::ChainlinkBtcUsd(value) => (
                        ObservationComponentId::ChainlinkBtcUsd,
                        ComponentDomainState::ChainlinkBtcUsd(value.clone()),
                    ),
                    CanonicalReferenceEventPayloadV1::BinanceDepthState(value) => (
                        ObservationComponentId::BinanceBtcUsdtDepth,
                        ComponentDomainState::BinanceBtcUsdtDepth(value.clone()),
                    ),
                    CanonicalReferenceEventPayloadV1::BinanceAggregateTrade(value) => (
                        ObservationComponentId::BinanceBtcUsdtTrade,
                        ComponentDomainState::BinanceBtcUsdtTrade(value.clone()),
                    ),
                };
                let anchor = EventAnchor {
                    event_id: event.event_id.clone(),
                    source_event_at_utc_ms: event.time.source_event_at_utc_ms,
                    received_at_utc_ms: event.time.received_at_utc_ms,
                };
                self.global_components
                    .get_mut(&component)
                    .expect("三个全局参考组件初始化完整")
                    .update(event.quality.availability, Some(state), &anchor)?;
                self.dirty_global_components.insert(component);
            }
        }
        Ok(())
    }

    fn finish_millisecond(
        &mut self,
        cutoff: i64,
    ) -> Result<Option<ProjectedObservation>, ObservationBuildError> {
        if !self.changed_in_current_millisecond {
            return Ok(None);
        }
        self.changed_in_current_millisecond = false;
        if let Some(market_state) = self.market_state.as_mut() {
            market_state.materialize_dirty_components()?;
        }
        for component in std::mem::take(&mut self.dirty_global_components) {
            self.global_components
                .get_mut(&component)
                .expect("三个全局参考组件初始化完整")
                .seal()?;
        }
        let Some(market_state) = self.market_state.as_ref() else {
            return Ok(None);
        };

        let mut component_states = Vec::with_capacity(9);
        let mut missing_mask = Vec::new();
        let mut visibility_by_id = BTreeMap::new();
        for component in data_pipeline_core::REQUIRED_OBSERVATION_COMPONENTS {
            let slot = if matches!(
                component,
                ObservationComponentId::ChainlinkBtcUsd
                    | ObservationComponentId::BinanceBtcUsdtDepth
                    | ObservationComponentId::BinanceBtcUsdtTrade
            ) {
                self.global_components
                    .get(&component)
                    .expect("三个全局参考组件初始化完整")
            } else {
                market_state
                    .components
                    .get(&component)
                    .expect("市场九组件初始化完整")
            };
            let state = slot.public_state(cutoff);
            if !is_visible(state.quality) {
                missing_mask.push(component);
            }
            if let (Some(event_id), Some(received_at_utc_ms)) =
                (&state.last_visible_event_id, state.received_at_utc_ms)
            {
                visibility_by_id.insert(
                    event_id.0.clone(),
                    (
                        event_id.clone(),
                        VisibilityIndexEntryV2 {
                            schema_version: contracts::VISIBILITY_INDEX_SCHEMA_V2.to_owned(),
                            event_id: event_id.clone(),
                            received_at_utc_ms,
                        },
                    ),
                );
            }
            component_states.push(state);
        }
        let contributing_event_ids = visibility_by_id
            .values()
            .map(|(event_id, _)| event_id.clone())
            .collect::<Vec<_>>();
        let market_state_digest_sha256 =
            market_state_digest(&market_state.market, &component_states, &missing_mask)?;
        let mut observation = MarketObservationCausalShellV3 {
            schema_version: MARKET_OBSERVATION_CAUSAL_SHELL_SCHEMA_V3.to_owned(),
            observation_id: Sha256Hex("0".repeat(64)),
            market: market_state.market.clone(),
            lineage: ObservationLineageV3 {
                canonical_event_record_contract_version: CANONICAL_EVENT_RECORD_SCHEMA_V1
                    .to_owned(),
                execution_event_contract_version: EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
                reference_event_contract_version: CANONICAL_REFERENCE_EVENT_SCHEMA_V1.to_owned(),
                input_set_identity_sha256: self.input_set_identity_sha256.clone(),
                visibility_index_manifest_sha256: self.visibility_index_manifest_sha256.clone(),
                contributing_event_ids,
            },
            visibility_cutoff: ObservationVisibilityCutoffV2 {
                received_at_utc_ms: cutoff,
            },
            market_state_digest_sha256,
            component_states,
            missing_mask,
            canonical_hash_sha256: Sha256Hex("0".repeat(64)),
        };
        observation.observation_id = derive_observation_v3_id(&observation)?;
        observation.canonical_hash_sha256 = derive_observation_v3_hash(&observation)?;
        validate_market_observation_causal_shell_v3(&observation)?;
        let contributing_visibility_entries = observation
            .lineage
            .contributing_event_ids
            .iter()
            .map(|event_id| {
                visibility_by_id
                    .remove(&event_id.0)
                    .map(|(_, entry)| entry)
                    .expect("贡献事件同时来自当前组件锚点")
            })
            .collect::<Vec<_>>();
        validate_market_observation_v3_visibility_index_entries(
            &observation,
            &contributing_visibility_entries,
        )?;
        Ok(Some(ProjectedObservation { observation }))
    }

    pub(crate) fn peak_retained_l2_levels(&self) -> usize {
        self.market_state
            .as_ref()
            .map_or(0, |state| state.peak_retained_l2_levels)
    }
}

fn is_visible(quality: MarketDataQuality) -> bool {
    matches!(quality, MarketDataQuality::Fresh | MarketDataQuality::Stale)
}

fn record_received_at(record: &CanonicalEventRecordV1) -> i64 {
    match record {
        CanonicalEventRecordV1::ExecutionMarketEvent(event) => event.time.received_at_utc_ms,
        CanonicalEventRecordV1::ReferenceEvent(event) => event.time.received_at_utc_ms,
    }
}

fn valid_l2_quality_boundary(event: &ExecutionMarketEventV3, reason_code: &str) -> bool {
    let ExecutionMarketEventPayloadV3::QualityBoundary(boundary) = &event.payload else {
        return false;
    };
    match reason_code {
        "POLYMARKET_L2_EPOCH_BASELINE_REQUIRED" => {
            boundary.previous_quality == MarketDataQuality::Fresh
                && boundary.current_quality == MarketDataQuality::Missing
                && event.quality.availability == MarketDataQuality::Missing
                && event.quality.resync == contracts::ResyncQuality::Required
        }
        "POLYMARKET_L2_EPOCH_BASELINE_RESTORED" => {
            boundary.previous_quality == MarketDataQuality::Missing
                && boundary.current_quality == MarketDataQuality::Fresh
                && event.quality.availability == MarketDataQuality::Fresh
                && event.quality.resync == contracts::ResyncQuality::ResyncBoundary
        }
        _ => false,
    }
}

fn is_terminal_conflict_reason(reason_code: &str) -> bool {
    matches!(
        reason_code,
        "POLYMARKET_TERMINAL_CLAIMANT_CONFLICT"
            | "POLYMARKET_TERMINAL_SOURCE_TIME_CONFLICT"
            | "POLYMARKET_TERMINAL_RECEIVE_TIME_CONFLICT"
            | "POLYMARKET_TERMINAL_MARKET_IDENTITY_CONFLICT"
            | "POLYMARKET_TERMINAL_ASSET_IDENTITY_CONFLICT"
            | "POLYMARKET_TERMINAL_WINNER_CONFLICT"
            | "POLYMARKET_TERMINAL_PAYOUT_EVIDENCE_CONFLICT"
            | "POLYMARKET_TERMINAL_DUPLICATE_CONFLICT"
    )
}

fn aggregate_asset_quality<T>(
    expected_assets: &Option<Vec<AssetIdentityV1>>,
    values: &BTreeMap<String, AssetComponent<T>>,
) -> (MarketDataQuality, bool) {
    let Some(expected_assets) = expected_assets else {
        return (MarketDataQuality::UnknownNotObserved, false);
    };
    let qualities = expected_assets
        .iter()
        .map(|asset| {
            values
                .get(&asset.token_id.0)
                .map_or(MarketDataQuality::UnknownNotObserved, |entry| entry.quality)
        })
        .collect::<Vec<_>>();
    let quality = qualities
        .into_iter()
        .max_by_key(|quality| quality_severity(*quality))
        .expect("市场至少包含一个已校验资产");
    let complete = is_visible(quality)
        && expected_assets
            .iter()
            .all(|asset| values.contains_key(&asset.token_id.0));
    (quality, complete)
}

fn quality_severity(quality: MarketDataQuality) -> u8 {
    match quality {
        MarketDataQuality::Fresh => 0,
        MarketDataQuality::Stale => 1,
        MarketDataQuality::UnknownNotObserved => 2,
        MarketDataQuality::Missing => 3,
        MarketDataQuality::ConfirmedGap => 4,
        MarketDataQuality::Invalid => 5,
    }
}

fn ordered_values<'a, T>(
    expected_assets: &'a Option<Vec<AssetIdentityV1>>,
    values: &'a BTreeMap<String, AssetComponent<T>>,
) -> Vec<&'a AssetComponent<T>> {
    expected_assets
        .as_ref()
        .expect("完整资产组件要求固定资产集合")
        .iter()
        .map(|asset| {
            values
                .get(&asset.token_id.0)
                .expect("完整资产组件要求每个固定资产都有状态")
        })
        .collect()
}

fn replace_level(
    levels: &mut Vec<PriceLevelV1>,
    price: &ExactDecimal,
    quantity: &ExactDecimal,
    side: BookSide,
) -> Result<(), ObservationBuildError> {
    let target = parse_canonical_decimal(price, "level.price")?;
    let quantity_value = parse_canonical_decimal(quantity, "level.quantity")?;
    levels.retain(|level| {
        parse_canonical_decimal(&level.price_pusd_per_share, "level.price")
            .map(|candidate| candidate != target)
            .unwrap_or(true)
    });
    if !quantity_value.is_zero() {
        levels.push(PriceLevelV1 {
            price_pusd_per_share: price.clone(),
            quantity_shares: quantity.clone(),
        });
    }
    levels.sort_by(|left, right| {
        let left = parse_canonical_decimal(&left.price_pusd_per_share, "level.price")
            .expect("已校验规范价格");
        let right = parse_canonical_decimal(&right.price_pusd_per_share, "level.price")
            .expect("已校验规范价格");
        match side {
            BookSide::Bid => right.cmp(&left),
            BookSide::Ask => left.cmp(&right),
        }
    });
    Ok(())
}

#[derive(Serialize)]
struct ComponentDigestMaterial<'a> {
    schema_version: &'static str,
    component: ObservationComponentId,
    state: &'a ComponentDomainState,
    quality: MarketDataQuality,
    last_visible_event_id: &'a CanonicalEventId,
}

fn component_state_digest(
    component: ObservationComponentId,
    state: &ComponentDomainState,
    quality: MarketDataQuality,
    last_visible_event_id: &CanonicalEventId,
) -> Result<Sha256Hex, ObservationBuildError> {
    let material = ComponentDigestMaterial {
        schema_version: COMPONENT_DIGEST_SCHEMA_V1,
        component,
        state,
        quality,
        last_visible_event_id,
    };
    Ok(domain_hash(
        COMPONENT_DIGEST_DOMAIN_V1,
        &serde_json::to_vec(&material)?,
    ))
}

#[derive(Serialize)]
struct MarketComponentDigestMaterial<'a> {
    component: ObservationComponentId,
    quality: MarketDataQuality,
    missing: bool,
    state_digest_sha256: &'a Option<Sha256Hex>,
}

#[derive(Serialize)]
struct MarketDigestMaterial<'a> {
    schema_version: &'static str,
    market: &'a MarketIdentityV2,
    components: Vec<MarketComponentDigestMaterial<'a>>,
}

fn market_state_digest(
    market: &MarketIdentityV2,
    states: &[ObservationComponentStateV2],
    missing_mask: &[ObservationComponentId],
) -> Result<Sha256Hex, ObservationBuildError> {
    let missing = missing_mask.iter().copied().collect::<BTreeSet<_>>();
    let material = MarketDigestMaterial {
        schema_version: MARKET_DIGEST_SCHEMA_V1,
        market,
        components: states
            .iter()
            .map(|state| MarketComponentDigestMaterial {
                component: state.component,
                quality: state.quality,
                missing: missing.contains(&state.component),
                state_digest_sha256: &state.state_digest_sha256,
            })
            .collect(),
    };
    Ok(domain_hash(
        MARKET_DIGEST_DOMAIN_V1,
        &serde_json::to_vec(&material)?,
    ))
}

fn domain_hash(domain: &[u8], bytes: &[u8]) -> Sha256Hex {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(bytes);
    Sha256Hex(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts::*;
    use data_pipeline_core::reduction::{
        PolymarketAssetContextV1, PolymarketMarketContextV1, PolymarketOrderbookInputV1,
        PolymarketReducer, PolymarketReductionBatch,
    };
    use data_pipeline_core::typed_raw::{
        POLYMARKET_ORDERBOOK_SCHEMA, PolymarketOrderbookEvent, PolymarketOrderbookRow,
        PolymarketPriceChangeEntry, PolymarketPriceLevel, PolymarketSide, TypedRawRow,
    };
    use data_pipeline_core::{
        derive_execution_market_event_v3_id, source_evidence_from_typed_raw,
        validate_execution_market_event_v3,
    };

    fn sha(character: char) -> Sha256Hex {
        Sha256Hex(character.to_string().repeat(64))
    }

    fn event_id(character: char) -> CanonicalEventId {
        CanonicalEventId(character.to_string().repeat(64))
    }

    fn market() -> MarketIdentityV2 {
        MarketIdentityV2 {
            condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
            clob_market_id: ClobMarketId("3195576".to_owned()),
            window: MarketWindowV2 {
                start_at_utc_ms: 900,
                end_at_utc_ms: 10_000,
            },
        }
    }

    fn asset(token: &str, index: u32, label: &str) -> AssetIdentityV1 {
        AssetIdentityV1 {
            token_id: TokenId(token.to_owned()),
            outcome: OutcomeIdentityV1 {
                index,
                label: label.to_owned(),
            },
        }
    }

    fn event(
        id: char,
        received_at_utc_ms: i64,
        payload: ExecutionMarketEventPayloadV3,
        availability: MarketDataQuality,
        resync: ResyncQuality,
    ) -> CanonicalEventRecordV1 {
        CanonicalEventRecordV1::ExecutionMarketEvent(Box::new(ExecutionMarketEventV3 {
            schema_version: EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
            event_id: event_id(id),
            market: market(),
            time: EventTimeV2 {
                source_event_at_utc_ms: Some(received_at_utc_ms - 1),
                received_at_utc_ms,
            },
            lineage: EventLineageV2 {
                canonical_rule_version: "canonical-rule-v3".to_owned(),
                source_evidence: Vec::new(),
            },
            quality: EventQualityV2 {
                availability,
                resync,
            },
            payload,
        }))
    }

    fn lifecycle(
        id: char,
        received: i64,
        up: &AssetIdentityV1,
        down: &AssetIdentityV1,
    ) -> CanonicalEventRecordV1 {
        event(
            id,
            received,
            ExecutionMarketEventPayloadV3::MarketLifecycleTransition(MarketLifecycleTransitionV2 {
                previous_state: None,
                current_state: MappedMarketLifecycleStateV1::Known {
                    value: KnownMarketLifecycleState::Active,
                },
                assets: vec![up.clone(), down.clone()],
            }),
            MarketDataQuality::Fresh,
            ResyncQuality::NotRequired,
        )
    }

    fn snapshot(id: char, received: i64, asset: &AssetIdentityV1) -> CanonicalEventRecordV1 {
        event(
            id,
            received,
            ExecutionMarketEventPayloadV3::InitialL2Snapshot(InitialL2SnapshotV2 {
                asset: asset.clone(),
                initial_tick_size_pusd_per_share: ExactDecimal("0.01".to_owned()),
                source_sequence_epoch_id: "epoch-1".to_owned(),
                source_cursor: 1,
                book_digest_sha256: sha('b'),
                bids: vec![PriceLevelV1 {
                    price_pusd_per_share: ExactDecimal("0.4".to_owned()),
                    quantity_shares: ExactDecimal("10".to_owned()),
                }],
                asks: Vec::new(),
            }),
            MarketDataQuality::Fresh,
            ResyncQuality::NotRequired,
        )
    }

    fn bba(id: char, received: i64, asset: &AssetIdentityV1) -> CanonicalEventRecordV1 {
        event(
            id,
            received,
            ExecutionMarketEventPayloadV3::BestBidAsk(BestBidAskV2 {
                asset: asset.clone(),
                best_bid_pusd_per_share: Some(ExactDecimal("0.4".to_owned())),
                best_ask_pusd_per_share: Some(ExactDecimal("0.6".to_owned())),
            }),
            MarketDataQuality::Fresh,
            ResyncQuality::NotRequired,
        )
    }

    fn l2_boundary(
        id: char,
        received: i64,
        asset: &AssetIdentityV1,
        restored: bool,
    ) -> CanonicalEventRecordV1 {
        let (previous_quality, current_quality, reason_code, resync) = if restored {
            (
                MarketDataQuality::Missing,
                MarketDataQuality::Fresh,
                "POLYMARKET_L2_EPOCH_BASELINE_RESTORED",
                ResyncQuality::ResyncBoundary,
            )
        } else {
            (
                MarketDataQuality::Fresh,
                MarketDataQuality::Missing,
                "POLYMARKET_L2_EPOCH_BASELINE_REQUIRED",
                ResyncQuality::Required,
            )
        };
        event(
            id,
            received,
            ExecutionMarketEventPayloadV3::QualityBoundary(QualityBoundaryV2 {
                scope: QualityScopeV1::Asset(asset.clone()),
                previous_quality,
                current_quality,
                reason_code: reason_code.to_owned(),
            }),
            current_quality,
            resync,
        )
    }

    fn component(
        observation: &MarketObservationCausalShellV3,
        component: ObservationComponentId,
    ) -> &ObservationComponentStateV2 {
        observation
            .component_states
            .iter()
            .find(|state| state.component == component)
            .expect("九组件齐全")
    }

    fn reducer_context(up: &AssetIdentityV1, down: &AssetIdentityV1) -> PolymarketMarketContextV1 {
        PolymarketMarketContextV1 {
            market: market(),
            assets: vec![
                PolymarketAssetContextV1 {
                    asset: up.clone(),
                    initial_tick_size_pusd_per_share: ExactDecimal("0.01".to_owned()),
                },
                PolymarketAssetContextV1 {
                    asset: down.clone(),
                    initial_tick_size_pusd_per_share: ExactDecimal("0.01".to_owned()),
                },
            ],
            identity_source_evidence: vec![SourceEvidenceRefV2 {
                source_kind: CanonicalSourceKind::PolymarketMarketMetadata,
                source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
                    sha256: sha('a'),
                }),
                source_event_at_utc_ms: None,
                received_at_utc_ms: 900,
                collector_admission: None,
                input_manifest_sha256: sha('b'),
                input_file_sha256: sha('c'),
            }],
        }
    }

    fn valid_lifecycle(up: &AssetIdentityV1, down: &AssetIdentityV1) -> CanonicalEventRecordV1 {
        let mut lifecycle = ExecutionMarketEventV3 {
            schema_version: EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
            event_id: event_id('0'),
            market: market(),
            time: EventTimeV2 {
                source_event_at_utc_ms: None,
                received_at_utc_ms: 900,
            },
            lineage: EventLineageV2 {
                canonical_rule_version: "canonical-rule-v3".to_owned(),
                source_evidence: reducer_context(up, down).identity_source_evidence,
            },
            quality: EventQualityV2 {
                availability: MarketDataQuality::Fresh,
                resync: ResyncQuality::NotRequired,
            },
            payload: ExecutionMarketEventPayloadV3::MarketLifecycleTransition(
                MarketLifecycleTransitionV2 {
                    previous_state: None,
                    current_state: MappedMarketLifecycleStateV1::Known {
                        value: KnownMarketLifecycleState::Active,
                    },
                    assets: vec![up.clone(), down.clone()],
                },
            ),
        };
        lifecycle.event_id = derive_execution_market_event_v3_id(&lifecycle).unwrap();
        validate_execution_market_event_v3(&lifecycle).unwrap();
        CanonicalEventRecordV1::ExecutionMarketEvent(Box::new(lifecycle))
    }

    fn reducer_row(
        epoch: &str,
        admission_sequence: u64,
        source_event_at_utc_ms: i64,
        received_at_utc_ms: i64,
        event: PolymarketOrderbookEvent,
    ) -> PolymarketOrderbookInputV1 {
        let row = PolymarketOrderbookRow {
            timestamp_received: received_at_utc_ms,
            timestamp: Some(source_event_at_utc_ms),
            market: market().condition_id.0,
            slug: Some("btc-updown-5m".to_owned()),
            collector_host_id: "collector-1".to_owned(),
            collector_region: "eu-west-1".to_owned(),
            source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
            schema_version: POLYMARKET_ORDERBOOK_SCHEMA.to_owned(),
            source_sequence_epoch_id: epoch.to_owned(),
            collector_admission_sequence: admission_sequence,
            collector_admission_time_utc_ms: received_at_utc_ms + 1,
            event,
        };
        let evidence = source_evidence_from_typed_raw(
            &TypedRawRow::PolymarketOrderbook(row.clone()),
            &sha('d'),
            &sha('e'),
        )
        .unwrap();
        PolymarketOrderbookInputV1 { row, evidence }
    }

    fn reducer_book(
        asset: &AssetIdentityV1,
        epoch: &str,
        admission_sequence: u64,
        source_event_at_utc_ms: i64,
        received_at_utc_ms: i64,
    ) -> PolymarketOrderbookInputV1 {
        reducer_row(
            epoch,
            admission_sequence,
            source_event_at_utc_ms,
            received_at_utc_ms,
            PolymarketOrderbookEvent::Book {
                asset_id: asset.token_id.0.clone(),
                bids: vec![PolymarketPriceLevel {
                    price: "0.52".to_owned(),
                    size: "250".to_owned(),
                }],
                asks: vec![PolymarketPriceLevel {
                    price: "0.53".to_owned(),
                    size: "260".to_owned(),
                }],
                last_trade_price: None,
            },
        )
    }

    fn reducer_bba(
        asset: &AssetIdentityV1,
        epoch: &str,
        admission_sequence: u64,
        source_event_at_utc_ms: i64,
        received_at_utc_ms: i64,
    ) -> PolymarketOrderbookInputV1 {
        reducer_row(
            epoch,
            admission_sequence,
            source_event_at_utc_ms,
            received_at_utc_ms,
            PolymarketOrderbookEvent::BestBidAsk {
                asset_id: asset.token_id.0.clone(),
                best_bid: Some("0.52".to_owned()),
                best_ask: Some("0.53".to_owned()),
                spread: Some("0.01".to_owned()),
            },
        )
    }

    fn reducer_price_change_without_bba(
        asset: &AssetIdentityV1,
        epoch: &str,
        admission_sequence: u64,
        source_event_at_utc_ms: i64,
        received_at_utc_ms: i64,
    ) -> PolymarketOrderbookInputV1 {
        reducer_row(
            epoch,
            admission_sequence,
            source_event_at_utc_ms,
            received_at_utc_ms,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![PolymarketPriceChangeEntry {
                    asset_id: asset.token_id.0.clone(),
                    price: "0.52".to_owned(),
                    size: Some("70".to_owned()),
                    side: PolymarketSide::Buy,
                    best_bid: None,
                    best_ask: None,
                }],
            },
        )
    }

    fn project_reducer_batch(
        projector: &mut ObservationProjector,
        observations: &mut Vec<MarketObservationCausalShellV3>,
        batch: &PolymarketReductionBatch,
    ) {
        for event in &batch.events {
            validate_execution_market_event_v3(event).unwrap();
            let record = CanonicalEventRecordV1::ExecutionMarketEvent(Box::new(event.clone()));
            if let Some(projected) = projector.push(&record).unwrap() {
                observations.push(projected.observation);
            }
        }
    }

    #[test]
    fn epoch_switch_clears_only_l2_and_same_bba_reanchors_before_l2_restores() {
        let up = asset("123", 0, "UP");
        let down = asset("456", 1, "DOWN");
        let mut projector =
            ObservationProjector::new(market().condition_id.0, sha('f'), sha('d'), 32_768);
        let records = vec![
            lifecycle('1', 1_000, &up, &down),
            snapshot('2', 1_001, &up),
            snapshot('3', 1_001, &down),
            bba('4', 1_002, &up),
            bba('5', 1_002, &down),
            l2_boundary('6', 1_003, &up, false),
            l2_boundary('7', 1_003, &down, false),
            bba('8', 1_004, &up),
            bba('9', 1_004, &down),
            l2_boundary('a', 1_005, &up, true),
            l2_boundary('b', 1_005, &down, true),
        ];
        let mut observations = Vec::new();
        for record in &records {
            if let Some(projected) = projector.push(record).unwrap() {
                observations.push(projected.observation);
            }
        }
        observations.push(projector.finish().unwrap().unwrap().observation);

        assert_eq!(observations.len(), 6, "同一市场每个接收毫秒最多一条");
        let missing = observations
            .iter()
            .find(|observation| observation.visibility_cutoff.received_at_utc_ms == 1_003)
            .unwrap();
        let missing_l2 = component(missing, ObservationComponentId::PolymarketL2);
        let old_bba = component(missing, ObservationComponentId::BestBidAsk);
        assert_eq!(missing_l2.quality, MarketDataQuality::Missing);
        assert!(missing_l2.last_visible_event_id.is_none());
        assert_eq!(old_bba.last_visible_event_id, Some(event_id('5')));
        assert!(
            projector
                .market_state
                .as_ref()
                .unwrap()
                .books
                .values()
                .all(|book| book.value.bids.is_empty() && book.value.asks.is_empty())
        );

        let reanchored = observations
            .iter()
            .find(|observation| observation.visibility_cutoff.received_at_utc_ms == 1_004)
            .unwrap();
        assert_eq!(
            component(reanchored, ObservationComponentId::PolymarketL2).quality,
            MarketDataQuality::Missing
        );
        assert_eq!(
            component(reanchored, ObservationComponentId::BestBidAsk).last_visible_event_id,
            Some(event_id('9'))
        );

        let restored = observations.last().unwrap();
        assert_eq!(
            component(restored, ObservationComponentId::PolymarketL2).quality,
            MarketDataQuality::Fresh
        );
        assert_eq!(
            component(restored, ObservationComponentId::BestBidAsk).last_visible_event_id,
            Some(event_id('9'))
        );
        assert_eq!(projector.peak_retained_l2_levels(), 2);
    }

    #[test]
    fn real_reducer_epoch_and_bba_outputs_preserve_observation_component_boundaries() {
        let up = asset("123", 0, "UP");
        let down = asset("456", 1, "DOWN");
        let mut reducer =
            PolymarketReducer::new(reducer_context(&up, &down), "canonical-rule-v3").unwrap();
        let mut projector =
            ObservationProjector::new(market().condition_id.0, sha('f'), sha('d'), 32_768);
        let mut observations = Vec::new();
        projector.push(&valid_lifecycle(&up, &down)).unwrap();

        for input in [
            reducer_book(&up, "epoch-1", 1, 1_000, 1_010),
            reducer_book(&down, "epoch-1", 2, 1_001, 1_011),
        ] {
            let batch = reducer.reduce_orderbook_group(vec![input]).unwrap();
            project_reducer_batch(&mut projector, &mut observations, &batch);
        }

        let old_up_bba = reducer
            .reduce_orderbook_group(vec![reducer_bba(&up, "epoch-1", 3, 1_002, 1_012)])
            .unwrap();
        let old_up_payload = old_up_bba
            .events
            .iter()
            .find_map(|event| match &event.payload {
                ExecutionMarketEventPayloadV3::BestBidAsk(payload) => Some(payload.clone()),
                _ => None,
            })
            .unwrap();
        project_reducer_batch(&mut projector, &mut observations, &old_up_bba);

        let old_down_bba = reducer
            .reduce_orderbook_group(vec![reducer_bba(&down, "epoch-1", 4, 1_003, 1_013)])
            .unwrap();
        let (old_down_bba_id, old_down_payload) = old_down_bba
            .events
            .iter()
            .find_map(|event| match &event.payload {
                ExecutionMarketEventPayloadV3::BestBidAsk(payload) => {
                    Some((event.event_id.clone(), payload.clone()))
                }
                _ => None,
            })
            .unwrap();
        project_reducer_batch(&mut projector, &mut observations, &old_down_bba);

        let invalidated = reducer
            .reduce_orderbook_group(vec![reducer_price_change_without_bba(
                &up, "epoch-2", 1, 2_000, 2_010,
            )])
            .unwrap();
        assert_eq!(
            invalidated
                .events
                .iter()
                .filter(|event| matches!(
                    &event.payload,
                    ExecutionMarketEventPayloadV3::QualityBoundary(boundary)
                        if boundary.reason_code == "POLYMARKET_L2_EPOCH_BASELINE_REQUIRED"
                ))
                .count(),
            2
        );
        project_reducer_batch(&mut projector, &mut observations, &invalidated);

        let new_up_bba = reducer
            .reduce_orderbook_group(vec![reducer_bba(&up, "epoch-2", 2, 2_001, 2_011)])
            .unwrap();
        let (new_up_bba_id, new_up_payload) = new_up_bba
            .events
            .iter()
            .find_map(|event| match &event.payload {
                ExecutionMarketEventPayloadV3::BestBidAsk(payload) => {
                    Some((event.event_id.clone(), payload.clone()))
                }
                _ => None,
            })
            .unwrap();
        assert_ne!(new_up_bba_id, old_up_bba.events[0].event_id);
        assert_eq!(new_up_payload, old_up_payload);
        project_reducer_batch(&mut projector, &mut observations, &new_up_bba);

        let new_down_bba = reducer
            .reduce_orderbook_group(vec![reducer_bba(&down, "epoch-2", 3, 2_002, 2_012)])
            .unwrap();
        let (new_down_bba_id, new_down_payload) = new_down_bba
            .events
            .iter()
            .find_map(|event| match &event.payload {
                ExecutionMarketEventPayloadV3::BestBidAsk(payload) => {
                    Some((event.event_id.clone(), payload.clone()))
                }
                _ => None,
            })
            .unwrap();
        assert_ne!(new_down_bba_id, old_down_bba_id);
        assert_eq!(new_down_payload, old_down_payload);
        project_reducer_batch(&mut projector, &mut observations, &new_down_bba);

        let up_rebuilt = reducer
            .reduce_orderbook_group(vec![reducer_book(&up, "epoch-2", 4, 2_003, 2_013)])
            .unwrap();
        assert!(
            up_rebuilt.events.iter().all(|event| !matches!(
                &event.payload,
                ExecutionMarketEventPayloadV3::BestBidAsk(_)
            ))
        );
        project_reducer_batch(&mut projector, &mut observations, &up_rebuilt);

        let down_rebuilt = reducer
            .reduce_orderbook_group(vec![reducer_book(&down, "epoch-2", 5, 2_004, 2_014)])
            .unwrap();
        assert!(
            down_rebuilt.events.iter().all(|event| !matches!(
                &event.payload,
                ExecutionMarketEventPayloadV3::BestBidAsk(_)
            ))
        );
        let down_recovery_id = down_rebuilt
            .events
            .iter()
            .find_map(|event| match &event.payload {
                ExecutionMarketEventPayloadV3::QualityBoundary(boundary)
                    if boundary.reason_code == "POLYMARKET_L2_EPOCH_BASELINE_RESTORED" =>
                {
                    Some(event.event_id.clone())
                }
                _ => None,
            })
            .unwrap();
        project_reducer_batch(&mut projector, &mut observations, &down_rebuilt);
        observations.push(projector.finish().unwrap().unwrap().observation);

        let at = |received_at_utc_ms| {
            observations
                .iter()
                .find(|observation| {
                    observation.visibility_cutoff.received_at_utc_ms == received_at_utc_ms
                })
                .unwrap()
        };
        let missing = at(2_010);
        let missing_l2 = component(missing, ObservationComponentId::PolymarketL2);
        let retained_bba = component(missing, ObservationComponentId::BestBidAsk);
        assert_eq!(missing_l2.quality, MarketDataQuality::Missing);
        assert!(missing_l2.last_visible_event_id.is_none());
        assert_eq!(
            retained_bba.last_visible_event_id,
            Some(old_down_bba_id.clone())
        );
        let old_bba_digest = retained_bba.state_digest_sha256.clone();

        let reanchored = at(2_012);
        assert_eq!(
            component(reanchored, ObservationComponentId::PolymarketL2).quality,
            MarketDataQuality::Missing
        );
        let reanchored_bba = component(reanchored, ObservationComponentId::BestBidAsk);
        assert_eq!(
            reanchored_bba.last_visible_event_id,
            Some(new_down_bba_id.clone())
        );
        assert_ne!(reanchored_bba.state_digest_sha256, old_bba_digest);

        assert_eq!(
            component(at(2_013), ObservationComponentId::PolymarketL2).quality,
            MarketDataQuality::Missing
        );

        let restored = at(2_014);
        let restored_l2 = component(restored, ObservationComponentId::PolymarketL2);
        let preserved_bba = component(restored, ObservationComponentId::BestBidAsk);
        assert_eq!(restored_l2.quality, MarketDataQuality::Fresh);
        assert_eq!(restored_l2.last_visible_event_id, Some(down_recovery_id));
        assert_eq!(preserved_bba.last_visible_event_id, Some(new_down_bba_id));
        assert_eq!(
            preserved_bba.state_digest_sha256,
            reanchored_bba.state_digest_sha256
        );
    }

    #[test]
    fn component_digest_preimage_is_frozen() {
        let state = ComponentDomainState::BestBidAsk(vec![AssetBestBidAskState {
            asset: asset("123", 0, "UP"),
            best_bid_pusd_per_share: Some(ExactDecimal("0.4".to_owned())),
            best_ask_pusd_per_share: Some(ExactDecimal("0.6".to_owned())),
        }]);
        let digest = component_state_digest(
            ObservationComponentId::BestBidAsk,
            &state,
            MarketDataQuality::Fresh,
            &event_id('1'),
        )
        .unwrap();
        assert_eq!(
            digest.0,
            "04b15eadf5ee6507381ec3214bfb0c8a46bafcd95851b3c73bf7fd33e9bd1dc8"
        );
    }

    #[test]
    fn confirmed_missing_is_not_collapsed_by_an_unobserved_peer_asset() {
        let up = asset("123", 0, "UP");
        let down = asset("456", 1, "DOWN");
        let expected_assets = Some(vec![up.clone(), down]);
        let values = BTreeMap::from([(
            up.token_id.0.clone(),
            AssetComponent {
                value: (),
                quality: MarketDataQuality::Missing,
            },
        )]);

        assert_eq!(
            aggregate_asset_quality(&expected_assets, &values),
            (MarketDataQuality::Missing, false)
        );
    }

    #[test]
    fn retained_l2_limit_counts_each_bid_and_ask_once() {
        let up = asset("123", 0, "UP");
        let down = asset("456", 1, "DOWN");
        let mut projector =
            ObservationProjector::new(market().condition_id.0, sha('f'), sha('d'), 1);
        projector.push(&lifecycle('1', 1_000, &up, &down)).unwrap();
        projector.push(&snapshot('2', 1_001, &up)).unwrap();

        assert!(matches!(
            projector.push(&snapshot('3', 1_002, &down)),
            Err(ObservationBuildError::RetainedStateLimit {
                observed: 2,
                limit: 1,
            })
        ));
    }

    #[test]
    fn unknown_quality_boundary_reason_is_rejected() {
        let up = asset("123", 0, "UP");
        let down = asset("456", 1, "DOWN");
        let mut projector =
            ObservationProjector::new(market().condition_id.0, sha('f'), sha('d'), 32_768);
        projector.push(&lifecycle('1', 1_000, &up, &down)).unwrap();
        projector.push(&snapshot('2', 1_001, &up)).unwrap();
        let unsupported = event(
            '3',
            1_002,
            ExecutionMarketEventPayloadV3::QualityBoundary(QualityBoundaryV2 {
                scope: QualityScopeV1::Asset(up),
                previous_quality: MarketDataQuality::Fresh,
                current_quality: MarketDataQuality::Missing,
                reason_code: "POLYMARKET_UNMODELED_BOUNDARY".to_owned(),
            }),
            MarketDataQuality::Missing,
            ResyncQuality::Required,
        );

        assert!(matches!(
            projector.push(&unsupported),
            Err(ObservationBuildError::UnsupportedQualityBoundary { reason_code, .. })
                if reason_code == "POLYMARKET_UNMODELED_BOUNDARY"
        ));
    }

    #[test]
    fn market_digest_preimage_is_frozen() {
        let component_states = data_pipeline_core::REQUIRED_OBSERVATION_COMPONENTS
            .into_iter()
            .map(|component| ObservationComponentStateV2 {
                component,
                quality: MarketDataQuality::UnknownNotObserved,
                source_event_at_utc_ms: None,
                received_at_utc_ms: None,
                information_age_ms: None,
                observed_transport_delay_ms: None,
                last_visible_event_id: None,
                state_digest_sha256: None,
            })
            .collect::<Vec<_>>();
        let missing_mask = data_pipeline_core::REQUIRED_OBSERVATION_COMPONENTS.to_vec();

        let digest = market_state_digest(&market(), &component_states, &missing_mask).unwrap();
        assert_eq!(
            digest.0,
            "411722a019fdf35d2b401dd026191f39593dc1446450ed3601d98506b5dffa0c"
        );
    }
}
