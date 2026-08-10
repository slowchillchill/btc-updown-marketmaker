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
use sha2::{Digest, Sha256};

use crate::matching::MatchingStateErrorV1;
use crate::scenario::{PlatformMarketV1, PlatformScenarioV1};
use crate::session::{SessionError, SimCallerRoleV1, SimSession};

const CLOSED_BUCKET_HASH_DOMAIN_V1: &[u8] = b"SIM_CLOSED_MARKET_EVENT_BUCKET_V1\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MarketEventPathV1 {
    HiddenPlatform,
    PublicVisibility,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

impl ClosedMarketEventBucketV1 {
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
    BucketIdentityConflict,
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
            Self::UnknownMarket(id) => write!(formatter, "市场事件不属于进程场景：{id}"),
            Self::MarketIdentityConflict(id) => write!(formatter, "市场完整身份与场景冲突：{id}"),
            Self::UnknownAsset(id) => write!(formatter, "市场事件资产不属于场景：{id}"),
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
            Self::BucketIdentityConflict => write!(formatter, "同一闭合水位绑定了不同市场事件桶"),
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
        Self::ExecutionState(format!("{value:?}"))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MarketLifecyclePhaseV1 {
    Preloading,
    Active,
    PostEndPendingResolution,
    ResolutionTerminal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AppliedBucketIdentityV1 {
    bucket_at_utc_ms: i64,
    start_index_position: u64,
    next_index_position: u64,
    sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MarketStateStoreV1 {
    markets: BTreeMap<String, MarketStateV1>,
    last_applied_bucket: Option<AppliedBucketIdentityV1>,
}

impl MarketStateStoreV1 {
    pub(crate) fn new(scenario: &PlatformScenarioV1) -> Self {
        let markets = scenario
            .market_set()
            .markets()
            .iter()
            .map(|member| {
                (
                    member.market().condition_id.0.clone(),
                    MarketStateV1::new(member),
                )
            })
            .collect();
        Self {
            markets,
            last_applied_bucket: None,
        }
    }

    pub(crate) fn market(&self, condition_id: &str) -> Option<&MarketStateV1> {
        self.markets.get(condition_id)
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
    ) -> Result<BTreeMap<String, MarketStateV1>, MarketEventErrorV1> {
        let mut prepared = BTreeMap::new();
        for event in events {
            let condition_id = event.market.condition_id.0.as_str();
            if !prepared.contains_key(condition_id) {
                let current = self
                    .markets
                    .get(condition_id)
                    .ok_or_else(|| MarketEventErrorV1::UnknownMarket(condition_id.to_owned()))?;
                prepared.insert(condition_id.to_owned(), current.clone());
            }
            let market = prepared
                .get_mut(condition_id)
                .expect("affected market was inserted above");
            market.apply_event(event, path)?;
        }
        Ok(prepared)
    }

    fn commit_prepared_events(
        &mut self,
        prepared: BTreeMap<String, MarketStateV1>,
        bucket_at_utc_ms: i64,
        identity: AppliedBucketIdentityV1,
    ) {
        for (condition_id, market) in prepared {
            self.markets.insert(condition_id, market);
        }
        self.advance_time_through(bucket_at_utc_ms);
        self.last_applied_bucket = Some(identity);
    }

    fn validate_initial_load_complete(
        &self,
        prepared: &BTreeMap<String, MarketStateV1>,
    ) -> Result<(), MarketEventErrorV1> {
        let incomplete = self
            .markets
            .iter()
            .filter_map(|(condition_id, current)| {
                let candidate = prepared.get(condition_id).unwrap_or(current);
                (!candidate.initial_load_complete()).then(|| condition_id.clone())
            })
            .collect::<Vec<_>>();
        if incomplete.is_empty() {
            Ok(())
        } else {
            Err(MarketEventErrorV1::IncompleteInitialLoad(incomplete))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MarketStateV1 {
    market: MarketIdentityV2,
    expected_assets: [AssetIdentityV1; 2],
    lifecycle_phase: MarketLifecyclePhaseV1,
    mapped_lifecycle_state: Option<MappedMarketLifecycleStateV1>,
    trading_state: MarketTradingStateV2,
    market_quality: MarketDataQuality,
    assets: BTreeMap<String, AssetMarketStateV1>,
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
            qualified_resolution: None,
            receive_only_resolution_event_id: None,
        }
    }

    pub(crate) fn lifecycle_phase(&self) -> MarketLifecyclePhaseV1 {
        self.lifecycle_phase
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

    pub(crate) fn qualified_resolution(&self) -> Option<&MarketResolutionTerminalV3> {
        (self.market_quality == MarketDataQuality::Fresh)
            .then_some(self.qualified_resolution.as_ref())
            .flatten()
    }

    pub(crate) fn receive_only_resolution_event_id(&self) -> Option<&CanonicalEventId> {
        self.receive_only_resolution_event_id.as_ref()
    }

    fn initial_load_complete(&self) -> bool {
        self.assets.values().all(AssetMarketStateV1::is_initialized)
    }

    fn advance_time_through(&mut self, closed_through_utc_ms: i64) {
        if self.lifecycle_phase == MarketLifecyclePhaseV1::Active
            && closed_through_utc_ms >= self.market.window.end_at_utc_ms
        {
            self.lifecycle_phase = MarketLifecyclePhaseV1::PostEndPendingResolution;
        }
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
                self.qualified_resolution = Some(terminal.clone());
                self.lifecycle_phase = MarketLifecyclePhaseV1::ResolutionTerminal;
            }
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExternalLevelV1 {
    quantity_shares: Decimal,
    causal_group_id: Option<Sha256Hex>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct AccountBookOverlayV1 {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AccountBookOverlayStoreV1 {
    markets: BTreeMap<String, BTreeMap<String, AccountBookOverlayV1>>,
}

impl AccountBookOverlayStoreV1 {
    pub(crate) fn new(scenario: &PlatformScenarioV1) -> Self {
        let markets = scenario
            .market_set()
            .markets()
            .iter()
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

#[derive(Clone, Debug, PartialEq, Eq)]
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
    pub(crate) fn apply_closed_market_event_bucket(
        &mut self,
        bucket: ClosedMarketEventBucketV1,
    ) -> Result<MarketBucketApplyDispositionV1, MarketEventErrorV1> {
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
        if let Some(previous) = &current_store.last_applied_bucket
            && previous.bucket_at_utc_ms == identity.bucket_at_utc_ms
            && previous.next_index_position == identity.next_index_position
        {
            if previous.start_index_position == identity.start_index_position
                && previous.sha256 == identity.sha256
            {
                if bucket.closure == MillisecondBucketClosureV1::EndOfInput {
                    current_store.validate_initial_load_complete(&BTreeMap::new())?;
                }
                return Ok(MarketBucketApplyDispositionV1::PhysicalRetry);
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

        let prepared = current_store.prepare_events(&events, bucket.path)?;
        if bucket.closure == MillisecondBucketClosureV1::EndOfInput {
            current_store.validate_initial_load_complete(&prepared)?;
        }
        let authorized_execution_path = match self.session_spec().caller_role {
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
                bucket.path == MarketEventPathV1::HiddenPlatform
            }
            SimCallerRoleV1::Paper => bucket.path == MarketEventPathV1::PublicVisibility,
        };
        let prepared_execution = if authorized_execution_path {
            let mut state = self.execution_state().clone();
            state.apply_authorized_market_events(
                self.scenario(),
                bucket.bucket_at_utc_ms,
                &events,
            )?;
            let query_projection_changed =
                state.query_projection_changed_from(self.execution_state());
            Some((state, query_projection_changed))
        } else {
            None
        };
        let prepared_query_projection_revision = match &prepared_execution {
            Some((_, true)) => Some(self.prepare_next_platform_state_revision()?),
            Some((_, false)) | None => None,
        };
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
        if let Some((state, _)) = prepared_execution {
            if let Some(next_revision) = prepared_query_projection_revision {
                self.commit_prepared_platform_state_revision(next_revision);
            }
            self.replace_execution_state(state);
        }
        Ok(MarketBucketApplyDispositionV1::Applied)
    }
}
