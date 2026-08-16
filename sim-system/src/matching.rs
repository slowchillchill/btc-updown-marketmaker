use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

use contracts::{
    BookSide, ExactDecimal, ExecutionMarketEventPayloadV3, ExecutionMarketEventV3,
    InitialL2SnapshotV2, LevelExecutionTransitionV3, MarketDataQuality, MarketResolutionTerminalV3,
    PlatformAccountBalanceDeltaV1, PlatformAccountDeltaReasonV1, PlatformAccountDeltaV1,
    PlatformOutcomeBalanceDeltaV1, PlatformOutcomeBalanceV1, QualityScopeV1, ResyncQuality,
    Sha256Hex, TokenId,
};
use data_pipeline_core::parse_canonical_decimal;
use num_bigint::{BigInt, BigUint};
use num_integer::Integer;
use num_traits::{One, ToPrimitive, Zero};
use polymarket_client_sdk_v2::types::U256;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::account::{
    AutoRedeemAccountEffectV1, FillEconomicAmountsV1, PlatformAccountErrorV1,
    PlatformAccountStateV1, validate_platform_account_delta_integrity,
};
use crate::finality::{
    TradeFinalityPathV1, TradeFinalityPolicyV1, TradeFinalitySelectorDomainV1, TradeFinalityStateV1,
};
use crate::market::{
    AccountBookOverlayStoreV1, MarketLifecyclePhaseV1, PreparedAccountBookOverlayLevelChangeV2,
};
use crate::market_directory::SessionMarketDirectoryV1;
use crate::platform_rules::{LifecycleAccountRuleV1, SealedProcessRuleSnapshot};
use crate::rebate::{
    CommittableTakerRebateRegistrationsV2, RebateKindV1, RebateRecoveryFillV1,
    RebateSettlementActionV1, RebateStateErrorV1, RebateStatePatchV2, RebateStateV1,
};
use crate::scenario::PlatformMarketV1;

const MICRO_SCALE: u32 = 6;
const FEE_SCALE: u32 = 5;
const EXTERNAL_SEGMENT_ID_DOMAIN_V1: &[u8] = b"SIM_L2_SYNTHETIC_MAKER_SEGMENT_V1\0";
const PENDING_MAKER_DEPLETION_ID_DOMAIN_V1: &[u8] = b"SIM_PENDING_MAKER_DEPLETION_V1\0";
const VENUE_DUST_ID_DOMAIN_V1: &[u8] = b"SIM_VENUE_DUST_TERMINAL_V1\0";
const MARKET_RESOLUTION_ORDER_ID_DOMAIN_V1: &[u8] = b"SIM_MARKET_RESOLUTION_ORDER_V1\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MatchingMathErrorV1 {
    NegativeAmount,
    InvalidPrice,
    InvalidSegment,
    ArithmeticOverflow,
}

impl fmt::Display for MatchingMathErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativeAmount => write!(formatter, "撮合数量不能为负数"),
            Self::InvalidPrice => write!(formatter, "撮合价格必须严格位于零和一之间"),
            Self::InvalidSegment => write!(formatter, "合成 maker 段状态非法"),
            Self::ArithmeticOverflow => write!(formatter, "撮合整数或十进制运算溢出"),
        }
    }
}

impl std::error::Error for MatchingMathErrorV1 {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MatchingStateErrorV1 {
    Math(MatchingMathErrorV1),
    Account(PlatformAccountErrorV1),
    Decode(String),
    UnknownMarket(String),
    UnknownAsset(String),
    MarketAlreadyInitialized(String),
    InitialQueueAlreadyLoaded(String),
    QueuePredecessorConflict,
    QueueUnderflow,
    AccountOrderConflict(String),
    Finality(String),
    ResolutionConflict(String),
    Rebate(String),
    LogicalTimeRegression,
    RestoredDirectoryMismatch(&'static str),
}

impl From<PlatformAccountErrorV1> for MatchingStateErrorV1 {
    fn from(value: PlatformAccountErrorV1) -> Self {
        Self::Account(value)
    }
}

impl From<MatchingMathErrorV1> for MatchingStateErrorV1 {
    fn from(value: MatchingMathErrorV1) -> Self {
        Self::Math(value)
    }
}

impl From<RebateStateErrorV1> for MatchingStateErrorV1 {
    fn from(value: RebateStateErrorV1) -> Self {
        Self::Rebate(value.to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TakerFillSliceV1 {
    pub segment_id: Sha256Hex,
    pub price_pusd_per_share: Decimal,
    pub base_units: u128,
    pub quote_units: u128,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MakerQueueDepletionV1 {
    pub source_event_id: String,
    pub platform_order_id: String,
    pub price_pusd_per_share: Decimal,
    pub base_units: u128,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VenueDustQueueCandidateV1 {
    platform_order_id: String,
    price_pusd_per_share: Decimal,
    remaining_base_units: u128,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CandidateVenueDustRemainderV2 {
    pub(crate) condition_id: String,
    pub(crate) token_id: String,
    pub(crate) platform_order_id: String,
    pub(crate) price_pusd_per_share: Decimal,
    pub(crate) remaining_base_units: u128,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct QueueEventEffectsV1 {
    maker_depletions: Vec<MakerQueueDepletionV1>,
    venue_dust_candidate: Option<VenueDustQueueCandidateV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VenueDustEffectV1 {
    pub platform_order_id: String,
    pub condition_id: String,
    pub token_id: String,
    pub closed_base_units: u128,
    pub released_open_quote_units: u128,
    pub account_delta: Arc<PlatformAccountDeltaV1>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AuthorizedMarketEventEffectsV1 {
    pub trade_finality_effects: Vec<TradeFinalityLifecycleEffectsV1>,
    pub venue_dust_effects: Vec<VenueDustEffectV1>,
    pub market_resolution_order_effects: Vec<MarketResolutionOrderEffectV1>,
    pub settlement_feedbacks: Vec<SettlementLifecycleEffectV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueueMatchPlanV1 {
    pub fills: Vec<TakerFillSliceV1>,
    pub matched_base_units: u128,
    pub used_quote_units: u128,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum QueueSegmentV1 {
    External(ExternalSyntheticMakerSegmentV1),
    Account(AccountOrderQueueSegmentV1),
}

impl QueueSegmentV1 {
    fn remaining_base_units(&self) -> Result<u128, MatchingStateErrorV1> {
        match self {
            Self::External(segment) => segment
                .total_base_units
                .checked_sub(segment.consumed_base_units)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict),
            Self::Account(segment) => Ok(segment.remaining_base_units),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ExternalSyntheticMakerSegmentV1 {
    segment_id: Sha256Hex,
    total_base_units: u128,
    total_quote_units: u128,
    consumed_base_units: u128,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct AccountOrderQueueSegmentV1 {
    platform_order_id: String,
    remaining_base_units: u128,
}

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct PriceQueueV1 {
    segments: VecDeque<QueueSegmentV1>,
    account_taker_depletion_debt_units: u128,
}

#[cfg(test)]
std::thread_local! {
    static PRICE_QUEUE_CLONE_COUNT_FOR_TESTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

impl Clone for PriceQueueV1 {
    fn clone(&self) -> Self {
        #[cfg(test)]
        PRICE_QUEUE_CLONE_COUNT_FOR_TESTS.with(|count| count.set(count.get() + 1));
        Self {
            segments: self.segments.clone(),
            account_taker_depletion_debt_units: self.account_taker_depletion_debt_units,
        }
    }
}

#[cfg(test)]
fn reset_price_queue_clone_count_for_tests() {
    PRICE_QUEUE_CLONE_COUNT_FOR_TESTS.with(|count| count.set(0));
}

#[cfg(test)]
fn price_queue_clone_count_for_tests() -> usize {
    PRICE_QUEUE_CLONE_COUNT_FOR_TESTS.with(std::cell::Cell::get)
}

impl PriceQueueV1 {
    fn unique_account_order_base_units(
        &self,
        platform_order_id: &str,
    ) -> Result<Option<u128>, MatchingStateErrorV1> {
        let mut found = None;
        for segment in &self.segments {
            let QueueSegmentV1::Account(account) = segment else {
                continue;
            };
            if account.platform_order_id == platform_order_id {
                if found.replace(account.remaining_base_units).is_some() {
                    return Err(MatchingStateErrorV1::QueuePredecessorConflict);
                }
            }
        }
        Ok(found)
    }

    fn remove_account_order(&mut self, platform_order_id: &str) {
        self.segments.retain(|segment| {
            !matches!(segment, QueueSegmentV1::Account(account)
                if account.platform_order_id == platform_order_id)
        });
    }
}

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct AssetQueueV1 {
    bids: BTreeMap<Decimal, PriceQueueV1>,
    asks: BTreeMap<Decimal, PriceQueueV1>,
    initial_loaded: bool,
}

#[cfg(test)]
std::thread_local! {
    static ASSET_QUEUE_CLONE_COUNT_FOR_TESTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

impl Clone for AssetQueueV1 {
    fn clone(&self) -> Self {
        #[cfg(test)]
        ASSET_QUEUE_CLONE_COUNT_FOR_TESTS.with(|count| count.set(count.get() + 1));
        Self {
            bids: self.bids.clone(),
            asks: self.asks.clone(),
            initial_loaded: self.initial_loaded,
        }
    }
}

#[cfg(test)]
fn reset_asset_queue_clone_count_for_tests() {
    ASSET_QUEUE_CLONE_COUNT_FOR_TESTS.with(|count| count.set(0));
}

#[cfg(test)]
fn asset_queue_clone_count_for_tests() -> usize {
    ASSET_QUEUE_CLONE_COUNT_FOR_TESTS.with(std::cell::Cell::get)
}

impl AssetQueueV1 {
    fn levels(&self, side: BookSide) -> &BTreeMap<Decimal, PriceQueueV1> {
        match side {
            BookSide::Bid => &self.bids,
            BookSide::Ask => &self.asks,
        }
    }

    fn levels_mut(&mut self, side: BookSide) -> &mut BTreeMap<Decimal, PriceQueueV1> {
        match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        }
    }

    fn contains_account_order(&self, platform_order_id: &str) -> bool {
        self.bids
            .values()
            .chain(self.asks.values())
            .flat_map(|queue| queue.segments.iter())
            .any(|segment| {
                matches!(segment, QueueSegmentV1::Account(account)
                    if account.platform_order_id == platform_order_id)
            })
    }

    fn insert_account_buy_remainder(
        &mut self,
        price: Decimal,
        platform_order_id: String,
        base_units: u128,
    ) -> Result<(), MatchingStateErrorV1> {
        if base_units == 0 {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        if self.contains_account_order(&platform_order_id) {
            return Err(MatchingStateErrorV1::AccountOrderConflict(
                platform_order_id,
            ));
        }
        self.bids
            .entry(price)
            .or_default()
            .segments
            .push_back(QueueSegmentV1::Account(AccountOrderQueueSegmentV1 {
                platform_order_id,
                remaining_base_units: base_units,
            }));
        Ok(())
    }

    fn venue_dust_candidate_for_order(
        &self,
        price: Decimal,
        platform_order_id: &str,
    ) -> Option<VenueDustQueueCandidateV1> {
        let queue = self.bids.get(&price)?;
        if queue.account_taker_depletion_debt_units != 0 || queue.segments.len() != 1 {
            return None;
        }
        match queue.segments.front() {
            Some(QueueSegmentV1::Account(account))
                if account.platform_order_id == platform_order_id
                    && account.remaining_base_units > 0 =>
            {
                Some(VenueDustQueueCandidateV1 {
                    platform_order_id: account.platform_order_id.clone(),
                    price_pusd_per_share: price,
                    remaining_base_units: account.remaining_base_units,
                })
            }
            Some(QueueSegmentV1::External(_)) | Some(QueueSegmentV1::Account(_)) | None => None,
        }
    }

    fn remove_account_order(&mut self, platform_order_id: &str) {
        for levels in [&mut self.bids, &mut self.asks] {
            levels.retain(|_, queue| {
                queue.segments.retain(|segment| {
                    !matches!(segment, QueueSegmentV1::Account(account)
                        if account.platform_order_id == platform_order_id)
                });
                !queue.segments.is_empty() || queue.account_taker_depletion_debt_units > 0
            });
        }
    }

    fn match_taker_buy(
        &mut self,
        maximum_price: Decimal,
        target: TakerTargetV1,
    ) -> Result<QueueMatchPlanV1, MatchingStateErrorV1> {
        let prices = self
            .asks
            .range(..=maximum_price)
            .map(|(price, _)| *price)
            .collect::<Vec<_>>();
        let mut fills = Vec::new();
        let mut matched_base_units = 0_u128;
        let mut used_quote_units = 0_u128;
        'prices: for price in prices {
            if target.complete(matched_base_units, used_quote_units) {
                break;
            }
            let queue = self
                .asks
                .get_mut(&price)
                .expect("collected ask price must remain present");
            for segment in &mut queue.segments {
                let QueueSegmentV1::External(external) = segment else {
                    continue;
                };
                if target.complete(matched_base_units, used_quote_units) {
                    break;
                }
                let remaining_base = external
                    .total_base_units
                    .checked_sub(external.consumed_base_units)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                if remaining_base == 0 {
                    continue;
                }
                if external.total_quote_units == 0 {
                    continue;
                }
                let next = match target {
                    TakerTargetV1::Quote(budget) => {
                        let remaining_budget = budget
                            .checked_sub(used_quote_units)
                            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                        maximum_base_for_quote_budget_v1(
                            external.total_base_units,
                            external.total_quote_units,
                            external.consumed_base_units,
                            remaining_budget,
                        )?
                    }
                    TakerTargetV1::LimitBase {
                        total_base_units,
                        total_quote_units,
                    } => {
                        let remaining_target = total_base_units
                            .checked_sub(matched_base_units)
                            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                        maximum_base_for_limit_order_segment_v1(
                            external.total_base_units,
                            external.total_quote_units,
                            external.consumed_base_units,
                            remaining_base.min(remaining_target),
                            total_base_units,
                            total_quote_units,
                            matched_base_units,
                            used_quote_units,
                        )?
                    }
                };
                let Some((delta_base, delta_quote)) = next else {
                    break 'prices;
                };
                external.consumed_base_units = external
                    .consumed_base_units
                    .checked_add(delta_base)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                queue.account_taker_depletion_debt_units = queue
                    .account_taker_depletion_debt_units
                    .checked_add(delta_base)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                matched_base_units = matched_base_units
                    .checked_add(delta_base)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                used_quote_units = used_quote_units
                    .checked_add(delta_quote)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                fills.push(TakerFillSliceV1 {
                    segment_id: external.segment_id.clone(),
                    price_pusd_per_share: price,
                    base_units: delta_base,
                    quote_units: delta_quote,
                });
                if delta_base < remaining_base {
                    break 'prices;
                }
            }
            queue.segments.retain(|segment| {
                segment
                    .remaining_base_units()
                    .is_ok_and(|remaining| remaining > 0)
            });
        }
        Ok(QueueMatchPlanV1 {
            fills,
            matched_base_units,
            used_quote_units,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionQueueStateV1 {
    markets: BTreeMap<String, BTreeMap<String, AssetQueueV1>>,
}

pub(crate) struct PreparedTakerAssetQueueChangeV2<'a> {
    queues: &'a mut ExecutionQueueStateV1,
    condition_id: String,
    token_id: String,
    candidate_asset: AssetQueueV1,
    plan: QueueMatchPlanV1,
}

impl PreparedTakerAssetQueueChangeV2<'_> {
    pub(crate) fn plan(&self) -> &QueueMatchPlanV1 {
        &self.plan
    }

    pub(crate) fn prepare_account_buy_remainder(
        &mut self,
        price: Decimal,
        platform_order_id: String,
        base_units: u128,
    ) -> Result<(), MatchingStateErrorV1> {
        if base_units == 0 {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        if self.queues.contains_account_order(&platform_order_id) {
            return Err(MatchingStateErrorV1::AccountOrderConflict(
                platform_order_id,
            ));
        }
        self.candidate_asset
            .insert_account_buy_remainder(price, platform_order_id, base_units)
    }

    pub(crate) fn candidate_venue_dust_remainder(
        &self,
        price: Decimal,
        platform_order_id: &str,
    ) -> Option<CandidateVenueDustRemainderV2> {
        let candidate = self
            .candidate_asset
            .venue_dust_candidate_for_order(price, platform_order_id)?;
        Some(CandidateVenueDustRemainderV2 {
            condition_id: self.condition_id.clone(),
            token_id: self.token_id.clone(),
            platform_order_id: candidate.platform_order_id,
            price_pusd_per_share: candidate.price_pusd_per_share,
            remaining_base_units: candidate.remaining_base_units,
        })
    }

    pub(crate) fn prepare_remove_venue_dust_remainder(
        &mut self,
        candidate: &CandidateVenueDustRemainderV2,
    ) -> Result<(), MatchingStateErrorV1> {
        if candidate.condition_id != self.condition_id || candidate.token_id != self.token_id {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        let current = self
            .candidate_asset
            .venue_dust_candidate_for_order(
                candidate.price_pusd_per_share,
                &candidate.platform_order_id,
            )
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        if current.platform_order_id != candidate.platform_order_id
            || current.price_pusd_per_share != candidate.price_pusd_per_share
            || current.remaining_base_units != candidate.remaining_base_units
        {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        self.candidate_asset
            .remove_account_order(&candidate.platform_order_id);
        Ok(())
    }

    pub(crate) fn commit(self) -> QueueMatchPlanV1 {
        let Self {
            queues,
            condition_id,
            token_id,
            candidate_asset,
            plan,
        } = self;
        *queues
            .asset_mut(&condition_id, &token_id)
            .expect("独占准备期间目标撮合资产必须仍在队列状态中") = candidate_asset;
        plan
    }
}

impl ExecutionQueueStateV1 {
    pub(crate) fn new(directory: &SessionMarketDirectoryV1) -> Self {
        let markets = directory
            .markets()
            .map(|market| {
                let assets = market
                    .assets()
                    .iter()
                    .map(|asset| (asset.token_id.0.clone(), AssetQueueV1::default()))
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
            .map(|asset| (asset.token_id.0.clone(), AssetQueueV1::default()))
            .collect();
        self.markets.insert(condition_id.clone(), assets);
        Ok(())
    }

    fn contains_market(&self, condition_id: &str) -> bool {
        self.markets.contains_key(condition_id)
    }

    fn has_exact_directory_shape(&self, directory: &SessionMarketDirectoryV1) -> bool {
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

    fn release_market_matching_state_if_initialized(
        &mut self,
        condition_id: &str,
    ) -> Result<bool, MatchingStateErrorV1> {
        let initial_load_complete = self
            .markets
            .get(condition_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownMarket(condition_id.to_owned()))?
            .values()
            .all(|asset| asset.initial_loaded);
        if !initial_load_complete {
            return Ok(false);
        }
        self.release_market_matching_state(condition_id)?;
        Ok(true)
    }

    fn apply_authorized_event(
        &mut self,
        event: &ExecutionMarketEventV3,
    ) -> Result<QueueEventEffectsV1, MatchingStateErrorV1> {
        match &event.payload {
            ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) => {
                self.load_initial_snapshot(event, snapshot)?;
                Ok(QueueEventEffectsV1::default())
            }
            ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) => {
                self.apply_level_transition(event, transition)
            }
            ExecutionMarketEventPayloadV3::QualityBoundary(boundary)
                if event.quality.resync == ResyncQuality::Required =>
            {
                if let QualityScopeV1::Asset(asset) = &boundary.scope {
                    self.clear_external_segments(&event.market.condition_id.0, &asset.token_id.0)?;
                }
                Ok(QueueEventEffectsV1::default())
            }
            _ => Ok(QueueEventEffectsV1::default()),
        }
    }

    pub(crate) fn insert_account_buy_order(
        &mut self,
        condition_id: &str,
        token_id: &str,
        price: Decimal,
        platform_order_id: String,
        base_units: u128,
    ) -> Result<(), MatchingStateErrorV1> {
        if base_units == 0 {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        if self.contains_account_order(&platform_order_id) {
            return Err(MatchingStateErrorV1::AccountOrderConflict(
                platform_order_id,
            ));
        }
        self.asset_mut(condition_id, token_id)?
            .insert_account_buy_remainder(price, platform_order_id, base_units)
    }

    pub(crate) fn match_taker_buy_by_quote(
        &mut self,
        condition_id: &str,
        token_id: &str,
        worst_price: Decimal,
        quote_budget_units: u128,
    ) -> Result<QueueMatchPlanV1, MatchingStateErrorV1> {
        self.match_taker_buy(
            condition_id,
            token_id,
            worst_price,
            TakerTargetV1::Quote(quote_budget_units),
        )
    }

    pub(crate) fn prepare_taker_buy_by_quote_v2(
        &mut self,
        condition_id: &str,
        token_id: &str,
        worst_price: Decimal,
        quote_budget_units: u128,
    ) -> Result<PreparedTakerAssetQueueChangeV2<'_>, MatchingStateErrorV1> {
        self.prepare_taker_buy_v2(
            condition_id,
            token_id,
            worst_price,
            TakerTargetV1::Quote(quote_budget_units),
        )
    }

    pub(crate) fn match_taker_buy_by_base(
        &mut self,
        condition_id: &str,
        token_id: &str,
        limit_price: Decimal,
        base_target_units: u128,
        quote_budget_units: u128,
    ) -> Result<QueueMatchPlanV1, MatchingStateErrorV1> {
        self.match_taker_buy(
            condition_id,
            token_id,
            limit_price,
            TakerTargetV1::LimitBase {
                total_base_units: base_target_units,
                total_quote_units: quote_budget_units,
            },
        )
    }

    pub(crate) fn prepare_taker_buy_by_base_v2(
        &mut self,
        condition_id: &str,
        token_id: &str,
        limit_price: Decimal,
        base_target_units: u128,
        quote_budget_units: u128,
    ) -> Result<PreparedTakerAssetQueueChangeV2<'_>, MatchingStateErrorV1> {
        self.prepare_taker_buy_v2(
            condition_id,
            token_id,
            limit_price,
            TakerTargetV1::LimitBase {
                total_base_units: base_target_units,
                total_quote_units: quote_budget_units,
            },
        )
    }

    pub(crate) fn best_executable_ask(
        &self,
        condition_id: &str,
        token_id: &str,
    ) -> Option<Decimal> {
        self.asset(condition_id, token_id)
            .ok()?
            .asks
            .iter()
            .find(|(_, queue)| {
                queue.segments.iter().any(|segment| {
                    matches!(segment, QueueSegmentV1::External(external)
                        if external.total_quote_units > 0
                            && external.consumed_base_units < external.total_base_units)
                })
            })
            .map(|(price, _)| *price)
    }

    pub(crate) fn remaining_account_order_base_units(
        &self,
        platform_order_id: &str,
    ) -> Option<u128> {
        self.markets
            .values()
            .flat_map(BTreeMap::values)
            .flat_map(|asset| asset.bids.values().chain(asset.asks.values()))
            .flat_map(|queue| queue.segments.iter())
            .find_map(|segment| match segment {
                QueueSegmentV1::Account(account)
                    if account.platform_order_id == platform_order_id =>
                {
                    Some(account.remaining_base_units)
                }
                _ => None,
            })
    }

    fn venue_dust_candidate_for_order(
        &self,
        condition_id: &str,
        token_id: &str,
        price: Decimal,
        platform_order_id: &str,
    ) -> Option<VenueDustQueueCandidateV1> {
        self.asset(condition_id, token_id)
            .ok()?
            .venue_dust_candidate_for_order(price, platform_order_id)
    }

    fn release_market_matching_state(
        &mut self,
        condition_id: &str,
    ) -> Result<(), MatchingStateErrorV1> {
        let assets = self
            .markets
            .get_mut(condition_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownMarket(condition_id.to_owned()))?;
        for asset in assets.values_mut() {
            asset.bids.clear();
            asset.asks.clear();
        }
        Ok(())
    }

    fn remove_account_order(&mut self, platform_order_id: &str) {
        for assets in self.markets.values_mut() {
            for asset in assets.values_mut() {
                asset.remove_account_order(platform_order_id);
            }
        }
    }

    fn load_initial_snapshot(
        &mut self,
        event: &ExecutionMarketEventV3,
        snapshot: &InitialL2SnapshotV2,
    ) -> Result<(), MatchingStateErrorV1> {
        let asset = self.asset_mut(&event.market.condition_id.0, &snapshot.asset.token_id.0)?;
        if asset.initial_loaded {
            return Err(MatchingStateErrorV1::InitialQueueAlreadyLoaded(
                snapshot.asset.token_id.0.clone(),
            ));
        }
        for (side, levels) in [
            (BookSide::Bid, &snapshot.bids),
            (BookSide::Ask, &snapshot.asks),
        ] {
            for (ordinal, level) in levels.iter().enumerate() {
                let price = parse_decimal(&level.price_pusd_per_share.0)?;
                let quantity = parse_decimal(&level.quantity_shares.0)?;
                let base_units = micro_units_v1(quantity)?;
                if base_units == 0 {
                    continue;
                }
                let segment =
                    external_segment(&event.event_id.0, side, price, ordinal as u64, base_units)?;
                asset
                    .levels_mut(side)
                    .entry(price)
                    .or_default()
                    .segments
                    .push_back(QueueSegmentV1::External(segment));
            }
        }
        asset.initial_loaded = true;
        Ok(())
    }

    fn apply_level_transition(
        &mut self,
        event: &ExecutionMarketEventV3,
        transition: &LevelExecutionTransitionV3,
    ) -> Result<QueueEventEffectsV1, MatchingStateErrorV1> {
        let price = parse_decimal(&transition.price_pusd_per_share.0)?;
        let before = micro_units_v1(parse_decimal(&transition.quantity_before_shares.0)?)?;
        let after = micro_units_v1(parse_decimal(&transition.quantity_after_shares.0)?)?;
        let asset = self.asset_mut(&event.market.condition_id.0, &transition.asset.token_id.0)?;
        if !asset.initial_loaded {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        if after > before {
            let added = after - before;
            let ordinal = asset
                .levels(transition.side)
                .get(&price)
                .map_or(0, |queue| queue.segments.len() as u64);
            let segment =
                external_segment(&event.event_id.0, transition.side, price, ordinal, added)?;
            asset
                .levels_mut(transition.side)
                .entry(price)
                .or_default()
                .segments
                .push_back(QueueSegmentV1::External(segment));
            return Ok(QueueEventEffectsV1::default());
        }
        if before == after {
            return Ok(QueueEventEffectsV1::default());
        }
        let levels = asset.levels_mut(transition.side);
        let queue = levels
            .get_mut(&price)
            .ok_or(MatchingStateErrorV1::QueueUnderflow)?;
        let mut remaining = before - after;
        let debt_offset = remaining.min(queue.account_taker_depletion_debt_units);
        queue.account_taker_depletion_debt_units -= debt_offset;
        remaining -= debt_offset;
        if remaining == 0 {
            if queue.segments.is_empty() && queue.account_taker_depletion_debt_units == 0 {
                levels.remove(&price);
            }
            return Ok(QueueEventEffectsV1::default());
        }
        let maker_depletions = consume_queue_head(queue, &event.event_id.0, price, remaining)?;
        let venue_dust_candidate = if queue.account_taker_depletion_debt_units == 0
            && queue.segments.len() == 1
        {
            match queue.segments.front() {
                Some(QueueSegmentV1::Account(account)) if account.remaining_base_units > 0 => {
                    Some(VenueDustQueueCandidateV1 {
                        platform_order_id: account.platform_order_id.clone(),
                        price_pusd_per_share: price,
                        remaining_base_units: account.remaining_base_units,
                    })
                }
                Some(QueueSegmentV1::External(_)) | Some(QueueSegmentV1::Account(_)) | None => None,
            }
        } else {
            None
        };
        if queue.segments.is_empty() && queue.account_taker_depletion_debt_units == 0 {
            levels.remove(&price);
        }
        Ok(QueueEventEffectsV1 {
            maker_depletions,
            venue_dust_candidate,
        })
    }

    fn clear_external_segments(
        &mut self,
        condition_id: &str,
        token_id: &str,
    ) -> Result<(), MatchingStateErrorV1> {
        let asset = self.asset_mut(condition_id, token_id)?;
        for levels in [&mut asset.bids, &mut asset.asks] {
            levels.retain(|_, queue| {
                queue
                    .segments
                    .retain(|segment| matches!(segment, QueueSegmentV1::Account(_)));
                queue.account_taker_depletion_debt_units = 0;
                !queue.segments.is_empty()
            });
        }
        Ok(())
    }

    fn match_taker_buy(
        &mut self,
        condition_id: &str,
        token_id: &str,
        maximum_price: Decimal,
        target: TakerTargetV1,
    ) -> Result<QueueMatchPlanV1, MatchingStateErrorV1> {
        self.asset_mut(condition_id, token_id)?
            .match_taker_buy(maximum_price, target)
    }

    fn prepare_taker_buy_v2(
        &mut self,
        condition_id: &str,
        token_id: &str,
        maximum_price: Decimal,
        target: TakerTargetV1,
    ) -> Result<PreparedTakerAssetQueueChangeV2<'_>, MatchingStateErrorV1> {
        let mut candidate_asset = self.asset(condition_id, token_id)?.clone();
        let plan = candidate_asset.match_taker_buy(maximum_price, target)?;
        Ok(PreparedTakerAssetQueueChangeV2 {
            queues: self,
            condition_id: condition_id.to_owned(),
            token_id: token_id.to_owned(),
            candidate_asset,
            plan,
        })
    }

    fn contains_account_order(&self, platform_order_id: &str) -> bool {
        self.remaining_account_order_base_units(platform_order_id)
            .is_some()
    }

    fn asset(
        &self,
        condition_id: &str,
        token_id: &str,
    ) -> Result<&AssetQueueV1, MatchingStateErrorV1> {
        self.markets
            .get(condition_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownMarket(condition_id.to_owned()))?
            .get(token_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownAsset(token_id.to_owned()))
    }

    fn asset_mut(
        &mut self,
        condition_id: &str,
        token_id: &str,
    ) -> Result<&mut AssetQueueV1, MatchingStateErrorV1> {
        self.markets
            .get_mut(condition_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownMarket(condition_id.to_owned()))?
            .get_mut(token_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownAsset(token_id.to_owned()))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum SimOrderKindStateV1 {
    Gtc,
    Gtd { expires_at_utc_ms: i64 },
    Fok,
    Fak,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum SimOrderTerminalReasonV1 {
    FakCompleted,
    VenueDustTerminal,
    MarketResolvedCanceled,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CallerCancelStateEffectV1 {
    pub pending_base_units_before: u128,
    pub open_base_units_before: u128,
    pub released_open_quote_units: u128,
    pub account_delta: Option<Arc<PlatformAccountDeltaV1>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GtdExpirationEffectV1 {
    pub platform_order_id: String,
    pub condition_id: String,
    pub token_id: String,
    pub expiration_identity: Sha256Hex,
    pub pending_base_units_before: u128,
    pub open_base_units_before: u128,
    pub released_open_quote_units: u128,
    pub account_delta: Option<Arc<PlatformAccountDeltaV1>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MarketResolutionOrderEffectV1 {
    pub platform_order_id: String,
    pub condition_id: String,
    pub token_id: String,
    pub canceled_base_units: u128,
    pub released_open_quote_units: u128,
    pub account_delta: Arc<PlatformAccountDeltaV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OrderCommitEffectsV1 {
    pub account_deltas: Vec<Arc<PlatformAccountDeltaV1>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CancelOrderStateDispositionV1 {
    NotFound,
    NoCancellableRemainder,
    Canceled(CallerCancelStateEffectV1),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum SimLiquidityRoleV1 {
    Taker,
    MakerInferred,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SimOrderRecordV1 {
    pub platform_order_id: String,
    pub global_request_id: String,
    pub item_id: String,
    pub item_idempotency_identity: String,
    pub condition_id: String,
    pub token_id: String,
    pub created_at_utc_ms: i64,
    pub last_updated_at_utc_ms: i64,
    pub kind: SimOrderKindStateV1,
    pub source_price_pusd_per_share: Decimal,
    pub accepted_base_units: Option<u128>,
    pub matched_base_units: u128,
    pub pending_maker_depletion_base_units: u128,
    pub pending_maker_depletion_evidence: Option<MakerDepletionEvidenceV1>,
    pub open_base_units: u128,
    pub expired_base_units: u128,
    pub expired_released_quote_units: u128,
    pub expired_at_utc_ms: Option<i64>,
    pub gtd_expiration_identity: Option<Sha256Hex>,
    pub venue_dust_closed_base_units: u128,
    pub venue_dust_released_quote_units: u128,
    pub venue_dust_at_utc_ms: Option<i64>,
    pub venue_dust_identity: Option<Sha256Hex>,
    pub market_resolved_canceled_base_units: u128,
    pub market_resolved_released_quote_units: u128,
    pub market_resolved_at_utc_ms: Option<i64>,
    pub market_resolution_effect_identity: Option<Sha256Hex>,
    pub canceled_base_units: u128,
    pub canceled_released_quote_units: u128,
    pub canceled_at_utc_ms: Option<i64>,
    pub caller_cancel_effect_identity: Option<Sha256Hex>,
    pub terminalized_pending_maker_depletion_evidence: Option<MakerDepletionEvidenceV1>,
    pub accepted_quote_units: u128,
    pub matched_quote_units: u128,
    pub open_quote_units: u128,
    pub released_quote_units: u128,
    pub sdk_taker_amount_audit_base_units: Option<u128>,
    pub immediate_taker_fee_encumbered_units: u128,
    pub terminal_reason: Option<SimOrderTerminalReasonV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MakerDepletionEvidenceV1 {
    pub sha256: Sha256Hex,
    pub event_count: u64,
    pub first_source_event_id: String,
    pub last_source_event_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SimFillRecordV1 {
    pub trade_id: String,
    pub platform_order_id: String,
    pub condition_id: String,
    pub token_id: String,
    pub matched_at_utc_ms: i64,
    pub liquidity_role: SimLiquidityRoleV1,
    pub source_identity: String,
    pub maker_depletion_evidence: Option<MakerDepletionEvidenceV1>,
    pub price_pusd_per_share: Decimal,
    pub base_units: u128,
    pub quote_units: u128,
    pub immediate_taker_fee_units: u128,
    pub(crate) finality_path: Option<TradeFinalityPathV1>,
    pub(crate) finality_path_ordinal: Option<u32>,
    pub(crate) finality_state: Option<TradeFinalityStateV1>,
    pub(crate) economic_application_delta: Option<Arc<PlatformAccountDeltaV1>>,
}

impl SimFillRecordV1 {
    pub(crate) fn finality_state(&self) -> Option<TradeFinalityStateV1> {
        self.finality_state
    }

    pub(crate) fn economic_disposition(&self) -> TradeEconomicDispositionStateV1 {
        match self.finality_state {
            Some(TradeFinalityStateV1::Confirmed) => TradeEconomicDispositionStateV1::FinalSuccess,
            Some(TradeFinalityStateV1::Failed) if self.economic_application_delta.is_some() => {
                TradeEconomicDispositionStateV1::FinalFailureCompensated
            }
            Some(TradeFinalityStateV1::Failed) => {
                TradeEconomicDispositionStateV1::FinalFailureUnapplied
            }
            Some(_) if self.economic_application_delta.is_some() => {
                TradeEconomicDispositionStateV1::PendingApplied
            }
            Some(_) | None => TradeEconomicDispositionStateV1::PendingUnapplied,
        }
    }

    pub(crate) fn economic_application_delta(&self) -> Option<&Arc<PlatformAccountDeltaV1>> {
        self.economic_application_delta.as_ref()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TradeEconomicDispositionStateV1 {
    PendingUnapplied,
    PendingApplied,
    FinalSuccess,
    FinalFailureUnapplied,
    FinalFailureCompensated,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TradeFinalityTransitionEffectV1 {
    pub trade_id: String,
    pub platform_order_id: String,
    pub condition_id: String,
    pub token_id: String,
    pub platform_event_at_utc_ms: i64,
    pub finality_state: TradeFinalityStateV1,
    pub economic_disposition: TradeEconomicDispositionStateV1,
    pub liquidity_role: SimLiquidityRoleV1,
    pub base_units: u128,
    pub quote_units: u128,
    pub immediate_taker_fee_units: u128,
    pub economic_application_delta: Option<Arc<PlatformAccountDeltaV1>>,
    pub account_delta: Option<Arc<PlatformAccountDeltaV1>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SettlementLifecycleEffectV1 {
    pub action: SettlementLifecycleActionV1,
    pub account_delta: Option<Arc<PlatformAccountDeltaV1>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SettlementLifecycleActionV1 {
    Rebate(RebateSettlementActionV1),
    AutoRedeem(AutoRedeemSettlementActionV1),
}

impl SettlementLifecycleActionV1 {
    pub(crate) fn reason_identity(&self) -> &str {
        match self {
            Self::Rebate(action) => action.reason_identity(),
            Self::AutoRedeem(action) => &action.reason_identity,
        }
    }

    pub(crate) fn platform_effective_at_utc_ms(&self) -> i64 {
        match self {
            Self::Rebate(action) => action.platform_effective_at_utc_ms(),
            Self::AutoRedeem(action) => action.platform_effective_at_utc_ms,
        }
    }

    pub(crate) fn redeemed_resolution(&self) -> Option<(&str, &MarketResolutionTerminalV3)> {
        match self {
            Self::AutoRedeem(action) => Some((&action.condition_id, &action.resolution)),
            Self::Rebate(_) => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AutoRedeemSettlementActionV1 {
    pub reason_identity: String,
    pub condition_id: String,
    pub resolution_event_id: String,
    pub resolution_source_event_at_utc_ms: i64,
    pub platform_effective_at_utc_ms: i64,
    pub resolution: MarketResolutionTerminalV3,
    pub redeemed_outcome_balances: Vec<PlatformOutcomeBalanceV1>,
    pub redeemed_pusd: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TradeFinalityLifecycleEffectsV1 {
    pub trade_feedback: TradeFinalityTransitionEffectV1,
    pub settlement_feedbacks: Vec<SettlementLifecycleEffectV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct ScheduledFinalityTransitionKeyV1 {
    platform_event_at_utc_ms: i64,
    fill_economic_delta_identity: String,
    path_ordinal: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TerminalAccountDeltaReferenceV1 {
    pub(crate) account_transition_seq: u64,
    pub(crate) delta_id: String,
    pub(crate) delta_sha256: Sha256Hex,
}

impl From<&PlatformAccountDeltaV1> for TerminalAccountDeltaReferenceV1 {
    fn from(delta: &PlatformAccountDeltaV1) -> Self {
        Self {
            account_transition_seq: delta.account_transition_seq,
            delta_id: delta.delta_id.clone(),
            delta_sha256: delta.delta_sha256.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TerminalFillEvictionWitnessV1 {
    pub(crate) rebate_fill: RebateRecoveryFillV1,
    pub(crate) token_id: String,
    pub(crate) quote_units: u128,
    pub(crate) finality_selector_identity: Option<String>,
    pub(crate) economic_application_delta: Option<TerminalAccountDeltaReferenceV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TerminalExecutionMarketEvictionV1 {
    pub(crate) condition_id: String,
    pub(crate) fills: BTreeMap<String, TerminalFillEvictionWitnessV1>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RecoveredTerminalFinalityV1 {
    terminal_ordinal: u32,
    terminal_at_utc_ms: i64,
    economic_application_ordinal: Option<u32>,
    economic_application_at_utc_ms: Option<i64>,
}

fn terminal_finality_selector_identity(
    policy: &TradeFinalityPolicyV1,
    order: &SimOrderRecordV1,
) -> Option<String> {
    match policy.override_selector_domain() {
        Some(TradeFinalitySelectorDomainV1::Request) => Some(order.global_request_id.clone()),
        Some(TradeFinalitySelectorDomainV1::BatchItem) => {
            Some(order.item_idempotency_identity.clone())
        }
        Some(TradeFinalitySelectorDomainV1::Fill) | None => None,
    }
}

fn recovered_terminal_finality(
    account_instance_key: &contracts::AccountInstanceKeyV1,
    policy: &TradeFinalityPolicyV1,
    witness: &TerminalFillEvictionWitnessV1,
) -> Result<RecoveredTerminalFinalityV1, MatchingStateErrorV1> {
    let trade_id = witness.rebate_fill.trade_id.as_str();
    let (global_request_id, item_idempotency_identity) = match policy.override_selector_domain() {
        Some(TradeFinalitySelectorDomainV1::Request) => (
            witness
                .finality_selector_identity
                .as_deref()
                .ok_or_else(|| {
                    MatchingStateErrorV1::Finality(format!(
                        "驱逐成交缺少请求级最终性选择身份：{trade_id}"
                    ))
                })?,
            "",
        ),
        Some(TradeFinalitySelectorDomainV1::BatchItem) => (
            "",
            witness
                .finality_selector_identity
                .as_deref()
                .ok_or_else(|| {
                    MatchingStateErrorV1::Finality(format!(
                        "驱逐成交缺少批次项最终性选择身份：{trade_id}"
                    ))
                })?,
        ),
        Some(TradeFinalitySelectorDomainV1::Fill) | None => {
            if witness.finality_selector_identity.is_some() {
                return Err(MatchingStateErrorV1::Finality(format!(
                    "驱逐成交含不适用的最终性选择身份：{trade_id}"
                )));
            }
            ("", "")
        }
    };
    let path = policy.select_path_for_fill(
        account_instance_key,
        global_request_id,
        item_idempotency_identity,
        trade_id,
    );
    let terminal_index = path.steps().len().checked_sub(1).ok_or_else(|| {
        MatchingStateErrorV1::Finality(format!("驱逐成交最终性路径为空：{trade_id}"))
    })?;
    let terminal_ordinal = u32::try_from(terminal_index).map_err(|_| {
        MatchingStateErrorV1::Finality(format!("驱逐成交最终性路径过长：{trade_id}"))
    })?;
    let terminal_step = &path.steps()[terminal_index];
    let expected_terminal_state = match witness.rebate_fill.terminal_succeeded {
        Some(true) => TradeFinalityStateV1::Confirmed,
        Some(false) => TradeFinalityStateV1::Failed,
        None => {
            return Err(MatchingStateErrorV1::Finality(format!(
                "驱逐成交缺少最终性终态：{trade_id}"
            )));
        }
    };
    if terminal_step.state() != expected_terminal_state {
        return Err(MatchingStateErrorV1::Finality(format!(
            "驱逐成交终态不等于已密封策略选择结果：{trade_id}"
        )));
    }
    let terminal_at_utc_ms = witness
        .rebate_fill
        .matched_at_utc_ms
        .checked_add(terminal_step.offset_ms())
        .ok_or_else(|| {
            MatchingStateErrorV1::Finality(format!("驱逐成交最终性时点溢出：{trade_id}"))
        })?;
    let economic_application_ordinal = path.economic_application_ordinal();
    let economic_application_at_utc_ms = economic_application_ordinal
        .map(|ordinal| {
            let index = usize::try_from(ordinal).map_err(|_| {
                MatchingStateErrorV1::Finality(format!("驱逐成交经济截点越界：{trade_id}"))
            })?;
            let step = path.steps().get(index).ok_or_else(|| {
                MatchingStateErrorV1::Finality(format!("驱逐成交经济截点超出路径：{trade_id}"))
            })?;
            witness
                .rebate_fill
                .matched_at_utc_ms
                .checked_add(step.offset_ms())
                .ok_or_else(|| {
                    MatchingStateErrorV1::Finality(format!("驱逐成交经济截点时点溢出：{trade_id}"))
                })
        })
        .transpose()?;
    if witness.economic_application_delta.is_some() != economic_application_ordinal.is_some() {
        return Err(MatchingStateErrorV1::Finality(format!(
            "驱逐成交经济变化与已密封策略截点不一致：{trade_id}"
        )));
    }
    Ok(RecoveredTerminalFinalityV1 {
        terminal_ordinal,
        terminal_at_utc_ms,
        economic_application_ordinal,
        economic_application_at_utc_ms,
    })
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionStateV1 {
    queues: ExecutionQueueStateV1,
    account_overlay: AccountBookOverlayStoreV1,
    account: PlatformAccountStateV1,
    orders: BTreeMap<String, SimOrderRecordV1>,
    fills: BTreeMap<String, SimFillRecordV1>,
    matching_closed_markets: BTreeMap<String, i64>,
    gtd_deadlines: BTreeSet<(i64, String)>,
    gtd_advanced_through_utc_ms: Option<i64>,
    scheduled_finality: BTreeSet<ScheduledFinalityTransitionKeyV1>,
    finality_advanced_through_utc_ms: Option<i64>,
    rebate: Arc<RebateStateV1>,
    market_resolutions: Arc<BTreeMap<String, MarketResolutionObligationV1>>,
}

#[cfg(test)]
std::thread_local! {
    static EXECUTION_STATE_CLONE_COUNT_FOR_TESTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static GTD_LIVE_DEADLINE_ENTRY_VISIT_COUNT_FOR_TESTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static SCHEDULED_FINALITY_INSERTED_ENTRY_VISIT_COUNT_FOR_TESTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

impl Clone for ExecutionStateV1 {
    fn clone(&self) -> Self {
        #[cfg(test)]
        EXECUTION_STATE_CLONE_COUNT_FOR_TESTS.with(|count| count.set(count.get() + 1));
        Self {
            queues: self.queues.clone(),
            account_overlay: self.account_overlay.clone(),
            account: self.account.clone(),
            orders: self.orders.clone(),
            fills: self.fills.clone(),
            matching_closed_markets: self.matching_closed_markets.clone(),
            gtd_deadlines: self.gtd_deadlines.clone(),
            gtd_advanced_through_utc_ms: self.gtd_advanced_through_utc_ms,
            scheduled_finality: self.scheduled_finality.clone(),
            finality_advanced_through_utc_ms: self.finality_advanced_through_utc_ms,
            rebate: Arc::clone(&self.rebate),
            market_resolutions: Arc::clone(&self.market_resolutions),
        }
    }
}

#[cfg(test)]
pub(crate) fn reset_execution_state_clone_count_for_tests() {
    EXECUTION_STATE_CLONE_COUNT_FOR_TESTS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn execution_state_clone_count_for_tests() -> usize {
    EXECUTION_STATE_CLONE_COUNT_FOR_TESTS.with(std::cell::Cell::get)
}

fn record_gtd_live_deadline_entry_visit_for_tests() {
    #[cfg(test)]
    GTD_LIVE_DEADLINE_ENTRY_VISIT_COUNT_FOR_TESTS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
fn reset_gtd_live_deadline_entry_visit_count_for_tests() {
    GTD_LIVE_DEADLINE_ENTRY_VISIT_COUNT_FOR_TESTS.with(|count| count.set(0));
}

#[cfg(test)]
fn gtd_live_deadline_entry_visit_count_for_tests() -> usize {
    GTD_LIVE_DEADLINE_ENTRY_VISIT_COUNT_FOR_TESTS.with(std::cell::Cell::get)
}

fn record_scheduled_finality_inserted_entry_visit_for_tests() {
    #[cfg(test)]
    SCHEDULED_FINALITY_INSERTED_ENTRY_VISIT_COUNT_FOR_TESTS
        .with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
fn reset_scheduled_finality_inserted_entry_visit_count_for_tests() {
    SCHEDULED_FINALITY_INSERTED_ENTRY_VISIT_COUNT_FOR_TESTS.with(|count| count.set(0));
}

#[cfg(test)]
fn scheduled_finality_inserted_entry_visit_count_for_tests() -> usize {
    SCHEDULED_FINALITY_INSERTED_ENTRY_VISIT_COUNT_FOR_TESTS.with(std::cell::Cell::get)
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ExecutionTouchedBidLevelKeyV2 {
    condition_id: String,
    token_id: String,
    price_pusd_per_share: Decimal,
}

struct PreparedGtdExpirationPatchV2 {
    last_consumed_live_deadline: Option<(i64, String)>,
    deadline_removals: BTreeSet<(i64, String)>,
    bid_queue_after_values: BTreeMap<ExecutionTouchedBidLevelKeyV2, Option<PriceQueueV1>>,
    overlay_after_values: BTreeMap<ExecutionTouchedBidLevelKeyV2, Decimal>,
    gtd_advanced_through_utc_ms: Option<i64>,
    effects: Vec<GtdExpirationEffectV1>,
}

#[cfg_attr(test, derive(Clone, Debug, PartialEq, Eq))]
struct PreparedScheduledFinalityPatchV2 {
    last_consumed_live_key: Option<ScheduledFinalityTransitionKeyV1>,
    schedule_removals: BTreeSet<ScheduledFinalityTransitionKeyV1>,
    schedule_insertions: BTreeSet<ScheduledFinalityTransitionKeyV1>,
    fill_replacements: BTreeMap<String, SimFillRecordV1>,
    rebate: RebateStatePatchV2,
    resolution: PreparedMarketResolutionPatchV2,
    finality_advanced_through_utc_ms: Option<i64>,
    effects: Vec<TradeFinalityLifecycleEffectsV1>,
    rebate_clock_effects: Vec<SettlementLifecycleEffectV1>,
}

#[derive(Default)]
#[cfg_attr(test, derive(Clone, Debug, PartialEq, Eq))]
struct PreparedMarketResolutionPatchV2 {
    pending_trade_removals: BTreeMap<String, BTreeSet<String>>,
    redeemed_at_after: BTreeMap<String, i64>,
}

pub(crate) struct PreparedExecutionDriveDeltaV2<'a> {
    live: &'a mut ExecutionStateV1,
    gtd: PreparedGtdExpirationPatchV2,
    finality: PreparedScheduledFinalityPatchV2,
    order_replacements: BTreeMap<String, SimOrderRecordV1>,
    candidate_account: Option<PlatformAccountStateV1>,
}

pub(crate) struct CommittableExecutionDriveDeltaV2<'a> {
    live: &'a mut ExecutionStateV1,
    gtd: PreparedGtdExpirationPatchV2,
    finality: PreparedScheduledFinalityPatchV2,
    order_replacements: BTreeMap<String, SimOrderRecordV1>,
    candidate_account: Option<PlatformAccountStateV1>,
}

impl PreparedGtdExpirationPatchV2 {
    fn prepare(
        live: &ExecutionStateV1,
        logical_time_utc_ms: i64,
        order_replacements: &mut BTreeMap<String, SimOrderRecordV1>,
        candidate_account: &mut Option<PlatformAccountStateV1>,
    ) -> Result<Self, MatchingStateErrorV1> {
        if live
            .gtd_advanced_through_utc_ms
            .is_some_and(|current| logical_time_utc_ms < current)
        {
            return Err(MatchingStateErrorV1::LogicalTimeRegression);
        }
        let mut patch = Self::prepare_incremental(live);
        while let Some(next_at_utc_ms) = patch.candidate_next_gtd_at(live) {
            if next_at_utc_ms > logical_time_utc_ms {
                break;
            }
            let _ =
                patch.stage_gtd_at(live, order_replacements, candidate_account, next_at_utc_ms)?;
        }
        patch.gtd_advanced_through_utc_ms = Some(logical_time_utc_ms);
        Ok(patch)
    }

    fn prepare_incremental(live: &ExecutionStateV1) -> Self {
        Self {
            last_consumed_live_deadline: None,
            deadline_removals: BTreeSet::new(),
            bid_queue_after_values: BTreeMap::new(),
            overlay_after_values: BTreeMap::new(),
            gtd_advanced_through_utc_ms: live.gtd_advanced_through_utc_ms,
            effects: Vec::new(),
        }
    }

    fn unconsumed_live_deadlines<'a>(
        &'a self,
        live: &'a ExecutionStateV1,
    ) -> impl Iterator<Item = &'a (i64, String)> {
        let lower_bound = self
            .last_consumed_live_deadline
            .as_ref()
            .map_or(Unbounded, Excluded);
        live.gtd_deadlines.range((lower_bound, Unbounded))
    }

    fn candidate_next_gtd_at(&self, live: &ExecutionStateV1) -> Option<i64> {
        self.unconsumed_live_deadlines(live)
            .find(|deadline| {
                record_gtd_live_deadline_entry_visit_for_tests();
                !self.deadline_removals.contains(*deadline)
            })
            .map(|(expires_at_utc_ms, _)| *expires_at_utc_ms)
    }

    fn stage_gtd_at<'a>(
        &'a mut self,
        live: &ExecutionStateV1,
        order_replacements: &mut BTreeMap<String, SimOrderRecordV1>,
        candidate_account: &mut Option<PlatformAccountStateV1>,
        exact_next_at_utc_ms: i64,
    ) -> Result<&'a [GtdExpirationEffectV1], MatchingStateErrorV1> {
        let first_new_effect = self.effects.len();
        if self.candidate_next_gtd_at(live) != Some(exact_next_at_utc_ms) {
            return Ok(&self.effects[first_new_effect..]);
        }
        let expiring = self
            .unconsumed_live_deadlines(live)
            .inspect(|_| record_gtd_live_deadline_entry_visit_for_tests())
            .take_while(|(expires_at_utc_ms, _)| *expires_at_utc_ms == exact_next_at_utc_ms)
            .filter(|deadline| !self.deadline_removals.contains(*deadline))
            .cloned()
            .collect::<Vec<_>>();
        if let Some(last_deadline) = expiring.last() {
            self.last_consumed_live_deadline = Some(last_deadline.clone());
        }
        self.stage_expiring(live, order_replacements, candidate_account, expiring)?;
        Ok(&self.effects[first_new_effect..])
    }

    fn stage_expiring(
        &mut self,
        live: &ExecutionStateV1,
        order_replacements: &mut BTreeMap<String, SimOrderRecordV1>,
        candidate_account: &mut Option<PlatformAccountStateV1>,
        expiring: Vec<(i64, String)>,
    ) -> Result<(), MatchingStateErrorV1> {
        let patch = self;
        for (expires_at_utc_ms, platform_order_id) in expiring {
            patch
                .deadline_removals
                .insert((expires_at_utc_ms, platform_order_id.clone()));
            let snapshot = order_replacements
                .get(&platform_order_id)
                .or_else(|| live.orders.get(&platform_order_id))
                .cloned()
                .ok_or_else(|| {
                    MatchingStateErrorV1::AccountOrderConflict(platform_order_id.clone())
                })?;
            let active_base_units = snapshot
                .pending_maker_depletion_base_units
                .checked_add(snapshot.open_base_units)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            if active_base_units == 0 {
                continue;
            }
            if snapshot.expired_base_units != 0
                || snapshot.expired_released_quote_units != 0
                || snapshot.expired_at_utc_ms.is_some()
                || snapshot.gtd_expiration_identity.is_some()
                || snapshot.venue_dust_closed_base_units != 0
                || snapshot.venue_dust_released_quote_units != 0
                || snapshot.venue_dust_at_utc_ms.is_some()
                || snapshot.venue_dust_identity.is_some()
                || snapshot.market_resolved_canceled_base_units != 0
                || snapshot.market_resolved_released_quote_units != 0
                || snapshot.market_resolved_at_utc_ms.is_some()
                || snapshot.market_resolution_effect_identity.is_some()
                || snapshot.canceled_base_units != 0
                || snapshot.canceled_released_quote_units != 0
                || snapshot.canceled_at_utc_ms.is_some()
                || snapshot.caller_cancel_effect_identity.is_some()
                || snapshot
                    .terminalized_pending_maker_depletion_evidence
                    .is_some()
            {
                return Err(MatchingStateErrorV1::QueuePredecessorConflict);
            }
            let matching_closed = live
                .matching_closed_markets
                .contains_key(&snapshot.condition_id);
            let level_key = ExecutionTouchedBidLevelKeyV2 {
                condition_id: snapshot.condition_id.clone(),
                token_id: snapshot.token_id.clone(),
                price_pusd_per_share: snapshot.source_price_pusd_per_share,
            };
            if snapshot.open_base_units > 0 && !matching_closed {
                if !patch.bid_queue_after_values.contains_key(&level_key) {
                    let queue = live
                        .queues
                        .asset(&level_key.condition_id, &level_key.token_id)?
                        .bids
                        .get(&level_key.price_pusd_per_share)
                        .cloned()
                        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                    patch
                        .bid_queue_after_values
                        .insert(level_key.clone(), Some(queue));
                }
                let level_after = patch
                    .bid_queue_after_values
                    .get_mut(&level_key)
                    .expect("已初始化 GTD touched bid level");
                let queue_after = level_after
                    .as_mut()
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                if queue_after.unique_account_order_base_units(&platform_order_id)?
                    != Some(snapshot.open_base_units)
                {
                    return Err(MatchingStateErrorV1::QueuePredecessorConflict);
                }
                queue_after.remove_account_order(&platform_order_id);
                if queue_after.segments.is_empty()
                    && queue_after.account_taker_depletion_debt_units == 0
                {
                    *level_after = None;
                }
            }
            let accepted_base = snapshot
                .accepted_base_units
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            if accepted_base
                != snapshot
                    .matched_base_units
                    .checked_add(snapshot.expired_base_units)
                    .and_then(|value| value.checked_add(snapshot.venue_dust_closed_base_units))
                    .and_then(|value| {
                        value.checked_add(snapshot.market_resolved_canceled_base_units)
                    })
                    .and_then(|value| value.checked_add(snapshot.canceled_base_units))
                    .and_then(|value| value.checked_add(active_base_units))
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
                || snapshot.accepted_quote_units
                    != snapshot
                        .matched_quote_units
                        .checked_add(snapshot.released_quote_units)
                        .and_then(|value| value.checked_add(snapshot.canceled_released_quote_units))
                        .and_then(|value| value.checked_add(snapshot.expired_released_quote_units))
                        .and_then(|value| {
                            value.checked_add(snapshot.venue_dust_released_quote_units)
                        })
                        .and_then(|value| {
                            value.checked_add(snapshot.market_resolved_released_quote_units)
                        })
                        .and_then(|value| value.checked_add(snapshot.open_quote_units))
                        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
            {
                return Err(MatchingStateErrorV1::QueuePredecessorConflict);
            }
            let expiration_identity = gtd_expiration_identity(
                &snapshot.platform_order_id,
                expires_at_utc_ms,
                snapshot.pending_maker_depletion_base_units,
                snapshot.open_base_units,
                snapshot.open_quote_units,
                snapshot.pending_maker_depletion_evidence.as_ref(),
            );
            if candidate_account.is_none() {
                *candidate_account = Some(live.account.clone());
            }
            let account_delta = candidate_account
                .as_mut()
                .expect("活动 GTD 到期必须已有候选账户")
                .apply_gtd_expiration_release(
                    &expiration_identity.0,
                    &snapshot.condition_id,
                    expires_at_utc_ms,
                    decimal_from_micro_units_v1(snapshot.open_quote_units)?,
                )?;
            if snapshot.open_base_units > 0 {
                let quantity = decimal_from_micro_units_v1(snapshot.open_base_units)?;
                let current = patch
                    .overlay_after_values
                    .get(&level_key)
                    .copied()
                    .unwrap_or_else(|| {
                        live.account_overlay.quantity(
                            &level_key.condition_id,
                            &level_key.token_id,
                            BookSide::Bid,
                            level_key.price_pusd_per_share,
                        )
                    });
                let next = current
                    .checked_sub(quantity)
                    .filter(|value| !value.is_sign_negative())
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                patch.overlay_after_values.insert(level_key, next);
            }
            let mut replacement = snapshot.clone();
            replacement.expired_base_units = active_base_units;
            replacement.expired_released_quote_units = replacement.open_quote_units;
            replacement.expired_at_utc_ms = Some(expires_at_utc_ms);
            replacement.last_updated_at_utc_ms = expires_at_utc_ms;
            replacement.gtd_expiration_identity = Some(expiration_identity.clone());
            replacement.terminalized_pending_maker_depletion_evidence =
                replacement.pending_maker_depletion_evidence.take();
            replacement.pending_maker_depletion_base_units = 0;
            replacement.open_base_units = 0;
            replacement.open_quote_units = 0;
            order_replacements.insert(platform_order_id.clone(), replacement);
            patch.effects.push(GtdExpirationEffectV1 {
                platform_order_id,
                condition_id: snapshot.condition_id,
                token_id: snapshot.token_id,
                expiration_identity,
                pending_base_units_before: snapshot.pending_maker_depletion_base_units,
                open_base_units_before: snapshot.open_base_units,
                released_open_quote_units: snapshot.open_quote_units,
                account_delta,
            });
        }
        Ok(())
    }

    fn has_state_change(&self, live: &ExecutionStateV1) -> bool {
        !self.deadline_removals.is_empty()
            || !self.bid_queue_after_values.is_empty()
            || !self.overlay_after_values.is_empty()
            || self.gtd_advanced_through_utc_ms != live.gtd_advanced_through_utc_ms
    }

    fn commit_prevalidated(self, live: &mut ExecutionStateV1) {
        for deadline in self.deadline_removals {
            live.gtd_deadlines.remove(&deadline);
        }
        for (key, after) in self.bid_queue_after_values {
            let levels = &mut live
                .queues
                .asset_mut(&key.condition_id, &key.token_id)
                .expect("独占准备期间 GTD touched asset 必须仍然存在")
                .bids;
            if let Some(queue) = after {
                levels.insert(key.price_pusd_per_share, queue);
            } else {
                levels.remove(&key.price_pusd_per_share);
            }
        }
        for (key, quantity) in self.overlay_after_values {
            live.account_overlay.set_quantity(
                &key.condition_id,
                &key.token_id,
                BookSide::Bid,
                key.price_pusd_per_share,
                quantity,
            );
        }
        live.gtd_advanced_through_utc_ms = self.gtd_advanced_through_utc_ms;
    }
}

impl PreparedMarketResolutionPatchV2 {
    fn stage_pending_trade_removal(
        &mut self,
        live: &BTreeMap<String, MarketResolutionObligationV1>,
        condition_id: &str,
        trade_id: &str,
    ) {
        let Some(obligation) = live.get(condition_id) else {
            return;
        };
        if obligation.pending_trade_ids.contains(trade_id) {
            self.pending_trade_removals
                .entry(condition_id.to_owned())
                .or_default()
                .insert(trade_id.to_owned());
        }
    }

    fn candidate_pending_is_empty(
        &self,
        obligation: &MarketResolutionObligationV1,
        condition_id: &str,
    ) -> Result<bool, MatchingStateErrorV1> {
        let removal_count = self
            .pending_trade_removals
            .get(condition_id)
            .map_or(0, BTreeSet::len);
        let remaining = obligation
            .pending_trade_ids
            .len()
            .checked_sub(removal_count)
            .ok_or_else(|| MatchingStateErrorV1::ResolutionConflict(condition_id.to_owned()))?;
        Ok(remaining == 0)
    }

    fn stage_auto_redeem(
        &mut self,
        live: &BTreeMap<String, MarketResolutionObligationV1>,
        live_rebate: &RebateStateV1,
        rebate_patch: &RebateStatePatchV2,
        candidate_account: &mut PlatformAccountStateV1,
        condition_id: &str,
        platform_effective_at_utc_ms: i64,
    ) -> Result<Option<SettlementLifecycleEffectV1>, MatchingStateErrorV1> {
        let Some(obligation) = live.get(condition_id) else {
            return Ok(None);
        };
        if self.redeemed_at_after.contains_key(condition_id)
            || obligation.redeemed_at_utc_ms.is_some()
            || !self.candidate_pending_is_empty(obligation, condition_id)?
            || rebate_patch
                .candidate_market_result(live_rebate, condition_id)
                .is_none()
        {
            return Ok(None);
        }
        if obligation.invalidated_by_quality_reason.is_some() {
            return Err(MatchingStateErrorV1::ResolutionConflict(
                condition_id.to_owned(),
            ));
        }
        let effect = prepare_auto_redeem_effect_v2(
            candidate_account,
            obligation,
            condition_id,
            platform_effective_at_utc_ms,
        )?;
        self.redeemed_at_after
            .insert(condition_id.to_owned(), platform_effective_at_utc_ms);
        Ok(Some(effect))
    }

    fn has_state_change(&self) -> bool {
        !self.pending_trade_removals.is_empty() || !self.redeemed_at_after.is_empty()
    }

    fn changed_condition_id(&self) -> Option<&str> {
        self.pending_trade_removals
            .keys()
            .chain(self.redeemed_at_after.keys())
            .next()
            .map(String::as_str)
    }

    fn commit_prevalidated(self, live: &mut BTreeMap<String, MarketResolutionObligationV1>) {
        for (condition_id, removals) in self.pending_trade_removals {
            let obligation = live
                .get_mut(&condition_id)
                .expect("判定 pending 稀疏移除只指向已验证义务");
            for trade_id in removals {
                assert!(
                    obligation.pending_trade_ids.remove(&trade_id),
                    "判定 pending 稀疏移除只指向已验证成交"
                );
            }
        }
        for (condition_id, redeemed_at_utc_ms) in self.redeemed_at_after {
            live.get_mut(&condition_id)
                .expect("判定赎回 after-image 只指向已验证义务")
                .redeemed_at_utc_ms = Some(redeemed_at_utc_ms);
        }
    }
}

impl PreparedScheduledFinalityPatchV2 {
    fn prepare_incremental(live: &ExecutionStateV1) -> Self {
        Self {
            last_consumed_live_key: None,
            schedule_removals: BTreeSet::new(),
            schedule_insertions: BTreeSet::new(),
            fill_replacements: BTreeMap::new(),
            rebate: RebateStatePatchV2::default(),
            resolution: PreparedMarketResolutionPatchV2::default(),
            finality_advanced_through_utc_ms: live.finality_advanced_through_utc_ms,
            effects: Vec::new(),
            rebate_clock_effects: Vec::new(),
        }
    }

    fn unconsumed_live_schedule<'a>(
        &'a self,
        live: &'a ExecutionStateV1,
    ) -> impl Iterator<Item = &'a ScheduledFinalityTransitionKeyV1> {
        let lower_bound = self
            .last_consumed_live_key
            .as_ref()
            .map_or(Unbounded, Excluded);
        live.scheduled_finality.range((lower_bound, Unbounded))
    }

    fn candidate_next_finality_at(&self, live: &ExecutionStateV1) -> Option<i64> {
        let live_next = self
            .unconsumed_live_schedule(live)
            .find(|key| !self.schedule_removals.contains(*key));
        let inserted_next = self.schedule_insertions.first();
        match (live_next, inserted_next) {
            (Some(live), Some(inserted)) => {
                Some(if live <= inserted { live } else { inserted }.platform_event_at_utc_ms)
            }
            (Some(live), None) => Some(live.platform_event_at_utc_ms),
            (None, Some(inserted)) => Some(inserted.platform_event_at_utc_ms),
            (None, None) => None,
        }
    }

    fn candidate_next_rebate_at(&self, live: &ExecutionStateV1) -> Option<i64> {
        self.rebate.candidate_next_clock(live.rebate.as_ref())
    }

    fn stage_rebate_clock_at<'a>(
        &'a mut self,
        live: &ExecutionStateV1,
        candidate_account: &mut Option<PlatformAccountStateV1>,
        exact_next_at_utc_ms: i64,
    ) -> Result<&'a [SettlementLifecycleEffectV1], MatchingStateErrorV1> {
        let first_new_effect = self.rebate_clock_effects.len();
        let actions = self
            .rebate
            .stage_clock_at(live.rebate.as_ref(), exact_next_at_utc_ms)
            .to_vec();
        if actions.is_empty() {
            return Ok(&self.rebate_clock_effects[first_new_effect..]);
        }
        if candidate_account.is_none() {
            *candidate_account = Some(live.account.clone());
        }
        let account = candidate_account
            .as_mut()
            .expect("返佣时钟动作使用唯一候选账户");
        for action in actions {
            let finalized_condition_id = match &action {
                RebateSettlementActionV1::MakerMarketFinalized(finalized) => {
                    Some(finalized.condition_id.clone())
                }
                RebateSettlementActionV1::TakerAccrualApplied(_)
                | RebateSettlementActionV1::DayFinalized(_)
                | RebateSettlementActionV1::Paid(_) => None,
            };
            let platform_effective_at_utc_ms = action.platform_effective_at_utc_ms();
            self.rebate_clock_effects
                .push(apply_rebate_action_to_account_v2(account, action)?);
            if let Some(condition_id) = finalized_condition_id
                && let Some(auto_redeem) = self.resolution.stage_auto_redeem(
                    live.market_resolutions.as_ref(),
                    live.rebate.as_ref(),
                    &self.rebate,
                    account,
                    &condition_id,
                    platform_effective_at_utc_ms,
                )?
            {
                self.rebate_clock_effects.push(auto_redeem);
            }
        }
        Ok(&self.rebate_clock_effects[first_new_effect..])
    }

    fn stage_finality_at<'a>(
        &'a mut self,
        live: &ExecutionStateV1,
        order_replacements: &mut BTreeMap<String, SimOrderRecordV1>,
        candidate_account: &mut Option<PlatformAccountStateV1>,
        exact_next_at_utc_ms: i64,
    ) -> Result<&'a [TradeFinalityLifecycleEffectsV1], MatchingStateErrorV1> {
        let first_new_effect = self.effects.len();
        if self.candidate_next_finality_at(live) != Some(exact_next_at_utc_ms) {
            return Ok(&self.effects[first_new_effect..]);
        }
        let live_keys = self
            .unconsumed_live_schedule(live)
            .take_while(|key| key.platform_event_at_utc_ms == exact_next_at_utc_ms)
            .filter(|key| !self.schedule_removals.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        let inserted_lower_bound = Included(ScheduledFinalityTransitionKeyV1 {
            platform_event_at_utc_ms: exact_next_at_utc_ms,
            fill_economic_delta_identity: String::new(),
            path_ordinal: u32::MIN,
        });
        let inserted_upper_bound = exact_next_at_utc_ms
            .checked_add(1)
            .map_or(Unbounded, |next| {
                Excluded(ScheduledFinalityTransitionKeyV1 {
                    platform_event_at_utc_ms: next,
                    fill_economic_delta_identity: String::new(),
                    path_ordinal: u32::MIN,
                })
            });
        let inserted_keys = self
            .schedule_insertions
            .range((inserted_lower_bound, inserted_upper_bound))
            .inspect(|_| record_scheduled_finality_inserted_entry_visit_for_tests())
            .cloned()
            .collect::<Vec<_>>();
        let keys = live_keys
            .iter()
            .chain(&inserted_keys)
            .cloned()
            .collect::<BTreeSet<_>>();

        for key in &keys {
            if live.scheduled_finality.contains(key) && self.schedule_insertions.contains(key) {
                return Err(MatchingStateErrorV1::Finality(format!(
                    "最终性候选重复排定：{}:{}",
                    key.fill_economic_delta_identity, key.path_ordinal
                )));
            }
            let fill = self
                .fill_replacements
                .get(&key.fill_economic_delta_identity)
                .or_else(|| live.fills.get(&key.fill_economic_delta_identity))
                .ok_or_else(|| {
                    MatchingStateErrorV1::Finality(format!(
                        "已排定成交缺少最终性实例：{}",
                        key.fill_economic_delta_identity
                    ))
                })?;
            let path = fill.finality_path.as_ref().ok_or_else(|| {
                MatchingStateErrorV1::Finality(format!(
                    "已排定成交缺少最终性路径：{}",
                    key.fill_economic_delta_identity
                ))
            })?;
            if fill.trade_id != key.fill_economic_delta_identity
                || fill
                    .finality_path_ordinal
                    .and_then(|current| current.checked_add(1))
                    != Some(key.path_ordinal)
            {
                return Err(MatchingStateErrorV1::Finality(
                    "成交最终性路径前驱冲突".to_owned(),
                ));
            }
            let ordinal = usize::try_from(key.path_ordinal)
                .map_err(|_| MatchingStateErrorV1::Finality("最终性 ordinal 越界".to_owned()))?;
            let step = path
                .steps()
                .get(ordinal)
                .ok_or_else(|| MatchingStateErrorV1::Finality("最终性 ordinal 越界".to_owned()))?;
            let expected_at = fill
                .matched_at_utc_ms
                .checked_add(step.offset_ms())
                .ok_or_else(|| MatchingStateErrorV1::Finality("最终性逻辑时点溢出".to_owned()))?;
            if expected_at != key.platform_event_at_utc_ms {
                return Err(MatchingStateErrorV1::Finality(
                    "最终性逻辑时点与路径偏移冲突".to_owned(),
                ));
            }
            let order = order_replacements
                .get(&fill.platform_order_id)
                .or_else(|| live.orders.get(&fill.platform_order_id))
                .ok_or_else(|| {
                    MatchingStateErrorV1::AccountOrderConflict(fill.platform_order_id.clone())
                })?;
            if order.platform_order_id != fill.platform_order_id {
                return Err(MatchingStateErrorV1::AccountOrderConflict(
                    fill.platform_order_id.clone(),
                ));
            }
        }

        if let Some(last_live_key) = live_keys.last() {
            self.last_consumed_live_key = Some(last_live_key.clone());
        }
        for key in keys {
            if !self.schedule_insertions.remove(&key) {
                let inserted = self.schedule_removals.insert(key.clone());
                debug_assert!(inserted);
            }
            let mut fill = self
                .fill_replacements
                .get(&key.fill_economic_delta_identity)
                .or_else(|| live.fills.get(&key.fill_economic_delta_identity))
                .expect("已排定最终性全时点预检保证成交存在")
                .clone();
            let mut order = order_replacements
                .get(&fill.platform_order_id)
                .or_else(|| live.orders.get(&fill.platform_order_id))
                .expect("已排定最终性全时点预检保证订单存在")
                .clone();
            if candidate_account.is_none() {
                *candidate_account = Some(live.account.clone());
            }
            let path = fill
                .finality_path
                .clone()
                .expect("已排定最终性全时点预检保证路径存在");
            let effect = apply_finality_transition_components_v2(
                candidate_account
                    .as_mut()
                    .expect("已排定最终性使用唯一候选账户"),
                &mut order,
                &mut fill,
                &live.scheduled_finality,
                &mut self.schedule_insertions,
                path,
                key.path_ordinal,
                key.platform_event_at_utc_ms,
                false,
            )?;
            order_replacements.insert(order.platform_order_id.clone(), order);
            self.fill_replacements.insert(fill.trade_id.clone(), fill);
            let terminal_succeeded = match effect.finality_state {
                TradeFinalityStateV1::Confirmed => Some(true),
                TradeFinalityStateV1::Failed => Some(false),
                TradeFinalityStateV1::MatchedNotBroadcasted
                | TradeFinalityStateV1::Matched
                | TradeFinalityStateV1::Retrying
                | TradeFinalityStateV1::Mined => None,
            };
            let mut settlement_feedbacks = Vec::new();
            if let Some(succeeded) = terminal_succeeded {
                let rebate_actions = match effect.liquidity_role {
                    SimLiquidityRoleV1::MakerInferred => self
                        .rebate
                        .stage_registered_fill_terminal(
                            live.rebate.as_ref(),
                            &effect.trade_id,
                            succeeded,
                            effect.platform_event_at_utc_ms,
                        )?
                        .to_vec(),
                    SimLiquidityRoleV1::Taker if effect.immediate_taker_fee_units > 0 => self
                        .rebate
                        .stage_registered_fill_terminal(
                            live.rebate.as_ref(),
                            &effect.trade_id,
                            succeeded,
                            effect.platform_event_at_utc_ms,
                        )?
                        .to_vec(),
                    SimLiquidityRoleV1::Taker => Vec::new(),
                };
                let (pre_redeem, post_redeem): (Vec<_>, Vec<_>) = rebate_actions
                    .into_iter()
                    .partition(rebate_action_precedes_auto_redeem_v2);
                let account = candidate_account
                    .as_mut()
                    .expect("终态最终性使用唯一候选账户");
                settlement_feedbacks
                    .extend(apply_rebate_actions_to_account_v2(account, pre_redeem)?);
                self.resolution.stage_pending_trade_removal(
                    live.market_resolutions.as_ref(),
                    &effect.condition_id,
                    &effect.trade_id,
                );
                if let Some(auto_redeem) = self.resolution.stage_auto_redeem(
                    live.market_resolutions.as_ref(),
                    live.rebate.as_ref(),
                    &self.rebate,
                    account,
                    &effect.condition_id,
                    effect.platform_event_at_utc_ms,
                )? {
                    settlement_feedbacks.push(auto_redeem);
                }
                settlement_feedbacks
                    .extend(apply_rebate_actions_to_account_v2(account, post_redeem)?);
            }
            self.effects.push(TradeFinalityLifecycleEffectsV1 {
                trade_feedback: effect,
                settlement_feedbacks,
            });
        }
        self.finality_advanced_through_utc_ms = Some(exact_next_at_utc_ms);
        Ok(&self.effects[first_new_effect..])
    }

    fn has_state_change(&self, live: &ExecutionStateV1) -> bool {
        !self.schedule_removals.is_empty()
            || !self.schedule_insertions.is_empty()
            || !self.fill_replacements.is_empty()
            || self.rebate.has_state_change()
            || self.resolution.has_state_change()
            || self.finality_advanced_through_utc_ms != live.finality_advanced_through_utc_ms
    }

    fn commit_prevalidated(self, live: &mut ExecutionStateV1) {
        let Self {
            schedule_removals,
            schedule_insertions,
            fill_replacements,
            rebate,
            resolution,
            finality_advanced_through_utc_ms,
            ..
        } = self;
        for key in schedule_removals {
            live.scheduled_finality.remove(&key);
        }
        live.scheduled_finality.extend(schedule_insertions);
        for (trade_id, fill) in fill_replacements {
            live.fills.insert(trade_id, fill);
        }
        if rebate.has_state_change() {
            rebate.commit_prevalidated(
                Arc::get_mut(&mut live.rebate)
                    .expect("非空返佣候选已在 into_committable 前验证独占所有权"),
            );
        }
        if resolution.has_state_change() {
            resolution.commit_prevalidated(
                Arc::get_mut(&mut live.market_resolutions)
                    .expect("非空判定候选已在 into_committable 前验证独占所有权"),
            );
        }
        live.finality_advanced_through_utc_ms = finality_advanced_through_utc_ms;
    }
}

impl<'a> PreparedExecutionDriveDeltaV2<'a> {
    fn prepare_without_scheduled_progress(live: &'a mut ExecutionStateV1) -> Self {
        let gtd = PreparedGtdExpirationPatchV2::prepare_incremental(live);
        let finality = PreparedScheduledFinalityPatchV2::prepare_incremental(live);
        Self {
            live,
            gtd,
            finality,
            order_replacements: BTreeMap::new(),
            candidate_account: None,
        }
    }

    fn prepare(
        live: &'a mut ExecutionStateV1,
        logical_time_utc_ms: i64,
    ) -> Result<Self, MatchingStateErrorV1> {
        if live
            .gtd_advanced_through_utc_ms
            .is_some_and(|current| logical_time_utc_ms < current)
            || live
                .finality_advanced_through_utc_ms
                .is_some_and(|current| logical_time_utc_ms < current)
        {
            return Err(MatchingStateErrorV1::LogicalTimeRegression);
        }
        let mut prepared = Self::prepare_without_scheduled_progress(live);

        loop {
            let next_gtd_at = prepared.gtd.candidate_next_gtd_at(prepared.live);
            let next_finality_at = prepared.finality.candidate_next_finality_at(prepared.live);
            let next_rebate_at = prepared.finality.candidate_next_rebate_at(prepared.live);
            let Some(next_at_utc_ms) = [next_gtd_at, next_finality_at, next_rebate_at]
                .into_iter()
                .flatten()
                .min()
            else {
                break;
            };
            if next_at_utc_ms > logical_time_utc_ms {
                break;
            }
            if next_gtd_at == Some(next_at_utc_ms) {
                let Self {
                    live,
                    gtd,
                    order_replacements,
                    candidate_account,
                    ..
                } = &mut prepared;
                let _ =
                    gtd.stage_gtd_at(live, order_replacements, candidate_account, next_at_utc_ms)?;
            }
            if next_finality_at == Some(next_at_utc_ms) {
                let Self {
                    live,
                    finality,
                    order_replacements,
                    candidate_account,
                    ..
                } = &mut prepared;
                let _ = finality.stage_finality_at(
                    live,
                    order_replacements,
                    candidate_account,
                    next_at_utc_ms,
                )?;
            }
            if prepared.finality.candidate_next_rebate_at(prepared.live) == Some(next_at_utc_ms) {
                let Self {
                    live,
                    finality,
                    candidate_account,
                    ..
                } = &mut prepared;
                let _ = finality.stage_rebate_clock_at(live, candidate_account, next_at_utc_ms)?;
            }
        }
        prepared.gtd.gtd_advanced_through_utc_ms = Some(logical_time_utc_ms);
        Ok(prepared)
    }

    pub(crate) fn gtd_expiration_effects(&self) -> &[GtdExpirationEffectV1] {
        &self.gtd.effects
    }

    pub(crate) fn scheduled_finality_effects(&self) -> &[TradeFinalityLifecycleEffectsV1] {
        &self.finality.effects
    }

    pub(crate) fn rebate_clock_effects(&self) -> &[SettlementLifecycleEffectV1] {
        &self.finality.rebate_clock_effects
    }

    pub(crate) fn has_state_change(&self) -> bool {
        self.gtd.has_state_change(self.live)
            || self.finality.has_state_change(self.live)
            || !self.order_replacements.is_empty()
            || self.candidate_account.is_some()
    }

    pub(crate) fn candidate_account(&self) -> &PlatformAccountStateV1 {
        self.candidate_account
            .as_ref()
            .unwrap_or(&self.live.account)
    }

    pub(crate) fn candidate_order(&self, platform_order_id: &str) -> Option<&SimOrderRecordV1> {
        self.order_replacements
            .get(platform_order_id)
            .or_else(|| self.live.orders.get(platform_order_id))
    }

    pub(crate) fn candidate_fill(&self, trade_id: &str) -> Option<&SimFillRecordV1> {
        self.finality
            .fill_replacements
            .get(trade_id)
            .or_else(|| self.live.fills.get(trade_id))
    }

    pub(crate) fn candidate_orders(&self) -> impl Iterator<Item = &SimOrderRecordV1> {
        self.live.orders.iter().map(|(platform_order_id, live)| {
            self.order_replacements
                .get(platform_order_id)
                .unwrap_or(live)
        })
    }

    pub(crate) fn candidate_fills(&self) -> impl Iterator<Item = &SimFillRecordV1> {
        self.live.fills.iter().map(|(trade_id, live)| {
            self.finality
                .fill_replacements
                .get(trade_id)
                .unwrap_or(live)
        })
    }

    pub(crate) fn candidate_orders_for_condition<'b>(
        &'b self,
        condition_id: &'b str,
    ) -> impl Iterator<Item = &'b SimOrderRecordV1> + 'b {
        self.live.orders.iter().filter_map(move |(order_id, live)| {
            (live.condition_id == condition_id)
                .then(|| self.order_replacements.get(order_id).unwrap_or(live))
        })
    }

    pub(crate) fn candidate_fills_for_condition<'b>(
        &'b self,
        condition_id: &'b str,
    ) -> impl Iterator<Item = &'b SimFillRecordV1> + 'b {
        self.live.fills.iter().filter_map(move |(trade_id, live)| {
            (live.condition_id == condition_id).then(|| {
                self.finality
                    .fill_replacements
                    .get(trade_id)
                    .unwrap_or(live)
            })
        })
    }

    pub(crate) fn prepare_caller_cancel_remainder(
        mut self,
        platform_order_id: &str,
        canceled_at_utc_ms: i64,
        caller_cancel_effect_identity: &Sha256Hex,
    ) -> Result<(Self, CancelOrderStateDispositionV1), MatchingStateErrorV1> {
        let Some(snapshot) = self.candidate_order(platform_order_id).cloned() else {
            return Ok((self, CancelOrderStateDispositionV1::NotFound));
        };
        let active_base_units = snapshot
            .pending_maker_depletion_base_units
            .checked_add(snapshot.open_base_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        if active_base_units == 0 {
            return Ok((self, CancelOrderStateDispositionV1::NoCancellableRemainder));
        }
        let accepted_base_units = snapshot
            .accepted_base_units
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        if !matches!(
            snapshot.kind,
            SimOrderKindStateV1::Gtc | SimOrderKindStateV1::Gtd { .. }
        ) || (snapshot.pending_maker_depletion_base_units == 0)
            != snapshot.pending_maker_depletion_evidence.is_none()
            || snapshot.canceled_base_units != 0
            || snapshot.canceled_released_quote_units != 0
            || snapshot.canceled_at_utc_ms.is_some()
            || snapshot.caller_cancel_effect_identity.is_some()
            || snapshot.expired_base_units != 0
            || snapshot.expired_released_quote_units != 0
            || snapshot.expired_at_utc_ms.is_some()
            || snapshot.gtd_expiration_identity.is_some()
            || snapshot.venue_dust_closed_base_units != 0
            || snapshot.venue_dust_released_quote_units != 0
            || snapshot.venue_dust_at_utc_ms.is_some()
            || snapshot.venue_dust_identity.is_some()
            || snapshot.market_resolved_canceled_base_units != 0
            || snapshot.market_resolved_released_quote_units != 0
            || snapshot.market_resolved_at_utc_ms.is_some()
            || snapshot.market_resolution_effect_identity.is_some()
            || snapshot
                .terminalized_pending_maker_depletion_evidence
                .is_some()
            || snapshot.terminal_reason.is_some()
        {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        let base_total = snapshot
            .matched_base_units
            .checked_add(snapshot.pending_maker_depletion_base_units)
            .and_then(|value| value.checked_add(snapshot.canceled_base_units))
            .and_then(|value| value.checked_add(snapshot.expired_base_units))
            .and_then(|value| value.checked_add(snapshot.venue_dust_closed_base_units))
            .and_then(|value| value.checked_add(snapshot.market_resolved_canceled_base_units))
            .and_then(|value| value.checked_add(snapshot.open_base_units))
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let quote_total = snapshot
            .matched_quote_units
            .checked_add(snapshot.open_quote_units)
            .and_then(|value| value.checked_add(snapshot.released_quote_units))
            .and_then(|value| value.checked_add(snapshot.canceled_released_quote_units))
            .and_then(|value| value.checked_add(snapshot.expired_released_quote_units))
            .and_then(|value| value.checked_add(snapshot.venue_dust_released_quote_units))
            .and_then(|value| value.checked_add(snapshot.market_resolved_released_quote_units))
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        if accepted_base_units != base_total || snapshot.accepted_quote_units != quote_total {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }

        let matching_closed = self
            .live
            .matching_closed_markets
            .contains_key(&snapshot.condition_id);
        let level_key = ExecutionTouchedBidLevelKeyV2 {
            condition_id: snapshot.condition_id.clone(),
            token_id: snapshot.token_id.clone(),
            price_pusd_per_share: snapshot.source_price_pusd_per_share,
        };
        let queued_base_units = match self.gtd.bid_queue_after_values.get(&level_key) {
            Some(Some(queue)) => queue.unique_account_order_base_units(platform_order_id)?,
            Some(None) => None,
            None => match self
                .live
                .queues
                .asset(&level_key.condition_id, &level_key.token_id)?
                .bids
                .get(&level_key.price_pusd_per_share)
            {
                Some(queue) => queue.unique_account_order_base_units(platform_order_id)?,
                None => None,
            },
        };
        if matching_closed {
            if queued_base_units.is_some() {
                return Err(MatchingStateErrorV1::QueuePredecessorConflict);
            }
        } else if snapshot.open_base_units > 0 {
            if queued_base_units != Some(snapshot.open_base_units) {
                return Err(MatchingStateErrorV1::QueuePredecessorConflict);
            }
        } else if queued_base_units.is_some() {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        let deadline_to_remove = match snapshot.kind {
            SimOrderKindStateV1::Gtd { expires_at_utc_ms } => {
                let deadline = (expires_at_utc_ms, platform_order_id.to_owned());
                if self.gtd.deadline_removals.contains(&deadline)
                    || !self.live.gtd_deadlines.contains(&deadline)
                {
                    return Err(MatchingStateErrorV1::QueuePredecessorConflict);
                }
                Some(deadline)
            }
            SimOrderKindStateV1::Gtc => None,
            SimOrderKindStateV1::Fok | SimOrderKindStateV1::Fak => {
                return Err(MatchingStateErrorV1::QueuePredecessorConflict);
            }
        };

        let next_canceled_base_units = snapshot
            .canceled_base_units
            .checked_add(active_base_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let next_canceled_released_quote_units = snapshot
            .canceled_released_quote_units
            .checked_add(snapshot.open_quote_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        if accepted_base_units
            != snapshot
                .matched_base_units
                .checked_add(snapshot.expired_base_units)
                .and_then(|value| value.checked_add(snapshot.venue_dust_closed_base_units))
                .and_then(|value| value.checked_add(snapshot.market_resolved_canceled_base_units))
                .and_then(|value| value.checked_add(next_canceled_base_units))
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
            || snapshot.accepted_quote_units
                != snapshot
                    .matched_quote_units
                    .checked_add(snapshot.released_quote_units)
                    .and_then(|value| value.checked_add(snapshot.expired_released_quote_units))
                    .and_then(|value| value.checked_add(snapshot.venue_dust_released_quote_units))
                    .and_then(|value| {
                        value.checked_add(snapshot.market_resolved_released_quote_units)
                    })
                    .and_then(|value| value.checked_add(next_canceled_released_quote_units))
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
        {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }

        let released_open_quote = decimal_from_micro_units_v1(snapshot.open_quote_units)?;
        let overlay_after = if snapshot.open_base_units > 0 {
            let removed = decimal_from_micro_units_v1(snapshot.open_base_units)?;
            let current = self
                .gtd
                .overlay_after_values
                .get(&level_key)
                .copied()
                .unwrap_or_else(|| {
                    self.live.account_overlay.quantity(
                        &level_key.condition_id,
                        &level_key.token_id,
                        BookSide::Bid,
                        level_key.price_pusd_per_share,
                    )
                });
            let after = current
                .checked_sub(removed)
                .filter(|value| !value.is_sign_negative())
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            Some(after)
        } else {
            None
        };
        let queue_seed = if snapshot.open_base_units > 0
            && !matching_closed
            && !self.gtd.bid_queue_after_values.contains_key(&level_key)
        {
            Some(
                self.live
                    .queues
                    .asset(&level_key.condition_id, &level_key.token_id)?
                    .bids
                    .get(&level_key.price_pusd_per_share)
                    .cloned()
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?,
            )
        } else {
            None
        };

        if self.candidate_account.is_none() {
            self.candidate_account = Some(self.live.account.clone());
        }
        let account_delta = self
            .candidate_account
            .as_mut()
            .expect("活动撤单必须已有候选账户")
            .apply_caller_cancel_release(
                &caller_cancel_effect_identity.0,
                &snapshot.condition_id,
                canceled_at_utc_ms,
                released_open_quote,
            )?;

        if let Some(queue) = queue_seed {
            self.gtd
                .bid_queue_after_values
                .insert(level_key.clone(), Some(queue));
        }
        if snapshot.open_base_units > 0 && !matching_closed {
            let level_after = self
                .gtd
                .bid_queue_after_values
                .get_mut(&level_key)
                .expect("已验证 caller-cancel touched bid level");
            let queue_after = level_after
                .as_mut()
                .expect("活动 caller-cancel 订单必须仍在候选价位");
            debug_assert_eq!(
                queue_after.unique_account_order_base_units(platform_order_id),
                Ok(Some(snapshot.open_base_units))
            );
            queue_after.remove_account_order(platform_order_id);
            if queue_after.segments.is_empty()
                && queue_after.account_taker_depletion_debt_units == 0
            {
                *level_after = None;
            }
        }
        if let Some(after) = overlay_after {
            self.gtd.overlay_after_values.insert(level_key, after);
        }
        if let Some(deadline) = deadline_to_remove {
            self.gtd.deadline_removals.insert(deadline);
        }
        let mut replacement = snapshot.clone();
        replacement.canceled_base_units = next_canceled_base_units;
        replacement.canceled_released_quote_units = next_canceled_released_quote_units;
        replacement.canceled_at_utc_ms = Some(canceled_at_utc_ms);
        replacement.last_updated_at_utc_ms = canceled_at_utc_ms;
        replacement.caller_cancel_effect_identity = Some(caller_cancel_effect_identity.clone());
        replacement.terminalized_pending_maker_depletion_evidence =
            replacement.pending_maker_depletion_evidence.take();
        replacement.pending_maker_depletion_base_units = 0;
        replacement.open_base_units = 0;
        replacement.open_quote_units = 0;
        self.order_replacements
            .insert(platform_order_id.to_owned(), replacement);
        Ok((
            self,
            CancelOrderStateDispositionV1::Canceled(CallerCancelStateEffectV1 {
                pending_base_units_before: snapshot.pending_maker_depletion_base_units,
                open_base_units_before: snapshot.open_base_units,
                released_open_quote_units: snapshot.open_quote_units,
                account_delta,
            }),
        ))
    }

    pub(crate) fn query_projection_changed(&self) -> bool {
        !self.order_replacements.is_empty() || !self.finality.fill_replacements.is_empty()
    }

    pub(crate) fn into_committable(
        self,
    ) -> Result<CommittableExecutionDriveDeltaV2<'a>, MatchingStateErrorV1> {
        if self.finality.rebate.has_state_change() && Arc::get_mut(&mut self.live.rebate).is_none()
        {
            return Err(RebateStateErrorV1::SharedStateRequiresUniqueOwnership.into());
        }
        if self.finality.resolution.has_state_change()
            && Arc::get_mut(&mut self.live.market_resolutions).is_none()
        {
            let condition_id = self
                .finality
                .resolution
                .changed_condition_id()
                .expect("非空判定候选必须包含变化 condition");
            return Err(MatchingStateErrorV1::Finality(format!(
                "非空判定义务候选提交前必须取得唯一状态所有权：{condition_id}"
            )));
        }
        Ok(CommittableExecutionDriveDeltaV2 {
            live: self.live,
            gtd: self.gtd,
            finality: self.finality,
            order_replacements: self.order_replacements,
            candidate_account: self.candidate_account,
        })
    }
}

impl CommittableExecutionDriveDeltaV2<'_> {
    pub(crate) fn commit(self) {
        let Self {
            live,
            gtd,
            finality,
            order_replacements,
            candidate_account,
        } = self;
        gtd.commit_prevalidated(live);
        finality.commit_prevalidated(live);
        for (platform_order_id, order) in order_replacements {
            live.orders.insert(platform_order_id, order);
        }
        if let Some(account) = candidate_account {
            live.account = account;
        }
    }
}

pub(crate) struct PreparedNewOrderMatchBuilderV2<'a> {
    queue_change: PreparedTakerAssetQueueChangeV2<'a>,
    account_overlay: &'a mut AccountBookOverlayStoreV1,
    account: &'a mut PlatformAccountStateV1,
    orders: &'a mut BTreeMap<String, SimOrderRecordV1>,
    fills: &'a mut BTreeMap<String, SimFillRecordV1>,
    gtd_deadlines: &'a mut BTreeSet<(i64, String)>,
    scheduled_finality: &'a mut BTreeSet<ScheduledFinalityTransitionKeyV1>,
    rebate: &'a mut Arc<RebateStateV1>,
    condition_id: String,
    token_id: String,
    maximum_price: Decimal,
    target: TakerTargetV1,
}

pub(crate) struct PreparedNewOrderExecutionDeltaV2<'a> {
    queue_change: PreparedTakerAssetQueueChangeV2<'a>,
    overlay_change: Option<PreparedAccountBookOverlayLevelChangeV2<'a>>,
    account: &'a mut PlatformAccountStateV1,
    candidate_account: PlatformAccountStateV1,
    orders: &'a mut BTreeMap<String, SimOrderRecordV1>,
    fills: &'a mut BTreeMap<String, SimFillRecordV1>,
    gtd_deadlines: &'a mut BTreeSet<(i64, String)>,
    scheduled_finality: &'a mut BTreeSet<ScheduledFinalityTransitionKeyV1>,
    rebate: Option<CommittableTakerRebateRegistrationsV2<'a>>,
    order: SimOrderRecordV1,
    candidate_fills: Vec<SimFillRecordV1>,
    gtd_deadline_to_insert: Option<(i64, String)>,
    scheduled_finality_to_insert: BTreeSet<ScheduledFinalityTransitionKeyV1>,
    order_commit_effects: OrderCommitEffectsV1,
    trade_finality_effects: Vec<TradeFinalityLifecycleEffectsV1>,
    venue_dust_effect: Option<VenueDustEffectV1>,
}

pub(crate) struct CommittableNewOrderExecutionDeltaV2<'a> {
    prepared: PreparedNewOrderExecutionDeltaV2<'a>,
}

struct PreparedNewOrderCoreV2 {
    candidate_account: PlatformAccountStateV1,
    order: SimOrderRecordV1,
    fills: Vec<SimFillRecordV1>,
    effects: OrderCommitEffectsV1,
}

fn prepare_new_order_core_v2(
    account: &PlatformAccountStateV1,
    existing_orders: &BTreeMap<String, SimOrderRecordV1>,
    existing_fills: &BTreeMap<String, SimFillRecordV1>,
    order: SimOrderRecordV1,
    fills: Vec<SimFillRecordV1>,
) -> Result<PreparedNewOrderCoreV2, MatchingStateErrorV1> {
    if order.platform_order_id.is_empty()
        || order.item_idempotency_identity.is_empty()
        || existing_orders.contains_key(&order.platform_order_id)
        || order.accepted_quote_units
            != order
                .matched_quote_units
                .checked_add(order.open_quote_units)
                .and_then(|value| value.checked_add(order.released_quote_units))
                .and_then(|value| value.checked_add(order.canceled_released_quote_units))
                .and_then(|value| value.checked_add(order.expired_released_quote_units))
                .and_then(|value| value.checked_add(order.venue_dust_released_quote_units))
                .and_then(|value| value.checked_add(order.market_resolved_released_quote_units))
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
    {
        return Err(MatchingStateErrorV1::AccountOrderConflict(
            order.platform_order_id,
        ));
    }
    if let Some(accepted_base) = order.accepted_base_units
        && accepted_base
            != order
                .matched_base_units
                .checked_add(order.pending_maker_depletion_base_units)
                .and_then(|value| value.checked_add(order.canceled_base_units))
                .and_then(|value| value.checked_add(order.expired_base_units))
                .and_then(|value| value.checked_add(order.venue_dust_closed_base_units))
                .and_then(|value| value.checked_add(order.market_resolved_canceled_base_units))
                .and_then(|value| value.checked_add(order.open_base_units))
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
    {
        return Err(MatchingStateErrorV1::QueuePredecessorConflict);
    }
    if (order.pending_maker_depletion_base_units == 0)
        != order.pending_maker_depletion_evidence.is_none()
        || order.expired_base_units != 0
        || order.expired_released_quote_units != 0
        || order.expired_at_utc_ms.is_some()
        || order.gtd_expiration_identity.is_some()
        || order.venue_dust_closed_base_units != 0
        || order.venue_dust_released_quote_units != 0
        || order.venue_dust_at_utc_ms.is_some()
        || order.venue_dust_identity.is_some()
        || order.market_resolved_canceled_base_units != 0
        || order.market_resolved_released_quote_units != 0
        || order.market_resolved_at_utc_ms.is_some()
        || order.market_resolution_effect_identity.is_some()
        || order.canceled_base_units != 0
        || order.canceled_released_quote_units != 0
        || order.canceled_at_utc_ms.is_some()
        || order.caller_cancel_effect_identity.is_some()
        || order
            .terminalized_pending_maker_depletion_evidence
            .is_some()
    {
        return Err(MatchingStateErrorV1::QueuePredecessorConflict);
    }
    if let Some(accepted_base) = order.accepted_base_units {
        let pending_end = order
            .matched_base_units
            .checked_add(order.pending_maker_depletion_base_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let allocated_before = cumulative_segment_quote_units_v1(
            accepted_base,
            order.accepted_quote_units,
            order.matched_base_units,
        )?;
        let allocated_after = cumulative_segment_quote_units_v1(
            accepted_base,
            order.accepted_quote_units,
            pending_end,
        )?;
        if allocated_before != allocated_after {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
    }
    let fill_base = fills.iter().try_fold(0_u128, |total, fill| {
        total
            .checked_add(fill.base_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)
    })?;
    let fill_quote = fills.iter().try_fold(0_u128, |total, fill| {
        total
            .checked_add(fill.quote_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)
    })?;
    let fill_fee = fills.iter().try_fold(0_u128, |total, fill| {
        total
            .checked_add(fill.immediate_taker_fee_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)
    })?;
    if fill_base != order.matched_base_units
        || fill_quote != order.matched_quote_units
        || fill_fee != order.immediate_taker_fee_encumbered_units
    {
        return Err(MatchingStateErrorV1::QueuePredecessorConflict);
    }
    let mut staged_trade_ids = BTreeSet::new();
    if fills.iter().any(|fill| {
        fill.platform_order_id != order.platform_order_id
            || fill.finality_path.is_some()
            || fill.finality_path_ordinal.is_some()
            || fill.finality_state.is_some()
            || fill.economic_application_delta.is_some()
            || existing_fills.contains_key(&fill.trade_id)
            || !staged_trade_ids.insert(fill.trade_id.as_str())
    }) {
        return Err(MatchingStateErrorV1::AccountOrderConflict(
            order.platform_order_id,
        ));
    }
    let required = order
        .accepted_quote_units
        .checked_add(order.immediate_taker_fee_encumbered_units)
        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
    if account.available_pusd_units() < required {
        return Err(MatchingStateErrorV1::QueueUnderflow);
    }
    let final_principal = order
        .matched_quote_units
        .checked_add(order.open_quote_units)
        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
    let accepted_principal = if matches!(order.kind, SimOrderKindStateV1::Fak) {
        order.accepted_quote_units
    } else {
        final_principal
    };
    let mut candidate_account = account.clone();
    let mut account_deltas = vec![candidate_account.apply_order_accepted(
        &format!("order-accepted:{}", order.platform_order_id),
        &order.condition_id,
        order.created_at_utc_ms,
        decimal_from_micro_units_v1(accepted_principal)?,
        decimal_from_micro_units_v1(order.immediate_taker_fee_encumbered_units)?,
    )?];
    if matches!(order.kind, SimOrderKindStateV1::Fak)
        && let Some(delta) = candidate_account.apply_fak_unspent_release(
            &format!("fak-unspent:{}", order.platform_order_id),
            &order.condition_id,
            order.created_at_utc_ms,
            decimal_from_micro_units_v1(order.released_quote_units)?,
        )?
    {
        account_deltas.push(delta);
    }
    Ok(PreparedNewOrderCoreV2 {
        candidate_account,
        order,
        fills,
        effects: OrderCommitEffectsV1 { account_deltas },
    })
}

fn prepare_initial_finality_path_v2(
    account_instance_key: &contracts::AccountInstanceKeyV1,
    policy: &TradeFinalityPolicyV1,
    order: &SimOrderRecordV1,
    fill: &SimFillRecordV1,
) -> Result<TradeFinalityPathV1, MatchingStateErrorV1> {
    if fill.finality_path.is_some()
        || fill.finality_path_ordinal.is_some()
        || fill.finality_state.is_some()
        || fill.economic_application_delta.is_some()
    {
        return Err(MatchingStateErrorV1::Finality(format!(
            "成交最终性已经实例化：{}",
            fill.trade_id
        )));
    }
    if order.platform_order_id != fill.platform_order_id {
        return Err(MatchingStateErrorV1::AccountOrderConflict(
            fill.platform_order_id.clone(),
        ));
    }
    let path = policy
        .select_path_for_fill(
            account_instance_key,
            &order.global_request_id,
            &order.item_idempotency_identity,
            &fill.trade_id,
        )
        .clone();
    for step in path.steps() {
        fill.matched_at_utc_ms
            .checked_add(step.offset_ms())
            .ok_or_else(|| MatchingStateErrorV1::Finality("最终性逻辑时点溢出".to_owned()))?;
    }
    Ok(path)
}

fn apply_finality_transition_components_v2(
    account: &mut PlatformAccountStateV1,
    order: &mut SimOrderRecordV1,
    fill: &mut SimFillRecordV1,
    existing_scheduled: &BTreeSet<ScheduledFinalityTransitionKeyV1>,
    scheduled_to_insert: &mut BTreeSet<ScheduledFinalityTransitionKeyV1>,
    path: TradeFinalityPathV1,
    path_ordinal: u32,
    platform_event_at_utc_ms: i64,
    initialize: bool,
) -> Result<TradeFinalityTransitionEffectV1, MatchingStateErrorV1> {
    let snapshot = fill.clone();
    if order.platform_order_id != snapshot.platform_order_id {
        return Err(MatchingStateErrorV1::AccountOrderConflict(
            snapshot.platform_order_id,
        ));
    }
    if initialize {
        if path_ordinal != 0 || snapshot.finality_path.is_some() {
            return Err(MatchingStateErrorV1::Finality(
                "成交最终性初始化身份冲突".to_owned(),
            ));
        }
    } else if snapshot.finality_path.as_ref() != Some(&path)
        || snapshot
            .finality_path_ordinal
            .and_then(|current| current.checked_add(1))
            != Some(path_ordinal)
    {
        return Err(MatchingStateErrorV1::Finality(
            "成交最终性路径前驱冲突".to_owned(),
        ));
    }
    let ordinal = usize::try_from(path_ordinal)
        .map_err(|_| MatchingStateErrorV1::Finality("最终性 ordinal 越界".to_owned()))?;
    let step = path
        .steps()
        .get(ordinal)
        .ok_or_else(|| MatchingStateErrorV1::Finality("最终性 ordinal 越界".to_owned()))?;
    let expected_at = snapshot
        .matched_at_utc_ms
        .checked_add(step.offset_ms())
        .ok_or_else(|| MatchingStateErrorV1::Finality("最终性逻辑时点溢出".to_owned()))?;
    if expected_at != platform_event_at_utc_ms {
        return Err(MatchingStateErrorV1::Finality(
            "最终性逻辑时点与路径偏移冲突".to_owned(),
        ));
    }
    let economic_application = path.economic_application_ordinal() == Some(path_ordinal);
    if economic_application && snapshot.economic_application_delta.is_some() {
        return Err(MatchingStateErrorV1::Finality(
            "成交经济变化重复应用".to_owned(),
        ));
    }
    let failed = step.state() == TradeFinalityStateV1::Failed;
    if economic_application && failed {
        return Err(MatchingStateErrorV1::Finality(
            "失败终态不能作为经济应用截点".to_owned(),
        ));
    }
    let order_fee_before = order.immediate_taker_fee_encumbered_units;
    let releases_fee =
        economic_application || (failed && snapshot.economic_application_delta.is_none());
    if releases_fee && order_fee_before < snapshot.immediate_taker_fee_units {
        return Err(MatchingStateErrorV1::Finality(
            "成交手续费份额超过订单剩余手续费占用".to_owned(),
        ));
    }
    let next_schedule = path
        .steps()
        .get(ordinal + 1)
        .map(|next| {
            let next_ordinal = path_ordinal
                .checked_add(1)
                .ok_or_else(|| MatchingStateErrorV1::Finality("最终性 ordinal 溢出".to_owned()))?;
            let next_at = snapshot
                .matched_at_utc_ms
                .checked_add(next.offset_ms())
                .ok_or_else(|| MatchingStateErrorV1::Finality("最终性逻辑时点溢出".to_owned()))?;
            Ok::<_, MatchingStateErrorV1>(ScheduledFinalityTransitionKeyV1 {
                platform_event_at_utc_ms: next_at,
                fill_economic_delta_identity: snapshot.trade_id.clone(),
                path_ordinal: next_ordinal,
            })
        })
        .transpose()?;
    if next_schedule
        .as_ref()
        .is_some_and(|key| existing_scheduled.contains(key) || scheduled_to_insert.contains(key))
    {
        return Err(MatchingStateErrorV1::Finality(
            "最终性后继重复排定".to_owned(),
        ));
    }

    let reason_identity = if economic_application {
        format!(
            "FILL_FINALITY_ECONOMIC:{}:{path_ordinal}",
            snapshot.trade_id
        )
    } else if failed && snapshot.economic_application_delta.is_some() {
        format!(
            "FILL_FINALITY_COMPENSATION:{}:{path_ordinal}",
            snapshot.trade_id
        )
    } else if failed {
        format!(
            "FILL_FINALITY_UNAPPLIED_FAILURE:{}:{path_ordinal}",
            snapshot.trade_id
        )
    } else {
        format!("FILL_FINALITY_STATUS:{}:{path_ordinal}", snapshot.trade_id)
    };
    let economic_amounts = FillEconomicAmountsV1 {
        base_units: snapshot.base_units,
        quote_units: snapshot.quote_units,
        immediate_taker_fee_units: snapshot.immediate_taker_fee_units,
    };
    let account_delta = if economic_application {
        Some(account.apply_fill_finality_economic(
            &reason_identity,
            &snapshot.condition_id,
            &snapshot.token_id,
            platform_event_at_utc_ms,
            economic_amounts,
        )?)
    } else if failed {
        Some(if snapshot.economic_application_delta.is_some() {
            account.apply_fill_finality_compensation(
                &reason_identity,
                &snapshot.condition_id,
                &snapshot.token_id,
                platform_event_at_utc_ms,
                economic_amounts,
            )?
        } else {
            account.apply_fill_finality_unapplied_failure_release(
                &reason_identity,
                &snapshot.condition_id,
                platform_event_at_utc_ms,
                snapshot.quote_units,
                snapshot.immediate_taker_fee_units,
            )?
        })
    } else {
        None
    };

    if releases_fee && snapshot.immediate_taker_fee_units > 0 {
        order.immediate_taker_fee_encumbered_units = order_fee_before
            .checked_sub(snapshot.immediate_taker_fee_units)
            .expect("手续费释放已在账户变化前校验");
        order.last_updated_at_utc_ms = platform_event_at_utc_ms;
    }
    if initialize {
        fill.finality_path = Some(path.clone());
    }
    fill.finality_path_ordinal = Some(path_ordinal);
    fill.finality_state = Some(step.state());
    if economic_application {
        fill.economic_application_delta = account_delta.clone();
    }
    if let Some(key) = next_schedule {
        let inserted = scheduled_to_insert.insert(key);
        debug_assert!(inserted);
    }
    Ok(TradeFinalityTransitionEffectV1 {
        trade_id: snapshot.trade_id,
        platform_order_id: snapshot.platform_order_id,
        condition_id: snapshot.condition_id,
        token_id: snapshot.token_id,
        platform_event_at_utc_ms,
        finality_state: step.state(),
        economic_disposition: fill.economic_disposition(),
        liquidity_role: snapshot.liquidity_role,
        base_units: snapshot.base_units,
        quote_units: snapshot.quote_units,
        immediate_taker_fee_units: snapshot.immediate_taker_fee_units,
        economic_application_delta: fill.economic_application_delta.clone(),
        account_delta,
    })
}

struct PreparedVenueDustCoreV2 {
    candidate_account: PlatformAccountStateV1,
    order: SimOrderRecordV1,
    effect: Option<VenueDustEffectV1>,
}

fn venue_dust_upper_exclusive_base_units_v2(
    lifecycle_account_rule: &LifecycleAccountRuleV1,
) -> Result<u128, MatchingStateErrorV1> {
    if !lifecycle_account_rule.venue_dust_requires_positive_fill {
        return Err(MatchingStateErrorV1::QueuePredecessorConflict);
    }
    let upper_exclusive_base_units = micro_units_v1(
        lifecycle_account_rule
            .venue_dust_upper_exclusive_shares
            .value(),
    )?;
    if upper_exclusive_base_units == 0 {
        return Err(MatchingStateErrorV1::QueuePredecessorConflict);
    }
    Ok(upper_exclusive_base_units)
}

fn prepare_venue_dust_core_v2(
    mut candidate_account: PlatformAccountStateV1,
    mut order: SimOrderRecordV1,
    candidate: &VenueDustQueueCandidateV1,
    platform_event_at_utc_ms: i64,
    trigger_identity: &str,
    upper_exclusive_base_units: u128,
) -> Result<PreparedVenueDustCoreV2, MatchingStateErrorV1> {
    if candidate.platform_order_id != order.platform_order_id
        || candidate.price_pusd_per_share != order.source_price_pusd_per_share
        || candidate.remaining_base_units != order.open_base_units
    {
        return Err(MatchingStateErrorV1::QueuePredecessorConflict);
    }
    if order.matched_base_units == 0
        || order.pending_maker_depletion_base_units != 0
        || order.open_base_units == 0
        || order.open_base_units >= upper_exclusive_base_units
    {
        return Ok(PreparedVenueDustCoreV2 {
            candidate_account,
            order,
            effect: None,
        });
    }
    let accepted_base_units = order
        .accepted_base_units
        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
    if !matches!(
        order.kind,
        SimOrderKindStateV1::Gtc | SimOrderKindStateV1::Gtd { .. }
    ) || order.expired_base_units != 0
        || order.expired_released_quote_units != 0
        || order.expired_at_utc_ms.is_some()
        || order.gtd_expiration_identity.is_some()
        || order.venue_dust_closed_base_units != 0
        || order.venue_dust_released_quote_units != 0
        || order.venue_dust_at_utc_ms.is_some()
        || order.venue_dust_identity.is_some()
        || order.market_resolved_canceled_base_units != 0
        || order.market_resolved_released_quote_units != 0
        || order.market_resolved_at_utc_ms.is_some()
        || order.market_resolution_effect_identity.is_some()
        || order.canceled_base_units != 0
        || order.canceled_released_quote_units != 0
        || order.canceled_at_utc_ms.is_some()
        || order.caller_cancel_effect_identity.is_some()
        || order.pending_maker_depletion_evidence.is_some()
        || order
            .terminalized_pending_maker_depletion_evidence
            .is_some()
        || order.terminal_reason.is_some()
    {
        return Err(MatchingStateErrorV1::QueuePredecessorConflict);
    }
    if accepted_base_units
        != order
            .matched_base_units
            .checked_add(order.open_base_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
        || order.accepted_quote_units
            != order
                .matched_quote_units
                .checked_add(order.open_quote_units)
                .and_then(|value| value.checked_add(order.released_quote_units))
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
    {
        return Err(MatchingStateErrorV1::QueuePredecessorConflict);
    }
    let venue_dust_identity = venue_dust_identity(
        trigger_identity,
        &order.platform_order_id,
        order.matched_base_units,
        order.open_base_units,
        order.open_quote_units,
    );
    let account_delta = candidate_account.apply_venue_dust_release(
        &venue_dust_identity.0,
        &order.condition_id,
        platform_event_at_utc_ms,
        decimal_from_micro_units_v1(order.open_quote_units)?,
    )?;
    let effect = VenueDustEffectV1 {
        platform_order_id: order.platform_order_id.clone(),
        condition_id: order.condition_id.clone(),
        token_id: order.token_id.clone(),
        closed_base_units: order.open_base_units,
        released_open_quote_units: order.open_quote_units,
        account_delta,
    };
    order.venue_dust_closed_base_units = order.open_base_units;
    order.venue_dust_released_quote_units = order.open_quote_units;
    order.venue_dust_at_utc_ms = Some(platform_event_at_utc_ms);
    order.venue_dust_identity = Some(venue_dust_identity);
    order.open_base_units = 0;
    order.open_quote_units = 0;
    order.last_updated_at_utc_ms = platform_event_at_utc_ms;
    order.terminal_reason = Some(SimOrderTerminalReasonV1::VenueDustTerminal);
    Ok(PreparedVenueDustCoreV2 {
        candidate_account,
        order,
        effect: Some(effect),
    })
}

impl<'a> PreparedNewOrderMatchBuilderV2<'a> {
    pub(crate) fn plan(&self) -> &QueueMatchPlanV1 {
        self.queue_change.plan()
    }

    pub(crate) fn finish_new_order(
        self,
        order: SimOrderRecordV1,
        fills: Vec<SimFillRecordV1>,
        finality_policy: &TradeFinalityPolicyV1,
        lifecycle_account_rule: &LifecycleAccountRuleV1,
    ) -> Result<PreparedNewOrderExecutionDeltaV2<'a>, MatchingStateErrorV1> {
        let Self {
            mut queue_change,
            account_overlay,
            account,
            orders,
            fills: existing_fills,
            gtd_deadlines,
            scheduled_finality,
            rebate,
            condition_id,
            token_id,
            maximum_price,
            target,
        } = self;
        let plan = queue_change.plan();
        if order.condition_id != condition_id
            || order.token_id != token_id
            || order.source_price_pusd_per_share != maximum_price
            || !matches!(
                (target, order.kind),
                (
                    TakerTargetV1::Quote(_),
                    SimOrderKindStateV1::Fok | SimOrderKindStateV1::Fak
                ) | (
                    TakerTargetV1::LimitBase { .. },
                    SimOrderKindStateV1::Gtc | SimOrderKindStateV1::Gtd { .. }
                )
            )
            || (matches!(order.kind, SimOrderKindStateV1::Fok)
                && !target.complete(plan.matched_base_units, plan.used_quote_units))
            || (matches!(order.kind, SimOrderKindStateV1::Fak) && plan.fills.is_empty())
            || (matches!(
                order.kind,
                SimOrderKindStateV1::Fok | SimOrderKindStateV1::Fak
            ) && (order.open_base_units != 0 || order.open_quote_units != 0))
        {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        match target {
            TakerTargetV1::Quote(quote_units)
                if order.accepted_base_units.is_none()
                    && order.accepted_quote_units == quote_units => {}
            TakerTargetV1::LimitBase {
                total_base_units,
                total_quote_units,
            } if order.accepted_base_units == Some(total_base_units)
                && order.accepted_quote_units == total_quote_units =>
            {
                let allocated_quote_units = cumulative_segment_quote_units_v1(
                    total_base_units,
                    total_quote_units,
                    plan.matched_base_units,
                )?;
                let expected_open_base_units = total_base_units
                    .checked_sub(plan.matched_base_units)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                let expected_open_quote_units = total_quote_units
                    .checked_sub(allocated_quote_units)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                let expected_released_quote_units = allocated_quote_units
                    .checked_sub(plan.used_quote_units)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                if order.matched_base_units != plan.matched_base_units
                    || order.matched_quote_units != plan.used_quote_units
                    || order.open_base_units != expected_open_base_units
                    || order.open_quote_units != expected_open_quote_units
                    || order.released_quote_units != expected_released_quote_units
                {
                    return Err(MatchingStateErrorV1::QueuePredecessorConflict);
                }
            }
            TakerTargetV1::Quote(_) | TakerTargetV1::LimitBase { .. } => {
                return Err(MatchingStateErrorV1::QueuePredecessorConflict);
            }
        }
        if fills.len() != plan.fills.len()
            || fills.iter().zip(&plan.fills).any(|(fill, slice)| {
                fill.condition_id != condition_id
                    || fill.token_id != token_id
                    || fill.liquidity_role != SimLiquidityRoleV1::Taker
                    || fill.source_identity != slice.segment_id.0
                    || fill.maker_depletion_evidence.is_some()
                    || fill.price_pusd_per_share != slice.price_pusd_per_share
                    || fill.base_units != slice.base_units
                    || fill.quote_units != slice.quote_units
            })
        {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        let PreparedNewOrderCoreV2 {
            mut candidate_account,
            mut order,
            fills: mut candidate_fills,
            effects: order_commit_effects,
        } = prepare_new_order_core_v2(account, orders, existing_fills, order, fills)?;

        let mut overlay_change = if order.open_base_units > 0 {
            queue_change.prepare_account_buy_remainder(
                order.source_price_pusd_per_share,
                order.platform_order_id.clone(),
                order.open_base_units,
            )?;
            let mut change = account_overlay
                .prepare_level_change_v2(
                    &order.condition_id,
                    &order.token_id,
                    BookSide::Bid,
                    order.source_price_pusd_per_share,
                )
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            change
                .add_quantity(decimal_from_micro_units_v1(order.open_base_units)?)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            Some(change)
        } else {
            None
        };
        let mut gtd_deadline_to_insert = match order.kind {
            SimOrderKindStateV1::Gtd { expires_at_utc_ms } if order.open_base_units > 0 => {
                let key = (expires_at_utc_ms, order.platform_order_id.clone());
                if gtd_deadlines.contains(&key) {
                    return Err(MatchingStateErrorV1::QueuePredecessorConflict);
                }
                Some(key)
            }
            SimOrderKindStateV1::Gtc
            | SimOrderKindStateV1::Gtd { .. }
            | SimOrderKindStateV1::Fok
            | SimOrderKindStateV1::Fak => None,
        };

        let mut fill_indexes = (0..candidate_fills.len()).collect::<Vec<_>>();
        fill_indexes.sort_by(|left, right| {
            candidate_fills[*left]
                .trade_id
                .cmp(&candidate_fills[*right].trade_id)
        });
        let mut finality_paths = Vec::with_capacity(fill_indexes.len());
        for index in &fill_indexes {
            finality_paths.push(prepare_initial_finality_path_v2(
                candidate_account.account_instance_key(),
                finality_policy,
                &order,
                &candidate_fills[*index],
            )?);
        }
        let rebate = if fill_indexes
            .iter()
            .any(|index| candidate_fills[*index].immediate_taker_fee_units > 0)
        {
            let rebate_state = Arc::get_mut(rebate).ok_or_else(|| {
                MatchingStateErrorV1::Rebate("非零 taker 手续费要求准备期独占返佣状态".to_owned())
            })?;
            let mut prepared = rebate_state.prepare_taker_rebate_registrations_v2();
            for index in &fill_indexes {
                let fill = &candidate_fills[*index];
                if fill.immediate_taker_fee_units > 0 {
                    prepared.stage_taker_fill(
                        &fill.trade_id,
                        &fill.condition_id,
                        fill.matched_at_utc_ms,
                        decimal_from_micro_units_v1(fill.immediate_taker_fee_units)?,
                    )?;
                }
            }
            Some(prepared.into_committable()?)
        } else {
            None
        };

        let mut scheduled_finality_to_insert = BTreeSet::new();
        let mut trade_finality_effects = Vec::with_capacity(fill_indexes.len());
        for (index, path) in fill_indexes.into_iter().zip(finality_paths) {
            let platform_event_at_utc_ms = candidate_fills[index].matched_at_utc_ms;
            let trade_feedback = apply_finality_transition_components_v2(
                &mut candidate_account,
                &mut order,
                &mut candidate_fills[index],
                scheduled_finality,
                &mut scheduled_finality_to_insert,
                path,
                0,
                platform_event_at_utc_ms,
                true,
            )?;
            trade_finality_effects.push(TradeFinalityLifecycleEffectsV1 {
                trade_feedback,
                settlement_feedbacks: Vec::new(),
            });
        }

        let venue_dust_effect = if order.open_base_units > 0 {
            if let Some(candidate) = queue_change.candidate_venue_dust_remainder(
                order.source_price_pusd_per_share,
                &order.platform_order_id,
            ) {
                let upper_exclusive_base_units =
                    venue_dust_upper_exclusive_base_units_v2(lifecycle_account_rule)?;
                let order_action_at_utc_ms = order.created_at_utc_ms;
                let queue_candidate = VenueDustQueueCandidateV1 {
                    platform_order_id: candidate.platform_order_id.clone(),
                    price_pusd_per_share: candidate.price_pusd_per_share,
                    remaining_base_units: candidate.remaining_base_units,
                };
                let prepared_dust = prepare_venue_dust_core_v2(
                    candidate_account,
                    order,
                    &queue_candidate,
                    order_action_at_utc_ms,
                    &format!("ORDER_ACTION:{}", candidate.platform_order_id),
                    upper_exclusive_base_units,
                )?;
                candidate_account = prepared_dust.candidate_account;
                order = prepared_dust.order;
                if let Some(effect) = prepared_dust.effect {
                    queue_change.prepare_remove_venue_dust_remainder(&candidate)?;
                    overlay_change
                        .as_mut()
                        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
                        .subtract_quantity(decimal_from_micro_units_v1(effect.closed_base_units)?)
                        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                    gtd_deadline_to_insert = None;
                    Some(effect)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(PreparedNewOrderExecutionDeltaV2 {
            queue_change,
            overlay_change,
            account,
            candidate_account,
            orders,
            fills: existing_fills,
            gtd_deadlines,
            scheduled_finality,
            rebate,
            order,
            candidate_fills,
            gtd_deadline_to_insert,
            scheduled_finality_to_insert,
            order_commit_effects,
            trade_finality_effects,
            venue_dust_effect,
        })
    }
}

impl<'a> PreparedNewOrderExecutionDeltaV2<'a> {
    pub(crate) fn plan(&self) -> &QueueMatchPlanV1 {
        self.queue_change.plan()
    }

    pub(crate) fn order(&self) -> &SimOrderRecordV1 {
        &self.order
    }

    pub(crate) fn fills(&self) -> &[SimFillRecordV1] {
        &self.candidate_fills
    }

    pub(crate) fn order_commit_effects(&self) -> &OrderCommitEffectsV1 {
        &self.order_commit_effects
    }

    pub(crate) fn trade_finality_effects(&self) -> &[TradeFinalityLifecycleEffectsV1] {
        &self.trade_finality_effects
    }

    pub(crate) fn venue_dust_effect(&self) -> Option<&VenueDustEffectV1> {
        self.venue_dust_effect.as_ref()
    }

    pub(crate) fn into_committable(self) -> CommittableNewOrderExecutionDeltaV2<'a> {
        CommittableNewOrderExecutionDeltaV2 { prepared: self }
    }
}

impl CommittableNewOrderExecutionDeltaV2<'_> {
    pub(crate) fn commit(self) {
        let PreparedNewOrderExecutionDeltaV2 {
            queue_change,
            overlay_change,
            account,
            candidate_account,
            orders,
            fills,
            gtd_deadlines,
            scheduled_finality,
            rebate,
            order,
            candidate_fills,
            gtd_deadline_to_insert,
            scheduled_finality_to_insert,
            ..
        } = self.prepared;
        queue_change.commit();
        if let Some(change) = overlay_change {
            change.commit();
        }
        *account = candidate_account;
        for fill in candidate_fills {
            fills.insert(fill.trade_id.clone(), fill);
        }
        if let Some(deadline) = gtd_deadline_to_insert {
            gtd_deadlines.insert(deadline);
        }
        scheduled_finality.extend(scheduled_finality_to_insert);
        if let Some(rebate) = rebate {
            rebate.commit();
        }
        orders.insert(order.platform_order_id.clone(), order);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct MarketResolutionObligationV1 {
    resolution_event_id: String,
    resolution_source_event_at_utc_ms: i64,
    resolution: MarketResolutionTerminalV3,
    pending_trade_ids: BTreeSet<String>,
    redeemed_at_utc_ms: Option<i64>,
    invalidated_by_quality_reason: Option<String>,
}

fn rebate_action_precedes_auto_redeem_v2(action: &RebateSettlementActionV1) -> bool {
    matches!(
        action,
        RebateSettlementActionV1::MakerMarketFinalized(_)
            | RebateSettlementActionV1::TakerAccrualApplied(_)
    )
}

fn apply_rebate_actions_to_account_v2(
    account: &mut PlatformAccountStateV1,
    actions: Vec<RebateSettlementActionV1>,
) -> Result<Vec<SettlementLifecycleEffectV1>, MatchingStateErrorV1> {
    actions
        .into_iter()
        .map(|action| apply_rebate_action_to_account_v2(account, action))
        .collect()
}

fn apply_rebate_action_to_account_v2(
    account: &mut PlatformAccountStateV1,
    action: RebateSettlementActionV1,
) -> Result<SettlementLifecycleEffectV1, MatchingStateErrorV1> {
    let account_delta = match &action {
        RebateSettlementActionV1::MakerMarketFinalized(value) => (value.accrual_pusd
            > Decimal::ZERO)
            .then(|| {
                account.apply_maker_rebate_accrual_finalized(
                    &value.reason_identity,
                    &value.condition_id,
                    value.platform_effective_at_utc_ms,
                    value.accrual_pusd,
                )
            })
            .transpose()?,
        RebateSettlementActionV1::TakerAccrualApplied(value) => {
            Some(account.apply_taker_rebate_accrual_applied(
                &value.reason_identity,
                &value.condition_id,
                value.platform_effective_at_utc_ms,
                value.accrual_pusd,
            )?)
        }
        RebateSettlementActionV1::DayFinalized(value) => match value.kind {
            RebateKindV1::Maker => account.apply_maker_rebate_day_finalized(
                &value.reason_identity,
                value.platform_effective_at_utc_ms,
                value.locked_remainder_pusd,
            )?,
            RebateKindV1::Taker => account.apply_taker_rebate_day_finalized(
                &value.reason_identity,
                value.platform_effective_at_utc_ms,
                value.locked_remainder_pusd,
            )?,
        },
        RebateSettlementActionV1::Paid(value) => Some(match value.kind {
            RebateKindV1::Maker => account.apply_maker_rebate_paid(
                &value.reason_identity,
                value.platform_effective_at_utc_ms,
                value.paid_pusd,
            )?,
            RebateKindV1::Taker => account.apply_taker_rebate_paid(
                &value.reason_identity,
                value.platform_effective_at_utc_ms,
                value.paid_pusd,
            )?,
        }),
    };
    Ok(SettlementLifecycleEffectV1 {
        action: SettlementLifecycleActionV1::Rebate(action),
        account_delta,
    })
}

fn prepare_auto_redeem_effect_v2(
    account: &mut PlatformAccountStateV1,
    obligation: &MarketResolutionObligationV1,
    condition_id: &str,
    platform_effective_at_utc_ms: i64,
) -> Result<SettlementLifecycleEffectV1, MatchingStateErrorV1> {
    let reason_identity = format!(
        "AUTO_REDEEM_V1:{}:{condition_id}:{}:{}",
        condition_id.len(),
        obligation.resolution_event_id.len(),
        obligation.resolution_event_id,
    );
    let MarketResolutionTerminalV3::Resolved { payout_vector, .. } = &obligation.resolution;
    let AutoRedeemAccountEffectV1 {
        redeemed_outcome_balances,
        redeemed_pusd,
        account_delta,
    } = account.apply_auto_redeem(
        &reason_identity,
        condition_id,
        platform_effective_at_utc_ms,
        payout_vector,
    )?;
    Ok(SettlementLifecycleEffectV1 {
        action: SettlementLifecycleActionV1::AutoRedeem(AutoRedeemSettlementActionV1 {
            reason_identity,
            condition_id: condition_id.to_owned(),
            resolution_event_id: obligation.resolution_event_id.clone(),
            resolution_source_event_at_utc_ms: obligation.resolution_source_event_at_utc_ms,
            platform_effective_at_utc_ms,
            resolution: obligation.resolution.clone(),
            redeemed_outcome_balances,
            redeemed_pusd,
        }),
        account_delta,
    })
}

impl ExecutionStateV1 {
    pub(crate) fn resource_active_order_count(&self) -> usize {
        self.orders
            .values()
            .filter(|order| {
                order.open_base_units != 0 || order.pending_maker_depletion_base_units != 0
            })
            .count()
    }

    pub(crate) fn resource_internal_event_count(&self) -> usize {
        self.gtd_deadlines
            .len()
            .saturating_add(self.scheduled_finality.len())
            .saturating_add(
                self.market_resolutions
                    .values()
                    .map(|obligation| obligation.pending_trade_ids.len())
                    .sum::<usize>(),
            )
            .saturating_add(self.rebate.resource_record_count())
    }

    pub(crate) fn resource_index_count(&self) -> usize {
        let queue_indexes = self
            .queues
            .markets
            .values()
            .flat_map(|assets| assets.values())
            .map(|asset| {
                asset
                    .bids
                    .values()
                    .chain(asset.asks.values())
                    .map(|queue| 1_usize.saturating_add(queue.segments.len()))
                    .sum::<usize>()
                    .saturating_add(asset.bids.len())
                    .saturating_add(asset.asks.len())
            })
            .sum::<usize>();
        self.orders
            .len()
            .saturating_add(self.fills.len())
            .saturating_add(self.gtd_deadlines.len())
            .saturating_add(self.scheduled_finality.len())
            .saturating_add(self.matching_closed_markets.len())
            .saturating_add(self.market_resolutions.len())
            .saturating_add(self.rebate.resource_record_count())
            .saturating_add(queue_indexes)
    }

    pub(crate) fn resource_order_and_fill_count(&self) -> usize {
        self.orders.len().saturating_add(self.fills.len())
    }

    pub(crate) fn resource_active_order_encoded_bytes(&self) -> Result<u64, String> {
        let active = self
            .orders
            .values()
            .filter(|order| {
                order.open_base_units != 0 || order.pending_maker_depletion_base_units != 0
            })
            .collect::<Vec<_>>();
        let bytes = serde_json::to_vec(&active)
            .map_err(|error| format!("活动订单资源计量序列化失败：{error}"))?;
        u64::try_from(bytes.len()).map_err(|_| "活动订单资源字节超出 u64".to_owned())
    }

    pub(crate) fn resource_internal_event_encoded_bytes(&self) -> Result<u64, String> {
        let bytes = serde_json::to_vec(&(
            &self.gtd_deadlines,
            &self.scheduled_finality,
            &self.market_resolutions,
            &self.rebate,
        ))
        .map_err(|error| format!("内部事件资源计量序列化失败：{error}"))?;
        u64::try_from(bytes.len()).map_err(|_| "内部事件资源字节超出 u64".to_owned())
    }

    pub(crate) fn resource_index_encoded_bytes(&self) -> Result<u64, String> {
        let bytes = serde_json::to_vec(&(
            &self.queues,
            &self.account_overlay,
            &self.orders,
            &self.fills,
            &self.matching_closed_markets,
            &self.gtd_deadlines,
            &self.scheduled_finality,
            &self.market_resolutions,
            &self.rebate,
        ))
        .map_err(|error| format!("撮合索引资源计量序列化失败：{error}"))?;
        u64::try_from(bytes.len()).map_err(|_| "撮合索引资源字节超出 u64".to_owned())
    }

    pub(crate) fn new(
        directory: &SessionMarketDirectoryV1,
        opening_anchor: &contracts::AccountOpeningAnchorV1,
        rule_snapshot: &SealedProcessRuleSnapshot,
    ) -> Result<Self, MatchingStateErrorV1> {
        Ok(Self {
            queues: ExecutionQueueStateV1::new(directory),
            account_overlay: AccountBookOverlayStoreV1::new(directory),
            account: PlatformAccountStateV1::try_from_opening_anchor(opening_anchor)?,
            orders: BTreeMap::new(),
            fills: BTreeMap::new(),
            matching_closed_markets: BTreeMap::new(),
            gtd_deadlines: BTreeSet::new(),
            gtd_advanced_through_utc_ms: None,
            scheduled_finality: BTreeSet::new(),
            finality_advanced_through_utc_ms: None,
            rebate: Arc::new(RebateStateV1::new(directory, rule_snapshot)),
            market_resolutions: Arc::new(BTreeMap::new()),
        })
    }

    pub(crate) fn insert_market(
        &mut self,
        market: &PlatformMarketV1,
    ) -> Result<(), MatchingStateErrorV1> {
        let condition_id = &market.market().condition_id.0;
        if self.queues.contains_market(condition_id)
            || self.account_overlay.contains_market(condition_id)
        {
            return Err(MatchingStateErrorV1::MarketAlreadyInitialized(
                condition_id.clone(),
            ));
        }
        self.queues.insert_market(market)?;
        self.account_overlay.insert_market(market)?;
        if !self.rebate_mut().insert_market(market) {
            return Err(MatchingStateErrorV1::MarketAlreadyInitialized(
                condition_id.clone(),
            ));
        }
        Ok(())
    }

    pub(crate) fn account_overlay(&self) -> &AccountBookOverlayStoreV1 {
        &self.account_overlay
    }

    #[cfg(test)]
    pub(crate) fn account_overlay_mut(&mut self) -> &mut AccountBookOverlayStoreV1 {
        &mut self.account_overlay
    }

    pub(crate) fn available_pusd(&self) -> Result<Decimal, MatchingStateErrorV1> {
        Ok(self.account.available_pusd())
    }

    pub(crate) fn available_pusd_units(&self) -> u128 {
        self.account.available_pusd_units()
    }

    pub(crate) fn principal_encumbered_pusd(&self) -> Result<Decimal, MatchingStateErrorV1> {
        Ok(self.account.principal_encumbrance_pusd())
    }

    pub(crate) fn immediate_taker_fee_encumbered_pusd(
        &self,
    ) -> Result<Decimal, MatchingStateErrorV1> {
        Ok(self.account.immediate_taker_fee_encumbrance_pusd())
    }

    #[cfg(test)]
    pub(crate) fn account(&self) -> &PlatformAccountStateV1 {
        &self.account
    }

    pub(crate) fn account_instance_key(&self) -> &contracts::AccountInstanceKeyV1 {
        self.account.account_instance_key()
    }

    pub(crate) fn validate_recovered(
        &self,
        directory: &SessionMarketDirectoryV1,
        opening_anchor: &contracts::AccountOpeningAnchorV1,
        rules: &SealedProcessRuleSnapshot,
        finality_policy: &TradeFinalityPolicyV1,
        logical_platform_driven_through: Option<i64>,
        evicted_markets: &BTreeMap<String, TerminalExecutionMarketEvictionV1>,
    ) -> Result<(), MatchingStateErrorV1> {
        self.account.validate_restored_state(opening_anchor)?;
        if !self.queues.has_exact_directory_shape(directory) {
            return Err(MatchingStateErrorV1::RestoredDirectoryMismatch(
                "撮合队列与会话目录不一致",
            ));
        }
        if !self.account_overlay.has_exact_directory_shape(directory) {
            return Err(MatchingStateErrorV1::RestoredDirectoryMismatch(
                "账户盘口叠加层与会话目录不一致",
            ));
        }
        let mut evicted_rebate_fills = BTreeMap::new();
        for (condition_id, eviction) in evicted_markets {
            if condition_id != &eviction.condition_id
                || self
                    .orders
                    .values()
                    .any(|order| order.condition_id == *condition_id)
                || self
                    .fills
                    .values()
                    .any(|fill| fill.condition_id == *condition_id)
            {
                return Err(MatchingStateErrorV1::Finality(format!(
                    "恢复已驱逐市场仍含热订单、成交或身份冲突：{condition_id}"
                )));
            }
            for (trade_id, witness) in &eviction.fills {
                if trade_id != &witness.rebate_fill.trade_id
                    || witness.rebate_fill.condition_id != *condition_id
                    || witness.rebate_fill.terminal_succeeded.is_none()
                    || evicted_rebate_fills
                        .insert(trade_id.clone(), witness.rebate_fill.clone())
                        .is_some()
                {
                    return Err(MatchingStateErrorV1::Finality(format!(
                        "恢复已驱逐成交见证身份或终态冲突：{trade_id}"
                    )));
                }
            }
        }
        if !self.rebate.has_consistent_recovery_state(
            directory,
            rules,
            &self.fills,
            &evicted_rebate_fills,
            &evicted_markets.keys().cloned().collect(),
        ) {
            return Err(MatchingStateErrorV1::Rebate(
                "恢复返佣目录、成交前驱、日桶或时钟集合不一致".to_owned(),
            ));
        }
        self.validate_recovered_order_graph(directory)?;
        let expected_gtd_deadlines = self
            .orders
            .values()
            .filter_map(|order| match order.kind {
                SimOrderKindStateV1::Gtd { expires_at_utc_ms }
                    if order.pending_maker_depletion_base_units > 0
                        || order.open_base_units > 0 =>
                {
                    Some((expires_at_utc_ms, order.platform_order_id.clone()))
                }
                SimOrderKindStateV1::Gtc
                | SimOrderKindStateV1::Gtd { .. }
                | SimOrderKindStateV1::Fok
                | SimOrderKindStateV1::Fak => None,
            })
            .collect::<BTreeSet<_>>();
        if self.gtd_deadlines != expected_gtd_deadlines {
            return Err(MatchingStateErrorV1::Finality(
                "恢复 GTD 到期前驱集合不完整或含额外成员".to_owned(),
            ));
        }
        if self.gtd_advanced_through_utc_ms != logical_platform_driven_through {
            return Err(MatchingStateErrorV1::Finality(
                "恢复 GTD 驱动水位与会话统一逻辑驱动水位不一致".to_owned(),
            ));
        }
        self.validate_recovered_finality_predecessors(directory, finality_policy, evicted_markets)?;
        Ok(())
    }

    fn validate_recovered_order_graph(
        &self,
        directory: &SessionMarketDirectoryV1,
    ) -> Result<(), MatchingStateErrorV1> {
        let mut expected_overlay = AccountBookOverlayStoreV1::new(directory);
        let mut expected_queued_orders = BTreeMap::new();
        let mut fill_totals = BTreeMap::<String, (u128, u128)>::new();

        for (trade_id, fill) in &self.fills {
            if fill.base_units == 0
                || fill.quote_units == 0
                || fill.price_pusd_per_share <= Decimal::ZERO
                || fill.price_pusd_per_share >= Decimal::ONE
                || match fill.liquidity_role {
                    SimLiquidityRoleV1::MakerInferred => {
                        fill.immediate_taker_fee_units != 0
                            || fill
                                .maker_depletion_evidence
                                .as_ref()
                                .is_none_or(|evidence| {
                                    fill.source_identity != evidence.sha256.0
                                        || evidence.event_count == 0
                                        || evidence.first_source_event_id.is_empty()
                                        || evidence.last_source_event_id.is_empty()
                                })
                    }
                    SimLiquidityRoleV1::Taker => fill.maker_depletion_evidence.is_some(),
                }
            {
                return Err(recovered_order_graph_error(trade_id));
            }
            let totals = fill_totals
                .entry(fill.platform_order_id.clone())
                .or_default();
            totals.0 = totals
                .0
                .checked_add(fill.base_units)
                .ok_or_else(|| recovered_order_graph_error(trade_id))?;
            totals.1 = totals
                .1
                .checked_add(fill.quote_units)
                .ok_or_else(|| recovered_order_graph_error(trade_id))?;
        }

        for (platform_order_id, order) in &self.orders {
            if platform_order_id != &order.platform_order_id
                || order.global_request_id.is_empty()
                || order.item_id.is_empty()
                || order.item_idempotency_identity.is_empty()
                || order.last_updated_at_utc_ms < order.created_at_utc_ms
                || !directory.markets().any(|market| {
                    market.market().condition_id.0 == order.condition_id
                        && market
                            .assets()
                            .iter()
                            .any(|asset| asset.token_id.0 == order.token_id)
                })
                || (order.pending_maker_depletion_base_units == 0)
                    != order.pending_maker_depletion_evidence.is_none()
            {
                return Err(recovered_order_graph_error(platform_order_id));
            }
            let quote_total = order
                .matched_quote_units
                .checked_add(order.open_quote_units)
                .and_then(|value| value.checked_add(order.released_quote_units))
                .and_then(|value| value.checked_add(order.canceled_released_quote_units))
                .and_then(|value| value.checked_add(order.expired_released_quote_units))
                .and_then(|value| value.checked_add(order.venue_dust_released_quote_units))
                .and_then(|value| value.checked_add(order.market_resolved_released_quote_units))
                .ok_or_else(|| recovered_order_graph_error(platform_order_id))?;
            if quote_total != order.accepted_quote_units {
                return Err(recovered_order_graph_error(platform_order_id));
            }
            if let Some(accepted_base_units) = order.accepted_base_units {
                let base_total = order
                    .matched_base_units
                    .checked_add(order.pending_maker_depletion_base_units)
                    .and_then(|value| value.checked_add(order.open_base_units))
                    .and_then(|value| value.checked_add(order.canceled_base_units))
                    .and_then(|value| value.checked_add(order.expired_base_units))
                    .and_then(|value| value.checked_add(order.venue_dust_closed_base_units))
                    .and_then(|value| value.checked_add(order.market_resolved_canceled_base_units))
                    .ok_or_else(|| recovered_order_graph_error(platform_order_id))?;
                if base_total != accepted_base_units {
                    return Err(recovered_order_graph_error(platform_order_id));
                }
            }
            let (fill_base_units, fill_quote_units) =
                fill_totals.remove(platform_order_id).unwrap_or_default();
            if fill_base_units != order.matched_base_units
                || fill_quote_units != order.matched_quote_units
            {
                return Err(recovered_order_graph_error(platform_order_id));
            }
            if order.open_base_units > 0 {
                if !matches!(
                    order.kind,
                    SimOrderKindStateV1::Gtc | SimOrderKindStateV1::Gtd { .. }
                ) || order.terminal_reason.is_some()
                    || expected_overlay
                        .add_quantity(
                            &order.condition_id,
                            &order.token_id,
                            BookSide::Bid,
                            order.source_price_pusd_per_share,
                            decimal_from_micro_units_v1(order.open_base_units)?,
                        )
                        .is_none()
                {
                    return Err(recovered_order_graph_error(platform_order_id));
                }
                if !self
                    .matching_closed_markets
                    .contains_key(&order.condition_id)
                    && expected_queued_orders
                        .insert(
                            platform_order_id.clone(),
                            (
                                order.condition_id.clone(),
                                order.token_id.clone(),
                                order.source_price_pusd_per_share,
                                order.open_base_units,
                            ),
                        )
                        .is_some()
                {
                    return Err(recovered_order_graph_error(platform_order_id));
                }
            }
        }
        if !fill_totals.is_empty() || self.account_overlay != expected_overlay {
            return Err(recovered_order_graph_error("订单、成交与账户盘口叠加层"));
        }

        for (condition_id, closed_at_utc_ms) in &self.matching_closed_markets {
            let Some(market) = directory
                .markets()
                .find(|market| market.market().condition_id.0 == *condition_id)
            else {
                return Err(recovered_order_graph_error(condition_id));
            };
            if *closed_at_utc_ms != market.market().window.end_at_utc_ms {
                return Err(recovered_order_graph_error(condition_id));
            }
        }

        let mut actual_queued_orders = BTreeMap::new();
        for (condition_id, assets) in &self.queues.markets {
            let matching_closed = self.matching_closed_markets.contains_key(condition_id);
            for (token_id, asset) in assets {
                if matching_closed && (!asset.bids.is_empty() || !asset.asks.is_empty()) {
                    return Err(recovered_order_graph_error(condition_id));
                }
                for (side, levels) in [(BookSide::Bid, &asset.bids), (BookSide::Ask, &asset.asks)] {
                    for (price, queue) in levels {
                        for segment in &queue.segments {
                            if let QueueSegmentV1::Account(account) = segment
                                && (side != BookSide::Bid
                                    || account.remaining_base_units == 0
                                    || actual_queued_orders
                                        .insert(
                                            account.platform_order_id.clone(),
                                            (
                                                condition_id.clone(),
                                                token_id.clone(),
                                                *price,
                                                account.remaining_base_units,
                                            ),
                                        )
                                        .is_some())
                            {
                                return Err(recovered_order_graph_error(
                                    &account.platform_order_id,
                                ));
                            }
                        }
                    }
                }
            }
        }
        if actual_queued_orders != expected_queued_orders {
            return Err(recovered_order_graph_error("活动订单与撮合队列"));
        }
        Ok(())
    }

    fn validate_recovered_finality_predecessors(
        &self,
        directory: &SessionMarketDirectoryV1,
        finality_policy: &TradeFinalityPolicyV1,
        evicted_markets: &BTreeMap<String, TerminalExecutionMarketEvictionV1>,
    ) -> Result<(), MatchingStateErrorV1> {
        let mut expected_scheduled = BTreeSet::new();
        let mut expected_finality_advanced_through_utc_ms = None;
        let mut expected_principal_encumbrance_units = self
            .orders
            .values()
            .try_fold(0_u128, |total, order| {
                total.checked_add(order.open_quote_units)
            })
            .ok_or_else(|| {
                MatchingStateErrorV1::Finality("恢复订单开放本金占用合计溢出".to_owned())
            })?;
        let mut expected_fee_encumbrance_units = 0_u128;
        let mut expected_order_fee_encumbrance = BTreeMap::<String, u128>::new();
        for (trade_id, fill) in &self.fills {
            if trade_id != &fill.trade_id {
                return Err(MatchingStateErrorV1::Finality(
                    "恢复成交映射键与正文身份不一致".to_owned(),
                ));
            }
            let order = self.orders.get(&fill.platform_order_id).ok_or_else(|| {
                MatchingStateErrorV1::Finality(format!("恢复成交缺少所属订单：{trade_id}"))
            })?;
            if fill.condition_id != order.condition_id
                || fill.token_id != order.token_id
                || !directory.contains(&fill.condition_id)
            {
                return Err(MatchingStateErrorV1::Finality(format!(
                    "恢复成交所属市场、资产或订单不一致：{trade_id}"
                )));
            }
            let path = fill.finality_path.as_ref().ok_or_else(|| {
                MatchingStateErrorV1::Finality(format!("恢复成交缺少已冻结最终性路径：{trade_id}"))
            })?;
            let selected_path = finality_policy.select_path_for_fill(
                self.account_instance_key(),
                &order.global_request_id,
                &order.item_idempotency_identity,
                trade_id,
            );
            if path != selected_path {
                return Err(MatchingStateErrorV1::Finality(format!(
                    "恢复成交最终性路径不等于已密封策略选择结果：{trade_id}"
                )));
            }
            let ordinal = fill.finality_path_ordinal.ok_or_else(|| {
                MatchingStateErrorV1::Finality(format!("恢复成交缺少当前最终性序号：{trade_id}"))
            })?;
            let ordinal_index = usize::try_from(ordinal).map_err(|_| {
                MatchingStateErrorV1::Finality(format!("恢复成交最终性序号越界：{trade_id}"))
            })?;
            let current_step = path.steps().get(ordinal_index).ok_or_else(|| {
                MatchingStateErrorV1::Finality(format!("恢复成交最终性序号超出路径：{trade_id}"))
            })?;
            if fill.finality_state != Some(current_step.state()) {
                return Err(MatchingStateErrorV1::Finality(format!(
                    "恢复成交当前最终性状态与路径不一致：{trade_id}"
                )));
            }
            if ordinal > 0 {
                let current_at = fill
                    .matched_at_utc_ms
                    .checked_add(current_step.offset_ms())
                    .ok_or_else(|| {
                        MatchingStateErrorV1::Finality(format!(
                            "恢复成交当前最终性时点溢出：{trade_id}"
                        ))
                    })?;
                expected_finality_advanced_through_utc_ms = Some(
                    expected_finality_advanced_through_utc_ms
                        .map_or(current_at, |current: i64| current.max(current_at)),
                );
            }
            let economic_application_reached = path
                .economic_application_ordinal()
                .is_some_and(|economic_ordinal| ordinal >= economic_ordinal);
            self.validate_recovered_economic_application(fill, path, economic_application_reached)?;
            let fee_released = economic_application_reached
                || fill.finality_state == Some(TradeFinalityStateV1::Failed);
            if !fee_released {
                expected_principal_encumbrance_units = expected_principal_encumbrance_units
                    .checked_add(fill.quote_units)
                    .ok_or_else(|| {
                        MatchingStateErrorV1::Finality(format!(
                            "恢复成交本金占用合计溢出：{trade_id}"
                        ))
                    })?;
                expected_fee_encumbrance_units = expected_fee_encumbrance_units
                    .checked_add(fill.immediate_taker_fee_units)
                    .ok_or_else(|| {
                        MatchingStateErrorV1::Finality(format!(
                            "恢复成交手续费占用合计溢出：{trade_id}"
                        ))
                    })?;
                let order_fee = expected_order_fee_encumbrance
                    .entry(fill.platform_order_id.clone())
                    .or_default();
                *order_fee = order_fee
                    .checked_add(fill.immediate_taker_fee_units)
                    .ok_or_else(|| {
                        MatchingStateErrorV1::Finality(format!(
                            "恢复订单手续费占用合计溢出：{}",
                            fill.platform_order_id
                        ))
                    })?;
            }
            if let Some(next_step) = path.steps().get(ordinal_index + 1) {
                let path_ordinal = ordinal.checked_add(1).ok_or_else(|| {
                    MatchingStateErrorV1::Finality(format!(
                        "恢复成交下一最终性序号溢出：{trade_id}"
                    ))
                })?;
                let platform_event_at_utc_ms = fill
                    .matched_at_utc_ms
                    .checked_add(next_step.offset_ms())
                    .ok_or_else(|| {
                        MatchingStateErrorV1::Finality(format!(
                            "恢复成交下一最终性时点溢出：{trade_id}"
                        ))
                    })?;
                expected_scheduled.insert(ScheduledFinalityTransitionKeyV1 {
                    platform_event_at_utc_ms,
                    fill_economic_delta_identity: trade_id.clone(),
                    path_ordinal,
                });
            }
        }
        for eviction in evicted_markets.values() {
            for witness in eviction.fills.values() {
                let finality = recovered_terminal_finality(
                    self.account_instance_key(),
                    finality_policy,
                    witness,
                )?;
                if finality.terminal_ordinal > 0 {
                    expected_finality_advanced_through_utc_ms = Some(
                        expected_finality_advanced_through_utc_ms
                            .map_or(finality.terminal_at_utc_ms, |current: i64| {
                                current.max(finality.terminal_at_utc_ms)
                            }),
                    );
                }
            }
        }
        if self.scheduled_finality != expected_scheduled {
            return Err(MatchingStateErrorV1::Finality(
                "恢复成交的已排定最终性前驱集合不完整或含额外成员".to_owned(),
            ));
        }
        if self.finality_advanced_through_utc_ms != expected_finality_advanced_through_utc_ms {
            return Err(MatchingStateErrorV1::Finality(
                "恢复成交最终性驱动水位与当前路径前缀不一致".to_owned(),
            ));
        }
        for order in self.orders.values() {
            if order.immediate_taker_fee_encumbered_units
                != expected_order_fee_encumbrance
                    .remove(&order.platform_order_id)
                    .unwrap_or_default()
            {
                return Err(MatchingStateErrorV1::Finality(format!(
                    "恢复订单手续费占用与未闭合成交不一致：{}",
                    order.platform_order_id
                )));
            }
        }
        if !expected_order_fee_encumbrance.is_empty()
            || micro_units_v1(self.account.principal_encumbrance_pusd())?
                != expected_principal_encumbrance_units
            || micro_units_v1(self.account.immediate_taker_fee_encumbrance_pusd())?
                != expected_fee_encumbrance_units
        {
            return Err(MatchingStateErrorV1::Finality(
                "恢复账户资金占用与订单、成交经济前驱不一致".to_owned(),
            ));
        }

        for (condition_id, obligation) in self.market_resolutions.iter() {
            if !directory.contains(condition_id) {
                return Err(MatchingStateErrorV1::ResolutionConflict(
                    condition_id.clone(),
                ));
            }
            let expected_pending = self
                .fills
                .values()
                .filter(|fill| {
                    fill.condition_id == *condition_id
                        && !matches!(
                            fill.finality_state,
                            Some(TradeFinalityStateV1::Confirmed | TradeFinalityStateV1::Failed)
                        )
                })
                .map(|fill| fill.trade_id.clone())
                .collect::<BTreeSet<_>>();
            if obligation.pending_trade_ids != expected_pending
                || (obligation.redeemed_at_utc_ms.is_some()
                    && !obligation.pending_trade_ids.is_empty())
            {
                return Err(MatchingStateErrorV1::ResolutionConflict(
                    condition_id.clone(),
                ));
            }
        }
        Ok(())
    }

    fn validate_recovered_economic_application(
        &self,
        fill: &SimFillRecordV1,
        path: &TradeFinalityPathV1,
        economic_application_reached: bool,
    ) -> Result<(), MatchingStateErrorV1> {
        let Some(economic_ordinal) = path.economic_application_ordinal() else {
            if fill.economic_application_delta.is_some() {
                return Err(MatchingStateErrorV1::Finality(format!(
                    "恢复无经济应用截点的成交含经济变化：{}",
                    fill.trade_id
                )));
            }
            return Ok(());
        };
        if !economic_application_reached {
            if fill.economic_application_delta.is_some() {
                return Err(MatchingStateErrorV1::Finality(format!(
                    "恢复成交在经济截点前已经含经济变化：{}",
                    fill.trade_id
                )));
            }
            return Ok(());
        }
        let delta = fill.economic_application_delta.as_deref().ok_or_else(|| {
            MatchingStateErrorV1::Finality(format!(
                "恢复成交越过经济截点但缺少经济变化：{}",
                fill.trade_id
            ))
        })?;
        validate_platform_account_delta_integrity(delta).map_err(|error| {
            MatchingStateErrorV1::Finality(format!(
                "恢复成交经济变化正文非法：{}：{error}",
                fill.trade_id
            ))
        })?;
        let economic_step = path
            .steps()
            .get(usize::try_from(economic_ordinal).map_err(|_| {
                MatchingStateErrorV1::Finality(format!("恢复成交经济截点越界：{}", fill.trade_id))
            })?)
            .ok_or_else(|| {
                MatchingStateErrorV1::Finality(format!(
                    "恢复成交经济截点超出路径：{}",
                    fill.trade_id
                ))
            })?;
        let platform_effective_at_utc_ms = fill
            .matched_at_utc_ms
            .checked_add(economic_step.offset_ms())
            .ok_or_else(|| {
                MatchingStateErrorV1::Finality(format!(
                    "恢复成交经济截点时点溢出：{}",
                    fill.trade_id
                ))
            })?;
        let total_quote_and_fee = fill
            .quote_units
            .checked_add(fill.immediate_taker_fee_units)
            .ok_or_else(|| {
                MatchingStateErrorV1::Finality(format!(
                    "恢复成交经济变化金额溢出：{}",
                    fill.trade_id
                ))
            })?;
        let expected_balance_delta = PlatformAccountBalanceDeltaV1 {
            cash_pusd_delta: exact_signed_micro_units_v1(negative_units(total_quote_and_fee)?)?,
            available_pusd_delta: exact_signed_micro_units_v1(0)?,
            principal_encumbrance_pusd_delta: exact_signed_micro_units_v1(negative_units(
                fill.quote_units,
            )?)?,
            immediate_taker_fee_encumbrance_pusd_delta: exact_signed_micro_units_v1(
                negative_units(fill.immediate_taker_fee_units)?,
            )?,
            outcome_balance_deltas: vec![PlatformOutcomeBalanceDeltaV1 {
                token_id: TokenId(fill.token_id.clone()),
                balance_shares_delta: exact_signed_micro_units_v1(
                    i128::try_from(fill.base_units)
                        .map_err(|_| MatchingMathErrorV1::ArithmeticOverflow)?,
                )?,
            }],
            maker_rebate_receivable_pusd_delta: exact_signed_micro_units_v1(0)?,
            taker_rebate_receivable_pusd_delta: exact_signed_micro_units_v1(0)?,
            maker_rebate_locked_remainder_pusd_delta: exact_signed_micro_units_v1(0)?,
            taker_rebate_locked_remainder_pusd_delta: exact_signed_micro_units_v1(0)?,
        };
        if delta.account_instance_key != *self.account_instance_key()
            || delta.reason != PlatformAccountDeltaReasonV1::FillFinalityEconomicApplied
            || delta.reason_identity
                != format!(
                    "FILL_FINALITY_ECONOMIC:{}:{economic_ordinal}",
                    fill.trade_id
                )
            || delta.condition_id.as_ref().map(|value| value.0.as_str())
                != Some(fill.condition_id.as_str())
            || delta.platform_effective_at_utc_ms != platform_effective_at_utc_ms
            || delta.balance_delta != expected_balance_delta
        {
            return Err(MatchingStateErrorV1::Finality(format!(
                "恢复成交经济变化正文与已冻结截点不一致：{}",
                fill.trade_id
            )));
        }
        Ok(())
    }

    pub(crate) fn validate_recovered_account_delta_chain(
        &self,
        opening_anchor: &contracts::AccountOpeningAnchorV1,
        deltas: &BTreeMap<u64, contracts::PlatformAccountDeltaV1>,
        finality_policy: &TradeFinalityPolicyV1,
        evicted_markets: &BTreeMap<String, TerminalExecutionMarketEvictionV1>,
    ) -> Result<(), MatchingStateErrorV1> {
        self.account
            .validate_restored_delta_chain(opening_anchor, deltas)?;
        let mut expected_economic_deltas = BTreeMap::new();
        let mut expected_failure_delta_identities = BTreeSet::new();
        for fill in self.fills.values() {
            if let Some(delta) = fill.economic_application_delta.as_deref()
                && (deltas.get(&delta.account_transition_seq) != Some(delta)
                    || expected_economic_deltas
                        .insert(delta.reason_identity.clone(), delta)
                        .is_some())
            {
                return Err(MatchingStateErrorV1::Finality(format!(
                    "恢复成交经济变化没有绑定同一持久账户变化正文：{}",
                    fill.trade_id
                )));
            }
            if fill.finality_state == Some(TradeFinalityStateV1::Failed) {
                let ordinal = fill.finality_path_ordinal.ok_or_else(|| {
                    MatchingStateErrorV1::Finality(format!(
                        "恢复失败成交缺少当前最终性序号：{}",
                        fill.trade_id
                    ))
                })?;
                let compensated = fill.economic_application_delta.is_some();
                let (reason, reason_identity) = if compensated {
                    (
                        PlatformAccountDeltaReasonV1::FillFinalityCompensationApplied,
                        format!("FILL_FINALITY_COMPENSATION:{}:{ordinal}", fill.trade_id),
                    )
                } else {
                    (
                        PlatformAccountDeltaReasonV1::FillFinalityUnappliedFailureReleased,
                        format!(
                            "FILL_FINALITY_UNAPPLIED_FAILURE:{}:{ordinal}",
                            fill.trade_id
                        ),
                    )
                };
                let mut candidates = deltas.values().filter(|delta| {
                    delta.reason == reason && delta.reason_identity == reason_identity
                });
                let delta = candidates.next().ok_or_else(|| {
                    MatchingStateErrorV1::Finality(format!(
                        "恢复失败成交缺少补偿或释放账户变化：{}",
                        fill.trade_id
                    ))
                })?;
                if candidates.next().is_some()
                    || delta.account_instance_key != *self.account_instance_key()
                    || delta.condition_id.as_ref().map(|value| value.0.as_str())
                        != Some(fill.condition_id.as_str())
                    || delta.platform_effective_at_utc_ms != recovered_fill_current_step_at(fill)?
                    || delta.balance_delta != expected_failure_balance_delta(fill, compensated)?
                    || !expected_failure_delta_identities.insert(reason_identity)
                {
                    return Err(MatchingStateErrorV1::Finality(format!(
                        "恢复失败成交补偿或释放正文与最终性路径不一致：{}",
                        fill.trade_id
                    )));
                }
            }
        }
        for eviction in evicted_markets.values() {
            for witness in eviction.fills.values() {
                let trade_id = witness.rebate_fill.trade_id.as_str();
                let finality = recovered_terminal_finality(
                    self.account_instance_key(),
                    finality_policy,
                    witness,
                )?;
                if let Some(reference) = &witness.economic_application_delta {
                    let economic_ordinal =
                        finality.economic_application_ordinal.ok_or_else(|| {
                            MatchingStateErrorV1::Finality(format!(
                                "驱逐成交经济变化缺少应用序号：{trade_id}"
                            ))
                        })?;
                    let delta = deltas
                        .get(&reference.account_transition_seq)
                        .ok_or_else(|| {
                            MatchingStateErrorV1::Finality(format!(
                                "驱逐成交经济变化缺少账户正文：{trade_id}"
                            ))
                        })?;
                    let expected_identity =
                        format!("FILL_FINALITY_ECONOMIC:{trade_id}:{economic_ordinal}");
                    if delta.delta_id != reference.delta_id
                        || delta.delta_sha256 != reference.delta_sha256
                        || delta.reason != PlatformAccountDeltaReasonV1::FillFinalityEconomicApplied
                        || delta.reason_identity != expected_identity
                        || delta.condition_id.as_ref().map(|value| value.0.as_str())
                            != Some(eviction.condition_id.as_str())
                        || Some(delta.platform_effective_at_utc_ms)
                            != finality.economic_application_at_utc_ms
                        || delta.balance_delta != expected_evicted_economic_balance_delta(witness)?
                        || expected_economic_deltas
                            .insert(delta.reason_identity.clone(), delta)
                            .is_some()
                    {
                        return Err(MatchingStateErrorV1::Finality(format!(
                            "驱逐成交经济变化见证不一致：{trade_id}"
                        )));
                    }
                }
                if witness.rebate_fill.terminal_succeeded == Some(false) {
                    let compensated = witness.economic_application_delta.is_some();
                    let (reason, reason_identity) = if compensated {
                        (
                            PlatformAccountDeltaReasonV1::FillFinalityCompensationApplied,
                            format!(
                                "FILL_FINALITY_COMPENSATION:{}:{}",
                                trade_id, finality.terminal_ordinal
                            ),
                        )
                    } else {
                        (
                            PlatformAccountDeltaReasonV1::FillFinalityUnappliedFailureReleased,
                            format!(
                                "FILL_FINALITY_UNAPPLIED_FAILURE:{}:{}",
                                trade_id, finality.terminal_ordinal
                            ),
                        )
                    };
                    let mut candidates = deltas.values().filter(|delta| {
                        delta.reason == reason && delta.reason_identity == reason_identity
                    });
                    let delta = candidates.next().ok_or_else(|| {
                        MatchingStateErrorV1::Finality(format!(
                            "驱逐失败成交缺少补偿或释放账户变化：{trade_id}"
                        ))
                    })?;
                    if candidates.next().is_some()
                        || delta.account_instance_key != *self.account_instance_key()
                        || delta.condition_id.as_ref().map(|value| value.0.as_str())
                            != Some(eviction.condition_id.as_str())
                        || delta.platform_effective_at_utc_ms != finality.terminal_at_utc_ms
                        || delta.balance_delta
                            != expected_evicted_failure_balance_delta(witness, compensated)?
                        || !expected_failure_delta_identities.insert(reason_identity)
                    {
                        return Err(MatchingStateErrorV1::Finality(format!(
                            "驱逐失败成交补偿或释放见证不一致：{trade_id}"
                        )));
                    }
                }
            }
        }
        for delta in deltas.values() {
            let expected = match delta.reason {
                PlatformAccountDeltaReasonV1::FillFinalityEconomicApplied => {
                    expected_economic_deltas
                        .get(&delta.reason_identity)
                        .is_some_and(|expected| **expected == *delta)
                }
                PlatformAccountDeltaReasonV1::FillFinalityUnappliedFailureReleased
                | PlatformAccountDeltaReasonV1::FillFinalityCompensationApplied => {
                    expected_failure_delta_identities.contains(&delta.reason_identity)
                }
                PlatformAccountDeltaReasonV1::OrderAcceptedEncumbranceApplied
                | PlatformAccountDeltaReasonV1::FakUnspentEncumbranceReleased
                | PlatformAccountDeltaReasonV1::CallerCancellationEncumbranceReleased
                | PlatformAccountDeltaReasonV1::GtdExpirationEncumbranceReleased
                | PlatformAccountDeltaReasonV1::VenueDustEncumbranceReleased
                | PlatformAccountDeltaReasonV1::MarketResolutionEncumbranceReleased
                | PlatformAccountDeltaReasonV1::MakerRebateAccrualFinalized
                | PlatformAccountDeltaReasonV1::TakerRebateAccrualApplied
                | PlatformAccountDeltaReasonV1::MakerRebateDayFinalized
                | PlatformAccountDeltaReasonV1::TakerRebateDayFinalized
                | PlatformAccountDeltaReasonV1::MakerRebatePaid
                | PlatformAccountDeltaReasonV1::TakerRebatePaid
                | PlatformAccountDeltaReasonV1::AutoRedeemApplied => true,
            };
            if !expected {
                return Err(MatchingStateErrorV1::Finality(format!(
                    "恢复账户变化含未由成交最终性图授权的变化：{}",
                    delta.reason_identity
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn validate_recovered_market_resolutions(
        &self,
        qualified: &BTreeMap<String, (MarketResolutionTerminalV3, MarketLifecyclePhaseV1)>,
        lifecycle_phases: &BTreeMap<String, MarketLifecyclePhaseV1>,
    ) -> Result<(), MatchingStateErrorV1> {
        if lifecycle_phases.len() != self.queues.markets.len() {
            return Err(MatchingStateErrorV1::ResolutionConflict(
                "恢复市场生命周期相位与执行市场集合不一致".to_owned(),
            ));
        }
        for condition_id in self.queues.markets.keys() {
            let phase = lifecycle_phases
                .get(condition_id)
                .ok_or_else(|| MatchingStateErrorV1::ResolutionConflict(condition_id.clone()))?;
            let matching_must_be_closed = matches!(
                phase,
                MarketLifecyclePhaseV1::PostEndPendingResolution
                    | MarketLifecyclePhaseV1::ResolutionTerminal
                    | MarketLifecyclePhaseV1::Evicted
            );
            if self.matching_closed_markets.contains_key(condition_id) != matching_must_be_closed
                || (matches!(
                    phase,
                    MarketLifecyclePhaseV1::ResolutionTerminal | MarketLifecyclePhaseV1::Evicted
                ) && (!qualified.contains_key(condition_id)
                    || self
                        .market_resolutions
                        .get(condition_id)
                        .is_none_or(|obligation| obligation.redeemed_at_utc_ms.is_none())))
            {
                return Err(MatchingStateErrorV1::ResolutionConflict(
                    condition_id.clone(),
                ));
            }
        }
        for (condition_id, (resolution, phase)) in qualified {
            if lifecycle_phases.get(condition_id) != Some(phase)
                || !matches!(
                    phase,
                    MarketLifecyclePhaseV1::PostEndPendingResolution
                        | MarketLifecyclePhaseV1::ResolutionTerminal
                        | MarketLifecyclePhaseV1::Evicted
                )
            {
                return Err(MatchingStateErrorV1::ResolutionConflict(
                    condition_id.clone(),
                ));
            }
            let Some(obligation) = self.market_resolutions.get(condition_id) else {
                continue;
            };
            let redeemed = obligation.redeemed_at_utc_ms.is_some();
            if obligation.resolution != *resolution
                || !matches!(
                    (redeemed, phase),
                    (false, MarketLifecyclePhaseV1::PostEndPendingResolution)
                        | (
                            true,
                            MarketLifecyclePhaseV1::ResolutionTerminal
                                | MarketLifecyclePhaseV1::Evicted
                        )
                )
            {
                return Err(MatchingStateErrorV1::ResolutionConflict(
                    condition_id.clone(),
                ));
            }
        }
        if self
            .market_resolutions
            .keys()
            .any(|condition_id| !qualified.contains_key(condition_id))
        {
            return Err(MatchingStateErrorV1::ResolutionConflict(
                "恢复执行结算义务缺少原始合格市场判定".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn order(&self, platform_order_id: &str) -> Option<&SimOrderRecordV1> {
        self.orders.get(platform_order_id)
    }

    pub(crate) fn orders(&self) -> impl Iterator<Item = &SimOrderRecordV1> {
        self.orders.values()
    }

    pub(crate) fn active_order_ids(&self) -> Result<Vec<String>, MatchingStateErrorV1> {
        let mut ids = Vec::new();
        for order in self.orders.values() {
            let active = order
                .pending_maker_depletion_base_units
                .checked_add(order.open_base_units)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            if active > 0 {
                ids.push(order.platform_order_id.clone());
            }
        }
        Ok(ids)
    }

    pub(crate) fn fill(&self, trade_id: &str) -> Option<&SimFillRecordV1> {
        self.fills.get(trade_id)
    }

    pub(crate) fn fills(&self) -> impl Iterator<Item = &SimFillRecordV1> {
        self.fills.values()
    }

    pub(crate) fn evict_terminal_market_hot_state(
        &mut self,
        condition_id: &str,
        lifecycle_phase: MarketLifecyclePhaseV1,
        finality_policy: &TradeFinalityPolicyV1,
    ) -> Result<TerminalExecutionMarketEvictionV1, MatchingStateErrorV1> {
        self.validate_terminal_market_economic_closure(condition_id, lifecycle_phase)?;
        let fills = self
            .fills
            .iter()
            .filter(|(_, fill)| fill.condition_id == condition_id)
            .map(|(key, fill)| {
                let order = self.orders.get(&fill.platform_order_id).ok_or_else(|| {
                    MatchingStateErrorV1::Finality(format!(
                        "终态成交缺少所属订单：{}",
                        fill.trade_id
                    ))
                })?;
                let finality_path = fill.finality_path.as_ref().ok_or_else(|| {
                    MatchingStateErrorV1::Finality(format!(
                        "终态成交缺少冻结最终性路径：{}",
                        fill.trade_id
                    ))
                })?;
                let finality_path_ordinal = fill.finality_path_ordinal.ok_or_else(|| {
                    MatchingStateErrorV1::Finality(format!(
                        "终态成交缺少最终性序号：{}",
                        fill.trade_id
                    ))
                })?;
                let selected_path = finality_policy.select_path_for_fill(
                    self.account_instance_key(),
                    &order.global_request_id,
                    &order.item_idempotency_identity,
                    &fill.trade_id,
                );
                let finality_path_ordinal_index =
                    usize::try_from(finality_path_ordinal).map_err(|_| {
                        MatchingStateErrorV1::Finality(format!(
                            "终态成交最终性序号越界：{}",
                            fill.trade_id
                        ))
                    })?;
                if finality_path != selected_path
                    || finality_path_ordinal_index.checked_add(1)
                        != Some(finality_path.steps().len())
                {
                    return Err(MatchingStateErrorV1::Finality(format!(
                        "终态成交最终性路径未闭合到已密封策略末项：{}",
                        fill.trade_id
                    )));
                }
                let terminal_succeeded = match fill.finality_state {
                    Some(TradeFinalityStateV1::Confirmed) => Some(true),
                    Some(TradeFinalityStateV1::Failed) => Some(false),
                    _ => None,
                };
                let witness = TerminalFillEvictionWitnessV1 {
                    rebate_fill: RebateRecoveryFillV1 {
                        trade_id: fill.trade_id.clone(),
                        condition_id: fill.condition_id.clone(),
                        matched_at_utc_ms: fill.matched_at_utc_ms,
                        base_units: fill.base_units,
                        price_pusd_per_share: fill.price_pusd_per_share,
                        immediate_taker_fee_units: fill.immediate_taker_fee_units,
                        liquidity_role: fill.liquidity_role,
                        terminal_succeeded,
                    },
                    token_id: fill.token_id.clone(),
                    quote_units: fill.quote_units,
                    finality_selector_identity: terminal_finality_selector_identity(
                        finality_policy,
                        order,
                    ),
                    economic_application_delta: fill
                        .economic_application_delta
                        .as_deref()
                        .map(TerminalAccountDeltaReferenceV1::from),
                };
                Ok((key.clone(), witness))
            })
            .collect::<Result<BTreeMap<_, _>, MatchingStateErrorV1>>()?;
        self.rebate_mut()
            .evict_terminal_market_fill_state(condition_id)?;
        self.orders
            .retain(|_, order| order.condition_id != condition_id);
        self.fills
            .retain(|_, fill| fill.condition_id != condition_id);
        Ok(TerminalExecutionMarketEvictionV1 {
            condition_id: condition_id.to_owned(),
            fills,
        })
    }

    fn validate_terminal_market_economic_closure(
        &self,
        condition_id: &str,
        lifecycle_phase: MarketLifecyclePhaseV1,
    ) -> Result<(), MatchingStateErrorV1> {
        if lifecycle_phase != MarketLifecyclePhaseV1::ResolutionTerminal {
            return Err(MatchingStateErrorV1::Finality(format!(
                "市场尚未进入 ResolutionTerminal：{condition_id}"
            )));
        }
        if !self.matching_closed_markets.contains_key(condition_id) {
            return Err(MatchingStateErrorV1::Finality(format!(
                "市场撮合尚未关闭：{condition_id}"
            )));
        }
        let queue_assets = self
            .queues
            .markets
            .get(condition_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownMarket(condition_id.to_owned()))?;
        if queue_assets
            .values()
            .any(|asset| !asset.bids.is_empty() || !asset.asks.is_empty())
            || self.account_overlay.market_is_empty(condition_id) != Some(true)
        {
            return Err(MatchingStateErrorV1::Finality(format!(
                "市场撮合队列或账户叠加层仍含热状态：{condition_id}"
            )));
        }

        let market_orders = self
            .orders
            .iter()
            .filter(|(_, order)| order.condition_id == condition_id)
            .collect::<BTreeMap<_, _>>();
        let market_fills = self
            .fills
            .iter()
            .filter(|(_, fill)| fill.condition_id == condition_id)
            .collect::<BTreeMap<_, _>>();
        if market_orders.iter().any(|(key, order)| {
            *key != &order.platform_order_id
                || order.pending_maker_depletion_base_units != 0
                || order.open_base_units != 0
                || order.open_quote_units != 0
                || order.immediate_taker_fee_encumbered_units != 0
        }) {
            return Err(MatchingStateErrorV1::Finality(format!(
                "市场订单仍有活动数量或经济占用：{condition_id}"
            )));
        }
        if self
            .gtd_deadlines
            .iter()
            .any(|(_, platform_order_id)| market_orders.contains_key(platform_order_id))
        {
            return Err(MatchingStateErrorV1::Finality(format!(
                "市场仍有 GTD 到期前驱：{condition_id}"
            )));
        }
        if market_fills.iter().any(|(key, fill)| {
            *key != &fill.trade_id
                || !market_orders.contains_key(&fill.platform_order_id)
                || !matches!(
                    fill.finality_state,
                    Some(TradeFinalityStateV1::Confirmed | TradeFinalityStateV1::Failed)
                )
        }) || self
            .scheduled_finality
            .iter()
            .any(|scheduled| market_fills.contains_key(&scheduled.fill_economic_delta_identity))
        {
            return Err(MatchingStateErrorV1::Finality(format!(
                "市场成交尚未全部进入最终性终态：{condition_id}"
            )));
        }
        let obligation = self.market_resolutions.get(condition_id).ok_or_else(|| {
            MatchingStateErrorV1::Finality(format!("市场缺少判定义务：{condition_id}"))
        })?;
        if !obligation.pending_trade_ids.is_empty()
            || obligation.redeemed_at_utc_ms.is_none()
            || obligation.invalidated_by_quality_reason.is_some()
        {
            return Err(MatchingStateErrorV1::Finality(format!(
                "市场判定义务尚未完成赎回：{condition_id}"
            )));
        }
        Ok(())
    }

    pub(crate) fn query_projection_changed_from(&self, previous: &Self) -> bool {
        self.orders != previous.orders || self.fills != previous.fills
    }

    #[cfg(test)]
    pub(crate) fn market_matching_queue_footprint(
        &self,
        condition_id: &str,
    ) -> Option<(usize, usize, u128)> {
        self.queues.markets.get(condition_id).map(|assets| {
            let mut level_count = 0_usize;
            let mut segment_count = 0_usize;
            let mut debt_base_units = 0_u128;
            for asset in assets.values() {
                for levels in [&asset.bids, &asset.asks] {
                    level_count += levels.len();
                    for queue in levels.values() {
                        segment_count += queue.segments.len();
                        debt_base_units = debt_base_units
                            .checked_add(queue.account_taker_depletion_debt_units)
                            .expect("测试撮合队列债务总量不得溢出");
                    }
                }
            }
            (level_count, segment_count, debt_base_units)
        })
    }

    #[cfg(test)]
    pub(crate) fn cloned_state_shares_rebate_storage(&self) -> bool {
        let cloned = self.clone();
        Arc::ptr_eq(&self.rebate, &cloned.rebate)
    }

    #[cfg(test)]
    pub(crate) fn retained_rebate_fill_state_count(&self) -> usize {
        self.rebate.retained_fill_state_count()
    }

    #[cfg(test)]
    pub(crate) fn cloned_state_shares_market_resolution_storage(&self) -> bool {
        let cloned = self.clone();
        Arc::ptr_eq(&self.market_resolutions, &cloned.market_resolutions)
    }

    pub(crate) fn initialize_fill_finality(
        &mut self,
        policy: &TradeFinalityPolicyV1,
        trade_id: &str,
    ) -> Result<TradeFinalityLifecycleEffectsV1, MatchingStateErrorV1> {
        let fill = self
            .fills
            .get(trade_id)
            .cloned()
            .ok_or_else(|| MatchingStateErrorV1::Finality(format!("未知成交：{trade_id}")))?;
        let order = self.orders.get(&fill.platform_order_id).ok_or_else(|| {
            MatchingStateErrorV1::AccountOrderConflict(fill.platform_order_id.clone())
        })?;
        let path =
            prepare_initial_finality_path_v2(self.account_instance_key(), policy, order, &fill)?;
        match fill.liquidity_role {
            SimLiquidityRoleV1::MakerInferred => self.rebate_mut().register_maker_fill(
                &fill.trade_id,
                &fill.condition_id,
                fill.matched_at_utc_ms,
                fill.base_units,
                fill.price_pusd_per_share,
            )?,
            SimLiquidityRoleV1::Taker => {
                self.rebate_mut().register_taker_fill(
                    &fill.trade_id,
                    &fill.condition_id,
                    fill.matched_at_utc_ms,
                    decimal_from_micro_units_v1(fill.immediate_taker_fee_units)?,
                )?;
            }
        }
        let trade_feedback =
            self.apply_finality_transition(trade_id, path, 0, fill.matched_at_utc_ms, true)?;
        let settlement_feedbacks = self.apply_terminal_settlement_actions(&trade_feedback)?;
        Ok(TradeFinalityLifecycleEffectsV1 {
            trade_feedback,
            settlement_feedbacks,
        })
    }

    pub(crate) fn next_scheduled_finality_at_utc_ms(&self) -> Option<i64> {
        self.scheduled_finality
            .first()
            .map(|key| key.platform_event_at_utc_ms)
    }

    pub(crate) fn advance_finality_at(
        &mut self,
        platform_event_at_utc_ms: i64,
    ) -> Result<Vec<TradeFinalityLifecycleEffectsV1>, MatchingStateErrorV1> {
        if self
            .finality_advanced_through_utc_ms
            .is_some_and(|current| platform_event_at_utc_ms < current)
        {
            return Err(MatchingStateErrorV1::LogicalTimeRegression);
        }
        let Some(next_at) = self.next_scheduled_finality_at_utc_ms() else {
            return Ok(Vec::new());
        };
        if next_at != platform_event_at_utc_ms {
            return Err(MatchingStateErrorV1::Finality(format!(
                "最终性驱动必须从最早时点 {next_at} 开始，实际为 {platform_event_at_utc_ms}"
            )));
        }
        let keys = self
            .scheduled_finality
            .iter()
            .take_while(|key| key.platform_event_at_utc_ms == platform_event_at_utc_ms)
            .cloned()
            .collect::<Vec<_>>();
        let mut effects = Vec::with_capacity(keys.len());
        for key in keys {
            if !self.scheduled_finality.remove(&key) {
                return Err(MatchingStateErrorV1::Finality(
                    "已排定最终性转换丢失".to_owned(),
                ));
            }
            let path = self
                .fills
                .get(&key.fill_economic_delta_identity)
                .and_then(|fill| fill.finality_path.clone())
                .ok_or_else(|| {
                    MatchingStateErrorV1::Finality(format!(
                        "已排定成交缺少最终性实例：{}",
                        key.fill_economic_delta_identity
                    ))
                })?;
            let trade_feedback = self.apply_finality_transition(
                &key.fill_economic_delta_identity,
                path,
                key.path_ordinal,
                key.platform_event_at_utc_ms,
                false,
            )?;
            let settlement_feedbacks = self.apply_terminal_settlement_actions(&trade_feedback)?;
            effects.push(TradeFinalityLifecycleEffectsV1 {
                trade_feedback,
                settlement_feedbacks,
            });
        }
        self.finality_advanced_through_utc_ms = Some(platform_event_at_utc_ms);
        Ok(effects)
    }

    pub(crate) fn next_rebate_clock_at_utc_ms(&self) -> Option<i64> {
        self.rebate.next_clock_at_utc_ms()
    }

    pub(crate) fn advance_rebate_clock_at(
        &mut self,
        platform_event_at_utc_ms: i64,
    ) -> Result<Vec<SettlementLifecycleEffectV1>, MatchingStateErrorV1> {
        let actions = self.rebate_mut().advance_clock_at(platform_event_at_utc_ms);
        let mut effects = Vec::new();
        for action in actions {
            let finalized_market = match &action {
                RebateSettlementActionV1::MakerMarketFinalized(finalized) => {
                    Some(finalized.condition_id.clone())
                }
                RebateSettlementActionV1::TakerAccrualApplied(_)
                | RebateSettlementActionV1::DayFinalized(_)
                | RebateSettlementActionV1::Paid(_) => None,
            };
            effects.extend(self.apply_rebate_actions(vec![action])?);
            if let Some(condition_id) = finalized_market
                && let Some(auto_redeem) =
                    self.try_auto_redeem(&condition_id, platform_event_at_utc_ms)?
            {
                effects.push(auto_redeem);
            }
        }
        Ok(effects)
    }

    fn apply_terminal_settlement_actions(
        &mut self,
        trade_feedback: &TradeFinalityTransitionEffectV1,
    ) -> Result<Vec<SettlementLifecycleEffectV1>, MatchingStateErrorV1> {
        let succeeded = match trade_feedback.finality_state {
            TradeFinalityStateV1::Confirmed => true,
            TradeFinalityStateV1::Failed => false,
            TradeFinalityStateV1::MatchedNotBroadcasted
            | TradeFinalityStateV1::Matched
            | TradeFinalityStateV1::Retrying
            | TradeFinalityStateV1::Mined => return Ok(Vec::new()),
        };
        let actions = self.rebate_mut().on_fill_terminal(
            &trade_feedback.trade_id,
            succeeded,
            trade_feedback.platform_event_at_utc_ms,
        );
        let (pre_redeem, post_redeem): (Vec<_>, Vec<_>) = actions
            .into_iter()
            .partition(rebate_action_precedes_auto_redeem_v2);
        let mut effects = self.apply_rebate_actions(pre_redeem)?;
        let pending_trade_exists = self
            .market_resolutions
            .get(&trade_feedback.condition_id)
            .is_some_and(|obligation| {
                obligation
                    .pending_trade_ids
                    .contains(&trade_feedback.trade_id)
            });
        if pending_trade_exists {
            self.market_resolutions_mut()
                .get_mut(&trade_feedback.condition_id)
                .expect("待闭合成交所属市场判定义务必须仍存在")
                .pending_trade_ids
                .remove(&trade_feedback.trade_id);
        }
        if let Some(auto_redeem) = self.try_auto_redeem(
            &trade_feedback.condition_id,
            trade_feedback.platform_event_at_utc_ms,
        )? {
            effects.push(auto_redeem);
        }
        effects.extend(self.apply_rebate_actions(post_redeem)?);
        Ok(effects)
    }

    fn try_auto_redeem(
        &mut self,
        condition_id: &str,
        platform_effective_at_utc_ms: i64,
    ) -> Result<Option<SettlementLifecycleEffectV1>, MatchingStateErrorV1> {
        let Some(obligation) = self.market_resolutions.get(condition_id).cloned() else {
            return Ok(None);
        };
        if obligation.redeemed_at_utc_ms.is_some()
            || !obligation.pending_trade_ids.is_empty()
            || self.rebate.market_result(condition_id).is_none()
        {
            return Ok(None);
        }
        if obligation.invalidated_by_quality_reason.is_some() {
            return Err(MatchingStateErrorV1::ResolutionConflict(
                condition_id.to_owned(),
            ));
        }
        let effect = prepare_auto_redeem_effect_v2(
            &mut self.account,
            &obligation,
            condition_id,
            platform_effective_at_utc_ms,
        )?;
        self.market_resolutions_mut()
            .get_mut(condition_id)
            .expect("自动赎回义务必须仍存在")
            .redeemed_at_utc_ms = Some(platform_effective_at_utc_ms);
        Ok(Some(effect))
    }

    fn apply_rebate_actions(
        &mut self,
        actions: Vec<RebateSettlementActionV1>,
    ) -> Result<Vec<SettlementLifecycleEffectV1>, MatchingStateErrorV1> {
        apply_rebate_actions_to_account_v2(&mut self.account, actions)
    }

    fn rebate_mut(&mut self) -> &mut RebateStateV1 {
        Arc::make_mut(&mut self.rebate)
    }

    fn market_resolutions_mut(&mut self) -> &mut BTreeMap<String, MarketResolutionObligationV1> {
        Arc::make_mut(&mut self.market_resolutions)
    }

    fn apply_finality_transition(
        &mut self,
        trade_id: &str,
        path: TradeFinalityPathV1,
        path_ordinal: u32,
        platform_event_at_utc_ms: i64,
        initialize: bool,
    ) -> Result<TradeFinalityTransitionEffectV1, MatchingStateErrorV1> {
        let platform_order_id = self
            .fills
            .get(trade_id)
            .ok_or_else(|| MatchingStateErrorV1::Finality(format!("未知成交：{trade_id}")))?
            .platform_order_id
            .clone();
        let order = self
            .orders
            .get_mut(&platform_order_id)
            .ok_or_else(|| MatchingStateErrorV1::AccountOrderConflict(platform_order_id.clone()))?;
        let fill = self
            .fills
            .get_mut(trade_id)
            .expect("已验证成交必须仍在执行状态中");
        let mut scheduled_to_insert = BTreeSet::new();
        let effect = apply_finality_transition_components_v2(
            &mut self.account,
            order,
            fill,
            &self.scheduled_finality,
            &mut scheduled_to_insert,
            path,
            path_ordinal,
            platform_event_at_utc_ms,
            initialize,
        )?;
        self.scheduled_finality.extend(scheduled_to_insert);
        Ok(effect)
    }

    #[cfg(test)]
    pub(crate) fn order_count(&self) -> usize {
        self.orders.len()
    }

    pub(crate) fn fills_for_order<'a>(
        &'a self,
        platform_order_id: &'a str,
    ) -> impl Iterator<Item = &'a SimFillRecordV1> + 'a {
        self.fills
            .values()
            .filter(move |fill| fill.platform_order_id == platform_order_id)
    }

    pub(crate) fn best_executable_ask(
        &self,
        condition_id: &str,
        token_id: &str,
    ) -> Option<Decimal> {
        self.queues.best_executable_ask(condition_id, token_id)
    }

    pub(crate) fn match_taker_buy_by_quote(
        &mut self,
        condition_id: &str,
        token_id: &str,
        worst_price: Decimal,
        quote_budget_units: u128,
    ) -> Result<QueueMatchPlanV1, MatchingStateErrorV1> {
        self.queues.match_taker_buy_by_quote(
            condition_id,
            token_id,
            worst_price,
            quote_budget_units,
        )
    }

    pub(crate) fn prepare_taker_buy_by_quote_v2(
        &mut self,
        condition_id: &str,
        token_id: &str,
        worst_price: Decimal,
        quote_budget_units: u128,
    ) -> Result<PreparedTakerAssetQueueChangeV2<'_>, MatchingStateErrorV1> {
        self.queues.prepare_taker_buy_by_quote_v2(
            condition_id,
            token_id,
            worst_price,
            quote_budget_units,
        )
    }

    pub(crate) fn prepare_new_order_taker_buy_by_quote_v2(
        &mut self,
        condition_id: &str,
        token_id: &str,
        worst_price: Decimal,
        quote_budget_units: u128,
    ) -> Result<PreparedNewOrderMatchBuilderV2<'_>, MatchingStateErrorV1> {
        let Self {
            queues,
            account_overlay,
            account,
            orders,
            fills,
            gtd_deadlines,
            scheduled_finality,
            rebate,
            ..
        } = self;
        let queue_change = queues.prepare_taker_buy_by_quote_v2(
            condition_id,
            token_id,
            worst_price,
            quote_budget_units,
        )?;
        Ok(PreparedNewOrderMatchBuilderV2 {
            queue_change,
            account_overlay,
            account,
            orders,
            fills,
            gtd_deadlines,
            scheduled_finality,
            rebate,
            condition_id: condition_id.to_owned(),
            token_id: token_id.to_owned(),
            maximum_price: worst_price,
            target: TakerTargetV1::Quote(quote_budget_units),
        })
    }

    pub(crate) fn match_taker_buy_by_base(
        &mut self,
        condition_id: &str,
        token_id: &str,
        limit_price: Decimal,
        base_target_units: u128,
        quote_budget_units: u128,
    ) -> Result<QueueMatchPlanV1, MatchingStateErrorV1> {
        self.queues.match_taker_buy_by_base(
            condition_id,
            token_id,
            limit_price,
            base_target_units,
            quote_budget_units,
        )
    }

    pub(crate) fn prepare_taker_buy_by_base_v2(
        &mut self,
        condition_id: &str,
        token_id: &str,
        limit_price: Decimal,
        base_target_units: u128,
        quote_budget_units: u128,
    ) -> Result<PreparedTakerAssetQueueChangeV2<'_>, MatchingStateErrorV1> {
        self.queues.prepare_taker_buy_by_base_v2(
            condition_id,
            token_id,
            limit_price,
            base_target_units,
            quote_budget_units,
        )
    }

    pub(crate) fn prepare_new_order_taker_buy_by_base_v2(
        &mut self,
        condition_id: &str,
        token_id: &str,
        limit_price: Decimal,
        base_target_units: u128,
        quote_budget_units: u128,
    ) -> Result<PreparedNewOrderMatchBuilderV2<'_>, MatchingStateErrorV1> {
        let Self {
            queues,
            account_overlay,
            account,
            orders,
            fills,
            gtd_deadlines,
            scheduled_finality,
            rebate,
            ..
        } = self;
        let queue_change = queues.prepare_taker_buy_by_base_v2(
            condition_id,
            token_id,
            limit_price,
            base_target_units,
            quote_budget_units,
        )?;
        Ok(PreparedNewOrderMatchBuilderV2 {
            queue_change,
            account_overlay,
            account,
            orders,
            fills,
            gtd_deadlines,
            scheduled_finality,
            rebate,
            condition_id: condition_id.to_owned(),
            token_id: token_id.to_owned(),
            maximum_price: limit_price,
            target: TakerTargetV1::LimitBase {
                total_base_units: base_target_units,
                total_quote_units: quote_budget_units,
            },
        })
    }

    pub(crate) fn apply_venue_dust_after_order_action(
        &mut self,
        platform_order_id: &str,
        platform_event_at_utc_ms: i64,
        lifecycle_account_rule: &LifecycleAccountRuleV1,
    ) -> Result<Option<VenueDustEffectV1>, MatchingStateErrorV1> {
        let order = self.orders.get(platform_order_id).ok_or_else(|| {
            MatchingStateErrorV1::AccountOrderConflict(platform_order_id.to_owned())
        })?;
        let upper_exclusive_base_units = micro_units_v1(
            lifecycle_account_rule
                .venue_dust_upper_exclusive_shares
                .value(),
        )?;
        if order.matched_base_units == 0
            || order.pending_maker_depletion_base_units != 0
            || order.open_base_units == 0
            || order.open_base_units >= upper_exclusive_base_units
        {
            return Ok(None);
        }
        let Some(candidate) = self.queues.venue_dust_candidate_for_order(
            &order.condition_id,
            &order.token_id,
            order.source_price_pusd_per_share,
            platform_order_id,
        ) else {
            return Ok(None);
        };
        self.apply_venue_dust_terminal(
            candidate,
            platform_event_at_utc_ms,
            &format!("ORDER_ACTION:{platform_order_id}"),
            lifecycle_account_rule,
        )
    }

    fn apply_qualified_market_resolution(
        &mut self,
        event: &ExecutionMarketEventV3,
        platform_effective_at_utc_ms: i64,
    ) -> Result<
        (
            Vec<MarketResolutionOrderEffectV1>,
            Option<SettlementLifecycleEffectV1>,
        ),
        MatchingStateErrorV1,
    > {
        let ExecutionMarketEventPayloadV3::MarketResolutionTerminal(resolution) = &event.payload
        else {
            return Ok((Vec::new(), None));
        };
        let condition_id = event.market.condition_id.0.as_str();
        let source_event_at_utc_ms = event.time.source_event_at_utc_ms.ok_or_else(|| {
            MatchingStateErrorV1::Finality("合格市场判定缺少数据源时点".to_owned())
        })?;
        if let Some(existing) = self.market_resolutions.get(condition_id) {
            if existing.resolution == *resolution {
                return Ok((Vec::new(), None));
            }
            return Err(MatchingStateErrorV1::ResolutionConflict(
                condition_id.to_owned(),
            ));
        }

        if !self.matching_closed_markets.contains_key(condition_id) {
            self.queues.release_market_matching_state(condition_id)?;
            self.matching_closed_markets
                .insert(condition_id.to_owned(), event.market.window.end_at_utc_ms);
        }

        let active_order_ids = self
            .orders
            .iter()
            .filter(|(_, order)| {
                order.condition_id == condition_id
                    && (order.pending_maker_depletion_base_units > 0 || order.open_base_units > 0)
            })
            .map(|(platform_order_id, _)| platform_order_id.clone())
            .collect::<Vec<_>>();
        let pending_trade_ids = self
            .fills
            .values()
            .filter(|fill| {
                fill.condition_id == condition_id
                    && !matches!(
                        fill.finality_state,
                        Some(TradeFinalityStateV1::Confirmed | TradeFinalityStateV1::Failed)
                    )
            })
            .map(|fill| fill.trade_id.clone())
            .collect::<BTreeSet<_>>();

        let mut order_effects = Vec::with_capacity(active_order_ids.len());
        for platform_order_id in active_order_ids {
            order_effects.push(self.apply_market_resolution_order_terminal(
                &platform_order_id,
                &event.event_id.0,
                platform_effective_at_utc_ms,
            )?);
        }
        self.market_resolutions_mut().insert(
            condition_id.to_owned(),
            MarketResolutionObligationV1 {
                resolution_event_id: event.event_id.0.clone(),
                resolution_source_event_at_utc_ms: source_event_at_utc_ms,
                resolution: resolution.clone(),
                pending_trade_ids,
                redeemed_at_utc_ms: None,
                invalidated_by_quality_reason: None,
            },
        );
        let auto_redeem = self.try_auto_redeem(condition_id, platform_effective_at_utc_ms)?;
        Ok((order_effects, auto_redeem))
    }

    fn apply_market_resolution_order_terminal(
        &mut self,
        platform_order_id: &str,
        resolution_event_id: &str,
        platform_effective_at_utc_ms: i64,
    ) -> Result<MarketResolutionOrderEffectV1, MatchingStateErrorV1> {
        let snapshot = self.orders.get(platform_order_id).cloned().ok_or_else(|| {
            MatchingStateErrorV1::AccountOrderConflict(platform_order_id.to_owned())
        })?;
        let active_base_units = snapshot
            .pending_maker_depletion_base_units
            .checked_add(snapshot.open_base_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let accepted_base_units = snapshot
            .accepted_base_units
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        if active_base_units == 0
            || snapshot.open_quote_units == 0
            || !matches!(
                snapshot.kind,
                SimOrderKindStateV1::Gtc | SimOrderKindStateV1::Gtd { .. }
            )
            || snapshot.terminal_reason.is_some()
            || snapshot.expired_base_units != 0
            || snapshot.venue_dust_closed_base_units != 0
            || snapshot.market_resolved_canceled_base_units != 0
            || snapshot.canceled_base_units != 0
            || accepted_base_units
                != snapshot
                    .matched_base_units
                    .checked_add(active_base_units)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
            || snapshot.accepted_quote_units
                != snapshot
                    .matched_quote_units
                    .checked_add(snapshot.open_quote_units)
                    .and_then(|value| value.checked_add(snapshot.released_quote_units))
                    .and_then(|value| value.checked_add(snapshot.canceled_released_quote_units))
                    .and_then(|value| value.checked_add(snapshot.expired_released_quote_units))
                    .and_then(|value| value.checked_add(snapshot.venue_dust_released_quote_units))
                    .and_then(|value| {
                        value.checked_add(snapshot.market_resolved_released_quote_units)
                    })
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
        {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        let effect_identity = market_resolution_order_identity(
            resolution_event_id,
            platform_order_id,
            snapshot.pending_maker_depletion_base_units,
            snapshot.open_base_units,
            snapshot.open_quote_units,
        );
        let mut next_account = self.account.clone();
        let account_delta = next_account.apply_market_resolution_encumbrance_release(
            &effect_identity.0,
            &snapshot.condition_id,
            platform_effective_at_utc_ms,
            decimal_from_micro_units_v1(snapshot.open_quote_units)?,
        )?;
        if snapshot.open_base_units > 0 {
            self.account_overlay
                .subtract_quantity(
                    &snapshot.condition_id,
                    &snapshot.token_id,
                    BookSide::Bid,
                    snapshot.source_price_pusd_per_share,
                    decimal_from_micro_units_v1(snapshot.open_base_units)?,
                )
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        }
        if let SimOrderKindStateV1::Gtd { expires_at_utc_ms } = snapshot.kind {
            self.gtd_deadlines
                .remove(&(expires_at_utc_ms, platform_order_id.to_owned()));
        }
        let order = self
            .orders
            .get_mut(platform_order_id)
            .expect("已验证订单仍存在");
        order.market_resolved_canceled_base_units = active_base_units;
        order.market_resolved_released_quote_units = snapshot.open_quote_units;
        order.market_resolved_at_utc_ms = Some(platform_effective_at_utc_ms);
        order.market_resolution_effect_identity = Some(effect_identity);
        order.terminalized_pending_maker_depletion_evidence =
            order.pending_maker_depletion_evidence.take();
        order.pending_maker_depletion_base_units = 0;
        order.open_base_units = 0;
        order.open_quote_units = 0;
        order.last_updated_at_utc_ms = platform_effective_at_utc_ms;
        order.terminal_reason = Some(SimOrderTerminalReasonV1::MarketResolvedCanceled);
        self.account = next_account;
        Ok(MarketResolutionOrderEffectV1 {
            platform_order_id: snapshot.platform_order_id,
            condition_id: snapshot.condition_id,
            token_id: snapshot.token_id,
            canceled_base_units: active_base_units,
            released_open_quote_units: snapshot.open_quote_units,
            account_delta,
        })
    }

    pub(crate) fn release_initialized_matching_buffers_through(
        &mut self,
        directory: &SessionMarketDirectoryV1,
        closed_through_utc_ms: i64,
    ) -> Result<(), MatchingStateErrorV1> {
        for market in directory.markets() {
            let condition_id = &market.market().condition_id.0;
            let end_at = market.market().window.end_at_utc_ms;
            if closed_through_utc_ms >= end_at
                && !self.matching_closed_markets.contains_key(condition_id)
                && self
                    .queues
                    .release_market_matching_state_if_initialized(condition_id)?
            {
                self.matching_closed_markets
                    .insert(condition_id.clone(), end_at);
            }
        }
        Ok(())
    }

    pub(crate) fn apply_authorized_market_events(
        &mut self,
        directory: &SessionMarketDirectoryV1,
        bucket_at_utc_ms: i64,
        events: &[ExecutionMarketEventV3],
        finality_policy: &TradeFinalityPolicyV1,
        lifecycle_account_rule: &LifecycleAccountRuleV1,
        qualified_resolution_event_ids: &BTreeSet<String>,
    ) -> Result<AuthorizedMarketEventEffectsV1, MatchingStateErrorV1> {
        self.release_initialized_matching_buffers_through(directory, bucket_at_utc_ms)?;
        let mut effects = AuthorizedMarketEventEffectsV1::default();
        for event in events {
            let should_invalidate_resolution =
                if let ExecutionMarketEventPayloadV3::QualityBoundary(boundary) = &event.payload
                    && matches!(boundary.scope, QualityScopeV1::Market)
                    && boundary.current_quality == MarketDataQuality::Invalid
                {
                    self.market_resolutions
                        .get(&event.market.condition_id.0)
                        .is_some_and(|obligation| {
                            obligation.redeemed_at_utc_ms.is_none()
                                && obligation.invalidated_by_quality_reason.is_none()
                        })
                } else {
                    false
                };
            if should_invalidate_resolution {
                let ExecutionMarketEventPayloadV3::QualityBoundary(boundary) = &event.payload
                else {
                    unreachable!("市场判定隔离门只可能由质量边界触发")
                };
                self.market_resolutions_mut()
                    .get_mut(&event.market.condition_id.0)
                    .expect("待隔离市场判定义务必须仍存在")
                    .invalidated_by_quality_reason = Some(boundary.reason_code.clone());
            }
            let queue_effects = if self
                .matching_closed_markets
                .contains_key(&event.market.condition_id.0)
            {
                QueueEventEffectsV1::default()
            } else {
                self.queues.apply_authorized_event(event)?
            };
            let mut event_fill_ids = Vec::new();
            for depletion in queue_effects.maker_depletions {
                if let Some(trade_id) =
                    self.apply_maker_queue_depletion(bucket_at_utc_ms, depletion)?
                {
                    event_fill_ids.push(trade_id);
                }
            }
            event_fill_ids.sort();
            for trade_id in event_fill_ids {
                effects
                    .trade_finality_effects
                    .push(self.initialize_fill_finality(finality_policy, &trade_id)?);
            }
            if let Some(candidate) = queue_effects.venue_dust_candidate
                && let Some(effect) = self.apply_venue_dust_terminal(
                    candidate,
                    bucket_at_utc_ms,
                    &format!("DPT_EVENT:{}", event.event_id.0),
                    lifecycle_account_rule,
                )?
            {
                effects.venue_dust_effects.push(effect);
            }
            if qualified_resolution_event_ids.contains(&event.event_id.0) {
                let (order_effects, auto_redeem) =
                    self.apply_qualified_market_resolution(event, bucket_at_utc_ms)?;
                effects
                    .market_resolution_order_effects
                    .extend(order_effects);
                if let Some(auto_redeem) = auto_redeem {
                    effects.settlement_feedbacks.push(auto_redeem);
                }
            }
            let condition_id = &event.market.condition_id.0;
            if bucket_at_utc_ms >= event.market.window.end_at_utc_ms
                && !self.matching_closed_markets.contains_key(condition_id)
                && self
                    .queues
                    .release_market_matching_state_if_initialized(condition_id)?
            {
                self.matching_closed_markets
                    .insert(condition_id.clone(), event.market.window.end_at_utc_ms);
            }
        }
        Ok(effects)
    }

    pub(crate) fn next_gtd_deadline_at_utc_ms(&self) -> Option<i64> {
        self.gtd_deadlines
            .first()
            .map(|(expires_at_utc_ms, _)| *expires_at_utc_ms)
    }

    pub(crate) fn redeemed_market_resolution(
        &self,
        condition_id: &str,
    ) -> Option<&MarketResolutionTerminalV3> {
        self.market_resolutions
            .get(condition_id)
            .filter(|obligation| obligation.redeemed_at_utc_ms.is_some())
            .map(|obligation| &obligation.resolution)
    }

    pub(crate) fn prepare_execution_drive_delta_v2(
        &mut self,
        logical_time_utc_ms: i64,
    ) -> Result<PreparedExecutionDriveDeltaV2<'_>, MatchingStateErrorV1> {
        PreparedExecutionDriveDeltaV2::prepare(self, logical_time_utc_ms)
    }

    pub(crate) fn advance_gtd_deadlines_through(
        &mut self,
        logical_time_utc_ms: i64,
    ) -> Result<Vec<GtdExpirationEffectV1>, MatchingStateErrorV1> {
        let mut order_replacements = BTreeMap::new();
        let mut candidate_account = None;
        let patch = PreparedGtdExpirationPatchV2::prepare(
            self,
            logical_time_utc_ms,
            &mut order_replacements,
            &mut candidate_account,
        )?;
        let effects = patch.effects.clone();
        patch.commit_prevalidated(self);
        for (platform_order_id, order) in order_replacements {
            self.orders.insert(platform_order_id, order);
        }
        if let Some(account) = candidate_account {
            self.account = account;
        }
        Ok(effects)
    }

    pub(crate) fn cancel_order_remainder(
        &mut self,
        platform_order_id: &str,
        canceled_at_utc_ms: i64,
        caller_cancel_effect_identity: &Sha256Hex,
    ) -> Result<CancelOrderStateDispositionV1, MatchingStateErrorV1> {
        let prepared = PreparedExecutionDriveDeltaV2::prepare_without_scheduled_progress(self);
        let (prepared, disposition) = prepared.prepare_caller_cancel_remainder(
            platform_order_id,
            canceled_at_utc_ms,
            caller_cancel_effect_identity,
        )?;
        prepared.into_committable()?.commit();
        Ok(disposition)
    }
    pub(crate) fn commit_new_order(
        &mut self,
        order: SimOrderRecordV1,
        fills: Vec<SimFillRecordV1>,
    ) -> Result<OrderCommitEffectsV1, MatchingStateErrorV1> {
        let PreparedNewOrderCoreV2 {
            candidate_account,
            order,
            fills,
            effects,
        } = prepare_new_order_core_v2(&self.account, &self.orders, &self.fills, order, fills)?;
        if order.open_base_units > 0 {
            self.queues.insert_account_buy_order(
                &order.condition_id,
                &order.token_id,
                order.source_price_pusd_per_share,
                order.platform_order_id.clone(),
                order.open_base_units,
            )?;
            self.account_overlay
                .add_quantity(
                    &order.condition_id,
                    &order.token_id,
                    BookSide::Bid,
                    order.source_price_pusd_per_share,
                    decimal_from_micro_units_v1(order.open_base_units)?,
                )
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        }
        self.account = candidate_account;
        for fill in fills {
            self.fills.insert(fill.trade_id.clone(), fill);
        }
        if let SimOrderKindStateV1::Gtd { expires_at_utc_ms } = order.kind
            && order.open_base_units > 0
        {
            self.gtd_deadlines
                .insert((expires_at_utc_ms, order.platform_order_id.clone()));
        }
        self.orders.insert(order.platform_order_id.clone(), order);
        Ok(effects)
    }

    fn apply_venue_dust_terminal(
        &mut self,
        candidate: VenueDustQueueCandidateV1,
        platform_event_at_utc_ms: i64,
        trigger_identity: &str,
        lifecycle_account_rule: &LifecycleAccountRuleV1,
    ) -> Result<Option<VenueDustEffectV1>, MatchingStateErrorV1> {
        let upper_exclusive_base_units =
            venue_dust_upper_exclusive_base_units_v2(lifecycle_account_rule)?;
        let snapshot = self
            .orders
            .get(&candidate.platform_order_id)
            .cloned()
            .ok_or_else(|| {
                MatchingStateErrorV1::AccountOrderConflict(candidate.platform_order_id.clone())
            })?;
        if candidate.price_pusd_per_share != snapshot.source_price_pusd_per_share
            || candidate.remaining_base_units != snapshot.open_base_units
            || self
                .queues
                .venue_dust_candidate_for_order(
                    &snapshot.condition_id,
                    &snapshot.token_id,
                    snapshot.source_price_pusd_per_share,
                    &candidate.platform_order_id,
                )
                .as_ref()
                != Some(&candidate)
        {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        let PreparedVenueDustCoreV2 {
            candidate_account,
            order,
            effect,
        } = prepare_venue_dust_core_v2(
            self.account.clone(),
            snapshot,
            &candidate,
            platform_event_at_utc_ms,
            trigger_identity,
            upper_exclusive_base_units,
        )?;
        let Some(effect) = effect else {
            return Ok(None);
        };
        self.account_overlay
            .subtract_quantity(
                &order.condition_id,
                &order.token_id,
                BookSide::Bid,
                order.source_price_pusd_per_share,
                decimal_from_micro_units_v1(effect.closed_base_units)?,
            )
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        self.queues.remove_account_order(&order.platform_order_id);
        if let SimOrderKindStateV1::Gtd { expires_at_utc_ms } = order.kind {
            self.gtd_deadlines
                .remove(&(expires_at_utc_ms, order.platform_order_id.clone()));
        }
        self.orders.insert(order.platform_order_id.clone(), order);
        self.account = candidate_account;
        Ok(Some(effect))
    }

    fn apply_maker_queue_depletion(
        &mut self,
        bucket_at_utc_ms: i64,
        depletion: MakerQueueDepletionV1,
    ) -> Result<Option<String>, MatchingStateErrorV1> {
        let order = self
            .orders
            .get_mut(&depletion.platform_order_id)
            .ok_or_else(|| {
                MatchingStateErrorV1::AccountOrderConflict(depletion.platform_order_id.clone())
            })?;
        if !matches!(
            order.kind,
            SimOrderKindStateV1::Gtc | SimOrderKindStateV1::Gtd { .. }
        ) || depletion.price_pusd_per_share != order.source_price_pusd_per_share
            || depletion.base_units > order.open_base_units
            || (order.pending_maker_depletion_base_units == 0)
                != order.pending_maker_depletion_evidence.is_none()
            || order.canceled_base_units != 0
            || order.expired_base_units != 0
            || order.venue_dust_closed_base_units != 0
            || order.venue_dust_released_quote_units != 0
            || order.venue_dust_at_utc_ms.is_some()
            || order.venue_dust_identity.is_some()
            || order.market_resolved_canceled_base_units != 0
            || order.market_resolved_released_quote_units != 0
            || order.market_resolved_at_utc_ms.is_some()
            || order.market_resolution_effect_identity.is_some()
            || order
                .terminalized_pending_maker_depletion_evidence
                .is_some()
            || order.terminal_reason.is_some()
        {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        let accepted_base = order
            .accepted_base_units
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let pending_end_before = order
            .matched_base_units
            .checked_add(order.pending_maker_depletion_base_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let maker_fill_base_units = order
            .pending_maker_depletion_base_units
            .checked_add(depletion.base_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let pending_end_after = pending_end_before
            .checked_add(depletion.base_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        if accepted_base
            != pending_end_before
                .checked_add(order.open_base_units)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
        {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        let allocated_quote_before = cumulative_segment_quote_units_v1(
            accepted_base,
            order.accepted_quote_units,
            order.matched_base_units,
        )?;
        let allocated_quote_at_pending_end = cumulative_segment_quote_units_v1(
            accepted_base,
            order.accepted_quote_units,
            pending_end_before,
        )?;
        if allocated_quote_at_pending_end != allocated_quote_before {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        let allocated_quote_after = cumulative_segment_quote_units_v1(
            accepted_base,
            order.accepted_quote_units,
            pending_end_after,
        )?;
        let quote_delta = allocated_quote_after
            .checked_sub(allocated_quote_before)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let next_open_base = order
            .open_base_units
            .checked_sub(depletion.base_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let evidence_sha256 = pending_maker_depletion_evidence_sha256(
            order
                .pending_maker_depletion_evidence
                .as_ref()
                .map(|evidence| &evidence.sha256),
            &order.platform_order_id,
            order.matched_base_units,
            &depletion.source_event_id,
            depletion.base_units,
        );
        let evidence_event_count = match &order.pending_maker_depletion_evidence {
            Some(evidence) => evidence
                .event_count
                .checked_add(1)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?,
            None => 1,
        };
        let evidence = MakerDepletionEvidenceV1 {
            sha256: evidence_sha256,
            event_count: evidence_event_count,
            first_source_event_id: order.pending_maker_depletion_evidence.as_ref().map_or_else(
                || depletion.source_event_id.clone(),
                |evidence| evidence.first_source_event_id.clone(),
            ),
            last_source_event_id: depletion.source_event_id.clone(),
        };
        self.account_overlay
            .subtract_quantity(
                &order.condition_id,
                &order.token_id,
                BookSide::Bid,
                order.source_price_pusd_per_share,
                decimal_from_micro_units_v1(depletion.base_units)?,
            )
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        order.open_base_units = next_open_base;
        order.last_updated_at_utc_ms = bucket_at_utc_ms;
        if quote_delta == 0 {
            order.pending_maker_depletion_base_units = order
                .pending_maker_depletion_base_units
                .checked_add(depletion.base_units)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            order.pending_maker_depletion_evidence = Some(evidence);
            return Ok(None);
        }

        let next_matched_quote = order
            .matched_quote_units
            .checked_add(quote_delta)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let next_open_quote = order
            .open_quote_units
            .checked_sub(quote_delta)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        if order.accepted_quote_units
            != next_matched_quote
                .checked_add(next_open_quote)
                .and_then(|value| value.checked_add(order.released_quote_units))
                .and_then(|value| value.checked_add(order.canceled_released_quote_units))
                .and_then(|value| value.checked_add(order.expired_released_quote_units))
                .and_then(|value| value.checked_add(order.venue_dust_released_quote_units))
                .and_then(|value| value.checked_add(order.market_resolved_released_quote_units))
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
        {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        let source_identity = evidence.sha256.0.clone();
        let trade_id = stable_fill_id(
            "MAKER_INFERRED",
            &order.platform_order_id,
            &source_identity,
            order.matched_base_units,
        );
        if self.fills.contains_key(&trade_id) {
            return Err(MatchingStateErrorV1::AccountOrderConflict(trade_id));
        }
        order.matched_base_units = pending_end_after;
        order.pending_maker_depletion_base_units = 0;
        order.pending_maker_depletion_evidence = None;
        order.matched_quote_units = next_matched_quote;
        order.open_quote_units = next_open_quote;
        let completed_gtd_deadline = match order.kind {
            SimOrderKindStateV1::Gtd { expires_at_utc_ms } if next_open_base == 0 => {
                Some((expires_at_utc_ms, order.platform_order_id.clone()))
            }
            SimOrderKindStateV1::Gtc
            | SimOrderKindStateV1::Gtd { .. }
            | SimOrderKindStateV1::Fok
            | SimOrderKindStateV1::Fak => None,
        };
        self.fills.insert(
            trade_id.clone(),
            SimFillRecordV1 {
                trade_id: trade_id.clone(),
                platform_order_id: order.platform_order_id.clone(),
                condition_id: order.condition_id.clone(),
                token_id: order.token_id.clone(),
                matched_at_utc_ms: bucket_at_utc_ms,
                liquidity_role: SimLiquidityRoleV1::MakerInferred,
                source_identity,
                maker_depletion_evidence: Some(evidence),
                price_pusd_per_share: depletion.price_pusd_per_share,
                base_units: maker_fill_base_units,
                quote_units: quote_delta,
                immediate_taker_fee_units: 0,
                finality_path: None,
                finality_path_ordinal: None,
                finality_state: None,
                economic_application_delta: None,
            },
        );
        if let Some(deadline) = completed_gtd_deadline {
            self.gtd_deadlines.remove(&deadline);
        }
        Ok(Some(trade_id))
    }
}

pub(crate) fn stable_fill_id(
    role: &str,
    platform_order_id: &str,
    source_identity: &str,
    cumulative_base_before: u128,
) -> String {
    let mut hash = Sha256::new();
    hash.update(b"SIM_FILL_ID_V1\0");
    hash.update(role.as_bytes());
    hash.update(platform_order_id.as_bytes());
    hash.update(source_identity.as_bytes());
    hash.update(cumulative_base_before.to_be_bytes());
    format!("{:x}", hash.finalize())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TakerTargetV1 {
    LimitBase {
        total_base_units: u128,
        total_quote_units: u128,
    },
    Quote(u128),
}

impl TakerTargetV1 {
    fn complete(self, matched_base_units: u128, used_quote_units: u128) -> bool {
        match self {
            Self::LimitBase {
                total_base_units, ..
            } => matched_base_units >= total_base_units,
            Self::Quote(target) => used_quote_units >= target,
        }
    }
}

fn consume_queue_head(
    queue: &mut PriceQueueV1,
    source_event_id: &str,
    price: Decimal,
    mut remaining_base_units: u128,
) -> Result<Vec<MakerQueueDepletionV1>, MatchingStateErrorV1> {
    let mut maker_depletions = Vec::new();
    while remaining_base_units > 0 {
        let segment = queue
            .segments
            .front_mut()
            .ok_or(MatchingStateErrorV1::QueueUnderflow)?;
        let available = segment.remaining_base_units()?;
        let consumed = available.min(remaining_base_units);
        match segment {
            QueueSegmentV1::External(external) => {
                external.consumed_base_units =
                    external
                        .consumed_base_units
                        .checked_add(consumed)
                        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            }
            QueueSegmentV1::Account(account) => {
                account.remaining_base_units =
                    account
                        .remaining_base_units
                        .checked_sub(consumed)
                        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                maker_depletions.push(MakerQueueDepletionV1 {
                    source_event_id: source_event_id.to_owned(),
                    platform_order_id: account.platform_order_id.clone(),
                    price_pusd_per_share: price,
                    base_units: consumed,
                });
            }
        }
        remaining_base_units -= consumed;
        if segment.remaining_base_units()? == 0 {
            queue.segments.pop_front();
        }
    }
    Ok(maker_depletions)
}

fn pending_maker_depletion_evidence_sha256(
    previous: Option<&Sha256Hex>,
    platform_order_id: &str,
    matched_base_units: u128,
    source_event_id: &str,
    base_units: u128,
) -> Sha256Hex {
    let mut hash = Sha256::new();
    hash.update(PENDING_MAKER_DEPLETION_ID_DOMAIN_V1);
    hash.update((platform_order_id.len() as u64).to_be_bytes());
    hash.update(platform_order_id.as_bytes());
    hash.update(matched_base_units.to_be_bytes());
    match previous {
        Some(previous) => {
            hash.update([1]);
            hash.update(previous.0.as_bytes());
        }
        None => hash.update([0]),
    }
    hash.update((source_event_id.len() as u64).to_be_bytes());
    hash.update(source_event_id.as_bytes());
    hash.update(base_units.to_be_bytes());
    Sha256Hex(format!("{:x}", hash.finalize()))
}

fn venue_dust_identity(
    trigger_identity: &str,
    platform_order_id: &str,
    matched_base_units: u128,
    open_base_units: u128,
    open_quote_units: u128,
) -> Sha256Hex {
    let mut hash = Sha256::new();
    hash.update(VENUE_DUST_ID_DOMAIN_V1);
    for value in [trigger_identity, platform_order_id] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hash.update(matched_base_units.to_be_bytes());
    hash.update(open_base_units.to_be_bytes());
    hash.update(open_quote_units.to_be_bytes());
    Sha256Hex(format!("{:x}", hash.finalize()))
}

fn market_resolution_order_identity(
    resolution_event_id: &str,
    platform_order_id: &str,
    pending_base_units: u128,
    open_base_units: u128,
    open_quote_units: u128,
) -> Sha256Hex {
    let mut hash = Sha256::new();
    hash.update(MARKET_RESOLUTION_ORDER_ID_DOMAIN_V1);
    for value in [resolution_event_id, platform_order_id] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hash.update(pending_base_units.to_be_bytes());
    hash.update(open_base_units.to_be_bytes());
    hash.update(open_quote_units.to_be_bytes());
    Sha256Hex(format!("{:x}", hash.finalize()))
}

fn gtd_expiration_identity(
    platform_order_id: &str,
    expires_at_utc_ms: i64,
    pending_base_units: u128,
    open_base_units: u128,
    released_open_quote_units: u128,
    pending_evidence: Option<&MakerDepletionEvidenceV1>,
) -> Sha256Hex {
    let mut hash = Sha256::new();
    hash.update(b"SIM_GTD_EXPIRATION_V1\0");
    hash.update((platform_order_id.len() as u64).to_be_bytes());
    hash.update(platform_order_id.as_bytes());
    hash.update(expires_at_utc_ms.to_be_bytes());
    hash.update(pending_base_units.to_be_bytes());
    hash.update(open_base_units.to_be_bytes());
    hash.update(released_open_quote_units.to_be_bytes());
    match pending_evidence {
        Some(evidence) => {
            hash.update([1]);
            hash.update(evidence.sha256.0.as_bytes());
            hash.update(evidence.event_count.to_be_bytes());
        }
        None => hash.update([0]),
    }
    Sha256Hex(format!("{:x}", hash.finalize()))
}

fn external_segment(
    event_id: &str,
    side: BookSide,
    price: Decimal,
    ordinal: u64,
    base_units: u128,
) -> Result<ExternalSyntheticMakerSegmentV1, MatchingStateErrorV1> {
    let mut hash = Sha256::new();
    hash.update(EXTERNAL_SEGMENT_ID_DOMAIN_V1);
    hash.update(event_id.as_bytes());
    hash.update([match side {
        BookSide::Bid => 0,
        BookSide::Ask => 1,
    }]);
    hash.update(price.normalize().to_string().as_bytes());
    hash.update(ordinal.to_be_bytes());
    let segment_id = Sha256Hex(format!("{:x}", hash.finalize()));
    Ok(ExternalSyntheticMakerSegmentV1 {
        segment_id,
        total_base_units: base_units,
        total_quote_units: segment_quote_units_v1(base_units, price)?,
        consumed_base_units: 0,
    })
}

fn parse_decimal(value: &str) -> Result<Decimal, MatchingStateErrorV1> {
    parse_canonical_decimal(
        &contracts::ExactDecimal(value.to_owned()),
        "sim execution queue",
    )
    .map_err(|error| MatchingStateErrorV1::Decode(error.to_string()))
}

fn recovered_order_graph_error(identity: &str) -> MatchingStateErrorV1 {
    MatchingStateErrorV1::AccountOrderConflict(format!("恢复订单经济图不守恒：{identity}"))
}

fn negative_units(value: u128) -> Result<i128, MatchingMathErrorV1> {
    i128::try_from(value)
        .map(|value| -value)
        .map_err(|_| MatchingMathErrorV1::ArithmeticOverflow)
}

fn exact_signed_micro_units_v1(value: i128) -> Result<ExactDecimal, MatchingMathErrorV1> {
    Decimal::try_from_i128_with_scale(value, MICRO_SCALE)
        .map(|value| ExactDecimal(value.normalize().to_string()))
        .map_err(|_| MatchingMathErrorV1::ArithmeticOverflow)
}

fn recovered_fill_current_step_at(fill: &SimFillRecordV1) -> Result<i64, MatchingStateErrorV1> {
    let path = fill.finality_path.as_ref().ok_or_else(|| {
        MatchingStateErrorV1::Finality(format!("恢复成交缺少最终性路径：{}", fill.trade_id))
    })?;
    let ordinal = fill.finality_path_ordinal.ok_or_else(|| {
        MatchingStateErrorV1::Finality(format!("恢复成交缺少最终性序号：{}", fill.trade_id))
    })?;
    let step = usize::try_from(ordinal)
        .ok()
        .and_then(|ordinal| path.steps().get(ordinal))
        .ok_or_else(|| {
            MatchingStateErrorV1::Finality(format!("恢复成交最终性序号超出路径：{}", fill.trade_id))
        })?;
    fill.matched_at_utc_ms
        .checked_add(step.offset_ms())
        .ok_or_else(|| {
            MatchingStateErrorV1::Finality(format!("恢复成交当前最终性时点溢出：{}", fill.trade_id))
        })
}

fn expected_failure_balance_delta(
    fill: &SimFillRecordV1,
    compensated: bool,
) -> Result<PlatformAccountBalanceDeltaV1, MatchingStateErrorV1> {
    let total_quote_and_fee = fill
        .quote_units
        .checked_add(fill.immediate_taker_fee_units)
        .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?;
    let positive_total =
        i128::try_from(total_quote_and_fee).map_err(|_| MatchingMathErrorV1::ArithmeticOverflow)?;
    let zero = || exact_signed_micro_units_v1(0);
    if compensated {
        Ok(PlatformAccountBalanceDeltaV1 {
            cash_pusd_delta: exact_signed_micro_units_v1(positive_total)?,
            available_pusd_delta: exact_signed_micro_units_v1(positive_total)?,
            principal_encumbrance_pusd_delta: zero()?,
            immediate_taker_fee_encumbrance_pusd_delta: zero()?,
            outcome_balance_deltas: vec![PlatformOutcomeBalanceDeltaV1 {
                token_id: TokenId(fill.token_id.clone()),
                balance_shares_delta: exact_signed_micro_units_v1(negative_units(
                    fill.base_units,
                )?)?,
            }],
            maker_rebate_receivable_pusd_delta: zero()?,
            taker_rebate_receivable_pusd_delta: zero()?,
            maker_rebate_locked_remainder_pusd_delta: zero()?,
            taker_rebate_locked_remainder_pusd_delta: zero()?,
        })
    } else {
        Ok(PlatformAccountBalanceDeltaV1 {
            cash_pusd_delta: zero()?,
            available_pusd_delta: exact_signed_micro_units_v1(positive_total)?,
            principal_encumbrance_pusd_delta: exact_signed_micro_units_v1(negative_units(
                fill.quote_units,
            )?)?,
            immediate_taker_fee_encumbrance_pusd_delta: exact_signed_micro_units_v1(
                negative_units(fill.immediate_taker_fee_units)?,
            )?,
            outcome_balance_deltas: Vec::new(),
            maker_rebate_receivable_pusd_delta: zero()?,
            taker_rebate_receivable_pusd_delta: zero()?,
            maker_rebate_locked_remainder_pusd_delta: zero()?,
            taker_rebate_locked_remainder_pusd_delta: zero()?,
        })
    }
}

fn expected_evicted_economic_balance_delta(
    witness: &TerminalFillEvictionWitnessV1,
) -> Result<PlatformAccountBalanceDeltaV1, MatchingStateErrorV1> {
    let total_quote_and_fee = witness
        .quote_units
        .checked_add(witness.rebate_fill.immediate_taker_fee_units)
        .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?;
    let zero = || exact_signed_micro_units_v1(0);
    Ok(PlatformAccountBalanceDeltaV1 {
        cash_pusd_delta: exact_signed_micro_units_v1(negative_units(total_quote_and_fee)?)?,
        available_pusd_delta: zero()?,
        principal_encumbrance_pusd_delta: exact_signed_micro_units_v1(negative_units(
            witness.quote_units,
        )?)?,
        immediate_taker_fee_encumbrance_pusd_delta: exact_signed_micro_units_v1(negative_units(
            witness.rebate_fill.immediate_taker_fee_units,
        )?)?,
        outcome_balance_deltas: vec![PlatformOutcomeBalanceDeltaV1 {
            token_id: TokenId(witness.token_id.clone()),
            balance_shares_delta: exact_signed_micro_units_v1(
                i128::try_from(witness.rebate_fill.base_units)
                    .map_err(|_| MatchingMathErrorV1::ArithmeticOverflow)?,
            )?,
        }],
        maker_rebate_receivable_pusd_delta: zero()?,
        taker_rebate_receivable_pusd_delta: zero()?,
        maker_rebate_locked_remainder_pusd_delta: zero()?,
        taker_rebate_locked_remainder_pusd_delta: zero()?,
    })
}

fn expected_evicted_failure_balance_delta(
    witness: &TerminalFillEvictionWitnessV1,
    compensated: bool,
) -> Result<PlatformAccountBalanceDeltaV1, MatchingStateErrorV1> {
    let total_quote_and_fee = witness
        .quote_units
        .checked_add(witness.rebate_fill.immediate_taker_fee_units)
        .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?;
    let positive_total =
        i128::try_from(total_quote_and_fee).map_err(|_| MatchingMathErrorV1::ArithmeticOverflow)?;
    let zero = || exact_signed_micro_units_v1(0);
    if compensated {
        Ok(PlatformAccountBalanceDeltaV1 {
            cash_pusd_delta: exact_signed_micro_units_v1(positive_total)?,
            available_pusd_delta: exact_signed_micro_units_v1(positive_total)?,
            principal_encumbrance_pusd_delta: zero()?,
            immediate_taker_fee_encumbrance_pusd_delta: zero()?,
            outcome_balance_deltas: vec![PlatformOutcomeBalanceDeltaV1 {
                token_id: TokenId(witness.token_id.clone()),
                balance_shares_delta: exact_signed_micro_units_v1(negative_units(
                    witness.rebate_fill.base_units,
                )?)?,
            }],
            maker_rebate_receivable_pusd_delta: zero()?,
            taker_rebate_receivable_pusd_delta: zero()?,
            maker_rebate_locked_remainder_pusd_delta: zero()?,
            taker_rebate_locked_remainder_pusd_delta: zero()?,
        })
    } else {
        Ok(PlatformAccountBalanceDeltaV1 {
            cash_pusd_delta: zero()?,
            available_pusd_delta: exact_signed_micro_units_v1(positive_total)?,
            principal_encumbrance_pusd_delta: exact_signed_micro_units_v1(negative_units(
                witness.quote_units,
            )?)?,
            immediate_taker_fee_encumbrance_pusd_delta: exact_signed_micro_units_v1(
                negative_units(witness.rebate_fill.immediate_taker_fee_units)?,
            )?,
            outcome_balance_deltas: Vec::new(),
            maker_rebate_receivable_pusd_delta: zero()?,
            taker_rebate_receivable_pusd_delta: zero()?,
            maker_rebate_locked_remainder_pusd_delta: zero()?,
            taker_rebate_locked_remainder_pusd_delta: zero()?,
        })
    }
}

pub(crate) fn micro_units_v1(value: Decimal) -> Result<u128, MatchingMathErrorV1> {
    if value.is_sign_negative() {
        return Err(MatchingMathErrorV1::NegativeAmount);
    }
    let mantissa =
        u128::try_from(value.mantissa()).map_err(|_| MatchingMathErrorV1::ArithmeticOverflow)?;
    if value.scale() <= MICRO_SCALE {
        mantissa
            .checked_mul(10_u128.pow(MICRO_SCALE - value.scale()))
            .ok_or(MatchingMathErrorV1::ArithmeticOverflow)
    } else {
        Ok(mantissa / 10_u128.pow(value.scale() - MICRO_SCALE))
    }
}

pub(crate) fn decimal_from_micro_units_v1(value: u128) -> Result<Decimal, MatchingMathErrorV1> {
    decimal_from_scaled_units(value, MICRO_SCALE)
}

pub(crate) fn segment_quote_units_v1(
    base_units: u128,
    price_pusd_per_share: Decimal,
) -> Result<u128, MatchingMathErrorV1> {
    if price_pusd_per_share <= Decimal::ZERO || price_pusd_per_share >= Decimal::ONE {
        return Err(MatchingMathErrorV1::InvalidPrice);
    }
    let price_mantissa = u128::try_from(price_pusd_per_share.mantissa())
        .map_err(|_| MatchingMathErrorV1::ArithmeticOverflow)?;
    let denominator = 10_u128
        .checked_pow(price_pusd_per_share.scale())
        .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?;
    u128_from_u256((U256::from(base_units) * U256::from(price_mantissa)) / U256::from(denominator))
}

pub(crate) fn cumulative_segment_quote_units_v1(
    total_base_units: u128,
    total_quote_units: u128,
    cumulative_base_units: u128,
) -> Result<u128, MatchingMathErrorV1> {
    if total_base_units == 0 || cumulative_base_units > total_base_units {
        return Err(MatchingMathErrorV1::InvalidSegment);
    }
    u128_from_u256(
        (U256::from(cumulative_base_units) * U256::from(total_quote_units))
            / U256::from(total_base_units),
    )
}

pub(crate) fn maximum_base_for_quote_budget_v1(
    total_base_units: u128,
    total_quote_units: u128,
    consumed_base_units: u128,
    quote_budget_units: u128,
) -> Result<Option<(u128, u128)>, MatchingMathErrorV1> {
    if total_base_units == 0 || consumed_base_units > total_base_units {
        return Err(MatchingMathErrorV1::InvalidSegment);
    }
    if total_quote_units == 0 || quote_budget_units == 0 || consumed_base_units == total_base_units
    {
        return Ok(None);
    }
    let consumed_quote = cumulative_segment_quote_units_v1(
        total_base_units,
        total_quote_units,
        consumed_base_units,
    )?;
    let remaining_quote = total_quote_units
        .checked_sub(consumed_quote)
        .ok_or(MatchingMathErrorV1::InvalidSegment)?;
    let next_cumulative_base = if quote_budget_units >= remaining_quote {
        total_base_units
    } else {
        let target_quote = consumed_quote
            .checked_add(quote_budget_units)
            .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?;
        let numerator = (U256::from(
            target_quote
                .checked_add(1)
                .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?,
        ) * U256::from(total_base_units))
        .checked_sub(U256::from(1_u8))
        .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?;
        u128_from_u256(numerator / U256::from(total_quote_units))?
    };
    let delta_base = next_cumulative_base
        .checked_sub(consumed_base_units)
        .ok_or(MatchingMathErrorV1::InvalidSegment)?;
    let next_quote = cumulative_segment_quote_units_v1(
        total_base_units,
        total_quote_units,
        next_cumulative_base,
    )?;
    let delta_quote = next_quote
        .checked_sub(consumed_quote)
        .ok_or(MatchingMathErrorV1::InvalidSegment)?;
    if delta_base == 0 || delta_quote == 0 || delta_quote > quote_budget_units {
        return Ok(None);
    }
    Ok(Some((delta_base, delta_quote)))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn maximum_base_for_limit_order_segment_v1(
    external_total_base_units: u128,
    external_total_quote_units: u128,
    external_consumed_base_units: u128,
    maximum_additional_base_units: u128,
    order_total_base_units: u128,
    order_total_quote_units: u128,
    order_matched_base_units: u128,
    order_used_quote_units: u128,
) -> Result<Option<(u128, u128)>, MatchingMathErrorV1> {
    if external_total_base_units == 0
        || order_total_base_units == 0
        || external_consumed_base_units > external_total_base_units
        || order_matched_base_units > order_total_base_units
        || maximum_additional_base_units > external_total_base_units - external_consumed_base_units
        || maximum_additional_base_units > order_total_base_units - order_matched_base_units
    {
        return Err(MatchingMathErrorV1::InvalidSegment);
    }
    if external_total_quote_units == 0
        || order_total_quote_units == 0
        || maximum_additional_base_units == 0
    {
        return Ok(None);
    }

    let order_allocated_quote = cumulative_segment_quote_units_v1(
        order_total_base_units,
        order_total_quote_units,
        order_matched_base_units,
    )?;
    let price_improvement_credit = order_allocated_quote
        .checked_sub(order_used_quote_units)
        .ok_or(MatchingMathErrorV1::InvalidSegment)?;
    let external_remainder = multiplication_remainder_v1(
        external_consumed_base_units,
        external_total_quote_units,
        external_total_base_units,
    )?;
    let order_remainder = multiplication_remainder_v1(
        order_matched_base_units,
        order_total_quote_units,
        order_total_base_units,
    )?;
    let first_positive_external_delta = {
        let missing = external_total_base_units
            .checked_sub(external_remainder)
            .ok_or(MatchingMathErrorV1::InvalidSegment)?;
        let quotient = missing / external_total_quote_units;
        quotient
            .checked_add(u128::from(missing % external_total_quote_units != 0))
            .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?
            .max(1)
    };
    if first_positive_external_delta > maximum_additional_base_units {
        return Ok(None);
    }

    let full = limit_segment_delta_v1(
        external_total_base_units,
        external_total_quote_units,
        external_consumed_base_units,
        order_total_base_units,
        order_total_quote_units,
        order_matched_base_units,
        order_used_quote_units,
        maximum_additional_base_units,
    )?;
    if full.0 > 0 && full.0 <= full.1 {
        return Ok(Some((maximum_additional_base_units, full.0)));
    }
    if full.0 == 0 {
        return Ok(None);
    }

    let external_base = BigInt::from(external_total_base_units);
    let external_quote = BigInt::from(external_total_quote_units);
    let order_base = BigInt::from(order_total_base_units);
    let order_quote = BigInt::from(order_total_quote_units);
    let denominator = &external_base * &order_base;
    let offset = BigInt::from(external_remainder) * &order_base
        - BigInt::from(order_remainder) * &external_base
        - BigInt::from(price_improvement_credit) * &denominator;
    let slope = external_quote * &order_base - order_quote * &external_base;
    let numerator = |delta: u128| &offset + &slope * BigInt::from(delta);

    let lower = first_positive_external_delta;
    let upper = maximum_additional_base_units;
    let ambiguous_bounds = match slope.cmp(&BigInt::zero()) {
        std::cmp::Ordering::Greater => {
            let first_nonnegative =
                first_true_u128_v1(lower, upper, |delta| numerator(delta) >= BigInt::zero());
            let ambiguous_upper = if numerator(upper) < denominator {
                upper
            } else {
                let first_at_least_one =
                    first_true_u128_v1(lower, upper, |delta| numerator(delta) >= denominator);
                if first_at_least_one == lower {
                    return Ok(None);
                }
                first_at_least_one - 1
            };
            if first_nonnegative <= ambiguous_upper {
                Some((first_nonnegative, ambiguous_upper))
            } else {
                None
            }
        }
        std::cmp::Ordering::Less => {
            if numerator(upper) >= denominator {
                return Ok(None);
            }
            let first_below_one =
                first_true_u128_v1(lower, upper, |delta| numerator(delta) < denominator);
            Some((first_below_one, upper))
        }
        std::cmp::Ordering::Equal => {
            if offset < BigInt::zero() {
                return verified_limit_segment_result_v1(
                    external_total_base_units,
                    external_total_quote_units,
                    external_consumed_base_units,
                    order_total_base_units,
                    order_total_quote_units,
                    order_matched_base_units,
                    order_used_quote_units,
                    upper,
                    upper,
                );
            }
            if offset >= denominator {
                return Ok(None);
            }
            Some((lower, upper))
        }
    };

    if let Some((ambiguous_lower, ambiguous_upper)) = ambiguous_bounds {
        if ambiguous_zero_count_v1(
            external_total_base_units,
            external_total_quote_units,
            external_remainder,
            order_total_base_units,
            order_total_quote_units,
            order_remainder,
            price_improvement_credit,
            ambiguous_lower,
            ambiguous_upper,
        )? > BigUint::zero()
        {
            let mut left = ambiguous_lower;
            let mut right = ambiguous_upper;
            while left < right {
                let midpoint = left + (right - left).div_ceil(2);
                let has_feasible_suffix = ambiguous_zero_count_v1(
                    external_total_base_units,
                    external_total_quote_units,
                    external_remainder,
                    order_total_base_units,
                    order_total_quote_units,
                    order_remainder,
                    price_improvement_credit,
                    midpoint,
                    ambiguous_upper,
                )? > BigUint::zero();
                if has_feasible_suffix {
                    left = midpoint;
                } else {
                    right = midpoint - 1;
                }
            }
            return verified_limit_segment_result_v1(
                external_total_base_units,
                external_total_quote_units,
                external_consumed_base_units,
                order_total_base_units,
                order_total_quote_units,
                order_matched_base_units,
                order_used_quote_units,
                left,
                upper,
            );
        }
        if slope > BigInt::zero() && ambiguous_lower > lower {
            return verified_limit_segment_result_v1(
                external_total_base_units,
                external_total_quote_units,
                external_consumed_base_units,
                order_total_base_units,
                order_total_quote_units,
                order_matched_base_units,
                order_used_quote_units,
                ambiguous_lower - 1,
                upper,
            );
        }
    }
    Ok(None)
}

fn multiplication_remainder_v1(
    left: u128,
    right: u128,
    modulus: u128,
) -> Result<u128, MatchingMathErrorV1> {
    if modulus == 0 {
        return Err(MatchingMathErrorV1::InvalidSegment);
    }
    u128_from_u256((U256::from(left) * U256::from(right)) % U256::from(modulus))
}

#[allow(clippy::too_many_arguments)]
fn limit_segment_delta_v1(
    external_total_base_units: u128,
    external_total_quote_units: u128,
    external_consumed_base_units: u128,
    order_total_base_units: u128,
    order_total_quote_units: u128,
    order_matched_base_units: u128,
    order_used_quote_units: u128,
    delta_base_units: u128,
) -> Result<(u128, u128), MatchingMathErrorV1> {
    let external_quote_before = cumulative_segment_quote_units_v1(
        external_total_base_units,
        external_total_quote_units,
        external_consumed_base_units,
    )?;
    let external_quote_after = cumulative_segment_quote_units_v1(
        external_total_base_units,
        external_total_quote_units,
        external_consumed_base_units
            .checked_add(delta_base_units)
            .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?,
    )?;
    let order_quote_after = cumulative_segment_quote_units_v1(
        order_total_base_units,
        order_total_quote_units,
        order_matched_base_units
            .checked_add(delta_base_units)
            .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?,
    )?;
    Ok((
        external_quote_after
            .checked_sub(external_quote_before)
            .ok_or(MatchingMathErrorV1::InvalidSegment)?,
        order_quote_after
            .checked_sub(order_used_quote_units)
            .ok_or(MatchingMathErrorV1::InvalidSegment)?,
    ))
}

#[allow(clippy::too_many_arguments)]
fn verified_limit_segment_result_v1(
    external_total_base_units: u128,
    external_total_quote_units: u128,
    external_consumed_base_units: u128,
    order_total_base_units: u128,
    order_total_quote_units: u128,
    order_matched_base_units: u128,
    order_used_quote_units: u128,
    delta_base_units: u128,
    maximum_additional_base_units: u128,
) -> Result<Option<(u128, u128)>, MatchingMathErrorV1> {
    if delta_base_units == 0 || delta_base_units > maximum_additional_base_units {
        return Err(MatchingMathErrorV1::InvalidSegment);
    }
    let (external_delta_quote, order_quote_allowance) = limit_segment_delta_v1(
        external_total_base_units,
        external_total_quote_units,
        external_consumed_base_units,
        order_total_base_units,
        order_total_quote_units,
        order_matched_base_units,
        order_used_quote_units,
        delta_base_units,
    )?;
    if external_delta_quote == 0 || external_delta_quote > order_quote_allowance {
        return Err(MatchingMathErrorV1::InvalidSegment);
    }
    Ok(Some((delta_base_units, external_delta_quote)))
}

fn first_true_u128_v1(
    mut lower: u128,
    mut upper: u128,
    mut predicate: impl FnMut(u128) -> bool,
) -> u128 {
    while lower < upper {
        let midpoint = lower + (upper - lower) / 2;
        if predicate(midpoint) {
            upper = midpoint;
        } else {
            lower = midpoint + 1;
        }
    }
    lower
}

#[allow(clippy::too_many_arguments)]
fn ambiguous_zero_count_v1(
    external_total_base_units: u128,
    external_total_quote_units: u128,
    external_remainder: u128,
    order_total_base_units: u128,
    order_total_quote_units: u128,
    order_remainder: u128,
    price_improvement_credit: u128,
    lower: u128,
    upper: u128,
) -> Result<BigUint, MatchingMathErrorV1> {
    if lower > upper {
        return Err(MatchingMathErrorV1::InvalidSegment);
    }
    let lower = BigUint::from(lower);
    let upper_exclusive = BigUint::from(upper) + BigUint::one();
    let external_sum = floor_sum_biguint_v1(
        upper_exclusive.clone(),
        BigUint::from(external_total_base_units),
        BigUint::from(external_total_quote_units),
        BigUint::from(external_remainder),
    )? - floor_sum_biguint_v1(
        lower.clone(),
        BigUint::from(external_total_base_units),
        BigUint::from(external_total_quote_units),
        BigUint::from(external_remainder),
    )?;
    let order_sum = floor_sum_biguint_v1(
        upper_exclusive.clone(),
        BigUint::from(order_total_base_units),
        BigUint::from(order_total_quote_units),
        BigUint::from(order_remainder),
    )? - floor_sum_biguint_v1(
        lower.clone(),
        BigUint::from(order_total_base_units),
        BigUint::from(order_total_quote_units),
        BigUint::from(order_remainder),
    )?;
    let length = upper_exclusive - lower;
    let sum_h = BigInt::from(external_sum)
        - BigInt::from(order_sum)
        - BigInt::from(&length * BigUint::from(price_improvement_credit));
    if sum_h < BigInt::zero() || sum_h > BigInt::from(length.clone()) {
        return Err(MatchingMathErrorV1::InvalidSegment);
    }
    let sum_h = BigUint::try_from(sum_h).map_err(|_| MatchingMathErrorV1::InvalidSegment)?;
    Ok(length - sum_h)
}

fn floor_sum_biguint_v1(
    mut count: BigUint,
    mut modulus: BigUint,
    mut slope: BigUint,
    mut offset: BigUint,
) -> Result<BigUint, MatchingMathErrorV1> {
    if modulus.is_zero() {
        return Err(MatchingMathErrorV1::InvalidSegment);
    }
    let mut result = BigUint::zero();
    loop {
        if count.is_zero() {
            return Ok(result);
        }
        if slope >= modulus {
            let quotient = &slope / &modulus;
            result += (&count * (&count - BigUint::one()) / BigUint::from(2_u8)) * quotient;
            slope %= &modulus;
        }
        if offset >= modulus {
            result += &count * (&offset / &modulus);
            offset %= &modulus;
        }
        let maximum = &slope * &count + &offset;
        if maximum < modulus {
            return Ok(result);
        }
        count = &maximum / &modulus;
        offset = maximum % &modulus;
        std::mem::swap(&mut modulus, &mut slope);
    }
}

pub(crate) fn taker_fee_pusd_v1(
    fills: &[(u128, Decimal)],
    rate: Decimal,
    exponent: u32,
) -> Result<Decimal, MatchingMathErrorV1> {
    if rate.is_sign_negative() || exponent == 0 {
        return Err(MatchingMathErrorV1::InvalidSegment);
    }
    let rate = nonnegative_decimal_ratio(rate)?;
    if rate.numerator.is_zero() || fills.iter().all(|(base_units, _)| *base_units == 0) {
        return Ok(Decimal::ZERO);
    }
    if fee_upper_bound_rounds_to_zero(fills, &rate, exponent)? {
        return Ok(Decimal::ZERO);
    }

    let mut total = ExactRatioV1::zero();
    for (base_units, price) in fills {
        total = total.add(exact_taker_fee_contribution_v1(
            *base_units,
            *price,
            &rate,
            exponent,
        )?)?;
    }
    rounded_fee_decimal(total)
}

pub(crate) fn allocate_taker_fee_micro_units_v1(
    fills: &[(&str, u128, Decimal)],
    rate: Decimal,
    exponent: u32,
) -> Result<(u128, Vec<u128>), MatchingMathErrorV1> {
    if rate.is_sign_negative() || exponent == 0 {
        return Err(MatchingMathErrorV1::InvalidSegment);
    }
    let rate = nonnegative_decimal_ratio(rate)?;
    let fee_inputs = fills
        .iter()
        .map(|(_, base_units, price)| (*base_units, *price))
        .collect::<Vec<_>>();
    if rate.numerator.is_zero() || fills.iter().all(|(_, base_units, _)| *base_units == 0) {
        return Ok((0, vec![0; fills.len()]));
    }
    if fee_upper_bound_rounds_to_zero(&fee_inputs, &rate, exponent)? {
        return Ok((0, vec![0; fills.len()]));
    }

    let fee_unit_scale = BigUint::from(10_u32.pow(FEE_SCALE));
    let mut total = ExactRatioV1::zero();
    let mut floor_units = Vec::with_capacity(fills.len());
    let mut remainders = Vec::with_capacity(fills.len());
    for (_, base_units, price) in fills {
        let contribution = exact_taker_fee_contribution_v1(*base_units, *price, &rate, exponent)?;
        let (floor, remainder) =
            (&contribution.numerator * &fee_unit_scale).div_rem(&contribution.denominator);
        floor_units.push(floor);
        remainders.push((remainder, contribution.denominator.clone()));
        total = total.add(contribution)?;
    }

    let rounded_total_units = rounded_fee_units_v1(&total);
    let floor_total = floor_units
        .iter()
        .fold(BigUint::zero(), |sum, units| sum + units);
    if rounded_total_units < floor_total {
        return Err(MatchingMathErrorV1::InvalidSegment);
    }
    let remaining = (&rounded_total_units - floor_total)
        .to_usize()
        .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?;
    if remaining > fills.len() {
        return Err(MatchingMathErrorV1::InvalidSegment);
    }

    let mut remainder_order = (0..fills.len()).collect::<Vec<_>>();
    remainder_order.sort_by(|left, right| {
        let (left_numerator, left_denominator) = &remainders[*left];
        let (right_numerator, right_denominator) = &remainders[*right];
        let left_cross = left_numerator * right_denominator;
        let right_cross = right_numerator * left_denominator;
        right_cross
            .cmp(&left_cross)
            .then_with(|| fills[*left].0.cmp(fills[*right].0))
    });
    for index in remainder_order.into_iter().take(remaining) {
        floor_units[index] += BigUint::one();
    }

    let micro_units_per_fee_unit = 10_u128
        .checked_pow(MICRO_SCALE - FEE_SCALE)
        .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?;
    let total_micro_units = rounded_total_units
        .to_u128()
        .and_then(|units| units.checked_mul(micro_units_per_fee_unit))
        .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?;
    let allocations = floor_units
        .into_iter()
        .map(|units| {
            units
                .to_u128()
                .and_then(|units| units.checked_mul(micro_units_per_fee_unit))
                .ok_or(MatchingMathErrorV1::ArithmeticOverflow)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((total_micro_units, allocations))
}

fn exact_taker_fee_contribution_v1(
    base_units: u128,
    price: Decimal,
    rate: &ExactRatioV1,
    exponent: u32,
) -> Result<ExactRatioV1, MatchingMathErrorV1> {
    if price <= Decimal::ZERO || price >= Decimal::ONE {
        return Err(MatchingMathErrorV1::InvalidPrice);
    }
    if base_units == 0 {
        return Ok(ExactRatioV1::zero());
    }
    let price = positive_decimal_ratio(price)?;
    let curve_numerator = &price.numerator * (&price.denominator - &price.numerator);
    let curve_denominator = &price.denominator * &price.denominator;
    ExactRatioV1::new(
        BigUint::from(base_units) * &rate.numerator * curve_numerator.pow(exponent),
        BigUint::from(1_000_000_u32) * &rate.denominator * curve_denominator.pow(exponent),
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExactRatioV1 {
    numerator: BigUint,
    denominator: BigUint,
}

impl ExactRatioV1 {
    fn zero() -> Self {
        Self {
            numerator: BigUint::zero(),
            denominator: BigUint::one(),
        }
    }

    fn new(numerator: BigUint, denominator: BigUint) -> Result<Self, MatchingMathErrorV1> {
        if denominator.is_zero() {
            return Err(MatchingMathErrorV1::InvalidSegment);
        }
        if numerator.is_zero() {
            return Ok(Self::zero());
        }
        let divisor = numerator.gcd(&denominator);
        Ok(Self {
            numerator: numerator / &divisor,
            denominator: denominator / divisor,
        })
    }

    fn add(self, other: Self) -> Result<Self, MatchingMathErrorV1> {
        let shared = self.denominator.gcd(&other.denominator);
        let left_multiplier = &other.denominator / &shared;
        let right_multiplier = &self.denominator / &shared;
        Self::new(
            self.numerator * &left_multiplier + other.numerator * &right_multiplier,
            self.denominator * left_multiplier,
        )
    }
}

fn nonnegative_decimal_ratio(value: Decimal) -> Result<ExactRatioV1, MatchingMathErrorV1> {
    if value.is_sign_negative() {
        return Err(MatchingMathErrorV1::NegativeAmount);
    }
    decimal_ratio(value)
}

fn positive_decimal_ratio(value: Decimal) -> Result<ExactRatioV1, MatchingMathErrorV1> {
    if value <= Decimal::ZERO {
        return Err(MatchingMathErrorV1::InvalidPrice);
    }
    decimal_ratio(value)
}

fn decimal_ratio(value: Decimal) -> Result<ExactRatioV1, MatchingMathErrorV1> {
    let numerator = u128::try_from(value.mantissa())
        .map(BigUint::from)
        .map_err(|_| MatchingMathErrorV1::ArithmeticOverflow)?;
    ExactRatioV1::new(numerator, BigUint::from(10_u8).pow(value.scale()))
}

fn fee_upper_bound_rounds_to_zero(
    fills: &[(u128, Decimal)],
    rate: &ExactRatioV1,
    exponent: u32,
) -> Result<bool, MatchingMathErrorV1> {
    for (_, price) in fills {
        if *price <= Decimal::ZERO || *price >= Decimal::ONE {
            return Err(MatchingMathErrorV1::InvalidPrice);
        }
    }
    let total_base_units = fills
        .iter()
        .fold(BigUint::zero(), |total, (base_units, _)| {
            total + BigUint::from(*base_units)
        });
    let left = total_base_units * &rate.numerator * BigUint::from(200_000_u32);
    let right_unshifted = BigUint::from(1_000_000_u32) * &rate.denominator;
    exact_less_than_power_of_two_shift(&left, &right_unshifted, u64::from(exponent) * 2)
}

fn exact_less_than_power_of_two_shift(
    left: &BigUint,
    right_unshifted: &BigUint,
    shift: u64,
) -> Result<bool, MatchingMathErrorV1> {
    if left.is_zero() {
        return Ok(true);
    }
    let right_bits = right_unshifted
        .bits()
        .checked_add(shift)
        .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?;
    match left.bits().cmp(&right_bits) {
        std::cmp::Ordering::Less => Ok(true),
        std::cmp::Ordering::Greater => Ok(false),
        std::cmp::Ordering::Equal => {
            let shift =
                usize::try_from(shift).map_err(|_| MatchingMathErrorV1::ArithmeticOverflow)?;
            Ok(left < &(right_unshifted << shift))
        }
    }
}

fn rounded_fee_units_v1(total: &ExactRatioV1) -> BigUint {
    if total.numerator.is_zero() {
        return BigUint::zero();
    }
    let scaled = &total.numerator * BigUint::from(10_u32.pow(FEE_SCALE));
    let (mut rounded_units, remainder) = scaled.div_rem(&total.denominator);
    if (&remainder << 1_usize) >= total.denominator {
        rounded_units += BigUint::one();
    }
    rounded_units
}

fn rounded_fee_decimal(total: ExactRatioV1) -> Result<Decimal, MatchingMathErrorV1> {
    let rounded_units = rounded_fee_units_v1(&total);
    if rounded_units.is_zero() {
        return Ok(Decimal::ZERO);
    }
    let rounded_units = rounded_units
        .to_u128()
        .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?;
    decimal_from_scaled_units(rounded_units, FEE_SCALE)
}

fn decimal_from_scaled_units(
    mut units: u128,
    mut scale: u32,
) -> Result<Decimal, MatchingMathErrorV1> {
    while scale > 0 && units.is_multiple_of(10) {
        units /= 10;
        scale -= 1;
    }
    let units = i128::try_from(units).map_err(|_| MatchingMathErrorV1::ArithmeticOverflow)?;
    Decimal::try_from_i128_with_scale(units, scale)
        .map_err(|_| MatchingMathErrorV1::ArithmeticOverflow)
}

fn u128_from_u256(value: U256) -> Result<u128, MatchingMathErrorV1> {
    u128::try_from(value).map_err(|_| MatchingMathErrorV1::ArithmeticOverflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_json_round_trip_preserves_every_field() {
        use contracts::{
            ACCOUNT_OPENING_ANCHOR_SCHEMA_V1, AccountInstanceKeyV1, AccountOpeningAnchorV1,
            ExactDecimal, PlatformAccountSnapshotV1, SimCallerRoleV1, TokenId,
        };

        let scenario = crate::scenario::scenario_for_sim_tests();
        let directory = SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("测试目录");
        let rules = crate::platform_rules::tests::successful_snapshot_for_sim_tests().await;
        let opening_anchor = AccountOpeningAnchorV1 {
            schema_version: ACCOUNT_OPENING_ANCHOR_SCHEMA_V1.to_owned(),
            anchor_id: "round-trip-anchor".to_owned(),
            anchor_sha256: Sha256Hex("a".repeat(64)),
            account_instance_key: AccountInstanceKeyV1 {
                run_id: "round-trip-run".to_owned(),
                caller_role: SimCallerRoleV1::Paper,
                caller_instance_id: "round-trip-caller".to_owned(),
                account_id: "round-trip-account".to_owned(),
                platform_source: contracts::PlatformSourceV1::Simulated,
            },
            session_id: "round-trip-session".to_owned(),
            initial_scenario_sha256: Sha256Hex("b".repeat(64)),
            initial_market_set_sha256: Sha256Hex("c".repeat(64)),
            initial_account_state_sha256: Sha256Hex("d".repeat(64)),
            initial_platform_account: PlatformAccountSnapshotV1 {
                cash_pusd: ExactDecimal("10".to_owned()),
                available_pusd: ExactDecimal("10".to_owned()),
                principal_encumbrance_pusd: ExactDecimal("0".to_owned()),
                immediate_taker_fee_encumbrance_pusd: ExactDecimal("0".to_owned()),
                outcome_balances: vec![
                    PlatformOutcomeBalanceV1 {
                        token_id: TokenId("1".to_owned()),
                        balance_shares: ExactDecimal("0".to_owned()),
                    },
                    PlatformOutcomeBalanceV1 {
                        token_id: TokenId("2".to_owned()),
                        balance_shares: ExactDecimal("0".to_owned()),
                    },
                ],
                maker_rebate_receivable_pusd: ExactDecimal("0".to_owned()),
                taker_rebate_receivable_pusd: ExactDecimal("0".to_owned()),
                maker_rebate_locked_remainder_pusd: ExactDecimal("0".to_owned()),
                taker_rebate_locked_remainder_pusd: ExactDecimal("0".to_owned()),
            },
        };
        let mut state =
            ExecutionStateV1::new(&directory, &opening_anchor, &rules).expect("测试执行状态");
        let finality_policy = crate::finality::successful_policy_for_tests();
        let condition_id = scenario.market_set().markets()[0]
            .market()
            .condition_id
            .0
            .clone();
        state
            .account
            .apply_order_accepted(
                "order-accepted:order-a",
                &condition_id,
                1_000,
                decimal("0.4"),
                Decimal::ZERO,
            )
            .expect("测试活动订单必须具有同一账户本金占用前驱");
        state
            .queues
            .markets
            .get_mut(&condition_id)
            .expect("测试撮合市场")
            .get_mut("1")
            .expect("测试撮合资产")
            .bids
            .insert(
                decimal("0.4"),
                PriceQueueV1 {
                    segments: VecDeque::from([
                        QueueSegmentV1::External(ExternalSyntheticMakerSegmentV1 {
                            segment_id: Sha256Hex("e".repeat(64)),
                            total_base_units: 1_000_000,
                            total_quote_units: 400_000,
                            consumed_base_units: 250_000,
                        }),
                        QueueSegmentV1::Account(AccountOrderQueueSegmentV1 {
                            platform_order_id: "order-a".to_owned(),
                            remaining_base_units: 1_000_000,
                        }),
                    ]),
                    account_taker_depletion_debt_units: 10,
                },
            );
        state.account_overlay.set_quantity(
            &condition_id,
            "1",
            BookSide::Bid,
            decimal("0.4"),
            Decimal::ONE,
        );
        state.gtd_deadlines = BTreeSet::from([(3_000, "order-a".to_owned())]);
        state.gtd_advanced_through_utc_ms = Some(2_500);
        state.orders.insert(
            "order-a".to_owned(),
            SimOrderRecordV1 {
                platform_order_id: "order-a".to_owned(),
                global_request_id: "request-a".to_owned(),
                item_id: "item-a".to_owned(),
                item_idempotency_identity: "item-idempotency-a".to_owned(),
                condition_id,
                token_id: "1".to_owned(),
                created_at_utc_ms: 1_000,
                last_updated_at_utc_ms: 1_100,
                kind: SimOrderKindStateV1::Gtd {
                    expires_at_utc_ms: 3_000,
                },
                source_price_pusd_per_share: decimal("0.4"),
                accepted_base_units: Some(1_000_000),
                matched_base_units: 0,
                pending_maker_depletion_base_units: 0,
                pending_maker_depletion_evidence: None,
                open_base_units: 1_000_000,
                expired_base_units: 0,
                expired_released_quote_units: 0,
                expired_at_utc_ms: None,
                gtd_expiration_identity: None,
                venue_dust_closed_base_units: 0,
                venue_dust_released_quote_units: 0,
                venue_dust_at_utc_ms: None,
                venue_dust_identity: None,
                market_resolved_canceled_base_units: 0,
                market_resolved_released_quote_units: 0,
                market_resolved_at_utc_ms: None,
                market_resolution_effect_identity: None,
                canceled_base_units: 0,
                canceled_released_quote_units: 0,
                canceled_at_utc_ms: None,
                caller_cancel_effect_identity: None,
                terminalized_pending_maker_depletion_evidence: None,
                accepted_quote_units: 400_000,
                matched_quote_units: 0,
                open_quote_units: 400_000,
                released_quote_units: 0,
                sdk_taker_amount_audit_base_units: None,
                immediate_taker_fee_encumbered_units: 0,
                terminal_reason: None,
            },
        );

        let encoded = serde_json::to_vec(&state).expect("执行状态 JSON 编码");
        let decoded: ExecutionStateV1 =
            serde_json::from_slice(&encoded).expect("执行状态 JSON 解码");

        assert_eq!(decoded, state);
        decoded
            .validate_recovered(
                &directory,
                &opening_anchor,
                rules.as_ref(),
                finality_policy.as_ref(),
                state.gtd_advanced_through_utc_ms,
                &BTreeMap::new(),
            )
            .expect("往返执行状态必须重新绑定原目录与开户锚点");

        let mut rebate_rule_tampered = serde_json::to_value(&state).expect("返佣规则篡改 JSON");
        *rebate_rule_tampered
            .pointer_mut("/rebate/taker_rebate_rate")
            .expect("返佣缓存规则字段") =
            serde_json::to_value(decimal("0.09")).expect("返佣规则十进制 JSON");
        let rebate_rule_tampered: ExecutionStateV1 =
            serde_json::from_value(rebate_rule_tampered).expect("返佣规则篡改仍可解码");
        assert!(matches!(
            rebate_rule_tampered.validate_recovered(
                &directory,
                &opening_anchor,
                rules.as_ref(),
                finality_policy.as_ref(),
                state.gtd_advanced_through_utc_ms,
                &BTreeMap::new(),
            ),
            Err(MatchingStateErrorV1::Rebate(_))
        ));

        let mut tampered = serde_json::to_value(&state).expect("执行状态 JSON 值");
        let markets = tampered
            .get_mut("queues")
            .and_then(|queues| queues.get_mut("markets"))
            .and_then(serde_json::Value::as_object_mut)
            .expect("撮合市场映射");
        let extra = markets.values().next().expect("至少一个测试市场").clone();
        markets.insert("extra-condition".to_owned(), extra);
        let tampered: ExecutionStateV1 =
            serde_json::from_value(tampered).expect("篡改执行状态仍可完成 JSON 解码");
        assert!(matches!(
            tampered.validate_recovered(
                &directory,
                &opening_anchor,
                rules.as_ref(),
                finality_policy.as_ref(),
                state.gtd_advanced_through_utc_ms,
                &BTreeMap::new(),
            ),
            Err(MatchingStateErrorV1::RestoredDirectoryMismatch(
                "撮合队列与会话目录不一致"
            ))
        ));

        let mut overlay_tampered = decoded.clone();
        overlay_tampered.account_overlay.set_quantity(
            &scenario.market_set().markets()[0].market().condition_id.0,
            "1",
            BookSide::Bid,
            decimal("0.4"),
            decimal("1.5"),
        );
        assert!(matches!(
            overlay_tampered.validate_recovered(
                &directory,
                &opening_anchor,
                rules.as_ref(),
                finality_policy.as_ref(),
                state.gtd_advanced_through_utc_ms,
                &BTreeMap::new(),
            ),
            Err(MatchingStateErrorV1::AccountOrderConflict(_))
        ));

        let mut queue_tampered = decoded;
        queue_tampered.queues.remove_account_order("order-a");
        assert!(matches!(
            queue_tampered.validate_recovered(
                &directory,
                &opening_anchor,
                rules.as_ref(),
                finality_policy.as_ref(),
                state.gtd_advanced_through_utc_ms,
                &BTreeMap::new(),
            ),
            Err(MatchingStateErrorV1::AccountOrderConflict(_))
        ));
    }

    fn recovery_fill_account_instance_key() -> contracts::AccountInstanceKeyV1 {
        contracts::AccountInstanceKeyV1 {
            run_id: "recovery-fill-run".to_owned(),
            caller_role: contracts::SimCallerRoleV1::ModelTrain,
            caller_instance_id: "recovery-fill-caller".to_owned(),
            account_id: "recovery-fill-account".to_owned(),
            platform_source: contracts::PlatformSourceV1::Simulated,
        }
    }

    async fn pending_taker_fill_recovery_fixture_with_policy(
        finality_policy: Arc<TradeFinalityPolicyV1>,
    ) -> (
        ExecutionStateV1,
        SessionMarketDirectoryV1,
        contracts::AccountOpeningAnchorV1,
        Arc<SealedProcessRuleSnapshot>,
        Arc<TradeFinalityPolicyV1>,
        Arc<PlatformAccountDeltaV1>,
    ) {
        use contracts::{
            ACCOUNT_OPENING_ANCHOR_SCHEMA_V1, AccountOpeningAnchorV1, ExactDecimal,
            PlatformAccountSnapshotV1,
        };

        let scenario = crate::scenario::scenario_for_sim_tests();
        let directory =
            SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("恢复测试目录");
        let rules = crate::platform_rules::tests::successful_snapshot_for_sim_tests().await;
        let condition_id = scenario.market_set().markets()[0]
            .market()
            .condition_id
            .0
            .clone();
        let outcome_balances = scenario.market_set().markets()[0]
            .assets()
            .iter()
            .map(|asset| PlatformOutcomeBalanceV1 {
                token_id: asset.token_id.clone(),
                balance_shares: ExactDecimal("0".to_owned()),
            })
            .collect();
        let opening_anchor = AccountOpeningAnchorV1 {
            schema_version: ACCOUNT_OPENING_ANCHOR_SCHEMA_V1.to_owned(),
            anchor_id: "recovery-fill-anchor".to_owned(),
            anchor_sha256: Sha256Hex("1".repeat(64)),
            account_instance_key: recovery_fill_account_instance_key(),
            session_id: "recovery-fill-session".to_owned(),
            initial_scenario_sha256: Sha256Hex("2".repeat(64)),
            initial_market_set_sha256: Sha256Hex("3".repeat(64)),
            initial_account_state_sha256: Sha256Hex("4".repeat(64)),
            initial_platform_account: PlatformAccountSnapshotV1 {
                cash_pusd: ExactDecimal("10".to_owned()),
                available_pusd: ExactDecimal("10".to_owned()),
                principal_encumbrance_pusd: ExactDecimal("0".to_owned()),
                immediate_taker_fee_encumbrance_pusd: ExactDecimal("0".to_owned()),
                outcome_balances,
                maker_rebate_receivable_pusd: ExactDecimal("0".to_owned()),
                taker_rebate_receivable_pusd: ExactDecimal("0".to_owned()),
                maker_rebate_locked_remainder_pusd: ExactDecimal("0".to_owned()),
                taker_rebate_locked_remainder_pusd: ExactDecimal("0".to_owned()),
            },
        };
        let mut state =
            ExecutionStateV1::new(&directory, &opening_anchor, &rules).expect("恢复测试执行状态");
        let order = SimOrderRecordV1 {
            platform_order_id: "recovery-fill-order".to_owned(),
            global_request_id: "recovery-fill-request".to_owned(),
            item_id: "recovery-fill-item".to_owned(),
            item_idempotency_identity: "recovery-fill-item-idempotency".to_owned(),
            condition_id: condition_id.clone(),
            token_id: "1".to_owned(),
            created_at_utc_ms: 1_000,
            last_updated_at_utc_ms: 1_000,
            kind: SimOrderKindStateV1::Fok,
            source_price_pusd_per_share: decimal("0.4"),
            accepted_base_units: None,
            matched_base_units: 1_000_000,
            pending_maker_depletion_base_units: 0,
            pending_maker_depletion_evidence: None,
            open_base_units: 0,
            expired_base_units: 0,
            expired_released_quote_units: 0,
            expired_at_utc_ms: None,
            gtd_expiration_identity: None,
            venue_dust_closed_base_units: 0,
            venue_dust_released_quote_units: 0,
            venue_dust_at_utc_ms: None,
            venue_dust_identity: None,
            market_resolved_canceled_base_units: 0,
            market_resolved_released_quote_units: 0,
            market_resolved_at_utc_ms: None,
            market_resolution_effect_identity: None,
            canceled_base_units: 0,
            canceled_released_quote_units: 0,
            canceled_at_utc_ms: None,
            caller_cancel_effect_identity: None,
            terminalized_pending_maker_depletion_evidence: None,
            accepted_quote_units: 400_000,
            matched_quote_units: 400_000,
            open_quote_units: 0,
            released_quote_units: 0,
            sdk_taker_amount_audit_base_units: Some(1_000_000),
            immediate_taker_fee_encumbered_units: 10_000,
            terminal_reason: None,
        };
        let fill = SimFillRecordV1 {
            trade_id: "recovery-fill-trade".to_owned(),
            platform_order_id: order.platform_order_id.clone(),
            condition_id,
            token_id: "1".to_owned(),
            matched_at_utc_ms: 1_000,
            liquidity_role: SimLiquidityRoleV1::Taker,
            source_identity: "recovery-fill-source".to_owned(),
            maker_depletion_evidence: None,
            price_pusd_per_share: decimal("0.4"),
            base_units: 1_000_000,
            quote_units: 400_000,
            immediate_taker_fee_units: 10_000,
            finality_path: None,
            finality_path_ordinal: None,
            finality_state: None,
            economic_application_delta: None,
        };
        let accepted_delta = state
            .commit_new_order(order, vec![fill])
            .expect("恢复测试订单提交")
            .account_deltas
            .into_iter()
            .next()
            .expect("恢复测试订单接受变化");
        state
            .initialize_fill_finality(finality_policy.as_ref(), "recovery-fill-trade")
            .expect("恢复测试成交最终性初始化");
        state
            .validate_recovered(
                &directory,
                &opening_anchor,
                rules.as_ref(),
                finality_policy.as_ref(),
                None,
                &BTreeMap::new(),
            )
            .expect("真实业务入口形成的待闭合成交必须可恢复");
        (
            state,
            directory,
            opening_anchor,
            rules,
            finality_policy,
            accepted_delta,
        )
    }

    async fn pending_taker_fill_recovery_fixture() -> (
        ExecutionStateV1,
        SessionMarketDirectoryV1,
        contracts::AccountOpeningAnchorV1,
        Arc<SealedProcessRuleSnapshot>,
        Arc<TradeFinalityPolicyV1>,
        Arc<PlatformAccountDeltaV1>,
    ) {
        pending_taker_fill_recovery_fixture_with_policy(
            crate::finality::successful_policy_for_tests(),
        )
        .await
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminal_market_hot_state_eviction_rejects_nonterminal_and_validates_compact_witness()
    {
        let foreign_request_id = "terminal-eviction-foreign-request";
        let compact_finality_policy = crate::finality::policy_for_tests(
            "terminal-eviction-request-selector-policy",
            &[
                (TradeFinalityStateV1::Matched, 0),
                (TradeFinalityStateV1::Mined, 10),
                (TradeFinalityStateV1::Confirmed, 20),
            ],
            1,
            vec![(
                crate::finality::TradeFinalitySelectorV1::Request {
                    account_instance_key: recovery_fill_account_instance_key(),
                    global_request_id: foreign_request_id.to_owned(),
                },
                vec![
                    (TradeFinalityStateV1::Matched, 0),
                    (TradeFinalityStateV1::Failed, 10),
                ],
                None,
            )],
        );
        let (mut state, directory, opening_anchor, rules, finality_policy, accepted_delta) =
            pending_taker_fill_recovery_fixture_with_policy(Arc::clone(&compact_finality_policy))
                .await;
        assert_eq!(finality_policy, compact_finality_policy);
        let mut account_deltas = BTreeMap::from([(
            accepted_delta.account_transition_seq,
            accepted_delta.as_ref().clone(),
        )]);
        while let Some(next_at) = state.next_scheduled_finality_at_utc_ms() {
            let effects = state
                .advance_finality_at(next_at)
                .expect("测试成交必须推进到最终性终态");
            for effect in effects {
                if let Some(delta) = effect.trade_feedback.account_delta {
                    account_deltas.insert(delta.account_transition_seq, delta.as_ref().clone());
                }
                for settlement in effect.settlement_feedbacks {
                    if let Some(delta) = settlement.account_delta {
                        account_deltas.insert(delta.account_transition_seq, delta.as_ref().clone());
                    }
                }
            }
        }
        let member = directory.markets().next().expect("测试目录至少一个市场");
        let condition_id = member.market().condition_id.0.clone();
        for settlement in state
            .advance_rebate_clock_at(member.market().window.end_at_utc_ms)
            .expect("测试市场结束时钟必须闭合返佣市场结果")
        {
            if let Some(delta) = settlement.account_delta {
                account_deltas.insert(delta.account_transition_seq, delta.as_ref().clone());
            }
        }
        let resolution = MarketResolutionTerminalV3::Resolved {
            winning_asset_id: member.assets()[0].token_id.0.clone(),
            winning_outcome: member.assets()[0].outcome.label.clone(),
            payout_vector: member
                .assets()
                .iter()
                .enumerate()
                .map(|(index, asset)| contracts::PayoutEntryV1 {
                    asset: asset.clone(),
                    payout_pusd_per_share: ExactDecimal(if index == 0 {
                        "1".to_owned()
                    } else {
                        "0".to_owned()
                    }),
                })
                .collect(),
            mapping_rule_version: "terminal-hot-state-test-v1".to_owned(),
        };
        let mut resolution_event = crate::task22_tests::sealed_event(
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(resolution),
            crate::task22_tests::orderbook_evidence(
                member.market().window.end_at_utc_ms,
                member.market().window.end_at_utc_ms,
                1,
            ),
            crate::task21_tests::fresh_quality(),
        );
        resolution_event.market = member.market().clone();
        resolution_event.event_id =
            data_pipeline_core::derive_execution_market_event_v3_id(&resolution_event)
                .expect("测试判定事件身份");
        let (order_effects, auto_redeem) = state
            .apply_qualified_market_resolution(
                &resolution_event,
                member.market().window.end_at_utc_ms,
            )
            .expect("测试判定必须通过真实入口闭合并自动赎回");
        assert!(order_effects.is_empty());
        if let Some(delta) = auto_redeem.and_then(|effect| effect.account_delta) {
            account_deltas.insert(delta.account_transition_seq, delta.as_ref().clone());
        }
        let full_state = state.clone();

        assert!(matches!(
            state.evict_terminal_market_hot_state(
                &condition_id,
                MarketLifecyclePhaseV1::PostEndPendingResolution,
                compact_finality_policy.as_ref(),
            ),
            Err(MatchingStateErrorV1::Finality(message))
                if message.contains("ResolutionTerminal")
        ));
        assert_eq!(state, full_state, "拒绝路径不得部分删除热状态");

        let hot_state = state
            .evict_terminal_market_hot_state(
                &condition_id,
                MarketLifecyclePhaseV1::ResolutionTerminal,
                compact_finality_policy.as_ref(),
            )
            .expect("经济闭合的终态市场可以驱逐订单与成交热状态");
        assert_eq!(hot_state.fills.len(), 1);
        assert_eq!(
            hot_state
                .fills
                .get("recovery-fill-trade")
                .and_then(|witness| witness.finality_selector_identity.as_deref()),
            Some("recovery-fill-request"),
            "紧凑见证必须保留真实订单的请求级最终性选择身份"
        );
        assert!(
            state
                .orders()
                .all(|order| order.condition_id != condition_id)
        );
        assert!(state.fills().all(|fill| fill.condition_id != condition_id));
        assert!(state.matching_closed_markets.contains_key(&condition_id));
        assert!(state.market_resolutions.contains_key(&condition_id));
        assert_eq!(state.retained_rebate_fill_state_count(), 0);
        assert_eq!(
            state.rebate.market_result(&condition_id),
            full_state.rebate.market_result(&condition_id),
            "驱逐只能删除逐成交返佣热态，不能改变市场返佣结果"
        );
        assert_eq!(
            state.rebate.day_snapshot(RebateKindV1::Taker, 0),
            full_state.rebate.day_snapshot(RebateKindV1::Taker, 0)
        );
        assert_eq!(
            state.rebate.next_clock_at_utc_ms(),
            full_state.rebate.next_clock_at_utc_ms()
        );

        let encoded = serde_json::to_vec(&hot_state).expect("终态市场紧凑驱逐见证编码");
        let recovered_witness: TerminalExecutionMarketEvictionV1 =
            serde_json::from_slice(&encoded).expect("终态市场紧凑驱逐见证解码");
        assert_eq!(recovered_witness, hot_state);
        state
            .validate_recovered(
                &directory,
                &opening_anchor,
                rules.as_ref(),
                compact_finality_policy.as_ref(),
                state.gtd_advanced_through_utc_ms,
                &BTreeMap::from([(condition_id.clone(), recovered_witness.clone())]),
            )
            .expect("紧凑驱逐见证必须足以验证删除后的执行状态");
        state
            .validate_recovered_account_delta_chain(
                &opening_anchor,
                &account_deltas,
                compact_finality_policy.as_ref(),
                &BTreeMap::from([(condition_id.clone(), recovered_witness.clone())]),
            )
            .expect("紧凑驱逐见证必须绑定同一持久账户变化链");

        let mut tampered_witness = recovered_witness.clone();
        tampered_witness
            .fills
            .get_mut("recovery-fill-trade")
            .expect("紧凑驱逐成交见证")
            .finality_selector_identity = Some(foreign_request_id.to_owned());
        assert!(matches!(
            state.validate_recovered(
                &directory,
                &opening_anchor,
                rules.as_ref(),
                compact_finality_policy.as_ref(),
                state.gtd_advanced_through_utc_ms,
                &BTreeMap::from([(condition_id.clone(), tampered_witness)]),
            ),
            Err(MatchingStateErrorV1::Finality(message))
                if message.contains("已密封策略选择结果")
        ));

        let mut tampered_delta = recovered_witness.clone();
        tampered_delta
            .fills
            .get_mut("recovery-fill-trade")
            .and_then(|witness| witness.economic_application_delta.as_mut())
            .expect("成功紧凑成交必须保留经济变化引用")
            .delta_id
            .push_str("-tampered");
        let tampered_delta_error = state
            .validate_recovered_account_delta_chain(
                &opening_anchor,
                &account_deltas,
                compact_finality_policy.as_ref(),
                &BTreeMap::from([(condition_id.clone(), tampered_delta)]),
            )
            .expect_err("篡改紧凑经济变化引用必须拒绝恢复");
        assert!(
            matches!(tampered_delta_error, MatchingStateErrorV1::Finality(_)),
            "紧凑经济变化引用必须由最终性恢复门拒绝，实际错误：{tampered_delta_error:?}"
        );

        let mut tampered_rebate = recovered_witness;
        tampered_rebate
            .fills
            .get_mut("recovery-fill-trade")
            .expect("成功紧凑成交返佣见证")
            .rebate_fill
            .immediate_taker_fee_units = 20_000;
        assert!(matches!(
            state.validate_recovered(
                &directory,
                &opening_anchor,
                rules.as_ref(),
                compact_finality_policy.as_ref(),
                state.gtd_advanced_through_utc_ms,
                &BTreeMap::from([(condition_id, tampered_rebate)]),
            ),
            Err(MatchingStateErrorV1::Rebate(message))
                if message.contains("成交前驱")
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recovery_reselects_finality_path_and_rejects_coherent_foreign_path() {
        let (mut state, directory, opening_anchor, rules, finality_policy, _) =
            pending_taker_fill_recovery_fixture().await;
        let foreign_policy = crate::finality::policy_for_tests(
            "recovery-foreign-policy",
            &[
                (TradeFinalityStateV1::Matched, 0),
                (TradeFinalityStateV1::Mined, 11),
                (TradeFinalityStateV1::Confirmed, 22),
            ],
            1,
            Vec::new(),
        );
        state
            .fills
            .get_mut("recovery-fill-trade")
            .expect("恢复测试成交")
            .finality_path = Some(foreign_policy.normal_path().clone());
        state.scheduled_finality = BTreeSet::from([ScheduledFinalityTransitionKeyV1 {
            platform_event_at_utc_ms: 1_011,
            fill_economic_delta_identity: "recovery-fill-trade".to_owned(),
            path_ordinal: 1,
        }]);

        assert!(matches!(
            state.validate_recovered(
                &directory,
                &opening_anchor,
                rules.as_ref(),
                finality_policy.as_ref(),
                None,
                &BTreeMap::new(),
            ),
            Err(MatchingStateErrorV1::Finality(message))
                if message.contains("已密封策略选择结果")
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recovery_rejects_valid_but_wrong_delta_at_economic_checkpoint() {
        let (mut state, directory, opening_anchor, rules, finality_policy, accepted_delta) =
            pending_taker_fill_recovery_fixture().await;
        state.advance_finality_at(1_010).expect("推进到经济截点");
        state
            .fills
            .get_mut("recovery-fill-trade")
            .expect("恢复测试成交")
            .economic_application_delta = Some(accepted_delta);

        assert!(matches!(
            state.validate_recovered(
                &directory,
                &opening_anchor,
                rules.as_ref(),
                finality_policy.as_ref(),
                None,
                &BTreeMap::new(),
            ),
            Err(MatchingStateErrorV1::Finality(message))
                if message.contains("经济变化正文与已冻结截点")
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recovery_rejects_rebate_terminal_state_ahead_of_fill_finality() {
        let (state, directory, opening_anchor, rules, finality_policy, _) =
            pending_taker_fill_recovery_fixture().await;
        let mut value = serde_json::to_value(&state).expect("恢复返佣篡改测试 JSON");
        *value
            .pointer_mut("/rebate/taker_fills/recovery-fill-trade/terminal_succeeded")
            .expect("恢复返佣成交终态字段") = serde_json::Value::Bool(true);
        let buckets = value
            .pointer_mut("/rebate/day_buckets")
            .and_then(serde_json::Value::as_array_mut)
            .expect("恢复返佣日桶数组");
        let taker_bucket = buckets
            .iter_mut()
            .find(|entry| {
                entry
                    .as_array()
                    .and_then(|entry| entry.first())
                    .and_then(serde_json::Value::as_str)
                    == Some("Taker")
            })
            .and_then(serde_json::Value::as_array_mut)
            .and_then(|entry| entry.get_mut(2))
            .expect("恢复 taker 日桶");
        *taker_bucket
            .get_mut("pending_predecessors")
            .expect("恢复 taker 日桶前驱") = serde_json::json!([]);
        *taker_bucket
            .get_mut("accrued_pusd")
            .expect("恢复 taker 日桶应计") =
            serde_json::to_value(decimal("0.0008")).expect("返佣应计 JSON");
        let tampered: ExecutionStateV1 =
            serde_json::from_value(value).expect("一致化篡改返佣状态仍可解码");

        assert!(matches!(
            tampered.validate_recovered(
                &directory,
                &opening_anchor,
                rules.as_ref(),
                finality_policy.as_ref(),
                None,
                &BTreeMap::new(),
            ),
            Err(MatchingStateErrorV1::Rebate(message))
                if message.contains("成交前驱")
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recovery_rejects_raised_platform_and_finality_drive_watermarks() {
        let (state, directory, opening_anchor, rules, finality_policy, _) =
            pending_taker_fill_recovery_fixture().await;

        let mut gtd_raised = state.clone();
        gtd_raised.gtd_advanced_through_utc_ms = Some(9_999);
        assert!(matches!(
            gtd_raised.validate_recovered(
                &directory,
                &opening_anchor,
                rules.as_ref(),
                finality_policy.as_ref(),
                None,
                &BTreeMap::new(),
            ),
            Err(MatchingStateErrorV1::Finality(message))
                if message.contains("统一逻辑驱动水位")
        ));

        let mut finality_raised = state;
        finality_raised.finality_advanced_through_utc_ms = Some(9_999);
        assert!(matches!(
            finality_raised.validate_recovered(
                &directory,
                &opening_anchor,
                rules.as_ref(),
                finality_policy.as_ref(),
                None,
                &BTreeMap::new(),
            ),
            Err(MatchingStateErrorV1::Finality(message))
                if message.contains("最终性驱动水位")
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recovery_rejects_post_end_phase_without_matching_close() {
        let (state, directory, _, _, _, _) = pending_taker_fill_recovery_fixture().await;
        let condition_id = directory
            .markets()
            .next()
            .expect("恢复测试目录至少一个市场")
            .market()
            .condition_id
            .0
            .clone();
        let lifecycle_phases = BTreeMap::from([(
            condition_id.clone(),
            MarketLifecyclePhaseV1::PostEndPendingResolution,
        )]);

        assert!(matches!(
            state.validate_recovered_market_resolutions(&BTreeMap::new(), &lifecycle_phases),
            Err(MatchingStateErrorV1::ResolutionConflict(actual))
                if actual == condition_id
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn recovery_allows_post_end_raw_resolution_without_execution_obligation() {
        let (mut state, directory, _, _, _, _) = pending_taker_fill_recovery_fixture().await;
        let mut lifecycle_phases = BTreeMap::new();
        for market in directory.markets() {
            let condition_id = market.market().condition_id.0.clone();
            state
                .queues
                .release_market_matching_state(&condition_id)
                .expect("恢复测试关闭撮合队列");
            state
                .matching_closed_markets
                .insert(condition_id.clone(), market.market().window.end_at_utc_ms);
            lifecycle_phases.insert(
                condition_id,
                MarketLifecyclePhaseV1::PostEndPendingResolution,
            );
        }
        let market = directory.markets().next().expect("恢复测试至少一个市场");
        let winning_asset = &market.assets()[0];
        let resolution = MarketResolutionTerminalV3::Resolved {
            winning_asset_id: winning_asset.token_id.0.clone(),
            winning_outcome: winning_asset.outcome.label.clone(),
            payout_vector: market
                .assets()
                .iter()
                .enumerate()
                .map(|(index, asset)| contracts::PayoutEntryV1 {
                    asset: asset.clone(),
                    payout_pusd_per_share: ExactDecimal(if index == 0 {
                        "1".to_owned()
                    } else {
                        "0".to_owned()
                    }),
                })
                .collect(),
            mapping_rule_version: "recovery-raw-resolution-v1".to_owned(),
        };
        let qualified = BTreeMap::from([(
            market.market().condition_id.0.clone(),
            (resolution, MarketLifecyclePhaseV1::PostEndPendingResolution),
        )]);

        state
            .validate_recovered_market_resolutions(&qualified, &lifecycle_phases)
            .expect("PostEnd 的原始判定可能从未形成执行结算义务");
    }

    fn decimal(value: &str) -> Decimal {
        value.parse().expect("测试十进制必须有效")
    }

    fn allocations_by_identity(
        fills: &[(&str, u128, Decimal)],
        allocations: &[u128],
    ) -> BTreeMap<String, u128> {
        fills
            .iter()
            .zip(allocations)
            .map(|((identity, _, _), units)| ((*identity).to_owned(), *units))
            .collect()
    }

    fn taker_asset_candidate_queue_fixture(unrelated_assets: usize) -> ExecutionQueueStateV1 {
        let mut assets = (0..unrelated_assets)
            .map(|index| {
                (
                    format!("unrelated-token-{index:04}"),
                    AssetQueueV1 {
                        initial_loaded: true,
                        ..AssetQueueV1::default()
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        assets.insert(
            "target-token".to_owned(),
            AssetQueueV1 {
                bids: BTreeMap::new(),
                asks: BTreeMap::from([(
                    decimal("0.5"),
                    PriceQueueV1 {
                        segments: VecDeque::from([QueueSegmentV1::External(
                            ExternalSyntheticMakerSegmentV1 {
                                segment_id: Sha256Hex("f".repeat(64)),
                                total_base_units: 1_000_000,
                                total_quote_units: 500_000,
                                consumed_base_units: 0,
                            },
                        )]),
                        account_taker_depletion_debt_units: 0,
                    },
                )]),
                initial_loaded: true,
            },
        );
        ExecutionQueueStateV1 {
            markets: BTreeMap::from([("target-condition".to_owned(), assets)]),
        }
    }

    async fn new_order_delta_state_fixture(
        unrelated_assets: usize,
    ) -> (
        ExecutionStateV1,
        Arc<SealedProcessRuleSnapshot>,
        String,
        String,
    ) {
        use contracts::{
            ACCOUNT_OPENING_ANCHOR_SCHEMA_V1, AccountOpeningAnchorV1, ExactDecimal,
            PlatformAccountSnapshotV1,
        };

        let scenario = crate::scenario::scenario_for_sim_tests();
        let directory =
            SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("新订单候选测试目录");
        let rules = crate::platform_rules::tests::successful_snapshot_for_sim_tests().await;
        let market = scenario
            .market_set()
            .markets()
            .first()
            .expect("新订单候选测试市场");
        let condition_id = market.market().condition_id.0.clone();
        let token_id = market.assets()[0].token_id.0.clone();
        let opening_anchor = AccountOpeningAnchorV1 {
            schema_version: ACCOUNT_OPENING_ANCHOR_SCHEMA_V1.to_owned(),
            anchor_id: "new-order-delta-anchor".to_owned(),
            anchor_sha256: Sha256Hex("a".repeat(64)),
            account_instance_key: recovery_fill_account_instance_key(),
            session_id: "new-order-delta-session".to_owned(),
            initial_scenario_sha256: Sha256Hex("b".repeat(64)),
            initial_market_set_sha256: Sha256Hex("c".repeat(64)),
            initial_account_state_sha256: Sha256Hex("d".repeat(64)),
            initial_platform_account: PlatformAccountSnapshotV1 {
                cash_pusd: ExactDecimal("100".to_owned()),
                available_pusd: ExactDecimal("100".to_owned()),
                principal_encumbrance_pusd: ExactDecimal("0".to_owned()),
                immediate_taker_fee_encumbrance_pusd: ExactDecimal("0".to_owned()),
                outcome_balances: market
                    .assets()
                    .iter()
                    .map(|asset| PlatformOutcomeBalanceV1 {
                        token_id: asset.token_id.clone(),
                        balance_shares: ExactDecimal("0".to_owned()),
                    })
                    .collect(),
                maker_rebate_receivable_pusd: ExactDecimal("0".to_owned()),
                taker_rebate_receivable_pusd: ExactDecimal("0".to_owned()),
                maker_rebate_locked_remainder_pusd: ExactDecimal("0".to_owned()),
                taker_rebate_locked_remainder_pusd: ExactDecimal("0".to_owned()),
            },
        };
        let mut state =
            ExecutionStateV1::new(&directory, &opening_anchor, &rules).expect("新订单候选执行状态");
        let assets = state
            .queues
            .markets
            .get_mut(&condition_id)
            .expect("新订单候选撮合市场");
        for index in 0..unrelated_assets {
            assets.insert(
                format!("unrelated-token-{index:04}"),
                AssetQueueV1 {
                    initial_loaded: true,
                    ..AssetQueueV1::default()
                },
            );
        }
        assets.insert(
            token_id.clone(),
            AssetQueueV1 {
                bids: BTreeMap::new(),
                asks: BTreeMap::from([(
                    decimal("0.5"),
                    PriceQueueV1 {
                        segments: VecDeque::from([QueueSegmentV1::External(
                            ExternalSyntheticMakerSegmentV1 {
                                segment_id: Sha256Hex("f".repeat(64)),
                                total_base_units: 1_000_000,
                                total_quote_units: 500_000,
                                consumed_base_units: 0,
                            },
                        )]),
                        account_taker_depletion_debt_units: 0,
                    },
                )]),
                initial_loaded: true,
            },
        );
        (state, rules, condition_id, token_id)
    }

    fn new_order_delta_records(
        label: &str,
        condition_id: &str,
        token_id: &str,
        kind: SimOrderKindStateV1,
        source_price_pusd_per_share: Decimal,
        accepted_base_units: Option<u128>,
        accepted_quote_units: u128,
        plan: &QueueMatchPlanV1,
        fill_fee_units: &[u128],
    ) -> (SimOrderRecordV1, Vec<SimFillRecordV1>) {
        assert_eq!(fill_fee_units.len(), plan.fills.len());
        let (open_base_units, open_quote_units, released_quote_units) =
            if let Some(accepted_base_units) = accepted_base_units {
                let allocated_quote_units = cumulative_segment_quote_units_v1(
                    accepted_base_units,
                    accepted_quote_units,
                    plan.matched_base_units,
                )
                .expect("测试限价订单 quote 分配");
                (
                    accepted_base_units
                        .checked_sub(plan.matched_base_units)
                        .expect("测试成交不超过接受 base"),
                    accepted_quote_units
                        .checked_sub(allocated_quote_units)
                        .expect("测试开放 quote 不超过接受本金"),
                    allocated_quote_units
                        .checked_sub(plan.used_quote_units)
                        .expect("测试价格改善 quote 不小于成交 quote"),
                )
            } else {
                (
                    0,
                    0,
                    accepted_quote_units
                        .checked_sub(plan.used_quote_units)
                        .expect("测试市价成交不超过 quote 预算"),
                )
            };
        let platform_order_id = format!("{label}-order");
        let fills = plan
            .fills
            .iter()
            .zip(fill_fee_units)
            .enumerate()
            .map(
                |(index, (slice, immediate_taker_fee_units))| SimFillRecordV1 {
                    trade_id: format!("{label}-trade-{index}"),
                    platform_order_id: platform_order_id.clone(),
                    condition_id: condition_id.to_owned(),
                    token_id: token_id.to_owned(),
                    matched_at_utc_ms: 1_000,
                    liquidity_role: SimLiquidityRoleV1::Taker,
                    source_identity: slice.segment_id.0.clone(),
                    maker_depletion_evidence: None,
                    price_pusd_per_share: slice.price_pusd_per_share,
                    base_units: slice.base_units,
                    quote_units: slice.quote_units,
                    immediate_taker_fee_units: *immediate_taker_fee_units,
                    finality_path: None,
                    finality_path_ordinal: None,
                    finality_state: None,
                    economic_application_delta: None,
                },
            )
            .collect::<Vec<_>>();
        let immediate_taker_fee_encumbered_units = fill_fee_units
            .iter()
            .try_fold(0_u128, |total, fee| total.checked_add(*fee))
            .expect("测试手续费总额");
        (
            SimOrderRecordV1 {
                platform_order_id,
                global_request_id: format!("{label}-request"),
                item_id: format!("{label}-item"),
                item_idempotency_identity: format!("{label}-item-idempotency"),
                condition_id: condition_id.to_owned(),
                token_id: token_id.to_owned(),
                created_at_utc_ms: 1_000,
                last_updated_at_utc_ms: 1_000,
                kind,
                source_price_pusd_per_share,
                accepted_base_units,
                matched_base_units: plan.matched_base_units,
                pending_maker_depletion_base_units: 0,
                pending_maker_depletion_evidence: None,
                open_base_units,
                expired_base_units: 0,
                expired_released_quote_units: 0,
                expired_at_utc_ms: None,
                gtd_expiration_identity: None,
                venue_dust_closed_base_units: 0,
                venue_dust_released_quote_units: 0,
                venue_dust_at_utc_ms: None,
                venue_dust_identity: None,
                market_resolved_canceled_base_units: 0,
                market_resolved_released_quote_units: 0,
                market_resolved_at_utc_ms: None,
                market_resolution_effect_identity: None,
                canceled_base_units: 0,
                canceled_released_quote_units: 0,
                canceled_at_utc_ms: None,
                caller_cancel_effect_identity: None,
                terminalized_pending_maker_depletion_evidence: None,
                accepted_quote_units,
                matched_quote_units: plan.used_quote_units,
                open_quote_units,
                released_quote_units,
                sdk_taker_amount_audit_base_units: accepted_base_units
                    .is_none()
                    .then_some(plan.matched_base_units),
                immediate_taker_fee_encumbered_units,
                terminal_reason: matches!(kind, SimOrderKindStateV1::Fak)
                    .then_some(SimOrderTerminalReasonV1::FakCompleted),
            },
            fills,
        )
    }

    fn insert_open_gtd_order_for_drive_delta(
        state: &mut ExecutionStateV1,
        label: &str,
        condition_id: &str,
        token_id: &str,
        price: Decimal,
        base_units: u128,
        quote_units: u128,
        expires_at_utc_ms: i64,
    ) -> String {
        let empty_plan = QueueMatchPlanV1 {
            fills: Vec::new(),
            matched_base_units: 0,
            used_quote_units: 0,
        };
        let (order, fills) = new_order_delta_records(
            label,
            condition_id,
            token_id,
            SimOrderKindStateV1::Gtd { expires_at_utc_ms },
            price,
            Some(base_units),
            quote_units,
            &empty_plan,
            &[],
        );
        let platform_order_id = order.platform_order_id.clone();
        state
            .commit_new_order(order, fills)
            .expect("建立 GTD touched-key 测试订单");
        platform_order_id
    }

    async fn pending_resolution_drive_fixture(
        finality_policy: Arc<TradeFinalityPolicyV1>,
        fill_fee_units: &[u128],
    ) -> (
        ExecutionStateV1,
        Arc<SealedProcessRuleSnapshot>,
        String,
        String,
        Vec<String>,
    ) {
        assert!(!fill_fee_units.is_empty());
        let fill_count = u128::try_from(fill_fee_units.len()).expect("测试成交数");
        let fill_base_units = 1_000_000_u128
            .checked_div(fill_count)
            .expect("测试成交数必须为正");
        assert_eq!(fill_base_units * fill_count, 1_000_000);
        let fill_quote_units = 400_000_u128
            .checked_div(fill_count)
            .expect("测试成交数必须为正");
        assert_eq!(fill_quote_units * fill_count, 400_000);

        let (mut state, rules, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let plan = QueueMatchPlanV1 {
            fills: fill_fee_units
                .iter()
                .enumerate()
                .map(|(index, _)| TakerFillSliceV1 {
                    segment_id: Sha256Hex(format!("{:064x}", index + 1)),
                    price_pusd_per_share: decimal("0.4"),
                    base_units: fill_base_units,
                    quote_units: fill_quote_units,
                })
                .collect(),
            matched_base_units: 1_000_000,
            used_quote_units: 400_000,
        };
        let (order, fills) = new_order_delta_records(
            "drive-terminal",
            &condition_id,
            &token_id,
            SimOrderKindStateV1::Fok,
            decimal("0.4"),
            Some(1_000_000),
            400_000,
            &plan,
            fill_fee_units,
        );
        let trade_ids = fills
            .iter()
            .map(|fill| fill.trade_id.clone())
            .collect::<Vec<_>>();
        state
            .commit_new_order(order, fills)
            .expect("正式提交 scheduled terminal 测试订单");
        for trade_id in &trade_ids {
            state
                .initialize_fill_finality(finality_policy.as_ref(), trade_id)
                .expect("正式初始化 scheduled terminal 测试成交");
        }
        state
            .prepare_execution_drive_delta_v2(1_010)
            .expect("正式准备 end 前 Mined 非终态")
            .into_committable()
            .expect("end 前非终态候选必须可提交")
            .commit();
        let market_end_effects = state
            .advance_rebate_clock_at(2_000)
            .expect("正式推进市场结束返佣时钟");
        assert_eq!(market_end_effects.len(), 1);

        let scenario = crate::scenario::scenario_for_sim_tests();
        let directory = SessionMarketDirectoryV1::try_from_scenario(&scenario)
            .expect("scheduled terminal 测试目录");
        let market = scenario
            .market_set()
            .markets()
            .iter()
            .find(|member| member.market().condition_id.0 == condition_id)
            .expect("测试市场必须存在于正式场景");
        let resolution = MarketResolutionTerminalV3::Resolved {
            winning_asset_id: market.assets()[0].token_id.0.clone(),
            winning_outcome: market.assets()[0].outcome.label.clone(),
            payout_vector: market
                .assets()
                .iter()
                .enumerate()
                .map(|(index, asset)| contracts::PayoutEntryV1 {
                    asset: asset.clone(),
                    payout_pusd_per_share: ExactDecimal(if index == 0 {
                        "1".to_owned()
                    } else {
                        "0".to_owned()
                    }),
                })
                .collect(),
            mapping_rule_version: "drive-terminal-resolution-v1".to_owned(),
        };
        let mut resolution_event = crate::task22_tests::sealed_event(
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(resolution),
            crate::task22_tests::orderbook_evidence(2_000, 2_000, 1),
            crate::task21_tests::fresh_quality(),
        );
        resolution_event.market = market.market().clone();
        resolution_event.event_id =
            data_pipeline_core::derive_execution_market_event_v3_id(&resolution_event)
                .expect("测试判定事件身份");
        let qualified_resolution_event_ids = BTreeSet::from([resolution_event.event_id.0.clone()]);
        let effects = state
            .apply_authorized_market_events(
                &directory,
                2_000,
                &[resolution_event],
                finality_policy.as_ref(),
                &rules
                    .snapshot()
                    .project_account_rules
                    .lifecycle_account_behavior
                    .value,
                &qualified_resolution_event_ids,
            )
            .expect("正式授权入口接纳待最终性闭合的合格判定");
        assert!(effects.trade_finality_effects.is_empty());
        assert!(effects.venue_dust_effects.is_empty());
        assert!(effects.market_resolution_order_effects.is_empty());
        assert!(effects.settlement_feedbacks.is_empty());
        assert_eq!(
            state
                .market_resolutions
                .get(&condition_id)
                .expect("正式判定义务必须存在")
                .pending_trade_ids,
            trade_ids.iter().cloned().collect()
        );
        (state, rules, condition_id, token_id, trade_ids)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_terminal_success_is_atomic_with_rebate_and_auto_redeem() {
        let policy = crate::finality::policy_for_tests(
            "drive-terminal-success-policy",
            &[
                (TradeFinalityStateV1::Matched, 0),
                (TradeFinalityStateV1::Mined, 10),
                (TradeFinalityStateV1::Confirmed, 1_500),
            ],
            2,
            Vec::new(),
        );
        let (mut state, rules, condition_id, _, trade_ids) =
            pending_resolution_drive_fixture(policy, &[10_000, 0]).await;
        let rebate_pointer = Arc::as_ptr(&state.rebate);
        let resolution_pointer = Arc::as_ptr(&state.market_resolutions);
        let next_account_transition_seq = state.account.next_account_transition_seq();
        let expected_pending_removals = trade_ids.iter().cloned().collect::<BTreeSet<_>>();
        let baseline = serde_json::to_vec(&state).expect("序列化 terminal success drop 基线");
        reset_execution_state_clone_count_for_tests();

        {
            let prepared = state
                .prepare_execution_drive_delta_v2(2_500)
                .expect("同一候选必须闭合 Confirmed、返佣、pending 与自动赎回");
            let [positive_fee, zero_fee] = prepared.scheduled_finality_effects() else {
                panic!("同刻两笔成交必须各形成一个完整最终性效果")
            };
            assert_eq!(positive_fee.trade_feedback.trade_id, trade_ids[0]);
            assert_eq!(zero_fee.trade_feedback.trade_id, trade_ids[1]);
            assert_eq!(
                positive_fee.trade_feedback.finality_state,
                TradeFinalityStateV1::Confirmed
            );
            assert_eq!(
                zero_fee.trade_feedback.finality_state,
                TradeFinalityStateV1::Confirmed
            );
            let positive_economic_delta = positive_fee
                .trade_feedback
                .account_delta
                .as_deref()
                .expect("首笔 Confirmed 必须在终态截点应用经济变化");
            let [positive_accrual] = positive_fee.settlement_feedbacks.as_slice() else {
                panic!("正手续费终态必须紧随唯一 taker 返佣应计")
            };
            assert!(matches!(
                positive_accrual.action,
                SettlementLifecycleActionV1::Rebate(RebateSettlementActionV1::TakerAccrualApplied(
                    _
                ))
            ));
            let positive_accrual_delta = positive_accrual
                .account_delta
                .as_deref()
                .expect("正手续费应计必须形成账户变化");
            let zero_economic_delta = zero_fee
                .trade_feedback
                .account_delta
                .as_deref()
                .expect("零手续费成交仍必须应用最终性经济变化");
            let [auto_redeem] = zero_fee.settlement_feedbacks.as_slice() else {
                panic!("最后一笔 pending 闭合必须触发唯一自动赎回")
            };
            assert!(matches!(
                auto_redeem.action,
                SettlementLifecycleActionV1::AutoRedeem(_)
            ));
            let auto_redeem_delta = auto_redeem
                .account_delta
                .as_deref()
                .expect("有胜出代币余额的自动赎回必须形成账户变化");

            assert_eq!(
                positive_economic_delta.account_transition_seq,
                next_account_transition_seq
            );
            assert_eq!(
                positive_accrual_delta.account_transition_seq,
                next_account_transition_seq + 1
            );
            assert_eq!(
                positive_accrual_delta.predecessor,
                contracts::PlatformAccountDeltaPredecessorV1::AccountDelta {
                    delta_id: positive_economic_delta.delta_id.clone(),
                    delta_sha256: positive_economic_delta.delta_sha256.clone(),
                    account_transition_seq: positive_economic_delta.account_transition_seq,
                }
            );
            assert_eq!(
                zero_economic_delta.account_transition_seq,
                next_account_transition_seq + 2
            );
            assert_eq!(
                zero_economic_delta.predecessor,
                contracts::PlatformAccountDeltaPredecessorV1::AccountDelta {
                    delta_id: positive_accrual_delta.delta_id.clone(),
                    delta_sha256: positive_accrual_delta.delta_sha256.clone(),
                    account_transition_seq: positive_accrual_delta.account_transition_seq,
                }
            );
            assert_eq!(
                auto_redeem_delta.account_transition_seq,
                next_account_transition_seq + 3
            );
            assert_eq!(
                auto_redeem_delta.predecessor,
                contracts::PlatformAccountDeltaPredecessorV1::AccountDelta {
                    delta_id: zero_economic_delta.delta_id.clone(),
                    delta_sha256: zero_economic_delta.delta_sha256.clone(),
                    account_transition_seq: zero_economic_delta.account_transition_seq,
                }
            );
            assert_eq!(
                prepared
                    .candidate_account
                    .as_ref()
                    .expect("终态组合必须形成唯一候选账户")
                    .next_account_transition_seq(),
                next_account_transition_seq + 4
            );
            assert!(prepared.finality.rebate.has_state_change());
            assert_eq!(
                prepared
                    .finality
                    .resolution
                    .pending_trade_removals
                    .get(&condition_id),
                Some(&expected_pending_removals)
            );
            assert_eq!(
                prepared
                    .finality
                    .resolution
                    .redeemed_at_after
                    .get(&condition_id),
                Some(&2_500)
            );
            assert_eq!(prepared.finality.fill_replacements.len(), 2);
            assert_eq!(prepared.order_replacements.len(), 1);
            assert_eq!(
                prepared.order_replacements["drive-terminal-order"]
                    .immediate_taker_fee_encumbered_units,
                0
            );
            assert!(prepared.query_projection_changed());
        }

        assert_eq!(
            serde_json::to_vec(&state).expect("序列化 terminal success drop 后状态"),
            baseline
        );
        let obligation = state
            .market_resolutions
            .get(&condition_id)
            .expect("drop 后判定义务仍存在");
        assert_eq!(obligation.pending_trade_ids.len(), 2);
        assert!(obligation.redeemed_at_utc_ms.is_none());
        assert_eq!(Arc::as_ptr(&state.rebate), rebate_pointer);
        assert_eq!(Arc::as_ptr(&state.market_resolutions), resolution_pointer);
        assert_eq!(Arc::strong_count(&state.rebate), 1);
        assert_eq!(Arc::strong_count(&state.market_resolutions), 1);
        assert_eq!(execution_state_clone_count_for_tests(), 0);

        let shared_rebate = Arc::clone(&state.rebate);
        let shared_rebate_error = match state
            .prepare_execution_drive_delta_v2(2_500)
            .expect("共享返佣 Arc 不影响只读准备")
            .into_committable()
        {
            Err(error) => error,
            Ok(_) => panic!("非空返佣候选不得跨过共享 Arc 提交门"),
        };
        assert!(matches!(
            shared_rebate_error,
            MatchingStateErrorV1::Rebate(message) if message.contains("唯一状态所有权")
        ));
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化共享返佣失败后状态"),
            baseline
        );
        assert_eq!(Arc::as_ptr(&state.rebate), rebate_pointer);
        assert_eq!(Arc::as_ptr(&state.market_resolutions), resolution_pointer);
        assert!(Arc::ptr_eq(&state.rebate, &shared_rebate));
        assert_eq!(Arc::strong_count(&state.rebate), 2);
        drop(shared_rebate);

        let shared_resolution = Arc::clone(&state.market_resolutions);
        let shared_resolution_error = match state
            .prepare_execution_drive_delta_v2(2_500)
            .expect("共享判定义务 Arc 不影响只读准备")
            .into_committable()
        {
            Err(error) => error,
            Ok(_) => panic!("非空判定义务候选不得跨过共享 Arc 提交门"),
        };
        assert!(matches!(
            shared_resolution_error,
            MatchingStateErrorV1::Finality(message)
                if message.contains("判定义务候选") && message.contains("唯一状态所有权")
        ));
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化共享判定义务失败后状态"),
            baseline
        );
        assert_eq!(Arc::as_ptr(&state.rebate), rebate_pointer);
        assert_eq!(Arc::as_ptr(&state.market_resolutions), resolution_pointer);
        assert!(Arc::ptr_eq(&state.market_resolutions, &shared_resolution));
        assert_eq!(Arc::strong_count(&state.market_resolutions), 2);
        drop(shared_resolution);

        let prepared = state
            .prepare_execution_drive_delta_v2(2_500)
            .expect("重新准备唯一所有权下的终态组合");
        let expected_effects = prepared.scheduled_finality_effects().to_vec();
        let expected_account = prepared
            .candidate_account
            .as_ref()
            .expect("终态组合必须形成候选账户")
            .clone();
        let (): () = prepared
            .into_committable()
            .expect("返佣与判定义务均独占时终态组合必须可提交")
            .commit();

        assert_eq!(state.account, expected_account);
        assert_eq!(
            state.account.next_account_transition_seq(),
            next_account_transition_seq + 4
        );
        assert_eq!(Arc::as_ptr(&state.rebate), rebate_pointer);
        assert_eq!(Arc::as_ptr(&state.market_resolutions), resolution_pointer);
        assert_eq!(Arc::strong_count(&state.rebate), 1);
        assert_eq!(Arc::strong_count(&state.market_resolutions), 1);
        for (trade_id, expected) in trade_ids.iter().zip(&expected_effects) {
            let fill = state.fills.get(trade_id).expect("提交后成交必须保留");
            assert_eq!(fill.finality_state, Some(TradeFinalityStateV1::Confirmed));
            assert_eq!(fill.finality_path_ordinal, Some(2));
            assert_eq!(
                fill.economic_application_delta.as_deref(),
                expected
                    .trade_feedback
                    .economic_application_delta
                    .as_deref()
            );
            assert!(
                !state
                    .scheduled_finality
                    .iter()
                    .any(|key| key.fill_economic_delta_identity == *trade_id)
            );
        }
        let committed_order = &state.orders["drive-terminal-order"];
        assert_eq!(committed_order.immediate_taker_fee_encumbered_units, 0);
        assert_eq!(committed_order.last_updated_at_utc_ms, 2_500);
        let obligation = state
            .market_resolutions
            .get(&condition_id)
            .expect("提交后判定义务仍存在");
        assert!(obligation.pending_trade_ids.is_empty());
        assert_eq!(obligation.redeemed_at_utc_ms, Some(2_500));
        let directory =
            SessionMarketDirectoryV1::try_from_scenario(&crate::scenario::scenario_for_sim_tests())
                .expect("终态提交后返佣恢复校验目录");
        assert!(state.rebate.has_consistent_recovery_state(
            &directory,
            rules.as_ref(),
            &state.fills,
            &BTreeMap::new(),
            &BTreeSet::new(),
        ));
        assert_eq!(state.gtd_advanced_through_utc_ms, Some(2_500));
        assert_eq!(state.finality_advanced_through_utc_ms, Some(2_500));

        let committed = serde_json::to_vec(&state).expect("序列化终态原子提交后状态");
        let repeated = state
            .prepare_execution_drive_delta_v2(2_500)
            .expect("同 cutoff 终态驱动必须可幂等重放");
        assert!(repeated.gtd_expiration_effects().is_empty());
        assert!(repeated.scheduled_finality_effects().is_empty());
        assert!(!repeated.query_projection_changed());
        repeated
            .into_committable()
            .expect("空重复候选不要求额外 Arc 所有权")
            .commit();
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化重复终态驱动后状态"),
            committed
        );
        assert_eq!(execution_state_clone_count_for_tests(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_candidate_iterators_overlay_each_live_key_once_without_write()
    {
        let policy = crate::finality::policy_for_tests(
            "drive-candidate-iterators-policy",
            &[
                (TradeFinalityStateV1::Matched, 0),
                (TradeFinalityStateV1::Mined, 10),
                (TradeFinalityStateV1::Confirmed, 1_500),
            ],
            2,
            Vec::new(),
        );
        let (mut state, _, _, _, trade_ids) =
            pending_resolution_drive_fixture(policy, &[10_000, 0]).await;
        let live_order_ids = state.orders.keys().cloned().collect::<Vec<_>>();
        let live_trade_ids = state.fills.keys().cloned().collect::<Vec<_>>();
        let baseline = serde_json::to_vec(&state).expect("序列化全局候选迭代器 drop 基线");
        reset_execution_state_clone_count_for_tests();

        {
            let prepared = state
                .prepare_execution_drive_delta_v2(2_500)
                .expect("准备全局候选迭代器真实最终性 after-image");
            let candidate_orders = prepared.candidate_orders().collect::<Vec<_>>();
            let candidate_fills = prepared.candidate_fills().collect::<Vec<_>>();

            assert_eq!(candidate_orders.len(), live_order_ids.len());
            assert_eq!(
                candidate_orders
                    .iter()
                    .map(|order| order.platform_order_id.clone())
                    .collect::<Vec<_>>(),
                live_order_ids
            );
            assert_eq!(candidate_fills.len(), live_trade_ids.len());
            assert_eq!(
                candidate_fills
                    .iter()
                    .map(|fill| fill.trade_id.clone())
                    .collect::<Vec<_>>(),
                live_trade_ids
            );
            for order in candidate_orders {
                let replacement = prepared
                    .order_replacements
                    .get(&order.platform_order_id)
                    .expect("真实终态驱动必须替换目标订单");
                assert!(std::ptr::eq(order, replacement));
                assert_eq!(order.immediate_taker_fee_encumbered_units, 0);
            }
            for fill in candidate_fills {
                let replacement = prepared
                    .finality
                    .fill_replacements
                    .get(&fill.trade_id)
                    .expect("真实终态驱动必须替换目标成交");
                assert!(std::ptr::eq(fill, replacement));
                assert_eq!(fill.finality_state, Some(TradeFinalityStateV1::Confirmed));
            }
            assert_eq!(trade_ids, live_trade_ids);
            assert_eq!(
                serde_json::to_vec(&*prepared.live).expect("序列化未提交 live 状态"),
                baseline
            );
            assert_eq!(execution_state_clone_count_for_tests(), 0);
        }

        assert_eq!(
            serde_json::to_vec(&state).expect("序列化全局候选迭代器 drop 后状态"),
            baseline
        );
        assert_eq!(execution_state_clone_count_for_tests(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_terminal_and_taker_maturity_share_exact_time_atomically() {
        const TAKER_MATURITY_AT_UTC_MS: i64 = 87_000_000;
        let policy = crate::finality::policy_for_tests(
            "drive-terminal-taker-maturity-collision-policy",
            &[
                (TradeFinalityStateV1::Matched, 0),
                (TradeFinalityStateV1::Mined, 10),
                (
                    TradeFinalityStateV1::Confirmed,
                    TAKER_MATURITY_AT_UTC_MS - 1_000,
                ),
            ],
            2,
            Vec::new(),
        );
        let (mut state, _, condition_id, _, trade_ids) =
            pending_resolution_drive_fixture(policy, &[20_000_000]).await;
        let live_fill_before = state
            .fills
            .get(&trade_ids[0])
            .expect("同刻 terminal+maturity 测试成交必须存在")
            .clone();
        let live_order_before = state
            .orders
            .get(&live_fill_before.platform_order_id)
            .expect("同刻 terminal+maturity 测试订单必须存在")
            .clone();
        let live_order_count = state
            .orders
            .values()
            .filter(|order| order.condition_id == condition_id)
            .count();
        let live_fill_count = state
            .fills
            .values()
            .filter(|fill| fill.condition_id == condition_id)
            .count();
        let rebate_pointer = Arc::as_ptr(&state.rebate);
        assert_eq!(
            state.next_rebate_clock_at_utc_ms(),
            Some(TAKER_MATURITY_AT_UTC_MS)
        );
        let next_account_transition_seq = state.account.next_account_transition_seq();
        let baseline = serde_json::to_vec(&state).expect("序列化 terminal+maturity drop 基线");

        {
            let prepared = state
                .prepare_execution_drive_delta_v2(TAKER_MATURITY_AT_UTC_MS)
                .expect("同一候选必须按同刻顺序闭合 terminal 与 taker maturity");
            let [lifecycle] = prepared.scheduled_finality_effects() else {
                panic!("同刻 Confirmed 必须形成唯一最终性效果")
            };
            assert_eq!(lifecycle.trade_feedback.trade_id, trade_ids[0]);
            assert!(prepared.has_state_change());
            assert_eq!(
                prepared
                    .candidate_fill(&trade_ids[0])
                    .expect("候选成交必须读到 prepared after-image")
                    .finality_state,
                Some(TradeFinalityStateV1::Confirmed)
            );
            assert_ne!(
                prepared
                    .candidate_fill(&trade_ids[0])
                    .expect("候选成交必须读到 prepared after-image"),
                &live_fill_before
            );
            assert_eq!(
                prepared
                    .candidate_order(&live_fill_before.platform_order_id)
                    .expect("候选订单必须读到 prepared after-image")
                    .immediate_taker_fee_encumbered_units,
                0
            );
            assert_ne!(
                prepared
                    .candidate_order(&live_fill_before.platform_order_id)
                    .expect("候选订单必须读到 prepared after-image"),
                &live_order_before
            );
            let candidate_orders = prepared
                .candidate_orders_for_condition(&condition_id)
                .collect::<Vec<_>>();
            assert_eq!(candidate_orders.len(), live_order_count);
            assert!(candidate_orders.iter().any(|order| {
                order.platform_order_id == live_fill_before.platform_order_id
                    && order.immediate_taker_fee_encumbered_units == 0
            }));
            let candidate_fills = prepared
                .candidate_fills_for_condition(&condition_id)
                .collect::<Vec<_>>();
            assert_eq!(candidate_fills.len(), live_fill_count);
            assert!(candidate_fills.iter().any(|fill| {
                fill.trade_id == trade_ids[0]
                    && fill.finality_state == Some(TradeFinalityStateV1::Confirmed)
            }));
            assert_eq!(
                lifecycle.trade_feedback.finality_state,
                TradeFinalityStateV1::Confirmed
            );
            let economic_delta = lifecycle
                .trade_feedback
                .account_delta
                .as_deref()
                .expect("同刻 Confirmed 必须先形成经济变化");
            let [accrual, auto_redeem] = lifecycle.settlement_feedbacks.as_slice() else {
                panic!("terminal 必须依次形成 taker 应计与自动赎回")
            };
            assert!(matches!(
                accrual.action,
                SettlementLifecycleActionV1::Rebate(RebateSettlementActionV1::TakerAccrualApplied(
                    _
                ))
            ));
            assert!(matches!(
                auto_redeem.action,
                SettlementLifecycleActionV1::AutoRedeem(_)
            ));
            let accrual_delta = accrual
                .account_delta
                .as_deref()
                .expect("正手续费 taker 应计必须形成账户变化");
            let auto_redeem_delta = auto_redeem
                .account_delta
                .as_deref()
                .expect("胜出结果代币自动赎回必须形成账户变化");
            let [day_finalized, paid] = prepared.rebate_clock_effects() else {
                panic!("terminal 后同刻成熟时钟必须依次形成日结与付款")
            };
            assert!(matches!(
                day_finalized.action,
                SettlementLifecycleActionV1::Rebate(RebateSettlementActionV1::DayFinalized(_))
            ));
            assert!(day_finalized.account_delta.is_none());
            assert!(matches!(
                paid.action,
                SettlementLifecycleActionV1::Rebate(RebateSettlementActionV1::Paid(_))
            ));
            let paid_delta = paid
                .account_delta
                .as_deref()
                .expect("达到付款门槛的 taker 日桶必须形成付款变化");
            for effect in lifecycle
                .settlement_feedbacks
                .iter()
                .chain(prepared.rebate_clock_effects())
            {
                assert_eq!(
                    effect.action.platform_effective_at_utc_ms(),
                    TAKER_MATURITY_AT_UTC_MS
                );
            }
            assert_eq!(
                accrual_delta.predecessor,
                contracts::PlatformAccountDeltaPredecessorV1::AccountDelta {
                    delta_id: economic_delta.delta_id.clone(),
                    delta_sha256: economic_delta.delta_sha256.clone(),
                    account_transition_seq: economic_delta.account_transition_seq,
                }
            );
            assert_eq!(
                auto_redeem_delta.predecessor,
                contracts::PlatformAccountDeltaPredecessorV1::AccountDelta {
                    delta_id: accrual_delta.delta_id.clone(),
                    delta_sha256: accrual_delta.delta_sha256.clone(),
                    account_transition_seq: accrual_delta.account_transition_seq,
                }
            );
            assert_eq!(
                paid_delta.predecessor,
                contracts::PlatformAccountDeltaPredecessorV1::AccountDelta {
                    delta_id: auto_redeem_delta.delta_id.clone(),
                    delta_sha256: auto_redeem_delta.delta_sha256.clone(),
                    account_transition_seq: auto_redeem_delta.account_transition_seq,
                }
            );
            assert_eq!(
                prepared
                    .finality
                    .rebate
                    .candidate_next_clock(prepared.live.rebate.as_ref()),
                None,
                "唯一 Drive 必须在 terminal 后重新读取并消费同刻 taker maturity"
            );
            assert_eq!(
                prepared.candidate_account().next_account_transition_seq(),
                next_account_transition_seq + 4
            );
            assert_eq!(
                prepared
                    .finality
                    .resolution
                    .redeemed_at_after
                    .get(&condition_id),
                Some(&TAKER_MATURITY_AT_UTC_MS)
            );
            assert!(prepared.finality.rebate.has_state_change());
        }

        assert_eq!(
            serde_json::to_vec(&state).expect("序列化 terminal+maturity drop 后状态"),
            baseline
        );

        let shared_rebate = Arc::clone(&state.rebate);
        let shared_error = match state
            .prepare_execution_drive_delta_v2(TAKER_MATURITY_AT_UTC_MS)
            .expect("共享返佣 Arc 不影响同刻候选准备")
            .into_committable()
        {
            Err(error) => error,
            Ok(_) => panic!("非空 terminal+maturity 返佣候选不得跨过共享 Arc 门"),
        };
        assert!(matches!(
            shared_error,
            MatchingStateErrorV1::Rebate(message) if message.contains("唯一状态所有权")
        ));
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化共享返佣失败后状态"),
            baseline
        );
        assert!(Arc::ptr_eq(&state.rebate, &shared_rebate));
        assert_eq!(Arc::as_ptr(&state.rebate), rebate_pointer);
        drop(shared_rebate);

        let prepared = state
            .prepare_execution_drive_delta_v2(TAKER_MATURITY_AT_UTC_MS)
            .expect("重新准备独占 terminal+maturity 候选");
        let expected_account = prepared
            .candidate_account
            .as_ref()
            .expect("同刻候选必须形成唯一账户 after-image")
            .clone();
        let expected_finality = prepared.scheduled_finality_effects()[0].clone();
        let expected_clock_effects = prepared.rebate_clock_effects().to_vec();
        let (): () = prepared
            .into_committable()
            .expect("独占 terminal+maturity 候选必须可提交")
            .commit();

        assert_eq!(state.account, expected_account);
        assert_eq!(Arc::as_ptr(&state.rebate), rebate_pointer);
        assert_eq!(state.next_rebate_clock_at_utc_ms(), None);
        let fill = state.fills.get(&trade_ids[0]).expect("提交后成交仍存在");
        assert_eq!(fill.finality_state, Some(TradeFinalityStateV1::Confirmed));
        assert_eq!(
            fill.economic_application_delta.as_deref(),
            expected_finality
                .trade_feedback
                .economic_application_delta
                .as_deref()
        );
        let day = state
            .rebate
            .day_snapshot(RebateKindV1::Taker, 0)
            .expect("提交后 taker 日桶仍存在");
        assert!(day.maturity_reached);
        assert_eq!(day.pending_predecessor_count, 0);
        assert_eq!(day.accrued_pusd, decimal("1.6"));
        assert!(day.finalized);
        assert_eq!(day.payable_pusd, decimal("1.6"));
        assert!(day.paid);
        let obligation = state
            .market_resolutions
            .get(&condition_id)
            .expect("提交后判定义务仍存在");
        assert!(obligation.pending_trade_ids.is_empty());
        assert_eq!(
            obligation.redeemed_at_utc_ms,
            Some(TAKER_MATURITY_AT_UTC_MS)
        );
        assert!(matches!(
            expected_clock_effects.as_slice(),
            [
                SettlementLifecycleEffectV1 {
                    action: SettlementLifecycleActionV1::Rebate(
                        RebateSettlementActionV1::DayFinalized(_)
                    ),
                    ..
                },
                SettlementLifecycleEffectV1 {
                    action: SettlementLifecycleActionV1::Rebate(RebateSettlementActionV1::Paid(_)),
                    ..
                }
            ]
        ));

        let committed = serde_json::to_vec(&state).expect("序列化同刻候选提交后状态");
        let repeated = state
            .prepare_execution_drive_delta_v2(TAKER_MATURITY_AT_UTC_MS)
            .expect("同 cutoff 必须可幂等重备");
        assert!(repeated.gtd_expiration_effects().is_empty());
        assert!(repeated.scheduled_finality_effects().is_empty());
        assert!(repeated.rebate_clock_effects().is_empty());
        assert!(!repeated.query_projection_changed());
        assert!(!repeated.has_state_change());
        repeated
            .into_committable()
            .expect("空重复候选不要求额外 Arc 所有权")
            .commit();
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化同刻候选重复提交后状态"),
            committed
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_empty_rebate_clock_actions_still_require_unique_arc() {
        const TAKER_MATURITY_AT_UTC_MS: i64 = 87_000_000;
        let policy = crate::finality::policy_for_tests(
            "drive-empty-rebate-clock-policy",
            &[
                (TradeFinalityStateV1::Matched, 0),
                (TradeFinalityStateV1::Mined, 10),
                (
                    TradeFinalityStateV1::Confirmed,
                    TAKER_MATURITY_AT_UTC_MS + 4_000,
                ),
            ],
            2,
            Vec::new(),
        );
        let (mut state, _, condition_id, _, trade_ids) =
            pending_resolution_drive_fixture(policy, &[10_000]).await;
        let live_fill = state
            .fills
            .get(&trade_ids[0])
            .expect("空动作成熟测试成交必须存在")
            .clone();
        let live_order = state
            .orders
            .get(&live_fill.platform_order_id)
            .expect("空动作成熟测试订单必须存在")
            .clone();
        let live_account = state.account.clone();
        assert_eq!(
            state.next_rebate_clock_at_utc_ms(),
            Some(TAKER_MATURITY_AT_UTC_MS)
        );
        let baseline = serde_json::to_vec(&state).expect("序列化空返佣动作时钟基线");
        let rebate_pointer = Arc::as_ptr(&state.rebate);
        let shared_rebate = Arc::clone(&state.rebate);

        let prepared = state
            .prepare_execution_drive_delta_v2(TAKER_MATURITY_AT_UTC_MS)
            .expect("成熟但仍有 pending 的返佣时钟必须可准备");
        assert!(prepared.scheduled_finality_effects().is_empty());
        assert!(prepared.rebate_clock_effects().is_empty());
        assert!(prepared.finality.rebate.has_state_change());
        assert!(prepared.candidate_account.is_none());
        assert!(prepared.has_state_change());
        assert_eq!(prepared.candidate_account(), &live_account);
        assert_eq!(prepared.candidate_fill(&trade_ids[0]), Some(&live_fill));
        assert_eq!(
            prepared.candidate_order(&live_fill.platform_order_id),
            Some(&live_order)
        );
        assert_eq!(
            prepared
                .candidate_orders_for_condition(&condition_id)
                .collect::<Vec<_>>(),
            vec![&live_order]
        );
        assert_eq!(
            prepared
                .candidate_fills_for_condition(&condition_id)
                .collect::<Vec<_>>(),
            vec![&live_fill]
        );
        let shared_error = match prepared.into_committable() {
            Err(error) => error,
            Ok(_) => panic!("无动作但有返佣状态变化的候选不得绕过共享 Arc 门"),
        };
        assert!(matches!(
            shared_error,
            MatchingStateErrorV1::Rebate(message) if message.contains("唯一状态所有权")
        ));
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化空动作共享 Arc 失败后状态"),
            baseline
        );
        assert!(Arc::ptr_eq(&state.rebate, &shared_rebate));
        assert_eq!(Arc::as_ptr(&state.rebate), rebate_pointer);
        drop(shared_rebate);

        state
            .prepare_execution_drive_delta_v2(TAKER_MATURITY_AT_UTC_MS)
            .expect("重新准备独占空动作返佣时钟")
            .into_committable()
            .expect("独占空动作返佣时钟必须可提交")
            .commit();
        assert_eq!(Arc::as_ptr(&state.rebate), rebate_pointer);
        assert_eq!(state.next_rebate_clock_at_utc_ms(), None);
        let day = state
            .rebate
            .day_snapshot(RebateKindV1::Taker, 0)
            .expect("提交后 pending taker 日桶仍存在");
        assert!(day.maturity_reached);
        assert_eq!(day.pending_predecessor_count, 1);
        assert!(!day.finalized);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_terminal_failure_keeps_compensation_and_empty_rebate_atomic()
    {
        let policy = crate::finality::policy_for_tests(
            "drive-terminal-failure-policy",
            &[
                (TradeFinalityStateV1::Matched, 0),
                (TradeFinalityStateV1::Mined, 10),
                (TradeFinalityStateV1::Confirmed, 1_500),
            ],
            2,
            vec![(
                crate::finality::TradeFinalitySelectorV1::Request {
                    account_instance_key: recovery_fill_account_instance_key(),
                    global_request_id: "drive-terminal-request".to_owned(),
                },
                vec![
                    (TradeFinalityStateV1::Matched, 0),
                    (TradeFinalityStateV1::Mined, 10),
                    (TradeFinalityStateV1::Failed, 1_500),
                ],
                Some(1),
            )],
        );
        let (mut state, rules, condition_id, _, trade_ids) =
            pending_resolution_drive_fixture(policy, &[10_000]).await;
        let rebate_pointer = Arc::as_ptr(&state.rebate);
        let resolution_pointer = Arc::as_ptr(&state.market_resolutions);
        let next_account_transition_seq = state.account.next_account_transition_seq();
        let economic_application_delta = state.fills[&trade_ids[0]]
            .economic_application_delta
            .as_ref()
            .expect("失败补偿夹具必须已在 Mined 应用经济变化")
            .clone();
        let baseline = serde_json::to_vec(&state).expect("序列化 terminal failure drop 基线");
        reset_execution_state_clone_count_for_tests();

        {
            let prepared = state
                .prepare_execution_drive_delta_v2(2_500)
                .expect("同一候选必须闭合 Failed 补偿、空返佣动作与零余额赎回");
            let [lifecycle] = prepared.scheduled_finality_effects() else {
                panic!("失败终态必须形成唯一完整最终性效果")
            };
            assert_eq!(lifecycle.trade_feedback.trade_id, trade_ids[0]);
            assert_eq!(
                lifecycle.trade_feedback.finality_state,
                TradeFinalityStateV1::Failed
            );
            assert_eq!(
                lifecycle.trade_feedback.economic_disposition,
                TradeEconomicDispositionStateV1::FinalFailureCompensated
            );
            assert_eq!(
                lifecycle
                    .trade_feedback
                    .economic_application_delta
                    .as_deref(),
                Some(economic_application_delta.as_ref())
            );
            let compensation_delta = lifecycle
                .trade_feedback
                .account_delta
                .as_deref()
                .expect("已应用失败终态必须形成补偿账户变化");
            assert_eq!(
                compensation_delta.reason,
                contracts::PlatformAccountDeltaReasonV1::FillFinalityCompensationApplied
            );
            assert_eq!(
                compensation_delta.account_transition_seq,
                next_account_transition_seq
            );
            assert_eq!(
                compensation_delta.predecessor,
                contracts::PlatformAccountDeltaPredecessorV1::AccountDelta {
                    delta_id: economic_application_delta.delta_id.clone(),
                    delta_sha256: economic_application_delta.delta_sha256.clone(),
                    account_transition_seq: economic_application_delta.account_transition_seq,
                }
            );
            assert!(matches!(
                lifecycle.settlement_feedbacks.as_slice(),
                [SettlementLifecycleEffectV1 {
                    action: SettlementLifecycleActionV1::AutoRedeem(_),
                    account_delta: None,
                }]
            ));
            assert!(
                prepared.finality.rebate.has_state_change(),
                "失败终态即使不产生返佣账户动作，也必须提交严格终态身份"
            );
            assert_eq!(
                prepared
                    .finality
                    .resolution
                    .pending_trade_removals
                    .get(&condition_id),
                Some(&BTreeSet::from([trade_ids[0].clone()]))
            );
            assert_eq!(
                prepared
                    .finality
                    .resolution
                    .redeemed_at_after
                    .get(&condition_id),
                Some(&2_500)
            );
            assert_eq!(
                prepared
                    .candidate_account
                    .as_ref()
                    .expect("失败终态必须保留共享候选账户")
                    .next_account_transition_seq(),
                next_account_transition_seq + 1
            );
        }

        assert_eq!(
            serde_json::to_vec(&state).expect("序列化 terminal failure drop 后状态"),
            baseline
        );
        let obligation = state
            .market_resolutions
            .get(&condition_id)
            .expect("drop 后失败判定义务仍存在");
        assert_eq!(obligation.pending_trade_ids.len(), 1);
        assert!(obligation.redeemed_at_utc_ms.is_none());
        assert_eq!(Arc::as_ptr(&state.rebate), rebate_pointer);
        assert_eq!(Arc::as_ptr(&state.market_resolutions), resolution_pointer);
        assert_eq!(execution_state_clone_count_for_tests(), 0);

        let prepared = state
            .prepare_execution_drive_delta_v2(2_500)
            .expect("重新准备 Failed 补偿、严格返佣终态与零余额赎回");
        let expected_effect = prepared.scheduled_finality_effects()[0].clone();
        let expected_account = prepared
            .candidate_account
            .as_ref()
            .expect("失败终态组合必须形成候选账户")
            .clone();
        let (): () = prepared
            .into_committable()
            .expect("失败终态组合在独占 Arc 下必须可提交")
            .commit();

        assert_eq!(state.account, expected_account);
        assert_eq!(
            state.account.next_account_transition_seq(),
            next_account_transition_seq + 1
        );
        assert_eq!(Arc::as_ptr(&state.rebate), rebate_pointer);
        assert_eq!(Arc::as_ptr(&state.market_resolutions), resolution_pointer);
        let committed_fill = state
            .fills
            .get(&trade_ids[0])
            .expect("失败终态提交后成交必须保留");
        assert_eq!(
            committed_fill.finality_state,
            Some(TradeFinalityStateV1::Failed)
        );
        assert_eq!(committed_fill.finality_path_ordinal, Some(2));
        assert_eq!(
            committed_fill.economic_application_delta.as_deref(),
            Some(economic_application_delta.as_ref())
        );
        assert_eq!(
            committed_fill.economic_disposition(),
            TradeEconomicDispositionStateV1::FinalFailureCompensated
        );
        assert_eq!(
            expected_effect
                .trade_feedback
                .account_delta
                .as_deref()
                .expect("失败终态输出必须冻结补偿变化")
                .reason,
            contracts::PlatformAccountDeltaReasonV1::FillFinalityCompensationApplied
        );
        assert!(
            !state
                .scheduled_finality
                .iter()
                .any(|key| key.fill_economic_delta_identity == trade_ids[0])
        );
        let committed_order = &state.orders["drive-terminal-order"];
        assert_eq!(committed_order.immediate_taker_fee_encumbered_units, 0);
        assert_eq!(committed_order.last_updated_at_utc_ms, 1_010);
        let obligation = state
            .market_resolutions
            .get(&condition_id)
            .expect("失败终态提交后判定义务仍存在");
        assert!(obligation.pending_trade_ids.is_empty());
        assert_eq!(obligation.redeemed_at_utc_ms, Some(2_500));
        let directory =
            SessionMarketDirectoryV1::try_from_scenario(&crate::scenario::scenario_for_sim_tests())
                .expect("失败终态提交后返佣恢复校验目录");
        assert!(state.rebate.has_consistent_recovery_state(
            &directory,
            rules.as_ref(),
            &state.fills,
            &BTreeMap::new(),
            &BTreeSet::new(),
        ));
        assert_eq!(state.gtd_advanced_through_utc_ms, Some(2_500));
        assert_eq!(state.finality_advanced_through_utc_ms, Some(2_500));
        assert_eq!(execution_state_clone_count_for_tests(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_nonterminal_finality_is_atomic_and_shares_account_order() {
        let (mut state, _, _, _, _, _) = pending_taker_fill_recovery_fixture().await;
        let fill = state
            .fills
            .get("recovery-fill-trade")
            .expect("真实入口形成待闭合成交")
            .clone();
        let same_time_gtd_order_id = insert_open_gtd_order_for_drive_delta(
            &mut state,
            "drive-finality-same-time-gtd",
            &fill.condition_id,
            &fill.token_id,
            decimal("0.3"),
            1_000_000,
            300_000,
            1_010,
        );
        let later_gtd_order_id = insert_open_gtd_order_for_drive_delta(
            &mut state,
            "drive-finality-later-gtd",
            &fill.condition_id,
            &fill.token_id,
            decimal("0.3"),
            1_000_000,
            300_000,
            1_015,
        );
        let baseline = state.clone();
        reset_execution_state_clone_count_for_tests();

        {
            let prepared = state
                .prepare_execution_drive_delta_v2(1_015)
                .expect("准备 GTD 与非终态最终性的全局有序候选");
            assert_eq!(
                prepared
                    .gtd_expiration_effects()
                    .iter()
                    .map(|effect| effect.platform_order_id.as_str())
                    .collect::<Vec<_>>(),
                vec![same_time_gtd_order_id.as_str(), later_gtd_order_id.as_str()]
            );
            let [lifecycle_effect] = prepared.scheduled_finality_effects() else {
                panic!("1010 非终态经济截点必须形成唯一成交效果")
            };
            assert!(lifecycle_effect.settlement_feedbacks.is_empty());
            let trade_effect = &lifecycle_effect.trade_feedback;
            assert_eq!(trade_effect.trade_id, "recovery-fill-trade");
            assert_eq!(trade_effect.platform_order_id, "recovery-fill-order");
            assert_eq!(trade_effect.condition_id, fill.condition_id);
            assert_eq!(trade_effect.token_id, fill.token_id);
            assert_eq!(trade_effect.platform_event_at_utc_ms, 1_010);
            assert_eq!(trade_effect.finality_state, TradeFinalityStateV1::Mined);
            assert_eq!(
                trade_effect.economic_disposition,
                TradeEconomicDispositionStateV1::PendingApplied
            );
            assert_eq!(trade_effect.liquidity_role, SimLiquidityRoleV1::Taker);
            assert_eq!(trade_effect.base_units, 1_000_000);
            assert_eq!(trade_effect.quote_units, 400_000);
            assert_eq!(trade_effect.immediate_taker_fee_units, 10_000);
            let economic_delta = trade_effect
                .account_delta
                .as_deref()
                .expect("经济截点必须形成账户变化");
            assert_eq!(economic_delta.account_transition_seq, 5);
            assert_eq!(
                economic_delta.reason,
                contracts::PlatformAccountDeltaReasonV1::FillFinalityEconomicApplied
            );
            assert_eq!(economic_delta.platform_effective_at_utc_ms, 1_010);
            assert_eq!(
                decimal(&economic_delta.balance_delta.cash_pusd_delta.0),
                decimal("-0.41")
            );
            assert_eq!(
                decimal(&economic_delta.balance_delta.available_pusd_delta.0),
                Decimal::ZERO
            );
            assert_eq!(
                decimal(
                    &economic_delta
                        .balance_delta
                        .principal_encumbrance_pusd_delta
                        .0
                ),
                decimal("-0.4")
            );
            assert_eq!(
                decimal(
                    &economic_delta
                        .balance_delta
                        .immediate_taker_fee_encumbrance_pusd_delta
                        .0
                ),
                decimal("-0.01")
            );
            let [outcome_delta] = economic_delta
                .balance_delta
                .outcome_balance_deltas
                .as_slice()
            else {
                panic!("taker 经济截点必须形成唯一结果代币变化")
            };
            assert_eq!(outcome_delta.token_id.0, fill.token_id);
            assert_eq!(decimal(&outcome_delta.balance_shares_delta.0), Decimal::ONE);
            assert_eq!(
                trade_effect.economic_application_delta.as_deref(),
                Some(economic_delta)
            );
            let same_time_gtd_delta = prepared.gtd_expiration_effects()[0]
                .account_delta
                .as_deref()
                .expect("同刻 GTD 必须先形成释放变化");
            assert_eq!(same_time_gtd_delta.account_transition_seq, 4);
            assert_eq!(
                economic_delta.predecessor,
                contracts::PlatformAccountDeltaPredecessorV1::AccountDelta {
                    delta_id: same_time_gtd_delta.delta_id.clone(),
                    delta_sha256: same_time_gtd_delta.delta_sha256.clone(),
                    account_transition_seq: same_time_gtd_delta.account_transition_seq,
                }
            );
            let later_gtd_delta = prepared.gtd_expiration_effects()[1]
                .account_delta
                .as_deref()
                .expect("1015 GTD 必须形成释放变化");
            assert_eq!(later_gtd_delta.account_transition_seq, 6);
            assert_eq!(
                later_gtd_delta.predecessor,
                contracts::PlatformAccountDeltaPredecessorV1::AccountDelta {
                    delta_id: economic_delta.delta_id.clone(),
                    delta_sha256: economic_delta.delta_sha256.clone(),
                    account_transition_seq: economic_delta.account_transition_seq,
                }
            );
            assert_eq!(prepared.finality.schedule_removals.len(), 1);
            assert!(prepared.finality.schedule_removals.contains(
                &ScheduledFinalityTransitionKeyV1 {
                    platform_event_at_utc_ms: 1_010,
                    fill_economic_delta_identity: "recovery-fill-trade".to_owned(),
                    path_ordinal: 1,
                }
            ));
            assert_eq!(prepared.finality.schedule_insertions.len(), 1);
            assert!(prepared.finality.schedule_insertions.contains(
                &ScheduledFinalityTransitionKeyV1 {
                    platform_event_at_utc_ms: 1_020,
                    fill_economic_delta_identity: "recovery-fill-trade".to_owned(),
                    path_ordinal: 2,
                }
            ));
            assert_eq!(prepared.finality.fill_replacements.len(), 1);
            assert_eq!(prepared.order_replacements.len(), 3);
            assert!(prepared.candidate_account.is_some());
            assert!(prepared.query_projection_changed());
        }

        assert_eq!(state, baseline, "SQLite 事务前 drop 必须保持 live 零变化");
        assert_eq!(execution_state_clone_count_for_tests(), 0);

        let prepared = state
            .prepare_execution_drive_delta_v2(1_015)
            .expect("重新准备可提交的 GTD 与最终性候选");
        let expected_trade_effect = prepared.scheduled_finality_effects()[0].clone();
        let (): () = prepared
            .into_committable()
            .expect("非终态候选必须可提交")
            .commit();
        let committed_fill = state
            .fills
            .get("recovery-fill-trade")
            .expect("提交后成交仍存在");
        assert_eq!(
            committed_fill.finality_state,
            Some(TradeFinalityStateV1::Mined)
        );
        assert_eq!(
            committed_fill.economic_disposition(),
            TradeEconomicDispositionStateV1::PendingApplied
        );
        assert_eq!(
            committed_fill.economic_application_delta.as_deref(),
            expected_trade_effect
                .trade_feedback
                .account_delta
                .as_deref()
        );
        let committed_order = state
            .orders
            .get("recovery-fill-order")
            .expect("提交后成交所属订单仍存在");
        assert_eq!(committed_order.immediate_taker_fee_encumbered_units, 0);
        assert_eq!(committed_order.last_updated_at_utc_ms, 1_010);
        assert!(
            !state
                .gtd_deadlines
                .contains(&(1_010, same_time_gtd_order_id))
        );
        assert!(!state.gtd_deadlines.contains(&(1_015, later_gtd_order_id)));
        assert!(
            state
                .scheduled_finality
                .contains(&ScheduledFinalityTransitionKeyV1 {
                    platform_event_at_utc_ms: 1_020,
                    fill_economic_delta_identity: "recovery-fill-trade".to_owned(),
                    path_ordinal: 2,
                })
        );
        assert_eq!(state.gtd_advanced_through_utc_ms, Some(1_015));
        assert_eq!(state.finality_advanced_through_utc_ms, Some(1_010));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_finality_inserted_schedule_iteration_is_linear() {
        const FILL_COUNT: usize = 128;
        const FIRST_MATCHED_AT: i64 = 10_000;
        const MATCHED_AT_STRIDE_MS: i64 = 100;
        const RETRYING_OFFSET_MS: i64 = 10;
        const MINED_OFFSET_MS: i64 = 20;
        const CONFIRMED_OFFSET_MS: i64 = 1_000_000;
        const FILL_BASE_UNITS: u128 = 1_000;
        const FILL_QUOTE_UNITS: u128 = 400;

        let (mut state, _, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let policy = crate::finality::policy_for_tests(
            "drive-finality-linear-inserted-schedule",
            &[
                (TradeFinalityStateV1::Matched, 0),
                (TradeFinalityStateV1::Retrying, RETRYING_OFFSET_MS),
                (TradeFinalityStateV1::Mined, MINED_OFFSET_MS),
                (TradeFinalityStateV1::Confirmed, CONFIRMED_OFFSET_MS),
            ],
            2,
            Vec::new(),
        );
        let matched_base_units = FILL_BASE_UNITS
            .checked_mul(u128::try_from(FILL_COUNT).expect("测试成交数"))
            .expect("测试成交 base 总量");
        let matched_quote_units = FILL_QUOTE_UNITS
            .checked_mul(u128::try_from(FILL_COUNT).expect("测试成交数"))
            .expect("测试成交 quote 总量");
        let plan = QueueMatchPlanV1 {
            fills: (0..FILL_COUNT)
                .map(|index| TakerFillSliceV1 {
                    segment_id: Sha256Hex(format!("{:064x}", index + 1)),
                    price_pusd_per_share: decimal("0.4"),
                    base_units: FILL_BASE_UNITS,
                    quote_units: FILL_QUOTE_UNITS,
                })
                .collect(),
            matched_base_units,
            used_quote_units: matched_quote_units,
        };
        let accepted_base_units = matched_base_units
            .checked_add(FILL_BASE_UNITS)
            .expect("测试部分成交 accepted base");
        let accepted_quote_units = matched_quote_units
            .checked_add(FILL_QUOTE_UNITS)
            .expect("测试部分成交 accepted quote");
        let (order, mut fills) = new_order_delta_records(
            "drive-finality-linear-shared-gtd",
            &condition_id,
            &token_id,
            SimOrderKindStateV1::Gtd {
                expires_at_utc_ms: 2_000_000,
            },
            decimal("0.4"),
            Some(accepted_base_units),
            accepted_quote_units,
            &plan,
            &vec![0; FILL_COUNT],
        );
        let platform_order_id = order.platform_order_id.clone();
        for (index, fill) in fills.iter_mut().enumerate() {
            fill.matched_at_utc_ms = FIRST_MATCHED_AT
                + i64::try_from(index).expect("测试成交索引") * MATCHED_AT_STRIDE_MS;
        }
        let trade_ids = fills
            .iter()
            .map(|fill| fill.trade_id.clone())
            .collect::<Vec<_>>();
        state
            .commit_new_order(order, fills)
            .expect("正式提交部分成交 GTD 多成交夹具");
        for trade_id in &trade_ids {
            state
                .initialize_fill_finality(policy.as_ref(), trade_id)
                .expect("正式初始化多成交最终性路径");
        }
        let cutoff = FIRST_MATCHED_AT
            + i64::try_from(FILL_COUNT - 1).expect("测试成交数") * MATCHED_AT_STRIDE_MS
            + MINED_OFFSET_MS;
        let baseline = serde_json::to_vec(&state).expect("序列化最终性线性门 drop 基线");
        reset_scheduled_finality_inserted_entry_visit_count_for_tests();

        {
            let prepared = state
                .prepare_execution_drive_delta_v2(cutoff)
                .expect("准备全部非终态最终性时点");
            assert_eq!(
                prepared.scheduled_finality_effects().len(),
                FILL_COUNT * 2,
                "每笔成交必须消费 Retrying 与 staged Mined 两个非终态时点"
            );
            assert_eq!(prepared.order_replacements.len(), 1);
            assert!(prepared.order_replacements.contains_key(&platform_order_id));
            assert_eq!(prepared.finality.schedule_insertions.len(), FILL_COUNT);
            assert!(
                prepared
                    .finality
                    .schedule_insertions
                    .iter()
                    .all(|key| key.platform_event_at_utc_ms > cutoff)
            );
            let visits = scheduled_finality_inserted_entry_visit_count_for_tests();
            assert!(
                visits >= FILL_COUNT,
                "计数必须来自真实 staged Mined BTreeSet 消费：visits={visits}, fills={FILL_COUNT}"
            );
            assert!(
                visits <= FILL_COUNT * 4 + 4,
                "staged finality BTreeSet 访问必须线性：visits={visits}, fills={FILL_COUNT}"
            );
        }

        assert_eq!(
            serde_json::to_vec(&state).expect("序列化最终性线性门 drop 后状态"),
            baseline
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_gtd_incremental_patch_stages_one_deadline_time_at_a_time() {
        let (mut state, _, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let first_order_id = insert_open_gtd_order_for_drive_delta(
            &mut state,
            "incremental-first",
            &condition_id,
            &token_id,
            decimal("0.4"),
            1_000_000,
            400_000,
            3_000,
        );
        let second_order_id = insert_open_gtd_order_for_drive_delta(
            &mut state,
            "incremental-second",
            &condition_id,
            &token_id,
            decimal("0.4"),
            1_000_000,
            400_000,
            4_000,
        );
        let baseline = serde_json::to_vec(&state).expect("序列化增量 GTD drop 基线");
        reset_execution_state_clone_count_for_tests();
        reset_asset_queue_clone_count_for_tests();
        reset_price_queue_clone_count_for_tests();

        {
            let mut patch = PreparedGtdExpirationPatchV2::prepare_incremental(&state);
            let mut order_replacements = BTreeMap::new();
            let mut candidate_account = None;
            assert_eq!(patch.candidate_next_gtd_at(&state), Some(3_000));
            let first_effects = patch
                .stage_gtd_at(
                    &state,
                    &mut order_replacements,
                    &mut candidate_account,
                    3_000,
                )
                .expect("只准备首个 GTD 时点");
            assert_eq!(first_effects.len(), 1);
            assert_eq!(first_effects[0].platform_order_id, first_order_id);
            assert_eq!(patch.candidate_next_gtd_at(&state), Some(4_000));
            assert_eq!(patch.deadline_removals.len(), 1);
            assert!(
                !patch
                    .deadline_removals
                    .contains(&(4_000, second_order_id.clone()))
            );
        }

        assert_eq!(
            serde_json::to_vec(&state).expect("序列化增量 GTD drop 后状态"),
            baseline
        );
        assert_eq!(execution_state_clone_count_for_tests(), 0);
        assert_eq!(asset_queue_clone_count_for_tests(), 0);
        assert_eq!(price_queue_clone_count_for_tests(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_gtd_distinct_deadline_iteration_is_linear() {
        const DEADLINE_COUNT: usize = 512;
        const FIRST_DEADLINE_AT: i64 = 10_000;
        let (mut state, _, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        for index in 0..DEADLINE_COUNT {
            insert_open_gtd_order_for_drive_delta(
                &mut state,
                &format!("linear-deadline-{index:04}"),
                &condition_id,
                &token_id,
                decimal("0.4"),
                10_000,
                4_000,
                FIRST_DEADLINE_AT + i64::try_from(index).expect("测试截止时间索引"),
            );
        }
        let baseline = serde_json::to_vec(&state).expect("序列化线性 GTD drop 基线");
        reset_execution_state_clone_count_for_tests();
        reset_asset_queue_clone_count_for_tests();
        reset_price_queue_clone_count_for_tests();
        reset_gtd_live_deadline_entry_visit_count_for_tests();

        {
            let mut patch = PreparedGtdExpirationPatchV2::prepare_incremental(&state);
            let mut order_replacements = BTreeMap::new();
            let mut candidate_account = None;
            for index in 0..DEADLINE_COUNT {
                let expected_at =
                    FIRST_DEADLINE_AT + i64::try_from(index).expect("测试截止时间索引");
                assert_eq!(patch.candidate_next_gtd_at(&state), Some(expected_at));
                assert_eq!(
                    patch
                        .stage_gtd_at(
                            &state,
                            &mut order_replacements,
                            &mut candidate_account,
                            expected_at,
                        )
                        .expect("逐时点准备 GTD")
                        .len(),
                    1
                );
            }
            assert_eq!(patch.candidate_next_gtd_at(&state), None);
            assert_eq!(patch.effects.len(), DEADLINE_COUNT);
            let visits = gtd_live_deadline_entry_visit_count_for_tests();
            assert!(
                visits >= DEADLINE_COUNT,
                "GTD deadline 访问计数必须来自真实 BTreeSet 迭代：visits={visits}, deadlines={DEADLINE_COUNT}"
            );
            assert!(
                visits <= DEADLINE_COUNT * 4 + 4,
                "真实 GTD deadline BTreeSet 访问必须线性：visits={visits}, deadlines={DEADLINE_COUNT}"
            );
        }

        assert_eq!(
            serde_json::to_vec(&state).expect("序列化线性 GTD drop 后状态"),
            baseline
        );
        assert_eq!(execution_state_clone_count_for_tests(), 0);
        assert_eq!(asset_queue_clone_count_for_tests(), 0);
        assert_eq!(price_queue_clone_count_for_tests(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_gtd_drop_and_commit_touch_only_due_levels() {
        let (mut state, _, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let due_order_id = insert_open_gtd_order_for_drive_delta(
            &mut state,
            "drive-due",
            &condition_id,
            &token_id,
            decimal("0.4"),
            1_000_000,
            400_000,
            3_000,
        );
        let later_order_id = insert_open_gtd_order_for_drive_delta(
            &mut state,
            "drive-later",
            &condition_id,
            &token_id,
            decimal("0.3"),
            1_000_000,
            300_000,
            5_000,
        );
        let target_asset = state
            .queues
            .asset_mut(&condition_id, &token_id)
            .expect("GTD touched-key 测试目标资产");
        for index in 0..512_u64 {
            target_asset.bids.insert(
                Decimal::new(i64::try_from(index + 1).expect("测试价格"), 6),
                PriceQueueV1 {
                    segments: VecDeque::from([QueueSegmentV1::External(
                        ExternalSyntheticMakerSegmentV1 {
                            segment_id: Sha256Hex(format!("{index:064x}")),
                            total_base_units: 10,
                            total_quote_units: 1,
                            consumed_base_units: 0,
                        },
                    )]),
                    account_taker_depletion_debt_units: 0,
                },
            );
        }
        let baseline = serde_json::to_vec(&state).expect("序列化 GTD drop 基线");
        reset_execution_state_clone_count_for_tests();
        reset_asset_queue_clone_count_for_tests();
        reset_price_queue_clone_count_for_tests();
        let prepared = state
            .prepare_execution_drive_delta_v2(3_000)
            .expect("准备 GTD touched-key patch");
        assert!(prepared.has_state_change());
        assert_eq!(prepared.gtd_expiration_effects().len(), 1);
        let effect = &prepared.gtd_expiration_effects()[0];
        assert_eq!(effect.platform_order_id, due_order_id);
        assert_eq!(effect.pending_base_units_before, 0);
        assert_eq!(effect.open_base_units_before, 1_000_000);
        assert_eq!(effect.released_open_quote_units, 400_000);
        let account_delta = effect
            .account_delta
            .as_ref()
            .expect("GTD 开放本金释放必须形成账户变化");
        assert_eq!(account_delta.account_transition_seq, 3);
        assert_eq!(
            account_delta.reason,
            contracts::PlatformAccountDeltaReasonV1::GtdExpirationEncumbranceReleased
        );
        assert_eq!(account_delta.platform_effective_at_utc_ms, 3_000);
        assert!(prepared.query_projection_changed());
        drop(prepared);
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化 GTD drop 后状态"),
            baseline
        );
        assert_eq!(execution_state_clone_count_for_tests(), 0);
        assert_eq!(asset_queue_clone_count_for_tests(), 0);
        assert_eq!(price_queue_clone_count_for_tests(), 1);

        reset_price_queue_clone_count_for_tests();
        let prepared = state
            .prepare_execution_drive_delta_v2(3_000)
            .expect("重新准备 GTD touched-key patch");
        assert_eq!(prepared.gtd_expiration_effects().len(), 1);
        let (): () = prepared
            .into_committable()
            .expect("GTD 候选必须可提交")
            .commit();
        let due_order = &state.orders[&due_order_id];
        assert_eq!(due_order.expired_base_units, 1_000_000);
        assert_eq!(due_order.expired_released_quote_units, 400_000);
        assert_eq!(due_order.expired_at_utc_ms, Some(3_000));
        assert_eq!(due_order.open_base_units, 0);
        assert_eq!(due_order.open_quote_units, 0);
        assert_eq!(
            state
                .queues
                .remaining_account_order_base_units(&due_order_id),
            None
        );
        assert_eq!(
            state
                .account_overlay
                .quantity(&condition_id, &token_id, BookSide::Bid, decimal("0.4")),
            Decimal::ZERO
        );
        assert!(!state.gtd_deadlines.contains(&(3_000, due_order_id)));
        assert!(state.gtd_deadlines.contains(&(5_000, later_order_id)));
        assert_eq!(state.gtd_advanced_through_utc_ms, Some(3_000));
        assert_eq!(execution_state_clone_count_for_tests(), 0);
        assert_eq!(asset_queue_clone_count_for_tests(), 0);
        assert_eq!(price_queue_clone_count_for_tests(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_gtd_reads_multiple_same_level_expirations_in_order() {
        let (mut state, _, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let first_order_id = insert_open_gtd_order_for_drive_delta(
            &mut state,
            "same-level-a",
            &condition_id,
            &token_id,
            decimal("0.4"),
            1_000_000,
            400_000,
            3_000,
        );
        let second_order_id = insert_open_gtd_order_for_drive_delta(
            &mut state,
            "same-level-b",
            &condition_id,
            &token_id,
            decimal("0.4"),
            1_000_000,
            400_000,
            3_000,
        );
        reset_price_queue_clone_count_for_tests();
        let prepared = state
            .prepare_execution_drive_delta_v2(3_000)
            .expect("准备同层多个 GTD 到期");
        assert_eq!(
            prepared
                .gtd_expiration_effects()
                .iter()
                .map(|effect| effect.platform_order_id.as_str())
                .collect::<Vec<_>>(),
            vec![first_order_id.as_str(), second_order_id.as_str()]
        );
        for effect in prepared.gtd_expiration_effects() {
            assert_eq!(effect.pending_base_units_before, 0);
            assert_eq!(effect.open_base_units_before, 1_000_000);
            assert_eq!(effect.released_open_quote_units, 400_000);
        }
        let first_delta = prepared.gtd_expiration_effects()[0]
            .account_delta
            .as_ref()
            .expect("首个同层 GTD 到期必须形成账户变化");
        let second_delta = prepared.gtd_expiration_effects()[1]
            .account_delta
            .as_ref()
            .expect("第二个同层 GTD 到期必须形成账户变化");
        assert_eq!(first_delta.account_transition_seq, 3);
        assert_eq!(second_delta.account_transition_seq, 4);
        assert_eq!(
            second_delta.predecessor,
            contracts::PlatformAccountDeltaPredecessorV1::AccountDelta {
                delta_id: first_delta.delta_id.clone(),
                delta_sha256: first_delta.delta_sha256.clone(),
                account_transition_seq: first_delta.account_transition_seq,
            }
        );
        prepared
            .into_committable()
            .expect("同层 GTD 候选必须可提交")
            .commit();
        for platform_order_id in [&first_order_id, &second_order_id] {
            let order = &state.orders[platform_order_id];
            assert_eq!(order.expired_base_units, 1_000_000);
            assert_eq!(order.expired_released_quote_units, 400_000);
            assert_eq!(order.expired_at_utc_ms, Some(3_000));
            assert_eq!(order.open_base_units, 0);
            assert_eq!(order.open_quote_units, 0);
            assert_eq!(
                state
                    .queues
                    .remaining_account_order_base_units(platform_order_id),
                None
            );
            assert!(
                !state
                    .gtd_deadlines
                    .contains(&(3_000, platform_order_id.to_string()))
            );
        }
        assert_eq!(
            state
                .account_overlay
                .quantity(&condition_id, &token_id, BookSide::Bid, decimal("0.4")),
            Decimal::ZERO
        );
        assert_eq!(state.gtd_advanced_through_utc_ms, Some(3_000));
        assert_eq!(price_queue_clone_count_for_tests(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_caller_cancel_reads_same_time_gtd_after_image() {
        let (mut state, _, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let platform_order_id = insert_open_gtd_order_for_drive_delta(
            &mut state,
            "drive-cancel-same-time-gtd",
            &condition_id,
            &token_id,
            decimal("0.4"),
            1_000_000,
            400_000,
            3_000,
        );
        let cancel_identity = Sha256Hex("c".repeat(64));
        let baseline = serde_json::to_vec(&state).expect("序列化同刻 GTD 撤单 drop 基线");
        reset_execution_state_clone_count_for_tests();
        reset_asset_queue_clone_count_for_tests();
        reset_price_queue_clone_count_for_tests();

        let prepared = state
            .prepare_execution_drive_delta_v2(3_000)
            .expect("准备同刻 GTD 优先候选");
        let (prepared, disposition) = prepared
            .prepare_caller_cancel_remainder(&platform_order_id, 3_000, &cancel_identity)
            .expect("同刻撤单必须读取 GTD after-image");
        assert_eq!(
            disposition,
            CancelOrderStateDispositionV1::NoCancellableRemainder
        );
        let candidate = prepared
            .candidate_order(&platform_order_id)
            .expect("同刻 GTD 候选订单必须存在");
        assert_eq!(candidate.expired_base_units, 1_000_000);
        assert_eq!(candidate.expired_released_quote_units, 400_000);
        assert_eq!(candidate.expired_at_utc_ms, Some(3_000));
        assert_eq!(candidate.open_base_units, 0);
        assert_eq!(candidate.open_quote_units, 0);
        assert!(candidate.caller_cancel_effect_identity.is_none());
        drop(prepared);

        assert_eq!(
            serde_json::to_vec(&state).expect("序列化同刻 GTD 撤单 drop 后状态"),
            baseline
        );
        assert_eq!(execution_state_clone_count_for_tests(), 0);
        assert_eq!(asset_queue_clone_count_for_tests(), 0);
        assert_eq!(price_queue_clone_count_for_tests(), 1);

        reset_price_queue_clone_count_for_tests();
        let prepared = state
            .prepare_execution_drive_delta_v2(3_000)
            .expect("重新准备同刻 GTD 优先候选");
        let (prepared, disposition) = prepared
            .prepare_caller_cancel_remainder(&platform_order_id, 3_000, &cancel_identity)
            .expect("重新准备同刻撤单 after-image");
        assert_eq!(
            disposition,
            CancelOrderStateDispositionV1::NoCancellableRemainder
        );
        prepared
            .into_committable()
            .expect("同刻 GTD 优先候选必须可提交")
            .commit();
        let committed = state
            .orders
            .get(&platform_order_id)
            .expect("同刻 GTD 提交后订单必须存在");
        assert_eq!(committed.expired_base_units, 1_000_000);
        assert_eq!(committed.open_base_units, 0);
        assert_eq!(committed.open_quote_units, 0);
        assert!(committed.caller_cancel_effect_identity.is_none());
        assert_eq!(
            state
                .queues
                .remaining_account_order_base_units(&platform_order_id),
            None
        );
        assert!(!state.gtd_deadlines.contains(&(3_000, platform_order_id)));
        assert_eq!(price_queue_clone_count_for_tests(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_caller_cancel_batch_is_sparse_and_failure_consumes_delta() {
        let (mut state, _, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let first_order_id = insert_open_gtd_order_for_drive_delta(
            &mut state,
            "drive-cancel-batch-first",
            &condition_id,
            &token_id,
            decimal("0.4"),
            1_000_000,
            400_000,
            5_000,
        );
        let second_order_id = insert_open_gtd_order_for_drive_delta(
            &mut state,
            "drive-cancel-batch-second",
            &condition_id,
            &token_id,
            decimal("0.4"),
            500_000,
            200_000,
            5_000,
        );
        let target_asset = state
            .queues
            .asset_mut(&condition_id, &token_id)
            .expect("caller-cancel sparse 测试目标资产");
        for index in 0..512_u64 {
            target_asset.bids.insert(
                Decimal::new(i64::try_from(index + 1).expect("测试价格"), 6),
                PriceQueueV1 {
                    segments: VecDeque::from([QueueSegmentV1::External(
                        ExternalSyntheticMakerSegmentV1 {
                            segment_id: Sha256Hex(format!("{index:064x}")),
                            total_base_units: 10,
                            total_quote_units: 1,
                            consumed_base_units: 0,
                        },
                    )]),
                    account_taker_depletion_debt_units: 0,
                },
            );
        }
        let first_identity = Sha256Hex("1".repeat(64));
        let second_identity = Sha256Hex("2".repeat(64));
        let conflicting_identity = Sha256Hex("f".repeat(64));
        let baseline = serde_json::to_vec(&state).expect("序列化批撤 sparse 基线");

        reset_execution_state_clone_count_for_tests();
        reset_asset_queue_clone_count_for_tests();
        reset_price_queue_clone_count_for_tests();
        let prepared = state
            .prepare_execution_drive_delta_v2(1_500)
            .expect("准备批撤失败消费候选");
        let (prepared, first_disposition) = prepared
            .prepare_caller_cancel_remainder(&first_order_id, 1_500, &conflicting_identity)
            .expect("准备批撤首项目标");
        assert!(matches!(
            first_disposition,
            CancelOrderStateDispositionV1::Canceled(_)
        ));
        let error = match prepared.prepare_caller_cancel_remainder(
            &second_order_id,
            1_500,
            &conflicting_identity,
        ) {
            Err(error) => error,
            Ok(_) => panic!("同一效果身份的不同撤单正文必须冲突并消费整份候选"),
        };
        assert!(matches!(
            error,
            MatchingStateErrorV1::Account(
                PlatformAccountErrorV1::TransitionIdentityConflict(identity)
            ) if identity == conflicting_identity.0
        ));
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化批撤失败后 live 状态"),
            baseline
        );
        assert_eq!(execution_state_clone_count_for_tests(), 0);
        assert_eq!(asset_queue_clone_count_for_tests(), 0);
        assert_eq!(price_queue_clone_count_for_tests(), 1);

        reset_price_queue_clone_count_for_tests();
        let prepared = state
            .prepare_execution_drive_delta_v2(1_500)
            .expect("重新准备批撤 sparse 候选");
        let (prepared, first_disposition) = prepared
            .prepare_caller_cancel_remainder(&first_order_id, 1_500, &first_identity)
            .expect("准备首个批撤目标");
        let (prepared, second_disposition) = prepared
            .prepare_caller_cancel_remainder(&second_order_id, 1_500, &second_identity)
            .expect("准备第二个批撤目标");
        let CancelOrderStateDispositionV1::Canceled(first_effect) = first_disposition else {
            panic!("首个批撤目标必须成功")
        };
        let CancelOrderStateDispositionV1::Canceled(second_effect) = second_disposition else {
            panic!("第二个批撤目标必须成功")
        };
        let first_delta = first_effect
            .account_delta
            .as_deref()
            .expect("首个批撤目标必须释放本金占用");
        let second_delta = second_effect
            .account_delta
            .as_deref()
            .expect("第二个批撤目标必须释放本金占用");
        assert_eq!(first_delta.account_transition_seq, 3);
        assert_eq!(second_delta.account_transition_seq, 4);
        assert_eq!(
            second_delta.predecessor,
            contracts::PlatformAccountDeltaPredecessorV1::AccountDelta {
                delta_id: first_delta.delta_id.clone(),
                delta_sha256: first_delta.delta_sha256.clone(),
                account_transition_seq: first_delta.account_transition_seq,
            }
        );
        assert_eq!(prepared.gtd.bid_queue_after_values.len(), 1);
        assert_eq!(prepared.gtd.overlay_after_values.len(), 1);
        assert_eq!(prepared.gtd.deadline_removals.len(), 2);
        assert_eq!(prepared.order_replacements.len(), 2);
        assert!(prepared.candidate_account.is_some());
        assert_eq!(price_queue_clone_count_for_tests(), 1);
        drop(prepared);
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化批撤 sparse drop 后状态"),
            baseline
        );

        reset_price_queue_clone_count_for_tests();
        let prepared = state
            .prepare_execution_drive_delta_v2(1_500)
            .expect("重新准备可提交批撤候选");
        let (prepared, _) = prepared
            .prepare_caller_cancel_remainder(&first_order_id, 1_500, &first_identity)
            .expect("重新准备首个可提交批撤目标");
        let (prepared, _) = prepared
            .prepare_caller_cancel_remainder(&second_order_id, 1_500, &second_identity)
            .expect("重新准备第二个可提交批撤目标");
        prepared
            .into_committable()
            .expect("批撤 sparse 候选必须可提交")
            .commit();
        for (platform_order_id, identity, canceled_base_units, released_quote_units) in [
            (&first_order_id, &first_identity, 1_000_000, 400_000),
            (&second_order_id, &second_identity, 500_000, 200_000),
        ] {
            let order = state
                .orders
                .get(platform_order_id)
                .expect("批撤提交后目标订单必须存在");
            assert_eq!(order.canceled_base_units, canceled_base_units);
            assert_eq!(order.canceled_released_quote_units, released_quote_units);
            assert_eq!(order.canceled_at_utc_ms, Some(1_500));
            assert_eq!(order.caller_cancel_effect_identity.as_ref(), Some(identity));
            assert_eq!(order.open_base_units, 0);
            assert_eq!(order.open_quote_units, 0);
            assert_eq!(
                state
                    .queues
                    .remaining_account_order_base_units(platform_order_id),
                None
            );
            assert!(
                !state
                    .gtd_deadlines
                    .contains(&(5_000, (*platform_order_id).clone()))
            );
        }
        assert_eq!(
            state
                .account_overlay
                .quantity(&condition_id, &token_id, BookSide::Bid, decimal("0.4")),
            Decimal::ZERO
        );
        assert_eq!(state.gtd_advanced_through_utc_ms, Some(1_500));
        assert_eq!(execution_state_clone_count_for_tests(), 0);
        assert_eq!(asset_queue_clone_count_for_tests(), 0);
        assert_eq!(price_queue_clone_count_for_tests(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_caller_cancel_preserves_same_order_finality_after_image() {
        let (mut state, _, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let policy = crate::finality::policy_for_tests(
            "drive-cancel-same-order-finality",
            &[
                (TradeFinalityStateV1::Matched, 0),
                (TradeFinalityStateV1::Mined, 10),
                (TradeFinalityStateV1::Confirmed, 1_000),
            ],
            1,
            Vec::new(),
        );
        let plan = QueueMatchPlanV1 {
            fills: vec![TakerFillSliceV1 {
                segment_id: Sha256Hex("a".repeat(64)),
                price_pusd_per_share: decimal("0.4"),
                base_units: 1_000_000,
                quote_units: 400_000,
            }],
            matched_base_units: 1_000_000,
            used_quote_units: 400_000,
        };
        let (order, fills) = new_order_delta_records(
            "drive-cancel-same-order-finality",
            &condition_id,
            &token_id,
            SimOrderKindStateV1::Gtd {
                expires_at_utc_ms: 5_000,
            },
            decimal("0.4"),
            Some(2_000_000),
            800_000,
            &plan,
            &[10_000],
        );
        let platform_order_id = order.platform_order_id.clone();
        let trade_id = fills[0].trade_id.clone();
        state
            .commit_new_order(order, fills)
            .expect("正式提交部分成交 GTD 撤单夹具");
        state
            .initialize_fill_finality(policy.as_ref(), &trade_id)
            .expect("正式初始化部分成交 GTD 最终性");
        let cancel_identity = Sha256Hex("d".repeat(64));
        let baseline = serde_json::to_vec(&state).expect("序列化同订单最终性撤单基线");
        reset_execution_state_clone_count_for_tests();
        reset_asset_queue_clone_count_for_tests();
        reset_price_queue_clone_count_for_tests();

        let prepared = state
            .prepare_execution_drive_delta_v2(1_010)
            .expect("准备同订单最终性与撤单候选");
        let [finality_effect] = prepared.scheduled_finality_effects() else {
            panic!("Mined 经济截点必须形成唯一最终性效果")
        };
        let finality_delta = finality_effect
            .trade_feedback
            .account_delta
            .as_ref()
            .expect("Mined 经济截点必须形成账户变化")
            .clone();
        let (prepared, disposition) = prepared
            .prepare_caller_cancel_remainder(&platform_order_id, 1_010, &cancel_identity)
            .expect("撤单必须读取同订单最终性 after-image");
        let CancelOrderStateDispositionV1::Canceled(cancel_effect) = disposition else {
            panic!("同订单最终性后的开放余量必须可撤")
        };
        let cancel_delta = cancel_effect
            .account_delta
            .as_deref()
            .expect("同订单最终性后的撤单必须释放开放本金");
        assert_eq!(finality_delta.account_transition_seq, 2);
        assert_eq!(cancel_delta.account_transition_seq, 3);
        assert_eq!(
            cancel_delta.predecessor,
            contracts::PlatformAccountDeltaPredecessorV1::AccountDelta {
                delta_id: finality_delta.delta_id.clone(),
                delta_sha256: finality_delta.delta_sha256.clone(),
                account_transition_seq: finality_delta.account_transition_seq,
            }
        );
        let candidate_order = prepared
            .candidate_order(&platform_order_id)
            .expect("同订单最终性撤单候选必须保留订单");
        assert_eq!(candidate_order.immediate_taker_fee_encumbered_units, 0);
        assert_eq!(candidate_order.canceled_base_units, 1_000_000);
        assert_eq!(candidate_order.canceled_released_quote_units, 400_000);
        assert_eq!(candidate_order.open_base_units, 0);
        assert_eq!(candidate_order.open_quote_units, 0);
        assert_eq!(
            candidate_order.caller_cancel_effect_identity.as_ref(),
            Some(&cancel_identity)
        );
        let candidate_fill = prepared
            .candidate_fill(&trade_id)
            .expect("同订单最终性撤单候选必须保留成交");
        assert_eq!(
            candidate_fill.finality_state,
            Some(TradeFinalityStateV1::Mined)
        );
        assert_eq!(candidate_fill.finality_path_ordinal, Some(1));
        drop(prepared);
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化同订单最终性撤单 drop 后状态"),
            baseline
        );
        assert_eq!(execution_state_clone_count_for_tests(), 0);
        assert_eq!(asset_queue_clone_count_for_tests(), 0);
        assert_eq!(price_queue_clone_count_for_tests(), 1);

        reset_price_queue_clone_count_for_tests();
        let prepared = state
            .prepare_execution_drive_delta_v2(1_010)
            .expect("重新准备同订单最终性与撤单候选");
        let (prepared, _) = prepared
            .prepare_caller_cancel_remainder(&platform_order_id, 1_010, &cancel_identity)
            .expect("重新准备同订单最终性撤单");
        prepared
            .into_committable()
            .expect("同订单最终性撤单候选必须可提交")
            .commit();
        let committed_order = state
            .orders
            .get(&platform_order_id)
            .expect("同订单最终性撤单提交后订单必须存在");
        assert_eq!(committed_order.immediate_taker_fee_encumbered_units, 0);
        assert_eq!(committed_order.canceled_base_units, 1_000_000);
        assert_eq!(committed_order.open_base_units, 0);
        assert_eq!(committed_order.open_quote_units, 0);
        let committed_fill = state
            .fills
            .get(&trade_id)
            .expect("同订单最终性撤单提交后成交必须存在");
        assert_eq!(
            committed_fill.finality_state,
            Some(TradeFinalityStateV1::Mined)
        );
        assert_eq!(committed_fill.finality_path_ordinal, Some(1));
        assert_eq!(
            committed_fill.economic_application_delta.as_deref(),
            Some(finality_delta.as_ref())
        );
        assert_eq!(
            state
                .queues
                .remaining_account_order_base_units(&platform_order_id),
            None
        );
        assert!(!state.gtd_deadlines.contains(&(5_000, platform_order_id)));
        assert_eq!(state.gtd_advanced_through_utc_ms, Some(1_010));
        assert_eq!(state.finality_advanced_through_utc_ms, Some(1_010));
        assert_eq!(price_queue_clone_count_for_tests(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_gtd_no_due_drop_is_read_only_and_commit_advances_only_watermark()
     {
        let (mut state, _, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let later_order_id = insert_open_gtd_order_for_drive_delta(
            &mut state,
            "not-yet-due",
            &condition_id,
            &token_id,
            decimal("0.4"),
            1_000_000,
            400_000,
            5_000,
        );
        let baseline = state.clone();

        let prepared = state
            .prepare_execution_drive_delta_v2(1_500)
            .expect("准备尚未到期的 GTD 水位推进");
        assert!(prepared.gtd_expiration_effects().is_empty());
        assert!(!prepared.query_projection_changed());
        assert!(prepared.has_state_change());
        assert_eq!(prepared.candidate_account(), &baseline.account);
        drop(prepared);
        assert_eq!(state, baseline);

        let prepared = state
            .prepare_execution_drive_delta_v2(1_500)
            .expect("重新准备尚未到期的 GTD 水位推进");
        assert!(prepared.gtd_expiration_effects().is_empty());
        prepared
            .into_committable()
            .expect("空效果水位候选必须可提交")
            .commit();
        assert!(state.gtd_deadlines.contains(&(5_000, later_order_id)));
        assert_eq!(state.gtd_advanced_through_utc_ms, Some(1_500));
        let mut expected = baseline;
        expected.gtd_advanced_through_utc_ms = Some(1_500);
        assert_eq!(state, expected);

        let after_first_commit = state.clone();
        let repeated = state
            .prepare_execution_drive_delta_v2(1_500)
            .expect("相同 GTD cutoff 必须可幂等重放");
        assert!(repeated.gtd_expiration_effects().is_empty());
        assert!(!repeated.query_projection_changed());
        assert!(!repeated.has_state_change());
        repeated
            .into_committable()
            .expect("重复空效果水位候选必须可提交")
            .commit();
        assert_eq!(state, after_first_commit);

        let before_regression = state.clone();
        match state.prepare_execution_drive_delta_v2(1_499) {
            Err(error) => assert_eq!(error, MatchingStateErrorV1::LogicalTimeRegression),
            Ok(_) => panic!("更小 GTD cutoff 必须拒绝逻辑时间回退"),
        }
        assert_eq!(state, before_regression);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn completed_maker_fill_removes_the_formally_registered_gtd_deadline() {
        let (mut state, rules, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let directory =
            SessionMarketDirectoryV1::try_from_scenario(&crate::scenario::scenario_for_sim_tests())
                .expect("GTD maker 成交测试目录");
        let platform_order_id = insert_open_gtd_order_for_drive_delta(
            &mut state,
            "maker-filled-gtd",
            &condition_id,
            &token_id,
            decimal("0.4"),
            1_000_000,
            400_000,
            1_900,
        );
        assert!(
            state
                .gtd_deadlines
                .contains(&(1_900, platform_order_id.clone()))
        );

        let addition = crate::task23_tests::bid_transition(1_100, 1_200, 1, "0.4", "0", "1");
        state
            .apply_authorized_market_events(
                &directory,
                1_100,
                &[addition],
                &crate::finality::successful_policy_for_tests(),
                &rules
                    .snapshot()
                    .project_account_rules
                    .lifecycle_account_behavior
                    .value,
                &BTreeSet::new(),
            )
            .expect("正式价位增加排在账户 GTD 后方");
        let depletion = crate::task23_tests::bid_transition(1_101, 1_201, 2, "0.4", "1", "0");
        state
            .apply_authorized_market_events(
                &directory,
                1_101,
                &[depletion],
                &crate::finality::successful_policy_for_tests(),
                &rules
                    .snapshot()
                    .project_account_rules
                    .lifecycle_account_behavior
                    .value,
                &BTreeSet::new(),
            )
            .expect("正式价位消耗完全成交账户 GTD");

        let order = &state.orders[&platform_order_id];
        assert_eq!(order.open_base_units, 0);
        assert_eq!(order.pending_maker_depletion_base_units, 0);
        assert!(
            !state.gtd_deadlines.contains(&(1_900, platform_order_id)),
            "完全成交后不得保留恢复合同拒绝的无业务义务 GTD 截止任务"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_new_order_v2_fok_partial_drop_then_fak_sees_full_liquidity() {
        let (mut state, rules, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let baseline = state.clone();
        let no_fill_fak = state
            .prepare_new_order_taker_buy_by_quote_v2(
                &condition_id,
                &token_id,
                decimal("0.4"),
                300_000,
            )
            .expect("准备零成交 FAK 候选");
        assert!(no_fill_fak.plan().fills.is_empty());
        let (order, fills) = new_order_delta_records(
            "no-fill-fak",
            &condition_id,
            &token_id,
            SimOrderKindStateV1::Fak,
            decimal("0.4"),
            None,
            300_000,
            no_fill_fak.plan(),
            &[],
        );
        let Err(error) = no_fill_fak.finish_new_order(
            order,
            fills,
            &crate::finality::successful_policy_for_tests(),
            &rules
                .snapshot()
                .project_account_rules
                .lifecycle_account_behavior
                .value,
        ) else {
            panic!("FAK 零成交必须在形成订单和账户变化前拒绝")
        };
        assert_eq!(error, MatchingStateErrorV1::QueuePredecessorConflict);
        assert_eq!(state, baseline);

        let fok = state
            .prepare_new_order_taker_buy_by_quote_v2(
                &condition_id,
                &token_id,
                decimal("0.6"),
                800_000,
            )
            .expect("准备 FOK 部分成交候选");
        assert_eq!(fok.plan().used_quote_units, 500_000);
        drop(fok);

        let fak = state
            .prepare_new_order_taker_buy_by_quote_v2(
                &condition_id,
                &token_id,
                decimal("0.6"),
                300_000,
            )
            .expect("FOK 丢弃后准备 FAK 候选");
        assert_eq!(fak.plan().used_quote_units, 300_000);
        drop(fak);
        assert_eq!(state, baseline);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_new_order_v2_rejects_target_kind_mismatch_without_live_mutation() {
        let (mut state, rules, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let baseline = state.clone();
        let policy = crate::finality::successful_policy_for_tests();
        let lifecycle_rule = &rules
            .snapshot()
            .project_account_rules
            .lifecycle_account_behavior
            .value;

        let quote_builder = state
            .prepare_new_order_taker_buy_by_quote_v2(
                &condition_id,
                &token_id,
                decimal("0.6"),
                500_000,
            )
            .expect("准备 quote + GTC 错配候选");
        let (quote_order, quote_fills) = new_order_delta_records(
            "quote-gtc-mismatch",
            &condition_id,
            &token_id,
            SimOrderKindStateV1::Gtc,
            decimal("0.6"),
            None,
            500_000,
            quote_builder.plan(),
            &[0],
        );
        let Err(error) =
            quote_builder.finish_new_order(quote_order, quote_fills, &policy, lifecycle_rule)
        else {
            panic!("quote builder 不得提交 GTC")
        };
        assert_eq!(error, MatchingStateErrorV1::QueuePredecessorConflict);
        assert_eq!(state, baseline);

        let base_builder = state
            .prepare_new_order_taker_buy_by_base_v2(
                &condition_id,
                &token_id,
                decimal("0.5"),
                1_000_000,
                500_000,
            )
            .expect("准备 base + FOK 错配候选");
        let (base_order, base_fills) = new_order_delta_records(
            "base-fok-mismatch",
            &condition_id,
            &token_id,
            SimOrderKindStateV1::Fok,
            decimal("0.5"),
            Some(1_000_000),
            500_000,
            base_builder.plan(),
            &[0],
        );
        let Err(error) =
            base_builder.finish_new_order(base_order, base_fills, &policy, lifecycle_rule)
        else {
            panic!("base builder 不得提交 FOK")
        };
        assert_eq!(error, MatchingStateErrorV1::QueuePredecessorConflict);
        assert_eq!(state, baseline);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_new_order_v2_rejects_noncanonical_limit_quote_split_without_live_mutation() {
        let (mut state, rules, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let baseline = state.clone();
        let builder = state
            .prepare_new_order_taker_buy_by_base_v2(
                &condition_id,
                &token_id,
                decimal("0.5"),
                1_020_000,
                510_000,
            )
            .expect("准备部分成交 GTC 候选");
        assert_eq!(builder.plan().matched_base_units, 1_000_000);
        assert_eq!(builder.plan().used_quote_units, 500_000);
        let (mut order, fills) = new_order_delta_records(
            "noncanonical-limit-quote-split",
            &condition_id,
            &token_id,
            SimOrderKindStateV1::Gtc,
            decimal("0.5"),
            Some(1_020_000),
            510_000,
            builder.plan(),
            &[0],
        );
        assert_eq!(order.open_quote_units, 10_000);
        assert_eq!(order.released_quote_units, 0);
        order.open_quote_units = 0;
        order.released_quote_units = 10_000;

        let Err(error) = builder.finish_new_order(
            order,
            fills,
            &crate::finality::successful_policy_for_tests(),
            &rules
                .snapshot()
                .project_account_rules
                .lifecycle_account_behavior
                .value,
        ) else {
            panic!("LimitBase 非规范 quote 分配必须在形成业务候选前拒绝")
        };
        assert_eq!(error, MatchingStateErrorV1::QueuePredecessorConflict);
        assert_eq!(state, baseline);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_new_order_v2_late_finality_error_drops_every_candidate_component() {
        let (mut state, rules, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        state
            .queues
            .markets
            .get_mut(&condition_id)
            .expect("测试市场")
            .get_mut(&token_id)
            .expect("测试目标资产")
            .asks
            .insert(
                decimal("0.55"),
                PriceQueueV1 {
                    segments: VecDeque::from([QueueSegmentV1::External(
                        ExternalSyntheticMakerSegmentV1 {
                            segment_id: Sha256Hex("e".repeat(64)),
                            total_base_units: 1_000_000,
                            total_quote_units: 550_000,
                            consumed_base_units: 0,
                        },
                    )]),
                    account_taker_depletion_debt_units: 0,
                },
            );
        let baseline = serde_json::to_vec(&state).expect("序列化失败前真实状态");
        let account_instance_key = state.account_instance_key().clone();
        let builder = state
            .prepare_new_order_taker_buy_by_quote_v2(
                &condition_id,
                &token_id,
                decimal("0.6"),
                1_050_000,
            )
            .expect("准备多价位 FOK 候选");
        let plan = builder.plan().clone();
        assert_eq!(plan.fills.len(), 2);
        let (order, fills) = new_order_delta_records(
            "late-finality",
            &condition_id,
            &token_id,
            SimOrderKindStateV1::Fok,
            decimal("0.6"),
            None,
            1_050_000,
            &plan,
            &[1_000, 1_000],
        );
        let failing_trade_id = fills[1].trade_id.clone();
        let policy = crate::finality::policy_for_tests(
            "new-order-v2-late-finality-overflow",
            &[
                (TradeFinalityStateV1::Matched, 0),
                (TradeFinalityStateV1::Mined, 10),
                (TradeFinalityStateV1::Confirmed, 20),
            ],
            0,
            vec![(
                crate::finality::TradeFinalitySelectorV1::Fill {
                    account_instance_key,
                    fill_economic_delta_identity: failing_trade_id,
                },
                vec![
                    (TradeFinalityStateV1::Matched, 0),
                    (TradeFinalityStateV1::Failed, i64::MAX),
                ],
                None,
            )],
        );
        let lifecycle_rule = &rules
            .snapshot()
            .project_account_rules
            .lifecycle_account_behavior
            .value;

        let Err(error) = builder.finish_new_order(order, fills, &policy, lifecycle_rule) else {
            panic!("后项 finality 时间溢出必须拒绝整个候选")
        };
        assert!(
            matches!(error, MatchingStateErrorV1::Finality(message) if message.contains("时点溢出"))
        );
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化失败后的真实状态"),
            baseline
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_new_order_v2_partial_gtd_dust_drop_leaves_live_state_unchanged() {
        let (mut state, rules, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let baseline = serde_json::to_vec(&state).expect("序列化 dust 前真实状态");
        let builder = state
            .prepare_new_order_taker_buy_by_base_v2(
                &condition_id,
                &token_id,
                decimal("0.5"),
                1_005_000,
                502_500,
            )
            .expect("准备 GTD 部分成交候选");
        let plan = builder.plan().clone();
        let (order, fills) = new_order_delta_records(
            "gtd-dust-drop",
            &condition_id,
            &token_id,
            SimOrderKindStateV1::Gtd {
                expires_at_utc_ms: 3_000,
            },
            decimal("0.5"),
            Some(1_005_000),
            502_500,
            &plan,
            &[0],
        );
        let policy = crate::finality::successful_policy_for_tests();
        let lifecycle_rule = &rules
            .snapshot()
            .project_account_rules
            .lifecycle_account_behavior
            .value;
        let prepared = builder
            .finish_new_order(order, fills, &policy, lifecycle_rule)
            .expect("准备完成 remainder、finality 与 dust");

        assert_eq!(
            prepared.order().terminal_reason,
            Some(SimOrderTerminalReasonV1::VenueDustTerminal)
        );
        assert!(prepared.venue_dust_effect().is_some());
        drop(prepared);
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化 dust 丢弃后真实状态"),
            baseline
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_new_order_v2_shared_rebate_arc_distinguishes_zero_and_nonzero_fee() {
        let policy = crate::finality::successful_policy_for_tests();

        let (mut zero_fee_state, zero_fee_rules, condition_id, token_id) =
            new_order_delta_state_fixture(0).await;
        let shared_zero_fee_rebate = Arc::clone(&zero_fee_state.rebate);
        let zero_fee_rebate_pointer = Arc::as_ptr(&zero_fee_state.rebate);
        let zero_fee_rebate_records = zero_fee_state.rebate.resource_record_count();
        let builder = zero_fee_state
            .prepare_new_order_taker_buy_by_quote_v2(
                &condition_id,
                &token_id,
                decimal("0.6"),
                500_000,
            )
            .expect("共享返佣 Arc 下准备零手续费撮合");
        let plan = builder.plan().clone();
        let (order, fills) = new_order_delta_records(
            "shared-zero-fee",
            &condition_id,
            &token_id,
            SimOrderKindStateV1::Fok,
            decimal("0.6"),
            None,
            500_000,
            &plan,
            &[0],
        );
        builder
            .finish_new_order(
                order,
                fills,
                &policy,
                &zero_fee_rules
                    .snapshot()
                    .project_account_rules
                    .lifecycle_account_behavior
                    .value,
            )
            .expect("零手续费不得要求独占返佣 Arc")
            .into_committable()
            .commit();
        assert_eq!(Arc::as_ptr(&zero_fee_state.rebate), zero_fee_rebate_pointer);
        assert!(Arc::ptr_eq(&zero_fee_state.rebate, &shared_zero_fee_rebate));
        assert_eq!(
            zero_fee_state.rebate.resource_record_count(),
            zero_fee_rebate_records
        );

        let (mut nonzero_fee_state, nonzero_fee_rules, condition_id, token_id) =
            new_order_delta_state_fixture(0).await;
        let shared_nonzero_fee_rebate = Arc::clone(&nonzero_fee_state.rebate);
        let nonzero_fee_rebate_pointer = Arc::as_ptr(&nonzero_fee_state.rebate);
        let baseline =
            serde_json::to_vec(&nonzero_fee_state).expect("序列化非零手续费共享 Arc 前真实状态");
        let builder = nonzero_fee_state
            .prepare_new_order_taker_buy_by_quote_v2(
                &condition_id,
                &token_id,
                decimal("0.6"),
                500_000,
            )
            .expect("共享返佣 Arc 下准备非零手续费撮合");
        let plan = builder.plan().clone();
        let (order, fills) = new_order_delta_records(
            "shared-nonzero-fee",
            &condition_id,
            &token_id,
            SimOrderKindStateV1::Fok,
            decimal("0.6"),
            None,
            500_000,
            &plan,
            &[1_000],
        );
        let Err(error) = builder.finish_new_order(
            order,
            fills,
            &policy,
            &nonzero_fee_rules
                .snapshot()
                .project_account_rules
                .lifecycle_account_behavior
                .value,
        ) else {
            panic!("非零手续费必须在准备期拒绝共享返佣 Arc")
        };
        assert!(matches!(error, MatchingStateErrorV1::Rebate(message)
            if message.contains("独占返佣状态")));
        assert_eq!(
            serde_json::to_vec(&nonzero_fee_state)
                .expect("序列化非零手续费共享 Arc 拒绝后真实状态"),
            baseline
        );
        assert_eq!(
            Arc::as_ptr(&nonzero_fee_state.rebate),
            nonzero_fee_rebate_pointer
        );
        assert!(Arc::ptr_eq(
            &nonzero_fee_state.rebate,
            &shared_nonzero_fee_rebate
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_new_order_v2_branch_table_matches_v1_and_touches_only_target_state() {
        #[derive(Clone, Copy)]
        enum Target {
            Quote(u128),
            Base {
                accepted_base_units: u128,
                accepted_quote_units: u128,
            },
        }
        struct Case {
            label: &'static str,
            kind: SimOrderKindStateV1,
            target: Target,
            unrelated_assets: usize,
            expect_dust: bool,
            expect_gtd: bool,
        }
        let cases = [
            Case {
                label: "fok-success",
                kind: SimOrderKindStateV1::Fok,
                target: Target::Quote(500_000),
                unrelated_assets: 0,
                expect_dust: false,
                expect_gtd: false,
            },
            Case {
                label: "fak-success",
                kind: SimOrderKindStateV1::Fak,
                target: Target::Quote(600_000),
                unrelated_assets: 0,
                expect_dust: false,
                expect_gtd: false,
            },
            Case {
                label: "gtc-dust",
                kind: SimOrderKindStateV1::Gtc,
                target: Target::Base {
                    accepted_base_units: 1_005_000,
                    accepted_quote_units: 502_500,
                },
                unrelated_assets: 0,
                expect_dust: true,
                expect_gtd: false,
            },
            Case {
                label: "gtd-remainder",
                kind: SimOrderKindStateV1::Gtd {
                    expires_at_utc_ms: 3_000,
                },
                target: Target::Base {
                    accepted_base_units: 1_020_000,
                    accepted_quote_units: 510_000,
                },
                unrelated_assets: 512,
                expect_dust: false,
                expect_gtd: true,
            },
        ];

        for case in cases {
            let (mut state, rules, condition_id, token_id) =
                new_order_delta_state_fixture(case.unrelated_assets).await;
            if case.unrelated_assets > 0 {
                for index in 0..512 {
                    state
                        .rebate_mut()
                        .register_taker_fill(
                            &format!("history-{index:04}"),
                            &condition_id,
                            100 + index,
                            decimal("1"),
                        )
                        .expect("建立大量既有返佣历史");
                }
            }
            let rebate_records_before = state.rebate.resource_record_count();
            let mut legacy = state.clone();
            let (legacy_plan, accepted_base_units, accepted_quote_units) = match case.target {
                Target::Quote(accepted_quote_units) => (
                    legacy
                        .match_taker_buy_by_quote(
                            &condition_id,
                            &token_id,
                            decimal("0.6"),
                            accepted_quote_units,
                        )
                        .expect("旧 V1 市价撮合"),
                    None,
                    accepted_quote_units,
                ),
                Target::Base {
                    accepted_base_units,
                    accepted_quote_units,
                } => (
                    legacy
                        .match_taker_buy_by_base(
                            &condition_id,
                            &token_id,
                            decimal("0.5"),
                            accepted_base_units,
                            accepted_quote_units,
                        )
                        .expect("旧 V1 限价撮合"),
                    Some(accepted_base_units),
                    accepted_quote_units,
                ),
            };
            let source_price = if accepted_base_units.is_some() {
                decimal("0.5")
            } else {
                decimal("0.6")
            };
            let legacy_fee_units = vec![1_000; legacy_plan.fills.len()];
            let (legacy_order, legacy_fills) = new_order_delta_records(
                case.label,
                &condition_id,
                &token_id,
                case.kind,
                source_price,
                accepted_base_units,
                accepted_quote_units,
                &legacy_plan,
                &legacy_fee_units,
            );
            let legacy_order_id = legacy_order.platform_order_id.clone();
            let legacy_order_effects = legacy
                .commit_new_order(legacy_order, legacy_fills.clone())
                .expect("旧 V1 提交新订单");
            let mut legacy_trade_ids = legacy_fills
                .iter()
                .map(|fill| fill.trade_id.clone())
                .collect::<Vec<_>>();
            legacy_trade_ids.sort();
            let policy = crate::finality::successful_policy_for_tests();
            let mut legacy_trade_effects = Vec::new();
            for trade_id in legacy_trade_ids {
                legacy_trade_effects.push(
                    legacy
                        .initialize_fill_finality(&policy, &trade_id)
                        .expect("旧 V1 初始化成交最终性"),
                );
            }
            let lifecycle_rule = &rules
                .snapshot()
                .project_account_rules
                .lifecycle_account_behavior
                .value;
            let legacy_dust = legacy
                .apply_venue_dust_after_order_action(&legacy_order_id, 1_000, lifecycle_rule)
                .expect("旧 V1 应用 venue dust");

            reset_asset_queue_clone_count_for_tests();
            reset_execution_state_clone_count_for_tests();
            let rebate_pointer_before = Arc::as_ptr(&state.rebate);
            let builder = match case.target {
                Target::Quote(accepted_quote_units) => state
                    .prepare_new_order_taker_buy_by_quote_v2(
                        &condition_id,
                        &token_id,
                        decimal("0.6"),
                        accepted_quote_units,
                    )
                    .expect("准备 V2 市价新订单候选"),
                Target::Base {
                    accepted_base_units,
                    accepted_quote_units,
                } => state
                    .prepare_new_order_taker_buy_by_base_v2(
                        &condition_id,
                        &token_id,
                        decimal("0.5"),
                        accepted_base_units,
                        accepted_quote_units,
                    )
                    .expect("准备 V2 限价新订单候选"),
            };
            let plan = builder.plan().clone();
            assert_eq!(plan, legacy_plan, "{} 撮合计划", case.label);
            let fee_units = vec![1_000; plan.fills.len()];
            let (order, fills) = new_order_delta_records(
                case.label,
                &condition_id,
                &token_id,
                case.kind,
                source_price,
                accepted_base_units,
                accepted_quote_units,
                &plan,
                &fee_units,
            );
            let prepared = builder
                .finish_new_order(order, fills, &policy, lifecycle_rule)
                .expect("V2 完成新订单候选");
            assert_eq!(prepared.order_commit_effects(), &legacy_order_effects);
            assert_eq!(prepared.trade_finality_effects(), &legacy_trade_effects);
            assert_eq!(prepared.venue_dust_effect(), legacy_dust.as_ref());
            assert_eq!(prepared.venue_dust_effect().is_some(), case.expect_dust);
            prepared.into_committable().commit();

            assert_eq!(state, legacy, "{} V2 与旧 V1 终态", case.label);
            assert_eq!(
                state
                    .gtd_deadlines
                    .contains(&(3_000, legacy_order_id.clone())),
                case.expect_gtd
            );
            assert_eq!(Arc::as_ptr(&state.rebate), rebate_pointer_before);
            assert_eq!(
                state.rebate.resource_record_count(),
                rebate_records_before
                    + 2 * legacy_fills.len()
                    + usize::from(case.unrelated_assets == 0) * 2
            );
            assert_eq!(execution_state_clone_count_for_tests(), 0);
            assert_eq!(asset_queue_clone_count_for_tests(), 1);
        }
    }

    #[test]
    fn fok_partial_taker_candidate_drop_does_not_consume_the_live_ask() {
        let mut queues = taker_asset_candidate_queue_fixture(0);
        let baseline = queues.clone();
        let prepared = queues
            .prepare_taker_buy_by_quote_v2(
                "target-condition",
                "target-token",
                decimal("0.6"),
                800_000,
            )
            .expect("准备 FOK 部分成交候选");
        assert!(!prepared.plan().fills.is_empty());
        assert!(prepared.plan().used_quote_units < 800_000);

        drop(prepared);
        assert_eq!(queues, baseline, "FOK 全不成必须丢弃候选盘口消耗");
    }

    #[test]
    fn fak_no_match_taker_candidate_drop_leaves_the_live_queue_unchanged() {
        let mut queues = taker_asset_candidate_queue_fixture(0);
        let baseline = queues.clone();
        let prepared = queues
            .prepare_taker_buy_by_quote_v2(
                "target-condition",
                "target-token",
                decimal("0.4"),
                300_000,
            )
            .expect("准备 FAK 零成交候选");
        assert!(prepared.plan().fills.is_empty());
        assert_eq!(prepared.plan().matched_base_units, 0);
        assert_eq!(prepared.plan().used_quote_units, 0);

        drop(prepared);
        assert_eq!(queues, baseline, "FAK 零成交拒绝不得修改真实队列");
    }

    #[test]
    fn committed_taker_asset_candidate_matches_the_existing_public_path_exactly() {
        let baseline = taker_asset_candidate_queue_fixture(0);
        let mut prepared_path = baseline.clone();
        let mut existing_path = baseline;

        let prepared = prepared_path
            .prepare_taker_buy_by_quote_v2(
                "target-condition",
                "target-token",
                decimal("0.6"),
                300_000,
            )
            .expect("准备成功 taker 资产候选");
        let prepared_plan = prepared.commit();
        let existing_plan = existing_path
            .match_taker_buy_by_quote("target-condition", "target-token", decimal("0.6"), 300_000)
            .expect("执行既有 taker 撮合路径");

        assert_eq!(prepared_plan, existing_plan);
        assert_eq!(prepared_path, existing_path);

        let baseline = taker_asset_candidate_queue_fixture(0);
        let mut prepared_path = baseline.clone();
        let mut existing_path = baseline;
        let prepared = prepared_path
            .prepare_taker_buy_by_base_v2(
                "target-condition",
                "target-token",
                decimal("0.6"),
                600_000,
                300_000,
            )
            .expect("准备成功限价 taker 资产候选");
        let prepared_plan = prepared.commit();
        let existing_plan = existing_path
            .match_taker_buy_by_base(
                "target-condition",
                "target-token",
                decimal("0.6"),
                600_000,
                300_000,
            )
            .expect("执行既有限价 taker 撮合路径");
        assert_eq!(prepared_plan, existing_plan);
        assert_eq!(prepared_path, existing_path);
    }

    #[test]
    fn partial_taker_candidate_remainder_drop_keeps_the_live_queue_unchanged() {
        let mut queues = taker_asset_candidate_queue_fixture(0);
        let baseline = queues.clone();
        let mut prepared = queues
            .prepare_taker_buy_by_base_v2(
                "target-condition",
                "target-token",
                decimal("0.6"),
                1_500_000,
                750_000,
            )
            .expect("准备部分成交限价候选");
        assert_eq!(prepared.plan().matched_base_units, 1_000_000);
        prepared
            .prepare_account_buy_remainder(decimal("0.5"), "candidate-order".to_owned(), 500_000)
            .expect("只在候选资产插入开放余量");
        assert!(matches!(
            prepared.prepare_account_buy_remainder(
                decimal("0.5"),
                "candidate-order".to_owned(),
                500_000,
            ),
            Err(MatchingStateErrorV1::AccountOrderConflict(order_id))
                if order_id == "candidate-order"
        ));

        drop(prepared);
        assert_eq!(queues, baseline, "后续失败丢弃候选不得留下开放余量");
    }

    #[test]
    fn candidate_remainder_insert_dust_remove_commit_matches_the_existing_path() {
        let baseline = taker_asset_candidate_queue_fixture(0);
        let mut prepared_path = baseline.clone();
        let mut existing_path = baseline;

        let mut prepared = prepared_path
            .prepare_taker_buy_by_base_v2(
                "target-condition",
                "target-token",
                decimal("0.6"),
                1_500_000,
                750_000,
            )
            .expect("准备开放余量候选");
        prepared
            .prepare_account_buy_remainder(decimal("0.5"), "candidate-order".to_owned(), 500_000)
            .expect("候选插入开放余量");
        let dust = prepared
            .candidate_venue_dust_remainder(decimal("0.5"), "candidate-order")
            .expect("候选开放余量形成精确 dust 候选");
        prepared
            .prepare_remove_venue_dust_remainder(&dust)
            .expect("从候选资产移除精确 dust 余量");
        let prepared_plan = prepared.commit();

        let existing_plan = existing_path
            .match_taker_buy_by_base(
                "target-condition",
                "target-token",
                decimal("0.6"),
                1_500_000,
                750_000,
            )
            .expect("既有路径完成部分成交");
        existing_path
            .insert_account_buy_order(
                "target-condition",
                "target-token",
                decimal("0.5"),
                "candidate-order".to_owned(),
                500_000,
            )
            .expect("既有路径插入开放余量");
        assert_eq!(
            existing_path.venue_dust_candidate_for_order(
                "target-condition",
                "target-token",
                decimal("0.5"),
                "candidate-order",
            ),
            Some(VenueDustQueueCandidateV1 {
                platform_order_id: "candidate-order".to_owned(),
                price_pusd_per_share: decimal("0.5"),
                remaining_base_units: 500_000,
            })
        );
        existing_path.remove_account_order("candidate-order");

        assert_eq!(prepared_plan, existing_plan);
        assert_eq!(prepared_path, existing_path);
    }

    #[test]
    fn candidate_remainder_rejects_an_order_id_owned_by_another_live_asset() {
        let mut queues = taker_asset_candidate_queue_fixture(1);
        queues
            .markets
            .get_mut("target-condition")
            .expect("测试市场")
            .get_mut("unrelated-token-0000")
            .expect("测试无关资产")
            .bids
            .insert(
                decimal("0.4"),
                PriceQueueV1 {
                    segments: VecDeque::from([QueueSegmentV1::Account(
                        AccountOrderQueueSegmentV1 {
                            platform_order_id: "live-other-order".to_owned(),
                            remaining_base_units: 100_000,
                        },
                    )]),
                    account_taker_depletion_debt_units: 0,
                },
            );
        let baseline = queues.clone();
        let mut prepared = queues
            .prepare_taker_buy_by_quote_v2(
                "target-condition",
                "target-token",
                decimal("0.6"),
                300_000,
            )
            .expect("准备目标资产撮合候选");

        assert!(matches!(
            prepared.prepare_account_buy_remainder(
                decimal("0.5"),
                "live-other-order".to_owned(),
                100_000,
            ),
            Err(MatchingStateErrorV1::AccountOrderConflict(order_id))
                if order_id == "live-other-order"
        ));
        drop(prepared);
        assert_eq!(queues, baseline);
    }

    #[test]
    fn tampered_candidate_remainder_is_rejected_and_drop_is_read_only() {
        let mut queues = taker_asset_candidate_queue_fixture(0);
        let baseline = queues.clone();
        let mut prepared = queues
            .prepare_taker_buy_by_base_v2(
                "target-condition",
                "target-token",
                decimal("0.6"),
                1_500_000,
                750_000,
            )
            .expect("准备 dust 篡改测试候选");
        prepared
            .prepare_account_buy_remainder(decimal("0.5"), "candidate-order".to_owned(), 500_000)
            .expect("候选插入开放余量");
        let mut tampered_condition = prepared
            .candidate_venue_dust_remainder(decimal("0.5"), "candidate-order")
            .expect("形成待篡改 dust 候选");
        tampered_condition.condition_id.push_str("-tampered");
        assert!(matches!(
            prepared.prepare_remove_venue_dust_remainder(&tampered_condition),
            Err(MatchingStateErrorV1::QueuePredecessorConflict)
        ));

        let mut tampered_token = prepared
            .candidate_venue_dust_remainder(decimal("0.5"), "candidate-order")
            .expect("形成待篡改 token 的 dust 候选");
        tampered_token.token_id.push_str("-tampered");
        assert!(matches!(
            prepared.prepare_remove_venue_dust_remainder(&tampered_token),
            Err(MatchingStateErrorV1::QueuePredecessorConflict)
        ));

        let mut tampered_price = prepared
            .candidate_venue_dust_remainder(decimal("0.5"), "candidate-order")
            .expect("形成待篡改价格的 dust 候选");
        tampered_price.price_pusd_per_share = decimal("0.4");
        assert!(matches!(
            prepared.prepare_remove_venue_dust_remainder(&tampered_price),
            Err(MatchingStateErrorV1::QueuePredecessorConflict)
        ));

        let mut tampered_order = prepared
            .candidate_venue_dust_remainder(decimal("0.5"), "candidate-order")
            .expect("形成待篡改订单 ID 的 dust 候选");
        tampered_order.platform_order_id.push_str("-tampered");
        assert!(matches!(
            prepared.prepare_remove_venue_dust_remainder(&tampered_order),
            Err(MatchingStateErrorV1::QueuePredecessorConflict)
        ));

        let mut tampered_base = prepared
            .candidate_venue_dust_remainder(decimal("0.5"), "candidate-order")
            .expect("形成待篡改数量的 dust 候选");
        tampered_base.remaining_base_units += 1;

        assert!(matches!(
            prepared.prepare_remove_venue_dust_remainder(&tampered_base),
            Err(MatchingStateErrorV1::QueuePredecessorConflict)
        ));
        drop(prepared);
        assert_eq!(queues, baseline);
    }

    #[test]
    fn taker_asset_candidate_keeps_large_unrelated_queue_identity_and_count() {
        let mut queues = taker_asset_candidate_queue_fixture(512);
        let before_count = queues.markets["target-condition"].len();
        let unrelated_before =
            std::ptr::from_ref(&queues.markets["target-condition"]["unrelated-token-0256"]);
        let unrelated_value = queues.markets["target-condition"]["unrelated-token-0256"].clone();
        let target_before = queues.markets["target-condition"]["target-token"].clone();

        let prepared = queues
            .prepare_taker_buy_by_quote_v2(
                "target-condition",
                "target-token",
                decimal("0.6"),
                300_000,
            )
            .expect("大量无关资产下准备目标候选");
        let _ = prepared.commit();

        assert_eq!(queues.markets["target-condition"].len(), before_count);
        assert_eq!(
            std::ptr::from_ref(&queues.markets["target-condition"]["unrelated-token-0256"]),
            unrelated_before,
            "提交目标资产不得替换无关资产队列引用"
        );
        assert_eq!(
            queues.markets["target-condition"]["unrelated-token-0256"],
            unrelated_value
        );
        assert_ne!(
            queues.markets["target-condition"]["target-token"],
            target_before
        );
    }

    #[test]
    fn end_at_release_drops_all_market_price_levels_and_taker_debt() {
        let mut bid = PriceQueueV1 {
            segments: VecDeque::from([
                QueueSegmentV1::External(ExternalSyntheticMakerSegmentV1 {
                    segment_id: Sha256Hex("a".repeat(64)),
                    total_base_units: 2_000_000,
                    total_quote_units: 800_000,
                    consumed_base_units: 100_000,
                }),
                QueueSegmentV1::Account(AccountOrderQueueSegmentV1 {
                    platform_order_id: "account-order".to_owned(),
                    remaining_base_units: 500_000,
                }),
            ]),
            account_taker_depletion_debt_units: 123_456,
        };
        let ask = PriceQueueV1 {
            segments: VecDeque::from([QueueSegmentV1::External(ExternalSyntheticMakerSegmentV1 {
                segment_id: Sha256Hex("b".repeat(64)),
                total_base_units: 1_000_000,
                total_quote_units: 600_000,
                consumed_base_units: 0,
            })]),
            account_taker_depletion_debt_units: 654_321,
        };
        bid.segments
            .push_back(QueueSegmentV1::Account(AccountOrderQueueSegmentV1 {
                platform_order_id: "second-account-order".to_owned(),
                remaining_base_units: 250_000,
            }));

        let mut queues = ExecutionQueueStateV1 {
            markets: BTreeMap::from([(
                "condition-a".to_owned(),
                BTreeMap::from([(
                    "token-a".to_owned(),
                    AssetQueueV1 {
                        bids: BTreeMap::from([(decimal("0.4"), bid)]),
                        asks: BTreeMap::from([(decimal("0.6"), ask)]),
                        initial_loaded: true,
                    },
                )]),
            )]),
        };

        queues
            .release_market_matching_state("condition-a")
            .expect("盘后释放已知市场撮合热状态");

        let asset = &queues.markets["condition-a"]["token-a"];
        assert!(asset.bids.is_empty());
        assert!(asset.asks.is_empty());
        assert!(asset.initial_loaded, "释放不得移除市场资产身份壳");
    }

    #[test]
    fn end_at_release_waits_for_every_asset_initial_snapshot() {
        let queued = PriceQueueV1 {
            segments: VecDeque::from([QueueSegmentV1::External(ExternalSyntheticMakerSegmentV1 {
                segment_id: Sha256Hex("c".repeat(64)),
                total_base_units: 1_000_000,
                total_quote_units: 400_000,
                consumed_base_units: 0,
            })]),
            account_taker_depletion_debt_units: 0,
        };
        let mut queues = ExecutionQueueStateV1 {
            markets: BTreeMap::from([(
                "condition-a".to_owned(),
                BTreeMap::from([
                    (
                        "token-a".to_owned(),
                        AssetQueueV1 {
                            bids: BTreeMap::from([(decimal("0.4"), queued)]),
                            asks: BTreeMap::new(),
                            initial_loaded: true,
                        },
                    ),
                    ("token-b".to_owned(), AssetQueueV1::default()),
                ]),
            )]),
        };

        assert!(
            !queues
                .release_market_matching_state_if_initialized("condition-a")
                .expect("盘后检查已知市场")
        );
        assert_eq!(queues.markets["condition-a"]["token-a"].bids.len(), 1);

        queues
            .markets
            .get_mut("condition-a")
            .expect("测试市场")
            .get_mut("token-b")
            .expect("测试资产")
            .initial_loaded = true;
        assert!(
            queues
                .release_market_matching_state_if_initialized("condition-a")
                .expect("完整初始装载后释放")
        );
        assert!(queues.markets["condition-a"]["token-a"].bids.is_empty());
    }

    #[test]
    fn taker_fee_allocation_uses_stable_identity_for_equal_exact_remainders() {
        let fills = [
            ("fill-z", 200, decimal("0.5")),
            ("fill-a", 200, decimal("0.5")),
        ];
        let (total, allocations) = allocate_taker_fee_micro_units_v1(&fills, decimal("0.07"), 1)
            .expect("相同余数手续费分配");
        assert_eq!(total, 10);
        assert_eq!(allocations, vec![0, 10]);

        let reversed = [fills[1], fills[0]];
        let (reversed_total, reversed_allocations) =
            allocate_taker_fee_micro_units_v1(&reversed, decimal("0.07"), 1)
                .expect("扰动后的相同余数手续费分配");
        assert_eq!(reversed_total, total);
        assert_eq!(
            allocations_by_identity(&reversed, &reversed_allocations),
            allocations_by_identity(&fills, &allocations),
        );
    }

    #[test]
    fn taker_fee_allocation_preserves_once_rounded_multi_price_total() {
        let fills = [
            ("fill-lower-remainder", 300, decimal("0.4")),
            ("fill-higher-remainder", 300, decimal("0.5")),
        ];
        let (total, allocations) = allocate_taker_fee_micro_units_v1(&fills, decimal("0.07"), 1)
            .expect("多价位手续费分配");
        let order_total = taker_fee_pusd_v1(
            &fills
                .iter()
                .map(|(_, base_units, price)| (*base_units, *price))
                .collect::<Vec<_>>(),
            decimal("0.07"),
            1,
        )
        .expect("整单手续费");

        assert_eq!(
            total,
            micro_units_v1(order_total).expect("整单手续费微单位")
        );
        assert_eq!(allocations, vec![0, 10]);
        assert_eq!(allocations.iter().sum::<u128>(), total);
    }

    #[test]
    fn gtd_expiration_identity_binds_terminalized_amounts_and_pending_evidence() {
        let evidence = MakerDepletionEvidenceV1 {
            sha256: Sha256Hex("a".repeat(64)),
            event_count: 2,
            first_source_event_id: "event-first".to_owned(),
            last_source_event_id: "event-last".to_owned(),
        };
        let baseline =
            gtd_expiration_identity("order-a", 391_000, 1, 4_999_999, 50, Some(&evidence));

        assert_ne!(
            baseline,
            gtd_expiration_identity("order-b", 391_000, 1, 4_999_999, 50, Some(&evidence),)
        );
        assert_ne!(
            baseline,
            gtd_expiration_identity("order-a", 391_001, 1, 4_999_999, 50, Some(&evidence),)
        );
        assert_ne!(
            baseline,
            gtd_expiration_identity("order-a", 391_000, 2, 4_999_999, 50, Some(&evidence),)
        );
        assert_ne!(
            baseline,
            gtd_expiration_identity("order-a", 391_000, 1, 4_999_998, 50, Some(&evidence),)
        );
        assert_ne!(
            baseline,
            gtd_expiration_identity("order-a", 391_000, 1, 4_999_999, 49, Some(&evidence),)
        );
        assert_ne!(
            baseline,
            gtd_expiration_identity("order-a", 391_000, 1, 4_999_999, 50, None)
        );
        let changed_evidence = MakerDepletionEvidenceV1 {
            sha256: Sha256Hex("b".repeat(64)),
            event_count: 3,
            ..evidence
        };
        assert_ne!(
            baseline,
            gtd_expiration_identity(
                "order-a",
                391_000,
                1,
                4_999_999,
                50,
                Some(&changed_evidence),
            )
        );
    }
}
