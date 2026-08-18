use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

use contracts::{
    AcceptedSubmissionItemV2, AccountInstanceKeyV1, BookSide, CanceledTargetMeasurementV2,
    ConditionId, ExactDecimal, ExecutionMarketEventPayloadV3, ExecutionMarketEventV3,
    InitialL2SnapshotV2, LevelExecutionTransitionV3, MakerRebateAccrualStatusV2,
    MakerRebateFillContributionV2, MarketDataQuality, MarketResolutionTerminalV3,
    OrderKindProjectionV2, OrderLifecycleEventV2, OrderProjectionV2, OrderQuantityProjectionV2,
    PlatformAccountBalanceDeltaV1, PlatformAccountDeltaReasonV1, PlatformAccountDeltaReasonV2,
    PlatformAccountDeltaV1, PlatformAccountDeltaV2, PlatformOutcomeBalanceDeltaV1,
    PlatformOutcomeBalanceV1, QualityScopeV1, ResyncQuality, SettlementLifecycleEventV2, Sha256Hex,
    SimOrderTerminalReasonV2, SimulatedOrderKnownStatusV2, SimulatedOrderStatusV2,
    SimulatedTradeKnownStatusV2, SimulatedTradeStatusV2, SubmissionFillV2,
    SubmissionMeasurementV2 as ContractSubmissionMeasurementV2, TokenId,
    TradeEconomicDispositionV2, TradeLiquidityRoleV2, TradeProjectionV2,
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
    AutoRedeemAccountEffectV1, AutoRedeemAccountEffectV2, FillEconomicAmountsV1,
    PlatformAccountErrorV1, PlatformAccountStateV1, PlatformAccountStateV2,
    validate_platform_account_delta_integrity,
};
use crate::finality::{
    SelectedTradeFinalityPathV2, TradeFinalityPathV1, TradeFinalityPolicyV1, TradeFinalityPolicyV2,
    TradeFinalitySelectorDomainV1, TradeFinalityStateV1,
};
use crate::market::{
    AccountBookOverlayStoreV1, MarketLifecyclePhaseV1, NormalizedMarketEventV2,
    PreparedAccountBookOverlayLevelChangeV2, QualifiedMarketResolutionV2,
};
use crate::market_directory::SessionMarketDirectoryV1;
use crate::platform_rules::{
    LifecycleAccountRuleV1, PlatformRuleSnapshotV3, SealedProcessRuleSnapshot,
};
use crate::rebate::{
    CommittableTakerRebateRegistrationsV2, MakerRebateContributionV1,
    MakerRebateMarketResultStatusV1, RebateDayFinalStateV1, RebateKindV1, RebateRecoveryFillV1,
    RebateSettlementActionV1, RebateStateErrorV1, RebateStatePatchV2, RebateStateV1,
    finalized_day_state, maker_rebate_contribution_v1, rebate_maturity_at, utc_day_start,
};
use crate::resource::BusinessResourceDeltaV2;
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
    pub ordered_lifecycle_effects: Vec<AuthorizedMarketLifecycleEffectV2>,
}

impl AuthorizedMarketEventEffectsV1 {
    pub(crate) fn append(&mut self, mut other: Self) {
        self.trade_finality_effects
            .append(&mut other.trade_finality_effects);
        self.venue_dust_effects
            .append(&mut other.venue_dust_effects);
        self.market_resolution_order_effects
            .append(&mut other.market_resolution_order_effects);
        self.settlement_feedbacks
            .append(&mut other.settlement_feedbacks);
        self.ordered_lifecycle_effects
            .append(&mut other.ordered_lifecycle_effects);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AuthorizedMarketLifecycleEffectV2 {
    TradeFinality(TradeFinalityLifecycleEffectsV1),
    VenueDust(VenueDustEffectV1),
    MarketResolutionOrder(MarketResolutionOrderEffectV1),
    Settlement(SettlementLifecycleEffectV1),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueueMatchPlanV1 {
    pub fills: Vec<TakerFillSliceV1>,
    pub matched_base_units: u128,
    pub used_quote_units: u128,
}

#[derive(Debug, PartialEq, Eq)]
struct CalculatedTakerFillV2 {
    fill: TakerFillSliceV2,
    segment_index: usize,
    consumed_base_units_after: u128,
    account_taker_depletion_debt_units_after: u128,
}

#[derive(Debug, PartialEq, Eq)]
struct CalculatedTakerConsumptionV2 {
    fills: Vec<CalculatedTakerFillV2>,
    matched_base_units: u128,
    used_quote_units: u128,
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
    fn best_executable_ask(&self) -> Option<Decimal> {
        self.asks
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

#[derive(Debug, PartialEq, Eq)]
enum QueueSegmentV2 {
    External {
        total_base_units: u128,
        total_quote_units: u128,
        consumed_base_units: u128,
    },
    Account {
        platform_order_id: String,
        remaining_base_units: u128,
    },
}

impl QueueSegmentV2 {
    fn remaining_base_units(&self) -> Result<u128, MatchingStateErrorV1> {
        match self {
            Self::External {
                total_base_units,
                consumed_base_units,
                ..
            } => total_base_units
                .checked_sub(*consumed_base_units)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict),
            Self::Account {
                remaining_base_units,
                ..
            } => Ok(*remaining_base_units),
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct PriceQueueV2 {
    segments: VecDeque<QueueSegmentV2>,
    account_taker_depletion_debt_units: u128,
}

#[derive(Debug, PartialEq, Eq)]
struct TakerFillSliceV2 {
    price_pusd_per_share: Decimal,
    base_units: u128,
    quote_units: u128,
}

#[derive(Debug, PartialEq, Eq)]
struct QueueMatchPlanV2 {
    fills: Vec<TakerFillSliceV2>,
    matched_base_units: u128,
    used_quote_units: u128,
}

#[derive(Debug, PartialEq, Eq)]
struct MakerQueueDepletionV2 {
    platform_order_id: String,
    price_pusd_per_share: Decimal,
    base_units: u128,
}

#[derive(Debug, PartialEq, Eq)]
struct AssetVenueDustCandidateV2 {
    platform_order_id: String,
    price_pusd_per_share: Decimal,
    remaining_base_units: u128,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct AssetQueueEffectsV2 {
    maker_depletions: Vec<MakerQueueDepletionV2>,
    venue_dust_candidate: Option<AssetVenueDustCandidateV2>,
    resources: BusinessResourceDeltaV2,
}

#[allow(dead_code)]
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct AssetQueueV2 {
    bids: BTreeMap<Decimal, PriceQueueV2>,
    asks: BTreeMap<Decimal, PriceQueueV2>,
}

#[allow(dead_code)]
impl AssetQueueV2 {
    fn from_initial_levels(
        bids: &[(Decimal, u128)],
        asks: &[(Decimal, u128)],
    ) -> Result<Self, MatchingStateErrorV1> {
        let mut queue = Self::default();
        for (side, levels) in [(BookSide::Bid, bids), (BookSide::Ask, asks)] {
            for (price, base_units) in levels {
                if *base_units > 0 {
                    queue.append_external_delta(side, *price, *base_units)?;
                }
            }
        }
        Ok(queue)
    }

    fn levels_mut(&mut self, side: BookSide) -> &mut BTreeMap<Decimal, PriceQueueV2> {
        match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        }
    }

    #[cfg(test)]
    fn external_base_units_for_tests(&self, side: BookSide, price: Decimal) -> u128 {
        let levels = match side {
            BookSide::Bid => &self.bids,
            BookSide::Ask => &self.asks,
        };
        levels.get(&price).map_or(0, |queue| {
            queue
                .segments
                .iter()
                .filter_map(|segment| match segment {
                    QueueSegmentV2::External {
                        total_base_units,
                        consumed_base_units,
                        ..
                    } => total_base_units.checked_sub(*consumed_base_units),
                    QueueSegmentV2::Account { .. } => None,
                })
                .sum()
        })
    }

    fn append_external_delta(
        &mut self,
        side: BookSide,
        price: Decimal,
        base_units: u128,
    ) -> Result<(), MatchingStateErrorV1> {
        let total_quote_units = segment_quote_units_v1(base_units, price)?;
        self.levels_mut(side)
            .entry(price)
            .or_default()
            .segments
            .push_back(QueueSegmentV2::External {
                total_base_units: base_units,
                total_quote_units,
                consumed_base_units: 0,
            });
        Ok(())
    }

    fn best_executable_ask(&self) -> Option<Decimal> {
        self.asks
            .iter()
            .find(|(_, queue)| {
                queue.segments.iter().any(|segment| {
                    matches!(
                        segment,
                        QueueSegmentV2::External {
                            total_base_units,
                            total_quote_units,
                            consumed_base_units,
                        } if *total_quote_units > 0 && consumed_base_units < total_base_units
                    )
                })
            })
            .map(|(price, _)| *price)
    }

    fn insert_account_buy_remainder(
        &mut self,
        price: Decimal,
        platform_order_id: String,
        base_units: u128,
    ) -> BusinessResourceDeltaV2 {
        let price_exists = self.bids.contains_key(&price);
        let platform_order_id_bytes = platform_order_id.len() as u64;
        self.bids
            .entry(price)
            .or_default()
            .segments
            .push_back(QueueSegmentV2::Account {
                platform_order_id,
                remaining_base_units: base_units,
            });
        let mut resources = BusinessResourceDeltaV2::default();
        resources.add_record(platform_order_id_bytes);
        if !price_exists {
            resources.add_record(0);
        }
        resources
    }

    fn venue_dust_candidate(
        &self,
        price: Decimal,
        platform_order_id: &str,
    ) -> Option<AssetVenueDustCandidateV2> {
        let queue = self.bids.get(&price)?;
        if queue.account_taker_depletion_debt_units != 0 || queue.segments.len() != 1 {
            return None;
        }
        match queue.segments.front() {
            Some(QueueSegmentV2::Account {
                platform_order_id: existing,
                remaining_base_units,
            }) if existing == platform_order_id && *remaining_base_units > 0 => {
                Some(AssetVenueDustCandidateV2 {
                    platform_order_id: existing.clone(),
                    price_pusd_per_share: price,
                    remaining_base_units: *remaining_base_units,
                })
            }
            Some(QueueSegmentV2::External { .. }) | Some(QueueSegmentV2::Account { .. }) | None => {
                None
            }
        }
    }

    fn remove_account_order(&mut self, platform_order_id: &str) -> BusinessResourceDeltaV2 {
        let mut resources = BusinessResourceDeltaV2::default();
        self.bids.retain(|_, queue| {
            queue.segments.retain(|segment| {
                let remove = matches!(segment, QueueSegmentV2::Account {
                    platform_order_id: existing,
                    ..
                } if existing == platform_order_id);
                if remove {
                    resources.remove_record(platform_order_id.len() as u64);
                }
                !remove
            });
            let keep = !queue.segments.is_empty() || queue.account_taker_depletion_debt_units > 0;
            if !keep {
                resources.remove_record(0);
            }
            keep
        });
        resources
    }

    fn remove_account_order_at_price(
        &mut self,
        price: Decimal,
        platform_order_id: &str,
    ) -> Option<(u128, BusinessResourceDeltaV2)> {
        let queue = self.bids.get_mut(&price)?;
        let index = queue.segments.iter().position(|segment| {
            matches!(segment, QueueSegmentV2::Account {
                platform_order_id: existing,
                ..
            } if existing == platform_order_id)
        })?;
        let QueueSegmentV2::Account {
            remaining_base_units,
            ..
        } = queue
            .segments
            .remove(index)
            .expect("刚定位的 V2 账户队列段必须仍存在")
        else {
            unreachable!("V2 账户订单定位只能命中账户队列段")
        };
        let mut resources = BusinessResourceDeltaV2::default();
        resources.remove_record(platform_order_id.len() as u64);
        if queue.segments.is_empty() && queue.account_taker_depletion_debt_units == 0 {
            self.bids.remove(&price);
            resources.remove_record(0);
        }
        Some((remaining_base_units, resources))
    }

    fn calculate_taker_buy(
        &self,
        maximum_price: Decimal,
        target: TakerTargetV1,
    ) -> Result<CalculatedTakerConsumptionV2, MatchingStateErrorV1> {
        let mut fills = Vec::new();
        let mut matched_base_units = 0_u128;
        let mut used_quote_units = 0_u128;
        'prices: for (price, queue) in self.asks.range(..=maximum_price) {
            if target.complete(matched_base_units, used_quote_units) {
                break;
            }
            let mut debt_after = queue.account_taker_depletion_debt_units;
            for (segment_index, segment) in queue.segments.iter().enumerate() {
                let QueueSegmentV2::External {
                    total_base_units,
                    total_quote_units,
                    consumed_base_units,
                } = segment
                else {
                    continue;
                };
                if target.complete(matched_base_units, used_quote_units) {
                    break;
                }
                let remaining_base = total_base_units
                    .checked_sub(*consumed_base_units)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                if remaining_base == 0 || *total_quote_units == 0 {
                    continue;
                }
                let next = match target {
                    TakerTargetV1::Quote(budget) => {
                        let remaining_budget = budget
                            .checked_sub(used_quote_units)
                            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                        maximum_base_for_quote_budget_v1(
                            *total_base_units,
                            *total_quote_units,
                            *consumed_base_units,
                            remaining_budget,
                        )?
                    }
                    TakerTargetV1::LimitBase {
                        total_base_units: target_base_units,
                        total_quote_units: target_quote_units,
                    } => {
                        let remaining_target = target_base_units
                            .checked_sub(matched_base_units)
                            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                        maximum_base_for_limit_order_segment_v1(
                            *total_base_units,
                            *total_quote_units,
                            *consumed_base_units,
                            remaining_base.min(remaining_target),
                            target_base_units,
                            target_quote_units,
                            matched_base_units,
                            used_quote_units,
                        )?
                    }
                };
                let Some((delta_base, delta_quote)) = next else {
                    break 'prices;
                };
                debt_after = debt_after
                    .checked_add(delta_base)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                fills.push(CalculatedTakerFillV2 {
                    fill: TakerFillSliceV2 {
                        price_pusd_per_share: *price,
                        base_units: delta_base,
                        quote_units: delta_quote,
                    },
                    segment_index,
                    consumed_base_units_after: consumed_base_units
                        .checked_add(delta_base)
                        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?,
                    account_taker_depletion_debt_units_after: debt_after,
                });
                matched_base_units = matched_base_units
                    .checked_add(delta_base)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                used_quote_units = used_quote_units
                    .checked_add(delta_quote)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                if delta_base < remaining_base {
                    break 'prices;
                }
            }
        }
        Ok(CalculatedTakerConsumptionV2 {
            fills,
            matched_base_units,
            used_quote_units,
        })
    }

    fn apply_taker_consumption(
        &mut self,
        calculated: CalculatedTakerConsumptionV2,
    ) -> (QueueMatchPlanV2, BusinessResourceDeltaV2) {
        let CalculatedTakerConsumptionV2 {
            fills: calculated_fills,
            matched_base_units,
            used_quote_units,
        } = calculated;
        let mut fills = Vec::with_capacity(calculated_fills.len());
        let mut resources = BusinessResourceDeltaV2::default();
        let mut calculated_fills = calculated_fills.into_iter().peekable();
        while let Some(calculated) = calculated_fills.next() {
            let is_last_at_price = calculated_fills.peek().map_or(true, |next| {
                next.fill.price_pusd_per_share != calculated.fill.price_pusd_per_share
            });
            let queue = self
                .asks
                .get_mut(&calculated.fill.price_pusd_per_share)
                .expect("刚计算的 V2 taker 价位必须仍在独占资产队列中");
            let QueueSegmentV2::External {
                consumed_base_units,
                ..
            } = queue
                .segments
                .get_mut(calculated.segment_index)
                .expect("刚计算的 V2 taker 外部段必须仍在独占价位队列中")
            else {
                unreachable!("刚计算的 V2 taker 段类型不能在应用前改变");
            };
            *consumed_base_units = calculated.consumed_base_units_after;
            queue.account_taker_depletion_debt_units =
                calculated.account_taker_depletion_debt_units_after;
            fills.push(calculated.fill);
            if is_last_at_price {
                let segment_count_before = queue.segments.len();
                queue.segments.retain(|segment| {
                    segment
                        .remaining_base_units()
                        .is_ok_and(|remaining| remaining > 0)
                });
                resources.remove_records(
                    segment_count_before
                        .checked_sub(queue.segments.len())
                        .expect("V2 taker 消耗只能移除队列段") as u64,
                );
            }
        }
        (
            QueueMatchPlanV2 {
                fills,
                matched_base_units,
                used_quote_units,
            },
            resources,
        )
    }

    fn apply_level_transition(
        &mut self,
        side: BookSide,
        price: Decimal,
        quantity_before_base_units: u128,
        quantity_after_base_units: u128,
    ) -> Result<AssetQueueEffectsV2, MatchingStateErrorV1> {
        if quantity_after_base_units > quantity_before_base_units {
            let price_exists = self.levels_mut(side).contains_key(&price);
            self.append_external_delta(
                side,
                price,
                quantity_after_base_units - quantity_before_base_units,
            )?;
            let mut resources = BusinessResourceDeltaV2::default();
            resources.add_record(0);
            if !price_exists {
                resources.add_record(0);
            }
            return Ok(AssetQueueEffectsV2 {
                resources,
                ..AssetQueueEffectsV2::default()
            });
        }
        if quantity_before_base_units == quantity_after_base_units {
            return Ok(AssetQueueEffectsV2::default());
        }

        let levels = self.levels_mut(side);
        let queue = levels
            .get_mut(&price)
            .ok_or(MatchingStateErrorV1::QueueUnderflow)?;
        let mut remaining = quantity_before_base_units - quantity_after_base_units;
        let debt_offset = remaining.min(queue.account_taker_depletion_debt_units);
        queue.account_taker_depletion_debt_units -= debt_offset;
        remaining -= debt_offset;
        if remaining == 0 {
            let mut resources = BusinessResourceDeltaV2::default();
            if queue.segments.is_empty() && queue.account_taker_depletion_debt_units == 0 {
                levels.remove(&price);
                resources.remove_record(0);
            }
            return Ok(AssetQueueEffectsV2 {
                resources,
                ..AssetQueueEffectsV2::default()
            });
        }

        let mut maker_depletions = Vec::new();
        let mut resources = BusinessResourceDeltaV2::default();
        while remaining > 0 {
            let segment = queue
                .segments
                .front_mut()
                .ok_or(MatchingStateErrorV1::QueueUnderflow)?;
            let available = segment.remaining_base_units()?;
            let consumed = available.min(remaining);
            match segment {
                QueueSegmentV2::External {
                    consumed_base_units,
                    ..
                } => {
                    *consumed_base_units = consumed_base_units
                        .checked_add(consumed)
                        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                }
                QueueSegmentV2::Account {
                    platform_order_id,
                    remaining_base_units,
                } => {
                    *remaining_base_units = remaining_base_units
                        .checked_sub(consumed)
                        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                    maker_depletions.push(MakerQueueDepletionV2 {
                        platform_order_id: platform_order_id.clone(),
                        price_pusd_per_share: price,
                        base_units: consumed,
                    });
                }
            }
            remaining -= consumed;
            if segment.remaining_base_units()? == 0 {
                if let Some(removed) = queue.segments.pop_front() {
                    let owned_bytes = match removed {
                        QueueSegmentV2::Account {
                            platform_order_id, ..
                        } => platform_order_id.len() as u64,
                        QueueSegmentV2::External { .. } => 0,
                    };
                    resources.remove_record(owned_bytes);
                }
            }
        }

        let venue_dust_candidate =
            if queue.account_taker_depletion_debt_units == 0 && queue.segments.len() == 1 {
                match queue.segments.front() {
                    Some(QueueSegmentV2::Account {
                        platform_order_id,
                        remaining_base_units,
                    }) if *remaining_base_units > 0 => Some(AssetVenueDustCandidateV2 {
                        platform_order_id: platform_order_id.clone(),
                        price_pusd_per_share: price,
                        remaining_base_units: *remaining_base_units,
                    }),
                    Some(QueueSegmentV2::External { .. })
                    | Some(QueueSegmentV2::Account { .. })
                    | None => None,
                }
            } else {
                None
            };
        if queue.segments.is_empty() && queue.account_taker_depletion_debt_units == 0 {
            levels.remove(&price);
            resources.remove_record(0);
        }
        Ok(AssetQueueEffectsV2 {
            maker_depletions,
            venue_dust_candidate,
            resources,
        })
    }

    fn clear_external_segments(&mut self) -> BusinessResourceDeltaV2 {
        let mut resources = BusinessResourceDeltaV2::default();
        for levels in [&mut self.bids, &mut self.asks] {
            levels.retain(|_, queue| {
                let external_segments = queue
                    .segments
                    .iter()
                    .filter(|segment| matches!(segment, QueueSegmentV2::External { .. }))
                    .count() as u64;
                resources.remove_records(external_segments);
                queue
                    .segments
                    .retain(|segment| matches!(segment, QueueSegmentV2::Account { .. }));
                queue.account_taker_depletion_debt_units = 0;
                let keep = !queue.segments.is_empty();
                if !keep {
                    resources.remove_record(0);
                }
                keep
            });
        }
        resources
    }

    fn clear_matching_segments(&mut self) -> BusinessResourceDeltaV2 {
        let mut resources = BusinessResourceDeltaV2::default();
        for levels in [&self.bids, &self.asks] {
            resources.remove_records(levels.len() as u64);
            for queue in levels.values() {
                for segment in &queue.segments {
                    resources.remove_record(match segment {
                        QueueSegmentV2::Account {
                            platform_order_id, ..
                        } => platform_order_id.len() as u64,
                        QueueSegmentV2::External { .. } => 0,
                    });
                }
            }
        }
        self.bids.clear();
        self.asks.clear();
        resources
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
struct ExecutionQueueStateV2 {
    markets: BTreeMap<String, MarketQueueStateV2>,
}

#[derive(Debug, PartialEq, Eq)]
struct MarketQueueStateV2 {
    matching_closed: bool,
    assets: BTreeMap<String, AssetQueueV2>,
}

#[allow(dead_code)]
impl ExecutionQueueStateV2 {
    fn new(directory: &SessionMarketDirectoryV1) -> Self {
        let markets = directory
            .markets()
            .map(|market| {
                let assets = market
                    .assets()
                    .iter()
                    .map(|asset| (asset.token_id.0.clone(), AssetQueueV2::default()))
                    .collect();
                (
                    market.market().condition_id.0.clone(),
                    MarketQueueStateV2 {
                        matching_closed: false,
                        assets,
                    },
                )
            })
            .collect();
        Self { markets }
    }

    fn commit_admitted_market(&mut self, market: &PlatformMarketV1) {
        let assets = market
            .assets()
            .iter()
            .map(|asset| (asset.token_id.0.clone(), AssetQueueV2::default()))
            .collect();
        self.markets.insert(
            market.market().condition_id.0.clone(),
            MarketQueueStateV2 {
                matching_closed: false,
                assets,
            },
        );
    }

    #[cfg(test)]
    fn market_and_asset_count_for_tests(&self) -> (usize, usize) {
        (
            self.markets.len(),
            self.markets
                .values()
                .map(|market| market.assets.len())
                .sum(),
        )
    }

    fn apply_initial_levels(
        &mut self,
        condition_id: &str,
        token_id: &str,
        bids: &[(Decimal, u128)],
        asks: &[(Decimal, u128)],
    ) -> Result<BusinessResourceDeltaV2, MatchingStateErrorV1> {
        let asset = self
            .markets
            .get_mut(condition_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownMarket(condition_id.to_owned()))?
            .assets
            .get_mut(token_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownAsset(token_id.to_owned()))?;
        *asset = AssetQueueV2::from_initial_levels(bids, asks)?;
        let level_count = (bids.iter().filter(|(_, units)| *units > 0).count()
            + asks.iter().filter(|(_, units)| *units > 0).count()) as u64;
        let mut resources = BusinessResourceDeltaV2::default();
        resources.add_records(level_count * 2);
        Ok(resources)
    }

    fn asset(
        &self,
        condition_id: &str,
        token_id: &str,
    ) -> Result<&AssetQueueV2, MatchingStateErrorV1> {
        self.markets
            .get(condition_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownMarket(condition_id.to_owned()))?
            .assets
            .get(token_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownAsset(token_id.to_owned()))
    }

    fn asset_mut(
        &mut self,
        condition_id: &str,
        token_id: &str,
    ) -> Result<&mut AssetQueueV2, MatchingStateErrorV1> {
        self.markets
            .get_mut(condition_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownMarket(condition_id.to_owned()))?
            .assets
            .get_mut(token_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownAsset(token_id.to_owned()))
    }

    fn matching_closed(&self, condition_id: &str) -> Result<bool, MatchingStateErrorV1> {
        self.markets
            .get(condition_id)
            .map(|market| market.matching_closed)
            .ok_or_else(|| MatchingStateErrorV1::UnknownMarket(condition_id.to_owned()))
    }

    fn end_matching(
        &mut self,
        condition_id: &str,
    ) -> Result<BusinessResourceDeltaV2, MatchingStateErrorV1> {
        let market = self
            .markets
            .get_mut(condition_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownMarket(condition_id.to_owned()))?;
        if market.matching_closed {
            return Ok(BusinessResourceDeltaV2::default());
        }
        market.matching_closed = true;
        let mut resources = BusinessResourceDeltaV2::default();
        for asset in market.assets.values_mut() {
            resources.merge(asset.clear_matching_segments());
        }
        Ok(resources)
    }
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BuyOrderTermsV2 {
    Gtc {
        limit_price_pusd_per_share: Decimal,
        accepted_base_units: u128,
        accepted_quote_units: u128,
        post_only: bool,
    },
    Gtd {
        limit_price_pusd_per_share: Decimal,
        accepted_base_units: u128,
        accepted_quote_units: u128,
        post_only: bool,
        expires_at_utc_ms: Option<i64>,
    },
    Fok {
        worst_price_pusd_per_share: Decimal,
        accepted_quote_budget_units: u128,
        sdk_taker_amount_audit_base_units: u128,
    },
    Fak {
        worst_price_pusd_per_share: Decimal,
        accepted_quote_budget_units: u128,
        sdk_taker_amount_audit_base_units: u128,
    },
}

impl BuyOrderTermsV2 {
    fn maximum_price(self) -> Decimal {
        match self {
            Self::Gtc {
                limit_price_pusd_per_share,
                ..
            }
            | Self::Gtd {
                limit_price_pusd_per_share,
                ..
            } => limit_price_pusd_per_share,
            Self::Fok {
                worst_price_pusd_per_share,
                ..
            }
            | Self::Fak {
                worst_price_pusd_per_share,
                ..
            } => worst_price_pusd_per_share,
        }
    }

    fn target(self) -> TakerTargetV1 {
        match self {
            Self::Gtc {
                accepted_base_units,
                accepted_quote_units,
                ..
            }
            | Self::Gtd {
                accepted_base_units,
                accepted_quote_units,
                ..
            } => TakerTargetV1::LimitBase {
                total_base_units: accepted_base_units,
                total_quote_units: accepted_quote_units,
            },
            Self::Fok {
                accepted_quote_budget_units,
                ..
            }
            | Self::Fak {
                accepted_quote_budget_units,
                ..
            } => TakerTargetV1::Quote(accepted_quote_budget_units),
        }
    }

    fn accepted_quote_units(self) -> u128 {
        match self {
            Self::Gtc {
                accepted_quote_units,
                ..
            }
            | Self::Gtd {
                accepted_quote_units,
                ..
            } => accepted_quote_units,
            Self::Fok {
                accepted_quote_budget_units,
                ..
            }
            | Self::Fak {
                accepted_quote_budget_units,
                ..
            } => accepted_quote_budget_units,
        }
    }

    fn post_only(self) -> bool {
        match self {
            Self::Gtc { post_only, .. } | Self::Gtd { post_only, .. } => post_only,
            Self::Fok { .. } | Self::Fak { .. } => false,
        }
    }

    fn is_fok(self) -> bool {
        matches!(self, Self::Fok { .. })
    }

    fn is_fak(self) -> bool {
        matches!(self, Self::Fak { .. })
    }

    fn order_kind(self) -> SimOrderKindStateV1 {
        match self {
            Self::Gtc { .. } => SimOrderKindStateV1::Gtc,
            Self::Gtd {
                expires_at_utc_ms, ..
            } => SimOrderKindStateV1::Gtd {
                expires_at_utc_ms: expires_at_utc_ms
                    .expect("已进入 V2 下单提交的 GTD 到期时点必须已经通过提前量门"),
            },
            Self::Fok { .. } => SimOrderKindStateV1::Fok,
            Self::Fak { .. } => SimOrderKindStateV1::Fak,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BuyOrderActionV2 {
    pub(crate) global_request_id: String,
    pub(crate) batch_item_ordinal: u16,
    pub(crate) item_id: String,
    pub(crate) condition_id: ConditionId,
    pub(crate) token_id: TokenId,
    pub(crate) effective_processing_at_utc_ms: i64,
    pub(crate) terms: BuyOrderTermsV2,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
enum OrderQuantityStateV2 {
    Base {
        accepted_base_units: u128,
        matched_base_units: u128,
        pending_maker_depletion_base_units: u128,
        open_base_units: u128,
        venue_dust_closed_base_units: u128,
        canceled_base_units: u128,
        expired_base_units: u128,
        market_resolved_canceled_base_units: u128,
        accepted_quote_units: u128,
        matched_quote_units: u128,
        open_encumbered_quote_units: u128,
        released_price_improvement_quote_units: u128,
        venue_dust_released_quote_units: u128,
        canceled_released_quote_units: u128,
        expired_released_quote_units: u128,
        market_resolved_released_quote_units: u128,
    },
    QuoteBudget {
        accepted_quote_budget_units: u128,
        matched_quote_units: u128,
        released_or_unspent_quote_units: u128,
        actual_matched_base_units: u128,
        sdk_taker_amount_audit_base_units: u128,
    },
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
struct SimOrderRecordV2 {
    platform_order_id: String,
    global_request_id: String,
    batch_item_ordinal: u16,
    item_id: String,
    condition_id: ConditionId,
    token_id: TokenId,
    created_at_utc_ms: i64,
    last_updated_at_utc_ms: i64,
    kind: SimOrderKindStateV1,
    source_price_pusd_per_share: Decimal,
    quantities: OrderQuantityStateV2,
    immediate_taker_fee_encumbered_units: u128,
    terminal_reason: Option<SimOrderTerminalReasonV1>,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
struct SimFillRecordV2 {
    trade_id: String,
    platform_order_id: String,
    matched_at_utc_ms: i64,
    liquidity_role: SimLiquidityRoleV1,
    price_pusd_per_share: Decimal,
    base_units: u128,
    quote_units: u128,
    immediate_taker_fee_units: u128,
    selected_finality_path: SelectedTradeFinalityPathV2,
    finality_path_ordinal: u32,
    economic_application_account_transition_seq: Option<u64>,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ScheduledFinalityTransitionKeyV2 {
    platform_event_at_utc_ms: i64,
    fill_sequence: u64,
    path_ordinal: u32,
}

#[allow(dead_code)]
#[derive(Debug, Default, PartialEq, Eq)]
struct RebateDayStateV2 {
    pending_predecessor_count: u64,
    accrued_pusd: Decimal,
    maturity_reached: bool,
    final_state: Option<RebateDayFinalStateV1>,
}

#[allow(dead_code)]
#[derive(Debug, Default, PartialEq, Eq)]
struct TakerRebateStateV2 {
    days: BTreeMap<i64, RebateDayStateV2>,
    clocks: BTreeSet<(i64, i64)>,
}

impl TakerRebateStateV2 {
    fn register_positive_fee_fill(
        &mut self,
        matched_at_utc_ms: i64,
    ) -> Result<BusinessResourceDeltaV2, RebateStateErrorV1> {
        let attribution_day_utc_ms = utc_day_start(matched_at_utc_ms)?;
        let maturity_at_utc_ms = rebate_maturity_at(RebateKindV1::Taker, attribution_day_utc_ms)?;
        let creates_day = !self.days.contains_key(&attribution_day_utc_ms);
        let day = self.days.entry(attribution_day_utc_ms).or_default();
        day.pending_predecessor_count = day
            .pending_predecessor_count
            .checked_add(1)
            .ok_or(RebateStateErrorV1::ArithmeticOverflow)?;
        if creates_day {
            self.clocks
                .insert((maturity_at_utc_ms, attribution_day_utc_ms));
        }
        let mut resources = BusinessResourceDeltaV2::default();
        if creates_day {
            resources.add_records(2);
        }
        Ok(resources)
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct MakerRebateMarketStateV2 {
    end_reached: bool,
    pending_fill_count: u64,
    successful_fill_count: u64,
    pending_by_day: BTreeMap<i64, u64>,
    successful_contributions: BTreeMap<u64, MakerRebateContributionV1>,
    result: Option<MakerRebateMarketResultStatusV1>,
}

#[allow(dead_code)]
#[derive(Debug, Default, PartialEq, Eq)]
struct MakerRebateStateV2 {
    pending_fill_sequences: BTreeSet<u64>,
    markets: BTreeMap<String, MakerRebateMarketStateV2>,
    days: BTreeMap<i64, RebateDayStateV2>,
    market_end_clocks: BTreeSet<(i64, String)>,
    day_clocks: BTreeSet<(i64, i64)>,
}

impl MakerRebateStateV2 {
    fn new(directory: &SessionMarketDirectoryV1) -> Self {
        let mut state = Self::default();
        for market in directory.markets() {
            state.commit_admitted_market(market);
        }
        state
    }

    fn commit_admitted_market(&mut self, market: &PlatformMarketV1) {
        let condition_id = market.market().condition_id.0.clone();
        self.markets
            .insert(condition_id.clone(), MakerRebateMarketStateV2::default());
        self.market_end_clocks
            .insert((market.market().window.end_at_utc_ms, condition_id));
    }

    fn register_fill(
        &mut self,
        fill_sequence: u64,
        condition_id: &str,
        matched_at_utc_ms: i64,
    ) -> Result<BusinessResourceDeltaV2, RebateStateErrorV1> {
        let attribution_day_utc_ms = utc_day_start(matched_at_utc_ms)?;
        let maturity_at_utc_ms = rebate_maturity_at(RebateKindV1::Maker, attribution_day_utc_ms)?;
        let creates_day = !self.days.contains_key(&attribution_day_utc_ms);
        let _ = self.pending_fill_sequences.insert(fill_sequence);
        let market = self
            .markets
            .get_mut(condition_id)
            .ok_or_else(|| RebateStateErrorV1::UnknownMarket(condition_id.to_owned()))?;
        market.pending_fill_count = market
            .pending_fill_count
            .checked_add(1)
            .ok_or(RebateStateErrorV1::ArithmeticOverflow)?;
        let creates_market_day = !market.pending_by_day.contains_key(&attribution_day_utc_ms);
        let pending_market_day = market
            .pending_by_day
            .entry(attribution_day_utc_ms)
            .or_default();
        *pending_market_day = pending_market_day
            .checked_add(1)
            .ok_or(RebateStateErrorV1::ArithmeticOverflow)?;
        if creates_day {
            self.day_clocks
                .insert((maturity_at_utc_ms, attribution_day_utc_ms));
        }
        if creates_market_day {
            let day = self.days.entry(attribution_day_utc_ms).or_default();
            day.pending_predecessor_count = day
                .pending_predecessor_count
                .checked_add(1)
                .ok_or(RebateStateErrorV1::ArithmeticOverflow)?;
        }
        let mut resources = BusinessResourceDeltaV2::default();
        resources.add_record(0);
        if creates_market_day {
            resources.add_record(0);
        }
        if creates_day {
            resources.add_records(2);
        }
        Ok(resources)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct MarketResolutionObligationV2 {
    resolution_event_id: String,
    resolution_source_event_at_utc_ms: i64,
    terminal: MarketResolutionTerminalV3,
    parsed_payouts: [Decimal; 2],
    pending_fill_sequences: BTreeSet<u64>,
    redeemed_at_utc_ms: Option<i64>,
    invalidated_by_quality_reason: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct GtdExpirationEffectV2 {
    pub(crate) platform_order_id: String,
    pub(crate) condition_id: ConditionId,
    pub(crate) token_id: TokenId,
    pub(crate) expires_at_utc_ms: i64,
    pub(crate) pending_base_units_before: u128,
    pub(crate) open_base_units_before: u128,
    pub(crate) released_open_quote_units: u128,
    pub(crate) account_transition_seq: Option<u64>,
}

#[allow(dead_code)]
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct GtdExpirationAdvanceEffectsV2 {
    pub(crate) expiration: Option<GtdExpirationEffectV2>,
    pub(crate) account_deltas: Vec<PlatformAccountDeltaV2>,
    pub(crate) resources: BusinessResourceDeltaV2,
}

impl GtdExpirationAdvanceEffectsV2 {
    pub(crate) fn into_lifecycle_feedback(
        self,
    ) -> Result<Vec<LifecycleFeedbackEffectV2>, MatchingStateErrorV1> {
        let Some(expiration) = self.expiration else {
            return Ok(Vec::new());
        };
        let mut account_deltas = account_deltas_by_sequence_v2(self.account_deltas);
        let expired_base_units = expiration
            .pending_base_units_before
            .checked_add(expiration.open_base_units_before)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let lifecycle = LifecycleFeedbackEffectV2::Order(OrderLifecycleEffectV2 {
            platform_event_at_utc_ms: expiration.expires_at_utc_ms,
            platform_order_id: expiration.platform_order_id,
            condition_id: expiration.condition_id,
            token_id: expiration.token_id,
            event: OrderLifecycleEventV2::GtdExpired {
                encoded_expires_at_utc_ms: expiration.expires_at_utc_ms,
                expired_base_quantity_shares: exact_decimal_from_micro_units_v2(
                    expired_base_units,
                )?,
                released_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                    expiration.released_open_quote_units,
                )?,
                account_delta: take_account_delta_v2(
                    &mut account_deltas,
                    expiration.account_transition_seq,
                ),
            },
        });
        debug_assert!(account_deltas.is_empty());
        Ok(vec![lifecycle])
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CallerCancelEffectV2 {
    pub(crate) condition_id: ConditionId,
    pub(crate) token_id: TokenId,
    pub(crate) pending_base_units_before: u128,
    pub(crate) open_base_units_before: u128,
    pub(crate) released_open_quote_units: u128,
    pub(crate) account_delta: Option<PlatformAccountDeltaV2>,
    pub(crate) resources: BusinessResourceDeltaV2,
}

impl CallerCancelEffectV2 {
    pub(crate) fn into_output_effects(
        self,
        platform_order_id: String,
        platform_effective_at_utc_ms: i64,
    ) -> Result<
        (
            CanceledTargetMeasurementV2,
            LifecycleFeedbackEffectV2,
            BusinessResourceDeltaV2,
        ),
        MatchingStateErrorV1,
    > {
        let canceled_base_units = self
            .pending_base_units_before
            .checked_add(self.open_base_units_before)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let account_transition_seq = self
            .account_delta
            .as_ref()
            .map(|delta| delta.account_transition_seq);
        let measurement = CanceledTargetMeasurementV2 {
            canceled_base_quantity_shares: exact_decimal_from_micro_units_v2(canceled_base_units)?,
            released_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                self.released_open_quote_units,
            )?,
            account_transition_seq,
        };
        let lifecycle = LifecycleFeedbackEffectV2::Order(OrderLifecycleEffectV2 {
            platform_event_at_utc_ms: platform_effective_at_utc_ms,
            platform_order_id,
            condition_id: self.condition_id,
            token_id: self.token_id,
            event: OrderLifecycleEventV2::CallerCanceled {
                canceled_base_quantity_shares: measurement.canceled_base_quantity_shares.clone(),
                released_quote_amount_pusd: measurement.released_quote_amount_pusd.clone(),
                account_delta: self.account_delta,
            },
        });
        Ok((measurement, lifecycle, self.resources))
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CallerCancelDispositionV2 {
    NotFound,
    NoCancellableRemainder,
    Canceled(CallerCancelEffectV2),
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TradeFinalityTransitionEffectV2 {
    pub(crate) fill_sequence: u64,
    pub(crate) trade_id: String,
    pub(crate) platform_order_id: String,
    pub(crate) condition_id: ConditionId,
    pub(crate) token_id: TokenId,
    pub(crate) matched_at_utc_ms: i64,
    pub(crate) platform_event_at_utc_ms: i64,
    pub(crate) path_ordinal: u32,
    pub(crate) finality_state: TradeFinalityStateV1,
    pub(crate) economic_disposition: TradeEconomicDispositionStateV1,
    pub(crate) liquidity_role: SimLiquidityRoleV1,
    pub(crate) source_price_pusd_per_share: Decimal,
    pub(crate) base_units: u128,
    pub(crate) quote_units: u128,
    pub(crate) immediate_taker_fee_units: u128,
    pub(crate) economic_application_account_transition_seq: Option<u64>,
    pub(crate) account_transition_seq: Option<u64>,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OrderLifecycleEffectV2 {
    pub(crate) platform_event_at_utc_ms: i64,
    pub(crate) platform_order_id: String,
    pub(crate) condition_id: ConditionId,
    pub(crate) token_id: TokenId,
    pub(crate) event: OrderLifecycleEventV2,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TradeLifecycleEffectV2 {
    pub(crate) platform_event_at_utc_ms: i64,
    pub(crate) path_ordinal: u32,
    pub(crate) trade_id: String,
    pub(crate) platform_order_id: String,
    pub(crate) condition_id: ConditionId,
    pub(crate) token_id: TokenId,
    pub(crate) status: SimulatedTradeStatusV2,
    pub(crate) economic_disposition: TradeEconomicDispositionV2,
    pub(crate) liquidity_role: TradeLiquidityRoleV2,
    pub(crate) source_price_pusd_per_share: ExactDecimal,
    pub(crate) base_quantity_shares: ExactDecimal,
    pub(crate) quote_amount_pusd: ExactDecimal,
    pub(crate) immediate_taker_fee_pusd: ExactDecimal,
    pub(crate) economic_application_account_transition_seq: Option<u64>,
    pub(crate) account_delta: Option<PlatformAccountDeltaV2>,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SettlementLifecycleEffectV2 {
    pub(crate) platform_event_at_utc_ms: i64,
    pub(crate) event: SettlementLifecycleEventV2,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LifecycleFeedbackEffectV2 {
    Order(OrderLifecycleEffectV2),
    Trade(TradeLifecycleEffectV2),
    Settlement(SettlementLifecycleEffectV2),
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
struct VenueDustEffectV2 {
    platform_order_id: String,
    condition_id: ConditionId,
    token_id: TokenId,
    platform_effective_at_utc_ms: i64,
    closed_base_units: u128,
    released_quote_units: u128,
    account_transition_seq: Option<u64>,
}

fn trade_status_v2(state: TradeFinalityStateV1) -> SimulatedTradeStatusV2 {
    let (known, raw) = match state {
        TradeFinalityStateV1::MatchedNotBroadcasted => (
            SimulatedTradeKnownStatusV2::MatchedNotBroadcasted,
            "MATCHED_NOT_BROADCASTED",
        ),
        TradeFinalityStateV1::Matched => (SimulatedTradeKnownStatusV2::Matched, "MATCHED"),
        TradeFinalityStateV1::Mined => (SimulatedTradeKnownStatusV2::Mined, "MINED"),
        TradeFinalityStateV1::Confirmed => (SimulatedTradeKnownStatusV2::Confirmed, "CONFIRMED"),
        TradeFinalityStateV1::Retrying => (SimulatedTradeKnownStatusV2::Retrying, "RETRYING"),
        TradeFinalityStateV1::Failed => (SimulatedTradeKnownStatusV2::Failed, "FAILED"),
    };
    SimulatedTradeStatusV2 {
        raw: raw.to_owned(),
        known: Some(known),
    }
}

fn trade_economic_disposition_v2(
    disposition: TradeEconomicDispositionStateV1,
) -> TradeEconomicDispositionV2 {
    match disposition {
        TradeEconomicDispositionStateV1::PendingUnapplied => {
            TradeEconomicDispositionV2::PendingUnapplied
        }
        TradeEconomicDispositionStateV1::PendingApplied => {
            TradeEconomicDispositionV2::PendingApplied
        }
        TradeEconomicDispositionStateV1::FinalSuccess => TradeEconomicDispositionV2::FinalSuccess,
        TradeEconomicDispositionStateV1::FinalFailureUnapplied => {
            TradeEconomicDispositionV2::FinalFailureUnapplied
        }
        TradeEconomicDispositionStateV1::FinalFailureCompensated => {
            TradeEconomicDispositionV2::FinalFailureCompensated
        }
    }
}

fn trade_liquidity_role_v2(role: SimLiquidityRoleV1) -> TradeLiquidityRoleV2 {
    match role {
        SimLiquidityRoleV1::Taker => TradeLiquidityRoleV2::Taker,
        SimLiquidityRoleV1::MakerInferred => TradeLiquidityRoleV2::MakerInferred,
    }
}

fn account_deltas_by_sequence_v2(
    account_deltas: Vec<PlatformAccountDeltaV2>,
) -> BTreeMap<u64, PlatformAccountDeltaV2> {
    account_deltas
        .into_iter()
        .map(|delta| (delta.account_transition_seq, delta))
        .collect()
}

fn take_account_delta_v2(
    account_deltas: &mut BTreeMap<u64, PlatformAccountDeltaV2>,
    account_transition_seq: Option<u64>,
) -> Option<PlatformAccountDeltaV2> {
    account_transition_seq.and_then(|sequence| account_deltas.remove(&sequence))
}

fn take_account_delta_by_reason_v2(
    account_deltas: &mut BTreeMap<u64, PlatformAccountDeltaV2>,
    matches_reason: impl Fn(&PlatformAccountDeltaReasonV2) -> bool,
) -> Option<PlatformAccountDeltaV2> {
    let sequence = account_deltas
        .iter()
        .find_map(|(sequence, delta)| matches_reason(&delta.reason).then_some(*sequence))?;
    account_deltas.remove(&sequence)
}

fn submission_status_v2(status: SubmissionPlatformOrderStatusV2) -> SimulatedOrderStatusV2 {
    let (raw, known) = match status {
        SubmissionPlatformOrderStatusV2::Live => ("LIVE", SimulatedOrderKnownStatusV2::Live),
        SubmissionPlatformOrderStatusV2::Matched => {
            ("MATCHED", SimulatedOrderKnownStatusV2::Matched)
        }
        SubmissionPlatformOrderStatusV2::VenueDustTerminal => (
            "VENUE_DUST_TERMINAL",
            SimulatedOrderKnownStatusV2::VenueDustTerminal,
        ),
    };
    SimulatedOrderStatusV2 {
        raw: raw.to_owned(),
        known: Some(known),
    }
}

fn submission_terminal_reason_v2(
    reason: Option<SimOrderTerminalReasonV1>,
) -> Option<SimOrderTerminalReasonV2> {
    reason.map(|reason| match reason {
        SimOrderTerminalReasonV1::FakCompleted => SimOrderTerminalReasonV2::FakCompleted,
        SimOrderTerminalReasonV1::VenueDustTerminal => SimOrderTerminalReasonV2::VenueDustTerminal,
        SimOrderTerminalReasonV1::MarketResolvedCanceled => {
            SimOrderTerminalReasonV2::MarketResolvedCanceled
        }
    })
}

impl TradeFinalityTransitionEffectV2 {
    fn into_lifecycle_feedback(
        self,
        account_delta: Option<PlatformAccountDeltaV2>,
    ) -> Result<LifecycleFeedbackEffectV2, MatchingStateErrorV1> {
        Ok(LifecycleFeedbackEffectV2::Trade(TradeLifecycleEffectV2 {
            platform_event_at_utc_ms: self.platform_event_at_utc_ms,
            path_ordinal: self.path_ordinal,
            trade_id: self.trade_id,
            platform_order_id: self.platform_order_id,
            condition_id: self.condition_id,
            token_id: self.token_id,
            status: trade_status_v2(self.finality_state),
            economic_disposition: trade_economic_disposition_v2(self.economic_disposition),
            liquidity_role: trade_liquidity_role_v2(self.liquidity_role),
            source_price_pusd_per_share: exact_decimal_v2(self.source_price_pusd_per_share),
            base_quantity_shares: exact_decimal_from_micro_units_v2(self.base_units)?,
            quote_amount_pusd: exact_decimal_from_micro_units_v2(self.quote_units)?,
            immediate_taker_fee_pusd: exact_decimal_from_micro_units_v2(
                self.immediate_taker_fee_units,
            )?,
            economic_application_account_transition_seq: self
                .economic_application_account_transition_seq,
            account_delta,
        }))
    }
}

fn economic_lifecycle_effect_v2(
    effect: EconomicLifecycleEffectV2,
    account_deltas: &mut BTreeMap<u64, PlatformAccountDeltaV2>,
) -> Result<LifecycleFeedbackEffectV2, MatchingStateErrorV1> {
    match effect {
        EconomicLifecycleEffectV2::MarketResolutionOrder(effect) => {
            let canceled_base_units = effect
                .pending_base_units_before
                .checked_add(effect.open_base_units_before)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            Ok(LifecycleFeedbackEffectV2::Order(OrderLifecycleEffectV2 {
                platform_event_at_utc_ms: effect.platform_effective_at_utc_ms,
                platform_order_id: effect.platform_order_id,
                condition_id: effect.condition_id,
                token_id: effect.token_id,
                event: OrderLifecycleEventV2::MarketResolvedCanceled {
                    resolution_event_id: effect.resolution_event_id,
                    canceled_base_quantity_shares: exact_decimal_from_micro_units_v2(
                        canceled_base_units,
                    )?,
                    released_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                        effect.released_open_quote_units,
                    )?,
                    account_delta: take_account_delta_v2(
                        account_deltas,
                        effect.account_transition_seq,
                    ),
                },
            }))
        }
        EconomicLifecycleEffectV2::MakerMarketFinalized(effect) => {
            let account_delta =
                take_account_delta_v2(account_deltas, effect.account_transition_seq);
            let status = match effect.status {
                MakerRebateMarketResultStatusV1::Calculated => {
                    MakerRebateAccrualStatusV2::Calculated
                }
                MakerRebateMarketResultStatusV1::ZeroNoEligibleMakerFill => {
                    MakerRebateAccrualStatusV2::ZeroNoEligibleMakerFill
                }
            };
            let contributions = effect
                .contributions
                .into_iter()
                .map(|contribution| {
                    Ok(MakerRebateFillContributionV2 {
                        trade_id: contribution.trade_id,
                        matched_at_utc_ms: contribution.matched_at_utc_ms,
                        attribution_day_start_utc_ms: contribution.attribution_day_utc_ms,
                        base_quantity_shares: exact_decimal_from_micro_units_v2(
                            contribution.base_units,
                        )?,
                        price_pusd_per_share: exact_decimal_v2(contribution.price_pusd_per_share),
                        maker_fee_equivalent_pusd: exact_decimal_v2(
                            contribution.fee_equivalent_pusd,
                        ),
                        rebate_rate: exact_decimal_v2(contribution.rebate_rate),
                        rebate_contribution_pusd: exact_decimal_v2(contribution.contribution_pusd),
                    })
                })
                .collect::<Result<Vec<_>, MatchingStateErrorV1>>()?;
            Ok(LifecycleFeedbackEffectV2::Settlement(
                SettlementLifecycleEffectV2 {
                    platform_event_at_utc_ms: effect.platform_effective_at_utc_ms,
                    event: SettlementLifecycleEventV2::MakerRebateAccrualFinalized {
                        condition_id: effect.condition_id,
                        status,
                        contributions,
                        accrual_pusd: exact_decimal_v2(effect.accrual_pusd),
                        account_delta,
                    },
                },
            ))
        }
        EconomicLifecycleEffectV2::TakerAccrual(effect) => {
            let account_delta =
                take_account_delta_v2(account_deltas, Some(effect.account_transition_seq))
                    .expect("V2 taker 应计效果必须拥有同次唯一账户变化");
            Ok(LifecycleFeedbackEffectV2::Settlement(
                SettlementLifecycleEffectV2 {
                    platform_event_at_utc_ms: effect.platform_effective_at_utc_ms,
                    event: SettlementLifecycleEventV2::TakerRebateAccrualApplied {
                        trade_id: effect.trade_id,
                        condition_id: effect.condition_id,
                        matched_at_utc_ms: effect.matched_at_utc_ms,
                        attribution_day_start_utc_ms: effect.attribution_day_utc_ms,
                        actual_taker_fee_pusd: exact_decimal_v2(effect.actual_taker_fee_pusd),
                        rebate_rate: exact_decimal_v2(effect.rebate_rate),
                        accrual_pusd: exact_decimal_v2(effect.accrual_pusd),
                        account_delta,
                    },
                },
            ))
        }
        EconomicLifecycleEffectV2::AutoRedeem(effect) => Ok(LifecycleFeedbackEffectV2::Settlement(
            SettlementLifecycleEffectV2 {
                platform_event_at_utc_ms: effect.platform_effective_at_utc_ms,
                event: SettlementLifecycleEventV2::AutoRedeemApplied {
                    condition_id: effect.condition_id,
                    resolution_event_id: effect.resolution_event_id,
                    resolution_source_event_at_utc_ms: effect.resolution_source_event_at_utc_ms,
                    resolution: effect.terminal,
                    redeemed_outcome_balances: effect.redeemed_outcome_balances,
                    redeemed_pusd: exact_decimal_v2(effect.redeemed_pusd),
                    account_delta: take_account_delta_v2(
                        account_deltas,
                        effect.account_transition_seq,
                    ),
                },
            },
        )),
        EconomicLifecycleEffectV2::DayFinalized(effect) => {
            let event = match effect.kind {
                RebateKindV1::Maker => SettlementLifecycleEventV2::MakerRebateDayFinalized {
                    attribution_day_start_utc_ms: effect.attribution_day_utc_ms,
                    accrual_pusd: exact_decimal_v2(effect.accrued_pusd),
                    payable_pusd: exact_decimal_v2(effect.payable_pusd),
                    locked_remainder_pusd: exact_decimal_v2(effect.locked_remainder_pusd),
                    account_delta: take_account_delta_v2(
                        account_deltas,
                        effect.account_transition_seq,
                    ),
                },
                RebateKindV1::Taker => SettlementLifecycleEventV2::TakerRebateDayFinalized {
                    attribution_day_start_utc_ms: effect.attribution_day_utc_ms,
                    accrual_pusd: exact_decimal_v2(effect.accrued_pusd),
                    payable_pusd: exact_decimal_v2(effect.payable_pusd),
                    locked_remainder_pusd: exact_decimal_v2(effect.locked_remainder_pusd),
                    account_delta: take_account_delta_v2(
                        account_deltas,
                        effect.account_transition_seq,
                    ),
                },
            };
            Ok(LifecycleFeedbackEffectV2::Settlement(
                SettlementLifecycleEffectV2 {
                    platform_event_at_utc_ms: effect.platform_effective_at_utc_ms,
                    event,
                },
            ))
        }
        EconomicLifecycleEffectV2::Paid(effect) => {
            let account_delta =
                take_account_delta_v2(account_deltas, Some(effect.account_transition_seq))
                    .expect("V2 返佣付款效果必须拥有同次唯一账户变化");
            let event = match effect.kind {
                RebateKindV1::Maker => SettlementLifecycleEventV2::MakerRebatePaid {
                    attribution_day_start_utc_ms: effect.attribution_day_utc_ms,
                    paid_pusd: exact_decimal_v2(effect.paid_pusd),
                    account_delta,
                },
                RebateKindV1::Taker => SettlementLifecycleEventV2::TakerRebatePaid {
                    attribution_day_start_utc_ms: effect.attribution_day_utc_ms,
                    paid_pusd: exact_decimal_v2(effect.paid_pusd),
                    account_delta,
                },
            };
            Ok(LifecycleFeedbackEffectV2::Settlement(
                SettlementLifecycleEffectV2 {
                    platform_event_at_utc_ms: effect.platform_effective_at_utc_ms,
                    event,
                },
            ))
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TakerRebateAccrualEffectV2 {
    pub(crate) fill_sequence: u64,
    pub(crate) trade_id: String,
    pub(crate) condition_id: ConditionId,
    pub(crate) matched_at_utc_ms: i64,
    pub(crate) attribution_day_utc_ms: i64,
    pub(crate) platform_effective_at_utc_ms: i64,
    pub(crate) actual_taker_fee_pusd: Decimal,
    pub(crate) rebate_rate: Decimal,
    pub(crate) accrual_pusd: Decimal,
    pub(crate) account_transition_seq: u64,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MakerRebateMarketFinalizedEffectV2 {
    pub(crate) condition_id: ConditionId,
    pub(crate) platform_effective_at_utc_ms: i64,
    pub(crate) status: MakerRebateMarketResultStatusV1,
    pub(crate) contributions: Vec<MakerRebateContributionV1>,
    pub(crate) accrual_pusd: Decimal,
    pub(crate) account_transition_seq: Option<u64>,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RebateDayFinalizedEffectV2 {
    pub(crate) kind: RebateKindV1,
    pub(crate) attribution_day_utc_ms: i64,
    pub(crate) platform_effective_at_utc_ms: i64,
    pub(crate) accrued_pusd: Decimal,
    pub(crate) payable_pusd: Decimal,
    pub(crate) locked_remainder_pusd: Decimal,
    pub(crate) account_transition_seq: Option<u64>,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RebatePaidEffectV2 {
    pub(crate) kind: RebateKindV1,
    pub(crate) attribution_day_utc_ms: i64,
    pub(crate) platform_effective_at_utc_ms: i64,
    pub(crate) paid_pusd: Decimal,
    pub(crate) account_transition_seq: u64,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MarketResolutionOrderEffectV2 {
    pub(crate) platform_order_id: String,
    pub(crate) condition_id: ConditionId,
    pub(crate) token_id: TokenId,
    pub(crate) resolution_event_id: String,
    pub(crate) platform_effective_at_utc_ms: i64,
    pub(crate) pending_base_units_before: u128,
    pub(crate) open_base_units_before: u128,
    pub(crate) released_open_quote_units: u128,
    pub(crate) account_transition_seq: Option<u64>,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct AutoRedeemEffectV2 {
    pub(crate) condition_id: ConditionId,
    pub(crate) resolution_event_id: String,
    pub(crate) resolution_source_event_at_utc_ms: i64,
    pub(crate) terminal: MarketResolutionTerminalV3,
    pub(crate) platform_effective_at_utc_ms: i64,
    pub(crate) redeemed_outcome_balances: Vec<PlatformOutcomeBalanceV1>,
    pub(crate) redeemed_pusd: Decimal,
    pub(crate) account_transition_seq: Option<u64>,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum EconomicLifecycleEffectV2 {
    MarketResolutionOrder(MarketResolutionOrderEffectV2),
    MakerMarketFinalized(MakerRebateMarketFinalizedEffectV2),
    TakerAccrual(TakerRebateAccrualEffectV2),
    AutoRedeem(AutoRedeemEffectV2),
    DayFinalized(RebateDayFinalizedEffectV2),
    Paid(RebatePaidEffectV2),
}

#[allow(dead_code)]
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ScheduledFinalityAdvanceEffectsV2 {
    pub(crate) transition: Option<TradeFinalityLifecycleEffectsV2>,
    pub(crate) account_deltas: Vec<PlatformAccountDeltaV2>,
    pub(crate) resources: BusinessResourceDeltaV2,
}

impl ScheduledFinalityAdvanceEffectsV2 {
    pub(crate) fn into_lifecycle_feedback(
        self,
    ) -> Result<Vec<LifecycleFeedbackEffectV2>, MatchingStateErrorV1> {
        let Some(transition) = self.transition else {
            return Ok(Vec::new());
        };
        let mut account_deltas = account_deltas_by_sequence_v2(self.account_deltas);
        let mut lifecycle = Vec::with_capacity(1 + transition.settlement.len());
        let account_delta = take_account_delta_v2(
            &mut account_deltas,
            transition.trade_finality.account_transition_seq,
        );
        lifecycle.push(
            transition
                .trade_finality
                .into_lifecycle_feedback(account_delta)?,
        );
        for settlement in transition.settlement {
            lifecycle.push(economic_lifecycle_effect_v2(
                settlement,
                &mut account_deltas,
            )?);
        }
        debug_assert!(account_deltas.is_empty());
        Ok(lifecycle)
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TradeFinalityLifecycleEffectsV2 {
    pub(crate) trade_finality: TradeFinalityTransitionEffectV2,
    pub(crate) settlement: Vec<EconomicLifecycleEffectV2>,
}

#[allow(dead_code)]
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RebateClockAdvanceEffectsV2 {
    pub(crate) settlement: Vec<EconomicLifecycleEffectV2>,
    pub(crate) account_deltas: Vec<PlatformAccountDeltaV2>,
    pub(crate) resources: BusinessResourceDeltaV2,
}

impl RebateClockAdvanceEffectsV2 {
    pub(crate) fn into_lifecycle_feedback(
        self,
    ) -> Result<Vec<LifecycleFeedbackEffectV2>, MatchingStateErrorV1> {
        let mut account_deltas = account_deltas_by_sequence_v2(self.account_deltas);
        let mut lifecycle = Vec::with_capacity(self.settlement.len());
        for settlement in self.settlement {
            lifecycle.push(economic_lifecycle_effect_v2(
                settlement,
                &mut account_deltas,
            )?);
        }
        debug_assert!(account_deltas.is_empty());
        Ok(lifecycle)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum PendingRebateClockV2 {
    MakerMarketEnd(String),
    MakerDay(i64),
    TakerDay(i64),
}

fn exact_decimal_v2(value: Decimal) -> ExactDecimal {
    ExactDecimal(value.normalize().to_string())
}

fn exact_decimal_from_micro_units_v2(value: u128) -> Result<ExactDecimal, MatchingStateErrorV1> {
    decimal_from_micro_units_v1(value)
        .map(exact_decimal_v2)
        .map_err(MatchingStateErrorV1::from)
}

fn order_projection_v2(
    order: &SimOrderRecordV2,
) -> Result<OrderProjectionV2, MatchingStateErrorV1> {
    let (kind, expires_at_utc_ms) = match order.kind {
        SimOrderKindStateV1::Gtc => (OrderKindProjectionV2::Gtc, None),
        SimOrderKindStateV1::Gtd { expires_at_utc_ms } => {
            (OrderKindProjectionV2::Gtd, Some(expires_at_utc_ms))
        }
        SimOrderKindStateV1::Fok => (OrderKindProjectionV2::Fok, None),
        SimOrderKindStateV1::Fak => (OrderKindProjectionV2::Fak, None),
    };
    let (active_base_units, status_after_inactive, quantities) = match &order.quantities {
        OrderQuantityStateV2::Base {
            accepted_base_units,
            matched_base_units,
            pending_maker_depletion_base_units,
            open_base_units,
            venue_dust_closed_base_units,
            canceled_base_units,
            expired_base_units,
            market_resolved_canceled_base_units,
            accepted_quote_units,
            matched_quote_units,
            open_encumbered_quote_units,
            released_price_improvement_quote_units,
            venue_dust_released_quote_units,
            canceled_released_quote_units,
            expired_released_quote_units,
            market_resolved_released_quote_units,
        } => {
            let active_base_units = pending_maker_depletion_base_units
                .checked_add(*open_base_units)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            let status_after_inactive = if *market_resolved_canceled_base_units > 0 {
                SimulatedOrderKnownStatusV2::CanceledMarketResolved
            } else if *venue_dust_closed_base_units > 0 {
                SimulatedOrderKnownStatusV2::VenueDustTerminal
            } else if *canceled_base_units > 0 {
                SimulatedOrderKnownStatusV2::Canceled
            } else if *expired_base_units > 0 {
                SimulatedOrderKnownStatusV2::Expired
            } else {
                SimulatedOrderKnownStatusV2::Matched
            };
            (
                active_base_units,
                status_after_inactive,
                OrderQuantityProjectionV2::Base {
                    accepted_base_quantity_shares: exact_decimal_from_micro_units_v2(
                        *accepted_base_units,
                    )?,
                    matched_base_quantity_shares: exact_decimal_from_micro_units_v2(
                        *matched_base_units,
                    )?,
                    active_unfilled_base_quantity_shares: exact_decimal_from_micro_units_v2(
                        active_base_units,
                    )?,
                    venue_dust_closed_base_quantity_shares: exact_decimal_from_micro_units_v2(
                        *venue_dust_closed_base_units,
                    )?,
                    market_resolved_canceled_base_quantity_shares:
                        exact_decimal_from_micro_units_v2(*market_resolved_canceled_base_units)?,
                    canceled_base_quantity_shares: exact_decimal_from_micro_units_v2(
                        *canceled_base_units,
                    )?,
                    expired_base_quantity_shares: exact_decimal_from_micro_units_v2(
                        *expired_base_units,
                    )?,
                    accepted_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                        *accepted_quote_units,
                    )?,
                    matched_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                        *matched_quote_units,
                    )?,
                    open_encumbered_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                        *open_encumbered_quote_units,
                    )?,
                    venue_dust_released_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                        *venue_dust_released_quote_units,
                    )?,
                    market_resolved_released_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                        *market_resolved_released_quote_units,
                    )?,
                    released_price_improvement_quote_amount_pusd:
                        exact_decimal_from_micro_units_v2(*released_price_improvement_quote_units)?,
                    canceled_released_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                        *canceled_released_quote_units,
                    )?,
                    expired_released_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                        *expired_released_quote_units,
                    )?,
                },
            )
        }
        OrderQuantityStateV2::QuoteBudget {
            accepted_quote_budget_units,
            matched_quote_units,
            released_or_unspent_quote_units,
            actual_matched_base_units,
            sdk_taker_amount_audit_base_units,
        } => (
            0,
            SimulatedOrderKnownStatusV2::Matched,
            OrderQuantityProjectionV2::QuoteBudget {
                accepted_quote_budget_pusd: exact_decimal_from_micro_units_v2(
                    *accepted_quote_budget_units,
                )?,
                matched_quote_amount_pusd: exact_decimal_from_micro_units_v2(*matched_quote_units)?,
                released_or_unspent_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                    *released_or_unspent_quote_units,
                )?,
                actual_matched_base_quantity_shares: exact_decimal_from_micro_units_v2(
                    *actual_matched_base_units,
                )?,
                sdk_taker_amount_audit_base_quantity_shares: exact_decimal_from_micro_units_v2(
                    *sdk_taker_amount_audit_base_units,
                )?,
            },
        ),
    };
    let known_status = if active_base_units > 0 {
        SimulatedOrderKnownStatusV2::Live
    } else {
        status_after_inactive
    };
    let raw_status = match known_status {
        SimulatedOrderKnownStatusV2::Live => "LIVE",
        SimulatedOrderKnownStatusV2::Matched => "MATCHED",
        SimulatedOrderKnownStatusV2::VenueDustTerminal => "VENUE_DUST_TERMINAL",
        SimulatedOrderKnownStatusV2::CanceledMarketResolved => {
            "ORDER_STATUS_CANCELED_MARKET_RESOLVED"
        }
        SimulatedOrderKnownStatusV2::Canceled => "CANCELED",
        SimulatedOrderKnownStatusV2::Expired => "EXPIRED",
        SimulatedOrderKnownStatusV2::Delayed => "DELAYED",
        SimulatedOrderKnownStatusV2::Unmatched => "UNMATCHED",
    };
    let terminal_reason = order.terminal_reason.map(|reason| match reason {
        SimOrderTerminalReasonV1::FakCompleted => SimOrderTerminalReasonV2::FakCompleted,
        SimOrderTerminalReasonV1::VenueDustTerminal => SimOrderTerminalReasonV2::VenueDustTerminal,
        SimOrderTerminalReasonV1::MarketResolvedCanceled => {
            SimOrderTerminalReasonV2::MarketResolvedCanceled
        }
    });
    Ok(OrderProjectionV2 {
        platform_order_id: order.platform_order_id.clone(),
        global_request_id: order.global_request_id.clone(),
        item_id: order.item_id.clone(),
        condition_id: order.condition_id.clone(),
        token_id: order.token_id.clone(),
        created_at_utc_ms: order.created_at_utc_ms,
        kind,
        expires_at_utc_ms,
        source_price_pusd_per_share: exact_decimal_v2(order.source_price_pusd_per_share),
        status: SimulatedOrderStatusV2 {
            raw: raw_status.to_owned(),
            known: Some(known_status),
        },
        terminal_reason,
        quantities,
        immediate_taker_fee_encumbered_pusd: exact_decimal_from_micro_units_v2(
            order.immediate_taker_fee_encumbered_units,
        )?,
    })
}

fn trade_projection_v2(
    fill: &SimFillRecordV2,
    order: &SimOrderRecordV2,
    finality_policy: &TradeFinalityPolicyV2,
) -> Result<TradeProjectionV2, MatchingStateErrorV1> {
    let path = finality_policy.path_for_selection(fill.selected_finality_path);
    let finality_state = path
        .steps()
        .get(usize::try_from(fill.finality_path_ordinal).expect("V2 最终性 ordinal 可表示为 usize"))
        .expect("V2 成交只引用已密封路径步骤")
        .state();
    let (known_status, raw_status) = match finality_state {
        TradeFinalityStateV1::MatchedNotBroadcasted => (
            SimulatedTradeKnownStatusV2::MatchedNotBroadcasted,
            "MATCHED_NOT_BROADCASTED",
        ),
        TradeFinalityStateV1::Matched => (SimulatedTradeKnownStatusV2::Matched, "MATCHED"),
        TradeFinalityStateV1::Retrying => (SimulatedTradeKnownStatusV2::Retrying, "RETRYING"),
        TradeFinalityStateV1::Mined => (SimulatedTradeKnownStatusV2::Mined, "MINED"),
        TradeFinalityStateV1::Confirmed => (SimulatedTradeKnownStatusV2::Confirmed, "CONFIRMED"),
        TradeFinalityStateV1::Failed => (SimulatedTradeKnownStatusV2::Failed, "FAILED"),
    };
    let economic_disposition = match finality_state {
        TradeFinalityStateV1::Confirmed => TradeEconomicDispositionV2::FinalSuccess,
        TradeFinalityStateV1::Failed
            if fill.economic_application_account_transition_seq.is_some() =>
        {
            TradeEconomicDispositionV2::FinalFailureCompensated
        }
        TradeFinalityStateV1::Failed => TradeEconomicDispositionV2::FinalFailureUnapplied,
        TradeFinalityStateV1::MatchedNotBroadcasted
        | TradeFinalityStateV1::Matched
        | TradeFinalityStateV1::Retrying
        | TradeFinalityStateV1::Mined
            if fill.economic_application_account_transition_seq.is_some() =>
        {
            TradeEconomicDispositionV2::PendingApplied
        }
        TradeFinalityStateV1::MatchedNotBroadcasted
        | TradeFinalityStateV1::Matched
        | TradeFinalityStateV1::Retrying
        | TradeFinalityStateV1::Mined => TradeEconomicDispositionV2::PendingUnapplied,
    };
    Ok(TradeProjectionV2 {
        trade_id: fill.trade_id.clone(),
        platform_order_id: fill.platform_order_id.clone(),
        condition_id: order.condition_id.clone(),
        token_id: order.token_id.clone(),
        matched_at_utc_ms: fill.matched_at_utc_ms,
        status: SimulatedTradeStatusV2 {
            raw: raw_status.to_owned(),
            known: Some(known_status),
        },
        liquidity_role: match fill.liquidity_role {
            SimLiquidityRoleV1::Taker => TradeLiquidityRoleV2::Taker,
            SimLiquidityRoleV1::MakerInferred => TradeLiquidityRoleV2::MakerInferred,
        },
        source_price_pusd_per_share: exact_decimal_v2(fill.price_pusd_per_share),
        base_quantity_shares: exact_decimal_from_micro_units_v2(fill.base_units)?,
        quote_amount_pusd: exact_decimal_from_micro_units_v2(fill.quote_units)?,
        immediate_taker_fee_pusd: exact_decimal_from_micro_units_v2(
            fill.immediate_taker_fee_units,
        )?,
        economic_disposition,
    })
}

pub(crate) fn order_projection_owned_bytes_v2(order: &OrderProjectionV2) -> u64 {
    let quantity_bytes: u64 = match &order.quantities {
        OrderQuantityProjectionV2::Base {
            accepted_base_quantity_shares,
            matched_base_quantity_shares,
            active_unfilled_base_quantity_shares,
            venue_dust_closed_base_quantity_shares,
            market_resolved_canceled_base_quantity_shares,
            canceled_base_quantity_shares,
            expired_base_quantity_shares,
            accepted_quote_amount_pusd,
            matched_quote_amount_pusd,
            open_encumbered_quote_amount_pusd,
            venue_dust_released_quote_amount_pusd,
            market_resolved_released_quote_amount_pusd,
            released_price_improvement_quote_amount_pusd,
            canceled_released_quote_amount_pusd,
            expired_released_quote_amount_pusd,
        } => [
            accepted_base_quantity_shares,
            matched_base_quantity_shares,
            active_unfilled_base_quantity_shares,
            venue_dust_closed_base_quantity_shares,
            market_resolved_canceled_base_quantity_shares,
            canceled_base_quantity_shares,
            expired_base_quantity_shares,
            accepted_quote_amount_pusd,
            matched_quote_amount_pusd,
            open_encumbered_quote_amount_pusd,
            venue_dust_released_quote_amount_pusd,
            market_resolved_released_quote_amount_pusd,
            released_price_improvement_quote_amount_pusd,
            canceled_released_quote_amount_pusd,
            expired_released_quote_amount_pusd,
        ]
        .into_iter()
        .map(|value| value.0.len() as u64)
        .sum(),
        OrderQuantityProjectionV2::QuoteBudget {
            accepted_quote_budget_pusd,
            matched_quote_amount_pusd,
            released_or_unspent_quote_amount_pusd,
            actual_matched_base_quantity_shares,
            sdk_taker_amount_audit_base_quantity_shares,
        } => [
            accepted_quote_budget_pusd,
            matched_quote_amount_pusd,
            released_or_unspent_quote_amount_pusd,
            actual_matched_base_quantity_shares,
            sdk_taker_amount_audit_base_quantity_shares,
        ]
        .into_iter()
        .map(|value| value.0.len() as u64)
        .sum(),
    };
    (order.platform_order_id.len()
        + order.global_request_id.len()
        + order.item_id.len()
        + order.condition_id.0.len()
        + order.token_id.0.len()
        + order.source_price_pusd_per_share.0.len()
        + order.status.raw.len()
        + order.immediate_taker_fee_encumbered_pusd.0.len()) as u64
        + quantity_bytes
}

pub(crate) fn trade_projection_owned_bytes_v2(trade: &TradeProjectionV2) -> u64 {
    (trade.trade_id.len()
        + trade.platform_order_id.len()
        + trade.condition_id.0.len()
        + trade.token_id.0.len()
        + trade.status.raw.len()
        + trade.source_price_pusd_per_share.0.len()
        + trade.base_quantity_shares.0.len()
        + trade.quote_amount_pusd.0.len()
        + trade.immediate_taker_fee_pusd.0.len()) as u64
}

fn market_resolution_obligation_owned_bytes_v2(
    condition_id: &str,
    obligation: &MarketResolutionObligationV2,
) -> u64 {
    let MarketResolutionTerminalV3::Resolved {
        winning_asset_id,
        winning_outcome,
        payout_vector,
        mapping_rule_version,
    } = &obligation.terminal;
    condition_id.len() as u64
        + obligation.resolution_event_id.len() as u64
        + obligation
            .invalidated_by_quality_reason
            .as_ref()
            .map_or(0, |reason| reason.len() as u64)
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

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BuyOrderRejectedReasonV2 {
    PostOnlyWouldTake,
    BelowMarketableLimitBuyMinimumPrincipal,
    GtdExpirationTooSoon,
    InsufficientAvailablePusd,
    FokNotFullyFilled,
    FakNoMatch,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SubmissionPlatformOrderStatusV2 {
    Live,
    Matched,
    VenueDustTerminal,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SubmissionMeasurementV2 {
    Base {
        accepted_base_units: u128,
        matched_base_units: u128,
        open_base_units: u128,
        venue_dust_closed_base_units: u128,
        order_type_terminated_base_units: u128,
        accepted_quote_units: u128,
        matched_quote_units: u128,
        open_encumbered_quote_units: u128,
        venue_dust_released_quote_units: u128,
        released_price_improvement_quote_units: u128,
        immediate_taker_fee_encumbered_units: u128,
    },
    QuoteBudget {
        accepted_quote_budget_units: u128,
        matched_quote_units: u128,
        released_or_unspent_quote_units: u128,
        actual_matched_base_units: u128,
        sdk_taker_amount_audit_base_units: u128,
        immediate_taker_fee_encumbered_units: u128,
    },
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BuyOrderAcceptedV2 {
    pub(crate) platform_order_id: String,
    platform_order_status: SubmissionPlatformOrderStatusV2,
    terminal_reason: Option<SimOrderTerminalReasonV1>,
    measurement: SubmissionMeasurementV2,
    initial_trade_finality: Vec<TradeFinalityTransitionEffectV2>,
    account_deltas: Vec<PlatformAccountDeltaV2>,
    pub(crate) resources: BusinessResourceDeltaV2,
}

impl BuyOrderAcceptedV2 {
    pub(crate) fn into_output_effects(
        self,
        condition_id: ConditionId,
        token_id: TokenId,
        platform_effective_at_utc_ms: i64,
    ) -> Result<
        (
            AcceptedSubmissionItemV2,
            Vec<LifecycleFeedbackEffectV2>,
            BusinessResourceDeltaV2,
        ),
        MatchingStateErrorV1,
    > {
        let fills = self
            .initial_trade_finality
            .iter()
            .map(|effect| {
                Ok(SubmissionFillV2 {
                    trade_id: effect.trade_id.clone(),
                    source_price_pusd_per_share: exact_decimal_v2(
                        effect.source_price_pusd_per_share,
                    ),
                    base_quantity_shares: exact_decimal_from_micro_units_v2(effect.base_units)?,
                    quote_amount_pusd: exact_decimal_from_micro_units_v2(effect.quote_units)?,
                })
            })
            .collect::<Result<Vec<_>, MatchingStateErrorV1>>()?;
        let account_transition_sequences = self
            .account_deltas
            .iter()
            .map(|delta| delta.account_transition_seq)
            .collect::<Vec<_>>();
        let mut account_deltas = account_deltas_by_sequence_v2(self.account_deltas);
        let accepted_account_delta =
            take_account_delta_by_reason_v2(&mut account_deltas, |reason| {
                matches!(
                    reason,
                    PlatformAccountDeltaReasonV2::OrderAcceptedEncumbranceApplied {
                        platform_order_id
                    } if platform_order_id == &self.platform_order_id
                )
            })
            .expect("V2 已接受订单必须拥有唯一账户接受变化");
        let accepted_status = match self.platform_order_status {
            SubmissionPlatformOrderStatusV2::VenueDustTerminal => {
                SubmissionPlatformOrderStatusV2::Live
            }
            status => status,
        };
        let mut lifecycle = Vec::with_capacity(
            1 + self.initial_trade_finality.len() + usize::from(self.terminal_reason.is_some()),
        );
        lifecycle.push(LifecycleFeedbackEffectV2::Order(OrderLifecycleEffectV2 {
            platform_event_at_utc_ms: platform_effective_at_utc_ms,
            platform_order_id: self.platform_order_id.clone(),
            condition_id: condition_id.clone(),
            token_id: token_id.clone(),
            event: OrderLifecycleEventV2::OrderAccepted {
                status: submission_status_v2(accepted_status),
                account_delta: accepted_account_delta,
            },
        }));

        if let SubmissionMeasurementV2::QuoteBudget {
            released_or_unspent_quote_units,
            ..
        } = self.measurement
            && let Some(account_delta) =
                take_account_delta_by_reason_v2(&mut account_deltas, |reason| {
                    matches!(
                        reason,
                        PlatformAccountDeltaReasonV2::FakUnspentEncumbranceReleased {
                            platform_order_id
                        } if platform_order_id == &self.platform_order_id
                    )
                })
        {
            lifecycle.push(LifecycleFeedbackEffectV2::Order(OrderLifecycleEffectV2 {
                platform_event_at_utc_ms: platform_effective_at_utc_ms,
                platform_order_id: self.platform_order_id.clone(),
                condition_id: condition_id.clone(),
                token_id: token_id.clone(),
                event: OrderLifecycleEventV2::FakUnspentReleased {
                    released_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                        released_or_unspent_quote_units,
                    )?,
                    account_delta,
                },
            }));
        }

        for trade in self.initial_trade_finality {
            let account_delta =
                take_account_delta_v2(&mut account_deltas, trade.account_transition_seq);
            lifecycle.push(trade.into_lifecycle_feedback(account_delta)?);
        }

        let measurement = match self.measurement {
            SubmissionMeasurementV2::Base {
                accepted_base_units,
                matched_base_units,
                open_base_units,
                venue_dust_closed_base_units,
                order_type_terminated_base_units,
                accepted_quote_units,
                matched_quote_units,
                open_encumbered_quote_units,
                venue_dust_released_quote_units,
                released_price_improvement_quote_units,
                immediate_taker_fee_encumbered_units,
            } => {
                if venue_dust_closed_base_units > 0 {
                    let account_delta =
                        take_account_delta_by_reason_v2(&mut account_deltas, |reason| {
                            matches!(
                                reason,
                                PlatformAccountDeltaReasonV2::VenueDustEncumbranceReleased {
                                    platform_order_id
                                } if platform_order_id == &self.platform_order_id
                            )
                        });
                    lifecycle.push(LifecycleFeedbackEffectV2::Order(OrderLifecycleEffectV2 {
                        platform_event_at_utc_ms: platform_effective_at_utc_ms,
                        platform_order_id: self.platform_order_id.clone(),
                        condition_id: condition_id.clone(),
                        token_id: token_id.clone(),
                        event: OrderLifecycleEventV2::VenueDustTerminal {
                            closed_base_quantity_shares: exact_decimal_from_micro_units_v2(
                                venue_dust_closed_base_units,
                            )?,
                            released_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                                venue_dust_released_quote_units,
                            )?,
                            account_delta,
                        },
                    }));
                }
                ContractSubmissionMeasurementV2::Base {
                    accepted_base_quantity_shares: exact_decimal_from_micro_units_v2(
                        accepted_base_units,
                    )?,
                    matched_base_quantity_shares: exact_decimal_from_micro_units_v2(
                        matched_base_units,
                    )?,
                    open_base_quantity_shares: exact_decimal_from_micro_units_v2(open_base_units)?,
                    venue_dust_closed_base_quantity_shares: exact_decimal_from_micro_units_v2(
                        venue_dust_closed_base_units,
                    )?,
                    order_type_terminated_base_quantity_shares: exact_decimal_from_micro_units_v2(
                        order_type_terminated_base_units,
                    )?,
                    accepted_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                        accepted_quote_units,
                    )?,
                    matched_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                        matched_quote_units,
                    )?,
                    open_encumbered_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                        open_encumbered_quote_units,
                    )?,
                    venue_dust_released_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                        venue_dust_released_quote_units,
                    )?,
                    released_price_improvement_quote_amount_pusd:
                        exact_decimal_from_micro_units_v2(released_price_improvement_quote_units)?,
                    immediate_taker_fee_encumbered_pusd: exact_decimal_from_micro_units_v2(
                        immediate_taker_fee_encumbered_units,
                    )?,
                }
            }
            SubmissionMeasurementV2::QuoteBudget {
                accepted_quote_budget_units,
                matched_quote_units,
                released_or_unspent_quote_units,
                actual_matched_base_units,
                sdk_taker_amount_audit_base_units,
                immediate_taker_fee_encumbered_units,
            } => ContractSubmissionMeasurementV2::QuoteBudget {
                accepted_quote_budget_pusd: exact_decimal_from_micro_units_v2(
                    accepted_quote_budget_units,
                )?,
                used_quote_amount_pusd: exact_decimal_from_micro_units_v2(matched_quote_units)?,
                released_or_unspent_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                    released_or_unspent_quote_units,
                )?,
                actual_matched_base_quantity_shares: exact_decimal_from_micro_units_v2(
                    actual_matched_base_units,
                )?,
                sdk_taker_amount_audit_base_quantity_shares: exact_decimal_from_micro_units_v2(
                    sdk_taker_amount_audit_base_units,
                )?,
                immediate_taker_fee_encumbered_pusd: exact_decimal_from_micro_units_v2(
                    immediate_taker_fee_encumbered_units,
                )?,
            },
        };
        debug_assert!(account_deltas.is_empty());
        let response = AcceptedSubmissionItemV2 {
            platform_order_id: self.platform_order_id,
            platform_order_status: submission_status_v2(self.platform_order_status),
            fills,
            terminal_reason: submission_terminal_reason_v2(self.terminal_reason),
            measurement,
            account_transition_sequences,
        };
        Ok((response, lifecycle, self.resources))
    }
}

#[cfg(test)]
impl BuyOrderAcceptedV2 {
    pub(crate) fn platform_status_for_tests(&self) -> &'static str {
        match self.platform_order_status {
            SubmissionPlatformOrderStatusV2::Live => "LIVE",
            SubmissionPlatformOrderStatusV2::Matched => "MATCHED",
            SubmissionPlatformOrderStatusV2::VenueDustTerminal => "VENUE_DUST_TERMINAL",
        }
    }

    pub(crate) fn terminal_reason_for_tests(&self) -> Option<SimOrderTerminalReasonV1> {
        self.terminal_reason
    }

    pub(crate) fn initial_fill_facts_for_tests(&self) -> Vec<(&str, Decimal, u128, u128)> {
        self.initial_trade_finality
            .iter()
            .map(|effect| {
                (
                    effect.trade_id.as_str(),
                    effect.source_price_pusd_per_share,
                    effect.base_units,
                    effect.quote_units,
                )
            })
            .collect()
    }

    pub(crate) fn base_measurement_for_tests(
        &self,
    ) -> Option<(
        u128,
        u128,
        u128,
        u128,
        u128,
        u128,
        u128,
        u128,
        u128,
        u128,
        u128,
    )> {
        let SubmissionMeasurementV2::Base {
            accepted_base_units,
            matched_base_units,
            open_base_units,
            venue_dust_closed_base_units,
            order_type_terminated_base_units,
            accepted_quote_units,
            matched_quote_units,
            open_encumbered_quote_units,
            venue_dust_released_quote_units,
            released_price_improvement_quote_units,
            immediate_taker_fee_encumbered_units,
        } = self.measurement
        else {
            return None;
        };
        Some((
            accepted_base_units,
            matched_base_units,
            open_base_units,
            venue_dust_closed_base_units,
            order_type_terminated_base_units,
            accepted_quote_units,
            matched_quote_units,
            open_encumbered_quote_units,
            venue_dust_released_quote_units,
            released_price_improvement_quote_units,
            immediate_taker_fee_encumbered_units,
        ))
    }

    pub(crate) fn quote_budget_measurement_for_tests(
        &self,
    ) -> Option<(u128, u128, u128, u128, u128, u128)> {
        let SubmissionMeasurementV2::QuoteBudget {
            accepted_quote_budget_units,
            matched_quote_units,
            released_or_unspent_quote_units,
            actual_matched_base_units,
            sdk_taker_amount_audit_base_units,
            immediate_taker_fee_encumbered_units,
        } = self.measurement
        else {
            return None;
        };
        Some((
            accepted_quote_budget_units,
            matched_quote_units,
            released_or_unspent_quote_units,
            actual_matched_base_units,
            sdk_taker_amount_audit_base_units,
            immediate_taker_fee_encumbered_units,
        ))
    }

    pub(crate) fn initial_finality_count_for_tests(&self) -> usize {
        self.initial_trade_finality.len()
    }

    pub(crate) fn account_delta_count_for_tests(&self) -> usize {
        self.account_deltas.len()
    }

    pub(crate) fn account_deltas_for_tests(&self) -> &[PlatformAccountDeltaV2] {
        &self.account_deltas
    }
}

#[allow(dead_code)]
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ExecutionMarketEventEffectsV2 {
    initial_trade_finality: Vec<TradeFinalityTransitionEffectV2>,
    account_deltas: Vec<PlatformAccountDeltaV2>,
    settlement: Vec<EconomicLifecycleEffectV2>,
    maker_fill_sequences: Vec<u64>,
    venue_dust: Vec<VenueDustEffectV2>,
    query_projection_changed: bool,
    pub(crate) resources: BusinessResourceDeltaV2,
}

impl ExecutionMarketEventEffectsV2 {
    pub(crate) fn merge(&mut self, other: Self) {
        self.initial_trade_finality
            .extend(other.initial_trade_finality);
        self.account_deltas.extend(other.account_deltas);
        self.settlement.extend(other.settlement);
        self.maker_fill_sequences.extend(other.maker_fill_sequences);
        self.venue_dust.extend(other.venue_dust);
        self.query_projection_changed |= other.query_projection_changed;
        self.resources.merge(other.resources);
    }

    pub(crate) fn query_projection_changed(&self) -> bool {
        self.query_projection_changed
    }

    pub(crate) fn into_lifecycle_feedback(
        self,
    ) -> Result<Vec<LifecycleFeedbackEffectV2>, MatchingStateErrorV1> {
        let mut account_deltas = account_deltas_by_sequence_v2(self.account_deltas);
        let mut lifecycle = Vec::with_capacity(
            self.initial_trade_finality.len() + self.venue_dust.len() + self.settlement.len(),
        );
        for trade in self.initial_trade_finality {
            let account_delta =
                take_account_delta_v2(&mut account_deltas, trade.account_transition_seq);
            lifecycle.push(trade.into_lifecycle_feedback(account_delta)?);
        }
        for dust in self.venue_dust {
            lifecycle.push(LifecycleFeedbackEffectV2::Order(OrderLifecycleEffectV2 {
                platform_event_at_utc_ms: dust.platform_effective_at_utc_ms,
                platform_order_id: dust.platform_order_id,
                condition_id: dust.condition_id,
                token_id: dust.token_id,
                event: OrderLifecycleEventV2::VenueDustTerminal {
                    closed_base_quantity_shares: exact_decimal_from_micro_units_v2(
                        dust.closed_base_units,
                    )?,
                    released_quote_amount_pusd: exact_decimal_from_micro_units_v2(
                        dust.released_quote_units,
                    )?,
                    account_delta: take_account_delta_v2(
                        &mut account_deltas,
                        dust.account_transition_seq,
                    ),
                },
            }));
        }
        for settlement in self.settlement {
            lifecycle.push(economic_lifecycle_effect_v2(
                settlement,
                &mut account_deltas,
            )?);
        }
        debug_assert!(account_deltas.is_empty());
        Ok(lifecycle)
    }
}

#[cfg(test)]
impl ExecutionMarketEventEffectsV2 {
    pub(crate) fn initial_finality_count_for_tests(&self) -> usize {
        self.initial_trade_finality.len()
    }

    pub(crate) fn maker_fill_sequences_for_tests(&self) -> &[u64] {
        &self.maker_fill_sequences
    }

    pub(crate) fn venue_dust_order_id_for_tests(&self) -> Option<&str> {
        self.venue_dust
            .first()
            .map(|effect| effect.platform_order_id.as_str())
    }

    pub(crate) fn account_delta_count_for_tests(&self) -> usize {
        self.account_deltas.len()
    }

    pub(crate) fn settlement_for_tests(&self) -> &[EconomicLifecycleEffectV2] {
        &self.settlement
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BuyOrderDispositionV2 {
    Accepted(BuyOrderAcceptedV2),
    RejectedNoEffect(BuyOrderRejectedReasonV2),
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ExecutionStateV2 {
    queues: ExecutionQueueStateV2,
    account_overlay: AccountBookOverlayStoreV1,
    account: PlatformAccountStateV2,
    orders: BTreeMap<String, SimOrderRecordV2>,
    fills: BTreeMap<u64, SimFillRecordV2>,
    gtd_deadlines: BTreeSet<(i64, String)>,
    scheduled_finality: BTreeSet<ScheduledFinalityTransitionKeyV2>,
    taker_rebate: TakerRebateStateV2,
    maker_rebate: MakerRebateStateV2,
    market_resolutions: BTreeMap<String, MarketResolutionObligationV2>,
    next_order_sequence: u64,
    next_fill_sequence: u64,
}

#[allow(dead_code)]
impl ExecutionStateV2 {
    pub(crate) fn new(
        directory: &SessionMarketDirectoryV1,
        account: PlatformAccountStateV2,
    ) -> Self {
        Self {
            queues: ExecutionQueueStateV2::new(directory),
            account_overlay: AccountBookOverlayStoreV1::new(directory),
            account,
            orders: BTreeMap::new(),
            fills: BTreeMap::new(),
            gtd_deadlines: BTreeSet::new(),
            scheduled_finality: BTreeSet::new(),
            taker_rebate: TakerRebateStateV2::default(),
            maker_rebate: MakerRebateStateV2::new(directory),
            market_resolutions: BTreeMap::new(),
            next_order_sequence: 1,
            next_fill_sequence: 1,
        }
    }

    pub(crate) fn commit_admitted_market(&mut self, market: &PlatformMarketV1) {
        self.queues.commit_admitted_market(market);
        self.account_overlay.commit_admitted_market(market);
        self.maker_rebate.commit_admitted_market(market);
    }

    pub(crate) fn account_instance_key(&self) -> &AccountInstanceKeyV1 {
        self.account.account_instance_key()
    }

    pub(crate) fn next_account_transition_seq(&self) -> u64 {
        self.account.next_account_transition_seq()
    }

    pub(crate) fn redeemed_market_resolution(
        &self,
        condition_id: &str,
    ) -> Option<&MarketResolutionTerminalV3> {
        self.market_resolutions
            .get(condition_id)
            .filter(|obligation| obligation.redeemed_at_utc_ms.is_some())
            .map(|obligation| &obligation.terminal)
    }

    #[cfg(test)]
    pub(crate) fn market_shape_for_tests(&self) -> (usize, usize, usize, usize) {
        let (queue_markets, queue_assets) = self.queues.market_and_asset_count_for_tests();
        let (overlay_markets, overlay_assets) =
            self.account_overlay.market_and_asset_count_for_tests();
        (queue_markets, queue_assets, overlay_markets, overlay_assets)
    }

    #[cfg(test)]
    pub(crate) fn external_queue_base_units_for_tests(
        &self,
        condition_id: &str,
        token_id: &str,
        side: BookSide,
        price: Decimal,
    ) -> u128 {
        self.queues
            .asset(condition_id, token_id)
            .expect("测试目标必须属于执行目录")
            .external_base_units_for_tests(side, price)
    }

    #[cfg(test)]
    pub(crate) fn base_order_quantities_for_tests(
        &self,
        platform_order_id: &str,
    ) -> Option<(u128, u128, u128, u128, u128)> {
        let order = self.orders.get(platform_order_id)?;
        let OrderQuantityStateV2::Base {
            matched_base_units,
            pending_maker_depletion_base_units,
            open_base_units,
            matched_quote_units,
            open_encumbered_quote_units,
            ..
        } = &order.quantities
        else {
            return None;
        };
        Some((
            *matched_base_units,
            *pending_maker_depletion_base_units,
            *open_base_units,
            *matched_quote_units,
            *open_encumbered_quote_units,
        ))
    }

    #[cfg(test)]
    pub(crate) fn account_overlay_quantity_for_tests(
        &self,
        condition_id: &str,
        token_id: &str,
        side: BookSide,
        price: Decimal,
    ) -> Decimal {
        self.account_overlay
            .quantity(condition_id, token_id, side, price)
    }

    #[cfg(test)]
    pub(crate) fn account_balances_for_tests(&self) -> (Decimal, Decimal) {
        (
            self.account.available_pusd(),
            self.account.principal_encumbrance_pusd(),
        )
    }

    #[cfg(test)]
    pub(crate) fn immediate_taker_fee_encumbrance_for_tests(&self) -> Decimal {
        self.account.immediate_taker_fee_encumbrance_pusd()
    }

    #[cfg(test)]
    pub(crate) fn base_order_dust_for_tests(
        &self,
        platform_order_id: &str,
    ) -> Option<(u128, u128, bool)> {
        let order = self.orders.get(platform_order_id)?;
        let OrderQuantityStateV2::Base {
            venue_dust_closed_base_units,
            venue_dust_released_quote_units,
            ..
        } = &order.quantities
        else {
            return None;
        };
        Some((
            *venue_dust_closed_base_units,
            *venue_dust_released_quote_units,
            order.terminal_reason == Some(SimOrderTerminalReasonV1::VenueDustTerminal),
        ))
    }

    #[cfg(test)]
    pub(crate) fn has_gtd_deadline_for_tests(
        &self,
        expires_at_utc_ms: i64,
        platform_order_id: &str,
    ) -> bool {
        self.gtd_deadlines
            .contains(&(expires_at_utc_ms, platform_order_id.to_owned()))
    }

    #[cfg(test)]
    pub(crate) fn fill_shape_for_tests(&self) -> (usize, u64) {
        (self.fills.len(), self.next_fill_sequence)
    }

    #[cfg(test)]
    pub(crate) fn maker_rebate_has_pending_fill_for_tests(&self, fill_sequence: u64) -> bool {
        self.maker_rebate
            .pending_fill_sequences
            .contains(&fill_sequence)
    }

    fn apply_initial_levels(
        &mut self,
        condition_id: &str,
        token_id: &str,
        bids: &[(Decimal, u128)],
        asks: &[(Decimal, u128)],
    ) -> Result<(), MatchingStateErrorV1> {
        self.queues
            .apply_initial_levels(condition_id, token_id, bids, asks)
            .map(|_| ())
    }

    fn insert_fill_with_initial_finality(
        &mut self,
        order: &mut SimOrderRecordV2,
        fill_sequence: u64,
        matched_at_utc_ms: i64,
        liquidity_role: SimLiquidityRoleV1,
        price_pusd_per_share: Decimal,
        base_units: u128,
        quote_units: u128,
        immediate_taker_fee_units: u128,
        finality_policy: &TradeFinalityPolicyV2,
    ) -> Result<
        (
            TradeFinalityTransitionEffectV2,
            Option<PlatformAccountDeltaV2>,
        ),
        MatchingStateErrorV1,
    > {
        let trade_id = format!("sim-{fill_sequence}");
        let selected_finality_path = finality_policy.select_path_for_fill_once(
            self.account.account_instance_key(),
            &order.global_request_id,
            order.batch_item_ordinal,
            fill_sequence,
        );
        let path = finality_policy.path_for_selection(selected_finality_path);
        matched_at_utc_ms
            .checked_add(
                path.steps()
                    .last()
                    .expect("已密封最终性路径至少包含首态")
                    .offset_ms(),
            )
            .ok_or(MatchingStateErrorV1::LogicalTimeRegression)?;
        let account_delta = if path.economic_application_ordinal() == Some(0) {
            let delta = self.account.apply_fill_finality_economic(
                &trade_id,
                0,
                &order.condition_id,
                &order.token_id,
                matched_at_utc_ms,
                FillEconomicAmountsV1 {
                    base_units,
                    quote_units,
                    immediate_taker_fee_units,
                },
            )?;
            order.immediate_taker_fee_encumbered_units = order
                .immediate_taker_fee_encumbered_units
                .checked_sub(immediate_taker_fee_units)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            Some(delta)
        } else {
            None
        };
        let economic_application_account_transition_seq = account_delta
            .as_ref()
            .map(|delta| delta.account_transition_seq);
        let initial_finality_state = path.steps()[0].state();
        let effect = TradeFinalityTransitionEffectV2 {
            fill_sequence,
            trade_id: trade_id.clone(),
            platform_order_id: order.platform_order_id.clone(),
            condition_id: order.condition_id.clone(),
            token_id: order.token_id.clone(),
            matched_at_utc_ms,
            platform_event_at_utc_ms: matched_at_utc_ms,
            path_ordinal: 0,
            finality_state: initial_finality_state,
            economic_disposition: if economic_application_account_transition_seq.is_some() {
                TradeEconomicDispositionStateV1::PendingApplied
            } else {
                TradeEconomicDispositionStateV1::PendingUnapplied
            },
            liquidity_role,
            source_price_pusd_per_share: price_pusd_per_share,
            base_units,
            quote_units,
            immediate_taker_fee_units,
            economic_application_account_transition_seq,
            account_transition_seq: economic_application_account_transition_seq,
        };
        if let Some(next_step) = path.steps().get(1) {
            self.scheduled_finality
                .insert(ScheduledFinalityTransitionKeyV2 {
                    platform_event_at_utc_ms: matched_at_utc_ms
                        .checked_add(next_step.offset_ms())
                        .ok_or(MatchingStateErrorV1::LogicalTimeRegression)?,
                    fill_sequence,
                    path_ordinal: 1,
                });
        }
        self.fills.insert(
            fill_sequence,
            SimFillRecordV2 {
                trade_id,
                platform_order_id: order.platform_order_id.clone(),
                matched_at_utc_ms,
                liquidity_role,
                price_pusd_per_share,
                base_units,
                quote_units,
                immediate_taker_fee_units,
                selected_finality_path,
                finality_path_ordinal: 0,
                economic_application_account_transition_seq,
            },
        );
        Ok((effect, account_delta))
    }

    pub(crate) fn apply_market_event(
        &mut self,
        condition_id: &str,
        normalized: &NormalizedMarketEventV2<'_>,
        platform_effective_at_utc_ms: i64,
        end_at_utc_ms: i64,
        rules: &PlatformRuleSnapshotV3,
        finality_policy: &TradeFinalityPolicyV2,
    ) -> Result<ExecutionMarketEventEffectsV2, MatchingStateErrorV1> {
        let mut effects = ExecutionMarketEventEffectsV2::default();
        if platform_effective_at_utc_ms >= end_at_utc_ms {
            effects
                .resources
                .merge(self.queues.end_matching(condition_id)?);
        }
        if self.queues.matching_closed(condition_id)? {
            return Ok(effects);
        }

        match normalized {
            NormalizedMarketEventV2::InitialL2Snapshot {
                snapshot,
                bids,
                asks,
                ..
            } => {
                let bids = bids
                    .iter()
                    .map(|level| {
                        Ok((
                            level.price_pusd_per_share,
                            micro_units_v1(level.quantity_shares)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, MatchingStateErrorV1>>()?;
                let asks = asks
                    .iter()
                    .map(|level| {
                        Ok((
                            level.price_pusd_per_share,
                            micro_units_v1(level.quantity_shares)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, MatchingStateErrorV1>>()?;
                effects.resources.merge(self.queues.apply_initial_levels(
                    condition_id,
                    &snapshot.asset.token_id.0,
                    &bids,
                    &asks,
                )?);
            }
            NormalizedMarketEventV2::LevelExecutionTransition {
                transition,
                price_pusd_per_share,
                quantity_before_shares,
                quantity_after_shares,
            } => {
                let queue_effects = self
                    .queues
                    .asset_mut(condition_id, &transition.asset.token_id.0)?
                    .apply_level_transition(
                        transition.side,
                        *price_pusd_per_share,
                        micro_units_v1(*quantity_before_shares)?,
                        micro_units_v1(*quantity_after_shares)?,
                    )?;
                self.apply_maker_depletions(
                    condition_id,
                    &transition.asset.token_id.0,
                    platform_effective_at_utc_ms,
                    queue_effects,
                    rules,
                    finality_policy,
                    &mut effects,
                )?;
            }
            NormalizedMarketEventV2::QualityBoundary { boundary, resync }
                if matches!(&boundary.scope, QualityScopeV1::Asset(_))
                    && *resync == contracts::ResyncQuality::Required =>
            {
                let QualityScopeV1::Asset(asset) = &boundary.scope else {
                    unreachable!("刚匹配的 V2 质量边界必须是资产范围")
                };
                effects.resources.merge(
                    self.queues
                        .asset_mut(condition_id, &asset.token_id.0)?
                        .clear_external_segments(),
                );
            }
            NormalizedMarketEventV2::MarketLifecycleTransition(_)
            | NormalizedMarketEventV2::MarketTradingState(_)
            | NormalizedMarketEventV2::BestBidAsk { .. }
            | NormalizedMarketEventV2::TickSizeChange { .. }
            | NormalizedMarketEventV2::PublicTrade(_)
            | NormalizedMarketEventV2::QualityBoundary { .. }
            | NormalizedMarketEventV2::MarketResolutionTerminal { .. } => {}
        }
        Ok(effects)
    }

    pub(crate) fn apply_qualified_resolution(
        &mut self,
        condition_id: &str,
        resolution: &QualifiedMarketResolutionV2,
        platform_effective_at_utc_ms: i64,
        finality_policy: &TradeFinalityPolicyV2,
    ) -> Result<ExecutionMarketEventEffectsV2, MatchingStateErrorV1> {
        if let Some(existing) = self.market_resolutions.get(condition_id) {
            if existing.terminal == resolution.terminal {
                return Ok(ExecutionMarketEventEffectsV2::default());
            }
            return Err(MatchingStateErrorV1::ResolutionConflict(
                condition_id.to_owned(),
            ));
        }
        let condition = ConditionId(condition_id.to_owned());
        let pending_fill_sequences = self
            .fills
            .iter()
            .filter_map(|(fill_sequence, fill)| {
                let order = self
                    .orders
                    .get(&fill.platform_order_id)
                    .expect("V2 成交只引用当前订单");
                if order.condition_id != condition {
                    return None;
                }
                let path = finality_policy.path_for_selection(fill.selected_finality_path);
                let state = path
                    .steps()
                    .get(
                        usize::try_from(fill.finality_path_ordinal)
                            .expect("V2 最终性 ordinal 可表示为 usize"),
                    )
                    .expect("V2 成交只引用已密封路径步骤")
                    .state();
                (!matches!(
                    state,
                    TradeFinalityStateV1::Confirmed | TradeFinalityStateV1::Failed
                ))
                .then_some(*fill_sequence)
            })
            .collect::<BTreeSet<_>>();
        let active_order_ids = self
            .orders
            .iter()
            .filter_map(|(platform_order_id, order)| {
                let OrderQuantityStateV2::Base {
                    pending_maker_depletion_base_units,
                    open_base_units,
                    ..
                } = &order.quantities
                else {
                    return None;
                };
                (order.condition_id == condition
                    && (*pending_maker_depletion_base_units > 0 || *open_base_units > 0))
                    .then(|| platform_order_id.clone())
            })
            .collect::<Vec<_>>();
        let mut effects = ExecutionMarketEventEffectsV2::default();
        for platform_order_id in active_order_ids {
            let effect = self.apply_market_resolution_order(
                &platform_order_id,
                &resolution.resolution_event_id,
                platform_effective_at_utc_ms,
                &mut effects.account_deltas,
                &mut effects.resources,
            )?;
            effects
                .settlement
                .push(EconomicLifecycleEffectV2::MarketResolutionOrder(effect));
            effects.query_projection_changed = true;
        }
        let obligation = MarketResolutionObligationV2 {
            resolution_event_id: resolution.resolution_event_id.clone(),
            resolution_source_event_at_utc_ms: resolution.resolution_source_event_at_utc_ms,
            terminal: resolution.terminal.clone(),
            parsed_payouts: resolution.parsed_payouts,
            pending_fill_sequences,
            redeemed_at_utc_ms: None,
            invalidated_by_quality_reason: None,
        };
        effects
            .resources
            .add_record(market_resolution_obligation_owned_bytes_v2(
                condition_id,
                &obligation,
            ));
        effects
            .resources
            .add_records(obligation.pending_fill_sequences.len() as u64);
        self.market_resolutions
            .insert(condition_id.to_owned(), obligation);
        if let Some(auto_redeem) = self.try_auto_redeem(
            &condition,
            platform_effective_at_utc_ms,
            &mut effects.account_deltas,
        )? {
            effects
                .settlement
                .push(EconomicLifecycleEffectV2::AutoRedeem(auto_redeem));
        }
        self.settle_ready_days(
            platform_effective_at_utc_ms,
            &mut effects.settlement,
            &mut effects.account_deltas,
        )?;
        Ok(effects)
    }

    pub(crate) fn invalidate_qualified_resolution(
        &mut self,
        condition_id: &str,
        reason_code: String,
    ) -> BusinessResourceDeltaV2 {
        let mut resources = BusinessResourceDeltaV2::default();
        if let Some(obligation) = self.market_resolutions.get_mut(condition_id)
            && obligation.redeemed_at_utc_ms.is_none()
            && obligation.invalidated_by_quality_reason.is_none()
        {
            resources.replace_bytes(0, reason_code.len() as u64);
            obligation.invalidated_by_quality_reason = Some(reason_code);
        }
        resources
    }

    fn apply_market_resolution_order(
        &mut self,
        platform_order_id: &str,
        resolution_event_id: &str,
        platform_effective_at_utc_ms: i64,
        account_deltas: &mut Vec<PlatformAccountDeltaV2>,
        resources: &mut BusinessResourceDeltaV2,
    ) -> Result<MarketResolutionOrderEffectV2, MatchingStateErrorV1> {
        let mut order = self.orders.remove(platform_order_id).ok_or_else(|| {
            MatchingStateErrorV1::AccountOrderConflict(platform_order_id.to_owned())
        })?;
        let (pending_base_units_before, open_base_units_before, released_open_quote_units) =
            match &order.quantities {
                OrderQuantityStateV2::Base {
                    pending_maker_depletion_base_units,
                    open_base_units,
                    open_encumbered_quote_units,
                    ..
                } => (
                    *pending_maker_depletion_base_units,
                    *open_base_units,
                    *open_encumbered_quote_units,
                ),
                OrderQuantityStateV2::QuoteBudget { .. } => {
                    return Err(MatchingStateErrorV1::AccountOrderConflict(
                        platform_order_id.to_owned(),
                    ));
                }
            };
        if open_base_units_before > 0 {
            if !self.queues.matching_closed(&order.condition_id.0)?
                && let Some((_, queue_resources)) = self
                    .queues
                    .asset_mut(&order.condition_id.0, &order.token_id.0)?
                    .remove_account_order_at_price(
                        order.source_price_pusd_per_share,
                        platform_order_id,
                    )
            {
                resources.merge(queue_resources);
            }
            let overlay_before = self.account_overlay.quantity(
                &order.condition_id.0,
                &order.token_id.0,
                BookSide::Bid,
                order.source_price_pusd_per_share,
            );
            self.account_overlay
                .subtract_quantity(
                    &order.condition_id.0,
                    &order.token_id.0,
                    BookSide::Bid,
                    order.source_price_pusd_per_share,
                    decimal_from_micro_units_v1(open_base_units_before)?,
                )
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            if !overlay_before.is_zero()
                && self
                    .account_overlay
                    .quantity(
                        &order.condition_id.0,
                        &order.token_id.0,
                        BookSide::Bid,
                        order.source_price_pusd_per_share,
                    )
                    .is_zero()
            {
                resources.remove_record(0);
            }
        }
        if let SimOrderKindStateV1::Gtd { expires_at_utc_ms } = order.kind
            && self
                .gtd_deadlines
                .remove(&(expires_at_utc_ms, platform_order_id.to_owned()))
        {
            resources.remove_record(platform_order_id.len() as u64);
        }
        let released_open_quote_pusd = decimal_from_micro_units_v1(released_open_quote_units)?;
        let account_delta = if released_open_quote_units > 0 {
            Some(self.account.apply_market_resolution_encumbrance_release(
                resolution_event_id,
                platform_order_id,
                &order.condition_id,
                platform_effective_at_utc_ms,
                released_open_quote_pusd,
            )?)
        } else {
            None
        };
        let account_transition_seq = account_delta
            .as_ref()
            .map(|delta| delta.account_transition_seq);
        if let Some(delta) = account_delta {
            account_deltas.push(delta);
        }
        let canceled_base_units = pending_base_units_before
            .checked_add(open_base_units_before)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let OrderQuantityStateV2::Base {
            pending_maker_depletion_base_units,
            open_base_units,
            market_resolved_canceled_base_units,
            open_encumbered_quote_units,
            market_resolved_released_quote_units,
            ..
        } = &mut order.quantities
        else {
            unreachable!("刚匹配的 V2 判定关闭订单必须使用 base 计量")
        };
        *market_resolved_canceled_base_units = market_resolved_canceled_base_units
            .checked_add(canceled_base_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        *market_resolved_released_quote_units = market_resolved_released_quote_units
            .checked_add(released_open_quote_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        *pending_maker_depletion_base_units = 0;
        *open_base_units = 0;
        *open_encumbered_quote_units = 0;
        order.last_updated_at_utc_ms = platform_effective_at_utc_ms;
        order.terminal_reason = Some(SimOrderTerminalReasonV1::MarketResolvedCanceled);
        let effect = MarketResolutionOrderEffectV2 {
            platform_order_id: order.platform_order_id.clone(),
            condition_id: order.condition_id.clone(),
            token_id: order.token_id.clone(),
            resolution_event_id: resolution_event_id.to_owned(),
            platform_effective_at_utc_ms,
            pending_base_units_before,
            open_base_units_before,
            released_open_quote_units,
            account_transition_seq,
        };
        self.orders.insert(order.platform_order_id.clone(), order);
        Ok(effect)
    }

    fn apply_maker_depletions(
        &mut self,
        condition_id: &str,
        token_id: &str,
        platform_effective_at_utc_ms: i64,
        queue_effects: AssetQueueEffectsV2,
        rules: &PlatformRuleSnapshotV3,
        finality_policy: &TradeFinalityPolicyV2,
        effects: &mut ExecutionMarketEventEffectsV2,
    ) -> Result<(), MatchingStateErrorV1> {
        let AssetQueueEffectsV2 {
            maker_depletions,
            venue_dust_candidate,
            resources,
        } = queue_effects;
        effects.resources.merge(resources);
        for depletion in maker_depletions {
            let mut order = self
                .orders
                .remove(&depletion.platform_order_id)
                .ok_or_else(|| {
                    MatchingStateErrorV1::AccountOrderConflict(depletion.platform_order_id.clone())
                })?;
            let OrderQuantityStateV2::Base {
                accepted_base_units,
                matched_base_units,
                pending_maker_depletion_base_units,
                open_base_units,
                accepted_quote_units,
                matched_quote_units,
                open_encumbered_quote_units,
                ..
            } = &mut order.quantities
            else {
                return Err(MatchingStateErrorV1::AccountOrderConflict(
                    depletion.platform_order_id,
                ));
            };
            *open_base_units = open_base_units
                .checked_sub(depletion.base_units)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            let overlay_quantity_before = self.account_overlay.quantity(
                condition_id,
                token_id,
                BookSide::Bid,
                depletion.price_pusd_per_share,
            );
            self.account_overlay
                .subtract_quantity(
                    condition_id,
                    token_id,
                    BookSide::Bid,
                    depletion.price_pusd_per_share,
                    decimal_from_micro_units_v1(depletion.base_units)?,
                )
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            if !overlay_quantity_before.is_zero()
                && self
                    .account_overlay
                    .quantity(
                        condition_id,
                        token_id,
                        BookSide::Bid,
                        depletion.price_pusd_per_share,
                    )
                    .is_zero()
            {
                effects.resources.remove_record(0);
            }
            let fill_base_units = pending_maker_depletion_base_units
                .checked_add(depletion.base_units)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            let allocated_before = cumulative_segment_quote_units_v1(
                *accepted_base_units,
                *accepted_quote_units,
                *matched_base_units,
            )?;
            let matched_after = matched_base_units
                .checked_add(fill_base_units)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            let allocated_after = cumulative_segment_quote_units_v1(
                *accepted_base_units,
                *accepted_quote_units,
                matched_after,
            )?;
            let fill_quote_units = allocated_after
                .checked_sub(allocated_before)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            order.last_updated_at_utc_ms = platform_effective_at_utc_ms;
            if fill_quote_units == 0 {
                *pending_maker_depletion_base_units = fill_base_units;
                self.orders.insert(order.platform_order_id.clone(), order);
                continue;
            }

            *matched_base_units = matched_after;
            *pending_maker_depletion_base_units = 0;
            *matched_quote_units = matched_quote_units
                .checked_add(fill_quote_units)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            *open_encumbered_quote_units = open_encumbered_quote_units
                .checked_sub(fill_quote_units)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            let fill_sequence = self.next_fill_sequence;
            let next_fill_sequence = fill_sequence
                .checked_add(1)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            let scheduled_before = self.scheduled_finality.len();
            let outcome_balances_before = self.account.outcome_balance_record_count();
            let (initial, account_delta) = self.insert_fill_with_initial_finality(
                &mut order,
                fill_sequence,
                platform_effective_at_utc_ms,
                SimLiquidityRoleV1::MakerInferred,
                depletion.price_pusd_per_share,
                fill_base_units,
                fill_quote_units,
                0,
                finality_policy,
            )?;
            let fill = self
                .fills
                .get(&fill_sequence)
                .expect("刚插入的 V2 maker 成交必须存在");
            effects
                .resources
                .add_record((fill.trade_id.len() + fill.platform_order_id.len()) as u64);
            effects.resources.add_records(
                self.scheduled_finality
                    .len()
                    .checked_sub(scheduled_before)
                    .expect("V2 maker 首态不能删除后续最终性任务") as u64,
            );
            if self.account.outcome_balance_record_count() > outcome_balances_before {
                effects.resources.add_record(token_id.len() as u64);
            }
            effects.resources.merge(self.maker_rebate.register_fill(
                fill_sequence,
                condition_id,
                platform_effective_at_utc_ms,
            )?);
            self.next_fill_sequence = next_fill_sequence;
            effects.maker_fill_sequences.push(fill_sequence);
            effects.query_projection_changed = true;
            effects.initial_trade_finality.push(initial);
            if let Some(account_delta) = account_delta {
                effects.account_deltas.push(account_delta);
            }
            if let (
                SimOrderKindStateV1::Gtd { expires_at_utc_ms },
                OrderQuantityStateV2::Base {
                    open_base_units: 0, ..
                },
            ) = (order.kind, &order.quantities)
            {
                if self
                    .gtd_deadlines
                    .remove(&(expires_at_utc_ms, order.platform_order_id.clone()))
                {
                    effects
                        .resources
                        .remove_record(order.platform_order_id.len() as u64);
                }
            }
            self.orders.insert(order.platform_order_id.clone(), order);
        }

        if let Some(candidate) = venue_dust_candidate {
            self.apply_venue_dust_candidate(
                condition_id,
                token_id,
                platform_effective_at_utc_ms,
                candidate,
                rules,
                effects,
            )?;
        }
        Ok(())
    }

    fn apply_venue_dust_candidate(
        &mut self,
        condition_id: &str,
        token_id: &str,
        platform_effective_at_utc_ms: i64,
        candidate: AssetVenueDustCandidateV2,
        rules: &PlatformRuleSnapshotV3,
        effects: &mut ExecutionMarketEventEffectsV2,
    ) -> Result<(), MatchingStateErrorV1> {
        let mut order = self
            .orders
            .remove(&candidate.platform_order_id)
            .ok_or_else(|| {
                MatchingStateErrorV1::AccountOrderConflict(candidate.platform_order_id.clone())
            })?;
        let OrderQuantityStateV2::Base {
            matched_base_units,
            pending_maker_depletion_base_units,
            open_base_units,
            venue_dust_closed_base_units,
            open_encumbered_quote_units,
            venue_dust_released_quote_units,
            ..
        } = &mut order.quantities
        else {
            return Err(MatchingStateErrorV1::AccountOrderConflict(
                candidate.platform_order_id,
            ));
        };
        let upper_exclusive = micro_units_v1(
            rules
                .project_account_rules
                .lifecycle_account_behavior
                .value
                .venue_dust_upper_exclusive_shares
                .value(),
        )?;
        if *matched_base_units == 0
            || *pending_maker_depletion_base_units > 0
            || *open_base_units == 0
            || *open_base_units >= upper_exclusive
            || *open_base_units != candidate.remaining_base_units
        {
            self.orders.insert(order.platform_order_id.clone(), order);
            return Ok(());
        }
        effects.resources.merge(
            self.queues
                .asset_mut(condition_id, token_id)?
                .remove_account_order(&candidate.platform_order_id),
        );
        self.account_overlay
            .subtract_quantity(
                condition_id,
                token_id,
                BookSide::Bid,
                candidate.price_pusd_per_share,
                decimal_from_micro_units_v1(*open_base_units)?,
            )
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let closed_base_units = *open_base_units;
        let released_quote_units = *open_encumbered_quote_units;
        let delta = self.account.apply_venue_dust_release(
            &candidate.platform_order_id,
            &order.condition_id,
            platform_effective_at_utc_ms,
            decimal_from_micro_units_v1(released_quote_units)?,
        )?;
        *venue_dust_closed_base_units = closed_base_units;
        *open_base_units = 0;
        *venue_dust_released_quote_units = released_quote_units;
        *open_encumbered_quote_units = 0;
        order.last_updated_at_utc_ms = platform_effective_at_utc_ms;
        order.terminal_reason = Some(SimOrderTerminalReasonV1::VenueDustTerminal);
        let account_transition_seq = delta.account_transition_seq;
        effects.account_deltas.push(delta);
        effects.venue_dust.push(VenueDustEffectV2 {
            platform_order_id: candidate.platform_order_id.clone(),
            condition_id: order.condition_id.clone(),
            token_id: order.token_id.clone(),
            platform_effective_at_utc_ms,
            closed_base_units,
            released_quote_units,
            account_transition_seq: Some(account_transition_seq),
        });
        effects.query_projection_changed = true;
        effects.resources.remove_record(0);
        if let SimOrderKindStateV1::Gtd { expires_at_utc_ms } = order.kind {
            if self
                .gtd_deadlines
                .remove(&(expires_at_utc_ms, order.platform_order_id.clone()))
            {
                effects
                    .resources
                    .remove_record(order.platform_order_id.len() as u64);
            }
        }
        self.orders.insert(order.platform_order_id.clone(), order);
        Ok(())
    }

    pub(crate) fn apply_buy_order(
        mut self,
        input: BuyOrderActionV2,
        rules: &PlatformRuleSnapshotV3,
        finality_policy: &TradeFinalityPolicyV2,
    ) -> Result<(Self, BuyOrderDispositionV2), MatchingStateErrorV1> {
        let terms = input.terms;
        let target = terms.target();
        let calculated = self
            .queues
            .asset(&input.condition_id.0, &input.token_id.0)?
            .calculate_taker_buy(terms.maximum_price(), target)?;
        if terms.post_only() && !calculated.fills.is_empty() {
            return Ok((
                self,
                BuyOrderDispositionV2::RejectedNoEffect(
                    BuyOrderRejectedReasonV2::PostOnlyWouldTake,
                ),
            ));
        }
        if terms.is_fok()
            && !target.complete(calculated.matched_base_units, calculated.used_quote_units)
        {
            return Ok((
                self,
                BuyOrderDispositionV2::RejectedNoEffect(
                    BuyOrderRejectedReasonV2::FokNotFullyFilled,
                ),
            ));
        }
        if terms.is_fak() && calculated.fills.is_empty() {
            return Ok((
                self,
                BuyOrderDispositionV2::RejectedNoEffect(BuyOrderRejectedReasonV2::FakNoMatch),
            ));
        }
        let marketable_limit_minimum = match terms {
            BuyOrderTermsV2::Gtc { .. } => Some(
                rules
                    .documented_platform_rules
                    .immediate_gtc_buy_minimum
                    .value
                    .minimum_principal_pusd
                    .value(),
            ),
            BuyOrderTermsV2::Gtd { .. } => Some(
                rules
                    .documented_platform_rules
                    .immediate_gtd_buy_minimum
                    .value
                    .minimum_principal_pusd
                    .value(),
            ),
            BuyOrderTermsV2::Fok { .. } | BuyOrderTermsV2::Fak { .. } => None,
        };
        if !calculated.fills.is_empty()
            && let Some(minimum) = marketable_limit_minimum
            && decimal_from_micro_units_v1(terms.accepted_quote_units())? < minimum
        {
            return Ok((
                self,
                BuyOrderDispositionV2::RejectedNoEffect(
                    BuyOrderRejectedReasonV2::BelowMarketableLimitBuyMinimumPrincipal,
                ),
            ));
        }
        if matches!(
            terms,
            BuyOrderTermsV2::Gtd {
                expires_at_utc_ms: None,
                ..
            }
        ) {
            return Ok((
                self,
                BuyOrderDispositionV2::RejectedNoEffect(
                    BuyOrderRejectedReasonV2::GtdExpirationTooSoon,
                ),
            ));
        }

        let mut candidate_next_fill_sequence = self.next_fill_sequence;
        let mut fee_inputs = Vec::with_capacity(calculated.fills.len());
        let mut fill_sequences = Vec::with_capacity(calculated.fills.len());
        for calculated_fill in &calculated.fills {
            let fill_sequence = candidate_next_fill_sequence;
            candidate_next_fill_sequence = candidate_next_fill_sequence
                .checked_add(1)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            fill_sequences.push(fill_sequence);
            fee_inputs.push((
                fill_sequence,
                calculated_fill.fill.base_units,
                calculated_fill.fill.price_pusd_per_share,
            ));
        }
        let fee_schedule = &rules.synchronized_platform_rules.fee_schedule;
        let (total_immediate_taker_fee_units, fee_allocations) =
            allocate_taker_fee_micro_units_by_key(
                &fee_inputs,
                fee_schedule.rate.value(),
                fee_schedule.exponent,
            )?;

        let matched_base_units = calculated.matched_base_units;
        let matched_quote_units = calculated.used_quote_units;
        let accepted_quote_units = terms.accepted_quote_units();
        let (
            quantities,
            open_base_units,
            open_encumbered_quote_units,
            initial_principal_encumbrance_units,
            terminal_reason,
        ) = match terms {
            BuyOrderTermsV2::Gtc {
                accepted_base_units,
                accepted_quote_units,
                ..
            }
            | BuyOrderTermsV2::Gtd {
                accepted_base_units,
                accepted_quote_units,
                ..
            } => {
                let allocated_matched_quote_units = cumulative_segment_quote_units_v1(
                    accepted_base_units,
                    accepted_quote_units,
                    matched_base_units,
                )?;
                let open_base_units = accepted_base_units
                    .checked_sub(matched_base_units)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                let open_encumbered_quote_units = accepted_quote_units
                    .checked_sub(allocated_matched_quote_units)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                let released_price_improvement_quote_units = allocated_matched_quote_units
                    .checked_sub(matched_quote_units)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                let principal_encumbrance_units = matched_quote_units
                    .checked_add(open_encumbered_quote_units)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                (
                    OrderQuantityStateV2::Base {
                        accepted_base_units,
                        matched_base_units,
                        pending_maker_depletion_base_units: 0,
                        open_base_units,
                        venue_dust_closed_base_units: 0,
                        canceled_base_units: 0,
                        expired_base_units: 0,
                        market_resolved_canceled_base_units: 0,
                        accepted_quote_units,
                        matched_quote_units,
                        open_encumbered_quote_units,
                        released_price_improvement_quote_units,
                        venue_dust_released_quote_units: 0,
                        canceled_released_quote_units: 0,
                        expired_released_quote_units: 0,
                        market_resolved_released_quote_units: 0,
                    },
                    open_base_units,
                    open_encumbered_quote_units,
                    principal_encumbrance_units,
                    None,
                )
            }
            BuyOrderTermsV2::Fok {
                accepted_quote_budget_units,
                sdk_taker_amount_audit_base_units,
                ..
            } => (
                OrderQuantityStateV2::QuoteBudget {
                    accepted_quote_budget_units,
                    matched_quote_units,
                    released_or_unspent_quote_units: 0,
                    actual_matched_base_units: matched_base_units,
                    sdk_taker_amount_audit_base_units,
                },
                0,
                0,
                matched_quote_units,
                None,
            ),
            BuyOrderTermsV2::Fak {
                accepted_quote_budget_units,
                sdk_taker_amount_audit_base_units,
                ..
            } => {
                let released_or_unspent_quote_units = accepted_quote_budget_units
                    .checked_sub(matched_quote_units)
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                (
                    OrderQuantityStateV2::QuoteBudget {
                        accepted_quote_budget_units,
                        matched_quote_units,
                        released_or_unspent_quote_units,
                        actual_matched_base_units: matched_base_units,
                        sdk_taker_amount_audit_base_units,
                    },
                    0,
                    0,
                    accepted_quote_budget_units,
                    Some(SimOrderTerminalReasonV1::FakCompleted),
                )
            }
        };

        let order_sequence = self.next_order_sequence;
        let next_order_sequence = order_sequence
            .checked_add(1)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let platform_order_id = format!("sim-{order_sequence}");
        let mut account_deltas = Vec::new();
        let required_available_pusd_units = accepted_quote_units
            .checked_add(total_immediate_taker_fee_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let account_delta = match self.account.apply_order_accepted_with_required_available(
            &platform_order_id,
            &input.condition_id,
            input.effective_processing_at_utc_ms,
            required_available_pusd_units,
            decimal_from_micro_units_v1(initial_principal_encumbrance_units)?,
            decimal_from_micro_units_v1(total_immediate_taker_fee_units)?,
        ) {
            Ok(delta) => delta,
            Err(PlatformAccountErrorV1::BalanceUnderflow("available_pusd")) => {
                return Ok((
                    self,
                    BuyOrderDispositionV2::RejectedNoEffect(
                        BuyOrderRejectedReasonV2::InsufficientAvailablePusd,
                    ),
                ));
            }
            Err(error) => return Err(MatchingStateErrorV1::Account(error)),
        };
        account_deltas.push(account_delta);
        if terms.is_fak()
            && let Some(delta) = self.account.apply_fak_unspent_release(
                &platform_order_id,
                &input.condition_id,
                input.effective_processing_at_utc_ms,
                decimal_from_micro_units_v1(
                    accepted_quote_units
                        .checked_sub(matched_quote_units)
                        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?,
                )?,
            )?
        {
            account_deltas.push(delta);
        }

        let (plan, queue_resources) = self
            .queues
            .asset_mut(&input.condition_id.0, &input.token_id.0)?
            .apply_taker_consumption(calculated);
        let mut resources = queue_resources;
        let mut order = SimOrderRecordV2 {
            platform_order_id: platform_order_id.clone(),
            global_request_id: input.global_request_id,
            batch_item_ordinal: input.batch_item_ordinal,
            item_id: input.item_id,
            condition_id: input.condition_id,
            token_id: input.token_id,
            created_at_utc_ms: input.effective_processing_at_utc_ms,
            last_updated_at_utc_ms: input.effective_processing_at_utc_ms,
            kind: terms.order_kind(),
            source_price_pusd_per_share: terms.maximum_price(),
            quantities,
            immediate_taker_fee_encumbered_units: total_immediate_taker_fee_units,
            terminal_reason,
        };
        let mut initial_trade_finality = Vec::with_capacity(fill_sequences.len());

        if open_base_units > 0 {
            resources.merge(
                self.queues
                    .asset_mut(&order.condition_id.0, &order.token_id.0)?
                    .insert_account_buy_remainder(
                        order.source_price_pusd_per_share,
                        platform_order_id.clone(),
                        open_base_units,
                    ),
            );
            let overlay_quantity_before = self.account_overlay.quantity(
                &order.condition_id.0,
                &order.token_id.0,
                BookSide::Bid,
                order.source_price_pusd_per_share,
            );
            self.account_overlay
                .add_quantity(
                    &order.condition_id.0,
                    &order.token_id.0,
                    BookSide::Bid,
                    order.source_price_pusd_per_share,
                    decimal_from_micro_units_v1(open_base_units)?,
                )
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            if overlay_quantity_before.is_zero() {
                resources.add_record(0);
            }
        }

        for ((fill_sequence, fill), immediate_taker_fee_units) in fill_sequences
            .iter()
            .copied()
            .zip(plan.fills)
            .zip(fee_allocations)
        {
            if immediate_taker_fee_units > 0 {
                resources.merge(
                    self.taker_rebate
                        .register_positive_fee_fill(input.effective_processing_at_utc_ms)?,
                );
            }
            let scheduled_before = self.scheduled_finality.len();
            let outcome_balances_before = self.account.outcome_balance_record_count();
            let (initial, account_delta) = self.insert_fill_with_initial_finality(
                &mut order,
                fill_sequence,
                input.effective_processing_at_utc_ms,
                SimLiquidityRoleV1::Taker,
                fill.price_pusd_per_share,
                fill.base_units,
                fill.quote_units,
                immediate_taker_fee_units,
                finality_policy,
            )?;
            let fill = self
                .fills
                .get(&fill_sequence)
                .expect("刚插入的 V2 taker 成交必须存在");
            resources.add_record((fill.trade_id.len() + fill.platform_order_id.len()) as u64);
            resources.add_records(
                self.scheduled_finality
                    .len()
                    .checked_sub(scheduled_before)
                    .expect("V2 taker 首态不能删除后续最终性任务") as u64,
            );
            if self.account.outcome_balance_record_count() > outcome_balances_before {
                resources.add_record(order.token_id.0.len() as u64);
            }
            initial_trade_finality.push(initial);
            if let Some(account_delta) = account_delta {
                account_deltas.push(account_delta);
            }
        }

        if open_base_units > 0 && matched_base_units > 0 {
            let venue_dust_upper_exclusive_base_units = micro_units_v1(
                rules
                    .project_account_rules
                    .lifecycle_account_behavior
                    .value
                    .venue_dust_upper_exclusive_shares
                    .value(),
            )?;
            if open_base_units < venue_dust_upper_exclusive_base_units
                && self
                    .queues
                    .asset(&order.condition_id.0, &order.token_id.0)?
                    .venue_dust_candidate(order.source_price_pusd_per_share, &platform_order_id)
                    .is_some()
            {
                resources.merge(
                    self.queues
                        .asset_mut(&order.condition_id.0, &order.token_id.0)?
                        .remove_account_order(&platform_order_id),
                );
                let overlay_quantity_before = self.account_overlay.quantity(
                    &order.condition_id.0,
                    &order.token_id.0,
                    BookSide::Bid,
                    order.source_price_pusd_per_share,
                );
                self.account_overlay
                    .subtract_quantity(
                        &order.condition_id.0,
                        &order.token_id.0,
                        BookSide::Bid,
                        order.source_price_pusd_per_share,
                        decimal_from_micro_units_v1(open_base_units)?,
                    )
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                if !overlay_quantity_before.is_zero()
                    && self
                        .account_overlay
                        .quantity(
                            &order.condition_id.0,
                            &order.token_id.0,
                            BookSide::Bid,
                            order.source_price_pusd_per_share,
                        )
                        .is_zero()
                {
                    resources.remove_record(0);
                }
                let delta = self.account.apply_venue_dust_release(
                    &platform_order_id,
                    &order.condition_id,
                    input.effective_processing_at_utc_ms,
                    decimal_from_micro_units_v1(open_encumbered_quote_units)?,
                )?;
                account_deltas.push(delta);
                let OrderQuantityStateV2::Base {
                    open_base_units: order_open_base_units,
                    venue_dust_closed_base_units,
                    open_encumbered_quote_units: order_open_encumbered_quote_units,
                    venue_dust_released_quote_units,
                    ..
                } = &mut order.quantities
                else {
                    unreachable!("只有 base 订单能形成平台小额余量")
                };
                *order_open_base_units = 0;
                *venue_dust_closed_base_units = open_base_units;
                *order_open_encumbered_quote_units = 0;
                *venue_dust_released_quote_units = open_encumbered_quote_units;
                order.terminal_reason = Some(SimOrderTerminalReasonV1::VenueDustTerminal);
            }
        }

        if let (
            SimOrderKindStateV1::Gtd { expires_at_utc_ms },
            OrderQuantityStateV2::Base {
                open_base_units, ..
            },
        ) = (order.kind, &order.quantities)
            && *open_base_units > 0
        {
            if self
                .gtd_deadlines
                .insert((expires_at_utc_ms, platform_order_id.clone()))
            {
                resources.add_record(platform_order_id.len() as u64);
            }
        }

        let (platform_order_status, measurement) = match &order.quantities {
            OrderQuantityStateV2::Base {
                accepted_base_units,
                matched_base_units,
                open_base_units,
                venue_dust_closed_base_units,
                accepted_quote_units,
                matched_quote_units,
                open_encumbered_quote_units,
                released_price_improvement_quote_units,
                venue_dust_released_quote_units,
                ..
            } => {
                let status = if *venue_dust_closed_base_units > 0 {
                    SubmissionPlatformOrderStatusV2::VenueDustTerminal
                } else if *open_base_units > 0 {
                    SubmissionPlatformOrderStatusV2::Live
                } else if *matched_base_units > 0 {
                    SubmissionPlatformOrderStatusV2::Matched
                } else {
                    unreachable!("V2 已接受 base 订单必须开放、成交或形成平台小额余量终态")
                };
                (
                    status,
                    SubmissionMeasurementV2::Base {
                        accepted_base_units: *accepted_base_units,
                        matched_base_units: *matched_base_units,
                        open_base_units: *open_base_units,
                        venue_dust_closed_base_units: *venue_dust_closed_base_units,
                        order_type_terminated_base_units: 0,
                        accepted_quote_units: *accepted_quote_units,
                        matched_quote_units: *matched_quote_units,
                        open_encumbered_quote_units: *open_encumbered_quote_units,
                        venue_dust_released_quote_units: *venue_dust_released_quote_units,
                        released_price_improvement_quote_units:
                            *released_price_improvement_quote_units,
                        immediate_taker_fee_encumbered_units: order
                            .immediate_taker_fee_encumbered_units,
                    },
                )
            }
            OrderQuantityStateV2::QuoteBudget {
                accepted_quote_budget_units,
                matched_quote_units,
                released_or_unspent_quote_units,
                actual_matched_base_units,
                sdk_taker_amount_audit_base_units,
            } => (
                SubmissionPlatformOrderStatusV2::Matched,
                SubmissionMeasurementV2::QuoteBudget {
                    accepted_quote_budget_units: *accepted_quote_budget_units,
                    matched_quote_units: *matched_quote_units,
                    released_or_unspent_quote_units: *released_or_unspent_quote_units,
                    actual_matched_base_units: *actual_matched_base_units,
                    sdk_taker_amount_audit_base_units: *sdk_taker_amount_audit_base_units,
                    immediate_taker_fee_encumbered_units: order
                        .immediate_taker_fee_encumbered_units,
                },
            ),
        };
        let terminal_reason = order.terminal_reason;

        resources.add_record(
            (platform_order_id.len()
                + order.platform_order_id.len()
                + order.global_request_id.len()
                + order.item_id.len()
                + order.condition_id.0.len()
                + order.token_id.0.len()) as u64,
        );
        self.orders.insert(platform_order_id.clone(), order);
        self.next_order_sequence = next_order_sequence;
        self.next_fill_sequence = candidate_next_fill_sequence;

        Ok((
            self,
            BuyOrderDispositionV2::Accepted(BuyOrderAcceptedV2 {
                platform_order_id,
                platform_order_status,
                terminal_reason,
                measurement,
                initial_trade_finality,
                account_deltas,
                resources,
            }),
        ))
    }

    pub(crate) fn apply_caller_cancel(
        mut self,
        platform_order_id: &str,
        platform_effective_at_utc_ms: i64,
    ) -> Result<(Self, CallerCancelDispositionV2), MatchingStateErrorV1> {
        let Some(order) = self.orders.get(platform_order_id) else {
            return Ok((self, CallerCancelDispositionV2::NotFound));
        };
        let OrderQuantityStateV2::Base {
            pending_maker_depletion_base_units,
            open_base_units,
            open_encumbered_quote_units,
            ..
        } = &order.quantities
        else {
            return Ok((self, CallerCancelDispositionV2::NoCancellableRemainder));
        };
        let pending_base_units_before = *pending_maker_depletion_base_units;
        let open_base_units_before = *open_base_units;
        let released_open_quote_units = *open_encumbered_quote_units;
        if pending_base_units_before == 0 && open_base_units_before == 0 {
            return Ok((self, CallerCancelDispositionV2::NoCancellableRemainder));
        }
        let condition_id = order.condition_id.clone();
        let token_id = order.token_id.clone();
        let price_pusd_per_share = order.source_price_pusd_per_share;
        let gtd_expires_at_utc_ms = match order.kind {
            SimOrderKindStateV1::Gtd { expires_at_utc_ms } => Some(expires_at_utc_ms),
            SimOrderKindStateV1::Gtc | SimOrderKindStateV1::Fok | SimOrderKindStateV1::Fak => None,
        };

        let mut resources = BusinessResourceDeltaV2::default();
        if open_base_units_before > 0 {
            if !self.queues.matching_closed(&condition_id.0)? {
                let (removed_base_units, queue_resources) = self
                    .queues
                    .asset_mut(&condition_id.0, &token_id.0)?
                    .remove_account_order_at_price(price_pusd_per_share, platform_order_id)
                    .ok_or_else(|| {
                        MatchingStateErrorV1::AccountOrderConflict(platform_order_id.to_owned())
                    })?;
                if removed_base_units != open_base_units_before {
                    return Err(MatchingStateErrorV1::AccountOrderConflict(
                        platform_order_id.to_owned(),
                    ));
                }
                resources.merge(queue_resources);
            }
            let overlay_before = self.account_overlay.quantity(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                price_pusd_per_share,
            );
            self.account_overlay
                .subtract_quantity(
                    &condition_id.0,
                    &token_id.0,
                    BookSide::Bid,
                    price_pusd_per_share,
                    decimal_from_micro_units_v1(open_base_units_before)?,
                )
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            if !overlay_before.is_zero()
                && self
                    .account_overlay
                    .quantity(
                        &condition_id.0,
                        &token_id.0,
                        BookSide::Bid,
                        price_pusd_per_share,
                    )
                    .is_zero()
            {
                resources.remove_record(0);
            }
        }
        if let Some(expires_at_utc_ms) = gtd_expires_at_utc_ms {
            assert!(
                self.gtd_deadlines
                    .remove(&(expires_at_utc_ms, platform_order_id.to_owned())),
                "仍有可撤余量的 V2 GTD 订单必须仍有唯一截止任务"
            );
            resources.remove_record(platform_order_id.len() as u64);
        }
        let account_delta = self.account.apply_caller_cancel_release(
            platform_order_id,
            &condition_id,
            platform_effective_at_utc_ms,
            decimal_from_micro_units_v1(released_open_quote_units)?,
        )?;
        let canceled_base_units = pending_base_units_before
            .checked_add(open_base_units_before)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let order = self
            .orders
            .get_mut(platform_order_id)
            .expect("刚读取的 V2 可撤订单必须仍存在");
        let OrderQuantityStateV2::Base {
            pending_maker_depletion_base_units,
            open_base_units,
            canceled_base_units: order_canceled_base_units,
            open_encumbered_quote_units,
            canceled_released_quote_units,
            ..
        } = &mut order.quantities
        else {
            unreachable!("V2 可撤订单计量类型不能在同一候选内改变")
        };
        *order_canceled_base_units = order_canceled_base_units
            .checked_add(canceled_base_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        *canceled_released_quote_units = canceled_released_quote_units
            .checked_add(released_open_quote_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        *pending_maker_depletion_base_units = 0;
        *open_base_units = 0;
        *open_encumbered_quote_units = 0;
        order.last_updated_at_utc_ms = platform_effective_at_utc_ms;
        Ok((
            self,
            CallerCancelDispositionV2::Canceled(CallerCancelEffectV2 {
                condition_id,
                token_id,
                pending_base_units_before,
                open_base_units_before,
                released_open_quote_units,
                account_delta,
                resources,
            }),
        ))
    }

    pub(crate) fn active_platform_order_ids(&self) -> Vec<String> {
        self.orders
            .iter()
            .filter_map(|(platform_order_id, order)| {
                let OrderQuantityStateV2::Base {
                    pending_maker_depletion_base_units,
                    open_base_units,
                    ..
                } = &order.quantities
                else {
                    return None;
                };
                (*pending_maker_depletion_base_units > 0 || *open_base_units > 0)
                    .then(|| platform_order_id.clone())
            })
            .collect()
    }

    pub(crate) fn order_query_projection_v2(
        &self,
        platform_order_id: &str,
    ) -> Result<Option<(OrderProjectionV2, bool, i64)>, MatchingStateErrorV1> {
        self.orders
            .get(platform_order_id)
            .map(|order| {
                let active = match &order.quantities {
                    OrderQuantityStateV2::Base {
                        pending_maker_depletion_base_units,
                        open_base_units,
                        ..
                    } => *pending_maker_depletion_base_units > 0 || *open_base_units > 0,
                    OrderQuantityStateV2::QuoteBudget { .. } => false,
                };
                order_projection_v2(order)
                    .map(|projection| (projection, active, order.last_updated_at_utc_ms))
            })
            .transpose()
    }

    pub(crate) fn order_projections_v2(
        &self,
    ) -> Result<Vec<OrderProjectionV2>, MatchingStateErrorV1> {
        self.orders.values().map(order_projection_v2).collect()
    }

    pub(crate) fn trade_projections_v2(
        &self,
        finality_policy: &TradeFinalityPolicyV2,
    ) -> Result<Vec<TradeProjectionV2>, MatchingStateErrorV1> {
        self.fills
            .values()
            .map(|fill| {
                let order = self
                    .orders
                    .get(&fill.platform_order_id)
                    .expect("V2 成交只引用当前订单");
                trade_projection_v2(fill, order, finality_policy)
            })
            .collect()
    }

    pub(crate) fn next_gtd_deadline_at_utc_ms(&self) -> Option<i64> {
        self.gtd_deadlines
            .first()
            .map(|(expires_at_utc_ms, _)| *expires_at_utc_ms)
    }

    pub(crate) fn advance_next_gtd_deadline_at(
        mut self,
        platform_event_at_utc_ms: i64,
    ) -> Result<(Self, GtdExpirationAdvanceEffectsV2), MatchingStateErrorV1> {
        let Some(next_at) = self.next_gtd_deadline_at_utc_ms() else {
            return Ok((self, GtdExpirationAdvanceEffectsV2::default()));
        };
        if next_at > platform_event_at_utc_ms {
            return Ok((self, GtdExpirationAdvanceEffectsV2::default()));
        }
        if next_at < platform_event_at_utc_ms {
            return Err(MatchingStateErrorV1::LogicalTimeRegression);
        }

        let (expires_at_utc_ms, platform_order_id) = self
            .gtd_deadlines
            .first()
            .cloned()
            .expect("刚读取的 V2 GTD 截止任务仍是首项");
        self.gtd_deadlines
            .remove(&(expires_at_utc_ms, platform_order_id.clone()));
        let mut resources = BusinessResourceDeltaV2::default();
        resources.remove_record(platform_order_id.len() as u64);

        let (
            condition_id,
            token_id,
            price_pusd_per_share,
            pending_base_units_before,
            open_base_units_before,
            released_open_quote_units,
        ) = {
            let order = self
                .orders
                .get(&platform_order_id)
                .expect("V2 GTD 截止任务只引用当前订单");
            let OrderQuantityStateV2::Base {
                pending_maker_depletion_base_units,
                open_base_units,
                open_encumbered_quote_units,
                ..
            } = &order.quantities
            else {
                unreachable!("只有 base 计量 GTD 订单会登记截止任务")
            };
            (
                order.condition_id.clone(),
                order.token_id.clone(),
                order.source_price_pusd_per_share,
                *pending_maker_depletion_base_units,
                *open_base_units,
                *open_encumbered_quote_units,
            )
        };

        if open_base_units_before > 0 {
            if !self.queues.matching_closed(&condition_id.0)?
                && let Some((_, queue_resources)) = self
                    .queues
                    .asset_mut(&condition_id.0, &token_id.0)?
                    .remove_account_order_at_price(price_pusd_per_share, &platform_order_id)
            {
                resources.merge(queue_resources);
            }
            let overlay_before = self.account_overlay.quantity(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                price_pusd_per_share,
            );
            self.account_overlay
                .subtract_quantity(
                    &condition_id.0,
                    &token_id.0,
                    BookSide::Bid,
                    price_pusd_per_share,
                    decimal_from_micro_units_v1(open_base_units_before)?,
                )
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            if !overlay_before.is_zero()
                && self
                    .account_overlay
                    .quantity(
                        &condition_id.0,
                        &token_id.0,
                        BookSide::Bid,
                        price_pusd_per_share,
                    )
                    .is_zero()
            {
                resources.remove_record(0);
            }
        }

        let account_delta = self.account.apply_gtd_expiration_release(
            &platform_order_id,
            expires_at_utc_ms,
            &condition_id,
            platform_event_at_utc_ms,
            decimal_from_micro_units_v1(released_open_quote_units)?,
        )?;
        let account_transition_seq = account_delta
            .as_ref()
            .map(|delta| delta.account_transition_seq);
        let account_deltas = account_delta.into_iter().collect();
        let expired_base_units = pending_base_units_before
            .checked_add(open_base_units_before)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;

        let order = self
            .orders
            .get_mut(&platform_order_id)
            .expect("V2 GTD 到期候选内仍有当前订单");
        let OrderQuantityStateV2::Base {
            pending_maker_depletion_base_units,
            open_base_units,
            expired_base_units: order_expired_base_units,
            open_encumbered_quote_units,
            expired_released_quote_units,
            ..
        } = &mut order.quantities
        else {
            unreachable!("只有 base 计量 GTD 订单会登记截止任务")
        };
        *order_expired_base_units = order_expired_base_units
            .checked_add(expired_base_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        *expired_released_quote_units = expired_released_quote_units
            .checked_add(released_open_quote_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        *pending_maker_depletion_base_units = 0;
        *open_base_units = 0;
        *open_encumbered_quote_units = 0;
        order.last_updated_at_utc_ms = platform_event_at_utc_ms;

        Ok((
            self,
            GtdExpirationAdvanceEffectsV2 {
                expiration: Some(GtdExpirationEffectV2 {
                    platform_order_id,
                    condition_id,
                    token_id,
                    expires_at_utc_ms,
                    pending_base_units_before,
                    open_base_units_before,
                    released_open_quote_units,
                    account_transition_seq,
                }),
                account_deltas,
                resources,
            },
        ))
    }

    pub(crate) fn next_scheduled_finality_at_utc_ms(&self) -> Option<i64> {
        self.scheduled_finality
            .first()
            .map(|key| key.platform_event_at_utc_ms)
    }

    pub(crate) fn advance_next_scheduled_finality_at(
        mut self,
        platform_event_at_utc_ms: i64,
        finality_policy: &TradeFinalityPolicyV2,
        rules: &PlatformRuleSnapshotV3,
    ) -> Result<(Self, ScheduledFinalityAdvanceEffectsV2), MatchingStateErrorV1> {
        let Some(next_at) = self.next_scheduled_finality_at_utc_ms() else {
            return Ok((self, ScheduledFinalityAdvanceEffectsV2::default()));
        };
        if next_at > platform_event_at_utc_ms {
            return Ok((self, ScheduledFinalityAdvanceEffectsV2::default()));
        }
        if next_at < platform_event_at_utc_ms {
            return Err(MatchingStateErrorV1::LogicalTimeRegression);
        }

        let key = self
            .scheduled_finality
            .first()
            .copied()
            .expect("刚读取的 V2 最终性排程仍是首项");
        self.scheduled_finality.remove(&key);
        let mut effects = ScheduledFinalityAdvanceEffectsV2::default();
        effects.resources.remove_record(0);
        effects.transition = Some(self.apply_scheduled_finality_transition(
            key,
            finality_policy,
            rules,
            &mut effects.account_deltas,
            &mut effects.resources,
        )?);
        Ok((self, effects))
    }

    fn apply_scheduled_finality_transition(
        &mut self,
        key: ScheduledFinalityTransitionKeyV2,
        finality_policy: &TradeFinalityPolicyV2,
        rules: &PlatformRuleSnapshotV3,
        account_deltas: &mut Vec<PlatformAccountDeltaV2>,
        resources: &mut BusinessResourceDeltaV2,
    ) -> Result<TradeFinalityLifecycleEffectsV2, MatchingStateErrorV1> {
        let (
            trade_id,
            platform_order_id,
            matched_at_utc_ms,
            liquidity_role,
            source_price_pusd_per_share,
            base_units,
            quote_units,
            immediate_taker_fee_units,
            economic_application_before,
            selected_finality_path,
        ) = {
            let fill = self
                .fills
                .get(&key.fill_sequence)
                .expect("V2 最终性排程只引用当前成交");
            (
                fill.trade_id.clone(),
                fill.platform_order_id.clone(),
                fill.matched_at_utc_ms,
                fill.liquidity_role,
                fill.price_pusd_per_share,
                fill.base_units,
                fill.quote_units,
                fill.immediate_taker_fee_units,
                fill.economic_application_account_transition_seq,
                fill.selected_finality_path,
            )
        };
        let (condition_id, token_id) = {
            let order = self
                .orders
                .get(&platform_order_id)
                .expect("V2 成交只引用当前订单");
            (order.condition_id.clone(), order.token_id.clone())
        };
        let path = finality_policy.path_for_selection(selected_finality_path);
        let step = path
            .steps()
            .get(usize::try_from(key.path_ordinal).expect("V2 最终性 ordinal 可表示为 usize"))
            .expect("V2 最终性排程只引用已密封路径步骤");
        let finality_state = step.state();
        let economic_application = path.economic_application_ordinal() == Some(key.path_ordinal);
        let failed = finality_state == TradeFinalityStateV1::Failed;
        let releases_fee =
            economic_application || (failed && economic_application_before.is_none());
        let amounts = FillEconomicAmountsV1 {
            base_units,
            quote_units,
            immediate_taker_fee_units,
        };

        let outcome_balances_before = self.account.outcome_balance_record_count();
        let account_delta = if economic_application {
            Some(self.account.apply_fill_finality_economic(
                &trade_id,
                key.path_ordinal,
                &condition_id,
                &token_id,
                key.platform_event_at_utc_ms,
                amounts,
            )?)
        } else if failed {
            Some(if economic_application_before.is_some() {
                self.account.apply_fill_finality_compensation(
                    &trade_id,
                    key.path_ordinal,
                    &condition_id,
                    &token_id,
                    key.platform_event_at_utc_ms,
                    amounts,
                )?
            } else {
                self.account.apply_fill_finality_unapplied_failure_release(
                    &trade_id,
                    key.path_ordinal,
                    &condition_id,
                    key.platform_event_at_utc_ms,
                    quote_units,
                    immediate_taker_fee_units,
                )?
            })
        } else {
            None
        };
        let account_transition_seq = account_delta
            .as_ref()
            .map(|delta| delta.account_transition_seq);
        if let Some(delta) = account_delta {
            account_deltas.push(delta);
        }
        if self.account.outcome_balance_record_count() > outcome_balances_before {
            resources.add_record(token_id.0.len() as u64);
        }

        if releases_fee {
            let order = self
                .orders
                .get_mut(&platform_order_id)
                .expect("V2 最终性订单在候选内仍存在");
            order.immediate_taker_fee_encumbered_units = order
                .immediate_taker_fee_encumbered_units
                .checked_sub(immediate_taker_fee_units)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            order.last_updated_at_utc_ms = key.platform_event_at_utc_ms;
        }

        let economic_application_after = {
            let fill = self
                .fills
                .get_mut(&key.fill_sequence)
                .expect("V2 最终性成交在候选内仍存在");
            fill.finality_path_ordinal = key.path_ordinal;
            if economic_application {
                fill.economic_application_account_transition_seq = account_transition_seq;
            }
            fill.economic_application_account_transition_seq
        };
        let next_path_ordinal = key
            .path_ordinal
            .checked_add(1)
            .expect("已密封有限最终性路径 ordinal 可递增");
        if let Some(next_step) = path
            .steps()
            .get(usize::try_from(next_path_ordinal).expect("V2 最终性 ordinal 可表示为 usize"))
        {
            self.scheduled_finality
                .insert(ScheduledFinalityTransitionKeyV2 {
                    platform_event_at_utc_ms: matched_at_utc_ms
                        .checked_add(next_step.offset_ms())
                        .expect("V2 成交创建时已证明完整路径时点可表示"),
                    fill_sequence: key.fill_sequence,
                    path_ordinal: next_path_ordinal,
                });
            resources.add_record(0);
        }

        let economic_disposition = match finality_state {
            TradeFinalityStateV1::Confirmed => TradeEconomicDispositionStateV1::FinalSuccess,
            TradeFinalityStateV1::Failed if economic_application_after.is_some() => {
                TradeEconomicDispositionStateV1::FinalFailureCompensated
            }
            TradeFinalityStateV1::Failed => TradeEconomicDispositionStateV1::FinalFailureUnapplied,
            TradeFinalityStateV1::MatchedNotBroadcasted
            | TradeFinalityStateV1::Matched
            | TradeFinalityStateV1::Retrying
            | TradeFinalityStateV1::Mined
                if economic_application_after.is_some() =>
            {
                TradeEconomicDispositionStateV1::PendingApplied
            }
            TradeFinalityStateV1::MatchedNotBroadcasted
            | TradeFinalityStateV1::Matched
            | TradeFinalityStateV1::Retrying
            | TradeFinalityStateV1::Mined => TradeEconomicDispositionStateV1::PendingUnapplied,
        };
        let trade_finality = TradeFinalityTransitionEffectV2 {
            fill_sequence: key.fill_sequence,
            trade_id: trade_id.clone(),
            platform_order_id,
            condition_id: condition_id.clone(),
            token_id,
            matched_at_utc_ms,
            platform_event_at_utc_ms: key.platform_event_at_utc_ms,
            path_ordinal: key.path_ordinal,
            finality_state,
            economic_disposition,
            liquidity_role,
            source_price_pusd_per_share,
            base_units,
            quote_units,
            immediate_taker_fee_units,
            economic_application_account_transition_seq: economic_application_after,
            account_transition_seq,
        };
        let mut settlement = Vec::new();
        if matches!(
            finality_state,
            TradeFinalityStateV1::Confirmed | TradeFinalityStateV1::Failed
        ) {
            let succeeded = finality_state == TradeFinalityStateV1::Confirmed;
            match liquidity_role {
                SimLiquidityRoleV1::MakerInferred => self.complete_maker_rebate_fill(
                    key.fill_sequence,
                    &trade_id,
                    &condition_id,
                    matched_at_utc_ms,
                    source_price_pusd_per_share,
                    base_units,
                    succeeded,
                    rules,
                    key.platform_event_at_utc_ms,
                    &mut settlement,
                    account_deltas,
                    resources,
                )?,
                SimLiquidityRoleV1::Taker if immediate_taker_fee_units > 0 => self
                    .complete_taker_rebate_fill(
                        key.fill_sequence,
                        &trade_id,
                        &condition_id,
                        matched_at_utc_ms,
                        immediate_taker_fee_units,
                        succeeded,
                        rules
                            .project_account_rules
                            .rebate_settlement
                            .value
                            .fixed_taker_rebate_rate
                            .value(),
                        key.platform_event_at_utc_ms,
                        &mut settlement,
                        account_deltas,
                    )?,
                SimLiquidityRoleV1::Taker => {}
            }
            if self
                .market_resolutions
                .get_mut(&condition_id.0)
                .is_some_and(|obligation| {
                    obligation.pending_fill_sequences.remove(&key.fill_sequence)
                })
            {
                resources.remove_record(0);
            }
            if let Some(auto_redeem) =
                self.try_auto_redeem(&condition_id, key.platform_event_at_utc_ms, account_deltas)?
            {
                settlement.push(EconomicLifecycleEffectV2::AutoRedeem(auto_redeem));
            }
            self.settle_ready_days(
                key.platform_event_at_utc_ms,
                &mut settlement,
                account_deltas,
            )?;
        }
        Ok(TradeFinalityLifecycleEffectsV2 {
            trade_finality,
            settlement,
        })
    }

    fn complete_taker_rebate_fill(
        &mut self,
        fill_sequence: u64,
        trade_id: &str,
        condition_id: &ConditionId,
        matched_at_utc_ms: i64,
        immediate_taker_fee_units: u128,
        succeeded: bool,
        rebate_rate: Decimal,
        platform_effective_at_utc_ms: i64,
        settlement: &mut Vec<EconomicLifecycleEffectV2>,
        account_deltas: &mut Vec<PlatformAccountDeltaV2>,
    ) -> Result<(), MatchingStateErrorV1> {
        let attribution_day_utc_ms = utc_day_start(matched_at_utc_ms)?;
        let accrual_effect = if succeeded {
            let actual_taker_fee_pusd = decimal_from_micro_units_v1(immediate_taker_fee_units)?;
            let accrual_pusd = actual_taker_fee_pusd
                .checked_mul(rebate_rate)
                .ok_or(RebateStateErrorV1::ArithmeticOverflow)?
                .normalize();
            let delta = self.account.apply_taker_rebate_accrual_applied(
                trade_id,
                condition_id,
                platform_effective_at_utc_ms,
                accrual_pusd,
            )?;
            let account_transition_seq = delta.account_transition_seq;
            account_deltas.push(delta);
            Some(TakerRebateAccrualEffectV2 {
                fill_sequence,
                trade_id: trade_id.to_owned(),
                condition_id: condition_id.clone(),
                matched_at_utc_ms,
                attribution_day_utc_ms,
                platform_effective_at_utc_ms,
                actual_taker_fee_pusd,
                rebate_rate,
                accrual_pusd,
                account_transition_seq,
            })
        } else {
            None
        };

        let day = self
            .taker_rebate
            .days
            .get_mut(&attribution_day_utc_ms)
            .expect("正手续费成交终态只引用已登记 taker 日桶");
        if let Some(accrual) = &accrual_effect {
            day.accrued_pusd = day
                .accrued_pusd
                .checked_add(accrual.accrual_pusd)
                .ok_or(RebateStateErrorV1::ArithmeticOverflow)?
                .normalize();
        }
        day.pending_predecessor_count = day
            .pending_predecessor_count
            .checked_sub(1)
            .expect("正手续费成交终态只闭合一个已登记前驱");
        if let Some(accrual_effect) = accrual_effect {
            settlement.push(EconomicLifecycleEffectV2::TakerAccrual(accrual_effect));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn complete_maker_rebate_fill(
        &mut self,
        fill_sequence: u64,
        trade_id: &str,
        condition_id: &ConditionId,
        matched_at_utc_ms: i64,
        price_pusd_per_share: Decimal,
        base_units: u128,
        succeeded: bool,
        rules: &PlatformRuleSnapshotV3,
        platform_effective_at_utc_ms: i64,
        settlement: &mut Vec<EconomicLifecycleEffectV2>,
        account_deltas: &mut Vec<PlatformAccountDeltaV2>,
        resources: &mut BusinessResourceDeltaV2,
    ) -> Result<(), MatchingStateErrorV1> {
        if !self
            .maker_rebate
            .pending_fill_sequences
            .remove(&fill_sequence)
        {
            return Err(MatchingStateErrorV1::Rebate(format!(
                "maker 成交前驱不存在：{fill_sequence}"
            )));
        }
        resources.remove_record(0);
        let attribution_day_utc_ms = utc_day_start(matched_at_utc_ms)?;
        let contribution = succeeded
            .then(|| {
                maker_rebate_contribution_v1(
                    trade_id,
                    matched_at_utc_ms,
                    base_units,
                    price_pusd_per_share,
                    rules.synchronized_platform_rules.fee_schedule.rate.value(),
                    rules.synchronized_platform_rules.fee_schedule.exponent,
                    rules
                        .project_account_rules
                        .rebate_settlement
                        .value
                        .fixed_maker_rebate_rate
                        .value(),
                )
            })
            .transpose()?;
        let market = self
            .maker_rebate
            .markets
            .get_mut(&condition_id.0)
            .ok_or_else(|| RebateStateErrorV1::UnknownMarket(condition_id.0.clone()))?;
        market.pending_fill_count = market
            .pending_fill_count
            .checked_sub(1)
            .expect("maker 成交终态只闭合已登记市场前驱");
        let pending_day = market
            .pending_by_day
            .get_mut(&attribution_day_utc_ms)
            .expect("maker 成交终态只引用已登记市场日前驱");
        *pending_day = pending_day
            .checked_sub(1)
            .expect("maker 成交终态只闭合一个市场日成交前驱");
        if let Some(contribution) = contribution {
            market.successful_fill_count = market
                .successful_fill_count
                .checked_add(1)
                .ok_or(RebateStateErrorV1::ArithmeticOverflow)?;
            resources.add_record(contribution.trade_id.len() as u64);
            market
                .successful_contributions
                .insert(fill_sequence, contribution);
        }
        if let Some(effect) = self.finalize_ready_maker_market(
            condition_id,
            platform_effective_at_utc_ms,
            account_deltas,
            resources,
        )? {
            settlement.push(EconomicLifecycleEffectV2::MakerMarketFinalized(effect));
        }
        Ok(())
    }

    fn finalize_ready_maker_market(
        &mut self,
        condition_id: &ConditionId,
        platform_effective_at_utc_ms: i64,
        account_deltas: &mut Vec<PlatformAccountDeltaV2>,
        resources: &mut BusinessResourceDeltaV2,
    ) -> Result<Option<MakerRebateMarketFinalizedEffectV2>, MatchingStateErrorV1> {
        let Some((status, contributions, pending_by_day)) = self
            .maker_rebate
            .markets
            .get_mut(&condition_id.0)
            .and_then(|market| {
                (market.end_reached && market.pending_fill_count == 0 && market.result.is_none())
                    .then(|| {
                        let status = if market.successful_fill_count > 0 {
                            MakerRebateMarketResultStatusV1::Calculated
                        } else {
                            MakerRebateMarketResultStatusV1::ZeroNoEligibleMakerFill
                        };
                        market.result = Some(status);
                        (
                            status,
                            std::mem::take(&mut market.successful_contributions),
                            std::mem::take(&mut market.pending_by_day),
                        )
                    })
            })
        else {
            return Ok(None);
        };
        let contributions = contributions.into_values().collect::<Vec<_>>();
        let mut accrual_by_day = BTreeMap::<i64, Decimal>::new();
        let mut accrual_pusd = Decimal::ZERO;
        for contribution in &contributions {
            resources.remove_record(contribution.trade_id.len() as u64);
            accrual_pusd = accrual_pusd
                .checked_add(contribution.contribution_pusd)
                .ok_or(RebateStateErrorV1::ArithmeticOverflow)?
                .normalize();
            let accrued = accrual_by_day
                .entry(contribution.attribution_day_utc_ms)
                .or_default();
            *accrued = accrued
                .checked_add(contribution.contribution_pusd)
                .ok_or(RebateStateErrorV1::ArithmeticOverflow)?
                .normalize();
        }
        for (attribution_day_utc_ms, _) in pending_by_day {
            resources.remove_record(0);
            let day = self
                .maker_rebate
                .days
                .get_mut(&attribution_day_utc_ms)
                .expect("maker 市场终态只引用已登记共享日桶");
            day.pending_predecessor_count = day
                .pending_predecessor_count
                .checked_sub(1)
                .expect("maker 市场终态只闭合一个共享日桶前驱");
            day.accrued_pusd = day
                .accrued_pusd
                .checked_add(
                    accrual_by_day
                        .get(&attribution_day_utc_ms)
                        .copied()
                        .unwrap_or(Decimal::ZERO),
                )
                .ok_or(RebateStateErrorV1::ArithmeticOverflow)?
                .normalize();
        }
        let account_delta = (accrual_pusd > Decimal::ZERO)
            .then(|| {
                self.account.apply_maker_rebate_accrual_finalized(
                    condition_id,
                    platform_effective_at_utc_ms,
                    accrual_pusd,
                )
            })
            .transpose()?;
        let account_transition_seq = account_delta
            .as_ref()
            .map(|delta| delta.account_transition_seq);
        if let Some(delta) = account_delta {
            account_deltas.push(delta);
        }
        Ok(Some(MakerRebateMarketFinalizedEffectV2 {
            condition_id: condition_id.clone(),
            platform_effective_at_utc_ms,
            status,
            contributions,
            accrual_pusd,
            account_transition_seq,
        }))
    }

    fn try_auto_redeem(
        &mut self,
        condition_id: &ConditionId,
        platform_effective_at_utc_ms: i64,
        account_deltas: &mut Vec<PlatformAccountDeltaV2>,
    ) -> Result<Option<AutoRedeemEffectV2>, MatchingStateErrorV1> {
        let Some(obligation) = self.market_resolutions.get(&condition_id.0) else {
            return Ok(None);
        };
        if obligation.redeemed_at_utc_ms.is_some()
            || !obligation.pending_fill_sequences.is_empty()
            || self
                .maker_rebate
                .markets
                .get(&condition_id.0)
                .and_then(|market| market.result)
                .is_none()
        {
            return Ok(None);
        }
        if obligation.invalidated_by_quality_reason.is_some() {
            return Err(MatchingStateErrorV1::ResolutionConflict(
                condition_id.0.clone(),
            ));
        }
        let (resolution_event_id, resolution_source_event_at_utc_ms, terminal, parsed_payouts) = (
            obligation.resolution_event_id.clone(),
            obligation.resolution_source_event_at_utc_ms,
            obligation.terminal.clone(),
            obligation.parsed_payouts,
        );
        let MarketResolutionTerminalV3::Resolved { payout_vector, .. } = &terminal;
        let parsed_binary_payout = [
            (payout_vector[0].asset.token_id.clone(), parsed_payouts[0]),
            (payout_vector[1].asset.token_id.clone(), parsed_payouts[1]),
        ];
        let AutoRedeemAccountEffectV2 {
            redeemed_outcome_balances,
            redeemed_pusd,
            account_delta,
        } = self.account.apply_auto_redeem(
            &resolution_event_id,
            condition_id,
            platform_effective_at_utc_ms,
            &parsed_binary_payout,
        )?;
        let account_transition_seq = account_delta
            .as_ref()
            .map(|delta| delta.account_transition_seq);
        if let Some(delta) = account_delta {
            account_deltas.push(delta);
        }
        self.market_resolutions
            .get_mut(&condition_id.0)
            .expect("自动赎回义务必须仍存在")
            .redeemed_at_utc_ms = Some(platform_effective_at_utc_ms);
        Ok(Some(AutoRedeemEffectV2 {
            condition_id: condition_id.clone(),
            resolution_event_id,
            resolution_source_event_at_utc_ms,
            terminal,
            platform_effective_at_utc_ms,
            redeemed_outcome_balances,
            redeemed_pusd,
            account_transition_seq,
        }))
    }

    fn settle_ready_days(
        &mut self,
        platform_effective_at_utc_ms: i64,
        settlement: &mut Vec<EconomicLifecycleEffectV2>,
        account_deltas: &mut Vec<PlatformAccountDeltaV2>,
    ) -> Result<(), MatchingStateErrorV1> {
        let ready_maker_days = self
            .maker_rebate
            .days
            .iter()
            .filter_map(|(day, state)| {
                (state.maturity_reached
                    && state.pending_predecessor_count == 0
                    && state.final_state.is_none())
                .then_some(*day)
            })
            .collect::<Vec<_>>();
        let ready_taker_days = self
            .taker_rebate
            .days
            .iter()
            .filter_map(|(day, state)| {
                (state.maturity_reached
                    && state.pending_predecessor_count == 0
                    && state.final_state.is_none())
                .then_some(*day)
            })
            .collect::<Vec<_>>();
        let mut maker_payments = Vec::new();
        let mut taker_payments = Vec::new();
        for (kind, days) in [
            (RebateKindV1::Maker, ready_maker_days),
            (RebateKindV1::Taker, ready_taker_days),
        ] {
            for attribution_day_utc_ms in days {
                let accrued_pusd = match kind {
                    RebateKindV1::Maker => {
                        self.maker_rebate
                            .days
                            .get(&attribution_day_utc_ms)
                            .expect("刚筛选的 maker 日桶仍存在")
                            .accrued_pusd
                    }
                    RebateKindV1::Taker => {
                        self.taker_rebate
                            .days
                            .get(&attribution_day_utc_ms)
                            .expect("刚筛选的 taker 日桶仍存在")
                            .accrued_pusd
                    }
                };
                let mut final_state = finalized_day_state(accrued_pusd);
                final_state.paid = false;
                let account_delta = match kind {
                    RebateKindV1::Maker => self.account.apply_maker_rebate_day_finalized(
                        attribution_day_utc_ms,
                        platform_effective_at_utc_ms,
                        final_state.locked_remainder_pusd,
                    )?,
                    RebateKindV1::Taker => self.account.apply_taker_rebate_day_finalized(
                        attribution_day_utc_ms,
                        platform_effective_at_utc_ms,
                        final_state.locked_remainder_pusd,
                    )?,
                };
                let account_transition_seq = account_delta
                    .as_ref()
                    .map(|delta| delta.account_transition_seq);
                if let Some(delta) = account_delta {
                    account_deltas.push(delta);
                }
                settlement.push(EconomicLifecycleEffectV2::DayFinalized(
                    RebateDayFinalizedEffectV2 {
                        kind,
                        attribution_day_utc_ms,
                        platform_effective_at_utc_ms,
                        accrued_pusd,
                        payable_pusd: final_state.payable_pusd,
                        locked_remainder_pusd: final_state.locked_remainder_pusd,
                        account_transition_seq,
                    },
                ));
                match kind {
                    RebateKindV1::Maker => {
                        self.maker_rebate
                            .days
                            .get_mut(&attribution_day_utc_ms)
                            .expect("刚终结的 maker 日桶仍存在")
                            .final_state = Some(final_state.clone());
                        if final_state.payable_pusd > Decimal::ZERO {
                            maker_payments.push((attribution_day_utc_ms, final_state.payable_pusd));
                        }
                    }
                    RebateKindV1::Taker => {
                        self.taker_rebate
                            .days
                            .get_mut(&attribution_day_utc_ms)
                            .expect("刚终结的 taker 日桶仍存在")
                            .final_state = Some(final_state.clone());
                        if final_state.payable_pusd > Decimal::ZERO {
                            taker_payments.push((attribution_day_utc_ms, final_state.payable_pusd));
                        }
                    }
                }
            }
        }
        for (kind, payments) in [
            (RebateKindV1::Maker, maker_payments),
            (RebateKindV1::Taker, taker_payments),
        ] {
            for (attribution_day_utc_ms, paid_pusd) in payments {
                let delta = match kind {
                    RebateKindV1::Maker => self.account.apply_maker_rebate_paid(
                        attribution_day_utc_ms,
                        platform_effective_at_utc_ms,
                        paid_pusd,
                    )?,
                    RebateKindV1::Taker => self.account.apply_taker_rebate_paid(
                        attribution_day_utc_ms,
                        platform_effective_at_utc_ms,
                        paid_pusd,
                    )?,
                };
                let account_transition_seq = delta.account_transition_seq;
                account_deltas.push(delta);
                settlement.push(EconomicLifecycleEffectV2::Paid(RebatePaidEffectV2 {
                    kind,
                    attribution_day_utc_ms,
                    platform_effective_at_utc_ms,
                    paid_pusd,
                    account_transition_seq,
                }));
                match kind {
                    RebateKindV1::Maker => {
                        self.maker_rebate
                            .days
                            .get_mut(&attribution_day_utc_ms)
                            .expect("刚付款的 maker 日桶仍存在")
                            .final_state
                            .as_mut()
                            .expect("maker 付款只发生在已日结日桶")
                            .paid = true
                    }
                    RebateKindV1::Taker => {
                        self.taker_rebate
                            .days
                            .get_mut(&attribution_day_utc_ms)
                            .expect("刚付款的 taker 日桶仍存在")
                            .final_state
                            .as_mut()
                            .expect("taker 付款只发生在已日结日桶")
                            .paid = true
                    }
                }
            }
        }
        Ok(())
    }

    fn next_rebate_clock(&self) -> Option<(i64, PendingRebateClockV2)> {
        let mut next = self
            .maker_rebate
            .market_end_clocks
            .first()
            .map(|(at, condition_id)| {
                (
                    *at,
                    0_u8,
                    PendingRebateClockV2::MakerMarketEnd(condition_id.clone()),
                )
            });
        for candidate in [
            self.maker_rebate
                .day_clocks
                .first()
                .map(|(at, day)| (*at, 1_u8, PendingRebateClockV2::MakerDay(*day))),
            self.taker_rebate
                .clocks
                .first()
                .map(|(at, day)| (*at, 2_u8, PendingRebateClockV2::TakerDay(*day))),
        ]
        .into_iter()
        .flatten()
        {
            if next
                .as_ref()
                .is_none_or(|current| (candidate.0, candidate.1) < (current.0, current.1))
            {
                next = Some(candidate);
            }
        }
        next.map(|(at, _, clock)| (at, clock))
    }

    pub(crate) fn next_rebate_clock_at_utc_ms(&self) -> Option<i64> {
        self.next_rebate_clock().map(|(at, _)| at)
    }

    pub(crate) fn advance_next_rebate_clock_at(
        mut self,
        platform_effective_at_utc_ms: i64,
    ) -> Result<(Self, RebateClockAdvanceEffectsV2), MatchingStateErrorV1> {
        let Some((next_at, clock)) = self.next_rebate_clock() else {
            return Ok((self, RebateClockAdvanceEffectsV2::default()));
        };
        if next_at > platform_effective_at_utc_ms {
            return Ok((self, RebateClockAdvanceEffectsV2::default()));
        }
        if next_at < platform_effective_at_utc_ms {
            return Err(MatchingStateErrorV1::LogicalTimeRegression);
        }
        let mut effects = RebateClockAdvanceEffectsV2::default();
        match clock {
            PendingRebateClockV2::MakerMarketEnd(condition_id) => {
                self.maker_rebate
                    .market_end_clocks
                    .remove(&(next_at, condition_id.clone()));
                effects.resources.remove_record(condition_id.len() as u64);
                self.maker_rebate
                    .markets
                    .get_mut(&condition_id)
                    .expect("maker 市场结束时钟只引用已登记市场")
                    .end_reached = true;
                let condition = ConditionId(condition_id);
                if let Some(effect) = self.finalize_ready_maker_market(
                    &condition,
                    platform_effective_at_utc_ms,
                    &mut effects.account_deltas,
                    &mut effects.resources,
                )? {
                    effects
                        .settlement
                        .push(EconomicLifecycleEffectV2::MakerMarketFinalized(effect));
                }
                if let Some(auto_redeem) = self.try_auto_redeem(
                    &condition,
                    platform_effective_at_utc_ms,
                    &mut effects.account_deltas,
                )? {
                    effects
                        .settlement
                        .push(EconomicLifecycleEffectV2::AutoRedeem(auto_redeem));
                }
            }
            PendingRebateClockV2::MakerDay(attribution_day_utc_ms) => {
                self.maker_rebate
                    .day_clocks
                    .remove(&(next_at, attribution_day_utc_ms));
                effects.resources.remove_record(0);
                self.maker_rebate
                    .days
                    .get_mut(&attribution_day_utc_ms)
                    .expect("maker 成熟时钟只引用已登记日桶")
                    .maturity_reached = true;
            }
            PendingRebateClockV2::TakerDay(attribution_day_utc_ms) => {
                self.taker_rebate
                    .clocks
                    .remove(&(next_at, attribution_day_utc_ms));
                effects.resources.remove_record(0);
                self.taker_rebate
                    .days
                    .get_mut(&attribution_day_utc_ms)
                    .expect("taker 成熟时钟只引用已登记日桶")
                    .maturity_reached = true;
            }
        }
        self.settle_ready_days(
            platform_effective_at_utc_ms,
            &mut effects.settlement,
            &mut effects.account_deltas,
        )?;
        Ok((self, effects))
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
            .best_executable_ask()
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

impl ExecutionStateV1 {
    pub(crate) fn detached_clone(&self) -> Self {
        let mut detached = self.clone();
        detached.rebate = Arc::new(self.rebate.as_ref().clone());
        detached.market_resolutions = Arc::new(self.market_resolutions.as_ref().clone());
        detached
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
    deadline_insertions: BTreeSet<(i64, String)>,
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
    obligation_insertions: BTreeMap<String, MarketResolutionObligationV1>,
    pending_trade_removals: BTreeMap<String, BTreeSet<String>>,
    redeemed_at_after: BTreeMap<String, i64>,
    invalidated_by_quality_after: BTreeMap<String, String>,
}

pub(crate) struct PreparedExecutionDriveDeltaV2<'a> {
    live: &'a mut ExecutionStateV1,
    gtd: PreparedGtdExpirationPatchV2,
    finality: PreparedScheduledFinalityPatchV2,
    asset_replacements: BTreeMap<(String, String), AssetQueueV1>,
    order_replacements: BTreeMap<String, SimOrderRecordV1>,
    candidate_account: Option<PlatformAccountStateV1>,
    matching_closed_market_insertions: BTreeMap<String, i64>,
}

pub(crate) struct CommittableExecutionDriveDeltaV2<'a> {
    live: &'a mut ExecutionStateV1,
    gtd: PreparedGtdExpirationPatchV2,
    finality: PreparedScheduledFinalityPatchV2,
    asset_replacements: BTreeMap<(String, String), AssetQueueV1>,
    order_replacements: BTreeMap<String, SimOrderRecordV1>,
    candidate_account: Option<PlatformAccountStateV1>,
    matching_closed_market_insertions: BTreeMap<String, i64>,
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
            deadline_insertions: BTreeSet::new(),
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
            || !self.deadline_insertions.is_empty()
            || !self.bid_queue_after_values.is_empty()
            || !self.overlay_after_values.is_empty()
            || self.gtd_advanced_through_utc_ms != live.gtd_advanced_through_utc_ms
    }

    fn commit_prevalidated(self, live: &mut ExecutionStateV1) {
        for deadline in self.deadline_removals {
            live.gtd_deadlines.remove(&deadline);
        }
        live.gtd_deadlines.extend(self.deadline_insertions);
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
    fn candidate_obligation<'a>(
        &'a self,
        live: &'a BTreeMap<String, MarketResolutionObligationV1>,
        condition_id: &str,
    ) -> Option<&'a MarketResolutionObligationV1> {
        self.obligation_insertions
            .get(condition_id)
            .or_else(|| live.get(condition_id))
    }

    fn stage_obligation_insertion(
        &mut self,
        live: &BTreeMap<String, MarketResolutionObligationV1>,
        condition_id: &str,
        obligation: MarketResolutionObligationV1,
    ) -> Result<bool, MatchingStateErrorV1> {
        if let Some(existing) = self.candidate_obligation(live, condition_id) {
            if existing.resolution == obligation.resolution {
                return Ok(false);
            }
            return Err(MatchingStateErrorV1::ResolutionConflict(
                condition_id.to_owned(),
            ));
        }
        self.obligation_insertions
            .insert(condition_id.to_owned(), obligation);
        Ok(true)
    }

    fn stage_quality_invalidation(
        &mut self,
        live: &BTreeMap<String, MarketResolutionObligationV1>,
        condition_id: &str,
        reason_code: &str,
    ) {
        let Some(obligation) = self.candidate_obligation(live, condition_id) else {
            return;
        };
        if obligation.redeemed_at_utc_ms.is_none()
            && !self.redeemed_at_after.contains_key(condition_id)
            && obligation.invalidated_by_quality_reason.is_none()
            && !self.invalidated_by_quality_after.contains_key(condition_id)
        {
            self.invalidated_by_quality_after
                .insert(condition_id.to_owned(), reason_code.to_owned());
        }
    }

    fn stage_pending_trade_removal(
        &mut self,
        live: &BTreeMap<String, MarketResolutionObligationV1>,
        condition_id: &str,
        trade_id: &str,
    ) {
        let Some(obligation) = self.candidate_obligation(live, condition_id) else {
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
        let Some(obligation) = self.candidate_obligation(live, condition_id) else {
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
        if obligation.invalidated_by_quality_reason.is_some()
            || self.invalidated_by_quality_after.contains_key(condition_id)
        {
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
        !self.obligation_insertions.is_empty()
            || !self.pending_trade_removals.is_empty()
            || !self.redeemed_at_after.is_empty()
            || !self.invalidated_by_quality_after.is_empty()
    }

    fn changed_condition_id(&self) -> Option<&str> {
        self.obligation_insertions
            .keys()
            .chain(self.pending_trade_removals.keys())
            .chain(self.redeemed_at_after.keys())
            .chain(self.invalidated_by_quality_after.keys())
            .next()
            .map(String::as_str)
    }

    fn commit_prevalidated(self, live: &mut BTreeMap<String, MarketResolutionObligationV1>) {
        live.extend(self.obligation_insertions);
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
        for (condition_id, reason_code) in self.invalidated_by_quality_after {
            live.get_mut(&condition_id)
                .expect("判定质量隔离 after-image 只指向已验证义务")
                .invalidated_by_quality_reason = Some(reason_code);
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
            asset_replacements: BTreeMap::new(),
            order_replacements: BTreeMap::new(),
            candidate_account: None,
            matching_closed_market_insertions: BTreeMap::new(),
        }
    }

    pub(crate) fn prepare_without_scheduled_progress_v2(live: &'a mut ExecutionStateV1) -> Self {
        Self::prepare_without_scheduled_progress(live)
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

    pub(crate) fn candidate_redeemed_market_resolution(
        &self,
        condition_id: &str,
    ) -> Option<&MarketResolutionTerminalV3> {
        let obligation = self
            .finality
            .resolution
            .candidate_obligation(self.live.market_resolutions.as_ref(), condition_id)?;
        (obligation.redeemed_at_utc_ms.is_some()
            || self
                .finality
                .resolution
                .redeemed_at_after
                .contains_key(condition_id))
        .then_some(&obligation.resolution)
    }

    pub(crate) fn has_state_change(&self) -> bool {
        self.gtd.has_state_change(self.live)
            || self.finality.has_state_change(self.live)
            || !self.asset_replacements.is_empty()
            || !self.order_replacements.is_empty()
            || self.candidate_account.is_some()
            || !self.matching_closed_market_insertions.is_empty()
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
        let mut live = self.live.orders.iter().peekable();
        let mut replacements = self.order_replacements.iter().peekable();
        std::iter::from_fn(move || match (live.peek(), replacements.peek()) {
            (Some((live_id, _)), Some((replacement_id, _))) => match live_id.cmp(replacement_id) {
                std::cmp::Ordering::Less => live.next().map(|(_, order)| order),
                std::cmp::Ordering::Equal => {
                    live.next();
                    replacements.next().map(|(_, order)| order)
                }
                std::cmp::Ordering::Greater => replacements.next().map(|(_, order)| order),
            },
            (Some(_), None) => live.next().map(|(_, order)| order),
            (None, Some(_)) => replacements.next().map(|(_, order)| order),
            (None, None) => None,
        })
    }

    pub(crate) fn candidate_fills(&self) -> impl Iterator<Item = &SimFillRecordV1> {
        let mut live = self.live.fills.iter().peekable();
        let mut replacements = self.finality.fill_replacements.iter().peekable();
        std::iter::from_fn(move || match (live.peek(), replacements.peek()) {
            (Some((live_id, _)), Some((replacement_id, _))) => match live_id.cmp(replacement_id) {
                std::cmp::Ordering::Less => live.next().map(|(_, fill)| fill),
                std::cmp::Ordering::Equal => {
                    live.next();
                    replacements.next().map(|(_, fill)| fill)
                }
                std::cmp::Ordering::Greater => replacements.next().map(|(_, fill)| fill),
            },
            (Some(_), None) => live.next().map(|(_, fill)| fill),
            (None, Some(_)) => replacements.next().map(|(_, fill)| fill),
            (None, None) => None,
        })
    }

    pub(crate) fn candidate_orders_for_condition<'b>(
        &'b self,
        condition_id: &'b str,
    ) -> impl Iterator<Item = &'b SimOrderRecordV1> + 'b {
        self.candidate_orders()
            .filter(move |candidate| candidate.condition_id == condition_id)
    }

    pub(crate) fn candidate_fills_for_condition<'b>(
        &'b self,
        condition_id: &'b str,
    ) -> impl Iterator<Item = &'b SimFillRecordV1> + 'b {
        self.candidate_fills()
            .filter(move |candidate| candidate.condition_id == condition_id)
    }

    fn candidate_matching_closed(&self, condition_id: &str) -> bool {
        self.matching_closed_market_insertions
            .contains_key(condition_id)
            || self.live.matching_closed_markets.contains_key(condition_id)
    }

    fn take_candidate_asset(
        &mut self,
        condition_id: &str,
        token_id: &str,
    ) -> Result<AssetQueueV1, MatchingStateErrorV1> {
        let asset_key = (condition_id.to_owned(), token_id.to_owned());
        if let Some(asset) = self.asset_replacements.remove(&asset_key) {
            return Ok(asset);
        }
        let mut asset = self.live.queues.asset(condition_id, token_id)?.clone();
        for (key, after) in &self.gtd.bid_queue_after_values {
            if key.condition_id == condition_id && key.token_id == token_id {
                if let Some(queue) = after {
                    asset.bids.insert(key.price_pusd_per_share, queue.clone());
                } else {
                    asset.bids.remove(&key.price_pusd_per_share);
                }
            }
        }
        Ok(asset)
    }

    fn stage_queue_event(
        &mut self,
        event: &ExecutionMarketEventV3,
    ) -> Result<QueueEventEffectsV1, MatchingStateErrorV1> {
        if self.candidate_matching_closed(&event.market.condition_id.0) {
            return Ok(QueueEventEffectsV1::default());
        }
        let token_id = match &event.payload {
            ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) => {
                &snapshot.asset.token_id.0
            }
            ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) => {
                &transition.asset.token_id.0
            }
            ExecutionMarketEventPayloadV3::QualityBoundary(boundary)
                if event.quality.resync == ResyncQuality::Required =>
            {
                let QualityScopeV1::Asset(asset) = &boundary.scope else {
                    return Ok(QueueEventEffectsV1::default());
                };
                &asset.token_id.0
            }
            ExecutionMarketEventPayloadV3::QualityBoundary(_)
            | ExecutionMarketEventPayloadV3::MarketLifecycleTransition(_)
            | ExecutionMarketEventPayloadV3::MarketTradingState(_)
            | ExecutionMarketEventPayloadV3::BestBidAsk(_)
            | ExecutionMarketEventPayloadV3::TickSizeChange(_)
            | ExecutionMarketEventPayloadV3::PublicTrade(_)
            | ExecutionMarketEventPayloadV3::MarketResolutionTerminal(_) => {
                return Ok(QueueEventEffectsV1::default());
            }
        };
        let condition_id = &event.market.condition_id.0;
        let candidate_asset = self.take_candidate_asset(condition_id, token_id)?;
        let mut queues = ExecutionQueueStateV1 {
            markets: BTreeMap::from([(
                condition_id.clone(),
                BTreeMap::from([(token_id.clone(), candidate_asset)]),
            )]),
        };
        let effects = queues.apply_authorized_event(event)?;
        let candidate_asset = queues
            .markets
            .remove(condition_id)
            .and_then(|mut assets| assets.remove(token_id))
            .expect("DPT 稀疏队列候选必须保留目标资产");
        self.asset_replacements
            .insert((condition_id.clone(), token_id.clone()), candidate_asset);
        Ok(effects)
    }

    fn stage_release_initialized_market(
        &mut self,
        market: &PlatformMarketV1,
    ) -> Result<(), MatchingStateErrorV1> {
        let condition_id = &market.market().condition_id.0;
        if self.candidate_matching_closed(condition_id) {
            return Ok(());
        }
        let fully_initialized = market.assets().iter().all(|asset| {
            self.asset_replacements
                .get(&(condition_id.clone(), asset.token_id.0.clone()))
                .or_else(|| {
                    self.live
                        .queues
                        .markets
                        .get(condition_id)
                        .and_then(|assets| assets.get(&asset.token_id.0))
                })
                .is_some_and(|asset| asset.initial_loaded)
        });
        if !fully_initialized {
            return Ok(());
        }
        for asset in market.assets() {
            let mut candidate = self.take_candidate_asset(condition_id, &asset.token_id.0)?;
            candidate.bids.clear();
            candidate.asks.clear();
            self.asset_replacements
                .insert((condition_id.clone(), asset.token_id.0.clone()), candidate);
        }
        self.matching_closed_market_insertions
            .insert(condition_id.clone(), market.market().window.end_at_utc_ms);
        Ok(())
    }

    fn stage_force_release_market(
        &mut self,
        market: &PlatformMarketV1,
    ) -> Result<(), MatchingStateErrorV1> {
        let condition_id = &market.market().condition_id.0;
        if self.candidate_matching_closed(condition_id) {
            return Ok(());
        }
        for asset in market.assets() {
            let mut candidate = self.take_candidate_asset(condition_id, &asset.token_id.0)?;
            candidate.bids.clear();
            candidate.asks.clear();
            self.asset_replacements
                .insert((condition_id.clone(), asset.token_id.0.clone()), candidate);
        }
        self.matching_closed_market_insertions
            .insert(condition_id.clone(), market.market().window.end_at_utc_ms);
        Ok(())
    }

    fn stage_maker_queue_depletion(
        &mut self,
        bucket_at_utc_ms: i64,
        depletion: &MakerQueueDepletionV1,
    ) -> Result<Option<String>, MatchingStateErrorV1> {
        let snapshot = self
            .candidate_order(&depletion.platform_order_id)
            .cloned()
            .ok_or_else(|| {
                MatchingStateErrorV1::AccountOrderConflict(depletion.platform_order_id.clone())
            })?;
        let PreparedMakerQueueDepletionCoreV2 {
            order,
            fill,
            completed_gtd_deadline,
        } = prepare_maker_queue_depletion_core_v2(snapshot, bucket_at_utc_ms, depletion)?;
        if fill
            .as_ref()
            .is_some_and(|fill| self.candidate_fill(&fill.trade_id).is_some())
        {
            return Err(MatchingStateErrorV1::AccountOrderConflict(
                fill.expect("重复检查已证明存在成交").trade_id,
            ));
        }
        let level_key = ExecutionTouchedBidLevelKeyV2 {
            condition_id: order.condition_id.clone(),
            token_id: order.token_id.clone(),
            price_pusd_per_share: order.source_price_pusd_per_share,
        };
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
        let next = current
            .checked_sub(decimal_from_micro_units_v1(depletion.base_units)?)
            .filter(|value| !value.is_sign_negative())
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        self.gtd.overlay_after_values.insert(level_key, next);
        if let Some(deadline) = completed_gtd_deadline {
            self.gtd.deadline_removals.insert(deadline);
        }
        self.order_replacements
            .insert(order.platform_order_id.clone(), order);
        let Some(fill) = fill else {
            return Ok(None);
        };
        let trade_id = fill.trade_id.clone();
        self.finality
            .fill_replacements
            .insert(trade_id.clone(), fill);
        Ok(Some(trade_id))
    }

    fn stage_maker_fill_finality(
        &mut self,
        finality_policy: &TradeFinalityPolicyV1,
        trade_id: &str,
    ) -> Result<TradeFinalityLifecycleEffectsV1, MatchingStateErrorV1> {
        let mut fill = self
            .candidate_fill(trade_id)
            .cloned()
            .ok_or_else(|| MatchingStateErrorV1::Finality(format!("未知成交：{trade_id}")))?;
        let mut order = self
            .candidate_order(&fill.platform_order_id)
            .cloned()
            .ok_or_else(|| {
                MatchingStateErrorV1::AccountOrderConflict(fill.platform_order_id.clone())
            })?;
        let path = prepare_initial_finality_path_v2(
            self.candidate_account().account_instance_key(),
            finality_policy,
            &order,
            &fill,
        )?;
        self.finality.rebate.stage_maker_fill(
            self.live.rebate.as_ref(),
            &fill.trade_id,
            &fill.condition_id,
            fill.matched_at_utc_ms,
            fill.base_units,
            fill.price_pusd_per_share,
        )?;
        if self.candidate_account.is_none() {
            self.candidate_account = Some(self.live.account.clone());
        }
        let matched_at_utc_ms = fill.matched_at_utc_ms;
        let trade_feedback = apply_finality_transition_components_v2(
            self.candidate_account
                .as_mut()
                .expect("maker 最终性初始化必须已有候选账户"),
            &mut order,
            &mut fill,
            &self.live.scheduled_finality,
            &mut self.finality.schedule_insertions,
            path,
            0,
            matched_at_utc_ms,
            true,
        )?;
        self.order_replacements
            .insert(order.platform_order_id.clone(), order);
        self.finality
            .fill_replacements
            .insert(fill.trade_id.clone(), fill);
        Ok(TradeFinalityLifecycleEffectsV1 {
            trade_feedback,
            settlement_feedbacks: Vec::new(),
        })
    }

    fn stage_venue_dust_terminal(
        &mut self,
        candidate: &VenueDustQueueCandidateV1,
        platform_event_at_utc_ms: i64,
        trigger_identity: &str,
        lifecycle_account_rule: &LifecycleAccountRuleV1,
    ) -> Result<Option<VenueDustEffectV1>, MatchingStateErrorV1> {
        let snapshot = self
            .candidate_order(&candidate.platform_order_id)
            .cloned()
            .ok_or_else(|| {
                MatchingStateErrorV1::AccountOrderConflict(candidate.platform_order_id.clone())
            })?;
        let mut candidate_asset =
            self.take_candidate_asset(&snapshot.condition_id, &snapshot.token_id)?;
        if candidate.price_pusd_per_share != snapshot.source_price_pusd_per_share
            || candidate.remaining_base_units != snapshot.open_base_units
            || candidate_asset
                .venue_dust_candidate_for_order(
                    snapshot.source_price_pusd_per_share,
                    &candidate.platform_order_id,
                )
                .as_ref()
                != Some(candidate)
        {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        let PreparedVenueDustCoreV2 {
            candidate_account,
            order,
            effect,
        } = prepare_venue_dust_core_v2(
            self.candidate_account().clone(),
            snapshot,
            candidate,
            platform_event_at_utc_ms,
            trigger_identity,
            venue_dust_upper_exclusive_base_units_v2(lifecycle_account_rule)?,
        )?;
        let Some(effect) = effect else {
            self.asset_replacements.insert(
                (order.condition_id.clone(), order.token_id.clone()),
                candidate_asset,
            );
            return Ok(None);
        };
        candidate_asset.remove_account_order(&candidate.platform_order_id);
        self.asset_replacements.insert(
            (order.condition_id.clone(), order.token_id.clone()),
            candidate_asset,
        );
        let level_key = ExecutionTouchedBidLevelKeyV2 {
            condition_id: order.condition_id.clone(),
            token_id: order.token_id.clone(),
            price_pusd_per_share: order.source_price_pusd_per_share,
        };
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
        let next = current
            .checked_sub(decimal_from_micro_units_v1(effect.closed_base_units)?)
            .filter(|value| !value.is_sign_negative())
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        self.gtd.overlay_after_values.insert(level_key, next);
        if let SimOrderKindStateV1::Gtd { expires_at_utc_ms } = order.kind {
            self.gtd
                .deadline_removals
                .insert((expires_at_utc_ms, order.platform_order_id.clone()));
        }
        self.order_replacements
            .insert(order.platform_order_id.clone(), order);
        self.candidate_account = Some(candidate_account);
        Ok(Some(effect))
    }

    fn stage_market_resolution_order_terminal(
        &mut self,
        platform_order_id: &str,
        resolution_event_id: &str,
        platform_effective_at_utc_ms: i64,
    ) -> Result<MarketResolutionOrderEffectV1, MatchingStateErrorV1> {
        let snapshot = self
            .candidate_order(platform_order_id)
            .cloned()
            .ok_or_else(|| {
                MatchingStateErrorV1::AccountOrderConflict(platform_order_id.to_owned())
            })?;
        let open_base_units = snapshot.open_base_units;
        let source_price_pusd_per_share = snapshot.source_price_pusd_per_share;
        let kind = snapshot.kind;
        let PreparedMarketResolutionOrderCoreV2 {
            candidate_account,
            order,
            effect,
        } = prepare_market_resolution_order_core_v2(
            self.candidate_account().clone(),
            snapshot,
            resolution_event_id,
            platform_effective_at_utc_ms,
        )?;
        if open_base_units > 0 {
            let level_key = ExecutionTouchedBidLevelKeyV2 {
                condition_id: order.condition_id.clone(),
                token_id: order.token_id.clone(),
                price_pusd_per_share: source_price_pusd_per_share,
            };
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
            let next = current
                .checked_sub(decimal_from_micro_units_v1(open_base_units)?)
                .filter(|value| !value.is_sign_negative())
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            self.gtd.overlay_after_values.insert(level_key, next);
        }
        if let SimOrderKindStateV1::Gtd { expires_at_utc_ms } = kind {
            self.gtd
                .deadline_removals
                .insert((expires_at_utc_ms, platform_order_id.to_owned()));
        }
        self.order_replacements
            .insert(order.platform_order_id.clone(), order);
        self.candidate_account = Some(candidate_account);
        Ok(effect)
    }

    fn stage_qualified_market_resolution(
        &mut self,
        directory: &SessionMarketDirectoryV1,
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
        if let Some(existing) = self
            .finality
            .resolution
            .candidate_obligation(self.live.market_resolutions.as_ref(), condition_id)
        {
            if existing.resolution == *resolution {
                return Ok((Vec::new(), None));
            }
            return Err(MatchingStateErrorV1::ResolutionConflict(
                condition_id.to_owned(),
            ));
        }
        let market = directory
            .market(condition_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownMarket(condition_id.to_owned()))?;
        self.stage_force_release_market(market)?;
        let active_order_ids = self
            .candidate_orders_for_condition(condition_id)
            .filter(|order| {
                order.pending_maker_depletion_base_units > 0 || order.open_base_units > 0
            })
            .map(|order| order.platform_order_id.clone())
            .collect::<Vec<_>>();
        let pending_trade_ids = self
            .candidate_fills_for_condition(condition_id)
            .filter(|fill| {
                !matches!(
                    fill.finality_state,
                    Some(TradeFinalityStateV1::Confirmed | TradeFinalityStateV1::Failed)
                )
            })
            .map(|fill| fill.trade_id.clone())
            .collect::<BTreeSet<_>>();
        let mut order_effects = Vec::with_capacity(active_order_ids.len());
        for platform_order_id in active_order_ids {
            order_effects.push(self.stage_market_resolution_order_terminal(
                &platform_order_id,
                &event.event_id.0,
                platform_effective_at_utc_ms,
            )?);
        }
        let inserted = self.finality.resolution.stage_obligation_insertion(
            self.live.market_resolutions.as_ref(),
            condition_id,
            MarketResolutionObligationV1 {
                resolution_event_id: event.event_id.0.clone(),
                resolution_source_event_at_utc_ms: source_event_at_utc_ms,
                resolution: resolution.clone(),
                pending_trade_ids,
                redeemed_at_utc_ms: None,
                invalidated_by_quality_reason: None,
            },
        )?;
        debug_assert!(inserted);
        if self.candidate_account.is_none() {
            self.candidate_account = Some(self.live.account.clone());
        }
        let auto_redeem = self.finality.resolution.stage_auto_redeem(
            self.live.market_resolutions.as_ref(),
            self.live.rebate.as_ref(),
            &self.finality.rebate,
            self.candidate_account
                .as_mut()
                .expect("合格判定自动赎回必须已有候选账户"),
            condition_id,
            platform_effective_at_utc_ms,
        )?;
        Ok((order_effects, auto_redeem))
    }

    pub(crate) fn prepare_authorized_market_events_v2(
        self,
        directory: &SessionMarketDirectoryV1,
        bucket_at_utc_ms: i64,
        events: &[ExecutionMarketEventV3],
        finality_policy: &TradeFinalityPolicyV1,
        lifecycle_account_rule: &LifecycleAccountRuleV1,
        qualified_resolution_event_ids: &BTreeSet<String>,
    ) -> Result<(Self, AuthorizedMarketEventEffectsV1), MatchingStateErrorV1> {
        let mut prepared =
            self.prepare_authorized_market_bucket_start_v2(directory, bucket_at_utc_ms)?;
        let mut effects = AuthorizedMarketEventEffectsV1::default();
        for event in events {
            let (next, event_effects) = prepared.prepare_authorized_market_event_v2(
                directory,
                bucket_at_utc_ms,
                event,
                finality_policy,
                lifecycle_account_rule,
                qualified_resolution_event_ids.contains(&event.event_id.0),
            )?;
            prepared = next;
            effects.append(event_effects);
        }
        Ok((prepared, effects))
    }

    pub(crate) fn prepare_authorized_market_bucket_start_v2(
        mut self,
        directory: &SessionMarketDirectoryV1,
        bucket_at_utc_ms: i64,
    ) -> Result<Self, MatchingStateErrorV1> {
        for market in directory.markets() {
            if bucket_at_utc_ms >= market.market().window.end_at_utc_ms {
                self.stage_release_initialized_market(market)?;
            }
        }
        Ok(self)
    }

    pub(crate) fn prepare_authorized_market_event_v2(
        mut self,
        directory: &SessionMarketDirectoryV1,
        bucket_at_utc_ms: i64,
        event: &ExecutionMarketEventV3,
        finality_policy: &TradeFinalityPolicyV1,
        lifecycle_account_rule: &LifecycleAccountRuleV1,
        qualified_resolution: bool,
    ) -> Result<(Self, AuthorizedMarketEventEffectsV1), MatchingStateErrorV1> {
        let mut effects = AuthorizedMarketEventEffectsV1::default();
        if let ExecutionMarketEventPayloadV3::QualityBoundary(boundary) = &event.payload
            && matches!(boundary.scope, QualityScopeV1::Market)
            && boundary.current_quality == MarketDataQuality::Invalid
        {
            self.finality.resolution.stage_quality_invalidation(
                self.live.market_resolutions.as_ref(),
                &event.market.condition_id.0,
                &boundary.reason_code,
            );
        }
        let queue_effects = self.stage_queue_event(event)?;
        let mut event_fill_ids = Vec::new();
        for depletion in &queue_effects.maker_depletions {
            if let Some(trade_id) = self.stage_maker_queue_depletion(bucket_at_utc_ms, depletion)? {
                event_fill_ids.push(trade_id);
            }
        }
        event_fill_ids.sort();
        for trade_id in event_fill_ids {
            let effect = self.stage_maker_fill_finality(finality_policy, &trade_id)?;
            effects.trade_finality_effects.push(effect.clone());
            effects
                .ordered_lifecycle_effects
                .push(AuthorizedMarketLifecycleEffectV2::TradeFinality(effect));
        }
        if let Some(candidate) = queue_effects.venue_dust_candidate
            && let Some(effect) = self.stage_venue_dust_terminal(
                &candidate,
                bucket_at_utc_ms,
                &format!("DPT_EVENT:{}", event.event_id.0),
                lifecycle_account_rule,
            )?
        {
            effects.venue_dust_effects.push(effect.clone());
            effects
                .ordered_lifecycle_effects
                .push(AuthorizedMarketLifecycleEffectV2::VenueDust(effect));
        }
        if qualified_resolution {
            let (order_effects, auto_redeem) =
                self.stage_qualified_market_resolution(directory, event, bucket_at_utc_ms)?;
            for effect in order_effects {
                effects.market_resolution_order_effects.push(effect.clone());
                effects.ordered_lifecycle_effects.push(
                    AuthorizedMarketLifecycleEffectV2::MarketResolutionOrder(effect),
                );
            }
            if let Some(auto_redeem) = auto_redeem {
                effects.settlement_feedbacks.push(auto_redeem.clone());
                effects
                    .ordered_lifecycle_effects
                    .push(AuthorizedMarketLifecycleEffectV2::Settlement(auto_redeem));
            }
        }
        if bucket_at_utc_ms >= event.market.window.end_at_utc_ms {
            let market = directory
                .market(&event.market.condition_id.0)
                .ok_or_else(|| {
                    MatchingStateErrorV1::UnknownMarket(event.market.condition_id.0.clone())
                })?;
            self.stage_release_initialized_market(market)?;
        }
        Ok((self, effects))
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
            asset_replacements: self.asset_replacements,
            order_replacements: self.order_replacements,
            candidate_account: self.candidate_account,
            matching_closed_market_insertions: self.matching_closed_market_insertions,
        })
    }
}

impl CommittableExecutionDriveDeltaV2<'_> {
    pub(crate) fn commit(self) {
        let Self {
            live,
            gtd,
            finality,
            asset_replacements,
            order_replacements,
            candidate_account,
            matching_closed_market_insertions,
        } = self;
        gtd.commit_prevalidated(live);
        for ((condition_id, token_id), asset) in asset_replacements {
            *live
                .queues
                .asset_mut(&condition_id, &token_id)
                .expect("独占准备期间新订单目标资产必须仍然存在") = asset;
        }
        finality.commit_prevalidated(live);
        for (platform_order_id, order) in order_replacements {
            live.orders.insert(platform_order_id, order);
        }
        if let Some(account) = candidate_account {
            live.account = account;
        }
        live.matching_closed_markets
            .extend(matching_closed_market_insertions);
    }
}

pub(crate) struct PreparedExecutionDriveNewOrderBuilderV2<'a> {
    execution: PreparedExecutionDriveDeltaV2<'a>,
    condition_id: String,
    token_id: String,
    maximum_price: Decimal,
    target: TakerTargetV1,
    candidate_asset: AssetQueueV1,
    plan: QueueMatchPlanV1,
}

pub(crate) struct PreparedNewOrderExecutionEffectsV2 {
    order: SimOrderRecordV1,
    fills: Vec<SimFillRecordV1>,
    order_commit_effects: OrderCommitEffectsV1,
    trade_finality_effects: Vec<TradeFinalityLifecycleEffectsV1>,
    venue_dust_effect: Option<VenueDustEffectV1>,
}

impl PreparedNewOrderExecutionEffectsV2 {
    pub(crate) fn order(&self) -> &SimOrderRecordV1 {
        &self.order
    }

    pub(crate) fn fills(&self) -> &[SimFillRecordV1] {
        &self.fills
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
}

impl<'a> PreparedExecutionDriveDeltaV2<'a> {
    pub(crate) fn prepare_new_order_taker_buy_by_quote_v2(
        self,
        condition_id: &str,
        token_id: &str,
        worst_price: Decimal,
        quote_budget_units: u128,
    ) -> Result<PreparedExecutionDriveNewOrderBuilderV2<'a>, MatchingStateErrorV1> {
        self.prepare_new_order_taker_buy_v2(
            condition_id,
            token_id,
            worst_price,
            TakerTargetV1::Quote(quote_budget_units),
        )
    }

    pub(crate) fn prepare_new_order_taker_buy_by_base_v2(
        self,
        condition_id: &str,
        token_id: &str,
        limit_price: Decimal,
        base_target_units: u128,
        quote_budget_units: u128,
    ) -> Result<PreparedExecutionDriveNewOrderBuilderV2<'a>, MatchingStateErrorV1> {
        self.prepare_new_order_taker_buy_v2(
            condition_id,
            token_id,
            limit_price,
            TakerTargetV1::LimitBase {
                total_base_units: base_target_units,
                total_quote_units: quote_budget_units,
            },
        )
    }

    fn prepare_new_order_taker_buy_v2(
        mut self,
        condition_id: &str,
        token_id: &str,
        maximum_price: Decimal,
        target: TakerTargetV1,
    ) -> Result<PreparedExecutionDriveNewOrderBuilderV2<'a>, MatchingStateErrorV1> {
        let asset_key = (condition_id.to_owned(), token_id.to_owned());
        let mut candidate_asset = match self.asset_replacements.remove(&asset_key) {
            Some(asset) => asset,
            None => {
                let mut asset = self.live.queues.asset(condition_id, token_id)?.clone();
                for (key, after) in &self.gtd.bid_queue_after_values {
                    if key.condition_id == condition_id && key.token_id == token_id {
                        if let Some(queue) = after {
                            asset.bids.insert(key.price_pusd_per_share, queue.clone());
                        } else {
                            asset.bids.remove(&key.price_pusd_per_share);
                        }
                    }
                }
                asset
            }
        };
        let plan = candidate_asset.match_taker_buy(maximum_price, target)?;
        Ok(PreparedExecutionDriveNewOrderBuilderV2 {
            execution: self,
            condition_id: condition_id.to_owned(),
            token_id: token_id.to_owned(),
            maximum_price,
            target,
            candidate_asset,
            plan,
        })
    }
}

impl<'a> PreparedExecutionDriveNewOrderBuilderV2<'a> {
    pub(crate) fn plan(&self) -> &QueueMatchPlanV1 {
        &self.plan
    }

    pub(crate) fn candidate_available_pusd_units(&self) -> u128 {
        self.execution.candidate_account().available_pusd_units()
    }

    pub(crate) fn into_execution_drive(self) -> PreparedExecutionDriveDeltaV2<'a> {
        self.execution
    }

    pub(crate) fn finish_new_order(
        self,
        order: SimOrderRecordV1,
        fills: Vec<SimFillRecordV1>,
        finality_policy: &TradeFinalityPolicyV1,
        lifecycle_account_rule: &LifecycleAccountRuleV1,
    ) -> Result<
        (
            PreparedExecutionDriveDeltaV2<'a>,
            PreparedNewOrderExecutionEffectsV2,
        ),
        MatchingStateErrorV1,
    > {
        let Self {
            mut execution,
            condition_id,
            token_id,
            maximum_price,
            target,
            mut candidate_asset,
            plan,
        } = self;
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
            || execution
                .candidate_order(&order.platform_order_id)
                .is_some()
            || fills
                .iter()
                .any(|fill| execution.candidate_fill(&fill.trade_id).is_some())
        {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }

        let PreparedNewOrderCoreV2 {
            mut candidate_account,
            mut order,
            fills: mut candidate_fills,
            effects: order_commit_effects,
        } = prepare_new_order_core_v2(
            execution.candidate_account(),
            &execution.live.orders,
            &execution.live.fills,
            order,
            fills,
        )?;

        if order.open_base_units > 0 {
            if execution
                .live
                .queues
                .contains_account_order(&order.platform_order_id)
            {
                return Err(MatchingStateErrorV1::AccountOrderConflict(
                    order.platform_order_id,
                ));
            }
            candidate_asset.insert_account_buy_remainder(
                order.source_price_pusd_per_share,
                order.platform_order_id.clone(),
                order.open_base_units,
            )?;
            let level_key = ExecutionTouchedBidLevelKeyV2 {
                condition_id: order.condition_id.clone(),
                token_id: order.token_id.clone(),
                price_pusd_per_share: order.source_price_pusd_per_share,
            };
            let current = execution
                .gtd
                .overlay_after_values
                .get(&level_key)
                .copied()
                .unwrap_or_else(|| {
                    execution.live.account_overlay.quantity(
                        &level_key.condition_id,
                        &level_key.token_id,
                        BookSide::Bid,
                        level_key.price_pusd_per_share,
                    )
                });
            let next = current
                .checked_add(decimal_from_micro_units_v1(order.open_base_units)?)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            execution.gtd.overlay_after_values.insert(level_key, next);
        }

        let mut gtd_deadline_to_insert = match order.kind {
            SimOrderKindStateV1::Gtd { expires_at_utc_ms } if order.open_base_units > 0 => {
                let key = (expires_at_utc_ms, order.platform_order_id.clone());
                let live_candidate_contains = execution.live.gtd_deadlines.contains(&key)
                    && !execution.gtd.deadline_removals.contains(&key);
                if live_candidate_contains || execution.gtd.deadline_insertions.contains(&key) {
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
        for index in &fill_indexes {
            let fill = &candidate_fills[*index];
            if fill.immediate_taker_fee_units > 0 {
                execution.finality.rebate.stage_taker_fill(
                    execution.live.rebate.as_ref(),
                    &fill.trade_id,
                    &fill.condition_id,
                    fill.matched_at_utc_ms,
                    decimal_from_micro_units_v1(fill.immediate_taker_fee_units)?,
                )?;
            }
        }

        let mut trade_finality_effects = Vec::with_capacity(fill_indexes.len());
        for (index, path) in fill_indexes.into_iter().zip(finality_paths) {
            let platform_event_at_utc_ms = candidate_fills[index].matched_at_utc_ms;
            let trade_feedback = apply_finality_transition_components_v2(
                &mut candidate_account,
                &mut order,
                &mut candidate_fills[index],
                &execution.live.scheduled_finality,
                &mut execution.finality.schedule_insertions,
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
            if let Some(candidate) = candidate_asset.venue_dust_candidate_for_order(
                order.source_price_pusd_per_share,
                &order.platform_order_id,
            ) {
                let upper_exclusive_base_units =
                    venue_dust_upper_exclusive_base_units_v2(lifecycle_account_rule)?;
                let order_action_at_utc_ms = order.created_at_utc_ms;
                let prepared_dust = prepare_venue_dust_core_v2(
                    candidate_account,
                    order,
                    &candidate,
                    order_action_at_utc_ms,
                    &format!("ORDER_ACTION:{}", candidate.platform_order_id),
                    upper_exclusive_base_units,
                )?;
                candidate_account = prepared_dust.candidate_account;
                order = prepared_dust.order;
                if let Some(effect) = prepared_dust.effect {
                    candidate_asset.remove_account_order(&candidate.platform_order_id);
                    let level_key = ExecutionTouchedBidLevelKeyV2 {
                        condition_id: effect.condition_id.clone(),
                        token_id: effect.token_id.clone(),
                        price_pusd_per_share: candidate.price_pusd_per_share,
                    };
                    let current = execution
                        .gtd
                        .overlay_after_values
                        .get(&level_key)
                        .copied()
                        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                    let next = current
                        .checked_sub(decimal_from_micro_units_v1(effect.closed_base_units)?)
                        .filter(|value| !value.is_sign_negative())
                        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
                    execution.gtd.overlay_after_values.insert(level_key, next);
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

        if let Some(deadline) = gtd_deadline_to_insert {
            execution.gtd.deadline_insertions.insert(deadline);
        }
        execution
            .asset_replacements
            .insert((condition_id, token_id), candidate_asset);
        for fill in &candidate_fills {
            execution
                .finality
                .fill_replacements
                .insert(fill.trade_id.clone(), fill.clone());
        }
        execution
            .order_replacements
            .insert(order.platform_order_id.clone(), order.clone());
        execution.candidate_account = Some(candidate_account);

        Ok((
            execution,
            PreparedNewOrderExecutionEffectsV2 {
                order,
                fills: candidate_fills,
                order_commit_effects,
                trade_finality_effects,
                venue_dust_effect,
            },
        ))
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

struct PreparedMarketResolutionOrderCoreV2 {
    candidate_account: PlatformAccountStateV1,
    order: SimOrderRecordV1,
    effect: MarketResolutionOrderEffectV1,
}

fn prepare_market_resolution_order_core_v2(
    mut candidate_account: PlatformAccountStateV1,
    mut order: SimOrderRecordV1,
    resolution_event_id: &str,
    platform_effective_at_utc_ms: i64,
) -> Result<PreparedMarketResolutionOrderCoreV2, MatchingStateErrorV1> {
    let active_base_units = order
        .pending_maker_depletion_base_units
        .checked_add(order.open_base_units)
        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
    let accepted_base_units = order
        .accepted_base_units
        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
    if active_base_units == 0
        || order.open_quote_units == 0
        || !matches!(
            order.kind,
            SimOrderKindStateV1::Gtc | SimOrderKindStateV1::Gtd { .. }
        )
        || order.terminal_reason.is_some()
        || order.expired_base_units != 0
        || order.venue_dust_closed_base_units != 0
        || order.market_resolved_canceled_base_units != 0
        || order.canceled_base_units != 0
        || accepted_base_units
            != order
                .matched_base_units
                .checked_add(active_base_units)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
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
        return Err(MatchingStateErrorV1::QueuePredecessorConflict);
    }
    let effect_identity = market_resolution_order_identity(
        resolution_event_id,
        &order.platform_order_id,
        order.pending_maker_depletion_base_units,
        order.open_base_units,
        order.open_quote_units,
    );
    let account_delta = candidate_account.apply_market_resolution_encumbrance_release(
        &effect_identity.0,
        &order.condition_id,
        platform_effective_at_utc_ms,
        decimal_from_micro_units_v1(order.open_quote_units)?,
    )?;
    let effect = MarketResolutionOrderEffectV1 {
        platform_order_id: order.platform_order_id.clone(),
        condition_id: order.condition_id.clone(),
        token_id: order.token_id.clone(),
        canceled_base_units: active_base_units,
        released_open_quote_units: order.open_quote_units,
        account_delta,
    };
    order.market_resolved_canceled_base_units = active_base_units;
    order.market_resolved_released_quote_units = order.open_quote_units;
    order.market_resolved_at_utc_ms = Some(platform_effective_at_utc_ms);
    order.market_resolution_effect_identity = Some(effect_identity);
    order.terminalized_pending_maker_depletion_evidence =
        order.pending_maker_depletion_evidence.take();
    order.pending_maker_depletion_base_units = 0;
    order.open_base_units = 0;
    order.open_quote_units = 0;
    order.last_updated_at_utc_ms = platform_effective_at_utc_ms;
    order.terminal_reason = Some(SimOrderTerminalReasonV1::MarketResolvedCanceled);
    Ok(PreparedMarketResolutionOrderCoreV2 {
        candidate_account,
        order,
        effect,
    })
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
        let open_base_units = snapshot.open_base_units;
        let kind = snapshot.kind;
        let PreparedMarketResolutionOrderCoreV2 {
            candidate_account,
            order,
            effect,
        } = prepare_market_resolution_order_core_v2(
            self.account.clone(),
            snapshot,
            resolution_event_id,
            platform_effective_at_utc_ms,
        )?;
        if open_base_units > 0 {
            self.account_overlay
                .subtract_quantity(
                    &order.condition_id,
                    &order.token_id,
                    BookSide::Bid,
                    order.source_price_pusd_per_share,
                    decimal_from_micro_units_v1(open_base_units)?,
                )
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        }
        if let SimOrderKindStateV1::Gtd { expires_at_utc_ms } = kind {
            self.gtd_deadlines
                .remove(&(expires_at_utc_ms, platform_order_id.to_owned()));
        }
        self.orders.insert(order.platform_order_id.clone(), order);
        self.account = candidate_account;
        Ok(effect)
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
                let effect = self.initialize_fill_finality(finality_policy, &trade_id)?;
                effects.trade_finality_effects.push(effect.clone());
                effects
                    .ordered_lifecycle_effects
                    .push(AuthorizedMarketLifecycleEffectV2::TradeFinality(effect));
            }
            if let Some(candidate) = queue_effects.venue_dust_candidate
                && let Some(effect) = self.apply_venue_dust_terminal(
                    candidate,
                    bucket_at_utc_ms,
                    &format!("DPT_EVENT:{}", event.event_id.0),
                    lifecycle_account_rule,
                )?
            {
                effects.venue_dust_effects.push(effect.clone());
                effects
                    .ordered_lifecycle_effects
                    .push(AuthorizedMarketLifecycleEffectV2::VenueDust(effect));
            }
            if qualified_resolution_event_ids.contains(&event.event_id.0) {
                let (order_effects, auto_redeem) =
                    self.apply_qualified_market_resolution(event, bucket_at_utc_ms)?;
                for effect in order_effects {
                    effects.market_resolution_order_effects.push(effect.clone());
                    effects.ordered_lifecycle_effects.push(
                        AuthorizedMarketLifecycleEffectV2::MarketResolutionOrder(effect),
                    );
                }
                if let Some(auto_redeem) = auto_redeem {
                    effects.settlement_feedbacks.push(auto_redeem.clone());
                    effects
                        .ordered_lifecycle_effects
                        .push(AuthorizedMarketLifecycleEffectV2::Settlement(auto_redeem));
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
        let snapshot = self
            .orders
            .get(&depletion.platform_order_id)
            .cloned()
            .ok_or_else(|| {
                MatchingStateErrorV1::AccountOrderConflict(depletion.platform_order_id.clone())
            })?;
        let PreparedMakerQueueDepletionCoreV2 {
            order,
            fill,
            completed_gtd_deadline,
        } = prepare_maker_queue_depletion_core_v2(snapshot, bucket_at_utc_ms, &depletion)?;
        if let Some(fill) = &fill
            && self.fills.contains_key(&fill.trade_id)
        {
            return Err(MatchingStateErrorV1::AccountOrderConflict(
                fill.trade_id.clone(),
            ));
        }
        self.account_overlay
            .subtract_quantity(
                &order.condition_id,
                &order.token_id,
                BookSide::Bid,
                order.source_price_pusd_per_share,
                decimal_from_micro_units_v1(depletion.base_units)?,
            )
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let trade_id = fill.as_ref().map(|fill| fill.trade_id.clone());
        if let Some(fill) = fill {
            self.fills.insert(fill.trade_id.clone(), fill);
        }
        if let Some(deadline) = completed_gtd_deadline {
            self.gtd_deadlines.remove(&deadline);
        }
        self.orders.insert(order.platform_order_id.clone(), order);
        Ok(trade_id)
    }
}

struct PreparedMakerQueueDepletionCoreV2 {
    order: SimOrderRecordV1,
    fill: Option<SimFillRecordV1>,
    completed_gtd_deadline: Option<(i64, String)>,
}

fn prepare_maker_queue_depletion_core_v2(
    mut order: SimOrderRecordV1,
    bucket_at_utc_ms: i64,
    depletion: &MakerQueueDepletionV1,
) -> Result<PreparedMakerQueueDepletionCoreV2, MatchingStateErrorV1> {
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
    order.open_base_units = next_open_base;
    order.last_updated_at_utc_ms = bucket_at_utc_ms;
    if quote_delta == 0 {
        order.pending_maker_depletion_base_units = order
            .pending_maker_depletion_base_units
            .checked_add(depletion.base_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        order.pending_maker_depletion_evidence = Some(evidence);
        return Ok(PreparedMakerQueueDepletionCoreV2 {
            order,
            fill: None,
            completed_gtd_deadline: None,
        });
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
    let fill = SimFillRecordV1 {
        trade_id,
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
    };
    Ok(PreparedMakerQueueDepletionCoreV2 {
        order,
        fill: Some(fill),
        completed_gtd_deadline,
    })
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

fn allocate_taker_fee_micro_units_by_key<K: Ord>(
    fills: &[(K, u128, Decimal)],
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
            .then_with(|| fills[*left].0.cmp(&fills[*right].0))
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

pub(crate) fn allocate_taker_fee_micro_units_v1(
    fills: &[(&str, u128, Decimal)],
    rate: Decimal,
    exponent: u32,
) -> Result<(u128, Vec<u128>), MatchingMathErrorV1> {
    allocate_taker_fee_micro_units_by_key(fills, rate, exponent)
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
            let mut prepared = state
                .prepare_execution_drive_delta_v2(2_500)
                .expect("准备全局候选迭代器真实最终性 after-image");
            let mut inserted_order = prepared
                .candidate_orders()
                .next()
                .expect("候选订单夹具必须非空")
                .clone();
            inserted_order.platform_order_id = "!prepared-inserted-order".to_owned();
            let inserted_order_id = inserted_order.platform_order_id.clone();
            let inserted_order_condition_id = inserted_order.condition_id.clone();
            assert!(
                prepared
                    .order_replacements
                    .insert(inserted_order_id.clone(), inserted_order)
                    .is_none()
            );
            let mut inserted_fill = prepared
                .candidate_fills()
                .next()
                .expect("候选成交夹具必须非空")
                .clone();
            inserted_fill.trade_id = "!prepared-inserted-fill".to_owned();
            let inserted_trade_id = inserted_fill.trade_id.clone();
            let inserted_fill_condition_id = inserted_fill.condition_id.clone();
            assert!(
                prepared
                    .finality
                    .fill_replacements
                    .insert(inserted_trade_id.clone(), inserted_fill)
                    .is_none()
            );
            let candidate_orders = prepared.candidate_orders().collect::<Vec<_>>();
            let candidate_fills = prepared.candidate_fills().collect::<Vec<_>>();

            let mut expected_order_ids = live_order_ids.clone();
            expected_order_ids.push(inserted_order_id.clone());
            expected_order_ids.sort();
            assert_eq!(candidate_orders.len(), expected_order_ids.len());
            assert_eq!(
                candidate_orders
                    .iter()
                    .map(|order| order.platform_order_id.clone())
                    .collect::<Vec<_>>(),
                expected_order_ids
            );
            let mut expected_trade_ids = live_trade_ids.clone();
            expected_trade_ids.push(inserted_trade_id.clone());
            expected_trade_ids.sort();
            assert_eq!(candidate_fills.len(), expected_trade_ids.len());
            assert_eq!(
                candidate_fills
                    .iter()
                    .map(|fill| fill.trade_id.clone())
                    .collect::<Vec<_>>(),
                expected_trade_ids
            );
            assert_eq!(
                prepared
                    .candidate_orders_for_condition(&inserted_order_condition_id)
                    .map(|order| order.platform_order_id.clone())
                    .collect::<Vec<_>>(),
                candidate_orders
                    .iter()
                    .filter(|order| order.condition_id == inserted_order_condition_id)
                    .map(|order| order.platform_order_id.clone())
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                prepared
                    .candidate_fills_for_condition(&inserted_fill_condition_id)
                    .map(|fill| fill.trade_id.clone())
                    .collect::<Vec<_>>(),
                candidate_fills
                    .iter()
                    .filter(|fill| fill.condition_id == inserted_fill_condition_id)
                    .map(|fill| fill.trade_id.clone())
                    .collect::<Vec<_>>()
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

        assert_eq!(state, baseline, "提交前 drop 必须保持 live 零变化");
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
    async fn prepared_execution_drive_consumes_dpt_maker_depletion_and_drop_is_read_only() {
        let (mut state, rules, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let directory =
            SessionMarketDirectoryV1::try_from_scenario(&crate::scenario::scenario_for_sim_tests())
                .expect("DPT 稀疏候选测试目录");
        let platform_order_id = insert_open_gtd_order_for_drive_delta(
            &mut state,
            "dpt-sparse-maker",
            &condition_id,
            &token_id,
            decimal("0.4"),
            1_000_000,
            400_000,
            1_900,
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
            .expect("DPT 稀疏候选测试先形成账户 maker 后方外部段");
        let depletion = crate::task23_tests::bid_transition(1_101, 1_201, 2, "0.4", "1", "0");
        let finality_policy = crate::finality::successful_policy_for_tests();
        let lifecycle_rule = &rules
            .snapshot()
            .project_account_rules
            .lifecycle_account_behavior
            .value;

        let mut expected = state.detached_clone();
        expected
            .prepare_execution_drive_delta_v2(1_101)
            .expect("准备 DPT 黄金 scheduled 前缀")
            .into_committable()
            .expect("DPT 黄金 scheduled 前缀必须可提交")
            .commit();
        let expected_effects = expected
            .apply_authorized_market_events(
                &directory,
                1_101,
                std::slice::from_ref(&depletion),
                &finality_policy,
                lifecycle_rule,
                &BTreeSet::new(),
            )
            .expect("形成既有 DPT 黄金终态");
        assert_eq!(expected_effects.trade_finality_effects.len(), 1);
        let baseline = serde_json::to_vec(&state).expect("序列化 DPT 稀疏候选基线");

        reset_execution_state_clone_count_for_tests();
        let (prepared, effects) = state
            .prepare_execution_drive_delta_v2(1_101)
            .expect("准备 DPT 稀疏 scheduled 前缀")
            .prepare_authorized_market_events_v2(
                &directory,
                1_101,
                std::slice::from_ref(&depletion),
                &finality_policy,
                lifecycle_rule,
                &BTreeSet::new(),
            )
            .expect("DPT 稀疏候选必须消费真实 maker 减量");
        assert_eq!(effects, expected_effects);
        assert_eq!(prepared.candidate_fills().count(), 1);
        assert_eq!(
            prepared
                .candidate_order(&platform_order_id)
                .expect("DPT 稀疏候选必须保留 maker 订单")
                .open_base_units,
            0
        );
        drop(prepared);
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化丢弃后的 DPT 实时状态"),
            baseline
        );
        assert_eq!(execution_state_clone_count_for_tests(), 0);

        let (prepared, effects) = state
            .prepare_execution_drive_delta_v2(1_101)
            .expect("重新准备 DPT 稀疏 scheduled 前缀")
            .prepare_authorized_market_events_v2(
                &directory,
                1_101,
                &[depletion],
                &finality_policy,
                lifecycle_rule,
                &BTreeSet::new(),
            )
            .expect("重新准备 DPT 稀疏 maker 减量");
        assert_eq!(effects, expected_effects);
        prepared
            .into_committable()
            .expect("DPT 稀疏候选必须可提交")
            .commit();
        assert_eq!(state, expected);
        assert_eq!(execution_state_clone_count_for_tests(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_resolution_uses_scheduled_after_image_and_drops_atomically() {
        let (mut state, rules, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let directory =
            SessionMarketDirectoryV1::try_from_scenario(&crate::scenario::scenario_for_sim_tests())
                .expect("DPT 判定稀疏候选测试目录");
        let member = directory
            .market(&condition_id)
            .expect("DPT 判定稀疏候选目标市场");
        let bucket_at_utc_ms = member.market().window.end_at_utc_ms;
        let platform_order_id = insert_open_gtd_order_for_drive_delta(
            &mut state,
            "dpt-sparse-resolution",
            &condition_id,
            &token_id,
            decimal("0.4"),
            1_000_000,
            400_000,
            bucket_at_utc_ms + 1_000,
        );
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
            mapping_rule_version: "dpt-sparse-resolution-v1".to_owned(),
        };
        let mut event = crate::task22_tests::sealed_event(
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(resolution),
            crate::task22_tests::orderbook_evidence(bucket_at_utc_ms, bucket_at_utc_ms, 1),
            crate::task21_tests::fresh_quality(),
        );
        event.market = member.market().clone();
        event.event_id = data_pipeline_core::derive_execution_market_event_v3_id(&event)
            .expect("DPT 判定稀疏候选事件身份");
        let qualified = BTreeSet::from([event.event_id.0.clone()]);
        let finality_policy = crate::finality::successful_policy_for_tests();
        let lifecycle_rule = &rules
            .snapshot()
            .project_account_rules
            .lifecycle_account_behavior
            .value;

        let mut expected = state.detached_clone();
        expected
            .prepare_execution_drive_delta_v2(bucket_at_utc_ms)
            .expect("准备 DPT 判定黄金 scheduled 前缀")
            .into_committable()
            .expect("DPT 判定黄金 scheduled 前缀必须可提交")
            .commit();
        let expected_effects = expected
            .apply_authorized_market_events(
                &directory,
                bucket_at_utc_ms,
                std::slice::from_ref(&event),
                &finality_policy,
                lifecycle_rule,
                &qualified,
            )
            .expect("形成既有 DPT 判定黄金终态");
        assert_eq!(expected_effects.market_resolution_order_effects.len(), 1);
        assert_eq!(expected_effects.settlement_feedbacks.len(), 1);
        let baseline = serde_json::to_vec(&state).expect("序列化 DPT 判定稀疏候选基线");

        reset_execution_state_clone_count_for_tests();
        let (prepared, effects) = state
            .prepare_execution_drive_delta_v2(bucket_at_utc_ms)
            .expect("准备 DPT 判定稀疏 scheduled 前缀")
            .prepare_authorized_market_events_v2(
                &directory,
                bucket_at_utc_ms,
                std::slice::from_ref(&event),
                &finality_policy,
                lifecycle_rule,
                &qualified,
            )
            .expect("DPT 判定稀疏候选必须消费合格判定");
        assert_eq!(effects, expected_effects);
        assert_eq!(
            prepared
                .candidate_order(&platform_order_id)
                .expect("DPT 判定稀疏候选必须保留终态订单")
                .terminal_reason,
            Some(SimOrderTerminalReasonV1::MarketResolvedCanceled)
        );
        drop(prepared);
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化丢弃后的 DPT 判定实时状态"),
            baseline
        );
        assert_eq!(execution_state_clone_count_for_tests(), 0);

        let (prepared, effects) = state
            .prepare_execution_drive_delta_v2(bucket_at_utc_ms)
            .expect("重新准备 DPT 判定稀疏 scheduled 前缀")
            .prepare_authorized_market_events_v2(
                &directory,
                bucket_at_utc_ms,
                &[event],
                &finality_policy,
                lifecycle_rule,
                &qualified,
            )
            .expect("重新准备 DPT 判定稀疏候选");
        assert_eq!(effects, expected_effects);
        prepared
            .into_committable()
            .expect("DPT 判定稀疏候选必须在写日志前完成唯一所有权预检")
            .commit();
        assert_eq!(state, expected);
        assert_eq!(execution_state_clone_count_for_tests(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_dpt_venue_dust_matches_legacy_and_drops_atomically() {
        let (mut state, rules, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let directory =
            SessionMarketDirectoryV1::try_from_scenario(&crate::scenario::scenario_for_sim_tests())
                .expect("DPT dust 稀疏候选测试目录");
        let addition = crate::task23_tests::bid_transition(1_100, 1_200, 1, "0.4", "0", "10");
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
            .expect("DPT dust 测试先形成账户订单前方外部段");
        let plan = QueueMatchPlanV1 {
            fills: vec![TakerFillSliceV1 {
                segment_id: Sha256Hex("e".repeat(64)),
                price_pusd_per_share: decimal("0.3"),
                base_units: 4_999_000,
                quote_units: 1_499_700,
            }],
            matched_base_units: 4_999_000,
            used_quote_units: 1_499_700,
        };
        let (order, fills) = new_order_delta_records(
            "dpt-sparse-dust",
            &condition_id,
            &token_id,
            SimOrderKindStateV1::Gtc,
            decimal("0.4"),
            Some(5_000_000),
            1_500_100,
            &plan,
            &[0],
        );
        let platform_order_id = order.platform_order_id.clone();
        state
            .commit_new_order(order, fills)
            .expect("建立 DPT dust 稀疏候选部分成交订单");
        let depletion = crate::task23_tests::bid_transition(1_101, 1_201, 2, "0.4", "10", "0");
        let finality_policy = crate::finality::successful_policy_for_tests();
        let lifecycle_rule = &rules
            .snapshot()
            .project_account_rules
            .lifecycle_account_behavior
            .value;

        let mut expected = state.detached_clone();
        expected
            .prepare_execution_drive_delta_v2(1_101)
            .expect("准备 DPT dust 黄金 scheduled 前缀")
            .into_committable()
            .expect("DPT dust 黄金 scheduled 前缀必须可提交")
            .commit();
        let expected_effects = expected
            .apply_authorized_market_events(
                &directory,
                1_101,
                std::slice::from_ref(&depletion),
                &finality_policy,
                lifecycle_rule,
                &BTreeSet::new(),
            )
            .expect("形成既有 DPT dust 黄金终态");
        assert_eq!(expected_effects.venue_dust_effects.len(), 1);
        assert!(expected_effects.trade_finality_effects.is_empty());
        let baseline = serde_json::to_vec(&state).expect("序列化 DPT dust 稀疏候选基线");

        reset_execution_state_clone_count_for_tests();
        let (prepared, effects) = state
            .prepare_execution_drive_delta_v2(1_101)
            .expect("准备 DPT dust 稀疏 scheduled 前缀")
            .prepare_authorized_market_events_v2(
                &directory,
                1_101,
                std::slice::from_ref(&depletion),
                &finality_policy,
                lifecycle_rule,
                &BTreeSet::new(),
            )
            .expect("DPT dust 稀疏候选必须形成终态");
        assert_eq!(effects, expected_effects);
        assert_eq!(
            prepared
                .candidate_order(&platform_order_id)
                .expect("DPT dust 稀疏候选必须保留订单")
                .terminal_reason,
            Some(SimOrderTerminalReasonV1::VenueDustTerminal)
        );
        drop(prepared);
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化丢弃后的 DPT dust 实时状态"),
            baseline
        );
        assert_eq!(execution_state_clone_count_for_tests(), 0);

        let (prepared, effects) = state
            .prepare_execution_drive_delta_v2(1_101)
            .expect("重新准备 DPT dust 稀疏 scheduled 前缀")
            .prepare_authorized_market_events_v2(
                &directory,
                1_101,
                &[depletion],
                &finality_policy,
                lifecycle_rule,
                &BTreeSet::new(),
            )
            .expect("重新准备 DPT dust 稀疏候选");
        assert_eq!(effects, expected_effects);
        prepared
            .into_committable()
            .expect("DPT dust 稀疏候选必须可提交")
            .commit();
        assert_eq!(state, expected);
        assert_eq!(execution_state_clone_count_for_tests(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_keeps_resolution_quality_order_and_late_conflict_is_atomic() {
        let (mut state, rules, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let directory =
            SessionMarketDirectoryV1::try_from_scenario(&crate::scenario::scenario_for_sim_tests())
                .expect("DPT 原序稀疏候选测试目录");
        let member = directory
            .market(&condition_id)
            .expect("DPT 原序稀疏候选目标市场");
        let bucket_at_utc_ms = member.market().window.end_at_utc_ms;
        let policy = crate::finality::policy_for_tests(
            "dpt-resolution-quality-order-policy",
            &[
                (TradeFinalityStateV1::Matched, 0),
                (TradeFinalityStateV1::Mined, 10),
                (TradeFinalityStateV1::Confirmed, 5_000),
            ],
            1,
            Vec::new(),
        );
        let plan = QueueMatchPlanV1 {
            fills: vec![TakerFillSliceV1 {
                segment_id: Sha256Hex("9".repeat(64)),
                price_pusd_per_share: decimal("0.4"),
                base_units: 1_000_000,
                quote_units: 400_000,
            }],
            matched_base_units: 1_000_000,
            used_quote_units: 400_000,
        };
        let (order, fills) = new_order_delta_records(
            "dpt-resolution-quality-order",
            &condition_id,
            &token_id,
            SimOrderKindStateV1::Fok,
            decimal("0.4"),
            Some(1_000_000),
            400_000,
            &plan,
            &[0],
        );
        let trade_id = fills[0].trade_id.clone();
        state
            .commit_new_order(order, fills)
            .expect("建立 DPT 原序待最终性成交");
        state
            .initialize_fill_finality(&policy, &trade_id)
            .expect("初始化 DPT 原序待最终性成交");

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
            mapping_rule_version: "dpt-resolution-quality-order-v1".to_owned(),
        };
        let mut resolution_event = crate::task22_tests::sealed_event(
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(resolution),
            crate::task22_tests::orderbook_evidence(bucket_at_utc_ms, bucket_at_utc_ms, 1),
            crate::task21_tests::fresh_quality(),
        );
        resolution_event.market = member.market().clone();
        resolution_event.event_id =
            data_pipeline_core::derive_execution_market_event_v3_id(&resolution_event)
                .expect("DPT 原序判定事件身份");
        let mut quality_event = crate::task22_tests::sealed_event(
            ExecutionMarketEventPayloadV3::QualityBoundary(contracts::QualityBoundaryV2 {
                scope: QualityScopeV1::Market,
                previous_quality: MarketDataQuality::Fresh,
                current_quality: MarketDataQuality::Invalid,
                reason_code: "DPT_RESOLUTION_THEN_INVALID".to_owned(),
            }),
            crate::task22_tests::orderbook_evidence(bucket_at_utc_ms, bucket_at_utc_ms, 2),
            contracts::EventQualityV2 {
                availability: MarketDataQuality::Invalid,
                resync: ResyncQuality::NotRequired,
            },
        );
        quality_event.market = member.market().clone();
        quality_event.event_id =
            data_pipeline_core::derive_execution_market_event_v3_id(&quality_event)
                .expect("DPT 原序质量事件身份");
        let qualified = BTreeSet::from([resolution_event.event_id.0.clone()]);
        let lifecycle_rule = &rules
            .snapshot()
            .project_account_rules
            .lifecycle_account_behavior
            .value;
        let baseline = serde_json::to_vec(&state).expect("序列化 DPT 原序候选基线");

        let mut conflicting_resolution = resolution_event.clone();
        let ExecutionMarketEventPayloadV3::MarketResolutionTerminal(
            MarketResolutionTerminalV3::Resolved {
                mapping_rule_version,
                ..
            },
        ) = &mut conflicting_resolution.payload
        else {
            panic!("DPT 原序冲突夹具必须是 resolved 判定");
        };
        *mapping_rule_version = "dpt-resolution-quality-conflict-v1".to_owned();
        conflicting_resolution.event_id =
            data_pipeline_core::derive_execution_market_event_v3_id(&conflicting_resolution)
                .expect("DPT 原序冲突判定事件身份");
        let conflicting_qualified = BTreeSet::from([
            resolution_event.event_id.0.clone(),
            conflicting_resolution.event_id.0.clone(),
        ]);
        reset_execution_state_clone_count_for_tests();
        let error = match state
            .prepare_execution_drive_delta_v2(bucket_at_utc_ms)
            .expect("准备 DPT 原序后项冲突 scheduled 前缀")
            .prepare_authorized_market_events_v2(
                &directory,
                bucket_at_utc_ms,
                &[resolution_event.clone(), conflicting_resolution],
                &policy,
                lifecycle_rule,
                &conflicting_qualified,
            ) {
            Err(error) => error,
            Ok(_) => panic!("DPT 后项判定冲突必须消费并丢弃整个候选"),
        };
        assert_eq!(
            error,
            MatchingStateErrorV1::ResolutionConflict(condition_id.clone())
        );
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化 DPT 原序后项冲突实时状态"),
            baseline
        );
        assert_eq!(execution_state_clone_count_for_tests(), 0);

        let events = [resolution_event, quality_event];
        let mut expected = state.detached_clone();
        expected
            .prepare_execution_drive_delta_v2(bucket_at_utc_ms)
            .expect("准备 DPT 原序黄金 scheduled 前缀")
            .into_committable()
            .expect("DPT 原序黄金 scheduled 前缀必须可提交")
            .commit();
        let expected_effects = expected
            .apply_authorized_market_events(
                &directory,
                bucket_at_utc_ms,
                &events,
                &policy,
                lifecycle_rule,
                &qualified,
            )
            .expect("形成 resolution 后 quality 的既有 DPT 黄金终态");
        assert_eq!(
            expected
                .market_resolutions
                .get(&condition_id)
                .and_then(|obligation| obligation.invalidated_by_quality_reason.as_deref()),
            Some("DPT_RESOLUTION_THEN_INVALID")
        );

        reset_execution_state_clone_count_for_tests();
        let (prepared, effects) = state
            .prepare_execution_drive_delta_v2(bucket_at_utc_ms)
            .expect("准备 DPT resolution 后 quality 稀疏 scheduled 前缀")
            .prepare_authorized_market_events_v2(
                &directory,
                bucket_at_utc_ms,
                &events,
                &policy,
                lifecycle_rule,
                &qualified,
            )
            .expect("DPT 稀疏候选必须保持 resolution 后 quality 原序");
        assert_eq!(effects, expected_effects);
        drop(prepared);
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化丢弃后的 DPT 原序实时状态"),
            baseline
        );
        assert_eq!(execution_state_clone_count_for_tests(), 0);

        let (prepared, effects) = state
            .prepare_execution_drive_delta_v2(bucket_at_utc_ms)
            .expect("重新准备 DPT 原序稀疏 scheduled 前缀")
            .prepare_authorized_market_events_v2(
                &directory,
                bucket_at_utc_ms,
                &events,
                &policy,
                lifecycle_rule,
                &qualified,
            )
            .expect("重新准备 DPT 原序稀疏候选");
        assert_eq!(effects, expected_effects);
        prepared
            .into_committable()
            .expect("DPT 原序稀疏候选必须可提交")
            .commit();
        assert_eq!(state, expected);
        assert_eq!(execution_state_clone_count_for_tests(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_execution_drive_chains_zero_quote_dpt_events_in_input_order() {
        let (mut state, rules, condition_id, token_id) = new_order_delta_state_fixture(0).await;
        let directory =
            SessionMarketDirectoryV1::try_from_scenario(&crate::scenario::scenario_for_sim_tests())
                .expect("DPT 零对价串联测试目录");
        let platform_order_id = insert_open_gtd_order_for_drive_delta(
            &mut state,
            "dpt-zero-quote-chain",
            &condition_id,
            &token_id,
            decimal("0.00001"),
            5_000_000,
            50,
            5_000,
        );
        let events = [
            crate::task23_tests::bid_transition(1_100, 1_200, 1, "0.00001", "0", "0.000001"),
            crate::task23_tests::bid_transition(1_101, 1_201, 2, "0.00001", "0.000001", "0"),
            crate::task23_tests::bid_transition(1_102, 1_202, 3, "0.00001", "0", "0.099999"),
            crate::task23_tests::bid_transition(1_103, 1_203, 4, "0.00001", "0.099999", "0"),
        ];
        let finality_policy = crate::finality::successful_policy_for_tests();
        let lifecycle_rule = &rules
            .snapshot()
            .project_account_rules
            .lifecycle_account_behavior
            .value;
        let mut expected = state.detached_clone();
        expected
            .prepare_execution_drive_delta_v2(1_103)
            .expect("准备 DPT 零对价串联黄金 scheduled 前缀")
            .into_committable()
            .expect("DPT 零对价串联黄金 scheduled 前缀必须可提交")
            .commit();
        let expected_effects = expected
            .apply_authorized_market_events(
                &directory,
                1_103,
                &events,
                &finality_policy,
                lifecycle_rule,
                &BTreeSet::new(),
            )
            .expect("形成 DPT 零对价串联黄金终态");
        assert_eq!(expected_effects.trade_finality_effects.len(), 1);
        let expected_order = expected
            .orders
            .get(&platform_order_id)
            .expect("DPT 零对价串联黄金订单");
        assert_eq!(expected_order.matched_base_units, 100_000);
        assert_eq!(expected_order.matched_quote_units, 1);
        assert_eq!(expected_order.pending_maker_depletion_base_units, 0);
        let baseline = serde_json::to_vec(&state).expect("序列化 DPT 零对价串联基线");

        reset_execution_state_clone_count_for_tests();
        let (prepared, effects) = state
            .prepare_execution_drive_delta_v2(1_103)
            .expect("准备 DPT 零对价串联稀疏 scheduled 前缀")
            .prepare_authorized_market_events_v2(
                &directory,
                1_103,
                &events,
                &finality_policy,
                lifecycle_rule,
                &BTreeSet::new(),
            )
            .expect("DPT 稀疏候选必须按输入原序累计零对价减量");
        assert_eq!(effects, expected_effects);
        assert_eq!(
            prepared
                .candidate_order(&platform_order_id)
                .expect("DPT 零对价串联稀疏候选订单"),
            expected_order
        );
        drop(prepared);
        assert_eq!(
            serde_json::to_vec(&state).expect("序列化丢弃后的 DPT 零对价串联状态"),
            baseline
        );
        assert_eq!(execution_state_clone_count_for_tests(), 0);

        let (prepared, effects) = state
            .prepare_execution_drive_delta_v2(1_103)
            .expect("重新准备 DPT 零对价串联 scheduled 前缀")
            .prepare_authorized_market_events_v2(
                &directory,
                1_103,
                &events,
                &finality_policy,
                lifecycle_rule,
                &BTreeSet::new(),
            )
            .expect("重新准备 DPT 零对价串联候选");
        assert_eq!(effects, expected_effects);
        prepared
            .into_committable()
            .expect("DPT 零对价串联候选必须可提交")
            .commit();
        assert_eq!(state, expected);
        assert_eq!(execution_state_clone_count_for_tests(), 0);
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
            legacy
                .prepare_execution_drive_delta_v2(0)
                .expect("旧 V1 黄金路径必须先推进相同 scheduled 水位")
                .into_committable()
                .expect("空 scheduled 水位候选必须可提交")
                .commit();
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
            let execution = state
                .prepare_execution_drive_delta_v2(0)
                .expect("准备新 execution-drive 订单候选前缀");
            let builder = match case.target {
                Target::Quote(accepted_quote_units) => execution
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
                } => execution
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
            let (prepared, effects) = builder
                .finish_new_order(order, fills, &policy, lifecycle_rule)
                .expect("execution-drive V2 完成新订单候选");
            assert_eq!(effects.order_commit_effects(), &legacy_order_effects);
            assert_eq!(effects.trade_finality_effects(), &legacy_trade_effects);
            assert_eq!(effects.venue_dust_effect(), legacy_dust.as_ref());
            assert_eq!(effects.venue_dust_effect().is_some(), case.expect_dust);
            prepared
                .into_committable()
                .expect("execution-drive V2 新订单候选必须可提交")
                .commit();

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
    fn asset_queue_v2_calculates_without_mutation_and_applies_exact_taker_consumption() {
        let initial_bids = [];
        let initial_asks = [
            (decimal("0.1"), 1),
            (decimal("0.5"), 2),
            (decimal("0.6"), 5),
        ];
        let mut queue = AssetQueueV2::from_initial_levels(&initial_bids, &initial_asks)
            .expect("构造无摘要 V2 资产队列");
        let mut untouched = AssetQueueV2::from_initial_levels(&initial_bids, &initial_asks)
            .expect("构造独立未变化 V2 资产队列");
        queue
            .apply_level_transition(BookSide::Ask, decimal("0.5"), 2, 7)
            .expect("DPT 正差在同价位追加第二个外部段");
        untouched
            .apply_level_transition(BookSide::Ask, decimal("0.5"), 2, 7)
            .expect("构造相同的未变化比较队列");

        let discarded = queue
            .calculate_taker_buy(decimal("0.6"), TakerTargetV1::Quote(6))
            .expect("一次只读计算跨同价多段和下一价格");
        assert_eq!(discarded.matched_base_units, 12);
        assert_eq!(discarded.used_quote_units, 6);
        assert_eq!(queue, untouched, "丢弃只读结果不得改变队列");

        let (matched, _) = queue.apply_taker_consumption(discarded);
        assert_eq!(
            matched,
            QueueMatchPlanV2 {
                fills: vec![
                    TakerFillSliceV2 {
                        price_pusd_per_share: decimal("0.5"),
                        base_units: 2,
                        quote_units: 1,
                    },
                    TakerFillSliceV2 {
                        price_pusd_per_share: decimal("0.5"),
                        base_units: 5,
                        quote_units: 2,
                    },
                    TakerFillSliceV2 {
                        price_pusd_per_share: decimal("0.6"),
                        base_units: 5,
                        quote_units: 3,
                    },
                ],
                matched_base_units: 12,
                used_quote_units: 6,
            }
        );
        assert_eq!(
            queue.asks[&decimal("0.5")].account_taker_depletion_debt_units,
            7
        );
        assert!(queue.asks[&decimal("0.5")].segments.is_empty());
        assert_eq!(
            queue.asks[&decimal("0.6")].account_taker_depletion_debt_units,
            5
        );
        assert!(queue.asks[&decimal("0.6")].segments.is_empty());
        assert_eq!(queue.best_executable_ask(), None);
    }

    #[test]
    fn asset_queue_v2_applies_debt_fifo_and_resync_without_source_identity() {
        let ask_price = decimal("0.5");
        let mut debt_queue = AssetQueueV2::from_initial_levels(&[], &[(ask_price, 10)])
            .expect("构造可执行卖方外部段");
        let taker = debt_queue
            .calculate_taker_buy(
                ask_price,
                TakerTargetV1::LimitBase {
                    total_base_units: 4,
                    total_quote_units: 2,
                },
            )
            .expect("形成真实 taker 消耗和队头债务");
        let _ = debt_queue.apply_taker_consumption(taker);
        let debt_echo = debt_queue
            .apply_level_transition(BookSide::Ask, ask_price, 10, 6)
            .expect("DPT 回声只抵扣既有 taker 债务");
        assert_eq!(debt_echo, AssetQueueEffectsV2::default());
        let ask = &debt_queue.asks[&ask_price];
        assert_eq!(ask.account_taker_depletion_debt_units, 0);
        assert!(matches!(
            ask.segments.front(),
            Some(QueueSegmentV2::External {
                consumed_base_units: 4,
                ..
            })
        ));

        let bid_price = decimal("0.4");
        let mut queue = AssetQueueV2::from_initial_levels(&[(bid_price, 2)], &[])
            .expect("从初始快照加入首个外部段");
        queue.insert_account_buy_remainder(bid_price, "account-order".to_owned(), 3);
        queue
            .apply_level_transition(BookSide::Bid, bid_price, 2, 7)
            .expect("DPT 正差把后续外部段加入队尾");

        let effects = queue
            .apply_level_transition(BookSide::Bid, bid_price, 7, 4)
            .expect("V2 队列按 FIFO 穿过外部段后消费账户段");
        assert_eq!(
            effects.maker_depletions,
            vec![MakerQueueDepletionV2 {
                platform_order_id: "account-order".to_owned(),
                price_pusd_per_share: bid_price,
                base_units: 1,
            }]
        );
        assert_eq!(effects.venue_dust_candidate, None);
        let price_queue = &queue.bids[&bid_price];
        assert_eq!(price_queue.account_taker_depletion_debt_units, 0);
        assert_eq!(price_queue.segments.len(), 2);
        assert!(matches!(
            price_queue.segments.front(),
            Some(QueueSegmentV2::Account {
                platform_order_id,
                remaining_base_units: 2,
            }) if platform_order_id == "account-order"
        ));
        assert!(matches!(
            price_queue.segments.back(),
            Some(QueueSegmentV2::External {
                consumed_base_units: 0,
                ..
            })
        ));

        queue.clear_external_segments();
        assert!(matches!(
            queue.bids[&bid_price].segments.front(),
            Some(QueueSegmentV2::Account {
                platform_order_id,
                remaining_base_units: 2,
            }) if platform_order_id == "account-order"
        ));

        let mut immediate_dust_queue = AssetQueueV2::default();
        immediate_dust_queue.insert_account_buy_remainder(
            bid_price,
            "immediate-dust-order".to_owned(),
            2,
        );
        assert_eq!(
            immediate_dust_queue.venue_dust_candidate(bid_price, "immediate-dust-order"),
            Some(AssetVenueDustCandidateV2 {
                platform_order_id: "immediate-dust-order".to_owned(),
                price_pusd_per_share: bid_price,
                remaining_base_units: 2,
            })
        );

        let mut dust_queue = AssetQueueV2::from_initial_levels(&[(bid_price, 2)], &[])
            .expect("从初始快照加入小额余量前的外部段");
        dust_queue.insert_account_buy_remainder(bid_price, "dust-order".to_owned(), 3);
        let dust = dust_queue
            .apply_level_transition(BookSide::Bid, bid_price, 2, 0)
            .expect("外部段被穿过后只剩账户余量");
        assert_eq!(
            dust.venue_dust_candidate,
            Some(AssetVenueDustCandidateV2 {
                platform_order_id: "dust-order".to_owned(),
                price_pusd_per_share: bid_price,
                remaining_base_units: 3,
            })
        );
    }

    fn execution_state_v2_for_resting_limit_tests(
        available_pusd: &str,
    ) -> (ExecutionStateV2, ConditionId, TokenId) {
        use contracts::{
            AccountInstanceKeyV1, AccountOpeningFactV2, InitialAccountStateV2, PlatformSourceV1,
            SimCallerRoleV1,
        };

        let scenario = crate::scenario::scenario_for_sim_tests();
        let directory =
            SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("V2 订单测试目录");
        let market = scenario
            .market_set()
            .markets()
            .first()
            .expect("V2 订单测试市场");
        let condition_id = market.market().condition_id.clone();
        let token_id = market.assets()[0].token_id.clone();
        let opening = AccountOpeningFactV2 {
            creation_request_id: "v2-order-create".to_owned(),
            account_instance_key: AccountInstanceKeyV1 {
                run_id: "v2-order-run".to_owned(),
                caller_role: SimCallerRoleV1::ModelTrain,
                caller_instance_id: "v2-order-caller".to_owned(),
                account_id: "v2-order-account".to_owned(),
                platform_source: PlatformSourceV1::Simulated,
            },
            session_id: "v2-order-session".to_owned(),
            initial_platform_scenario_file_sha256: Sha256Hex("a".repeat(64)),
            initial_account_state: InitialAccountStateV2 {
                available_pusd: ExactDecimal(available_pusd.to_owned()),
            },
        };
        let account =
            PlatformAccountStateV2::try_from_opening_fact(&opening).expect("V2 订单测试账户");
        let mut state = ExecutionStateV2::new(&directory, account);
        state
            .apply_initial_levels(&condition_id.0, &token_id.0, &[], &[])
            .expect("V2 订单测试初始空盘口");
        (state, condition_id, token_id)
    }

    fn economic_at_zero_policy_v2_for_tests() -> TradeFinalityPolicyV2 {
        TradeFinalityPolicyV2::decode_and_seal(
            br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10},{"state":"CONFIRMED","offset_ms":20}],"economic_application_ordinal":0},"failure_overrides":[]}"#,
        )
        .expect("V2 零偏移经济应用测试策略")
    }

    fn decoded_level_event_at_v2_for_tests(
        side: BookSide,
        price_pusd_per_share: &str,
        before: &str,
        after: &str,
        source_at_utc_ms: i64,
        received_at_utc_ms: i64,
        sequence: u64,
    ) -> ExecutionMarketEventV3 {
        let before_decimal: Decimal = before.parse().expect("测试 before 十进制");
        let after_decimal: Decimal = after.parse().expect("测试 after 十进制");
        let (added, reduced, queue) = if after_decimal > before_decimal {
            (after_decimal - before_decimal, Decimal::ZERO, Decimal::ZERO)
        } else {
            (
                Decimal::ZERO,
                before_decimal - after_decimal,
                before_decimal - after_decimal,
            )
        };
        let event = crate::task21_tests::sealed_event(
            ExecutionMarketEventPayloadV3::LevelExecutionTransition(LevelExecutionTransitionV3 {
                causal_group_id: Sha256Hex(format!("{:064x}", sequence + 10)),
                market: crate::task21_tests::market(),
                asset: crate::task21_tests::asset("1", 0, "Up"),
                side,
                price_pusd_per_share: ExactDecimal(price_pusd_per_share.to_owned()),
                quantity_before_shares: ExactDecimal(before.to_owned()),
                quantity_after_shares: ExactDecimal(after.to_owned()),
                attributed_trade_quantity_shares: ExactDecimal("0".to_owned()),
                already_covered_quantity_shares: ExactDecimal("0".to_owned()),
                net_added_quantity_shares: ExactDecimal(added.to_string()),
                net_reduced_quantity_shares: ExactDecimal(reduced.to_string()),
                queue_consumption_quantity_shares: ExactDecimal(queue.to_string()),
                basis: contracts::LevelExecutionBasis::LevelDeltaInference,
                platform_effective_at_utc_ms: source_at_utc_ms,
                visible_at_utc_ms: received_at_utc_ms,
                source_attribution_quality: contracts::SourceAttributionQuality::Unattributed,
                queue_result_quality: contracts::QueueResultQuality::DeterministicInferred,
            }),
            crate::task21_tests::orderbook_evidence(source_at_utc_ms, received_at_utc_ms, sequence),
            crate::task21_tests::fresh_quality(),
        );
        let bytes = data_pipeline_core::encode_execution_market_event_v3(&event)
            .expect("编码真实 V2 档位测试事件");
        data_pipeline_core::decode_execution_market_event_v3(&bytes)
            .expect("从真实边界解码 V2 档位测试事件")
    }

    fn decoded_level_event_v2_for_tests(
        before: &str,
        after: &str,
        source_at_utc_ms: i64,
        received_at_utc_ms: i64,
        sequence: u64,
    ) -> ExecutionMarketEventV3 {
        decoded_level_event_at_v2_for_tests(
            BookSide::Bid,
            "0.3",
            before,
            after,
            source_at_utc_ms,
            received_at_utc_ms,
            sequence,
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_accumulates_zero_quote_maker_then_creates_one_fill() {
        let (mut state, condition_id, token_id) = execution_state_v2_for_resting_limit_tests("10");
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = economic_at_zero_policy_v2_for_tests();
        let (next, disposition) = state
            .apply_buy_order(
                BuyOrderActionV2 {
                    global_request_id: "v2-maker-request".to_owned(),
                    batch_item_ordinal: 0,
                    item_id: "v2-maker-item".to_owned(),
                    condition_id: condition_id.clone(),
                    token_id: token_id.clone(),
                    effective_processing_at_utc_ms: 1_200,
                    terms: BuyOrderTermsV2::Gtc {
                        limit_price_pusd_per_share: decimal("0.3"),
                        accepted_base_units: 5_000_000,
                        accepted_quote_units: 1_500_000,
                        post_only: false,
                    },
                },
                rules.snapshot(),
                &finality_policy,
            )
            .expect("建立真实可达的 V2 maker 挂单");
        state = next;
        assert!(matches!(disposition, BuyOrderDispositionV2::Accepted(_)));

        for cycle in 0..4_u64 {
            let added = decoded_level_event_v2_for_tests(
                "0",
                "0.000001",
                1_300 + (cycle as i64) * 20,
                1_301 + (cycle as i64) * 20,
                10 + cycle * 2,
            );
            let normalized =
                crate::market::normalize_market_event_v2(&added).expect("一次规范化档位增加");
            let added_effects = state
                .apply_market_event(
                    &condition_id.0,
                    &normalized,
                    added.time.source_event_at_utc_ms.expect("隐藏事件时点"),
                    2_000,
                    rules.snapshot(),
                    &finality_policy,
                )
                .expect("档位增加只追加外部队尾");
            assert!(added_effects.initial_trade_finality.is_empty());
            assert!(added_effects.account_deltas.is_empty());
            assert!(added_effects.maker_fill_sequences.is_empty());
            assert!(added_effects.venue_dust.is_empty());
            assert_eq!(added_effects.resources.added_records, 1);
            assert_eq!(added_effects.resources.removed_records, 0);

            let reduced = decoded_level_event_v2_for_tests(
                "0.000001",
                "0",
                1_310 + (cycle as i64) * 20,
                1_311 + (cycle as i64) * 20,
                11 + cycle * 2,
            );
            let normalized =
                crate::market::normalize_market_event_v2(&reduced).expect("一次规范化档位减量");
            let reduced_effects = state
                .apply_market_event(
                    &condition_id.0,
                    &normalized,
                    reduced.time.source_event_at_utc_ms.expect("隐藏事件时点"),
                    2_000,
                    rules.snapshot(),
                    &finality_policy,
                )
                .expect("档位减量按队列原序处理 maker");
            if cycle < 3 {
                assert!(reduced_effects.maker_fill_sequences.is_empty());
                assert_eq!(state.next_fill_sequence, 1);
            } else {
                assert_eq!(reduced_effects.maker_fill_sequences, vec![1]);
                assert_eq!(reduced_effects.initial_trade_finality.len(), 1);
                assert_eq!(reduced_effects.account_deltas.len(), 1);
                assert_eq!(state.next_fill_sequence, 2);
            }
        }

        let order = state.orders.get("sim-1").expect("maker 订单保留");
        assert_eq!(
            order.quantities,
            OrderQuantityStateV2::Base {
                accepted_base_units: 5_000_000,
                matched_base_units: 4,
                pending_maker_depletion_base_units: 0,
                open_base_units: 4_999_996,
                venue_dust_closed_base_units: 0,
                canceled_base_units: 0,
                expired_base_units: 0,
                market_resolved_canceled_base_units: 0,
                accepted_quote_units: 1_500_000,
                matched_quote_units: 1,
                open_encumbered_quote_units: 1_499_999,
                released_price_improvement_quote_units: 0,
                venue_dust_released_quote_units: 0,
                canceled_released_quote_units: 0,
                expired_released_quote_units: 0,
                market_resolved_released_quote_units: 0,
            }
        );
        assert_eq!(state.fills.len(), 1);
        assert_eq!(
            state.fills[&1].liquidity_role,
            SimLiquidityRoleV1::MakerInferred
        );
        assert_eq!(state.fills[&1].base_units, 4);
        assert_eq!(state.fills[&1].quote_units, 1);
        assert!(state.maker_rebate.pending_fill_sequences.contains(&1));
        assert_eq!(
            state.maker_rebate.markets[&condition_id.0].pending_fill_count,
            1
        );
        assert_eq!(
            state.account_overlay.quantity(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                decimal("0.3"),
            ),
            decimal("4.999996"),
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_caller_cancel_closes_pending_and_open_once_without_fake_fill() {
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = economic_at_zero_policy_v2_for_tests();
        let expires_at_utc_ms = 300_000;
        let (state, condition_id, token_id) = execution_state_v2_for_resting_limit_tests("10");
        let (mut state, disposition) = state
            .apply_buy_order(
                BuyOrderActionV2 {
                    global_request_id: "v2-cancel-pending-request".to_owned(),
                    batch_item_ordinal: 0,
                    item_id: "v2-cancel-pending-item".to_owned(),
                    condition_id: condition_id.clone(),
                    token_id: token_id.clone(),
                    effective_processing_at_utc_ms: 1_200,
                    terms: BuyOrderTermsV2::Gtd {
                        limit_price_pusd_per_share: decimal("0.3"),
                        accepted_base_units: 5_000_000,
                        accepted_quote_units: 1_500_000,
                        post_only: false,
                        expires_at_utc_ms: Some(expires_at_utc_ms),
                    },
                },
                rules.snapshot(),
                &finality_policy,
            )
            .expect("建立 pending 撤单测试 GTD 挂单");
        assert!(matches!(disposition, BuyOrderDispositionV2::Accepted(_)));

        for event in [
            decoded_level_event_v2_for_tests("0", "0.000001", 1_300, 1_301, 30),
            decoded_level_event_v2_for_tests("0.000001", "0", 1_310, 1_311, 31),
        ] {
            let normalized = crate::market::normalize_market_event_v2(&event)
                .expect("一次规范化 pending 档位事件");
            let effects = state
                .apply_market_event(
                    &condition_id.0,
                    &normalized,
                    event.time.source_event_at_utc_ms.expect("隐藏事件时点"),
                    2_000,
                    rules.snapshot(),
                    &finality_policy,
                )
                .expect("pending 档位事件必须成功");
            assert!(effects.initial_trade_finality.is_empty());
            assert!(effects.account_deltas.is_empty());
            assert!(effects.maker_fill_sequences.is_empty());
        }
        let OrderQuantityStateV2::Base {
            pending_maker_depletion_base_units,
            open_base_units,
            open_encumbered_quote_units,
            ..
        } = &state.orders["sim-1"].quantities
        else {
            panic!("GTD 必须使用 base 计量")
        };
        assert_eq!(*pending_maker_depletion_base_units, 1);
        assert_eq!(*open_base_units, 4_999_999);
        assert_eq!(*open_encumbered_quote_units, 1_500_000);
        assert!(state.fills.is_empty(), "零 quote maker 不得伪造成交");
        assert_eq!(state.next_fill_sequence, 1);

        let (state, disposition) = state
            .apply_caller_cancel("sim-1", 1_400)
            .expect("pending 与开放余量必须可一并撤销");
        let CallerCancelDispositionV2::Canceled(canceled) = disposition else {
            panic!("pending 与开放余量必须形成成功撤单")
        };
        assert_eq!(canceled.pending_base_units_before, 1);
        assert_eq!(canceled.open_base_units_before, 4_999_999);
        assert_eq!(canceled.released_open_quote_units, 1_500_000);
        assert_eq!(canceled.resources.removed_records, 3);
        assert_eq!(canceled.resources.removed_bytes, 2 * "sim-1".len() as u64);
        assert_eq!(
            canceled
                .account_delta
                .as_ref()
                .expect("撤单必须释放开放本金")
                .account_transition_seq,
            2
        );
        assert_eq!(
            state.external_queue_base_units_for_tests(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                decimal("0.3"),
            ),
            1,
            "撤单不得误删仍属于外部证据的队尾段"
        );
        assert_eq!(
            state.account_overlay.quantity(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                decimal("0.3"),
            ),
            Decimal::ZERO
        );
        assert_eq!(state.next_gtd_deadline_at_utc_ms(), None);
        assert!(state.fills.is_empty());
        assert_eq!(state.account.available_pusd(), decimal("10"));
        assert_eq!(state.account.principal_encumbrance_pusd(), Decimal::ZERO);
        let OrderQuantityStateV2::Base {
            pending_maker_depletion_base_units,
            open_base_units,
            canceled_base_units,
            canceled_released_quote_units,
            ..
        } = &state.orders["sim-1"].quantities
        else {
            unreachable!("刚确认的 GTD 必须使用 base 计量")
        };
        assert_eq!(*pending_maker_depletion_base_units, 0);
        assert_eq!(*open_base_units, 0);
        assert_eq!(*canceled_base_units, 5_000_000);
        assert_eq!(*canceled_released_quote_units, 1_500_000);

        let account_sequence_before_retry = state.account.next_account_transition_seq();
        let last_updated_before_retry = state.orders["sim-1"].last_updated_at_utc_ms;
        let (state, retry) = state
            .apply_caller_cancel("sim-1", 1_401)
            .expect("重复撤单必须是规范无效果结果");
        assert_eq!(retry, CallerCancelDispositionV2::NoCancellableRemainder);
        assert_eq!(
            state.account.next_account_transition_seq(),
            account_sequence_before_retry
        );
        assert_eq!(
            state.orders["sim-1"].last_updated_at_utc_ms, last_updated_before_retry,
            "重复撤单不得改写订单时点"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_maker_terminal_removes_gtd_deadline() {
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = economic_at_zero_policy_v2_for_tests();
        let expires_at_utc_ms = 300_000;

        let (state, condition_id, token_id) = execution_state_v2_for_resting_limit_tests("10");
        let (mut state, disposition) = state
            .apply_buy_order(
                BuyOrderActionV2 {
                    global_request_id: "v2-maker-gtd-complete".to_owned(),
                    batch_item_ordinal: 0,
                    item_id: "v2-maker-gtd-complete-item".to_owned(),
                    condition_id: condition_id.clone(),
                    token_id: token_id.clone(),
                    effective_processing_at_utc_ms: 1_200,
                    terms: BuyOrderTermsV2::Gtd {
                        limit_price_pusd_per_share: decimal("0.3"),
                        accepted_base_units: 5_000_000,
                        accepted_quote_units: 1_500_000,
                        post_only: false,
                        expires_at_utc_ms: Some(expires_at_utc_ms),
                    },
                },
                rules.snapshot(),
                &finality_policy,
            )
            .expect("建立真实可达的 V2 GTD maker 挂单");
        assert!(matches!(disposition, BuyOrderDispositionV2::Accepted(_)));
        assert_eq!(state.next_gtd_deadline_at_utc_ms(), Some(expires_at_utc_ms));

        for (index, event) in [
            decoded_level_event_v2_for_tests("0", "5", 1_300, 1_301, 40),
            decoded_level_event_v2_for_tests("5", "0", 1_310, 1_311, 41),
        ]
        .into_iter()
        .enumerate()
        {
            let normalized = crate::market::normalize_market_event_v2(&event)
                .expect("一次规范化 GTD maker 档位事件");
            let effects = state
                .apply_market_event(
                    &condition_id.0,
                    &normalized,
                    event.time.source_event_at_utc_ms.expect("隐藏事件时点"),
                    2_000,
                    rules.snapshot(),
                    &finality_policy,
                )
                .expect("GTD maker 档位事件必须成功");
            if index == 1 {
                assert_eq!(effects.resources.removed_records, 3);
                assert_eq!(effects.resources.removed_bytes, 2 * "sim-1".len() as u64);
            }
        }

        assert!(state.gtd_deadlines.is_empty());
        assert_eq!(state.next_gtd_deadline_at_utc_ms(), None);
        assert_eq!(state.orders["sim-1"].terminal_reason, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_end_at_closes_matching_without_releasing_gtd_order() {
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = economic_at_zero_policy_v2_for_tests();
        let expires_at_utc_ms = 300_000;
        let (state, condition_id, token_id) = execution_state_v2_for_resting_limit_tests("10");
        let (mut state, disposition) = state
            .apply_buy_order(
                BuyOrderActionV2 {
                    global_request_id: "v2-end-at-gtd".to_owned(),
                    batch_item_ordinal: 0,
                    item_id: "v2-end-at-gtd-item".to_owned(),
                    condition_id: condition_id.clone(),
                    token_id: token_id.clone(),
                    effective_processing_at_utc_ms: 1_200,
                    terms: BuyOrderTermsV2::Gtd {
                        limit_price_pusd_per_share: decimal("0.3"),
                        accepted_base_units: 5_000_000,
                        accepted_quote_units: 1_500_000,
                        post_only: false,
                        expires_at_utc_ms: Some(expires_at_utc_ms),
                    },
                },
                rules.snapshot(),
                &finality_policy,
            )
            .expect("建立 end_at 测试 GTD 挂单");
        assert!(matches!(disposition, BuyOrderDispositionV2::Accepted(_)));

        let event = decoded_level_event_v2_for_tests("0", "1", 2_000, 2_001, 50);
        let normalized =
            crate::market::normalize_market_event_v2(&event).expect("一次规范化 end_at 档位事件");
        let effects = state
            .apply_market_event(
                &condition_id.0,
                &normalized,
                2_000,
                2_000,
                rules.snapshot(),
                &finality_policy,
            )
            .expect("end_at 只关闭撮合资格");

        assert!(effects.initial_trade_finality.is_empty());
        assert!(effects.account_deltas.is_empty());
        assert!(effects.maker_fill_sequences.is_empty());
        assert!(effects.venue_dust.is_empty());
        assert_eq!(effects.resources.added_records, 0);
        assert_eq!(effects.resources.removed_records, 2);
        assert_eq!(effects.resources.removed_bytes, "sim-1".len() as u64);
        assert!(state.queues.matching_closed(&condition_id.0).unwrap());
        assert!(
            state
                .queues
                .asset(&condition_id.0, &token_id.0)
                .expect("end_at 后目标队列仍存在")
                .bids
                .is_empty()
        );
        assert_eq!(
            state.orders["sim-1"].quantities,
            OrderQuantityStateV2::Base {
                accepted_base_units: 5_000_000,
                matched_base_units: 0,
                pending_maker_depletion_base_units: 0,
                open_base_units: 5_000_000,
                venue_dust_closed_base_units: 0,
                canceled_base_units: 0,
                expired_base_units: 0,
                market_resolved_canceled_base_units: 0,
                accepted_quote_units: 1_500_000,
                matched_quote_units: 0,
                open_encumbered_quote_units: 1_500_000,
                released_price_improvement_quote_units: 0,
                venue_dust_released_quote_units: 0,
                canceled_released_quote_units: 0,
                expired_released_quote_units: 0,
                market_resolved_released_quote_units: 0,
            }
        );
        assert_eq!(state.next_fill_sequence, 1);
        assert_eq!(state.account.available_pusd(), decimal("8.5"));
        assert_eq!(state.account.principal_encumbrance_pusd(), decimal("1.5"));
        assert_eq!(
            state.account_overlay.quantity(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                decimal("0.3"),
            ),
            decimal("5")
        );
        assert_eq!(state.next_gtd_deadline_at_utc_ms(), Some(expires_at_utc_ms));

        let (state, disposition) = state
            .apply_caller_cancel("sim-1", 2_001)
            .expect("end_at 后仍须允许撤销既有 GTD 余量");
        let CallerCancelDispositionV2::Canceled(canceled) = disposition else {
            panic!("end_at 后既有 GTD 余量必须可撤")
        };
        assert_eq!(canceled.pending_base_units_before, 0);
        assert_eq!(canceled.open_base_units_before, 5_000_000);
        assert_eq!(canceled.released_open_quote_units, 1_500_000);
        assert_eq!(canceled.resources.removed_records, 2);
        assert_eq!(canceled.resources.removed_bytes, "sim-1".len() as u64);
        assert_eq!(state.next_gtd_deadline_at_utc_ms(), None);
        assert_eq!(state.account.available_pusd(), decimal("10"));
        assert_eq!(state.account.principal_encumbrance_pusd(), Decimal::ZERO);
        assert_eq!(
            state.account_overlay.quantity(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                decimal("0.3"),
            ),
            Decimal::ZERO
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_resting_gtc_commits_one_order_account_and_overlay_transition() {
        let (state, condition_id, token_id) = execution_state_v2_for_resting_limit_tests("10");
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = crate::finality::successful_policy_v2_for_tests();
        let input = BuyOrderActionV2 {
            global_request_id: "v2-gtc-request".to_owned(),
            batch_item_ordinal: 0,
            item_id: "v2-gtc-item".to_owned(),
            condition_id: condition_id.clone(),
            token_id: token_id.clone(),
            effective_processing_at_utc_ms: 1_500,
            terms: BuyOrderTermsV2::Gtc {
                limit_price_pusd_per_share: decimal("0.5"),
                accepted_base_units: 2_000_000,
                accepted_quote_units: 1_000_000,
                post_only: false,
            },
        };

        let (state, disposition) = state
            .apply_buy_order(input, rules.snapshot(), &finality_policy)
            .expect("无可成交 ask 的 GTC 应进入挂单");
        let BuyOrderDispositionV2::Accepted(accepted) = disposition else {
            panic!("资金充足的 GTC 必须接受");
        };

        assert_eq!(accepted.platform_order_id, "sim-1");
        assert_eq!(accepted.resources.added_records, 4);
        assert_eq!(accepted.resources.removed_records, 0);
        assert_eq!(accepted.account_deltas.len(), 1);
        assert_eq!(accepted.account_deltas[0].account_transition_seq, 1);
        assert!(accepted.initial_trade_finality.is_empty());
        assert_eq!(state.next_order_sequence, 2);
        assert_eq!(state.account.next_account_transition_seq(), 2);
        assert_eq!(state.account.available_pusd(), decimal("9"));
        assert_eq!(state.account.principal_encumbrance_pusd(), decimal("1"));
        assert!(matches!(
            state
                .queues
                .asset(&condition_id.0, &token_id.0)
                .expect("GTC 接受后的目标队列")
                .bids[&decimal("0.5")]
                .segments
                .front(),
            Some(QueueSegmentV2::Account {
                platform_order_id,
                remaining_base_units: 2_000_000,
            }) if platform_order_id == "sim-1"
        ));
        assert_eq!(
            state.account_overlay.quantity(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                decimal("0.5"),
            ),
            decimal("2")
        );
        assert!(state.gtd_deadlines.is_empty());
        assert_eq!(
            state.orders.get("sim-1").map(|order| &order.quantities),
            Some(&OrderQuantityStateV2::Base {
                accepted_base_units: 2_000_000,
                matched_base_units: 0,
                pending_maker_depletion_base_units: 0,
                open_base_units: 2_000_000,
                venue_dust_closed_base_units: 0,
                canceled_base_units: 0,
                expired_base_units: 0,
                market_resolved_canceled_base_units: 0,
                accepted_quote_units: 1_000_000,
                matched_quote_units: 0,
                open_encumbered_quote_units: 1_000_000,
                released_price_improvement_quote_units: 0,
                venue_dust_released_quote_units: 0,
                canceled_released_quote_units: 0,
                expired_released_quote_units: 0,
                market_resolved_released_quote_units: 0,
            })
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_rejected_limit_buy_consumes_nothing_and_later_gtd_uses_first_sequence()
     {
        let (state, condition_id, token_id) = execution_state_v2_for_resting_limit_tests("0.5");
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = crate::finality::successful_policy_v2_for_tests();
        let insufficient = BuyOrderActionV2 {
            global_request_id: "v2-insufficient-request".to_owned(),
            batch_item_ordinal: 0,
            item_id: "v2-insufficient-item".to_owned(),
            condition_id: condition_id.clone(),
            token_id: token_id.clone(),
            effective_processing_at_utc_ms: 1_500,
            terms: BuyOrderTermsV2::Gtc {
                limit_price_pusd_per_share: decimal("0.5"),
                accepted_base_units: 2_000_000,
                accepted_quote_units: 1_000_000,
                post_only: false,
            },
        };
        let (state, disposition) = state
            .apply_buy_order(insufficient, rules.snapshot(), &finality_policy)
            .expect("资金不足是规范无效果结果");
        assert_eq!(
            disposition,
            BuyOrderDispositionV2::RejectedNoEffect(
                BuyOrderRejectedReasonV2::InsufficientAvailablePusd
            )
        );
        assert_eq!(state.next_order_sequence, 1);
        assert_eq!(state.account.next_account_transition_seq(), 1);
        assert_eq!(state.account.available_pusd(), decimal("0.5"));
        assert!(state.orders.is_empty());
        assert!(
            state
                .queues
                .asset(&condition_id.0, &token_id.0)
                .expect("资金不足后的目标队列")
                .bids
                .is_empty()
        );
        assert_eq!(
            state.account_overlay.quantity(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                decimal("0.5"),
            ),
            Decimal::ZERO
        );

        let expires_at_utc_ms = 300_000;
        let accepted = BuyOrderActionV2 {
            global_request_id: "v2-gtd-request".to_owned(),
            batch_item_ordinal: 0,
            item_id: "v2-gtd-item".to_owned(),
            condition_id: condition_id.clone(),
            token_id: token_id.clone(),
            effective_processing_at_utc_ms: 2_000,
            terms: BuyOrderTermsV2::Gtd {
                expires_at_utc_ms: Some(expires_at_utc_ms),
                limit_price_pusd_per_share: decimal("0.4"),
                accepted_base_units: 1_000_000,
                accepted_quote_units: 400_000,
                post_only: false,
            },
        };
        let (state, disposition) = state
            .apply_buy_order(accepted, rules.snapshot(), &finality_policy)
            .expect("后续资金可承受的 GTD 应接受");
        let BuyOrderDispositionV2::Accepted(accepted) = disposition else {
            panic!("资金充足的 GTD 必须接受");
        };
        assert_eq!(accepted.platform_order_id, "sim-1");
        assert_eq!(state.next_order_sequence, 2);
        assert!(matches!(
            state
                .queues
                .asset(&condition_id.0, &token_id.0)
                .expect("GTD 接受后的目标队列")
                .bids[&decimal("0.4")]
                .segments
                .front(),
            Some(QueueSegmentV2::Account {
                platform_order_id,
                remaining_base_units: 1_000_000,
            }) if platform_order_id == "sim-1"
        ));
        assert!(
            state
                .gtd_deadlines
                .contains(&(expires_at_utc_ms, "sim-1".to_owned()))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_rejections_preserve_liquidity_then_full_fok_uses_first_sequences() {
        let (mut state, condition_id, token_id) = execution_state_v2_for_resting_limit_tests("10");
        state
            .apply_initial_levels(
                &condition_id.0,
                &token_id.0,
                &[],
                &[(decimal("0.5"), 1_000_000)],
            )
            .expect("FOK/FAK 零效果测试 ask");
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = crate::finality::successful_policy_v2_for_tests();

        let (state, fok) = state
            .apply_buy_order(
                BuyOrderActionV2 {
                    global_request_id: "v2-fok-partial".to_owned(),
                    batch_item_ordinal: 0,
                    item_id: "v2-fok-partial-item".to_owned(),
                    condition_id: condition_id.clone(),
                    token_id: token_id.clone(),
                    effective_processing_at_utc_ms: 1_500,
                    terms: BuyOrderTermsV2::Fok {
                        worst_price_pusd_per_share: decimal("0.5"),
                        accepted_quote_budget_units: 800_000,
                        sdk_taker_amount_audit_base_units: 1_600_000,
                    },
                },
                rules.snapshot(),
                &finality_policy,
            )
            .expect("部分可成的 FOK 是规范无效果结果");
        assert_eq!(
            fok,
            BuyOrderDispositionV2::RejectedNoEffect(BuyOrderRejectedReasonV2::FokNotFullyFilled)
        );
        let (state, fak) = state
            .apply_buy_order(
                BuyOrderActionV2 {
                    global_request_id: "v2-fak-no-match".to_owned(),
                    batch_item_ordinal: 0,
                    item_id: "v2-fak-no-match-item".to_owned(),
                    condition_id: condition_id.clone(),
                    token_id: token_id.clone(),
                    effective_processing_at_utc_ms: 1_600,
                    terms: BuyOrderTermsV2::Fak {
                        worst_price_pusd_per_share: decimal("0.4"),
                        accepted_quote_budget_units: 300_000,
                        sdk_taker_amount_audit_base_units: 750_000,
                    },
                },
                rules.snapshot(),
                &finality_policy,
            )
            .expect("没有可成交 ask 的 FAK 是规范无效果结果");
        assert_eq!(
            fak,
            BuyOrderDispositionV2::RejectedNoEffect(BuyOrderRejectedReasonV2::FakNoMatch)
        );
        assert_eq!(state.next_order_sequence, 1);
        assert_eq!(state.next_fill_sequence, 1);
        assert_eq!(state.account.next_account_transition_seq(), 1);
        assert!(state.orders.is_empty());
        assert!(state.fills.is_empty());
        assert_eq!(
            state
                .queues
                .asset(&condition_id.0, &token_id.0)
                .unwrap()
                .asks[&decimal("0.5")]
                .segments
                .front(),
            Some(&QueueSegmentV2::External {
                total_base_units: 1_000_000,
                total_quote_units: 500_000,
                consumed_base_units: 0,
            })
        );

        let (state, fok) = state
            .apply_buy_order(
                BuyOrderActionV2 {
                    global_request_id: "v2-fok-full".to_owned(),
                    batch_item_ordinal: 0,
                    item_id: "v2-fok-full-item".to_owned(),
                    condition_id: condition_id.clone(),
                    token_id: token_id.clone(),
                    effective_processing_at_utc_ms: 1_700,
                    terms: BuyOrderTermsV2::Fok {
                        worst_price_pusd_per_share: decimal("0.5"),
                        accepted_quote_budget_units: 500_000,
                        sdk_taker_amount_audit_base_units: 1_000_000,
                    },
                },
                rules.snapshot(),
                &finality_policy,
            )
            .expect("同一流动性仍能让完整可成的 FOK 接受");
        let BuyOrderDispositionV2::Accepted(accepted) = fok else {
            panic!("完整可成的 FOK 必须接受");
        };
        assert_eq!(accepted.platform_order_id, "sim-1");
        assert_eq!(accepted.initial_trade_finality[0].fill_sequence, 1);
        assert_eq!(state.next_order_sequence, 2);
        assert_eq!(state.next_fill_sequence, 2);
        assert_eq!(state.account.next_account_transition_seq(), 2);
        assert_eq!(
            state.orders["sim-1"].quantities,
            OrderQuantityStateV2::QuoteBudget {
                accepted_quote_budget_units: 500_000,
                matched_quote_units: 500_000,
                released_or_unspent_quote_units: 0,
                actual_matched_base_units: 1_000_000,
                sdk_taker_amount_audit_base_units: 1_000_000,
            }
        );
        assert_eq!(
            state
                .queues
                .asset(&condition_id.0, &token_id.0)
                .expect("完整 FOK 后的目标队列")
                .best_executable_ask(),
            None
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_fak_positive_fill_releases_unspent_quote_and_registers_one_clock() {
        let (mut state, condition_id, token_id) = execution_state_v2_for_resting_limit_tests("10");
        state
            .apply_initial_levels(
                &condition_id.0,
                &token_id.0,
                &[],
                &[(decimal("0.5"), 1_000_000)],
            )
            .expect("FAK 正成交测试 ask");
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = crate::finality::successful_policy_v2_for_tests();
        let matched_at_utc_ms = 1_500;

        let (state, disposition) = state
            .apply_buy_order(
                BuyOrderActionV2 {
                    global_request_id: "v2-fak-positive".to_owned(),
                    batch_item_ordinal: 0,
                    item_id: "v2-fak-positive-item".to_owned(),
                    condition_id: condition_id.clone(),
                    token_id,
                    effective_processing_at_utc_ms: matched_at_utc_ms,
                    terms: BuyOrderTermsV2::Fak {
                        worst_price_pusd_per_share: decimal("0.5"),
                        accepted_quote_budget_units: 800_000,
                        sdk_taker_amount_audit_base_units: 1_600_000,
                    },
                },
                rules.snapshot(),
                &finality_policy,
            )
            .expect("FAK 正成交必须接受");
        let BuyOrderDispositionV2::Accepted(accepted) = disposition else {
            panic!("FAK 正成交必须形成订单");
        };
        assert_eq!(accepted.platform_order_id, "sim-1");
        assert_eq!(accepted.initial_trade_finality[0].fill_sequence, 1);
        assert_eq!(accepted.account_deltas.len(), 2);
        assert_eq!(accepted.account_deltas[0].account_transition_seq, 1);
        assert_eq!(accepted.account_deltas[1].account_transition_seq, 2);
        assert_eq!(state.next_order_sequence, 2);
        assert_eq!(state.next_fill_sequence, 2);
        assert_eq!(state.fills[&1].trade_id, "sim-1");
        assert!(state.fills[&1].immediate_taker_fee_units > 0);
        assert_eq!(state.fills[&1].finality_path_ordinal, 0);
        assert_eq!(
            state.orders["sim-1"].quantities,
            OrderQuantityStateV2::QuoteBudget {
                accepted_quote_budget_units: 800_000,
                matched_quote_units: 500_000,
                released_or_unspent_quote_units: 300_000,
                actual_matched_base_units: 1_000_000,
                sdk_taker_amount_audit_base_units: 1_600_000,
            }
        );
        assert_eq!(
            state.orders["sim-1"].terminal_reason,
            Some(SimOrderTerminalReasonV1::FakCompleted)
        );
        assert_eq!(state.account.principal_encumbrance_pusd(), decimal("0.5"));
        assert_eq!(state.taker_rebate.days[&0].pending_predecessor_count, 1);
        assert_eq!(
            state.taker_rebate.clocks,
            BTreeSet::from([(86_400_000 + 10 * 60_000, 0)])
        );
        assert_eq!(
            state.scheduled_finality,
            BTreeSet::from([ScheduledFinalityTransitionKeyV2 {
                platform_event_at_utc_ms: matched_at_utc_ms + 10,
                fill_sequence: 1,
                path_ordinal: 1,
            }])
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_gtc_uses_full_budget_gate_but_only_actual_principal_and_zero_offset_economic()
     {
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = economic_at_zero_policy_v2_for_tests();
        let (mut insufficient, condition_id, token_id) =
            execution_state_v2_for_resting_limit_tests("0.95");
        insufficient
            .apply_initial_levels(
                &condition_id.0,
                &token_id.0,
                &[],
                &[(decimal("0.4"), 1_000_000)],
            )
            .expect("GTC 价格改善资金门测试 ask");
        let action = || BuyOrderActionV2 {
            global_request_id: "v2-gtc-price-improvement".to_owned(),
            batch_item_ordinal: 0,
            item_id: "v2-gtc-price-improvement-item".to_owned(),
            condition_id: condition_id.clone(),
            token_id: token_id.clone(),
            effective_processing_at_utc_ms: 1_500,
            terms: BuyOrderTermsV2::Gtc {
                limit_price_pusd_per_share: decimal("0.5"),
                accepted_base_units: 2_000_000,
                accepted_quote_units: 1_000_000,
                post_only: false,
            },
        };
        let (insufficient, disposition) = insufficient
            .apply_buy_order(action(), rules.snapshot(), &finality_policy)
            .expect("资金不足必须是规范无效果结果");
        assert_eq!(
            disposition,
            BuyOrderDispositionV2::RejectedNoEffect(
                BuyOrderRejectedReasonV2::InsufficientAvailablePusd
            )
        );
        assert_eq!(insufficient.next_order_sequence, 1);
        assert_eq!(insufficient.next_fill_sequence, 1);
        assert_eq!(insufficient.account.available_pusd(), decimal("0.95"));

        let (mut state, _, _) = execution_state_v2_for_resting_limit_tests("10");
        state
            .apply_initial_levels(
                &condition_id.0,
                &token_id.0,
                &[],
                &[(decimal("0.4"), 1_000_000)],
            )
            .expect("GTC 价格改善成交测试 ask");
        let (state, disposition) = state
            .apply_buy_order(action(), rules.snapshot(), &finality_policy)
            .expect("资金充足的 GTC 价格改善必须接受");
        let BuyOrderDispositionV2::Accepted(accepted) = disposition else {
            panic!("资金充足的 GTC 必须接受");
        };
        assert_eq!(accepted.initial_trade_finality.len(), 1);
        assert_eq!(accepted.initial_trade_finality[0].fill_sequence, 1);
        assert_eq!(
            accepted.initial_trade_finality[0].finality_state,
            TradeFinalityStateV1::Matched
        );
        assert_eq!(
            accepted.initial_trade_finality[0].economic_disposition,
            TradeEconomicDispositionStateV1::PendingApplied
        );
        assert_eq!(
            accepted.initial_trade_finality[0].economic_application_account_transition_seq,
            Some(2)
        );
        assert_eq!(
            accepted.initial_trade_finality[0].account_transition_seq,
            Some(2)
        );
        assert_eq!(accepted.account_deltas.len(), 2);
        assert_eq!(accepted.account_deltas[0].account_transition_seq, 1);
        assert_eq!(accepted.account_deltas[1].account_transition_seq, 2);
        assert_eq!(accepted.resources.added_records, 9);
        assert_eq!(accepted.resources.removed_records, 1);
        assert_eq!(
            accepted.resources.added_bytes,
            26 + "v2-gtc-price-improvement".len() as u64
                + "v2-gtc-price-improvement-item".len() as u64
                + condition_id.0.len() as u64
                + token_id.0.len() as u64
        );
        assert_eq!(accepted.resources.removed_bytes, 0);
        assert_eq!(
            state.orders["sim-1"].quantities,
            OrderQuantityStateV2::Base {
                accepted_base_units: 2_000_000,
                matched_base_units: 1_000_000,
                pending_maker_depletion_base_units: 0,
                open_base_units: 1_000_000,
                venue_dust_closed_base_units: 0,
                canceled_base_units: 0,
                expired_base_units: 0,
                market_resolved_canceled_base_units: 0,
                accepted_quote_units: 1_000_000,
                matched_quote_units: 400_000,
                open_encumbered_quote_units: 500_000,
                released_price_improvement_quote_units: 100_000,
                venue_dust_released_quote_units: 0,
                canceled_released_quote_units: 0,
                expired_released_quote_units: 0,
                market_resolved_released_quote_units: 0,
            }
        );
        assert_eq!(state.account.principal_encumbrance_pusd(), decimal("0.5"));
        assert_eq!(
            state.account.immediate_taker_fee_encumbrance_pusd(),
            Decimal::ZERO
        );
        assert_eq!(state.fills[&1].immediate_taker_fee_units, 16_800);
        assert_eq!(
            state.fills[&1].economic_application_account_transition_seq,
            Some(2)
        );
        assert_eq!(
            state.account_overlay.quantity(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                decimal("0.5")
            ),
            Decimal::ONE
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_gtd_partial_fill_closes_immediate_dust_without_deadline() {
        let (mut state, condition_id, token_id) = execution_state_v2_for_resting_limit_tests("10");
        state
            .apply_initial_levels(
                &condition_id.0,
                &token_id.0,
                &[],
                &[(decimal("0.5"), 5_000_000)],
            )
            .expect("GTD 平台小额余量测试 ask");
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = crate::finality::successful_policy_v2_for_tests();
        let expires_at_utc_ms = 300_000;
        let (state, disposition) = state
            .apply_buy_order(
                BuyOrderActionV2 {
                    global_request_id: "v2-gtd-dust".to_owned(),
                    batch_item_ordinal: 0,
                    item_id: "v2-gtd-dust-item".to_owned(),
                    condition_id: condition_id.clone(),
                    token_id: token_id.clone(),
                    effective_processing_at_utc_ms: 1_500,
                    terms: BuyOrderTermsV2::Gtd {
                        limit_price_pusd_per_share: decimal("0.5"),
                        accepted_base_units: 5_005_000,
                        accepted_quote_units: 2_502_500,
                        post_only: false,
                        expires_at_utc_ms: Some(expires_at_utc_ms),
                    },
                },
                rules.snapshot(),
                &finality_policy,
            )
            .expect("GTD 部分成交后的平台小额余量必须原子关闭");
        let BuyOrderDispositionV2::Accepted(accepted) = disposition else {
            panic!("GTD 部分成交必须接受");
        };
        assert_eq!(accepted.account_deltas.len(), 2);
        assert_eq!(
            state.orders["sim-1"].quantities,
            OrderQuantityStateV2::Base {
                accepted_base_units: 5_005_000,
                matched_base_units: 5_000_000,
                pending_maker_depletion_base_units: 0,
                open_base_units: 0,
                venue_dust_closed_base_units: 5_000,
                canceled_base_units: 0,
                expired_base_units: 0,
                market_resolved_canceled_base_units: 0,
                accepted_quote_units: 2_502_500,
                matched_quote_units: 2_500_000,
                open_encumbered_quote_units: 0,
                released_price_improvement_quote_units: 0,
                venue_dust_released_quote_units: 2_500,
                canceled_released_quote_units: 0,
                expired_released_quote_units: 0,
                market_resolved_released_quote_units: 0,
            }
        );
        assert_eq!(
            state.orders["sim-1"].terminal_reason,
            Some(SimOrderTerminalReasonV1::VenueDustTerminal)
        );
        assert!(state.gtd_deadlines.is_empty());
        assert_eq!(
            state.account_overlay.quantity(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                decimal("0.5")
            ),
            Decimal::ZERO
        );
        assert!(
            state
                .queues
                .asset(&condition_id.0, &token_id.0)
                .expect("GTD dust 后的目标队列")
                .bids
                .is_empty()
        );
        assert_eq!(state.account.principal_encumbrance_pusd(), decimal("2.5"));

        let (response, lifecycle, state_resources) = accepted
            .into_output_effects(condition_id, token_id, 1_500)
            .expect("GTD 平台小额余量必须形成冻结的直接响应和生命周期效果");
        assert_eq!(state_resources.added_records, 8);
        assert_eq!(response.account_transition_sequences, vec![1, 2]);
        assert_eq!(
            response.platform_order_status.known,
            Some(SimulatedOrderKnownStatusV2::VenueDustTerminal)
        );
        let [
            LifecycleFeedbackEffectV2::Order(accepted_lifecycle),
            LifecycleFeedbackEffectV2::Trade(initial_trade),
            LifecycleFeedbackEffectV2::Order(dust_lifecycle),
        ] = lifecycle.as_slice()
        else {
            panic!("立即平台小额余量必须按接受、成交首态、小额余量终态形成效果")
        };
        let OrderLifecycleEventV2::OrderAccepted {
            status,
            account_delta,
        } = &accepted_lifecycle.event
        else {
            panic!("首个订单生命周期效果必须是接受")
        };
        assert_eq!(status.known, Some(SimulatedOrderKnownStatusV2::Live));
        assert_eq!(account_delta.account_transition_seq, 1);
        assert_eq!(
            initial_trade.status.known,
            Some(SimulatedTradeKnownStatusV2::Matched)
        );
        assert!(initial_trade.account_delta.is_none());
        let OrderLifecycleEventV2::VenueDustTerminal {
            closed_base_quantity_shares,
            released_quote_amount_pusd,
            account_delta,
        } = &dust_lifecycle.event
        else {
            panic!("末个订单生命周期效果必须是平台小额余量终态")
        };
        assert_eq!(closed_base_quantity_shares.0, "0.005");
        assert_eq!(released_quote_amount_pusd.0, "0.0025");
        assert_eq!(
            account_delta
                .as_ref()
                .map(|delta| delta.account_transition_seq),
            Some(2)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_gtd_expiration_releases_only_open_quote_before_same_time_finality()
    {
        let (mut state, condition_id, token_id) = execution_state_v2_for_resting_limit_tests("10");
        state
            .apply_initial_levels(
                &condition_id.0,
                &token_id.0,
                &[],
                &[(decimal("0.4"), 5_000_000)],
            )
            .expect("GTD 到期测试 ask");
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = TradeFinalityPolicyV2::decode_and_seal(
            br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":298500},{"state":"CONFIRMED","offset_ms":298510}],"economic_application_ordinal":1},"failure_overrides":[]}"#,
        )
        .expect("GTD 同刻最终性测试策略必须合法");
        let expires_at_utc_ms = 300_000;
        let (state, disposition) = state
            .apply_buy_order(
                BuyOrderActionV2 {
                    global_request_id: "v2-gtd-expiration".to_owned(),
                    batch_item_ordinal: 0,
                    item_id: "v2-gtd-expiration-item".to_owned(),
                    condition_id: condition_id.clone(),
                    token_id: token_id.clone(),
                    effective_processing_at_utc_ms: 1_500,
                    terms: BuyOrderTermsV2::Gtd {
                        limit_price_pusd_per_share: decimal("0.5"),
                        accepted_base_units: 10_000_000,
                        accepted_quote_units: 5_000_000,
                        post_only: false,
                        expires_at_utc_ms: Some(expires_at_utc_ms),
                    },
                },
                rules.snapshot(),
                &finality_policy,
            )
            .expect("部分成交 GTD 必须接受");
        let BuyOrderDispositionV2::Accepted(accepted) = disposition else {
            panic!("部分成交 GTD 必须形成订单");
        };
        assert_eq!(accepted.account_deltas[0].account_transition_seq, 1);
        assert_eq!(state.next_gtd_deadline_at_utc_ms(), Some(expires_at_utc_ms));
        assert_eq!(
            state.next_scheduled_finality_at_utc_ms(),
            Some(expires_at_utc_ms)
        );
        assert_eq!(state.account.principal_encumbrance_pusd(), decimal("4.5"));
        assert_eq!(
            state.account.immediate_taker_fee_encumbrance_pusd(),
            decimal("0.084")
        );

        let (state, early) = state
            .advance_next_gtd_deadline_at(expires_at_utc_ms - 1)
            .expect("到期前推进必须无效果");
        assert!(early.expiration.is_none());
        assert!(early.account_deltas.is_empty());
        assert_eq!(state.next_gtd_deadline_at_utc_ms(), Some(expires_at_utc_ms));

        let (state, expired) = state
            .advance_next_gtd_deadline_at(expires_at_utc_ms)
            .expect("GTD 必须在编码截止时点到期");
        let expiration = expired.expiration.as_ref().expect("必须形成 GTD 到期效果");
        assert_eq!(expiration.platform_order_id, "sim-1");
        assert_eq!(expiration.pending_base_units_before, 0);
        assert_eq!(expiration.open_base_units_before, 5_000_000);
        assert_eq!(expiration.released_open_quote_units, 2_500_000);
        assert_eq!(expiration.account_transition_seq, Some(2));
        assert_eq!(expired.account_deltas.len(), 1);
        assert_eq!(expired.account_deltas[0].account_transition_seq, 2);
        assert_eq!(expired.resources.removed_records, 4);
        assert_eq!(expired.resources.removed_bytes, 2 * "sim-1".len() as u64);
        assert_eq!(
            expired.account_deltas[0].reason,
            contracts::PlatformAccountDeltaReasonV2::GtdExpirationEncumbranceReleased {
                platform_order_id: "sim-1".to_owned(),
                encoded_expires_at_utc_ms: expires_at_utc_ms,
            }
        );
        assert_eq!(
            state.orders["sim-1"].quantities,
            OrderQuantityStateV2::Base {
                accepted_base_units: 10_000_000,
                matched_base_units: 5_000_000,
                pending_maker_depletion_base_units: 0,
                open_base_units: 0,
                venue_dust_closed_base_units: 0,
                canceled_base_units: 0,
                expired_base_units: 5_000_000,
                market_resolved_canceled_base_units: 0,
                accepted_quote_units: 5_000_000,
                matched_quote_units: 2_000_000,
                open_encumbered_quote_units: 0,
                released_price_improvement_quote_units: 500_000,
                venue_dust_released_quote_units: 0,
                canceled_released_quote_units: 0,
                expired_released_quote_units: 2_500_000,
                market_resolved_released_quote_units: 0,
            }
        );
        assert_eq!(state.next_gtd_deadline_at_utc_ms(), None);
        assert!(
            state
                .queues
                .asset(&condition_id.0, &token_id.0)
                .expect("GTD 到期后的目标队列")
                .bids
                .is_empty()
        );
        assert_eq!(
            state.account_overlay.quantity(
                &condition_id.0,
                &token_id.0,
                BookSide::Bid,
                decimal("0.5")
            ),
            Decimal::ZERO
        );
        assert_eq!(state.account.available_pusd(), decimal("7.916"));
        assert_eq!(state.account.principal_encumbrance_pusd(), decimal("2"));
        assert_eq!(
            state.account.immediate_taker_fee_encumbrance_pusd(),
            decimal("0.084")
        );

        let expiration_lifecycle = expired
            .into_lifecycle_feedback()
            .expect("GTD 到期必须形成自包含订单生命周期效果");
        let [LifecycleFeedbackEffectV2::Order(expiration_lifecycle)] =
            expiration_lifecycle.as_slice()
        else {
            panic!("一次 GTD 到期只能形成一个订单生命周期效果")
        };
        let OrderLifecycleEventV2::GtdExpired {
            encoded_expires_at_utc_ms,
            expired_base_quantity_shares,
            released_quote_amount_pusd,
            account_delta,
        } = &expiration_lifecycle.event
        else {
            panic!("GTD 到期必须保持自身方法特定效果")
        };
        assert_eq!(*encoded_expires_at_utc_ms, expires_at_utc_ms);
        assert_eq!(expired_base_quantity_shares.0, "5");
        assert_eq!(released_quote_amount_pusd.0, "2.5");
        assert_eq!(
            account_delta
                .as_ref()
                .map(|delta| delta.account_transition_seq),
            Some(2)
        );

        let account_sequence_before_cancel = state.account.next_account_transition_seq();
        let (state, cancel_after_expiration) = state
            .apply_caller_cancel("sim-1", expires_at_utc_ms)
            .expect("同刻 GTD 已先到期时撤单必须规范无效果");
        assert_eq!(
            cancel_after_expiration,
            CallerCancelDispositionV2::NoCancellableRemainder
        );
        assert_eq!(
            state.account.next_account_transition_seq(),
            account_sequence_before_cancel,
            "GTD 已释放开放本金后撤单不得重复释放"
        );

        let (state, mined) = state
            .advance_next_scheduled_finality_at(
                expires_at_utc_ms,
                &finality_policy,
                rules.snapshot(),
            )
            .expect("同刻最终性必须在 GTD 后应用成交经济变化");
        assert_eq!(
            mined
                .transition
                .as_ref()
                .expect("必须形成 MINED 转换")
                .trade_finality
                .fill_sequence,
            1
        );
        assert_eq!(mined.account_deltas[0].account_transition_seq, 3);
        assert_eq!(state.account.principal_encumbrance_pusd(), Decimal::ZERO);
        assert_eq!(
            state.account.immediate_taker_fee_encumbrance_pusd(),
            Decimal::ZERO
        );
        assert_eq!(
            state.orders["sim-1"].immediate_taker_fee_encumbered_units,
            0
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_same_time_gtd_expirations_consume_one_order_per_call() {
        let (state, condition_id, token_id) = execution_state_v2_for_resting_limit_tests("10");
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = crate::finality::successful_policy_v2_for_tests();
        let expires_at_utc_ms = 300_000;
        let order = |request: &str, item: &str, price: Decimal| BuyOrderActionV2 {
            global_request_id: request.to_owned(),
            batch_item_ordinal: 0,
            item_id: item.to_owned(),
            condition_id: condition_id.clone(),
            token_id: token_id.clone(),
            effective_processing_at_utc_ms: 1_500,
            terms: BuyOrderTermsV2::Gtd {
                limit_price_pusd_per_share: price,
                accepted_base_units: 5_000_000,
                accepted_quote_units: segment_quote_units_v1(5_000_000, price)
                    .expect("测试价格与数量必须可量化"),
                post_only: true,
                expires_at_utc_ms: Some(expires_at_utc_ms),
            },
        };
        let (state, first) = state
            .apply_buy_order(
                order(
                    "v2-gtd-same-time-a",
                    "v2-gtd-same-time-item-a",
                    decimal("0.4"),
                ),
                rules.snapshot(),
                &finality_policy,
            )
            .expect("首个同刻 GTD 必须接受");
        assert!(matches!(first, BuyOrderDispositionV2::Accepted(_)));
        let (state, second) = state
            .apply_buy_order(
                order(
                    "v2-gtd-same-time-b",
                    "v2-gtd-same-time-item-b",
                    decimal("0.5"),
                ),
                rules.snapshot(),
                &finality_policy,
            )
            .expect("第二个同刻 GTD 必须接受");
        assert!(matches!(second, BuyOrderDispositionV2::Accepted(_)));
        assert_eq!(state.next_gtd_deadline_at_utc_ms(), Some(expires_at_utc_ms));

        let (state, first_expired) = state
            .advance_next_gtd_deadline_at(expires_at_utc_ms)
            .expect("首个同刻 GTD 必须先到期");
        assert_eq!(
            first_expired
                .expiration
                .as_ref()
                .map(|effect| effect.platform_order_id.as_str()),
            Some("sim-1")
        );
        assert_eq!(state.next_gtd_deadline_at_utc_ms(), Some(expires_at_utc_ms));
        assert_eq!(state.orders["sim-2"].last_updated_at_utc_ms, 1_500);

        let (state, second_expired) = state
            .advance_next_gtd_deadline_at(expires_at_utc_ms)
            .expect("第二个同刻 GTD 必须随后到期");
        assert_eq!(
            second_expired
                .expiration
                .as_ref()
                .map(|effect| effect.platform_order_id.as_str()),
            Some("sim-2")
        );
        assert_eq!(state.next_gtd_deadline_at_utc_ms(), None);
        assert_eq!(first_expired.account_deltas[0].account_transition_seq, 3);
        assert_eq!(second_expired.account_deltas[0].account_transition_seq, 4);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_finality_applies_economics_then_accrues_taker_rebate_once() {
        let (mut state, condition_id, token_id) = execution_state_v2_for_resting_limit_tests("10");
        state
            .apply_initial_levels(
                &condition_id.0,
                &token_id.0,
                &[],
                &[(decimal("0.5"), 1_000_000)],
            )
            .expect("V2 最终性测试 ask");
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = crate::finality::successful_policy_v2_for_tests();
        let matched_at_utc_ms = 1_500;
        let (state, disposition) = state
            .apply_buy_order(
                BuyOrderActionV2 {
                    global_request_id: "v2-finality-success".to_owned(),
                    batch_item_ordinal: 0,
                    item_id: "v2-finality-success-item".to_owned(),
                    condition_id: condition_id.clone(),
                    token_id,
                    effective_processing_at_utc_ms: matched_at_utc_ms,
                    terms: BuyOrderTermsV2::Fok {
                        worst_price_pusd_per_share: decimal("0.5"),
                        accepted_quote_budget_units: 500_000,
                        sdk_taker_amount_audit_base_units: 1_000_000,
                    },
                },
                rules.snapshot(),
                &finality_policy,
            )
            .expect("V2 FOK 必须形成待最终性成交");
        let BuyOrderDispositionV2::Accepted(accepted) = disposition else {
            panic!("V2 FOK 必须接受");
        };
        assert_eq!(accepted.initial_trade_finality[0].fill_sequence, 1);
        assert_eq!(state.next_scheduled_finality_at_utc_ms(), Some(1_510));

        let (state, mined) = state
            .advance_next_scheduled_finality_at(1_510, &finality_policy, rules.snapshot())
            .expect("MINED 必须在唯一经济截点应用一次");
        let mined_transition = mined.transition.as_ref().expect("必须形成 MINED 转换");
        assert_eq!(mined_transition.trade_finality.fill_sequence, 1);
        assert_eq!(
            mined_transition.trade_finality.finality_state,
            TradeFinalityStateV1::Mined
        );
        assert_eq!(mined.account_deltas.len(), 1);
        assert_eq!(mined.account_deltas[0].account_transition_seq, 2);
        assert_eq!(
            state.fills[&1].economic_application_account_transition_seq,
            Some(2)
        );
        assert_eq!(
            state.orders["sim-1"].immediate_taker_fee_encumbered_units,
            0
        );
        assert_eq!(state.next_scheduled_finality_at_utc_ms(), Some(1_520));
        let mined_lifecycle = mined
            .into_lifecycle_feedback()
            .expect("MINED 必须把唯一经济变化交给成交生命周期效果");
        let [LifecycleFeedbackEffectV2::Trade(mined_lifecycle)] = mined_lifecycle.as_slice() else {
            panic!("MINED 转换只能形成一个成交生命周期效果")
        };
        assert_eq!(mined_lifecycle.path_ordinal, 1);
        assert_eq!(
            mined_lifecycle.status.known,
            Some(SimulatedTradeKnownStatusV2::Mined)
        );
        assert_eq!(
            mined_lifecycle.economic_application_account_transition_seq,
            Some(2)
        );
        assert_eq!(
            mined_lifecycle
                .account_delta
                .as_ref()
                .map(|delta| delta.account_transition_seq),
            Some(2)
        );

        let (state, confirmed) = state
            .advance_next_scheduled_finality_at(1_520, &finality_policy, rules.snapshot())
            .expect("CONFIRMED 必须形成唯一 taker 应计");
        let confirmed_transition = confirmed
            .transition
            .as_ref()
            .expect("必须形成 CONFIRMED 转换");
        assert_eq!(
            confirmed_transition.trade_finality.finality_state,
            TradeFinalityStateV1::Confirmed
        );
        let [EconomicLifecycleEffectV2::TakerAccrual(accrual)] =
            confirmed_transition.settlement.as_slice()
        else {
            panic!("CONFIRMED 必须只形成唯一 taker 应计")
        };
        assert_eq!(accrual.fill_sequence, 1);
        assert_eq!(accrual.accrual_pusd, decimal("0.0014"));
        assert_eq!(accrual.account_transition_seq, 3);
        assert_eq!(confirmed.account_deltas.len(), 1);
        assert_eq!(confirmed.account_deltas[0].account_transition_seq, 3);
        assert_eq!(state.taker_rebate.days[&0].pending_predecessor_count, 0);
        assert_eq!(state.taker_rebate.days[&0].accrued_pusd, decimal("0.0014"));
        assert_eq!(state.next_scheduled_finality_at_utc_ms(), None);
        assert_eq!(state.next_rebate_clock_at_utc_ms(), Some(2_000));
        let confirmed_lifecycle = confirmed
            .into_lifecycle_feedback()
            .expect("CONFIRMED 必须按成交终态、taker 应计形成生命周期效果");
        let [
            LifecycleFeedbackEffectV2::Trade(confirmed_trade),
            LifecycleFeedbackEffectV2::Settlement(confirmed_accrual),
        ] = confirmed_lifecycle.as_slice()
        else {
            panic!("CONFIRMED 必须先形成成交终态，再形成 taker 应计")
        };
        assert_eq!(
            confirmed_trade.status.known,
            Some(SimulatedTradeKnownStatusV2::Confirmed)
        );
        assert_eq!(
            confirmed_trade.economic_application_account_transition_seq,
            Some(2)
        );
        assert!(confirmed_trade.account_delta.is_none());
        let SettlementLifecycleEventV2::TakerRebateAccrualApplied { account_delta, .. } =
            &confirmed_accrual.event
        else {
            panic!("CONFIRMED 的第二个效果必须是 taker 应计")
        };
        assert_eq!(account_delta.account_transition_seq, 3);

        let (state, maker_end) = state
            .advance_next_rebate_clock_at(2_000)
            .expect("maker 市场结束时钟必须先形成零成交终态");
        assert!(matches!(
            maker_end.settlement.as_slice(),
            [EconomicLifecycleEffectV2::MakerMarketFinalized(
                MakerRebateMarketFinalizedEffectV2 {
                    status: MakerRebateMarketResultStatusV1::ZeroNoEligibleMakerFill,
                    ..
                }
            )]
        ));
        let maker_end_lifecycle = maker_end
            .into_lifecycle_feedback()
            .expect("零合格 maker 成交仍必须形成应计终态效果");
        let [LifecycleFeedbackEffectV2::Settlement(maker_end_lifecycle)] =
            maker_end_lifecycle.as_slice()
        else {
            panic!("maker 零结果必须形成一个结算生命周期效果")
        };
        assert!(matches!(
            &maker_end_lifecycle.event,
            SettlementLifecycleEventV2::MakerRebateAccrualFinalized {
                status: MakerRebateAccrualStatusV2::ZeroNoEligibleMakerFill,
                account_delta: None,
                ..
            }
        ));
        let (state, taker_day) = state
            .advance_next_rebate_clock_at(87_000_000)
            .expect("终态先到时成熟时钟必须日结一次");
        assert!(matches!(
            taker_day.settlement.as_slice(),
            [EconomicLifecycleEffectV2::DayFinalized(
                RebateDayFinalizedEffectV2 {
                    kind: RebateKindV1::Taker,
                    attribution_day_utc_ms: 0,
                    payable_pusd,
                    account_transition_seq: None,
                    ..
                }
            )] if *payable_pusd == Decimal::ZERO
        ));
        assert!(taker_day.account_deltas.is_empty());
        let taker_day_lifecycle = taker_day
            .into_lifecycle_feedback()
            .expect("低于付款门槛的 taker 日结仍必须形成终态效果");
        let [LifecycleFeedbackEffectV2::Settlement(taker_day_lifecycle)] =
            taker_day_lifecycle.as_slice()
        else {
            panic!("低于付款门槛的 taker 日结只能形成一个结算效果")
        };
        assert!(matches!(
            &taker_day_lifecycle.event,
            SettlementLifecycleEventV2::TakerRebateDayFinalized {
                payable_pusd,
                account_delta: None,
                ..
            } if payable_pusd.0 == "0"
        ));
        assert_eq!(state.account.next_account_transition_seq(), 4);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_same_time_failures_compensate_before_unapplied_release() {
        let (mut state, condition_id, token_id) = execution_state_v2_for_resting_limit_tests("10");
        state
            .apply_initial_levels(
                &condition_id.0,
                &token_id.0,
                &[],
                &[(decimal("0.4"), 1_000_000), (decimal("0.5"), 1_000_000)],
            )
            .expect("V2 双成交失败测试 ask");
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let account_instance_key = state.account.account_instance_key().clone();
        let policy_bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": "trade_finality_policy_v2",
            "normal_path": {
                "steps": [
                    { "state": "MATCHED", "offset_ms": 0 },
                    { "state": "MINED", "offset_ms": 10 },
                    { "state": "CONFIRMED", "offset_ms": 20 }
                ],
                "economic_application_ordinal": 1
            },
            "failure_overrides": [
                {
                    "selector": {
                        "selector_domain": "FILL",
                        "account_instance_key": account_instance_key,
                        "fill_sequence": 2
                    },
                    "path": {
                        "steps": [
                            { "state": "MATCHED", "offset_ms": 0 },
                            { "state": "FAILED", "offset_ms": 20 }
                        ],
                        "economic_application_ordinal": null
                    }
                },
                {
                    "selector": {
                        "selector_domain": "FILL",
                        "account_instance_key": state.account.account_instance_key(),
                        "fill_sequence": 1
                    },
                    "path": {
                        "steps": [
                            { "state": "MATCHED", "offset_ms": 0 },
                            { "state": "MINED", "offset_ms": 10 },
                            { "state": "FAILED", "offset_ms": 20 }
                        ],
                        "economic_application_ordinal": 1
                    }
                }
            ]
        }))
        .expect("V2 双成交失败策略必须可编码");
        let finality_policy = TradeFinalityPolicyV2::decode_and_seal(&policy_bytes)
            .expect("V2 双成交失败策略必须合法");
        let matched_at_utc_ms = 1_500;
        let (state, disposition) = state
            .apply_buy_order(
                BuyOrderActionV2 {
                    global_request_id: "v2-finality-failures".to_owned(),
                    batch_item_ordinal: 0,
                    item_id: "v2-finality-failures-item".to_owned(),
                    condition_id: condition_id.clone(),
                    token_id,
                    effective_processing_at_utc_ms: matched_at_utc_ms,
                    terms: BuyOrderTermsV2::Fok {
                        worst_price_pusd_per_share: decimal("0.5"),
                        accepted_quote_budget_units: 900_000,
                        sdk_taker_amount_audit_base_units: 1_800_000,
                    },
                },
                rules.snapshot(),
                &finality_policy,
            )
            .expect("V2 FOK 必须形成两笔待最终性成交");
        let BuyOrderDispositionV2::Accepted(accepted) = disposition else {
            panic!("V2 双成交 FOK 必须接受");
        };
        assert_eq!(
            accepted
                .initial_trade_finality
                .iter()
                .map(|effect| effect.fill_sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(state.next_scheduled_finality_at_utc_ms(), Some(1_510));

        let (state, mined) = state
            .advance_next_scheduled_finality_at(1_510, &finality_policy, rules.snapshot())
            .expect("第一笔 MINED 必须应用经济变化");
        assert_eq!(
            mined
                .transition
                .as_ref()
                .expect("必须形成第一笔 MINED 转换")
                .trade_finality
                .fill_sequence,
            1
        );
        assert_eq!(mined.account_deltas.len(), 1);
        assert_eq!(mined.account_deltas[0].account_transition_seq, 2);
        let mined_lifecycle = mined
            .into_lifecycle_feedback()
            .expect("失败轨迹的 MINED 必须形成唯一成交生命周期效果");
        let [LifecycleFeedbackEffectV2::Trade(mined_trade)] = mined_lifecycle.as_slice() else {
            panic!("失败轨迹的 MINED 只能形成一个成交效果")
        };
        assert_eq!(
            mined_trade
                .account_delta
                .as_ref()
                .map(|delta| delta.account_transition_seq),
            Some(2)
        );

        let (state, compensated) = state
            .advance_next_scheduled_finality_at(1_520, &finality_policy, rules.snapshot())
            .expect("同刻第一笔失败必须先完成补偿");
        let compensated_transition = compensated
            .transition
            .as_ref()
            .expect("必须形成第一笔 FAILED 转换");
        assert_eq!(compensated_transition.trade_finality.fill_sequence, 1);
        assert_eq!(
            compensated_transition.trade_finality.economic_disposition,
            TradeEconomicDispositionStateV1::FinalFailureCompensated
        );
        assert!(compensated_transition.settlement.is_empty());
        assert_eq!(compensated.account_deltas.len(), 1);
        assert_eq!(compensated.account_deltas[0].account_transition_seq, 3);
        assert_eq!(
            compensated.account_deltas[0].reason,
            contracts::PlatformAccountDeltaReasonV2::FillFinalityCompensationApplied {
                trade_id: "sim-1".to_owned(),
                path_ordinal: 2,
            }
        );
        let compensated_lifecycle = compensated
            .into_lifecycle_feedback()
            .expect("已应用失败必须把补偿正文交给同一成交终态效果");
        let [LifecycleFeedbackEffectV2::Trade(compensated_trade)] =
            compensated_lifecycle.as_slice()
        else {
            panic!("已应用失败只能形成一个成交终态效果")
        };
        assert_eq!(
            compensated_trade.status.known,
            Some(SimulatedTradeKnownStatusV2::Failed)
        );
        assert_eq!(
            compensated_trade.economic_disposition,
            TradeEconomicDispositionV2::FinalFailureCompensated
        );
        assert_eq!(
            compensated_trade.economic_application_account_transition_seq,
            Some(2)
        );
        assert_eq!(
            compensated_trade
                .account_delta
                .as_ref()
                .map(|delta| delta.account_transition_seq),
            Some(3)
        );
        assert_eq!(state.next_scheduled_finality_at_utc_ms(), Some(1_520));

        let (state, released) = state
            .advance_next_scheduled_finality_at(1_520, &finality_policy, rules.snapshot())
            .expect("同刻第二笔失败必须随后释放未应用占用");
        let released_transition = released
            .transition
            .as_ref()
            .expect("必须形成第二笔 FAILED 转换");
        assert_eq!(released_transition.trade_finality.fill_sequence, 2);
        assert_eq!(
            released_transition.trade_finality.economic_disposition,
            TradeEconomicDispositionStateV1::FinalFailureUnapplied
        );
        assert!(released_transition.settlement.is_empty());
        assert_eq!(released.account_deltas.len(), 1);
        assert_eq!(released.account_deltas[0].account_transition_seq, 4);
        assert_eq!(
            released.account_deltas[0].reason,
            contracts::PlatformAccountDeltaReasonV2::FillFinalityUnappliedFailureReleased {
                trade_id: "sim-2".to_owned(),
                path_ordinal: 1,
            }
        );
        let released_lifecycle = released
            .into_lifecycle_feedback()
            .expect("未应用失败必须把释放正文交给同一成交终态效果");
        let [LifecycleFeedbackEffectV2::Trade(released_trade)] = released_lifecycle.as_slice()
        else {
            panic!("未应用失败只能形成一个成交终态效果")
        };
        assert_eq!(
            released_trade.status.known,
            Some(SimulatedTradeKnownStatusV2::Failed)
        );
        assert_eq!(
            released_trade.economic_disposition,
            TradeEconomicDispositionV2::FinalFailureUnapplied
        );
        assert_eq!(
            released_trade.economic_application_account_transition_seq,
            None
        );
        assert_eq!(
            released_trade
                .account_delta
                .as_ref()
                .map(|delta| delta.account_transition_seq),
            Some(4)
        );
        assert_eq!(
            state.fills[&1].economic_application_account_transition_seq,
            Some(2)
        );
        assert_eq!(
            state.fills[&2].economic_application_account_transition_seq,
            None
        );
        assert_eq!(state.account.available_pusd(), decimal("10"));
        assert_eq!(state.account.principal_encumbrance_pusd(), Decimal::ZERO);
        assert_eq!(
            state.account.immediate_taker_fee_encumbrance_pusd(),
            Decimal::ZERO
        );
        assert_eq!(
            state.orders["sim-1"].immediate_taker_fee_encumbered_units,
            0
        );
        assert_eq!(state.taker_rebate.days[&0].pending_predecessor_count, 0);
        assert_eq!(state.taker_rebate.days[&0].accrued_pusd, Decimal::ZERO);
        assert_eq!(state.account.next_account_transition_seq(), 5);
        assert_eq!(state.next_scheduled_finality_at_utc_ms(), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_mature_taker_day_waits_for_terminal_then_settles_in_causal_order() {
        let (mut state, condition_id, token_id) =
            execution_state_v2_for_resting_limit_tests("517.50052");
        state
            .apply_initial_levels(
                &condition_id.0,
                &token_id.0,
                &[],
                &[(decimal("0.5"), 1_000_001_000)],
            )
            .expect("V2 taker 成熟屏障测试 ask");
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = TradeFinalityPolicyV2::decode_and_seal(
            br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10},{"state":"CONFIRMED","offset_ms":87000000}],"economic_application_ordinal":1},"failure_overrides":[]}"#,
        )
        .expect("V2 taker 成熟屏障测试策略必须合法");
        let matched_at_utc_ms = 1_500;
        let maturity_at_utc_ms = 87_000_000;
        let confirmed_at_utc_ms = 87_001_500;
        let (state, disposition) = state
            .apply_buy_order(
                BuyOrderActionV2 {
                    global_request_id: "v2-taker-maturity-before-terminal".to_owned(),
                    batch_item_ordinal: 0,
                    item_id: "v2-taker-maturity-before-terminal-item".to_owned(),
                    condition_id: condition_id.clone(),
                    token_id,
                    effective_processing_at_utc_ms: matched_at_utc_ms,
                    terms: BuyOrderTermsV2::Fok {
                        worst_price_pusd_per_share: decimal("0.5"),
                        accepted_quote_budget_units: 500_000_500,
                        sdk_taker_amount_audit_base_units: 1_000_001_000,
                    },
                },
                rules.snapshot(),
                &finality_policy,
            )
            .expect("V2 大额 FOK 必须形成一笔待最终性成交");
        let BuyOrderDispositionV2::Accepted(accepted) = disposition else {
            panic!("V2 大额 FOK 必须接受");
        };
        assert_eq!(accepted.initial_trade_finality[0].fill_sequence, 1);
        assert_eq!(state.fills[&1].immediate_taker_fee_units, 17_500_020);
        assert_eq!(state.next_rebate_clock_at_utc_ms(), Some(2_000));

        let (state, mined) = state
            .advance_next_scheduled_finality_at(1_510, &finality_policy, rules.snapshot())
            .expect("大额成交 MINED 必须应用经济变化");
        assert_eq!(mined.account_deltas.len(), 1);
        assert_eq!(mined.account_deltas[0].account_transition_seq, 2);
        assert_eq!(state.account.available_pusd(), Decimal::ZERO);

        let (state, maker_end) = state
            .advance_next_rebate_clock_at(2_000)
            .expect("maker 市场结束时钟必须先形成零成交终态");
        assert!(matches!(
            maker_end.settlement.as_slice(),
            [EconomicLifecycleEffectV2::MakerMarketFinalized(
                MakerRebateMarketFinalizedEffectV2 {
                    status: MakerRebateMarketResultStatusV1::ZeroNoEligibleMakerFill,
                    ..
                }
            )]
        ));
        let (state, maturity) = state
            .advance_next_rebate_clock_at(maturity_at_utc_ms)
            .expect("taker 成熟时钟必须只形成成熟屏障");
        assert!(maturity.settlement.is_empty());
        assert!(maturity.account_deltas.is_empty());
        assert_eq!(state.next_rebate_clock_at_utc_ms(), None);
        assert_eq!(state.account.next_account_transition_seq(), 3);
        assert_eq!(state.account.available_pusd(), Decimal::ZERO);
        assert_eq!(state.taker_rebate.days[&0].pending_predecessor_count, 1);
        assert_eq!(state.taker_rebate.days[&0].accrued_pusd, Decimal::ZERO);
        assert!(state.taker_rebate.days[&0].maturity_reached);

        let (state, confirmed) = state
            .advance_next_scheduled_finality_at(
                confirmed_at_utc_ms,
                &finality_policy,
                rules.snapshot(),
            )
            .expect("成熟后的 CONFIRMED 必须先形成唯一应计和 ready 日桶");
        let confirmed_transition = confirmed
            .transition
            .as_ref()
            .expect("必须形成 CONFIRMED 转换");
        assert_eq!(
            confirmed_transition.trade_finality.finality_state,
            TradeFinalityStateV1::Confirmed
        );
        let [
            EconomicLifecycleEffectV2::TakerAccrual(accrual),
            EconomicLifecycleEffectV2::DayFinalized(finalized),
            EconomicLifecycleEffectV2::Paid(paid),
        ] = confirmed_transition.settlement.as_slice()
        else {
            panic!("taker 终态必须按应计、日结、付款的因果顺序排空")
        };
        assert_eq!(accrual.fill_sequence, 1);
        assert_eq!(accrual.attribution_day_utc_ms, 0);
        assert_eq!(accrual.platform_effective_at_utc_ms, confirmed_at_utc_ms);
        assert_eq!(accrual.actual_taker_fee_pusd, decimal("17.50002"));
        assert_eq!(accrual.rebate_rate, decimal("0.08"));
        assert_eq!(accrual.accrual_pusd, decimal("1.4000016"));
        assert_eq!(accrual.account_transition_seq, 3);
        assert_eq!(finalized.kind, RebateKindV1::Taker);
        assert_eq!(finalized.attribution_day_utc_ms, 0);
        assert_eq!(finalized.accrued_pusd, decimal("1.4000016"));
        assert_eq!(finalized.payable_pusd, decimal("1.4"));
        assert_eq!(finalized.locked_remainder_pusd, decimal("0.0000016"));
        assert_eq!(finalized.account_transition_seq, Some(4));
        assert_eq!(paid.kind, RebateKindV1::Taker);
        assert_eq!(paid.paid_pusd, decimal("1.4"));
        assert_eq!(paid.account_transition_seq, 5);
        assert_eq!(confirmed.account_deltas.len(), 3);
        assert_eq!(
            confirmed
                .account_deltas
                .iter()
                .map(|delta| delta.account_transition_seq)
                .collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
        assert_eq!(
            confirmed.account_deltas[0].reason,
            contracts::PlatformAccountDeltaReasonV2::TakerRebateAccrualApplied {
                trade_id: "sim-1".to_owned(),
            }
        );
        assert_eq!(state.account.available_pusd(), decimal("1.4"));
        assert_eq!(state.account.next_account_transition_seq(), 6);
        assert_eq!(state.taker_rebate.days[&0].pending_predecessor_count, 0);
        assert_eq!(
            state.taker_rebate.days[&0].accrued_pusd,
            decimal("1.4000016")
        );

        let confirmed_lifecycle = confirmed
            .into_lifecycle_feedback()
            .expect("成熟后终态必须按成交、应计、日结、付款形成效果");
        let [
            LifecycleFeedbackEffectV2::Trade(confirmed_trade),
            LifecycleFeedbackEffectV2::Settlement(accrual_lifecycle),
            LifecycleFeedbackEffectV2::Settlement(day_lifecycle),
            LifecycleFeedbackEffectV2::Settlement(paid_lifecycle),
        ] = confirmed_lifecycle.as_slice()
        else {
            panic!("成熟后终态必须严格保持成交、应计、日结、付款因果顺序")
        };
        assert!(confirmed_trade.account_delta.is_none());
        let SettlementLifecycleEventV2::TakerRebateAccrualApplied { account_delta, .. } =
            &accrual_lifecycle.event
        else {
            panic!("第二个效果必须是 taker 应计")
        };
        assert_eq!(account_delta.account_transition_seq, 3);
        let SettlementLifecycleEventV2::TakerRebateDayFinalized {
            account_delta: Some(account_delta),
            ..
        } = &day_lifecycle.event
        else {
            panic!("第三个效果必须是带唯一账户变化的 taker 日结")
        };
        assert_eq!(account_delta.account_transition_seq, 4);
        let SettlementLifecycleEventV2::TakerRebatePaid { account_delta, .. } =
            &paid_lifecycle.event
        else {
            panic!("第四个效果必须是 taker 付款")
        };
        assert_eq!(account_delta.account_transition_seq, 5);

        let (state, repeated_clock) = state
            .advance_next_rebate_clock_at(maturity_at_utc_ms)
            .expect("已消费的成熟时钟不得重复日结或付款");
        assert!(repeated_clock.settlement.is_empty());
        assert!(repeated_clock.account_deltas.is_empty());
        let (state, repeated_finality) = state
            .advance_next_scheduled_finality_at(
                confirmed_at_utc_ms,
                &finality_policy,
                rules.snapshot(),
            )
            .expect("已闭合最终性不得重复产生变化");
        assert!(repeated_finality.transition.is_none());
        assert!(repeated_finality.account_deltas.is_empty());
        assert_eq!(state.account.next_account_transition_seq(), 6);
    }

    #[derive(Debug, PartialEq, Eq)]
    struct EconomicResolutionSummaryV2 {
        maker_matched_base_units: u128,
        maker_pending_base_units: u128,
        maker_open_base_units: u128,
        maker_market_resolved_base_units: u128,
        maker_matched_quote_units: u128,
        maker_market_resolved_quote_units: u128,
        cash_pusd: Decimal,
        available_pusd: Decimal,
        principal_encumbrance_pusd: Decimal,
        immediate_taker_fee_encumbrance_pusd: Decimal,
        maker_rebate_receivable_pusd: Decimal,
        taker_rebate_receivable_pusd: Decimal,
        maker_locked_remainder_pusd: Decimal,
        taker_locked_remainder_pusd: Decimal,
        outcome_balances: Vec<(String, Decimal)>,
        fill_finality_ordinals: Vec<(u64, u32, bool)>,
        effect_counts: [usize; 6],
        economic_record_delta: i128,
        economic_byte_delta: i128,
        next_account_transition_seq: u64,
    }

    fn observe_economic_effects_v2_for_tests(
        effects: &[EconomicLifecycleEffectV2],
        counts: &mut [usize; 6],
        resolution_event_id: &str,
        resolution_at_utc_ms: i64,
    ) {
        for effect in effects {
            match effect {
                EconomicLifecycleEffectV2::MarketResolutionOrder(effect) => {
                    counts[0] += 1;
                    assert_eq!(effect.platform_order_id, "sim-1");
                    assert_eq!(effect.resolution_event_id, resolution_event_id);
                    assert_eq!(effect.platform_effective_at_utc_ms, resolution_at_utc_ms);
                    assert_eq!(effect.pending_base_units_before, 1);
                    assert_eq!(effect.open_base_units_before, 4_999_999);
                    assert_eq!(effect.released_open_quote_units, 2_000_000);
                    assert!(effect.account_transition_seq.is_some());
                }
                EconomicLifecycleEffectV2::MakerMarketFinalized(effect) => {
                    counts[1] += 1;
                    assert_eq!(effect.status, MakerRebateMarketResultStatusV1::Calculated);
                    assert_eq!(effect.contributions.len(), 1);
                    assert_eq!(effect.accrual_pusd, decimal("1.008"));
                    assert!(effect.account_transition_seq.is_some());
                }
                EconomicLifecycleEffectV2::TakerAccrual(effect) => {
                    counts[2] += 1;
                    assert_eq!(effect.fill_sequence, 2);
                    assert_eq!(effect.actual_taker_fee_pusd, decimal("17.50002"));
                    assert_eq!(effect.rebate_rate, decimal("0.08"));
                    assert_eq!(effect.accrual_pusd, decimal("1.4000016"));
                }
                EconomicLifecycleEffectV2::AutoRedeem(effect) => {
                    counts[3] += 1;
                    assert_eq!(effect.resolution_event_id, resolution_event_id);
                    assert_eq!(effect.redeemed_pusd, decimal("1300.001"));
                    assert_eq!(effect.account_transition_seq, Some(8));
                }
                EconomicLifecycleEffectV2::DayFinalized(effect) => {
                    counts[4] += 1;
                    match effect.kind {
                        RebateKindV1::Maker => {
                            assert_eq!(effect.accrued_pusd, decimal("1.008"));
                            assert_eq!(effect.payable_pusd, decimal("1.008"));
                            assert_eq!(effect.locked_remainder_pusd, Decimal::ZERO);
                            assert_eq!(effect.account_transition_seq, None);
                        }
                        RebateKindV1::Taker => {
                            assert_eq!(effect.accrued_pusd, decimal("1.4000016"));
                            assert_eq!(effect.payable_pusd, decimal("1.4"));
                            assert_eq!(effect.locked_remainder_pusd, decimal("0.0000016"));
                            assert!(effect.account_transition_seq.is_some());
                        }
                    }
                }
                EconomicLifecycleEffectV2::Paid(effect) => {
                    counts[5] += 1;
                    match effect.kind {
                        RebateKindV1::Maker => assert_eq!(effect.paid_pusd, decimal("1.008")),
                        RebateKindV1::Taker => assert_eq!(effect.paid_pusd, decimal("1.4")),
                    }
                }
            }
        }
    }

    async fn economic_resolution_summary_v2_for_tests(
        resolution_first: bool,
    ) -> EconomicResolutionSummaryV2 {
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let finality_policy = TradeFinalityPolicyV2::decode_and_seal(
            br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":5000},{"state":"CONFIRMED","offset_ms":6000}],"economic_application_ordinal":2},"failure_overrides":[]}"#,
        )
        .expect("V2 判定顺序测试最终性策略必须合法");
        let (state, condition_id, token_id) =
            execution_state_v2_for_resting_limit_tests("639.50052");
        let (mut state, disposition) = state
            .apply_buy_order(
                BuyOrderActionV2 {
                    global_request_id: "v2-economic-maker".to_owned(),
                    batch_item_ordinal: 0,
                    item_id: "v2-economic-maker-item".to_owned(),
                    condition_id: condition_id.clone(),
                    token_id: token_id.clone(),
                    effective_processing_at_utc_ms: 1_200,
                    terms: BuyOrderTermsV2::Gtc {
                        limit_price_pusd_per_share: decimal("0.4"),
                        accepted_base_units: 305_000_000,
                        accepted_quote_units: 122_000_000,
                        post_only: false,
                    },
                },
                rules.snapshot(),
                &finality_policy,
            )
            .expect("建立 V2 判定顺序测试 maker 挂单");
        assert!(matches!(disposition, BuyOrderDispositionV2::Accepted(_)));

        for event in [
            decoded_level_event_at_v2_for_tests(
                BookSide::Bid,
                "0.4",
                "0",
                "300",
                1_300,
                1_301,
                100,
            ),
            decoded_level_event_at_v2_for_tests(
                BookSide::Bid,
                "0.4",
                "300",
                "0",
                1_310,
                1_311,
                101,
            ),
            decoded_level_event_at_v2_for_tests(
                BookSide::Bid,
                "0.4",
                "0",
                "0.000001",
                1_320,
                1_321,
                102,
            ),
            decoded_level_event_at_v2_for_tests(
                BookSide::Bid,
                "0.4",
                "0.000001",
                "0",
                1_330,
                1_331,
                103,
            ),
            decoded_level_event_at_v2_for_tests(
                BookSide::Ask,
                "0.5",
                "0",
                "1000.001",
                1_400,
                1_401,
                104,
            ),
        ] {
            let normalized = crate::market::normalize_market_event_v2(&event)
                .expect("一次规范化 V2 判定顺序测试档位事件");
            state
                .apply_market_event(
                    &condition_id.0,
                    &normalized,
                    event.time.source_event_at_utc_ms.expect("隐藏事件时点"),
                    2_000,
                    rules.snapshot(),
                    &finality_policy,
                )
                .expect("V2 判定顺序测试档位事件必须成功");
        }
        assert_eq!(state.fills[&1].base_units, 300_000_000);
        assert_eq!(state.fills[&1].quote_units, 120_000_000);
        let OrderQuantityStateV2::Base {
            matched_base_units,
            pending_maker_depletion_base_units,
            open_base_units,
            ..
        } = &state.orders["sim-1"].quantities
        else {
            panic!("maker GTC 必须使用 base 计量")
        };
        assert_eq!(*matched_base_units, 300_000_000);
        assert_eq!(*pending_maker_depletion_base_units, 1);
        assert_eq!(*open_base_units, 4_999_999);

        let (next, disposition) = state
            .apply_buy_order(
                BuyOrderActionV2 {
                    global_request_id: "v2-economic-taker".to_owned(),
                    batch_item_ordinal: 0,
                    item_id: "v2-economic-taker-item".to_owned(),
                    condition_id: condition_id.clone(),
                    token_id: token_id.clone(),
                    effective_processing_at_utc_ms: 1_500,
                    terms: BuyOrderTermsV2::Fok {
                        worst_price_pusd_per_share: decimal("0.5"),
                        accepted_quote_budget_units: 500_000_500,
                        sdk_taker_amount_audit_base_units: 1_000_001_000,
                    },
                },
                rules.snapshot(),
                &finality_policy,
            )
            .expect("V2 判定顺序测试 taker FOK 必须成交");
        state = next;
        assert!(matches!(disposition, BuyOrderDispositionV2::Accepted(_)));
        assert_eq!(state.fills[&2].base_units, 1_000_001_000);
        assert_eq!(state.fills[&2].quote_units, 500_000_500);
        assert_eq!(state.fills[&2].immediate_taker_fee_units, 17_500_020);

        let end_event =
            decoded_level_event_at_v2_for_tests(BookSide::Bid, "0.4", "0", "1", 2_000, 2_001, 105);
        let normalized = crate::market::normalize_market_event_v2(&end_event)
            .expect("一次规范化 V2 end_at 事件");
        state
            .apply_market_event(
                &condition_id.0,
                &normalized,
                2_000,
                2_000,
                rules.snapshot(),
                &finality_policy,
            )
            .expect("end_at 必须只结束撮合资格");

        let mut effect_counts = [0_usize; 6];
        let mut economic_resources = BusinessResourceDeltaV2::default();
        let (next, maker_end) = state
            .advance_next_rebate_clock_at(2_000)
            .expect("maker 市场结束时钟必须先建立终态屏障");
        state = next;
        assert!(maker_end.settlement.is_empty());
        assert_eq!(maker_end.resources.added_records, 0);
        assert_eq!(maker_end.resources.removed_records, 1);
        assert_eq!(maker_end.resources.added_bytes, 0);
        assert_eq!(
            maker_end.resources.removed_bytes,
            condition_id.0.len() as u64
        );
        economic_resources.merge(maker_end.resources);

        let scenario = crate::scenario::scenario_for_sim_tests();
        let member = &scenario.market_set().markets()[0];
        let terminal = MarketResolutionTerminalV3::Resolved {
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
            mapping_rule_version: "v2-economic-resolution-v1".to_owned(),
        };
        let resolution_event_id = "v2-economic-resolution";
        let resolution_at_utc_ms = if resolution_first { 4_000 } else { 8_000 };
        let resolution = QualifiedMarketResolutionV2 {
            resolution_event_id: resolution_event_id.to_owned(),
            resolution_source_event_at_utc_ms: 4_000,
            terminal: terminal.clone(),
            parsed_payouts: [Decimal::ONE, Decimal::ZERO],
        };
        let MarketResolutionTerminalV3::Resolved {
            winning_asset_id,
            winning_outcome,
            payout_vector,
            mapping_rule_version,
        } = &terminal;
        let resolution_owned_bytes = (condition_id.0.len()
            + resolution_event_id.len()
            + winning_asset_id.len()
            + winning_outcome.len()
            + mapping_rule_version.len()
            + payout_vector
                .iter()
                .map(|entry| {
                    entry.asset.token_id.0.len()
                        + entry.asset.outcome.label.len()
                        + entry.payout_pusd_per_share.0.len()
                })
                .sum::<usize>()) as u64;

        if resolution_first {
            let effects = state
                .apply_qualified_resolution(
                    &condition_id.0,
                    &resolution,
                    resolution_at_utc_ms,
                    &finality_policy,
                )
                .expect("最终性前到达的合格判定必须建立屏障");
            observe_economic_effects_v2_for_tests(
                &effects.settlement,
                &mut effect_counts,
                resolution_event_id,
                resolution_at_utc_ms,
            );
            assert!(matches!(
                effects.settlement.as_slice(),
                [EconomicLifecycleEffectV2::MarketResolutionOrder(_)]
            ));
            assert_eq!(effects.resources.added_records, 3);
            assert_eq!(effects.resources.removed_records, 1);
            assert_eq!(effects.resources.added_bytes, resolution_owned_bytes);
            assert_eq!(effects.resources.removed_bytes, 0);
            economic_resources.merge(effects.resources);
            let lifecycle = effects
                .into_lifecycle_feedback()
                .expect("最终性前判定必须冻结订单关闭生命周期效果");
            let [LifecycleFeedbackEffectV2::Order(order_lifecycle)] = lifecycle.as_slice() else {
                panic!("最终性前判定只能先形成订单关闭效果")
            };
            assert!(matches!(
                &order_lifecycle.event,
                OrderLifecycleEventV2::MarketResolvedCanceled {
                    resolution_event_id: event_id,
                    account_delta: Some(_),
                    ..
                } if event_id == resolution_event_id
            ));
            assert!(state.redeemed_market_resolution(&condition_id.0).is_none());
        }

        for platform_event_at_utc_ms in [6_310, 6_500, 7_310, 7_500] {
            let (next, effects) = state
                .advance_next_scheduled_finality_at(
                    platform_event_at_utc_ms,
                    &finality_policy,
                    rules.snapshot(),
                )
                .expect("V2 判定顺序测试最终性必须按稳定顺序推进");
            state = next;
            if let Some(transition) = &effects.transition {
                observe_economic_effects_v2_for_tests(
                    &transition.settlement,
                    &mut effect_counts,
                    resolution_event_id,
                    resolution_at_utc_ms,
                );
                match platform_event_at_utc_ms {
                    6_310 | 6_500 => assert!(transition.settlement.is_empty()),
                    7_310 => assert!(matches!(
                        transition.settlement.as_slice(),
                        [EconomicLifecycleEffectV2::MakerMarketFinalized(_)]
                    )),
                    7_500 if resolution_first => assert!(matches!(
                        transition.settlement.as_slice(),
                        [
                            EconomicLifecycleEffectV2::TakerAccrual(_),
                            EconomicLifecycleEffectV2::AutoRedeem(_)
                        ]
                    )),
                    7_500 => assert!(matches!(
                        transition.settlement.as_slice(),
                        [EconomicLifecycleEffectV2::TakerAccrual(_)]
                    )),
                    _ => unreachable!("判定顺序测试只推进四个固定最终性时点"),
                }
            }
            let expected_resources = match platform_event_at_utc_ms {
                6_310 | 6_500 => (1, 1, 0, 0),
                7_310 if resolution_first => (2, 5, 6, 5),
                7_310 => (2, 4, 6, 5),
                7_500 if resolution_first => (0, 2, 0, 0),
                7_500 => (0, 1, 0, 0),
                _ => unreachable!("判定顺序测试只推进四个固定最终性时点"),
            };
            assert_eq!(
                (
                    effects.resources.added_records,
                    effects.resources.removed_records,
                    effects.resources.added_bytes,
                    effects.resources.removed_bytes,
                ),
                expected_resources
            );
            economic_resources.merge(effects.resources);
            let lifecycle = effects
                .into_lifecycle_feedback()
                .expect("最终性转换必须形成因果有序的成交与结算效果");
            match platform_event_at_utc_ms {
                6_310 | 6_500 => assert!(matches!(
                    lifecycle.as_slice(),
                    [LifecycleFeedbackEffectV2::Trade(_)]
                )),
                7_310 => assert!(matches!(
                    lifecycle.as_slice(),
                    [
                        LifecycleFeedbackEffectV2::Trade(_),
                        LifecycleFeedbackEffectV2::Settlement(SettlementLifecycleEffectV2 {
                            event: SettlementLifecycleEventV2::MakerRebateAccrualFinalized {
                                status: MakerRebateAccrualStatusV2::Calculated,
                                ..
                            },
                            ..
                        })
                    ]
                )),
                7_500 if resolution_first => assert!(matches!(
                    lifecycle.as_slice(),
                    [
                        LifecycleFeedbackEffectV2::Trade(_),
                        LifecycleFeedbackEffectV2::Settlement(SettlementLifecycleEffectV2 {
                            event: SettlementLifecycleEventV2::TakerRebateAccrualApplied { .. },
                            ..
                        }),
                        LifecycleFeedbackEffectV2::Settlement(SettlementLifecycleEffectV2 {
                            event: SettlementLifecycleEventV2::AutoRedeemApplied { .. },
                            ..
                        })
                    ]
                )),
                7_500 => assert!(matches!(
                    lifecycle.as_slice(),
                    [
                        LifecycleFeedbackEffectV2::Trade(_),
                        LifecycleFeedbackEffectV2::Settlement(SettlementLifecycleEffectV2 {
                            event: SettlementLifecycleEventV2::TakerRebateAccrualApplied { .. },
                            ..
                        })
                    ]
                )),
                _ => unreachable!("判定顺序测试只推进四个固定最终性时点"),
            }
        }

        if !resolution_first {
            let effects = state
                .apply_qualified_resolution(
                    &condition_id.0,
                    &resolution,
                    resolution_at_utc_ms,
                    &finality_policy,
                )
                .expect("最终性后到达的合格判定必须立即完成赎回");
            observe_economic_effects_v2_for_tests(
                &effects.settlement,
                &mut effect_counts,
                resolution_event_id,
                resolution_at_utc_ms,
            );
            assert!(matches!(
                effects.settlement.as_slice(),
                [
                    EconomicLifecycleEffectV2::MarketResolutionOrder(_),
                    EconomicLifecycleEffectV2::AutoRedeem(_)
                ]
            ));
            assert_eq!(effects.resources.added_records, 1);
            assert_eq!(effects.resources.removed_records, 1);
            assert_eq!(effects.resources.added_bytes, resolution_owned_bytes);
            assert_eq!(effects.resources.removed_bytes, 0);
            economic_resources.merge(effects.resources);
            let lifecycle = effects
                .into_lifecycle_feedback()
                .expect("最终性后判定必须按订单关闭、自动赎回形成效果");
            assert!(matches!(
                lifecycle.as_slice(),
                [
                    LifecycleFeedbackEffectV2::Order(OrderLifecycleEffectV2 {
                        event: OrderLifecycleEventV2::MarketResolvedCanceled { .. },
                        ..
                    }),
                    LifecycleFeedbackEffectV2::Settlement(SettlementLifecycleEffectV2 {
                        event: SettlementLifecycleEventV2::AutoRedeemApplied { .. },
                        ..
                    })
                ]
            ));
        }
        assert_eq!(
            state.redeemed_market_resolution(&condition_id.0),
            Some(&terminal)
        );

        for platform_event_at_utc_ms in [87_000_000, 89_100_000] {
            let (next, effects) = state
                .advance_next_rebate_clock_at(platform_event_at_utc_ms)
                .expect("V2 maker/taker 成熟时钟必须各消费一次");
            state = next;
            observe_economic_effects_v2_for_tests(
                &effects.settlement,
                &mut effect_counts,
                resolution_event_id,
                resolution_at_utc_ms,
            );
            assert!(matches!(
                effects.settlement.as_slice(),
                [
                    EconomicLifecycleEffectV2::DayFinalized(_),
                    EconomicLifecycleEffectV2::Paid(_)
                ]
            ));
            assert_eq!(effects.resources.added_records, 0);
            assert_eq!(effects.resources.removed_records, 1);
            assert_eq!(effects.resources.added_bytes, 0);
            assert_eq!(effects.resources.removed_bytes, 0);
            economic_resources.merge(effects.resources);
            let lifecycle = effects
                .into_lifecycle_feedback()
                .expect("成熟时钟必须按日结、付款形成效果");
            assert!(matches!(
                lifecycle.as_slice(),
                [
                    LifecycleFeedbackEffectV2::Settlement(SettlementLifecycleEffectV2 {
                        event: SettlementLifecycleEventV2::MakerRebateDayFinalized { .. }
                            | SettlementLifecycleEventV2::TakerRebateDayFinalized { .. },
                        ..
                    }),
                    LifecycleFeedbackEffectV2::Settlement(SettlementLifecycleEffectV2 {
                        event: SettlementLifecycleEventV2::MakerRebatePaid { .. }
                            | SettlementLifecycleEventV2::TakerRebatePaid { .. },
                        ..
                    })
                ]
            ));
        }
        assert_eq!(effect_counts, [1, 1, 1, 1, 2, 2]);

        let repeated_resolution = state
            .apply_qualified_resolution(
                &condition_id.0,
                &resolution,
                resolution_at_utc_ms,
                &finality_policy,
            )
            .expect("相同判定重到必须是规范无效果");
        assert_eq!(
            repeated_resolution,
            ExecutionMarketEventEffectsV2::default()
        );
        let (state, repeated_finality) = state
            .advance_next_scheduled_finality_at(7_500, &finality_policy, rules.snapshot())
            .expect("最终性重到不得重复经济变化");
        assert!(repeated_finality.transition.is_none());
        let (state, repeated_clock) = state
            .advance_next_rebate_clock_at(89_100_000)
            .expect("返佣时钟重到不得重复结算");
        assert_eq!(repeated_clock, RebateClockAdvanceEffectsV2::default());

        let OrderQuantityStateV2::Base {
            matched_base_units,
            pending_maker_depletion_base_units,
            open_base_units,
            market_resolved_canceled_base_units,
            matched_quote_units,
            market_resolved_released_quote_units,
            ..
        } = &state.orders["sim-1"].quantities
        else {
            unreachable!("maker GTC 必须使用 base 计量")
        };
        let (
            cash_pusd,
            available_pusd,
            maker_rebate_receivable_pusd,
            taker_rebate_receivable_pusd,
            maker_locked_remainder_pusd,
            taker_locked_remainder_pusd,
            outcome_balances,
        ) = state.account.economic_balances_for_tests();
        assert_eq!(
            state.maker_rebate.markets[&condition_id.0].result,
            Some(MakerRebateMarketResultStatusV1::Calculated)
        );
        assert!(
            state.maker_rebate.days[&0]
                .final_state
                .as_ref()
                .is_some_and(|final_state| final_state.paid)
        );
        assert!(
            state.taker_rebate.days[&0]
                .final_state
                .as_ref()
                .is_some_and(|final_state| final_state.paid)
        );
        EconomicResolutionSummaryV2 {
            maker_matched_base_units: *matched_base_units,
            maker_pending_base_units: *pending_maker_depletion_base_units,
            maker_open_base_units: *open_base_units,
            maker_market_resolved_base_units: *market_resolved_canceled_base_units,
            maker_matched_quote_units: *matched_quote_units,
            maker_market_resolved_quote_units: *market_resolved_released_quote_units,
            cash_pusd,
            available_pusd,
            principal_encumbrance_pusd: state.account.principal_encumbrance_pusd(),
            immediate_taker_fee_encumbrance_pusd: state
                .account
                .immediate_taker_fee_encumbrance_pusd(),
            maker_rebate_receivable_pusd,
            taker_rebate_receivable_pusd,
            maker_locked_remainder_pusd,
            taker_locked_remainder_pusd,
            outcome_balances,
            fill_finality_ordinals: state
                .fills
                .iter()
                .map(|(sequence, fill)| {
                    (
                        *sequence,
                        fill.finality_path_ordinal,
                        fill.economic_application_account_transition_seq.is_some(),
                    )
                })
                .collect(),
            effect_counts,
            economic_record_delta: i128::from(economic_resources.added_records)
                - i128::from(economic_resources.removed_records),
            economic_byte_delta: i128::from(economic_resources.added_bytes)
                - i128::from(economic_resources.removed_bytes),
            next_account_transition_seq: state.account.next_account_transition_seq(),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_resolution_and_finality_order_converge_after_one_economic_closure()
    {
        let resolution_first = economic_resolution_summary_v2_for_tests(true).await;
        let finality_first = economic_resolution_summary_v2_for_tests(false).await;
        assert_eq!(resolution_first, finality_first);
        assert_eq!(
            resolution_first,
            EconomicResolutionSummaryV2 {
                maker_matched_base_units: 300_000_000,
                maker_pending_base_units: 0,
                maker_open_base_units: 0,
                maker_market_resolved_base_units: 5_000_000,
                maker_matched_quote_units: 120_000_000,
                maker_market_resolved_quote_units: 2_000_000,
                cash_pusd: decimal("1304.409"),
                available_pusd: decimal("1304.409"),
                principal_encumbrance_pusd: Decimal::ZERO,
                immediate_taker_fee_encumbrance_pusd: Decimal::ZERO,
                maker_rebate_receivable_pusd: Decimal::ZERO,
                taker_rebate_receivable_pusd: Decimal::ZERO,
                maker_locked_remainder_pusd: Decimal::ZERO,
                taker_locked_remainder_pusd: decimal("0.0000016"),
                outcome_balances: vec![("1".to_owned(), Decimal::ZERO)],
                fill_finality_ordinals: vec![(1, 2, true), (2, 2, true)],
                effect_counts: [1, 1, 1, 1, 2, 2],
                economic_record_delta: -6,
                economic_byte_delta: 61,
                next_account_transition_seq: 12,
            }
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn execution_state_v2_zero_balance_resolution_redeems_once_without_account_delta() {
        let (state, condition_id, _) = execution_state_v2_for_resting_limit_tests("10");
        let (state, maker_end) = state
            .advance_next_rebate_clock_at(2_000)
            .expect("零成交市场结束必须形成 maker 零结果");
        assert!(matches!(
            maker_end.settlement.as_slice(),
            [EconomicLifecycleEffectV2::MakerMarketFinalized(
                MakerRebateMarketFinalizedEffectV2 {
                    status: MakerRebateMarketResultStatusV1::ZeroNoEligibleMakerFill,
                    account_transition_seq: None,
                    ..
                }
            )]
        ));
        assert!(maker_end.account_deltas.is_empty());
        assert_eq!(maker_end.resources.added_records, 0);
        assert_eq!(maker_end.resources.removed_records, 1);
        assert_eq!(maker_end.resources.added_bytes, 0);
        assert_eq!(
            maker_end.resources.removed_bytes,
            condition_id.0.len() as u64
        );
        let maker_end_lifecycle = maker_end
            .into_lifecycle_feedback()
            .expect("零成交 maker 市场仍必须形成终态生命周期效果");
        let [LifecycleFeedbackEffectV2::Settlement(maker_end_lifecycle)] =
            maker_end_lifecycle.as_slice()
        else {
            panic!("零成交 maker 市场只能形成一个结算终态效果")
        };
        assert!(matches!(
            &maker_end_lifecycle.event,
            SettlementLifecycleEventV2::MakerRebateAccrualFinalized {
                status: MakerRebateAccrualStatusV2::ZeroNoEligibleMakerFill,
                contributions,
                account_delta: None,
                ..
            } if contributions.is_empty()
        ));

        let scenario = crate::scenario::scenario_for_sim_tests();
        let member = &scenario.market_set().markets()[0];
        let terminal = MarketResolutionTerminalV3::Resolved {
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
            mapping_rule_version: "v2-zero-balance-resolution-v1".to_owned(),
        };
        let resolution = QualifiedMarketResolutionV2 {
            resolution_event_id: "v2-zero-balance-resolution".to_owned(),
            resolution_source_event_at_utc_ms: 4_000,
            terminal: terminal.clone(),
            parsed_payouts: [Decimal::ONE, Decimal::ZERO],
        };
        let finality_policy = crate::finality::successful_policy_v2_for_tests();
        let account_sequence_before = state.account.next_account_transition_seq();
        let mut state = state;
        let effects = state
            .apply_qualified_resolution(&condition_id.0, &resolution, 4_000, &finality_policy)
            .expect("零余额合格判定仍必须形成一次赎回终态");
        let [EconomicLifecycleEffectV2::AutoRedeem(redeem)] = effects.settlement.as_slice() else {
            panic!("零余额合格判定必须只形成一次自动赎回终态")
        };
        assert_eq!(redeem.resolution_event_id, resolution.resolution_event_id);
        assert_eq!(redeem.redeemed_pusd, Decimal::ZERO);
        assert_eq!(redeem.account_transition_seq, None);
        assert_eq!(redeem.redeemed_outcome_balances.len(), 2);
        assert!(
            redeem
                .redeemed_outcome_balances
                .iter()
                .all(|balance| balance.balance_shares.0 == "0")
        );
        assert!(effects.account_deltas.is_empty());
        let MarketResolutionTerminalV3::Resolved {
            winning_asset_id,
            winning_outcome,
            payout_vector,
            mapping_rule_version,
        } = &terminal;
        let expected_resolution_bytes = (condition_id.0.len()
            + resolution.resolution_event_id.len()
            + winning_asset_id.len()
            + winning_outcome.len()
            + mapping_rule_version.len()
            + payout_vector
                .iter()
                .map(|entry| {
                    entry.asset.token_id.0.len()
                        + entry.asset.outcome.label.len()
                        + entry.payout_pusd_per_share.0.len()
                })
                .sum::<usize>()) as u64;
        assert_eq!(effects.resources.added_records, 1);
        assert_eq!(effects.resources.removed_records, 0);
        assert_eq!(effects.resources.added_bytes, expected_resolution_bytes);
        assert_eq!(effects.resources.removed_bytes, 0);
        assert_eq!(
            state.account.next_account_transition_seq(),
            account_sequence_before
        );
        assert_eq!(
            state.redeemed_market_resolution(&condition_id.0),
            Some(&terminal)
        );
        let redemption_lifecycle = effects
            .into_lifecycle_feedback()
            .expect("零余额赎回必须形成自包含结算终态效果");
        let [LifecycleFeedbackEffectV2::Settlement(redemption_lifecycle)] =
            redemption_lifecycle.as_slice()
        else {
            panic!("零余额判定只能形成一个自动赎回生命周期效果")
        };
        let SettlementLifecycleEventV2::AutoRedeemApplied {
            resolution_event_id,
            redeemed_outcome_balances,
            redeemed_pusd,
            account_delta,
            ..
        } = &redemption_lifecycle.event
        else {
            panic!("零余额判定必须形成自动赎回效果")
        };
        assert_eq!(resolution_event_id, &resolution.resolution_event_id);
        assert_eq!(redeemed_pusd.0, "0");
        assert_eq!(redeemed_outcome_balances.len(), 2);
        assert!(account_delta.is_none());

        let repeated = state
            .apply_qualified_resolution(&condition_id.0, &resolution, 4_000, &finality_policy)
            .expect("零余额判定重到必须是规范无效果");
        assert_eq!(repeated, ExecutionMarketEventEffectsV2::default());
        assert_eq!(
            state.account.next_account_transition_seq(),
            account_sequence_before
        );
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
    fn taker_fee_allocation_v2_breaks_equal_remainders_by_numeric_fill_sequence() {
        let fills = [(10_u64, 200, decimal("0.5")), (9_u64, 200, decimal("0.5"))];
        let (total, allocations) =
            allocate_taker_fee_micro_units_by_key(&fills, decimal("0.07"), 1)
                .expect("V2 数值成交序号手续费分配");
        assert_eq!(total, 10);
        assert_eq!(allocations, vec![0, 10]);

        let reversed = [fills[1], fills[0]];
        let (reversed_total, reversed_allocations) =
            allocate_taker_fee_micro_units_by_key(&reversed, decimal("0.07"), 1)
                .expect("扰动后的 V2 数值成交序号手续费分配");
        assert_eq!(reversed_total, total);
        assert_eq!(reversed_allocations, vec![10, 0]);
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
