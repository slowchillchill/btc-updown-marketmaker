/// 原SIM订单序号对应的平台身份；GPU公开读取沿同一构造。
pub fn platform_order_id_v1(order_sequence: u64) -> String {
    format!("sim-{order_sequence}")
}

pub mod settlement;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

use contracts::{
    AcceptedSubmissionItemV2, AccountInstanceKeyV1, BookSide, CanceledTargetMeasurementV2,
    ConditionId, ExactDecimal, MakerRebateAccrualStatusV2, MakerRebateFillContributionV2,
    MarketResolutionTerminalV3, OrderKindProjectionV2, OrderLifecycleEventV2, OrderProjectionV2,
    OrderQuantityProjectionV2, PlatformAccountDeltaReasonV2, PlatformAccountDeltaV2,
    PlatformOutcomeBalanceV1, QualityScopeV1, SettlementLifecycleEventV2, SimOrderTerminalReasonV2,
    SimulatedOrderKnownStatusV2, SimulatedOrderStatusV2, SimulatedTradeKnownStatusV2,
    SimulatedTradeStatusV2, SubmissionFillV2,
    SubmissionMeasurementV2 as ContractSubmissionMeasurementV2, TokenId,
    TradeEconomicDispositionV2, TradeLiquidityRoleV2, TradeProjectionV2,
};
#[cfg(test)]
use contracts::{
    ExecutionMarketEventPayloadV3, ExecutionMarketEventV3, LevelExecutionTransitionV3, Sha256Hex,
};
use num_bigint::{BigInt, BigUint};
use num_integer::Integer;
use num_traits::{One, ToPrimitive, Zero};
use polymarket_client_sdk_v2::types::U256;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::account::{
    AutoRedeemAccountEffectV2, FillEconomicAmountsV1, PlatformAccountErrorV1,
    PlatformAccountStateV2,
};
use crate::finality::{SelectedTradeFinalityPathV2, TradeFinalityPolicyV2, TradeFinalityStateV1};
use crate::market::{
    AccountBookOverlayStoreV1, NormalizedMarketEventV2, QualifiedMarketResolutionV2,
};
use crate::market_directory::SessionMarketDirectoryV1;
use crate::platform_rules::PlatformRuleSnapshotV3;
use crate::rebate::{
    MakerRebateContributionV1, MakerRebateMarketResultStatusV1, RebateDayFinalStateV1,
    RebateKindV1, RebateStateErrorV1, finalized_day_state, maker_rebate_contribution_v1,
    rebate_maturity_at, utc_day_start,
};
use crate::scenario::PlatformMarketV1;

const MICRO_SCALE: u32 = 6;
const FEE_SCALE: u32 = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchingMathErrorV1 {
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
pub enum MatchingStateErrorV1 {
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
}

#[derive(Debug, Default, PartialEq, Eq)]
struct AssetQueueEffectsV2 {
    maker_depletions: Vec<MakerQueueDepletionV2>,
    venue_dust_candidate: Option<AssetVenueDustCandidateV2>,
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
    ) {
        self.bids
            .entry(price)
            .or_default()
            .segments
            .push_back(QueueSegmentV2::Account {
                platform_order_id,
                remaining_base_units: base_units,
            });
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
                })
            }
            Some(QueueSegmentV2::External { .. }) | Some(QueueSegmentV2::Account { .. }) | None => {
                None
            }
        }
    }

    fn remove_account_order(&mut self, platform_order_id: &str) {
        self.bids.retain(|_, queue| {
            queue.segments.retain(|segment| {
                let remove = matches!(segment, QueueSegmentV2::Account {
                    platform_order_id: existing,
                    ..
                } if existing == platform_order_id);
                !remove
            });
            let keep = !queue.segments.is_empty() || queue.account_taker_depletion_debt_units > 0;
            keep
        });
    }

    fn remove_account_order_at_price(
        &mut self,
        price: Decimal,
        platform_order_id: &str,
    ) -> Option<()> {
        let queue = self.bids.get_mut(&price)?;
        let index = queue.segments.iter().position(|segment| {
            matches!(segment, QueueSegmentV2::Account {
                platform_order_id: existing,
                ..
            } if existing == platform_order_id)
        })?;
        queue.segments.remove(index);

        if queue.segments.is_empty() && queue.account_taker_depletion_debt_units == 0 {
            self.bids.remove(&price);
        }
        Some(())
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
    ) -> QueueMatchPlanV2 {
        let CalculatedTakerConsumptionV2 {
            fills: calculated_fills,
            matched_base_units,
            used_quote_units,
        } = calculated;
        let mut fills = Vec::with_capacity(calculated_fills.len());

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
                queue.segments.retain(|segment| {
                    segment
                        .remaining_base_units()
                        .is_ok_and(|remaining| remaining > 0)
                });
            }
        }
        QueueMatchPlanV2 {
            fills,
            matched_base_units,
            used_quote_units,
        }
    }

    fn apply_level_transition(
        &mut self,
        side: BookSide,
        price: Decimal,
        quantity_before_base_units: u128,
        quantity_after_base_units: u128,
    ) -> Result<AssetQueueEffectsV2, MatchingStateErrorV1> {
        if quantity_after_base_units > quantity_before_base_units {
            self.append_external_delta(
                side,
                price,
                quantity_after_base_units - quantity_before_base_units,
            )?;
            return Ok(AssetQueueEffectsV2 {
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
            if queue.segments.is_empty() && queue.account_taker_depletion_debt_units == 0 {
                levels.remove(&price);
            }
            return Ok(AssetQueueEffectsV2 {
                ..AssetQueueEffectsV2::default()
            });
        }

        let mut maker_depletions = Vec::new();

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
                queue.segments.pop_front();
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
        }
        Ok(AssetQueueEffectsV2 {
            maker_depletions,
            venue_dust_candidate,
        })
    }

    fn clear_external_segments(&mut self) {
        for levels in [&mut self.bids, &mut self.asks] {
            levels.retain(|_, queue| {
                queue
                    .segments
                    .retain(|segment| matches!(segment, QueueSegmentV2::Account { .. }));
                queue.account_taker_depletion_debt_units = 0;
                let keep = !queue.segments.is_empty();
                keep
            });
        }
    }

    fn clear_matching_segments(&mut self) {
        self.bids.clear();
        self.asks.clear();
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
    ) -> Result<(), MatchingStateErrorV1> {
        let asset = self
            .markets
            .get_mut(condition_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownMarket(condition_id.to_owned()))?
            .assets
            .get_mut(token_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownAsset(token_id.to_owned()))?;
        *asset = AssetQueueV2::from_initial_levels(bids, asks)?;

        Ok(())
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

    fn end_matching(&mut self, condition_id: &str) -> Result<(), MatchingStateErrorV1> {
        let market = self
            .markets
            .get_mut(condition_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownMarket(condition_id.to_owned()))?;
        if market.matching_closed {
            return Ok(());
        }
        market.matching_closed = true;

        for asset in market.assets.values_mut() {
            asset.clear_matching_segments();
        }
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
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
#[cfg_attr(test, derive(serde::Serialize))]
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
    contribution_trade_ids: Vec<String>,
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
    ) -> Result<(), RebateStateErrorV1> {
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
        Ok(())
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct MakerRebateMarketStateV2 {
    end_reached: bool,
    pending_fill_count: u64,
    pending_by_day: BTreeMap<i64, u64>,
    successful_contributions: BTreeMap<u64, MakerRebateContributionV1>,
    result: Option<MakerRebateMarketResultStatusV1>,
}

#[allow(dead_code)]
#[derive(Debug, Default, PartialEq, Eq)]
// 成熟前驱由各市场总数和日归属持有；兑付另有 MarketResolution 的实际成交屏障。
struct MakerRebateStateV2 {
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
        condition_id: &str,
        matched_at_utc_ms: i64,
    ) -> Result<(), RebateStateErrorV1> {
        let attribution_day_utc_ms = utc_day_start(matched_at_utc_ms)?;
        let maturity_at_utc_ms = rebate_maturity_at(RebateKindV1::Maker, attribution_day_utc_ms)?;
        let creates_day = !self.days.contains_key(&attribution_day_utc_ms);
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
        Ok(())
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
}

impl GtdExpirationAdvanceEffectsV2 {
    pub(crate) fn into_lifecycle_feedback(
        self,
        rules: &PlatformRuleSnapshotV3,
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
            platform_event_at_utc_ms: rules
                .effective_gtd_expiration_ms(expiration.expires_at_utc_ms),
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
}

impl CallerCancelEffectV2 {
    pub(crate) fn into_output_effects(
        self,
        platform_order_id: String,
        platform_effective_at_utc_ms: i64,
    ) -> Result<(CanceledTargetMeasurementV2, LifecycleFeedbackEffectV2), MatchingStateErrorV1>
    {
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
        Ok((measurement, lifecycle))
    }
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CallerCancelDispositionV2 {
    TemporaryFailureNoEffect,
    NotFound,
    NoCancellableRemainder,
    Canceled(CallerCancelEffectV2),
}

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TradeFinalityTransitionEffectV2 {
    pub(crate) maker_fee_equivalent_pusd: Option<ExactDecimal>,
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
    pub(crate) maker_fee_equivalent_pusd: Option<ExactDecimal>,
    pub(crate) fill_sequence: u64,
    pub(crate) matched_at_utc_ms: i64,
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SettlementLifecycleEffectV2 {
    /// 重发保留原事件时间，以实际重发时刻起算原入站延迟。
    pub(crate) repeated_at_utc_ms: Option<i64>,
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
            maker_fee_equivalent_pusd: self.maker_fee_equivalent_pusd,
            fill_sequence: self.fill_sequence,
            matched_at_utc_ms: self.matched_at_utc_ms,
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
        EconomicLifecycleEffectV2::ResolvedPosition {
            condition_id,
            resolution_event_id,
            resolution,
            platform_effective_at_utc_ms,
        } => Ok(LifecycleFeedbackEffectV2::Settlement(
            SettlementLifecycleEffectV2 {
                repeated_at_utc_ms: None,
                platform_event_at_utc_ms: platform_effective_at_utc_ms,
                event: SettlementLifecycleEventV2::ResolvedPosition {
                    condition_id,
                    resolution_event_id,
                    resolution,
                },
            },
        )),
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
                    repeated_at_utc_ms: None,
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
                    repeated_at_utc_ms: None,
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
                repeated_at_utc_ms: None,
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
                    contribution_trade_ids: effect.contribution_trade_ids,
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
                    contribution_trade_ids: effect.contribution_trade_ids,
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
                    repeated_at_utc_ms: None,
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
                    repeated_at_utc_ms: None,
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
    pub(crate) contribution_trade_ids: Vec<String>,
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
    ResolvedPosition {
        condition_id: ConditionId,
        resolution_event_id: String,
        resolution: MarketResolutionTerminalV3,
        platform_effective_at_utc_ms: i64,
    },
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

#[cfg(feature = "measurement")]
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

#[cfg(feature = "measurement")]
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

#[cfg(feature = "measurement")]
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
}

impl BuyOrderAcceptedV2 {
    pub(crate) fn into_output_effects(
        self,
        condition_id: ConditionId,
        token_id: TokenId,
        platform_effective_at_utc_ms: i64,
    ) -> Result<(AcceptedSubmissionItemV2, Vec<LifecycleFeedbackEffectV2>), MatchingStateErrorV1>
    {
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
        Ok((response, lifecycle))
    }
}

#[cfg(test)]
impl BuyOrderAcceptedV2 {
    pub(crate) fn initial_finality_count_for_tests(&self) -> usize {
        self.initial_trade_finality.len()
    }

    pub(crate) fn account_delta_count_for_tests(&self) -> usize {
        self.account_deltas.len()
    }
}

#[allow(dead_code)]
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ExecutionMarketEventEffectsV2 {
    initial_trade_finality: Vec<TradeFinalityTransitionEffectV2>,
    account_deltas: Vec<PlatformAccountDeltaV2>,
    settlement: Vec<EconomicLifecycleEffectV2>,
    venue_dust: Vec<VenueDustEffectV2>,
    query_projection_changed: bool,
}

impl ExecutionMarketEventEffectsV2 {
    pub(crate) fn merge(&mut self, other: Self) {
        self.initial_trade_finality
            .extend(other.initial_trade_finality);
        self.account_deltas.extend(other.account_deltas);
        self.settlement.extend(other.settlement);
        self.venue_dust.extend(other.venue_dust);
        self.query_projection_changed |= other.query_projection_changed;
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
    pub(crate) fn initial_finality_for_tests(&self) -> &[TradeFinalityTransitionEffectV2] {
        &self.initial_trade_finality
    }

    pub(crate) fn initial_finality_count_for_tests(&self) -> usize {
        self.initial_trade_finality.len()
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
    cancel_scenario: BTreeMap<(String, String), u64>,
    pub(crate) cancel_scenario_hits: Vec<crate::runtime_v2::CancelScenarioHitV1>,
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
    requested_settlement: settlement::RequestedSettlementStateV1,
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
            cancel_scenario: BTreeMap::new(),
            cancel_scenario_hits: Vec::new(),
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
            requested_settlement: settlement::RequestedSettlementStateV1::default(),
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
    pub(crate) fn gpu_order_checkpoint_for_tests(&self, order_id: &str) -> serde_json::Value {
        let (cash, available, maker, taker, maker_locked, taker_locked, outcomes) =
            self.account.economic_balances_for_tests();
        serde_json::json!({
            "quantities": self.orders.get(order_id).map(|order| &order.quantities),
            "last_updated_at": self.orders.get(order_id).map(|order| order.last_updated_at_utc_ms),
            "cash": cash.to_string(), "available": available.to_string(),
            "principal": self.account.principal_encumbrance_pusd().to_string(),
            "fee": self.account.immediate_taker_fee_encumbrance_pusd().to_string(),
            "rebates": [maker.to_string(), taker.to_string(), maker_locked.to_string(), taker_locked.to_string()],
            "outcomes": outcomes.into_iter().map(|(token, value)| (token, value.to_string())).collect::<Vec<_>>(),
            "next_sequence": self.account.next_account_transition_seq(),
            "fill_shape": self.fill_shape_for_tests(),
            "fill_records": self.fills.iter().map(|(sequence,fill)| serde_json::json!({
                "sequence":sequence,"ordinal":fill.finality_path_ordinal,
                "economic_application_sequence":fill.economic_application_account_transition_seq,
            })).collect::<Vec<_>>(),
        })
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
        rules: &PlatformRuleSnapshotV3,
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
            maker_fee_equivalent_pusd: maker_fee_equivalent(
                &trade_id,
                matched_at_utc_ms,
                base_units,
                price_pusd_per_share,
                liquidity_role,
                rules,
            )?,
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
            self.queues.end_matching(condition_id)?;
        }
        if self.queues.matching_closed(condition_id)? {
            return Ok(effects);
        }

        match normalized {
            NormalizedMarketEventV2::InitialL2Snapshot {
                asset: snapshot_asset,
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
                self.queues.apply_initial_levels(
                    condition_id,
                    &snapshot_asset.token_id.0,
                    &bids,
                    &asks,
                )?;
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
                self.queues
                    .asset_mut(condition_id, &asset.token_id.0)?
                    .clear_external_segments();
            }
            NormalizedMarketEventV2::MarketLifecycleSnapshot(_)
            | NormalizedMarketEventV2::InitialTick { .. }
            | NormalizedMarketEventV2::MarketLifecycleTransition(_)
            | NormalizedMarketEventV2::MarketTradingState(_)
            | NormalizedMarketEventV2::BestBidAsk { .. }
            | NormalizedMarketEventV2::TickSizeChange { .. }
            | NormalizedMarketEventV2::PublicTrade(_)
            | NormalizedMarketEventV2::QualityBoundary { .. }
            | NormalizedMarketEventV2::MarketResolutionTerminal { .. } => {}
        }
        Ok(effects)
    }

    pub(crate) fn advance_market_time(
        &mut self,
        directory: &SessionMarketDirectoryV1,
        platform_effective_at_utc_ms: i64,
    ) -> Result<(), MatchingStateErrorV1> {
        for market in directory.markets() {
            if platform_effective_at_utc_ms >= market.market().window.end_at_utc_ms {
                self.queues.end_matching(&market.market().condition_id.0)?;
            }
        }
        Ok(())
    }

    pub(crate) fn apply_qualified_resolution(
        &mut self,
        condition_id: &str,
        resolution: &QualifiedMarketResolutionV2,
        platform_effective_at_utc_ms: i64,
        finality_policy: &TradeFinalityPolicyV2,
    ) -> Result<ExecutionMarketEventEffectsV2, MatchingStateErrorV1> {
        if self.market_resolutions.contains_key(condition_id) {
            return Ok(ExecutionMarketEventEffectsV2::default());
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
            )?;
            effects
                .settlement
                .push(EconomicLifecycleEffectV2::MarketResolutionOrder(effect));
            effects.query_projection_changed = true;
        }
        effects
            .settlement
            .push(EconomicLifecycleEffectV2::ResolvedPosition {
                condition_id: condition.clone(),
                resolution_event_id: resolution.resolution_event_id.clone(),
                resolution: resolution.terminal.clone(),
                platform_effective_at_utc_ms,
            });
        let obligation = MarketResolutionObligationV2 {
            resolution_event_id: resolution.resolution_event_id.clone(),
            resolution_source_event_at_utc_ms: resolution.resolution_source_event_at_utc_ms,
            terminal: resolution.terminal.clone(),
            parsed_payouts: resolution.parsed_payouts,
            pending_fill_sequences,
            redeemed_at_utc_ms: None,
            invalidated_by_quality_reason: None,
        };

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
    ) {
        if let Some(obligation) = self.market_resolutions.get_mut(condition_id)
            && obligation.redeemed_at_utc_ms.is_none()
            && obligation.invalidated_by_quality_reason.is_none()
        {
            obligation.invalidated_by_quality_reason = Some(reason_code);
        }
    }

    fn apply_market_resolution_order(
        &mut self,
        platform_order_id: &str,
        resolution_event_id: &str,
        platform_effective_at_utc_ms: i64,
        account_deltas: &mut Vec<PlatformAccountDeltaV2>,
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
            if !self.queues.matching_closed(&order.condition_id.0)? {
                self.queues
                    .asset_mut(&order.condition_id.0, &order.token_id.0)?
                    .remove_account_order_at_price(
                        order.source_price_pusd_per_share,
                        platform_order_id,
                    );
            }

            self.account_overlay
                .subtract_quantity(
                    &order.condition_id.0,
                    &order.token_id.0,
                    BookSide::Bid,
                    order.source_price_pusd_per_share,
                    decimal_from_micro_units_v1(open_base_units_before)?,
                )
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        }
        if let SimOrderKindStateV1::Gtd { expires_at_utc_ms } = order.kind {
            self.gtd_deadlines
                .remove(&(expires_at_utc_ms, platform_order_id.to_owned()));
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
        } = queue_effects;

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

            self.account_overlay
                .subtract_quantity(
                    condition_id,
                    token_id,
                    BookSide::Bid,
                    depletion.price_pusd_per_share,
                    decimal_from_micro_units_v1(depletion.base_units)?,
                )
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
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
                rules,
            )?;
            self.maker_rebate
                .register_fill(condition_id, platform_effective_at_utc_ms)?;
            self.next_fill_sequence = next_fill_sequence;
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
                self.gtd_deadlines
                    .remove(&(expires_at_utc_ms, order.platform_order_id.clone()));
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
        {
            self.orders.insert(order.platform_order_id.clone(), order);
            return Ok(());
        }
        self.queues
            .asset_mut(condition_id, token_id)?
            .remove_account_order(&candidate.platform_order_id);
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

        if let SimOrderKindStateV1::Gtd { expires_at_utc_ms } = order.kind {
            self.gtd_deadlines
                .remove(&(expires_at_utc_ms, order.platform_order_id.clone()));
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
        let platform_order_id = platform_order_id_v1(order_sequence);
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

        let plan = self
            .queues
            .asset_mut(&input.condition_id.0, &input.token_id.0)?
            .apply_taker_consumption(calculated);
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
            self.queues
                .asset_mut(&order.condition_id.0, &order.token_id.0)?
                .insert_account_buy_remainder(
                    order.source_price_pusd_per_share,
                    platform_order_id.clone(),
                    open_base_units,
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
        }

        for ((fill_sequence, fill), immediate_taker_fee_units) in fill_sequences
            .iter()
            .copied()
            .zip(plan.fills)
            .zip(fee_allocations)
        {
            if immediate_taker_fee_units > 0 {
                self.taker_rebate
                    .register_positive_fee_fill(input.effective_processing_at_utc_ms)?;
            }

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
                rules,
            )?;
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
                self.queues
                    .asset_mut(&order.condition_id.0, &order.token_id.0)?
                    .remove_account_order(&platform_order_id);

                self.account_overlay
                    .subtract_quantity(
                        &order.condition_id.0,
                        &order.token_id.0,
                        BookSide::Bid,
                        order.source_price_pusd_per_share,
                        decimal_from_micro_units_v1(open_base_units)?,
                    )
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
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
            self.gtd_deadlines
                .insert((expires_at_utc_ms, platform_order_id.clone()));
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
            }),
        ))
    }

    #[cfg(feature = "measurement")]
    pub(crate) fn cancel_scenario_resources(&self) -> crate::resource::ResourceQuantityV1 {
        crate::resource::ResourceQuantityV1::new(
            self.cancel_scenario.len() as u64,
            self.cancel_scenario
                .keys()
                .map(|(request, order)| (request.len() + order.len()) as u64)
                .sum(),
        )
    }
    pub(crate) fn configure_cancel_scenario(
        &mut self,
        scenario: &crate::runtime_v2::CancelFailureScenarioV1,
        input_sequence: u64,
    ) {
        for target in &scenario.targets {
            self.cancel_scenario.insert(
                (
                    target.global_request_id.clone(),
                    target.platform_order_id.clone(),
                ),
                input_sequence,
            );
        }
    }
    pub(crate) fn apply_caller_cancel(
        self,
        platform_order_id: &str,
        platform_effective_at_utc_ms: i64,
    ) -> Result<(Self, CallerCancelDispositionV2), MatchingStateErrorV1> {
        self.apply_caller_cancel_for_request(None, platform_order_id, platform_effective_at_utc_ms)
    }
    pub(crate) fn apply_caller_cancel_for_request(
        mut self,
        global_request_id: Option<&str>,
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
        if let Some(global_request_id) = global_request_id
            && let Some(&configuration_input_sequence) = self
                .cancel_scenario
                .get(&(global_request_id.to_owned(), platform_order_id.to_owned()))
        {
            self.cancel_scenario_hits
                .push(crate::runtime_v2::CancelScenarioHitV1 {
                    configuration_input_sequence,
                    global_request_id: global_request_id.to_owned(),
                    platform_order_id: platform_order_id.to_owned(),
                    effective_processing_at_utc_ms: platform_effective_at_utc_ms,
                });
            return Ok((self, CallerCancelDispositionV2::TemporaryFailureNoEffect));
        }
        let condition_id = order.condition_id.clone();
        let token_id = order.token_id.clone();
        let price_pusd_per_share = order.source_price_pusd_per_share;
        let gtd_expires_at_utc_ms = match order.kind {
            SimOrderKindStateV1::Gtd { expires_at_utc_ms } => Some(expires_at_utc_ms),
            SimOrderKindStateV1::Gtc | SimOrderKindStateV1::Fok | SimOrderKindStateV1::Fak => None,
        };

        if open_base_units_before > 0 {
            if !self.queues.matching_closed(&condition_id.0)? {
                self.queues
                    .asset_mut(&condition_id.0, &token_id.0)?
                    .remove_account_order_at_price(price_pusd_per_share, platform_order_id)
                    .ok_or_else(|| {
                        MatchingStateErrorV1::AccountOrderConflict(platform_order_id.to_owned())
                    })?;
            }

            self.account_overlay
                .subtract_quantity(
                    &condition_id.0,
                    &token_id.0,
                    BookSide::Bid,
                    price_pusd_per_share,
                    decimal_from_micro_units_v1(open_base_units_before)?,
                )
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        }
        if let Some(expires_at_utc_ms) = gtd_expires_at_utc_ms {
            assert!(
                self.gtd_deadlines
                    .remove(&(expires_at_utc_ms, platform_order_id.to_owned())),
                "仍有可撤余量的 V2 GTD 订单必须仍有唯一截止任务"
            );
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

    pub(crate) fn active_platform_order_ids_for_condition(
        &self,
        condition_id: &ConditionId,
    ) -> Vec<String> {
        self.active_platform_order_ids()
            .into_iter()
            .filter(|platform_order_id| {
                self.orders
                    .get(platform_order_id)
                    .is_some_and(|order| order.condition_id == *condition_id)
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

    pub(crate) fn next_gtd_deadline_at_utc_ms(
        &self,
        rules: &PlatformRuleSnapshotV3,
    ) -> Option<i64> {
        self.gtd_deadlines
            .first()
            .map(|(encoded_at, _)| rules.effective_gtd_expiration_ms(*encoded_at))
    }

    pub(crate) fn advance_next_gtd_deadline_at(
        mut self,
        platform_event_at_utc_ms: i64,
        rules: &PlatformRuleSnapshotV3,
    ) -> Result<(Self, GtdExpirationAdvanceEffectsV2), MatchingStateErrorV1> {
        let Some(next_at) = self.next_gtd_deadline_at_utc_ms(rules) else {
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
            if !self.queues.matching_closed(&condition_id.0)? {
                self.queues
                    .asset_mut(&condition_id.0, &token_id.0)?
                    .remove_account_order_at_price(price_pusd_per_share, &platform_order_id);
            }

            self.account_overlay
                .subtract_quantity(
                    &condition_id.0,
                    &token_id.0,
                    BookSide::Bid,
                    price_pusd_per_share,
                    decimal_from_micro_units_v1(open_base_units_before)?,
                )
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
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

        effects.transition = Some(self.apply_scheduled_finality_transition(
            key,
            finality_policy,
            rules,
            &mut effects.account_deltas,
        )?);
        Ok((self, effects))
    }

    fn apply_scheduled_finality_transition(
        &mut self,
        key: ScheduledFinalityTransitionKeyV2,
        finality_policy: &TradeFinalityPolicyV2,
        rules: &PlatformRuleSnapshotV3,
        account_deltas: &mut Vec<PlatformAccountDeltaV2>,
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
            maker_fee_equivalent_pusd: maker_fee_equivalent(
                &trade_id,
                matched_at_utc_ms,
                base_units,
                source_price_pusd_per_share,
                liquidity_role,
                rules,
            )?,
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
            if let Some(obligation) = self.market_resolutions.get_mut(&condition_id.0) {
                obligation.pending_fill_sequences.remove(&key.fill_sequence);
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
            day.contribution_trade_ids.push(trade_id.to_owned());
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
    ) -> Result<(), MatchingStateErrorV1> {
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
            market
                .successful_contributions
                .insert(fill_sequence, contribution);
        }
        if let Some(effect) = self.finalize_ready_maker_market(
            condition_id,
            platform_effective_at_utc_ms,
            account_deltas,
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
    ) -> Result<Option<MakerRebateMarketFinalizedEffectV2>, MatchingStateErrorV1> {
        let Some((status, contributions, pending_by_day)) = self
            .maker_rebate
            .markets
            .get_mut(&condition_id.0)
            .and_then(|market| {
                (market.end_reached && market.pending_fill_count == 0 && market.result.is_none())
                    .then(|| {
                        let status = if !market.successful_contributions.is_empty() {
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
            let day = self
                .maker_rebate
                .days
                .get_mut(&attribution_day_utc_ms)
                .expect("maker 市场终态只引用已登记共享日桶");
            day.pending_predecessor_count = day
                .pending_predecessor_count
                .checked_sub(1)
                .expect("maker 市场终态只闭合一个共享日桶前驱");
            for contribution in contributions
                .iter()
                .filter(|c| c.attribution_day_utc_ms == attribution_day_utc_ms)
            {
                day.contribution_trade_ids
                    .push(contribution.trade_id.clone());
            }
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
        if self.requested_settlement.scenario.is_some() {
            // 原终局、补偿和应计屏障已闭合，最终纯输家没有请求或费用。
            if !self.account.has_resolution_payout(&parsed_binary_payout) {
                self.market_resolutions
                    .get_mut(&condition_id.0)
                    .expect("已有合格判定")
                    .redeemed_at_utc_ms = Some(platform_effective_at_utc_ms);
            }
            return Ok(None);
        }
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
                        contribution_trade_ids: match kind {
                            RebateKindV1::Maker => self
                                .maker_rebate
                                .days
                                .get(&attribution_day_utc_ms)
                                .expect("已准备日桶")
                                .contribution_trade_ids
                                .clone(),
                            RebateKindV1::Taker => self
                                .taker_rebate
                                .days
                                .get(&attribution_day_utc_ms)
                                .expect("已准备日桶")
                                .contribution_trade_ids
                                .clone(),
                        },
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum SimLiquidityRoleV1 {
    Taker,
    MakerInferred,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TradeEconomicDispositionStateV1 {
    PendingUnapplied,
    PendingApplied,
    FinalSuccess,
    FinalFailureUnapplied,
    FinalFailureCompensated,
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
                return limit_segment_result_v1(
                    external_total_base_units,
                    external_total_quote_units,
                    external_consumed_base_units,
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
            return limit_segment_result_v1(
                external_total_base_units,
                external_total_quote_units,
                external_consumed_base_units,
                left,
            );
        }
        if slope > BigInt::zero() && ambiguous_lower > lower {
            return limit_segment_result_v1(
                external_total_base_units,
                external_total_quote_units,
                external_consumed_base_units,
                ambiguous_lower - 1,
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

fn limit_segment_result_v1(
    external_total_base_units: u128,
    external_total_quote_units: u128,
    external_consumed_base_units: u128,
    delta_base_units: u128,
) -> Result<Option<(u128, u128)>, MatchingMathErrorV1> {
    let before = cumulative_segment_quote_units_v1(
        external_total_base_units,
        external_total_quote_units,
        external_consumed_base_units,
    )?;
    let after = cumulative_segment_quote_units_v1(
        external_total_base_units,
        external_total_quote_units,
        external_consumed_base_units
            .checked_add(delta_base_units)
            .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?,
    )?;
    Ok(Some((
        delta_base_units,
        after
            .checked_sub(before)
            .ok_or(MatchingMathErrorV1::InvalidSegment)?,
    )))
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
    let prices = fills
        .iter()
        .map(|(_, price)| fee_price_ratio_v1(*price))
        .collect::<Result<Vec<_>, _>>()?;
    let total_base_units = fills.iter().map(|(base, _)| BigUint::from(*base)).sum();
    if fee_upper_bound_rounds_to_zero(total_base_units, &rate, exponent)? {
        return Ok(Decimal::ZERO);
    }

    let mut total = ExactRatioV1::zero();
    for ((base_units, _), price) in fills.iter().zip(&prices) {
        total = total.add(exact_taker_fee_contribution_v1(
            &BigUint::from(*base_units),
            price,
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
    if rate.numerator.is_zero() || fills.iter().all(|(_, base_units, _)| *base_units == 0) {
        return Ok((0, vec![0; fills.len()]));
    }
    let prices = fills
        .iter()
        .map(|(_, _, price)| fee_price_ratio_v1(*price))
        .collect::<Result<Vec<_>, _>>()?;
    let total_base_units = fills.iter().map(|(_, base, _)| BigUint::from(*base)).sum();
    if fee_upper_bound_rounds_to_zero(total_base_units, &rate, exponent)? {
        return Ok((0, vec![0; fills.len()]));
    }

    let fee_unit_scale = BigUint::from(10_u32.pow(FEE_SCALE));
    let mut total = ExactRatioV1::zero();
    let mut floor_units = Vec::with_capacity(fills.len());
    let mut remainders = Vec::with_capacity(fills.len());
    for ((_, base_units, _), price) in fills.iter().zip(&prices) {
        let contribution =
            exact_taker_fee_contribution_v1(&BigUint::from(*base_units), price, &rate, exponent)?;
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

#[cfg(test)]
pub(crate) fn allocate_taker_fee_micro_units_v1(
    fills: &[(&str, u128, Decimal)],
    rate: Decimal,
    exponent: u32,
) -> Result<(u128, Vec<u128>), MatchingMathErrorV1> {
    allocate_taker_fee_micro_units_by_key(fills, rate, exponent)
}

pub(crate) fn exact_taker_fee_contribution_v1(
    base_units: &BigUint,
    price: &ExactRatioV1,
    rate: &ExactRatioV1,
    exponent: u32,
) -> Result<ExactRatioV1, MatchingMathErrorV1> {
    if base_units.is_zero() {
        return Ok(ExactRatioV1::zero());
    }
    let curve_numerator = &price.numerator * (&price.denominator - &price.numerator);
    let curve_denominator = &price.denominator * &price.denominator;
    ExactRatioV1::new(
        base_units * &rate.numerator * curve_numerator.pow(exponent),
        BigUint::from(1_000_000_u32) * &rate.denominator * curve_denominator.pow(exponent),
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExactRatioV1 {
    pub(crate) numerator: BigUint,
    pub(crate) denominator: BigUint,
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

pub(crate) fn nonnegative_decimal_ratio(
    value: Decimal,
) -> Result<ExactRatioV1, MatchingMathErrorV1> {
    if value.is_sign_negative() {
        return Err(MatchingMathErrorV1::NegativeAmount);
    }
    decimal_ratio(value)
}

pub(crate) fn fee_price_ratio_v1(value: Decimal) -> Result<ExactRatioV1, MatchingMathErrorV1> {
    if value <= Decimal::ZERO || value >= Decimal::ONE {
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
    total_base_units: BigUint,
    rate: &ExactRatioV1,
    exponent: u32,
) -> Result<bool, MatchingMathErrorV1> {
    let left = total_base_units * &rate.numerator * BigUint::from(200_000_u32);
    let right_unshifted = BigUint::from(1_000_000_u32) * &rate.denominator;
    exact_less_than_power_of_two_shift(&left, &right_unshifted, u64::from(exponent) * 2)
}

pub(crate) fn exact_less_than_power_of_two_shift(
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

pub(crate) fn rounded_fee_units_v1(total: &ExactRatioV1) -> BigUint {
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

#[cfg(feature = "measurement")]
impl ExecutionStateV2 {
    pub(crate) fn measurement_resources(&self) -> crate::resource::ResourceQuantityV1 {
        let mut total = self.account.measurement_resources();
        total.include(self.account_overlay.measurement_resources());
        total.include(self.cancel_scenario_resources());
        total.include(self.requested_settlement_resources());
        for (condition, market) in &self.queues.markets {
            total.add_record(condition.len() as u64);
            for (token, asset) in &market.assets {
                total.add_record(token.len() as u64);
                for levels in [&asset.bids, &asset.asks] {
                    total.add_records(levels.len() as u64);
                    for queue in levels.values() {
                        for segment in &queue.segments {
                            total.add_record(match segment {
                                QueueSegmentV2::External { .. } => 0,
                                QueueSegmentV2::Account {
                                    platform_order_id, ..
                                } => platform_order_id.len() as u64,
                            });
                        }
                    }
                }
            }
        }
        for (key, order) in &self.orders {
            total.add_record(
                (key.len()
                    + order.platform_order_id.len()
                    + order.global_request_id.len()
                    + order.item_id.len()
                    + order.condition_id.0.len()
                    + order.token_id.0.len()) as u64,
            );
        }
        for fill in self.fills.values() {
            total.add_record((fill.trade_id.len() + fill.platform_order_id.len()) as u64);
        }
        for (_, id) in &self.gtd_deadlines {
            total.add_record(id.len() as u64);
        }
        total.add_records(self.scheduled_finality.len() as u64);
        for (condition, resolution) in &self.market_resolutions {
            total.add_record(market_resolution_obligation_owned_bytes_v2(
                condition, resolution,
            ));
            total.add_records(resolution.pending_fill_sequences.len() as u64);
        }
        for days in [&self.taker_rebate.days, &self.maker_rebate.days] {
            for day in days.values() {
                total.add_record(0);
                for id in &day.contribution_trade_ids {
                    total.add_record(id.len() as u64);
                }
            }
        }
        total.add_records(
            (self.taker_rebate.clocks.len() + self.maker_rebate.day_clocks.len()) as u64,
        );
        for (_, condition) in &self.maker_rebate.market_end_clocks {
            total.add_record(condition.len() as u64);
        }
        for (condition, market) in &self.maker_rebate.markets {
            total.add_record(condition.len() as u64);
            total.add_records(market.pending_by_day.len() as u64);
            for contribution in market.successful_contributions.values() {
                total.add_record(contribution.trade_id.len() as u64);
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn limit_segment_matches_finite_cash_allowance_enumeration() {
        // 独立枚举实际付款与订单剩余额度，包含三种斜率、floor 余数和改善价信用。
        for (
            external_base,
            external_quote,
            consumed,
            limit,
            order_base,
            order_quote,
            matched,
            used,
        ) in [
            (10_u128, 5, 0, 10, 10, 5, 0, 0),
            (10, 4, 0, 10, 10, 6, 0, 0),
            (10, 7, 0, 10, 10, 4, 0, 0),
            (11, 7, 3, 6, 13, 8, 4, 2),
            (11, 7, 3, 6, 13, 8, 4, 0),
            (12, 6, 1, 7, 14, 7, 3, 1),
            (17, 12, 2, 8, 19, 8, 5, 0),
            (10, 1, 0, 9, 10, 5, 0, 0),
            (10, 1, 0, 10, 10, 5, 0, 0),
        ] {
            let expected = (1..=limit)
                .filter_map(|delta| {
                    let charge = (consumed + delta) * external_quote / external_base
                        - consumed * external_quote / external_base;
                    let allowance = (matched + delta) * order_quote / order_base - used;
                    (charge > 0 && charge <= allowance).then_some((delta, charge))
                })
                .last();
            assert_eq!(
                maximum_base_for_limit_order_segment_v1(
                    external_base,
                    external_quote,
                    consumed,
                    limit,
                    order_base,
                    order_quote,
                    matched,
                    used,
                )
                .expect("有效限价段可计算"),
                expected,
                "external={external_base}/{external_quote}, consumed={consumed}, limit={limit}, order={order_base}/{order_quote}, matched={matched}, used={used}"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn market_amount_bounds_cover_real_segment_floor_partial_and_fee_half_unit() {
        use crate::platform_rules::tests::snapshot_v3_with_fee_for_cost_tests;
        for (rate, expected_units) in [
            ("0.07", None),
            ("0.000008333", Some(0u32)),
            ("0.000008334", Some(1u32)),
        ] {
            let rules = snapshot_v3_with_fee_for_cost_tests(rate, 1).await;
            // 真实初始深度及原计算/消费入口；没有手改段的已消耗位置。
            let mut queue =
                AssetQueueV2::from_initial_levels(&[], &[(decimal("0.4"), 5_000_003)]).unwrap();
            for ordinal in 0..3 {
                let candidate = queue
                    .calculate_taker_buy(decimal("0.6"), TakerTargetV1::Quote(1_000_000))
                    .unwrap();
                let plan = queue.apply_taker_consumption(candidate);
                if ordinal == 0 {
                    assert!(
                        plan.matched_base_units > 2_500_000,
                        "原floor分配可超出spend/实际最低价格"
                    );
                } else if ordinal == 2 {
                    assert!(
                        plan.used_quote_units < 1_000_000,
                        "第三次合法1pUSD金额目标只有部分流动性"
                    );
                }
                let bounds = rules.sim_market_buy_bounds_v1(decimal("1")).unwrap();
                assert!(BigUint::from(plan.matched_base_units) <= bounds.maximum_base_micro);
                let fee = taker_fee_pusd_v1(
                    &plan
                        .fills
                        .iter()
                        .map(|f| (f.base_units, f.price_pusd_per_share))
                        .collect::<Vec<_>>(),
                    decimal(rate),
                    1,
                )
                .unwrap();
                let actual_units = BigUint::from(micro_units_v1(fee).unwrap() / 10);
                assert!(actual_units <= bounds.maximum_fee_1e5_units);
                if ordinal == 0
                    && let Some(expected) = expected_units
                {
                    assert_eq!(actual_units, BigUint::from(expected));
                }
            }
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

        let matched = queue.apply_taker_consumption(discarded);
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
            assert!(added_effects.venue_dust.is_empty());

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
                assert!(reduced_effects.initial_trade_finality.is_empty());
                assert_eq!(state.next_fill_sequence, 1);
            } else {
                assert_eq!(reduced_effects.initial_trade_finality[0].fill_sequence, 1);
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
        assert_eq!(state.next_gtd_deadline_at_utc_ms(rules.snapshot()), None);
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
        assert_eq!(
            state.next_gtd_deadline_at_utc_ms(rules.snapshot()),
            Some(expires_at_utc_ms)
        );

        for event in [
            decoded_level_event_v2_for_tests("0", "5", 1_300, 1_301, 40),
            decoded_level_event_v2_for_tests("5", "0", 1_310, 1_311, 41),
        ] {
            let normalized = crate::market::normalize_market_event_v2(&event)
                .expect("一次规范化 GTD maker 档位事件");
            state
                .apply_market_event(
                    &condition_id.0,
                    &normalized,
                    event.time.source_event_at_utc_ms.expect("隐藏事件时点"),
                    2_000,
                    rules.snapshot(),
                    &finality_policy,
                )
                .expect("GTD maker 档位事件必须成功");
        }

        assert!(state.gtd_deadlines.is_empty());
        assert_eq!(state.next_gtd_deadline_at_utc_ms(rules.snapshot()), None);
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
        assert!(effects.venue_dust.is_empty());

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
        assert_eq!(
            state.next_gtd_deadline_at_utc_ms(rules.snapshot()),
            Some(expires_at_utc_ms)
        );

        let (state, disposition) = state
            .apply_caller_cancel("sim-1", 2_001)
            .expect("end_at 后仍须允许撤销既有 GTD 余量");
        let CallerCancelDispositionV2::Canceled(canceled) = disposition else {
            panic!("end_at 后既有 GTD 余量必须可撤")
        };
        assert_eq!(canceled.pending_base_units_before, 0);
        assert_eq!(canceled.open_base_units_before, 5_000_000);
        assert_eq!(canceled.released_open_quote_units, 1_500_000);

        assert_eq!(state.next_gtd_deadline_at_utc_ms(rules.snapshot()), None);
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

        let (response, lifecycle) = accepted
            .into_output_effects(condition_id, token_id, 1_500)
            .expect("GTD 平台小额余量必须形成冻结的直接响应和生命周期效果");

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
        for current in [false, true] {
            assert_gtd_partial_expiration(current).await;
        }
    }

    async fn assert_gtd_partial_expiration(current: bool) {
        let (mut state, condition_id, token_id) = execution_state_v2_for_resting_limit_tests("10");
        state
            .apply_initial_levels(
                &condition_id.0,
                &token_id.0,
                &[],
                &[(decimal("0.4"), 5_000_000)],
            )
            .expect("GTD 到期测试 ask");
        let rules = if current {
            crate::platform_rules::tests::current_gtd_snapshot_v3_for_sim_tests().await
        } else {
            crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await
        };
        let expires_at_utc_ms = 300_000;
        let deadline = if current { 240_000 } else { 300_000 };
        let policy = format!(
            r#"{{"schema_version":"trade_finality_policy_v2","normal_path":{{"steps":[{{"state":"MATCHED","offset_ms":0}},{{"state":"MINED","offset_ms":{}}},{{"state":"CONFIRMED","offset_ms":{}}}],"economic_application_ordinal":1}},"failure_overrides":[]}}"#,
            deadline - 1500,
            deadline - 1490
        );
        let finality_policy = TradeFinalityPolicyV2::decode_and_seal(policy.as_bytes())
            .expect("GTD 同刻最终性测试策略必须合法");
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
        assert_eq!(
            state.next_gtd_deadline_at_utc_ms(rules.snapshot()),
            Some(deadline)
        );
        assert_eq!(state.next_scheduled_finality_at_utc_ms(), Some(deadline));
        assert_eq!(state.account.principal_encumbrance_pusd(), decimal("4.5"));
        assert_eq!(
            state.account.immediate_taker_fee_encumbrance_pusd(),
            decimal("0.084")
        );

        let (state, early) = state
            .advance_next_gtd_deadline_at(deadline - 1, rules.snapshot())
            .expect("到期前推进必须无效果");
        assert!(early.expiration.is_none());
        assert!(early.account_deltas.is_empty());
        assert_eq!(
            state.next_gtd_deadline_at_utc_ms(rules.snapshot()),
            Some(deadline)
        );

        let (state, expired) = state
            .advance_next_gtd_deadline_at(deadline, rules.snapshot())
            .expect("GTD 必须在该版本的有效截止时点到期");
        let expiration = expired.expiration.as_ref().expect("必须形成 GTD 到期效果");
        assert_eq!(expiration.platform_order_id, "sim-1");
        assert_eq!(expiration.pending_base_units_before, 0);
        assert_eq!(expiration.open_base_units_before, 5_000_000);
        assert_eq!(expiration.released_open_quote_units, 2_500_000);
        assert_eq!(expiration.account_transition_seq, Some(2));
        assert_eq!(expired.account_deltas.len(), 1);
        assert_eq!(expired.account_deltas[0].account_transition_seq, 2);

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
        assert_eq!(state.next_gtd_deadline_at_utc_ms(rules.snapshot()), None);
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
            .into_lifecycle_feedback(rules.snapshot())
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
        assert_eq!(expiration_lifecycle.platform_event_at_utc_ms, deadline);
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
            .apply_caller_cancel("sim-1", deadline)
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
            .advance_next_scheduled_finality_at(deadline, &finality_policy, rules.snapshot())
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
        let sequence = state.account.next_account_transition_seq();
        let (state, duplicate) = state
            .advance_next_gtd_deadline_at(deadline + 1, rules.snapshot())
            .unwrap();
        assert!(duplicate.expiration.is_none());
        assert_eq!(state.account.next_account_transition_seq(), sequence);
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
        assert_eq!(
            state.next_gtd_deadline_at_utc_ms(rules.snapshot()),
            Some(expires_at_utc_ms)
        );

        let (state, first_expired) = state
            .advance_next_gtd_deadline_at(expires_at_utc_ms, rules.snapshot())
            .expect("首个同刻 GTD 必须先到期");
        assert_eq!(
            first_expired
                .expiration
                .as_ref()
                .map(|effect| effect.platform_order_id.as_str()),
            Some("sim-1")
        );
        assert_eq!(
            state.next_gtd_deadline_at_utc_ms(rules.snapshot()),
            Some(expires_at_utc_ms)
        );
        assert_eq!(state.orders["sim-2"].last_updated_at_utc_ms, 1_500);

        let (state, second_expired) = state
            .advance_next_gtd_deadline_at(expires_at_utc_ms, rules.snapshot())
            .expect("第二个同刻 GTD 必须随后到期");
        assert_eq!(
            second_expired
                .expiration
                .as_ref()
                .map(|effect| effect.platform_order_id.as_str()),
            Some("sim-2")
        );
        assert_eq!(state.next_gtd_deadline_at_utc_ms(rules.snapshot()), None);
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

        if let Ok(validator) = std::env::var("B_GPU_TAKER_DAY_VALIDATOR") {
            use std::io::Write;
            use std::process::{Command, Stdio};
            let artifact = std::path::PathBuf::from(
                std::env::var("B_GPU_ORDER_ARTIFACT_DIR").expect("GPU返佣验证产物目录"),
            );
            std::fs::create_dir_all(&artifact).unwrap();
            let fill = &state.fills[&1];
            let settlements = [accrual_lifecycle, day_lifecycle, paid_lifecycle]
                .into_iter()
                .map(|effect| serde_json::json!({
                    "platform_event_at_utc_ms":effect.platform_event_at_utc_ms,
                    "repeated_at_utc_ms":effect.repeated_at_utc_ms,"event":effect.event,
                }))
                .collect::<Vec<_>>();
            let source = serde_json::json!({
                "source_contract":"original_SIM_matching_Taker_rebate_v2",
                "opening_cash":"517.50052","fill_sequence":1,
                "matched_at":fill.matched_at_utc_ms,"base_units":fill.base_units.to_string(),
                "source_price":fill.price_pusd_per_share.to_string(),
                "quote_units":fill.quote_units.to_string(),"fee_units":fill.immediate_taker_fee_units.to_string(),
                "finality_path":finality_policy.normal_path(),
                "taker_rebate_rate":rules.snapshot().project_account_rules.rebate_settlement.value.fixed_taker_rebate_rate.value().to_string(),
                "submission_deltas":accepted.account_deltas,"settlements":settlements,
                "maturity_at":maturity_at_utc_ms,"confirmed_at":confirmed_at_utc_ms,
                "final_available":state.account.available_pusd().to_string(),
                "next_account_sequence":state.account.next_account_transition_seq(),
            });
            let wire = serde_json::to_vec_pretty(&source).unwrap();
            std::fs::write(artifact.join("original-taker-mature-day.json"), &wire).unwrap();
            let mut child = Command::new(std::env::var("B_GPU_PYTHON").expect("锁定GPU Python"))
                .arg(validator).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped())
                .spawn().expect("启动GPU真实Taker日验证");
            child.stdin.take().unwrap().write_all(&wire).unwrap();
            let output = child.wait_with_output().unwrap();
            std::fs::write(artifact.join("taker-day-report.json"), &output.stdout).unwrap();
            assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
            println!("{}", String::from_utf8_lossy(&output.stdout));
        }

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
                EconomicLifecycleEffectV2::ResolvedPosition {
                    resolution_event_id: id,
                    platform_effective_at_utc_ms,
                    ..
                } => {
                    assert_eq!(id, resolution_event_id);
                    assert_eq!(*platform_effective_at_utc_ms, resolution_at_utc_ms);
                }
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

        let (next, maker_end) = state
            .advance_next_rebate_clock_at(2_000)
            .expect("maker 市场结束时钟必须先建立终态屏障");
        state = next;
        assert!(maker_end.settlement.is_empty());

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
                [
                    EconomicLifecycleEffectV2::MarketResolutionOrder(_),
                    EconomicLifecycleEffectV2::ResolvedPosition { .. }
                ]
            ));

            let lifecycle = effects
                .into_lifecycle_feedback()
                .expect("最终性前判定必须冻结订单关闭生命周期效果");
            let [
                LifecycleFeedbackEffectV2::Order(order_lifecycle),
                LifecycleFeedbackEffectV2::Settlement(SettlementLifecycleEffectV2 {
                    event: SettlementLifecycleEventV2::ResolvedPosition { .. },
                    ..
                }),
            ] = lifecycle.as_slice()
            else {
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
                    EconomicLifecycleEffectV2::ResolvedPosition { .. },
                    EconomicLifecycleEffectV2::AutoRedeem(_)
                ]
            ));

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
                        event: SettlementLifecycleEventV2::ResolvedPosition { .. },
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
        let [
            EconomicLifecycleEffectV2::ResolvedPosition { .. },
            EconomicLifecycleEffectV2::AutoRedeem(redeem),
        ] = effects.settlement.as_slice()
        else {
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
        let [
            LifecycleFeedbackEffectV2::Settlement(resolved),
            LifecycleFeedbackEffectV2::Settlement(redemption_lifecycle),
        ] = redemption_lifecycle.as_slice()
        else {
            panic!("零余额判定先公开原判定事实，再形成唯一自动赎回生命周期效果")
        };
        assert!(matches!(
            resolved.event,
            SettlementLifecycleEventV2::ResolvedPosition { .. }
        ));
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

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "AutoDL真实Maker队列→市场结束→日结/付款设备验证"]
    async fn gpu_maker_market_and_day_follow_original_positive_fill() {
        let rules = crate::platform_rules::tests::successful_snapshot_v3_for_sim_tests().await;
        let policy = TradeFinalityPolicyV2::decode_and_seal(
            br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":5000},{"state":"CONFIRMED","offset_ms":6000}],"economic_application_ordinal":2},"failure_overrides":[]}"#,
        ).unwrap();
        let (state, condition, token) = execution_state_v2_for_resting_limit_tests("639.50052");
        let (mut state, disposition) = state.apply_buy_order(BuyOrderActionV2 {
            global_request_id:"gpu-positive-maker".into(),batch_item_ordinal:0,item_id:"gpu-positive-maker-item".into(),
            condition_id:condition.clone(),token_id:token,effective_processing_at_utc_ms:1200,
            terms:BuyOrderTermsV2::Gtc {limit_price_pusd_per_share:decimal("0.4"),accepted_base_units:305_000_000,
                accepted_quote_units:122_000_000,post_only:false},
        },rules.snapshot(),&policy).unwrap();
        assert!(matches!(disposition,BuyOrderDispositionV2::Accepted(_)));
        let before = state.gpu_order_checkpoint_for_tests("sim-1");
        let mut checkpoints = Vec::new();
        for event in [
            decoded_level_event_at_v2_for_tests(BookSide::Bid,"0.4","0","300",1300,1301,100),
            decoded_level_event_at_v2_for_tests(BookSide::Bid,"0.4","300","0",1310,1311,101),
        ] {
            let normalized=crate::market::normalize_market_event_v2(&event).unwrap();
            state.apply_market_event(&condition.0,&normalized,event.time.source_event_at_utc_ms.unwrap(),2000,
                rules.snapshot(),&policy).unwrap();
        }
        assert_eq!(state.fills[&1].base_units,300_000_000);
        assert_eq!(state.fills[&1].quote_units,120_000_000);
        assert_eq!(state.fills[&1].liquidity_role,SimLiquidityRoleV1::MakerInferred);
        checkpoints.push(serde_json::json!({"stage":"matched","at":1310,"state":state.gpu_order_checkpoint_for_tests("sim-1")}));
        let (next,end)=state.advance_next_rebate_clock_at(2000).unwrap();
        state=next;
        assert!(end.settlement.is_empty());
        checkpoints.push(serde_json::json!({"stage":"end_pending","at":2000,"state":state.gpu_order_checkpoint_for_tests("sim-1")}));
        let mut settlements=Vec::new();
        let mut maker_fee_equivalent=None;
        for at in [6310,7310] {
            let (next,effect)=state.advance_next_scheduled_finality_at(at,&policy,rules.snapshot()).unwrap();
            state=next;
            maker_fee_equivalent=effect.transition.as_ref().and_then(|value|value.trade_finality.maker_fee_equivalent_pusd.as_ref()).map(|value|value.0.clone());
            for effect in effect.into_lifecycle_feedback().unwrap() {
                if let LifecycleFeedbackEffectV2::Settlement(effect)=effect {
                    settlements.push(serde_json::json!({"at":effect.platform_event_at_utc_ms,"event":effect.event}));
                }
            }
            checkpoints.push(serde_json::json!({"stage":"finality","at":at,"state":state.gpu_order_checkpoint_for_tests("sim-1")}));
        }
        let (next,effect)=state.advance_next_rebate_clock_at(89_100_000).unwrap();
        state=next;
        for effect in effect.into_lifecycle_feedback().unwrap() {
            if let LifecycleFeedbackEffectV2::Settlement(effect)=effect {
                settlements.push(serde_json::json!({"at":effect.platform_event_at_utc_ms,"event":effect.event}));
            }
        }
        let source=serde_json::json!({"source_contract":"original_matching_positive_Maker_queue_market_day",
            "before":before,"checkpoints":checkpoints,"after":state.gpu_order_checkpoint_for_tests("sim-1"),
            "settlements":settlements,"finality_path":policy.normal_path(),"maker_fee_equivalent":maker_fee_equivalent.unwrap(),
            "fee_rate":rules.snapshot().synchronized_platform_rules.fee_schedule.rate.value().to_string(),
            "fee_exponent":rules.snapshot().synchronized_platform_rules.fee_schedule.exponent,
            "maker_rate":rules.snapshot().project_account_rules.rebate_settlement.value.fixed_maker_rebate_rate.value().to_string()});
        let directory=std::path::PathBuf::from(std::env::var("B_GPU_ORDER_ARTIFACT_DIR").unwrap());
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("original-positive-maker-day.json"),serde_json::to_vec_pretty(&source).unwrap()).unwrap();
        let mut child=std::process::Command::new(std::env::var("B_GPU_PYTHON").unwrap())
            .arg(std::env::var("B_GPU_MAKER_DAY_VALIDATOR").unwrap()).stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn().unwrap();
        serde_json::to_writer(child.stdin.take().unwrap(),&source).unwrap();
        let result=child.wait_with_output().unwrap();
        assert!(result.status.success(),"{}",String::from_utf8_lossy(&result.stderr));
        std::fs::write(directory.join("maker-day-report.json"),&result.stdout).unwrap();
        eprintln!("gpu_positive_maker_day={}",String::from_utf8(result.stdout).unwrap());
    }
}

fn maker_fee_equivalent(
    trade_id: &str,
    matched_at: i64,
    base_units: u128,
    price: Decimal,
    role: SimLiquidityRoleV1,
    rules: &PlatformRuleSnapshotV3,
) -> Result<Option<ExactDecimal>, MatchingStateErrorV1> {
    if role == SimLiquidityRoleV1::Taker {
        return Ok(None);
    }
    let contribution = maker_rebate_contribution_v1(
        trade_id,
        matched_at,
        base_units,
        price,
        rules.synchronized_platform_rules.fee_schedule.rate.value(),
        rules.synchronized_platform_rules.fee_schedule.exponent,
        rules
            .project_account_rules
            .rebate_settlement
            .value
            .fixed_maker_rebate_rate
            .value(),
    )?;
    Ok(Some(exact_decimal_v2(contribution.fee_equivalent_pusd)))
}

#[cfg(test)]
#[path = "matching/gpu_queue_tests.rs"]
pub(crate) mod gpu_queue_tests;
