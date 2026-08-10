use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

use contracts::{
    BookSide, ExecutionMarketEventPayloadV3, ExecutionMarketEventV3, InitialL2SnapshotV2,
    LevelExecutionTransitionV3, QualityScopeV1, ResyncQuality, Sha256Hex,
};
use data_pipeline_core::parse_canonical_decimal;
use num_bigint::{BigInt, BigUint};
use num_integer::Integer;
use num_traits::{One, ToPrimitive, Zero};
use polymarket_client_sdk_v2::types::U256;
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};

use crate::account::InitialAccountStateV1;
use crate::market::AccountBookOverlayStoreV1;
use crate::scenario::PlatformScenarioV1;

const MICRO_SCALE: u32 = 6;
const FEE_SCALE: u32 = 5;
const EXTERNAL_SEGMENT_ID_DOMAIN_V1: &[u8] = b"SIM_L2_SYNTHETIC_MAKER_SEGMENT_V1\0";
const PENDING_MAKER_DEPLETION_ID_DOMAIN_V1: &[u8] = b"SIM_PENDING_MAKER_DEPLETION_V1\0";

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
    Decode(String),
    UnknownMarket(String),
    UnknownAsset(String),
    InitialQueueAlreadyLoaded(String),
    QueuePredecessorConflict,
    QueueUnderflow,
    AccountOrderConflict(String),
    LogicalTimeRegression,
}

impl From<MatchingMathErrorV1> for MatchingStateErrorV1 {
    fn from(value: MatchingMathErrorV1) -> Self {
        Self::Math(value)
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
pub(crate) struct QueueMatchPlanV1 {
    pub fills: Vec<TakerFillSliceV1>,
    pub matched_base_units: u128,
    pub used_quote_units: u128,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExternalSyntheticMakerSegmentV1 {
    segment_id: Sha256Hex,
    total_base_units: u128,
    total_quote_units: u128,
    consumed_base_units: u128,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AccountOrderQueueSegmentV1 {
    platform_order_id: String,
    remaining_base_units: u128,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PriceQueueV1 {
    segments: VecDeque<QueueSegmentV1>,
    account_taker_depletion_debt_units: u128,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct AssetQueueV1 {
    bids: BTreeMap<Decimal, PriceQueueV1>,
    asks: BTreeMap<Decimal, PriceQueueV1>,
    initial_loaded: bool,
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExecutionQueueStateV1 {
    markets: BTreeMap<String, BTreeMap<String, AssetQueueV1>>,
}

impl ExecutionQueueStateV1 {
    pub(crate) fn new(scenario: &PlatformScenarioV1) -> Self {
        let markets = scenario
            .market_set()
            .markets()
            .iter()
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

    pub(crate) fn apply_authorized_events(
        &mut self,
        events: &[ExecutionMarketEventV3],
    ) -> Result<Vec<MakerQueueDepletionV1>, MatchingStateErrorV1> {
        let mut maker_depletions = Vec::new();
        for event in events {
            match &event.payload {
                ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) => {
                    self.load_initial_snapshot(event, snapshot)?;
                }
                ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) => {
                    maker_depletions.extend(self.apply_level_transition(event, transition)?);
                }
                ExecutionMarketEventPayloadV3::QualityBoundary(boundary)
                    if event.quality.resync == ResyncQuality::Required =>
                {
                    if let QualityScopeV1::Asset(asset) = &boundary.scope {
                        self.clear_external_segments(
                            &event.market.condition_id.0,
                            &asset.token_id.0,
                        )?;
                    }
                }
                _ => {}
            }
        }
        Ok(maker_depletions)
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
            .bids
            .entry(price)
            .or_default()
            .segments
            .push_back(QueueSegmentV1::Account(AccountOrderQueueSegmentV1 {
                platform_order_id,
                remaining_base_units: base_units,
            }));
        Ok(())
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

    fn close_account_orders_for_market(
        &mut self,
        condition_id: &str,
    ) -> Result<(), MatchingStateErrorV1> {
        let assets = self
            .markets
            .get_mut(condition_id)
            .ok_or_else(|| MatchingStateErrorV1::UnknownMarket(condition_id.to_owned()))?;
        for asset in assets.values_mut() {
            for levels in [&mut asset.bids, &mut asset.asks] {
                levels.retain(|_, queue| {
                    queue
                        .segments
                        .retain(|segment| matches!(segment, QueueSegmentV1::External(_)));
                    !queue.segments.is_empty()
                });
            }
        }
        Ok(())
    }

    fn remove_account_order(&mut self, platform_order_id: &str) {
        for assets in self.markets.values_mut() {
            for asset in assets.values_mut() {
                for levels in [&mut asset.bids, &mut asset.asks] {
                    levels.retain(|_, queue| {
                        queue.segments.retain(|segment| {
                            !matches!(segment, QueueSegmentV1::Account(account)
                                if account.platform_order_id == platform_order_id)
                        });
                        !queue.segments.is_empty() || queue.account_taker_depletion_debt_units > 0
                    });
                }
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
    ) -> Result<Vec<MakerQueueDepletionV1>, MatchingStateErrorV1> {
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
            return Ok(Vec::new());
        }
        if before == after {
            return Ok(Vec::new());
        }
        let levels = asset.levels_mut(transition.side);
        let queue = levels
            .get_mut(&price)
            .ok_or(MatchingStateErrorV1::QueueUnderflow)?;
        let mut remaining = before - after;
        let debt_offset = remaining.min(queue.account_taker_depletion_debt_units);
        queue.account_taker_depletion_debt_units -= debt_offset;
        remaining -= debt_offset;
        let fills = consume_queue_head(queue, &event.event_id.0, price, remaining)?;
        if queue.segments.is_empty() && queue.account_taker_depletion_debt_units == 0 {
            levels.remove(&price);
        }
        Ok(fills)
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
        let asset = self.asset_mut(condition_id, token_id)?;
        let prices = asset
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
            let queue = asset
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SimOrderKindStateV1 {
    Gtc,
    Gtd { expires_at_utc_ms: i64 },
    Fok,
    Fak,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SimOrderTerminalReasonV1 {
    FakCompleted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CallerCancelStateEffectV1 {
    pub pending_base_units_before: u128,
    pub open_base_units_before: u128,
    pub released_open_quote_units: u128,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CancelOrderStateDispositionV1 {
    NotFound,
    NoCancellableRemainder,
    Canceled(CallerCancelStateEffectV1),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SimLiquidityRoleV1 {
    Taker,
    MakerInferred,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SimOrderRecordV1 {
    pub platform_order_id: String,
    pub global_request_id: String,
    pub item_id: String,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MakerDepletionEvidenceV1 {
    pub sha256: Sha256Hex,
    pub event_count: u64,
    pub first_source_event_id: String,
    pub last_source_event_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExecutionStateV1 {
    queues: ExecutionQueueStateV1,
    account_overlay: AccountBookOverlayStoreV1,
    available_pusd_units: u128,
    principal_encumbered_pusd_units: u128,
    immediate_taker_fee_encumbered_pusd_units: u128,
    orders: BTreeMap<String, SimOrderRecordV1>,
    fills: BTreeMap<String, SimFillRecordV1>,
    matching_closed_markets: BTreeMap<String, i64>,
    gtd_deadlines: BTreeSet<(i64, String)>,
    gtd_advanced_through_utc_ms: Option<i64>,
}

impl ExecutionStateV1 {
    pub(crate) fn new(
        scenario: &PlatformScenarioV1,
        initial_account: &InitialAccountStateV1,
    ) -> Result<Self, MatchingStateErrorV1> {
        Ok(Self {
            queues: ExecutionQueueStateV1::new(scenario),
            account_overlay: AccountBookOverlayStoreV1::new(scenario),
            available_pusd_units: micro_units_v1(initial_account.available_pusd())?,
            principal_encumbered_pusd_units: 0,
            immediate_taker_fee_encumbered_pusd_units: 0,
            orders: BTreeMap::new(),
            fills: BTreeMap::new(),
            matching_closed_markets: BTreeMap::new(),
            gtd_deadlines: BTreeSet::new(),
            gtd_advanced_through_utc_ms: None,
        })
    }

    pub(crate) fn account_overlay(&self) -> &AccountBookOverlayStoreV1 {
        &self.account_overlay
    }

    #[cfg(test)]
    pub(crate) fn account_overlay_mut(&mut self) -> &mut AccountBookOverlayStoreV1 {
        &mut self.account_overlay
    }

    pub(crate) fn available_pusd(&self) -> Result<Decimal, MatchingStateErrorV1> {
        Ok(decimal_from_micro_units_v1(self.available_pusd_units)?)
    }

    pub(crate) fn available_pusd_units(&self) -> u128 {
        self.available_pusd_units
    }

    pub(crate) fn principal_encumbered_pusd(&self) -> Result<Decimal, MatchingStateErrorV1> {
        Ok(decimal_from_micro_units_v1(
            self.principal_encumbered_pusd_units,
        )?)
    }

    pub(crate) fn immediate_taker_fee_encumbered_pusd(
        &self,
    ) -> Result<Decimal, MatchingStateErrorV1> {
        Ok(decimal_from_micro_units_v1(
            self.immediate_taker_fee_encumbered_pusd_units,
        )?)
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

    pub(crate) fn fills(&self) -> impl Iterator<Item = &SimFillRecordV1> {
        self.fills.values()
    }

    pub(crate) fn query_projection_changed_from(&self, previous: &Self) -> bool {
        self.orders != previous.orders || self.fills != previous.fills
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

    pub(crate) fn apply_authorized_market_events(
        &mut self,
        scenario: &PlatformScenarioV1,
        bucket_at_utc_ms: i64,
        events: &[ExecutionMarketEventV3],
    ) -> Result<(), MatchingStateErrorV1> {
        self.advance_gtd_deadlines_through(bucket_at_utc_ms)?;
        for market in scenario.market_set().markets() {
            let condition_id = &market.market().condition_id.0;
            let end_at = market.market().window.end_at_utc_ms;
            if bucket_at_utc_ms >= end_at
                && !self.matching_closed_markets.contains_key(condition_id)
            {
                self.queues.close_account_orders_for_market(condition_id)?;
                self.matching_closed_markets
                    .insert(condition_id.clone(), end_at);
            }
        }
        let maker_depletions = self.queues.apply_authorized_events(events)?;
        for depletion in maker_depletions {
            self.apply_maker_queue_depletion(bucket_at_utc_ms, depletion)?;
        }
        Ok(())
    }

    pub(crate) fn next_gtd_deadline_at_utc_ms(&self) -> Option<i64> {
        self.gtd_deadlines
            .first()
            .map(|(expires_at_utc_ms, _)| *expires_at_utc_ms)
    }

    pub(crate) fn advance_gtd_deadlines_through(
        &mut self,
        logical_time_utc_ms: i64,
    ) -> Result<(), MatchingStateErrorV1> {
        if self
            .gtd_advanced_through_utc_ms
            .is_some_and(|current| logical_time_utc_ms < current)
        {
            return Err(MatchingStateErrorV1::LogicalTimeRegression);
        }
        let expiring = self
            .gtd_deadlines
            .iter()
            .take_while(|(expires_at_utc_ms, _)| *expires_at_utc_ms <= logical_time_utc_ms)
            .cloned()
            .collect::<Vec<_>>();
        for (expires_at_utc_ms, platform_order_id) in expiring {
            self.gtd_deadlines
                .remove(&(expires_at_utc_ms, platform_order_id.clone()));
            let snapshot = self
                .orders
                .get(&platform_order_id)
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
            if snapshot.open_base_units > 0
                && !self
                    .matching_closed_markets
                    .contains_key(&snapshot.condition_id)
                && self
                    .queues
                    .remaining_account_order_base_units(&platform_order_id)
                    != Some(snapshot.open_base_units)
            {
                return Err(MatchingStateErrorV1::QueuePredecessorConflict);
            }
            let next_available = self
                .available_pusd_units
                .checked_add(snapshot.open_quote_units)
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            let next_principal = self
                .principal_encumbered_pusd_units
                .checked_sub(snapshot.open_quote_units)
                .ok_or(MatchingStateErrorV1::QueueUnderflow)?;
            let accepted_base = snapshot
                .accepted_base_units
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
            if accepted_base
                != snapshot
                    .matched_base_units
                    .checked_add(snapshot.expired_base_units)
                    .and_then(|value| value.checked_add(snapshot.canceled_base_units))
                    .and_then(|value| value.checked_add(active_base_units))
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
                || snapshot.accepted_quote_units
                    != snapshot
                        .matched_quote_units
                        .checked_add(snapshot.released_quote_units)
                        .and_then(|value| value.checked_add(snapshot.canceled_released_quote_units))
                        .and_then(|value| value.checked_add(snapshot.expired_released_quote_units))
                        .and_then(|value| value.checked_add(snapshot.open_quote_units))
                        .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
            {
                return Err(MatchingStateErrorV1::QueuePredecessorConflict);
            }

            self.queues.remove_account_order(&platform_order_id);
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
            let order = self.orders.get_mut(&platform_order_id).ok_or_else(|| {
                MatchingStateErrorV1::AccountOrderConflict(platform_order_id.clone())
            })?;
            order.expired_base_units = active_base_units;
            order.expired_released_quote_units = order.open_quote_units;
            order.expired_at_utc_ms = Some(expires_at_utc_ms);
            order.last_updated_at_utc_ms = expires_at_utc_ms;
            order.gtd_expiration_identity = Some(gtd_expiration_identity(
                &order.platform_order_id,
                expires_at_utc_ms,
                order.pending_maker_depletion_base_units,
                order.open_base_units,
                order.open_quote_units,
                order.pending_maker_depletion_evidence.as_ref(),
            ));
            order.terminalized_pending_maker_depletion_evidence =
                order.pending_maker_depletion_evidence.take();
            order.pending_maker_depletion_base_units = 0;
            order.open_base_units = 0;
            order.open_quote_units = 0;
            self.available_pusd_units = next_available;
            self.principal_encumbered_pusd_units = next_principal;
        }
        self.gtd_advanced_through_utc_ms = Some(logical_time_utc_ms);
        Ok(())
    }

    pub(crate) fn cancel_order_remainder(
        &mut self,
        platform_order_id: &str,
        canceled_at_utc_ms: i64,
        caller_cancel_effect_identity: &Sha256Hex,
    ) -> Result<CancelOrderStateDispositionV1, MatchingStateErrorV1> {
        let Some(snapshot) = self.orders.get(platform_order_id).cloned() else {
            return Ok(CancelOrderStateDispositionV1::NotFound);
        };
        let active_base_units = snapshot
            .pending_maker_depletion_base_units
            .checked_add(snapshot.open_base_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        if active_base_units == 0 {
            return Ok(CancelOrderStateDispositionV1::NoCancellableRemainder);
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
            .and_then(|value| value.checked_add(snapshot.open_base_units))
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let quote_total = snapshot
            .matched_quote_units
            .checked_add(snapshot.open_quote_units)
            .and_then(|value| value.checked_add(snapshot.released_quote_units))
            .and_then(|value| value.checked_add(snapshot.canceled_released_quote_units))
            .and_then(|value| value.checked_add(snapshot.expired_released_quote_units))
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        if accepted_base_units != base_total || snapshot.accepted_quote_units != quote_total {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }

        let matching_closed = self
            .matching_closed_markets
            .contains_key(&snapshot.condition_id);
        let queued_base = self
            .queues
            .remaining_account_order_base_units(platform_order_id);
        if matching_closed {
            if queued_base.is_some() {
                return Err(MatchingStateErrorV1::QueuePredecessorConflict);
            }
        } else if snapshot.open_base_units > 0 {
            if queued_base != Some(snapshot.open_base_units) {
                return Err(MatchingStateErrorV1::QueuePredecessorConflict);
            }
        } else if queued_base.is_some() {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        if let SimOrderKindStateV1::Gtd { expires_at_utc_ms } = snapshot.kind
            && !self
                .gtd_deadlines
                .contains(&(expires_at_utc_ms, platform_order_id.to_owned()))
        {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }

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
                .and_then(|value| value.checked_add(next_canceled_base_units))
                .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
            || snapshot.accepted_quote_units
                != snapshot
                    .matched_quote_units
                    .checked_add(snapshot.released_quote_units)
                    .and_then(|value| value.checked_add(snapshot.expired_released_quote_units))
                    .and_then(|value| value.checked_add(next_canceled_released_quote_units))
                    .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?
        {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        let next_available = self
            .available_pusd_units
            .checked_add(snapshot.open_quote_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let next_principal = self
            .principal_encumbered_pusd_units
            .checked_sub(snapshot.open_quote_units)
            .ok_or(MatchingStateErrorV1::QueueUnderflow)?;

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
        if !matching_closed {
            self.queues.remove_account_order(platform_order_id);
        }
        if let SimOrderKindStateV1::Gtd { expires_at_utc_ms } = snapshot.kind {
            self.gtd_deadlines
                .remove(&(expires_at_utc_ms, platform_order_id.to_owned()));
        }
        let order = self.orders.get_mut(platform_order_id).ok_or_else(|| {
            MatchingStateErrorV1::AccountOrderConflict(platform_order_id.to_owned())
        })?;
        order.canceled_base_units = next_canceled_base_units;
        order.canceled_released_quote_units = next_canceled_released_quote_units;
        order.canceled_at_utc_ms = Some(canceled_at_utc_ms);
        order.last_updated_at_utc_ms = canceled_at_utc_ms;
        order.caller_cancel_effect_identity = Some(caller_cancel_effect_identity.clone());
        order.terminalized_pending_maker_depletion_evidence =
            order.pending_maker_depletion_evidence.take();
        order.pending_maker_depletion_base_units = 0;
        order.open_base_units = 0;
        order.open_quote_units = 0;
        self.available_pusd_units = next_available;
        self.principal_encumbered_pusd_units = next_principal;
        Ok(CancelOrderStateDispositionV1::Canceled(
            CallerCancelStateEffectV1 {
                pending_base_units_before: snapshot.pending_maker_depletion_base_units,
                open_base_units_before: snapshot.open_base_units,
                released_open_quote_units: snapshot.open_quote_units,
            },
        ))
    }

    pub(crate) fn commit_new_order(
        &mut self,
        order: SimOrderRecordV1,
        fills: Vec<SimFillRecordV1>,
    ) -> Result<(), MatchingStateErrorV1> {
        if order.platform_order_id.is_empty()
            || self.orders.contains_key(&order.platform_order_id)
            || order.accepted_quote_units
                != order
                    .matched_quote_units
                    .checked_add(order.open_quote_units)
                    .and_then(|value| value.checked_add(order.released_quote_units))
                    .and_then(|value| value.checked_add(order.canceled_released_quote_units))
                    .and_then(|value| value.checked_add(order.expired_released_quote_units))
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
        if fill_base != order.matched_base_units || fill_quote != order.matched_quote_units {
            return Err(MatchingStateErrorV1::QueuePredecessorConflict);
        }
        if fills.iter().any(|fill| {
            fill.platform_order_id != order.platform_order_id
                || self.fills.contains_key(&fill.trade_id)
        }) {
            return Err(MatchingStateErrorV1::AccountOrderConflict(
                order.platform_order_id,
            ));
        }
        let required = order
            .accepted_quote_units
            .checked_add(order.immediate_taker_fee_encumbered_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        if self.available_pusd_units < required {
            return Err(MatchingStateErrorV1::QueueUnderflow);
        }
        let final_principal = order
            .matched_quote_units
            .checked_add(order.open_quote_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let final_debit = final_principal
            .checked_add(order.immediate_taker_fee_encumbered_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let next_available = self
            .available_pusd_units
            .checked_sub(final_debit)
            .ok_or(MatchingStateErrorV1::QueueUnderflow)?;
        let next_principal = self
            .principal_encumbered_pusd_units
            .checked_add(final_principal)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
        let next_fee = self
            .immediate_taker_fee_encumbered_pusd_units
            .checked_add(order.immediate_taker_fee_encumbered_units)
            .ok_or(MatchingStateErrorV1::QueuePredecessorConflict)?;
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
        self.available_pusd_units = next_available;
        self.principal_encumbered_pusd_units = next_principal;
        self.immediate_taker_fee_encumbered_pusd_units = next_fee;
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
        Ok(())
    }

    fn apply_maker_queue_depletion(
        &mut self,
        bucket_at_utc_ms: i64,
        depletion: MakerQueueDepletionV1,
    ) -> Result<(), MatchingStateErrorV1> {
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
            || order
                .terminalized_pending_maker_depletion_evidence
                .is_some()
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
            return Ok(());
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
        self.fills.insert(
            trade_id.clone(),
            SimFillRecordV1 {
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
            },
        );
        Ok(())
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
        if *price <= Decimal::ZERO || *price >= Decimal::ONE {
            return Err(MatchingMathErrorV1::InvalidPrice);
        }
        if *base_units == 0 {
            continue;
        }
        let price = positive_decimal_ratio(*price)?;
        let curve_numerator = &price.numerator * (&price.denominator - &price.numerator);
        let curve_denominator = &price.denominator * &price.denominator;
        let term = ExactRatioV1::new(
            BigUint::from(*base_units) * &rate.numerator * curve_numerator.pow(exponent),
            BigUint::from(1_000_000_u32) * &rate.denominator * curve_denominator.pow(exponent),
        )?;
        total = total.add(term)?;
    }
    rounded_fee_decimal(total)
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

fn rounded_fee_decimal(total: ExactRatioV1) -> Result<Decimal, MatchingMathErrorV1> {
    if total.numerator.is_zero() {
        return Ok(Decimal::ZERO);
    }
    let scaled = total.numerator * BigUint::from(10_u32.pow(FEE_SCALE));
    let (mut rounded_units, remainder) = scaled.div_rem(&total.denominator);
    if (&remainder << 1_usize) >= total.denominator {
        rounded_units += BigUint::one();
    }
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
