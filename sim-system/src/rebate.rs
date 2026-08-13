use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::finality::TradeFinalityStateV1;
use crate::market_directory::SessionMarketDirectoryV1;
use crate::matching::{
    MatchingMathErrorV1, SimFillRecordV1, SimLiquidityRoleV1, decimal_from_micro_units_v1,
    taker_fee_pusd_v1,
};
use crate::platform_rules::SealedProcessRuleSnapshot;
use crate::scenario::PlatformMarketV1;

const UTC_DAY_MILLIS: i64 = 86_400_000;
const TAKER_PAYMENT_MINUTE_UTC: i64 = 10;
const MAKER_PAYMENT_MINUTE_UTC: i64 = 45;
const MILLIS_PER_MINUTE: i64 = 60_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) enum RebateKindV1 {
    Maker,
    Taker,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum MakerRebateMarketResultStatusV1 {
    Calculated,
    ZeroNoEligibleMakerFill,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RebateRecoveryFillV1 {
    pub(crate) trade_id: String,
    pub(crate) condition_id: String,
    pub(crate) matched_at_utc_ms: i64,
    pub(crate) base_units: u128,
    pub(crate) price_pusd_per_share: Decimal,
    pub(crate) immediate_taker_fee_units: u128,
    pub(crate) liquidity_role: SimLiquidityRoleV1,
    pub(crate) terminal_succeeded: Option<bool>,
}

impl RebateRecoveryFillV1 {
    fn from_hot_fill(fill: &SimFillRecordV1) -> Self {
        let terminal_succeeded = match fill.finality_state() {
            Some(TradeFinalityStateV1::Confirmed) => Some(true),
            Some(TradeFinalityStateV1::Failed) => Some(false),
            Some(
                TradeFinalityStateV1::MatchedNotBroadcasted
                | TradeFinalityStateV1::Matched
                | TradeFinalityStateV1::Retrying
                | TradeFinalityStateV1::Mined,
            )
            | None => None,
        };
        Self {
            trade_id: fill.trade_id.clone(),
            condition_id: fill.condition_id.clone(),
            matched_at_utc_ms: fill.matched_at_utc_ms,
            base_units: fill.base_units,
            price_pusd_per_share: fill.price_pusd_per_share,
            immediate_taker_fee_units: fill.immediate_taker_fee_units,
            liquidity_role: fill.liquidity_role,
            terminal_succeeded,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MakerRebateContributionV1 {
    pub(crate) trade_id: String,
    pub(crate) matched_at_utc_ms: i64,
    pub(crate) attribution_day_utc_ms: i64,
    pub(crate) base_units: u128,
    pub(crate) price_pusd_per_share: Decimal,
    pub(crate) fee_equivalent_pusd: Decimal,
    pub(crate) rebate_rate: Decimal,
    pub(crate) contribution_pusd: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MakerRebateMarketFinalizedActionV1 {
    pub(crate) reason_identity: String,
    pub(crate) condition_id: String,
    pub(crate) platform_effective_at_utc_ms: i64,
    pub(crate) status: MakerRebateMarketResultStatusV1,
    pub(crate) contributions: Vec<MakerRebateContributionV1>,
    pub(crate) accrual_pusd: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TakerRebateAccrualAppliedActionV1 {
    pub(crate) reason_identity: String,
    pub(crate) trade_id: String,
    pub(crate) condition_id: String,
    pub(crate) matched_at_utc_ms: i64,
    pub(crate) attribution_day_utc_ms: i64,
    pub(crate) platform_effective_at_utc_ms: i64,
    pub(crate) actual_taker_fee_pusd: Decimal,
    pub(crate) rebate_rate: Decimal,
    pub(crate) accrual_pusd: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RebateDayFinalizedActionV1 {
    pub(crate) reason_identity: String,
    pub(crate) kind: RebateKindV1,
    pub(crate) attribution_day_utc_ms: i64,
    pub(crate) platform_effective_at_utc_ms: i64,
    pub(crate) accrued_pusd: Decimal,
    pub(crate) payable_pusd: Decimal,
    pub(crate) locked_remainder_pusd: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RebatePaidActionV1 {
    pub(crate) reason_identity: String,
    pub(crate) kind: RebateKindV1,
    pub(crate) attribution_day_utc_ms: i64,
    pub(crate) platform_effective_at_utc_ms: i64,
    pub(crate) paid_pusd: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RebateSettlementActionV1 {
    MakerMarketFinalized(MakerRebateMarketFinalizedActionV1),
    TakerAccrualApplied(TakerRebateAccrualAppliedActionV1),
    DayFinalized(RebateDayFinalizedActionV1),
    Paid(RebatePaidActionV1),
}

impl RebateSettlementActionV1 {
    pub(crate) fn reason_identity(&self) -> &str {
        match self {
            Self::MakerMarketFinalized(action) => &action.reason_identity,
            Self::TakerAccrualApplied(action) => &action.reason_identity,
            Self::DayFinalized(action) => &action.reason_identity,
            Self::Paid(action) => &action.reason_identity,
        }
    }

    pub(crate) fn platform_effective_at_utc_ms(&self) -> i64 {
        match self {
            Self::MakerMarketFinalized(action) => action.platform_effective_at_utc_ms,
            Self::TakerAccrualApplied(action) => action.platform_effective_at_utc_ms,
            Self::DayFinalized(action) => action.platform_effective_at_utc_ms,
            Self::Paid(action) => action.platform_effective_at_utc_ms,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RebateStateErrorV1 {
    UnknownMarket(String),
    MarketNotTerminal(String),
    DuplicateTrade(String),
    NegativeActualTakerFee,
    ArithmeticOverflow,
    TimeOverflow,
    FeeMath(MatchingMathErrorV1),
}

impl fmt::Display for RebateStateErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownMarket(condition_id) => {
                write!(formatter, "返佣状态不存在市场：{condition_id}")
            }
            Self::MarketNotTerminal(condition_id) => {
                write!(formatter, "返佣市场仍有未闭合逐成交前驱：{condition_id}")
            }
            Self::DuplicateTrade(trade_id) => write!(formatter, "返佣成交身份重复：{trade_id}"),
            Self::NegativeActualTakerFee => write!(formatter, "实际 taker 手续费不能为负数"),
            Self::ArithmeticOverflow => write!(formatter, "返佣十进制运算溢出"),
            Self::TimeOverflow => write!(formatter, "返佣 UTC 日结时点溢出"),
            Self::FeeMath(error) => write!(formatter, "maker 手续费等价金额计算失败：{error}"),
        }
    }
}

impl std::error::Error for RebateStateErrorV1 {}

impl From<MatchingMathErrorV1> for RebateStateErrorV1 {
    fn from(value: MatchingMathErrorV1) -> Self {
        Self::FeeMath(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct MakerFillStateV1 {
    contribution: MakerRebateContributionV1,
    terminal_succeeded: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct MakerMarketStateV1 {
    end_at_utc_ms: i64,
    end_reached: bool,
    fills: BTreeMap<String, MakerFillStateV1>,
    result: Option<MakerRebateMarketResultStatusV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TakerFillStateV1 {
    condition_id: String,
    matched_at_utc_ms: i64,
    attribution_day_utc_ms: i64,
    actual_taker_fee_pusd: Decimal,
    rebate_rate: Decimal,
    accrual_pusd: Decimal,
    terminal_succeeded: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RebateDayFinalStateV1 {
    payable_pusd: Decimal,
    locked_remainder_pusd: Decimal,
    paid: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RebateDayBucketV1 {
    maturity_at_utc_ms: i64,
    maturity_reached: bool,
    pending_predecessors: BTreeSet<String>,
    potential_accrual_pusd: Decimal,
    accrued_pusd: Decimal,
    final_state: Option<RebateDayFinalStateV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
enum RebateClockIdentityV1 {
    MarketEnd(String),
    Day(RebateKindV1, i64),
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RebateDaySnapshotV1 {
    pub(crate) maturity_at_utc_ms: i64,
    pub(crate) maturity_reached: bool,
    pub(crate) pending_predecessor_count: usize,
    pub(crate) accrued_pusd: Decimal,
    pub(crate) finalized: bool,
    pub(crate) payable_pusd: Decimal,
    pub(crate) locked_remainder_pusd: Decimal,
    pub(crate) paid: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RebateStateV1 {
    fee_rate: Decimal,
    fee_exponent: u32,
    maker_rebate_rate: Decimal,
    taker_rebate_rate: Decimal,
    markets: BTreeMap<String, MakerMarketStateV1>,
    maker_trade_conditions: BTreeMap<String, String>,
    taker_fills: BTreeMap<String, TakerFillStateV1>,
    #[serde(with = "rebate_day_buckets_serde")]
    day_buckets: BTreeMap<(RebateKindV1, i64), RebateDayBucketV1>,
    clocks: BTreeSet<(i64, RebateClockIdentityV1)>,
}

mod rebate_day_buckets_serde {
    use super::*;

    pub(super) fn serialize<S>(
        value: &BTreeMap<(RebateKindV1, i64), RebateDayBucketV1>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value
            .iter()
            .map(|((kind, day), bucket)| (*kind, *day, bucket))
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<(RebateKindV1, i64), RebateDayBucketV1>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = Vec::<(RebateKindV1, i64, RebateDayBucketV1)>::deserialize(deserializer)?;
        let mut value = BTreeMap::new();
        for (kind, day, bucket) in entries {
            if value.insert((kind, day), bucket).is_some() {
                return Err(serde::de::Error::custom("返佣日桶键重复"));
            }
        }
        Ok(value)
    }
}

impl RebateStateV1 {
    pub(crate) fn resource_record_count(&self) -> usize {
        self.markets
            .len()
            .saturating_add(
                self.markets
                    .values()
                    .map(|market| market.fills.len())
                    .sum::<usize>(),
            )
            .saturating_add(self.maker_trade_conditions.len())
            .saturating_add(self.taker_fills.len())
            .saturating_add(self.day_buckets.len())
            .saturating_add(
                self.day_buckets
                    .values()
                    .map(|bucket| bucket.pending_predecessors.len())
                    .sum::<usize>(),
            )
            .saturating_add(self.clocks.len())
    }

    pub(crate) fn new(
        directory: &SessionMarketDirectoryV1,
        rules: &SealedProcessRuleSnapshot,
    ) -> Self {
        let synchronized = rules.synchronized_rules();
        let project_rebate = &rules
            .snapshot()
            .project_account_rules
            .rebate_settlement
            .value;
        let mut state = Self {
            fee_rate: synchronized.fee_schedule.rate.value(),
            fee_exponent: synchronized.fee_schedule.exponent,
            maker_rebate_rate: synchronized.fee_schedule.rebate_rate.value(),
            taker_rebate_rate: project_rebate.fixed_taker_rebate_rate.value(),
            markets: BTreeMap::new(),
            maker_trade_conditions: BTreeMap::new(),
            taker_fills: BTreeMap::new(),
            day_buckets: BTreeMap::new(),
            clocks: BTreeSet::new(),
        };
        for market in directory.markets() {
            state.insert_market(market);
        }
        state
    }

    pub(crate) fn insert_market(&mut self, market: &PlatformMarketV1) -> bool {
        let condition_id = market.market().condition_id.0.clone();
        if self.markets.contains_key(&condition_id) {
            return false;
        }
        let end_at_utc_ms = market.market().window.end_at_utc_ms;
        self.markets.insert(
            condition_id.clone(),
            MakerMarketStateV1 {
                end_at_utc_ms,
                end_reached: false,
                fills: BTreeMap::new(),
                result: None,
            },
        );
        self.clocks.insert((
            end_at_utc_ms,
            RebateClockIdentityV1::MarketEnd(condition_id),
        ));
        true
    }

    pub(crate) fn evict_terminal_market_fill_state(
        &mut self,
        condition_id: &str,
    ) -> Result<(), RebateStateErrorV1> {
        let market = self
            .markets
            .get(condition_id)
            .ok_or_else(|| RebateStateErrorV1::UnknownMarket(condition_id.to_owned()))?;
        if !market.end_reached
            || market.result.is_none()
            || market
                .fills
                .values()
                .any(|fill| fill.terminal_succeeded.is_none())
            || self
                .taker_fills
                .values()
                .any(|fill| fill.condition_id == condition_id && fill.terminal_succeeded.is_none())
        {
            return Err(RebateStateErrorV1::MarketNotTerminal(
                condition_id.to_owned(),
            ));
        }
        self.markets
            .get_mut(condition_id)
            .expect("市场存在性已在驱逐入口核验")
            .fills
            .clear();
        self.maker_trade_conditions
            .retain(|_, stored_condition_id| stored_condition_id != condition_id);
        self.taker_fills
            .retain(|_, fill| fill.condition_id != condition_id);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn retained_fill_state_count(&self) -> usize {
        self.markets
            .values()
            .map(|market| market.fills.len())
            .sum::<usize>()
            .checked_add(self.taker_fills.len())
            .expect("测试返佣逐成交状态计数不得溢出")
    }

    pub(crate) fn has_consistent_recovery_state(
        &self,
        directory: &SessionMarketDirectoryV1,
        rules: &SealedProcessRuleSnapshot,
        fills: &BTreeMap<String, SimFillRecordV1>,
        evicted_fills: &BTreeMap<String, RebateRecoveryFillV1>,
        evicted_condition_ids: &BTreeSet<String>,
    ) -> bool {
        let synchronized = rules.synchronized_rules();
        let project_rebate = &rules
            .snapshot()
            .project_account_rules
            .rebate_settlement
            .value;
        if self.fee_rate != synchronized.fee_schedule.rate.value()
            || self.fee_exponent != synchronized.fee_schedule.exponent
            || self.maker_rebate_rate != synchronized.fee_schedule.rebate_rate.value()
            || self.taker_rebate_rate != project_rebate.fixed_taker_rebate_rate.value()
            || self.markets.len() != directory.markets().len()
            || directory.markets().any(|market| {
                self.markets
                    .get(&market.market().condition_id.0)
                    .is_none_or(|state| state.end_at_utc_ms != market.market().window.end_at_utc_ms)
            })
        {
            return false;
        }
        let expected_clocks = self
            .markets
            .iter()
            .filter(|(_, market)| !market.end_reached)
            .map(|(condition_id, market)| {
                (
                    market.end_at_utc_ms,
                    RebateClockIdentityV1::MarketEnd(condition_id.clone()),
                )
            })
            .chain(
                self.day_buckets
                    .iter()
                    .filter(|(_, bucket)| !bucket.maturity_reached)
                    .map(|((kind, day), bucket)| {
                        (
                            bucket.maturity_at_utc_ms,
                            RebateClockIdentityV1::Day(*kind, *day),
                        )
                    }),
            )
            .collect::<BTreeSet<_>>();
        if self.clocks != expected_clocks {
            return false;
        }

        let mut recovery_fills = fills
            .iter()
            .map(|(trade_id, fill)| (trade_id.clone(), RebateRecoveryFillV1::from_hot_fill(fill)))
            .collect::<BTreeMap<_, _>>();
        for (trade_id, fill) in evicted_fills {
            if recovery_fills
                .insert(trade_id.clone(), fill.clone())
                .is_some()
            {
                return false;
            }
        }

        let mut expected_maker_trade_conditions = BTreeMap::new();
        let mut expected_taker_trade_ids = BTreeSet::new();
        let mut evicted_maker_success = BTreeSet::new();
        let mut expected_day_values =
            BTreeMap::<(RebateKindV1, i64), (Decimal, Decimal, BTreeSet<String>)>::new();

        for (trade_id, fill) in &recovery_fills {
            let is_evicted = evicted_condition_ids.contains(&fill.condition_id);
            if trade_id != &fill.trade_id
                || is_evicted != evicted_fills.contains_key(trade_id)
                || !self.markets.contains_key(&fill.condition_id)
                || (is_evicted && fill.terminal_succeeded.is_none())
            {
                return false;
            }
            match fill.liquidity_role {
                SimLiquidityRoleV1::MakerInferred => {
                    let Ok(attribution_day_utc_ms) = utc_day_start(fill.matched_at_utc_ms) else {
                        return false;
                    };
                    let Ok(fee_equivalent_pusd) = taker_fee_pusd_v1(
                        &[(fill.base_units, fill.price_pusd_per_share)],
                        self.fee_rate,
                        self.fee_exponent,
                    ) else {
                        return false;
                    };
                    let Some(contribution_pusd) = fee_equivalent_pusd
                        .checked_mul(self.maker_rebate_rate)
                        .map(|value| value.normalize())
                    else {
                        return false;
                    };
                    if is_evicted {
                        if self
                            .markets
                            .get(&fill.condition_id)
                            .is_none_or(|market| market.fills.contains_key(trade_id))
                            || self.maker_trade_conditions.contains_key(trade_id)
                        {
                            return false;
                        }
                        if fill.terminal_succeeded == Some(true) {
                            evicted_maker_success.insert(fill.condition_id.clone());
                        }
                    } else {
                        let Some(recovered) = self
                            .markets
                            .get(&fill.condition_id)
                            .and_then(|market| market.fills.get(trade_id))
                        else {
                            return false;
                        };
                        if recovered.contribution.trade_id != fill.trade_id
                            || recovered.contribution.matched_at_utc_ms != fill.matched_at_utc_ms
                            || recovered.contribution.attribution_day_utc_ms
                                != attribution_day_utc_ms
                            || recovered.contribution.base_units != fill.base_units
                            || recovered.contribution.price_pusd_per_share
                                != fill.price_pusd_per_share
                            || recovered.contribution.fee_equivalent_pusd != fee_equivalent_pusd
                            || recovered.contribution.rebate_rate != self.maker_rebate_rate
                            || recovered.contribution.contribution_pusd != contribution_pusd
                            || recovered.terminal_succeeded != fill.terminal_succeeded
                            || expected_maker_trade_conditions
                                .insert(trade_id.clone(), fill.condition_id.clone())
                                .is_some()
                        {
                            return false;
                        }
                    }
                    let value = expected_day_values
                        .entry((RebateKindV1::Maker, attribution_day_utc_ms))
                        .or_insert((Decimal::ZERO, Decimal::ZERO, BTreeSet::new()));
                    let Some(potential) = value
                        .0
                        .checked_add(contribution_pusd)
                        .map(|value| value.normalize())
                    else {
                        return false;
                    };
                    value.0 = potential;
                    let market_result = self
                        .markets
                        .get(&fill.condition_id)
                        .and_then(|market| market.result);
                    if market_result.is_none() {
                        value.2.insert(trade_id.clone());
                    } else if fill.terminal_succeeded == Some(true) {
                        let Some(accrued) = value
                            .1
                            .checked_add(contribution_pusd)
                            .map(|value| value.normalize())
                        else {
                            return false;
                        };
                        value.1 = accrued;
                    }
                }
                SimLiquidityRoleV1::Taker if fill.immediate_taker_fee_units > 0 => {
                    let Ok(attribution_day_utc_ms) = utc_day_start(fill.matched_at_utc_ms) else {
                        return false;
                    };
                    let Ok(actual_taker_fee_pusd) =
                        decimal_from_micro_units_v1(fill.immediate_taker_fee_units)
                    else {
                        return false;
                    };
                    let Some(accrual_pusd) = actual_taker_fee_pusd
                        .checked_mul(self.taker_rebate_rate)
                        .map(|value| value.normalize())
                    else {
                        return false;
                    };
                    if is_evicted {
                        if self.taker_fills.contains_key(trade_id) {
                            return false;
                        }
                    } else {
                        let Some(recovered) = self.taker_fills.get(trade_id) else {
                            return false;
                        };
                        if recovered.condition_id != fill.condition_id
                            || recovered.matched_at_utc_ms != fill.matched_at_utc_ms
                            || recovered.attribution_day_utc_ms != attribution_day_utc_ms
                            || recovered.actual_taker_fee_pusd != actual_taker_fee_pusd
                            || recovered.rebate_rate != self.taker_rebate_rate
                            || recovered.accrual_pusd != accrual_pusd
                            || recovered.terminal_succeeded != fill.terminal_succeeded
                            || !expected_taker_trade_ids.insert(trade_id.clone())
                        {
                            return false;
                        }
                    }
                    let value = expected_day_values
                        .entry((RebateKindV1::Taker, attribution_day_utc_ms))
                        .or_insert((Decimal::ZERO, Decimal::ZERO, BTreeSet::new()));
                    let Some(potential) = value
                        .0
                        .checked_add(accrual_pusd)
                        .map(|value| value.normalize())
                    else {
                        return false;
                    };
                    value.0 = potential;
                    match fill.terminal_succeeded {
                        Some(true) => {
                            let Some(accrued) = value
                                .1
                                .checked_add(accrual_pusd)
                                .map(|value| value.normalize())
                            else {
                                return false;
                            };
                            value.1 = accrued;
                        }
                        Some(false) => {}
                        None => {
                            value.2.insert(trade_id.clone());
                        }
                    }
                }
                SimLiquidityRoleV1::Taker => {
                    if self.taker_fills.contains_key(trade_id)
                        || self.maker_trade_conditions.contains_key(trade_id)
                    {
                        return false;
                    }
                }
            }
        }

        if self.maker_trade_conditions != expected_maker_trade_conditions
            || self.taker_fills.len() != expected_taker_trade_ids.len()
            || self
                .taker_fills
                .keys()
                .any(|trade_id| !expected_taker_trade_ids.contains(trade_id))
            || self
                .markets
                .values()
                .map(|market| market.fills.len())
                .sum::<usize>()
                != expected_maker_trade_conditions.len()
        {
            return false;
        }

        for (condition_id, market) in &self.markets {
            let valid = if evicted_condition_ids.contains(condition_id) {
                market.end_reached
                    && market.fills.is_empty()
                    && market.result
                        == Some(if evicted_maker_success.contains(condition_id) {
                            MakerRebateMarketResultStatusV1::Calculated
                        } else {
                            MakerRebateMarketResultStatusV1::ZeroNoEligibleMakerFill
                        })
            } else {
                market.result == ready_maker_market_result(market)
            };
            if !valid {
                return false;
            }
        }

        if self.day_buckets.len() != expected_day_values.len() {
            return false;
        }
        for (key, (potential, accrued, pending)) in expected_day_values {
            let Some(bucket) = self.day_buckets.get(&key) else {
                return false;
            };
            let Ok(maturity_at_utc_ms) = rebate_maturity_at(key.0, key.1) else {
                return false;
            };
            if bucket.maturity_at_utc_ms != maturity_at_utc_ms
                || bucket.potential_accrual_pusd != potential
                || bucket.accrued_pusd != accrued
                || bucket.pending_predecessors != pending
            {
                return false;
            }
            let expected_final = (bucket.maturity_reached
                && bucket.pending_predecessors.is_empty())
            .then(|| finalized_day_state(accrued));
            if bucket.final_state != expected_final {
                return false;
            }
        }
        true
    }

    pub(crate) fn register_maker_fill(
        &mut self,
        trade_id: &str,
        condition_id: &str,
        matched_at_utc_ms: i64,
        base_units: u128,
        price_pusd_per_share: Decimal,
    ) -> Result<(), RebateStateErrorV1> {
        if !self.markets.contains_key(condition_id) {
            return Err(RebateStateErrorV1::UnknownMarket(condition_id.to_owned()));
        }
        self.ensure_new_trade(trade_id)?;

        let attribution_day_utc_ms = utc_day_start(matched_at_utc_ms)?;
        let fee_equivalent_pusd = taker_fee_pusd_v1(
            &[(base_units, price_pusd_per_share)],
            self.fee_rate,
            self.fee_exponent,
        )?;
        let contribution_pusd = fee_equivalent_pusd
            .checked_mul(self.maker_rebate_rate)
            .ok_or(RebateStateErrorV1::ArithmeticOverflow)?
            .normalize();
        self.ensure_potential_total(
            RebateKindV1::Maker,
            attribution_day_utc_ms,
            contribution_pusd,
        )?;

        let contribution = MakerRebateContributionV1 {
            trade_id: trade_id.to_owned(),
            matched_at_utc_ms,
            attribution_day_utc_ms,
            base_units,
            price_pusd_per_share,
            fee_equivalent_pusd,
            rebate_rate: self.maker_rebate_rate,
            contribution_pusd,
        };
        self.markets
            .get_mut(condition_id)
            .expect("市场存在性已在注册入口核验")
            .fills
            .insert(
                trade_id.to_owned(),
                MakerFillStateV1 {
                    contribution,
                    terminal_succeeded: None,
                },
            );
        self.maker_trade_conditions
            .insert(trade_id.to_owned(), condition_id.to_owned());
        self.day_buckets
            .get_mut(&(RebateKindV1::Maker, attribution_day_utc_ms))
            .expect("注册前已建立 maker 归属日桶")
            .pending_predecessors
            .insert(trade_id.to_owned());
        Ok(())
    }

    pub(crate) fn register_taker_fill(
        &mut self,
        trade_id: &str,
        condition_id: &str,
        matched_at_utc_ms: i64,
        actual_taker_fee_pusd: Decimal,
    ) -> Result<bool, RebateStateErrorV1> {
        if !self.markets.contains_key(condition_id) {
            return Err(RebateStateErrorV1::UnknownMarket(condition_id.to_owned()));
        }
        if actual_taker_fee_pusd.is_sign_negative() {
            return Err(RebateStateErrorV1::NegativeActualTakerFee);
        }
        if actual_taker_fee_pusd.is_zero() {
            return Ok(false);
        }
        self.ensure_new_trade(trade_id)?;

        let attribution_day_utc_ms = utc_day_start(matched_at_utc_ms)?;
        let accrual_pusd = actual_taker_fee_pusd
            .checked_mul(self.taker_rebate_rate)
            .ok_or(RebateStateErrorV1::ArithmeticOverflow)?
            .normalize();
        self.ensure_potential_total(RebateKindV1::Taker, attribution_day_utc_ms, accrual_pusd)?;

        self.taker_fills.insert(
            trade_id.to_owned(),
            TakerFillStateV1 {
                condition_id: condition_id.to_owned(),
                matched_at_utc_ms,
                attribution_day_utc_ms,
                actual_taker_fee_pusd,
                rebate_rate: self.taker_rebate_rate,
                accrual_pusd,
                terminal_succeeded: None,
            },
        );
        self.day_buckets
            .get_mut(&(RebateKindV1::Taker, attribution_day_utc_ms))
            .expect("注册前已建立 taker 归属日桶")
            .pending_predecessors
            .insert(trade_id.to_owned());
        Ok(true)
    }

    pub(crate) fn on_fill_terminal(
        &mut self,
        trade_id: &str,
        succeeded: bool,
        platform_effective_at_utc_ms: i64,
    ) -> Vec<RebateSettlementActionV1> {
        let mut immediate_actions = Vec::new();
        let mut affected_markets = BTreeSet::new();
        let affected_maker_days = BTreeSet::new();
        let mut affected_taker_days = BTreeSet::new();
        if let Some(condition_id) = self.maker_trade_conditions.get(trade_id).cloned() {
            let fill = self
                .markets
                .get_mut(&condition_id)
                .and_then(|market| market.fills.get_mut(trade_id))
                .expect("maker 成交索引只指向已注册成交");
            if fill.terminal_succeeded.is_some() {
                return immediate_actions;
            }
            fill.terminal_succeeded = Some(succeeded);
            affected_markets.insert(condition_id);
        } else if let Some(fill) = self.taker_fills.get_mut(trade_id) {
            if fill.terminal_succeeded.is_some() {
                return immediate_actions;
            }
            fill.terminal_succeeded = Some(succeeded);
            let day = fill.attribution_day_utc_ms;
            affected_taker_days.insert(day);
            let bucket = self
                .day_buckets
                .get_mut(&(RebateKindV1::Taker, day))
                .expect("taker 成交只指向已注册日桶");
            bucket.pending_predecessors.remove(trade_id);
            if succeeded {
                bucket.accrued_pusd = bucket
                    .accrued_pusd
                    .checked_add(fill.accrual_pusd)
                    .expect("已预检的潜在日应计总额保证成功子集可表示")
                    .normalize();
                immediate_actions.push(RebateSettlementActionV1::TakerAccrualApplied(
                    TakerRebateAccrualAppliedActionV1 {
                        reason_identity: taker_accrual_reason(trade_id),
                        trade_id: trade_id.to_owned(),
                        condition_id: fill.condition_id.clone(),
                        matched_at_utc_ms: fill.matched_at_utc_ms,
                        attribution_day_utc_ms: day,
                        platform_effective_at_utc_ms,
                        actual_taker_fee_pusd: fill.actual_taker_fee_pusd,
                        rebate_rate: fill.rebate_rate,
                        accrual_pusd: fill.accrual_pusd,
                    },
                ));
            }
        } else {
            return immediate_actions;
        }

        immediate_actions.extend(self.drain_affected(
            platform_effective_at_utc_ms,
            affected_markets,
            affected_maker_days,
            affected_taker_days,
        ));
        immediate_actions
    }

    pub(crate) fn next_clock_at_utc_ms(&self) -> Option<i64> {
        self.clocks.first().map(|(at, _)| *at)
    }

    pub(crate) fn advance_clock_at(
        &mut self,
        exact_next_at_utc_ms: i64,
    ) -> Vec<RebateSettlementActionV1> {
        if self.next_clock_at_utc_ms() != Some(exact_next_at_utc_ms) {
            return Vec::new();
        }
        let keys = self
            .clocks
            .iter()
            .take_while(|(at, _)| *at == exact_next_at_utc_ms)
            .cloned()
            .collect::<Vec<_>>();
        let mut affected_markets = BTreeSet::new();
        let mut affected_maker_days = BTreeSet::new();
        let mut affected_taker_days = BTreeSet::new();
        for key in keys {
            self.clocks.remove(&key);
            match key.1 {
                RebateClockIdentityV1::MarketEnd(condition_id) => {
                    self.markets
                        .get_mut(&condition_id)
                        .expect("市场时钟只指向已登记市场")
                        .end_reached = true;
                    affected_markets.insert(condition_id);
                }
                RebateClockIdentityV1::Day(kind, day) => {
                    self.day_buckets
                        .get_mut(&(kind, day))
                        .expect("日结时钟只指向已登记日桶")
                        .maturity_reached = true;
                    match kind {
                        RebateKindV1::Maker => {
                            affected_maker_days.insert(day);
                        }
                        RebateKindV1::Taker => {
                            affected_taker_days.insert(day);
                        }
                    }
                }
            }
        }
        self.drain_affected(
            exact_next_at_utc_ms,
            affected_markets,
            affected_maker_days,
            affected_taker_days,
        )
    }

    pub(crate) fn market_result(
        &self,
        condition_id: &str,
    ) -> Option<MakerRebateMarketResultStatusV1> {
        self.markets.get(condition_id)?.result
    }

    #[cfg(test)]
    pub(crate) fn day_snapshot(
        &self,
        kind: RebateKindV1,
        attribution_day_utc_ms: i64,
    ) -> Option<RebateDaySnapshotV1> {
        let bucket = self.day_buckets.get(&(kind, attribution_day_utc_ms))?;
        let (finalized, payable_pusd, locked_remainder_pusd, paid) = bucket
            .final_state
            .as_ref()
            .map(|state| {
                (
                    true,
                    state.payable_pusd,
                    state.locked_remainder_pusd,
                    state.paid,
                )
            })
            .unwrap_or((false, Decimal::ZERO, Decimal::ZERO, false));
        Some(RebateDaySnapshotV1 {
            maturity_at_utc_ms: bucket.maturity_at_utc_ms,
            maturity_reached: bucket.maturity_reached,
            pending_predecessor_count: bucket.pending_predecessors.len(),
            accrued_pusd: bucket.accrued_pusd,
            finalized,
            payable_pusd,
            locked_remainder_pusd,
            paid,
        })
    }

    fn ensure_new_trade(&self, trade_id: &str) -> Result<(), RebateStateErrorV1> {
        if self.maker_trade_conditions.contains_key(trade_id)
            || self.taker_fills.contains_key(trade_id)
        {
            return Err(RebateStateErrorV1::DuplicateTrade(trade_id.to_owned()));
        }
        Ok(())
    }

    fn ensure_potential_total(
        &mut self,
        kind: RebateKindV1,
        attribution_day_utc_ms: i64,
        contribution_pusd: Decimal,
    ) -> Result<(), RebateStateErrorV1> {
        let maturity_at_utc_ms = rebate_maturity_at(kind, attribution_day_utc_ms)?;
        let key = (kind, attribution_day_utc_ms);
        let next_potential_accrual_pusd = self
            .day_buckets
            .get(&key)
            .map_or(Ok(contribution_pusd), |bucket| {
                bucket
                    .potential_accrual_pusd
                    .checked_add(contribution_pusd)
                    .ok_or(RebateStateErrorV1::ArithmeticOverflow)
            })?
            .normalize();
        if !self.day_buckets.contains_key(&key) {
            self.clocks.insert((
                maturity_at_utc_ms,
                RebateClockIdentityV1::Day(kind, attribution_day_utc_ms),
            ));
        }
        let bucket = self
            .day_buckets
            .entry(key)
            .or_insert_with(|| RebateDayBucketV1 {
                maturity_at_utc_ms,
                maturity_reached: false,
                pending_predecessors: BTreeSet::new(),
                potential_accrual_pusd: Decimal::ZERO,
                accrued_pusd: Decimal::ZERO,
                final_state: None,
            });
        bucket.potential_accrual_pusd = next_potential_accrual_pusd;
        Ok(())
    }

    fn drain_affected(
        &mut self,
        platform_effective_at_utc_ms: i64,
        affected_markets: BTreeSet<String>,
        mut affected_maker_days: BTreeSet<i64>,
        affected_taker_days: BTreeSet<i64>,
    ) -> Vec<RebateSettlementActionV1> {
        let mut actions = Vec::new();
        for condition_id in affected_markets {
            let ready = self.markets.get(&condition_id).is_some_and(|market| {
                market.result.is_none() && ready_maker_market_result(market).is_some()
            });
            if ready {
                let (action, days) =
                    self.finalize_maker_market(&condition_id, platform_effective_at_utc_ms);
                affected_maker_days.extend(days);
                actions.push(RebateSettlementActionV1::MakerMarketFinalized(action));
            }
        }

        let mut finalized_days = Vec::new();
        for (kind, days) in [
            (RebateKindV1::Maker, affected_maker_days),
            (RebateKindV1::Taker, affected_taker_days),
        ] {
            for day in days {
                let ready = self.day_buckets.get(&(kind, day)).is_some_and(|bucket| {
                    bucket.maturity_reached
                        && bucket.pending_predecessors.is_empty()
                        && bucket.final_state.is_none()
                });
                if ready {
                    actions.push(RebateSettlementActionV1::DayFinalized(self.finalize_day(
                        kind,
                        day,
                        platform_effective_at_utc_ms,
                    )));
                    finalized_days.push((kind, day));
                }
            }
        }

        for kind in [RebateKindV1::Maker, RebateKindV1::Taker] {
            for &(_, day) in finalized_days
                .iter()
                .filter(|(finalized_kind, _)| *finalized_kind == kind)
            {
                let bucket = self
                    .day_buckets
                    .get_mut(&(kind, day))
                    .expect("刚完成的日结桶必须仍存在");
                let final_state = bucket
                    .final_state
                    .as_mut()
                    .expect("刚完成的日结桶必须有终态");
                if final_state.payable_pusd > Decimal::ZERO {
                    final_state.paid = true;
                    actions.push(RebateSettlementActionV1::Paid(RebatePaidActionV1 {
                        reason_identity: payment_reason(kind, day),
                        kind,
                        attribution_day_utc_ms: day,
                        platform_effective_at_utc_ms,
                        paid_pusd: final_state.payable_pusd,
                    }));
                }
            }
        }
        actions
    }

    fn finalize_maker_market(
        &mut self,
        condition_id: &str,
        platform_effective_at_utc_ms: i64,
    ) -> (MakerRebateMarketFinalizedActionV1, BTreeSet<i64>) {
        let fill_snapshots = self
            .markets
            .get(condition_id)
            .expect("待最终化市场来自当前状态索引")
            .fills
            .values()
            .map(|fill| {
                (
                    fill.contribution.clone(),
                    fill.terminal_succeeded
                        .expect("待最终化市场的 maker 成交均已终态"),
                )
            })
            .collect::<Vec<_>>();
        let contributions = fill_snapshots
            .iter()
            .filter(|(_, succeeded)| *succeeded)
            .map(|(contribution, _)| contribution.clone())
            .collect::<Vec<_>>();
        let affected_days = fill_snapshots
            .iter()
            .map(|(contribution, _)| contribution.attribution_day_utc_ms)
            .collect::<BTreeSet<_>>();
        let status = ready_maker_market_result(
            self.markets
                .get(condition_id)
                .expect("待最终化市场来自当前状态索引"),
        )
        .expect("待最终化市场必须已经闭合全部 maker 成交前驱");
        let accrual_pusd = contributions
            .iter()
            .fold(Decimal::ZERO, |total, contribution| {
                total
                    .checked_add(contribution.contribution_pusd)
                    .expect("已预检的潜在日应计总额保证成功子集可表示")
            })
            .normalize();

        for (contribution, succeeded) in &fill_snapshots {
            let bucket = self
                .day_buckets
                .get_mut(&(RebateKindV1::Maker, contribution.attribution_day_utc_ms))
                .expect("maker 成交只指向已注册日桶");
            bucket.pending_predecessors.remove(&contribution.trade_id);
            if *succeeded {
                bucket.accrued_pusd = bucket
                    .accrued_pusd
                    .checked_add(contribution.contribution_pusd)
                    .expect("已预检的潜在日应计总额保证成功子集可表示")
                    .normalize();
            }
        }
        self.markets
            .get_mut(condition_id)
            .expect("待最终化市场来自当前状态索引")
            .result = Some(status);

        (
            MakerRebateMarketFinalizedActionV1 {
                reason_identity: maker_market_reason(condition_id),
                condition_id: condition_id.to_owned(),
                platform_effective_at_utc_ms,
                status,
                contributions,
                accrual_pusd,
            },
            affected_days,
        )
    }

    fn finalize_day(
        &mut self,
        kind: RebateKindV1,
        attribution_day_utc_ms: i64,
        platform_effective_at_utc_ms: i64,
    ) -> RebateDayFinalizedActionV1 {
        let bucket = self
            .day_buckets
            .get_mut(&(kind, attribution_day_utc_ms))
            .expect("待日结桶来自当前状态索引");
        let accrued_pusd = bucket.accrued_pusd.normalize();
        let mut final_state = finalized_day_state(accrued_pusd);
        final_state.paid = false;
        let payable_pusd = final_state.payable_pusd;
        let locked_remainder_pusd = final_state.locked_remainder_pusd;
        bucket.final_state = Some(final_state);
        RebateDayFinalizedActionV1 {
            reason_identity: day_finalization_reason(kind, attribution_day_utc_ms),
            kind,
            attribution_day_utc_ms,
            platform_effective_at_utc_ms,
            accrued_pusd,
            payable_pusd,
            locked_remainder_pusd,
        }
    }
}

fn ready_maker_market_result(
    market: &MakerMarketStateV1,
) -> Option<MakerRebateMarketResultStatusV1> {
    (market.end_reached
        && market
            .fills
            .values()
            .all(|fill| fill.terminal_succeeded.is_some()))
    .then(|| {
        if market
            .fills
            .values()
            .any(|fill| fill.terminal_succeeded == Some(true))
        {
            MakerRebateMarketResultStatusV1::Calculated
        } else {
            MakerRebateMarketResultStatusV1::ZeroNoEligibleMakerFill
        }
    })
}

fn finalized_day_state(accrued_pusd: Decimal) -> RebateDayFinalStateV1 {
    let payable_pusd = if accrued_pusd >= Decimal::ONE {
        accrued_pusd.trunc_with_scale(4).normalize()
    } else {
        Decimal::ZERO
    };
    let locked_remainder_pusd = if payable_pusd > Decimal::ZERO {
        accrued_pusd
            .checked_sub(payable_pusd)
            .expect("付款金额不超过非负日应计")
            .normalize()
    } else {
        Decimal::ZERO
    };
    RebateDayFinalStateV1 {
        payable_pusd,
        locked_remainder_pusd,
        paid: payable_pusd > Decimal::ZERO,
    }
}

fn utc_day_start(timestamp_utc_ms: i64) -> Result<i64, RebateStateErrorV1> {
    timestamp_utc_ms
        .div_euclid(UTC_DAY_MILLIS)
        .checked_mul(UTC_DAY_MILLIS)
        .ok_or(RebateStateErrorV1::TimeOverflow)
}

fn rebate_maturity_at(
    kind: RebateKindV1,
    attribution_day_utc_ms: i64,
) -> Result<i64, RebateStateErrorV1> {
    let minute = match kind {
        RebateKindV1::Maker => MAKER_PAYMENT_MINUTE_UTC,
        RebateKindV1::Taker => TAKER_PAYMENT_MINUTE_UTC,
    };
    attribution_day_utc_ms
        .checked_add(UTC_DAY_MILLIS)
        .and_then(|value| value.checked_add(minute * MILLIS_PER_MINUTE))
        .ok_or(RebateStateErrorV1::TimeOverflow)
}

fn maker_market_reason(condition_id: &str) -> String {
    format!("maker-rebate-market-finalized-v1:{condition_id}")
}

fn taker_accrual_reason(trade_id: &str) -> String {
    format!("taker-rebate-accrual-applied-v1:{trade_id}")
}

fn day_finalization_reason(kind: RebateKindV1, attribution_day_utc_ms: i64) -> String {
    format!(
        "{}-rebate-day-finalized-v1:{attribution_day_utc_ms}",
        kind_label(kind)
    )
}

fn payment_reason(kind: RebateKindV1, attribution_day_utc_ms: i64) -> String {
    format!(
        "{}-rebate-paid-v1:{attribution_day_utc_ms}",
        kind_label(kind)
    )
}

fn kind_label(kind: RebateKindV1) -> &'static str {
    match kind {
        RebateKindV1::Maker => "maker",
        RebateKindV1::Taker => "taker",
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use contracts::{
        AssetIdentityV1, ClobMarketId, ConditionId, MarketIdentityV2, MarketWindowV2,
        OutcomeIdentityV1, TokenId,
    };

    use super::*;
    use crate::platform_rules::tests::successful_snapshot_for_sim_tests;
    use crate::scenario::PlatformScenarioV1;

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str(value).expect("测试十进制")
    }

    fn market(digit: char, start_at_utc_ms: i64, end_at_utc_ms: i64) -> PlatformMarketV1 {
        PlatformMarketV1::try_new(
            MarketIdentityV2 {
                condition_id: ConditionId(format!("0x{}", digit.to_string().repeat(64))),
                clob_market_id: ClobMarketId(format!("10{digit}")),
                window: MarketWindowV2 {
                    start_at_utc_ms,
                    end_at_utc_ms,
                },
            },
            [
                AssetIdentityV1 {
                    token_id: TokenId(format!("20{digit}1")),
                    outcome: OutcomeIdentityV1 {
                        index: 0,
                        label: "Up".to_owned(),
                    },
                },
                AssetIdentityV1 {
                    token_id: TokenId(format!("20{digit}2")),
                    outcome: OutcomeIdentityV1 {
                        index: 1,
                        label: "Down".to_owned(),
                    },
                },
            ],
        )
        .expect("测试市场")
    }

    async fn state(markets: Vec<PlatformMarketV1>) -> RebateStateV1 {
        let scenario = PlatformScenarioV1::try_new(markets).expect("测试场景");
        let directory = SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("市场目录");
        let rules = successful_snapshot_for_sim_tests().await;
        RebateStateV1::new(&directory, &rules)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rebate_state_json_round_trip_preserves_tuple_keyed_day_buckets() {
        let member = market('1', 1_000, 2_000);
        let condition_id = member.market().condition_id.0.clone();
        let mut state = state(vec![member]).await;
        state
            .register_taker_fill("round-trip-trade", &condition_id, 1_500, decimal("2.5"))
            .expect("注册往返测试成交");

        let encoded = serde_json::to_vec(&state).expect("返佣状态 JSON 编码");
        let decoded: RebateStateV1 = serde_json::from_slice(&encoded).expect("返佣状态 JSON 解码");

        assert_eq!(decoded, state);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn maker_contribution_reuses_single_fill_fee_rounding_before_twenty_percent() {
        let member = market('1', 1_000, 2_000);
        let condition_id = member.market().condition_id.0.clone();
        let mut state = state(vec![member]).await;

        state
            .register_maker_fill(
                "maker-trade",
                &condition_id,
                1_500,
                10_000_000,
                decimal("0.5"),
            )
            .expect("注册 maker 成交");
        assert!(
            state
                .on_fill_terminal("maker-trade", true, 1_900)
                .is_empty()
        );

        let actions = state.advance_clock_at(2_000);
        let RebateSettlementActionV1::MakerMarketFinalized(finalized) = &actions[0] else {
            panic!("首个动作必须是 maker 市场最终化")
        };
        assert_eq!(
            finalized.status,
            MakerRebateMarketResultStatusV1::Calculated
        );
        assert_eq!(finalized.accrual_pusd, decimal("0.035"));
        assert_eq!(finalized.contributions.len(), 1);
        assert_eq!(
            finalized.contributions[0].fee_equivalent_pusd,
            decimal("0.175")
        );
        assert_eq!(
            finalized.contributions[0].contribution_pusd,
            decimal("0.035")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn maker_terminal_fill_compacts_but_still_rebuilds_recovery_aggregates() {
        let member = market('1', 1_000, 2_000);
        let condition_id = member.market().condition_id.0.clone();
        let scenario = PlatformScenarioV1::try_new(vec![member]).expect("测试场景");
        let directory = SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("市场目录");
        let rules = successful_snapshot_for_sim_tests().await;
        let mut state = RebateStateV1::new(&directory, &rules);
        state
            .register_maker_fill(
                "compact-maker-trade",
                &condition_id,
                1_500,
                10_000_000,
                decimal("0.5"),
            )
            .expect("注册待压缩 maker 成交");
        assert!(
            state
                .on_fill_terminal("compact-maker-trade", true, 1_900)
                .is_empty()
        );
        assert!(matches!(
            state.advance_clock_at(2_000).as_slice(),
            [RebateSettlementActionV1::MakerMarketFinalized(_)]
        ));
        let market_result = state.market_result(&condition_id);
        let day = state.day_snapshot(RebateKindV1::Maker, 0);
        state
            .evict_terminal_market_fill_state(&condition_id)
            .expect("终态 maker 逐成交返佣热态可压缩");
        assert_eq!(state.retained_fill_state_count(), 0);
        assert_eq!(state.market_result(&condition_id), market_result);
        assert_eq!(state.day_snapshot(RebateKindV1::Maker, 0), day);

        let evicted_fill = RebateRecoveryFillV1 {
            trade_id: "compact-maker-trade".to_owned(),
            condition_id: condition_id.clone(),
            matched_at_utc_ms: 1_500,
            base_units: 10_000_000,
            price_pusd_per_share: decimal("0.5"),
            immediate_taker_fee_units: 0,
            liquidity_role: SimLiquidityRoleV1::MakerInferred,
            terminal_succeeded: Some(true),
        };
        assert!(state.has_consistent_recovery_state(
            &directory,
            &rules,
            &BTreeMap::new(),
            &BTreeMap::from([("compact-maker-trade".to_owned(), evicted_fill.clone())]),
            &BTreeSet::from([condition_id.clone()]),
        ));

        let mut tampered = evicted_fill;
        tampered.price_pusd_per_share = decimal("0.6");
        assert!(!state.has_consistent_recovery_state(
            &directory,
            &rules,
            &BTreeMap::new(),
            &BTreeMap::from([("compact-maker-trade".to_owned(), tampered)]),
            &BTreeSet::from([condition_id]),
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn market_end_retains_pending_finality_and_late_terminal_uses_current_time() {
        let member = market('1', 1_000, 2_000);
        let condition_id = member.market().condition_id.0.clone();
        let mut state = state(vec![member]).await;
        state
            .register_maker_fill(
                "late-maker",
                &condition_id,
                1_500,
                10_000_000,
                decimal("0.5"),
            )
            .expect("注册 maker 成交");

        assert!(state.advance_clock_at(2_000).is_empty());
        assert_eq!(state.market_result(&condition_id), None);
        let actions = state.on_fill_terminal("late-maker", true, 3_000);

        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].platform_effective_at_utc_ms(), 3_000);
        assert!(matches!(
            actions[0],
            RebateSettlementActionV1::MakerMarketFinalized(_)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn separate_days_do_not_pool_below_threshold_and_lock_only_floor_remainder() {
        let first = market('1', 0, 1_000);
        let second = market('2', UTC_DAY_MILLIS, UTC_DAY_MILLIS + 1_000);
        let first_condition = first.market().condition_id.0.clone();
        let second_condition = second.market().condition_id.0.clone();
        let mut state = state(vec![first, second]).await;
        state
            .register_taker_fill("day-zero", &first_condition, 500, decimal("10"))
            .expect("注册首日 taker 成交");
        state
            .register_taker_fill(
                "day-one",
                &second_condition,
                UTC_DAY_MILLIS + 500,
                decimal("15.43210"),
            )
            .expect("注册次日 taker 成交");
        assert!(matches!(
            state.on_fill_terminal("day-zero", true, 600)[0],
            RebateSettlementActionV1::TakerAccrualApplied(_)
        ));
        assert!(matches!(
            state.on_fill_terminal("day-one", true, UTC_DAY_MILLIS + 600)[0],
            RebateSettlementActionV1::TakerAccrualApplied(_)
        ));

        let final_maturity = UTC_DAY_MILLIS * 2 + TAKER_PAYMENT_MINUTE_UTC * MILLIS_PER_MINUTE;
        let mut settlement_actions = Vec::new();
        while let Some(next) = state.next_clock_at_utc_ms() {
            if next > final_maturity {
                break;
            }
            settlement_actions.extend(state.advance_clock_at(next));
        }

        let first_day = state.day_snapshot(RebateKindV1::Taker, 0).expect("首日桶");
        assert_eq!(first_day.accrued_pusd, decimal("0.8"));
        assert_eq!(first_day.payable_pusd, Decimal::ZERO);
        assert_eq!(first_day.locked_remainder_pusd, Decimal::ZERO);
        assert!(!first_day.paid);

        let second_day = state
            .day_snapshot(RebateKindV1::Taker, UTC_DAY_MILLIS)
            .expect("次日桶");
        assert_eq!(second_day.accrued_pusd, decimal("1.234568"));
        assert_eq!(second_day.payable_pusd, decimal("1.2345"));
        assert_eq!(second_day.locked_remainder_pusd, decimal("0.000068"));
        assert!(second_day.paid);
        let paid = settlement_actions
            .iter()
            .filter_map(|action| match action {
                RebateSettlementActionV1::Paid(payment) if payment.kind == RebateKindV1::Taker => {
                    Some(payment.paid_pusd)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(paid, vec![decimal("1.2345")]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn matured_pending_day_closes_once_at_last_predecessor_current_time() {
        let member = market('1', 0, 1_000);
        let condition_id = member.market().condition_id.0.clone();
        let mut state = state(vec![member]).await;
        state
            .register_taker_fill("late-taker", &condition_id, 500, decimal("20"))
            .expect("注册 taker 成交");

        let _market_zero = state.advance_clock_at(1_000);
        let maturity_at = UTC_DAY_MILLIS + TAKER_PAYMENT_MINUTE_UTC * MILLIS_PER_MINUTE;
        assert!(state.advance_clock_at(maturity_at).is_empty());
        let platform_now = maturity_at + 5_000;
        let actions = state.on_fill_terminal("late-taker", true, platform_now);

        assert!(matches!(
            actions.as_slice(),
            [
                RebateSettlementActionV1::TakerAccrualApplied(_),
                RebateSettlementActionV1::DayFinalized(_),
                RebateSettlementActionV1::Paid(_)
            ]
        ));
        assert!(
            actions
                .iter()
                .all(|action| action.platform_effective_at_utc_ms() == platform_now)
        );
        assert!(
            state
                .on_fill_terminal("late-taker", true, platform_now)
                .is_empty()
        );
    }

    #[test]
    fn ready_action_order_is_market_then_maker_day_taker_day_maker_pay_taker_pay() {
        let mut state = RebateStateV1 {
            fee_rate: decimal("0.07"),
            fee_exponent: 1,
            maker_rebate_rate: decimal("0.2"),
            taker_rebate_rate: decimal("0.08"),
            markets: BTreeMap::new(),
            maker_trade_conditions: BTreeMap::new(),
            taker_fills: BTreeMap::new(),
            day_buckets: BTreeMap::from([
                (
                    (RebateKindV1::Maker, 0),
                    RebateDayBucketV1 {
                        maturity_at_utc_ms: 1,
                        maturity_reached: true,
                        pending_predecessors: BTreeSet::new(),
                        potential_accrual_pusd: decimal("1.2"),
                        accrued_pusd: decimal("1.2"),
                        final_state: None,
                    },
                ),
                (
                    (RebateKindV1::Taker, 0),
                    RebateDayBucketV1 {
                        maturity_at_utc_ms: 2,
                        maturity_reached: true,
                        pending_predecessors: BTreeSet::new(),
                        potential_accrual_pusd: decimal("1.3"),
                        accrued_pusd: decimal("1.3"),
                        final_state: None,
                    },
                ),
            ]),
            clocks: BTreeSet::new(),
        };

        let actions = state.drain_affected(
            10,
            BTreeSet::new(),
            BTreeSet::from([0]),
            BTreeSet::from([0]),
        );
        assert!(matches!(
            actions.as_slice(),
            [
                RebateSettlementActionV1::DayFinalized(RebateDayFinalizedActionV1 {
                    kind: RebateKindV1::Maker,
                    ..
                }),
                RebateSettlementActionV1::DayFinalized(RebateDayFinalizedActionV1 {
                    kind: RebateKindV1::Taker,
                    ..
                }),
                RebateSettlementActionV1::Paid(RebatePaidActionV1 {
                    kind: RebateKindV1::Maker,
                    ..
                }),
                RebateSettlementActionV1::Paid(RebatePaidActionV1 {
                    kind: RebateKindV1::Taker,
                    ..
                })
            ]
        ));
    }
}
