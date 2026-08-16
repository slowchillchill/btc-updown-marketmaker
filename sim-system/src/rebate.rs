use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

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
    UnknownTrade(String),
    MarketNotTerminal(String),
    DuplicateTrade(String),
    TerminalIdentityConflict(String),
    NegativeActualTakerFee,
    ArithmeticOverflow,
    TimeOverflow,
    SharedStateRequiresUniqueOwnership,
    FeeMath(MatchingMathErrorV1),
}

impl fmt::Display for RebateStateErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownMarket(condition_id) => {
                write!(formatter, "返佣状态不存在市场：{condition_id}")
            }
            Self::UnknownTrade(trade_id) => write!(formatter, "返佣状态不存在成交：{trade_id}"),
            Self::MarketNotTerminal(condition_id) => {
                write!(formatter, "返佣市场仍有未闭合逐成交前驱：{condition_id}")
            }
            Self::DuplicateTrade(trade_id) => write!(formatter, "返佣成交身份重复：{trade_id}"),
            Self::TerminalIdentityConflict(trade_id) => {
                write!(formatter, "返佣成交终态身份冲突：{trade_id}")
            }
            Self::NegativeActualTakerFee => write!(formatter, "实际 taker 手续费不能为负数"),
            Self::ArithmeticOverflow => write!(formatter, "返佣十进制运算溢出"),
            Self::TimeOverflow => write!(formatter, "返佣 UTC 日结时点溢出"),
            Self::SharedStateRequiresUniqueOwnership => {
                write!(formatter, "非空返佣候选提交前必须取得唯一状态所有权")
            }
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[cfg(test)]
std::thread_local! {
    static REBATE_STATE_CLONE_COUNT_FOR_TESTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static REBATE_DAY_BUCKET_CLONE_COUNT_FOR_TESTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static REBATE_LIVE_CLOCK_ENTRY_VISIT_COUNT_FOR_TESTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static REBATE_INSERTED_CLOCK_ENTRY_VISIT_COUNT_FOR_TESTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static REBATE_PENDING_MEMBERSHIP_ENTRY_VISIT_COUNT_FOR_TESTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    static REBATE_MAKER_STATUS_FILL_ENTRY_VISIT_COUNT_FOR_TESTS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

impl Clone for RebateDayBucketV1 {
    fn clone(&self) -> Self {
        #[cfg(test)]
        REBATE_DAY_BUCKET_CLONE_COUNT_FOR_TESTS.with(|count| count.set(count.get() + 1));
        Self {
            maturity_at_utc_ms: self.maturity_at_utc_ms,
            maturity_reached: self.maturity_reached,
            pending_predecessors: self.pending_predecessors.clone(),
            potential_accrual_pusd: self.potential_accrual_pusd,
            accrued_pusd: self.accrued_pusd,
            final_state: self.final_state.clone(),
        }
    }
}

impl Clone for RebateStateV1 {
    fn clone(&self) -> Self {
        #[cfg(test)]
        REBATE_STATE_CLONE_COUNT_FOR_TESTS.with(|count| count.set(count.get() + 1));
        Self {
            fee_rate: self.fee_rate,
            fee_exponent: self.fee_exponent,
            maker_rebate_rate: self.maker_rebate_rate,
            taker_rebate_rate: self.taker_rebate_rate,
            markets: self.markets.clone(),
            maker_trade_conditions: self.maker_trade_conditions.clone(),
            taker_fills: self.taker_fills.clone(),
            day_buckets: self.day_buckets.clone(),
            clocks: self.clocks.clone(),
        }
    }
}

#[cfg(test)]
fn reset_rebate_state_clone_count_for_tests() {
    REBATE_STATE_CLONE_COUNT_FOR_TESTS.with(|count| count.set(0));
}

#[cfg(test)]
fn rebate_state_clone_count_for_tests() -> usize {
    REBATE_STATE_CLONE_COUNT_FOR_TESTS.with(std::cell::Cell::get)
}

#[cfg(test)]
fn reset_rebate_day_bucket_clone_count_for_tests() {
    REBATE_DAY_BUCKET_CLONE_COUNT_FOR_TESTS.with(|count| count.set(0));
}

#[cfg(test)]
fn rebate_day_bucket_clone_count_for_tests() -> usize {
    REBATE_DAY_BUCKET_CLONE_COUNT_FOR_TESTS.with(std::cell::Cell::get)
}

fn record_rebate_live_clock_entry_visit_for_tests() {
    #[cfg(test)]
    REBATE_LIVE_CLOCK_ENTRY_VISIT_COUNT_FOR_TESTS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
fn reset_rebate_live_clock_entry_visit_count_for_tests() {
    REBATE_LIVE_CLOCK_ENTRY_VISIT_COUNT_FOR_TESTS.with(|count| count.set(0));
}

#[cfg(test)]
fn rebate_live_clock_entry_visit_count_for_tests() -> usize {
    REBATE_LIVE_CLOCK_ENTRY_VISIT_COUNT_FOR_TESTS.with(std::cell::Cell::get)
}

fn record_rebate_inserted_clock_entry_visit_for_tests() {
    #[cfg(test)]
    REBATE_INSERTED_CLOCK_ENTRY_VISIT_COUNT_FOR_TESTS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
fn reset_rebate_inserted_clock_entry_visit_count_for_tests() {
    REBATE_INSERTED_CLOCK_ENTRY_VISIT_COUNT_FOR_TESTS.with(|count| count.set(0));
}

#[cfg(test)]
fn rebate_inserted_clock_entry_visit_count_for_tests() -> usize {
    REBATE_INSERTED_CLOCK_ENTRY_VISIT_COUNT_FOR_TESTS.with(std::cell::Cell::get)
}

fn record_rebate_pending_membership_entry_visit_for_tests() {
    #[cfg(test)]
    REBATE_PENDING_MEMBERSHIP_ENTRY_VISIT_COUNT_FOR_TESTS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
fn reset_rebate_pending_membership_entry_visit_count_for_tests() {
    REBATE_PENDING_MEMBERSHIP_ENTRY_VISIT_COUNT_FOR_TESTS.with(|count| count.set(0));
}

#[cfg(test)]
fn rebate_pending_membership_entry_visit_count_for_tests() -> usize {
    REBATE_PENDING_MEMBERSHIP_ENTRY_VISIT_COUNT_FOR_TESTS.with(std::cell::Cell::get)
}

fn record_rebate_maker_status_fill_entry_visit_for_tests() {
    #[cfg(test)]
    REBATE_MAKER_STATUS_FILL_ENTRY_VISIT_COUNT_FOR_TESTS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
fn reset_rebate_maker_status_fill_entry_visit_count_for_tests() {
    REBATE_MAKER_STATUS_FILL_ENTRY_VISIT_COUNT_FOR_TESTS.with(|count| count.set(0));
}

#[cfg(test)]
fn rebate_maker_status_fill_entry_visit_count_for_tests() -> usize {
    REBATE_MAKER_STATUS_FILL_ENTRY_VISIT_COUNT_FOR_TESTS.with(std::cell::Cell::get)
}

#[derive(Default)]
#[cfg_attr(test, derive(Clone, Debug, PartialEq, Eq))]
pub(crate) struct RebateStatePatchV2 {
    maker_fills: BTreeMap<String, (String, MakerFillStateV1)>,
    maker_day_buckets: BTreeMap<i64, PreparedTakerRebateDayBucketPatchV2>,
    taker_fills: BTreeMap<String, TakerFillStateV1>,
    day_buckets: BTreeMap<i64, PreparedTakerRebateDayBucketPatchV2>,
    maker_terminal_after: BTreeMap<String, bool>,
    taker_terminal_after: BTreeMap<String, bool>,
    maker_market_traversal_progress: BTreeMap<String, MakerMarketTraversalProgressV2>,
    last_consumed_live_clock: Option<(i64, RebateClockIdentityV1)>,
    clock_insertions: BTreeSet<(i64, RebateClockIdentityV1)>,
    clock_removals: BTreeSet<(i64, RebateClockIdentityV1)>,
    market_end_reached: BTreeSet<String>,
    market_results: BTreeMap<String, MakerRebateMarketResultStatusV1>,
    day_bucket_updates: BTreeMap<(RebateKindV1, i64), RebateDayBucketUpdateV2>,
    settlement_actions: Vec<RebateSettlementActionV1>,
}

#[derive(Default)]
#[cfg_attr(test, derive(Clone, Debug, PartialEq, Eq))]
struct RebateDayBucketUpdateV2 {
    maturity_reached: bool,
    pending_predecessors_to_remove: BTreeSet<String>,
    accrued_pusd_after: Option<Decimal>,
    final_state_after: Option<RebateDayFinalStateV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MakerMarketTraversalProgressV2 {
    remaining_unterminal: usize,
    has_success: bool,
}

#[cfg_attr(test, derive(Clone, Debug, PartialEq, Eq))]
struct PreparedTakerRebateDayBucketPatchV2 {
    maturity_at_utc_ms: i64,
    potential_accrual_pusd: Decimal,
    pending_predecessors_to_insert: BTreeSet<String>,
    creates_day_bucket: bool,
}

struct PreparedTakerFillRegistrationV2 {
    trade_id: String,
    fill: TakerFillStateV1,
    maturity_at_utc_ms: i64,
    potential_accrual_pusd: Decimal,
    creates_day_bucket: bool,
}

pub(crate) struct PreparedTakerRebateRegistrationsV2<'a> {
    state: &'a mut RebateStateV1,
    patch: RebateStatePatchV2,
    preparation_failed: Option<RebateStateErrorV1>,
}

pub(crate) struct CommittableTakerRebateRegistrationsV2<'a> {
    state: &'a mut RebateStateV1,
    patch: RebateStatePatchV2,
}

pub(crate) struct PreparedRebateStateDeltaV2<'a> {
    live: &'a mut Arc<RebateStateV1>,
    patch: RebateStatePatchV2,
}

pub(crate) struct CommittableRebateStateDeltaV2<'a> {
    state: Option<&'a mut RebateStateV1>,
    patch: RebateStatePatchV2,
}

impl RebateStatePatchV2 {
    pub(crate) fn stage_maker_fill(
        &mut self,
        state: &RebateStateV1,
        trade_id: &str,
        condition_id: &str,
        matched_at_utc_ms: i64,
        base_units: u128,
        price_pusd_per_share: Decimal,
    ) -> Result<(), RebateStateErrorV1> {
        if !state.markets.contains_key(condition_id) {
            return Err(RebateStateErrorV1::UnknownMarket(condition_id.to_owned()));
        }
        state.ensure_new_trade(trade_id)?;
        if self.maker_fills.contains_key(trade_id) || self.taker_fills.contains_key(trade_id) {
            return Err(RebateStateErrorV1::DuplicateTrade(trade_id.to_owned()));
        }

        let attribution_day_utc_ms = utc_day_start(matched_at_utc_ms)?;
        let fee_equivalent_pusd = taker_fee_pusd_v1(
            &[(base_units, price_pusd_per_share)],
            state.fee_rate,
            state.fee_exponent,
        )?;
        let contribution_pusd = fee_equivalent_pusd
            .checked_mul(state.maker_rebate_rate)
            .ok_or(RebateStateErrorV1::ArithmeticOverflow)?
            .normalize();
        let maturity_at_utc_ms = rebate_maturity_at(RebateKindV1::Maker, attribution_day_utc_ms)?;
        let potential_accrual_pusd = self
            .maker_day_buckets
            .get(&attribution_day_utc_ms)
            .map(|day| day.potential_accrual_pusd)
            .or_else(|| {
                state
                    .day_buckets
                    .get(&(RebateKindV1::Maker, attribution_day_utc_ms))
                    .map(|day| day.potential_accrual_pusd)
            })
            .map_or(Ok(contribution_pusd), |potential| {
                potential
                    .checked_add(contribution_pusd)
                    .ok_or(RebateStateErrorV1::ArithmeticOverflow)
            })?
            .normalize();
        let creates_day_bucket = !state
            .day_buckets
            .contains_key(&(RebateKindV1::Maker, attribution_day_utc_ms));
        let day = self
            .maker_day_buckets
            .entry(attribution_day_utc_ms)
            .or_insert_with(|| PreparedTakerRebateDayBucketPatchV2 {
                maturity_at_utc_ms,
                potential_accrual_pusd: Decimal::ZERO,
                pending_predecessors_to_insert: BTreeSet::new(),
                creates_day_bucket,
            });
        if creates_day_bucket {
            self.clock_insertions.insert((
                maturity_at_utc_ms,
                RebateClockIdentityV1::Day(RebateKindV1::Maker, attribution_day_utc_ms),
            ));
        }
        day.potential_accrual_pusd = potential_accrual_pusd;
        day.pending_predecessors_to_insert
            .insert(trade_id.to_owned());
        self.maker_fills.insert(
            trade_id.to_owned(),
            (
                condition_id.to_owned(),
                MakerFillStateV1 {
                    contribution: MakerRebateContributionV1 {
                        trade_id: trade_id.to_owned(),
                        matched_at_utc_ms,
                        attribution_day_utc_ms,
                        base_units,
                        price_pusd_per_share,
                        fee_equivalent_pusd,
                        rebate_rate: state.maker_rebate_rate,
                        contribution_pusd,
                    },
                    terminal_succeeded: None,
                },
            ),
        );
        Ok(())
    }

    pub(crate) fn stage_taker_fill(
        &mut self,
        state: &RebateStateV1,
        trade_id: &str,
        condition_id: &str,
        matched_at_utc_ms: i64,
        actual_taker_fee_pusd: Decimal,
    ) -> Result<bool, RebateStateErrorV1> {
        let Some(prepared) = state.prepare_taker_fill_registration_v2(
            self,
            trade_id,
            condition_id,
            matched_at_utc_ms,
            actual_taker_fee_pusd,
        )?
        else {
            return Ok(false);
        };
        let PreparedTakerFillRegistrationV2 {
            trade_id,
            fill,
            maturity_at_utc_ms,
            potential_accrual_pusd,
            creates_day_bucket,
        } = prepared;
        let attribution_day_utc_ms = fill.attribution_day_utc_ms;
        let day = self
            .day_buckets
            .entry(attribution_day_utc_ms)
            .or_insert_with(|| PreparedTakerRebateDayBucketPatchV2 {
                maturity_at_utc_ms,
                potential_accrual_pusd: Decimal::ZERO,
                pending_predecessors_to_insert: BTreeSet::new(),
                creates_day_bucket,
            });
        if creates_day_bucket {
            self.clock_insertions.insert((
                maturity_at_utc_ms,
                RebateClockIdentityV1::Day(RebateKindV1::Taker, attribution_day_utc_ms),
            ));
        }
        day.potential_accrual_pusd = potential_accrual_pusd;
        day.pending_predecessors_to_insert.insert(trade_id.clone());
        self.taker_fills.insert(trade_id, fill);
        Ok(true)
    }

    pub(crate) fn candidate_next_clock(&self, state: &RebateStateV1) -> Option<i64> {
        let live_next = self
            .unconsumed_live_clocks(state)
            .find(|clock| {
                record_rebate_live_clock_entry_visit_for_tests();
                !self.clock_removals.contains(*clock)
            })
            .map(|(at, _)| *at);
        let inserted_next = self.clock_insertions.first().map(|(at, _)| *at);
        match (live_next, inserted_next) {
            (Some(live), Some(inserted)) => Some(live.min(inserted)),
            (Some(live), None) => Some(live),
            (None, Some(inserted)) => Some(inserted),
            (None, None) => None,
        }
    }

    fn unconsumed_live_clocks<'a>(
        &'a self,
        state: &'a RebateStateV1,
    ) -> impl Iterator<Item = &'a (i64, RebateClockIdentityV1)> {
        let lower_bound = self
            .last_consumed_live_clock
            .as_ref()
            .map_or(Unbounded, Excluded);
        state.clocks.range((lower_bound, Unbounded))
    }

    pub(crate) fn candidate_market_result(
        &self,
        state: &RebateStateV1,
        condition_id: &str,
    ) -> Option<MakerRebateMarketResultStatusV1> {
        self.market_results
            .get(condition_id)
            .copied()
            .or_else(|| state.market_result(condition_id))
    }

    pub(crate) fn stage_clock_at<'a>(
        &'a mut self,
        state: &RebateStateV1,
        exact_next_at_utc_ms: i64,
    ) -> &'a [RebateSettlementActionV1] {
        let first_new_action = self.settlement_actions.len();
        if self.candidate_next_clock(state) != Some(exact_next_at_utc_ms) {
            return &self.settlement_actions[first_new_action..];
        }

        let live_keys = self
            .unconsumed_live_clocks(state)
            .inspect(|_| record_rebate_live_clock_entry_visit_for_tests())
            .take_while(|clock| clock.0 == exact_next_at_utc_ms)
            .filter(|clock| !self.clock_removals.contains(*clock))
            .cloned()
            .collect::<Vec<_>>();
        if let Some(last_live_key) = live_keys.last() {
            self.last_consumed_live_clock = Some(last_live_key.clone());
        }
        let mut keys = live_keys.into_iter().collect::<BTreeSet<_>>();
        let inserted_lower_bound = Included((
            exact_next_at_utc_ms,
            RebateClockIdentityV1::MarketEnd(String::new()),
        ));
        let inserted_upper_bound = exact_next_at_utc_ms
            .checked_add(1)
            .map_or(Unbounded, |next| {
                Excluded((next, RebateClockIdentityV1::MarketEnd(String::new())))
            });
        keys.extend(
            self.clock_insertions
                .range((inserted_lower_bound, inserted_upper_bound))
                .inspect(|_| record_rebate_inserted_clock_entry_visit_for_tests())
                .cloned(),
        );

        let mut affected_markets = BTreeSet::new();
        let mut affected_maker_days = BTreeSet::new();
        let mut affected_taker_days = BTreeSet::new();
        for key in keys {
            if !self.clock_insertions.remove(&key) {
                self.clock_removals.insert(key.clone());
            }
            match key.1 {
                RebateClockIdentityV1::MarketEnd(condition_id) => {
                    self.market_end_reached.insert(condition_id.clone());
                    affected_markets.insert(condition_id);
                }
                RebateClockIdentityV1::Day(kind, day) => {
                    let key = (kind, day);
                    assert!(
                        self.candidate_day_exists(state, key),
                        "日结时钟候选只指向已登记日桶"
                    );
                    self.day_bucket_updates
                        .entry(key)
                        .or_default()
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
        self.stage_drain_affected(
            state,
            exact_next_at_utc_ms,
            affected_markets,
            affected_maker_days,
            affected_taker_days,
        );
        &self.settlement_actions[first_new_action..]
    }

    fn candidate_market_end_reached(&self, state: &RebateStateV1, condition_id: &str) -> bool {
        self.market_end_reached.contains(condition_id)
            || state
                .markets
                .get(condition_id)
                .is_some_and(|market| market.end_reached)
    }

    fn candidate_day_exists(&self, state: &RebateStateV1, key: (RebateKindV1, i64)) -> bool {
        state.day_buckets.contains_key(&key)
            || (key.0 == RebateKindV1::Taker
                && self
                    .day_buckets
                    .get(&key.1)
                    .is_some_and(|patch| patch.creates_day_bucket))
    }

    fn candidate_day_maturity_reached(
        &self,
        state: &RebateStateV1,
        key: (RebateKindV1, i64),
    ) -> bool {
        self.day_bucket_updates
            .get(&key)
            .is_some_and(|update| update.maturity_reached)
            || state
                .day_buckets
                .get(&key)
                .is_some_and(|bucket| bucket.maturity_reached)
    }

    fn candidate_day_pending_count(
        &self,
        state: &RebateStateV1,
        key: (RebateKindV1, i64),
    ) -> usize {
        let live_bucket = state.day_buckets.get(&key);
        let staged_removal_count = self
            .day_bucket_updates
            .get(&key)
            .map_or(0, |update| update.pending_predecessors_to_remove.len());
        let staged_insert_count = if key.0 == RebateKindV1::Taker {
            self.day_buckets
                .get(&key.1)
                .map_or(0, |day| day.pending_predecessors_to_insert.len())
        } else {
            0
        };
        let live_count = live_bucket.map_or(0, |bucket| bucket.pending_predecessors.len());
        live_count
            .checked_add(staged_insert_count)
            .and_then(|total| total.checked_sub(staged_removal_count))
            .expect("返佣 pending 稀疏计数只移除真实 live/staged 前驱且受资源上限约束")
    }

    fn candidate_day_contains_pending_predecessor_for_debug(
        &self,
        state: &RebateStateV1,
        key: (RebateKindV1, i64),
        trade_id: &str,
    ) -> bool {
        if let Some(bucket) = state.day_buckets.get(&key) {
            record_rebate_pending_membership_entry_visit_for_tests();
            if bucket.pending_predecessors.contains(trade_id) {
                return true;
            }
        }
        if key.0 == RebateKindV1::Taker {
            if let Some(day) = self.day_buckets.get(&key.1) {
                record_rebate_pending_membership_entry_visit_for_tests();
                if day.pending_predecessors_to_insert.contains(trade_id) {
                    return true;
                }
            }
        }
        false
    }

    fn candidate_day_accrued_pusd(
        &self,
        state: &RebateStateV1,
        key: (RebateKindV1, i64),
    ) -> Decimal {
        self.day_bucket_updates
            .get(&key)
            .and_then(|update| update.accrued_pusd_after)
            .or_else(|| {
                state
                    .day_buckets
                    .get(&key)
                    .map(|bucket| bucket.accrued_pusd)
            })
            .unwrap_or(Decimal::ZERO)
    }

    fn candidate_day_has_final_state(
        &self,
        state: &RebateStateV1,
        key: (RebateKindV1, i64),
    ) -> bool {
        self.day_bucket_updates
            .get(&key)
            .is_some_and(|update| update.final_state_after.is_some())
            || state
                .day_buckets
                .get(&key)
                .is_some_and(|bucket| bucket.final_state.is_some())
    }

    fn candidate_day_is_ready(&self, state: &RebateStateV1, key: (RebateKindV1, i64)) -> bool {
        self.candidate_day_exists(state, key)
            && self.candidate_day_maturity_reached(state, key)
            && self.candidate_day_pending_count(state, key) == 0
            && !self.candidate_day_has_final_state(state, key)
    }

    fn candidate_maker_terminal(&self, trade_id: &str, fill: &MakerFillStateV1) -> Option<bool> {
        self.maker_terminal_after
            .get(trade_id)
            .copied()
            .or(fill.terminal_succeeded)
    }

    fn candidate_taker_terminal(&self, trade_id: &str, fill: &TakerFillStateV1) -> Option<bool> {
        self.taker_terminal_after
            .get(trade_id)
            .copied()
            .or(fill.terminal_succeeded)
    }

    fn candidate_maker_market_status(
        &mut self,
        state: &RebateStateV1,
        condition_id: &str,
    ) -> Option<MakerRebateMarketResultStatusV1> {
        if !self.candidate_market_end_reached(state, condition_id) {
            return None;
        }
        if !self
            .maker_market_traversal_progress
            .contains_key(condition_id)
        {
            let market = state.markets.get(condition_id)?;
            let mut remaining_unterminal = 0usize;
            let mut has_success = false;
            for (trade_id, fill) in &market.fills {
                record_rebate_maker_status_fill_entry_visit_for_tests();
                match self.candidate_maker_terminal(trade_id, fill) {
                    Some(succeeded) => has_success |= succeeded,
                    None => {
                        remaining_unterminal = remaining_unterminal
                            .checked_add(1)
                            .expect("maker 成交数受资源上限约束");
                    }
                }
            }
            self.maker_market_traversal_progress.insert(
                condition_id.to_owned(),
                MakerMarketTraversalProgressV2 {
                    remaining_unterminal,
                    has_success,
                },
            );
        }
        let progress = self
            .maker_market_traversal_progress
            .get(condition_id)
            .expect("maker traversal progress 已按当前市场初始化");
        if progress.remaining_unterminal != 0 {
            return None;
        }
        Some(if progress.has_success {
            MakerRebateMarketResultStatusV1::Calculated
        } else {
            MakerRebateMarketResultStatusV1::ZeroNoEligibleMakerFill
        })
    }

    pub(crate) fn stage_registered_fill_terminal<'a>(
        &'a mut self,
        state: &RebateStateV1,
        trade_id: &str,
        succeeded: bool,
        platform_effective_at_utc_ms: i64,
    ) -> Result<&'a [RebateSettlementActionV1], RebateStateErrorV1> {
        let first_new_action = self.settlement_actions.len();
        let existing_terminal =
            if let Some(condition_id) = state.maker_trade_conditions.get(trade_id) {
                let fill = state
                    .markets
                    .get(condition_id)
                    .and_then(|market| market.fills.get(trade_id))
                    .expect("maker 成交索引只指向已注册成交");
                self.candidate_maker_terminal(trade_id, fill)
            } else if let Some(fill) = self
                .taker_fills
                .get(trade_id)
                .or_else(|| state.taker_fills.get(trade_id))
            {
                self.candidate_taker_terminal(trade_id, fill)
            } else {
                return Err(RebateStateErrorV1::UnknownTrade(trade_id.to_owned()));
            };

        match existing_terminal {
            Some(existing) if existing == succeeded => {
                Ok(&self.settlement_actions[first_new_action..])
            }
            Some(_) => Err(RebateStateErrorV1::TerminalIdentityConflict(
                trade_id.to_owned(),
            )),
            None => Ok(self.stage_fill_terminal(
                state,
                trade_id,
                succeeded,
                platform_effective_at_utc_ms,
            )),
        }
    }

    pub(crate) fn stage_fill_terminal<'a>(
        &'a mut self,
        state: &RebateStateV1,
        trade_id: &str,
        succeeded: bool,
        platform_effective_at_utc_ms: i64,
    ) -> &'a [RebateSettlementActionV1] {
        let first_new_action = self.settlement_actions.len();
        if let Some(condition_id) = state.maker_trade_conditions.get(trade_id).cloned() {
            let fill = state
                .markets
                .get(&condition_id)
                .and_then(|market| market.fills.get(trade_id))
                .expect("maker 成交索引只指向已注册成交");
            if self.candidate_maker_terminal(trade_id, fill).is_some() {
                return &self.settlement_actions[first_new_action..];
            }
            self.maker_terminal_after
                .insert(trade_id.to_owned(), succeeded);
            if let Some(progress) = self.maker_market_traversal_progress.get_mut(&condition_id) {
                progress.remaining_unterminal = progress
                    .remaining_unterminal
                    .checked_sub(1)
                    .expect("maker traversal progress 只消费尚未终态成交");
                progress.has_success |= succeeded;
            }
            self.stage_drain_affected(
                state,
                platform_effective_at_utc_ms,
                BTreeSet::from([condition_id]),
                BTreeSet::new(),
                BTreeSet::new(),
            );
            return &self.settlement_actions[first_new_action..];
        }

        let Some(fill) = self
            .taker_fills
            .get(trade_id)
            .or_else(|| state.taker_fills.get(trade_id))
            .cloned()
        else {
            return &self.settlement_actions[first_new_action..];
        };
        if self.candidate_taker_terminal(trade_id, &fill).is_some() {
            return &self.settlement_actions[first_new_action..];
        }

        self.taker_terminal_after
            .insert(trade_id.to_owned(), succeeded);
        let key = (RebateKindV1::Taker, fill.attribution_day_utc_ms);
        debug_assert!(
            self.candidate_day_contains_pending_predecessor_for_debug(state, key, trade_id,)
        );
        self.day_bucket_updates
            .entry(key)
            .or_default()
            .pending_predecessors_to_remove
            .insert(trade_id.to_owned());
        if succeeded {
            let accrued_pusd_after = self
                .candidate_day_accrued_pusd(state, key)
                .checked_add(fill.accrual_pusd)
                .expect("已预检的潜在 taker 日应计总额保证成功子集可表示")
                .normalize();
            self.day_bucket_updates
                .entry(key)
                .or_default()
                .accrued_pusd_after = Some(accrued_pusd_after);
            self.settlement_actions
                .push(RebateSettlementActionV1::TakerAccrualApplied(
                    TakerRebateAccrualAppliedActionV1 {
                        reason_identity: taker_accrual_reason(trade_id),
                        trade_id: trade_id.to_owned(),
                        condition_id: fill.condition_id,
                        matched_at_utc_ms: fill.matched_at_utc_ms,
                        attribution_day_utc_ms: fill.attribution_day_utc_ms,
                        platform_effective_at_utc_ms,
                        actual_taker_fee_pusd: fill.actual_taker_fee_pusd,
                        rebate_rate: fill.rebate_rate,
                        accrual_pusd: fill.accrual_pusd,
                    },
                ));
        }
        self.stage_drain_affected(
            state,
            platform_effective_at_utc_ms,
            BTreeSet::new(),
            BTreeSet::new(),
            BTreeSet::from([key.1]),
        );
        &self.settlement_actions[first_new_action..]
    }

    fn stage_drain_affected(
        &mut self,
        state: &RebateStateV1,
        platform_effective_at_utc_ms: i64,
        affected_markets: BTreeSet<String>,
        mut affected_maker_days: BTreeSet<i64>,
        affected_taker_days: BTreeSet<i64>,
    ) {
        for condition_id in affected_markets {
            let ready = self.candidate_market_result(state, &condition_id).is_none()
                && self
                    .candidate_maker_market_status(state, &condition_id)
                    .is_some();
            if ready {
                let (action, days) = self.stage_finalize_maker_market(
                    state,
                    &condition_id,
                    platform_effective_at_utc_ms,
                );
                affected_maker_days.extend(days);
                self.settlement_actions
                    .push(RebateSettlementActionV1::MakerMarketFinalized(action));
            }
        }

        let mut finalized_days = Vec::new();
        for (kind, days) in [
            (RebateKindV1::Maker, affected_maker_days),
            (RebateKindV1::Taker, affected_taker_days),
        ] {
            for day in days {
                let key = (kind, day);
                if self.candidate_day_is_ready(state, key) {
                    let action =
                        self.stage_finalize_day(state, kind, day, platform_effective_at_utc_ms);
                    self.settlement_actions
                        .push(RebateSettlementActionV1::DayFinalized(action));
                    finalized_days.push((kind, day));
                }
            }
        }

        for kind in [RebateKindV1::Maker, RebateKindV1::Taker] {
            for &(_, day) in finalized_days
                .iter()
                .filter(|(finalized_kind, _)| *finalized_kind == kind)
            {
                let final_state = self
                    .day_bucket_updates
                    .get_mut(&(kind, day))
                    .expect("刚完成的日结候选必须仍存在")
                    .final_state_after
                    .as_mut()
                    .expect("刚完成的日结候选必须有终态");
                if final_state.payable_pusd > Decimal::ZERO {
                    final_state.paid = true;
                    self.settlement_actions.push(RebateSettlementActionV1::Paid(
                        RebatePaidActionV1 {
                            reason_identity: payment_reason(kind, day),
                            kind,
                            attribution_day_utc_ms: day,
                            platform_effective_at_utc_ms,
                            paid_pusd: final_state.payable_pusd,
                        },
                    ));
                }
            }
        }
    }

    fn stage_finalize_maker_market(
        &mut self,
        state: &RebateStateV1,
        condition_id: &str,
        platform_effective_at_utc_ms: i64,
    ) -> (MakerRebateMarketFinalizedActionV1, BTreeSet<i64>) {
        let market = state
            .markets
            .get(condition_id)
            .expect("待最终化市场候选来自当前状态索引");
        let status = self
            .candidate_maker_market_status(state, condition_id)
            .expect("待最终化市场候选必须已经闭合全部 maker 成交前驱");
        let contributions = market
            .fills
            .iter()
            .filter(|(trade_id, fill)| self.candidate_maker_terminal(trade_id, fill) == Some(true))
            .map(|(_, fill)| fill.contribution.clone())
            .collect::<Vec<_>>();
        let affected_days = market
            .fills
            .values()
            .map(|fill| fill.contribution.attribution_day_utc_ms)
            .collect::<BTreeSet<_>>();
        let accrual_pusd = contributions
            .iter()
            .fold(Decimal::ZERO, |total, contribution| {
                total
                    .checked_add(contribution.contribution_pusd)
                    .expect("已预检的潜在日应计总额保证成功子集可表示")
            })
            .normalize();

        for (trade_id, fill) in &market.fills {
            let contribution = &fill.contribution;
            let key = (RebateKindV1::Maker, contribution.attribution_day_utc_ms);
            debug_assert!(
                self.candidate_day_contains_pending_predecessor_for_debug(state, key, trade_id,)
            );
            self.day_bucket_updates
                .entry(key)
                .or_default()
                .pending_predecessors_to_remove
                .insert(contribution.trade_id.clone());
            if self.candidate_maker_terminal(trade_id, fill) == Some(true) {
                let accrued_pusd_after = self
                    .candidate_day_accrued_pusd(state, key)
                    .checked_add(contribution.contribution_pusd)
                    .expect("已预检的潜在日应计总额保证成功子集可表示")
                    .normalize();
                self.day_bucket_updates
                    .entry(key)
                    .or_default()
                    .accrued_pusd_after = Some(accrued_pusd_after);
            }
        }
        self.market_results.insert(condition_id.to_owned(), status);

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

    fn stage_finalize_day(
        &mut self,
        state: &RebateStateV1,
        kind: RebateKindV1,
        attribution_day_utc_ms: i64,
        platform_effective_at_utc_ms: i64,
    ) -> RebateDayFinalizedActionV1 {
        let key = (kind, attribution_day_utc_ms);
        let accrued_pusd = self.candidate_day_accrued_pusd(state, key).normalize();
        let mut final_state = finalized_day_state(accrued_pusd);
        final_state.paid = false;
        let payable_pusd = final_state.payable_pusd;
        let locked_remainder_pusd = final_state.locked_remainder_pusd;
        self.day_bucket_updates
            .entry(key)
            .or_default()
            .final_state_after = Some(final_state);
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

    pub(crate) fn has_state_change(&self) -> bool {
        !self.maker_fills.is_empty()
            || !self.maker_day_buckets.is_empty()
            || !self.taker_fills.is_empty()
            || !self.day_buckets.is_empty()
            || !self.maker_terminal_after.is_empty()
            || !self.taker_terminal_after.is_empty()
            || !self.clock_insertions.is_empty()
            || !self.clock_removals.is_empty()
            || !self.market_end_reached.is_empty()
            || !self.market_results.is_empty()
            || !self.day_bucket_updates.is_empty()
    }

    fn take_settlement_actions(&mut self) -> Vec<RebateSettlementActionV1> {
        std::mem::take(&mut self.settlement_actions)
    }

    pub(crate) fn commit_prevalidated(self, state: &mut RebateStateV1) {
        for (attribution_day_utc_ms, day_patch) in self.maker_day_buckets {
            let key = (RebateKindV1::Maker, attribution_day_utc_ms);
            if day_patch.creates_day_bucket {
                state.day_buckets.insert(
                    key,
                    RebateDayBucketV1 {
                        maturity_at_utc_ms: day_patch.maturity_at_utc_ms,
                        maturity_reached: false,
                        pending_predecessors: day_patch.pending_predecessors_to_insert,
                        potential_accrual_pusd: day_patch.potential_accrual_pusd,
                        accrued_pusd: Decimal::ZERO,
                        final_state: None,
                    },
                );
            } else {
                let bucket = state
                    .day_buckets
                    .get_mut(&key)
                    .expect("独占准备期间既有 maker 返佣日桶必须仍然存在");
                bucket.potential_accrual_pusd = day_patch.potential_accrual_pusd;
                bucket
                    .pending_predecessors
                    .extend(day_patch.pending_predecessors_to_insert);
            }
        }
        for (attribution_day_utc_ms, day_patch) in self.day_buckets {
            let key = (RebateKindV1::Taker, attribution_day_utc_ms);
            if day_patch.creates_day_bucket {
                state.day_buckets.insert(
                    key,
                    RebateDayBucketV1 {
                        maturity_at_utc_ms: day_patch.maturity_at_utc_ms,
                        maturity_reached: false,
                        pending_predecessors: day_patch.pending_predecessors_to_insert,
                        potential_accrual_pusd: day_patch.potential_accrual_pusd,
                        accrued_pusd: Decimal::ZERO,
                        final_state: None,
                    },
                );
            } else {
                let bucket = state
                    .day_buckets
                    .get_mut(&key)
                    .expect("独占准备期间既有 taker 返佣日桶必须仍然存在");
                bucket.potential_accrual_pusd = day_patch.potential_accrual_pusd;
                bucket
                    .pending_predecessors
                    .extend(day_patch.pending_predecessors_to_insert);
            }
        }
        for (trade_id, fill) in self.taker_fills {
            state.taker_fills.insert(trade_id, fill);
        }
        for (trade_id, (condition_id, fill)) in self.maker_fills {
            state
                .markets
                .get_mut(&condition_id)
                .expect("独占准备期间 maker 成交市场必须仍然存在")
                .fills
                .insert(trade_id.clone(), fill);
            state.maker_trade_conditions.insert(trade_id, condition_id);
        }
        for (trade_id, succeeded) in self.maker_terminal_after {
            let condition_id = state
                .maker_trade_conditions
                .get(&trade_id)
                .expect("maker terminal 候选只指向已注册成交")
                .clone();
            state
                .markets
                .get_mut(&condition_id)
                .and_then(|market| market.fills.get_mut(&trade_id))
                .expect("maker terminal 候选只指向已注册成交")
                .terminal_succeeded = Some(succeeded);
        }
        for (trade_id, succeeded) in self.taker_terminal_after {
            state
                .taker_fills
                .get_mut(&trade_id)
                .expect("taker terminal 候选只指向已注册成交")
                .terminal_succeeded = Some(succeeded);
        }
        for condition_id in self.market_end_reached {
            state
                .markets
                .get_mut(&condition_id)
                .expect("市场结束候选只指向已登记市场")
                .end_reached = true;
        }
        for (condition_id, result) in self.market_results {
            state
                .markets
                .get_mut(&condition_id)
                .expect("市场返佣结果候选只指向已登记市场")
                .result = Some(result);
        }
        for (key, update) in self.day_bucket_updates {
            let bucket = state
                .day_buckets
                .get_mut(&key)
                .expect("返佣日桶稀疏更新只指向已登记或本次新增日桶");
            if update.maturity_reached {
                bucket.maturity_reached = true;
            }
            for trade_id in update.pending_predecessors_to_remove {
                bucket.pending_predecessors.remove(&trade_id);
            }
            if let Some(accrued_pusd_after) = update.accrued_pusd_after {
                bucket.accrued_pusd = accrued_pusd_after;
            }
            if let Some(final_state_after) = update.final_state_after {
                bucket.final_state = Some(final_state_after);
            }
        }
        for clock in self.clock_removals {
            state.clocks.remove(&clock);
        }
        state.clocks.extend(self.clock_insertions);
    }
}

impl<'a> PreparedRebateStateDeltaV2<'a> {
    pub(crate) fn prepare(live: &'a mut Arc<RebateStateV1>) -> Self {
        Self {
            live,
            patch: RebateStatePatchV2::default(),
        }
    }

    pub(crate) fn candidate_next_clock(&self) -> Option<i64> {
        self.patch.candidate_next_clock(self.live.as_ref())
    }

    pub(crate) fn candidate_market_result(
        &self,
        condition_id: &str,
    ) -> Option<MakerRebateMarketResultStatusV1> {
        self.patch
            .candidate_market_result(self.live.as_ref(), condition_id)
    }

    pub(crate) fn stage_clock_at(
        &mut self,
        exact_next_at_utc_ms: i64,
    ) -> &[RebateSettlementActionV1] {
        self.patch
            .stage_clock_at(self.live.as_ref(), exact_next_at_utc_ms)
    }

    pub(crate) fn stage_fill_terminal(
        &mut self,
        trade_id: &str,
        succeeded: bool,
        platform_effective_at_utc_ms: i64,
    ) -> &[RebateSettlementActionV1] {
        self.patch.stage_fill_terminal(
            self.live.as_ref(),
            trade_id,
            succeeded,
            platform_effective_at_utc_ms,
        )
    }

    pub(crate) fn has_state_change(&self) -> bool {
        self.patch.has_state_change()
    }

    pub(crate) fn settlement_actions(&self) -> &[RebateSettlementActionV1] {
        &self.patch.settlement_actions
    }

    pub(crate) fn into_committable(
        self,
    ) -> Result<CommittableRebateStateDeltaV2<'a>, RebateStateErrorV1> {
        let Self { live, patch } = self;
        let state = if patch.has_state_change() {
            Some(Arc::get_mut(live).ok_or(RebateStateErrorV1::SharedStateRequiresUniqueOwnership)?)
        } else {
            None
        };
        Ok(CommittableRebateStateDeltaV2 { state, patch })
    }
}

impl CommittableRebateStateDeltaV2<'_> {
    pub(crate) fn commit(self) {
        if let Some(state) = self.state {
            self.patch.commit_prevalidated(state);
        }
    }
}

impl<'a> PreparedTakerRebateRegistrationsV2<'a> {
    fn failed_error(&self) -> Option<RebateStateErrorV1> {
        self.preparation_failed.clone()
    }

    pub(crate) fn stage_taker_fill(
        &mut self,
        trade_id: &str,
        condition_id: &str,
        matched_at_utc_ms: i64,
        actual_taker_fee_pusd: Decimal,
    ) -> Result<bool, RebateStateErrorV1> {
        if let Some(error) = self.failed_error() {
            return Err(error);
        }
        let result = self.patch.stage_taker_fill(
            self.state,
            trade_id,
            condition_id,
            matched_at_utc_ms,
            actual_taker_fee_pusd,
        );
        if let Err(error) = &result {
            self.preparation_failed = Some(error.clone());
        }
        result
    }

    pub(crate) fn into_committable(
        self,
    ) -> Result<CommittableTakerRebateRegistrationsV2<'a>, RebateStateErrorV1> {
        if let Some(error) = self.preparation_failed {
            return Err(error);
        }
        Ok(CommittableTakerRebateRegistrationsV2 {
            state: self.state,
            patch: self.patch,
        })
    }
}

impl CommittableTakerRebateRegistrationsV2<'_> {
    pub(crate) fn commit(self) {
        self.patch.commit_prevalidated(self.state);
    }
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
        let mut patch = RebateStatePatchV2::default();
        patch.stage_maker_fill(
            self,
            trade_id,
            condition_id,
            matched_at_utc_ms,
            base_units,
            price_pusd_per_share,
        )?;
        patch.commit_prevalidated(self);
        Ok(())
    }
    pub(crate) fn register_taker_fill(
        &mut self,
        trade_id: &str,
        condition_id: &str,
        matched_at_utc_ms: i64,
        actual_taker_fee_pusd: Decimal,
    ) -> Result<bool, RebateStateErrorV1> {
        let mut patch = RebateStatePatchV2::default();
        let registered = patch.stage_taker_fill(
            self,
            trade_id,
            condition_id,
            matched_at_utc_ms,
            actual_taker_fee_pusd,
        )?;
        patch.commit_prevalidated(self);
        Ok(registered)
    }

    pub(crate) fn prepare_taker_rebate_registrations_v2(
        &mut self,
    ) -> PreparedTakerRebateRegistrationsV2<'_> {
        PreparedTakerRebateRegistrationsV2 {
            state: self,
            patch: RebateStatePatchV2::default(),
            preparation_failed: None,
        }
    }

    fn prepare_taker_fill_registration_v2(
        &self,
        patch: &RebateStatePatchV2,
        trade_id: &str,
        condition_id: &str,
        matched_at_utc_ms: i64,
        actual_taker_fee_pusd: Decimal,
    ) -> Result<Option<PreparedTakerFillRegistrationV2>, RebateStateErrorV1> {
        if !self.markets.contains_key(condition_id) {
            return Err(RebateStateErrorV1::UnknownMarket(condition_id.to_owned()));
        }
        if actual_taker_fee_pusd.is_sign_negative() {
            return Err(RebateStateErrorV1::NegativeActualTakerFee);
        }
        if actual_taker_fee_pusd.is_zero() {
            return Ok(None);
        }
        self.ensure_new_trade(trade_id)?;
        if patch.maker_fills.contains_key(trade_id) || patch.taker_fills.contains_key(trade_id) {
            return Err(RebateStateErrorV1::DuplicateTrade(trade_id.to_owned()));
        }

        let attribution_day_utc_ms = utc_day_start(matched_at_utc_ms)?;
        let accrual_pusd = actual_taker_fee_pusd
            .checked_mul(self.taker_rebate_rate)
            .ok_or(RebateStateErrorV1::ArithmeticOverflow)?
            .normalize();
        let maturity_at_utc_ms = rebate_maturity_at(RebateKindV1::Taker, attribution_day_utc_ms)?;
        let potential_accrual_pusd = patch
            .day_buckets
            .get(&attribution_day_utc_ms)
            .map(|day| day.potential_accrual_pusd)
            .or_else(|| {
                self.day_buckets
                    .get(&(RebateKindV1::Taker, attribution_day_utc_ms))
                    .map(|day| day.potential_accrual_pusd)
            })
            .map_or(Ok(accrual_pusd), |potential| {
                potential
                    .checked_add(accrual_pusd)
                    .ok_or(RebateStateErrorV1::ArithmeticOverflow)
            })?
            .normalize();

        Ok(Some(PreparedTakerFillRegistrationV2 {
            trade_id: trade_id.to_owned(),
            fill: TakerFillStateV1 {
                condition_id: condition_id.to_owned(),
                matched_at_utc_ms,
                attribution_day_utc_ms,
                actual_taker_fee_pusd,
                rebate_rate: self.taker_rebate_rate,
                accrual_pusd,
                terminal_succeeded: None,
            },
            maturity_at_utc_ms,
            potential_accrual_pusd,
            creates_day_bucket: !self
                .day_buckets
                .contains_key(&(RebateKindV1::Taker, attribution_day_utc_ms)),
        }))
    }

    pub(crate) fn on_fill_terminal(
        &mut self,
        trade_id: &str,
        succeeded: bool,
        platform_effective_at_utc_ms: i64,
    ) -> Vec<RebateSettlementActionV1> {
        let mut patch = RebateStatePatchV2::default();
        let _ = patch.stage_fill_terminal(self, trade_id, succeeded, platform_effective_at_utc_ms);
        let actions = patch.take_settlement_actions();
        patch.commit_prevalidated(self);
        actions
    }

    pub(crate) fn next_clock_at_utc_ms(&self) -> Option<i64> {
        self.clocks.first().map(|(at, _)| *at)
    }

    pub(crate) fn advance_clock_at(
        &mut self,
        exact_next_at_utc_ms: i64,
    ) -> Vec<RebateSettlementActionV1> {
        let mut patch = RebateStatePatchV2::default();
        let _ = patch.stage_clock_at(self, exact_next_at_utc_ms);
        let actions = patch.take_settlement_actions();
        patch.commit_prevalidated(self);
        actions
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

    fn drain_affected(
        &mut self,
        platform_effective_at_utc_ms: i64,
        affected_markets: BTreeSet<String>,
        affected_maker_days: BTreeSet<i64>,
        affected_taker_days: BTreeSet<i64>,
    ) -> Vec<RebateSettlementActionV1> {
        let mut patch = RebateStatePatchV2::default();
        patch.stage_drain_affected(
            self,
            platform_effective_at_utc_ms,
            affected_markets,
            affected_maker_days,
            affected_taker_days,
        );
        let actions = patch.take_settlement_actions();
        patch.commit_prevalidated(self);
        actions
    }
}

fn ready_maker_market_result(
    market: &MakerMarketStateV1,
) -> Option<MakerRebateMarketResultStatusV1> {
    ready_maker_market_result_at_end(market, market.end_reached)
}

fn ready_maker_market_result_at_end(
    market: &MakerMarketStateV1,
    end_reached: bool,
) -> Option<MakerRebateMarketResultStatusV1> {
    (end_reached
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
    use std::sync::Arc;

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

    fn indexed_market(index: u64, end_at_utc_ms: i64) -> PlatformMarketV1 {
        PlatformMarketV1::try_new(
            MarketIdentityV2 {
                condition_id: ConditionId(format!("0x{index:064x}")),
                clob_market_id: ClobMarketId(format!("30{index}")),
                window: MarketWindowV2 {
                    start_at_utc_ms: 0,
                    end_at_utc_ms,
                },
            },
            [
                AssetIdentityV1 {
                    token_id: TokenId(format!("40{index}1")),
                    outcome: OutcomeIdentityV1 {
                        index: 0,
                        label: "Up".to_owned(),
                    },
                },
                AssetIdentityV1 {
                    token_id: TokenId(format!("40{index}2")),
                    outcome: OutcomeIdentityV1 {
                        index: 1,
                        label: "Down".to_owned(),
                    },
                },
            ],
        )
        .expect("索引测试市场")
    }

    async fn state(markets: Vec<PlatformMarketV1>) -> RebateStateV1 {
        let scenario = PlatformScenarioV1::try_new(markets).expect("测试场景");
        let directory = SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("市场目录");
        let rules = successful_snapshot_for_sim_tests().await;
        RebateStateV1::new(&directory, &rules)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_rebate_state_delta_v2_sequential_clock_visits_are_linear() {
        const CLOCK_COUNT: usize = 512;
        const FIRST_CLOCK_AT: i64 = 10_000;
        let markets = (0..CLOCK_COUNT)
            .map(|index| indexed_market(index as u64 + 1, FIRST_CLOCK_AT + index as i64))
            .collect::<Vec<_>>();
        let live = state(markets).await;
        let mut live = Arc::new(live);
        reset_rebate_live_clock_entry_visit_count_for_tests();

        let mut prepared = PreparedRebateStateDeltaV2::prepare(&mut live);
        for index in 0..CLOCK_COUNT {
            let expected_at = FIRST_CLOCK_AT + index as i64;
            assert_eq!(prepared.candidate_next_clock(), Some(expected_at));
            assert_eq!(prepared.stage_clock_at(expected_at).len(), 1);
        }
        assert_eq!(prepared.candidate_next_clock(), None);
        assert_eq!(prepared.settlement_actions().len(), CLOCK_COUNT);
        let visits = rebate_live_clock_entry_visit_count_for_tests();
        assert!(
            visits <= CLOCK_COUNT * 4 + 4,
            "live clock 访问必须线性：visits={visits}, clocks={CLOCK_COUNT}"
        );
        prepared
            .into_committable()
            .expect("连续返佣时钟候选可提交")
            .commit();

        assert_eq!(live.next_clock_at_utc_ms(), None);
        for index in 0..CLOCK_COUNT {
            let condition_id = format!("0x{:064x}", index as u64 + 1);
            assert_eq!(
                live.market_result(&condition_id),
                Some(MakerRebateMarketResultStatusV1::ZeroNoEligibleMakerFill)
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rebate_state_patch_v2_inserted_maturity_clock_visits_are_linear() {
        const CLOCK_COUNT: usize = 128;
        const MARKET_END_AT: i64 = (CLOCK_COUNT as i64 + 2) * UTC_DAY_MILLIS;
        let member = market('1', 0, MARKET_END_AT);
        let condition_id = member.market().condition_id.0.clone();
        let mut live = state(vec![member]).await;
        let mut patch = RebateStatePatchV2::default();

        for index in 0..CLOCK_COUNT {
            let matched_at_utc_ms = index as i64 * UTC_DAY_MILLIS + 500;
            assert!(
                patch
                    .stage_taker_fill(
                        &live,
                        &format!("inserted-clock-{index:04}"),
                        &condition_id,
                        matched_at_utc_ms,
                        decimal("1"),
                    )
                    .expect("通过正式 patch API 登记跨日 taker 成交")
            );
        }
        assert_eq!(patch.clock_insertions.len(), CLOCK_COUNT);
        reset_rebate_inserted_clock_entry_visit_count_for_tests();

        let mut staged_clock_count = 0;
        while let Some(next_at_utc_ms) = patch.candidate_next_clock(&live) {
            let _ = patch.stage_clock_at(&live, next_at_utc_ms);
            staged_clock_count += 1;
        }
        assert_eq!(staged_clock_count, CLOCK_COUNT + 1);
        assert!(patch.clock_insertions.is_empty());
        let visits = rebate_inserted_clock_entry_visit_count_for_tests();
        assert_eq!(
            visits, CLOCK_COUNT,
            "每个 distinct exact-time 插入桶只能访问自己的唯一成员"
        );
        assert!(
            visits <= CLOCK_COUNT * 4 + 4,
            "共享 patch 供后续唯一 Drive 组合时，inserted maturity clock 访问必须线性；这是组合资源边界而非当前生产事故：visits={visits}, clocks={CLOCK_COUNT}"
        );

        patch.commit_prevalidated(&mut live);
        assert_eq!(live.next_clock_at_utc_ms(), None);
        assert_eq!(
            live.market_result(&condition_id),
            Some(MakerRebateMarketResultStatusV1::ZeroNoEligibleMakerFill)
        );
        for index in 0..CLOCK_COUNT {
            let attribution_day_utc_ms = index as i64 * UTC_DAY_MILLIS;
            let day = live
                .day_snapshot(RebateKindV1::Taker, attribution_day_utc_ms)
                .expect("提交后每个正式登记日桶必须存在");
            assert!(day.maturity_reached);
            assert_eq!(day.pending_predecessor_count, 1);
            assert!(!day.finalized);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rebate_state_patch_v2_mature_taker_terminals_count_pending_linearly() {
        const FILL_COUNT: usize = 128;
        const MARKET_END_AT: i64 = 2_000;
        let member = market('1', 0, MARKET_END_AT);
        let condition_id = member.market().condition_id.0.clone();
        let mut live = state(vec![member]).await;

        let mut prepared = live.prepare_taker_rebate_registrations_v2();
        for index in 0..FILL_COUNT {
            assert!(
                prepared
                    .stage_taker_fill(
                        &format!("pending-hot-{index:04}"),
                        &condition_id,
                        500 + index as i64,
                        decimal("1"),
                    )
                    .expect("通过正式批量入口登记同日 taker 成交")
            );
        }
        prepared
            .into_committable()
            .expect("同日 taker 注册候选可提交")
            .commit();

        assert!(matches!(
            live.advance_clock_at(MARKET_END_AT).as_slice(),
            [RebateSettlementActionV1::MakerMarketFinalized(_)]
        ));
        let maturity_at = UTC_DAY_MILLIS + TAKER_PAYMENT_MINUTE_UTC * MILLIS_PER_MINUTE;
        assert!(live.advance_clock_at(maturity_at).is_empty());
        let matured_day = live
            .day_snapshot(RebateKindV1::Taker, 0)
            .expect("同日 taker 日桶");
        assert!(matured_day.maturity_reached);
        assert_eq!(matured_day.pending_predecessor_count, FILL_COUNT);
        assert!(!matured_day.finalized);

        let mut patch = RebateStatePatchV2::default();
        reset_rebate_pending_membership_entry_visit_count_for_tests();
        for index in 0..FILL_COUNT {
            let actions = patch
                .stage_registered_fill_terminal(
                    &live,
                    &format!("pending-hot-{index:04}"),
                    true,
                    maturity_at + index as i64,
                )
                .expect("严格入口必须接受已登记 taker 终态");
            if index + 1 == FILL_COUNT {
                assert!(matches!(
                    actions,
                    [
                        RebateSettlementActionV1::TakerAccrualApplied(_),
                        RebateSettlementActionV1::DayFinalized(_),
                        RebateSettlementActionV1::Paid(_)
                    ]
                ));
            } else {
                assert!(matches!(
                    actions,
                    [RebateSettlementActionV1::TakerAccrualApplied(_)]
                ));
            }
        }
        let visits = rebate_pending_membership_entry_visit_count_for_tests();
        assert_eq!(
            visits, FILL_COUNT,
            "同一候选 patch 的 pending 计数不得累计重扫既有 removals"
        );
        assert!(
            visits <= FILL_COUNT * 4 + 4,
            "成熟日逐成交终态的 pending membership 访问必须线性：visits={visits}, fills={FILL_COUNT}"
        );
        assert_eq!(
            patch
                .settlement_actions
                .iter()
                .filter(|action| matches!(action, RebateSettlementActionV1::TakerAccrualApplied(_)))
                .count(),
            FILL_COUNT
        );
        assert!(matches!(
            patch.settlement_actions.as_slice(),
            [
                ..,
                RebateSettlementActionV1::DayFinalized(_),
                RebateSettlementActionV1::Paid(_)
            ]
        ));

        patch.commit_prevalidated(&mut live);
        assert!(
            live.taker_fills
                .values()
                .all(|fill| fill.terminal_succeeded == Some(true))
        );
        let final_day = live
            .day_snapshot(RebateKindV1::Taker, 0)
            .expect("终态提交后 taker 日桶");
        assert_eq!(final_day.pending_predecessor_count, 0);
        assert_eq!(final_day.accrued_pusd, decimal("10.24"));
        assert!(final_day.finalized);
        assert_eq!(final_day.payable_pusd, decimal("10.24"));
        assert!(final_day.paid);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rebate_state_patch_v2_late_maker_terminals_track_readiness_linearly() {
        const FILL_COUNT: usize = 128;
        const MARKET_END_AT: i64 = 2_000;
        let member = market('1', 0, MARKET_END_AT);
        let condition_id = member.market().condition_id.0.clone();
        let mut live = state(vec![member]).await;

        for index in 0..FILL_COUNT {
            live.register_maker_fill(
                &format!("maker-hot-{index:04}"),
                &condition_id,
                1_000 + index as i64,
                10_000_000,
                decimal("0.5"),
            )
            .expect("通过正式入口登记 maker 成交");
        }
        assert!(live.advance_clock_at(MARKET_END_AT).is_empty());
        assert_eq!(live.market_result(&condition_id), None);

        let mut patch = RebateStatePatchV2::default();
        reset_rebate_maker_status_fill_entry_visit_count_for_tests();
        for index in 0..FILL_COUNT {
            let actions = patch
                .stage_registered_fill_terminal(
                    &live,
                    &format!("maker-hot-{index:04}"),
                    index == 0,
                    3_000 + index as i64,
                )
                .expect("严格入口必须接受已登记 maker 终态");
            if index + 1 == FILL_COUNT {
                assert!(matches!(
                    actions,
                    [RebateSettlementActionV1::MakerMarketFinalized(_)]
                ));
            } else {
                assert!(actions.is_empty());
            }
        }
        let visits = rebate_maker_status_fill_entry_visit_count_for_tests();
        assert_eq!(
            visits, FILL_COUNT,
            "同一候选 patch 的 maker readiness 不得从头累计重扫 fills"
        );
        assert!(
            visits <= FILL_COUNT * 4 + 4,
            "盘后逐成交终态的 maker status fill 访问必须线性：visits={visits}, fills={FILL_COUNT}"
        );
        let [RebateSettlementActionV1::MakerMarketFinalized(action)] =
            patch.settlement_actions.as_slice()
        else {
            panic!("最后一个 maker 终态必须唯一形成市场返佣结果")
        };
        assert_eq!(action.status, MakerRebateMarketResultStatusV1::Calculated);
        assert_eq!(action.contributions.len(), 1);
        assert_eq!(action.contributions[0].trade_id, "maker-hot-0000");
        assert_eq!(action.accrual_pusd, decimal("0.035"));
        assert_eq!(action.platform_effective_at_utc_ms, 3_127);

        patch.commit_prevalidated(&mut live);
        assert_eq!(
            live.market_result(&condition_id),
            Some(MakerRebateMarketResultStatusV1::Calculated)
        );
        let market = live.markets.get(&condition_id).expect("maker 市场");
        assert_eq!(
            market
                .fills
                .values()
                .filter(|fill| fill.terminal_succeeded == Some(true))
                .count(),
            1
        );
        assert!(
            market
                .fills
                .values()
                .all(|fill| fill.terminal_succeeded.is_some())
        );
        let day = live
            .day_snapshot(RebateKindV1::Maker, 0)
            .expect("终态提交后 maker 日桶");
        assert_eq!(day.pending_predecessor_count, 0);
        assert_eq!(day.accrued_pusd, decimal("0.035"));
        assert!(!day.maturity_reached);
        assert!(!day.finalized);
        assert!(!day.paid);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_rebate_state_delta_v2_large_history_drop_and_commit_are_sparse() {
        let member = market('1', 0, 2_000);
        let condition_id = member.market().condition_id.0.clone();
        let mut live = state(vec![member]).await;
        for index in 0..512 {
            live.register_taker_fill(
                &format!("unrelated-history-{index:04}"),
                &condition_id,
                500 + index,
                decimal("1"),
            )
            .expect("建立大规模无关返佣历史");
        }
        let before_drop = live.clone();
        let mut live = Arc::new(live);
        reset_rebate_state_clone_count_for_tests();

        {
            let mut prepared = PreparedRebateStateDeltaV2::prepare(&mut live);
            assert_eq!(prepared.candidate_next_clock(), Some(2_000));
            let actions = prepared.stage_clock_at(2_000);
            let [RebateSettlementActionV1::MakerMarketFinalized(action)] = actions else {
                panic!("市场结束必须只形成一个 maker 市场最终化动作")
            };
            assert_eq!(
                action.reason_identity,
                format!("maker-rebate-market-finalized-v1:{condition_id}")
            );
            assert_eq!(action.condition_id, condition_id);
            assert_eq!(action.platform_effective_at_utc_ms, 2_000);
            assert_eq!(
                action.status,
                MakerRebateMarketResultStatusV1::ZeroNoEligibleMakerFill
            );
            assert!(action.contributions.is_empty());
            assert_eq!(action.accrual_pusd, Decimal::ZERO);
            assert!(prepared.has_state_change());
            assert_eq!(
                prepared.candidate_market_result(&condition_id),
                Some(MakerRebateMarketResultStatusV1::ZeroNoEligibleMakerFill)
            );
            assert_eq!(prepared.patch.clock_removals.len(), 1);
            assert_eq!(prepared.patch.market_results.len(), 1);
            assert!(prepared.patch.day_bucket_updates.is_empty());
            assert!(prepared.patch.taker_fills.is_empty());
        }

        assert_eq!(live.as_ref(), &before_drop);
        assert_eq!(rebate_state_clone_count_for_tests(), 0);

        let mut prepared = PreparedRebateStateDeltaV2::prepare(&mut live);
        let actions = prepared.stage_clock_at(2_000);
        let [RebateSettlementActionV1::MakerMarketFinalized(action)] = actions else {
            panic!("提交候选也必须只形成一个 maker 市场最终化动作")
        };
        assert_eq!(
            action.reason_identity,
            format!("maker-rebate-market-finalized-v1:{condition_id}")
        );
        assert_eq!(action.condition_id, condition_id);
        assert_eq!(action.platform_effective_at_utc_ms, 2_000);
        assert_eq!(
            action.status,
            MakerRebateMarketResultStatusV1::ZeroNoEligibleMakerFill
        );
        assert!(action.contributions.is_empty());
        assert_eq!(action.accrual_pusd, Decimal::ZERO);
        assert_eq!(prepared.settlement_actions().len(), 1);
        prepared
            .into_committable()
            .expect("独占返佣状态候选可提交")
            .commit();

        let target_market = live.markets.get(&condition_id).expect("目标返佣市场");
        assert!(target_market.end_reached);
        assert_eq!(
            target_market.result,
            Some(MakerRebateMarketResultStatusV1::ZeroNoEligibleMakerFill)
        );
        assert!(target_market.fills.is_empty());
        let mut expected_clocks = before_drop.clocks.clone();
        assert!(expected_clocks.remove(&(
            2_000,
            RebateClockIdentityV1::MarketEnd(condition_id.clone())
        )));
        assert_eq!(live.clocks, expected_clocks);
        assert_eq!(
            live.next_clock_at_utc_ms(),
            Some(UTC_DAY_MILLIS + TAKER_PAYMENT_MINUTE_UTC * MILLIS_PER_MINUTE)
        );
        assert_eq!(live.taker_fills, before_drop.taker_fills);
        assert_eq!(live.day_buckets, before_drop.day_buckets);
        assert_eq!(rebate_state_clone_count_for_tests(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_rebate_state_delta_v2_maturity_with_pending_predecessor_changes_state_only() {
        let member = market('1', 0, 1_000);
        let condition_id = member.market().condition_id.0.clone();
        let mut live = state(vec![member]).await;
        live.register_taker_fill("pending-taker", &condition_id, 500, decimal("20"))
            .expect("注册未终态 taker 成交");
        let _ = live.advance_clock_at(1_000);
        let maturity_at = UTC_DAY_MILLIS + TAKER_PAYMENT_MINUTE_UTC * MILLIS_PER_MINUTE;
        let before = live.clone();
        let mut live = Arc::new(live);
        reset_rebate_state_clone_count_for_tests();

        {
            let mut prepared = PreparedRebateStateDeltaV2::prepare(&mut live);
            assert_eq!(prepared.candidate_next_clock(), Some(maturity_at));
            assert!(prepared.stage_clock_at(maturity_at).is_empty());
            assert!(prepared.has_state_change());
            assert_eq!(prepared.patch.clock_removals.len(), 1);
            assert_eq!(prepared.patch.day_bucket_updates.len(), 1);
        }
        assert_eq!(live.as_ref(), &before);

        let mut prepared = PreparedRebateStateDeltaV2::prepare(&mut live);
        assert!(prepared.stage_clock_at(maturity_at).is_empty());
        assert!(prepared.settlement_actions().is_empty());
        prepared
            .into_committable()
            .expect("独占状态变化候选可提交")
            .commit();
        let day = live
            .day_snapshot(RebateKindV1::Taker, 0)
            .expect("taker 归属日桶");
        assert!(day.maturity_reached);
        assert_eq!(day.pending_predecessor_count, 1);
        assert!(!day.finalized);
        assert_eq!(rebate_state_clone_count_for_tests(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_rebate_state_delta_v2_large_pending_clock_stage_does_not_clone_bucket() {
        let member = market('1', 0, 1_000);
        let condition_id = member.market().condition_id.0.clone();
        let mut live = state(vec![member]).await;
        for index in 0..512 {
            live.register_taker_fill(
                &format!("large-pending-{index:04}"),
                &condition_id,
                500 + index,
                decimal("1"),
            )
            .expect("建立大规模 pending 前驱");
        }
        let _ = live.advance_clock_at(1_000);
        let maturity_at = UTC_DAY_MILLIS + TAKER_PAYMENT_MINUTE_UTC * MILLIS_PER_MINUTE;
        let before = live.clone();
        let mut live = Arc::new(live);
        reset_rebate_state_clone_count_for_tests();
        reset_rebate_day_bucket_clone_count_for_tests();

        {
            let mut prepared = PreparedRebateStateDeltaV2::prepare(&mut live);
            assert!(prepared.stage_clock_at(maturity_at).is_empty());
            assert!(prepared.has_state_change());
            assert_eq!(prepared.patch.day_bucket_updates.len(), 1);
            let update = prepared
                .patch
                .day_bucket_updates
                .get(&(RebateKindV1::Taker, 0))
                .expect("taker 成熟时钟只形成目标日桶稀疏更新");
            assert!(update.maturity_reached);
            assert!(update.pending_predecessors_to_remove.is_empty());
            assert!(update.accrued_pusd_after.is_none());
            assert!(update.final_state_after.is_none());
            assert_eq!(rebate_day_bucket_clone_count_for_tests(), 0);
        }

        assert_eq!(live.as_ref(), &before);
        assert_eq!(rebate_state_clone_count_for_tests(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_rebate_strict_registered_terminal_is_idempotent_and_conflict_safe() {
        let member = market('1', 0, 2_000);
        let condition_id = member.market().condition_id.0.clone();
        let mut live = state(vec![member]).await;
        live.register_taker_fill("strict-taker", &condition_id, 500, decimal("20"))
            .expect("注册严格终态测试 taker 成交");
        let live_before_commit = live.clone();
        let mut patch = RebateStatePatchV2::default();

        let first = patch
            .stage_registered_fill_terminal(&live, "strict-taker", true, 1_000)
            .expect("已登记 taker 首次终态可准备");
        let [RebateSettlementActionV1::TakerAccrualApplied(accrual)] = first else {
            panic!("已登记 taker 首次成功终态必须形成唯一应计动作")
        };
        assert_eq!(accrual.trade_id, "strict-taker");
        assert_eq!(accrual.condition_id, condition_id);
        assert_eq!(accrual.platform_effective_at_utc_ms, 1_000);
        assert_eq!(accrual.accrual_pusd, decimal("1.6"));
        let patch_after_first = patch.clone();

        assert!(
            patch
                .stage_registered_fill_terminal(&live, "strict-taker", true, 1_001)
                .expect("相同终态值必须幂等成功")
                .is_empty()
        );
        assert_eq!(patch, patch_after_first);

        assert_eq!(
            patch
                .stage_registered_fill_terminal(&live, "strict-taker", false, 1_002)
                .expect_err("相反终态值必须明确冲突"),
            RebateStateErrorV1::TerminalIdentityConflict("strict-taker".to_owned())
        );
        assert_eq!(patch, patch_after_first);

        assert_eq!(
            patch
                .stage_registered_fill_terminal(&live, "unknown-strict-fill", true, 1_003)
                .expect_err("严格入口不得静默接受未知成交"),
            RebateStateErrorV1::UnknownTrade("unknown-strict-fill".to_owned())
        );
        assert_eq!(patch, patch_after_first);
        assert_eq!(live, live_before_commit);

        patch.commit_prevalidated(&mut live);
        assert_eq!(
            live.taker_fills
                .get("strict-taker")
                .expect("严格终态成交仍存在")
                .terminal_succeeded,
            Some(true)
        );
        let day = live
            .day_snapshot(RebateKindV1::Taker, 0)
            .expect("严格终态 taker 日桶");
        assert_eq!(day.pending_predecessor_count, 0);
        assert_eq!(day.accrued_pusd, decimal("1.6"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_rebate_state_delta_v2_terminal_then_same_clock_reads_candidate_terminal() {
        let maker_member = market('1', 1_000, 2_000);
        let maker_condition_id = maker_member.market().condition_id.0.clone();
        let mut maker_live = state(vec![maker_member]).await;
        maker_live
            .register_maker_fill(
                "candidate-maker",
                &maker_condition_id,
                1_500,
                10_000_000,
                decimal("0.5"),
            )
            .expect("注册 maker 候选成交");
        let mut maker_live = Arc::new(maker_live);
        let mut prepared = PreparedRebateStateDeltaV2::prepare(&mut maker_live);
        assert!(
            prepared
                .stage_fill_terminal("candidate-maker", true, 2_000)
                .is_empty()
        );
        assert_eq!(prepared.patch.maker_terminal_after.len(), 1);
        let actions = prepared.stage_clock_at(2_000);
        let [RebateSettlementActionV1::MakerMarketFinalized(action)] = actions else {
            panic!("maker terminal 候选必须让同刻市场时钟完成市场返佣")
        };
        assert_eq!(action.condition_id, maker_condition_id);
        assert_eq!(action.platform_effective_at_utc_ms, 2_000);
        assert_eq!(action.status, MakerRebateMarketResultStatusV1::Calculated);
        assert_eq!(action.contributions.len(), 1);
        assert_eq!(action.contributions[0].trade_id, "candidate-maker");
        assert_eq!(action.accrual_pusd, decimal("0.035"));
        assert_eq!(
            prepared.candidate_market_result(&maker_condition_id),
            Some(MakerRebateMarketResultStatusV1::Calculated)
        );
        prepared
            .into_committable()
            .expect("maker terminal 与时钟候选可提交")
            .commit();
        let maker_market = maker_live
            .markets
            .get(&maker_condition_id)
            .expect("maker 市场");
        assert_eq!(
            maker_market
                .fills
                .get("candidate-maker")
                .expect("maker 成交")
                .terminal_succeeded,
            Some(true)
        );
        assert_eq!(
            maker_market.result,
            Some(MakerRebateMarketResultStatusV1::Calculated)
        );
        let maker_day = maker_live
            .day_snapshot(RebateKindV1::Maker, 0)
            .expect("maker 日桶");
        assert_eq!(maker_day.pending_predecessor_count, 0);
        assert_eq!(maker_day.accrued_pusd, decimal("0.035"));

        let taker_member = market('2', 0, 1_000);
        let taker_condition_id = taker_member.market().condition_id.0.clone();
        let mut taker_live = state(vec![taker_member]).await;
        taker_live
            .register_taker_fill("candidate-taker", &taker_condition_id, 500, decimal("20"))
            .expect("注册 taker 候选成交");
        let _ = taker_live.advance_clock_at(1_000);
        let maturity_at = UTC_DAY_MILLIS + TAKER_PAYMENT_MINUTE_UTC * MILLIS_PER_MINUTE;
        let mut taker_live = Arc::new(taker_live);
        let mut prepared = PreparedRebateStateDeltaV2::prepare(&mut taker_live);
        let terminal_actions = prepared.stage_fill_terminal("candidate-taker", true, maturity_at);
        let [RebateSettlementActionV1::TakerAccrualApplied(accrual)] = terminal_actions else {
            panic!("taker terminal 候选必须先形成一个应计动作")
        };
        assert_eq!(
            accrual.reason_identity,
            "taker-rebate-accrual-applied-v1:candidate-taker"
        );
        assert_eq!(accrual.trade_id, "candidate-taker");
        assert_eq!(accrual.condition_id, taker_condition_id);
        assert_eq!(accrual.matched_at_utc_ms, 500);
        assert_eq!(accrual.attribution_day_utc_ms, 0);
        assert_eq!(accrual.platform_effective_at_utc_ms, maturity_at);
        assert_eq!(accrual.actual_taker_fee_pusd, decimal("20"));
        assert_eq!(accrual.accrual_pusd, decimal("1.6"));
        assert_eq!(prepared.patch.taker_terminal_after.len(), 1);
        let clock_actions = prepared.stage_clock_at(maturity_at);
        let [
            RebateSettlementActionV1::DayFinalized(finalized),
            RebateSettlementActionV1::Paid(paid),
        ] = clock_actions
        else {
            panic!("taker 成熟时钟必须依次形成日结与付款动作")
        };
        assert_eq!(finalized.reason_identity, "taker-rebate-day-finalized-v1:0");
        assert_eq!(finalized.kind, RebateKindV1::Taker);
        assert_eq!(finalized.attribution_day_utc_ms, 0);
        assert_eq!(finalized.platform_effective_at_utc_ms, maturity_at);
        assert_eq!(finalized.accrued_pusd, decimal("1.6"));
        assert_eq!(finalized.payable_pusd, decimal("1.6"));
        assert_eq!(finalized.locked_remainder_pusd, Decimal::ZERO);
        assert_eq!(paid.reason_identity, "taker-rebate-paid-v1:0");
        assert_eq!(paid.kind, RebateKindV1::Taker);
        assert_eq!(paid.attribution_day_utc_ms, 0);
        assert_eq!(paid.platform_effective_at_utc_ms, maturity_at);
        assert_eq!(paid.paid_pusd, decimal("1.6"));
        assert!(matches!(
            prepared.settlement_actions(),
            [
                RebateSettlementActionV1::TakerAccrualApplied(_),
                RebateSettlementActionV1::DayFinalized(_),
                RebateSettlementActionV1::Paid(_)
            ]
        ));
        prepared
            .into_committable()
            .expect("taker terminal 与成熟时钟候选可提交")
            .commit();
        assert_eq!(
            taker_live
                .taker_fills
                .get("candidate-taker")
                .expect("taker 成交")
                .terminal_succeeded,
            Some(true)
        );
        let taker_day = taker_live
            .day_snapshot(RebateKindV1::Taker, 0)
            .expect("taker 日桶");
        assert_eq!(taker_day.pending_predecessor_count, 0);
        assert_eq!(taker_day.accrued_pusd, decimal("1.6"));
        assert!(taker_day.finalized);
        assert_eq!(taker_day.payable_pusd, decimal("1.6"));
        assert!(taker_day.paid);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_rebate_state_delta_v2_late_last_terminal_drains_candidate_maturity_once() {
        let member = market('1', 0, 1_000);
        let condition_id = member.market().condition_id.0.clone();
        let mut live = state(vec![member]).await;
        live.register_taker_fill("late-candidate", &condition_id, 500, decimal("20"))
            .expect("注册晚终态 taker 成交");
        let _ = live.advance_clock_at(1_000);
        let maturity_at = UTC_DAY_MILLIS + TAKER_PAYMENT_MINUTE_UTC * MILLIS_PER_MINUTE;
        let terminal_at = maturity_at + 5_000;
        let mut live = Arc::new(live);
        let mut prepared = PreparedRebateStateDeltaV2::prepare(&mut live);
        assert!(prepared.stage_clock_at(maturity_at).is_empty());
        let actions = prepared.stage_fill_terminal("late-candidate", true, terminal_at);
        assert!(matches!(
            actions,
            [
                RebateSettlementActionV1::TakerAccrualApplied(_),
                RebateSettlementActionV1::DayFinalized(_),
                RebateSettlementActionV1::Paid(_)
            ]
        ));
        assert!(
            actions
                .iter()
                .all(|action| action.platform_effective_at_utc_ms() == terminal_at)
        );
        assert!(
            prepared
                .stage_fill_terminal("late-candidate", true, terminal_at + 1)
                .is_empty()
        );
        assert_eq!(prepared.settlement_actions().len(), 3);
        prepared
            .into_committable()
            .expect("成熟后最后前驱候选可提交")
            .commit();
        let day = live
            .day_snapshot(RebateKindV1::Taker, 0)
            .expect("taker 日桶");
        assert_eq!(day.pending_predecessor_count, 0);
        assert_eq!(day.accrued_pusd, decimal("1.6"));
        assert!(day.finalized);
        assert!(day.paid);
        assert!(
            Arc::get_mut(&mut live)
                .expect("提交后 Arc 仍唯一")
                .on_fill_terminal("late-candidate", true, terminal_at + 2)
                .is_empty()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_rebate_state_delta_v2_large_pending_terminal_is_sparse_and_arc_safe() {
        let member = market('1', 0, 1_000);
        let condition_id = member.market().condition_id.0.clone();
        let mut live = state(vec![member]).await;
        for index in 0..512 {
            live.register_taker_fill(
                &format!("sparse-terminal-{index:04}"),
                &condition_id,
                500 + index,
                decimal("1"),
            )
            .expect("建立大规模 pending 前驱");
        }
        let before = live.clone();
        let mut live = Arc::new(live);
        let shared = Arc::clone(&live);
        reset_rebate_state_clone_count_for_tests();
        reset_rebate_day_bucket_clone_count_for_tests();

        {
            let mut prepared = PreparedRebateStateDeltaV2::prepare(&mut live);
            assert!(
                prepared
                    .stage_fill_terminal("sparse-terminal-0000", false, 2_000)
                    .is_empty()
            );
            assert!(prepared.has_state_change());
            assert_eq!(prepared.patch.taker_terminal_after.len(), 1);
            let update = prepared
                .patch
                .day_bucket_updates
                .get(&(RebateKindV1::Taker, 0))
                .expect("taker 日桶稀疏更新");
            assert_eq!(update.pending_predecessors_to_remove.len(), 1);
            assert!(update.accrued_pusd_after.is_none());
            assert_eq!(rebate_day_bucket_clone_count_for_tests(), 0);
        }
        assert_eq!(live.as_ref(), &before);

        let mut prepared = PreparedRebateStateDeltaV2::prepare(&mut live);
        assert!(
            prepared
                .stage_fill_terminal("sparse-terminal-0000", false, 2_000)
                .is_empty()
        );
        let Err(error) = prepared.into_committable() else {
            panic!("共享 Arc 的非空 terminal 候选不得提交")
        };
        assert_eq!(
            error,
            RebateStateErrorV1::SharedStateRequiresUniqueOwnership
        );
        assert_eq!(live.as_ref(), &before);

        let mut prepared = PreparedRebateStateDeltaV2::prepare(&mut live);
        assert!(
            prepared
                .stage_fill_terminal("unregistered-zero-fee", true, 2_000)
                .is_empty()
        );
        assert!(!prepared.has_state_change());
        prepared
            .into_committable()
            .expect("unknown terminal no-op 不要求 Arc 独占")
            .commit();
        assert!(Arc::ptr_eq(&live, &shared));
        assert_eq!(live.as_ref(), &before);
        assert_eq!(rebate_state_clone_count_for_tests(), 0);
        assert_eq!(rebate_day_bucket_clone_count_for_tests(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_rebate_state_delta_v2_shared_arc_allows_noop_and_rejects_due_write() {
        let member = market('1', 0, 1_000);
        let condition_id = member.market().condition_id.0.clone();
        let live_state = state(vec![member]).await;
        let before = live_state.clone();
        let mut live = Arc::new(live_state);
        let shared = Arc::clone(&live);
        reset_rebate_state_clone_count_for_tests();

        let mut prepared = PreparedRebateStateDeltaV2::prepare(&mut live);
        assert!(prepared.stage_clock_at(999).is_empty());
        assert!(!prepared.has_state_change());
        assert!(prepared.settlement_actions().is_empty());
        prepared
            .into_committable()
            .expect("零变化不要求返佣 Arc 独占")
            .commit();
        assert!(Arc::ptr_eq(&live, &shared));
        assert_eq!(live.as_ref(), &before);

        let mut prepared = PreparedRebateStateDeltaV2::prepare(&mut live);
        assert!(!prepared.stage_clock_at(1_000).is_empty());
        assert!(prepared.has_state_change());
        assert_eq!(
            prepared.candidate_market_result(&condition_id),
            Some(MakerRebateMarketResultStatusV1::ZeroNoEligibleMakerFill)
        );
        let Err(error) = prepared.into_committable() else {
            panic!("共享返佣 Arc 的非空候选不得成为可提交令牌")
        };
        assert_eq!(
            error,
            RebateStateErrorV1::SharedStateRequiresUniqueOwnership
        );
        assert!(Arc::ptr_eq(&live, &shared));
        assert_eq!(live.as_ref(), &before);
        assert_eq!(rebate_state_clone_count_for_tests(), 0);
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
    async fn prepared_taker_rebate_v2_large_history_stages_sparse_patch_without_state_clone() {
        let member = market('1', 0, UTC_DAY_MILLIS * 3);
        let condition_id = member.market().condition_id.0.clone();
        let mut state = state(vec![member]).await;
        for index in 0..512 {
            state
                .register_taker_fill(
                    &format!("history-{index:04}"),
                    &condition_id,
                    1_000 + index,
                    decimal("1"),
                )
                .expect("建立大规模既有 taker 历史");
        }
        let before_drop = state.clone();
        reset_rebate_state_clone_count_for_tests();

        {
            let mut prepared = state.prepare_taker_rebate_registrations_v2();
            assert!(
                prepared
                    .stage_taker_fill("drop-a", &condition_id, 10_000, decimal("2"))
                    .expect("准备同日 taker 成交")
            );
            assert!(
                prepared
                    .stage_taker_fill("drop-b", &condition_id, 20_000, decimal("3"))
                    .expect("准备第二笔同日 taker 成交")
            );
            assert!(
                prepared
                    .stage_taker_fill(
                        "drop-c",
                        &condition_id,
                        UTC_DAY_MILLIS + 1_000,
                        decimal("4"),
                    )
                    .expect("准备跨日 taker 成交")
            );
            assert_eq!(prepared.patch.taker_fills.len(), 3);
            assert_eq!(prepared.patch.day_buckets.len(), 2);
            assert_eq!(
                prepared
                    .patch
                    .day_buckets
                    .values()
                    .map(|day| day.pending_predecessors_to_insert.len())
                    .sum::<usize>(),
                3
            );
        }

        assert_eq!(state, before_drop);
        assert_eq!(rebate_state_clone_count_for_tests(), 0);
        let before_commit_count = state.resource_record_count();

        let mut prepared = state.prepare_taker_rebate_registrations_v2();
        assert!(
            prepared
                .stage_taker_fill("commit-a", &condition_id, 30_000, decimal("2"))
                .expect("准备提交同日 taker 成交")
        );
        assert!(
            prepared
                .stage_taker_fill("commit-b", &condition_id, 40_000, decimal("3"))
                .expect("准备提交第二笔同日 taker 成交")
        );
        assert!(
            prepared
                .stage_taker_fill(
                    "commit-c",
                    &condition_id,
                    UTC_DAY_MILLIS + 2_000,
                    decimal("4"),
                )
                .expect("准备提交跨日 taker 成交")
        );
        let committable = prepared.into_committable().expect("候选可提交");
        committable.commit();

        assert_eq!(rebate_state_clone_count_for_tests(), 0);
        assert_eq!(state.resource_record_count(), before_commit_count + 8);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_taker_rebate_v2_sticky_error_blocks_commit_and_preserves_live_state() {
        let member = market('1', 0, UTC_DAY_MILLIS * 2);
        let condition_id = member.market().condition_id.0.clone();
        let mut state = state(vec![member]).await;
        let before = state.clone();

        let mut prepared = state.prepare_taker_rebate_registrations_v2();
        assert!(
            prepared
                .stage_taker_fill("sticky-a", &condition_id, 1_000, decimal("2"))
                .expect("首笔候选可准备")
        );
        let duplicate = prepared
            .stage_taker_fill("sticky-a", &condition_id, 2_000, decimal("3"))
            .expect_err("候选内重复成交必须失败");
        assert_eq!(
            duplicate,
            RebateStateErrorV1::DuplicateTrade("sticky-a".to_owned())
        );
        assert_eq!(
            prepared
                .stage_taker_fill("sticky-b", &condition_id, 3_000, decimal("4"))
                .expect_err("首次错误后不能继续准备"),
            duplicate
        );
        let Err(into_error) = prepared.into_committable() else {
            panic!("失败候选不得转换为可提交令牌")
        };
        assert_eq!(into_error, duplicate);
        assert_eq!(state, before);

        let mut prepared = state.prepare_taker_rebate_registrations_v2();
        assert!(
            prepared
                .stage_taker_fill("overflow-a", &condition_id, 1_000, decimal("2"))
                .expect("溢出前的首笔候选可准备")
        );
        let overflow = prepared
            .stage_taker_fill("overflow-b", &condition_id, i64::MAX, decimal("3"))
            .expect_err("候选日桶成熟时点溢出必须失败");
        assert_eq!(overflow, RebateStateErrorV1::TimeOverflow);
        assert_eq!(
            prepared
                .stage_taker_fill("overflow-c", &condition_id, 4_000, decimal("4"))
                .expect_err("溢出后不能继续准备"),
            overflow
        );
        let Err(into_error) = prepared.into_committable() else {
            panic!("溢出候选不得转换为可提交令牌")
        };
        assert_eq!(into_error, overflow);
        assert_eq!(state, before);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepared_taker_rebate_v2_batch_matches_v1_same_day_cross_day_and_zero_fee() {
        let member = market('1', 0, UTC_DAY_MILLIS * 3);
        let condition_id = member.market().condition_id.0.clone();
        let mut legacy = state(vec![member]).await;
        legacy
            .register_taker_fill("existing", &condition_id, 500, decimal("1"))
            .expect("建立既有同日桶");
        let mut prepared_state = legacy.clone();

        let expected_results = [
            legacy.register_taker_fill("batch-a", &condition_id, 1_500, decimal("2")),
            legacy.register_taker_fill("batch-b", &condition_id, 2_500, decimal("3")),
            legacy.register_taker_fill(
                "batch-c",
                &condition_id,
                UTC_DAY_MILLIS + 500,
                decimal("4"),
            ),
            legacy.register_taker_fill("batch-a", &condition_id, 3_500, Decimal::ZERO),
        ];

        let mut prepared = prepared_state.prepare_taker_rebate_registrations_v2();
        assert_eq!(
            prepared.stage_taker_fill("batch-a", &condition_id, 1_500, decimal("2")),
            expected_results[0]
        );
        assert_eq!(
            prepared.stage_taker_fill("batch-b", &condition_id, 2_500, decimal("3")),
            expected_results[1]
        );
        assert_eq!(
            prepared
                .stage_taker_fill("batch-c", &condition_id, UTC_DAY_MILLIS + 500, decimal("4"),),
            expected_results[2]
        );
        let patch_shape_before_zero = (
            prepared.patch.taker_fills.len(),
            prepared.patch.day_buckets.len(),
            prepared
                .patch
                .day_buckets
                .values()
                .map(|day| day.pending_predecessors_to_insert.len())
                .sum::<usize>(),
        );
        assert_eq!(
            prepared.stage_taker_fill("batch-a", &condition_id, 3_500, Decimal::ZERO),
            expected_results[3]
        );
        assert_eq!(
            (
                prepared.patch.taker_fills.len(),
                prepared.patch.day_buckets.len(),
                prepared
                    .patch
                    .day_buckets
                    .values()
                    .map(|day| day.pending_predecessors_to_insert.len())
                    .sum::<usize>(),
            ),
            patch_shape_before_zero
        );
        prepared
            .into_committable()
            .expect("批量候选可提交")
            .commit();

        assert_eq!(prepared_state, legacy);
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
