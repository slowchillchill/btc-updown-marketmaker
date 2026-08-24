use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::matching::{MatchingMathErrorV1, taker_fee_pusd_v1};

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
pub(crate) enum RebateStateErrorV1 {
    UnknownMarket(String),
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
pub(crate) struct RebateDayFinalStateV1 {
    pub(crate) payable_pusd: Decimal,
    pub(crate) locked_remainder_pusd: Decimal,
    pub(crate) paid: bool,
}

pub(crate) fn maker_rebate_contribution_v1(
    trade_id: &str,
    matched_at_utc_ms: i64,
    base_units: u128,
    price_pusd_per_share: Decimal,
    fee_rate: Decimal,
    fee_exponent: u32,
    maker_rebate_rate: Decimal,
) -> Result<MakerRebateContributionV1, RebateStateErrorV1> {
    let attribution_day_utc_ms = utc_day_start(matched_at_utc_ms)?;
    let fee_equivalent_pusd = taker_fee_pusd_v1(
        &[(base_units, price_pusd_per_share)],
        fee_rate,
        fee_exponent,
    )?;
    let contribution_pusd = fee_equivalent_pusd
        .checked_mul(maker_rebate_rate)
        .ok_or(RebateStateErrorV1::ArithmeticOverflow)?
        .normalize();
    Ok(MakerRebateContributionV1 {
        trade_id: trade_id.to_owned(),
        matched_at_utc_ms,
        attribution_day_utc_ms,
        base_units,
        price_pusd_per_share,
        fee_equivalent_pusd,
        rebate_rate: maker_rebate_rate,
        contribution_pusd,
    })
}

pub(crate) fn finalized_day_state(accrued_pusd: Decimal) -> RebateDayFinalStateV1 {
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

pub(crate) fn utc_day_start(timestamp_utc_ms: i64) -> Result<i64, RebateStateErrorV1> {
    timestamp_utc_ms
        .div_euclid(UTC_DAY_MILLIS)
        .checked_mul(UTC_DAY_MILLIS)
        .ok_or(RebateStateErrorV1::TimeOverflow)
}

pub(crate) fn rebate_maturity_at(
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
