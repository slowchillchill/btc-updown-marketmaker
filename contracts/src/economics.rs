//! #106 的精确经济归属。实际账户金额仍沿 ExactDecimal，不用分数重定义订单数量。
use num_bigint::BigInt;
use num_integer::Integer;
use num_traits::{One, Signed, Zero};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt, str::FromStr};

/// 唯一公共有理叶子；规范形式分母为正、互素、零为 0/1。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExactRationalV1 {
    pub numerator: String,
    pub denominator: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RationalEncodingErrorV1;
impl fmt::Display for RationalEncodingErrorV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("精确经济分数不是规范整数比")
    }
}
impl std::error::Error for RationalEncodingErrorV1 {}

/// 内部计算沿成熟 BigInt 整数运算；序列化只使用上述公共叶子。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EconomicRational {
    numerator: BigInt,
    denominator: BigInt,
}
impl Default for EconomicRational {
    fn default() -> Self {
        Self::zero()
    }
}
impl EconomicRational {
    pub fn zero() -> Self {
        Self {
            numerator: BigInt::zero(),
            denominator: BigInt::one(),
        }
    }
    pub fn from_decimal(value: Decimal) -> Self {
        Self::ratio(
            BigInt::from(value.mantissa()),
            BigInt::from(10u8).pow(value.scale()),
        )
    }
    fn ratio(mut numerator: BigInt, mut denominator: BigInt) -> Self {
        assert!(!denominator.is_zero(), "内部有理除法要求已知非零分母");
        if denominator.is_negative() {
            numerator = -numerator;
            denominator = -denominator;
        }
        let gcd = numerator.gcd(&denominator);
        Self {
            numerator: numerator / &gcd,
            denominator: denominator / gcd,
        }
    }
    pub fn is_zero(&self) -> bool {
        self.numerator.is_zero()
    }
    pub fn is_positive(&self) -> bool {
        self.numerator.is_positive()
    }
    pub fn add(&self, rhs: &Self) -> Self {
        Self::ratio(
            &self.numerator * &rhs.denominator + &rhs.numerator * &self.denominator,
            &self.denominator * &rhs.denominator,
        )
    }
    pub fn sub(&self, rhs: &Self) -> Self {
        Self::ratio(
            &self.numerator * &rhs.denominator - &rhs.numerator * &self.denominator,
            &self.denominator * &rhs.denominator,
        )
    }
    pub fn mul(&self, rhs: &Self) -> Self {
        Self::ratio(
            &self.numerator * &rhs.numerator,
            &self.denominator * &rhs.denominator,
        )
    }
    pub fn divided_by(&self, rhs: &Self) -> Self {
        Self::ratio(
            &self.numerator * &rhs.denominator,
            &self.denominator * &rhs.numerator,
        )
    }
    pub fn public(&self) -> ExactRationalV1 {
        ExactRationalV1 {
            numerator: self.numerator.to_string(),
            denominator: self.denominator.to_string(),
        }
    }
}
impl Ord for EconomicRational {
    fn cmp(&self, rhs: &Self) -> Ordering {
        (&self.numerator * &rhs.denominator).cmp(&(&rhs.numerator * &self.denominator))
    }
}
impl PartialOrd for EconomicRational {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        Some(self.cmp(rhs))
    }
}
impl TryFrom<&ExactRationalV1> for EconomicRational {
    type Error = RationalEncodingErrorV1;
    fn try_from(value: &ExactRationalV1) -> Result<Self, Self::Error> {
        let n = BigInt::from_str(&value.numerator).map_err(|_| RationalEncodingErrorV1)?;
        let d = BigInt::from_str(&value.denominator).map_err(|_| RationalEncodingErrorV1)?;
        if !d.is_positive() {
            return Err(RationalEncodingErrorV1);
        }
        let result = Self::ratio(n, d);
        if result.public() != *value {
            return Err(RationalEncodingErrorV1);
        }
        Ok(result)
    }
}
impl Serialize for EconomicRational {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.public().serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for EconomicRational {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = ExactRationalV1::deserialize(deserializer)?;
        Self::try_from(&value).map_err(serde::de::Error::custom)
    }
}

use crate::ExecutionReadValueV1;
use crate::execution_normalization::SourceNeutralTradeLifecycleResultV2;
use crate::{AccountInstanceKeyV1, ConditionId, ExactDecimal, MarketResolutionTerminalV3};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralTradeLifecycleResultV3<I> {
    pub result: SourceNeutralTradeLifecycleResultV2<I>,
    pub maker_fee_equivalent_pusd: ExecutionReadValueV1<ExactDecimal>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RebateKindV1 {
    Maker,
    Taker,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettlementCostAttributionV1 {
    Maker,
    Directional,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementRequestIdV1<I> {
    pub account: AccountInstanceKeyV1,
    pub condition_id: ConditionId,
    pub resolution_identity: I,
    pub policy_version: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebateContributionFactV1<I> {
    pub trade_identity: I,
    pub condition_id: ConditionId,
    pub kind: RebateKindV1,
    pub attribution_day_start_utc_ms: i64,
    pub contribution_pusd: ExactDecimal,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebateDayResultV1<I> {
    pub kind: RebateKindV1,
    pub attribution_day_start_utc_ms: i64,
    pub accrued_pusd: ExactDecimal,
    pub payable_pusd: ExactDecimal,
    pub locked_remainder_pusd: ExactDecimal,
    pub contribution_refs: Vec<I>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebatePaymentFactV1<I> {
    pub kind: RebateKindV1,
    pub attribution_day_start_utc_ms: i64,
    pub day_result_identity: I,
    pub paid_pusd: ExactDecimal,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedPositionFactV1<I> {
    pub condition_id: ConditionId,
    pub resolution_identity: I,
    pub payout_vector: MarketResolutionTerminalV3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettlementRequestOutcomeV1 {
    Registered,
    Pending,
    FinalSuccess,
    FinalFailureNoEffect,
}
/// 原请求的真实有限后端观察；超时、查询与受控重试不等同经济终局。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettlementObservationV1 {
    Registered,
    SubmissionUnknown,
    ObservationTimeout,
    QueryUnknown,
    QueryAcceptedPending,
    QueryFinalSuccess,
    QueryConfirmedNoTransaction,
    RetryAuthorized,
    RetryNotAuthorized,
    TerminalSuccess,
    TerminalFailureNoEffect,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementRequestFactV1<I> {
    pub index_sets: [alloy_primitives::U256; 2],
    pub settlement_request_id: SettlementRequestIdV1<I>,
    pub outcome: SettlementRequestOutcomeV1,
    pub observation: SettlementObservationV1,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementCostFactV1<I> {
    pub settlement_request_id: SettlementRequestIdV1<I>,
    pub cost_identity: I,
    pub recognized_pusd: ExactDecimal,
    pub paid_pusd: ExactDecimal,
    pub correction_of: Option<I>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralSettlementLifecycleResultV2<I> {
    MakerAccrualFinalized {
        condition_id: ConditionId,
        status: crate::execution_normalization::SourceNeutralMakerRebateAccrualStatusV1,
        contributions: Vec<RebateContributionFactV1<I>>,
    },
    Contribution(RebateContributionFactV1<I>),
    DayFinalized(RebateDayResultV1<I>),
    Payment(RebatePaymentFactV1<I>),
    Resolved(ResolvedPositionFactV1<I>),
    Request(SettlementRequestFactV1<I>),
    Cost(SettlementCostFactV1<I>),
    ExternalCash(ExternalCashFactV1<I>),
}

use crate::execution_normalization::{
    NoeEconomicFactReferenceV1, SimV2SourceReferenceV1, SourceNeutralAccountBalanceDeltaV1,
    SourceNeutralAccountTransitionKindV1, SourceNeutralEconomicDispositionV1,
    SourceNeutralEconomicFactV1, SourceNeutralEconomicIdentityV1, StableLogicalIdentityV1,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExternalCashAttributionV1 {
    Deposit,
    ManualAdjustment,
    UnknownActivity,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalCashFactV1<I> {
    pub identity: I,
    pub signed_delta_pusd: ExactDecimal,
    pub attribution: ExternalCashAttributionV1,
    pub correction_of: Option<I>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralAccountBalanceDeltaV2 {
    pub existing: SourceNeutralAccountBalanceDeltaV1,
    pub settlement_fee_liability_pusd_delta: ExactDecimal,
    pub actual_taker_fee_liability_pusd_delta: ExactDecimal,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralAccountTransitionKindV2<I> {
    Existing(SourceNeutralAccountTransitionKindV1<I>),
    ResolutionRedeemApplied {
        resolution_identity: I,
        settlement_request_id: SettlementRequestIdV1<I>,
    },
    SettlementCostRecognized {
        settlement_request_id: SettlementRequestIdV1<I>,
        cost_identity: I,
    },
    SettlementCostPaid {
        settlement_request_id: SettlementRequestIdV1<I>,
        cost_identity: I,
    },
    ExternalCashApplied {
        cash_identity: I,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralEconomicDispositionV2<I> {
    AccountOpening {
        available_pusd: ExactDecimal,
    },
    AccountTransition {
        economic_effective_at_utc_ms: i64,
        condition_id: Option<ConditionId>,
        transition: SourceNeutralAccountTransitionKindV2<I>,
        balance_delta: Box<SourceNeutralAccountBalanceDeltaV2>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralEconomicFactV2<I = StableLogicalIdentityV1, F = NoeEconomicFactReferenceV1>
{
    pub fact_reference: F,
    pub logical_identity: I,
    pub economic_identity: SourceNeutralEconomicIdentityV1,
    pub source_reference: SimV2SourceReferenceV1,
    pub disposition: SourceNeutralEconomicDispositionV2<I>,
}
impl<I> From<SourceNeutralEconomicDispositionV1<I>> for SourceNeutralEconomicDispositionV2<I> {
    fn from(value: SourceNeutralEconomicDispositionV1<I>) -> Self {
        match value {
            SourceNeutralEconomicDispositionV1::AccountOpening { available_pusd } => {
                Self::AccountOpening { available_pusd }
            }
            SourceNeutralEconomicDispositionV1::AccountTransition {
                economic_effective_at_utc_ms,
                condition_id,
                transition,
                balance_delta,
            } => Self::AccountTransition {
                economic_effective_at_utc_ms,
                condition_id,
                transition: SourceNeutralAccountTransitionKindV2::Existing(transition),
                balance_delta: Box::new(SourceNeutralAccountBalanceDeltaV2 {
                    existing: *balance_delta,
                    settlement_fee_liability_pusd_delta: ExactDecimal("0".into()),
                    actual_taker_fee_liability_pusd_delta: ExactDecimal("0".into()),
                }),
            },
        }
    }
}
impl<I, F> From<SourceNeutralEconomicFactV1<I, F>> for SourceNeutralEconomicFactV2<I, F> {
    fn from(value: SourceNeutralEconomicFactV1<I, F>) -> Self {
        Self {
            fact_reference: value.fact_reference,
            logical_identity: value.logical_identity,
            economic_identity: value.economic_identity,
            source_reference: value.source_reference,
            disposition: value.disposition.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralInitialMarketV1 {
    pub market: crate::MarketIdentityV2,
    pub assets: [crate::AssetIdentityV1; 2],
}

/// RESOLUTION_REDEEM_ONLY 的原二元请求；不携带可替换份额、赢家或金额。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementRequestV1 {
    pub identity: SettlementRequestIdV1<String>,
    pub index_sets: [alloy_primitives::U256; 2],
}
/// 模拟赎回后端只生成请求结果和费用事实；其他结算事实仍走原生产入口。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestedSettlementResultV1 {
    Request(SettlementRequestFactV1<String>),
    Cost(SettlementCostFactV1<String>),
}
