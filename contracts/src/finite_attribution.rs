//! #181 第 4 步的有限分摊原语；尚未接入 Ledger，不更换历史经济版本。
//!
//! 货币归属量子为 10^-12 pUSD。多项容量分配仍是诊断候选：用于份额时量子
//! 是 10^-12 shares，不是平台订单或真实份额精度。调用方负责固定身份顺序。
//! R 继续承担成熟 BigInt 算术，只有分配写回点舍入；模型 log/sqrt 不受影响。
//! 有限十进制总额及容量经本模块仍为有限十进制，细源精度可留在补差/边界项。
//! 这不承诺把任意旧有理状态原地变成有限状态；0/全额端点必须保留原值。
use crate::economics::EconomicRational as R;
use num_bigint::BigInt;
use num_integer::Integer;
use num_traits::Signed;
use std::fmt;

pub const FINITE_ATTRIBUTION_RULE_V1: &str = "finite_attribution_scale12_rne_v1";
pub const ATTRIBUTION_DECIMAL_PLACES_V1: u32 = 12;
const SCALE: u64 = 1_000_000_000_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FiniteSplitV1 {
    pub allocated: R,
    pub remaining: R,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FiniteAttributionErrorV1 {
    NegativeTotal,
    InvalidRatio,
    NegativeCapacity,
    TotalExceedsCapacity,
}
impl fmt::Display for FiniteAttributionErrorV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NegativeTotal => "有限分摊要求非负总量",
            Self::InvalidRatio => "有限分摊要求 0 <= part <= whole 且 whole > 0",
            Self::NegativeCapacity => "有限分摊候选要求非负容量",
            Self::TotalExceedsCapacity => "有限分摊候选总量超过给定容量之和",
        })
    }
}
impl std::error::Error for FiniteAttributionErrorV1 {}

/// 单次符号对称最近偶数舍入；不限制原金额大小，不经 f32/f64 或 Decimal 除法。
pub fn round_scale12_rne_v1(value: &R) -> R {
    let scaled = value.numerator().abs() * SCALE;
    let (mut quotient, remainder) = scaled.div_rem(value.denominator());
    let twice = &remainder * 2u8;
    if twice > *value.denominator() || (twice == *value.denominator() && quotient.is_odd()) {
        quotient += 1u8;
    }
    if value.numerator().is_negative() {
        quotient = -quotient;
    }
    R::ratio(quotient, BigInt::from(SCALE))
}

/// 前提：total >= 0, whole > 0, 0 <= part <= whole。
///
/// 只舍入 allocated，再以原 total 取差；0/全额端点保持原细精度。
/// 细源额可能让 RNE 跨过 total，此时返回精确 total 边界，而非负的补差项。
/// 此边界只保证分配合法，不是交易限额，也不裁剪调用方的源金额。
pub fn split_scale12_rne_v1(
    total: &R,
    part: &R,
    whole: &R,
) -> Result<FiniteSplitV1, FiniteAttributionErrorV1> {
    if total.numerator().is_negative() {
        return Err(FiniteAttributionErrorV1::NegativeTotal);
    }
    if !whole.is_positive() || part.numerator().is_negative() || part > whole {
        return Err(FiniteAttributionErrorV1::InvalidRatio);
    }
    let allocated = if part.is_zero() || total.is_zero() {
        R::zero()
    } else if part == whole {
        total.clone()
    } else {
        round_scale12_rne_v1(&total.mul(part).divided_by(whole))
            .max(R::zero())
            .min(total.clone())
    };
    Ok(FiniteSplitV1 {
        remaining: total.sub(&allocated),
        allocated,
    })
}

/// 尚未成为 Ledger 生产政策的多项候选；输入顺序必须由稳定身份确定。
///
/// 容量与 total 单位相同、非负，且 total <= sum(capacities)。
/// 对剩余总量/容量逐项二分，再限制在可行区间，保证每项不超过原容量、
/// 总和等于原 total；最后一个正容量项保留精确补差。零容量输出零。
/// 不适用于静默裁剪超付源事实；超过容量时返回错误，由调用合同另行处理。
pub fn allocate_capacity_scale12_candidate_v1(
    total: &R,
    capacities: &[R],
) -> Result<Vec<R>, FiniteAttributionErrorV1> {
    if total.numerator().is_negative() {
        return Err(FiniteAttributionErrorV1::NegativeTotal);
    }
    if capacities.iter().any(|v| v.numerator().is_negative()) {
        return Err(FiniteAttributionErrorV1::NegativeCapacity);
    }
    let mut capacity = capacities.iter().fold(R::zero(), |sum, v| sum.add(v));
    if total > &capacity {
        return Err(FiniteAttributionErrorV1::TotalExceedsCapacity);
    }
    let mut remaining = total.clone();
    let mut allocated = Vec::with_capacity(capacities.len());
    for current in capacities {
        if current.is_zero() {
            allocated.push(R::zero());
            continue;
        }
        let future = capacity.sub(current);
        let lower = remaining.sub(&future).max(R::zero());
        let upper = current.clone().min(remaining.clone());
        let value = split_scale12_rne_v1(&remaining, current, &capacity)?
            .allocated
            .max(lower)
            .min(upper);
        remaining = remaining.sub(&value);
        capacity = future;
        allocated.push(value);
    }
    debug_assert!(remaining.is_zero());
    Ok(allocated)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(n: i64, d: i64) -> R {
        R::ratio(n.into(), d.into())
    }

    #[test]
    fn half_even_is_signed_and_does_not_round_source_endpoints() {
        for (n, expected_units) in [(1, 0), (3, 2), (5, 2), (-1, 0), (-3, -2)] {
            assert_eq!(
                round_scale12_rne_v1(&r(n, 2_000_000_000_000)),
                r(expected_units, 1_000_000_000_000)
            );
        }
        let fine = r(10_000_000_000_007, 10_000_000_000_000);
        let full = split_scale12_rne_v1(&fine, &r(1, 1), &r(1, 1)).unwrap();
        assert_eq!(full.allocated, fine);
        assert_eq!(full.remaining, R::zero());
        let zero = split_scale12_rne_v1(&fine, &r(0, 1), &r(1, 1)).unwrap();
        assert_eq!(zero.allocated, R::zero());
        assert_eq!(zero.remaining, fine);
    }

    #[test]
    fn fine_source_round_up_cannot_create_negative_remainder() {
        // 0.6e-12 * 9/10 = 0.54e-12；直接 RNE 会得到 1e-12，超过原总额。
        let total = r(6, 10_000_000_000_000);
        let split = split_scale12_rne_v1(&total, &r(9, 1), &r(10, 1)).unwrap();
        assert_eq!(split.allocated, total);
        assert_eq!(split.remaining, R::zero());
        let thirds = split_scale12_rne_v1(&r(1, 1), &r(1, 1), &r(3, 1)).unwrap();
        assert_eq!(thirds.allocated, r(333_333_333_333, 1_000_000_000_000));
        assert_eq!(thirds.remaining, r(666_666_666_667, 1_000_000_000_000));
    }

    #[test]
    fn ordered_capacity_candidate_keeps_nonnegative_source_tail() {
        let unit = 10_000_000_000_000;
        // 三份独立 RNE 会使尾项为负；剩余量二分得到 1e-12、0、0.6e-12。
        let capacities = [r(10, unit), r(10, unit), r(10, unit)];
        let result = allocate_capacity_scale12_candidate_v1(&r(16, unit), &capacities).unwrap();
        assert_eq!(result, [r(10, unit), r(0, 1), r(6, unit)]);
        // 首项 RNE 为零，但后续容量只有 0.6e-12，必须取可行下界 0.3e-12。
        let result =
            allocate_capacity_scale12_candidate_v1(&r(9, unit), &[r(6, unit), r(6, unit)]).unwrap();
        assert_eq!(result, [r(3, unit), r(6, unit)]);
        assert_eq!(
            allocate_capacity_scale12_candidate_v1(&r(1, 1), &[r(0, 1), r(2, 1), r(0, 1)]).unwrap(),
            [r(0, 1), r(1, 1), r(0, 1)]
        );
        assert!(
            allocate_capacity_scale12_candidate_v1(&R::zero(), &[])
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            allocate_capacity_scale12_candidate_v1(&r(2, 1), &[r(1, 1)]),
            Err(FiniteAttributionErrorV1::TotalExceedsCapacity)
        );
    }

    #[test]
    fn repeated_cost_split_conserves_sources_and_meets_closed_form_error_bound() {
        const N: u32 = 3654;
        let mut cost = r(3, 2);
        let mut allocated = R::zero();
        for n in 1..=N {
            let split = split_scale12_rne_v1(&cost.add(&r(1, 250)), &r(1, 1), &r(501, 1)).unwrap();
            cost = split.remaining;
            allocated = allocated.add(&split.allocated);
            assert_eq!(cost.add(&allocated), r(3, 2).add(&r(i64::from(n), 250)));
            assert!(cost.denominator().bits() <= 40);
        }
        // 独立闭式精确解，不重跑相同分摊循环或从 helper 生成期望值。
        let exact = r(2, 1).sub(&R::ratio(
            BigInt::from(500u32).pow(N),
            BigInt::from(2u8) * BigInt::from(501u32).pow(N),
        ));
        assert_eq!(exact.denominator().bits(), 32772);
        let difference = cost.sub(&exact);
        let error = R::ratio(
            difference.numerator().abs(),
            difference.denominator().clone(),
        );
        assert!(error <= r(i64::from(N), 2_000_000_000_000));
    }
}
