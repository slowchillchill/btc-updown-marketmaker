//! 原精确有理数求和后投影；区间端点同舍入才确定自然对数的 f32。
use contracts::{ExactDecimal, ExactRationalV1, economics::EconomicRational as R};
use num_bigint::{BigInt, BigUint};
use num_integer::Integer;
use num_traits::{One, Signed, ToPrimitive, Zero};
use rust_decimal::Decimal;
use std::sync::OnceLock;

type SharedProjection = std::sync::Arc<OnceLock<f32>>;

/// 一次原生组构造内共享纯投影；精确毫秒差和规范有理数分别作为键。
/// 不保存账户/市场业务状态；字段的单位、缺失位和来源时点仍由调用方保留。
#[derive(Default)]
pub(crate) struct InputProjection {
    times: std::sync::Mutex<std::collections::HashMap<i128, SharedProjection>>,
    values: std::sync::Mutex<std::collections::HashMap<(BigInt, BigInt), SharedProjection>>,
}
impl InputProjection {
    pub(crate) fn project(&self, milliseconds: i128) -> f32 {
        let value = self
            .times
            .lock()
            .unwrap()
            .entry(milliseconds)
            .or_default()
            .clone();
        // 只同步同键的首次计算；不同精确值仍可并行，不持有整张表的锁计算。
        *value.get_or_init(|| signed_log_f32(&ratio(milliseconds, 1000)))
    }
    pub(crate) fn log(&self, exact: &R) -> f32 {
        if exact.is_zero() {
            return 0.;
        }
        let key = (exact.numerator().clone(), exact.denominator().clone());
        let value = self.values.lock().unwrap().entry(key).or_default().clone();
        *value.get_or_init(|| signed_log_f32(exact))
    }
}

pub(crate) fn ratio(n: impl Into<BigInt>, d: impl Into<BigInt>) -> R {
    R::ratio(n.into(), d.into())
}
pub(crate) fn decimal(d: &ExactDecimal) -> R {
    R::from_decimal(Decimal::from_str_exact(&d.0).expect("公共精确金额"))
}
pub(crate) fn rational(r: &ExactRationalV1) -> R {
    R::try_from(r).expect("公共整数比")
}
pub(crate) fn integer(n: impl Into<BigInt>) -> R {
    ratio(n, 1)
}

/// 一次最近偶数舍入，指数与余数比较全部在整数域。
pub fn round_f32(value: &R) -> f32 {
    let n = value.numerator();
    let d = value.denominator().magnitude();
    let sign = if n.is_negative() { 1u32 << 31 } else { 0 };
    let n = n.magnitude();
    if n.is_zero() {
        return f32::from_bits(sign);
    }
    let mut e = i128::from(n.bits()) - i128::from(d.bits());
    let lower = if e >= 0 {
        n < &(d << usize::try_from(e).unwrap())
    } else {
        &(n << usize::try_from(-e).unwrap()) < d
    };
    if lower {
        e -= 1;
    }
    if e > 127 {
        return f32::from_bits(sign | 0x7f800000);
    }
    if e < -150 {
        return f32::from_bits(sign);
    }
    let shift = (e - 23).max(-149);
    let (a, b) = if shift >= 0 {
        (n.clone(), d << usize::try_from(shift).unwrap())
    } else {
        (n << usize::try_from(-shift).unwrap(), d.clone())
    };
    let (mut q, r) = a.div_rem(&b);
    if (&r << 1) > b || ((&r << 1) == b && q.is_odd()) {
        q += BigUint::one();
    }
    let mut q = q.to_u32().unwrap();
    if q == 0 {
        return f32::from_bits(sign);
    }
    if shift == -149 && q < (1 << 23) {
        return f32::from_bits(sign | q);
    }
    let mut exponent = shift + 23;
    if q == (1 << 24) {
        q >>= 1;
        exponent += 1;
    }
    if exponent > 127 {
        return f32::from_bits(sign | 0x7f800000);
    }
    f32::from_bits(sign | ((exponent + 127) as u32) << 23 | (q - (1 << 23)))
}
fn log_interval(t: &R, terms: &BigInt) -> (R, R) {
    let square = t.mul(t);
    let mut power = t.clone();
    let mut sum = R::zero();
    let mut j = BigInt::zero();
    while &j < terms {
        sum = sum.add(&power.divided_by(&integer(2 * &j + 1)));
        power = power.mul(&square);
        j += 1;
    }
    let lower = sum.mul(&integer(2));
    let remainder = power
        .mul(&integer(2))
        .divided_by(&integer(2 * terms + 1).mul(&integer(1).sub(&square)));
    (lower.clone(), lower.add(&remainder))
}
pub fn signed_log_f32(value: &R) -> f32 {
    if value.is_zero() {
        return 0.0;
    }
    let n = value.numerator();
    let b = value.denominator();
    let a = n.abs() + b;
    let mut k = a.bits() - b.bits();
    if a < (b << usize::try_from(k).unwrap()) {
        k -= 1;
    }
    let scaled = b << usize::try_from(k).unwrap();
    let t = ratio(&a - &scaled, &a + &scaled);
    let mut terms = BigInt::from(8);
    loop {
        let (ml, mh) = log_interval(&t, &terms);
        // 相同有理区间只初始化一次；精度加倍仍走原算法，不改近似或舍入规则。
        static LN2_INITIAL: OnceLock<(R, R)> = OnceLock::new();
        let (low, high) = if k == 0 {
            (ml, mh)
        } else {
            let refined;
            let (ll, lh) = if terms == BigInt::from(8) {
                LN2_INITIAL.get_or_init(|| log_interval(&ratio(1, 3), &BigInt::from(8)))
            } else {
                refined = log_interval(&ratio(1, 3), &terms);
                &refined
            };
            (ml.add(&ll.mul(&integer(k))), mh.add(&lh.mul(&integer(k))))
        };
        let (low, high) = (round_f32(&low), round_f32(&high));
        if low.to_bits() == high.to_bits() {
            return if n.is_negative() { -low } else { low };
        }
        terms *= 2;
    }
}

/// 非负精确平方均值的 sqrt/log1p 投影；有理区间端点同舍入才返回。
pub(crate) fn sqrt_log_f32(value: &R) -> f32 {
    if value.is_zero() {
        return 0.;
    }
    let n = value.numerator().magnitude();
    let d = value.denominator().magnitude();
    let mut bits = 32usize;
    loop {
        let scale = BigUint::one() << bits;
        let q = ((n << (2 * bits)) / d).sqrt();
        let low = ratio(BigInt::from(q.clone()), BigInt::from(scale.clone()));
        let high = ratio(BigInt::from(q + BigUint::one()), BigInt::from(scale));
        let (a, b) = (signed_log_f32(&low), signed_log_f32(&high));
        if a.to_bits() == b.to_bits() {
            return a;
        }
        bits *= 2;
    }
}

#[cfg(test)]
mod projection_tests {
    use super::*;
    use rayon::prelude::*;

    #[test]
    fn shared_time_projection_preserves_signed_wide_differences_and_original_bits() {
        let projection = InputProjection::default();
        // 同低64位但不同精确差值不能串用；负来源年龄不能截为零。
        let differences = [
            0,
            1,
            -1,
            17,
            -17,
            999,
            1000,
            1001,
            i128::from(u64::MAX),
            i128::from(u64::MAX) + 2,
            -i128::from(u64::MAX),
        ];
        let expected: Vec<_> = differences
            .iter()
            .map(|&d| signed_log_f32(&ratio(d, 1000)).to_bits())
            .collect();
        for _ in 0..3 {
            let actual: Vec<_> = differences
                .par_iter()
                .map(|&d| projection.project(d).to_bits())
                .collect();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn shared_value_projection_keeps_denominators_signs_and_nonterminating_values() {
        let projection = InputProjection::default();
        let values = [
            ratio(0, 1),
            ratio(1, 2),
            ratio(2, 4),
            ratio(-1, 2),
            ratio(1, 3),
            ratio(25001, 50003),
            ratio(1000, 1),
        ];
        let expected: Vec<_> = values.iter().map(|v| signed_log_f32(v).to_bits()).collect();
        for _ in 0..3 {
            let actual: Vec<_> = values
                .par_iter()
                .map(|v| projection.log(v).to_bits())
                .collect();
            assert_eq!(actual, expected);
        }
    }
}
