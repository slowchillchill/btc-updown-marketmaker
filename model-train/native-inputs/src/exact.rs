//! 原精确有理数求和后投影；区间端点同舍入才确定自然对数的 f32。
use contracts::{ExactDecimal, ExactRationalV1, economics::EconomicRational as R};
use num_bigint::{BigInt, BigUint};
use num_integer::Integer;
use num_traits::{One, Signed, ToPrimitive, Zero};
use rust_decimal::Decimal;

pub(crate) fn ratio(n: impl Into<BigInt>, d: impl Into<BigInt>) -> R {
    let (n, d) = (n.into(), d.into());
    let g = n.gcd(&d);
    R::try_from(&ExactRationalV1 {
        numerator: (n / &g).to_string(),
        denominator: (d / g).to_string(),
    })
    .unwrap()
}
pub(crate) fn decimal(d: &ExactDecimal) -> R {
    R::from_decimal(Decimal::from_str_exact(&d.0).expect("公共精确金额"))
}
pub(crate) fn rational(r: &ExactRationalV1) -> R {
    R::try_from(r).expect("公共规范有理数")
}
pub(crate) fn integer(n: impl Into<BigInt>) -> R {
    ratio(n, 1)
}

/// 一次最近偶数舍入，指数与余数比较全部在整数域。
pub fn round_f32(value: &R) -> f32 {
    let public = value.public();
    let n: BigInt = public.numerator.parse().unwrap();
    let d: BigUint = public.denominator.parse().unwrap();
    let sign = if n.is_negative() { 1u32 << 31 } else { 0 };
    let n = n.magnitude();
    if n.is_zero() {
        return f32::from_bits(sign);
    }
    let mut e = i128::from(n.bits()) - i128::from(d.bits());
    let lower = if e >= 0 {
        n < &(&d << usize::try_from(e).unwrap())
    } else {
        (n << usize::try_from(-e).unwrap()) < d
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
        (n.clone(), &d << usize::try_from(shift).unwrap())
    } else {
        (n << usize::try_from(-shift).unwrap(), d)
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
    let p = value.public();
    let n: BigInt = p.numerator.parse().unwrap();
    let b: BigInt = p.denominator.parse().unwrap();
    let a = n.abs() + &b;
    let mut k = a.bits() - b.bits();
    if a < (&b << usize::try_from(k).unwrap()) {
        k -= 1;
    }
    let scaled = &b << usize::try_from(k).unwrap();
    let t = ratio(&a - &scaled, &a + &scaled);
    let ln2t = ratio(1, 3);
    let mut terms = BigInt::from(8);
    loop {
        let (ml, mh) = log_interval(&t, &terms);
        let (ll, lh) = log_interval(&ln2t, &terms);
        let low = ml.add(&ll.mul(&integer(k)));
        let high = mh.add(&lh.mul(&integer(k)));
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
    let p = value.public();
    let n: BigUint = p.numerator.parse().unwrap();
    let d: BigUint = p.denominator.parse().unwrap();
    let mut bits = 32usize;
    loop {
        let scale = BigUint::one() << bits;
        let q = ((&n << (2 * bits)) / &d).sqrt();
        let low = ratio(BigInt::from(q.clone()), BigInt::from(scale.clone()));
        let high = ratio(BigInt::from(q + BigUint::one()), BigInt::from(scale));
        let (a, b) = (signed_log_f32(&low), signed_log_f32(&high));
        if a.to_bits() == b.to_bits() {
            return a;
        }
        bits *= 2;
    }
}
