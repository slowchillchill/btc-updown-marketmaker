//! #5生产域的精确十进制读取；原q=n/100且P=q*p，不引入新有效域或舍入。
use contracts::ExactDecimal;
use num_bigint::BigInt;
use num_traits::{Signed, Zero};
use rust_decimal::Decimal;
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Quantity {
    coefficient: BigInt,
    scale: u32,
}
impl Quantity {
    pub(crate) fn zero() -> Self {
        Self {
            coefficient: BigInt::zero(),
            scale: 0,
        }
    }
    pub(crate) fn parse(value: &ExactDecimal) -> Self {
        let (whole, fraction) = value.0.split_once('.').unwrap_or((&value.0, ""));
        Self {
            coefficient: format!("{whole}{fraction}")
                .parse()
                .expect("规范化有限十进制"),
            scale: fraction.len() as u32,
        }
    }
    pub(crate) fn add(&mut self, other: &Self) {
        let scale = self.scale.max(other.scale);
        self.coefficient = &self.coefficient * ten(scale - self.scale)
            + &other.coefficient * ten(scale - other.scale);
        self.scale = scale;
    }
    pub(crate) fn subtract(&mut self, other: &Self) {
        let negative = Self {
            coefficient: -other.coefficient.clone(),
            scale: other.scale,
        };
        self.add(&negative);
    }
    pub(crate) fn is_negative(&self) -> bool {
        self.coefficient.is_negative()
    }
    pub(crate) fn equal(&self, other: &Self) -> bool {
        let scale = self.scale.max(other.scale);
        &self.coefficient * ten(scale - self.scale) == &other.coefficient * ten(scale - other.scale)
    }
    pub(crate) fn exact(&self) -> ExactDecimal {
        if self.coefficient.is_zero() {
            return ExactDecimal("0".into());
        }
        let negative = self.coefficient.is_negative();
        let mut digits = self.coefficient.abs().to_string();
        if self.scale > 0 {
            let needed = self.scale as usize + 1;
            if digits.len() < needed {
                digits = format!("{}{}", "0".repeat(needed - digits.len()), digits)
            }
            digits.insert(digits.len() - self.scale as usize, '.');
            while digits.ends_with('0') {
                digits.pop();
            }
            if digits.ends_with('.') {
                digits.pop();
            }
        }
        ExactDecimal(if negative {
            format!("-{digits}")
        } else {
            digits
        })
    }
}
fn ten(power: u32) -> BigInt {
    BigInt::from(10u8).pow(power)
}
/// 输入沿原模型P=q*p生产合同；不收窄旧Bridge任意字段映射接口。
pub(crate) fn original_target(spend: Decimal, price: Decimal) -> ExactDecimal {
    // q=n/100，直接恢复原整数n。这里不是Decimal除法，也不是乘回舍入值。
    let numerator = BigInt::from(spend.mantissa()) * ten(price.scale() + 2);
    let denominator = BigInt::from(price.mantissa()) * ten(spend.scale());
    Quantity {
        coefficient: numerator / denominator,
        scale: 2,
    }
    .exact()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn production_target_and_sum_do_not_round_decimal_coefficients() {
        assert_eq!(
            original_target("7.95".parse().unwrap(), "0.53".parse().unwrap()).0,
            "15"
        );
        let mut sum = Quantity::parse(&ExactDecimal("79228162514264337593543950335".into()));
        sum.add(&Quantity::parse(&ExactDecimal("0.01".into())));
        assert_eq!(sum.exact().0, "79228162514264337593543950335.01");
    }
}
