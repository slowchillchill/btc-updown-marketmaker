//! 原 LIMIT 编码本金与最大即时费用；不读取盘口、账户或推进运行时。

use contracts::ExactDecimal;
use num_bigint::BigUint;
use num_traits::Zero;
use rust_decimal::Decimal;

pub use crate::matching::MatchingMathErrorV1;
use crate::matching::{
    exact_less_than_power_of_two_shift, exact_taker_fee_contribution_v1, micro_units_v1,
    nonnegative_decimal_ratio, rounded_fee_units_v1,
};
use crate::platform_rules::SealedProcessRuleSnapshotV3;

/// 总成本的微单位整数为 `principal_micro + 10 * fee_1e5_units`。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExactBuyCostV1 {
    pub principal_micro: BigUint,
    pub fee_1e5_units: BigUint,
}

impl SealedProcessRuleSnapshotV3 {
    /// #6 正式续篇 G／I：GTC 与 GTD 共用的单笔 LIMIT 最大准入成本。
    ///
    /// 调用方保持原 SDK 编码域、六档 tick、数量与价格轴及非资金资格；
    /// 本方法不判断到期或账户可负担性，也不改变这些支持条件。
    pub fn limit_buy_cost_v1(
        &self,
        target_centishares: &BigUint,
        price_index: u16,
        tick: &ExactDecimal,
        post_only: bool,
    ) -> Result<ExactBuyCostV1, MatchingMathErrorV1> {
        let tick: Decimal = tick
            .0
            .parse()
            .map_err(|_| MatchingMathErrorV1::InvalidPrice)?;
        let price = tick
            .checked_mul(Decimal::from(price_index))
            .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?;
        let principal_micro =
            target_centishares * BigUint::from(micro_units_v1(price)?) / BigUint::from(100_u8);
        let snapshot = self.snapshot();
        // 合法 V3 快照已保证 GTC、GTD 的即时最低本金相同，均为 1。
        let minimum_micro = micro_units_v1(
            snapshot
                .documented_platform_rules
                .immediate_gtc_buy_minimum
                .value
                .minimum_principal_pusd
                .value(),
        )?;
        let mut result = ExactBuyCostV1 {
            principal_micro,
            fee_1e5_units: BigUint::zero(),
        };
        if post_only || result.principal_micro < BigUint::from(minimum_micro) {
            return Ok(result);
        }
        let fee = &snapshot.synchronized_platform_rules.fee_schedule;
        let rate = nonnegative_decimal_ratio(fee.rate.value())?;
        // I.3 的严格半单位判据依赖实际 n；不把 I.5 的域内 e≥95
        // 证书误用为任意 BigUint 输入的新上限或无条件零费分支。
        let left = target_centishares * &rate.numerator * BigUint::from(2_000_u32);
        if exact_less_than_power_of_two_shift(
            &left,
            &rate.denominator,
            u64::from(fee.exponent) * 2,
        )? {
            return Ok(result);
        }
        let base_units = target_centishares * BigUint::from(10_000_u32);
        let total = exact_taker_fee_contribution_v1(
            &base_units,
            price.min(Decimal::new(5, 1)),
            &rate,
            fee.exponent,
        )?;
        result.fee_1e5_units = rounded_fee_units_v1(&total);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform_rules::tests::{
        snapshot_v3_with_fee_for_cost_tests, successful_snapshot_v3_for_sim_tests,
    };

    #[tokio::test(flavor = "current_thread")]
    async fn limit_cost_branches_round_each_order_and_preserve_snapshot() {
        let rules = successful_snapshot_v3_for_sim_tests().await;
        let before = (*rules).clone();
        let n = BigUint::from(500_u32);
        for (index, post_only, principal, fee) in [
            (25, true, 1_250_000, 0),
            (10, false, 500_000, 0),
            (20, false, 1_000_000, 5_600),
            (25, false, 1_250_000, 6_563),
            (50, false, 2_500_000, 8_750),
            (90, false, 4_500_000, 8_750),
        ] {
            let cost = rules
                .limit_buy_cost_v1(&n, index, &ExactDecimal("0.01".into()), post_only)
                .unwrap();
            assert_eq!(cost.principal_micro, BigUint::from(principal as u32));
            assert_eq!(cost.fee_1e5_units, BigUint::from(fee as u32));
        }
        let a = rules
            .limit_buy_cost_v1(&n, 25, &ExactDecimal("0.01".into()), false)
            .unwrap();
        let b = rules
            .limit_buy_cost_v1(&n, 100, &ExactDecimal("0.0025".into()), false)
            .unwrap();
        assert_eq!(a, b, "价格索引须乘实际 tick");
        assert_eq!(a.fee_1e5_units + b.fee_1e5_units, BigUint::from(13_126_u32));
        assert_eq!(*rules, before);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn limit_cost_strict_half_unit_and_large_exponent() {
        let n = BigUint::from(500_u32);
        let tick = ExactDecimal("0.1".into());
        for (rate, expected) in [("0.000004", 1_u32), ("0.0000039999", 0)] {
            let rules = snapshot_v3_with_fee_for_cost_tests(rate, 1).await;
            let result = rules.limit_buy_cost_v1(&n, 5, &tick, false).unwrap();
            assert_eq!(result.fee_1e5_units, BigUint::from(expected));
        }
        let n: BigUint = "7922816251426433759354395".parse().unwrap();
        let rate = "79228162514264337593543950335";
        for exponent in [95, u32::MAX] {
            let rules = snapshot_v3_with_fee_for_cost_tests(rate, exponent).await;
            assert_eq!(
                rules
                    .limit_buy_cost_v1(&n, 5, &tick, false)
                    .unwrap()
                    .fee_1e5_units,
                BigUint::zero()
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn limit_cost_keeps_encoding_boundary_and_unaffordable_fee_exact() {
        let n: BigUint = "7922816251426433759354395".parse().unwrap();
        let d: BigUint = "79228162514264337593543950335".parse().unwrap();
        let rules = snapshot_v3_with_fee_for_cost_tests(&d.to_string(), 1).await;
        let cost = rules
            .limit_buy_cost_v1(&n, 9999, &ExactDecimal("0.0001".into()), false)
            .unwrap();
        assert_eq!(cost.principal_micro, &n * BigUint::from(9999_u32));
        // q*r/4*10^5 = n*r*250，已为整数，远超 Decimal 与 u128。
        assert_eq!(cost.fee_1e5_units, &n * &d * BigUint::from(250_u32));
        assert!(cost.fee_1e5_units > BigUint::from(u128::MAX));
        let price = Decimal::new(9999, 4);
        let size: Decimal = "79228162514264337593543.95".parse().unwrap();
        let encoded = polymarket_client_sdk_v2::clob::order_builder::encode_limit_buy_order(
            price,
            size,
            Decimal::new(1, 4),
        )
        .unwrap();
        assert_eq!(
            cost.principal_micro.to_string(),
            encoded.maker_amount.mantissa().to_string()
        );
        assert!(
            polymarket_client_sdk_v2::clob::order_builder::encode_limit_buy_order(
                price,
                size + Decimal::new(1, 2),
                Decimal::new(1, 4),
            )
            .is_err()
        );
    }
}
