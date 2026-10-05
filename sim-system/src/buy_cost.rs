//! LIMIT 最大即时成本与市价单价情景估费；不读取盘口、账户或推进运行时。

use contracts::ExactDecimal;
use num_bigint::BigUint;
use num_traits::Zero;
use rust_decimal::Decimal;

pub use crate::matching::MatchingMathErrorV1;
use crate::matching::{
    exact_less_than_power_of_two_shift, exact_taker_fee_contribution_v1, fee_price_ratio_v1,
    micro_units_v1, nonnegative_decimal_ratio, rounded_fee_units_v1,
};
use crate::platform_rules::SealedProcessRuleSnapshotV3;

/// 总成本的微单位整数为 `principal_micro + 10 * fee_1e5_units`。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExactBuyCostV1 {
    pub principal_micro: BigUint,
    pub fee_1e5_units: BigUint,
}

/// 市价候选 q、p 的单价情景估计；不是最大费、预留或实际成交费用。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketFeeEstimateV1 {
    pub fee_1e5_units: BigUint,
}

/// 原 SIM 金额撮合域的保守界；份额以微份整数表示，费用以五位金额单位表示。
/// 不使用当前盘口或 tick 作最低价格，不承诺 LIVE 或具体风险参数下存在可行金额。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimMarketBuyBoundsV1 {
    pub principal_micro: BigUint,
    pub maximum_base_micro: BigUint,
    pub maximum_fee_1e5_units: BigUint,
}

impl SealedProcessRuleSnapshotV3 {
    /// 所有原核接纳价格 0<p<1、金额目标和合法部分段；证明见买入成本文档。
    /// 每段正本金及两次 floor 误差给出 delta_base < 4*delta_quote/p。
    /// Decimal 最小正价格为 10^-28；指数>=1时费用可直接以 4*spend*rate 约束。
    pub fn sim_market_buy_bounds_v1(
        &self,
        spend_pusd: Decimal,
    ) -> Result<SimMarketBuyBoundsV1, MatchingMathErrorV1> {
        let spend = nonnegative_decimal_ratio(spend_pusd)?;
        let principal_micro = &spend.numerator * BigUint::from(1_000_000_u32) / &spend.denominator;
        let maximum_base_micro =
            &principal_micro * BigUint::from(4_u8) * BigUint::from(10_u8).pow(28);
        let schedule = &self.snapshot().synchronized_platform_rules.fee_schedule;
        let rate = nonnegative_decimal_ratio(schedule.rate.value())?;
        // 原封存规则已保证 exponent>=1（平台加载与历史校验的既有条件）。
        let fee_base = &principal_micro * BigUint::from(4_u8);
        let total = crate::matching::ExactRatioV1 {
            numerator: fee_base * rate.numerator,
            denominator: BigUint::from(1_000_000_u32) * rate.denominator,
        };
        Ok(SimMarketBuyBoundsV1 {
            principal_micro,
            maximum_base_micro,
            maximum_fee_1e5_units: rounded_fee_units_v1(&total),
        })
    }

    /// 按同一冻结费表计算 H5(q*r*[p*(1-p)]^e)，只供模型参考。
    /// 调用方沿原数量、价格和 SDK 编码域；本方法不判断支持集或改变本金。
    pub fn market_buy_fee_estimate_v1(
        &self,
        target_centishares: &BigUint,
        price_index: u16,
        tick: &ExactDecimal,
    ) -> Result<MarketFeeEstimateV1, MatchingMathErrorV1> {
        let tick: Decimal = tick
            .0
            .parse()
            .map_err(|_| MatchingMathErrorV1::InvalidPrice)?;
        let price = tick
            .checked_mul(Decimal::from(price_index))
            .ok_or(MatchingMathErrorV1::ArithmeticOverflow)?;
        Ok(MarketFeeEstimateV1 {
            fee_1e5_units: self.buy_fee_at_price(target_centishares, price)?,
        })
    }

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
        result.fee_1e5_units =
            self.buy_fee_at_price(target_centishares, price.min(Decimal::new(5, 1)))?;
        Ok(result)
    }

    fn buy_fee_at_price(
        &self,
        target_centishares: &BigUint,
        price: Decimal,
    ) -> Result<BigUint, MatchingMathErrorV1> {
        let fee = &self.snapshot().synchronized_platform_rules.fee_schedule;
        let rate = nonnegative_decimal_ratio(fee.rate.value())?;
        // q*r/4^e 严格小于半个五位单位才归零；等于时须沿原核向上舍入。
        // 依赖实际 n，不将域内统一指数证书变成人为 BigUint 数量上限。
        let left = target_centishares * &rate.numerator * BigUint::from(2_000_u32);
        if exact_less_than_power_of_two_shift(
            &left,
            &rate.denominator,
            u64::from(fee.exponent) * 2,
        )? {
            return Ok(BigUint::zero());
        }
        let base_units = target_centishares * BigUint::from(10_000_u32);
        let price = fee_price_ratio_v1(price)?;
        let total = exact_taker_fee_contribution_v1(&base_units, &price, &rate, fee.exponent)?;
        Ok(rounded_fee_units_v1(&total))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform_rules::tests::{
        snapshot_v3_with_fee_for_cost_tests, successful_snapshot_v3_for_sim_tests,
    };

    #[tokio::test(flavor = "current_thread")]
    async fn amount_bounds_keep_large_valid_rule_and_decimal_domain_exact() {
        let rules =
            snapshot_v3_with_fee_for_cost_tests("79228162514264337593543950335", u32::MAX).await;
        let bounds = rules.sim_market_buy_bounds_v1(Decimal::MAX).unwrap();
        assert!(bounds.maximum_base_micro > BigUint::from(u128::MAX));
        assert!(bounds.maximum_fee_1e5_units > BigUint::from(u128::MAX));
        assert_eq!(
            rules
                .sim_market_buy_bounds_v1(Decimal::ZERO)
                .unwrap()
                .maximum_base_micro,
            BigUint::zero()
        );
    }

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
    async fn limit_cost_keeps_micro_units_and_unaffordable_fee_exact() {
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
            crate::matching::micro_units_v1(encoded.maker_amount)
                .unwrap()
                .to_string()
        );
        assert!(
            polymarket_client_sdk_v2::clob::order_builder::encode_limit_buy_order(
                price,
                size + Decimal::new(1, 2),
                Decimal::new(1, 4),
            )
            .is_ok()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "显式导出已有超过 u128 的公共费用反例，供有限 GPU 样板对照"]
    async fn export_gpu_wide_buy_reference_v1() {
        let n: BigUint = "7922816251426433759354395".parse().unwrap();
        let rate = "79228162514264337593543950335";
        // 复用已有本地规则夹具的解析与密封入口，不访问外部平台。
        let rules = snapshot_v3_with_fee_for_cost_tests(rate, 1).await;
        let cost = rules
            .limit_buy_cost_v1(&n, 9999, &ExactDecimal("0.0001".into()), false)
            .unwrap();
        assert!(cost.fee_1e5_units > BigUint::from(u128::MAX));
        let report = serde_json::json!({
            "schema": "gpu_wide_buy_reference_v1",
            "source_test": "sim-system/src/buy_cost.rs::tests::limit_cost_keeps_micro_units_and_unaffordable_fee_exact",
            "scope": "既有极端规则费用反例；不是普通平台费率或完整合法域证明",
            "rule_snapshot_sha256": rules.file_sha256().0,
            "input": {
                "quantity_centishares": n.to_string(),
                "fee_rate": rate,
                "fee_exponent": 1,
                "tick_pusd_per_share": "0.0001",
                "price_index": 9999,
                "post_only": false,
            },
            "output": {
                "principal_micro": cost.principal_micro.to_string(),
                "fee_1e5_units": cost.fee_1e5_units.to_string(),
            },
        });
        println!("BTC_GPU_WIDE_REFERENCE_V1={report}");
    }
    #[tokio::test(flavor = "current_thread")]
    async fn market_estimate_uses_candidate_price_and_actual_tick_without_mutation() {
        let rules = successful_snapshot_v3_for_sim_tests().await;
        let before = (*rules).clone();
        // q=5, r=.07：p=.9 的情景费 .0315，不是 LIMIT 的 .0875 最大费。
        for (index, tick, expected) in [
            (90, "0.01", 3150_u32),
            (25, "0.01", 6563),
            (100, "0.0025", 6563),
        ] {
            let fee = rules
                .market_buy_fee_estimate_v1(
                    &BigUint::from(500_u32),
                    index,
                    &ExactDecimal(tick.into()),
                )
                .unwrap();
            assert_eq!(fee.fee_1e5_units, BigUint::from(expected));
        }
        assert_eq!(*rules, before);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn market_estimate_keeps_half_unit_large_exponent_and_large_integer_exact() {
        let tick = ExactDecimal("0.1".into());
        for (rate, expected) in [("0.000004", 1_u32), ("0.0000039999", 0)] {
            let rules = snapshot_v3_with_fee_for_cost_tests(rate, 1).await;
            assert_eq!(
                rules
                    .market_buy_fee_estimate_v1(&BigUint::from(500_u32), 5, &tick)
                    .unwrap()
                    .fee_1e5_units,
                BigUint::from(expected)
            );
        }
        let n: BigUint = "7922816251426433759354395".parse().unwrap();
        let rate: BigUint = "79228162514264337593543950335".parse().unwrap();
        let rules = snapshot_v3_with_fee_for_cost_tests(&rate.to_string(), 1).await;
        let fee = rules.market_buy_fee_estimate_v1(&n, 9, &tick).unwrap();
        // n/100 * r * .9*.1 * 100000 = n*r*90，无 Decimal/u128 中间量。
        assert_eq!(fee.fee_1e5_units, &n * &rate * BigUint::from(90_u8));
        assert!(fee.fee_1e5_units > BigUint::from(u128::MAX));
        for exponent in [95, u32::MAX] {
            let rules = snapshot_v3_with_fee_for_cost_tests(&rate.to_string(), exponent).await;
            assert_eq!(
                rules
                    .market_buy_fee_estimate_v1(&n, 5, &tick)
                    .unwrap()
                    .fee_1e5_units,
                BigUint::zero()
            );
        }
    }
}
