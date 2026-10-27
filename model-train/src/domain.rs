//! SDK Decimal 完整份额域的唯一排名。排名只是精确编码，不是数量网格。
use contracts::ExactDecimal;
use num_bigint::BigUint;
use num_traits::{One, ToPrimitive, Zero};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sim_system::{buy_cost::ExactBuyCostV1, platform_rules::SealedProcessRuleSnapshotV3};

pub fn coefficient_max() -> BigUint {
    (BigUint::one() << 96usize) - BigUint::one()
}
pub fn quantity_count() -> BigUint {
    let m = coefficient_max();
    &m + (&m - &m / 10u8) * 2u8
}
/// 0基排名依次覆盖 centishares 1..M、(M,10M]的10倍数、(10M,100M]的100倍数。
pub fn rank_to_centishares(rank: &BigUint) -> Result<BigUint, String> {
    let m = coefficient_max();
    let high = &m - &m / 10u8;
    if rank < &m {
        Ok(rank + 1u8)
    } else if rank < &(&m + &high) {
        Ok((rank - &m + &m / 10u8 + 1u8) * 10u8)
    } else if rank < &quantity_count() {
        Ok((rank - &m - &high + &m / 10u8 + 1u8) * 100u8)
    } else {
        Err("份额排名超出SDK Decimal域".into())
    }
}
pub fn scaled_string(value: &BigUint, scale: usize) -> String {
    let mut digits = value.to_string();
    if digits.len() <= scale {
        digits = format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits);
    }
    if scale > 0 {
        digits.insert(digits.len() - scale, '.');
    }
    digits
}
pub fn shares(n: &BigUint) -> Result<Decimal, String> {
    // 三段分别构造系数，避免解析器把大数的小数尾部舍入。
    let m = coefficient_max();
    let (coefficient, scale) = if n <= &m {
        (n.clone(), 2)
    } else if n <= &(&m * 10u8) && (n % 10u8).is_zero() {
        (n / 10u8, 1)
    } else if n <= &(&m * 100u8) && (n % 100u8).is_zero() {
        (n / 100u8, 0)
    } else {
        return Err("份额不能由SDK Decimal精确表达".into());
    };
    let words = coefficient.to_u128().ok_or("份额系数溢出")?;
    Ok(Decimal::from_parts(
        words as u32,
        (words >> 32) as u32,
        (words >> 64) as u32,
        false,
        scale,
    ))
}
pub fn cash_micro(cash: Decimal) -> BigUint {
    let mantissa = BigUint::from(cash.mantissa().unsigned_abs());
    if cash.is_sign_negative() {
        BigUint::zero()
    } else if cash.scale() <= 6 {
        mantissa * BigUint::from(10u8).pow(6 - cash.scale())
    } else {
        mantissa / BigUint::from(10u8).pow(cash.scale() - 6)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitLeg {
    pub price_index: u16,
    pub tick: ExactDecimal,
    pub post_only: bool,
}
/// 公开tick的原SDK索引边界；生产与有限公共材料重建共用。
pub(crate) fn market_limit_bounds(
    market: &shared_run::FrozenMarketReadV1,
) -> Result<([ExactDecimal; 2], [u16; 2]), String> {
    let mut ticks = [ExactDecimal("0".into()), ExactDecimal("0".into())];
    let mut maximum = [0u16; 2];
    if market.observation.assets.len() == 2 {
        for side in 0..2 {
            if let Some(tick) = &market.observation.assets[side].tick.value {
                let value = Decimal::from_str_exact(&tick.0).map_err(|e| e.to_string())?;
                if value > Decimal::ZERO {
                    ticks[side] = tick.clone();
                    maximum[side] =
                        u16::try_from(((Decimal::ONE - value) / value).floor().mantissa())
                            .map_err(|_| "tick超出原SDK六档索引")?;
                }
            }
        }
    }
    Ok((ticks, maximum))
}
pub fn costs(
    rules: &SealedProcessRuleSnapshotV3,
    n: &BigUint,
    legs: &[LimitLeg],
) -> Result<Vec<ExactBuyCostV1>, String> {
    legs.iter()
        .map(|l| {
            rules
                .limit_buy_cost_v1(n, l.price_index, &l.tick, l.post_only)
                .map_err(|e| format!("公共LIMIT成本: {e:?}"))
        })
        .collect()
}
/// 公共逐腿成本单调，按完整可表示域二分最大可负担排名；两腿分别舍入再相加。
pub fn affordable_count(
    rules: &SealedProcessRuleSnapshotV3,
    cash: Decimal,
    legs: &[LimitLeg],
) -> Result<BigUint, String> {
    let available = cash_micro(cash);
    let mut lo = BigUint::zero();
    let mut hi = quantity_count();
    while lo < hi {
        let middle = (&lo + &hi + 1u8) / 2u8;
        let n = rank_to_centishares(&(&middle - 1u8))?;
        let total = costs(rules, &n, legs)?
            .into_iter()
            .fold(BigUint::zero(), |sum, c| {
                sum + c.principal_micro + c.fee_1e5_units * 10u8
            });
        if total <= available {
            lo = middle;
        } else {
            hi = middle - 1u8;
        }
    }
    Ok(lo)
}

/// 冻结帧中同一精确资金域；Python 只调用此实现，不复制金额或费用算法。
pub fn supported_posts(
    rules: &SealedProcessRuleSnapshotV3,
    cash: Decimal,
    ticks: &[ExactDecimal; 2],
    sides: &[usize],
    selected: &[LimitLeg],
    ordinal: usize,
) -> Result<Vec<bool>, String> {
    let side = *sides.get(ordinal).ok_or("属性腿序号越界")?;
    let tick = ticks.get(side).ok_or("属性方向越界")?;
    let mut supported = Vec::new();
    for post in [false, true] {
        let mut minimum = selected.to_vec();
        minimum.push(LimitLeg {
            price_index: 1,
            tick: tick.clone(),
            post_only: post,
        });
        for other in sides.iter().skip(ordinal + 1) {
            minimum.push(LimitLeg {
                price_index: 1,
                tick: ticks.get(*other).ok_or("属性余腿方向越界")?.clone(),
                post_only: true,
            });
        }
        if !affordable_count(rules, cash, &minimum)?.is_zero() {
            supported.push(post);
        }
    }
    Ok(supported)
}

/// 所选价格至少容纳0.01份，余腿保持最便宜合法价格；没有添加平台MOS掩码。
pub fn maximum_affordable_price(
    rules: &SealedProcessRuleSnapshotV3,
    cash: Decimal,
    ticks: &[ExactDecimal; 2],
    max_prices: &[u16; 2],
    sides: &[usize],
    selected: &[LimitLeg],
    ordinal: usize,
    post_only: bool,
) -> Result<u16, String> {
    let side = *sides.get(ordinal).ok_or("价格腿序号越界")?;
    let tick = ticks.get(side).ok_or("价格方向越界")?;
    let mut lower = 1u16;
    let mut upper = *max_prices.get(side).ok_or("价格方向越界")?;
    while lower < upper {
        let mid = lower + (upper - lower).div_ceil(2);
        let mut candidate = selected.to_vec();
        candidate.push(LimitLeg {
            price_index: mid,
            tick: tick.clone(),
            post_only,
        });
        for other in sides.iter().skip(ordinal + 1) {
            candidate.push(LimitLeg {
                price_index: 1,
                tick: ticks.get(*other).ok_or("价格余腿方向越界")?.clone(),
                post_only: true,
            });
        }
        let total = costs(rules, &BigUint::one(), &candidate)?
            .into_iter()
            .fold(BigUint::zero(), |sum, c| {
                sum + c.principal_micro + c.fee_1e5_units * 10u8
            });
        if total <= cash_micro(cash) {
            lower = mid;
        } else {
            upper = mid - 1;
        }
    }
    Ok(lower)
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts::Sha256Hex;
    use sha2::{Digest, Sha256};
    fn rules() -> std::sync::Arc<SealedProcessRuleSnapshotV3> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json");
        let hash = format!("{:x}", Sha256::digest(std::fs::read(&path).unwrap()));
        sim_system::platform_rules::load_historical_process_snapshot_v3(
            sim_system::platform_rules::HistoricalProcessSnapshotRequestV1 {
                snapshot_path: path,
                expected_file_sha256: Sha256Hex(hash),
                replay_causal_start_utc_ms: i64::MAX,
            },
        )
        .unwrap()
    }
    #[test]
    fn original_cost_rounds_legs_separately_and_does_not_mask_platform_minimum() {
        let rules = rules();
        let leg = LimitLeg {
            price_index: 25,
            tick: ExactDecimal("0.01".into()),
            post_only: false,
        };
        let values = costs(&rules, &BigUint::from(500u32), &[leg.clone(), leg.clone()]).unwrap();
        assert_eq!(values[0].principal_micro, BigUint::from(1_250_000u32));
        assert_eq!(values[0].fee_1e5_units, BigUint::from(6563u32));
        assert_eq!(
            affordable_count(&rules, "2.63125".parse().unwrap(), &[leg.clone(), leg]).unwrap(),
            BigUint::from(499u32)
        );
        let tiny = LimitLeg {
            price_index: 10,
            tick: ExactDecimal("0.01".into()),
            post_only: true,
        };
        assert_eq!(
            affordable_count(&rules, "0.001".parse().unwrap(), &[tiny]).unwrap(),
            BigUint::one()
        );
    }
    #[test]
    fn public_cost_can_exceed_ledger_decimal_coefficient_at_affordable_size() {
        // 已知接口边界的反例；此通过仅证明缺口存在，不证明消费者完整资金域已验收。
        let rules = rules();
        let n = coefficient_max();
        let cost = costs(
            &rules,
            &n,
            &[LimitLeg {
                price_index: 9999,
                tick: ExactDecimal("0.0001".into()),
                post_only: true,
            }],
        )
        .unwrap()
        .remove(0);
        assert!(shares(&n).is_ok());
        assert_eq!(cost.principal_micro, &n * 9999u32);
        assert!(cost.principal_micro < cash_micro(Decimal::MAX));
        assert!(Decimal::from_str_exact(&scaled_string(&cost.principal_micro, 6)).is_err());
    }
    #[test]
    fn decimal_domain_has_no_duplicates_or_u64_ceiling() {
        let m = coefficient_max();
        let high = &m - &m / 10u8;
        for rank in [
            BigUint::zero(),
            &m - 1u8,
            m.clone(),
            &m + &high - 1u8,
            &m + &high,
            quantity_count() - 1u8,
        ] {
            let n = rank_to_centishares(&rank).unwrap();
            let q = shares(&n).unwrap();
            assert!(q > Decimal::ZERO);
            let exact =
                BigUint::from(q.mantissa() as u128) * BigUint::from(10u8).pow(2 - q.scale());
            assert_eq!(exact, n);
            if rank > BigUint::zero() {
                assert!(rank_to_centishares(&(&rank - 1u8)).unwrap() < n);
            }
        }
        assert_eq!(shares(&(&m * 100u8)).unwrap(), Decimal::MAX);
        assert_eq!(shares(&BigUint::from(1u8)).unwrap(), Decimal::new(1, 2));
    }
}
