//! 人工候选共用的精确表示、来源健康与原终止概率计算；不选择普通报价政策。
use contracts::{economics::EconomicRational as R, *};
use num_bigint::{BigInt, BigUint};
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;
mod health;
pub use health::*;
pub(crate) mod probability;
pub(crate) use probability::SignalMemory;
pub use probability::{EstimateUnavailableV1, ProbabilityEstimateV1};
mod spot_statistics;
pub use spot_statistics::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbabilityConfigV1 {
    /// 因果价格变化的滚动样本窗口，毫秒；不作为数据陈旧阈值。
    pub volatility_window_ms: u64,
    pub initial_sigma_usd_per_sqrt_second: f64,
    pub estimation_error_usd: f64,
    /// Binance 相对因果基差的价格创新；0 表示禁用该修正。
    pub binance_innovation_weight: f64,
    pub binance_pressure_weight_usd: f64,
    pub binance_trade_pressure_weight_usd: f64,
}
#[derive(Debug)]
pub enum StrategyErrorV1 {
    Configuration(String),
    Arithmetic(&'static str),
    Cost(String),
}
impl fmt::Display for StrategyErrorV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(s) => write!(f, "策略配置：{s}"),
            Self::Arithmetic(s) => write!(f, "策略计算：{s}"),
            Self::Cost(s) => write!(f, "公共成本：{s}"),
        }
    }
}
impl std::error::Error for StrategyErrorV1 {}
pub(crate) fn decimal(v: &ExactDecimal) -> Result<Decimal, StrategyErrorV1> {
    Decimal::from_str_exact(&v.0).map_err(|_| StrategyErrorV1::Arithmetic("非规范公共金额"))
}
pub(crate) fn r(v: Decimal) -> R {
    R::from_decimal(v)
}
pub(crate) fn rational(v: &ExactRationalV1) -> Result<R, StrategyErrorV1> {
    R::try_from(v).map_err(|_| StrategyErrorV1::Arithmetic("非规范公共分数"))
}
pub(crate) fn float(v: &Decimal) -> Result<f64, StrategyErrorV1> {
    v.to_f64()
        .ok_or(StrategyErrorV1::Arithmetic("浮点映射溢出"))
}
pub(crate) fn fd(v: f64) -> Result<Decimal, StrategyErrorV1> {
    Decimal::from_f64_retain(v).ok_or(StrategyErrorV1::Arithmetic("统计值无法映射合法报价"))
}
pub(crate) fn exact(v: Decimal) -> ExactDecimal {
    ExactDecimal(v.normalize().to_string())
}
pub(crate) fn value<T>(f: &ObservedFactV1<T>) -> Option<&T> {
    matches!(
        f.quality,
        MarketDataQuality::Fresh | MarketDataQuality::Stale
    )
    .then_some(f.value.as_ref())
    .flatten()
}
pub(crate) fn known<T>(v: &ExecutionReadValueV1<T>) -> Option<&T> {
    match v {
        ExecutionReadValueV1::Known(v) => Some(v),
        _ => None,
    }
}
pub(crate) fn fraction_units(n: &BigUint, places: u32) -> R {
    R::ratio(BigInt::from(n.clone()), BigInt::from(10_u32).pow(places))
}
pub(crate) fn floor_decimal(v: &R, places: u32) -> Result<Decimal, StrategyErrorV1> {
    let n = v.numerator() * BigInt::from(10_u32).pow(places) / v.denominator();
    let n = n
        .to_i128()
        .ok_or(StrategyErrorV1::Arithmetic("精确金额不能编码为Decimal"))?;
    Decimal::try_from_i128_with_scale(n, places)
        .map_err(|_| StrategyErrorV1::Arithmetic("编码金额溢出"))
}

mod orders;
pub(crate) use orders::{limit_cost, reserved_buy_item};

mod profit;
pub use profit::ProfitAllocationV2;
pub(crate) use profit::{
    RemainingRepairEstimate, limit_leg, remaining_estimate, remaining_inventory_estimate,
    remaining_inventory_profit_occupancy, remaining_profit_occupancy,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryMarketRangeV1 {
    pub start_at_utc_ms: i64,
    pub end_at_utc_ms: i64,
}
impl EntryMarketRangeV1 {
    pub fn contains(&self, market_start: i64) -> bool {
        self.start_at_utc_ms <= market_start && market_start < self.end_at_utc_ms
    }
}

pub(crate) fn probability_sources(
    config: &ProbabilityConfigV1,
    o: &MarketBusinessObservationV1,
) -> Vec<SourceStreamScopeV1> {
    let chainlink = if eligible_chainlink_spot_v1(&o.chainlink_spot).is_some() {
        SourceStreamKindV1::ChainlinkSpot
    } else {
        match value(&o.settlement_reference) {
            Some(BtcSettlementReferenceV1::ChainlinkTwapThirtySeconds) => {
                SourceStreamKindV1::ChainlinkTwapThirty
            }
            Some(BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds) => {
                SourceStreamKindV1::ChainlinkTwapSixty
            }
            _ => SourceStreamKindV1::ChainlinkSpot,
        }
    };
    let mut sources = vec![SourceStreamScopeV1 {
        source: chainlink,
        condition_id: None,
        asset_or_symbol: "btc/usd".into(),
    }];
    let p = config;
    if p.binance_innovation_weight != 0.0 || p.binance_trade_pressure_weight_usd != 0.0 {
        sources.push(SourceStreamScopeV1 {
            source: SourceStreamKindV1::BinanceAggregateTrade,
            condition_id: None,
            asset_or_symbol: "BTCUSDT".into(),
        });
    }
    // 成交压力读取深度分母，即使独立深度系数为零仍参与。
    if p.binance_pressure_weight_usd != 0.0 || p.binance_trade_pressure_weight_usd != 0.0 {
        sources.push(SourceStreamScopeV1 {
            source: SourceStreamKindV1::BinanceDepth,
            condition_id: None,
            asset_or_symbol: "BTCUSDT".into(),
        });
    }
    sources
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepairConfigV2 {
    pub base_loss_allowance_per_share: Decimal,
    pub time_loss_allowance_per_share: Decimal,
    pub inventory_loss_allowance_per_share: Decimal,
    /// 比较等待/Maker/FAK/留孤腿的候选系数，没有独立溢价门。
    pub maker_fill_weight: f64,
    pub wait_price_improvement_per_share: Decimal,
    pub residual_risk_weight: Decimal,
    pub maker_path_ms: u64,
    pub active_path_ms: u64,
    pub enable_active_repair: bool,
}

pub(crate) fn loss_allowance_v1(
    config: &RepairConfigV2,
    limit: Decimal,
    cost: &R,
    start: i64,
    end: i64,
    now: i64,
) -> Result<Decimal, StrategyErrorV1> {
    let duration = (end - start).max(1);
    let elapsed = Decimal::from((now - start).clamp(0, duration)) / Decimal::from(duration);
    let pressure = if limit > Decimal::ZERO {
        floor_decimal(cost, 8)? / limit
    } else {
        Decimal::ZERO
    };
    Ok(config.base_loss_allowance_per_share
        + config.time_loss_allowance_per_share * elapsed
        + config.inventory_loss_allowance_per_share * pressure)
}
pub(crate) fn maker_repair_values_v1(
    config: &RepairConfigV2,
    basis: f64,
    maker: Option<Decimal>,
    old_unit_cost: &R,
    orphan_cost: &R,
    market_limit: Decimal,
    start: i64,
    end: i64,
    now: i64,
) -> Result<(f64, f64, Option<f64>), StrategyErrorV1> {
    let time_pressure = ((now - start) as f64 / (end - start).max(1) as f64).clamp(0.0, 1.0);
    let cost_per_share = float(&floor_decimal(old_unit_cost, 16)?)?;
    let inventory_pressure = if market_limit > Decimal::ZERO {
        float(&floor_decimal(orphan_cost, 16)?)? / float(&market_limit)?
    } else {
        0.0
    };
    let risk = float(&config.residual_risk_weight)?
        * cost_per_share
        * (time_pressure + inventory_pressure);
    let retain = -risk;
    let weight = config.maker_fill_weight;
    let maker =
        maker.map(|p| weight * (basis - float(&p).unwrap_or(0.0)) + (1.0 - weight) * retain);
    let wait = retain + float(&config.wait_price_improvement_per_share)? * (1.0 - time_pressure);
    Ok((retain, wait, maker))
}

pub(crate) mod public_probability;
pub use public_probability::PublicProbabilityInputsV4;
