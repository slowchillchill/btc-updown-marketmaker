use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// 全部数值显式指定；没有未经校准的正式风险默认值或溢价否决开关。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComplementaryPairConfigV1 {
    pub probability: ProbabilityConfigV1,
    pub quote: QuoteConfigV1,
    pub repair: RepairConfigV1,
    pub risk: InventoryRiskConfigV1,
}
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuoteConfigV1 {
    pub base_pair_shares: Decimal,
    pub base_half_spread: Decimal,
    pub inventory_shift_per_pusd: Decimal,
    pub uncertainty_spread_weight: Decimal,
    pub signal_spread_weight: Decimal,
    /// 联合情景 [双腿、仅Up、仅Down、都不成交]；研究候选，非真实成交概率。
    pub joint_fill_scenario_weights: [f64; 4],
    pub minimum_business_result_per_share: Decimal,
    pub execution_path_ms: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepairConfigV1 {
    pub base_loss_allowance_per_share: Decimal,
    pub time_loss_allowance_per_share: Decimal,
    pub inventory_loss_allowance_per_share: Decimal,
    pub maximum_overfill_shares: Decimal,
    pub maximum_overfill_pusd: Decimal,
    /// 比较等待/Maker/FAK/留孤腿的候选系数，没有独立溢价门。
    pub maker_fill_weight: f64,
    pub wait_price_improvement_per_share: Decimal,
    pub residual_risk_weight: Decimal,
    pub maker_path_ms: u64,
    pub active_path_ms: u64,
    pub enable_active_repair: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryRiskConfigV1 {
    pub per_market_orphan_limit_pusd: Decimal,
    pub account_orphan_limit_pusd: Decimal,
}
impl ComplementaryPairConfigV1 {
    /// 只解析一份完整策略配置；CLI 与文件覆盖由上层宿主负责。
    pub fn parse_toml(text: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(text)
    }
}
