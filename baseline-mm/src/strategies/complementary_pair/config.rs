use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// 全部数值显式指定；没有未经校准的正式风险默认值或溢价否决开关。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComplementaryPairConfigV3 {
    pub health: SourceHealthConfigV1,
    pub probability: ProbabilityConfigV1,
    pub quote: QuoteConfigV2,
    pub repair: RepairConfigV2,
    pub risk: InventoryRiskConfigV1,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuoteConfigV2 {
    /// 初始双腿和一般维护使用的价格参考；修复内部仍使用公平价。
    pub initial_pair_price_reference: InitialPairPriceReferenceV1,
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
pub struct InventoryRiskConfigV1 {
    pub per_market_orphan_limit_pusd: Decimal,
    pub account_orphan_limit_pusd: Decimal,
}
impl ComplementaryPairConfigV3 {
    /// 只解析一份完整策略配置；CLI 与文件覆盖由上层宿主负责。
    pub fn parse_toml(text: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(text)
    }
}

/// 显式选择报价规则；旧配置不自动补为公平价。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InitialPairPriceReferenceV1 {
    FairValue,
    BestBid,
}

/// C0/C1 共同采用公共统计；42个原经营参数保持正式旧类型，唯一附加差别是BLB否决。
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComplementaryPairConfigV4 {
    pub parameters: ComplementaryPairConfigV3,
    pub blb_decrease_veto: bool,
}

/// 同版本C0/低价初始批否决对照；不叠加BLB门，原42参数继续复用V3。
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComplementaryPairConfigV5 {
    pub parameters: ComplementaryPairConfigV3,
    pub initial_pair_low_price_veto: bool,
}

pub use super::super::common::{ProbabilityConfigV1, SourceHealthConfigV1, SourceHealthLimitsV1};

pub use super::super::common::RepairConfigV2;
