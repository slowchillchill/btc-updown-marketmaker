//! 后继公共行情统计合同。F0 沿用 MarketBusinessObservationV1，不复制原行情。
use crate::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const PUBLIC_STATISTICS_VERSION_V1: &str = "public-market-statistics-v1";
/// 参数在消费前固定；所有窗口单位为毫秒。无隐含 A/B 经营默认值。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicStatisticsConfigV1 {
    pub momentum_windows_ms: Vec<u64>,
    pub spot_variance_windows_ms: Vec<u64>,
    pub terminal_integral_windows_ms: Vec<u64>,
    /// 显式消费者所需的 F2 asof 回看；空集合不物化历史端点。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blb_lookback_windows_ms: Vec<u64>,
}
/// 绑定输入内容、归约/观察、排序、规则及实际交付时序。配置另以完整值绑定。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicStatisticsIdentityV1 {
    pub input_set_identity: Sha256Hex,
    pub stream_header: PublicInputStreamHeaderV1,
    pub rules_identity: Sha256Hex,
    pub delivery_policy: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StatisticValueV1<T> {
    pub value: Option<T>,
    pub missing_reason: Option<String>,
}
impl<T> StatisticValueV1<T> {
    pub fn known(value: T) -> Self {
        Self {
            value: Some(value),
            missing_reason: None,
        }
    }
    pub fn missing(reason: &str) -> Self {
        Self {
            value: None,
            missing_reason: Some(reason.into()),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PublicMomentumV1 {
    pub window_ms: u64,
    pub change_usdt_per_btc: StatisticValueV1<Decimal>,
    pub left_applied_at_utc_ms: Option<i64>,
    pub right_applied_at_utc_ms: Option<i64>,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PublicSpotVarianceV1 {
    pub window_ms: u64,
    pub sigma_usd_per_btc_per_sqrt_second: Option<f64>,
    pub statistics: SpotVarianceStatisticsV1,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PublicSpotIntegralV1 {
    pub window_ms: u64,
    pub observed_through_utc_ms: i64,
    pub covered_from_utc_ms: Option<i64>,
    pub observed_seconds: f64,
    pub integral_usd_per_btc_seconds: StatisticValueV1<f64>,
    /// 持价路径只覆盖已观察事实，不证明真实行情连续完整。
    pub coverage: String,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PublicBasisInnovationV1 {
    pub reference: BtcSettlementReferenceV1,
    pub initialized: bool,
    pub delta_binance_usdt_per_btc: Decimal,
    pub delta_reference_usd_per_btc: Decimal,
    pub innovation_usd_per_btc_numeric_proxy: Decimal,
    pub baseline_group: PublicGroupReferenceV1,
    pub related_group: PublicGroupReferenceV1,
    pub related_applied_at_utc_ms: i64,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PublicAssetQuantitiesV1 {
    pub asset: AssetIdentityV1,
    pub bid_shares: StatisticValueV1<Decimal>,
    pub ask_shares: StatisticValueV1<Decimal>,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PublicDepthStatisticsV1 {
    pub best_bid_usdt_per_btc: StatisticValueV1<Decimal>,
    pub best_ask_usdt_per_btc: StatisticValueV1<Decimal>,
    pub midpoint_usdt_per_btc: StatisticValueV1<Decimal>,
    pub spread_usdt_per_btc: StatisticValueV1<Decimal>,
    pub bid_quantity_btc: StatisticValueV1<Decimal>,
    pub ask_quantity_btc: StatisticValueV1<Decimal>,
    pub imbalance: StatisticValueV1<Decimal>,
    pub interval_coverage: &'static str,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PublicMarketStatisticsV1 {
    pub definition_version: &'static str,
    pub identity: Arc<PublicStatisticsIdentityV1>,
    pub config: Arc<PublicStatisticsConfigV1>,
    pub group: PublicGroupReferenceV1,
    pub applied_at_utc_ms: i64,
    pub observed_at_utc_ms: i64,
    pub market: MarketIdentityV2,
    pub since_market_start_ms: i64,
    pub until_market_end_ms: i64,
    pub depth: Arc<PublicDepthStatisticsV1>,
    pub blb: BlbResearchV1,
    pub blb_lookbacks: Vec<PublicBlbLookbackV1>,
    pub momentum: Vec<PublicMomentumV1>,
    pub spot_variance: Vec<PublicSpotVarianceV1>,
    pub spot_integrals: Vec<PublicSpotIntegralV1>,
    pub latest_trade_pressure: StatisticValueV1<Decimal>,
    pub basis: [Option<PublicBasisInnovationV1>; 3],
    pub pm_quantities: Vec<PublicAssetQuantitiesV1>,
    /// F9 原质量、证据、来源时间由同前缀 F0 观察提供；首次应用时间保存在此。
    pub source_applied_at_utc_ms: [Option<i64>; 5],
    pub source_received_age_ms: [Option<i64>; 5],
}
/// 正式应用时保存的 F2 必要标量及同端 F1/锚事实；不含盘口或档位数组。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PublicBlbEndpointV1 {
    pub definition_version: String,
    pub identity: Arc<PublicStatisticsIdentityV1>,
    pub config: Arc<PublicStatisticsConfigV1>,
    pub market: MarketIdentityV2,
    pub group: PublicGroupReferenceV1,
    pub applied_at_utc_ms: i64,
    pub status: String,
    pub unknown_reason: Option<String>,
    pub midpoint_usdt_per_btc: Option<Decimal>,
    pub official_k_usd_per_btc: Option<Decimal>,
    pub side: Option<String>,
    pub at_anchor: bool,
    pub visible_quantity_btc: Option<Decimal>,
    pub official_anchor: ObservedFactV1<MarketPriceAnchorV1>,
    pub best_bid_usdt_per_btc: StatisticValueV1<Decimal>,
    pub best_ask_usdt_per_btc: StatisticValueV1<Decimal>,
    pub depth_quality: MarketDataQuality,
    pub depth_anchors: Vec<DirectObservationAnchorV1>,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PublicBlbLookbackV1 {
    pub window_ms: u64,
    pub left_query_at_utc_ms: i64,
    pub left: Option<Arc<PublicBlbEndpointV1>>,
    pub right: Option<Arc<PublicBlbEndpointV1>>,
}
pub const BLB_RESEARCH_VERSION_V1: &str = "blb-visible-interval-v1";
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BlbResearchV1 {
    pub definition_version: &'static str,
    pub status: &'static str,
    pub unknown_reason: Option<&'static str>,
    pub midpoint_usdt_per_btc: Option<Decimal>,
    pub official_k_usd_per_btc: Option<Decimal>,
    pub distance: Option<Decimal>,
    pub side: Option<&'static str>,
    pub at_anchor: bool,
    pub visible_positive_levels: Vec<ReferencePriceLevelV1>,
    pub visible_level_count: Option<usize>,
    pub visible_quantity_btc: Option<Decimal>,
    pub visible_notional_usdt: Option<Decimal>,
    /// 维护档位没有区间完整性证明；零可见量也不等于交易所零量。
    pub interval_coverage: &'static str,
}

/// 两端来自已接纳现价；按右端纳入，保留跨左界的完整价对。
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct SpotVariancePairV1 {
    pub squared_change_usd: f64,
    pub elapsed_seconds: f64,
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Serialize)]
pub struct SpotVarianceStatisticsV1 {
    pub positive_pair_count: u64,
    pub squared_change_sum_usd_squared: f64,
    pub elapsed_seconds: f64,
}
impl SpotVarianceStatisticsV1 {
    pub fn include(&mut self, pair: SpotVariancePairV1) {
        self.positive_pair_count += 1;
        self.squared_change_sum_usd_squared += pair.squared_change_usd;
        self.elapsed_seconds += pair.elapsed_seconds;
    }
    pub fn sigma_usd_per_sqrt_second(&self) -> Option<f64> {
        (self.elapsed_seconds > 0.0)
            .then(|| (self.squared_change_sum_usd_squared / self.elapsed_seconds).sqrt())
    }
}
