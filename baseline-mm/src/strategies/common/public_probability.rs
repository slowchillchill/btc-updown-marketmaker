//! 新版本只融合公共纯统计；不保存决定频率采样或私有基线。
use super::*;
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PublicProbabilityInputsV4 {
    pub group: Option<PublicGroupReferenceV1>,
    pub observed_at_utc_ms: Option<i64>,
    pub reference_channel: Option<BtcSettlementReferenceV1>,
    pub volatility_window_ms: u64,
    pub public_sigma: Option<f64>,
    pub initial_sigma_fallback: bool,
    pub basis_innovation: Option<Decimal>,
    pub depth_imbalance: Option<Decimal>,
    pub latest_trade_pressure: Option<Decimal>,
    pub settlement_window_ms: Option<u64>,
    pub observed_integral_usd_seconds: Option<f64>,
}
pub(crate) fn estimate(
    o: &MarketBusinessObservationV1,
    statistics: Option<&PublicMarketStatisticsV1>,
    c: &ProbabilityConfigV1,
) -> Result<(ProbabilityEstimateV1, PublicProbabilityInputsV4), StrategyErrorV1> {
    use EstimateUnavailableV1 as U;
    let mut evidence = PublicProbabilityInputsV4 {
        group: statistics.map(|s| s.group.clone()),
        observed_at_utc_ms: statistics.map(|s| s.observed_at_utc_ms),
        reference_channel: None,
        volatility_window_ms: c.volatility_window_ms,
        public_sigma: statistics
            .and_then(|s| {
                s.spot_variance
                    .iter()
                    .find(|v| v.window_ms == c.volatility_window_ms)
            })
            .and_then(|v| v.sigma_usd_per_btc_per_sqrt_second),
        initial_sigma_fallback: false,
        basis_innovation: None,
        depth_imbalance: statistics.and_then(|s| s.depth.imbalance.value),
        latest_trade_pressure: statistics.and_then(|s| s.latest_trade_pressure.value),
        settlement_window_ms: None,
        observed_integral_usd_seconds: None,
    };
    macro_rules! unavailable {
        ($reason:expr) => {
            return Ok((ProbabilityEstimateV1::Unavailable($reason), evidence));
        };
    }
    let Some(stats) = statistics else {
        unavailable!(U::MissingPublicStatistics);
    };
    let now = stats.observed_at_utc_ms;
    let end = o.market.window.end_at_utc_ms;
    if now >= end {
        unavailable!(U::MarketEnded);
    }
    let Some(rule) = value(&o.settlement_reference).copied() else {
        unavailable!(U::MissingSettlementRule);
    };
    let spot = eligible_chainlink_spot_v1(&o.chainlink_spot);
    let (price, channel, anchors) = if let Some(spot) = spot {
        (
            &spot.price_usd_per_btc,
            BtcSettlementReferenceV1::ChainlinkSpot,
            &o.chainlink_spot.direct_anchors,
        )
    } else {
        match rule {
            BtcSettlementReferenceV1::ChainlinkTwapThirtySeconds => {
                let Some(v) = value(&o.chainlink_twap_thirty) else {
                    unavailable!(U::MissingSettlementPrice);
                };
                (
                    &v.price_usd_per_btc,
                    rule,
                    &o.chainlink_twap_thirty.direct_anchors,
                )
            }
            BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds => {
                let Some(v) = value(&o.chainlink_twap_sixty) else {
                    unavailable!(U::MissingSettlementPrice);
                };
                (
                    &v.price_usd_per_btc,
                    rule,
                    &o.chainlink_twap_sixty.direct_anchors,
                )
            }
            BtcSettlementReferenceV1::ChainlinkSpot => {
                unavailable!(U::MissingSettlementPrice);
            }
        }
    };
    evidence.reference_channel = Some(channel);
    let basis_index = match channel {
        BtcSettlementReferenceV1::ChainlinkSpot => 0,
        BtcSettlementReferenceV1::ChainlinkTwapThirtySeconds => 1,
        BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds => 2,
    };
    evidence.basis_innovation = stats.basis[basis_index]
        .as_ref()
        .map(|v| v.innovation_usd_per_btc_numeric_proxy);
    let s = spot_price_number_v1(price)?;
    let reference = match &o.effective_reference {
        EffectiveMarketReferenceV1::OfficialAnchor(f) => {
            value(f).map(|a| (&a.price_usd_per_btc, false))
        }
        EffectiveMarketReferenceV1::TemporaryTwap(t) => Some((&t.value.price_usd_per_btc, true)),
        EffectiveMarketReferenceV1::Missing => None,
    };
    let Some((k, temp)) = reference else {
        unavailable!(U::MissingReference);
    };
    let k = spot_price_number_v1(k)?;
    evidence.initial_sigma_fallback = evidence.public_sigma.is_none();
    let sigma = evidence
        .public_sigma
        .unwrap_or(c.initial_sigma_usd_per_sqrt_second);
    // 缺失分量沿策略不贡献；证据仍为None，绝不把公共未知改成事实零。
    let component = |v: Option<Decimal>, weight: f64| -> Result<f64, StrategyErrorV1> {
        v.map(|v| float(&v))
            .transpose()
            .map(|v| v.unwrap_or(0.0) * weight)
    };
    let innovation = component(evidence.basis_innovation, c.binance_innovation_weight)?
        + component(evidence.depth_imbalance, c.binance_pressure_weight_usd)?
        + component(
            evidence.latest_trade_pressure,
            c.binance_trade_pressure_weight_usd,
        )?;
    let w = probability::window(rule);
    let tau = (end - now) as f64 / 1000.;
    let ws = w as f64 / 1000.;
    let (mu, variance, approx) = if w == 0 {
        (s + innovation, sigma * sigma * tau, false)
    } else if tau >= ws {
        (s + innovation, sigma * sigma * (tau - 2. * ws / 3.), false)
    } else {
        evidence.settlement_window_ms = Some(w as u64);
        let integral = stats
            .spot_integrals
            .iter()
            .find(|v| v.window_ms == w as u64);
        evidence.observed_integral_usd_seconds =
            integral.and_then(|v| v.integral_usd_per_btc_seconds.value);
        let Some(integral) = integral.filter(|v| {
            v.covered_from_utc_ms.is_some_and(|t| t <= end - w) && v.observed_through_utc_ms >= now
        }) else {
            unavailable!(U::MissingWindowHistory);
        };
        let Some(value) = integral.integral_usd_per_btc_seconds.value else {
            unavailable!(U::MissingWindowHistory);
        };
        (
            (value + tau * (s + innovation)) / ws,
            sigma * sigma * tau.powi(3) / (3. * ws * ws),
            true,
        )
    };
    let uncertainty = (variance + c.estimation_error_usd.powi(2)).sqrt();
    let z = (uncertainty > 0.).then(|| (mu - k) / uncertainty);
    let up = z
        .map(|z| 0.5 * (1. + libm::erf(z / std::f64::consts::SQRT_2)))
        .unwrap_or(if mu >= k { 1. } else { 0. });
    if !mu.is_finite() || !uncertainty.is_finite() || !up.is_finite() {
        return Err(StrategyErrorV1::Arithmetic("概率计算非有限"));
    }
    Ok((
        ProbabilityEstimateV1::Known {
            up,
            mean_usd: mu,
            uncertainty_usd: uncertainty,
            standardized_distance: z,
            sigma_usd_per_sqrt_second: sigma,
            binance_innovation_usd: innovation,
            information_age_ms: anchors
                .iter()
                .map(|a| a.received_at_utc_ms)
                .max()
                .and_then(|at| u64::try_from(now - at).ok()),
            temporary_reference: temp,
            approximate_window_integral: approx,
        },
        evidence,
    ))
}
