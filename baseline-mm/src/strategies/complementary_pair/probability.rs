use super::*;
use std::collections::VecDeque;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum EstimateUnavailableV1 {
    MissingReference,
    MissingSettlementRule,
    MissingSettlementPrice,
    MissingWindowHistory,
    MarketEnded,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ProbabilityEstimateV1 {
    Known {
        up: f64,
        mean_usd: f64,
        uncertainty_usd: f64,
        standardized_distance: Option<f64>,
        sigma_usd_per_sqrt_second: f64,
        binance_innovation_usd: f64,
        information_age_ms: Option<u64>,
        temporary_reference: bool,
        approximate_window_integral: bool,
    },
    Unavailable(EstimateUnavailableV1),
}
impl ProbabilityEstimateV1 {
    pub(super) fn up(&self) -> Option<f64> {
        match self {
            Self::Known { up, .. } => Some(*up),
            _ => None,
        }
    }
}
#[derive(Default)]
pub(super) struct SignalMemory {
    chainlink: VecDeque<(i64, f64)>,
    binance_baseline: Option<(f64, f64, i64)>,
}
fn number(v: &ExactDecimal) -> Result<f64, StrategyErrorV1> {
    float(&decimal(v)?)
}
fn fact_time<T>(f: &ObservedFactV1<T>, fallback: i64) -> i64 {
    f.direct_anchors
        .iter()
        .map(|a| a.received_at_utc_ms)
        .max()
        .unwrap_or(fallback)
}
pub(super) fn window(rule: BtcSettlementReferenceV1) -> i64 {
    match rule {
        BtcSettlementReferenceV1::ChainlinkSpot => 0,
        BtcSettlementReferenceV1::ChainlinkTwapThirtySeconds => 30_000,
        BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds => 60_000,
    }
}
impl SignalMemory {
    pub(super) fn estimate(
        &mut self,
        o: &MarketBusinessObservationV1,
        now: i64,
        c: &ProbabilityConfigV1,
    ) -> Result<ProbabilityEstimateV1, StrategyErrorV1> {
        use EstimateUnavailableV1 as U;
        let end = o.market.window.end_at_utc_ms;
        if now >= end {
            return Ok(ProbabilityEstimateV1::Unavailable(U::MarketEnded));
        }
        let Some(rule) = value(&o.settlement_reference).copied() else {
            return Ok(ProbabilityEstimateV1::Unavailable(U::MissingSettlementRule));
        };
        let w = window(rule);
        let twap = match rule {
            BtcSettlementReferenceV1::ChainlinkTwapThirtySeconds => value(&o.chainlink_twap_thirty),
            BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds => value(&o.chainlink_twap_sixty),
            _ => None,
        };
        // TWAP 可供未来水平近似；滚动 TWAP 从不被拿作已发生积分。
        let spot = value(&o.chainlink_spot);
        let price = spot
            .map(|p| (&p.price_usd_per_btc, fact_time(&o.chainlink_spot, now)))
            .or_else(|| {
                twap.map(|p| {
                    (
                        &p.price_usd_per_btc,
                        match rule {
                            BtcSettlementReferenceV1::ChainlinkTwapThirtySeconds => {
                                fact_time(&o.chainlink_twap_thirty, now)
                            }
                            BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds => {
                                fact_time(&o.chainlink_twap_sixty, now)
                            }
                            BtcSettlementReferenceV1::ChainlinkSpot => now,
                        },
                    )
                })
            });
        let Some((price, time)) = price else {
            return Ok(ProbabilityEstimateV1::Unavailable(
                U::MissingSettlementPrice,
            ));
        };
        let s = number(price)?;
        // 只有现价样本进入持价路径；rolling TWAP回退只参与未来水平估计。
        if spot.is_some() && time <= now && self.chainlink.back().is_none_or(|(t, _)| time > *t) {
            self.chainlink.push_back((time, s));
        }
        // 只丢弃已不参与波动或本市场终局窗口积分的样本，保留左端持价。
        let retain_from =
            (now - i64::try_from(c.volatility_window_ms).unwrap_or(i64::MAX)).min(end - w);
        while self.chainlink.len() > 2
            && self.chainlink.get(1).is_some_and(|(t, _)| *t < retain_from)
        {
            self.chainlink.pop_front();
        }
        let reference = match &o.effective_reference {
            EffectiveMarketReferenceV1::OfficialAnchor(f) => {
                value(f).map(|a| (&a.price_usd_per_btc, false))
            }
            EffectiveMarketReferenceV1::TemporaryTwap(t) => {
                Some((&t.value.price_usd_per_btc, true))
            }
            EffectiveMarketReferenceV1::Missing => None,
        };
        let Some((k, temp)) = reference else {
            return Ok(ProbabilityEstimateV1::Unavailable(U::MissingReference));
        };
        let k = number(k)?;
        let mut variance = 0.0;
        let mut elapsed = 0.0;
        let from = now.saturating_sub(i64::try_from(c.volatility_window_ms).unwrap_or(i64::MAX));
        for ((t0, p0), (t1, p1)) in self.chainlink.iter().zip(self.chainlink.iter().skip(1)) {
            if *t1 >= from && t1 > t0 {
                variance += (p1 - p0).powi(2);
                elapsed += (*t1 - *t0) as f64 / 1000.0;
            }
        }
        let sigma = if elapsed > 0.0 {
            (variance / elapsed).sqrt()
        } else {
            c.initial_sigma_usd_per_sqrt_second
        };
        let mut innovation = 0.0;
        if let Some(b) = value(&o.binance_trade) {
            let b = number(&b.price_usdt_per_btc)?;
            let baseline = self.binance_baseline.get_or_insert((b, s, time));
            // 同时刻配对建立因果基差；只融合后续变化，不直接减BTCUSDT与BTCUSD。
            innovation = ((b - baseline.0) - (s - baseline.1)) * c.binance_innovation_weight;
            if time > baseline.2 {
                *baseline = (b, s, time);
            }
        }
        if let Some(trade) = value(&o.binance_trade) {
            let q = number(&trade.quantity_btc)?;
            let depth = value(&o.binance_depth)
                .map(|depth| {
                    depth
                        .bids
                        .iter()
                        .chain(&depth.asks)
                        .try_fold(0.0, |sum, l| {
                            Ok::<_, StrategyErrorV1>(sum + number(&l.quantity_btc)?)
                        })
                })
                .transpose()?
                .unwrap_or(0.0);
            let pressure = if q + depth > 0.0 {
                q / (q + depth)
            } else {
                0.0
            };
            innovation += c.binance_trade_pressure_weight_usd
                * pressure
                * if trade.buyer_is_maker { -1.0 } else { 1.0 };
        }
        if let Some(b) = value(&o.binance_depth) {
            let bid = b.bids.iter().try_fold(0.0, |a, p| {
                Ok::<_, StrategyErrorV1>(a + number(&p.quantity_btc)?)
            })?;
            let ask = b.asks.iter().try_fold(0.0, |a, p| {
                Ok::<_, StrategyErrorV1>(a + number(&p.quantity_btc)?)
            })?;
            if bid + ask > 0.0 {
                innovation += c.binance_pressure_weight_usd * (bid - ask) / (bid + ask);
            }
        }
        let tau = (end - now) as f64 / 1000.0;
        let ws = w as f64 / 1000.0;
        let (mu, v, approx) = if w == 0 {
            (s + innovation, sigma * sigma * tau, false)
        } else if tau >= ws {
            (
                s + innovation,
                sigma * sigma * (tau - 2.0 * ws / 3.0),
                false,
            )
        } else {
            let start = end - w;
            let Some((_, mut last)) = self
                .chainlink
                .iter()
                .rev()
                .find(|(t, _)| *t <= start)
                .copied()
            else {
                return Ok(ProbabilityEstimateV1::Unavailable(U::MissingWindowHistory));
            };
            let mut cursor = start;
            let mut integral = 0.0;
            for &(t, p) in self
                .chainlink
                .iter()
                .filter(|(t, _)| *t > start && *t <= now)
            {
                integral += last * (t - cursor) as f64 / 1000.0;
                cursor = t;
                last = p;
            }
            integral += last * (now - cursor) as f64 / 1000.0;
            (
                (integral + tau * (s + innovation)) / ws,
                sigma * sigma * tau.powi(3) / (3.0 * ws * ws),
                true,
            )
        };
        let uncertainty = (v + c.estimation_error_usd.powi(2)).sqrt();
        let z = (uncertainty > 0.0).then(|| (mu - k) / uncertainty);
        // 零方差是退化分布：相等沿平台Up判定，不伪装成信息缺失或除零。
        let up = z
            .map(|z| 0.5 * (1.0 + libm::erf(z / std::f64::consts::SQRT_2)))
            .unwrap_or(if mu >= k { 1.0 } else { 0.0 });
        if !mu.is_finite() || !uncertainty.is_finite() || !up.is_finite() {
            return Err(StrategyErrorV1::Arithmetic("概率计算非有限"));
        }
        Ok(ProbabilityEstimateV1::Known {
            up,
            mean_usd: mu,
            uncertainty_usd: uncertainty,
            standardized_distance: z,
            sigma_usd_per_sqrt_second: sigma,
            binance_innovation_usd: innovation,
            information_age_ms: if value(&o.chainlink_spot).is_some() {
                o.chainlink_spot
                    .direct_anchors
                    .iter()
                    .map(|a| a.received_at_utc_ms)
                    .max()
            } else {
                match rule {
                    BtcSettlementReferenceV1::ChainlinkTwapThirtySeconds => o
                        .chainlink_twap_thirty
                        .direct_anchors
                        .iter()
                        .map(|a| a.received_at_utc_ms)
                        .max(),
                    BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds => o
                        .chainlink_twap_sixty
                        .direct_anchors
                        .iter()
                        .map(|a| a.received_at_utc_ms)
                        .max(),
                    BtcSettlementReferenceV1::ChainlinkSpot => None,
                }
            }
            .and_then(|time| u64::try_from(now - time).ok()),
            temporary_reference: temp,
            approximate_window_integral: approx,
        })
    }
}
