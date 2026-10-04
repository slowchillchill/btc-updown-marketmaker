//! 生产现价接纳与逐对方差原语；来源研究复用，市场记忆及回退仍由原调用方负责。
use super::*;

pub fn eligible_chainlink_spot_v1(
    fact: &ObservedFactV1<ChainlinkBtcUsdReferenceV1>,
) -> Option<&ChainlinkBtcUsdReferenceV1> {
    value(fact)
}
pub fn spot_price_number_v1(v: &ExactDecimal) -> Result<f64, StrategyErrorV1> {
    float(&decimal(v)?)
}
pub fn direct_received_time_v1<T>(fact: &ObservedFactV1<T>, now: i64) -> i64 {
    fact.direct_anchors
        .iter()
        .map(|a| a.received_at_utc_ms)
        .max()
        .unwrap_or(now)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SpotSampleAcceptanceV1 {
    Accepted,
    Future,
    SameTime,
    Backward,
}
pub fn spot_sample_acceptance_v1(time: i64, now: i64, last: Option<i64>) -> SpotSampleAcceptanceV1 {
    if time > now {
        SpotSampleAcceptanceV1::Future
    } else if last == Some(time) {
        SpotSampleAcceptanceV1::SameTime
    } else if last.is_some_and(|t| time < t) {
        SpotSampleAcceptanceV1::Backward
    } else {
        SpotSampleAcceptanceV1::Accepted
    }
}
/// 两端来自已接纳现价；按右端纳入，保留跨左界的完整价对。
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct SpotVariancePairV1 {
    pub squared_change_usd: f64,
    pub elapsed_seconds: f64,
}
pub fn spot_variance_pair_v1(
    left: (i64, f64),
    right: (i64, f64),
    from: i64,
) -> Option<SpotVariancePairV1> {
    (right.0 >= from && right.0 > left.0).then(|| SpotVariancePairV1 {
        squared_change_usd: (right.1 - left.1).powi(2),
        elapsed_seconds: (right.0 - left.0) as f64 / 1000.0,
    })
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
pub fn spot_variance_statistics_v1(
    points: impl IntoIterator<Item = (i64, f64)>,
    from: i64,
) -> SpotVarianceStatisticsV1 {
    let mut result = SpotVarianceStatisticsV1::default();
    let mut previous = None;
    for point in points {
        if let Some(left) = previous
            && let Some(pair) = spot_variance_pair_v1(left, point, from)
        {
            result.include(pair);
        }
        previous = Some(point);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn production_acceptance_right_endpoint_and_absent_vs_zero() {
        let mut accepted = Vec::new();
        for point in [
            (0, 100.0),
            (1000, 104.0),
            (1000, 200.0),
            (500, 300.0),
            (2000, 103.0),
        ] {
            if spot_sample_acceptance_v1(point.0, 2000, accepted.last().map(|p: &(i64, f64)| p.0))
                == SpotSampleAcceptanceV1::Accepted
            {
                accepted.push(point);
            }
        }
        assert_eq!(accepted, vec![(0, 100.0), (1000, 104.0), (2000, 103.0)]);
        let s = spot_variance_statistics_v1(accepted, 1000);
        assert_eq!(s.squared_change_sum_usd_squared, 17.0);
        assert_eq!(s.elapsed_seconds, 2.0);
        assert_eq!(
            spot_variance_statistics_v1([(2000, 103.0)], 1000).sigma_usd_per_sqrt_second(),
            None
        );
        assert_eq!(
            spot_variance_statistics_v1([(0, 100.0), (1000, 100.0)], 1000)
                .sigma_usd_per_sqrt_second(),
            Some(0.0)
        );
        assert_eq!(
            spot_sample_acceptance_v1(3000, 2000, None),
            SpotSampleAcceptanceV1::Future
        );
        assert_eq!(
            spot_variance_statistics_v1([(900, 100.0), (1500, 101.0), (2500, 103.0)], 1000)
                .elapsed_seconds,
            1.6
        );
    }
}
