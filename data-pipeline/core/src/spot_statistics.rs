//! 共享 spot 逐对数学原语；接纳时钟由各版本调用方维护。
use contracts::{SpotVariancePairV1, SpotVarianceStatisticsV1};
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
