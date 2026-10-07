use super::*;

#[test]
fn body_rule_excludes_long_tail_and_preserves_bucket_upper_quantiles() {
    let h = stats::Histogram {
        bins: BTreeMap::from([(100, 90), (1_000_000, 10)]),
    };
    assert_eq!(h.body_normal(), Some(200));
    assert_eq!(h.quantile(95, None), Some(1_001_000));
    let h = stats::Histogram {
        bins: BTreeMap::from([(86_400_000, 1)]),
    };
    assert_eq!(h.quantile(90, None), Some(172_800_000));
}

#[test]
fn complete_candidates_keep_lower_daily_median_and_multiplier_order() {
    let mut days = stats::DailyDistributions::default();
    for (day, lower) in [(0, 100), (1, 300)] {
        for group in ["polymarket", "chainlink", "binance_trade", "binance_depth"] {
            let d = days
                .entry(day)
                .or_default()
                .entry(group.into())
                .or_default();
            d.silence_time_ms.bins.insert(lower, 1);
            d.event_age_time_ms.bins.insert(lower, 1);
        }
    }
    let rows = stats::candidates(&days).unwrap();
    assert_eq!(
        rows.iter().map(|x| x.name.as_str()).collect::<Vec<_>>(),
        [
            "daily-body90-max2x",
            "daily-body90-max4x",
            "daily-body90-max8x"
        ]
    );
    for (row, multiplier) in rows.iter().zip([2, 4, 8]) {
        assert_eq!(row.health.polymarket_silence_normal_ms, 200);
        assert_eq!(row.health.polymarket_silence_ms, 200 * multiplier);
    }
    days.clear();
    assert!(
        stats::candidates(&days)
            .err()
            .unwrap()
            .to_string()
            .contains("no eligible daily body distribution")
    );
}
