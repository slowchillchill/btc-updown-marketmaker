use contracts::{ExactDecimal, Sha256Hex};
use num_bigint::BigUint;
use rust_decimal::Decimal;
use sim_system::platform_rules::*;
#[test]
fn public_multilevel_scene_reuses_original_total_rounding_and_segment_allocations() {
    let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/platform-rule-snapshot-v3-gate-b.json"),
        expected_file_sha256: Sha256Hex(
            "d63c0e7b09edc4b2c4c3f0295f806b25f6f5fd03663cdf60ff062117e53e4e54".into(),
        ),
        replay_causal_start_utc_ms: i64::MAX,
    })
    .unwrap();
    let before = (*rules).clone();
    let estimate = rules
        .market_buy_scenario_fee_estimate_v1(&[
            (4_000_000, Decimal::new(3, 1)),
            (6_000_000, Decimal::new(4, 1)),
        ])
        .unwrap();
    assert_eq!(estimate.fee_1e5_units, BigUint::from(15960u32));
    assert_eq!(
        estimate.segment_fee_1e5_units,
        vec![BigUint::from(5880u32), BigUint::from(10080u32)]
    );
    // 两段各0.0000025025，总额0.000005005舍入一次为0.00001；逐段舍入会误报0。
    let tiny = rules
        .market_buy_scenario_fee_estimate_v1(&[
            (143, Decimal::new(5, 1)),
            (143, Decimal::new(5, 1)),
        ])
        .unwrap();
    assert_eq!(tiny.fee_1e5_units, BigUint::from(1u8));
    assert_eq!(
        tiny.segment_fee_1e5_units,
        vec![BigUint::from(1u8), BigUint::from(0u8)]
    );
    let single = rules
        .market_buy_scenario_fee_estimate_v1(&[(100_000_000, Decimal::new(9, 1))])
        .unwrap();
    assert_eq!(
        single.fee_1e5_units,
        rules
            .market_buy_fee_estimate_v1(&BigUint::from(10000u32), 90, &ExactDecimal("0.01".into()))
            .unwrap()
            .fee_1e5_units
    );
    assert_eq!(*rules, before);
}
