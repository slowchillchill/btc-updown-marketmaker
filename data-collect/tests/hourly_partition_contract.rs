use data_collect::hourly_partition::{HourlyPartitionPolicy, is_legacy_source_first_partition};
use tempfile::tempdir;

#[test]
fn hourly_partition_uses_day_first_source_kind_and_only_running_and_final_paths() {
    let tmp = tempdir().unwrap();
    let policy = HourlyPartitionPolicy::new(tmp.path()).unwrap();
    let partition = policy
        .partition("polymarket_market", 1_783_407_604_599, None)
        .unwrap();

    assert_eq!(partition.utc_day, "2026-07-07");
    assert_eq!(partition.utc_hour, "2026-07-07T07");
    assert_eq!(partition.hour_start_utc_ms, 1_783_407_600_000);
    assert_eq!(partition.hour_end_utc_ms, 1_783_411_200_000);
    assert!(
        partition
            .partition_path
            .ends_with("training/typed_raw/utc_day=2026-07-07/source_kind=polymarket_market")
    );
    assert!(
        partition
            .running_path
            .ends_with("polymarket_market_07.parquet.running")
    );
    assert!(
        partition
            .final_path
            .ends_with("polymarket_market_07.parquet")
    );
}

#[test]
fn hourly_partition_rolls_at_utc_hour_boundary_and_supports_restart_parts() {
    let tmp = tempdir().unwrap();
    let policy = HourlyPartitionPolicy::new(tmp.path()).unwrap();
    let last_ms = policy
        .partition("binance_depth_update", 1_783_411_199_999, None)
        .unwrap();
    let first_ms = policy
        .partition("binance_depth_update", 1_783_411_200_000, None)
        .unwrap();
    let part = policy
        .partition("binance_depth_update", 1_783_411_200_000, Some(2))
        .unwrap();

    assert_eq!(last_ms.utc_hour, "2026-07-07T07");
    assert_eq!(first_ms.utc_hour, "2026-07-07T08");
    assert!(
        part.final_path
            .ends_with("binance_depth_update_08-part-2.parquet")
    );
    assert!(
        part.running_path
            .ends_with("binance_depth_update_08-part-2.parquet.running")
    );
}

#[test]
fn hourly_partition_rejects_unsafe_source_kind_and_marks_legacy_source_first() {
    let tmp = tempdir().unwrap();
    let policy = HourlyPartitionPolicy::new(tmp.path()).unwrap();

    assert!(
        policy
            .partition("../evil", 1_783_411_200_000, None)
            .is_err()
    );
    assert!(is_legacy_source_first_partition(&tmp.path().join(
        "training/typed_raw/source_kind=polymarket_market/utc_day=2026-07-07"
    )));
    assert!(!is_legacy_source_first_partition(&tmp.path().join(
        "training/typed_raw/utc_day=2026-07-07/source_kind=polymarket_market"
    )));
}
