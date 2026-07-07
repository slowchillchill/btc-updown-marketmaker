use data_collect::live_runtime::{LIVE_SOURCES, is_live_runtime_source_gap_reason};
use data_collect::runtime::{
    SourceTaskPlan, TrainingRuntimeConfig, TrainingRuntimeMode, run_training_runtime,
};
use tempfile::tempdir;

#[test]
fn source_task_plan_starts_aux_sources_without_polymarket_discovery() {
    let plan = SourceTaskPlan::four_sources();
    let decisions = plan.startup_decisions(false);

    assert_eq!(
        decisions.get("polymarket_market").map(String::as_str),
        Some("record_source_gap")
    );
    assert_eq!(
        decisions.get("binance_depth_update").map(String::as_str),
        Some("start")
    );
    assert_eq!(
        decisions.get("binance_aggtrade").map(String::as_str),
        Some("start")
    );
    assert_eq!(
        decisions
            .get("polymarket_rtds_chainlink_btc_usd")
            .map(String::as_str),
        Some("start")
    );
}

#[test]
fn live_runtime_sources_match_typed_raw_source_kind_contract() {
    assert!(LIVE_SOURCES.contains(&"binance_depth_snapshot"));
    assert!(LIVE_SOURCES.contains(&"binance_depth_update"));
    assert!(!LIVE_SOURCES.contains(&"binance_depth"));
}

#[test]
fn recoverable_backpressure_wait_is_not_a_live_source_gap() {
    assert!(!is_live_runtime_source_gap_reason(
        "bounded_queue_backpressure_wait:binance_depth_update"
    ));
    assert!(!is_live_runtime_source_gap_reason(
        "writer_queue_backpressure_wait:polymarket_market"
    ));
    assert!(is_live_runtime_source_gap_reason(
        "bounded_queue_backpressure_timeout:binance_depth_update"
    ));
    assert!(is_live_runtime_source_gap_reason(
        "bounded_queue_send_closed:binance_depth_update"
    ));
    assert!(is_live_runtime_source_gap_reason(
        "writer_queue_disconnected:polymarket_market"
    ));
}

#[tokio::test]
async fn synthetic_training_runtime_writes_segment_manifest_and_coverage() {
    let run_root = tempdir().unwrap();
    let dataset_root = run_root.path().join("datasets");
    let config = TrainingRuntimeConfig {
        dataset_root: dataset_root.clone(),
        run_id: "runtime-contract".to_owned(),
        mode: TrainingRuntimeMode::Synthetic,
        max_runtime_secs: Some(1),
        collector_region: Some("local-test".to_owned()),
        code_version: None,
        collector_binary_sha256: None,
        source_config_digest: None,
    };

    let receipt = run_training_runtime(config).await.unwrap();

    assert_eq!(receipt.run_id, "runtime-contract");
    assert!(receipt.run_manifest_path.is_file());
    assert_eq!(receipt.source_coverage.len(), 4);
    assert!(receipt.source_gaps.is_empty());
    assert!(receipt.resource_watermark_recorded);
    assert!(receipt.writer_used_blocking_pool);
    assert!(
        receipt
            .finalized_segments
            .iter()
            .all(|segment| segment.parquet_path.is_file())
    );
    assert!(
        receipt
            .finalized_segments
            .iter()
            .all(|segment| segment.manifest_path.is_file())
    );
    assert!(
        receipt
            .finalized_segments
            .iter()
            .all(|segment| segment.source_kind.starts_with("binance")
                || segment.source_kind.starts_with("polymarket"))
    );
}

#[tokio::test]
async fn live_training_runtime_zero_duration_writes_control_manifest() {
    let run_root = tempdir().unwrap();
    let dataset_root = run_root.path().join("datasets");
    let config = TrainingRuntimeConfig {
        dataset_root,
        run_id: "live-zero-duration-contract".to_owned(),
        mode: TrainingRuntimeMode::Live,
        max_runtime_secs: Some(0),
        collector_region: Some("local-test".to_owned()),
        code_version: None,
        collector_binary_sha256: None,
        source_config_digest: None,
    };

    let receipt = run_training_runtime(config).await.unwrap();

    assert_eq!(receipt.run_id, "live-zero-duration-contract");
    assert!(receipt.run_manifest_path.is_file());
    assert!(receipt.resource_watermark_recorded);
    assert!(receipt.writer_used_blocking_pool);
    assert!(
        receipt
            .source_gaps
            .iter()
            .any(|gap| gap.contains("missing_source_coverage"))
    );
}
