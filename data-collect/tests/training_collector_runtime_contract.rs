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
        stop_after_complete_polymarket_market: None,
    };

    let receipt = run_training_runtime(config).await.unwrap();

    assert_eq!(receipt.run_id, "runtime-contract");
    assert!(receipt.run_manifest_path.is_file());
    let run_manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&receipt.run_manifest_path).unwrap()).unwrap();
    let retention_path = run_manifest["retention_policy_receipt_path"]
        .as_str()
        .expect("run manifest records retention policy receipt path");
    assert!(dataset_root.join(retention_path).is_file());
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
        stop_after_complete_polymarket_market: None,
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

    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&receipt.run_manifest_path).unwrap()).unwrap();
    let artifact_dir = manifest["artifact_dir"]
        .as_str()
        .expect("run manifest records artifact dir");
    let artifact_dir = std::path::PathBuf::from(artifact_dir);
    assert!(artifact_dir.is_dir());
    for forbidden in [
        "source_coverage.json",
        "source_gaps.json",
        "rollover_edges.json",
        "write_receipts.jsonl",
        "queue_samples.jsonl",
    ] {
        assert!(
            !artifact_dir.join(forbidden).exists(),
            "{forbidden} must not be produced in the default runtime artifact dir"
        );
    }
    assert!(
        std::fs::read_dir(&artifact_dir).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("retention_after_finalize_")),
        "source-specific retention_after_finalize receipts must not be produced"
    );

    assert!(manifest["source_coverage"].is_array());
    assert!(manifest["source_gaps"].is_array());
    assert!(manifest["rollover_edges"].is_array());
    assert!(manifest["finalized_segments"].is_array());
    assert!(
        manifest["source_writer_stats"].is_array(),
        "run manifest must carry aggregated writer stats instead of write_receipts.jsonl"
    );
    assert_eq!(
        manifest["queue_summary"]["source_event_queue"]["queue_name"],
        "source_event_queue"
    );
    assert_eq!(
        manifest["queue_summary"]["writer_chunk_queue"]["queue_name"],
        "writer_chunk_queue"
    );

    let stages = manifest["shutdown_stage_timings"]
        .as_object()
        .expect("shutdown_stage_timings must be a stage-name keyed object");
    for required_stage in [
        "source_task_stop",
        "event_queue_drain",
        "flush_batches",
        "writer_finalize",
        "retention_shutdown_scan",
        "run_manifest_atomic_write",
        "post_manifest_cleanup",
    ] {
        let stage = stages
            .get(required_stage)
            .unwrap_or_else(|| panic!("missing shutdown stage {required_stage}"));
        assert!(stage["started_utc_ms"].is_i64());
        assert!(stage["ended_utc_ms"].is_i64());
        assert!(stage["duration_ms"].is_u64());
        assert!(matches!(
            stage["status"].as_str(),
            Some("success" | "skipped" | "timeout" | "failed")
        ));
        assert!(stage.get("error").is_some());
    }
    assert_eq!(
        stages["run_manifest_atomic_write"]["status"], "success",
        "manifest atomic write must be explicitly recorded as successful"
    );
    assert_eq!(
        stages["run_manifest_atomic_write"]["manifest_path"],
        receipt.run_manifest_path.to_string_lossy().as_ref(),
        "manifest atomic write stage must record the final manifest path"
    );
    let retention_budget = manifest["shutdown_retention_scan_budget_ms"]
        .as_u64()
        .expect("run manifest records retention shutdown scan budget");
    if stages["retention_shutdown_scan"]["status"] == "success" {
        assert!(
            stages["retention_shutdown_scan"]["duration_ms"]
                .as_u64()
                .unwrap()
                <= retention_budget,
            "successful shutdown retention scan must fit the fixed budget"
        );
    }
    assert!(
        manifest["resource_watermark"]["unavailable"]
            .as_array()
            .is_none_or(|items| items.is_empty()),
        "runtime resource_watermark must not carry noisy not-measured placeholders"
    );
}
