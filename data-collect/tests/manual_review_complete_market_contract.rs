use std::fs;

use data_collect::retention::RetentionDeleteMode;
use data_collect::runtime::{
    TrainingRuntimeConfig, TrainingRuntimeMode, parse_args, run_training_runtime,
};
use tempfile::tempdir;

#[test]
fn manual_review_cli_flag_parses_and_rejects_zero() {
    let cli = parse_args([
        "--dataset-root",
        "/tmp/data-collect-datasets",
        "--run-id",
        "manual-review-contract",
        "--collector-region",
        "eu-west-1",
        "--stop-after-complete-polymarket-market",
        "1",
    ])
    .unwrap();

    assert_eq!(cli.stop_after_complete_polymarket_market, Some(1));
    assert!(!cli.run_forever);
    assert_eq!(cli.retention_delete_mode, RetentionDeleteMode::DryRun);
    assert!(
        parse_args([
            "--dataset-root",
            "/tmp/data-collect-datasets",
            "--run-id",
            "manual-review-contract",
            "--stop-after-complete-polymarket-market",
            "0",
        ])
        .is_err()
    );
}

#[test]
fn production_cli_flags_parse_and_reject_bounded_forever_mix() {
    let cli = parse_args([
        "--dataset-root",
        "/tmp/data-collect-datasets",
        "--run-id",
        "production-contract",
        "--run-forever",
        "--retention-delete-mode",
        "enabled",
        "--retention-threshold-bytes",
        "1024",
    ])
    .unwrap();

    assert!(cli.run_forever);
    assert_eq!(cli.max_runtime_secs, None);
    assert_eq!(cli.retention_delete_mode, RetentionDeleteMode::Enabled);
    assert_eq!(cli.retention_threshold_bytes, Some(1024));

    assert!(
        parse_args([
            "--dataset-root",
            "/tmp/data-collect-datasets",
            "--run-id",
            "bad-production-contract",
            "--run-forever",
            "--max-runtime-secs",
            "7200",
        ])
        .is_err()
    );
}

#[tokio::test]
async fn manual_review_runtime_records_mode_and_shutdown_finalize_reason() {
    let run_root = tempdir().unwrap();
    let dataset_root = run_root.path().join("datasets");
    let receipt = run_training_runtime(TrainingRuntimeConfig {
        dataset_root,
        run_id: "manual-review-contract".to_owned(),
        mode: TrainingRuntimeMode::Synthetic,
        max_runtime_secs: Some(1),
        run_forever: false,
        collector_region: Some("local-test".to_owned()),
        code_version: None,
        collector_binary_sha256: None,
        source_config_digest: None,
        stop_after_complete_polymarket_market: Some(1),
        retention_delete_mode: RetentionDeleteMode::DryRun,
        retention_threshold_bytes: None,
    })
    .await
    .unwrap();

    let run_manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&receipt.run_manifest_path).unwrap()).unwrap();
    assert_eq!(run_manifest["mode"], "manual_review_complete_market");
    assert_eq!(
        run_manifest["manual_review_complete_market"]["target_market_count"],
        1
    );
    assert_eq!(
        run_manifest["manual_review_complete_market"]["stop_reason"],
        "manual_review_complete_market_shutdown"
    );
    assert!(run_manifest["manual_review_complete_market"]["selected_market_slug"].is_null());
    assert!(
        run_manifest["manual_review_complete_market"]["subscribed_asset_ids_for_market"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    for segment in receipt.finalized_segments {
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(segment.manifest_path).unwrap()).unwrap();
        assert_eq!(
            manifest["rollover_reason"],
            "manual_review_complete_market_shutdown"
        );
    }
}

#[test]
fn deploy_docs_include_manual_review_complete_market_command() {
    let package_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = package_root.parent().unwrap();
    let readme = fs::read_to_string(
        workspace_root.join("data-collect/docs/training-collector-deploy-readme.md"),
    )
    .unwrap();
    let systemd = fs::read_to_string(
        workspace_root
            .join("data-collect/deploy/systemd/data-collect-training-collector.service.example"),
    )
    .unwrap();

    for expected in [
        "--stop-after-complete-polymarket-market 1",
        "manual_review_complete_market",
        "manual_review_complete_market_shutdown",
    ] {
        assert!(readme.contains(expected), "README missing {expected}");
    }
    assert!(systemd.contains("# Manual review one-market command:"));
    assert!(systemd.contains("--stop-after-complete-polymarket-market 1"));
}
