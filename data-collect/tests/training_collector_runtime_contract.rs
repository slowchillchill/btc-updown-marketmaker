use std::fs;
use std::process::Command;

use data_collect::runtime::{
    DEFAULT_TRAINING_DATASET_MAX_BYTES, OPERATOR_INTERVENTION_REQUIRED_EXIT_CODE,
    RESOURCE_HARD_THRESHOLD_EXIT_CODE, collector_exit_code, parse_args,
};
use data_collect::training_runtime::{
    TRAINING_SOURCE_KINDS, is_training_runtime_source_gap_reason,
};
use tempfile::tempdir;

fn training_binary() -> &'static str {
    env!("CARGO_BIN_EXE_data-collect-training-collector")
}

#[test]
fn simplified_training_cli_has_one_dataset_root_and_no_run_or_retention_arguments() {
    let cli = parse_args([
        "--dataset-root",
        "/tmp/data-collect-contract",
        "--collector-region",
        "eu-west-1",
        "--run-forever",
        "--max-dataset-bytes",
        "50000000000",
    ])
    .unwrap();
    assert_eq!(
        cli.dataset_root,
        std::path::Path::new("/tmp/data-collect-contract")
    );
    assert!(cli.run_forever);
    assert_eq!(cli.max_dataset_bytes, DEFAULT_TRAINING_DATASET_MAX_BYTES);

    for obsolete in [
        "--run-id",
        "--retention-delete-mode",
        "--retention-threshold-bytes",
    ] {
        let error = parse_args(["--dataset-root", "/tmp/data", obsolete, "value"])
            .expect_err("obsolete training argument must be rejected");
        assert!(error.to_string().contains("unknown argument"));
    }
}

#[test]
fn reserved_collector_exit_codes_distinguish_capacity_and_operator_intervention() {
    let capacity = std::io::Error::other("dataset_capacity_reached");
    let operator =
        std::io::Error::other("operator_intervention_required:daily manifest identity mismatch");
    let runtime = std::io::Error::other("writer_queue_disconnected:binance_aggtrade");

    assert_eq!(
        collector_exit_code(&capacity),
        RESOURCE_HARD_THRESHOLD_EXIT_CODE
    );
    assert_eq!(
        collector_exit_code(&operator),
        OPERATOR_INTERVENTION_REQUIRED_EXIT_CODE
    );
    assert_eq!(collector_exit_code(&runtime), 1);
}

#[test]
fn help_and_version_fast_paths_are_successful_and_create_nothing() {
    let tmp = tempdir().unwrap();
    let nonexistent_dataset = tmp.path().join("must-not-exist");
    for flag in ["--help", "--version"] {
        let output = Command::new(training_binary())
            .arg(flag)
            .env("HOME", tmp.path())
            .env("DATASET_ROOT", &nonexistent_dataset)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{flag}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!output.stdout.is_empty());
        assert!(!nonexistent_dataset.exists());
    }
    assert!(fs::read_dir(tmp.path()).unwrap().next().is_none());
}

#[test]
fn startup_capacity_gate_exits_75_without_manifest_or_network_startup() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().join("datasets");
    let training_root = dataset_root.join("training");
    fs::create_dir_all(&training_root).unwrap();
    fs::write(training_root.join("existing-byte"), [1_u8]).unwrap();

    let output = Command::new(training_binary())
        .args([
            "--dataset-root",
            dataset_root.to_str().unwrap(),
            "--collector-region",
            "eu-west-1",
            "--max-runtime-secs",
            "1",
            "--max-dataset-bytes",
            "1",
        ])
        .output()
        .unwrap();

    assert_eq!(
        output.status.code(),
        Some(RESOURCE_HARD_THRESHOLD_EXIT_CODE)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("dataset_capacity_reached"));
    assert!(training_root.join("existing-byte").is_file());
    assert!(!training_root.join("typed_raw").exists());
    assert!(!training_root.join("run_manifests").exists());
    assert!(!training_root.join("runtime_artifacts").exists());
}

#[test]
fn training_runtime_sources_match_typed_raw_source_kind_contract() {
    assert!(TRAINING_SOURCE_KINDS.contains(&"binance_depth_snapshot"));
    assert!(TRAINING_SOURCE_KINDS.contains(&"binance_depth_update"));
    assert!(TRAINING_SOURCE_KINDS.contains(&"polymarket_market_metadata"));
    assert!(!TRAINING_SOURCE_KINDS.contains(&"binance_depth"));
}

#[test]
fn recoverable_backpressure_wait_is_not_a_live_source_gap() {
    assert!(!is_training_runtime_source_gap_reason(
        "bounded_queue_backpressure_wait:binance_depth_update"
    ));
    assert!(!is_training_runtime_source_gap_reason(
        "writer_queue_backpressure_wait:polymarket_market"
    ));
    assert!(is_training_runtime_source_gap_reason(
        "bounded_queue_backpressure_timeout:binance_depth_update"
    ));
    assert!(is_training_runtime_source_gap_reason(
        "writer_queue_disconnected:polymarket_market"
    ));
}
