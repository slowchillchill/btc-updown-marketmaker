use std::fs;
use std::process::Command;

use data_collect::runtime::{
    OPERATOR_INTERVENTION_REQUIRED_EXIT_CODE, RESOURCE_HARD_THRESHOLD_EXIT_CODE,
    collector_exit_code, parse_args,
};
use data_collect::training_runtime::{
    is_training_runtime_source_gap_reason, training_source_kinds,
};
use tempfile::tempdir;

fn training_binary() -> &'static str {
    env!("CARGO_BIN_EXE_data-collect-training-collector")
}

#[test]
fn simplified_training_cli_has_one_dataset_root_and_no_run_or_retention_arguments() {
    let service = include_str!("../deploy/systemd/data-collect-training-collector.service");
    let command = service
        .lines()
        .find_map(|line| line.strip_prefix("ExecStart="))
        .unwrap();
    let cli = parse_args(command.split_whitespace().skip(1)).unwrap();
    assert_eq!(
        cli.dataset_root,
        std::path::Path::new("/home/ubuntu/btc-updown-marketmaker/datasets")
    );
    assert!(cli.run_forever);

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
        if flag == "--help" {
            assert!(String::from_utf8_lossy(&output.stdout).contains("--dataset-root"));
        }
        assert!(!nonexistent_dataset.exists());
    }
    assert!(fs::read_dir(tmp.path()).unwrap().next().is_none());
}

#[test]
fn startup_path_check_rejects_training_root_symlink_before_network_startup() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().join("datasets");
    let training_root = dataset_root.join("training");
    fs::create_dir_all(&dataset_root).unwrap();
    let outside = tmp.path().join("outside");
    fs::create_dir_all(&outside).unwrap();
    fs::write(outside.join("existing-byte"), [1_u8]).unwrap();
    std::os::unix::fs::symlink(&outside, &training_root).unwrap();

    let output = Command::new(training_binary())
        .args([
            "--dataset-root",
            dataset_root.to_str().unwrap(),
            "--collector-region",
            "eu-west-1",
            "--max-runtime-secs",
            "1",
        ])
        .output()
        .unwrap();

    assert_eq!(
        output.status.code(),
        Some(OPERATOR_INTERVENTION_REQUIRED_EXIT_CODE)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("dataset_path_check_failed"));
    assert!(training_root.join("existing-byte").is_file());
    assert!(!training_root.join("typed_raw").exists());
    assert!(!training_root.join("run_manifests").exists());
    assert!(!training_root.join("runtime_artifacts").exists());
}

#[test]
fn training_runtime_sources_match_typed_raw_source_kind_contract() {
    assert!(training_source_kinds().contains(&"binance_depth_snapshot"));
    assert!(training_source_kinds().contains(&"binance_depth_update"));
    assert!(training_source_kinds().contains(&"polymarket_market_metadata"));
    assert!(training_source_kinds().contains(&"polymarket_rtds_chainlink_twap"));
    assert!(!training_source_kinds().contains(&"binance_depth"));
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
