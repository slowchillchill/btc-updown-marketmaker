use data_collect::deployment::{
    CollectorBinaryRole, CollectorStartupReceipt, DeploymentRuntimeConfig, validate_runtime_config,
};
use tempfile::tempdir;

#[test]
fn binary_roles_are_isolated_by_output_roots_and_runtime_contracts() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();

    let training = DeploymentRuntimeConfig::for_role(
        CollectorBinaryRole::TrainingCold,
        dataset_root.clone(),
        "training-run",
    )
    .unwrap();
    assert!(training.output_root.ends_with("training/typed_raw"));

    let paper = DeploymentRuntimeConfig::for_role(
        CollectorBinaryRole::PaperHot,
        dataset_root.clone(),
        "paper-run",
    )
    .unwrap();
    assert!(paper.output_root.ends_with("paper/data_collect_handoff"));

    let live =
        DeploymentRuntimeConfig::for_role(CollectorBinaryRole::LiveHot, dataset_root, "live-run")
            .unwrap();
    assert!(live.output_root.ends_with("live/data_collect_handoff"));
}

#[test]
fn deployment_config_rejects_path_traversal_and_missing_run_id() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();
    let mut config = DeploymentRuntimeConfig::for_role(
        CollectorBinaryRole::TrainingCold,
        dataset_root.clone(),
        "training-run",
    )
    .unwrap();

    config.output_root = dataset_root.join("..").join("outside");
    assert!(validate_runtime_config(&config).is_err());

    let config =
        DeploymentRuntimeConfig::for_role(CollectorBinaryRole::PaperHot, dataset_root, "  ");
    assert!(config.is_err());
}

#[test]
fn runtime_receipt_records_binary_and_host_identity() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();
    let config =
        DeploymentRuntimeConfig::for_role(CollectorBinaryRole::TrainingCold, dataset_root, "run-1")
            .unwrap();

    let receipt = CollectorStartupReceipt::new(
        &config,
        "git-commit",
        "binary-sha256",
        "config-digest",
        "collector-host",
        "eu-west-1",
        "chrony synchronized",
    )
    .unwrap();

    assert_eq!(receipt.binary_role, "training_cold");
    assert_eq!(receipt.git_commit, "git-commit");
    assert_eq!(receipt.binary_sha256, "binary-sha256");
    assert_eq!(receipt.config_digest, "config-digest");
    assert!(receipt.dataset_root_canonical.is_absolute());
    assert_eq!(receipt.collector_host_id, "collector-host");
    assert_eq!(receipt.collector_region, "eu-west-1");
    assert_eq!(receipt.host_clock_sync_status, "chrony synchronized");
}
