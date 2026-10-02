#[allow(dead_code)]
#[path = "../../tests/common/mod.rs"]
mod common;
use baseline_mm::*;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{path::Path, process::Command};

#[test]
fn actual_baseline_command_applies_explicit_values_and_false_then_records_effective_config() {
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = common::setup(temp.path());
    let strategy = ComplementaryPairConfigV1::parse_toml(include_str!(
        "../../../baseline-mm/configs/strategies/complementary_pair/synthetic-engineering.toml"
    ))
    .unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .find(|path| path.join("sim-system").is_dir())
        .unwrap();
    let rule_path = root.join("sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json");
    let mut run = serde_json::to_value(&config).unwrap();
    run.as_object_mut().unwrap().remove("strategy");
    let command_config = json!({"run": run, "rules": {"snapshot_path":rule_path,"expected_file_sha256":format!("{:x}",Sha256::digest(std::fs::read(&rule_path).unwrap())),"replay_causal_start_utc_ms":common::BASE}, "strategy": strategy,"request_namespace":"actual-baseline-command"});
    let path = temp.path().join("command.json");
    std::fs::write(&path, serde_json::to_vec(&command_config).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_backtest-cli"))
        .args([
            "--config",
            path.to_str().unwrap(),
            "--strategy.repair.enable_active_repair",
            "false",
            "--strategy.quote.base_pair_shares",
            "12",
            "--strategy.probability.initial_sigma_usd_per_sqrt_second",
            "3.5",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["end_reason"], "RangeEnd");
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&config.metadata_path).unwrap()).unwrap();
    let descriptor = &metadata["config"]["strategy"];
    assert_eq!(descriptor["strategy_id"], "complementary_pair");
    assert_eq!(descriptor["strategy_rule_version"], "candidate_v1");
    assert_eq!(
        descriptor["effective_parameters"]["repair"]["enable_active_repair"],
        false
    );
    assert_eq!(
        descriptor["effective_parameters"]["quote"]["base_pair_shares"],
        "12"
    );
    assert_eq!(
        descriptor["effective_parameters"]["probability"]["initial_sigma_usd_per_sqrt_second"],
        3.5
    );
    assert_eq!(
        descriptor["effective_parameters"]["repair"]["maker_fill_weight"],
        command_config["strategy"]["repair"]["maker_fill_weight"]
    );
}
