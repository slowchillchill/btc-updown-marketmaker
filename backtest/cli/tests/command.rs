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
    let mut command_config = json!({"run": run, "rules": {"snapshot_path":rule_path,"expected_file_sha256":format!("{:x}",Sha256::digest(std::fs::read(&rule_path).unwrap())),"replay_causal_start_utc_ms":common::BASE}, "strategy": strategy,"request_namespace":"actual-baseline-command"});
    // CLI 必填值不要求文件冗余占位；同时补齐一个字段和整个缺失区段。
    command_config["strategy"]["quote"]
        .as_object_mut()
        .unwrap()
        .remove("base_pair_shares");
    let risk = command_config["strategy"]
        .as_object_mut()
        .unwrap()
        .remove("risk")
        .unwrap();
    let path = temp.path().join("command.json");
    std::fs::write(&path, serde_json::to_vec(&command_config).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_backtest-cli"))
        .args([
            "--config",
            path.to_str().unwrap(),
            "--strategy.repair.enable_active_repair",
            "false",
            "--strategy.health.chainlink_silence_ms",
            "1234",
            "--strategy.health.chainlink_silence_normal_ms",
            "123",
            "--strategy.quote.base_pair_shares",
            "12",
            "--strategy.probability.initial_sigma_usd_per_sqrt_second",
            "3.5",
        ])
        .args([
            "--strategy.risk.per_market_orphan_limit_pusd",
            risk["per_market_orphan_limit_pusd"].as_str().unwrap(),
            "--strategy.risk.account_orphan_limit_pusd",
            risk["account_orphan_limit_pusd"].as_str().unwrap(),
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
        descriptor["effective_parameters"]["health"]["chainlink_silence_ms"],
        1234
    );
    assert_eq!(
        descriptor["effective_parameters"]["health"]["chainlink_silence_normal_ms"],
        123
    );
    assert_eq!(descriptor["effective_parameters"]["risk"], risk);
    assert_eq!(
        descriptor["effective_parameters"]["repair"]["maker_fill_weight"],
        command_config["strategy"]["repair"]["maker_fill_weight"]
    );
}

#[test]
fn prepare_mode_publishes_without_strategy_or_account_and_reuse_mode_uses_only_the_descriptor() {
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = common::setup(temp.path());
    let prepare_path = temp.path().join("preparation.json");
    std::fs::write(
        &prepare_path,
        serde_json::to_vec(&config.preparation).unwrap(),
    )
    .unwrap();
    let binary = env!("CARGO_BIN_EXE_backtest-cli");
    let prepare = Command::new(binary)
        .args([
            "--mode",
            "prepare",
            "--config",
            prepare_path.to_str().unwrap(),
            "--progress-interval-seconds",
            "0",
        ])
        .output()
        .unwrap();
    assert!(
        prepare.status.success(),
        "{}",
        String::from_utf8_lossy(&prepare.stderr)
    );
    assert!(prepare.stderr.is_empty());
    assert!(!config.sim_log.exists() && !config.noe_root.exists() && !config.ledger_root.exists());
    let receipt: serde_json::Value = serde_json::from_slice(&prepare.stdout).unwrap();
    let description = receipt["prepared_description"].as_str().unwrap();
    let material = backtest::PreparedHistoricalMaterialV1::read(Path::new(description)).unwrap();
    let material_before = std::fs::read(description).unwrap();
    std::fs::rename(temp.path().join("inputs"), temp.path().join("moved-raw")).unwrap();
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .find(|p| p.join("sim-system").is_dir())
        .unwrap();
    let rule_path =
        workspace.join("sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json");
    let strategy = ComplementaryPairConfigV1::parse_toml(include_str!(
        "../../../baseline-mm/configs/strategies/complementary_pair/synthetic-engineering.toml"
    ))
    .unwrap();
    let mut results = Vec::new();
    for name in ["first-reuse", "second-reuse"] {
        let output_root = temp.path().join(name);
        std::fs::create_dir(&output_root).unwrap();
        let mut run = backtest::HistoricalPreparedRunConfigV1::from(&config);
        run.sim_log = output_root.join("sim.jsonl");
        run.noe_root = output_root.join("noe");
        run.ledger_root = output_root.join("ledger");
        run.metadata_path = output_root.join("run.json");
        run.result_path = output_root.join("result.json");
        let mut value = serde_json::to_value(&run).unwrap();
        value.as_object_mut().unwrap().remove("strategy");
        let command_path = output_root.join("command.json");
        std::fs::write(&command_path, serde_json::to_vec(&json!({"run":value, "strategy":strategy, "rules":{"snapshot_path":rule_path,"expected_file_sha256":format!("{:x}",Sha256::digest(std::fs::read(&rule_path).unwrap())), "replay_causal_start_utc_ms":common::BASE}, "request_namespace":"reused-cli-engineering"})).unwrap()).unwrap();
        let mut command = Command::new(binary);
        command.args([
            "--mode",
            "reuse",
            "--prepared",
            description,
            "--config",
            command_path.to_str().unwrap(),
            "--strategy.repair.enable_active_repair",
            "false",
        ]);
        if name == "first-reuse" {
            command.args(["--progress-interval-seconds", "0"]);
        } else {
            command.stderr(
                std::fs::OpenOptions::new()
                    .write(true)
                    .open("/dev/full")
                    .unwrap(),
            );
        }
        let result = command.output().unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(result.stderr.is_empty());
        let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
        assert_eq!(report["end_reason"], "RangeEnd");
        let metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&run.metadata_path).unwrap()).unwrap();
        assert!(metadata["config"].get("preparation").is_none());
        assert_eq!(
            metadata["prepared_material"],
            serde_json::to_value(&material).unwrap()
        );
        assert_eq!(
            metadata["config"]["strategy"]["effective_parameters"]["repair"]["enable_active_repair"],
            false
        );
        results.push(report);
    }
    assert_eq!(results[0], results[1]);
    assert_eq!(std::fs::read(description).unwrap(), material_before);
}
