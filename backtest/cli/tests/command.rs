#[path = "../../tests/common/default.rs"]
mod fixture_default;
#[path = "../../tests/common/fixture_material.rs"]
mod fixture_material;
mod common {
    pub(crate) use super::fixture_default::*;
    pub(crate) use super::fixture_material::*;
}

use baseline_mm::*;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{path::Path, process::Command};

fn current_setup(
    root: &Path,
) -> (
    backtest::HistoricalRunConfigV1,
    std::sync::Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
) {
    let (mut config, rules) = common::setup(root);
    config.strategy.strategy_id = STRATEGY_ID.into();
    config.strategy.strategy_rule_version = RULE_VERSION.into();
    (config, rules)
}
#[test]
fn actual_baseline_command_applies_explicit_values_and_false_then_records_effective_config() {
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = current_setup(temp.path());
    let strategy = ComplementaryPairConfigV2::parse_toml(include_str!(
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
    assert_eq!(descriptor["strategy_rule_version"], "candidate_v2");
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
    let (config, _) = current_setup(temp.path());
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
    let strategy = ComplementaryPairConfigV2::parse_toml(include_str!(
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

#[test]
fn day_command_uses_full_candidate_configuration_and_has_no_resume_outputs() {
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = current_setup(temp.path());
    let strategy = ComplementaryPairConfigV2::parse_toml(include_str!(
        "../../../baseline-mm/configs/strategies/complementary_pair/synthetic-engineering.toml"
    ))
    .unwrap();
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .find(|p| p.join("sim-system").is_dir())
        .unwrap();
    let rules = workspace.join("sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json");
    let mut run =
        serde_json::to_value(backtest::HistoricalPreparedRunConfigV1::from(&config)).unwrap();
    run.as_object_mut().unwrap().remove("strategy");
    let value = json!({"rules":{"snapshot_path":rules,"expected_file_sha256":format!("{:x}",Sha256::digest(std::fs::read(&rules).unwrap())),"replay_causal_start_utc_ms":common::BASE},
        "parallel_dates":2,"days":[{"preparation":config.preparation,"candidates":[{"run":run,"strategy":strategy,"request_namespace":"day-cli-engineering"}],
        "producer_lead_groups":1}]});
    let path = temp.path().join("day-command.json");
    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    let execute = || {
        Command::new(env!("CARGO_BIN_EXE_backtest-cli"))
            .args([
                "--mode",
                "day",
                "--config",
                path.to_str().unwrap(),
                "--progress-interval-seconds",
                "0",
                "--strategy.repair.enable_active_repair",
                "false",
            ])
            .output()
            .unwrap()
    };
    let first = execute();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(
        first["days"][0]["candidates"][0]["report"]["end_reason"],
        "RangeEnd"
    );
    assert!(first["days"][0]["candidates"][0].get("reused").is_none());
    assert!(first["days"][0].get("published_checkpoint").is_none());
    assert!(!temp.path().join("day-checkpoints").exists());
    assert_eq!(first["summaries"][0]["days"], 1);
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&config.metadata_path).unwrap()).unwrap();
    assert_eq!(
        metadata["config"]["strategy_request_namespace"],
        "day-cli-engineering"
    );
    assert_eq!(
        metadata["config"]["strategy"]["effective_parameters"]["repair"]["enable_active_repair"],
        false
    );
    assert!(
        !config.preparation.work_root.join("public").exists(),
        "新day调用不创建完整公共录制"
    );
    let sim = std::fs::read(&config.sim_log).unwrap();
    let second = execute();
    assert!(
        !second.status.success(),
        "已有运行产物不能作为新运行自动恢复或复用"
    );
    assert_eq!(std::fs::read(&config.sim_log).unwrap(), sim);
}

#[test]
fn day_session_failure_outputs_one_partial_json_and_nonzero_status() {
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = current_setup(temp.path());
    let strategy = ComplementaryPairConfigV2::parse_toml(include_str!(
        "../../../baseline-mm/configs/strategies/complementary_pair/synthetic-engineering.toml"
    ))
    .unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let rule_path = root.join("sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json");
    let mut run =
        serde_json::to_value(backtest::HistoricalPreparedRunConfigV1::from(&config)).unwrap();
    run["session"]["session_seed"] = json!("old arbitrary seed");
    let command = json!({"rules":{"snapshot_path":rule_path,"expected_file_sha256":format!("{:x}",Sha256::digest(std::fs::read(&rule_path).unwrap())),"replay_causal_start_utc_ms":common::BASE},"days":[{"preparation":config.preparation,"candidates":[{"run":run,"strategy":strategy,"request_namespace":"failure-json"}]}]});
    let path = temp.path().join("day.json");
    std::fs::write(&path, serde_json::to_vec(&command).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_backtest-cli"))
        .args([
            "--mode",
            "day",
            "--config",
            path.to_str().unwrap(),
            "--research-summary",
            "--progress-interval-seconds",
            "0",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["status"], "failed");
    assert_eq!(report["days"][0]["candidates"][0]["outcome"], "NoReport");
    assert!(report["days"][0]["candidates"][0]["report"].is_null());
    assert!(report["error"].as_str().unwrap().contains("session_seed"));
}

#[test]
fn research_script_summarizes_missing_reports_and_preserves_original_exit_code() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let config = temp.path().join("config.json");
    std::fs::write(&config,serde_json::to_vec(&json!({"days":[{"preparation":{"scope":{"received_start_utc_ms":0,"received_end_utc_ms":43200000}},"candidates":[{"run":{"session":{"run_id":"missing-report"},"metadata_path":temp.path().join("new-candidate/run.json")}}]}]})).unwrap()).unwrap();
    let fixture = temp.path().join("fixture-cli.sh");
    std::fs::write(&fixture,"#!/bin/sh\nprintf '%s\\n' '{\"status\":\"failed\",\"error\":\"original\",\"days\":[{\"public_applied_inputs\":0,\"candidates\":[{\"run_id\":\"missing-report\",\"initial_available_pusd\":\"2000\",\"outcome\":\"NoReport\",\"report\":null}]}]}'\nexit 37\n").unwrap();
    std::fs::set_permissions(&fixture, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output_dir = temp.path().join("out");
    let status = Command::new("bash")
        .arg(root.join("scripts/run_research.sh"))
        .args([
            fixture.as_os_str(),
            config.as_os_str(),
            output_dir.as_os_str(),
        ])
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(37));
    assert!(temp.path().join("new-candidate").is_dir());
    let summary: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output_dir.join("summary.json")).unwrap()).unwrap();
    assert_eq!(summary["error"], "original");
    assert_eq!(summary["all_candidates_complete_window"], false);
    assert_eq!(summary["candidates"][0]["outcome"], "NoReport");
    // 原业务错误优先于汇总错误；空日期也不会伪造完整结果。
    std::fs::write(
        &fixture,
        "#!/bin/sh\nprintf '%s\\n' '{\"status\":\"failed\",\"days\":[]}'\nexit 37\n",
    )
    .unwrap();
    let status = Command::new("bash")
        .arg(root.join("scripts/run_research.sh"))
        .args([
            fixture.as_os_str(),
            config.as_os_str(),
            output_dir.as_os_str(),
        ])
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(37));
}

#[test]
fn day_result_output_failure_preserves_completed_business_json_and_nonzero_status() {
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = current_setup(temp.path());
    std::fs::create_dir(&config.result_path).unwrap();
    let strategy = ComplementaryPairConfigV2::parse_toml(include_str!(
        "../../../baseline-mm/configs/strategies/complementary_pair/synthetic-engineering.toml"
    ))
    .unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let rules = root.join("sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json");
    let command = json!({"rules":{"snapshot_path":rules,"expected_file_sha256":format!("{:x}",Sha256::digest(std::fs::read(&rules).unwrap())),"replay_causal_start_utc_ms":common::BASE},"days":[{"preparation":config.preparation,"candidates":[{"run":backtest::HistoricalPreparedRunConfigV1::from(&config),"strategy":strategy,"request_namespace":"tail-output-failure"}]}]});
    let path = temp.path().join("day-result-output-error.json");
    std::fs::write(&path, serde_json::to_vec(&command).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_backtest-cli"))
        .args([
            "--mode",
            "day",
            "--config",
            path.to_str().unwrap(),
            "--research-summary",
            "--progress-interval-seconds",
            "0",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["status"], "failed");
    let candidate = &result["days"][0]["candidates"][0];
    assert_eq!(candidate["outcome"], "OriginalFailure");
    assert!(candidate.get("reused").is_none());
    assert_eq!(candidate["report"]["end_reason"], "RangeEnd");
    assert_eq!(candidate["report"]["recording_drained"], true);
    assert!(candidate["report"]["account_economics_memory"].is_object());
    assert!(candidate["report"]["research"]["final_snapshot"].is_object());
    assert!(result["error"].as_str().unwrap().contains("IsADirectory"));
}

#[test]
fn explicit_old_rule_is_rejected_before_any_run_in_both_command_loaders() {
    for day in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let config = temp.path().join("old.json");
        let run = json!({"strategy":{"strategy_rule_version":"candidate_v1"}});
        let value = if day {
            json!({"days":[{"candidates":[{"run":run}]}]})
        } else {
            json!({"run":run})
        };
        std::fs::write(&config, serde_json::to_vec(&value).unwrap()).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_backtest-cli"));
        if day {
            command.args(["--mode", "day"]);
        }
        let output = command
            .args(["--config", config.to_str().unwrap()])
            .output()
            .unwrap();
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(
            error.contains("candidate_v1") && error.contains("candidate_v2"),
            "{error}"
        );
    }
}
#[test]
fn removed_overfill_field_requires_explicit_migration() {
    let mut config = serde_json::to_value(
        ComplementaryPairConfigV2::parse_toml(include_str!(
            "../../../baseline-mm/configs/strategies/complementary_pair/synthetic-engineering.toml"
        ))
        .unwrap(),
    )
    .unwrap();
    config["repair"]["maximum_overfill_shares"] = json!("5");
    let error = serde_json::from_value::<ComplementaryPairConfigV2>(config).unwrap_err();
    assert!(error.to_string().contains("maximum_overfill_shares"));
}
