#[path = "../../tests/common/default.rs"]
mod fixture_default;
#[path = "../../tests/common/fixture_material.rs"]
mod fixture_material;
mod common {
    pub(crate) use super::{fixture_default::*, fixture_material::*};
}
use backtest::*;
use contracts::*;
use data_pipeline_training::{day_producer::DayProducerV1, day_stream::DayIngressV1};
use serde_json::{Value, json};
use std::{fs, process::Command};
fn first_group(config: &HistoricalPreparationConfigV1) -> PublicGroupReferenceV1 {
    let inputs =
        data_pipeline_training::freeze_public_source_input_set_v1(&config.scope.manifest_paths)
            .unwrap();
    let admitted = inputs.admitted_daily_inputs();
    let files = admitted.iter().flat_map(|d| d.files()).collect::<Vec<_>>();
    let ingress = DayIngressV1::open(
        &files,
        config.header.input_stream_id.clone(),
        config.read_page_rows,
        config.max_read_batch_bytes,
        config.scope.received_start_utc_ms..config.scope.received_end_utc_ms,
        vec![],
        config.canonical.max_epoch_count_per_source,
    )
    .unwrap();
    let mut producer = DayProducerV1::new(
        ingress,
        config.effective_header(),
        Sha256Hex(inputs.input_set_identity_sha256().into()),
        config.hidden_stream_id.clone(),
    )
    .unwrap();
    let applied = producer.next_applied().unwrap().unwrap().applied;
    let g = &applied.closed_groups[0];
    PublicGroupReferenceV1 {
        input_stream_id: applied.applied_prefix.input_stream_id.clone(),
        applied_prefix: applied.applied_prefix,
        group_position: g.group_position,
        group_id: g.group_id.clone(),
        closed_at_input_position: g.closed_at_input_position,
    }
}
#[test]
fn command_exports_four_requests_across_three_streams_and_stops_before_input() {
    let root = tempfile::tempdir().unwrap();
    let (config, _) = common::setup(root.path());
    let mut preparations = vec![];
    let mut targets = vec![];
    for stream in ["stream-a", "stream-b", "stream-c"] {
        let mut p = config.preparation.clone();
        p.header.input_stream_id = stream.into();
        let g = first_group(&p);
        for account in if stream == "stream-c" {
            vec!["c", "d"]
        } else {
            vec![stream]
        } {
            targets.push(json!({"account":account,"request_id":account,"condition_id":common::FIRST,"cutoff":{"receipt":{"group":g}},"source":{"line_1based":1},"submission_items":[{"original":"preserved"}]}));
        }
        preparations.push(p);
    }
    let config_path = root.path().join("export.json");
    fs::write(
        &config_path,
        json!({"preparations":preparations}).to_string(),
    )
    .unwrap();
    let targets_path = root.path().join("targets.jsonl");
    fs::write(
        &targets_path,
        targets.iter().map(|t| format!("{t}\n")).collect::<String>(),
    )
    .unwrap();
    let path = root.path().join("complete.jsonl");
    let run = |output: &std::path::Path, stop: Option<&std::path::Path>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_export_observations"));
        command
            .arg("--config")
            .arg(&config_path)
            .arg("--targets")
            .arg(&targets_path)
            .arg("--output")
            .arg(output);
        if let Some(stop) = stop {
            command.arg("--stop-file").arg(stop);
        }
        command.output().unwrap()
    };
    let result = run(&path, None);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let rows = fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str::<Value>(s).unwrap())
        .collect::<Vec<_>>();
    let exported = rows
        .iter()
        .filter(|v| v["record_type"] == "target")
        .collect::<Vec<_>>();
    assert_eq!(exported.len(), 4);
    for (row, target) in exported.iter().zip(&targets) {
        assert_eq!(&row["public"]["target"], target);
        assert!(row["public"]["unknown_reason"].is_null());
        assert_eq!(
            row["public"]["matched_group"],
            target["cutoff"]["receipt"]["group"]
        );
        assert_eq!(row["blb"]["status"], "unknown");
    }
    assert_eq!(rows.last().unwrap()["completed_targets"], 4);
    eprintln!("完整输出实测：{}", rows.last().unwrap());
    let stop = root.path().join("stop");
    fs::write(&stop, "").unwrap();
    let partial = root.path().join("partial.jsonl");
    let result = run(&partial, Some(&stop));
    assert!(!result.status.success());
    let completion: Value =
        serde_json::from_str(fs::read_to_string(partial).unwrap().trim()).unwrap();
    assert_eq!(completion["status"], "stopped");
    assert_eq!(completion["completed_targets"], 0);
    eprintln!(
        "停止输出实测：{}；标准错误：{}",
        completion,
        String::from_utf8_lossy(&result.stderr)
    );
    let check = Command::new(env!("CARGO_BIN_EXE_export_observations"))
        .arg("--check-targets")
        .arg("--targets")
        .arg(&targets_path)
        .output()
        .unwrap();
    assert!(check.status.success());
    let summary: Value = serde_json::from_slice(&check.stdout).unwrap();
    assert_eq!(summary["targets"], 4);
    assert_eq!(summary["streams"].as_object().unwrap().len(), 3);
}
