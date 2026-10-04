//! #185 最早小范围材料试制：原准备入口、公开可见观察统计，无策略或账户。
use backtest::{
    HistoricalPreparationConfigV1, HistoricalPreparationProgressV1, MaterialError,
    ProcessCountersV1, prepare_historical_run_with_statistics_v1,
    process_clock_ticks_per_second_v1, process_measurement_v1,
};
#[cfg(test)]
use contracts::*;
use serde_json::{Value, json};
use std::{path::Path, time::Instant};

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../tests/common/mod.rs"]
mod common;

fn save(path: &Path, v: &Value) -> Result<(), MaterialError> {
    std::fs::write(path, serde_json::to_vec_pretty(v)?)?;
    Ok(())
}
fn main() -> Result<(), MaterialError> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    let progress_seconds = if args
        .first()
        .is_some_and(|s| s == "--progress-interval-seconds")
    {
        if args.len() < 2 {
            return Err("--progress-interval-seconds 缺少秒数".into());
        }
        let seconds = args[1].parse::<u64>()?;
        args.drain(..2);
        seconds
    } else {
        30
    };
    if args.len() != 2 {
        return Err("用法 material_trial [--progress-interval-seconds <SECONDS>] <PREPARATION_JSON> <REPORT_JSON>".into());
    }
    let config: HistoricalPreparationConfigV1 = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let report_path = Path::new(&args[1]);
    run_trial(config, report_path, progress_seconds)
}
fn run_trial(
    config: HistoricalPreparationConfigV1,
    report_path: &Path,
    progress_seconds: u64,
) -> Result<(), MaterialError> {
    run_trial_with_counter_reader(
        config,
        report_path,
        progress_seconds,
        ProcessCountersV1::read,
    )
}
fn run_trial_with_counter_reader(
    mut config: HistoricalPreparationConfigV1,
    report_path: &Path,
    progress_seconds: u64,
    counter_reader: fn() -> Result<ProcessCountersV1, MaterialError>,
) -> Result<(), MaterialError> {
    // 有效配置保持；诊断源不可用不能阻止或遮蔽原业务。
    config.header = config.effective_header();
    let hz = process_clock_ticks_per_second_v1();
    let mut report = json!({"schema":"complementary_pair_material_trial_v1", "clock_ticks_per_second":hz,
        "scope":config.scope,"status":"started", "statistics":null,"statistics_measurement":null,"statistics_measurement_applicability":"not_applicable_separate_replay_removed; included_in_prepare_and_statistics_measurement","progress_interval_seconds":progress_seconds});
    // 私有报告保留完整有效配置及原错误；发布版由交付文档移除物理定位。
    report["effective_preparation"] = serde_json::to_value(&config)?;
    save(report_path, &report)?;
    let before = counter_reader().ok();
    let began = Instant::now();
    let progress = HistoricalPreparationProgressV1::new(
        std::time::Duration::from_secs(progress_seconds),
        std::io::stderr(),
    );
    let phases = progress.measurements();
    let prepared = prepare_historical_run_with_statistics_v1(&config, Some(progress));
    let after = counter_reader().ok();
    report["prepare_and_statistics_measurement"] = process_measurement_v1(
        before.as_ref(),
        after.as_ref(),
        hz,
        began.elapsed().as_secs_f64(),
    );
    report["preparation_phase_measurements"] =
        phases.snapshot(&report["prepare_and_statistics_measurement"]);
    let prepared = match prepared {
        Ok(value) => value,
        Err(error) => {
            report["status"] = json!("prepare_failed");
            report["original_error"] = json!(error.to_string());
            report["original_error_debug"] = json!(format!("{error:?}"));
            // 报告写入故障同样不能覆盖已经得到的原prepare错误。
            let _ = save(report_path, &report);
            return Err(error);
        }
    };
    let statistics = prepared.statistics;
    let prepared = prepared.prepared;
    report["input_set_identity_sha256"] = json!(prepared.input_set_identity_sha256);
    report["source_identity_sha256"] = json!(prepared.source_identity_sha256);
    report["initialization"] = json!(prepared.initialization);
    report["source_manifests"] = json!(prepared.source_manifests);
    report["prepared_markets"] = json!(prepared.markets.keys().collect::<Vec<_>>());
    report["hidden_preparation"] = json!(prepared.hidden);
    report["statistics"] = serde_json::to_value(statistics)?;
    report["status"] = json!("success");
    save(report_path, &report)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn initialization_sources_do_not_enter_research_observation_denominators() {
        let temp = tempfile::tempdir().unwrap();
        let (mut config, _) = common::setup(temp.path());
        common::initialize_at(&mut config, common::BASE + 1000, &[common::FIRST]);
        let report_path = temp.path().join("trial.json");
        run_trial(config.preparation, &report_path, 0).unwrap();
        let report: Value = serde_json::from_slice(&std::fs::read(report_path).unwrap()).unwrap();
        let stats = &report["statistics"];
        assert_eq!(stats["initialization_public_groups"], 1);
        assert_eq!(stats["public_groups"], 7);
        assert_eq!(
            stats["initialization_markets"][common::FIRST]["observations"],
            1
        );
        assert!(
            stats["markets"][common::FIRST]["first_visible_at"]
                .as_i64()
                .unwrap()
                >= common::BASE + 1000
        );
        assert!(
            stats["research_groups_with_initialization_dependencies"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_ne!(
            report["source_identity_sha256"],
            report["input_set_identity_sha256"]
        );
    }

    #[test]
    fn legacy_ordering_label_records_actual_version_on_success_and_prepare_failure() {
        for fail in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let (config, _) = common::setup(temp.path());
            let mut preparation = config.preparation;
            assert_eq!(
                preparation.header.ordering_version,
                "historical-source-head-v1"
            );
            if fail {
                preparation.recording.max_capture_output_bytes = 1;
            }
            let expected = preparation.effective_header();
            let report_path = temp.path().join("trial.json");
            let result = run_trial(preparation.clone(), &report_path, 0);
            let report: Value =
                serde_json::from_slice(&std::fs::read(report_path).unwrap()).unwrap();
            assert!(report["prepare_and_statistics_measurement"]["wall_seconds"].is_number());
            assert_eq!(report["statistics_measurement"], Value::Null);
            assert!(
                report["statistics_measurement_applicability"]
                    .as_str()
                    .unwrap()
                    .contains("not_applicable")
            );
            assert!(report.get("prepare_measurement").is_none());
            let stream: PublicInputStreamHeaderV1 = serde_json::from_slice(
                &std::fs::read(preparation.work_root.join("public/stream.json")).unwrap(),
            )
            .unwrap();
            let effective: HistoricalPreparationConfigV1 =
                serde_json::from_value(report["effective_preparation"].clone()).unwrap();
            assert_eq!(stream, expected);
            assert_eq!(effective.header, stream);
            assert_eq!(
                preparation.header.ordering_version,
                "historical-source-head-v1"
            );
            if fail {
                let error = result.unwrap_err();
                assert_eq!(report["status"], "prepare_failed");
                assert_eq!(report["original_error"], error.to_string());
                assert_eq!(report["statistics"], Value::Null);
            } else {
                result.unwrap();
                assert_eq!(report["status"], "success");
                assert_eq!(report["statistics"]["public_groups"], 8);
            }
        }
    }
    #[test]
    fn unavailable_endpoint_metrics_do_not_mask_original_prepare_failure() {
        fn unavailable() -> Result<ProcessCountersV1, MaterialError> {
            Err("计量源不可用".into())
        }
        let temp = tempfile::tempdir().unwrap();
        let (config, _) = common::setup(temp.path());
        let mut prep = config.preparation;
        prep.recording.max_capture_output_bytes = 1;
        let path = temp.path().join("trial.json");
        let error = run_trial_with_counter_reader(prep, &path, 0, unavailable).unwrap_err();
        assert_eq!(error.to_string(), "External: 捕获产物超过冻结的总字节上限");
        let report: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(report["status"], "prepare_failed");
        assert_eq!(report["original_error"], error.to_string());
        assert!(report["statistics"].is_null());
        assert_eq!(
            report["prepare_and_statistics_measurement"]["available"],
            false
        );
        assert!(report["prepare_and_statistics_measurement"]["user_seconds"].is_null());
        assert!(report["prepare_and_statistics_measurement"]["wall_seconds"].is_number());
    }
}
