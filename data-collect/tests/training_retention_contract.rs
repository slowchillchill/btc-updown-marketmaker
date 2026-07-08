use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use data_collect::backpressure::BackpressureReceipt;
use data_collect::manifest::{
    ManifestDraft, QueueWatermark, RequiredFile, SourceCoverage, TimeWindow,
    required_consumer_boundary_declarations,
};
use data_collect::resource::{
    CgroupMemoryStat, PsiSample, ResourceAction, ResourceSeverity, ResourceThresholdPolicyReceipt,
    ResourceThresholds,
};
use data_collect::retention::{RetentionDeleteMode, TrainingRetentionPolicy};
use data_collect::{ResourceWatermark, ValidationVerdict};
use tempfile::tempdir;

#[test]
fn retention_dry_run_and_enabled_delete_only_oldest_complete_utc_day() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().join("datasets");
    fs::create_dir_all(&dataset_root).unwrap();

    let day5 = complete_day(&dataset_root, "2026-07-05", &["polymarket_market"]);
    let day6 = complete_day(&dataset_root, "2026-07-06", &["binance_aggtrade"]);
    let current_running =
        dataset_root.join("training/typed_raw/utc_day=2026-07-07/source_kind=polymarket_market");
    fs::create_dir_all(&current_running).unwrap();
    let current_running_file =
        current_running.join("polymarket_market_2026-07-07T00-running.parquet");
    fs::write(&current_running_file, b"running").unwrap();

    let incomplete_day = incomplete_day(&dataset_root, "2026-07-04");
    let manual_inspection = dataset_root.join("training/manual-inspection/manual.txt");
    fs::create_dir_all(manual_inspection.parent().unwrap()).unwrap();
    fs::write(&manual_inspection, b"manual").unwrap();
    let run_manifest = dataset_root.join("training/run_manifests/run.manifest.json");
    fs::create_dir_all(run_manifest.parent().unwrap()).unwrap();
    fs::write(&run_manifest, b"{}").unwrap();
    let startup_receipt = dataset_root.join("training/runtime_artifacts/run.startup_receipt.json");
    fs::create_dir_all(startup_receipt.parent().unwrap()).unwrap();
    fs::write(&startup_receipt, b"{}").unwrap();

    let receipt_path = dataset_root.join("training/runtime_artifacts/retention_policy.json");
    let dry_run = TrainingRetentionPolicy::new(&dataset_root)
        .unwrap()
        .with_threshold_bytes(1)
        .with_current_utc_day("2026-07-07")
        .with_delete_mode(RetentionDeleteMode::DryRun)
        .enforce(&receipt_path)
        .unwrap();

    assert_eq!(dry_run.action, "dry_run");
    assert_eq!(dry_run.delete_mode, RetentionDeleteMode::DryRun);
    assert_eq!(dry_run.candidate_utc_day.as_deref(), Some("2026-07-05"));
    assert_eq!(dry_run.deletion_receipt, None);
    assert!(day5.is_dir(), "dry-run must not delete candidate");
    assert!(receipt_path.is_file());
    let dry_run_json: serde_json::Value =
        serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    assert_eq!(dry_run_json["action"], "dry_run");
    assert_eq!(dry_run_json["would_delete_utc_day"], "2026-07-05");

    let delete_receipt_path = dataset_root.join("training/runtime_artifacts/deletion.json");
    let enabled = TrainingRetentionPolicy::new(&dataset_root)
        .unwrap()
        .with_threshold_bytes(1)
        .with_current_utc_day("2026-07-07")
        .with_delete_mode(RetentionDeleteMode::Enabled)
        .enforce(&delete_receipt_path)
        .unwrap();

    assert_eq!(enabled.action, "delete");
    assert_eq!(enabled.delete_mode, RetentionDeleteMode::Enabled);
    assert_eq!(enabled.candidate_utc_day.as_deref(), Some("2026-07-05"));
    let deletion = enabled.deletion_receipt.as_ref().unwrap();
    assert_eq!(deletion.deleted_utc_day, "2026-07-05");
    assert_eq!(deletion.delete_mode, RetentionDeleteMode::Enabled);
    assert!(
        deletion
            .deleted_path
            .ends_with("training/typed_raw/utc_day=2026-07-05")
    );
    assert!(deletion.deleted_bytes > 0);

    assert!(
        !day5.exists(),
        "enabled mode deletes only oldest complete day"
    );
    assert!(day6.is_dir(), "newer complete day must remain");
    assert!(
        current_running_file.is_file(),
        "current running file must remain"
    );
    assert!(incomplete_day.is_dir(), "incomplete day must remain");
    assert!(manual_inspection.is_file(), "manual-inspection must remain");
    assert!(run_manifest.is_file(), "run manifests must remain");
    assert!(startup_receipt.is_file(), "startup receipts must remain");
}

#[test]
fn retention_scan_honors_expired_deadline_without_writing_receipt() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().join("datasets");
    fs::create_dir_all(dataset_root.join("training/typed_raw")).unwrap();
    let receipt_path = dataset_root.join("training/runtime_artifacts/retention_policy.json");

    let err = TrainingRetentionPolicy::new(&dataset_root)
        .unwrap()
        .enforce_with_deadline(&receipt_path, Instant::now() - Duration::from_millis(1))
        .unwrap_err();

    assert!(err.to_string().contains("retention scan exceeded deadline"));
    assert!(!receipt_path.exists());
}

#[test]
fn retention_fail_closed_when_no_complete_day_is_deletable() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().join("datasets");
    fs::create_dir_all(&dataset_root).unwrap();

    let incomplete = incomplete_day(&dataset_root, "2026-07-04");
    let current_running =
        dataset_root.join("training/typed_raw/utc_day=2026-07-07/source_kind=polymarket_market");
    fs::create_dir_all(&current_running).unwrap();
    let current_running_file =
        current_running.join("polymarket_market_2026-07-07T00-running.parquet");
    fs::write(&current_running_file, b"running").unwrap();

    let receipt = TrainingRetentionPolicy::new(&dataset_root)
        .unwrap()
        .with_threshold_bytes(1)
        .with_current_utc_day("2026-07-07")
        .with_delete_mode(RetentionDeleteMode::Enabled)
        .enforce(&dataset_root.join("training/runtime_artifacts/retention_fail_closed.json"))
        .unwrap();

    assert_eq!(receipt.action, "fail_closed");
    assert_eq!(receipt.verdict, "no_deletable_finalized_complete_utc_day");
    assert_eq!(receipt.deletion_receipt, None);
    assert!(incomplete.is_dir());
    assert!(current_running_file.is_file());
}

fn complete_day(dataset_root: &Path, utc_day: &str, sources: &[&str]) -> PathBuf {
    let day = dataset_root
        .join("training")
        .join("typed_raw")
        .join(format!("utc_day={utc_day}"));
    for source in sources {
        let source_dir = day.join(format!("source_kind={source}"));
        fs::create_dir_all(&source_dir).unwrap();
        let parquet_name = format!("{source}_{utc_day}T00.parquet");
        let parquet_path = source_dir.join(&parquet_name);
        fs::write(&parquet_path, b"finalized parquet bytes").unwrap();
        let manifest_path = parquet_path.with_extension("manifest.json");
        fs::write(
            manifest_path,
            serde_json::to_vec(&complete_manifest_for(source, &parquet_name)).unwrap(),
        )
        .unwrap();
    }
    day
}

fn incomplete_day(dataset_root: &Path, utc_day: &str) -> PathBuf {
    let day = dataset_root
        .join("training")
        .join("typed_raw")
        .join(format!("utc_day={utc_day}"))
        .join("source_kind=polymarket_market");
    fs::create_dir_all(&day).unwrap();
    let parquet = day.join(format!("polymarket_market_{utc_day}T00.parquet"));
    fs::write(&parquet, b"incomplete parquet bytes").unwrap();
    let mut manifest = complete_manifest_for(
        "polymarket_market",
        parquet.file_name().unwrap().to_str().unwrap(),
    );
    manifest.validation_verdict = ValidationVerdict::Incomplete;
    fs::write(
        parquet.with_extension("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    day.parent().unwrap().to_path_buf()
}

fn complete_manifest_for(source_kind: &str, parquet_file_name: &str) -> ManifestDraft {
    ManifestDraft {
        artifact_path: Some(parquet_file_name.into()),
        row_count: Some(2),
        sha256: Some("0123456789abcdef".to_owned()),
        schema_version: Some(format!("{source_kind}_v1")),
        code_version: Some("test-code-version".to_owned()),
        collector_binary_sha256: Some("binary-sha256".to_owned()),
        source_config_digest: Some("source-config-digest".to_owned()),
        validation_verdict: ValidationVerdict::Complete,
        consumer_boundary_declarations: required_consumer_boundary_declarations(),
        source_coverage: vec![SourceCoverage {
            source_kind: source_kind.to_owned(),
            first_recv_time_utc_ms: 1,
            last_recv_time_utc_ms: 2,
            rows: 2,
        }],
        source_gaps: Vec::new(),
        manifest_source_gaps: Vec::new(),
        coverage_incomplete_reason: None,
        health_artifact_path: Some("health.jsonl".into()),
        health_artifact_digest: None,
        gap_artifact_path: Some("gap.jsonl".into()),
        gap_artifact_digest: None,
        reconnect_artifact_path: Some("reconnect.jsonl".into()),
        reconnect_artifact_digest: None,
        start_time_utc_ms: Some(1),
        end_time_utc_ms: Some(2),
        coverage_window: Some(TimeWindow {
            start_utc_ms: 1,
            end_utc_ms: 2,
        }),
        resource_watermark: Some(resource_watermark()),
        resource_threshold_policy_receipt: Some(policy_receipt()),
        writer_queue_high_watermark: Some(QueueWatermark {
            rows: 2,
            bytes: 512,
            oldest_event_age_ms: 10,
            writer_lag_ms: 5,
        }),
        backpressure_receipt: Some(BackpressureReceipt::empty(source_kind)),
        flush_latency_ms: Some(3),
        collector_host_id: Some("collector-local".to_owned()),
        collector_region: Some("local-dev".to_owned()),
        required_files: vec![RequiredFile {
            file_role: "typed_raw_parquet".to_owned(),
            path: parquet_file_name.into(),
            finalized: true,
            row_count: Some(2),
            sha256: Some("0123456789abcdef".to_owned()),
            schema_version: Some(format!("{source_kind}_v1")),
            missing_reason: None,
        }],
    }
}

fn resource_watermark() -> ResourceWatermark {
    ResourceWatermark {
        rss_bytes: Some(1),
        cpu_percent: Some(1.0),
        disk_free_bytes: Some(1),
        artifact_dir_bytes: Some(1),
        journal_log_bytes: Some(1),
        journal_limit_status: Some("within_limit".to_owned()),
        open_files: Some(1),
        queue_bytes: Some(1),
        queue_age_ms: Some(1),
        parquet_writer_memory_estimate_bytes: Some(1),
        cgroup_memory_current_bytes: Some(1),
        cgroup_memory_stat: Some(CgroupMemoryStat {
            anon_bytes: 1,
            file_bytes: 1,
            slab_bytes: 1,
        }),
        cgroup_memory_events: Some("low 0\nhigh 0\nmax 0\noom 0\noom_kill 0".to_owned()),
        psi_cpu: Some(PsiSample {
            some_avg10: 0.0,
            full_avg10: 0.0,
        }),
        psi_memory: Some(PsiSample {
            some_avg10: 0.0,
            full_avg10: 0.0,
        }),
        psi_io: Some(PsiSample {
            some_avg10: 0.0,
            full_avg10: 0.0,
        }),
        stage_latency_ms: Some(1),
        ttl_ms: Some(1),
        unavailable: Vec::new(),
    }
}

fn policy_receipt() -> ResourceThresholdPolicyReceipt {
    let thresholds = ResourceThresholds {
        min_disk_free_bytes: 1,
        max_rss_bytes: 1,
        max_cpu_percent: 1.0,
        max_artifact_dir_bytes: 1,
        max_journal_log_bytes: 1,
        max_open_files: 1,
        max_queue_bytes: 1,
        max_queue_age_ms: 1,
        max_parquet_writer_memory_estimate_bytes: 1,
        max_cgroup_memory_current_bytes: 1,
        max_cgroup_memory_anon_bytes: 1,
        max_cgroup_memory_file_bytes: 1,
        max_cgroup_memory_slab_bytes: 1,
        max_psi_some_avg10: 1.0,
        max_psi_full_avg10: 1.0,
        max_stage_latency_ms: 1,
        max_ttl_ms: 1,
    };
    ResourceThresholdPolicyReceipt {
        soft_thresholds: thresholds,
        hard_thresholds: thresholds,
        threshold_unit: "bytes/percent/count/milliseconds/psi_avg10".to_owned(),
        hard_hysteresis_samples: 2,
        recovery_hysteresis_samples: 2,
        soft_actions: vec![
            ResourceAction::RecordBackpressure,
            ResourceAction::ObserveRecovery,
        ],
        hard_actions: vec![
            ResourceAction::StopNewIntake,
            ResourceAction::FinalizeOpenSegments,
            ResourceAction::WriteIncompleteManifest,
        ],
        recovery_actions: vec![
            ResourceAction::ObserveRecovery,
            ResourceAction::ResumeIntake,
        ],
        recovery_condition: "below soft thresholds".to_owned(),
        last_result: ResourceSeverity::Healthy,
    }
}
