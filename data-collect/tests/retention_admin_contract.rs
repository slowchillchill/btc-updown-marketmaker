use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use data_collect::backpressure::BackpressureReceipt;
use data_collect::manifest::{
    ManifestDraft, QueueWatermark, RequiredFile, SourceCoverage, TimeWindow,
    required_consumer_boundary_declarations,
};
use data_collect::resource::{
    CgroupMemoryStat, PsiSample, ResourceAction, ResourceSeverity, ResourceThresholdPolicyReceipt,
    ResourceThresholds,
};
use data_collect::{ResourceWatermark, ValidationVerdict};
use tempfile::tempdir;

fn admin_bin() -> &'static str {
    env!("CARGO_BIN_EXE_data-collect-retention-admin")
}

#[test]
fn admin_rejects_enabled_delete_for_production_root_before_scan() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().join("production/datasets");
    fs::create_dir_all(&dataset_root).unwrap();
    let old_day = complete_day(&dataset_root, "2026-07-05", &["polymarket_market"]);
    let receipt_path = tmp.path().join("evidence/production-enabled-negative.json");

    let output = Command::new(admin_bin())
        .arg("--dataset-root")
        .arg(&dataset_root)
        .arg("--receipt-path")
        .arg(&receipt_path)
        .arg("--threshold-bytes")
        .arg("1")
        .arg("--delete-mode")
        .arg("enabled")
        .arg("--current-utc-day")
        .arg("2026-07-07")
        .arg("--validate-options-only")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--allow-enabled-delete-validation-root")
    );
    assert!(!receipt_path.exists());
    assert!(old_day.is_dir(), "validate-only failure must not delete");
}

#[test]
fn admin_enabled_delete_requires_validation_root_containment() {
    let tmp = tempdir().unwrap();
    let validation_root = tmp.path().join("datasets-retention-validation/run-1");
    let dataset_root = validation_root.join("dataset");
    let evidence_root = validation_root.join("evidence/validation-enabled");
    fs::create_dir_all(&evidence_root).unwrap();
    fs::create_dir_all(&dataset_root).unwrap();
    let day5 = complete_day(&dataset_root, "2026-07-05", &["polymarket_market"]);
    let day6 = complete_day(&dataset_root, "2026-07-06", &["binance_aggtrade"]);
    let current_running =
        dataset_root.join("training/typed_raw/utc_day=2026-07-07/source_kind=polymarket_market");
    fs::create_dir_all(&current_running).unwrap();
    let current_file = current_running.join("polymarket_market_2026-07-07T00-running.parquet");
    fs::write(&current_file, b"running").unwrap();
    let receipt_path = evidence_root.join("retention_policy_receipt.json");

    let output = Command::new(admin_bin())
        .arg("--dataset-root")
        .arg(&dataset_root)
        .arg("--receipt-path")
        .arg(&receipt_path)
        .arg("--threshold-bytes")
        .arg("1")
        .arg("--delete-mode")
        .arg("enabled")
        .arg("--allow-enabled-delete-validation-root")
        .arg("--validation-root")
        .arg(&validation_root)
        .arg("--current-utc-day")
        .arg("2026-07-07")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!day5.exists(), "oldest complete day must be deleted");
    assert!(day6.is_dir(), "newer complete day must remain");
    assert!(current_file.is_file(), "current day must remain");
    assert!(receipt_path.is_file());
    let receipt: serde_json::Value =
        serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    assert_eq!(receipt["action"], "delete");
    assert_eq!(receipt["deletion_receipt"]["deleted_utc_day"], "2026-07-05");
}

#[test]
fn admin_rejects_receipt_path_inside_typed_raw_day() {
    let tmp = tempdir().unwrap();
    let validation_root = tmp.path().join("datasets-retention-validation/run-1");
    let dataset_root = validation_root.join("dataset");
    fs::create_dir_all(validation_root.join("evidence")).unwrap();
    fs::create_dir_all(&dataset_root).unwrap();
    let day5 = complete_day(&dataset_root, "2026-07-05", &["polymarket_market"]);
    let receipt_path = day5.join("retention_policy_receipt.json");

    let output = Command::new(admin_bin())
        .arg("--dataset-root")
        .arg(&dataset_root)
        .arg("--receipt-path")
        .arg(&receipt_path)
        .arg("--threshold-bytes")
        .arg("1")
        .arg("--delete-mode")
        .arg("enabled")
        .arg("--allow-enabled-delete-validation-root")
        .arg("--validation-root")
        .arg(&validation_root)
        .arg("--current-utc-day")
        .arg("2026-07-07")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("receipt parent must be under")
            || String::from_utf8_lossy(&output.stderr).contains("training/typed_raw")
    );
    assert!(day5.is_dir());
    assert!(!receipt_path.exists());
}

#[test]
fn admin_rejects_dry_run_receipt_path_inside_typed_raw_day() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().join("datasets");
    fs::create_dir_all(&dataset_root).unwrap();
    let day5 = complete_day(&dataset_root, "2026-07-05", &["polymarket_market"]);
    let receipt_path = day5.join("dry_run_retention_policy_receipt.json");

    let output = Command::new(admin_bin())
        .arg("--dataset-root")
        .arg(&dataset_root)
        .arg("--receipt-path")
        .arg(&receipt_path)
        .arg("--threshold-bytes")
        .arg("1")
        .arg("--delete-mode")
        .arg("dry-run")
        .arg("--current-utc-day")
        .arg("2026-07-07")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("training/typed_raw"));
    assert!(day5.is_dir());
    assert!(!receipt_path.exists());
}

#[cfg(unix)]
#[test]
fn admin_rejects_dry_run_receipt_parent_symlink_to_typed_raw_day() {
    let tmp = tempdir().unwrap();
    let dataset_root = tmp.path().join("datasets");
    fs::create_dir_all(&dataset_root).unwrap();
    let day5 = complete_day(&dataset_root, "2026-07-05", &["polymarket_market"]);
    let evidence_root = tmp.path().join("evidence");
    fs::create_dir_all(&evidence_root).unwrap();
    let symlink_parent = evidence_root.join("typed_raw_link");
    std::os::unix::fs::symlink(&day5, &symlink_parent).unwrap();
    let receipt_path = symlink_parent.join("dry_run_retention_policy_receipt.json");

    let output = Command::new(admin_bin())
        .arg("--dataset-root")
        .arg(&dataset_root)
        .arg("--receipt-path")
        .arg(&receipt_path)
        .arg("--threshold-bytes")
        .arg("1")
        .arg("--delete-mode")
        .arg("dry-run")
        .arg("--current-utc-day")
        .arg("2026-07-07")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("symlink")
            || String::from_utf8_lossy(&output.stderr).contains("training/typed_raw")
    );
    assert!(day5.is_dir());
    assert!(!day5.join("dry_run_retention_policy_receipt.json").exists());
}

#[cfg(unix)]
#[test]
fn admin_rejects_receipt_symlink_before_enabled_delete() {
    let tmp = tempdir().unwrap();
    let validation_root = tmp.path().join("datasets-retention-validation/run-1");
    let dataset_root = validation_root.join("dataset");
    let evidence_root = validation_root.join("evidence/validation-enabled");
    fs::create_dir_all(&evidence_root).unwrap();
    fs::create_dir_all(&dataset_root).unwrap();
    let day5 = complete_day(&dataset_root, "2026-07-05", &["polymarket_market"]);
    let symlink_target = tmp.path().join("outside-receipt.json");
    let receipt_path = evidence_root.join("retention_policy_receipt.json");
    std::os::unix::fs::symlink(&symlink_target, &receipt_path).unwrap();

    let output = Command::new(admin_bin())
        .arg("--dataset-root")
        .arg(&dataset_root)
        .arg("--receipt-path")
        .arg(&receipt_path)
        .arg("--threshold-bytes")
        .arg("1")
        .arg("--delete-mode")
        .arg("enabled")
        .arg("--allow-enabled-delete-validation-root")
        .arg("--validation-root")
        .arg(&validation_root)
        .arg("--current-utc-day")
        .arg("2026-07-07")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("symlink"));
    assert!(
        day5.is_dir(),
        "receipt symlink failure must happen before delete"
    );
    assert!(!symlink_target.exists());
}

#[test]
fn admin_rejects_receipt_directory_before_enabled_delete() {
    let tmp = tempdir().unwrap();
    let validation_root = tmp.path().join("datasets-retention-validation/run-1");
    let dataset_root = validation_root.join("dataset");
    let evidence_root = validation_root.join("evidence/validation-enabled");
    fs::create_dir_all(&evidence_root).unwrap();
    fs::create_dir_all(&dataset_root).unwrap();
    let day5 = complete_day(&dataset_root, "2026-07-05", &["polymarket_market"]);
    let receipt_path = evidence_root.join("retention_policy_receipt.json");
    fs::create_dir_all(&receipt_path).unwrap();

    let output = Command::new(admin_bin())
        .arg("--dataset-root")
        .arg(&dataset_root)
        .arg("--receipt-path")
        .arg(&receipt_path)
        .arg("--threshold-bytes")
        .arg("1")
        .arg("--delete-mode")
        .arg("enabled")
        .arg("--allow-enabled-delete-validation-root")
        .arg("--validation-root")
        .arg(&validation_root)
        .arg("--current-utc-day")
        .arg("2026-07-07")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("directory"));
    assert!(
        day5.is_dir(),
        "receipt directory failure must happen before delete"
    );
}

#[cfg(unix)]
#[test]
fn admin_treats_symlink_inside_candidate_day_as_not_deletable() {
    let tmp = tempdir().unwrap();
    let validation_root = tmp.path().join("datasets-retention-validation/run-1");
    let dataset_root = validation_root.join("dataset");
    let evidence_root = validation_root.join("evidence/fail-closed");
    fs::create_dir_all(&evidence_root).unwrap();
    fs::create_dir_all(&dataset_root).unwrap();
    let day5 = complete_day(&dataset_root, "2026-07-05", &["polymarket_market"]);
    let outside = tmp.path().join("outside-target");
    fs::create_dir_all(&outside).unwrap();
    fs::write(outside.join("must_remain.txt"), b"outside").unwrap();
    std::os::unix::fs::symlink(
        &outside,
        day5.join("source_kind=polymarket_market/link-out"),
    )
    .unwrap();
    let receipt_path = evidence_root.join("retention_policy_receipt.json");

    let output = Command::new(admin_bin())
        .arg("--dataset-root")
        .arg(&dataset_root)
        .arg("--receipt-path")
        .arg(&receipt_path)
        .arg("--threshold-bytes")
        .arg("1")
        .arg("--delete-mode")
        .arg("enabled")
        .arg("--allow-enabled-delete-validation-root")
        .arg("--validation-root")
        .arg(&validation_root)
        .arg("--current-utc-day")
        .arg("2026-07-07")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(day5.is_dir(), "symlink candidate must not be deleted");
    assert!(outside.join("must_remain.txt").is_file());
    let receipt: serde_json::Value =
        serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    assert_eq!(receipt["action"], "fail_closed");
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
