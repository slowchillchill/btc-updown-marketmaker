use std::fs;

use data_collect::backpressure::BackpressureReceipt;
use data_collect::manifest::{
    ManifestDraft, ManifestSourceGap, QueueWatermark, RequiredFile, SourceCoverage,
    SourceGapSeverity, TimeWindow, required_consumer_boundary_declarations,
};
use data_collect::publish::{
    complete_publish_decision, discover_finalized_artifacts, failed_atomic_publish_decision,
    manifest_discoverable_as_complete, manifest_path_for_parquet,
};
use data_collect::resource::{
    CgroupMemoryStat, PsiSample, ResourceAction, ResourceSeverity, ResourceThresholdPolicyReceipt,
    ResourceThresholds,
};
use data_collect::{PublishedSegment, ResourceWatermark, ValidationVerdict};
use tempfile::tempdir;

fn complete_resource_watermark() -> ResourceWatermark {
    ResourceWatermark {
        rss_bytes: Some(100),
        cpu_percent: Some(1.0),
        disk_free_bytes: Some(1024),
        artifact_dir_bytes: Some(2048),
        journal_log_bytes: Some(128),
        journal_limit_status: Some("within_limit".to_owned()),
        open_files: Some(16),
        queue_bytes: Some(512),
        queue_age_ms: Some(10),
        parquet_writer_memory_estimate_bytes: Some(4096),
        cgroup_memory_current_bytes: Some(8192),
        cgroup_memory_stat: Some(CgroupMemoryStat {
            anon_bytes: 100,
            file_bytes: 200,
            slab_bytes: 300,
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
        stage_latency_ms: Some(3),
        ttl_ms: Some(1000),
        unavailable: Vec::new(),
    }
}

fn complete_resource_threshold_policy_receipt() -> ResourceThresholdPolicyReceipt {
    let thresholds = ResourceThresholds {
        min_disk_free_bytes: 1,
        max_rss_bytes: 1_000_000,
        max_cpu_percent: 95.0,
        max_artifact_dir_bytes: 1_000_000,
        max_journal_log_bytes: 1_000_000,
        max_open_files: 1_000,
        max_queue_bytes: 1_000_000,
        max_queue_age_ms: 1_000,
        max_parquet_writer_memory_estimate_bytes: 1_000_000,
        max_cgroup_memory_current_bytes: 1_000_000,
        max_cgroup_memory_anon_bytes: 1_000_000,
        max_cgroup_memory_file_bytes: 1_000_000,
        max_cgroup_memory_slab_bytes: 1_000_000,
        max_psi_some_avg10: 90.0,
        max_psi_full_avg10: 50.0,
        max_stage_latency_ms: 1_000,
        max_ttl_ms: 10_000,
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
        recovery_condition: "below soft thresholds for recovery_hysteresis_samples".to_owned(),
        last_result: ResourceSeverity::Healthy,
    }
}

fn complete_manifest() -> ManifestDraft {
    complete_manifest_for("probe.parquet")
}

fn complete_manifest_for(parquet_file_name: &str) -> ManifestDraft {
    ManifestDraft {
        artifact_path: Some(parquet_file_name.into()),
        row_count: Some(2),
        sha256: Some("0123456789abcdef".to_owned()),
        schema_version: Some("parquet_raw_layout_v1".to_owned()),
        code_version: Some("test-code-version".to_owned()),
        collector_binary_sha256: Some("binary-sha256".to_owned()),
        source_config_digest: Some("source-config-digest".to_owned()),
        validation_verdict: ValidationVerdict::Complete,
        consumer_boundary_declarations: required_consumer_boundary_declarations(),
        source_coverage: vec![SourceCoverage {
            source_kind: "polymarket_market".to_owned(),
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
        resource_watermark: Some(complete_resource_watermark()),
        resource_threshold_policy_receipt: Some(complete_resource_threshold_policy_receipt()),
        writer_queue_high_watermark: Some(QueueWatermark {
            rows: 2,
            bytes: 512,
            oldest_event_age_ms: 10,
            writer_lag_ms: 5,
        }),
        backpressure_receipt: Some(BackpressureReceipt::empty("polymarket_market")),
        flush_latency_ms: Some(3),
        collector_host_id: Some("collector-local".to_owned()),
        collector_region: Some("local-dev".to_owned()),
        required_files: vec![RequiredFile {
            file_role: "typed_raw_parquet".to_owned(),
            path: parquet_file_name.into(),
            finalized: true,
            row_count: Some(2),
            sha256: Some("0123456789abcdef".to_owned()),
            schema_version: Some("parquet_raw_layout_v1".to_owned()),
            missing_reason: None,
        }],
    }
}

#[test]
fn finalized_discovery_excludes_partial_files() {
    let tmp = tempdir().unwrap();
    let good_parquet = tmp.path().join("good.parquet");
    let good_manifest = manifest_path_for_parquet(&good_parquet);
    fs::write(&good_parquet, b"final parquet").unwrap();
    fs::write(
        &good_manifest,
        serde_json::to_vec(&complete_manifest_for("good.parquet")).unwrap(),
    )
    .unwrap();

    fs::write(tmp.path().join("no-manifest.parquet"), b"missing manifest").unwrap();
    let garbage_manifest_parquet = tmp.path().join("garbage-manifest.parquet");
    fs::write(&garbage_manifest_parquet, b"parquet with garbage manifest").unwrap();
    fs::write(
        manifest_path_for_parquet(&garbage_manifest_parquet),
        b"not-json",
    )
    .unwrap();
    let incomplete_manifest_parquet = tmp.path().join("incomplete-manifest.parquet");
    fs::write(
        &incomplete_manifest_parquet,
        b"parquet with incomplete manifest",
    )
    .unwrap();
    let mut incomplete_manifest = complete_manifest_for("incomplete-manifest.parquet");
    incomplete_manifest.validation_verdict = ValidationVerdict::Incomplete;
    fs::write(
        manifest_path_for_parquet(&incomplete_manifest_parquet),
        serde_json::to_vec(&incomplete_manifest).unwrap(),
    )
    .unwrap();
    fs::write(tmp.path().join("probe-running.parquet"), b"partial").unwrap();
    fs::write(
        tmp.path().join("probe-running.manifest.json"),
        b"partial manifest",
    )
    .unwrap();
    fs::write(tmp.path().join("probe-digesting.parquet"), b"partial").unwrap();
    fs::write(tmp.path().join("sync.parquet.rsync-partial"), b"partial").unwrap();
    fs::write(tmp.path().join("upload.parquet.partial"), b"partial").unwrap();

    let discovered = discover_finalized_artifacts(tmp.path()).unwrap();

    assert_eq!(
        discovered,
        vec![data_collect::publish::FinalizedArtifact {
            parquet_path: good_parquet,
            manifest_path: good_manifest,
        }]
    );
}

#[test]
fn atomic_rename_failure_marks_manifest_incomplete() {
    let tmp = tempdir().unwrap();
    let failed = failed_atomic_publish_decision(
        "typed_raw_parquet",
        tmp.path().join("probe.parquet"),
        "rename_failed",
    );

    assert_eq!(failed.verdict, ValidationVerdict::Incomplete);
    assert!(!failed.discoverable_as_complete);
    assert!(!failed.required_file.finalized);
    assert_eq!(failed.required_file.row_count, None);
    assert_eq!(failed.required_file.sha256, None);
    assert_eq!(
        failed.required_file.missing_reason.as_deref(),
        Some("atomic_publish_failed:rename_failed")
    );

    let segment = PublishedSegment {
        final_path: tmp.path().join("probe.parquet"),
        row_count: 2,
        sha256: "0123456789abcdef".to_owned(),
        schema_version: "parquet_raw_layout_v1".to_owned(),
    };
    let complete = complete_publish_decision("typed_raw_parquet", &segment);
    assert_eq!(complete.verdict, ValidationVerdict::Complete);
    assert!(complete.discoverable_as_complete);
}

#[test]
fn coverage_incomplete_does_not_publish_as_complete_or_stop_collector() {
    let mut manifest = complete_manifest();
    manifest.source_coverage.clear();
    manifest.coverage_incomplete_reason = Some("polymarket source coverage missing".to_owned());
    manifest.manifest_source_gaps.push(ManifestSourceGap {
        source_kind: "polymarket_market".to_owned(),
        reason: "polymarket_source_gap".to_owned(),
        severity: SourceGapSeverity::SourceDegraded,
        first_seen_utc_ms: 1,
        recovered_at_utc_ms: None,
    });

    assert_eq!(manifest.effective_verdict(), ValidationVerdict::Incomplete);
    assert!(!manifest_discoverable_as_complete(&manifest));
    assert!(!manifest.implies_process_fatal());
}
