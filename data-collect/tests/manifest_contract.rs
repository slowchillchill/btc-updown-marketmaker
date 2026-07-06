use data_collect::manifest::{
    ManifestFailureClass, ManifestSourceGap, SourceGapSeverity, TimeWindow,
};
use data_collect::resource::{CgroupMemoryStat, PsiSample};
use data_collect::{
    ManifestDraft, QueueWatermark, RequiredFile, ResourceWatermark, SourceCoverage,
    ValidationVerdict,
};

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

fn complete_manifest() -> ManifestDraft {
    ManifestDraft {
        artifact_path: Some("probe.parquet".into()),
        row_count: Some(2),
        sha256: Some("0123456789abcdef".to_owned()),
        schema_version: Some("parquet_raw_layout_v1".to_owned()),
        code_version: Some("git-commit".to_owned()),
        collector_binary_sha256: Some("binary-sha256".to_owned()),
        source_config_digest: Some("source-config-digest".to_owned()),
        validation_verdict: ValidationVerdict::Complete,
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
        writer_queue_high_watermark: Some(QueueWatermark {
            rows: 2,
            bytes: 512,
            oldest_event_age_ms: 10,
            writer_lag_ms: 5,
        }),
        flush_latency_ms: Some(3),
        collector_host_id: Some("collector-local".to_owned()),
        collector_region: Some("eu-west-1".to_owned()),
        required_files: vec![RequiredFile {
            file_role: "typed_raw_parquet".to_owned(),
            path: "probe.parquet".into(),
            finalized: true,
            row_count: Some(2),
            sha256: Some("0123456789abcdef".to_owned()),
            schema_version: Some("parquet_raw_layout_v1".to_owned()),
            missing_reason: None,
        }],
    }
}

#[test]
fn manifest_contract_requires_files_coverage_health_resource_and_time_windows() {
    let complete = complete_manifest();
    assert_eq!(complete.effective_verdict(), ValidationVerdict::Complete);

    let mut missing_reconnect = complete.clone();
    missing_reconnect.reconnect_artifact_path = None;
    assert_eq!(
        missing_reconnect.effective_verdict(),
        ValidationVerdict::Incomplete
    );

    let mut missing_window = complete.clone();
    missing_window.coverage_window = None;
    assert_eq!(
        missing_window.effective_verdict(),
        ValidationVerdict::Incomplete
    );

    let mut missing_required_file_sha = complete;
    missing_required_file_sha.required_files[0].sha256 = None;
    assert_eq!(
        missing_required_file_sha.effective_verdict(),
        ValidationVerdict::Incomplete
    );
}

#[test]
fn manifest_rejects_invalid_source_coverage() {
    let mut manifest = complete_manifest();
    manifest.source_coverage[0].rows = 0;
    assert_eq!(manifest.effective_verdict(), ValidationVerdict::Incomplete);

    let mut manifest = complete_manifest();
    manifest.source_coverage[0].source_kind.clear();
    assert_eq!(manifest.effective_verdict(), ValidationVerdict::Incomplete);

    let mut manifest = complete_manifest();
    manifest.source_coverage[0].first_recv_time_utc_ms = 3;
    manifest.source_coverage[0].last_recv_time_utc_ms = 2;
    assert_eq!(manifest.effective_verdict(), ValidationVerdict::Incomplete);
}

#[test]
fn incomplete_manifest_does_not_imply_process_fatal() {
    let mut manifest = complete_manifest();
    manifest.source_coverage.clear();
    manifest.coverage_incomplete_reason = Some("polymarket source coverage missing".to_owned());
    manifest.manifest_source_gaps.push(ManifestSourceGap {
        source_kind: "polymarket_market".to_owned(),
        reason: "coverage_incomplete".to_owned(),
        severity: SourceGapSeverity::SourceDegraded,
        first_seen_utc_ms: 1,
        recovered_at_utc_ms: None,
    });

    assert_eq!(manifest.effective_verdict(), ValidationVerdict::Incomplete);
    assert_eq!(
        manifest.failure_class(),
        ManifestFailureClass::ArtifactIncomplete
    );
    assert!(!manifest.implies_process_fatal());
}
