use std::fs;

use data_collect::backpressure::BackpressureReceipt;
use data_collect::hourly_partition::{HourlyPartitionPolicy, is_legacy_source_first_partition};
use data_collect::manifest::{
    ManifestDraft, QueueWatermark, RequiredFile, SourceCoverage, TimeWindow,
    required_consumer_boundary_declarations,
};
use data_collect::publish::{discover_finalized_artifacts, manifest_path_for_parquet};
use data_collect::resource::{
    CgroupMemoryStat, PsiSample, ResourceAction, ResourceSeverity, ResourceThresholdPolicyReceipt,
    ResourceThresholds,
};
use data_collect::{ResourceWatermark, ValidationVerdict};
use tempfile::tempdir;

#[test]
fn hourly_partition_uses_day_first_source_kind_hour_path() {
    let tmp = tempdir().unwrap();
    let policy = HourlyPartitionPolicy::new(tmp.path()).unwrap();

    let partition = policy
        .partition("polymarket_market", 1_783_407_604_599, None)
        .unwrap();

    assert_eq!(partition.utc_day, "2026-07-07");
    assert_eq!(partition.utc_hour, "2026-07-07T07");
    assert_eq!(partition.hour_start_utc_ms, 1_783_407_600_000);
    assert_eq!(partition.hour_end_utc_ms, 1_783_411_200_000);
    assert!(
        partition
            .partition_path
            .ends_with("training/typed_raw/utc_day=2026-07-07/source_kind=polymarket_market")
    );
    assert!(
        partition
            .final_path
            .ends_with("polymarket_market_2026-07-07T07.parquet")
    );
    assert!(
        partition
            .running_path
            .ends_with("polymarket_market_2026-07-07T07-running.parquet")
    );
    assert!(
        partition
            .digesting_path
            .ends_with("polymarket_market_2026-07-07T07-digesting.parquet")
    );
    assert_eq!(
        partition.manifest_path,
        manifest_path_for_parquet(&partition.final_path)
    );
}

#[test]
fn hourly_partition_rolls_at_utc_hour_boundary_and_supports_part_files() {
    let tmp = tempdir().unwrap();
    let policy = HourlyPartitionPolicy::new(tmp.path()).unwrap();

    let last_ms = policy
        .partition("binance_depth_update", 1_783_411_199_999, None)
        .unwrap();
    let first_ms = policy
        .partition("binance_depth_update", 1_783_411_200_000, None)
        .unwrap();
    let part = policy
        .partition("binance_depth_update", 1_783_411_200_000, Some(2))
        .unwrap();

    assert_eq!(last_ms.utc_hour, "2026-07-07T07");
    assert_eq!(first_ms.utc_hour, "2026-07-07T08");
    assert!(
        part.final_path
            .ends_with("binance_depth_update_2026-07-07T08--part=2.parquet")
    );
}

#[test]
fn hourly_partition_rejects_unsafe_source_kind_and_marks_legacy_source_first() {
    let tmp = tempdir().unwrap();
    let policy = HourlyPartitionPolicy::new(tmp.path()).unwrap();

    assert!(
        policy
            .partition("../evil", 1_783_411_200_000, None)
            .is_err()
    );
    assert!(is_legacy_source_first_partition(&tmp.path().join(
        "training/typed_raw/source_kind=polymarket_market/utc_day=2026-07-07"
    )));
    assert!(!is_legacy_source_first_partition(&tmp.path().join(
        "training/typed_raw/utc_day=2026-07-07/source_kind=polymarket_market"
    )));
}

#[test]
fn finalized_discovery_ignores_hourly_running_and_digesting_files() {
    let tmp = tempdir().unwrap();
    let policy = HourlyPartitionPolicy::new(tmp.path()).unwrap();
    let partition = policy
        .partition("binance_aggtrade", 1_783_411_200_000, None)
        .unwrap();
    fs::create_dir_all(&partition.partition_path).unwrap();

    fs::write(&partition.running_path, b"running").unwrap();
    fs::write(&partition.digesting_path, b"digesting").unwrap();
    fs::write(&partition.final_path, b"final").unwrap();
    fs::write(
        &partition.manifest_path,
        serde_json::to_vec(&complete_manifest_for(
            partition.final_path.file_name().unwrap().to_str().unwrap(),
        ))
        .unwrap(),
    )
    .unwrap();

    let discovered = discover_finalized_artifacts(&partition.partition_path).unwrap();
    assert_eq!(discovered.len(), 1);
    assert_eq!(discovered[0].parquet_path, partition.final_path);
    assert_eq!(discovered[0].manifest_path, partition.manifest_path);
}

fn complete_manifest_for(parquet_file_name: &str) -> ManifestDraft {
    ManifestDraft {
        artifact_path: Some(parquet_file_name.into()),
        row_count: Some(2),
        sha256: Some("0123456789abcdef".to_owned()),
        schema_version: Some("binance_aggtrade_v2".to_owned()),
        code_version: Some("test-code-version".to_owned()),
        collector_binary_sha256: Some("binary-sha256".to_owned()),
        source_config_digest: Some("source-config-digest".to_owned()),
        validation_verdict: ValidationVerdict::Complete,
        consumer_boundary_declarations: required_consumer_boundary_declarations(),
        source_coverage: vec![SourceCoverage {
            source_kind: "binance_aggtrade".to_owned(),
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
        resource_watermark: Some(ResourceWatermark {
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
        }),
        resource_threshold_policy_receipt: Some(policy_receipt()),
        writer_queue_high_watermark: Some(QueueWatermark {
            rows: 2,
            bytes: 512,
            oldest_event_age_ms: 10,
            writer_lag_ms: 5,
        }),
        backpressure_receipt: Some(BackpressureReceipt::empty("binance_aggtrade")),
        flush_latency_ms: Some(3),
        collector_host_id: Some("collector-local".to_owned()),
        collector_region: Some("local-dev".to_owned()),
        required_files: vec![RequiredFile {
            file_role: "typed_raw_parquet".to_owned(),
            path: parquet_file_name.into(),
            finalized: true,
            row_count: Some(2),
            sha256: Some("0123456789abcdef".to_owned()),
            schema_version: Some("binance_aggtrade_v2".to_owned()),
            missing_reason: None,
        }],
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
