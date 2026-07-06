use std::fs;

use data_collect::{
    ManifestDraft, QueueWatermark, RawEvent, RequiredFile, ResourceWatermark, SourceCoverage,
    ValidationVerdict, discover_finalized_parquet, resource::CgroupMemoryStat, resource::PsiSample,
    sha256_file, writer::ParquetSegmentWriter, writer::write_segment_on_blocking_pool,
};
use tempfile::tempdir;

fn synthetic_event(seq: u64) -> RawEvent {
    RawEvent {
        source_kind: "polymarket_market".to_owned(),
        source_event_type: "book".to_owned(),
        source_identifier: format!("asset-{seq}"),
        source_ts: Some("1766789469958".to_owned()),
        recv_time_utc_ms: 1_766_789_469_999 + seq as i64,
        collector_host_id: "collector-local".to_owned(),
        collector_region: "local-dev".to_owned(),
        raw_json: format!(r#"{{"event_type":"book","asset_id":"asset-{seq}"}}"#),
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: "polymarket_market".to_owned(),
    }
}

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

#[test]
fn writer_running_file_is_not_discoverable() {
    let tmp = tempdir().unwrap();
    let writer =
        ParquetSegmentWriter::create(tmp.path(), "probe", "parquet_raw_layout_v1").unwrap();

    fs::write(tmp.path().join("ignored.tmp"), b"partial").unwrap();
    fs::write(tmp.path().join("old-running.parquet"), b"partial").unwrap();
    fs::write(tmp.path().join("old-digesting.parquet"), b"partial").unwrap();
    fs::write(tmp.path().join(".rsync-partial"), b"partial").unwrap();

    let discovered = discover_finalized_parquet(tmp.path()).unwrap();

    assert!(writer.running_path().exists());
    assert_eq!(discovered, Vec::<std::path::PathBuf>::new());
}

#[test]
fn writer_refuses_to_truncate_existing_running_segment() {
    let tmp = tempdir().unwrap();
    let running = tmp.path().join("probe-running.parquet");
    fs::write(&running, b"partial evidence").unwrap();

    let result = ParquetSegmentWriter::create(tmp.path(), "probe", "parquet_raw_layout_v1");
    assert!(result.is_err());
    let err = result.err().unwrap();

    assert!(err.to_string().contains("running parquet already exists"));
    assert_eq!(fs::read(&running).unwrap(), b"partial evidence");
}

#[test]
fn writer_finalize_atomic_rename_publishes_only_finalized() {
    let tmp = tempdir().unwrap();
    let mut writer =
        ParquetSegmentWriter::create(tmp.path(), "probe", "parquet_raw_layout_v1").unwrap();

    writer
        .write_events(&[synthetic_event(1), synthetic_event(2)])
        .unwrap();
    let running_path = writer.running_path().to_path_buf();
    let published = writer.finalize().unwrap();

    assert!(!running_path.exists());
    assert!(published.final_path.exists());
    assert_eq!(published.row_count, 2);
    assert_eq!(
        published.sha256,
        sha256_file(&published.final_path).unwrap()
    );

    let discovered = discover_finalized_parquet(tmp.path()).unwrap();
    assert_eq!(discovered, vec![published.final_path]);
}

#[tokio::test]
async fn writer_segment_can_run_on_blocking_pool() {
    let tmp = tempdir().unwrap();

    let published = write_segment_on_blocking_pool(
        tmp.path().to_path_buf(),
        "probe".to_owned(),
        "parquet_raw_layout_v1".to_owned(),
        vec![vec![synthetic_event(1), synthetic_event(2)]],
    )
    .await
    .unwrap();

    assert!(published.final_path.exists());
    assert_eq!(published.row_count, 2);
    assert_eq!(
        discover_finalized_parquet(tmp.path()).unwrap(),
        vec![published.final_path]
    );
}

#[test]
fn manifest_requires_row_count_sha256_schema_source_coverage() {
    let complete = ManifestDraft {
        artifact_path: Some("probe.parquet".into()),
        row_count: Some(2),
        sha256: Some("0123456789abcdef".to_owned()),
        schema_version: Some("parquet_raw_layout_v1".to_owned()),
        code_version: Some("test-code-version".to_owned()),
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
        resource_watermark: Some(complete_resource_watermark()),
        writer_queue_high_watermark: Some(QueueWatermark {
            rows: 2,
            bytes: 512,
            oldest_event_age_ms: 10,
            writer_lag_ms: 5,
        }),
        flush_latency_ms: Some(3),
        collector_host_id: Some("collector-local".to_owned()),
        collector_region: Some("local-dev".to_owned()),
        required_files: vec![RequiredFile {
            file_role: "typed_raw_parquet".to_owned(),
            path: "probe.parquet".into(),
            finalized: true,
            row_count: Some(2),
            sha256: Some("0123456789abcdef".to_owned()),
            schema_version: Some("parquet_raw_layout_v1".to_owned()),
            missing_reason: None,
        }],
    };

    assert_eq!(complete.effective_verdict(), ValidationVerdict::Complete);

    let mut missing_sha = complete.clone();
    missing_sha.sha256 = None;
    assert_eq!(
        missing_sha.effective_verdict(),
        ValidationVerdict::Incomplete
    );

    let mut missing_coverage = complete.clone();
    missing_coverage.source_coverage.clear();
    assert_eq!(
        missing_coverage.effective_verdict(),
        ValidationVerdict::Incomplete
    );

    let mut missing_resource_metric = complete.clone();
    let resource_watermark = missing_resource_metric.resource_watermark.as_mut().unwrap();
    resource_watermark.psi_io = None;
    assert_eq!(
        missing_resource_metric.effective_verdict(),
        ValidationVerdict::Incomplete
    );

    let mut empty_required_file_role = complete;
    empty_required_file_role.required_files[0].file_role.clear();
    assert_eq!(
        empty_required_file_role.effective_verdict(),
        ValidationVerdict::Incomplete
    );
}
