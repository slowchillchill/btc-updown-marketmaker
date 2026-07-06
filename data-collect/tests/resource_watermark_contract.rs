use data_collect::resource::{
    CgroupMemoryStat, MetricUnavailable, PsiSample, ResourceAction, ResourcePolicy, ResourceSample,
    ResourceSeverity, ResourceThresholds,
};

fn complete_sample() -> ResourceSample {
    ResourceSample {
        rss_bytes: Some(100),
        cpu_percent: Some(12.5),
        disk_free_bytes: Some(10_000),
        artifact_dir_bytes: Some(2_000),
        journal_log_bytes: Some(1_000),
        journal_limit_status: Some("within_limit".to_owned()),
        open_files: Some(32),
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

fn hard_thresholds() -> ResourceThresholds {
    ResourceThresholds {
        min_disk_free_bytes: 1_000,
        max_rss_bytes: 20_000,
        max_cpu_percent: 95.0,
        max_artifact_dir_bytes: 50_000,
        max_journal_log_bytes: 20_000,
        max_open_files: 1_000,
        max_queue_bytes: 10_000,
        max_queue_age_ms: 5_000,
        max_parquet_writer_memory_estimate_bytes: 20_000,
        max_cgroup_memory_current_bytes: 25_000,
        max_cgroup_memory_anon_bytes: 10_000,
        max_cgroup_memory_file_bytes: 10_000,
        max_cgroup_memory_slab_bytes: 10_000,
        max_psi_some_avg10: 90.0,
        max_psi_full_avg10: 50.0,
        max_stage_latency_ms: 1_000,
        max_ttl_ms: 10_000,
    }
}

fn soft_thresholds() -> ResourceThresholds {
    ResourceThresholds {
        min_disk_free_bytes: 5_000,
        max_rss_bytes: 10_000,
        max_cpu_percent: 80.0,
        max_artifact_dir_bytes: 20_000,
        max_journal_log_bytes: 10_000,
        max_open_files: 500,
        max_queue_bytes: 2_000,
        max_queue_age_ms: 1_000,
        max_parquet_writer_memory_estimate_bytes: 10_000,
        max_cgroup_memory_current_bytes: 15_000,
        max_cgroup_memory_anon_bytes: 5_000,
        max_cgroup_memory_file_bytes: 5_000,
        max_cgroup_memory_slab_bytes: 5_000,
        max_psi_some_avg10: 20.0,
        max_psi_full_avg10: 10.0,
        max_stage_latency_ms: 250,
        max_ttl_ms: 5_000,
    }
}

#[test]
fn resource_watermark_records_required_metrics_or_unavailable_reason() {
    let complete = complete_sample().into_watermark();
    assert!(complete.unavailable.is_empty());
    assert_eq!(complete.rss_bytes, Some(100));
    assert_eq!(complete.cpu_percent, Some(12.5));
    assert_eq!(complete.disk_free_bytes, Some(10_000));
    assert_eq!(complete.artifact_dir_bytes, Some(2_000));
    assert_eq!(complete.journal_log_bytes, Some(1_000));
    assert_eq!(complete.open_files, Some(32));
    assert_eq!(complete.queue_bytes, Some(512));
    assert_eq!(complete.queue_age_ms, Some(10));
    assert_eq!(complete.parquet_writer_memory_estimate_bytes, Some(4096));
    assert_eq!(complete.cgroup_memory_current_bytes, Some(8192));
    assert_eq!(
        complete.cgroup_memory_stat.as_ref().unwrap().anon_bytes,
        100
    );
    assert!(complete.psi_cpu.is_some());
    assert!(complete.psi_memory.is_some());
    assert!(complete.psi_io.is_some());
    assert_eq!(complete.stage_latency_ms, Some(3));
    assert_eq!(complete.ttl_ms, Some(1000));

    let mut partial = complete_sample();
    partial.psi_io = None;
    partial.unavailable.push(MetricUnavailable {
        metric: "psi_io".to_owned(),
        reason: "not available in kernel".to_owned(),
    });

    let watermark = partial.into_watermark();
    assert_eq!(watermark.psi_io, None);
    assert_eq!(watermark.unavailable[0].metric, "psi_io");
}

#[test]
fn hard_threshold_records_degrade_stop_and_finalize_actions() {
    let policy = ResourcePolicy::new(hard_thresholds(), soft_thresholds());
    let mut sample = complete_sample();
    sample.disk_free_bytes = Some(500);

    let verdict = policy.evaluate(sample.into_watermark());

    assert_eq!(verdict.severity, ResourceSeverity::HardDegraded);
    assert!(!verdict.process_fatal);
    assert!(verdict.actions.contains(&ResourceAction::StopNewIntake));
    assert!(
        verdict
            .actions
            .contains(&ResourceAction::FinalizeOpenSegments)
    );
    assert!(
        verdict
            .actions
            .contains(&ResourceAction::WriteIncompleteManifest)
    );
}

#[test]
fn soft_threshold_records_degraded_without_process_fatal() {
    let policy = ResourcePolicy::new(hard_thresholds(), soft_thresholds());
    let mut sample = complete_sample();
    sample.queue_bytes = Some(3_000);

    let verdict = policy.evaluate(sample.into_watermark());

    assert_eq!(verdict.severity, ResourceSeverity::SoftDegraded);
    assert!(!verdict.process_fatal);
    assert_eq!(
        verdict.actions,
        vec![
            ResourceAction::RecordBackpressure,
            ResourceAction::ObserveRecovery
        ]
    );
}

#[test]
fn resource_policy_evaluates_non_disk_watermarks() {
    let policy = ResourcePolicy::new(hard_thresholds(), soft_thresholds());

    let mut sample = complete_sample();
    sample.rss_bytes = Some(25_000);
    assert_eq!(
        policy.evaluate(sample.into_watermark()).severity,
        ResourceSeverity::HardDegraded
    );

    let mut sample = complete_sample();
    sample.open_files = Some(750);
    assert_eq!(
        policy.evaluate(sample.into_watermark()).severity,
        ResourceSeverity::SoftDegraded
    );

    let mut sample = complete_sample();
    sample.psi_memory = Some(PsiSample {
        some_avg10: 95.0,
        full_avg10: 55.0,
    });
    assert_eq!(
        policy.evaluate(sample.into_watermark()).severity,
        ResourceSeverity::HardDegraded
    );
}
