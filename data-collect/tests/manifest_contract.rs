use data_collect::backpressure::BackpressureReceipt;
use data_collect::manifest::{
    ManifestFailureClass, ManifestSourceGap, SourceGapSeverity, TimeWindow,
    paper_live_consumer_boundary_declaration, required_consumer_boundary_declarations,
};
use data_collect::resource::{
    CgroupMemoryStat, PsiSample, ResourceAction, ResourceSeverity, ResourceThresholdPolicyReceipt,
    ResourceThresholds,
};
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
    ManifestDraft {
        artifact_path: Some("probe.parquet".into()),
        row_count: Some(2),
        sha256: Some("0123456789abcdef".to_owned()),
        schema_version: Some("parquet_raw_layout_v1".to_owned()),
        code_version: Some("git-commit".to_owned()),
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
        health_artifact_path: None,
        health_artifact_digest: None,
        gap_artifact_path: None,
        gap_artifact_digest: None,
        reconnect_artifact_path: None,
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

    let mut missing_window = complete.clone();
    missing_window.coverage_window = None;
    assert_eq!(
        missing_window.effective_verdict(),
        ValidationVerdict::Incomplete
    );

    let mut missing_consumer_boundary = complete.clone();
    missing_consumer_boundary
        .consumer_boundary_declarations
        .clear();
    assert_eq!(
        missing_consumer_boundary.effective_verdict(),
        ValidationVerdict::Incomplete
    );

    let mut legacy_without_consumer_boundary_json = serde_json::to_value(complete.clone()).unwrap();
    legacy_without_consumer_boundary_json
        .as_object_mut()
        .unwrap()
        .remove("consumer_boundary_declarations");
    let legacy_without_consumer_boundary: ManifestDraft =
        serde_json::from_value(legacy_without_consumer_boundary_json).unwrap();
    assert_eq!(
        legacy_without_consumer_boundary.effective_verdict(),
        ValidationVerdict::Incomplete
    );
    assert_eq!(
        legacy_without_consumer_boundary.failure_class(),
        ManifestFailureClass::ArtifactIncomplete
    );

    let mut conflicting_consumer_boundary = complete.clone();
    let mut conflicting_paper_live = paper_live_consumer_boundary_declaration();
    conflicting_paper_live.may_emit_trading_decision = true;
    conflicting_consumer_boundary
        .consumer_boundary_declarations
        .push(conflicting_paper_live);
    assert_eq!(
        conflicting_consumer_boundary.effective_verdict(),
        ValidationVerdict::Incomplete
    );

    for missing_role in [
        "backpressure_state",
        "paper_live_market_context_builder",
        "risk_control",
    ] {
        let mut missing_canonical_role = complete.clone();
        let paper_live = missing_canonical_role
            .consumer_boundary_declarations
            .iter_mut()
            .find(|declaration| {
                declaration.consumer == data_collect::manifest::ConsumerBoundaryKind::PaperLive
            })
            .unwrap();
        paper_live
            .allowed_data_collect_roles
            .retain(|role| role != missing_role);
        paper_live
            .required_external_roles
            .retain(|role| role != missing_role);
        paper_live
            .forbidden_data_collect_roles
            .retain(|role| role != missing_role);
        assert_eq!(
            missing_canonical_role.effective_verdict(),
            ValidationVerdict::Incomplete,
            "{missing_role} must be part of the canonical paper/live boundary"
        );
    }

    let mut missing_backpressure = complete.clone();
    missing_backpressure.backpressure_receipt = None;
    assert_eq!(
        missing_backpressure.effective_verdict(),
        ValidationVerdict::Incomplete
    );

    let mut fatal_backpressure = complete.clone();
    let mut receipt = BackpressureReceipt::empty("polymarket_market");
    receipt.process_fatal = true;
    fatal_backpressure.backpressure_receipt = Some(receipt);
    assert_eq!(
        fatal_backpressure.effective_verdict(),
        ValidationVerdict::Incomplete
    );

    let mut timeout_backpressure = complete.clone();
    let mut receipt = BackpressureReceipt::empty("polymarket_market");
    receipt.send_timeout_count = 1;
    timeout_backpressure.backpressure_receipt = Some(receipt);
    assert_eq!(
        timeout_backpressure.effective_verdict(),
        ValidationVerdict::Incomplete
    );

    let mut dropped_backpressure = complete.clone();
    let mut receipt = BackpressureReceipt::empty("polymarket_market");
    receipt.drop_count = 1;
    dropped_backpressure.backpressure_receipt = Some(receipt);
    assert_eq!(
        dropped_backpressure.effective_verdict(),
        ValidationVerdict::Incomplete
    );

    let mut paused_backpressure = complete.clone();
    let mut receipt = BackpressureReceipt::empty("polymarket_market");
    receipt.pause_intake_count = 1;
    paused_backpressure.backpressure_receipt = Some(receipt);
    assert_eq!(
        paused_backpressure.effective_verdict(),
        ValidationVerdict::Incomplete
    );

    let mut missing_policy_receipt = complete.clone();
    missing_policy_receipt.resource_threshold_policy_receipt = None;
    assert_eq!(
        missing_policy_receipt.effective_verdict(),
        ValidationVerdict::Incomplete
    );

    let mut incomplete_policy_receipt = complete.clone();
    let mut receipt = complete_resource_threshold_policy_receipt();
    receipt.hard_hysteresis_samples = 0;
    incomplete_policy_receipt.resource_threshold_policy_receipt = Some(receipt);
    assert_eq!(
        incomplete_policy_receipt.effective_verdict(),
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
fn segment_manifest_must_not_depend_on_deleted_runtime_sidecars() {
    let complete = complete_manifest();
    assert_eq!(
        complete.effective_verdict(),
        ValidationVerdict::Complete,
        "source coverage, gap, reconnect, and resource evidence can be self-contained"
    );

    for forbidden in [
        "training/runtime_artifacts/run/source_coverage.json",
        "training/runtime_artifacts/run/source_gaps.json",
        "training/runtime_artifacts/run/rollover_edges.json",
    ] {
        let mut manifest = complete.clone();
        manifest.health_artifact_path = Some(forbidden.into());
        assert_eq!(
            manifest.effective_verdict(),
            ValidationVerdict::Incomplete,
            "{forbidden} must not be accepted as required segment evidence"
        );

        let mut manifest = complete.clone();
        manifest.gap_artifact_path = Some(forbidden.into());
        assert_eq!(
            manifest.effective_verdict(),
            ValidationVerdict::Incomplete,
            "{forbidden} must not be accepted as required gap evidence"
        );

        let mut manifest = complete.clone();
        manifest.reconnect_artifact_path = Some(forbidden.into());
        assert_eq!(
            manifest.effective_verdict(),
            ValidationVerdict::Incomplete,
            "{forbidden} must not be accepted as required reconnect evidence"
        );
    }
}

#[test]
fn manifest_backpressure_receipt_must_cover_all_source_coverage_kinds() {
    let mut manifest = complete_manifest();
    manifest.source_coverage.push(SourceCoverage {
        source_kind: "binance_depth_update".to_owned(),
        first_recv_time_utc_ms: 1,
        last_recv_time_utc_ms: 2,
        rows: 10,
    });

    assert_eq!(manifest.effective_verdict(), ValidationVerdict::Incomplete);

    let mut receipt = BackpressureReceipt::empty("polymarket_market");
    receipt.source_kinds.push("binance_depth_update".to_owned());
    manifest.backpressure_receipt = Some(receipt);
    assert_eq!(manifest.effective_verdict(), ValidationVerdict::Complete);
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

    let mut manifest = complete_manifest();
    manifest
        .source_gaps
        .push("binance_depth_stitch_gap".to_owned());
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
