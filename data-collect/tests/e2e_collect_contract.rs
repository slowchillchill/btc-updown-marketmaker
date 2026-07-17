use std::fs;
use std::path::Path;

use data_collect::backpressure::BackpressureReceipt;
use data_collect::manifest::{
    ManifestDraft, QueueWatermark, RequiredFile, SourceCoverage, TimeWindow,
    required_consumer_boundary_declarations,
};
use data_collect::publish::{discover_finalized_artifacts, manifest_path_for_parquet};
use data_collect::resource::{
    CgroupMemoryStat, PsiSample, ResourceAction, ResourceSeverity, ResourceThresholdPolicyReceipt,
    ResourceThresholds,
};
use data_collect::scheduler::{
    MarketCoveragePhase, ObservedSourceCoverage, SourceRole, SourceScheduler,
};
use data_collect::writer::write_segment_on_blocking_pool_for_source;
use data_collect::{RawEvent, ResourceWatermark, ValidationVerdict};
use tempfile::tempdir;

const CURRENT_START_UTC_MS: i64 = 1_766_789_400_000;
const NEXT_START_UTC_MS: i64 = 1_766_789_700_000;
const MARKET_DURATION_MS: i64 = 300_000;

#[tokio::test]
async fn e2e_four_source_collect_contract() {
    let tmp = tempdir().unwrap();
    let artifact_root = tmp.path().join("artifacts");
    let evidence_root = tmp.path().join("evidence");
    fs::create_dir_all(&artifact_root).unwrap();
    fs::create_dir_all(&evidence_root).unwrap();
    let scheduler = SourceScheduler::four_source(30);
    let windows = scheduler.market_windows(
        "btc-updown-5m-1766789400",
        CURRENT_START_UTC_MS,
        "btc-updown-5m-1766789700",
        NEXT_START_UTC_MS,
        MARKET_DURATION_MS,
    );

    for role in SourceRole::all() {
        let source_kind = role.as_str();
        let published = write_segment_on_blocking_pool_for_source(
            artifact_root.clone(),
            format!("{source_kind}-typed-raw"),
            "parquet_raw_layout_v1".to_owned(),
            source_kind.to_owned(),
            vec![vec![
                synthetic_event(role, 1, windows[0].coverage_start_utc_ms),
                synthetic_event(role, 2, windows[0].market_end_utc_ms),
                synthetic_event(role, 3, windows[1].coverage_start_utc_ms),
                synthetic_event(role, 4, windows[1].market_start_utc_ms),
            ]],
        )
        .await
        .unwrap();
        let health_path = write_evidence(&evidence_root, source_kind, "health").unwrap();
        let gap_path = write_evidence(&evidence_root, source_kind, "source_gap").unwrap();
        let reconnect_path =
            write_evidence(&evidence_root, source_kind, "reconnect_trace").unwrap();

        let manifest = complete_manifest_for_source(
            &published,
            source_kind,
            &windows,
            &health_path,
            &gap_path,
            &reconnect_path,
        );
        assert_eq!(manifest.effective_verdict(), ValidationVerdict::Complete);
        fs::write(
            manifest_path_for_parquet(&published.final_path),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
    }
    fs::write(artifact_root.join("ignored-running.parquet"), b"partial").unwrap();
    fs::write(
        artifact_root.join("ignored.parquet.rsync-partial"),
        b"partial",
    )
    .unwrap();

    let discovered = discover_finalized_artifacts(&artifact_root).unwrap();
    assert_eq!(discovered.len(), SourceRole::all().len());
    let mut observed = Vec::new();
    let mut discovered_source_kinds = Vec::new();
    for artifact in &discovered {
        assert!(artifact.parquet_path.exists());
        assert!(artifact.manifest_path.exists());
        let manifest: ManifestDraft =
            serde_json::from_slice(&fs::read(&artifact.manifest_path).unwrap()).unwrap();
        assert_eq!(manifest.effective_verdict(), ValidationVerdict::Complete);
        assert_eq!(manifest.consumer_boundary_declarations.len(), 2);
        assert_eq!(manifest.source_coverage.len(), 2);
        let manifest_start = manifest.start_time_utc_ms.expect("manifest start");
        let manifest_end = manifest.end_time_utc_ms.expect("manifest end");
        assert!(
            manifest_start
                <= manifest
                    .source_coverage
                    .iter()
                    .map(|coverage| coverage.first_recv_time_utc_ms)
                    .min()
                    .unwrap()
        );
        assert!(
            manifest_end
                >= manifest
                    .source_coverage
                    .iter()
                    .map(|coverage| coverage.last_recv_time_utc_ms)
                    .max()
                    .unwrap()
        );

        for coverage in &manifest.source_coverage {
            let role = source_role_for_source_kind(&coverage.source_kind);
            let (phase, market_slug) = phase_and_slug_for_coverage(coverage, &windows);
            observed.push(ObservedSourceCoverage {
                role,
                phase,
                market_slug,
                coverage: Some(coverage.clone()),
                first_business_event_utc_ms: Some(coverage.first_recv_time_utc_ms),
                source_gap: None,
                source_gap_evidence: None,
            });
            discovered_source_kinds.push(coverage.source_kind.clone());
        }
    }

    discovered_source_kinds.sort();
    discovered_source_kinds.dedup();
    assert_eq!(
        discovered_source_kinds,
        [
            "binance_aggtrade",
            "binance_depth_update",
            "polymarket_market",
            "polymarket_rtds_chainlink_btc_usd"
        ]
    );

    assert_eq!(windows[0].market_end_utc_ms, windows[1].market_start_utc_ms);
    assert_eq!(
        windows[1].coverage_start_utc_ms,
        windows[1].market_start_utc_ms - 30_000
    );

    let report = scheduler.evaluate_four_source_preload_coverage(&windows, &observed);
    assert!(
        report.is_complete_or_gap_accounted(),
        "all four sources must cover current active and planned next preload windows"
    );
}

fn synthetic_event(role: SourceRole, seq: u64, recv_time_utc_ms: i64) -> RawEvent {
    RawEvent {
        source_kind: role.as_str().to_owned(),
        source_event_type: event_type(role).to_owned(),
        source_identifier: format!("{}-{seq}", role.as_str()),
        source_ts: Some(recv_time_utc_ms.to_string()),
        recv_time_utc_ms,
        collector_host_id: "collector-local".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: endpoint(role).to_owned(),
        raw_json: format!(
            r#"{{"source_kind":"{}","event_type":"{}","seq":{seq}}}"#,
            role.as_str(),
            event_type(role)
        ),
        collector_admission: None,
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: role.as_str().to_owned(),
    }
}

fn event_type(role: SourceRole) -> &'static str {
    match role {
        SourceRole::PolymarketMarket => "book",
        SourceRole::BinanceDepth => "depth_update",
        SourceRole::BinanceAggTrade => "aggTrade",
        SourceRole::RtdsChainlinkBtcUsd => "crypto_prices_chainlink",
    }
}

fn endpoint(role: SourceRole) -> &'static str {
    match role {
        SourceRole::PolymarketMarket => "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        SourceRole::BinanceDepth => "wss://stream.binance.com:9443/ws/btcusdt@depth",
        SourceRole::BinanceAggTrade => "wss://stream.binance.com:9443/ws/btcusdt@aggTrade",
        SourceRole::RtdsChainlinkBtcUsd => "wss://ws-subscriptions-clob.polymarket.com/ws/rtds",
    }
}

fn source_role_for_source_kind(source_kind: &str) -> SourceRole {
    SourceRole::all()
        .into_iter()
        .find(|role| role.as_str() == source_kind)
        .unwrap_or_else(|| panic!("unknown source kind {source_kind}"))
}

fn phase_and_slug_for_coverage(
    coverage: &SourceCoverage,
    windows: &[data_collect::scheduler::MarketCoverageWindow; 2],
) -> (MarketCoveragePhase, String) {
    for window in windows {
        let required_end_utc_ms = match window.phase {
            MarketCoveragePhase::CurrentActive => window.market_end_utc_ms,
            MarketCoveragePhase::PlannedNextPreload => window.market_start_utc_ms,
        };
        if coverage.first_recv_time_utc_ms == window.coverage_start_utc_ms
            && coverage.last_recv_time_utc_ms == required_end_utc_ms
        {
            return (window.phase, window.market_slug.clone());
        }
    }
    panic!("manifest source coverage does not match current/preload windows: {coverage:?}");
}

fn write_evidence(
    root: &Path,
    source_kind: &str,
    evidence_kind: &str,
) -> std::io::Result<std::path::PathBuf> {
    let path = root.join(format!("{source_kind}-{evidence_kind}.jsonl"));
    fs::write(
        &path,
        format!(r#"{{"source_kind":"{source_kind}","evidence_kind":"{evidence_kind}"}}"#),
    )?;
    Ok(path)
}

fn complete_manifest_for_source(
    published: &data_collect::PublishedSegment,
    source_kind: &str,
    windows: &[data_collect::scheduler::MarketCoverageWindow; 2],
    health_path: &Path,
    gap_path: &Path,
    reconnect_path: &Path,
) -> ManifestDraft {
    let source_coverage = windows
        .iter()
        .map(|window| {
            let required_end_utc_ms = match window.phase {
                MarketCoveragePhase::CurrentActive => window.market_end_utc_ms,
                MarketCoveragePhase::PlannedNextPreload => window.market_start_utc_ms,
            };
            SourceCoverage {
                source_kind: source_kind.to_owned(),
                first_recv_time_utc_ms: window.coverage_start_utc_ms,
                last_recv_time_utc_ms: required_end_utc_ms,
                rows: 2,
            }
        })
        .collect();

    ManifestDraft {
        artifact_path: Some(published.final_path.clone()),
        row_count: Some(published.row_count),
        sha256: Some(published.sha256.clone()),
        schema_version: Some(published.schema_version.clone()),
        code_version: Some("test-code-version".to_owned()),
        collector_binary_sha256: Some("binary-sha256".to_owned()),
        source_config_digest: Some(format!("{source_kind}-source-config-digest")),
        validation_verdict: ValidationVerdict::Complete,
        consumer_boundary_declarations: required_consumer_boundary_declarations(),
        source_coverage,
        source_gaps: Vec::new(),
        manifest_source_gaps: Vec::new(),
        coverage_incomplete_reason: None,
        health_artifact_path: Some(health_path.to_path_buf()),
        health_artifact_digest: None,
        gap_artifact_path: Some(gap_path.to_path_buf()),
        gap_artifact_digest: None,
        reconnect_artifact_path: Some(reconnect_path.to_path_buf()),
        reconnect_artifact_digest: None,
        start_time_utc_ms: Some(CURRENT_START_UTC_MS),
        end_time_utc_ms: Some(windows[1].market_start_utc_ms),
        coverage_window: Some(TimeWindow {
            start_utc_ms: CURRENT_START_UTC_MS,
            end_utc_ms: CURRENT_START_UTC_MS + MARKET_DURATION_MS,
        }),
        resource_watermark: Some(complete_resource_watermark()),
        resource_threshold_policy_receipt: Some(complete_resource_threshold_policy_receipt()),
        writer_queue_high_watermark: Some(QueueWatermark {
            rows: published.row_count,
            bytes: 1024,
            oldest_event_age_ms: 1,
            writer_lag_ms: 1,
        }),
        backpressure_receipt: Some(BackpressureReceipt::empty(source_kind)),
        flush_latency_ms: Some(1),
        collector_host_id: Some("collector-local".to_owned()),
        collector_region: Some("eu-west-1".to_owned()),
        required_files: vec![RequiredFile {
            file_role: "typed_raw_parquet".to_owned(),
            path: published.final_path.clone(),
            finalized: true,
            row_count: Some(published.row_count),
            sha256: Some(published.sha256.clone()),
            schema_version: Some(published.schema_version.clone()),
            missing_reason: None,
        }],
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
