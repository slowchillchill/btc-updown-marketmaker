//! 隔离夹具的发布闭包验证；外部资源与命令记录均为明确标记的测试数据，非真实发布证据。

use super::*;
use crate::gate_a_publication::tests::{immutable_file_snapshot, passing_external_resource_facts};
use crate::gate_a_publication::*;
use contracts::Sha256Hex;

fn digest(bytes: &[u8]) -> Sha256Hex {
    Sha256Hex(format!("{:x}", Sha256::digest(bytes)))
}

struct Prepared {
    stage: PathBuf,
    quality: DptObservationQualityReportV1,
    resource: GateAResourceHighWaterReportV1,
    measured: GateAMeasuredBuildStateV1,
    tests: GateATestEvidenceReportV1,
}

fn prepare(
    input: &crate::FrozenDirectSourceInputSetV1,
    selected_scope: &CanonicalRunScope,
    stage: PathBuf,
) -> Prepared {
    fs::create_dir(&stage).unwrap();
    fs::create_dir(stage.join("reports")).unwrap();
    let mut candidate = build(input, selected_scope, &stage.join("artifacts"), false).unwrap();
    let validated = validate_gate_a_candidate(
        &candidate.candidate_path,
        &stage.join(".prepare-validation"),
        &candidate.output_config,
    )
    .unwrap();
    candidate
        .resource_metrics
        .peak_formal_work_directory_bytes_bound = candidate
        .resource_metrics
        .peak_formal_work_directory_bytes_bound
        .max(validated.peak_formal_work_directory_bytes_bound);
    let quality = build_dpt_observation_quality_report(input, &candidate, &validated).unwrap();
    let bytes = encode_dpt_observation_quality_report(&quality).unwrap();
    fs::write(stage.join("reports/quality-isolation-report.json"), &bytes).unwrap();
    let measured = build_dpt_observation_measured_build_state(
        &candidate,
        digest(&bytes),
        regular_directory_bytes(&candidate.candidate_path).unwrap(),
        "1".repeat(40),
    )
    .unwrap();
    assert_eq!(
        decode_dpt_observation_measured_build_state(
            &encode_dpt_observation_measured_build_state(&measured).unwrap()
        )
        .unwrap(),
        measured
    );
    let resource = build_dpt_observation_resource_report_from_state(
        &measured,
        GateAHostFactsV1 {
            operating_system: "fixture-only".into(),
            architecture: "fixture-only".into(),
            cpu_model: "fixture-only".into(),
            logical_cpu_count: 1,
            physical_memory_bytes: 1,
        },
        passing_external_resource_facts(),
    )
    .unwrap();
    assert!(resource.evidence.passed, "{resource:#?}");
    fs::write(
        stage.join("reports/resource-evidence.json"),
        encode_resource_high_water_report(&resource).unwrap(),
    )
    .unwrap();
    let output = "仅测试夹具：模拟成功的外部测试记录，不是实际命令执行证据";
    let tests = build_dpt_observation_test_evidence_report(
        "1".repeat(40),
        quality.facts.artifact_manifests.clone(),
        vec![GateATestCommandEvidenceV1 {
            argv: vec!["fixture-only".into()],
            working_directory: "/home/chillchill/btc-updown-marketmaker".into(),
            exit_code: 0,
            combined_output: output.into(),
            combined_output_sha256: digest(output.as_bytes()),
            covered_evidence_ids: REQUIRED_DPT_OBSERVATION_TEST_EVIDENCE_IDS
                .iter()
                .map(|id| (*id).into())
                .collect(),
        }],
    )
    .unwrap();
    Prepared {
        stage,
        quality,
        resource,
        measured,
        tests,
    }
}

pub(super) fn assert_publication(
    input: &crate::FrozenDirectSourceInputSetV1,
    expected: &GateACandidate,
) {
    let output = TempDir::new().unwrap();
    let prepared = prepare(
        input,
        &expected.scope,
        output.path().join(".fixture.candidate"),
    );
    assert_eq!(
        prepared.quality.facts.selected_source_rows,
        expected.canonical_metrics.selected_rows
    );
    let published =
        finalize_and_publish_dpt_observation_candidate(&prepared.stage, &prepared.tests).unwrap();
    assert!(!prepared.stage.exists());
    assert!(!published.receipt.training_dataset_ready);
    let before = immutable_file_snapshot(&published.root);
    let opened =
        open_published_dpt_observation(&published.root, &output.path().join("read-validation"))
            .unwrap();
    assert_eq!(before, immutable_file_snapshot(&published.root));
    assert_eq!(opened.receipt(), &published.receipt);
    assert_eq!(
        opened.producer_pipeline_core_identity_sha256(),
        &crate::gate_a::pipeline_core_identity_sha256()
    );
    assert_eq!(
        opened.reader_pipeline_core_identity_sha256(),
        &crate::gate_a::pipeline_core_identity_sha256()
    );
    assert_eq!(
        opened.run_manifest().input_manifests.len(),
        input.admitted_daily_inputs().len()
    );
    assert_eq!(
        opened.run_manifest().input_manifest_set_sha256.0,
        input.input_manifest_set_sha256()
    );
    assert_eq!(
        opened.candidate().recomputed_stream_digests,
        expected.canonical_digests
    );
    // 五种游标均读取已重新验证的发布产物，逐行对照构建测试中已有固定业务断言的候选。
    let actual = opened.candidate();
    let mut events = CanonicalEventStoreCursor::open(
        actual.canonical_event_store.manifest_path(),
        2,
        16 * 1024 * 1024,
        100,
    )
    .unwrap();
    assert_eq!(events.next_page(100).unwrap(), records(expected));
    let mut observations_cursor = MarketObservationCursor::open(
        actual.market_observations.manifest_path(),
        None,
        None,
        2,
        16 * 1024 * 1024,
        100,
    )
    .unwrap();
    assert_eq!(
        observations_cursor.next_page(100).unwrap(),
        observations(expected)
    );
    let mut reductions = ReductionReportCursor::open(
        actual.reduction_reports.manifest_path(),
        2,
        16 * 1024 * 1024,
        100,
    )
    .unwrap();
    assert_eq!(reductions.next_page(100).unwrap(), reports(expected));
    let mut event_index = EventTimeIndexCursor::open(
        actual.event_time_index.manifest_path(),
        None,
        2,
        16 * 1024 * 1024,
        100,
    )
    .unwrap();
    let mut expected_index = EventTimeIndexCursor::open(
        &expected.event_time_index.manifest_path,
        None,
        2,
        16 * 1024 * 1024,
        100,
    )
    .unwrap();
    assert_eq!(
        event_index.next_page(100).unwrap(),
        expected_index.next_page(100).unwrap()
    );
    let mut visibility = VisibilityIndexCursor::open(
        actual.visibility_index.manifest_path(),
        None,
        2,
        16 * 1024 * 1024,
        100,
    )
    .unwrap();
    let mut expected_visibility = VisibilityIndexCursor::open(
        &expected.visibility_index.manifest_path,
        None,
        2,
        16 * 1024 * 1024,
        100,
    )
    .unwrap();
    assert_eq!(
        visibility.next_page(100).unwrap(),
        expected_visibility.next_page(100).unwrap()
    );
    assert!(
        open_published_gate_a_final(&published.root, &output.path().join("wrong-purpose")).is_err()
    );
}

#[test]
fn identity_only_publication_reopens_with_empty_event_index_and_zero_l2() {
    let mut fixture = Fixture::new();
    fixture.day(
        DAY,
        DAY_START,
        vec![(
            POLYMARKET_METADATA_SOURCE,
            POLYMARKET_METADATA_SCHEMA,
            vec![metadata(START)],
        )],
    );
    let input = fixture.freeze();
    let output = TempDir::new().unwrap();
    let candidate = build(&input, &scope(), &output.path().join("candidate"), false).unwrap();
    assert_eq!(candidate.event_time_index.manifest.file.rows, 0);
    assert_eq!(candidate.resource_metrics.peak_observation_l2_levels, 0);
    assert_publication(&input, &candidate);
}

#[test]
fn valid_invalid_input_evidence_survives_publication_without_a_fabricated_trade() {
    let at = START + 10;
    let endpoint = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
    let payload_sha256 = digest(b"{").0;
    let envelope = json!({
        "capture_limit_bytes": 524_288, "declared_content_length": null, "endpoint": endpoint,
        "evidence_schema": "binance_aggtrade_invalid_evidence_v1", "invalid_reason_code": "invalid_json",
        "observed_bytes_at_least": 1, "payload_base64": "ew==", "payload_capture_status": "complete",
        "payload_encoding": "base64", "payload_length_bytes": 1, "payload_sha256": payload_sha256,
        "receive_time_utc_ms": at, "transport": "websocket"
    });
    let invalid = json!({
        "record_kind": "invalid_input", "timestamp_received": at, "ingest_source": "websocket",
        "source_native_payload_json": envelope.to_string(), "source_sequence_epoch_id": "aggtrade-epoch-1",
        "connection_generation": 1, "websocket_ingress_sequence": 1, "collector_admission_sequence": 1,
        "collector_admission_time_utc_ms": at + 1, "collector_host_id": "collector-1", "collector_region": "eu-west-1",
        "source_endpoint": endpoint, "schema_version": BINANCE_AGGTRADE_SCHEMA,
        "invalid_reason_code": "invalid_json", "invalid_reason_detail": "测试夹具的非法 JSON 帧",
        "payload_encoding": "base64", "payload_length_bytes": 1, "payload_sha256": payload_sha256,
        "payload_capture_status": "complete"
    });
    let mut fixture = Fixture::new();
    fixture.day(
        DAY,
        DAY_START,
        vec![
            (
                POLYMARKET_METADATA_SOURCE,
                POLYMARKET_METADATA_SCHEMA,
                vec![metadata(START)],
            ),
            (
                BINANCE_AGGTRADE_SOURCE,
                BINANCE_AGGTRADE_SCHEMA,
                vec![invalid],
            ),
        ],
    );
    let input = fixture.freeze();
    let output = TempDir::new().unwrap();
    let candidate = build(&input, &scope(), &output.path().join("candidate"), false).unwrap();
    let reports = reports(&candidate);
    let invalid = reports
        .iter()
        .find(|r| r.reason_code == "BINANCE_AGGTRADE_INVALID_INPUT")
        .unwrap();
    assert_eq!(
        invalid.operation,
        data_pipeline_core::reduction::ReductionOperationKindV1::Reject
    );
    assert_eq!(
        candidate.canonical_metrics.selected_rows.binance_aggtrade,
        1
    );
    assert_eq!(observations(&candidate).len(), 1);
    assert_eq!(candidate.event_time_index.manifest.file.rows, 0);
    assert_publication(&input, &candidate);
}

#[test]
fn recovered_gap_before_identity_is_published_without_fabricated_observation_witness() {
    let mut fixture = Fixture::new();
    let mut after_recovery = update(START + 35, "applied");
    after_recovery["first_update_id"] = json!(201);
    after_recovery["final_update_id"] = json!(201);
    after_recovery["snapshot_last_update_id"] = json!(200);
    after_recovery["depth_ingress_sequence"] = json!(4);
    after_recovery["collector_admission_sequence"] = json!(4);
    fixture.day(
        DAY,
        DAY_START,
        vec![
            (
                POLYMARKET_METADATA_SOURCE,
                POLYMARKET_METADATA_SCHEMA,
                vec![metadata(START + 40)],
            ),
            (
                BINANCE_DEPTH_SNAPSHOT_SOURCE,
                BINANCE_DEPTH_SNAPSHOT_SCHEMA,
                vec![snapshot(START + 10, 1, 100), snapshot(START + 30, 3, 200)],
            ),
            (
                BINANCE_DEPTH_UPDATE_SOURCE,
                BINANCE_DEPTH_UPDATE_SCHEMA,
                vec![
                    update(START + 20, "not_applied_confirmed_hole"),
                    after_recovery,
                ],
            ),
        ],
    );
    let input = fixture.freeze();
    let output = TempDir::new().unwrap();
    let prepared = prepare(&input, &scope(), output.path().join(".fixture.candidate"));
    let gap = prepared
        .quality
        .facts
        .evidence
        .iter()
        .find(|e| e.evidence_id == "quality/gap-resync-watermark")
        .unwrap();
    assert!(!gap.passed, "{gap:#?}");
    assert_eq!(
        gap.blocking_reason.as_deref(),
        Some("quality/gap-resync-watermark/witness_not_observed")
    );
    assert!(gap.event_ids.is_empty());
    assert!(gap.observation_ids.is_empty());
    assert_eq!(prepared.quality.facts.event_quality_counts.confirmed_gap, 1);
    assert_eq!(prepared.quality.facts.fresh_resync_boundary, 1);
    let published =
        finalize_and_publish_dpt_observation_candidate(&prepared.stage, &prepared.tests).unwrap();
    let opened =
        open_published_dpt_observation(&published.root, &output.path().join("validate")).unwrap();
    assert_eq!(
        opened.candidate().market_observations.manifest().file.rows,
        1
    );
    let actual = decode_dpt_observation_quality_report(
        &fs::read(published.root.join("reports/quality-isolation-report.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(actual, prepared.quality);
}

#[test]
fn explicit_range_and_all_frozen_manifests_survive_without_original_inputs() {
    let mut fixture = Fixture::new();
    fixture.day(
        DAY,
        DAY_START,
        vec![(
            POLYMARKET_METADATA_SOURCE,
            POLYMARKET_METADATA_SCHEMA,
            vec![metadata(START)],
        )],
    );
    fixture.day("2026-08-09", DAY_START + DAY_MS, vec![]);
    fixture.day("2026-08-10", DAY_START + 2 * DAY_MS, vec![]);
    let input = fixture.freeze();
    let output = TempDir::new().unwrap();
    // 范围只覆盖两天，提供的清单有三天，所选文件只有一天。
    let selected_scope = CanonicalRunScope::new(CONDITION, START, DAY_START + 2 * DAY_MS).unwrap();
    let prepared = prepare(
        &input,
        &selected_scope,
        output.path().join(".fixture.candidate"),
    );
    let published =
        finalize_and_publish_dpt_observation_candidate(&prepared.stage, &prepared.tests).unwrap();
    drop(input);
    drop(fixture);
    let before = immutable_file_snapshot(&published.root);
    let opened =
        open_published_dpt_observation(&published.root, &output.path().join("validate")).unwrap();
    assert_eq!(opened.run_manifest().covered_utc_dates, [DAY, "2026-08-09"]);
    assert_eq!(
        opened
            .run_manifest()
            .input_manifests
            .iter()
            .map(|binding| binding.utc_day.as_str())
            .collect::<Vec<_>>(),
        [DAY, "2026-08-09", "2026-08-10"]
    );
    assert_eq!(opened.run_manifest().selected_files.len(), 1);
    assert_eq!(before, immutable_file_snapshot(&published.root));
}

#[test]
fn failed_or_missing_test_and_failed_resource_evidence_cannot_publish() {
    let mut fixture = Fixture::new();
    fixture.day(DAY, DAY_START, complete_sources());
    let input = fixture.freeze();
    for mode in 0..3 {
        let output = TempDir::new().unwrap();
        let mut prepared = prepare(&input, &scope(), output.path().join(".fixture.candidate"));
        if mode < 2 {
            let mut commands = prepared.tests.commands;
            if mode == 0 {
                commands[0].exit_code = 1;
            } else {
                commands[0].covered_evidence_ids.pop();
            }
            prepared.tests = build_dpt_observation_test_evidence_report(
                "1".repeat(40),
                prepared.quality.facts.artifact_manifests.clone(),
                commands,
            )
            .unwrap();
            assert!(prepared.tests.evidence.iter().any(|e| !e.passed));
        } else {
            let mut external = passing_external_resource_facts();
            external.memory_peak_bytes = GATE_A_CGROUP_MEMORY_MAX_BYTES + 1;
            prepared.resource = build_dpt_observation_resource_report_from_state(
                &prepared.measured,
                prepared.resource.host.clone(),
                external,
            )
            .unwrap();
            assert!(!prepared.resource.evidence.passed);
            fs::write(
                prepared.stage.join("reports/resource-evidence.json"),
                encode_resource_high_water_report(&prepared.resource).unwrap(),
            )
            .unwrap();
        }
        assert!(
            finalize_and_publish_dpt_observation_candidate(&prepared.stage, &prepared.tests)
                .is_err()
        );
        assert!(!prepared.stage.exists());
        assert_eq!(fs::read_dir(output.path()).unwrap().count(), 0);
    }
}

#[test]
fn existing_receipt_directory_is_never_replaced() {
    let mut fixture = Fixture::new();
    fixture.day(DAY, DAY_START, complete_sources());
    let input = fixture.freeze();
    let output = TempDir::new().unwrap();
    let first = prepare(&input, &scope(), output.path().join(".first.candidate"));
    let published =
        finalize_and_publish_dpt_observation_candidate(&first.stage, &first.tests).unwrap();
    let before = immutable_file_snapshot(&published.root);
    let second = prepare(&input, &scope(), output.path().join(".second.candidate"));
    assert_eq!(first.quality, second.quality);
    assert_eq!(first.resource, second.resource);
    assert!(finalize_and_publish_dpt_observation_candidate(&second.stage, &second.tests).is_err());
    assert!(!second.stage.exists());
    assert_eq!(before, immutable_file_snapshot(&published.root));
    open_published_dpt_observation(&published.root, &output.path().join("validate")).unwrap();
}

// 仅测试持久化边界：以完整摘要闭包模拟另一实现生产的产物，或攻击者重签的虚假统计。
fn rewrite_control_closure(
    root: &Path,
    prepared: &mut Prepared,
    validation_path: &Path,
) -> PathBuf {
    let validated = validate_gate_a_candidate(
        &root.join("artifacts"),
        validation_path,
        &prepared.resource.output_config,
    )
    .unwrap();
    let quality_bytes = encode_dpt_observation_quality_report(&prepared.quality).unwrap();
    prepared.resource.quality_isolation_report_sha256 = digest(&quality_bytes);
    let manifest = build_dpt_observation_run_manifest(
        &prepared.quality,
        &prepared.resource,
        &prepared.tests,
        &validated,
    )
    .unwrap();
    let receipt = build_dpt_observation_receipt(&manifest).unwrap();
    fs::write(
        root.join("reports/quality-isolation-report.json"),
        quality_bytes,
    )
    .unwrap();
    fs::write(
        root.join("reports/resource-evidence.json"),
        encode_resource_high_water_report(&prepared.resource).unwrap(),
    )
    .unwrap();
    fs::write(
        root.join("reports/test-evidence.json"),
        encode_dpt_observation_test_evidence_report(&prepared.tests).unwrap(),
    )
    .unwrap();
    fs::write(
        root.join("observation-run-manifest.json"),
        encode_dpt_observation_run_manifest(&manifest).unwrap(),
    )
    .unwrap();
    fs::write(
        root.join("observation-receipt.json"),
        data_pipeline_core::encode_dpt_observation_receipt_v1(&receipt).unwrap(),
    )
    .unwrap();
    let renamed = root.parent().unwrap().join(receipt.receipt_id.0);
    fs::rename(root, &renamed).unwrap();
    renamed
}

#[test]
fn reopen_keeps_producer_identity_separate_and_rejects_rehashed_false_statistics() {
    let mut fixture = Fixture::new();
    fixture.day(DAY, DAY_START, complete_sources());
    let input = fixture.freeze();
    let output = TempDir::new().unwrap();
    let mut prepared = prepare(&input, &scope(), output.path().join(".fixture.candidate"));
    let published =
        finalize_and_publish_dpt_observation_candidate(&prepared.stage, &prepared.tests).unwrap();
    let producer = Sha256Hex("e".repeat(64));
    prepared.quality.facts.pipeline_core_identity_sha256 = producer.clone();
    prepared.resource.pipeline_core_identity_sha256 = producer.clone();
    prepared.tests.pipeline_core_identity_sha256 = producer.clone();
    let root = rewrite_control_closure(
        &published.root,
        &mut prepared,
        &output.path().join("producer-binding"),
    );
    let before = immutable_file_snapshot(&root);
    let opened =
        open_published_dpt_observation(&root, &output.path().join("open-older-producer")).unwrap();
    assert_eq!(opened.producer_pipeline_core_identity_sha256(), &producer);
    assert_eq!(
        opened.reader_pipeline_core_identity_sha256(),
        &crate::gate_a::pipeline_core_identity_sha256()
    );
    assert_ne!(
        opened.producer_pipeline_core_identity_sha256(),
        opened.reader_pipeline_core_identity_sha256()
    );
    assert_eq!(before, immutable_file_snapshot(&root));
    let (reason, count) = prepared
        .quality
        .facts
        .reduction_reason_counts
        .pop_first()
        .unwrap();
    assert_ne!(reason, "fixture/not-an-observed-reason");
    prepared
        .quality
        .facts
        .reduction_reason_counts
        .insert("fixture/not-an-observed-reason".into(), count);
    let root = rewrite_control_closure(
        &root,
        &mut prepared,
        &output.path().join("rehashed-statistics"),
    );
    // 单个报告及回执均可解码且摘要闭合，但真正归约事实必须由产物重建，不能只信回执。
    data_pipeline_core::decode_dpt_observation_receipt_v1(
        &fs::read(root.join("observation-receipt.json")).unwrap(),
    )
    .unwrap();
    let before = immutable_file_snapshot(&root);
    let error =
        open_published_dpt_observation(&root, &output.path().join("reject-false-statistics"))
            .unwrap_err();
    assert!(
        error.to_string().contains("观测质量事实不能从产物"),
        "{error}"
    );
    assert_eq!(before, immutable_file_snapshot(&root));
}
