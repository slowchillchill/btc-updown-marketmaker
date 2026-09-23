//! 发布后通过真实游标读取，保留业务结果与目录所有权。
use super::*;
use crate::gate_a_publication::*;
use contracts::Sha256Hex;

fn digest(bytes: &[u8]) -> Sha256Hex {
    Sha256Hex(format!("{:x}", Sha256::digest(bytes)))
}
fn immutable_file_snapshot(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    fn visit(root: &Path, path: &Path, out: &mut Vec<(PathBuf, Vec<u8>)>) {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, out);
            } else {
                out.push((
                    path.strip_prefix(root).unwrap().to_owned(),
                    fs::read(path).unwrap(),
                ));
            }
        }
    }
    let mut out = Vec::new();
    visit(root, root, &mut out);
    out.sort();
    out
}
fn prepare(
    input: &crate::FrozenDirectSourceInputSetV1,
    scope: &CanonicalRunScope,
    stage: PathBuf,
) -> PathBuf {
    build_gate_a_publication(
        input,
        scope,
        &stage,
        &gate_a_formal_canonical_config(),
        &gate_a_formal_output_config(),
    )
    .unwrap();
    stage
}
pub(super) fn assert_publication(
    input: &crate::FrozenDirectSourceInputSetV1,
    expected: &GateACandidate,
) {
    let output = TempDir::new().unwrap();
    let stage = prepare(input, &expected.scope, output.path().join("candidate"));
    let published = publish_gate_a_candidate(&stage, &output.path().join("published")).unwrap();
    assert!(!stage.exists());
    let before = immutable_file_snapshot(&published);
    let opened = open_published_gate_a_final(&published).unwrap();
    assert_eq!(before, immutable_file_snapshot(&published));
    // 五种真实游标逐行对照已有固定业务断言的候选。
    let actual = opened.artifacts();
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
    assert_eq!(candidate.peak_observation_l2_levels, 0);
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
    let published = publish_gate_a_candidate(&prepared, &output.path().join("published")).unwrap();
    let opened = open_published_gate_a_final(&published).unwrap();
    assert_eq!(
        opened.artifacts().market_observations.manifest().file.rows,
        1
    );
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
    let published = publish_gate_a_candidate(&prepared, &output.path().join("published")).unwrap();
    drop(input);
    drop(fixture);
    let before = immutable_file_snapshot(&published);
    let opened = open_published_gate_a_final(&published).unwrap();
    let manifest = opened.artifacts().canonical_event_store.manifest();
    assert_eq!(manifest.received_start_utc_ms, START);
    assert_eq!(manifest.received_end_utc_ms, DAY_START + 2 * DAY_MS);
    assert_eq!(before, immutable_file_snapshot(&published));
}

#[test]
fn existing_publication_is_not_replaced_and_failed_publish_keeps_input() {
    let mut fixture = Fixture::new();
    fixture.day(DAY, DAY_START, complete_sources());
    let input = fixture.freeze();
    let output = TempDir::new().unwrap();
    let first = prepare(&input, &scope(), output.path().join("first"));
    let destination = output.path().join("published");
    publish_gate_a_candidate(&first, &destination).unwrap();
    let before = immutable_file_snapshot(&destination);
    let second = prepare(&input, &scope(), output.path().join("second"));
    let second_before = immutable_file_snapshot(&second);
    assert!(publish_gate_a_candidate(&second, &destination).is_err());
    assert_eq!(before, immutable_file_snapshot(&destination));
    assert_eq!(second_before, immutable_file_snapshot(&second));
    open_published_gate_a_final(&destination).unwrap();
}

#[test]
fn failed_build_only_cleans_the_directory_it_created() {
    let mut fixture = Fixture::new();
    fixture.day(DAY, DAY_START, complete_sources());
    let input = fixture.freeze();
    let root = TempDir::new().unwrap();
    let existing = root.path().join("existing");
    fs::create_dir(&existing).unwrap();
    fs::write(existing.join("user-content"), b"keep").unwrap();
    let config = gate_a_formal_canonical_config();
    let mut output = gate_a_formal_output_config();
    assert!(build_gate_a_publication(&input, &scope(), &existing, &config, &output).is_err());
    assert_eq!(fs::read(existing.join("user-content")).unwrap(), b"keep");
    output.max_artifact_payload_bytes = 0;
    let fresh = root.path().join("fresh");
    assert!(build_gate_a_publication(&input, &scope(), &fresh, &config, &output).is_err());
    assert!(!fresh.exists());
}
