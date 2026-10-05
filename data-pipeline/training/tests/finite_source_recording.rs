use contracts::*;
use data_collect::{
    source_hourly_layout::RtdsChainlinkBtcUsdHourlyRow, writer::TypedParquetSegmentWriter,
};
use data_pipeline_training::{
    finite_recording::*, public_input::PublicFiniteFileV1, public_replay::*,
};
use serde_json::json;
use std::path::Path;
fn row(t: i64, value: &str) -> RtdsChainlinkBtcUsdHourlyRow {
    RtdsChainlinkBtcUsdHourlyRow {
        timestamp_received: t,
        message_timestamp: t - 1,
        payload_timestamp: t - 2,
        age_ms: Some(2),
        symbol: "btc/usd".into(),
        value: value.into(),
        topic: "crypto_prices_chainlink".into(),
        message_type: "update".into(),
        freshness_status: "fresh".into(),
        subscription_filter: "{\"symbol\":\"btc/usd\"}".into(),
        source_native_payload_json: "{}".into(),
        collector_host_id: "host".into(),
        collector_region: "hk".into(),
        source_endpoint: "wss://ws-live-data.polymarket.com".into(),
        schema_version: "polymarket_rtds_chainlink_btc_usd_v4".into(),
    }
}
fn file(root: &Path, name: &str, rows: &[RtdsChainlinkBtcUsdHourlyRow]) -> PublicFiniteFileV1 {
    let source = "polymarket_rtds_chainlink_btc_usd";
    let version = "polymarket_rtds_chainlink_btc_usd_v4";
    let mut w = TypedParquetSegmentWriter::create_for_current_source(root, name, source).unwrap();
    w.write_rtds_chainlink_btc_usd_rows(rows).unwrap();
    w.append_key_value_metadata("data_collect_daily_file_fact_v1",json!({"source_kind":source,"schema_version":version,"utc_hour":"2026-09-09T23","part":0,"rows":rows.len()}).to_string()).unwrap();
    let s = w.finalize().unwrap();
    PublicFiniteFileV1 {
        bytes: std::fs::metadata(&s.final_path).unwrap().len(),
        path: s.final_path,
        source_kind: source.into(),
        schema_version: version.into(),
        rows: rows.len() as u64,
        sha256: s.sha256,
    }
}
fn config(root: &Path, files: Vec<PublicFiniteFileV1>) -> FinitePublicRecordingConfigV1 {
    let mut canonical = data_pipeline_training::gate_a_formal_canonical_config();
    canonical.decode_batch_rows = 1;
    canonical.sort.batch_rows = 1;
    let mut recording = data_pipeline_training::gate_a_formal_output_config();
    recording.sort.batch_rows = 1;
    FinitePublicRecordingConfigV1 {
        files,
        received_start_utc_ms: 86_399_000,
        received_end_utc_ms: 86_402_000,
        work_root: root.to_owned(),
        header: PublicInputStreamHeaderV1 {
            input_stream_id: "selected-test".into(),
            canonical_rule_version: "canonical-v1".into(),
            observation_version: "public-observation-v1".into(),
            ordering_version: HISTORICAL_SOURCE_HEAD_ORDERING_VERSION.into(),
            input_scope: "selected-chainlink".into(),
            max_pending_ws_terminals: 1024,
            initial_state: PublicInitialStateV1::Empty,
        },
        canonical,
        recording,
    }
}
#[test]
fn selected_parts_cross_midnight_keep_full_record_and_current_state() {
    let temp = tempfile::tempdir().unwrap();
    let f1 = file(temp.path(), "part-a", &[row(86_399_500, "100")]);
    let f2 = file(
        temp.path(),
        "part-b",
        &[row(86_400_500, "104"), row(86_401_500, "103")],
    );
    let c = config(&temp.path().join("prepared"), vec![f1, f2]);
    let mut prepared = Vec::new();
    let prefix = prepare_finite_public_recording_v1(&c, None, |input, applied, _| {
        if let Some(a) = applied {
            prepared.push((input.clone(), a.applied.clone()));
        }
    })
    .unwrap()
    .unwrap();
    assert!(prefix.normal_end);
    let mut replay =
        PublicReplayV1::open_committed(&c.work_root.join("public"), prefix, 1, 64 * 1024 * 1024)
            .unwrap();
    let mut replayed = Vec::new();
    let mut prices = Vec::new();
    while let Some((input, applied, _)) = replay.next_prepared_applied().unwrap() {
        for group in &applied.applied.closed_groups {
            assert_eq!(applied.applied.closed_groups.len(), 1);
            assert!(group.observations.is_empty());
            prices.push(
                replay
                    .core
                    .current_chainlink_spot()
                    .value
                    .as_ref()
                    .unwrap()
                    .price_usd_per_btc
                    .0
                    .clone(),
            );
        }
        replayed.push((input, applied.applied));
    }
    assert_eq!(prepared, replayed);
    assert_eq!(prices, ["100", "104", "103"]);
    assert_eq!(replayed.len(), 4);
}
#[test]
fn selected_file_bad_out_of_range_row_cannot_publish_success() {
    let temp = tempfile::tempdir().unwrap();
    let f = file(
        temp.path(),
        "bad-tail",
        &[row(86_399_500, "100"), row(86_403_000, "not-a-price")],
    );
    let c = config(&temp.path().join("prepared"), vec![f]);
    let error = prepare_finite_public_recording_v1(&c, None, |_, _, _| {}).unwrap_err();
    assert!(error.to_string().contains("value"), "{error}");
    assert!(!c.work_root.join("public/prefix.json").exists());
}

#[test]
fn compact_finite_consumption_crosses_midnight_without_recording_and_propagates_bad_rows() {
    use data_pipeline_training::finite_source_progress::*;
    let temp = tempfile::tempdir().unwrap();
    let files = vec![
        file(temp.path(), "day-a", &[row(86_399_500, "100")]),
        file(
            temp.path(),
            "day-b",
            &[row(86_400_500, "100"), row(86_401_500, "103")],
        ),
    ];
    let original = config(&temp.path().join("unused"), files);
    let mut c = FiniteSourceProgressConfigV1 {
        files: original.files,
        received_start_utc_ms: original.received_start_utc_ms,
        received_end_utc_ms: original.received_end_utc_ms,
        work_root: temp.path().join("compact"),
        header: original.header,
        canonical: original.canonical,
        metadata_initialization: None,
    };
    let mut confirmations = Vec::new();
    let core = prepare_finite_source_progress_v1(&c, None, |_, applied, _, current_core| {
        assert_eq!(current_core.applied_prefix(), applied.applied_prefix);
        let _on_demand_counts = current_core.object_counts();
        for g in &applied.closed_groups {
            for p in &g.source_progress_changes {
                confirmations.push(
                    p.last_valid_business_message
                        .as_ref()
                        .unwrap()
                        .received_at_utc_ms,
                );
            }
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(confirmations, [86_399_500, 86_400_500, 86_401_500]);
    assert_eq!(core.applied_prefix().next_input_position, 4);
    assert!(!c.work_root.join("public").exists());
    c.work_root = temp.path().join("without-telemetry");
    let without = prepare_finite_source_progress_v1(&c, None, |_, _, _, _| Ok(())).unwrap();
    assert_eq!(core.applied_prefix(), without.applied_prefix());
    assert_eq!(core.object_counts(), without.object_counts());
    assert_eq!(
        core.current_chainlink_spot(),
        without.current_chainlink_spot()
    );
    c.files = vec![file(temp.path(), "bad", &[row(86_403_000, "not-a-price")])];
    c.work_root = temp.path().join("bad-compact");
    let error = prepare_finite_source_progress_v1(&c, None, |_, _, _, _| Ok(()))
        .err()
        .unwrap();
    assert!(error.to_string().contains("value"), "{error}");
}
