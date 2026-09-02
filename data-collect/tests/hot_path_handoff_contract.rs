use base64::Engine as _;
use data_collect::binance_aggtrade::BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES;
use data_collect::handoff::{
    DEPTH_HANDOFF_SOURCE_EVIDENCE_SCHEMA_VERSION, DepthHandoffRecordKind,
    DepthHandoffSourceEvidence, HANDOFF_SOURCE_EVIDENCE_SCHEMA_VERSION, HandoffFrame,
    HandoffFrameType, HandoffRecordKind, HandoffRuntimeConfig, HandoffSourceEvidence,
    RuntimeReceivedFrame, run_single_frame_unix_handoff, run_single_frame_unix_server_handoff,
};
use data_collect::publish::PaperLiveEnvironment;
use data_collect::source_hourly_layout::{
    BINANCE_AGGTRADE_CAPTURE_COMPLETE, BINANCE_AGGTRADE_RECORD_INVALID_INPUT,
    BINANCE_AGGTRADE_RECORD_TRADE, BINANCE_AGGTRADE_SCHEMA_VERSION,
    BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION, BINANCE_DEPTH_UPDATE_SCHEMA_VERSION,
    BINANCE_DEPTH_VERDICT_APPLIED, BinanceAggTradeHourlyRow, BinanceDepthSnapshotHourlyRow,
    BinanceDepthUpdateHourlyRow,
};
use sha2::{Digest as _, Sha256};
use tempfile::tempdir;

fn websocket_trade_row() -> BinanceAggTradeHourlyRow {
    BinanceAggTradeHourlyRow {
        record_kind: BINANCE_AGGTRADE_RECORD_TRADE.to_owned(),
        timestamp_received: 1_767_225_600_010,
        rest_backfill_timestamp_received: None,
        ingest_source: "websocket".to_owned(),
        event_type: Some("aggTrade".to_owned()),
        event_time: Some(1_767_225_600_000),
        trade_time: Some(1_767_225_600_001),
        symbol: Some("BTCUSDT".to_owned()),
        aggregate_trade_id: Some(123),
        price: Some("100000.00".to_owned()),
        quantity: Some("0.001".to_owned()),
        first_trade_id: Some(456),
        last_trade_id: Some(456),
        buyer_is_maker: Some(false),
        stream: Some("btcusdt@aggTrade".to_owned()),
        ws_ignore: Some(true),
        rest_was_best_price_match: None,
        source_native_payload_json: serde_json::json!({
            "websocket": {
                "source": "binance_aggtrade_websocket",
                "stream": "btcusdt@aggTrade",
                "payload": {
                    "e": "aggTrade",
                    "E": 1_767_225_600_000_i64,
                    "s": "BTCUSDT",
                    "a": 123,
                    "p": "100000.00",
                    "q": "0.001",
                    "f": 456,
                    "l": 456,
                    "T": 1_767_225_600_001_i64,
                    "m": false,
                    "M": true,
                }
            },
            "rest_backfill": null,
        })
        .to_string(),
        source_sequence_epoch_id: "aggtrade-source-epoch-trade".to_owned(),
        connection_generation: 3,
        websocket_ingress_sequence: Some(17),
        rest_response_ingress_sequence: None,
        rest_response_item_index: None,
        collector_admission_sequence: 9,
        collector_admission_time_utc_ms: 1_767_225_600_011,
        invalid_reason_code: None,
        invalid_reason_detail: None,
        payload_encoding: None,
        payload_length_bytes: None,
        payload_sha256: None,
        payload_capture_status: None,
        collector_host_id: "collector-host".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://stream.binance.com/ws/btcusdt@aggTrade".to_owned(),
        schema_version: BINANCE_AGGTRADE_SCHEMA_VERSION.to_owned(),
    }
}

fn websocket_invalid_row() -> BinanceAggTradeHourlyRow {
    let payload = b"{";
    let payload_base64 = base64::engine::general_purpose::STANDARD.encode(payload);
    let payload_sha256 = format!("{:x}", Sha256::digest(payload));
    let timestamp_received = 1_767_225_600_020;
    let source_endpoint = "wss://stream.binance.com/ws/btcusdt@aggTrade";
    let source_native_payload_json = serde_json::json!({
        "evidence_schema": "binance_aggtrade_invalid_evidence_v1",
        "transport": "websocket",
        "endpoint": source_endpoint,
        "receive_time_utc_ms": timestamp_received,
        "invalid_reason_code": "malformed_json",
        "payload_encoding": "base64",
        "payload_length_bytes": payload.len(),
        "payload_sha256": payload_sha256,
        "payload_capture_status": BINANCE_AGGTRADE_CAPTURE_COMPLETE,
        "payload_base64": payload_base64,
        "observed_bytes_at_least": payload.len(),
        "declared_content_length": null,
        "capture_limit_bytes": BINANCE_AGGTRADE_INVALID_INPUT_MAX_BYTES,
    })
    .to_string();

    BinanceAggTradeHourlyRow {
        record_kind: BINANCE_AGGTRADE_RECORD_INVALID_INPUT.to_owned(),
        timestamp_received,
        rest_backfill_timestamp_received: None,
        ingest_source: "websocket".to_owned(),
        event_type: None,
        event_time: None,
        trade_time: None,
        symbol: None,
        aggregate_trade_id: None,
        price: None,
        quantity: None,
        first_trade_id: None,
        last_trade_id: None,
        buyer_is_maker: None,
        stream: None,
        ws_ignore: None,
        rest_was_best_price_match: None,
        source_native_payload_json,
        source_sequence_epoch_id: "aggtrade-source-epoch-invalid".to_owned(),
        connection_generation: 3,
        websocket_ingress_sequence: Some(18),
        rest_response_ingress_sequence: None,
        rest_response_item_index: None,
        collector_admission_sequence: 10,
        collector_admission_time_utc_ms: 1_767_225_600_021,
        invalid_reason_code: Some("malformed_json".to_owned()),
        invalid_reason_detail: None,
        payload_encoding: Some("base64".to_owned()),
        payload_length_bytes: Some(payload.len() as u64),
        payload_sha256: Some(payload_sha256),
        payload_capture_status: Some(BINANCE_AGGTRADE_CAPTURE_COMPLETE.to_owned()),
        collector_host_id: "collector-host".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: source_endpoint.to_owned(),
        schema_version: BINANCE_AGGTRADE_SCHEMA_VERSION.to_owned(),
    }
}

#[test]
fn paper_and_live_handoff_roots_are_distinct_but_schema_is_shared() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();

    let paper =
        HandoffRuntimeConfig::for_environment(&dataset_root, PaperLiveEnvironment::Paper).unwrap();
    let live =
        HandoffRuntimeConfig::for_environment(&dataset_root, PaperLiveEnvironment::Live).unwrap();

    assert!(
        paper
            .socket_path
            .ends_with("paper/data_collect_handoff/hot.sock")
    );
    assert!(
        live.socket_path
            .ends_with("live/data_collect_handoff/hot.sock")
    );
    assert_ne!(paper.socket_path, live.socket_path);
    assert_eq!(paper.frame_schema_version, live.frame_schema_version);

    assert!(
        HandoffRuntimeConfig::for_environment(
            std::path::Path::new("relative-dataset-root"),
            PaperLiveEnvironment::Paper,
        )
        .is_err()
    );
    assert!(
        HandoffRuntimeConfig::for_environment(
            &dataset_root.join("..").join("escape"),
            PaperLiveEnvironment::Live,
        )
        .is_err()
    );
}

#[test]
fn generic_external_market_event_frame_remains_available_for_other_sources() {
    let frame = HandoffFrame::external_market_event(
        42,
        "polymarket_market",
        "price_change",
        "condition-1",
        Some("1767225600000"),
        1_767_225_600_010,
        1_767_225_600_011,
        "collector-host",
        "eu-west-1",
        r#"{"event_type":"price_change"}"#,
    )
    .unwrap();

    assert_eq!(frame.frame_type, HandoffFrameType::ExternalMarketEvent);
    assert_eq!(frame.event_seq, 42);
    assert_eq!(frame.collector_recv_time_utc_ms, 1_767_225_600_010);
    assert_eq!(frame.handoff_send_time_utc_ms, 1_767_225_600_011);
    assert_eq!(frame.collector_region, "eu-west-1");
    assert!(frame.raw_json.as_deref().unwrap().contains("price_change"));
    assert!(frame.source_evidence.is_none());
}

#[test]
fn aggtrade_external_event_rejects_legacy_generic_frame_without_source_evidence() {
    assert!(
        HandoffFrame::external_market_event(
            43,
            "binance_aggtrade",
            "aggTrade",
            "BTCUSDT",
            Some("1767225600000"),
            1_767_225_600_010,
            1_767_225_600_011,
            "collector-host",
            "eu-west-1",
            r#"{"e":"aggTrade"}"#,
        )
        .is_err()
    );
}

#[test]
fn invalid_source_evidence_round_trips_with_independent_correlation_coordinates() {
    let evidence = HandoffSourceEvidence::invalid_input(
        "aggtrade-source-epoch-7",
        3,
        17,
        9,
        1_767_225_600_010,
        "malformed_json",
    )
    .unwrap();
    let frame = HandoffFrame::source_evidence(
        400,
        "binance_aggtrade",
        "invalid_input",
        "BTCUSDT",
        None,
        1_767_225_600_009,
        1_767_225_600_011,
        "collector-host",
        "eu-west-1",
        Some(r#"{"payload_encoding":"base64","payload":"e30="}"#),
        evidence,
    )
    .unwrap();

    let json_line = frame.to_json_line().unwrap();
    let restored: HandoffFrame = serde_json::from_str(&json_line).unwrap();
    let restored_evidence = restored.source_evidence.as_ref().unwrap();

    assert_eq!(restored, frame);
    assert_eq!(
        restored_evidence.record_kind,
        HandoffRecordKind::InvalidInput
    );
    assert_eq!(
        restored_evidence.evidence_schema_version,
        HANDOFF_SOURCE_EVIDENCE_SCHEMA_VERSION
    );
    assert_eq!(
        restored_evidence.correlation_key(),
        ("aggtrade-source-epoch-7", 9)
    );
    assert_eq!(restored_evidence.connection_generation, 3);
    assert_eq!(restored_evidence.websocket_ingress_sequence, 17);
    assert_eq!(
        restored_evidence.invalid_reason_code.as_deref(),
        Some("malformed_json")
    );
    assert_eq!(restored.event_seq, 400);
    assert_ne!(
        restored.event_seq, restored_evidence.collector_admission_sequence,
        "handoff transport event_seq must not masquerade as collector admission order"
    );
}

#[test]
fn websocket_trade_evidence_keeps_connection_and_ingress_coordinates() {
    let evidence =
        HandoffSourceEvidence::trade("aggtrade-source-epoch-8", 4, 31, 41, 1_767_225_600_020)
            .unwrap();
    let frame = HandoffFrame::source_evidence(
        900,
        "binance_aggtrade",
        "aggTrade",
        "BTCUSDT",
        Some("1767225600019"),
        1_767_225_600_019,
        1_767_225_600_021,
        "collector-host",
        "eu-west-1",
        Some(r#"{"e":"aggTrade","a":123}"#),
        evidence,
    )
    .unwrap();

    let evidence = frame.source_evidence.as_ref().unwrap();
    assert_eq!(evidence.record_kind.as_str(), "trade");
    assert_eq!(evidence.connection_generation, 4);
    assert_eq!(evidence.websocket_ingress_sequence, 31);
    assert!(evidence.invalid_reason_code.is_none());
}

#[test]
fn aggtrade_hourly_rows_map_to_matching_handoff_evidence() {
    let cases = [
        (websocket_trade_row(), HandoffRecordKind::Trade, 4_001),
        (
            websocket_invalid_row(),
            HandoffRecordKind::InvalidInput,
            4_002,
        ),
    ];

    for (row, expected_kind, handoff_event_seq) in cases {
        assert!(row.validate().is_complete(), "{:?}", row.validate());
        let evidence = HandoffSourceEvidence::try_from(&row).unwrap();
        assert_eq!(evidence.record_kind, expected_kind);
        assert_eq!(
            evidence.source_sequence_epoch_id,
            row.source_sequence_epoch_id
        );
        assert_eq!(evidence.connection_generation, row.connection_generation);
        assert_eq!(
            evidence.websocket_ingress_sequence,
            row.websocket_ingress_sequence.unwrap()
        );
        assert_eq!(
            evidence.collector_admission_sequence,
            row.collector_admission_sequence
        );
        assert_eq!(
            evidence.collector_admission_time_utc_ms,
            row.collector_admission_time_utc_ms
        );
        assert_eq!(
            evidence.invalid_reason_code.as_deref(),
            row.invalid_reason_code.as_deref()
        );

        let frame = HandoffFrame::source_evidence(
            handoff_event_seq,
            "binance_aggtrade",
            match expected_kind {
                HandoffRecordKind::Trade => "aggTrade",
                HandoffRecordKind::InvalidInput => "invalid_input",
            },
            "BTCUSDT",
            row.trade_time
                .map(|timestamp| timestamp.to_string())
                .as_deref(),
            row.timestamp_received,
            row.collector_admission_time_utc_ms + 1,
            &row.collector_host_id,
            &row.collector_region,
            Some(&row.source_native_payload_json),
            evidence,
        )
        .unwrap();
        assert_eq!(frame.event_seq, handoff_event_seq);
        assert_ne!(frame.event_seq, row.collector_admission_sequence);
        assert_ne!(frame.event_seq, row.websocket_ingress_sequence.unwrap());
    }
}

#[test]
fn depth_snapshot_and_update_handoff_evidence_preserve_causal_coordinates() {
    let snapshot =
        DepthHandoffSourceEvidence::snapshot("depth-source-epoch-1", 7, 12, 20, 1_767_225_600_021)
            .unwrap();
    let snapshot_frame = HandoffFrame::depth_source_evidence(
        901,
        "binance_depth_snapshot",
        "depth_snapshot",
        "BTCUSDT",
        None,
        1_767_225_600_020,
        1_767_225_600_022,
        "collector-host",
        "eu-west-1",
        Some(r#"{"lastUpdateId":100}"#),
        snapshot,
    )
    .unwrap();

    let update = DepthHandoffSourceEvidence::update(
        "depth-source-epoch-1",
        7,
        11,
        21,
        1_767_225_600_023,
        "applied",
    )
    .unwrap();
    let update_frame = HandoffFrame::depth_source_evidence(
        902,
        "binance_depth_update",
        "depth_update",
        "BTCUSDT",
        Some("1767225600019"),
        1_767_225_600_019,
        1_767_225_600_024,
        "collector-host",
        "eu-west-1",
        Some(r#"{"e":"depthUpdate","U":101,"u":101}"#),
        update,
    )
    .unwrap();

    let snapshot_evidence = snapshot_frame.depth_source_evidence.as_ref().unwrap();
    let update_evidence = update_frame.depth_source_evidence.as_ref().unwrap();
    assert_eq!(
        snapshot_evidence.evidence_schema_version,
        DEPTH_HANDOFF_SOURCE_EVIDENCE_SCHEMA_VERSION
    );
    assert_eq!(
        snapshot_evidence.record_kind,
        DepthHandoffRecordKind::Snapshot
    );
    assert_eq!(update_evidence.record_kind, DepthHandoffRecordKind::Update);
    assert_eq!(snapshot_evidence.depth_ingress_sequence, 12);
    assert_eq!(update_evidence.depth_ingress_sequence, 11);
    assert_eq!(snapshot_evidence.collector_admission_sequence, 20);
    assert_eq!(update_evidence.collector_admission_sequence, 21);
    assert_eq!(update_evidence.stitch_verdict.as_deref(), Some("applied"));
    assert!(snapshot_evidence.stitch_verdict.is_none());
    assert_ne!(
        snapshot_frame.event_seq,
        snapshot_evidence.depth_ingress_sequence
    );
    assert_ne!(
        update_frame.event_seq,
        update_evidence.collector_admission_sequence
    );

    let restored: HandoffFrame =
        serde_json::from_str(&update_frame.to_json_line().unwrap()).unwrap();
    assert_eq!(restored, update_frame);
}

#[test]
fn depth_handoff_evidence_rejects_wrong_source_and_invalid_variant_fields() {
    assert!(
        DepthHandoffSourceEvidence::update("depth-source-epoch-1", 1, 1, 1, 1_767_225_600_001, "",)
            .is_err()
    );

    let evidence =
        DepthHandoffSourceEvidence::snapshot("depth-source-epoch-1", 1, 1, 1, 1_767_225_600_001)
            .unwrap();
    assert!(
        HandoffFrame::depth_source_evidence(
            1,
            "binance_depth_update",
            "depth_update",
            "BTCUSDT",
            None,
            1_767_225_600_001,
            1_767_225_600_002,
            "collector-host",
            "eu-west-1",
            Some(r#"{"e":"depthUpdate"}"#),
            evidence,
        )
        .is_err()
    );
}

#[test]
fn exact_depth_hourly_rows_map_field_for_field_to_handoff_evidence() {
    let snapshot = BinanceDepthSnapshotHourlyRow {
        timestamp_received: 1_767_225_600_020,
        snapshot_fetch_start_utc: 1_767_225_600_010,
        last_update_id: 100,
        bids: r#"[["1","2"]]"#.to_owned(),
        asks: r#"[["3","4"]]"#.to_owned(),
        symbol: "BTCUSDT".to_owned(),
        source_endpoint: "https://api.binance.com/api/v3/depth".to_owned(),
        snapshot_status: "complete".to_owned(),
        source_sequence_epoch_id: Some("depth-source-epoch-row".to_owned()),
        connection_generation: Some(7),
        depth_ingress_sequence: Some(12),
        collector_admission_sequence: Some(20),
        collector_admission_time_utc_ms: Some(1_767_225_600_021),
        collector_host_id: "collector-host".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        schema_version: BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION.to_owned(),
    };
    let update = BinanceDepthUpdateHourlyRow {
        timestamp_received: 1_767_225_600_019,
        event_type: "depthUpdate".to_owned(),
        event_time: 1_767_225_600_018,
        symbol: "BTCUSDT".to_owned(),
        first_update_id: 101,
        final_update_id: 101,
        bids: r#"[["1","2"]]"#.to_owned(),
        asks: r#"[["3","4"]]"#.to_owned(),
        stream: "btcusdt@depth".to_owned(),
        snapshot_last_update_id: Some(100),
        local_u_before: None,
        stitch_verdict: BINANCE_DEPTH_VERDICT_APPLIED.to_owned(),
        stitch_evidence: Some("U <= lastUpdateId + 1 <= u".to_owned()),
        stitch_gap_reason: None,
        source_sequence_epoch_id: Some("depth-source-epoch-row".to_owned()),
        connection_generation: Some(7),
        depth_ingress_sequence: Some(11),
        collector_admission_sequence: Some(21),
        collector_admission_time_utc_ms: Some(1_767_225_600_023),
        collector_host_id: "collector-host".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://stream.binance.com/ws/btcusdt@depth".to_owned(),
        schema_version: BINANCE_DEPTH_UPDATE_SCHEMA_VERSION.to_owned(),
    };

    assert!(snapshot.validate().is_complete());
    assert!(update.validate().is_complete());
    let snapshot_evidence = DepthHandoffSourceEvidence::try_from(&snapshot).unwrap();
    let update_evidence = DepthHandoffSourceEvidence::try_from(&update).unwrap();

    assert_eq!(
        snapshot_evidence.source_sequence_epoch_id,
        snapshot.source_sequence_epoch_id.unwrap()
    );
    assert_eq!(
        snapshot_evidence.connection_generation,
        snapshot.connection_generation.unwrap()
    );
    assert_eq!(
        snapshot_evidence.depth_ingress_sequence,
        snapshot.depth_ingress_sequence.unwrap()
    );
    assert_eq!(
        snapshot_evidence.collector_admission_sequence,
        snapshot.collector_admission_sequence.unwrap()
    );
    assert_eq!(
        snapshot_evidence.collector_admission_time_utc_ms,
        snapshot.collector_admission_time_utc_ms.unwrap()
    );
    assert_eq!(
        update_evidence.source_sequence_epoch_id,
        update.source_sequence_epoch_id.unwrap()
    );
    assert_eq!(
        update_evidence.connection_generation,
        update.connection_generation.unwrap()
    );
    assert_eq!(
        update_evidence.depth_ingress_sequence,
        update.depth_ingress_sequence.unwrap()
    );
    assert_eq!(
        update_evidence.collector_admission_sequence,
        update.collector_admission_sequence.unwrap()
    );
    assert_eq!(
        update_evidence.collector_admission_time_utc_ms,
        update.collector_admission_time_utc_ms.unwrap()
    );
    assert_eq!(
        update_evidence.stitch_verdict.as_deref(),
        Some(BINANCE_DEPTH_VERDICT_APPLIED)
    );
}

#[test]
fn legacy_generic_depth_external_frame_remains_accepted_without_new_evidence() {
    let frame = HandoffFrame::external_market_event(
        44,
        "binance_depth_update",
        "depth_update",
        "BTCUSDT",
        Some("1767225600018"),
        1_767_225_600_019,
        1_767_225_600_020,
        "collector-host",
        "eu-west-1",
        r#"{"e":"depthUpdate","U":101,"u":101}"#,
    )
    .unwrap();
    assert!(frame.depth_source_evidence.is_none());
}

#[test]
fn handoff_rejects_new_rows_with_legacy_rest_compatibility_values() {
    let mut row = websocket_trade_row();
    row.rest_response_ingress_sequence = Some(99);

    assert!(HandoffSourceEvidence::try_from(&row).is_err());
}

#[test]
fn source_evidence_rejects_invalid_coordinate_shapes_and_zero_sequences() {
    assert!(
        HandoffSourceEvidence::invalid_input(
            "aggtrade-source-epoch-9",
            0,
            1,
            1,
            1_767_225_600_010,
            "missing_payload",
        )
        .is_err()
    );
    assert!(
        HandoffSourceEvidence::trade("aggtrade-source-epoch-9", 1, 0, 2, 1_767_225_600_011,)
            .is_err()
    );
    assert!(
        HandoffSourceEvidence::trade("aggtrade-source-epoch-9", 1, 1, 0, 1_767_225_600_017,)
            .is_err()
    );
    for reason in ["", "contains space", "unicode_错误", "contains$dollar"] {
        assert!(
            HandoffSourceEvidence::invalid_input(
                "aggtrade-source-epoch-9",
                1,
                1,
                8,
                1_767_225_600_018,
                reason,
            )
            .is_err(),
            "reason code {reason:?} was accepted"
        );
    }
}

#[test]
fn trade_frame_requires_raw_json_but_invalid_frame_may_omit_it() {
    let trade = HandoffSourceEvidence::try_from(&websocket_trade_row()).unwrap();
    assert!(
        HandoffFrame::source_evidence(
            4_003,
            "binance_aggtrade",
            "trade",
            "BTCUSDT",
            None,
            1_767_225_600_010,
            1_767_225_600_012,
            "collector-host",
            "eu-west-1",
            None,
            trade,
        )
        .is_err()
    );

    let invalid = HandoffSourceEvidence::try_from(&websocket_invalid_row()).unwrap();
    assert!(
        HandoffFrame::source_evidence(
            4_004,
            "binance_aggtrade",
            "invalid_input",
            "BTCUSDT",
            None,
            1_767_225_600_020,
            1_767_225_600_022,
            "collector-host",
            "eu-west-1",
            None,
            invalid,
        )
        .is_ok()
    );
}

#[test]
fn deserialized_source_evidence_frames_reject_cross_contract_mismatches() {
    let evidence = HandoffSourceEvidence::try_from(&websocket_trade_row()).unwrap();
    let valid = HandoffFrame::source_evidence(
        4_005,
        "binance_aggtrade",
        "aggTrade",
        "BTCUSDT",
        None,
        1_767_225_600_010,
        1_767_225_600_012,
        "collector-host",
        "eu-west-1",
        Some(r#"{"e":"aggTrade","a":123}"#),
        evidence,
    )
    .unwrap();
    let valid_json = serde_json::to_value(valid).unwrap();

    for (field, replacement) in [
        ("source_kind", serde_json::json!("binance_depth")),
        ("source_event_type", serde_json::json!("invalid_input")),
        ("raw_json", serde_json::Value::Null),
    ] {
        let mut mismatched = valid_json.clone();
        mismatched[field] = replacement;
        let restored: HandoffFrame = serde_json::from_value(mismatched).unwrap();
        assert!(restored.validate().is_err(), "field {field} was accepted");
    }

    let mut zero_sequence = valid_json;
    zero_sequence["source_evidence"]["websocket_ingress_sequence"] = serde_json::json!(0);
    let restored: HandoffFrame = serde_json::from_value(zero_sequence).unwrap();
    assert!(restored.validate().is_err());

    let evidence = HandoffSourceEvidence::try_from(&websocket_trade_row()).unwrap();
    let valid = HandoffFrame::source_evidence(
        4_006,
        "binance_aggtrade",
        "aggTrade",
        "BTCUSDT",
        None,
        1_767_225_600_010,
        1_767_225_600_012,
        "collector-host",
        "eu-west-1",
        Some(r#"{"e":"aggTrade","a":123}"#),
        evidence,
    )
    .unwrap();
    let mut zero_generation = serde_json::to_value(valid).unwrap();
    zero_generation["source_evidence"]["connection_generation"] = serde_json::json!(0);
    let restored: HandoffFrame = serde_json::from_value(zero_generation).unwrap();
    assert!(restored.validate().is_err());
}

#[tokio::test]
async fn fake_consumer_records_runtime_receive_time_on_local_stream() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();
    let config =
        HandoffRuntimeConfig::for_environment(&dataset_root, PaperLiveEnvironment::Paper).unwrap();
    let frame = HandoffFrame::status(
        1,
        "source_status",
        "polymarket_market",
        1_767_225_600_001,
        "collector-host",
        "eu-west-1",
    )
    .unwrap();

    let received = run_single_frame_unix_handoff(&config, frame).await.unwrap();

    assert_eq!(received.frame.frame_type, HandoffFrameType::SourceStatus);
    assert!(received.runtime_receive_time_utc_ms >= received.frame.handoff_send_time_utc_ms);
    assert_eq!(
        RuntimeReceivedFrame::runtime_timestamp_field_name(),
        "runtime_receive_time_utc_ms"
    );
}

#[tokio::test]
async fn data_collect_server_can_send_status_frame_to_fake_consumer() {
    let dataset_root = tempdir().unwrap();
    let dataset_root = dataset_root.path().canonicalize().unwrap();
    let config =
        HandoffRuntimeConfig::for_environment(&dataset_root, PaperLiveEnvironment::Live).unwrap();
    let frame = HandoffFrame::status(
        7,
        "heartbeat",
        "data_collect_hot_handoff",
        1_767_225_600_001,
        "collector-host",
        "eu-west-1",
    )
    .unwrap();

    let received = run_single_frame_unix_server_handoff(&config, frame)
        .await
        .unwrap();

    assert_eq!(received.frame.frame_type, HandoffFrameType::SourceStatus);
    assert_eq!(received.frame.event_seq, 7);
    assert!(received.frame.source_evidence.is_none());
    assert!(received.runtime_receive_time_utc_ms >= received.frame.handoff_send_time_utc_ms);
}
