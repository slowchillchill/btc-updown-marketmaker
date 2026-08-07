use base64::Engine as _;
use contracts::{
    CanonicalEventRecordV1, CanonicalReferenceEventPayloadV1, CanonicalSourceKind,
    MarketDataQuality, ResyncQuality, Sha256Hex,
};
use data_pipeline_core::reduction::{
    BinanceAggTradeReducer, BinanceDepthReducer, ReductionDispositionV1, ReductionOperationKindV1,
    ReferenceReductionOutcomeV1, decode_reduction_report_entry_v1,
    encode_reduction_report_entry_v1, validate_reduction_report_entry_v1,
};
use data_pipeline_core::typed_raw::{
    BinanceAggTradeCaptureStatus, BinanceAggTradeInvalidReason, BinanceAggTradePayloadEncoding,
    BinanceAggTradeRecord, BinanceAggTradeRow, BinanceDepthSnapshotRow, BinanceDepthStitchVerdict,
    BinanceDepthUpdateRow, DepthLevel,
};
use data_pipeline_core::{
    project_record_event_time_index_entry, project_record_visibility_index_entry,
};
use sha2::{Digest, Sha256};

const RULE_VERSION: &str = "canonical-rule-v3";

fn sha(character: char) -> Sha256Hex {
    Sha256Hex(character.to_string().repeat(64))
}

fn level(price: &str, quantity: &str) -> DepthLevel {
    DepthLevel {
        price: price.to_owned(),
        quantity: quantity.to_owned(),
    }
}

fn snapshot(epoch: &str, last_update_id: u64, admission_sequence: u64) -> BinanceDepthSnapshotRow {
    BinanceDepthSnapshotRow {
        timestamp_received: 2_000 + admission_sequence as i64,
        snapshot_fetch_start_utc: 900,
        last_update_id,
        bids: vec![level("64000", "0.5"), level("64100", "0.7")],
        asks: vec![level("64300", "0.3"), level("64200", "0.4")],
        symbol: "BTCUSDT".to_owned(),
        source_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
            .to_owned(),
        snapshot_status: "complete".to_owned(),
        source_sequence_epoch_id: epoch.to_owned(),
        connection_generation: 1,
        depth_ingress_sequence: admission_sequence,
        collector_admission_sequence: admission_sequence,
        collector_admission_time_utc_ms: 2_100 + admission_sequence as i64,
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        schema_version: "binance_depth_snapshot_v3".to_owned(),
    }
}

#[allow(clippy::too_many_arguments)]
fn update(
    epoch: &str,
    first_update_id: u64,
    final_update_id: u64,
    admission_sequence: u64,
    snapshot_last_update_id: Option<u64>,
    local_u_before: Option<u64>,
    verdict: BinanceDepthStitchVerdict,
    bids: Vec<DepthLevel>,
) -> BinanceDepthUpdateRow {
    let (stitch_evidence, stitch_gap_reason) = match verdict {
        BinanceDepthStitchVerdict::Applied => (Some("continuous_depth_update".to_owned()), None),
        BinanceDepthStitchVerdict::NotAppliedStaleDuplicate => {
            (Some("u <= local_u".to_owned()), None)
        }
        BinanceDepthStitchVerdict::NotAppliedConfirmedHole => (
            Some("U > local_u + 1".to_owned()),
            Some("U > local_u + 1".to_owned()),
        ),
        BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable => (None, None),
    };
    BinanceDepthUpdateRow {
        timestamp_received: 2_000 + admission_sequence as i64,
        event_type: "depthUpdate".to_owned(),
        event_time: 1_500 + admission_sequence as i64,
        symbol: "BTCUSDT".to_owned(),
        first_update_id,
        final_update_id,
        bids,
        asks: Vec::new(),
        stream: "btcusdt@depth".to_owned(),
        snapshot_last_update_id,
        local_u_before,
        stitch_verdict: verdict,
        stitch_evidence,
        stitch_gap_reason,
        source_sequence_epoch_id: epoch.to_owned(),
        connection_generation: 1,
        depth_ingress_sequence: admission_sequence,
        collector_admission_sequence: admission_sequence,
        collector_admission_time_utc_ms: 2_100 + admission_sequence as i64,
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@depth".to_owned(),
        schema_version: "binance_depth_update_v4".to_owned(),
    }
}

fn trade(aggregate_trade_id: u64, admission_sequence: u64, price: &str) -> BinanceAggTradeRow {
    BinanceAggTradeRow {
        timestamp_received: 3_000 + admission_sequence as i64,
        rest_backfill_timestamp_received: None,
        ingest_source: "websocket".to_owned(),
        rest_was_best_price_match: None,
        source_native_payload_json: format!(
            r#"{{"e":"aggTrade","E":2800,"T":2700,"s":"BTCUSDT","a":{aggregate_trade_id},"p":"{price}"}}"#
        ),
        source_sequence_epoch_id: "aggtrade-epoch-1".to_owned(),
        connection_generation: 1,
        websocket_ingress_sequence: admission_sequence,
        rest_response_ingress_sequence: None,
        rest_response_item_index: None,
        collector_admission_sequence: admission_sequence,
        collector_admission_time_utc_ms: 3_100 + admission_sequence as i64,
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@aggTrade".to_owned(),
        schema_version: "binance_aggtrade_v7".to_owned(),
        record: BinanceAggTradeRecord::Trade {
            event_type: "aggTrade".to_owned(),
            event_time: 2_800,
            trade_time: 2_700,
            symbol: "BTCUSDT".to_owned(),
            aggregate_trade_id,
            price: price.to_owned(),
            quantity: "0.01".to_owned(),
            first_trade_id: aggregate_trade_id * 10,
            last_trade_id: aggregate_trade_id * 10 + 1,
            buyer_is_maker: false,
            stream: "btcusdt@aggTrade".to_owned(),
            ws_ignore: false,
        },
    }
}

fn invalid_trade(admission_sequence: u64) -> BinanceAggTradeRow {
    let timestamp_received = 3_000 + admission_sequence as i64;
    let source_endpoint = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
    let invalid_payload = br#"{"e":"aggTrade""#;
    let payload_length_bytes = u64::try_from(invalid_payload.len()).unwrap();
    let payload_sha256 = format!("{:x}", Sha256::digest(invalid_payload));
    BinanceAggTradeRow {
        timestamp_received,
        rest_backfill_timestamp_received: None,
        ingest_source: "websocket".to_owned(),
        rest_was_best_price_match: None,
        source_native_payload_json: serde_json::json!({
            "capture_limit_bytes": 524_288,
            "declared_content_length": null,
            "endpoint": source_endpoint,
            "evidence_schema": "binance_aggtrade_invalid_evidence_v1",
            "invalid_reason_code": "invalid_json",
            "observed_bytes_at_least": payload_length_bytes,
            "payload_base64": base64::engine::general_purpose::STANDARD.encode(invalid_payload),
            "payload_capture_status": "complete",
            "payload_encoding": "base64",
            "payload_length_bytes": payload_length_bytes,
            "payload_sha256": payload_sha256,
            "receive_time_utc_ms": timestamp_received,
            "transport": "websocket",
        })
        .to_string(),
        source_sequence_epoch_id: "aggtrade-epoch-1".to_owned(),
        connection_generation: 1,
        websocket_ingress_sequence: admission_sequence,
        rest_response_ingress_sequence: None,
        rest_response_item_index: None,
        collector_admission_sequence: admission_sequence,
        collector_admission_time_utc_ms: 3_100 + admission_sequence as i64,
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: source_endpoint.to_owned(),
        schema_version: "binance_aggtrade_v7".to_owned(),
        record: BinanceAggTradeRecord::InvalidInput {
            invalid_reason_code: BinanceAggTradeInvalidReason::InvalidJson,
            invalid_reason_detail: Some("representative invalid frame".to_owned()),
            payload_encoding: Some(BinanceAggTradePayloadEncoding::Base64),
            payload_length_bytes: Some(payload_length_bytes),
            payload_sha256: Some(payload_sha256),
            payload_capture_status: BinanceAggTradeCaptureStatus::Complete,
        },
    }
}

#[test]
fn invalid_trade_rejects_an_evidence_envelope_digest_that_disagrees_with_the_row() {
    let mut row = invalid_trade(1);
    let mut envelope: serde_json::Value =
        serde_json::from_str(&row.source_native_payload_json).unwrap();
    envelope["payload_sha256"] = serde_json::Value::String("0".repeat(64));
    row.source_native_payload_json = envelope.to_string();

    assert!(row.validate().is_err());
}

#[test]
fn non_snapshot_unavailable_depth_row_requires_structured_stitch_evidence() {
    let mut row = update(
        "depth-epoch-1",
        101,
        101,
        2,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![level("64100", "0.8")],
    );
    row.stitch_evidence = None;

    assert!(row.validate().is_err());
}

#[test]
fn depth_row_rejects_duplicate_prices_before_state_reduction() {
    let mut row = update(
        "depth-epoch-1",
        101,
        101,
        2,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![level("64100", "0.8"), level("64100.0", "0.9")],
    );
    row.asks = vec![level("64200", "0.4"), level("64200.00", "0.3")];

    assert!(row.validate().is_err());
}

fn produced_event(outcome: &ReferenceReductionOutcomeV1) -> &contracts::CanonicalReferenceEventV1 {
    let ReductionDispositionV1::Produced { result_event_id } = &outcome.report.disposition else {
        panic!("expected a produced reduction result");
    };
    let event = outcome.event.as_ref().expect("produced result needs event");
    assert_eq!(result_event_id, &event.event_id);
    event
}

fn assert_no_output_with_evidence(outcome: &ReferenceReductionOutcomeV1) {
    assert!(outcome.event.is_none());
    let ReductionDispositionV1::NoOutput { operation_evidence } = &outcome.report.disposition
    else {
        panic!("expected NO_OUTPUT");
    };
    assert!(!operation_evidence.is_empty());
}

fn assert_suppressed_with_evidence(outcome: &ReferenceReductionOutcomeV1) {
    assert!(outcome.event.is_none());
    let ReductionDispositionV1::Suppressed { operation_evidence } = &outcome.report.disposition
    else {
        panic!("expected SUPPRESSED");
    };
    assert!(!operation_evidence.is_empty());
}

#[test]
fn snapshot_emits_sorted_full_state_and_only_a_visibility_index_entry() {
    let mut reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    let outcome = reducer
        .apply_snapshot(&snapshot("depth-epoch-1", 100, 1), &sha('a'), &sha('b'))
        .unwrap();
    let event = produced_event(&outcome);

    let CanonicalReferenceEventPayloadV1::BinanceDepthState(state) = &event.payload else {
        panic!("expected Binance depth state");
    };
    assert_eq!(state.last_update_id, 100);
    assert_eq!(state.bids[0].price_usdt_per_btc.0, "64100");
    assert_eq!(state.bids[1].price_usdt_per_btc.0, "64000");
    assert_eq!(state.asks[0].price_usdt_per_btc.0, "64200");
    assert_eq!(state.asks[1].price_usdt_per_btc.0, "64300");
    assert_eq!(event.time.source_event_at_utc_ms, None);
    assert_eq!(event.lineage.source_evidence.len(), 1);
    assert_eq!(
        event.lineage.source_evidence[0].source_kind,
        CanonicalSourceKind::BinanceDepthSnapshot
    );

    let record = CanonicalEventRecordV1::ReferenceEvent(Box::new(event.clone()));
    assert_eq!(
        project_record_event_time_index_entry(&record).unwrap(),
        None
    );
    assert_eq!(
        project_record_visibility_index_entry(&record)
            .unwrap()
            .event_id,
        event.event_id
    );
}

#[test]
fn applied_update_uses_absolute_quantities_deletes_zero_and_advances_anchor_once() {
    let mut reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    reducer
        .apply_snapshot(&snapshot("depth-epoch-1", 100, 1), &sha('a'), &sha('b'))
        .unwrap();
    let applied = update(
        "depth-epoch-1",
        101,
        102,
        2,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![level("64100", "0"), level("64000", "0.8")],
    );
    let outcome = reducer
        .apply_update(&applied, &sha('a'), &sha('c'))
        .unwrap();
    let event = produced_event(&outcome);
    let CanonicalReferenceEventPayloadV1::BinanceDepthState(state) = &event.payload else {
        panic!("expected Binance depth state");
    };

    assert_eq!(state.last_update_id, 102);
    assert_eq!(state.bids.len(), 1);
    assert_eq!(state.bids[0].price_usdt_per_btc.0, "64000");
    assert_eq!(state.bids[0].quantity_btc.0, "0.8");
    assert_eq!(event.time.source_event_at_utc_ms, Some(applied.event_time));
    assert!(
        event
            .lineage
            .source_evidence
            .iter()
            .any(|source| source.source_kind == CanonicalSourceKind::BinanceDepthSnapshot)
    );
    assert!(
        event
            .lineage
            .source_evidence
            .iter()
            .any(|source| source.source_kind == CanonicalSourceKind::BinanceDepthUpdate)
    );
}

#[test]
fn full_depth_state_keeps_overwritten_and_deleted_level_evidence() {
    let mut reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    reducer
        .apply_snapshot(&snapshot("depth-epoch-1", 100, 1), &sha('a'), &sha('b'))
        .unwrap();

    reducer
        .apply_update(
            &update(
                "depth-epoch-1",
                101,
                101,
                2,
                Some(100),
                None,
                BinanceDepthStitchVerdict::Applied,
                vec![level("64000", "0.8")],
            ),
            &sha('a'),
            &sha('c'),
        )
        .unwrap();
    let overwritten = reducer
        .apply_update(
            &update(
                "depth-epoch-1",
                102,
                102,
                3,
                Some(100),
                Some(101),
                BinanceDepthStitchVerdict::Applied,
                vec![level("64000", "0.6")],
            ),
            &sha('a'),
            &sha('d'),
        )
        .unwrap();
    let overwritten_event = produced_event(&overwritten);
    for expected_sequence in [2, 3] {
        assert!(
            overwritten_event
                .lineage
                .source_evidence
                .iter()
                .any(|evidence| {
                    evidence
                        .collector_admission
                        .as_ref()
                        .is_some_and(|coordinate| {
                            coordinate.collector_admission_sequence == expected_sequence
                        })
                })
        );
    }

    reducer
        .apply_update(
            &update(
                "depth-epoch-1",
                103,
                103,
                4,
                Some(100),
                Some(102),
                BinanceDepthStitchVerdict::Applied,
                vec![level("64100", "0")],
            ),
            &sha('a'),
            &sha('e'),
        )
        .unwrap();
    let after_unrelated_update = reducer
        .apply_update(
            &update(
                "depth-epoch-1",
                104,
                104,
                5,
                Some(100),
                Some(103),
                BinanceDepthStitchVerdict::Applied,
                vec![level("64000", "0.7")],
            ),
            &sha('a'),
            &sha('f'),
        )
        .unwrap();
    let event = produced_event(&after_unrelated_update);
    assert!(event.lineage.source_evidence.iter().any(|evidence| {
        evidence
            .collector_admission
            .as_ref()
            .is_some_and(|coordinate| coordinate.collector_admission_sequence == 4)
    }));
}

#[test]
fn an_applied_pending_update_cannot_become_visible_before_its_snapshot() {
    let mut reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    let mut late_snapshot = snapshot("depth-epoch-1", 100, 1);
    late_snapshot.timestamp_received = 5_000;
    late_snapshot.collector_admission_time_utc_ms = 5_100;
    reducer
        .apply_snapshot(&late_snapshot, &sha('a'), &sha('b'))
        .unwrap();

    let mut pending_update = update(
        "depth-epoch-1",
        101,
        101,
        2,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![
            level("64000", "0"),
            level("64100", "0"),
            level("65000", "1"),
        ],
    );
    pending_update.timestamp_received = 4_000;
    pending_update.collector_admission_time_utc_ms = 5_200;
    pending_update.asks = vec![
        level("64200", "0"),
        level("64300", "0"),
        level("65100", "1"),
    ];

    let outcome = reducer
        .apply_update(&pending_update, &sha('a'), &sha('c'))
        .unwrap();
    let event = produced_event(&outcome);
    assert_eq!(event.time.received_at_utc_ms, 5_000);
    assert!(
        event
            .lineage
            .source_evidence
            .iter()
            .any(|evidence| { evidence.source_kind == CanonicalSourceKind::BinanceDepthSnapshot })
    );
}

#[test]
fn only_applied_stitch_verdict_emits_a_new_depth_state() {
    let mut applied_reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    applied_reducer
        .apply_snapshot(&snapshot("applied-epoch", 100, 1), &sha('a'), &sha('b'))
        .unwrap();
    let applied = applied_reducer
        .apply_update(
            &update(
                "applied-epoch",
                101,
                101,
                2,
                Some(100),
                None,
                BinanceDepthStitchVerdict::Applied,
                vec![level("64100", "0.8")],
            ),
            &sha('a'),
            &sha('c'),
        )
        .unwrap();
    produced_event(&applied);

    let mut stale_reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    stale_reducer
        .apply_snapshot(&snapshot("stale-epoch", 100, 1), &sha('a'), &sha('b'))
        .unwrap();
    let stale = stale_reducer
        .apply_update(
            &update(
                "stale-epoch",
                90,
                100,
                2,
                Some(100),
                None,
                BinanceDepthStitchVerdict::NotAppliedStaleDuplicate,
                vec![level("64100", "9")],
            ),
            &sha('a'),
            &sha('c'),
        )
        .unwrap();
    assert_suppressed_with_evidence(&stale);

    let mut hole_reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    hole_reducer
        .apply_snapshot(&snapshot("hole-epoch", 100, 1), &sha('a'), &sha('b'))
        .unwrap();
    let hole = hole_reducer
        .apply_update(
            &update(
                "hole-epoch",
                102,
                102,
                2,
                Some(100),
                None,
                BinanceDepthStitchVerdict::NotAppliedConfirmedHole,
                vec![level("64100", "9")],
            ),
            &sha('a'),
            &sha('c'),
        )
        .unwrap();
    let event = produced_event(&hole);
    assert_eq!(event.quality.availability, MarketDataQuality::ConfirmedGap);
    assert_eq!(event.quality.resync, ResyncQuality::Required);

    let mut unavailable_reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    let unavailable = unavailable_reducer
        .apply_update(
            &update(
                "unavailable-epoch",
                101,
                101,
                1,
                None,
                None,
                BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable,
                vec![level("64100", "9")],
            ),
            &sha('a'),
            &sha('c'),
        )
        .unwrap();
    assert_no_output_with_evidence(&unavailable);
}

#[test]
fn confirmed_hole_preserves_the_last_trusted_book_until_a_full_snapshot_resyncs() {
    let mut reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    reducer
        .apply_snapshot(&snapshot("depth-epoch-1", 100, 1), &sha('a'), &sha('b'))
        .unwrap();

    let hole = reducer
        .apply_update(
            &update(
                "depth-epoch-1",
                102,
                102,
                2,
                Some(100),
                None,
                BinanceDepthStitchVerdict::NotAppliedConfirmedHole,
                vec![level("64100", "9")],
            ),
            &sha('a'),
            &sha('c'),
        )
        .unwrap();
    let event = produced_event(&hole);
    assert_eq!(hole.report.operation, ReductionOperationKindV1::Reject);
    assert_eq!(hole.report.reason_code, "BINANCE_DEPTH_CONFIRMED_HOLE");
    assert_eq!(event.quality.availability, MarketDataQuality::ConfirmedGap);
    assert_eq!(event.quality.resync, ResyncQuality::Required);
    let CanonicalReferenceEventPayloadV1::BinanceDepthState(state) = &event.payload else {
        panic!("expected Binance depth state");
    };
    assert_eq!(state.last_update_id, 100);
    assert_eq!(state.bids[0].quantity_btc.0, "0.7");
    assert!(event.lineage.source_evidence.iter().any(|source| {
        source
            .collector_admission
            .as_ref()
            .is_some_and(|coordinate| coordinate.collector_admission_sequence == 2)
    }));

    let still_unavailable = reducer
        .apply_update(
            &update(
                "depth-epoch-1",
                103,
                103,
                3,
                None,
                None,
                BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable,
                vec![level("64100", "8")],
            ),
            &sha('a'),
            &sha('d'),
        )
        .unwrap();
    assert_no_output_with_evidence(&still_unavailable);

    let recovery = reducer
        .apply_snapshot(&snapshot("depth-epoch-1", 200, 4), &sha('a'), &sha('e'))
        .unwrap();
    let recovery_event = produced_event(&recovery);
    assert_eq!(
        recovery_event.quality.availability,
        MarketDataQuality::Fresh
    );
    assert_eq!(recovery_event.quality.resync, ResyncQuality::ResyncBoundary);
}

#[test]
fn snapshot_unavailable_update_is_never_backfilled_after_snapshot_arrives() {
    let mut reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    let unavailable = reducer
        .apply_update(
            &update(
                "depth-epoch-1",
                101,
                101,
                1,
                None,
                None,
                BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable,
                vec![level("64100", "9")],
            ),
            &sha('a'),
            &sha('c'),
        )
        .unwrap();
    assert_no_output_with_evidence(&unavailable);

    let snapshot_outcome = reducer
        .apply_snapshot(&snapshot("depth-epoch-1", 100, 2), &sha('a'), &sha('b'))
        .unwrap();
    let event = produced_event(&snapshot_outcome);
    let CanonicalReferenceEventPayloadV1::BinanceDepthState(state) = &event.payload else {
        panic!("expected Binance depth state");
    };
    assert_eq!(state.last_update_id, 100);
    assert_eq!(state.bids[0].quantity_btc.0, "0.7");
    assert!(
        state
            .bids
            .iter()
            .all(|price_level| price_level.quantity_btc.0 != "9")
    );
}

#[test]
fn a_new_epoch_cannot_inherit_the_previous_epoch_book() {
    let mut reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    reducer
        .apply_snapshot(&snapshot("depth-epoch-1", 100, 1), &sha('a'), &sha('b'))
        .unwrap();

    let forged_applied_without_epoch_snapshot = update(
        "depth-epoch-2",
        101,
        101,
        1,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![level("65000", "2")],
    );
    assert!(
        reducer
            .apply_update(&forged_applied_without_epoch_snapshot, &sha('a'), &sha('c'))
            .is_err()
    );

    let mut epoch_two_snapshot = snapshot("depth-epoch-2", 200, 2);
    epoch_two_snapshot.bids = vec![level("65000", "2")];
    epoch_two_snapshot.asks = vec![level("65100", "3")];
    let outcome = reducer
        .apply_snapshot(&epoch_two_snapshot, &sha('a'), &sha('d'))
        .unwrap();
    let event = produced_event(&outcome);
    let CanonicalReferenceEventPayloadV1::BinanceDepthState(state) = &event.payload else {
        panic!("expected Binance depth state");
    };
    assert_eq!(state.last_update_id, 200);
    assert_eq!(state.bids.len(), 1);
    assert_eq!(state.bids[0].price_usdt_per_btc.0, "65000");
    assert!(event.lineage.source_evidence.iter().all(|source| {
        source
            .collector_admission
            .as_ref()
            .is_some_and(|coordinate| coordinate.source_sequence_epoch_id == "depth-epoch-2")
    }));
}

#[test]
fn a_new_epoch_marks_the_old_book_missing_until_its_own_snapshot_resyncs() {
    let mut reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    reducer
        .apply_snapshot(&snapshot("depth-epoch-1", 100, 1), &sha('a'), &sha('b'))
        .unwrap();

    let unavailable = reducer
        .apply_update(
            &update(
                "depth-epoch-2",
                201,
                201,
                1,
                None,
                None,
                BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable,
                vec![level("65000", "2")],
            ),
            &sha('a'),
            &sha('c'),
        )
        .unwrap();
    let event = produced_event(&unavailable);
    assert_eq!(
        unavailable.report.operation,
        ReductionOperationKindV1::Reject
    );
    assert_eq!(
        unavailable.report.reason_code,
        "BINANCE_DEPTH_EPOCH_SNAPSHOT_REQUIRED"
    );
    assert_eq!(event.quality.availability, MarketDataQuality::Missing);
    assert_eq!(event.quality.resync, ResyncQuality::Required);
    let CanonicalReferenceEventPayloadV1::BinanceDepthState(state) = &event.payload else {
        panic!("expected Binance depth state");
    };
    assert_eq!(state.last_update_id, 100);
    assert!(event.lineage.source_evidence.iter().any(|source| {
        source
            .collector_admission
            .as_ref()
            .is_some_and(|coordinate| {
                coordinate.source_sequence_epoch_id == "depth-epoch-2"
                    && coordinate.collector_admission_sequence == 1
            })
    }));

    let repeated_unavailable = reducer
        .apply_update(
            &update(
                "depth-epoch-2",
                202,
                202,
                2,
                None,
                None,
                BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable,
                vec![level("65000", "3")],
            ),
            &sha('a'),
            &sha('d'),
        )
        .unwrap();
    assert_no_output_with_evidence(&repeated_unavailable);

    let mut epoch_two_snapshot = snapshot("depth-epoch-2", 200, 3);
    epoch_two_snapshot.bids = vec![level("65000", "2")];
    epoch_two_snapshot.asks = vec![level("65100", "3")];
    let recovery = reducer
        .apply_snapshot(&epoch_two_snapshot, &sha('a'), &sha('e'))
        .unwrap();
    let recovery_event = produced_event(&recovery);
    assert_eq!(
        recovery_event.quality.availability,
        MarketDataQuality::Fresh
    );
    assert_eq!(recovery_event.quality.resync, ResyncQuality::ResyncBoundary);
}

#[test]
fn a_completed_depth_epoch_cannot_resume_after_a_new_epoch_begins() {
    let mut reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    reducer
        .apply_snapshot(&snapshot("depth-epoch-1", 100, 1), &sha('a'), &sha('b'))
        .unwrap();
    reducer
        .apply_snapshot(&snapshot("depth-epoch-2", 200, 1), &sha('a'), &sha('d'))
        .unwrap();

    let resumed_old_epoch = update(
        "depth-epoch-1",
        101,
        101,
        2,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![level("64100", "0.8")],
    );
    assert!(
        reducer
            .apply_update(&resumed_old_epoch, &sha('a'), &sha('c'))
            .is_err()
    );
}

#[test]
fn aggtrade_valid_invalid_duplicate_and_conflict_paths_are_distinct() {
    let mut reducer = BinanceAggTradeReducer::new(RULE_VERSION).unwrap();
    let valid_row = trade(21, 1, "64123.4");
    let valid = reducer.apply(&valid_row, &sha('a'), &sha('e')).unwrap();
    let event = produced_event(&valid);
    assert_eq!(event.time.source_event_at_utc_ms, Some(2_700));
    let CanonicalReferenceEventPayloadV1::BinanceAggregateTrade(payload) = &event.payload else {
        panic!("expected aggregate trade");
    };
    assert_eq!(payload.aggregate_trade_id, 21);
    assert_eq!(payload.event_time_utc_ms, 2_800);

    let invalid = reducer
        .apply(&invalid_trade(2), &sha('a'), &sha('f'))
        .unwrap();
    assert_no_output_with_evidence(&invalid);
    let ReductionDispositionV1::NoOutput { operation_evidence } = &invalid.report.disposition
    else {
        unreachable!();
    };
    assert_eq!(invalid.report.reason_code, "BINANCE_AGGTRADE_INVALID_INPUT");
    assert_eq!(operation_evidence.len(), 1);
    let invalid_evidence = &operation_evidence[0].source_evidence;
    assert_eq!(invalid_evidence.source_event_at_utc_ms, None);
    assert_eq!(
        invalid_evidence.source_kind,
        CanonicalSourceKind::BinanceAggtrade
    );
    assert_eq!(invalid_evidence.received_at_utc_ms, 3_002);
    assert_eq!(invalid_evidence.input_manifest_sha256, sha('a'));
    assert_eq!(invalid_evidence.input_file_sha256, sha('f'));
    let admission = invalid_evidence
        .collector_admission
        .as_ref()
        .expect("invalid aggregate-trade evidence keeps its admission coordinate");
    assert_eq!(admission.source_sequence_epoch_id, "aggtrade-epoch-1");
    assert_eq!(admission.collector_admission_sequence, 2);
    assert_eq!(admission.admitted_at_utc_ms, 3_102);

    let duplicate = reducer
        .apply(&trade(21, 3, "64123.4"), &sha('a'), &sha('e'))
        .unwrap();
    assert_suppressed_with_evidence(&duplicate);

    assert!(
        reducer
            .apply(&trade(21, 4, "64123.5"), &sha('a'), &sha('e'))
            .is_err()
    );
}

#[test]
fn produced_reports_reference_the_event_while_non_events_own_their_evidence() {
    let mut depth_reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    let produced = depth_reducer
        .apply_snapshot(&snapshot("depth-epoch-1", 100, 1), &sha('a'), &sha('b'))
        .unwrap();
    let event = produced_event(&produced);
    let ReductionDispositionV1::Produced { result_event_id } = &produced.report.disposition else {
        unreachable!();
    };
    assert_eq!(result_event_id, &event.event_id);

    let suppressed = depth_reducer
        .apply_update(
            &update(
                "depth-epoch-1",
                90,
                100,
                2,
                Some(100),
                None,
                BinanceDepthStitchVerdict::NotAppliedStaleDuplicate,
                vec![level("64100", "9")],
            ),
            &sha('a'),
            &sha('c'),
        )
        .unwrap();
    assert_suppressed_with_evidence(&suppressed);

    let mut trade_reducer = BinanceAggTradeReducer::new(RULE_VERSION).unwrap();
    let no_output = trade_reducer
        .apply(&invalid_trade(1), &sha('a'), &sha('f'))
        .unwrap();
    assert_no_output_with_evidence(&no_output);
}

#[test]
fn reduction_report_round_trips_and_rejects_broken_quantity_conservation() {
    let mut reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    reducer
        .apply_snapshot(&snapshot("depth-epoch-1", 100, 1), &sha('a'), &sha('b'))
        .unwrap();
    let outcome = reducer
        .apply_update(
            &update(
                "depth-epoch-1",
                101,
                101,
                2,
                Some(100),
                None,
                BinanceDepthStitchVerdict::Applied,
                vec![level("64100", "0.8")],
            ),
            &sha('a'),
            &sha('c'),
        )
        .unwrap();

    let encoded = encode_reduction_report_entry_v1(&outcome.report).unwrap();
    assert_eq!(
        decode_reduction_report_entry_v1(&encoded).unwrap(),
        outcome.report
    );

    let mut broken = outcome.report;
    broken
        .quantity_conservation
        .as_mut()
        .expect("applied depth update must carry conservation evidence")
        .quantity_after = contracts::ExactDecimal("999".to_owned());
    assert!(validate_reduction_report_entry_v1(&broken).is_err());
}

#[test]
fn reduction_report_rejects_an_impossible_operation_disposition_pair() {
    let mut reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    let outcome = reducer
        .apply_snapshot(&snapshot("depth-epoch-1", 100, 1), &sha('a'), &sha('b'))
        .unwrap();
    let mut value = serde_json::to_value(outcome.report).unwrap();
    value["operation"] = serde_json::Value::String("SUPPRESS".to_owned());

    assert!(decode_reduction_report_entry_v1(&serde_json::to_vec(&value).unwrap()).is_err());
}
