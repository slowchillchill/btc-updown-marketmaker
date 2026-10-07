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
fn consecutive_applied_updates_preserve_the_depth_visibility_frontier() {
    let mut reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    reducer
        .apply_snapshot(&snapshot("depth-epoch-1", 100, 1), &sha('a'), &sha('b'))
        .unwrap();

    let mut first = update(
        "depth-epoch-1",
        101,
        101,
        2,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![level("64100", "0.8")],
    );
    first.timestamp_received = 5_000;
    first.collector_admission_time_utc_ms = 5_100;
    reducer.apply_update(&first, &sha('a'), &sha('c')).unwrap();

    let mut second = update(
        "depth-epoch-1",
        102,
        102,
        3,
        Some(100),
        Some(101),
        BinanceDepthStitchVerdict::Applied,
        vec![level("64000", "0.8")],
    );
    second.timestamp_received = 4_000;
    second.collector_admission_time_utc_ms = 5_200;
    let outcome = reducer.apply_update(&second, &sha('a'), &sha('d')).unwrap();
    let event = produced_event(&outcome);

    assert_eq!(event.time.source_event_at_utc_ms, Some(second.event_time));
    assert_eq!(event.time.received_at_utc_ms, 5_000);
    assert!(event.lineage.source_evidence.iter().any(|evidence| {
        evidence
            .collector_admission
            .as_ref()
            .is_some_and(|coordinate| coordinate.collector_admission_sequence == 2)
    }));
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
    assert_eq!(recovery_event.time.source_event_at_utc_ms, None);
    assert!(
        recovery_event
            .lineage
            .source_evidence
            .iter()
            .all(|source| { source.source_kind == CanonicalSourceKind::BinanceDepthSnapshot })
    );
    let record = CanonicalEventRecordV1::ReferenceEvent(Box::new(recovery_event.clone()));
    assert_eq!(
        project_record_event_time_index_entry(&record).unwrap(),
        None
    );
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

    let totals = outcome.report.quantity_conservation.as_ref().unwrap();
    assert_eq!(totals.quantity_before.0, "0.7");
    assert_eq!(totals.quantity_after.0, "0.8");
    assert_eq!(totals.quantity_added.0, "0.1");
    assert_eq!(totals.quantity_reduced.0, "0");

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
fn depth_update_reports_actual_decimal_total_overflow() {
    let mut reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    reducer
        .apply_snapshot(&snapshot("depth-epoch-1", 100, 1), &sha('a'), &sha('b'))
        .unwrap();
    let row = update(
        "depth-epoch-1",
        101,
        101,
        2,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![
            level("64001", "79228162514264337593543950335"),
            level("64002", "1"),
        ],
    );
    let error = reducer
        .apply_update(&row, &sha('a'), &sha('c'))
        .unwrap_err();
    assert!(
        matches!(error, data_pipeline_core::ContractError::InvalidField { field, reason }
        if field == "quantity_after" && reason == "decimal addition overflow")
    );
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

#[test]
fn prepared_binance_inputs_keep_stateful_results_before_late_materialization() {
    use contracts::*;
    use data_pipeline_core::pm_reduction::PreparedPmEvidenceV1;
    use data_pipeline_core::public_input::*;
    use data_pipeline_core::public_reduction::*;
    use data_pipeline_core::public_reference::source_evidence_v3_from_typed_raw_v2;
    use data_pipeline_core::typed_raw::TypedRawRow;
    let recorded = |row: TypedRawRow, position| {
        let row = TypedRawRowV2::Existing(row);
        let source_evidence = source_evidence_v3_from_typed_raw_v2(&row).unwrap();
        RecordedIngressV1 {
            input_stream_id: "binance".to_owned(),
            input_position: position,
            ingress: DptIngressV1::Message {
                row,
                source_evidence,
            },
            dependency_record_refs: vec![],
        }
    };
    let context = |source: SourceEvidenceRefV3, position| PublicReductionContextV1 {
        input_stream_id: "binance".to_owned(),
        closed_at_input_position: position,
        first_fact_ordinal: 0,
        first_report_ordinal: 0,
        group_id: source.source_message_id.clone(),
        source_refs: vec![CanonicalInputRecordRefV1 {
            input_stream_id: "binance".to_owned(),
            input_position: position,
        }],
        direct_sources: vec![source],
    };
    let mut depth = BinanceDepthReducer::<PreparedPublicEvidenceV1>::new(RULE_VERSION).unwrap();
    let first = recorded(
        TypedRawRow::BinanceDepthSnapshot(snapshot("epoch", 10, 1)),
        0,
    );
    let prepared =
        prepare_ingress_v1(&first, None, &Default::default(), &[], RULE_VERSION).unwrap();
    let PreparedCanonicalInputV1::BinanceDepthSnapshot { row, source } = prepared.current else {
        panic!("snapshot")
    };
    let direct = source.source().clone();
    let first_result = bind_reference_reduction_v1(
        depth.compute_snapshot(row, source).unwrap(),
        context(direct, 0),
    );
    let second = recorded(
        TypedRawRow::BinanceDepthUpdate(update(
            "epoch",
            11,
            11,
            2,
            Some(10),
            None,
            BinanceDepthStitchVerdict::Applied,
            vec![level("64100", "0.2")],
        )),
        1,
    );
    let prepared =
        prepare_ingress_v1(&second, None, &Default::default(), &[], RULE_VERSION).unwrap();
    let PreparedCanonicalInputV1::BinanceDepthUpdate { row, source } = prepared.current else {
        panic!("update")
    };
    let updated = depth.compute_update(row, source).unwrap();
    assert!(
        matches!(&updated.event.unwrap().payload, CanonicalReferenceEventPayloadV1::BinanceDepthState(value) if value.bids[0].quantity_btc.0 == "0.2")
    );
    let durable = materialize_public_reduction_v1(first_result).unwrap();
    let CanonicalEventRecordV2::ReferenceEvent(event) = &durable.records[0] else {
        panic!("reference")
    };
    assert_eq!(event.schema_version, CANONICAL_REFERENCE_EVENT_SCHEMA_V2);
    assert!(
        matches!(&event.payload, CanonicalReferenceEventPayloadV2::BinanceDepthState(value) if value.last_update_id == 10 && value.bids[0].quantity_btc.0 == "0.7")
    );
    assert!(
        matches!(&durable.reports[0].report.disposition, ReductionDispositionV1::Produced { result_event_id } if *result_event_id == event.event_id)
    );
    let mut trades = BinanceAggTradeReducer::<PreparedPublicEvidenceV1>::new(RULE_VERSION).unwrap();
    for (position, raw, expected) in [
        (2, trade(100, 1, "64000"), "BINANCE_AGGTRADE_ACCEPTED"),
        (
            3,
            trade(100, 2, "64000"),
            "BINANCE_AGGTRADE_IDENTICAL_DUPLICATE",
        ),
        (4, invalid_trade(3), "BINANCE_AGGTRADE_INVALID_INPUT"),
    ] {
        let input = recorded(TypedRawRow::BinanceAggTrade(raw), position);
        let prepared =
            prepare_ingress_v1(&input, None, &Default::default(), &[], RULE_VERSION).unwrap();
        let PreparedCanonicalInputV1::BinanceAggregateTrade {
            row,
            source,
            payload,
        } = prepared.current
        else {
            panic!("trade")
        };
        let outcome = trades.compute(row, source, payload.as_ref()).unwrap();
        assert_eq!(outcome.report.reason_code, expected);
        assert_eq!(outcome.event.is_some(), position == 2);
    }
    let conflict = recorded(TypedRawRow::BinanceAggTrade(trade(100, 4, "65000")), 5);
    let prepared =
        prepare_ingress_v1(&conflict, None, &Default::default(), &[], RULE_VERSION).unwrap();
    let PreparedCanonicalInputV1::BinanceAggregateTrade {
        row,
        source,
        payload,
    } = prepared.current
    else {
        panic!("trade")
    };
    assert!(
        trades
            .compute(row, source, payload.as_ref())
            .unwrap_err()
            .to_string()
            .contains("conflicting payloads")
    );
    // 真实 V2 来源保留物理绑定；延后物化仍取第一次真实状态。
    let mut old_depth = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    let mut pending_depth = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    let mut depth_results = Vec::new();
    for row in [
        snapshot("legacy-compare", 100, 1),
        snapshot("legacy-compare", 100, 2),
    ] {
        let expected = old_depth
            .apply_snapshot(&row, &sha('a'), &sha('b'))
            .unwrap();
        let source = data_pipeline_core::source_evidence_from_typed_raw(
            &TypedRawRow::BinanceDepthSnapshot(row.clone()),
            &sha('a'),
            &sha('b'),
        )
        .unwrap();
        let actual = pending_depth
            .compute_snapshot(&row, PreparedPmEvidenceV1::prepare(source, None).unwrap())
            .unwrap();
        depth_results.push((actual, expected));
    }
    let changed = update(
        "legacy-compare",
        101,
        101,
        3,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![level("64100", "0.2")],
    );
    let expected = old_depth
        .apply_update(&changed, &sha('a'), &sha('c'))
        .unwrap();
    let source = data_pipeline_core::source_evidence_from_typed_raw(
        &TypedRawRow::BinanceDepthUpdate(changed.clone()),
        &sha('a'),
        &sha('c'),
    )
    .unwrap();
    let actual = pending_depth
        .compute_update(
            &changed,
            PreparedPmEvidenceV1::prepare(source, None).unwrap(),
        )
        .unwrap();
    depth_results.push((actual, expected));
    for (pending, expected) in depth_results {
        let actual =
            data_pipeline_core::reduction::materialize_legacy_reference_reduction_v1(pending)
                .unwrap();
        assert_eq!(
            serde_json::to_value((&actual.event, &actual.report)).unwrap(),
            serde_json::to_value((&expected.event, &expected.report)).unwrap()
        );
    }
    let mut old_trades = BinanceAggTradeReducer::new(RULE_VERSION).unwrap();
    let mut deferred_trades = BinanceAggTradeReducer::new(RULE_VERSION).unwrap();
    let mut trade_results = Vec::new();
    for (position, raw) in [trade(300, 1, "64000"), trade(300, 2, "64000")]
        .into_iter()
        .enumerate()
    {
        let expected = old_trades.apply(&raw, &sha('a'), &sha('b')).unwrap();
        let v2 = data_pipeline_core::source_evidence_from_typed_raw(
            &TypedRawRow::BinanceAggTrade(raw.clone()),
            &sha('a'),
            &sha('b'),
        )
        .unwrap();
        let input = recorded(TypedRawRow::BinanceAggTrade(raw), position as u64);
        let prepared =
            prepare_ingress_v1(&input, None, &Default::default(), &[], RULE_VERSION).unwrap();
        let PreparedCanonicalInputV1::BinanceAggregateTrade { row, payload, .. } = prepared.current
        else {
            panic!("trade")
        };
        let actual = deferred_trades
            .compute(
                row,
                PreparedPmEvidenceV1::prepare(v2, None).unwrap(),
                payload.as_ref(),
            )
            .unwrap();
        trade_results.push((actual, expected));
    }
    for (pending, expected) in trade_results {
        let actual =
            data_pipeline_core::reduction::materialize_legacy_reference_reduction_v1(pending)
                .unwrap();
        assert_eq!(
            serde_json::to_value((&actual.event, &actual.report)).unwrap(),
            serde_json::to_value((&expected.event, &expected.report)).unwrap()
        );
    }
    let row = snapshot("legacy", 100, 1);
    let source = data_pipeline_core::source_evidence_from_typed_raw(
        &TypedRawRow::BinanceDepthSnapshot(row.clone()),
        &sha('a'),
        &sha('b'),
    )
    .unwrap();
    let mut legacy =
        BinanceDepthReducer::<PreparedPmEvidenceV1<SourceEvidenceRefV2>>::new(RULE_VERSION)
            .unwrap();
    let first = legacy
        .compute_snapshot(&row, PreparedPmEvidenceV1::prepare(source, None).unwrap())
        .unwrap();
    let later = snapshot("legacy", 200, 2);
    let source = data_pipeline_core::source_evidence_from_typed_raw(
        &TypedRawRow::BinanceDepthSnapshot(later.clone()),
        &sha('a'),
        &sha('c'),
    )
    .unwrap();
    legacy
        .compute_snapshot(&later, PreparedPmEvidenceV1::prepare(source, None).unwrap())
        .unwrap();
    let first = data_pipeline_core::reduction::materialize_legacy_reference_reduction_v1(first)
        .unwrap()
        .event
        .unwrap();
    assert_eq!(first.lineage.source_evidence[0].input_file_sha256, sha('b'));
    assert!(
        matches!(first.payload, CanonicalReferenceEventPayloadV1::BinanceDepthState(value) if value.last_update_id == 100)
    );
}

#[test]
fn consecutive_snapshotless_epochs_move_one_book_and_preserve_evidence() {
    let mut reducer = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    let mut outcomes = vec![
        reducer
            .apply_snapshot(&snapshot("epoch-1", 100, 1), &sha('a'), &sha('b'))
            .unwrap(),
    ];
    let mut retained = vec![reducer.retained_state_entries()];
    for (sequence, quantity, file) in [(2, "0", 'c'), (3, "0.8", 'd')] {
        outcomes.push(
            reducer
                .apply_update(
                    &update(
                        "epoch-1",
                        99 + sequence,
                        99 + sequence,
                        sequence,
                        Some(100),
                        (sequence > 2).then_some(98 + sequence),
                        BinanceDepthStitchVerdict::Applied,
                        vec![level("64100", quantity)],
                    ),
                    &sha('a'),
                    &sha(file),
                )
                .unwrap(),
        );
        retained.push(reducer.retained_state_entries());
    }
    let restored = produced_event(&outcomes[2]).clone();
    assert!(
        restored
            .lineage
            .source_evidence
            .iter()
            .any(|evidence| evidence
                .collector_admission
                .as_ref()
                .is_some_and(|coordinate| coordinate.collector_admission_sequence == 2)),
        "重现保留删除前序证据"
    );
    for (epoch, sequence, file) in [("epoch-2", 4, 'e'), ("epoch-3", 5, 'f')] {
        outcomes.push(
            reducer
                .apply_update(
                    &update(
                        epoch,
                        200 + sequence,
                        200 + sequence,
                        sequence,
                        None,
                        None,
                        BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable,
                        vec![level("65000", "2")],
                    ),
                    &sha('a'),
                    &sha(file),
                )
                .unwrap(),
        );
        retained.push(reducer.retained_state_entries());
    }
    let missing = produced_event(&outcomes[3]);
    assert_eq!(missing.payload, restored.payload);
    assert_eq!(missing.quality.availability, MarketDataQuality::Missing);
    assert_eq!(missing.quality.resync, ResyncQuality::Required);
    assert_eq!(missing.time.source_event_at_utc_ms, Some(1504));
    assert_eq!(missing.time.received_at_utc_ms, 2004);
    assert_no_output_with_evidence(&outcomes[4]);
    assert_eq!(
        outcomes[4].report.reason_code,
        "BINANCE_DEPTH_SNAPSHOT_UNAVAILABLE"
    );
    let mut fresh = snapshot("epoch-3", 300, 6);
    fresh.bids = vec![level("65000", "2")];
    fresh.asks = vec![level("65100", "3")];
    outcomes.push(
        reducer
            .apply_snapshot(&fresh, &sha('a'), &sha('1'))
            .unwrap(),
    );
    retained.push(reducer.retained_state_entries());
    let resynced = produced_event(&outcomes[5]);
    assert_eq!(resynced.quality.resync, ResyncQuality::ResyncBoundary);
    assert_eq!(resynced.time.received_at_utc_ms, 2006);
    assert!(resynced.lineage.source_evidence.iter().all(|evidence| {
        evidence
            .collector_admission
            .as_ref()
            .is_some_and(|coordinate| coordinate.source_sequence_epoch_id == "epoch-3")
    }));
    let error = reducer
        .apply_snapshot(&snapshot("epoch-1", 400, 7), &sha('a'), &sha('2'))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("completed Binance depth epoch cannot resume")
    );
    assert_eq!(reducer.retained_state_entries(), *retained.last().unwrap());
    let semantic_bytes = serde_json::to_vec(
        &outcomes
            .iter()
            .map(|outcome| (&outcome.event, &outcome.report))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    // 固定合法轨迹在修改前主线 9d1b290 的完整事件、身份、证据、双时间与报告摘要。
    let digest = format!("{:x}", Sha256::digest(semantic_bytes));
    assert_eq!(
        digest,
        "7c2782f7526fbcfeb0d17078533102809a48b1f86e7655008356f1d5ddcf10ed"
    );
    assert_eq!(retained, vec![6, 6, 6, 7, 8, 6]);
    println!("semantic_sha256={digest}; retained={retained:?}");
}

#[test]
fn quantity_only_path_reuses_depth_admission_gap_and_maps() {
    use data_pipeline_core::{source_evidence_from_typed_raw, typed_raw::TypedRawRow};
    let mut full = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    let mut thin = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    let initial = snapshot("quantity-1", 100, 1);
    let evidence = source_evidence_from_typed_raw(
        &TypedRawRow::BinanceDepthSnapshot(initial.clone()),
        &sha('a'),
        &sha('b'),
    )
    .unwrap();
    full.compute_snapshot(&initial, evidence.clone()).unwrap();
    thin.apply_quantity_snapshot(&initial, evidence).unwrap();
    assert_eq!(
        thin.current_quantity_totals().unwrap(),
        full.current_quantity_totals().unwrap()
    );
    let overwrite = update(
        "quantity-1",
        101,
        101,
        2,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![level("64000", "0"), level("64100", "2")],
    );
    let hole = update(
        "quantity-1",
        105,
        105,
        3,
        Some(100),
        Some(101),
        BinanceDepthStitchVerdict::NotAppliedConfirmedHole,
        vec![level("64100", "1")],
    );
    let switched = update(
        "quantity-2",
        106,
        106,
        4,
        None,
        None,
        BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable,
        vec![level("64100", "1")],
    );
    for row in [&overwrite, &hole, &switched] {
        let evidence = source_evidence_from_typed_raw(
            &TypedRawRow::BinanceDepthUpdate(row.clone()),
            &sha('a'),
            &sha('b'),
        )
        .unwrap();
        full.compute_update(row, evidence.clone()).unwrap();
        thin.apply_quantity_update(row, evidence).unwrap();
        assert_eq!(
            thin.current_quantity_totals().unwrap(),
            full.current_quantity_totals().unwrap()
        );
        if row.collector_admission_sequence == 2 {
            let (bid, ask) = thin.current_quantity_totals().unwrap().unwrap();
            assert_eq!(
                (bid.numerator.as_str(), bid.denominator.as_str()),
                ("2", "1")
            );
            assert_eq!(
                (ask.numerator.as_str(), ask.denominator.as_str()),
                ("7", "10")
            );
        } else {
            assert!(thin.current_quantity_totals().unwrap().is_none());
        }
    }
    let restore = snapshot("quantity-2", 200, 5);
    let evidence = source_evidence_from_typed_raw(
        &TypedRawRow::BinanceDepthSnapshot(restore.clone()),
        &sha('a'),
        &sha('b'),
    )
    .unwrap();
    full.compute_snapshot(&restore, evidence.clone()).unwrap();
    thin.apply_quantity_snapshot(&restore, evidence.clone())
        .unwrap();
    assert_eq!(
        thin.current_quantity_totals().unwrap(),
        full.current_quantity_totals().unwrap()
    );
    assert_eq!(
        full.compute_snapshot(&restore, evidence.clone())
            .unwrap_err()
            .to_string(),
        thin.apply_quantity_snapshot(&restore, evidence)
            .unwrap_err()
            .to_string()
    );
}

#[test]
fn quantity_sampling_preserves_valid_sum_beyond_decimal_capacity() {
    use data_pipeline_core::{source_evidence_from_typed_raw, typed_raw::TypedRawRow};
    let mut row = snapshot("quantity-large", 100, 1);
    row.bids = data_pipeline_core::typed_raw::parse_depth_levels_json(
        r#"[["100","79228162514264337593543950335"],["99","79228162514264337593543950335"]]"#,
        "bids",
        true,
    )
    .unwrap();
    let evidence = source_evidence_from_typed_raw(
        &TypedRawRow::BinanceDepthSnapshot(row.clone()),
        &sha('a'),
        &sha('b'),
    )
    .unwrap();
    let mut thin = BinanceDepthReducer::new("rule-v1").unwrap();
    thin.apply_quantity_snapshot(&row, evidence).unwrap();
    let (bid, _) = thin.current_quantity_totals().unwrap().unwrap();
    assert_eq!(bid.numerator, "158456325028528675187087900670");
    assert_eq!(bid.denominator, "1");

    // 同样合法数量来自 applied 更新时，也不能触发无消费者的报告溢出。
    let initial = snapshot("quantity-large-update", 100, 1);
    let mut thin = BinanceDepthReducer::new("rule-v1").unwrap();
    let evidence = source_evidence_from_typed_raw(
        &TypedRawRow::BinanceDepthSnapshot(initial.clone()),
        &sha('a'),
        &sha('b'),
    )
    .unwrap();
    thin.apply_quantity_snapshot(&initial, evidence).unwrap();
    let applied = update(
        "quantity-large-update",
        101,
        101,
        2,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![
            level("64000", "79228162514264337593543950335"),
            level("64100", "79228162514264337593543950335"),
        ],
    );
    let evidence = source_evidence_from_typed_raw(
        &TypedRawRow::BinanceDepthUpdate(applied.clone()),
        &sha('a'),
        &sha('b'),
    )
    .unwrap();
    thin.apply_quantity_update(&applied, evidence).unwrap();
    let (bid, _) = thin.current_quantity_totals().unwrap().unwrap();
    assert_eq!(bid.numerator, "158456325028528675187087900670");
    assert_eq!(bid.denominator, "1");
}

#[test]
fn received_day_reset_and_snapshot_provenance_preserve_pending_and_strict_stitching() {
    use data_pipeline_core::{source_evidence_from_typed_raw, typed_raw::TypedRawRow};
    const DAY: i64 = 86_400_000;
    let epoch = "day-continuity";
    let mut full = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    let mut thin = BinanceDepthReducer::new(RULE_VERSION).unwrap();
    let mut initial = snapshot(epoch, 100, 1);
    initial.timestamp_received = DAY - 200;
    initial.collector_admission_time_utc_ms = DAY - 190;
    initial.depth_ingress_sequence = 10;
    let mut applied = update(
        epoch,
        101,
        101,
        2,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![level("64100", "0.7")],
    );
    applied.timestamp_received = DAY - 100;
    applied.collector_admission_time_utc_ms = DAY - 90;
    applied.depth_ingress_sequence = 11;
    let mut unavailable = update(
        epoch,
        102,
        102,
        3,
        None,
        None,
        BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable,
        vec![level("64100", "0.7")],
    );
    unavailable.timestamp_received = DAY + 15;
    unavailable.event_time = DAY - 5; // 日界取接收日，不能取交易所事件日。
    unavailable.collector_admission_time_utc_ms = DAY + 3_369;
    unavailable.depth_ingress_sequence = 12;
    let mut restored = snapshot(epoch, 200, 4);
    restored.timestamp_received = DAY + 3_609;
    restored.collector_admission_time_utc_ms = DAY + 3_616;
    restored.depth_ingress_sequence = 30;
    let mut pending = update(
        epoch,
        201,
        201,
        5,
        Some(200),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![level("64100", "0.7")],
    );
    pending.timestamp_received = DAY - 10;
    pending.collector_admission_time_utc_ms = DAY + 3_617;
    pending.depth_ingress_sequence = 13; // 快照响应前已收到，新快照后才准入。
    let mut settled = snapshot(epoch, 201, 6);
    settled.timestamp_received = DAY + 4_000;
    settled.collector_admission_time_utc_ms = DAY + 4_010;
    settled.depth_ingress_sequence = 35;
    let mut identical = settled.clone();
    identical.timestamp_received = 2 * DAY + 10;
    identical.collector_admission_time_utc_ms = 2 * DAY + 20;
    identical.collector_admission_sequence = 7;
    identical.depth_ingress_sequence = 40;
    let mut drained_after_identical = update(
        epoch,
        190,
        200,
        8,
        Some(201),
        None,
        BinanceDepthStitchVerdict::NotAppliedStaleDuplicate,
        vec![level("64100", "0.7")],
    );
    drained_after_identical.timestamp_received = DAY + 4_100;
    drained_after_identical.collector_admission_time_utc_ms = 2 * DAY + 25;
    drained_after_identical.depth_ingress_sequence = 36;
    let mut current = update(
        epoch,
        202,
        202,
        9,
        Some(201),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![level("64100", "0.7")],
    );
    current.timestamp_received = 2 * DAY + 30;
    current.collector_admission_time_utc_ms = 2 * DAY + 40;
    current.depth_ingress_sequence = 41;
    let mut unproved = update(
        epoch,
        203,
        203,
        10,
        None,
        None,
        BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable,
        vec![level("64100", "0.7")],
    );
    unproved.timestamp_received = 2 * DAY + 50;
    unproved.collector_admission_time_utc_ms = 2 * DAY + 60;
    unproved.depth_ingress_sequence = 42;

    let trajectory = [
        TypedRawRow::BinanceDepthSnapshot(initial),
        TypedRawRow::BinanceDepthUpdate(applied),
        TypedRawRow::BinanceDepthUpdate(unavailable),
        TypedRawRow::BinanceDepthSnapshot(restored),
        TypedRawRow::BinanceDepthUpdate(pending),
        TypedRawRow::BinanceDepthSnapshot(settled),
        TypedRawRow::BinanceDepthSnapshot(identical),
        TypedRawRow::BinanceDepthUpdate(drained_after_identical),
        TypedRawRow::BinanceDepthUpdate(current),
        TypedRawRow::BinanceDepthUpdate(unproved),
    ];
    let mut pre_boundary_totals = None;
    for (step, row) in trajectory.iter().enumerate() {
        let evidence = source_evidence_from_typed_raw(row, &sha('a'), &sha('b')).unwrap();
        let (complete, quantity) = match row {
            TypedRawRow::BinanceDepthSnapshot(row) => (
                full.compute_snapshot(row, evidence.clone()),
                thin.apply_quantity_snapshot(row, evidence),
            ),
            TypedRawRow::BinanceDepthUpdate(row) => (
                full.compute_update(row, evidence.clone()),
                thin.apply_quantity_update(row, evidence),
            ),
            _ => unreachable!(),
        };
        if step == 9 {
            let error = complete.unwrap_err().to_string();
            assert!(error.contains("binance_depth_update.stitch_coordinates"));
            assert_eq!(error, quantity.unwrap_err().to_string());
            assert!(thin.current_quantity_totals().unwrap().is_some());
            continue;
        }
        let outcome = complete.unwrap();
        quantity.unwrap();
        assert_eq!(
            full.current_quantity_totals().unwrap(),
            thin.current_quantity_totals().unwrap()
        );
        if step == 1 {
            // 前单元结束后，午夜采样仍读原book；后到可见行不能回填采样。
            pre_boundary_totals = thin.current_quantity_totals().unwrap();
            assert!(pre_boundary_totals.is_some());
        } else if step == 2 {
            assert!(thin.current_quantity_totals().unwrap().is_none());
            let event = produced_event(&outcome);
            assert_eq!(event.quality.availability, MarketDataQuality::Missing);
            assert_eq!(event.quality.resync, ResyncQuality::Required);
            assert_eq!(event.time.received_at_utc_ms, DAY + 15);
            assert_eq!(
                outcome.report.reason_code,
                "BINANCE_DEPTH_UTC_DAY_SNAPSHOT_REQUIRED"
            );
            assert!(event.lineage.source_evidence.iter().any(|e| {
                e.collector_admission.as_ref().is_some_and(|a| {
                    a.collector_admission_sequence == 3 && a.admitted_at_utc_ms == DAY + 3_369
                })
            }));
            assert!(pre_boundary_totals.is_some());
        } else if step == 3 {
            assert_eq!(
                produced_event(&outcome).quality.resync,
                ResyncQuality::ResyncBoundary
            );
            assert!(thin.current_quantity_totals().unwrap().is_some());
        } else if step == 4 {
            let event = produced_event(&outcome);
            assert_eq!(event.time.received_at_utc_ms, DAY + 3_609);
            assert!(matches!(&event.payload,
                CanonicalReferenceEventPayloadV1::BinanceDepthState(v) if v.last_update_id == 201));
        } else if step == 6 {
            assert!(outcome.event.is_none());
            assert_eq!(outcome.report.operation, ReductionOperationKindV1::Confirm);
            assert_eq!(
                outcome.report.reason_code,
                "BINANCE_DEPTH_IDENTICAL_SNAPSHOT"
            );
        } else if step == 7 {
            assert!(outcome.event.is_none());
            assert_eq!(outcome.report.reason_code, "BINANCE_DEPTH_STALE_DUPLICATE");
        } else if step == 8 {
            assert!(matches!(&produced_event(&outcome).payload,
                CanonicalReferenceEventPayloadV1::BinanceDepthState(v) if v.last_update_id == 202));
        }
    }
}
