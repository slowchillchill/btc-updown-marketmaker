use super::*;
use crate::polymarket_market_metadata_layout::{
    POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION, PolymarketMarketMetadataV5Row,
};
use crate::polymarket_orderbook_v3::{
    PolymarketOrderbookV3Admission, polymarket_orderbook_v3_messages_from_batch,
    polymarket_orderbook_v3_messages_to_batch,
};
use polymarket_client_sdk_v2::clob::ws::interest::MessageInterest;
use polymarket_client_sdk_v2::clob::ws::types::response::parse_if_interested;

#[test]
fn polymarket_late_activation_builder_rejects_applied_at_e() {
    let market = test_btc_discovery().current;
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let market_end_utc_ms = market.end_ts_s * 1_000;
    let mut accepted =
        polymarket_candidate_to_active_v3_row(&market, market_end_utc_ms - 1, &identity).unwrap();
    accepted.collector_admission_sequence = 1;
    accepted.collector_admission_time_utc_ms = market_end_utc_ms - 1;
    assert!(accepted.validate().is_complete());
    assert!(
        polymarket_candidate_to_active_v3_row(&market, market_end_utc_ms, &identity)
            .unwrap_err()
            .to_string()
            .contains("outside active window")
    );
}

#[test]
fn polymarket_metadata_v5_nonclaimant_raw_event_round_trips_and_writes() {
    let discovery = test_btc_discovery();
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let prefix = polymarket_metadata_snapshot_v3_row(
        &discovery.current,
        MarketRuntimeRole::Current,
        &collector_identity,
    )
    .unwrap();
    let admission_time_utc_ms = prefix.fixed_identity_verified_at_utc + 1;
    let expected = PolymarketMarketMetadataV5Row::from_v3_prefix(prefix, None, None);
    let mut event = polymarket_metadata_v5_raw_event(expected.clone()).unwrap();
    let draft = serde_json::from_str::<Value>(&event.raw_json).unwrap();
    assert_eq!(
        draft["schema_version"],
        POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION
    );
    assert!(draft["ws_claimant_source_sequence_epoch_id"].is_null());
    assert!(draft["ws_claimant_collector_admission_sequence"].is_null());
    assert!(draft.get("resolution_evidence_raw_sha256").is_none());
    assert!(draft.get("collector_admission_sequence").is_none());
    assert!(draft.get("collector_admission_time_utc_ms").is_none());

    event.collector_admission = Some(RawEventAdmission {
        sequence: 1,
        time_utc_ms: admission_time_utc_ms,
    });
    let mut forbidden_event = event.clone();
    let mut forbidden_draft = draft.clone();
    forbidden_draft
        .as_object_mut()
        .unwrap()
        .insert("resolution_evidence_raw_sha256".to_owned(), Value::Null);
    forbidden_event.raw_json = serde_json::to_string(&forbidden_draft).unwrap();
    let error = polymarket_metadata_v5_rows_from_event(&forbidden_event)
        .unwrap_err()
        .to_string();
    assert!(error.contains("contains removed field"), "{error}");

    let rows = polymarket_metadata_v5_rows_from_event(&event).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].compatibility_prefix().collector_admission_sequence,
        1
    );
    assert_eq!(
        rows[0]
            .compatibility_prefix()
            .collector_admission_time_utc_ms,
        admission_time_utc_ms
    );
    assert!(rows[0].validate().is_complete());

    let tmp = tempdir().unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "metadata-v5-nonclaimant",
        POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION,
        "polymarket_market_metadata",
    )
    .unwrap();
    let logical_bytes = u64::try_from(owned_event_bytes(&event)).unwrap();
    assert_eq!(
        write_polymarket_metadata_v5_live_event(&mut writer, &event, logical_bytes).unwrap(),
        1
    );
    assert_eq!(writer.finalize().unwrap().row_count, 1);
}

#[test]
fn polymarket_metadata_v5_ws_claimant_raw_event_round_trips_and_writes() {
    let discovery = test_btc_discovery();
    let identity = PendingPolymarketIdentity::from_discovered(&discovery.current).unwrap();
    let source_received_at_utc_ms = identity.market_end_utc_ms + 3 * 60_000;
    let source_payload_json = r#"{"event_type":"market_resolved"}"#.to_owned();
    let evidence = PolymarketWsTerminalEvidence {
        source_sequence_epoch_id: "test-polymarket-source-epoch".to_owned(),
        collector_admission_sequence: 1,
        source_received_at_utc_ms,
        validation_completed_at_utc_ms: source_received_at_utc_ms + 1,
        source_payload_sha256: Sha256::digest(source_payload_json.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        source_payload_json,
        platform_timestamp_raw: source_received_at_utc_ms.to_string(),
        platform_timestamp_utc_ms: source_received_at_utc_ms,
        winning_asset_id: identity.up_asset_id.clone(),
        winning_outcome: "Up".to_owned(),
        raw_day_ticket: accepted_raw_day_ticket_for_sync_test(source_received_at_utc_ms),
    };
    let pending = PendingPolymarketResolution::new(identity, None);
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let applied_at_utc_ms = source_received_at_utc_ms + 2;
    let prefix = polymarket_resolution_terminal_ws_v3_row(
        &pending,
        &evidence,
        applied_at_utc_ms,
        &collector_identity,
    )
    .unwrap();
    let expected = PolymarketMarketMetadataV5Row::from_v3_prefix(
        prefix,
        Some("source-epoch-1".to_owned()),
        Some(41),
    );
    let mut event = polymarket_metadata_v5_raw_event(expected).unwrap();
    let draft = serde_json::from_str::<Value>(&event.raw_json).unwrap();
    assert_eq!(
        draft["ws_claimant_source_sequence_epoch_id"],
        "source-epoch-1"
    );
    assert_eq!(draft["ws_claimant_collector_admission_sequence"], 41);
    assert!(draft.get("resolution_evidence_raw_sha256").is_none());

    event.collector_admission = Some(RawEventAdmission {
        sequence: 2,
        time_utc_ms: applied_at_utc_ms + 1,
    });
    let rows = polymarket_metadata_v5_rows_from_event(&event).unwrap();
    assert_eq!(
        rows[0].ws_claimant_source_sequence_epoch_id.as_deref(),
        Some("source-epoch-1")
    );
    assert_eq!(rows[0].ws_claimant_collector_admission_sequence, Some(41));
    assert!(rows[0].validate().is_complete());

    let tmp = tempdir().unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "metadata-v5-claimant",
        POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION,
        "polymarket_market_metadata",
    )
    .unwrap();
    let logical_bytes = u64::try_from(owned_event_bytes(&event)).unwrap();
    assert_eq!(
        write_polymarket_metadata_v5_live_event(&mut writer, &event, logical_bytes).unwrap(),
        1
    );
    assert_eq!(writer.finalize().unwrap().row_count, 1);
}

#[test]
fn polymarket_v3_target_admission_keeps_one_ordered_mixed_price_change_projection() {
    let discovery = test_btc_discovery();
    let asset_to_slug = discovery_asset_to_slug(&discovery);
    let message = serde_json::from_str::<WsMessage>(
        r#"{
            "event_type":"price_change",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "timestamp":"1783392404600",
            "price_changes":[
                {"asset_id":"102","price":"0.51","size":"3","side":"SELL"},
                {"asset_id":"999","price":"0.50","size":"2","side":"BUY"},
                {"asset_id":"101","price":"0.49","size":"4","side":"BUY"}
            ]
        }"#,
    )
    .unwrap();

    let admitted = target_admitted_polymarket_message(&message, &discovery, &asset_to_slug)
        .unwrap()
        .expect("mixed message retains target entries");
    assert_eq!(
        admitted.matched_assets,
        BTreeSet::from(["101".to_owned(), "102".to_owned()])
    );
    assert_eq!(admitted.slug.as_str(), discovery.current.slug.as_str());
    let projection = admitted.polymarket_v3_projection().unwrap();
    let PolymarketOrderbookV3Event::PriceChange(price_change) = projection.event else {
        panic!("expected price_change projection");
    };
    assert_eq!(
        price_change
            .price_changes
            .iter()
            .map(|change| change.asset_id.as_str())
            .collect::<Vec<_>>(),
        ["102", "101"]
    );
}

#[test]
fn polymarket_v3_target_admission_ignores_unmatched_and_rejects_target_contradiction() {
    let discovery = test_btc_discovery();
    let asset_to_slug = discovery_asset_to_slug(&discovery);
    let unmatched = serde_json::from_str::<WsMessage>(
        r#"{
            "event_type":"book",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000009",
            "asset_id":"999",
            "timestamp":"1783392404600",
            "bids":[],
            "asks":[]
        }"#,
    )
    .unwrap();
    assert!(
        target_admitted_polymarket_message(&unmatched, &discovery, &asset_to_slug)
            .unwrap()
            .is_none()
    );

    let contradictory = serde_json::from_str::<WsMessage>(
        r#"{
            "event_type":"book",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000009",
            "asset_id":"101",
            "timestamp":"1783392404600",
            "bids":[],
            "asks":[]
        }"#,
    )
    .unwrap();
    assert!(
        target_admitted_polymarket_message(&contradictory, &discovery, &asset_to_slug,)
            .unwrap_err()
            .to_string()
            .contains("contradictory condition_id")
    );
}

#[test]
fn polymarket_v3_seven_parser_fixtures_round_trip_after_real_target_admission() {
    let discovery = test_btc_discovery();
    let asset_to_slug = discovery_asset_to_slug(&discovery);
    let cases = [
        r#"{
            "event_type":"book",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "asset_id":"101",
            "timestamp":"1783392404600",
            "bids":[{"price":"0.49","size":"3"}],
            "asks":[{"price":"0.51","size":"4"}],
            "last_trade_price":"0.50"
        }"#,
        r#"{
            "event_type":"price_change",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "timestamp":"1783392404600",
            "price_changes":[
                {"asset_id":"102","price":"0.51","size":"3","side":"SELL"},
                {"asset_id":"101","price":"0.49","size":"4","side":"BUY"}
            ]
        }"#,
        r#"{
            "event_type":"tick_size_change",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "asset_id":"101",
            "old_tick_size":null,
            "new_tick_size":"0.001",
            "timestamp":"1783392404600"
        }"#,
        r#"{
            "event_type":"last_trade_price",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "asset_id":"101",
            "price":"0.50",
            "side":null,
            "size":null,
            "fee_rate_bps":null,
            "timestamp":"1783392404600"
        }"#,
        r#"{
            "event_type":"best_bid_ask",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "asset_id":"101",
            "best_bid":null,
            "best_ask":null,
            "spread":null,
            "timestamp":"1783392404600"
        }"#,
        r#"{
            "event_type":"new_market",
            "id":"market-current",
            "question":null,
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "slug":"btc-updown-5m-1783392300",
            "description":null,
            "assets_ids":["101","102"],
            "outcomes":["Up","Down"],
            "event_message":{"id":"event-1"},
            "timestamp":"1783392404600"
        }"#,
    ];
    let mut messages = Vec::with_capacity(7);
    for (index, raw) in cases.into_iter().enumerate() {
        let parsed = parse_if_interested(raw.as_bytes(), &MessageInterest::ALL).unwrap();
        assert_eq!(parsed.len(), 1);
        let admitted = target_admitted_polymarket_message(&parsed[0], &discovery, &asset_to_slug)
            .unwrap()
            .expect("fixture must pass the real target-admission owner");
        messages.push(
            admitted
                .polymarket_v3_projection()
                .unwrap()
                .admit(PolymarketOrderbookV3Admission {
                    timestamp_received: 1_783_392_404_599,
                    collector_host_id: "collector-test".to_owned(),
                    collector_region: "local-test".to_owned(),
                    source_endpoint: MARKET_WS_ENDPOINT.to_owned(),
                    source_sequence_epoch_id: "source-epoch-1".to_owned(),
                    collector_admission_sequence: u64::try_from(index + 1).unwrap(),
                    collector_admission_time_utc_ms: 1_783_392_404_600,
                })
                .unwrap(),
        );
    }

    let resolved_raw = r#"{
        "event_type":"market_resolved",
        "id":"market-current",
        "question":null,
        "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
        "slug":null,
        "description":null,
        "assets_ids":["101","102"],
        "outcomes":["Up","Down"],
        "winning_asset_id":"101",
        "winning_outcome":"Up",
        "event_message":null,
        "timestamp":"1783392404600"
    }"#;
    let parsed = parse_if_interested(resolved_raw.as_bytes(), &MessageInterest::ALL).unwrap();
    let WsMessage::MarketResolved(resolved) = &parsed[0] else {
        panic!("fixture must decode as market_resolved");
    };
    messages.push(
        target_admitted_market_resolved_v3_projection(
            resolved,
            &PolymarketProcessingContext::new(discovery),
        )
        .unwrap()
        .expect("resolved fixture must pass the source-owned target admission")
        .admit(PolymarketOrderbookV3Admission {
            timestamp_received: 1_783_392_404_599,
            collector_host_id: "collector-test".to_owned(),
            collector_region: "local-test".to_owned(),
            source_endpoint: MARKET_WS_ENDPOINT.to_owned(),
            source_sequence_epoch_id: "source-epoch-1".to_owned(),
            collector_admission_sequence: 7,
            collector_admission_time_utc_ms: 1_783_392_404_600,
        })
        .unwrap(),
    );

    assert_eq!(
        messages
            .iter()
            .map(|message| message.event_type())
            .collect::<Vec<_>>(),
        [
            "book",
            "price_change",
            "tick_size_change",
            "last_trade_price",
            "best_bid_ask",
            "new_market",
            "market_resolved",
        ]
    );
    let batch = polymarket_orderbook_v3_messages_to_batch(&messages).unwrap();
    assert_eq!(
        polymarket_orderbook_v3_messages_from_batch(&batch).unwrap(),
        messages
    );
}

#[test]
fn polymarket_business_assets_must_match_their_message_condition() {
    let current = test_btc_discovery().current;
    let mut next = current.clone();
    next.id = "market-next".to_owned();
    next.slug = "btc-updown-5m-1783392600".to_owned();
    next.condition_id =
        "0x0000000000000000000000000000000000000000000000000000000000000003".to_owned();
    next.up_asset_id = "201".to_owned();
    next.down_asset_id = "202".to_owned();
    next.start_ts_s += 300;
    next.end_ts_s += 300;
    next.candidate_start_ts_s += 300;
    next.candidate_end_ts_s += 300;
    let discovery = BtcUpdownDiscovery {
        current: current.clone(),
        next: Some(next.clone()),
        relation: None,
    };
    let asset_to_slug = discovery_asset_to_slug(&discovery);
    let assert_error = |message: WsMessage| {
        let error = target_admitted_polymarket_message(&message, &discovery, &asset_to_slug)
            .expect_err("a target asset under the wrong condition must be terminal");
        assert!(error.to_string().contains("contradictory condition_id"));
    };

    assert_error(
        serde_json::from_value::<WsMessage>(json!({
            "event_type": "book",
            "asset_id": "101",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000009",
            "timestamp": "1783392404600",
            "bids": [],
            "asks": []
        }))
        .unwrap(),
    );
    assert_error(
        serde_json::from_value::<WsMessage>(json!({
            "event_type": "book",
            "asset_id": "201",
            "market": current.condition_id,
            "timestamp": "1783392404600",
            "bids": [],
            "asks": []
        }))
        .unwrap(),
    );
    assert_error(
        serde_json::from_value::<WsMessage>(json!({
            "event_type": "price_change",
            "market": current.condition_id,
            "timestamp": "1783392404600",
            "price_changes": [
                {"asset_id":"101","price":"0.52","size":"10","side":"BUY"},
                {"asset_id":"201","price":"0.48","size":"8","side":"SELL"},
                {"asset_id":"999","price":"0.50","size":"1","side":"BUY"}
            ]
        }))
        .unwrap(),
    );
}
