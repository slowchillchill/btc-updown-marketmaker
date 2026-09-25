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
fn polymarket_metadata_v6_preserves_v5_identity_evidence_and_admission() {
    use crate::polymarket_market_metadata_v6_layout::{
        PolymarketMarketMetadataV6Row, polymarket_market_metadata_v6_row_from_batch,
        polymarket_market_metadata_v6_rows_to_batch, polymarket_market_metadata_v6_schema,
    };
    let discovery = test_btc_discovery();
    let prefix = polymarket_metadata_snapshot_row(
        &discovery.current,
        MarketRuntimeRole::Current,
        &CollectorIdentity {
            host_id: "collector-test".to_owned(),
            region: "local-test".to_owned(),
        },
    )
    .unwrap();
    let original_payload = prefix.source_evidence_payload_json.clone();
    let original_digest = prefix.source_evidence_raw_sha256.clone();
    let row = PolymarketMarketMetadataV6Row::Existing(PolymarketMarketMetadataV5Row::from_common(
        prefix, None, None,
    ));
    let mut event = polymarket_metadata_v6_raw_event(row).unwrap();
    event.collector_admission = Some(RawEventAdmission {
        sequence: 7,
        time_utc_ms: discovery.current.fixed_identity_verified_at_utc_ms + 1,
    });
    let row = polymarket_metadata_v6_row_for_event(&event).unwrap();
    let batch =
        polymarket_market_metadata_v6_rows_to_batch(polymarket_market_metadata_v6_schema(), &[row])
            .unwrap();
    let decoded = polymarket_market_metadata_v6_row_from_batch(&batch, 0).unwrap();
    let common = decoded.existing().unwrap().common();
    assert_eq!(common.source_evidence_payload_json, original_payload);
    assert_eq!(common.source_evidence_raw_sha256, original_digest);
    assert_eq!(common.collector_admission_sequence, 7);
    assert_eq!(
        common.metadata_row_kind,
        PolymarketMetadataRowKind::MetadataSnapshot
    );
    assert!(
        batch
            .column_by_name("price_to_beat_decimal")
            .unwrap()
            .is_null(0)
    );
    assert!(batch.column_by_name("http_body").unwrap().is_null(0));
}

#[test]
fn polymarket_metadata_v6_anchor_is_independent_and_preserves_exact_http_evidence() {
    use crate::market_anchor::{MarketAnchorIdentity, MarketAnchorObservation, MarketAnchorOrigin};
    use crate::polymarket_market_metadata_v6_layout::{
        MarketAnchorRuleUpdateRow, PolymarketMarketMetadataV6Row,
        polymarket_market_metadata_v6_row_from_batch, polymarket_market_metadata_v6_rows_to_batch,
        polymarket_market_metadata_v6_schema,
    };
    use polymarket_client_sdk_v2::gamma::types::response::{Event, ResponseCapture};
    let market = test_btc_discovery().current;
    let identity = MarketAnchorIdentity {
        slug: market.slug.clone(),
        condition_id: market.condition_id.clone(),
        clob_market_id: market.id.clone(),
        asset_ids: serde_json::to_string(&[&market.up_asset_id, &market.down_asset_id]).unwrap(),
        market_start_utc: market.start_ts_s * 1_000,
        market_end_utc: market.end_ts_s * 1_000,
        fixed_identity_verified_at_utc: market.fixed_identity_verified_at_utc_ms,
    };
    let body = format!(
        r#"{{"id":"event-1","slug":"{}","eventMetadata":{{"priceToBeat":64950.0142185114400000001}},"markets":[{}]}}"#,
        market.slug,
        market.raw_json.as_ref().unwrap()
    );
    let received = identity.market_start_utc + 1;
    let capture = ResponseCapture::builder()
        .result(Ok(serde_json::from_str::<Event>(&body).unwrap()))
        .raw_body(body.as_bytes().to_vec())
        .request_url(
            format!(
                "https://gamma-api.polymarket.com/events/slug/{}",
                market.slug
            )
            .parse()
            .unwrap(),
        )
        .response_url(
            format!(
                "https://gamma-api.polymarket.com/events/slug/{}",
                market.slug
            )
            .parse()
            .unwrap(),
        )
        .received_at_utc(
            polymarket_client_sdk_v2::types::DateTime::from_timestamp_millis(received).unwrap(),
        )
        .status_code(reqwest::StatusCode::OK)
        .build();
    let (observation, _) = MarketAnchorObservation::from_gamma_capture(capture, &identity);
    let row = PolymarketMarketMetadataV6Row::AnchorRule(MarketAnchorRuleUpdateRow {
        timestamp_received: received,
        collector_admission_sequence: 0,
        collector_admission_time_utc_ms: 0,
        metadata_origin: MarketAnchorOrigin::GammaEventBySlug,
        collector_host_id: "collector-test".to_owned(),
        collector_region: "local-test".to_owned(),
        identity,
        market_lifecycle_state: MarketLifecycleState::Active,
        market_runtime_role: MarketRuntimeRole::Current,
        metadata_fetch_start_utc: received - 1,
        anchor_query_source_epoch_id: "epoch-1".to_owned(),
        anchor_query_ordinal: 1,
        observation,
    });
    let mut event = polymarket_metadata_v6_raw_event(row).unwrap();
    event.collector_admission = Some(RawEventAdmission {
        sequence: 8,
        time_utc_ms: received + 10,
    });
    let row = polymarket_metadata_v6_row_for_event(&event).unwrap();
    let batch =
        polymarket_market_metadata_v6_rows_to_batch(polymarket_market_metadata_v6_schema(), &[row])
            .unwrap();
    let decoded = polymarket_market_metadata_v6_row_from_batch(&batch, 0).unwrap();
    assert!(decoded.existing().is_none());
    let PolymarketMarketMetadataV6Row::AnchorRule(decoded) = decoded else {
        unreachable!()
    };
    assert_eq!(
        decoded.observation.price_to_beat_decimal.as_deref(),
        Some("64950.0142185114400000001")
    );
    assert_eq!(
        decoded.observation.http_body.as_deref(),
        Some(body.as_str())
    );
    assert_eq!(decoded.timestamp_received, received);
    assert_eq!(decoded.collector_admission_time_utc_ms, received + 10);
    assert!(
        batch
            .column_by_name("source_evidence_payload_json")
            .unwrap()
            .is_null(0)
    );
    assert!(batch.column_by_name("winning_asset_id").unwrap().is_null(0));
    let mut corrupted: Value = serde_json::from_str(&event.raw_json).unwrap();
    corrupted["winning_asset_id"] = "101".into();
    assert!(
        PolymarketMarketMetadataV6Row::from_raw_value(corrupted, 8, received + 10)
            .unwrap_err()
            .contains("winning_asset_id")
    );
}

#[test]
fn polymarket_late_activation_builder_rejects_applied_at_e() {
    let market = test_btc_discovery().current;
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let market_end_utc_ms = market.end_ts_s * 1_000;
    let mut accepted =
        polymarket_candidate_to_active_row(&market, market_end_utc_ms - 1, &identity).unwrap();
    accepted.collector_admission_sequence = 1;
    accepted.collector_admission_time_utc_ms = market_end_utc_ms - 1;
    assert!(
        PolymarketMarketMetadataV5Row::from_common(accepted, None, None)
            .validate()
            .is_complete()
    );
    assert!(
        polymarket_candidate_to_active_row(&market, market_end_utc_ms, &identity)
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
    let prefix = polymarket_metadata_snapshot_row(
        &discovery.current,
        MarketRuntimeRole::Current,
        &collector_identity,
    )
    .unwrap();
    let admission_time_utc_ms = prefix.fixed_identity_verified_at_utc + 1;
    let expected = PolymarketMarketMetadataV5Row::from_common(prefix, None, None);
    assert!(expected.validate_draft().is_complete());
    let pre_admission_validation = expected.validate();
    assert!(!pre_admission_validation.is_complete());
    assert!(
        pre_admission_validation
            .violations
            .contains(&"collector_admission_sequence:required".to_owned())
    );
    assert!(
        pre_admission_validation
            .violations
            .contains(&"collector_admission_time_utc_ms:required".to_owned())
    );
    let mut event = polymarket_metadata_raw_event(expected.clone()).unwrap();
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
    let error = polymarket_metadata_row_from_event(&forbidden_event)
        .unwrap_err()
        .to_string();
    assert!(error.contains("contains removed field"), "{error}");

    let row = polymarket_metadata_row_from_event(&event).unwrap();
    assert_eq!(row.common().collector_admission_sequence, 1);
    assert_eq!(
        row.common().collector_admission_time_utc_ms,
        admission_time_utc_ms
    );
    assert!(row.validate().is_complete());

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
        write_polymarket_metadata_event(&mut writer, &event, logical_bytes).unwrap(),
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
        raw_day_ticket: Some(accepted_raw_day_ticket_for_sync_test(
            source_received_at_utc_ms,
        )),
    };
    let pending = PendingPolymarketResolution::new(identity, None);
    let collector_identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let applied_at_utc_ms = source_received_at_utc_ms + 2;
    let prefix = polymarket_resolution_terminal_ws_row(
        &pending,
        &evidence,
        applied_at_utc_ms,
        &collector_identity,
    )
    .unwrap();
    let expected = PolymarketMarketMetadataV5Row::from_common(
        prefix,
        Some("source-epoch-1".to_owned()),
        Some(41),
    );
    let mut event = polymarket_metadata_raw_event(expected).unwrap();
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
    let row = polymarket_metadata_row_from_event(&event).unwrap();
    assert_eq!(
        row.ws_claimant_source_sequence_epoch_id.as_deref(),
        Some("source-epoch-1")
    );
    assert_eq!(row.ws_claimant_collector_admission_sequence, Some(41));
    assert!(row.validate().is_complete());

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
        write_polymarket_metadata_event(&mut writer, &event, logical_bytes).unwrap(),
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

    let admitted =
        target_admitted_polymarket_message(&message, (&discovery).into(), &asset_to_slug)
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
        target_admitted_polymarket_message(&unmatched, (&discovery).into(), &asset_to_slug)
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
        target_admitted_polymarket_message(&contradictory, (&discovery).into(), &asset_to_slug,)
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
        let admitted =
            target_admitted_polymarket_message(&parsed[0], (&discovery).into(), &asset_to_slug)
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
    };
    let asset_to_slug = discovery_asset_to_slug(&discovery);
    let assert_error = |message: WsMessage| {
        let error =
            target_admitted_polymarket_message(&message, (&discovery).into(), &asset_to_slug)
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

#[test]
fn metadata_v5_writer_and_routed_entries_keep_error_precedence() {
    let discovery = test_btc_discovery();
    let row = polymarket_metadata_snapshot_row(
        &discovery.current,
        MarketRuntimeRole::Current,
        &CollectorIdentity {
            host_id: "collector-test".into(),
            region: "local-test".into(),
        },
    )
    .unwrap();
    let mut event =
        polymarket_metadata_raw_event(PolymarketMarketMetadataV5Row::from_common(row, None, None))
            .unwrap();
    event.raw_json = "{".into();
    assert!(
        polymarket_metadata_row_from_event(&event)
            .unwrap_err()
            .to_string()
            .contains("missing collector admission")
    );
    assert!(
        !polymarket_metadata_v6_row_for_event(&event)
            .unwrap_err()
            .to_string()
            .contains("missing collector admission")
    );
    for version in [
        "polymarket_market_metadata_v5",
        "polymarket_market_metadata_v6",
    ] {
        event.raw_json = serde_json::json!({"schema_version":version}).to_string();
        assert!(
            polymarket_metadata_v6_row_for_event(&event)
                .unwrap_err()
                .to_string()
                .contains("missing collector admission")
        );
        event.collector_admission = Some(RawEventAdmission {
            sequence: 1,
            time_utc_ms: 1,
        });
        assert!(
            polymarket_metadata_v6_row_for_event(&event).is_err(),
            "缺失必需字段不能因复用而接受"
        );
        event.collector_admission = None;
    }
}

#[test]
fn reversed_gamma_preserves_fixed_identity_and_raw_evidence() {
    use crate::market_discovery::{BtcUpdownMarketCandidate, select_btc_updown_markets};
    let seed = test_btc_discovery().current;
    let mut raw: serde_json::Value = serde_json::from_str(seed.raw_json.as_ref().unwrap()).unwrap();
    raw["id"] = serde_json::json!("3195576");
    raw["orderPriceMinTickSize"] = serde_json::json!("0.01");
    raw["outcomes"] = serde_json::json!("[\"Down\",\"Up\"]");
    raw["outcomePrices"] = serde_json::json!("[\"0.4\",\"0.6\"]");
    raw["clobTokenIds"] = serde_json::json!("[\"102\",\"101\"]");
    let sdk: SdkGammaMarket = serde_json::from_value(raw).unwrap();
    let mut candidate = BtcUpdownMarketCandidate::from_sdk_market(&sdk);
    candidate.metadata_fetch_start_utc_ms = Some(seed.start_ts_s * 1000 + 1);
    candidate.metadata_received_utc_ms = Some(seed.start_ts_s * 1000 + 2);
    let discovered =
        select_btc_updown_markets(seed.start_ts_s * 1000 + 3, &[candidate], &[]).unwrap();
    assert_eq!(discovered.current.up_asset_id, "101");
    assert_eq!(discovered.current.down_asset_id, "102");
    let row = polymarket_metadata_snapshot_row(
        &discovered.current,
        MarketRuntimeRole::Current,
        &CollectorIdentity {
            host_id: "semantic-collector".into(),
            region: "local".into(),
        },
    )
    .unwrap();
    use data_pipeline_core::public_input::*;
    use data_pipeline_core::public_observation_core::PublicObservationCoreV1;
    let admission_time = row.fixed_identity_verified_at_utc + 1;
    let wrapped =
        crate::polymarket_market_metadata_v6_layout::PolymarketMarketMetadataV6Row::Existing(
            PolymarketMarketMetadataV5Row::from_common(row, None, None),
        );
    let mut event = polymarket_metadata_v6_raw_event(wrapped).unwrap();
    event.collector_admission = Some(RawEventAdmission {
        sequence: 1,
        time_utc_ms: admission_time,
    });
    use crate::polymarket_market_metadata_v6_layout::{
        polymarket_market_metadata_v6_row_from_batch, polymarket_market_metadata_v6_rows_to_batch,
        polymarket_market_metadata_v6_schema,
    };
    let persisted = polymarket_metadata_v6_row_for_event(&event).unwrap();
    let source_payload = persisted
        .existing()
        .unwrap()
        .common()
        .source_evidence_payload_json
        .clone();
    let batch = polymarket_market_metadata_v6_rows_to_batch(
        polymarket_market_metadata_v6_schema(),
        &[persisted],
    )
    .unwrap();
    let recovered = polymarket_market_metadata_v6_row_from_batch(&batch, 0).unwrap();
    assert_eq!(
        recovered
            .existing()
            .unwrap()
            .common()
            .source_evidence_payload_json,
        source_payload
    );
    assert_eq!(
        recovered
            .existing()
            .unwrap()
            .common()
            .outcomes_raw
            .as_deref(),
        Some(r#"["Down","Up"]"#)
    );
    assert_eq!(
        recovered
            .existing()
            .unwrap()
            .common()
            .outcome_prices_raw
            .as_deref(),
        Some(r#"["0.4","0.6"]"#)
    );
    let admission = event.collector_admission.clone();
    event = polymarket_metadata_v6_raw_event(recovered).unwrap();
    event.collector_admission = admission;
    let row = raw_event_to_public_input(&event).unwrap().unwrap();
    let TypedRawRowV2::PolymarketMarketMetadataV6(metadata) = &row else {
        panic!("snapshot must retain its V6 envelope")
    };
    let PolymarketMarketMetadataV6Input::Existing(metadata) = metadata.as_ref() else {
        panic!("snapshot must retain the metadata row")
    };
    let outcomes = metadata.outcome_evidence().unwrap();
    assert_eq!(outcomes[0].asset_id, "101");
    assert_eq!(outcomes[0].outcome.as_deref(), Some("Up"));
    assert_eq!(outcomes[0].price.as_ref().unwrap().exact.0, "0.6");
    let old_evidence = data_pipeline_core::source_evidence_from_typed_raw(
        &data_pipeline_core::typed_raw::TypedRawRow::PolymarketMarketMetadata(Box::new(
            metadata.clone(),
        )),
        &serde_json::from_value(serde_json::json!("a".repeat(64))).unwrap(),
        &serde_json::from_value(serde_json::json!("b".repeat(64))).unwrap(),
    )
    .unwrap();
    let (_, context, _) =
        data_pipeline_core::reduction::PolymarketMetadataReducer::from_identity_snapshot(
            data_pipeline_core::reduction::PolymarketMetadataInputV1 {
                row: metadata.clone(),
                evidence: old_evidence,
            },
            "rule-v1",
        )
        .unwrap();
    assert_eq!(
        context
            .assets
            .iter()
            .find(|a| a.asset.token_id.0 == "101")
            .unwrap()
            .asset
            .outcome
            .label,
        "Up"
    );
    let source_evidence =
        data_pipeline_core::public_reference::source_evidence_v3_from_typed_raw_v2(&row).unwrap();
    let mut core = PublicObservationCoreV1::new(
        serde_json::from_value(serde_json::json!({
            "input_stream_id":"semantic-collector", "canonical_rule_version":"rule-v1",
            "observation_version":"public-observation-v1", "ordering_version":"original-input-v1",
            "input_scope":"constructed-finite", "max_pending_ws_terminals":2,
            "initial_state":"Empty"
        }))
        .unwrap(),
    )
    .unwrap();
    let discovery_row = row.clone();
    let input = RecordedIngressV1 {
        input_stream_id: "semantic-collector".into(),
        input_position: 0,
        ingress: DptIngressV1::Message {
            row,
            source_evidence,
        },
        dependency_record_refs: vec![],
    };
    let prepared = prepare_public_ingress_v1(&core, &input, &Default::default(), &[]).unwrap();
    let applied = core.push(&prepared).unwrap();
    let obs = &applied.applied.closed_groups.last().unwrap().observations[0];
    let up = obs
        .assets
        .iter()
        .find(|a| a.asset.token_id.0 == "101")
        .unwrap();
    assert_eq!(up.asset.outcome.label, "Up");
    assert_eq!(up.asset.outcome.index, 0);
    // 后续终态仍从同一SDK响应的token顺序推导winner，不能在DPT之后手工修正标签。
    let identity = PendingPolymarketIdentity::from_discovered(&discovered.current).unwrap();
    let mut pending = PendingPolymarketResolution::new(identity.clone(), None);
    pending.in_flight_ordinal = Some(1);
    let scheduled = pending.slot_utc_ms(1);
    let mut terminal_payload = serde_json::to_value(&sdk).unwrap();
    terminal_payload["closed"] = serde_json::json!(true);
    terminal_payload["acceptingOrders"] = serde_json::json!(false);
    terminal_payload["automaticallyResolved"] = serde_json::json!(true);
    terminal_payload["umaResolutionStatus"] = serde_json::json!("resolved");
    terminal_payload["closedTime"] = serde_json::json!(
        ({
            let value =
                OffsetDateTime::from_unix_timestamp(identity.market_end_utc_ms / 1000).unwrap();
            format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                value.year(),
                value.month() as u8,
                value.day(),
                value.hour(),
                value.minute(),
                value.second()
            )
        })
    );
    terminal_payload["outcomePrices"] = serde_json::json!("[\"0\",\"1\"]");
    let mut terminal = polymarket_resolution_attempt_row(
        &pending,
        &GammaResolutionPollCompletion {
            slug: identity.slug.clone(),
            ordinal: 1,
            scheduled_at_utc_ms: scheduled,
            fetch_start_utc_ms: scheduled,
            completed_at_utc_ms: scheduled + 10,
            result: Ok(serde_json::from_value(terminal_payload).unwrap()),
            raw_day_ticket: RawDayTicketAcceptance::Accepted(
                accepted_raw_day_ticket_for_sync_test(scheduled + 10),
            ),
        },
        &|| scheduled + 11,
        false,
        &CollectorIdentity {
            host_id: "semantic-collector".into(),
            region: "local".into(),
        },
    )
    .unwrap();
    assert_eq!(terminal.winning_asset_id.as_deref(), Some("101"));
    complete_polymarket_terminal_transition(&mut terminal, scheduled + 12);
    let terminal =
        crate::polymarket_market_metadata_v6_layout::PolymarketMarketMetadataV6Row::Existing(
            PolymarketMarketMetadataV5Row::from_common(terminal, None, None),
        );
    let mut terminal_event = polymarket_metadata_v6_raw_event(terminal).unwrap();
    terminal_event.collector_admission = Some(RawEventAdmission {
        sequence: 2,
        time_utc_ms: scheduled + 13,
    });
    let terminal_row = raw_event_to_public_input(&terminal_event).unwrap().unwrap();
    let source_evidence =
        data_pipeline_core::public_reference::source_evidence_v3_from_typed_raw_v2(&terminal_row)
            .unwrap();
    let input = RecordedIngressV1 {
        input_stream_id: "semantic-collector".into(),
        input_position: 1,
        ingress: DptIngressV1::Message {
            row: terminal_row.clone(),
            source_evidence,
        },
        dependency_record_refs: vec![],
    };
    let prepared = prepare_public_ingress_v1(&core, &input, &Default::default(), &[]).unwrap();
    let applied = core.push(&prepared).unwrap();
    let obs = &applied.applied.closed_groups.last().unwrap().observations[0];
    let encoded = serde_json::to_value(obs.terminal.value.as_ref().unwrap()).unwrap();
    assert_eq!(encoded["winning_asset_id"], "101");
    assert_eq!(encoded["winning_outcome"], "Up");
    // 同一正式SDK入口的采集产物同时供原生A/B及DPT持久回放使用，不另拼正确的后端值。
    let fixture: Vec<TypedRawRowV2> = serde_json::from_str(include_str!(
        "../../../../data-pipeline/core/tests/fixtures/gamma-reversed-sdk-metadata-v6.json"
    ))
    .unwrap();
    assert_eq!(vec![discovery_row, terminal_row], fixture);
}
