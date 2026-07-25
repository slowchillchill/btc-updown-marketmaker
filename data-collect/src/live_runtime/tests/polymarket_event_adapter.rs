use super::*;
use crate::polymarket_market_metadata_layout::{
    POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION, PolymarketMarketMetadataV4Row,
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
fn polymarket_metadata_v4_nonclaimant_raw_event_round_trips_and_writes() {
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
    let expected = PolymarketMarketMetadataV4Row::from_v3_prefix(prefix, None, None);
    let mut event = polymarket_metadata_v4_raw_event(expected.clone()).unwrap();
    let draft = serde_json::from_str::<Value>(&event.raw_json).unwrap();
    assert_eq!(
        draft["schema_version"],
        POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION
    );
    assert!(draft["ws_claimant_source_sequence_epoch_id"].is_null());
    assert!(draft["ws_claimant_collector_admission_sequence"].is_null());
    assert!(draft.get("collector_admission_sequence").is_none());
    assert!(draft.get("collector_admission_time_utc_ms").is_none());

    event.collector_admission = Some(RawEventAdmission {
        sequence: 1,
        time_utc_ms: admission_time_utc_ms,
    });
    let rows = polymarket_metadata_v4_rows_from_event(&event).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].v3_prefix.collector_admission_sequence, 1);
    assert_eq!(
        rows[0].v3_prefix.collector_admission_time_utc_ms,
        admission_time_utc_ms
    );
    assert!(rows[0].validate().is_complete());

    let tmp = tempdir().unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "metadata-v4-nonclaimant",
        POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION,
        "polymarket_market_metadata",
    )
    .unwrap();
    let logical_bytes = u64::try_from(owned_event_bytes(&event)).unwrap();
    assert_eq!(
        write_polymarket_metadata_v4_live_event(&mut writer, &event, logical_bytes).unwrap(),
        1
    );
    assert_eq!(writer.finalize().unwrap().row_count, 1);
}

#[test]
fn polymarket_metadata_v4_ws_claimant_raw_event_round_trips_and_writes() {
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
    let expected = PolymarketMarketMetadataV4Row::from_v3_prefix(
        prefix,
        Some("source-epoch-1".to_owned()),
        Some(41),
    );
    let mut event = polymarket_metadata_v4_raw_event(expected).unwrap();
    let draft = serde_json::from_str::<Value>(&event.raw_json).unwrap();
    assert_eq!(
        draft["ws_claimant_source_sequence_epoch_id"],
        "source-epoch-1"
    );
    assert_eq!(draft["ws_claimant_collector_admission_sequence"], 41);

    event.collector_admission = Some(RawEventAdmission {
        sequence: 2,
        time_utc_ms: applied_at_utc_ms + 1,
    });
    let rows = polymarket_metadata_v4_rows_from_event(&event).unwrap();
    assert_eq!(
        rows[0].ws_claimant_source_sequence_epoch_id.as_deref(),
        Some("source-epoch-1")
    );
    assert_eq!(rows[0].ws_claimant_collector_admission_sequence, Some(41));
    assert!(rows[0].validate().is_complete());

    let tmp = tempdir().unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "metadata-v4-claimant",
        POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION,
        "polymarket_market_metadata",
    )
    .unwrap();
    let logical_bytes = u64::try_from(owned_event_bytes(&event)).unwrap();
    assert_eq!(
        write_polymarket_metadata_v4_live_event(&mut writer, &event, logical_bytes).unwrap(),
        1
    );
    assert_eq!(writer.finalize().unwrap().row_count, 1);
}

#[test]
fn polymarket_book_raw_event_keeps_full_depth_not_lengths() {
    let WsMessage::Book(book) = serde_json::from_str(
        r#"{
            "event_type": "book",
            "asset_id": "106585164761922456203746651621390029417453862034640469075081961934906147433548",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
            "timestamp": "1783392404599",
            "bids": [{"price": "0.49", "size": "120.5"}],
            "asks": [{"price": "0.51", "size": "88"}],
            "last_trade_price": "0.50",
            "hash": "book-hash"
        }"#,
    )
    .unwrap()
    else {
        panic!("expected book message");
    };

    let canonical = polymarket_source_native_payload_json(&WsMessage::Book(book.clone()))
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&canonical).unwrap()["last_trade_price"],
        "0.50"
    );

    let raw = polymarket_book_raw_json(&book, "btc-updown-5m-1783392300");

    assert_eq!(raw["event_type"], "book");
    assert_eq!(raw["bids"][0][0], "0.49");
    assert_eq!(raw["bids"][0][1], "120.5");
    assert_eq!(raw["asks"][0][0], "0.51");
    assert_eq!(raw["asks"][0][1], "88");
    assert_eq!(raw["last_trade_price"], "0.50");
    assert!(raw.get("bids_len").is_none());
    assert!(raw.get("asks_len").is_none());
}

#[test]
fn polymarket_book_canonical_omits_missing_optional_last_trade_price() {
    let message = serde_json::from_str::<WsMessage>(
        r#"{
            "event_type": "book",
            "asset_id": "106585164761922456203746651621390029417453862034640469075081961934906147433548",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000001",
            "timestamp": "1783392404599",
            "bids": [],
            "asks": []
        }"#,
    )
    .unwrap();

    let canonical = polymarket_source_native_payload_json(&message)
        .unwrap()
        .unwrap();
    assert!(!canonical.contains("last_trade_price"));
}

#[test]
fn polymarket_mixed_price_change_keeps_only_target_entries() {
    let target = "101";
    let external = "999";
    let WsMessage::PriceChange(mut event) = serde_json::from_str(&format!(
        r#"{{
            "event_type": "price_change",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000002",
            "timestamp": "1783392404600",
            "price_changes": [
                {{"asset_id":"{target}","price":"0.52","size":"10","side":"BUY"}},
                {{"asset_id":"{external}","price":"0.48","size":"8","side":"SELL"}}
            ]
        }}"#
    ))
    .unwrap() else {
        panic!("expected price_change message");
    };
    let asset_to_slug =
        BTreeMap::from([(target.to_owned(), "btc-updown-5m-1783392300".to_owned())]);

    let matched = retain_target_price_changes(&mut event, &asset_to_slug);
    let raw = polymarket_price_change_raw_json(&event, &asset_to_slug);

    assert_eq!(matched, BTreeSet::from([target.to_owned()]));
    assert_eq!(event.price_changes.len(), 1);
    assert_eq!(event.price_changes[0].asset_id.to_string(), target);
    assert_eq!(raw["price_changes"].as_array().unwrap().len(), 1);
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
fn synthetic_discovery_produces_no_market_rows() {
    let synthetic = RawEvent {
        source_kind: "polymarket_market".to_owned(),
        source_event_type: "market_discovery_switch".to_owned(),
        source_identifier: "current:next".to_owned(),
        source_ts: Some("1783392404600".to_owned()),
        recv_time_utc_ms: 1_783_392_404_600,
        collector_host_id: "collector-test".to_owned(),
        collector_region: "local-test".to_owned(),
        source_endpoint: "https://gamma-api.polymarket.com/markets".to_owned(),
        raw_json: json!({
            "event_type": "market_discovery_switch",
            "current": {"condition_id":"0x01","up_asset_id":"101","down_asset_id":"102"},
            "next": {"condition_id":"0x02","up_asset_id":"201","down_asset_id":"202"}
        })
        .to_string(),
        collector_admission: None,
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: "polymarket_market".to_owned(),
    };
    assert!(
        polymarket_orderbook_rows_from_event(&synthetic)
            .unwrap()
            .is_empty()
    );

    let native = RawEvent {
        source_event_type: "new_market".to_owned(),
        source_identifier: "native".to_owned(),
        raw_json: json!({
            "event_type":"new_market",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "asset_ids":["101","102"],
            "timestamp":1_783_392_404_600_i64
        })
        .to_string(),
        ..synthetic
    };
    let rows = polymarket_orderbook_rows_from_event(&native).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].event_type, "new_market");
}

#[test]
fn polymarket_writer_rejects_unsplit_fanout_before_writing() {
    let tmp = tempdir().unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "polymarket-fanout",
        POLYMARKET_ORDERBOOK_SCHEMA_VERSION,
        "polymarket_market",
    )
    .unwrap();
    let event = test_polymarket_price_change_event(1_783_392_404_600);
    let logical_bytes = u64::try_from(owned_event_bytes(&event)).unwrap();

    let error =
        write_polymarket_orderbook_live_event(&mut writer, &event, logical_bytes).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("typed event fan-out must be split before ingress"),
        "{error}"
    );

    let published = writer.finalize().unwrap();
    assert_eq!(published.row_count, 0);
}

#[test]
fn polymarket_dispatch_writes_only_matching_native_business_events() {
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
    let discovery = test_btc_discovery();
    let asset_to_slug = discovery_asset_to_slug(&discovery);

    let WsMessage::PriceChange(mixed) = serde_json::from_str(
        r#"{
            "event_type":"price_change",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "timestamp":"1783392404600",
            "price_changes":[
                {"asset_id":"101","price":"0.52","size":"10","side":"BUY"},
                {"asset_id":"999","price":"0.48","size":"8","side":"SELL"}
            ]
        }"#,
    )
    .unwrap() else {
        panic!("expected price_change message");
    };
    let (outcome, written) = build_polymarket_events(
        WsMessage::PriceChange(mixed),
        &discovery,
        &asset_to_slug,
        &identity,
        1_783_392_404_599,
        r#"{"source":"test"}"#,
    )
    .unwrap();
    assert_eq!(outcome.matched_assets, BTreeSet::from(["101".to_owned()]));
    assert_eq!(written.len(), 1);
    let written = &written[0];
    assert_eq!(written.recv_time_utc_ms, 1_783_392_404_599);
    let written_json: Value = serde_json::from_str(&written.raw_json).unwrap();
    assert_eq!(written_json["price_changes"].as_array().unwrap().len(), 1);
    assert_eq!(written_json["price_changes"][0]["asset_id"], "101");

    let WsMessage::NewMarket(unmatched_new) = serde_json::from_str(
        r#"{
            "id":"market-unmatched",
            "question":"Question",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000009",
            "slug":"unmatched",
            "description":"Description",
            "assets_ids":["999","1000"],
            "outcomes":["Up","Down"],
            "timestamp":"1783392404601",
            "event_type":"new_market"
        }"#,
    )
    .unwrap() else {
        panic!("expected new_market message");
    };
    let (outcome, written) = build_polymarket_events(
        WsMessage::NewMarket(unmatched_new),
        &discovery,
        &asset_to_slug,
        &identity,
        1_783_392_404_599,
        r#"{"source":"test"}"#,
    )
    .unwrap();
    assert!(outcome.matched_assets.is_empty());
    assert!(written.is_empty());

    let WsMessage::MarketResolved(unmatched_resolved) = serde_json::from_str(
        r#"{
            "id":"market-unmatched",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000009",
            "assets_ids":["999","1000"],
            "winning_asset_id":"999",
            "winning_outcome":"Up",
            "timestamp":"1783392404602",
            "event_type":"market_resolved"
        }"#,
    )
    .unwrap() else {
        panic!("expected market_resolved message");
    };
    let error = build_polymarket_events(
        WsMessage::MarketResolved(unmatched_resolved),
        &discovery,
        &asset_to_slug,
        &identity,
        1_783_392_404_599,
        r#"{"source":"test"}"#,
    )
    .err()
    .expect("generic dispatch must reject market_resolved");
    assert!(
        error
            .to_string()
            .contains("market_resolved must use the validate-first typed terminal handler")
    );

    let WsMessage::NewMarket(matching_new) = serde_json::from_str(
        r#"{
            "id":"market-current",
            "question":"Bitcoin Up or Down",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "slug":"btc-updown-5m-1783392300",
            "description":"Description",
            "assets_ids":["101","102"],
            "outcomes":["Up","Down"],
            "timestamp":"1783392404603",
            "event_type":"new_market"
        }"#,
    )
    .unwrap() else {
        panic!("expected new_market message");
    };
    let (outcome, written) = build_polymarket_events(
        WsMessage::NewMarket(matching_new),
        &discovery,
        &asset_to_slug,
        &identity,
        1_783_392_404_599,
        r#"{"source":"test"}"#,
    )
    .unwrap();
    assert_eq!(
        outcome.matched_assets,
        BTreeSet::from(["101".to_owned(), "102".to_owned()])
    );
    assert_eq!(written.len(), 1);
    assert_eq!(
        written
            .iter()
            .filter(|event| {
                event.source_kind == "polymarket_market" && event.source_event_type == "new_market"
            })
            .count(),
        1
    );
    assert!(
        written
            .iter()
            .all(|event| event.source_kind == "polymarket_market"),
        "new_market is raw transport evidence only under metadata v3"
    );
}

#[test]
fn polymarket_business_assets_must_match_their_message_condition() {
    let identity = CollectorIdentity {
        host_id: "collector-test".to_owned(),
        region: "local-test".to_owned(),
    };
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
        let error = build_polymarket_events(
            message,
            &discovery,
            &asset_to_slug,
            &identity,
            1_783_392_404_599,
            r#"{"source":"test"}"#,
        )
        .err()
        .expect("a target asset under the wrong condition must be terminal");
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
fn polymarket_source_native_canonical_covers_fields_lost_by_the_old_layout() {
    let message = serde_json::from_str::<WsMessage>(
        r#"{
            "event_type":"best_bid_ask",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "asset_id":"101",
            "best_bid":"0.49",
            "best_ask":"0.51",
            "spread":"0.02",
            "timestamp":"1783392404600"
        }"#,
    )
    .unwrap();
    let changed_spread = serde_json::from_str::<WsMessage>(
        r#"{
            "event_type":"best_bid_ask",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "asset_id":"101",
            "best_bid":"0.49",
            "best_ask":"0.51",
            "spread":"0.020",
            "timestamp":"1783392404600"
        }"#,
    )
    .unwrap();
    let first = polymarket_source_native_payload_json(&message)
        .unwrap()
        .unwrap();
    let second = polymarket_source_native_payload_json(&changed_spread)
        .unwrap()
        .unwrap();
    assert_ne!(first, second);
    assert!(!first.contains("btc-updown-5m"));

    let new_market_a = serde_json::from_str::<WsMessage>(
        r#"{
            "id":"market-native",
            "question":"Bitcoin Up or Down",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "slug":"btc-updown-5m-1783392300",
            "description":"description-a",
            "assets_ids":["101","102"],
            "outcomes":["Up","Down"],
            "event_message":{"id":"1","ticker":"btc","slug":"btc","title":"title","description":"event-a"},
            "timestamp":"1783392404600",
            "event_type":"new_market"
        }"#,
    )
    .unwrap();
    let new_market_b = serde_json::from_str::<WsMessage>(
        r#"{
            "id":"market-native",
            "question":"Bitcoin Up or Down",
            "market":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "slug":"btc-updown-5m-1783392300",
            "description":"description-b",
            "assets_ids":["101","102"],
            "outcomes":["Up","Down"],
            "event_message":{"id":"1","ticker":"btc","slug":"btc","title":"title","description":"event-b"},
            "timestamp":"1783392404600",
            "event_type":"new_market"
        }"#,
    )
    .unwrap();
    assert_ne!(
        polymarket_source_native_payload_json(&new_market_a)
            .unwrap()
            .unwrap(),
        polymarket_source_native_payload_json(&new_market_b)
            .unwrap()
            .unwrap()
    );
}

#[test]
fn polymarket_source_native_canonical_covers_every_market_variant() {
    let cases = [
        (
            "book",
            r#"{"event_type":"book","market":"0x0000000000000000000000000000000000000000000000000000000000000002","asset_id":"101","timestamp":"1783392404600","bids":[{"price":"0.49","size":"10"}],"asks":[{"price":"0.51","size":"8"}],"hash":"h"}"#,
        ),
        (
            "price_change",
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000002","timestamp":"1783392404600","price_changes":[{"asset_id":"101","price":"0.50","size":"10","side":"BUY","hash":"h","best_bid":"0.49","best_ask":"0.51"}]}"#,
        ),
        (
            "tick_size_change",
            r#"{"event_type":"tick_size_change","market":"0x0000000000000000000000000000000000000000000000000000000000000002","asset_id":"101","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"1783392404600"}"#,
        ),
        (
            "last_trade_price",
            r#"{"event_type":"last_trade_price","market":"0x0000000000000000000000000000000000000000000000000000000000000002","asset_id":"101","price":"0.50","side":"BUY","size":"10","fee_rate_bps":"0","timestamp":"1783392404600"}"#,
        ),
        (
            "best_bid_ask",
            r#"{"event_type":"best_bid_ask","market":"0x0000000000000000000000000000000000000000000000000000000000000002","asset_id":"101","best_bid":"0.49","best_ask":"0.51","spread":"0.02","timestamp":"1783392404600"}"#,
        ),
        (
            "new_market",
            r#"{"event_type":"new_market","id":"m","question":"q","market":"0x0000000000000000000000000000000000000000000000000000000000000002","slug":"native-slug","description":"d","assets_ids":["101","102"],"outcomes":["Up","Down"],"timestamp":"1783392404600"}"#,
        ),
        (
            "market_resolved",
            r#"{"event_type":"market_resolved","id":"m","question":"q","market":"0x0000000000000000000000000000000000000000000000000000000000000002","slug":"native-slug","description":"d","assets_ids":["101","102"],"outcomes":["Up","Down"],"winning_asset_id":"101","winning_outcome":"Up","timestamp":"1783392404600"}"#,
        ),
    ];

    for (event_type, raw) in cases {
        let message = serde_json::from_str::<WsMessage>(raw).unwrap();
        let canonical = prepare_polymarket_message(&message).unwrap().unwrap();
        let value = serde_json::from_str::<Value>(&canonical).unwrap();
        assert_eq!(value["source"], "polymarket_market");
        assert_eq!(value["event_type"], event_type);
    }
}

#[test]
fn polymarket_current_v2_rejects_sdk_nullable_shapes_before_canonicalization() {
    let cases: [(&str, &[&str], &str); 7] = [
        (
            "book",
            &["timestamp"],
            r#"{"event_type":"book","market":"0x0000000000000000000000000000000000000000000000000000000000000002","asset_id":"101","timestamp":null,"bids":[],"asks":[]}"#,
        ),
        (
            "price_change",
            &["timestamp"],
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000002","price_changes":[{"asset_id":"101","price":"0.50","size":"10","side":"BUY"}]}"#,
        ),
        (
            "last_trade_price",
            &["timestamp"],
            r#"{"event_type":"last_trade_price","market":"0x0000000000000000000000000000000000000000000000000000000000000002","asset_id":"101","price":"0.50","timestamp":null}"#,
        ),
        (
            "tick_size_change",
            &["old_tick_size", "timestamp"],
            r#"{"event_type":"tick_size_change","market":"0x0000000000000000000000000000000000000000000000000000000000000002","asset_id":"101","old_tick_size":null,"new_tick_size":"0.001","timestamp":null}"#,
        ),
        (
            "best_bid_ask",
            &["best_bid", "best_ask", "spread", "timestamp"],
            r#"{"event_type":"best_bid_ask","market":"0x0000000000000000000000000000000000000000000000000000000000000002","asset_id":"101","best_bid":null,"best_ask":null,"spread":null,"timestamp":null}"#,
        ),
        (
            "new_market",
            &[
                "question",
                "slug",
                "description",
                "asset_ids",
                "outcomes",
                "timestamp",
                "event_message.ticker",
                "event_message.slug",
                "event_message.title",
                "event_message.description",
            ],
            r#"{"event_type":"new_market","id":"m","question":null,"market":"0x0000000000000000000000000000000000000000000000000000000000000002","slug":null,"description":null,"assets_ids":null,"outcomes":null,"event_message":{"id":"1"},"timestamp":null}"#,
        ),
        (
            "market_resolved",
            &[
                "asset_ids",
                "winning_asset_id",
                "winning_outcome",
                "timestamp",
                "event_message.ticker",
                "event_message.slug",
                "event_message.title",
                "event_message.description",
            ],
            r#"{"event_type":"market_resolved","id":"m","question":null,"market":"0x0000000000000000000000000000000000000000000000000000000000000002","slug":null,"description":null,"assets_ids":null,"outcomes":null,"winning_asset_id":null,"winning_outcome":null,"event_message":{"id":"1"},"timestamp":null}"#,
        ),
    ];

    for (event_type, missing_fields, raw) in cases {
        let message = serde_json::from_str::<WsMessage>(raw)
            .unwrap_or_else(|error| panic!("{event_type} must decode through the SDK: {error}"));
        let error = prepare_polymarket_message(&message)
            .expect_err("current v2 must reject the newly nullable SDK shape");
        let error = error.to_string();
        let expected_prefix =
            "polymarket_current_v2_typed_shape_missing_required_fields:event_type=";
        assert!(
            error.starts_with(&format!("{expected_prefix}{event_type}:fields=")),
            "{error}"
        );
        for missing_field in missing_fields {
            assert!(
                error.split_once(":fields=").is_some_and(|(_, fields)| {
                    fields.split(',').any(|field| field == *missing_field)
                }),
                "{event_type} must reject missing {missing_field}: {error}"
            );
        }
        assert!(
            error.ends_with(&format!("fields={}", missing_fields.join(","))),
            "{event_type} must reject exactly the current-v2-required fields: {error}"
        );
    }
}

#[test]
fn polymarket_target_raw_events_keep_event_specific_fields() {
    let asset_id = "106585164761922456203746651621390029417453862034640469075081961934906147433548";
    let mut asset_to_slug = BTreeMap::new();
    asset_to_slug.insert(asset_id.to_owned(), "btc-updown-5m-1783392300".to_owned());

    let WsMessage::PriceChange(price_change) = serde_json::from_str(&format!(
        r#"{{
            "event_type": "price_change",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000002",
            "timestamp": "1783392404600",
            "price_changes": [{{
                "asset_id": "{asset_id}",
                "price": "0.52",
                "size": "10",
                "side": "BUY",
                "hash": "price-hash",
                "best_bid": "0.51",
                "best_ask": "0.53"
            }}]
        }}"#
    ))
    .unwrap() else {
        panic!("expected price_change message");
    };
    let raw = polymarket_price_change_raw_json(&price_change, &asset_to_slug);
    assert_eq!(raw["event_type"], "price_change");
    assert_eq!(raw["price_changes"][0]["price"], "0.52");
    assert_eq!(raw["price_changes"][0]["best_bid"], "0.51");
    assert_eq!(raw["price_changes"][0]["slug"], "btc-updown-5m-1783392300");

    let WsMessage::LastTradePrice(last_trade) = serde_json::from_str(&format!(
        r#"{{
            "asset_id": "{asset_id}",
            "event_type": "last_trade_price",
            "fee_rate_bps": "0",
            "market": "0x6a67b9d828d53862160e470329ffea5246f338ecfffdf2cab45211ec578b0347",
            "price": "0.456",
            "side": "BUY",
            "size": "219.217767",
            "timestamp": "1750428146322"
        }}"#
    ))
    .unwrap() else {
        panic!("expected last_trade_price message");
    };
    let raw = polymarket_last_trade_raw_json(&last_trade, "btc-updown-5m-1783392300");
    assert_eq!(raw["event_type"], "last_trade_price");
    assert_eq!(raw["price"], "0.456");
    assert_eq!(raw["size"], "219.217767");
    assert_eq!(raw["fee_rate_bps"], "0");

    let WsMessage::TickSizeChange(tick_size) = serde_json::from_str(&format!(
        r#"{{
            "event_type": "tick_size_change",
            "asset_id": "{asset_id}",
            "market": "0x6a67b9d828d53862160e470329ffea5246f338ecfffdf2cab45211ec578b0347",
            "old_tick_size": "0.01",
            "new_tick_size": "0.001",
            "timestamp": "1750428146323"
        }}"#
    ))
    .unwrap() else {
        panic!("expected tick_size_change message");
    };
    let raw = polymarket_tick_size_change_raw_json(&tick_size, "btc-updown-5m-1783392300");
    assert_eq!(raw["event_type"], "tick_size_change");
    assert_eq!(raw["old_tick_size"], "0.01");
    assert_eq!(raw["new_tick_size"], "0.001");

    let WsMessage::NewMarket(new_market) = serde_json::from_str(
        r#"{
            "id": "1031769",
            "question": "Will NVIDIA (NVDA) close above $240 end of January?",
            "market": "0x311d0c4b6671ab54af4970c06fcf58662516f5168997bdda209ec3db5aa6b0c1",
            "slug": "nvda-above-240-on-january-30-2026",
            "description": "This market will resolve to Yes or No.",
            "assets_ids": [
                "76043073756653678226373981964075571318267289248134717369284518995922789326425",
                "31690934263385727664202099278545688007799199447969475608906331829650099442770"
            ],
            "outcomes": ["Yes", "No"],
            "event_message": {
                "id": "125819",
                "ticker": "nvda-above-in-january-2026",
                "slug": "nvda-above-in-january-2026",
                "title": "Will NVIDIA (NVDA) close above ___ end of January?",
                "description": "Market description"
            },
            "timestamp": "1766790415550",
            "event_type": "new_market"
        }"#,
    )
    .unwrap() else {
        panic!("expected new_market message");
    };
    let raw = polymarket_new_market_raw_json(&new_market);
    assert_eq!(raw["event_type"], "new_market");
    assert_eq!(raw["slug"], "nvda-above-240-on-january-30-2026");
    assert_eq!(raw["asset_ids"].as_array().unwrap().len(), 2);
    assert_eq!(raw["event_message"]["ticker"], "nvda-above-in-january-2026");

    let WsMessage::MarketResolved(resolved) = serde_json::from_str(
        r#"{
            "id": "1031769",
            "question": "Will NVIDIA (NVDA) close above $240 end of January?",
            "market": "0x311d0c4b6671ab54af4970c06fcf58662516f5168997bdda209ec3db5aa6b0c1",
            "slug": "nvda-above-240-on-january-30-2026",
            "description": "This market will resolve to Yes or No.",
            "assets_ids": [
                "76043073756653678226373981964075571318267289248134717369284518995922789326425",
                "31690934263385727664202099278545688007799199447969475608906331829650099442770"
            ],
            "outcomes": ["Yes", "No"],
            "winning_asset_id": "76043073756653678226373981964075571318267289248134717369284518995922789326425",
            "winning_outcome": "Yes",
            "timestamp": "1766790415550",
            "event_type": "market_resolved"
        }"#,
    )
    .unwrap()
    else {
        panic!("expected market_resolved message");
    };
    let raw = polymarket_market_resolved_raw_json(&resolved);
    assert_eq!(raw["event_type"], "market_resolved");
    assert_eq!(raw["winning_outcome"], "Yes");
    assert_eq!(raw["resolved_event_asset_ids"].as_array().unwrap().len(), 2);
}

#[test]
fn polymarket_metadata_runtime_conversion_marks_unavailable_rows() {
    let active = test_metadata_raw_event(
        "active_scan",
        json!({
            "event_type": "active_scan",
            "slug": "btc-updown-5m-1783392300",
            "condition_id": "0x0000000000000000000000000000000000000000000000000000000000000002",
            "clob_market_id": "123",
            "asset_ids": ["1", "2"],
            "metadata_fetch_start_utc": 1_783_392_299_900_i64,
            "market_start_utc": 1_783_392_300_000_i64,
            "market_end_utc": 1_783_392_600_000_i64,
            "active": true,
            "accepting_orders": true,
            "market_lifecycle_state": "active",
            "tick_size": "0.01",
            "min_size": "1",
            "fee_schedule_raw": r#"{"fee":"0"}"#,
            "reward_eligibility_raw": r#"{"rewards_min_size":"1"}"#,
            "same_condition_market_verified": true
        }),
    );
    let rows = polymarket_metadata_rows_from_event(&active).unwrap();
    assert_eq!(rows[0].metadata_source, "active_scan");
    assert_eq!(rows[0].metadata_fetch_start_utc, Some(1_783_392_299_900));
    assert_eq!(rows[0].metadata_unavailable_reason, None);
    assert!(rows[0].validate().is_complete());

    let new_market = test_metadata_raw_event(
        "new_market",
        json!({
            "event_type": "new_market",
            "slug": "btc-updown-5m-1783392600",
            "condition_id": "0x0000000000000000000000000000000000000000000000000000000000000003",
            "asset_ids": ["3", "4"],
            "metadata_unavailable_reason": "market_ws_new_market_payload_lacks_market_start_end_active_accepting_orders_tick_size_min_size_fee_schedule_reward_eligibility",
            "same_condition_market_verified": true
        }),
    );
    let rows = polymarket_metadata_rows_from_event(&new_market).unwrap();
    assert_eq!(rows[0].metadata_source, "new_market");
    assert_eq!(rows[0].metadata_fetch_start_utc, None);
    assert!(
        rows[0]
            .metadata_unavailable_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("market_ws_new_market_payload_lacks"))
    );

    let fallback = test_metadata_raw_event(
        "fallback_close",
        json!({
            "event_type": "fallback_close",
            "slug": "btc-updown-5m-1783392300",
            "market": "0x0000000000000000000000000000000000000000000000000000000000000002",
            "asset_ids": ["1", "2"],
            "metadata_unavailable_reason": "market_resolved_ws_payload_lacks_market_start_end_tick_size_min_size_fee_schedule_reward_eligibility",
            "same_condition_market_verified": true,
            "fallback_close_reason": "market_resolved"
        }),
    );
    let rows = polymarket_metadata_rows_from_event(&fallback).unwrap();
    assert_eq!(rows[0].metadata_source, "fallback_close");
    assert_eq!(rows[0].metadata_fetch_start_utc, None);
    assert_eq!(rows[0].market_lifecycle_state, "retired");
    assert!(
        rows[0]
            .metadata_unavailable_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("market_resolved_ws_payload_lacks"))
    );

    for flag in [None, Some(false)] {
        let mut raw_json = json!({
            "event_type": "new_market",
            "slug": "btc-updown-5m-1783392600",
            "condition_id": "0x0000000000000000000000000000000000000000000000000000000000000003",
            "asset_ids": ["3", "4"],
            "metadata_unavailable_reason": "market_ws_new_market_payload_lacks_market_start_end_active_accepting_orders_tick_size_min_size_fee_schedule_reward_eligibility"
        });
        if let Some(flag) = flag {
            raw_json["same_condition_market_verified"] = Value::Bool(flag);
        }
        let error =
            polymarket_metadata_rows_from_event(&test_metadata_raw_event("new_market", raw_json))
                .expect_err("unavailable fields must not excuse an unverified target identity");
        assert!(error.to_string().contains("not verified"));
    }
}

#[test]
fn next_gamma_metadata_is_marked_prefetched() {
    let market = test_btc_discovery().current;
    assert_eq!(
        polymarket_metadata_snapshot_raw_json(&market, "current")["market_lifecycle_state"],
        "active"
    );
    assert_eq!(
        polymarket_metadata_snapshot_raw_json(&market, "next")["market_lifecycle_state"],
        "prefetched"
    );
}

#[test]
fn window_end_without_resolved_is_observation_not_retired_close() {
    let market = crate::market_discovery::DiscoveredBtcUpdownMarket {
        id: "123".to_owned(),
        slug: "btc-updown-5m-1783392300".to_owned(),
        condition_id: "0x0000000000000000000000000000000000000000000000000000000000000002"
            .to_owned(),
        up_asset_id: "1".to_owned(),
        down_asset_id: "2".to_owned(),
        start_ts_s: 1_783_392_300,
        end_ts_s: 1_783_392_600,
        candidate_start_ts_s: 1_783_392_300,
        candidate_end_ts_s: 1_783_392_600,
        active: true,
        accepting_orders: true,
        tick_size: Some("0.01".to_owned()),
        min_size: Some("1".to_owned()),
        fee_schedule_raw: Some(r#"{"fee":"0"}"#.to_owned()),
        reward_eligibility_raw: Some(r#"{"rewards_min_size":"1"}"#.to_owned()),
        raw_json_sha256: Some("raw-sha256".to_owned()),
        raw_json: Some(r#"{"id":"123"}"#.to_owned()),
        metadata_fetch_start_utc_ms: Some(1_783_392_299_900),
        metadata_received_utc_ms: Some(1_783_392_300_000),
        fixed_identity_verified_at_utc_ms: 1_783_392_300_001,
        metadata_validation_completed_at_utc_ms: 1_783_392_300_001,
    };
    let raw = polymarket_window_end_observation_raw_json_from_discovery(&market);

    assert_eq!(raw["event_type"], "active_scan");
    assert_eq!(raw["metadata_source"], "active_scan");
    assert_eq!(
        raw["market_lifecycle_state"],
        "unresolved_window_end_observation"
    );
    assert_ne!(raw["market_lifecycle_state"], "retired");
    assert!(
        raw["metadata_unavailable_reason"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason == "market_resolved_not_observed_before_window_end")
    );

    let event = test_metadata_raw_event("active_scan", raw);
    let rows = polymarket_metadata_rows_from_event(&event).unwrap();
    assert_eq!(rows[0].metadata_source, "active_scan");
    assert_eq!(
        rows[0].market_lifecycle_state,
        "unresolved_window_end_observation"
    );
    assert!(
        rows[0]
            .metadata_unavailable_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("market_resolved_not_observed"))
    );
}
