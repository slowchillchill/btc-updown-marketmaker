use super::*;
use crate::public_reference::*;
use sha2::{Digest, Sha256};
#[path = "public_observation_depth_tests.rs"]
mod depth_tests;
const CONDITION_ID: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const CLOB_MARKET_ID: &str = "3195576";
const UP_TOKEN_ID: &str = "20";
const DOWN_TOKEN_ID: &str = "3";
const SNAPSHOT_RECEIVED_AT: i64 = 10_000;
const MARKET_START_AT: i64 = 20_000;
const MARKET_END_AT: i64 = 30_000;
const TERMINAL_RECEIVED_TIME_MS: i64 = 31_001;
const TERMINAL_ADMITTED_TIME_MS: i64 = 31_003;
const TERMINAL_SOURCE_TIME_MS: i64 = 31_000;
const WS_EPOCH: &str = "epoch-a";
const WS_ADMISSION_SEQUENCE: u64 = 50;
fn market() -> MarketIdentityV2 {
    MarketIdentityV2 {
        condition_id: ConditionId(CONDITION_ID.to_owned()),
        clob_market_id: ClobMarketId(CLOB_MARKET_ID.to_owned()),
        window: MarketWindowV2 {
            start_at_utc_ms: MARKET_START_AT,
            end_at_utc_ms: MARKET_END_AT,
        },
    }
}
fn metadata_snapshot_row() -> PolymarketMarketMetadataRow {
    let payload = r#"{"orderPriceMinTickSize":"0.01"}"#.to_owned();
    let payload_sha256 = format!("{:x}", Sha256::digest(payload.as_bytes()));
    PolymarketMarketMetadataRow {
        metadata_row_kind: PolymarketMetadataRowKind::MetadataSnapshot,
        timestamp_received: SNAPSHOT_RECEIVED_AT,
        collector_admission_sequence: 1,
        collector_admission_time_utc_ms: SNAPSHOT_RECEIVED_AT + 1,
        metadata_origin: PolymarketMetadataOrigin::GammaMarketsDiscovery,
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        schema_version: POLYMARKET_MARKET_METADATA_SCHEMA.to_owned(),
        slug: "btc-updown-5m".to_owned(),
        condition_id: CONDITION_ID.to_owned(),
        clob_market_id: CLOB_MARKET_ID.to_owned(),
        asset_ids: format!(r#"["{UP_TOKEN_ID}","{DOWN_TOKEN_ID}"]"#),
        market_start_utc: MARKET_START_AT,
        market_end_utc: MARKET_END_AT,
        fixed_identity_verified_at_utc: SNAPSHOT_RECEIVED_AT - 1,
        market_lifecycle_state: MetadataMarketLifecycleState::Prefetched,
        market_runtime_role: MetadataMarketRuntimeRole::Next,
        source_endpoint: Some("https://gamma-api.polymarket.com/markets".to_owned()),
        source_endpoint_unavailable_reason: None,
        source_evidence_origin: Some(MetadataSourceEvidenceOrigin::GammaMarketsDiscovery),
        source_evidence_event_type: Some(
            MetadataSourceEvidenceEventType::GammaMarketsDiscoveryResponse,
        ),
        source_evidence_channel: None,
        source_evidence_received_at_utc: Some(SNAPSHOT_RECEIVED_AT),
        row_evidence_validation_completed_at_utc: Some(SNAPSHOT_RECEIVED_AT + 1),
        source_evidence_payload_json: Some(payload),
        source_evidence_raw_sha256: Some(payload_sha256),
        market_lifecycle_transition: None,
        lifecycle_scheduled_at_utc: None,
        lifecycle_applied_at_utc: None,
        market_runtime_role_transition: None,
        runtime_role_scheduled_at_utc: None,
        runtime_role_applied_at_utc: None,
        metadata_fetch_start_utc: Some(SNAPSHOT_RECEIVED_AT - 10),
        closed: Some(false),
        closed_time_raw: None,
        closed_time_utc: None,
        closed_time_check_failure_reason: None,
        automatically_resolved: None,
        uma_resolution_status: None,
        outcomes_raw: Some(r#"["UP","DOWN"]"#.to_owned()),
        outcome_prices_raw: Some(r#"["0.5","0.5"]"#.to_owned()),
        active: Some(true),
        active_unavailable_reason: None,
        accepting_orders: Some(true),
        ws_platform_timestamp_raw: None,
        ws_platform_timestamp_utc: None,
        winning_asset_id: None,
        winning_outcome: None,
        resolution_evidence_source: None,
        terminal_claim_owner: None,
        resolution_poll_attempt_ordinal: None,
        resolution_poll_scheduled_utc: None,
        resolution_poll_result: None,
        resolution_transport_error_class: None,
        resolution_transport_http_status: None,
        resolution_integrity_error_class: None,
        resolution_budget_horizon_utc: None,
        resolution_followup_status: None,
        resolution_poll_attempts_recorded: None,
        resolution_valid_response_count: None,
        resolution_transport_unavailable_count: None,
        resolution_missed_slot_count: None,
        last_valid_metadata_received_utc: None,
        terminal_predicate_failure_reason: None,
        response_fields_unavailable_reason: None,
        ws_claimant_source_sequence_epoch_id: None,
        ws_claimant_collector_admission_sequence: None,
    }
}
fn row(
    epoch: &str,
    sequence: u64,
    source_time: i64,
    received_time: i64,
    event: PolymarketOrderbookEvent,
) -> PolymarketOrderbookRow {
    PolymarketOrderbookRow {
        timestamp_received: received_time,
        timestamp: Some(source_time),
        market: market().condition_id.0,
        slug: Some("btc-updown-5m".to_owned()),
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
        schema_version: POLYMARKET_ORDERBOOK_SCHEMA.to_owned(),
        source_sequence_epoch_id: epoch.to_owned(),
        collector_admission_sequence: sequence,
        collector_admission_time_utc_ms: received_time + 1,
        event,
    }
}
fn book(quantity: &str) -> PolymarketOrderbookEvent {
    PolymarketOrderbookEvent::Book {
        asset_id: UP_TOKEN_ID.to_owned(),
        bids: vec![PolymarketPriceLevel {
            price: "0.5".to_owned(),
            size: quantity.to_owned(),
        }],
        asks: vec![PolymarketPriceLevel {
            price: "0.6".to_owned(),
            size: "80".to_owned(),
        }],
        last_trade_price: Some("0.55".to_owned()),
    }
}
fn change(quantity: Option<&str>) -> PolymarketPriceChangeEntry {
    PolymarketPriceChangeEntry {
        asset_id: UP_TOKEN_ID.to_owned(),
        price: "0.5".to_owned(),
        size: quantity.map(str::to_owned),
        side: PolymarketSide::Buy,
        best_bid: None,
        best_ask: None,
    }
}
fn terminal_metadata_row(market: &MarketIdentityV2) -> PolymarketMarketMetadataRow {
    PolymarketMarketMetadataRow {
        metadata_row_kind: PolymarketMetadataRowKind::ResolutionTerminalWs,
        timestamp_received: TERMINAL_RECEIVED_TIME_MS,
        collector_admission_sequence: 41,
        collector_admission_time_utc_ms: TERMINAL_ADMITTED_TIME_MS,
        metadata_origin: PolymarketMetadataOrigin::MarketResolvedWs,
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        schema_version: "polymarket_market_metadata_v5".to_owned(),
        slug: "btc-updown-5m".to_owned(),
        condition_id: market.condition_id.0.clone(),
        clob_market_id: market.clob_market_id.0.clone(),
        asset_ids: format!(r#"["{UP_TOKEN_ID}","{DOWN_TOKEN_ID}"]"#),
        market_start_utc: market.window.start_at_utc_ms,
        market_end_utc: market.window.end_at_utc_ms,
        fixed_identity_verified_at_utc: 800,
        market_lifecycle_state: MetadataMarketLifecycleState::Retired,
        market_runtime_role: MetadataMarketRuntimeRole::None,
        source_endpoint: Some("wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned()),
        source_endpoint_unavailable_reason: None,
        source_evidence_origin: Some(MetadataSourceEvidenceOrigin::MarketResolvedWs),
        source_evidence_event_type: Some(MetadataSourceEvidenceEventType::MarketResolved),
        source_evidence_channel: Some(MetadataSourceEvidenceChannel::Market),
        source_evidence_received_at_utc: Some(TERMINAL_RECEIVED_TIME_MS),
        row_evidence_validation_completed_at_utc: Some(TERMINAL_RECEIVED_TIME_MS + 1),
        source_evidence_payload_json: None,
        source_evidence_raw_sha256: Some("a".repeat(64)),
        market_lifecycle_transition: Some(MetadataMarketLifecycleTransition::ActiveToRetired),
        lifecycle_scheduled_at_utc: None,
        lifecycle_applied_at_utc: Some(TERMINAL_RECEIVED_TIME_MS + 2),
        market_runtime_role_transition: Some(
            MetadataMarketRuntimeRoleTransition::ResolutionPendingToNone,
        ),
        runtime_role_scheduled_at_utc: None,
        runtime_role_applied_at_utc: Some(TERMINAL_RECEIVED_TIME_MS + 2),
        metadata_fetch_start_utc: None,
        closed: None,
        closed_time_raw: None,
        closed_time_utc: None,
        closed_time_check_failure_reason: None,
        automatically_resolved: None,
        uma_resolution_status: None,
        outcomes_raw: None,
        outcome_prices_raw: None,
        active: None,
        active_unavailable_reason: Some(MetadataActiveUnavailableReason::NotApplicableForRowKind),
        accepting_orders: None,
        ws_platform_timestamp_raw: Some(TERMINAL_SOURCE_TIME_MS.to_string()),
        ws_platform_timestamp_utc: Some(TERMINAL_SOURCE_TIME_MS),
        winning_asset_id: Some(UP_TOKEN_ID.to_owned()),
        winning_outcome: Some("UP".to_owned()),
        resolution_evidence_source: Some(MetadataResolutionEvidenceSource::MarketResolvedWs),
        terminal_claim_owner: Some(MetadataTerminalClaimOwner::MarketResolvedWs),
        resolution_poll_attempt_ordinal: None,
        resolution_poll_scheduled_utc: None,
        resolution_poll_result: None,
        resolution_transport_error_class: None,
        resolution_transport_http_status: None,
        resolution_integrity_error_class: None,
        resolution_budget_horizon_utc: Some(market.window.end_at_utc_ms + 8 * 60_000),
        resolution_followup_status: Some(MetadataResolutionFollowupStatus::TerminalWs),
        resolution_poll_attempts_recorded: None,
        resolution_valid_response_count: None,
        resolution_transport_unavailable_count: None,
        resolution_missed_slot_count: None,
        last_valid_metadata_received_utc: None,
        terminal_predicate_failure_reason: None,
        response_fields_unavailable_reason: None,
        ws_claimant_source_sequence_epoch_id: Some(WS_EPOCH.to_owned()),
        ws_claimant_collector_admission_sequence: Some(WS_ADMISSION_SEQUENCE),
    }
}
fn terminal_orderbook_row(market: &MarketIdentityV2) -> PolymarketOrderbookRow {
    PolymarketOrderbookRow {
        timestamp_received: TERMINAL_RECEIVED_TIME_MS,
        timestamp: Some(TERMINAL_SOURCE_TIME_MS),
        market: market.condition_id.0.clone(),
        slug: Some("btc-updown-5m".to_owned()),
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
        schema_version: POLYMARKET_ORDERBOOK_SCHEMA.to_owned(),
        source_sequence_epoch_id: WS_EPOCH.to_owned(),
        collector_admission_sequence: WS_ADMISSION_SEQUENCE,
        collector_admission_time_utc_ms: TERMINAL_ADMITTED_TIME_MS,
        event: PolymarketOrderbookEvent::MarketResolved {
            clob_market_id: market.clob_market_id.0.clone(),
            question: None,
            description: None,
            asset_ids: vec![UP_TOKEN_ID.to_owned(), DOWN_TOKEN_ID.to_owned()],
            outcomes: Vec::new(),
            event_message: None,
            winning_asset_id: UP_TOKEN_ID.to_owned(),
            winning_outcome: "UP".to_owned(),
        },
    }
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
fn anchor_input() -> crate::public_input::MarketAnchorRuleUpdateInput {
    serde_json::from_value(serde_json::json!({
        "schema_version":"polymarket_market_metadata_v6", "timestamp_received":2000,
        "collector_admission_sequence":1, "collector_admission_time_utc_ms":2001,
        "metadata_origin":"gamma_event_by_slug", "collector_host_id":"host", "collector_region":"hk",
        "slug":"test", "condition_id":"condition", "clob_market_id":"123", "asset_ids":"[\"1\",\"2\"]",
        "market_start_utc":1000,"market_end_utc":301000,"fixed_identity_verified_at_utc":900,
        "market_lifecycle_state":"active","market_runtime_role":"current","metadata_fetch_start_utc":1990,
        "anchor_query_source_epoch_id":"query-epoch","anchor_query_ordinal":1,"anchor_query_result":"observed",
        "price_to_beat_decimal":"64950.01421851144","price_to_beat_source_field":"eventMetadata.priceToBeat",
        "twap_enabled":true,"twap_lookback_seconds":30,"http_received_at_utc":2000,
        "http_request_url":"https://gamma-api.polymarket.com/events/slug/test",
        "http_body_sha256":"123"
    })).unwrap()
}
fn twap_input(window_s: u64) -> crate::public_input::PolymarketRtdsChainlinkTwapInput {
    use crate::typed_raw::{PolymarketRtdsChainlinkBtcUsdRow, RtdsFreshnessStatus};
    crate::public_input::PolymarketRtdsChainlinkTwapInput {
        common: PolymarketRtdsChainlinkBtcUsdRow {
            timestamp_received: 2000,
            message_timestamp: 1999,
            payload_timestamp: 1998,
            age_ms: Some(2),
            symbol: "btc/usd".to_owned(),
            value: "64950.01".to_owned(),
            topic: if window_s == 30 {
                "crypto_prices_twap_thirty"
            } else {
                "crypto_prices_twap_sixty"
            }
            .to_owned(),
            message_type: "update".to_owned(),
            freshness_status: RtdsFreshnessStatus::Fresh,
            subscription_filter: "{}".to_owned(),
            source_native_payload_json: format!("{{\"window_s\":{window_s}}}"),
            collector_host_id: "host".to_owned(),
            collector_region: "hk".to_owned(),
            source_endpoint: "wss://ws-live-data.polymarket.com".to_owned(),
            schema_version: "polymarket_rtds_chainlink_twap_v1".to_owned(),
        },
        full_accuracy_value: "64950014218511440012345".to_owned(),
        window_s,
        source_sequence_epoch_id: "rtds-epoch".to_owned(),
        collector_admission_sequence: 1,
        collector_admission_time_utc_ms: 2001,
    }
}

fn core() -> PublicObservationCoreV1 {
    PublicObservationCoreV1::new(PublicInputStreamHeaderV1 {
        input_stream_id: "core-test".to_owned(),
        canonical_rule_version: "rule-v1".to_owned(),
        observation_version: "public-observation-v1".to_owned(),
        ordering_version: "original-input-v1".to_owned(),
        input_scope: "constructed-finite".to_owned(),
        max_pending_ws_terminals: 2,
        initial_state: PublicInitialStateV1::Empty,
    })
    .unwrap()
}
fn record(position: u64, row: TypedRawRowV2) -> RecordedIngressV1 {
    let source_evidence = source_evidence_v3_from_typed_raw_v2(&row).unwrap();
    RecordedIngressV1 {
        input_stream_id: "core-test".to_owned(),
        input_position: position,
        ingress: DptIngressV1::Message {
            row,
            source_evidence,
        },
        dependency_record_refs: vec![],
    }
}
fn meta(position: u64) -> RecordedIngressV1 {
    record(
        position,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
            metadata_snapshot_row(),
        ))),
    )
}
fn pm(position: u64, seq: u64, source: i64, event: PolymarketOrderbookEvent) -> RecordedIngressV1 {
    record(
        position,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
            "epoch-a", seq, source, 22_000, event,
        ))),
    )
}
fn push(core: &mut PublicObservationCoreV1, record: &RecordedIngressV1) -> AppliedIngressV1 {
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_v1(core, record, &ids, &[]).unwrap();
    crate::without_identity_encoding(|| core.push(&prepared))
        .unwrap()
        .applied
}
fn up(observation: &MarketBusinessObservationV1) -> &AssetBusinessObservationV1 {
    observation
        .assets
        .iter()
        .find(|a| a.asset.token_id.0 == UP_TOKEN_ID)
        .unwrap()
}

#[test]
fn complete_groups_keep_independent_unknowns_confirmations_and_preimages() {
    let mut core = core();
    assert_eq!(core.applied_prefix().next_input_position, 0);
    let initial = push(&mut core, &meta(0));
    let obs = &initial.closed_groups[0].observations[0];
    assert_eq!(initial.closed_groups[0].records.len(), 5);
    assert!(up(obs).bids.value.is_none());
    assert_eq!(up(obs).tick.value.as_ref().unwrap().0, "0.01");
    let tick_anchor = up(obs).tick.direct_anchors.clone();
    let first = push(&mut core, &pm(1, 81856, 21_605, book("4557.61")));
    let obs = &first.closed_groups[0].observations[0];
    assert_eq!(
        up(obs).bids.value.as_ref().unwrap()[0].quantity_shares.0,
        "4557.61"
    );
    let down = obs
        .assets
        .iter()
        .find(|a| a.asset.token_id.0 == DOWN_TOKEN_ID)
        .unwrap();
    for side in [&down.bids, &down.asks] {
        assert!(side.value.is_none());
        assert_eq!(side.quality, MarketDataQuality::UnknownNotObserved);
        assert!(side.direct_anchors.is_empty());
    }
    assert_eq!(up(obs).tick.direct_anchors, tick_anchor);
    let predecessor =
        core.markets[CONDITION_ID].pm.levels[&key(UP_TOKEN_ID, BookSide::Bid, d("0.5"))].clone();
    let later = pm(
        2,
        81857,
        21_700,
        PolymarketOrderbookEvent::PriceChange {
            price_changes: vec![change(Some("4557.61"))],
        },
    );
    let confirmed = push(&mut core, &later);
    assert_eq!(
        up(&confirmed.closed_groups[0].observations[0])
            .bids
            .direct_anchors[0]
            .position
            .closed_at_input_position,
        2
    );
    assert_eq!(
        core.markets[CONDITION_ID].pm.levels[&key(UP_TOKEN_ID, BookSide::Bid, d("0.5"))],
        predecessor
    );
    let old = pm(
        3,
        81860,
        21_433,
        PolymarketOrderbookEvent::PriceChange {
            price_changes: vec![change(Some("4362.61"))],
        },
    );
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_v1(&core, &old, &ids, &[]).unwrap();
    let result = crate::without_identity_encoding(|| core.push(&prepared)).unwrap();
    assert!(
        !result.pm_reductions[0]
            .1
            .steps
            .iter()
            .any(|s| matches!(s.change, PmStepChangeV1::LevelChange { .. }))
    );
    assert_eq!(
        up(&result.applied.closed_groups[0].observations[0])
            .bids
            .value
            .as_ref()
            .unwrap()[0]
            .quantity_shares
            .0,
        "4557.61"
    );
    assert_eq!(
        result.applied.closed_groups[0].records[0]
            .direct_time
            .source_event_at_utc_ms,
        Some(21_433)
    );
    let duplicate = RecordedIngressV1 {
        input_position: 4,
        ..old.clone()
    };
    let prepared = prepare_public_ingress_v1(&core, &duplicate, &ids, &[]).unwrap();
    let result = crate::without_identity_encoding(|| core.push(&prepared)).unwrap();
    assert!(result.applied.closed_groups.is_empty());
    assert_eq!(
        result.reductions[0].reports[0].report.reason_code,
        "POLYMARKET_IDENTICAL_MESSAGE_RETRY"
    );
}

#[test]
fn whole_array_and_same_millisecond_messages_keep_steps_and_delayed_materialization() {
    let mut core = core();
    push(&mut core, &meta(0));
    push(&mut core, &pm(1, 1, 21_000, book("100")));
    let record = pm(
        2,
        2,
        21_001,
        PolymarketOrderbookEvent::PriceChange {
            price_changes: vec![change(Some("80")), change(Some("100"))],
        },
    );
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_v1(&core, &record, &ids, &[]).unwrap();
    let result = crate::without_identity_encoding(|| core.push(&prepared)).unwrap();
    let transitions: Vec<_> = result.pm_reductions[0]
        .1
        .steps
        .iter()
        .filter_map(|s| match &s.change {
            PmStepChangeV1::LevelChange { transition, .. } => Some((
                transition.quantity_before_shares.0.clone(),
                transition.quantity_after_shares.0.clone(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        transitions,
        vec![
            ("100".to_owned(), "80".to_owned()),
            ("80".to_owned(), "100".to_owned())
        ]
    );
    assert_eq!(result.applied.closed_groups.len(), 1);
    let next = push(
        &mut core,
        &pm(
            3,
            3,
            21_001,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("150"))],
            },
        ),
    );
    assert_ne!(
        result.applied.closed_groups[0].group_id,
        next.closed_groups[0].group_id
    );
    assert_eq!(
        up(&next.closed_groups[0].observations[0])
            .bids
            .value
            .as_ref()
            .unwrap()[0]
            .quantity_shares
            .0,
        "150"
    );
    let materialized =
        materialize_pm_step_identity_v1(result.pm_reductions.into_iter().next().unwrap().1)
            .unwrap();
    assert!(!materialized.bindings.is_empty());
    assert!(materialized.reduction.steps.iter().any(|s|matches!(&s.change,PmStepChangeV1::LevelChange {transition,..} if transition.quantity_before_shares.0=="80"&&transition.quantity_after_shares.0=="100")));
}

#[test]
fn terminal_both_orders_and_identity_last_close_once_with_real_direct_anchors() {
    for order in [[0, 1, 2], [1, 0, 2], [2, 1, 0]] {
        let mut core = core();
        let mut metadata = terminal_metadata_row(&market());
        metadata.timestamp_received += 86_400_000;
        metadata.collector_admission_time_utc_ms += 86_400_000;
        metadata.row_evidence_validation_completed_at_utc = Some(metadata.timestamp_received);
        metadata.lifecycle_applied_at_utc = Some(metadata.timestamp_received);
        metadata.runtime_role_applied_at_utc = Some(metadata.timestamp_received);
        let inputs = [
            meta(0),
            record(
                0,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata))),
            ),
            record(
                0,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(terminal_orderbook_row(
                    &market(),
                ))),
            ),
        ];
        let mut all = vec![];
        for (position, index) in order.into_iter().enumerate() {
            let mut input = inputs[index].clone();
            input.input_position = position as u64;
            all.extend(push(&mut core, &input).closed_groups);
        }
        let terminal: Vec<_> = all
            .iter()
            .filter(|g| {
                g.records.iter().any(|f| {
                    matches!(
                        f.payload,
                        PublicBusinessPayloadV1::MarketFact {
                            payload: CanonicalMarketFactPayloadV1::Terminal(_),
                            ..
                        }
                    )
                })
            })
            .collect();
        assert_eq!(terminal.len(), 1);
        assert_eq!(core.pending_ws_terminal_count(), 0);
        assert_eq!(terminal[0].closed_at_input_position, 2);
        let fact = terminal[0]
            .records
            .iter()
            .find(|f| {
                matches!(
                    f.payload,
                    PublicBusinessPayloadV1::MarketFact {
                        payload: CanonicalMarketFactPayloadV1::Terminal(_),
                        ..
                    }
                )
            })
            .unwrap();
        let observed = &terminal[0].observations[0].terminal;
        assert_eq!(observed.direct_anchors.len(), 2);
        for anchor in &observed.direct_anchors {
            let source = fact
                .direct_sources
                .iter()
                .find(|s| s.source_message_id == anchor.source_message_id)
                .unwrap();
            assert_eq!(anchor.source_event_at_utc_ms, source.source_event_at_utc_ms);
            assert_eq!(anchor.received_at_utc_ms, source.received_at_utc_ms);
        }
    }
}

#[test]
fn pending_duplicate_error_precedes_limit_end_is_unpaired_and_abort_is_not_eof() {
    let mut core = core();
    core.header.max_pending_ws_terminals = 1;
    let first = record(
        0,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
            terminal_metadata_row(&market()),
        ))),
    );
    assert!(push(&mut core, &first).closed_groups.is_empty());
    let duplicate = RecordedIngressV1 {
        input_position: 1,
        ..first
    };
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_v1(&core, &duplicate, &ids, &[]).unwrap();
    assert!(matches!(
        crate::without_identity_encoding(|| core.push(&prepared)),
        Err(DptCoreErrorV1::DuplicateWsTerminalClaim { .. })
    ));
    let end = RecordedIngressV1 {
        input_stream_id: "core-test".into(),
        input_position: 1,
        ingress: DptIngressV1::EndOfInput,
        dependency_record_refs: vec![],
    };
    let prepared = prepare_public_ingress_v1(&core, &end, &ids, &[]).unwrap();
    assert!(matches!(
        core.push(&prepared),
        Err(DptCoreErrorV1::UnpairedWsTerminalMetadata { count: 1 })
    ));
    let abort = RecordedIngressV1 {
        ingress: DptIngressV1::Aborted {
            reason: "upstream disconnected".into(),
        },
        ..end
    };
    let prepared = prepare_public_ingress_v1(&core, &abort, &ids, &[]).unwrap();
    assert!(matches!(
        core.push(&prepared),
        Err(DptCoreErrorV1::Aborted { .. })
    ));
    assert_eq!(core.applied_prefix().next_input_position, 1);
}

fn twap_record(position: u64, window: u64, received: i64, price: &str) -> RecordedIngressV1 {
    let mut row = twap_input(window);
    row.common.timestamp_received = received;
    row.common.payload_timestamp = received - 2;
    row.common.message_timestamp = received - 1;
    row.common.age_ms = Some(2);
    row.collector_admission_time_utc_ms = received + 1;
    row.collector_admission_sequence = position + 1;
    row.full_accuracy_value = price.into();
    record(position, TypedRawRowV2::PolymarketRtdsChainlinkTwap(row))
}
fn rule_record(position: u64, window: u32, price: Option<&str>) -> RecordedIngressV1 {
    let mut row = anchor_input();
    row.condition_id = CONDITION_ID.into();
    row.clob_market_id = CLOB_MARKET_ID.into();
    row.asset_ids = format!("[\"{UP_TOKEN_ID}\",\"{DOWN_TOKEN_ID}\"]");
    row.market_start_utc = MARKET_START_AT;
    row.market_end_utc = MARKET_END_AT;
    row.twap_lookback_seconds = Some(window);
    row.price_to_beat_decimal = price.map(str::to_owned);
    row.timestamp_received = 22_000 + position as i64;
    row.collector_admission_sequence = position + 1;
    row.collector_admission_time_utc_ms = row.timestamp_received + 1;
    row.anchor_query_ordinal = position + 1;
    row.http_received_at_utc = Some(row.timestamp_received);
    record(
        position,
        TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
            PolymarketMarketMetadataV6Input::AnchorRule(row),
        )),
    )
}
fn capture_twap(
    core: &mut PublicObservationCoreV1,
    recorded: &RecordedIngressV1,
) -> TemporaryReferenceCandidateV1 {
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_v1(core, recorded, &ids, &[]).unwrap();
    let PreparedCanonicalInputV1::ChainlinkTwap(event) = &prepared.current else {
        panic!()
    };
    let result = crate::without_identity_encoding(|| core.push(&prepared)).unwrap();
    let fact = &result.applied.closed_groups[0].records[0];
    TemporaryReferenceCandidateV1 {
        canonical_fact: event.clone(),
        record_ref: CanonicalInputRecordRefV1 {
            input_stream_id: recorded.input_stream_id.clone(),
            input_position: recorded.input_position,
        },
        anchor: DirectObservationAnchorV1 {
            field_path: "chainlink_twap_sixty".into(),
            position: fact.position.clone(),
            group_id: prepared.prepared_group_id.clone().unwrap(),
            source_message_id: fact.direct_sources[0].source_message_id.clone(),
            source_event_at_utc_ms: fact.direct_sources[0].source_event_at_utc_ms,
            received_at_utc_ms: fact.direct_sources[0].received_at_utc_ms,
        },
    }
}
fn selected_push(
    core: &mut PublicObservationCoreV1,
    recorded: &mut RecordedIngressV1,
    candidates: &[TemporaryReferenceCandidateV1],
    page_size: usize,
) -> AppliedIngressV1 {
    let ids = BTreeMap::new();
    let prepared =
        prepare_public_ingress_with_reference_v1(core, recorded, &ids, &[], &market(), |cursor| {
            let remaining: Vec<_> = candidates
                .iter()
                .filter(|c| c.record_ref.input_position >= cursor.next_input_position)
                .collect();
            let page: Vec<_> = remaining
                .iter()
                .take(page_size)
                .map(|c| (*c).clone())
                .collect();
            let next = page.last().map_or(cursor.next_input_position, |c| {
                c.record_ref.input_position + 1
            });
            Ok((
                page,
                TemporaryReferenceCursorV1 {
                    next_input_position: next,
                },
                remaining.len() <= page_size,
            ))
        })
        .unwrap();
    if prepared
        .reference_selection
        .as_ref()
        .unwrap()
        .selected_record_ref
        .as_ref()
        .is_some_and(|r| r.input_position < prepared.recorded.input_position)
    {
        assert_eq!(prepared.dependencies.len(), 1);
    }
    crate::without_identity_encoding(|| core.push(&prepared))
        .unwrap()
        .applied
}

#[test]
fn configuration_late_selects_first_original_record_and_replay_pages_match() {
    let mut traces = vec![];
    for page_size in [1, 8] {
        let mut core = core();
        push(&mut core, &meta(0));
        let a = capture_twap(
            &mut core,
            &twap_record(1, 60, 21_000, "65000000000000000000000"),
        );
        let b = capture_twap(
            &mut core,
            &twap_record(2, 60, 21_001, "66000000000000000000000"),
        );
        assert!(core.reference_state(CONDITION_ID).is_none());
        let mut config = rule_record(3, 60, None);
        let selected = selected_push(&mut core, &mut config, &[a.clone(), b], page_size);
        assert_eq!(config.dependency_record_refs, vec![a.record_ref.clone()]);
        let value = selected.closed_groups[0].observations[0]
            .temporary_reference
            .as_ref()
            .unwrap();
        assert_eq!(value.value.price_usd_per_btc.0, "65000");
        assert_eq!(value.direct_anchor.received_at_utc_ms, 21_000);
        assert_eq!(value.dependency_frontier.next_input_position, 4);
        assert_eq!(selected.closed_groups[0].closed_at_input_position, 3);
        let mut later = rule_record(4, 60, Some("65000"));
        let official = selected_push(&mut core, &mut later, &[a], page_size);
        assert!(matches!(
            official.closed_groups[0].observations[0].effective_reference,
            EffectiveMarketReferenceV1::OfficialAnchor(_)
        ));
        assert_eq!(
            official.closed_groups[0].observations[0]
                .temporary_reference
                .as_ref()
                .unwrap()
                .direct_anchor
                .received_at_utc_ms,
            21_000
        );
        traces.push((selected, official, config.dependency_record_refs));
    }
    assert_eq!(traces[0], traces[1]);
}

#[test]
fn actual_rule_change_does_not_retroactively_make_earlier_sample_applicable() {
    let mut core = core();
    push(&mut core, &meta(0));
    push(&mut core, &rule_record(1, 30, None));
    let a = capture_twap(
        &mut core,
        &twap_record(2, 60, 21_000, "65000000000000000000000"),
    );
    let mut changed = rule_record(3, 60, None);
    let output = selected_push(&mut core, &mut changed, &[a], 1);
    assert!(matches!(
        output.closed_groups[0].observations[0].effective_reference,
        EffectiveMarketReferenceV1::Missing
    ));
    assert!(changed.dependency_record_refs.is_empty());
    // 下一条真实 TWAP 使用已经观测的60秒配置；不把配置的查询时间冒充平台生效时刻。
    let mut current = twap_record(4, 60, 23_000, "67000000000000000000000");
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_v1(&core, &current, &ids, &[]).unwrap();
    let PreparedCanonicalInputV1::ChainlinkTwap(event) = &prepared.current else {
        panic!()
    };
    let candidate = TemporaryReferenceCandidateV1 {
        canonical_fact: event.clone(),
        record_ref: CanonicalInputRecordRefV1 {
            input_stream_id: "core-test".into(),
            input_position: 4,
        },
        anchor: DirectObservationAnchorV1 {
            field_path: "chainlink_twap_sixty".into(),
            position: PublicFactPositionV1 {
                input_stream_id: "core-test".into(),
                closed_at_input_position: 4,
                fact_ordinal: 0,
            },
            group_id: prepared.prepared_group_id.clone().unwrap(),
            source_message_id: event.lineage.source_evidence[0].source_message_id.clone(),
            source_event_at_utc_ms: event.time.source_event_at_utc_ms,
            received_at_utc_ms: event.time.received_at_utc_ms,
        },
    };
    drop(prepared);
    let output = selected_push(&mut core, &mut current, &[candidate], 1);
    assert_eq!(
        output.closed_groups[0].observations[0]
            .temporary_reference
            .as_ref()
            .unwrap()
            .value
            .price_usd_per_btc
            .0,
        "67000"
    );
    assert!(current.dependency_record_refs.is_empty());
}

#[test]
fn rtds_anomalies_do_not_replace_legal_price_or_its_signed_time() {
    for previous in [false, true] {
        for status in [
            RtdsFreshnessStatus::InvalidType,
            RtdsFreshnessStatus::InvalidTopic,
            RtdsFreshnessStatus::InvalidSymbol,
            RtdsFreshnessStatus::InvalidTimestamp,
        ] {
            let mut core = core();
            push(&mut core, &meta(0));
            let mut row = twap_input(60).common;
            row.schema_version = POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into();
            row.topic = "crypto_prices_chainlink".into();
            row.payload_timestamp = 2100;
            row.age_ms = Some(-100);
            let mut position = 1;
            let old = if previous {
                let result = push(
                    &mut core,
                    &record(
                        position,
                        TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(
                            row.clone(),
                        )),
                    ),
                );
                position += 1;
                Some(
                    result.closed_groups[0].observations[0]
                        .chainlink_spot
                        .clone(),
                )
            } else {
                None
            };
            row.freshness_status = status;
            let output = push(
                &mut core,
                &record(
                    position,
                    TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row)),
                ),
            );
            assert_eq!(output.closed_groups.len(), 1);
            let group = &output.closed_groups[0];
            assert_eq!(
                core.current_chainlink_spot(),
                &group.observations[0].chainlink_spot
            );
            assert!(matches!(
                group.records[0].payload,
                PublicBusinessPayloadV1::ReferenceFact(
                    CanonicalReferenceEventPayloadV2::RtdsObservedAnomaly(_)
                )
            ));
            if status == RtdsFreshnessStatus::InvalidTimestamp {
                assert_eq!(group.records[0].direct_time.source_event_at_utc_ms, None);
            }
            if let Some(old) = old {
                assert_eq!(group.observations[0].chainlink_spot, old);
                let a = &old.direct_anchors[0];
                assert_eq!(
                    a.received_at_utc_ms - a.source_event_at_utc_ms.unwrap(),
                    -100
                );
            } else {
                assert_eq!(
                    group.observations[0].chainlink_spot,
                    ObservedFactV1::default()
                );
            }
        }
    }
}

#[test]
fn binance_branches_use_the_existing_state_under_encoding_guard() {
    let mut core = core();
    let first = push(
        &mut core,
        &record(
            0,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot(
                "depth-a", 100, 1,
            ))),
        ),
    );
    assert!(first.closed_groups[0].observations.is_empty());
    let update = update(
        "depth-a",
        101,
        101,
        2,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![level("64100", "0.2")],
    );
    push(
        &mut core,
        &record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(update)),
        ),
    );
    push(&mut core, &meta(2));
    let first = push(
        &mut core,
        &record(
            3,
            TypedRawRowV2::Existing(TypedRawRow::BinanceAggTrade(trade(1, 1, "64000"))),
        ),
    );
    assert_eq!(
        first.closed_groups[0].observations[0]
            .binance_trade
            .value
            .as_ref()
            .unwrap()
            .price_usdt_per_btc
            .0,
        "64000"
    );
    let input = record(
        4,
        TypedRawRowV2::Existing(TypedRawRow::BinanceAggTrade(trade(1, 2, "64000"))),
    );
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_v1(&core, &input, &ids, &[]).unwrap();
    let result = crate::without_identity_encoding(|| core.push(&prepared)).unwrap();
    assert!(result.applied.closed_groups.is_empty());
    assert!(matches!(
        result.reductions[0].reports[0].report.disposition,
        ReductionDispositionV1::Suppressed { .. }
    ));
}

const GAMMA_TERMINAL_RECEIVED_TIME_MS: i64 = 50_000;
fn gamma_terminal_metadata_row(market: &MarketIdentityV2) -> PolymarketMarketMetadataRow {
    let mut row = terminal_metadata_row(market);
    row.metadata_row_kind = PolymarketMetadataRowKind::ResolutionPollAttempt;
    row.timestamp_received = GAMMA_TERMINAL_RECEIVED_TIME_MS;
    row.collector_admission_sequence = 43;
    row.collector_admission_time_utc_ms = GAMMA_TERMINAL_RECEIVED_TIME_MS + 10;
    row.metadata_origin = PolymarketMetadataOrigin::GammaBySlug;
    row.source_endpoint = Some("https://gamma-api.polymarket.com/markets/slug".to_owned());
    row.source_evidence_origin = Some(MetadataSourceEvidenceOrigin::GammaBySlug);
    row.source_evidence_event_type =
        Some(MetadataSourceEvidenceEventType::GammaMarketBySlugResponse);
    row.source_evidence_channel = None;
    row.source_evidence_received_at_utc = Some(GAMMA_TERMINAL_RECEIVED_TIME_MS);
    row.row_evidence_validation_completed_at_utc = Some(GAMMA_TERMINAL_RECEIVED_TIME_MS + 1);
    row.lifecycle_applied_at_utc = Some(GAMMA_TERMINAL_RECEIVED_TIME_MS + 2);
    row.runtime_role_applied_at_utc = Some(GAMMA_TERMINAL_RECEIVED_TIME_MS + 2);
    row.metadata_fetch_start_utc = Some(GAMMA_TERMINAL_RECEIVED_TIME_MS - 20);
    row.closed = Some(true);
    row.closed_time_raw = Some("1970-01-01T00:00:48Z".to_owned());
    row.closed_time_utc = Some(market.window.end_at_utc_ms + 18_000);

    row.closed_time_check_failure_reason = None;
    row.automatically_resolved = Some(true);
    row.uma_resolution_status = Some("resolved".to_owned());
    row.outcomes_raw = Some(r#"["UP","DOWN"]"#.to_owned());
    row.outcome_prices_raw = Some(r#"["1","0"]"#.to_owned());
    row.active = Some(true);
    row.active_unavailable_reason = None;
    row.accepting_orders = Some(false);
    row.ws_platform_timestamp_raw = None;
    row.ws_platform_timestamp_utc = None;
    row.resolution_evidence_source = Some(MetadataResolutionEvidenceSource::GammaBySlug);
    row.terminal_claim_owner = Some(MetadataTerminalClaimOwner::GammaBySlug);
    row.resolution_poll_attempt_ordinal = Some(4);
    row.resolution_poll_scheduled_utc = Some(GAMMA_TERMINAL_RECEIVED_TIME_MS - 30);
    row.resolution_poll_result = Some(MetadataResolutionPollResult::Terminal);
    row.resolution_followup_status = Some(MetadataResolutionFollowupStatus::TerminalGammaFallback);
    row.resolution_integrity_error_class = None;

    row.terminal_predicate_failure_reason = None;
    row.response_fields_unavailable_reason = None;
    row.ws_claimant_source_sequence_epoch_id = None;
    row.ws_claimant_collector_admission_sequence = None;
    row
}

fn local_transition_row(
    mut row: PolymarketMarketMetadataRow,
    received_at: i64,
) -> PolymarketMarketMetadataRow {
    row.metadata_row_kind = PolymarketMetadataRowKind::MarketStateTransition;
    row.timestamp_received = received_at;
    row.collector_admission_sequence += 1;
    row.collector_admission_time_utc_ms = received_at + 1;
    row.metadata_origin = PolymarketMetadataOrigin::LocalMarketStateTimer;
    row.source_endpoint = None;
    row.source_endpoint_unavailable_reason =
        Some(MetadataSourceEndpointUnavailableReason::NotApplicableNoSourceEvidence);
    row.source_evidence_origin = None;
    row.source_evidence_event_type = None;
    row.source_evidence_channel = None;
    row.source_evidence_received_at_utc = None;
    row.row_evidence_validation_completed_at_utc = None;
    row.source_evidence_payload_json = None;
    row.metadata_fetch_start_utc = None;
    row.closed = None;
    row.active = None;
    row.active_unavailable_reason = Some(MetadataActiveUnavailableReason::NotApplicableForRowKind);
    row.accepting_orders = None;
    row.outcomes_raw = None;
    row.outcome_prices_raw = None;
    row.response_fields_unavailable_reason = None;
    row
}

fn resolution_followup_summary_row(
    mut row: PolymarketMarketMetadataRow,
    status: MetadataResolutionFollowupStatus,
    received_at: i64,
) -> PolymarketMarketMetadataRow {
    row.metadata_row_kind = PolymarketMetadataRowKind::ResolutionFollowupSummary;
    row.timestamp_received = received_at;
    row.collector_admission_sequence += 1;
    row.collector_admission_time_utc_ms = received_at;
    row.metadata_origin = PolymarketMetadataOrigin::LocalFollowupScheduler;
    row.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    row.market_runtime_role = MetadataMarketRuntimeRole::None;
    row.source_endpoint = None;
    row.source_endpoint_unavailable_reason =
        Some(MetadataSourceEndpointUnavailableReason::NotApplicableNoSourceEvidence);
    row.source_evidence_origin = None;
    row.source_evidence_event_type = None;
    row.source_evidence_channel = None;
    row.source_evidence_received_at_utc = None;
    row.row_evidence_validation_completed_at_utc = None;
    row.source_evidence_payload_json = None;
    row.source_evidence_raw_sha256 = None;
    row.market_lifecycle_transition = None;
    row.lifecycle_scheduled_at_utc = None;
    row.lifecycle_applied_at_utc = None;
    row.market_runtime_role_transition =
        Some(MetadataMarketRuntimeRoleTransition::ResolutionPendingToNone);
    row.runtime_role_scheduled_at_utc = None;
    row.runtime_role_applied_at_utc = Some(received_at);
    row.metadata_fetch_start_utc = None;
    row.closed = None;
    row.closed_time_raw = None;
    row.closed_time_utc = None;

    row.closed_time_check_failure_reason = None;
    row.automatically_resolved = None;
    row.uma_resolution_status = None;
    row.outcomes_raw = None;
    row.outcome_prices_raw = None;
    row.active = None;
    row.active_unavailable_reason = Some(MetadataActiveUnavailableReason::NotApplicableForRowKind);
    row.accepting_orders = None;
    row.winning_asset_id = None;
    row.winning_outcome = None;
    row.resolution_evidence_source = None;
    row.terminal_claim_owner = None;
    row.resolution_poll_attempt_ordinal = None;
    row.resolution_poll_scheduled_utc = None;
    row.resolution_poll_result = None;
    row.resolution_transport_error_class = None;
    row.resolution_transport_http_status = None;
    row.resolution_integrity_error_class = None;
    row.resolution_budget_horizon_utc = Some(received_at);
    row.resolution_followup_status = Some(status);
    row.resolution_poll_attempts_recorded = Some(6);
    match status {
        MetadataResolutionFollowupStatus::BudgetExhaustedAfterValidNonterminal => {
            row.resolution_valid_response_count = Some(6);
            row.resolution_transport_unavailable_count = Some(0);
            row.resolution_missed_slot_count = Some(0);
            row.last_valid_metadata_received_utc = Some(received_at - 1);
        }
        MetadataResolutionFollowupStatus::BudgetExhaustedWithoutValidResponse => {
            row.resolution_valid_response_count = Some(0);
            row.resolution_transport_unavailable_count = Some(6);
            row.resolution_missed_slot_count = Some(0);
            row.last_valid_metadata_received_utc = None;
        }
        _ => {}
    }

    row.terminal_predicate_failure_reason = None;
    row.response_fields_unavailable_reason =
        Some(MetadataResponseFieldsUnavailableReason::NotApplicableForRowKind);
    row.ws_claimant_source_sequence_epoch_id = None;
    row.ws_claimant_collector_admission_sequence = None;
    row
}

use base64::Engine as _;
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
fn fullbook_absence_version_and_epoch_recovery_do_not_create_false_quantity() {
    let mut core = core();
    push(&mut core, &meta(0));
    push(&mut core, &pm(1, 1, 21_100, book("100")));
    push(
        &mut core,
        &pm(
            2,
            2,
            21_300,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("7"))],
            },
        ),
    );
    let mut absent = book("100");
    if let PolymarketOrderbookEvent::Book { bids, .. } = &mut absent {
        bids.clear();
    }
    let old = push(&mut core, &pm(3, 3, 21_200, absent.clone()));
    assert_eq!(
        up(&old.closed_groups[0].observations[0])
            .bids
            .value
            .as_ref()
            .unwrap()[0]
            .quantity_shares
            .0,
        "7"
    );
    let new = push(&mut core, &pm(4, 4, 21_400, absent));
    assert_eq!(
        up(&new.closed_groups[0].observations[0]).bids.value,
        Some(vec![])
    );
    let old = push(
        &mut core,
        &pm(
            5,
            5,
            21_350,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("5"))],
            },
        ),
    );
    assert_eq!(
        up(&old.closed_groups[0].observations[0]).bids.value,
        Some(vec![])
    );
    let bba = PolymarketOrderbookEvent::BestBidAsk {
        asset_id: UP_TOKEN_ID.into(),
        best_bid: Some("0.4".into()),
        best_ask: Some("0.6".into()),
        spread: Some("0.2".into()),
    };
    let input = record(
        6,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
            "epoch-b", 1, 20_000, 22_100, bba,
        ))),
    );
    let reset = push(&mut core, &input);
    let obs = up(&reset.closed_groups[0].observations[0]);
    assert!(obs.bids.value.is_none());
    assert_eq!(obs.bids.quality, MarketDataQuality::Missing);
    assert_eq!(
        obs.best_bid_ask
            .value
            .as_ref()
            .unwrap()
            .spread_pusd_per_share
            .as_ref()
            .unwrap()
            .0,
        "0.2"
    );
    // 尚未恢复就绪时再次换代，既有 Missing 不能被重置成未知。
    let input = record(
        7,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
            "epoch-c",
            1,
            20_001,
            22_101,
            PolymarketOrderbookEvent::BestBidAsk {
                asset_id: UP_TOKEN_ID.into(),
                best_bid: Some("0.4".into()),
                best_ask: Some("0.6".into()),
                spread: Some("0.2".into()),
            },
        ))),
    );
    let reset_again = push(&mut core, &input);
    let obs = up(&reset_again.closed_groups[0].observations[0]);
    assert!(obs.bids.value.is_none());
    assert_eq!(obs.bids.quality, MarketDataQuality::Missing);
    let input = record(
        8,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
            "epoch-c",
            2,
            20_001,
            22_101,
            book("15"),
        ))),
    );
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_v1(&core, &input, &ids, &[]).unwrap();
    let restored = crate::without_identity_encoding(|| core.push(&prepared)).unwrap();
    assert!(
        !restored.pm_reductions[0]
            .1
            .steps
            .iter()
            .any(|s| matches!(s.change, PmStepChangeV1::InitialSnapshot { .. }))
    );
    assert_eq!(
        up(&restored.applied.closed_groups[0].observations[0])
            .bids
            .value
            .as_ref()
            .unwrap()[0]
            .quantity_shares
            .0,
        "15"
    );
}

#[test]
fn metadata_variants_and_gamma_terminal_preserve_operational_no_output() {
    let mut core = core();
    push(&mut core, &meta(0));
    let mut transition = local_transition_row(metadata_snapshot_row(), 20_000);
    transition.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    transition.market_lifecycle_transition =
        Some(MetadataMarketLifecycleTransition::PrefetchedToActive);
    transition.lifecycle_scheduled_at_utc = Some(20_000);
    transition.lifecycle_applied_at_utc = Some(20_000);
    transition.market_runtime_role = MetadataMarketRuntimeRole::Current;
    transition.market_runtime_role_transition =
        Some(MetadataMarketRuntimeRoleTransition::NextToCurrent);
    transition.runtime_role_scheduled_at_utc = Some(20_000);
    transition.runtime_role_applied_at_utc = Some(20_000);
    let applied = push(
        &mut core,
        &record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(transition))),
        ),
    );
    assert!(matches!(
        applied.closed_groups[0].observations[0].lifecycle.value,
        Some(MappedMarketLifecycleStateV1::Known {
            value: KnownMarketLifecycleState::Active
        })
    ));
    let summary = resolution_followup_summary_row(
        metadata_snapshot_row(),
        MetadataResolutionFollowupStatus::BudgetExhaustedAfterValidNonterminal,
        60_000,
    );
    let input = record(
        2,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(summary))),
    );
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_v1(&core, &input, &ids, &[]).unwrap();
    let result = crate::without_identity_encoding(|| core.push(&prepared)).unwrap();
    assert!(result.applied.closed_groups.is_empty());
    assert!(
        result.reductions[0].reports[0]
            .report
            .reason_code
            .contains("BUDGET_EXHAUSTED")
    );
    let gamma = record(
        3,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
            gamma_terminal_metadata_row(&market()),
        ))),
    );
    let output = push(&mut core, &gamma);
    assert!(
        output.closed_groups[0].observations[0]
            .terminal
            .value
            .is_some()
    );
    let mut other = self::core();
    let row = metadata_snapshot_row();
    push(
        &mut other,
        &record(
            0,
            TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
                PolymarketMarketMetadataV6Input::Existing(row.clone()),
            )),
        ),
    );
    let mut active = row;
    active.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    active.timestamp_received = 20_000;
    active.collector_admission_sequence = 2;
    active.collector_admission_time_utc_ms = 20_001;
    let output = push(
        &mut other,
        &record(
            1,
            TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
                PolymarketMarketMetadataV6Input::Existing(active),
            )),
        ),
    );
    assert!(output.closed_groups[0].records.iter().any(|f| matches!(
        f.payload,
        PublicBusinessPayloadV1::MarketFact {
            payload: CanonicalMarketFactPayloadV1::MarketLifecycleSnapshot { .. },
            ..
        }
    )));
}

#[test]
fn invalid_aggregate_trade_keeps_report_without_a_group_and_old_price_survives() {
    let mut core = core();
    push(&mut core, &meta(0));
    push(
        &mut core,
        &record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::BinanceAggTrade(trade(1, 1, "64000"))),
        ),
    );
    let input = record(
        2,
        TypedRawRowV2::Existing(TypedRawRow::BinanceAggTrade(invalid_trade(2))),
    );
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_v1(&core, &input, &ids, &[]).unwrap();
    let result = crate::without_identity_encoding(|| core.push(&prepared)).unwrap();
    assert!(result.applied.closed_groups.is_empty());
    assert_eq!(
        result.reductions[0].reports[0].report.reason_code,
        "BINANCE_AGGTRADE_INVALID_INPUT"
    );
    assert_eq!(
        core.global
            .trade
            .value
            .as_ref()
            .unwrap()
            .price_usdt_per_btc
            .0,
        "64000"
    );
}

#[test]
fn changing_call_batches_preserves_every_group_step_report_anchor_and_prefix() {
    let mut book_down = book("50");
    if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut book_down {
        *asset_id = DOWN_TOKEN_ID.into();
    }
    let rows = vec![
        meta(0),
        pm(1, 1, 21_000, book("100")),
        pm(2, 2, 21_001, book_down),
        pm(
            3,
            3,
            21_002,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("80")), change(Some("100"))],
            },
        ),
        record(
            4,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot("depth", 100, 1))),
        ),
        record(
            5,
            TypedRawRowV2::Existing(TypedRawRow::BinanceAggTrade(trade(1, 1, "64000"))),
        ),
    ];
    let run = |batch_size| {
        let mut core = core();
        let mut output = vec![];
        let ids = BTreeMap::new();
        for batch in rows.chunks(batch_size) {
            for row in batch {
                let prepared = prepare_public_ingress_v1(&core, row, &ids, &[]).unwrap();
                let result = crate::without_identity_encoding(|| core.push(&prepared)).unwrap();
                let pm: Vec<_> = result
                    .pm_reductions
                    .iter()
                    .map(|(position, reduction)| serde_json::json!([position, reduction]))
                    .collect();
                let reports: Vec<_> = result
                    .reductions
                    .iter()
                    .map(|r| {
                        serde_json::json!([
                            r.facts,
                            r.reports
                                .iter()
                                .map(|r| (&r.position, &r.report))
                                .collect::<Vec<_>>()
                        ])
                    })
                    .collect();
                output.push((result.applied, pm, reports));
            }
        }
        let end = RecordedIngressV1 {
            input_stream_id: "core-test".into(),
            input_position: 6,
            ingress: DptIngressV1::EndOfInput,
            dependency_record_refs: vec![],
        };
        assert!(push(&mut core, &end).closed_groups.is_empty());
        assert_eq!(core.applied_prefix().next_input_position, 7);
        output
    };
    let reference = run(1);
    assert_eq!(reference.len(), 6);
    assert_eq!(
        reference.last().unwrap().0.closed_groups[0].closed_at_input_position,
        5
    );
    for size in [2, 4, 20] {
        assert_eq!(run(size), reference);
    }
}

#[test]
fn one_historical_response_keeps_all_markets_effective_evidence_and_query_time() {
    let mut core = core();
    push(&mut core, &meta(0));
    let mut other = metadata_snapshot_row();
    other.condition_id = format!("0x{}", "2".repeat(64));
    other.clob_market_id = "3195577".into();
    other.collector_admission_sequence = 2;
    let other_id = other.condition_id.clone();
    push(
        &mut core,
        &record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(other))),
        ),
    );
    let response = HttpResponseEvidenceV1 {
        origin: "gamma_events".into(),
        request_url: "https://gamma-api.polymarket.com/events".into(),
        received_at_utc: "2026-09-08T04:00:00.123456Z".into(),
        received_at_utc_ms: 1_788_840_000_123,
        receipt_observation: "curl_completed".into(),
        response_body_sha256: "a".repeat(64),
        response_body_path: "response.json".into(),
        original_response_file: "/original/response.json".into(),
        http_status_code: None,
        response_url: None,
    };
    let source = historical_response_source_evidence_v3(&response).unwrap();
    let mut identities = BTreeMap::new();
    let mut rows = vec![];
    for (i, id) in [CONDITION_ID.to_owned(), other_id.clone()]
        .into_iter()
        .enumerate()
    {
        let context = core.pm_context(&id).unwrap();
        let key = format!("identity-{i}");
        identities.insert(key.clone(), context.identity_source_evidence[0].clone());
        rows.push(HistoricalPriceAnchorRepairRowV1 {
            schema_version: HISTORICAL_PRICE_ANCHOR_REPAIR_SCHEMA_V1.into(),
            condition_id: id,
            clob_market_id: context.market.clob_market_id.0,
            slug: "btc-updown-5m".into(),
            asset_ids_json: format!("[\"{UP_TOKEN_ID}\",\"{DOWN_TOKEN_ID}\"]"),
            market_start_at_utc_ms: MARKET_START_AT,
            market_end_at_utc_ms: MARKET_END_AT,
            anchor_source: MarketAnchorSourceV1::GammaEventMetadataPriceToBeat,
            price_to_beat_raw: "65000".into(),
            response_group_id: source.source_message_id.0.clone(),
            response_member_index: i as u32,
            response_received_at_utc_ms: response.received_at_utc_ms,
            response_sha256: response.response_body_sha256.clone(),
            source_value_path: format!("/{i}/eventMetadata/priceToBeat"),
            source_event_at_utc_ms: None,
            historical_identity_ready_at_utc_ms: [10_000, 21_000][i],
            historical_identity_evidence_ids_json: format!("[\"{key}\"]"),
            repair_policy_version: "price_anchor_historical_repair_v1".into(),
        });
    }
    let batch = HistoricalPriceAnchorRepairBatchInput {
        response_group_id: source.source_message_id.0.clone(),
        response: response.clone(),
        rows,
        identity_evidence: BTreeMap::new(),
    };
    let mut input = record(2, TypedRawRowV2::HistoricalPriceAnchorRepairBatch(batch));
    input.dependency_record_refs = vec![
        CanonicalInputRecordRefV1 {
            input_stream_id: "core-test".into(),
            input_position: 0,
        },
        CanonicalInputRecordRefV1 {
            input_stream_id: "core-test".into(),
            input_position: 1,
        },
    ];
    let prepared = prepare_public_ingress_v1(&core, &input, &identities, &[]).unwrap();
    let result = crate::without_identity_encoding(|| core.push(&prepared)).unwrap();
    let group = &result.applied.closed_groups[0];
    assert_eq!(group.records.len(), 2);
    assert_eq!(group.observations.len(), 2);
    for (i, observation) in group.observations.iter().enumerate() {
        let anchor = observation.official_anchor.value.as_ref().unwrap();
        let PriceAnchorApplicationV1::HistoricalRepair {
            effective_from_utc_ms,
            historical_identity_evidence,
            ..
        } = &anchor.application
        else {
            panic!()
        };
        assert_eq!(*effective_from_utc_ms, [20_000, 21_000][i]);
        assert_eq!(historical_identity_evidence.len(), 1);
        assert_eq!(
            observation.official_anchor.direct_anchors[0].received_at_utc_ms,
            response.received_at_utc_ms
        );
        assert_eq!(
            observation
                .official_anchor
                .dependency_frontier
                .as_ref()
                .unwrap()
                .next_input_position,
            3
        );
    }
}

#[test]
fn multiple_ready_claims_follow_original_acceptance_not_key_sort_and_release() {
    let mut core = core();
    core.header.max_pending_ws_terminals = 2;
    let mut ws = terminal_orderbook_row(&market());
    ws.collector_admission_sequence = 100;
    let mut metadata = terminal_metadata_row(&market());
    metadata.ws_claimant_collector_admission_sequence = Some(100);
    let first_group_source = source_evidence_v3_from_typed_raw_v2(&TypedRawRowV2::Existing(
        TypedRawRow::PolymarketOrderbook(ws.clone()),
    ))
    .unwrap()
    .source_message_id;
    let mut inputs = vec![
        record(
            0,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(ws)),
        ),
        record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata))),
        ),
        record(
            2,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(terminal_orderbook_row(
                &market(),
            ))),
        ),
        record(
            3,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
                terminal_metadata_row(&market()),
            ))),
        ),
    ];
    for input in &mut inputs {
        assert!(push(&mut core, input).closed_groups.is_empty());
    }
    assert_eq!(core.pending_ws_terminal_count(), 2);
    let identity = meta(4);
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_v1(&core, &identity, &ids, &[]).unwrap();
    let result = crate::without_identity_encoding(|| core.push(&prepared)).unwrap();
    assert_eq!(core.pending_ws_terminal_count(), 0);
    let terminal = result
        .applied
        .closed_groups
        .iter()
        .find(|g| {
            g.records.iter().any(|f| {
                matches!(
                    f.payload,
                    PublicBusinessPayloadV1::MarketFact {
                        payload: CanonicalMarketFactPayloadV1::Terminal(_),
                        ..
                    }
                )
            })
        })
        .unwrap();
    assert_eq!(terminal.member_input_positions, vec![0, 1, 4]);
    assert!(
        terminal.records[0]
            .direct_sources
            .iter()
            .any(|s| s.source_message_id == first_group_source)
    );
    assert!(
        result
            .reductions
            .iter()
            .flat_map(|r| &r.reports)
            .any(|r| matches!(
                r.report.disposition,
                ReductionDispositionV1::Suppressed { .. }
            ))
    );
}

#[test]
fn retained_statistics_remain_available_without_a_public_capacity_gate() {
    let mut core = core();
    push(&mut core, &meta(0));
    push(&mut core, &pm(1, 1, 21_000, book("100")));
    let before = core.retained_state_entries();
    assert_eq!(before, 5); // 代际、准入、两价位、完整簿缺价基准。
    assert!(!core.markets[CONDITION_ID].pm.selected.is_empty());
    let input = pm(
        2,
        2,
        21_001,
        PolymarketOrderbookEvent::PriceChange {
            price_changes: vec![change(Some("100"))],
        },
    );
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_v1(&core, &input, &ids, &[]).unwrap();
    crate::without_identity_encoding(|| core.push(&prepared)).unwrap();
    assert_eq!(core.retained_state_entries(), 6);
    assert_eq!(core.applied_prefix().next_input_position, 3);
}

#[test]
fn direct_empty_bba_dynamic_tick_trade_and_new_market_are_complete_original_groups() {
    let mut core = core();
    push(&mut core, &meta(0));
    let events = [
        PolymarketOrderbookEvent::BestBidAsk {
            asset_id: UP_TOKEN_ID.into(),
            best_bid: None,
            best_ask: None,
            spread: None,
        },
        PolymarketOrderbookEvent::TickSizeChange {
            asset_id: UP_TOKEN_ID.into(),
            old_tick_size: Some("0.01".into()),
            new_tick_size: "0.02".into(),
        },
        PolymarketOrderbookEvent::LastTradePrice {
            asset_id: UP_TOKEN_ID.into(),
            price: "0.5".into(),
            size: Some("1".into()),
            side: Some(PolymarketSide::Buy),
            fee_rate_bps: Some("10".into()),
        },
        PolymarketOrderbookEvent::NewMarket {
            clob_market_id: CLOB_MARKET_ID.into(),
            question: None,
            description: None,
            asset_ids: vec![UP_TOKEN_ID.into(), DOWN_TOKEN_ID.into()],
            outcomes: vec!["UP".into(), "DOWN".into()],
            event_message: None,
        },
    ];
    for (i, event) in events.into_iter().enumerate() {
        let result = push(
            &mut core,
            &pm(i as u64 + 1, i as u64 + 1, 21_000 + i as i64, event),
        );
        assert_eq!(result.closed_groups.len(), 1);
        assert_eq!(result.closed_groups[0].records.len(), 1);
        let state = up(&result.closed_groups[0].observations[0]);
        assert!(state.bids.value.is_none());
        if i == 0 {
            assert_eq!(
                state.best_bid_ask.value,
                Some(CanonicalBbaFactV1 {
                    best_bid_pusd_per_share: None,
                    best_ask_pusd_per_share: None,
                    spread_pusd_per_share: None
                })
            );
        }
        if i >= 1 {
            assert_eq!(state.tick.value.as_ref().unwrap().0, "0.02");
        }
        if i >= 2 {
            assert_eq!(
                state.public_trade.value.as_ref().unwrap().quantity_shares.0,
                "1"
            );
        }
    }
}

fn timed_twap_record(
    position: u64,
    observed: i64,
    received: i64,
    price: &str,
) -> RecordedIngressV1 {
    let mut row = twap_input(60);
    row.common.payload_timestamp = observed;
    row.common.message_timestamp = observed + 1;
    row.common.timestamp_received = received;
    row.common.age_ms = Some(received - observed);
    row.collector_admission_sequence = position + 1;
    row.collector_admission_time_utc_ms = received + 1;
    row.full_accuracy_value = price.into();
    record(position, TypedRawRowV2::PolymarketRtdsChainlinkTwap(row))
}

#[test]
fn temporary_reference_uses_observation_window_then_original_reception_order() {
    let mut core = core();
    push(&mut core, &meta(0));
    let early = capture_twap(
        &mut core,
        &timed_twap_record(1, 19_999, 20_001, "61000000000000000000000"),
    );
    let end = capture_twap(
        &mut core,
        &timed_twap_record(2, 30_000, 30_001, "69000000000000000000000"),
    );
    let mut config = rule_record(3, 60, None);
    let rejected = selected_push(&mut core, &mut config, &[early.clone(), end.clone()], 1);
    assert!(matches!(
        rejected.closed_groups[0].observations[0].effective_reference,
        EffectiveMarketReferenceV1::Missing
    ));
    assert!(config.dependency_record_refs.is_empty());
    let open = capture_twap(
        &mut core,
        &timed_twap_record(4, 20_000, 30_002, "65000000000000000000000"),
    );
    let mut config = rule_record(5, 60, None);
    let accepted = selected_push(&mut core, &mut config, &[early, end, open.clone()], 2);
    let temporary = accepted.closed_groups[0].observations[0]
        .temporary_reference
        .as_ref()
        .unwrap();
    assert_eq!(temporary.value.payload_timestamp_utc_ms, 20_000);
    assert_eq!(temporary.direct_anchor.received_at_utc_ms, 30_002);
    assert_eq!(config.dependency_record_refs, vec![open.record_ref]);
}

#[test]
fn late_exact_open_observation_does_not_replace_already_fixed_reference() {
    let mut core = core();
    push(&mut core, &meta(0));
    let first = capture_twap(
        &mut core,
        &timed_twap_record(1, 20_010, 20_011, "65000000000000000000000"),
    );
    let mut config = rule_record(2, 60, None);
    let selected = selected_push(&mut core, &mut config, &[first.clone()], 1);
    let old = selected.closed_groups[0].observations[0]
        .temporary_reference
        .clone();
    let later = capture_twap(
        &mut core,
        &timed_twap_record(3, 20_000, 22_000, "66000000000000000000000"),
    );
    let mut config = rule_record(4, 60, None);
    let kept = selected_push(&mut core, &mut config, &[first, later], 1);
    assert_eq!(
        kept.closed_groups[0].observations[0].temporary_reference,
        old
    );
    assert!(config.dependency_record_refs.is_empty());
    let mut official = rule_record(5, 60, Some("65000"));
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_with_reference_v1(
        &core,
        &mut official,
        &ids,
        &[],
        &market(),
        |_| panic!("正式参考已准备，不应再读取候选页"),
    )
    .unwrap();
    let result = crate::without_identity_encoding(|| core.push(&prepared)).unwrap();
    assert!(matches!(
        result.applied.closed_groups[0].observations[0].effective_reference,
        EffectiveMarketReferenceV1::OfficialAnchor(_)
    ));
}

#[test]
fn object_counts_separate_quantity_evidence_selection_and_are_read_only() {
    let mut core = core();
    assert_eq!(core.object_counts().markets, 0);
    push(&mut core, &meta(0));
    push(&mut core, &pm(1, 1, 21_000, book("100")));
    let before = core.object_counts();
    assert_eq!(before.markets, 1);
    assert_eq!(before.assets, 2);
    assert_eq!(before.pm_quantity_keys, 2);
    assert_eq!(before.pm_level_evidence_keys, 2);
    assert_eq!(before.pm_selection_keys, 2);
    assert_eq!(before.pending_terminal_keys, 0);
    assert_eq!(
        before.original_retained_state_entries,
        core.retained_state_entries()
    );
    assert_eq!(core.object_counts(), before);
    push(
        &mut core,
        &pm(
            2,
            2,
            21_001,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("0"))],
            },
        ),
    );
    let after = core.object_counts();
    assert_eq!(after.pm_quantity_keys, 1);
    assert_eq!(after.pm_absence_keys, 1);
    assert_eq!(after.pm_selection_keys, 2, "选中零量仍有自己的版本键");
}

#[test]
fn original_input_identity_and_position_are_checked_before_core_mutation() {
    let mut core = core();
    let before = core.applied_prefix();
    let mut wrong = meta(0);
    wrong.input_stream_id = "another-stream".into();
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_v1(&core, &wrong, &ids, &[]).unwrap();
    assert!(core.push(&prepared).is_err());
    assert_eq!(core.applied_prefix(), before);
    assert_eq!(core.fixed_markets().count(), 0);
    let wrong = meta(1);
    let prepared = prepare_public_ingress_v1(&core, &wrong, &ids, &[]).unwrap();
    assert!(core.push(&prepared).is_err());
    assert_eq!(core.applied_prefix(), before);
    push(&mut core, &meta(0));
    assert_eq!(core.applied_prefix().next_input_position, 1);
}

#[test]
fn old_public_header_ignores_removed_capacity_field_and_new_header_omits_it() {
    let header = core().header().clone();
    let mut old = serde_json::to_value(&header).unwrap();
    assert!(old.get("max_retained_reducer_state_entries").is_none());
    old["max_retained_reducer_state_entries"] = serde_json::json!(163840);
    assert_eq!(
        serde_json::from_value::<PublicInputStreamHeaderV1>(old).unwrap(),
        header
    );
}

#[test]
fn valuation_bid_selects_public_versions_without_rewriting_direct_bba() {
    let mut core = core();
    let first = push(&mut core, &meta(0));
    assert_eq!(
        up(&first.closed_groups[0].observations[0])
            .valuation_best_bid
            .value,
        None
    );
    let full = push(&mut core, &pm(1, 1, 21_000, book("1")));
    let asset = up(&full.closed_groups[0].observations[0]);
    assert_eq!(
        asset.valuation_best_bid.value,
        Some(Some(ExactDecimal("0.5".into())))
    );
    assert!(asset.best_bid_ask.value.is_none(), "FullBook不重写直接BBA");
    let quote = |bid: Option<&str>| PolymarketOrderbookEvent::BestBidAsk {
        asset_id: UP_TOKEN_ID.into(),
        best_bid: bid.map(str::to_owned),
        best_ask: Some("0.6".into()),
        spread: None,
    };
    let direct = push(&mut core, &pm(2, 2, 23_000, quote(Some("0.4"))));
    assert_eq!(
        up(&direct.closed_groups[0].observations[0])
            .valuation_best_bid
            .value,
        Some(Some(ExactDecimal("0.4".into())))
    );
    // 旧全簿会改变部分合法L2事实，但不能覆盖更新的报价。
    let old = push(&mut core, &pm(3, 3, 22_000, book("2")));
    assert_eq!(
        up(&old.closed_groups[0].observations[0])
            .valuation_best_bid
            .value,
        Some(Some(ExactDecimal("0.4".into())))
    );
    let lower = push(
        &mut core,
        &pm(
            4,
            4,
            24_000,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![PolymarketPriceChangeEntry {
                    asset_id: UP_TOKEN_ID.into(),
                    side: PolymarketSide::Buy,
                    price: "0.1".into(),
                    size: Some("3".into()),
                    best_bid: None,
                    best_ask: None,
                }],
            },
        ),
    );
    assert_eq!(
        up(&lower.closed_groups[0].observations[0])
            .valuation_best_bid
            .value,
        Some(Some(ExactDecimal("0.4".into())))
    );
    let empty = push(&mut core, &pm(5, 5, 25_000, quote(None)));
    assert_eq!(
        up(&empty.closed_groups[0].observations[0])
            .valuation_best_bid
            .value,
        Some(None)
    );
    let stale = push(&mut core, &pm(6, 6, 24_500, quote(Some("0.5"))));
    assert_eq!(
        up(&stale.closed_groups[0].observations[0])
            .valuation_best_bid
            .value,
        Some(None)
    );
    // 新代际独立BBA合法，即使完整L2仍Missing。
    let reset = push(
        &mut core,
        &record(
            7,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-b",
                1,
                20_000,
                25_000,
                quote(Some("0.4")),
            ))),
        ),
    );
    let asset = up(&reset.closed_groups[0].observations[0]);
    assert_eq!(asset.bids.quality, MarketDataQuality::Missing);
    assert_eq!(asset.valuation_best_bid.quality, MarketDataQuality::Fresh);
    assert_eq!(
        asset.valuation_best_bid.value,
        Some(Some(ExactDecimal("0.4".into())))
    );
    // 更新FullBook-only的空买盘替换旧BBA标记，但BBA原字段保持。
    let mut empty_book = book("1");
    if let PolymarketOrderbookEvent::Book { bids, .. } = &mut empty_book {
        bids.clear();
    }
    let full_empty = push(
        &mut core,
        &record(
            8,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-b", 2, 26_000, 26_001, empty_book,
            ))),
        ),
    );
    let asset = up(&full_empty.closed_groups[0].observations[0]);
    assert_eq!(asset.valuation_best_bid.value, Some(None));
    assert_eq!(
        asset
            .best_bid_ask
            .value
            .as_ref()
            .unwrap()
            .best_bid_pusd_per_share,
        Some(ExactDecimal("0.4".into()))
    );
    let direct_after_empty = push(
        &mut core,
        &record(
            9,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-b",
                3,
                27_000,
                27_001,
                quote(Some("0.4")),
            ))),
        ),
    );
    assert_eq!(
        up(&direct_after_empty.closed_groups[0].observations[0])
            .valuation_best_bid
            .value,
        Some(Some(ExactDecimal("0.4".into())))
    );
    let low_delete = push(
        &mut core,
        &record(
            10,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-b",
                4,
                28_000,
                28_001,
                PolymarketOrderbookEvent::PriceChange {
                    price_changes: vec![PolymarketPriceChangeEntry {
                        asset_id: UP_TOKEN_ID.into(),
                        side: PolymarketSide::Buy,
                        price: "0.1".into(),
                        size: Some("0".into()),
                        best_bid: None,
                        best_ask: None,
                    }],
                },
            ))),
        ),
    );
    assert_eq!(
        up(&low_delete.closed_groups[0].observations[0])
            .valuation_best_bid
            .value,
        Some(Some(ExactDecimal("0.4".into()))),
        "低价删除不能把旧空盘当成更新的全局无bid证明"
    );
    let missing_book = push(
        &mut core,
        &record(
            11,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-c",
                1,
                29_000,
                29_001,
                PolymarketOrderbookEvent::LastTradePrice {
                    asset_id: UP_TOKEN_ID.into(),
                    price: "0.5".into(),
                    size: None,
                    side: None,
                    fee_rate_bps: None,
                },
            ))),
        ),
    );
    let asset = up(&missing_book.closed_groups[0].observations[0]);
    assert_eq!(asset.bids.quality, MarketDataQuality::Missing);
    assert_eq!(
        asset.valuation_best_bid.value,
        Some(Some(ExactDecimal("0.4".into()))),
        "L2换代不否决正式保留的独立BBA"
    );
}

#[test]
fn unrelated_high_delete_cannot_promote_old_l2_over_new_direct_bid() {
    let mut core = core();
    push(&mut core, &meta(0));
    let mut full = book("1");
    if let PolymarketOrderbookEvent::Book { bids, .. } = &mut full {
        bids.push(PolymarketPriceLevel {
            price: "0.3".into(),
            size: "1".into(),
        });
    }
    push(&mut core, &pm(1, 1, 21_000, full));
    push(
        &mut core,
        &pm(
            2,
            2,
            23_000,
            PolymarketOrderbookEvent::BestBidAsk {
                asset_id: UP_TOKEN_ID.into(),
                best_bid: Some("0.4".into()),
                best_ask: Some("0.6".into()),
                spread: None,
            },
        ),
    );
    for (position, source, price, size, expected) in [
        (3, 24_000, "0.9", "0", "0.4"), // 正式Review反例：删除未曾存在的高档不证明首档更新。
        (4, 25_000, "0.5", "0", "0.4"), // 原L2首档变化，也未撤销独立0.4。
        (5, 26_000, "0.4", "2", "0.4"), // 本价位有效数量确认保留直接价格。
        (6, 27_000, "0.4", "0", "0.3"), // 本价位真实删除后采用原L2实际剩余首档。
        (7, 28_000, "0.55", "1", "0.55"), // 新首档自身的来源版本能支持更新价格。
    ] {
        let result = push(
            &mut core,
            &pm(
                position,
                position,
                source,
                PolymarketOrderbookEvent::PriceChange {
                    price_changes: vec![PolymarketPriceChangeEntry {
                        asset_id: UP_TOKEN_ID.into(),
                        price: price.into(),
                        size: Some(size.into()),
                        side: PolymarketSide::Buy,
                        best_bid: None,
                        best_ask: None,
                    }],
                },
            ),
        );
        let asset = up(&result.closed_groups[0].observations[0]);
        assert_eq!(
            asset.valuation_best_bid.value,
            Some(Some(ExactDecimal(expected.into()))),
            "source {source}"
        );
        assert_eq!(
            asset
                .best_bid_ask
                .value
                .as_ref()
                .unwrap()
                .best_bid_pusd_per_share,
            Some(ExactDecimal("0.4".into())),
            "不改直接BBA字段"
        );
    }
}

#[test]
fn lower_quantity_update_cannot_refresh_direct_bid_over_new_high_bid() {
    for changes in [
        vec![
            (3, 24_000, "0.5", "2", "0.5"),
            (4, 25_000, "0.4", "1", "0.5"), // 正式Review原序列。
        ],
        vec![
            (3, 24_000, "0.4", "0", "0.3"), // 本价位删除不让被BBA排除的旧0.5复活。
            (4, 25_000, "0.5", "2", "0.5"), // 新的高价本档证据胜过旧直接BBA。
            (5, 26_000, "0.4", "1", "0.5"), // 低档数量不能刷新整体最优结论。
        ],
    ] {
        let mut core = core();
        push(&mut core, &meta(0));
        let mut full = book("1");
        if let PolymarketOrderbookEvent::Book { bids, .. } = &mut full {
            bids.push(PolymarketPriceLevel {
                price: "0.3".into(),
                size: "1".into(),
            });
        }
        push(&mut core, &pm(1, 1, 21_000, full));
        push(
            &mut core,
            &pm(
                2,
                2,
                23_000,
                PolymarketOrderbookEvent::BestBidAsk {
                    asset_id: UP_TOKEN_ID.into(),
                    best_bid: Some("0.4".into()),
                    best_ask: Some("0.6".into()),
                    spread: None,
                },
            ),
        );
        for (position, source, price, size, expected) in changes {
            let result = push(
                &mut core,
                &pm(
                    position,
                    position,
                    source,
                    PolymarketOrderbookEvent::PriceChange {
                        price_changes: vec![PolymarketPriceChangeEntry {
                            asset_id: UP_TOKEN_ID.into(),
                            price: price.into(),
                            size: Some(size.into()),
                            side: PolymarketSide::Buy,
                            best_bid: None,
                            best_ask: None,
                        }],
                    },
                ),
            );
            let asset = up(&result.closed_groups[0].observations[0]);
            assert_eq!(
                asset.valuation_best_bid.value,
                Some(Some(ExactDecimal(expected.into()))),
                "source {source}"
            );
            assert_eq!(
                asset
                    .best_bid_ask
                    .value
                    .as_ref()
                    .unwrap()
                    .best_bid_pusd_per_share,
                Some(ExactDecimal("0.4".into())),
                "不改直接BBA字段"
            );
        }
    }
}

#[test]
fn range_received_fast_path_matches_full_source_and_original_identity() {
    let mut spot = twap_input(60).common;
    spot.schema_version = POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into();
    spot.topic = "crypto_prices_chainlink".into();
    let rows = vec![
        TypedRawRow::PolymarketOrderbook(row("epoch", 1, 1000, 2000, book("1"))),
        TypedRawRow::PolymarketMarketMetadata(Box::new(metadata_snapshot_row())),
        TypedRawRow::PolymarketRtdsChainlinkBtcUsd(spot),
        TypedRawRow::BinanceDepthSnapshot(snapshot("epoch", 100, 1)),
        TypedRawRow::BinanceDepthUpdate(update(
            "epoch",
            101,
            101,
            2,
            Some(100),
            Some(100),
            BinanceDepthStitchVerdict::Applied,
            vec![level("64000", "1")],
        )),
        TypedRawRow::BinanceAggTrade(trade(100, 1, "64000")),
    ];
    let digest = Sha256Hex("a".repeat(64));
    for row in rows {
        row.validate().unwrap();
        // 独立旧 V2 构造与原 V3 编码入口保留全部正式字段/身份，不只比接收时间。
        let old = crate::source_evidence_from_typed_raw(&row, &digest, &digest).unwrap();
        let input = TypedRawRowV2::Existing(row);
        let full = source_evidence_v3_from_typed_raw_v2(&input).unwrap();
        assert_eq!(full, source_evidence_v3_from_existing(&old).unwrap());
        assert_eq!(
            validated_received_at_from_typed_raw_v2(&input).unwrap(),
            full.received_at_utc_ms
        );
    }
    let v6 = TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
        crate::public_input::PolymarketMarketMetadataV6Input::Existing(metadata_snapshot_row()),
    ));
    assert_eq!(
        source_evidence_v3_from_typed_raw_v2(&v6).unwrap(),
        source_evidence_v3_from_typed_raw_v2(&TypedRawRowV2::Existing(
            TypedRawRow::PolymarketMarketMetadata(Box::new(metadata_snapshot_row()))
        ))
        .unwrap()
    );
    assert_eq!(
        validated_received_at_from_typed_raw_v2(&v6).unwrap(),
        source_evidence_v3_from_typed_raw_v2(&v6)
            .unwrap()
            .received_at_utc_ms
    );
}

#[test]
fn range_received_fast_path_avoids_final_encoding_but_retains_semantic_errors() {
    let input = TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
        "epoch",
        1,
        1000,
        2000,
        book("1"),
    )));
    let received =
        crate::without_identity_encoding(|| validated_received_at_from_typed_raw_v2(&input))
            .unwrap();
    assert_eq!(received, 2000);
    assert!((2000..3000).contains(&received));
    assert!(!(1000..2000).contains(&received));
    let TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(mut bad)) = input else {
        unreachable!()
    };
    bad.timestamp = None;
    bad.validate().unwrap();
    let bad = TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(bad));
    assert_eq!(
        validated_received_at_from_typed_raw_v2(&bad),
        source_evidence_v3_from_typed_raw_v2(&bad).map(|s| s.received_at_utc_ms)
    );
}

#[test]
fn marketless_rtds_input_exposes_only_its_current_complete_spot() {
    let mut core = core();
    let mut saved = None;
    for (position, status, price) in [
        (0, RtdsFreshnessStatus::Fresh, "65000"),
        (1, RtdsFreshnessStatus::Stale, "65001"),
    ] {
        let mut row = twap_input(60).common;
        row.schema_version = POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into();
        row.topic = "crypto_prices_chainlink".into();
        row.value = price.into();
        row.freshness_status = status;
        row.timestamp_received += position as i64;
        row.payload_timestamp = row.timestamp_received - 2;
        row.age_ms = Some(2);
        let applied = push(
            &mut core,
            &record(
                position,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row)),
            ),
        );
        assert_eq!(applied.closed_groups.len(), 1);
        let group = &applied.closed_groups[0];
        assert!(group.observations.is_empty());
        let current = core.current_chainlink_spot();
        assert_eq!(current.value.as_ref().unwrap().price_usd_per_btc.0, price);
        assert_eq!(
            current.direct_anchors[0].position,
            group.records[0].position
        );
        if position == 0 {
            saved = Some(current.clone());
        } else {
            assert_ne!(saved.as_ref().unwrap(), current);
        }
    }
    assert_eq!(saved.unwrap().value.unwrap().price_usd_per_btc.0, "65000");
    let end = RecordedIngressV1 {
        input_stream_id: "core-test".into(),
        input_position: 2,
        ingress: DptIngressV1::EndOfInput,
        dependency_record_refs: vec![],
    };
    assert!(push(&mut core, &end).closed_groups.is_empty());
}

#[test]
fn opted_progress_is_asset_scoped_ignores_retry_and_keeps_unknown_connection() {
    let mut core = core();
    core.enable_source_progress();
    push(&mut core, &meta(0));
    let first = row("epoch-a", 1, 1000, 2000, book("100"));
    let a = push(
        &mut core,
        &record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(first.clone())),
        ),
    );
    let progress = a.closed_groups[0].observations[0].source_progress[0].clone();
    assert_eq!(progress.connection_generation, None);
    assert_eq!(progress.scope.asset_or_symbol, UP_TOKEN_ID);
    let duplicate = push(
        &mut core,
        &record(
            2,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(first)),
        ),
    );
    assert!(duplicate.closed_groups.is_empty());
    let mut other = row("epoch-a", 2, 900, 3000, book("100"));
    if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut other.event {
        *asset_id = DOWN_TOKEN_ID.into();
    }
    let b = push(
        &mut core,
        &record(
            3,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(other)),
        ),
    );
    let scopes = &b.closed_groups[0].observations[0].source_progress;
    assert_eq!(scopes.len(), 2);
    assert_eq!(
        scopes
            .iter()
            .find(|s| s.scope.asset_or_symbol == UP_TOKEN_ID)
            .unwrap(),
        &progress
    );
    let lagged = scopes
        .iter()
        .find(|s| s.scope.asset_or_symbol == DOWN_TOKEN_ID)
        .unwrap()
        .last_valid_business_message
        .as_ref()
        .unwrap();
    assert_eq!(lagged.source_event_at_utc_ms, Some(900));
    assert_eq!(lagged.received_at_utc_ms, 3000);
}

#[test]
fn invalid_rtds_and_pong_never_refresh_valid_stream_progress() {
    let mut core = core();
    core.enable_source_progress();
    push(&mut core, &meta(0));
    let mut row = twap_input(60).common;
    row.schema_version = POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into();
    row.topic = "crypto_prices_chainlink".into();
    let first = push(
        &mut core,
        &record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row.clone())),
        ),
    );
    let progress = first.closed_groups[0].observations[0]
        .source_progress
        .clone();
    row.timestamp_received += 1000;
    row.message_type = "PONG".into();
    row.freshness_status = RtdsFreshnessStatus::InvalidType;
    let pong = push(
        &mut core,
        &record(
            2,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row.clone())),
        ),
    );
    assert_eq!(
        pong.closed_groups[0].observations[0].source_progress,
        progress
    );
    row.timestamp_received += 1000;
    row.message_type = "update".into();
    row.symbol = "eth/usd".into();
    row.freshness_status = RtdsFreshnessStatus::InvalidSymbol;
    let other = push(
        &mut core,
        &record(
            3,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row)),
        ),
    );
    assert_eq!(
        other.closed_groups[0].observations[0].source_progress,
        progress
    );
}

#[test]
fn source_epoch_resync_requires_each_asset_once_without_resetting_already_rebuilt_asset() {
    let mut core = core();
    core.enable_source_progress();
    push(&mut core, &meta(0));
    let mut position = 1;
    for epoch in ["epoch-a", "epoch-b"] {
        for (i, token) in [UP_TOKEN_ID, DOWN_TOKEN_ID].into_iter().enumerate() {
            let mut message = row(
                epoch,
                i as u64 + 1,
                1000 + position as i64,
                2000 + position as i64,
                book("100"),
            );
            if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut message.event {
                *asset_id = token.into();
            }
            let output = push(
                &mut core,
                &record(
                    position,
                    TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(message)),
                ),
            );
            let scopes = &output.closed_groups[0].observations[0].source_progress;
            let current = scopes
                .iter()
                .find(|p| p.scope.asset_or_symbol == token)
                .unwrap();
            assert_eq!(
                current.last_valid_business_message.as_ref().unwrap(),
                &current.last_observed_message,
                "当前有效快照推进业务证据，不能被代际观察提前标成已处理"
            );
            assert_eq!(
                current.last_observed_message.received_at_utc_ms,
                2000 + position as i64
            );
            if epoch == "epoch-b" && i == 0 {
                let other = scopes
                    .iter()
                    .find(|p| p.scope.asset_or_symbol == DOWN_TOKEN_ID)
                    .unwrap();
                assert_eq!(
                    other.synchronization,
                    SourceSynchronizationV1::SnapshotRequired
                );
                assert_eq!(
                    other
                        .last_valid_business_message
                        .as_ref()
                        .unwrap()
                        .received_at_utc_ms,
                    2002,
                    "代际变化不能替其他资产续期"
                );
                assert_eq!(other.last_observed_message, current.last_observed_message);
            }
            if i == 1 {
                assert!(scopes.iter().all(|p| matches!(
                    p.synchronization,
                    SourceSynchronizationV1::SnapshotObserved { .. }
                )));
            }
            position += 1;
        }
    }
}

#[test]
fn compact_progress_matches_complete_groups_through_isolation_retry_and_resync() {
    let mut full = core();
    let mut compact = core();
    full.enable_source_progress();
    let mut rows = vec![meta(0)];
    let mut add = |r| rows.push(record(rows.len() as u64, TypedRawRowV2::Existing(r)));
    for (epoch, sequence, token, received) in [
        ("epoch-a", 1, UP_TOKEN_ID, 21_100),
        ("epoch-a", 1, UP_TOKEN_ID, 21_100), // exact retry
        ("epoch-a", 2, DOWN_TOKEN_ID, 21_200),
        ("epoch-a", 3, UP_TOKEN_ID, 21_300), // equal book, new evidence
        ("epoch-b", 1, UP_TOKEN_ID, 21_400), // other asset needs snapshot
        ("epoch-b", 2, DOWN_TOKEN_ID, 21_500),
    ] {
        let mut r = row(epoch, sequence, 20_000, received, book("100"));
        if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut r.event {
            *asset_id = token.into();
        }
        add(TypedRawRow::PolymarketOrderbook(r));
    }
    let mut spot = twap_input(60).common;
    spot.schema_version = POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into();
    spot.topic = "crypto_prices_chainlink".into();
    add(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(spot.clone()));
    spot.timestamp_received += 1;
    add(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(spot.clone()));
    spot.message_type = "PONG".into();
    spot.freshness_status = RtdsFreshnessStatus::InvalidType;
    add(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(spot.clone()));
    spot.message_type = "update".into();
    spot.symbol = "eth/usd".into();
    spot.freshness_status = RtdsFreshnessStatus::InvalidSymbol;
    add(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(spot));
    add(TypedRawRow::BinanceDepthSnapshot(snapshot(
        "depth-a", 100, 1,
    )));
    add(TypedRawRow::BinanceDepthUpdate(update(
        "depth-a",
        101,
        101,
        2,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![level("64100", "0.2")],
    )));
    add(TypedRawRow::BinanceDepthUpdate(update(
        "depth-a",
        104,
        104,
        3,
        Some(100),
        Some(101),
        BinanceDepthStitchVerdict::NotAppliedConfirmedHole,
        vec![level("64100", "0.9")],
    )));
    add(TypedRawRow::BinanceDepthSnapshot(snapshot(
        "depth-b", 200, 1,
    )));
    add(TypedRawRow::BinanceAggTrade(invalid_trade(1)));
    rows.push(RecordedIngressV1 {
        input_stream_id: "core-test".into(),
        input_position: rows.len() as u64,
        ingress: DptIngressV1::EndOfInput,
        dependency_record_refs: vec![],
    });
    let mut reconstructed = BTreeMap::new();
    let mut total_deltas = 0;
    for input in &rows {
        let full_prepared = prepare_public_ingress_v1(&full, input, &BTreeMap::new(), &[]).unwrap();
        let compact_prepared =
            prepare_public_ingress_v1(&compact, input, &BTreeMap::new(), &[]).unwrap();
        let full_applied = full.push(&full_prepared).unwrap();
        let compact_applied = compact.push_source_progress(&compact_prepared).unwrap();
        assert_eq!(
            full_applied.applied.applied_prefix,
            compact_applied.applied_prefix
        );
        assert_eq!(
            full_applied.applied.closed_groups.len(),
            compact_applied.closed_groups.len()
        );
        for (f, c) in full_applied
            .applied
            .closed_groups
            .iter()
            .zip(&compact_applied.closed_groups)
        {
            assert_eq!(f.group_id, c.group_id);
            assert_eq!(f.group_position, c.group_position);
            assert_eq!(f.member_input_positions, c.member_input_positions);
            assert_eq!(f.closed_at_input_position, c.closed_at_input_position);
            // Independent old formula, including collector admission later than receive.
            let available = match &input.ingress {
                DptIngressV1::Message {
                    source_evidence, ..
                } => source_evidence
                    .collector_admission
                    .as_ref()
                    .map_or(source_evidence.received_at_utc_ms, |a| {
                        a.admitted_at_utc_ms.max(source_evidence.received_at_utc_ms)
                    }),
                _ => i64::MIN,
            };
            assert_eq!(
                c.visible_at_utc_ms,
                f.records
                    .iter()
                    .map(|v| v.dependency_time.received_at_utc_ms)
                    .chain([available])
                    .max()
                    .unwrap()
            );
            for p in &c.source_progress_changes {
                reconstructed.insert(p.scope.clone(), p.clone());
            }
            total_deltas += c.source_progress_changes.len();
            assert_eq!(
                reconstructed
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
                    .as_slice(),
                &*f.observations[0].source_progress
            );
            if input.input_position == 0 {
                assert_eq!(c.market_bindings[0].market, market());
                assert_eq!(
                    c.market_bindings[0].assets,
                    f.observations[0]
                        .assets
                        .iter()
                        .map(|a| a.asset.clone())
                        .collect::<Vec<_>>()
                );
            } else {
                assert!(c.market_bindings.is_empty());
            }
            if input.input_position == 5 {
                assert_eq!(
                    c.source_progress_changes.len(),
                    2,
                    "epoch changes include the untouched asset"
                );
                assert_eq!(
                    reconstructed
                        .values()
                        .find(|p| p.scope.asset_or_symbol == DOWN_TOKEN_ID)
                        .unwrap()
                        .synchronization,
                    SourceSynchronizationV1::SnapshotRequired
                );
            }
            if matches!(input.input_position, 9 | 10) {
                assert!(c.source_progress_changes.is_empty());
            }
        }
    }
    assert_eq!(full.object_counts(), compact.object_counts());
    assert!(total_deltas < rows.len() * reconstructed.len());
}

#[test]
fn thin_reference_facts_match_full_actual_reductions() {
    let mut full = core();
    let mut thin = core();
    for input in [meta(0), twap_record(1, 60, 22001, "100000000000000000000")] {
        let ids = BTreeMap::new();
        let prepared = prepare_public_ingress_v1(&full, &input, &ids, &[]).unwrap();
        let expected = full.push(&prepared).unwrap().reductions;
        let prepared = prepare_public_ingress_v1(&thin, &input, &ids, &[]).unwrap();
        let actual = thin.push_reference_facts(&prepared).unwrap();
        let facts = |batches: Vec<PublicReductionBatchV1>| {
            batches
                .into_iter()
                .flat_map(|b| b.facts.iter().cloned().collect::<Vec<_>>())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            serde_json::to_value(facts(expected)).unwrap(),
            serde_json::to_value(facts(actual)).unwrap()
        );
    }
}
