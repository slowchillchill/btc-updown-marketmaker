use data_collect::ValidationVerdict;
use data_collect::event::{
    EventEnvelope, HostClockSyncStatus, RawEvidence, SourceKind, SourceTimestamp,
};
use data_collect::source_format::{
    BinanceAggTradeRaw, BinanceDepthLevel, BinanceDepthSnapshotRaw, BinanceDepthUpdateRaw,
    MarketLifecycleState, PolymarketMarketMetadataRaw, PolymarketMarketRaw,
    PolymarketMarketResolutionRaw, PolymarketSubscriptionEvidence, RtdsChainlinkBtcUsdRaw,
    SubscriptionConfirmation,
};

fn envelope(source_kind: SourceKind, event_type: &str, raw_json: &str) -> EventEnvelope {
    EventEnvelope {
        source_kind,
        source_event_type: event_type.to_owned(),
        source_identifier: "source-id".to_owned(),
        source_timestamp: SourceTimestamp::Timestamp {
            value: "1766789469958".to_owned(),
        },
        recv_time_utc_ms: Some(1_766_789_469_999),
        collector_host_id: "collector-host".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "source-endpoint".to_owned(),
        host_clock_sync_status: HostClockSyncStatus::Synchronized {
            source: "chrony".to_owned(),
        },
        raw_evidence: RawEvidence::Json(raw_json.to_owned()),
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: source_kind,
    }
}

fn subscription() -> PolymarketSubscriptionEvidence {
    PolymarketSubscriptionEvidence {
        raw_json:
            r#"{"type":"market","assets_ids":["asset-1","asset-2"],"custom_feature_enabled":true}"#
                .to_owned(),
        assets_ids: vec!["asset-1".to_owned(), "asset-2".to_owned()],
        custom_feature_enabled: true,
        confirmation: SubscriptionConfirmation::FirstBusinessEvent {
            event_type: "book".to_owned(),
        },
        sdk_support_status: "supported".to_owned(),
    }
}

#[test]
fn polymarket_market_schema_requires_subscription_and_platform_timestamp() {
    let market = PolymarketMarketRaw {
        envelope: envelope(
            SourceKind::PolymarketMarket,
            "book",
            r#"{"event_type":"book","timestamp":"1766789469958","asset_id":"asset-1"}"#,
        ),
        event_type: "book".to_owned(),
        platform_timestamp: "1766789469958".to_owned(),
        market_id: Some("market-1".to_owned()),
        condition_id: Some("condition-1".to_owned()),
        asset_ids: vec!["asset-1".to_owned(), "asset-2".to_owned()],
        subscription: subscription(),
    };

    assert_eq!(
        market.validate_contract().verdict,
        ValidationVerdict::Complete
    );

    let mut missing_custom_feature = market;
    missing_custom_feature.subscription.custom_feature_enabled = false;
    let report = missing_custom_feature.validate_contract();
    assert_eq!(report.verdict, ValidationVerdict::Incomplete);
    assert!(
        report
            .source_gaps
            .contains(&"custom_feature_enabled_missing".to_owned())
    );
}

#[test]
fn polymarket_market_resolved_schema_records_retired_event_predicate_fields() {
    let resolved = PolymarketMarketResolutionRaw {
        envelope: envelope(
            SourceKind::PolymarketMarket,
            "market_resolved",
            r#"{"event_type":"market_resolved","market":"condition-1","asset_id":"asset-1"}"#,
        ),
        condition_id: "condition-1".to_owned(),
        market_id: Some("market-1".to_owned()),
        subscribed_asset_ids_for_market: vec!["asset-1".to_owned(), "asset-2".to_owned()],
        resolved_event_asset_ids: vec!["asset-1".to_owned(), "asset-2".to_owned()],
        winning_asset_id: "asset-1".to_owned(),
        winning_outcome: "Up".to_owned(),
        close_reason_if_present: None,
    };

    assert_eq!(
        resolved.validate_contract().verdict,
        ValidationVerdict::Complete
    );

    let mut incomplete = resolved;
    incomplete.resolved_event_asset_ids = vec!["asset-1".to_owned()];
    let report = incomplete.validate_contract();
    assert_eq!(report.verdict, ValidationVerdict::Incomplete);
    assert!(
        report
            .source_gaps
            .contains(&"resolved_assets_do_not_cover_subscription".to_owned())
    );

    let extra_winner = PolymarketMarketResolutionRaw {
        envelope: envelope(
            SourceKind::PolymarketMarket,
            "market_resolved",
            r#"{"event_type":"market_resolved","market":"condition-1","asset_id":"asset-3"}"#,
        ),
        condition_id: "condition-1".to_owned(),
        market_id: Some("market-1".to_owned()),
        subscribed_asset_ids_for_market: vec!["asset-1".to_owned(), "asset-2".to_owned()],
        resolved_event_asset_ids: vec![
            "asset-1".to_owned(),
            "asset-2".to_owned(),
            "asset-3".to_owned(),
        ],
        winning_asset_id: "asset-3".to_owned(),
        winning_outcome: "Other".to_owned(),
        close_reason_if_present: None,
    };
    let report = extra_winner.validate_contract();
    assert_eq!(report.verdict, ValidationVerdict::Incomplete);
    assert!(
        report
            .source_gaps
            .contains(&"winning_asset_not_in_subscribed_assets".to_owned())
    );
}

#[test]
fn polymarket_metadata_schema_rejects_slug_only_discovery() {
    let metadata = PolymarketMarketMetadataRaw {
        envelope: envelope(
            SourceKind::PolymarketMarketMetadata,
            "new_market",
            r#"{"event_type":"new_market","condition_id":"condition-1"}"#,
        ),
        condition_id: "condition-1".to_owned(),
        asset_ids: vec!["asset-1".to_owned(), "asset-2".to_owned()],
        clob_market_id: "clob-1".to_owned(),
        slug: "btc-updown-5m-1766789400".to_owned(),
        start_time_utc_ms: 1_766_789_400_000,
        end_time_utc_ms: 1_766_789_700_000,
        active: true,
        accepting_orders: true,
        market_lifecycle_state: MarketLifecycleState::Active,
        tick_size: "0.01".to_owned(),
        min_size: "1".to_owned(),
        fee_raw: Some(r#"{"maker":0}"#.to_owned()),
        reward_raw: Some(r#"{"eligible":true}"#.to_owned()),
        candidate_slug: "btc-updown-5m-1766789400".to_owned(),
        active_scan_evidence: Some("gamma active scan row digest".to_owned()),
        metadata_snapshot_evidence: Some("metadata snapshot digest".to_owned()),
        clock_drift_evidence: Some("host clock drift 2ms".to_owned()),
    };

    assert_eq!(
        metadata.validate_contract().verdict,
        ValidationVerdict::Complete
    );

    let mut slug_only = metadata;
    slug_only.active_scan_evidence = None;
    slug_only.metadata_snapshot_evidence = None;
    let report = slug_only.validate_contract();
    assert_eq!(report.verdict, ValidationVerdict::Incomplete);
    assert!(
        report
            .source_gaps
            .contains(&"slug_only_discovery".to_owned())
    );
}

#[test]
fn binance_depth_update_and_snapshot_schema_keep_official_fields() {
    let update = BinanceDepthUpdateRaw {
        envelope: envelope(
            SourceKind::BinanceDepthUpdate,
            "depthUpdate",
            r#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":10,"u":12,"b":[],"a":[]}"#,
        ),
        event_type: "depthUpdate".to_owned(),
        event_time_ms: 1,
        symbol: "BTCUSDT".to_owned(),
        stream_name: "btcusdt@depth".to_owned(),
        first_update_id: 10,
        final_update_id: 12,
        bids: vec![BinanceDepthLevel {
            price: "100".to_owned(),
            quantity: "1".to_owned(),
        }],
        asks: vec![BinanceDepthLevel {
            price: "101".to_owned(),
            quantity: "2".to_owned(),
        }],
        combined_stream: None,
        local_u: Some(12),
        stitch_evidence: Some("U <= lastUpdateId + 1 <= u".to_owned()),
    };

    assert_eq!(
        update.validate_contract().verdict,
        ValidationVerdict::Complete
    );

    let mut one_sided_update = update.clone();
    one_sided_update.asks.clear();
    assert_eq!(
        one_sided_update.validate_contract().verdict,
        ValidationVerdict::Complete
    );

    let mut empty_update = update.clone();
    empty_update.bids.clear();
    empty_update.asks.clear();
    let report = empty_update.validate_contract();
    assert_eq!(report.verdict, ValidationVerdict::Incomplete);
    assert!(report.missing_fields.contains(&"b_or_a".to_owned()));

    let mut missing_stream = update.clone();
    missing_stream.stream_name.clear();
    assert_eq!(
        missing_stream.validate_contract().verdict,
        ValidationVerdict::Incomplete
    );

    let mut combined_wrapper = update;
    combined_wrapper.combined_stream = Some("btcusdt@depth".to_owned());
    assert_eq!(
        combined_wrapper.validate_contract().verdict,
        ValidationVerdict::Complete
    );

    let mut blank_combined_wrapper = combined_wrapper;
    blank_combined_wrapper.combined_stream = Some("  ".to_owned());
    assert_eq!(
        blank_combined_wrapper.validate_contract().verdict,
        ValidationVerdict::Incomplete
    );

    let snapshot = BinanceDepthSnapshotRaw {
        envelope: EventEnvelope {
            source_timestamp: SourceTimestamp::NoSourceTimestamp {
                reason: "rest_depth_snapshot_has_no_exchange_timestamp".to_owned(),
                snapshot_boundary: "snapshot_fetch_start_utc_ms=1 snapshot_recv_time_utc_ms=2"
                    .to_owned(),
            },
            ..envelope(
                SourceKind::BinanceDepthSnapshot,
                "depth_snapshot",
                r#"{"lastUpdateId":12,"bids":[],"asks":[]}"#,
            )
        },
        last_update_id: 12,
        bids: vec![BinanceDepthLevel {
            price: "100".to_owned(),
            quantity: "1".to_owned(),
        }],
        asks: vec![BinanceDepthLevel {
            price: "101".to_owned(),
            quantity: "2".to_owned(),
        }],
        snapshot_fetch_start_utc_ms: 1,
        snapshot_recv_time_utc_ms: 2,
        official_rest_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT".to_owned(),
        no_exchange_timestamp_evidence: "REST snapshot has no exchange timestamp".to_owned(),
    };

    assert_eq!(
        snapshot.validate_contract().verdict,
        ValidationVerdict::Complete
    );
}

#[test]
fn binance_aggtrade_schema_keeps_full_trade_fields() {
    let trade = BinanceAggTradeRaw {
        envelope: envelope(
            SourceKind::BinanceAggTrade,
            "aggTrade",
            r#"{"e":"aggTrade","E":1,"s":"BTCUSDT","a":2,"p":"100","q":"0.1","f":3,"l":4,"T":5,"m":true,"M":false}"#,
        ),
        event_type: "aggTrade".to_owned(),
        event_time_ms: 1,
        symbol: "BTCUSDT".to_owned(),
        aggregate_trade_id: 2,
        price: "100".to_owned(),
        quantity: "0.1".to_owned(),
        first_trade_id: 3,
        last_trade_id: 4,
        trade_time_ms: 5,
        buyer_is_maker: true,
        ws_ignore: false,
        stream_name: "btcusdt@aggTrade".to_owned(),
    };

    assert_eq!(
        trade.validate_contract().verdict,
        ValidationVerdict::Complete
    );
}

#[test]
fn rtds_chainlink_schema_keeps_subscription_and_timestamps() {
    let rtds = RtdsChainlinkBtcUsdRaw {
        envelope: envelope(
            SourceKind::PolymarketRtdsChainlinkBtcUsd,
            "crypto_prices_chainlink",
            r#"{"topic":"crypto_prices_chainlink","symbol":"btc/usd","value":"100000"}"#,
        ),
        message_timestamp_ms: 1_766_789_469_900,
        payload_timestamp_ms: 1_766_789_469_800,
        symbol: "btc/usd".to_owned(),
        value: "100000".to_owned(),
        source_topic: "crypto_prices_chainlink".to_owned(),
        message_type: "update".to_owned(),
        age_ms: Some(199),
        freshness_status: "fresh".to_owned(),
        subscription_raw_json:
            r#"{"action":"subscribe","topic":"crypto_prices_chainlink","type":"*","filters":"{\"symbol\":\"btc/usd\"}"}"#
                .to_owned(),
        ack_or_first_message_evidence: Some("first crypto_prices_chainlink message".to_owned()),
        close_or_error_evidence: None,
    };

    assert_eq!(
        rtds.validate_contract().verdict,
        ValidationVerdict::Complete
    );

    let mut wrong_filter = rtds;
    wrong_filter.subscription_raw_json =
        r#"{"action":"subscribe","topic":"crypto_prices_chainlink","type":"*","filters":"{\"symbol\":\"eth/usd\"}"}"#
            .to_owned();
    let report = wrong_filter.validate_contract();
    assert_eq!(report.verdict, ValidationVerdict::Incomplete);
    assert!(
        report
            .source_gaps
            .contains(&"rtds_subscription_payload_mismatch".to_owned())
    );
}
