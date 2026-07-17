use data_collect::ValidationVerdict;
use data_collect::event::{
    EventEnvelope, HostClockSyncStatus, RawEvidence, SourceKind, SourceTimestamp,
};
use data_collect::source_format::{
    MarketLifecycleState, PolymarketMarketMetadataRaw, PolymarketMarketRaw,
    PolymarketMarketResolutionRaw, SubscriptionConfirmation,
};
use data_collect::sources::polymarket::{
    DiscoveryMissStats, LifecycleInput, PolymarketDiscoveryEvidence, PolymarketMarketAdapter,
    PolymarketRetiredFallbackMetadata, PolymarketRetirementEvidence, classify_lifecycle,
    evaluate_retired_predicate, t30_prefetch_window,
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
        source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
        host_clock_sync_status: HostClockSyncStatus::Synchronized {
            source: "chrony".to_owned(),
        },
        raw_evidence: RawEvidence::Json(raw_json.to_owned()),
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: source_kind,
    }
}

#[test]
fn polymarket_market_channel_contract() {
    let adapter = PolymarketMarketAdapter::default();
    let supported = adapter.supported_market_event_types();
    for event_type in [
        "book",
        "price_change",
        "last_trade_price",
        "best_bid_ask",
        "tick_size_change",
        "new_market",
        "market_resolved",
    ] {
        assert!(supported.contains(event_type));
    }

    let subscription = adapter
        .subscription_evidence(
            &["1".to_owned(), "2".to_owned()],
            SubscriptionConfirmation::FirstBusinessEvent {
                event_type: "book".to_owned(),
            },
        )
        .expect("official SDK subscription evidence");
    let raw_subscription: serde_json::Value =
        serde_json::from_str(&subscription.raw_json).expect("subscription json");
    assert_eq!(raw_subscription["type"], "market");
    assert_eq!(raw_subscription["custom_feature_enabled"], true);
    assert_eq!(raw_subscription["assets_ids"][0], "1");

    let market = PolymarketMarketRaw {
        envelope: envelope(
            SourceKind::PolymarketMarket,
            "book",
            r#"{"event_type":"book","timestamp":"1766789469958","asset_id":"1"}"#,
        ),
        event_type: "book".to_owned(),
        platform_timestamp: "1766789469958".to_owned(),
        market_id: Some("market-1".to_owned()),
        condition_id: Some("condition-1".to_owned()),
        asset_ids: vec!["1".to_owned(), "2".to_owned()],
        subscription,
    };
    assert_eq!(
        market.validate_contract().verdict,
        ValidationVerdict::Complete
    );
}

#[test]
fn polymarket_retired_predicate_contract() {
    let resolved = PolymarketMarketResolutionRaw {
        envelope: envelope(
            SourceKind::PolymarketMarket,
            "market_resolved",
            r#"{"event_type":"market_resolved","market":"condition-1","asset_id":"1"}"#,
        ),
        condition_id: "condition-1".to_owned(),
        market_id: Some("market-1".to_owned()),
        subscribed_asset_ids_for_market: vec!["1".to_owned(), "2".to_owned()],
        resolved_event_asset_ids: vec!["1".to_owned(), "2".to_owned()],
        winning_asset_id: "1".to_owned(),
        winning_outcome: "Up".to_owned(),
        close_reason_if_present: Some("resolved".to_owned()),
    };

    let decision = evaluate_retired_predicate(
        "condition-1",
        &["1".to_owned(), "2".to_owned()],
        &PolymarketRetirementEvidence::ResolvedEvent(Box::new(resolved.clone())),
    );
    assert!(decision.retired);
    assert!(decision.source_gap.is_none());

    let mut single_leg = resolved;
    single_leg.resolved_event_asset_ids = vec!["1".to_owned()];
    let decision = evaluate_retired_predicate(
        "condition-1",
        &["1".to_owned(), "2".to_owned()],
        &PolymarketRetirementEvidence::ResolvedEvent(Box::new(single_leg)),
    );
    assert!(!decision.retired);
    assert_eq!(
        decision.source_gap.as_deref(),
        Some("market_resolved_event_incomplete")
    );

    let extra_winner = PolymarketMarketResolutionRaw {
        envelope: envelope(
            SourceKind::PolymarketMarket,
            "market_resolved",
            r#"{"event_type":"market_resolved","market":"condition-1","asset_id":"3"}"#,
        ),
        condition_id: "condition-1".to_owned(),
        market_id: Some("market-1".to_owned()),
        subscribed_asset_ids_for_market: vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
        resolved_event_asset_ids: vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
        winning_asset_id: "3".to_owned(),
        winning_outcome: "Other".to_owned(),
        close_reason_if_present: Some("resolved".to_owned()),
    };
    let decision = evaluate_retired_predicate(
        "condition-1",
        &["1".to_owned(), "2".to_owned()],
        &PolymarketRetirementEvidence::ResolvedEvent(Box::new(extra_winner)),
    );
    assert!(!decision.retired);
    assert_eq!(
        decision.source_gap.as_deref(),
        Some("market_resolved_winning_asset_not_in_target_market")
    );

    let fallback = PolymarketRetiredFallbackMetadata {
        market_resolved_missing_reason: "no market_resolved event before close".to_owned(),
        fallback_metadata_asset_ids: vec!["1".to_owned(), "2".to_owned()],
        fallback_freshness_ms: 2_000,
        same_condition_confirmed: true,
        condition_id: "condition-1".to_owned(),
        closed: true,
        active: false,
        accepting_orders: false,
        closed_time_utc_ms: Some(1_766_790_000_000),
        resolution_evidence: Some(
            "metadata closed true, active false, acceptingOrders false, winning outcome".to_owned(),
        ),
        max_freshness_ms: 5_000,
    };
    let decision = evaluate_retired_predicate(
        "condition-1",
        &["1".to_owned(), "2".to_owned()],
        &PolymarketRetirementEvidence::FallbackMetadata(fallback.clone()),
    );
    assert!(decision.retired);

    let decision = evaluate_retired_predicate(
        "condition-1",
        &[],
        &PolymarketRetirementEvidence::FallbackMetadata(fallback.clone()),
    );
    assert!(!decision.retired);
    assert_eq!(
        decision.source_gap.as_deref(),
        Some("subscribed_asset_ids_missing")
    );

    let mut incomplete_fallback = fallback;
    incomplete_fallback.active = true;
    let decision = evaluate_retired_predicate(
        "condition-1",
        &["1".to_owned(), "2".to_owned()],
        &PolymarketRetirementEvidence::FallbackMetadata(incomplete_fallback),
    );
    assert!(!decision.retired);
    assert_eq!(
        decision.source_gap.as_deref(),
        Some("fallback_resolution_evidence_missing")
    );
}

#[test]
fn polymarket_t30_prefetch_coverage_contract() {
    let (preload_start, market_start) = t30_prefetch_window(1_766_789_700_000);
    assert_eq!(market_start - preload_start, 30_000);

    assert_eq!(
        classify_lifecycle(&LifecycleInput {
            now_utc_ms: preload_start + 1,
            start_time_utc_ms: market_start,
            end_time_utc_ms: market_start + 300_000,
            has_prefetch_evidence: true,
            retired: false,
        }),
        MarketLifecycleState::Prefetched
    );
    assert_eq!(
        classify_lifecycle(&LifecycleInput {
            now_utc_ms: market_start + 1,
            start_time_utc_ms: market_start,
            end_time_utc_ms: market_start + 300_000,
            has_prefetch_evidence: true,
            retired: false,
        }),
        MarketLifecycleState::Active
    );
    assert_eq!(
        classify_lifecycle(&LifecycleInput {
            now_utc_ms: market_start + 300_001,
            start_time_utc_ms: market_start,
            end_time_utc_ms: market_start + 300_000,
            has_prefetch_evidence: true,
            retired: false,
        }),
        MarketLifecycleState::Active
    );
    assert_eq!(
        classify_lifecycle(&LifecycleInput {
            now_utc_ms: market_start + 300_001,
            start_time_utc_ms: market_start,
            end_time_utc_ms: market_start + 300_000,
            has_prefetch_evidence: true,
            retired: true,
        }),
        MarketLifecycleState::Retired
    );
}

#[test]
fn polymarket_discovery_requires_active_scan_or_metadata_snapshot() {
    let mut evidence = PolymarketDiscoveryEvidence {
        candidate_slug: "btc-updown-5m-1766789700".to_owned(),
        condition_id: Some("condition-1".to_owned()),
        asset_ids: vec!["1".to_owned(), "2".to_owned()],
        active_scan_evidence: None,
        metadata_snapshot_evidence: None,
        metadata_snapshot_recv_time_utc_ms: Some(1_766_789_650_000),
        freshness_deadline_utc_ms: Some(1_766_789_700_000),
    };
    let verdict = evidence.completeness(1_766_789_660_000);
    assert!(!verdict.complete);
    assert_eq!(verdict.source_gap.as_deref(), Some("slug_only_discovery"));

    evidence.metadata_snapshot_evidence = Some("metadata snapshot sha256".to_owned());
    let verdict = evidence.completeness(1_766_789_660_000);
    assert!(verdict.complete);

    let metadata = PolymarketMarketMetadataRaw {
        envelope: envelope(
            SourceKind::PolymarketMarketMetadata,
            "new_market",
            r#"{"event_type":"new_market","condition_id":"condition-1"}"#,
        ),
        condition_id: "condition-1".to_owned(),
        asset_ids: vec!["1".to_owned(), "2".to_owned()],
        clob_market_id: "market-1".to_owned(),
        slug: "btc-updown-5m-1766789700".to_owned(),
        start_time_utc_ms: 1_766_789_700_000,
        end_time_utc_ms: 1_766_790_000_000,
        active: true,
        accepting_orders: true,
        market_lifecycle_state: MarketLifecycleState::Prefetched,
        tick_size: "0.01".to_owned(),
        min_size: "1".to_owned(),
        fee_raw: Some(r#"{"maker":0}"#.to_owned()),
        reward_raw: Some(r#"{"eligible":true}"#.to_owned()),
        candidate_slug: "btc-updown-5m-1766789700".to_owned(),
        active_scan_evidence: None,
        metadata_snapshot_evidence: Some("metadata snapshot sha256".to_owned()),
        clock_drift_evidence: Some("chrony offset 2ms".to_owned()),
    };
    assert_eq!(
        metadata.validate_contract().verdict,
        ValidationVerdict::Complete
    );
}

#[test]
fn polymarket_discovery_miss_does_not_block_later_markets() {
    let mut stats = DiscoveryMissStats {
        attempts: 0,
        misses: 0,
    };
    let decision = stats.record_miss(1_766_790_001_000, 1_766_790_000_000);

    assert!(decision.miss_recorded);
    assert_eq!(decision.discovery_miss_rate, 1.0);
    assert!(decision.skip_expired_target);
    assert!(decision.recompute_target);
}
