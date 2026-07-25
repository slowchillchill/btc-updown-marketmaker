use data_collect::ValidationVerdict;
use data_collect::event::{
    EventEnvelope, HostClockSyncStatus, RawEvidence, SourceKind, SourceTimestamp,
};
use data_collect::polymarket_market_metadata_layout::{
    ResolutionIntegrityErrorClass, ResolutionPollResult, TerminalPredicateFailureReason,
};
use data_collect::source_format::{
    MarketLifecycleState, PolymarketMarketMetadataRaw, PolymarketMarketRaw,
    SubscriptionConfirmation,
};
use data_collect::sources::polymarket::{
    DiscoveryMissStats, GammaTerminalAttemptContext, GammaTerminalFixedIdentity,
    GammaTerminalResponse, GammaTerminalWinner, LifecycleInput, PolymarketDiscoveryEvidence,
    PolymarketMarketAdapter, classify_lifecycle, evaluate_gamma_terminal_response,
    t30_prefetch_window,
};
use polymarket_client_sdk_v2::gamma::types::response::Market as SdkGammaMarket;
use serde_json::{Value, json};

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

fn gamma_terminal_market_json() -> Value {
    json!({
        "id": "market-1",
        "conditionId": "0x0000000000000000000000000000000000000000000000000000000000000001",
        "slug": "btc-updown-5m-1767225600",
        "outcomes": "[\"Up\",\"Down\"]",
        "outcomePrices": "[\"1\",\"0\"]",
        "active": true,
        "closed": true,
        "closedTime": "2026-01-01T00:00:30Z",
        "umaResolutionStatus": " resolved ",
        "clobTokenIds": "[\"101\",\"102\"]",
        "acceptingOrders": false,
        "automaticallyResolved": true
    })
}

fn decode_gamma_market(value: Value) -> SdkGammaMarket {
    serde_json::from_value(value).expect("official SDK Gamma Market fixture must decode")
}

fn gamma_terminal_identity() -> GammaTerminalFixedIdentity {
    GammaTerminalFixedIdentity {
        slug: "btc-updown-5m-1767225600".to_owned(),
        clob_market_id: "market-1".to_owned(),
        condition_id: "0x0000000000000000000000000000000000000000000000000000000000000001"
            .to_owned(),
        up_asset_id: "101".to_owned(),
        down_asset_id: "102".to_owned(),
        market_end_utc_ms: 1_767_225_600_000,
    }
}

fn gamma_terminal_attempt() -> GammaTerminalAttemptContext {
    GammaTerminalAttemptContext {
        scheduled_at_utc_ms: 1_767_225_780_000,
        fetch_start_utc_ms: 1_767_225_780_010,
        received_at_utc_ms: 1_767_225_780_020,
        response_bound_to_attempt: true,
    }
}

type GammaFailureMutation = fn(&mut Value, &mut GammaTerminalFixedIdentity);

#[test]
fn gamma_terminal_predicate_contract() {
    let market = decode_gamma_market(gamma_terminal_market_json());
    let identity = gamma_terminal_identity();
    let evaluation = evaluate_gamma_terminal_response(
        &identity,
        gamma_terminal_attempt(),
        GammaTerminalResponse::Decoded(&market),
    );
    assert_eq!(evaluation.terminal_predicate_passed, Some(true));
    assert_eq!(evaluation.terminal_predicate_failure_reason, None);
    assert_eq!(
        evaluation.resolution_poll_result,
        ResolutionPollResult::Terminal
    );
    assert_eq!(evaluation.resolution_integrity_error_class, None);
    assert_eq!(evaluation.observed_active, Some(true));
    assert_eq!(
        evaluation.winner,
        Some(GammaTerminalWinner {
            winning_asset_id: "101".to_owned(),
            winning_outcome: "Up".to_owned(),
        })
    );
    let stored_payload = serde_json::to_string(&market).unwrap();
    let reconstructed_market: SdkGammaMarket = serde_json::from_str(&stored_payload).unwrap();
    assert_eq!(
        evaluate_gamma_terminal_response(
            &identity,
            gamma_terminal_attempt(),
            GammaTerminalResponse::Decoded(&reconstructed_market),
        ),
        evaluation,
        "the deterministic SDK payload plus fixed identity and attempt timing reconstructs the predicate"
    );

    let mut future_closed_time_json = gamma_terminal_market_json();
    future_closed_time_json["closedTime"] = json!("2026-01-02T00:00:00Z");
    let future_closed_time_market = decode_gamma_market(future_closed_time_json);
    let evaluation = evaluate_gamma_terminal_response(
        &identity,
        gamma_terminal_attempt(),
        GammaTerminalResponse::Decoded(&future_closed_time_market),
    );
    assert_eq!(evaluation.terminal_predicate_passed, Some(true));
    assert!(
        evaluation.closed_time_utc_ms.unwrap() > gamma_terminal_attempt().received_at_utc_ms,
        "closedTime has no future-skew gate against response receive time"
    );

    let mut down_winner_json = gamma_terminal_market_json();
    down_winner_json["outcomePrices"] = json!("[\"0\",\"1\"]");
    let down_winner_market = decode_gamma_market(down_winner_json);
    let evaluation = evaluate_gamma_terminal_response(
        &identity,
        gamma_terminal_attempt(),
        GammaTerminalResponse::Decoded(&down_winner_market),
    );
    assert_eq!(
        evaluation.winner,
        Some(GammaTerminalWinner {
            winning_asset_id: "102".to_owned(),
            winning_outcome: "Down".to_owned(),
        })
    );

    let mut active_null_json = gamma_terminal_market_json();
    active_null_json
        .as_object_mut()
        .expect("object")
        .remove("active");
    let active_null_market = decode_gamma_market(active_null_json);
    let evaluation = evaluate_gamma_terminal_response(
        &identity,
        gamma_terminal_attempt(),
        GammaTerminalResponse::Decoded(&active_null_market),
    );
    assert_eq!(evaluation.terminal_predicate_passed, Some(true));
    assert_eq!(evaluation.observed_active, None);
    assert_eq!(
        evaluation.resolution_poll_result,
        ResolutionPollResult::Terminal
    );

    let mut active_false_json = gamma_terminal_market_json();
    active_false_json["active"] = json!(false);
    let active_false_market = decode_gamma_market(active_false_json);
    let evaluation = evaluate_gamma_terminal_response(
        &identity,
        gamma_terminal_attempt(),
        GammaTerminalResponse::Decoded(&active_false_market),
    );
    assert_eq!(evaluation.terminal_predicate_passed, Some(true));
    assert_eq!(evaluation.observed_active, Some(false));
    assert_eq!(
        evaluation.resolution_poll_result,
        ResolutionPollResult::Terminal
    );

    let failure_cases: [(
        TerminalPredicateFailureReason,
        ResolutionPollResult,
        Option<ResolutionIntegrityErrorClass>,
        GammaFailureMutation,
    ); 19] = [
        (
            TerminalPredicateFailureReason::SlugMismatch,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| value["slug"] = json!("wrong-slug"),
        ),
        (
            TerminalPredicateFailureReason::MarketIdMismatch,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| value["id"] = json!("wrong-market"),
        ),
        (
            TerminalPredicateFailureReason::ConditionIdMismatch,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| {
                value["conditionId"] =
                    json!("0x0000000000000000000000000000000000000000000000000000000000000002");
            },
        ),
        (
            TerminalPredicateFailureReason::AssetCountNotTwo,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| value["clobTokenIds"] = json!("[\"101\"]"),
        ),
        (
            TerminalPredicateFailureReason::AssetDuplicate,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| value["clobTokenIds"] = json!("[\"101\",\"101\"]"),
        ),
        (
            TerminalPredicateFailureReason::AssetSetMismatch,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| value["clobTokenIds"] = json!("[\"101\",\"103\"]"),
        ),
        (
            TerminalPredicateFailureReason::OutcomesCountNotTwo,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| value["outcomes"] = json!("[\"Up\"]"),
        ),
        (
            TerminalPredicateFailureReason::OutcomeMappingInvalid,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| value["outcomes"] = json!("[\"Down\",\"Up\"]"),
        ),
        (
            TerminalPredicateFailureReason::OutcomePricesCountNotTwo,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction),
            |value, _| value["outcomePrices"] = json!("[\"1\"]"),
        ),
        (
            TerminalPredicateFailureReason::ClosedMissing,
            ResolutionPollResult::ValidNonterminal,
            None,
            |value, _| {
                value.as_object_mut().expect("object").remove("closed");
            },
        ),
        (
            TerminalPredicateFailureReason::ClosedFalse,
            ResolutionPollResult::ValidNonterminal,
            None,
            |value, _| value["closed"] = json!(false),
        ),
        (
            TerminalPredicateFailureReason::AcceptingOrdersMissing,
            ResolutionPollResult::ValidNonterminal,
            None,
            |value, _| {
                value
                    .as_object_mut()
                    .expect("object")
                    .remove("acceptingOrders");
            },
        ),
        (
            TerminalPredicateFailureReason::AcceptingOrdersTrue,
            ResolutionPollResult::ValidNonterminal,
            None,
            |value, _| value["acceptingOrders"] = json!(true),
        ),
        (
            TerminalPredicateFailureReason::ClosedTimeMissing,
            ResolutionPollResult::ValidNonterminal,
            None,
            |value, _| {
                value.as_object_mut().expect("object").remove("closedTime");
            },
        ),
        (
            TerminalPredicateFailureReason::ClosedTimeParseError,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::ClosedTimeInvalid),
            |value, _| value["closedTime"] = json!("not-a-timestamp"),
        ),
        (
            TerminalPredicateFailureReason::ClosedTimeBeforeMarketEnd,
            ResolutionPollResult::IntegrityError,
            Some(ResolutionIntegrityErrorClass::ClosedTimeInvalid),
            |value, _| value["closedTime"] = json!("2025-12-31T23:59:59Z"),
        ),
        (
            TerminalPredicateFailureReason::UmaResolutionStatusMissing,
            ResolutionPollResult::ValidNonterminal,
            None,
            |value, _| {
                value
                    .as_object_mut()
                    .expect("object")
                    .remove("umaResolutionStatus");
            },
        ),
        (
            TerminalPredicateFailureReason::UmaResolutionStatusNotResolved,
            ResolutionPollResult::ValidNonterminal,
            None,
            |value, _| value["umaResolutionStatus"] = json!("proposed"),
        ),
        (
            TerminalPredicateFailureReason::OutcomePricesNotOneHot,
            ResolutionPollResult::ValidNonterminal,
            None,
            |value, _| value["outcomePrices"] = json!("[\"0.5\",\"0.5\"]"),
        ),
    ];
    assert_eq!(
        failure_cases.map(|case| case.0),
        TerminalPredicateFailureReason::ALL
    );

    for (expected_reason, expected_result, expected_integrity, mutate) in failure_cases {
        let mut value = gamma_terminal_market_json();
        let mut identity = gamma_terminal_identity();
        mutate(&mut value, &mut identity);
        let market = decode_gamma_market(value);
        let evaluation = evaluate_gamma_terminal_response(
            &identity,
            gamma_terminal_attempt(),
            GammaTerminalResponse::Decoded(&market),
        );
        assert_eq!(
            evaluation.terminal_predicate_passed,
            Some(false),
            "{} predicate",
            expected_reason.as_str()
        );
        assert_eq!(
            evaluation.terminal_predicate_failure_reason,
            Some(expected_reason),
            "{} reason",
            expected_reason.as_str()
        );
        assert_eq!(
            evaluation.resolution_poll_result,
            expected_result,
            "{} result",
            expected_reason.as_str()
        );
        assert_eq!(
            evaluation.resolution_integrity_error_class,
            expected_integrity,
            "{} integrity class",
            expected_reason.as_str()
        );
        assert!(
            evaluation.winner.is_none(),
            "{} winner",
            expected_reason.as_str()
        );
    }

    let mut combined = gamma_terminal_market_json();
    combined["slug"] = json!("wrong-slug");
    combined["closed"] = json!(false);
    combined["outcomePrices"] = json!("[\"0.5\",\"0.5\"]");
    let combined = decode_gamma_market(combined);
    let evaluation = evaluate_gamma_terminal_response(
        &identity,
        gamma_terminal_attempt(),
        GammaTerminalResponse::Decoded(&combined),
    );
    assert_eq!(
        evaluation.terminal_predicate_failure_reason,
        Some(TerminalPredicateFailureReason::SlugMismatch)
    );
}

#[test]
fn gamma_terminal_predicate_not_evaluated_boundaries() {
    let market = decode_gamma_market(gamma_terminal_market_json());
    let identity = gamma_terminal_identity();
    let mut unbound_attempt = gamma_terminal_attempt();
    unbound_attempt.response_bound_to_attempt = false;
    let mut early_fetch_attempt = gamma_terminal_attempt();
    early_fetch_attempt.fetch_start_utc_ms = early_fetch_attempt.scheduled_at_utc_ms - 1;
    let mut receive_before_fetch_attempt = gamma_terminal_attempt();
    receive_before_fetch_attempt.received_at_utc_ms =
        receive_before_fetch_attempt.fetch_start_utc_ms - 1;
    for attempt in [
        unbound_attempt,
        early_fetch_attempt,
        receive_before_fetch_attempt,
    ] {
        let evaluation = evaluate_gamma_terminal_response(
            &identity,
            attempt,
            GammaTerminalResponse::Decoded(&market),
        );
        assert_eq!(evaluation.terminal_predicate_passed, None);
        assert_eq!(evaluation.terminal_predicate_failure_reason, None);
        assert_eq!(evaluation.observed_active, Some(true));
        assert_eq!(
            evaluation.closed_time_raw.as_deref(),
            Some("2026-01-01T00:00:30Z")
        );
        assert_eq!(
            evaluation.resolution_integrity_error_class,
            Some(ResolutionIntegrityErrorClass::AttemptBindingInvariant)
        );
    }

    let evaluation = evaluate_gamma_terminal_response(
        &identity,
        gamma_terminal_attempt(),
        GammaTerminalResponse::SdkDecodeOrSchemaError,
    );
    assert_eq!(evaluation.terminal_predicate_passed, None);
    assert_eq!(evaluation.terminal_predicate_failure_reason, None);
    assert_eq!(evaluation.observed_active, None);
    assert_eq!(evaluation.closed_time_raw, None);
    assert_eq!(
        evaluation.resolution_integrity_error_class,
        Some(ResolutionIntegrityErrorClass::SdkDecodeOrSchema)
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
