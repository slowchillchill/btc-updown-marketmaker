use contracts::*;
use data_pipeline_core::reduction::{
    PolymarketAssetContextV1, PolymarketMarketContextV1, PolymarketMetadataInputV1,
    PolymarketOrderbookInputV1, PolymarketTerminalMerger, ReductionDispositionV1,
    ReductionEvidenceRoleV1, ReductionOperationKindV1,
};
use data_pipeline_core::typed_raw::{
    MetadataActiveUnavailableReason, MetadataMarketLifecycleState,
    MetadataMarketLifecycleTransition, MetadataMarketRuntimeRole,
    MetadataMarketRuntimeRoleTransition, MetadataResolutionEvidenceSource,
    MetadataResolutionFollowupStatus, MetadataResolutionPollResult, MetadataSourceEvidenceChannel,
    MetadataSourceEvidenceEventType, MetadataSourceEvidenceOrigin, MetadataTerminalClaimOwner,
    POLYMARKET_ORDERBOOK_SCHEMA, PolymarketMarketMetadataRow, PolymarketMetadataOrigin,
    PolymarketMetadataRowKind, PolymarketOrderbookEvent, PolymarketOrderbookRow, TypedRawRow,
};
use data_pipeline_core::{
    project_record_event_time_index_entry, project_record_visibility_index_entry,
    source_evidence_from_typed_raw,
};

const UP_TOKEN_ID: &str = "20";
const DOWN_TOKEN_ID: &str = "3";
const WS_EPOCH: &str = "ws-epoch-1";
const WS_ADMISSION_SEQUENCE: u64 = 42;
const TERMINAL_SOURCE_TIME_MS: i64 = 302_000;
const TERMINAL_RECEIVED_TIME_MS: i64 = 302_010;
const TERMINAL_ADMITTED_TIME_MS: i64 = 302_020;
const GAMMA_TERMINAL_RECEIVED_TIME_MS: i64 = 420_010;

fn sha(character: char) -> Sha256Hex {
    Sha256Hex(character.to_string().repeat(64))
}

fn market(condition_digit: char, clob_market_id: &str) -> MarketIdentityV2 {
    MarketIdentityV2 {
        condition_id: ConditionId(format!("0x{}", condition_digit.to_string().repeat(64))),
        clob_market_id: ClobMarketId(clob_market_id.to_owned()),
        window: MarketWindowV2 {
            start_at_utc_ms: 1_000,
            end_at_utc_ms: 301_000,
        },
    }
}

fn asset(token_id: &str, index: u32, label: &str) -> AssetIdentityV1 {
    AssetIdentityV1 {
        token_id: TokenId(token_id.to_owned()),
        outcome: OutcomeIdentityV1 {
            index,
            label: label.to_owned(),
        },
    }
}

fn identity_evidence(identity_digit: char) -> SourceEvidenceRefV2 {
    SourceEvidenceRefV2 {
        source_kind: CanonicalSourceKind::PolymarketMarketMetadata,
        source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
            sha256: sha(identity_digit),
        }),
        source_event_at_utc_ms: None,
        received_at_utc_ms: 900,
        collector_admission: None,
        input_manifest_sha256: sha('8'),
        input_file_sha256: sha('9'),
    }
}

fn market_context(market: MarketIdentityV2) -> PolymarketMarketContextV1 {
    PolymarketMarketContextV1 {
        market,
        assets: vec![
            PolymarketAssetContextV1 {
                asset: asset(UP_TOKEN_ID, 0, "UP"),
                initial_tick_size_pusd_per_share: ExactDecimal("0.01".to_owned()),
            },
            PolymarketAssetContextV1 {
                asset: asset(DOWN_TOKEN_ID, 1, "DOWN"),
                initial_tick_size_pusd_per_share: ExactDecimal("0.01".to_owned()),
            },
        ],
        identity_source_evidence: vec![identity_evidence('7')],
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
        closed_time_check_passed: None,
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
        terminal_predicate_passed: Some(true),
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

fn terminal_inputs(
    market: &MarketIdentityV2,
) -> (PolymarketMetadataInputV1, PolymarketOrderbookInputV1) {
    let metadata_row = terminal_metadata_row(market);
    let metadata_evidence = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketMarketMetadata(Box::new(metadata_row.clone())),
        &sha('b'),
        &sha('c'),
    )
    .unwrap();
    let orderbook_row = terminal_orderbook_row(market);
    let orderbook_evidence = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketOrderbook(orderbook_row.clone()),
        &sha('d'),
        &sha('e'),
    )
    .unwrap();
    (
        PolymarketMetadataInputV1 {
            row: metadata_row,
            evidence: metadata_evidence,
        },
        PolymarketOrderbookInputV1 {
            row: orderbook_row,
            evidence: orderbook_evidence,
        },
    )
}

fn terminal_redelivery_inputs(
    market: &MarketIdentityV2,
) -> (PolymarketMetadataInputV1, PolymarketOrderbookInputV1) {
    let mut metadata_row = terminal_metadata_row(market);
    metadata_row.ws_claimant_collector_admission_sequence = Some(WS_ADMISSION_SEQUENCE + 1);
    let metadata_evidence = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketMarketMetadata(Box::new(metadata_row.clone())),
        &sha('b'),
        &sha('c'),
    )
    .unwrap();
    let mut orderbook_row = terminal_orderbook_row(market);
    orderbook_row.collector_admission_sequence = WS_ADMISSION_SEQUENCE + 1;
    orderbook_row.collector_admission_time_utc_ms = TERMINAL_ADMITTED_TIME_MS + 1;
    let orderbook_evidence = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketOrderbook(orderbook_row.clone()),
        &sha('d'),
        &sha('e'),
    )
    .unwrap();
    (
        PolymarketMetadataInputV1 {
            row: metadata_row,
            evidence: metadata_evidence,
        },
        PolymarketOrderbookInputV1 {
            row: orderbook_row,
            evidence: orderbook_evidence,
        },
    )
}

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
    row.closed_time_raw = Some("1970-01-01T00:05:01.018Z".to_owned());
    row.closed_time_utc = Some(market.window.end_at_utc_ms + 18_000);
    row.closed_time_check_passed = Some(true);
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
    row.terminal_predicate_passed = Some(true);
    row.terminal_predicate_failure_reason = None;
    row.response_fields_unavailable_reason = None;
    row.ws_claimant_source_sequence_epoch_id = None;
    row.ws_claimant_collector_admission_sequence = None;
    row
}

fn gamma_terminal_input(market: &MarketIdentityV2) -> PolymarketMetadataInputV1 {
    let row = gamma_terminal_metadata_row(market);
    let evidence = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketMarketMetadata(Box::new(row.clone())),
        &sha('f'),
        &sha('1'),
    )
    .unwrap();
    PolymarketMetadataInputV1 { row, evidence }
}

fn assert_resolved(event: &ExecutionMarketEventV3) -> &MarketResolutionTerminalV3 {
    let ExecutionMarketEventPayloadV3::MarketResolutionTerminal(terminal) = &event.payload else {
        panic!(
            "expected one resolved terminal event, got {:?}",
            event.payload
        );
    };
    terminal
}

fn assert_retired(event: &ExecutionMarketEventV3) -> &MarketLifecycleTransitionV2 {
    let ExecutionMarketEventPayloadV3::MarketLifecycleTransition(transition) = &event.payload
    else {
        panic!("expected a qualified active-to-retired lifecycle event");
    };
    transition
}

#[test]
fn matching_ws_terminal_merges_once_with_all_direct_evidence_and_sorted_payouts() {
    let market = market('1', "3195576");
    let context = market_context(market.clone());
    let context_evidence = context.identity_source_evidence[0].clone();
    let (metadata, orderbook) = terminal_inputs(&market);
    let expected_metadata_evidence = metadata.evidence.clone();
    let expected_orderbook_evidence = orderbook.evidence.clone();
    let mut merger =
        PolymarketTerminalMerger::new(context, "polymarket-terminal-merger-v1").unwrap();

    let first = merger
        .merge_ws_terminal(metadata.clone(), orderbook.clone())
        .unwrap();
    assert_eq!(first.events.len(), 2);
    assert_eq!(first.reports.len(), 2);
    let event = &first.events[0];
    let terminal = assert_resolved(event);
    let MarketResolutionTerminalV3::Resolved {
        winning_asset_id,
        winning_outcome,
        payout_vector,
        ..
    } = terminal;
    assert_eq!(winning_asset_id, UP_TOKEN_ID);
    assert_eq!(winning_outcome, "UP");
    assert_eq!(
        payout_vector
            .iter()
            .map(|entry| entry.asset.token_id.0.as_str())
            .collect::<Vec<_>>(),
        [DOWN_TOKEN_ID, UP_TOKEN_ID]
    );
    assert_eq!(
        payout_vector
            .iter()
            .map(|entry| entry.asset.outcome.index)
            .collect::<Vec<_>>(),
        [1, 0]
    );
    assert_eq!(
        payout_vector
            .iter()
            .map(|entry| entry.payout_pusd_per_share.0.as_str())
            .collect::<Vec<_>>(),
        ["0", "1"]
    );
    assert_eq!(event.lineage.source_evidence.len(), 3);
    assert!(
        event
            .lineage
            .source_evidence
            .contains(&expected_metadata_evidence)
    );
    assert!(
        event
            .lineage
            .source_evidence
            .contains(&expected_orderbook_evidence)
    );
    assert!(event.lineage.source_evidence.contains(&context_evidence));
    assert_eq!(
        event.time.source_event_at_utc_ms,
        Some(TERMINAL_SOURCE_TIME_MS)
    );
    assert_eq!(event.time.received_at_utc_ms, TERMINAL_RECEIVED_TIME_MS);
    assert_eq!(first.reports[0].operation, ReductionOperationKindV1::Merge);
    assert!(matches!(
        &first.reports[0].disposition,
        ReductionDispositionV1::Produced { result_event_id } if result_event_id == &event.event_id
    ));
    let retired = assert_retired(&first.events[1]);
    assert_eq!(
        retired.previous_state,
        Some(MappedMarketLifecycleStateV1::Known {
            value: KnownMarketLifecycleState::Active,
        })
    );
    assert_eq!(
        retired.current_state,
        MappedMarketLifecycleStateV1::Known {
            value: KnownMarketLifecycleState::Retired,
        }
    );
    assert_eq!(
        first.events[1].time.source_event_at_utc_ms,
        Some(TERMINAL_SOURCE_TIME_MS)
    );
    data_pipeline_core::validate_execution_market_event_v3(event).unwrap();

    let duplicate = merger.merge_ws_terminal(metadata, orderbook).unwrap();
    assert!(duplicate.events.is_empty());
    assert_eq!(duplicate.reports.len(), 1);
    assert_eq!(
        duplicate.reports[0].operation,
        ReductionOperationKindV1::Suppress
    );
    assert!(matches!(
        &duplicate.reports[0].disposition,
        ReductionDispositionV1::Suppressed { operation_evidence }
            if operation_evidence.len() == 5
                && operation_evidence.iter().any(|evidence| {
                    evidence.role == ReductionEvidenceRoleV1::Context
                        && evidence.source_evidence == context_evidence
                })
                && operation_evidence.iter().filter(|evidence| {
                    evidence.role == ReductionEvidenceRoleV1::MatchedExisting
                }).count() == 2
                && operation_evidence.iter().filter(|evidence| {
                    evidence.role == ReductionEvidenceRoleV1::IncomingDuplicate
                }).count() == 2
    ));
}

#[test]
fn ws_terminal_rejects_a_row_without_the_completed_terminal_transition() {
    let market = market('1', "3195576");
    let (mut metadata, orderbook) = terminal_inputs(&market);
    metadata.row.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    metadata.evidence = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketMarketMetadata(Box::new(metadata.row.clone())),
        &sha('b'),
        &sha('c'),
    )
    .unwrap();
    let mut merger =
        PolymarketTerminalMerger::new(market_context(market), "polymarket-terminal-merger-v1")
            .unwrap();

    let error = merger.merge_ws_terminal(metadata, orderbook).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("metadata: does not represent a qualified WebSocket terminal claim")
    );
}

#[test]
fn terminal_qualifiers_accept_upstream_trim_equivalent_status_and_labels() {
    let gamma_market = market('1', "3195576");
    let mut gamma = gamma_terminal_input(&gamma_market);
    gamma.row.uma_resolution_status = Some(" resolved ".to_owned());
    gamma.row.outcomes_raw = Some(r#"[" UP "," DOWN "]"#.to_owned());
    gamma.row.winning_outcome = Some(" UP ".to_owned());
    gamma.evidence = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketMarketMetadata(Box::new(gamma.row.clone())),
        &sha('f'),
        &sha('1'),
    )
    .unwrap();
    let mut gamma_merger = PolymarketTerminalMerger::new(
        market_context(gamma_market),
        "polymarket-terminal-merger-v1",
    )
    .unwrap();

    let gamma_batch = gamma_merger.merge_gamma_terminal(gamma).unwrap();

    assert_eq!(gamma_batch.events.len(), 3);

    let ws_market = market('2', "3195577");
    let (mut metadata, mut orderbook) = terminal_inputs(&ws_market);
    metadata.row.winning_outcome = Some(" UP ".to_owned());
    metadata.row.ws_platform_timestamp_raw = Some(format!(" {TERMINAL_SOURCE_TIME_MS} "));
    metadata.evidence = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketMarketMetadata(Box::new(metadata.row.clone())),
        &sha('b'),
        &sha('c'),
    )
    .unwrap();
    let PolymarketOrderbookEvent::MarketResolved {
        winning_outcome, ..
    } = &mut orderbook.row.event
    else {
        unreachable!("terminal fixture must be market_resolved");
    };
    *winning_outcome = " up ".to_owned();
    orderbook.evidence = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketOrderbook(orderbook.row.clone()),
        &sha('d'),
        &sha('e'),
    )
    .unwrap();
    let mut ws_merger =
        PolymarketTerminalMerger::new(market_context(ws_market), "polymarket-terminal-merger-v1")
            .unwrap();

    let ws_batch = ws_merger.merge_ws_terminal(metadata, orderbook).unwrap();

    assert_eq!(ws_batch.events.len(), 2);
}

#[test]
fn qualified_gamma_terminal_is_visible_without_an_event_time_projection() {
    let market = market('1', "3195576");
    let context = market_context(market.clone());
    let expected_identity_evidence = context.identity_source_evidence[0].clone();
    let input = gamma_terminal_input(&market);
    let expected_gamma_evidence = input.evidence.clone();
    let mut merger =
        PolymarketTerminalMerger::new(context, "polymarket-terminal-merger-v1").unwrap();

    let batch = merger.merge_gamma_terminal(input).unwrap();

    assert_eq!(batch.events.len(), 3);
    assert_eq!(batch.reports.len(), 3);
    let event = &batch.events[0];
    let terminal = assert_resolved(event);
    let MarketResolutionTerminalV3::Resolved {
        winning_asset_id,
        winning_outcome,
        payout_vector,
        ..
    } = terminal;
    assert_eq!(winning_asset_id, UP_TOKEN_ID);
    assert_eq!(winning_outcome, "UP");
    assert_eq!(
        payout_vector
            .iter()
            .map(|entry| entry.payout_pusd_per_share.0.as_str())
            .collect::<Vec<_>>(),
        ["0", "1"]
    );
    assert_eq!(event.time.source_event_at_utc_ms, None);
    assert_eq!(
        event.time.received_at_utc_ms,
        GAMMA_TERMINAL_RECEIVED_TIME_MS
    );
    assert_eq!(event.lineage.source_evidence.len(), 2);
    assert!(
        event
            .lineage
            .source_evidence
            .contains(&expected_identity_evidence)
    );
    assert!(
        event
            .lineage
            .source_evidence
            .contains(&expected_gamma_evidence)
    );
    assert_eq!(
        batch.reports[0].reason_code,
        "POLYMARKET_TERMINAL_GAMMA_VISIBLE_ONLY_NO_SOURCE_TIME"
    );
    let record = CanonicalEventRecordV1::ExecutionMarketEvent(Box::new(event.clone()));
    assert_eq!(
        project_record_event_time_index_entry(&record).unwrap(),
        None
    );
    assert_eq!(
        project_record_visibility_index_entry(&record)
            .unwrap()
            .received_at_utc_ms,
        GAMMA_TERMINAL_RECEIVED_TIME_MS
    );
    let retired = assert_retired(&batch.events[1]);
    assert_eq!(
        retired.current_state,
        MappedMarketLifecycleStateV1::Known {
            value: KnownMarketLifecycleState::Retired,
        }
    );
    assert_eq!(batch.events[1].time.source_event_at_utc_ms, None);
    assert!(matches!(
        batch.events[2].payload,
        ExecutionMarketEventPayloadV3::MarketTradingState(MarketTradingStateV2 {
            active: Some(true),
            closed: Some(true),
            accepting_orders: Some(false),
        })
    ));
    assert_eq!(batch.events[2].time.source_event_at_utc_ms, None);
}

#[test]
fn gamma_winner_and_one_hot_conflict_quarantines_only_the_affected_market() {
    let affected_market = market('1', "3195576");
    let mut input = gamma_terminal_input(&affected_market);
    input.row.outcome_prices_raw = Some(r#"["0","1"]"#.to_owned());
    input.evidence = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketMarketMetadata(Box::new(input.row.clone())),
        &sha('f'),
        &sha('1'),
    )
    .unwrap();
    let mut merger = PolymarketTerminalMerger::new(
        market_context(affected_market.clone()),
        "polymarket-terminal-merger-v1",
    )
    .unwrap();

    let isolated = merger.merge_gamma_terminal(input).unwrap();

    assert_eq!(isolated.events.len(), 1);
    let event = &isolated.events[0];
    assert_eq!(event.market, affected_market);
    assert_eq!(event.quality.availability, MarketDataQuality::Invalid);
    let ExecutionMarketEventPayloadV3::QualityBoundary(boundary) = &event.payload else {
        panic!("conflicting Gamma winner evidence must isolate the market");
    };
    assert_eq!(
        boundary.previous_quality,
        MarketDataQuality::UnknownNotObserved
    );
    assert_eq!(boundary.current_quality, MarketDataQuality::Invalid);
    assert_eq!(
        isolated.reports[0].reason_code,
        "POLYMARKET_TERMINAL_PAYOUT_EVIDENCE_CONFLICT"
    );

    let unaffected_market = market('2', "3195577");
    let mut unaffected = PolymarketTerminalMerger::new(
        market_context(unaffected_market.clone()),
        "polymarket-terminal-merger-v1",
    )
    .unwrap();
    let unaffected_batch = unaffected
        .merge_gamma_terminal(gamma_terminal_input(&unaffected_market))
        .unwrap();
    assert_eq!(unaffected_batch.events.len(), 3);
    let _ = assert_resolved(&unaffected_batch.events[0]);
}

#[test]
fn same_terminal_semantics_with_a_new_admission_is_suppressed_not_quarantined() {
    let market = market('1', "3195576");
    let mut merger = PolymarketTerminalMerger::new(
        market_context(market.clone()),
        "polymarket-terminal-merger-v1",
    )
    .unwrap();
    let (first_metadata, first_orderbook) = terminal_inputs(&market);
    let first = merger
        .merge_ws_terminal(first_metadata, first_orderbook)
        .unwrap();
    assert_eq!(first.events.len(), 2);

    let (redelivered_metadata, redelivered_orderbook) = terminal_redelivery_inputs(&market);
    let duplicate = merger
        .merge_ws_terminal(redelivered_metadata, redelivered_orderbook)
        .unwrap();

    assert!(duplicate.events.is_empty());
    assert_eq!(duplicate.reports.len(), 1);
    assert_eq!(
        duplicate.reports[0].operation,
        ReductionOperationKindV1::Suppress
    );
    assert_eq!(
        duplicate.reports[0].reason_code,
        "POLYMARKET_TERMINAL_IDENTICAL_DUPLICATE"
    );
}

#[test]
fn conflicting_claim_after_a_valid_terminal_transitions_from_fresh_to_invalid() {
    let market = market('1', "3195576");
    let mut merger = PolymarketTerminalMerger::new(
        market_context(market.clone()),
        "polymarket-terminal-merger-v1",
    )
    .unwrap();
    let (metadata, orderbook) = terminal_inputs(&market);
    let terminal = merger.merge_ws_terminal(metadata, orderbook).unwrap();
    let terminal_event = terminal.events[0].clone();
    let (mut conflicting_metadata, mut conflicting_orderbook) =
        conflicting_inputs(&market, TerminalConflict::Winner);
    conflicting_metadata.row.timestamp_received = TERMINAL_RECEIVED_TIME_MS - 100;
    conflicting_metadata.row.source_evidence_received_at_utc =
        Some(TERMINAL_RECEIVED_TIME_MS - 100);
    conflicting_metadata.evidence = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketMarketMetadata(Box::new(conflicting_metadata.row.clone())),
        &sha('b'),
        &sha('c'),
    )
    .unwrap();
    conflicting_orderbook.row.timestamp_received = TERMINAL_RECEIVED_TIME_MS - 100;
    conflicting_orderbook.evidence = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketOrderbook(conflicting_orderbook.row.clone()),
        &sha('d'),
        &sha('e'),
    )
    .unwrap();

    let conflict = merger
        .merge_ws_terminal(conflicting_metadata, conflicting_orderbook)
        .unwrap();

    assert_eq!(conflict.events.len(), 1);
    let ExecutionMarketEventPayloadV3::QualityBoundary(boundary) = &conflict.events[0].payload
    else {
        panic!("a later conflicting terminal claim must isolate the market");
    };
    assert_eq!(boundary.previous_quality, MarketDataQuality::Fresh);
    assert_eq!(boundary.current_quality, MarketDataQuality::Invalid);
    assert_eq!(
        conflict.events[0].time.received_at_utc_ms,
        terminal_event.time.received_at_utc_ms
    );
    assert!(
        terminal_event
            .lineage
            .source_evidence
            .iter()
            .all(|evidence| conflict.events[0]
                .lineage
                .source_evidence
                .contains(evidence))
    );
}

#[derive(Clone, Copy)]
enum TerminalConflict {
    Winner,
    Market,
    SourceTime,
}

fn conflicting_inputs(
    market: &MarketIdentityV2,
    conflict: TerminalConflict,
) -> (PolymarketMetadataInputV1, PolymarketOrderbookInputV1) {
    let metadata = terminal_metadata_row(market);
    let mut orderbook = terminal_orderbook_row(market);
    match conflict {
        TerminalConflict::Winner => {
            let PolymarketOrderbookEvent::MarketResolved {
                winning_asset_id,
                winning_outcome,
                ..
            } = &mut orderbook.event
            else {
                unreachable!();
            };
            *winning_asset_id = DOWN_TOKEN_ID.to_owned();
            *winning_outcome = "DOWN".to_owned();
        }
        TerminalConflict::Market => {
            orderbook.market = format!("0x{}", "f".repeat(64));
        }
        TerminalConflict::SourceTime => {
            orderbook.timestamp = Some(TERMINAL_SOURCE_TIME_MS + 1);
        }
    }
    let metadata_evidence = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketMarketMetadata(Box::new(metadata.clone())),
        &sha('b'),
        &sha('c'),
    )
    .unwrap();
    let orderbook_evidence = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketOrderbook(orderbook.clone()),
        &sha('d'),
        &sha('e'),
    )
    .unwrap();
    (
        PolymarketMetadataInputV1 {
            row: metadata,
            evidence: metadata_evidence,
        },
        PolymarketOrderbookInputV1 {
            row: orderbook,
            evidence: orderbook_evidence,
        },
    )
}

#[test]
fn winner_market_and_source_time_conflicts_quarantine_only_the_affected_market() {
    let affected_market = market('1', "3195576");
    for conflict in [
        TerminalConflict::Winner,
        TerminalConflict::Market,
        TerminalConflict::SourceTime,
    ] {
        let context = market_context(affected_market.clone());
        let context_evidence = context.identity_source_evidence[0].clone();
        let (metadata, orderbook) = conflicting_inputs(&affected_market, conflict);
        let mut merger =
            PolymarketTerminalMerger::new(context, "polymarket-terminal-merger-v1").unwrap();
        let isolated = merger.merge_ws_terminal(metadata, orderbook).unwrap();
        assert_eq!(isolated.events.len(), 1);
        assert!(isolated.events.iter().all(|event| !matches!(
            &event.payload,
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(
                MarketResolutionTerminalV3::Resolved { .. }
            )
        )));
        let event = &isolated.events[0];
        assert_eq!(event.market, affected_market);
        assert_eq!(event.quality.availability, MarketDataQuality::Invalid);
        assert!(event.lineage.source_evidence.contains(&context_evidence));
        let ExecutionMarketEventPayloadV3::QualityBoundary(boundary) = &event.payload else {
            panic!("terminal conflict must produce a market-scoped quality boundary");
        };
        assert_eq!(boundary.scope, QualityScopeV1::Market);
        assert_eq!(
            boundary.previous_quality,
            MarketDataQuality::UnknownNotObserved
        );
        assert_eq!(boundary.current_quality, MarketDataQuality::Invalid);
        assert_eq!(isolated.reports.len(), 1);
        assert_eq!(
            isolated.reports[0].operation,
            ReductionOperationKindV1::Reject
        );
        assert!(matches!(
            &isolated.reports[0].disposition,
            ReductionDispositionV1::Produced { result_event_id }
                if result_event_id == &event.event_id
        ));
    }

    let unaffected_market = market('2', "3195577");
    let context = market_context(unaffected_market.clone());
    let (metadata, orderbook) = terminal_inputs(&unaffected_market);
    let mut merger =
        PolymarketTerminalMerger::new(context, "polymarket-terminal-merger-v1").unwrap();
    let unaffected = merger.merge_ws_terminal(metadata, orderbook).unwrap();
    assert_eq!(unaffected.events.len(), 2);
    assert_eq!(unaffected.events[0].market, unaffected_market);
    let _ = assert_resolved(&unaffected.events[0]);
}

#[test]
fn prepared_ws_and_gamma_terminal_share_one_state_and_materialize_original_result() {
    use data_pipeline_core::pm_reduction::PreparedPmEvidenceV1;
    use data_pipeline_core::public_input::*;
    use data_pipeline_core::public_reduction::*;
    use data_pipeline_core::public_reference::{
        source_evidence_v3_from_existing, source_evidence_v3_from_typed_raw_v2,
    };
    let market = market('1', "123");
    let original_context = market_context(market.clone());
    let mut original_merger =
        PolymarketTerminalMerger::new(original_context.clone(), "rule-v3").unwrap();
    let mut deferred_merger = PolymarketTerminalMerger::new(
        PolymarketMarketContextV1 {
            market: original_context.market,
            assets: original_context.assets,
            identity_source_evidence: original_context
                .identity_source_evidence
                .into_iter()
                .map(|source| PreparedPmEvidenceV1::prepare(source, None).unwrap())
                .collect(),
        },
        "rule-v3",
    )
    .unwrap();
    let mut pending = Vec::new();
    for (metadata, ws) in [
        terminal_inputs(&market),
        terminal_redelivery_inputs(&market),
    ] {
        let original = original_merger
            .merge_ws_terminal(metadata.clone(), ws.clone())
            .unwrap();
        let outcomes = data_pipeline_core::typed_raw::map_metadata_outcomes(
            &metadata.row.asset_ids,
            metadata.row.outcomes_raw.as_deref(),
            metadata.row.outcome_prices_raw.as_deref(),
        )
        .unwrap();
        let delayed = deferred_merger
            .compute_ws_terminal(
                PolymarketMetadataInputV1 {
                    row: metadata.row,
                    evidence: PreparedPmEvidenceV1::prepare(metadata.evidence, None).unwrap(),
                },
                &outcomes,
                PolymarketOrderbookInputV1 {
                    row: ws.row,
                    evidence: PreparedPmEvidenceV1::prepare(ws.evidence, None).unwrap(),
                },
            )
            .unwrap();
        pending.push((delayed, original));
    }
    // 先推进到原重复报告，再物化首终态；胜出、状态事件、所有摘要和报告一起比较。
    for (delayed, original) in pending {
        let actual =
            data_pipeline_core::reduction::materialize_legacy_market_reduction_v1(delayed).unwrap();
        assert_eq!(
            serde_json::to_value((&actual.events, &actual.reports)).unwrap(),
            serde_json::to_value((&original.events, &original.reports)).unwrap()
        );
    }
    let old_context = market_context(market.clone());
    let context = PolymarketMarketContextV1 {
        market: old_context.market,
        assets: old_context.assets,
        identity_source_evidence: old_context
            .identity_source_evidence
            .iter()
            .map(|source| {
                PreparedPmEvidenceV1::prepare(
                    source_evidence_v3_from_existing(source).unwrap(),
                    None,
                )
                .unwrap()
            })
            .collect(),
    };
    let mut merger = PolymarketTerminalMerger::new(context, "rule-v3").unwrap();
    let record = |row: TypedRawRow, position| {
        let row = TypedRawRowV2::Existing(row);
        let source_evidence = source_evidence_v3_from_typed_raw_v2(&row).unwrap();
        RecordedIngressV1 {
            input_stream_id: "terminal".to_owned(),
            input_position: position,
            ingress: DptIngressV1::Message {
                row,
                source_evidence,
            },
            dependency_record_refs: vec![],
        }
    };
    let ws = record(
        TypedRawRow::PolymarketOrderbook(terminal_orderbook_row(&market)),
        0,
    );
    let metadata = record(
        TypedRawRow::PolymarketMarketMetadata(Box::new(terminal_metadata_row(&market))),
        1,
    );
    let prepared_ws = prepare_ingress_v1(&ws, None, &Default::default(), &[], "rule-v3").unwrap();
    let group_id = prepared_ws.prepared_group_id.unwrap();
    let PreparedCanonicalInputV1::PolymarketWsTerminal { row, source } = prepared_ws.current else {
        panic!("terminal")
    };
    let direct = source.source().clone();
    let ws_input = PolymarketOrderbookInputV1 {
        row: row.clone(),
        evidence: source,
    };
    let prepared =
        prepare_ingress_v1(&metadata, None, &Default::default(), &[], "rule-v3").unwrap();
    let PreparedCanonicalInputV1::Metadata(value) = prepared.current else {
        panic!("metadata")
    };
    let result = merger
        .compute_ws_terminal(
            PolymarketMetadataInputV1 {
                row: value.row.clone(),
                evidence: value.source.clone(),
            },
            &value.outcomes,
            ws_input.clone(),
        )
        .unwrap();
    assert_eq!(result.events.len(), 2);
    let context = PublicReductionContextV1 {
        input_stream_id: "terminal".to_owned(),
        closed_at_input_position: 1,
        first_fact_ordinal: 0,
        first_report_ordinal: 0,
        group_id: group_id.clone(),
        source_refs: vec![
            CanonicalInputRecordRefV1 {
                input_stream_id: "terminal".to_owned(),
                input_position: 0,
            },
            CanonicalInputRecordRefV1 {
                input_stream_id: "terminal".to_owned(),
                input_position: 1,
            },
        ],
        direct_sources: vec![direct],
    };
    let hot = bind_market_reduction_v1(result, context);
    let duplicate = merger
        .compute_ws_terminal(
            PolymarketMetadataInputV1 {
                row: value.row.clone(),
                evidence: value.source,
            },
            &value.outcomes,
            ws_input,
        )
        .unwrap();
    assert!(duplicate.events.is_empty());
    assert!(matches!(
        duplicate.reports[0].disposition,
        ReductionDispositionV1::Suppressed { .. }
    ));
    let gamma = record(
        TypedRawRow::PolymarketMarketMetadata(Box::new(gamma_terminal_metadata_row(&market))),
        2,
    );
    let prepared = prepare_ingress_v1(&gamma, None, &Default::default(), &[], "rule-v3").unwrap();
    let PreparedCanonicalInputV1::Metadata(value) = prepared.current else {
        panic!("gamma")
    };
    let gamma = merger
        .compute_gamma_terminal(
            PolymarketMetadataInputV1 {
                row: value.row.clone(),
                evidence: value.source,
            },
            &value.outcomes,
        )
        .unwrap();
    assert!(gamma.events.is_empty());
    let durable = materialize_public_reduction_v1(hot).unwrap();
    assert_eq!(durable.records.len(), 2);
    let CanonicalEventRecordV2::MarketFact(terminal) = &durable.records[0] else {
        panic!("market fact")
    };
    assert_eq!(terminal.schema_version, CANONICAL_MARKET_FACT_SCHEMA_V1);
    assert_eq!(terminal.group_id, group_id);
    assert!(
        matches!(&terminal.payload, CanonicalMarketFactPayloadV1::Terminal(MarketResolutionTerminalV3::Resolved { winning_asset_id, .. }) if winning_asset_id == UP_TOKEN_ID)
    );
    assert!(terminal.lineage.source_evidence.len() >= 3);
    assert!(
        matches!(&durable.reports[0].report.disposition, ReductionDispositionV1::Produced { result_event_id } if *result_event_id == terminal.event_id)
    );
}
