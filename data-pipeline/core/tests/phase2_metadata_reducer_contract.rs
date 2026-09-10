use contracts::*;
use data_pipeline_core::reduction::{
    PolymarketMetadataInputV1, PolymarketMetadataReducer, ReductionDispositionV1,
    ReductionOperationKindV1,
};
use data_pipeline_core::source_evidence_from_typed_raw;
use data_pipeline_core::typed_raw::{
    MetadataActiveUnavailableReason, MetadataMarketLifecycleState,
    MetadataMarketLifecycleTransition, MetadataMarketRuntimeRole,
    MetadataMarketRuntimeRoleTransition, MetadataResolutionFollowupStatus,
    MetadataResponseFieldsUnavailableReason, MetadataSourceEndpointUnavailableReason,
    MetadataSourceEvidenceEventType, MetadataSourceEvidenceOrigin,
    POLYMARKET_MARKET_METADATA_SCHEMA, PolymarketMarketMetadataRow, PolymarketMetadataOrigin,
    PolymarketMetadataRowKind, TypedRawRow,
};
use sha2::{Digest, Sha256};

const CONDITION_ID: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const CLOB_MARKET_ID: &str = "3195576";
const UP_TOKEN_ID: &str = "20";
const DOWN_TOKEN_ID: &str = "3";
const SNAPSHOT_RECEIVED_AT: i64 = 10_000;
const MARKET_START_AT: i64 = 20_000;
const MARKET_END_AT: i64 = 30_000;

fn sha(character: char) -> Sha256Hex {
    Sha256Hex(character.to_string().repeat(64))
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
        closed_time_check_passed: None,
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
        terminal_predicate_passed: None,
        terminal_predicate_failure_reason: None,
        response_fields_unavailable_reason: None,
        ws_claimant_source_sequence_epoch_id: None,
        ws_claimant_collector_admission_sequence: None,
    }
}

fn metadata_input(row: PolymarketMarketMetadataRow) -> PolymarketMetadataInputV1 {
    let evidence = source_evidence_from_typed_raw(
        &TypedRawRow::PolymarketMarketMetadata(Box::new(row.clone())),
        &sha('a'),
        &sha('b'),
    )
    .unwrap();
    PolymarketMetadataInputV1 { row, evidence }
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
    row.closed_time_check_passed = None;
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
    row.terminal_predicate_passed = None;
    row.terminal_predicate_failure_reason = None;
    row.response_fields_unavailable_reason =
        Some(MetadataResponseFieldsUnavailableReason::NotApplicableForRowKind);
    row.ws_claimant_source_sequence_epoch_id = None;
    row.ws_claimant_collector_admission_sequence = None;
    row
}

fn payloads(batch: &[ExecutionMarketEventV3]) -> Vec<&ExecutionMarketEventPayloadV3> {
    batch.iter().map(|event| &event.payload).collect()
}

#[test]
fn identity_snapshot_builds_the_exact_window_and_initial_independent_states() {
    let input = metadata_input(metadata_snapshot_row());

    let (reducer, context, batch) =
        PolymarketMetadataReducer::from_identity_snapshot(input, "polymarket-metadata-reducer-v1")
            .unwrap();

    assert_eq!(context.market.condition_id.0, CONDITION_ID);
    assert_eq!(context.market.clob_market_id.0, CLOB_MARKET_ID);
    assert_eq!(context.market.window.start_at_utc_ms, MARKET_START_AT);
    assert_eq!(context.market.window.end_at_utc_ms, MARKET_END_AT);
    assert_eq!(context.assets.len(), 2);
    assert!(context.assets.iter().all(|asset| {
        asset.initial_tick_size_pusd_per_share == ExactDecimal("0.01".to_owned())
    }));
    assert_eq!(reducer.market(), &context.market);
    assert_eq!(batch.events.len(), 2);
    assert_eq!(batch.reports.len(), 2);
    assert!(
        batch
            .events
            .iter()
            .all(|event| event.time.source_event_at_utc_ms.is_none()
                && event.time.received_at_utc_ms == SNAPSHOT_RECEIVED_AT)
    );
    assert!(payloads(&batch.events).iter().any(|payload| matches!(
        payload,
        ExecutionMarketEventPayloadV3::MarketLifecycleTransition(MarketLifecycleTransitionV2 {
            previous_state: None,
            current_state: MappedMarketLifecycleStateV1::Known {
                value: KnownMarketLifecycleState::Prefetched
            },
            ..
        })
    )));
    assert!(payloads(&batch.events).iter().any(|payload| matches!(
        payload,
        ExecutionMarketEventPayloadV3::MarketTradingState(MarketTradingStateV2 {
            active: Some(true),
            closed: Some(false),
            accepting_orders: Some(true),
        })
    )));
}

#[test]
fn start_maps_to_market_start_and_end_does_not_retire_the_market() {
    let snapshot = metadata_snapshot_row();
    let (mut reducer, _, _) = PolymarketMetadataReducer::from_identity_snapshot(
        metadata_input(snapshot.clone()),
        "polymarket-metadata-reducer-v1",
    )
    .unwrap();
    let mut start = local_transition_row(snapshot.clone(), MARKET_START_AT + 5);
    start.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    start.market_lifecycle_transition = Some(MetadataMarketLifecycleTransition::PrefetchedToActive);
    start.lifecycle_scheduled_at_utc = Some(MARKET_START_AT);
    start.lifecycle_applied_at_utc = Some(MARKET_START_AT + 5);
    start.market_runtime_role = MetadataMarketRuntimeRole::Current;
    start.market_runtime_role_transition = Some(MetadataMarketRuntimeRoleTransition::NextToCurrent);
    start.runtime_role_scheduled_at_utc = Some(MARKET_START_AT);
    start.runtime_role_applied_at_utc = Some(MARKET_START_AT + 5);

    let started = reducer.reduce_metadata(metadata_input(start)).unwrap();

    assert_eq!(started.events.len(), 1);
    let ExecutionMarketEventPayloadV3::MarketLifecycleTransition(transition) =
        &started.events[0].payload
    else {
        panic!("market start must emit the explicit lifecycle transition");
    };
    assert_eq!(
        transition.previous_state,
        Some(MappedMarketLifecycleStateV1::Known {
            value: KnownMarketLifecycleState::Prefetched,
        })
    );
    assert_eq!(
        transition.current_state,
        MappedMarketLifecycleStateV1::Known {
            value: KnownMarketLifecycleState::Active,
        }
    );
    assert_eq!(reducer.market().window.start_at_utc_ms, MARKET_START_AT);
    assert_eq!(started.events[0].time.source_event_at_utc_ms, None);

    let mut end = local_transition_row(snapshot, MARKET_END_AT + 5);
    end.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    end.market_lifecycle_transition = None;
    end.lifecycle_scheduled_at_utc = None;
    end.lifecycle_applied_at_utc = None;
    end.market_runtime_role = MetadataMarketRuntimeRole::ResolutionPending;
    end.market_runtime_role_transition =
        Some(MetadataMarketRuntimeRoleTransition::CurrentToResolutionPending);
    end.runtime_role_scheduled_at_utc = Some(MARKET_END_AT);
    end.runtime_role_applied_at_utc = Some(MARKET_END_AT + 5);
    end.resolution_budget_horizon_utc = Some(MARKET_END_AT + 8 * 60_000);
    end.resolution_followup_status = Some(MetadataResolutionFollowupStatus::Pending);

    let ended = reducer.reduce_metadata(metadata_input(end)).unwrap();

    assert!(ended.events.is_empty());
    assert_eq!(ended.reports.len(), 1);
    assert_eq!(
        ended.reports[0].operation,
        ReductionOperationKindV1::Confirm
    );
    assert!(matches!(
        ended.reports[0].disposition,
        ReductionDispositionV1::Suppressed { .. }
    ));
    assert_eq!(reducer.market().window.end_at_utc_ms, MARKET_END_AT);
}

#[test]
fn trading_flags_are_preserved_and_emitted_only_when_the_observed_tuple_changes() {
    let snapshot = metadata_snapshot_row();
    let (mut reducer, _, _) = PolymarketMetadataReducer::from_identity_snapshot(
        metadata_input(snapshot.clone()),
        "polymarket-metadata-reducer-v1",
    )
    .unwrap();

    let same = reducer
        .reduce_metadata(metadata_input(snapshot.clone()))
        .unwrap();
    assert!(same.events.is_empty());

    let mut changed = snapshot;
    changed.timestamp_received += 1_000;
    changed.collector_admission_sequence += 1;
    changed.collector_admission_time_utc_ms = changed.timestamp_received + 1;
    changed.source_evidence_received_at_utc = Some(changed.timestamp_received);
    changed.row_evidence_validation_completed_at_utc = Some(changed.timestamp_received + 1);
    changed.closed = Some(true);
    changed.accepting_orders = Some(false);

    let changed = reducer.reduce_metadata(metadata_input(changed)).unwrap();

    assert_eq!(changed.events.len(), 1);
    assert!(matches!(
        changed.events[0].payload,
        ExecutionMarketEventPayloadV3::MarketTradingState(MarketTradingStateV2 {
            active: Some(true),
            closed: Some(true),
            accepting_orders: Some(false),
        })
    ));
}

#[test]
fn a_different_transition_preimage_cannot_be_hidden_as_an_active_duplicate() {
    let snapshot = metadata_snapshot_row();
    let (mut reducer, _, _) = PolymarketMetadataReducer::from_identity_snapshot(
        metadata_input(snapshot.clone()),
        "polymarket-metadata-reducer-v1",
    )
    .unwrap();
    let mut start = local_transition_row(snapshot.clone(), MARKET_START_AT + 5);
    start.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    start.market_lifecycle_transition = Some(MetadataMarketLifecycleTransition::PrefetchedToActive);
    start.lifecycle_scheduled_at_utc = Some(MARKET_START_AT);
    start.lifecycle_applied_at_utc = Some(MARKET_START_AT + 5);
    reducer.reduce_metadata(metadata_input(start)).unwrap();

    let mut conflicting = local_transition_row(snapshot, MARKET_START_AT + 6);
    conflicting.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    conflicting.market_lifecycle_transition =
        Some(MetadataMarketLifecycleTransition::CandidateToActive);
    conflicting.lifecycle_scheduled_at_utc = Some(MARKET_START_AT);
    conflicting.lifecycle_applied_at_utc = Some(MARKET_START_AT + 6);

    assert!(
        reducer
            .reduce_metadata(metadata_input(conflicting))
            .is_err()
    );
}

#[test]
fn a_later_metadata_row_cannot_reorder_the_fixed_asset_mapping() {
    let snapshot = metadata_snapshot_row();
    let (mut reducer, _, _) = PolymarketMetadataReducer::from_identity_snapshot(
        metadata_input(snapshot.clone()),
        "polymarket-metadata-reducer-v1",
    )
    .unwrap();
    let mut reordered = local_transition_row(snapshot, MARKET_START_AT + 5);
    reordered.asset_ids = format!(r#"["{DOWN_TOKEN_ID}","{UP_TOKEN_ID}"]"#);

    assert!(reducer.reduce_metadata(metadata_input(reordered)).is_err());
}

#[test]
fn metadata_identity_comparison_accepts_upstream_trim_equivalent_labels() {
    let snapshot = metadata_snapshot_row();
    let (mut reducer, _, _) = PolymarketMetadataReducer::from_identity_snapshot(
        metadata_input(snapshot.clone()),
        "polymarket-metadata-reducer-v1",
    )
    .unwrap();
    let mut equivalent = snapshot;
    equivalent.timestamp_received += 1;
    equivalent.collector_admission_sequence += 1;
    equivalent.collector_admission_time_utc_ms += 1;
    equivalent.source_evidence_received_at_utc = Some(equivalent.timestamp_received);
    equivalent.row_evidence_validation_completed_at_utc = Some(equivalent.timestamp_received + 1);
    equivalent.outcomes_raw = Some(r#"[" UP "," down "]"#.to_owned());

    let batch = reducer.reduce_metadata(metadata_input(equivalent)).unwrap();

    assert!(batch.events.is_empty());
    assert_eq!(batch.reports.len(), 1);
}

#[test]
fn waiting_exhaustion_emits_no_event_isolation_reports_with_precise_reasons() {
    for (status, reason_code) in [
        (
            MetadataResolutionFollowupStatus::BudgetExhaustedAfterValidNonterminal,
            "POLYMARKET_RESOLUTION_WAIT_BUDGET_EXHAUSTED_AFTER_VALID_NONTERMINAL",
        ),
        (
            MetadataResolutionFollowupStatus::BudgetExhaustedWithoutValidResponse,
            "POLYMARKET_RESOLUTION_WAIT_BUDGET_EXHAUSTED_WITHOUT_VALID_RESPONSE",
        ),
    ] {
        let mut snapshot = metadata_snapshot_row();
        snapshot.market_lifecycle_state = MetadataMarketLifecycleState::Active;
        snapshot.market_runtime_role = MetadataMarketRuntimeRole::Current;
        let (mut reducer, _, _) = PolymarketMetadataReducer::from_identity_snapshot(
            metadata_input(snapshot.clone()),
            "polymarket-metadata-reducer-v1",
        )
        .unwrap();
        let summary = resolution_followup_summary_row(snapshot, status, MARKET_END_AT + 480_000);
        let summary_input = metadata_input(summary);
        let summary_evidence = summary_input.evidence.clone();

        let batch = reducer.reduce_metadata(summary_input).unwrap();

        assert!(batch.events.is_empty());
        assert_eq!(batch.reports.len(), 1);
        assert_eq!(batch.reports[0].operation, ReductionOperationKindV1::Reject);
        assert_eq!(batch.reports[0].reason_code, reason_code);
        let ReductionDispositionV1::NoOutput { operation_evidence } = &batch.reports[0].disposition
        else {
            panic!("waiting exhaustion must remain an audit-only no-output result");
        };
        assert!(operation_evidence.iter().any(|evidence| {
            evidence.source_evidence == summary_evidence
                && evidence.source_evidence.source_event_at_utc_ms.is_none()
        }));
    }
}

#[test]
fn resolution_followup_summary_rejects_a_non_exhaustion_status() {
    let row = resolution_followup_summary_row(
        metadata_snapshot_row(),
        MetadataResolutionFollowupStatus::Pending,
        MARKET_END_AT + 480_000,
    );

    assert!(
        source_evidence_from_typed_raw(
            &TypedRawRow::PolymarketMarketMetadata(Box::new(row)),
            &sha('a'),
            &sha('b'),
        )
        .is_err()
    );
}

#[test]
fn prepared_metadata_keeps_initial_lifecycle_and_only_public_narrow_snapshot() {
    use data_pipeline_core::pm_reduction::PreparedPmEvidenceV1;
    use data_pipeline_core::public_input::*;
    use data_pipeline_core::public_reduction::*;
    use data_pipeline_core::public_reference::source_evidence_v3_from_typed_raw_v2;
    let prepare = |row: PolymarketMarketMetadataRow, position| {
        let row = TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(row)));
        let source_evidence = source_evidence_v3_from_typed_raw_v2(&row).unwrap();
        RecordedIngressV1 {
            input_stream_id: "metadata-stream".to_owned(),
            input_position: position,
            ingress: DptIngressV1::Message {
                row,
                source_evidence,
            },
            dependency_record_refs: vec![],
        }
    };
    let initial_row = metadata_snapshot_row();
    let initial = prepare(initial_row.clone(), 0);
    let prepared = prepare_ingress_v1(&initial, None, &Default::default(), &[], "rule-v3").unwrap();
    let PreparedCanonicalInputV1::Metadata(value) = prepared.current else {
        panic!("metadata")
    };
    let context_for = |source: SourceEvidenceRefV3, position| PublicReductionContextV1 {
        input_stream_id: "metadata-stream".to_owned(),
        closed_at_input_position: position,
        first_fact_ordinal: 0,
        first_report_ordinal: 0,
        group_id: source.source_message_id.clone(),
        source_refs: vec![CanonicalInputRecordRefV1 {
            input_stream_id: "metadata-stream".to_owned(),
            input_position: position,
        }],
        direct_sources: vec![source],
    };
    let direct = value.source.source().clone();
    let (mut state, market_context, initial_result) = PolymarketMetadataReducer::<
        PreparedPmEvidenceV1<SourceEvidenceRefV3>,
    >::compute_identity_snapshot(
        PolymarketMetadataInputV1 {
            row: value.row.clone(),
            evidence: value.source,
        },
        value.outcomes,
        value.initial_tick,
        "rule-v3",
    )
    .unwrap();
    let initial_facts =
        bind_initial_market_reduction_v1(&market_context, initial_result, context_for(direct, 0));
    assert!(initial_facts.facts.iter().any(|fact| matches!(
        &fact.payload,
        PublicBusinessPayloadV1::MarketFact {
            payload: CanonicalMarketFactPayloadV1::LifecycleTransition(_),
            ..
        }
    )));
    assert!(!initial_facts.facts.iter().any(|fact| matches!(
        &fact.payload,
        PublicBusinessPayloadV1::MarketFact {
            payload: CanonicalMarketFactPayloadV1::MarketLifecycleSnapshot { .. },
            ..
        }
    )));
    let mut active = initial_row.clone();
    active.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    active.market_runtime_role = MetadataMarketRuntimeRole::Current;
    active.timestamp_received += 1;
    active.source_evidence_received_at_utc = Some(active.timestamp_received);
    active.collector_admission_sequence += 1;
    active.collector_admission_time_utc_ms += 1;
    let recorded = prepare(active.clone(), 1);
    let prepared =
        prepare_ingress_v1(&recorded, None, &Default::default(), &[], "rule-v3").unwrap();
    let PreparedCanonicalInputV1::Metadata(value) = prepared.current else {
        panic!("metadata")
    };
    let direct = value.source.source().clone();
    let result = state
        .compute_metadata(
            PolymarketMetadataInputV1 {
                row: value.row.clone(),
                evidence: value.source.clone(),
            },
            &value.outcomes,
            true,
        )
        .unwrap();
    let hot = bind_market_reduction_v1(result, context_for(direct, 1));
    assert_eq!(
        hot.facts
            .iter()
            .filter(|fact| matches!(
                &fact.payload,
                PublicBusinessPayloadV1::MarketFact {
                    payload: CanonicalMarketFactPayloadV1::MarketLifecycleSnapshot { .. },
                    ..
                }
            ))
            .count(),
        1
    );
    // 推进当前状态后再物化此前初始结果，仍必须保留原 Prefetched。
    let initial_durable = materialize_public_reduction_v1(initial_facts).unwrap();
    assert!(initial_durable.records.iter().any(|record| matches!(
        record,
        CanonicalEventRecordV2::MarketFact(CanonicalMarketFactV1 {
            payload: CanonicalMarketFactPayloadV1::LifecycleTransition(
                MarketLifecycleTransitionV2 {
                    current_state: MappedMarketLifecycleStateV1::Known {
                        value: KnownMarketLifecycleState::Prefetched
                    },
                    ..
                }
            ),
            ..
        })
    )));
    let (mut old, _, _) =
        PolymarketMetadataReducer::from_identity_snapshot(metadata_input(initial_row), "rule-v3")
            .unwrap();
    assert!(
        old.reduce_metadata(metadata_input(active))
            .unwrap_err()
            .to_string()
            .contains("changed without an explicit lifecycle transition")
    );
    let repeat = state
        .compute_metadata(
            PolymarketMetadataInputV1 {
                row: value.row.clone(),
                evidence: value.source,
            },
            &value.outcomes,
            true,
        )
        .unwrap();
    assert!(repeat.events.is_empty());
}

#[test]
fn preparation_preserves_v6_existing_transition_and_followup_row_kinds() {
    use data_pipeline_core::public_input::*;
    use data_pipeline_core::public_reference::source_evidence_v3_from_typed_raw_v2;
    let snapshot = metadata_snapshot_row();
    let mut transition = local_transition_row(snapshot.clone(), MARKET_START_AT + 5);
    transition.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    transition.market_lifecycle_transition =
        Some(MetadataMarketLifecycleTransition::PrefetchedToActive);
    transition.lifecycle_scheduled_at_utc = Some(MARKET_START_AT);
    transition.lifecycle_applied_at_utc = Some(MARKET_START_AT + 5);
    transition.market_runtime_role = MetadataMarketRuntimeRole::Current;
    transition.market_runtime_role_transition =
        Some(MetadataMarketRuntimeRoleTransition::NextToCurrent);
    transition.runtime_role_scheduled_at_utc = Some(MARKET_START_AT);
    transition.runtime_role_applied_at_utc = Some(MARKET_START_AT + 5);
    let followup = resolution_followup_summary_row(
        snapshot.clone(),
        MetadataResolutionFollowupStatus::BudgetExhaustedWithoutValidResponse,
        MARKET_END_AT + 5,
    );
    for (row, expected) in [
        (
            TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
                PolymarketMarketMetadataV6Input::Existing(snapshot),
            )),
            PolymarketMetadataRowKind::MetadataSnapshot,
        ),
        (
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(transition))),
            PolymarketMetadataRowKind::MarketStateTransition,
        ),
        (
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(followup))),
            PolymarketMetadataRowKind::ResolutionFollowupSummary,
        ),
    ] {
        let source_evidence = source_evidence_v3_from_typed_raw_v2(&row).unwrap();
        let input = RecordedIngressV1 {
            input_stream_id: "metadata-kinds".to_owned(),
            input_position: 0,
            ingress: DptIngressV1::Message {
                row,
                source_evidence,
            },
            dependency_record_refs: vec![],
        };
        let prepared = prepare_ingress_v1(&input, None, &Default::default(), &[], "rule").unwrap();
        let PreparedCanonicalInputV1::Metadata(metadata) = prepared.current else {
            panic!("metadata")
        };
        assert_eq!(metadata.row.metadata_row_kind, expected);
        assert_eq!(metadata.outcomes.len(), 2);
    }
}
