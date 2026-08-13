use std::sync::Arc;

use contracts::{
    AssetIdentityV1, CanonicalEventId, CanonicalSourceKind, ClobMarketId,
    CollectorAdmissionCoordinateV1, ConditionId, EventLineageV2, EventQualityV2, EventTimeV2,
    ExactDecimal, ExecutionMarketEventPayloadV3, ExecutionMarketEventV3, InitialL2SnapshotV2,
    MarketDataQuality, MarketIdentityV2, MarketTradingStateV2, MarketWindowV2, OutcomeIdentityV1,
    PriceLevelV1, QualityBoundaryV2, QualityScopeV1, ResyncQuality, Sha256Hex, SourceEvidenceRefV2,
    SourceNativeIdentityV1, TickSizeChangeV2, TokenId,
};
use data_pipeline_core::{derive_execution_market_event_v3_id, encode_execution_market_event_v3};
use rust_decimal::Decimal;

use crate::execution::{
    AuthorizedPlatformRevisionV1, InitialAccountStateV1, PlatformMarketV1, PlatformScenarioV1,
    PostOrderItemV1, PostOrderRequestV1, PostOrdersRequestV1, PreparedBuyOrderV1,
    SimBuyOrderKindV1, SimBuyOrderV1, SimCallerRoleV1, SimExecutionSessionV1,
    SubmissionAdmissionDecisionV1, SubmissionAdmissionRejectionV1, SubmissionEvidenceUnknownV1,
    accepting_submission_response_bridge, execution_process_for_tests,
};
use crate::latency_profile::{
    DelaySampleDomainV1, EmpiricalDelayProfile, load_embedded_proxy_latency_profile,
};
use crate::market::{ClosedMarketEventBucketV1, MillisecondBucketClosureV1};
use crate::platform_rules::tests::successful_snapshot_for_sim_tests_with_itode;
use crate::session::{RequestContextV1, SIM_EXECUTION_CONTRACT_V1, SessionSpecV2};

const MARKET_START_AT_UTC_MS: i64 = 100_000;
const MARKET_END_AT_UTC_MS: i64 = 500_000;

fn sha(byte: char) -> Sha256Hex {
    Sha256Hex(byte.to_string().repeat(64))
}

fn decimal(value: &str) -> Decimal {
    value.parse().expect("test decimal")
}

pub(crate) fn market() -> MarketIdentityV2 {
    MarketIdentityV2 {
        condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
        clob_market_id: ClobMarketId("123".to_owned()),
        window: MarketWindowV2 {
            start_at_utc_ms: MARKET_START_AT_UTC_MS,
            end_at_utc_ms: MARKET_END_AT_UTC_MS,
        },
    }
}

pub(crate) fn asset(token_id: &str, outcome_index: u32, label: &str) -> AssetIdentityV1 {
    AssetIdentityV1 {
        token_id: TokenId(token_id.to_owned()),
        outcome: OutcomeIdentityV1 {
            index: outcome_index,
            label: label.to_owned(),
        },
    }
}

pub(crate) fn scenario() -> Arc<PlatformScenarioV1> {
    let market = PlatformMarketV1::try_new(market(), [asset("2", 1, "Down"), asset("1", 0, "Up")])
        .expect("platform market");
    Arc::new(PlatformScenarioV1::try_new(vec![market]).expect("scenario"))
}

pub(crate) fn session_spec(
    scenario: &PlatformScenarioV1,
    role: SimCallerRoleV1,
    id: &str,
) -> SessionSpecV2 {
    SessionSpecV2 {
        run_id: "component-test-run".to_owned(),
        session_id: id.to_owned(),
        account_id: format!("{id}-account"),
        caller_role: role,
        caller_instance_id: format!("{id}-caller"),
        creation_request_id: format!("{id}-creation"),
        response_bridge_binding_id: format!("{id}-bridge"),
        initial_account_state: InitialAccountStateV1::try_new(decimal("1000"))
            .expect("initial account"),
        response_outbox_capacity: std::num::NonZeroUsize::new(2).expect("positive test capacity"),
        market_set_sha256: scenario.market_set_sha256().clone(),
        scenario_sha256: scenario.scenario_sha256().clone(),
        session_seed: sha('3'),
        execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
        market_event_schema_version: contracts::EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
    }
}

async fn execution_session(
    role: SimCallerRoleV1,
    id: &str,
    itode: bool,
) -> (SimExecutionSessionV1, Arc<EmpiricalDelayProfile>) {
    let rules = successful_snapshot_for_sim_tests_with_itode(itode).await;
    let profile = load_embedded_proxy_latency_profile().expect("profile");
    let scenario = scenario();
    let spec = session_spec(&scenario, role, id);
    let session = execution_process_for_tests(rules, Arc::clone(&profile), scenario)
        .create_active_session_for_tests(spec, accepting_submission_response_bridge());
    (session, profile)
}

fn metadata_evidence(received_at: i64, identity: char) -> SourceEvidenceRefV2 {
    SourceEvidenceRefV2 {
        source_kind: CanonicalSourceKind::PolymarketMarketMetadata,
        source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
            sha256: sha(identity),
        }),
        source_event_at_utc_ms: None,
        received_at_utc_ms: received_at,
        collector_admission: None,
        input_manifest_sha256: sha('a'),
        input_file_sha256: sha('b'),
    }
}

pub(crate) fn orderbook_evidence(
    source_at: i64,
    received_at: i64,
    sequence: u64,
) -> SourceEvidenceRefV2 {
    SourceEvidenceRefV2 {
        source_kind: CanonicalSourceKind::PolymarketOrderbook,
        source_native_identity: None,
        source_event_at_utc_ms: Some(source_at),
        received_at_utc_ms: received_at,
        collector_admission: Some(CollectorAdmissionCoordinateV1 {
            source_sequence_epoch_id: "task22-epoch".to_owned(),
            collector_admission_sequence: sequence,
            admitted_at_utc_ms: received_at,
        }),
        input_manifest_sha256: sha('c'),
        input_file_sha256: sha('d'),
    }
}

pub(crate) fn sealed_event(
    payload: ExecutionMarketEventPayloadV3,
    evidence: SourceEvidenceRefV2,
    quality: EventQualityV2,
) -> ExecutionMarketEventV3 {
    let mut event = ExecutionMarketEventV3 {
        schema_version: contracts::EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
        event_id: CanonicalEventId("0".repeat(64)),
        market: market(),
        time: EventTimeV2 {
            source_event_at_utc_ms: evidence.source_event_at_utc_ms,
            received_at_utc_ms: evidence.received_at_utc_ms,
        },
        lineage: EventLineageV2 {
            canonical_rule_version: "task22-rule-v1".to_owned(),
            source_evidence: vec![evidence],
        },
        quality,
        payload,
    };
    event.event_id = derive_execution_market_event_v3_id(&event).expect("event identity");
    event
}

fn fresh_quality() -> EventQualityV2 {
    EventQualityV2 {
        availability: MarketDataQuality::Fresh,
        resync: ResyncQuality::NotRequired,
    }
}

fn trading_state_event(received_at: i64, state: MarketTradingStateV2) -> ExecutionMarketEventV3 {
    sealed_event(
        ExecutionMarketEventPayloadV3::MarketTradingState(state),
        metadata_evidence(received_at, '8'),
        fresh_quality(),
    )
}

fn initial_snapshot_event(
    snapshot_asset: AssetIdentityV1,
    source_at: i64,
    received_at: i64,
    sequence: u64,
    tick: &str,
    best_ask: &str,
    ask_quantity: &str,
) -> ExecutionMarketEventV3 {
    sealed_event(
        ExecutionMarketEventPayloadV3::InitialL2Snapshot(InitialL2SnapshotV2 {
            book_digest_sha256: sha(if snapshot_asset.token_id.0 == "1" {
                'e'
            } else {
                'f'
            }),
            asset: snapshot_asset,
            initial_tick_size_pusd_per_share: ExactDecimal(tick.to_owned()),
            source_sequence_epoch_id: "task22-epoch".to_owned(),
            source_cursor: sequence,
            bids: vec![PriceLevelV1 {
                price_pusd_per_share: ExactDecimal("0.4".to_owned()),
                quantity_shares: ExactDecimal("10".to_owned()),
            }],
            asks: vec![PriceLevelV1 {
                price_pusd_per_share: ExactDecimal(best_ask.to_owned()),
                quantity_shares: ExactDecimal(ask_quantity.to_owned()),
            }],
        }),
        orderbook_evidence(source_at, received_at, sequence),
        fresh_quality(),
    )
}

pub(crate) fn initial_events(
    tick: &str,
    best_ask: &str,
    ask_quantity: &str,
) -> [ExecutionMarketEventV3; 2] {
    [
        initial_snapshot_event(
            asset("1", 0, "Up"),
            110_000,
            120_000,
            1,
            tick,
            best_ask,
            ask_quantity,
        ),
        initial_snapshot_event(asset("2", 1, "Down"), 110_000, 120_000, 2, tick, "0.6", "5"),
    ]
}

fn tick_event(source_at: i64, sequence: u64, old: &str, new: &str) -> ExecutionMarketEventV3 {
    sealed_event(
        ExecutionMarketEventPayloadV3::TickSizeChange(TickSizeChangeV2 {
            asset: asset("1", 0, "Up"),
            old_tick_size_pusd_per_share: Some(ExactDecimal(old.to_owned())),
            new_tick_size_pusd_per_share: ExactDecimal(new.to_owned()),
        }),
        orderbook_evidence(source_at, source_at + 10_000, sequence),
        fresh_quality(),
    )
}

fn gap_event(source_at: i64, sequence: u64, token_id: &str) -> ExecutionMarketEventV3 {
    let (outcome_index, label) = if token_id == "1" {
        (0, "Up")
    } else {
        (1, "Down")
    };
    sealed_event(
        ExecutionMarketEventPayloadV3::QualityBoundary(QualityBoundaryV2 {
            scope: QualityScopeV1::Asset(asset(token_id, outcome_index, label)),
            previous_quality: MarketDataQuality::Fresh,
            current_quality: MarketDataQuality::Missing,
            reason_code: "POLYMARKET_L2_EPOCH_BASELINE_REQUIRED".to_owned(),
        }),
        orderbook_evidence(source_at, source_at + 10_000, sequence),
        EventQualityV2 {
            availability: MarketDataQuality::Missing,
            resync: ResyncQuality::Required,
        },
    )
}

fn encoded(event: &ExecutionMarketEventV3) -> Vec<u8> {
    encode_execution_market_event_v3(event).expect("canonical event bytes")
}

fn public_bucket(
    at: i64,
    start: u64,
    next: u64,
    events: &[ExecutionMarketEventV3],
) -> ClosedMarketEventBucketV1 {
    ClosedMarketEventBucketV1::public(
        at,
        start,
        next,
        MillisecondBucketClosureV1::NextEventAt(at + 1),
        events.iter().map(encoded).collect::<Vec<_>>(),
    )
}

pub(crate) fn hidden_bucket(
    at: i64,
    start: u64,
    next: u64,
    events: &[ExecutionMarketEventV3],
) -> ClosedMarketEventBucketV1 {
    ClosedMarketEventBucketV1::hidden(
        at,
        start,
        next,
        MillisecondBucketClosureV1::NextEventAt(at + 1),
        events.iter().map(encoded).collect::<Vec<_>>(),
    )
}

fn apply_role_bucket(
    session: &mut SimExecutionSessionV1,
    at: i64,
    start: u64,
    next: u64,
    events: &[ExecutionMarketEventV3],
) {
    let bucket = match session.session_spec().caller_role {
        SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
            hidden_bucket(at, start, next, events)
        }
        SimCallerRoleV1::Paper => public_bucket(at, start, next, events),
    };
    session
        .apply_closed_market_event_bucket(bucket)
        .expect("role-authorized bucket");
}

pub(crate) fn load_initial_market(
    session: &mut SimExecutionSessionV1,
    tick: &str,
    best_ask: &str,
    ask_quantity: &str,
) -> u64 {
    let events = initial_events(tick, best_ask, ask_quantity);
    let at = match session.session_spec().caller_role {
        SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => 110_000,
        SimCallerRoleV1::Paper => 120_000,
    };
    apply_role_bucket(session, at, 0, 2, &events);
    2
}

pub(crate) fn context_for_arrival(
    session: &SimExecutionSessionV1,
    profile: &EmpiricalDelayProfile,
    request_id: &str,
    caller_sequence: u64,
    desired_arrival_at_utc_ms: i64,
) -> RequestContextV1 {
    let delay = profile.sample_delay_ms(
        session.session_spec().session_seed.0.as_bytes(),
        request_id,
        DelaySampleDomainV1::RequestOutbound,
    );
    let delay = i64::try_from(delay).expect("test delay fits i64");
    RequestContextV1 {
        execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
        session_id: session.session_spec().session_id.clone(),
        account_id: session.session_spec().account_id.clone(),
        caller_role: session.session_spec().caller_role,
        caller_instance_id: session.session_spec().caller_instance_id.clone(),
        global_request_id: request_id.to_owned(),
        caller_correlation_id: Some("task22".to_owned()),
        caller_sequence,
        request_sent_at_utc_ms: desired_arrival_at_utc_ms - delay,
        model_visible_cursor: session.model_visible_cursor(),
    }
}

fn gtc_item(item_id: &str) -> PostOrderItemV1 {
    limit_item(item_id, "0.5", "5", false)
}

fn limit_item(item_id: &str, price: &str, size: &str, post_only: bool) -> PostOrderItemV1 {
    PostOrderItemV1 {
        item_id: item_id.to_owned(),
        order: SimBuyOrderV1 {
            condition_id: market().condition_id,
            token_id: TokenId("1".to_owned()),
            kind: SimBuyOrderKindV1::GtcLimit {
                limit_price_pusd_per_share: decimal(price),
                size_shares: decimal(size),
                post_only,
            },
        },
    }
}

fn gtd_item(item_id: &str, price: &str, size: &str, expires_at_utc_ms: i64) -> PostOrderItemV1 {
    PostOrderItemV1 {
        item_id: item_id.to_owned(),
        order: SimBuyOrderV1 {
            condition_id: market().condition_id,
            token_id: TokenId("1".to_owned()),
            kind: SimBuyOrderKindV1::GtdLimit {
                limit_price_pusd_per_share: decimal(price),
                size_shares: decimal(size),
                post_only: false,
                expires_at_utc_ms,
            },
        },
    }
}

fn market_item(item_id: &str, fok: bool, spend: &str, worst_price: &str) -> PostOrderItemV1 {
    let kind = if fok {
        SimBuyOrderKindV1::FokMarket {
            spend_pusd: decimal(spend),
            worst_price_pusd_per_share: decimal(worst_price),
        }
    } else {
        SimBuyOrderKindV1::FakMarket {
            spend_pusd: decimal(spend),
            worst_price_pusd_per_share: decimal(worst_price),
        }
    };
    PostOrderItemV1 {
        item_id: item_id.to_owned(),
        order: SimBuyOrderV1 {
            condition_id: market().condition_id,
            token_id: TokenId("1".to_owned()),
            kind,
        },
    }
}

fn register_order_for_arrival(
    session: &mut SimExecutionSessionV1,
    profile: &EmpiricalDelayProfile,
    request_id: &str,
    caller_sequence: u64,
    desired_arrival_at_utc_ms: i64,
    item: PostOrderItemV1,
) -> crate::execution::PostOrderRegistrationV1 {
    session
        .post_order(PostOrderRequestV1 {
            context: context_for_arrival(
                session,
                profile,
                request_id,
                caller_sequence,
                desired_arrival_at_utc_ms,
            ),
            idempotency_key: format!("{request_id}-idem"),
            item,
        })
        .expect("registration")
}

fn close_take_and_prepare(
    session: &mut SimExecutionSessionV1,
    effective_at: i64,
    index_position: u64,
) -> (
    crate::session::QueuedLogicalActionV1,
    SubmissionAdmissionDecisionV1,
) {
    apply_role_bucket(session, effective_at, index_position, index_position, &[]);
    let action = session
        .take_next_action_through(effective_at)
        .expect("take action")
        .expect("one action");
    let decision = session.prepare_submission(&action).expect("pure admission");
    (action, decision)
}

async fn evaluate_ready_order(
    id: &str,
    itode: bool,
    tick: &str,
    best_ask: &str,
    ask_quantity: &str,
    desired_arrival_at_utc_ms: i64,
    item: PostOrderItemV1,
) -> (
    SimExecutionSessionV1,
    crate::execution::PostOrderRegistrationV1,
    crate::session::QueuedLogicalActionV1,
    SubmissionAdmissionDecisionV1,
) {
    let (mut session, profile) = execution_session(SimCallerRoleV1::ModelTrain, id, itode).await;
    load_initial_market(&mut session, tick, best_ask, ask_quantity);
    let registration = register_order_for_arrival(
        &mut session,
        &profile,
        &format!("{id}-request"),
        1,
        desired_arrival_at_utc_ms,
        item,
    );
    let (action, decision) = close_take_and_prepare(
        &mut session,
        registration.item.effective_processing_at_utc_ms,
        2,
    );
    (session, registration, action, decision)
}

#[tokio::test(flavor = "current_thread")]
async fn task22_end_at_outranks_every_lower_gate() {
    let (mut session, profile) = execution_session(SimCallerRoleV1::Paper, "end-gate", true).await;
    let flags = trading_state_event(
        130_000,
        MarketTradingStateV2 {
            active: Some(false),
            closed: None,
            accepting_orders: None,
        },
    );
    session
        .apply_closed_market_event_bucket(public_bucket(130_000, 0, 1, &[flags]))
        .expect("public flags");

    let registration = session
        .post_order(PostOrderRequestV1 {
            context: context_for_arrival(
                &session,
                &profile,
                "end-request",
                1,
                MARKET_END_AT_UTC_MS,
            ),
            idempotency_key: "end-idem".to_owned(),
            item: gtc_item("end-item"),
        })
        .expect("registration");
    assert_eq!(
        registration.item.effective_processing_at_utc_ms,
        MARKET_END_AT_UTC_MS
    );
    session
        .apply_closed_market_event_bucket(public_bucket(MARKET_END_AT_UTC_MS, 1, 1, &[]))
        .expect("close through effective time");
    let action = session
        .take_next_action_through(MARKET_END_AT_UTC_MS)
        .expect("take action")
        .expect("one action");
    let decision = session.prepare_submission(&action).expect("pure admission");
    assert_eq!(
        decision,
        SubmissionAdmissionDecisionV1::RejectedNoEffect(
            SubmissionAdmissionRejectionV1::EndAtReached
        )
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task22_unknown_market_and_asset_are_rejected_before_evidence_gates() {
    let mut unknown_market = gtc_item("unknown-market-item");
    unknown_market.order.condition_id = ConditionId(format!("0x{}", "9".repeat(64)));
    let mut unknown_asset = gtc_item("unknown-asset-item");
    unknown_asset.order.token_id = TokenId("foreign-token".to_owned());

    for (id, item, expected) in [
        (
            "unknown-market",
            unknown_market,
            SubmissionAdmissionRejectionV1::MarketNotInScenario,
        ),
        (
            "unknown-asset",
            unknown_asset,
            SubmissionAdmissionRejectionV1::AssetNotInMarket,
        ),
    ] {
        let (mut session, profile) = execution_session(SimCallerRoleV1::ModelTrain, id, true).await;
        let registration = register_order_for_arrival(
            &mut session,
            &profile,
            &format!("{id}-request"),
            1,
            200_000,
            item,
        );
        let (_, decision) = close_take_and_prepare(
            &mut session,
            registration.item.effective_processing_at_utc_ms,
            0,
        );
        assert_eq!(
            decision,
            SubmissionAdmissionDecisionV1::RejectedNoEffect(expected)
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task22_nullable_trading_flags_are_independent_vetoes() {
    let states = [
        MarketTradingStateV2 {
            active: Some(false),
            closed: None,
            accepting_orders: None,
        },
        MarketTradingStateV2 {
            active: None,
            closed: Some(true),
            accepting_orders: None,
        },
        MarketTradingStateV2 {
            active: None,
            closed: None,
            accepting_orders: Some(false),
        },
    ];
    for (index, state) in states.into_iter().enumerate() {
        let id = format!("flags-{index}");
        let (mut session, profile) = execution_session(SimCallerRoleV1::Paper, &id, true).await;
        load_initial_market(&mut session, "0.01", "0.6", "5");
        let flags = trading_state_event(130_000, state);
        session
            .apply_closed_market_event_bucket(public_bucket(130_000, 2, 3, &[flags]))
            .expect("public flags");
        let registration = register_order_for_arrival(
            &mut session,
            &profile,
            &format!("flags-request-{index}"),
            1,
            200_000,
            gtc_item(&format!("flags-item-{index}")),
        );
        let (_, decision) = close_take_and_prepare(
            &mut session,
            registration.item.effective_processing_at_utc_ms,
            3,
        );
        assert_eq!(
            decision,
            SubmissionAdmissionDecisionV1::RejectedNoEffect(
                SubmissionAdmissionRejectionV1::ExplicitTradingStateVeto
            )
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task22_one_predicate_uses_only_the_role_authorized_projection() {
    for (role, id) in [
        (SimCallerRoleV1::ModelTrain, "projection-train"),
        (SimCallerRoleV1::OfflineEvaluation, "projection-offline"),
    ] {
        let (mut session, profile) = execution_session(role, id, true).await;
        load_initial_market(&mut session, "0.01", "0.6", "5");
        let flags = trading_state_event(
            130_000,
            MarketTradingStateV2 {
                active: None,
                closed: None,
                accepting_orders: Some(false),
            },
        );
        session
            .apply_closed_market_event_bucket(public_bucket(130_000, 0, 1, &[flags]))
            .expect("public-only veto");
        let registration = register_order_for_arrival(
            &mut session,
            &profile,
            &format!("{id}-request"),
            1,
            200_000,
            gtc_item(&format!("{id}-item")),
        );
        let (_, decision) = close_take_and_prepare(
            &mut session,
            registration.item.effective_processing_at_utc_ms,
            2,
        );
        let SubmissionAdmissionDecisionV1::Prepared(prepared) = decision else {
            panic!("public-only flags must not veto the hidden projection");
        };
        let AuthorizedPlatformRevisionV1::EventTime(cursor) = prepared.authorized_platform_revision
        else {
            panic!("historical roles must bind the event-time revision");
        };
        assert_eq!(
            cursor.closed_through_source_event_at_utc_ms(),
            Some(registration.item.effective_processing_at_utc_ms)
        );
        assert_eq!(cursor.next_event_time_index_position(), 2);
    }

    let (mut paper, profile) =
        execution_session(SimCallerRoleV1::Paper, "projection-paper", true).await;
    load_initial_market(&mut paper, "0.01", "0.6", "5");
    let flags = trading_state_event(
        130_000,
        MarketTradingStateV2 {
            active: None,
            closed: None,
            accepting_orders: Some(false),
        },
    );
    paper
        .apply_closed_market_event_bucket(public_bucket(130_000, 2, 3, &[flags]))
        .expect("paper veto");
    let registration = register_order_for_arrival(
        &mut paper,
        &profile,
        "projection-paper-request",
        1,
        200_000,
        gtc_item("projection-paper-item"),
    );
    let (_, decision) = close_take_and_prepare(
        &mut paper,
        registration.item.effective_processing_at_utc_ms,
        3,
    );
    assert_eq!(
        decision,
        SubmissionAdmissionDecisionV1::RejectedNoEffect(
            SubmissionAdmissionRejectionV1::ExplicitTradingStateVeto
        )
    );

    let (mut paper, profile) =
        execution_session(SimCallerRoleV1::Paper, "projection-paper-neutral", true).await;
    load_initial_market(&mut paper, "0.01", "0.6", "5");
    let registration = register_order_for_arrival(
        &mut paper,
        &profile,
        "projection-paper-neutral-request",
        1,
        200_000,
        gtc_item("projection-paper-neutral-item"),
    );
    let (_, decision) = close_take_and_prepare(
        &mut paper,
        registration.item.effective_processing_at_utc_ms,
        2,
    );
    let SubmissionAdmissionDecisionV1::Prepared(prepared) = decision else {
        panic!("neutral paper projection should prepare");
    };
    let AuthorizedPlatformRevisionV1::ReceivedTime(cursor) = prepared.authorized_platform_revision
    else {
        panic!("paper must bind the received-time revision");
    };
    assert_eq!(
        cursor.closed_through_received_at_utc_ms(),
        Some(registration.item.effective_processing_at_utc_ms)
    );
    assert_eq!(cursor.next_visibility_index_position(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn task22_preloading_precedes_quality_and_quality_is_target_local() {
    for (token_id, outcome_index, label, id) in [
        ("1", 0, "Up", "preload-target"),
        ("2", 1, "Down", "preload-other"),
    ] {
        let (mut session, profile) = execution_session(SimCallerRoleV1::ModelTrain, id, true).await;
        let initial = initial_snapshot_event(
            asset(token_id, outcome_index, label),
            110_000,
            120_000,
            1,
            "0.01",
            "0.6",
            "5",
        );
        session
            .apply_closed_market_event_bucket(hidden_bucket(110_000, 0, 1, &[initial]))
            .expect("one initial asset");
        let registration = register_order_for_arrival(
            &mut session,
            &profile,
            &format!("{id}-request"),
            1,
            200_000,
            gtc_item(&format!("{id}-item")),
        );
        let (_, decision) = close_take_and_prepare(
            &mut session,
            registration.item.effective_processing_at_utc_ms,
            1,
        );
        assert_eq!(
            decision,
            SubmissionAdmissionDecisionV1::EvidenceUnknownNoEffect(
                SubmissionEvidenceUnknownV1::Preloading
            )
        );
    }

    for (gap_token, expected_unknown, id) in
        [("1", true, "quality-target"), ("2", false, "quality-other")]
    {
        let (mut session, profile) = execution_session(SimCallerRoleV1::ModelTrain, id, true).await;
        load_initial_market(&mut session, "0.01", "0.6", "5");
        let gap = gap_event(111_000, 3, gap_token);
        session
            .apply_closed_market_event_bucket(hidden_bucket(111_000, 2, 3, &[gap]))
            .expect("gap boundary");
        let registration = register_order_for_arrival(
            &mut session,
            &profile,
            &format!("{id}-request"),
            1,
            200_000,
            gtc_item(&format!("{id}-item")),
        );
        let (_, decision) = close_take_and_prepare(
            &mut session,
            registration.item.effective_processing_at_utc_ms,
            3,
        );
        if expected_unknown {
            assert_eq!(
                decision,
                SubmissionAdmissionDecisionV1::EvidenceUnknownNoEffect(
                    SubmissionEvidenceUnknownV1::TargetAssetNotFresh(MarketDataQuality::Missing)
                )
            );
        } else {
            assert!(matches!(
                decision,
                SubmissionAdmissionDecisionV1::Prepared(_)
            ));
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task22_dynamic_tick_uses_the_shared_sdk_price_and_amount_rules() {
    for (id, price, expected_prepared) in [
        ("dynamic-valid", "0.50001", true),
        ("dynamic-over-scale", "0.500015", false),
    ] {
        let (mut session, profile) = execution_session(SimCallerRoleV1::ModelTrain, id, true).await;
        load_initial_market(&mut session, "0.01", "0.6", "5");
        for (at, sequence, start, old, new) in [
            (111_000, 3, 2, "0.01", "0.001"),
            (112_000, 4, 3, "0.001", "0.0001"),
            (113_000, 5, 4, "0.0001", "0.00001"),
        ] {
            let tick = tick_event(at, sequence, old, new);
            session
                .apply_closed_market_event_bucket(hidden_bucket(at, start, start + 1, &[tick]))
                .expect("dynamic tick");
        }
        let registration = register_order_for_arrival(
            &mut session,
            &profile,
            &format!("{id}-request"),
            1,
            200_000,
            limit_item(&format!("{id}-item"), price, "5", false),
        );
        let (_, decision) = close_take_and_prepare(
            &mut session,
            registration.item.effective_processing_at_utc_ms,
            5,
        );
        if expected_prepared {
            let SubmissionAdmissionDecisionV1::Prepared(prepared) = decision else {
                panic!("aligned five-decimal price must prepare");
            };
            assert_eq!(
                prepared.order,
                PreparedBuyOrderV1::GtcLimit {
                    limit_price_pusd_per_share: decimal("0.50001"),
                    maker_amount_pusd: decimal("2.50005"),
                    taker_amount_shares: decimal("5"),
                    post_only: false,
                }
            );
        } else {
            assert_eq!(
                decision,
                SubmissionAdmissionDecisionV1::RejectedNoEffect(
                    SubmissionAdmissionRejectionV1::OrderEncodingRejected
                )
            );
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task22_limit_minimum_post_only_and_principal_gates_are_ordered() {
    let cases = [
        (
            "limit-below-mos",
            "0.6",
            limit_item("below-mos", "0.5", "4.99", false),
            Some(SubmissionAdmissionRejectionV1::BelowMinimumShares),
        ),
        (
            "limit-post-only",
            "0.6",
            limit_item("post-only", "0.6", "5", true),
            Some(SubmissionAdmissionRejectionV1::PostOnlyWouldTake),
        ),
        (
            "limit-below-principal",
            "0.1",
            limit_item("below-principal", "0.1", "5", false),
            Some(SubmissionAdmissionRejectionV1::BelowMarketableLimitBuyMinimumPrincipal),
        ),
        (
            "limit-principal-equal",
            "0.2",
            limit_item("principal-equal", "0.2", "5", false),
            None,
        ),
        (
            "limit-nonmarketable-small",
            "0.6",
            limit_item("nonmarketable-small", "0.1", "5", false),
            None,
        ),
    ];
    for (id, best_ask, item, rejection) in cases {
        let (_, _, _, decision) =
            evaluate_ready_order(id, true, "0.01", best_ask, "5", 200_000, item).await;
        if let Some(rejection) = rejection {
            assert_eq!(
                decision,
                SubmissionAdmissionDecisionV1::RejectedNoEffect(rejection)
            );
        } else {
            assert!(matches!(
                decision,
                SubmissionAdmissionDecisionV1::Prepared(_)
            ));
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task22_gtd_uses_floor_expiry_ceil_processing_and_the_240_second_boundary() {
    let cases = [
        ("gtd-ceil-equal", 150_001, 391_999, Some(391_000)),
        ("gtd-ceil-short", 150_001, 390_999, None),
        ("gtd-exact-second", 150_000, 390_000, Some(390_000)),
    ];
    for (id, effective_at, requested_expiry, normalized_expiry) in cases {
        let (_, registration, _, decision) = evaluate_ready_order(
            id,
            true,
            "0.01",
            "0.6",
            "5",
            effective_at,
            gtd_item(id, "0.5", "5", requested_expiry),
        )
        .await;
        assert_eq!(
            registration.item.effective_processing_at_utc_ms,
            effective_at
        );
        if let Some(expires_at_utc_ms) = normalized_expiry {
            let SubmissionAdmissionDecisionV1::Prepared(prepared) = decision else {
                panic!("GTD boundary should prepare");
            };
            assert_eq!(
                prepared.order,
                PreparedBuyOrderV1::GtdLimit {
                    limit_price_pusd_per_share: decimal("0.5"),
                    maker_amount_pusd: decimal("2.5"),
                    taker_amount_shares: decimal("5"),
                    post_only: false,
                    expires_at_utc_ms,
                }
            );
        } else {
            assert_eq!(
                decision,
                SubmissionAdmissionDecisionV1::RejectedNoEffect(
                    SubmissionAdmissionRejectionV1::GtdExpirationTooSoon
                )
            );
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task22_fok_fak_compare_final_sdk_taker_shares_and_itode_is_conditional() {
    for (fok, label) in [(true, "fok"), (false, "fak")] {
        for (spend, expected_prepared, suffix) in
            [("3", true, "equal"), ("2.999999", false, "below")]
        {
            let id = format!("{label}-{suffix}");
            let (_, registration, _, decision) = evaluate_ready_order(
                &id,
                true,
                "0.00001",
                "0.6",
                "1",
                200_000,
                market_item(&id, fok, spend, "0.6"),
            )
            .await;
            assert_eq!(
                registration.item.effective_processing_at_utc_ms
                    - registration.receipt.transit.platform_arrived_at_utc_ms,
                250
            );
            if expected_prepared {
                let SubmissionAdmissionDecisionV1::Prepared(prepared) = decision else {
                    panic!("encoded takerAmount equal to mos must prepare");
                };
                let expected = if fok {
                    PreparedBuyOrderV1::FokMarket {
                        worst_price_pusd_per_share: decimal("0.6"),
                        maker_amount_pusd: decimal("3"),
                        taker_amount_shares: decimal("5"),
                    }
                } else {
                    PreparedBuyOrderV1::FakMarket {
                        worst_price_pusd_per_share: decimal("0.6"),
                        maker_amount_pusd: decimal("3"),
                        taker_amount_shares: decimal("5"),
                    }
                };
                assert_eq!(prepared.order, expected);
            } else {
                assert_eq!(
                    decision,
                    SubmissionAdmissionDecisionV1::RejectedNoEffect(
                        SubmissionAdmissionRejectionV1::BelowMinimumShares
                    )
                );
            }
        }
    }

    let (_, _, _, no_principal_gate) = evaluate_ready_order(
        "fok-no-principal",
        true,
        "0.00001",
        "0.1",
        "1",
        200_000,
        market_item("fok-no-principal", true, "0.5", "0.1"),
    )
    .await;
    assert!(matches!(
        no_principal_gate,
        SubmissionAdmissionDecisionV1::Prepared(_)
    ));

    let (_, registration, _, decision) = evaluate_ready_order(
        "fak-itode-false",
        false,
        "0.00001",
        "0.6",
        "1",
        200_000,
        market_item("fak-itode-false", false, "3", "0.6"),
    )
    .await;
    assert_eq!(
        registration.item.effective_processing_at_utc_ms,
        registration.receipt.transit.platform_arrived_at_utc_ms
    );
    assert!(matches!(
        decision,
        SubmissionAdmissionDecisionV1::Prepared(_)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn task22_itode_rechecks_the_complete_gate_at_plus_250ms() {
    for (id, desired_arrival, expected) in [
        (
            "itode-quality",
            200_000,
            SubmissionAdmissionDecisionV1::EvidenceUnknownNoEffect(
                SubmissionEvidenceUnknownV1::TargetAssetNotFresh(MarketDataQuality::Missing),
            ),
        ),
        (
            "itode-end",
            MARKET_END_AT_UTC_MS - 250,
            SubmissionAdmissionDecisionV1::RejectedNoEffect(
                SubmissionAdmissionRejectionV1::EndAtReached,
            ),
        ),
    ] {
        let (mut session, profile) = execution_session(SimCallerRoleV1::ModelTrain, id, true).await;
        load_initial_market(&mut session, "0.00001", "0.6", "1");
        let registration = register_order_for_arrival(
            &mut session,
            &profile,
            &format!("{id}-request"),
            1,
            desired_arrival,
            market_item(&format!("{id}-item"), true, "3", "0.6"),
        );
        assert_eq!(
            registration.item.effective_processing_at_utc_ms - desired_arrival,
            250
        );
        let gap_at = desired_arrival + 100;
        let gap = gap_event(gap_at, 3, "1");
        session
            .apply_closed_market_event_bucket(hidden_bucket(gap_at, 2, 3, &[gap]))
            .expect("gap while deferred");
        let (_, decision) = close_take_and_prepare(
            &mut session,
            registration.item.effective_processing_at_utc_ms,
            3,
        );
        assert_eq!(decision, expected);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task22_batch_mapping_and_physical_retry_have_no_admission_side_effects() {
    let (mut session, profile) =
        execution_session(SimCallerRoleV1::ModelTrain, "batch-retry", true).await;
    load_initial_market(&mut session, "0.01", "0.6", "5");
    let request = PostOrdersRequestV1 {
        context: context_for_arrival(&session, &profile, "batch-retry-request", 1, 200_000),
        idempotency_key: "batch-retry-idem".to_owned(),
        items: vec![
            limit_item("batch-good", "0.5", "5", false),
            limit_item("batch-rejected", "0.5", "4.99", false),
        ],
    };
    let registration = session
        .post_orders(request.clone())
        .expect("batch registration");
    let retry = session
        .post_orders(request.clone())
        .expect("physical retry");
    assert_eq!(registration.items, retry.items);
    assert_eq!(registration.receipt.transit, retry.receipt.transit);
    assert_eq!(
        retry.receipt.disposition,
        crate::session::RequestRegistrationDispositionV1::PhysicalRetry
    );
    assert_eq!(registration.items[0].scheduled_action_ordinal, 0);
    assert_eq!(registration.items[1].scheduled_action_ordinal, 1);
    let effective_at = registration.items[0].effective_processing_at_utc_ms;
    assert_eq!(
        registration.items[1].effective_processing_at_utc_ms,
        effective_at
    );

    let before_market = session
        .platform_market_state(&market().condition_id.0)
        .expect("market")
        .clone();
    let before_private = session.model_visible_cursor().private_feedback_sequence;
    let before_overlay = session.account_overlay_quantity(
        &market().condition_id.0,
        "1",
        contracts::BookSide::Bid,
        decimal("0.4"),
    );
    apply_role_bucket(&mut session, effective_at, 2, 2, &[]);

    let first_action = session
        .take_next_action_through(effective_at)
        .expect("take first")
        .expect("first action");
    let mut forged_time = first_action.clone();
    forged_time.key.effective_processing_at_utc_ms -= 1;
    assert!(matches!(
        session.prepare_submission(&forged_time),
        Err(crate::execution::SubmissionPreparationErrorV1::Session(
            crate::session::SessionError::ActionIdentityConflict(_)
        ))
    ));
    let mut forged_sequence = first_action.clone();
    forged_sequence.key.caller_sequence += 1;
    assert!(matches!(
        session.prepare_submission(&forged_sequence),
        Err(crate::execution::SubmissionPreparationErrorV1::Session(
            crate::session::SessionError::ActionIdentityConflict(_)
        ))
    ));
    let first = session
        .prepare_submission(&first_action)
        .expect("first admission");
    let SubmissionAdmissionDecisionV1::Prepared(prepared) = first else {
        panic!("first batch item should prepare");
    };
    assert_eq!(prepared.global_request_id, "batch-retry-request");
    assert_eq!(prepared.action_identity, first_action.action_identity);
    assert_eq!(prepared.item_id, "batch-good");
    assert_eq!(prepared.batch_item_ordinal, 0);
    assert_eq!(prepared.effective_processing_at_utc_ms, effective_at);
    assert_eq!(prepared.condition_id, market().condition_id);
    assert_eq!(prepared.token_id, TokenId("1".to_owned()));
    assert!(matches!(
        prepared.authorized_platform_revision,
        AuthorizedPlatformRevisionV1::EventTime(_)
    ));
    let in_flight_retry = session
        .post_orders(request.clone())
        .expect("in-flight retry");
    assert_eq!(registration.items, in_flight_retry.items);
    session
        .complete_logical_item(&first_action.action_identity)
        .expect("task 2.3 consumer completes first item");
    assert!(matches!(
        session.prepare_submission(&first_action),
        Err(crate::execution::SubmissionPreparationErrorV1::Session(
            crate::session::SessionError::ActionNotLogicallyReleased(_)
        ))
    ));

    let second_action = session
        .take_next_action_through(effective_at)
        .expect("take second")
        .expect("second action");
    assert_eq!(second_action.key.batch_item_ordinal, 1);
    assert_eq!(
        session
            .prepare_submission(&second_action)
            .expect("second admission"),
        SubmissionAdmissionDecisionV1::RejectedNoEffect(
            SubmissionAdmissionRejectionV1::BelowMinimumShares
        )
    );
    session
        .complete_logical_item(&second_action.action_identity)
        .expect("task 2.3 consumer completes second item");
    let completed_retry = session.post_orders(request).expect("completed retry");
    assert_eq!(registration.items, completed_retry.items);
    assert!(
        session
            .take_next_action_through(effective_at)
            .expect("no requeued action")
            .is_none()
    );

    assert_eq!(
        session
            .platform_market_state(&market().condition_id.0)
            .expect("market"),
        &before_market
    );
    assert_eq!(
        session.account_overlay_quantity(
            &market().condition_id.0,
            "1",
            contracts::BookSide::Bid,
            decimal("0.4"),
        ),
        before_overlay
    );
    assert_eq!(
        session.model_visible_cursor().private_feedback_sequence,
        before_private
    );
    assert_eq!(
        registration.receipt.transit.platform_responded_at_utc_ms,
        None
    );
    assert_eq!(registration.receipt.transit.private_emission_sequence, None);
    assert_eq!(
        registration.receipt.transit.response_received_at_utc_ms,
        None
    );
}
