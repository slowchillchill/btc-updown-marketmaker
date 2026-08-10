use std::sync::Arc;

use contracts::{
    AssetIdentityV1, BestBidAskV2, BookSide, CanonicalEventId, CanonicalSourceKind, ClobMarketId,
    CollectorAdmissionCoordinateV1, ConditionId, EventLineageV2, EventQualityV2, EventTimeV2,
    ExactDecimal, ExecutionMarketEventPayloadV3, ExecutionMarketEventV3, InitialL2SnapshotV2,
    KnownMarketLifecycleState, LevelExecutionBasis, LevelExecutionTransitionV3,
    MappedMarketLifecycleStateV1, MarketDataQuality, MarketIdentityV2, MarketLifecycleTransitionV2,
    MarketResolutionTerminalV3, MarketTradingStateV2, MarketWindowV2, OutcomeIdentityV1,
    PayoutEntryV1, PriceLevelV1, QualityBoundaryV2, QualityScopeV1, QueueResultQuality,
    ResyncQuality, Sha256Hex, SourceAttributionQuality, SourceEvidenceRefV2,
    SourceNativeIdentityV1, TickSizeChangeV2, TokenId,
};
use data_pipeline_core::{derive_execution_market_event_v3_id, encode_execution_market_event_v3};
use rust_decimal::Decimal;

use crate::execution::{
    InitialAccountStateV1, PlatformMarketV1, PlatformScenarioV1, SimCallerRoleV1,
    SimExecutionProcessV1, accepting_submission_response_bridge,
};
use crate::latency_profile::load_embedded_proxy_latency_profile;
use crate::market::{
    ClosedMarketEventBucketV1, MarketBucketApplyDispositionV1, MarketEventErrorV1,
    MarketLifecyclePhaseV1, MillisecondBucketClosureV1,
};
use crate::platform_rules::tests::successful_snapshot_for_sim_tests;
use crate::session::{SIM_EXECUTION_CONTRACT_V1, SessionSpecV1};

pub(crate) fn market() -> MarketIdentityV2 {
    MarketIdentityV2 {
        condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
        clob_market_id: ClobMarketId("123".to_owned()),
        window: MarketWindowV2 {
            start_at_utc_ms: 1_000,
            end_at_utc_ms: 2_000,
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
) -> SessionSpecV1 {
    SessionSpecV1 {
        session_id: id.to_owned(),
        account_id: format!("{id}-account"),
        caller_role: role,
        caller_instance_id: format!("{id}-caller"),
        initial_account_state: InitialAccountStateV1::try_new(
            "1000".parse().expect("initial account amount"),
        )
        .expect("initial account"),
        response_outbox_capacity: std::num::NonZeroUsize::new(2).expect("positive test capacity"),
        market_set_sha256: scenario.market_set_sha256().clone(),
        scenario_sha256: scenario.scenario_sha256().clone(),
        session_seed: Sha256Hex("3".repeat(64)),
        execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
        market_event_schema_version: contracts::EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
    }
}

async fn execution_session(
    role: SimCallerRoleV1,
    id: &str,
) -> crate::execution::SimExecutionSessionV1 {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("profile");
    let scenario = scenario();
    let spec = session_spec(&scenario, role, id);
    SimExecutionProcessV1::new(rules, profile, scenario)
        .create_session(spec, accepting_submission_response_bridge())
        .expect("task 2.1 session")
}

fn sha(byte: char) -> Sha256Hex {
    Sha256Hex(byte.to_string().repeat(64))
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
            source_sequence_epoch_id: "epoch-1".to_owned(),
            collector_admission_sequence: sequence,
            admitted_at_utc_ms: received_at,
        }),
        input_manifest_sha256: sha('a'),
        input_file_sha256: sha('b'),
    }
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
            canonical_rule_version: "task21-rule-v1".to_owned(),
            source_evidence: vec![evidence],
        },
        quality,
        payload,
    };
    event.event_id = derive_execution_market_event_v3_id(&event).expect("event identity");
    event
}

pub(crate) fn fresh_quality() -> EventQualityV2 {
    EventQualityV2 {
        availability: MarketDataQuality::Fresh,
        resync: ResyncQuality::NotRequired,
    }
}

pub(crate) fn initial_snapshot_event(
    token_id: &str,
    outcome_index: u32,
    label: &str,
    source_at: i64,
    received_at: i64,
    sequence: u64,
    quantity: &str,
) -> ExecutionMarketEventV3 {
    sealed_event(
        ExecutionMarketEventPayloadV3::InitialL2Snapshot(InitialL2SnapshotV2 {
            asset: asset(token_id, outcome_index, label),
            initial_tick_size_pusd_per_share: ExactDecimal("0.01".to_owned()),
            source_sequence_epoch_id: "epoch-1".to_owned(),
            source_cursor: sequence,
            book_digest_sha256: sha(if token_id == "1" { 'e' } else { 'f' }),
            bids: vec![PriceLevelV1 {
                price_pusd_per_share: ExactDecimal("0.4".to_owned()),
                quantity_shares: ExactDecimal(quantity.to_owned()),
            }],
            asks: vec![PriceLevelV1 {
                price_pusd_per_share: ExactDecimal("0.6".to_owned()),
                quantity_shares: ExactDecimal("5".to_owned()),
            }],
        }),
        orderbook_evidence(source_at, received_at, sequence),
        fresh_quality(),
    )
}

fn encoded(event: &ExecutionMarketEventV3) -> Vec<u8> {
    encode_execution_market_event_v3(event).expect("canonical event bytes")
}

pub(crate) fn hidden_bucket(
    at: i64,
    next_position: u64,
    events: &[ExecutionMarketEventV3],
) -> ClosedMarketEventBucketV1 {
    let event_count = u64::try_from(events.len()).expect("test event count fits u64");
    ClosedMarketEventBucketV1::hidden(
        at,
        next_position
            .checked_sub(event_count)
            .expect("test bucket positions cover its events"),
        next_position,
        MillisecondBucketClosureV1::NextEventAt(at + 1),
        events.iter().map(encoded).collect::<Vec<_>>(),
    )
}

fn public_bucket(
    at: i64,
    next_position: u64,
    events: &[ExecutionMarketEventV3],
) -> ClosedMarketEventBucketV1 {
    let event_count = u64::try_from(events.len()).expect("test event count fits u64");
    ClosedMarketEventBucketV1::public(
        at,
        next_position
            .checked_sub(event_count)
            .expect("test bucket positions cover its events"),
        next_position,
        MillisecondBucketClosureV1::NextEventAt(at + 1),
        events.iter().map(encoded).collect::<Vec<_>>(),
    )
}

#[test]
fn task21_scenario_is_canonical_and_content_addressed() {
    let scenario = scenario();
    assert_eq!(
        scenario.market_set_sha256().0,
        "ea656eb3ebcf22211cc912a3de56544f35df88eef4e2abfecccbb7044631ddf1"
    );
    assert_eq!(
        scenario.scenario_sha256().0,
        "59bda2cf491b89f2b7a3809361e5038c96e550a9725110643b69528f05e250ad"
    );
    let member = &scenario.market_set().markets()[0];
    assert_eq!(member.assets()[0].token_id.0, "1");
    assert_eq!(member.assets()[1].token_id.0, "2");
    assert_eq!(scenario.market_set_sha256().0.len(), 64);
    assert_eq!(scenario.scenario_sha256().0.len(), 64);
    assert_ne!(
        scenario.market_set_sha256(),
        scenario.scenario_sha256(),
        "两个内容身份必须使用独立摘要域"
    );
    assert!(
        PlatformMarketV1::try_new(market(), [asset("1", 0, "Up"), asset("1", 1, "Down")],).is_err()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task21_process_rejects_session_hashes_not_bound_to_injected_scenario() {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("profile");
    let scenario = scenario();
    let mut process = SimExecutionProcessV1::new(rules, profile, Arc::clone(&scenario));
    let mut spec = session_spec(&scenario, SimCallerRoleV1::ModelTrain, "task21-session");
    spec.scenario_sha256 = contracts::Sha256Hex("4".repeat(64));
    assert!(
        process
            .create_session(spec, accepting_submission_response_bridge())
            .is_err()
    );
    let mut spec = session_spec(&scenario, SimCallerRoleV1::ModelTrain, "task21-session");
    spec.market_set_sha256 = contracts::Sha256Hex("5".repeat(64));
    assert!(
        process
            .create_session(spec, accepting_submission_response_bridge())
            .is_err()
    );
    process
        .create_session(
            session_spec(&scenario, SimCallerRoleV1::ModelTrain, "task21-session"),
            accepting_submission_response_bridge(),
        )
        .expect("摘要错误不得占用 session_id");
}

#[tokio::test(flavor = "current_thread")]
async fn task21_events_must_match_the_full_scenario_market_and_expected_assets() {
    let mut session = execution_session(SimCallerRoleV1::ModelTrain, "scope").await;
    let mut wrong_market = initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10");
    wrong_market.market.clob_market_id = ClobMarketId("999".to_owned());
    wrong_market.event_id =
        derive_execution_market_event_v3_id(&wrong_market).expect("wrong-market event identity");
    assert!(matches!(
        session.apply_closed_market_event_bucket(hidden_bucket(1_100, 1, &[wrong_market])),
        Err(MarketEventErrorV1::MarketIdentityConflict(_))
    ));

    let third_asset = initial_snapshot_event("3", 2, "Other", 1_100, 1_200, 2, "9");
    assert!(matches!(
        session.apply_closed_market_event_bucket(hidden_bucket(1_100, 1, &[third_asset])),
        Err(MarketEventErrorV1::UnknownAsset(id)) if id == "3"
    ));
    assert_eq!(session.platform_event_index_position(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn task21_end_of_input_fails_before_watermark_when_initial_assets_are_missing() {
    let mut session = execution_session(SimCallerRoleV1::ModelTrain, "incomplete-eof").await;
    let bucket = ClosedMarketEventBucketV1::hidden(
        1_100,
        0,
        0,
        MillisecondBucketClosureV1::EndOfInput,
        Vec::new(),
    );
    assert!(matches!(
        session.apply_closed_market_event_bucket(bucket),
        Err(MarketEventErrorV1::IncompleteInitialLoad(markets)) if markets == vec![market().condition_id.0]
    ));
    assert_eq!(session.platform_event_index_position(), 0);

    let up = initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10");
    let bytes = vec![encoded(&up)];
    session
        .apply_closed_market_event_bucket(ClosedMarketEventBucketV1::hidden(
            1_100,
            0,
            1,
            MillisecondBucketClosureV1::NextEventAt(1_101),
            bytes.clone(),
        ))
        .expect("one-asset bucket may close before the input ends");
    assert!(matches!(
        session.apply_closed_market_event_bucket(ClosedMarketEventBucketV1::hidden(
            1_100,
            0,
            1,
            MillisecondBucketClosureV1::EndOfInput,
            bytes,
        )),
        Err(MarketEventErrorV1::IncompleteInitialLoad(markets)) if markets == vec![market().condition_id.0]
    ));
    assert_eq!(session.platform_event_index_position(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn task21_bucket_must_start_at_the_current_index_position() {
    let mut session = execution_session(SimCallerRoleV1::ModelTrain, "bucket-start").await;
    let up = initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10");
    assert!(matches!(
        session.apply_closed_market_event_bucket(ClosedMarketEventBucketV1::hidden(
            1_100,
            1,
            2,
            MillisecondBucketClosureV1::NextEventAt(1_101),
            vec![encoded(&up)],
        )),
        Err(MarketEventErrorV1::BucketStartPositionConflict {
            expected: 0,
            actual: 1
        })
    ));
    assert_eq!(session.platform_event_index_position(), 0);

    let down = initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "12");
    assert!(matches!(
        session.apply_closed_market_event_bucket(ClosedMarketEventBucketV1::hidden(
            1_100,
            0,
            1,
            MillisecondBucketClosureV1::NextEventAt(1_101),
            vec![encoded(&up), encoded(&down)],
        )),
        Err(MarketEventErrorV1::InvalidBucket(
            "索引位置必须覆盖全部桶内市场事件"
        ))
    ));
    assert_eq!(session.platform_event_index_position(), 0);
    assert_eq!(
        session
            .platform_market_state(&market().condition_id.0)
            .expect("market")
            .asset("1")
            .expect("up")
            .current_tick_size_pusd_per_share(),
        None
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task21_dual_time_paths_are_isolated_and_paper_has_no_hidden_projection() {
    let condition_id = market().condition_id.0;
    let initial = [
        initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10"),
        initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "12"),
    ];
    let mut training = execution_session(SimCallerRoleV1::ModelTrain, "dual-train").await;
    let hidden = hidden_bucket(1_100, 2, &initial);
    assert_eq!(
        training
            .apply_closed_market_event_bucket(hidden.clone())
            .expect("hidden bucket"),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(
        training
            .platform_market_state(&condition_id)
            .expect("platform market")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::Active
    );
    assert_eq!(
        training
            .public_market_state(&condition_id)
            .expect("public market")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::Preloading
    );
    assert_eq!(
        training
            .apply_closed_market_event_bucket(hidden)
            .expect("physical retry"),
        MarketBucketApplyDispositionV1::PhysicalRetry
    );
    training
        .apply_closed_market_event_bucket(public_bucket(1_200, 2, &initial))
        .expect("public bucket");
    assert_eq!(
        training
            .public_market_state(&condition_id)
            .expect("public market")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::Active
    );

    let mut paper = execution_session(SimCallerRoleV1::Paper, "dual-paper").await;
    assert!(matches!(
        paper.apply_closed_market_event_bucket(hidden_bucket(1_100, 2, &initial)),
        Err(MarketEventErrorV1::Session(
            crate::session::SessionError::HiddenPlatformReplayForbidden
        ))
    ));
    assert_eq!(
        paper
            .platform_market_state(&condition_id)
            .expect("paper market")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::Preloading
    );
    paper
        .apply_closed_market_event_bucket(public_bucket(1_200, 2, &initial))
        .expect("paper received bucket");
    assert_eq!(
        paper
            .platform_market_state(&condition_id)
            .expect("paper market")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::Active
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task21_preloading_requires_both_scenario_assets_and_tick_is_event_owned() {
    let condition_id = market().condition_id.0;
    let up = initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10");
    let down = initial_snapshot_event("2", 1, "Down", 1_101, 1_201, 2, "12");
    let mut session = execution_session(SimCallerRoleV1::ModelTrain, "preloading").await;
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_100, 1, &[up]))
        .expect("up initial");
    assert_eq!(
        session
            .platform_market_state(&condition_id)
            .expect("market")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::Preloading
    );
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_101, 2, &[down]))
        .expect("down initial");
    assert_eq!(
        session
            .platform_market_state(&condition_id)
            .expect("market")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::Active
    );

    let tick = sealed_event(
        ExecutionMarketEventPayloadV3::TickSizeChange(TickSizeChangeV2 {
            asset: asset("1", 0, "Up"),
            old_tick_size_pusd_per_share: None,
            new_tick_size_pusd_per_share: ExactDecimal("0.001".to_owned()),
        }),
        orderbook_evidence(1_102, 1_202, 3),
        fresh_quality(),
    );
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_102, 3, &[tick]))
        .expect("dynamic tick");
    let state = session
        .platform_market_state(&condition_id)
        .expect("market");
    assert_eq!(
        state
            .asset("1")
            .expect("up")
            .current_tick_size_pusd_per_share(),
        Some(Decimal::new(1, 3))
    );
    assert_eq!(
        state
            .asset("2")
            .expect("down")
            .current_tick_size_pusd_per_share(),
        Some(Decimal::new(1, 2))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task21_best_bid_ask_never_mutates_external_level_quantities() {
    let condition_id = market().condition_id.0;
    let initial = [
        initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10"),
        initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "12"),
    ];
    let mut session = execution_session(SimCallerRoleV1::ModelTrain, "bba").await;
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_100, 2, &initial))
        .expect("initial bucket");
    let bba = sealed_event(
        ExecutionMarketEventPayloadV3::BestBidAsk(BestBidAskV2 {
            asset: asset("1", 0, "Up"),
            best_bid_pusd_per_share: Some(ExactDecimal("0.3".to_owned())),
            best_ask_pusd_per_share: Some(ExactDecimal("0.7".to_owned())),
        }),
        orderbook_evidence(1_101, 1_201, 3),
        fresh_quality(),
    );
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_101, 3, &[bba]))
        .expect("BBA bucket");
    assert_eq!(
        session
            .platform_market_state(&condition_id)
            .expect("market")
            .asset("1")
            .expect("up")
            .external_quantity(BookSide::Bid, Decimal::new(4, 1)),
        Decimal::TEN
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task21_bad_later_item_leaves_market_state_and_watermark_unchanged() {
    let condition_id = market().condition_id.0;
    let first = initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10");
    let second = initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 2, "11");
    let mut session = execution_session(SimCallerRoleV1::ModelTrain, "atomic").await;
    assert!(matches!(
        session.apply_closed_market_event_bucket(hidden_bucket(1_100, 2, &[first, second])),
        Err(MarketEventErrorV1::InitialSnapshotAlreadyLoaded(id)) if id == "1"
    ));
    let state = session
        .platform_market_state(&condition_id)
        .expect("market");
    assert_eq!(state.lifecycle_phase(), MarketLifecyclePhaseV1::Preloading);
    assert_eq!(
        state
            .asset("1")
            .expect("up")
            .current_tick_size_pusd_per_share(),
        None
    );
    assert_eq!(session.public_event_index_position(), 0);
    assert_eq!(session.platform_event_index_position(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn task21_dpt_decoder_rejects_corrupt_conservation_before_any_bucket_effect() {
    let condition_id = market().condition_id.0;
    let initial = [
        initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10"),
        initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "12"),
    ];
    let mut session = execution_session(SimCallerRoleV1::ModelTrain, "decode").await;
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_100, 2, &initial))
        .expect("initial bucket");
    let tick = sealed_event(
        ExecutionMarketEventPayloadV3::TickSizeChange(TickSizeChangeV2 {
            asset: asset("1", 0, "Up"),
            old_tick_size_pusd_per_share: Some(ExactDecimal("0.01".to_owned())),
            new_tick_size_pusd_per_share: ExactDecimal("0.001".to_owned()),
        }),
        orderbook_evidence(1_101, 1_201, 3),
        fresh_quality(),
    );
    let transition = level_transition_event(1_101, 1_201, 4, "10", "8", "0", "2");
    let mut corrupt: serde_json::Value =
        serde_json::from_slice(&encoded(&transition)).expect("event JSON");
    corrupt["payload"]["event"]["quantity_after_shares"] =
        serde_json::Value::String("9".to_owned());
    let mut corrupt_event: ExecutionMarketEventV3 =
        serde_json::from_value(corrupt).expect("plain serde accepts the damaged event");
    corrupt_event.event_id =
        derive_execution_market_event_v3_id(&corrupt_event).expect("damaged semantic identity");
    let bucket = ClosedMarketEventBucketV1::hidden(
        1_101,
        2,
        4,
        MillisecondBucketClosureV1::NextEventAt(1_102),
        vec![
            encoded(&tick),
            serde_json::to_vec(&corrupt_event).expect("corrupt bytes"),
        ],
    );
    assert!(matches!(
        session.apply_closed_market_event_bucket(bucket),
        Err(MarketEventErrorV1::Decode { index: 1, message })
            if message.contains("quantity conservation")
    ));
    let state = session
        .platform_market_state(&condition_id)
        .expect("market");
    assert_eq!(
        state
            .asset("1")
            .expect("up")
            .current_tick_size_pusd_per_share(),
        Some(Decimal::new(1, 2))
    );
    assert_eq!(
        state
            .asset("1")
            .expect("up")
            .external_quantity(BookSide::Bid, Decimal::new(4, 1)),
        Decimal::TEN
    );
    assert_eq!(session.platform_event_index_position(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn task21_causal_group_cannot_bind_conflicting_receive_time() {
    let condition_id = market().condition_id.0;
    let initial = [
        initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10"),
        initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "12"),
    ];
    let mut session = execution_session(SimCallerRoleV1::ModelTrain, "causal-group").await;
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_100, 2, &initial))
        .expect("initial bucket");
    let first = level_transition_event(1_101, 1_201, 3, "10", "8", "0", "2");
    let second = level_transition_event(1_101, 1_202, 4, "8", "7", "0", "1");
    assert!(matches!(
        session.apply_closed_market_event_bucket(hidden_bucket(1_101, 4, &[first, second])),
        Err(MarketEventErrorV1::CausalGroupConflict(id)) if id == sha('9').0
    ));
    assert_eq!(session.platform_event_index_position(), 2);
    assert_eq!(
        session
            .platform_market_state(&condition_id)
            .expect("market")
            .asset("1")
            .expect("up")
            .external_quantity(BookSide::Bid, Decimal::new(4, 1)),
        Decimal::TEN
    );
}

pub(crate) fn level_transition_event(
    source_at: i64,
    received_at: i64,
    sequence: u64,
    before: &str,
    after: &str,
    added: &str,
    reduced: &str,
) -> ExecutionMarketEventV3 {
    sealed_event(
        ExecutionMarketEventPayloadV3::LevelExecutionTransition(LevelExecutionTransitionV3 {
            causal_group_id: sha('9'),
            market: market(),
            asset: asset("1", 0, "Up"),
            side: BookSide::Bid,
            price_pusd_per_share: ExactDecimal("0.4".to_owned()),
            quantity_before_shares: ExactDecimal(before.to_owned()),
            quantity_after_shares: ExactDecimal(after.to_owned()),
            attributed_trade_quantity_shares: ExactDecimal("0".to_owned()),
            already_covered_quantity_shares: ExactDecimal("0".to_owned()),
            net_added_quantity_shares: ExactDecimal(added.to_owned()),
            net_reduced_quantity_shares: ExactDecimal(reduced.to_owned()),
            queue_consumption_quantity_shares: ExactDecimal(reduced.to_owned()),
            basis: LevelExecutionBasis::BookResyncDeltaInference,
            platform_effective_at_utc_ms: source_at,
            visible_at_utc_ms: received_at,
            source_attribution_quality: SourceAttributionQuality::Unattributed,
            queue_result_quality: QueueResultQuality::DeterministicInferred,
        }),
        orderbook_evidence(source_at, received_at, sequence),
        EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::ResyncBoundary,
        },
    )
}

#[tokio::test(flavor = "current_thread")]
async fn task21_gap_resync_and_account_overlay_keep_separate_owners() {
    let condition_id = market().condition_id.0;
    let initial = [
        initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10"),
        initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "12"),
    ];
    let mut session = execution_session(SimCallerRoleV1::ModelTrain, "resync").await;
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_100, 2, &initial))
        .expect("initial bucket");
    session.set_account_overlay_quantity(
        &condition_id,
        "1",
        BookSide::Bid,
        Decimal::new(4, 1),
        Decimal::new(3, 0),
    );
    let missing = sealed_event(
        ExecutionMarketEventPayloadV3::QualityBoundary(QualityBoundaryV2 {
            scope: QualityScopeV1::Asset(asset("1", 0, "Up")),
            previous_quality: MarketDataQuality::Fresh,
            current_quality: MarketDataQuality::Missing,
            reason_code: "POLYMARKET_L2_EPOCH_BASELINE_REQUIRED".to_owned(),
        }),
        orderbook_evidence(1_101, 1_201, 3),
        EventQualityV2 {
            availability: MarketDataQuality::Missing,
            resync: ResyncQuality::Required,
        },
    );
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_101, 3, &[missing]))
        .expect("missing boundary");
    let after_gap = session
        .platform_market_state(&condition_id)
        .expect("market")
        .asset("1")
        .expect("up");
    assert_eq!(
        after_gap.external_quantity(BookSide::Bid, Decimal::new(4, 1)),
        Decimal::ZERO
    );
    assert_eq!(
        session.account_overlay_quantity(&condition_id, "1", BookSide::Bid, Decimal::new(4, 1)),
        Decimal::new(3, 0)
    );

    let rebuilt = level_transition_event(1_102, 1_202, 4, "0", "7", "7", "0");
    let recovered = sealed_event(
        ExecutionMarketEventPayloadV3::QualityBoundary(QualityBoundaryV2 {
            scope: QualityScopeV1::Asset(asset("1", 0, "Up")),
            previous_quality: MarketDataQuality::Missing,
            current_quality: MarketDataQuality::Fresh,
            reason_code: "POLYMARKET_L2_EPOCH_BASELINE_RESTORED".to_owned(),
        }),
        orderbook_evidence(1_102, 1_202, 4),
        EventQualityV2 {
            availability: MarketDataQuality::Fresh,
            resync: ResyncQuality::ResyncBoundary,
        },
    );
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_102, 5, &[rebuilt, recovered]))
        .expect("atomic resync");
    let asset = session
        .platform_market_state(&condition_id)
        .expect("market")
        .asset("1")
        .expect("up");
    assert_eq!(asset.quality(), MarketDataQuality::Fresh);
    assert_eq!(
        asset.external_quantity(BookSide::Bid, Decimal::new(4, 1)),
        Decimal::new(7, 0)
    );
    assert_eq!(
        session.platform_derived_quantity(&condition_id, "1", BookSide::Bid, Decimal::new(4, 1)),
        Some(Decimal::TEN)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task21_flags_end_gate_and_source_qualified_resolution_remain_distinct() {
    let condition_id = market().condition_id.0;
    let initial = [
        initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10"),
        initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "12"),
    ];
    let mut paper = execution_session(SimCallerRoleV1::Paper, "terminal").await;
    paper
        .apply_closed_market_event_bucket(public_bucket(1_200, 2, &initial))
        .expect("initial visible bucket");
    let flags = sealed_event(
        ExecutionMarketEventPayloadV3::MarketTradingState(MarketTradingStateV2 {
            active: Some(true),
            closed: Some(true),
            accepting_orders: None,
        }),
        metadata_evidence(1_300, '8'),
        fresh_quality(),
    );
    paper
        .apply_closed_market_event_bucket(public_bucket(1_300, 3, &[flags]))
        .expect("metadata flags");
    let changed_flags = sealed_event(
        ExecutionMarketEventPayloadV3::MarketTradingState(MarketTradingStateV2 {
            active: None,
            closed: Some(false),
            accepting_orders: Some(true),
        }),
        metadata_evidence(1_301, '6'),
        fresh_quality(),
    );
    paper
        .apply_closed_market_event_bucket(public_bucket(1_301, 4, &[changed_flags]))
        .expect("independent metadata flags");
    let end_bucket = ClosedMarketEventBucketV1::public(
        2_000,
        4,
        4,
        MillisecondBucketClosureV1::NextEventAt(2_100),
        Vec::new(),
    );
    paper
        .apply_closed_market_event_bucket(end_bucket)
        .expect("empty closed bucket at end");
    let state = paper.platform_market_state(&condition_id).expect("market");
    assert_eq!(
        state.lifecycle_phase(),
        MarketLifecyclePhaseV1::PostEndPendingResolution
    );
    assert_eq!(
        state.trading_state(),
        &MarketTradingStateV2 {
            active: None,
            closed: Some(false),
            accepting_orders: Some(true),
        }
    );

    let receive_only = terminal_event(
        None,
        2_100,
        5,
        CanonicalSourceKind::PolymarketMarketMetadata,
    );
    paper
        .apply_closed_market_event_bucket(public_bucket(2_100, 5, &[receive_only]))
        .expect("receive-only terminal is retained as unqualified evidence");
    let state = paper.platform_market_state(&condition_id).expect("market");
    assert!(state.qualified_resolution().is_none());
    assert!(state.receive_only_resolution_event_id().is_some());
    assert_eq!(
        state.lifecycle_phase(),
        MarketLifecyclePhaseV1::PostEndPendingResolution
    );

    let qualified = terminal_event(
        Some(2_001),
        2_101,
        6,
        CanonicalSourceKind::PolymarketOrderbook,
    );
    paper
        .apply_closed_market_event_bucket(public_bucket(2_101, 6, &[qualified]))
        .expect("qualified terminal");
    let state = paper.platform_market_state(&condition_id).expect("market");
    assert!(state.qualified_resolution().is_some());
    assert_eq!(
        state.lifecycle_phase(),
        MarketLifecyclePhaseV1::ResolutionTerminal
    );
    assert_eq!(state.trading_state().active, None);
    assert_eq!(state.trading_state().closed, Some(false));
    assert_eq!(state.trading_state().accepting_orders, Some(true));

    let conflict = sealed_event(
        ExecutionMarketEventPayloadV3::QualityBoundary(QualityBoundaryV2 {
            scope: QualityScopeV1::Market,
            previous_quality: MarketDataQuality::Fresh,
            current_quality: MarketDataQuality::Invalid,
            reason_code: "POLYMARKET_TERMINAL_DUPLICATE_CONFLICT".to_owned(),
        }),
        metadata_evidence(2_102, '7'),
        EventQualityV2 {
            availability: MarketDataQuality::Invalid,
            resync: ResyncQuality::NotRequired,
        },
    );
    paper
        .apply_closed_market_event_bucket(public_bucket(2_102, 7, &[conflict]))
        .expect("terminal conflict must advance Fresh to Invalid");
    let state = paper.platform_market_state(&condition_id).expect("market");
    assert_eq!(state.market_quality(), MarketDataQuality::Invalid);
    assert!(state.qualified_resolution().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn task21_hidden_terminal_bundle_can_seed_its_missing_public_lifecycle_predecessor() {
    let condition_id = market().condition_id.0;
    let initial = [
        initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10"),
        initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "12"),
    ];
    let mut session =
        execution_session(SimCallerRoleV1::OfflineEvaluation, "hidden-terminal").await;
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_100, 2, &initial))
        .expect("initial bucket");
    session
        .apply_closed_market_event_bucket(hidden_bucket(2_000, 2, &[]))
        .expect("post-end closure");
    let terminal = terminal_event(
        Some(2_001),
        2_100,
        5,
        CanonicalSourceKind::PolymarketOrderbook,
    );
    let retired = sealed_event(
        ExecutionMarketEventPayloadV3::MarketLifecycleTransition(MarketLifecycleTransitionV2 {
            previous_state: Some(MappedMarketLifecycleStateV1::Known {
                value: KnownMarketLifecycleState::Active,
            }),
            current_state: MappedMarketLifecycleStateV1::Known {
                value: KnownMarketLifecycleState::Retired,
            },
            assets: vec![asset("1", 0, "Up"), asset("2", 1, "Down")],
        }),
        orderbook_evidence(2_001, 2_100, 6),
        fresh_quality(),
    );
    session
        .apply_closed_market_event_bucket(hidden_bucket(2_001, 4, &[terminal, retired]))
        .expect("qualified terminal bundle");
    let state = session
        .platform_market_state(&condition_id)
        .expect("market");
    assert_eq!(
        state.lifecycle_phase(),
        MarketLifecyclePhaseV1::ResolutionTerminal
    );
    assert_eq!(
        state.mapped_lifecycle_state(),
        Some(&MappedMarketLifecycleStateV1::Known {
            value: KnownMarketLifecycleState::Retired,
        })
    );
}

fn terminal_event(
    source_at: Option<i64>,
    received_at: i64,
    sequence: u64,
    source_kind: CanonicalSourceKind,
) -> ExecutionMarketEventV3 {
    let evidence = match source_kind {
        CanonicalSourceKind::PolymarketOrderbook => orderbook_evidence(
            source_at.expect("orderbook source time"),
            received_at,
            sequence,
        ),
        CanonicalSourceKind::PolymarketMarketMetadata => metadata_evidence(received_at, '7'),
        _ => unreachable!("task 2.1 terminal uses Polymarket evidence"),
    };
    sealed_event(
        ExecutionMarketEventPayloadV3::MarketResolutionTerminal(
            MarketResolutionTerminalV3::Resolved {
                winning_asset_id: "1".to_owned(),
                winning_outcome: "Up".to_owned(),
                payout_vector: vec![
                    PayoutEntryV1 {
                        asset: asset("1", 0, "Up"),
                        payout_pusd_per_share: ExactDecimal("1".to_owned()),
                    },
                    PayoutEntryV1 {
                        asset: asset("2", 1, "Down"),
                        payout_pusd_per_share: ExactDecimal("0".to_owned()),
                    },
                ],
                mapping_rule_version: "task21-resolution-v1".to_owned(),
            },
        ),
        evidence,
        fresh_quality(),
    )
}
