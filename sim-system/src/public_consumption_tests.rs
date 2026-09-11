use super::*;
use contracts::*;
use data_pipeline_core::{
    public_input::*, public_observation_core::*, public_reference::*, typed_raw::*,
};
use data_pipeline_training::{hidden_preparation::*, public_recording::*};
use sha2::{Digest, Sha256};
const CONDITION_ID: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const CLOB_MARKET_ID: &str = "3195576";
const UP_TOKEN_ID: &str = "20";
const DOWN_TOKEN_ID: &str = "3";
const SNAPSHOT_RECEIVED_AT: i64 = 10_000;
const MARKET_START_AT: i64 = 20_000;
const MARKET_END_AT: i64 = 30_000;
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

fn core() -> PublicObservationCoreV1 {
    PublicObservationCoreV1::new(PublicInputStreamHeaderV1 {
        input_stream_id: "core-test".to_owned(),
        canonical_rule_version: "rule-v1".to_owned(),
        observation_version: "public-observation-v1".to_owned(),
        ordering_version: "original-input-v1".to_owned(),
        input_scope: "constructed-finite".to_owned(),
        max_pending_ws_terminals: 2,
        max_retained_reducer_state_entries: 163840,
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

fn config() -> data_pipeline_training::gate_a::GateAOutputConfig {
    let mut config = data_pipeline_training::gate_a_formal_output_config();
    config.sort.batch_rows = 2;
    config.sort.max_run_rows = 4;
    config.sort.max_run_bytes = 1 << 20;
    config.max_artifact_payload_bytes = 1 << 19;
    config
}
fn end(position: u64) -> RecordedIngressV1 {
    RecordedIngressV1 {
        input_stream_id: "core-test".into(),
        input_position: position,
        ingress: DptIngressV1::EndOfInput,
        dependency_record_refs: vec![],
    }
}
fn apply(
    core: &mut PublicObservationCoreV1,
    input: &RecordedIngressV1,
) -> PublicAppliedIngressV1<'static> {
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_v1(core, input, &ids, &[]).unwrap();
    core.push(&prepared).unwrap().into_owned()
}

struct RecordedTimes;
impl PublicConsumptionTimeSourceV1 for RecordedTimes {
    fn execution_time_for_group(
        &mut self,
        group: &PublicGroupReferenceV1,
    ) -> Result<i64, SimRunActorErrorV2> {
        Ok(if group.group_position == 0 {
            SNAPSHOT_RECEIVED_AT
        } else if group.group_position >= 5 {
            TERMINAL_ADMITTED_TIME_MS
        } else {
            22_000
        })
    }
}
#[derive(Default)]
struct DeliveredFeedbackState {
    sequence: std::cell::Cell<u64>,
    responses: RefCell<Vec<contracts::DirectResponseMetadataV2>>,
}
impl DeliveredFeedbackState {
    fn deliver_through(&self, at: i64) {
        for response in self.responses.borrow().iter() {
            if response.response_received_at_utc_ms <= at {
                self.sequence
                    .set(self.sequence.get().max(response.private_feedback_sequence));
            }
        }
    }
}
struct DeliveredProjection {
    accepted: u64,
    delivered: Rc<DeliveredFeedbackState>,
}
impl BusinessProjectionPortV2 for DeliveredProjection {
    fn try_accept(&mut self, batch: Arc<BusinessProjectionBatchV2>) -> bool {
        self.accepted = batch.projection_batch_sequence;
        for response in batch.direct_responses() {
            if let BusinessDirectResponseV2::CancelAllOrders(response) = response {
                self.delivered
                    .responses
                    .borrow_mut()
                    .push(response.metadata.clone());
            }
        }
        true
    }
    fn durable_through(&self, _: &str) -> u64 {
        self.accepted
    }
}
impl PublicProjectionPortV1 for DeliveredProjection {
    fn delivered_private_feedback(&self, _: &str) -> u64 {
        self.delivered.sequence.get()
    }
}
fn recorded_books(root: &Path) -> PublicObservationCoreV1 {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    for input in [
        meta(0),
        pm(1, 1, 21_000, book("100")),
        pm(
            2,
            2,
            21_001,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("80"))],
            },
        ),
        end(3),
    ] {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    core
}
async fn public_execution(
    path: &Path,
    core: &PublicObservationCoreV1,
    role: SimCallerRoleV1,
) -> (PublicSimExecutionV1, Rc<DeliveredFeedbackState>) {
    let context = core.pm_context(CONDITION_ID).unwrap();
    let scenario = sealed_scenario_for_markets(vec![
        crate::scenario::PlatformMarketV1::try_new(
            context.market,
            context
                .assets
                .into_iter()
                .map(|a| a.asset)
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        )
        .unwrap(),
    ]);
    public_execution_with_scenario(path, scenario, role).await
}
async fn public_execution_with_scenario(
    path: &Path,
    scenario: Arc<SealedPlatformScenarioV1>,
    role: SimCallerRoleV1,
) -> (PublicSimExecutionV1, Rc<DeliveredFeedbackState>) {
    let rules = successful_snapshot_v3_for_sim_tests().await;
    let finality = successful_policy_v2_for_tests();
    let latency = load_embedded_proxy_latency_profile().unwrap();
    let opened = opened(
        "public-consumption-test",
        &rules,
        &finality,
        &scenario,
        &latency,
    );
    let spec_opened = PublicRunOpenedV1 {
        projection_version: opened.projection_version,
        run_id: opened.run_id,
        platform_rule_snapshot_file_sha256: opened.platform_rule_snapshot_file_sha256,
        trade_finality_policy_file_sha256: opened.trade_finality_policy_file_sha256,
        platform_scenario_file_sha256: opened.platform_scenario_file_sha256,
        proxy_latency_profile_file_sha256: opened.proxy_latency_profile_file_sha256,
        implementation_identity: opened.implementation_identity,
        market_event_source_identity_sha256: opened.market_event_source_identity_sha256,
    };
    let delivered = Rc::new(DeliveredFeedbackState::default());
    let mut execution = PublicSimExecutionV1::create_new(
        path,
        spec_opened,
        rules,
        finality,
        scenario,
        latency,
        Box::new(DeliveredProjection {
            accepted: 0,
            delivered: delivered.clone(),
        }),
        MAX_GROUP_BYTES,
        32,
    )
    .unwrap();
    let mut session = spec("public-consumption-test", "create-public");
    session.caller_role = role;
    execution.create_session(session).unwrap();
    execution
        .activate_session("activate-public".into())
        .unwrap();
    (execution, delivered)
}
#[tokio::test(flavor = "current_thread")]
async fn recorded_public_groups_apply_immediately_without_closing_the_millisecond() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_books(&root);
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    let (execution, _) = public_execution(
        &temp.path().join("sim.jsonl"),
        &core,
        SimCallerRoleV1::Paper,
    )
    .await;
    let mut driver =
        FinitePublicConsumptionV1::new(execution, source, "finite-recorded-times".into());
    let (identity, _) = driver.next_group().unwrap().unwrap();
    assert_eq!(identity.group.group_position, 0);
    let (first, cutoff) = driver.next_group().unwrap().unwrap();
    assert_eq!(first.group.group_position, 1);
    assert_eq!(cutoff.private_feedback_sequence(), 0);
    let session = driver
        .execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap();
    assert_eq!(
        session.public_visibility_frontier.closed_through_utc_ms,
        Some(10_000)
    );
    assert_eq!(
        session.public_markets.pm_quantity(
            CONDITION_ID,
            UP_TOKEN_ID,
            BookSide::Bid,
            Decimal::new(5, 1)
        ),
        Decimal::from(100)
    );
    let request = BusinessRequestV2::CancelAllOrders(CancelAllOrdersRequestV2 {
        context: RequestContextV2 {
            global_request_id: "same-public-request".into(),
            caller_correlation_id: None,
        },
    });
    let registered = driver.decision().unwrap().request(request.clone()).unwrap();
    let sequence = driver.execution.actor.applied_input_sequence;
    assert_eq!(
        driver.decision().unwrap().request(request).unwrap(),
        registered
    );
    assert_eq!(
        driver.execution.actor.applied_input_sequence, sequence,
        "严格重试不新增记录或动作"
    );
    let (second, _) = driver.next_group().unwrap().unwrap();
    assert_eq!(second.group.group_position, 2);
    assert_eq!(second.actor_input_sequence, first.actor_input_sequence + 2);
    let session = driver
        .execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap();
    assert_eq!(
        session.public_visibility_frontier.closed_through_utc_ms,
        Some(10_000)
    );
    assert_eq!(
        session.public_markets.pm_quantity(
            CONDITION_ID,
            UP_TOKEN_ID,
            BookSide::Bid,
            Decimal::new(5, 1)
        ),
        Decimal::from(80)
    );
}

const TERMINAL_RECEIVED_TIME_MS: i64 = 31_001;
const TERMINAL_ADMITTED_TIME_MS: i64 = 31_003;
const TERMINAL_SOURCE_TIME_MS: i64 = 31_000;
const WS_EPOCH: &str = "epoch-a";
const WS_ADMISSION_SEQUENCE: u64 = 50;
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

fn recorded_complete(root: &Path) -> PublicObservationCoreV1 {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let mut down = book("100");
    if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut down {
        *asset_id = DOWN_TOKEN_ID.into();
    }
    let mut active = metadata_snapshot_row();
    active.timestamp_received = 20_000;
    active.collector_admission_sequence = 2;
    active.collector_admission_time_utc_ms = 20_001;
    active.metadata_fetch_start_utc = Some(19_990);
    active.source_evidence_received_at_utc = Some(20_000);
    active.row_evidence_validation_completed_at_utc = Some(20_001);
    active.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    active.market_runtime_role = MetadataMarketRuntimeRole::Current;
    let active = record(
        1,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(active))),
    );
    for input in [
        meta(0),
        active,
        pm(2, 1, 21_000, book("100")),
        pm(3, 2, 21_000, down),
        pm(
            4,
            3,
            21_001,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("80"))],
            },
        ),
        record(
            5,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
                terminal_metadata_row(&market()),
            ))),
        ),
        record(
            6,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(terminal_orderbook_row(
                &market(),
            ))),
        ),
        end(7),
    ] {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    core
}
struct RecordedMaterials {
    root: std::path::PathBuf,
    hidden: Option<HiddenPreparationV1>,
}
impl PublicConsumptionMaterialReaderV1 for RecordedMaterials {
    fn public_group(
        &mut self,
        reference: &PublicGroupReferenceV1,
    ) -> Result<Arc<PreparedPublicGroupV1>, SimRunActorErrorV2> {
        for result in
            RecordedPublicGroupSourceV1::open(&self.root, 1, 1 << 20, RecordedTimes).unwrap()
        {
            let group = result?;
            if group.prepared.reference.group_position == reference.group_position {
                assert_eq!(&group.prepared.reference, reference);
                return Ok(group.prepared);
            }
        }
        panic!("原记录组缺失")
    }
    fn hidden_bucket(
        &mut self,
        reference: &ClosedHiddenStepBucketV1,
    ) -> Result<Arc<[HiddenExecutionRecordV1]>, SimRunActorErrorV2> {
        let mut reader = RecordedHiddenBucketSourceV1::open(
            self.hidden.as_ref().unwrap(),
            &self.root,
            1,
            1 << 20,
        )
        .unwrap();
        while let Some(bucket) = reader.next_bucket().unwrap() {
            if bucket.reference.index_start == reference.index_start {
                assert_eq!(&bucket.reference, reference);
                return Ok(bucket.records);
            }
        }
        panic!("原隐藏桶缺失")
    }
}
#[tokio::test(flavor = "current_thread")]
async fn public_normal_end_replays_original_groups_and_own_pm_bindings() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_complete(&root);
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    let path = temp.path().join("sim.jsonl");
    let (execution, _) = public_execution(&path, &core, SimCallerRoleV1::Paper).await;
    let mut driver =
        FinitePublicConsumptionV1::new(execution, source, "finite-recorded-times".into());
    let mut receipts = Vec::new();
    while let Some((receipt, _)) = driver.next_group().unwrap() {
        receipts.push(receipt);
    }
    assert_eq!(receipts.len(), 6);
    let expected_count = driver.execution.actor.applied_input_sequence - 1;
    let mut original = PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap();
    let mut original_bindings = BTreeMap::new();
    while let Some(row) = original.next_row().unwrap() {
        if let PublicStoredRowV1::PmStepBinding(binding) = row {
            let p = binding.position;
            original_bindings.insert(
                (p.canonical_record_position, p.member_index, p.step_index),
                binding.derived_id,
            );
        }
    }
    let mut materials = RecordedMaterials { root, hidden: None };
    let candidate = driver.complete(&mut materials).unwrap();
    assert_eq!(candidate.accepted_input_count(), expected_count);
    let (_, mut reader) = SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).unwrap();
    let mut groups = 0;
    let mut pm_bindings = 0;
    loop {
        let (next, inputs) = reader.next_group().unwrap();
        reader = next;
        let Some(inputs) = inputs else {
            break;
        };
        for input in inputs {
            if let LoggedBusinessInputV2::PublicGroup(group) = input {
                groups += 1;
                pm_bindings += group.pm_step_bindings.len();
                for binding in group.pm_step_bindings {
                    let p = binding.position;
                    assert_eq!(
                        original_bindings.get(&(
                            p.canonical_record_position,
                            p.member_index,
                            p.step_index
                        )),
                        Some(&binding.derived_id),
                        "后台恢复原快照数组后沿原身份域物化"
                    );
                }
            }
        }
    }
    reader.finish_sealed_clean_run().unwrap();
    assert_eq!(groups, 6);
    assert!(
        pm_bindings > 0,
        "模拟器当次前序材料已后台物化，日志不只剩 DPT 引用"
    );
    let wire = std::fs::read_to_string(path).unwrap();
    assert!(
        !wire.contains("quantity_after_shares"),
        "模拟器原日志不复制 DPT 正文"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn hidden_index_pages_preserve_real_steps_market_facts_and_execution_isolation() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_complete(&root);
    let (execution, _) = public_execution(
        &temp.path().join("sim.jsonl"),
        &core,
        SimCallerRoleV1::ModelTrain,
    )
    .await;
    let reader = PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap();
    let hidden = prepare_hidden_v1(
        core,
        reader,
        &temp.path().join("hidden"),
        &config(),
        "hidden-test".into(),
    )
    .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    let mut driver =
        FinitePublicConsumptionV1::new(execution, source, "finite-recorded-times".into());
    assert_eq!(
        driver.next_group().unwrap().unwrap().0.group.group_position,
        0
    );
    // 有限消费时序：公开身份 10000，隐藏 21000/21001，公开各组 22000，隐藏终态 31000，公开终态 31003。
    let mut small = RecordedHiddenBucketSourceV1::open(&hidden, &root, 1, 1 << 20).unwrap();
    let mut wide = RecordedHiddenBucketSourceV1::open(&hidden, &root, 4, 1 << 20).unwrap();
    let mut times = Vec::new();
    let mut terminal_count = 0;
    while let Some(bucket) = small.next_bucket().unwrap() {
        let other = wide.next_bucket().unwrap().unwrap();
        assert_eq!(bucket, other, "页边界不改变桶或索引序材料");
        times.push(bucket.reference.bucket_time_utc_ms);
        for record in bucket.records.iter() {
            match record {
                HiddenExecutionRecordV1::Step { step, .. } => {
                    if let data_pipeline_core::pm_reduction::PmStepChangeV1::LevelChange {
                        transition,
                        ..
                    } = &step.change
                    {
                        assert_eq!(
                            transition.quantity_before_shares,
                            ExactDecimal("100".into())
                        );
                        assert_eq!(transition.quantity_after_shares, ExactDecimal("80".into()));
                    }
                }
                HiddenExecutionRecordV1::MarketFact(fact) => {
                    assert_eq!(
                        fact.dependency_time.source_event_at_utc_ms,
                        Some(TERMINAL_SOURCE_TIME_MS)
                    );
                    if matches!(fact.payload, CanonicalMarketFactPayloadV1::Terminal(_)) {
                        terminal_count += 1;
                    }
                }
                _ => {}
            }
        }
        let at = bucket.reference.bucket_time_utc_ms;
        driver.hidden_bucket(bucket).unwrap();
        if at == 21_000 {
            let session = driver
                .execution
                .actor
                .state
                .as_ref()
                .unwrap()
                .session
                .as_ref()
                .unwrap();
            assert_eq!(
                session.public_markets.pm_quantity(
                    CONDITION_ID,
                    UP_TOKEN_ID,
                    BookSide::Bid,
                    Decimal::new(5, 1)
                ),
                Decimal::ZERO
            );
            assert_eq!(
                session
                    .hidden_platform_markets
                    .as_ref()
                    .unwrap()
                    .pm_quantity(CONDITION_ID, UP_TOKEN_ID, BookSide::Bid, Decimal::new(5, 1)),
                Decimal::from(100)
            );
        }
        if at == 21_001 {
            for expected in 1..=4 {
                assert_eq!(
                    driver.next_group().unwrap().unwrap().0.group.group_position,
                    expected
                );
            }
            let session = driver
                .execution
                .actor
                .state
                .as_ref()
                .unwrap()
                .session
                .as_ref()
                .unwrap();
            assert_eq!(
                session.public_markets.pm_quantity(
                    CONDITION_ID,
                    UP_TOKEN_ID,
                    BookSide::Bid,
                    Decimal::new(5, 1)
                ),
                Decimal::from(80)
            );
            assert_eq!(
                session
                    .hidden_platform_markets
                    .as_ref()
                    .unwrap()
                    .pm_quantity(CONDITION_ID, UP_TOKEN_ID, BookSide::Bid, Decimal::new(5, 1)),
                Decimal::from(80)
            );
        }
        if at == 31_000 {
            while driver.next_group().unwrap().is_some() {}
        }
    }
    assert!(wide.next_bucket().unwrap().is_none());
    assert_eq!(times, [21_000, 21_001, 31_000]);
    assert_eq!(terminal_count, 1, "WS 终态的规范市场事实只消费一次");
    let mut materials = RecordedMaterials {
        root,
        hidden: Some(hidden),
    };
    driver.complete(&mut materials).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn full_public_receipt_precedes_background_encoding_and_later_write_failure_is_incomplete() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_books(&root);
    let path = temp.path().join("sim.jsonl");
    let (execution, _) = public_execution(&path, &core, SimCallerRoleV1::Paper).await;
    let release = execution
        .actor
        .background_log
        .as_ref()
        .unwrap()
        .pause_for_tests(true);
    let before = std::fs::read(&path).unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    let mut driver =
        FinitePublicConsumptionV1::new(execution, source, "finite-recorded-times".into());
    let (receipt, cutoff) = driver.next_group().unwrap().unwrap();
    assert_eq!(receipt.group.group_position, 0);
    assert_eq!(cutoff.receipt(), &receipt);
    assert_eq!(driver.frames()[0].receipt, receipt);
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "完整回执已交付，后台编码尚被屏障阻止"
    );
    let applied = driver.execution.actor.applied_input_sequence;
    release.send(()).unwrap();
    let background = driver.execution.actor.background_log.take().unwrap();
    assert!(matches!(
        background.abort_and_join_for_tests(),
        Err(crate::event_log::SimEventLogErrorV2::Io { .. })
    ));
    assert_eq!(
        driver.execution.actor.applied_input_sequence, applied,
        "已发生的内存应用没有伪称回滚"
    );
    let (_, mut reader) = SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).unwrap();
    loop {
        let (next, group) = reader.next_group().unwrap();
        reader = next;
        if group.is_none() {
            break;
        }
    }
    assert!(matches!(
        reader.finish_sealed_clean_run(),
        Err(crate::event_log::SimEventLogErrorV2::UnsealedCleanRun)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn decision_cutoff_uses_delivered_feedback_and_frame_borrows_original_observation() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_complete(&root);
    let (execution, delivered) = public_execution(
        &temp.path().join("sim.jsonl"),
        &core,
        SimCallerRoleV1::Paper,
    )
    .await;
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    let mut driver =
        FinitePublicConsumptionV1::new(execution, source, "finite-recorded-times".into());
    driver.next_group().unwrap().unwrap();
    let (active, _cutoff) = driver.next_group().unwrap().unwrap();
    driver
        .decision()
        .unwrap()
        .request(BusinessRequestV2::CancelAllOrders(
            CancelAllOrdersRequestV2 {
                context: RequestContextV2 {
                    global_request_id: "actual-delivery".into(),
                    caller_correlation_id: None,
                },
            },
        ))
        .unwrap();
    for expected in 2..=4 {
        let (receipt, cutoff) = driver.next_group().unwrap().unwrap();
        assert_eq!(receipt.group.group_position, expected);
        assert_eq!(cutoff.private_feedback_sequence(), 0);
    }
    let last = driver.last_receipt.as_ref().unwrap().clone();
    driver
        .execution
        .close_public_time(TrustedPublicTimeClosureV1 {
            execution_time: driver.time(22_000),
            consumption_sequence_ref: driver.consumption_sequence_ref.clone(),
            next_group_position: 5,
            closure: contracts::ReplayIndexClosureV1::NextGlobalEventAt(TERMINAL_ADMITTED_TIME_MS),
        })
        .unwrap();
    driver.close_pending = false;
    let required = driver
        .execution
        .actor
        .next_required_logical_drive()
        .unwrap();
    driver
        .execution
        .close_public_time(TrustedPublicTimeClosureV1 {
            execution_time: driver.time(required.required_at_utc_ms),
            consumption_sequence_ref: driver.consumption_sequence_ref.clone(),
            next_group_position: 5,
            closure: contracts::ReplayIndexClosureV1::NextGlobalEventAt(TERMINAL_ADMITTED_TIME_MS),
        })
        .unwrap();
    let response = delivered.responses.borrow()[0].clone();
    delivered.deliver_through(response.response_received_at_utc_ms - 1);
    assert_eq!(
        driver
            .execution
            .decision_cutoff(last)
            .private_feedback_sequence(),
        0,
        "已生成并接纳的未来反馈尚未真实交付"
    );
    delivered.deliver_through(response.response_received_at_utc_ms);
    let (terminal, cutoff) = driver.next_group().unwrap().unwrap();
    assert_eq!(
        cutoff.private_feedback_sequence(),
        response.private_feedback_sequence
    );
    assert!(terminal.actor_input_sequence > active.actor_input_sequence);
    let frames = driver.frames();
    let frame = &frames[0];
    assert_eq!(frame.decision_cutoff, cutoff);
    assert!(std::ptr::eq(
        frame.observation,
        &driver.last_prepared.as_ref().unwrap().group.observations[0]
    ));
    assert_eq!(
        frame.observation.visible_prefix,
        terminal.group.applied_prefix
    );
    assert_eq!(frame.rule_context.market, frame.observation.market);
    assert_eq!(frame.rule_context.taker_delay_ms, 150);
    assert_eq!(frame.rule_context.gtd_lifetime_seconds, [180, 240, 300]);
    assert_eq!(
        frame.rule_context.queried_rules_source.category,
        PublicRuleSourceCategoryV1::QueriedPlatform
    );
    assert_eq!(
        frame.rule_context.project_rules_source.category,
        PublicRuleSourceCategoryV1::ApprovedProjectRule
    );
    assert_eq!(
        frame.rule_context.snapshot_file_sha256,
        driver.execution.actor._rule_snapshot.file_sha256().clone()
    );
    assert!(
        frame
            .observation
            .assets
            .iter()
            .all(|asset| asset.tick.value == Some(ExactDecimal("0.01".into())))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn hidden_cross_epoch_index_keeps_the_real_predecessor_failure() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    for input in [
        meta(0),
        pm(1, 1, 20_050, book("10")),
        pm(
            2,
            2,
            20_200,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("20"))],
            },
        ),
        record(
            3,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-b",
                1,
                20_100,
                23_000,
                book("15"),
            ))),
        ),
        end(4),
    ] {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    let (mut execution, _) = public_execution(
        &temp.path().join("sim.jsonl"),
        &core,
        SimCallerRoleV1::ModelTrain,
    )
    .await;
    let reader = PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap();
    let hidden = prepare_hidden_v1(
        core,
        reader,
        &temp.path().join("hidden"),
        &config(),
        "hidden-test".into(),
    )
    .unwrap();
    let mut source = RecordedHiddenBucketSourceV1::open(&hidden, &root, 1, 1 << 20).unwrap();
    let first = source.next_bucket().unwrap().unwrap();
    assert_eq!(first.reference.bucket_time_utc_ms, 20_050);
    execution.hidden_bucket(first).unwrap();
    let recovery = source.next_bucket().unwrap().unwrap();
    assert_eq!(recovery.reference.bucket_time_utc_ms, 20_100);
    execution.hidden_bucket(recovery).unwrap();
    let late = source.next_bucket().unwrap().unwrap();
    assert_eq!(late.reference.bucket_time_utc_ms, 20_200);
    assert!(late.records.iter().any(|record| matches!(record, HiddenExecutionRecordV1::Step { step, .. }
        if matches!(&step.change, data_pipeline_core::pm_reduction::PmStepChangeV1::LevelChange { transition, .. }
        if transition.quantity_before_shares.0 == "10" && transition.quantity_after_shares.0 == "20"))));
    assert!(matches!(
        execution.hidden_bucket(late),
        Err(SimRunActorErrorV2::MarketEvent(
            MarketEventErrorV1::LevelPredecessorConflict(_)
        ))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn aborted_recording_prefix_never_becomes_a_finite_normal_time_close() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let input = meta(0);
    let result = apply(&mut core, &input);
    writer.record(input, result).unwrap();
    writer.stabilize().unwrap();
    writer
        .record_aborted(RecordedIngressV1 {
            input_stream_id: "core-test".into(),
            input_position: 1,
            ingress: DptIngressV1::Aborted {
                reason: "原供料中断".into(),
            },
            dependency_record_refs: vec![],
        })
        .unwrap();
    assert!(writer.stabilize().is_err());
    let (execution, _) = public_execution(
        &temp.path().join("sim.jsonl"),
        &core,
        SimCallerRoleV1::Paper,
    )
    .await;
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    let mut driver =
        FinitePublicConsumptionV1::new(execution, source, "finite-recorded-times".into());
    let receipt = driver.next_group().unwrap().unwrap().0;
    let sequence = driver.execution.actor.applied_input_sequence;
    for _ in 0..2 {
        assert!(matches!(
            driver.next_group(),
            Err(SimRunActorErrorV2::PublicConsumptionSource(_))
        ));
    }
    assert_eq!(driver.execution.actor.applied_input_sequence, sequence);
    assert_eq!(driver.last_receipt.as_ref().unwrap(), &receipt);
    assert!(
        !driver
            .execution
            .actor
            .state
            .as_ref()
            .unwrap()
            .session
            .as_ref()
            .unwrap()
            .public_visibility_frontier
            .sealed
    );
}

#[tokio::test(flavor = "current_thread")]
async fn request_due_with_three_groups_waits_for_the_independent_time_close() {
    struct ArrivalTimes(i64);
    impl PublicConsumptionTimeSourceV1 for ArrivalTimes {
        fn execution_time_for_group(
            &mut self,
            group: &PublicGroupReferenceV1,
        ) -> Result<i64, SimRunActorErrorV2> {
            Ok(match group.group_position {
                0 => 10_000,
                1 => self.0,
                2..=4 => 22_000,
                _ => TERMINAL_ADMITTED_TIME_MS,
            })
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_complete(&root);
    let (execution, delivered) = public_execution(
        &temp.path().join("sim.jsonl"),
        &core,
        SimCallerRoleV1::Paper,
    )
    .await;
    let request_id = "arrives-with-books";
    let session = execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap();
    let delay = execution.actor._latency_profile.sample_delay_ms(
        session.session_seed.0.as_bytes(),
        request_id,
        DelaySampleDomainV1::RequestOutbound,
    );
    let decision_at = 22_000 - i64::try_from(delay).unwrap();
    assert!((20_000..22_000).contains(&decision_at));
    let source =
        RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, ArrivalTimes(decision_at)).unwrap();
    let mut driver =
        FinitePublicConsumptionV1::new(execution, source, "finite-arrival-times".into());
    driver.next_group().unwrap().unwrap();
    driver.next_group().unwrap().unwrap();
    driver
        .decision()
        .unwrap()
        .request(BusinessRequestV2::CancelAllOrders(
            CancelAllOrdersRequestV2 {
                context: RequestContextV2 {
                    global_request_id: request_id.into(),
                    caller_correlation_id: None,
                },
            },
        ))
        .unwrap();
    for position in 2..=4 {
        assert_eq!(
            driver.next_group().unwrap().unwrap().0.group.group_position,
            position
        );
        assert!(
            delivered.responses.borrow().is_empty(),
            "同刻各组立即交付，但正文尚未独立关闭"
        );
    }
    driver.next_group().unwrap().unwrap();
    let responses = delivered.responses.borrow();
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].platform_responded_at_utc_ms, 22_000);
}

#[tokio::test(flavor = "current_thread")]
async fn rule_view_preserves_snapshot_times_and_distinct_observed_leg_ticks() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    for input in [
        meta(0),
        pm(1, 1, 21_000, book("100")),
        pm(
            2,
            2,
            21_001,
            PolymarketOrderbookEvent::TickSizeChange {
                asset_id: UP_TOKEN_ID.into(),
                old_tick_size: Some("0.01".into()),
                new_tick_size: "0.001".into(),
            },
        ),
        end(3),
    ] {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    let (execution, _) = public_execution(
        &temp.path().join("sim.jsonl"),
        &core,
        SimCallerRoleV1::Paper,
    )
    .await;
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    let mut driver =
        FinitePublicConsumptionV1::new(execution, source, "finite-recorded-times".into());
    for _ in 0..3 {
        driver.next_group().unwrap().unwrap();
    }
    let frames = driver.frames();
    let frame = &frames[0];
    let provenance = &driver.execution.actor._rule_snapshot.snapshot().provenance;
    assert_eq!(
        frame.rule_context.query_started_at_utc_ms,
        provenance.query_started_at_utc_ms
    );
    assert_eq!(
        frame.rule_context.query_completed_at_utc_ms,
        provenance.query_completed_at_utc_ms
    );
    assert_eq!(
        frame.rule_context.frozen_at_utc_ms,
        provenance.rules_observed_at_utc_ms
    );
    let ticks: Vec<_> = frame
        .observation
        .assets
        .iter()
        .map(|a| a.tick.value.clone())
        .collect();
    assert!(ticks.contains(&Some(ExactDecimal("0.001".into()))));
    assert!(ticks.contains(&Some(ExactDecimal("0.01".into()))));
}

fn twap_input(window_s: u64) -> data_pipeline_core::public_input::PolymarketRtdsChainlinkTwapInput {
    use data_pipeline_core::typed_raw::{PolymarketRtdsChainlinkBtcUsdRow, RtdsFreshnessStatus};
    data_pipeline_core::public_input::PolymarketRtdsChainlinkTwapInput {
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

#[tokio::test(flavor = "current_thread")]
async fn pure_reference_bucket_closes_without_market_effect_and_missing_time_is_not_fabricated() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let mut reference = twap_input(60);
    reference.common.timestamp_received = 21_001;
    reference.common.message_timestamp = 21_000;
    reference.common.payload_timestamp = 21_000;
    reference.common.age_ms = Some(1);
    reference.collector_admission_time_utc_ms = 21_002;
    let mut missing = reference.clone();
    missing.collector_admission_sequence = 2;
    missing.common.freshness_status = RtdsFreshnessStatus::InvalidTimestamp;
    for input in [
        meta(0),
        record(1, TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference)),
        record(2, TypedRawRowV2::PolymarketRtdsChainlinkTwap(missing)),
        pm(3, 1, 22_000, book("100")),
        end(4),
    ] {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    let (mut execution, _) = public_execution(
        &temp.path().join("sim.jsonl"),
        &core,
        SimCallerRoleV1::OfflineEvaluation,
    )
    .await;
    let reader = PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap();
    let hidden = prepare_hidden_v1(
        core,
        reader,
        &temp.path().join("hidden"),
        &config(),
        "hidden-reference".into(),
    )
    .unwrap();
    let mut source = RecordedHiddenBucketSourceV1::open(&hidden, &root, 1, 1 << 20).unwrap();
    let bucket = source.next_bucket().unwrap().unwrap();
    assert_eq!(bucket.reference.bucket_time_utc_ms, 21_000);
    assert_eq!(bucket.records.len(), 1);
    assert!(matches!(
        bucket.records[0],
        HiddenExecutionRecordV1::Reference(_)
    ));
    assert_eq!(
        source
            .next_bucket()
            .unwrap()
            .unwrap()
            .reference
            .bucket_time_utc_ms,
        22_000
    );
    assert!(
        source.next_bucket().unwrap().is_none(),
        "缺少来源时间的异常不获得伪造的隐藏时刻"
    );
    execution.hidden_bucket(bucket).unwrap();
    let session = execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap();
    assert_eq!(
        session.hidden_platform_frontier.closed_through_utc_ms,
        Some(21_000)
    );
    assert_eq!(
        session
            .hidden_platform_markets
            .as_ref()
            .unwrap()
            .pm_quantity(CONDITION_ID, UP_TOKEN_ID, BookSide::Bid, Decimal::new(5, 1)),
        Decimal::ZERO
    );
}

#[tokio::test(flavor = "current_thread")]
async fn enrolled_market_has_the_same_snapshot_rule_frame() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_books(&root);
    let scenario = sealed_scenario_for_markets(vec![super::market('2', "456", "4", "5")]);
    let (mut execution, _) = public_execution_with_scenario(
        &temp.path().join("sim.jsonl"),
        scenario,
        SimCallerRoleV1::ModelTrain,
    )
    .await;
    let context = core.pm_context(CONDITION_ID).unwrap();
    let target = crate::scenario::PlatformMarketV1::try_new(
        context.market,
        context
            .assets
            .into_iter()
            .map(|a| a.asset)
            .collect::<Vec<_>>()
            .try_into()
            .unwrap(),
    )
    .unwrap();
    execution
        .enroll_market(MarketEnrollmentRequestV1 {
            enrollment_request_id: "enroll-before-public-groups".into(),
            market: target,
        })
        .unwrap();
    let expected = execution.actor._rule_snapshot.public_rule_context(market());
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    let mut driver =
        FinitePublicConsumptionV1::new(execution, source, "finite-enrolled-market".into());
    let (receipt, cutoff) = driver.next_group().unwrap().unwrap();
    assert_eq!(receipt.group.group_position, 0);
    let frames = driver.frames();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].observation.market, market());
    assert_eq!(frames[0].rule_context.as_ref(), &expected);
    assert_eq!(frames[0].receipt, receipt);
    assert_eq!(frames[0].decision_cutoff, cutoff);
}
