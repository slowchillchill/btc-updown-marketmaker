//! 通过正式typed_raw准备、DPT记录与sim解析构造有限组合夹具。
use contracts::*;
use data_pipeline_core::{
    public_input::*, public_observation_core::*, public_reference::*, typed_raw::*,
};
use data_pipeline_training::public_recording::*;
use sha2::{Digest, Sha256};
use sim_system::{
    execution::*, finality::*, latency_profile::*, platform_rules::*, public_consumption::*,
    scenario::*,
};
use std::{collections::BTreeMap, path::Path, sync::Arc};
pub const CONDITION_ID: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const CLOB_MARKET_ID: &str = "3195576";
pub const UP_TOKEN_ID: &str = "20";
pub const DOWN_TOKEN_ID: &str = "3";
const SNAPSHOT_RECEIVED_AT: i64 = 10_000;
const MARKET_START_AT: i64 = 20_000;
const MARKET_END_AT: i64 = 30_000;
pub fn market() -> MarketIdentityV2 {
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

pub fn core() -> PublicObservationCoreV1 {
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

pub fn record(position: u64, row: TypedRawRowV2) -> RecordedIngressV1 {
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

pub fn meta(position: u64) -> RecordedIngressV1 {
    record(
        position,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
            metadata_snapshot_row(),
        ))),
    )
}

pub fn pm(
    position: u64,
    seq: u64,
    source: i64,
    event: PolymarketOrderbookEvent,
) -> RecordedIngressV1 {
    record(
        position,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
            "epoch-a", seq, source, 22_000, event,
        ))),
    )
}

pub fn config() -> data_pipeline_training::gate_a::GateAOutputConfig {
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

const TERMINAL_RECEIVED_TIME_MS: i64 = 31_001;
const TERMINAL_ADMITTED_TIME_MS: i64 = 31_003;
const TERMINAL_SOURCE_TIME_MS: i64 = 31_000;
const WS_EPOCH: &str = "epoch-a";
const WS_ADMISSION_SEQUENCE: u64 = 50;
pub fn terminal_metadata_row(market: &MarketIdentityV2) -> PolymarketMarketMetadataRow {
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
pub fn terminal_orderbook_row(market: &MarketIdentityV2) -> PolymarketOrderbookRow {
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

pub fn recorded_memory_complete(root: &Path) -> PublicObservationCoreV1 {
    recorded_pair(root, false)
}
pub fn recorded_pair(root: &Path, reject_second: bool) -> PublicObservationCoreV1 {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let mut down = book("100");
    if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut down {
        *asset_id = DOWN_TOKEN_ID.into();
    }
    if reject_second && let PolymarketOrderbookEvent::Book { bids, asks, .. } = &mut down {
        bids[0].price = "0.2".into();
        asks[0].price = "0.3".into();
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
    let mut inputs = vec![
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
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                4,
                25_000,
                25_001,
                book("80"),
            ))),
        ),
        record(
            6,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
                terminal_metadata_row(&market()),
            ))),
        ),
        record(
            7,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(terminal_orderbook_row(
                &market(),
            ))),
        ),
        end(8),
    ];
    for input in &mut inputs[6..] {
        input.input_position += 1;
    }
    inputs.insert(
        6,
        record(
            6,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                5,
                27_000,
                27_001,
                book("80"),
            ))),
        ),
    );
    for input in inputs {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    core
}
pub struct MemoryRecordedTimes;
impl PublicConsumptionTimeSourceV1 for MemoryRecordedTimes {
    fn execution_time_for_group(
        &mut self,
        group: &PublicGroupReferenceV1,
    ) -> Result<i64, SimRunActorErrorV2> {
        Ok(match group.group_position {
            0 => SNAPSHOT_RECEIVED_AT,
            1..=4 => 22_000,
            5 => 26_000,
            6 => 28_000,
            _ => TERMINAL_ADMITTED_TIME_MS,
        })
    }
}

pub fn execution(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
) -> (PublicSimExecutionV1, MemoryNoeObservationV3) {
    execution_with_delay(path, core, policy, None)
}
pub fn execution_with_delay(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
    delay: Option<u64>,
) -> (PublicSimExecutionV1, MemoryNoeObservationV3) {
    execution_with_seed(path, core, policy, delay, "e".repeat(64))
}
pub fn execution_with_seed(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
    delay: Option<u64>,
    seed: String,
) -> (PublicSimExecutionV1, MemoryNoeObservationV3) {
    execution_with_seed_and_cash(path, core, policy, delay, seed, "100")
}
pub fn execution_with_seed_and_cash(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
    delay: Option<u64>,
    seed: String,
    cash: &str,
) -> (PublicSimExecutionV1, MemoryNoeObservationV3) {
    let (run, observation, _) = execution_with_rules(path, core, policy, delay, seed, cash);
    (run, observation)
}
pub fn execution_with_rules(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
    delay: Option<u64>,
    seed: String,
    cash: &str,
) -> (
    PublicSimExecutionV1,
    MemoryNoeObservationV3,
    Arc<SealedProcessRuleSnapshotV3>,
) {
    execution_with_identity(path, core, policy, delay, seed, cash, "shared-combination")
}
pub fn execution_with_identity(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
    delay: Option<u64>,
    seed: String,
    cash: &str,
    run_id: &str,
) -> (
    PublicSimExecutionV1,
    MemoryNoeObservationV3,
    Arc<SealedProcessRuleSnapshotV3>,
) {
    execution_with_evidence::<MeasuredOrderEvidence>(
        path,
        core,
        policy,
        delay,
        seed,
        cash,
        run_id,
        BusinessProjectionVersionV1::V3,
    )
}
pub fn execution_with_identity_v4(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
    delay: Option<u64>,
    seed: String,
    cash: &str,
    run_id: &str,
) -> (
    PublicSimExecutionV1,
    MemoryNoeObservationV4,
    Arc<SealedProcessRuleSnapshotV3>,
) {
    execution_with_evidence::<MeasuredCancellationEvidence>(
        path,
        core,
        policy,
        delay,
        seed,
        cash,
        run_id,
        BusinessProjectionVersionV1::V4,
    )
}
pub fn execution_with_rules_v4(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
    delay: Option<u64>,
    seed: String,
    cash: &str,
) -> (
    PublicSimExecutionV1,
    MemoryNoeObservationV4,
    Arc<SealedProcessRuleSnapshotV3>,
) {
    execution_with_identity_v4(path, core, policy, delay, seed, cash, "shared-combination")
}
// 同一真实夹具仅显式区分源投影版本；不再包装一份运行配置。
#[allow(clippy::too_many_arguments)]
fn execution_with_evidence<P: MemoryOrderEvidence + 'static>(
    path: &Path,
    core: &PublicObservationCoreV1,
    policy: &[u8],
    delay: Option<u64>,
    seed: String,
    cash: &str,
    run_id: &str,
    projection_version: BusinessProjectionVersionV1,
) -> (
    PublicSimExecutionV1,
    MemoryNoeObservation<P>,
    Arc<SealedProcessRuleSnapshotV3>,
) {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = Path::new(file!());
    // workspace 编译的 file! 带包目录；独立 #[path] 消费则相对调用包。
    let source = manifest.join(
        source
            .strip_prefix(manifest.file_name().unwrap())
            .unwrap_or(source),
    );
    let original = source
        .parent()
        .unwrap()
        .join("../../../sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json");
    let (rule_path, rule_sha) = if let Some(delay) = delay {
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&original).unwrap()).unwrap();
        value["documented_platform_rules"]["taker_delay"]["value"]["delay_ms"] =
            serde_json::json!(delay);
        let bytes = serde_json::to_vec(&value).unwrap();
        let output = path.parent().unwrap().join("rules-contract-150.json");
        std::fs::write(&output, &bytes).unwrap();
        (output, format!("{:x}", Sha256::digest(&bytes)))
    } else {
        (
            original,
            "d63c0e7b09edc4b2c4c3f0295f806b25f6f5fd03663cdf60ff062117e53e4e54".into(),
        )
    };
    let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: rule_path,
        expected_file_sha256: Sha256Hex(rule_sha),
        replay_causal_start_utc_ms: i64::MAX,
    })
    .unwrap();
    let finality = Arc::new(TradeFinalityPolicyV2::decode_and_seal(policy).unwrap());
    let scenario = PlatformScenarioV1::try_new(
        core.fixed_markets()
            .map(|market| {
                let c = core.pm_context(&market.condition_id.0).unwrap();
                PlatformMarketV1::try_new(
                    c.market,
                    c.assets
                        .into_iter()
                        .map(|a| a.asset)
                        .collect::<Vec<_>>()
                        .try_into()
                        .unwrap(),
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let artifact = SimulationScenarioArtifactV1 {
        schema_version: PLATFORM_SCENARIO_SCHEMA_V1.into(),
        scenario_sha256: scenario.scenario_sha256().clone(),
        market_set_sha256: scenario.market_set_sha256().clone(),
        markets: scenario.market_set().markets().to_vec(),
    };
    let scenario = Arc::new(
        SealedPlatformScenarioV1::decode_and_seal(&serde_json::to_vec(&artifact).unwrap()).unwrap(),
    );
    let latency = load_embedded_proxy_latency_profile().unwrap();
    let opened = RunOpenedV2 {
        projection_version,
        run_id: run_id.into(),
        durability_role: RunDurabilityRoleV2::CleanRun,
        platform_rule_snapshot_file_sha256: rules.file_sha256().clone(),
        trade_finality_policy_file_sha256: finality.policy_id().clone(),
        platform_scenario_file_sha256: scenario.file_sha256().clone(),
        proxy_latency_profile_file_sha256: latency.file_sha256().clone(),
        implementation_identity: ImplementationIdentityV1 {
            git_commit: "a".repeat(40),
            source_identity_sha256: Sha256Hex("b".repeat(64)),
            cargo_lock_sha256: Sha256Hex("c".repeat(64)),
        },
        market_event_source_identity_sha256: Some(Sha256Hex("d".repeat(64))),
    };
    let reference = contracts::execution_normalization::DurableSourceReferenceV1 {
        source: contracts::execution_normalization::ImmutableSourceReferenceV1::Location(
            path.display().to_string(),
        ),
    };
    let port = MemoryNoeProjectionPort::<P>::new(
        path.parent().unwrap().join("noe"),
        opened.clone(),
        reference.clone(),
        reference,
        "create".into(),
        "shared-test".into(),
    );
    let observation = port.observation();
    let spec = PublicRunOpenedV1 {
        projection_version: opened.projection_version,
        run_id: opened.run_id.clone(),
        platform_rule_snapshot_file_sha256: opened.platform_rule_snapshot_file_sha256,
        trade_finality_policy_file_sha256: opened.trade_finality_policy_file_sha256,
        platform_scenario_file_sha256: opened.platform_scenario_file_sha256,
        proxy_latency_profile_file_sha256: opened.proxy_latency_profile_file_sha256,
        implementation_identity: opened.implementation_identity,
        market_event_source_identity_sha256: opened.market_event_source_identity_sha256,
    };
    let mut run = PublicSimExecutionV1::create_new(
        path,
        spec,
        rules.clone(),
        finality,
        scenario,
        latency,
        Box::new(port),
        2 << 20,
        128,
    )
    .unwrap();
    run.create_session(SessionSpecV3 {
        run_id: opened.run_id,
        session_id: "shared-session".into(),
        account_id: "shared-account".into(),
        caller_role: SimCallerRoleV1::Paper,
        caller_instance_id: "shared-caller".into(),
        creation_request_id: "create".into(),
        response_bridge_binding_id: "shared-bridge".into(),
        initial_account_state: InitialAccountStateV2 {
            available_pusd: ExactDecimal(cash.into()),
        },
        session_seed: Sha256Hex(seed),
    })
    .unwrap();
    run.activate_session("activate".into()).unwrap();
    (run, observation, rules)
}
pub const SUCCESS_POLICY:&[u8]=br#"{"schema_version":"trade_finality_policy_v2","normal_path":{"steps":[{"state":"MATCHED","offset_ms":0},{"state":"MINED","offset_ms":10},{"state":"CONFIRMED","offset_ms":20}],"economic_application_ordinal":1},"failure_overrides":[]}"#;
pub struct Materials {
    pub root: std::path::PathBuf,
}
impl PublicConsumptionMaterialReaderV1 for Materials {
    fn public_group(
        &mut self,
        reference: &PublicGroupReferenceV1,
    ) -> Result<Arc<PreparedPublicGroupV1>, SimRunActorErrorV2> {
        for result in
            RecordedPublicGroupSourceV1::open(&self.root, 1, 1 << 20, MemoryRecordedTimes).unwrap()
        {
            let group = result?;
            if group.prepared.reference == *reference {
                return Ok(group.prepared);
            }
        }
        panic!("原组引用必须存在")
    }
    fn hidden_bucket(
        &mut self,
        _: &ClosedHiddenStepBucketV1,
    ) -> Result<Arc<[HiddenExecutionRecordV1]>, SimRunActorErrorV2> {
        panic!("此夹具只含公共组")
    }
}
#[derive(Clone)]
pub struct TradingTimes(pub Vec<i64>);
impl PublicConsumptionTimeSourceV1 for TradingTimes {
    fn execution_time_for_group(
        &mut self,
        group: &PublicGroupReferenceV1,
    ) -> Result<i64, SimRunActorErrorV2> {
        Ok(self.0[group.group_position as usize])
    }
}
pub fn recorded_trading(
    root: &Path,
    initial_taker: &str,
    maker: bool,
) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_trading_with_maker(
        root,
        initial_taker,
        if maker { Some(("0.6", "3")) } else { None },
    )
}
pub fn recorded_trading_with_maker(
    root: &Path,
    initial_taker: &str,
    maker: Option<(&str, &str)>,
) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_trading_window(root, initial_taker, maker, 60_000, false)
}
pub fn recorded_trading_window(
    root: &Path,
    initial_taker: &str,
    maker: Option<(&str, &str)>,
    end_at: i64,
    after_terminal: bool,
) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_trading_with_tail(
        root,
        initial_taker,
        maker,
        end_at,
        if after_terminal { 2 } else { 0 },
        "100",
    )
}
pub fn recorded_settlement(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    // 覆盖显式 2Δ 顺序的 S3 最后成功及费用支付，不改变任何原代理延迟。
    recorded_trading_with_tail(root, "5", None, 60_000, 80, "100")
}
pub fn recorded_settlement_pending_loser(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_trading_with_tail(root, "6", None, 60_000, 40, "6")
}
fn recorded_trading_with_tail(
    root: &Path,
    initial_taker: &str,
    maker: Option<(&str, &str)>,
    end_at: i64,
    tail: u64,
    down_initial: &str,
) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_trading_with_probes(root, initial_taker, maker, end_at, tail, down_initial, &[])
}
#[allow(clippy::too_many_arguments)]
pub fn recorded_trading_with_probes(
    root: &Path,
    initial_taker: &str,
    maker: Option<(&str, &str)>,
    end_at: i64,
    tail: u64,
    down_initial: &str,
    extra_probes: &[i64],
) -> (PublicObservationCoreV1, TradingTimes) {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let mut metadata = metadata_snapshot_row();
    metadata.market_end_utc = end_at;
    let first = record(
        0,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
            metadata.clone(),
        ))),
    );
    metadata.timestamp_received = 20_000;
    metadata.collector_admission_sequence = 2;
    metadata.collector_admission_time_utc_ms = 20_001;
    metadata.metadata_fetch_start_utc = Some(19_990);
    metadata.source_evidence_received_at_utc = Some(20_000);
    metadata.row_evidence_validation_completed_at_utc = Some(20_001);
    metadata.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    metadata.market_runtime_role = MetadataMarketRuntimeRole::Current;
    let active = record(
        1,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata))),
    );
    let mut up = book("100");
    if let PolymarketOrderbookEvent::Book { asks, .. } = &mut up {
        asks[0].size = initial_taker.into();
        asks.push(PolymarketPriceLevel {
            price: "0.7".into(),
            size: "80".into(),
        });
    }
    let mut down = book("100");
    if let PolymarketOrderbookEvent::Book { asset_id, asks, .. } = &mut down {
        *asset_id = DOWN_TOKEN_ID.into();
        asks[0].size = down_initial.into();
    }
    let mut inputs = vec![first, active, pm(2, 1, 21_000, up), pm(3, 2, 21_000, down)];
    let mut times = vec![10_000, 22_000, 22_000, 22_000];
    // 先消耗原taker债务，再以真实价位新增/删除形成唯一maker3。
    let changes = if let Some((_, size)) = maker {
        vec![
            (25_000, 26_000, PolymarketSide::Sell, "0"),
            (27_000, 28_000, PolymarketSide::Buy, size),
            (27_002, 28_000, PolymarketSide::Buy, "0"),
        ]
    } else {
        vec![
            (25_000, 26_000, PolymarketSide::Sell, initial_taker),
            (27_000, 28_000, PolymarketSide::Sell, initial_taker),
        ]
    };
    for (at, visible, side, size) in changes {
        let p = inputs.len() as u64;
        inputs.push(record(
            p,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                p - 1,
                at,
                at + 1,
                PolymarketOrderbookEvent::PriceChange {
                    price_changes: vec![PolymarketPriceChangeEntry {
                        asset_id: UP_TOKEN_ID.into(),
                        price: if matches!(side, PolymarketSide::Buy) {
                            maker.unwrap().0.into()
                        } else {
                            "0.6".into()
                        },
                        size: Some(size.into()),
                        side,
                        best_bid: None,
                        best_ask: None,
                    }],
                },
            ))),
        ));
        times.push(visible);
    }
    let mut probes = vec![29_000, 35_000, 45_000, 55_000];
    if end_at > 100_000 {
        probes.extend([295_000, 305_000, 315_000, 395_000]);
    }
    if !extra_probes.is_empty() {
        probes.extend(extra_probes);
        probes.sort_unstable();
        probes.dedup();
    }
    // 这些探针属于终态前的交易窗口；320s窗口不能先交付396s，
    // 再回退到321.003s终态。更长窗口仍保留395s探针。
    for at in probes.into_iter().filter(|at| *at < end_at) {
        let p = inputs.len() as u64;
        inputs.push(record(
            p,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                p - 1,
                at,
                at + 1,
                PolymarketOrderbookEvent::LastTradePrice {
                    asset_id: UP_TOKEN_ID.into(),
                    price: "0.6".into(),
                    size: None,
                    side: None,
                    fee_rate_bps: None,
                },
            ))),
        ));
        times.push(at + 1_000);
    }
    let mut market = market();
    market.window.end_at_utc_ms = end_at;
    let mut terminal = terminal_metadata_row(&market);
    terminal.timestamp_received = 61_001;
    terminal.collector_admission_time_utc_ms = 61_003;
    terminal.ws_platform_timestamp_raw = Some("61000".into());
    terminal.source_evidence_received_at_utc = Some(61_001);
    terminal.row_evidence_validation_completed_at_utc = Some(61_003);
    // 完整终态字段复用原夹具，仅整体平移结束后的所有原时间。
    let mut value = serde_json::to_value(&terminal).unwrap();
    shift_terminal_times(&mut value);
    if end_at != 60_000 {
        shift_window_times(&mut value, end_at - 60_000);
    }
    terminal = serde_json::from_value(value).unwrap();
    let p = inputs.len() as u64;
    inputs.push(record(
        p,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(terminal))),
    ));
    times.push(end_at + 1_003);
    let mut ws = terminal_orderbook_row(&market);
    ws.timestamp = Some(end_at + 1_000);
    ws.timestamp_received = end_at + 1_001;
    ws.collector_admission_time_utc_ms = end_at + 1_002;
    let p = inputs.len() as u64;
    inputs.push(record(
        p,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(ws)),
    ));
    times.push(end_at + 1_003);
    if tail > 0 {
        for sequence in 1..=tail {
            let at = end_at + sequence as i64 * 5_000;
            let p = inputs.len() as u64;
            inputs.push(record(
                p,
                TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference_after(at, sequence)),
            ));
            times.push(at + 1_000);
        }
    }
    inputs.push(end(inputs.len() as u64));
    let mut group_times = Vec::new();
    for input in inputs {
        let result = apply(&mut core, &input);
        // 终态metadata与对应WS可合为一个真实组；消费时序按已关闭组，
        // 不能把每条输入位置直接当作group_position导致尾部时间错位。
        for group in &result.applied.closed_groups {
            group_times.push(times[group.closed_at_input_position as usize]);
        }
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    (core, TradingTimes(group_times))
}
fn shift_terminal_times(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Number(n) => {
            if let Some(v) = n.as_i64()
                && (30_000..=31_010).contains(&v)
            {
                *value = serde_json::json!(v + 30_000)
            }
        }
        serde_json::Value::Object(m) => {
            for v in m.values_mut() {
                shift_terminal_times(v)
            }
        }
        serde_json::Value::Array(a) => {
            for v in a {
                shift_terminal_times(v)
            }
        }
        _ => {}
    }
}

fn shift_window_times(value: &mut serde_json::Value, delta: i64) {
    match value {
        serde_json::Value::Number(n) => {
            if let Some(v) = n.as_i64()
                && (60_000..=61_010).contains(&v)
            {
                *value = serde_json::json!(v + delta)
            }
        }
        serde_json::Value::String(s) if s == "61000" => *s = (61_000 + delta).to_string(),
        serde_json::Value::Object(m) => {
            for v in m.values_mut() {
                shift_window_times(v, delta)
            }
        }
        serde_json::Value::Array(a) => {
            for v in a {
                shift_window_times(v, delta)
            }
        }
        _ => {}
    }
}
pub fn reference_after(at: i64, sequence: u64) -> PolymarketRtdsChainlinkTwapInput {
    PolymarketRtdsChainlinkTwapInput {
        common: PolymarketRtdsChainlinkBtcUsdRow {
            timestamp_received: at,
            message_timestamp: at - 1,
            payload_timestamp: at - 2,
            age_ms: Some(2),
            symbol: "btc/usd".into(),
            value: "64950.01".into(),
            topic: "crypto_prices_twap_thirty".into(),
            message_type: "update".into(),
            freshness_status: RtdsFreshnessStatus::Fresh,
            subscription_filter: "{}".into(),
            source_native_payload_json: "{\"window_s\":30}".into(),
            collector_host_id: "host".into(),
            collector_region: "eu-west-1".into(),
            source_endpoint: "wss://ws-live-data.polymarket.com".into(),
            schema_version: "polymarket_rtds_chainlink_twap_v1".into(),
        },
        full_accuracy_value: "64950014218511440012345".into(),
        window_s: 30,
        source_sequence_epoch_id: "rtds-epoch".into(),
        collector_admission_sequence: sequence,
        collector_admission_time_utc_ms: at + 1,
    }
}

pub const OTHER_CONDITION_ID: &str =
    "0x2222222222222222222222222222222222222222222222222222222222222222";
pub fn recorded_equal_start_markets(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_two_market_rebate(root, false, false)
}
pub fn recorded_rebate_contributions(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_two_market_rebate(root, true, false)
}
pub fn recorded_staggered_market_ends(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    recorded_two_market_rebate(root, false, true)
}
fn recorded_two_market_rebate(
    root: &Path,
    rebate: bool,
    staggered: bool,
) -> (PublicObservationCoreV1, TradingTimes) {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let mut rows = Vec::new();
    let mut times = Vec::new();
    for active in [false, true] {
        for high in [true, false] {
            let mut metadata = metadata_snapshot_row();
            if rebate {
                metadata.market_end_utc = 320_000;
            }
            if staggered && !high {
                metadata.market_end_utc = 40_000;
            }
            metadata.collector_admission_sequence = rows.len() as u64 + 1;
            if high {
                metadata.condition_id = OTHER_CONDITION_ID.into();
                metadata.clob_market_id = "3195577".into();
                metadata.asset_ids = "[\"21\",\"4\"]".into();
            }
            if active {
                metadata.timestamp_received = 20_000;
                metadata.collector_admission_time_utc_ms = 20_001;
                metadata.metadata_fetch_start_utc = Some(19_990);
                metadata.source_evidence_received_at_utc = Some(20_000);
                metadata.row_evidence_validation_completed_at_utc = Some(20_001);
                metadata.market_lifecycle_state = MetadataMarketLifecycleState::Active;
                metadata.market_runtime_role = MetadataMarketRuntimeRole::Current;
            }
            rows.push(TypedRawRowV2::Existing(
                TypedRawRow::PolymarketMarketMetadata(Box::new(metadata)),
            ));
            times.push(if active { 22_000 } else { 10_000 });
        }
    }
    let mut sequence = 0;
    for (high, token) in [
        (true, "21"),
        (true, "4"),
        (false, UP_TOKEN_ID),
        (false, DOWN_TOKEN_ID),
    ] {
        sequence += 1;
        let mut event = book("100");
        if let PolymarketOrderbookEvent::Book { asset_id, bids, .. } = &mut event {
            *asset_id = token.into();
            if rebate {
                bids[0].price = "0.01".into();
            }
        }
        let mut r = row("epoch-two", sequence, 21_000, 21_001, event);
        if high {
            r.market = OTHER_CONDITION_ID.into();
        }
        rows.push(TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)));
        times.push(22_000);
    }
    for at in [25_000, 27_000] {
        for high in [true, false] {
            sequence += 1;
            let mut r = row(
                "epoch-two",
                sequence,
                at,
                at + 1,
                if rebate {
                    PolymarketOrderbookEvent::PriceChange {
                        price_changes: vec![PolymarketPriceChangeEntry {
                            asset_id: if high { "21" } else { UP_TOKEN_ID }.into(),
                            price: "0.5".into(),
                            size: Some(
                                if at == 27_000 {
                                    "0"
                                } else if high {
                                    "142.868571"
                                } else {
                                    "142.862857"
                                }
                                .into(),
                            ),
                            side: PolymarketSide::Buy,
                            best_bid: None,
                            best_ask: None,
                        }],
                    }
                } else {
                    PolymarketOrderbookEvent::LastTradePrice {
                        asset_id: if high { "21" } else { UP_TOKEN_ID }.into(),
                        price: "0.6".into(),
                        size: None,
                        side: None,
                        fee_rate_bps: None,
                    }
                },
            );
            if high {
                r.market = OTHER_CONDITION_ID.into();
            }
            rows.push(TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(r)));
            times.push(at + 1000);
        }
    }
    if staggered {
        for (seq, at) in [(1, 31_000), (2, 41_000), (3, 51_000)] {
            rows.push(TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference_after(
                at, seq,
            )));
            times.push(at + 1000);
        }
    }
    if rebate {
        for (seq, at) in [
            (1, 35_000),
            (2, 89_099_000),
            (3, 89_101_000),
            (4, 89_110_000),
        ] {
            rows.push(TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference_after(
                at, seq,
            )));
            times.push(at + 1000);
        }
    }
    for (position, row) in rows.into_iter().enumerate() {
        let input = record(position as u64, row);
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    (core, TradingTimes(times))
}

pub fn recorded_reference_then_market(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let inputs = reference_then_market_inputs();
    for input in inputs {
        let applied = apply(&mut core, &input);
        writer.record(input, applied).unwrap();
    }
    writer.stabilize().unwrap();
    (core, TradingTimes(vec![9_001, 10_001, 22_000]))
}

/// 合法原 GTC 各5份；真实盘口正增量及耗用只使其中1/2份成交，其余单独撤销。
pub fn recorded_average_pool(
    root: &Path,
    fractional: bool,
) -> (PublicObservationCoreV1, TradingTimes) {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let mut metadata = metadata_snapshot_row();
    metadata.market_end_utc = 320_000;
    let first = record(
        0,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
            metadata.clone(),
        ))),
    );
    metadata.timestamp_received = 20_000;
    metadata.collector_admission_sequence = 2;
    metadata.collector_admission_time_utc_ms = 20_001;
    metadata.metadata_fetch_start_utc = Some(19_990);
    metadata.source_evidence_received_at_utc = Some(20_000);
    metadata.row_evidence_validation_completed_at_utc = Some(20_001);
    metadata.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    metadata.market_runtime_role = MetadataMarketRuntimeRole::Current;
    let active = record(
        1,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata))),
    );
    let books = [UP_TOKEN_ID, DOWN_TOKEN_ID].map(|token| {
        let mut book = book("100");
        if let PolymarketOrderbookEvent::Book {
            asset_id,
            bids,
            asks,
            ..
        } = &mut book
        {
            *asset_id = token.into();
            bids[0].price = "0.01".into();
            asks[0].price = "0.99".into();
        }
        book
    });
    let mut inputs = vec![
        first,
        active,
        pm(2, 1, 21_000, books[0].clone()),
        pm(3, 2, 21_000, books[1].clone()),
    ];
    let mut times = vec![10_000, 22_000, 22_000, 22_000];
    for (at, visible, token, price, size) in [
        (25_000, 26_000, UP_TOKEN_ID, "0.1", "1"),
        (25_002, 28_000, UP_TOKEN_ID, "0.1", "0"),
        (
            35_000,
            36_000,
            UP_TOKEN_ID,
            if fractional { "0.45" } else { "0.9" },
            if fractional { "2" } else { "1" },
        ),
        (
            35_002,
            38_000,
            UP_TOKEN_ID,
            if fractional { "0.45" } else { "0.9" },
            "0",
        ),
        (45_000, 46_000, DOWN_TOKEN_ID, "0.2", "1"),
        (45_002, 48_000, DOWN_TOKEN_ID, "0.2", "0"),
    ] {
        let position = inputs.len() as u64;
        inputs.push(record(
            position,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                position - 1,
                at,
                at + 1,
                PolymarketOrderbookEvent::PriceChange {
                    price_changes: vec![PolymarketPriceChangeEntry {
                        asset_id: token.into(),
                        price: price.into(),
                        size: Some(size.into()),
                        side: PolymarketSide::Buy,
                        best_bid: None,
                        best_ask: None,
                    }],
                },
            ))),
        ));
        times.push(visible);
    }
    for at in [50_000, 55_000, 60_000] {
        let position = inputs.len() as u64;
        inputs.push(record(
            position,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                position - 1,
                at,
                at + 1,
                PolymarketOrderbookEvent::LastTradePrice {
                    asset_id: UP_TOKEN_ID.into(),
                    price: "0.1".into(),
                    size: None,
                    side: None,
                    fee_rate_bps: None,
                },
            ))),
        ));
        times.push(at + 1_000);
    }
    inputs.push(end(inputs.len() as u64));
    for input in inputs {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    (core, TradingTimes(times))
}

/// 在线测试直接用原构造材料，不经记录读取回灌。
pub fn reference_then_market_inputs() -> Vec<RecordedIngressV1> {
    vec![
        record(
            0,
            TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference_after(9_000, 1)),
        ),
        meta(1),
        pm(2, 1, 22_000, book("100")),
    ]
}

/// 决定时 .9，提交后、平台处理前由真实 Book 更新为 .1；原请求仍保留 .9。
pub fn recorded_market_fee_change(root: &Path) -> (PublicObservationCoreV1, TradingTimes) {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let mut active = metadata_snapshot_row();
    active.timestamp_received = 20_000;
    active.collector_admission_sequence = 2;
    active.collector_admission_time_utc_ms = 20_001;
    active.metadata_fetch_start_utc = Some(19_990);
    active.source_evidence_received_at_utc = Some(20_000);
    active.row_evidence_validation_completed_at_utc = Some(20_001);
    active.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    active.market_runtime_role = MetadataMarketRuntimeRole::Current;
    let mut initial = book("1000");
    if let PolymarketOrderbookEvent::Book { bids, asks, .. } = &mut initial {
        bids[0].price = "0.05".into();
        asks[0].price = "0.9".into();
        asks[0].size = "1000".into();
    }
    let mut down = initial.clone();
    if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut down {
        *asset_id = DOWN_TOKEN_ID.into();
    }
    let mut improved = initial.clone();
    if let PolymarketOrderbookEvent::Book { asks, .. } = &mut improved {
        asks[0].price = "0.1".into();
        asks[0].size = "900".into();
    }
    let mut inputs = vec![
        meta(0),
        record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(active))),
        ),
        pm(2, 1, 21_000, initial),
        pm(3, 2, 21_000, down),
        record(
            4,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a", 3, 22_001, 22_002, improved,
            ))),
        ),
    ];
    let mut times = vec![10_000, 22_000, 22_000, 22_000, 22_002];
    for at in [25_000, 27_000, 29_000] {
        let p = inputs.len() as u64;
        inputs.push(record(
            p,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                p - 1,
                at,
                at + 1,
                PolymarketOrderbookEvent::LastTradePrice {
                    asset_id: UP_TOKEN_ID.into(),
                    price: "0.1".into(),
                    size: None,
                    side: None,
                    fee_rate_bps: None,
                },
            ))),
        ));
        times.push(at + 1_000);
    }
    let p = inputs.len() as u64;
    inputs.push(record(
        p,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
            terminal_metadata_row(&market()),
        ))),
    ));
    times.push(31_003);
    inputs.push(record(
        p + 1,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(terminal_orderbook_row(
            &market(),
        ))),
    ));
    times.push(31_003);
    // 等待原有代理私有回执，保留判定未赎回的真实后继状态。
    for sequence in 1..=8 {
        let at = 35_000 + sequence as i64 * 5_000;
        inputs.push(record(
            inputs.len() as u64,
            TypedRawRowV2::PolymarketRtdsChainlinkTwap(reference_after(at, sequence)),
        ));
        times.push(at + 1_000);
    }
    inputs.push(end(inputs.len() as u64));
    for input in inputs {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    (core, TradingTimes(times))
}
