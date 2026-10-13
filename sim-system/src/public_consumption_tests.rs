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

#[test]
fn checkpoint_prepared_groups_keep_nonempty_empty_and_unknown_valuation() {
    let mut core = core();
    apply(&mut core, &meta(0));
    for input in [
        pm(1, 1, 20_001, book("100")),
        pm(
            2,
            2,
            20_002,
            PolymarketOrderbookEvent::BestBidAsk {
                asset_id: UP_TOKEN_ID.into(),
                best_bid: None,
                best_ask: Some("0.6".into()),
                spread: None,
            },
        ),
    ] {
        let canonical_position = core.next_canonical_position();
        let prepared = prepare_public_ingress_v1(&core, &input, &BTreeMap::new(), &[]).unwrap();
        let applied = core.push(&prepared).unwrap().into_owned();
        let pm = match prepared.current {
            PreparedCanonicalInputV1::PolymarketMessage(pm) => Some(pm),
            _ => panic!("真实PM输入"),
        };
        let position = input.input_position;
        let mut day = data_pipeline_training::day_producer::DayAppliedV1 {
            input,
            applied: applied.applied,
            first_canonical_position: canonical_position,
            prepared_pm: pm,
            contexts: BTreeMap::from([(
                CONDITION_ID.into(),
                core.prepared_pm_context(CONDITION_ID).unwrap(),
            )]),
            initialization_binding: None,
            hidden: vec![],
        };
        for delivery in PreparedPublicGroupV1::from_day(&mut day) {
            let group = delivery.prepared;
            let assets = &group.group.observations[0].assets;
            assert_eq!(
                assets
                    .iter()
                    .find(|a| a.asset.token_id.0 == UP_TOKEN_ID)
                    .unwrap()
                    .valuation_best_bid
                    .value,
                if position == 1 {
                    Some(Some(ExactDecimal("0.5".into())))
                } else {
                    Some(None)
                }
            );
            assert_eq!(
                assets
                    .iter()
                    .find(|a| a.asset.token_id.0 == DOWN_TOKEN_ID)
                    .unwrap()
                    .valuation_best_bid
                    .value,
                None
            );
            let saved = checkpoint::PreparedGroup::capture(&group);
            let mut bytes = Vec::new();
            ciborium::into_writer(&saved, &mut bytes).unwrap();
            let restored: checkpoint::PreparedGroup =
                ciborium::from_reader(bytes.as_slice()).unwrap();
            let restored = restored.restore().unwrap();
            assert_eq!(group.group, restored.group);
        }
    }
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
    settlements: RefCell<Vec<SettlementLifecycleFeedbackV2>>,
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
        self.delivered
            .settlements
            .borrow_mut()
            .extend(batch.settlement_lifecycle.iter().cloned());
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

mod gpu_hidden_tests {
    use super::*;
    include!("gpu_hidden_tests.rs");
}

// 有界GTC验证复用原typedraw→公共归约→独立hidden准备入口。
// 新材料自行派生真实V3身份；旧ExecutionMarketEvent夹具身份不复制进材料。
#[cfg(feature = "shared-execution")]
pub(super) fn gpu_compact_hidden_for_order_fixture(
    initial: &[ExecutionMarketEventV3], events: &[ExecutionMarketEventV3], empty_initial: bool,
) -> (tempfile::TempDir, serde_json::Value) {
    gpu_compact_hidden_for_order_fixture_with_terminal(initial,events,empty_initial,false)
}

#[cfg(feature = "shared-execution")]
pub(super) fn gpu_compact_hidden_for_order_fixture_with_terminal(
    initial: &[ExecutionMarketEventV3], events: &[ExecutionMarketEventV3], empty_initial: bool, terminal: bool,
) -> (tempfile::TempDir, serde_json::Value) {
    gpu_compact_hidden_for_order_fixture_selected(initial,events,empty_initial,terminal,None)
}

#[cfg(feature = "shared-execution")]
pub(super) fn gpu_compact_hidden_for_order_fixture_with_received_range(
    initial: &[ExecutionMarketEventV3], events: &[ExecutionMarketEventV3], terminal: bool, received: std::ops::Range<i64>,
) -> (tempfile::TempDir, serde_json::Value) {
    gpu_compact_hidden_for_order_fixture_selected(initial,events,false,terminal,Some(received))
}

#[cfg(feature = "shared-execution")]
fn gpu_compact_hidden_for_order_fixture_selected(
    initial: &[ExecutionMarketEventV3], events: &[ExecutionMarketEventV3], empty_initial: bool,
    terminal: bool, received: Option<std::ops::Range<i64>>,
) -> (tempfile::TempDir, serde_json::Value) {
    use data_pipeline_training::{compact_hidden_material::{prepare_compact_hidden_material_v1,CompactHiddenMaterialReaderV1},
        compact_hidden_gpu::next_compact_hidden_gpu_bucket_v1,gpu_initial_sort::GpuInitialRunOrderV1};
    let (directory,public)=gpu_public_for_order_fixture_selected(initial,events,empty_initial,terminal,received);
    let public_path=directory.path().join("public");
    let hidden_path=directory.path().join("hidden");
    let mut assets=Vec::new();
    let mut conditions=std::collections::BTreeSet::new();
    for event in initial {
        if conditions.insert(&event.market.condition_id.0) {
            assets.extend(public.pm_context(&event.market.condition_id.0).unwrap().assets.into_iter().map(|v|v.asset));
        }
    }
    let mut sort=config().sort;sort.merge_workers=8;
    let order=Arc::new(GpuInitialRunOrderV1::start(Path::new(&std::env::var("B_GPU_PYTHON").unwrap()),
        Path::new(&std::env::var("B_GPU_SORT_WORKER").unwrap())).unwrap());
    prepare_compact_hidden_material_v1(&public_path,&hidden_path,1<<20,&sort,order,"gpu-order-hidden".into(),None,|_,_,_,_| {}).unwrap();
    let mut reader=CompactHiddenMaterialReaderV1::open(&hidden_path,&public_path,1,1<<20,1<<20,|_,_,_,_| {}).unwrap();
    let mut buckets=Vec::new();let mut terminal_facts=Vec::new();
    while let Some(bucket)=next_compact_hidden_gpu_bucket_v1(&mut reader).unwrap() {
        if terminal {
            for position in &bucket.bucket.reference_positions {
                let (_,fact)=reader.reference_fact_with_market(*position).unwrap();
                if let Some(fact)=fact {terminal_facts.push(serde_json::json!({"at":bucket.bucket.bucket_time_utc_ms,"fact":fact}));}
            }
        }
        buckets.push(bucket);
    }
    (directory,serde_json::json!({"buckets":buckets,"assets":assets,"terminal_facts":terminal_facts,
        "source_binding":{"binary":std::env::var("B_GPU_HIDDEN_CLI").unwrap(),"hidden":hidden_path,"public":public_path,
            "rows":1,"bytes":1<<20,"max_batch_bytes":1<<20}}))
}

// 复用同一typedraw/Core/writer冷构建；联合冻结不需要额外hidden准备。
#[cfg(feature = "shared-execution")]
pub(super) fn gpu_public_for_order_fixture(
    initial: &[ExecutionMarketEventV3], events: &[ExecutionMarketEventV3], empty_initial: bool,
) -> (tempfile::TempDir, PublicObservationCoreV1) {
    gpu_public_for_order_fixture_with_terminal(initial,events,empty_initial,false)
}

#[cfg(feature = "shared-execution")]
fn gpu_public_for_order_fixture_with_terminal(
    initial: &[ExecutionMarketEventV3], events: &[ExecutionMarketEventV3], empty_initial: bool, terminal: bool,
) -> (tempfile::TempDir, PublicObservationCoreV1) {
    gpu_public_for_order_fixture_selected(initial,events,empty_initial,terminal,None)
}

#[cfg(feature = "shared-execution")]
fn gpu_public_for_order_fixture_selected(
    initial: &[ExecutionMarketEventV3], events: &[ExecutionMarketEventV3], empty_initial: bool,
    terminal: bool, received_range: Option<std::ops::Range<i64>>,
) -> (tempfile::TempDir, PublicObservationCoreV1) {
    use data_pipeline_training::compact_public::PublicCompactWriterV1;
    let directory=tempfile::tempdir().unwrap();
    let public_path=directory.path().join("public");
    let received=initial[0].market.window.start_at_utc_ms-100;
    let mut inputs=Vec::new();
    let mut conditions=std::collections::BTreeSet::new();
    for event in initial {
        let market=&event.market;
        if !conditions.insert(&market.condition_id.0) {continue;}
        let assets=initial.iter().filter(|event|event.market==*market).map(|event|match &event.payload {
            ExecutionMarketEventPayloadV3::InitialL2Snapshot(v)=>v.asset.clone(),
            _=>panic!("原初始化必须是双资产完整快照"),
        }).collect::<Vec<_>>();
        let mut metadata=metadata_snapshot_row();
        metadata.condition_id=market.condition_id.0.clone();metadata.clob_market_id=market.clob_market_id.0.clone();
        metadata.asset_ids=serde_json::to_string(&assets.iter().map(|v|&v.token_id.0).collect::<Vec<_>>()).unwrap();
        metadata.outcomes_raw=Some(serde_json::to_string(&assets.iter().map(|v|&v.outcome.label).collect::<Vec<_>>()).unwrap());
        metadata.market_start_utc=market.window.start_at_utc_ms;metadata.market_end_utc=market.window.end_at_utc_ms;
        metadata.timestamp_received=received;metadata.collector_admission_time_utc_ms=received+1;
        metadata.fixed_identity_verified_at_utc=received-1;metadata.metadata_fetch_start_utc=Some(received-10);
        metadata.source_evidence_received_at_utc=Some(received);metadata.row_evidence_validation_completed_at_utc=Some(received+1);
        inputs.push(record(inputs.len() as u64,TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata)))));
    }
    for event in initial.iter().chain(events) {
        let payload=match &event.payload {
            ExecutionMarketEventPayloadV3::InitialL2Snapshot(v)=>PolymarketOrderbookEvent::Book {
                asset_id:v.asset.token_id.0.clone(),last_trade_price:None,
                bids:if empty_initial {Vec::new()} else {v.bids.iter().map(|v|PolymarketPriceLevel {price:v.price_pusd_per_share.0.clone(),size:v.quantity_shares.0.clone()}).collect()},
                asks:if empty_initial {Vec::new()} else {v.asks.iter().map(|v|PolymarketPriceLevel {price:v.price_pusd_per_share.0.clone(),size:v.quantity_shares.0.clone()}).collect()},
            },
            ExecutionMarketEventPayloadV3::LevelExecutionTransition(v)=>PolymarketOrderbookEvent::PriceChange {
                price_changes:vec![PolymarketPriceChangeEntry {asset_id:v.asset.token_id.0.clone(),
                    price:v.price_pusd_per_share.0.clone(),size:Some(v.quantity_after_shares.0.clone()),
                    side:if v.side==BookSide::Bid {PolymarketSide::Buy} else {PolymarketSide::Sell},best_bid:None,best_ask:None}],
            },
            ExecutionMarketEventPayloadV3::BestBidAsk(v)=>PolymarketOrderbookEvent::BestBidAsk {
                asset_id:v.asset.token_id.0.clone(),best_bid:v.best_bid_pusd_per_share.as_ref().map(|v|v.0.clone()),
                best_ask:v.best_ask_pusd_per_share.as_ref().map(|v|v.0.clone()),spread:None,
            },
            ExecutionMarketEventPayloadV3::PublicTrade(v)=>PolymarketOrderbookEvent::LastTradePrice {
                asset_id:v.asset.token_id.0.clone(),price:v.price_pusd_per_share.0.clone(),size:Some(v.quantity_shares.0.clone()),
                side:Some(if v.aggressor_side==contracts::TradeSide::Buy {PolymarketSide::Buy}else{PolymarketSide::Sell}),
                fee_rate_bps:v.fee_rate_bps.as_ref().map(|v|v.0.clone()),
            },
            _=>panic!("这个夹具只重编码原Book、PriceChange、BestBidAsk与PublicTrade源值，不抄before"),
        };
        let source=event.time.source_event_at_utc_ms.expect("原PM须有真实源时刻");
        let mut pm=row("epoch-1",inputs.len() as u64,source,event.time.received_at_utc_ms,payload);
        pm.market=event.market.condition_id.0.clone();
        inputs.push(record(inputs.len() as u64,TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(pm))));
    }
    if terminal && std::env::var_os("B_GPU_AUTO_TWO_MARKETS").is_some() {
        let mut seen=std::collections::BTreeSet::new();
        for event in initial {
            let market=&event.market;
            if !seen.insert(&market.condition_id.0) {continue;}
            let assets=initial.iter().filter(|event|event.market==*market).map(|event|match &event.payload {
                ExecutionMarketEventPayloadV3::InitialL2Snapshot(book)=>book.asset.clone(),_=>unreachable!(),
            }).collect::<Vec<_>>();
            assert_eq!(assets.len(),2);
            let at=market.window.end_at_utc_ms+9000;
            let mut metadata=terminal_metadata_row(market);
            let mut ws=terminal_orderbook_row(market);
            metadata.timestamp_received=at+1;metadata.collector_admission_time_utc_ms=at+3;
            metadata.collector_admission_sequence+=seen.len() as u64;
            metadata.source_evidence_received_at_utc=Some(at+1);
            metadata.row_evidence_validation_completed_at_utc=Some(at+2);
            metadata.lifecycle_applied_at_utc=Some(at+3);metadata.runtime_role_applied_at_utc=Some(at+3);
            metadata.ws_platform_timestamp_utc=Some(at);metadata.ws_platform_timestamp_raw=Some(at.to_string());
            metadata.asset_ids=serde_json::to_string(&assets.iter().map(|v|&v.token_id.0).collect::<Vec<_>>()).unwrap();
            metadata.winning_asset_id=Some(assets[0].token_id.0.clone());metadata.winning_outcome=Some(assets[0].outcome.label.clone());
            ws.timestamp=Some(at);ws.timestamp_received=at+1;ws.collector_admission_time_utc_ms=at+3;
            ws.collector_admission_sequence+=seen.len() as u64;
            metadata.ws_claimant_collector_admission_sequence=Some(ws.collector_admission_sequence);
            let PolymarketOrderbookEvent::MarketResolved {asset_ids,winning_asset_id,winning_outcome,..}=&mut ws.event else {unreachable!()};
            *asset_ids=assets.iter().map(|v|v.token_id.0.clone()).collect();
            *winning_asset_id=assets[0].token_id.0.clone();*winning_outcome=assets[0].outcome.label.clone();
            inputs.push(record(inputs.len() as u64,TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata)))));
            inputs.push(record(inputs.len() as u64,TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(ws))));
        }
        assert_eq!(seen.len(),2);
    } else if terminal {
        let market=&initial[0].market;
        let assets=initial.iter().map(|event| match &event.payload {
            ExecutionMarketEventPayloadV3::InitialL2Snapshot(book)=>book.asset.clone(),_=>unreachable!(),
        }).collect::<Vec<_>>();
        // 原两行WS终态模板；仅替换本场景的完整市场/资产身份，保source/received/admission。
        let mut metadata=terminal_metadata_row(market);
        metadata.asset_ids=serde_json::to_string(&assets.iter().map(|asset|&asset.token_id.0).collect::<Vec<_>>()).unwrap();
        metadata.winning_asset_id=Some(assets[0].token_id.0.clone());metadata.winning_outcome=Some(assets[0].outcome.label.clone());
        let mut ws=terminal_orderbook_row(market);
        let PolymarketOrderbookEvent::MarketResolved {asset_ids,winning_asset_id,winning_outcome,..}=&mut ws.event else {unreachable!()};
        *asset_ids=assets.iter().map(|asset|asset.token_id.0.clone()).collect();
        *winning_asset_id=assets[0].token_id.0.clone();*winning_outcome=assets[0].outcome.label.clone();
        inputs.push(record(inputs.len() as u64,TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata)))));
        inputs.push(record(inputs.len() as u64,TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(ws))));
    }
    if let Some(range)=&received_range {
        inputs.retain(|input| match &input.ingress {
            DptIngressV1::Message {source_evidence,..}=>range.contains(&source_evidence.received_at_utc_ms),
            _=>true,
        });
        for (position,input) in inputs.iter_mut().enumerate() {input.input_position=position as u64;}
    }
    inputs.push(end(inputs.len() as u64));
    let mut public=core();
    let mut writer=match received_range {
        Some(range)=>PublicCompactWriterV1::create_for_received_range(&public_path,public.header().clone(),range),
        None=>PublicCompactWriterV1::create(&public_path,public.header().clone()),
    }.unwrap();
    for input in inputs {
        let prepared=prepare_public_ingress_v1(&public,&input,&BTreeMap::new(),&[]).unwrap();
        let applied=public.push_facts(&prepared).unwrap();
        writer.push_prepared(&input,&prepared,&applied).unwrap();
    }
    writer.publish().unwrap();
    (directory,public)
}
async fn public_execution(
    path: &Path,
    core: &PublicObservationCoreV1,
    role: SimCallerRoleV1,
) -> (PublicSimExecutionV1, Rc<DeliveredFeedbackState>) {
    public_execution_capacity(path, core, role, 32, None).await
}
async fn public_execution_capacity(
    path: &Path,
    core: &PublicObservationCoreV1,
    role: SimCallerRoleV1,
    capacity: usize,
    rules: Option<Arc<SealedProcessRuleSnapshotV3>>,
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
    public_execution_with_scenario_capacity(path, scenario, role, capacity, rules).await
}
async fn public_execution_with_scenario(
    path: &Path,
    scenario: Arc<SealedPlatformScenarioV1>,
    role: SimCallerRoleV1,
) -> (PublicSimExecutionV1, Rc<DeliveredFeedbackState>) {
    public_execution_with_scenario_capacity(path, scenario, role, 32, None).await
}
async fn public_execution_with_scenario_capacity(
    path: &Path,
    scenario: Arc<SealedPlatformScenarioV1>,
    role: SimCallerRoleV1,
    capacity: usize,
    rules: Option<Arc<SealedProcessRuleSnapshotV3>>,
) -> (PublicSimExecutionV1, Rc<DeliveredFeedbackState>) {
    public_execution_with_scenario_capacity_and_projection(path,scenario,role,capacity,rules,None,None).await
}
async fn public_execution_with_scenario_capacity_and_projection(
    path: &Path,
    scenario: Arc<SealedPlatformScenarioV1>,
    role: SimCallerRoleV1,
    capacity: usize,
    rules: Option<Arc<SealedProcessRuleSnapshotV3>>,
    make_projection: Option<Box<dyn FnOnce(&Path,&mut RunOpenedV2)->Box<dyn PublicProjectionPortV1>>>,
    finality: Option<Arc<TradeFinalityPolicyV2>>,
) -> (PublicSimExecutionV1, Rc<DeliveredFeedbackState>) {
    public_execution_with_scenario_capacity_configuration(
        path,scenario,role,capacity,rules,make_projection,finality,None,false,"10",
    ).await
}
async fn public_execution_with_scenario_capacity_layout(
    path: &Path,
    scenario: Arc<SealedPlatformScenarioV1>,
    role: SimCallerRoleV1,
    capacity: usize,
    rules: Option<Arc<SealedProcessRuleSnapshotV3>>,
    day_binding: Option<HistoricalDayLogBindingV1>,
    finite_historical: bool,
) -> (PublicSimExecutionV1, Rc<DeliveredFeedbackState>) {
    public_execution_with_scenario_capacity_configuration(
        path,scenario,role,capacity,rules,None,None,day_binding,finite_historical,"10",
    ).await
}
async fn public_execution_with_scenario_capacity_configuration(
    path: &Path,
    scenario: Arc<SealedPlatformScenarioV1>,
    role: SimCallerRoleV1,
    capacity: usize,
    rules: Option<Arc<SealedProcessRuleSnapshotV3>>,
    make_projection: Option<Box<dyn FnOnce(&Path,&mut RunOpenedV2)->Box<dyn PublicProjectionPortV1>>>,
    finality: Option<Arc<TradeFinalityPolicyV2>>,
    day_binding: Option<HistoricalDayLogBindingV1>,
    finite_historical: bool,
    cash: &str,
) -> (PublicSimExecutionV1, Rc<DeliveredFeedbackState>) {
    let rules = match rules {
        Some(rules) => rules,
        None => successful_snapshot_v3_for_sim_tests().await,
    };
    let finality = finality.unwrap_or_else(successful_policy_v2_for_tests);
    let latency = load_embedded_proxy_latency_profile().unwrap();
    let mut opened = opened(
        "public-consumption-test",
        &rules,
        &finality,
        &scenario,
        &latency,
    );
    let delivered = Rc::new(DeliveredFeedbackState::default());
    let projection = match make_projection {
        Some(make) => make(path,&mut opened),
        None => Box::new(DeliveredProjection {accepted:0,delivered:delivered.clone()}) as Box<dyn PublicProjectionPortV1>,
    };
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
    let mut execution = match (day_binding, finite_historical) {
        (Some(binding), _) => PublicSimExecutionV1::create_new_historical_day(
            path,
            spec_opened,
            rules,
            finality,
            scenario,
            latency,
            projection,
            MAX_GROUP_BYTES,
            capacity,
            binding,
        ),
        (None, true) => PublicSimExecutionV1::create_new_historical(
            path,
            spec_opened,
            rules,
            finality,
            scenario,
            latency,
            projection,
            MAX_GROUP_BYTES,
            capacity,
        ),
        (None, false) => PublicSimExecutionV1::create_new(
            path,
            spec_opened,
            rules,
            finality,
            scenario,
            latency,
            projection,
            MAX_GROUP_BYTES,
            capacity,
        ),
    }
    .unwrap();
    let mut session = spec("public-consumption-test", "create-public");
    session.caller_role = role;
    session.initial_account_state.available_pusd = ExactDecimal(cash.into());
    finite_feeding::feed(
        &mut execution,
        |execution| execution.create_session(session.clone()),
        PublicSimExecutionV1::wait_for_recording_capacity,
    )
    .unwrap();
    finite_feeding::feed(
        &mut execution,
        |execution| execution.activate_session("activate-public".into()),
        PublicSimExecutionV1::wait_for_recording_capacity,
    )
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
        Some(21_999)
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
        Some(21_999)
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
#[tokio::test(flavor = "current_thread")]
async fn public_normal_end_reads_original_groups_and_own_pm_bindings() {
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
    let mut original_reports = Vec::new();
    while let Some(row) = original.next_row().unwrap() {
        if let PublicStoredRowV1::PmReport { report, .. } = &row {
            original_reports.push(report.clone());
        }
        if let PublicStoredRowV1::PmStepBinding(binding) = row {
            let p = binding.position;
            original_bindings.insert(
                (p.canonical_record_position, p.member_index, p.step_index),
                binding.derived_id,
            );
        }
    }
    let candidate = driver.complete().unwrap();
    assert_eq!(candidate.accepted_input_count(), expected_count);
    let (_, mut reader) = SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).unwrap();
    let mut groups = 0;
    let mut pm_bindings = 0;
    let mut reports = Vec::new();
    loop {
        let (next, inputs) = reader.next_group().unwrap();
        reader = next;
        let Some(inputs) = inputs else {
            break;
        };
        for input in inputs {
            if let LoggedBusinessInputV2::PublicGroup(group) = input {
                groups += 1;
                reports.extend(group.pm_reports);
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
    assert!(!original_reports.is_empty());
    assert_eq!(
        reports, original_reports,
        "报告来源、处置和顺序沿原公共归约保留"
    );
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

// 同一原录制夹具同时验证旧日志和原输入可重建的日布局；不是新真实日作业。
fn fixture_log_counts(
    path: &Path,
    root: &Path,
    hidden: &data_pipeline_training::hidden_preparation::HiddenPreparationV1,
) {
    let bytes = std::fs::metadata(path).unwrap().len();
    let (opened, mut recovery) = SimEventLogRecoveryV2::open(path, MAX_GROUP_BYTES).unwrap();
    let layout = if recovery.historical_day_binding_for_tests().is_some() {
        "SIMDAY01"
    } else {
        "JSON"
    };
    let mut original_hidden = RecordedHiddenBucketSourceV1::open(hidden, root, 1, 1 << 20).unwrap();
    let mut frames = 0;
    let mut categories = BTreeMap::<&str, u64>::new();
    loop {
        let (next, inputs) = recovery.next_group().unwrap();
        recovery = next;
        let Some(inputs) = inputs else { break };
        frames += 1;
        for input in inputs {
            if let LoggedBusinessInputV2::DayHiddenStepBucket(locator) = &input {
                let original = original_hidden.next_bucket().unwrap().unwrap();
                let HiddenBucketReferenceV2::Recorded(reference) = original.reference else {
                    unreachable!()
                };
                assert_eq!(
                    locator.applied_input_position,
                    reference.committed_prefix.next_input_position
                );
                assert_eq!(locator.bucket_time_utc_ms, reference.bucket_time_utc_ms);
                assert_eq!(
                    (locator.index_start, locator.index_end),
                    (reference.index_start, reference.index_end)
                );
                assert_eq!(locator.closure, reference.closure);
                assert_eq!(
                    original.records.len() as u64,
                    locator.index_end - locator.index_start
                );
            }
            let category = match input {
                LoggedBusinessInputV2::PublicGroup(_)
                | LoggedBusinessInputV2::DayPublicGroup(_) => "public",
                LoggedBusinessInputV2::HiddenStepBucket(_)
                | LoggedBusinessInputV2::DayHiddenStepBucket(_) => "hidden",
                LoggedBusinessInputV2::PublicObserveTime(_) => "observe",
                LoggedBusinessInputV2::PublicCloseTime(_) => "close",
                LoggedBusinessInputV2::PlatformPrefixAdvance(_) => "prefix",
                _ => "full-business",
            };
            *categories.entry(category).or_default() += 1;
        }
    }
    eprintln!(
        "fixture SIM layout={layout}, role={:?}, bytes={bytes}, accepted_frames={frames}, inputs={categories:?}",
        opened.durability_role
    );
}
fn fixture_day_binding(
    core: &PublicObservationCoreV1,
    sequence: &str,
    hidden: &str,
) -> HistoricalDayLogBindingV1 {
    HistoricalDayLogBindingV1 {
        input_stream_id: core.header().input_stream_id.clone(),
        consumption_sequence_ref: sequence.into(),
        hidden_input_set_identity_sha256: Sha256Hex("a".repeat(64)),
        hidden_consumer_stream_id: hidden.into(),
        hidden_ordering_version:
            data_pipeline_training::hidden_stream::STREAMING_HIDDEN_ORDERING_VERSION.into(),
    }
}
fn fixture_day_bucket(
    bucket: AcceptedHiddenStepBucketV1,
    binding: &HistoricalDayLogBindingV1,
) -> AcceptedHiddenStepBucketV1 {
    let HiddenBucketReferenceV2::Recorded(reference) = bucket.reference else {
        return bucket;
    };
    AcceptedHiddenStepBucketV1 {
        reference: HiddenBucketReferenceV2::Streaming(ClosedStreamingHiddenStepBucketV1 {
            source: StreamingHiddenSourceBindingV1 {
                input_stream_id: binding.input_stream_id.clone(),
                input_set_identity_sha256: binding.hidden_input_set_identity_sha256.clone(),
                applied_prefix: AppliedIngressPrefixV1 {
                    input_stream_id: reference.committed_prefix.input_stream_id,
                    next_input_position: reference.committed_prefix.next_input_position,
                },
                ordering_version: binding.hidden_ordering_version.clone(),
            },
            bucket_time_utc_ms: reference.bucket_time_utc_ms,
            index_start: reference.index_start,
            index_end: reference.index_end,
            closure: reference.closure,
        }),
        records: bucket.records,
    }
}
#[tokio::test(flavor = "current_thread")]
async fn historical_day_layout_rebuilds_original_inputs_and_preserves_business_sequence() {
    // 两布局均使用正式HistoricalBacktest/FiniteHistorical入口；原CleanRun/Paper用例保留。
    for day_log in [false, true] {
        price_change_bba_before_initial_books_preserves_preloading_and_private_quantities_layout(
            day_log, true,
        )
        .await;
        midnight_resolution_without_initial_books_redeems_zero_balance_once_layout(day_log, true)
            .await;
        trusted_observation_keeps_source_prefix_and_sends_at_independent_time_layout(day_log, true)
            .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn price_change_bba_before_initial_books_preserves_preloading_and_private_quantities() {
    price_change_bba_before_initial_books_preserves_preloading_and_private_quantities_layout(
        false, false,
    )
    .await;
}

async fn price_change_bba_before_initial_books_preserves_preloading_and_private_quantities_layout(
    day_log: bool,
    finite_historical: bool,
) {
    // 09-12 首 price_change 的真实资产、价格、局部数量和三项时间。
    // 后续完整 book 是独立构造的有效夹具，不冒称真实日回放。
    const START: i64 = 1_789_171_200_000;
    const UP: &str =
        "107908796001205601126362103961466850840177701751257482698676558640773709147754";
    const DOWN: &str =
        "19032856667784098829291953695654902969996580284236420719127791921975594839098";
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let mut metadata = metadata_snapshot_row();
    metadata.asset_ids = format!(r#"["{UP}","{DOWN}"]"#);
    metadata.market_start_utc = START;
    metadata.market_end_utc = START + 300_000;
    metadata.timestamp_received = START - 299_934;
    metadata.collector_admission_time_utc_ms = metadata.timestamp_received + 1;
    metadata.fixed_identity_verified_at_utc = metadata.timestamp_received - 1;
    metadata.source_evidence_received_at_utc = Some(metadata.timestamp_received);
    metadata.row_evidence_validation_completed_at_utc = Some(metadata.timestamp_received + 1);
    metadata.metadata_fetch_start_utc = Some(metadata.timestamp_received - 10);
    let message = |position, source, received, admitted, event| {
        let mut value = row("epoch-a", position, source, received, event);
        value.collector_admission_time_utc_ms = admitted;
        record(
            position,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(value)),
        )
    };
    let initial_book = |token: &str| PolymarketOrderbookEvent::Book {
        asset_id: token.into(),
        bids: vec![PolymarketPriceLevel {
            price: "0.50".into(),
            size: "100".into(),
        }],
        asks: vec![PolymarketPriceLevel {
            price: "0.51".into(),
            size: "80".into(),
        }],
        last_trade_price: None,
    };
    let inputs = [
        record(
            0,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata))),
        ),
        message(
            1,
            START,
            START + 12,
            START + 2331,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![
                    PolymarketPriceChangeEntry {
                        asset_id: UP.into(),
                        price: "0.35".into(),
                        size: Some("40".into()),
                        side: PolymarketSide::Buy,
                        best_bid: Some("0.50".into()),
                        best_ask: Some("0.51".into()),
                    },
                    PolymarketPriceChangeEntry {
                        asset_id: DOWN.into(),
                        price: "0.65".into(),
                        size: Some("40".into()),
                        side: PolymarketSide::Sell,
                        best_bid: Some("0.49".into()),
                        best_ask: Some("0.50".into()),
                    },
                ],
            },
        ),
        message(
            2,
            START + 100_000,
            START + 100_012,
            START + 100_013,
            initial_book(UP),
        ),
        message(
            3,
            START + 110_000,
            START + 110_012,
            START + 110_013,
            initial_book(DOWN),
        ),
        message(
            4,
            START + 120_000,
            START + 120_012,
            START + 120_013,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![PolymarketPriceChangeEntry {
                    asset_id: UP.into(),
                    price: "0.50".into(),
                    size: Some("80".into()),
                    side: PolymarketSide::Buy,
                    best_bid: None,
                    best_ask: None,
                }],
            },
        ),
        end(5),
    ];
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    for input in inputs {
        let result = apply(&mut core, &input);
        if input.input_position == 1 {
            let reports = &result.pm_reductions[0].1.reports;
            assert_eq!(
                reports
                    .iter()
                    .map(|r| r.reason_code.as_str())
                    .collect::<Vec<_>>(),
                [
                    "POLYMARKET_PRICE_CHANGE_FULL_BOOK_REQUIRED",
                    "POLYMARKET_BBA_UPDATE",
                    "POLYMARKET_PRICE_CHANGE_FULL_BOOK_REQUIRED",
                    "POLYMARKET_BBA_UPDATE",
                ]
            );
        }
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    let path = temp.path().join("sim.jsonl");
    let binding = fixture_day_binding(&core, "preloading-bba", "hidden-test");
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
    let (execution, _) = public_execution_with_scenario_capacity_layout(
        &path,
        scenario,
        SimCallerRoleV1::HistoricalBacktest,
        32,
        None,
        day_log.then(|| binding.clone()),
        finite_historical,
    )
    .await;
    let hidden = prepare_hidden_v1(
        core,
        PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap(),
        &temp.path().join("hidden"),
        &config(),
        "hidden-test".into(),
    )
    .unwrap();
    let mut recorded = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    // 本夹具不添加宿主延迟，公共组沿正式来源可见时刻交付。
    let source = std::iter::from_fn(move || recorded.next_visible_group());
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "preloading-bba".into());
    driver.enable_finite_recording_wait().unwrap();
    driver.next_group().unwrap().unwrap(); // 元数据准入先于任何隐藏价格事实。
    let mut buckets = RecordedHiddenBucketSourceV1::open(&hidden, &root, 1, 1 << 20).unwrap();
    let mut seen = Vec::new();
    while let Some(bucket) = buckets.next_bucket().unwrap() {
        let bucket = if day_log {
            fixture_day_bucket(bucket, &binding)
        } else {
            bucket
        };
        let at = bucket.reference.bucket_time_utc_ms();
        if at == START {
            let quotes = bucket
                .records
                .iter()
                .filter_map(|r| match r {
                    HiddenExecutionRecordV1::Step { step, .. } => match &step.change {
                        data_pipeline_core::pm_reduction::PmStepChangeV1::BestBidAsk {
                            quote,
                            ..
                        } => Some(quote),
                        _ => None,
                    },
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(quotes.len(), 2);
            // 正式十进制文本去除尾零；独立期望保留精确价格，不复用被测转换。
            for (token, bid, ask) in [(UP, "0.5", "0.51"), (DOWN, "0.49", "0.5")] {
                let quote = quotes.iter().find(|q| q.asset.token_id.0 == token).unwrap();
                assert_eq!(
                    quote.best_bid_pusd_per_share,
                    Some(ExactDecimal(bid.into()))
                );
                assert_eq!(
                    quote.best_ask_pusd_per_share,
                    Some(ExactDecimal(ask.into()))
                );
            }
            assert!(!bucket.records.iter().any(|r| matches!(r,
            HiddenExecutionRecordV1::Step { step, .. } if matches!(&step.change,
                data_pipeline_core::pm_reduction::PmStepChangeV1::InitialSnapshot { .. }
                | data_pipeline_core::pm_reduction::PmStepChangeV1::LevelChange { .. }))));
        }
        driver.hidden_bucket(bucket).unwrap();
        driver.next_group().unwrap().unwrap();
        seen.push(at);
        let session = driver
            .execution
            .actor
            .state
            .as_ref()
            .unwrap()
            .session
            .as_ref()
            .unwrap();
        let platform = session.hidden_platform_markets.as_ref().unwrap();
        if at <= START + 100_000 {
            assert_eq!(
                platform.buy_admission(CONDITION_ID, UP, at, START + 300_000),
                crate::market::MarketBuyAdmissionV2::EvidenceUnknownNoEffect(
                    crate::market::MarketBuyEvidenceUnknownV2::Preloading
                )
            );
        } else {
            assert!(matches!(
                platform.buy_admission(CONDITION_ID, UP, at, START + 300_000),
                crate::market::MarketBuyAdmissionV2::Ready { .. }
            ));
        }
        assert_eq!(
            platform.pm_quantity(CONDITION_ID, UP, BookSide::Bid, Decimal::new(5, 1)),
            if at == START {
                Decimal::ZERO
            } else if at == START + 120_000 {
                Decimal::from(80)
            } else {
                Decimal::from(100)
            }
        );
        if at == START {
            for asset in &driver.frames()[0].observation.assets {
                assert!(asset.bids.value.is_none() && asset.asks.value.is_none());
                assert!(asset.best_bid_ask.value.is_some());
            }
            let before = session.execution_state.next_account_transition_seq();
            let receipt = driver
                .decision()
                .unwrap()
                .request(BusinessRequestV2::PostOrder(PostOrderRequestV2 {
                    context: RequestContextV2 {
                        global_request_id: "preloading-buy".into(),
                        caller_correlation_id: None,
                    },
                    item: post_item(
                        "preloading-buy",
                        &ConditionId(CONDITION_ID.into()),
                        &TokenId(UP.into()),
                        SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: "0.2".parse().unwrap(),
                            size_shares: "5".parse().unwrap(),
                            post_only: true,
                        },
                    ),
                }))
                .unwrap();
            assert!(receipt.platform_arrived_at_utc_ms < START + 100_000);
            driver
                .observe_at(receipt.platform_arrived_at_utc_ms + 1)
                .unwrap();
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
                session.execution_state.next_account_transition_seq(),
                before
            );
            assert!(
                session
                    .execution_state
                    .active_platform_order_ids()
                    .is_empty()
            );
        }
    }
    assert_eq!(
        seen,
        [START, START + 100_000, START + 110_000, START + 120_000]
    );
    driver.stop_and_drain().unwrap();
    fixture_log_counts(&path, &root, &hidden);
}

#[tokio::test(flavor = "current_thread")]
async fn midnight_resolution_without_initial_books_redeems_zero_balance_once() {
    midnight_resolution_without_initial_books_redeems_zero_balance_once_layout(false, false).await;
}

async fn midnight_resolution_without_initial_books_redeems_zero_balance_once_layout(
    day_log: bool,
    finite_historical: bool,
) {
    // 已发生失败的旧市场：午夜仅登记 metadata，不带前日盘口或账户。
    // 终态 source/received/admitted、WS epoch/sequence 和赢家均取原事故证据。
    const CONDITION: &str = "0x958f8bdb135b6b06eb2943729196ec066aa5499599fb6f9fe00e718b44169af9";
    const UP: &str =
        "86414155968606164816894781771088030693649396591540249038646879610300960485835";
    const DOWN: &str =
        "114524727566509658620026900094951031660526443095197303364728454725101893208982";
    const END: i64 = 1_789_171_200_000;
    const SOURCE: i64 = 1_789_171_353_344;
    const RECEIVED: i64 = 1_789_171_353_356;
    const EPOCH: &str = "2c8534678df0e1a21812eed94cd0a8e9a4d2c437308afd1c5e3b19b75a9bd237";
    const SEQUENCE: u64 = 24_734_572;
    let market = MarketIdentityV2 {
        condition_id: ConditionId(CONDITION.into()),
        clob_market_id: ClobMarketId("4457758".into()),
        window: MarketWindowV2 {
            start_at_utc_ms: END - 300_000,
            end_at_utc_ms: END,
        },
    };
    let mut metadata = metadata_snapshot_row();
    metadata.condition_id = CONDITION.into();
    metadata.clob_market_id = market.clob_market_id.0.clone();
    metadata.asset_ids = format!(r#"["{UP}","{DOWN}"]"#);
    metadata.market_start_utc = market.window.start_at_utc_ms;
    metadata.market_end_utc = END;
    metadata.timestamp_received = 1_789_170_600_107;
    metadata.collector_admission_time_utc_ms = 1_789_170_600_111;
    metadata.fixed_identity_verified_at_utc = metadata.timestamp_received - 1;
    metadata.source_evidence_received_at_utc = Some(metadata.timestamp_received);
    metadata.row_evidence_validation_completed_at_utc =
        Some(metadata.collector_admission_time_utc_ms);
    metadata.metadata_fetch_start_utc = Some(metadata.timestamp_received - 10);
    // 原小证据核实了此中间行的种类、时间及 sequence，未包含状态枚举。
    // 其余状态/无外源字段沿 core_tests::local_transition_row 构造，非原行复写。
    let mut active = metadata.clone();
    active.metadata_row_kind = PolymarketMetadataRowKind::MarketStateTransition;
    active.timestamp_received = 1_789_170_900_013;
    active.collector_admission_time_utc_ms = 1_789_170_900_014;
    active.collector_admission_sequence = 13_509;
    active.metadata_origin = PolymarketMetadataOrigin::LocalMarketStateTimer;
    active.source_endpoint = None;
    active.source_endpoint_unavailable_reason =
        Some(MetadataSourceEndpointUnavailableReason::NotApplicableNoSourceEvidence);
    active.source_evidence_origin = None;
    active.source_evidence_event_type = None;
    active.source_evidence_channel = None;
    active.source_evidence_received_at_utc = None;
    active.row_evidence_validation_completed_at_utc = None;
    active.source_evidence_payload_json = None;
    active.metadata_fetch_start_utc = None;
    active.closed = None;
    active.active = None;
    active.active_unavailable_reason =
        Some(MetadataActiveUnavailableReason::NotApplicableForRowKind);
    active.accepting_orders = None;
    active.outcomes_raw = None;
    active.outcome_prices_raw = None;
    active.response_fields_unavailable_reason = None;
    active.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    active.market_lifecycle_transition =
        Some(MetadataMarketLifecycleTransition::PrefetchedToActive);
    active.lifecycle_scheduled_at_utc = Some(market.window.start_at_utc_ms);
    active.lifecycle_applied_at_utc = Some(active.timestamp_received);
    active.market_runtime_role = MetadataMarketRuntimeRole::Current;
    active.market_runtime_role_transition =
        Some(MetadataMarketRuntimeRoleTransition::NextToCurrent);
    active.runtime_role_scheduled_at_utc = Some(market.window.start_at_utc_ms);
    active.runtime_role_applied_at_utc = Some(active.timestamp_received);
    let mut terminal_metadata = terminal_metadata_row(&market);
    terminal_metadata.asset_ids = metadata.asset_ids.clone();
    terminal_metadata.timestamp_received = RECEIVED;
    terminal_metadata.collector_admission_time_utc_ms = RECEIVED;
    terminal_metadata.source_evidence_received_at_utc = Some(RECEIVED);
    terminal_metadata.row_evidence_validation_completed_at_utc = Some(RECEIVED);
    terminal_metadata.lifecycle_applied_at_utc = Some(RECEIVED);
    terminal_metadata.runtime_role_applied_at_utc = Some(RECEIVED);
    terminal_metadata.ws_platform_timestamp_raw = Some(SOURCE.to_string());
    terminal_metadata.ws_platform_timestamp_utc = Some(SOURCE);
    terminal_metadata.winning_asset_id = Some(UP.into());
    terminal_metadata.ws_claimant_source_sequence_epoch_id = Some(EPOCH.into());
    terminal_metadata.ws_claimant_collector_admission_sequence = Some(SEQUENCE);
    let mut ws = terminal_orderbook_row(&market);
    ws.timestamp_received = RECEIVED;
    ws.timestamp = Some(SOURCE);
    ws.collector_admission_time_utc_ms = RECEIVED;
    ws.source_sequence_epoch_id = EPOCH.into();
    ws.collector_admission_sequence = SEQUENCE;
    if let PolymarketOrderbookEvent::MarketResolved {
        asset_ids,
        winning_asset_id,
        ..
    } = &mut ws.event
    {
        *asset_ids = vec![UP.into(), DOWN.into()];
        *winning_asset_id = UP.into();
    }
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    for input in [
        record(
            0,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(metadata))),
        ),
        record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(active))),
        ),
        record(
            2,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
                terminal_metadata,
            ))),
        ),
        record(
            3,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(ws)),
        ),
        end(4),
    ] {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    let context = core.pm_context(CONDITION).unwrap();
    let scenario = sealed_scenario_for_markets(vec![
        crate::scenario::PlatformMarketV1::try_new(
            context.market,
            context
                .assets
                .into_iter()
                .map(|asset| asset.asset)
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        )
        .unwrap(),
    ]);
    let binding = fixture_day_binding(&core, "midnight-terminal", "midnight-terminal");
    let (execution, delivered) = public_execution_with_scenario_capacity_layout(
        &temp.path().join("sim.jsonl"),
        scenario,
        SimCallerRoleV1::HistoricalBacktest,
        32,
        None,
        day_log.then(|| binding.clone()),
        finite_historical,
    )
    .await;
    let hidden = prepare_hidden_v1(
        core,
        PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap(),
        &temp.path().join("hidden"),
        &config(),
        "midnight-terminal".into(),
    )
    .unwrap();
    let mut recorded = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    // 零额外宿主延迟；不把实际来源可见时刻映射到旧小数值夹具时间。
    let source = std::iter::from_fn(move || recorded.next_visible_group());
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "midnight-terminal".into());
    driver.enable_finite_recording_wait().unwrap();
    driver.next_group().unwrap().unwrap(); // 先登记身份，未提供任何初始 L2。
    let account_sequence = driver
        .execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap()
        .execution_state
        .next_account_transition_seq();
    let mut buckets = RecordedHiddenBucketSourceV1::open(&hidden, &root, 1, 1 << 20).unwrap();
    let mut terminal_count = 0;
    while let Some(bucket) = buckets.next_bucket().unwrap() {
        let bucket = if day_log {
            fixture_day_bucket(bucket, &binding)
        } else {
            bucket
        };
        let at = bucket.reference.bucket_time_utc_ms();
        while driver
            .next_group_at()
            .unwrap()
            .is_some_and(|next| next < at)
        {
            driver.next_group().unwrap().unwrap();
        }
        for record in bucket.records.iter() {
            if let HiddenExecutionRecordV1::MarketFact(fact) = record
                && matches!(fact.payload, CanonicalMarketFactPayloadV1::Terminal(_))
            {
                terminal_count += 1;
                assert_eq!(at, SOURCE);
                assert_eq!(fact.dependency_time.source_event_at_utc_ms, Some(SOURCE));
            }
        }
        driver.hidden_bucket(bucket).unwrap();
    }
    while driver.next_group().unwrap().is_some() {}
    assert_eq!(terminal_count, 1, "配对后的终态只归约一次");
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
        session.execution_state.next_account_transition_seq(),
        account_sequence
    );
    assert!(
        session
            .execution_state
            .redeemed_market_resolution(CONDITION)
            .is_some()
    );
    for state in [
        &session.public_markets,
        session.hidden_platform_markets.as_ref().unwrap(),
    ] {
        assert_eq!(state.external_level_count_for_tests(CONDITION), 0);
        assert_eq!(
            state.validate_input_complete(),
            Err(crate::market::MarketEventErrorV1::IncompleteInitialLoad(
                vec![CONDITION.into()]
            ))
        );
    }
    let feedback = delivered.settlements.borrow();
    let redemptions = feedback
        .iter()
        .filter_map(|feedback| match &feedback.event {
            SettlementLifecycleEventV2::AutoRedeemApplied {
                condition_id,
                resolution_source_event_at_utc_ms,
                redeemed_outcome_balances,
                redeemed_pusd,
                account_delta,
                ..
            } => Some((
                condition_id,
                resolution_source_event_at_utc_ms,
                redeemed_outcome_balances,
                redeemed_pusd,
                account_delta,
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [(condition, at, balances, amount, delta)] = redemptions.as_slice() else {
        panic!("零余额自动赎回反馈必须恰好一次: {redemptions:?}");
    };
    assert_eq!(condition.0, CONDITION);
    assert_eq!(**at, SOURCE);
    assert_eq!(amount.0, "0");
    assert!(delta.is_none());
    assert_eq!(balances.len(), 2);
    assert!(
        balances
            .iter()
            .all(|balance| balance.balance_shares.0 == "0")
    );
    drop(feedback);
    if finite_historical {
        driver.stop_and_drain().unwrap();
    } else {
        driver.complete().unwrap();
    }
    fixture_log_counts(&temp.path().join("sim.jsonl"), &root, &hidden);
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
        times.push(bucket.reference.bucket_time_utc_ms());
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
        let at = bucket.reference.bucket_time_utc_ms();
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
    driver.complete().unwrap();
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
    let closure = TrustedPublicTimeClosureV1 {
        execution_time: driver.time(22_000),
        consumption_sequence_ref: driver.consumption_sequence_ref.clone(),
        next_group_position: 5,
        closure: PublicTimeClosureReasonV1::NextGlobalEventAt(TERMINAL_ADMITTED_TIME_MS),
    };
    driver.execution.close_public_time(closure).unwrap();
    driver.close_pending = false;
    let required = driver
        .execution
        .actor
        .next_required_logical_drive()
        .unwrap();
    let closure = TrustedPublicTimeClosureV1 {
        execution_time: driver.time(required.required_at_utc_ms),
        consumption_sequence_ref: driver.consumption_sequence_ref.clone(),
        next_group_position: 5,
        closure: PublicTimeClosureReasonV1::NextGlobalEventAt(TERMINAL_ADMITTED_TIME_MS),
    };
    driver.execution.close_public_time(closure).unwrap();
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
async fn hidden_trade_and_bba_before_initial_book_have_no_execution_effect() {
    use data_pipeline_core::pm_reduction::PmStepChangeV1;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let trade = |price: &str, size: &str, side| PolymarketOrderbookEvent::LastTradePrice {
        asset_id: DOWN_TOKEN_ID.into(),
        price: price.into(),
        size: Some(size.into()),
        side: Some(side),
        fee_rate_bps: Some("0".into()),
    };
    let mut down = book("100");
    if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut down {
        *asset_id = DOWN_TOKEN_ID.into();
    }
    // 真实失败顺序：新 epoch 的两笔成交先于初始 book；BBA 也不是 L2 初始化。
    for input in [
        meta(0),
        pm(1, 1, 21_000, trade("0.6", "1", PolymarketSide::Sell)),
        pm(2, 2, 21_000, trade("0.63", "11", PolymarketSide::Buy)),
        pm(
            3,
            3,
            21_001,
            PolymarketOrderbookEvent::BestBidAsk {
                asset_id: DOWN_TOKEN_ID.into(),
                best_bid: Some("0.5".into()),
                best_ask: Some("0.6".into()),
                spread: Some("0.1".into()),
            },
        ),
        pm(4, 4, 21_002, book("100")),
        pm(5, 5, 21_002, down),
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
        "hidden-before-book".into(),
    )
    .unwrap();
    let session = execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap();
    let balances = session.execution_state.account_balances_for_tests();
    let sequence = session.execution_state.next_account_transition_seq();
    let fill_shape = session.execution_state.fill_shape_for_tests();
    let mut source = RecordedHiddenBucketSourceV1::open(&hidden, &root, 1, 1 << 20).unwrap();
    let mut changes = Vec::new();
    while let Some(bucket) = source.next_bucket().unwrap() {
        let at = bucket.reference.bucket_time_utc_ms();
        if at > 21_002 {
            break;
        }
        for record in bucket.records.iter() {
            if let HiddenExecutionRecordV1::Step { step, .. } = record {
                match step.change {
                    PmStepChangeV1::Epoch { .. } => changes.push("epoch"),
                    PmStepChangeV1::ResetAsset { .. } => changes.push("reset"),
                    PmStepChangeV1::PublicTrade { .. } => changes.push("trade"),
                    PmStepChangeV1::BestBidAsk { .. } => changes.push("bba"),
                    PmStepChangeV1::InitialSnapshot { .. } => changes.push("book"),
                    _ => {}
                }
            }
        }
        execution
            .hidden_bucket(bucket)
            .expect("观察消费不能依赖尚未到来的 L2");
        let session = execution
            .actor
            .state
            .as_ref()
            .unwrap()
            .session
            .as_ref()
            .unwrap();
        let markets = session.hidden_platform_markets.as_ref().unwrap();
        assert_eq!(
            session.execution_state.account_balances_for_tests(),
            balances
        );
        assert_eq!(
            session.execution_state.next_account_transition_seq(),
            sequence
        );
        assert_eq!(session.execution_state.fill_shape_for_tests(), fill_shape);
        if at < 21_002 {
            assert_eq!(markets.external_level_count_for_tests(CONDITION_ID), 0);
            assert_eq!(
                markets.buy_admission(CONDITION_ID, DOWN_TOKEN_ID, at, MARKET_END_AT),
                crate::market::MarketBuyAdmissionV2::EvidenceUnknownNoEffect(
                    crate::market::MarketBuyEvidenceUnknownV2::Preloading
                )
            );
            for token in [UP_TOKEN_ID, DOWN_TOKEN_ID] {
                for (side, price) in [
                    (BookSide::Bid, Decimal::new(5, 1)),
                    (BookSide::Ask, Decimal::new(6, 1)),
                ] {
                    assert_eq!(
                        session.execution_state.external_queue_base_units_for_tests(
                            CONDITION_ID,
                            token,
                            side,
                            price
                        ),
                        0
                    );
                }
            }
        } else {
            assert_eq!(at, 21_002);
            assert_eq!(
                markets.lifecycle_phase_for_tests(CONDITION_ID),
                Some(crate::market::MarketLifecyclePhaseV1::Active)
            );
            assert_eq!(
                markets.pm_quantity(
                    CONDITION_ID,
                    DOWN_TOKEN_ID,
                    BookSide::Bid,
                    Decimal::new(5, 1)
                ),
                Decimal::from(100)
            );
            assert_eq!(
                session.execution_state.external_queue_base_units_for_tests(
                    CONDITION_ID,
                    DOWN_TOKEN_ID,
                    BookSide::Bid,
                    Decimal::new(5, 1)
                ),
                100_000_000
            );
        }
    }
    assert_eq!(
        changes,
        [
            "epoch", "reset", "reset", "trade", "trade", "bba", "book", "book"
        ]
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
    assert_eq!(first.reference.bucket_time_utc_ms(), 20_050);
    execution.hidden_bucket(first).unwrap();
    let recovery = source.next_bucket().unwrap().unwrap();
    assert_eq!(recovery.reference.bucket_time_utc_ms(), 20_100);
    execution.hidden_bucket(recovery).unwrap();
    let late = source.next_bucket().unwrap().unwrap();
    assert_eq!(late.reference.bucket_time_utc_ms(), 20_200);
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
    assert_eq!(bucket.reference.bucket_time_utc_ms(), 21_000);
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
            .bucket_time_utc_ms(),
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

// 与两个真实有限例程共用供料策略；不向生产 API 暴露后台测试屏障。
pub(super) mod finite_feeding {
    use crate as sim_system;
    include!("../examples/public_finite_acceptance/feeding.rs");
}

#[tokio::test(flavor = "current_thread")]
async fn finite_public_feed_continues_after_closed_time_and_due_action_without_reapplying() {
    struct Times;
    impl PublicConsumptionTimeSourceV1 for Times {
        fn execution_time_for_group(
            &mut self,
            group: &PublicGroupReferenceV1,
        ) -> Result<i64, SimRunActorErrorV2> {
            Ok(match group.group_position {
                0 => 10_000,
                1 => 21_000,
                2..=4 => 22_000,
                _ => TERMINAL_ADMITTED_TIME_MS,
            })
        }
    }
    let rules = successful_snapshot_v3_for_sim_tests().await;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_complete(&root);
    let mut reference = None;
    let mut prior_steps = 0;
    for (blocked, automatic) in [(false, false), (true, false), (true, true)] {
        let path = temp
            .path()
            .join(format!("public-{blocked}-{automatic}.jsonl"));
        let (execution, delivered) = public_execution_capacity(
            &path,
            &core,
            SimCallerRoleV1::Paper,
            if blocked {
                prior_steps as usize - 1
            } else {
                32
            },
            Some(rules.clone()),
        )
        .await;
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, Times).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "finite-times".into());
        let mut outputs = Vec::new();
        for _ in 0..2 {
            let value = finite_feeding::feed(
                &mut driver,
                FinitePublicConsumptionV1::next_group,
                FinitePublicConsumptionV1::wait_for_recording_capacity,
            )
            .unwrap()
            .unwrap();
            outputs.push(serde_json::json!([value, driver.frames()]));
        }
        finite_feeding::feed(
            &mut driver,
            |driver| {
                driver
                    .decision()
                    .unwrap()
                    .request(BusinessRequestV2::CancelAllOrders(
                        CancelAllOrdersRequestV2 {
                            context: RequestContextV2 {
                                global_request_id: "finite-due".into(),
                                caller_correlation_id: None,
                            },
                        },
                    ))
            },
            FinitePublicConsumptionV1::wait_for_recording_capacity,
        )
        .unwrap();
        let due = driver
            .execution
            .actor
            .next_required_logical_drive()
            .unwrap();
        assert!((21_001..22_000).contains(&due.required_at_utc_ms));
        driver.wait_for_recording_capacity().unwrap();
        let mut release = blocked.then(|| {
            driver
                .execution
                .actor
                .background_log
                .as_ref()
                .unwrap()
                .pause_for_tests(false)
        });
        let automatic_release = if automatic {
            driver.enable_finite_recording_wait().unwrap();
            let (notice, entered) = std::sync::mpsc::sync_channel(1);
            driver
                .execution
                .actor
                .background_log
                .as_mut()
                .unwrap()
                .notify_finite_wait_for_tests(notice);
            let release = release.take().unwrap();
            Some(std::thread::spawn(move || {
                entered.recv().unwrap();
                release.send(()).unwrap();
            }))
        } else {
            None
        };
        let before = driver.execution.actor.applied_input_sequence;
        let drive_before = driver.drive_position;
        let mut waits = 0;
        let value = finite_feeding::feed(
            &mut driver,
            FinitePublicConsumptionV1::next_group,
            |driver| {
                waits += 1;
                assert_eq!(
                    driver.execution.actor.applied_input_sequence,
                    before + prior_steps
                );
                assert_eq!(driver.drive_position, drive_before + prior_steps);
                assert!(!driver.close_pending);
                assert_eq!(
                    driver
                        .source
                        .peek()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .prepared
                        .reference
                        .group_position,
                    2
                );
                assert_eq!(
                    driver.last_receipt.as_ref().unwrap().group.group_position,
                    1
                );
                assert!(
                    driver
                        .execution
                        .actor
                        .next_required_logical_drive()
                        .is_none_or(|required| required.required_at_utc_ms >= 22_000)
                );
                release.take().unwrap().send(()).unwrap();
                driver.wait_for_recording_capacity()
            },
        )
        .unwrap()
        .unwrap();
        if let Some(worker) = automatic_release {
            worker.join().unwrap();
        }
        assert_eq!(waits, usize::from(blocked && !automatic));
        if !blocked {
            prior_steps = driver.execution.actor.applied_input_sequence - before - 1;
        }
        outputs.push(serde_json::json!([value, driver.frames()]));
        while let Some(value) = finite_feeding::feed(
            &mut driver,
            FinitePublicConsumptionV1::next_group,
            FinitePublicConsumptionV1::wait_for_recording_capacity,
        )
        .unwrap()
        {
            outputs.push(serde_json::json!([value, driver.frames()]));
        }
        assert_eq!(outputs.len(), 6);
        let actual = serde_json::json!({"outputs":outputs, "sequence":driver.execution.actor.applied_input_sequence,
            "drive_position":driver.drive_position,"responses":*delivered.responses.borrow()});
        if let Some(expected) = &reference {
            assert_eq!(&actual, expected);
        } else {
            reference = Some(actual);
        }
        driver.complete().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn finite_hidden_feed_resumes_after_due_actions_and_applies_each_bucket_once() {
    let rules = successful_snapshot_v3_for_sim_tests().await;
    let temp = tempfile::tempdir().unwrap();
    let mut reference = None;
    let mut prior_steps = 0;
    for blocked in [false, true] {
        let root = temp.path().join(format!("dpt-{blocked}"));
        let core = recorded_complete(&root);
        let path = temp.path().join(format!("hidden-{blocked}.jsonl"));
        let (execution, delivered) = public_execution_capacity(
            &path,
            &core,
            SimCallerRoleV1::ModelTrain,
            if blocked {
                prior_steps as usize - 1
            } else {
                32
            },
            Some(rules.clone()),
        )
        .await;
        let hidden = prepare_hidden_v1(
            core,
            PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap(),
            &temp.path().join(format!("material-{blocked}")),
            &config(),
            "hidden-test".into(),
        )
        .unwrap();
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "finite-times".into());
        finite_feeding::feed(
            &mut driver,
            FinitePublicConsumptionV1::next_group,
            FinitePublicConsumptionV1::wait_for_recording_capacity,
        )
        .unwrap()
        .unwrap();
        finite_feeding::feed(
            &mut driver,
            |driver| {
                driver
                    .decision()
                    .unwrap()
                    .request(BusinessRequestV2::CancelAllOrders(
                        CancelAllOrdersRequestV2 {
                            context: RequestContextV2 {
                                global_request_id: "hidden-due".into(),
                                caller_correlation_id: None,
                            },
                        },
                    ))
            },
            FinitePublicConsumptionV1::wait_for_recording_capacity,
        )
        .unwrap();
        assert!(
            driver
                .execution
                .actor
                .next_required_logical_drive()
                .unwrap()
                .required_at_utc_ms
                < 21_000
        );
        driver.wait_for_recording_capacity().unwrap();
        let mut release = blocked.then(|| {
            driver
                .execution
                .actor
                .background_log
                .as_ref()
                .unwrap()
                .pause_for_tests(false)
        });
        let before = driver.execution.actor.applied_input_sequence;
        let mut buckets = RecordedHiddenBucketSourceV1::open(&hidden, &root, 1, 1 << 20).unwrap();
        let mut outputs = Vec::new();
        let mut waits = 0;
        while let Some(bucket) = buckets.next_bucket().unwrap() {
            finite_feeding::feed(
                &mut driver,
                |driver| driver.hidden_bucket(bucket.clone()),
                |driver| {
                    if let Some(release) = release.take() {
                        waits += 1;
                        assert_eq!(
                            driver.execution.actor.applied_input_sequence,
                            before + prior_steps
                        );
                        let session = driver
                            .execution
                            .actor
                            .state
                            .as_ref()
                            .unwrap()
                            .session
                            .as_ref()
                            .unwrap();
                        assert_eq!(session.hidden_platform_frontier.next_index_position, 0);
                        assert!(
                            driver
                                .execution
                                .actor
                                .next_required_logical_drive()
                                .is_none_or(|required| required.required_at_utc_ms
                                    >= bucket.reference.bucket_time_utc_ms())
                        );
                        release.send(()).unwrap();
                    }
                    driver.wait_for_recording_capacity()
                },
            )
            .unwrap();
            if !blocked && outputs.is_empty() {
                prior_steps = driver.execution.actor.applied_input_sequence - before - 1;
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
            if bucket.reference.bucket_time_utc_ms() == 21_001 {
                assert_eq!(
                    session
                        .hidden_platform_markets
                        .as_ref()
                        .unwrap()
                        .pm_quantity(CONDITION_ID, UP_TOKEN_ID, BookSide::Bid, Decimal::new(5, 1)),
                    Decimal::from(80)
                );
            }
            outputs.push(serde_json::json!({"bucket":bucket.reference(), "records":bucket.records(),
                "frontier":[session.hidden_platform_frontier.closed_through_utc_ms, session.hidden_platform_frontier.next_index_position],"sequence":driver.execution.actor.applied_input_sequence}));
        }
        assert_eq!(outputs.len(), 3);
        assert_eq!(waits, usize::from(blocked));
        let actual =
            serde_json::json!({"outputs":outputs,"responses":*delivered.responses.borrow()});
        if let Some(expected) = &reference {
            assert_eq!(&actual, expected);
        } else {
            reference = Some(actual);
        }
        driver.wait_for_recording_capacity().unwrap();
        let background = driver.execution.actor.background_log.take().unwrap();
        assert!(matches!(
            background.abort_and_join_for_tests(),
            Err(crate::event_log::SimEventLogErrorV2::UnsealedCleanRun)
        ));
        assert!(!fs::read_to_string(path).unwrap().contains("RUN_SEAL"));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn finite_feed_wait_propagates_original_background_io_and_stops_without_seal() {
    for automatic in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let core = recorded_books(&root);
        let path = temp.path().join("sim.jsonl");
        let (mut execution, _) =
            public_execution_capacity(&path, &core, SimCallerRoleV1::Paper, 0, None).await;
        execution.wait_for_recording_capacity().unwrap();
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "finite-times".into());
        let first = finite_feeding::feed(
            &mut driver,
            FinitePublicConsumptionV1::next_group,
            FinitePublicConsumptionV1::wait_for_recording_capacity,
        )
        .unwrap()
        .unwrap();
        driver.wait_for_recording_capacity().unwrap();
        let (entered, release) = driver
            .execution
            .actor
            .background_log
            .as_ref()
            .unwrap()
            .pause_write_for_tests();
        // 先让真实上一组闭合占住后台写屏障，再测空档闭合后的原背压错误传播。
        driver
            .state
            .close_at(10_000, PublicTimeClosureReasonV1::NextGlobalEventAt(22_000))
            .unwrap();
        driver.wait_for_recording_capacity().unwrap();
        entered.recv().unwrap();
        let mut release = Some(release);
        let automatic_release = if automatic {
            driver.enable_finite_recording_wait().unwrap();
            let (notice, waiting) = std::sync::mpsc::sync_channel(1);
            driver
                .execution
                .actor
                .background_log
                .as_mut()
                .unwrap()
                .notify_finite_wait_for_tests(notice);
            let release = release.take().unwrap();
            Some(std::thread::spawn(move || {
                waiting.recv().unwrap();
                release.send(()).unwrap();
            }))
        } else {
            None
        };
        let before = driver.execution.actor.applied_input_sequence;
        let mut waits = 0;
        let mut outputs = Vec::new();
        let result = finite_feeding::feed(
            &mut driver,
            FinitePublicConsumptionV1::next_group,
            |driver| {
                waits += 1;
                assert_eq!(driver.execution.actor.applied_input_sequence, before + 1);
                assert_eq!(
                    driver
                        .source
                        .peek()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .prepared
                        .reference
                        .group_position,
                    1
                );
                release.take().unwrap().send(()).unwrap();
                driver.wait_for_recording_capacity()
            },
        );
        if let Ok(Some(value)) = &result {
            outputs.push(value.clone());
        }
        let Err(SimRunActorErrorV2::Log(error @ crate::event_log::SimEventLogErrorV2::Io { .. })) =
            result
        else {
            panic!("必须传播真实后台写错误")
        };
        if let Some(worker) = automatic_release {
            worker.join().unwrap();
        }
        assert_eq!(waits, usize::from(!automatic));
        assert!(outputs.is_empty());
        assert_eq!(driver.last_receipt.as_ref(), Some(&first.0));
        assert_eq!(driver.execution.actor.applied_input_sequence, before + 1);
        let background = driver.execution.actor.background_log.take().unwrap();
        assert_eq!(background.abort_and_join_for_tests().unwrap_err(), error);
        assert!(!fs::read_to_string(&path).unwrap().contains("RUN_SEAL"));
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
}

#[tokio::test(flavor = "current_thread")]
async fn finite_session_feed_does_not_wait_on_a_real_business_error() {
    let temp = tempfile::tempdir().unwrap();
    let core = recorded_books(&temp.path().join("dpt"));
    let (mut execution, _) = public_execution(
        &temp.path().join("sim.jsonl"),
        &core,
        SimCallerRoleV1::Paper,
    )
    .await;
    execution.wait_for_recording_capacity().unwrap();
    let before = execution.actor.applied_input_sequence;
    let error = finite_feeding::feed(
        &mut execution,
        |execution| execution.create_session(spec("public-consumption-test", "another-creation")),
        |_| panic!("业务错误不得进入日志等待"),
    )
    .unwrap_err();
    assert!(matches!(error, SimRunActorErrorV2::SessionAlreadyCreated));
    assert_eq!(execution.actor.applied_input_sequence, before);
}

mod memory_noe_tests {
    use super::*;
    include!("memory_noe_tests.rs");
}

#[cfg(feature = "measurement")]
#[tokio::test(flavor = "current_thread")]
async fn public_pm_evidence_counts_follow_delete_and_epoch_reset() {
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
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("0"))],
            },
        ),
        record(
            3,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-b",
                1,
                21_002,
                22_000,
                PolymarketOrderbookEvent::LastTradePrice {
                    asset_id: UP_TOKEN_ID.into(),
                    price: "0.55".into(),
                    size: None,
                    side: None,
                    fee_rate_bps: None,
                },
            ))),
        ),
        end(4),
    ] {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    let (execution, _) = public_execution(
        &temp.path().join("sim.jsonl"),
        &core,
        SimCallerRoleV1::Paper,
    )
    .await;
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "evidence-lifecycle".into());
    driver.next_group().unwrap().unwrap();
    let mut observed = Vec::new();
    for expected_quantity in [100, 0, 0] {
        driver.next_group().unwrap().unwrap();
        let resources = driver.measurement_resources();
        assert_eq!(
            resources.business,
            driver
                .execution
                .actor
                .measurement_resource_snapshot()
                .business
        );
        assert_eq!(
            resources.public_pm_evidence,
            driver.measurement_resources().public_pm_evidence
        );
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
            Decimal::from(expected_quantity)
        );
        observed.push(resources.public_pm_evidence);
    }
    assert_eq!(
        observed,
        vec![
            PmEvidenceObjectCountsV1 {
                markets: 1,
                assets: 2,
                levels: 2,
                absences: 0,
                selected: 2
            },
            PmEvidenceObjectCountsV1 {
                markets: 1,
                assets: 2,
                levels: 1,
                absences: 1,
                selected: 2
            },
            PmEvidenceObjectCountsV1 {
                markets: 1,
                assets: 2,
                levels: 0,
                absences: 0,
                selected: 0
            },
        ]
    );
}

fn controlled_continuous(
    execution: PublicSimExecutionV1,
    at: i64,
) -> (
    ContinuousPublicConsumptionV1,
    Arc<std::sync::atomic::AtomicI64>,
) {
    let clock = Arc::new(std::sync::atomic::AtomicI64::new(at));
    let mut driver = ContinuousPublicConsumptionV1::new(execution, "online-test".into());
    driver.clock.controlled = Some(Arc::clone(&clock));
    (driver, clock)
}

#[tokio::test(flavor = "current_thread")]
async fn continuous_same_millisecond_groups_precede_buy_and_idle_never_observes_public_time() {
    use std::sync::atomic::Ordering;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let mut active = metadata_snapshot_row();
    active.timestamp_received = 20_000;
    active.collector_admission_sequence = 2;
    active.collector_admission_time_utc_ms = 20_001;
    active.metadata_fetch_start_utc = Some(19_990);
    active.source_evidence_received_at_utc = Some(20_000);
    active.row_evidence_validation_completed_at_utc = Some(20_001);
    active.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    active.market_runtime_role = MetadataMarketRuntimeRole::Current;
    let mut down = book("100");
    if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut down {
        *asset_id = DOWN_TOKEN_ID.into();
    }
    let mut removal = book("100");
    if let PolymarketOrderbookEvent::Book { asks, .. } = &mut removal {
        asks.clear();
    }
    for input in [
        meta(0),
        record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(active))),
        ),
        pm(2, 1, 21_000, down),
        pm(3, 2, 21_000, book("100")),
        pm(4, 3, 21_000, removal),
        end(5),
    ] {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    let path = temp.path().join("sim.jsonl");
    let (execution, _) = public_execution(&path, &core, SimCallerRoleV1::Paper).await;
    let (mut driver, clock) = controlled_continuous(execution, 10_000);
    let mut source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    driver
        .apply_group(source.next().unwrap().unwrap().prepared)
        .unwrap();
    clock.store(20_001, Ordering::SeqCst);
    driver
        .apply_group(source.next().unwrap().unwrap().prepared)
        .unwrap();
    clock.store(22_000, Ordering::SeqCst);
    driver
        .apply_group(source.next().unwrap().unwrap().prepared)
        .unwrap();
    let first = source.next().unwrap().unwrap().prepared;
    let before = serde_json::to_value(&first.group).unwrap();
    driver.apply_group(Arc::clone(&first)).unwrap();
    driver
        .decision()
        .unwrap()
        .request(BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "online-buy".into(),
                caller_correlation_id: None,
            },
            item: post_item(
                "buy",
                &ConditionId(CONDITION_ID.into()),
                &TokenId(UP_TOKEN_ID.into()),
                SimBuyOrderKindV1::FokMarket {
                    spend_pusd: Decimal::from(3),
                    worst_price_pusd_per_share: Decimal::new(6, 1),
                },
            ),
        }))
        .unwrap();
    let second = source.next().unwrap().unwrap().prepared;
    let receipt = driver.apply_group(second).unwrap().0;
    assert_eq!(receipt.execution_time().logical_time_utc_ms(), 22_000);
    assert_eq!(serde_json::to_value(&first.group).unwrap(), before);
    let due = driver.required_at().unwrap();
    assert!(due >= 22_000);
    clock.store(due, Ordering::SeqCst);
    driver.advance_due().unwrap();
    assert!(completed_post_response(&driver.execution.actor, "online-buy").is_none());
    assert_eq!(
        driver.next_wake_after(),
        Some(std::time::Duration::from_millis(1))
    );
    clock.store(due + 1, Ordering::SeqCst);
    driver.advance_due().unwrap();
    let response = completed_post_response(&driver.execution.actor, "online-buy").unwrap();
    assert_eq!(response.metadata.platform_responded_at_utc_ms, due);
    assert_eq!(
        response.item.outcome,
        PostOrderItemOutcomeV2::RejectedNoEffect(SubmissionRejectedReasonV2::FokNotFullyFilled)
    );
    assert_eq!(driver.last_receipt.as_ref().unwrap(), &receipt);
    assert_eq!(driver.frames()[0].receipt, receipt);
    assert_eq!(
        driver
            .decision()
            .unwrap()
            .cutoff()
            .private_feedback_sequence(),
        0
    );
    driver.stop_and_drain().unwrap();
    let log = std::fs::read_to_string(path).unwrap();
    assert!(log.contains("ExecutionClockAdvancedTo"));
    assert!(!log.contains("RunSeal"));
    assert!(!log.contains("EndOfInput"));
}

#[tokio::test(flavor = "current_thread")]
async fn continuous_actual_clock_splits_old_source_times_and_drains_earlier_due_before_group() {
    use std::sync::atomic::Ordering;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_complete(&root);
    let (execution, delivered) = public_execution(
        &temp.path().join("sim.jsonl"),
        &core,
        SimCallerRoleV1::Paper,
    )
    .await;
    let (mut driver, clock) = controlled_continuous(execution, 10_000);
    // 初始市场已经携带窗口结束 30000 的原返佣时钟，不能按“无请求”等同无义务。
    let execution_state = &driver
        .execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap()
        .execution_state;
    assert_eq!(
        execution_state.next_rebate_clock_at_utc_ms(),
        Some(MARKET_END_AT)
    );
    assert_eq!(
        execution_state
            .next_gtd_deadline_at_utc_ms(driver.execution.actor._rule_snapshot.snapshot()),
        None
    );
    assert_eq!(execution_state.next_scheduled_finality_at_utc_ms(), None);
    assert_eq!(
        driver.next_wake_after(),
        Some(std::time::Duration::from_millis(
            (MARKET_END_AT + 1 - 10_000) as u64
        ))
    );
    let mut source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    driver
        .apply_group(source.next().unwrap().unwrap().prepared)
        .unwrap();
    clock.store(20_001, Ordering::SeqCst);
    driver
        .apply_group(source.next().unwrap().unwrap().prepared)
        .unwrap();
    clock.store(22_000, Ordering::SeqCst);
    let first = source.next().unwrap().unwrap().prepared;
    let first_original = serde_json::to_value(&first.group).unwrap();
    let first_receipt = driver.apply_group(Arc::clone(&first)).unwrap().0;
    driver
        .decision()
        .unwrap()
        .request(BusinessRequestV2::CancelAllOrders(
            CancelAllOrdersRequestV2 {
                context: RequestContextV2 {
                    global_request_id: "clock-earlier-due".into(),
                    caller_correlation_id: None,
                },
            },
        ))
        .unwrap();
    let due = driver.required_at().unwrap();
    assert!(due >= 22_000);
    let second = source.next().unwrap().unwrap().prepared;
    let second_original = serde_json::to_value(&second.group).unwrap();
    clock.store(due + 10, Ordering::SeqCst);
    let second_receipt = driver.apply_group(Arc::clone(&second)).unwrap().0;
    assert_eq!(first_receipt.execution_time().logical_time_utc_ms(), 22_000);
    assert_eq!(
        second_receipt.execution_time().logical_time_utc_ms(),
        due + 10
    );
    assert_eq!(delivered.responses.borrow().len(), 1);
    assert_eq!(
        delivered.responses.borrow()[0].platform_responded_at_utc_ms,
        due
    );
    assert_eq!(serde_json::to_value(&first.group).unwrap(), first_original);
    assert_eq!(
        serde_json::to_value(&second.group).unwrap(),
        second_original
    );
    assert_eq!(
        driver.last_prepared.as_ref().unwrap().reference,
        second.reference
    );
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
    driver.stop_and_drain().unwrap();
}

#[test]
fn continuous_closure_codec_retains_both_old_finite_shapes() {
    for old in [
        contracts::ReplayIndexClosureV1::NextGlobalEventAt(123),
        contracts::ReplayIndexClosureV1::EndOfInput,
    ] {
        let wire = serde_json::to_value(old).unwrap();
        let decoded: PublicTimeClosureReasonV1 = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), wire);
    }
    let reason = PublicTimeClosureReasonV1::ExecutionClockAdvancedTo(124);
    assert_eq!(
        serde_json::from_str::<PublicTimeClosureReasonV1>(&serde_json::to_string(&reason).unwrap())
            .unwrap(),
        reason
    );
}

#[tokio::test(flavor = "current_thread")]
async fn continuous_capacity_resume_samples_clock_and_never_reapplies_accepted_groups() {
    use std::sync::atomic::Ordering;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_complete(&root);
    let (execution, _) = public_execution_capacity(
        &temp.path().join("sim.jsonl"),
        &core,
        SimCallerRoleV1::Paper,
        1,
        None,
    )
    .await;
    let (mut driver, clock) = controlled_continuous(execution, 22_000);
    driver.wait_for_recording_capacity().unwrap();
    let release = driver
        .execution
        .actor
        .background_log
        .as_ref()
        .unwrap()
        .pause_for_tests(false);
    let mut source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    let initial = driver.execution.actor.applied_input_sequence;
    for position in 0..2 {
        let receipt = driver
            .apply_group(source.next().unwrap().unwrap().prepared)
            .unwrap()
            .0;
        assert_eq!(receipt.group.group_position, position);
    }
    assert!(
        !driver
            .execution
            .actor
            .background_log
            .as_mut()
            .unwrap()
            .ready()
            .unwrap()
    );
    assert_eq!(driver.execution.actor.applied_input_sequence, initial + 2);
    let resume = std::thread::spawn(move || {
        clock.store(23_000, Ordering::SeqCst);
        release.send(()).unwrap();
    });
    let receipt = driver
        .apply_group(source.next().unwrap().unwrap().prepared)
        .unwrap()
        .0;
    resume.join().unwrap();
    assert_eq!(receipt.group.group_position, 2);
    assert_eq!(receipt.execution_time().logical_time_utc_ms(), 23_000);
    assert_eq!(driver.drive_position, 4); // 三个组，旧 22000 毫秒关闭一次。
    assert_eq!(driver.execution.actor.applied_input_sequence, initial + 4);
    driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn continuous_equal_time_finality_gtd_and_rebate_keep_original_pre_market_priority() {
    use std::sync::atomic::Ordering;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let mut metadata = metadata_snapshot_row();
    metadata.market_end_utc = 600_000;
    let initial = record(
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
    let mut down = book("100");
    if let PolymarketOrderbookEvent::Book { asset_id, .. } = &mut down {
        *asset_id = DOWN_TOKEN_ID.into();
    }
    let inputs = [
        initial,
        active,
        pm(2, 1, 21_000, down),
        pm(3, 2, 21_000, book("100")),
        pm(4, 3, 21_000, book("100")),
        pm(5, 4, 21_000, book("100")),
        pm(6, 5, 21_000, book("100")),
        end(7),
    ];
    for input in inputs {
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
    let (mut driver, clock) = controlled_continuous(execution, 22_000);
    let mut source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    for _ in 0..4 {
        driver
            .apply_group(source.next().unwrap().unwrap().prepared)
            .unwrap();
    }
    let deadline = encoded_gtd_expiration_for_lead_for_tests(
        driver.execution.actor.state.as_ref().unwrap(),
        driver.execution.actor._latency_profile.as_ref(),
        "online-gtd",
        22_000,
        240,
    );
    for (id, kind) in [
        (
            "online-gtd",
            SimBuyOrderKindV1::GtdLimit {
                limit_price_pusd_per_share: Decimal::new(2, 1),
                size_shares: Decimal::from(5),
                post_only: true,
                expires_at_utc_ms: deadline,
            },
        ),
        (
            "online-fok",
            SimBuyOrderKindV1::FokMarket {
                spend_pusd: Decimal::from(3),
                worst_price_pusd_per_share: Decimal::new(6, 1),
            },
        ),
    ] {
        driver
            .decision()
            .unwrap()
            .request(BusinessRequestV2::PostOrder(PostOrderRequestV2 {
                context: RequestContextV2 {
                    global_request_id: id.into(),
                    caller_correlation_id: None,
                },
                item: post_item(
                    id,
                    &ConditionId(CONDITION_ID.into()),
                    &TokenId(UP_TOKEN_ID.into()),
                    kind,
                ),
            }))
            .unwrap();
    }
    while completed_post_response(&driver.execution.actor, "online-gtd").is_none()
        || completed_post_response(&driver.execution.actor, "online-fok").is_none()
    {
        let at = driver.required_at().unwrap();
        assert!(at < deadline);
        clock.store(at + 1, Ordering::SeqCst);
        driver.advance_due().unwrap();
    }
    for id in ["online-gtd", "online-fok"] {
        assert!(matches!(
            completed_post_response(&driver.execution.actor, id)
                .unwrap()
                .item
                .outcome,
            PostOrderItemOutcomeV2::Accepted(_)
        ));
    }
    let state = &driver
        .execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap()
        .execution_state;
    assert_eq!(
        state.next_gtd_deadline_at_utc_ms(driver.execution.actor._rule_snapshot.snapshot()),
        Some(deadline)
    );
    let finality_at = state.next_scheduled_finality_at_utc_ms().unwrap();
    for (at, class) in [
        (finality_at, "finality"),
        (deadline, "gtd"),
        (600_000, "rebate"),
    ] {
        clock.store(at, Ordering::SeqCst);
        let receipt = driver
            .apply_group(source.next().unwrap().unwrap().prepared)
            .unwrap()
            .0;
        assert_eq!(receipt.execution_time().logical_time_utc_ms(), at);
        let session = driver
            .execution
            .actor
            .state
            .as_ref()
            .unwrap()
            .session
            .as_ref()
            .unwrap();
        assert!(
            session
                .public_visibility_frontier
                .closed_through_utc_ms
                .is_none_or(|closed| closed < at)
        );
        match class {
            "finality" => assert_ne!(
                session.execution_state.next_scheduled_finality_at_utc_ms(),
                Some(at)
            ),
            "gtd" => assert_eq!(
                session
                    .execution_state
                    .next_gtd_deadline_at_utc_ms(driver.execution.actor._rule_snapshot.snapshot()),
                None
            ),
            _ => assert_ne!(
                session.execution_state.next_rebate_clock_at_utc_ms(),
                Some(at)
            ),
        }
    }
    driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn trusted_observation_keeps_source_prefix_and_sends_at_independent_time() {
    trusted_observation_keeps_source_prefix_and_sends_at_independent_time_layout(false, false)
        .await;
}

async fn trusted_observation_keeps_source_prefix_and_sends_at_independent_time_layout(
    day_log: bool,
    finite_historical: bool,
) {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_books(&root);
    let path = temp.path().join("sim.jsonl");
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    let binding = fixture_day_binding(&core, "time-test", "hidden-test");
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
    let (execution, _) = public_execution_with_scenario_capacity_layout(
        &path,
        scenario,
        if finite_historical {
            SimCallerRoleV1::HistoricalBacktest
        } else {
            SimCallerRoleV1::Paper
        },
        32,
        None,
        day_log.then(|| binding.clone()),
        finite_historical,
    )
    .await;
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "time-test".into());
    for _ in 0..3 {
        driver.next_group().unwrap().unwrap();
    }
    driver
        .decision()
        .unwrap()
        .request(BusinessRequestV2::CancelAllOrders(
            CancelAllOrdersRequestV2 {
                context: RequestContextV2 {
                    global_request_id: "legacy-send".into(),
                    caller_correlation_id: None,
                },
            },
        ))
        .unwrap();
    let frozen = driver.observe_at(22_000).unwrap().unwrap();
    let prepared = Arc::clone(driver.last_prepared.as_ref().unwrap());
    driver.observe_at(22_020).unwrap().unwrap();
    assert!(Arc::ptr_eq(
        &prepared,
        driver.last_prepared.as_ref().unwrap()
    ));
    assert_eq!(frozen.decision_time().logical_time_utc_ms(), 22_000);
    assert_eq!(
        driver.frames()[0]
            .decision_cutoff
            .decision_time()
            .logical_time_utc_ms(),
        22_020
    );
    assert_eq!(
        driver
            .execution
            .actor
            .state
            .as_ref()
            .unwrap()
            .session
            .as_ref()
            .unwrap()
            .public_visibility_frontier
            .closed_through_utc_ms,
        Some(22_019)
    );
    let request = BusinessRequestV2::CancelAllOrders(CancelAllOrdersRequestV2 {
        context: RequestContextV2 {
            global_request_id: "delayed-send".into(),
            caller_correlation_id: None,
        },
    });
    let legacy = BusinessInputV2::PublicRequest {
        cutoff: frozen.clone(),
        send_time: None,
        request: request.clone(),
    };
    let legacy_json = serde_json::to_value(&legacy).unwrap();
    assert!(legacy_json["body"].get("send_time").is_none());
    let legacy_logged: LoggedBusinessInputV2 = serde_json::from_value(legacy_json.clone()).unwrap();
    let old_replayed =
        materialize_logged_input_v2(legacy_logged, &mut |_| unreachable!("请求不读取行情桶"))
            .unwrap();
    assert_eq!(serde_json::to_value(old_replayed).unwrap(), legacy_json);
    let receipt = driver
        .send_decision_at(frozen.clone(), 22_050)
        .unwrap()
        .request(request.clone())
        .unwrap();
    let state = driver
        .execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap();
    let registered = &state.mutation_requests.requests_by_id["delayed-send"];
    assert_eq!(registered.request_sent_at_utc_ms, 22_050);
    assert_eq!(
        registered.platform_arrived_at_utc_ms,
        receipt.platform_arrived_at_utc_ms
    );
    let delay = driver.execution.actor._latency_profile.sample_delay_ms(
        state.session_seed.0.as_bytes(),
        "delayed-send",
        DelaySampleDomainV1::RequestOutbound,
    );
    assert_eq!(receipt.platform_arrived_at_utc_ms, 22_050 + delay as i64);
    assert_eq!(
        state.public_visibility_frontier.closed_through_utc_ms,
        Some(22_049)
    );
    let send_time = driver.last_cutoff.as_ref().unwrap().decision_time.clone();
    let current = BusinessInputV2::PublicRequest {
        cutoff: frozen,
        send_time: Some(send_time),
        request,
    };
    let current_json = serde_json::to_value(current).unwrap();
    let current_logged = serde_json::from_value(current_json.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(
            materialize_logged_input_v2(current_logged, &mut |_| unreachable!("请求不读取行情桶"))
                .unwrap()
        )
        .unwrap(),
        current_json
    );
    let rules = Arc::clone(&driver.execution.actor._rule_snapshot);
    let finality = Arc::clone(&driver.execution.actor._finality_policy);
    let scenario = Arc::clone(&driver.execution.actor.scenario);
    let latency = Arc::clone(&driver.execution.actor._latency_profile);
    let expected = driver
        .execution
        .actor
        .state
        .as_ref()
        .unwrap()
        .session
        .as_ref()
        .unwrap()
        .mutation_requests
        .requests_by_id
        .iter()
        .map(|(id, request)| {
            (
                id.clone(),
                request.request_sent_at_utc_ms,
                request.platform_arrived_at_utc_ms,
            )
        })
        .collect::<Vec<_>>();
    driver.stop_and_drain().unwrap();
    // 原录制读取器恢复公共材料，规范日志恢复观察、关闭和请求，重用生产归约与投影。
    let mut source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    let (opened, mut recovery) = SimEventLogRecoveryV2::open(&path, MAX_GROUP_BYTES).unwrap();
    let mut replay = BusinessReducerStateV2 {
        session: None,
        public_pm_evidence: BTreeMap::new(),
        pm_materials: Vec::new(),
    };
    let mut sequence = 1;
    let mut saw_observation = false;
    if day_log {
        assert_eq!(recovery.historical_day_binding_for_tests(), Some(&binding));
    }
    loop {
        let (next, inputs) = recovery.next_group().unwrap();
        recovery = next;
        let Some(inputs) = inputs else { break };
        for logged in inputs {
            saw_observation |= matches!(logged, LoggedBusinessInputV2::PublicObserveTime(_));
            let input = match logged {
                LoggedBusinessInputV2::DayPublicGroup(group) => {
                    let prepared = source.next().unwrap().unwrap().prepared;
                    assert_eq!(prepared.reference.input_stream_id, binding.input_stream_id);
                    assert_eq!(
                        prepared.reference.applied_prefix.next_input_position,
                        group.next_input_position
                    );
                    assert_eq!(prepared.reference.group_position, group.group_position);
                    assert_eq!(
                        prepared.reference.closed_at_input_position,
                        group.closed_at_input_position
                    );
                    BusinessInputV2::PublicGroup(AcceptedPublicGroupV1 {
                        reference: prepared.reference.clone(),
                        execution_time: TrustedActorExecutionTimeV1(group.execution_time),
                        prepared,
                        pm_materials: Vec::new(),
                        pm_step_bindings: Vec::new(),
                        pm_reports: Vec::new(),
                    })
                }
                LoggedBusinessInputV2::PublicGroup(group) => {
                    let prepared = source.next().unwrap().unwrap().prepared;
                    assert_eq!(prepared.reference, group.reference);
                    BusinessInputV2::PublicGroup(AcceptedPublicGroupV1 {
                        reference: group.reference,
                        execution_time: TrustedActorExecutionTimeV1(group.execution_time),
                        prepared,
                        pm_materials: Vec::new(),
                        pm_step_bindings: group.pm_step_bindings,
                        pm_reports: group.pm_reports,
                    })
                }
                other => materialize_logged_input_v2(other, &mut |_| {
                    unreachable!("公共录制无需历史行情桶")
                })
                .unwrap(),
            };
            let (state, output) = reduce_actor_input_v2(
                &opened,
                scenario.initial_market_directory(),
                replay,
                &input,
                sequence,
                &latency,
                rules.snapshot(),
                &finality,
            )
            .unwrap();
            let (state, _, _) =
                finalize_projection_batch_v2(&opened.run_id, state, output, &latency).unwrap();
            replay = state;
            replay.pm_materials.clear();
            sequence += 1;
        }
    }
    let actual = replay
        .session
        .unwrap()
        .mutation_requests
        .requests_by_id
        .into_iter()
        .map(|(id, request)| {
            (
                id,
                request.request_sent_at_utc_ms,
                request.platform_arrived_at_utc_ms,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert!(saw_observation);
    let log = String::from_utf8_lossy(&std::fs::read(&path).unwrap()).into_owned();
    if !day_log {
        assert!(log.contains("PUBLIC_OBSERVE_TIME"));
    }
    assert!(!log.contains("EndOfInput"));
    assert!(!log.contains("RUN_SEAL"));
}

#[tokio::test(flavor = "current_thread")]
async fn paper_send_samples_clock_at_request_instead_of_capability_acquisition() {
    use std::sync::atomic::Ordering;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let core = recorded_books(&root);
    let (execution, _) = public_execution(
        &temp.path().join("paper.jsonl"),
        &core,
        SimCallerRoleV1::Paper,
    )
    .await;
    let (mut driver, clock) = controlled_continuous(execution, 10_000);
    let mut source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, RecordedTimes).unwrap();
    driver
        .apply_group(source.next().unwrap().unwrap().prepared)
        .unwrap();
    clock.store(22_000, Ordering::SeqCst);
    driver
        .apply_group(source.next().unwrap().unwrap().prepared)
        .unwrap();
    let observed = driver.observe().unwrap().unwrap();
    let mut send = driver.send_decision(observed.clone()).unwrap();
    clock.store(22_050, Ordering::SeqCst);
    let receipt = send
        .request(BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "paper-time".into(),
                caller_correlation_id: None,
            },
            item: post_item(
                "buy",
                &ConditionId(CONDITION_ID.into()),
                &TokenId(UP_TOKEN_ID.into()),
                SimBuyOrderKindV1::FokMarket {
                    spend_pusd: Decimal::from(3),
                    worst_price_pusd_per_share: Decimal::new(6, 1),
                },
            ),
        }))
        .unwrap();
    assert_eq!(send.cutoff(), &observed);
    assert!(receipt.platform_arrived_at_utc_ms >= 22_050);
    drop(send);
    assert_eq!(
        driver
            .execution
            .actor
            .state
            .as_ref()
            .unwrap()
            .session
            .as_ref()
            .unwrap()
            .mutation_requests
            .requests_by_id["paper-time"]
            .request_sent_at_utc_ms,
        22_050
    );
    let due = driver.required_at().unwrap();
    clock.store(due, Ordering::SeqCst);
    driver.observe().unwrap();
    assert!(
        completed_post_response(&driver.execution.actor, "paper-time").is_none(),
        "观察不关闭当前毫秒的 BUY"
    );
    clock.store(due + 1, Ordering::SeqCst);
    driver.observe().unwrap();
    assert!(completed_post_response(&driver.execution.actor, "paper-time").is_some());
    driver.stop_and_drain().unwrap();
}

#[test]
fn shared_day_observation_encoding_preserves_eight_markets_and_lagging_groups() {
    let mut core = core();
    let mut deliveries = Vec::new();
    let mut inputs = vec![meta(0)];
    for i in 2..=8_u64 {
        let mut row = metadata_snapshot_row();
        row.condition_id = format!("0x{}", format!("{i:x}").repeat(64));
        row.clob_market_id = (3_195_575 + i).to_string();
        row.asset_ids = format!("[\"{}\",\"{}\"]", 20 + 2 * i, 21 + 2 * i);
        row.collector_admission_sequence = i;
        inputs.push(record(
            i - 1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(row))),
        ));
    }
    let level = |price: &str, quantity: &str| DepthLevel {
        price: price.into(),
        quantity: quantity.into(),
    };
    inputs.push(record(
        8,
        TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(BinanceDepthSnapshotRow {
            timestamp_received: 13_000,
            snapshot_fetch_start_utc: 12_000,
            last_update_id: 100,
            bids: vec![level("64000", "0.5"), level("64100", "0.7")],
            asks: vec![level("64300", "0.3"), level("64200", "0.4")],
            symbol: "BTCUSDT".into(),
            source_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
                .into(),
            snapshot_status: "complete".into(),
            source_sequence_epoch_id: "depth-a".into(),
            connection_generation: 1,
            depth_ingress_sequence: 1,
            collector_admission_sequence: 1,
            collector_admission_time_utc_ms: 13_001,
            collector_host_id: "collector-1".into(),
            collector_region: "eu-west-1".into(),
            schema_version: "binance_depth_snapshot_v3".into(),
        })),
    ));
    inputs.extend([
        pm(9, 1, 21_000, book("100")),
        pm(
            10,
            2,
            21_001,
            PolymarketOrderbookEvent::BestBidAsk {
                asset_id: UP_TOKEN_ID.into(),
                best_bid: Some("0.5".into()),
                best_ask: Some("0.6".into()),
                spread: Some("0.1".into()),
            },
        ),
        pm(
            11,
            3,
            21_002,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("80"))],
            },
        ),
    ]);
    for input in inputs {
        let first_canonical_position = core.next_canonical_position();
        let prepared = prepare_public_ingress_v1(&core, &input, &BTreeMap::new(), &[]).unwrap();
        let applied = core.push(&prepared).unwrap().into_owned();
        let prepared_pm = match prepared.current {
            PreparedCanonicalInputV1::PolymarketMessage(pm) => Some(pm),
            _ => None,
        };
        let contexts = applied
            .applied
            .closed_groups
            .iter()
            .flat_map(|g| &g.observations)
            .filter_map(|o| {
                core.prepared_pm_context(&o.market.condition_id.0)
                    .map(|c| (o.market.condition_id.0.clone(), c))
            })
            .collect();
        let mut day = data_pipeline_training::day_producer::DayAppliedV1 {
            input,
            applied: applied.applied,
            first_canonical_position,
            prepared_pm,
            contexts,
            initialization_binding: None,
            hidden: vec![],
        };
        deliveries.extend(PreparedPublicGroupV1::from_day(&mut day));
    }
    assert_eq!(deliveries.len(), 12);
    let group = |i: usize| &deliveries[i].prepared.group;
    let up = |group: &ClosedBusinessGroupV1| {
        group.observations[0]
            .assets
            .iter()
            .position(|asset| asset.asset.token_id.0 == UP_TOKEN_ID)
            .unwrap()
    };
    let depth = &group(8).observations[0].binance_depth;
    assert_eq!(
        depth.value.as_ref().unwrap().full().unwrap().bids[0]
            .quantity_btc
            .0,
        "0.7"
    );
    let PublicBusinessPayloadV1::ReferenceFact(
        CanonicalReferenceEventPayloadV2::BinanceDepthState(fact_depth),
    ) = &group(8).records[0].payload
    else {
        panic!("正式深度事实")
    };
    let Some(BinanceDepthObservationV1::Full(observed_depth)) = &depth.value else {
        panic!("默认完整深度")
    };
    assert!(Arc::ptr_eq(fact_depth, observed_depth));
    assert!(Arc::ptr_eq(
        &group(9).observations[0].assets[up(group(9))].asks,
        &group(10).observations[0].assets[up(group(10))].asks
    ));
    assert!(!Arc::ptr_eq(
        &group(10).observations[0].assets[up(group(10))].bids,
        &group(11).observations[0].assets[up(group(11))].bids
    ));
    assert_eq!(
        group(9).observations[0].assets[up(group(9))]
            .bids
            .value
            .as_ref()
            .unwrap()[0]
            .quantity_shares
            .0,
        "100"
    );
    assert_eq!(
        group(11).observations[0].assets[up(group(11))]
            .bids
            .value
            .as_ref()
            .unwrap()[0]
            .quantity_shares
            .0,
        "80"
    );
    let mut original = Vec::new();
    ciborium::into_writer(
        &deliveries
            .iter()
            .map(|d| &d.prepared.group)
            .collect::<Vec<_>>(),
        &mut original,
    )
    .unwrap();
    let saved = checkpoint::DeliveryBatch::capture(deliveries.iter());
    let mut bytes = Vec::new();
    ciborium::into_writer(&saved, &mut bytes).unwrap();
    let restored: checkpoint::DeliveryBatch = ciborium::from_reader(bytes.as_slice()).unwrap();
    let restored = restored.restore().unwrap();
    for (a, b) in deliveries.iter().zip(&restored) {
        assert_eq!(a.prepared.reference, b.prepared.reference);
        assert_eq!(a.delivered_at_utc_ms, b.delivered_at_utc_ms);
        assert_eq!(a.prepared.group, b.prepared.group);
        for o in &b.prepared.group.observations {
            assert!(Arc::ptr_eq(&o.group_events, &b.prepared.group.records));
        }
    }
    assert!(Arc::ptr_eq(
        &restored[9].prepared.group.observations[0].assets[up(&restored[9].prepared.group)].asks,
        &restored[10].prepared.group.observations[0].assets[up(&restored[10].prepared.group)].asks
    ));
    assert!(Arc::ptr_eq(
        &restored[8].prepared.group.observations[0].binance_depth,
        &restored[11].prepared.group.observations[7].binance_depth
    ));
    let encoded: ciborium::Value = ciborium::from_reader(bytes.as_slice()).unwrap();
    let pool = encoded
        .as_map()
        .unwrap()
        .iter()
        .find(|(k, _)| k.as_text() == Some("pool"))
        .unwrap()
        .1
        .as_map()
        .unwrap();
    let count = |name| {
        pool.iter()
            .find(|(k, _)| k.as_text() == Some(name))
            .unwrap()
            .1
            .as_array()
            .unwrap()
            .len()
    };
    assert_eq!(count("depth_values"), 1, "共享数量及组事实仅编码一次");
    println!(
        "day_shared_encoding markets=8 groups={} ordinary_group_bytes={} shared_delivery_bytes={} books={} depth_values={} event_sets={}",
        deliveries.len(),
        original.len(),
        bytes.len(),
        count("books"),
        count("depth_values"),
        count("events")
    );
}
