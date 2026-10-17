use contracts::*;
use data_pipeline_core::{
    public_input::*, public_observation_core::*, public_reference::*, typed_raw::*,
};
use sha2::{Digest, Sha256};

pub const CONDITION_ID: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";

const CLOB_MARKET_ID: &str = "3195576";

pub const UP_TOKEN_ID: &str = "20";

pub const DOWN_TOKEN_ID: &str = "3";

pub(super) const SNAPSHOT_RECEIVED_AT: i64 = 10_000;

const MARKET_START_AT: i64 = 20_000;

pub(super) const MARKET_END_AT: i64 = 30_000;

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

pub(super) fn metadata_snapshot_row() -> PolymarketMarketMetadataRow {
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

pub(super) fn row(
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

pub(super) fn book(quantity: &str) -> PolymarketOrderbookEvent {
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
