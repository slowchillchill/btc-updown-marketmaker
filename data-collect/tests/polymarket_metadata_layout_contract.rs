use std::collections::BTreeMap;
use std::fs::File;
use std::sync::Arc;

use arrow_array::{
    Array as _, ArrayRef, BooleanArray, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::DataType;
use data_collect::polymarket_market_metadata_layout::{
    ActiveUnavailableReason, ClosedTimeCheckFailureReason, MarketLifecycleState,
    MarketLifecycleTransition, MarketRuntimeRole, MarketRuntimeRoleTransition,
    POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION, POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION,
    PolymarketMarketMetadataContext, PolymarketMarketMetadataRow, PolymarketMarketMetadataV3Row,
    PolymarketMetadataOrigin, PolymarketMetadataRowKind,
    PolymarketMetadataV3ReferenceIntegrityError, PolymarketMetadataV3ReferenceRecord,
    ResolutionEvidenceSource, ResolutionFollowupStatus, ResolutionIntegrityErrorClass,
    ResolutionPollResult, ResolutionTransportErrorClass, ResponseFieldsUnavailableReason,
    SourceEndpointUnavailableReason, SourceEvidenceChannel, SourceEvidenceEventType,
    SourceEvidenceOrigin, TerminalClaimOwner, TerminalPredicateFailureReason,
    polymarket_market_metadata_schema, polymarket_market_metadata_v3_schema,
    validate_polymarket_metadata_v3_local_references,
};
use data_collect::sources::polymarket::{
    GammaTerminalAttemptContext, GammaTerminalFixedIdentity, GammaTerminalResponse,
    evaluate_gamma_terminal_response,
};
use data_collect::writer::TypedParquetSegmentWriter;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use polymarket_client_sdk_v2::clob::ws::WsMessage;
use polymarket_client_sdk_v2::gamma::types::response::Market as SdkGammaMarket;
use serde_json::json;
use sha2::{Digest as _, Sha256};
use tempfile::tempdir;

fn context() -> PolymarketMarketMetadataContext {
    PolymarketMarketMetadataContext {
        timestamp_received_ms: 1_783_392_404_599,
        metadata_fetch_start_utc_ms: Some(1_783_392_404_100),
        collector_host_id: "collector-host".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "https://gamma-api.polymarket.com/markets".to_owned(),
        schema_version: POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION.to_owned(),
    }
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column(batch.schema().index_of(name).unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
}

fn timestamp_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a TimestampMillisecondArray {
    batch
        .column(batch.schema().index_of(name).unwrap())
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap()
}

fn v3_reference_records_from_batch(
    batch: &RecordBatch,
) -> Vec<PolymarketMetadataV3ReferenceRecord> {
    let row_kind = string_column(batch, "metadata_row_kind");
    let slug = string_column(batch, "slug");
    let condition_id = string_column(batch, "condition_id");
    let clob_market_id = string_column(batch, "clob_market_id");
    let asset_ids = string_column(batch, "asset_ids");
    let market_start = timestamp_column(batch, "market_start_utc");
    let market_end = timestamp_column(batch, "market_end_utc");
    let fixed_verified = timestamp_column(batch, "fixed_identity_verified_at_utc");
    let source_origin = string_column(batch, "source_evidence_origin");
    let source_received = timestamp_column(batch, "source_evidence_received_at_utc");
    let source_digest = string_column(batch, "source_evidence_raw_sha256");
    let source_payload = string_column(batch, "source_evidence_payload_json");

    (0..batch.num_rows())
        .map(|index| PolymarketMetadataV3ReferenceRecord {
            metadata_row_kind: row_kind.value(index).to_owned(),
            slug: slug.value(index).to_owned(),
            condition_id: condition_id.value(index).to_owned(),
            clob_market_id: clob_market_id.value(index).to_owned(),
            asset_ids: asset_ids.value(index).to_owned(),
            market_start_utc: market_start.value(index),
            market_end_utc: market_end.value(index),
            fixed_identity_verified_at_utc: fixed_verified.value(index),
            source_evidence_origin: source_origin
                .is_valid(index)
                .then(|| source_origin.value(index).to_owned()),
            source_evidence_received_at_utc: source_received
                .is_valid(index)
                .then(|| source_received.value(index)),
            source_evidence_raw_sha256: source_digest
                .is_valid(index)
                .then(|| source_digest.value(index).to_owned()),
            source_evidence_payload_present: source_payload.is_valid(index),
        })
        .collect()
}

#[test]
fn new_market_metadata_row_keeps_condition_tokens_and_source() {
    let row = PolymarketMarketMetadataRow::new_market(
        "btc-updown-5m-1783392300",
        "0x0000000000000000000000000000000000000000000000000000000000000001",
        Some("clob-market-1"),
        &[
            "106585164761922456203746651621390029417453862034640469075081961934906147433548"
                .to_owned(),
            "206585164761922456203746651621390029417453862034640469075081961934906147433549"
                .to_owned(),
        ],
        Some(1_783_392_300_000),
        Some(1_783_392_600_000),
        Some(true),
        Some(true),
        "prefetched",
        Some("0.01"),
        Some("1"),
        Some(r#"{"maker":0}"#),
        Some(r#"{"eligible":true}"#),
        Some(true),
        context(),
    );

    assert_eq!(row.metadata_source, "new_market");
    assert_eq!(row.slug, "btc-updown-5m-1783392300");
    assert_eq!(
        row.asset_ids.as_deref(),
        Some(
            r#"["106585164761922456203746651621390029417453862034640469075081961934906147433548","206585164761922456203746651621390029417453862034640469075081961934906147433549"]"#
        )
    );
    assert_eq!(row.same_condition_market_verified, Some(true));
    assert!(row.validate().is_complete());
}

#[test]
fn active_scan_and_fallback_close_metadata_rows_are_complete_with_evidence() {
    let active_scan = PolymarketMarketMetadataRow::active_scan(
        "btc-updown-5m-1783392300",
        "condition-1",
        Some("clob-market-1"),
        &["asset-1".to_owned(), "asset-2".to_owned()],
        Some(1_783_392_300_000),
        Some(1_783_392_600_000),
        Some(true),
        Some(true),
        "active",
        Some("0.01"),
        Some("1"),
        Some(r#"{"maker":0}"#),
        Some(r#"{"eligible":true}"#),
        Some(true),
        context(),
    );
    assert_eq!(active_scan.metadata_source, "active_scan");
    assert!(active_scan.validate().is_complete());

    let fallback = PolymarketMarketMetadataRow::fallback_close(
        "btc-updown-5m-1783392300",
        "condition-1",
        Some("clob-market-1"),
        &["asset-1".to_owned(), "asset-2".to_owned()],
        Some(1_783_392_300_000),
        Some(1_783_392_600_000),
        Some(false),
        Some(false),
        "retired",
        Some("0.01"),
        Some("1"),
        Some(r#"{"maker":0}"#),
        Some(r#"{"eligible":true}"#),
        Some(true),
        context(),
    );
    assert_eq!(fallback.metadata_source, "fallback_close");
    assert!(fallback.validate().is_complete());
}

#[test]
fn slug_only_candidate_is_not_complete_discovery() {
    let row = PolymarketMarketMetadataRow::candidate_slug_only(
        "btc-updown-5m-1783392300",
        Some(1_783_392_300_000),
        Some(1_783_392_600_000),
        context(),
    );

    let report = row.validate();
    assert!(!report.is_complete());
    assert!(
        report
            .source_gaps
            .contains(&"slug_only_discovery".to_owned())
    );
    assert!(report.missing_fields.contains(&"condition_id".to_owned()));
    assert!(report.missing_fields.contains(&"asset_ids".to_owned()));
}

#[test]
fn schema_contains_metadata_columns() {
    let schema = polymarket_market_metadata_schema();
    for field in [
        "timestamp_received",
        "metadata_fetch_start_utc",
        "slug",
        "condition_id",
        "clob_market_id",
        "asset_ids",
        "market_start_utc",
        "market_end_utc",
        "active",
        "accepting_orders",
        "market_lifecycle_state",
        "tick_size",
        "min_size",
        "fee_schedule_raw",
        "reward_eligibility_raw",
        "metadata_unavailable_reason",
        "metadata_source",
        "same_condition_market_verified",
    ] {
        schema.field_with_name(field).expect(field);
    }
    assert!(schema.field_with_name("metadata_recv_time_utc").is_err());
    assert!(schema.field_with_name("token_ids").is_err());
    assert!(matches!(
        schema
            .field_with_name("timestamp_received")
            .expect("timestamp_received")
            .data_type(),
        DataType::Timestamp(_, Some(timezone)) if timezone.as_ref() == "UTC"
    ));
}

#[test]
fn historical_v2_parquet_remains_readable_without_reopening_the_production_writer() {
    let schema = polymarket_market_metadata_schema();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(
            TimestampMillisecondArray::from_iter_values([1_783_392_404_599]).with_timezone("UTC"),
        ),
        Arc::new(
            TimestampMillisecondArray::from(vec![Some(1_783_392_404_100)]).with_timezone("UTC"),
        ),
        Arc::new(StringArray::from_iter_values(["btc-updown-5m-1783392300"])),
        Arc::new(StringArray::from_iter_values(["condition-1"])),
        Arc::new(StringArray::from(vec![Some("clob-market-1")])),
        Arc::new(StringArray::from(vec![Some(r#"["asset-1","asset-2"]"#)])),
        Arc::new(
            TimestampMillisecondArray::from(vec![Some(1_783_392_300_000)]).with_timezone("UTC"),
        ),
        Arc::new(
            TimestampMillisecondArray::from(vec![Some(1_783_392_600_000)]).with_timezone("UTC"),
        ),
        Arc::new(BooleanArray::from(vec![Some(true)])),
        Arc::new(BooleanArray::from(vec![Some(true)])),
        Arc::new(StringArray::from_iter_values(["active"])),
        Arc::new(StringArray::from(vec![Some("0.01")])),
        Arc::new(StringArray::from(vec![Some("1")])),
        Arc::new(StringArray::from(vec![Some(r#"{"maker":0}"#)])),
        Arc::new(StringArray::from(vec![Some(r#"{"eligible":true}"#)])),
        Arc::new(StringArray::from(vec![None::<&str>])),
        Arc::new(StringArray::from_iter_values(["active_scan"])),
        Arc::new(BooleanArray::from(vec![Some(true)])),
        Arc::new(StringArray::from_iter_values(["collector-host"])),
        Arc::new(StringArray::from_iter_values(["eu-west-1"])),
        Arc::new(StringArray::from_iter_values([
            "https://gamma-api.polymarket.com/markets",
        ])),
        Arc::new(StringArray::from_iter_values([
            POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION,
        ])),
    ];
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("historical-metadata-v2.parquet");
    let mut historical_writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    historical_writer.write(&batch).unwrap();
    historical_writer.close().unwrap();

    let mut reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let historical = reader.next().unwrap().unwrap();
    assert_eq!(historical.num_rows(), 1);
    assert_eq!(
        string_column(&historical, "schema_version").value(0),
        POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION
    );
    assert_eq!(
        string_column(&historical, "slug").value(0),
        "btc-updown-5m-1783392300"
    );
    assert!(historical.schema().index_of("metadata_row_kind").is_err());
    assert!(
        historical
            .schema()
            .index_of("collector_admission_sequence")
            .is_err()
    );

    let error = match TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "new-metadata-v2",
        POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION,
        "polymarket_market_metadata",
    ) {
        Ok(_) => panic!("historical metadata v2 must remain read-only"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("unsupported Polymarket metadata schema")
    );
}

const T: i64 = 1_783_392_300_000;
const E: i64 = T + 300_000;
const SUMMARY_T: i64 = E;
const SUMMARY_E: i64 = SUMMARY_T + 300_000;
const CONDITION_ID_1: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
const CONDITION_ID_2: &str = "0x0000000000000000000000000000000000000000000000000000000000000002";
const TERMINAL_CLOSED_TIME: &str = "2026-07-07T02:50:16Z";
const TERMINAL_WS_TIMESTAMP_RAW: &str = "1783392839999";
const TERMINAL_WS_PAYLOAD: &str = r#"{"event_type":"market_resolved","id":"clob-market-1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","slug":"btc-updown-5m-1783392300","assets_ids":["101","102"],"outcomes":["Up","Down"],"winning_asset_id":"101","winning_outcome":"Up","timestamp":"1783392839999"}"#;

fn sdk_gamma_payload(
    closed: bool,
    accepting_orders: bool,
    outcome_prices: Option<&str>,
    closed_time: Option<&str>,
    uma_resolution_status: Option<&str>,
) -> String {
    let decoded: SdkGammaMarket = serde_json::from_value(json!({
        "id": "clob-market-1",
        "conditionId": CONDITION_ID_1,
        "slug": "btc-updown-5m-1783392300",
        "clobTokenIds": r#"["101","102"]"#,
        "outcomes": r#"["Up","Down"]"#,
        "outcomePrices": outcome_prices,
        "active": true,
        "closed": closed,
        "acceptingOrders": accepting_orders,
        "closedTime": closed_time,
        "umaResolutionStatus": uma_resolution_status,
    }))
    .unwrap();
    serde_json::to_string(&decoded).unwrap()
}

fn discovery_payload() -> String {
    sdk_gamma_payload(false, true, None, None, None)
}

fn gamma_nonterminal_payload() -> String {
    sdk_gamma_payload(false, false, Some(r#"["0.5","0.5"]"#), None, None)
}

fn terminal_gamma_payload() -> String {
    sdk_gamma_payload(
        true,
        false,
        Some(r#"["1","0"]"#),
        Some(TERMINAL_CLOSED_TIME),
        Some(" ReSoLvEd "),
    )
}

fn sha256_hex(payload: &str) -> String {
    Sha256::digest(payload.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn v3_base(
    metadata_row_kind: PolymarketMetadataRowKind,
    metadata_origin: PolymarketMetadataOrigin,
) -> PolymarketMarketMetadataV3Row {
    PolymarketMarketMetadataV3Row {
        metadata_row_kind,
        timestamp_received: E + 500_000,
        collector_admission_sequence: 1,
        collector_admission_time_utc_ms: E + 500_010,
        metadata_origin,
        collector_host_id: "collector-host".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        schema_version: POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION.to_owned(),
        slug: "btc-updown-5m-1783392300".to_owned(),
        condition_id: CONDITION_ID_1.to_owned(),
        clob_market_id: "clob-market-1".to_owned(),
        asset_ids: r#"["101","102"]"#.to_owned(),
        market_start_utc: T,
        market_end_utc: E,
        fixed_identity_verified_at_utc: T - 120_000,
        market_lifecycle_state: MarketLifecycleState::Active,
        market_runtime_role: MarketRuntimeRole::None,
        source_endpoint: None,
        source_endpoint_unavailable_reason: Some(
            SourceEndpointUnavailableReason::NotApplicableNoSourceEvidence,
        ),
        source_evidence_origin: None,
        source_evidence_event_type: None,
        source_evidence_channel: None,
        source_evidence_received_at_utc: None,
        row_evidence_validation_completed_at_utc: None,
        source_evidence_payload_json: None,
        source_evidence_raw_sha256: None,
        market_lifecycle_transition: None,
        lifecycle_scheduled_at_utc: None,
        lifecycle_applied_at_utc: None,
        market_runtime_role_transition: None,
        runtime_role_scheduled_at_utc: None,
        runtime_role_applied_at_utc: None,
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
        active_unavailable_reason: Some(ActiveUnavailableReason::NotApplicableForRowKind),
        accepting_orders: None,
        ws_platform_timestamp_raw: None,
        ws_platform_timestamp_utc: None,
        winning_asset_id: None,
        winning_outcome: None,
        resolution_evidence_source: None,
        resolution_evidence_raw_sha256: None,
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
        response_fields_unavailable_reason: Some(
            ResponseFieldsUnavailableReason::NotApplicableForRowKind,
        ),
    }
}

fn set_source_endpoint(row: &mut PolymarketMarketMetadataV3Row, endpoint: &str) {
    row.source_endpoint = Some(endpoint.to_owned());
    row.source_endpoint_unavailable_reason = None;
}

fn set_payload(row: &mut PolymarketMarketMetadataV3Row, payload: &str) {
    row.source_evidence_payload_json = Some(payload.to_owned());
    row.source_evidence_raw_sha256 = Some(sha256_hex(payload));
}

fn metadata_snapshot_v3() -> PolymarketMarketMetadataV3Row {
    let mut row = v3_base(
        PolymarketMetadataRowKind::MetadataSnapshot,
        PolymarketMetadataOrigin::GammaMarketsDiscovery,
    );
    let received = T - 120_010;
    row.timestamp_received = received;
    row.collector_admission_time_utc_ms = received + 20;
    row.market_lifecycle_state = MarketLifecycleState::Prefetched;
    row.market_runtime_role = MarketRuntimeRole::Next;
    row.fixed_identity_verified_at_utc = received + 10;
    set_source_endpoint(&mut row, "https://gamma-api.polymarket.com/markets");
    row.source_evidence_origin = Some(SourceEvidenceOrigin::GammaMarketsDiscovery);
    row.source_evidence_event_type = Some(SourceEvidenceEventType::GammaMarketsDiscoveryResponse);
    row.source_evidence_received_at_utc = Some(received);
    row.row_evidence_validation_completed_at_utc = Some(received + 10);
    row.metadata_fetch_start_utc = Some(received - 100);
    let payload = discovery_payload();
    set_payload(&mut row, &payload);
    row.closed = Some(false);
    row.active = Some(true);
    row.active_unavailable_reason = None;
    row.accepting_orders = Some(true);
    row.response_fields_unavailable_reason = None;
    row
}

fn market_state_transition_v3() -> PolymarketMarketMetadataV3Row {
    let mut row = v3_base(
        PolymarketMetadataRowKind::MarketStateTransition,
        PolymarketMetadataOrigin::LocalMarketStateTimer,
    );
    row.timestamp_received = T + 2;
    row.collector_admission_time_utc_ms = T + 5;
    row.market_runtime_role = MarketRuntimeRole::Current;
    set_source_endpoint(&mut row, "https://gamma-api.polymarket.com/markets");
    row.source_evidence_origin = Some(SourceEvidenceOrigin::GammaMarketsDiscovery);
    row.source_evidence_event_type = Some(SourceEvidenceEventType::GammaMarketsDiscoveryResponse);
    row.source_evidence_received_at_utc = Some(T - 120_010);
    row.source_evidence_raw_sha256 = Some(sha256_hex(&discovery_payload()));
    row.market_lifecycle_transition = Some(MarketLifecycleTransition::PrefetchedToActive);
    row.lifecycle_scheduled_at_utc = Some(T);
    row.lifecycle_applied_at_utc = Some(T + 1);
    row.market_runtime_role_transition = Some(MarketRuntimeRoleTransition::NextToCurrent);
    row.runtime_role_scheduled_at_utc = Some(T);
    row.runtime_role_applied_at_utc = Some(T + 1);
    row
}

fn decoded_gamma_attempt_v3() -> PolymarketMarketMetadataV3Row {
    let mut row = v3_base(
        PolymarketMetadataRowKind::ResolutionPollAttempt,
        PolymarketMetadataOrigin::GammaBySlug,
    );
    let received = E + 180_010;
    row.timestamp_received = received;
    row.collector_admission_time_utc_ms = received + 20;
    row.market_runtime_role = MarketRuntimeRole::ResolutionPending;
    set_source_endpoint(&mut row, "https://gamma-api.polymarket.com/markets/slug");
    row.source_evidence_origin = Some(SourceEvidenceOrigin::GammaBySlug);
    row.source_evidence_event_type = Some(SourceEvidenceEventType::GammaMarketBySlugResponse);
    row.source_evidence_received_at_utc = Some(received);
    row.row_evidence_validation_completed_at_utc = Some(received + 10);
    row.metadata_fetch_start_utc = Some(E + 180_001);
    let payload = gamma_nonterminal_payload();
    set_payload(&mut row, &payload);
    row.closed = Some(false);
    row.active = Some(true);
    row.active_unavailable_reason = None;
    row.accepting_orders = Some(false);
    row.resolution_poll_attempt_ordinal = Some(1);
    row.resolution_poll_scheduled_utc = Some(E + 180_000);
    row.resolution_poll_result = Some(ResolutionPollResult::ValidNonterminal);
    row.resolution_budget_horizon_utc = Some(E + 480_000);
    row.resolution_followup_status = Some(ResolutionFollowupStatus::Pending);
    row.terminal_predicate_passed = Some(false);
    row.terminal_predicate_failure_reason = Some(TerminalPredicateFailureReason::ClosedFalse);
    row.response_fields_unavailable_reason = None;
    row
}

fn terminal_gamma_attempt_v3() -> PolymarketMarketMetadataV3Row {
    let mut row = decoded_gamma_attempt_v3();
    let payload = terminal_gamma_payload();
    set_payload(&mut row, &payload);
    row.market_lifecycle_state = MarketLifecycleState::Retired;
    row.market_runtime_role = MarketRuntimeRole::None;
    row.market_lifecycle_transition = Some(MarketLifecycleTransition::ActiveToRetired);
    row.lifecycle_applied_at_utc = Some(E + 180_021);
    row.market_runtime_role_transition = Some(MarketRuntimeRoleTransition::ResolutionPendingToNone);
    row.runtime_role_applied_at_utc = Some(E + 180_021);
    row.closed = Some(true);
    row.closed_time_raw = Some(TERMINAL_CLOSED_TIME.to_owned());
    row.closed_time_utc = Some(E + 16_000);
    row.closed_time_check_passed = Some(true);
    row.uma_resolution_status = Some(" ReSoLvEd ".to_owned());
    row.outcomes_raw = Some(r#"["Up","Down"]"#.to_owned());
    row.outcome_prices_raw = Some(r#"["1","0"]"#.to_owned());
    row.resolution_poll_result = Some(ResolutionPollResult::Terminal);
    row.resolution_followup_status = Some(ResolutionFollowupStatus::TerminalGammaFallback);
    row.winning_asset_id = Some("101".to_owned());
    row.winning_outcome = Some("Up".to_owned());
    row.resolution_evidence_source = Some(ResolutionEvidenceSource::GammaBySlug);
    row.resolution_evidence_raw_sha256 = row.source_evidence_raw_sha256.clone();
    row.terminal_claim_owner = Some(TerminalClaimOwner::GammaBySlug);
    row.terminal_predicate_passed = Some(true);
    row.terminal_predicate_failure_reason = None;
    row
}

fn resolution_terminal_ws_v3() -> PolymarketMarketMetadataV3Row {
    let mut row = v3_base(
        PolymarketMetadataRowKind::ResolutionTerminalWs,
        PolymarketMetadataOrigin::MarketResolvedWs,
    );
    let received = E + 240_000;
    row.timestamp_received = received;
    row.collector_admission_time_utc_ms = received + 20;
    row.market_lifecycle_state = MarketLifecycleState::Retired;
    set_source_endpoint(
        &mut row,
        "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    );
    row.source_evidence_origin = Some(SourceEvidenceOrigin::MarketResolvedWs);
    row.source_evidence_event_type = Some(SourceEvidenceEventType::MarketResolved);
    row.source_evidence_channel = Some(SourceEvidenceChannel::Market);
    row.source_evidence_received_at_utc = Some(received);
    row.row_evidence_validation_completed_at_utc = Some(received + 10);
    set_payload(&mut row, TERMINAL_WS_PAYLOAD);
    row.ws_platform_timestamp_raw = Some(TERMINAL_WS_TIMESTAMP_RAW.to_owned());
    row.ws_platform_timestamp_utc = Some(1_783_392_839_999);
    row.market_lifecycle_transition = Some(MarketLifecycleTransition::ActiveToRetired);
    row.lifecycle_applied_at_utc = Some(received + 11);
    row.market_runtime_role_transition = Some(MarketRuntimeRoleTransition::ResolutionPendingToNone);
    row.runtime_role_applied_at_utc = Some(received + 11);
    row.winning_asset_id = Some("101".to_owned());
    row.winning_outcome = Some("Up".to_owned());
    row.resolution_evidence_source = Some(ResolutionEvidenceSource::MarketResolvedWs);
    row.resolution_evidence_raw_sha256 = row.source_evidence_raw_sha256.clone();
    row.terminal_claim_owner = Some(TerminalClaimOwner::MarketResolvedWs);
    row.resolution_budget_horizon_utc = Some(E + 480_000);
    row.resolution_followup_status = Some(ResolutionFollowupStatus::TerminalWs);
    row.terminal_predicate_passed = Some(true);
    row.response_fields_unavailable_reason = None;
    row
}

fn resolution_followup_summary_v3() -> PolymarketMarketMetadataV3Row {
    let mut row = v3_base(
        PolymarketMetadataRowKind::ResolutionFollowupSummary,
        PolymarketMetadataOrigin::LocalFollowupScheduler,
    );
    row.slug = "btc-updown-5m-1783392600".to_owned();
    row.condition_id = CONDITION_ID_2.to_owned();
    row.clob_market_id = "clob-market-2".to_owned();
    row.asset_ids = r#"["201","202"]"#.to_owned();
    row.market_start_utc = SUMMARY_T;
    row.market_end_utc = SUMMARY_E;
    row.fixed_identity_verified_at_utc = SUMMARY_T - 120_000;
    row.timestamp_received = SUMMARY_E + 480_030;
    row.collector_admission_time_utc_ms = SUMMARY_E + 480_040;
    row.market_runtime_role_transition = Some(MarketRuntimeRoleTransition::ResolutionPendingToNone);
    row.runtime_role_applied_at_utc = Some(SUMMARY_E + 480_020);
    row.resolution_budget_horizon_utc = Some(SUMMARY_E + 480_000);
    row.resolution_followup_status =
        Some(ResolutionFollowupStatus::BudgetExhaustedAfterValidNonterminal);
    row.resolution_poll_attempts_recorded = Some(6);
    row.resolution_valid_response_count = Some(4);
    row.resolution_transport_unavailable_count = Some(1);
    row.resolution_missed_slot_count = Some(1);
    row.last_valid_metadata_received_utc = Some(SUMMARY_E + 420_010);
    row
}

fn assert_snake_case_codec<T>(value: T, expected: &str)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
{
    let encoded = serde_json::to_string(&value).unwrap();
    assert_eq!(encoded, serde_json::to_string(expected).unwrap());
    assert_eq!(serde_json::from_str::<T>(&encoded).unwrap(), value);
}

#[test]
fn v3_internal_json_enums_keep_the_frozen_snake_case_values() {
    macro_rules! assert_values {
        ($($value:expr),+ $(,)?) => {
            $(
                let value = $value;
                assert_snake_case_codec(value, value.as_str());
            )+
        };
    }

    assert_values!(
        PolymarketMetadataRowKind::MetadataSnapshot,
        PolymarketMetadataRowKind::MarketStateTransition,
        PolymarketMetadataRowKind::ResolutionPollAttempt,
        PolymarketMetadataRowKind::ResolutionTerminalWs,
        PolymarketMetadataRowKind::ResolutionFollowupSummary,
        PolymarketMetadataOrigin::GammaMarketsDiscovery,
        PolymarketMetadataOrigin::GammaBySlug,
        PolymarketMetadataOrigin::MarketResolvedWs,
        PolymarketMetadataOrigin::LocalMarketStateTimer,
        PolymarketMetadataOrigin::LocalFollowupScheduler,
        MarketLifecycleState::Candidate,
        MarketLifecycleState::Prefetched,
        MarketLifecycleState::Active,
        MarketLifecycleState::Retired,
        MarketRuntimeRole::Current,
        MarketRuntimeRole::Next,
        MarketRuntimeRole::ResolutionPending,
        MarketRuntimeRole::None,
        SourceEndpointUnavailableReason::NotApplicableNoSourceEvidence,
        SourceEvidenceOrigin::GammaMarketsDiscovery,
        SourceEvidenceOrigin::GammaBySlug,
        SourceEvidenceOrigin::MarketResolvedWs,
        SourceEvidenceEventType::GammaMarketsDiscoveryResponse,
        SourceEvidenceEventType::GammaMarketBySlugResponse,
        SourceEvidenceEventType::MarketResolved,
        SourceEvidenceChannel::Market,
        MarketLifecycleTransition::PrefetchedToActive,
        MarketLifecycleTransition::CandidateToActive,
        MarketLifecycleTransition::ActiveToRetired,
        MarketRuntimeRoleTransition::NoneToCurrent,
        MarketRuntimeRoleTransition::NextToCurrent,
        MarketRuntimeRoleTransition::CurrentToResolutionPending,
        MarketRuntimeRoleTransition::ResolutionPendingToNone,
        ClosedTimeCheckFailureReason::Missing,
        ClosedTimeCheckFailureReason::ParseError,
        ClosedTimeCheckFailureReason::BeforeMarketEnd,
        ActiveUnavailableReason::SourceFieldAbsent,
        ActiveUnavailableReason::NotApplicableForRowKind,
        ResolutionEvidenceSource::MarketResolvedWs,
        ResolutionEvidenceSource::GammaBySlug,
        TerminalClaimOwner::MarketResolvedWs,
        TerminalClaimOwner::GammaBySlug,
        ResolutionPollResult::Terminal,
        ResolutionPollResult::ValidNonterminal,
        ResolutionPollResult::TransportUnavailable,
        ResolutionPollResult::IntegrityError,
        ResolutionPollResult::MissedDueToDowntime,
        ResolutionPollResult::PostTerminalClaimObservation,
        ResolutionTransportErrorClass::Connect,
        ResolutionTransportErrorClass::Timeout,
        ResolutionTransportErrorClass::HttpStatus,
        ResolutionTransportErrorClass::RateLimited,
        ResolutionTransportErrorClass::SdkTransport,
        ResolutionIntegrityErrorClass::AttemptBindingInvariant,
        ResolutionIntegrityErrorClass::SdkDecodeOrSchema,
        ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction,
        ResolutionIntegrityErrorClass::ClosedTimeInvalid,
        ResolutionIntegrityErrorClass::DerivedWinnerInvariant,
        ResolutionFollowupStatus::Pending,
        ResolutionFollowupStatus::TerminalWs,
        ResolutionFollowupStatus::TerminalGammaFallback,
        ResolutionFollowupStatus::BudgetExhaustedAfterValidNonterminal,
        ResolutionFollowupStatus::BudgetExhaustedWithoutValidResponse,
        ResponseFieldsUnavailableReason::NotApplicableForRowKind,
        ResponseFieldsUnavailableReason::TransportUnavailable,
        ResponseFieldsUnavailableReason::MissedDueToDowntime,
        ResponseFieldsUnavailableReason::SdkDecodeOrSchemaError,
    );
    for value in TerminalPredicateFailureReason::ALL {
        assert_snake_case_codec(value, value.as_str());
    }
}

#[test]
fn v3_internal_json_draft_excludes_and_cannot_override_collector_admission() {
    let row = metadata_snapshot_v3();
    let draft_json = row.to_draft_json().unwrap();
    let mut draft_value = serde_json::from_str::<serde_json::Value>(&draft_json).unwrap();
    assert!(draft_value.get("collector_admission_sequence").is_none());
    assert!(draft_value.get("collector_admission_time_utc_ms").is_none());

    let draft_object = draft_value.as_object_mut().unwrap();
    draft_object.insert("collector_admission_sequence".to_owned(), json!(999_999));
    draft_object.insert(
        "collector_admission_time_utc_ms".to_owned(),
        json!(9_999_999_999_999_i64),
    );

    let external_sequence = 41;
    let external_time = row.collector_admission_time_utc_ms;
    let restored = PolymarketMarketMetadataV3Row::from_draft_json_with_admission(
        &serde_json::to_string(&draft_value).unwrap(),
        external_sequence,
        external_time,
    )
    .unwrap();
    let mut expected = row;
    expected.collector_admission_sequence = external_sequence;
    assert_eq!(restored, expected);
    assert!(restored.validate().is_complete());
}

#[test]
fn v3_schema_has_the_frozen_fields_types_and_utc_units() {
    let schema = polymarket_market_metadata_v3_schema();
    let expected_names = [
        "metadata_row_kind",
        "timestamp_received",
        "collector_admission_sequence",
        "collector_admission_time_utc_ms",
        "metadata_origin",
        "collector_host_id",
        "collector_region",
        "schema_version",
        "slug",
        "condition_id",
        "clob_market_id",
        "asset_ids",
        "market_start_utc",
        "market_end_utc",
        "fixed_identity_verified_at_utc",
        "market_lifecycle_state",
        "market_runtime_role",
        "source_endpoint",
        "source_endpoint_unavailable_reason",
        "source_evidence_origin",
        "source_evidence_event_type",
        "source_evidence_channel",
        "source_evidence_received_at_utc",
        "row_evidence_validation_completed_at_utc",
        "source_evidence_payload_json",
        "source_evidence_raw_sha256",
        "market_lifecycle_transition",
        "lifecycle_scheduled_at_utc",
        "lifecycle_applied_at_utc",
        "market_runtime_role_transition",
        "runtime_role_scheduled_at_utc",
        "runtime_role_applied_at_utc",
        "metadata_fetch_start_utc",
        "closed",
        "closed_time_raw",
        "closed_time_utc",
        "closed_time_check_passed",
        "closed_time_check_failure_reason",
        "automatically_resolved",
        "uma_resolution_status",
        "outcomes_raw",
        "outcome_prices_raw",
        "active",
        "active_unavailable_reason",
        "accepting_orders",
        "ws_platform_timestamp_raw",
        "ws_platform_timestamp_utc",
        "winning_asset_id",
        "winning_outcome",
        "resolution_evidence_source",
        "resolution_evidence_raw_sha256",
        "terminal_claim_owner",
        "resolution_poll_attempt_ordinal",
        "resolution_poll_scheduled_utc",
        "resolution_poll_result",
        "resolution_transport_error_class",
        "resolution_transport_http_status",
        "resolution_integrity_error_class",
        "resolution_budget_horizon_utc",
        "resolution_followup_status",
        "resolution_poll_attempts_recorded",
        "resolution_valid_response_count",
        "resolution_transport_unavailable_count",
        "resolution_missed_slot_count",
        "last_valid_metadata_received_utc",
        "terminal_predicate_passed",
        "terminal_predicate_failure_reason",
        "response_fields_unavailable_reason",
    ];
    assert_eq!(schema.fields().len(), expected_names.len());
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        expected_names
    );

    for name in [
        "timestamp_received",
        "collector_admission_time_utc_ms",
        "market_start_utc",
        "market_end_utc",
        "fixed_identity_verified_at_utc",
        "source_evidence_received_at_utc",
        "row_evidence_validation_completed_at_utc",
        "lifecycle_scheduled_at_utc",
        "lifecycle_applied_at_utc",
        "runtime_role_scheduled_at_utc",
        "runtime_role_applied_at_utc",
        "metadata_fetch_start_utc",
        "closed_time_utc",
        "ws_platform_timestamp_utc",
        "resolution_poll_scheduled_utc",
        "resolution_budget_horizon_utc",
        "last_valid_metadata_received_utc",
    ] {
        assert!(matches!(
            schema.field_with_name(name).expect(name).data_type(),
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, Some(timezone))
                if timezone.as_ref() == "UTC"
        ));
    }
    assert_eq!(
        schema
            .field_with_name("resolution_poll_attempt_ordinal")
            .expect("ordinal")
            .data_type(),
        &DataType::UInt8
    );
    assert_eq!(
        schema
            .field_with_name("resolution_transport_http_status")
            .expect("http status")
            .data_type(),
        &DataType::UInt16
    );
    assert!(
        !schema
            .field_with_name("metadata_row_kind")
            .unwrap()
            .is_nullable()
    );
    assert!(
        schema
            .field_with_name("source_endpoint")
            .unwrap()
            .is_nullable()
    );
    assert!(
        polymarket_market_metadata_schema()
            .field_with_name("metadata_row_kind")
            .is_err()
    );
}

#[test]
fn v3_stable_row_kinds_and_failure_precedence_are_frozen() {
    assert_eq!(
        [
            PolymarketMetadataRowKind::MetadataSnapshot,
            PolymarketMetadataRowKind::MarketStateTransition,
            PolymarketMetadataRowKind::ResolutionPollAttempt,
            PolymarketMetadataRowKind::ResolutionTerminalWs,
            PolymarketMetadataRowKind::ResolutionFollowupSummary,
        ]
        .map(PolymarketMetadataRowKind::as_str),
        [
            "metadata_snapshot",
            "market_state_transition",
            "resolution_poll_attempt",
            "resolution_terminal_ws",
            "resolution_followup_summary",
        ]
    );
    assert_eq!(
        TerminalPredicateFailureReason::ALL.map(TerminalPredicateFailureReason::as_str),
        [
            "slug_mismatch",
            "market_id_mismatch",
            "condition_id_mismatch",
            "asset_count_not_two",
            "asset_duplicate",
            "asset_set_mismatch",
            "outcomes_count_not_two",
            "outcome_mapping_invalid",
            "outcome_prices_count_not_two",
            "closed_missing",
            "closed_false",
            "accepting_orders_missing",
            "accepting_orders_true",
            "closed_time_missing",
            "closed_time_parse_error",
            "closed_time_before_market_end",
            "uma_resolution_status_missing",
            "uma_resolution_status_not_resolved",
            "outcome_prices_not_one_hot",
        ]
    );
    assert_eq!(
        TerminalPredicateFailureReason::SlugMismatch.integrity_error_class(),
        Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction)
    );
    assert_eq!(
        TerminalPredicateFailureReason::ClosedTimeParseError.integrity_error_class(),
        Some(ResolutionIntegrityErrorClass::ClosedTimeInvalid)
    );
    assert_eq!(
        TerminalPredicateFailureReason::ClosedFalse.poll_result(),
        ResolutionPollResult::ValidNonterminal
    );

    assert_eq!(
        [
            PolymarketMetadataOrigin::GammaMarketsDiscovery,
            PolymarketMetadataOrigin::GammaBySlug,
            PolymarketMetadataOrigin::MarketResolvedWs,
            PolymarketMetadataOrigin::LocalMarketStateTimer,
            PolymarketMetadataOrigin::LocalFollowupScheduler,
        ]
        .map(PolymarketMetadataOrigin::as_str),
        [
            "gamma_markets_discovery",
            "gamma_by_slug",
            "market_resolved_ws",
            "local_market_state_timer",
            "local_followup_scheduler",
        ]
    );
    assert_eq!(
        [
            MarketLifecycleState::Candidate,
            MarketLifecycleState::Prefetched,
            MarketLifecycleState::Active,
            MarketLifecycleState::Retired,
        ]
        .map(MarketLifecycleState::as_str),
        ["candidate", "prefetched", "active", "retired"]
    );
    assert_eq!(
        [
            MarketRuntimeRole::Current,
            MarketRuntimeRole::Next,
            MarketRuntimeRole::ResolutionPending,
            MarketRuntimeRole::None,
        ]
        .map(MarketRuntimeRole::as_str),
        ["current", "next", "resolution_pending", "none"]
    );
    assert_eq!(
        [SourceEndpointUnavailableReason::NotApplicableNoSourceEvidence]
            .map(SourceEndpointUnavailableReason::as_str),
        ["not_applicable_no_source_evidence"]
    );
    assert_eq!(
        [
            SourceEvidenceOrigin::GammaMarketsDiscovery,
            SourceEvidenceOrigin::GammaBySlug,
            SourceEvidenceOrigin::MarketResolvedWs,
        ]
        .map(SourceEvidenceOrigin::as_str),
        [
            "gamma_markets_discovery",
            "gamma_by_slug",
            "market_resolved_ws",
        ]
    );
    assert_eq!(
        [
            SourceEvidenceEventType::GammaMarketsDiscoveryResponse,
            SourceEvidenceEventType::GammaMarketBySlugResponse,
            SourceEvidenceEventType::MarketResolved,
        ]
        .map(SourceEvidenceEventType::as_str),
        [
            "gamma_markets_discovery_response",
            "gamma_market_by_slug_response",
            "market_resolved",
        ]
    );
    assert_eq!(
        [SourceEvidenceChannel::Market].map(SourceEvidenceChannel::as_str),
        ["market"]
    );
    assert_eq!(
        [
            MarketLifecycleTransition::PrefetchedToActive,
            MarketLifecycleTransition::CandidateToActive,
            MarketLifecycleTransition::ActiveToRetired,
        ]
        .map(MarketLifecycleTransition::as_str),
        [
            "prefetched_to_active",
            "candidate_to_active",
            "active_to_retired",
        ]
    );
    assert_eq!(
        [
            MarketRuntimeRoleTransition::NoneToCurrent,
            MarketRuntimeRoleTransition::NextToCurrent,
            MarketRuntimeRoleTransition::CurrentToResolutionPending,
            MarketRuntimeRoleTransition::ResolutionPendingToNone,
        ]
        .map(MarketRuntimeRoleTransition::as_str),
        [
            "none_to_current",
            "next_to_current",
            "current_to_resolution_pending",
            "resolution_pending_to_none",
        ]
    );
    assert_eq!(
        [
            ClosedTimeCheckFailureReason::Missing,
            ClosedTimeCheckFailureReason::ParseError,
            ClosedTimeCheckFailureReason::BeforeMarketEnd,
        ]
        .map(ClosedTimeCheckFailureReason::as_str),
        ["missing", "parse_error", "before_market_end"]
    );
    assert_eq!(
        [
            ActiveUnavailableReason::SourceFieldAbsent,
            ActiveUnavailableReason::NotApplicableForRowKind,
        ]
        .map(ActiveUnavailableReason::as_str),
        ["source_field_absent", "not_applicable_for_row_kind"]
    );
    assert_eq!(
        [
            ResolutionEvidenceSource::MarketResolvedWs,
            ResolutionEvidenceSource::GammaBySlug,
        ]
        .map(ResolutionEvidenceSource::as_str),
        ["market_resolved_ws", "gamma_by_slug"]
    );
    assert_eq!(
        [
            TerminalClaimOwner::MarketResolvedWs,
            TerminalClaimOwner::GammaBySlug,
        ]
        .map(TerminalClaimOwner::as_str),
        ["market_resolved_ws", "gamma_by_slug"]
    );
    assert_eq!(
        [
            ResolutionPollResult::Terminal,
            ResolutionPollResult::ValidNonterminal,
            ResolutionPollResult::TransportUnavailable,
            ResolutionPollResult::IntegrityError,
            ResolutionPollResult::MissedDueToDowntime,
            ResolutionPollResult::PostTerminalClaimObservation,
        ]
        .map(ResolutionPollResult::as_str),
        [
            "terminal",
            "valid_nonterminal",
            "transport_unavailable",
            "integrity_error",
            "missed_due_to_downtime",
            "post_terminal_claim_observation",
        ]
    );
    assert_eq!(
        [
            ResolutionTransportErrorClass::Connect,
            ResolutionTransportErrorClass::Timeout,
            ResolutionTransportErrorClass::HttpStatus,
            ResolutionTransportErrorClass::RateLimited,
            ResolutionTransportErrorClass::SdkTransport,
        ]
        .map(ResolutionTransportErrorClass::as_str),
        [
            "connect",
            "timeout",
            "http_status",
            "rate_limited",
            "sdk_transport",
        ]
    );
    assert_eq!(
        ResolutionTransportErrorClass::TYPED_CLASSIFICATION_PRECEDENCE
            .map(ResolutionTransportErrorClass::as_str),
        [
            "rate_limited",
            "http_status",
            "timeout",
            "connect",
            "sdk_transport",
        ],
        "transport classification must use public typed first-match precedence"
    );
    assert!(
        serde_json::from_str::<ResolutionTransportErrorClass>(r#""dns""#).is_err(),
        "v3 must not retain an unprovable dns subtype"
    );
    assert_eq!(
        [
            ResolutionIntegrityErrorClass::AttemptBindingInvariant,
            ResolutionIntegrityErrorClass::SdkDecodeOrSchema,
            ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction,
            ResolutionIntegrityErrorClass::ClosedTimeInvalid,
            ResolutionIntegrityErrorClass::DerivedWinnerInvariant,
        ]
        .map(ResolutionIntegrityErrorClass::as_str),
        [
            "attempt_binding_invariant",
            "sdk_decode_or_schema",
            "source_identity_or_shape_contradiction",
            "closed_time_invalid",
            "derived_winner_invariant",
        ]
    );
    assert_eq!(
        [
            ResolutionFollowupStatus::Pending,
            ResolutionFollowupStatus::TerminalWs,
            ResolutionFollowupStatus::TerminalGammaFallback,
            ResolutionFollowupStatus::BudgetExhaustedAfterValidNonterminal,
            ResolutionFollowupStatus::BudgetExhaustedWithoutValidResponse,
        ]
        .map(ResolutionFollowupStatus::as_str),
        [
            "pending",
            "terminal_ws",
            "terminal_gamma_fallback",
            "budget_exhausted_after_valid_nonterminal",
            "budget_exhausted_without_valid_response",
        ]
    );
    assert_eq!(
        [
            ResponseFieldsUnavailableReason::NotApplicableForRowKind,
            ResponseFieldsUnavailableReason::TransportUnavailable,
            ResponseFieldsUnavailableReason::MissedDueToDowntime,
            ResponseFieldsUnavailableReason::SdkDecodeOrSchemaError,
        ]
        .map(ResponseFieldsUnavailableReason::as_str),
        [
            "not_applicable_for_row_kind",
            "transport_unavailable",
            "missed_due_to_downtime",
            "sdk_decode_or_schema_error",
        ]
    );
}

#[test]
fn five_v3_row_kinds_pass_the_direct_required_null_contract() {
    let rows = [
        ("metadata_snapshot", metadata_snapshot_v3()),
        ("market_state_transition", market_state_transition_v3()),
        ("resolution_poll_attempt", decoded_gamma_attempt_v3()),
        ("resolution_terminal_ws", resolution_terminal_ws_v3()),
        (
            "resolution_followup_summary",
            resolution_followup_summary_v3(),
        ),
    ];
    for (name, row) in rows {
        let validation = row.validate();
        assert!(
            validation.is_complete(),
            "{name}: {:?}",
            validation.violations
        );
    }
}

#[test]
fn source_payloads_reconstruct_through_the_pinned_sdk_and_unique_terminal_evaluator() {
    let gamma_row = terminal_gamma_attempt_v3();
    let gamma_payload = gamma_row.source_evidence_payload_json.as_deref().unwrap();
    let decoded_gamma: SdkGammaMarket = serde_json::from_str(gamma_payload).unwrap();
    let fixed_identity = GammaTerminalFixedIdentity {
        slug: gamma_row.slug.clone(),
        market_id: gamma_row.clob_market_id.clone(),
        condition_id: gamma_row.condition_id.clone(),
        up_asset_id: "101".to_owned(),
        down_asset_id: "102".to_owned(),
        market_end_utc_ms: gamma_row.market_end_utc,
    };
    let attempt = GammaTerminalAttemptContext {
        scheduled_at_utc_ms: gamma_row.resolution_poll_scheduled_utc.unwrap(),
        fetch_start_utc_ms: gamma_row.metadata_fetch_start_utc.unwrap(),
        received_at_utc_ms: gamma_row.source_evidence_received_at_utc.unwrap(),
        response_bound_to_attempt: true,
    };
    let evaluation = evaluate_gamma_terminal_response(
        &fixed_identity,
        attempt,
        GammaTerminalResponse::Decoded(&decoded_gamma),
    );
    assert_eq!(
        evaluation.terminal_predicate_passed,
        gamma_row.terminal_predicate_passed
    );
    assert_eq!(
        evaluation.terminal_predicate_failure_reason,
        gamma_row.terminal_predicate_failure_reason
    );
    assert_eq!(
        evaluation.resolution_poll_result,
        gamma_row.resolution_poll_result.unwrap()
    );
    assert_eq!(evaluation.observed_active, gamma_row.active);
    assert_eq!(evaluation.closed_time_raw, gamma_row.closed_time_raw);
    assert_eq!(evaluation.closed_time_utc_ms, gamma_row.closed_time_utc);
    assert_eq!(
        evaluation
            .winner
            .map(|winner| (winner.winning_asset_id, winner.winning_outcome,)),
        Some((
            gamma_row.winning_asset_id.clone().unwrap(),
            gamma_row.winning_outcome.clone().unwrap(),
        ))
    );

    let unbound = evaluate_gamma_terminal_response(
        &fixed_identity,
        GammaTerminalAttemptContext {
            response_bound_to_attempt: false,
            ..attempt
        },
        GammaTerminalResponse::Decoded(&decoded_gamma),
    );
    assert_eq!(
        unbound.resolution_integrity_error_class,
        Some(ResolutionIntegrityErrorClass::AttemptBindingInvariant)
    );
    let decode_error = evaluate_gamma_terminal_response(
        &fixed_identity,
        attempt,
        GammaTerminalResponse::SdkDecodeOrSchemaError,
    );
    assert_eq!(
        decode_error.resolution_integrity_error_class,
        Some(ResolutionIntegrityErrorClass::SdkDecodeOrSchema)
    );

    let ws_row = resolution_terminal_ws_v3();
    let ws_message: WsMessage =
        serde_json::from_str(ws_row.source_evidence_payload_json.as_deref().unwrap()).unwrap();
    let WsMessage::MarketResolved(resolved) = ws_message else {
        panic!("fixture must decode as MarketResolved");
    };
    assert_eq!(resolved.id, ws_row.clob_market_id);
    assert_eq!(format!("{:#x}", resolved.market), ws_row.condition_id);
    assert_eq!(resolved.slug.as_deref(), Some(ws_row.slug.as_str()));
    assert_eq!(
        resolved
            .asset_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        serde_json::from_str::<Vec<String>>(&ws_row.asset_ids).unwrap()
    );
    assert_eq!(
        resolved
            .winning_asset_id
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("101")
    );
    assert_eq!(resolved.winning_outcome, ws_row.winning_outcome);
    assert_eq!(resolved.timestamp, ws_row.ws_platform_timestamp_utc);
    assert_eq!(
        resolved.timestamp.map(|timestamp| timestamp.to_string()),
        ws_row.ws_platform_timestamp_raw
    );
}

#[test]
fn five_v3_row_kinds_round_trip_through_the_versioned_writer_in_admission_order() {
    let mut rows = vec![
        metadata_snapshot_v3(),
        market_state_transition_v3(),
        decoded_gamma_attempt_v3(),
        resolution_terminal_ws_v3(),
        resolution_followup_summary_v3(),
    ];
    let snapshot = &rows[0];
    for row in &rows[1..4] {
        assert_eq!(row.slug, snapshot.slug);
        assert_eq!(row.condition_id, snapshot.condition_id);
        assert_eq!(row.clob_market_id, snapshot.clob_market_id);
        assert_eq!(row.asset_ids, snapshot.asset_ids);
        assert_eq!(row.market_start_utc, snapshot.market_start_utc);
        assert_eq!(row.market_end_utc, snapshot.market_end_utc);
        assert_eq!(
            row.fixed_identity_verified_at_utc,
            snapshot.fixed_identity_verified_at_utc
        );
    }
    let transition = &rows[1];
    let transition_reference_matches = rows
        .iter()
        .filter(|row| {
            row.slug == transition.slug
                && row.fixed_identity_verified_at_utc == transition.fixed_identity_verified_at_utc
                && row.source_evidence_payload_json.is_some()
                && row.source_evidence_received_at_utc == transition.source_evidence_received_at_utc
                && row.source_evidence_raw_sha256 == transition.source_evidence_raw_sha256
        })
        .count();
    assert_eq!(transition_reference_matches, 1);
    let summary = &rows[4];
    assert_eq!(summary.market_start_utc, SUMMARY_T);
    assert_eq!(summary.market_end_utc, SUMMARY_E);
    assert_eq!(summary.slug, format!("btc-updown-5m-{}", SUMMARY_T / 1_000));
    assert_ne!(summary.condition_id, snapshot.condition_id);

    for (index, row) in rows.iter_mut().enumerate() {
        row.collector_admission_sequence = index as u64 + 1;
    }

    let tmp = tempdir().unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        tmp.path(),
        "metadata-v3-layout-contract",
        POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION,
        "polymarket_market_metadata",
    )
    .unwrap();
    writer
        .write_polymarket_market_metadata_v3_rows(&rows)
        .unwrap();

    let mut duplicate = metadata_snapshot_v3();
    duplicate.collector_admission_sequence = 5;
    let error = writer
        .write_polymarket_market_metadata_v3_rows(&[duplicate])
        .unwrap_err();
    assert!(error.to_string().contains("strictly increasing"));

    let published = writer.finalize().unwrap();
    assert_eq!(published.row_count, 5);
    assert_eq!(
        published.schema_version,
        POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION
    );

    let builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&published.final_path).unwrap())
            .unwrap();
    let metadata = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap()
        .iter()
        .map(|entry| (entry.key.clone(), entry.value.clone()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        metadata
            .get("parquet_raw_layout_version")
            .and_then(|value| value.as_deref()),
        Some(POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION)
    );
    assert_eq!(
        metadata
            .get("parquet_raw_source_kind")
            .and_then(|value| value.as_deref()),
        Some("polymarket_market_metadata")
    );

    let mut reader = builder.build().unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 5);
    let row_kinds = batch
        .column(batch.schema().index_of("metadata_row_kind").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        (0..5)
            .map(|index| row_kinds.value(index))
            .collect::<Vec<_>>(),
        [
            "metadata_snapshot",
            "market_state_transition",
            "resolution_poll_attempt",
            "resolution_terminal_ws",
            "resolution_followup_summary",
        ]
    );
    let source_endpoint = batch.column(batch.schema().index_of("source_endpoint").unwrap());
    assert!(source_endpoint.is_valid(0));
    assert!(source_endpoint.is_null(4));
    let poll_result = batch.column(batch.schema().index_of("resolution_poll_result").unwrap());
    assert!(poll_result.is_null(0));
    assert!(poll_result.is_valid(2));
    let terminal_owner = batch.column(batch.schema().index_of("terminal_claim_owner").unwrap());
    assert!(terminal_owner.is_null(2));
    assert!(terminal_owner.is_valid(3));

    let references = v3_reference_records_from_batch(&batch);
    validate_polymarket_metadata_v3_local_references(&references).unwrap();

    let mut unique_digest_collision = references.clone();
    let mut different_payload = references[0].clone();
    different_payload.source_evidence_raw_sha256 = Some("f".repeat(64));
    unique_digest_collision.push(different_payload);
    validate_polymarket_metadata_v3_local_references(&unique_digest_collision)
        .expect("the full reference tuple must select the unique matching digest");

    let mut missing = references.clone();
    missing.remove(0);
    assert!(matches!(
        validate_polymarket_metadata_v3_local_references(&missing),
        Err(PolymarketMetadataV3ReferenceIntegrityError::Missing { .. })
    ));

    let mut duplicate = references.clone();
    duplicate.push(references[0].clone());
    assert!(matches!(
        validate_polymarket_metadata_v3_local_references(&duplicate),
        Err(PolymarketMetadataV3ReferenceIntegrityError::Multiple { matches: 2, .. })
    ));

    let mut digest_mismatch = references;
    digest_mismatch[0].source_evidence_raw_sha256 = Some("0".repeat(64));
    assert!(matches!(
        validate_polymarket_metadata_v3_local_references(&digest_mismatch),
        Err(PolymarketMetadataV3ReferenceIntegrityError::DigestMismatch { .. })
    ));
}

#[test]
fn v3_subtype_contracts_cover_transition_attempt_and_summary_variants() {
    let mut late_transition = market_state_transition_v3();
    late_transition.market_lifecycle_transition =
        Some(MarketLifecycleTransition::CandidateToActive);
    late_transition.market_runtime_role_transition =
        Some(MarketRuntimeRoleTransition::NoneToCurrent);
    late_transition.source_evidence_received_at_utc = Some(T - 5);
    late_transition.row_evidence_validation_completed_at_utc = Some(T - 2);
    late_transition.fixed_identity_verified_at_utc = T - 2;
    late_transition.metadata_fetch_start_utc = Some(T - 10);
    let late_payload = discovery_payload();
    set_payload(&mut late_transition, &late_payload);
    late_transition.active = Some(true);
    late_transition.active_unavailable_reason = None;
    late_transition.accepting_orders = Some(true);
    late_transition.response_fields_unavailable_reason = None;

    let mut end_transition = market_state_transition_v3();
    end_transition.timestamp_received = E + 2;
    end_transition.collector_admission_time_utc_ms = E + 5;
    end_transition.market_runtime_role = MarketRuntimeRole::ResolutionPending;
    end_transition.market_lifecycle_transition = None;
    end_transition.lifecycle_scheduled_at_utc = None;
    end_transition.lifecycle_applied_at_utc = None;
    end_transition.market_runtime_role_transition =
        Some(MarketRuntimeRoleTransition::CurrentToResolutionPending);
    end_transition.runtime_role_scheduled_at_utc = Some(E);
    end_transition.runtime_role_applied_at_utc = Some(E + 1);
    end_transition.resolution_budget_horizon_utc = Some(E + 480_000);
    end_transition.resolution_followup_status = Some(ResolutionFollowupStatus::Pending);

    let terminal_gamma = terminal_gamma_attempt_v3();

    let mut transport = decoded_gamma_attempt_v3();
    transport.source_evidence_origin = None;
    transport.source_evidence_event_type = None;
    transport.source_evidence_received_at_utc = None;
    transport.row_evidence_validation_completed_at_utc = None;
    transport.source_evidence_payload_json = None;
    transport.source_evidence_raw_sha256 = None;
    transport.closed = None;
    transport.active = None;
    transport.active_unavailable_reason = Some(ActiveUnavailableReason::SourceFieldAbsent);
    transport.accepting_orders = None;
    transport.resolution_poll_result = Some(ResolutionPollResult::TransportUnavailable);
    transport.resolution_transport_error_class = Some(ResolutionTransportErrorClass::Timeout);
    transport.terminal_predicate_passed = None;
    transport.terminal_predicate_failure_reason = None;
    transport.response_fields_unavailable_reason =
        Some(ResponseFieldsUnavailableReason::TransportUnavailable);

    let mut binding = decoded_gamma_attempt_v3();
    binding.resolution_poll_result = Some(ResolutionPollResult::IntegrityError);
    binding.resolution_integrity_error_class =
        Some(ResolutionIntegrityErrorClass::AttemptBindingInvariant);
    binding.terminal_predicate_passed = None;
    binding.terminal_predicate_failure_reason = None;

    let mut decode = decoded_gamma_attempt_v3();
    decode.source_evidence_origin = None;
    decode.source_evidence_event_type = None;
    decode.source_evidence_received_at_utc = None;
    decode.row_evidence_validation_completed_at_utc = None;
    decode.source_evidence_payload_json = None;
    decode.source_evidence_raw_sha256 = None;
    decode.closed = None;
    decode.active = None;
    decode.active_unavailable_reason = Some(ActiveUnavailableReason::SourceFieldAbsent);
    decode.accepting_orders = None;
    decode.resolution_poll_result = Some(ResolutionPollResult::IntegrityError);
    decode.resolution_integrity_error_class =
        Some(ResolutionIntegrityErrorClass::SdkDecodeOrSchema);
    decode.terminal_predicate_passed = None;
    decode.terminal_predicate_failure_reason = None;
    decode.response_fields_unavailable_reason =
        Some(ResponseFieldsUnavailableReason::SdkDecodeOrSchemaError);

    let mut missed = transport.clone();
    missed.metadata_origin = PolymarketMetadataOrigin::LocalFollowupScheduler;
    missed.source_endpoint = None;
    missed.source_endpoint_unavailable_reason =
        Some(SourceEndpointUnavailableReason::NotApplicableNoSourceEvidence);
    missed.metadata_fetch_start_utc = None;
    missed.active_unavailable_reason = Some(ActiveUnavailableReason::NotApplicableForRowKind);
    missed.resolution_poll_result = Some(ResolutionPollResult::MissedDueToDowntime);
    missed.resolution_transport_error_class = None;
    missed.response_fields_unavailable_reason =
        Some(ResponseFieldsUnavailableReason::MissedDueToDowntime);

    let mut post = decoded_gamma_attempt_v3();
    post.resolution_poll_result = Some(ResolutionPollResult::PostTerminalClaimObservation);
    post.terminal_claim_owner = Some(TerminalClaimOwner::MarketResolvedWs);

    let mut no_valid_summary = resolution_followup_summary_v3();
    no_valid_summary.resolution_followup_status =
        Some(ResolutionFollowupStatus::BudgetExhaustedWithoutValidResponse);
    no_valid_summary.resolution_valid_response_count = Some(0);
    no_valid_summary.resolution_transport_unavailable_count = Some(4);
    no_valid_summary.resolution_missed_slot_count = Some(2);
    no_valid_summary.last_valid_metadata_received_utc = None;

    let mut late_end_transition = end_transition.clone();
    late_end_transition.fixed_identity_verified_at_utc =
        late_transition.fixed_identity_verified_at_utc;
    late_end_transition.source_evidence_received_at_utc =
        late_transition.source_evidence_received_at_utc;
    late_end_transition.source_evidence_raw_sha256 =
        late_transition.source_evidence_raw_sha256.clone();
    validate_polymarket_metadata_v3_local_references(&[
        PolymarketMetadataV3ReferenceRecord::from(&late_transition),
        PolymarketMetadataV3ReferenceRecord::from(&late_end_transition),
    ])
    .expect("late candidate-to-active evidence must be the unique authority for its E transition");

    for (name, row) in [
        ("late_transition", late_transition),
        ("end_transition", end_transition),
        ("terminal_gamma", terminal_gamma),
        ("transport", transport),
        ("binding", binding),
        ("decode", decode),
        ("missed", missed),
        ("post_terminal", post),
        ("no_valid_summary", no_valid_summary),
    ] {
        let validation = row.validate();
        assert!(
            validation.is_complete(),
            "{name}: {:?}",
            validation.violations
        );
    }
}

#[test]
fn v3_validator_rejects_required_null_ownership_and_causal_time_violations() {
    let mut missing_admission = metadata_snapshot_v3();
    missing_admission.collector_admission_sequence = 0;

    let mut later_snapshot = metadata_snapshot_v3();
    later_snapshot.fixed_identity_verified_at_utc -= 1_000;

    let mut fixed_after_validation = metadata_snapshot_v3();
    fixed_after_validation.fixed_identity_verified_at_utc = fixed_after_validation
        .row_evidence_validation_completed_at_utc
        .unwrap()
        + 1;

    let mut bad_digest = metadata_snapshot_v3();
    bad_digest.source_evidence_raw_sha256 = Some("0".repeat(64));

    let mut source_after_admission = market_state_transition_v3();
    source_after_admission.source_evidence_received_at_utc =
        Some(source_after_admission.collector_admission_time_utc_ms + 1);

    let mut applied_after_admission = market_state_transition_v3();
    applied_after_admission.lifecycle_applied_at_utc =
        Some(applied_after_admission.collector_admission_time_utc_ms + 1);
    applied_after_admission.runtime_role_applied_at_utc =
        applied_after_admission.lifecycle_applied_at_utc;

    let mut fetch_before_slot = decoded_gamma_attempt_v3();
    fetch_before_slot.metadata_fetch_start_utc =
        Some(fetch_before_slot.resolution_poll_scheduled_utc.unwrap() - 1);

    let mut classification_before_fetch = decoded_gamma_attempt_v3();
    classification_before_fetch.metadata_fetch_start_utc =
        Some(classification_before_fetch.timestamp_received + 1);

    let mut transition_before_validation = terminal_gamma_attempt_v3();
    transition_before_validation.lifecycle_applied_at_utc =
        transition_before_validation.row_evidence_validation_completed_at_utc;
    transition_before_validation.runtime_role_applied_at_utc =
        transition_before_validation.lifecycle_applied_at_utc;
    transition_before_validation.row_evidence_validation_completed_at_utc =
        transition_before_validation
            .lifecycle_applied_at_utc
            .map(|applied| applied + 1);

    let mut post_with_second_winner = decoded_gamma_attempt_v3();
    post_with_second_winner.resolution_poll_result =
        Some(ResolutionPollResult::PostTerminalClaimObservation);
    post_with_second_winner.terminal_claim_owner = Some(TerminalClaimOwner::MarketResolvedWs);
    post_with_second_winner.winning_asset_id = Some("101".to_owned());

    let mut post_with_integrity_failure = decoded_gamma_attempt_v3();
    post_with_integrity_failure.resolution_poll_result =
        Some(ResolutionPollResult::PostTerminalClaimObservation);
    post_with_integrity_failure.terminal_claim_owner = Some(TerminalClaimOwner::MarketResolvedWs);
    post_with_integrity_failure.terminal_predicate_failure_reason =
        Some(TerminalPredicateFailureReason::SlugMismatch);

    let mut bad_summary = resolution_followup_summary_v3();
    bad_summary.resolution_poll_attempts_recorded = Some(5);

    let mut closed_time_mismatch = terminal_gamma_attempt_v3();
    closed_time_mismatch.closed_time_raw = Some("2026-07-07T02:50:17Z".to_owned());

    let mut parse_error_with_valid_raw = terminal_gamma_attempt_v3();
    parse_error_with_valid_raw.closed_time_check_passed = Some(false);
    parse_error_with_valid_raw.closed_time_check_failure_reason =
        Some(ClosedTimeCheckFailureReason::ParseError);
    parse_error_with_valid_raw.closed_time_utc = None;

    let mut ws_without_platform_timestamp = resolution_terminal_ws_v3();
    ws_without_platform_timestamp.ws_platform_timestamp_raw = None;
    ws_without_platform_timestamp.ws_platform_timestamp_utc = None;

    let mut ws_platform_timestamp_mismatch = resolution_terminal_ws_v3();
    ws_platform_timestamp_mismatch.ws_platform_timestamp_raw = Some("1783392840000".to_owned());

    let mut summary_before_horizon = resolution_followup_summary_v3();
    summary_before_horizon.runtime_role_applied_at_utc = summary_before_horizon
        .resolution_budget_horizon_utc
        .map(|horizon| horizon - 1);

    let mut summary_with_future_last_valid = resolution_followup_summary_v3();
    summary_with_future_last_valid.last_valid_metadata_received_utc =
        summary_with_future_last_valid
            .runtime_role_applied_at_utc
            .map(|applied| applied + 1);

    let mut summary_applied_after_timestamp = resolution_followup_summary_v3();
    summary_applied_after_timestamp.runtime_role_applied_at_utc =
        Some(summary_applied_after_timestamp.timestamp_received + 1);
    summary_applied_after_timestamp.collector_admission_time_utc_ms =
        summary_applied_after_timestamp.timestamp_received + 2;

    for (name, row, expected) in [
        (
            "missing_admission",
            missing_admission,
            "collector_admission_sequence:required",
        ),
        (
            "fixed_after_validation",
            fixed_after_validation,
            "metadata_snapshot:fixed_identity_after_row_validation",
        ),
        (
            "bad_digest",
            bad_digest,
            "source_evidence_raw_sha256:payload_mismatch",
        ),
        (
            "source_after_admission",
            source_after_admission,
            "source_evidence_received_at_utc:after_admission",
        ),
        (
            "applied_after_admission",
            applied_after_admission,
            "lifecycle_applied_at_utc:after_admission",
        ),
        (
            "fetch_before_slot",
            fetch_before_slot,
            "metadata_fetch_start_utc:before_poll_scheduled",
        ),
        (
            "classification_before_fetch",
            classification_before_fetch,
            "timestamp_received:before_metadata_fetch_start",
        ),
        (
            "transition_before_validation",
            transition_before_validation,
            "terminal_transition:validation_after_applied",
        ),
        (
            "post_with_second_winner",
            post_with_second_winner,
            "winning_asset_id:must_be_null",
        ),
        (
            "post_with_integrity_failure",
            post_with_integrity_failure,
            "terminal_predicate_failure_reason:post_terminal_attempt",
        ),
        (
            "bad_summary",
            bad_summary,
            "resolution_followup_summary:counters_not_six",
        ),
        (
            "closed_time_mismatch",
            closed_time_mismatch,
            "closed_time_utc:raw_mismatch",
        ),
        (
            "parse_error_with_valid_raw",
            parse_error_with_valid_raw,
            "closed_time_raw:unexpected_parse_success",
        ),
        (
            "ws_without_platform_timestamp",
            ws_without_platform_timestamp,
            "ws_platform_timestamp_raw:required",
        ),
        (
            "ws_platform_timestamp_mismatch",
            ws_platform_timestamp_mismatch,
            "ws_platform_timestamp_utc:raw_mismatch",
        ),
        (
            "summary_before_horizon",
            summary_before_horizon,
            "resolution_followup_summary:applied_before_budget_horizon",
        ),
        (
            "summary_with_future_last_valid",
            summary_with_future_last_valid,
            "resolution_followup_summary:last_valid_after_role_exit",
        ),
        (
            "summary_applied_after_timestamp",
            summary_applied_after_timestamp,
            "resolution_followup_summary:applied_after_timestamp_received",
        ),
    ] {
        let validation = row.validate();
        assert!(
            validation
                .violations
                .iter()
                .any(|violation| violation == expected),
            "{name}: expected {expected}, got {:?}",
            validation.violations
        );
    }

    assert!(
        later_snapshot.validate().is_complete(),
        "later snapshots retain the first fixed-identity time"
    );
}

#[test]
fn terminal_gamma_status_is_trimmed_ascii_case_insensitive_and_has_no_future_skew_gate() {
    let mut far_future_closed_time = terminal_gamma_attempt_v3();
    far_future_closed_time.closed_time_raw = Some("2026-07-08T02:50:00Z".to_owned());
    far_future_closed_time.closed_time_utc = Some(E + 86_400_000);
    assert!(
        far_future_closed_time.validate().is_complete(),
        "RF-09 deliberately has no closedTime-to-receive future-skew bound"
    );

    let mut invalid_status = terminal_gamma_attempt_v3();
    invalid_status.uma_resolution_status = Some("proposed".to_owned());
    assert!(
        invalid_status
            .validate()
            .violations
            .contains(&"uma_resolution_status:terminal_gamma_attempt".to_owned())
    );
}
