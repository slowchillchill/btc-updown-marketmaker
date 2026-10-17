use super::fixture_input::{DOWN_TOKEN_ID, UP_TOKEN_ID};
use contracts::*;
use data_pipeline_core::typed_raw::*;

pub(super) const TERMINAL_RECEIVED_TIME_MS: i64 = 31_001;

pub(super) const TERMINAL_ADMITTED_TIME_MS: i64 = 31_003;

pub(super) const TERMINAL_SOURCE_TIME_MS: i64 = 31_000;

pub(super) const WS_EPOCH: &str = "epoch-a";

pub(super) const WS_ADMISSION_SEQUENCE: u64 = 50;

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

pub(super) fn shift_terminal_times(value: &mut serde_json::Value) {
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

pub(super) fn shift_window_times(value: &mut serde_json::Value, delta: i64) {
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
