use super::*;
use crate::polymarket_market_metadata_v6_layout::{
    MarketAnchorRuleUpdateRow, PolymarketMarketMetadataV6Row,
};
use data_pipeline_core::public_input::{
    MarketAnchorOriginV1, MarketAnchorQueryResultV1, MarketAnchorRuleUpdateInput,
    PolymarketMarketMetadataV6Input, PolymarketRtdsChainlinkTwapInput, TypedRawRowV2,
};
use data_pipeline_core::typed_raw as public;

// 原冷写入器的来源形成函数；冷热调用同一解析和原验证。
pub(super) fn binance_depth_snapshot_row_from_event(
    event: &RawEvent,
) -> Result<BinanceDepthSnapshotHourlyRow> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let snapshot_fetch_start_utc = required_i64(&value, "snapshot_fetch_start_utc")?;
    let raw = parse_depth_snapshot_raw(
        event_envelope(event, SourceKind::BinanceDepthSnapshot),
        &value,
        snapshot_fetch_start_utc,
        event.recv_time_utc_ms,
        event.source_endpoint.clone(),
    )
    .map_err(invalid_data)?;
    BinanceDepthSnapshotHourlyRow::from_raw_with_ordering(
        &raw,
        binance_depth_ordering_from_event(event, &value)?,
    )
    .map_err(|error| invalid_data(error).into())
}

pub(super) fn binance_depth_update_row_from_event(
    event: &RawEvent,
) -> Result<BinanceDepthUpdateHourlyRow> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let raw = parse_depth_update_raw(
        event_envelope(event, SourceKind::BinanceDepthUpdate),
        &value,
    )
    .map_err(invalid_data)?;
    let stitch_verdict = required_value_string(&value, "stitch_verdict")?;
    let verdict = DepthUpdateVerdict::try_from(stitch_verdict.as_str()).map_err(invalid_data)?;
    let assessment = DepthUpdateAssessment {
        snapshot_last_update_id: value.get("snapshot_last_update_id").and_then(Value::as_u64),
        local_u_before: value.get("local_u_before").and_then(Value::as_u64),
        verdict,
        local_u_after: value.get("local_u_after").and_then(Value::as_u64),
        stitch_evidence: optional_string(&value, "stitch_evidence"),
        gap_reason: optional_string(&value, "stitch_gap_reason"),
    };
    assessment.validate_contract().map_err(invalid_data)?;
    BinanceDepthUpdateHourlyRow::from_assessment(
        &raw,
        &assessment,
        binance_depth_ordering_from_event(event, &value)?,
    )
    .map_err(|error| invalid_data(error).into())
}

fn binance_depth_ordering_from_event(
    event: &RawEvent,
    value: &Value,
) -> Result<BinanceDepthOrdering> {
    let admission = event.collector_admission.as_ref().ok_or_else(|| {
        invalid_data("Binance depth event reached writer without collector admission")
    })?;
    let ordering = BinanceDepthOrdering {
        source_sequence_epoch_id: required_value_string(value, "source_sequence_epoch_id")?,
        connection_generation: value
            .get("connection_generation")
            .and_then(Value::as_u64)
            .filter(|generation| *generation > 0)
            .ok_or_else(|| invalid_data("Binance depth connection_generation missing or zero"))?,
        depth_ingress_sequence: value
            .get("depth_ingress_sequence")
            .and_then(Value::as_u64)
            .filter(|sequence| *sequence > 0)
            .ok_or_else(|| invalid_data("Binance depth ingress sequence missing or zero"))?,
        collector_admission_sequence: admission.sequence,
        collector_admission_time_utc_ms: admission.time_utc_ms,
    };
    ordering.validate_contract().map_err(invalid_data)?;
    Ok(ordering)
}

pub(super) fn binance_aggtrade_row_from_event(
    event: &RawEvent,
) -> Result<BinanceAggTradeHourlyRow> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let record_kind = required_value_string(&value, "record_kind")?;
    let timestamp_received = required_i64(&value, "timestamp_received")?;
    if timestamp_received != event.recv_time_utc_ms {
        return Err(invalid_data("aggTrade raw/event receive time mismatch").into());
    }
    let admission = event.collector_admission.as_ref().ok_or_else(|| {
        invalid_data("binance aggTrade event reached writer without collector admission")
    })?;
    let ingest_source = required_value_string(&value, "ingest_source")?;
    let rest_backfill_timestamp_received = optional_i64(&value, "rest_backfill_timestamp_received");
    let row = BinanceAggTradeHourlyRow {
        record_kind,
        timestamp_received,
        rest_backfill_timestamp_received,
        ingest_source,
        event_type: optional_string(&value, "event_type"),
        event_time: optional_i64(&value, "event_time"),
        trade_time: optional_i64(&value, "trade_time"),
        symbol: optional_string(&value, "symbol"),
        aggregate_trade_id: value.get("aggregate_trade_id").and_then(Value::as_u64),
        price: optional_string(&value, "price"),
        quantity: optional_string(&value, "quantity"),
        first_trade_id: value.get("first_trade_id").and_then(Value::as_u64),
        last_trade_id: value.get("last_trade_id").and_then(Value::as_u64),
        buyer_is_maker: optional_bool(&value, "buyer_is_maker"),
        stream: optional_string(&value, "stream"),
        ws_ignore: optional_bool(&value, "ws_ignore"),
        rest_was_best_price_match: optional_bool(&value, "rest_was_best_price_match"),
        source_native_payload_json: required_value_string(&value, "source_native_payload_json")?,
        source_sequence_epoch_id: required_value_string(&value, "source_sequence_epoch_id")?,
        connection_generation: value
            .get("connection_generation")
            .and_then(Value::as_u64)
            .filter(|generation| *generation != 0)
            .ok_or_else(|| invalid_data("aggTrade connection_generation missing or zero"))?,
        websocket_ingress_sequence: value
            .get("websocket_ingress_sequence")
            .and_then(Value::as_u64),
        rest_response_ingress_sequence: value
            .get("rest_response_ingress_sequence")
            .and_then(Value::as_u64),
        rest_response_item_index: value
            .get("rest_response_item_index")
            .and_then(Value::as_u64),
        collector_admission_sequence: admission.sequence,
        collector_admission_time_utc_ms: admission.time_utc_ms,
        invalid_reason_code: optional_string(&value, "invalid_reason_code"),
        invalid_reason_detail: optional_string(&value, "invalid_reason_detail"),
        payload_encoding: optional_string(&value, "payload_encoding"),
        payload_length_bytes: value.get("payload_length_bytes").and_then(Value::as_u64),
        payload_sha256: optional_string(&value, "payload_sha256"),
        payload_capture_status: optional_string(&value, "payload_capture_status"),
        collector_host_id: event.collector_host_id.clone(),
        collector_region: event.collector_region.clone(),
        source_endpoint: event.source_endpoint.clone(),
        schema_version: BINANCE_AGGTRADE_SCHEMA_VERSION.to_owned(),
    };
    let validation = row.validate();
    if !validation.is_complete() {
        return Err(invalid_data(format!(
            "Binance aggTrade row missing fields: {:?}",
            validation.missing_fields
        ))
        .into());
    }
    Ok(row)
}

pub(super) fn rtds_row_from_event(event: &RawEvent) -> Result<RtdsChainlinkBtcUsdHourlyRow> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let row = rtds_common_row_from_event(event, &value)?;
    let validation = row.validate();
    if !validation.is_complete() {
        return Err(invalid_data(format!(
            "RTDS row missing fields: {:?}",
            validation.missing_fields
        ))
        .into());
    }
    Ok(row)
}

fn rtds_common_row_from_event(
    event: &RawEvent,
    value: &Value,
) -> Result<RtdsChainlinkBtcUsdHourlyRow> {
    Ok(RtdsChainlinkBtcUsdHourlyRow {
        timestamp_received: event.recv_time_utc_ms,
        message_timestamp: required_i64(value, "message_timestamp")?,
        payload_timestamp: required_i64(value, "payload_timestamp")?,
        symbol: required_value_string(value, "symbol")?,
        value: required_value_string(value, "value")?,
        topic: required_value_string(value, "topic")?,
        message_type: required_value_string(value, "type")?,
        age_ms: optional_i64(value, "age_ms"),
        freshness_status: required_value_string(value, "freshness_status")?,
        subscription_filter: value
            .get("subscription_raw_json")
            .and_then(Value::as_str)
            .unwrap_or_else(|| rtds_subscription_evidence_json())
            .to_owned(),
        source_native_payload_json: required_value_string(value, "source_native_payload_json")?,
        collector_host_id: event.collector_host_id.clone(),
        collector_region: event.collector_region.clone(),
        source_endpoint: event.source_endpoint.clone(),
        schema_version: "polymarket_rtds_chainlink_btc_usd_v4".to_owned(),
    })
}

pub(super) fn rtds_twap_row_from_event(event: &RawEvent) -> Result<RtdsChainlinkTwapHourlyRow> {
    let value: Value = serde_json::from_str(&event.raw_json)?;
    let payload = value
        .get("payload")
        .ok_or_else(|| invalid_data("TWAP payload missing"))?;
    let admission = event
        .collector_admission
        .as_ref()
        .ok_or_else(|| invalid_data("TWAP collector admission missing"))?;
    let mut common = rtds_common_row_from_event(event, &value)?;
    common.schema_version = RTDS_CHAINLINK_TWAP_SCHEMA_VERSION.to_owned();
    let row = RtdsChainlinkTwapHourlyRow {
        common,
        full_accuracy_value: required_value_string(payload, "full_accuracy_value")?,
        window_s: payload
            .get("window_s")
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid_data("TWAP window_s missing"))?,
        source_sequence_epoch_id: required_value_string(&value, "source_sequence_epoch_id")?,
        collector_admission_sequence: admission.sequence,
        collector_admission_time_utc_ms: admission.time_utc_ms,
    };
    let validation = row.validate();
    if !validation.is_complete() {
        return Err(invalid_data(format!(
            "RTDS TWAP row missing fields: {:?}",
            validation.missing_fields
        ))
        .into());
    }
    Ok(row)
}

pub(super) fn required_value_string(value: &Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid_data(format!("{field} missing")).into())
}

pub(super) fn optional_string(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

pub(super) fn optional_bool(value: &Value, field: &str) -> Option<bool> {
    value.get(field).and_then(Value::as_bool)
}

pub(super) fn metadata_unavailable_reason(value: &Value) -> Option<String> {
    match value.get("metadata_unavailable_reason") {
        Some(Value::String(reason)) if !reason.trim().is_empty() => Some(reason.clone()),
        Some(Value::Array(reasons)) if !reasons.is_empty() => {
            let reasons = reasons
                .iter()
                .map(json_scalar_to_string)
                .collect::<Result<Vec<_>>>()
                .ok()?;
            Some(reasons.join(";"))
        }
        Some(other) if !other.is_null() => Some(other.to_string()),
        _ => None,
    }
}

pub(super) fn required_i64(value: &Value, field: &str) -> Result<i64> {
    optional_i64(value, field).ok_or_else(|| invalid_data(format!("{field} missing")).into())
}

pub(super) fn optional_i64(value: &Value, field: &str) -> Option<i64> {
    value.get(field).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
    })
}

pub(super) fn json_scalar_to_string(value: &Value) -> Result<String> {
    match value {
        Value::String(value) if !value.trim().is_empty() => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        _ => Err(invalid_data("expected string or number JSON scalar").into()),
    }
}

pub(super) fn raw_event_to_public_input(event: &RawEvent) -> Result<Option<TypedRawRowV2>> {
    let row = match event.source_kind.as_str() {
        "polymarket_market_metadata" => {
            let row = polymarket_metadata_v6_row_for_event(event)?;
            let input = match row {
                PolymarketMarketMetadataV6Row::Existing(row) => {
                    PolymarketMarketMetadataV6Input::Existing(metadata_public_row(row)?)
                }
                PolymarketMarketMetadataV6Row::AnchorRule(row) => {
                    PolymarketMarketMetadataV6Input::AnchorRule(anchor_public_row(row))
                }
            };
            return Ok(Some(TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
                input,
            ))));
        }
        "polymarket_rtds_chainlink_twap" => {
            let row = rtds_twap_row_from_event(event)?;
            return Ok(Some(TypedRawRowV2::PolymarketRtdsChainlinkTwap(
                PolymarketRtdsChainlinkTwapInput {
                    common: rtds_public_row(row.common)?,
                    full_accuracy_value: row.full_accuracy_value,
                    window_s: row.window_s,
                    source_sequence_epoch_id: row.source_sequence_epoch_id,
                    collector_admission_sequence: row.collector_admission_sequence,
                    collector_admission_time_utc_ms: row.collector_admission_time_utc_ms,
                },
            )));
        }
        "polymarket_rtds_chainlink_btc_usd" => public::TypedRawRow::PolymarketRtdsChainlinkBtcUsd(
            rtds_public_row(rtds_row_from_event(event)?)?,
        ),
        "binance_depth_snapshot" => public::TypedRawRow::BinanceDepthSnapshot(snapshot_public_row(
            binance_depth_snapshot_row_from_event(event)?,
        )?),
        "binance_depth_update" => public::TypedRawRow::BinanceDepthUpdate(depth_public_row(
            binance_depth_update_row_from_event(event)?,
        )?),
        "binance_aggtrade" => public::TypedRawRow::BinanceAggTrade(aggtrade_public_row(
            binance_aggtrade_row_from_event(event)?,
        )?),
        other => {
            return Err(
                invalid_data(format!("unsupported training typed source kind: {other}")).into(),
            );
        }
    };
    Ok(Some(TypedRawRowV2::Existing(row)))
}

fn required_public<T>(value: Option<T>, field: &str) -> Result<T> {
    value.ok_or_else(|| invalid_data(format!("typed source field missing: {field}")).into())
}

fn rtds_public_row(
    row: RtdsChainlinkBtcUsdHourlyRow,
) -> Result<public::PolymarketRtdsChainlinkBtcUsdRow> {
    Ok(public::PolymarketRtdsChainlinkBtcUsdRow {
        timestamp_received: row.timestamp_received,
        message_timestamp: row.message_timestamp,
        payload_timestamp: row.payload_timestamp,
        age_ms: row.age_ms,
        symbol: row.symbol,
        value: row.value,
        topic: row.topic,
        message_type: row.message_type,
        freshness_status: row.freshness_status.as_str().try_into()?,
        subscription_filter: row.subscription_filter,
        source_native_payload_json: row.source_native_payload_json,
        collector_host_id: row.collector_host_id,
        collector_region: row.collector_region,
        source_endpoint: row.source_endpoint,
        schema_version: row.schema_version,
    })
}

fn snapshot_public_row(
    row: BinanceDepthSnapshotHourlyRow,
) -> Result<public::BinanceDepthSnapshotRow> {
    Ok(public::BinanceDepthSnapshotRow {
        timestamp_received: row.timestamp_received,
        snapshot_fetch_start_utc: row.snapshot_fetch_start_utc,
        last_update_id: row.last_update_id,
        bids: public::parse_depth_levels_json(&row.bids, "bids", false)?,
        asks: public::parse_depth_levels_json(&row.asks, "asks", false)?,
        symbol: row.symbol,
        source_endpoint: row.source_endpoint,
        snapshot_status: row.snapshot_status,
        source_sequence_epoch_id: required_public(
            row.source_sequence_epoch_id,
            "source_sequence_epoch_id",
        )?,
        connection_generation: required_public(row.connection_generation, "connection_generation")?,
        depth_ingress_sequence: required_public(
            row.depth_ingress_sequence,
            "depth_ingress_sequence",
        )?,
        collector_admission_sequence: required_public(
            row.collector_admission_sequence,
            "collector_admission_sequence",
        )?,
        collector_admission_time_utc_ms: required_public(
            row.collector_admission_time_utc_ms,
            "collector_admission_time_utc_ms",
        )?,
        collector_host_id: row.collector_host_id,
        collector_region: row.collector_region,
        schema_version: row.schema_version,
    })
}

fn depth_public_row(row: BinanceDepthUpdateHourlyRow) -> Result<public::BinanceDepthUpdateRow> {
    Ok(public::BinanceDepthUpdateRow {
        timestamp_received: row.timestamp_received,
        event_type: row.event_type,
        event_time: row.event_time,
        symbol: row.symbol,
        first_update_id: row.first_update_id,
        final_update_id: row.final_update_id,
        bids: public::parse_depth_levels_json(&row.bids, "bids", true)?,
        asks: public::parse_depth_levels_json(&row.asks, "asks", true)?,
        stream: row.stream,
        snapshot_last_update_id: row.snapshot_last_update_id,
        local_u_before: row.local_u_before,
        stitch_verdict: row.stitch_verdict.as_str().try_into()?,
        stitch_evidence: row.stitch_evidence,
        stitch_gap_reason: row.stitch_gap_reason,
        source_sequence_epoch_id: required_public(
            row.source_sequence_epoch_id,
            "source_sequence_epoch_id",
        )?,
        connection_generation: required_public(row.connection_generation, "connection_generation")?,
        depth_ingress_sequence: required_public(
            row.depth_ingress_sequence,
            "depth_ingress_sequence",
        )?,
        collector_admission_sequence: required_public(
            row.collector_admission_sequence,
            "collector_admission_sequence",
        )?,
        collector_admission_time_utc_ms: required_public(
            row.collector_admission_time_utc_ms,
            "collector_admission_time_utc_ms",
        )?,
        collector_host_id: row.collector_host_id,
        collector_region: row.collector_region,
        source_endpoint: row.source_endpoint,
        schema_version: row.schema_version,
    })
}

fn metadata_public_row(
    mut row: PolymarketMarketMetadataV5Row,
) -> Result<public::PolymarketMarketMetadataRow> {
    let ws_claimant_source_sequence_epoch_id = row.ws_claimant_source_sequence_epoch_id.take();
    let ws_claimant_collector_admission_sequence = row.ws_claimant_collector_admission_sequence;
    let row = row.into_common();
    Ok(public::PolymarketMarketMetadataRow {
        metadata_row_kind: row.metadata_row_kind.as_str().try_into()?,
        timestamp_received: row.timestamp_received,
        collector_admission_sequence: row.collector_admission_sequence,
        collector_admission_time_utc_ms: row.collector_admission_time_utc_ms,
        metadata_origin: row.metadata_origin.as_str().try_into()?,
        collector_host_id: row.collector_host_id,
        collector_region: row.collector_region,
        schema_version: row.schema_version,
        slug: row.slug,
        condition_id: row.condition_id,
        clob_market_id: row.clob_market_id,
        asset_ids: row.asset_ids,
        market_start_utc: row.market_start_utc,
        market_end_utc: row.market_end_utc,
        fixed_identity_verified_at_utc: row.fixed_identity_verified_at_utc,
        market_lifecycle_state: row.market_lifecycle_state.as_str().try_into()?,
        market_runtime_role: row.market_runtime_role.as_str().try_into()?,
        source_endpoint: row.source_endpoint,
        source_endpoint_unavailable_reason: row
            .source_endpoint_unavailable_reason
            .map(|value| value.as_str().try_into())
            .transpose()?,
        source_evidence_origin: row
            .source_evidence_origin
            .map(|value| value.as_str().try_into())
            .transpose()?,
        source_evidence_event_type: row
            .source_evidence_event_type
            .map(|value| value.as_str().try_into())
            .transpose()?,
        source_evidence_channel: row
            .source_evidence_channel
            .map(|value| value.as_str().try_into())
            .transpose()?,
        source_evidence_received_at_utc: row.source_evidence_received_at_utc,
        row_evidence_validation_completed_at_utc: row.row_evidence_validation_completed_at_utc,
        source_evidence_payload_json: row.source_evidence_payload_json,
        source_evidence_raw_sha256: row.source_evidence_raw_sha256,
        market_lifecycle_transition: row
            .market_lifecycle_transition
            .map(|value| value.as_str().try_into())
            .transpose()?,
        lifecycle_scheduled_at_utc: row.lifecycle_scheduled_at_utc,
        lifecycle_applied_at_utc: row.lifecycle_applied_at_utc,
        market_runtime_role_transition: row
            .market_runtime_role_transition
            .map(|value| value.as_str().try_into())
            .transpose()?,
        runtime_role_scheduled_at_utc: row.runtime_role_scheduled_at_utc,
        runtime_role_applied_at_utc: row.runtime_role_applied_at_utc,
        metadata_fetch_start_utc: row.metadata_fetch_start_utc,
        closed: row.closed,
        closed_time_raw: row.closed_time_raw,
        closed_time_utc: row.closed_time_utc,
        closed_time_check_passed: row.closed_time_check_passed,
        closed_time_check_failure_reason: row
            .closed_time_check_failure_reason
            .map(|value| value.as_str().try_into())
            .transpose()?,
        automatically_resolved: row.automatically_resolved,
        uma_resolution_status: row.uma_resolution_status,
        outcomes_raw: row.outcomes_raw,
        outcome_prices_raw: row.outcome_prices_raw,
        active: row.active,
        active_unavailable_reason: row
            .active_unavailable_reason
            .map(|value| value.as_str().try_into())
            .transpose()?,
        accepting_orders: row.accepting_orders,
        ws_platform_timestamp_raw: row.ws_platform_timestamp_raw,
        ws_platform_timestamp_utc: row.ws_platform_timestamp_utc,
        winning_asset_id: row.winning_asset_id,
        winning_outcome: row.winning_outcome,
        resolution_evidence_source: row
            .resolution_evidence_source
            .map(|value| value.as_str().try_into())
            .transpose()?,
        terminal_claim_owner: row
            .terminal_claim_owner
            .map(|value| value.as_str().try_into())
            .transpose()?,
        resolution_poll_attempt_ordinal: row.resolution_poll_attempt_ordinal,
        resolution_poll_scheduled_utc: row.resolution_poll_scheduled_utc,
        resolution_poll_result: row
            .resolution_poll_result
            .map(|value| value.as_str().try_into())
            .transpose()?,
        resolution_transport_error_class: row
            .resolution_transport_error_class
            .map(|value| value.as_str().try_into())
            .transpose()?,
        resolution_transport_http_status: row.resolution_transport_http_status,
        resolution_integrity_error_class: row
            .resolution_integrity_error_class
            .map(|value| value.as_str().try_into())
            .transpose()?,
        resolution_budget_horizon_utc: row.resolution_budget_horizon_utc,
        resolution_followup_status: row
            .resolution_followup_status
            .map(|value| value.as_str().try_into())
            .transpose()?,
        resolution_poll_attempts_recorded: row.resolution_poll_attempts_recorded,
        resolution_valid_response_count: row.resolution_valid_response_count,
        resolution_transport_unavailable_count: row.resolution_transport_unavailable_count,
        resolution_missed_slot_count: row.resolution_missed_slot_count,
        last_valid_metadata_received_utc: row.last_valid_metadata_received_utc,
        terminal_predicate_passed: row.terminal_predicate_passed,
        terminal_predicate_failure_reason: row
            .terminal_predicate_failure_reason
            .map(|value| value.as_str().try_into())
            .transpose()?,
        response_fields_unavailable_reason: row
            .response_fields_unavailable_reason
            .map(|value| value.as_str().try_into())
            .transpose()?,
        ws_claimant_source_sequence_epoch_id: ws_claimant_source_sequence_epoch_id,
        ws_claimant_collector_admission_sequence: ws_claimant_collector_admission_sequence,
    })
}

fn anchor_public_row(row: MarketAnchorRuleUpdateRow) -> MarketAnchorRuleUpdateInput {
    MarketAnchorRuleUpdateInput {
        schema_version: crate::polymarket_market_metadata_v6_layout::POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION.to_owned(),
        timestamp_received: row.timestamp_received,
        collector_admission_sequence: row.collector_admission_sequence,
        collector_admission_time_utc_ms: row.collector_admission_time_utc_ms,
        metadata_origin: match row.metadata_origin {
            crate::market_anchor::MarketAnchorOrigin::GammaEventBySlug => MarketAnchorOriginV1::GammaEventBySlug,
            crate::market_anchor::MarketAnchorOrigin::WebsiteCryptoPrice => MarketAnchorOriginV1::WebsiteCryptoPrice,
        },
        collector_host_id: row.collector_host_id,
        collector_region: row.collector_region,
        slug: row.identity.slug,
        condition_id: row.identity.condition_id,
        clob_market_id: row.identity.clob_market_id,
        asset_ids: row.identity.asset_ids,
        market_start_utc: row.identity.market_start_utc,
        market_end_utc: row.identity.market_end_utc,
        fixed_identity_verified_at_utc: row.identity.fixed_identity_verified_at_utc,
        market_lifecycle_state: row.market_lifecycle_state.as_str().to_owned(),
        market_runtime_role: row.market_runtime_role.as_str().to_owned(),
        source_endpoint: row.observation.http_request_url.clone(),
        metadata_fetch_start_utc: row.metadata_fetch_start_utc,
        anchor_query_source_epoch_id: row.anchor_query_source_epoch_id,
        anchor_query_ordinal: row.anchor_query_ordinal,
        anchor_query_result: match row.observation.anchor_query_result {
            crate::market_anchor::MarketAnchorQueryResult::Observed => MarketAnchorQueryResultV1::Observed,
            crate::market_anchor::MarketAnchorQueryResult::HttpError => MarketAnchorQueryResultV1::HttpError,
            crate::market_anchor::MarketAnchorQueryResult::DecodeError => MarketAnchorQueryResultV1::DecodeError,
            crate::market_anchor::MarketAnchorQueryResult::TransportError => MarketAnchorQueryResultV1::TransportError,
            crate::market_anchor::MarketAnchorQueryResult::Timeout => MarketAnchorQueryResultV1::Timeout,
        },
        anchor_event_id: row.observation.anchor_event_id,
        price_to_beat_decimal: row.observation.price_to_beat_decimal,
        price_to_beat_source_field: row.observation.price_to_beat_source_field,
        anchor_source_computed_at_utc: row.observation.anchor_source_computed_at_utc,
        crypto_market_config_id: row.observation.crypto_market_config_id,
        crypto_market_config_nested_id: row.observation.crypto_market_config_nested_id,
        crypto_market_config_asset: row.observation.crypto_market_config_asset,
        crypto_market_config_duration: row.observation.crypto_market_config_duration,
        twap_enabled: row.observation.twap_enabled,
        twap_lookback_seconds: row.observation.twap_lookback_seconds,
        resolution_source: row.observation.resolution_source,
        http_request_url: row.observation.http_request_url,
        http_response_url: row.observation.http_response_url,
        http_status_code: row.observation.http_status_code,
        http_received_at_utc: row.observation.http_received_at_utc,
        http_body_encoding: row.observation.http_body_encoding,
        http_body: row.observation.http_body,
        http_body_sha256: row.observation.http_body_sha256,
    }
}

pub(super) fn polymarket_public_input(
    message: PolymarketOrderbookV3Message,
) -> Result<TypedRawRowV2> {
    use public::PolymarketOrderbookEvent as E;
    let event = match message.event {
        PolymarketOrderbookV3Event::Book(row) => E::Book {
            asset_id: row.asset_id,
            bids: row
                .bids
                .into_iter()
                .map(|level| public::PolymarketPriceLevel {
                    price: level.price,
                    size: level.size,
                })
                .collect(),
            asks: row
                .asks
                .into_iter()
                .map(|level| public::PolymarketPriceLevel {
                    price: level.price,
                    size: level.size,
                })
                .collect(),
            last_trade_price: row.last_trade_price,
        },
        PolymarketOrderbookV3Event::PriceChange(row) => E::PriceChange {
            price_changes: row
                .price_changes
                .into_iter()
                .map(|row| {
                    Ok(public::PolymarketPriceChangeEntry {
                        asset_id: row.asset_id,
                        price: row.price,
                        size: row.size,
                        side: row.side.as_str().try_into()?,
                        best_bid: row.best_bid,
                        best_ask: row.best_ask,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        },
        PolymarketOrderbookV3Event::TickSizeChange(row) => E::TickSizeChange {
            asset_id: row.asset_id,
            old_tick_size: row.old_tick_size,
            new_tick_size: row.new_tick_size,
        },
        PolymarketOrderbookV3Event::LastTradePrice(row) => E::LastTradePrice {
            asset_id: row.asset_id,
            price: row.price,
            size: row.size,
            side: row
                .side
                .map(|value| value.as_str().try_into())
                .transpose()?,
            fee_rate_bps: row.fee_rate_bps,
        },
        PolymarketOrderbookV3Event::BestBidAsk(row) => E::BestBidAsk {
            asset_id: row.asset_id,
            best_bid: row.best_bid,
            best_ask: row.best_ask,
            spread: row.spread,
        },
        PolymarketOrderbookV3Event::NewMarket(row) => E::NewMarket {
            clob_market_id: row.clob_market_id,
            question: row.question,
            description: row.description,
            asset_ids: row.asset_ids,
            outcomes: row.outcomes,
            event_message: row.event_message.map(|row| public::PolymarketEventMessage {
                id: row.id,
                ticker: row.ticker,
                slug: row.slug,
                title: row.title,
                description: row.description,
            }),
        },
        PolymarketOrderbookV3Event::MarketResolved(row) => E::MarketResolved {
            clob_market_id: row.clob_market_id,
            question: row.question,
            description: row.description,
            asset_ids: row.asset_ids,
            outcomes: row.outcomes,
            event_message: row.event_message.map(|row| public::PolymarketEventMessage {
                id: row.id,
                ticker: row.ticker,
                slug: row.slug,
                title: row.title,
                description: row.description,
            }),
            winning_asset_id: row.winning_asset_id,
            winning_outcome: row.winning_outcome,
        },
    };
    let row = message.envelope;
    Ok(TypedRawRowV2::Existing(
        public::TypedRawRow::PolymarketOrderbook(public::PolymarketOrderbookRow {
            timestamp_received: row.timestamp_received,
            timestamp: row.timestamp,
            market: row.market,
            slug: row.slug,
            collector_host_id: row.collector_host_id,
            collector_region: row.collector_region,
            source_endpoint: row.source_endpoint,
            schema_version: row.schema_version,
            source_sequence_epoch_id: row.source_sequence_epoch_id,
            collector_admission_sequence: row.collector_admission_sequence,
            collector_admission_time_utc_ms: row.collector_admission_time_utc_ms,
            event: event,
        }),
    ))
}

fn aggtrade_public_row(row: BinanceAggTradeHourlyRow) -> Result<public::BinanceAggTradeRow> {
    let record = match row.record_kind.as_str() {
        "trade" => public::BinanceAggTradeRecord::Trade {
            event_type: required_public(row.event_type, "event_type")?,
            event_time: required_public(row.event_time, "event_time")?,
            trade_time: required_public(row.trade_time, "trade_time")?,
            symbol: required_public(row.symbol, "symbol")?,
            aggregate_trade_id: required_public(row.aggregate_trade_id, "aggregate_trade_id")?,
            price: required_public(row.price, "price")?,
            quantity: required_public(row.quantity, "quantity")?,
            first_trade_id: required_public(row.first_trade_id, "first_trade_id")?,
            last_trade_id: required_public(row.last_trade_id, "last_trade_id")?,
            buyer_is_maker: required_public(row.buyer_is_maker, "buyer_is_maker")?,
            stream: required_public(row.stream, "stream")?,
            ws_ignore: required_public(row.ws_ignore, "ws_ignore")?,
        },
        "invalid_input" => public::BinanceAggTradeRecord::InvalidInput {
            invalid_reason_code: required_public(row.invalid_reason_code, "invalid_reason_code")?
                .as_str()
                .try_into()?,
            invalid_reason_detail: row.invalid_reason_detail,
            payload_encoding: row
                .payload_encoding
                .map(|value| value.as_str().try_into())
                .transpose()?,
            payload_length_bytes: row.payload_length_bytes,
            payload_sha256: row.payload_sha256,
            payload_capture_status: required_public(
                row.payload_capture_status,
                "payload_capture_status",
            )?
            .as_str()
            .try_into()?,
        },
        other => {
            return Err(invalid_data(format!("unsupported aggTrade record kind: {other}")).into());
        }
    };
    Ok(public::BinanceAggTradeRow {
        timestamp_received: row.timestamp_received,
        rest_backfill_timestamp_received: row.rest_backfill_timestamp_received,
        ingest_source: row.ingest_source,
        rest_was_best_price_match: row.rest_was_best_price_match,
        source_native_payload_json: row.source_native_payload_json,
        source_sequence_epoch_id: row.source_sequence_epoch_id,
        connection_generation: row.connection_generation,
        websocket_ingress_sequence: required_public(
            row.websocket_ingress_sequence,
            "websocket_ingress_sequence",
        )?,
        rest_response_ingress_sequence: row.rest_response_ingress_sequence,
        rest_response_item_index: row.rest_response_item_index,
        collector_admission_sequence: row.collector_admission_sequence,
        collector_admission_time_utc_ms: row.collector_admission_time_utc_ms,
        collector_host_id: row.collector_host_id,
        collector_region: row.collector_region,
        source_endpoint: row.source_endpoint,
        schema_version: row.schema_version,
        record: record,
    })
}
