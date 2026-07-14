use polymarket_client_sdk_v2::rtds::{Subscription, SubscriptionRequest};
use serde_json::Value;

use crate::event::{EventEnvelope, SourceKind};
use crate::health::{CoverageVerdict, FreshnessStatus};
use crate::source_format::RtdsChainlinkBtcUsdRaw;

pub const RTDS_WS_ENDPOINT: &str = "wss://ws-live-data.polymarket.com";
pub const RTDS_CHAINLINK_TOPIC: &str = "crypto_prices_chainlink";
pub const BTC_USD_SYMBOL: &str = "btc/usd";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtdsSubscriptionTrace {
    pub raw_json: String,
    pub action: String,
    pub topic: String,
    pub msg_type: String,
    pub filters: String,
}

pub fn build_chainlink_btc_usd_subscription_trace() -> Result<RtdsSubscriptionTrace, String> {
    let subscription = Subscription::chainlink_prices(Some(BTC_USD_SYMBOL.to_owned()));
    let request = SubscriptionRequest::subscribe(vec![subscription]);
    let raw_json =
        serde_json::to_string(&request).map_err(|err| format!("serialize_rtds_request:{err}"))?;
    rtds_subscription_trace_from_json(&raw_json)
}

pub fn rtds_subscription_trace_from_json(raw_json: &str) -> Result<RtdsSubscriptionTrace, String> {
    let value = serde_json::from_str::<Value>(raw_json)
        .map_err(|err| format!("parse_rtds_subscription_json:{err}"))?;
    let action = value
        .get("action")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing_action".to_owned())?
        .to_owned();
    let subscription = if let Some(subscriptions) = value.get("subscriptions") {
        subscriptions
            .as_array()
            .and_then(|items| items.first())
            .ok_or_else(|| "missing_subscription_item".to_owned())?
    } else {
        &value
    };
    let topic = subscription
        .get("topic")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing_topic".to_owned())?
        .to_owned();
    let msg_type = subscription
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing_type".to_owned())?
        .to_owned();
    let filters = subscription
        .get("filters")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing_filters".to_owned())?
        .to_owned();

    Ok(RtdsSubscriptionTrace {
        raw_json: raw_json.to_owned(),
        action,
        topic,
        msg_type,
        filters,
    })
}

impl RtdsSubscriptionTrace {
    pub fn validate_chainlink_btc_usd(&self) -> Result<(), String> {
        if self.action != "subscribe" {
            return Err("rtds_action_mismatch".to_owned());
        }
        if self.topic != RTDS_CHAINLINK_TOPIC {
            return Err("rtds_topic_mismatch".to_owned());
        }
        if self.msg_type != "*" {
            return Err("rtds_type_mismatch".to_owned());
        }
        let filters = serde_json::from_str::<Value>(&self.filters)
            .map_err(|err| format!("parse_rtds_filter_json:{err}"))?;
        if filters.get("symbol").and_then(Value::as_str) != Some(BTC_USD_SYMBOL) {
            return Err("rtds_symbol_filter_mismatch".to_owned());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtdsReconnectValidation {
    pub topic_revalidated: bool,
    pub symbol_revalidated: bool,
    pub payload_timestamp_present: bool,
    pub freshness_status: FreshnessStatus,
    pub coverage_verdict: CoverageVerdict,
    pub source_gap: Option<String>,
}

pub fn validate_reconnect_business_freshness(
    trace: &RtdsSubscriptionTrace,
    payload_timestamp_ms: Option<i64>,
    last_business_recv_time_utc_ms: Option<i64>,
    now_utc_ms: i64,
    freshness_timeout_ms: i64,
) -> RtdsReconnectValidation {
    let subscription_ok = trace.validate_chainlink_btc_usd().is_ok();
    let payload_timestamp_present = payload_timestamp_ms.is_some_and(|ts| ts > 0);
    let business_fresh = last_business_recv_time_utc_ms
        .is_some_and(|last| now_utc_ms.saturating_sub(last) <= freshness_timeout_ms);

    if subscription_ok && payload_timestamp_present && business_fresh {
        RtdsReconnectValidation {
            topic_revalidated: true,
            symbol_revalidated: true,
            payload_timestamp_present: true,
            freshness_status: FreshnessStatus::Healthy,
            coverage_verdict: CoverageVerdict::Complete,
            source_gap: None,
        }
    } else {
        RtdsReconnectValidation {
            topic_revalidated: subscription_ok,
            symbol_revalidated: subscription_ok,
            payload_timestamp_present,
            freshness_status: FreshnessStatus::Stale,
            coverage_verdict: CoverageVerdict::Stale,
            source_gap: Some("rtds_business_freshness_not_revalidated".to_owned()),
        }
    }
}

pub fn build_chainlink_raw_event(
    envelope: EventEnvelope,
    message_timestamp_ms: i64,
    payload_timestamp_ms: i64,
    symbol: impl Into<String>,
    value: impl Into<String>,
    trace: &RtdsSubscriptionTrace,
    ack_or_first_message_evidence: Option<String>,
) -> Result<RtdsChainlinkBtcUsdRaw, String> {
    if envelope.source_kind != SourceKind::PolymarketRtdsChainlinkBtcUsd {
        return Err("envelope_source_kind_mismatch".to_owned());
    }
    trace.validate_chainlink_btc_usd()?;
    let age_ms = envelope
        .recv_time_utc_ms
        .and_then(|received| received.checked_sub(payload_timestamp_ms));
    let freshness_status = if age_ms.is_some_and(|age| (-5_000..=30_000).contains(&age)) {
        "fresh"
    } else {
        "stale"
    };
    Ok(RtdsChainlinkBtcUsdRaw {
        envelope,
        message_timestamp_ms,
        payload_timestamp_ms,
        symbol: symbol.into(),
        value: value.into(),
        source_topic: trace.topic.clone(),
        message_type: "update".to_owned(),
        age_ms,
        freshness_status: freshness_status.to_owned(),
        subscription_raw_json: trace.raw_json.clone(),
        ack_or_first_message_evidence,
        close_or_error_evidence: None,
    })
}
