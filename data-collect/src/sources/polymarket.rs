use std::collections::BTreeSet;

use crate::ValidationVerdict;
use crate::market_discovery::build_market_subscription_request_json;
use crate::source_format::{
    MarketLifecycleState, PolymarketMarketResolutionRaw, PolymarketSubscriptionEvidence,
    SubscriptionConfirmation,
};

pub const MARKET_WS_ENDPOINT: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
pub const T30_PREFETCH_MS: i64 = 30_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMarketAdapter {
    pub sdk_crate: String,
    pub sdk_support_status: String,
    pub market_ws_endpoint: String,
}

impl Default for PolymarketMarketAdapter {
    fn default() -> Self {
        Self {
            sdk_crate: "polymarket_client_sdk_v2".to_owned(),
            sdk_support_status: "supported_by_official_sdk".to_owned(),
            market_ws_endpoint: MARKET_WS_ENDPOINT.to_owned(),
        }
    }
}

impl PolymarketMarketAdapter {
    pub fn supported_market_event_types(&self) -> BTreeSet<&'static str> {
        [
            "book",
            "price_change",
            "last_trade_price",
            "best_bid_ask",
            "tick_size_change",
            "new_market",
            "market_resolved",
        ]
        .into_iter()
        .collect()
    }

    pub fn subscription_evidence(
        &self,
        asset_ids: &[String],
        confirmation: SubscriptionConfirmation,
    ) -> Result<PolymarketSubscriptionEvidence, String> {
        let raw_json = build_market_subscription_request_json(asset_ids)?;
        Ok(PolymarketSubscriptionEvidence {
            raw_json,
            assets_ids: asset_ids.to_vec(),
            custom_feature_enabled: true,
            confirmation,
            sdk_support_status: self.sdk_support_status.clone(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketDiscoveryEvidence {
    pub candidate_slug: String,
    pub condition_id: Option<String>,
    pub asset_ids: Vec<String>,
    pub active_scan_evidence: Option<String>,
    pub metadata_snapshot_evidence: Option<String>,
    pub metadata_snapshot_recv_time_utc_ms: Option<i64>,
    pub freshness_deadline_utc_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveryCompleteness {
    pub complete: bool,
    pub source_gap: Option<String>,
}

impl PolymarketDiscoveryEvidence {
    pub fn completeness(&self, now_utc_ms: i64) -> DiscoveryCompleteness {
        if self.asset_ids.is_empty() {
            return DiscoveryCompleteness {
                complete: false,
                source_gap: Some("asset_ids_missing".to_owned()),
            };
        }
        if self.condition_id.as_deref().is_none_or(str::is_empty) {
            return DiscoveryCompleteness {
                complete: false,
                source_gap: Some("condition_id_missing".to_owned()),
            };
        }
        if self.active_scan_evidence.is_none() && self.metadata_snapshot_evidence.is_none() {
            return DiscoveryCompleteness {
                complete: false,
                source_gap: Some("slug_only_discovery".to_owned()),
            };
        }
        if self
            .freshness_deadline_utc_ms
            .is_some_and(|deadline| now_utc_ms > deadline)
        {
            return DiscoveryCompleteness {
                complete: false,
                source_gap: Some("metadata_snapshot_stale".to_owned()),
            };
        }

        DiscoveryCompleteness {
            complete: true,
            source_gap: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DiscoveryMissStats {
    pub attempts: u64,
    pub misses: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DiscoveryMissDecision {
    pub miss_recorded: bool,
    pub discovery_miss_rate: f64,
    pub skip_expired_target: bool,
    pub recompute_target: bool,
}

impl DiscoveryMissStats {
    pub fn record_miss(
        &mut self,
        now_utc_ms: i64,
        target_end_utc_ms: i64,
    ) -> DiscoveryMissDecision {
        self.attempts = self.attempts.saturating_add(1);
        self.misses = self.misses.saturating_add(1);
        let expired = now_utc_ms >= target_end_utc_ms;
        DiscoveryMissDecision {
            miss_recorded: true,
            discovery_miss_rate: self.misses as f64 / self.attempts.max(1) as f64,
            skip_expired_target: expired,
            recompute_target: expired,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleInput {
    pub now_utc_ms: i64,
    pub start_time_utc_ms: i64,
    pub end_time_utc_ms: i64,
    pub has_prefetch_evidence: bool,
    pub retired: bool,
}

pub fn classify_lifecycle(input: &LifecycleInput) -> MarketLifecycleState {
    if input.retired {
        return MarketLifecycleState::Retired;
    }
    if input.now_utc_ms >= input.start_time_utc_ms {
        return MarketLifecycleState::Active;
    }
    if input.has_prefetch_evidence
        && input.now_utc_ms >= input.start_time_utc_ms.saturating_sub(T30_PREFETCH_MS)
    {
        return MarketLifecycleState::Prefetched;
    }
    MarketLifecycleState::Candidate
}

pub fn t30_prefetch_window(market_start_utc_ms: i64) -> (i64, i64) {
    (
        market_start_utc_ms.saturating_sub(T30_PREFETCH_MS),
        market_start_utc_ms,
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketRetiredFallbackMetadata {
    pub market_resolved_missing_reason: String,
    pub fallback_metadata_asset_ids: Vec<String>,
    pub fallback_freshness_ms: i64,
    pub same_condition_confirmed: bool,
    pub condition_id: String,
    pub closed: bool,
    pub active: bool,
    pub accepting_orders: bool,
    pub closed_time_utc_ms: Option<i64>,
    pub resolution_evidence: Option<String>,
    pub max_freshness_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolymarketRetirementEvidence {
    ResolvedEvent(Box<PolymarketMarketResolutionRaw>),
    FallbackMetadata(PolymarketRetiredFallbackMetadata),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetiredPredicateDecision {
    pub retired: bool,
    pub source_gap: Option<String>,
}

pub fn evaluate_retired_predicate(
    condition_id: &str,
    subscribed_asset_ids_for_market: &[String],
    evidence: &PolymarketRetirementEvidence,
) -> RetiredPredicateDecision {
    if subscribed_asset_ids_for_market.is_empty() {
        return not_retired("subscribed_asset_ids_missing");
    }

    match evidence {
        PolymarketRetirementEvidence::ResolvedEvent(resolved) => {
            if resolved.validate_contract().verdict != ValidationVerdict::Complete {
                return not_retired("market_resolved_event_incomplete");
            }
            if resolved.condition_id != condition_id {
                return not_retired("market_resolved_condition_mismatch");
            }
            let subscribed_match = subscribed_asset_ids_for_market
                .iter()
                .all(|asset_id| resolved.subscribed_asset_ids_for_market.contains(asset_id));
            if !subscribed_match {
                return not_retired("market_resolved_subscription_mismatch");
            }
            if !subscribed_asset_ids_for_market.contains(&resolved.winning_asset_id) {
                return not_retired("market_resolved_winning_asset_not_in_target_market");
            }
            RetiredPredicateDecision {
                retired: true,
                source_gap: None,
            }
        }
        PolymarketRetirementEvidence::FallbackMetadata(fallback) => {
            if fallback.market_resolved_missing_reason.trim().is_empty() {
                return not_retired("market_resolved_missing_reason_absent");
            }
            if fallback.condition_id != condition_id || !fallback.same_condition_confirmed {
                return not_retired("fallback_condition_mismatch");
            }
            if fallback.fallback_freshness_ms > fallback.max_freshness_ms {
                return not_retired("fallback_metadata_stale");
            }
            if !fallback.closed
                || fallback.active
                || fallback.accepting_orders
                || fallback
                    .closed_time_utc_ms
                    .is_none_or(|closed_time| closed_time <= 0)
                || fallback
                    .resolution_evidence
                    .as_deref()
                    .is_none_or(|evidence| evidence.trim().is_empty())
            {
                return not_retired("fallback_resolution_evidence_missing");
            }
            let fallback_covers_subscription = subscribed_asset_ids_for_market
                .iter()
                .all(|asset_id| fallback.fallback_metadata_asset_ids.contains(asset_id));
            if !fallback_covers_subscription {
                return not_retired("fallback_asset_set_mismatch");
            }
            RetiredPredicateDecision {
                retired: true,
                source_gap: None,
            }
        }
    }
}

fn not_retired(reason: &str) -> RetiredPredicateDecision {
    RetiredPredicateDecision {
        retired: false,
        source_gap: Some(reason.to_owned()),
    }
}
