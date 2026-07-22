use std::collections::BTreeSet;

use polymarket_client_sdk_v2::gamma::types::response::Market as SdkGammaMarket;
use polymarket_client_sdk_v2::types::Decimal;

use crate::ValidationVerdict;
use crate::market_discovery::build_market_subscription_request_json;
use crate::polymarket_market_metadata_layout::{
    ResolutionIntegrityErrorClass, ResolutionPollResult, TerminalPredicateFailureReason,
    parse_polymarket_gamma_closed_time_utc_ms,
};
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GammaTerminalFixedIdentity {
    pub slug: String,
    pub market_id: String,
    pub condition_id: String,
    pub up_asset_id: String,
    pub down_asset_id: String,
    pub market_end_utc_ms: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GammaTerminalAttemptContext {
    pub scheduled_at_utc_ms: i64,
    pub fetch_start_utc_ms: i64,
    pub received_at_utc_ms: i64,
    pub response_bound_to_attempt: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum GammaTerminalResponse<'a> {
    Decoded(&'a SdkGammaMarket),
    SdkDecodeOrSchemaError,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GammaTerminalWinner {
    pub winning_asset_id: String,
    pub winning_outcome: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GammaTerminalEvaluation {
    pub terminal_predicate_passed: Option<bool>,
    pub terminal_predicate_failure_reason: Option<TerminalPredicateFailureReason>,
    pub resolution_poll_result: ResolutionPollResult,
    pub resolution_integrity_error_class: Option<ResolutionIntegrityErrorClass>,
    pub observed_active: Option<bool>,
    pub closed_time_raw: Option<String>,
    pub closed_time_utc_ms: Option<i64>,
    pub winner: Option<GammaTerminalWinner>,
}

pub fn evaluate_gamma_terminal_response(
    fixed_identity: &GammaTerminalFixedIdentity,
    attempt: GammaTerminalAttemptContext,
    response: GammaTerminalResponse<'_>,
) -> GammaTerminalEvaluation {
    let market = match response {
        GammaTerminalResponse::Decoded(market) => market,
        GammaTerminalResponse::SdkDecodeOrSchemaError => {
            return not_evaluated(ResolutionIntegrityErrorClass::SdkDecodeOrSchema, None, None);
        }
    };
    let observed_active = market.active;
    let closed_time_raw = market.closed_time.clone();

    if !attempt.response_bound_to_attempt
        || attempt.fetch_start_utc_ms < attempt.scheduled_at_utc_ms
        || attempt.received_at_utc_ms < attempt.fetch_start_utc_ms
    {
        return not_evaluated(
            ResolutionIntegrityErrorClass::AttemptBindingInvariant,
            observed_active,
            closed_time_raw,
        );
    }

    if market.slug.as_deref() != Some(fixed_identity.slug.as_str()) {
        return source_failure(
            TerminalPredicateFailureReason::SlugMismatch,
            observed_active,
            closed_time_raw,
            None,
        );
    }
    if market.id != fixed_identity.market_id {
        return source_failure(
            TerminalPredicateFailureReason::MarketIdMismatch,
            observed_active,
            closed_time_raw,
            None,
        );
    }
    let condition_id = market
        .condition_id
        .as_ref()
        .map(|condition_id| format!("{condition_id:#x}"));
    if condition_id.as_deref() != Some(fixed_identity.condition_id.as_str()) {
        return source_failure(
            TerminalPredicateFailureReason::ConditionIdMismatch,
            observed_active,
            closed_time_raw,
            None,
        );
    }

    let asset_ids = market
        .clob_token_ids
        .as_ref()
        .map(|asset_ids| {
            asset_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if asset_ids.len() != 2 {
        return source_failure(
            TerminalPredicateFailureReason::AssetCountNotTwo,
            observed_active,
            closed_time_raw,
            None,
        );
    }
    if asset_ids[0] == asset_ids[1] {
        return source_failure(
            TerminalPredicateFailureReason::AssetDuplicate,
            observed_active,
            closed_time_raw,
            None,
        );
    }
    let actual_asset_set = asset_ids
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let fixed_asset_set = [
        fixed_identity.up_asset_id.as_str(),
        fixed_identity.down_asset_id.as_str(),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    if actual_asset_set != fixed_asset_set {
        return source_failure(
            TerminalPredicateFailureReason::AssetSetMismatch,
            observed_active,
            closed_time_raw,
            None,
        );
    }

    let outcomes = market.outcomes.as_deref().unwrap_or_default();
    if outcomes.len() != 2 {
        return source_failure(
            TerminalPredicateFailureReason::OutcomesCountNotTwo,
            observed_active,
            closed_time_raw,
            None,
        );
    }
    if !outcome_mapping_matches_fixed_identity(fixed_identity, &asset_ids, outcomes) {
        return source_failure(
            TerminalPredicateFailureReason::OutcomeMappingInvalid,
            observed_active,
            closed_time_raw,
            None,
        );
    }

    let outcome_prices = market.outcome_prices.as_deref().unwrap_or_default();
    if outcome_prices.len() != 2 {
        return source_failure(
            TerminalPredicateFailureReason::OutcomePricesCountNotTwo,
            observed_active,
            closed_time_raw,
            None,
        );
    }
    match market.closed {
        None => {
            return source_failure(
                TerminalPredicateFailureReason::ClosedMissing,
                observed_active,
                closed_time_raw,
                None,
            );
        }
        Some(false) => {
            return source_failure(
                TerminalPredicateFailureReason::ClosedFalse,
                observed_active,
                closed_time_raw,
                None,
            );
        }
        Some(true) => {}
    }
    match market.accepting_orders {
        None => {
            return source_failure(
                TerminalPredicateFailureReason::AcceptingOrdersMissing,
                observed_active,
                closed_time_raw,
                None,
            );
        }
        Some(true) => {
            return source_failure(
                TerminalPredicateFailureReason::AcceptingOrdersTrue,
                observed_active,
                closed_time_raw,
                None,
            );
        }
        Some(false) => {}
    }

    let Some(raw_closed_time) = closed_time_raw.as_deref() else {
        return source_failure(
            TerminalPredicateFailureReason::ClosedTimeMissing,
            observed_active,
            None,
            None,
        );
    };
    let closed_time_utc_ms = match parse_polymarket_gamma_closed_time_utc_ms(raw_closed_time) {
        Some(closed_time) => closed_time,
        None => {
            return source_failure(
                TerminalPredicateFailureReason::ClosedTimeParseError,
                observed_active,
                closed_time_raw,
                None,
            );
        }
    };
    if closed_time_utc_ms < fixed_identity.market_end_utc_ms {
        return source_failure(
            TerminalPredicateFailureReason::ClosedTimeBeforeMarketEnd,
            observed_active,
            closed_time_raw,
            Some(closed_time_utc_ms),
        );
    }

    match market.uma_resolution_status.as_deref() {
        None => {
            return source_failure(
                TerminalPredicateFailureReason::UmaResolutionStatusMissing,
                observed_active,
                closed_time_raw,
                Some(closed_time_utc_ms),
            );
        }
        Some(status) if !status.trim().eq_ignore_ascii_case("resolved") => {
            return source_failure(
                TerminalPredicateFailureReason::UmaResolutionStatusNotResolved,
                observed_active,
                closed_time_raw,
                Some(closed_time_utc_ms),
            );
        }
        Some(_) => {}
    }

    let winning_index = if outcome_prices[0] == Decimal::ONE && outcome_prices[1] == Decimal::ZERO {
        0
    } else if outcome_prices[0] == Decimal::ZERO && outcome_prices[1] == Decimal::ONE {
        1
    } else {
        return source_failure(
            TerminalPredicateFailureReason::OutcomePricesNotOneHot,
            observed_active,
            closed_time_raw,
            Some(closed_time_utc_ms),
        );
    };

    finalize_gamma_terminal(
        fixed_identity,
        &asset_ids,
        outcomes,
        winning_index,
        observed_active,
        closed_time_raw,
        closed_time_utc_ms,
    )
}

fn outcome_mapping_matches_fixed_identity(
    fixed_identity: &GammaTerminalFixedIdentity,
    asset_ids: &[String],
    outcomes: &[String],
) -> bool {
    let mut saw_up = false;
    let mut saw_down = false;
    for (asset_id, outcome) in asset_ids.iter().zip(outcomes) {
        match outcome.trim().to_ascii_lowercase().as_str() {
            "up" if asset_id == &fixed_identity.up_asset_id && !saw_up => saw_up = true,
            "down" if asset_id == &fixed_identity.down_asset_id && !saw_down => saw_down = true,
            _ => return false,
        }
    }
    saw_up && saw_down
}

fn finalize_gamma_terminal(
    fixed_identity: &GammaTerminalFixedIdentity,
    asset_ids: &[String],
    outcomes: &[String],
    winning_index: usize,
    observed_active: Option<bool>,
    closed_time_raw: Option<String>,
    closed_time_utc_ms: i64,
) -> GammaTerminalEvaluation {
    let winner = asset_ids
        .get(winning_index)
        .zip(outcomes.get(winning_index))
        .map(|(winning_asset_id, winning_outcome)| GammaTerminalWinner {
            winning_asset_id: winning_asset_id.clone(),
            winning_outcome: winning_outcome.clone(),
        });
    let winner_matches_fixed_identity = winner.as_ref().is_some_and(|winner| {
        matches!(
            winner.winning_outcome.trim().to_ascii_lowercase().as_str(),
            "up" if winner.winning_asset_id == fixed_identity.up_asset_id
        ) || matches!(
            winner.winning_outcome.trim().to_ascii_lowercase().as_str(),
            "down" if winner.winning_asset_id == fixed_identity.down_asset_id
        )
    });
    if !winner_matches_fixed_identity {
        return GammaTerminalEvaluation {
            terminal_predicate_passed: None,
            terminal_predicate_failure_reason: None,
            resolution_poll_result: ResolutionPollResult::IntegrityError,
            resolution_integrity_error_class: Some(
                ResolutionIntegrityErrorClass::DerivedWinnerInvariant,
            ),
            observed_active,
            closed_time_raw,
            closed_time_utc_ms: Some(closed_time_utc_ms),
            winner: None,
        };
    }

    GammaTerminalEvaluation {
        terminal_predicate_passed: Some(true),
        terminal_predicate_failure_reason: None,
        resolution_poll_result: ResolutionPollResult::Terminal,
        resolution_integrity_error_class: None,
        observed_active,
        closed_time_raw,
        closed_time_utc_ms: Some(closed_time_utc_ms),
        winner,
    }
}

fn source_failure(
    reason: TerminalPredicateFailureReason,
    observed_active: Option<bool>,
    closed_time_raw: Option<String>,
    closed_time_utc_ms: Option<i64>,
) -> GammaTerminalEvaluation {
    GammaTerminalEvaluation {
        terminal_predicate_passed: Some(false),
        terminal_predicate_failure_reason: Some(reason),
        resolution_poll_result: reason.poll_result(),
        resolution_integrity_error_class: reason.integrity_error_class(),
        observed_active,
        closed_time_raw,
        closed_time_utc_ms,
        winner: None,
    }
}

fn not_evaluated(
    class: ResolutionIntegrityErrorClass,
    observed_active: Option<bool>,
    closed_time_raw: Option<String>,
) -> GammaTerminalEvaluation {
    GammaTerminalEvaluation {
        terminal_predicate_passed: None,
        terminal_predicate_failure_reason: None,
        resolution_poll_result: ResolutionPollResult::IntegrityError,
        resolution_integrity_error_class: Some(class),
        observed_active,
        closed_time_raw,
        closed_time_utc_ms: None,
        winner: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_winner_invariant_is_not_a_source_predicate_failure() {
        let evaluation = finalize_gamma_terminal(
            &GammaTerminalFixedIdentity {
                slug: "btc-updown-5m-1".to_owned(),
                market_id: "market-1".to_owned(),
                condition_id: "0x0000000000000000000000000000000000000000000000000000000000000001"
                    .to_owned(),
                up_asset_id: "101".to_owned(),
                down_asset_id: "102".to_owned(),
                market_end_utc_ms: 0,
            },
            &["101".to_owned(), "102".to_owned()],
            &["Up".to_owned(), "Down".to_owned()],
            2,
            Some(true),
            Some("2026-01-01T00:00:00Z".to_owned()),
            1_767_225_600_000,
        );

        assert_eq!(evaluation.terminal_predicate_passed, None);
        assert_eq!(evaluation.terminal_predicate_failure_reason, None);
        assert_eq!(
            evaluation.resolution_poll_result,
            ResolutionPollResult::IntegrityError
        );
        assert_eq!(
            evaluation.resolution_integrity_error_class,
            Some(ResolutionIntegrityErrorClass::DerivedWinnerInvariant)
        );
        assert!(evaluation.winner.is_none());
    }
}
