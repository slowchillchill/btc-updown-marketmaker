use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use polymarket_client_sdk_v2::types::DateTime;
use sha2::{Digest as _, Sha256};

pub use crate::source_format::MarketLifecycleState;

pub const POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION: &str = "polymarket_market_metadata_v2";
pub const POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION: &str = "polymarket_market_metadata_v3";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolymarketMetadataRowKind {
    MetadataSnapshot,
    MarketStateTransition,
    ResolutionPollAttempt,
    ResolutionTerminalWs,
    ResolutionFollowupSummary,
}

impl PolymarketMetadataRowKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MetadataSnapshot => "metadata_snapshot",
            Self::MarketStateTransition => "market_state_transition",
            Self::ResolutionPollAttempt => "resolution_poll_attempt",
            Self::ResolutionTerminalWs => "resolution_terminal_ws",
            Self::ResolutionFollowupSummary => "resolution_followup_summary",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolymarketMetadataOrigin {
    GammaMarketsDiscovery,
    GammaBySlug,
    MarketResolvedWs,
    LocalMarketStateTimer,
    LocalFollowupScheduler,
}

impl PolymarketMetadataOrigin {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GammaMarketsDiscovery => "gamma_markets_discovery",
            Self::GammaBySlug => "gamma_by_slug",
            Self::MarketResolvedWs => "market_resolved_ws",
            Self::LocalMarketStateTimer => "local_market_state_timer",
            Self::LocalFollowupScheduler => "local_followup_scheduler",
        }
    }
}

impl MarketLifecycleState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Prefetched => "prefetched",
            Self::Active => "active",
            Self::Retired => "retired",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketRuntimeRole {
    Current,
    Next,
    ResolutionPending,
    None,
}

impl MarketRuntimeRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Next => "next",
            Self::ResolutionPending => "resolution_pending",
            Self::None => "none",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceEndpointUnavailableReason {
    NotApplicableNoSourceEvidence,
}

impl SourceEndpointUnavailableReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotApplicableNoSourceEvidence => "not_applicable_no_source_evidence",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceEvidenceOrigin {
    GammaMarketsDiscovery,
    GammaBySlug,
    MarketResolvedWs,
}

impl SourceEvidenceOrigin {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GammaMarketsDiscovery => "gamma_markets_discovery",
            Self::GammaBySlug => "gamma_by_slug",
            Self::MarketResolvedWs => "market_resolved_ws",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceEvidenceEventType {
    GammaMarketsDiscoveryResponse,
    GammaMarketBySlugResponse,
    MarketResolved,
}

impl SourceEvidenceEventType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GammaMarketsDiscoveryResponse => "gamma_markets_discovery_response",
            Self::GammaMarketBySlugResponse => "gamma_market_by_slug_response",
            Self::MarketResolved => "market_resolved",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceEvidenceChannel {
    Market,
}

impl SourceEvidenceChannel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Market => "market",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketLifecycleTransition {
    PrefetchedToActive,
    CandidateToActive,
    ActiveToRetired,
}

impl MarketLifecycleTransition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PrefetchedToActive => "prefetched_to_active",
            Self::CandidateToActive => "candidate_to_active",
            Self::ActiveToRetired => "active_to_retired",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketRuntimeRoleTransition {
    NoneToCurrent,
    NextToCurrent,
    CurrentToResolutionPending,
    ResolutionPendingToNone,
}

impl MarketRuntimeRoleTransition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoneToCurrent => "none_to_current",
            Self::NextToCurrent => "next_to_current",
            Self::CurrentToResolutionPending => "current_to_resolution_pending",
            Self::ResolutionPendingToNone => "resolution_pending_to_none",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClosedTimeCheckFailureReason {
    Missing,
    ParseError,
    BeforeMarketEnd,
}

impl ClosedTimeCheckFailureReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::ParseError => "parse_error",
            Self::BeforeMarketEnd => "before_market_end",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActiveUnavailableReason {
    SourceFieldAbsent,
    NotApplicableForRowKind,
}

impl ActiveUnavailableReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SourceFieldAbsent => "source_field_absent",
            Self::NotApplicableForRowKind => "not_applicable_for_row_kind",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolutionEvidenceSource {
    MarketResolvedWs,
    GammaBySlug,
}

impl ResolutionEvidenceSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MarketResolvedWs => "market_resolved_ws",
            Self::GammaBySlug => "gamma_by_slug",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalClaimOwner {
    MarketResolvedWs,
    GammaBySlug,
}

impl TerminalClaimOwner {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MarketResolvedWs => "market_resolved_ws",
            Self::GammaBySlug => "gamma_by_slug",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolutionPollResult {
    Terminal,
    ValidNonterminal,
    TransportUnavailable,
    IntegrityError,
    MissedDueToDowntime,
    PostTerminalClaimObservation,
}

impl ResolutionPollResult {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Terminal => "terminal",
            Self::ValidNonterminal => "valid_nonterminal",
            Self::TransportUnavailable => "transport_unavailable",
            Self::IntegrityError => "integrity_error",
            Self::MissedDueToDowntime => "missed_due_to_downtime",
            Self::PostTerminalClaimObservation => "post_terminal_claim_observation",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolutionTransportErrorClass {
    Dns,
    Connect,
    Timeout,
    HttpStatus,
    RateLimited,
    SdkTransport,
}

impl ResolutionTransportErrorClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dns => "dns",
            Self::Connect => "connect",
            Self::Timeout => "timeout",
            Self::HttpStatus => "http_status",
            Self::RateLimited => "rate_limited",
            Self::SdkTransport => "sdk_transport",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolutionIntegrityErrorClass {
    AttemptBindingInvariant,
    SdkDecodeOrSchema,
    SourceIdentityOrShapeContradiction,
    ClosedTimeInvalid,
    DerivedWinnerInvariant,
}

impl ResolutionIntegrityErrorClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AttemptBindingInvariant => "attempt_binding_invariant",
            Self::SdkDecodeOrSchema => "sdk_decode_or_schema",
            Self::SourceIdentityOrShapeContradiction => "source_identity_or_shape_contradiction",
            Self::ClosedTimeInvalid => "closed_time_invalid",
            Self::DerivedWinnerInvariant => "derived_winner_invariant",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolutionFollowupStatus {
    Pending,
    TerminalWs,
    TerminalGammaFallback,
    BudgetExhaustedAfterValidNonterminal,
    BudgetExhaustedWithoutValidResponse,
}

impl ResolutionFollowupStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::TerminalWs => "terminal_ws",
            Self::TerminalGammaFallback => "terminal_gamma_fallback",
            Self::BudgetExhaustedAfterValidNonterminal => {
                "budget_exhausted_after_valid_nonterminal"
            }
            Self::BudgetExhaustedWithoutValidResponse => "budget_exhausted_without_valid_response",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalPredicateFailureReason {
    SlugMismatch,
    MarketIdMismatch,
    ConditionIdMismatch,
    AssetCountNotTwo,
    AssetDuplicate,
    AssetSetMismatch,
    OutcomesCountNotTwo,
    OutcomeMappingInvalid,
    OutcomePricesCountNotTwo,
    ClosedMissing,
    ClosedFalse,
    AcceptingOrdersMissing,
    AcceptingOrdersTrue,
    ClosedTimeMissing,
    ClosedTimeParseError,
    ClosedTimeBeforeMarketEnd,
    UmaResolutionStatusMissing,
    UmaResolutionStatusNotResolved,
    OutcomePricesNotOneHot,
}

impl TerminalPredicateFailureReason {
    pub const ALL: [Self; 19] = [
        Self::SlugMismatch,
        Self::MarketIdMismatch,
        Self::ConditionIdMismatch,
        Self::AssetCountNotTwo,
        Self::AssetDuplicate,
        Self::AssetSetMismatch,
        Self::OutcomesCountNotTwo,
        Self::OutcomeMappingInvalid,
        Self::OutcomePricesCountNotTwo,
        Self::ClosedMissing,
        Self::ClosedFalse,
        Self::AcceptingOrdersMissing,
        Self::AcceptingOrdersTrue,
        Self::ClosedTimeMissing,
        Self::ClosedTimeParseError,
        Self::ClosedTimeBeforeMarketEnd,
        Self::UmaResolutionStatusMissing,
        Self::UmaResolutionStatusNotResolved,
        Self::OutcomePricesNotOneHot,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SlugMismatch => "slug_mismatch",
            Self::MarketIdMismatch => "market_id_mismatch",
            Self::ConditionIdMismatch => "condition_id_mismatch",
            Self::AssetCountNotTwo => "asset_count_not_two",
            Self::AssetDuplicate => "asset_duplicate",
            Self::AssetSetMismatch => "asset_set_mismatch",
            Self::OutcomesCountNotTwo => "outcomes_count_not_two",
            Self::OutcomeMappingInvalid => "outcome_mapping_invalid",
            Self::OutcomePricesCountNotTwo => "outcome_prices_count_not_two",
            Self::ClosedMissing => "closed_missing",
            Self::ClosedFalse => "closed_false",
            Self::AcceptingOrdersMissing => "accepting_orders_missing",
            Self::AcceptingOrdersTrue => "accepting_orders_true",
            Self::ClosedTimeMissing => "closed_time_missing",
            Self::ClosedTimeParseError => "closed_time_parse_error",
            Self::ClosedTimeBeforeMarketEnd => "closed_time_before_market_end",
            Self::UmaResolutionStatusMissing => "uma_resolution_status_missing",
            Self::UmaResolutionStatusNotResolved => "uma_resolution_status_not_resolved",
            Self::OutcomePricesNotOneHot => "outcome_prices_not_one_hot",
        }
    }

    pub const fn integrity_error_class(self) -> Option<ResolutionIntegrityErrorClass> {
        match self {
            Self::SlugMismatch
            | Self::MarketIdMismatch
            | Self::ConditionIdMismatch
            | Self::AssetCountNotTwo
            | Self::AssetDuplicate
            | Self::AssetSetMismatch
            | Self::OutcomesCountNotTwo
            | Self::OutcomeMappingInvalid
            | Self::OutcomePricesCountNotTwo => {
                Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction)
            }
            Self::ClosedTimeParseError | Self::ClosedTimeBeforeMarketEnd => {
                Some(ResolutionIntegrityErrorClass::ClosedTimeInvalid)
            }
            Self::ClosedMissing
            | Self::ClosedFalse
            | Self::AcceptingOrdersMissing
            | Self::AcceptingOrdersTrue
            | Self::ClosedTimeMissing
            | Self::UmaResolutionStatusMissing
            | Self::UmaResolutionStatusNotResolved
            | Self::OutcomePricesNotOneHot => None,
        }
    }

    pub const fn poll_result(self) -> ResolutionPollResult {
        if self.integrity_error_class().is_some() {
            ResolutionPollResult::IntegrityError
        } else {
            ResolutionPollResult::ValidNonterminal
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseFieldsUnavailableReason {
    NotApplicableForRowKind,
    TransportUnavailable,
    MissedDueToDowntime,
    SdkDecodeOrSchemaError,
}

impl ResponseFieldsUnavailableReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotApplicableForRowKind => "not_applicable_for_row_kind",
            Self::TransportUnavailable => "transport_unavailable",
            Self::MissedDueToDowntime => "missed_due_to_downtime",
            Self::SdkDecodeOrSchemaError => "sdk_decode_or_schema_error",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMarketMetadataContext {
    pub timestamp_received_ms: i64,
    pub metadata_fetch_start_utc_ms: Option<i64>,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub schema_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMarketMetadataValidation {
    pub missing_fields: Vec<String>,
    pub source_gaps: Vec<String>,
}

impl PolymarketMarketMetadataValidation {
    pub fn is_complete(&self) -> bool {
        self.missing_fields.is_empty() && self.source_gaps.is_empty()
    }

    fn require_non_empty(&mut self, field: &str, value: Option<&str>) {
        if value.is_none_or(|value| value.trim().is_empty()) {
            self.missing_fields.push(field.to_owned());
        }
    }

    fn require_some<T>(&mut self, field: &str, value: Option<T>) {
        if value.is_none() {
            self.missing_fields.push(field.to_owned());
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMarketMetadataRow {
    pub timestamp_received: i64,
    pub metadata_fetch_start_utc: Option<i64>,
    pub slug: String,
    pub condition_id: String,
    pub clob_market_id: Option<String>,
    pub asset_ids: Option<String>,
    pub market_start_utc: Option<i64>,
    pub market_end_utc: Option<i64>,
    pub active: Option<bool>,
    pub accepting_orders: Option<bool>,
    pub market_lifecycle_state: String,
    pub tick_size: Option<String>,
    pub min_size: Option<String>,
    pub fee_schedule_raw: Option<String>,
    pub reward_eligibility_raw: Option<String>,
    pub metadata_unavailable_reason: Option<String>,
    pub metadata_source: String,
    pub same_condition_market_verified: Option<bool>,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub schema_version: String,
}

impl PolymarketMarketMetadataRow {
    #[allow(clippy::too_many_arguments)]
    pub fn new_market(
        slug: &str,
        condition_id: &str,
        clob_market_id: Option<&str>,
        asset_ids: &[String],
        market_start_utc: Option<i64>,
        market_end_utc: Option<i64>,
        active: Option<bool>,
        accepting_orders: Option<bool>,
        market_lifecycle_state: &str,
        tick_size: Option<&str>,
        min_size: Option<&str>,
        fee_schedule_raw: Option<&str>,
        reward_eligibility_raw: Option<&str>,
        same_condition_market_verified: Option<bool>,
        context: PolymarketMarketMetadataContext,
    ) -> Self {
        metadata_row(
            "new_market",
            slug,
            condition_id,
            clob_market_id,
            asset_ids,
            market_start_utc,
            market_end_utc,
            active,
            accepting_orders,
            market_lifecycle_state,
            tick_size,
            min_size,
            fee_schedule_raw,
            reward_eligibility_raw,
            same_condition_market_verified,
            context,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn active_scan(
        slug: &str,
        condition_id: &str,
        clob_market_id: Option<&str>,
        asset_ids: &[String],
        market_start_utc: Option<i64>,
        market_end_utc: Option<i64>,
        active: Option<bool>,
        accepting_orders: Option<bool>,
        market_lifecycle_state: &str,
        tick_size: Option<&str>,
        min_size: Option<&str>,
        fee_schedule_raw: Option<&str>,
        reward_eligibility_raw: Option<&str>,
        same_condition_market_verified: Option<bool>,
        context: PolymarketMarketMetadataContext,
    ) -> Self {
        metadata_row(
            "active_scan",
            slug,
            condition_id,
            clob_market_id,
            asset_ids,
            market_start_utc,
            market_end_utc,
            active,
            accepting_orders,
            market_lifecycle_state,
            tick_size,
            min_size,
            fee_schedule_raw,
            reward_eligibility_raw,
            same_condition_market_verified,
            context,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn fallback_close(
        slug: &str,
        condition_id: &str,
        clob_market_id: Option<&str>,
        asset_ids: &[String],
        market_start_utc: Option<i64>,
        market_end_utc: Option<i64>,
        active: Option<bool>,
        accepting_orders: Option<bool>,
        market_lifecycle_state: &str,
        tick_size: Option<&str>,
        min_size: Option<&str>,
        fee_schedule_raw: Option<&str>,
        reward_eligibility_raw: Option<&str>,
        same_condition_market_verified: Option<bool>,
        context: PolymarketMarketMetadataContext,
    ) -> Self {
        metadata_row(
            "fallback_close",
            slug,
            condition_id,
            clob_market_id,
            asset_ids,
            market_start_utc,
            market_end_utc,
            active,
            accepting_orders,
            market_lifecycle_state,
            tick_size,
            min_size,
            fee_schedule_raw,
            reward_eligibility_raw,
            same_condition_market_verified,
            context,
        )
    }

    pub fn candidate_slug_only(
        slug: &str,
        market_start_utc: Option<i64>,
        market_end_utc: Option<i64>,
        context: PolymarketMarketMetadataContext,
    ) -> Self {
        metadata_row(
            "candidate_slug",
            slug,
            "",
            None,
            &[],
            market_start_utc,
            market_end_utc,
            None,
            None,
            "candidate",
            None,
            None,
            None,
            None,
            None,
            context,
        )
    }

    pub fn validate(&self) -> PolymarketMarketMetadataValidation {
        let mut report = PolymarketMarketMetadataValidation {
            missing_fields: Vec::new(),
            source_gaps: Vec::new(),
        };
        if self.timestamp_received <= 0 {
            report.missing_fields.push("timestamp_received".to_owned());
        }
        report.require_non_empty("slug", Some(&self.slug));
        report.require_non_empty("condition_id", Some(&self.condition_id));
        report.require_non_empty("asset_ids", self.asset_ids.as_deref());
        if !self.has_metadata_unavailable_reason() {
            report.require_some("market_start_utc", self.market_start_utc);
            report.require_some("market_end_utc", self.market_end_utc);
            report.require_some("active", self.active);
            report.require_some("accepting_orders", self.accepting_orders);
        }
        report.require_non_empty("market_lifecycle_state", Some(&self.market_lifecycle_state));
        if !self.has_metadata_unavailable_reason() {
            report.require_non_empty("tick_size", self.tick_size.as_deref());
            report.require_non_empty("min_size", self.min_size.as_deref());
            report.require_non_empty("fee_schedule_raw", self.fee_schedule_raw.as_deref());
            report.require_non_empty(
                "reward_eligibility_raw",
                self.reward_eligibility_raw.as_deref(),
            );
        }
        report.require_non_empty("metadata_source", Some(&self.metadata_source));
        report.require_some(
            "same_condition_market_verified",
            self.same_condition_market_verified,
        );
        report.require_non_empty("collector_host_id", Some(&self.collector_host_id));
        report.require_non_empty("collector_region", Some(&self.collector_region));
        report.require_non_empty("source_endpoint", Some(&self.source_endpoint));
        report.require_non_empty("schema_version", Some(&self.schema_version));

        if self.metadata_source == "candidate_slug" {
            report.source_gaps.push("slug_only_discovery".to_owned());
        }
        if self
            .same_condition_market_verified
            .is_some_and(|verified| !verified)
        {
            report
                .source_gaps
                .push("same_condition_market_unverified".to_owned());
        }
        if let (Some(start), Some(end)) = (self.market_start_utc, self.market_end_utc)
            && start >= end
        {
            report.missing_fields.push("market_start_end".to_owned());
        }
        report
    }

    pub fn with_metadata_unavailable_reason(mut self, reason: Option<String>) -> Self {
        self.metadata_unavailable_reason = reason;
        self
    }

    fn has_metadata_unavailable_reason(&self) -> bool {
        self.metadata_unavailable_reason
            .as_deref()
            .is_some_and(|reason| !reason.trim().is_empty())
    }
}

pub fn polymarket_market_metadata_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp_received",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "metadata_fetch_start_utc",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new("slug", DataType::Utf8, false),
        Field::new("condition_id", DataType::Utf8, false),
        Field::new("clob_market_id", DataType::Utf8, true),
        Field::new("asset_ids", DataType::Utf8, true),
        Field::new(
            "market_start_utc",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "market_end_utc",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new("active", DataType::Boolean, true),
        Field::new("accepting_orders", DataType::Boolean, true),
        Field::new("market_lifecycle_state", DataType::Utf8, false),
        Field::new("tick_size", DataType::Utf8, true),
        Field::new("min_size", DataType::Utf8, true),
        Field::new("fee_schedule_raw", DataType::Utf8, true),
        Field::new("reward_eligibility_raw", DataType::Utf8, true),
        Field::new("metadata_unavailable_reason", DataType::Utf8, true),
        Field::new("metadata_source", DataType::Utf8, false),
        Field::new("same_condition_market_verified", DataType::Boolean, true),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]))
}

#[allow(clippy::too_many_arguments)]
fn metadata_row(
    metadata_source: &str,
    slug: &str,
    condition_id: &str,
    clob_market_id: Option<&str>,
    asset_ids: &[String],
    market_start_utc: Option<i64>,
    market_end_utc: Option<i64>,
    active: Option<bool>,
    accepting_orders: Option<bool>,
    market_lifecycle_state: &str,
    tick_size: Option<&str>,
    min_size: Option<&str>,
    fee_schedule_raw: Option<&str>,
    reward_eligibility_raw: Option<&str>,
    same_condition_market_verified: Option<bool>,
    context: PolymarketMarketMetadataContext,
) -> PolymarketMarketMetadataRow {
    let asset_ids_json = non_empty_json_list(asset_ids);
    PolymarketMarketMetadataRow {
        timestamp_received: context.timestamp_received_ms,
        metadata_fetch_start_utc: context.metadata_fetch_start_utc_ms,
        slug: slug.to_owned(),
        condition_id: condition_id.to_owned(),
        clob_market_id: clob_market_id.map(ToOwned::to_owned),
        asset_ids: asset_ids_json,
        market_start_utc,
        market_end_utc,
        active,
        accepting_orders,
        market_lifecycle_state: market_lifecycle_state.to_owned(),
        tick_size: tick_size.map(ToOwned::to_owned),
        min_size: min_size.map(ToOwned::to_owned),
        fee_schedule_raw: fee_schedule_raw.map(ToOwned::to_owned),
        reward_eligibility_raw: reward_eligibility_raw.map(ToOwned::to_owned),
        metadata_unavailable_reason: None,
        metadata_source: metadata_source.to_owned(),
        same_condition_market_verified,
        collector_host_id: context.collector_host_id,
        collector_region: context.collector_region,
        source_endpoint: context.source_endpoint,
        schema_version: context.schema_version,
    }
}

fn non_empty_json_list(values: &[String]) -> Option<String> {
    if values.is_empty() {
        return None;
    }
    Some(serde_json::to_string(values).expect("string vec serializes"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMarketMetadataV3Validation {
    pub violations: Vec<String>,
}

impl PolymarketMarketMetadataV3Validation {
    pub fn is_complete(&self) -> bool {
        self.violations.is_empty()
    }

    fn require(&mut self, field: &str, present: bool) {
        if !present {
            self.violations.push(format!("{field}:required"));
        }
    }

    fn require_null(&mut self, field: &str, is_null: bool) {
        if !is_null {
            self.violations.push(format!("{field}:must_be_null"));
        }
    }

    fn reject(&mut self, violation: &str) {
        self.violations.push(violation.to_owned());
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMarketMetadataV3Row {
    pub metadata_row_kind: PolymarketMetadataRowKind,
    pub timestamp_received: i64,
    pub collector_admission_sequence: u64,
    pub collector_admission_time_utc_ms: i64,
    pub metadata_origin: PolymarketMetadataOrigin,
    pub collector_host_id: String,
    pub collector_region: String,
    pub schema_version: String,
    pub slug: String,
    pub condition_id: String,
    pub clob_market_id: String,
    pub asset_ids: String,
    pub market_start_utc: i64,
    pub market_end_utc: i64,
    pub fixed_identity_verified_at_utc: i64,
    pub market_lifecycle_state: MarketLifecycleState,
    pub market_runtime_role: MarketRuntimeRole,
    pub source_endpoint: Option<String>,
    pub source_endpoint_unavailable_reason: Option<SourceEndpointUnavailableReason>,
    pub source_evidence_origin: Option<SourceEvidenceOrigin>,
    pub source_evidence_event_type: Option<SourceEvidenceEventType>,
    pub source_evidence_channel: Option<SourceEvidenceChannel>,
    pub source_evidence_received_at_utc: Option<i64>,
    pub row_evidence_validation_completed_at_utc: Option<i64>,
    pub source_evidence_payload_json: Option<String>,
    pub source_evidence_raw_sha256: Option<String>,
    pub market_lifecycle_transition: Option<MarketLifecycleTransition>,
    pub lifecycle_scheduled_at_utc: Option<i64>,
    pub lifecycle_applied_at_utc: Option<i64>,
    pub market_runtime_role_transition: Option<MarketRuntimeRoleTransition>,
    pub runtime_role_scheduled_at_utc: Option<i64>,
    pub runtime_role_applied_at_utc: Option<i64>,
    pub metadata_fetch_start_utc: Option<i64>,
    pub closed: Option<bool>,
    pub closed_time_raw: Option<String>,
    pub closed_time_utc: Option<i64>,
    pub closed_time_check_passed: Option<bool>,
    pub closed_time_check_failure_reason: Option<ClosedTimeCheckFailureReason>,
    pub automatically_resolved: Option<bool>,
    pub uma_resolution_status: Option<String>,
    pub outcomes_raw: Option<String>,
    pub outcome_prices_raw: Option<String>,
    pub active: Option<bool>,
    pub active_unavailable_reason: Option<ActiveUnavailableReason>,
    pub accepting_orders: Option<bool>,
    pub ws_platform_timestamp_raw: Option<String>,
    pub ws_platform_timestamp_utc: Option<i64>,
    pub winning_asset_id: Option<String>,
    pub winning_outcome: Option<String>,
    pub resolution_evidence_source: Option<ResolutionEvidenceSource>,
    pub resolution_evidence_raw_sha256: Option<String>,
    pub terminal_claim_owner: Option<TerminalClaimOwner>,
    pub resolution_poll_attempt_ordinal: Option<u8>,
    pub resolution_poll_scheduled_utc: Option<i64>,
    pub resolution_poll_result: Option<ResolutionPollResult>,
    pub resolution_transport_error_class: Option<ResolutionTransportErrorClass>,
    pub resolution_transport_http_status: Option<u16>,
    pub resolution_integrity_error_class: Option<ResolutionIntegrityErrorClass>,
    pub resolution_budget_horizon_utc: Option<i64>,
    pub resolution_followup_status: Option<ResolutionFollowupStatus>,
    pub resolution_poll_attempts_recorded: Option<u32>,
    pub resolution_valid_response_count: Option<u32>,
    pub resolution_transport_unavailable_count: Option<u32>,
    pub resolution_missed_slot_count: Option<u32>,
    pub last_valid_metadata_received_utc: Option<i64>,
    pub terminal_predicate_passed: Option<bool>,
    pub terminal_predicate_failure_reason: Option<TerminalPredicateFailureReason>,
    pub response_fields_unavailable_reason: Option<ResponseFieldsUnavailableReason>,
}

impl PolymarketMarketMetadataV3Row {
    pub fn validate(&self) -> PolymarketMarketMetadataV3Validation {
        let mut report = PolymarketMarketMetadataV3Validation {
            violations: Vec::new(),
        };
        self.validate_common(&mut report);
        match self.metadata_row_kind {
            PolymarketMetadataRowKind::MetadataSnapshot => {
                self.validate_metadata_snapshot(&mut report)
            }
            PolymarketMetadataRowKind::MarketStateTransition => {
                self.validate_market_state_transition(&mut report)
            }
            PolymarketMetadataRowKind::ResolutionPollAttempt => {
                self.validate_resolution_poll_attempt(&mut report)
            }
            PolymarketMetadataRowKind::ResolutionTerminalWs => {
                self.validate_resolution_terminal_ws(&mut report)
            }
            PolymarketMetadataRowKind::ResolutionFollowupSummary => {
                self.validate_resolution_followup_summary(&mut report)
            }
        }
        report
    }

    fn validate_common(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        report.require("timestamp_received", self.timestamp_received > 0);
        report.require(
            "collector_admission_sequence",
            self.collector_admission_sequence > 0,
        );
        report.require(
            "collector_admission_time_utc_ms",
            self.collector_admission_time_utc_ms > 0,
        );
        if self.timestamp_received > self.collector_admission_time_utc_ms {
            report.reject("timestamp_received:after_admission");
        }
        for (field, value) in [
            ("collector_host_id", self.collector_host_id.as_str()),
            ("collector_region", self.collector_region.as_str()),
            ("slug", self.slug.as_str()),
            ("condition_id", self.condition_id.as_str()),
            ("clob_market_id", self.clob_market_id.as_str()),
            ("asset_ids", self.asset_ids.as_str()),
        ] {
            report.require(field, !value.trim().is_empty());
        }
        if self.schema_version != POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION {
            report.reject("schema_version:expected_v3");
        }
        self.validate_fixed_identity(report);
        self.validate_source_times_and_digests(report);
        self.validate_closed_time(report);
        self.validate_active_reason(report);
        self.validate_followup_horizon(report);
    }

    fn validate_fixed_identity(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        report.require("market_start_utc", self.market_start_utc > 0);
        report.require("market_end_utc", self.market_end_utc > 0);
        if self.market_start_utc >= self.market_end_utc {
            report.reject("market_start_utc:not_before_market_end_utc");
        }
        report.require(
            "fixed_identity_verified_at_utc",
            self.fixed_identity_verified_at_utc > 0,
        );
        if self.fixed_identity_verified_at_utc > self.collector_admission_time_utc_ms {
            report.reject("fixed_identity_verified_at_utc:after_admission");
        }

        match serde_json::from_str::<Vec<String>>(&self.asset_ids) {
            Ok(asset_ids)
                if asset_ids.len() == 2
                    && asset_ids.iter().all(|asset_id| !asset_id.trim().is_empty())
                    && asset_ids[0] != asset_ids[1]
                    && serde_json::to_string(&asset_ids)
                        .is_ok_and(|encoded| encoded == self.asset_ids) => {}
            _ => report.reject("asset_ids:expected_canonical_two_distinct_ids"),
        }
    }

    fn validate_source_times_and_digests(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        match (
            &self.source_endpoint,
            self.source_endpoint_unavailable_reason,
        ) {
            (Some(endpoint), None) if !endpoint.trim().is_empty() => {}
            (None, Some(SourceEndpointUnavailableReason::NotApplicableNoSourceEvidence)) => {}
            (Some(_), Some(_)) => report.reject("source_endpoint_unavailable_reason:unexpected"),
            (None, None) => report.require("source_endpoint_unavailable_reason", false),
            (Some(_), None) => report.require("source_endpoint", false),
        }

        if let Some(validation_completed) = self.row_evidence_validation_completed_at_utc {
            match self.source_evidence_received_at_utc {
                Some(received) if received <= validation_completed => {}
                Some(_) => report.reject("row_evidence_validation_completed_at_utc:before_receive"),
                None => report.require("source_evidence_received_at_utc", false),
            }
            if validation_completed > self.collector_admission_time_utc_ms {
                report.reject("row_evidence_validation_completed_at_utc:after_admission");
            }
        }
        if self
            .source_evidence_received_at_utc
            .is_some_and(|received| received > self.collector_admission_time_utc_ms)
        {
            report.reject("source_evidence_received_at_utc:after_admission");
        }
        if self
            .lifecycle_applied_at_utc
            .is_some_and(|applied| applied > self.collector_admission_time_utc_ms)
        {
            report.reject("lifecycle_applied_at_utc:after_admission");
        }
        if self
            .runtime_role_applied_at_utc
            .is_some_and(|applied| applied > self.collector_admission_time_utc_ms)
        {
            report.reject("runtime_role_applied_at_utc:after_admission");
        }
        if let (Some(fetch_start), Some(received)) = (
            self.metadata_fetch_start_utc,
            self.source_evidence_received_at_utc,
        ) && fetch_start > received
        {
            report.reject("metadata_fetch_start_utc:after_source_receive");
        }

        if let Some(digest) = self.source_evidence_raw_sha256.as_deref() {
            if !is_sha256_hex(digest) {
                report.reject("source_evidence_raw_sha256:invalid");
            }
            if let Some(payload) = self.source_evidence_payload_json.as_deref()
                && sha256_hex(payload.as_bytes()) != digest
            {
                report.reject("source_evidence_raw_sha256:payload_mismatch");
            }
        } else if self.source_evidence_payload_json.is_some() {
            report.require("source_evidence_raw_sha256", false);
        }
        if self
            .source_evidence_payload_json
            .as_deref()
            .is_some_and(|payload| serde_json::from_str::<serde_json::Value>(payload).is_err())
        {
            report.reject("source_evidence_payload_json:invalid_json");
        }

        if let Some(digest) = self.resolution_evidence_raw_sha256.as_deref() {
            if !is_sha256_hex(digest) {
                report.reject("resolution_evidence_raw_sha256:invalid");
            }
            if self.source_evidence_raw_sha256.as_deref() != Some(digest) {
                report.reject("resolution_evidence_raw_sha256:source_digest_mismatch");
            }
        }
    }

    fn validate_closed_time(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        match self.closed_time_check_passed {
            Some(true) => {
                report.require(
                    "closed_time_raw",
                    non_empty(self.closed_time_raw.as_deref()),
                );
                report.require("closed_time_utc", self.closed_time_utc.is_some());
                report.require_null(
                    "closed_time_check_failure_reason",
                    self.closed_time_check_failure_reason.is_none(),
                );
                self.validate_closed_time_raw_matches_utc(report);
                if self
                    .closed_time_utc
                    .is_some_and(|closed_time| closed_time < self.market_end_utc)
                {
                    report.reject("closed_time_utc:before_market_end");
                }
            }
            Some(false) => match self.closed_time_check_failure_reason {
                Some(ClosedTimeCheckFailureReason::Missing) => {
                    report.require_null("closed_time_raw", self.closed_time_raw.is_none());
                    report.require_null("closed_time_utc", self.closed_time_utc.is_none());
                }
                Some(ClosedTimeCheckFailureReason::ParseError) => {
                    report.require(
                        "closed_time_raw",
                        non_empty(self.closed_time_raw.as_deref()),
                    );
                    report.require_null("closed_time_utc", self.closed_time_utc.is_none());
                    if self
                        .closed_time_raw
                        .as_deref()
                        .and_then(parse_polymarket_gamma_closed_time_utc_ms)
                        .is_some()
                    {
                        report.reject("closed_time_raw:unexpected_parse_success");
                    }
                }
                Some(ClosedTimeCheckFailureReason::BeforeMarketEnd) => {
                    report.require(
                        "closed_time_raw",
                        non_empty(self.closed_time_raw.as_deref()),
                    );
                    report.require("closed_time_utc", self.closed_time_utc.is_some());
                    self.validate_closed_time_raw_matches_utc(report);
                    if self
                        .closed_time_utc
                        .is_some_and(|closed_time| closed_time >= self.market_end_utc)
                    {
                        report.reject("closed_time_utc:not_before_market_end");
                    }
                }
                None => report.require("closed_time_check_failure_reason", false),
            },
            None => report.require_null(
                "closed_time_check_failure_reason",
                self.closed_time_check_failure_reason.is_none(),
            ),
        }
    }

    fn validate_closed_time_raw_matches_utc(
        &self,
        report: &mut PolymarketMarketMetadataV3Validation,
    ) {
        if let (Some(raw), Some(parsed_utc)) =
            (self.closed_time_raw.as_deref(), self.closed_time_utc)
            && parse_polymarket_gamma_closed_time_utc_ms(raw) != Some(parsed_utc)
        {
            report.reject("closed_time_utc:raw_mismatch");
        }
    }

    fn validate_active_reason(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        match (self.active, self.active_unavailable_reason) {
            (Some(_), None) | (None, Some(_)) => {}
            (Some(_), Some(_)) => report.reject("active_unavailable_reason:unexpected"),
            (None, None) => report.require("active_unavailable_reason", false),
        }
    }

    fn validate_followup_horizon(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        if let Some(horizon) = self.resolution_budget_horizon_utc
            && horizon != self.market_end_utc.saturating_add(8 * 60_000)
        {
            report.reject("resolution_budget_horizon_utc:not_e_plus_8m");
        }
        if let (Some(ordinal), Some(scheduled)) = (
            self.resolution_poll_attempt_ordinal,
            self.resolution_poll_scheduled_utc,
        ) {
            if !(1..=6).contains(&ordinal) {
                report.reject("resolution_poll_attempt_ordinal:outside_1_to_6");
            }
            let expected = self
                .market_end_utc
                .saturating_add((i64::from(ordinal) + 2) * 60_000);
            if scheduled != expected {
                report.reject("resolution_poll_scheduled_utc:not_absolute_slot");
            }
        }
    }

    fn validate_metadata_snapshot(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        if self.metadata_origin != PolymarketMetadataOrigin::GammaMarketsDiscovery {
            report.reject("metadata_origin:metadata_snapshot");
        }
        if !matches!(
            (self.market_lifecycle_state, self.market_runtime_role),
            (MarketLifecycleState::Prefetched, MarketRuntimeRole::Next)
                | (MarketLifecycleState::Active, MarketRuntimeRole::Current)
        ) {
            report.reject("metadata_snapshot:invalid_state_role");
        }
        self.require_discovery_evidence(report, true, true, true);
        if self.source_evidence_received_at_utc != Some(self.timestamp_received) {
            report.reject("metadata_snapshot:timestamp_not_source_receive");
        }
        if self
            .row_evidence_validation_completed_at_utc
            .is_some_and(|validation| self.fixed_identity_verified_at_utc > validation)
        {
            report.reject("metadata_snapshot:fixed_identity_after_row_validation");
        }
        self.require_no_lifecycle_transition(report);
        self.require_no_runtime_role_transition(report);
        self.require_no_attempt(report);
        self.require_no_terminal_ownership(report);
        self.require_no_summary_counters(report);
        report.require_null(
            "resolution_budget_horizon_utc",
            self.resolution_budget_horizon_utc.is_none(),
        );
        report.require_null(
            "resolution_followup_status",
            self.resolution_followup_status.is_none(),
        );
        report.require_null(
            "terminal_predicate_passed",
            self.terminal_predicate_passed.is_none(),
        );
        report.require_null(
            "terminal_predicate_failure_reason",
            self.terminal_predicate_failure_reason.is_none(),
        );
        report.require_null(
            "response_fields_unavailable_reason",
            self.response_fields_unavailable_reason.is_none(),
        );
        report.require_null(
            "ws_platform_timestamp_raw",
            self.ws_platform_timestamp_raw.is_none(),
        );
        report.require_null(
            "ws_platform_timestamp_utc",
            self.ws_platform_timestamp_utc.is_none(),
        );
        if self.active.is_none()
            && self.active_unavailable_reason != Some(ActiveUnavailableReason::SourceFieldAbsent)
        {
            report.reject("active_unavailable_reason:metadata_snapshot");
        }
    }

    fn validate_market_state_transition(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        if self.metadata_origin != PolymarketMetadataOrigin::LocalMarketStateTimer {
            report.reject("metadata_origin:market_state_transition");
        }
        self.require_no_attempt(report);
        self.require_no_terminal_ownership(report);
        self.require_no_summary_counters(report);
        report.require_null(
            "terminal_predicate_passed",
            self.terminal_predicate_passed.is_none(),
        );
        report.require_null(
            "terminal_predicate_failure_reason",
            self.terminal_predicate_failure_reason.is_none(),
        );

        match (
            self.market_lifecycle_transition,
            self.market_runtime_role_transition,
        ) {
            (
                Some(MarketLifecycleTransition::PrefetchedToActive),
                Some(MarketRuntimeRoleTransition::NextToCurrent),
            ) => self.validate_prefetched_to_active_transition(report),
            (
                Some(MarketLifecycleTransition::CandidateToActive),
                Some(MarketRuntimeRoleTransition::NoneToCurrent),
            ) => self.validate_candidate_to_active_transition(report),
            (None, Some(MarketRuntimeRoleTransition::CurrentToResolutionPending)) => {
                self.validate_current_to_pending_transition(report)
            }
            _ => report.reject("market_state_transition:unsupported_subtype"),
        }
    }

    fn validate_prefetched_to_active_transition(
        &self,
        report: &mut PolymarketMarketMetadataV3Validation,
    ) {
        if (self.market_lifecycle_state, self.market_runtime_role)
            != (MarketLifecycleState::Active, MarketRuntimeRole::Current)
        {
            report.reject("prefetched_to_active:invalid_state_role");
        }
        self.require_local_response_fields_null(report);
        self.require_discovery_evidence(report, false, false, false);
        self.require_timer_transition_times(report, self.market_start_utc, true);
        report.require_null(
            "resolution_budget_horizon_utc",
            self.resolution_budget_horizon_utc.is_none(),
        );
        report.require_null(
            "resolution_followup_status",
            self.resolution_followup_status.is_none(),
        );
    }

    fn validate_candidate_to_active_transition(
        &self,
        report: &mut PolymarketMarketMetadataV3Validation,
    ) {
        if (self.market_lifecycle_state, self.market_runtime_role)
            != (MarketLifecycleState::Active, MarketRuntimeRole::Current)
        {
            report.reject("candidate_to_active:invalid_state_role");
        }
        report.require_null(
            "response_fields_unavailable_reason",
            self.response_fields_unavailable_reason.is_none(),
        );
        if self.active.is_none()
            && self.active_unavailable_reason != Some(ActiveUnavailableReason::SourceFieldAbsent)
        {
            report.reject("active_unavailable_reason:candidate_to_active");
        }
        self.require_discovery_evidence(report, true, true, true);
        self.require_timer_transition_times(report, self.market_start_utc, true);
        if self.row_evidence_validation_completed_at_utc
            != Some(self.fixed_identity_verified_at_utc)
        {
            report.reject("candidate_to_active:fixed_identity_not_validation");
        }
        if let (Some(validation), Some(applied)) = (
            self.row_evidence_validation_completed_at_utc,
            self.lifecycle_applied_at_utc,
        ) && validation > applied
        {
            report.reject("candidate_to_active:validation_after_applied");
        }
        if self.lifecycle_applied_at_utc.is_some_and(|applied| {
            applied < self.market_start_utc || applied >= self.market_end_utc
        }) {
            report.reject("candidate_to_active:applied_outside_active_window");
        }
        report.require_null(
            "resolution_budget_horizon_utc",
            self.resolution_budget_horizon_utc.is_none(),
        );
        report.require_null(
            "resolution_followup_status",
            self.resolution_followup_status.is_none(),
        );
    }

    fn validate_current_to_pending_transition(
        &self,
        report: &mut PolymarketMarketMetadataV3Validation,
    ) {
        if (self.market_lifecycle_state, self.market_runtime_role)
            != (
                MarketLifecycleState::Active,
                MarketRuntimeRole::ResolutionPending,
            )
        {
            report.reject("current_to_resolution_pending:invalid_state_role");
        }
        self.require_local_response_fields_null(report);
        self.require_discovery_evidence(report, false, false, false);
        self.require_no_lifecycle_transition(report);
        report.require(
            "runtime_role_scheduled_at_utc",
            self.runtime_role_scheduled_at_utc == Some(self.market_end_utc),
        );
        report.require(
            "runtime_role_applied_at_utc",
            self.runtime_role_applied_at_utc.is_some(),
        );
        if self
            .runtime_role_applied_at_utc
            .is_some_and(|applied| applied < self.market_end_utc)
        {
            report.reject("current_to_resolution_pending:applied_before_market_end");
        }
        report.require(
            "resolution_budget_horizon_utc",
            self.resolution_budget_horizon_utc.is_some(),
        );
        if self.resolution_followup_status != Some(ResolutionFollowupStatus::Pending) {
            report.reject("resolution_followup_status:current_to_resolution_pending");
        }
    }

    fn validate_resolution_poll_attempt(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        report.require(
            "resolution_poll_attempt_ordinal",
            self.resolution_poll_attempt_ordinal.is_some(),
        );
        report.require(
            "resolution_poll_scheduled_utc",
            self.resolution_poll_scheduled_utc.is_some(),
        );
        report.require(
            "resolution_poll_result",
            self.resolution_poll_result.is_some(),
        );
        report.require(
            "resolution_budget_horizon_utc",
            self.resolution_budget_horizon_utc.is_some(),
        );
        if let (Some(scheduled), Some(fetch_start)) = (
            self.resolution_poll_scheduled_utc,
            self.metadata_fetch_start_utc,
        ) {
            if fetch_start < scheduled {
                report.reject("metadata_fetch_start_utc:before_poll_scheduled");
            }
            if self.timestamp_received < fetch_start {
                report.reject("timestamp_received:before_metadata_fetch_start");
            }
        }
        self.require_no_summary_counters(report);
        report.require_null(
            "source_evidence_channel",
            self.source_evidence_channel.is_none(),
        );
        report.require_null(
            "ws_platform_timestamp_raw",
            self.ws_platform_timestamp_raw.is_none(),
        );
        report.require_null(
            "ws_platform_timestamp_utc",
            self.ws_platform_timestamp_utc.is_none(),
        );

        match self.resolution_poll_result {
            Some(ResolutionPollResult::Terminal) => self.validate_terminal_gamma_attempt(report),
            Some(ResolutionPollResult::ValidNonterminal) => {
                self.validate_valid_nonterminal_attempt(report)
            }
            Some(ResolutionPollResult::TransportUnavailable) => {
                self.validate_transport_attempt(report)
            }
            Some(ResolutionPollResult::IntegrityError) => self.validate_integrity_attempt(report),
            Some(ResolutionPollResult::MissedDueToDowntime) => self.validate_missed_attempt(report),
            Some(ResolutionPollResult::PostTerminalClaimObservation) => {
                self.validate_post_terminal_attempt(report)
            }
            None => {}
        }
    }

    fn validate_terminal_gamma_attempt(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        self.require_gamma_decoded_evidence(report);
        self.require_gamma_terminal_response(report);
        self.require_completed_terminal_transition(report);
        if self.metadata_origin != PolymarketMetadataOrigin::GammaBySlug {
            report.reject("metadata_origin:terminal_gamma_attempt");
        }
        if self.resolution_followup_status != Some(ResolutionFollowupStatus::TerminalGammaFallback)
        {
            report.reject("resolution_followup_status:terminal_gamma_attempt");
        }
        if self.terminal_claim_owner != Some(TerminalClaimOwner::GammaBySlug) {
            report.reject("terminal_claim_owner:terminal_gamma_attempt");
        }
        if self.resolution_evidence_source != Some(ResolutionEvidenceSource::GammaBySlug) {
            report.reject("resolution_evidence_source:terminal_gamma_attempt");
        }
        self.require_owned_terminal_values(report);
        if self.terminal_predicate_passed != Some(true) {
            report.reject("terminal_predicate_passed:terminal_gamma_attempt");
        }
        report.require_null(
            "terminal_predicate_failure_reason",
            self.terminal_predicate_failure_reason.is_none(),
        );
        report.require_null(
            "resolution_transport_error_class",
            self.resolution_transport_error_class.is_none(),
        );
        report.require_null(
            "resolution_transport_http_status",
            self.resolution_transport_http_status.is_none(),
        );
        report.require_null(
            "resolution_integrity_error_class",
            self.resolution_integrity_error_class.is_none(),
        );
    }

    fn validate_valid_nonterminal_attempt(
        &self,
        report: &mut PolymarketMarketMetadataV3Validation,
    ) {
        self.require_pending_attempt_state(report);
        self.require_gamma_decoded_evidence(report);
        if self.metadata_origin != PolymarketMetadataOrigin::GammaBySlug {
            report.reject("metadata_origin:valid_nonterminal_attempt");
        }
        if self.terminal_predicate_passed != Some(false) {
            report.reject("terminal_predicate_passed:valid_nonterminal_attempt");
        }
        if !self
            .terminal_predicate_failure_reason
            .is_some_and(|reason| reason.poll_result() == ResolutionPollResult::ValidNonterminal)
        {
            report.reject("terminal_predicate_failure_reason:valid_nonterminal_attempt");
        }
        self.require_no_terminal_ownership(report);
        report.require_null(
            "resolution_transport_error_class",
            self.resolution_transport_error_class.is_none(),
        );
        report.require_null(
            "resolution_transport_http_status",
            self.resolution_transport_http_status.is_none(),
        );
        report.require_null(
            "resolution_integrity_error_class",
            self.resolution_integrity_error_class.is_none(),
        );
    }

    fn validate_transport_attempt(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        self.require_pending_attempt_state(report);
        self.require_gamma_request_without_response(report);
        if self.metadata_origin != PolymarketMetadataOrigin::GammaBySlug {
            report.reject("metadata_origin:transport_attempt");
        }
        if self.response_fields_unavailable_reason
            != Some(ResponseFieldsUnavailableReason::TransportUnavailable)
        {
            report.reject("response_fields_unavailable_reason:transport_attempt");
        }
        if self.active_unavailable_reason != Some(ActiveUnavailableReason::SourceFieldAbsent) {
            report.reject("active_unavailable_reason:transport_attempt");
        }
        report.require(
            "resolution_transport_error_class",
            self.resolution_transport_error_class.is_some(),
        );
        match self.resolution_transport_error_class {
            Some(ResolutionTransportErrorClass::HttpStatus)
            | Some(ResolutionTransportErrorClass::RateLimited) => report.require(
                "resolution_transport_http_status",
                self.resolution_transport_http_status.is_some(),
            ),
            Some(_) | None => report.require_null(
                "resolution_transport_http_status",
                self.resolution_transport_http_status.is_none(),
            ),
        }
        report.require_null(
            "resolution_integrity_error_class",
            self.resolution_integrity_error_class.is_none(),
        );
        self.require_not_evaluated_predicate(report);
        self.require_no_terminal_ownership(report);
    }

    fn validate_integrity_attempt(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        self.require_pending_attempt_state(report);
        if self.metadata_origin != PolymarketMetadataOrigin::GammaBySlug {
            report.reject("metadata_origin:integrity_attempt");
        }
        report.require(
            "resolution_integrity_error_class",
            self.resolution_integrity_error_class.is_some(),
        );
        report.require_null(
            "resolution_transport_error_class",
            self.resolution_transport_error_class.is_none(),
        );
        report.require_null(
            "resolution_transport_http_status",
            self.resolution_transport_http_status.is_none(),
        );
        self.require_no_terminal_ownership(report);

        match self.resolution_integrity_error_class {
            Some(ResolutionIntegrityErrorClass::SdkDecodeOrSchema) => {
                self.require_gamma_request_without_response(report);
                if self.response_fields_unavailable_reason
                    != Some(ResponseFieldsUnavailableReason::SdkDecodeOrSchemaError)
                {
                    report.reject("response_fields_unavailable_reason:sdk_decode_attempt");
                }
                if self.active_unavailable_reason
                    != Some(ActiveUnavailableReason::SourceFieldAbsent)
                {
                    report.reject("active_unavailable_reason:sdk_decode_attempt");
                }
                self.require_not_evaluated_predicate(report);
            }
            Some(ResolutionIntegrityErrorClass::AttemptBindingInvariant) => {
                self.require_gamma_decoded_evidence(report);
                self.require_not_evaluated_predicate(report);
            }
            Some(ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction) => {
                self.require_gamma_decoded_evidence(report);
                if self.terminal_predicate_passed != Some(false) {
                    report.reject("terminal_predicate_passed:source_integrity_attempt");
                }
                if !self
                    .terminal_predicate_failure_reason
                    .is_some_and(|reason| {
                        reason.integrity_error_class()
                            == Some(
                                ResolutionIntegrityErrorClass::SourceIdentityOrShapeContradiction,
                            )
                    })
                {
                    report.reject("terminal_predicate_failure_reason:source_integrity_attempt");
                }
            }
            Some(ResolutionIntegrityErrorClass::ClosedTimeInvalid) => {
                self.require_gamma_decoded_evidence(report);
                if self.terminal_predicate_passed != Some(false) {
                    report.reject("terminal_predicate_passed:closed_time_integrity_attempt");
                }
                if !self
                    .terminal_predicate_failure_reason
                    .is_some_and(|reason| {
                        reason.integrity_error_class()
                            == Some(ResolutionIntegrityErrorClass::ClosedTimeInvalid)
                    })
                {
                    report
                        .reject("terminal_predicate_failure_reason:closed_time_integrity_attempt");
                }
            }
            Some(ResolutionIntegrityErrorClass::DerivedWinnerInvariant) => {
                self.require_gamma_decoded_evidence(report);
                self.require_not_evaluated_predicate(report);
            }
            None => {}
        }
    }

    fn validate_missed_attempt(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        self.require_pending_attempt_state(report);
        if self.metadata_origin != PolymarketMetadataOrigin::LocalFollowupScheduler {
            report.reject("metadata_origin:missed_attempt");
        }
        self.require_no_source_evidence(report);
        report.require_null(
            "metadata_fetch_start_utc",
            self.metadata_fetch_start_utc.is_none(),
        );
        self.require_response_fields_null(report);
        if self.source_endpoint_unavailable_reason
            != Some(SourceEndpointUnavailableReason::NotApplicableNoSourceEvidence)
        {
            report.reject("source_endpoint_unavailable_reason:missed_attempt");
        }
        if self.response_fields_unavailable_reason
            != Some(ResponseFieldsUnavailableReason::MissedDueToDowntime)
        {
            report.reject("response_fields_unavailable_reason:missed_attempt");
        }
        if self.active_unavailable_reason != Some(ActiveUnavailableReason::NotApplicableForRowKind)
        {
            report.reject("active_unavailable_reason:missed_attempt");
        }
        report.require_null(
            "resolution_transport_error_class",
            self.resolution_transport_error_class.is_none(),
        );
        report.require_null(
            "resolution_transport_http_status",
            self.resolution_transport_http_status.is_none(),
        );
        report.require_null(
            "resolution_integrity_error_class",
            self.resolution_integrity_error_class.is_none(),
        );
        self.require_not_evaluated_predicate(report);
        self.require_no_terminal_ownership(report);
    }

    fn validate_post_terminal_attempt(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        self.require_pending_attempt_state(report);
        self.require_gamma_decoded_evidence(report);
        if self.metadata_origin != PolymarketMetadataOrigin::GammaBySlug {
            report.reject("metadata_origin:post_terminal_attempt");
        }
        if self.terminal_claim_owner != Some(TerminalClaimOwner::MarketResolvedWs) {
            report.reject("terminal_claim_owner:post_terminal_attempt");
        }
        report.require(
            "terminal_predicate_passed",
            self.terminal_predicate_passed.is_some(),
        );
        if self.terminal_predicate_passed == Some(true) {
            report.require_null(
                "terminal_predicate_failure_reason",
                self.terminal_predicate_failure_reason.is_none(),
            );
        } else if self.terminal_predicate_passed == Some(false)
            && !self
                .terminal_predicate_failure_reason
                .is_some_and(|reason| {
                    reason.poll_result() == ResolutionPollResult::ValidNonterminal
                })
        {
            report.reject("terminal_predicate_failure_reason:post_terminal_attempt");
        }
        for (field, is_null) in [
            ("winning_asset_id", self.winning_asset_id.is_none()),
            ("winning_outcome", self.winning_outcome.is_none()),
            (
                "resolution_evidence_source",
                self.resolution_evidence_source.is_none(),
            ),
            (
                "resolution_evidence_raw_sha256",
                self.resolution_evidence_raw_sha256.is_none(),
            ),
        ] {
            report.require_null(field, is_null);
        }
        report.require_null(
            "resolution_transport_error_class",
            self.resolution_transport_error_class.is_none(),
        );
        report.require_null(
            "resolution_transport_http_status",
            self.resolution_transport_http_status.is_none(),
        );
        report.require_null(
            "resolution_integrity_error_class",
            self.resolution_integrity_error_class.is_none(),
        );
    }

    fn validate_resolution_terminal_ws(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        if self.metadata_origin != PolymarketMetadataOrigin::MarketResolvedWs {
            report.reject("metadata_origin:resolution_terminal_ws");
        }
        if self.source_evidence_origin != Some(SourceEvidenceOrigin::MarketResolvedWs) {
            report.reject("source_evidence_origin:resolution_terminal_ws");
        }
        if self.source_evidence_event_type != Some(SourceEvidenceEventType::MarketResolved) {
            report.reject("source_evidence_event_type:resolution_terminal_ws");
        }
        if self.source_evidence_channel != Some(SourceEvidenceChannel::Market) {
            report.reject("source_evidence_channel:resolution_terminal_ws");
        }
        report.require(
            "source_endpoint",
            non_empty(self.source_endpoint.as_deref()),
        );
        report.require_null(
            "source_endpoint_unavailable_reason",
            self.source_endpoint_unavailable_reason.is_none(),
        );
        self.require_source_payload_evidence(report, true);
        if self.source_evidence_received_at_utc != Some(self.timestamp_received) {
            report.reject("resolution_terminal_ws:timestamp_not_source_receive");
        }
        report.require_null(
            "metadata_fetch_start_utc",
            self.metadata_fetch_start_utc.is_none(),
        );
        report.require(
            "ws_platform_timestamp_raw",
            non_empty(self.ws_platform_timestamp_raw.as_deref()),
        );
        report.require(
            "ws_platform_timestamp_utc",
            self.ws_platform_timestamp_utc
                .is_some_and(|timestamp| timestamp > 0),
        );
        if let (Some(raw), Some(parsed_utc)) = (
            self.ws_platform_timestamp_raw.as_deref(),
            self.ws_platform_timestamp_utc,
        ) && raw.trim().parse::<i64>().ok() != Some(parsed_utc)
        {
            report.reject("ws_platform_timestamp_utc:raw_mismatch");
        }
        self.require_gamma_only_fields_null(report);
        if self.active_unavailable_reason != Some(ActiveUnavailableReason::NotApplicableForRowKind)
        {
            report.reject("active_unavailable_reason:resolution_terminal_ws");
        }
        report.require_null(
            "response_fields_unavailable_reason",
            self.response_fields_unavailable_reason.is_none(),
        );
        self.require_completed_terminal_transition(report);
        if self.resolution_followup_status != Some(ResolutionFollowupStatus::TerminalWs) {
            report.reject("resolution_followup_status:resolution_terminal_ws");
        }
        if self.terminal_claim_owner != Some(TerminalClaimOwner::MarketResolvedWs) {
            report.reject("terminal_claim_owner:resolution_terminal_ws");
        }
        if self.resolution_evidence_source != Some(ResolutionEvidenceSource::MarketResolvedWs) {
            report.reject("resolution_evidence_source:resolution_terminal_ws");
        }
        self.require_owned_terminal_values(report);
        if self.terminal_predicate_passed != Some(true) {
            report.reject("terminal_predicate_passed:resolution_terminal_ws");
        }
        report.require_null(
            "terminal_predicate_failure_reason",
            self.terminal_predicate_failure_reason.is_none(),
        );
        self.require_no_attempt(report);
        self.require_no_summary_counters(report);
    }

    fn validate_resolution_followup_summary(
        &self,
        report: &mut PolymarketMarketMetadataV3Validation,
    ) {
        if self.metadata_origin != PolymarketMetadataOrigin::LocalFollowupScheduler {
            report.reject("metadata_origin:resolution_followup_summary");
        }
        if (self.market_lifecycle_state, self.market_runtime_role)
            != (MarketLifecycleState::Active, MarketRuntimeRole::None)
        {
            report.reject("resolution_followup_summary:invalid_state_role");
        }
        self.require_no_source_evidence(report);
        if self.source_endpoint_unavailable_reason
            != Some(SourceEndpointUnavailableReason::NotApplicableNoSourceEvidence)
        {
            report.reject("source_endpoint_unavailable_reason:resolution_followup_summary");
        }
        report.require_null(
            "metadata_fetch_start_utc",
            self.metadata_fetch_start_utc.is_none(),
        );
        self.require_response_fields_null(report);
        if self.active_unavailable_reason != Some(ActiveUnavailableReason::NotApplicableForRowKind)
        {
            report.reject("active_unavailable_reason:resolution_followup_summary");
        }
        if self.response_fields_unavailable_reason
            != Some(ResponseFieldsUnavailableReason::NotApplicableForRowKind)
        {
            report.reject("response_fields_unavailable_reason:resolution_followup_summary");
        }
        self.require_no_lifecycle_transition(report);
        if self.market_runtime_role_transition
            != Some(MarketRuntimeRoleTransition::ResolutionPendingToNone)
        {
            report.reject("market_runtime_role_transition:resolution_followup_summary");
        }
        report.require_null(
            "runtime_role_scheduled_at_utc",
            self.runtime_role_scheduled_at_utc.is_none(),
        );
        report.require(
            "runtime_role_applied_at_utc",
            self.runtime_role_applied_at_utc.is_some(),
        );
        report.require(
            "resolution_budget_horizon_utc",
            self.resolution_budget_horizon_utc.is_some(),
        );
        if let (Some(applied), Some(horizon)) = (
            self.runtime_role_applied_at_utc,
            self.resolution_budget_horizon_utc,
        ) && applied < horizon
        {
            report.reject("resolution_followup_summary:applied_before_budget_horizon");
        }
        if self
            .runtime_role_applied_at_utc
            .is_some_and(|applied| applied > self.timestamp_received)
        {
            report.reject("resolution_followup_summary:applied_after_timestamp_received");
        }
        if !matches!(
            self.resolution_followup_status,
            Some(ResolutionFollowupStatus::BudgetExhaustedAfterValidNonterminal)
                | Some(ResolutionFollowupStatus::BudgetExhaustedWithoutValidResponse)
        ) {
            report.reject("resolution_followup_status:resolution_followup_summary");
        }
        for (field, present) in [
            (
                "resolution_poll_attempts_recorded",
                self.resolution_poll_attempts_recorded.is_some(),
            ),
            (
                "resolution_valid_response_count",
                self.resolution_valid_response_count.is_some(),
            ),
            (
                "resolution_transport_unavailable_count",
                self.resolution_transport_unavailable_count.is_some(),
            ),
            (
                "resolution_missed_slot_count",
                self.resolution_missed_slot_count.is_some(),
            ),
        ] {
            report.require(field, present);
        }
        if let (Some(attempts), Some(valid), Some(transport), Some(missed)) = (
            self.resolution_poll_attempts_recorded,
            self.resolution_valid_response_count,
            self.resolution_transport_unavailable_count,
            self.resolution_missed_slot_count,
        ) {
            if attempts != 6 || attempts != valid.saturating_add(transport).saturating_add(missed) {
                report.reject("resolution_followup_summary:counters_not_six");
            }
            if self.resolution_followup_status
                == Some(ResolutionFollowupStatus::BudgetExhaustedAfterValidNonterminal)
                && (valid == 0 || self.last_valid_metadata_received_utc.is_none())
            {
                report.reject("resolution_followup_summary:missing_last_valid_receive");
            }
            if self.resolution_followup_status
                == Some(ResolutionFollowupStatus::BudgetExhaustedWithoutValidResponse)
                && (valid != 0 || self.last_valid_metadata_received_utc.is_some())
            {
                report.reject("resolution_followup_summary:unexpected_valid_response");
            }
        }
        if let (Some(last_valid), Some(applied)) = (
            self.last_valid_metadata_received_utc,
            self.runtime_role_applied_at_utc,
        ) && last_valid > applied
        {
            report.reject("resolution_followup_summary:last_valid_after_role_exit");
        }
        self.require_no_attempt(report);
        self.require_no_terminal_ownership(report);
        self.require_not_evaluated_predicate(report);
    }

    fn require_discovery_evidence(
        &self,
        report: &mut PolymarketMarketMetadataV3Validation,
        payload_required: bool,
        validation_required: bool,
        fetch_required: bool,
    ) {
        report.require(
            "source_endpoint",
            non_empty(self.source_endpoint.as_deref()),
        );
        report.require_null(
            "source_endpoint_unavailable_reason",
            self.source_endpoint_unavailable_reason.is_none(),
        );
        if self.source_evidence_origin != Some(SourceEvidenceOrigin::GammaMarketsDiscovery) {
            report.reject("source_evidence_origin:expected_gamma_markets_discovery");
        }
        if self.source_evidence_event_type
            != Some(SourceEvidenceEventType::GammaMarketsDiscoveryResponse)
        {
            report.reject("source_evidence_event_type:expected_discovery_response");
        }
        report.require_null(
            "source_evidence_channel",
            self.source_evidence_channel.is_none(),
        );
        report.require(
            "source_evidence_received_at_utc",
            self.source_evidence_received_at_utc.is_some(),
        );
        report.require(
            "source_evidence_raw_sha256",
            non_empty(self.source_evidence_raw_sha256.as_deref()),
        );
        if payload_required {
            report.require(
                "source_evidence_payload_json",
                non_empty(self.source_evidence_payload_json.as_deref()),
            );
        } else {
            report.require_null(
                "source_evidence_payload_json",
                self.source_evidence_payload_json.is_none(),
            );
        }
        if validation_required {
            report.require(
                "row_evidence_validation_completed_at_utc",
                self.row_evidence_validation_completed_at_utc.is_some(),
            );
        } else {
            report.require_null(
                "row_evidence_validation_completed_at_utc",
                self.row_evidence_validation_completed_at_utc.is_none(),
            );
        }
        if fetch_required {
            report.require(
                "metadata_fetch_start_utc",
                self.metadata_fetch_start_utc.is_some(),
            );
        } else {
            report.require_null(
                "metadata_fetch_start_utc",
                self.metadata_fetch_start_utc.is_none(),
            );
        }
    }

    fn require_gamma_decoded_evidence(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        report.require(
            "source_endpoint",
            non_empty(self.source_endpoint.as_deref()),
        );
        report.require_null(
            "source_endpoint_unavailable_reason",
            self.source_endpoint_unavailable_reason.is_none(),
        );
        if self.source_evidence_origin != Some(SourceEvidenceOrigin::GammaBySlug) {
            report.reject("source_evidence_origin:expected_gamma_by_slug");
        }
        if self.source_evidence_event_type
            != Some(SourceEvidenceEventType::GammaMarketBySlugResponse)
        {
            report.reject("source_evidence_event_type:expected_gamma_by_slug_response");
        }
        self.require_source_payload_evidence(report, true);
        report.require(
            "metadata_fetch_start_utc",
            self.metadata_fetch_start_utc.is_some(),
        );
        report.require_null(
            "response_fields_unavailable_reason",
            self.response_fields_unavailable_reason.is_none(),
        );
        if self.source_evidence_received_at_utc != Some(self.timestamp_received) {
            report.reject("resolution_poll_attempt:timestamp_not_source_receive");
        }
        if self.active.is_none()
            && self.active_unavailable_reason != Some(ActiveUnavailableReason::SourceFieldAbsent)
        {
            report.reject("active_unavailable_reason:decoded_gamma_response");
        }
    }

    fn require_source_payload_evidence(
        &self,
        report: &mut PolymarketMarketMetadataV3Validation,
        validation_required: bool,
    ) {
        report.require(
            "source_evidence_received_at_utc",
            self.source_evidence_received_at_utc.is_some(),
        );
        report.require(
            "source_evidence_payload_json",
            non_empty(self.source_evidence_payload_json.as_deref()),
        );
        report.require(
            "source_evidence_raw_sha256",
            non_empty(self.source_evidence_raw_sha256.as_deref()),
        );
        if validation_required {
            report.require(
                "row_evidence_validation_completed_at_utc",
                self.row_evidence_validation_completed_at_utc.is_some(),
            );
        }
    }

    fn require_gamma_request_without_response(
        &self,
        report: &mut PolymarketMarketMetadataV3Validation,
    ) {
        report.require(
            "source_endpoint",
            non_empty(self.source_endpoint.as_deref()),
        );
        report.require_null(
            "source_endpoint_unavailable_reason",
            self.source_endpoint_unavailable_reason.is_none(),
        );
        report.require(
            "metadata_fetch_start_utc",
            self.metadata_fetch_start_utc.is_some(),
        );
        self.require_no_source_evidence(report);
        self.require_response_fields_null(report);
    }

    fn require_no_source_evidence(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        for (field, is_null) in [
            (
                "source_evidence_origin",
                self.source_evidence_origin.is_none(),
            ),
            (
                "source_evidence_event_type",
                self.source_evidence_event_type.is_none(),
            ),
            (
                "source_evidence_channel",
                self.source_evidence_channel.is_none(),
            ),
            (
                "source_evidence_received_at_utc",
                self.source_evidence_received_at_utc.is_none(),
            ),
            (
                "row_evidence_validation_completed_at_utc",
                self.row_evidence_validation_completed_at_utc.is_none(),
            ),
            (
                "source_evidence_payload_json",
                self.source_evidence_payload_json.is_none(),
            ),
            (
                "source_evidence_raw_sha256",
                self.source_evidence_raw_sha256.is_none(),
            ),
        ] {
            report.require_null(field, is_null);
        }
    }

    fn require_response_fields_null(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        self.require_gamma_only_fields_null(report);
        report.require_null(
            "ws_platform_timestamp_raw",
            self.ws_platform_timestamp_raw.is_none(),
        );
        report.require_null(
            "ws_platform_timestamp_utc",
            self.ws_platform_timestamp_utc.is_none(),
        );
    }

    fn require_gamma_only_fields_null(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        for (field, is_null) in [
            ("closed", self.closed.is_none()),
            ("closed_time_raw", self.closed_time_raw.is_none()),
            ("closed_time_utc", self.closed_time_utc.is_none()),
            (
                "closed_time_check_passed",
                self.closed_time_check_passed.is_none(),
            ),
            (
                "closed_time_check_failure_reason",
                self.closed_time_check_failure_reason.is_none(),
            ),
            (
                "automatically_resolved",
                self.automatically_resolved.is_none(),
            ),
            (
                "uma_resolution_status",
                self.uma_resolution_status.is_none(),
            ),
            ("outcomes_raw", self.outcomes_raw.is_none()),
            ("outcome_prices_raw", self.outcome_prices_raw.is_none()),
            ("active", self.active.is_none()),
            ("accepting_orders", self.accepting_orders.is_none()),
        ] {
            report.require_null(field, is_null);
        }
    }

    fn require_local_response_fields_null(
        &self,
        report: &mut PolymarketMarketMetadataV3Validation,
    ) {
        self.require_response_fields_null(report);
        if self.active_unavailable_reason != Some(ActiveUnavailableReason::NotApplicableForRowKind)
        {
            report.reject("active_unavailable_reason:local_row");
        }
        if self.response_fields_unavailable_reason
            != Some(ResponseFieldsUnavailableReason::NotApplicableForRowKind)
        {
            report.reject("response_fields_unavailable_reason:local_row");
        }
    }

    fn require_no_lifecycle_transition(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        for (field, is_null) in [
            (
                "market_lifecycle_transition",
                self.market_lifecycle_transition.is_none(),
            ),
            (
                "lifecycle_scheduled_at_utc",
                self.lifecycle_scheduled_at_utc.is_none(),
            ),
            (
                "lifecycle_applied_at_utc",
                self.lifecycle_applied_at_utc.is_none(),
            ),
        ] {
            report.require_null(field, is_null);
        }
    }

    fn require_no_runtime_role_transition(
        &self,
        report: &mut PolymarketMarketMetadataV3Validation,
    ) {
        for (field, is_null) in [
            (
                "market_runtime_role_transition",
                self.market_runtime_role_transition.is_none(),
            ),
            (
                "runtime_role_scheduled_at_utc",
                self.runtime_role_scheduled_at_utc.is_none(),
            ),
            (
                "runtime_role_applied_at_utc",
                self.runtime_role_applied_at_utc.is_none(),
            ),
        ] {
            report.require_null(field, is_null);
        }
    }

    fn require_no_attempt(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        for (field, is_null) in [
            (
                "resolution_poll_attempt_ordinal",
                self.resolution_poll_attempt_ordinal.is_none(),
            ),
            (
                "resolution_poll_scheduled_utc",
                self.resolution_poll_scheduled_utc.is_none(),
            ),
            (
                "resolution_poll_result",
                self.resolution_poll_result.is_none(),
            ),
            (
                "resolution_transport_error_class",
                self.resolution_transport_error_class.is_none(),
            ),
            (
                "resolution_transport_http_status",
                self.resolution_transport_http_status.is_none(),
            ),
            (
                "resolution_integrity_error_class",
                self.resolution_integrity_error_class.is_none(),
            ),
        ] {
            report.require_null(field, is_null);
        }
    }

    fn require_no_terminal_ownership(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        for (field, is_null) in [
            ("winning_asset_id", self.winning_asset_id.is_none()),
            ("winning_outcome", self.winning_outcome.is_none()),
            (
                "resolution_evidence_source",
                self.resolution_evidence_source.is_none(),
            ),
            (
                "resolution_evidence_raw_sha256",
                self.resolution_evidence_raw_sha256.is_none(),
            ),
            ("terminal_claim_owner", self.terminal_claim_owner.is_none()),
        ] {
            report.require_null(field, is_null);
        }
    }

    fn require_no_summary_counters(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        for (field, is_null) in [
            (
                "resolution_poll_attempts_recorded",
                self.resolution_poll_attempts_recorded.is_none(),
            ),
            (
                "resolution_valid_response_count",
                self.resolution_valid_response_count.is_none(),
            ),
            (
                "resolution_transport_unavailable_count",
                self.resolution_transport_unavailable_count.is_none(),
            ),
            (
                "resolution_missed_slot_count",
                self.resolution_missed_slot_count.is_none(),
            ),
            (
                "last_valid_metadata_received_utc",
                self.last_valid_metadata_received_utc.is_none(),
            ),
        ] {
            report.require_null(field, is_null);
        }
    }

    fn require_not_evaluated_predicate(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        report.require_null(
            "terminal_predicate_passed",
            self.terminal_predicate_passed.is_none(),
        );
        report.require_null(
            "terminal_predicate_failure_reason",
            self.terminal_predicate_failure_reason.is_none(),
        );
    }

    fn require_pending_attempt_state(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        if (self.market_lifecycle_state, self.market_runtime_role)
            != (
                MarketLifecycleState::Active,
                MarketRuntimeRole::ResolutionPending,
            )
        {
            report.reject("resolution_poll_attempt:invalid_pending_state_role");
        }
        self.require_no_lifecycle_transition(report);
        self.require_no_runtime_role_transition(report);
        if self.resolution_followup_status != Some(ResolutionFollowupStatus::Pending) {
            report.reject("resolution_followup_status:pending_attempt");
        }
    }

    fn require_gamma_terminal_response(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        if self.closed != Some(true) {
            report.reject("closed:terminal_gamma_attempt");
        }
        if self.accepting_orders != Some(false) {
            report.reject("accepting_orders:terminal_gamma_attempt");
        }
        if self.closed_time_check_passed != Some(true) {
            report.reject("closed_time_check_passed:terminal_gamma_attempt");
        }
        if self
            .uma_resolution_status
            .as_deref()
            .is_none_or(|status| !status.trim().eq_ignore_ascii_case("resolved"))
        {
            report.reject("uma_resolution_status:terminal_gamma_attempt");
        }
        report.require("outcomes_raw", non_empty(self.outcomes_raw.as_deref()));
        report.require(
            "outcome_prices_raw",
            non_empty(self.outcome_prices_raw.as_deref()),
        );
    }

    fn require_completed_terminal_transition(
        &self,
        report: &mut PolymarketMarketMetadataV3Validation,
    ) {
        if (self.market_lifecycle_state, self.market_runtime_role)
            != (MarketLifecycleState::Retired, MarketRuntimeRole::None)
        {
            report.reject("terminal_transition:invalid_state_role");
        }
        if self.market_lifecycle_transition != Some(MarketLifecycleTransition::ActiveToRetired) {
            report.reject("market_lifecycle_transition:terminal_transition");
        }
        if self.market_runtime_role_transition
            != Some(MarketRuntimeRoleTransition::ResolutionPendingToNone)
        {
            report.reject("market_runtime_role_transition:terminal_transition");
        }
        report.require_null(
            "lifecycle_scheduled_at_utc",
            self.lifecycle_scheduled_at_utc.is_none(),
        );
        report.require_null(
            "runtime_role_scheduled_at_utc",
            self.runtime_role_scheduled_at_utc.is_none(),
        );
        report.require(
            "lifecycle_applied_at_utc",
            self.lifecycle_applied_at_utc.is_some(),
        );
        report.require(
            "runtime_role_applied_at_utc",
            self.runtime_role_applied_at_utc.is_some(),
        );
        if self.lifecycle_applied_at_utc != self.runtime_role_applied_at_utc {
            report.reject("terminal_transition:applied_times_differ");
        }
        if let (Some(validation), Some(applied)) = (
            self.row_evidence_validation_completed_at_utc,
            self.lifecycle_applied_at_utc,
        ) && validation > applied
        {
            report.reject("terminal_transition:validation_after_applied");
        }
        report.require(
            "resolution_budget_horizon_utc",
            self.resolution_budget_horizon_utc.is_some(),
        );
    }

    fn require_timer_transition_times(
        &self,
        report: &mut PolymarketMarketMetadataV3Validation,
        scheduled: i64,
        both_transitions: bool,
    ) {
        report.require(
            "lifecycle_scheduled_at_utc",
            self.lifecycle_scheduled_at_utc == Some(scheduled),
        );
        report.require(
            "lifecycle_applied_at_utc",
            self.lifecycle_applied_at_utc.is_some(),
        );
        if both_transitions {
            report.require(
                "runtime_role_scheduled_at_utc",
                self.runtime_role_scheduled_at_utc == Some(scheduled),
            );
            report.require(
                "runtime_role_applied_at_utc",
                self.runtime_role_applied_at_utc.is_some(),
            );
            if self.lifecycle_applied_at_utc != self.runtime_role_applied_at_utc {
                report.reject("market_state_transition:applied_times_differ");
            }
        }
        if self
            .lifecycle_applied_at_utc
            .is_some_and(|applied| applied < scheduled)
        {
            report.reject("lifecycle_applied_at_utc:before_scheduled");
        }
    }

    fn require_owned_terminal_values(&self, report: &mut PolymarketMarketMetadataV3Validation) {
        report.require(
            "winning_asset_id",
            non_empty(self.winning_asset_id.as_deref()),
        );
        report.require(
            "winning_outcome",
            non_empty(self.winning_outcome.as_deref()),
        );
        report.require(
            "resolution_evidence_raw_sha256",
            non_empty(self.resolution_evidence_raw_sha256.as_deref()),
        );
        if let (Some(winner), Ok(asset_ids)) = (
            self.winning_asset_id.as_deref(),
            serde_json::from_str::<Vec<String>>(&self.asset_ids),
        ) && !asset_ids.iter().any(|asset_id| asset_id == winner)
        {
            report.reject("winning_asset_id:not_in_fixed_identity");
        }
    }
}

fn non_empty(value: Option<&str>) -> bool {
    value.is_some_and(|value| !value.trim().is_empty())
}

pub fn parse_polymarket_gamma_closed_time_utc_ms(raw: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(raw.trim())
        .ok()
        .map(|closed_time| closed_time.timestamp_millis())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn polymarket_market_metadata_v3_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("metadata_row_kind", DataType::Utf8, false),
        v3_timestamp_field("timestamp_received", false),
        Field::new("collector_admission_sequence", DataType::UInt64, false),
        v3_timestamp_field("collector_admission_time_utc_ms", false),
        Field::new("metadata_origin", DataType::Utf8, false),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
        Field::new("slug", DataType::Utf8, false),
        Field::new("condition_id", DataType::Utf8, false),
        Field::new("clob_market_id", DataType::Utf8, false),
        Field::new("asset_ids", DataType::Utf8, false),
        v3_timestamp_field("market_start_utc", false),
        v3_timestamp_field("market_end_utc", false),
        v3_timestamp_field("fixed_identity_verified_at_utc", false),
        Field::new("market_lifecycle_state", DataType::Utf8, false),
        Field::new("market_runtime_role", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, true),
        Field::new("source_endpoint_unavailable_reason", DataType::Utf8, true),
        Field::new("source_evidence_origin", DataType::Utf8, true),
        Field::new("source_evidence_event_type", DataType::Utf8, true),
        Field::new("source_evidence_channel", DataType::Utf8, true),
        v3_timestamp_field("source_evidence_received_at_utc", true),
        v3_timestamp_field("row_evidence_validation_completed_at_utc", true),
        Field::new("source_evidence_payload_json", DataType::Utf8, true),
        Field::new("source_evidence_raw_sha256", DataType::Utf8, true),
        Field::new("market_lifecycle_transition", DataType::Utf8, true),
        v3_timestamp_field("lifecycle_scheduled_at_utc", true),
        v3_timestamp_field("lifecycle_applied_at_utc", true),
        Field::new("market_runtime_role_transition", DataType::Utf8, true),
        v3_timestamp_field("runtime_role_scheduled_at_utc", true),
        v3_timestamp_field("runtime_role_applied_at_utc", true),
        v3_timestamp_field("metadata_fetch_start_utc", true),
        Field::new("closed", DataType::Boolean, true),
        Field::new("closed_time_raw", DataType::Utf8, true),
        v3_timestamp_field("closed_time_utc", true),
        Field::new("closed_time_check_passed", DataType::Boolean, true),
        Field::new("closed_time_check_failure_reason", DataType::Utf8, true),
        Field::new("automatically_resolved", DataType::Boolean, true),
        Field::new("uma_resolution_status", DataType::Utf8, true),
        Field::new("outcomes_raw", DataType::Utf8, true),
        Field::new("outcome_prices_raw", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("active_unavailable_reason", DataType::Utf8, true),
        Field::new("accepting_orders", DataType::Boolean, true),
        Field::new("ws_platform_timestamp_raw", DataType::Utf8, true),
        v3_timestamp_field("ws_platform_timestamp_utc", true),
        Field::new("winning_asset_id", DataType::Utf8, true),
        Field::new("winning_outcome", DataType::Utf8, true),
        Field::new("resolution_evidence_source", DataType::Utf8, true),
        Field::new("resolution_evidence_raw_sha256", DataType::Utf8, true),
        Field::new("terminal_claim_owner", DataType::Utf8, true),
        Field::new("resolution_poll_attempt_ordinal", DataType::UInt8, true),
        v3_timestamp_field("resolution_poll_scheduled_utc", true),
        Field::new("resolution_poll_result", DataType::Utf8, true),
        Field::new("resolution_transport_error_class", DataType::Utf8, true),
        Field::new("resolution_transport_http_status", DataType::UInt16, true),
        Field::new("resolution_integrity_error_class", DataType::Utf8, true),
        v3_timestamp_field("resolution_budget_horizon_utc", true),
        Field::new("resolution_followup_status", DataType::Utf8, true),
        Field::new("resolution_poll_attempts_recorded", DataType::UInt32, true),
        Field::new("resolution_valid_response_count", DataType::UInt32, true),
        Field::new(
            "resolution_transport_unavailable_count",
            DataType::UInt32,
            true,
        ),
        Field::new("resolution_missed_slot_count", DataType::UInt32, true),
        v3_timestamp_field("last_valid_metadata_received_utc", true),
        Field::new("terminal_predicate_passed", DataType::Boolean, true),
        Field::new("terminal_predicate_failure_reason", DataType::Utf8, true),
        Field::new("response_fields_unavailable_reason", DataType::Utf8, true),
    ]))
}

fn v3_timestamp_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        nullable,
    )
}
