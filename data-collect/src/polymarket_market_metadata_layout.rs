use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use polymarket_client_sdk_v2::types::DateTime;
use serde::{Deserialize, Serialize};

use crate::polymarket_identity::{
    PolymarketMarketIdentity, SourceAuthority, SourceEvidenceLocator, SourceReference,
    VerifiedPolymarketMarketIdentity, source_reference_candidate_matches_authority,
};
use sha2::{Digest as _, Sha256};

pub const POLYMARKET_MARKET_METADATA_V2_SCHEMA_VERSION: &str = "polymarket_market_metadata_v2";
pub const POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION: &str = "polymarket_market_metadata_v3";
pub const POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION: &str = "polymarket_market_metadata_v4";
pub const POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION: &str = "polymarket_market_metadata_v5";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketLifecycleState {
    Candidate,
    Prefetched,
    Active,
    Retired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionTransportErrorClass {
    Connect,
    Timeout,
    HttpStatus,
    RateLimited,
    SdkTransport,
}

impl ResolutionTransportErrorClass {
    /// First-match order for the public typed transport classification contract:
    /// HTTP 429, other HTTP status, timeout, connect, then SDK transport fallback.
    pub const TYPED_CLASSIFICATION_PRECEDENCE: [Self; 5] = [
        Self::RateLimited,
        Self::HttpStatus,
        Self::Timeout,
        Self::Connect,
        Self::SdkTransport,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::Timeout => "timeout",
            Self::HttpStatus => "http_status",
            Self::RateLimited => "rate_limited",
            Self::SdkTransport => "sdk_transport",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
pub struct PolymarketMarketMetadataValidation {
    pub violations: Vec<String>,
    validate_legacy_resolution_digest: bool,
}

impl PolymarketMarketMetadataValidation {
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

    fn validates_legacy_resolution_digest(&self) -> bool {
        self.validate_legacy_resolution_digest
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PolymarketMarketMetadataRow {
    pub metadata_row_kind: PolymarketMetadataRowKind,
    pub timestamp_received: i64,
    #[serde(skip)]
    pub collector_admission_sequence: u64,
    #[serde(skip)]
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
    pub terminal_predicate_failure_reason: Option<TerminalPredicateFailureReason>,
    pub response_fields_unavailable_reason: Option<ResponseFieldsUnavailableReason>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolymarketMarketMetadataV4Row {
    #[serde(flatten)]
    pub common: PolymarketMarketMetadataRow,
    pub ws_claimant_source_sequence_epoch_id: Option<String>,
    pub ws_claimant_collector_admission_sequence: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMarketMetadataV5Row {
    common: PolymarketMarketMetadataRow,
    pub ws_claimant_source_sequence_epoch_id: Option<String>,
    pub ws_claimant_collector_admission_sequence: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolymarketMetadataReferenceIntegrityError {
    Missing { slug: String },
    Multiple { slug: String, matches: usize },
    DigestMismatch { slug: String },
}

impl std::fmt::Display for PolymarketMetadataReferenceIntegrityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing { slug } => {
                write!(formatter, "missing metadata source authority for {slug}")
            }
            Self::Multiple { slug, matches } => {
                write!(
                    formatter,
                    "multiple metadata source authorities for {slug}:matches={matches}"
                )
            }
            Self::DigestMismatch { slug } => {
                write!(
                    formatter,
                    "metadata source authority digest mismatch for {slug}"
                )
            }
        }
    }
}

impl std::error::Error for PolymarketMetadataReferenceIntegrityError {}

pub fn validate_polymarket_metadata_local_references(
    rows: &[PolymarketMarketMetadataRow],
) -> Result<(), PolymarketMetadataReferenceIntegrityError> {
    let authorities = rows
        .iter()
        .filter_map(source_authority_from_metadata_row)
        .collect::<Vec<_>>();
    let references = rows
        .iter()
        .filter_map(|row| source_reference_from_metadata_row(row).transpose())
        .collect::<Result<Vec<_>, _>>()?;
    validate_polymarket_source_references(&authorities, &references)
}

pub(crate) fn validate_polymarket_source_references(
    authorities: &[SourceAuthority],
    references: &[SourceReference],
) -> Result<(), PolymarketMetadataReferenceIntegrityError> {
    for reference in references {
        let mut candidates = 0_usize;
        let mut matches = 0_usize;
        for authority in authorities {
            if source_reference_candidate_matches_authority(reference, authority) {
                candidates += 1;
                if reference.evidence.raw_sha256 == authority.evidence.raw_sha256 {
                    matches += 1;
                }
            }
        }
        match matches {
            0 => {
                if candidates == 0 {
                    return Err(PolymarketMetadataReferenceIntegrityError::Missing {
                        slug: reference.identity.market.slug.clone(),
                    });
                }
                return Err(PolymarketMetadataReferenceIntegrityError::DigestMismatch {
                    slug: reference.identity.market.slug.clone(),
                });
            }
            1 => {}
            _ => {
                return Err(PolymarketMetadataReferenceIntegrityError::Multiple {
                    slug: reference.identity.market.slug.clone(),
                    matches,
                });
            }
        }
    }
    Ok(())
}

pub(crate) fn verified_identity_from_metadata_row(
    row: &PolymarketMarketMetadataRow,
) -> Option<VerifiedPolymarketMarketIdentity> {
    let [up_asset_id, down_asset_id] = serde_json::from_str::<[String; 2]>(&row.asset_ids).ok()?;
    Some(VerifiedPolymarketMarketIdentity {
        market: PolymarketMarketIdentity {
            slug: row.slug.clone(),
            condition_id: row.condition_id.clone(),
            clob_market_id: row.clob_market_id.clone(),
            up_asset_id,
            down_asset_id,
            market_start_utc_ms: row.market_start_utc,
            market_end_utc_ms: row.market_end_utc,
        },
        first_verified_at_utc_ms: row.fixed_identity_verified_at_utc,
    })
}

pub(crate) fn source_authority_from_metadata_row(
    row: &PolymarketMarketMetadataRow,
) -> Option<SourceAuthority> {
    row.source_evidence_payload_json.as_ref()?;
    let raw_sha256 = row
        .source_evidence_raw_sha256
        .clone()
        .filter(|digest| !digest.is_empty())?;
    Some(SourceAuthority {
        identity: verified_identity_from_metadata_row(row)?,
        origin: row.source_evidence_origin?.as_str().to_owned(),
        evidence: SourceEvidenceLocator {
            received_at_utc_ms: row.source_evidence_received_at_utc?,
            raw_sha256,
        },
    })
}

pub(crate) fn source_reference_from_metadata_row(
    row: &PolymarketMarketMetadataRow,
) -> Result<Option<SourceReference>, PolymarketMetadataReferenceIntegrityError> {
    if row.metadata_row_kind != PolymarketMetadataRowKind::MarketStateTransition
        || row.source_evidence_payload_json.is_some()
    {
        return Ok(None);
    }
    let missing = || PolymarketMetadataReferenceIntegrityError::Missing {
        slug: row.slug.clone(),
    };
    Ok(Some(SourceReference {
        identity: verified_identity_from_metadata_row(row).ok_or_else(missing)?,
        origin: row
            .source_evidence_origin
            .map(|origin| origin.as_str().to_owned())
            .ok_or_else(missing)?,
        evidence: SourceEvidenceLocator {
            received_at_utc_ms: row.source_evidence_received_at_utc.ok_or_else(missing)?,
            raw_sha256: row.source_evidence_raw_sha256.clone().ok_or_else(missing)?,
        },
    }))
}

impl PolymarketMarketMetadataRow {
    /// Serializes the producer-side metadata draft. Collector admission is
    /// deliberately omitted because it is assigned by the ingress authority.
    pub fn to_draft_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }

    /// Restores a producer-side draft and injects the collector-owned admission
    /// evidence. Any admission-looking fields in `raw_json` are ignored by serde.
    pub fn from_draft_json_with_admission(
        raw_json: &str,
        collector_admission_sequence: u64,
        collector_admission_time_utc_ms: i64,
    ) -> serde_json::Result<Self> {
        let mut row = serde_json::from_str::<Self>(raw_json)?;
        row.collector_admission_sequence = collector_admission_sequence;
        row.collector_admission_time_utc_ms = collector_admission_time_utc_ms;
        Ok(row)
    }

    pub fn validate_v3(&self) -> PolymarketMarketMetadataValidation {
        self.validate_for_schema(POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION, true, true)
    }

    fn validate_for_schema(
        &self,
        expected_schema_version: &str,
        validate_legacy_resolution_digest: bool,
        require_collector_admission: bool,
    ) -> PolymarketMarketMetadataValidation {
        let mut report = PolymarketMarketMetadataValidation {
            violations: Vec::new(),
            validate_legacy_resolution_digest,
        };
        let asset_ids = serde_json::from_str::<Vec<String>>(&self.asset_ids).ok();
        self.validate_common(
            &mut report,
            expected_schema_version,
            require_collector_admission,
            asset_ids.as_deref(),
        );
        match self.metadata_row_kind {
            PolymarketMetadataRowKind::MetadataSnapshot => {
                self.validate_metadata_snapshot(&mut report)
            }
            PolymarketMetadataRowKind::MarketStateTransition => {
                self.validate_market_state_transition(&mut report)
            }
            PolymarketMetadataRowKind::ResolutionPollAttempt => {
                self.validate_resolution_poll_attempt(&mut report, asset_ids.as_deref())
            }
            PolymarketMetadataRowKind::ResolutionTerminalWs => {
                self.validate_resolution_terminal_ws(&mut report, asset_ids.as_deref())
            }
            PolymarketMetadataRowKind::ResolutionFollowupSummary => {
                self.validate_resolution_followup_summary(&mut report)
            }
        }
        report
    }

    fn validate_common(
        &self,
        report: &mut PolymarketMarketMetadataValidation,
        expected_schema_version: &str,
        require_collector_admission: bool,
        asset_ids: Option<&[String]>,
    ) {
        report.require("timestamp_received", self.timestamp_received > 0);
        if require_collector_admission {
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
        if self.schema_version != expected_schema_version {
            report.reject(match expected_schema_version {
                POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION => "schema_version:expected_v3",
                POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION => "schema_version:expected_v4",
                POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION => "schema_version:expected_v5",
                _ => "schema_version:unexpected",
            });
        }
        self.validate_fixed_identity(report, require_collector_admission, asset_ids);
        self.validate_source_times_and_digests(report, require_collector_admission);
        self.validate_closed_time(report);
        self.validate_active_reason(report);
        self.validate_followup_horizon(report);
    }

    fn validate_fixed_identity(
        &self,
        report: &mut PolymarketMarketMetadataValidation,
        require_collector_admission: bool,
        asset_ids: Option<&[String]>,
    ) {
        report.require("market_start_utc", self.market_start_utc > 0);
        report.require("market_end_utc", self.market_end_utc > 0);
        if self.market_start_utc >= self.market_end_utc {
            report.reject("market_start_utc:not_before_market_end_utc");
        }
        report.require(
            "fixed_identity_verified_at_utc",
            self.fixed_identity_verified_at_utc > 0,
        );
        if require_collector_admission
            && self.fixed_identity_verified_at_utc > self.collector_admission_time_utc_ms
        {
            report.reject("fixed_identity_verified_at_utc:after_admission");
        }

        match asset_ids {
            Some(asset_ids)
                if asset_ids.len() == 2
                    && asset_ids.iter().all(|asset_id| !asset_id.trim().is_empty())
                    && asset_ids[0] != asset_ids[1]
                    && serde_json::to_string(&asset_ids)
                        .is_ok_and(|encoded| encoded == self.asset_ids) => {}
            _ => report.reject("asset_ids:expected_canonical_two_distinct_ids"),
        }
    }

    fn validate_source_times_and_digests(
        &self,
        report: &mut PolymarketMarketMetadataValidation,
        require_collector_admission: bool,
    ) {
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
            if require_collector_admission
                && validation_completed > self.collector_admission_time_utc_ms
            {
                report.reject("row_evidence_validation_completed_at_utc:after_admission");
            }
        }
        if require_collector_admission {
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

        if report.validates_legacy_resolution_digest()
            && let Some(digest) = self.resolution_evidence_raw_sha256.as_deref()
        {
            if !is_sha256_hex(digest) {
                report.reject("resolution_evidence_raw_sha256:invalid");
            }
            if self.source_evidence_raw_sha256.as_deref() != Some(digest) {
                report.reject("resolution_evidence_raw_sha256:source_digest_mismatch");
            }
        }
    }

    fn validate_closed_time(&self, report: &mut PolymarketMarketMetadataValidation) {
        match self.closed_time_check_failure_reason {
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
            None if self.closed_time_utc.is_some() => {
                report.require(
                    "closed_time_raw",
                    non_empty(self.closed_time_raw.as_deref()),
                );
                self.validate_closed_time_raw_matches_utc(report);
                if self
                    .closed_time_utc
                    .is_some_and(|closed_time| closed_time < self.market_end_utc)
                {
                    report.reject("closed_time_utc:before_market_end");
                }
            }
            None => {}
        }
    }

    fn validate_closed_time_raw_matches_utc(
        &self,
        report: &mut PolymarketMarketMetadataValidation,
    ) {
        if let (Some(raw), Some(parsed_utc)) =
            (self.closed_time_raw.as_deref(), self.closed_time_utc)
            && parse_polymarket_gamma_closed_time_utc_ms(raw) != Some(parsed_utc)
        {
            report.reject("closed_time_utc:raw_mismatch");
        }
    }

    fn validate_active_reason(&self, report: &mut PolymarketMarketMetadataValidation) {
        match (self.active, self.active_unavailable_reason) {
            (Some(_), None) | (None, Some(_)) => {}
            (Some(_), Some(_)) => report.reject("active_unavailable_reason:unexpected"),
            (None, None) => report.require("active_unavailable_reason", false),
        }
    }

    fn validate_followup_horizon(&self, report: &mut PolymarketMarketMetadataValidation) {
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

    fn validate_metadata_snapshot(&self, report: &mut PolymarketMarketMetadataValidation) {
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

    fn validate_market_state_transition(&self, report: &mut PolymarketMarketMetadataValidation) {
        if self.metadata_origin != PolymarketMetadataOrigin::LocalMarketStateTimer {
            report.reject("metadata_origin:market_state_transition");
        }
        self.require_no_attempt(report);
        self.require_no_terminal_ownership(report);
        self.require_no_summary_counters(report);
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
        report: &mut PolymarketMarketMetadataValidation,
    ) {
        if (self.market_lifecycle_state, self.market_runtime_role)
            != (MarketLifecycleState::Active, MarketRuntimeRole::Current)
        {
            report.reject("prefetched_to_active:invalid_state_role");
        }
        self.require_local_response_fields_null(report);
        self.require_discovery_evidence(report, false, false, false);
        self.require_timer_transition_times(report, self.market_start_utc);
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
        report: &mut PolymarketMarketMetadataValidation,
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
        self.require_timer_transition_times(report, self.market_start_utc);
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
        report: &mut PolymarketMarketMetadataValidation,
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

    fn validate_resolution_poll_attempt(
        &self,
        report: &mut PolymarketMarketMetadataValidation,
        asset_ids: Option<&[String]>,
    ) {
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
            Some(ResolutionPollResult::Terminal) => {
                self.validate_terminal_gamma_attempt(report, asset_ids)
            }
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

    fn validate_terminal_gamma_attempt(
        &self,
        report: &mut PolymarketMarketMetadataValidation,
        asset_ids: Option<&[String]>,
    ) {
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
        self.require_owned_terminal_values(report, asset_ids);
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

    fn validate_valid_nonterminal_attempt(&self, report: &mut PolymarketMarketMetadataValidation) {
        self.require_pending_attempt_state(report);
        self.require_gamma_decoded_evidence(report);
        if self.metadata_origin != PolymarketMetadataOrigin::GammaBySlug {
            report.reject("metadata_origin:valid_nonterminal_attempt");
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

    fn validate_transport_attempt(&self, report: &mut PolymarketMarketMetadataValidation) {
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

    fn validate_integrity_attempt(&self, report: &mut PolymarketMarketMetadataValidation) {
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

    fn validate_missed_attempt(&self, report: &mut PolymarketMarketMetadataValidation) {
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

    fn validate_post_terminal_attempt(&self, report: &mut PolymarketMarketMetadataValidation) {
        self.require_pending_attempt_state(report);
        self.require_gamma_decoded_evidence(report);
        if self.metadata_origin != PolymarketMetadataOrigin::GammaBySlug {
            report.reject("metadata_origin:post_terminal_attempt");
        }
        if self.terminal_claim_owner != Some(TerminalClaimOwner::MarketResolvedWs) {
            report.reject("terminal_claim_owner:post_terminal_attempt");
        }
        if self
            .terminal_predicate_failure_reason
            .is_some_and(|reason| reason.poll_result() != ResolutionPollResult::ValidNonterminal)
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
        ] {
            report.require_null(field, is_null);
        }
        if report.validates_legacy_resolution_digest() {
            report.require_null(
                "resolution_evidence_raw_sha256",
                self.resolution_evidence_raw_sha256.is_none(),
            );
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

    fn validate_resolution_terminal_ws(
        &self,
        report: &mut PolymarketMarketMetadataValidation,
        asset_ids: Option<&[String]>,
    ) {
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
        self.require_source_payload_evidence(report);
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
        self.require_owned_terminal_values(report, asset_ids);
        report.require_null(
            "terminal_predicate_failure_reason",
            self.terminal_predicate_failure_reason.is_none(),
        );
        self.require_no_attempt(report);
        self.require_no_summary_counters(report);
    }

    fn validate_resolution_followup_summary(
        &self,
        report: &mut PolymarketMarketMetadataValidation,
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
        report: &mut PolymarketMarketMetadataValidation,
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

    fn require_gamma_decoded_evidence(&self, report: &mut PolymarketMarketMetadataValidation) {
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
        self.require_source_payload_evidence(report);
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

    fn require_source_payload_evidence(&self, report: &mut PolymarketMarketMetadataValidation) {
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
        report.require(
            "row_evidence_validation_completed_at_utc",
            self.row_evidence_validation_completed_at_utc.is_some(),
        );
    }

    fn require_gamma_request_without_response(
        &self,
        report: &mut PolymarketMarketMetadataValidation,
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

    fn require_no_source_evidence(&self, report: &mut PolymarketMarketMetadataValidation) {
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

    fn require_response_fields_null(&self, report: &mut PolymarketMarketMetadataValidation) {
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

    fn require_gamma_only_fields_null(&self, report: &mut PolymarketMarketMetadataValidation) {
        for (field, is_null) in [
            ("closed", self.closed.is_none()),
            ("closed_time_raw", self.closed_time_raw.is_none()),
            ("closed_time_utc", self.closed_time_utc.is_none()),
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

    fn require_local_response_fields_null(&self, report: &mut PolymarketMarketMetadataValidation) {
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

    fn require_no_lifecycle_transition(&self, report: &mut PolymarketMarketMetadataValidation) {
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

    fn require_no_runtime_role_transition(&self, report: &mut PolymarketMarketMetadataValidation) {
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

    fn require_no_attempt(&self, report: &mut PolymarketMarketMetadataValidation) {
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

    fn require_no_terminal_ownership(&self, report: &mut PolymarketMarketMetadataValidation) {
        for (field, is_null) in [
            ("winning_asset_id", self.winning_asset_id.is_none()),
            ("winning_outcome", self.winning_outcome.is_none()),
            (
                "resolution_evidence_source",
                self.resolution_evidence_source.is_none(),
            ),
            ("terminal_claim_owner", self.terminal_claim_owner.is_none()),
        ] {
            report.require_null(field, is_null);
        }
        if report.validates_legacy_resolution_digest() {
            report.require_null(
                "resolution_evidence_raw_sha256",
                self.resolution_evidence_raw_sha256.is_none(),
            );
        }
    }

    fn require_no_summary_counters(&self, report: &mut PolymarketMarketMetadataValidation) {
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

    fn require_not_evaluated_predicate(&self, report: &mut PolymarketMarketMetadataValidation) {
        report.require_null(
            "terminal_predicate_failure_reason",
            self.terminal_predicate_failure_reason.is_none(),
        );
    }

    fn require_pending_attempt_state(&self, report: &mut PolymarketMarketMetadataValidation) {
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

    fn require_gamma_terminal_response(&self, report: &mut PolymarketMarketMetadataValidation) {
        if self.closed != Some(true) {
            report.reject("closed:terminal_gamma_attempt");
        }
        if self.accepting_orders != Some(false) {
            report.reject("accepting_orders:terminal_gamma_attempt");
        }
        report.require("closed_time_utc", self.closed_time_utc.is_some());
        report.require_null(
            "closed_time_check_failure_reason",
            self.closed_time_check_failure_reason.is_none(),
        );
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
        report: &mut PolymarketMarketMetadataValidation,
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
        report: &mut PolymarketMarketMetadataValidation,
        scheduled: i64,
    ) {
        report.require(
            "lifecycle_scheduled_at_utc",
            self.lifecycle_scheduled_at_utc == Some(scheduled),
        );
        report.require(
            "lifecycle_applied_at_utc",
            self.lifecycle_applied_at_utc.is_some(),
        );
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
        if self
            .lifecycle_applied_at_utc
            .is_some_and(|applied| applied < scheduled)
        {
            report.reject("lifecycle_applied_at_utc:before_scheduled");
        }
    }

    fn require_owned_terminal_values(
        &self,
        report: &mut PolymarketMarketMetadataValidation,
        asset_ids: Option<&[String]>,
    ) {
        report.require(
            "winning_asset_id",
            non_empty(self.winning_asset_id.as_deref()),
        );
        report.require(
            "winning_outcome",
            non_empty(self.winning_outcome.as_deref()),
        );
        if report.validates_legacy_resolution_digest() {
            report.require(
                "resolution_evidence_raw_sha256",
                non_empty(self.resolution_evidence_raw_sha256.as_deref()),
            );
        }
        if let (Some(winner), Some(asset_ids)) = (self.winning_asset_id.as_deref(), asset_ids)
            && !asset_ids.iter().any(|asset_id| asset_id == winner)
        {
            report.reject("winning_asset_id:not_in_fixed_identity");
        }
    }
}

impl PolymarketMarketMetadataV4Row {
    pub fn from_common(
        mut common: PolymarketMarketMetadataRow,
        ws_claimant_source_sequence_epoch_id: Option<String>,
        ws_claimant_collector_admission_sequence: Option<u64>,
    ) -> Self {
        common.schema_version = POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION.to_owned();
        Self {
            common,
            ws_claimant_source_sequence_epoch_id,
            ws_claimant_collector_admission_sequence,
        }
    }

    pub fn validate(&self) -> PolymarketMarketMetadataValidation {
        let mut report = self.common.validate_for_schema(
            POLYMARKET_MARKET_METADATA_V4_SCHEMA_VERSION,
            true,
            true,
        );
        validate_ws_claimant_pair(
            &self.common,
            self.ws_claimant_source_sequence_epoch_id.as_deref(),
            self.ws_claimant_collector_admission_sequence,
            &mut report,
        );
        report
    }
}

impl PolymarketMarketMetadataV5Row {
    pub fn from_common(
        mut common: PolymarketMarketMetadataRow,
        ws_claimant_source_sequence_epoch_id: Option<String>,
        ws_claimant_collector_admission_sequence: Option<u64>,
    ) -> Self {
        common.schema_version = POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION.to_owned();
        common.resolution_evidence_raw_sha256 = None;
        Self {
            common,
            ws_claimant_source_sequence_epoch_id,
            ws_claimant_collector_admission_sequence,
        }
    }

    pub fn validate(&self) -> PolymarketMarketMetadataValidation {
        self.validate_for_admission_stage(true)
    }

    pub(crate) fn validate_draft(&self) -> PolymarketMarketMetadataValidation {
        self.validate_for_admission_stage(false)
    }

    fn validate_for_admission_stage(
        &self,
        require_collector_admission: bool,
    ) -> PolymarketMarketMetadataValidation {
        let mut report = self.common.validate_for_schema(
            POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION,
            false,
            require_collector_admission,
        );
        validate_ws_claimant_pair(
            &self.common,
            self.ws_claimant_source_sequence_epoch_id.as_deref(),
            self.ws_claimant_collector_admission_sequence,
            &mut report,
        );
        report
    }

    pub fn schema_version(&self) -> &str {
        &self.common.schema_version
    }

    pub(crate) fn common(&self) -> &PolymarketMarketMetadataRow {
        &self.common
    }

    pub(crate) fn common_mut(&mut self) -> &mut PolymarketMarketMetadataRow {
        &mut self.common
    }

    pub(crate) fn into_common(self) -> PolymarketMarketMetadataRow {
        self.common
    }
}

fn validate_ws_claimant_pair(
    common: &PolymarketMarketMetadataRow,
    claimant_epoch: Option<&str>,
    claimant_sequence: Option<u64>,
    report: &mut PolymarketMarketMetadataValidation,
) {
    let claimant_required = common.metadata_row_kind
        == PolymarketMetadataRowKind::ResolutionTerminalWs
        && common.terminal_claim_owner == Some(TerminalClaimOwner::MarketResolvedWs);
    if claimant_required {
        report.require(
            "ws_claimant_source_sequence_epoch_id",
            non_empty(claimant_epoch),
        );
        report.require(
            "ws_claimant_collector_admission_sequence",
            claimant_sequence.is_some_and(|sequence| sequence > 0),
        );
    } else {
        report.require_null(
            "ws_claimant_source_sequence_epoch_id",
            claimant_epoch.is_none(),
        );
        report.require_null(
            "ws_claimant_collector_admission_sequence",
            claimant_sequence.is_none(),
        );
    }
}

fn non_empty(value: Option<&str>) -> bool {
    value.is_some_and(|value| !value.trim().is_empty())
}

pub fn parse_polymarket_gamma_closed_time_utc_ms(raw: &str) -> Option<i64> {
    let raw = raw.trim();
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .or_else(|| {
            let prefix = raw.strip_suffix("+00")?;
            DateTime::parse_from_rfc3339(&format!("{prefix}+00:00")).ok()
        })
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
        Field::new("terminal_predicate_failure_reason", DataType::Utf8, true),
        Field::new("response_fields_unavailable_reason", DataType::Utf8, true),
    ]))
}

pub fn polymarket_market_metadata_v4_schema() -> Arc<Schema> {
    let mut fields = polymarket_market_metadata_v3_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new("ws_claimant_source_sequence_epoch_id", DataType::Utf8, true),
        Field::new(
            "ws_claimant_collector_admission_sequence",
            DataType::UInt64,
            true,
        ),
    ]);
    Arc::new(Schema::new(fields))
}

pub fn polymarket_market_metadata_v5_schema() -> Arc<Schema> {
    Arc::new(Schema::new(
        polymarket_market_metadata_v4_schema()
            .fields()
            .iter()
            .filter(|field| field.name() != "resolution_evidence_raw_sha256")
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>(),
    ))
}

fn v3_timestamp_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        nullable,
    )
}
