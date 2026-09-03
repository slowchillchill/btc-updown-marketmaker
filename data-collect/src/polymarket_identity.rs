#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PolymarketMarketIdentity {
    pub(crate) slug: String,
    pub(crate) condition_id: String,
    pub(crate) clob_market_id: String,
    pub(crate) ordered_up_down_asset_ids_json: String,
    pub(crate) market_start_utc_ms: i64,
    pub(crate) market_end_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct VerifiedPolymarketMarketIdentity {
    pub(crate) market: PolymarketMarketIdentity,
    pub(crate) first_verified_at_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SourceEvidenceLocator {
    pub(crate) received_at_utc_ms: i64,
    pub(crate) raw_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SourceAuthority {
    pub(crate) identity: VerifiedPolymarketMarketIdentity,
    pub(crate) origin: String,
    pub(crate) evidence: SourceEvidenceLocator,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SourceReference {
    pub(crate) identity: VerifiedPolymarketMarketIdentity,
    pub(crate) origin: String,
    pub(crate) evidence: SourceEvidenceLocator,
}

pub(crate) fn same_market_identity(
    left: &PolymarketMarketIdentity,
    right: &PolymarketMarketIdentity,
) -> bool {
    left == right
}

pub(crate) fn same_identity_authority(
    left: &VerifiedPolymarketMarketIdentity,
    right: &VerifiedPolymarketMarketIdentity,
) -> bool {
    same_market_identity(&left.market, &right.market)
        && left.first_verified_at_utc_ms == right.first_verified_at_utc_ms
}

pub(crate) fn source_reference_matches_authority(
    reference: &SourceReference,
    authority: &SourceAuthority,
) -> bool {
    same_identity_authority(&reference.identity, &authority.identity)
        && reference.origin == authority.origin
        && reference.evidence == authority.evidence
}

pub(crate) fn source_reference_candidate_matches_authority(
    reference: &SourceReference,
    authority: &SourceAuthority,
) -> bool {
    same_identity_authority(&reference.identity, &authority.identity)
        && reference.origin == authority.origin
        && reference.evidence.received_at_utc_ms == authority.evidence.received_at_utc_ms
}
