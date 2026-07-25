use super::*;
use crate::polymarket_market_metadata_layout::PolymarketMarketMetadataV4Row;

#[derive(Clone, Debug)]
pub(super) struct TargetAdmittedPolymarketMessage<'a> {
    pub(super) message: &'a WsMessage,
    pub(super) slug: String,
    pub(super) matched_assets: BTreeSet<String>,
}

pub(super) fn target_admitted_polymarket_message<'a>(
    message: &'a WsMessage,
    discovery: &BtcUpdownDiscovery,
    asset_to_slug: &BTreeMap<String, String>,
) -> Result<Option<TargetAdmittedPolymarketMessage<'a>>> {
    let mut matched_assets = BTreeSet::new();
    let (message, slug) = match message {
        WsMessage::Book(event) => {
            let asset_id = event.asset_id.to_string();
            let Some(slug) = verify_polymarket_business_asset(
                discovery,
                asset_to_slug,
                &event.market.to_string(),
                &asset_id,
            )?
            else {
                return Ok(None);
            };
            matched_assets.insert(asset_id);
            (message, slug.to_owned())
        }
        WsMessage::PriceChange(event) => {
            let condition_id = event.market.to_string();
            let mut matched_slug = None::<String>;
            for change in &event.price_changes {
                let asset_id = change.asset_id.to_string();
                let Some(slug) = verify_polymarket_business_asset(
                    discovery,
                    asset_to_slug,
                    &condition_id,
                    &asset_id,
                )?
                else {
                    continue;
                };
                if matched_slug
                    .as_deref()
                    .is_some_and(|matched| matched != slug)
                {
                    return Err(invalid_data(
                        "Polymarket price_change matched multiple lifecycle slugs",
                    )
                    .into());
                }
                matched_slug = Some(slug.to_owned());
                matched_assets.insert(asset_id);
            }
            let Some(slug) = matched_slug else {
                return Ok(None);
            };
            (message, slug)
        }
        WsMessage::TickSizeChange(event) => {
            let asset_id = event.asset_id.to_string();
            let Some(slug) = verify_polymarket_business_asset(
                discovery,
                asset_to_slug,
                &event.market.to_string(),
                &asset_id,
            )?
            else {
                return Ok(None);
            };
            matched_assets.insert(asset_id);
            (message, slug.to_owned())
        }
        WsMessage::LastTradePrice(event) => {
            let asset_id = event.asset_id.to_string();
            let Some(slug) = verify_polymarket_business_asset(
                discovery,
                asset_to_slug,
                &event.market.to_string(),
                &asset_id,
            )?
            else {
                return Ok(None);
            };
            matched_assets.insert(asset_id);
            (message, slug.to_owned())
        }
        WsMessage::BestBidAsk(event) => {
            let asset_id = event.asset_id.to_string();
            let Some(slug) = verify_polymarket_business_asset(
                discovery,
                asset_to_slug,
                &event.market.to_string(),
                &asset_id,
            )?
            else {
                return Ok(None);
            };
            matched_assets.insert(asset_id);
            (message, slug.to_owned())
        }
        WsMessage::NewMarket(event) => {
            let Some((market, assets)) = verify_target_market_identity(
                discovery,
                &event.id,
                &event.market.to_string(),
                event.slug.as_deref(),
                &event.asset_ids,
                Some(&event.outcomes),
                None,
                None,
            )?
            else {
                return Ok(None);
            };
            matched_assets = assets;
            (message, market.slug.clone())
        }
        WsMessage::MarketResolved(_) => {
            return Err(invalid_data(
                "market_resolved must use the validate-first typed terminal handler",
            )
            .into());
        }
        WsMessage::Trade(_) | WsMessage::Order(_) => return Ok(None),
        _ => {
            return Err(
                invalid_data("unsupported target Polymarket SDK market-data variant").into(),
            );
        }
    };
    Ok(Some(TargetAdmittedPolymarketMessage {
        message,
        slug,
        matched_assets,
    }))
}

impl TargetAdmittedPolymarketMessage<'_> {
    pub(super) fn polymarket_v3_projection(&self) -> Result<PolymarketOrderbookV3Projection> {
        let mut projection =
            PolymarketOrderbookV3Projection::from_target_admitted_sdk_message(self.message)?
                .ok_or_else(|| {
                    invalid_data("target-admitted Polymarket v3 message was not projectable")
                })?;
        if let crate::polymarket_orderbook_v3::PolymarketOrderbookV3Event::PriceChange(
            price_change,
        ) = &mut projection.event
        {
            price_change
                .price_changes
                .retain(|change| self.matched_assets.contains(&change.asset_id));
        }
        Ok(projection.with_slug(Some(self.slug.clone())))
    }
}

fn verify_polymarket_business_asset<'a>(
    discovery: &'a BtcUpdownDiscovery,
    asset_to_slug: &BTreeMap<String, String>,
    condition_id: &str,
    asset_id: &str,
) -> Result<Option<&'a str>> {
    let Some(market) = std::iter::once(&discovery.current)
        .chain(discovery.next.as_ref())
        .find(|market| market.up_asset_id == asset_id || market.down_asset_id == asset_id)
    else {
        return Ok(None);
    };
    if market.condition_id != condition_id {
        return Err(invalid_data(format!(
            "Polymarket target asset {asset_id} carried contradictory condition_id {condition_id}"
        ))
        .into());
    }
    if asset_to_slug.get(asset_id).map(String::as_str) != Some(market.slug.as_str()) {
        return Ok(None);
    }
    Ok(Some(market.slug.as_str()))
}

#[allow(clippy::too_many_arguments)]
fn verify_target_market_identity<'a>(
    discovery: &'a BtcUpdownDiscovery,
    clob_market_id: &str,
    condition_id: &str,
    slug: Option<&str>,
    asset_ids: &[U256],
    outcomes: Option<&[String]>,
    winning_asset_id: Option<&U256>,
    winning_outcome: Option<&str>,
) -> Result<
    Option<(
        &'a crate::market_discovery::DiscoveredBtcUpdownMarket,
        BTreeSet<String>,
    )>,
> {
    let observed_assets = asset_ids
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    let target_assets = discovery.asset_ids();
    let observed_target_assets = observed_assets
        .intersection(&target_assets)
        .cloned()
        .collect::<BTreeSet<_>>();
    let winning_asset = winning_asset_id.map(ToString::to_string);
    let winning_target_asset = winning_asset
        .as_ref()
        .filter(|asset| target_assets.contains(*asset));
    let markets = std::iter::once(&discovery.current).chain(discovery.next.as_ref());
    let id_match = markets.clone().find(|market| market.id == clob_market_id);
    let condition_match = markets
        .clone()
        .find(|market| market.condition_id == condition_id);
    let slug_match = slug.and_then(|slug| markets.clone().find(|market| market.slug == slug));
    if id_match.is_none()
        && condition_match.is_none()
        && slug_match.is_none()
        && observed_target_assets.is_empty()
        && winning_target_asset.is_none()
    {
        return Ok(None);
    }

    let asset_match = markets.clone().find(|market| {
        observed_target_assets.contains(&market.up_asset_id)
            || observed_target_assets.contains(&market.down_asset_id)
            || winning_target_asset
                .is_some_and(|asset| asset == &market.up_asset_id || asset == &market.down_asset_id)
    });
    let expected = condition_match
        .or(slug_match)
        .or(id_match)
        .or(asset_match)
        .ok_or_else(|| {
            invalid_data(format!(
                "Polymarket target identity could not be mapped to condition_id {condition_id}"
            ))
        })?;
    if clob_market_id != expected.id {
        return Err(invalid_data(format!(
            "Polymarket target identity carried unexpected market id {clob_market_id}"
        ))
        .into());
    }
    if condition_id != expected.condition_id {
        return Err(invalid_data(format!(
            "Polymarket target identity carried unexpected condition_id {condition_id}"
        ))
        .into());
    }
    if slug.is_some_and(|slug| slug != expected.slug.as_str()) {
        return Err(invalid_data(format!(
            "Polymarket condition {} carried unexpected slug {:?}",
            expected.condition_id, slug
        ))
        .into());
    }
    let expected_assets =
        BTreeSet::from([expected.up_asset_id.clone(), expected.down_asset_id.clone()]);
    let assets_valid = asset_ids.len() == expected_assets.len()
        && observed_assets.len() == expected_assets.len()
        && observed_assets == expected_assets;
    if !assets_valid {
        return Err(invalid_data(format!(
            "Polymarket condition {} carried contradictory asset mapping",
            expected.condition_id
        ))
        .into());
    }
    if let Some(outcomes) = outcomes
        && (outcomes.len() != 2
            || asset_ids.iter().zip(outcomes).any(|(asset, outcome)| {
                let expected_asset = match outcome.trim().to_ascii_lowercase().as_str() {
                    "up" => &expected.up_asset_id,
                    "down" => &expected.down_asset_id,
                    _ => return true,
                };
                asset.to_string() != *expected_asset
            }))
    {
        return Err(invalid_data(format!(
            "Polymarket condition {} carried contradictory outcome mapping",
            expected.condition_id
        ))
        .into());
    }
    if winning_asset
        .as_ref()
        .is_some_and(|asset| !expected_assets.contains(asset))
    {
        return Err(invalid_data(format!(
            "Polymarket condition {} carried a winning asset outside its subscribed asset set",
            expected.condition_id
        ))
        .into());
    }
    if let (Some(winning_asset), Some(winning_outcome)) = (&winning_asset, winning_outcome) {
        let expected_winning_outcome = if winning_asset == &expected.up_asset_id {
            "up"
        } else {
            "down"
        };
        if !same_outcome_label(winning_outcome, expected_winning_outcome) {
            return Err(invalid_data(format!(
                "Polymarket condition {} carried contradictory winning outcome",
                expected.condition_id
            ))
            .into());
        }
    }
    Ok(Some((expected, expected_assets)))
}

fn polymarket_v3_base_row(
    identity: &PendingPolymarketIdentity,
    row_kind: PolymarketMetadataRowKind,
    origin: PolymarketMetadataOrigin,
    timestamp_received: i64,
    lifecycle: MarketLifecycleState,
    runtime_role: MarketRuntimeRole,
    collector_identity: &CollectorIdentity,
) -> Result<PolymarketMarketMetadataV3Row> {
    Ok(PolymarketMarketMetadataV3Row {
        metadata_row_kind: row_kind,
        timestamp_received,
        collector_admission_sequence: 0,
        collector_admission_time_utc_ms: 0,
        metadata_origin: origin,
        collector_host_id: collector_identity.host_id.clone(),
        collector_region: collector_identity.region.clone(),
        schema_version: POLYMARKET_MARKET_METADATA_V3_SCHEMA_VERSION.to_owned(),
        slug: identity.slug.clone(),
        condition_id: identity.condition_id.clone(),
        clob_market_id: identity.clob_market_id.clone(),
        asset_ids: serde_json::to_string(&[
            identity.up_asset_id.as_str(),
            identity.down_asset_id.as_str(),
        ])?,
        market_start_utc: identity.market_start_utc_ms,
        market_end_utc: identity.market_end_utc_ms,
        fixed_identity_verified_at_utc: identity.fixed_identity_verified_at_utc_ms,
        market_lifecycle_state: lifecycle,
        market_runtime_role: runtime_role,
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
    })
}

pub(super) fn polymarket_metadata_snapshot_v3_row(
    market: &DiscoveredBtcUpdownMarket,
    role: MarketRuntimeRole,
    collector_identity: &CollectorIdentity,
) -> Result<PolymarketMarketMetadataV3Row> {
    let identity = PendingPolymarketIdentity::from_discovered(market)?;
    let received = market
        .metadata_received_utc_ms
        .ok_or_else(|| invalid_data("Polymarket metadata snapshot is missing receive time"))?;
    let payload = market
        .raw_json
        .as_deref()
        .filter(|payload| !payload.trim().is_empty())
        .ok_or_else(|| invalid_data("Polymarket metadata snapshot is missing source payload"))?;
    let digest = Sha256::digest(payload.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if digest != identity.discovery_source_raw_sha256 {
        return Err(invalid_data(format!(
            "Polymarket metadata snapshot digest mismatch:slug={}",
            identity.slug
        ))
        .into());
    }
    let sdk_market = serde_json::from_str::<SdkGammaMarket>(payload).map_err(|error| {
        invalid_data(format!(
            "Polymarket metadata snapshot SDK payload decode failed:slug={}:error={error}",
            identity.slug
        ))
    })?;
    let lifecycle = match role {
        MarketRuntimeRole::Current => MarketLifecycleState::Active,
        MarketRuntimeRole::Next => MarketLifecycleState::Prefetched,
        _ => {
            return Err(invalid_data("metadata snapshot role must be current or next").into());
        }
    };
    let mut row = polymarket_v3_base_row(
        &identity,
        PolymarketMetadataRowKind::MetadataSnapshot,
        PolymarketMetadataOrigin::GammaMarketsDiscovery,
        received,
        lifecycle,
        role,
        collector_identity,
    )?;
    row.source_endpoint = Some(GAMMA_MARKETS_ENDPOINT.to_owned());
    row.source_endpoint_unavailable_reason = None;
    row.source_evidence_origin = Some(SourceEvidenceOrigin::GammaMarketsDiscovery);
    row.source_evidence_event_type = Some(SourceEvidenceEventType::GammaMarketsDiscoveryResponse);
    row.source_evidence_received_at_utc = Some(received);
    row.row_evidence_validation_completed_at_utc =
        Some(market.metadata_validation_completed_at_utc_ms);
    row.source_evidence_payload_json = Some(payload.to_owned());
    row.source_evidence_raw_sha256 = Some(digest);
    row.metadata_fetch_start_utc = market.metadata_fetch_start_utc_ms;
    row.closed = sdk_market.closed;
    row.closed_time_raw = sdk_market.closed_time.clone();
    row.automatically_resolved = sdk_market.automatically_resolved;
    row.uma_resolution_status = sdk_market.uma_resolution_status.clone();
    row.outcomes_raw = sdk_market
        .outcomes
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    row.outcome_prices_raw = sdk_market
        .outcome_prices
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    row.active = sdk_market.active;
    row.active_unavailable_reason = sdk_market
        .active
        .is_none()
        .then_some(ActiveUnavailableReason::SourceFieldAbsent);
    row.accepting_orders = sdk_market.accepting_orders;
    row.response_fields_unavailable_reason = None;
    Ok(row)
}

pub(super) fn polymarket_market_state_transition_v3_row(
    identity: &PendingPolymarketIdentity,
    transition: MarketRuntimeRoleTransition,
    applied_at_utc_ms: i64,
    collector_identity: &CollectorIdentity,
) -> Result<PolymarketMarketMetadataV3Row> {
    let (lifecycle_transition, scheduled_at, lifecycle, role, horizon, status) = match transition {
        MarketRuntimeRoleTransition::NextToCurrent => (
            Some(MarketLifecycleTransition::PrefetchedToActive),
            identity.market_start_utc_ms,
            MarketLifecycleState::Active,
            MarketRuntimeRole::Current,
            None,
            None,
        ),
        MarketRuntimeRoleTransition::CurrentToResolutionPending => (
            None,
            identity.market_end_utc_ms,
            MarketLifecycleState::Active,
            MarketRuntimeRole::ResolutionPending,
            Some(identity.market_end_utc_ms.saturating_add(8 * 60_000)),
            Some(ResolutionFollowupStatus::Pending),
        ),
        _ => {
            return Err(invalid_data(format!(
                "unsupported timer-owned Polymarket state transition:{}",
                transition.as_str()
            ))
            .into());
        }
    };
    let mut row = polymarket_v3_base_row(
        identity,
        PolymarketMetadataRowKind::MarketStateTransition,
        PolymarketMetadataOrigin::LocalMarketStateTimer,
        applied_at_utc_ms,
        lifecycle,
        role,
        collector_identity,
    )?;
    row.source_endpoint = Some(GAMMA_MARKETS_ENDPOINT.to_owned());
    row.source_endpoint_unavailable_reason = None;
    row.source_evidence_origin = Some(SourceEvidenceOrigin::GammaMarketsDiscovery);
    row.source_evidence_event_type = Some(SourceEvidenceEventType::GammaMarketsDiscoveryResponse);
    row.source_evidence_received_at_utc = Some(identity.discovery_source_received_at_utc_ms);
    row.source_evidence_raw_sha256 = Some(identity.discovery_source_raw_sha256.clone());
    row.market_lifecycle_transition = lifecycle_transition;
    row.lifecycle_scheduled_at_utc = lifecycle_transition.map(|_| scheduled_at);
    row.lifecycle_applied_at_utc = lifecycle_transition.map(|_| applied_at_utc_ms);
    row.market_runtime_role_transition = Some(transition);
    row.runtime_role_scheduled_at_utc = Some(scheduled_at);
    row.runtime_role_applied_at_utc = Some(applied_at_utc_ms);
    row.resolution_budget_horizon_utc = horizon;
    row.resolution_followup_status = status;
    Ok(row)
}

pub(super) fn polymarket_candidate_to_active_v3_row(
    market: &DiscoveredBtcUpdownMarket,
    applied_at_utc_ms: i64,
    collector_identity: &CollectorIdentity,
) -> Result<PolymarketMarketMetadataV3Row> {
    let mut row = polymarket_metadata_snapshot_v3_row(
        market,
        MarketRuntimeRole::Current,
        collector_identity,
    )?;
    if applied_at_utc_ms < row.market_start_utc || applied_at_utc_ms >= row.market_end_utc {
        return Err(invalid_data(format!(
            "late Polymarket identity applied outside active window:slug={}:applied={applied_at_utc_ms}",
            row.slug
        ))
        .into());
    }
    row.metadata_row_kind = PolymarketMetadataRowKind::MarketStateTransition;
    row.metadata_origin = PolymarketMetadataOrigin::LocalMarketStateTimer;
    row.timestamp_received = applied_at_utc_ms;
    row.market_lifecycle_transition = Some(MarketLifecycleTransition::CandidateToActive);
    row.lifecycle_scheduled_at_utc = Some(row.market_start_utc);
    row.lifecycle_applied_at_utc = Some(applied_at_utc_ms);
    row.market_runtime_role_transition = Some(MarketRuntimeRoleTransition::NoneToCurrent);
    row.runtime_role_scheduled_at_utc = Some(row.market_start_utc);
    row.runtime_role_applied_at_utc = Some(applied_at_utc_ms);
    Ok(row)
}

fn gamma_by_slug_endpoint(slug: &str) -> String {
    GAMMA_MARKET_BY_SLUG_ENDPOINT.replace("{slug}", slug)
}

pub(super) fn polymarket_resolution_attempt_v3_row<N>(
    pending: &PendingPolymarketResolution,
    completion: &GammaResolutionPollCompletion,
    validation_now_utc_ms: &N,
    ws_claimed: bool,
    collector_identity: &CollectorIdentity,
) -> Result<PolymarketMarketMetadataV3Row>
where
    N: Fn() -> i64 + Send + Sync,
{
    let mut row = polymarket_v3_base_row(
        &pending.identity,
        PolymarketMetadataRowKind::ResolutionPollAttempt,
        PolymarketMetadataOrigin::GammaBySlug,
        completion.completed_at_utc_ms,
        MarketLifecycleState::Active,
        MarketRuntimeRole::ResolutionPending,
        collector_identity,
    )?;
    row.source_endpoint = Some(gamma_by_slug_endpoint(&pending.identity.slug));
    row.source_endpoint_unavailable_reason = None;
    row.metadata_fetch_start_utc = Some(completion.fetch_start_utc_ms);
    row.resolution_poll_attempt_ordinal = Some(completion.ordinal);
    row.resolution_poll_scheduled_utc = Some(pending.slot_utc_ms(completion.ordinal));
    row.resolution_budget_horizon_utc = Some(pending.budget_horizon_utc_ms());
    row.resolution_followup_status = Some(ResolutionFollowupStatus::Pending);

    match &completion.result {
        Err(GammaResolutionPollFailure::Transport { class, http_status }) => {
            row.resolution_poll_result = Some(ResolutionPollResult::TransportUnavailable);
            row.resolution_transport_error_class = Some(*class);
            row.resolution_transport_http_status = *http_status;
            row.active_unavailable_reason = Some(ActiveUnavailableReason::SourceFieldAbsent);
            row.response_fields_unavailable_reason =
                Some(ResponseFieldsUnavailableReason::TransportUnavailable);
        }
        Err(GammaResolutionPollFailure::SdkDecodeOrSchema) => {
            row.resolution_poll_result = Some(ResolutionPollResult::IntegrityError);
            row.resolution_integrity_error_class =
                Some(ResolutionIntegrityErrorClass::SdkDecodeOrSchema);
            row.active_unavailable_reason = Some(ActiveUnavailableReason::SourceFieldAbsent);
            row.response_fields_unavailable_reason =
                Some(ResponseFieldsUnavailableReason::SdkDecodeOrSchemaError);
        }
        Ok(market) => {
            let payload = serde_json::to_string(market)?;
            let payload_sha256 = Sha256::digest(payload.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let evaluation = evaluate_gamma_terminal_response(
                &pending.identity.gamma_terminal_identity(),
                GammaTerminalAttemptContext {
                    scheduled_at_utc_ms: pending.slot_utc_ms(completion.ordinal),
                    fetch_start_utc_ms: completion.fetch_start_utc_ms,
                    received_at_utc_ms: completion.completed_at_utc_ms,
                    response_bound_to_attempt: pending.in_flight_ordinal
                        == Some(completion.ordinal)
                        && pending.slot_utc_ms(completion.ordinal)
                            == completion.scheduled_at_utc_ms,
                },
                GammaTerminalResponse::Decoded(market),
            );
            let validation_completed_at_utc_ms = validation_now_utc_ms();
            if validation_completed_at_utc_ms < completion.completed_at_utc_ms {
                return Err(invalid_data(format!(
                    "Gamma resolution validation clock regressed:slug={}:ordinal={}:received={}:validation_completed={validation_completed_at_utc_ms}",
                    completion.slug, completion.ordinal, completion.completed_at_utc_ms
                ))
                .into());
            }
            let integrity_clean = matches!(
                evaluation.resolution_poll_result,
                ResolutionPollResult::Terminal | ResolutionPollResult::ValidNonterminal
            );
            let result = if ws_claimed && integrity_clean {
                ResolutionPollResult::PostTerminalClaimObservation
            } else {
                evaluation.resolution_poll_result
            };

            row.source_evidence_origin = Some(SourceEvidenceOrigin::GammaBySlug);
            row.source_evidence_event_type =
                Some(SourceEvidenceEventType::GammaMarketBySlugResponse);
            row.source_evidence_received_at_utc = Some(completion.completed_at_utc_ms);
            row.row_evidence_validation_completed_at_utc = Some(validation_completed_at_utc_ms);
            row.source_evidence_payload_json = Some(payload);
            row.source_evidence_raw_sha256 = Some(payload_sha256.clone());
            row.closed = market.closed;
            row.closed_time_raw = evaluation.closed_time_raw.clone();
            row.closed_time_utc = evaluation.closed_time_utc_ms;
            match evaluation.terminal_predicate_failure_reason {
                Some(TerminalPredicateFailureReason::ClosedTimeMissing) => {
                    row.closed_time_check_passed = Some(false);
                    row.closed_time_check_failure_reason =
                        Some(ClosedTimeCheckFailureReason::Missing);
                }
                Some(TerminalPredicateFailureReason::ClosedTimeParseError) => {
                    row.closed_time_check_passed = Some(false);
                    row.closed_time_check_failure_reason =
                        Some(ClosedTimeCheckFailureReason::ParseError);
                }
                Some(TerminalPredicateFailureReason::ClosedTimeBeforeMarketEnd) => {
                    row.closed_time_check_passed = Some(false);
                    row.closed_time_check_failure_reason =
                        Some(ClosedTimeCheckFailureReason::BeforeMarketEnd);
                }
                _ if evaluation.closed_time_utc_ms.is_some() => {
                    row.closed_time_check_passed = Some(true);
                }
                _ => {}
            }
            row.automatically_resolved = market.automatically_resolved;
            row.uma_resolution_status = market.uma_resolution_status.clone();
            row.outcomes_raw = market
                .outcomes
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            row.outcome_prices_raw = market
                .outcome_prices
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            row.active = evaluation.observed_active;
            row.active_unavailable_reason = evaluation
                .observed_active
                .is_none()
                .then_some(ActiveUnavailableReason::SourceFieldAbsent);
            row.accepting_orders = market.accepting_orders;
            row.resolution_poll_result = Some(result);
            row.resolution_integrity_error_class = evaluation.resolution_integrity_error_class;
            row.terminal_predicate_passed = evaluation.terminal_predicate_passed;
            row.terminal_predicate_failure_reason = evaluation.terminal_predicate_failure_reason;
            row.response_fields_unavailable_reason = None;

            if result == ResolutionPollResult::Terminal {
                let winner = evaluation.winner.ok_or_else(|| {
                    invalid_data("Gamma terminal predicate passed without a derived winner")
                })?;
                row.winning_asset_id = Some(winner.winning_asset_id);
                row.winning_outcome = Some(winner.winning_outcome);
                row.resolution_evidence_source = Some(ResolutionEvidenceSource::GammaBySlug);
                row.resolution_evidence_raw_sha256 = Some(payload_sha256);
                row.terminal_claim_owner = Some(TerminalClaimOwner::GammaBySlug);
                row.resolution_followup_status =
                    Some(ResolutionFollowupStatus::TerminalGammaFallback);
            } else if result == ResolutionPollResult::PostTerminalClaimObservation {
                row.terminal_claim_owner = Some(TerminalClaimOwner::MarketResolvedWs);
            }
        }
    }
    Ok(row)
}

pub(super) fn polymarket_resolution_missed_v3_row(
    pending: &PendingPolymarketResolution,
    ordinal: u8,
    observed_at_utc_ms: i64,
    collector_identity: &CollectorIdentity,
) -> Result<PolymarketMarketMetadataV3Row> {
    if !(1..=POLYMARKET_RESOLUTION_ATTEMPT_COUNT).contains(&ordinal) {
        return Err(invalid_data(format!(
            "recovered Polymarket missed ordinal is out of range:slug={}:ordinal={ordinal}",
            pending.identity.slug
        ))
        .into());
    }
    let scheduled_at_utc_ms = pending.slot_utc_ms(ordinal);
    if observed_at_utc_ms < scheduled_at_utc_ms {
        return Err(invalid_data(format!(
            "recovered Polymarket missed slot is still in the future:slug={}:ordinal={ordinal}:scheduled={scheduled_at_utc_ms}:observed={observed_at_utc_ms}",
            pending.identity.slug
        ))
        .into());
    }
    let mut row = polymarket_v3_base_row(
        &pending.identity,
        PolymarketMetadataRowKind::ResolutionPollAttempt,
        PolymarketMetadataOrigin::LocalFollowupScheduler,
        observed_at_utc_ms,
        MarketLifecycleState::Active,
        MarketRuntimeRole::ResolutionPending,
        collector_identity,
    )?;
    row.resolution_poll_attempt_ordinal = Some(ordinal);
    row.resolution_poll_scheduled_utc = Some(scheduled_at_utc_ms);
    row.resolution_poll_result = Some(ResolutionPollResult::MissedDueToDowntime);
    row.resolution_budget_horizon_utc = Some(pending.budget_horizon_utc_ms());
    row.resolution_followup_status = Some(ResolutionFollowupStatus::Pending);
    row.response_fields_unavailable_reason =
        Some(ResponseFieldsUnavailableReason::MissedDueToDowntime);
    Ok(row)
}

pub(super) fn complete_polymarket_terminal_transition(
    row: &mut PolymarketMarketMetadataV3Row,
    applied_at_utc_ms: i64,
) {
    row.market_lifecycle_state = MarketLifecycleState::Retired;
    row.market_runtime_role = MarketRuntimeRole::None;
    row.market_lifecycle_transition = Some(MarketLifecycleTransition::ActiveToRetired);
    row.lifecycle_applied_at_utc = Some(applied_at_utc_ms);
    row.market_runtime_role_transition = Some(MarketRuntimeRoleTransition::ResolutionPendingToNone);
    row.runtime_role_applied_at_utc = Some(applied_at_utc_ms);
}

pub(super) fn polymarket_resolution_terminal_ws_v3_row(
    pending: &PendingPolymarketResolution,
    evidence: &PolymarketWsTerminalEvidence,
    applied_at_utc_ms: i64,
    collector_identity: &CollectorIdentity,
) -> Result<PolymarketMarketMetadataV3Row> {
    let mut row = polymarket_v3_base_row(
        &pending.identity,
        PolymarketMetadataRowKind::ResolutionTerminalWs,
        PolymarketMetadataOrigin::MarketResolvedWs,
        evidence.source_received_at_utc_ms,
        MarketLifecycleState::Retired,
        MarketRuntimeRole::None,
        collector_identity,
    )?;
    row.source_endpoint = Some(MARKET_WS_ENDPOINT.to_owned());
    row.source_endpoint_unavailable_reason = None;
    row.source_evidence_origin = Some(SourceEvidenceOrigin::MarketResolvedWs);
    row.source_evidence_event_type = Some(SourceEvidenceEventType::MarketResolved);
    row.source_evidence_channel = Some(SourceEvidenceChannel::Market);
    row.source_evidence_received_at_utc = Some(evidence.source_received_at_utc_ms);
    row.row_evidence_validation_completed_at_utc = Some(evidence.validation_completed_at_utc_ms);
    row.source_evidence_payload_json = Some(evidence.source_payload_json.clone());
    row.source_evidence_raw_sha256 = Some(evidence.source_payload_sha256.clone());
    row.ws_platform_timestamp_raw = Some(evidence.platform_timestamp_raw.clone());
    row.ws_platform_timestamp_utc = Some(evidence.platform_timestamp_utc_ms);
    row.winning_asset_id = Some(evidence.winning_asset_id.clone());
    row.winning_outcome = Some(evidence.winning_outcome.clone());
    row.resolution_evidence_source = Some(ResolutionEvidenceSource::MarketResolvedWs);
    row.resolution_evidence_raw_sha256 = Some(evidence.source_payload_sha256.clone());
    row.terminal_claim_owner = Some(TerminalClaimOwner::MarketResolvedWs);
    row.resolution_budget_horizon_utc = Some(pending.budget_horizon_utc_ms());
    row.resolution_followup_status = Some(ResolutionFollowupStatus::TerminalWs);
    row.terminal_predicate_passed = Some(true);
    row.response_fields_unavailable_reason = None;
    complete_polymarket_terminal_transition(&mut row, applied_at_utc_ms);
    Ok(row)
}

pub(super) fn polymarket_resolution_followup_summary_v3_row(
    pending: &PendingPolymarketResolution,
    applied_at_utc_ms: i64,
    collector_identity: &CollectorIdentity,
) -> Result<PolymarketMarketMetadataV3Row> {
    let mut row = polymarket_v3_base_row(
        &pending.identity,
        PolymarketMetadataRowKind::ResolutionFollowupSummary,
        PolymarketMetadataOrigin::LocalFollowupScheduler,
        applied_at_utc_ms,
        MarketLifecycleState::Active,
        MarketRuntimeRole::None,
        collector_identity,
    )?;
    row.market_runtime_role_transition = Some(MarketRuntimeRoleTransition::ResolutionPendingToNone);
    row.runtime_role_applied_at_utc = Some(applied_at_utc_ms);
    row.resolution_budget_horizon_utc = Some(pending.budget_horizon_utc_ms());
    row.resolution_followup_status = Some(if pending.valid_response_count > 0 {
        ResolutionFollowupStatus::BudgetExhaustedAfterValidNonterminal
    } else {
        ResolutionFollowupStatus::BudgetExhaustedWithoutValidResponse
    });
    row.resolution_poll_attempts_recorded = Some(pending.attempts_recorded);
    row.resolution_valid_response_count = Some(pending.valid_response_count);
    row.resolution_transport_unavailable_count = Some(pending.transport_unavailable_count);
    row.resolution_missed_slot_count = Some(pending.missed_slot_count);
    row.last_valid_metadata_received_utc = pending.last_valid_metadata_received_at_utc_ms;
    Ok(row)
}

pub(super) fn polymarket_metadata_v4_raw_event(
    row: PolymarketMarketMetadataV4Row,
) -> Result<RawEvent> {
    let event_type = row.v3_prefix.metadata_row_kind.as_str();
    let endpoint = row.v3_prefix.source_endpoint.as_deref().unwrap_or_default();
    Ok(raw_event_with_recv_time(
        "polymarket_market_metadata",
        event_type,
        &row.v3_prefix.slug,
        None,
        row.v3_prefix.timestamp_received,
        endpoint,
        &CollectorIdentity {
            host_id: row.v3_prefix.collector_host_id.clone(),
            region: row.v3_prefix.collector_region.clone(),
        },
        serde_json::to_value(&row)?,
    ))
}

pub(super) fn polymarket_metadata_v4_rows_from_event(
    event: &RawEvent,
) -> Result<Vec<PolymarketMarketMetadataV4Row>> {
    let admission = event.collector_admission.as_ref().ok_or_else(|| {
        invalid_data("Polymarket metadata v4 event is missing collector admission")
    })?;
    let mut row = serde_json::from_str::<PolymarketMarketMetadataV4Row>(&event.raw_json)?;
    row.v3_prefix.collector_admission_sequence = admission.sequence;
    row.v3_prefix.collector_admission_time_utc_ms = admission.time_utc_ms;
    Ok(vec![row])
}

pub(super) fn write_polymarket_metadata_v4_live_event(
    writer: &mut TypedParquetSegmentWriter,
    event: &RawEvent,
    logical_bytes: u64,
) -> Result<u64> {
    let rows = polymarket_metadata_v4_rows_from_event(event)?;
    let written = rows.len() as u64;
    require_at_most_one_typed_row(event, written)?;
    writer.write_polymarket_market_metadata_v4_rows_with_logical_bytes(&rows, logical_bytes)?;
    Ok(written)
}

fn require_at_most_one_typed_row(event: &RawEvent, rows: u64) -> Result<()> {
    if rows <= 1 {
        return Ok(());
    }
    Err(invalid_data(format!(
        "typed event fan-out must be split before ingress: source={} event_type={} rows={rows}",
        event.source_kind, event.source_event_type
    ))
    .into())
}
