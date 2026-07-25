use super::*;

pub(super) const POLYMARKET_ORDERBOOK_SCHEMA_VERSION: &str = "polymarket_orderbook_v2";

#[derive(Default)]
pub(super) struct PolymarketDispatchOutcome {
    pub(super) matched_assets: BTreeSet<String>,
}

pub(super) fn polymarket_target_fanout_counts(
    message: &WsMessage,
    asset_to_slug: &BTreeMap<String, String>,
) -> (usize, usize) {
    let target = |asset: &U256| asset_to_slug.contains_key(&asset.to_string());
    match message {
        WsMessage::BestBidAsk(event) => (usize::from(target(&event.asset_id)), 0),
        WsMessage::Book(event) => (usize::from(target(&event.asset_id)), 0),
        WsMessage::PriceChange(event) => (
            event
                .price_changes
                .iter()
                .filter(|change| target(&change.asset_id))
                .count(),
            0,
        ),
        WsMessage::LastTradePrice(event) => (usize::from(target(&event.asset_id)), 0),
        WsMessage::TickSizeChange(event) => (usize::from(target(&event.asset_id)), 0),
        WsMessage::NewMarket(event) => {
            let matches = event.asset_ids.iter().any(target);
            (usize::from(matches), 0)
        }
        WsMessage::MarketResolved(event) => {
            let matches = event.asset_ids.iter().any(target);
            (usize::from(matches), usize::from(matches))
        }
        _ => (0, 0),
    }
}

fn polymarket_event_message_json(
    message: &polymarket_client_sdk_v2::clob::ws::EventMessage,
) -> Value {
    json!({
        "id": message.id,
        "ticker": message.ticker,
        "slug": message.slug,
        "title": message.title,
        "description": message.description,
    })
}

pub(super) fn polymarket_source_native_payload_json(message: &WsMessage) -> Result<Option<String>> {
    let value = match message {
        WsMessage::Book(event) => {
            let mut value = json!({
                "source": "polymarket_market",
                "event_type": "book",
                "market": event.market.to_string(),
                "asset_id": event.asset_id.to_string(),
                "timestamp": event.timestamp,
                "bids": event.bids.iter().map(|level| {
                    json!([level.price.to_string(), level.size.to_string()])
                }).collect::<Vec<_>>(),
                "asks": event.asks.iter().map(|level| {
                    json!([level.price.to_string(), level.size.to_string()])
                }).collect::<Vec<_>>(),
                "hash": event.hash,
            });
            if let Some(last_trade_price) = event.last_trade_price {
                value["last_trade_price"] = Value::String(last_trade_price.to_string());
            }
            value
        }
        WsMessage::PriceChange(event) => json!({
            "source": "polymarket_market",
            "event_type": "price_change",
            "market": event.market.to_string(),
            "timestamp": event.timestamp,
            "price_changes": event.price_changes.iter().map(|change| json!({
                "asset_id": change.asset_id.to_string(),
                "price": change.price.to_string(),
                "size": change.size.as_ref().map(ToString::to_string),
                "side": change.side.to_string(),
                "hash": change.hash,
                "best_bid": change.best_bid.as_ref().map(ToString::to_string),
                "best_ask": change.best_ask.as_ref().map(ToString::to_string),
            })).collect::<Vec<_>>(),
        }),
        WsMessage::TickSizeChange(event) => json!({
            "source": "polymarket_market",
            "event_type": "tick_size_change",
            "market": event.market.to_string(),
            "asset_id": event.asset_id.to_string(),
            "old_tick_size": event.old_tick_size.as_ref().map(ToString::to_string),
            "new_tick_size": event.new_tick_size.to_string(),
            "timestamp": event.timestamp,
        }),
        WsMessage::LastTradePrice(event) => json!({
            "source": "polymarket_market",
            "event_type": "last_trade_price",
            "market": event.market.to_string(),
            "asset_id": event.asset_id.to_string(),
            "price": event.price.to_string(),
            "side": event.side.as_ref().map(ToString::to_string),
            "size": event.size.as_ref().map(ToString::to_string),
            "fee_rate_bps": event.fee_rate_bps.as_ref().map(ToString::to_string),
            "timestamp": event.timestamp,
        }),
        WsMessage::BestBidAsk(event) => json!({
            "source": "polymarket_market",
            "event_type": "best_bid_ask",
            "market": event.market.to_string(),
            "asset_id": event.asset_id.to_string(),
            "best_bid": event.best_bid.as_ref().map(ToString::to_string),
            "best_ask": event.best_ask.as_ref().map(ToString::to_string),
            "spread": event.spread.as_ref().map(ToString::to_string),
            "timestamp": event.timestamp,
        }),
        WsMessage::NewMarket(event) => json!({
            "source": "polymarket_market",
            "event_type": "new_market",
            "id": event.id,
            "question": event.question,
            "market": event.market.to_string(),
            "slug": event.slug,
            "description": event.description,
            "asset_ids": event.asset_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "outcomes": event.outcomes,
            "event_message": event.event_message.as_ref().map(polymarket_event_message_json),
            "timestamp": event.timestamp,
        }),
        WsMessage::MarketResolved(event) => json!({
            "source": "polymarket_market",
            "event_type": "market_resolved",
            "id": event.id,
            "question": event.question,
            "market": event.market.to_string(),
            "slug": event.slug,
            "description": event.description,
            "asset_ids": event.asset_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "outcomes": event.outcomes,
            "winning_asset_id": event.winning_asset_id.as_ref().map(ToString::to_string),
            "winning_outcome": event.winning_outcome,
            "event_message": event.event_message.as_ref().map(polymarket_event_message_json),
            "timestamp": event.timestamp,
        }),
        WsMessage::Trade(_) | WsMessage::Order(_) => return Ok(None),
        _ => return Ok(None),
    };
    Ok(Some(serde_json::to_string(&value)?))
}

fn require_current_v2_polymarket_shape(message: &WsMessage) -> Result<()> {
    let (event_type, missing_fields) = match message {
        WsMessage::Book(event) => (
            "book",
            [("timestamp", event.timestamp.is_none())]
                .into_iter()
                .filter_map(|(field, missing)| missing.then_some(field))
                .collect::<Vec<_>>(),
        ),
        WsMessage::PriceChange(event) => (
            "price_change",
            [("timestamp", event.timestamp.is_none())]
                .into_iter()
                .filter_map(|(field, missing)| missing.then_some(field))
                .collect::<Vec<_>>(),
        ),
        WsMessage::LastTradePrice(event) => (
            "last_trade_price",
            [("timestamp", event.timestamp.is_none())]
                .into_iter()
                .filter_map(|(field, missing)| missing.then_some(field))
                .collect::<Vec<_>>(),
        ),
        WsMessage::TickSizeChange(event) => (
            "tick_size_change",
            [
                ("old_tick_size", event.old_tick_size.is_none()),
                ("timestamp", event.timestamp.is_none()),
            ]
            .into_iter()
            .filter_map(|(field, missing)| missing.then_some(field))
            .collect::<Vec<_>>(),
        ),
        WsMessage::BestBidAsk(event) => (
            "best_bid_ask",
            [
                ("best_bid", event.best_bid.is_none()),
                ("best_ask", event.best_ask.is_none()),
                ("spread", event.spread.is_none()),
                ("timestamp", event.timestamp.is_none()),
            ]
            .into_iter()
            .filter_map(|(field, missing)| missing.then_some(field))
            .collect::<Vec<_>>(),
        ),
        WsMessage::NewMarket(event) => {
            let mut missing_fields = [
                ("question", event.question.is_none()),
                ("slug", event.slug.is_none()),
                ("description", event.description.is_none()),
                ("asset_ids", event.asset_ids.is_empty()),
                ("outcomes", event.outcomes.is_empty()),
                ("timestamp", event.timestamp.is_none()),
            ]
            .into_iter()
            .filter_map(|(field, missing)| missing.then_some(field))
            .collect::<Vec<_>>();
            if let Some(event_message) = &event.event_message {
                missing_fields.extend(
                    [
                        ("event_message.ticker", event_message.ticker.is_none()),
                        ("event_message.slug", event_message.slug.is_none()),
                        ("event_message.title", event_message.title.is_none()),
                        (
                            "event_message.description",
                            event_message.description.is_none(),
                        ),
                    ]
                    .into_iter()
                    .filter_map(|(field, missing)| missing.then_some(field)),
                );
            }
            ("new_market", missing_fields)
        }
        WsMessage::MarketResolved(event) => {
            let mut missing_fields = [
                ("asset_ids", event.asset_ids.is_empty()),
                ("winning_asset_id", event.winning_asset_id.is_none()),
                ("winning_outcome", event.winning_outcome.is_none()),
                ("timestamp", event.timestamp.is_none()),
            ]
            .into_iter()
            .filter_map(|(field, missing)| missing.then_some(field))
            .collect::<Vec<_>>();
            if let Some(event_message) = &event.event_message {
                missing_fields.extend(
                    [
                        ("event_message.ticker", event_message.ticker.is_none()),
                        ("event_message.slug", event_message.slug.is_none()),
                        ("event_message.title", event_message.title.is_none()),
                        (
                            "event_message.description",
                            event_message.description.is_none(),
                        ),
                    ]
                    .into_iter()
                    .filter_map(|(field, missing)| missing.then_some(field)),
                );
            }
            ("market_resolved", missing_fields)
        }
        _ => return Ok(()),
    };
    if missing_fields.is_empty() {
        return Ok(());
    }
    Err(invalid_data(format!(
        "polymarket_current_v2_typed_shape_missing_required_fields:event_type={event_type}:fields={}",
        missing_fields.join(",")
    ))
    .into())
}

pub(super) fn prepare_polymarket_message(message: &WsMessage) -> Result<Option<String>> {
    require_current_v2_polymarket_shape(message)?;
    let Some(source_native_payload_json) = polymarket_source_native_payload_json(message)? else {
        return Ok(None);
    };
    if source_native_payload_json.len() > MAX_TYPED_ROW_BYTES {
        return Err(io::Error::other(format!(
            "operator_intervention_required:Polymarket source-native payload exceeds {MAX_TYPED_ROW_BYTES} bytes"
        ))
        .into());
    }
    Ok(Some(source_native_payload_json))
}

pub(super) fn visit_polymarket_events<F>(
    message: &WsMessage,
    discovery: &BtcUpdownDiscovery,
    asset_to_slug: &BTreeMap<String, String>,
    collector_identity: &CollectorIdentity,
    recv_time_utc_ms: i64,
    source_native_payload_json: &str,
    mut emit: F,
) -> Result<PolymarketDispatchOutcome>
where
    F: FnMut(RawEvent) -> Result<()>,
{
    let mut outcome = PolymarketDispatchOutcome::default();
    match message {
        WsMessage::BestBidAsk(event) => {
            let asset_id = event.asset_id.to_string();
            let Some(slug) = verify_polymarket_business_asset(
                discovery,
                asset_to_slug,
                &event.market.to_string(),
                &asset_id,
            )?
            else {
                return Ok(outcome);
            };
            outcome.matched_assets.insert(asset_id.clone());
            emit(raw_event_with_recv_time(
                "polymarket_market",
                "best_bid_ask",
                &format!("{slug}:{}", event.asset_id),
                event.timestamp.map(|timestamp| timestamp.to_string()),
                recv_time_utc_ms,
                MARKET_WS_ENDPOINT,
                collector_identity,
                with_source_native_payload_json(
                    json!({
                        "event_type": "best_bid_ask",
                        "market": event.market.to_string(),
                        "asset_id": asset_id,
                        "slug": slug,
                        "best_bid": event.best_bid.as_ref().map(ToString::to_string),
                        "best_ask": event.best_ask.as_ref().map(ToString::to_string),
                        "spread": event.spread.as_ref().map(ToString::to_string),
                        "timestamp": event.timestamp
                    }),
                    source_native_payload_json,
                ),
            ))?;
        }
        WsMessage::Book(event) => {
            let asset_id = event.asset_id.to_string();
            let Some(slug) = verify_polymarket_business_asset(
                discovery,
                asset_to_slug,
                &event.market.to_string(),
                &asset_id,
            )?
            else {
                return Ok(outcome);
            };
            outcome.matched_assets.insert(asset_id.clone());
            emit(raw_event_with_recv_time(
                "polymarket_market",
                "book",
                &format!("{slug}:{}", event.asset_id),
                event.timestamp.map(|timestamp| timestamp.to_string()),
                recv_time_utc_ms,
                MARKET_WS_ENDPOINT,
                collector_identity,
                with_source_native_payload_json(
                    polymarket_book_raw_json(event, slug),
                    source_native_payload_json,
                ),
            ))?;
        }
        WsMessage::PriceChange(event) => {
            let condition_id = event.market.to_string();
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
                outcome.matched_assets.insert(asset_id.clone());
                let mut row_event = event.clone();
                row_event.price_changes = vec![change.clone()];
                emit(raw_event_with_recv_time(
                    "polymarket_market",
                    "price_change",
                    &format!("{slug}:{asset_id}"),
                    row_event.timestamp.map(|timestamp| timestamp.to_string()),
                    recv_time_utc_ms,
                    MARKET_WS_ENDPOINT,
                    collector_identity,
                    with_source_native_payload_json(
                        polymarket_price_change_raw_json(&row_event, asset_to_slug),
                        source_native_payload_json,
                    ),
                ))?;
            }
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
                return Ok(outcome);
            };
            outcome.matched_assets.insert(asset_id.clone());
            emit(raw_event_with_recv_time(
                "polymarket_market",
                "last_trade_price",
                &format!("{slug}:{asset_id}"),
                event.timestamp.map(|timestamp| timestamp.to_string()),
                recv_time_utc_ms,
                MARKET_WS_ENDPOINT,
                collector_identity,
                with_source_native_payload_json(
                    polymarket_last_trade_raw_json(event, slug),
                    source_native_payload_json,
                ),
            ))?;
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
                return Ok(outcome);
            };
            outcome.matched_assets.insert(asset_id.clone());
            emit(raw_event_with_recv_time(
                "polymarket_market",
                "tick_size_change",
                &format!("{slug}:{asset_id}"),
                event.timestamp.map(|timestamp| timestamp.to_string()),
                recv_time_utc_ms,
                MARKET_WS_ENDPOINT,
                collector_identity,
                with_source_native_payload_json(
                    polymarket_tick_size_change_raw_json(event, slug),
                    source_native_payload_json,
                ),
            ))?;
        }
        WsMessage::NewMarket(event) => {
            let slug = event.slug.as_deref().ok_or_else(|| {
                invalid_data(
                    "polymarket_current_v2_typed_shape_missing_required_fields:event_type=new_market:fields=slug",
                )
            })?;
            let Some((_verified_market, matched_assets)) = verify_target_market_identity(
                discovery,
                &event.id,
                &event.market.to_string(),
                Some(slug),
                &event.asset_ids,
                Some(&event.outcomes),
                None,
                None,
            )?
            else {
                return Ok(outcome);
            };
            outcome.matched_assets = matched_assets;
            let market_raw_json = with_source_native_payload_json(
                polymarket_new_market_raw_json(event),
                source_native_payload_json,
            );
            emit(raw_event_with_recv_time(
                "polymarket_market",
                "new_market",
                slug,
                event.timestamp.map(|timestamp| timestamp.to_string()),
                recv_time_utc_ms,
                MARKET_WS_ENDPOINT,
                collector_identity,
                market_raw_json,
            ))?;
        }
        WsMessage::MarketResolved(_) => {
            return Err(
                invalid_data("market_resolved must use the raw-first terminal handler").into(),
            );
        }
        _ => return Ok(outcome),
    }
    Ok(outcome)
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

#[cfg(test)]
pub(super) fn build_polymarket_events(
    message: WsMessage,
    discovery: &BtcUpdownDiscovery,
    asset_to_slug: &BTreeMap<String, String>,
    collector_identity: &CollectorIdentity,
    recv_time_utc_ms: i64,
    source_native_payload_json: &str,
) -> Result<(PolymarketDispatchOutcome, Vec<RawEvent>)> {
    let mut events = Vec::new();
    let outcome = visit_polymarket_events(
        &message,
        discovery,
        asset_to_slug,
        collector_identity,
        recv_time_utc_ms,
        source_native_payload_json,
        |event| {
            events.push(event);
            Ok(())
        },
    )?;
    Ok((outcome, events))
}

#[cfg(test)]
pub(super) fn retain_target_price_changes(
    event: &mut PriceChange,
    asset_to_slug: &BTreeMap<String, String>,
) -> BTreeSet<String> {
    event
        .price_changes
        .retain(|change| asset_to_slug.contains_key(&change.asset_id.to_string()));
    event
        .price_changes
        .iter()
        .map(|change| change.asset_id.to_string())
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn verify_target_market_identity<'a>(
    discovery: &'a BtcUpdownDiscovery,
    market_id: &str,
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
    let id_match = markets.clone().find(|market| market.id == market_id);
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
    if market_id != expected.id {
        return Err(invalid_data(format!(
            "Polymarket target identity carried unexpected market id {market_id}"
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
        if !winning_outcome
            .trim()
            .eq_ignore_ascii_case(expected_winning_outcome)
        {
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
        clob_market_id: identity.market_id.clone(),
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

pub(super) fn polymarket_metadata_v3_raw_event(
    row: PolymarketMarketMetadataV3Row,
) -> Result<RawEvent> {
    let event_type = row.metadata_row_kind.as_str();
    let endpoint = row.source_endpoint.as_deref().unwrap_or_default();
    Ok(raw_event_with_recv_time(
        "polymarket_market_metadata",
        event_type,
        &row.slug,
        None,
        row.timestamp_received,
        endpoint,
        &CollectorIdentity {
            host_id: row.collector_host_id.clone(),
            region: row.collector_region.clone(),
        },
        serde_json::from_str(&row.to_draft_json()?)?,
    ))
}

#[cfg(test)]
pub(super) fn polymarket_metadata_snapshot_raw_json(
    market: &crate::market_discovery::DiscoveredBtcUpdownMarket,
    discovery_slot: &str,
) -> Value {
    json!({
        "event_type": "active_scan",
        "metadata_source": "active_scan",
        "discovery_slot": discovery_slot,
        "slug": market.slug,
        "condition_id": market.condition_id,
        "clob_market_id": market.id,
        "asset_ids": [market.up_asset_id, market.down_asset_id],
        "metadata_fetch_start_utc": market.metadata_fetch_start_utc_ms,
        "market_start_utc": market.start_ts_s * 1000,
        "market_end_utc": market.end_ts_s * 1000,
        "active": market.active,
        "accepting_orders": market.accepting_orders,
        "market_lifecycle_state": if discovery_slot == "next" {
            "prefetched"
        } else if market.active {
            "active"
        } else {
            "candidate"
        },
        "tick_size": market.tick_size,
        "min_size": market.min_size,
        "fee_schedule_raw": market.fee_schedule_raw,
        "reward_eligibility_raw": market.reward_eligibility_raw,
        "metadata_unavailable_reason": missing_metadata_reason(market),
        "same_condition_market_verified": true,
        "raw_json_sha256": market.raw_json_sha256,
        "gamma_market_raw_json": market.raw_json,
    })
}

#[cfg(test)]
pub(super) fn polymarket_window_end_observation_raw_json_from_discovery(
    market: &crate::market_discovery::DiscoveredBtcUpdownMarket,
) -> Value {
    let mut value = polymarket_metadata_snapshot_raw_json(market, "current_window_end_unresolved");
    let reason = "market_resolved_not_observed_before_window_end";
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "market_lifecycle_state".to_owned(),
            json!("unresolved_window_end_observation"),
        );
        object.insert("window_end_observation_reason".to_owned(), json!(reason));
        let mut reasons = missing_metadata_reason(market)
            .into_iter()
            .collect::<Vec<_>>();
        reasons.push(reason.to_owned());
        object.insert("metadata_unavailable_reason".to_owned(), json!(reasons));
    }
    value
}

#[cfg(test)]
fn missing_metadata_reason(
    market: &crate::market_discovery::DiscoveredBtcUpdownMarket,
) -> Option<String> {
    let missing = [
        ("tick_size", market.tick_size.is_none()),
        ("min_size", market.min_size.is_none()),
        ("fee_schedule_raw", market.fee_schedule_raw.is_none()),
        (
            "reward_eligibility_raw",
            market.reward_eligibility_raw.is_none(),
        ),
    ]
    .into_iter()
    .filter_map(|(field, missing)| missing.then_some(field))
    .collect::<Vec<_>>();
    (!missing.is_empty()).then(|| format!("gamma_market_fields_unavailable:{}", missing.join(",")))
}

pub(super) fn with_source_native_payload_json(
    mut value: Value,
    source_native_payload_json: &str,
) -> Value {
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "source_native_payload_json".to_owned(),
            Value::String(source_native_payload_json.to_owned()),
        );
    }
    value
}

pub(super) fn with_polymarket_row_context(
    mut value: Value,
    slug: Option<&str>,
    source_native_payload_json: &str,
) -> Value {
    if let Some(object) = value.as_object_mut()
        && let Some(slug) = slug
    {
        object.insert("slug".to_owned(), Value::String(slug.to_owned()));
    }
    with_source_native_payload_json(value, source_native_payload_json)
}

pub(super) fn polymarket_book_raw_json(event: &BookUpdate, slug: &str) -> Value {
    let mut value = json!({
        "event_type": "book",
        "market": event.market.to_string(),
        "asset_id": event.asset_id.to_string(),
        "slug": slug,
        "timestamp": event.timestamp,
        "bids": event
            .bids
            .iter()
            .map(|level| json!([level.price.to_string(), level.size.to_string()]))
            .collect::<Vec<_>>(),
        "asks": event
            .asks
            .iter()
            .map(|level| json!([level.price.to_string(), level.size.to_string()]))
            .collect::<Vec<_>>(),
        "hash": event.hash,
    });
    if let Some(last_trade_price) = event.last_trade_price {
        value["last_trade_price"] = Value::String(last_trade_price.to_string());
    }
    value
}

pub(super) fn polymarket_price_change_raw_json(
    event: &PriceChange,
    asset_to_slug: &BTreeMap<String, String>,
) -> Value {
    json!({
        "event_type": "price_change",
        "market": event.market.to_string(),
        "timestamp": event.timestamp,
        "price_changes": event
            .price_changes
            .iter()
            .map(|change| {
                let asset_id = change.asset_id.to_string();
                json!({
                    "asset_id": asset_id,
                    "slug": asset_to_slug.get(&asset_id),
                    "price": change.price.to_string(),
                    "size": change.size.as_ref().map(ToString::to_string),
                    "side": change.side.to_string(),
                    "hash": change.hash.clone(),
                    "best_bid": change.best_bid.as_ref().map(ToString::to_string),
                    "best_ask": change.best_ask.as_ref().map(ToString::to_string),
                })
            })
            .collect::<Vec<_>>()
    })
}

pub(super) fn polymarket_last_trade_raw_json(event: &LastTradePrice, slug: &str) -> Value {
    json!({
        "event_type": "last_trade_price",
        "market": event.market.to_string(),
        "asset_id": event.asset_id.to_string(),
        "slug": slug,
        "price": event.price.to_string(),
        "side": event.side.as_ref().map(ToString::to_string),
        "size": event.size.as_ref().map(ToString::to_string),
        "fee_rate_bps": event.fee_rate_bps.as_ref().map(ToString::to_string),
        "timestamp": event.timestamp,
    })
}

pub(super) fn polymarket_tick_size_change_raw_json(event: &TickSizeChange, slug: &str) -> Value {
    json!({
        "event_type": "tick_size_change",
        "market": event.market.to_string(),
        "asset_id": event.asset_id.to_string(),
        "slug": slug,
        "old_tick_size": event.old_tick_size.as_ref().map(ToString::to_string),
        "new_tick_size": event.new_tick_size.to_string(),
        "timestamp": event.timestamp,
    })
}

pub(super) fn polymarket_new_market_raw_json(event: &NewMarket) -> Value {
    json!({
        "event_type": "new_market",
        "id": event.id,
        "question": event.question,
        "market": event.market.to_string(),
        "slug": event.slug,
        "description": event.description,
        "asset_ids": event
            .asset_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        "outcomes": event.outcomes,
        "metadata_unavailable_reason": "market_ws_new_market_payload_lacks_market_start_end_active_accepting_orders_tick_size_min_size_fee_schedule_reward_eligibility",
        "same_condition_market_verified": true,
        "event_message": event.event_message.as_ref().map(|message| {
            json!({
                "id": message.id,
                "ticker": message.ticker,
                "slug": message.slug,
                "title": message.title,
                "description": message.description,
            })
        }),
        "timestamp": event.timestamp,
    })
}

pub(super) fn polymarket_market_resolved_raw_json(event: &MarketResolved) -> Value {
    let asset_ids = event
        .asset_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    json!({
        "event_type": "market_resolved",
        "id": event.id,
        "question": event.question,
        "market": event.market.to_string(),
        "slug": event.slug,
        "description": event.description,
        "asset_ids": asset_ids,
        "resolved_event_asset_ids": asset_ids,
        "outcomes": event.outcomes,
        "winning_asset_id": event.winning_asset_id.as_ref().map(ToString::to_string),
        "winning_outcome": event.winning_outcome,
        "close_reason_if_present": Value::Null,
        "event_message": event.event_message.as_ref().map(|message| {
            json!({
                "id": message.id,
                "ticker": message.ticker,
                "slug": message.slug,
                "title": message.title,
                "description": message.description,
            })
        }),
        "timestamp": event.timestamp,
    })
}

pub(super) fn polymarket_metadata_v3_rows_from_event(
    event: &RawEvent,
) -> Result<Vec<PolymarketMarketMetadataV3Row>> {
    let admission = event.collector_admission.as_ref().ok_or_else(|| {
        invalid_data("Polymarket metadata v3 event is missing collector admission")
    })?;
    let row = PolymarketMarketMetadataV3Row::from_draft_json_with_admission(
        &event.raw_json,
        admission.sequence,
        admission.time_utc_ms,
    )?;
    Ok(vec![row])
}

#[cfg(test)]
pub(super) fn polymarket_metadata_rows_from_event(
    event: &RawEvent,
) -> Result<Vec<PolymarketMarketMetadataRow>> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let event_type = value
        .get("event_type")
        .and_then(Value::as_str)
        .unwrap_or(event.source_event_type.as_str());
    let row = match event_type {
        "active_scan" => metadata_row_from_value(event, &value, "active_scan")?,
        "new_market" => metadata_row_from_value(event, &value, "new_market")?,
        "fallback_close" => metadata_row_from_value(event, &value, "fallback_close")?,
        other => {
            return Err(invalid_data(format!(
                "unsupported polymarket metadata event type: {other}"
            ))
            .into());
        }
    };
    Ok(vec![row])
}

#[cfg(test)]
fn metadata_row_from_value(
    event: &RawEvent,
    value: &Value,
    metadata_source: &str,
) -> Result<PolymarketMarketMetadataRow> {
    let context = PolymarketMarketMetadataContext {
        timestamp_received_ms: event.recv_time_utc_ms,
        metadata_fetch_start_utc_ms: value
            .get("metadata_fetch_start_utc")
            .and_then(Value::as_i64),
        collector_host_id: event.collector_host_id.clone(),
        collector_region: event.collector_region.clone(),
        source_endpoint: event.source_endpoint.clone(),
        schema_version: "polymarket_market_metadata_v2".to_owned(),
    };
    let slug = required_value_string(value, "slug")?;
    let condition_id = value
        .get("condition_id")
        .and_then(Value::as_str)
        .or_else(|| value.get("market").and_then(Value::as_str))
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid_data("condition_id missing"))?;
    let clob_market_id = value
        .get("clob_market_id")
        .and_then(Value::as_str)
        .or_else(|| value.get("id").and_then(Value::as_str));
    let asset_ids = json_string_vec(value, "asset_ids")?;
    let market_start_utc = optional_i64(value, "market_start_utc");
    let market_end_utc = optional_i64(value, "market_end_utc");
    let active = optional_bool(value, "active");
    let accepting_orders = optional_bool(value, "accepting_orders");
    let market_lifecycle_state = value
        .get("market_lifecycle_state")
        .and_then(Value::as_str)
        .unwrap_or(match metadata_source {
            "new_market" => "prefetched",
            "fallback_close" => "retired",
            _ => "active",
        });
    let mut row = match metadata_source {
        "new_market" => PolymarketMarketMetadataRow::new_market(
            &slug,
            condition_id,
            clob_market_id,
            &asset_ids,
            market_start_utc,
            market_end_utc,
            active,
            accepting_orders,
            market_lifecycle_state,
            optional_json_string(value, "tick_size").as_deref(),
            optional_json_string(value, "min_size").as_deref(),
            optional_json_string(value, "fee_schedule_raw").as_deref(),
            optional_json_string(value, "reward_eligibility_raw").as_deref(),
            optional_bool(value, "same_condition_market_verified"),
            context,
        ),
        "fallback_close" => PolymarketMarketMetadataRow::fallback_close(
            &slug,
            condition_id,
            clob_market_id,
            &asset_ids,
            market_start_utc,
            market_end_utc,
            active,
            accepting_orders,
            market_lifecycle_state,
            optional_json_string(value, "tick_size").as_deref(),
            optional_json_string(value, "min_size").as_deref(),
            optional_json_string(value, "fee_schedule_raw").as_deref(),
            optional_json_string(value, "reward_eligibility_raw").as_deref(),
            optional_bool(value, "same_condition_market_verified"),
            context,
        ),
        _ => PolymarketMarketMetadataRow::active_scan(
            &slug,
            condition_id,
            clob_market_id,
            &asset_ids,
            market_start_utc,
            market_end_utc,
            active,
            accepting_orders,
            market_lifecycle_state,
            optional_json_string(value, "tick_size").as_deref(),
            optional_json_string(value, "min_size").as_deref(),
            optional_json_string(value, "fee_schedule_raw").as_deref(),
            optional_json_string(value, "reward_eligibility_raw").as_deref(),
            optional_bool(value, "same_condition_market_verified"),
            context,
        ),
    };
    row.metadata_unavailable_reason = metadata_unavailable_reason(value);
    if row.same_condition_market_verified != Some(true) {
        return Err(invalid_data(
            "polymarket metadata row is not verified against its subscribed condition",
        )
        .into());
    }
    let validation = row.validate();
    if !validation.is_complete() && row.metadata_unavailable_reason.is_none() {
        return Err(invalid_data(format!(
            "polymarket metadata row missing fields without unavailable reason: {:?}",
            validation.missing_fields
        ))
        .into());
    }
    Ok(row)
}

pub(super) fn polymarket_orderbook_rows_from_event(
    event: &RawEvent,
) -> Result<Vec<PolymarketOrderbookRow>> {
    let value = serde_json::from_str::<Value>(&event.raw_json)?;
    let event_type = value
        .get("event_type")
        .and_then(Value::as_str)
        .unwrap_or(event.source_event_type.as_str());
    let context = polymarket_context(event, &value);
    let rows = match event_type {
        "book" => vec![
            PolymarketOrderbookRow::book_from_raw_json(&event.raw_json, context)
                .map_err(invalid_data)?,
        ],
        "price_change" => {
            let market = required_value_string(&value, "market")?;
            let timestamp = optional_i64(&value, "timestamp");
            value
                .get("price_changes")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid_data("price_changes missing"))?
                .iter()
                .map(|change| {
                    let mut row = base_polymarket_row(
                        event,
                        &value,
                        "price_change",
                        market.clone(),
                        Some(required_value_string(change, "asset_id")?),
                        timestamp,
                    );
                    row.slug = change
                        .get("slug")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                        .or_else(|| {
                            value
                                .get("slug")
                                .and_then(Value::as_str)
                                .map(ToOwned::to_owned)
                        });
                    row.price = optional_string(change, "price");
                    row.size = optional_string(change, "size");
                    row.side = optional_string(change, "side");
                    row.best_bid = optional_string(change, "best_bid");
                    row.best_ask = optional_string(change, "best_ask");
                    row.hash = optional_string(change, "hash");
                    Ok(row)
                })
                .collect::<Result<Vec<_>>>()?
        }
        "last_trade_price" => {
            let row = PolymarketOrderbookRow::last_trade_price(
                &required_value_string(&value, "market")?,
                &required_value_string(&value, "asset_id")?,
                required_i64(&value, "timestamp")?,
                &required_value_string(&value, "price")?,
                value.get("fee_rate_bps").and_then(Value::as_str),
                value.get("side").and_then(Value::as_str),
                value.get("size").and_then(Value::as_str),
                value.get("transaction_hash").and_then(Value::as_str),
                context,
            );
            vec![row]
        }
        "tick_size_change" => vec![PolymarketOrderbookRow::tick_size_change(
            &required_value_string(&value, "market")?,
            &required_value_string(&value, "asset_id")?,
            required_i64(&value, "timestamp")?,
            &required_value_string(&value, "old_tick_size")?,
            &required_value_string(&value, "new_tick_size")?,
            context,
        )],
        "market_resolved" => {
            let resolved_asset_ids = value
                .get("resolved_event_asset_ids")
                .or_else(|| value.get("asset_ids"))
                .and_then(Value::as_array)
                .ok_or_else(|| invalid_data("resolved_event_asset_ids missing"))?
                .iter()
                .map(json_scalar_to_string)
                .collect::<Result<Vec<_>>>()?;
            vec![PolymarketOrderbookRow::market_resolved(
                &required_value_string(&value, "market")?,
                optional_i64(&value, "timestamp").unwrap_or(event.recv_time_utc_ms),
                &required_value_string(&value, "winning_asset_id")?,
                &required_value_string(&value, "winning_outcome")?,
                value.get("close_reason_if_present").and_then(Value::as_str),
                &resolved_asset_ids,
                context,
            )]
        }
        "new_market" => {
            let mut row = base_polymarket_row(
                event,
                &value,
                "new_market",
                required_value_string(&value, "market")?,
                None,
                optional_i64(&value, "timestamp"),
            );
            let asset_ids = value
                .get("asset_ids")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid_data("asset_ids missing"))?
                .iter()
                .map(json_scalar_to_string)
                .collect::<Result<Vec<_>>>()?;
            row.subscribed_asset_ids_for_market = Some(serde_json::to_string(&asset_ids)?);
            vec![row]
        }
        "market_discovery_switch" => Vec::new(),
        "best_bid_ask" => {
            let mut row = base_polymarket_row(
                event,
                &value,
                "best_bid_ask",
                required_value_string(&value, "market")?,
                Some(required_value_string(&value, "asset_id")?),
                optional_i64(&value, "timestamp"),
            );
            row.best_bid = optional_string(&value, "best_bid");
            row.best_ask = optional_string(&value, "best_ask");
            vec![row]
        }
        other => {
            return Err(
                invalid_data(format!("unsupported polymarket market event type: {other}")).into(),
            );
        }
    };
    Ok(rows)
}

pub(super) fn write_polymarket_orderbook_live_event(
    writer: &mut TypedParquetSegmentWriter,
    event: &RawEvent,
    logical_bytes: u64,
) -> Result<u64> {
    let rows = polymarket_orderbook_rows_from_event(event)?;
    let written = rows.len() as u64;
    require_at_most_one_typed_row(event, written)?;
    writer.write_polymarket_orderbook_rows_with_logical_bytes(&rows, logical_bytes)?;
    Ok(written)
}

pub(super) fn write_polymarket_metadata_v3_live_event(
    writer: &mut TypedParquetSegmentWriter,
    event: &RawEvent,
    logical_bytes: u64,
) -> Result<u64> {
    let rows = polymarket_metadata_v3_rows_from_event(event)?;
    let written = rows.len() as u64;
    require_at_most_one_typed_row(event, written)?;
    writer.write_polymarket_market_metadata_v3_rows_with_logical_bytes(&rows, logical_bytes)?;
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

fn polymarket_context(event: &RawEvent, value: &Value) -> PolymarketOrderbookContext {
    PolymarketOrderbookContext {
        timestamp_received_ms: event.recv_time_utc_ms,
        slug: value
            .get("slug")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        source_native_payload_json: value
            .get("source_native_payload_json")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        collector_host_id: event.collector_host_id.clone(),
        collector_region: event.collector_region.clone(),
        source_endpoint: event.source_endpoint.clone(),
        schema_version: POLYMARKET_ORDERBOOK_SCHEMA_VERSION.to_owned(),
    }
}

fn base_polymarket_row(
    event: &RawEvent,
    value: &Value,
    event_type: &str,
    market: String,
    asset_id: Option<String>,
    timestamp: Option<i64>,
) -> PolymarketOrderbookRow {
    let context = polymarket_context(event, value);
    PolymarketOrderbookRow {
        timestamp_received: context.timestamp_received_ms,
        timestamp,
        market,
        event_type: event_type.to_owned(),
        source_native_payload_json: context.source_native_payload_json,
        asset_id,
        bids: None,
        asks: None,
        price: None,
        size: None,
        side: None,
        best_bid: None,
        best_ask: None,
        hash: None,
        fee_rate_bps: None,
        transaction_hash: None,
        old_tick_size: None,
        new_tick_size: None,
        winning_asset_id: None,
        winning_outcome: None,
        close_reason_if_present: None,
        subscribed_asset_ids_for_market: None,
        resolved_event_asset_ids: None,
        slug: context.slug,
        collector_host_id: context.collector_host_id,
        collector_region: context.collector_region,
        source_endpoint: context.source_endpoint,
        schema_version: context.schema_version,
    }
}
