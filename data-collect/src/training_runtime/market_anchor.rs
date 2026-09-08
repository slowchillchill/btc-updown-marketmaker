use super::*;
use crate::market_anchor::{
    MarketAnchorIdentity, MarketAnchorObservation, MarketAnchorOrigin, MarketAnchorQueryResult,
};
use crate::polymarket_market_metadata_v6_layout::{
    MarketAnchorRuleUpdateRow, PolymarketMarketMetadataV6Row,
};
use polymarket_client_sdk_v2::gamma::types::request::{
    EventBySlugRequest, WebsiteCryptoPriceRequest,
};

#[derive(Default)]
pub(super) struct MarketAnchorRuntime {
    pub(super) tasks: JoinSet<MarketAnchorCompletion>,
    slots: BTreeMap<String, MarketAnchorSlot>,
}

struct MarketAnchorSlot {
    ordinal: u64,
    next_at: TokioInstant,
    in_flight: bool,
    fallback: Option<WebsiteCryptoPriceRequest>,
}

pub(super) struct MarketAnchorCompletion {
    pub(super) row: MarketAnchorRuleUpdateRow,
    pub(super) raw_day_ticket: RawDayTicketAcceptance,
    fallback: Option<WebsiteCryptoPriceRequest>,
}

fn anchor_identity(market: &DiscoveredBtcUpdownMarket) -> Result<MarketAnchorIdentity> {
    Ok(MarketAnchorIdentity {
        slug: market.slug.clone(),
        condition_id: market.condition_id.clone(),
        clob_market_id: market.id.clone(),
        asset_ids: serde_json::to_string(&[
            market.up_asset_id.as_str(),
            market.down_asset_id.as_str(),
        ])?,
        market_start_utc: market.start_ts_s * 1_000,
        market_end_utc: market.end_ts_s * 1_000,
        fixed_identity_verified_at_utc: market.fixed_identity_verified_at_utc_ms,
    })
}

pub(super) fn spawn_due_market_anchor_requests<N>(
    gamma: &GammaClient,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    collector: &CollectorIdentity,
    timeline: &Arc<N>,
) -> Result<()>
where
    N: Fn() -> PolymarketTimelineNow + Send + Sync + 'static,
{
    let now = timeline();
    let markets = context
        .current
        .iter()
        .chain(context.next.iter())
        .cloned()
        .collect::<Vec<_>>();
    context.market_anchor.slots.retain(|slug, slot| {
        slot.in_flight
            || markets
                .iter()
                .any(|market| market.slug == *slug && now.utc_ms < market.end_ts_s * 1_000)
    });
    for market in markets {
        if now.utc_ms >= market.end_ts_s * 1_000 {
            continue;
        }
        let slot = context
            .market_anchor
            .slots
            .entry(market.slug.clone())
            .or_insert(MarketAnchorSlot {
                ordinal: 0,
                next_at: now.monotonic,
                in_flight: false,
                fallback: None,
            });
        if slot.in_flight
            || (slot.fallback.is_none()
                && (now.monotonic < slot.next_at
                    || (slot.ordinal > 0 && now.utc_ms < market.start_ts_s * 1_000)))
        {
            continue;
        }
        let fallback = slot.fallback.take();
        let origin = if fallback.is_some() {
            MarketAnchorOrigin::WebsiteCryptoPrice
        } else {
            MarketAnchorOrigin::GammaEventBySlug
        };
        if fallback.is_none() {
            slot.ordinal += 1;
        }
        let ordinal = slot.ordinal;
        slot.in_flight = true;
        slot.next_at = now.monotonic + Duration::from_secs(MARKET_NEXT_DISCOVERY_INTERVAL_SECS);
        let identity = anchor_identity(&market)?;
        let gamma = gamma.clone();
        let gate = sink
            .source_queue_backpressure
            .raw_day_gate()
            .ok_or_else(|| invalid_data("raw-day gate missing before anchor request"))?;
        let collector = collector.clone();
        let epoch = context.source_sequence_epoch_id.clone();
        let timeline = Arc::clone(timeline);
        context.market_anchor.tasks.spawn(async move {
            let started = timeline().utc_ms;
            let (received, raw_day_ticket, observation, next_fallback) =
                if let Some(request) = fallback {
                    match timeout(
                        Duration::from_secs(GAMMA_REQUEST_TIMEOUT_SECS),
                        gamma.website_crypto_price_capture(&request),
                    )
                    .await
                    {
                        Ok(Ok(capture)) => {
                            let received = capture.received_at_utc.timestamp_millis();
                            let ticket = anchor_response_ticket(&gate, received).await;
                            (
                                received,
                                ticket,
                                MarketAnchorObservation::from_website_capture(capture),
                                None,
                            )
                        }
                        result => {
                            let received = timeline().utc_ms;
                            let ticket = anchor_response_ticket(&gate, received).await;
                            let result = if result.is_err() {
                                MarketAnchorQueryResult::Timeout
                            } else {
                                MarketAnchorQueryResult::TransportError
                            };
                            (
                                received,
                                ticket,
                                MarketAnchorObservation::unavailable(result),
                                None,
                            )
                        }
                    }
                } else {
                    let request = EventBySlugRequest::builder()
                        .slug(identity.slug.clone())
                        .build();
                    match timeout(
                        Duration::from_secs(GAMMA_REQUEST_TIMEOUT_SECS),
                        gamma.event_by_slug_capture(&request),
                    )
                    .await
                    {
                        Ok(Ok(capture)) => {
                            let received = capture.received_at_utc.timestamp_millis();
                            let ticket = anchor_response_ticket(&gate, received).await;
                            let (observation, fallback) =
                                MarketAnchorObservation::from_gamma_capture(capture, &identity);
                            (received, ticket, observation, fallback)
                        }
                        result => {
                            let received = timeline().utc_ms;
                            let ticket = anchor_response_ticket(&gate, received).await;
                            let result = if result.is_err() {
                                MarketAnchorQueryResult::Timeout
                            } else {
                                MarketAnchorQueryResult::TransportError
                            };
                            (
                                received,
                                ticket,
                                MarketAnchorObservation::unavailable(result),
                                None,
                            )
                        }
                    }
                };
            let (lifecycle, role) = if received < identity.market_start_utc {
                (MarketLifecycleState::Prefetched, MarketRuntimeRole::Next)
            } else if received < identity.market_end_utc {
                (MarketLifecycleState::Active, MarketRuntimeRole::Current)
            } else {
                (MarketLifecycleState::Retired, MarketRuntimeRole::None)
            };
            MarketAnchorCompletion {
                row: MarketAnchorRuleUpdateRow {
                    timestamp_received: received,
                    collector_admission_sequence: 0,
                    collector_admission_time_utc_ms: 0,
                    metadata_origin: origin,
                    collector_host_id: collector.host_id,
                    collector_region: collector.region,
                    identity,
                    market_lifecycle_state: lifecycle,
                    market_runtime_role: role,
                    metadata_fetch_start_utc: started,
                    anchor_query_source_epoch_id: epoch,
                    anchor_query_ordinal: ordinal,
                    observation,
                },
                raw_day_ticket,
                fallback: next_fallback,
            }
        });
    }
    Ok(())
}

async fn anchor_response_ticket(gate: &Arc<RawDayGate>, received: i64) -> RawDayTicketAcceptance {
    match gate
        .accept_unadmitted_event_ticket("polymarket", received)
        .await
    {
        Ok(Some(ticket)) => RawDayTicketAcceptance::Accepted(ticket),
        Ok(None) => RawDayTicketAcceptance::Cutoff,
        Err(error) => RawDayTicketAcceptance::Failed(error.to_string()),
    }
}

pub(super) async fn record_market_anchor_completion(
    completion: MarketAnchorCompletion,
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    run_started: Instant,
) -> Result<()> {
    if let Some(slot) = context
        .market_anchor
        .slots
        .get_mut(&completion.row.identity.slug)
    {
        slot.in_flight = false;
        slot.fallback = completion.fallback;
    }
    let ticket = match completion.raw_day_ticket {
        RawDayTicketAcceptance::Accepted(ticket) => ticket,
        RawDayTicketAcceptance::Cutoff => return Ok(()),
        RawDayTicketAcceptance::Failed(error) => return Err(invalid_data(error).into()),
    };
    let event = polymarket_metadata_v6_raw_event(PolymarketMarketMetadataV6Row::AnchorRule(
        completion.row,
    ))?;
    send_event_with_ticket(sink, run_started, event, &ticket).await
}

pub(super) fn collect_ready_market_anchor_items(
    context: &mut PolymarketProcessingContext,
    items: &mut Vec<PolymarketBoundaryDrainItem>,
) -> Result<()> {
    while let Some(result) = context.market_anchor.tasks.try_join_next() {
        items.push(PolymarketBoundaryDrainItem::Anchor(Box::new(
            result.map_err(|error| {
                invalid_data(format!("market anchor request task failed:{error}"))
            })?,
        )));
    }
    Ok(())
}

pub(super) async fn collect_all_market_anchor_items(
    context: &mut PolymarketProcessingContext,
    items: &mut Vec<PolymarketBoundaryDrainItem>,
) -> Result<()> {
    while let Some(result) = context.market_anchor.tasks.join_next().await {
        items.push(PolymarketBoundaryDrainItem::Anchor(Box::new(
            result.map_err(|error| {
                invalid_data(format!("market anchor request task failed:{error}"))
            })?,
        )));
    }
    Ok(())
}

pub(super) async fn drain_ready_market_anchor_requests(
    context: &mut PolymarketProcessingContext,
    sink: &SourceEventSink,
    run_started: Instant,
) -> Result<()> {
    while let Some(result) = context.market_anchor.tasks.try_join_next() {
        record_market_anchor_completion(
            result.map_err(|error| {
                invalid_data(format!("market anchor request task failed:{error}"))
            })?,
            context,
            sink,
            run_started,
        )
        .await?;
    }
    Ok(())
}

pub(super) fn next_market_anchor_request_at(
    context: &PolymarketProcessingContext,
    now: PolymarketTimelineNow,
) -> Option<TokioInstant> {
    context
        .current
        .iter()
        .chain(context.next.iter())
        .filter(|market| now.utc_ms < market.end_ts_s * 1_000)
        .filter_map(|market| {
            let Some(slot) = context.market_anchor.slots.get(&market.slug) else {
                return Some(now.monotonic);
            };
            if slot.in_flight {
                return None;
            }
            if slot.fallback.is_some() {
                return Some(now.monotonic);
            }
            let start = if slot.ordinal > 0 {
                now.monotonic
                    + Duration::from_millis(
                        u64::try_from((market.start_ts_s * 1_000).saturating_sub(now.utc_ms))
                            .unwrap_or(0),
                    )
            } else {
                now.monotonic
            };
            Some(slot.next_at.max(start))
        })
        .min()
}
