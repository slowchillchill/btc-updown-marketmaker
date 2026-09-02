use super::*;

pub(super) fn owned_event_bytes(event: &RawEvent) -> usize {
    std::mem::size_of::<RawEvent>()
        .saturating_add(event.source_kind.capacity())
        .saturating_add(event.source_event_type.capacity())
        .saturating_add(event.source_identifier.capacity())
        .saturating_add(event.source_ts.as_ref().map_or(0, String::capacity))
        .saturating_add(event.collector_host_id.capacity())
        .saturating_add(event.collector_region.capacity())
        .saturating_add(event.source_endpoint.capacity())
        .saturating_add(event.raw_json.capacity())
}

pub(super) fn owned_polymarket_message_bytes(message: &WsMessage) -> usize {
    let base = std::mem::size_of::<WsMessage>();
    match message {
        WsMessage::Book(event) => base
            .saturating_add(
                event
                    .bids
                    .capacity()
                    .saturating_mul(std::mem::size_of::<OrderBookLevel>()),
            )
            .saturating_add(
                event
                    .asks
                    .capacity()
                    .saturating_mul(std::mem::size_of::<OrderBookLevel>()),
            )
            .saturating_add(event.hash.as_ref().map_or(0, String::capacity)),
        WsMessage::PriceChange(event) => base
            .saturating_add(
                event
                    .price_changes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<PriceChangeBatchEntry>()),
            )
            .saturating_add(
                event
                    .price_changes
                    .iter()
                    .map(|change| change.hash.as_ref().map_or(0, String::capacity))
                    .sum::<usize>(),
            ),
        WsMessage::NewMarket(event) => base
            .saturating_add(event.id.capacity())
            .saturating_add(event.question.as_ref().map_or(0, String::capacity))
            .saturating_add(event.slug.as_ref().map_or(0, String::capacity))
            .saturating_add(event.description.as_ref().map_or(0, String::capacity))
            .saturating_add(
                event
                    .asset_ids
                    .capacity()
                    .saturating_mul(std::mem::size_of::<U256>()),
            )
            .saturating_add(owned_string_vec_bytes(&event.outcomes))
            .saturating_add(
                event
                    .event_message
                    .as_ref()
                    .map_or(0, owned_polymarket_event_message_bytes),
            ),
        WsMessage::MarketResolved(event) => base
            .saturating_add(event.id.capacity())
            .saturating_add(event.question.as_ref().map_or(0, String::capacity))
            .saturating_add(event.slug.as_ref().map_or(0, String::capacity))
            .saturating_add(event.description.as_ref().map_or(0, String::capacity))
            .saturating_add(
                event
                    .asset_ids
                    .capacity()
                    .saturating_mul(std::mem::size_of::<U256>()),
            )
            .saturating_add(owned_string_vec_bytes(&event.outcomes))
            .saturating_add(event.winning_outcome.as_ref().map_or(0, String::capacity))
            .saturating_add(
                event
                    .event_message
                    .as_ref()
                    .map_or(0, owned_polymarket_event_message_bytes),
            ),
        WsMessage::TickSizeChange(_)
        | WsMessage::LastTradePrice(_)
        | WsMessage::BestBidAsk(_)
        | WsMessage::Trade(_)
        | WsMessage::Order(_) => base,
        _ => MAX_TYPED_ROW_BYTES.saturating_add(1),
    }
}

#[allow(clippy::ptr_arg)]
fn owned_string_vec_bytes(values: &Vec<String>) -> usize {
    values
        .capacity()
        .saturating_mul(std::mem::size_of::<String>())
        .saturating_add(values.iter().map(String::capacity).sum::<usize>())
}

fn owned_polymarket_event_message_bytes(message: &EventMessage) -> usize {
    message
        .id
        .capacity()
        .saturating_add(message.ticker.as_ref().map_or(0, String::capacity))
        .saturating_add(message.slug.as_ref().map_or(0, String::capacity))
        .saturating_add(message.title.as_ref().map_or(0, String::capacity))
        .saturating_add(message.description.as_ref().map_or(0, String::capacity))
}
