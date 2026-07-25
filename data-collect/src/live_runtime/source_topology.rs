use super::*;

pub(super) const LOGICAL_PRODUCERS: &[&str] = &[
    "polymarket",
    "polymarket_rtds",
    "binance_depth",
    "binance_aggtrade",
];

pub(super) fn logical_producer_for_source_task(source_kind: &str) -> Result<&'static str> {
    match source_kind {
        "polymarket_market" => Ok("polymarket"),
        "polymarket_rtds_chainlink_btc_usd" => Ok("polymarket_rtds"),
        "binance_depth_update" => Ok("binance_depth"),
        "binance_aggtrade" => Ok("binance_aggtrade"),
        other => Err(invalid_data(format!(
            "source task has no logical producer mapping: {other}"
        ))
        .into()),
    }
}

pub(super) fn producer_writer_sources(producer: &str) -> Result<&'static [&'static str]> {
    match producer {
        "polymarket" => Ok(&["polymarket_market", "polymarket_market_metadata"]),
        "polymarket_rtds" => Ok(&["polymarket_rtds_chainlink_btc_usd"]),
        "binance_depth" => Ok(&["binance_depth_snapshot", "binance_depth_update"]),
        "binance_aggtrade" => Ok(&["binance_aggtrade"]),
        other => Err(invalid_data(format!("unknown logical producer: {other}")).into()),
    }
}

pub(super) fn logical_producer_for_source_kind(source_kind: &str) -> Result<&'static str> {
    match source_kind {
        "polymarket_market" | "polymarket_market_metadata" => Ok("polymarket"),
        "polymarket_rtds_chainlink_btc_usd" => Ok("polymarket_rtds"),
        "binance_depth_snapshot" | "binance_depth_update" => Ok("binance_depth"),
        "binance_aggtrade" => Ok("binance_aggtrade"),
        other => Err(invalid_data(format!(
            "raw event source has no mandatory logical producer mapping: {other}"
        ))
        .into()),
    }
}

pub(super) fn validate_mandatory_producer_source_mapping() -> Result<()> {
    let producers = LOGICAL_PRODUCERS.iter().copied().collect::<BTreeSet<_>>();
    if producers.len() != LOGICAL_PRODUCERS.len() {
        return Err(invalid_data("mandatory logical producer list contains duplicates").into());
    }
    let live_sources = LIVE_SOURCES.iter().copied().collect::<BTreeSet<_>>();
    if live_sources.len() != LIVE_SOURCES.len() {
        return Err(invalid_data("mandatory live source list contains duplicates").into());
    }

    let mut forward_sources = BTreeSet::new();
    for producer in LOGICAL_PRODUCERS {
        let sources = producer_writer_sources(producer)?;
        if sources.is_empty() {
            return Err(invalid_data(format!(
                "mandatory logical producer has no writer source: {producer}"
            ))
            .into());
        }
        for source_kind in sources {
            if !live_sources.contains(source_kind) {
                return Err(invalid_data(format!(
                    "producer mapping references non-live source: producer={producer} source={source_kind}"
                ))
                .into());
            }
            if !forward_sources.insert(*source_kind) {
                return Err(invalid_data(format!(
                    "live source is mapped by more than one producer: {source_kind}"
                ))
                .into());
            }
            if logical_producer_for_source_kind(source_kind)? != *producer {
                return Err(invalid_data(format!(
                    "producer/source mapping is not bidirectionally consistent: producer={producer} source={source_kind}"
                ))
                .into());
            }
        }
    }
    if forward_sources != live_sources {
        return Err(invalid_data(format!(
            "producer mapping does not cover the exact live source set: mapped={forward_sources:?} live={live_sources:?}"
        ))
        .into());
    }
    for source_kind in LIVE_SOURCES {
        let producer = logical_producer_for_source_kind(source_kind)?;
        if !producers.contains(producer)
            || !producer_writer_sources(producer)?.contains(source_kind)
        {
            return Err(invalid_data(format!(
                "live source inverse mapping is not present in the producer mapping: source={source_kind} producer={producer}"
            ))
            .into());
        }
    }
    Ok(())
}
