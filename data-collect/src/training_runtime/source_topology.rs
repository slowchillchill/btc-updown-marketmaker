use super::*;

pub(super) const LOGICAL_PRODUCERS: &[&str] = &[
    "polymarket",
    "polymarket_rtds",
    "binance_depth",
    "binance_aggtrade",
];

const SOURCE_PRODUCER_OWNERSHIP: &[(SourceKind, &str)] = &[
    (SourceKind::PolymarketMarket, "polymarket"),
    (SourceKind::PolymarketMarketMetadata, "polymarket"),
    (SourceKind::PolymarketRtdsChainlinkBtcUsd, "polymarket_rtds"),
    (SourceKind::PolymarketRtdsChainlinkTwap, "polymarket_rtds"),
    (SourceKind::BinanceDepthSnapshot, "binance_depth"),
    (SourceKind::BinanceDepthUpdate, "binance_depth"),
    (SourceKind::BinanceAggTrade, "binance_aggtrade"),
];

pub fn training_source_kinds() -> [&'static str; 7] {
    SourceKind::ALL.map(SourceKind::as_str)
}

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

pub(super) fn producer_writer_sources(producer: &str) -> Result<Vec<&'static str>> {
    if !LOGICAL_PRODUCERS.contains(&producer) {
        return Err(invalid_data(format!("unknown logical producer: {producer}")).into());
    }
    Ok(SOURCE_PRODUCER_OWNERSHIP
        .iter()
        .filter_map(|(source_kind, owner)| (*owner == producer).then_some(source_kind.as_str()))
        .collect())
}

pub(super) fn logical_producer_for_source_kind(source_kind: &str) -> Result<&'static str> {
    let source_kind = source_kind.parse::<SourceKind>().map_err(invalid_data)?;
    SOURCE_PRODUCER_OWNERSHIP
        .iter()
        .find_map(|(candidate, producer)| (*candidate == source_kind).then_some(*producer))
        .ok_or_else(|| {
            invalid_data(format!(
                "raw event source has no mandatory logical producer mapping: {}",
                source_kind.as_str()
            ))
            .into()
        })
}
