use super::*;

#[derive(Default)]
pub(super) struct CoverageStats {
    first_recv_time_utc_ms: Option<i64>,
    last_recv_time_utc_ms: Option<i64>,
    pub(super) rows: u64,
    market_windows: BTreeSet<String>,
    market_window_first_recv_time_utc_ms: BTreeMap<String, i64>,
    event_type_counts: BTreeMap<String, u64>,
}

impl CoverageStats {
    pub(super) fn to_source_coverage(&self, source_kind: &str) -> Option<SourceCoverage> {
        Some(SourceCoverage {
            source_kind: source_kind.to_owned(),
            first_recv_time_utc_ms: self.first_recv_time_utc_ms?,
            last_recv_time_utc_ms: self.last_recv_time_utc_ms?,
            rows: self.rows,
        })
    }
}

pub(super) fn update_coverage(coverage: &mut BTreeMap<String, CoverageStats>, event: &RawEvent) {
    let stats = coverage.entry(event.source_kind.clone()).or_default();
    stats.rows = stats.rows.saturating_add(1);
    *stats
        .event_type_counts
        .entry(event.source_event_type.clone())
        .or_default() += 1;
    stats.first_recv_time_utc_ms = stats
        .first_recv_time_utc_ms
        .map_or(Some(event.recv_time_utc_ms), |value| {
            Some(value.min(event.recv_time_utc_ms))
        });
    stats.last_recv_time_utc_ms = stats
        .last_recv_time_utc_ms
        .map_or(Some(event.recv_time_utc_ms), |value| {
            Some(value.max(event.recv_time_utc_ms))
        });
    if event.source_kind == "polymarket_market"
        && let Some((slug, _)) = event.source_identifier.split_once(':')
        && slug.starts_with("btc-updown-5m-")
    {
        stats.market_windows.insert(slug.to_owned());
        stats
            .market_window_first_recv_time_utc_ms
            .entry(slug.to_owned())
            .and_modify(|value| *value = (*value).min(event.recv_time_utc_ms))
            .or_insert(event.recv_time_utc_ms);
    }
}

pub(super) fn update_polymarket_v3_coverage(
    coverage: &mut BTreeMap<String, CoverageStats>,
    message: &PolymarketOrderbookV3Message,
) {
    let recv_time_utc_ms = message.envelope.timestamp_received;
    let stats = coverage.entry("polymarket_market".to_owned()).or_default();
    stats.rows = stats.rows.saturating_add(1);
    *stats
        .event_type_counts
        .entry(message.event_type().to_owned())
        .or_default() += 1;
    stats.first_recv_time_utc_ms = stats
        .first_recv_time_utc_ms
        .map_or(Some(recv_time_utc_ms), |value| {
            Some(value.min(recv_time_utc_ms))
        });
    stats.last_recv_time_utc_ms = stats
        .last_recv_time_utc_ms
        .map_or(Some(recv_time_utc_ms), |value| {
            Some(value.max(recv_time_utc_ms))
        });
    if let Some(slug) = message
        .envelope
        .slug
        .as_deref()
        .filter(|slug| slug.starts_with("btc-updown-5m-"))
    {
        stats.market_windows.insert(slug.to_owned());
        stats
            .market_window_first_recv_time_utc_ms
            .entry(slug.to_owned())
            .and_modify(|value| *value = (*value).min(recv_time_utc_ms))
            .or_insert(recv_time_utc_ms);
    }
}
