use super::*;

#[derive(Default)]
pub(super) struct CoverageStats {
    first_recv_time_utc_ms: Option<i64>,
    last_recv_time_utc_ms: Option<i64>,
    pub(super) rows: u64,
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
}

pub(super) fn update_polymarket_v3_coverage(
    coverage: &mut BTreeMap<String, CoverageStats>,
    message: &PolymarketOrderbookV3Message,
) {
    let recv_time_utc_ms = message.envelope.timestamp_received;
    let stats = coverage.entry("polymarket_market".to_owned()).or_default();
    stats.rows = stats.rows.saturating_add(1);
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
}
