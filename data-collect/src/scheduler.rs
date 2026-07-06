use std::collections::BTreeMap;

use crate::event::SourceKind;
use crate::manifest::SourceCoverage;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SourceRole {
    PolymarketMarket,
    BinanceDepth,
    BinanceAggTrade,
    RtdsChainlinkBtcUsd,
}

impl SourceRole {
    pub const fn all() -> [Self; 4] {
        [
            Self::PolymarketMarket,
            Self::BinanceDepth,
            Self::BinanceAggTrade,
            Self::RtdsChainlinkBtcUsd,
        ]
    }

    pub const fn source_kind(self) -> SourceKind {
        match self {
            Self::PolymarketMarket => SourceKind::PolymarketMarket,
            Self::BinanceDepth => SourceKind::BinanceDepthUpdate,
            Self::BinanceAggTrade => SourceKind::BinanceAggTrade,
            Self::RtdsChainlinkBtcUsd => SourceKind::PolymarketRtdsChainlinkBtcUsd,
        }
    }

    pub const fn requires_polymarket_assets(self) -> bool {
        matches!(self, Self::PolymarketMarket)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PolymarketMarket => "polymarket_market",
            Self::BinanceDepth => "binance_depth_update",
            Self::BinanceAggTrade => "binance_aggtrade",
            Self::RtdsChainlinkBtcUsd => "polymarket_rtds_chainlink_btc_usd",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MarketCoveragePhase {
    CurrentActive,
    PlannedNextPreload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceStartupPlan {
    pub role: SourceRole,
    pub source_kind: String,
    pub requires_polymarket_assets: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceStartupAction {
    Start,
    RecordSourceGap,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceStartupDecision {
    pub role: SourceRole,
    pub action: SourceStartupAction,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketCoverageWindow {
    pub phase: MarketCoveragePhase,
    pub market_slug: String,
    pub coverage_start_utc_ms: i64,
    pub market_start_utc_ms: i64,
    pub market_end_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ObservedSourceCoverage {
    pub role: SourceRole,
    pub phase: MarketCoveragePhase,
    pub market_slug: String,
    pub coverage: Option<SourceCoverage>,
    pub first_business_event_utc_ms: Option<i64>,
    pub source_gap: Option<String>,
    pub source_gap_evidence: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoverageAlignmentEntry {
    pub role: SourceRole,
    pub phase: MarketCoveragePhase,
    pub covered_or_gap: bool,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FourSourceCoverageReport {
    pub entries: Vec<CoverageAlignmentEntry>,
}

impl FourSourceCoverageReport {
    pub fn is_complete_or_gap_accounted(&self) -> bool {
        !self.entries.is_empty() && self.entries.iter().all(|entry| entry.covered_or_gap)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceScheduler {
    pub preload_seconds: i64,
    pub startup_plan: Vec<SourceStartupPlan>,
}

impl SourceScheduler {
    pub fn four_source(preload_seconds: i64) -> Self {
        Self {
            preload_seconds,
            startup_plan: SourceRole::all()
                .into_iter()
                .map(|role| SourceStartupPlan {
                    role,
                    source_kind: role.as_str().to_owned(),
                    requires_polymarket_assets: role.requires_polymarket_assets(),
                })
                .collect(),
        }
    }

    pub fn startup_decisions(&self, polymarket_assets_ready: bool) -> Vec<SourceStartupDecision> {
        self.startup_plan
            .iter()
            .map(|plan| {
                if plan.requires_polymarket_assets && !polymarket_assets_ready {
                    SourceStartupDecision {
                        role: plan.role,
                        action: SourceStartupAction::RecordSourceGap,
                        reason: Some("polymarket_asset_ids_or_metadata_not_ready".to_owned()),
                    }
                } else {
                    SourceStartupDecision {
                        role: plan.role,
                        action: SourceStartupAction::Start,
                        reason: None,
                    }
                }
            })
            .collect()
    }

    pub fn market_windows(
        &self,
        current_slug: impl Into<String>,
        current_start_utc_ms: i64,
        next_slug: impl Into<String>,
        next_start_utc_ms: i64,
        market_duration_ms: i64,
    ) -> [MarketCoverageWindow; 2] {
        let preload_ms = self.preload_seconds.saturating_mul(1000);
        [
            MarketCoverageWindow {
                phase: MarketCoveragePhase::CurrentActive,
                market_slug: current_slug.into(),
                coverage_start_utc_ms: current_start_utc_ms,
                market_start_utc_ms: current_start_utc_ms,
                market_end_utc_ms: current_start_utc_ms.saturating_add(market_duration_ms),
            },
            MarketCoverageWindow {
                phase: MarketCoveragePhase::PlannedNextPreload,
                market_slug: next_slug.into(),
                coverage_start_utc_ms: next_start_utc_ms.saturating_sub(preload_ms),
                market_start_utc_ms: next_start_utc_ms,
                market_end_utc_ms: next_start_utc_ms.saturating_add(market_duration_ms),
            },
        ]
    }

    pub fn evaluate_four_source_preload_coverage(
        &self,
        windows: &[MarketCoverageWindow],
        observed: &[ObservedSourceCoverage],
    ) -> FourSourceCoverageReport {
        let observed_by_key = observed
            .iter()
            .map(|item| ((item.role, item.phase), item))
            .collect::<BTreeMap<_, _>>();

        let mut entries = Vec::new();
        for role in SourceRole::all() {
            for phase in [
                MarketCoveragePhase::CurrentActive,
                MarketCoveragePhase::PlannedNextPreload,
            ] {
                let Some(window) = windows.iter().find(|window| window.phase == phase) else {
                    entries.push(CoverageAlignmentEntry {
                        role,
                        phase,
                        covered_or_gap: false,
                        reason: Some("missing_market_coverage_window".to_owned()),
                    });
                    continue;
                };
                let entry = match observed_by_key.get(&(role, phase)) {
                    Some(observed) => evaluate_observed_window(role, phase, window, observed),
                    None => CoverageAlignmentEntry {
                        role,
                        phase,
                        covered_or_gap: false,
                        reason: Some("missing_source_coverage_or_gap".to_owned()),
                    },
                };
                entries.push(entry);
            }
        }

        FourSourceCoverageReport { entries }
    }
}

fn evaluate_observed_window(
    role: SourceRole,
    phase: MarketCoveragePhase,
    window: &MarketCoverageWindow,
    observed: &ObservedSourceCoverage,
) -> CoverageAlignmentEntry {
    let market_slug_ok = observed.market_slug == window.market_slug;
    if let Some(source_gap) = observed
        .source_gap
        .as_deref()
        .filter(|reason| !reason.trim().is_empty())
    {
        if !market_slug_ok {
            return CoverageAlignmentEntry {
                role,
                phase,
                covered_or_gap: false,
                reason: Some("market_slug_mismatch".to_owned()),
            };
        }
        if observed
            .source_gap_evidence
            .as_deref()
            .is_none_or(|evidence| evidence.trim().is_empty())
        {
            return CoverageAlignmentEntry {
                role,
                phase,
                covered_or_gap: false,
                reason: Some("source_gap_evidence_missing".to_owned()),
            };
        }
        return CoverageAlignmentEntry {
            role,
            phase,
            covered_or_gap: true,
            reason: Some(source_gap.to_owned()),
        };
    }

    let required_coverage_end_utc_ms = match phase {
        MarketCoveragePhase::CurrentActive => window.market_end_utc_ms,
        MarketCoveragePhase::PlannedNextPreload => window.market_start_utc_ms,
    };
    let coverage_ok = observed.coverage.as_ref().is_some_and(|coverage| {
        coverage.source_kind == role.as_str()
            && coverage.rows > 0
            && coverage.first_recv_time_utc_ms <= window.coverage_start_utc_ms
            && coverage.last_recv_time_utc_ms >= required_coverage_end_utc_ms
    });
    let first_business_ok = observed
        .first_business_event_utc_ms
        .is_some_and(|ts| ts >= window.coverage_start_utc_ms && ts <= required_coverage_end_utc_ms);

    CoverageAlignmentEntry {
        role,
        phase,
        covered_or_gap: market_slug_ok && coverage_ok && first_business_ok,
        reason: if market_slug_ok && coverage_ok && first_business_ok {
            None
        } else if !market_slug_ok {
            Some("market_slug_mismatch".to_owned())
        } else {
            Some("coverage_or_first_business_event_missing".to_owned())
        },
    }
}
