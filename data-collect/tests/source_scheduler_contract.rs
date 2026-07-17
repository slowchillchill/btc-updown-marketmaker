use data_collect::SourceCoverage;
use data_collect::scheduler::{
    MarketCoveragePhase, ObservedSourceCoverage, SourceRole, SourceScheduler, SourceStartupAction,
};

#[test]
fn source_scheduler_starts_aux_sources_independently() {
    let scheduler = SourceScheduler::four_source(30);

    let decisions = scheduler.startup_decisions(false);

    assert_eq!(
        decisions
            .iter()
            .find(|decision| decision.role == SourceRole::PolymarketMarket)
            .map(|decision| decision.action),
        Some(SourceStartupAction::RecordSourceGap)
    );
    for role in [
        SourceRole::BinanceDepth,
        SourceRole::BinanceAggTrade,
        SourceRole::RtdsChainlinkBtcUsd,
    ] {
        assert_eq!(
            decisions
                .iter()
                .find(|decision| decision.role == role)
                .map(|decision| decision.action),
            Some(SourceStartupAction::Start),
            "{role:?} must not wait for Polymarket asset discovery"
        );
    }
}

#[test]
fn four_sources_have_aligned_t30_preload_coverage_or_gap() {
    let scheduler = SourceScheduler::four_source(30);
    let windows = scheduler.market_windows(
        "btc-updown-5m-1766789400",
        1_766_789_400_000,
        "btc-updown-5m-1766789700",
        1_766_789_700_000,
        300_000,
    );
    assert_eq!(
        windows[1].coverage_start_utc_ms,
        windows[1].market_start_utc_ms - 30_000
    );

    let mut observed = Vec::new();
    for role in SourceRole::all() {
        for phase in [
            MarketCoveragePhase::CurrentActive,
            MarketCoveragePhase::PlannedNextPreload,
        ] {
            observed.push(ObservedSourceCoverage {
                role,
                phase,
                market_slug: if phase == MarketCoveragePhase::PlannedNextPreload {
                    windows[1].market_slug.clone()
                } else {
                    windows[0].market_slug.clone()
                },
                coverage: Some(SourceCoverage {
                    source_kind: role.as_str().to_owned(),
                    first_recv_time_utc_ms: if phase == MarketCoveragePhase::PlannedNextPreload {
                        windows[1].coverage_start_utc_ms
                    } else {
                        windows[0].coverage_start_utc_ms
                    },
                    last_recv_time_utc_ms: if phase == MarketCoveragePhase::PlannedNextPreload {
                        windows[1].market_start_utc_ms
                    } else {
                        windows[0].market_end_utc_ms
                    },
                    rows: 1,
                }),
                first_business_event_utc_ms: Some(
                    if phase == MarketCoveragePhase::PlannedNextPreload {
                        windows[1].coverage_start_utc_ms + 1
                    } else {
                        windows[0].coverage_start_utc_ms + 1
                    },
                ),
                source_gap: None,
                source_gap_evidence: None,
            });
        }
    }
    let rtds_next = observed
        .iter_mut()
        .find(|item| {
            item.role == SourceRole::RtdsChainlinkBtcUsd
                && item.phase == MarketCoveragePhase::PlannedNextPreload
        })
        .expect("rtds next observed");
    rtds_next.coverage = None;
    rtds_next.first_business_event_utc_ms = None;
    rtds_next.source_gap = Some("rtds_sdk_stream_fatal:receiver lagged by 1 messages".to_owned());
    rtds_next.source_gap_evidence = Some("lagged_messages=1".to_owned());

    let report = scheduler.evaluate_four_source_preload_coverage(&windows, &observed);
    assert!(report.is_complete_or_gap_accounted());

    observed.retain(|item| {
        !(item.role == SourceRole::BinanceAggTrade
            && item.phase == MarketCoveragePhase::PlannedNextPreload)
    });
    let report = scheduler.evaluate_four_source_preload_coverage(&windows, &observed);
    assert!(!report.is_complete_or_gap_accounted());
    assert!(report.entries.iter().any(|entry| {
        entry.role == SourceRole::BinanceAggTrade
            && entry.phase == MarketCoveragePhase::PlannedNextPreload
            && entry.reason.as_deref() == Some("missing_source_coverage_or_gap")
    }));

    let mut late_observed = observed.clone();
    let depth_next = late_observed
        .iter_mut()
        .find(|item| {
            item.role == SourceRole::BinanceDepth
                && item.phase == MarketCoveragePhase::PlannedNextPreload
        })
        .expect("depth next observed");
    depth_next.coverage = Some(SourceCoverage {
        source_kind: SourceRole::BinanceDepth.as_str().to_owned(),
        first_recv_time_utc_ms: windows[1].market_start_utc_ms + 1,
        last_recv_time_utc_ms: windows[1].market_start_utc_ms + 2,
        rows: 1,
    });
    depth_next.first_business_event_utc_ms = Some(windows[1].market_start_utc_ms + 1);
    let report = scheduler.evaluate_four_source_preload_coverage(&windows, &late_observed);
    assert!(!report.is_complete_or_gap_accounted());
    assert!(report.entries.iter().any(|entry| {
        entry.role == SourceRole::BinanceDepth
            && entry.phase == MarketCoveragePhase::PlannedNextPreload
            && entry.reason.as_deref() == Some("coverage_or_first_business_event_missing")
    }));

    let mut gap_without_evidence = observed;
    let polymarket_current = gap_without_evidence
        .iter_mut()
        .find(|item| {
            item.role == SourceRole::PolymarketMarket
                && item.phase == MarketCoveragePhase::CurrentActive
        })
        .expect("polymarket current observed");
    polymarket_current.coverage = None;
    polymarket_current.first_business_event_utc_ms = None;
    polymarket_current.source_gap = Some("polymarket_asset_ids_or_metadata_not_ready".to_owned());
    polymarket_current.source_gap_evidence = None;
    let report = scheduler.evaluate_four_source_preload_coverage(&windows, &gap_without_evidence);
    assert!(!report.is_complete_or_gap_accounted());
    assert!(report.entries.iter().any(|entry| {
        entry.role == SourceRole::PolymarketMarket
            && entry.phase == MarketCoveragePhase::CurrentActive
            && entry.reason.as_deref() == Some("source_gap_evidence_missing")
    }));
}

#[test]
fn source_scheduler_requires_current_and_preload_windows() {
    let scheduler = SourceScheduler::four_source(30);
    let windows = scheduler.market_windows(
        "btc-updown-5m-1766789400",
        1_766_789_400_000,
        "btc-updown-5m-1766789700",
        1_766_789_700_000,
        300_000,
    );

    let report = scheduler.evaluate_four_source_preload_coverage(&windows[..1], &[]);

    assert!(!report.is_complete_or_gap_accounted());
    assert!(report.entries.iter().any(|entry| {
        entry.phase == MarketCoveragePhase::PlannedNextPreload
            && entry.reason.as_deref() == Some("missing_market_coverage_window")
    }));
}

#[test]
fn source_gap_must_match_target_market_slug() {
    let scheduler = SourceScheduler::four_source(30);
    let windows = scheduler.market_windows(
        "btc-updown-5m-1766789400",
        1_766_789_400_000,
        "btc-updown-5m-1766789700",
        1_766_789_700_000,
        300_000,
    );
    let observed = vec![ObservedSourceCoverage {
        role: SourceRole::PolymarketMarket,
        phase: MarketCoveragePhase::PlannedNextPreload,
        market_slug: "btc-updown-5m-1766790000".to_owned(),
        coverage: None,
        first_business_event_utc_ms: None,
        source_gap: Some("polymarket_asset_ids_or_metadata_not_ready".to_owned()),
        source_gap_evidence: Some("gap_artifact_sha256=abc123".to_owned()),
    }];

    let report = scheduler.evaluate_four_source_preload_coverage(&windows, &observed);

    assert!(!report.is_complete_or_gap_accounted());
    assert!(report.entries.iter().any(|entry| {
        entry.role == SourceRole::PolymarketMarket
            && entry.phase == MarketCoveragePhase::PlannedNextPreload
            && entry.reason.as_deref() == Some("market_slug_mismatch")
    }));
}
