use super::*;
use contracts::*;
use std::sync::Arc;
fn observed<T>(value: T) -> ObservedFactV1<T> {
    ObservedFactV1 {
        value: Some(value),
        ..Default::default()
    }
}
fn observation(k: Option<&str>) -> MarketBusinessObservationV1 {
    let level = |p: &str, q: &str| ReferencePriceLevelV1 {
        price_usdt_per_btc: ExactDecimal(p.into()),
        quantity_btc: ExactDecimal(q.into()),
    };
    MarketBusinessObservationV1 {
        schema_version: "market_business_observation_v1".into(),
        market: MarketIdentityV2 {
            condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
            clob_market_id: ClobMarketId("3195576".into()),
            window: MarketWindowV2 {
                start_at_utc_ms: 0,
                end_at_utc_ms: 300000,
            },
        },
        group_id: Sha256Hex("a".repeat(64)),
        group_position: 0,
        visible_prefix: AppliedIngressPrefixV1 {
            input_stream_id: "test".into(),
            next_input_position: 1,
        },
        assets: vec![],
        lifecycle: Default::default(),
        active: Default::default(),
        closed: Default::default(),
        accepting_orders: Default::default(),
        terminal: Default::default(),
        official_anchor: k
            .map(|k| {
                observed(MarketPriceAnchorV1 {
                    price_usd_per_btc: ExactDecimal(k.into()),
                    source: MarketAnchorSourceV1::GammaEventMetadataPriceToBeat,
                    application: PriceAnchorApplicationV1::Observed,
                })
            })
            .unwrap_or_default(),
        settlement_reference: Default::default(),
        temporary_reference: None,
        effective_reference: EffectiveMarketReferenceV1::Missing,
        chainlink_spot: Default::default(),
        chainlink_twap_thirty: Default::default(),
        chainlink_twap_sixty: Default::default(),
        binance_depth: Arc::new(observed(BinanceDepthObservationV1::Full(Arc::new(
            BinanceDepthStateReferenceV1 {
                symbol: "BTCUSDT".into(),
                last_update_id: 1,
                bids: vec![level("99", "2"), level("98", "3"), level("97", "9")],
                asks: vec![level("101", "2"), level("102", "3"), level("103", "9")],
            },
        )))),
        binance_trade: Default::default(),
        group_events: Arc::from([]),
        source_progress: Default::default(),
    }
}
#[test]
fn hand_calculated_bid_and_ask_intervals() {
    for (k, side, v) in [("98", "bid", 492), ("102", "ask", 508)] {
        let result = blb_research_v1(&observation(Some(k)));
        assert_eq!(result.midpoint_usdt_per_btc, Some(Decimal::from(100)));
        assert_eq!(result.side, Some(side));
        assert_eq!(result.distance, Some(Decimal::TWO));
        assert_eq!(result.visible_level_count, Some(2));
        assert_eq!(result.visible_quantity_btc, Some(Decimal::from(5)));
        assert_eq!(result.visible_notional_usdt, Some(Decimal::from(v)));
        assert_eq!(result.visible_positive_levels.len(), 2);
        assert_eq!(result.interval_coverage, "unknown_no_coverage_evidence");
    }
}
#[test]
fn anchor_missing_at_anchor_and_empty_visible_interval_are_distinct() {
    let missing = blb_research_v1(&observation(None));
    assert_eq!(missing.unknown_reason, Some("official_anchor_not_observed"));
    assert_eq!(missing.midpoint_usdt_per_btc, Some(Decimal::from(100)));
    assert_eq!(missing.visible_quantity_btc, None);
    let at = blb_research_v1(&observation(Some("100")));
    assert!(at.at_anchor);
    assert_eq!(at.status, "at_anchor");
    assert_eq!(at.visible_quantity_btc, None);
    let empty = blb_research_v1(&observation(Some("99.5")));
    assert_eq!(empty.visible_quantity_btc, Some(Decimal::ZERO));
    assert_eq!(empty.interval_coverage, "unknown_no_coverage_evidence");
}
