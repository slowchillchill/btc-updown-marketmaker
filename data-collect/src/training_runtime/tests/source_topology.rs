use super::*;

#[test]
fn typed_source_ownership_covers_the_seven_sources_and_four_producers() {
    let approved = [
        (
            "polymarket",
            vec!["polymarket_market", "polymarket_market_metadata"],
        ),
        (
            "polymarket_rtds",
            vec![
                "polymarket_rtds_chainlink_btc_usd",
                "polymarket_rtds_chainlink_twap",
            ],
        ),
        (
            "binance_depth",
            vec!["binance_depth_snapshot", "binance_depth_update"],
        ),
        ("binance_aggtrade", vec!["binance_aggtrade"]),
    ];

    assert_eq!(
        training_source_kinds(),
        SourceKind::ALL.map(SourceKind::as_str)
    );
    for (producer, expected_sources) in approved {
        assert_eq!(producer_writer_sources(producer).unwrap(), expected_sources);
        for source_kind in expected_sources {
            assert_eq!(
                logical_producer_for_source_kind(source_kind).unwrap(),
                producer
            );
        }
    }
    assert_eq!(LOGICAL_PRODUCERS.len(), 4);
    assert_eq!(expected_startup_participants().len(), 11);
}
