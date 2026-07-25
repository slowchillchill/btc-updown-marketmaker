use super::*;

#[test]
fn mandatory_producer_source_mapping_is_exact_and_bidirectional() {
    let approved = [
        (
            "polymarket",
            &["polymarket_market", "polymarket_market_metadata"][..],
        ),
        (
            "polymarket_rtds",
            &["polymarket_rtds_chainlink_btc_usd"][..],
        ),
        (
            "binance_depth",
            &["binance_depth_snapshot", "binance_depth_update"][..],
        ),
        ("binance_aggtrade", &["binance_aggtrade"][..]),
    ];

    validate_mandatory_producer_source_mapping().unwrap();
    let mut mapped_sources = BTreeSet::new();
    for (producer, expected_sources) in approved {
        assert_eq!(producer_writer_sources(producer).unwrap(), expected_sources);
        for source_kind in expected_sources {
            assert!(mapped_sources.insert(*source_kind));
            assert_eq!(
                logical_producer_for_source_kind(source_kind).unwrap(),
                producer
            );
        }
    }
    assert_eq!(
        mapped_sources,
        LIVE_SOURCES.iter().copied().collect::<BTreeSet<_>>()
    );
    assert_eq!(
        expected_startup_participants(),
        LOGICAL_PRODUCERS
            .iter()
            .map(|producer| StartupParticipant::producer(producer))
            .chain(
                LIVE_SOURCES
                    .iter()
                    .map(|source_kind| StartupParticipant::writer(source_kind))
            )
            .collect()
    );
}
