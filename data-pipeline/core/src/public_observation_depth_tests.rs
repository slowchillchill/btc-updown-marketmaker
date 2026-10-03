use super::*;

fn scenario() -> (PublicObservationCoreV1, Vec<Arc<ClosedBusinessGroupV1>>) {
    let mut core = core();
    push(&mut core, &meta(0));
    let mut other = metadata_snapshot_row();
    other.condition_id = format!("0x{}", "2".repeat(64));
    other.clob_market_id = "3195577".into();
    other.collector_admission_sequence = 2;
    let mut groups = push(
        &mut core,
        &record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(other))),
        ),
    )
    .closed_groups;
    let rows = [
        TypedRawRow::BinanceDepthSnapshot(snapshot("depth-a", 100, 1)),
        TypedRawRow::BinanceDepthUpdate(update(
            "depth-a",
            101,
            101,
            2,
            Some(100),
            None,
            BinanceDepthStitchVerdict::Applied,
            vec![level("64100", "0.2")],
        )),
        TypedRawRow::BinanceDepthUpdate(update(
            "depth-a",
            104,
            104,
            3,
            Some(100),
            Some(101),
            BinanceDepthStitchVerdict::NotAppliedConfirmedHole,
            vec![level("64100", "0.9")],
        )),
        TypedRawRow::BinanceDepthSnapshot(snapshot("depth-b", 200, 1)),
    ];
    for (i, row) in rows.into_iter().enumerate() {
        groups.extend(
            push(
                &mut core,
                &record(i as u64 + 2, TypedRawRowV2::Existing(row)),
            )
            .closed_groups,
        );
    }
    (core, groups)
}

#[test]
fn one_depth_owner_per_group_keeps_complete_values_quality_and_old_snapshots() {
    // scenario retains every group while later inputs advance the original core.
    let (core, groups) = scenario();
    assert_eq!(groups.len(), 5);
    // Four levels plus the original epoch bookkeeping; returned projections
    // are not another resident reducer quantity table.
    assert_eq!(core.object_counts().binance_depth_retained_entries, 7);
    let expected_quality = [
        MarketDataQuality::UnknownNotObserved,
        MarketDataQuality::Fresh,
        MarketDataQuality::Fresh,
        MarketDataQuality::ConfirmedGap,
        MarketDataQuality::Fresh,
    ];
    for (i, group) in groups.iter().enumerate() {
        assert_eq!(group.observations.len(), 2);
        let a = &group.observations[0].binance_depth;
        let b = &group.observations[1].binance_depth;
        assert!(Arc::ptr_eq(a, b));
        assert_eq!(Arc::strong_count(a), 2);
        assert_eq!(a.quality, expected_quality[i]);
        if i == 0 {
            assert!(a.value.is_none());
            assert!(a.direct_anchors.is_empty());
            assert!(a.dependency_frontier.is_none());
        } else {
            assert_eq!(a.direct_anchors.len(), 1);
            let anchor = &a.direct_anchors[0];
            assert_eq!(anchor.field_path, "binance_depth");
            assert_eq!(anchor.position.closed_at_input_position, i as u64 + 1);
            assert_eq!(anchor.position.fact_ordinal, 0);
            assert_eq!(anchor.group_id, group.group_id);
            assert_eq!(anchor.source_message_id, group.group_id);
            assert_eq!(group.records[0].source_refs[0].input_position, i as u64 + 1);
            assert_eq!(
                a.dependency_frontier.as_ref().unwrap().next_input_position,
                i as u64 + 2
            );
            let (source_time, received_time) = match i {
                1 | 4 => (None, 2001),
                2 => (Some(1502), 2002),
                3 => (Some(1503), 2003),
                _ => unreachable!(),
            };
            assert_eq!(anchor.source_event_at_utc_ms, source_time);
            assert_eq!(anchor.received_at_utc_ms, received_time);
        }
        for other in groups.iter().take(i) {
            assert!(!Arc::ptr_eq(a, &other.observations[0].binance_depth));
        }
    }
    let reference = |id, best_quantity: &str| BinanceDepthStateReferenceV1 {
        symbol: "BTCUSDT".into(),
        last_update_id: id,
        bids: vec![
            ReferencePriceLevelV1 {
                price_usdt_per_btc: ExactDecimal("64100".into()),
                quantity_btc: ExactDecimal(best_quantity.into()),
            },
            ReferencePriceLevelV1 {
                price_usdt_per_btc: ExactDecimal("64000".into()),
                quantity_btc: ExactDecimal("0.5".into()),
            },
        ],
        asks: vec![
            ReferencePriceLevelV1 {
                price_usdt_per_btc: ExactDecimal("64200".into()),
                quantity_btc: ExactDecimal("0.4".into()),
            },
            ReferencePriceLevelV1 {
                price_usdt_per_btc: ExactDecimal("64300".into()),
                quantity_btc: ExactDecimal("0.3".into()),
            },
        ],
    };
    assert_eq!(
        groups[1].observations[0].binance_depth.value,
        Some(reference(100, "0.7"))
    );
    assert_eq!(
        groups[2].observations[0].binance_depth.value,
        Some(reference(101, "0.2"))
    );
    assert!(groups[3].observations[0].binance_depth.value.is_none());
    assert_eq!(
        groups[4].observations[0].binance_depth.value,
        Some(reference(200, "0.7"))
    );
    let before = serde_json::to_value(&groups[1].observations).unwrap();
    drop(core);
    assert_eq!(
        serde_json::to_value(&groups[1].observations).unwrap(),
        before
    );
    assert_eq!(
        groups[1].observations[0].binance_depth.value,
        Some(reference(100, "0.7"))
    );
}

#[test]
fn complete_pre_arc_observation_json_remains_readable_and_identical() {
    // Generated through the original core at 73e1e44c, before either production
    // file was edited; all observation fields, not a selected summary, are kept.
    let old_json = include_str!("fixtures/binance-depth-pre-arc-observations.json");
    let old_value: serde_json::Value = serde_json::from_str(old_json).unwrap();
    let old_observations: Vec<Vec<MarketBusinessObservationV1>> =
        serde_json::from_str(old_json).unwrap();
    let (_, groups) = scenario();
    let observations: Vec<_> = groups.iter().map(|g| &g.observations).collect();
    assert_eq!(serde_json::to_value(&observations).unwrap(), old_value);
    assert_eq!(serde_json::to_value(&old_observations).unwrap(), old_value);
    for (old, new) in old_observations.iter().zip(&groups) {
        assert_eq!(old, &new.observations);
        // Reading old JSON need not reconstruct in-memory ownership sharing.
        assert!(!Arc::ptr_eq(&old[0].binance_depth, &old[1].binance_depth));
    }
}
