use super::*;

fn scenario() -> (PublicObservationCoreV1, Vec<Arc<ClosedBusinessGroupV1>>) {
    scenario_with_requirements(PublicObservationRequirementsV1::FULL)
}
fn scenario_with_requirements(
    requirements: PublicObservationRequirementsV1,
) -> (PublicObservationCoreV1, Vec<Arc<ClosedBusinessGroupV1>>) {
    let mut core = core();
    core.set_observation_requirements(requirements);
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
    for pair in groups.windows(2) {
        let (a, _) = pair[0].observations.shared_parts().unwrap();
        let (b, _) = pair[1].observations.shared_parts().unwrap();
        for id in a.keys() {
            assert!(Arc::ptr_eq(&a[id], &b[id]), "全局深度不重建市场事实");
        }
    }
    for (i, group) in groups.iter().enumerate() {
        assert_eq!(group.observations.len(), 2);
        let a = &group.observations[0].binance_depth;
        let b = &group.observations[1].binance_depth;
        assert!(Arc::ptr_eq(a, b));
        // 本组事实及显式完整读取可增加 Arc 持有者；ptr_eq 证明数量正文没有复制。
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
        Some(BinanceDepthObservationV1::Full(
            reference(100, "0.7").into()
        ))
    );
    assert_eq!(
        groups[2].observations[0].binance_depth.value,
        Some(BinanceDepthObservationV1::Full(
            reference(101, "0.2").into()
        ))
    );
    assert!(groups[3].observations[0].binance_depth.value.is_none());
    assert_eq!(
        groups[4].observations[0].binance_depth.value,
        Some(BinanceDepthObservationV1::Full(
            reference(200, "0.7").into()
        ))
    );
    let before = serde_json::to_value(&groups[1].observations).unwrap();
    drop(core);
    assert_eq!(
        serde_json::to_value(&groups[1].observations).unwrap(),
        before
    );
    assert_eq!(
        groups[1].observations[0].binance_depth.value,
        Some(BinanceDepthObservationV1::Full(
            reference(100, "0.7").into()
        ))
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
        assert_eq!(old.as_slice(), new.observations.as_slice());
        // Reading old JSON need not reconstruct in-memory ownership sharing.
        assert!(!Arc::ptr_eq(&old[0].binance_depth, &old[1].binance_depth));
    }
}

// 独立沿改动前完整公式投影，核对所有字段而非只看价格/指针。
fn original_projection(
    core: &PublicObservationCoreV1,
) -> ObservedFactV1<BinanceDepthObservationV1> {
    ObservedFactV1 {
        value: if core.global.depth.value.is_some() {
            core.binance_depth
                .current_depth_value()
                .map(|value| BinanceDepthObservationV1::Full(value.into()))
        } else {
            None
        },
        quality: core.global.depth.quality,
        direct_anchors: core.global.depth.direct_anchors.clone(),
        dependency_frontier: core.global.depth.dependency_frontier.clone(),
    }
}
fn assert_original_projection(core: &PublicObservationCoreV1, result: &AppliedIngressV1) {
    let expected = original_projection(core);
    for group in &result.closed_groups {
        for observation in &group.observations {
            assert_eq!(*observation.binance_depth, expected);
            assert_eq!(
                serde_json::to_value(&observation.binance_depth).unwrap(),
                serde_json::to_value(&expected).unwrap()
            );
        }
    }
}

#[test]
fn unchanged_depth_is_shared_without_refreshing_anchors_and_old_targets_are_released() {
    let mut core = core();
    push(&mut core, &meta(0));
    let first = push(
        &mut core,
        &record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot("a", 100, 1))),
        ),
    );
    assert_original_projection(&core, &first);
    let old = &first.closed_groups[0].observations[0];
    let weak = Arc::downgrade(&old.binance_depth);
    let old_json = serde_json::to_value(old).unwrap();
    let second = push(
        &mut core,
        &record(
            2,
            TypedRawRowV2::Existing(TypedRawRow::BinanceAggTrade(trade(1, 1, "64000"))),
        ),
    );
    let new = &second.closed_groups[0].observations[0];
    assert!(Arc::ptr_eq(&old.binance_depth, &new.binance_depth));
    assert_ne!(old.group_id, new.group_id);
    assert_ne!(old.visible_prefix, new.visible_prefix);
    assert_original_projection(&core, &second);
    // 相同接收毫秒、相同数量也要失效；更新ID、来源、依赖必须前移。
    let mut row = update(
        "a",
        101,
        101,
        2,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![level("64100", "0.7")],
    );
    row.timestamp_received = 2001;
    let third = push(
        &mut core,
        &record(
            3,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(row)),
        ),
    );
    assert_original_projection(&core, &third);
    let depth = &third.closed_groups[0].observations[0].binance_depth;
    assert!(!Arc::ptr_eq(&old.binance_depth, depth));
    assert_eq!(
        depth.value.as_ref().unwrap().full().unwrap().last_update_id,
        101
    );
    assert_ne!(old.binance_depth.direct_anchors, depth.direct_anchors);
    assert_ne!(
        old.binance_depth.dependency_frontier,
        depth.dependency_frontier
    );
    assert_eq!(serde_json::to_value(old).unwrap(), old_json);
    let value = depth.value.as_ref().unwrap().full().unwrap();
    let strings: usize = value
        .bids
        .iter()
        .chain(&value.asks)
        .map(|l| l.price_usdt_per_btc.0.capacity() + l.quantity_btc.0.capacity())
        .sum();
    println!(
        "representative_depth_capacity target_inline={} levels={} bids_capacity={} asks_capacity={} vec_allocated={} payload_string_capacity={}",
        std::mem::size_of_val(depth.as_ref()),
        value.bids.len() + value.asks.len(),
        value.bids.capacity(),
        value.asks.capacity(),
        (value.bids.capacity() + value.asks.capacity())
            * std::mem::size_of::<ReferencePriceLevelV1>(),
        strings + value.symbol.capacity()
    );
    let missing = push(
        &mut core,
        &record(
            4,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(update(
                "b",
                201,
                201,
                1,
                None,
                None,
                BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable,
                vec![],
            ))),
        ),
    );
    assert_original_projection(&core, &missing);
    assert_eq!(
        missing.closed_groups[0].observations[0]
            .binance_depth
            .quality,
        MarketDataQuality::Missing
    );
    assert!(
        missing.closed_groups[0].observations[0]
            .binance_depth
            .value
            .is_none()
    );
    drop(core);
    assert!(weak.upgrade().is_some());
    drop(first);
    assert!(weak.upgrade().is_some());
    drop(second);
    assert!(weak.upgrade().is_none());
}

#[test]
fn no_output_gap_epoch_recovery_and_failed_compute_invalidate_before_reuse() {
    let mut core = core();
    push(&mut core, &meta(0));
    let first = push(
        &mut core,
        &record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot("a", 100, 1))),
        ),
    );
    let old = &first.closed_groups[0].observations[0].binance_depth;
    let identical = push(
        &mut core,
        &record(
            2,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot("a", 100, 2))),
        ),
    );
    assert!(identical.closed_groups.is_empty());
    assert!(core.current_depth_projection.is_none());
    let trade_result = push(
        &mut core,
        &record(
            3,
            TypedRawRowV2::Existing(TypedRawRow::BinanceAggTrade(trade(1, 1, "64000"))),
        ),
    );
    let rebuilt = &trade_result.closed_groups[0].observations[0].binance_depth;
    assert!(!Arc::ptr_eq(old, rebuilt));
    assert_eq!(old, rebuilt);
    let rows = [
        update(
            "a",
            100,
            100,
            3,
            Some(100),
            None,
            BinanceDepthStitchVerdict::NotAppliedStaleDuplicate,
            vec![],
        ),
        update(
            "a",
            102,
            102,
            4,
            Some(100),
            None,
            BinanceDepthStitchVerdict::NotAppliedConfirmedHole,
            vec![],
        ),
        update(
            "b",
            201,
            201,
            1,
            None,
            None,
            BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable,
            vec![],
        ),
        update(
            "b",
            202,
            202,
            2,
            None,
            None,
            BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable,
            vec![],
        ),
    ];
    for (i, row) in rows.into_iter().enumerate() {
        let result = push(
            &mut core,
            &record(
                4 + i as u64,
                TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(row)),
            ),
        );
        assert_original_projection(&core, &result);
        if result.closed_groups.is_empty() {
            assert!(core.current_depth_projection.is_none());
        }
    }
    let recovered = push(
        &mut core,
        &record(
            8,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot("b", 200, 3))),
        ),
    );
    assert_original_projection(&core, &recovered);
    assert_eq!(
        recovered.closed_groups[0].observations[0]
            .binance_depth
            .quality,
        MarketDataQuality::Fresh
    );
    // 换代发生在错误前；不能沿用旧投影，不能假装归约器已回滚。
    let invalid = record(
        9,
        TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(update(
            "c",
            301,
            301,
            1,
            None,
            None,
            BinanceDepthStitchVerdict::Applied,
            vec![],
        ))),
    );
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_v1(&core, &invalid, &ids, &[]).unwrap();
    let error = core.push(&prepared).err().unwrap();
    assert!(
        matches!(error, DptCoreErrorV1::Contract(ContractError::InvalidField { field, .. }) if field == "binance_depth_update.stitch_verdict")
    );
    assert!(core.current_depth_projection.is_none());
    let after_error = push(
        &mut core,
        &record(
            9,
            TypedRawRowV2::Existing(TypedRawRow::BinanceAggTrade(trade(2, 2, "64001"))),
        ),
    );
    assert_original_projection(&core, &after_error);
    assert!(
        after_error.closed_groups[0].observations[0]
            .binance_depth
            .value
            .is_none()
    );
}

#[test]
fn opted_source_progress_tracks_same_value_and_requires_snapshot_after_generation_change() {
    let mut core = core();
    core.enable_source_progress();
    push(&mut core, &meta(0));
    let first = push(
        &mut core,
        &record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot(
                "same-epoch",
                100,
                1,
            ))),
        ),
    );
    let anchor = first.closed_groups[0].observations[0]
        .binance_depth
        .direct_anchors
        .clone();
    let same = push(
        &mut core,
        &record(
            2,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot(
                "same-epoch",
                100,
                2,
            ))),
        ),
    );
    assert!(
        same.closed_groups[0].records.is_empty(),
        "仅流进展，旧字段事实不改"
    );
    let observation = &same.closed_groups[0].observations[0];
    assert_eq!(observation.binance_depth.direct_anchors, anchor);
    let progress = &observation.source_progress[0];
    assert_eq!(
        progress
            .last_valid_business_message
            .as_ref()
            .unwrap()
            .received_at_utc_ms,
        2002
    );
    assert_eq!(progress.connection_generation, Some(1));

    let mut reconnect = update(
        "same-epoch",
        101,
        101,
        3,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![],
    );
    reconnect.connection_generation = 2;
    let switched = push(
        &mut core,
        &record(
            3,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(reconnect)),
        ),
    );
    let changed = &switched.closed_groups[0].observations[0].source_progress[0];
    assert_eq!(changed.connection_generation, Some(2));
    assert_eq!(
        changed.synchronization,
        SourceSynchronizationV1::SnapshotRequired
    );
    assert_eq!(
        switched.closed_groups[0].observations[0]
            .binance_depth
            .quality,
        MarketDataQuality::Fresh,
        "健康投影不反写旧质量"
    );

    let duplicate = update(
        "same-epoch",
        101,
        101,
        4,
        Some(100),
        Some(101),
        BinanceDepthStitchVerdict::NotAppliedStaleDuplicate,
        vec![],
    );
    // 真实代次必须一致，不能凭重复消息伪造新连接。
    let mut duplicate = duplicate;
    duplicate.connection_generation = 2;
    let replayed = push(
        &mut core,
        &record(
            4,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(duplicate)),
        ),
    );
    assert!(replayed.closed_groups.is_empty());
    let mut rebuilt = snapshot("same-epoch", 102, 5);
    rebuilt.connection_generation = 2;
    let rebuilt = push(
        &mut core,
        &record(
            5,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(rebuilt)),
        ),
    );
    let restored = &rebuilt.closed_groups[0].observations[0].source_progress[0];
    assert!(matches!(
        restored.synchronization,
        SourceSynchronizationV1::SnapshotObserved { .. }
    ));
    assert_eq!(
        restored
            .last_valid_business_message
            .as_ref()
            .unwrap()
            .received_at_utc_ms,
        2005
    );
}

#[test]
fn source_progress_disabled_keeps_identical_confirmation_without_group() {
    let mut core = core();
    push(&mut core, &meta(0));
    let first = push(
        &mut core,
        &record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot(
                "same-epoch",
                100,
                1,
            ))),
        ),
    );
    assert!(
        first.closed_groups[0].observations[0]
            .source_progress
            .is_empty()
    );
    let same = push(
        &mut core,
        &record(
            2,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot(
                "same-epoch",
                100,
                2,
            ))),
        ),
    );
    assert!(same.closed_groups.is_empty());
}

#[test]
fn quantity_free_depth_keeps_original_unknown_quality_anchors_and_prefix() {
    let (full_core, full) = scenario();
    let (compact_core, compact) = scenario_with_requirements(PublicObservationRequirementsV1 {
        binance_depth_quantities: false,
    });
    assert_eq!(full_core.object_counts(), compact_core.object_counts());
    for (a, b) in full.iter().zip(&compact) {
        assert_eq!(a.records, b.records);
        assert_eq!(a.group_id, b.group_id);
        assert_eq!(a.group_position, b.group_position);
        for (a, b) in a.observations.iter().zip(&b.observations) {
            assert_eq!(a.visible_prefix, b.visible_prefix);
            assert_eq!(a.assets, b.assets);
            assert_eq!(a.binance_depth.quality, b.binance_depth.quality);
            assert_eq!(
                a.binance_depth.direct_anchors,
                b.binance_depth.direct_anchors
            );
            assert_eq!(
                a.binance_depth.dependency_frontier,
                b.binance_depth.dependency_frontier
            );
            assert_eq!(
                a.binance_depth.value.is_some(),
                b.binance_depth.value.is_some()
            );
            if b.binance_depth.value.is_some() {
                assert_eq!(
                    b.binance_depth.value,
                    Some(BinanceDepthObservationV1::NotMaterialized)
                );
            }
        }
    }
    let mut bytes = Vec::new();
    compact_core.write_checkpoint(&mut bytes).unwrap();
    let restored = PublicObservationCoreV1::read_checkpoint(bytes.as_slice()).unwrap();
    assert!(!restored.observation_requirements().binance_depth_quantities);
}

#[test]
fn day_depth_positions_keep_canonical_times_quality_health_and_frozen_quantities() {
    let mut full = core();
    let mut day = core();
    let mut no_quantities = core();
    for core in [&mut full, &mut day, &mut no_quantities] {
        core.enable_source_progress();
        push(core, &meta(0));
    }
    no_quantities.set_observation_requirements(PublicObservationRequirementsV1 {
        binance_depth_quantities: false,
    });
    let applied = |seq, id, before, source, bids| {
        let mut row = update(
            "a",
            id,
            id,
            seq,
            Some(100),
            before,
            BinanceDepthStitchVerdict::Applied,
            bids,
        );
        row.event_time = source;
        if seq == 7 {
            row.timestamp_received = 1900;
        }
        TypedRawRow::BinanceDepthUpdate(row)
    };
    let mut reset = update(
        "b",
        201,
        201,
        3,
        None,
        None,
        BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable,
        vec![],
    );
    reset.timestamp_received += 86_400_000;
    reset.collector_admission_time_utc_ms = reset.timestamp_received + 100;
    let rows = vec![
        TypedRawRow::BinanceDepthSnapshot(snapshot("a", 100, 1)),
        TypedRawRow::BinanceDepthSnapshot(snapshot("a", 100, 2)),
        TypedRawRow::BinanceDepthUpdate(update(
            "a",
            100,
            100,
            3,
            Some(100),
            None,
            BinanceDepthStitchVerdict::NotAppliedStaleDuplicate,
            vec![],
        )),
        applied(4, 101, None, 1800, vec![level("64100", "0.2")]),
        applied(5, 102, Some(101), 1700, vec![level("64100", "0")]),
        applied(6, 103, Some(102), 1500, vec![level("64100", "0")]),
        // A high source time held only by the old anchor must disappear on the next empty update.
        applied(7, 104, Some(103), 1900, vec![]),
        applied(8, 105, Some(104), 1400, vec![]),
        applied(9, 106, Some(105), 1600, vec![level("64100", "0.3")]),
        applied(10, 107, Some(106), 1300, vec![]),
        TypedRawRow::BinanceDepthUpdate(update(
            "a",
            110,
            110,
            11,
            Some(100),
            Some(107),
            BinanceDepthStitchVerdict::NotAppliedConfirmedHole,
            vec![],
        )),
        TypedRawRow::BinanceDepthUpdate(update(
            "a",
            111,
            111,
            12,
            None,
            None,
            BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable,
            vec![],
        )),
        TypedRawRow::BinanceDepthSnapshot(snapshot("b", 200, 2)),
        TypedRawRow::BinanceDepthUpdate(reset),
        TypedRawRow::BinanceDepthSnapshot(snapshot("b", 210, 4)),
        TypedRawRow::BinanceDepthUpdate(update(
            "c",
            220,
            220,
            1,
            None,
            None,
            BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable,
            vec![],
        )),
    ];
    let expected_sources = [
        None,
        None,
        None,
        Some(1800),
        Some(1800),
        Some(1700),
        Some(1900),
        Some(1500),
        Some(1600),
        Some(1600),
    ];
    let mut frozen = None;
    for (i, row) in rows.into_iter().enumerate() {
        let input = record(i as u64 + 1, TypedRawRowV2::Existing(row));
        let expected = push(&mut full, &input);
        let run_day = |core: &mut PublicObservationCoreV1| {
            let ids = BTreeMap::new();
            let prepared = prepare_public_ingress_v1(core, &input, &ids, &[]).unwrap();
            // Fails if day production reaches the old full identity encoding path.
            crate::without_identity_encoding(|| core.push_day(&prepared))
                .unwrap()
                .applied
        };
        let actual = run_day(&mut day);
        let compact = run_day(&mut no_quantities);
        for (quantities, result) in [(true, &actual), (false, &compact)] {
            assert_eq!(result.applied_prefix, expected.applied_prefix);
            assert_eq!(result.closed_groups.len(), expected.closed_groups.len());
            for (old, new) in expected.closed_groups.iter().zip(&result.closed_groups) {
                assert_eq!(old.group_id, new.group_id);
                assert_eq!(old.group_position, new.group_position);
                assert_eq!(old.member_input_positions, new.member_input_positions);
                assert_eq!(old.records.len(), new.records.len());
                for (old, new) in old.records.iter().zip(new.records.iter()) {
                    assert_eq!(old.position, new.position);
                    assert_eq!(old.source_refs, new.source_refs);
                    assert_eq!(old.direct_sources, new.direct_sources);
                    assert_eq!(old.direct_time, new.direct_time);
                    assert_eq!(old.dependency_time, new.dependency_time);
                    assert_eq!(old.quality, new.quality);
                    assert!(matches!(
                        new.lineage,
                        PublicBusinessLineageV1::StateReference { .. }
                    ));
                    if i < expected_sources.len() {
                        assert_eq!(
                            new.dependency_time.source_event_at_utc_ms,
                            expected_sources[i]
                        );
                    }
                }
                for (old, new) in old.observations.iter().zip(&new.observations) {
                    let mut expected = old.clone();
                    if !quantities && expected.binance_depth.value.is_some() {
                        let mut value = (*expected.binance_depth).clone();
                        value.value = Some(BinanceDepthObservationV1::NotMaterialized);
                        expected.binance_depth = Arc::new(value);
                    }
                    expected.group_events = new.group_events.clone();
                    assert_eq!(
                        expected, *new,
                        "including confirmation/stale/reset health and depth"
                    );
                }
            }
        }
        if i == 3 {
            frozen = actual
                .closed_groups
                .first()
                .map(|g| Arc::clone(&g.observations[0].binance_depth));
        }
        if i == 4 {
            // Restore after a deletion, including its index entry. The next source maximum may fall.
            for core in [&mut day, &mut no_quantities] {
                let mut bytes = Vec::new();
                core.write_checkpoint(&mut bytes).unwrap();
                *core = PublicObservationCoreV1::read_checkpoint(bytes.as_slice()).unwrap();
            }
        }
    }
    let old = frozen.unwrap();
    assert_eq!(
        old.value.as_ref().unwrap().full().unwrap().bids[0].quantity_btc,
        ExactDecimal("0.2".into())
    );
}
