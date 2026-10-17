use super::*;
use crate::test_common as common;
use data_pipeline_core::{
    public_input::*, public_observation_core::*,
    public_reference::source_evidence_v3_from_typed_raw_v2,
};
use std::{cell::Cell, collections::BTreeSet};
fn reference(a: &AppliedIngressV1, g: &ClosedBusinessGroupV1) -> PublicGroupReferenceV1 {
    PublicGroupReferenceV1 {
        input_stream_id: a.applied_prefix.input_stream_id.clone(),
        applied_prefix: a.applied_prefix.clone(),
        group_position: g.group_position,
        group_id: g.group_id.clone(),
        closed_at_input_position: g.closed_at_input_position,
    }
}
fn target(g: PublicGroupReferenceV1, id: &str) -> HistoricalObservationTargetV1 {
    read_observation_targets_v1(std::io::Cursor::new(serde_json::json!({"account":"test","request_id":id,"condition_id":common::FIRST,"cutoff":{"receipt":{"group":g}},"source":{"line_1based":1},"submission_items":[]}).to_string())).unwrap().remove(0)
}
fn small_inputs(config: &HistoricalPreparationConfigV1) -> Vec<RecordedIngressV1> {
    let inputs =
        data_pipeline_training::freeze_public_source_input_set_v1(&config.scope.manifest_paths)
            .unwrap();
    let admitted = inputs.admitted_daily_inputs();
    let files = admitted.iter().flat_map(|d| d.files()).collect::<Vec<_>>();
    let ingress = DayIngressV1::open(
        &files,
        config.header.input_stream_id.clone(),
        config.read_page_rows,
        config.max_read_batch_bytes,
        config.scope.received_start_utc_ms..config.scope.received_end_utc_ms,
        vec![],
        config.canonical.max_epoch_count_per_source,
    )
    .unwrap();
    let mut p = DayProducerV1::new(
        ingress,
        config.effective_header(),
        Sha256Hex(inputs.input_set_identity_sha256().into()),
        config.hidden_stream_id.clone(),
    )
    .unwrap();
    let mut result = vec![];
    while let Some(a) = p.next_applied().unwrap() {
        result.push(a.input)
    }
    result
}
fn apply(core: &mut PublicObservationCoreV1, input: &RecordedIngressV1) -> AppliedIngressV1 {
    let prepared = prepare_public_ingress_v1(core, input, &Default::default(), &[]).unwrap();
    core.push(&prepared).unwrap().applied
}
#[test]
fn exact_same_millisecond_groups_keep_requests_and_do_not_cross_streams() {
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = common::setup(temp.path());
    let inputs = small_inputs(&config.preparation);
    let mut full = PublicObservationCoreV1::new(config.preparation.effective_header()).unwrap();
    full.enable_source_progress();
    // 原人工策略启用来源进展；原权重为零时仅省略数量正文。
    let mut original = PublicObservationCoreV1::new(config.preparation.effective_header()).unwrap();
    original.enable_source_progress();
    original.set_observation_requirements(PublicObservationRequirementsV1 {
        binance_depth_quantities: false,
    });
    let mut selected = vec![];
    for input in &inputs {
        let a = apply(&mut full, input);
        let original_applied = apply(&mut original, input);
        assert_eq!(a.applied_prefix, original_applied.applied_prefix);
        assert_eq!(
            a.closed_groups
                .iter()
                .map(|g| reference(&a, g))
                .collect::<Vec<_>>(),
            original_applied
                .closed_groups
                .iter()
                .map(|g| reference(&original_applied, g))
                .collect::<Vec<_>>()
        );
        for group in &a.closed_groups {
            if let Some(o) = group.observations.get_market(common::FIRST)
                && o.assets
                    .iter()
                    .any(|asset| asset.asks.value.as_ref().is_some_and(|v| !v.is_empty()))
            {
                selected.push((a.clone(), group.clone()));
            }
        }
    }
    let (early, group) = &selected[0];
    let g = reference(early, group);
    let mut targets = vec![target(g.clone(), "one"), target(g.clone(), "two")];
    let mut wrong = target(g.clone(), "other-stream");
    wrong.group.as_mut().unwrap().input_stream_id = "different".into();
    targets.push(wrong);
    let rows = rows_at_applied(early, &targets, &BTreeSet::from([0, 1, 2]));
    assert_eq!(rows.len(), 2);
    assert_ne!(
        rows[0].1.target["request_id"],
        rows[1].1.target["request_id"]
    );
    let early_o = rows[0].1.observation.as_ref().unwrap();
    assert_eq!(
        early_o
            .assets
            .iter()
            .filter(|a| a.asks.value.as_ref().is_some_and(|v| !v.is_empty()))
            .count(),
        1
    );
    let later = &selected[1];
    assert_ne!(reference(&later.0, &later.1), g);
    // 两条原始 book 具有同一接收毫秒；第二组新增另一腿，不能回填第一组。
    let first_time = group
        .records
        .iter()
        .flat_map(|f| &f.direct_sources)
        .map(|s| s.received_at_utc_ms)
        .max()
        .unwrap();
    let second_time = later
        .1
        .records
        .iter()
        .flat_map(|f| &f.direct_sources)
        .map(|s| s.received_at_utc_ms)
        .max()
        .unwrap();
    assert_eq!(first_time, second_time);
    assert!(rows_at_applied(&later.0, &targets, &BTreeSet::from([0, 1, 2])).is_empty());
    let mut exported = vec![];
    let state = export_historical_observations_v1(
        &config.preparation,
        &targets,
        |r| {
            exported.push(r.clone());
            Ok(())
        },
        |_| {},
        || false,
    )
    .unwrap();
    assert_eq!(state.completed_targets, 3);
    assert_eq!(exported[0].observation.as_ref().unwrap(), early_o);
    assert_eq!(
        exported[2].unknown_reason.as_deref(),
        Some("input_stream_mismatch")
    );
}
#[test]
fn later_official_anchor_and_revision_do_not_fill_earlier_target() {
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = common::setup(temp.path());
    let mut inputs = small_inputs(&config.preparation);
    inputs.retain(|i| !matches!(i.ingress, DptIngressV1::EndOfInput));
    let mut core = PublicObservationCoreV1::new(config.preparation.effective_header()).unwrap();
    let mut early = None;
    for input in &inputs {
        let a = apply(&mut core, input);
        for g in &a.closed_groups {
            if g.observations.get_market(common::FIRST).is_some() {
                early = Some((a.clone(), g.clone()));
            }
        }
    }
    let (a, g) = early.unwrap();
    let t = target(reference(&a, &g), "before-anchor");
    let before = rows_at_applied(&a, std::slice::from_ref(&t), &BTreeSet::from([0]))
        .remove(0)
        .1;
    assert!(
        before
            .observation
            .as_ref()
            .unwrap()
            .official_anchor
            .value
            .is_none()
    );
    for (n, price) in [(0, "64950"), (1, "64960")] {
        let pos = inputs.len() as u64 + n;
        let at = common::BASE + 11000 + n as i64;
        let row:MarketAnchorRuleUpdateInput=serde_json::from_value(serde_json::json!({"schema_version":"polymarket_market_metadata_v6","timestamp_received":at,"collector_admission_sequence":pos+1,"collector_admission_time_utc_ms":at+1,"metadata_origin":"gamma_event_by_slug","collector_host_id":"synthetic-collector","collector_region":"test","slug":"btc-updown-5m-engineering","condition_id":common::FIRST,"clob_market_id":"3195576","asset_ids":"[\"20\",\"3\"]","market_start_utc":common::BASE+1000,"market_end_utc":common::BASE+10000,"fixed_identity_verified_at_utc":common::BASE-1,"market_lifecycle_state":"active","market_runtime_role":"current","metadata_fetch_start_utc":at-10,"anchor_query_source_epoch_id":"anchor-test","anchor_query_ordinal":n+1,"anchor_query_result":"observed","price_to_beat_decimal":price,"price_to_beat_source_field":"eventMetadata.priceToBeat","twap_enabled":true,"twap_lookback_seconds":30,"http_received_at_utc":at,"http_request_url":"https://gamma-api.polymarket.com/events/slug/btc-updown-5m-engineering","http_body_sha256":"a".repeat(64)})).unwrap();
        let row = TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
            PolymarketMarketMetadataV6Input::AnchorRule(row),
        ));
        let input = RecordedIngressV1 {
            input_stream_id: config.preparation.header.input_stream_id.clone(),
            input_position: pos,
            ingress: DptIngressV1::Message {
                source_evidence: source_evidence_v3_from_typed_raw_v2(&row).unwrap(),
                row,
            },
            dependency_record_refs: vec![],
        };
        let applied = apply(&mut core, &input);
        let observation = applied
            .closed_groups
            .last()
            .unwrap()
            .observations
            .get_market(common::FIRST)
            .unwrap();
        assert_eq!(
            observation
                .official_anchor
                .value
                .as_ref()
                .unwrap()
                .price_usd_per_btc
                .0,
            price
        );
        assert!(
            rows_at_applied(&applied, std::slice::from_ref(&t), &BTreeSet::from([0])).is_empty()
        );
        assert!(
            before
                .observation
                .as_ref()
                .unwrap()
                .official_anchor
                .value
                .is_none()
        );
    }
}
#[test]
fn stop_and_sink_failure_preserve_only_successful_targets() {
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = common::setup(temp.path());
    let inputs = small_inputs(&config.preparation);
    let mut core = PublicObservationCoreV1::new(config.preparation.effective_header()).unwrap();
    let a = apply(&mut core, &inputs[0]);
    let t = target(reference(&a, &a.closed_groups[0]), "first");
    let mut wrong = t.clone();
    wrong.group.as_mut().unwrap().group_id = Sha256Hex("f".repeat(64));
    let stopped = Cell::new(false);
    let state = export_historical_observations_v1(
        &config.preparation,
        &[t.clone(), wrong.clone()],
        |_| {
            stopped.set(true);
            Ok(())
        },
        |_| {},
        || stopped.get(),
    )
    .unwrap();
    assert!(state.stopped);
    assert_eq!(state.completed_targets, 1);
    let last = Cell::new(0);
    let failure = export_historical_observations_v1(
        &config.preparation,
        &[t, wrong],
        |_| Err(std::io::Error::other("fixture write failure").into()),
        |p| last.set(p.completed_targets),
        || false,
    );
    assert!(failure.is_err());
    assert_eq!(last.get(), 0);
    let mut missing = read_observation_targets_v1(std::io::Cursor::new(
        serde_json::json!({"condition_id":common::FIRST,"cutoff":null}).to_string(),
    ))
    .unwrap();
    let mut wrong = target(reference(&a, &a.closed_groups[0]), "bad-prefix");
    wrong
        .group
        .as_mut()
        .unwrap()
        .applied_prefix
        .next_input_position += 100;
    missing.push(wrong);
    let mut reasons = vec![];
    export_historical_observations_v1(
        &config.preparation,
        &missing,
        |row| {
            reasons.push(row.unknown_reason.clone());
            Ok(())
        },
        |_| {},
        || false,
    )
    .unwrap();
    assert_eq!(
        reasons,
        vec![
            Some("target_group_missing".into()),
            Some("exact_group_prefix_not_found".into())
        ]
    );
}
#[test]
fn missing_group_and_bad_json_have_specific_boundaries() {
    assert!(read_observation_targets_v1(std::io::Cursor::new("{bad\n")).is_err());
    let target = read_observation_targets_v1(std::io::Cursor::new(
        "{\"condition_id\":\"condition\",\"cutoff\":null}\n",
    ))
    .unwrap();
    assert!(target[0].group.is_none());
}

#[test]
fn source_progress_enabled_repeated_snapshot_preserves_full_and_original_target_groups() {
    use data_pipeline_core::typed_raw::{BinanceDepthSnapshotRow, DepthLevel, TypedRawRow};
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = common::setup(temp.path());
    let metadata = small_inputs(&config.preparation).remove(0);
    let snapshot = |position: u64, update_id: u64, quantity: &str| {
        let level = |price: &str, quantity: &str| DepthLevel {
            price: price.into(),
            quantity: quantity.into(),
        };
        let row =
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(BinanceDepthSnapshotRow {
                timestamp_received: common::BASE + 500 + position as i64,
                snapshot_fetch_start_utc: common::BASE + 400,
                last_update_id: update_id,
                bids: vec![level("64000", "0.5"), level("64100", quantity)],
                asks: vec![level("64300", "0.3"), level("64200", "0.4")],
                symbol: "BTCUSDT".into(),
                source_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
                    .into(),
                snapshot_status: "complete".into(),
                source_sequence_epoch_id: "same-epoch".into(),
                connection_generation: 1,
                depth_ingress_sequence: position,
                collector_admission_sequence: position,
                collector_admission_time_utc_ms: common::BASE + 600 + position as i64,
                collector_host_id: "synthetic-collector".into(),
                collector_region: "test".into(),
                schema_version: "binance_depth_snapshot_v3".into(),
            }));
        RecordedIngressV1 {
            input_stream_id: config.preparation.header.input_stream_id.clone(),
            input_position: position,
            ingress: DptIngressV1::Message {
                source_evidence: source_evidence_v3_from_typed_raw_v2(&row).unwrap(),
                row,
            },
            dependency_record_refs: vec![],
        }
    };
    let mut full = PublicObservationCoreV1::new(config.preparation.effective_header()).unwrap();
    full.enable_source_progress();
    let mut original = PublicObservationCoreV1::new(config.preparation.effective_header()).unwrap();
    original.enable_source_progress();
    original.set_observation_requirements(PublicObservationRequirementsV1 {
        binance_depth_quantities: false,
    });
    let mut saved = vec![];
    for input in [
        metadata,
        snapshot(1, 100, "0.7"),
        snapshot(2, 100, "0.7"),
        snapshot(3, 101, "0.8"),
    ] {
        let full_applied = apply(&mut full, &input);
        let original_applied = apply(&mut original, &input);
        assert_eq!(full_applied.applied_prefix, original_applied.applied_prefix);
        assert_eq!(
            full_applied
                .closed_groups
                .iter()
                .map(|g| reference(&full_applied, g))
                .collect::<Vec<_>>(),
            original_applied
                .closed_groups
                .iter()
                .map(|g| reference(&original_applied, g))
                .collect::<Vec<_>>()
        );
        assert_eq!(full_applied.closed_groups.len(), 1);
        saved.push((full_applied, original_applied));
    }
    // 同值快照仍产生来源进展组，不能把它当作无组输入跳过。
    let (repeated, old_repeated) = &saved[2];
    assert!(repeated.closed_groups[0].records.is_empty());
    let requested = reference(old_repeated, &old_repeated.closed_groups[0]);
    let targets = [target(requested.clone(), "same-snapshot-confirmation")];
    let rows = rows_at_applied(repeated, &targets, &BTreeSet::from([0]));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.matched_group.as_ref(), Some(&requested));
    let visible = rows[0].1.observation.as_ref().unwrap();
    let old_visible = old_repeated.closed_groups[0]
        .observations
        .get_market(common::FIRST)
        .unwrap();
    assert_eq!(visible.visible_prefix, old_visible.visible_prefix);
    assert_eq!(
        visible.binance_depth.direct_anchors,
        old_visible.binance_depth.direct_anchors
    );
    assert_eq!(visible.source_progress, old_visible.source_progress);
    assert!(matches!(
        old_visible.binance_depth.value,
        Some(BinanceDepthObservationV1::NotMaterialized)
    ));
    let depth = visible
        .binance_depth
        .value
        .as_ref()
        .unwrap()
        .full()
        .unwrap();
    assert_eq!(depth.last_update_id, 100);
    assert_eq!(
        depth
            .bids
            .iter()
            .find(|l| l.price_usdt_per_btc.0 == "64100")
            .unwrap()
            .quantity_btc
            .0,
        "0.7"
    );
    let progress = visible
        .source_progress
        .iter()
        .find(|p| p.scope.source == SourceStreamKindV1::BinanceDepth)
        .unwrap();
    assert_eq!(
        progress
            .last_valid_business_message
            .as_ref()
            .unwrap()
            .received_at_utc_ms,
        common::BASE + 502
    );
    assert!(rows_at_applied(&saved[1].0, &targets, &BTreeSet::from([0])).is_empty());
    assert!(rows_at_applied(&saved[3].0, &targets, &BTreeSet::from([0])).is_empty());
    eprintln!(
        "启用来源进展，同值快照组精确命中：{}；FULL与原数量省略模式完整引用一致",
        serde_json::to_string(&requested).unwrap()
    );
}
