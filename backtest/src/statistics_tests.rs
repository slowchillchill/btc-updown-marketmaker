use super::*;
use crate::test_common as common;
use crate::*;
use data_pipeline_core::public_reference::source_evidence_v3_from_typed_raw_v2;
use data_pipeline_core::{
    public_input::*, public_observation_core::PublicObservationCoreV1, typed_raw::*,
};
use data_pipeline_training::public_replay::PublicReplayV1;
use sim_system::public_consumption::{PublicConsumptionTimeSourceV1, RecordedPublicGroupSourceV1};
use std::{
    io::Write,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct OriginalVisibility(Option<i64>);
impl PublicConsumptionTimeSourceV1 for OriginalVisibility {
    fn execution_time_for_group(
        &mut self,
        _: &PublicGroupReferenceV1,
    ) -> Result<i64, sim_system::execution::SimRunActorErrorV2> {
        panic!("来源必须提供可见时间")
    }
    fn execution_time_for_visible_group(
        &mut self,
        _: &PublicGroupReferenceV1,
        visible: i64,
    ) -> Result<i64, sim_system::execution::SimRunActorErrorV2> {
        let at = self.0.map_or(visible, |last| last.max(visible));
        self.0 = Some(at);
        Ok(at)
    }
}
fn replay_statistics(run: &PreparedHistoricalRunV1) -> Value {
    let source = RecordedPublicGroupSourceV1::open(
        &run.public_root,
        1,
        1 << 20,
        OriginalVisibility::default(),
    )
    .unwrap();
    let mut collector = HistoricalStatisticsCollectorV1::default();
    for group in source {
        let delivered = group.unwrap();
        collector.observe_at(
            &delivered.prepared.group,
            delivered.delivered_at_utc_ms,
            &run.initialization,
        );
    }
    serde_json::to_value(collector.finish()).unwrap()
}
#[test]
fn actual_prepare_statistics_equal_independent_recorded_replay_with_and_without_initialization() {
    for initial in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (mut config, _) = common::setup(temp.path());
        if initial {
            common::initialize_at(&mut config, common::BASE + 1000, &[common::FIRST]);
        }
        let result = prepare_historical_run_with_statistics_v1(&config.preparation, None).unwrap();
        let stats = serde_json::to_value(result.statistics).unwrap();
        assert_eq!(stats, replay_statistics(&result.prepared));
        assert_eq!(stats["public_groups"], if initial { 7 } else { 8 });
        assert_eq!(
            stats["initialization_public_groups"],
            if initial { 1 } else { 0 }
        );
        assert_eq!(stats["markets"].as_object().unwrap().len(), 2);
        let first = &stats["markets"][common::FIRST];
        assert_eq!(
            first["fields"]["chainlink_spot"]["denominator"],
            first["observations"]
        );
        assert_eq!(
            first["fields"]["chainlink_spot"]["categories"]["absent:UnknownNotObserved"],
            first["observations"]
        );
        if initial {
            assert!(
                stats["research_groups_with_initialization_dependencies"]
                    .as_u64()
                    .unwrap()
                    > 0
            );
            assert_eq!(
                stats["initialization_markets"][common::FIRST]["observations"],
                1
            );
        }
        // 真实首行保留collector admission下界，首见为接收后的BASE+1。
        else {
            assert_eq!(first["first_visible_at"], common::BASE + 1);
        }
    }
}
#[test]
fn initialization_only_and_empty_research_keep_end_counts_and_observed_catalog() {
    for initial in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (mut config, _) = common::setup(temp.path());
        common::initialize_at(&mut config, common::BASE + 1000, &[common::FIRST]);
        config.preparation.scope.received_end_utc_ms = common::BASE + 1001;
        config
            .preparation
            .metadata_initialization
            .as_mut()
            .unwrap()
            .received_end_utc_ms = common::BASE + 1001;
        if !initial {
            config.preparation.metadata_initialization = None;
        }
        let result = prepare_historical_run_with_statistics_v1(&config.preparation, None).unwrap();
        let stats = serde_json::to_value(result.statistics).unwrap();
        assert_eq!(stats, replay_statistics(&result.prepared));
        assert_eq!(stats["public_groups"], 0);
        assert!(stats["markets"].as_object().unwrap().is_empty());
        assert_eq!(
            stats["initialization_public_groups"],
            if initial { 1 } else { 0 }
        );
        assert_eq!(result.prepared.markets.len(), usize::from(initial));
        assert!(result.prepared.hidden.prefix.normal_end);
    }
}
#[test]
fn original_group_dependency_visibility_and_monotonic_regression_keep_normal_end() {
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = common::setup(temp.path());
    let run = prepare_historical_run_v1(&config.preparation).unwrap();
    let mut replay = PublicReplayV1::open(&run.public_root, 1, 1 << 20).unwrap();
    let mut end = false;
    let mut collector = HistoricalStatisticsCollectorV1::default();
    while let Some((input, applied)) = replay.next_applied().unwrap() {
        if matches!(input.ingress, DptIngressV1::EndOfInput) {
            end = true;
            assert!(applied.applied.closed_groups.is_empty());
        }
        for group in &applied.applied.closed_groups {
            assert!(
                original_public_group_visible_at_utc_ms(&input, group)
                    >= group
                        .records
                        .iter()
                        .map(|fact| fact.dependency_time.received_at_utc_ms)
                        .max()
                        .unwrap()
            );
            collector.last_visible = Some(common::BASE + 20000);
            collector.observe(&input, group, &run.initialization);
            assert_eq!(collector.last_visible, Some(common::BASE + 20000));
            assert!(
                collector
                    .statistics
                    .markets
                    .values()
                    .all(|m| m.last_visible_at == Some(common::BASE + 20000))
            );
        }
    }
    assert!(end);
    assert!(run.hidden.prefix.normal_end);
}
#[test]
fn fresh_empty_bid_and_retained_unavailable_are_distinct() {
    let mut bid: ObservedFactV1<Option<ExactDecimal>> = ObservedFactV1 {
        value: Some(None),
        quality: MarketDataQuality::Fresh,
        ..Default::default()
    };
    assert_eq!(best_bid(&bid), "known_empty:Fresh");
    bid.quality = MarketDataQuality::ConfirmedGap;
    assert_eq!(best_bid(&bid), "unavailable:ConfirmedGap");
    bid.value = None;
    bid.quality = MarketDataQuality::UnknownNotObserved;
    assert_eq!(best_bid(&bid), "unavailable:UnknownNotObserved");
}
#[test]
fn spot_repetition_and_source_time_regression_keep_original_statistics() {
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = common::setup(temp.path());
    let run = prepare_historical_run_v1(&config.preparation).unwrap();
    let mut replay = PublicReplayV1::open(&run.public_root, 1, 1 << 20).unwrap();
    let (metadata, _) = replay.next_applied().unwrap().unwrap();
    let mut core = PublicObservationCoreV1::new(config.preparation.effective_header()).unwrap();
    let mut collector = HistoricalStatisticsCollectorV1::default();
    let mut inputs = vec![metadata];
    for (received, source) in [(2000, 1800), (2000, 1800), (2300, 1700)] {
        let row:PolymarketRtdsChainlinkBtcUsdRow=serde_json::from_value(json!({"timestamp_received":common::BASE+received,"message_timestamp":common::BASE+received-1,"payload_timestamp":common::BASE+source,"age_ms":received-source,"symbol":"btc/usd","value":"64950.01","topic":"crypto_prices_chainlink","message_type":"update","freshness_status":"Fresh","subscription_filter":"{}","source_native_payload_json":"{\"value\":64950.01}","collector_host_id":"test","collector_region":"test","source_endpoint":"wss://ws-live-data.polymarket.com","schema_version":"polymarket_rtds_chainlink_btc_usd_v4"})).unwrap();
        let row = TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row));
        inputs.push(RecordedIngressV1 {
            input_stream_id: core.header().input_stream_id.clone(),
            input_position: inputs.len() as u64,
            ingress: DptIngressV1::Message {
                source_evidence: source_evidence_v3_from_typed_raw_v2(&row).unwrap(),
                row,
            },
            dependency_record_refs: vec![],
        });
    }
    let DptIngressV1::Message {
        row: TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(spot)),
        ..
    } = &inputs.last().unwrap().ingress
    else {
        unreachable!()
    };
    let mut common_row = spot.clone();
    common_row.schema_version = POLYMARKET_RTDS_CHAINLINK_TWAP_SCHEMA_V1.into();
    common_row.topic = "crypto_prices_twap_sixty".into();
    common_row.timestamp_received = common::BASE + 2400;
    common_row.age_ms = Some(common_row.timestamp_received - common_row.payload_timestamp);
    let row = TypedRawRowV2::PolymarketRtdsChainlinkTwap(PolymarketRtdsChainlinkTwapInput {
        common: common_row,
        full_accuracy_value: "64950010000000000000000".into(),
        window_s: 60,
        source_sequence_epoch_id: "test-rtds-epoch".into(),
        collector_admission_sequence: 1,
        collector_admission_time_utc_ms: common::BASE + 2401,
    });
    inputs.push(RecordedIngressV1 {
        input_stream_id: core.header().input_stream_id.clone(),
        input_position: inputs.len() as u64,
        ingress: DptIngressV1::Message {
            source_evidence: source_evidence_v3_from_typed_raw_v2(&row).unwrap(),
            row,
        },
        dependency_record_refs: vec![],
    });
    let mut admission_witness = false;
    for input in inputs {
        let prepared = prepare_public_ingress_v1(&core, &input, &Default::default(), &[]).unwrap();
        let applied = core.push(&prepared).unwrap();
        for group in &applied.applied.closed_groups {
            if input.input_position == 4 {
                assert_eq!(
                    original_public_group_visible_at_utc_ms(&input, group),
                    common::BASE + 2401
                );
                admission_witness = true;
            }
            collector.observe(
                &input,
                group,
                &HistoricalInitializationProvenanceV1::default(),
            );
        }
    }
    assert!(admission_witness);
    let stats = &collector.statistics.markets[common::FIRST];
    assert_eq!(stats.distinct_fresh_spot_samples, 2);
    assert_eq!(stats.source_time_regressions, 1);
    assert_eq!(stats.first_fresh_spot_source_at, Some(common::BASE + 1800));
    assert_eq!(stats.last_fresh_spot_source_at, Some(common::BASE + 1700));
}
#[derive(Clone, Default)]
struct Output(Arc<Mutex<Vec<u8>>>);
impl Write for Output {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
struct Broken;
impl Write for Broken {
    fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::other("输出故障"))
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
#[test]
fn progress_off_on_and_output_fault_preserve_full_business_and_failed_prepare_returns_no_statistics()
 {
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = common::setup(temp.path());
    let mut prep = config.preparation;
    let off = prepare_historical_run_with_statistics_v1(&prep, None).unwrap();
    let expected = serde_json::to_value(off.statistics).unwrap();
    let output = Output::default();
    prep.work_root = temp.path().join("progress");
    let on = prepare_historical_run_with_statistics_v1(
        &prep,
        Some(HistoricalPreparationProgressV1::new(
            Duration::from_secs(30),
            output.clone(),
        )),
    )
    .unwrap();
    assert_eq!(serde_json::to_value(on.statistics).unwrap(), expected);
    let rows = output.0.lock().unwrap().clone();
    let rows = std::str::from_utf8(&rows)
        .unwrap()
        .lines()
        .map(|r| serde_json::from_str::<Value>(r).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows.last().unwrap()["stage"], "complete");
    assert_eq!(rows.last().unwrap()["work"]["generated_groups"], 8);
    assert_eq!(rows.last().unwrap()["work"]["applied_inputs"], 9);
    assert!(rows.iter().all(|r| r["eta_seconds"].is_null()));
    assert!(
        rows.iter()
            .filter(|r| r["stage"] != "complete")
            .all(|r| r["published"] == false)
    );
    // 正式无统计准备使用计数路径；进度仍保留相同逻辑观察数量。
    let counted_output = Output::default();
    prep.work_root = temp.path().join("counted-progress");
    prepare_and_publish_historical_run_v1(
        &prep,
        Some(HistoricalPreparationProgressV1::new(
            Duration::from_secs(30),
            counted_output.clone(),
        )),
    )
    .unwrap();
    let counted = counted_output.0.lock().unwrap().clone();
    let counted_last: Value = serde_json::from_str(
        std::str::from_utf8(&counted)
            .unwrap()
            .lines()
            .last()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(counted_last["stage"], "complete");
    for name in [
        "generated_groups",
        "generated_observations",
        "applied_inputs",
    ] {
        assert_eq!(
            counted_last["work"][name],
            rows.last().unwrap()["work"][name]
        );
    }
    prep.work_root = temp.path().join("broken");
    let broken = prepare_historical_run_with_statistics_v1(
        &prep,
        Some(HistoricalPreparationProgressV1::new(
            Duration::from_secs(30),
            Broken,
        )),
    )
    .unwrap();
    assert_eq!(serde_json::to_value(broken.statistics).unwrap(), expected);
    let disabled = Output::default();
    prep.work_root = temp.path().join("disabled");
    prepare_historical_run_with_statistics_v1(
        &prep,
        Some(HistoricalPreparationProgressV1::new(
            Duration::ZERO,
            disabled.clone(),
        )),
    )
    .unwrap();
    assert!(disabled.0.lock().unwrap().is_empty());
    prep.recording.max_capture_output_bytes = 1;
    prep.work_root = temp.path().join("old-failure");
    let error = prepare_historical_run_v1(&prep).err().unwrap().to_string();
    prep.work_root = temp.path().join("new-failure");
    assert_eq!(
        prepare_historical_run_with_statistics_v1(
            &prep,
            Some(HistoricalPreparationProgressV1::new(
                Duration::from_secs(30),
                Broken
            ))
        )
        .err()
        .unwrap()
        .to_string(),
        error
    );
}

#[test]
fn initialization_dependency_uses_exact_shared_group_and_keeps_legacy_arrays() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, _) = common::setup(temp.path());
    common::initialize_at(&mut config, common::BASE + 1000, &[common::FIRST]);
    let prepared = prepare_historical_run_v1(&config.preparation).unwrap();
    let mut replay = PublicReplayV1::open(&prepared.public_root, 1, 1 << 20).unwrap();
    let mut groups = Vec::new();
    while let Some((_, applied)) = replay.next_applied().unwrap() {
        groups.extend(applied.applied.closed_groups);
    }
    assert!(groups.len() > 2);
    let first = &groups[0];
    let last = groups.last().unwrap();
    assert!(first.observations.shared_parts().is_some());
    assert!(first.observations.get_market(common::SECOND).is_none());
    assert!(last.observations.get_market(common::SECOND).is_some());
    let later_source = last
        .observations
        .get_market(common::SECOND)
        .unwrap()
        .lifecycle
        .direct_anchors[0]
        .source_message_id
        .0
        .clone();
    let later = HistoricalInitializationProvenanceV1 {
        source_message_ids: [later_source].into_iter().collect(),
        ..Default::default()
    };
    assert!(!later.observations_have_initialization_dependency(&first.observations));
    assert!(later.observations_have_initialization_dependency(&last.observations));
    // 已保存的早前缀不因后续市场出现而改变。
    assert!(!later.observations_have_initialization_dependency(&first.observations));
    let missing = HistoricalInitializationProvenanceV1 {
        source_message_ids: ["f".repeat(64)].into_iter().collect(),
        ..Default::default()
    };
    for provenance in [&prepared.initialization, &later, &missing] {
        let mut classifier = crate::preparation::InitializationDependencyClassifier::default();
        for group in &groups {
            let legacy_values: Vec<_> = group.observations.clone().iter().cloned().collect();
            let original = legacy_values
                .iter()
                .any(|o| provenance.observation_has_initialization_dependency(o));
            // 只比较本次读取前后的持有者变化；不限定合法共享结构的精确数量。
            let books: Vec<_> = group
                .observations
                .shared_parts()
                .unwrap()
                .0
                .values()
                .flat_map(|market| market.assets.iter().map(|asset| &asset.bids))
                .collect();
            let holders: Vec<_> = books.iter().map(|book| Arc::strong_count(book)).collect();
            assert_eq!(
                provenance.observations_have_initialization_dependency(&group.observations),
                original
            );
            assert_eq!(
                classifier.classify(provenance, &group.observations).0,
                original
            );
            assert_eq!(
                holders,
                books
                    .iter()
                    .map(|book| Arc::strong_count(book))
                    .collect::<Vec<_>>()
            );

            assert_eq!(
                provenance.observations_have_initialization_dependency(&legacy_values.into()),
                original
            );
        }
    }
}

fn dependency_fixture() -> (MarketObservationSnapshotsV1, MarketObservationGroupV1) {
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = common::setup(temp.path());
    let prepared = prepare_historical_run_v1(&config.preparation).unwrap();
    let mut replay = PublicReplayV1::open(&prepared.public_root, 1, 1 << 20).unwrap();
    let mut latest = None;
    while let Some((_, applied)) = replay.next_applied().unwrap() {
        for group in applied.applied.closed_groups {
            let (markets, facts) = group.observations.shared_parts().unwrap();
            latest = Some((markets.clone(), facts.as_ref().clone()));
        }
    }
    latest.unwrap()
}

#[test]
fn initialization_classifier_tracks_sources_fields_and_old_consumed_snapshots() {
    use crate::preparation::InitializationDependencyClassifier;
    let (base, facts) = dependency_fixture();
    let key = base.keys().next().unwrap().clone();
    let marker = "a".repeat(64);
    let replacement = "b".repeat(64);
    let provenance = |ids: &[&str]| HistoricalInitializationProvenanceV1 {
        source_message_ids: ids.iter().map(|id| (*id).into()).collect(),
        ..Default::default()
    };
    let initial = provenance(&[&marker]);
    let mut anchor = base[&key].lifecycle.direct_anchors[0].clone();
    anchor.source_message_id = Sha256Hex(marker.clone());
    for field in 0..18 {
        let mut markets = base.clone();
        let mut market = markets[&key].as_ref().clone();
        let mut group = facts.clone();
        match field {
            0 => market.lifecycle.direct_anchors.push(anchor.clone()),
            1 => market.active.direct_anchors.push(anchor.clone()),
            2 => market.closed.direct_anchors.push(anchor.clone()),
            3 => market.accepting_orders.direct_anchors.push(anchor.clone()),
            4 => market.terminal.direct_anchors.push(anchor.clone()),
            5 => market.official_anchor.direct_anchors.push(anchor.clone()),
            6 => market
                .settlement_reference
                .direct_anchors
                .push(anchor.clone()),
            7 => Arc::make_mut(&mut market.assets[0].bids)
                .direct_anchors
                .push(anchor.clone()),
            8 => Arc::make_mut(&mut market.assets[0].asks)
                .direct_anchors
                .push(anchor.clone()),
            9 => market.assets[0].tick.direct_anchors.push(anchor.clone()),
            10 => market.assets[0]
                .public_trade
                .direct_anchors
                .push(anchor.clone()),
            11 => market.assets[0]
                .best_bid_ask
                .direct_anchors
                .push(anchor.clone()),
            12 => Arc::make_mut(&mut market.assets[0].valuation_best_bid)
                .direct_anchors
                .push(anchor.clone()),
            13 => group.chainlink_spot.direct_anchors.push(anchor.clone()),
            14 => group
                .chainlink_twap_thirty
                .direct_anchors
                .push(anchor.clone()),
            15 => group
                .chainlink_twap_sixty
                .direct_anchors
                .push(anchor.clone()),
            16 => Arc::make_mut(&mut group.binance_depth)
                .direct_anchors
                .push(anchor.clone()),
            17 => group.binance_trade.direct_anchors.push(anchor.clone()),
            _ => unreachable!(),
        }
        markets.insert(key.clone(), Arc::new(market));
        let clean = MarketObservationSetV1::shared(base.clone(), facts.clone());
        let dependent = MarketObservationSetV1::shared(markets.clone(), group.clone());
        let mut classifier = InitializationDependencyClassifier::default();
        assert!(!classifier.classify(&initial, &clean).0);
        assert!(classifier.classify(&initial, &dependent).0, "field={field}");
        assert!(
            !classifier
                .classify(&provenance(&[&replacement]), &dependent)
                .0,
            "等大小替换"
        );
        assert!(
            classifier
                .classify(&provenance(&[&replacement, &marker]), &dependent)
                .0,
            "扩充"
        );
        assert!(!classifier.classify(&provenance(&[]), &dependent).0, "清空");
        assert!(classifier.classify(&initial, &dependent).0);
        assert!(
            !classifier.classify(&initial, &clean).0,
            "消费滞后组，不读生产者最新状态"
        );
        let mut restored = InitializationDependencyClassifier::default();
        assert!(!restored.classify(&initial, &clean).0);
        assert!(restored.classify(&initial, &dependent).0);
        let legacy: Vec<_> = dependent.iter().cloned().collect();
        assert!(classifier.classify(&initial, &legacy.into()).0);
        assert!(
            !classifier
                .classify(&initial, &Vec::<MarketBusinessObservationV1>::new().into())
                .0
        );
        assert!(classifier.classify(&initial, &dependent).0);
        assert!(
            !classifier
                .classify(
                    &initial,
                    &MarketObservationSetV1::shared(Default::default(), group)
                )
                .0,
            "空市场仍是原空观察语义"
        );
    }
    let mut markets = base.clone();
    let mut changed = markets[&key].as_ref().clone();
    changed.active.direct_anchors.push(anchor.clone());
    markets.insert(key.clone(), Arc::new(changed));
    let dependent = MarketObservationSetV1::shared(markets.clone(), facts.clone());
    let mut classifier = InitializationDependencyClassifier::default();
    assert!(classifier.classify(&initial, &dependent).0);
    markets.remove(&key);
    assert!(
        !classifier
            .classify(
                &initial,
                &MarketObservationSetV1::shared(markets, facts.clone())
            )
            .0
    );
    assert!(
        classifier.classify(&initial, &dependent).0,
        "重新新增依赖市场"
    );
    let mut global = facts.clone();
    global.chainlink_spot.direct_anchors.push(anchor);
    assert!(
        classifier
            .classify(
                &initial,
                &MarketObservationSetV1::shared(base, global.clone())
            )
            .0
    );
    let (markets, _) = dependent.shared_parts().unwrap();
    assert!(
        classifier
            .classify(
                &initial,
                &MarketObservationSetV1::shared(markets.clone(), global)
            )
            .0
    );
    assert!(
        classifier.classify(&initial, &dependent).0,
        "全局命中期间也必须更新市场缓存"
    );
}

#[test]
fn initialization_classifier_visits_only_changed_market_after_history_build() {
    use crate::preparation::InitializationDependencyClassifier;
    let (base, facts) = dependency_fixture();
    let template = base.values().next().unwrap();
    let initial = HistoricalInitializationProvenanceV1 {
        source_message_ids: ["a".repeat(64)].into_iter().collect(),
        ..Default::default()
    };
    for history in [128, 2048, 8192] {
        let mut markets = MarketObservationSnapshotsV1::new();
        for index in 0..history {
            let mut market = template.as_ref().clone();
            market.market.condition_id = ConditionId(format!("0x{index:064x}"));
            market.market.clob_market_id = ClobMarketId((index + 1).to_string());
            for (side, asset) in market.assets.iter_mut().enumerate() {
                asset.asset.token_id = TokenId((index * 2 + side + 1).to_string());
            }
            if index > 0 {
                market.lifecycle.value = Some(MappedMarketLifecycleStateV1::Known {
                    value: KnownMarketLifecycleState::Retired,
                });
                market.active.value = Some(false);
                market.closed.value = Some(true);
                market.accepting_orders.value = Some(false);
                market.market.window.start_at_utc_ms -= index as i64 * 300_000;
                market.market.window.end_at_utc_ms -= index as i64 * 300_000;
            }
            markets.insert(market.market.condition_id.0.clone(), Arc::new(market));
        }
        let mut classifier = InitializationDependencyClassifier::default();
        let started = std::time::Instant::now();
        let (dependent, build) = classifier.classify(
            &initial,
            &MarketObservationSetV1::shared(markets.clone(), facts.clone()),
        );
        assert!(!dependent);
        assert_eq!(build.markets, history as u64);
        let build_ns = started.elapsed().as_nanos();
        let key = markets.keys().next().unwrap().clone();
        let mut visits = 0;
        let mut source_ids = 0;
        let started = std::time::Instant::now();
        for index in 0..256 {
            let mut changed = markets[&key].as_ref().clone();
            changed.active.value = Some(index % 2 == 0);
            markets.insert(key.clone(), Arc::new(changed));
            let (dependent, work) = classifier.classify(
                &initial,
                &MarketObservationSetV1::shared(markets.clone(), facts.clone()),
            );
            assert!(!dependent);
            assert!(work.markets <= 2, "只分类一个变化市场的旧、新版本");
            visits += work.markets;
            source_ids += work.source_ids;
        }
        eprintln!(
            "initialization_history_diagnostic={}",
            json!({"history_markets":history,
            "active_markets":1,"groups":256,"build_ns":build_ns,"build_market_visits":build.markets,
            "incremental_wall_ns":started.elapsed().as_nanos(),"incremental_market_visits":visits,
            "incremental_source_id_visits":source_ids})
        );
        let (_, unchanged) = classifier.classify(
            &initial,
            &MarketObservationSetV1::shared(markets, facts.clone()),
        );
        assert_eq!(unchanged.markets, 0);
    }
}
