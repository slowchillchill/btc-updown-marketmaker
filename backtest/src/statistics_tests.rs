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
