mod common;
use backtest::*;
use common::*;
use contracts::*;
use data_pipeline_core::{public_input::*, public_reference::*, typed_raw::*};
use data_pipeline_training::{
    historical_initialization::*, public_input::*, public_recording::*, public_replay::*,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use shared_run::*;
use std::path::{Path, PathBuf};

#[test]
fn selected_metadata_is_recorded_replayable_and_bound_without_admitting_unselected_files() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, _) = setup(temp.path());
    initialize_at(&mut config, BASE + 1000, &[FIRST]);
    let mut initial = config.preparation.metadata_initialization.clone().unwrap();
    // 初始化清单可含不读取的其他文件：单文件选择不冒充整日准入。
    let path = temp.path().join("inputs/initial-manifest.json");
    let mut manifest: Value =
        serde_json::from_slice(&std::fs::read(&initial.files[0].manifest_path).unwrap()).unwrap();
    manifest["files"].as_array_mut().unwrap().push(json!({"path":"not-present.parquet","source_kind":"polymarket_market","schema_version":"polymarket_orderbook_v3","bytes":1,"rows":1,"sha256":"f".repeat(64)}));
    std::fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    initial.files[0].manifest_path = path;
    // 重复配置同一物理文件不重复注入。
    initial.files.push(initial.files[0].clone());
    initial.markets.push(initial.markets[0].clone());
    config.preparation.metadata_initialization = Some(initial);
    let prepared = prepare_historical_run_v1(&config.preparation).unwrap();
    assert_eq!(
        prepared
            .initialization
            .input_positions
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![0]
    );
    assert_eq!(prepared.initialization.files.len(), 1);
    assert_ne!(
        prepared.source_identity_sha256,
        prepared.input_set_identity_sha256
    );
    assert!(prepared.hidden.prefix.normal_end);
    let mut recovered =
        PublicRecordingReaderV1::recover(&prepared.public_root, 1, 1 << 20).unwrap();
    let mut bindings = vec![];
    let mut inputs = 0;
    let mut indexes = vec![];
    while let Some(row) = recovered.next_row().unwrap() {
        match row {
            PublicStoredRowV1::SourceBinding(binding) => bindings.push(binding),
            PublicStoredRowV1::Input(_) => inputs += 1,
            PublicStoredRowV1::Group(index) => indexes.push(index),
            _ => {}
        }
    }
    assert_eq!(bindings.len(), 1);
    assert_eq!(inputs, 9);
    assert!(
        prepared
            .initialization
            .group_is_initialization(&indexes[0].member_input_positions)
    );
    assert!(
        !prepared
            .initialization
            .group_is_initialization(&indexes[1].member_input_positions)
    );
    let mut first = PublicReplayV1::open(&prepared.public_root, 1, 1 << 20).unwrap();
    let mut second = PublicReplayV1::open(&prepared.public_root, 7, 1 << 20).unwrap();
    loop {
        let a = first
            .next_applied()
            .unwrap()
            .map(|(input, applied)| (input, applied.applied));
        let b = second
            .next_applied()
            .unwrap()
            .map(|(input, applied)| (input, applied.applied));
        assert_eq!(a, b);
        if a.is_none() {
            break;
        }
    }
    assert_eq!(first.core.fixed_markets().count(), 2);
    let stored: HistoricalInitializationProvenanceV1 = serde_json::from_slice(
        &std::fs::read(prepared.public_root.join("historical-initialization.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        stored.input_positions,
        prepared.initialization.input_positions
    );
}

#[test]
fn overlapping_snapshot_selections_keep_the_union_and_intermediate_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, _) = setup(temp.path());
    initialize_at(&mut config, BASE + 1000, &[FIRST]);
    let root = temp.path().join("overlapping");
    std::fs::create_dir(&root).unwrap();
    let mut writer = data_collect::writer::TypedParquetSegmentWriter::create_for_typed_source(
        &root,
        "metadata",
        "polymarket_market_metadata_v5",
        "polymarket_market_metadata",
    )
    .unwrap();
    writer
        .write_polymarket_market_metadata_v5_rows(&[
            metadata(
                FIRST,
                "3195576",
                ["20", "3"],
                BASE,
                BASE + 1000,
                BASE + 10000,
                1,
            ),
            metadata(
                FIRST,
                "3195576",
                ["20", "3"],
                BASE + 500,
                BASE + 1000,
                BASE + 10000,
                2,
            ),
        ])
        .unwrap();
    let file = writer.finalize().unwrap();
    let relative = file.final_path.file_name().unwrap().to_str().unwrap();
    let manifest = root.join("manifest.json");
    std::fs::write(&manifest,serde_json::to_vec(&json!({"files":[{"path":relative,"source_kind":"polymarket_market_metadata","schema_version":"polymarket_market_metadata_v5","bytes":std::fs::metadata(&file.final_path).unwrap().len(),"rows":2,"sha256":file.sha256}]})).unwrap()).unwrap();
    let facts = PublicFiniteFileV1 {
        path: file.final_path.clone(),
        source_kind: "polymarket_market_metadata".into(),
        schema_version: "polymarket_market_metadata_v5".into(),
        bytes: std::fs::metadata(root.join(relative)).unwrap().len(),
        rows: 2,
        sha256: file.sha256,
    };
    let targets = open_finite_public_input_v1(&facts, 1, 1 << 20)
        .unwrap()
        .flat_map(|batch| batch.unwrap())
        .map(|row| MetadataInitializationMarketV1 {
            condition_id: ConditionId(FIRST.into()),
            snapshot_source_message_id: source_evidence_v3_from_typed_raw_v2(&row)
                .unwrap()
                .source_message_id,
        })
        .collect::<Vec<_>>();
    let initial = config.preparation.metadata_initialization.as_mut().unwrap();
    initial.files = vec![MetadataInitializationFileSelectionV1 {
        manifest_path: manifest,
        relative_paths: vec![relative.into()],
    }];
    // 选择先列后一个Snapshot，也必须保留较早Snapshot开始的完整并集。
    initial.markets = targets.into_iter().rev().collect();
    let prepared = prepare_historical_run_v1(&config.preparation).unwrap();
    assert_eq!(
        prepared
            .initialization
            .input_positions
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    let mut reader = PublicRecordingReaderV1::recover(&prepared.public_root, 1, 1 << 20).unwrap();
    let mut initialization_rows = vec![];
    while let Some(row) = reader.next_row().unwrap() {
        if let PublicStoredRowV1::Input(input) = row
            && prepared
                .initialization
                .input_positions
                .contains(&input.input_position)
            && let DptIngressV1::Message {
                row: TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(metadata)),
                ..
            } = input.ingress
        {
            initialization_rows.push(metadata.collector_admission_sequence);
        }
    }
    assert_eq!(initialization_rows, vec![1, 2]);
}

fn raw_fixture(root: &Path, file: &Path, day: &str, start: i64, hour: &str, rows: u64) -> PathBuf {
    std::fs::create_dir_all(root).unwrap();
    let path = root.join("metadata.parquet");
    std::fs::copy(file, &path).unwrap();
    let manifest = json!({"schema_version":"data_collect_daily_manifest_v3","state":"closed","utc_day":day,"closure_reason":"utc_day_barrier","collection_day_coverage":{"schema_version":"data_collect_collection_day_coverage_v1","state":"ended_known","first_started_utc_ms":start,"last_started_utc_ms":start,"ended_utc_ms":start+86400000,"reactivated_within_day":false},"producer_barriers":["binance_aggtrade","binance_depth","polymarket","polymarket_rtds"],"writer_finalize_acks":["binance_aggtrade","binance_depth_snapshot","binance_depth_update","polymarket_market","polymarket_market_metadata","polymarket_rtds_chainlink_btc_usd","polymarket_rtds_chainlink_twap"],"files":[{"path":"metadata.parquet","source_kind":"polymarket_market_metadata","schema_version":"polymarket_market_metadata_v6","utc_hour":hour,"part":0,"rows":rows,"bytes":std::fs::metadata(&path).unwrap().len(),"sha256":format!("{:x}",Sha256::digest(std::fs::read(path).unwrap()))}]});
    let p = root.join("manifest.json");
    std::fs::write(&p, serde_json::to_vec(&manifest).unwrap()).unwrap();
    p
}
#[test]
fn real_two_market_boundary_keeps_complete_existing_segment_and_required_evidence_errors() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, _) = setup(temp.path());
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let start = 1788912000000;
    let prior = raw_fixture(
        &temp.path().join("prior"),
        &repo.join("backtest/tests/fixtures/metadata-initialization-boundary-v6.parquet"),
        "2026-09-08",
        start - 86400000,
        "2026-09-08T23",
        4,
    );
    let current = raw_fixture(
        &temp.path().join("current"),
        &repo.join("backtest/tests/fixtures/metadata-range-boundary-v6.parquet"),
        "2026-09-09",
        start,
        "2026-09-09T00",
        3,
    );
    let raw: Value = serde_json::from_slice(&std::fs::read(&prior).unwrap()).unwrap();
    let fact = &raw["files"][0];
    let file = PublicFiniteFileV1 {
        path: prior.parent().unwrap().join("metadata.parquet"),
        source_kind: "polymarket_market_metadata".into(),
        schema_version: "polymarket_market_metadata_v6".into(),
        bytes: fact["bytes"].as_u64().unwrap(),
        rows: 4,
        sha256: fact["sha256"].as_str().unwrap().into(),
    };
    let mut targets = vec![];
    for batch in open_finite_public_input_v1(&file, 2, 1 << 20).unwrap() {
        for row in batch.unwrap() {
            if let TypedRawRowV2::PolymarketMarketMetadataV6(input) = &row
                && let PolymarketMarketMetadataV6Input::Existing(meta) = input.as_ref()
                && meta.metadata_row_kind == PolymarketMetadataRowKind::MetadataSnapshot
            {
                targets.push(MetadataInitializationMarketV1 {
                    condition_id: ConditionId(meta.condition_id.clone()),
                    snapshot_source_message_id: source_evidence_v3_from_typed_raw_v2(&row)
                        .unwrap()
                        .source_message_id,
                });
            }
        }
    }
    config.preparation.scope.manifest_paths = vec![current];
    config.preparation.scope.received_start_utc_ms = start;
    config.preparation.scope.received_end_utc_ms = start + 300000;
    let initial = MetadataInitializationConfigV1 {
        files: vec![MetadataInitializationFileSelectionV1 {
            manifest_path: prior,
            relative_paths: vec!["metadata.parquet".into()],
        }],
        markets: targets,
        received_start_utc_ms: start,
        received_end_utc_ms: start + 300000,
        purpose: "真实元数据边界回归；非账户运行".into(),
    };
    let selection = MetadataInitializationSelectionV1::open(&initial, 1, 1 << 20).unwrap();
    let selected = selection
        .read(1, 1 << 20)
        .map(|row| row.unwrap().0)
        .collect::<Vec<_>>();
    let seq = |row: &TypedRawRowV2| match row {
        TypedRawRowV2::PolymarketMarketMetadataV6(input) => match input.as_ref() {
            PolymarketMarketMetadataV6Input::Existing(row) => row.collector_admission_sequence,
            _ => panic!("AnchorRule must be excluded"),
        },
        _ => panic!(),
    };
    assert_eq!(
        selected.iter().map(seq).collect::<Vec<_>>(),
        vec![6601, 6665, 6667]
    );
    config.preparation.metadata_initialization = Some(initial.clone());
    let prepared = prepare_historical_run_v1(&config.preparation).unwrap();
    assert_eq!(prepared.initialization.input_positions.len(), 3);
    assert_eq!(prepared.markets.len(), 2);
    // 不通过selector绕过原归约：缺少Gamma载荷或真实转换前态仍拒绝。
    let range = data_pipeline_training::freeze_public_source_input_set_v1(
        &config.preparation.scope.manifest_paths,
    )
    .unwrap();
    let mut current_rows = vec![];
    for batch in
        open_validated_public_input_parquet_v1(&range.admitted_daily_inputs()[0].files()[0], 2)
            .unwrap()
    {
        current_rows.extend(batch.unwrap());
    }
    for (omit, expected) in [
        (6601, "market context requires a complete Gamma"),
        (6665, "changed without an explicit lifecycle transition"),
        (6667, "market context requires a complete Gamma"),
    ] {
        let rows = selected
            .iter()
            .filter(|row| seq(row) != omit)
            .cloned()
            .chain(current_rows.clone())
            .collect::<Vec<_>>();
        let mut merge = prepare_historical_rows_v1(
            &temp.path().join(format!("omit{omit}")),
            &config.preparation.canonical,
            "deletion".into(),
            || Ok(rows.clone().into_iter().map(Ok)),
        )
        .unwrap();
        let mut header = config.preparation.effective_header();
        header.input_stream_id = "deletion".into();
        let mut core =
            data_pipeline_core::public_observation_core::PublicObservationCoreV1::new(header)
                .unwrap();
        let mut error = None;
        while let Some(mut input) = merge.next_input().unwrap() {
            let prepared = merge.prepare_input(&core, &mut input).unwrap();
            if let Err(e) = core.push(&prepared) {
                error = Some(format!("{e:?}"));
                break;
            }
        }
        assert!(
            error.as_ref().unwrap().contains(expected),
            "omit {omit}: {error:?}"
        );
    }
    let mut wrong = initial.clone();
    let mut absent = wrong.markets[0].clone();
    absent.snapshot_source_message_id = Sha256Hex("0".repeat(64));
    wrong.markets.push(absent);
    assert!(
        MetadataInitializationSelectionV1::open(&wrong, 1, 1 << 20)
            .err()
            .unwrap()
            .to_string()
            .contains("未在所选文件中找到")
    );
    let mut wrong_market = initial.clone();
    wrong_market.markets[0].condition_id = ConditionId(FIRST.into());
    assert!(
        MetadataInitializationSelectionV1::open(&wrong_market, 1, 1 << 20)
            .err()
            .unwrap()
            .to_string()
            .contains("不匹配")
    );
    let mut corrupted: Value =
        serde_json::from_slice(&std::fs::read(&initial.files[0].manifest_path).unwrap()).unwrap();
    corrupted["files"][0]["sha256"] = json!("0".repeat(64));
    std::fs::write(
        &initial.files[0].manifest_path,
        serde_json::to_vec(&corrupted).unwrap(),
    )
    .unwrap();
    let error = MetadataInitializationSelectionV1::open(&initial, 1, 1 << 20)
        .err()
        .unwrap();
    assert!(error.to_string().contains("SHA-256"));
}

#[derive(Default)]
struct MemoryProbe {
    callbacks: Vec<i64>,
    dispatched: usize,
    economics: Option<AccountEconomicsReadV3>,
}
impl StrategyV1 for MemoryProbe {
    type Error = &'static str;
    type Diagnostics = ();
    fn decide(
        &mut self,
        input: &StrategyInputV1,
        at: i64,
    ) -> Result<StrategyOutputV1, Self::Error> {
        if self.callbacks.len() > 30 {
            return Err("no-progress");
        }
        self.callbacks.push(at);
        self.economics = Some(input.action_before.economics().clone());
        Ok(StrategyOutputV1::Continue {
            intents: vec![],
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        })
    }
    fn dispatched(
        &mut self,
        _: &StrategyCalculationV1,
        _: &Result<Vec<SentIntentReceiptV1>, BatchExecutionErrorV1>,
    ) {
        self.dispatched += 1;
    }
}
#[tokio::test(flavor = "current_thread")]
async fn one_driver_keeps_prestart_zero_lifecycle_and_starts_calculation_sequences_at_zero() {
    let temp = tempfile::tempdir().unwrap();
    let (mut config, rules) = setup(temp.path());
    initialize_at(&mut config, BASE + 10100, &[FIRST, SECOND]);
    config.timing.public_delivery_delay_ms = DelayScheduleV1::Sequence(vec![300, 200, 500, 0]);
    config.timing.calculation_start_delay_ms = DelayScheduleV1::Sequence(vec![7]);
    config.timing.calculation_duration_ms = DelayScheduleV1::Sequence(vec![11]);
    config.timing.dispatch_delay_ms = DelayScheduleV1::Sequence(vec![13]);
    let mut probe = MemoryProbe::default();
    let report = run_historical_v1(&config, rules, &mut probe).await.unwrap();
    assert!(!probe.callbacks.is_empty());
    assert!(probe.callbacks.iter().all(|at| *at >= BASE + 10100));
    assert_eq!(probe.callbacks[0], BASE + 10201 + 500 + 7);
    assert_eq!(report.calculation_count, probe.callbacks.len());
    assert_eq!(probe.dispatched, probe.callbacks.len());
    let timing = report.last_dispatched_calculation_timing.as_ref().unwrap();
    assert_eq!(
        timing.calculation_completed_at_utc_ms - timing.calculation_started_at_utc_ms,
        11
    );
    assert_eq!(
        timing.scheduled_send_at_utc_ms - timing.calculation_completed_at_utc_ms,
        13
    );
    let log = std::fs::read_to_string(&config.sim_log).unwrap();
    // SIM输入日志只保存原输入；零金额自动经济事实在NOE持久记录中核对。
    let mut zero_results = vec![];
    fn context_paths(root: &Path, output: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if path.join("context.json").exists() {
                    output.push(path);
                } else {
                    context_paths(&path, output);
                }
            }
        }
    }
    let mut contexts = vec![];
    context_paths(&config.noe_root, &mut contexts);
    for context in contexts {
        let references = ::execution_normalization::durable::StagedNormalizationWriter::<
            contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
            contracts::execution_normalization::SourceNeutralControlResultV3,
        >::published_result_references(&context, u64::MAX)
        .unwrap();
        for reference in references {
            let record = ::execution_normalization::read_durable_noe_record_v2(&reference).unwrap();
            let value = serde_json::to_value(record.record()).unwrap();
            if value.to_string().contains("ZeroNoEligibleMakerFill") {
                zero_results.push(value);
            }
        }
    }
    assert_eq!(
        zero_results.len(),
        1,
        "original empty-market lifecycle preserved"
    );
    assert!(!log.contains("SUBMIT_SINGLE_BUY"));
    assert_eq!(log.matches("CREATE_SESSION").count(), 1);
    let summary = report.delivery_summary.as_ref().unwrap();
    assert_eq!(summary.before_operating_start.groups, 2);
    assert_eq!(summary.within_operating_range.groups, 2);
    assert_eq!(summary.within_operating_range.first_group_position, Some(2));
    let economics = report.account_economics_memory.unwrap();
    assert_eq!(economics, probe.economics.unwrap());
    assert_eq!(
        economics.cash.actual_available_pusd,
        ExactDecimal("2000".into())
    );
    assert_eq!(
        economics.cash.actual_reserved_pusd,
        ExactDecimal("0".into())
    );
    assert!(
        economics
            .markets
            .iter()
            .all(|market| market.success_shares.iter().all(|shares| shares.0 == "0"))
    );
    let metadata: Value =
        serde_json::from_slice(&std::fs::read(config.metadata_path).unwrap()).unwrap();
    assert_ne!(
        metadata["source_identity_sha256"],
        metadata["input_set_identity_sha256"]
    );
}
#[tokio::test(flavor = "current_thread")]
async fn prestart_hidden_time_and_late_initialization_do_not_backdate_or_add_hidden_triggers() {
    for late in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (mut config, rules) = setup(temp.path());
        initialize_at(&mut config, BASE + 2000, &[FIRST]);
        // 研究接收2200的行情，其隐藏源时间1701仍在起点之前；晚到初始化按真实交付生效。
        config.timing.public_delivery_delay_ms =
            DelayScheduleV1::Sequence(vec![if late { 2100 } else { 50 }, 100, 100, 100, 100]);
        config.timing.calculation_start_delay_ms = DelayScheduleV1::Sequence(vec![0; 30]);
        config.timing.calculation_duration_ms = DelayScheduleV1::Fixed(0);
        config.timing.dispatch_delay_ms = DelayScheduleV1::Fixed(0);
        let mut probe = MemoryProbe::default();
        run_historical_v1(&config, rules, &mut probe).await.unwrap();
        assert!(probe.callbacks.iter().all(|at| *at >= BASE + 2000));
        assert_eq!(
            probe.callbacks[0],
            if late { BASE + 2101 } else { BASE + 2301 }
        );
        assert!(!probe.callbacks.contains(&(BASE + 1701)));
    }
}

fn complete_hidden_projection(
    hidden: &data_pipeline_training::hidden_preparation::HiddenPreparationV1,
) -> Value {
    use data_pipeline_training::hidden_preparation::*;
    let mut rows = HiddenStepReaderV1::open(hidden, 1, 1 << 20).unwrap();
    let mut steps_and_reports = Vec::new();
    while let Some(row) = rows.next_row().unwrap() {
        steps_and_reports.push(serde_json::to_value(row).unwrap());
    }
    let mut reader = HiddenIndexReaderV1::open(hidden, 1, 1 << 20).unwrap();
    let mut index = Vec::new();
    while let Some(row) = reader.next_entry().unwrap() {
        index.push(serde_json::to_value(row).unwrap());
    }
    let mut reader = HiddenBucketReaderV1::open(hidden, 1, 1 << 20).unwrap();
    let mut buckets = Vec::new();
    while let Some(bucket) = reader.next_bucket().unwrap() {
        buckets.push(serde_json::to_value(bucket).unwrap());
    }
    json!({"prefix":hidden.prefix,"consumer_stream_id":hidden.consumer_stream_id,
        "steps_and_reports":steps_and_reports,"index":index,"buckets":buckets})
}

fn append_tick_change(config: &HistoricalPreparationConfigV1) {
    use data_collect::{polymarket_orderbook_v3::*, writer::*};
    let path = &config.scope.manifest_paths[0];
    let root = path.parent().unwrap();
    let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
        root,
        "tick-change",
        POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
        "polymarket_market",
    )
    .unwrap();
    let row = PolymarketOrderbookV3Message {
        envelope: PolymarketOrderbookV3Envelope {
            timestamp_received: BASE + 3000,
            timestamp: Some(BASE + 3000),
            market: FIRST.into(),
            slug: Some("btc-updown-5m-engineering".into()),
            collector_host_id: "synthetic-collector".into(),
            collector_region: "test".into(),
            source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
            schema_version: POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION.into(),
            source_sequence_epoch_id: "tick-epoch".into(),
            collector_admission_sequence: 1,
            collector_admission_time_utc_ms: BASE + 3001,
        },
        event: PolymarketOrderbookV3Event::TickSizeChange(PolymarketTickSizeChangeV3 {
            asset_id: "20".into(),
            old_tick_size: Some("0.01".into()),
            new_tick_size: "0.001".into(),
        }),
    };
    writer
        .write_polymarket_orderbook_v3_messages(
            &[row],
            &mut PolymarketOrderbookV3WriteSequenceState::default(),
        )
        .unwrap();
    writer
        .append_key_value_metadata(
            "data_collect_daily_file_fact_v1",
            json!({
        "source_kind":"polymarket_market","schema_version":POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
        "utc_hour":"2026-08-24T00","part":1,"rows":1})
            .to_string(),
        )
        .unwrap();
    let file = writer.finalize().unwrap();
    let mut manifest: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    manifest["files"].as_array_mut().unwrap().push(json!({
        "path":file.final_path.file_name().unwrap().to_str().unwrap(),
        "source_kind":"polymarket_market","schema_version":POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
        "utc_hour":"2026-08-24T00","part":1,"rows":file.row_count,
        "bytes":std::fs::metadata(&file.final_path).unwrap().len(),"sha256":file.sha256}));
    std::fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
}

#[test]
fn production_prepare_direct_core_matches_independent_replay_hidden_with_initial_tick() {
    use data_pipeline_core::pm_reduction::PmStepChangeV1;
    use data_pipeline_training::hidden_preparation::*;
    use sim_system::scenario::PlatformMarketV1;
    for initialized in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (mut config, _) = setup(temp.path());
        append_tick_change(&config.preparation);
        if initialized {
            initialize_at(&mut config, BASE + 1000, &[FIRST]);
        }
        // 第一个分支由原prepare刚完成的生产core直接进入hidden；不是两次replay。
        let direct = prepare_historical_run_v1(&config.preparation).unwrap();
        assert_eq!(
            direct.initialization.input_positions.len(),
            usize::from(initialized)
        );
        let mut replay = PublicReplayV1::open(&direct.public_root, 1, 1 << 20).unwrap();
        let mut current_tick_changed = false;
        while let Some((_, applied)) = replay.next_applied().unwrap() {
            current_tick_changed |= applied
                .applied
                .closed_groups
                .iter()
                .flat_map(|g| &g.observations)
                .filter(|o| o.market.condition_id.0 == FIRST)
                .flat_map(|o| &o.assets)
                .any(|a| {
                    a.asset.token_id.0 == "20"
                        && a.tick.value.as_ref().is_some_and(|t| t.0 == "0.001")
                });
        }
        assert!(current_tick_changed);
        let core = replay.into_core();
        let mut expected_markets = std::collections::BTreeMap::new();
        for market in core.fixed_markets() {
            let context = core.pm_context(&market.condition_id.0).unwrap();
            assert!(!context.identity_source_evidence.is_empty());
            for asset in &context.assets {
                assert_eq!(asset.initial_tick_size_pusd_per_share.0, "0.01");
            }
            let assets = context
                .assets
                .into_iter()
                .map(|a| a.asset)
                .collect::<Vec<_>>()
                .try_into()
                .unwrap();
            expected_markets.insert(
                market.condition_id.0.clone(),
                PlatformMarketV1::try_new(market, assets).unwrap(),
            );
        }
        assert_eq!(direct.markets, expected_markets);
        let rebuilt = prepare_hidden_v1(
            core,
            PublicRecordingReaderV1::recover(&direct.public_root, 1, 1 << 20).unwrap(),
            &temp.path().join("hidden-from-independent-replay"),
            &config.preparation.recording,
            config.preparation.hidden_stream_id.clone(),
        )
        .unwrap();
        assert_eq!(
            complete_hidden_projection(&direct.hidden),
            complete_hidden_projection(&rebuilt)
        );
        let mut steps = HiddenStepReaderV1::open(&direct.hidden, 1, 1 << 20).unwrap();
        let mut saw_first_snapshot = false;
        let mut saw_tick_change = false;
        while let Some(row) = steps.next_row().unwrap() {
            if let HiddenStoredRowV1::Step(row) = row {
                match row.step.change {
                    PmStepChangeV1::InitialSnapshot {
                        asset,
                        initial_tick_size_pusd_per_share,
                        ..
                    } if asset.token_id.0 == "20" => {
                        assert_eq!(initial_tick_size_pusd_per_share.0, "0.01");
                        saw_first_snapshot = true;
                    }
                    PmStepChangeV1::Tick { .. } => saw_tick_change = true,
                    _ => {}
                }
            }
        }
        assert!(saw_first_snapshot && saw_tick_change);
    }
}

#[test]
fn direct_hidden_read_still_fails_original_bad_eof_count_before_success() {
    use data_pipeline_training::{
        hidden_preparation::prepare_hidden_with_progress_v1,
        historical_preparation_progress::{
            HistoricalPreparationNoticeV1, HistoricalPreparationNotifierV1,
            HistoricalPreparationStageV1,
        },
    };
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = setup(temp.path());
    let prepared = prepare_historical_run_v1(&config.preparation).unwrap();
    let mut replay = PublicReplayV1::open(&prepared.public_root, 1, 1 << 20).unwrap();
    while replay.next_applied().unwrap().is_some() {}
    let core = replay.into_core();
    let mut prefix = prepared.hidden.prefix.clone();
    prefix.next_canonical_position += 1;
    std::fs::write(
        prepared
            .public_root
            .join(&prefix.stable_storage_ref)
            .join("prefix.json"),
        serde_json::to_vec(&prefix).unwrap(),
    )
    .unwrap();
    let root = temp.path().join("hidden-bad-eof");
    let mut stages = Vec::new();
    let mut callback = |notice| {
        if let HistoricalPreparationNoticeV1::Stage(stage) = notice {
            stages.push(stage);
        }
    };
    let notifications = HistoricalPreparationNotifierV1::new(&mut callback);
    let result = prepare_hidden_with_progress_v1(
        core,
        PublicRecordingReaderV1::recover(&prepared.public_root, 1, 1 << 20).unwrap(),
        &root,
        &config.preparation.recording,
        config.preparation.hidden_stream_id.clone(),
        Some(&notifications),
    );
    assert_eq!(
        result.unwrap_err().to_string(),
        "稳定分片内容与前缀计数不符"
    );
    assert_eq!(
        stages,
        vec![HistoricalPreparationStageV1::HiddenScanAndSupply]
    );
    assert!(root.join("messages").is_dir()); // 已批准的错误阶段允许保留未完成目录。
    assert!(!root.join("derived").exists());
}

#[test]
fn first_real_gamma_transition_prepares_current_state_at_admission_and_shared_hidden_identity() {
    use data_collect::polymarket_orderbook_v3::*;
    use data_pipeline_core::pm_reduction::PmStepChangeV1;
    use data_pipeline_training::hidden_preparation::*;
    const RECEIVED: i64 = 1_789_240_506_938;
    const ADMITTED: i64 = RECEIVED + 2;
    const DAY_START: i64 = 1_789_171_200_000;
    struct VisibleTime;
    impl sim_system::public_consumption::PublicConsumptionTimeSourceV1 for VisibleTime {
        fn execution_time_for_group(
            &mut self,
            _: &PublicGroupReferenceV1,
        ) -> std::result::Result<i64, sim_system::execution::SimRunActorErrorV2> {
            panic!("需要真实可见前沿")
        }
        fn execution_time_for_visible_group(
            &mut self,
            _: &PublicGroupReferenceV1,
            at: i64,
        ) -> std::result::Result<i64, sim_system::execution::SimRunActorErrorV2> {
            Ok(at)
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let (mut config, _) = setup(temp.path());
    let input_root = temp.path().join("gamma-first");
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/metadata-gamma-initial-transition-v6.parquet");
    let manifest = raw_fixture(
        &input_root,
        &fixture,
        "2026-09-12",
        DAY_START,
        "2026-09-12T19",
        1,
    );
    let facts = PublicFiniteFileV1 {
        path: input_root.join("metadata.parquet"),
        source_kind: "polymarket_market_metadata".into(),
        schema_version: POLYMARKET_MARKET_METADATA_SCHEMA_V6.into(),
        bytes: std::fs::metadata(&fixture).unwrap().len(),
        rows: 1,
        sha256: format!("{:x}", Sha256::digest(std::fs::read(&fixture).unwrap())),
    };
    let original = open_finite_public_input_v1(&facts, 1, 1 << 20)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .remove(0);
    let TypedRawRowV2::PolymarketMarketMetadataV6(metadata) = &original else {
        panic!("V6 metadata")
    };
    let PolymarketMarketMetadataV6Input::Existing(row) = metadata.as_ref() else {
        panic!("Existing metadata")
    };
    assert_eq!(row.clob_market_id, "4475338");
    assert_eq!(
        row.metadata_row_kind,
        PolymarketMetadataRowKind::MarketStateTransition
    );
    assert_eq!(
        row.metadata_origin,
        PolymarketMetadataOrigin::LocalMarketStateTimer
    );
    assert_eq!(
        row.source_evidence_origin,
        Some(MetadataSourceEvidenceOrigin::GammaMarketsDiscovery)
    );
    assert_eq!(row.source_evidence_received_at_utc, Some(RECEIVED - 211));
    assert_eq!(row.timestamp_received, RECEIVED);
    assert_eq!(row.collector_admission_time_utc_ms, ADMITTED);
    let outcomes = row.outcome_evidence().unwrap();
    let expected = outcomes
        .iter()
        .map(|o| (o.asset_id.clone(), o.outcome.clone().unwrap()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        expected
            .values()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        ["Down".to_owned(), "Up".to_owned()].into_iter().collect()
    );
    let condition = row.condition_id.clone();
    let mut writer = data_collect::writer::TypedParquetSegmentWriter::create_for_typed_source(
        &input_root,
        "books",
        POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
        "polymarket_market",
    )
    .unwrap();
    let books = outcomes
        .iter()
        .enumerate()
        .map(|(index, o)| PolymarketOrderbookV3Message {
            envelope: PolymarketOrderbookV3Envelope {
                timestamp_received: ADMITTED + 10,
                timestamp: Some(ADMITTED + 9),
                market: condition.clone(),
                slug: Some(row.slug.clone()),
                collector_host_id: "synthetic-collector".into(),
                collector_region: "test".into(),
                source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
                schema_version: POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION.into(),
                source_sequence_epoch_id: "gamma-first-books".into(),
                collector_admission_sequence: index as u64 + 1,
                collector_admission_time_utc_ms: ADMITTED + 11,
            },
            event: PolymarketOrderbookV3Event::Book(PolymarketBookV3 {
                asset_id: o.asset_id.clone(),
                bids: vec![PolymarketPriceLevelV3 {
                    price: "0.4".into(),
                    size: "10".into(),
                }],
                asks: vec![PolymarketPriceLevelV3 {
                    price: "0.6".into(),
                    size: "10".into(),
                }],
                last_trade_price: None,
            }),
        })
        .collect::<Vec<_>>();
    writer
        .write_polymarket_orderbook_v3_messages(
            &books,
            &mut data_collect::writer::PolymarketOrderbookV3WriteSequenceState::default(),
        )
        .unwrap();
    writer.append_key_value_metadata("data_collect_daily_file_fact_v1", json!({"source_kind":"polymarket_market","schema_version":POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,"utc_hour":"2026-09-12T19","part":0,"rows":2}).to_string()).unwrap();
    let book_file = writer.finalize().unwrap();
    let mut value: Value = serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    value["files"].as_array_mut().unwrap().push(json!({"path":book_file.final_path.file_name().unwrap().to_str().unwrap(),"source_kind":"polymarket_market","schema_version":POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,"utc_hour":"2026-09-12T19","part":0,"rows":2,"bytes":std::fs::metadata(&book_file.final_path).unwrap().len(),"sha256":book_file.sha256}));
    std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
    config.preparation.scope.manifest_paths = vec![manifest];
    config.preparation.scope.received_start_utc_ms = row.market_start_utc;
    config.preparation.scope.received_end_utc_ms = row.market_end_utc;
    let direct = prepare_historical_run_v1(&config.preparation).unwrap();
    assert!(direct.initialization.input_positions.is_empty());
    assert!(direct.hidden.prefix.normal_end);
    let mut delivered = sim_system::public_consumption::RecordedPublicGroupSourceV1::open(
        &direct.public_root,
        1,
        1 << 20,
        VisibleTime,
    )
    .unwrap();
    assert_eq!(
        delivered.next().unwrap().unwrap().delivered_at_utc_ms,
        ADMITTED
    );
    let mut replay = PublicReplayV1::open(&direct.public_root, 1, 1 << 20).unwrap();
    assert_eq!(replay.core.fixed_markets().count(), 0);
    let (first_input, first_applied) = replay.next_applied().unwrap().unwrap();
    assert_eq!(
        first_input.ingress,
        DptIngressV1::Message {
            source_evidence: source_evidence_v3_from_typed_raw_v2(&original).unwrap(),
            row: original
        }
    );
    assert_eq!(first_applied.applied.closed_groups.len(), 1);
    let first_group = &first_applied.applied.closed_groups[0];
    assert_eq!(
        data_pipeline_core::public_observation_core::public_group_visible_at_utc_ms(
            &first_input.ingress,
            &first_group.records
        ),
        ADMITTED
    );
    assert!(
        first_group
            .records
            .iter()
            .all(|fact| fact.direct_time.received_at_utc_ms == RECEIVED)
    );
    assert!(first_group.records.iter().any(|fact| matches!(
        &fact.payload,
        PublicBusinessPayloadV1::MarketFact {
            payload: CanonicalMarketFactPayloadV1::MarketLifecycleSnapshot {
                current_state: MappedMarketLifecycleStateV1::Known {
                    value: KnownMarketLifecycleState::Active
                },
                ..
            },
            ..
        }
    )));
    assert!(!first_group.records.iter().any(|fact| matches!(
        &fact.payload,
        PublicBusinessPayloadV1::MarketFact {
            payload: CanonicalMarketFactPayloadV1::LifecycleTransition(_),
            ..
        }
    )));
    while replay.next_applied().unwrap().is_some() {}
    let core = replay.into_core();
    let context = core.pm_context(&condition).unwrap();
    let actual = context
        .assets
        .iter()
        .map(|a| {
            assert_eq!(a.initial_tick_size_pusd_per_share.0, "0.01");
            (a.asset.token_id.0.clone(), a.asset.outcome.label.clone())
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(actual, expected);
    assert_eq!(
        direct.markets[&condition]
            .assets()
            .iter()
            .map(|a| (a.token_id.0.clone(), a.outcome.label.clone()))
            .collect::<std::collections::BTreeMap<_, _>>(),
        expected
    );
    let rebuilt = prepare_hidden_v1(
        core,
        PublicRecordingReaderV1::recover(&direct.public_root, 1, 1 << 20).unwrap(),
        &temp.path().join("hidden-independent"),
        &config.preparation.recording,
        config.preparation.hidden_stream_id.clone(),
    )
    .unwrap();
    assert_eq!(
        complete_hidden_projection(&direct.hidden),
        complete_hidden_projection(&rebuilt)
    );
    let mut steps = HiddenStepReaderV1::open(&direct.hidden, 1, 1 << 20).unwrap();
    let mut snapshots = std::collections::BTreeMap::new();
    while let Some(value) = steps.next_row().unwrap() {
        if let HiddenStoredRowV1::Step(value) = value
            && let PmStepChangeV1::InitialSnapshot {
                asset,
                initial_tick_size_pusd_per_share,
                ..
            } = value.step.change
        {
            assert_eq!(initial_tick_size_pusd_per_share.0, "0.01");
            snapshots.insert(asset.token_id.0, asset.outcome.label);
        }
    }
    assert_eq!(snapshots, expected);
}
