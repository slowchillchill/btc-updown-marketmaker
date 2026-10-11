use super::*;
use crate::{day_producer::DayProducerV1, day_stream::DayIngressV1, public_replay::*};
use contracts::*;
use data_collect::polymarket_orderbook_v3::*;
use data_pipeline_core::{pm_reduction::*, public_observation_core::PublicObservationCoreV1};
use parquet::{
    arrow::ArrowWriter,
    file::{metadata::KeyValue, properties::WriterProperties},
};
const ID: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
fn write(
    root: &Path,
    source: &str,
    version: &str,
    batches: Vec<RecordBatch>,
    part: u32,
) -> PathBuf {
    let day = root.join("2026-07-30/typed_raw/utc_day=2026-07-30");
    fs::create_dir_all(&day).unwrap();
    let path = day.join(format!("{source}-part-{part}.parquet"));
    let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    let fact = json!({"source_kind":source,"schema_version":version,"utc_hour":"2026-07-30T00","part":part,"rows":rows});
    let options = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
        .set_key_value_metadata(Some(vec![
            KeyValue::new("parquet_raw_source_kind".into(), source.to_string()),
            KeyValue::new("parquet_raw_layout_version".into(), version.to_string()),
            KeyValue::new(
                "data_collect_artifact_kind".into(),
                "typed_raw_parquet".to_string(),
            ),
            KeyValue::new("data_collect_daily_file_fact_v1".into(), fact.to_string()),
        ]))
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(&path).unwrap(),
        batches[0].schema(),
        Some(options),
    )
    .unwrap();
    for batch in batches {
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    let manifest = day.join("manifest.json");
    let value = json!({"schema_version":"data_collect_daily_manifest_v3","state":"closed","utc_day":"2026-07-30","closure_reason":"utc_day_barrier","collection_day_coverage":{"schema_version":"data_collect_collection_day_coverage_v1","state":"ended_known","first_started_utc_ms":1_785_369_600_000_i64,"last_started_utc_ms":1_785_369_600_000_i64,"ended_utc_ms":1_785_456_000_000_i64,"reactivated_within_day":false},"producer_barriers":crate::REQUIRED_PRODUCER_BARRIERS,"writer_finalize_acks":crate::PUBLIC_WRITER_FINALIZE_ACKS,"files":[{"path":path.file_name().unwrap().to_str().unwrap(),"source_kind":source,"schema_version":version,"utc_hour":"2026-07-30T00","part":part,"rows":rows,"bytes":fs::metadata(&path).unwrap().len(),"sha256":format!("{:x}",Sha256::digest(fs::read(&path).unwrap()))}]});
    fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
    manifest
}
fn pm(seq: u64, at: i64, event: PolymarketOrderbookV3Event) -> PolymarketOrderbookV3Message {
    PolymarketOrderbookV3Message {
        envelope: PolymarketOrderbookV3Envelope {
            timestamp_received: RECEIVED + 100 + seq as i64,
            timestamp: Some(at),
            market: ID.into(),
            slug: Some("btc-updown-5m-test".into()),
            collector_host_id: "collector-1".into(),
            collector_region: "hk".into(),
            source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
            schema_version: POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION.into(),
            source_sequence_epoch_id: "epoch-a".into(),
            collector_admission_sequence: seq,
            collector_admission_time_utc_ms: RECEIVED + 101 + seq as i64,
        },
        event,
    }
}
fn book(size: &str) -> PolymarketOrderbookV3Event {
    PolymarketOrderbookV3Event::Book(PolymarketBookV3 {
        asset_id: "1".into(),
        bids: vec![PolymarketPriceLevelV3 {
            price: "0.5".into(),
            size: size.into(),
        }],
        asks: vec![PolymarketPriceLevelV3 {
            price: "0.6".into(),
            size: "80".into(),
        }],
        last_trade_price: Some("0.55".into()),
    })
}
fn change(size: &str) -> PolymarketOrderbookV3Event {
    PolymarketOrderbookV3Event::PriceChange(PolymarketPriceChangeV3 {
        price_changes: vec![PolymarketPriceChangeEntryV3 {
            asset_id: "1".into(),
            price: "0.5".into(),
            size: Some(size.into()),
            side: "BUY".into(),
            best_bid: None,
            best_ask: None,
        }],
    })
}
fn header() -> PublicInputStreamHeaderV1 {
    PublicInputStreamHeaderV1 {
        input_stream_id: "day-test".into(),
        canonical_rule_version: "canonical-reduction-v1".into(),
        observation_version: "market_business_observation_v1".into(),
        ordering_version: HISTORICAL_SOURCE_HEAD_ORDERING_VERSION.into(),
        input_scope: "fixture".into(),
        max_pending_ws_terminals: 2,
        initial_state: PublicInitialStateV1::Empty,
    }
}
#[test]
fn original_parquet_day_keeps_head_order_private_quantities_and_complete_buckets() {
    let tmp = tempfile::tempdir().unwrap();
    let mut row = common_metadata();
    row.condition_id = ID.into();
    row.clob_market_id = "3195576".into();
    let metadata = polymarket_market_metadata_v6_rows_to_batch(
        polymarket_market_metadata_v6_schema(),
        &[PolymarketMarketMetadataV6Row::Existing(
            PolymarketMarketMetadataV5Row::from_common(row, None, None),
        )],
    )
    .unwrap();
    let manifests = [
        write(
            &tmp.path().join("meta"),
            METADATA_SOURCE,
            POLYMARKET_MARKET_METADATA_SCHEMA_V6,
            vec![metadata],
            0,
        ),
        // 同一个文件的物理行也不能覆盖准入顺序；不要求 Footer 时间统计。
        write(
            &tmp.path().join("pm10"),
            "polymarket_market",
            POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
            vec![
                polymarket_orderbook_v3_messages_to_batch(&[
                    pm(3, RECEIVED + 200, change("90")),
                    pm(1, RECEIVED + 150, book("100")),
                ])
                .unwrap(),
            ],
            10,
        ),
        write(
            &tmp.path().join("pm2"),
            "polymarket_market",
            POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
            vec![
                polymarket_orderbook_v3_messages_to_batch(&[pm(
                    2,
                    RECEIVED + 200,
                    PolymarketOrderbookV3Event::BestBidAsk(PolymarketBestBidAskV3 {
                        asset_id: "1".into(),
                        best_bid: Some("0.5".into()),
                        best_ask: Some("0.6".into()),
                        spread: Some("0.1".into()),
                    }),
                )])
                .unwrap(),
            ],
            2,
        ),
    ];
    let admissions = manifests
        .iter()
        .map(|m| crate::validate_public_source_daily_manifest_admission_v1(m).unwrap())
        .collect::<Vec<_>>();
    let files = admissions
        .iter()
        .flat_map(|a| a.files())
        .collect::<Vec<_>>();
    let range = RECEIVED - 1..RECEIVED + 1000;
    let limits = crate::gate_a_formal_canonical_config();
    let mut old = prepare_historical_files_in_range_v1(
        &tmp.path().join("old"),
        &files,
        &limits,
        "day-test".into(),
        Some(range.clone()),
    )
    .unwrap();
    let mut expected = Vec::new();
    while let Some(input) = old.next_input().unwrap() {
        expected.push(input.ingress);
    }
    let mut ingress = DayIngressV1::open(
        &files,
        "day-test".into(),
        1,
        1 << 20,
        range.clone(),
        vec![],
        limits.max_epoch_count_per_source,
    )
    .unwrap();
    let mut actual = Vec::new();
    while let Some(input) = ingress.next_input().unwrap() {
        if !matches!(input.ingress, DptIngressV1::EndOfInput) {
            actual.push(input.ingress);
        }
    }
    assert_eq!(actual, expected);
    let ingress = DayIngressV1::open(
        &files,
        "day-test".into(),
        1,
        1 << 20,
        range,
        vec![],
        limits.max_epoch_count_per_source,
    )
    .unwrap();
    let mut producer = DayProducerV1::new(
        ingress,
        header(),
        Sha256Hex("a".repeat(64)),
        "hidden-test".into(),
    )
    .unwrap();
    // 公共生产已领先、隐藏桶尚未关闭时保存真实状态；恢复不应用已提交前缀。
    let first = producer.next_applied().unwrap().unwrap();
    assert_eq!(first.input.input_position, 0);
    let second = producer.next_applied().unwrap().unwrap();
    assert_eq!(second.input.input_position, 1);
    assert!(second.hidden.is_empty());
    let order_path = tmp.path().join("source-order.cbor");
    producer.ingress.persist_order_index(&order_path).unwrap();
    let mut core_state = Vec::new();
    producer.core.write_checkpoint(&mut core_state).unwrap();
    let mut source_state = Vec::new();
    producer.write_checkpoint(&mut source_state).unwrap();
    let mut buckets = Vec::new();
    let mut applied = 2;
    let mut remainder = Vec::new();
    while let Some(next) = producer.next_applied().unwrap() {
        applied += 1;
        remainder.push((
            next.input.input_position,
            serde_json::to_value(&next.applied).unwrap(),
            serde_json::to_value(&next.hidden).unwrap(),
        ));
        buckets.extend(next.hidden);
    }
    let restored_core = PublicObservationCoreV1::read_checkpoint(core_state.as_slice()).unwrap();
    assert_eq!(restored_core.applied_prefix().next_input_position, 2);
    let mut restored = DayProducerV1::restore(
        restored_core,
        &files,
        source_state.as_slice(),
        1,
        1 << 20,
        RECEIVED - 1..RECEIVED + 1000,
    )
    .unwrap();
    let mut resumed = Vec::new();
    while let Some(next) = restored.next_applied().unwrap() {
        resumed.push((
            next.input.input_position,
            serde_json::to_value(&next.applied).unwrap(),
            serde_json::to_value(&next.hidden).unwrap(),
        ));
    }
    assert_eq!(resumed, remainder);
    // 解码后的尾部也属于发布摘要；损坏索引必须在恢复供料前拒绝。
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&order_path)
        .unwrap()
        .write_all(b"tail")
        .unwrap();
    assert!(
        DayProducerV1::restore(
            PublicObservationCoreV1::read_checkpoint(core_state.as_slice()).unwrap(),
            &files,
            source_state.as_slice(),
            1,
            1 << 20,
            RECEIVED - 1..RECEIVED + 1000,
        )
        .is_err()
    );

    assert_eq!(applied, 5);
    assert!(
        buckets
            .iter()
            .any(|b| b.reference.bucket_time_utc_ms == RECEIVED + 200)
    );
    let same = buckets
        .iter()
        .filter(|b| b.reference.bucket_time_utc_ms == RECEIVED + 200)
        .collect::<Vec<_>>();
    assert_eq!(same.len(), 1);
    let transition = same[0]
        .records
        .iter()
        .find_map(|r| match r {
            crate::hidden_preparation::ResolvedHiddenRecordV1::Step { stored, .. } => {
                match &stored.step.change {
                    PmStepChangeV1::LevelChange { transition, .. } => Some(transition),
                    _ => None,
                }
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(
        transition.quantity_before_shares,
        ExactDecimal("100".into())
    );
    assert_eq!(transition.quantity_after_shares, ExactDecimal("90".into()));
    assert!(same[0].records.iter().any(|r|matches!(r,crate::hidden_preparation::ResolvedHiddenRecordV1::Step { stored,.. } if matches!(stored.step.change,PmStepChangeV1::BestBidAsk { .. }))));
}

#[test]
fn day_initialization_and_prior_twap_follow_original_visible_reference_selection() {
    use crate::historical_initialization::*;
    let tmp = tempfile::tempdir().unwrap();
    let mut row = common_metadata();
    row.condition_id = ID.into();
    row.clob_market_id = "3195576".into();
    let metadata = polymarket_market_metadata_v6_rows_to_batch(
        polymarket_market_metadata_v6_schema(),
        &[PolymarketMarketMetadataV6Row::Existing(
            PolymarketMarketMetadataV5Row::from_common(row, None, None),
        )],
    )
    .unwrap();
    let metadata_manifest = write(
        &tmp.path().join("meta"),
        METADATA_SOURCE,
        POLYMARKET_MARKET_METADATA_SCHEMA_V6,
        vec![metadata],
        0,
    );
    let mut anchor = match anchor_row() {
        PolymarketMarketMetadataV6Row::AnchorRule(r) => r,
        _ => unreachable!(),
    };
    anchor.identity.condition_id = ID.into();
    anchor.identity.clob_market_id = "3195576".into();
    anchor.identity.slug = "btc-updown-5m-test".into();
    anchor.identity.market_start_utc = RECEIVED + 100;
    anchor.identity.market_end_utc = RECEIVED + 300100;
    anchor.timestamp_received = RECEIVED + 180;
    anchor.collector_admission_time_utc_ms = RECEIVED + 181;
    anchor.observation.price_to_beat_decimal = None;
    anchor.observation.price_to_beat_source_field = None;
    let anchor_batch = polymarket_market_metadata_v6_rows_to_batch(
        polymarket_market_metadata_v6_schema(),
        &[PolymarketMarketMetadataV6Row::AnchorRule(anchor)],
    )
    .unwrap();
    let anchor_manifest = write(
        &tmp.path().join("anchor"),
        METADATA_SOURCE,
        POLYMARKET_MARKET_METADATA_SCHEMA_V6,
        vec![anchor_batch],
        1,
    );
    let twap_root = tmp.path().join("twap");
    let directory = twap_root.join("2026-07-30/typed_raw/utc_day=2026-07-30");
    fs::create_dir_all(&directory).unwrap();
    let mut writer =
        TypedParquetSegmentWriter::create_for_current_source(&directory, "twap", TWAP_SOURCE)
            .unwrap();
    let twap = RtdsChainlinkTwapHourlyRow {
        common: RtdsChainlinkBtcUsdHourlyRow {
            timestamp_received: RECEIVED + 110,
            message_timestamp: RECEIVED + 105,
            payload_timestamp: RECEIVED + 100,
            age_ms: Some(10),
            symbol: "btc/usd".into(),
            value: "64950.01".into(),
            topic: "crypto_prices_twap_thirty".into(),
            message_type: "update".into(),
            freshness_status: "fresh".into(),
            subscription_filter: "{\"symbol\":\"btc/usd\"}".into(),
            source_native_payload_json: "{\"value\":\"64950014218511440012345\"}".into(),
            collector_host_id: "host".into(),
            collector_region: "hk".into(),
            source_endpoint: "wss://ws-live-data.polymarket.com".into(),
            schema_version: POLYMARKET_RTDS_CHAINLINK_TWAP_SCHEMA_V1.into(),
        },
        full_accuracy_value: "64950014218511440012345".into(),
        window_s: 30,
        source_sequence_epoch_id: "twap-epoch".into(),
        collector_admission_sequence: 1,
        collector_admission_time_utc_ms: RECEIVED + 111,
    };
    writer
        .write_rtds_chainlink_twap_rows_with_logical_bytes(&[twap], 100)
        .unwrap();
    let segment = finalize_with_fact(
        writer,
        TWAP_SOURCE,
        POLYMARKET_RTDS_CHAINLINK_TWAP_SCHEMA_V1,
        1,
    );
    let twap_manifest = manifest_for(
        &twap_root,
        &segment,
        TWAP_SOURCE,
        POLYMARKET_RTDS_CHAINLINK_TWAP_SCHEMA_V1,
        true,
    );
    let manifests = [metadata_manifest.clone(), anchor_manifest, twap_manifest];
    let admitted = manifests
        .iter()
        .map(|p| crate::validate_public_source_daily_manifest_admission_v1(p).unwrap())
        .collect::<Vec<_>>();
    let files = admitted.iter().flat_map(|d| d.files()).collect::<Vec<_>>();
    let input = open_validated_public_input_parquet_v1(files[0], 1)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .pop()
        .unwrap();
    let source =
        data_pipeline_core::public_reference::source_evidence_v3_from_typed_raw_v2(&input).unwrap();
    let initial = MetadataInitializationConfigV1 {
        files: vec![MetadataInitializationFileSelectionV1 {
            manifest_path: metadata_manifest,
            relative_paths: vec![files[0].file_path().file_name().unwrap().into()],
        }],
        markets: vec![MetadataInitializationMarketV1 {
            condition_id: ConditionId(ID.into()),
            snapshot_source_message_id: source.source_message_id,
        }],
        received_start_utc_ms: RECEIVED + 100,
        received_end_utc_ms: RECEIVED + 1000,
        purpose: "工程夹具".into(),
    };
    let selection = MetadataInitializationSelectionV1::open(&initial, 1, 1 << 20).unwrap();
    let selected = selection
        .read(1, 1 << 20)
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(selected.len(), 1);
    // 两份正式清单定位同一canonical文件；相同事实保留首次来源绑定。
    let first_manifest = &initial.files[0].manifest_path;
    let duplicate_manifest = first_manifest
        .parent()
        .unwrap()
        .join("duplicate-manifest.json");
    let raw = fs::read(first_manifest).unwrap();
    let mut duplicate_raw = raw.clone();
    duplicate_raw.push(b'\n');
    fs::write(&duplicate_manifest, duplicate_raw).unwrap();
    let mut overlapping = initial.clone();
    overlapping
        .files
        .push(MetadataInitializationFileSelectionV1 {
            manifest_path: duplicate_manifest.clone(),
            relative_paths: initial.files[0].relative_paths.clone(),
        });
    let reused = MetadataInitializationSelectionV1::open(&overlapping, 1, 1 << 20).unwrap();
    assert_eq!(reused.files.len(), 1);
    let duplicate_selected = reused
        .read(1, 1 << 20)
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&duplicate_selected).unwrap(),
        serde_json::to_value(&selected).unwrap()
    );
    assert_eq!(
        reused.files[0].manifest_sha256,
        selection.files[0].manifest_sha256
    );
    // 矛盾SHA仍来自同一有效清单schema，不能因为路径重复而跳过声明核对。
    let mut conflicting: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    conflicting["files"][0]["sha256"] = json!("0".repeat(64));
    fs::write(
        &duplicate_manifest,
        serde_json::to_vec(&conflicting).unwrap(),
    )
    .unwrap();
    let error = MetadataInitializationSelectionV1::open(&overlapping, 1, 1 << 20)
        .err()
        .expect("矛盾清单必须拒绝");
    assert!(error.to_string().contains("清单事实互相矛盾"), "{error}");
    let limits = crate::gate_a_formal_canonical_config();
    let mut merge = prepare_historical_files_with_initialization_and_progress_v1(
        &tmp.path().join("old"),
        &files,
        &limits,
        "day-test".into(),
        Some(RECEIVED + 100..RECEIVED + 1000),
        Some(&selection),
        None,
    )
    .unwrap();
    let mut old_core = PublicObservationCoreV1::new(header()).unwrap();
    let mut expected = Vec::new();
    while let Some(mut input) = merge.next_input().unwrap() {
        let prepared = merge.prepare_input(&old_core, &mut input).unwrap();
        let applied = old_core.push(&prepared).unwrap().into_owned();
        expected.push(serde_json::to_value(applied.applied).unwrap());
    }
    let ingress = DayIngressV1::open(
        &files,
        "day-test".into(),
        1,
        1 << 20,
        RECEIVED + 100..RECEIVED + 1000,
        selected,
        limits.max_epoch_count_per_source,
    )
    .unwrap();
    let mut producer = DayProducerV1::new(
        ingress,
        header(),
        Sha256Hex("a".repeat(64)),
        "hidden-test".into(),
    )
    .unwrap();
    let first = producer.next_applied().unwrap().unwrap();
    assert!(first.initialization_binding.is_some());
    let mut actual = vec![serde_json::to_value(&first.applied).unwrap()];
    let mut core = Vec::new();
    producer.core.write_checkpoint(&mut core).unwrap();
    let mut saved = Vec::new();
    producer.write_checkpoint(&mut saved).unwrap();
    let mut producer = DayProducerV1::restore(
        PublicObservationCoreV1::read_checkpoint(core.as_slice()).unwrap(),
        &files,
        saved.as_slice(),
        1,
        1 << 20,
        RECEIVED + 100..RECEIVED + 1000,
    )
    .unwrap();
    while let Some(next) = producer.next_applied().unwrap() {
        actual.push(serde_json::to_value(next.applied).unwrap());
    }
    // 原外排 next_input 不自动签发 End；比较真实来源应用和原初始化/参考选择。
    actual.pop();
    assert_eq!(actual, expected);
    assert!(
        actual
            .iter()
            .any(|value| value.to_string().contains("64950.014218511440012345"))
    );
    assert_eq!(
        producer
            .core
            .reference_state(ID)
            .unwrap()
            .temporary
            .as_ref()
            .unwrap()
            .value
            .price_usd_per_btc,
        ExactDecimal("64950.014218511440012345".into())
    );
    assert_eq!(
        serde_json::to_value(
            producer
                .core
                .reference_state(ID)
                .unwrap()
                .temporary
                .as_ref()
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(
            old_core
                .reference_state(ID)
                .unwrap()
                .temporary
                .as_ref()
                .unwrap()
        )
        .unwrap()
    );
}

#[test]
fn day_quantity_projection_keeps_original_binding_hidden_buckets_and_pm_asks() {
    use data_collect::source_hourly_layout::{
        BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION, BinanceDepthSnapshotHourlyRow,
    };
    let tmp = tempfile::tempdir().unwrap();
    let mut row = common_metadata();
    row.condition_id = ID.into();
    row.clob_market_id = "3195576".into();
    let metadata = polymarket_market_metadata_v6_rows_to_batch(
        polymarket_market_metadata_v6_schema(),
        &[PolymarketMarketMetadataV6Row::Existing(
            PolymarketMarketMetadataV5Row::from_common(row, None, None),
        )],
    )
    .unwrap();
    let depth_root = tmp.path().join("depth");
    let depth_day = depth_root.join("2026-07-30/typed_raw/utc_day=2026-07-30");
    let mut writer = TypedParquetSegmentWriter::create_for_current_source(
        &depth_day,
        "binance_depth_snapshot_00",
        "binance_depth_snapshot",
    )
    .unwrap();
    writer
        .write_binance_depth_snapshot_rows(&[BinanceDepthSnapshotHourlyRow {
            timestamp_received: RECEIVED + 10,
            snapshot_fetch_start_utc: RECEIVED + 9,
            last_update_id: 100,
            bids: r#"[["64100","0.7"]]"#.into(),
            asks: r#"[["64200","0.4"]]"#.into(),
            symbol: "BTCUSDT".into(),
            source_endpoint: "https://api.binance.com/api/v3/depth".into(),
            snapshot_status: "complete".into(),
            source_sequence_epoch_id: Some("depth-a".into()),
            connection_generation: Some(1),
            depth_ingress_sequence: Some(1),
            collector_admission_sequence: Some(1),
            collector_admission_time_utc_ms: Some(RECEIVED + 11),
            collector_host_id: "collector-1".into(),
            collector_region: "hk".into(),
            schema_version: BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION.into(),
        }])
        .unwrap();
    let segment = finalize_with_fact(
        writer,
        "binance_depth_snapshot",
        BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
        1,
    );
    let manifests = [
        write(
            &tmp.path().join("meta"),
            METADATA_SOURCE,
            POLYMARKET_MARKET_METADATA_SCHEMA_V6,
            vec![metadata],
            0,
        ),
        manifest_for(
            &depth_root,
            &segment,
            "binance_depth_snapshot",
            BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
            true,
        ),
        write(
            &tmp.path().join("pm"),
            "polymarket_market",
            POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
            vec![
                polymarket_orderbook_v3_messages_to_batch(&[pm(1, RECEIVED + 100, book("100"))])
                    .unwrap(),
            ],
            0,
        ),
    ];
    let admissions = manifests
        .iter()
        .map(|m| crate::validate_public_source_daily_manifest_admission_v1(m).unwrap())
        .collect::<Vec<_>>();
    let files = admissions
        .iter()
        .flat_map(|a| a.files())
        .collect::<Vec<_>>();
    let run = |requirements| {
        let ingress = DayIngressV1::open(
            &files,
            "day-test".into(),
            1,
            1 << 20,
            RECEIVED - 1..RECEIVED + 1000,
            vec![],
            crate::gate_a_formal_canonical_config().max_epoch_count_per_source,
        )
        .unwrap();
        let mut producer = DayProducerV1::new(
            ingress,
            header(),
            Sha256Hex("a".repeat(64)),
            "hidden-test".into(),
        )
        .unwrap();
        producer.core.set_observation_requirements(requirements);
        let mut groups = Vec::new();
        let mut buckets = Vec::new();
        while let Some(next) = producer.next_applied().unwrap() {
            groups.extend(next.applied.closed_groups);
            buckets.extend(next.hidden);
        }
        (groups, serde_json::to_value(buckets).unwrap())
    };
    let (full, full_hidden) = run(PublicObservationRequirementsV1::FULL);
    let (compact, compact_hidden) = run(PublicObservationRequirementsV1 {
        binance_depth_quantities: false,
    });
    assert_eq!(
        full_hidden, compact_hidden,
        "私有数量与桶来源绑定不受观察投影影响"
    );
    assert!(!full_hidden.as_array().unwrap().is_empty());
    assert_eq!(full.len(), compact.len());
    let mut depth_facts = 0;
    for (a, b) in full.iter().zip(&compact) {
        assert_eq!(
            (
                a.group_id.clone(),
                a.group_position,
                a.closed_at_input_position
            ),
            (
                b.group_id.clone(),
                b.group_position,
                b.closed_at_input_position
            )
        );
        assert_eq!(a.records.len(), b.records.len());
        for (old, new) in a.records.iter().zip(b.records.iter()) {
            if matches!(
                old.payload,
                PublicBusinessPayloadV1::ReferenceFact(
                    CanonicalReferenceEventPayloadV2::BinanceDepthState(_)
                )
            ) {
                depth_facts += 1;
                let PublicBusinessPayloadV1::BinanceDepthReference(binding) = &new.payload else {
                    panic!("完整事实改为同来源引用")
                };
                assert_eq!(binding.position, new.position);
                let mut expected = old.clone();
                expected.payload = new.payload.clone();
                assert_eq!(expected, *new);
            } else {
                assert_eq!(old, new);
            }
        }
        for (old, new) in a.observations.iter().zip(&b.observations) {
            let mut expected = old.clone();
            if expected.binance_depth.value.is_some() {
                assert!(old.binance_depth.value.as_ref().unwrap().full().is_some());
                let mut depth = (*expected.binance_depth).clone();
                depth.value = Some(BinanceDepthObservationV1::NotMaterialized);
                expected.binance_depth = std::sync::Arc::new(depth);
            }
            expected.group_events = new.group_events.clone();
            assert_eq!(expected, *new, "质量/锚点/依赖/PM逐档数量保持");
            assert!(std::sync::Arc::ptr_eq(&b.records, &new.group_events));
        }
    }
    assert_eq!(depth_facts, 1);
    let last = compact.last().unwrap();
    let up = last.observations[0]
        .assets
        .iter()
        .find(|a| a.asset.token_id.0 == "1")
        .unwrap();
    assert_eq!(
        up.asks.value.as_ref().unwrap()[0].quantity_shares,
        ExactDecimal("80".into())
    );
}
