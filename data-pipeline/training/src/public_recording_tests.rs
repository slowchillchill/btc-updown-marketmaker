use super::*;
use crate::{hidden_preparation::*, public_replay::*};
use data_pipeline_core::{public_observation_core::*, public_reference::*, typed_raw::*};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
const CONDITION_ID: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const CLOB_MARKET_ID: &str = "3195576";
const UP_TOKEN_ID: &str = "20";
const DOWN_TOKEN_ID: &str = "3";
const SNAPSHOT_RECEIVED_AT: i64 = 10_000;
const MARKET_START_AT: i64 = 20_000;
const MARKET_END_AT: i64 = 30_000;
fn market() -> MarketIdentityV2 {
    MarketIdentityV2 {
        condition_id: ConditionId(CONDITION_ID.to_owned()),
        clob_market_id: ClobMarketId(CLOB_MARKET_ID.to_owned()),
        window: MarketWindowV2 {
            start_at_utc_ms: MARKET_START_AT,
            end_at_utc_ms: MARKET_END_AT,
        },
    }
}

fn metadata_snapshot_row() -> PolymarketMarketMetadataRow {
    let payload = r#"{"orderPriceMinTickSize":"0.01"}"#.to_owned();
    let payload_sha256 = format!("{:x}", Sha256::digest(payload.as_bytes()));
    PolymarketMarketMetadataRow {
        metadata_row_kind: PolymarketMetadataRowKind::MetadataSnapshot,
        timestamp_received: SNAPSHOT_RECEIVED_AT,
        collector_admission_sequence: 1,
        collector_admission_time_utc_ms: SNAPSHOT_RECEIVED_AT + 1,
        metadata_origin: PolymarketMetadataOrigin::GammaMarketsDiscovery,
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        schema_version: POLYMARKET_MARKET_METADATA_SCHEMA.to_owned(),
        slug: "btc-updown-5m".to_owned(),
        condition_id: CONDITION_ID.to_owned(),
        clob_market_id: CLOB_MARKET_ID.to_owned(),
        asset_ids: format!(r#"["{UP_TOKEN_ID}","{DOWN_TOKEN_ID}"]"#),
        market_start_utc: MARKET_START_AT,
        market_end_utc: MARKET_END_AT,
        fixed_identity_verified_at_utc: SNAPSHOT_RECEIVED_AT - 1,
        market_lifecycle_state: MetadataMarketLifecycleState::Prefetched,
        market_runtime_role: MetadataMarketRuntimeRole::Next,
        source_endpoint: Some("https://gamma-api.polymarket.com/markets".to_owned()),
        source_endpoint_unavailable_reason: None,
        source_evidence_origin: Some(MetadataSourceEvidenceOrigin::GammaMarketsDiscovery),
        source_evidence_event_type: Some(
            MetadataSourceEvidenceEventType::GammaMarketsDiscoveryResponse,
        ),
        source_evidence_channel: None,
        source_evidence_received_at_utc: Some(SNAPSHOT_RECEIVED_AT),
        row_evidence_validation_completed_at_utc: Some(SNAPSHOT_RECEIVED_AT + 1),
        source_evidence_payload_json: Some(payload),
        source_evidence_raw_sha256: Some(payload_sha256),
        market_lifecycle_transition: None,
        lifecycle_scheduled_at_utc: None,
        lifecycle_applied_at_utc: None,
        market_runtime_role_transition: None,
        runtime_role_scheduled_at_utc: None,
        runtime_role_applied_at_utc: None,
        metadata_fetch_start_utc: Some(SNAPSHOT_RECEIVED_AT - 10),
        closed: Some(false),
        closed_time_raw: None,
        closed_time_utc: None,
        closed_time_check_failure_reason: None,
        automatically_resolved: None,
        uma_resolution_status: None,
        outcomes_raw: Some(r#"["UP","DOWN"]"#.to_owned()),
        outcome_prices_raw: Some(r#"["0.5","0.5"]"#.to_owned()),
        active: Some(true),
        active_unavailable_reason: None,
        accepting_orders: Some(true),
        ws_platform_timestamp_raw: None,
        ws_platform_timestamp_utc: None,
        winning_asset_id: None,
        winning_outcome: None,
        resolution_evidence_source: None,
        terminal_claim_owner: None,
        resolution_poll_attempt_ordinal: None,
        resolution_poll_scheduled_utc: None,
        resolution_poll_result: None,
        resolution_transport_error_class: None,
        resolution_transport_http_status: None,
        resolution_integrity_error_class: None,
        resolution_budget_horizon_utc: None,
        resolution_followup_status: None,
        resolution_poll_attempts_recorded: None,
        resolution_valid_response_count: None,
        resolution_transport_unavailable_count: None,
        resolution_missed_slot_count: None,
        last_valid_metadata_received_utc: None,
        terminal_predicate_failure_reason: None,
        response_fields_unavailable_reason: None,
        ws_claimant_source_sequence_epoch_id: None,
        ws_claimant_collector_admission_sequence: None,
    }
}

fn row(
    epoch: &str,
    sequence: u64,
    source_time: i64,
    received_time: i64,
    event: PolymarketOrderbookEvent,
) -> PolymarketOrderbookRow {
    PolymarketOrderbookRow {
        timestamp_received: received_time,
        timestamp: Some(source_time),
        market: market().condition_id.0,
        slug: Some("btc-updown-5m".to_owned()),
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
        schema_version: POLYMARKET_ORDERBOOK_SCHEMA.to_owned(),
        source_sequence_epoch_id: epoch.to_owned(),
        collector_admission_sequence: sequence,
        collector_admission_time_utc_ms: received_time + 1,
        event,
    }
}

pub(crate) fn book(quantity: &str) -> PolymarketOrderbookEvent {
    PolymarketOrderbookEvent::Book {
        asset_id: UP_TOKEN_ID.to_owned(),
        bids: vec![PolymarketPriceLevel {
            price: "0.5".to_owned(),
            size: quantity.to_owned(),
        }],
        asks: vec![PolymarketPriceLevel {
            price: "0.6".to_owned(),
            size: "80".to_owned(),
        }],
        last_trade_price: Some("0.55".to_owned()),
    }
}

pub(crate) fn change(quantity: Option<&str>) -> PolymarketPriceChangeEntry {
    PolymarketPriceChangeEntry {
        asset_id: UP_TOKEN_ID.to_owned(),
        price: "0.5".to_owned(),
        size: quantity.map(str::to_owned),
        side: PolymarketSide::Buy,
        best_bid: None,
        best_ask: None,
    }
}

fn anchor_input() -> data_pipeline_core::public_input::MarketAnchorRuleUpdateInput {
    serde_json::from_value(serde_json::json!({
        "schema_version":"polymarket_market_metadata_v6", "timestamp_received":2000,
        "collector_admission_sequence":1, "collector_admission_time_utc_ms":2001,
        "metadata_origin":"gamma_event_by_slug", "collector_host_id":"host", "collector_region":"hk",
        "slug":"test", "condition_id":"condition", "clob_market_id":"123", "asset_ids":"[\"1\",\"2\"]",
        "market_start_utc":1000,"market_end_utc":301000,"fixed_identity_verified_at_utc":900,
        "market_lifecycle_state":"active","market_runtime_role":"current","metadata_fetch_start_utc":1990,
        "anchor_query_source_epoch_id":"query-epoch","anchor_query_ordinal":1,"anchor_query_result":"observed",
        "price_to_beat_decimal":"64950.01421851144","price_to_beat_source_field":"eventMetadata.priceToBeat",
        "twap_enabled":true,"twap_lookback_seconds":30,"http_received_at_utc":2000,
        "http_request_url":"https://gamma-api.polymarket.com/events/slug/test",
        "http_body_sha256":"123"
    })).unwrap()
}

pub(crate) fn twap_input(
    window_s: u64,
) -> data_pipeline_core::public_input::PolymarketRtdsChainlinkTwapInput {
    use data_pipeline_core::typed_raw::{PolymarketRtdsChainlinkBtcUsdRow, RtdsFreshnessStatus};
    data_pipeline_core::public_input::PolymarketRtdsChainlinkTwapInput {
        common: PolymarketRtdsChainlinkBtcUsdRow {
            timestamp_received: 2000,
            message_timestamp: 1999,
            payload_timestamp: 1998,
            age_ms: Some(2),
            symbol: "btc/usd".to_owned(),
            value: "64950.01".to_owned(),
            topic: if window_s == 30 {
                "crypto_prices_twap_thirty"
            } else {
                "crypto_prices_twap_sixty"
            }
            .to_owned(),
            message_type: "update".to_owned(),
            freshness_status: RtdsFreshnessStatus::Fresh,
            subscription_filter: "{}".to_owned(),
            source_native_payload_json: format!("{{\"window_s\":{window_s}}}"),
            collector_host_id: "host".to_owned(),
            collector_region: "hk".to_owned(),
            source_endpoint: "wss://ws-live-data.polymarket.com".to_owned(),
            schema_version: "polymarket_rtds_chainlink_twap_v1".to_owned(),
        },
        full_accuracy_value: "64950014218511440012345".to_owned(),
        window_s,
        source_sequence_epoch_id: "rtds-epoch".to_owned(),
        collector_admission_sequence: 1,
        collector_admission_time_utc_ms: 2001,
    }
}

pub(crate) fn core() -> PublicObservationCoreV1 {
    PublicObservationCoreV1::new(PublicInputStreamHeaderV1 {
        input_stream_id: "core-test".to_owned(),
        canonical_rule_version: "rule-v1".to_owned(),
        observation_version: "public-observation-v1".to_owned(),
        ordering_version: "original-input-v1".to_owned(),
        input_scope: "constructed-finite".to_owned(),
        max_pending_ws_terminals: 2,
        initial_state: PublicInitialStateV1::Empty,
    })
    .unwrap()
}

pub(crate) fn record(position: u64, row: TypedRawRowV2) -> RecordedIngressV1 {
    let source_evidence = source_evidence_v3_from_typed_raw_v2(&row).unwrap();
    RecordedIngressV1 {
        input_stream_id: "core-test".to_owned(),
        input_position: position,
        ingress: DptIngressV1::Message {
            row,
            source_evidence,
        },
        dependency_record_refs: vec![],
    }
}

pub(crate) fn meta(position: u64) -> RecordedIngressV1 {
    record(
        position,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
            metadata_snapshot_row(),
        ))),
    )
}

pub(crate) fn pm(
    position: u64,
    seq: u64,
    source: i64,
    event: PolymarketOrderbookEvent,
) -> RecordedIngressV1 {
    record(
        position,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
            "epoch-a", seq, source, 22_000, event,
        ))),
    )
}

pub(crate) fn twap_record(
    position: u64,
    window: u64,
    received: i64,
    price: &str,
) -> RecordedIngressV1 {
    let mut row = twap_input(window);
    row.common.timestamp_received = received;
    row.common.payload_timestamp = received - 2;
    row.common.message_timestamp = received - 1;
    row.common.age_ms = Some(2);
    row.collector_admission_time_utc_ms = received + 1;
    row.collector_admission_sequence = position + 1;
    row.full_accuracy_value = price.into();
    record(position, TypedRawRowV2::PolymarketRtdsChainlinkTwap(row))
}

pub(crate) fn rule_record(position: u64, window: u32, price: Option<&str>) -> RecordedIngressV1 {
    let mut row = anchor_input();
    row.condition_id = CONDITION_ID.into();
    row.clob_market_id = CLOB_MARKET_ID.into();
    row.asset_ids = format!("[\"{UP_TOKEN_ID}\",\"{DOWN_TOKEN_ID}\"]");
    row.market_start_utc = MARKET_START_AT;
    row.market_end_utc = MARKET_END_AT;
    row.twap_lookback_seconds = Some(window);
    row.price_to_beat_decimal = price.map(str::to_owned);
    row.timestamp_received = 22_000 + position as i64;
    row.collector_admission_sequence = position + 1;
    row.collector_admission_time_utc_ms = row.timestamp_received + 1;
    row.anchor_query_ordinal = position + 1;
    row.http_received_at_utc = Some(row.timestamp_received);
    record(
        position,
        TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
            PolymarketMarketMetadataV6Input::AnchorRule(row),
        )),
    )
}
pub(crate) fn config() -> GateAOutputConfig {
    let mut config = crate::gate_a_formal_output_config();
    config.sort.batch_rows = 2;
    config.sort.max_run_rows = 4;
    config.sort.max_run_bytes = 1 << 20;
    config.max_artifact_payload_bytes = 1 << 19;
    config
}
pub(crate) fn end(position: u64) -> RecordedIngressV1 {
    RecordedIngressV1 {
        input_stream_id: "core-test".into(),
        input_position: position,
        ingress: DptIngressV1::EndOfInput,
        dependency_record_refs: vec![],
    }
}
pub(crate) fn apply(
    core: &mut PublicObservationCoreV1,
    input: &RecordedIngressV1,
) -> PublicAppliedIngressV1<'static> {
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_v1(core, input, &ids, &[]).unwrap();
    core.push(&prepared).unwrap().into_owned()
}
fn all_rows(root: &Path, rows: usize) -> Vec<PublicStoredRowV1> {
    let mut reader = PublicRecordingReaderV1::recover(root, rows, 1 << 20).unwrap();
    let mut all = Vec::new();
    while let Some(row) = reader.next_row().unwrap() {
        all.push(row)
    }
    all
}
#[test]
fn delayed_materialization_unique_records_and_every_group_replay_match() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("recording");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let first = meta(0);
    let result = apply(&mut core, &first);
    let mut expected = vec![fingerprint(&result)];
    writer.record(first, result).unwrap();
    let input = pm(1, 1, 21_000, book("100"));
    let result = apply(&mut core, &input);
    let golden = materialize_pm_step_identity_v1(result.pm_reductions[0].1.clone())
        .unwrap()
        .bindings;
    expected.push(fingerprint(&result));
    let next = pm(
        2,
        2,
        21_001,
        PolymarketOrderbookEvent::PriceChange {
            price_changes: vec![change(Some("80"))],
        },
    );
    let advanced = apply(&mut core, &next);
    expected.push(fingerprint(&advanced));
    // 来源输入和准备借用已释放，核心已经推进；后台仍物化同次旧前量。
    let input_copy = input.clone();
    drop(input);
    writer.record(input_copy, result).unwrap();
    writer.record(next, advanced).unwrap();
    assert_eq!(core.applied_prefix().next_input_position, 3);
    assert!(writer.committed_prefix().is_none());
    assert!(all_rows(&root, 1).is_empty());
    let terminal = end(3);
    let result = apply(&mut core, &terminal);
    expected.push(fingerprint(&result));
    writer.record(terminal, result).unwrap();
    let prefix = writer.stabilize().unwrap().unwrap();
    assert_eq!(prefix.next_input_position, 4);
    assert_eq!(prefix.next_canonical_position, 7);
    assert!(prefix.normal_end);
    let rows = all_rows(&root, 1);
    let bindings: Vec<_> = rows
        .iter()
        .filter_map(|r| {
            if let PublicStoredRowV1::PmStepBinding(b) = r {
                Some(b)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        &bindings[..golden.len()],
        &golden.iter().collect::<Vec<_>>()
    );
    assert_eq!(
        rows.iter()
            .filter(|r| matches!(r, PublicStoredRowV1::Canonical { .. }))
            .count(),
        7
    );
    for page in [1, 3, 20] {
        let mut replay = PublicReplayV1::open(&root, page, 1 << 20).unwrap();
        let mut got = Vec::new();
        while let Some((_, result)) = replay.next_applied().unwrap() {
            got.push(fingerprint(&result));
        }
        assert_eq!(got, expected);
    }
}
#[test]
fn failures_do_not_publish_partial_input_or_index_and_old_prefix_remains() {
    for fault in ["after_input", "before_publish", "after_rename"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("recording");
        let mut core = core();
        let mut writer =
            PublicRecordingWriterV1::create_historical(&root, core.header(), config()).unwrap();
        let input = meta(0);
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
        writer.stabilize().unwrap();
        let old = serde_json::to_value(all_rows(&root, 1)).unwrap();
        let input = record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot("depth", 100, 1))),
        );
        let result = apply(&mut core, &input);
        faults::set(fault);
        if fault == "after_input" {
            assert!(writer.record(input, result).is_err());
            assert!(writer.stabilize().is_err());
        } else {
            writer.record(input, result).unwrap();
            assert!(writer.stabilize().is_err());
        }
        assert_eq!(writer.committed_prefix().unwrap().next_input_position, 1);
        assert_eq!(serde_json::to_value(all_rows(&root, 1)).unwrap(), old);
    }
}
#[test]
fn malformed_stable_parquet_is_an_error_not_finite_eof() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("recording");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let input = meta(0);
    let result = apply(&mut core, &input);
    writer.record(input, result).unwrap();
    let p = writer.stabilize().unwrap().unwrap();
    fs::write(
        root.join(p.stable_storage_ref).join("captured.parquet"),
        b"broken",
    )
    .unwrap();
    let mut reader = PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap();
    assert!(reader.next_row().is_err());
}
#[test]
fn hidden_buckets_cross_pages_and_reference_only_buckets_close_globally() {
    let temp = tempfile::tempdir().unwrap();
    let mut snapshot = snapshot("depth", 100, 1);
    snapshot.timestamp_received = 26_000;
    snapshot.collector_admission_time_utc_ms = 26_001;
    let mut first = update(
        "depth",
        101,
        101,
        2,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![level("64100", "0.2")],
    );
    first.timestamp_received = 26_002;
    first.collector_admission_time_utc_ms = 26_003;
    first.event_time = 24_000;
    let mut second = update(
        "depth",
        102,
        102,
        3,
        Some(100),
        Some(101),
        BinanceDepthStitchVerdict::Applied,
        vec![level("64100", "0.3")],
    );
    second.timestamp_received = 26_004;
    second.collector_admission_time_utc_ms = 26_005;
    second.event_time = 23_000;
    let inputs = [
        meta(0),
        pm(1, 1, 21_000, book("100")),
        pm(
            2,
            2,
            21_000,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("80"))],
            },
        ),
        twap_record(3, 60, 25_002, "65000000000000000000000"),
        record(
            4,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot)),
        ),
        record(
            5,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(first)),
        ),
        record(
            6,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(second)),
        ),
        end(7),
    ];
    let mut all_indices = Vec::new();
    let mut all_buckets = Vec::new();
    for historical in [false, true] {
        let root = temp
            .path()
            .join(if historical { "compact" } else { "legacy" });
        let mut core = core();
        let mut writer = if historical {
            PublicRecordingWriterV1::create_historical(&root, core.header(), config())
        } else {
            PublicRecordingWriterV1::create(&root, core.header(), config())
        }
        .unwrap();
        let mut expected_replay = Vec::new();
        for input in &inputs {
            let result = apply(&mut core, input);
            expected_replay.push(fingerprint(&result));
            writer.record(input.clone(), result).unwrap();
            if historical && [0, 3].contains(&input.input_position) {
                writer
                    .writer
                    .as_mut()
                    .unwrap()
                    .split_part_for_test()
                    .unwrap();
            }
        }
        writer.stabilize().unwrap();
        let mut replay = PublicReplayV1::open(&root, 1, 1 << 20).unwrap();
        let mut actual_replay = Vec::new();
        while let Some((_, result)) = replay.next_applied().unwrap() {
            actual_replay.push(fingerprint(&result));
        }
        assert_eq!(actual_replay, expected_replay);
        let rows = all_rows(&root, 1);
        let expected_descriptions: Vec<_> = rows
            .iter()
            .filter_map(|row| match row {
                PublicStoredRowV1::BinanceDepthReference {
                    canonical_position,
                    binding,
                    time,
                } if time.source_event_at_utc_ms.is_some() => {
                    Some((*canonical_position, binding.clone(), time.clone()))
                }
                _ => None,
            })
            .collect();
        let expected_references: Vec<_> = rows
            .iter()
            .filter_map(|row| match row {
                PublicStoredRowV1::Canonical {
                    canonical_position,
                    record: CanonicalEventRecordV2::ReferenceEvent(event),
                    ..
                } if event.time.source_event_at_utc_ms.is_some() => Some((
                    *canonical_position,
                    serde_json::to_value(CanonicalEventRecordV2::ReferenceEvent(event.clone()))
                        .unwrap(),
                )),
                _ => None,
            })
            .collect();
        let reader = PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap();
        let prepared = prepare_hidden_v1(
            core,
            reader,
            &temp.path().join(if historical {
                "hidden-compact"
            } else {
                "hidden-legacy"
            }),
            &config(),
            "hidden-test".into(),
        )
        .unwrap();
        let mut prepared = prepared;
        if historical {
            // 小夹具仅在物理层切片：首个同时间桶和派生步骤跨文件仍是连续输入。
            for (label, path) in [
                ("split-index", &mut prepared.index_path),
                ("split-derived", &mut prepared.derived_path),
            ] {
                let mut cursor = crate::PartCursor::open(
                    &crate::SortedRun::read_parts(path).unwrap(),
                    1,
                    1 << 20,
                )
                .unwrap();
                let mut writer = crate::gate_a::SequentialCaptureWriter::create(
                    label,
                    &temp.path().join(label),
                    std::sync::Arc::new(std::sync::Mutex::new(0)),
                    &config(),
                )
                .unwrap();
                let mut sequence = 0;
                while let Some(row) = cursor.next_row().unwrap() {
                    writer
                        .push(sequence, row.sort_key_bytes().to_vec(), row.payload())
                        .unwrap();
                    if sequence == 0 {
                        writer.split_part_for_test().unwrap();
                    }
                    sequence += 1;
                }
                *path = writer.close_segment().unwrap();
                assert_eq!(crate::SortedRun::read_parts(path).unwrap().parts.len(), 2);
            }
        }
        let mut index = HiddenIndexReaderV1::open(&prepared, 1, 1 << 20).unwrap();
        let mut indices = Vec::new();
        let mut references = Vec::new();
        while let Some((position, entry)) = index.next_entry().unwrap() {
            if let HiddenIndexEntryV1::Reference {
                canonical_position,
                source_at_utc_ms,
                derivation_position,
            } = &entry
            {
                references.push((
                    position,
                    *canonical_position,
                    *source_at_utc_ms,
                    *derivation_position,
                ));
            }
            indices.push(serde_json::to_value(entry).unwrap());
        }
        assert_eq!(
            references
                .iter()
                .map(|r| (r.1, r.2, r.3))
                .collect::<Vec<_>>(),
            [(9, 24_000, 1), (10, 24_000, 2), (7, 25_000, 0)],
            "源时间归并须保留原规范位置和派生顺序"
        );
        let mut snapshots = Vec::new();
        for page in [1, 2, 20] {
            let mut reader = HiddenBucketReaderV1::open(&prepared, page, 1 << 20).unwrap();
            let mut buckets = Vec::new();
            while let Some(bucket) = reader.next_bucket().unwrap() {
                buckets.push(bucket);
            }
            snapshots.push(buckets);
        }
        assert_eq!(snapshots[0], snapshots[1]);
        assert_eq!(snapshots[0], snapshots[2]);
        let buckets = &snapshots[0];
        assert!(
            buckets
                .iter()
                .any(|b| b.bucket_time_utc_ms == 21_000 && b.steps.len() > 2)
        );
        let depth = buckets
            .iter()
            .find(|b| b.bucket_time_utc_ms == 24_000)
            .unwrap();
        assert!(depth.steps.is_empty());
        assert_eq!(depth.reference_positions.len(), 2, "纯参考桶不能丢失");
        assert_eq!(
            depth.closure,
            ReplayIndexClosureV1::NextGlobalEventAt(25_000)
        );
        let last = buckets.last().unwrap();
        assert!(last.steps.is_empty());
        assert_eq!(last.reference_positions, vec![references.last().unwrap().0]);
        assert_eq!(last.closure, ReplayIndexClosureV1::EndOfInput);
        for page_rows in [1, 2, 20] {
            let mut material =
                HiddenMaterialReaderV1::open(&prepared, &root, page_rows, 1 << 20).unwrap();
            let mut positions = Vec::new();
            let mut descriptions = Vec::new();
            let mut actual_references = Vec::new();
            loop {
                let page = material.next_page().unwrap();
                if page.is_empty() {
                    break;
                }
                for (position, record) in page {
                    positions.push(position);
                    match record {
                        ResolvedHiddenRecordV1::BinanceDepthReference {
                            canonical_position,
                            binding,
                            time,
                        } => descriptions.push((canonical_position, binding, time)),
                        ResolvedHiddenRecordV1::Reference {
                            canonical_position,
                            record,
                        } => actual_references
                            .push((canonical_position, serde_json::to_value(record).unwrap())),
                        _ => {}
                    }
                }
            }
            assert_eq!(positions, (0..last.index_end).collect::<Vec<_>>());
            assert_eq!(descriptions, expected_descriptions);
            actual_references.sort_by_key(|r| r.0);
            assert_eq!(actual_references, expected_references);
            assert!(material.metrics().decoded_derived_rows > 0);
            assert!(material.metrics().decoded_recording_rows > 0);
            assert_eq!(material.metrics().index_entries, last.index_end);
            assert!(material.metrics().peak_page_entries <= page_rows);
            let completed = material.metrics().clone();
            assert!(material.next_page().unwrap().is_empty());
            assert_eq!(material.metrics(), &completed);
        }
        // derivation_position相同不足以配对：实际索引与派生步骤的完整引用必须一致。
        let candidate = temp.path().join(if historical {
            "mixed-compact-index"
        } else {
            "mixed-legacy-index"
        });
        let mut writer = crate::gate_a::SequentialCaptureWriter::create(
            "mixed-index",
            &candidate,
            std::sync::Arc::new(std::sync::Mutex::new(0)),
            &config(),
        )
        .unwrap();
        let mut original_index = crate::PartCursor::open(
            &crate::SortedRun::read_parts(&prepared.index_path).unwrap(),
            2,
            1 << 20,
        )
        .unwrap();
        let mut changed = false;
        let mut sequence = 0;
        while let Some(row) = original_index.next_row().unwrap() {
            let mut entry: HiddenIndexEntryV1 = serde_json::from_slice(row.payload()).unwrap();
            if !changed && let HiddenIndexEntryV1::Step(reference) = &mut entry {
                reference.step_index += 1;
                changed = true;
            }
            writer
                .push(
                    sequence,
                    row.sort_key_bytes().to_vec(),
                    &serde_json::to_vec(&entry).unwrap(),
                )
                .unwrap();
            sequence += 1;
        }
        assert!(changed);
        let mut mixed = prepared.clone();
        mixed.index_path = writer.close_segment().unwrap();
        let mut reader = HiddenMaterialReaderV1::open(&mixed, &root, 2, 1 << 20).unwrap();
        let error = match reader.next_page() {
            Err(error) => error,
            Ok(_) => panic!("完整隐藏引用不同不能消费成功"),
        };
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(
            error.to_string(),
            "隐藏步骤完整引用或consumer身份与索引不符"
        );
        all_indices.push(indices);
        all_buckets.push(snapshots.remove(0));
    }
    assert_eq!(all_indices[0], all_indices[1]);
    assert_eq!(all_buckets[0], all_buckets[1]);
}

#[test]
fn source_heads_preserve_real_within_source_order_before_receive_ties() {
    let temp = tempfile::tempdir().unwrap();
    let mut inputs = Vec::new();
    for r in [
        record(
            2,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                2,
                9001,
                9002,
                book("80"),
            ))),
        ),
        meta(0),
        record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-a",
                1,
                9000,
                10000,
                book("100"),
            ))),
        ),
    ] {
        if let DptIngressV1::Message { row, .. } = r.ingress {
            inputs.push(row)
        }
    }
    let mut limits = crate::gate_a_formal_canonical_config();
    limits.sort = config().sort;
    let mut merge =
        prepare_historical_rows_v1(&temp.path().join("sort"), &limits, "history".into(), || {
            Ok(inputs.clone().into_iter().map(Ok))
        })
        .unwrap();
    let mut times = Vec::new();
    while let Some(input) = merge.next_input().unwrap() {
        if let DptIngressV1::Message {
            source_evidence, ..
        } = input.ingress
        {
            times.push(source_evidence.received_at_utc_ms)
        }
    }
    assert_eq!(times, vec![10000, 10000, 9002]);
}
#[test]
fn configuration_late_dependency_pages_rebuild_the_same_first_reference() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("recording");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let input = meta(0);
    let result = apply(&mut core, &input);
    writer.record(input, result).unwrap();
    let mut candidates = Vec::new();
    let mut expected = Vec::new();
    for input in [
        twap_record(1, 60, 21_002, "65000000000000000000000"),
        twap_record(2, 60, 21_003, "66000000000000000000000"),
    ] {
        let ids = BTreeMap::new();
        let prepared = prepare_public_ingress_v1(&core, &input, &ids, &[]).unwrap();
        let PreparedCanonicalInputV1::ChainlinkTwap(event) = &prepared.current else {
            panic!()
        };
        let result = core.push(&prepared).unwrap().into_owned();
        let fact = &result.applied.closed_groups[0].records[0];
        candidates.push(TemporaryReferenceCandidateV1 {
            canonical_fact: event.clone(),
            record_ref: CanonicalInputRecordRefV1 {
                input_stream_id: "core-test".into(),
                input_position: input.input_position,
            },
            anchor: DirectObservationAnchorV1 {
                field_path: "chainlink_twap_sixty".into(),
                position: fact.position.clone(),
                group_id: prepared.prepared_group_id.clone().unwrap(),
                source_message_id: fact.direct_sources[0].source_message_id.clone(),
                source_event_at_utc_ms: fact.direct_sources[0].source_event_at_utc_ms,
                received_at_utc_ms: fact.direct_sources[0].received_at_utc_ms,
            },
        });
        expected.push(fingerprint(&result));
        drop(prepared);
        writer.record(input, result).unwrap();
    }
    let mut input = rule_record(3, 60, None);
    let ids = BTreeMap::new();
    let prepared = prepare_public_ingress_with_reference_v1(
        &core,
        &mut input,
        &ids,
        &[],
        &market(),
        |cursor| {
            let values = candidates
                .iter()
                .filter(|v| v.record_ref.input_position >= cursor.next_input_position)
                .cloned()
                .collect();
            Ok((
                values,
                TemporaryReferenceCursorV1 {
                    next_input_position: 3,
                },
                true,
            ))
        },
    )
    .unwrap();
    let result = core.push(&prepared).unwrap().into_owned();
    drop(prepared);
    assert_eq!(input.dependency_record_refs[0].input_position, 1);
    expected.push(fingerprint(&result));
    writer.record(input, result).unwrap();
    let input = end(4);
    let result = apply(&mut core, &input);
    writer.record(input, result).unwrap();
    writer.stabilize().unwrap();
    for page in [1, 4] {
        let mut replay = PublicReplayV1::open(&root, page, 1 << 20).unwrap();
        let mut results = Vec::new();
        while let Some((input, result)) = replay.next_applied().unwrap() {
            if (1..=3).contains(&input.input_position) {
                results.push(fingerprint(&result));
            }
        }
        assert_eq!(results, expected);
    }
}
#[test]
fn hidden_typed_steps_keep_cross_epoch_predecessor_counterexample_and_missing_times() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("recording");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let mut anomaly = twap_input(60);
    anomaly.common.freshness_status = RtdsFreshnessStatus::InvalidTimestamp;
    for input in [
        meta(0),
        pm(1, 1, 20_050, book("10")),
        pm(
            2,
            2,
            20_200,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("20"))],
            },
        ),
        record(
            3,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
                "epoch-b",
                1,
                20_100,
                23_000,
                book("15"),
            ))),
        ),
        record(4, TypedRawRowV2::PolymarketRtdsChainlinkTwap(anomaly)),
        end(5),
    ] {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    let reader = PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap();
    let prepared = prepare_hidden_v1(
        core,
        reader,
        &temp.path().join("hidden"),
        &config(),
        "hidden-test".into(),
    )
    .unwrap();
    let mut reader = HiddenStepReaderV1::open(&prepared, 1, 1 << 20).unwrap();
    let mut changes = BTreeMap::new();
    while let Some(row) = reader.next_row().unwrap() {
        if let HiddenStoredRowV1::Step(s) = row {
            if let PmStepChangeV1::LevelChange { transition, .. } = s.step.change {
                changes.insert(s.reference.derivation_position, transition);
            }
        }
    }
    let mut buckets = HiddenBucketReaderV1::open(&prepared, 1, 1 << 20).unwrap();
    let mut order = Vec::new();
    while let Some(bucket) = buckets.next_bucket().unwrap() {
        assert!(bucket.reference_positions.is_empty());
        for step in bucket.steps {
            if let Some(change) = changes.get(&step.derivation_position) {
                order.push((
                    bucket.bucket_time_utc_ms,
                    change.quantity_before_shares.0.clone(),
                    change.quantity_after_shares.0.clone(),
                ));
            }
        }
    }

    // 两个真实步骤及原时间原封保留，不通过删除或钳时间改变外排结果。
    assert!(
        order
            .iter()
            .any(|(_, before, after)| before == "10" && after == "20")
    );
    assert!(
        order
            .iter()
            .any(|(_, before, after)| before == "0" && after == "15")
    );
    let recovery = order
        .iter()
        .position(|(_, b, a)| b == "0" && a == "15")
        .unwrap();
    let old = order
        .iter()
        .position(|(_, b, a)| b == "10" && a == "20")
        .unwrap();
    assert!(recovery < old);
    assert_ne!(order[recovery].2, order[old].1); // 消费时真实前量15与原步骤before10冲突，不改写材料。
    // 外排时间使索引位置真实倒退到更早派生记录；分页不得跳过或重排。
    let mut originals = BTreeMap::new();
    let mut reader = HiddenStepReaderV1::open(&prepared, 2, 1 << 20).unwrap();
    while let Some(row) = reader.next_row().unwrap() {
        if let HiddenStoredRowV1::Step(step) = row {
            originals.insert(
                step.reference.derivation_position,
                serde_json::to_value(step).unwrap(),
            );
        }
    }
    let mut index = HiddenIndexReaderV1::open(&prepared, 2, 1 << 20).unwrap();
    let mut expected = Vec::new();
    while let Some((position, entry)) = index.next_entry().unwrap() {
        if let HiddenIndexEntryV1::Step(step) = entry {
            expected.push((position, step.derivation_position));
        }
    }
    assert!(expected.windows(2).any(|pair| pair[0].1 > pair[1].1));
    for page_rows in [1, 2, 20] {
        let mut reader =
            HiddenMaterialReaderV1::open(&prepared, &root, page_rows, 1 << 20).unwrap();
        let mut actual = Vec::new();
        let mut previous_scanned = 0;
        loop {
            let page = reader.next_page().unwrap();
            if page.is_empty() {
                break;
            }
            assert!(reader.metrics().decoded_recording_rows >= previous_scanned);
            previous_scanned = reader.metrics().decoded_recording_rows;
            for (position, value) in page {
                let ResolvedHiddenRecordV1::Step {
                    stored,
                    market: observed_market,
                } = value
                else {
                    panic!()
                };
                assert_eq!(observed_market, market());
                let derived = stored.reference.derivation_position;
                assert_eq!(serde_json::to_value(stored).unwrap(), originals[&derived]);
                actual.push((position, derived));
            }
        }
        assert_eq!(actual, expected);
    }
}
#[test]
fn abort_and_binding_write_failure_cannot_become_normal_stable_end() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("recording");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let input = meta(0);
    let result = apply(&mut core, &input);
    writer.record(input, result).unwrap();
    writer.stabilize().unwrap();
    let aborted = RecordedIngressV1 {
        input_stream_id: "core-test".into(),
        input_position: 1,
        ingress: DptIngressV1::Aborted {
            reason: "source stopped".into(),
        },
        dependency_record_refs: vec![],
    };
    assert!(
        prepare_public_ingress_v1(&core, &aborted, &BTreeMap::new(), &[])
            .and_then(|p| core.push(&p).map(|_| ()).map_err(|e| {
                data_pipeline_core::ContractError::InvalidField {
                    field: "abort".into(),
                    reason: e.to_string(),
                }
            }))
            .is_err()
    );
    writer.record_aborted(aborted).unwrap();
    assert!(writer.stabilize().is_err());
    let reader = PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap();
    assert!(!reader.prefix.as_ref().unwrap().normal_end);
    let header = core.header().clone();
    assert!(
        prepare_hidden_v1(
            core,
            reader,
            &temp.path().join("hidden"),
            &config(),
            "h".into()
        )
        .is_err()
    );
    let root = temp.path().join("binding_failure");
    let mut limits = config();
    limits.max_artifact_payload_bytes = 1;
    let mut writer = PublicRecordingWriterV1::create(&root, &header, limits).unwrap();
    assert!(
        writer
            .record_source_binding(SourceArtifactBindingV1 {
                source_message_id: Sha256Hex("1".repeat(64)),
                manifest_sha256: Sha256Hex("2".repeat(64)),
                file_sha256: Sha256Hex("3".repeat(64)),
                relative_path: "source.parquet".into()
            })
            .is_err()
    );
    assert!(writer.stabilize().is_err());
    assert!(all_rows(&root, 1).is_empty());
}

fn fingerprint(result: &PublicAppliedIngressV1<'_>) -> serde_json::Value {
    serde_json::json!({"applied":result.applied,"pm":result.pm_reductions,
        "reductions":result.reductions.iter().map(|r|serde_json::json!({"facts":r.facts,"reports":r.reports})).collect::<Vec<_>>()})
}

fn level(price: &str, quantity: &str) -> DepthLevel {
    DepthLevel {
        price: price.to_owned(),
        quantity: quantity.to_owned(),
    }
}

pub(crate) fn snapshot(
    epoch: &str,
    last_update_id: u64,
    admission_sequence: u64,
) -> BinanceDepthSnapshotRow {
    BinanceDepthSnapshotRow {
        timestamp_received: 2_000 + admission_sequence as i64,
        snapshot_fetch_start_utc: 900,
        last_update_id,
        bids: vec![level("64000", "0.5"), level("64100", "0.7")],
        asks: vec![level("64300", "0.3"), level("64200", "0.4")],
        symbol: "BTCUSDT".to_owned(),
        source_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
            .to_owned(),
        snapshot_status: "complete".to_owned(),
        source_sequence_epoch_id: epoch.to_owned(),
        connection_generation: 1,
        depth_ingress_sequence: admission_sequence,
        collector_admission_sequence: admission_sequence,
        collector_admission_time_utc_ms: 2_100 + admission_sequence as i64,
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        schema_version: "binance_depth_snapshot_v3".to_owned(),
    }
}

pub(crate) fn update(
    epoch: &str,
    first_update_id: u64,
    final_update_id: u64,
    admission_sequence: u64,
    snapshot_last_update_id: Option<u64>,
    local_u_before: Option<u64>,
    verdict: BinanceDepthStitchVerdict,
    bids: Vec<DepthLevel>,
) -> BinanceDepthUpdateRow {
    let (stitch_evidence, stitch_gap_reason) = match verdict {
        BinanceDepthStitchVerdict::Applied => (Some("continuous_depth_update".to_owned()), None),
        BinanceDepthStitchVerdict::NotAppliedStaleDuplicate => {
            (Some("u <= local_u".to_owned()), None)
        }
        BinanceDepthStitchVerdict::NotAppliedConfirmedHole => (
            Some("U > local_u + 1".to_owned()),
            Some("U > local_u + 1".to_owned()),
        ),
        BinanceDepthStitchVerdict::NotAppliedSnapshotUnavailable => (None, None),
    };
    BinanceDepthUpdateRow {
        timestamp_received: 2_000 + admission_sequence as i64,
        event_type: "depthUpdate".to_owned(),
        event_time: 1_500 + admission_sequence as i64,
        symbol: "BTCUSDT".to_owned(),
        first_update_id,
        final_update_id,
        bids,
        asks: Vec::new(),
        stream: "btcusdt@depth".to_owned(),
        snapshot_last_update_id,
        local_u_before,
        stitch_verdict: verdict,
        stitch_evidence,
        stitch_gap_reason,
        source_sequence_epoch_id: epoch.to_owned(),
        connection_generation: 1,
        depth_ingress_sequence: admission_sequence,
        collector_admission_sequence: admission_sequence,
        collector_admission_time_utc_ms: 2_100 + admission_sequence as i64,
        collector_host_id: "collector-1".to_owned(),
        collector_region: "eu-west-1".to_owned(),
        source_endpoint: "wss://stream.binance.com:9443/ws/btcusdt@depth".to_owned(),
        schema_version: "binance_depth_update_v4".to_owned(),
    }
}

#[test]
fn historical_depth_description_preserves_complete_input_and_replay_through_gap_and_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let mut inputs = vec![meta(0)];
    for row in [
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
    ] {
        inputs.push(record(inputs.len() as u64, TypedRawRowV2::Existing(row)));
    }
    inputs.push(end(inputs.len() as u64));
    let mut recordings = Vec::new();
    let mut prefixes = Vec::new();
    for historical in [false, true] {
        let root = temp.path().join(if historical {
            "historical"
        } else {
            "online-old-format"
        });
        let mut core = core();
        let mut writer = if historical {
            PublicRecordingWriterV1::create_historical(&root, core.header(), config())
        } else {
            PublicRecordingWriterV1::create(&root, core.header(), config())
        }
        .unwrap();
        let mut expected = Vec::new();
        let mut states = Vec::new();
        for input in &inputs {
            let result = apply(&mut core, input);
            for group in &result.applied.closed_groups {
                for observation in &group.observations {
                    states.push(observation.binance_depth.value.as_ref().map(|depth| {
                        (
                            depth.full().unwrap().last_update_id,
                            depth
                                .full()
                                .unwrap()
                                .bids
                                .iter()
                                .map(|l| (l.price_usdt_per_btc.0.clone(), l.quantity_btc.0.clone()))
                                .collect::<Vec<_>>(),
                        )
                    }));
                }
            }
            expected.push(fingerprint(&result));
            writer.record(input.clone(), result).unwrap();
        }
        assert_eq!(states[1].as_ref().unwrap().0, 100);
        assert!(
            states[2]
                .as_ref()
                .unwrap()
                .1
                .contains(&("64100".into(), "0.2".into()))
        );
        assert!(states[3].is_none(), "确认缺口必须使深度未知");
        assert_eq!(states[4].as_ref().unwrap().0, 200);
        prefixes.push(writer.stabilize().unwrap().unwrap());
        let rows = all_rows(&root, 1);
        let saved_inputs: Vec<_> = rows
            .iter()
            .filter_map(|r| match r {
                PublicStoredRowV1::Input(i) => Some(i.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(saved_inputs, inputs, "原始快照/增量和全部来源时序必须完整");
        for page in [1, 3] {
            let mut replay = PublicReplayV1::open(&root, page, 1 << 20).unwrap();
            let mut actual = Vec::new();
            while let Some((_, result)) = replay.next_applied().unwrap() {
                actual.push(fingerprint(&result));
            }
            assert_eq!(actual, expected, "公共事实、观察、数量、质量与来源时序");
        }
        recordings.push(rows);
    }
    assert_eq!(prefixes[0], prefixes[1]);
    let mut compact_count = 0;
    for (old, new) in recordings[0].iter().zip(&recordings[1]) {
        match (old, new) {
            (
                PublicStoredRowV1::Canonical {
                    canonical_position,
                    binding,
                    record: CanonicalEventRecordV2::ReferenceEvent(event),
                },
                PublicStoredRowV1::BinanceDepthReference {
                    canonical_position: new_position,
                    binding: new_binding,
                    time,
                },
            ) => {
                assert!(matches!(
                    event.payload,
                    CanonicalReferenceEventPayloadV2::BinanceDepthState(_)
                ));
                assert_eq!(canonical_position, new_position);
                assert_eq!(binding, new_binding);
                assert_eq!(&event.time, time);
                let bytes = serde_json::to_value(new).unwrap();
                let fields = bytes["BinanceDepthReference"].as_object().unwrap();
                assert_eq!(
                    fields.len(),
                    3,
                    "只保存位置、完整绑定和时间，不保存深度或谱系"
                );
                compact_count += 1;
            }
            _ => assert_eq!(
                serde_json::to_value(old).unwrap(),
                serde_json::to_value(new).unwrap()
            ),
        }
    }
    assert_eq!(recordings[0].len(), recordings[1].len());
    assert_eq!(compact_count, 4);
}

#[test]
fn compact_depth_original_position_mismatch_is_rejected_by_stable_reader() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("recording");
    let mut core = core();
    let input = record(
        0,
        TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot("depth", 100, 1))),
    );
    let result = apply(&mut core, &input);
    let mut writer =
        PublicRecordingWriterV1::create_historical(&root, core.header(), config()).unwrap();
    writer.record(input, result).unwrap();
    writer.stabilize().unwrap();
    let mut description = all_rows(&root, 1)
        .into_iter()
        .find(|r| matches!(r, PublicStoredRowV1::BinanceDepthReference { .. }))
        .unwrap();
    if let PublicStoredRowV1::BinanceDepthReference {
        canonical_position, ..
    } = &mut description
    {
        *canonical_position += 1;
    }
    let bad_root = temp.path().join("wrong-position");
    let mut writer =
        PublicRecordingWriterV1::create_historical(&bad_root, core.header(), config()).unwrap();
    writer.write(description).unwrap();
    writer.pending.next_canonical_position = 1;
    writer.stabilize().unwrap();
    let mut reader = PublicRecordingReaderV1::recover(&bad_root, 1, 1 << 20).unwrap();
    assert!(
        reader
            .next_row()
            .unwrap_err()
            .to_string()
            .contains("原序或流身份")
    );
}

#[test]
fn real_admission_coordinates_rank_epochs_and_share_snapshot_update_order() {
    let temp = tempfile::tempdir().unwrap();
    let mut snapshot_a = snapshot("depth-a", 100, 1);
    snapshot_a.collector_admission_time_utc_ms = 10001;
    let mut update_a = update(
        "depth-a",
        101,
        101,
        2,
        Some(100),
        None,
        BinanceDepthStitchVerdict::Applied,
        vec![level("65000", "2")],
    );
    update_a.collector_admission_time_utc_ms = 10002;
    let mut snapshot_b = snapshot("depth-b", 200, 1);
    snapshot_b.collector_admission_time_utc_ms = 11001;
    let rows = vec![
        TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot_b)),
        TypedRawRowV2::Existing(TypedRawRow::BinanceDepthUpdate(update_a)),
        TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot_a)),
    ];
    let mut limits = crate::gate_a_formal_canonical_config();
    limits.max_epoch_count_per_source = 2;
    limits.sort = config().sort;
    let mut outputs = Vec::new();
    for reverse in [false, true] {
        let mut rows = rows.clone();
        if reverse {
            rows.reverse()
        }
        let mut merge = prepare_historical_rows_v1(
            &temp.path().join(if reverse { "b" } else { "a" }),
            &limits,
            "history".into(),
            || Ok(rows.clone().into_iter().map(Ok)),
        )
        .unwrap();
        let mut order = Vec::new();
        while let Some(input) = merge.next_input().unwrap() {
            if let DptIngressV1::Message {
                source_evidence, ..
            } = input.ingress
            {
                let a = source_evidence.collector_admission.unwrap();
                order.push((
                    source_evidence.source_kind,
                    a.source_sequence_epoch_id,
                    a.collector_admission_sequence,
                ));
            }
        }
        outputs.push(order);
    }
    assert_eq!(outputs[0], outputs[1]);
    assert_eq!(
        outputs[0],
        vec![
            (
                CanonicalSourceKindV2::BinanceDepthSnapshot,
                "depth-a".into(),
                1
            ),
            (
                CanonicalSourceKindV2::BinanceDepthUpdate,
                "depth-a".into(),
                2
            ),
            (
                CanonicalSourceKindV2::BinanceDepthSnapshot,
                "depth-b".into(),
                1
            )
        ]
    );
}
#[test]
fn explicit_committed_reader_does_not_discover_later_publication() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("recording");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let input = meta(0);
    let result = apply(&mut core, &input);
    writer.record(input, result).unwrap();
    let first = writer.stabilize().unwrap();
    let input = pm(1, 1, 21000, book("100"));
    let result = apply(&mut core, &input);
    writer.record(input, result).unwrap();
    writer.stabilize().unwrap();
    let mut reader = PublicRecordingReaderV1::open_committed(&root, first, 1, 1 << 20).unwrap();
    while let Some(row) = reader.next_row().unwrap() {
        if let PublicStoredRowV1::Input(r) = row {
            assert_eq!(r.input_position, 0)
        }
    }
    let mut empty = PublicRecordingReaderV1::open_committed(&root, None, 1, 1 << 20).unwrap();
    assert!(empty.next_row().unwrap().is_none());
}
#[test]
fn source_without_admission_uses_original_reception_not_payload_time_or_file_order() {
    let temp = tempfile::tempdir().unwrap();
    let mut rows = Vec::new();
    for (received, payload) in [(22000, 21900), (21000, 22100)] {
        let mut row = twap_input(60).common;
        row.schema_version = POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA.into();
        row.topic = "crypto_prices_chainlink".into();
        row.timestamp_received = received;
        row.payload_timestamp = payload;
        row.age_ms = Some(received - payload);
        let row = TypedRawRowV2::Existing(TypedRawRow::PolymarketRtdsChainlinkBtcUsd(row));
        assert!(
            source_evidence_v3_from_typed_raw_v2(&row)
                .unwrap()
                .collector_admission
                .is_none()
        );
        rows.push(row);
    }
    let mut limits = crate::gate_a_formal_canonical_config();
    limits.sort = config().sort;
    let mut merge =
        prepare_historical_rows_v1(&temp.path().join("sort"), &limits, "history".into(), || {
            Ok(rows.clone().into_iter().map(Ok))
        })
        .unwrap();
    let mut times = Vec::new();
    while let Some(input) = merge.next_input().unwrap() {
        if let DptIngressV1::Message {
            source_evidence, ..
        } = input.ingress
        {
            times.push((
                source_evidence.received_at_utc_ms,
                source_evidence.source_event_at_utc_ms.unwrap(),
            ));
        }
    }
    assert_eq!(times, vec![(21000, 22100), (22000, 21900)]);
}

pub(crate) fn historical_identity_fixture(
    root: &Path,
) -> Vec<(RecordedIngressV1, serde_json::Value)> {
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(root, core.header(), config()).unwrap();
    let mut expected = Vec::new();
    let mut other = metadata_snapshot_row();
    other.condition_id = format!("0x{}", "2".repeat(64));
    other.clob_market_id = "3195577".into();
    other.collector_admission_sequence = 2;
    let other_id = other.condition_id.clone();
    for input in [
        meta(0),
        record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(other))),
        ),
    ] {
        let result = apply(&mut core, &input);
        expected.push((input.clone(), fingerprint(&result)));
        writer.record(input, result).unwrap();
    }
    let response = HttpResponseEvidenceV1 {
        origin: "gamma_events".into(),
        request_url: "https://gamma-api.polymarket.com/events".into(),
        received_at_utc: "2026-09-08T04:00:00.123456Z".into(),
        received_at_utc_ms: 1_788_840_000_123,
        receipt_observation: "curl_completed".into(),
        response_body_sha256: "a".repeat(64),
        response_body_path: "response.json".into(),
        original_response_file: "/original/response.json".into(),
        http_status_code: None,
        response_url: None,
    };
    let source = historical_response_source_evidence_v3(&response).unwrap();
    let mut identities = BTreeMap::new();
    let mut rows = vec![];
    for (i, id) in [CONDITION_ID.to_owned(), other_id.clone()]
        .into_iter()
        .enumerate()
    {
        let context = core.pm_context(&id).unwrap();
        let key = format!("identity-{i}");
        identities.insert(key.clone(), context.identity_source_evidence[0].clone());
        rows.push(HistoricalPriceAnchorRepairRowV1 {
            schema_version: HISTORICAL_PRICE_ANCHOR_REPAIR_SCHEMA_V1.into(),
            condition_id: id,
            clob_market_id: context.market.clob_market_id.0,
            slug: "btc-updown-5m".into(),
            asset_ids_json: format!("[\"{UP_TOKEN_ID}\",\"{DOWN_TOKEN_ID}\"]"),
            market_start_at_utc_ms: MARKET_START_AT,
            market_end_at_utc_ms: MARKET_END_AT,
            anchor_source: MarketAnchorSourceV1::GammaEventMetadataPriceToBeat,
            price_to_beat_raw: "65000".into(),
            response_group_id: source.source_message_id.0.clone(),
            response_member_index: i as u32,
            response_received_at_utc_ms: response.received_at_utc_ms,
            response_sha256: response.response_body_sha256.clone(),
            source_value_path: format!("/{i}/eventMetadata/priceToBeat"),
            source_event_at_utc_ms: None,
            historical_identity_ready_at_utc_ms: [10_000, 21_000][i],
            historical_identity_evidence_ids_json: format!("[\"{key}\"]"),
            repair_policy_version: "price_anchor_historical_repair_v1".into(),
        });
    }
    let batch = HistoricalPriceAnchorRepairBatchInput {
        response_group_id: source.source_message_id.0.clone(),
        response: response.clone(),
        rows,
        identity_evidence: BTreeMap::new(),
    };
    let mut input = record(2, TypedRawRowV2::HistoricalPriceAnchorRepairBatch(batch));
    input.dependency_record_refs = vec![
        CanonicalInputRecordRefV1 {
            input_stream_id: "core-test".into(),
            input_position: 0,
        },
        CanonicalInputRecordRefV1 {
            input_stream_id: "core-test".into(),
            input_position: 1,
        },
    ];

    let prepared = prepare_public_ingress_v1(&core, &input, &identities, &[]).unwrap();
    let result = core.push(&prepared).unwrap().into_owned();
    drop(prepared);
    assert_eq!(result.applied.closed_groups.len(), 1);
    assert_eq!(result.applied.closed_groups[0].records.len(), 2);
    expected.push((input.clone(), fingerprint(&result)));
    writer.record(input, result).unwrap();
    writer.stabilize().unwrap();
    expected
}
#[test]
fn replay_historical_original_identity_keys_preserve_all_members() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("recording");
    let expected = historical_identity_fixture(&root);
    for page in [1, 4] {
        let session = (page == 4).then(crate::public_replay::cost::Session::enable);
        let mut replay = PublicReplayV1::open(&root, page, 1 << 20).unwrap();
        let mut actual = Vec::new();
        while let Some((input, result)) = replay.next_applied().unwrap() {
            actual.push((input, fingerprint(&result)));
        }
        assert_eq!(actual, expected);
        if let Some(session) = session {
            let measured = crate::public_replay::cost::report_value(session.finish());
            assert_eq!(measured["scan_totals"]["identity_scan"]["invocations"], 1);
            assert_eq!(
                measured["scan_totals"]["identity_scan"]["matched_records"],
                2
            );
            assert_eq!(measured["scan_totals"]["identity_scan"]["returned"], 2);
            assert_eq!(
                measured["scan_by_input"]["2"]["context"]["historical_member_conditions"]
                    .as_array()
                    .unwrap()
                    .len(),
                2
            );
            assert_eq!(measured["negative_residual_entries"], 0);
        }
    }
    // 同一真实身份修复沿固定凭据全读；更晚损坏目录不能替换已绑定边界。
    let prefix = PublicRecordingReaderV1::recover(&root, 1, 1 << 20)
        .unwrap()
        .prefix
        .unwrap();
    let later = root.join("stable-99999999999999999999");
    std::fs::create_dir(&later).unwrap();
    std::fs::write(later.join("prefix.json"), b"broken-later-prefix").unwrap();
    let mut replay = PublicReplayV1::open_committed(&root, prefix.clone(), 1, 1 << 20).unwrap();
    let mut actual = Vec::new();
    while let Some((input, result)) = replay.next_applied().unwrap() {
        actual.push((input, fingerprint(&result)));
    }
    assert_eq!(actual, expected);
    assert_eq!(
        replay.core.applied_prefix().next_input_position,
        prefix.next_input_position
    );
}

#[test]
fn replay_first_identity_selects_prior_twap_at_its_original_position() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("recording");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let mut expected = Vec::new();
    for input in [
        twap_record(0, 60, 21_002, "65000000000000000000000"),
        rule_record(1, 60, None),
    ] {
        let result = apply(&mut core, &input);
        expected.push((input.clone(), fingerprint(&result)));
        writer.record(input, result).unwrap();
    }
    writer.stabilize().unwrap();
    let mut input = meta(2);
    let ids = BTreeMap::new();
    let mut page = ReferenceCandidateReaderV1::open(&root, 1, 1 << 20, 2).unwrap();
    let prepared = prepare_public_ingress_with_reference_v1(
        &core,
        &mut input,
        &ids,
        &[],
        &market(),
        |cursor| Ok(page.page(cursor).unwrap()),
    )
    .unwrap();
    let result = core.push(&prepared).unwrap().into_owned();
    drop(prepared);
    assert!(
        core.reference_state(CONDITION_ID)
            .unwrap()
            .temporary
            .is_some()
    );
    assert_eq!(
        input.dependency_record_refs,
        vec![CanonicalInputRecordRefV1 {
            input_stream_id: "core-test".into(),
            input_position: 0
        }]
    );
    expected.push((input.clone(), fingerprint(&result)));
    writer.record(input, result).unwrap();
    writer.stabilize().unwrap();
    for page in [1, 4] {
        let mut replay = PublicReplayV1::open(&root, page, 1 << 20).unwrap();
        let mut actual = Vec::new();
        while let Some((input, result)) = replay.next_applied().unwrap() {
            actual.push((input, fingerprint(&result)));
        }
        assert_eq!(actual, expected);
    }
}

#[test]
fn stable_segments_require_the_complete_prefix_and_declared_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("recording");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let mut row = metadata_snapshot_row();
    row.market_end_utc = row.market_start_utc + 300_000;
    row.validate().unwrap();
    let input = record(
        0,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(row))),
    );
    writer
        .record(input.clone(), apply(&mut core, &input))
        .unwrap();
    let first = writer.stabilize().unwrap().unwrap();
    assert!(!first.normal_end);
    let input = end(1);
    writer
        .record(input.clone(), apply(&mut core, &input))
        .unwrap();
    let last = writer.stabilize().unwrap().unwrap();
    assert!(last.normal_end);
    assert_eq!(last.next_input_position, 2);
    assert_eq!(last.next_group_position, 1);
    assert_eq!(last.next_public_index_position, 1);
    let mut old =
        PublicRecordingReaderV1::open_committed(&root, Some(first.clone()), 1, 1 << 20).unwrap();
    while old.next_row().unwrap().is_some() {}
    assert_eq!(old.prefix.as_ref(), Some(&first));
    assert_eq!(
        all_rows(&root, 1)
            .iter()
            .filter(|r| matches!(r, PublicStoredRowV1::Input(_)))
            .count(),
        2
    );
    let mut mismatched = last.clone();
    mismatched.next_input_position += 1;
    assert!(PublicRecordingReaderV1::open_committed(&root, Some(mismatched), 1, 1 << 20).is_err());
    let copy = temp.path().join("missing-first");
    fs::create_dir(&copy).unwrap();
    fs::copy(root.join("stream.json"), copy.join("stream.json")).unwrap();
    fs::create_dir(copy.join(&last.stable_storage_ref)).unwrap();
    for name in ["prefix.json", "captured.parquet"] {
        fs::copy(
            root.join(&last.stable_storage_ref).join(name),
            copy.join(&last.stable_storage_ref).join(name),
        )
        .unwrap();
    }
    assert!(PublicRecordingReaderV1::recover(&copy, 1, 1 << 20).is_err());
    fs::remove_dir_all(root.join(&last.stable_storage_ref)).unwrap();
    assert!(PublicRecordingReaderV1::open_committed(&root, Some(last), 1, 1 << 20).is_err());
    let mut recovered = PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap();
    while recovered.next_row().unwrap().is_some() {}
    assert_eq!(recovered.prefix, Some(first));
}

#[test]
fn stable_segment_payload_counts_are_checked_during_the_only_read_pass() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("recording");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let input = meta(0);
    writer
        .record(input.clone(), apply(&mut core, &input))
        .unwrap();
    let mut prefix = writer.stabilize().unwrap().unwrap();
    prefix.next_canonical_position += 1;
    serde_json::to_writer(
        File::create(root.join(&prefix.stable_storage_ref).join("prefix.json")).unwrap(),
        &prefix,
    )
    .unwrap();
    let mut reader = PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap();
    loop {
        match reader.next_row() {
            Ok(Some(_)) => {}
            Err(_) => break,
            Ok(None) => panic!("未核对分片末端计数"),
        }
    }
}

#[test]
fn finite_prepare_current_and_late_reference_match_record_and_paged_replay() {
    for (case, order) in [
        ("current", ["identity", "rule", "twap"]),
        ("late-rule", ["identity", "twap", "rule"]),
        ("late-identity", ["twap", "rule", "identity"]),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let mut rows = Vec::new();
        for (index, kind) in order.into_iter().chain(["official"]).enumerate() {
            let received = 21_010 + index as i64 * 10;
            let input = match kind {
                "identity" => {
                    let mut row = metadata_snapshot_row();
                    row.market_end_utc = MARKET_START_AT + 300_000;
                    row.timestamp_received = received;
                    row.collector_admission_time_utc_ms = received + 1;
                    row.collector_admission_sequence = index as u64 + 1;
                    row.market_lifecycle_state = MetadataMarketLifecycleState::Active;
                    row.market_runtime_role = MetadataMarketRuntimeRole::Current;
                    record(
                        0,
                        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
                            row,
                        ))),
                    )
                }
                "twap" => twap_record(index as u64, 60, received, "65000000000000000000000"),
                _ => {
                    let input =
                        rule_record(index as u64, 60, (kind == "official").then_some("64999"));
                    let DptIngressV1::Message {
                        row: TypedRawRowV2::PolymarketMarketMetadataV6(mut row),
                        ..
                    } = input.ingress
                    else {
                        panic!()
                    };
                    let PolymarketMarketMetadataV6Input::AnchorRule(rule) = row.as_mut() else {
                        panic!()
                    };
                    rule.market_end_utc = MARKET_START_AT + 300_000;
                    rule.timestamp_received = received;
                    rule.collector_admission_time_utc_ms = received + 1;
                    rule.http_received_at_utc = Some(received);
                    record(0, TypedRawRowV2::PolymarketMarketMetadataV6(row))
                }
            };
            let DptIngressV1::Message { row, .. } = input.ingress else {
                panic!()
            };
            rows.push(row);
        }
        let mut limits = crate::gate_a_formal_canonical_config();
        limits.sort = config().sort;
        limits.sort.batch_rows = 1;
        let mut expected = Vec::new();
        let root = temp.path().join("recording");
        for recorded in [false, true] {
            let mut merge = prepare_historical_rows_v1(
                &temp
                    .path()
                    .join(if recorded { "record-sort" } else { "core-sort" }),
                &limits,
                "core-test".into(),
                || Ok(rows.clone().into_iter().map(Ok)),
            )
            .unwrap();
            let mut core = core();
            let mut writer = recorded
                .then(|| PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap());
            let mut actual = Vec::new();
            while let Some(mut input) = merge.next_input().unwrap() {
                let prepared = merge.prepare_input(&core, &mut input).unwrap();
                let result = core.push(&prepared).unwrap().into_owned();
                drop(prepared);
                if input.input_position == 2 {
                    let state = core.reference_state(CONDITION_ID).unwrap();
                    let selected = state.temporary.as_ref().unwrap();
                    let twap_position =
                        order.iter().position(|kind| *kind == "twap").unwrap() as u64;
                    assert_eq!(
                        selected.selected_position.closed_at_input_position, twap_position,
                        "{case}"
                    );
                    assert_eq!(selected.direct_anchor.position, selected.selected_position);
                    if case != "current" {
                        assert_eq!(
                            input.dependency_record_refs[0].input_position,
                            twap_position
                        );
                    }
                }
                if input.input_position == 3 {
                    assert!(
                        core.reference_state(CONDITION_ID)
                            .unwrap()
                            .official
                            .value
                            .is_some()
                    );
                }
                actual.push((input.clone(), fingerprint(&result)));
                if let Some(writer) = &mut writer {
                    writer.record(input, result).unwrap();
                }
            }
            let mut input = end(4);
            let prepared = merge.prepare_input(&core, &mut input).unwrap();
            let result = core.push(&prepared).unwrap().into_owned();
            actual.push((input.clone(), fingerprint(&result)));
            if let Some(writer) = &mut writer {
                assert!(writer.committed_prefix().is_none());
                writer.record(input, result).unwrap();
                writer.stabilize().unwrap();
                assert_eq!(actual, expected, "{case}");
            } else {
                expected = actual;
            }
        }
        for page_rows in [1, 4] {
            let mut replay = PublicReplayV1::open(&root, page_rows, 1 << 20).unwrap();
            let mut actual = Vec::new();
            while let Some((input, result)) = replay.next_applied().unwrap() {
                actual.push((input, fingerprint(&result)));
            }
            assert_eq!(actual, expected, "{case}, page={page_rows}");
        }
    }
}

#[test]
fn stored_pm_reports_keep_original_schema_bytes_and_positions_for_all_dispositions() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("reports");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    let inputs = vec![
        meta(0),
        pm(
            1,
            1,
            21_000,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("80"))],
            },
        ),
        pm(2, 2, 21_001, book("100")),
        pm(3, 3, 21_002, book("100")),
        end(4),
    ];
    let mut next_canonical = 0_u64;
    let mut expected = Vec::new();
    let mut dispositions = std::collections::BTreeSet::new();
    for input in inputs {
        let result = apply(&mut core, &input);
        let positions: BTreeMap<_, _> = result
            .applied
            .closed_groups
            .iter()
            .flat_map(|g| g.records.iter())
            .enumerate()
            .map(|(i, f)| (f.position.clone(), next_canonical + i as u64))
            .collect();
        next_canonical += positions.len() as u64;
        for (position, reduction) in &result.pm_reductions {
            let material = materialize_pm_step_identity_v1(reduction.clone()).unwrap();
            for (ordinal, report) in material.reduction.reports.into_iter().enumerate() {
                // 使用原持久DTO作为独立格式预期，包含原Prepared证据的透明序列化。
                let report: PmReductionReportV2<SourceEvidenceRefV3> =
                    serde_json::from_value(serde_json::to_value(report).unwrap()).unwrap();
                dispositions.insert(match report.disposition {
                    PmReductionDispositionV2::Produced { .. } => 0,
                    PmReductionDispositionV2::NoOutput { .. } => 1,
                    PmReductionDispositionV2::Suppressed { .. } => 2,
                });
                expected.push(
                    serde_json::to_vec(&PublicStoredRowV1::PmReport {
                        input_position: input.input_position,
                        canonical_position: positions[position],
                        report_ordinal: ordinal as u32,
                        report,
                    })
                    .unwrap(),
                );
            }
        }
        writer.record(input, result).unwrap();
    }
    let prefix = writer.stabilize().unwrap().unwrap();
    assert_eq!(dispositions, [0, 1, 2].into_iter().collect());
    let decoded: Vec<_> = all_rows(&root, 1)
        .into_iter()
        .filter(|r| matches!(r, PublicStoredRowV1::PmReport { .. }))
        .map(|r| serde_json::to_vec(&r).unwrap())
        .collect();
    assert_eq!(decoded, expected);
    let mut cursor = RunCursor::open(
        &root
            .join(prefix.stable_storage_ref)
            .join("captured.parquet"),
        1,
        1 << 20,
    )
    .unwrap();
    let mut bytes = Vec::new();
    while let Some(row) = cursor.next_row().unwrap() {
        if matches!(
            serde_json::from_slice::<PublicStoredRowV1>(row.payload()).unwrap(),
            PublicStoredRowV1::PmReport { .. }
        ) {
            bytes.push(row.payload().to_vec());
        }
    }
    assert_eq!(bytes, expected, "底层正文逐字节保持原格式");
}

#[path = "public_replay_cost_tests.rs"]
mod replay_cost_tests;

#[path = "public_recording_layout_tests.rs"]
mod layout_tests;

#[test]
fn range_received_other_v2_variants_keep_full_source_fallback() {
    let root = tempfile::tempdir().unwrap();
    let repaired = historical_identity_fixture(&root.path().join("recording"));
    let DptIngressV1::Message { row: repair, .. } = repaired[2].0.ingress.clone() else {
        unreachable!()
    };
    for input in [
        TypedRawRowV2::PolymarketMarketMetadataV6(Box::new(
            PolymarketMarketMetadataV6Input::AnchorRule(anchor_input()),
        )),
        TypedRawRowV2::PolymarketRtdsChainlinkTwap(twap_input(60)),
        repair,
    ] {
        assert_eq!(
            validated_received_at_from_typed_raw_v2(&input),
            source_evidence_v3_from_typed_raw_v2(&input).map(|source| source.received_at_utc_ms)
        );
    }
}

#[test]
fn opted_replay_recovers_identical_business_confirmation_from_original_recording() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("recording");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    for input in [
        meta(0),
        record(
            1,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot("e", 100, 1))),
        ),
        record(
            2,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot("e", 100, 2))),
        ),
    ] {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    let terminal = end(3);
    let result = apply(&mut core, &terminal);
    writer.record(terminal, result).unwrap();
    writer.stabilize().unwrap();
    let mut replay = crate::public_replay::PublicReplayV1::open(&root, 1, 1 << 20).unwrap();
    replay.core.enable_source_progress();
    let mut same = None;
    while let Some((input, result)) = replay.next_applied().unwrap() {
        if input.input_position == 2 {
            same = Some(result.applied);
        }
    }
    let same = same.unwrap();
    assert_eq!(same.closed_groups.len(), 1);
    assert!(same.closed_groups[0].records.is_empty());
    let observation = &same.closed_groups[0].observations[0];
    assert_eq!(
        observation.source_progress[0]
            .last_valid_business_message
            .as_ref()
            .unwrap()
            .received_at_utc_ms,
        2002
    );
    assert_eq!(
        observation.binance_depth.direct_anchors[0].received_at_utc_ms,
        2001
    );
    let mut legacy = crate::public_replay::PublicReplayV1::open(&root, 1, 1 << 20).unwrap();
    while let Some((input, result)) = legacy.next_applied().unwrap() {
        if input.input_position == 2 {
            assert!(result.applied.closed_groups.is_empty());
        }
    }
}

#[test]
fn physical_source_parts_preserve_continuous_core_and_prior_twap_reread() {
    let temp = tempfile::tempdir().unwrap();
    let rows: Vec<_> = [
        meta(0),
        twap_record(1, 60, 21001, "64000000000000000000000"),
        twap_record(2, 60, 21002, "65000000000000000000000"),
        rule_record(3, 60, None),
        pm(4, 1, 22004, book("100")),
        pm(5, 2, 22005, book("80")),
        pm(6, 3, 22006, book("90")),
        pm(7, 4, 22007, book("70")),
        pm(8, 5, 22008, book("60")),
        pm(9, 6, 22009, book("50")),
        pm(10, 7, 22010, book("40")),
        pm(11, 8, 22011, book("30")),
    ]
    .into_iter()
    .map(|input| match input.ingress {
        DptIngressV1::Message { row, .. } => row,
        _ => unreachable!(),
    })
    .collect();
    let mut limits = crate::gate_a_formal_canonical_config();
    limits.sort = config().sort;
    limits.sort.max_run_rows = 1;
    limits.sort.merge_fan_in = 2;
    limits.sort.max_open_files = 12;
    let mut outputs = Vec::new();
    for split in [false, true] {
        limits.sort.merge_workers = if split { 4 } else { 1 };
        crate::sort_parallel::TEST_START_BARRIER.with(|barrier| {
            *barrier.borrow_mut() = split.then(|| std::sync::Arc::new(std::sync::Barrier::new(2)));
        });
        let mut merge = prepare_historical_rows_v1(
            &temp.path().join(if split { "parts" } else { "single" }),
            &limits,
            "core-test".into(),
            || Ok(rows.clone().into_iter().map(Ok)),
        )
        .unwrap();
        crate::sort_parallel::TEST_START_BARRIER.with(|barrier| *barrier.borrow_mut() = None);
        let pm_metrics = &merge.sort_metrics()[HistoricalSourceV1::Polymarket as usize];
        assert_eq!(pm_metrics.input_rows, 8);
        if split {
            assert!(pm_metrics.peak_merge_workers > 1);
        } else {
            assert_eq!(pm_metrics.peak_merge_workers, 1);
        }
        if split {
            merge.split_runs_for_test(&temp.path().join("physical"));
        }
        let mut core = core();
        let mut output = Vec::new();
        while let Some(mut input) = merge.next_input().unwrap() {
            let prepared = merge.prepare_input(&core, &mut input).unwrap();
            let result = core.push(&prepared).unwrap().into_owned();
            drop(prepared);
            output.push((input, fingerprint(&result)));
        }
        assert!(
            core.reference_state(CONDITION_ID)
                .unwrap()
                .temporary
                .is_some()
        );
        outputs.push(output);
    }
    assert_eq!(outputs[0], outputs[1]);
    assert!(
        outputs[1]
            .iter()
            .any(|(input, _)| !input.dependency_record_refs.is_empty())
    );
}

fn observation_range_inputs() -> Vec<RecordedIngressV1> {
    let snapshot = |digit: char, clob: &str, at: i64, sequence: u64| {
        let mut row = metadata_snapshot_row();
        row.condition_id = format!("0x{}", digit.to_string().repeat(64));
        row.clob_market_id = clob.into();
        row.timestamp_received = at;
        row.collector_admission_sequence = sequence;
        row.collector_admission_time_utc_ms = at + 1;
        row.fixed_identity_verified_at_utc = at - 1;
        row.source_evidence_received_at_utc = Some(at);
        row.row_evidence_validation_completed_at_utc = Some(at + 1);
        row.metadata_fetch_start_utc = Some(at - 10);
        row
    };
    let mut first = snapshot('1', "3195576", 21_000, 1);
    first.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    first.market_runtime_role = MetadataMarketRuntimeRole::Current;
    let second = snapshot('3', "3195578", 21_002, 2);
    // Gamma闭市字段与终局退休是两种事实；本夹具只声明真实closed状态。
    let mut closed = snapshot('1', "3195576", 31_000, 3);
    closed.market_lifecycle_state = MetadataMarketLifecycleState::Active;
    closed.market_runtime_role = MetadataMarketRuntimeRole::Current;
    closed.closed = Some(true);
    closed.accepting_orders = Some(false);
    let payload =
        r#"{"orderPriceMinTickSize":"0.01","closed":true,"acceptingOrders":false}"#.to_owned();
    closed.source_evidence_raw_sha256 = Some(format!("{:x}", Sha256::digest(payload.as_bytes())));
    closed.source_evidence_payload_json = Some(payload);
    let mut third = snapshot('2', "3195577", 31_002, 4);
    third.market_start_utc = 40_000;
    third.market_end_utc = 50_000;
    let mut inputs: Vec<_> = [first, second, closed, third]
        .into_iter()
        .enumerate()
        .map(|(position, row)| {
            record(
                position as u64,
                TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(row))),
            )
        })
        .collect();
    inputs.push(twap_record(4, 60, 41_000, "65000000000000000000000"));
    inputs.push(end(5));
    inputs
}

#[test]
fn compact_observation_ranges_restore_all_original_fields_across_market_close_pages_and_segments() {
    let temp = tempfile::tempdir().unwrap();
    let inputs = observation_range_inputs();
    let mut all_prefixes = Vec::new();
    let mut all_logical_rows = Vec::new();
    for mode in [0, 1, 2] {
        let historical = mode != 0;
        let mut counted_core = core();
        let root = temp.path().join(["legacy", "compact", "counts"][mode]);
        let mut core = core();
        let mut writer = if historical {
            PublicRecordingWriterV1::create_historical(&root, core.header(), config())
        } else {
            PublicRecordingWriterV1::create(&root, core.header(), config())
        }
        .unwrap();
        let mut expected_applied = Vec::new();
        let mut prefixes = Vec::new();
        for input in &inputs {
            let result = apply(&mut core, input);
            if input.input_position == 2 {
                assert_eq!(result.applied.closed_groups[0].observations.len(), 2);
                assert_eq!(
                    result.applied.closed_groups[0].observations[0].closed.value,
                    Some(true)
                );
                assert_eq!(
                    result.applied.closed_groups[0].observations[0]
                        .accepting_orders
                        .value,
                    Some(false)
                );
            }
            if input.input_position >= 3 {
                for group in &result.applied.closed_groups {
                    assert_eq!(
                        group
                            .observations
                            .iter()
                            .map(|o| o.market.condition_id.0.as_str())
                            .collect::<Vec<_>>(),
                        [
                            format!("0x{}", "1".repeat(64)),
                            format!("0x{}", "2".repeat(64)),
                            format!("0x{}", "3".repeat(64))
                        ]
                    );
                }
            }
            expected_applied.push(fingerprint(&result));
            if mode == 2 {
                let ids = BTreeMap::new();
                let prepared = prepare_public_ingress_v1(&counted_core, input, &ids, &[]).unwrap();
                let counts = counted_core.push_recording(&prepared).unwrap().into_owned();
                assert_eq!(counts.applied_prefix, result.applied.applied_prefix);
                assert_eq!(
                    counts
                        .closed_groups
                        .iter()
                        .map(|g| g.observation_count)
                        .collect::<Vec<_>>(),
                    result
                        .applied
                        .closed_groups
                        .iter()
                        .map(|g| g.observations.len() as u64)
                        .collect::<Vec<_>>()
                );
                writer.record_counts(input.clone(), counts).unwrap();
            } else {
                writer.record(input.clone(), result).unwrap();
            }
            if [1, 2, 5].contains(&input.input_position) {
                let prefix = writer.stabilize().unwrap().unwrap();
                prefixes.push(prefix);
            }
        }
        assert_eq!(
            prefixes
                .iter()
                .map(|p| p.next_observation_position)
                .collect::<Vec<_>>(),
            [3, 5, 11]
        );
        for prefix in &prefixes {
            let mut logical =
                PublicRecordingReaderV1::open_committed(&root, Some(prefix.clone()), 1, 1 << 20)
                    .unwrap();
            let mut logical_observations = 0;
            while let Some(row) = logical.next_row().unwrap() {
                if matches!(row, PublicStoredRowV1::Observation(_)) {
                    logical_observations += 1;
                }
                assert!(!matches!(row, PublicStoredRowV1::GroupObservationRange(_)));
            }
            assert_eq!(logical_observations, prefix.next_observation_position);
        }
        let mut logical_pages = Vec::new();
        for page in [1, 2, 20] {
            let mut logical = PublicRecordingReaderV1::recover(&root, page, 1 << 20).unwrap();
            let mut rows = Vec::new();
            loop {
                let values = logical.next_page(page).unwrap();
                if values.is_empty() {
                    break;
                }
                rows.extend(values);
            }
            logical_pages.push(serde_json::to_value(rows).unwrap());
        }
        assert_eq!(logical_pages[0], logical_pages[1]);
        assert_eq!(logical_pages[0], logical_pages[2]);
        let mut physical = PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap();
        let mut counts = Vec::new();
        let mut old_observations = 0;
        while let Some(row) = physical.next_physical_row().unwrap() {
            match row {
                PublicStoredRowV1::GroupObservationRange(group) => {
                    counts.push(group.observation_count);
                    let value = serde_json::to_value(&group).unwrap();
                    assert!(value.get("observation_positions").is_none());
                    assert!(value.get("market").is_none());
                }
                PublicStoredRowV1::Observation(_) => old_observations += 1,
                PublicStoredRowV1::Group(_) if historical => panic!("紧凑材料不能再保存逐观察索引"),
                _ => {}
            }
        }
        if historical {
            assert_eq!(counts, [1, 2, 2, 3, 3]);
            assert_eq!(old_observations, 0);
        } else {
            assert!(counts.is_empty());
            assert_eq!(old_observations, 11);
        }
        let mut replay = PublicReplayV1::open(&root, 1, 1 << 20).unwrap();
        let mut actual_applied = Vec::new();
        while let Some((_, result)) = replay.next_applied().unwrap() {
            actual_applied.push(fingerprint(&result));
        }
        assert_eq!(actual_applied, expected_applied);
        all_prefixes.push(prefixes);
        all_logical_rows.push(logical_pages.remove(0));
    }
    assert_eq!(all_prefixes[0], all_prefixes[1]);
    assert_eq!(all_prefixes[0], all_prefixes[2]);
    assert_eq!(all_logical_rows[0], all_logical_rows[1]);
    assert_eq!(all_logical_rows[0], all_logical_rows[2]);
}

#[test]
fn compact_observation_range_misalignment_fails_original_order_boundary() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("original");
    let mut core = core();
    let mut writer =
        PublicRecordingWriterV1::create_historical(&root, core.header(), config()).unwrap();
    for input in observation_range_inputs() {
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
    }
    let prefix = writer.stabilize().unwrap().unwrap();
    let mut reader = PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap();
    let bad_root = temp.path().join("misaligned");
    let mut writer =
        PublicRecordingWriterV1::create_historical(&bad_root, core.header(), config()).unwrap();
    let mut changed = false;
    while let Some(mut row) = reader.next_physical_row().unwrap() {
        if !changed && let PublicStoredRowV1::GroupObservationRange(group) = &mut row {
            group.observation_start += 1;
            changed = true;
        }
        writer.write(row).unwrap();
    }
    assert!(changed);
    writer.pending = prefix;
    writer.stabilize().unwrap();
    for physical in [false, true] {
        let mut reader = PublicRecordingReaderV1::recover(&bad_root, 1, 1 << 20).unwrap();
        let error = loop {
            let row = if physical {
                reader.next_physical_row()
            } else {
                reader.next_row()
            };
            match row {
                Err(error) => break error,
                Ok(Some(_)) => {}
                Ok(None) => panic!("错位区间不能成功"),
            }
        };
        assert_eq!(error.to_string(), "稳定记录原序或流身份不符");
    }
}

#[test]
fn compact_observation_closed_position_overflow_returns_invalid_data() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("original");
    let mut core = core();
    let input = meta(0);
    let result = apply(&mut core, &input);
    let mut writer =
        PublicRecordingWriterV1::create_historical(&root, core.header(), config()).unwrap();
    writer.record(input, result).unwrap();
    let prefix = writer.stabilize().unwrap().unwrap();
    let mut reader = PublicRecordingReaderV1::recover(&root, 1, 1 << 20).unwrap();
    let bad_root = temp.path().join("overflow");
    let mut writer =
        PublicRecordingWriterV1::create_historical(&bad_root, core.header(), config()).unwrap();
    let mut changed = false;
    while let Some(mut row) = reader.next_physical_row().unwrap() {
        if let PublicStoredRowV1::GroupObservationRange(group) = &mut row {
            group.closed_at_input_position = u64::MAX;
            changed = true;
        }
        writer.write(row).unwrap();
    }
    assert!(changed);
    writer.pending = prefix;
    writer.stabilize().unwrap();
    let mut reader = PublicRecordingReaderV1::recover(&bad_root, 1, 1 << 20).unwrap();
    let error = loop {
        match reader.next_row() {
            Err(error) => break error,
            Ok(Some(_)) => {}
            Ok(None) => panic!("不能表示的前沿不得成功展开"),
        }
    };
    assert_eq!(
        error.downcast_ref::<io::Error>().unwrap().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn core_checkpoint_restores_exact_quantity_dedup_and_reference_state_without_replay() {
    let mut original = core();
    let inputs = [
        meta(0),
        pm(1, 1, 20_001, book("100")),
        pm(
            2,
            2,
            20_002,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("70"))],
            },
        ),
        record(
            3,
            TypedRawRowV2::Existing(TypedRawRow::BinanceDepthSnapshot(snapshot(
                "snapshot-checkpoint",
                100,
                1,
            ))),
        ),
        twap_record(4, 30, 22_004, "65010000000000000000000"),
    ];
    for input in &inputs {
        apply(&mut original, input);
    }
    let before = original.object_counts();
    let mut bytes = Vec::new();
    original.write_checkpoint(&mut bytes).unwrap();
    let mut restored = PublicObservationCoreV1::read_checkpoint(bytes.as_slice()).unwrap();
    assert_eq!(before, restored.object_counts());
    assert_eq!(original.applied_prefix(), restored.applied_prefix());
    // 恢复构造没有push；已提交5输入保持原位置，不从空Core计算。
    assert_eq!(restored.applied_prefix().next_input_position, 5);
    for input in [
        pm(
            5,
            2,
            20_002,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("70"))],
            },
        ),
        pm(
            6,
            3,
            20_003,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("30"))],
            },
        ),
        end(7),
    ] {
        assert_eq!(
            fingerprint(&apply(&mut original, &input)),
            fingerprint(&apply(&mut restored, &input))
        );
    }
}

#[test]
fn core_checkpoint_preserves_observed_empty_bid_and_unknown_asset() {
    let mut original = core();
    apply(&mut original, &meta(0));
    apply(&mut original, &pm(1, 1, 20_001, book("100")));
    let quote = pm(
        2,
        2,
        20_002,
        PolymarketOrderbookEvent::BestBidAsk {
            asset_id: UP_TOKEN_ID.into(),
            best_bid: None,
            best_ask: Some("0.6".into()),
            spread: None,
        },
    );
    let result = apply(&mut original, &quote);
    let assets = &result.applied.closed_groups[0].observations[0].assets;
    assert_eq!(
        assets
            .iter()
            .find(|a| a.asset.token_id.0 == UP_TOKEN_ID)
            .unwrap()
            .valuation_best_bid
            .value,
        Some(None),
        "真实BBA明确没有买价，不是尚未观察"
    );
    assert_eq!(
        assets
            .iter()
            .find(|a| a.asset.token_id.0 != UP_TOKEN_ID)
            .unwrap()
            .valuation_best_bid
            .value,
        None,
        "另一资产尚未观察"
    );
    let mut bytes = Vec::new();
    original.write_checkpoint(&mut bytes).unwrap();
    let mut restored = PublicObservationCoreV1::read_checkpoint(bytes.as_slice()).unwrap();
    for input in [
        twap_record(3, 30, 22_004, "65010000000000000000000"),
        end(4),
    ] {
        assert_eq!(
            fingerprint(&apply(&mut original, &input)),
            fingerprint(&apply(&mut restored, &input))
        );
    }
}

#[test]
fn pm_side_projection_keeps_other_side_and_old_group_on_same_quantity_confirmation() {
    let mut core = core();
    apply(&mut core, &meta(0));
    let initial = apply(&mut core, &pm(1, 1, 20_001, book("100")));
    let before = &initial.applied.closed_groups[0].observations[0]
        .assets
        .iter()
        .find(|a| a.asset.token_id.0 == UP_TOKEN_ID)
        .unwrap();
    let next = apply(
        &mut core,
        &pm(
            2,
            2,
            20_002,
            PolymarketOrderbookEvent::PriceChange {
                price_changes: vec![change(Some("100"))],
            },
        ),
    );
    let after = &next.applied.closed_groups[0].observations[0]
        .assets
        .iter()
        .find(|a| a.asset.token_id.0 == UP_TOKEN_ID)
        .unwrap();
    assert_eq!(before.bids.value, after.bids.value);
    assert!(!std::sync::Arc::ptr_eq(&before.bids, &after.bids));
    assert!(std::sync::Arc::ptr_eq(&before.asks, &after.asks));
    assert_ne!(before.bids.direct_anchors, after.bids.direct_anchors);
    assert_eq!(
        before.bids.direct_anchors[0]
            .position
            .closed_at_input_position,
        1
    );
    assert_eq!(
        after.bids.direct_anchors[0]
            .position
            .closed_at_input_position,
        2
    );
    let empty = apply(
        &mut core,
        &pm(
            3,
            3,
            20_003,
            PolymarketOrderbookEvent::Book {
                asset_id: UP_TOKEN_ID.to_owned(),
                bids: vec![],
                asks: vec![],
                last_trade_price: None,
            },
        ),
    );
    let cleared = &empty.applied.closed_groups[0].observations[0]
        .assets
        .iter()
        .find(|a| a.asset.token_id.0 == UP_TOKEN_ID)
        .unwrap();
    assert!(cleared.bids.value.as_ref().unwrap().is_empty());
    assert!(cleared.asks.value.as_ref().unwrap().is_empty());
    assert!(!cleared.bids.direct_anchors.is_empty());
    assert!(!cleared.asks.direct_anchors.is_empty());
    assert_eq!(
        before.bids.value.as_ref().unwrap()[0].quantity_shares.0,
        "100"
    );
}
