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
        let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
        let input = meta(0);
        let result = apply(&mut core, &input);
        writer.record(input, result).unwrap();
        writer.stabilize().unwrap();
        let old = serde_json::to_value(all_rows(&root, 1)).unwrap();
        let input = pm(1, 1, 21_000, book("100"));
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
    let root = temp.path().join("recording");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
    for input in [
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
        end(4),
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
    let mut index = HiddenIndexReaderV1::open(&prepared, 1, 1 << 20).unwrap();
    let mut reference_indices = Vec::new();
    while let Some((position, entry)) = index.next_entry().unwrap() {
        if let HiddenIndexEntryV1::Reference {
            canonical_position, ..
        } = entry
        {
            reference_indices.push(position);
            assert_eq!(canonical_position, 7);
        }
    }
    let mut snapshots = Vec::new();
    for page in [1, 2, 20] {
        let mut reader = HiddenBucketReaderV1::open(&prepared, page, 1 << 20).unwrap();
        let mut buckets = Vec::new();
        while let Some(bucket) = reader.next_bucket().unwrap() {
            buckets.push(bucket)
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
    let last = buckets.last().unwrap();
    assert!(last.steps.is_empty());
    assert_eq!(last.reference_positions, reference_indices);
    assert_eq!(last.closure, ReplayIndexClosureV1::EndOfInput);
    let mut material = HiddenMaterialReaderV1::open(&prepared, &root, 2, 1 << 20).unwrap();
    assert_eq!(material.metrics(), &HiddenMaterialReadMetricsV1::default());
    let mut positions = Vec::new();
    let mut actual_references = Vec::new();
    loop {
        let page = material.next_page().unwrap();
        if page.is_empty() {
            break;
        }
        for (position, record) in page {
            positions.push(position);
            if let ResolvedHiddenRecordV1::Reference {
                canonical_position,
                record,
            } = record
            {
                actual_references.push((canonical_position, serde_json::to_value(record).unwrap()));
            }
        }
    }
    assert_eq!(positions, (0..last.index_end).collect::<Vec<_>>());
    assert_eq!(material.metrics().index_entries, last.index_end);
    assert_eq!(material.metrics().peak_page_entries, 2);
    assert!(material.metrics().decoded_derived_rows > 0);
    assert!(material.metrics().decoded_recording_rows > 0);
    // 原位置递增且同一消息跨页时，不应反复读取已经消费的历史。
    let mut all = PublicRecordingReaderV1::recover(&root, 2, 1 << 20).unwrap();
    let mut recording_rows = 0;
    let mut expected_references = Vec::new();
    while let Some(row) = all.next_row().unwrap() {
        recording_rows += 1;
        if let PublicStoredRowV1::Canonical {
            canonical_position: 7,
            record,
            ..
        } = row
        {
            expected_references.push((7, serde_json::to_value(record).unwrap()));
        }
    }
    assert_eq!(actual_references, expected_references);
    let mut all = HiddenStepReaderV1::open(&prepared, 2, 1 << 20).unwrap();
    let mut derived_rows = 0;
    while all.next_row().unwrap().is_some() {
        derived_rows += 1;
    }
    assert!(
        material.metrics().decoded_recording_rows <= recording_rows,
        "原序跨页重复扫描: {:?}, 原文件行数 {recording_rows}",
        material.metrics()
    );
    assert!(material.metrics().decoded_derived_rows <= derived_rows);
    let completed = material.metrics().clone();
    assert!(material.next_page().unwrap().is_empty());
    assert_eq!(material.metrics(), &completed);
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

#[test]
fn replay_historical_original_identity_keys_preserve_all_members() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("recording");
    let mut core = core();
    let mut writer = PublicRecordingWriterV1::create(&root, core.header(), config()).unwrap();
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
