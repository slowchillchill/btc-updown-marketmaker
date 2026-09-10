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
        closed_time_check_passed: None,
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
        terminal_predicate_passed: None,
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

fn book(quantity: &str) -> PolymarketOrderbookEvent {
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

fn change(quantity: Option<&str>) -> PolymarketPriceChangeEntry {
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

fn twap_input(window_s: u64) -> data_pipeline_core::public_input::PolymarketRtdsChainlinkTwapInput {
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

fn core() -> PublicObservationCoreV1 {
    PublicObservationCoreV1::new(PublicInputStreamHeaderV1 {
        input_stream_id: "core-test".to_owned(),
        canonical_rule_version: "rule-v1".to_owned(),
        observation_version: "public-observation-v1".to_owned(),
        ordering_version: "original-input-v1".to_owned(),
        input_scope: "constructed-finite".to_owned(),
        max_pending_ws_terminals: 2,
        max_retained_reducer_state_entries: 163840,
        initial_state: PublicInitialStateV1::Empty,
    })
    .unwrap()
}

fn record(position: u64, row: TypedRawRowV2) -> RecordedIngressV1 {
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

fn meta(position: u64) -> RecordedIngressV1 {
    record(
        position,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketMarketMetadata(Box::new(
            metadata_snapshot_row(),
        ))),
    )
}

fn pm(position: u64, seq: u64, source: i64, event: PolymarketOrderbookEvent) -> RecordedIngressV1 {
    record(
        position,
        TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row(
            "epoch-a", seq, source, 22_000, event,
        ))),
    )
}

fn twap_record(position: u64, window: u64, received: i64, price: &str) -> RecordedIngressV1 {
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

fn rule_record(position: u64, window: u32, price: Option<&str>) -> RecordedIngressV1 {
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
fn config() -> GateAOutputConfig {
    let mut config = crate::gate_a_formal_output_config();
    config.sort.batch_rows = 2;
    config.sort.max_run_rows = 4;
    config.sort.max_run_bytes = 1 << 20;
    config.max_artifact_payload_bytes = 1 << 19;
    config
}
fn end(position: u64) -> RecordedIngressV1 {
    RecordedIngressV1 {
        input_stream_id: "core-test".into(),
        input_position: position,
        ingress: DptIngressV1::EndOfInput,
        dependency_record_refs: vec![],
    }
}
fn apply(
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

fn snapshot(epoch: &str, last_update_id: u64, admission_sequence: u64) -> BinanceDepthSnapshotRow {
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

fn update(
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
