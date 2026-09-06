//! 从真实物理格式和完整日清单进入唯一公开构建链的来源缺失回归。

#[path = "zero_source_publication_tests.rs"]
mod publication_tests;

use super::*;
use std::fs;
use std::path::Path;

use arrow_array::{ArrayRef, new_null_array};
use contracts::{
    CanonicalEventRecordV1, CanonicalReferenceEventPayloadV1, MarketDataQuality,
    MarketObservationCausalShellV3, ObservationComponentId, ObservationComponentStateV2,
};
use parquet::arrow::ArrowWriter;
use parquet::file::{metadata::KeyValue, properties::WriterProperties};
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::canonical_run::{CanonicalRunError, CanonicalRunScope, SourceRowCounts};
use crate::gate_a::{
    CanonicalEventStoreCursor, EventTimeIndexCursor, GateACandidate, GateAError,
    MarketObservationCursor, ReductionReportCursor, VisibilityIndexCursor, build_gate_a_candidate,
    validate_gate_a_candidate,
};
use crate::gate_a_publication::{gate_a_formal_canonical_config, gate_a_formal_output_config};

const CONDITION: &str = "0xfdf68d373e45f59d9bb1ebc776b3a45eaaaf5f6845b52d9f51dae9bb90394169";
const START: i64 = 1_786_162_857_624;
const DAY: &str = "2026-08-08";
const DAY_START: i64 = 1_786_147_200_000;
const DAY_MS: i64 = 86_400_000;

fn metadata(at: i64) -> Value {
    let payload = r#"{"orderPriceMinTickSize":"0.01"}"#;
    json!({
        "metadata_row_kind": "metadata_snapshot", "timestamp_received": at,
        "collector_admission_sequence": 1, "collector_admission_time_utc_ms": at + 1,
        "metadata_origin": "gamma_markets_discovery", "collector_host_id": "collector-1",
        "collector_region": "eu-west-1", "schema_version": POLYMARKET_METADATA_SCHEMA,
        "slug": "btc-updown-5m", "condition_id": CONDITION, "clob_market_id": "3195576",
        "asset_ids": "[\"20\",\"3\"]", "market_start_utc": at + 100,
        "market_end_utc": at + 300_100, "fixed_identity_verified_at_utc": at - 1,
        "market_lifecycle_state": "prefetched", "market_runtime_role": "next",
        "source_endpoint": "https://gamma-api.polymarket.com/markets",
        "source_evidence_origin": "gamma_markets_discovery",
        "source_evidence_event_type": "gamma_markets_discovery_response",
        "source_evidence_received_at_utc": at, "row_evidence_validation_completed_at_utc": at + 1,
        "source_evidence_payload_json": payload,
        "source_evidence_raw_sha256": format!("{:x}", Sha256::digest(payload)),
        "metadata_fetch_start_utc": at - 10, "closed": false,
        "outcomes_raw": "[\"UP\",\"DOWN\"]", "outcome_prices_raw": "[\"0.5\",\"0.5\"]",
        "active": true, "accepting_orders": true
    })
}

fn bba(at: i64) -> Value {
    json!({
        "timestamp_received": at, "timestamp": at - 2, "event_type": "best_bid_ask",
        "market": CONDITION, "asset_id": "20", "best_bid": "0.37", "best_ask": "0.38",
        "spread": "0.01", "collector_host_id": "collector-1", "collector_region": "eu-west-1",
        "source_endpoint": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        "schema_version": POLYMARKET_ORDERBOOK_SCHEMA, "source_sequence_epoch_id": "market-epoch-1",
        "collector_admission_sequence": 1, "collector_admission_time_utc_ms": at + 1
    })
}

fn chainlink(at: i64, price: &str) -> Value {
    json!({
        "timestamp_received": at, "message_timestamp": at - 1, "payload_timestamp": at - 3,
        "age_ms": 3, "symbol": "btc/usd", "value": price, "topic": "crypto_prices_chainlink",
        "message_type": "update", "freshness_status": "fresh", "subscription_filter": "{\"symbol\":\"btc/usd\"}",
        "source_native_payload_json": json!({"topic": "crypto_prices_chainlink", "type": "update",
            "timestamp": at - 1, "payload": {"symbol": "btc/usd", "timestamp": at - 3, "value": price}}).to_string(),
        "collector_host_id": "collector-1", "collector_region": "eu-west-1",
        "source_endpoint": "wss://ws-live-data.polymarket.com", "schema_version": POLYMARKET_RTDS_SCHEMA
    })
}

fn snapshot(at: i64, sequence: u64, last: u64) -> Value {
    json!({
        "timestamp_received": at, "snapshot_fetch_start_utc": at - 5, "last_update_id": last,
        "bids": "[[\"64000\",\"0.5\"]]", "asks": "[[\"64200\",\"0.4\"]]", "symbol": "BTCUSDT",
        "source_endpoint": "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000",
        "snapshot_status": "complete", "source_sequence_epoch_id": "depth-epoch-1",
        "connection_generation": 1, "depth_ingress_sequence": sequence,
        "collector_admission_sequence": sequence, "collector_admission_time_utc_ms": at + 1,
        "collector_host_id": "collector-1", "collector_region": "eu-west-1",
        "schema_version": BINANCE_DEPTH_SNAPSHOT_SCHEMA
    })
}

fn update(at: i64, verdict: &str) -> Value {
    let unavailable = verdict == "not_applied_snapshot_unavailable";
    let hole = verdict == "not_applied_confirmed_hole";
    json!({
        "timestamp_received": at, "event_type": "depthUpdate", "event_time": at - 2,
        "symbol": "BTCUSDT", "first_update_id": if hole { 102 } else { 101 },
        "final_update_id": if hole { 102 } else { 101 },
        "bids": "[[\"64000\",\"0.7\"]]", "asks": "[]", "stream": "btcusdt@depth",
        "snapshot_last_update_id": if unavailable { None } else { Some(100) }, "local_u_before": null,
        "stitch_verdict": verdict,
        "stitch_evidence": if unavailable { None } else if hole { Some("U > local_u + 1") } else { Some("continuous_depth_update") },
        "stitch_gap_reason": if hole { Some("U > local_u + 1") } else { None },
        "source_sequence_epoch_id": "depth-epoch-1", "connection_generation": 1,
        "depth_ingress_sequence": 2, "collector_admission_sequence": 2,
        "collector_admission_time_utc_ms": at + 1, "collector_host_id": "collector-1",
        "collector_region": "eu-west-1", "source_endpoint": "wss://stream.binance.com:9443/ws/btcusdt@depth",
        "schema_version": BINANCE_DEPTH_UPDATE_SCHEMA
    })
}

fn trade(at: i64, id: u64, price: &str) -> Value {
    json!({
        "record_kind": "trade", "timestamp_received": at, "ingest_source": "websocket",
        "event_type": "aggTrade", "event_time": at - 2, "trade_time": at - 3,
        "symbol": "BTCUSDT", "aggregate_trade_id": id, "price": price, "quantity": "0.01",
        "first_trade_id": id * 10, "last_trade_id": id * 10 + 1,
        "buyer_is_maker": false, "stream": "btcusdt@aggTrade", "ws_ignore": false,
        "source_native_payload_json": json!({"e": "aggTrade", "E": at - 2, "T": at - 3,
            "s": "BTCUSDT", "a": id, "p": price, "q": "0.01", "f": id * 10,
            "l": id * 10 + 1, "m": false, "M": false}).to_string(),
        "source_sequence_epoch_id": "aggtrade-epoch-1", "connection_generation": 1,
        "websocket_ingress_sequence": id, "collector_admission_sequence": id,
        "collector_admission_time_utc_ms": at + 1, "collector_host_id": "collector-1",
        "collector_region": "eu-west-1", "source_endpoint": "wss://stream.binance.com:9443/ws/btcusdt@aggTrade",
        "schema_version": BINANCE_AGGTRADE_SCHEMA
    })
}

// 仅为微型夹具编码物理列；所有输入随后经正式解码、原始类型验证及归约。
fn column(field: &Field, rows: &[Value]) -> ArrayRef {
    let values: Vec<_> = rows.iter().map(|row| &row[field.name()]).collect();
    if values.iter().all(|value| value.is_null()) {
        return new_null_array(field.data_type(), rows.len());
    }
    match field.data_type() {
        DataType::Utf8 => Arc::new(StringArray::from(
            values.iter().map(|v| v.as_str()).collect::<Vec<_>>(),
        )),
        DataType::Boolean => Arc::new(BooleanArray::from(
            values.iter().map(|v| v.as_bool()).collect::<Vec<_>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(
            values.iter().map(|v| v.as_i64()).collect::<Vec<_>>(),
        )),
        DataType::UInt64 => Arc::new(UInt64Array::from(
            values.iter().map(|v| v.as_u64()).collect::<Vec<_>>(),
        )),
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => Arc::new(
            TimestampMillisecondArray::from(values.iter().map(|v| v.as_i64()).collect::<Vec<_>>())
                .with_timezone_opt(timezone.clone()),
        ),
        other => panic!("夹具未使用的非空列类型：{other:?}"),
    }
}

struct Fixture {
    root: TempDir,
    manifests: Vec<PathBuf>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            root: TempDir::new().unwrap(),
            manifests: Vec::new(),
        }
    }

    fn day(&mut self, day: &str, day_start: i64, sources: Vec<(&str, &str, Vec<Value>)>) {
        let directory = self
            .root
            .path()
            .join(format!("{day}/typed_raw/utc_day={day}"));
        fs::create_dir_all(&directory).unwrap();
        let mut files = Vec::new();
        for (source, version, rows) in sources {
            let hour = rows.first().map_or((START - DAY_START) / 3_600_000, |row| {
                (row["timestamp_received"].as_i64().unwrap() - day_start) / 3_600_000
            });
            let relative = format!("source_kind={source}/{source}_{hour:02}.parquet");
            let path = directory.join(&relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            let fact = json!({"source_kind": source, "schema_version": version,
                "utc_hour": format!("{day}T{hour:02}"), "part": 0, "rows": rows.len()});
            let properties = WriterProperties::builder()
                .set_key_value_metadata(Some(vec![
                    KeyValue::new("parquet_raw_source_kind".to_owned(), source.to_owned()),
                    KeyValue::new("parquet_raw_layout_version".to_owned(), version.to_owned()),
                    KeyValue::new(
                        "data_collect_artifact_kind".to_owned(),
                        "typed_raw_parquet".to_owned(),
                    ),
                    KeyValue::new(
                        "data_collect_daily_file_fact_v1".to_owned(),
                        fact.to_string(),
                    ),
                ]))
                .build();
            let schema = expected_schema(source, version).unwrap();
            let mut writer = ArrowWriter::try_new(
                File::create(&path).unwrap(),
                schema.clone(),
                Some(properties),
            )
            .unwrap();
            if !rows.is_empty() {
                let arrays = schema
                    .fields()
                    .iter()
                    .map(|field| column(field, &rows))
                    .collect();
                writer
                    .write(&RecordBatch::try_new(schema, arrays).unwrap())
                    .unwrap();
            }
            writer.close().unwrap();
            let bytes = fs::read(&path).unwrap();
            let mut entry = fact;
            entry["path"] = json!(relative);
            entry["bytes"] = json!(bytes.len());
            entry["sha256"] = json!(format!("{:x}", Sha256::digest(bytes)));
            files.push(entry);
        }
        let manifest = directory.join("manifest.json");
        fs::write(&manifest, serde_json::to_vec(&json!({
            "schema_version": "data_collect_daily_manifest_v3", "state": "closed",
            "utc_day": day, "closure_reason": "utc_day_barrier",
            "collection_day_coverage": {"schema_version": "data_collect_collection_day_coverage_v1",
                "state": "ended_known", "first_started_utc_ms": day_start,
                "last_started_utc_ms": day_start, "ended_utc_ms": day_start + DAY_MS,
                "reactivated_within_day": false},
            "producer_barriers": ["binance_aggtrade", "binance_depth", "polymarket", "polymarket_rtds"],
            "writer_finalize_acks": ["binance_aggtrade", "binance_depth_snapshot", "binance_depth_update",
                "polymarket_market", "polymarket_market_metadata", "polymarket_rtds_chainlink_btc_usd"],
            "files": files
        })).unwrap()).unwrap();
        self.manifests.push(manifest);
    }

    fn freeze(&self) -> crate::FrozenDirectSourceInputSetV1 {
        let mut input =
            crate::freeze_direct_source_input_set_with_admission(&self.manifests, |path| {
                crate::validate_daily_manifest_admission_at_root(self.root.path(), path)
            })
            .expect("夹具必须通过完整清单及文件准入");
        input.test_admission_root = Some(self.root.path().to_path_buf());
        input
    }
}

fn complete_sources() -> Vec<(&'static str, &'static str, Vec<Value>)> {
    vec![
        (
            POLYMARKET_METADATA_SOURCE,
            POLYMARKET_METADATA_SCHEMA,
            vec![metadata(START)],
        ),
        (
            POLYMARKET_ORDERBOOK_SOURCE,
            POLYMARKET_ORDERBOOK_SCHEMA,
            vec![bba(START + 10)],
        ),
        (
            POLYMARKET_RTDS_SOURCE,
            POLYMARKET_RTDS_SCHEMA,
            vec![chainlink(START + 20, "64001")],
        ),
        (
            BINANCE_DEPTH_SNAPSHOT_SOURCE,
            BINANCE_DEPTH_SNAPSHOT_SCHEMA,
            vec![snapshot(START + 30, 1, 100)],
        ),
        (
            BINANCE_DEPTH_UPDATE_SOURCE,
            BINANCE_DEPTH_UPDATE_SCHEMA,
            vec![update(START + 40, "applied")],
        ),
        (
            BINANCE_AGGTRADE_SOURCE,
            BINANCE_AGGTRADE_SCHEMA,
            vec![trade(START + 50, 1, "64002")],
        ),
    ]
}

fn build(
    input: &crate::FrozenDirectSourceInputSetV1,
    scope: &CanonicalRunScope,
    path: &Path,
    perturbed: bool,
) -> Result<GateACandidate, GateAError> {
    let mut config = gate_a_formal_canonical_config();
    let mut output = gate_a_formal_output_config();
    if perturbed {
        config.decode_batch_rows = 1;
        config.sort.max_run_rows = 2;
        config.sort.batch_rows = 1;
        output.sort.max_run_rows = 2;
        output.sort.batch_rows = 1;
    }
    build_gate_a_candidate(input, scope, path, &config, &output)
}

fn scope() -> CanonicalRunScope {
    CanonicalRunScope::new(CONDITION, START, START + 100).unwrap()
}

fn observations(candidate: &GateACandidate) -> Vec<MarketObservationCausalShellV3> {
    let mut cursor = MarketObservationCursor::open(
        &candidate.market_observations.manifest_path,
        None,
        None,
        2,
        16 * 1024 * 1024,
        100,
    )
    .unwrap();
    let mut rows = Vec::new();
    loop {
        let page = cursor.next_page(100).unwrap();
        if page.is_empty() {
            return rows;
        }
        rows.extend(page);
    }
}

fn records(candidate: &GateACandidate) -> Vec<CanonicalEventRecordV1> {
    let mut cursor = CanonicalEventStoreCursor::open(
        &candidate.canonical_event_store.manifest_path,
        2,
        16 * 1024 * 1024,
        100,
    )
    .unwrap();
    let mut rows = Vec::new();
    loop {
        let page = cursor.next_page(100).unwrap();
        if page.is_empty() {
            return rows;
        }
        rows.extend(page);
    }
}

fn reports(
    candidate: &GateACandidate,
) -> Vec<data_pipeline_core::reduction::ReductionReportEntryV1> {
    let mut cursor = ReductionReportCursor::open(
        &candidate.reduction_reports.manifest_path,
        2,
        16 * 1024 * 1024,
        100,
    )
    .unwrap();
    let mut rows = Vec::new();
    loop {
        let page = cursor.next_page(100).unwrap();
        if page.is_empty() {
            return rows;
        }
        rows.extend(page);
    }
}

fn component(
    observation: &MarketObservationCausalShellV3,
    id: ObservationComponentId,
) -> &ObservationComponentStateV2 {
    observation
        .component_states
        .iter()
        .find(|state| state.component == id)
        .unwrap()
}

fn assert_unknown(observation: &MarketObservationCausalShellV3, id: ObservationComponentId) {
    let state = component(observation, id);
    assert_eq!(state.quality, MarketDataQuality::UnknownNotObserved);
    assert_eq!(state.source_event_at_utc_ms, None);
    assert_eq!(state.received_at_utc_ms, None);
    assert_eq!(state.last_visible_event_id, None);
    assert_eq!(state.state_digest_sha256, None);
    assert_eq!(state.information_age_ms, None);
    assert_eq!(state.observed_transport_delay_ms, None);
    assert!(observation.missing_mask.contains(&id));
}

#[test]
fn zero_source_three_physical_inputs_use_the_public_candidate_builder() {
    for mode in 0..3 {
        let mut sources = complete_sources();
        match mode {
            0 => {
                sources.remove(2);
            }
            1 => sources[2].2.clear(),
            2 => sources[2].2 = vec![chainlink(START + 100, "99999")],
            _ => unreachable!(),
        }
        let mut fixture = Fixture::new();
        fixture.day(DAY, DAY_START, sources);
        let input = fixture.freeze();
        let output = TempDir::new().unwrap();
        let candidate = build(&input, &scope(), &output.path().join("candidate"), false)
            .expect("合法零参考来源应完成公开候选构建");
        publication_tests::assert_publication(&input, &candidate);
        let reference_file = candidate
            .selected_files
            .iter()
            .find(|file| file.source_kind == POLYMARKET_RTDS_SOURCE);
        match mode {
            0 => assert!(reference_file.is_none()),
            1 => assert_eq!(
                reference_file.unwrap().rows,
                0,
                "合法空文件必须实际进入所选文件集"
            ),
            2 => assert_eq!(reference_file.unwrap().rows, 1, "非空文件的行由范围排除"),
            _ => unreachable!(),
        }
        assert_eq!(
            candidate.canonical_metrics.selected_rows,
            SourceRowCounts {
                polymarket_orderbook: 1,
                polymarket_market_metadata: 1,
                polymarket_rtds_chainlink_btc_usd: 0,
                binance_depth_snapshot: 1,
                binance_depth_update: 1,
                binance_aggtrade: 1,
            }
        );
        let rows = observations(&candidate);
        assert!(!rows.is_empty());
        for row in &rows {
            assert_unknown(row, ObservationComponentId::ChainlinkBtcUsd);
        }
        assert_eq!(
            rows.last().unwrap().visibility_cutoff.received_at_utc_ms,
            START + 50
        );
        assert_eq!(
            component(
                rows.last().unwrap(),
                ObservationComponentId::BinanceBtcUsdtTrade
            )
            .received_at_utc_ms,
            Some(START + 50)
        );
    }
}

#[test]
fn complete_sources_preserve_fixed_business_values_and_times() {
    let mut fixture = Fixture::new();
    fixture.day(DAY, DAY_START, complete_sources());
    let input = fixture.freeze();
    let output = TempDir::new().unwrap();
    let candidate = build(&input, &scope(), &output.path().join("candidate"), false).unwrap();
    let rows = observations(&candidate);
    assert_eq!(
        rows.iter()
            .map(|row| row.visibility_cutoff.received_at_utc_ms)
            .collect::<Vec<_>>(),
        vec![
            START,
            START + 10,
            START + 20,
            START + 30,
            START + 40,
            START + 50
        ]
    );
    let refs: Vec<_> = records(&candidate)
        .into_iter()
        .filter_map(|record| match record {
            CanonicalEventRecordV1::ReferenceEvent(event) => Some(event),
            _ => None,
        })
        .collect();
    assert_eq!(refs.len(), 4);
    assert_eq!(
        refs.iter()
            .map(|event| event.time.received_at_utc_ms)
            .collect::<Vec<_>>(),
        vec![START + 20, START + 30, START + 40, START + 50]
    );
    assert_eq!(
        refs.iter()
            .map(|event| event.time.source_event_at_utc_ms)
            .collect::<Vec<_>>(),
        vec![Some(START + 17), None, Some(START + 38), Some(START + 47)]
    );
    let CanonicalReferenceEventPayloadV1::ChainlinkBtcUsd(price) = &refs[0].payload else {
        panic!("缺少 Chainlink");
    };
    assert_eq!(price.price_usd_per_btc.0, "64001");
    let CanonicalReferenceEventPayloadV1::BinanceDepthState(depth) = &refs[2].payload else {
        panic!("缺少深度更新");
    };
    assert_eq!(depth.last_update_id, 101);
    assert_eq!(depth.bids[0].price_usdt_per_btc.0, "64000");
    assert_eq!(depth.bids[0].quantity_btc.0, "0.7");
    let CanonicalReferenceEventPayloadV1::BinanceAggregateTrade(trade) = &refs[3].payload else {
        panic!("缺少成交");
    };
    assert_eq!(trade.price_usdt_per_btc.0, "64002");
    assert_eq!(trade.quantity_btc.0, "0.01");
    publication_tests::assert_publication(&input, &candidate);
}

#[test]
fn silence_preserves_original_reference_anchor_until_real_recovery() {
    let mut fixture = Fixture::new();
    fixture.day(
        DAY,
        DAY_START,
        vec![
            (
                POLYMARKET_METADATA_SOURCE,
                POLYMARKET_METADATA_SCHEMA,
                vec![metadata(START)],
            ),
            (
                POLYMARKET_RTDS_SOURCE,
                POLYMARKET_RTDS_SCHEMA,
                vec![
                    chainlink(START + 10, "64001"),
                    chainlink(START + 80, "64003"),
                ],
            ),
            (
                BINANCE_AGGTRADE_SOURCE,
                BINANCE_AGGTRADE_SCHEMA,
                vec![trade(START + 50, 1, "64002"), trade(START + 79, 2, "64004")],
            ),
        ],
    );
    let output = TempDir::new().unwrap();
    let candidate = build(
        &fixture.freeze(),
        &scope(),
        &output.path().join("candidate"),
        false,
    )
    .unwrap();
    let rows = observations(&candidate);
    assert_eq!(
        rows.iter()
            .map(|row| row.visibility_cutoff.received_at_utc_ms)
            .collect::<Vec<_>>(),
        vec![START, START + 10, START + 50, START + 79, START + 80]
    );
    assert_unknown(&rows[0], ObservationComponentId::ChainlinkBtcUsd);
    let first = component(&rows[1], ObservationComponentId::ChainlinkBtcUsd);
    for row in &rows[2..4] {
        let state = component(row, ObservationComponentId::ChainlinkBtcUsd);
        assert_eq!(state.received_at_utc_ms, Some(START + 10));
        assert_eq!(state.source_event_at_utc_ms, Some(START + 7));
        assert_eq!(state.last_visible_event_id, first.last_visible_event_id);
        assert_eq!(state.state_digest_sha256, first.state_digest_sha256);
        assert_eq!(state.quality, MarketDataQuality::Fresh);
        assert!(
            !row.missing_mask
                .contains(&ObservationComponentId::ChainlinkBtcUsd)
        );
    }
    let recovery = component(&rows[4], ObservationComponentId::ChainlinkBtcUsd);
    assert_eq!(recovery.received_at_utc_ms, Some(START + 80));
    assert_eq!(recovery.source_event_at_utc_ms, Some(START + 77));
    assert_ne!(recovery.last_visible_event_id, first.last_visible_event_id);
    publication_tests::assert_publication(&fixture.freeze(), &candidate);
}

#[test]
fn snapshot_only_and_genuine_snapshot_unavailable_keep_distinct_facts() {
    for snapshot_present in [true, false] {
        let depth = if snapshot_present {
            (
                BINANCE_DEPTH_SNAPSHOT_SOURCE,
                BINANCE_DEPTH_SNAPSHOT_SCHEMA,
                vec![snapshot(START + 10, 1, 100)],
            )
        } else {
            (
                BINANCE_DEPTH_UPDATE_SOURCE,
                BINANCE_DEPTH_UPDATE_SCHEMA,
                vec![update(START + 10, "not_applied_snapshot_unavailable")],
            )
        };
        let mut fixture = Fixture::new();
        fixture.day(
            DAY,
            DAY_START,
            vec![
                (
                    POLYMARKET_METADATA_SOURCE,
                    POLYMARKET_METADATA_SCHEMA,
                    vec![metadata(START)],
                ),
                depth,
                (
                    BINANCE_AGGTRADE_SOURCE,
                    BINANCE_AGGTRADE_SCHEMA,
                    vec![trade(START + 50, 1, "64002")],
                ),
            ],
        );
        let output = TempDir::new().unwrap();
        let candidate = build(
            &fixture.freeze(),
            &scope(),
            &output.path().join("candidate"),
            false,
        )
        .unwrap();
        let rows = observations(&candidate);
        let depth_events: Vec<_> = records(&candidate).into_iter().filter(|record| matches!(record,
            CanonicalEventRecordV1::ReferenceEvent(event) if matches!(event.payload, CanonicalReferenceEventPayloadV1::BinanceDepthState(_)))).collect();
        publication_tests::assert_publication(&fixture.freeze(), &candidate);
        if snapshot_present {
            assert_eq!(
                candidate
                    .canonical_metrics
                    .selected_rows
                    .binance_depth_update,
                0
            );
            assert_eq!(depth_events.len(), 1);
            let depth = component(
                rows.last().unwrap(),
                ObservationComponentId::BinanceBtcUsdtDepth,
            );
            assert_eq!(depth.received_at_utc_ms, Some(START + 10));
            assert_eq!(depth.source_event_at_utc_ms, None);
            assert_eq!(depth.quality, MarketDataQuality::Fresh);
            assert!(depth.last_visible_event_id.is_some());
        } else {
            assert_eq!(
                candidate
                    .canonical_metrics
                    .selected_rows
                    .binance_depth_snapshot,
                0
            );
            assert!(depth_events.is_empty());
            assert_eq!(rows.len(), 2, "未应用原始更新不能伪造观测触发");
            for row in &rows {
                assert_unknown(row, ObservationComponentId::BinanceBtcUsdtDepth);
            }
            assert!(reports(&candidate).iter().any(|report| report.reason_code
                == "BINANCE_DEPTH_SNAPSHOT_UNAVAILABLE"
                && report.operation
                    == data_pipeline_core::reduction::ReductionOperationKindV1::Reject));
        }
    }
}

#[test]
fn applied_update_without_its_snapshot_keeps_the_dependency_error() {
    let mut fixture = Fixture::new();
    fixture.day(
        DAY,
        DAY_START,
        vec![
            (
                POLYMARKET_METADATA_SOURCE,
                POLYMARKET_METADATA_SCHEMA,
                vec![metadata(START)],
            ),
            (
                BINANCE_DEPTH_UPDATE_SOURCE,
                BINANCE_DEPTH_UPDATE_SCHEMA,
                vec![update(START + 10, "applied")],
            ),
        ],
    );
    let output = TempDir::new().unwrap();
    let path = output.path().join("candidate");
    let error = build(&fixture.freeze(), &scope(), &path, false).unwrap_err();
    assert!(matches!(
        &error,
        GateAError::CanonicalRun(CanonicalRunError::Contract(_))
    ));
    assert!(
        error
            .to_string()
            .contains("binance_depth_update.stitch_verdict"),
        "{error}"
    );
    assert!(!path.exists());
}

#[test]
fn identity_only_empty_and_missing_identity_do_not_fabricate_observations() {
    for mode in 0..4 {
        let sources = match mode {
            0 => vec![(
                POLYMARKET_METADATA_SOURCE,
                POLYMARKET_METADATA_SCHEMA,
                vec![metadata(START)],
            )],
            1 => vec![],
            2 => vec![(
                BINANCE_AGGTRADE_SOURCE,
                BINANCE_AGGTRADE_SCHEMA,
                vec![trade(START + 10, 1, "64002")],
            )],
            3 => vec![(
                POLYMARKET_ORDERBOOK_SOURCE,
                POLYMARKET_ORDERBOOK_SCHEMA,
                vec![bba(START + 10)],
            )],
            _ => unreachable!(),
        };
        let mut fixture = Fixture::new();
        fixture.day(DAY, DAY_START, sources);
        let output = TempDir::new().unwrap();
        let candidate_path = output.path().join("candidate");
        let result = build(&fixture.freeze(), &scope(), &candidate_path, false);
        if mode != 0 {
            assert!(matches!(
                result,
                Err(GateAError::CanonicalRun(
                    CanonicalRunError::MissingIdentitySnapshot
                ))
            ));
            assert!(!candidate_path.exists(), "无身份或全静默不留下虚假候选");
            continue;
        }
        let candidate = result.unwrap();
        let rows = observations(&candidate);
        assert_eq!(candidate.resource_metrics.peak_observation_l2_levels, 0);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].visibility_cutoff.received_at_utc_ms, START);
        assert_eq!(candidate.event_time_index.manifest.file.rows, 0);
        for id in [
            ObservationComponentId::PolymarketL2,
            ObservationComponentId::BestBidAsk,
            ObservationComponentId::ChainlinkBtcUsd,
            ObservationComponentId::BinanceBtcUsdtDepth,
            ObservationComponentId::BinanceBtcUsdtTrade,
        ] {
            assert_unknown(&rows[0], id);
        }
    }
}

#[test]
fn unrecovered_cross_day_gap_and_real_same_epoch_recovery_are_preserved() {
    for recover in [false, true] {
        let mut fixture = Fixture::new();
        fixture.day(
            DAY,
            DAY_START,
            vec![
                (
                    POLYMARKET_METADATA_SOURCE,
                    POLYMARKET_METADATA_SCHEMA,
                    vec![metadata(START)],
                ),
                (
                    BINANCE_DEPTH_SNAPSHOT_SOURCE,
                    BINANCE_DEPTH_SNAPSHOT_SCHEMA,
                    vec![snapshot(START + 10, 1, 100)],
                ),
                (
                    BINANCE_DEPTH_UPDATE_SOURCE,
                    BINANCE_DEPTH_UPDATE_SCHEMA,
                    vec![update(START + 20, "not_applied_confirmed_hole")],
                ),
            ],
        );
        fixture.day("2026-08-09", DAY_START + DAY_MS, vec![]);
        let later = START + 2 * DAY_MS;
        let mut last_sources = vec![(
            BINANCE_AGGTRADE_SOURCE,
            BINANCE_AGGTRADE_SCHEMA,
            vec![trade(later, 1, "64002")],
        )];
        if recover {
            last_sources.push((
                BINANCE_DEPTH_SNAPSHOT_SOURCE,
                BINANCE_DEPTH_SNAPSHOT_SCHEMA,
                vec![snapshot(later + 10, 3, 200)],
            ));
        }
        fixture.day("2026-08-10", DAY_START + 2 * DAY_MS, last_sources);
        let input = fixture.freeze();
        assert_eq!(input.admitted_daily_inputs().len(), 3);
        assert!(input.admitted_daily_inputs()[1].files().is_empty());
        let output = TempDir::new().unwrap();
        let selected_scope = CanonicalRunScope::new(CONDITION, START, later + 100).unwrap();
        let candidate = build(
            &input,
            &selected_scope,
            &output.path().join("candidate"),
            false,
        )
        .unwrap();
        assert_eq!(candidate.scope, selected_scope);
        publication_tests::assert_publication(&input, &candidate);
        assert_eq!(
            candidate.input_manifest_set_sha256.0,
            input.input_manifest_set_sha256()
        );
        let rows = observations(&candidate);
        let mut expected_times = vec![START, START + 10, START + 20, later];
        if recover {
            expected_times.push(later + 10);
        }
        assert_eq!(
            rows.iter()
                .map(|row| row.visibility_cutoff.received_at_utc_ms)
                .collect::<Vec<_>>(),
            expected_times
        );
        for row in &rows[2..4] {
            let state = component(row, ObservationComponentId::BinanceBtcUsdtDepth);
            assert_eq!(state.quality, MarketDataQuality::ConfirmedGap);
            assert!(
                row.missing_mask
                    .contains(&ObservationComponentId::BinanceBtcUsdtDepth)
            );
            assert_eq!(state.last_visible_event_id, None);
        }
        let gap_events: Vec<_> = records(&candidate)
            .into_iter()
            .filter_map(|record| match record {
                CanonicalEventRecordV1::ReferenceEvent(event)
                    if event.quality.availability == MarketDataQuality::ConfirmedGap =>
                {
                    Some(event)
                }
                _ => None,
            })
            .collect();
        assert_eq!(gap_events.len(), 1);
        assert_eq!(gap_events[0].time.received_at_utc_ms, START + 20);
        let CanonicalReferenceEventPayloadV1::BinanceDepthState(depth) = &gap_events[0].payload
        else {
            panic!("缺少真实缺口状态");
        };
        assert_eq!(depth.last_update_id, 100);
        assert_eq!(
            depth.bids[0].quantity_btc.0, "0.5",
            "拒绝的更新不得改写可信盘口"
        );
        assert!(reports(&candidate).iter().any(|report| report.reason_code
            == "BINANCE_DEPTH_CONFIRMED_HOLE"
            && report.operation
                == data_pipeline_core::reduction::ReductionOperationKindV1::Reject));
        if recover {
            let state = component(
                rows.last().unwrap(),
                ObservationComponentId::BinanceBtcUsdtDepth,
            );
            assert_eq!(state.quality, MarketDataQuality::Fresh);
            assert_eq!(state.received_at_utc_ms, Some(later + 10));
            assert!(state.last_visible_event_id.is_some());
        }
    }
}

#[test]
fn complete_and_zero_source_inputs_are_invariant_to_physical_batching() {
    for zero_source in [false, true] {
        let mut sources = complete_sources();
        if zero_source {
            sources.remove(2);
        }
        // 同一文件含多行，确保解码批次扰动实际发生。
        sources
            .last_mut()
            .unwrap()
            .2
            .push(trade(START + 80, 2, "64003"));
        let mut fixture = Fixture::new();
        fixture.day(DAY, DAY_START, sources);
        let input = fixture.freeze();
        let output = TempDir::new().unwrap();
        let first = build(&input, &scope(), &output.path().join("first"), false).unwrap();
        let second = build(&input, &scope(), &output.path().join("second"), true).unwrap();
        publication_tests::assert_publication(&input, &first);
        assert_ne!(
            first.canonical_metrics.sort.initial_runs,
            second.canonical_metrics.sort.initial_runs
        );
        assert_ne!(
            first.canonical_metrics.peak_decode_batch_rows,
            second.canonical_metrics.peak_decode_batch_rows
        );
        assert_eq!(records(&first), records(&second));
        assert_eq!(reports(&first), reports(&second));
        assert_eq!(
            first.canonical_metrics.selected_rows,
            second.canonical_metrics.selected_rows
        );
        assert_eq!(first.canonical_digests, second.canonical_digests);
        let semantic_observations = |candidate: &GateACandidate| {
            observations(candidate)
                .into_iter()
                .map(|row| {
                    (
                        row.market,
                        row.visibility_cutoff,
                        row.component_states,
                        row.missing_mask,
                        row.market_state_digest_sha256,
                        row.lineage.contributing_event_ids,
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            semantic_observations(&first),
            semantic_observations(&second)
        );
        let event_index = |candidate: &GateACandidate| {
            EventTimeIndexCursor::open(
                &candidate.event_time_index.manifest_path,
                None,
                2,
                16 * 1024 * 1024,
                100,
            )
            .unwrap()
            .next_page(100)
            .unwrap()
        };
        let visibility_index = |candidate: &GateACandidate| {
            VisibilityIndexCursor::open(
                &candidate.visibility_index.manifest_path,
                None,
                2,
                16 * 1024 * 1024,
                100,
            )
            .unwrap()
            .next_page(100)
            .unwrap()
        };
        assert_eq!(event_index(&first), event_index(&second));
        assert_eq!(visibility_index(&first), visibility_index(&second));
        for (name, candidate) in [("validate-first", &first), ("validate-second", &second)] {
            validate_gate_a_candidate(
                &candidate.candidate_path,
                &output.path().join(name),
                &candidate.output_config,
            )
            .unwrap();
        }
    }
}

#[test]
fn frozen_input_rechecks_the_same_complete_admission_after_file_damage() {
    let mut fixture = Fixture::new();
    fixture.day(
        DAY,
        DAY_START,
        vec![(
            POLYMARKET_METADATA_SOURCE,
            POLYMARKET_METADATA_SCHEMA,
            vec![metadata(START)],
        )],
    );
    let input = fixture.freeze();
    input.verify_unchanged().unwrap();
    let path = input.admitted_daily_inputs()[0].files()[0].canonical_path();
    let mut bytes = fs::read(path).unwrap();
    let offset = bytes.len() / 2;
    bytes[offset] ^= 1;
    fs::write(path, bytes).unwrap();
    assert!(matches!(
        input.verify_unchanged(),
        Err(crate::InputSetSnapshotError::Admission { .. })
    ));
    let output = TempDir::new().unwrap();
    assert!(build(&input, &scope(), &output.path().join("candidate"), false).is_err());
}
