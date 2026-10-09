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
    open_gate_a_artifacts,
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
        crate::freeze_direct_source_input_set(&self.manifests)
            .expect("夹具必须通过实际公共输入入口")
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
        assert_eq!(candidate.peak_observation_l2_levels, 0);
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
            candidate
                .canonical_event_store
                .manifest
                .input_manifest_set_sha256
                .0,
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
        for candidate in [&first, &second] {
            open_gate_a_artifacts(&candidate.candidate_path).unwrap();
        }
    }
}

#[test]
fn selected_file_damage_after_freeze_is_rejected_by_actual_build() {
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
    let path = input.admitted_daily_inputs()[0].files()[0].file_path();
    let mut bytes = fs::read(path).unwrap();
    let offset = bytes.len() / 2;
    bytes[offset] ^= 1;
    fs::write(path, bytes).unwrap();
    let output = TempDir::new().unwrap();
    assert!(build(&input, &scope(), &output.path().join("candidate"), false).is_err());
}

#[test]
fn explicit_input_paths_and_pre_floor_date_build_publish_and_read() {
    let day = "2026-07-29";
    let day_start = 1_785_283_200_000;
    let at = day_start + 1000;
    let mut fixture = Fixture {
        root: tempfile::tempdir_in(".").unwrap(),
        manifests: Vec::new(),
    };
    fixture.day(
        day,
        day_start,
        vec![
            (
                POLYMARKET_METADATA_SOURCE,
                POLYMARKET_METADATA_SCHEMA,
                vec![metadata(at)],
            ),
            (
                POLYMARKET_ORDERBOOK_SOURCE,
                POLYMARKET_ORDERBOOK_SCHEMA,
                vec![bba(at + 102)],
            ),
        ],
    );
    let original = fixture.freeze();
    let selected = CanonicalRunScope::new(CONDITION, at, at + 200).unwrap();
    let outputs = TempDir::new().unwrap();
    let expected = build(
        &original,
        &selected,
        &outputs.path().join("original"),
        false,
    )
    .unwrap();
    assert!(!observations(&expected).is_empty());

    let relocated = fixture.root.path().join("arbitrary-input");
    fs::create_dir_all(&relocated).unwrap();
    let old_manifest = &fixture.manifests[0];
    let mut manifest: Value = serde_json::from_slice(&fs::read(old_manifest).unwrap()).unwrap();
    for (index, entry) in manifest["files"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .enumerate()
    {
        let renamed = format!("nested/materials/row-{index}.bin");
        let destination = relocated.join(&renamed);
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        fs::rename(
            old_manifest
                .parent()
                .unwrap()
                .join(entry["path"].as_str().unwrap()),
            destination,
        )
        .unwrap();
        entry["path"] = json!(renamed);
    }
    fs::write(relocated.join("unlisted-running.parquet"), b"unrelated").unwrap();
    let manifest_path = relocated.join("explicit-input.json");
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let input = crate::freeze_direct_source_input_set(&[manifest_path.clone()]).unwrap();
    let initial_manifest_hash = input.admitted_daily_inputs()[0]
        .manifest_sha256()
        .to_owned();
    fs::remove_file(&manifest_path).unwrap();
    let actual = build(&input, &selected, &outputs.path().join("relocated"), false).unwrap();
    let actual_observations = observations(&actual);
    let expected_observations = observations(&expected);
    assert_eq!(actual_observations.len(), expected_observations.len());
    for (actual, expected) in actual_observations.iter().zip(&expected_observations) {
        assert_eq!(actual.market, expected.market);
        assert_eq!(actual.visibility_cutoff, expected.visibility_cutoff);
        assert_eq!(actual.component_states, expected.component_states);
        assert_eq!(actual.missing_mask, expected.missing_mask);
        assert_eq!(
            actual.market_state_digest_sha256,
            expected.market_state_digest_sha256
        );
    }
    // 真实定位 manifest 的字节会随产物路径变化，lineage/观测身份不承诺逐字相同。

    let actual_records = records(&actual);
    let expected_records = records(&expected);
    assert_eq!(actual_records.len(), expected_records.len());
    for (actual, expected) in actual_records.iter().zip(&expected_records) {
        let actual = actual.as_execution_market_event().unwrap();
        let expected = expected.as_execution_market_event().unwrap();
        assert_eq!(actual.event_id, expected.event_id);
        assert_eq!(actual.market, expected.market);
        assert_eq!(actual.time, expected.time);
        assert_eq!(actual.payload, expected.payload);
        assert_eq!(actual.quality, expected.quality);
        assert_eq!(
            actual.lineage.source_evidence.len(),
            expected.lineage.source_evidence.len()
        );
        for (actual_source, expected_source) in actual
            .lineage
            .source_evidence
            .iter()
            .zip(&expected.lineage.source_evidence)
        {
            assert_eq!(actual_source.input_manifest_sha256.0, initial_manifest_hash);
            assert_eq!(
                expected_source.input_manifest_sha256.0,
                original.admitted_daily_inputs()[0].manifest_sha256()
            );
            assert_eq!(
                actual_source.input_file_sha256,
                expected_source.input_file_sha256
            );
            assert_eq!(
                actual_source.source_native_identity,
                expected_source.source_native_identity
            );
        }
    }
    assert_eq!(
        input.admitted_daily_inputs()[0].manifest_sha256(),
        initial_manifest_hash
    );
    publication_tests::assert_publication(&input, &actual);

    let narrower = CanonicalRunScope::new(CONDITION, at, at + 50).unwrap();
    let narrowed = build(&input, &narrower, &outputs.path().join("narrower"), false).unwrap();
    assert!(records(&narrowed).len() < actual_records.len());
    assert!(records(&narrowed).iter().all(|record| {
        record
            .as_execution_market_event()
            .unwrap()
            .time
            .received_at_utc_ms
            < at + 50
    }));
    let other = CanonicalRunScope::new(&format!("0x{}", "1".repeat(64)), at, at + 200).unwrap();
    assert!(matches!(
        build(&input, &other, &outputs.path().join("other-market"), false),
        Err(GateAError::CanonicalRun(
            CanonicalRunError::MissingIdentitySnapshot
        ))
    ));
}

#[test]
fn byte_window_complete_multi_page_stream_and_actual_file_reads() {
    use super::byte_window::{WindowedFile, tests::CountedFile};
    use parquet::file::reader::ChunkReader;
    let root = TempDir::new().unwrap();
    let path = root.path().join("multi-page.parquet");
    let schema = expected_schema(POLYMARKET_ORDERBOOK_SOURCE, POLYMARKET_ORDERBOOK_SCHEMA).unwrap();
    let rows: Vec<_> = (0..1024)
        .map(|i| {
            let mut row = bba(START + i);
            // 有效独特来源字符串形成跨 64KiB 的列和大页请求，不使用损坏负载。
            row["source_endpoint"] = json!(format!(
                "wss://ws-subscriptions-clob.polymarket.com/ws/market/{i}/{}",
                "x".repeat(256)
            ));
            row
        })
        .collect();
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_data_page_size_limit(128 * 1024)
        .set_write_batch_size(32)
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(&path).unwrap(),
        schema.clone(),
        Some(properties),
    )
    .unwrap();
    for part in rows.chunks(512) {
        let arrays = schema.fields().iter().map(|f| column(f, part)).collect();
        writer
            .write(&RecordBatch::try_new(schema.clone(), arrays).unwrap())
            .unwrap();
        writer.flush().unwrap();
    }
    let metadata = writer.close().unwrap();
    assert!(
        metadata
            .row_groups()
            .iter()
            .any(|g| g.columns().iter().any(|c| c.compressed_size() > 64 * 1024))
    );
    let raw = fs::read(&path).unwrap();
    let digest = format!("{:x}", Sha256::digest(&raw));
    let facts = || ParquetInputFacts {
        path: &path,
        source_kind: POLYMARKET_ORDERBOOK_SOURCE,
        schema_version: POLYMARKET_ORDERBOOK_SCHEMA,
        bytes: raw.len() as u64,
        rows: rows.len() as u64,
        sha256: &digest,
    };
    let original = CountedFile::new(File::open(&path).unwrap());
    let window = WindowedFile::new(File::open(&path).unwrap());
    let mut old = stream_with_chunk_reader(
        facts(),
        37,
        None,
        None,
        expected_schema,
        decode_typed_raw_record_batch,
        original.clone(),
        0,
    )
    .unwrap();
    let mut new = stream_with_chunk_reader(
        facts(),
        37,
        None,
        None,
        expected_schema,
        decode_typed_raw_record_batch,
        window.clone(),
        0,
    )
    .unwrap();
    let mut plain = stream_with_chunk_reader(
        facts(),
        37,
        None,
        None,
        expected_schema,
        decode_typed_raw_record_batch,
        File::open(&path).unwrap(),
        0,
    )
    .unwrap();
    let mut total = 0;
    loop {
        let a = old.next();
        let b = new.next();
        let c = plain.next();
        assert_eq!(
            a.as_ref().map(|r| r.as_ref().unwrap()),
            c.as_ref().map(|r| r.as_ref().unwrap())
        );
        assert_eq!(
            a.as_ref().map(|r| r.as_ref().unwrap()),
            b.as_ref().map(|r| r.as_ref().unwrap())
        );
        assert_eq!(
            (
                old.next_row_index,
                old.next_batch_index,
                old.last_physical_batch_rows
            ),
            (
                new.next_row_index,
                new.next_batch_index,
                new.last_physical_batch_rows
            )
        );
        match a {
            Some(Ok(batch)) => total += batch.len(),
            None => break,
            Some(Err(error)) => panic!("{error}"),
        }
    }
    assert_eq!(total, rows.len());
    assert!(old.next().is_none() && new.next().is_none());
    let old_counts = original.counts();
    let new_counts = window.read_counts();
    eprintln!(
        "byte-window fixture actual File read calls/bytes: old={old_counts:?}, new={new_counts:?}"
    );
    assert!(
        new_counts.0 < old_counts.0 && new_counts.1 < old_counts.1,
        "{old_counts:?} vs {new_counts:?}"
    );
    // 原生产 File 路径完整值也与同一计数包装相等；Bytes 返回自持有。
    assert_eq!(
        File::open(&path)
            .unwrap()
            .get_bytes(0, raw.len())
            .unwrap()
            .as_ref(),
        raw
    );
}

#[test]
fn byte_window_outside_scope_bad_later_page_keeps_original_batch_error() {
    let root = TempDir::new().unwrap();
    let path = root.path().join("bad-later-page.parquet");
    let schema = expected_schema(POLYMARKET_ORDERBOOK_SOURCE, POLYMARKET_ORDERBOOK_SCHEMA).unwrap();
    let mut writer = ArrowWriter::try_new(
        File::create(&path).unwrap(),
        schema.clone(),
        Some(
            WriterProperties::builder()
                .set_dictionary_enabled(false)
                .build(),
        ),
    )
    .unwrap();
    for at in [START, START + 1000] {
        let rows = [bba(at)];
        let arrays = schema.fields().iter().map(|f| column(f, &rows)).collect();
        writer
            .write(&RecordBatch::try_new(schema.clone(), arrays).unwrap())
            .unwrap();
        writer.flush().unwrap();
    }
    let metadata = writer.close().unwrap();
    let offset = metadata.row_group(1).column(0).data_page_offset() as usize;
    let mut raw = fs::read(&path).unwrap();
    // 原第二行组页头损坏；其真实接收时间原在研究范围外。
    raw[offset..offset + 16].fill(0xff);
    fs::write(&path, &raw).unwrap();
    let digest = format!("{:x}", Sha256::digest(&raw));
    let facts = || ParquetInputFacts {
        path: &path,
        source_kind: POLYMARKET_ORDERBOOK_SOURCE,
        schema_version: POLYMARKET_ORDERBOOK_SCHEMA,
        bytes: raw.len() as u64,
        rows: 2,
        sha256: &digest,
    };
    let scope = Some(TypedRawReadScope::new(START, START + 100, CONDITION));
    let mut old = stream_with_chunk_reader(
        facts(),
        1,
        scope.clone(),
        None,
        expected_schema,
        decode_typed_raw_record_batch,
        File::open(&path).unwrap(),
        0,
    )
    .unwrap();
    let mut new = open_parquet_with_decoder(
        facts(),
        1,
        scope,
        None,
        expected_schema,
        decode_typed_raw_record_batch,
    )
    .unwrap();
    // 生产入口包裹同一打开 File；明确有效前批先交付。
    assert_eq!(old.next().unwrap().unwrap(), new.next().unwrap().unwrap());
    let old_error = old.next().unwrap().unwrap_err();
    let new_error = new.next().unwrap().unwrap_err();
    assert!(
        matches!(&old_error, TypedRawReadError::Parquet { path: p, batch_index: Some(1), .. } if p == &path)
    );
    assert_eq!(old_error.to_string(), new_error.to_string());
    assert!(old.next().is_none() && new.next().is_none());
}

#[test]
fn range_received_outside_missing_pm_timestamp_keeps_second_pass_source_error() {
    use crate::historical_preparation_progress::{
        HistoricalPreparationNoticeV1 as Notice, HistoricalPreparationNotifierV1,
        HistoricalPreparationStageV1 as Stage,
    };
    use crate::public_replay::*;
    use data_pipeline_core::public_reference::*;
    let mut bad = bba(START);
    bad["timestamp"] = Value::Null;
    let mut fixture = Fixture::new();
    fixture.day(
        DAY,
        DAY_START,
        vec![(
            POLYMARKET_ORDERBOOK_SOURCE,
            POLYMARKET_ORDERBOOK_SCHEMA,
            vec![bad],
        )],
    );
    let input = fixture.freeze();
    let files: Vec<_> = input
        .admitted_daily_inputs()
        .iter()
        .flat_map(|d| d.files())
        .collect();
    let config = gate_a_formal_canonical_config();
    let range = (START + 1)..(START + 2); // 真正窗口外；不是用 Err 提前返回。
    let rows: Vec<_> = crate::public_input::open_public_input_bounded_v1(files[0], 1, 1 << 20)
        .unwrap()
        .flat_map(|b| b.unwrap())
        .collect();
    assert_eq!(rows.len(), 1);
    let original_error = source_evidence_v3_from_typed_raw_v2(&rows[0]).unwrap_err();
    assert!(
        matches!(&original_error, data_pipeline_core::ContractError::InvalidField { field, .. } if field == "lineage.source_evidence[0].source_event_at_utc_ms")
    );
    // 原完整来源筛选在实际两遍准备中保留错误行，第二遍才正式构造失败。
    let mut passes = 0;
    let old = prepare_historical_rows_v1(
        &fixture.root.path().join("old"),
        &config,
        "old".into(),
        || {
            passes += 1;
            Ok(rows
                .clone()
                .into_iter()
                .filter(|row| {
                    source_evidence_v3_from_typed_raw_v2(row)
                        .map(|s| range.contains(&s.received_at_utc_ms))
                        .unwrap_or(true)
                })
                .map(Ok))
        },
    )
    .err()
    .unwrap();
    assert_eq!(passes, 2);
    assert_eq!(
        old.downcast_ref::<data_pipeline_core::ContractError>(),
        Some(&original_error)
    );
    let mut stages = Vec::new();
    let mut callback = |notice| {
        if let Notice::Stage(stage) = notice {
            stages.push(stage)
        }
    };
    let new = {
        let notifier = HistoricalPreparationNotifierV1::new(&mut callback);
        prepare_historical_files_with_initialization_and_progress_v1(
            &fixture.root.path().join("new"),
            &files,
            &config,
            "new".into(),
            Some(range),
            None,
            Some(&notifier),
        )
        .err()
        .unwrap()
    };
    assert_eq!(
        stages,
        vec![Stage::EpochPass, Stage::SecondSupplyAndInitialSort]
    );
    assert_eq!(
        new.downcast_ref::<data_pipeline_core::ContractError>(),
        Some(&original_error)
    );
}
