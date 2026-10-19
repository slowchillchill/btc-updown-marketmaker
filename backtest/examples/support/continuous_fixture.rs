//! 仅生成有限合成Parquet；不打开真实数据、不运行账户。
#[path = "../../tests/common/mod.rs"]
#[allow(dead_code, unused_imports)]
pub(crate) mod common;
use backtest::*;
use contracts::*;
use data_collect::{
    polymarket_orderbook_v3::*,
    writer::{PolymarketOrderbookV3WriteSequenceState, TypedParquetSegmentWriter},
};
use serde_json::{Value, json};
use std::path::Path;
pub fn build(root: &Path, trade_count: u64) -> ContinuousPublicExportConfigV1 {
    let (run, rules) = common::setup(root);
    let manifest = &run.preparation.scope.manifest_paths[0];
    let day = manifest.parent().unwrap();
    let mut m: Value = serde_json::from_slice(&std::fs::read(manifest).unwrap()).unwrap();
    if trade_count > 0 {
        let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
            day,
            "new-trades",
            POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
            "polymarket_market",
        )
        .unwrap();
        let mut seq = PolymarketOrderbookV3WriteSequenceState::default();
        for chunk in 0..trade_count.div_ceil(1000) {
            let rows = (chunk * 1000..((chunk + 1) * 1000).min(trade_count))
                .map(|i| {
                    let at = common::BASE + 2300 + (i as i64 % 7000);
                    let mut row = common::book(
                        common::FIRST,
                        if i % 2 == 0 { "20" } else { "3" },
                        at,
                        at,
                        i + 10,
                        "0.6",
                    );
                    row.event =
                        PolymarketOrderbookV3Event::LastTradePrice(PolymarketLastTradePriceV3 {
                            asset_id: if i % 2 == 0 { "20" } else { "3" }.into(),
                            price: "0.4".into(),
                            size: if i == 1 { None } else { Some("2".into()) },
                            side: Some("SELL".into()),
                            fee_rate_bps: None,
                        });
                    row
                })
                .collect::<Vec<_>>();
            writer
                .write_polymarket_orderbook_v3_messages(&rows, &mut seq)
                .unwrap();
        }
        writer.append_key_value_metadata("data_collect_daily_file_fact_v1",json!({"source_kind":"polymarket_market","schema_version":POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,"utc_hour":"2026-08-24T00","part":1,"rows":trade_count}).to_string()).unwrap();
        let f = writer.finalize().unwrap();
        m["files"].as_array_mut().unwrap().push(json!({"path":f.final_path.file_name().unwrap().to_str().unwrap(),"source_kind":"polymarket_market","schema_version":POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,"utc_hour":"2026-08-24T00","part":1,"rows":f.row_count,"bytes":std::fs::metadata(&f.final_path).unwrap().len(),"sha256":f.sha256}));
        std::fs::write(manifest, serde_json::to_vec(&m).unwrap()).unwrap();
    }
    ContinuousPublicExportConfigV1 {
        rules: PublicExportRulesV1 {
            snapshot_path: rules.path().into(),
            expected_file_sha256: rules.file_sha256().clone(),
        },
        preparation: run.preparation,
        markets: PublicObservationMarketsV1::All,
        statistics: PublicStatisticsConfigV1 {
            momentum_windows_ms: vec![250, 500, 1000, 2000, 5000],
            spot_variance_windows_ms: vec![1000],
            terminal_integral_windows_ms: vec![1000],
        },
        public_delivery_delay_ms: DelayScheduleV1::Fixed(100),
        observe_after_groups: true,
        query_times_utc_ms: vec![
            common::BASE + 1201,
            common::BASE + 1400,
            common::BASE + 12000,
        ],
        export_new_trades: true,
    }
}
#[allow(dead_code)]
pub fn append_pm(root: &Path, rows: &[PolymarketOrderbookV3Message], part: u64) {
    let day = root.join("inputs");
    let manifest = day.join("manifest.json");
    let mut m: Value = serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    let mut w = TypedParquetSegmentWriter::create_for_typed_source(
        &day,
        &format!("extra-{part}"),
        POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
        "polymarket_market",
    )
    .unwrap();
    w.write_polymarket_orderbook_v3_messages(
        rows,
        &mut PolymarketOrderbookV3WriteSequenceState::default(),
    )
    .unwrap();
    w.append_key_value_metadata("data_collect_daily_file_fact_v1",json!({"source_kind":"polymarket_market","schema_version":POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,"utc_hour":"2026-08-24T00","part":part,"rows":rows.len()}).to_string()).unwrap();
    let f = w.finalize().unwrap();
    m["files"].as_array_mut().unwrap().push(json!({"path":f.final_path.file_name().unwrap().to_str().unwrap(),"source_kind":"polymarket_market","schema_version":POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,"utc_hour":"2026-08-24T00","part":part,"rows":f.row_count,"bytes":std::fs::metadata(&f.final_path).unwrap().len(),"sha256":f.sha256}));
    std::fs::write(manifest, serde_json::to_vec(&m).unwrap()).unwrap();
}
#[allow(dead_code)]
pub fn pm(sequence: u64, at: i64) -> PolymarketOrderbookV3Message {
    common::book(
        common::FIRST,
        "20",
        common::BASE + at,
        common::BASE + at,
        sequence,
        "0.6",
    )
}
#[allow(dead_code)]
pub fn initialized(root: &Path) -> ContinuousPublicExportConfigV1 {
    let (mut run, rules) = common::setup(root);
    common::initialize_at(&mut run, common::BASE + 1000, &[common::FIRST]);
    ContinuousPublicExportConfigV1 {
        rules: PublicExportRulesV1 {
            snapshot_path: rules.path().into(),
            expected_file_sha256: rules.file_sha256().clone(),
        },
        preparation: run.preparation,
        markets: PublicObservationMarketsV1::All,
        statistics: PublicStatisticsConfigV1 {
            momentum_windows_ms: vec![1000],
            spot_variance_windows_ms: vec![1000],
            terminal_integral_windows_ms: vec![1000],
        },
        public_delivery_delay_ms: DelayScheduleV1::Fixed(100),
        observe_after_groups: true,
        query_times_utc_ms: vec![common::BASE + 1201],
        export_new_trades: true,
    }
}
/// 为真实Parquet出口补充有限深度/spot时间线；每个来源保持原正式格式。
#[allow(dead_code)]
pub fn append_reference_inputs(root: &Path) {
    append_reference_inputs_at(root, common::BASE);
}
#[allow(dead_code)]
pub fn append_reference_inputs_at(root: &Path, base: i64) {
    use data_collect::source_hourly_layout::*;
    let day = root.join("inputs");
    let mut depth = TypedParquetSegmentWriter::create_for_typed_source(
        &day,
        "depth",
        BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
        "binance_depth_snapshot",
    )
    .unwrap();
    let rows = [(1500, 1, "99", "101"), (2500, 2, "100", "102")].map(|(at, n, bid, ask)| {
        BinanceDepthSnapshotHourlyRow {
            timestamp_received: base + at,
            snapshot_fetch_start_utc: base + at - 10,
            last_update_id: n,
            bids: format!(r#"[["{bid}","5"]]"#),
            asks: format!(r#"[["{ask}","3"]]"#),
            symbol: "BTCUSDT".into(),
            source_endpoint: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
                .into(),
            snapshot_status: "complete".into(),
            source_sequence_epoch_id: Some("depth-test".into()),
            connection_generation: Some(1),
            depth_ingress_sequence: Some(n),
            collector_admission_sequence: Some(n),
            collector_admission_time_utc_ms: Some(base + at + 1),
            collector_host_id: "synthetic-collector".into(),
            collector_region: "test".into(),
            schema_version: BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION.into(),
        }
    });
    depth.write_binance_depth_snapshot_rows(&rows).unwrap();
    finish_source(
        root,
        depth,
        "binance_depth_snapshot",
        BINANCE_DEPTH_SNAPSHOT_SCHEMA_VERSION,
        0,
        2,
    );
    let version = data_pipeline_core::typed_raw::POLYMARKET_RTDS_CHAINLINK_BTC_USD_SCHEMA;
    let mut spot = TypedParquetSegmentWriter::create_for_typed_source(
        &day,
        "spot",
        version,
        "polymarket_rtds_chainlink_btc_usd",
    )
    .unwrap();
    let rows=[(1500,"100"),(2500,"103"),(2500,"104"),(3500,"103")].into_iter().enumerate().map(|(n,(at,value))|RtdsChainlinkBtcUsdHourlyRow{timestamp_received:base+at,message_timestamp:base+at-1,payload_timestamp:base+at-1,age_ms:Some(1),symbol:"btc/usd".into(),value:value.into(),topic:"crypto_prices_chainlink".into(),message_type:"update".into(),freshness_status:"fresh".into(),subscription_filter:"{}".into(),source_native_payload_json:format!(r#"{{"symbol":"btc/usd","timestamp":{},"value":"{value}","synthetic_message":{n}}}"#,base+at-1),collector_host_id:"synthetic-collector".into(),collector_region:"test".into(),source_endpoint:"wss://ws-live-data.polymarket.com".into(),schema_version:version.into()}).collect::<Vec<_>>();
    spot.write_rtds_chainlink_btc_usd_rows(&rows).unwrap();
    finish_source(
        root,
        spot,
        "polymarket_rtds_chainlink_btc_usd",
        version,
        0,
        4,
    );
}
fn finish_source(
    root: &Path,
    mut writer: TypedParquetSegmentWriter,
    source: &str,
    version: &str,
    part: u64,
    rows: usize,
) {
    writer.append_key_value_metadata("data_collect_daily_file_fact_v1",json!({"source_kind":source,"schema_version":version,"utc_hour":"2026-08-24T00","part":part,"rows":rows}).to_string()).unwrap();
    let f = writer.finalize().unwrap();
    let manifest = root.join("inputs/manifest.json");
    let mut m: Value = serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    if version == "polymarket_market_metadata_v6" {
        m["writer_finalize_acks"]
            .as_array_mut()
            .unwrap()
            .push(json!("polymarket_rtds_chainlink_twap"));
    }
    m["files"].as_array_mut().unwrap().push(json!({"path":f.final_path.file_name().unwrap().to_str().unwrap(),"source_kind":source,"schema_version":version,"utc_hour":"2026-08-24T00","part":part,"rows":f.row_count,"bytes":std::fs::metadata(&f.final_path).unwrap().len(),"sha256":f.sha256}));
    std::fs::write(manifest, serde_json::to_vec(&m).unwrap()).unwrap();
}
#[allow(dead_code)]
pub fn append_late_anchor(root: &Path) {
    append_late_anchor_at(root, common::BASE);
}
#[allow(dead_code)]
pub fn append_late_anchor_at(root: &Path, base: i64) {
    use data_collect::polymarket_market_metadata_v6_layout::*;
    use sha2::{Digest, Sha256};
    let at = base + 2000;
    let body = r#"{"eventMetadata":{"priceToBeat":100}}"#;
    let row=PolymarketMarketMetadataV6Row::from_raw_value(json!({"schema_version":POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION,"metadata_row_kind":"market_anchor_rule_update","timestamp_received":at,"metadata_origin":"gamma_event_by_slug","collector_host_id":"synthetic-collector","collector_region":"test","slug":"btc-updown-5m-engineering","condition_id":common::FIRST,"clob_market_id":"3195576","asset_ids":"[\"20\",\"3\"]","market_start_utc":base+1000,"market_end_utc":base+10000,"fixed_identity_verified_at_utc":base-1,"market_lifecycle_state":"active","market_runtime_role":"current","metadata_fetch_start_utc":at-10,"anchor_query_source_epoch_id":"anchor-test","anchor_query_ordinal":1,"anchor_query_result":"observed","price_to_beat_decimal":"100","price_to_beat_source_field":"eventMetadata.priceToBeat","twap_enabled":true,"twap_lookback_seconds":30,"http_received_at_utc":at,"http_request_url":"https://gamma-api.polymarket.com/events/slug/btc-updown-5m-engineering","http_body_encoding":"utf8","http_body":body,"http_body_sha256":format!("{:x}",Sha256::digest(body.as_bytes()))}),4,at+1).unwrap();
    let mut w = TypedParquetSegmentWriter::create_for_typed_source(
        &root.join("inputs"),
        "late-anchor",
        POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION,
        "polymarket_market_metadata",
    )
    .unwrap();
    w.write_polymarket_market_metadata_v6_rows(&[row]).unwrap();
    finish_source(
        root,
        w,
        "polymarket_market_metadata",
        POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION,
        1,
        1,
    );
}
