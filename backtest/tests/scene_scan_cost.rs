//! #392 有限公共Parquet扫描；显式运行，输入/输出在仓库外。
#![cfg(feature = "stage-timing")]
mod common;
use backtest::{offline_scene_source::*, *};
use common::*;
use data_collect::{
    polymarket_orderbook_v3::*,
    writer::{PolymarketOrderbookV3WriteSequenceState, TypedParquetSegmentWriter},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use sim_system::offline_scene::*;
use std::{
    fs,
    path::{Path, PathBuf},
    time::Instant,
};
fn material(root: &Path, files: usize) -> PathBuf {
    fs::create_dir(root).unwrap();
    let start = BASE + 4 * 3_600_000 - 5_000;
    let boundary = BASE + 4 * 3_600_000;
    let parts = (files - 2) / 2;
    let mut entries = Vec::new();
    for hour in [3, 4] {
        let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
            root,
            &format!("metadata-h{hour}"),
            "polymarket_market_metadata_v5",
            "polymarket_market_metadata",
        )
        .unwrap();
        writer
            .write_polymarket_market_metadata_v5_rows(&[metadata(
                FIRST,
                "3195576",
                ["20", "3"],
                if hour == 3 { start - 100 } else { boundary },
                start - 500,
                start + 300_000,
                hour as u64,
            )])
            .unwrap();
        writer.append_key_value_metadata("data_collect_daily_file_fact_v1",json!({"source_kind":"polymarket_market_metadata","schema_version":"polymarket_market_metadata_v5","utc_hour":format!("2026-08-24T0{hour}"),"part":0,"rows":1}).to_string()).unwrap();
        let file = writer.finalize().unwrap();
        entries.push(json!({"path":file.final_path.file_name().unwrap().to_str().unwrap(),"source_kind":"polymarket_market_metadata","schema_version":"polymarket_market_metadata_v5","utc_hour":format!("2026-08-24T0{hour}"),"part":0,"rows":1,"bytes":fs::metadata(&file.final_path).unwrap().len(),"sha256":file.sha256}));
        for part in 0..parts {
            let mut rows = Vec::new();
            for i in if hour == 3 { 0..10_000 } else { 10_000..20_000 } {
                if i % parts != part {
                    continue;
                }
                let received = start + (i / 2) as i64;
                let mut row = book(
                    FIRST,
                    if i % 2 == 0 { "20" } else { "3" },
                    received - (i % 7) as i64,
                    received,
                    i as u64 + 1,
                    "0.6",
                );
                if i >= 2 && i % 4096 != 0 {
                    row.event = PolymarketOrderbookV3Event::PriceChange(PolymarketPriceChangeV3 {
                        price_changes: vec![PolymarketPriceChangeEntryV3 {
                            asset_id: if i % 2 == 0 { "20" } else { "3" }.into(),
                            price: "0.4".into(),
                            size: Some(if i % 4 < 2 { "99" } else { "100" }.into()),
                            side: "BUY".into(),
                            best_bid: None,
                            best_ask: None,
                        }],
                    });
                }
                rows.push(row);
            }
            let mut writer = TypedParquetSegmentWriter::create_for_typed_source(
                root,
                &format!("pm-h{hour}-p{part}"),
                POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,
                "polymarket_market",
            )
            .unwrap();
            writer
                .write_polymarket_orderbook_v3_messages(
                    &rows,
                    &mut PolymarketOrderbookV3WriteSequenceState::default(),
                )
                .unwrap();
            writer.append_key_value_metadata("data_collect_daily_file_fact_v1",json!({"source_kind":"polymarket_market","schema_version":POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,"utc_hour":format!("2026-08-24T0{hour}"),"part":part,"rows":rows.len()}).to_string()).unwrap();
            let file = writer.finalize().unwrap();
            entries.push(json!({"path":file.final_path.file_name().unwrap().to_str().unwrap(),"source_kind":"polymarket_market","schema_version":POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION,"utc_hour":format!("2026-08-24T0{hour}"),"part":part,"rows":rows.len(),"bytes":fs::metadata(&file.final_path).unwrap().len(),"sha256":file.sha256}));
        }
    }
    assert_eq!(entries.len(), files);
    let path = root.join("manifest.json");
    fs::write(&path,serde_json::to_vec_pretty(&json!({"schema_version":"data_collect_daily_manifest_v3","state":"closed","utc_day":"2026-08-24","closure_reason":"utc_day_barrier","collection_day_coverage":{"schema_version":"data_collect_collection_day_coverage_v1","state":"ended_known","first_started_utc_ms":BASE,"last_started_utc_ms":BASE,"ended_utc_ms":BASE+86400000,"reactivated_within_day":false},"producer_barriers":["binance_aggtrade","binance_depth","polymarket","polymarket_rtds"],"writer_finalize_acks":["binance_aggtrade","binance_depth_snapshot","binance_depth_update","polymarket_market","polymarket_market_metadata","polymarket_rtds_chainlink_btc_usd"],"files":entries})).unwrap()).unwrap();
    path
}
#[test]
#[ignore = "explicit #392 finite 4/12-file DayIngress/DayProducer scan measurement"]
fn finite_four_twelve_fifty_two_file_scene_scan() {
    let fixture =
        PathBuf::from(std::env::var_os("BACKTEST_SCAN_FIXTURE_ROOT").expect("frozen fixture root"));
    let output = PathBuf::from(
        std::env::var_os("BACKTEST_COST_TEST_OUTPUT_ROOT").expect("independent output root"),
    );
    fs::create_dir(&output).unwrap();
    fs::create_dir_all(&fixture).unwrap();
    for files in [4, 12, 52] {
        let manifest = fixture.join(format!("files-{files}/manifest.json"));
        if !manifest.exists() {
            material(manifest.parent().unwrap(), files);
        }
        let cell = output.join(format!("files-{files}"));
        fs::create_dir(&cell).unwrap();
        fs::create_dir(cell.join("seed")).unwrap();
        let (mut config, _) = setup(&cell.join("seed"));
        let start = BASE + 4 * 3_600_000 - 5_000;
        config.preparation.scope.manifest_paths = vec![manifest.clone()];
        config.preparation.scope.received_start_utc_ms = start - 1000;
        config.preparation.scope.received_end_utc_ms = start + 10_000;
        config.preparation.read_page_rows = 512;
        config.preparation.max_read_batch_bytes = Some(8 << 20);
        fs::write(
            cell.join("request.json"),
            serde_json::to_vec_pretty(&config.preparation).unwrap(),
        )
        .unwrap();
        let mut results = Vec::new();
        for trial in 0..3 {
            let before = ProcessCountersV1::read().unwrap();
            let time = Instant::now();
            let (analysis, markets, assets, sources) =
                collect_scene_facts_v1(&config.preparation, start - 1000, start + 10_000, 1)
                    .unwrap();
            let scan_ns = time.elapsed().as_nanos();
            let after = ProcessCountersV1::read().unwrap();
            let scene = analysis.prepare(Parameters {
                association_ms: 250,
                allocation: Allocation::Nearest,
                cancellation: Cancellation::SegmentProportionalV2,
                internal_time: InternalTime::SourceWhenPresent,
                seed: 7,
                association_trade_shift_ms: 0,
            });
            let mut digest = Sha256::new();
            for event in &scene.events {
                digest.update(serde_json::to_vec(event).unwrap());
                digest.update(b"\n");
            }
            let result = json!({"files":files,"trial":trial,"scan_ns":scan_ns,"process_before":before,"process_after":after,"events":scene.events.len(),"canonical_sha256":format!("{:x}",digest.finalize()),"markets":markets,"assets":assets,"sources":sources});
            eprintln!("scene_scan_cost={result}");
            results.push(result);
        }
        assert_eq!(
            results[0]["canonical_sha256"],
            results[1]["canonical_sha256"]
        );
        assert_eq!(
            results[1]["canonical_sha256"],
            results[2]["canonical_sha256"]
        );
        fs::write(
            cell.join("results.json"),
            serde_json::to_vec_pretty(&results).unwrap(),
        )
        .unwrap();
    }
}
