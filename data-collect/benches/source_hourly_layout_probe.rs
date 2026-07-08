use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use data_collect::polymarket_orderbook_layout::{
    PolymarketOrderbookContext, PolymarketOrderbookRow,
};
use data_collect::writer::TypedParquetSegmentWriter;

const ROWS: u64 = 20_000;

fn main() {
    run_case("polymarket_receive_order_zstd3", false, 3);
    run_case("polymarket_market_asset_timestamp_sorted_zstd3", true, 3);
    run_case("polymarket_receive_order_zstd9", false, 9);
    run_case("polymarket_market_asset_timestamp_sorted_zstd9", true, 9);
}

fn run_case(case_name: &str, sorted: bool, zstd_level: i32) {
    let root = probe_root(case_name);
    fs::create_dir_all(&root).expect("create source hourly probe root");

    let mut rows = (0..ROWS).map(synthetic_book_row).collect::<Vec<_>>();
    if sorted {
        rows.sort_by(|left, right| {
            left.market
                .cmp(&right.market)
                .then_with(|| left.asset_id.cmp(&right.asset_id))
                .then_with(|| left.timestamp_received.cmp(&right.timestamp_received))
        });
    }

    let started = Instant::now();
    let mut writer = TypedParquetSegmentWriter::create_polymarket_orderbook_with_zstd_level(
        &root,
        case_name,
        "polymarket_orderbook_v1",
        zstd_level,
    )
    .expect("create typed writer");
    writer
        .write_polymarket_orderbook_rows(&rows)
        .expect("write orderbook rows");
    let finalize_started = Instant::now();
    let published = writer.finalize().expect("finalize typed parquet");
    let finalize_ms = finalize_started.elapsed().as_millis();
    let elapsed_ms = started.elapsed().as_millis();
    let file_size = fs::metadata(&published.final_path)
        .expect("final parquet metadata")
        .len();

    println!(
        "case={} rows={} sorted={} zstd_level={} elapsed_ms={} finalize_ms={} file_size_bytes={} final_path={} sha256={}",
        case_name,
        published.row_count,
        sorted,
        zstd_level,
        elapsed_ms,
        finalize_ms,
        file_size,
        published.final_path.display(),
        published.sha256
    );

    if std::env::var_os("DATA_COLLECT_KEEP_SOURCE_HOURLY_PROBE").is_none() {
        fs::remove_dir_all(&root).expect("remove source hourly probe root");
    }
}

fn synthetic_book_row(seq: u64) -> PolymarketOrderbookRow {
    let market_index = seq % 12;
    let asset_index = seq % 24;
    let raw_json = format!(
        r#"{{
            "event_type": "book",
            "market": "0x{market_index:064x}",
            "asset_id": "{}",
            "timestamp": {},
            "bids": [["0.49","{}"],["0.48","{}"]],
            "asks": [["0.51","{}"],["0.52","{}"]],
            "hash": "hash-{seq}"
        }}"#,
        100_000_000_000_000_000_000_000_000_000_u128 + u128::from(asset_index),
        1_783_392_400_000_i64 + seq as i64,
        100 + seq % 50,
        80 + seq % 40,
        90 + seq % 35,
        70 + seq % 30,
    );
    PolymarketOrderbookRow::book_from_raw_json(
        &raw_json,
        PolymarketOrderbookContext {
            timestamp_received_ms: 1_783_392_400_000_i64 + seq as i64,
            slug: Some(format!("btc-updown-5m-{}", 1_783_392_300 + (seq / 4) % 12)),
            collector_host_id: "bench-host".to_owned(),
            collector_region: "local-dev".to_owned(),
            source_endpoint: "synthetic://polymarket-market".to_owned(),
            schema_version: "polymarket_orderbook_v1".to_owned(),
        },
    )
    .expect("synthetic book row")
}

fn probe_root(case_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../target/tmp")
        .join(format!(
            "data-collect-source-hourly-layout-probe-{}-{case_name}",
            std::process::id()
        ))
}
