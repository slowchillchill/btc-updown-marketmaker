use std::fs;
use std::time::Instant;

use data_collect::{ParquetSegmentWriter, RawEvent};

fn main() {
    let root = std::env::temp_dir().join(format!(
        "data-collect-parquet-writer-probe-{}",
        std::process::id()
    ));
    fs::create_dir_all(&root).expect("create probe root");

    let rows = 10_000_u64;
    let events: Vec<RawEvent> = (0..rows).map(synthetic_event).collect();

    let started = Instant::now();
    let mut writer =
        ParquetSegmentWriter::create(&root, "phase0-writer-probe", "parquet_raw_layout_v1")
            .expect("create parquet writer");
    writer.write_events(&events).expect("write events");
    let published = writer.finalize().expect("finalize parquet segment");
    let elapsed = started.elapsed();

    println!(
        "rows={} elapsed_ms={} final_path={} sha256={}",
        published.row_count,
        elapsed.as_millis(),
        published.final_path.display(),
        published.sha256
    );

    fs::remove_dir_all(&root).expect("remove probe root");
}

fn synthetic_event(seq: u64) -> RawEvent {
    RawEvent {
        source_kind: "polymarket_market".to_owned(),
        source_event_type: "book".to_owned(),
        source_identifier: format!("asset-{seq}"),
        source_ts: Some("1766789469958".to_owned()),
        recv_time_utc_ms: 1_766_789_469_999 + seq as i64,
        collector_admission: None,
        collector_host_id: "collector-local".to_owned(),
        collector_region: "local-dev".to_owned(),
        source_endpoint: "synthetic://polymarket-market".to_owned(),
        raw_json: format!(r#"{{"event_type":"book","asset_id":"asset-{seq}"}}"#),
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: "polymarket_market".to_owned(),
    }
}
