use std::fs;

use data_collect::{
    RawEvent, sha256_file, writer::ParquetSegmentWriter, writer::write_segment_on_blocking_pool,
};
use tempfile::tempdir;

fn synthetic_event(seq: u64) -> RawEvent {
    RawEvent {
        source_kind: "polymarket_market".to_owned(),
        source_event_type: "book".to_owned(),
        source_identifier: format!("asset-{seq}"),
        source_ts: Some("1766789469958".to_owned()),
        recv_time_utc_ms: 1_766_789_469_999 + seq as i64,
        collector_host_id: "collector-local".to_owned(),
        collector_region: "local-dev".to_owned(),
        source_endpoint: "synthetic://polymarket-market".to_owned(),
        raw_json: format!(r#"{{"event_type":"book","asset_id":"asset-{seq}"}}"#),
        collector_admission: None,
    }
}

#[test]
fn writer_running_file_is_not_discoverable() {
    let tmp = tempdir().unwrap();
    let writer =
        ParquetSegmentWriter::create(tmp.path(), "probe", "parquet_raw_layout_v1").unwrap();

    fs::write(tmp.path().join("ignored.tmp"), b"partial").unwrap();
    fs::write(tmp.path().join("old-running.parquet"), b"partial").unwrap();
    fs::write(tmp.path().join("old-digesting.parquet"), b"partial").unwrap();
    fs::write(tmp.path().join(".rsync-partial"), b"partial").unwrap();

    assert!(writer.running_path().exists());
    assert!(!tmp.path().join("probe.parquet").exists());
}

#[test]
fn writer_refuses_to_truncate_existing_running_segment() {
    let tmp = tempdir().unwrap();
    let running = tmp.path().join("probe-running.parquet");
    fs::write(&running, b"partial evidence").unwrap();

    let result = ParquetSegmentWriter::create(tmp.path(), "probe", "parquet_raw_layout_v1");
    assert!(result.is_err());
    let err = result.err().unwrap();

    assert!(err.to_string().contains("running parquet already exists"));
    assert_eq!(fs::read(&running).unwrap(), b"partial evidence");
}

#[test]
fn writer_finalize_atomic_rename_publishes_only_finalized() {
    let tmp = tempdir().unwrap();
    let mut writer =
        ParquetSegmentWriter::create(tmp.path(), "probe", "parquet_raw_layout_v1").unwrap();

    writer
        .write_events(&[synthetic_event(1), synthetic_event(2)])
        .unwrap();
    let running_path = writer.running_path().to_path_buf();
    let published = writer.finalize().unwrap();

    assert!(!running_path.exists());
    assert!(published.final_path.exists());
    assert_eq!(published.row_count, 2);
    assert_eq!(
        published.sha256,
        sha256_file(&published.final_path).unwrap()
    );
}

#[tokio::test]
async fn writer_segment_can_run_on_blocking_pool() {
    let tmp = tempdir().unwrap();

    let published = write_segment_on_blocking_pool(
        tmp.path().to_path_buf(),
        "probe".to_owned(),
        "parquet_raw_layout_v1".to_owned(),
        vec![vec![synthetic_event(1), synthetic_event(2)]],
    )
    .await
    .unwrap();

    assert!(published.final_path.exists());
    assert_eq!(published.row_count, 2);
}
