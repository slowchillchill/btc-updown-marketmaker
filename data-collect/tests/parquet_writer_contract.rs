use std::collections::BTreeMap;
use std::fs::File;

use data_collect::RawEvent;
use data_collect::writer::{ParquetSegmentWriter, write_segment_on_blocking_pool_for_source};
use parquet::file::reader::{FileReader, SerializedFileReader};
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
        parquet_raw_layout_version: 1,
        parquet_raw_source_kind: "polymarket_market".to_owned(),
    }
}

#[test]
fn parquet_writer_schema_metadata_contract() {
    let tmp = tempdir().unwrap();
    let first_event = synthetic_event(1);
    assert!(serde_json::from_str::<serde_json::Value>(&first_event.raw_json).is_ok());
    let mut writer = ParquetSegmentWriter::create_for_source(
        tmp.path(),
        "polymarket-market",
        "parquet_raw_layout_v1",
        "polymarket_market",
    )
    .unwrap();

    let stats = writer
        .write_events_with_stats(&[first_event, synthetic_event(2)])
        .unwrap();
    let published = writer.finalize().unwrap();

    assert_eq!(stats.rows, 2);
    assert!(stats.estimated_event_bytes > 0);
    assert!(stats.row_group_memory_estimate_bytes >= stats.estimated_event_bytes);
    assert_eq!(published.row_count, 2);

    let reader = SerializedFileReader::new(File::open(&published.final_path).unwrap()).unwrap();
    let metadata = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("parquet key-value metadata");
    let metadata = metadata
        .iter()
        .map(|item| (item.key.clone(), item.value.clone()))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(
        metadata
            .get("parquet_raw_layout_version")
            .and_then(|value| value.as_deref()),
        Some("parquet_raw_layout_v1")
    );
    assert_eq!(
        metadata
            .get("parquet_raw_source_kind")
            .and_then(|value| value.as_deref()),
        Some("polymarket_market")
    );
    assert_eq!(
        metadata
            .get("data_collect_artifact_kind")
            .and_then(|value| value.as_deref()),
        Some("typed_raw_parquet")
    );
}

#[test]
fn parquet_writer_rejects_path_traversal_segment_stem() {
    let tmp = tempdir().unwrap();

    let result = ParquetSegmentWriter::create_for_source(
        tmp.path(),
        "../escape",
        "parquet_raw_layout_v1",
        "polymarket_market",
    );
    assert!(result.is_err(), "unsafe stem must be rejected");
    let error = result.err().unwrap();

    assert!(
        error
            .to_string()
            .contains("parquet segment stem is not a safe path component")
    );
    assert!(!tmp.path().join("../escape-running.parquet").exists());
}

#[tokio::test]
async fn blocking_work_is_isolated_from_main_runtime() {
    let tmp = tempdir().unwrap();

    let published = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        write_segment_on_blocking_pool_for_source(
            tmp.path().to_path_buf(),
            "polymarket-market".to_owned(),
            "parquet_raw_layout_v1".to_owned(),
            "polymarket_market".to_owned(),
            vec![vec![synthetic_event(1), synthetic_event(2)]],
        ),
    )
    .await
    .expect("blocking writer task should not stall runtime")
    .unwrap();

    assert_eq!(published.row_count, 2);
    assert!(published.final_path.exists());
}
