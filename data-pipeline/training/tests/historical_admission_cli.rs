//! 一个有限人工组合：正式writer→独立CLI→公共reader/原选择器。
use data_collect::polymarket_market_metadata_layout::{
    PolymarketMarketMetadataRow, PolymarketMarketMetadataV5Row,
};
use data_collect::polymarket_market_metadata_v6_layout::PolymarketMarketMetadataV6Row;
use data_collect::writer::TypedParquetSegmentWriter;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::Instant;

const DAY: i64 = 1_785_369_600_000;
const S: i64 = DAY + 1000;

fn metadata(at: i64, sequence: u64) -> PolymarketMarketMetadataV6Row {
    // 复用既有public_input_contract的合法snapshot构造；不改Frozen状态。
    let payload = r#"{"orderPriceMinTickSize":"0.01"}"#;
    let mut row: PolymarketMarketMetadataRow = serde_json::from_value(json!({
        "metadata_row_kind":"metadata_snapshot", "timestamp_received":at,
        "metadata_origin":"gamma_markets_discovery", "collector_host_id":"collector-1",
        "collector_region":"hk", "schema_version":"polymarket_market_metadata_v5",
        "slug":"btc-updown-5m-test", "condition_id":"condition", "clob_market_id":"123",
        "asset_ids":"[\"1\",\"2\"]", "market_start_utc":DAY+300_000,
        "market_end_utc":DAY+600_000, "fixed_identity_verified_at_utc":at-1,
        "market_lifecycle_state":"prefetched", "market_runtime_role":"next",
        "source_endpoint":"https://gamma-api.polymarket.com/markets",
        "source_evidence_origin":"gamma_markets_discovery",
        "source_evidence_event_type":"gamma_markets_discovery_response",
        "source_evidence_received_at_utc":at, "row_evidence_validation_completed_at_utc":at+1,
        "source_evidence_payload_json":payload,
        "source_evidence_raw_sha256":format!("{:x}",Sha256::digest(payload.as_bytes())),
        "metadata_fetch_start_utc":at-10, "closed":false,
        "outcomes_raw":"[\"UP\",\"DOWN\"]", "outcome_prices_raw":"[\"0.5\",\"0.5\"]",
        "active":true,"accepting_orders":true
    }))
    .unwrap();
    row.collector_admission_sequence = sequence;
    row.collector_admission_time_utc_ms = at + 1;
    PolymarketMarketMetadataV6Row::Existing(PolymarketMarketMetadataV5Row::from_common(
        row, None, None,
    ))
}

fn rows(path: &Path) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn invoke(mode: &str, config: &Path, report: &Path) -> std::process::Output {
    let started = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_dpt-historical-admission"))
        .args([
            mode,
            config.to_str().unwrap(),
            report.to_str().unwrap(),
            "--config-bytes",
            "1048576",
        ])
        .env("RAYON_NUM_THREADS", "2")
        .output()
        .unwrap();
    eprintln!(
        "{}",
        json!({"artificial_cli":mode,"status":output.status.code(),
        "elapsed_seconds":started.elapsed().as_secs_f64(),"stdout_bytes":output.stdout.len(),
        "stderr_bytes":output.stderr.len(),"report":report})
    );
    output
}

#[test]
fn historical_admission_cli_roundtrip() {
    let started = Instant::now();
    let retained = tempfile::Builder::new()
        .prefix("historical-admission-")
        .tempdir()
        .unwrap()
        .keep();
    let root = retained.as_path();
    eprintln!("人工材料保留目录：{}", root.display());
    let mut writer = TypedParquetSegmentWriter::create_for_current_source(
        root,
        "metadata",
        "polymarket_market_metadata",
    )
    .unwrap();
    writer
        .write_polymarket_market_metadata_v6_rows(&[
            metadata(S - 20, 1),
            metadata(S - 10, 2),
            metadata(S + 5, 3),
            metadata(S + 500, 4),
        ])
        .unwrap();
    writer.append_key_value_metadata("data_collect_daily_file_fact_v1",json!({
        "source_kind":"polymarket_market_metadata","schema_version":"polymarket_market_metadata_v6",
        "utc_hour":"2026-07-30T00","part":0,"rows":4
    }).to_string()).unwrap();
    let segment = writer.finalize().unwrap();
    let parquet_bytes = fs::metadata(&segment.final_path).unwrap().len();
    let relative = segment.final_path.file_name().unwrap().to_str().unwrap();
    let manifest = json!({
        "schema_version":"data_collect_daily_manifest_v3","state":"closed","utc_day":"2026-07-30",
        "closure_reason":"utc_day_barrier","collection_day_coverage":{
            "schema_version":"data_collect_collection_day_coverage_v1","state":"ended_known",
            "first_started_utc_ms":DAY,"last_started_utc_ms":DAY,
            "ended_utc_ms":DAY+86_400_000,"reactivated_within_day":false
        },
        "producer_barriers":["binance_aggtrade","binance_depth","polymarket","polymarket_rtds"],
        "writer_finalize_acks":["binance_aggtrade","binance_depth_snapshot","binance_depth_update",
            "polymarket_market","polymarket_market_metadata","polymarket_rtds_chainlink_btc_usd",
            "polymarket_rtds_chainlink_twap"],
        "gaps":["人工记录间隔；不得判为业务连续"],
        "files":[{"path":relative,"source_kind":"polymarket_market_metadata",
            "schema_version":"polymarket_market_metadata_v6","utc_hour":"2026-07-30T00","part":0,
            "bytes":parquet_bytes,"rows":4,"sha256":segment.sha256,"source_gaps":[]}]
    });
    let manifest_path = root.join("manifest.json");
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let mut config = json!({"daily_manifests":[manifest_path],
        "initialization":{"files":[{"manifest_path":manifest_path,"relative_paths":[relative]}],
            "markets":[],"received_start_utc_ms":S,"received_end_utc_ms":S+1000,"purpose":"无模型工程人工材料"},
        "active_end_utc_ms":S+600,
        "limits":{"batch_rows":2,"batch_bytes":1048576,"input_file_bytes":16777216,
            "input_rows":100,"output_bytes":2097152,"source_epochs":16}});
    let config_path = root.join("inspect.json");
    fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();
    let inspect_path = root.join("inspect.jsonl");
    let inspect = invoke("inspect", &config_path, &inspect_path);
    assert!(
        inspect.status.success(),
        "inspect: {}",
        String::from_utf8_lossy(&inspect.stderr)
    );
    let inspected = rows(&inspect_path);
    assert_eq!(inspected.last().unwrap()["complete"], true);
    assert_eq!(inspected.last().unwrap()["business_continuity"], "Unknown");
    assert_eq!(inspected.last().unwrap()["initialization_rows_decoded"], 4);
    assert_eq!(inspected.last().unwrap()["window_rows"], 2);
    let candidates = inspected
        .iter()
        .filter(|v| v["kind"] == "metadata_candidate")
        .collect::<Vec<_>>();
    assert_eq!(candidates.len(), 4);
    assert_eq!(
        candidates
            .iter()
            .filter(|v| v["fact"]["before_start"] == true)
            .count(),
        2
    );
    let window = inspected
        .iter()
        .find(|v| v["kind"] == "window_source")
        .unwrap();
    assert_eq!(window["largest_forward_interval_ms"], 495);
    assert_eq!(
        window["interval_or_sequence_step_is_missing_business_data"],
        "Unknown"
    );
    assert!(
        inspected
            .iter()
            .any(|v| v["known_gaps"] == manifest["gaps"])
    );
    config["initialization"]["markets"] = json!([{"condition_id":"condition",
        "snapshot_source_message_id":candidates[0]["fact"]["source_message_id"]}]);
    let verify_config = root.join("verify.json");
    fs::write(&verify_config, serde_json::to_vec(&config).unwrap()).unwrap();
    let verify_path = root.join("verify.jsonl");
    let verify = invoke("verify", &verify_config, &verify_path);
    assert!(
        verify.status.success(),
        "verify: {}",
        String::from_utf8_lossy(&verify.stderr)
    );
    let verified = rows(&verify_path);
    let selected = verified
        .iter()
        .filter(|v| v["kind"] == "selected_initialization")
        .collect::<Vec<_>>();
    assert_eq!(selected.len(), 2);
    assert!(selected.iter().all(|v| v["fact"]["before_start"] == true));
    assert_eq!(
        selected[0]["fact"]["source_message_id"],
        candidates[0]["fact"]["source_message_id"]
    );
    assert_eq!(
        selected[1]["fact"]["source_message_id"],
        candidates[1]["fact"]["source_message_id"]
    );
    let selection = verified
        .iter()
        .find(|v| v["kind"] == "selection_verified")
        .unwrap();
    assert_eq!(selection["excluded_input_rows"], 2);
    assert_eq!(
        selection["source_identity_sha256"].as_str().unwrap().len(),
        64
    );
    assert_eq!(verified.last().unwrap()["complete"], true);
    config["initialization"]["markets"][0]["snapshot_source_message_id"] =
        candidates[2]["fact"]["source_message_id"].clone();
    let bad_config = root.join("bad.json");
    fs::write(&bad_config, serde_json::to_vec(&config).unwrap()).unwrap();
    let bad_path = root.join("bad.jsonl");
    let bad = invoke("verify", &bad_config, &bad_path);
    assert!(!bad.status.success(), "S后的Snapshot不能通过原选择器");
    assert!(
        String::from_utf8_lossy(&bad.stderr)
            .contains("指定初始化Snapshot与市场、类型或研究起点不匹配")
    );
    assert!(!rows(&bad_path).iter().any(|v| v["complete"] == true));
    let report_bytes = fs::metadata(inspect_path).unwrap().len()
        + fs::metadata(verify_path).unwrap().len()
        + fs::metadata(bad_path).unwrap().len();
    println!(
        "{}",
        json!({"artificial":true,"cli_processes":3,"writer_rows":4,
        "parquet_bytes":parquet_bytes,"reports_bytes":report_bytes,"elapsed_seconds":started.elapsed().as_secs_f64(),
        "models":0,"business_material_reads":0})
    );
}
