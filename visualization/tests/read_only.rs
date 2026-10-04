#[path = "../../backtest/tests/common/mod.rs"]
mod common;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use contracts::*;
use data_pipeline_core::public_input::{DptIngressV1, RecordedIngressV1};
use data_pipeline_training::public_recording::PublicRecordingWriterV1;
use serde_json::Value;
use shared_run::{StrategyInputV1, StrategyOutputV1, StrategyV1};
use std::{collections::BTreeMap, path::Path};
use tower::ServiceExt;
use visualization::{
    api::{load_sources, router},
    dto::*,
    sources::{backtest::load_document, public_recording::PublicPages},
};

struct Observer;
impl StrategyV1 for Observer {
    type Error = &'static str;
    type Diagnostics = ();
    fn decide(&mut self, _: &StrategyInputV1, _: i64) -> Result<StrategyOutputV1, Self::Error> {
        Ok(StrategyOutputV1::Continue {
            intents: vec![],
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        })
    }
}
fn files(path: &Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
    let mut result = BTreeMap::new();
    for entry in std::fs::read_dir(path).unwrap() {
        let p = entry.unwrap().path();
        if p.is_dir() {
            result.extend(files(&p));
        } else {
            result.insert(p.clone(), std::fs::read(p).unwrap());
        }
    }
    result
}
async fn query(app: &axum::Router, path: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    (
        response.status(),
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap(),
    )
}
#[tokio::test]
async fn production_range_end_report_and_missing_metadata_are_independent_read_only_sources() {
    let temp = tempfile::tempdir().unwrap();
    let (config, rules) = common::setup(temp.path());
    let report = backtest::run_historical_v1(&config, rules, &mut Observer)
        .await
        .unwrap();
    assert!(matches!(
        report.end_reason,
        backtest::HistoricalEndReasonV1::RangeEnd
    ));
    let before = files(temp.path());
    let app = router(
        load_sources(SourcesConfig {
            runs: vec![RunSourceConfig {
                research: None,
                label: "合成工程验证".into(),
                report_kind: Default::default(),
                report: Some(config.result_path.clone()),
                metadata: Some(temp.path().join("missing.json")),
                public_recording: None,
                noe_results: vec![],
            }],
        })
        .unwrap(),
    );
    let (_, snapshot) = query(&app, "/api/runs/0/snapshot").await;
    assert_eq!(snapshot["report"]["status"], "ready");
    assert_eq!(snapshot["metadata"]["status"], "read_error");
    assert_eq!(snapshot["report"]["exact"]["end_reason"], "RangeEnd");
    assert_eq!(snapshot["report"]["exact"]["recording_drained"], true);
    assert_eq!(
        snapshot["report"]["exact"]["account_economics_memory"]["cash"]["actual_available_pusd"],
        "2000"
    );
    assert_eq!(snapshot["report"]["decode_attempts"], "1");
    let (_, again) = query(&app, "/api/runs/0/snapshot").await;
    assert_eq!(snapshot, again);
    assert_eq!(
        query(&app, "/api/runs/100/snapshot").await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(before, files(temp.path()));
}
#[test]
fn exact_zero_unavailable_decimal_rational_and_large_identity_survive_the_browser_boundary() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("report.json");
    // JSON 传输边界的独立小样例；不是伪造研究报告或领域生成器。
    let raw = r#"{"economics_version":"account_economics_v2","end_reason":"RangeEnd","identity":18446744073709551615,"zero":{"Known":{"numerator":"0","denominator":"1"}},"unavailable":"Unavailable","decimal":"0.000000000000000000000001","ratio":{"numerator":"9007199254740993","denominator":"7"},"fraction":0.123456789123456789123456789}"#;
    std::fs::write(&path, raw).unwrap();
    let document = load_document(Some(&path), true);
    let exact = document.exact.unwrap();
    assert_eq!(exact["identity"], "18446744073709551615");
    assert_eq!(exact["fraction"], "0.123456789123456789123456789");
    assert_eq!(exact["zero"]["Known"]["numerator"], "0");
    assert_eq!(exact["unavailable"], "Unavailable");
    assert_eq!(exact["decimal"], "0.000000000000000000000001");
    assert_eq!(exact["ratio"]["denominator"], "7");
    assert_eq!(document.raw_json.unwrap(), raw);
    assert_eq!(std::fs::read_to_string(path).unwrap(), raw);
}
#[test]
fn corrupt_and_unknown_report_versions_fail_without_repairing_originals() {
    let temp = tempfile::tempdir().unwrap();
    for (name, raw, status) in [
        (
            "truncated",
            r#"{"economics_version":"account_economics_v2","end_reason":"#,
            "invalid_json",
        ),
        (
            "future",
            r#"{"economics_version":"account_economics_v99","end_reason":"RangeEnd","identity":9007199254740993}"#,
            "unknown_version",
        ),
    ] {
        let path = temp.path().join(name);
        std::fs::write(&path, raw).unwrap();
        let doc = load_document(Some(&path), true);
        assert_eq!(doc.status, status);
        assert!(doc.detail.is_some());
        assert_eq!(doc.raw_json.unwrap(), raw);
        assert_eq!(std::fs::read_to_string(path).unwrap(), raw);
    }
    assert_eq!(load_document(None, false).status, "not_configured");
}
#[tokio::test]
async fn committed_pages_keep_original_identity_exclude_later_and_pending_segments_and_do_not_restart()
 {
    let temp = tempfile::tempdir().unwrap();
    let (config, _) = common::setup(temp.path());
    let root = temp.path().join("public");
    let mut writer = PublicRecordingWriterV1::create(
        &root,
        &config.preparation.header,
        config.preparation.recording.clone(),
    )
    .unwrap();
    let binding = |character: char| SourceArtifactBindingV1 {
        source_message_id: Sha256Hex(character.to_string().repeat(64)),
        manifest_sha256: Sha256Hex("a".repeat(64)),
        file_sha256: Sha256Hex("b".repeat(64)),
        relative_path: "源 \"订单\".parquet".into(),
    };
    writer.record_source_binding(binding('1')).unwrap();
    writer.record_source_binding(binding('2')).unwrap();
    let prefix = writer.stabilize().unwrap().unwrap();
    let prefix_path = temp.path().join("prefix.json");
    std::fs::write(&prefix_path, serde_json::to_vec(&prefix).unwrap()).unwrap();
    writer.record_source_binding(binding('3')).unwrap();
    writer.stabilize().unwrap();
    writer
        .record_aborted(RecordedIngressV1 {
            input_stream_id: config.preparation.header.input_stream_id.clone(),
            input_position: 0,
            ingress: DptIngressV1::EndOfInput,
            dependency_record_refs: vec![],
        })
        .unwrap();
    let before = files(&root);
    // report 可省略；公共流独立于损坏报告/缺失元数据，不建立运行或账户身份。
    let broken_report = temp.path().join("invalid-report.json");
    std::fs::write(&broken_report, "{broken").unwrap();
    let source = serde_json::json!({"root":root,"committed_prefix":prefix_path,"page_rows":1,"max_batch_bytes":1048576});
    let config:SourcesConfig=serde_json::from_value(serde_json::json!({"runs":[
        {"label":"public-only","public_recording":source},
        {"label":"independent-errors","report":broken_report,"metadata":temp.path().join("missing-meta"),"public_recording":source}
    ]})).unwrap();
    let app = router(load_sources(config).unwrap());
    let (_, snapshot) = query(&app, "/api/runs/0/snapshot").await;
    assert_eq!(snapshot["report"]["status"], "not_configured");
    assert_eq!(snapshot["report"]["read_attempts"], "0");
    assert_eq!(snapshot["metadata"]["status"], "not_configured");
    assert_eq!(snapshot["public_recording"]["status"], "ready");
    assert_eq!(query(&app, "/api/runs/0/public").await.0, StatusCode::OK);
    let (_, snapshot) = query(&app, "/api/runs/1/snapshot").await;
    assert_eq!(snapshot["report"]["status"], "invalid_json");
    assert_eq!(snapshot["metadata"]["status"], "read_error");
    assert_eq!(snapshot["public_recording"]["status"], "ready");
    assert_eq!(query(&app, "/api/runs/1/public").await.0, StatusCode::OK);
    let (_, unknown) = query(
        &app,
        "/api/runs/0/public-query?page=1&kind=SourceBinding&condition=unread-market",
    )
    .await;
    assert_eq!(unknown["query"]["page"], "1");
    assert_eq!(unknown["matching"]["scanned_rows"], "1");
    assert_eq!(unknown["matching"]["unknown_rows"], "1");
    assert_eq!(
        unknown["matching"]["matched_offsets"],
        serde_json::json!([])
    );
    assert_eq!(unknown["batch"]["row_start"], "1");
    let (_, retry) = query(&app, "/api/runs/0/public-query?page=1&kind=SourceBinding&position_domain=storage_row&position_start=1&position_end=9999999999999999999999999999999999999999").await;
    assert_eq!(
        retry["matching"]["matched_offsets"],
        serde_json::json!(["0"])
    );
    assert_eq!(retry["matching_reused"], false);
    let (_, repeated) = query(&app, "/api/runs/0/public-query?page=1&kind=SourceBinding&position_domain=storage_row&position_start=1&position_end=9999999999999999999999999999999999999999").await;
    assert_eq!(repeated["matching_reused"], true);
    assert_eq!(repeated["matching"], retry["matching"]);
    assert_eq!(repeated["batch"], retry["batch"]);
    assert_eq!(retry["batch"], unknown["batch"]);
    assert_eq!(retry["batch"]["cost"]["next_page_calls"], "2");
    let (status, changed) = query(&app, "/api/runs/0/public-query?page=0").await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(changed["cursor"]["current_page"], "1");
    assert_eq!(changed["cursor"]["next_page"], "2");
    let (status, invalid) = query(&app, "/api/runs/0/public-query?page=2&time_start=0").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(invalid["cursor"]["next_page"], "2");
    assert_eq!(before, files(&root));
    let (mut reader, status) = PublicPages::open(&PublicSourceConfig {
        root: root.clone(),
        committed_prefix: prefix_path,
        page_rows: 1,
        max_batch_bytes: 1 << 20,
    })
    .unwrap();
    assert_eq!(status["reader_open_count"], "1");
    let first = reader.current().unwrap();
    let first_json: Value = serde_json::from_slice(&first).unwrap();
    assert_eq!(
        first_json["rows"][0]["SourceBinding"]["source_message_id"],
        "1".repeat(64)
    );
    assert_eq!(
        first_json["cost"]["rows_json_bytes"],
        serde_json::to_vec(&first_json["rows"])
            .unwrap()
            .len()
            .to_string()
    );
    assert_eq!(
        reader.last_encoding_cost().unwrap().response_bytes,
        first.len()
    );
    let second = reader.page(1).unwrap();
    let second_json: Value = serde_json::from_slice(&second).unwrap();
    assert_eq!(
        second_json["rows"][0]["SourceBinding"]["source_message_id"],
        "2".repeat(64)
    );
    let second_cost = reader.last_encoding_cost().unwrap();
    assert_eq!(
        second_json["cost"]["rows_json_bytes"],
        serde_json::to_vec(&second_json["rows"])
            .unwrap()
            .len()
            .to_string()
    );
    assert_eq!(second, reader.current().unwrap());
    assert_eq!(second, reader.page(1).unwrap());
    assert_eq!(
        reader.last_encoding_cost().unwrap().response_encoding_us,
        second_cost.response_encoding_us
    );
    assert_eq!(
        reader.last_encoding_cost().unwrap().response_bytes,
        second.len()
    );
    assert!(reader.page(0).is_err());
    let end: Value = serde_json::from_slice(&reader.page(2).unwrap()).unwrap();
    assert_eq!(end["at_end"], true);
    assert_eq!(end["cost"]["rows_json_bytes"], "2");
    assert_eq!(end["total_returned_rows"], "2");
    assert_eq!(
        end["prefix"]["stable_storage_ref"],
        prefix.stable_storage_ref
    );
    assert_eq!(before, files(&root));
    // 在测试副本注入容器截断；原生解码错误后游标停止，不恢复或补尾。
    let damaged = temp.path().join("damaged");
    std::fs::create_dir(&damaged).unwrap();
    for (path, bytes) in before {
        let p = damaged.join(path.strip_prefix(&root).unwrap());
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(
            &p,
            if p.extension().is_some_and(|e| e == "parquet") {
                &bytes[..bytes.len().min(10)]
            } else {
                &bytes
            },
        )
        .unwrap();
    }
    let damaged_before = files(&damaged);
    let (mut broken, _) = PublicPages::open(&PublicSourceConfig {
        root: damaged.clone(),
        committed_prefix: temp.path().join("prefix.json"),
        page_rows: 1,
        max_batch_bytes: 1 << 20,
    })
    .unwrap();
    let error = broken.current().unwrap_err();
    assert!(error.contains("读取页 0 失败"));
    assert_eq!(error, broken.current().unwrap_err());
    assert_eq!(damaged_before, files(&damaged));
}

#[test]
fn engineering_projection_is_explicit_and_has_no_fabricated_host_end_or_economics_version() {
    use visualization::sources::backtest::load_engineering_projection;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("display.json");
    std::fs::write(
        &path,
        r#"{"engineering_display_boundary":"Ledger 原投影","ledger_consumed_memory":{}}"#,
    )
    .unwrap();
    let raw = std::fs::read(&path).unwrap();
    let engineering = load_engineering_projection(Some(&path));
    assert_eq!(engineering.status, "ready");
    let exact = engineering.exact.unwrap();
    assert!(exact.get("end_reason").is_none());
    assert!(exact.get("economics_version").is_none());
    assert_eq!(load_document(Some(&path), true).status, "unknown_version");
    assert_eq!(raw, std::fs::read(&path).unwrap());
}
