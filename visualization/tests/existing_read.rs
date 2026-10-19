use serde_json::{Value, json};
use std::collections::BTreeMap;
use visualization::sources::existing::{ExistingDocument, ExistingQuery, ExistingSourceConfig};
fn q(document: &str, path: &str, start: Option<usize>, count: Option<usize>) -> ExistingQuery {
    ExistingQuery {
        document: document.into(),
        path: path.into(),
        start,
        count,
    }
}
#[test]
fn saved_cost_reports_preserve_overlapping_views_csv_strings_and_original_row_order() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let summary = json!([
        {"segment": "all", "service_ms": {"count": 64}},
        {"segment": "entry", "service_ms": {"count": 1}},
        {"segment": "continuation", "service_ms": {"count": 63}}
    ]);
    std::fs::write(root.join("continuous-summary.json"), summary.to_string()).unwrap();
    std::fs::write(root.join("pooled-summary.json"), r#"[{"calls":128}]"#).unwrap();
    let gzip = flate2::write::GzEncoder::new(
        std::fs::File::create(root.join("continuous-calls.csv.gz")).unwrap(),
        flate2::Compression::default(),
    );
    let mut calls = csv::Writer::from_writer(gzip);
    calls
        .write_record([
            "run",
            "now",
            "position",
            "service_ns",
            "model",
            "version",
            "cpu_ns",
            "actions",
        ])
        .unwrap();
    // 重复身份和非单调时钟必须保持输入行序。
    for row in [
        [
            "run-b",
            "1789517395414",
            "7",
            "700000000",
            "fixture",
            "v1",
            "624252762",
            r#"[{"intent":"None","note":"opaque, quoted"}]"#,
        ],
        [
            "run-a",
            "1789517395000",
            "2",
            "800000000",
            "fixture",
            "v1",
            "9007199254740993",
            "[]",
        ],
        [
            "run-b",
            "1789517395414",
            "7",
            "900000000",
            "fixture",
            "v1",
            "42",
            "[]",
        ],
    ] {
        calls.write_record(row).unwrap();
    }
    calls.into_inner().unwrap().finish().unwrap();
    let config = ExistingSourceConfig {
        kind: "continuous".into(),
        paths: BTreeMap::from([
            ("summary".into(), root.join("continuous-summary.json")),
            ("pooled".into(), root.join("pooled-summary.json")),
            ("calls".into(), root.join("continuous-calls.csv.gz")),
        ]),
    };
    let mut d = ExistingDocument::open(Some(&config));
    assert_eq!(d.status()["status"], "ready");
    let first = d.select(q("summary", "", Some(0), Some(3))).unwrap();
    let v: Value = serde_json::from_slice(&first).unwrap();
    assert_eq!(v["total"], "3");
    for (i, n) in ["64", "1", "63"].iter().enumerate() {
        assert_eq!(v["rows"][i]["exact"]["service_ms"]["count"], *n);
        assert_eq!(v["rows"][i]["path"], format!("/{i}"));
    }
    let pooled: Value =
        serde_json::from_slice(&d.select(q("pooled", "", Some(0), Some(1))).unwrap()).unwrap();
    assert_eq!(pooled["total"], "1");
    assert_eq!(pooled["rows"][0]["exact"]["calls"], "128");
    let query = q("calls", "", Some(0), Some(1));
    let first = d.select(query.clone()).unwrap();
    let v: Value = serde_json::from_slice(&first).unwrap();
    assert_eq!(v["total"], "3");
    assert_eq!(v["rows"][0]["csv_line"], "2");
    assert_eq!(v["rows"][0]["exact"]["now"], "1789517395414");
    assert_eq!(v["rows"][0]["exact"]["cpu_ns"], "624252762");
    assert_eq!(
        v["rows"][0]["exact"]["actions"],
        r#"[{"intent":"None","note":"opaque, quoted"}]"#
    );
    let cost = d.last_encoding_cost().clone();
    let retry = d.select(query).unwrap();
    assert_eq!(first.as_ptr(), retry.as_ptr());
    assert_eq!(first, retry);
    assert_eq!(d.last_encoding_cost(), &cost);
    let v: Value =
        serde_json::from_slice(&d.select(q("calls", "", Some(1), Some(2))).unwrap()).unwrap();
    assert_eq!(v["rows"][0]["path"], "/1");
    assert_eq!(v["rows"][0]["csv_line"], "3");
    assert_eq!(v["rows"][0]["exact"]["run"], "run-a");
    assert_eq!(v["rows"][0]["exact"]["now"], "1789517395000");
    assert_eq!(v["rows"][0]["exact"]["position"], "2");
    assert_eq!(v["rows"][0]["exact"]["cpu_ns"], "9007199254740993");
    assert_eq!(v["rows"][1]["path"], "/2");
    assert_eq!(v["rows"][1]["csv_line"], "4");
    assert_eq!(v["rows"][1]["exact"]["run"], "run-b");
    assert_eq!(v["rows"][1]["exact"]["now"], "1789517395414");
    assert_eq!(v["rows"][1]["exact"]["position"], "7");
    assert!(d.select(q("calls", "", Some(4), Some(1))).is_err());
}
#[test]
fn original_manifest_reader_validates_coverage_without_opening_declared_files() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("manifest.json");
    let v = json!({"schema_version":"data_collect_daily_manifest_v3","state":"closed","utc_day":"2026-09-09","collection_day_coverage":{"schema_version":"data_collect_collection_day_coverage_v1","state":"ended_known","first_started_utc_ms":1788912000000i64,"last_started_utc_ms":1788912000000i64,"ended_utc_ms":1788998400000i64,"reactivated_within_day":false},"files":[{"path":"not-existing.parquet","rows":1}],"gaps":[]});
    std::fs::write(&p, serde_json::to_vec(&v).unwrap()).unwrap();
    let c = ExistingSourceConfig {
        kind: "collection_manifest".into(),
        paths: BTreeMap::from([("manifest".into(), dir.path().to_path_buf())]),
    };
    let mut d = ExistingDocument::open(Some(&c));
    assert_eq!(d.status()["status"], "ready");
    let body = d.select(q("manifest", "/files", Some(0), Some(1))).unwrap();
    let b: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(b["rows"][0]["exact"]["path"], "not-existing.parquet");
    assert_eq!(std::fs::read(&p).unwrap(), serde_json::to_vec(&v).unwrap());
    assert!(!dir.path().join("not-existing.parquet").exists());
    let mut bad = v.clone();
    bad["collection_day_coverage"]["state"] = json!("active");
    std::fs::write(&p, bad.to_string()).unwrap();
    assert_eq!(
        ExistingDocument::open(Some(&c)).status()["documents"][0]["status"],
        "unsupported_version"
    );
    bad["schema_version"] = json!("future");
    std::fs::write(&p, bad.to_string()).unwrap();
    assert_eq!(
        ExistingDocument::open(Some(&c)).status()["documents"][0]["status"],
        "unsupported_version"
    );
    std::fs::write(&p, b"{").unwrap();
    let d = ExistingDocument::open(Some(&c));
    assert_eq!(d.status()["documents"][0]["status"], "read_error");
    assert_eq!(std::fs::read(&p).unwrap(), b"{");
    std::fs::remove_file(&p).unwrap();
    assert_eq!(
        ExistingDocument::open(Some(&c)).status()["status"],
        "read_error"
    );
}
#[test]
fn unknown_kind_and_non_projection_completion_do_not_masquerade_as_reports() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("completion.json");
    std::fs::write(&p, b"{\"complete\":true,\"final_ledger\":{}}").unwrap();
    let c = ExistingSourceConfig {
        kind: "completion".into(),
        paths: BTreeMap::from([("completion".into(), p)]),
    };
    assert_eq!(
        ExistingDocument::open(Some(&c)).status()["documents"][0]["status"],
        "unsupported_version"
    );
    let c = ExistingSourceConfig {
        kind: "future".into(),
        paths: BTreeMap::new(),
    };
    assert_eq!(
        ExistingDocument::open(Some(&c)).status()["status"],
        "unsupported_version"
    );
}
#[test]
fn twelve_minute_report_preserves_measured_identity_null_and_microsecond_text() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    let c = ExistingSourceConfig {
        kind: "remeasurement_12m".into(),
        paths: BTreeMap::from([(
            "measurement".into(),
            root.join(
                "baseline-mm/docs/research/complementary-pair-end-to-end-20260909-0000-0012.json",
            ),
        )]),
    };
    let mut d = ExistingDocument::open(Some(&c));
    assert_eq!(d.status()["status"], "ready");
    let read = |d: &mut ExistingDocument, path: &str| -> Value {
        serde_json::from_slice(&d.select(q("measurement", path, None, None)).unwrap()).unwrap()
    };
    assert_eq!(
        read(&mut d, "/measurement_source_commit")["exact"],
        "10a8ce588f2b2d6e192253ef90e4205733a54160"
    );
    assert_eq!(
        read(
            &mut d,
            "/execution/phases/two_market_research/new/exclusive_cpu_seconds"
        )["exact"],
        Value::Null
    );
    assert_eq!(
        read(&mut d, "/execution/timeline/material_launcher_utc")["exact"],
        "2026-10-04T04:16:05.737613+00:00"
    );
    assert!(
        d.select(q(
            "measurement",
            "/execution/phases/prepare/new/start_at_utc_ms",
            None,
            None
        ))
        .is_err()
    );
    let b = d
        .select(q(
            "measurement",
            "/complete_result_comparison",
            Some(1),
            Some(1),
        ))
        .unwrap();
    let v: Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["rows"][0]["path"], "/complete_result_comparison/1");
    assert_eq!(v["rows"][0]["exact"]["difference_count"], "0");
    let cost = d.last_encoding_cost().clone();
    assert_eq!(
        b.as_ptr(),
        d.select(q(
            "measurement",
            "/complete_result_comparison",
            Some(1),
            Some(1)
        ))
        .unwrap()
        .as_ptr()
    );
    assert_eq!(&cost, d.last_encoding_cost());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("future.json");
    std::fs::write(&path, r#"{"schema":"future"}"#).unwrap();
    let c = ExistingSourceConfig {
        kind: "remeasurement_12m".into(),
        paths: BTreeMap::from([("measurement".into(), path)]),
    };
    assert_eq!(
        ExistingDocument::open(Some(&c)).status()["documents"][0]["status"],
        "unsupported_version"
    );
}
#[test]
fn diagnostic_jsonl_preserves_original_order_debug_text_and_rejects_truncation_without_repair() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("trace.jsonl");
    let original=b"{\"kind\":\"observation\",\"at\":86100000}\n{\"kind\":\"completion-send\",\"observation\":86100000,\"completion_at\":86100050,\"compute_wall_ns\":10084179,\"receipt\":\"Some(Debug)\"}\n";
    std::fs::write(&p, original).unwrap();
    let c = ExistingSourceConfig {
        kind: "model_timing".into(),
        paths: BTreeMap::from([("trace".into(), p.clone())]),
    };
    let mut d = ExistingDocument::open(Some(&c));
    let b = d.select(q("trace", "", Some(1), Some(1))).unwrap();
    let v: Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["total"], "2");
    assert_eq!(v["rows"][0]["path"], "/1");
    assert_eq!(v["rows"][0]["exact"]["receipt"], "Some(Debug)");
    assert_eq!(std::fs::read(&p).unwrap(), original);
    assert!(d.select(q("trace", "", Some(3), Some(1))).is_err());
    std::fs::write(&p, b"{\"kind\":\"observation\"}\n{").unwrap();
    assert_eq!(
        ExistingDocument::open(Some(&c)).status()["documents"][1]["status"],
        "read_error"
    );
    assert_eq!(std::fs::read(&p).unwrap(), b"{\"kind\":\"observation\"}\n{");
}
