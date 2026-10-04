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
fn committed_cost_evidence_preserves_overlapping_views_csv_strings_and_original_row_order() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("model-train/validation/issue-178/evidence/continuous");
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
    assert_eq!(v["total"], "48");
    for (i, n) in ["64", "1", "63"].iter().enumerate() {
        assert_eq!(v["rows"][i]["exact"]["service_ms"]["count"], *n);
        assert_eq!(v["rows"][i]["path"], format!("/{i}"));
    }
    let pooled: Value =
        serde_json::from_slice(&d.select(q("pooled", "", Some(0), Some(1))).unwrap()).unwrap();
    assert_eq!(pooled["total"], "8");
    assert_eq!(pooled["rows"][0]["exact"]["calls"], "128");
    let query = q("calls", "", Some(0), Some(1));
    let first = d.select(query.clone()).unwrap();
    let v: Value = serde_json::from_slice(&first).unwrap();
    assert_eq!(v["total"], "1024");
    assert_eq!(v["rows"][0]["csv_line"], "2");
    assert_eq!(v["rows"][0]["exact"]["now"], "1789517395414");
    assert_eq!(v["rows"][0]["exact"]["cpu_ns"], "624252762");
    assert!(
        v["rows"][0]["exact"]["actions"]
            .as_str()
            .unwrap()
            .contains("\"intent\":\"None\"")
    );
    let cost = d.last_encoding_cost().clone();
    let retry = d.select(query).unwrap();
    assert_eq!(first.as_ptr(), retry.as_ptr());
    assert_eq!(first, retry);
    assert_eq!(d.last_encoding_cost(), &cost);
    let v: Value =
        serde_json::from_slice(&d.select(q("calls", "", Some(17), Some(1))).unwrap()).unwrap();
    assert_eq!(v["rows"][0]["path"], "/17");
    assert_eq!(v["rows"][0]["csv_line"], "19");
    assert!(d.select(q("calls", "", Some(1025), Some(1))).is_err());
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
