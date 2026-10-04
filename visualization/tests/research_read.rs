use serde_json::Value;
use visualization::sources::research::{ResearchDocument, ResearchQuery};
fn source() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().join("baseline-mm/docs/research/complementary-pair-first-fit-hour-budget-20260909-0000-0100.json")
}
fn q(path: &str, start: Option<usize>, count: Option<usize>) -> ResearchQuery {
    ResearchQuery {
        path: path.into(),
        start,
        count,
    }
}
#[test]
fn real_research_preserves_scalar_null_original_offsets_clocks_and_latest_bytes_without_report() {
    let path = source();
    let before = std::fs::read(&path).unwrap();
    let mut r = ResearchDocument::open(Some(&path));
    let s = r.status();
    assert_eq!(s["status"], "ready");
    assert_eq!(s["bytes_read"], "15253473");
    assert_eq!(s["catalog"].as_array().unwrap().len(), 14);
    assert_eq!(s["targets"].as_array().unwrap().len(), 12);
    let p = "/research/markets/1/frozen_sampling/0/descriptive_windows/4";
    let a = r.select(q(p, None, None)).unwrap();
    let v: Value = serde_json::from_slice(&a).unwrap();
    assert_eq!(v["exact"]["price_pairs"], "280");
    assert_eq!(v["exact"]["same_received_time_pairs"], "1");
    assert_eq!(
        v["exact"]["squared_price_change_sum"],
        "510.3332819460326887224024"
    );
    assert!(v["exact"]["squared_change_per_second_description"].is_null());
    assert!(v["exact"].get("price_pair_update_indices").is_none());
    let cost = r.last_encoding_cost().clone();
    let retry = r.select(q(p, None, None)).unwrap();
    assert_eq!(a, retry);
    assert_eq!(a.as_ptr(), retry.as_ptr());
    assert_eq!(&cost, r.last_encoding_cost());
    let p = "/research/global_spot/updates_original_visible_order";
    let v: Value = serde_json::from_slice(&r.select(q(p, Some(1), Some(1))).unwrap()).unwrap();
    assert_eq!(v["rows"][0]["path"], format!("{p}/1"));
    let spot = &v["rows"][0]["exact"];
    assert_eq!(spot["source_event_at_utc_ms"][0], "1788911999000");
    assert_eq!(spot["received_at_utc_ms"], "1788912002228");
    assert_eq!(spot["first_visible_at_utc_ms"], "1788912002251");
    assert_eq!(spot["group_position"], "334");
    let v: Value = serde_json::from_slice(
        &r.select(q(
            "/research/markets/11/posthoc_label_qualification",
            None,
            None,
        ))
        .unwrap(),
    )
    .unwrap();
    assert!(v["exact"]["first_usable_terminal"].is_null());
    assert_eq!(v["exact"]["latest_terminal_usable"], false);
    assert!(r.select(q(p, Some(3309), Some(1))).is_err());
    assert!(r.select(q("", None, None)).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
}
#[test]
fn unsupported_and_corrupt_research_are_independent_non_mutating_failures() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("research.json");
    for data in [b"{\"schema\":\"unknown\"}".as_slice(), b"".as_slice()] {
        std::fs::write(&p, data).unwrap();
        let mut r = ResearchDocument::open(Some(&p));
        assert_ne!(r.status()["status"], "ready");
        assert_eq!(r.status()["decode_attempts"], "1");
        assert!(r.select(q("/measurement", None, None)).is_err());
        assert_eq!(std::fs::read(&p).unwrap(), data);
    }
}
