#[allow(dead_code)]
#[path = "../examples/execution_projection.rs"]
mod projection_generator;
use contracts::execution_normalization::*;
use projection_generator::noe as generator;
use serde_json::Value;
use visualization::{dto::NoeSourceConfig, sources::noe::NoeResults};
fn config(
    root: &std::path::Path,
    version: &str,
    reference: &NoeResultReferenceV1,
) -> NoeSourceConfig {
    let path = root.join(format!("{version}-ref.json"));
    std::fs::write(&path, serde_json::to_vec(reference).unwrap()).unwrap();
    NoeSourceConfig {
        label: "原 writer 工程引用".into(),
        version: version.into(),
        reference_file: path,
    }
}
fn load(reader: &mut NoeResults, index: usize) -> Value {
    serde_json::from_slice(&reader.load(index).unwrap()).unwrap()
}
#[test]
fn explicit_versions_preserve_variants_exact_identity_and_retry_without_consuming() {
    let root = tempfile::tempdir().unwrap();
    let mut configs = vec![];
    macro_rules! generated {
        ($v:literal,$t:ident,$c:ident,$o:ident) => {{
            let refs = generator::sample::<$t, $c>(
                &root.path().join($v),
                $v,
                $c::Submission {
                    outcome: $o::RejectedNoEffect,
                },
                $c::Submission {
                    outcome: $o::EvidenceUnknownNoEffect,
                },
            );
            assert_eq!(refs.len(), 6);
            for (i, r) in refs.iter().enumerate() {
                configs.push(config(root.path(), &format!("{v}-{i}", v = $v), r));
                configs.last_mut().unwrap().version = $v.into();
            }
        }};
    }
    generated!(
        "v1",
        SourceNeutralTradeLifecycleResultV1,
        SourceNeutralControlResultV1,
        SourceNeutralSubmissionOutcomeV1
    );
    generated!(
        "v2",
        SourceNeutralTradeLifecycleResultV2,
        SourceNeutralControlResultV1,
        SourceNeutralSubmissionOutcomeV1
    );
    generated!(
        "v3",
        SourceNeutralTradeLifecycleResultV2,
        SourceNeutralControlResultV2,
        SourceNeutralSubmissionOutcomeV2
    );
    generated!(
        "v4",
        SourceNeutralTradeLifecycleResultV2,
        SourceNeutralControlResultV3,
        SourceNeutralSubmissionOutcomeV2
    );
    let mut reader = NoeResults::new(configs);
    assert_eq!(reader.index().len(), 24);
    let expected = [
        "EconomicFact",
        "EconomicAlias",
        "Evidence",
        "Duplicate",
        "Conflict",
        "Pending",
    ];
    for i in 0..24 {
        let data = load(&mut reader, i);
        assert_eq!(data["status"], "ready");
        let variant = data["exact"]["record"]
            .as_object()
            .unwrap()
            .keys()
            .next()
            .unwrap();
        assert_eq!(
            variant,
            if i == 18 {
                "EconomicFactV2"
            } else {
                expected[i % 6]
            }
        );
        assert_eq!(
            data["exact"]["boundary"]["durable_available_at_utc_ms"],
            "1788840000002"
        );
        assert!(data["exact"].to_string().contains("9007199254740993"));
        let encoded = reader.last_encoding_cost();
        let cached = reader.load(i).unwrap();
        assert_eq!(encoded.response_bytes, cached.len());
        assert_eq!(
            reader.last_encoding_cost().response_encoding_us,
            encoded.response_encoding_us
        );
        assert_eq!(data, serde_json::from_slice::<Value>(&cached).unwrap());
        assert_eq!(data["cost"]["native_read_calls_total"], (i + 1).to_string());
    }
}
#[test]
fn independent_version_corruption_and_reference_bounds_fail_without_repair() {
    let root = tempfile::tempdir().unwrap();
    let refs = generator::sample::<SourceNeutralTradeLifecycleResultV2, SourceNeutralControlResultV3>(
        &root.path().join("v4"),
        "v4",
        SourceNeutralControlResultV3::Submission {
            outcome: SourceNeutralSubmissionOutcomeV2::RejectedNoEffect,
        },
        SourceNeutralControlResultV3::Submission {
            outcome: SourceNeutralSubmissionOutcomeV2::EvidenceUnknownNoEffect,
        },
    );
    let original = std::fs::read(&refs[0].commit_container.location).unwrap();
    let mut outside = refs[0].clone();
    outside.record_ordinal = 999;
    let unknown = config(root.path(), "v999", &refs[0]);
    let wrong = config(root.path(), "v1", &refs[0]);
    let bad = config(root.path(), "v4-bounds", &outside);
    let mut reader = NoeResults::new(vec![
        unknown,
        wrong,
        NoeSourceConfig {
            version: "v4".into(),
            ..bad
        },
        config(root.path(), "v4", &refs[0]),
    ]);
    assert_eq!(load(&mut reader, 0)["status"], "unknown_version");
    assert_eq!(load(&mut reader, 0)["cost"]["native_read_attempts"], "0");
    assert_eq!(load(&mut reader, 1)["status"], "read_error");
    assert_eq!(load(&mut reader, 2)["status"], "read_error");
    assert_eq!(load(&mut reader, 3)["status"], "ready");
    assert_eq!(
        std::fs::read(&refs[0].commit_container.location).unwrap(),
        original
    );
    // 缺原上下文独立失败；不创建它，也不更改提交容器。
    let context_path = std::path::Path::new(&refs[0].commit_container.location)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("context.json");
    let context_bytes = std::fs::read(&context_path).unwrap();
    std::fs::remove_file(&context_path).unwrap();
    let mut missing = config(root.path(), "missing-context", &refs[0]);
    missing.version = "v4".into();
    assert_eq!(
        load(&mut NoeResults::new(vec![missing]), 0)["status"],
        "read_error"
    );
    assert!(!context_path.exists());
    assert_eq!(
        std::fs::read(&refs[0].commit_container.location).unwrap(),
        original
    );
    std::fs::write(&context_path, context_bytes).unwrap();
    // 单一可达损坏代表：截断真实 writer 容器；读取不能补尾/恢复它。
    std::fs::write(
        &refs[0].commit_container.location,
        &original[..original.len() / 2],
    )
    .unwrap();
    let mut bad = config(root.path(), "corrupt", &refs[0]);
    bad.version = "v4".into();
    let mut corrupt = NoeResults::new(vec![bad]);
    assert_eq!(load(&mut corrupt, 0)["status"], "read_error");
    assert_eq!(
        std::fs::read(&refs[0].commit_container.location).unwrap(),
        original[..original.len() / 2]
    );
}
#[test]
fn legal_ledger_projection_includes_confirmed_pending_and_projection_only_market() {
    let root = tempfile::tempdir().unwrap();
    let p = projection_generator::projection(root.path());
    assert_eq!(p.local_pending_reservation_projection.before_send_count, 2);
    assert_eq!(
        p.local_pending_reservation_projection
            .before_send_principal_pusd
            .0,
        "5.5"
    );
    assert_eq!(
        p.local_pending_reservation_projection
            .after_send_unknown_count,
        1
    );
    assert_eq!(
        p.platform_confirmed_projection
            .platform_confirmation_pending_economic_count,
        1
    );
    assert_eq!(
        p.platform_confirmed_projection.principal_encumbrance_pusd.0,
        "3.3"
    );
    assert_eq!(
        p.platform_confirmed_projection
            .immediate_taker_fee_encumbrance_pusd
            .0,
        "0.03"
    );
    assert_eq!(p.platform_confirmed_projection.open_order_count, 1);
    assert_eq!(p.platform_confirmed_projection.pending_fill_count, 1);
    assert_eq!(p.market_projections.len(), 2);
    assert_eq!(
        p.market_projections[1].local_before_send_principal_pusd.0,
        "4.4"
    );
}
#[tokio::test]
async fn noe_only_source_starts_with_unknown_version_and_no_eager_file_read() {
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;
    use visualization::{
        api::{load_sources, router},
        dto::{RunSourceConfig, SourcesConfig},
    };
    let missing = std::path::PathBuf::from("not-a-returned-reference.json");
    let app = router(
        load_sources(SourcesConfig {
            runs: vec![RunSourceConfig {
                label: "独立 NOE 工程来源".into(),
                report_kind: Default::default(),
                report: None,
                metadata: None,
                public_recording: None,
                noe_results: vec![NoeSourceConfig {
                    label: "未知".into(),
                    version: "v999".into(),
                    reference_file: missing,
                }],
            }],
        })
        .unwrap(),
    );
    for (path, expected) in [
        ("/api/runs/0/snapshot", "not_configured"),
        ("/api/runs/0/noe/0", "unknown_version"),
    ] {
        let r = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let data: Value =
            serde_json::from_slice(&to_bytes(r.into_body(), usize::MAX).await.unwrap()).unwrap();
        if path.ends_with("snapshot") {
            assert_eq!(data["report"]["status"], expected);
            assert_eq!(data["public_recording"]["status"], expected);
            assert_eq!(data["noe_results"][0]["status"], "unknown_version");
        } else {
            assert_eq!(data["status"], expected);
            assert_eq!(data["cost"]["native_read_attempts"], "0");
        }
    }
}
