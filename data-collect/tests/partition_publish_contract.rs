use data_collect::publish::{
    DATA_COLLECT_ALLOWED_ARTIFACT_ROLES, PAPER_LIVE_FORBIDDEN_RUNTIME_ROLES, PaperLiveEnvironment,
    paper_live_handoff_boundary, training_utc_day_partition,
};
use tempfile::tempdir;

#[test]
fn utc_day_partition_contract() {
    let tmp = tempdir().unwrap();

    let partition =
        training_utc_day_partition(tmp.path(), "polymarket_market", 86_400_000).unwrap();

    assert_eq!(partition.utc_day, "1970-01-02");
    assert_eq!(partition.source_kind, "polymarket_market");
    assert!(partition.partition_path.starts_with(tmp.path()));
    assert!(
        partition
            .partition_path
            .ends_with("training/typed_raw/source_kind=polymarket_market/utc_day=1970-01-02")
    );
}

#[test]
fn utc_day_partition_requires_server_local_absolute_root_and_safe_source_kind() {
    let tmp = tempdir().unwrap();

    let relative_root = std::path::Path::new("datasets");
    assert!(training_utc_day_partition(relative_root, "polymarket_market", 0).is_err());
    assert!(training_utc_day_partition(tmp.path(), "../evil", 0).is_err());
}

#[test]
fn paper_live_boundary_excludes_runtime_trade_artifacts() {
    let tmp = tempdir().unwrap();

    for environment in [PaperLiveEnvironment::Paper, PaperLiveEnvironment::Live] {
        let boundary = paper_live_handoff_boundary(tmp.path(), environment).unwrap();
        assert!(boundary.artifact_root.starts_with(tmp.path()));
        assert!(
            boundary
                .artifact_root
                .ends_with(format!("{}/data_collect_handoff", environment.as_str()))
        );

        for role in DATA_COLLECT_ALLOWED_ARTIFACT_ROLES {
            assert!(boundary.allows_role(role), "{role} should be allowed");
        }
        for role in PAPER_LIVE_FORBIDDEN_RUNTIME_ROLES {
            assert!(
                boundary.forbids_runtime_role(role),
                "{role} should be forbidden"
            );
            assert!(
                !boundary.allows_role(role),
                "{role} must not be published by data-collect"
            );
        }
    }
}
