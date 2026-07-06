use data_collect::manifest::{ConsumerBoundaryKind, paper_live_consumer_boundary_declaration};
use data_collect::publish::{PaperLiveEnvironment, paper_live_handoff_boundary};
use data_collect::source_format::{ConsumerBoundaryContract, ConsumerKind};
use tempfile::tempdir;

#[test]
fn paper_live_boundary_requires_realtime_handoff_not_finalized_parquet_hot_path() {
    let declaration = paper_live_consumer_boundary_declaration();

    assert_eq!(declaration.consumer, ConsumerBoundaryKind::PaperLive);
    assert!(declaration.requires_realtime_handoff);
    assert!(declaration.requires_consumer_host_runtime_receive_timestamp);
    assert!(declaration.allows_data_collect_role("external_market_event"));
    assert!(declaration.allows_data_collect_role("timestamp_namespace"));
    assert!(declaration.allows_data_collect_role("source_coverage"));
    assert!(declaration.allows_data_collect_role("source_health"));
    assert!(declaration.allows_data_collect_role("source_gap"));
    assert!(declaration.allows_data_collect_role("reconnect_trace"));
    assert!(declaration.allows_data_collect_role("backpressure_state"));
    assert!(declaration.allows_data_collect_role("realtime_handoff_boundary_evidence"));
    assert!(declaration.requires_external_role("paper_live_runtime_receive_timestamp"));
    assert!(!declaration.finalized_parquet_is_hot_path_source_of_truth);
    assert!(declaration.is_valid_for_manifest());
}

#[test]
fn paper_live_boundary_forbids_trading_runtime_artifacts_and_submit_path() {
    let declaration = paper_live_consumer_boundary_declaration();

    for forbidden in [
        "paper_live_append_only_event_log",
        "rotation_artifact",
        "live_artifact",
        "order_evidence",
        "NormalizedOrderEvent",
        "ledger",
        "execution_feedback_ingestion",
        "trading_decision",
        "risk_control",
        "submit_cancel_path",
        "model_input_assembly",
    ] {
        assert!(
            declaration.forbids_data_collect_role(forbidden),
            "{forbidden} must stay outside data-collect"
        );
    }
    assert!(!declaration.may_emit_trading_decision);
}

#[test]
fn paper_live_boundary_matches_source_format_contract() {
    let contract = ConsumerBoundaryContract::paper_live();

    assert_eq!(contract.consumer, ConsumerKind::PaperLive);
    assert!(contract.requires_realtime_handoff);
    assert!(contract.requires_runtime_receive_timestamp);
    assert!(!contract.may_replace_append_only_log);
    assert!(!contract.may_emit_trading_decision);
    assert_eq!(
        contract.validate().verdict,
        data_collect::ValidationVerdict::Complete
    );
}

#[test]
fn paper_live_handoff_boundary_keeps_runtime_artifacts_out_of_data_collect() {
    let tmp = tempdir().unwrap();

    for environment in [PaperLiveEnvironment::Paper, PaperLiveEnvironment::Live] {
        let boundary = paper_live_handoff_boundary(tmp.path(), environment).unwrap();
        assert!(boundary.artifact_root.ends_with("data_collect_handoff"));
        assert!(boundary.allows_role("finalized_typed_raw_parquet"));
        assert!(boundary.allows_role("source_gap"));
        assert!(boundary.allows_role("backpressure_state"));
        assert!(boundary.allows_role("realtime_handoff_boundary_evidence"));

        let declaration = paper_live_consumer_boundary_declaration();
        for role in &declaration.allowed_data_collect_roles {
            assert!(
                boundary.allows_role(role),
                "{role} should be allowed by paper/live handoff boundary"
            );
        }

        for forbidden in [
            "paper_live_append_only_event_log",
            "rotation_artifact",
            "live_artifact",
            "order_evidence",
            "NormalizedOrderEvent",
            "ledger",
            "noe_ledger",
            "execution_feedback_ingestion",
            "trading_decision",
            "risk_control",
            "submit_cancel_path",
            "model_input_assembly",
        ] {
            assert!(boundary.forbids_runtime_role(forbidden));
        }
        for forbidden in &declaration.forbidden_data_collect_roles {
            assert!(
                boundary.forbids_runtime_role(forbidden),
                "{forbidden} must be forbidden by paper/live handoff boundary"
            );
        }
    }
}
