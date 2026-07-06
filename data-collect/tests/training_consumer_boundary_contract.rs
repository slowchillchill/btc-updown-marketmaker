use data_collect::manifest::{
    ConsumerBoundaryKind, required_consumer_boundary_declarations,
    training_consumer_boundary_declaration,
};
use data_collect::publish::training_utc_day_partition;
use data_collect::source_format::{ConsumerBoundaryContract, ConsumerKind};
use tempfile::tempdir;

#[test]
fn training_consumer_reads_finalized_raw_mirror_but_not_training_outputs() {
    let declaration = training_consumer_boundary_declaration();

    assert_eq!(declaration.consumer, ConsumerBoundaryKind::Training);
    assert!(declaration.allows_data_collect_role("finalized_typed_raw_parquet"));
    assert!(declaration.allows_data_collect_role("manifest"));
    assert!(declaration.allows_data_collect_role("source_coverage"));
    assert!(declaration.requires_external_role("data_pipeline_labels"));
    assert!(declaration.requires_external_role("data_pipeline_features"));
    assert!(declaration.requires_external_role("data_pipeline_tick_stream"));
    assert!(declaration.requires_external_role("data_pipeline_training_split"));
    assert!(declaration.forbids_data_collect_role("labels"));
    assert!(declaration.forbids_data_collect_role("features"));
    assert!(declaration.forbids_data_collect_role("tick_stream"));
    assert!(declaration.forbids_data_collect_role("canonical_split"));
    assert!(declaration.forbids_data_collect_role("purged_embargo_split"));
    assert!(declaration.forbids_data_collect_role("t30_training_sample"));
    assert!(declaration.forbids_data_collect_role("training_approval"));
    assert!(!declaration.data_collect_output_is_training_approved);
    assert!(!declaration.finalized_parquet_is_hot_path_source_of_truth);
    assert!(declaration.is_valid_for_manifest());
}

#[test]
fn training_boundary_uses_utc_day_finalized_partition_under_dataset_root() {
    let tmp = tempdir().unwrap();
    let partition =
        training_utc_day_partition(tmp.path(), "polymarket_market", 1_767_225_600_000).unwrap();

    let partition = partition.partition_path.to_string_lossy();
    assert!(partition.contains("/training/typed_raw/"));
    assert!(partition.contains("source_kind=polymarket_market"));
    assert!(partition.contains("utc_day=2026-01-01"));
}

#[test]
fn training_consumer_boundary_matches_source_format_contract() {
    let contract = ConsumerBoundaryContract::training();

    assert_eq!(contract.consumer, ConsumerKind::Training);
    assert!(contract.requires_data_pipeline_outputs);
    assert!(!contract.data_collect_generates_labels);
    assert!(!contract.data_collect_generates_features);
    assert!(!contract.data_collect_generates_tick_stream);
    assert!(!contract.data_collect_generates_training_split);
    assert!(!contract.data_collect_output_is_training_approved);
    assert!(!contract.requires_realtime_handoff);
    assert_eq!(
        contract.validate().verdict,
        data_collect::ValidationVerdict::Complete
    );
}

#[test]
fn manifest_requires_training_and_paper_live_boundary_declarations_together() {
    let declarations = required_consumer_boundary_declarations();

    assert!(
        declarations
            .iter()
            .any(|declaration| declaration.consumer == ConsumerBoundaryKind::Training)
    );
    assert!(
        declarations
            .iter()
            .any(|declaration| declaration.consumer == ConsumerBoundaryKind::PaperLive)
    );
    assert!(
        declarations
            .iter()
            .all(|declaration| declaration.is_valid_for_manifest())
    );
}
