use data_collect::ValidationVerdict;
use data_collect::source_format::{
    ConsumerBoundaryContract, ConsumerKind, SharedConsumerSchema, TimestampNamespace,
};

#[test]
fn data_pipeline_contract_consumes_raw_mirror_without_labels_or_features() {
    let contract = ConsumerBoundaryContract::data_pipeline();
    let report = contract.validate();

    assert_eq!(report.verdict, ValidationVerdict::Complete);
    assert!(contract.consumes_typed_raw_parquet);
    assert!(!contract.data_collect_generates_labels);
    assert!(!contract.data_collect_generates_features);
    assert!(!contract.data_collect_generates_tick_stream);
    assert!(!contract.data_collect_generates_training_split);
}

#[test]
fn training_contract_requires_data_pipeline_outputs_before_training() {
    let contract = ConsumerBoundaryContract::training();
    let report = contract.validate();

    assert_eq!(report.verdict, ValidationVerdict::Complete);
    assert_eq!(contract.consumer, ConsumerKind::Training);
    assert!(!contract.consumes_typed_raw_parquet);
    assert!(contract.requires_data_pipeline_outputs);
    assert!(!contract.data_collect_output_is_training_approved);
}

#[test]
fn model_train_contract_rejects_direct_typed_raw_consumption() {
    let mut contract = ConsumerBoundaryContract::training();
    contract.consumes_typed_raw_parquet = true;

    let report = contract.validate();

    assert_eq!(report.verdict, ValidationVerdict::Incomplete);
    assert!(
        report
            .source_gaps
            .contains(&"model_train_may_not_consume_typed_raw_parquet".to_owned())
    );
}

#[test]
fn data_pipeline_contract_rejects_missing_typed_raw_consumption_boundary() {
    let mut contract = ConsumerBoundaryContract::data_pipeline();
    contract.consumes_typed_raw_parquet = false;

    let report = contract.validate();

    assert_eq!(report.verdict, ValidationVerdict::Incomplete);
    assert!(
        report
            .missing_fields
            .contains(&"typed_raw_parquet_consumer_boundary".to_owned())
    );
}

#[test]
fn paper_live_contract_does_not_replace_runtime_event_log_or_trading_logic() {
    let contract = ConsumerBoundaryContract::paper_live();
    let report = contract.validate();

    assert_eq!(report.verdict, ValidationVerdict::Complete);
    assert_eq!(contract.consumer, ConsumerKind::PaperLive);
    assert!(contract.requires_realtime_handoff);
    assert!(contract.requires_runtime_receive_timestamp);
    assert!(!contract.may_replace_append_only_log);
    assert!(!contract.may_generate_noe_or_ledger);
    assert!(!contract.may_emit_trading_decision);
}

#[test]
fn shared_schema_keeps_source_kind_schema_version_coverage_and_timestamp_namespace_consistent() {
    let schema = SharedConsumerSchema {
        source_kind: "polymarket_market".to_owned(),
        schema_version: "parquet_raw_layout_v1".to_owned(),
        manifest_required: true,
        coverage_window_start_utc_ms: Some(1),
        coverage_window_end_utc_ms: Some(2),
        timestamp_namespace: TimestampNamespace {
            source_ts_name: "source_ts".to_owned(),
            collector_recv_time_name: "recv_time_utc".to_owned(),
            runtime_receive_time_name: Some("paper_live_runtime_recv_time_utc".to_owned()),
        },
        nullable_fields_have_reason: true,
    };

    assert_eq!(schema.validate().verdict, ValidationVerdict::Complete);

    let mut invalid = schema;
    invalid.timestamp_namespace.collector_recv_time_name = "timestamp".to_owned();
    let report = invalid.validate();
    assert_eq!(report.verdict, ValidationVerdict::Incomplete);
    assert!(
        report
            .source_gaps
            .contains(&"collector_recv_time_namespace_invalid".to_owned())
    );
}
