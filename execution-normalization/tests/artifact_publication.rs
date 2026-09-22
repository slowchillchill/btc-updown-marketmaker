use contracts::execution_normalization::{
    DurableSourceReferenceV1, ExecutionNormalizationContextReferenceV1, ImmutableSourceReferenceV1,
    NoeComponentCapabilityReceiptV1, NoeInclusiveSequenceRangeV1, NoePublishedContainerReferenceV1,
    NoeRunConsumptionManifestV1, SimV2SourceRecordReferenceV1, SimV2SourceWatermarkV1,
};
use contracts::{AccountInstanceKeyV1, PlatformSourceV1, Sha256Hex, SimCallerRoleV1};
use execution_normalization::{
    DurableContextStoreV1, NoeConsumerFixtureV1, NoeResourceReportV1, NoeTestEvidenceV1,
    NormalizationBatchInputV1, PendingScopeV1, SimV2ContextSeedV1, SourceNeutralEvidenceKindV1,
    SourceNeutralEvidenceV1, SourceNeutralLogicalKeyV1, publish_noe_component_receipt_v1,
    publish_noe_consumer_fixture_v1, publish_noe_resource_report_v1, publish_noe_run_manifest_v1,
    publish_noe_test_evidence_v1, read_noe_component_receipt_v1, read_noe_consumer_fixture_v1,
    read_noe_resource_report_v1, read_noe_result_v1, read_noe_run_manifest_v1,
    read_noe_test_evidence_v1,
};
use tempfile::TempDir;

#[test]
fn reports_receipt_and_run_manifest_round_trip_through_no_replace_containers() {
    let temporary = TempDir::new().unwrap();
    let test_report = NoeTestEvidenceV1 {
        implementation_identity: "noe-58".to_owned(),
        commands: vec!["cargo test -p execution-normalization --locked --offline".to_owned()],
        skipped_tests: vec![
            "four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second"
                .to_owned(),
        ],
    };
    let test_reference =
        publish_noe_test_evidence_v1(temporary.path().join("test.json"), &test_report).unwrap();
    assert_eq!(
        read_noe_test_evidence_v1(&test_reference).unwrap(),
        test_report
    );

    let resource_report = NoeResourceReportV1 {
        implementation_identity: "noe-58".to_owned(),
        trajectory_batches: 10_000,
        elapsed_milliseconds: 2_000,
        throughput_batches_per_second_milli: 5_000_000,
        peak_resident_memory_bytes: 50_000_000,
        peak_pending_projection_batches: 1,
        durable_disk_bytes: 4_000_000,
        reopen_elapsed_microseconds: 1_000,
        durable_bytes_growth_per_batch_milli: 400_000,
    };
    let resource_reference =
        publish_noe_resource_report_v1(temporary.path().join("resource.json"), &resource_report)
            .unwrap();
    assert_eq!(
        read_noe_resource_report_v1(&resource_reference).unwrap(),
        resource_report
    );

    let receipt = NoeComponentCapabilityReceiptV1 {
        contract_identity: "execution_normalization_v1".to_owned(),
        implementation_identity: "noe-58".to_owned(),
        test_report: test_reference,
        resource_report: resource_reference,
    };
    let receipt_reference =
        publish_noe_component_receipt_v1(temporary.path().join("receipt.json"), &receipt).unwrap();
    assert_eq!(
        read_noe_component_receipt_v1(&receipt_reference).unwrap(),
        receipt
    );

    let context_container = NoePublishedContainerReferenceV1 {
        location: "/tmp/noe/context.json".to_owned(),
        boundary_sha256: Sha256Hex("b".repeat(64)),
    };
    let manifest = NoeRunConsumptionManifestV1 {
        context_reference: ExecutionNormalizationContextReferenceV1("context-58".to_owned()),
        input_projection_batch_range: NoeInclusiveSequenceRangeV1 { first: 1, last: 2 },
        output_commit_range: NoeInclusiveSequenceRangeV1 { first: 0, last: 1 },
        source_watermark: SimV2SourceWatermarkV1 {
            run_id: "run-58".to_owned(),
            projection_batch_sequence: 2,
        },
        context_container: context_container.clone(),
        commit_containers: vec![context_container],
    };
    let manifest_reference =
        publish_noe_run_manifest_v1(temporary.path().join("manifest.json"), &manifest).unwrap();
    assert_eq!(
        read_noe_run_manifest_v1(&manifest_reference).unwrap(),
        manifest
    );
    assert!(
        publish_noe_run_manifest_v1(temporary.path().join("manifest.json"), &manifest).is_err(),
        "同一路径不得替换已经发布的容器"
    );
}

fn required_u64(name: &str) -> u64 {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("缺少资源观测环境变量 {name}"))
        .parse()
        .unwrap_or_else(|_| panic!("资源观测环境变量 {name} 不是 u64"))
}

#[test]
fn publish_stage2_evidence_bundle_when_explicitly_requested() {
    let Ok(evidence_root) = std::env::var("NOE_STAGE2_EVIDENCE_ROOT") else {
        return;
    };
    let head_sha = std::env::var("NOE_STAGE2_HEAD_SHA").expect("必须显式传入待审头 SHA");
    let evidence_root = std::path::PathBuf::from(evidence_root);
    let runtime_root = evidence_root.join("noe-runtime");
    let seed = SimV2ContextSeedV1 {
        run_id: "stage-2-consumer-fixture".to_owned(),
        account_instance_key: AccountInstanceKeyV1 {
            run_id: "stage-2-consumer-fixture".to_owned(),
            caller_role: SimCallerRoleV1::ModelTrain,
            caller_instance_id: "stage-2-evidence".to_owned(),
            account_id: "stage-2-evidence".to_owned(),
            platform_source: PlatformSourceV1::Simulated,
        },
        run_opened: DurableSourceReferenceV1 {
            source: ImmutableSourceReferenceV1::Sha256(Sha256Hex("1".repeat(64))),
        },
        account_opening: DurableSourceReferenceV1 {
            source: ImmutableSourceReferenceV1::Sha256(Sha256Hex("2".repeat(64))),
        },
        account_opening_evidence_identity: "stage-2-opening".to_owned(),
        implementation_identity: head_sha.clone(),
    };
    let mut store = DurableContextStoreV1::open_or_create_sim_v2(&runtime_root, seed).unwrap();
    let watermark = SimV2SourceWatermarkV1 {
        run_id: "stage-2-consumer-fixture".to_owned(),
        projection_batch_sequence: 1,
    };
    store
        .commit_batch(
            NormalizationBatchInputV1 {
                source_watermark: watermark.clone(),
                evidence: vec![SourceNeutralEvidenceV1 {
                    source_record: SimV2SourceRecordReferenceV1::OrderLifecycle {
                        private_feedback_sequence: 1,
                        platform_event_at_utc_ms: 1,
                        feedback_received_at_utc_ms: 2,
                    },
                    source_ordinal: 0,
                    logical_key: SourceNeutralLogicalKeyV1::AuditEvidence {
                        evidence_identity: "stage-2-consumer-fixture".to_owned(),
                    },
                    kind: SourceNeutralEvidenceKindV1::Pending {
                        scope: PendingScopeV1::EvidenceGap {
                            scope_identity: "stage-2-consumer-fixture".to_owned(),
                        },
                    },
                }],
            },
            1,
        )
        .unwrap();
    let result_reference = store.commits()[0].result_reference(0).unwrap();

    let test_report = NoeTestEvidenceV1 {
        implementation_identity: head_sha.clone(),
        commands: vec![
            "cargo fmt --all -- --check".to_owned(),
            "cargo check --workspace --all-targets --locked --offline".to_owned(),
            "cargo test -p contracts --test noe_stage2_contract --locked --offline".to_owned(),
            "cargo test -p execution-normalization --locked --offline".to_owned(),
            "cargo test -p sim-system --locked --offline".to_owned(),
            "cargo tree -p execution-normalization --locked --offline --edges normal".to_owned(),
            "cargo clippy --workspace --all-targets --locked --offline".to_owned(),
            "cargo test --workspace --lib --bins --tests --examples --locked --offline -- --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second".to_owned(),
        ],
        skipped_tests: vec![
            "four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second"
                .to_owned(),
        ],
    };
    let test_reference =
        publish_noe_test_evidence_v1(evidence_root.join("test-evidence-v1.json"), &test_report)
            .unwrap();
    let resource_report = NoeResourceReportV1 {
        implementation_identity: head_sha.clone(),
        trajectory_batches: required_u64("NOE_STAGE2_TRAJECTORY_BATCHES"),
        elapsed_milliseconds: required_u64("NOE_STAGE2_ELAPSED_MILLISECONDS"),
        throughput_batches_per_second_milli: required_u64(
            "NOE_STAGE2_THROUGHPUT_BATCHES_PER_SECOND_MILLI",
        ),
        peak_resident_memory_bytes: required_u64("NOE_STAGE2_PEAK_RESIDENT_MEMORY_BYTES"),
        peak_pending_projection_batches: required_u64("NOE_STAGE2_PEAK_PENDING_PROJECTION_BATCHES"),
        durable_disk_bytes: required_u64("NOE_STAGE2_DURABLE_DISK_BYTES"),
        reopen_elapsed_microseconds: required_u64("NOE_STAGE2_REOPEN_ELAPSED_MICROSECONDS"),
        durable_bytes_growth_per_batch_milli: required_u64(
            "NOE_STAGE2_DURABLE_BYTES_GROWTH_PER_BATCH_MILLI",
        ),
    };
    let resource_reference = publish_noe_resource_report_v1(
        evidence_root.join("resource-report-v1.json"),
        &resource_report,
    )
    .unwrap();
    let fixture = NoeConsumerFixtureV1 {
        result_reference: result_reference.clone(),
    };
    let fixture_reference =
        publish_noe_consumer_fixture_v1(evidence_root.join("consumer-fixture-v1.json"), &fixture)
            .unwrap();
    let receipt = NoeComponentCapabilityReceiptV1 {
        contract_identity: "execution_normalization_v1".to_owned(),
        implementation_identity: head_sha,
        test_report: test_reference.clone(),
        resource_report: resource_reference.clone(),
    };
    let receipt_reference = publish_noe_component_receipt_v1(
        evidence_root.join("component-capability-receipt-v1.json"),
        &receipt,
    )
    .unwrap();
    let manifest = NoeRunConsumptionManifestV1 {
        context_reference: store.context().context_reference.clone(),
        input_projection_batch_range: NoeInclusiveSequenceRangeV1 { first: 1, last: 1 },
        output_commit_range: NoeInclusiveSequenceRangeV1 { first: 0, last: 0 },
        source_watermark: watermark,
        context_container: store.context_container_reference().clone(),
        commit_containers: vec![store.commits()[0].container_reference.clone()],
    };
    let manifest_reference = publish_noe_run_manifest_v1(
        evidence_root.join("run-consumption-manifest-v1.json"),
        &manifest,
    )
    .unwrap();

    assert_eq!(
        read_noe_test_evidence_v1(&test_reference).unwrap(),
        test_report
    );
    assert_eq!(
        read_noe_resource_report_v1(&resource_reference).unwrap(),
        resource_report
    );
    assert_eq!(
        read_noe_consumer_fixture_v1(&fixture_reference).unwrap(),
        fixture
    );
    assert_eq!(
        read_noe_component_receipt_v1(&receipt_reference).unwrap(),
        receipt
    );
    assert_eq!(
        read_noe_run_manifest_v1(&manifest_reference).unwrap(),
        manifest
    );
    assert_eq!(
        read_noe_result_v1(&result_reference)
            .unwrap()
            .context
            .context_reference,
        store.context().context_reference
    );
}
