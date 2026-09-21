use std::fs;

use contracts::execution_normalization::{
    DurableSourceReferenceV1, ExecutionNormalizationContextReferenceV1, ImmutableSourceReferenceV1,
    SimV2SourceRecordReferenceV1, SimV2SourceReferenceV1, SimV2SourceWatermarkV1,
};
use contracts::{AccountInstanceKeyV1, PlatformSourceV1, Sha256Hex, SimCallerRoleV1};
use execution_normalization::{
    DurableCommitOutcomeV1, DurableContextStoreV1, DurableErrorV1, NormalizationBatchInputV1,
    PendingScopeV1, SimV2ContextSeedV1, SourceNeutralEvidenceKindV1, SourceNeutralEvidenceV1,
    SourceNeutralLogicalKeyV1, read_noe_result_v1,
};
use tempfile::TempDir;

fn seed() -> SimV2ContextSeedV1 {
    SimV2ContextSeedV1 {
        run_id: "run-result-58".to_owned(),
        account_instance_key: AccountInstanceKeyV1 {
            run_id: "run-result-58".to_owned(),
            caller_role: SimCallerRoleV1::ModelTrain,
            caller_instance_id: "caller-result-58".to_owned(),
            account_id: "account-result-58".to_owned(),
            platform_source: PlatformSourceV1::Simulated,
        },
        run_opened: DurableSourceReferenceV1 {
            source: ImmutableSourceReferenceV1::Sha256(Sha256Hex("1".repeat(64))),
        },
        account_opening: DurableSourceReferenceV1 {
            source: ImmutableSourceReferenceV1::Sha256(Sha256Hex("2".repeat(64))),
        },
        account_opening_evidence_identity: "opening-result-58".to_owned(),
        implementation_identity: "noe-result-58".to_owned(),
    }
}

fn batch(context_reference: ExecutionNormalizationContextReferenceV1) -> NormalizationBatchInputV1 {
    let watermark = SimV2SourceWatermarkV1 {
        run_id: "run-result-58".to_owned(),
        projection_batch_sequence: 1,
    };
    NormalizationBatchInputV1 {
        source_watermark: watermark.clone(),
        evidence: (0..2)
            .map(|ordinal| SourceNeutralEvidenceV1 {
                source_reference: SimV2SourceReferenceV1 {
                    context_reference: context_reference.clone(),
                    watermark: watermark.clone(),
                    record: SimV2SourceRecordReferenceV1::OrderLifecycle {
                        private_feedback_sequence: 1,
                        platform_event_at_utc_ms: 10,
                        feedback_received_at_utc_ms: 20,
                    },
                },
                source_ordinal: ordinal,
                logical_key: SourceNeutralLogicalKeyV1::AuditEvidence {
                    evidence_identity: format!("evidence-{ordinal}"),
                },
                kind: SourceNeutralEvidenceKindV1::Pending {
                    scope: PendingScopeV1::EvidenceGap {
                        scope_identity: format!("gap-{ordinal}"),
                    },
                },
            })
            .collect(),
    }
}

#[test]
fn one_reference_reads_the_same_context_boundary_and_record_after_reopen() {
    let temporary = TempDir::new().unwrap();
    let mut store = DurableContextStoreV1::open_or_create_sim_v2(temporary.path(), seed()).unwrap();
    let context = store.context().clone();
    let input = batch(context.context_reference.clone());
    store.commit_batch(input.clone(), 30).unwrap();
    let references: Vec<_> = store.commits()[0].result_references().collect();
    assert_eq!(references.len(), 2);
    assert_ne!(references[0].record_ordinal, references[1].record_ordinal);

    let first = read_noe_result_v1(&references[0]).unwrap();
    let second = read_noe_result_v1(&references[1]).unwrap();
    assert_eq!(first.context, context);
    assert_eq!(first.boundary, second.boundary);
    assert_ne!(
        first.record.logical_identity(),
        second.record.logical_identity()
    );

    let mut reopened =
        DurableContextStoreV1::open_or_create_sim_v2(temporary.path(), seed()).unwrap();
    assert!(matches!(
        reopened.commit_batch(input, 31).unwrap(),
        DurableCommitOutcomeV1::AlreadyDurable { .. }
    ));
    assert_eq!(
        reopened.commits()[0].result_reference(0).unwrap(),
        references[0]
    );
}

#[test]
fn a_wrong_digest_corrupt_container_and_out_of_range_ordinal_are_rejected() {
    let temporary = TempDir::new().unwrap();
    let mut store = DurableContextStoreV1::open_or_create_sim_v2(temporary.path(), seed()).unwrap();
    let input = batch(store.context().context_reference.clone());
    store.commit_batch(input, 30).unwrap();
    let reference = store.commits()[0].result_reference(0).unwrap();

    let mut wrong_digest = reference.clone();
    wrong_digest.commit_container.boundary_sha256 = Sha256Hex("f".repeat(64));
    assert!(matches!(
        read_noe_result_v1(&wrong_digest),
        Err(DurableErrorV1::BoundaryDigestMismatch { .. })
    ));

    let mut out_of_range = reference.clone();
    out_of_range.record_ordinal = 2;
    assert!(matches!(
        read_noe_result_v1(&out_of_range),
        Err(DurableErrorV1::RecordOrdinalOutOfRange { .. })
    ));

    let bytes = fs::read(&reference.commit_container.location).unwrap();
    let mut corrupt = bytes;
    corrupt[0] ^= 1;
    fs::write(&reference.commit_container.location, corrupt).unwrap();
    assert!(matches!(
        read_noe_result_v1(&reference),
        Err(DurableErrorV1::ContainerDecode { .. })
            | Err(DurableErrorV1::BoundaryDigestMismatch { .. })
    ));
}

#[test]
fn recovery_position_slot_checks_each_ordinal_and_revalidates_each_new_key() {
    use execution_normalization::{
        DurableContextStoreV3, NoeRecoveryPositionReader, NormalizationBatchInputV3,
        SourceNeutralEvidenceKindV3, SourceNeutralEvidenceV3, read_durable_noe_record_position,
    };
    let temporary = TempDir::new().unwrap();
    let mut store = DurableContextStoreV3::open_or_create_sim_v2(temporary.path(), seed()).unwrap();
    for sequence in 1..=2 {
        let mut input = batch(store.context().context_reference.clone());
        input.source_watermark.projection_batch_sequence = sequence;
        let input = NormalizationBatchInputV3 {
            source_watermark: input.source_watermark.clone(),
            evidence: input
                .evidence
                .into_iter()
                .map(|e| SourceNeutralEvidenceV3 {
                    source_reference: SimV2SourceReferenceV1 {
                        watermark: input.source_watermark.clone(),
                        ..e.source_reference
                    },
                    source_ordinal: e.source_ordinal,
                    logical_key: SourceNeutralLogicalKeyV1::AuditEvidence {
                        evidence_identity: format!("{sequence}-{}", e.source_ordinal),
                    },
                    kind: SourceNeutralEvidenceKindV3::Pending {
                        scope: PendingScopeV1::EvidenceGap {
                            scope_identity: format!("{sequence}-{}", e.source_ordinal),
                        },
                    },
                })
                .collect(),
        };
        store.commit_batch_v3(input, 30).unwrap();
    }
    let first = store.commits()[0].result_reference(0).unwrap();
    let second = store.commits()[0].result_reference(1).unwrap();
    let next = store.commits()[1].result_reference(0).unwrap();
    let mut reader = NoeRecoveryPositionReader::default();
    assert_eq!(
        reader.read(&first).unwrap(),
        read_durable_noe_record_position(&first).unwrap()
    );
    assert_eq!(
        reader.read(&second).unwrap(),
        read_durable_noe_record_position(&second).unwrap()
    );
    let mut invalid = first.clone();
    invalid.record_ordinal = 2;
    assert!(matches!(
        reader.read(&invalid),
        Err(DurableErrorV1::RecordOrdinalOutOfRange { .. })
    ));
    let mut wrong_hash = first.clone();
    wrong_hash.commit_container.boundary_sha256 = Sha256Hex("f".repeat(64));
    assert!(matches!(
        reader.read(&wrong_hash),
        Err(DurableErrorV1::BoundaryDigestMismatch { .. })
    ));
    // 已提交文件无合法原地改写路径；同键命中不持续监测，独立读取和新恢复重新检查。
    let bytes = fs::read(&first.commit_container.location).unwrap();
    fs::write(&first.commit_container.location, b"broken").unwrap();
    assert_eq!(reader.read(&second).unwrap().record_ordinal, 1);
    assert!(read_durable_noe_record_position(&second).is_err());
    let mut reopened = NoeRecoveryPositionReader::default();
    assert!(reopened.read(&first).is_err(), "首次损坏不能成为已验证槽");
    assert_eq!(
        reader
            .read(&next)
            .unwrap()
            .source_watermark
            .projection_batch_sequence,
        2
    );
    assert!(reader.read(&first).is_err(), "换批后不能保留全历史命中");
    fs::write(&first.commit_container.location, bytes).unwrap();
    assert!(reopened.read(&first).is_ok(), "失败没有保存验证结果");
}
