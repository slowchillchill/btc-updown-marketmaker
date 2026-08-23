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
