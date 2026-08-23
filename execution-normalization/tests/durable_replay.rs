use std::fs;
use std::path::{Path, PathBuf};

use contracts::execution_normalization::{
    DurableSourceReferenceV1, ExecutionNormalizationContextReferenceV1,
    ExecutionNormalizationContextV1, ImmutableSourceReferenceV1, SimV2ContextSourceV1,
    SimV2SourceRecordReferenceV1, SimV2SourceReferenceV1, SimV2SourceWatermarkV1,
    SourceNeutralAccountIdentityV1, SourceNeutralEconomicIdentityV1,
};
use contracts::{AccountInstanceKeyV1, ExactDecimal, PlatformSourceV1, SimCallerRoleV1};
use execution_normalization::{
    DurableCommitOutcomeV1, DurableContextStoreV1, DurableErrorV1, NormalizationBatchInputV1,
    NormalizationRecordV1, SourceNeutralEconomicEvidenceV1, SourceNeutralEvidenceKindV1,
    SourceNeutralEvidenceV1, SourceNeutralLogicalKeyV1, SubmissionDispositionV1,
};
use tempfile::TempDir;

fn account_identity() -> SourceNeutralAccountIdentityV1 {
    SourceNeutralAccountIdentityV1 {
        source_category: "platform".to_owned(),
        environment: "simulation".to_owned(),
        run_scope: "run-55-durable".to_owned(),
        account_scope: "account-55-durable".to_owned(),
    }
}

fn context() -> ExecutionNormalizationContextV1 {
    ExecutionNormalizationContextV1 {
        context_reference: ExecutionNormalizationContextReferenceV1(
            "context-55-durable".to_owned(),
        ),
        account_identity: account_identity(),
        source: SimV2ContextSourceV1 {
            account_instance_key: AccountInstanceKeyV1 {
                run_id: "run-55-durable".to_owned(),
                caller_role: SimCallerRoleV1::ModelTrain,
                caller_instance_id: "caller-55".to_owned(),
                account_id: "account-55-durable".to_owned(),
                platform_source: PlatformSourceV1::Simulated,
            },
            run_opened: DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Location(
                    "artifact://sim/run-55-durable/opened".to_owned(),
                ),
            },
            account_opening_evidence_identity: "opening-55-durable".to_owned(),
            account_opening: DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Location(
                    "artifact://sim/run-55-durable/account-opening".to_owned(),
                ),
            },
        },
        contract_version: "execution_normalization_v1".to_owned(),
        implementation_identity: "noe-stage-1-durable-test".to_owned(),
    }
}

fn watermark(sequence: u64) -> SimV2SourceWatermarkV1 {
    SimV2SourceWatermarkV1 {
        run_id: "run-55-durable".to_owned(),
        projection_batch_sequence: sequence,
    }
}

fn batch(sequence: u64) -> NormalizationBatchInputV1 {
    let source_watermark = watermark(sequence);
    NormalizationBatchInputV1 {
        source_watermark: source_watermark.clone(),
        evidence: vec![SourceNeutralEvidenceV1 {
            source_reference: SimV2SourceReferenceV1 {
                context_reference: ExecutionNormalizationContextReferenceV1(
                    "context-55-durable".to_owned(),
                ),
                watermark: source_watermark,
                record: SimV2SourceRecordReferenceV1::DirectResponse {
                    private_feedback_sequence: 100 + sequence,
                    platform_responded_at_utc_ms: 2_000 + sequence as i64,
                    response_received_at_utc_ms: 2_100 + sequence as i64,
                    association: contracts::execution_normalization::SimV2DirectResponseAssociationV1::SubmissionItem {
                        global_request_id: format!("request-{sequence}"),
                        batch_item_ordinal: 0,
                    },
                },
            },
            source_ordinal: 0,
            logical_key: SourceNeutralLogicalKeyV1::SubmissionItem {
                global_request_id: format!("request-{sequence}"),
                batch_item_ordinal: 0,
            },
            kind: SourceNeutralEvidenceKindV1::Submission {
                disposition: SubmissionDispositionV1::Accepted,
            },
        }],
    }
}

fn economic_batch(sequence: u64) -> NormalizationBatchInputV1 {
    NormalizationBatchInputV1 {
        source_watermark: watermark(sequence),
        evidence: vec![economic_evidence_record(
            sequence,
            0,
            "opening-economic-55",
            "100",
        )],
    }
}

fn economic_evidence_record(
    sequence: u64,
    source_ordinal: u32,
    evidence_identity: &str,
    available_pusd: &str,
) -> SourceNeutralEvidenceV1 {
    let source_watermark = watermark(sequence);
    SourceNeutralEvidenceV1 {
        source_reference: SimV2SourceReferenceV1 {
            context_reference: ExecutionNormalizationContextReferenceV1(
                "context-55-durable".to_owned(),
            ),
            watermark: source_watermark,
            record: SimV2SourceRecordReferenceV1::AccountOpening,
        },
        source_ordinal,
        logical_key: SourceNeutralLogicalKeyV1::EconomicEvidence {
            evidence_identity: evidence_identity.to_owned(),
        },
        kind: SourceNeutralEvidenceKindV1::Economic {
            evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
                opening_evidence_identity: "opening-55-durable".to_owned(),
                available_pusd: ExactDecimal(available_pusd.to_owned()),
            },
        },
    }
}

fn economic_classification_batch(sequence: u64) -> NormalizationBatchInputV1 {
    NormalizationBatchInputV1 {
        source_watermark: watermark(sequence),
        evidence: vec![
            economic_evidence_record(sequence, 0, "opening-economic-55", "100"),
            economic_evidence_record(sequence, 1, &format!("opening-alias-{sequence}"), "100"),
            economic_evidence_record(sequence, 2, &format!("opening-conflict-{sequence}"), "101"),
        ],
    }
}

fn context_directory(root: &Path) -> PathBuf {
    fs::read_dir(root)
        .unwrap()
        .map(Result::unwrap)
        .find(|entry| entry.file_type().unwrap().is_dir())
        .expect("耐久根应只包含当前上下文目录")
        .path()
}

#[test]
fn results_and_watermark_become_visible_together_and_replay_is_idempotent() {
    let temporary = TempDir::new().unwrap();
    let mut store = DurableContextStoreV1::create(temporary.path(), context()).unwrap();

    let committed = store
        .commit_batch(batch(1), 3_000)
        .expect("首个提交应在真实耐久边界发布");
    assert!(matches!(
        committed,
        DurableCommitOutcomeV1::Committed {
            commit_sequence: 0,
            ..
        }
    ));
    assert_eq!(store.commits().len(), 1);
    assert_eq!(store.commits()[0].records.len(), 1);
    assert_eq!(
        store
            .durable_through()
            .expect("提交结果与水位必须共同可见")
            .projection_batch_sequence,
        1
    );

    let replay = store
        .commit_batch(batch(1), 3_100)
        .expect("已耐久后缀重放应幂等返回现有水位");
    assert!(matches!(
        replay,
        DurableCommitOutcomeV1::AlreadyDurable { .. }
    ));
    assert_eq!(store.commits().len(), 1);

    let reopened = DurableContextStoreV1::open(
        temporary.path(),
        &ExecutionNormalizationContextReferenceV1("context-55-durable".to_owned()),
    )
    .unwrap();
    assert_eq!(reopened.commits(), store.commits());
    assert_eq!(reopened.context(), store.context());
}

#[test]
fn fully_written_candidate_that_cannot_publish_does_not_advance_core_or_watermark() {
    let temporary = TempDir::new().unwrap();
    let mut store = DurableContextStoreV1::create(temporary.path(), context()).unwrap();
    let commit_directory = context_directory(temporary.path()).join("commits");
    let blocking_target = commit_directory.join("00000000000000000000.json");
    fs::create_dir(&blocking_target).unwrap();

    let error = store.commit_batch(batch(1), 3_000).unwrap_err();
    assert!(matches!(
        error,
        DurableErrorV1::CommitAlreadyExists { commit_sequence: 0 }
    ));
    assert!(store.durable_through().is_none());
    assert!(store.commits().is_empty());
    assert_eq!(
        fs::read_dir(&commit_directory)
            .unwrap()
            .map(Result::unwrap)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
            .count(),
        0
    );
    fs::remove_dir(blocking_target).unwrap();
    let committed = store.commit_batch(batch(1), 3_100).unwrap();
    assert!(matches!(
        committed,
        DurableCommitOutcomeV1::Committed {
            commit_sequence: 0,
            ..
        }
    ));
    assert!(matches!(
        store.commits()[0].records[0],
        NormalizationRecordV1::Evidence { .. }
    ));
}

#[test]
fn recovery_discards_unpublished_suffix_and_rejects_a_corrupt_published_boundary() {
    let temporary = TempDir::new().unwrap();
    let mut store = DurableContextStoreV1::create(temporary.path(), context()).unwrap();
    store.commit_batch(batch(1), 3_000).unwrap();

    let commit_directory = context_directory(temporary.path()).join("commits");
    let unpublished_suffix = commit_directory.join(".00000000000000000001.json.tmp");
    fs::write(&unpublished_suffix, b"complete bytes without publication").unwrap();

    let mut recovered = DurableContextStoreV1::open(
        temporary.path(),
        &ExecutionNormalizationContextReferenceV1("context-55-durable".to_owned()),
    )
    .unwrap();
    assert!(!unpublished_suffix.exists());
    recovered.commit_batch(batch(2), 3_100).unwrap();
    assert_eq!(recovered.commits().len(), 2);

    let second_commit = commit_directory.join("00000000000000000001.json");
    let mut bytes = fs::read(&second_commit).unwrap();
    bytes[0] ^= 1;
    fs::write(&second_commit, bytes).unwrap();
    let error = DurableContextStoreV1::open(
        temporary.path(),
        &ExecutionNormalizationContextReferenceV1("context-55-durable".to_owned()),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        DurableErrorV1::ContainerDecode { .. } | DurableErrorV1::BoundaryDigestMismatch { .. }
    ));
}

#[test]
fn reopen_rebuilds_the_owned_core_before_economic_replay() {
    let temporary = TempDir::new().unwrap();
    let mut store = DurableContextStoreV1::create(temporary.path(), context()).unwrap();
    store.commit_batch(economic_batch(1), 3_000).unwrap();
    assert!(matches!(
        store.commits()[0].records[0],
        NormalizationRecordV1::EconomicFact { .. }
    ));

    store
        .commit_batch(economic_classification_batch(2), 3_100)
        .unwrap();
    assert!(matches!(
        store.commits()[1].records.as_slice(),
        [
            NormalizationRecordV1::Duplicate { .. },
            NormalizationRecordV1::EconomicAlias { .. },
            NormalizationRecordV1::Conflict { .. }
        ]
    ));

    let mut reopened = DurableContextStoreV1::open(
        temporary.path(),
        &ExecutionNormalizationContextReferenceV1("context-55-durable".to_owned()),
    )
    .unwrap();
    reopened
        .commit_batch(economic_classification_batch(3), 3_200)
        .unwrap();

    assert!(matches!(
        reopened.commits()[2].records.as_slice(),
        [
            NormalizationRecordV1::Duplicate { .. },
            NormalizationRecordV1::EconomicAlias { .. },
            NormalizationRecordV1::Conflict { .. }
        ]
    ));
    assert_eq!(
        reopened.commits()[0].records[0]
            .economic_fact()
            .unwrap()
            .economic_identity,
        SourceNeutralEconomicIdentityV1::AccountOpening {
            account_identity: account_identity(),
            opening_evidence_identity: "opening-55-durable".to_owned(),
        }
    );
}

#[test]
fn reopen_preserves_the_incoming_minimum_conflict_evidence() {
    let temporary = TempDir::new().unwrap();
    let mut store = DurableContextStoreV1::create(temporary.path(), context()).unwrap();
    let mut input = batch(1);
    let mut conflicting = input.evidence[0].clone();
    conflicting.source_ordinal = 1;
    conflicting.kind = SourceNeutralEvidenceKindV1::Submission {
        disposition: SubmissionDispositionV1::RejectedNoEffect,
    };
    input.evidence.push(conflicting);
    store.commit_batch(input, 3_000).unwrap();
    drop(store);

    let reopened = DurableContextStoreV1::open(
        temporary.path(),
        &ExecutionNormalizationContextReferenceV1("context-55-durable".to_owned()),
    )
    .unwrap();
    assert!(matches!(
        &reopened.commits()[0].records[1],
        NormalizationRecordV1::Conflict {
            conflicting_evidence: SourceNeutralEvidenceKindV1::Submission {
                disposition: SubmissionDispositionV1::RejectedNoEffect,
            },
            ..
        }
    ));
}
