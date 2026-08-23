use std::fs;
use std::path::{Path, PathBuf};

use contracts::execution_normalization::{
    DurableSourceReferenceV1, ExecutionNormalizationContextReferenceV1,
    ExecutionNormalizationContextV1, ImmutableSourceReferenceV1, SimV2ContextSourceV1,
    SimV2SourceRecordReferenceV1, SimV2SourceReferenceV1, SimV2SourceWatermarkV1,
    SourceNeutralAccountIdentityV1,
};
use contracts::{AccountInstanceKeyV1, PlatformSourceV1, SimCallerRoleV1};
use execution_normalization::{
    DurableCommitOutcomeV1, DurableContextStoreV1, DurableErrorV1, NormalizationBatchInputV1,
    NormalizationCoreV1, NormalizationRecordV1, SourceNeutralEvidenceKindV1,
    SourceNeutralEvidenceV1, SourceNeutralLogicalKeyV1, SourceOrderV1, SubmissionDispositionV1,
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
            source_order: SourceOrderV1 {
                sequence: 100 + sequence,
                ordinal: 0,
            },
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
    let mut core = NormalizationCoreV1::new(context());
    let mut store = DurableContextStoreV1::create(temporary.path(), context()).unwrap();

    let committed = store
        .commit_batch(&mut core, batch(1), 3_000)
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
        .commit_batch(&mut core, batch(1), 3_100)
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
    let mut core = NormalizationCoreV1::new(context());
    let mut store = DurableContextStoreV1::create(temporary.path(), context()).unwrap();
    let commit_directory = context_directory(temporary.path()).join("commits");
    let blocking_target = commit_directory.join("00000000000000000000.json");
    fs::create_dir(&blocking_target).unwrap();

    let error = store.commit_batch(&mut core, batch(1), 3_000).unwrap_err();
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
    let retry_preview = core.prepare_batch(batch(1)).unwrap();
    assert!(matches!(
        retry_preview.batch().records[0],
        NormalizationRecordV1::Evidence { .. }
    ));

    fs::remove_dir(blocking_target).unwrap();
    let committed = store.commit_batch(&mut core, batch(1), 3_100).unwrap();
    assert!(matches!(
        committed,
        DurableCommitOutcomeV1::Committed {
            commit_sequence: 0,
            ..
        }
    ));
}

#[test]
fn recovery_discards_unpublished_suffix_and_rejects_a_corrupt_published_boundary() {
    let temporary = TempDir::new().unwrap();
    let mut core = NormalizationCoreV1::new(context());
    let mut store = DurableContextStoreV1::create(temporary.path(), context()).unwrap();
    store.commit_batch(&mut core, batch(1), 3_000).unwrap();

    let commit_directory = context_directory(temporary.path()).join("commits");
    let unpublished_suffix = commit_directory.join(".00000000000000000001.json.tmp");
    fs::write(&unpublished_suffix, b"complete bytes without publication").unwrap();

    let mut recovered = DurableContextStoreV1::open(
        temporary.path(),
        &ExecutionNormalizationContextReferenceV1("context-55-durable".to_owned()),
    )
    .unwrap();
    assert!(!unpublished_suffix.exists());
    let mut recovered_core = NormalizationCoreV1::restore(
        recovered.context().clone(),
        recovered
            .commits()
            .iter()
            .flat_map(|commit| commit.records.iter()),
    );
    recovered
        .commit_batch(&mut recovered_core, batch(2), 3_100)
        .unwrap();
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
