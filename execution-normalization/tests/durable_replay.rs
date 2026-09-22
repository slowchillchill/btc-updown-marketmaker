use std::fs;
use std::path::{Path, PathBuf};

use contracts::execution_normalization::{
    DurableSourceReferenceV1, ExecutionNormalizationContextReferenceV1,
    ExecutionNormalizationContextV1, ImmutableSourceReferenceV1, SimV2ContextSourceV1,
    SimV2SourceRecordReferenceV1, SimV2SourceWatermarkV1, SourceNeutralAccountIdentityV1,
    SourceNeutralControlResultV1, SourceNeutralEconomicIdentityV1,
    SourceNeutralSubmissionOutcomeV1,
};
use contracts::{AccountInstanceKeyV1, ExactDecimal, PlatformSourceV1, SimCallerRoleV1};
use execution_normalization::{
    DurableCommitOutcomeV1, DurableContextStoreV1, DurableErrorV1, NormalizationBatchInputV1,
    NormalizationConflictScopeV1, NormalizationRecordV1, SourceNeutralEconomicEvidenceV1,
    SourceNeutralEconomicKeyV1, SourceNeutralEvidenceKindV1, SourceNeutralEvidenceV1,
    SourceNeutralLogicalKeyV1,
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
        source_watermark,
        evidence: vec![SourceNeutralEvidenceV1 {
            source_record: SimV2SourceRecordReferenceV1::DirectResponse {
                    private_feedback_sequence: 100 + sequence,
                    platform_responded_at_utc_ms: 2_000 + sequence as i64,
                    response_received_at_utc_ms: 2_100 + sequence as i64,
                    association: contracts::execution_normalization::SimV2DirectResponseAssociationV1::SubmissionItem {
                        global_request_id: format!("request-{sequence}"),
                        batch_item_ordinal: 0,
                    },
                },
            source_ordinal: 0,
            logical_key: SourceNeutralLogicalKeyV1::SubmissionItem {
                global_request_id: format!("request-{sequence}"),
                batch_item_ordinal: 0,
            },
            kind: SourceNeutralEvidenceKindV1::Control {
                result: SourceNeutralControlResultV1::Submission {
                    outcome: SourceNeutralSubmissionOutcomeV1::RejectedNoEffect,
                },
            },
        }],
    }
}

fn economic_batch(sequence: u64) -> NormalizationBatchInputV1 {
    NormalizationBatchInputV1 {
        source_watermark: watermark(sequence),
        evidence: vec![economic_evidence_record(0, "opening-economic-55", "100")],
    }
}

fn economic_evidence_record(
    source_ordinal: u32,
    evidence_identity: &str,
    available_pusd: &str,
) -> SourceNeutralEvidenceV1 {
    opening_economic_evidence_record(
        source_ordinal,
        evidence_identity,
        "opening-55-durable",
        available_pusd,
    )
}

fn opening_economic_evidence_record(
    source_ordinal: u32,
    evidence_identity: &str,
    opening_evidence_identity: &str,
    available_pusd: &str,
) -> SourceNeutralEvidenceV1 {
    SourceNeutralEvidenceV1 {
        source_record: SimV2SourceRecordReferenceV1::AccountOpening,
        source_ordinal,
        logical_key: SourceNeutralLogicalKeyV1::EconomicEvidence {
            evidence_identity: evidence_identity.to_owned(),
        },
        kind: SourceNeutralEvidenceKindV1::Economic {
            evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
                opening_evidence_identity: opening_evidence_identity.to_owned(),
                available_pusd: ExactDecimal(available_pusd.to_owned()),
            },
        },
    }
}

fn late_opening_coverage_record(
    sequence: u64,
    source_ordinal: u32,
    evidence_identity: &str,
    opening_evidence_identity: &str,
) -> SourceNeutralEvidenceV1 {
    SourceNeutralEvidenceV1 {
        source_record: SimV2SourceRecordReferenceV1::OrderLifecycle {
            private_feedback_sequence: 500 + sequence,
            platform_event_at_utc_ms: 2_500 + sequence as i64,
            feedback_received_at_utc_ms: 2_600 + sequence as i64,
        },
        source_ordinal,
        logical_key: SourceNeutralLogicalKeyV1::AuditEvidence {
            evidence_identity: evidence_identity.to_owned(),
        },
        kind: SourceNeutralEvidenceKindV1::LateEconomicCoverage {
            economic_key: SourceNeutralEconomicKeyV1::AccountOpening {
                opening_evidence_identity: opening_evidence_identity.to_owned(),
            },
        },
    }
}

fn economic_classification_batch(sequence: u64) -> NormalizationBatchInputV1 {
    NormalizationBatchInputV1 {
        source_watermark: watermark(sequence),
        evidence: vec![
            economic_evidence_record(0, "opening-economic-55", "100"),
            economic_evidence_record(1, &format!("opening-alias-{sequence}"), "100"),
            economic_evidence_record(2, &format!("opening-conflict-{sequence}"), "101"),
        ],
    }
}

fn normalization_batch(
    sequence: u64,
    evidence: Vec<SourceNeutralEvidenceV1>,
) -> NormalizationBatchInputV1 {
    NormalizationBatchInputV1 {
        source_watermark: watermark(sequence),
        evidence,
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
fn reopen_restores_alias_and_economic_conflict_logical_decisions() {
    let temporary = TempDir::new().unwrap();
    let mut store = DurableContextStoreV1::create(temporary.path(), context()).unwrap();
    store
        .commit_batch(
            normalization_batch(
                1,
                vec![opening_economic_evidence_record(0, "A", "E1", "100")],
            ),
            3_000,
        )
        .unwrap();
    store
        .commit_batch(
            normalization_batch(
                2,
                vec![
                    opening_economic_evidence_record(0, "B", "E1", "100"),
                    opening_economic_evidence_record(1, "C", "E1", "101"),
                    late_opening_coverage_record(2, 0, "L", "E1"),
                ],
            ),
            3_100,
        )
        .unwrap();
    assert!(matches!(
        store.commits()[1].records.as_slice(),
        [
            NormalizationRecordV1::EconomicAlias { .. },
            NormalizationRecordV1::Conflict {
                scope: NormalizationConflictScopeV1::EconomicIdentity { .. },
                ..
            },
            NormalizationRecordV1::EconomicAlias { .. },
        ]
    ));
    let economic_alias = serde_json::to_value(&store.commits()[1].records[0]).unwrap();
    assert_eq!(
        economic_alias["EconomicAlias"]["decision"],
        "EconomicEvidence"
    );
    assert!(economic_alias["EconomicAlias"].get("evidence").is_none());
    let late_alias = serde_json::to_value(&store.commits()[1].records[2]).unwrap();
    assert_eq!(
        late_alias["EconomicAlias"]["decision"],
        "LateEconomicCoverage"
    );
    assert!(late_alias["EconomicAlias"].get("evidence").is_none());
    drop(store);

    let mut reopened = DurableContextStoreV1::open(
        temporary.path(),
        &ExecutionNormalizationContextReferenceV1("context-55-durable".to_owned()),
    )
    .unwrap();
    reopened
        .commit_batch(
            normalization_batch(
                3,
                vec![
                    opening_economic_evidence_record(0, "B", "E1", "100"),
                    opening_economic_evidence_record(1, "B", "E2", "200"),
                    opening_economic_evidence_record(2, "C", "E1", "101"),
                    opening_economic_evidence_record(3, "C", "E2", "200"),
                    late_opening_coverage_record(3, 0, "L", "E1"),
                ],
            ),
            3_200,
        )
        .unwrap();

    assert!(matches!(
        reopened.commits()[2].records.as_slice(),
        [
            NormalizationRecordV1::Duplicate { .. },
            NormalizationRecordV1::Conflict {
                scope: NormalizationConflictScopeV1::LogicalIdentity { .. },
                ..
            },
            NormalizationRecordV1::Duplicate { .. },
            NormalizationRecordV1::Conflict {
                scope: NormalizationConflictScopeV1::LogicalIdentity { .. },
                ..
            },
            NormalizationRecordV1::EconomicAlias { .. },
        ]
    ));
    assert!(
        reopened.commits()[2]
            .records
            .iter()
            .all(|record| record.economic_fact().is_none())
    );
    reopened
        .commit_batch(
            normalization_batch(
                4,
                vec![opening_economic_evidence_record(0, "A", "E1", "100")],
            ),
            3_300,
        )
        .unwrap();
    assert!(matches!(
        reopened.commits()[3].records.as_slice(),
        [NormalizationRecordV1::Duplicate { .. }]
    ));
}

#[test]
fn reopen_preserves_the_incoming_minimum_conflict_evidence() {
    let temporary = TempDir::new().unwrap();
    let mut store = DurableContextStoreV1::create(temporary.path(), context()).unwrap();
    let mut input = batch(1);
    let mut conflicting = input.evidence[0].clone();
    conflicting.source_ordinal = 1;
    conflicting.kind = SourceNeutralEvidenceKindV1::Control {
        result: SourceNeutralControlResultV1::Submission {
            outcome: SourceNeutralSubmissionOutcomeV1::EvidenceUnknownNoEffect,
        },
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
            conflicting_evidence: SourceNeutralEvidenceKindV1::Control {
                result: SourceNeutralControlResultV1::Submission {
                    outcome: SourceNeutralSubmissionOutcomeV1::EvidenceUnknownNoEffect,
                },
            },
            ..
        }
    ));
}

#[test]
fn reopened_cancellation_keeps_reference_presence_order_and_content() {
    use contracts::execution_normalization::{
        SimV2DirectResponseAssociationV1 as Association, SourceNeutralCancellationOutcomeV1,
        StableLogicalIdentityV1,
    };
    use contracts::{CancelInputItemRefV2, ExecutionMethodV2};
    let item = |id: &str, ordinal| CancelInputItemRefV2 {
        item_id: id.into(),
        batch_item_ordinal: ordinal,
    };
    let original = vec![item("a", 0), item("b", 1)];
    let evidence = |sequence, target_ordinal, refs: Option<Vec<CancelInputItemRefV2>>| {
        SourceNeutralEvidenceV1 {
            source_record: SimV2SourceRecordReferenceV1::DirectResponse {
                private_feedback_sequence: sequence,
                platform_responded_at_utc_ms: 2000,
                response_received_at_utc_ms: 2100,
                association: match refs {
                    Some(input_item_refs) => Association::CancellationTarget {
                        global_request_id: "cancel".into(),
                        method: ExecutionMethodV2::CancelOrders,
                        target_ordinal,
                        input_item_refs,
                    },
                    None => Association::Request {
                        global_request_id: "cancel".into(),
                    },
                },
            },
            source_ordinal: 0,
            logical_key: SourceNeutralLogicalKeyV1::CancellationTarget {
                global_request_id: "cancel".into(),
                target_ordinal,
            },
            kind: SourceNeutralEvidenceKindV1::Control {
                result: SourceNeutralControlResultV1::Cancellation {
                    order_identity: StableLogicalIdentityV1("order".into()),
                    outcome: SourceNeutralCancellationOutcomeV1::NotCanceledNoEffect,
                },
            },
        }
    };
    let temporary = TempDir::new().unwrap();
    let mut store = DurableContextStoreV1::create(temporary.path(), context()).unwrap();
    store
        .commit_batch(
            NormalizationBatchInputV1 {
                source_watermark: watermark(1),
                evidence: vec![
                    evidence(1, 0, Some(original.clone())),
                    evidence(2, 1, Some(vec![])),
                ],
            },
            3000,
        )
        .unwrap();
    drop(store);
    let mut reopened =
        DurableContextStoreV1::open(temporary.path(), &context().context_reference).unwrap();
    reopened
        .commit_batch(
            NormalizationBatchInputV1 {
                source_watermark: watermark(2),
                evidence: vec![
                    evidence(2, 0, Some(original)),
                    evidence(3, 0, Some(vec![item("b", 1), item("a", 0)])),
                    evidence(4, 0, Some(vec![])),
                    evidence(5, 0, None),
                    evidence(6, 0, Some(vec![item("a", 0), item("changed", 1)])),
                    evidence(7, 1, None),
                    evidence(8, 1, Some(vec![])),
                ],
            },
            3001,
        )
        .unwrap();
    let records = &reopened.commits()[1].records;
    assert!(matches!(
        records[0],
        NormalizationRecordV1::Duplicate { .. }
    ));
    assert!(matches!(
        records[6],
        NormalizationRecordV1::Duplicate { .. }
    ));
    assert!(
        records[1..6]
            .iter()
            .all(|record| matches!(record, NormalizationRecordV1::Conflict { .. }))
    );
}
