use std::fs;

use contracts::execution_normalization::{
    DurableSourceReferenceV1, ImmutableSourceReferenceV1, SimV2SourceRecordReferenceV1,
    SimV2SourceWatermarkV1,
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

fn batch() -> NormalizationBatchInputV1 {
    let watermark = SimV2SourceWatermarkV1 {
        run_id: "run-result-58".to_owned(),
        projection_batch_sequence: 1,
    };
    NormalizationBatchInputV1 {
        source_watermark: watermark,
        evidence: (0..2)
            .map(|ordinal| SourceNeutralEvidenceV1 {
                source_record: SimV2SourceRecordReferenceV1::OrderLifecycle {
                    private_feedback_sequence: 1,
                    platform_event_at_utc_ms: 10,
                    feedback_received_at_utc_ms: 20,
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
    let input = batch();
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
    let input = batch();
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
        let mut input = batch();
        input.source_watermark.projection_batch_sequence = sequence;
        let input = NormalizationBatchInputV3 {
            source_watermark: input.source_watermark.clone(),
            evidence: input
                .evidence
                .into_iter()
                .map(|e| SourceNeutralEvidenceV3 {
                    source_record: e.source_record,
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

#[test]
fn cold_v3_query_keeps_embedded_account_and_economic_decisions_after_reopen() {
    use contracts::execution_normalization::*;
    use contracts::{ConditionId, ExactDecimal, TokenId};
    use execution_normalization::*;
    let temporary = TempDir::new().unwrap();
    let mut store = DurableContextStoreV3::open_or_create_sim_v2(temporary.path(), seed()).unwrap();
    // 被读取的身份使用其编码内的账户，不能从当前容器上下文重建。
    let mut other_seed = seed();
    other_seed.account_instance_key.account_id = "another-account".into();
    let other_account = std::sync::Arc::new(
        sim_v2_context(other_seed, &store.context().contract_version)
            .unwrap()
            .account_identity,
    );
    let order_key = SourceNeutralEntityKeyV1::Order {
        source_order_id: "original-order".into(),
    };
    let order_identity = StableLogicalIdentityV1::entity(&other_account, &order_key).unwrap();
    let query = SourceNeutralEvidenceKindV3::Control {
        result: SourceNeutralControlResult::OrdersQuery {
            result: SourceNeutralOrdersQueryResult::Page(SourceNeutralOrdersPage {
                root_sequence: 7,
                items: vec![SourceNeutralOrderSnapshotV2 {
                    order_identity,
                    condition_id: ConditionId("condition".into()),
                    token_id: TokenId("token".into()),
                    state: SourceNeutralOrderStateV1::Open,
                    quantities: SourceNeutralOrderQuantityEvidenceV1::Base {
                        matched_base_quantity_shares: ExactDecimal("2.5".into()),
                        active_unfilled_base_quantity_shares: ExactDecimal("7.5".into()),
                    },
                }],
                next_cursor: Some(SourceNeutralQueryCursorV1 {
                    root_sequence: 7,
                    next_offset: 1,
                }),
            }),
        },
    };
    let opening = SourceNeutralEvidenceKindV3::Economic {
        evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
            opening_evidence_identity: "opening-result-58".into(),
            available_pusd: ExactDecimal("100".into()),
        },
    };
    let make = |ordinal: u32, key: &str, kind| SourceNeutralEvidenceV3 {
        source_record: batch().evidence.remove(0).source_record,
        source_ordinal: ordinal,
        logical_key: SourceNeutralLogicalKeyV1::AuditEvidence {
            evidence_identity: key.into(),
        },
        kind,
    };
    let input = NormalizationBatchInputV3 {
        source_watermark: batch().source_watermark,
        evidence: vec![
            make(0, "query", query.clone()),
            make(1, "query", query),
            make(
                2,
                "query",
                SourceNeutralEvidenceKindV3::Control {
                    result: SourceNeutralControlResult::OrdersQuery {
                        result: SourceNeutralOrdersQueryResult::InvalidCursor,
                    },
                },
            ),
            make(3, "economic", opening.clone()),
            make(4, "economic-alias", opening),
            make(
                5,
                "pending",
                SourceNeutralEvidenceKindV3::Pending {
                    scope: PendingScopeV1::EvidenceGap {
                        scope_identity: "gap".into(),
                    },
                },
            ),
        ],
    };
    store.commit_batch_v3(input, 30).unwrap();
    let refs: Vec<_> = store.commits()[0].result_references().collect();
    drop(store);
    let _reopened = DurableContextStoreV3::open_or_create_sim_v2(temporary.path(), seed()).unwrap();
    let views: Vec<_> = refs
        .iter()
        .map(|r| read_durable_noe_record_v1(r).unwrap())
        .collect();
    let NormalizationRecord::Evidence {
        evidence:
            SourceNeutralEvidenceKind::Control {
                result:
                    SourceNeutralControlResult::OrdersQuery {
                        result: SourceNeutralOrdersQueryResult::Page(page),
                    },
            },
        ..
    } = views[0].record()
    else {
        panic!("原查询页")
    };
    assert_eq!(page.root_sequence, 7);
    assert_eq!(page.next_cursor.as_ref().unwrap().next_offset, 1);
    assert_eq!(page.items[0].order_identity.account, other_account);
    assert_eq!(
        page.items[0].order_identity.key,
        MemoryIdentityKeyV1::Entity(order_key)
    );
    assert!(
        matches!(&page.items[0].quantities, SourceNeutralOrderQuantityEvidenceV1::Base { active_unfilled_base_quantity_shares, .. } if active_unfilled_base_quantity_shares.0 == "7.5")
    );
    assert!(matches!(
        views[1].record(),
        NormalizationRecord::Duplicate { .. }
    ));
    assert!(matches!(
        views[2].record(),
        NormalizationRecord::Conflict {
            scope: NormalizationConflictScopeV1::LogicalIdentity { .. },
            conflicting_evidence: SourceNeutralEvidenceKind::Control {
                result: SourceNeutralControlResult::OrdersQuery {
                    result: SourceNeutralOrdersQueryResult::InvalidCursor
                }
            },
            ..
        }
    ));
    let NormalizationRecord::EconomicFact { fact, .. } = views[3].record() else {
        panic!("首次资金事实")
    };
    assert!(
        matches!(&fact.disposition, SourceNeutralEconomicDispositionV1::AccountOpening { available_pusd } if available_pusd.0 == "100")
    );
    let NormalizationRecord::EconomicAlias {
        primary_logical_identity,
        fact_reference,
        ..
    } = views[4].record()
    else {
        panic!("同一资金事实别名")
    };
    assert_eq!(primary_logical_identity, &fact.logical_identity);
    assert_eq!(fact_reference, &fact.fact_reference);
    assert!(
        matches!(views[5].record(), NormalizationRecord::Pending { scope: NormalizationPendingScopeV1::EvidenceGap { scope_identity }, .. } if scope_identity == "gap")
    );
}
