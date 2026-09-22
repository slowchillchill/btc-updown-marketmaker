use contracts::execution_normalization::{
    DurableSourceReferenceV1, ExecutionNormalizationContextReferenceV1,
    ExecutionNormalizationContextV1, ImmutableSourceReferenceV1, SimV2ContextSourceV1,
    SimV2DirectResponseAssociationV1, SimV2SourceRecordReferenceV1, SimV2SourceWatermarkV1,
    SourceNeutralAccountBalanceDeltaV1, SourceNeutralAccountIdentityV1,
    SourceNeutralAccountTransitionKindV1, SourceNeutralCancellationOutcomeV1,
    SourceNeutralControlResultV1, SourceNeutralEconomicIdentityV1,
    SourceNeutralOrderLifecycleEventV1, SourceNeutralOrderLifecycleResultV1,
    SourceNeutralOrderStateV1, SourceNeutralSubmissionOutcomeV1, StableLogicalIdentityV1,
};
use contracts::{
    AccountInstanceKeyV1, CancelInputItemRefV2, ConditionId, ExactDecimal, ExecutionMethodV2,
    PlatformSourceV1, SimCallerRoleV1, TokenId,
};
use execution_normalization::{
    NormalizationBatchInputV1, NormalizationConflictScopeV1, NormalizationCoreV1,
    NormalizationPendingScopeV1, NormalizationRecordV1, PendingScopeV1,
    SourceNeutralEconomicEvidenceV1, SourceNeutralEconomicKeyV1, SourceNeutralEvidenceKindV1,
    SourceNeutralEvidenceV1, SourceNeutralLogicalKeyV1, SourceSequenceV1,
};

fn account_identity() -> SourceNeutralAccountIdentityV1 {
    SourceNeutralAccountIdentityV1 {
        source_category: "platform".to_owned(),
        environment: "simulation".to_owned(),
        run_scope: "run-55".to_owned(),
        account_scope: "account-55".to_owned(),
    }
}

fn context() -> ExecutionNormalizationContextV1 {
    ExecutionNormalizationContextV1 {
        context_reference: ExecutionNormalizationContextReferenceV1("context-55".to_owned()),
        account_identity: account_identity(),
        source: SimV2ContextSourceV1 {
            account_instance_key: AccountInstanceKeyV1 {
                run_id: "run-55".to_owned(),
                caller_role: SimCallerRoleV1::ModelTrain,
                caller_instance_id: "caller-55".to_owned(),
                account_id: "account-55".to_owned(),
                platform_source: PlatformSourceV1::Simulated,
            },
            run_opened: DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Location(
                    "artifact://sim/run-55/opened".to_owned(),
                ),
            },
            account_opening_evidence_identity: "opening-55".to_owned(),
            account_opening: DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Location(
                    "artifact://sim/run-55/account-opening".to_owned(),
                ),
            },
        },
        contract_version: "execution_normalization_v1".to_owned(),
        implementation_identity: "noe-stage-1-test".to_owned(),
    }
}

fn watermark(sequence: u64) -> SimV2SourceWatermarkV1 {
    SimV2SourceWatermarkV1 {
        run_id: "run-55".to_owned(),
        projection_batch_sequence: sequence,
    }
}

fn source_record(private_feedback_sequence: u64) -> SimV2SourceRecordReferenceV1 {
    SimV2SourceRecordReferenceV1::OrderLifecycle {
        private_feedback_sequence,
        platform_event_at_utc_ms: 1_000 + private_feedback_sequence as i64,
        feedback_received_at_utc_ms: 1_100 + private_feedback_sequence as i64,
    }
}

fn evidence(
    source_sequence: u64,
    ordinal: u32,
    logical_key: SourceNeutralLogicalKeyV1,
    kind: SourceNeutralEvidenceKindV1,
) -> SourceNeutralEvidenceV1 {
    let mut source_record = source_record(source_sequence);
    if let SourceNeutralLogicalKeyV1::SubmissionItem {
        global_request_id,
        batch_item_ordinal,
    } = &logical_key
    {
        source_record = SimV2SourceRecordReferenceV1::DirectResponse {
            private_feedback_sequence: source_sequence,
            platform_responded_at_utc_ms: 1_000 + source_sequence as i64,
            response_received_at_utc_ms: 1_100 + source_sequence as i64,
            association: SimV2DirectResponseAssociationV1::SubmissionItem {
                global_request_id: global_request_id.clone(),
                batch_item_ordinal: *batch_item_ordinal,
            },
        };
    }
    SourceNeutralEvidenceV1 {
        source_record,
        source_ordinal: ordinal,
        logical_key,
        kind,
    }
}

fn economic_evidence(
    account_transition_seq: u64,
    available_delta: &str,
) -> SourceNeutralEconomicEvidenceV1 {
    SourceNeutralEconomicEvidenceV1::AccountTransition {
        account_transition_seq,
        economic_effective_at_utc_ms: 2_000,
        condition_id: None,
        transition: SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity: StableLogicalIdentityV1("order-55".to_owned()),
        },
        balance_delta: Box::new(SourceNeutralAccountBalanceDeltaV1 {
            cash_pusd_delta: ExactDecimal("0".to_owned()),
            available_pusd_delta: ExactDecimal(available_delta.to_owned()),
            principal_encumbrance_pusd_delta: ExactDecimal("10".to_owned()),
            immediate_taker_fee_encumbrance_pusd_delta: ExactDecimal("0".to_owned()),
            outcome_balance_deltas: vec![
                contracts::execution_normalization::SourceNeutralOutcomeBalanceDeltaV1 {
                    token_id: TokenId("token-up-55".to_owned()),
                    balance_shares_delta: ExactDecimal("0".to_owned()),
                },
            ],
            maker_rebate_receivable_pusd_delta: ExactDecimal("0".to_owned()),
            taker_rebate_receivable_pusd_delta: ExactDecimal("0".to_owned()),
            maker_rebate_locked_remainder_pusd_delta: ExactDecimal("0".to_owned()),
            taker_rebate_locked_remainder_pusd_delta: ExactDecimal("0".to_owned()),
        }),
    }
}

fn submission(outcome: SourceNeutralSubmissionOutcomeV1) -> SourceNeutralEvidenceKindV1 {
    SourceNeutralEvidenceKindV1::Control {
        result: SourceNeutralControlResultV1::Submission { outcome },
    }
}

fn order_lifecycle(event: SourceNeutralOrderLifecycleEventV1) -> SourceNeutralEvidenceKindV1 {
    SourceNeutralEvidenceKindV1::OrderLifecycle {
        result: SourceNeutralOrderLifecycleResultV1 {
            order_identity: StableLogicalIdentityV1("order-55".to_owned()),
            condition_id: ConditionId("condition-55".to_owned()),
            token_id: TokenId("token-up-55".to_owned()),
            event,
        },
    }
}

#[test]
fn feedback_sorting_preserves_submission_layers_partial_fill_and_zero_effects() {
    let core = NormalizationCoreV1::new(context());
    let prepared = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(0),
            evidence: vec![
                evidence(
                    14,
                    0,
                    SourceNeutralLogicalKeyV1::OrderLifecycle {
                        order_identity: "order-55".to_owned(),
                        occurrence: 14,
                    },
                    order_lifecycle(SourceNeutralOrderLifecycleEventV1::CallerCanceled),
                ),
                evidence(
                    10,
                    0,
                    SourceNeutralLogicalKeyV1::SubmissionItem {
                        global_request_id: "request-55".to_owned(),
                        batch_item_ordinal: 0,
                    },
                    submission(SourceNeutralSubmissionOutcomeV1::EvidenceUnknownNoEffect),
                ),
                evidence(
                    13,
                    0,
                    SourceNeutralLogicalKeyV1::OrderLifecycle {
                        order_identity: "order-55".to_owned(),
                        occurrence: 13,
                    },
                    order_lifecycle(SourceNeutralOrderLifecycleEventV1::VenueDustTerminal),
                ),
                evidence(
                    12,
                    0,
                    SourceNeutralLogicalKeyV1::OrderLifecycle {
                        order_identity: "order-55".to_owned(),
                        occurrence: 12,
                    },
                    order_lifecycle(SourceNeutralOrderLifecycleEventV1::Accepted {
                        state: SourceNeutralOrderStateV1::Matched,
                    }),
                ),
                evidence(
                    11,
                    0,
                    SourceNeutralLogicalKeyV1::OrderLifecycle {
                        order_identity: "order-55".to_owned(),
                        occurrence: 11,
                    },
                    order_lifecycle(SourceNeutralOrderLifecycleEventV1::Accepted {
                        state: SourceNeutralOrderStateV1::Open,
                    }),
                ),
            ],
        })
        .expect("来源中立核心应接受有效的代表性证据");

    let sequences: Vec<_> = prepared
        .batch()
        .records
        .iter()
        .map(|record| record.source_order().sequence.clone())
        .collect();
    assert_eq!(
        sequences,
        vec![10, 11, 12, 13, 14]
            .into_iter()
            .map(
                |private_feedback_sequence| SourceSequenceV1::PrivateFeedback {
                    private_feedback_sequence,
                }
            )
            .collect::<Vec<_>>()
    );
    assert!(matches!(
        &prepared.batch().records[0],
        NormalizationRecordV1::Evidence {
            evidence: SourceNeutralEvidenceKindV1::Control {
                result: SourceNeutralControlResultV1::Submission {
                    outcome: SourceNeutralSubmissionOutcomeV1::EvidenceUnknownNoEffect
                }
            },
            ..
        }
    ));
    assert!(matches!(
        &prepared.batch().records[2],
        NormalizationRecordV1::Evidence {
            evidence: SourceNeutralEvidenceKindV1::OrderLifecycle {
                result: SourceNeutralOrderLifecycleResultV1 {
                    event: SourceNeutralOrderLifecycleEventV1::Accepted {
                        state: SourceNeutralOrderStateV1::Matched
                    },
                    ..
                }
            },
            ..
        }
    ));
    assert!(matches!(
        &prepared.batch().records[3],
        NormalizationRecordV1::Evidence {
            evidence: SourceNeutralEvidenceKindV1::OrderLifecycle {
                result: SourceNeutralOrderLifecycleResultV1 {
                    event: SourceNeutralOrderLifecycleEventV1::VenueDustTerminal,
                    ..
                }
            },
            ..
        }
    ));
    assert!(matches!(
        &prepared.batch().records[4],
        NormalizationRecordV1::Evidence {
            evidence: SourceNeutralEvidenceKindV1::OrderLifecycle {
                result: SourceNeutralOrderLifecycleResultV1 {
                    event: SourceNeutralOrderLifecycleEventV1::CallerCanceled,
                    ..
                }
            },
            ..
        }
    ));
    assert_eq!(
        prepared
            .batch()
            .records
            .iter()
            .filter(|record| record.economic_fact().is_some())
            .count(),
        0
    );
}

#[test]
fn stable_logical_identity_is_deterministic_idempotent_and_conflict_scoped() {
    let core = NormalizationCoreV1::new(context());
    let logical_key = SourceNeutralLogicalKeyV1::SubmissionItem {
        global_request_id: "request-idempotent-55".to_owned(),
        batch_item_ordinal: 7,
    };
    let prepared = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(0),
            evidence: vec![
                evidence(
                    20,
                    0,
                    logical_key.clone(),
                    submission(SourceNeutralSubmissionOutcomeV1::RejectedNoEffect),
                ),
                evidence(
                    21,
                    0,
                    logical_key.clone(),
                    submission(SourceNeutralSubmissionOutcomeV1::RejectedNoEffect),
                ),
                evidence(
                    22,
                    0,
                    logical_key,
                    submission(SourceNeutralSubmissionOutcomeV1::EvidenceUnknownNoEffect),
                ),
            ],
        })
        .unwrap();
    let first_identity = prepared.batch().records[0].logical_identity().clone();
    assert_eq!(
        prepared.batch().records[1].logical_identity(),
        &first_identity
    );
    assert!(matches!(
        prepared.batch().records[1],
        NormalizationRecordV1::Duplicate { .. }
    ));
    assert!(matches!(
        &prepared.batch().records[2],
        NormalizationRecordV1::Conflict {
            scope: NormalizationConflictScopeV1::LogicalIdentity { logical_identity },
            ..
        } if logical_identity == &first_identity
    ));
}

#[test]
fn economic_identity_is_applied_once_and_late_coverage_becomes_alias() {
    let core = NormalizationCoreV1::new(context());
    let economic_key = SourceNeutralEconomicKeyV1::AccountTransition {
        account_transition_seq: 41,
    };
    let economic_identity = SourceNeutralEconomicIdentityV1::AccountTransition {
        account_identity: account_identity(),
        account_transition_seq: 41,
    };
    let prepared = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(0),
            evidence: vec![
                evidence(
                    30,
                    0,
                    SourceNeutralLogicalKeyV1::EconomicEvidence {
                        evidence_identity: "account-delta-41".to_owned(),
                    },
                    SourceNeutralEvidenceKindV1::Economic {
                        evidence: economic_evidence(41, "-10"),
                    },
                ),
                evidence(
                    31,
                    0,
                    SourceNeutralLogicalKeyV1::EconomicEvidence {
                        evidence_identity: "late-account-delta-41".to_owned(),
                    },
                    SourceNeutralEvidenceKindV1::Economic {
                        evidence: economic_evidence(41, "-10"),
                    },
                ),
                evidence(
                    32,
                    0,
                    SourceNeutralLogicalKeyV1::AuditEvidence {
                        evidence_identity: "late-order-confirmation-41".to_owned(),
                    },
                    SourceNeutralEvidenceKindV1::LateEconomicCoverage {
                        economic_key: economic_key.clone(),
                    },
                ),
                evidence(
                    33,
                    0,
                    SourceNeutralLogicalKeyV1::EconomicEvidence {
                        evidence_identity: "conflicting-account-delta-41".to_owned(),
                    },
                    SourceNeutralEvidenceKindV1::Economic {
                        evidence: economic_evidence(41, "-9"),
                    },
                ),
            ],
        })
        .unwrap();
    let fact = prepared.batch().records[0]
        .economic_fact()
        .expect("首个经济身份必须生成唯一经济事实");
    assert_eq!(fact.economic_identity, economic_identity);
    let fact_reference = fact.fact_reference.clone();
    assert!(prepared.batch().records[1..3].iter().all(|record| matches!(
        record,
        NormalizationRecordV1::EconomicAlias { fact_reference: alias, .. }
            if alias == &fact_reference
    )));
    assert_eq!(
        prepared
            .batch()
            .records
            .iter()
            .filter(|record| record.economic_fact().is_some())
            .count(),
        1
    );
    assert!(matches!(
        &prepared.batch().records[3],
        NormalizationRecordV1::Conflict {
            scope: NormalizationConflictScopeV1::EconomicIdentity { economic_identity: seen },
            ..
        } if seen == &economic_identity
    ));
}

#[test]
fn alias_and_economic_conflict_claim_their_new_logical_identity_in_the_same_batch() {
    let core = NormalizationCoreV1::new(context());
    let alias_logical_key = SourceNeutralLogicalKeyV1::EconomicEvidence {
        evidence_identity: "same-batch-alias-55".to_owned(),
    };
    let conflict_logical_key = SourceNeutralLogicalKeyV1::EconomicEvidence {
        evidence_identity: "same-batch-economic-conflict-55".to_owned(),
    };
    let prepared = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(0),
            evidence: vec![
                evidence(
                    30,
                    0,
                    SourceNeutralLogicalKeyV1::EconomicEvidence {
                        evidence_identity: "same-batch-primary-55".to_owned(),
                    },
                    SourceNeutralEvidenceKindV1::Economic {
                        evidence: economic_evidence(41, "-10"),
                    },
                ),
                evidence(
                    31,
                    0,
                    alias_logical_key.clone(),
                    SourceNeutralEvidenceKindV1::Economic {
                        evidence: economic_evidence(41, "-10"),
                    },
                ),
                evidence(
                    33,
                    0,
                    alias_logical_key,
                    SourceNeutralEvidenceKindV1::Economic {
                        evidence: economic_evidence(42, "-20"),
                    },
                ),
                evidence(
                    34,
                    0,
                    conflict_logical_key.clone(),
                    SourceNeutralEvidenceKindV1::Economic {
                        evidence: economic_evidence(41, "-9"),
                    },
                ),
                evidence(
                    36,
                    0,
                    conflict_logical_key,
                    SourceNeutralEvidenceKindV1::Economic {
                        evidence: economic_evidence(42, "-20"),
                    },
                ),
            ],
        })
        .unwrap();

    assert!(matches!(
        prepared.batch().records.as_slice(),
        [
            NormalizationRecordV1::EconomicFact { .. },
            NormalizationRecordV1::EconomicAlias { .. },
            NormalizationRecordV1::Conflict {
                scope: NormalizationConflictScopeV1::LogicalIdentity { .. },
                ..
            },
            NormalizationRecordV1::Conflict {
                scope: NormalizationConflictScopeV1::EconomicIdentity { .. },
                ..
            },
            NormalizationRecordV1::Conflict {
                scope: NormalizationConflictScopeV1::LogicalIdentity { .. },
                ..
            },
        ]
    ));
    assert_eq!(
        prepared
            .batch()
            .records
            .iter()
            .filter(|record| record.economic_fact().is_some())
            .count(),
        1
    );
}

#[test]
fn late_coverage_stays_pending_until_the_economic_fact_is_committed() {
    let core = NormalizationCoreV1::new(context());
    let economic_key = SourceNeutralEconomicKeyV1::AccountTransition {
        account_transition_seq: 77,
    };
    let late_key = SourceNeutralLogicalKeyV1::AuditEvidence {
        evidence_identity: "late-before-economic-77".to_owned(),
    };
    let late_kind = SourceNeutralEvidenceKindV1::LateEconomicCoverage {
        economic_key: economic_key.clone(),
    };
    let prepared = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(0),
            evidence: vec![
                evidence(40, 0, late_key.clone(), late_kind.clone()),
                evidence(
                    41,
                    0,
                    SourceNeutralLogicalKeyV1::EconomicEvidence {
                        evidence_identity: "account-delta-77".to_owned(),
                    },
                    SourceNeutralEvidenceKindV1::Economic {
                        evidence: economic_evidence(77, "-10"),
                    },
                ),
                evidence(42, 0, late_key, late_kind),
            ],
        })
        .unwrap();
    assert!(matches!(
        prepared.batch().records[0],
        NormalizationRecordV1::Pending { .. }
    ));
    assert!(matches!(
        prepared.batch().records[2],
        NormalizationRecordV1::EconomicAlias { .. }
    ));
}

#[test]
fn private_feedback_sequence_from_the_source_reference_is_authoritative() {
    let core = NormalizationCoreV1::new(context());
    let source_ten = evidence(
        10,
        0,
        SourceNeutralLogicalKeyV1::OrderLifecycle {
            order_identity: "source-order-10".to_owned(),
            occurrence: 0,
        },
        order_lifecycle(SourceNeutralOrderLifecycleEventV1::Accepted {
            state: SourceNeutralOrderStateV1::Open,
        }),
    );
    let source_twenty = evidence(
        20,
        0,
        SourceNeutralLogicalKeyV1::OrderLifecycle {
            order_identity: "source-order-20".to_owned(),
            occurrence: 0,
        },
        order_lifecycle(SourceNeutralOrderLifecycleEventV1::Accepted {
            state: SourceNeutralOrderStateV1::Open,
        }),
    );
    let prepared = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(0),
            evidence: vec![source_twenty, source_ten],
        })
        .unwrap();

    let source_sequences: Vec<_> = prepared
        .batch()
        .records
        .iter()
        .map(|record| match &record.source_reference().record {
            SimV2SourceRecordReferenceV1::OrderLifecycle {
                private_feedback_sequence,
                ..
            } => *private_feedback_sequence,
            other => panic!("预期订单生命周期来源，实际 {other:?}"),
        })
        .collect();
    assert_eq!(source_sequences, vec![10, 20]);
}

fn cancellation_source_record(
    private_feedback_sequence: u64,
    item_id: &str,
) -> SimV2SourceRecordReferenceV1 {
    SimV2SourceRecordReferenceV1::DirectResponse {
        private_feedback_sequence,
        platform_responded_at_utc_ms: 4_000,
        response_received_at_utc_ms: 4_100,
        association: SimV2DirectResponseAssociationV1::CancellationTarget {
            global_request_id: "cancel-request-55".to_owned(),
            method: ExecutionMethodV2::CancelOrders,
            target_ordinal: 3,
            input_item_refs: vec![CancelInputItemRefV2 {
                item_id: item_id.to_owned(),
                batch_item_ordinal: 7,
            }],
        },
    }
}

#[test]
fn cancellation_input_item_refs_participate_in_same_payload_decisions() {
    let core = NormalizationCoreV1::new(context());
    let logical_key = SourceNeutralLogicalKeyV1::CancellationTarget {
        global_request_id: "cancel-request-55".to_owned(),
        target_ordinal: 3,
    };
    let kind = SourceNeutralEvidenceKindV1::Control {
        result: SourceNeutralControlResultV1::Cancellation {
            order_identity: StableLogicalIdentityV1("cancel-order-55".to_owned()),
            outcome: SourceNeutralCancellationOutcomeV1::NotCanceledNoEffect,
        },
    };
    let changed_association = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(0),
            evidence: vec![
                SourceNeutralEvidenceV1 {
                    source_record: cancellation_source_record(50, "cancel-item-a"),
                    source_ordinal: 0,
                    logical_key: logical_key.clone(),
                    kind: kind.clone(),
                },
                SourceNeutralEvidenceV1 {
                    source_record: cancellation_source_record(51, "cancel-item-a"),
                    source_ordinal: 0,
                    logical_key: logical_key.clone(),
                    kind: kind.clone(),
                },
                SourceNeutralEvidenceV1 {
                    source_record: cancellation_source_record(52, "cancel-item-b"),
                    source_ordinal: 0,
                    logical_key,
                    kind,
                },
            ],
        })
        .unwrap();
    assert!(matches!(
        changed_association.batch().records[1],
        NormalizationRecordV1::Duplicate { .. }
    ));
    assert!(matches!(
        changed_association.batch().records[2],
        NormalizationRecordV1::Conflict { .. }
    ));
}

#[test]
fn conflict_record_keeps_the_incoming_minimum_evidence() {
    let core = NormalizationCoreV1::new(context());
    let logical_key = SourceNeutralLogicalKeyV1::SubmissionItem {
        global_request_id: "conflict-evidence-55".to_owned(),
        batch_item_ordinal: 0,
    };
    let conflict = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(0),
            evidence: vec![
                evidence(
                    60,
                    0,
                    logical_key.clone(),
                    submission(SourceNeutralSubmissionOutcomeV1::RejectedNoEffect),
                ),
                evidence(
                    61,
                    0,
                    logical_key,
                    submission(SourceNeutralSubmissionOutcomeV1::EvidenceUnknownNoEffect),
                ),
            ],
        })
        .unwrap();

    let encoded = serde_json::to_value(&conflict.batch().records[1]).unwrap();
    assert_eq!(
        encoded["Conflict"]["conflicting_evidence"]["Control"]["result"]["Submission"]["outcome"],
        "EvidenceUnknownNoEffect"
    );
}

#[test]
fn pending_economic_identity_is_derived_from_the_current_context() {
    let core = NormalizationCoreV1::new(context());
    let prepared = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(0),
            evidence: vec![evidence(
                70,
                0,
                SourceNeutralLogicalKeyV1::AuditEvidence {
                    evidence_identity: "pending-economic-55".to_owned(),
                },
                SourceNeutralEvidenceKindV1::Pending {
                    scope: PendingScopeV1::EconomicKey {
                        economic_key: SourceNeutralEconomicKeyV1::AccountTransition {
                            account_transition_seq: 70,
                        },
                    },
                },
            )],
        })
        .unwrap();

    assert!(matches!(
        &prepared.batch().records[0],
        NormalizationRecordV1::Pending {
            scope: NormalizationPendingScopeV1::EconomicIdentity {
                economic_identity: SourceNeutralEconomicIdentityV1::AccountTransition {
                    account_identity: seen,
                    account_transition_seq: 70,
                },
            },
            ..
        } if seen == &account_identity()
    ));
}
