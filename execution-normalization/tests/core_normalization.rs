use contracts::execution_normalization::{
    DurableSourceReferenceV1, ExecutionNormalizationContextReferenceV1,
    ExecutionNormalizationContextV1, ImmutableSourceReferenceV1, SimV2ContextSourceV1,
    SimV2SourceRecordReferenceV1, SimV2SourceReferenceV1, SimV2SourceWatermarkV1,
    SourceNeutralAccountBalanceDeltaV1, SourceNeutralAccountIdentityV1,
    SourceNeutralAccountTransitionKindV1, SourceNeutralEconomicIdentityV1, StableLogicalIdentityV1,
};
use contracts::{AccountInstanceKeyV1, ExactDecimal, PlatformSourceV1, SimCallerRoleV1, TokenId};
use execution_normalization::{
    NormalizationBatchInputV1, NormalizationConflictScopeV1, NormalizationCoreV1,
    NormalizationRecordV1, OrderLifecycleDispositionV1, SourceNeutralEconomicEvidenceV1,
    SourceNeutralEconomicKeyV1, SourceNeutralEvidenceKindV1, SourceNeutralEvidenceV1,
    SourceNeutralLogicalKeyV1, SourceOrderV1, SubmissionDispositionV1,
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

fn source_reference(batch_sequence: u64, private_feedback_sequence: u64) -> SimV2SourceReferenceV1 {
    SimV2SourceReferenceV1 {
        context_reference: ExecutionNormalizationContextReferenceV1("context-55".to_owned()),
        watermark: watermark(batch_sequence),
        record: SimV2SourceRecordReferenceV1::OrderLifecycle {
            private_feedback_sequence,
            platform_event_at_utc_ms: 1_000 + private_feedback_sequence as i64,
            feedback_received_at_utc_ms: 1_100 + private_feedback_sequence as i64,
        },
    }
}

fn evidence(
    batch_sequence: u64,
    source_sequence: u64,
    ordinal: u32,
    logical_key: SourceNeutralLogicalKeyV1,
    kind: SourceNeutralEvidenceKindV1,
) -> SourceNeutralEvidenceV1 {
    SourceNeutralEvidenceV1 {
        source_reference: source_reference(batch_sequence, source_sequence),
        source_order: SourceOrderV1 {
            sequence: source_sequence,
            ordinal,
        },
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

#[test]
fn feedback_sorting_preserves_submission_layers_partial_fill_and_zero_effects() {
    let core = NormalizationCoreV1::new(context());
    let prepared = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(0),
            evidence: vec![
                evidence(
                    0,
                    14,
                    0,
                    SourceNeutralLogicalKeyV1::OrderLifecycle {
                        order_identity: "order-55".to_owned(),
                        occurrence: 14,
                    },
                    SourceNeutralEvidenceKindV1::OrderLifecycle {
                        disposition: OrderLifecycleDispositionV1::Cancelled,
                    },
                ),
                evidence(
                    0,
                    10,
                    0,
                    SourceNeutralLogicalKeyV1::SubmissionItem {
                        global_request_id: "request-55".to_owned(),
                        batch_item_ordinal: 0,
                    },
                    SourceNeutralEvidenceKindV1::Submission {
                        disposition: SubmissionDispositionV1::EvidenceUnknownNoEffect,
                    },
                ),
                evidence(
                    0,
                    13,
                    0,
                    SourceNeutralLogicalKeyV1::OrderLifecycle {
                        order_identity: "order-55".to_owned(),
                        occurrence: 13,
                    },
                    SourceNeutralEvidenceKindV1::OrderLifecycle {
                        disposition: OrderLifecycleDispositionV1::VenueDustClosed,
                    },
                ),
                evidence(
                    0,
                    12,
                    0,
                    SourceNeutralLogicalKeyV1::OrderLifecycle {
                        order_identity: "order-55".to_owned(),
                        occurrence: 12,
                    },
                    SourceNeutralEvidenceKindV1::OrderLifecycle {
                        disposition: OrderLifecycleDispositionV1::PartiallyFilled {
                            cumulative_filled_shares: ExactDecimal("1.25".to_owned()),
                        },
                    },
                ),
                evidence(
                    0,
                    11,
                    0,
                    SourceNeutralLogicalKeyV1::OrderLifecycle {
                        order_identity: "order-55".to_owned(),
                        occurrence: 11,
                    },
                    SourceNeutralEvidenceKindV1::OrderLifecycle {
                        disposition: OrderLifecycleDispositionV1::Open,
                    },
                ),
            ],
        })
        .expect("来源中立核心应接受有效的代表性证据");

    let sequences: Vec<_> = prepared
        .batch()
        .records
        .iter()
        .map(|record| record.source_order().sequence)
        .collect();
    assert_eq!(sequences, vec![10, 11, 12, 13, 14]);
    assert!(matches!(
        &prepared.batch().records[0],
        NormalizationRecordV1::Evidence {
            evidence: SourceNeutralEvidenceKindV1::Submission {
                disposition: SubmissionDispositionV1::EvidenceUnknownNoEffect
            },
            ..
        }
    ));
    assert!(matches!(
        &prepared.batch().records[2],
        NormalizationRecordV1::Evidence {
            evidence: SourceNeutralEvidenceKindV1::OrderLifecycle {
                disposition: OrderLifecycleDispositionV1::PartiallyFilled { .. }
            },
            ..
        }
    ));
    assert!(matches!(
        &prepared.batch().records[3],
        NormalizationRecordV1::Evidence {
            evidence: SourceNeutralEvidenceKindV1::OrderLifecycle {
                disposition: OrderLifecycleDispositionV1::VenueDustClosed
            },
            ..
        }
    ));
    assert!(matches!(
        &prepared.batch().records[4],
        NormalizationRecordV1::Evidence {
            evidence: SourceNeutralEvidenceKindV1::OrderLifecycle {
                disposition: OrderLifecycleDispositionV1::Cancelled
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
    let mut core = NormalizationCoreV1::new(context());
    let logical_key = SourceNeutralLogicalKeyV1::SubmissionItem {
        global_request_id: "request-idempotent-55".to_owned(),
        batch_item_ordinal: 7,
    };
    let first = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(0),
            evidence: vec![evidence(
                0,
                20,
                0,
                logical_key.clone(),
                SourceNeutralEvidenceKindV1::Submission {
                    disposition: SubmissionDispositionV1::Accepted,
                },
            )],
        })
        .unwrap();
    let first_identity = first.batch().records[0].logical_identity().clone();
    core.accept_committed(first);

    let duplicate = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(1),
            evidence: vec![evidence(
                1,
                21,
                0,
                logical_key.clone(),
                SourceNeutralEvidenceKindV1::Submission {
                    disposition: SubmissionDispositionV1::Accepted,
                },
            )],
        })
        .unwrap();
    assert_eq!(
        duplicate.batch().records[0].logical_identity(),
        &first_identity
    );
    assert!(matches!(
        duplicate.batch().records[0],
        NormalizationRecordV1::Duplicate { .. }
    ));

    let conflict = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(1),
            evidence: vec![evidence(
                1,
                21,
                0,
                logical_key,
                SourceNeutralEvidenceKindV1::Submission {
                    disposition: SubmissionDispositionV1::RejectedNoEffect,
                },
            )],
        })
        .unwrap();
    assert!(matches!(
        &conflict.batch().records[0],
        NormalizationRecordV1::Conflict {
            scope: NormalizationConflictScopeV1::LogicalIdentity { logical_identity },
            ..
        } if logical_identity == &first_identity
    ));
}

#[test]
fn economic_identity_is_applied_once_and_late_coverage_becomes_alias() {
    let mut core = NormalizationCoreV1::new(context());
    let economic_key = SourceNeutralEconomicKeyV1::AccountTransition {
        account_transition_seq: 41,
    };
    let economic_identity = SourceNeutralEconomicIdentityV1::AccountTransition {
        account_identity: account_identity(),
        account_transition_seq: 41,
    };
    let first = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(0),
            evidence: vec![evidence(
                0,
                30,
                0,
                SourceNeutralLogicalKeyV1::EconomicEvidence {
                    evidence_identity: "account-delta-41".to_owned(),
                },
                SourceNeutralEvidenceKindV1::Economic {
                    evidence: economic_evidence(41, "-10"),
                },
            )],
        })
        .unwrap();
    let fact = first.batch().records[0]
        .economic_fact()
        .expect("首个经济身份必须生成唯一经济事实");
    assert_eq!(fact.economic_identity, economic_identity);
    let fact_reference = fact.fact_reference.clone();
    core.accept_committed(first);

    let aliases = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(1),
            evidence: vec![
                evidence(
                    1,
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
                    1,
                    32,
                    0,
                    SourceNeutralLogicalKeyV1::AuditEvidence {
                        evidence_identity: "late-order-confirmation-41".to_owned(),
                    },
                    SourceNeutralEvidenceKindV1::LateEconomicCoverage {
                        economic_key: economic_key.clone(),
                    },
                ),
            ],
        })
        .unwrap();
    assert!(aliases.batch().records.iter().all(|record| matches!(
        record,
        NormalizationRecordV1::EconomicAlias { fact_reference: alias, .. }
            if alias == &fact_reference
    )));
    assert_eq!(
        aliases
            .batch()
            .records
            .iter()
            .filter(|record| record.economic_fact().is_some())
            .count(),
        0
    );

    let conflict = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(1),
            evidence: vec![evidence(
                1,
                33,
                0,
                SourceNeutralLogicalKeyV1::EconomicEvidence {
                    evidence_identity: "conflicting-account-delta-41".to_owned(),
                },
                SourceNeutralEvidenceKindV1::Economic {
                    evidence: economic_evidence(41, "-9"),
                },
            )],
        })
        .unwrap();
    assert!(matches!(
        &conflict.batch().records[0],
        NormalizationRecordV1::Conflict {
            scope: NormalizationConflictScopeV1::EconomicIdentity { economic_identity: seen },
            ..
        } if seen == &economic_identity
    ));
}

#[test]
fn restored_economic_fact_preserves_logical_idempotence() {
    let logical_key = SourceNeutralLogicalKeyV1::EconomicEvidence {
        evidence_identity: "restart-account-delta-51".to_owned(),
    };
    let mut core = NormalizationCoreV1::new(context());
    let first = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(0),
            evidence: vec![evidence(
                0,
                35,
                0,
                logical_key.clone(),
                SourceNeutralEvidenceKindV1::Economic {
                    evidence: economic_evidence(51, "-10"),
                },
            )],
        })
        .unwrap();
    let committed_records = first.batch().records.clone();
    core.accept_committed(first);

    let restored = NormalizationCoreV1::restore(context(), committed_records.iter());
    let replay = restored
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(1),
            evidence: vec![evidence(
                1,
                36,
                0,
                logical_key,
                SourceNeutralEvidenceKindV1::Economic {
                    evidence: economic_evidence(51, "-10"),
                },
            )],
        })
        .unwrap();

    assert!(matches!(
        replay.batch().records[0],
        NormalizationRecordV1::Duplicate { .. }
    ));
}

#[test]
fn late_coverage_stays_pending_until_the_economic_fact_is_committed() {
    let mut core = NormalizationCoreV1::new(context());
    let economic_key = SourceNeutralEconomicKeyV1::AccountTransition {
        account_transition_seq: 77,
    };
    let late_key = SourceNeutralLogicalKeyV1::AuditEvidence {
        evidence_identity: "late-before-economic-77".to_owned(),
    };
    let late_kind = SourceNeutralEvidenceKindV1::LateEconomicCoverage {
        economic_key: economic_key.clone(),
    };
    let pending = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(0),
            evidence: vec![evidence(0, 40, 0, late_key.clone(), late_kind.clone())],
        })
        .unwrap();
    assert!(matches!(
        pending.batch().records[0],
        NormalizationRecordV1::Pending { .. }
    ));
    core.accept_committed(pending);

    let economic = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(1),
            evidence: vec![evidence(
                1,
                41,
                0,
                SourceNeutralLogicalKeyV1::EconomicEvidence {
                    evidence_identity: "account-delta-77".to_owned(),
                },
                SourceNeutralEvidenceKindV1::Economic {
                    evidence: economic_evidence(77, "-10"),
                },
            )],
        })
        .unwrap();
    core.accept_committed(economic);

    let resolved = core
        .prepare_batch(NormalizationBatchInputV1 {
            source_watermark: watermark(2),
            evidence: vec![evidence(2, 42, 0, late_key, late_kind)],
        })
        .unwrap();
    assert!(matches!(
        resolved.batch().records[0],
        NormalizationRecordV1::EconomicAlias { .. }
    ));
}
