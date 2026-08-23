use contracts::execution_normalization::{
    DurableSourceReferenceV1, EXECUTION_NORMALIZATION_CONTRACT_V1,
    ExecutionNormalizationContextReferenceV1, ExecutionNormalizationContextV1,
    ImmutableSourceReferenceV1, NoeEconomicFactReferenceV1, NoeSourceCommitBoundaryV1,
    SimV2ContextSourceV1, SimV2DirectResponseAssociationV1, SimV2SourceRecordReferenceV1,
    SimV2SourceReferenceV1, SimV2SourceWatermarkV1, SourceNeutralAccountBalanceDeltaV1,
    SourceNeutralAccountIdentityV1, SourceNeutralAccountTransitionKindV1,
    SourceNeutralEconomicDispositionV1, SourceNeutralEconomicFactV1,
    SourceNeutralEconomicIdentityV1, StableLogicalIdentityV1,
};
use contracts::{
    AccountInstanceKeyV1, CancelInputItemRefV2, ExactDecimal, PlatformSourceV1, SimCallerRoleV1,
};

fn account_identity() -> SourceNeutralAccountIdentityV1 {
    SourceNeutralAccountIdentityV1 {
        source_category: "platform".to_owned(),
        environment: "simulation".to_owned(),
        run_scope: "run-53".to_owned(),
        account_scope: "account-53".to_owned(),
    }
}

fn source_watermark() -> SimV2SourceWatermarkV1 {
    SimV2SourceWatermarkV1 {
        run_id: "run-53".to_owned(),
        projection_batch_sequence: 7,
    }
}

#[test]
fn context_and_sim_v2_source_references_are_lightweight_and_discriminated() {
    let context_reference = ExecutionNormalizationContextReferenceV1("context-53".to_owned());
    let context = ExecutionNormalizationContextV1 {
        context_reference: context_reference.clone(),
        account_identity: account_identity(),
        source: SimV2ContextSourceV1 {
            account_instance_key: AccountInstanceKeyV1 {
                run_id: "run-53".to_owned(),
                caller_role: SimCallerRoleV1::ModelTrain,
                caller_instance_id: "caller-53".to_owned(),
                account_id: "account-53".to_owned(),
                platform_source: PlatformSourceV1::Simulated,
            },
            run_opened: DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Location(
                    "artifact://sim/run-53/opened".to_owned(),
                ),
            },
            account_opening_evidence_identity: "creation-53".to_owned(),
            account_opening: DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Location(
                    "artifact://sim/run-53/account-opening".to_owned(),
                ),
            },
        },
        contract_version: EXECUTION_NORMALIZATION_CONTRACT_V1.to_owned(),
        implementation_identity: "noe-implementation-53".to_owned(),
    };

    assert_eq!(context.context_reference, context_reference);
    let SimV2ContextSourceV1 {
        account_instance_key,
        run_opened,
        account_opening_evidence_identity,
        account_opening,
    } = context.source;
    assert_eq!(account_instance_key.run_id, "run-53");
    assert!(matches!(
        run_opened.source,
        ImmutableSourceReferenceV1::Location(_)
    ));
    assert_eq!(account_opening_evidence_identity, "creation-53");
    assert!(matches!(
        account_opening.source,
        ImmutableSourceReferenceV1::Location(_)
    ));

    let watermark = source_watermark();
    let source_records = [
        SimV2SourceRecordReferenceV1::AccountOpening,
        SimV2SourceRecordReferenceV1::MarketEnrollment {
            enrollment_sequence: 1,
        },
        SimV2SourceRecordReferenceV1::DirectResponse {
            private_feedback_sequence: 10,
            platform_responded_at_utc_ms: 100,
            response_received_at_utc_ms: 120,
            association: SimV2DirectResponseAssociationV1::SubmissionItem {
                global_request_id: "post-batch-53".to_owned(),
                batch_item_ordinal: 3,
            },
        },
        SimV2SourceRecordReferenceV1::OrderLifecycle {
            private_feedback_sequence: 11,
            platform_event_at_utc_ms: 130,
            feedback_received_at_utc_ms: 150,
        },
        SimV2SourceRecordReferenceV1::TradeLifecycle {
            private_feedback_sequence: 12,
            platform_event_at_utc_ms: 160,
            feedback_received_at_utc_ms: 180,
        },
        SimV2SourceRecordReferenceV1::SettlementLifecycle {
            private_feedback_sequence: 13,
            platform_event_at_utc_ms: 190,
            feedback_received_at_utc_ms: 210,
        },
    ];

    for record in source_records {
        let source = SimV2SourceReferenceV1 {
            context_reference: context_reference.clone(),
            watermark: watermark.clone(),
            record,
        };
        assert_eq!(source.context_reference, context_reference);
        assert_eq!(source.watermark, watermark);
    }
}

#[test]
fn response_correlations_and_economic_identity_have_distinct_owners() {
    let submission = SimV2DirectResponseAssociationV1::SubmissionItem {
        global_request_id: "request-53".to_owned(),
        batch_item_ordinal: 4,
    };
    let cancellation = SimV2DirectResponseAssociationV1::CancellationTarget {
        global_request_id: "request-53".to_owned(),
        target_ordinal: 4,
        input_item_refs: vec![CancelInputItemRefV2 {
            item_id: "cancel-item-53".to_owned(),
            batch_item_ordinal: 9,
        }],
    };
    assert_ne!(submission, cancellation);

    let SimV2DirectResponseAssociationV1::CancellationTarget {
        global_request_id,
        target_ordinal,
        input_item_refs,
    } = cancellation
    else {
        unreachable!("已构造撤单目标关联")
    };
    assert_eq!(global_request_id, "request-53");
    assert_eq!(target_ordinal, 4);
    assert_eq!(input_item_refs[0].item_id, "cancel-item-53");
    assert_eq!(input_item_refs[0].batch_item_ordinal, 9);

    let economic_identity = SourceNeutralEconomicIdentityV1::AccountTransition {
        account_identity: account_identity(),
        account_transition_seq: 41,
    };
    let fact_reference = NoeEconomicFactReferenceV1("economic-fact-41".to_owned());
    let logical_identity = StableLogicalIdentityV1("logical-order-53".to_owned());
    assert_ne!(logical_identity.0, fact_reference.0);

    let economic_fact = SourceNeutralEconomicFactV1 {
        fact_reference: fact_reference.clone(),
        logical_identity,
        economic_identity: economic_identity.clone(),
        source_reference: SimV2SourceReferenceV1 {
            context_reference: ExecutionNormalizationContextReferenceV1("context-53".to_owned()),
            watermark: source_watermark(),
            record: SimV2SourceRecordReferenceV1::OrderLifecycle {
                private_feedback_sequence: 99,
                platform_event_at_utc_ms: 2_000,
                feedback_received_at_utc_ms: 2_050,
            },
        },
        disposition: SourceNeutralEconomicDispositionV1::AccountTransition {
            economic_effective_at_utc_ms: 2_000,
            condition_id: None,
            transition: SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
                order_identity: StableLogicalIdentityV1("logical-order-53".to_owned()),
            },
            balance_delta: Box::new(SourceNeutralAccountBalanceDeltaV1 {
                cash_pusd_delta: ExactDecimal("0".to_owned()),
                available_pusd_delta: ExactDecimal("-10".to_owned()),
                principal_encumbrance_pusd_delta: ExactDecimal("10".to_owned()),
                immediate_taker_fee_encumbrance_pusd_delta: ExactDecimal("0".to_owned()),
                outcome_balance_deltas: Vec::new(),
                maker_rebate_receivable_pusd_delta: ExactDecimal("0".to_owned()),
                taker_rebate_receivable_pusd_delta: ExactDecimal("0".to_owned()),
                maker_rebate_locked_remainder_pusd_delta: ExactDecimal("0".to_owned()),
                taker_rebate_locked_remainder_pusd_delta: ExactDecimal("0".to_owned()),
            }),
        },
    };

    assert!(matches!(
        economic_fact.economic_identity,
        SourceNeutralEconomicIdentityV1::AccountTransition {
            account_transition_seq: 41,
            ..
        }
    ));
    assert_eq!(economic_fact.fact_reference, fact_reference);
}

#[test]
fn account_opening_economics_exist_only_in_the_source_neutral_fact() {
    let context_reference = ExecutionNormalizationContextReferenceV1("context-53".to_owned());
    let opening_identity = SourceNeutralEconomicIdentityV1::AccountOpening {
        account_identity: account_identity(),
        opening_evidence_identity: "creation-53".to_owned(),
    };
    let opening = SourceNeutralEconomicFactV1 {
        fact_reference: NoeEconomicFactReferenceV1("opening-fact-53".to_owned()),
        logical_identity: StableLogicalIdentityV1("logical-opening-53".to_owned()),
        economic_identity: opening_identity.clone(),
        source_reference: SimV2SourceReferenceV1 {
            context_reference: context_reference.clone(),
            watermark: SimV2SourceWatermarkV1 {
                run_id: "run-53".to_owned(),
                projection_batch_sequence: 0,
            },
            record: SimV2SourceRecordReferenceV1::AccountOpening,
        },
        disposition: SourceNeutralEconomicDispositionV1::AccountOpening {
            available_pusd: ExactDecimal("2000".to_owned()),
        },
    };
    let SourceNeutralEconomicDispositionV1::AccountOpening { available_pusd } = opening.disposition
    else {
        unreachable!("已构造来源中立开户经济事实")
    };
    assert_eq!(available_pusd.0, "2000");
    assert_eq!(opening.fact_reference.0, "opening-fact-53");
}

#[test]
fn source_watermark_and_durable_time_share_one_commit_boundary() {
    let boundary = NoeSourceCommitBoundaryV1 {
        context_reference: ExecutionNormalizationContextReferenceV1("context-53".to_owned()),
        source_watermark: source_watermark(),
        durable_available_at_utc_ms: 1_800,
    };

    assert_eq!(boundary.source_watermark.run_id, "run-53");
    assert_eq!(boundary.source_watermark.projection_batch_sequence, 7);
    assert_eq!(boundary.durable_available_at_utc_ms, 1_800);
}
