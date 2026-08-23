use contracts::execution_normalization::{
    DurableSourceReferenceV1, EXECUTION_NORMALIZATION_CONTRACT_V1,
    ExecutionNormalizationContextReferenceV1, ExecutionNormalizationContextV1,
    ImmutableSourceReferenceV1, LedgerEconomicFactReferenceV1, NoeEconomicFactReferenceV1,
    NoeSourceCommitBoundaryV1, SimV2ContextSourceV1, SimV2DirectResponseAssociationV1,
    SimV2EconomicIdentityV1, SimV2SourceRecordReferenceV1, SimV2SourceReferenceV1,
    SimV2SourceWatermarkV1, SourceNeutralAccountIdentityV1, SourceNeutralEconomicFactV1,
    StableLogicalIdentityV1,
};
use contracts::{
    AccountInstanceKeyV1, ExactDecimal, PlatformAccountBalanceDeltaV1,
    PlatformAccountDeltaReasonV2, PlatformSourceV1, SimCallerRoleV1,
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
            available_pusd: contracts::ExactDecimal("2000".to_owned()),
        },
        contract_version: EXECUTION_NORMALIZATION_CONTRACT_V1.to_owned(),
        implementation_identity: "noe-implementation-53".to_owned(),
    };

    assert_eq!(context.context_reference, context_reference);
    assert_eq!(context.source.available_pusd.0, "2000");

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
fn response_correlations_and_sim_economic_identity_have_distinct_owners() {
    let submission = SimV2DirectResponseAssociationV1::SubmissionItem {
        global_request_id: "request-53".to_owned(),
        batch_item_ordinal: 4,
    };
    let cancellation = SimV2DirectResponseAssociationV1::CancellationTarget {
        global_request_id: "request-53".to_owned(),
        target_ordinal: 4,
    };
    assert_ne!(submission, cancellation);

    let economic_identity = SimV2EconomicIdentityV1 {
        account_identity: account_identity(),
        account_transition_seq: 41,
    };
    let fact_reference = NoeEconomicFactReferenceV1("economic-fact-41".to_owned());
    let ledger_reference = LedgerEconomicFactReferenceV1 {
        context_reference: ExecutionNormalizationContextReferenceV1("context-53".to_owned()),
        economic_identity: economic_identity.clone(),
        fact_reference: fact_reference.clone(),
    };

    assert_eq!(ledger_reference.economic_identity, economic_identity);
    assert_eq!(ledger_reference.fact_reference, fact_reference);

    let logical_identity = StableLogicalIdentityV1("logical-order-53".to_owned());
    assert_ne!(logical_identity.0, ledger_reference.fact_reference.0);

    let economic_fact = SourceNeutralEconomicFactV1 {
        fact_reference: fact_reference.clone(),
        logical_identity,
        economic_identity: economic_identity.clone(),
        source_reference: SimV2SourceReferenceV1 {
            context_reference: ledger_reference.context_reference.clone(),
            watermark: source_watermark(),
            record: SimV2SourceRecordReferenceV1::OrderLifecycle {
                private_feedback_sequence: 99,
                platform_event_at_utc_ms: 2_000,
                feedback_received_at_utc_ms: 2_050,
            },
        },
        platform_effective_at_utc_ms: 2_000,
        reason: PlatformAccountDeltaReasonV2::OrderAcceptedEncumbranceApplied {
            platform_order_id: "order-53".to_owned(),
        },
        condition_id: None,
        balance_delta: PlatformAccountBalanceDeltaV1 {
            cash_pusd_delta: ExactDecimal("0".to_owned()),
            available_pusd_delta: ExactDecimal("-10".to_owned()),
            principal_encumbrance_pusd_delta: ExactDecimal("10".to_owned()),
            immediate_taker_fee_encumbrance_pusd_delta: ExactDecimal("0".to_owned()),
            outcome_balance_deltas: Vec::new(),
            maker_rebate_receivable_pusd_delta: ExactDecimal("0".to_owned()),
            taker_rebate_receivable_pusd_delta: ExactDecimal("0".to_owned()),
            maker_rebate_locked_remainder_pusd_delta: ExactDecimal("0".to_owned()),
            taker_rebate_locked_remainder_pusd_delta: ExactDecimal("0".to_owned()),
        },
    };

    assert_eq!(economic_fact.economic_identity.account_transition_seq, 41);
    assert_eq!(
        economic_fact.fact_reference,
        ledger_reference.fact_reference
    );
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
