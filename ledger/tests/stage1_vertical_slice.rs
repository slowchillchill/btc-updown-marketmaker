use contracts::execution_normalization::{
    DurableSourceReferenceV1, ImmutableSourceReferenceV1, NoeResultReferenceV1,
    SimV2SourceRecordReferenceV1, SimV2SourceWatermarkV1, SourceNeutralAccountBalanceDeltaV1,
    SourceNeutralAccountTransitionKindV1, SourceNeutralOutcomeBalanceDeltaV1,
    StableLogicalIdentityV1,
};
use contracts::{
    AccountInstanceKeyV1, ConditionId, ExactDecimal, PlatformSourceV1, Sha256Hex, SimCallerRoleV1,
    TokenId,
};
use execution_normalization::{
    DurableContextStoreV1, DurableErrorV1, NormalizationBatchInputV1, PendingScopeV1,
    SimV2ContextSeedV1, SourceNeutralEconomicEvidenceV1, SourceNeutralEvidenceKindV1,
    SourceNeutralEvidenceV1, SourceNeutralLogicalKeyV1,
};
use ledger::{
    AccountAdmissionPolicyV1, LedgerApplyOutcomeV1, LedgerErrorV1, LedgerMarketProjectionV1,
    LedgerWriterV1,
};
use tempfile::TempDir;

const OPENING_PUSD: &str = "2000";
const MARKET_A: &str = "condition-a";
const MARKET_B: &str = "condition-b";
const TOKEN_A: &str = "token-a";
const ACCEPTED_PRINCIPAL_A: &str = "10.00000000";
const ACCEPTED_FEE_A: &str = "0.12345678";
const ACCEPTED_TOTAL_A: &str = "-10.12345678";
const FILLED_SHARES_A: &str = "20.24691356";
const ACCEPTED_PRINCIPAL_B: &str = "5.00000000";
const EXPECTED_CASH: &str = "1989.87654322";
const EXPECTED_AVAILABLE: &str = "1984.87654322";

struct NoeFixture {
    _temporary: TempDir,
    store: DurableContextStoreV1,
    run_id: String,
    opening_evidence_identity: String,
    next_projection_batch_sequence: u64,
}

impl NoeFixture {
    fn new(run_id: &str, account_id: &str, opening_evidence_identity: &str) -> Self {
        let temporary = TempDir::new().unwrap();
        let seed = SimV2ContextSeedV1 {
            run_id: run_id.to_owned(),
            account_instance_key: account_key(run_id, account_id),
            run_opened: DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Sha256(Sha256Hex("1".repeat(64))),
            },
            account_opening: DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Sha256(Sha256Hex("2".repeat(64))),
            },
            account_opening_evidence_identity: opening_evidence_identity.to_owned(),
            implementation_identity: "ledger-stage-1-noe-fixture".to_owned(),
        };
        let store = DurableContextStoreV1::open_or_create_sim_v2(temporary.path(), seed).unwrap();
        Self {
            _temporary: temporary,
            store,
            run_id: run_id.to_owned(),
            opening_evidence_identity: opening_evidence_identity.to_owned(),
            next_projection_batch_sequence: 1,
        }
    }

    fn commit_opening(&mut self, available_pusd: &str) -> NoeResultReferenceV1 {
        let sequence = self.next_projection_batch_sequence;
        self.commit(
            SimV2SourceRecordReferenceV1::AccountOpening,
            SourceNeutralLogicalKeyV1::EconomicEvidence {
                evidence_identity: format!("opening-evidence-{sequence}"),
            },
            SourceNeutralEvidenceKindV1::Economic {
                evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
                    opening_evidence_identity: self.opening_evidence_identity.clone(),
                    available_pusd: decimal(available_pusd),
                },
            },
        )
    }

    fn commit_transition(
        &mut self,
        account_transition_seq: u64,
        condition_id: &str,
        transition: SourceNeutralAccountTransitionKindV1,
        balance_delta: SourceNeutralAccountBalanceDeltaV1,
    ) -> NoeResultReferenceV1 {
        let sequence = self.next_projection_batch_sequence;
        self.commit(
            SimV2SourceRecordReferenceV1::OrderLifecycle {
                private_feedback_sequence: sequence,
                platform_event_at_utc_ms: 200 + sequence as i64,
                feedback_received_at_utc_ms: 300 + sequence as i64,
            },
            SourceNeutralLogicalKeyV1::EconomicEvidence {
                evidence_identity: format!("transition-evidence-{sequence}"),
            },
            SourceNeutralEvidenceKindV1::Economic {
                evidence: SourceNeutralEconomicEvidenceV1::AccountTransition {
                    account_transition_seq,
                    economic_effective_at_utc_ms: 200 + sequence as i64,
                    condition_id: Some(ConditionId(condition_id.to_owned())),
                    transition,
                    balance_delta: Box::new(balance_delta),
                },
            },
        )
    }

    fn commit_non_economic(&mut self) -> NoeResultReferenceV1 {
        let sequence = self.next_projection_batch_sequence;
        self.commit(
            SimV2SourceRecordReferenceV1::OrderLifecycle {
                private_feedback_sequence: sequence,
                platform_event_at_utc_ms: 200 + sequence as i64,
                feedback_received_at_utc_ms: 300 + sequence as i64,
            },
            SourceNeutralLogicalKeyV1::AuditEvidence {
                evidence_identity: format!("non-economic-{sequence}"),
            },
            SourceNeutralEvidenceKindV1::Pending {
                scope: PendingScopeV1::EvidenceGap {
                    scope_identity: format!("gap-{sequence}"),
                },
            },
        )
    }

    fn commit(
        &mut self,
        record: SimV2SourceRecordReferenceV1,
        logical_key: SourceNeutralLogicalKeyV1,
        kind: SourceNeutralEvidenceKindV1,
    ) -> NoeResultReferenceV1 {
        let sequence = self.next_projection_batch_sequence;
        let watermark = SimV2SourceWatermarkV1 {
            run_id: self.run_id.clone(),
            projection_batch_sequence: sequence,
        };
        self.store
            .commit_batch(
                NormalizationBatchInputV1 {
                    source_watermark: watermark,
                    evidence: vec![SourceNeutralEvidenceV1 {
                        source_record: record,
                        source_ordinal: 0,
                        logical_key,
                        kind,
                    }],
                },
                1_000 + sequence as i64,
            )
            .unwrap();
        self.next_projection_batch_sequence += 1;
        self.store
            .commits()
            .last()
            .unwrap()
            .result_reference(0)
            .unwrap()
    }
}

fn account_key(run_id: &str, account_id: &str) -> AccountInstanceKeyV1 {
    AccountInstanceKeyV1 {
        run_id: run_id.to_owned(),
        caller_role: SimCallerRoleV1::ModelTrain,
        caller_instance_id: format!("caller-{account_id}"),
        account_id: account_id.to_owned(),
        platform_source: PlatformSourceV1::Simulated,
    }
}

fn policy() -> AccountAdmissionPolicyV1 {
    AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: decimal("1000"),
    }
}

fn decimal(value: &str) -> ExactDecimal {
    ExactDecimal(value.to_owned())
}

fn balance_delta(
    cash: &str,
    available: &str,
    principal: &str,
    immediate_fee: &str,
    outcome: Option<(&str, &str)>,
) -> SourceNeutralAccountBalanceDeltaV1 {
    SourceNeutralAccountBalanceDeltaV1 {
        cash_pusd_delta: decimal(cash),
        available_pusd_delta: decimal(available),
        principal_encumbrance_pusd_delta: decimal(principal),
        immediate_taker_fee_encumbrance_pusd_delta: decimal(immediate_fee),
        outcome_balance_deltas: outcome
            .into_iter()
            .map(|(token_id, balance)| SourceNeutralOutcomeBalanceDeltaV1 {
                token_id: TokenId(token_id.to_owned()),
                balance_shares_delta: decimal(balance),
            })
            .collect(),
        maker_rebate_receivable_pusd_delta: decimal("0"),
        taker_rebate_receivable_pusd_delta: decimal("0"),
        maker_rebate_locked_remainder_pusd_delta: decimal("0"),
        taker_rebate_locked_remainder_pusd_delta: decimal("0"),
    }
}

fn market<'a>(
    markets: &'a [LedgerMarketProjectionV1],
    condition_id: &str,
) -> &'a LedgerMarketProjectionV1 {
    markets
        .iter()
        .find(|market| market.condition_id.0 == condition_id)
        .unwrap()
}

#[test]
fn real_noe_references_drive_idempotent_balanced_multi_market_projection() {
    let mut fixture = NoeFixture::new("run-stage-1", "account-stage-1", "opening-stage-1");
    let opening = fixture.commit_opening(OPENING_PUSD);
    let accepted_a = fixture.commit_transition(
        1,
        MARKET_A,
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity: StableLogicalIdentityV1("order-a".to_owned()),
        },
        balance_delta(
            "0",
            ACCEPTED_TOTAL_A,
            ACCEPTED_PRINCIPAL_A,
            ACCEPTED_FEE_A,
            None,
        ),
    );
    let filled_a = fixture.commit_transition(
        2,
        MARKET_A,
        SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
            trade_identity: StableLogicalIdentityV1("trade-a".to_owned()),
            path_ordinal: 0,
        },
        balance_delta(
            ACCEPTED_TOTAL_A,
            "0",
            "-10.00000000",
            "-0.12345678",
            Some((TOKEN_A, FILLED_SHARES_A)),
        ),
    );
    let accepted_b = fixture.commit_transition(
        3,
        MARKET_B,
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity: StableLogicalIdentityV1("order-b".to_owned()),
        },
        balance_delta("0", "-5.00000000", ACCEPTED_PRINCIPAL_B, "0", None),
    );

    let mut writer = LedgerWriterV1::new(policy());
    assert_eq!(writer.projection(), None);
    assert_eq!(
        writer
            .admission_policy()
            .per_market_all_in_commitment_limit_pusd,
        decimal("1000")
    );
    assert_eq!(
        writer.apply_noe_result(&opening).unwrap(),
        LedgerApplyOutcomeV1::NewlyApplied {
            ledger_entry_seq: 1
        }
    );
    let opened = writer.projection().unwrap();
    assert_eq!(opened.ledger_entry_seq, 1);
    assert_eq!(opened.platform_source_prefix, 0);
    assert_eq!(
        opened.platform_confirmed_projection.cash_pusd,
        decimal(OPENING_PUSD)
    );
    assert_eq!(
        writer.apply_noe_result(&opening).unwrap(),
        LedgerApplyOutcomeV1::AlreadyApplied {
            ledger_entry_seq: 1
        }
    );

    assert_eq!(
        writer.apply_noe_result(&filled_a).unwrap(),
        LedgerApplyOutcomeV1::MissingPlatformPredecessor {
            expected_account_transition_seq: 1,
            found_account_transition_seq: 2,
        }
    );
    assert_eq!(writer.projection().unwrap(), opened);
    assert_eq!(
        writer.apply_noe_result(&accepted_a).unwrap(),
        LedgerApplyOutcomeV1::NewlyApplied {
            ledger_entry_seq: 2
        }
    );
    assert_eq!(
        writer.apply_noe_result(&accepted_a).unwrap(),
        LedgerApplyOutcomeV1::AlreadyApplied {
            ledger_entry_seq: 2
        }
    );
    assert_eq!(
        writer.apply_noe_result(&filled_a).unwrap(),
        LedgerApplyOutcomeV1::NewlyApplied {
            ledger_entry_seq: 3
        }
    );
    assert_eq!(
        writer.apply_noe_result(&accepted_b).unwrap(),
        LedgerApplyOutcomeV1::NewlyApplied {
            ledger_entry_seq: 4
        }
    );

    let projection = writer.projection().unwrap();
    assert_eq!(projection.ledger_entry_seq, 4);
    assert_eq!(projection.platform_source_prefix, 3);
    assert_eq!(projection.ledger_local_prefix, 0);
    assert_eq!(projection.available_through_utc_ms, 1_004);
    let global = &projection.platform_confirmed_projection;
    assert_eq!(global.cash_pusd, decimal(EXPECTED_CASH));
    assert_eq!(global.available_pusd, decimal(EXPECTED_AVAILABLE));
    assert_eq!(
        global.principal_encumbrance_pusd,
        decimal(ACCEPTED_PRINCIPAL_B)
    );
    assert_eq!(global.immediate_taker_fee_encumbrance_pusd, decimal("0"));
    assert_eq!(global.outcome_balances.len(), 1);
    assert_eq!(global.outcome_balances[0].token_id.0, TOKEN_A);
    assert_eq!(
        global.outcome_balances[0].balance_shares,
        decimal(FILLED_SHARES_A)
    );
    assert_eq!(
        global.outcome_balances[0].position_cost_basis_pusd,
        decimal(ACCEPTED_PRINCIPAL_A)
    );
    assert_eq!(
        global.position_cost_basis_pusd,
        decimal(ACCEPTED_PRINCIPAL_A)
    );
    assert_eq!(global.fees_pusd, decimal(ACCEPTED_FEE_A));
    assert_eq!(global.realized_pnl_pusd, decimal("0"));
    assert_eq!(
        projection
            .local_pending_reservation_projection
            .principal_pusd,
        decimal("0")
    );

    assert_eq!(projection.market_projections.len(), 2);
    assert_eq!(projection.market_projections[0].condition_id.0, MARKET_A);
    assert_eq!(projection.market_projections[1].condition_id.0, MARKET_B);
    let market_a = market(&projection.market_projections, MARKET_A);
    let market_b = market(&projection.market_projections, MARKET_B);
    assert_eq!(market_a.principal_encumbrance_pusd, decimal("0.00000000"));
    assert_eq!(
        market_a.immediate_taker_fee_encumbrance_pusd,
        decimal("0.00000000")
    );
    assert_eq!(
        market_a.outcome_balances[0].balance_shares,
        decimal(FILLED_SHARES_A)
    );
    assert_eq!(
        market_a.outcome_balances[0].position_cost_basis_pusd,
        decimal(ACCEPTED_PRINCIPAL_A)
    );
    assert_eq!(
        market_a.position_cost_basis_pusd,
        decimal(ACCEPTED_PRINCIPAL_A)
    );
    assert_eq!(market_a.fees_pusd, decimal(ACCEPTED_FEE_A));
    assert_eq!(market_a.realized_pnl_pusd, decimal("0"));
    assert_eq!(
        market_b.principal_encumbrance_pusd,
        decimal(ACCEPTED_PRINCIPAL_B)
    );
    assert_eq!(market_b.outcome_balances.len(), 0);
    assert_eq!(market_b.position_cost_basis_pusd, decimal("0"));
    assert_eq!(market_b.fees_pusd, decimal("0"));
    assert_eq!(market_b.realized_pnl_pusd, decimal("0"));

    let mut rebuilt = LedgerWriterV1::new(policy());
    for reference in [&opening, &accepted_a, &filled_a, &accepted_b] {
        rebuilt.apply_noe_result(reference).unwrap();
    }
    assert_eq!(rebuilt.projection().unwrap(), projection);
}

#[test]
fn opening_evidence_and_account_instances_remain_isolated() {
    let mut first = NoeFixture::new("run-isolation", "account-isolation", "opening-first");
    let first_opening = first.commit_opening("2000");
    let mut changed_opening =
        NoeFixture::new("run-isolation", "account-isolation", "opening-second");
    let changed_opening_reference = changed_opening.commit_opening("3000");
    let mut other = NoeFixture::new("run-other", "account-other", "opening-other");
    let other_opening = other.commit_opening("500");
    let other_transition = other.commit_transition(
        1,
        MARKET_A,
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity: StableLogicalIdentityV1("order-other".to_owned()),
        },
        balance_delta("0", "-1", "1", "0", None),
    );

    let mut writer = LedgerWriterV1::new(policy());
    writer.apply_noe_result(&first_opening).unwrap();
    assert!(matches!(
        writer.apply_noe_result(&changed_opening_reference),
        Err(LedgerErrorV1::DifferentOpeningEvidence { existing, found })
            if existing == "opening-first" && found == "opening-second"
    ));
    assert!(matches!(
        writer.apply_noe_result(&other_transition),
        Err(LedgerErrorV1::AccountIsolation { .. })
    ));
    assert_eq!(writer.projection().unwrap().ledger_entry_seq, 1);

    let mut other_writer = LedgerWriterV1::new(policy());
    other_writer.apply_noe_result(&other_opening).unwrap();
    other_writer.apply_noe_result(&other_transition).unwrap();
    assert_eq!(
        other_writer
            .projection()
            .unwrap()
            .account_instance_key
            .account_id,
        "account-other"
    );
}

#[test]
fn boundary_failures_and_pre_opening_changes_have_zero_side_effects() {
    let mut fixture = NoeFixture::new("run-boundary", "account-boundary", "opening-boundary");
    let _opening = fixture.commit_opening("2000");
    let transition = fixture.commit_transition(
        1,
        MARKET_A,
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity: StableLogicalIdentityV1("order-boundary".to_owned()),
        },
        balance_delta("0", "-1", "1", "0", None),
    );
    let non_economic = fixture.commit_non_economic();

    let mut writer = LedgerWriterV1::new(policy());
    assert!(matches!(
        writer.apply_noe_result(&transition),
        Err(LedgerErrorV1::AccountTransitionBeforeOpening)
    ));
    assert_eq!(writer.projection(), None);
    assert!(matches!(
        writer.apply_noe_result(&non_economic),
        Err(LedgerErrorV1::NonEconomicRecord)
    ));
    assert_eq!(writer.projection(), None);

    let mut invalid_reference = non_economic;
    invalid_reference.record_ordinal = 99;
    assert!(matches!(
        writer.apply_noe_result(&invalid_reference),
        Err(LedgerErrorV1::NoeRead(
            DurableErrorV1::RecordOrdinalOutOfRange { .. }
        ))
    ));
    assert_eq!(writer.projection(), None);
}

#[test]
fn fixed_precision_parse_and_overflow_fail_without_advancing_the_ledger() {
    let mut invalid = NoeFixture::new("run-invalid", "account-invalid", "opening-invalid");
    let invalid_opening = invalid.commit_opening("not-a-decimal");
    let mut invalid_writer = LedgerWriterV1::new(policy());
    assert!(matches!(
        invalid_writer.apply_noe_result(&invalid_opening),
        Err(LedgerErrorV1::DecimalParse {
            field: "opening.available_pusd",
            ..
        })
    ));
    assert_eq!(invalid_writer.projection(), None);

    const DECIMAL_MAX: &str = "79228162514264337593543950335";
    let mut overflow = NoeFixture::new("run-overflow", "account-overflow", "opening-overflow");
    let max_opening = overflow.commit_opening(DECIMAL_MAX);
    let overflow_transition = overflow.commit_transition(
        1,
        MARKET_A,
        SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
            trade_identity: StableLogicalIdentityV1("trade-overflow".to_owned()),
            path_ordinal: 0,
        },
        balance_delta("1", "0", "0", "0", None),
    );
    let mut overflow_writer = LedgerWriterV1::new(policy());
    overflow_writer.apply_noe_result(&max_opening).unwrap();
    let before = overflow_writer.projection().unwrap();
    assert!(matches!(
        overflow_writer.apply_noe_result(&overflow_transition),
        Err(LedgerErrorV1::ArithmeticOverflow { .. })
    ));
    assert_eq!(overflow_writer.projection().unwrap(), before);
}
