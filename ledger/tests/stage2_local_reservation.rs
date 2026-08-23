use contracts::execution_normalization::{
    DurableSourceReferenceV1, ImmutableSourceReferenceV1, NoeResultReferenceV1,
    SimV2SourceRecordReferenceV1, SimV2SourceReferenceV1, SimV2SourceWatermarkV1,
    SourceNeutralAccountBalanceDeltaV1, SourceNeutralAccountTransitionKindV1,
    StableLogicalIdentityV1,
};
use contracts::{
    AccountInstanceKeyV1, ConditionId, ExactDecimal, PlatformSourceV1, Sha256Hex, SimCallerRoleV1,
};
use execution_normalization::{
    DurableContextStoreV1, NormalizationBatchInputV1, SimV2ContextSeedV1,
    SourceNeutralEconomicEvidenceV1, SourceNeutralEvidenceKindV1, SourceNeutralEvidenceV1,
    SourceNeutralLogicalKeyV1,
};
use ledger::{
    AccountAdmissionPolicyV1, LedgerApplyOutcomeV1, LedgerErrorV1, LedgerLocalEventIdentityV1,
    LedgerMarketProjectionV1, LedgerRequestIdentityV1, LedgerReservationOutcomeV1,
    LedgerSubmissionItemIdentityV1, LedgerWriterV1, ReserveIntentV1,
};
use tempfile::TempDir;

const MARKET_A: &str = "condition-a";
const MARKET_B: &str = "condition-b";
const MARKET_C: &str = "condition-c";

fn submission(global_request_id: &str) -> LedgerSubmissionItemIdentityV1 {
    LedgerSubmissionItemIdentityV1 {
        global_request_id: global_request_id.to_owned(),
        batch_item_ordinal: 0,
    }
}

struct NoeFixture {
    _temporary: TempDir,
    store: DurableContextStoreV1,
    run_id: String,
    opening_evidence_identity: String,
    next_projection_batch_sequence: u64,
}

impl NoeFixture {
    fn new(run_id: &str) -> Self {
        let temporary = TempDir::new().unwrap();
        let opening_evidence_identity = format!("opening-{run_id}");
        let seed = SimV2ContextSeedV1 {
            run_id: run_id.to_owned(),
            account_instance_key: AccountInstanceKeyV1 {
                run_id: run_id.to_owned(),
                caller_role: SimCallerRoleV1::ModelTrain,
                caller_instance_id: format!("caller-{run_id}"),
                account_id: format!("account-{run_id}"),
                platform_source: PlatformSourceV1::Simulated,
            },
            run_opened: DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Sha256(Sha256Hex("1".repeat(64))),
            },
            account_opening: DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Sha256(Sha256Hex("2".repeat(64))),
            },
            account_opening_evidence_identity: opening_evidence_identity.clone(),
            implementation_identity: "ledger-stage-2-noe-fixture".to_owned(),
        };
        let store = DurableContextStoreV1::open_or_create_sim_v2(temporary.path(), seed).unwrap();
        Self {
            _temporary: temporary,
            store,
            run_id: run_id.to_owned(),
            opening_evidence_identity,
            next_projection_batch_sequence: 1,
        }
    }

    fn commit_opening(&mut self, available_pusd: &str) -> NoeResultReferenceV1 {
        let sequence = self.next_projection_batch_sequence;
        self.commit(
            SimV2SourceRecordReferenceV1::AccountOpening,
            format!("opening-{sequence}"),
            SourceNeutralEconomicEvidenceV1::AccountOpening {
                opening_evidence_identity: self.opening_evidence_identity.clone(),
                available_pusd: decimal(available_pusd),
            },
        )
    }

    fn commit_platform_encumbrance(
        &mut self,
        account_transition_seq: u64,
        condition_id: &str,
        available_delta: &str,
        principal_delta: &str,
        immediate_fee_delta: &str,
    ) -> NoeResultReferenceV1 {
        let sequence = self.next_projection_batch_sequence;
        self.commit(
            SimV2SourceRecordReferenceV1::OrderLifecycle {
                private_feedback_sequence: sequence,
                platform_event_at_utc_ms: 86_400_000 + sequence as i64,
                feedback_received_at_utc_ms: 86_400_100 + sequence as i64,
            },
            format!("encumbrance-{sequence}"),
            SourceNeutralEconomicEvidenceV1::AccountTransition {
                account_transition_seq,
                economic_effective_at_utc_ms: 86_400_000 + sequence as i64,
                condition_id: Some(ConditionId(condition_id.to_owned())),
                transition: SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
                    order_identity: StableLogicalIdentityV1(format!("order-{sequence}")),
                },
                balance_delta: Box::new(SourceNeutralAccountBalanceDeltaV1 {
                    cash_pusd_delta: decimal("0"),
                    available_pusd_delta: decimal(available_delta),
                    principal_encumbrance_pusd_delta: decimal(principal_delta),
                    immediate_taker_fee_encumbrance_pusd_delta: decimal(immediate_fee_delta),
                    outcome_balance_deltas: Vec::new(),
                    maker_rebate_receivable_pusd_delta: decimal("0"),
                    taker_rebate_receivable_pusd_delta: decimal("0"),
                    maker_rebate_locked_remainder_pusd_delta: decimal("0"),
                    taker_rebate_locked_remainder_pusd_delta: decimal("0"),
                }),
            },
        )
    }

    fn commit(
        &mut self,
        record: SimV2SourceRecordReferenceV1,
        evidence_identity: String,
        evidence: SourceNeutralEconomicEvidenceV1,
    ) -> NoeResultReferenceV1 {
        let sequence = self.next_projection_batch_sequence;
        let watermark = SimV2SourceWatermarkV1 {
            run_id: self.run_id.clone(),
            projection_batch_sequence: sequence,
        };
        self.store
            .commit_batch(
                NormalizationBatchInputV1 {
                    source_watermark: watermark.clone(),
                    evidence: vec![SourceNeutralEvidenceV1 {
                        source_reference: SimV2SourceReferenceV1 {
                            context_reference: self.store.context().context_reference.clone(),
                            watermark,
                            record,
                        },
                        source_ordinal: 0,
                        logical_key: SourceNeutralLogicalKeyV1::EconomicEvidence {
                            evidence_identity,
                        },
                        kind: SourceNeutralEvidenceKindV1::Economic { evidence },
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

fn decimal(value: &str) -> ExactDecimal {
    ExactDecimal(value.to_owned())
}

fn request(value: &str) -> LedgerRequestIdentityV1 {
    LedgerRequestIdentityV1(value.to_owned())
}

fn event(value: &str) -> LedgerLocalEventIdentityV1 {
    LedgerLocalEventIdentityV1(value.to_owned())
}

fn policy(limit: &str) -> AccountAdmissionPolicyV1 {
    AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: decimal(limit),
    }
}

fn intent(
    request_identity: &str,
    condition_id: &str,
    principal: &str,
    fee: &str,
) -> ReserveIntentV1 {
    ReserveIntentV1 {
        request_identity: request(request_identity),
        condition_id: ConditionId(condition_id.to_owned()),
        requested_principal_pusd: decimal(principal),
        immediate_fee_reserve_pusd: decimal(fee),
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
fn stale_readers_and_stable_batch_use_actual_current_global_and_market_capacity() {
    let mut fixture = NoeFixture::new("linearized-capacity");
    let opening = fixture.commit_opening("2000");
    let platform_encumbrance =
        fixture.commit_platform_encumbrance(1, MARKET_A, "-210", "200", "10");
    let mut writer = LedgerWriterV1::new(policy("1000"));
    writer.apply_noe_result(&opening).unwrap();
    writer.apply_noe_result(&platform_encumbrance).unwrap();
    let shared_old_projection = writer.projection().unwrap();

    let results = writer.try_reserve_intents(&[
        intent("caller-1/request-a", MARKET_A, "700", "50"),
        intent("caller-2/request-a", MARKET_A, "50", "1"),
        intent("caller-3/request-b", MARKET_B, "850", "50"),
        intent("caller-4/request-c", MARKET_C, "150", "1"),
    ]);
    assert_eq!(
        results[0].as_ref().unwrap(),
        &LedgerReservationOutcomeV1::Reserved {
            ledger_entry_seq: 3
        }
    );
    assert_eq!(
        results[1].as_ref().unwrap(),
        &LedgerReservationOutcomeV1::PerMarketLimitExceeded
    );
    assert_eq!(
        results[2].as_ref().unwrap(),
        &LedgerReservationOutcomeV1::Reserved {
            ledger_entry_seq: 4
        }
    );
    assert_eq!(
        results[3].as_ref().unwrap(),
        &LedgerReservationOutcomeV1::InsufficientGlobalAvailableFunds
    );

    let projection = writer.projection().unwrap();
    assert_eq!(projection.ledger_entry_seq, 4);
    assert_eq!(projection.ledger_local_prefix, 2);
    assert_eq!(projection.platform_source_prefix, 1);
    assert_eq!(projection.available_through_utc_ms, 1_002);
    assert_eq!(
        projection.platform_confirmed_projection,
        shared_old_projection.platform_confirmed_projection
    );
    assert_eq!(
        projection
            .local_pending_reservation_projection
            .principal_pusd,
        decimal("1550")
    );
    assert_eq!(
        projection
            .local_pending_reservation_projection
            .immediate_fee_pusd,
        decimal("100")
    );
    assert_eq!(projection.market_projections.len(), 2);
    assert_eq!(projection.market_projections[0].condition_id.0, MARKET_A);
    assert_eq!(projection.market_projections[1].condition_id.0, MARKET_B);
    let market_a = market(&projection.market_projections, MARKET_A);
    assert_eq!(market_a.principal_encumbrance_pusd, decimal("200"));
    assert_eq!(market_a.immediate_taker_fee_encumbrance_pusd, decimal("10"));
    assert_eq!(market_a.local_pending_principal_pusd, decimal("700"));
    assert_eq!(market_a.local_pending_immediate_fee_pusd, decimal("50"));
    let market_b = market(&projection.market_projections, MARKET_B);
    assert_eq!(market_b.local_pending_principal_pusd, decimal("850"));
    assert_eq!(market_b.local_pending_immediate_fee_pusd, decimal("50"));

    assert_eq!(
        writer
            .try_reserve_intent(
                &request("caller-2/request-a"),
                &ConditionId(MARKET_A.to_owned()),
                &decimal("50"),
                &decimal("1"),
            )
            .unwrap(),
        LedgerReservationOutcomeV1::PerMarketLimitExceeded
    );
    assert_eq!(writer.projection().unwrap(), projection);
}

#[test]
fn request_results_are_sticky_and_configuration_values_are_not_hidden_defaults() {
    let mut fixture = NoeFixture::new("configured-capacity");
    let opening = fixture.commit_opening("500");
    let mut writer = LedgerWriterV1::new(policy("100"));
    writer.apply_noe_result(&opening).unwrap();
    let first_request = request("day-1/file-a/batch-1/request-a");
    assert_eq!(
        writer
            .try_reserve_intent(
                &first_request,
                &ConditionId(MARKET_A.to_owned()),
                &decimal("80"),
                &decimal("10"),
            )
            .unwrap(),
        LedgerReservationOutcomeV1::Reserved {
            ledger_entry_seq: 2
        }
    );
    let after_first = writer.projection().unwrap();

    assert_eq!(
        writer
            .try_reserve_intent(
                &first_request,
                &ConditionId(MARKET_A.to_owned()),
                &decimal("80"),
                &decimal("10"),
            )
            .unwrap(),
        LedgerReservationOutcomeV1::Reserved {
            ledger_entry_seq: 2
        }
    );
    assert_eq!(writer.projection().unwrap(), after_first);

    let rejected_request = request("day-2/file-b/batch-2/request-a");
    assert_eq!(
        writer
            .try_reserve_intent(
                &rejected_request,
                &ConditionId(MARKET_A.to_owned()),
                &decimal("15"),
                &decimal("1"),
            )
            .unwrap(),
        LedgerReservationOutcomeV1::PerMarketLimitExceeded
    );
    assert_eq!(writer.projection().unwrap(), after_first);

    assert_eq!(
        writer
            .release_before_send(&event("release/day-2/file-b"), &first_request)
            .unwrap(),
        LedgerApplyOutcomeV1::NewlyApplied {
            ledger_entry_seq: 3
        }
    );
    assert_eq!(
        writer
            .try_reserve_intent(
                &rejected_request,
                &ConditionId(MARKET_A.to_owned()),
                &decimal("15"),
                &decimal("1"),
            )
            .unwrap(),
        LedgerReservationOutcomeV1::PerMarketLimitExceeded
    );
    assert_eq!(writer.projection().unwrap().ledger_entry_seq, 3);

    assert_eq!(
        writer
            .try_reserve_intent(
                &request("day-2/file-b/batch-3/request-b"),
                &ConditionId(MARKET_B.to_owned()),
                &decimal("90"),
                &decimal("5"),
            )
            .unwrap(),
        LedgerReservationOutcomeV1::Reserved {
            ledger_entry_seq: 4
        }
    );
    let projection = writer.projection().unwrap();
    assert_eq!(
        projection.platform_confirmed_projection.cash_pusd,
        decimal("500")
    );
    assert_eq!(
        projection.platform_confirmed_projection.available_pusd,
        decimal("500")
    );
    assert_eq!(projection.ledger_local_prefix, 3);
    assert_eq!(
        projection
            .local_pending_reservation_projection
            .principal_pusd,
        decimal("90")
    );
}

#[test]
fn pre_send_release_and_submission_boundary_are_idempotent_and_causal() {
    let unopened_request = request("unopened-request");
    let mut unopened = LedgerWriterV1::new(policy("1000"));
    assert!(matches!(
        unopened.try_reserve_intent(
            &unopened_request,
            &ConditionId(MARKET_A.to_owned()),
            &decimal("10"),
            &decimal("1"),
        ),
        Err(LedgerErrorV1::AccountNotOpened)
    ));

    let mut fixture = NoeFixture::new("submission-boundary");
    let opening = fixture.commit_opening("2000");
    let mut writer = LedgerWriterV1::new(policy("1000"));
    writer.apply_noe_result(&opening).unwrap();
    let platform_only = writer.projection().unwrap();
    assert!(matches!(
        writer.record_submission_started(
            &event("missing-event"),
            &request("missing-request"),
            &submission("missing-submission"),
        ),
        Err(LedgerErrorV1::ReservationRequestNotFound { .. })
    ));
    assert_eq!(writer.projection().unwrap(), platform_only);

    let released_request = request("release-request");
    writer
        .try_reserve_intent(
            &released_request,
            &ConditionId(MARKET_A.to_owned()),
            &decimal("100"),
            &decimal("5"),
        )
        .unwrap();
    let release_event = event("release-event");
    assert_eq!(
        writer
            .release_before_send(&release_event, &released_request)
            .unwrap(),
        LedgerApplyOutcomeV1::NewlyApplied {
            ledger_entry_seq: 3
        }
    );
    assert_eq!(
        writer
            .release_before_send(&release_event, &released_request)
            .unwrap(),
        LedgerApplyOutcomeV1::AlreadyApplied {
            ledger_entry_seq: 3
        }
    );
    assert_eq!(
        writer
            .release_before_send(&event("second-release"), &released_request)
            .unwrap(),
        LedgerApplyOutcomeV1::AlreadyApplied {
            ledger_entry_seq: 3
        }
    );

    let retained_request = request("retained-request");
    assert_eq!(
        writer
            .try_reserve_intent(
                &retained_request,
                &ConditionId(MARKET_B.to_owned()),
                &decimal("200"),
                &decimal("5"),
            )
            .unwrap(),
        LedgerReservationOutcomeV1::Reserved {
            ledger_entry_seq: 4
        }
    );
    let submission_event = event("submission-started");
    assert_eq!(
        writer
            .record_submission_started(
                &submission_event,
                &retained_request,
                &submission("retained-submission"),
            )
            .unwrap(),
        LedgerApplyOutcomeV1::NewlyApplied {
            ledger_entry_seq: 5
        }
    );
    let after_submission = writer.projection().unwrap();
    assert_eq!(
        writer
            .record_submission_started(
                &submission_event,
                &retained_request,
                &submission("retained-submission"),
            )
            .unwrap(),
        LedgerApplyOutcomeV1::AlreadyApplied {
            ledger_entry_seq: 5
        }
    );
    assert_eq!(
        writer
            .release_before_send(&event("release-after-send"), &retained_request)
            .unwrap(),
        LedgerApplyOutcomeV1::AlreadyApplied {
            ledger_entry_seq: 5
        }
    );
    assert_eq!(writer.projection().unwrap(), after_submission);

    assert_eq!(after_submission.ledger_entry_seq, 5);
    assert_eq!(after_submission.ledger_local_prefix, 4);
    assert_eq!(after_submission.platform_source_prefix, 0);
    assert_eq!(
        after_submission.available_through_utc_ms,
        platform_only.available_through_utc_ms
    );
    assert_eq!(
        after_submission.platform_confirmed_projection,
        platform_only.platform_confirmed_projection
    );
    assert_eq!(after_submission.market_projections.len(), 2);
    assert_eq!(
        market(&after_submission.market_projections, MARKET_A).local_pending_principal_pusd,
        decimal("0")
    );
    assert_eq!(
        market(&after_submission.market_projections, MARKET_B).local_pending_principal_pusd,
        decimal("200")
    );
}

#[test]
fn identical_local_sequences_rebuild_deterministically() {
    let mut fixture = NoeFixture::new("deterministic-local");
    let opening = fixture.commit_opening("2000");
    let mut first = LedgerWriterV1::new(policy("1000"));
    let mut second = LedgerWriterV1::new(policy("1000"));
    first.apply_noe_result(&opening).unwrap();
    second.apply_noe_result(&opening).unwrap();
    let request_identity = request("deterministic-request");
    let local_event_identity = event("deterministic-submission");

    let first_reservation = first
        .try_reserve_intent(
            &request_identity,
            &ConditionId(MARKET_A.to_owned()),
            &decimal("123.45678901"),
            &decimal("0.10000000"),
        )
        .unwrap();
    let second_reservation = second
        .try_reserve_intent(
            &request_identity,
            &ConditionId(MARKET_A.to_owned()),
            &decimal("123.45678901"),
            &decimal("0.10000000"),
        )
        .unwrap();
    assert_eq!(first_reservation, second_reservation);
    assert_eq!(
        first
            .record_submission_started(
                &local_event_identity,
                &request_identity,
                &submission("deterministic-submission"),
            )
            .unwrap(),
        second
            .record_submission_started(
                &local_event_identity,
                &request_identity,
                &submission("deterministic-submission"),
            )
            .unwrap()
    );
    assert_eq!(first.projection().unwrap(), second.projection().unwrap());
}
