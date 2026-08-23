use std::fs::{self, OpenOptions};
use std::io::Write;

use contracts::execution_normalization::{
    DurableSourceReferenceV1, ImmutableSourceReferenceV1, NoeResultReferenceV1,
    SimV2SourceRecordReferenceV1, SimV2SourceReferenceV1, SimV2SourceWatermarkV1,
    SourceNeutralAccountBalanceDeltaV1, SourceNeutralAccountTransitionKindV1,
};
use contracts::{AccountInstanceKeyV1, ExactDecimal, PlatformSourceV1, Sha256Hex, SimCallerRoleV1};
use execution_normalization::{
    DurableContextStoreV1, NormalizationBatchInputV1, SimV2ContextSeedV1,
    SourceNeutralEconomicEvidenceV1, SourceNeutralEvidenceKindV1, SourceNeutralEvidenceV1,
    SourceNeutralLogicalKeyV1,
};
use ledger::{
    AccountAdmissionPolicyV1, LedgerApplyOutcomeV1, LedgerDurableErrorV1, LedgerErrorV1,
    LedgerLocalEventIdentityV1, LedgerRequestIdentityV1, LedgerReservationOutcomeV1,
    LedgerSubmissionItemIdentityV1, LedgerWriterV1,
};
use tempfile::TempDir;

const ACTIVE_FILE_NAME: &str = "ledger-active-v1.jsonl";

struct NoeFixture {
    temporary: TempDir,
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
            implementation_identity: "ledger-stage-4-noe-fixture".to_owned(),
        };
        let store = DurableContextStoreV1::open_or_create_sim_v2(temporary.path(), seed).unwrap();
        Self {
            temporary,
            store,
            run_id: run_id.to_owned(),
            opening_evidence_identity,
            next_projection_batch_sequence: 1,
        }
    }

    fn commit_opening(&mut self, available_pusd: &str) -> NoeResultReferenceV1 {
        let opening_identity = self.opening_evidence_identity.clone();
        self.commit(SourceNeutralEvidenceKindV1::Economic {
            evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
                opening_evidence_identity: opening_identity,
                available_pusd: decimal(available_pusd),
            },
        })
    }

    fn commit_transition(
        &mut self,
        account_transition_seq: u64,
        transition: SourceNeutralAccountTransitionKindV1,
        balance_delta: SourceNeutralAccountBalanceDeltaV1,
    ) -> NoeResultReferenceV1 {
        self.commit(SourceNeutralEvidenceKindV1::Economic {
            evidence: SourceNeutralEconomicEvidenceV1::AccountTransition {
                account_transition_seq,
                economic_effective_at_utc_ms: 100 + account_transition_seq as i64,
                condition_id: None,
                transition,
                balance_delta: Box::new(balance_delta),
            },
        })
    }

    fn commit(&mut self, kind: SourceNeutralEvidenceKindV1) -> NoeResultReferenceV1 {
        let sequence = self.next_projection_batch_sequence;
        let watermark = SimV2SourceWatermarkV1 {
            run_id: self.run_id.clone(),
            projection_batch_sequence: sequence,
        };
        let context_reference = self.store.context().context_reference.clone();
        self.store
            .commit_batch(
                NormalizationBatchInputV1 {
                    source_watermark: watermark.clone(),
                    evidence: vec![SourceNeutralEvidenceV1 {
                        source_reference: SimV2SourceReferenceV1 {
                            context_reference,
                            watermark,
                            record: if sequence == 1 {
                                SimV2SourceRecordReferenceV1::AccountOpening
                            } else {
                                SimV2SourceRecordReferenceV1::OrderLifecycle {
                                    private_feedback_sequence: sequence,
                                    platform_event_at_utc_ms: 100 + sequence as i64,
                                    feedback_received_at_utc_ms: 200 + sequence as i64,
                                }
                            },
                        },
                        source_ordinal: 0,
                        logical_key: SourceNeutralLogicalKeyV1::EconomicEvidence {
                            evidence_identity: format!("stage4-evidence-{sequence}"),
                        },
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

fn policy(limit: &str) -> AccountAdmissionPolicyV1 {
    AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: decimal(limit),
    }
}

fn decimal(value: &str) -> ExactDecimal {
    ExactDecimal(value.to_owned())
}

fn balance_delta(
    cash: &str,
    available: &str,
    maker_rebate_receivable: &str,
) -> SourceNeutralAccountBalanceDeltaV1 {
    SourceNeutralAccountBalanceDeltaV1 {
        cash_pusd_delta: decimal(cash),
        available_pusd_delta: decimal(available),
        principal_encumbrance_pusd_delta: decimal("0"),
        immediate_taker_fee_encumbrance_pusd_delta: decimal("0"),
        outcome_balance_deltas: Vec::new(),
        maker_rebate_receivable_pusd_delta: decimal(maker_rebate_receivable),
        taker_rebate_receivable_pusd_delta: decimal("0"),
        maker_rebate_locked_remainder_pusd_delta: decimal("0"),
        taker_rebate_locked_remainder_pusd_delta: decimal("0"),
    }
}

#[test]
fn committed_effect_recovers_without_noe_and_keeps_both_idempotence_domains() {
    let durable = TempDir::new().unwrap();
    let mut noe = NoeFixture::new("self-contained");
    let opening = noe.commit_opening("100");
    let request = LedgerRequestIdentityV1("request-self-contained".to_owned());
    let event = LedgerLocalEventIdentityV1("event-submitted".to_owned());
    let submission = LedgerSubmissionItemIdentityV1 {
        global_request_id: "submission-self-contained".to_owned(),
        batch_item_ordinal: 0,
    };
    let condition = contracts::ConditionId("condition-shared".to_owned());

    let mut writer = LedgerWriterV1::create_durable(policy("80"), durable.path()).unwrap();
    assert_eq!(
        writer.apply_noe_result(&opening).unwrap(),
        LedgerApplyOutcomeV1::NewlyApplied {
            ledger_entry_seq: 1
        }
    );
    assert_eq!(
        writer
            .try_reserve_intent(&request, &condition, &decimal("5"), &decimal("1"))
            .unwrap(),
        LedgerReservationOutcomeV1::Reserved {
            ledger_entry_seq: 2
        }
    );
    assert_eq!(
        writer
            .record_submission_started(&event, &request, &submission)
            .unwrap(),
        LedgerApplyOutcomeV1::NewlyApplied {
            ledger_entry_seq: 3
        }
    );
    drop(writer);
    let noe_root = noe.temporary.path().to_path_buf();
    drop(noe);
    assert!(!noe_root.exists());

    let mut recovered = LedgerWriterV1::open_durable(durable.path()).unwrap();
    let projection = recovered.projection().unwrap();
    assert_eq!(projection.ledger_entry_seq, 3);
    assert_eq!(projection.ledger_local_prefix, 2);
    assert_eq!(projection.platform_source_prefix, 0);
    assert_eq!(
        projection.platform_confirmed_projection.cash_pusd,
        decimal("100")
    );
    assert_eq!(
        projection
            .local_pending_reservation_projection
            .principal_pusd,
        decimal("5")
    );
    assert_eq!(
        projection
            .local_pending_reservation_projection
            .after_send_unknown_immediate_fee_pusd,
        decimal("1")
    );
    assert_eq!(
        projection
            .local_pending_reservation_projection
            .after_send_unknown_count,
        1
    );
    assert_eq!(
        recovered
            .admission_policy()
            .per_market_all_in_commitment_limit_pusd,
        decimal("80")
    );
    assert_eq!(
        recovered.apply_noe_result(&opening).unwrap(),
        LedgerApplyOutcomeV1::AlreadyApplied {
            ledger_entry_seq: 1
        }
    );
    assert_eq!(
        recovered
            .record_submission_started(&event, &request, &submission)
            .unwrap(),
        LedgerApplyOutcomeV1::AlreadyApplied {
            ledger_entry_seq: 3
        }
    );
}

#[test]
fn capacity_rejection_is_durable_sticky_and_never_advances_ledger_sequence() {
    let durable = TempDir::new().unwrap();
    let mut noe = NoeFixture::new("sticky-rejection");
    let opening = noe.commit_opening("100");
    let rebate_accrual = noe.commit_transition(
        1,
        SourceNeutralAccountTransitionKindV1::MakerRebateAccrualFinalized,
        balance_delta("0", "0", "50"),
    );
    let rebate_payment = noe.commit_transition(
        2,
        SourceNeutralAccountTransitionKindV1::MakerRebatePaid {
            attribution_day_utc_ms: 86_400_000,
        },
        balance_delta("50", "50", "-50"),
    );
    let request = LedgerRequestIdentityV1("request-rejected".to_owned());
    let condition = contracts::ConditionId("condition-rejected".to_owned());
    let rejected = LedgerReservationOutcomeV1::InsufficientGlobalAvailableFunds;

    let mut writer = LedgerWriterV1::create_durable(policy("1000"), durable.path()).unwrap();
    writer.apply_noe_result(&opening).unwrap();
    assert_eq!(
        writer
            .try_reserve_intent(&request, &condition, &decimal("120"), &decimal("0"))
            .unwrap(),
        rejected
    );
    assert_eq!(writer.projection().unwrap().ledger_entry_seq, 1);
    writer.apply_noe_result(&rebate_accrual).unwrap();
    writer.apply_noe_result(&rebate_payment).unwrap();
    assert_eq!(
        writer
            .try_reserve_intent(&request, &condition, &decimal("120"), &decimal("0"))
            .unwrap(),
        rejected
    );
    assert_eq!(writer.projection().unwrap().ledger_entry_seq, 3);
    drop(writer);

    let mut recovered = LedgerWriterV1::open_durable(durable.path()).unwrap();
    assert_eq!(recovered.projection().unwrap().ledger_entry_seq, 3);
    assert_eq!(
        recovered
            .try_reserve_intent(&request, &condition, &decimal("120"), &decimal("0"))
            .unwrap(),
        rejected
    );
    assert_eq!(recovered.projection().unwrap().ledger_entry_seq, 3);
}

#[test]
fn active_complete_incomplete_and_duplicate_tails_recover_once() {
    let durable = TempDir::new().unwrap();
    let writer = LedgerWriterV1::create_durable(policy("77"), durable.path()).unwrap();
    drop(writer);
    let active = durable.path().join(ACTIVE_FILE_NAME);
    let complete_len = fs::metadata(&active).unwrap().len();

    OpenOptions::new()
        .append(true)
        .open(&active)
        .unwrap()
        .write_all(b"{\"incomplete\":")
        .unwrap();
    let recovered = LedgerWriterV1::open_durable(durable.path()).unwrap();
    assert_eq!(
        recovered
            .admission_policy()
            .per_market_all_in_commitment_limit_pusd,
        decimal("77")
    );
    drop(recovered);
    assert_eq!(fs::metadata(&active).unwrap().len(), complete_len);

    let complete_record = fs::read(&active).unwrap();
    OpenOptions::new()
        .append(true)
        .open(&active)
        .unwrap()
        .write_all(&complete_record)
        .unwrap();
    let recovered = LedgerWriterV1::open_durable(durable.path()).unwrap();
    assert_eq!(
        recovered
            .admission_policy()
            .per_market_all_in_commitment_limit_pusd,
        decimal("77")
    );
    drop(recovered);
    assert_eq!(fs::metadata(&active).unwrap().len(), complete_len);
}

#[test]
fn multiple_sealed_segments_keep_record_and_ledger_continuity() {
    let durable = TempDir::new().unwrap();
    let mut noe = NoeFixture::new("multiple-segments");
    let opening = noe.commit_opening("100");
    let request = LedgerRequestIdentityV1("request-segments".to_owned());
    let condition = contracts::ConditionId("condition-segments".to_owned());
    let release = LedgerLocalEventIdentityV1("release-segments".to_owned());

    let mut writer = LedgerWriterV1::create_durable(policy("100"), durable.path()).unwrap();
    writer.apply_noe_result(&opening).unwrap();
    let first = writer.seal_active_segment().unwrap().unwrap();
    assert_eq!(first.first_durable_record_seq, 1);
    assert_eq!(first.last_durable_record_seq, 2);
    assert_eq!(first.first_ledger_entry_seq, Some(1));
    assert_eq!(first.last_ledger_entry_seq, Some(1));
    writer
        .try_reserve_intent(&request, &condition, &decimal("10"), &decimal("1"))
        .unwrap();
    let second = writer.seal_active_segment().unwrap().unwrap();
    assert_eq!(second.first_durable_record_seq, 3);
    assert_eq!(second.last_durable_record_seq, 3);
    assert_eq!(second.first_ledger_entry_seq, Some(2));
    assert_eq!(second.last_ledger_entry_seq, Some(2));
    drop(writer);

    let mut recovered = LedgerWriterV1::open_durable(durable.path()).unwrap();
    assert_eq!(recovered.projection().unwrap().ledger_entry_seq, 2);
    recovered.release_before_send(&release, &request).unwrap();
    drop(recovered);
    let recovered = LedgerWriterV1::open_durable(durable.path()).unwrap();
    let projection = recovered.projection().unwrap();
    assert_eq!(projection.ledger_entry_seq, 3);
    assert_eq!(projection.ledger_local_prefix, 2);
    assert_eq!(
        projection
            .local_pending_reservation_projection
            .principal_pusd,
        decimal("0")
    );
}

#[test]
fn one_corrupted_sealed_byte_is_rejected_by_the_outer_sha() {
    let durable = TempDir::new().unwrap();
    let mut writer = LedgerWriterV1::create_durable(policy("100"), durable.path()).unwrap();
    let sealed = writer.seal_active_segment().unwrap().unwrap();
    drop(writer);
    let path = std::path::Path::new(&sealed.location);
    let mut bytes = fs::read(path).unwrap();
    bytes[0] ^= 1;
    fs::write(path, bytes).unwrap();

    assert!(matches!(
        LedgerWriterV1::open_durable(durable.path()),
        Err(LedgerErrorV1::Durable(
            LedgerDurableErrorV1::SealedSegmentHashMismatch { .. }
        ))
    ));
}
