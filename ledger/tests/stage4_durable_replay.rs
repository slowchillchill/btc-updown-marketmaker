use std::fs::{self, OpenOptions};
use std::io::Write;

use contracts::execution_normalization::{
    DurableSourceReferenceV1, ImmutableSourceReferenceV1, NoeResultReferenceV1,
    SimV2DirectResponseAssociationV1, SimV2SourceRecordReferenceV1, SimV2SourceWatermarkV1,
    SourceNeutralAccountBalanceDeltaV1, SourceNeutralAccountTransitionKindV1,
    SourceNeutralControlResultV1, SourceNeutralOrderLifecycleEventV1,
    SourceNeutralOrderLifecycleResultV1, SourceNeutralOrderQueryResultV1,
    SourceNeutralOrderSnapshotV1, SourceNeutralOrderStateV1, SourceNeutralOutcomeBalanceDeltaV1,
    SourceNeutralSettlementLifecycleResultV1, SourceNeutralSubmissionOutcomeV1,
    SourceNeutralTradeEconomicDispositionV1, SourceNeutralTradeLifecycleResultV1,
    SourceNeutralTradeStateV1, StableLogicalIdentityV1,
};
use contracts::{
    AccountInstanceKeyV1, ConditionId, ExactDecimal, PlatformSourceV1, Sha256Hex, SimCallerRoleV1,
    TokenId,
};
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

    fn commit_market_transition(
        &mut self,
        account_transition_seq: u64,
        condition_id: &str,
        transition: SourceNeutralAccountTransitionKindV1,
        balance_delta: SourceNeutralAccountBalanceDeltaV1,
    ) -> NoeResultReferenceV1 {
        self.commit(SourceNeutralEvidenceKindV1::Economic {
            evidence: SourceNeutralEconomicEvidenceV1::AccountTransition {
                account_transition_seq,
                economic_effective_at_utc_ms: 100 + account_transition_seq as i64,
                condition_id: Some(ConditionId(condition_id.to_owned())),
                transition,
                balance_delta: Box::new(balance_delta),
            },
        })
    }

    fn commit_submission(
        &mut self,
        global_request_id: &str,
        outcome: SourceNeutralSubmissionOutcomeV1,
    ) -> NoeResultReferenceV1 {
        let sequence = self.next_projection_batch_sequence;
        self.commit_record(
            direct_response(
                sequence,
                SimV2DirectResponseAssociationV1::SubmissionItem {
                    global_request_id: global_request_id.to_owned(),
                    batch_item_ordinal: 0,
                },
            ),
            SourceNeutralLogicalKeyV1::SubmissionItem {
                global_request_id: global_request_id.to_owned(),
                batch_item_ordinal: 0,
            },
            SourceNeutralEvidenceKindV1::Control {
                result: SourceNeutralControlResultV1::Submission { outcome },
            },
        )
    }

    fn commit_query(
        &mut self,
        global_request_id: &str,
        result: SourceNeutralOrderQueryResultV1,
    ) -> NoeResultReferenceV1 {
        let sequence = self.next_projection_batch_sequence;
        self.commit_record(
            direct_response(
                sequence,
                SimV2DirectResponseAssociationV1::Request {
                    global_request_id: global_request_id.to_owned(),
                },
            ),
            SourceNeutralLogicalKeyV1::DirectResponseRequest {
                global_request_id: global_request_id.to_owned(),
            },
            SourceNeutralEvidenceKindV1::Control {
                result: SourceNeutralControlResultV1::OrderQuery { result },
            },
        )
    }

    fn commit_order_lifecycle(
        &mut self,
        order_identity: &str,
        condition_id: &str,
        token_id: &str,
        event: SourceNeutralOrderLifecycleEventV1,
    ) -> NoeResultReferenceV1 {
        let sequence = self.next_projection_batch_sequence;
        self.commit_record(
            SimV2SourceRecordReferenceV1::OrderLifecycle {
                private_feedback_sequence: sequence,
                platform_event_at_utc_ms: 2_000 + sequence as i64,
                feedback_received_at_utc_ms: 2_100 + sequence as i64,
            },
            SourceNeutralLogicalKeyV1::OrderLifecycle {
                order_identity: order_identity.to_owned(),
                occurrence: sequence,
            },
            SourceNeutralEvidenceKindV1::OrderLifecycle {
                result: SourceNeutralOrderLifecycleResultV1 {
                    order_identity: identity(order_identity),
                    condition_id: ConditionId(condition_id.to_owned()),
                    token_id: TokenId(token_id.to_owned()),
                    event,
                },
            },
        )
    }

    fn commit_trade(
        &mut self,
        trade_identity: &str,
        order_identity: &str,
        condition_id: &str,
        token_id: &str,
        disposition: SourceNeutralTradeEconomicDispositionV1,
    ) -> NoeResultReferenceV1 {
        let sequence = self.next_projection_batch_sequence;
        self.commit_record(
            SimV2SourceRecordReferenceV1::TradeLifecycle {
                private_feedback_sequence: sequence,
                platform_event_at_utc_ms: 3_000 + sequence as i64,
                feedback_received_at_utc_ms: 3_100 + sequence as i64,
            },
            SourceNeutralLogicalKeyV1::TradeLifecycle {
                trade_identity: trade_identity.to_owned(),
                occurrence: sequence,
            },
            SourceNeutralEvidenceKindV1::TradeLifecycle {
                result: SourceNeutralTradeLifecycleResultV1 {
                    trade_identity: identity(trade_identity),
                    order_identity: identity(order_identity),
                    condition_id: ConditionId(condition_id.to_owned()),
                    token_id: TokenId(token_id.to_owned()),
                    state: SourceNeutralTradeStateV1::Matched,
                    economic_disposition: disposition,
                },
            },
        )
    }

    fn commit_settlement(
        &mut self,
        result: SourceNeutralSettlementLifecycleResultV1,
    ) -> NoeResultReferenceV1 {
        let sequence = self.next_projection_batch_sequence;
        self.commit_record(
            SimV2SourceRecordReferenceV1::SettlementLifecycle {
                private_feedback_sequence: sequence,
                platform_event_at_utc_ms: 4_000 + sequence as i64,
                feedback_received_at_utc_ms: 4_100 + sequence as i64,
            },
            SourceNeutralLogicalKeyV1::SettlementLifecycle {
                private_feedback_sequence: sequence,
            },
            SourceNeutralEvidenceKindV1::SettlementLifecycle { result },
        )
    }

    fn commit(&mut self, kind: SourceNeutralEvidenceKindV1) -> NoeResultReferenceV1 {
        let sequence = self.next_projection_batch_sequence;
        self.commit_record(
            if sequence == 1 {
                SimV2SourceRecordReferenceV1::AccountOpening
            } else {
                SimV2SourceRecordReferenceV1::OrderLifecycle {
                    private_feedback_sequence: sequence,
                    platform_event_at_utc_ms: 100 + sequence as i64,
                    feedback_received_at_utc_ms: 200 + sequence as i64,
                }
            },
            SourceNeutralLogicalKeyV1::EconomicEvidence {
                evidence_identity: format!("stage4-evidence-{sequence}"),
            },
            kind,
        )
    }

    fn commit_record(
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

fn direct_response(
    sequence: u64,
    association: SimV2DirectResponseAssociationV1,
) -> SimV2SourceRecordReferenceV1 {
    SimV2SourceRecordReferenceV1::DirectResponse {
        private_feedback_sequence: sequence,
        platform_responded_at_utc_ms: 1_000 + sequence as i64,
        response_received_at_utc_ms: 1_100 + sequence as i64,
        association,
    }
}

fn identity(value: &str) -> StableLogicalIdentityV1 {
    StableLogicalIdentityV1(value.to_owned())
}

fn order(
    order_identity: &str,
    condition_id: &str,
    token_id: &str,
    state: SourceNeutralOrderStateV1,
) -> SourceNeutralOrderSnapshotV1 {
    SourceNeutralOrderSnapshotV1 {
        order_identity: identity(order_identity),
        condition_id: ConditionId(condition_id.to_owned()),
        token_id: TokenId(token_id.to_owned()),
        state,
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
    economic_delta(cash, available, "0", "0", None, maker_rebate_receivable)
}

fn economic_delta(
    cash: &str,
    available: &str,
    principal: &str,
    immediate_fee: &str,
    outcome: Option<(&str, &str)>,
    maker_rebate_receivable: &str,
) -> SourceNeutralAccountBalanceDeltaV1 {
    SourceNeutralAccountBalanceDeltaV1 {
        cash_pusd_delta: decimal(cash),
        available_pusd_delta: decimal(available),
        principal_encumbrance_pusd_delta: decimal(principal),
        immediate_taker_fee_encumbrance_pusd_delta: decimal(immediate_fee),
        outcome_balance_deltas: outcome
            .into_iter()
            .map(|(token_id, shares)| SourceNeutralOutcomeBalanceDeltaV1 {
                token_id: TokenId(token_id.to_owned()),
                balance_shares_delta: decimal(shares),
            })
            .collect(),
        maker_rebate_receivable_pusd_delta: decimal(maker_rebate_receivable),
        taker_rebate_receivable_pusd_delta: decimal("0"),
        maker_rebate_locked_remainder_pusd_delta: decimal("0"),
        taker_rebate_locked_remainder_pusd_delta: decimal("0"),
    }
}

#[test]
fn committed_effect_recovers_without_noe_and_keeps_both_idempotence_domains() {
    const MARKET_A: &str = "condition-golden-a";
    const MARKET_B: &str = "condition-golden-b";
    const TOKEN_A: &str = "token-golden-a";
    const TOKEN_B: &str = "token-golden-b";
    let durable = TempDir::new().unwrap();
    let mut noe = NoeFixture::new("self-contained");
    let opening = noe.commit_opening("100");
    let order_open = noe.commit_order_lifecycle(
        "order-golden-a",
        MARKET_A,
        TOKEN_A,
        SourceNeutralOrderLifecycleEventV1::Accepted {
            state: SourceNeutralOrderStateV1::Open,
        },
    );
    let trade_pending = noe.commit_trade(
        "trade-golden-a",
        "order-golden-a",
        MARKET_A,
        TOKEN_A,
        SourceNeutralTradeEconomicDispositionV1::PendingApplied,
    );
    let accepted_a = noe.commit_market_transition(
        1,
        MARKET_A,
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity: identity("order-golden-a"),
        },
        economic_delta("0", "-11", "10", "1", None, "0"),
    );
    let fill_a = noe.commit_market_transition(
        2,
        MARKET_A,
        SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
            trade_identity: identity("trade-golden-a"),
            path_ordinal: 0,
        },
        economic_delta("-11", "0", "-10", "-1", Some((TOKEN_A, "20")), "0"),
    );
    let accepted_b = noe.commit_market_transition(
        3,
        MARKET_B,
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity: identity("order-golden-b"),
        },
        economic_delta("0", "-10", "10", "0", None, "0"),
    );
    let fill_b = noe.commit_market_transition(
        4,
        MARKET_B,
        SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
            trade_identity: identity("trade-golden-b"),
            path_ordinal: 0,
        },
        economic_delta("-10", "0", "-10", "0", Some((TOKEN_B, "20")), "0"),
    );
    let rebate = noe.commit_market_transition(
        5,
        MARKET_B,
        SourceNeutralAccountTransitionKindV1::MakerRebateAccrualFinalized,
        economic_delta("0", "0", "0", "0", None, "2"),
    );
    let settlement = noe.commit_settlement(
        SourceNeutralSettlementLifecycleResultV1::AutoRedeemApplied {
            condition_id: ConditionId(MARKET_B.to_owned()),
            resolution_identity: identity("resolution-golden-b"),
            resolution_source_event_at_utc_ms: 9_000,
        },
    );
    let redeem = noe.commit_market_transition(
        6,
        MARKET_B,
        SourceNeutralAccountTransitionKindV1::AutoRedeemApplied {
            resolution_identity: identity("resolution-golden-b"),
        },
        economic_delta("15", "15", "0", "0", Some((TOKEN_B, "-20")), "0"),
    );
    let submission_request = LedgerRequestIdentityV1("request-submission".to_owned());
    let query_request = LedgerRequestIdentityV1("request-query".to_owned());
    let submission_event = LedgerLocalEventIdentityV1("event-submission".to_owned());
    let query_submission_event = LedgerLocalEventIdentityV1("event-query-submission".to_owned());
    let query_association_event = LedgerLocalEventIdentityV1("event-query-association".to_owned());
    let submission_identity = LedgerSubmissionItemIdentityV1 {
        global_request_id: "submission-golden".to_owned(),
        batch_item_ordinal: 0,
    };
    let query_submission_identity = LedgerSubmissionItemIdentityV1 {
        global_request_id: "query-submission-golden".to_owned(),
        batch_item_ordinal: 0,
    };

    let mut writer = LedgerWriterV1::create_durable(policy("80"), durable.path()).unwrap();
    writer.apply_noe_result(&opening).unwrap();
    writer
        .try_reserve_intent(
            &submission_request,
            &ConditionId(MARKET_A.to_owned()),
            &decimal("5"),
            &decimal("1"),
        )
        .unwrap();
    writer
        .record_submission_started(&submission_event, &submission_request, &submission_identity)
        .unwrap();
    writer
        .try_reserve_intent(
            &query_request,
            &ConditionId(MARKET_B.to_owned()),
            &decimal("4"),
            &decimal("0.5"),
        )
        .unwrap();
    writer
        .record_submission_started(
            &query_submission_event,
            &query_request,
            &query_submission_identity,
        )
        .unwrap();
    writer
        .record_order_query_association(&query_association_event, "query-golden", &query_request)
        .unwrap();
    for reference in [
        &order_open,
        &trade_pending,
        &accepted_a,
        &fill_a,
        &accepted_b,
        &fill_b,
        &rebate,
        &settlement,
        &redeem,
    ] {
        writer.apply_noe_result(reference).unwrap();
    }
    drop(writer);
    let noe_root = noe.temporary.path().to_path_buf();
    drop(noe);
    assert!(!noe_root.exists());

    let mut recovered = LedgerWriterV1::open_durable(durable.path()).unwrap();
    let projection = recovered.projection().unwrap();
    assert_eq!(projection.ledger_entry_seq, 15);
    assert_eq!(projection.ledger_local_prefix, 5);
    assert_eq!(projection.platform_source_prefix, 6);
    assert_eq!(projection.available_through_utc_ms, 1_010);
    assert_eq!(
        projection.platform_confirmed_projection.cash_pusd,
        decimal("94")
    );
    assert_eq!(
        projection.platform_confirmed_projection.available_pusd,
        decimal("94")
    );
    assert_eq!(
        projection
            .platform_confirmed_projection
            .principal_encumbrance_pusd,
        decimal("0")
    );
    assert_eq!(
        projection
            .platform_confirmed_projection
            .immediate_taker_fee_encumbrance_pusd,
        decimal("0")
    );
    assert_eq!(
        projection
            .platform_confirmed_projection
            .position_cost_basis_pusd,
        decimal("10")
    );
    assert_eq!(
        projection.platform_confirmed_projection.fees_pusd,
        decimal("1")
    );
    assert_eq!(
        projection
            .platform_confirmed_projection
            .maker_rebate_receivable_pusd,
        decimal("2")
    );
    assert_eq!(
        projection.platform_confirmed_projection.realized_pnl_pusd,
        decimal("7")
    );
    assert_eq!(projection.platform_confirmed_projection.open_order_count, 1);
    assert_eq!(
        projection.platform_confirmed_projection.pending_fill_count,
        1
    );
    assert_eq!(
        projection
            .platform_confirmed_projection
            .cumulative_accepted_principal_pusd,
        decimal("20")
    );
    assert_eq!(
        projection
            .local_pending_reservation_projection
            .principal_pusd,
        decimal("9")
    );
    assert_eq!(
        projection
            .local_pending_reservation_projection
            .after_send_unknown_immediate_fee_pusd,
        decimal("1.5")
    );
    assert_eq!(
        projection
            .local_pending_reservation_projection
            .after_send_unknown_count,
        2
    );
    let token_a = projection
        .platform_confirmed_projection
        .outcome_balances
        .iter()
        .find(|outcome| outcome.token_id.0 == TOKEN_A)
        .unwrap();
    assert_eq!(token_a.balance_shares, decimal("20"));
    assert_eq!(token_a.position_cost_basis_pusd, decimal("10"));
    let token_b = projection
        .platform_confirmed_projection
        .outcome_balances
        .iter()
        .find(|outcome| outcome.token_id.0 == TOKEN_B)
        .unwrap();
    assert_eq!(token_b.balance_shares, decimal("0"));
    assert_eq!(token_b.position_cost_basis_pusd, decimal("0"));
    let market_a = projection
        .market_projections
        .iter()
        .find(|market| market.condition_id.0 == MARKET_A)
        .unwrap();
    assert_eq!(market_a.position_cost_basis_pusd, decimal("10"));
    assert_eq!(market_a.fees_pusd, decimal("1"));
    assert_eq!(market_a.realized_pnl_pusd, decimal("0"));
    assert_eq!(market_a.open_order_count, 1);
    assert_eq!(market_a.pending_fill_count, 1);
    assert_eq!(market_a.cumulative_accepted_principal_pusd, decimal("10"));
    assert_eq!(market_a.outcome_balances.len(), 1);
    assert_eq!(market_a.outcome_balances[0].token_id.0, TOKEN_A);
    assert_eq!(market_a.outcome_balances[0].balance_shares, decimal("20"));
    assert_eq!(
        market_a.outcome_balances[0].position_cost_basis_pusd,
        decimal("10")
    );
    let market_b = projection
        .market_projections
        .iter()
        .find(|market| market.condition_id.0 == MARKET_B)
        .unwrap();
    assert_eq!(market_b.position_cost_basis_pusd, decimal("0"));
    assert_eq!(market_b.fees_pusd, decimal("0"));
    assert_eq!(market_b.maker_rebate_receivable_pusd, decimal("2"));
    assert_eq!(market_b.realized_pnl_pusd, decimal("7"));
    assert_eq!(market_b.cumulative_accepted_principal_pusd, decimal("10"));
    assert_eq!(market_b.outcome_balances.len(), 1);
    assert_eq!(market_b.outcome_balances[0].token_id.0, TOKEN_B);
    assert_eq!(market_b.outcome_balances[0].balance_shares, decimal("0"));
    assert_eq!(
        market_b.outcome_balances[0].position_cost_basis_pusd,
        decimal("0")
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
            .record_submission_started(
                &submission_event,
                &submission_request,
                &submission_identity,
            )
            .unwrap(),
        LedgerApplyOutcomeV1::AlreadyApplied {
            ledger_entry_seq: 3
        }
    );

    let mut post_recovery_noe = NoeFixture::new("self-contained");
    post_recovery_noe.commit_opening("100");
    let accepted_submission = post_recovery_noe.commit_submission(
        "submission-golden",
        SourceNeutralSubmissionOutcomeV1::Accepted {
            order: order(
                "order-from-submission",
                MARKET_A,
                TOKEN_A,
                SourceNeutralOrderStateV1::Open,
            ),
        },
    );
    let found_query = post_recovery_noe.commit_query(
        "query-golden",
        SourceNeutralOrderQueryResultV1::Found {
            order: order(
                "order-from-query",
                MARKET_B,
                TOKEN_B,
                SourceNeutralOrderStateV1::Open,
            ),
        },
    );
    let order_terminal = post_recovery_noe.commit_order_lifecycle(
        "order-golden-a",
        MARKET_A,
        TOKEN_A,
        SourceNeutralOrderLifecycleEventV1::FakUnspentReleased,
    );
    let trade_terminal = post_recovery_noe.commit_trade(
        "trade-golden-a",
        "order-golden-a",
        MARKET_A,
        TOKEN_A,
        SourceNeutralTradeEconomicDispositionV1::FinalSuccess,
    );
    let compensation = post_recovery_noe.commit_market_transition(
        7,
        MARKET_A,
        SourceNeutralAccountTransitionKindV1::FillFinalityCompensationApplied {
            trade_identity: identity("trade-golden-a"),
            path_ordinal: 0,
        },
        economic_delta("11", "0", "10", "1", Some((TOKEN_A, "-20")), "0"),
    );
    for reference in [
        &accepted_submission,
        &found_query,
        &order_terminal,
        &trade_terminal,
        &compensation,
    ] {
        assert!(matches!(
            recovered.apply_noe_result(reference).unwrap(),
            LedgerApplyOutcomeV1::NewlyApplied { .. }
        ));
    }
    let after_reachable_index_uses = recovered.projection().unwrap();
    assert_eq!(after_reachable_index_uses.ledger_entry_seq, 20);
    assert_eq!(
        after_reachable_index_uses
            .local_pending_reservation_projection
            .after_send_unknown_count,
        0
    );
    assert_eq!(
        after_reachable_index_uses
            .platform_confirmed_projection
            .platform_confirmation_pending_economic_count,
        2
    );
    assert_eq!(
        after_reachable_index_uses
            .platform_confirmed_projection
            .open_order_count,
        2
    );
    assert_eq!(
        after_reachable_index_uses
            .platform_confirmed_projection
            .pending_fill_count,
        0
    );
    assert_eq!(
        after_reachable_index_uses
            .platform_confirmed_projection
            .position_cost_basis_pusd,
        decimal("0")
    );
    assert_eq!(
        after_reachable_index_uses
            .platform_confirmed_projection
            .fees_pusd,
        decimal("0")
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

#[test]
fn legacy_noe_reference_digest_in_jsonl_reopens_and_deduplicates_by_position() {
    let durable = TempDir::new().unwrap();
    let mut noe = NoeFixture::new("legacy-reference");
    let reference = noe.commit_opening("100");
    let mut writer = LedgerWriterV1::create_durable(policy("1000"), durable.path()).unwrap();
    writer.apply_noe_result(&reference).unwrap();
    let before = writer.projection().unwrap();
    drop(writer);
    let active = durable.path().join(ACTIVE_FILE_NAME);
    let mut inserted = 0;
    fn add_old_field(value: &mut serde_json::Value, inserted: &mut usize) {
        match value {
            serde_json::Value::Object(fields) => {
                if let Some(container) = fields
                    .get_mut("commit_container")
                    .and_then(serde_json::Value::as_object_mut)
                {
                    container.insert("boundary_sha256".into(), serde_json::json!("a".repeat(64)));
                    *inserted += 1;
                }
                for value in fields.values_mut() {
                    add_old_field(value, inserted);
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    add_old_field(value, inserted);
                }
            }
            _ => {}
        }
    }
    let mut lines = Vec::new();
    for line in fs::read_to_string(&active).unwrap().lines() {
        let mut value: serde_json::Value = serde_json::from_str(line).unwrap();
        add_old_field(&mut value, &mut inserted);
        serde_json::to_writer(&mut lines, &value).unwrap();
        lines.push(b'\n');
    }
    assert!(inserted > 0);
    fs::write(active, lines).unwrap();
    let mut restored = LedgerWriterV1::open_durable(durable.path()).unwrap();
    assert_eq!(restored.projection().unwrap(), before);
    assert!(matches!(
        restored.apply_noe_result(&reference).unwrap(),
        LedgerApplyOutcomeV1::AlreadyApplied { .. }
    ));
    assert_eq!(restored.projection().unwrap(), before);
}
