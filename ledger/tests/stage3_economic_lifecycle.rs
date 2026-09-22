use contracts::execution_normalization::{
    DurableSourceReferenceV1, ImmutableSourceReferenceV1, NoeResultReferenceV1,
    SimV2DirectResponseAssociationV1, SimV2SourceRecordReferenceV1, SimV2SourceWatermarkV1,
    SourceNeutralAccountBalanceDeltaV1, SourceNeutralAccountTransitionKindV1,
    SourceNeutralControlResultV1, SourceNeutralOrderLifecycleEventV1,
    SourceNeutralOrderLifecycleResultV1, SourceNeutralOrderQueryResultV1,
    SourceNeutralOrderSnapshotV1, SourceNeutralOrderStateV1, SourceNeutralOutcomeBalanceDeltaV1,
    SourceNeutralQueryOutOfRetentionV1, SourceNeutralSettlementLifecycleResultV1,
    SourceNeutralSubmissionOutcomeV1, SourceNeutralTradeEconomicDispositionV1,
    SourceNeutralTradeLifecycleResultV1, SourceNeutralTradeStateV1, StableLogicalIdentityV1,
};
use contracts::{
    AccountInstanceKeyV1, ConditionId, ExactDecimal, PlatformSourceV1, Sha256Hex, SimCallerRoleV1,
    TokenId,
};
use execution_normalization::{
    DurableContextStoreV1, NormalizationBatchInputV1, PendingScopeV1, SimV2ContextSeedV1,
    SourceNeutralEconomicEvidenceV1, SourceNeutralEvidenceKindV1, SourceNeutralEvidenceV1,
    SourceNeutralLogicalKeyV1,
};
use ledger::{
    AccountAdmissionPolicyV1, LedgerApplyOutcomeV1, LedgerErrorV1, LedgerLocalEventIdentityV1,
    LedgerRequestIdentityV1, LedgerSubmissionItemIdentityV1, LedgerWriterV1,
};
use tempfile::TempDir;

const MARKET: &str = "condition-stage-3";
const TOKEN: &str = "token-stage-3-up";

struct NoeFixture {
    _temporary: TempDir,
    store: DurableContextStoreV1,
    run_id: String,
    opening_evidence_identity: String,
    next_batch_sequence: u64,
}

impl NoeFixture {
    fn new(name: &str) -> Self {
        let temporary = TempDir::new().unwrap();
        let run_id = format!("run-{name}");
        let opening_evidence_identity = format!("opening-{name}");
        let seed = SimV2ContextSeedV1 {
            run_id: run_id.clone(),
            account_instance_key: AccountInstanceKeyV1 {
                run_id: run_id.clone(),
                caller_role: SimCallerRoleV1::ModelTrain,
                caller_instance_id: format!("caller-{name}"),
                account_id: format!("account-{name}"),
                platform_source: PlatformSourceV1::Simulated,
            },
            run_opened: DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Sha256(Sha256Hex("1".repeat(64))),
            },
            account_opening: DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Sha256(Sha256Hex("2".repeat(64))),
            },
            account_opening_evidence_identity: opening_evidence_identity.clone(),
            implementation_identity: "ledger-stage-3-noe-fixture".to_owned(),
        };
        let store = DurableContextStoreV1::open_or_create_sim_v2(temporary.path(), seed).unwrap();
        Self {
            _temporary: temporary,
            store,
            run_id,
            opening_evidence_identity,
            next_batch_sequence: 1,
        }
    }

    fn commit_opening(&mut self, available_pusd: &str) -> NoeResultReferenceV1 {
        let sequence = self.next_batch_sequence;
        self.commit(
            SimV2SourceRecordReferenceV1::AccountOpening,
            SourceNeutralLogicalKeyV1::EconomicEvidence {
                evidence_identity: format!("opening-{sequence}"),
            },
            SourceNeutralEvidenceKindV1::Economic {
                evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
                    opening_evidence_identity: self.opening_evidence_identity.clone(),
                    available_pusd: decimal(available_pusd),
                },
            },
        )
    }

    fn commit_submission(
        &mut self,
        global_request_id: &str,
        outcome: SourceNeutralSubmissionOutcomeV1,
    ) -> NoeResultReferenceV1 {
        let sequence = self.next_batch_sequence;
        self.commit(
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
        let sequence = self.next_batch_sequence;
        self.commit(
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

    fn commit_pending_submission(&mut self, global_request_id: &str) -> NoeResultReferenceV1 {
        let sequence = self.next_batch_sequence;
        self.commit(
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
            SourceNeutralEvidenceKindV1::Pending {
                scope: PendingScopeV1::EvidenceGap {
                    scope_identity: format!("pending-{global_request_id}"),
                },
            },
        )
    }

    fn commit_order_lifecycle(
        &mut self,
        order_identity: &str,
        event: SourceNeutralOrderLifecycleEventV1,
    ) -> NoeResultReferenceV1 {
        let sequence = self.next_batch_sequence;
        self.commit(
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
                    condition_id: condition(),
                    token_id: token(),
                    event,
                },
            },
        )
    }

    fn commit_trade(
        &mut self,
        trade_identity: &str,
        order_identity: &str,
        disposition: SourceNeutralTradeEconomicDispositionV1,
    ) -> NoeResultReferenceV1 {
        let sequence = self.next_batch_sequence;
        self.commit(
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
                    condition_id: condition(),
                    token_id: token(),
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
        let sequence = self.next_batch_sequence;
        self.commit(
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

    fn commit_transition(
        &mut self,
        account_transition_seq: u64,
        source_submission: Option<&str>,
        transition: SourceNeutralAccountTransitionKindV1,
        delta: SourceNeutralAccountBalanceDeltaV1,
    ) -> NoeResultReferenceV1 {
        let sequence = self.next_batch_sequence;
        let record = source_submission.map_or_else(
            || SimV2SourceRecordReferenceV1::OrderLifecycle {
                private_feedback_sequence: sequence,
                platform_event_at_utc_ms: 5_000 + sequence as i64,
                feedback_received_at_utc_ms: 5_100 + sequence as i64,
            },
            |global_request_id| {
                direct_response(
                    sequence,
                    SimV2DirectResponseAssociationV1::SubmissionItem {
                        global_request_id: global_request_id.to_owned(),
                        batch_item_ordinal: 0,
                    },
                )
            },
        );
        self.commit(
            record,
            SourceNeutralLogicalKeyV1::EconomicEvidence {
                evidence_identity: format!("transition-{sequence}"),
            },
            SourceNeutralEvidenceKindV1::Economic {
                evidence: SourceNeutralEconomicEvidenceV1::AccountTransition {
                    account_transition_seq,
                    economic_effective_at_utc_ms: 5_000 + sequence as i64,
                    condition_id: Some(condition()),
                    transition,
                    balance_delta: Box::new(delta),
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
        let sequence = self.next_batch_sequence;
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
                10_000 + sequence as i64,
            )
            .unwrap();
        self.next_batch_sequence += 1;
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

fn condition() -> ConditionId {
    ConditionId(MARKET.to_owned())
}

fn token() -> TokenId {
    TokenId(TOKEN.to_owned())
}

fn order(order_identity: &str, state: SourceNeutralOrderStateV1) -> SourceNeutralOrderSnapshotV1 {
    SourceNeutralOrderSnapshotV1 {
        order_identity: identity(order_identity),
        condition_id: condition(),
        token_id: token(),
        state,
    }
}

fn decimal(value: &str) -> ExactDecimal {
    ExactDecimal(value.to_owned())
}

#[allow(clippy::too_many_arguments)]
fn balance_delta(
    cash: &str,
    available: &str,
    principal: &str,
    immediate_fee: &str,
    shares: Option<&str>,
    maker_receivable: &str,
    taker_receivable: &str,
    maker_locked: &str,
    taker_locked: &str,
) -> SourceNeutralAccountBalanceDeltaV1 {
    SourceNeutralAccountBalanceDeltaV1 {
        cash_pusd_delta: decimal(cash),
        available_pusd_delta: decimal(available),
        principal_encumbrance_pusd_delta: decimal(principal),
        immediate_taker_fee_encumbrance_pusd_delta: decimal(immediate_fee),
        outcome_balance_deltas: shares
            .into_iter()
            .map(|shares| SourceNeutralOutcomeBalanceDeltaV1 {
                token_id: token(),
                balance_shares_delta: decimal(shares),
            })
            .collect(),
        maker_rebate_receivable_pusd_delta: decimal(maker_receivable),
        taker_rebate_receivable_pusd_delta: decimal(taker_receivable),
        maker_rebate_locked_remainder_pusd_delta: decimal(maker_locked),
        taker_rebate_locked_remainder_pusd_delta: decimal(taker_locked),
    }
}

fn money_delta(
    cash: &str,
    available: &str,
    principal: &str,
    immediate_fee: &str,
    shares: Option<&str>,
) -> SourceNeutralAccountBalanceDeltaV1 {
    balance_delta(
        cash,
        available,
        principal,
        immediate_fee,
        shares,
        "0",
        "0",
        "0",
        "0",
    )
}

fn policy() -> AccountAdmissionPolicyV1 {
    AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: decimal("1000"),
    }
}

fn opened(name: &str, available: &str) -> (NoeFixture, LedgerWriterV1) {
    let mut fixture = NoeFixture::new(name);
    let opening = fixture.commit_opening(available);
    let mut writer = LedgerWriterV1::new(policy());
    writer.apply_noe_result(&opening).unwrap();
    (fixture, writer)
}

fn request(value: &str) -> LedgerRequestIdentityV1 {
    LedgerRequestIdentityV1(value.to_owned())
}

fn event(value: &str) -> LedgerLocalEventIdentityV1 {
    LedgerLocalEventIdentityV1(value.to_owned())
}

fn submission(value: &str) -> LedgerSubmissionItemIdentityV1 {
    LedgerSubmissionItemIdentityV1 {
        global_request_id: value.to_owned(),
        batch_item_ordinal: 0,
    }
}

fn reserve_and_submit(writer: &mut LedgerWriterV1, name: &str, principal: &str, fee: &str) {
    writer
        .try_reserve_intent(
            &request(name),
            &condition(),
            &decimal(principal),
            &decimal(fee),
        )
        .unwrap();
    writer
        .record_submission_started(
            &event(&format!("event-{name}")),
            &request(name),
            &submission(name),
        )
        .unwrap();
}

#[test]
fn submission_results_convert_release_or_retain_once() {
    let (mut fixture, mut writer) = opened("submission-results", "100");
    for (name, principal, fee) in [
        ("accepted", "10", "1"),
        ("rejected", "6", "0.6"),
        ("unknown-no-effect", "7", "0.7"),
        ("pending", "5", "0.5"),
    ] {
        reserve_and_submit(&mut writer, name, principal, fee);
    }

    let accepted = fixture.commit_submission(
        "accepted",
        SourceNeutralSubmissionOutcomeV1::Accepted {
            order: order("order-accepted", SourceNeutralOrderStateV1::Open),
        },
    );
    let rejected = fixture.commit_submission(
        "rejected",
        SourceNeutralSubmissionOutcomeV1::RejectedNoEffect,
    );
    let unknown = fixture.commit_submission(
        "unknown-no-effect",
        SourceNeutralSubmissionOutcomeV1::EvidenceUnknownNoEffect,
    );
    let pending = fixture.commit_pending_submission("pending");

    let accepted_outcome = writer.apply_noe_result(&accepted).unwrap();
    writer.apply_noe_result(&rejected).unwrap();
    writer.apply_noe_result(&unknown).unwrap();
    writer.apply_noe_result(&pending).unwrap();
    let projection = writer.projection().unwrap();
    assert_eq!(
        projection
            .local_pending_reservation_projection
            .principal_pusd,
        decimal("5")
    );
    assert_eq!(
        projection
            .local_pending_reservation_projection
            .after_send_unknown_count,
        1
    );
    assert_eq!(
        projection
            .platform_confirmed_projection
            .platform_confirmation_pending_economic_principal_pusd,
        decimal("10")
    );
    assert_eq!(
        projection
            .platform_confirmed_projection
            .platform_confirmation_pending_economic_count,
        1
    );
    assert_eq!(
        projection.platform_confirmed_projection.available_pusd,
        decimal("89")
    );
    assert_eq!(projection.platform_confirmed_projection.open_order_count, 1);
    assert_eq!(
        writer.apply_noe_result(&accepted).unwrap(),
        match accepted_outcome {
            LedgerApplyOutcomeV1::NewlyApplied { ledger_entry_seq } => {
                LedgerApplyOutcomeV1::AlreadyApplied { ledger_entry_seq }
            }
            other => panic!("首次接受必须新应用：{other:?}"),
        }
    );
    assert_eq!(writer.projection().unwrap(), projection);
}

#[test]
fn registered_query_found_converts_while_not_found_and_out_of_retention_retain() {
    let (mut fixture, mut writer) = opened("query-results", "100");
    for name in ["found", "not-found", "retained", "missing-association"] {
        reserve_and_submit(&mut writer, name, "5", "0");
    }
    for name in ["found", "not-found", "retained"] {
        writer
            .record_order_query_association(
                &event(&format!("query-event-{name}")),
                &format!("query-{name}"),
                &request(name),
            )
            .unwrap();
    }

    let not_found =
        fixture.commit_query("query-not-found", SourceNeutralOrderQueryResultV1::NotFound);
    let out_of_retention = fixture.commit_query(
        "query-retained",
        SourceNeutralOrderQueryResultV1::OutOfRetention(
            SourceNeutralQueryOutOfRetentionV1::OrderHistory {
                retained_from_utc_ms: 1_000,
            },
        ),
    );
    let missing_association = fixture.commit_query(
        "query-missing",
        SourceNeutralOrderQueryResultV1::Found {
            order: order("order-missing", SourceNeutralOrderStateV1::Open),
        },
    );
    let found = fixture.commit_query(
        "query-found",
        SourceNeutralOrderQueryResultV1::Found {
            order: order("order-found", SourceNeutralOrderStateV1::Matched),
        },
    );
    for reference in [&not_found, &out_of_retention, &missing_association, &found] {
        writer.apply_noe_result(reference).unwrap();
    }

    let projection = writer.projection().unwrap();
    assert_eq!(
        projection
            .local_pending_reservation_projection
            .principal_pusd,
        decimal("15")
    );
    assert_eq!(
        projection
            .local_pending_reservation_projection
            .after_send_unknown_count,
        3
    );
    assert_eq!(
        projection
            .platform_confirmed_projection
            .platform_confirmation_pending_economic_principal_pusd,
        decimal("5")
    );
    assert_eq!(projection.platform_confirmed_projection.open_order_count, 0);
}

#[test]
fn same_reservation_is_consumed_by_only_one_definitive_result() {
    let (mut fixture, mut writer) = opened("single-definitive", "50");
    reserve_and_submit(&mut writer, "single", "10", "1");
    writer
        .record_order_query_association(&event("query-single"), "query-single", &request("single"))
        .unwrap();
    let accepted = fixture.commit_submission(
        "single",
        SourceNeutralSubmissionOutcomeV1::Accepted {
            order: order("order-single", SourceNeutralOrderStateV1::Open),
        },
    );
    let accepted_economic = fixture.commit_transition(
        1,
        Some("single"),
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity: identity("order-single"),
        },
        money_delta("0", "-11", "10", "1", None),
    );
    let also_found = fixture.commit_query(
        "query-single",
        SourceNeutralOrderQueryResultV1::Found {
            order: order("order-single", SourceNeutralOrderStateV1::Open),
        },
    );
    writer.apply_noe_result(&accepted_economic).unwrap();
    let once = writer.projection().unwrap();
    writer.apply_noe_result(&accepted).unwrap();
    writer.apply_noe_result(&also_found).unwrap();
    let twice = writer.projection().unwrap();
    assert_eq!(
        twice
            .platform_confirmed_projection
            .platform_confirmation_pending_economic_principal_pusd,
        decimal("0")
    );
    assert_eq!(
        once.platform_confirmed_projection
            .principal_encumbrance_pusd,
        decimal("10")
    );
    assert_eq!(
        twice
            .platform_confirmed_projection
            .principal_encumbrance_pusd,
        decimal("10")
    );
    assert_eq!(
        twice
            .platform_confirmed_projection
            .cumulative_accepted_principal_pusd,
        decimal("10")
    );
    assert_eq!(twice.platform_confirmed_projection.open_order_count, 1);
}

#[test]
fn order_fill_release_fee_and_compensation_form_balanced_transactions() {
    let (mut fixture, mut writer) = opened("economic-lifecycle", "100");
    reserve_and_submit(&mut writer, "economic", "20", "1");
    let accepted = fixture.commit_submission(
        "economic",
        SourceNeutralSubmissionOutcomeV1::Accepted {
            order: order("order-economic", SourceNeutralOrderStateV1::Open),
        },
    );
    let encumbrance = fixture.commit_transition(
        1,
        Some("economic"),
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity: identity("order-economic"),
        },
        money_delta("0", "-21", "20", "1", None),
    );
    writer.apply_noe_result(&accepted).unwrap();
    writer.apply_noe_result(&encumbrance).unwrap();

    let partial_pending = fixture.commit_trade(
        "trade-partial",
        "order-economic",
        SourceNeutralTradeEconomicDispositionV1::PendingUnapplied,
    );
    let partial_fill = fixture.commit_transition(
        2,
        None,
        SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
            trade_identity: identity("trade-partial"),
            path_ordinal: 0,
        },
        money_delta("-10.5", "0", "-10", "-0.5", Some("20")),
    );
    writer.apply_noe_result(&partial_pending).unwrap();
    writer.apply_noe_result(&partial_fill).unwrap();
    let partial = writer.projection().unwrap();
    assert_eq!(
        partial
            .platform_confirmed_projection
            .principal_encumbrance_pusd,
        decimal("10")
    );
    assert_eq!(
        partial
            .platform_confirmed_projection
            .position_cost_basis_pusd,
        decimal("10")
    );
    assert_eq!(
        partial.platform_confirmed_projection.fees_pusd,
        decimal("0.5")
    );
    assert_eq!(partial.platform_confirmed_projection.pending_fill_count, 1);
    assert_eq!(
        partial.platform_confirmed_projection.realized_pnl_pusd,
        decimal("0")
    );
    let fill_outcome = writer.apply_noe_result(&partial_fill).unwrap();
    assert!(matches!(
        fill_outcome,
        LedgerApplyOutcomeV1::AlreadyApplied { .. }
    ));
    assert_eq!(writer.projection().unwrap(), partial);

    let compensation = fixture.commit_transition(
        3,
        None,
        SourceNeutralAccountTransitionKindV1::FillFinalityCompensationApplied {
            trade_identity: identity("trade-partial"),
            path_ordinal: 0,
        },
        money_delta("10.5", "0", "10", "0.5", Some("-20")),
    );
    writer.apply_noe_result(&compensation).unwrap();
    let compensated = writer.projection().unwrap();
    assert_eq!(
        compensated
            .platform_confirmed_projection
            .position_cost_basis_pusd,
        decimal("0")
    );
    assert_eq!(
        compensated.platform_confirmed_projection.fees_pusd,
        decimal("0.0")
    );
    assert_eq!(
        compensated.platform_confirmed_projection.pending_fill_count,
        1
    );
    let compensated_terminal = fixture.commit_trade(
        "trade-partial",
        "order-economic",
        SourceNeutralTradeEconomicDispositionV1::FinalFailureCompensated,
    );
    writer.apply_noe_result(&compensated_terminal).unwrap();
    assert_eq!(
        writer
            .projection()
            .unwrap()
            .platform_confirmed_projection
            .pending_fill_count,
        0
    );

    let failed_pending = fixture.commit_trade(
        "trade-failed",
        "order-economic",
        SourceNeutralTradeEconomicDispositionV1::PendingUnapplied,
    );
    let failure = fixture.commit_transition(
        4,
        None,
        SourceNeutralAccountTransitionKindV1::FillFinalityUnappliedFailureReleased {
            trade_identity: identity("trade-failed"),
            path_ordinal: 0,
        },
        money_delta("0", "5", "-5", "0", None),
    );
    writer.apply_noe_result(&failed_pending).unwrap();
    assert_eq!(
        writer
            .projection()
            .unwrap()
            .platform_confirmed_projection
            .pending_fill_count,
        1
    );
    writer.apply_noe_result(&failure).unwrap();
    assert_eq!(
        writer
            .projection()
            .unwrap()
            .platform_confirmed_projection
            .pending_fill_count,
        1
    );
    let failed_terminal = fixture.commit_trade(
        "trade-failed",
        "order-economic",
        SourceNeutralTradeEconomicDispositionV1::FinalFailureUnapplied,
    );
    writer.apply_noe_result(&failed_terminal).unwrap();
    assert_eq!(
        writer
            .projection()
            .unwrap()
            .platform_confirmed_projection
            .pending_fill_count,
        0
    );

    let full_pending = fixture.commit_trade(
        "trade-full",
        "order-economic",
        SourceNeutralTradeEconomicDispositionV1::PendingApplied,
    );
    let full_fill = fixture.commit_transition(
        5,
        None,
        SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
            trade_identity: identity("trade-full"),
            path_ordinal: 0,
        },
        money_delta("-16", "0", "-15", "-1", Some("30")),
    );
    let terminal = fixture.commit_order_lifecycle(
        "order-economic",
        SourceNeutralOrderLifecycleEventV1::FakUnspentReleased,
    );
    writer.apply_noe_result(&full_pending).unwrap();
    assert_eq!(
        writer
            .projection()
            .unwrap()
            .platform_confirmed_projection
            .pending_fill_count,
        1
    );
    writer.apply_noe_result(&full_fill).unwrap();
    assert_eq!(
        writer
            .projection()
            .unwrap()
            .platform_confirmed_projection
            .pending_fill_count,
        1
    );
    writer.apply_noe_result(&terminal).unwrap();
    assert_eq!(
        writer
            .projection()
            .unwrap()
            .platform_confirmed_projection
            .pending_fill_count,
        1
    );
    let full_terminal = fixture.commit_trade(
        "trade-full",
        "order-economic",
        SourceNeutralTradeEconomicDispositionV1::FinalSuccess,
    );
    writer.apply_noe_result(&full_terminal).unwrap();
    let final_projection = writer.projection().unwrap();
    assert_eq!(
        final_projection
            .platform_confirmed_projection
            .principal_encumbrance_pusd,
        decimal("0")
    );
    assert_eq!(
        final_projection
            .platform_confirmed_projection
            .immediate_taker_fee_encumbrance_pusd,
        decimal("0.0")
    );
    assert_eq!(
        final_projection
            .platform_confirmed_projection
            .position_cost_basis_pusd,
        decimal("15")
    );
    assert_eq!(
        final_projection.platform_confirmed_projection.fees_pusd,
        decimal("1")
    );
    assert_eq!(
        final_projection
            .platform_confirmed_projection
            .open_order_count,
        0
    );
    assert_eq!(
        final_projection
            .platform_confirmed_projection
            .pending_fill_count,
        0
    );
}

#[test]
fn pending_unapplied_keeps_exact_total_and_only_changes_pending_count() {
    let (mut fixture, mut writer) = opened("pending-unapplied", "50");
    let accepted = fixture.commit_transition(
        1,
        None,
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity: identity("order-pending"),
        },
        money_delta("0", "-32", "30", "2", None),
    );
    let pending = fixture.commit_trade(
        "trade-pending",
        "order-pending",
        SourceNeutralTradeEconomicDispositionV1::PendingUnapplied,
    );
    writer.apply_noe_result(&accepted).unwrap();
    let before_pending = writer.projection().unwrap();
    writer.apply_noe_result(&pending).unwrap();
    let after_pending = writer.projection().unwrap();
    assert_eq!(
        after_pending
            .platform_confirmed_projection
            .principal_encumbrance_pusd,
        decimal("30")
    );
    assert_eq!(
        after_pending
            .platform_confirmed_projection
            .immediate_taker_fee_encumbrance_pusd,
        decimal("2")
    );
    assert_eq!(
        after_pending
            .platform_confirmed_projection
            .current_all_in_commitment_pusd,
        decimal("32")
    );
    assert_eq!(
        after_pending
            .platform_confirmed_projection
            .pending_fill_count,
        1
    );
    assert_eq!(
        after_pending
            .platform_confirmed_projection
            .position_cost_basis_pusd,
        decimal("0")
    );
    assert_eq!(
        before_pending
            .platform_confirmed_projection
            .principal_encumbrance_pusd,
        after_pending
            .platform_confirmed_projection
            .principal_encumbrance_pusd
    );
}

#[test]
fn rebate_accrual_payment_and_auto_redeem_move_only_owned_buckets() {
    let (mut fixture, mut writer) = opened("rebate-redeem", "30");
    let accepted = fixture.commit_transition(
        1,
        None,
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity: identity("order-redeem"),
        },
        money_delta("0", "-10", "10", "0", None),
    );
    let fill = fixture.commit_transition(
        2,
        None,
        SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
            trade_identity: identity("trade-redeem"),
            path_ordinal: 0,
        },
        money_delta("-10", "0", "-10", "0", Some("20")),
    );
    let accrual = fixture.commit_transition(
        3,
        None,
        SourceNeutralAccountTransitionKindV1::MakerRebateAccrualFinalized,
        balance_delta("0", "0", "0", "0", None, "2", "0", "0", "0"),
    );
    let payment = fixture.commit_transition(
        4,
        None,
        SourceNeutralAccountTransitionKindV1::MakerRebatePaid {
            attribution_day_utc_ms: 86_400_000,
        },
        balance_delta("2", "2", "0", "0", None, "-2", "0", "0", "0"),
    );
    for reference in [&accepted, &fill, &accrual] {
        writer.apply_noe_result(reference).unwrap();
    }
    let accrued = writer.projection().unwrap();
    assert_eq!(
        accrued.platform_confirmed_projection.cash_pusd,
        decimal("20")
    );
    assert_eq!(
        accrued
            .platform_confirmed_projection
            .maker_rebate_receivable_pusd,
        decimal("2")
    );
    assert_eq!(
        accrued.platform_confirmed_projection.realized_pnl_pusd,
        decimal("2")
    );
    writer.apply_noe_result(&payment).unwrap();
    let paid = writer.projection().unwrap();
    assert_eq!(paid.platform_confirmed_projection.cash_pusd, decimal("22"));
    assert_eq!(
        paid.platform_confirmed_projection
            .maker_rebate_receivable_pusd,
        decimal("0")
    );
    assert_eq!(
        paid.platform_confirmed_projection.realized_pnl_pusd,
        decimal("2")
    );

    let settlement = fixture.commit_settlement(
        SourceNeutralSettlementLifecycleResultV1::AutoRedeemApplied {
            condition_id: condition(),
            resolution_identity: identity("resolution-redeem"),
            resolution_source_event_at_utc_ms: 9_000,
        },
    );
    writer.apply_noe_result(&settlement).unwrap();
    let before_economic_redeem = writer.projection().unwrap();
    assert_eq!(
        before_economic_redeem
            .platform_confirmed_projection
            .cash_pusd,
        decimal("22")
    );
    let redeem = fixture.commit_transition(
        5,
        None,
        SourceNeutralAccountTransitionKindV1::AutoRedeemApplied {
            resolution_identity: identity("resolution-redeem"),
        },
        money_delta("15", "15", "0", "0", Some("-20")),
    );
    writer.apply_noe_result(&redeem).unwrap();
    let redeemed = writer.projection().unwrap();
    assert_eq!(
        redeemed.platform_confirmed_projection.cash_pusd,
        decimal("37")
    );
    assert_eq!(
        redeemed
            .platform_confirmed_projection
            .position_cost_basis_pusd,
        decimal("0")
    );
    assert_eq!(
        redeemed.platform_confirmed_projection.realized_pnl_pusd,
        decimal("7")
    );
    assert_eq!(
        redeemed
            .platform_confirmed_projection
            .redeemable_receivable_pusd,
        decimal("0")
    );
    assert_eq!(
        redeemed.platform_confirmed_projection.outcome_balances[0].balance_shares,
        decimal("0")
    );
}

#[test]
fn zero_payout_auto_redeem_is_a_valid_full_loss() {
    let (mut fixture, mut writer) = opened("zero-payout", "10");
    let accepted = fixture.commit_transition(
        1,
        None,
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity: identity("order-loss"),
        },
        money_delta("0", "-10", "10", "0", None),
    );
    let fill = fixture.commit_transition(
        2,
        None,
        SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
            trade_identity: identity("trade-loss"),
            path_ordinal: 0,
        },
        money_delta("-10", "0", "-10", "0", Some("20")),
    );
    let redeem = fixture.commit_transition(
        3,
        None,
        SourceNeutralAccountTransitionKindV1::AutoRedeemApplied {
            resolution_identity: identity("resolution-loss"),
        },
        money_delta("0", "0", "0", "0", Some("-20")),
    );
    for reference in [&accepted, &fill, &redeem] {
        writer.apply_noe_result(reference).unwrap();
    }
    let projection = writer.projection().unwrap();
    assert_eq!(
        projection.platform_confirmed_projection.cash_pusd,
        decimal("0")
    );
    assert_eq!(
        projection
            .platform_confirmed_projection
            .position_cost_basis_pusd,
        decimal("0")
    );
    assert_eq!(
        projection.platform_confirmed_projection.realized_pnl_pusd,
        decimal("-10")
    );
    assert_eq!(
        projection
            .platform_confirmed_projection
            .redeemable_receivable_pusd,
        decimal("0")
    );
}

#[test]
fn zero_cash_states_remain_mechanically_distinguishable() {
    let (mut fixture, mut writer) = opened("zero-cash-state", "11");
    reserve_and_submit(&mut writer, "zero", "10", "1");
    let accepted = fixture.commit_submission(
        "zero",
        SourceNeutralSubmissionOutcomeV1::Accepted {
            order: order("order-zero", SourceNeutralOrderStateV1::Open),
        },
    );
    writer.apply_noe_result(&accepted).unwrap();
    let pending_economic = writer.projection().unwrap();
    assert_eq!(
        pending_economic
            .platform_confirmed_projection
            .available_pusd,
        decimal("0")
    );
    assert_eq!(
        pending_economic
            .platform_confirmed_projection
            .platform_confirmation_pending_economic_count,
        1
    );

    let encumbrance = fixture.commit_transition(
        1,
        Some("zero"),
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity: identity("order-zero"),
        },
        money_delta("0", "-11", "10", "1", None),
    );
    let fill = fixture.commit_transition(
        2,
        None,
        SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
            trade_identity: identity("trade-zero"),
            path_ordinal: 0,
        },
        money_delta("-11", "0", "-10", "-1", Some("20")),
    );
    writer.apply_noe_result(&encumbrance).unwrap();
    writer.apply_noe_result(&fill).unwrap();
    let holding = writer.projection().unwrap();
    assert_eq!(
        holding.platform_confirmed_projection.cash_pusd,
        decimal("0")
    );
    assert_eq!(
        holding.platform_confirmed_projection.outcome_balances[0].balance_shares,
        decimal("20")
    );
    assert_eq!(
        holding
            .platform_confirmed_projection
            .position_cost_basis_pusd,
        decimal("10")
    );
    assert_eq!(
        holding.platform_confirmed_projection.fees_pusd,
        decimal("1")
    );

    let (_, closed) = opened("zero-cash-closed", "0");
    let closed = closed.projection().unwrap();
    assert_eq!(closed.platform_confirmed_projection.cash_pusd, decimal("0"));
    assert_eq!(closed.platform_confirmed_projection.open_order_count, 0);
    assert_eq!(closed.platform_confirmed_projection.pending_fill_count, 0);
    assert!(
        closed
            .platform_confirmed_projection
            .outcome_balances
            .is_empty()
    );
    assert_ne!(holding, closed);
}

#[test]
fn missing_predecessor_is_zero_side_effect_and_retryable() {
    let (mut fixture, mut writer) = opened("missing-predecessor", "30");
    reserve_and_submit(&mut writer, "causal", "10", "1");
    let accepted_control = fixture.commit_submission(
        "causal",
        SourceNeutralSubmissionOutcomeV1::Accepted {
            order: order("order-causal", SourceNeutralOrderStateV1::Open),
        },
    );
    writer.apply_noe_result(&accepted_control).unwrap();
    let predecessor = fixture.commit_transition(
        1,
        Some("causal"),
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity: identity("order-causal"),
        },
        money_delta("0", "-11", "10", "1", None),
    );
    let successor = fixture.commit_transition(
        2,
        None,
        SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
            trade_identity: identity("trade-causal"),
            path_ordinal: 0,
        },
        money_delta("-11", "0", "-10", "-1", Some("20")),
    );
    let before = writer.projection().unwrap();
    assert_eq!(
        writer.apply_noe_result(&successor).unwrap(),
        LedgerApplyOutcomeV1::MissingPlatformPredecessor {
            expected_account_transition_seq: 1,
            found_account_transition_seq: 2,
        }
    );
    assert_eq!(writer.projection().unwrap(), before);
    writer.apply_noe_result(&predecessor).unwrap();
    writer.apply_noe_result(&successor).unwrap();
    let after = writer.projection().unwrap();
    assert_eq!(after.platform_source_prefix, 2);
    assert_eq!(
        after.platform_confirmed_projection.position_cost_basis_pusd,
        decimal("10")
    );
}

#[test]
fn causal_projection_advances_only_after_successful_application() {
    let (mut fixture, mut writer) = opened("causal-visibility", "20");
    let first = fixture.commit_submission(
        "unassociated",
        SourceNeutralSubmissionOutcomeV1::RejectedNoEffect,
    );
    let duplicate = fixture.commit_submission(
        "unassociated",
        SourceNeutralSubmissionOutcomeV1::RejectedNoEffect,
    );
    writer.apply_noe_result(&first).unwrap();
    let successful = writer.projection().unwrap();
    assert!(matches!(
        writer.apply_noe_result(&duplicate),
        Err(LedgerErrorV1::NonEconomicRecord)
    ));
    assert_eq!(writer.projection().unwrap(), successful);
}

#[test]
fn same_reference_chain_rebuilds_deterministically() {
    let mut fixture = NoeFixture::new("deterministic-stage-3");
    let opening = fixture.commit_opening("50");
    let accepted_control = fixture.commit_submission(
        "deterministic",
        SourceNeutralSubmissionOutcomeV1::Accepted {
            order: order("order-deterministic", SourceNeutralOrderStateV1::Open),
        },
    );
    let accepted_economic = fixture.commit_transition(
        1,
        Some("deterministic"),
        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
            order_identity: identity("order-deterministic"),
        },
        money_delta("0", "-11", "10", "1", None),
    );
    let pending = fixture.commit_trade(
        "trade-deterministic",
        "order-deterministic",
        SourceNeutralTradeEconomicDispositionV1::PendingUnapplied,
    );
    let fill = fixture.commit_transition(
        2,
        None,
        SourceNeutralAccountTransitionKindV1::FillFinalityEconomicApplied {
            trade_identity: identity("trade-deterministic"),
            path_ordinal: 0,
        },
        money_delta("-11", "0", "-10", "-1", Some("20")),
    );
    let mut first = LedgerWriterV1::new(policy());
    let mut second = LedgerWriterV1::new(policy());
    for writer in [&mut first, &mut second] {
        writer.apply_noe_result(&opening).unwrap();
        reserve_and_submit(writer, "deterministic", "10", "1");
        for reference in [&accepted_control, &accepted_economic, &pending, &fill] {
            writer.apply_noe_result(reference).unwrap();
        }
    }
    assert_eq!(first.projection().unwrap(), second.projection().unwrap());
}
