//! 既有返佣/预留反例通过正式NOE核心与来源领取入口；不是事故续接或平台全域证明。
use ::execution_normalization::*;
use contracts::{execution_normalization::*, *};
use ledger::*;
use std::sync::{Arc, Mutex};
type Identity = MemoryLogicalIdentityV1;
type Trade = SourceNeutralTradeLifecycleResultV2<Identity>;
type Control = SourceNeutralControlResult<
    SourceNeutralSubmissionOutcomeV2<Identity>,
    SourceNeutralOrderSnapshotV2<Identity>,
    Identity,
>;
type Core = NormalizationCore<Trade, Control, Identity, MemoryEconomicFactReferenceV1>;
type Kind = SourceNeutralEvidenceKind<Trade, Control, Identity>;
struct Source {
    context: Arc<ExecutionNormalizationContextV1>,
    core: Core,
    source: NoeConsumptionSourceV1,
    writer: StagedNormalizationWriter<
        SourceNeutralTradeLifecycleResultV2,
        SourceNeutralControlResultV2,
    >,
    mappings: Vec<(u64, NoeBatchDurabilityV1)>,
    seq: u64,
}
impl Source {
    fn new(root: &std::path::Path, run: &str) -> Self {
        let reference = DurableSourceReferenceV1 {
            source: ImmutableSourceReferenceV1::Identity(run.into()),
        };
        let context = Arc::new(
            sim_v2_context(
                SimV2ContextSeedV1 {
                    run_id: run.into(),
                    account_instance_key: AccountInstanceKeyV1 {
                        run_id: run.into(),
                        caller_role: SimCallerRoleV1::Paper,
                        caller_instance_id: "caller".into(),
                        account_id: "account".into(),
                        platform_source: PlatformSourceV1::Simulated,
                    },
                    run_opened: reference.clone(),
                    account_opening: reference,
                    account_opening_evidence_identity: "opening".into(),
                    implementation_identity: "memory-mechanism".into(),
                },
                "execution_normalization_v3",
            )
            .unwrap(),
        );
        Self {
            core: Core::new((*context).clone()),
            writer: StagedNormalizationWriter::create(root, (*context).clone()).unwrap(),
            context,
            source: NoeConsumptionSourceV1::default(),
            mappings: Vec::new(),
            seq: 0,
        }
    }
    fn accept(&mut self, kind: Kind) {
        let n = self.seq;
        self.seq += 1;
        let watermark = SimV2SourceWatermarkV1 {
            run_id: self.context.source.account_instance_key.run_id.clone(),
            projection_batch_sequence: n + 41,
        };
        let record = if n == 0 {
            SimV2SourceRecordReferenceV1::AccountOpening
        } else {
            SimV2SourceRecordReferenceV1::SettlementLifecycle {
                private_feedback_sequence: n,
                platform_event_at_utc_ms: 100 + n as i64,
                feedback_received_at_utc_ms: 200 + n as i64,
            }
        };
        let batch = Arc::new(
            self.core
                .normalize_and_accept(NormalizationBatchInput {
                    source_watermark: watermark.clone(),
                    evidence: vec![SourceNeutralEvidence {
                        source_reference: SimV2SourceReferenceV1 {
                            context_reference: self.context.context_reference.clone(),
                            watermark,
                            record,
                        },
                        source_ordinal: 0,
                        logical_key: SourceNeutralLogicalKeyV1::EconomicEvidence {
                            evidence_identity: format!("effect-{n}"),
                        },
                        kind,
                    }],
                })
                .unwrap(),
        );
        self.writer.stage(batch.as_ref()).unwrap();
        self.mappings
            .push((n + 41, self.source.accept(self.context.clone(), batch)));
    }
    fn stable(&mut self) {
        let mappings = &self.mappings;
        self.writer
            .stabilize(
                || 999,
                |commit| {
                    mappings
                        .iter()
                        .find(|(seq, _)| {
                            *seq == commit.boundary.source_watermark.projection_batch_sequence
                        })
                        .unwrap()
                        .1
                        .publish(&commit.container_reference)
                },
            )
            .unwrap();
    }
}
fn number(s: &str) -> ExactDecimal {
    ExactDecimal(s.into())
}
fn delta(cash: &str, available: &str, rebate: &str) -> SourceNeutralAccountBalanceDeltaV1 {
    SourceNeutralAccountBalanceDeltaV1 {
        cash_pusd_delta: number(cash),
        available_pusd_delta: number(available),
        principal_encumbrance_pusd_delta: number("0"),
        immediate_taker_fee_encumbrance_pusd_delta: number("0"),
        outcome_balance_deltas: vec![],
        maker_rebate_receivable_pusd_delta: number(rebate),
        taker_rebate_receivable_pusd_delta: number("0"),
        maker_rebate_locked_remainder_pusd_delta: number("0"),
        taker_rebate_locked_remainder_pusd_delta: number("0"),
    }
}
#[test]
fn real_normalization_early_stability_visibility_sticky_decision_and_v2_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let mut source = Source::new(&temp.path().join("noe"), "memory-sticky");
    source.accept(Kind::Economic {
        evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
            opening_evidence_identity: "opening".into(),
            available_pusd: number("100"),
        },
    });
    let opening = source.source.opening_record().unwrap();
    let mut writer = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: number("1000"),
    });
    let materials = Arc::new(Mutex::new(Vec::new()));
    let sink = materials.clone();
    writer.bind_record_sink_v2(Box::new(move |m| sink.lock().unwrap().push(m)));
    writer.apply_noe_memory_v1(&opening).unwrap();
    let id = LedgerRequestIdentityV1("reject120".into());
    let condition = ConditionId("condition".into());
    let rejected = LedgerReservationOutcomeV1::InsufficientGlobalAvailableFunds;
    assert_eq!(
        writer
            .try_reserve_intent(&id, &condition, &number("120"), &number("0"))
            .unwrap(),
        rejected
    );
    assert_eq!(writer.projection_v2().unwrap().ledger_entry_seq, 1);
    source.accept(Kind::Economic {
        evidence: SourceNeutralEconomicEvidenceV1::AccountTransition {
            account_transition_seq: 1,
            economic_effective_at_utc_ms: 101,
            condition_id: None,
            transition: SourceNeutralAccountTransitionKindV1::MakerRebateAccrualFinalized,
            balance_delta: Box::new(delta("0", "0", "50")),
        },
    });
    source.accept(Kind::Economic {
        evidence: SourceNeutralEconomicEvidenceV1::AccountTransition {
            account_transition_seq: 2,
            economic_effective_at_utc_ms: 102,
            condition_id: None,
            transition: SourceNeutralAccountTransitionKindV1::MakerRebatePaid {
                attribution_day_utc_ms: 86_400_000,
            },
            balance_delta: Box::new(delta("50", "50", "-50")),
        },
    });
    source.stable();
    assert!(opening.durable_reference().is_some());
    assert!(
        source.source.take_closed(199, 199).is_empty(),
        "后台发布不提前业务领取"
    );
    let records = source.source.take_closed(500, 700);
    assert_eq!(records.len(), 2);
    assert_eq!(
        records[0].position().record_ordinal,
        records[1].position().record_ordinal
    );
    assert_ne!(records[0].position(), records[1].position());
    let mut other = Source::new(&temp.path().join("other-noe"), "different-account");
    other.accept(Kind::Economic {
        evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
            opening_evidence_identity: "opening".into(),
            available_pusd: number("100"),
        },
    });
    let other_opening = other.source.opening_record().unwrap();
    assert_eq!(
        other_opening.position().record_ordinal,
        opening.position().record_ordinal
    );
    assert_ne!(other_opening.position(), opening.position());
    let before = writer.projection_v2().unwrap();
    assert!(writer.apply_noe_memory_v1(&other_opening).is_err());
    assert_eq!(writer.projection_v2().unwrap(), before);
    let before = writer.projection_v2().unwrap();
    assert!(matches!(
        writer.apply_noe_memory_v1(&records[1]).unwrap(),
        LedgerMemoryApplyOutcomeV1::Applied(
            LedgerApplyOutcomeV1::MissingPlatformPredecessor { .. }
        )
    ));
    assert_eq!(writer.projection_v2().unwrap(), before);
    for r in &records {
        writer.apply_noe_memory_v1(r).unwrap();
    }
    assert_eq!(
        writer
            .projection_v2()
            .unwrap()
            .platform_confirmed_projection
            .cash_pusd,
        number("150")
    );
    assert_eq!(
        writer.projection_v2().unwrap().available_through_utc_ms,
        Some(700)
    );
    assert_eq!(
        writer
            .try_reserve_intent(&id, &condition, &number("120"), &number("0"))
            .unwrap(),
        rejected
    );
    let mut background = LedgerBackgroundWriterV2::new(temp.path().join("ledger"));
    let material = materials.lock().unwrap();
    assert_eq!(
        material
            .iter()
            .filter(|m| m.kind() == LedgerRecordMaterialKindV2::ReservationDecision)
            .count(),
        1
    );
    assert_eq!(material.len(), 5);
    for m in material.iter() {
        assert!(background.write(m).unwrap());
    }
    background.finish().unwrap();
    let mut restored = LedgerWriterV1::open_durable(temp.path().join("ledger")).unwrap();
    assert_eq!(restored.projection_v2(), writer.projection_v2());
    assert_eq!(
        restored
            .try_reserve_intent(&id, &condition, &number("120"), &number("0"))
            .unwrap(),
        rejected
    );
    for r in &records {
        assert!(matches!(
            restored.apply_noe_memory_v1(r).unwrap(),
            LedgerMemoryApplyOutcomeV1::Applied(LedgerApplyOutcomeV1::AlreadyApplied { .. })
        ));
    }
    assert_eq!(restored.projection_v2().unwrap().ledger_entry_seq, 3);
    // 不同批号与后台commit序号真实不同；NOE映射按源批恢复。
    for r in records {
        let stable = r.durable_reference().unwrap();
        let view = read_durable_noe_record_v1(stable.result()).unwrap();
        assert_eq!(view.position(), r.position());
    }
}
