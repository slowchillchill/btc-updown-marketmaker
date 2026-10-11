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
        self.accept_many(vec![kind]);
    }
    fn accept_many(&mut self, kinds: Vec<Kind>) {
        let n = self.seq;
        self.seq += 1;
        let watermark = SimV2SourceWatermarkV1 {
            run_id: self.context.source.account_instance_key.run_id.clone(),
            projection_batch_sequence: n + 41,
        };
        let record = if n == 0 {
            SimV2SourceRecordReferenceV1::AccountOpening
        } else if matches!(
            kinds.first(),
            Some(Kind::TradeLifecycle { .. } | Kind::TradeLifecycleV3 { .. })
        ) {
            SimV2SourceRecordReferenceV1::TradeLifecycle {
                private_feedback_sequence: n,
                platform_event_at_utc_ms: 100 + n as i64,
                feedback_received_at_utc_ms: 200 + n as i64,
            }
        } else if matches!(kinds.first(), Some(Kind::Control { .. })) {
            SimV2SourceRecordReferenceV1::DirectResponse {
                private_feedback_sequence: n,
                platform_responded_at_utc_ms: 100 + n as i64,
                response_received_at_utc_ms: 200 + n as i64,
                association: if matches!(
                    kinds.first(),
                    Some(Kind::Control {
                        result: Control::Submission { .. }
                    })
                ) {
                    SimV2DirectResponseAssociationV1::SubmissionItem {
                        global_request_id: format!("submit-{n}"),
                        batch_item_ordinal: 0,
                    }
                } else if matches!(
                    kinds.first(),
                    Some(Kind::Control {
                        result: Control::Cancellation { .. }
                    })
                ) {
                    SimV2DirectResponseAssociationV1::CancellationTarget {
                        global_request_id: format!("cancel-{n}"),
                        method: ExecutionMethodV2::CancelOrder,
                        target_ordinal: 0,
                        input_item_refs: vec![CancelInputItemRefV2 {
                            item_id: format!("cancel-item-{n}"),
                            batch_item_ordinal: 0,
                        }],
                    }
                } else {
                    SimV2DirectResponseAssociationV1::Request {
                        global_request_id: format!("query-{n}"),
                    }
                },
            }
        } else if matches!(kinds.first(), Some(Kind::OrderLifecycle { .. })) {
            SimV2SourceRecordReferenceV1::OrderLifecycle {
                private_feedback_sequence: n,
                platform_event_at_utc_ms: 100 + n as i64,
                feedback_received_at_utc_ms: 200 + n as i64,
            }
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
                    source_watermark: watermark,
                    evidence: kinds
                        .into_iter()
                        .enumerate()
                        .map(|(ordinal, kind)| SourceNeutralEvidence {
                            source_record: record.clone(),
                            source_ordinal: ordinal as u32,
                            logical_key: SourceNeutralLogicalKeyV1::EconomicEvidence {
                                evidence_identity: format!("effect-{n}-{ordinal}"),
                            },
                            kind,
                        })
                        .collect(),
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
    verify_memory_recovery(false);
    verify_memory_recovery(true);
}

fn verify_memory_recovery(same_batch: bool) {
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
    let first = Kind::Economic {
        evidence: SourceNeutralEconomicEvidenceV1::AccountTransition {
            account_transition_seq: 1,
            economic_effective_at_utc_ms: 101,
            condition_id: None,
            transition: SourceNeutralAccountTransitionKindV1::MakerRebateAccrualFinalized,
            balance_delta: Box::new(delta("0", "0", "50")),
        },
    };
    let second = Kind::Economic {
        evidence: SourceNeutralEconomicEvidenceV1::AccountTransition {
            account_transition_seq: 2,
            economic_effective_at_utc_ms: 102,
            condition_id: None,
            transition: SourceNeutralAccountTransitionKindV1::MakerRebatePaid {
                attribution_day_utc_ms: 86_400_000,
            },
            balance_delta: Box::new(delta("50", "50", "-50")),
        },
    };
    if same_batch {
        source.accept_many(vec![first, second]);
    } else {
        source.accept(first);
        source.accept(second);
    }
    source.stable();
    assert!(opening.durable_reference().is_some());
    assert!(
        source.source.take_closed(199, 199).is_empty(),
        "后台发布不提前业务领取"
    );
    let records = source.source.take_closed(500, 700);
    assert_eq!(records.len(), 2);
    assert_eq!(
        records[0].position().source_watermark == records[1].position().source_watermark,
        same_batch
    );
    assert_eq!(
        records[0].position().record_ordinal == records[1].position().record_ordinal,
        !same_batch
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
    // 同一真实三类材料在清单发布失败时，只能恢复先前已提交的Genesis。
    let failed_root = temp.path().join("ledger-manifest-failure");
    let mut failed = LedgerBackgroundWriterV2::new(&failed_root);
    assert!(failed.write(&material[0]).unwrap());
    failed.finish().unwrap();
    for m in &material[1..] {
        assert!(failed.write(m).unwrap());
    }
    std::fs::write(
        failed_root.join(".ledger-parquet-00000000000000000001.manifest.json.tmp"),
        b"incomplete",
    )
    .unwrap();
    assert!(failed.finish().is_err());
    assert!(failed.finish().is_err(), "失败句柄不能报告稳定成功");
    drop(failed);
    assert!(
        LedgerWriterV1::open_durable(&failed_root)
            .unwrap()
            .projection_v2()
            .is_none(),
        "不可变分片存在但清单未提交，不恢复其中Transaction或ReservationDecision"
    );
    assert!(!failed_root.join("ledger-active-v1.jsonl").exists());

    // 使用真实 Parquet 读取/写入补回旧已持久引用字段，再经正式 Ledger 恢复。
    // 分片摘要仍属于尚未改动的 Ledger 格式，因此这里只更新夹具的真实文件摘要。
    {
        use sha2::{Digest, Sha256};
        fn old_reference(value: &mut serde_json::Value) -> usize {
            match value {
                serde_json::Value::Object(fields) => {
                    let mut count = 0;
                    if let Some(container) = fields
                        .get_mut("commit_container")
                        .and_then(serde_json::Value::as_object_mut)
                    {
                        container
                            .insert("boundary_sha256".into(), serde_json::json!("a".repeat(64)));
                        count += 1;
                    }
                    count + fields.values_mut().map(old_reference).sum::<usize>()
                }
                serde_json::Value::Array(values) => values.iter_mut().map(old_reference).sum(),
                _ => 0,
            }
        }
        let manifest_path = temp
            .path()
            .join("ledger/ledger-parquet-00000000000000000000.manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        let path = std::path::PathBuf::from(manifest["location"].as_str().unwrap());
        let (boundary, mut rows) =
            ::execution_normalization::record_file::read(&path, "ledger_arrow_parquet_v1").unwrap();
        let mut inserted = 0;
        for (_, _, bytes) in &mut rows {
            let mut value: serde_json::Value = serde_json::from_slice(bytes).unwrap();
            inserted += old_reference(&mut value);
            *bytes = serde_json::to_vec(&value).unwrap();
        }
        assert!(inserted > 0);
        let replacement = path.with_extension("legacy-fixture");
        ::execution_normalization::record_file::write(
            &replacement,
            "ledger_arrow_parquet_v1",
            &boundary,
            &rows,
        )
        .unwrap();
        std::fs::rename(replacement, &path).unwrap();
        manifest["sha256"] = serde_json::json!(format!(
            "{:x}",
            Sha256::digest(std::fs::read(&path).unwrap())
        ));
        manifest["first_durable_record_seq"] = serde_json::json!(1);
        manifest["last_durable_record_seq"] = serde_json::json!(rows.last().unwrap().0);
        manifest["first_ledger_entry_seq"] = serde_json::json!(1);
        manifest["last_ledger_entry_seq"] =
            serde_json::json!(writer.projection_v2().unwrap().ledger_entry_seq);
        std::fs::write(manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    }
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
    // 恢复后原本地请求沿同一Parquet记录序列续写，不生成active JSONL。
    let fresh = LedgerRequestIdentityV1("new-after-stable-recovery".into());
    restored
        .try_reserve_intent(&fresh, &condition, &number("1"), &number("0"))
        .unwrap();
    let after_append = restored.projection_v2();
    assert!(
        restored.seal_active_segment().unwrap().is_none(),
        "恢复后的同步追加已稳定"
    );
    let segment: LedgerSealedSegmentReferenceV1 = serde_json::from_slice(
        &std::fs::read(
            temp.path()
                .join("ledger/ledger-parquet-00000000000000000001.manifest.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(segment.location.ends_with(".parquet"));
    drop(restored);
    assert_eq!(
        LedgerWriterV1::open_durable(temp.path().join("ledger"))
            .unwrap()
            .projection_v2(),
        after_append
    );
    assert!(!temp.path().join("ledger/ledger-active-v1.jsonl").exists());
    std::fs::OpenOptions::new()
        .write(true)
        .open(segment.location)
        .unwrap()
        .set_len(12)
        .unwrap();
    assert!(
        LedgerWriterV1::open_durable(temp.path().join("ledger")).is_err(),
        "已提交Parquet损坏不能静默回退到先前账户"
    );
}

#[test]
fn query_pages_keep_their_committed_sequence_and_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let mut source = Source::new(&temp.path().join("noe"), "query-pages");
    source.accept(Kind::Economic {
        evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
            opening_evidence_identity: "opening".into(),
            available_pusd: number("100"),
        },
    });
    let mut writer = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: number("1000"),
    });
    let materials = Arc::new(Mutex::new(Vec::new()));
    let sink = materials.clone();
    writer.bind_record_sink_v2(Box::new(move |m| sink.lock().unwrap().push(m)));
    writer
        .apply_noe_memory_v1(&source.source.opening_record().unwrap())
        .unwrap();
    source.accept(Kind::Control {
        result: Control::OrdersQuery {
            result: SourceNeutralOrdersQueryResult::Page(SourceNeutralOrdersPage {
                root_sequence: 1,
                items: vec![],
                next_cursor: None,
            }),
        },
    });
    source.accept(Kind::Control {
        result: Control::TradesQuery {
            result: SourceNeutralTradesQueryResultV1::Page(SourceNeutralTradesPageV1 {
                root_sequence: 2,
                items: vec![],
                next_cursor: None,
            }),
        },
    });
    source.stable();
    let records = source.source.take_closed(500, 700);
    assert_eq!(records.len(), 2);
    for (i, record) in records.iter().enumerate() {
        assert_eq!(
            writer.apply_noe_memory_v1(record).unwrap(),
            LedgerMemoryApplyOutcomeV1::Applied(LedgerApplyOutcomeV1::NewlyApplied {
                ledger_entry_seq: i as u64 + 2,
            })
        );
    }
    assert_eq!(
        writer
            .projection_v2()
            .unwrap()
            .platform_confirmed_projection
            .cash_pusd,
        number("100")
    );
    let root = temp.path().join("ledger");
    let mut background = LedgerBackgroundWriterV2::new(&root);
    for material in materials.lock().unwrap().iter() {
        assert!(background.write(material).unwrap());
    }
    background.finish().unwrap();
    let mut restored = LedgerWriterV1::open_durable(&root).unwrap();
    assert_eq!(restored.projection_v2(), writer.projection_v2());
    for (i, record) in records.iter().enumerate() {
        assert_eq!(
            restored.apply_noe_memory_v1(record).unwrap(),
            LedgerMemoryApplyOutcomeV1::Applied(LedgerApplyOutcomeV1::AlreadyApplied {
                ledger_entry_seq: i as u64 + 2,
            })
        );
    }
}

#[test]
fn terminal_success_read_is_not_economic_application() {
    let temp = tempfile::tempdir().unwrap();
    let mut source = Source::new(&temp.path().join("noe"), "success-before-economic");
    source.accept(Kind::Economic {
        evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
            opening_evidence_identity: "opening".into(),
            available_pusd: number("100"),
        },
    });
    let mut writer =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    writer
        .apply_noe_memory_v1(&source.source.opening_record().unwrap())
        .unwrap();
    let account = Arc::new(source.context.account_identity.clone());
    let trade = Identity::entity(
        &account,
        &SourceNeutralEntityKeyV1::Trade {
            source_trade_id: "trade".into(),
        },
    )
    .unwrap();
    let order = Identity::entity(
        &account,
        &SourceNeutralEntityKeyV1::Order {
            source_order_id: "order".into(),
        },
    )
    .unwrap();
    source.accept(Kind::TradeLifecycle {
        result: Trade {
            trade_identity: trade,
            order_identity: order,
            condition_id: ConditionId("condition".into()),
            token_id: TokenId("up".into()),
            state: SourceNeutralTradeStateV1::Matched,
            economic_disposition: SourceNeutralTradeEconomicDispositionV1::FinalSuccess,
            fill_sequence: 1,
            matched_at_utc_ms: 100,
            liquidity_role: TradeLiquidityRoleV2::Taker,
            source_price_pusd_per_share: number("0.5"),
            base_quantity_shares: number("2"),
            quote_amount_pusd: number("1"),
            immediate_taker_fee_pusd: number("0.01"),
            economic_application_account_transition_seq: Some(1),
        },
    });
    source.stable();
    for record in source.source.take_closed(500, 700) {
        writer.apply_noe_memory_v1(&record).unwrap();
    }
    let read = writer.economic_read().unwrap();
    let trade = read.trades().next().unwrap();
    assert_eq!(
        trade.fact.economic_disposition,
        SourceNeutralTradeEconomicDispositionV1::FinalSuccess
    );
    assert!(!trade.economic_applied && !trade.success_applied);
    assert_eq!(trade.actual_net_fee_pusd, number("0"));
    assert_eq!(
        trade.unpaired_contribution_shares,
        ExecutionReadValueV1::NotApplicable
    );
    assert_eq!(read.cash().actual_available_pusd, number("100"));
}

fn order_identity(source: &Source, id: &str) -> Identity {
    Identity::entity(
        &Arc::new(source.context.account_identity.clone()),
        &SourceNeutralEntityKeyV1::Order {
            source_order_id: id.into(),
        },
    )
    .unwrap()
}
fn accepted_quantity(order: &Identity, matched: &str, open: &str) -> Kind {
    Kind::Control {
        result: Control::Submission {
            outcome: SourceNeutralSubmissionOutcomeV2::Accepted {
                order: SourceNeutralOrderSnapshotV1 {
                    order_identity: order.clone(),
                    condition_id: ConditionId("condition".into()),
                    token_id: TokenId("up".into()),
                    state: SourceNeutralOrderStateV1::Open,
                },
                measurement: SourceNeutralSubmissionMeasurementV1::Base {
                    matched_base_quantity_shares: number(matched),
                    open_base_quantity_shares: number(open),
                },
                effective_processing_at_utc_ms: 100,
            },
        },
    }
}
fn counted_trade(
    source: &Source,
    order: &Identity,
    id: &str,
    quantity: &str,
    role: TradeLiquidityRoleV2,
    disposition: SourceNeutralTradeEconomicDispositionV1,
    modern: bool,
) -> Kind {
    let state = match disposition {
        SourceNeutralTradeEconomicDispositionV1::PendingUnapplied
        | SourceNeutralTradeEconomicDispositionV1::PendingApplied => {
            SourceNeutralTradeStateV1::Matched
        }
        SourceNeutralTradeEconomicDispositionV1::FinalSuccess => {
            SourceNeutralTradeStateV1::Confirmed
        }
        _ => SourceNeutralTradeStateV1::Failed,
    };
    let price = if id == "sim-257" { "0.81" } else { "0.5" };
    let result = Trade {
        trade_identity: Identity::entity(
            &Arc::new(source.context.account_identity.clone()),
            &SourceNeutralEntityKeyV1::Trade {
                source_trade_id: id.into(),
            },
        )
        .unwrap(),
        order_identity: order.clone(),
        condition_id: ConditionId("condition".into()),
        token_id: TokenId("up".into()),
        state,
        economic_disposition: disposition.clone(),
        fill_sequence: id.trim_start_matches("sim-").parse().unwrap(),
        matched_at_utc_ms: 100,
        liquidity_role: role,
        source_price_pusd_per_share: number(price),
        base_quantity_shares: number(quantity),
        quote_amount_pusd: number(
            &(quantity.parse::<rust_decimal::Decimal>().unwrap()
                * price.parse::<rust_decimal::Decimal>().unwrap())
            .to_string(),
        ),
        immediate_taker_fee_pusd: number("0"),
        economic_application_account_transition_seq: matches!(
            disposition,
            SourceNeutralTradeEconomicDispositionV1::PendingApplied
                | SourceNeutralTradeEconomicDispositionV1::FinalFailureCompensated
        )
        .then_some(1),
    };
    if modern {
        Kind::TradeLifecycleV3 {
            result: contracts::economics::SourceNeutralTradeLifecycleResultV3 {
                result,
                maker_fee_equivalent_pusd: ExecutionReadValueV1::Known(number("0")),
            },
        }
    } else {
        Kind::TradeLifecycle { result }
    }
}
fn assert_counts(writer: &LedgerWriterV1, open: u64, pending: u64) {
    let projection = writer.projection_v2().unwrap();
    assert_eq!(
        projection.platform_confirmed_projection.open_order_count,
        open
    );
    assert_eq!(
        projection.platform_confirmed_projection.pending_fill_count,
        pending
    );
    assert_eq!(
        projection
            .market_projections
            .iter()
            .map(|m| m.open_order_count)
            .sum::<u64>(),
        open
    );
    assert_eq!(
        projection
            .market_projections
            .iter()
            .map(|m| m.pending_fill_count)
            .sum::<u64>(),
        pending
    );
    assert_eq!(
        projection.platform_confirmed_projection.cash_pusd,
        number("100")
    );
    assert_eq!(
        projection.platform_confirmed_projection.fees_pusd,
        number("0")
    );
}
#[test]
fn maker_full_match_closes_open_quantity_once_independent_of_finality_and_recovery() {
    use SourceNeutralTradeEconomicDispositionV1::*;
    let temp = tempfile::tempdir().unwrap();
    let mut source = Source::new(&temp.path().join("noe"), "maker-open-count");
    source.accept(Kind::Economic {
        evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
            opening_evidence_identity: "opening".into(),
            available_pusd: number("100"),
        },
    });
    let order = order_identity(&source, "sim-413");
    source.accept(accepted_quantity(&order, "0", "39.45"));
    // 原事故数量：sim-257 耗尽 sim-413 的全部 39.45 份开放余量。
    source.accept(counted_trade(
        &source,
        &order,
        "sim-257",
        "39.45",
        TradeLiquidityRoleV2::MakerInferred,
        PendingUnapplied,
        true,
    ));
    source.accept(counted_trade(
        &source,
        &order,
        "sim-257",
        "39.45",
        TradeLiquidityRoleV2::MakerInferred,
        FinalSuccess,
        true,
    ));
    source.accept(counted_trade(
        &source,
        &order,
        "sim-257",
        "39.45",
        TradeLiquidityRoleV2::MakerInferred,
        PendingUnapplied,
        true,
    ));
    source.accept(accepted_quantity(&order, "0", "39.45"));
    source.stable();
    let records = source.source.take_closed(500, 700);
    assert_eq!(records.len(), 5);
    let mut writer =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    let materials = Arc::new(Mutex::new(Vec::new()));
    let sink = materials.clone();
    writer.bind_record_sink_v2(Box::new(move |m| sink.lock().unwrap().push(m)));
    writer
        .apply_noe_memory_v1(&source.source.opening_record().unwrap())
        .unwrap();
    writer.apply_noe_memory_v1(&records[0]).unwrap();
    assert_counts(&writer, 1, 0);
    writer.apply_noe_memory_v1(&records[1]).unwrap();
    assert_counts(&writer, 0, 1);
    let mut checkpoint = Vec::new();
    writer.write_checkpoint(&mut checkpoint).unwrap();
    // 只有恢复支继续原耐久序列；不中断支仅作业务状态对照，不再次写入尾部。
    writer.reconnect_record_sink_v2(Box::new(|_| {}));
    let mut restored = LedgerWriterV1::restore_checkpoint(checkpoint.as_slice()).unwrap();
    let sink = materials.clone();
    restored.reconnect_record_sink_v2(Box::new(move |m| sink.lock().unwrap().push(m)));
    for record in &records[2..] {
        writer.apply_noe_memory_v1(record).unwrap();
        restored.apply_noe_memory_v1(record).unwrap();
        assert_counts(&restored, 0, 0);
        assert_eq!(writer.projection_v2(), restored.projection_v2());
    }
    for record in &records {
        restored.apply_noe_memory_v1(record).unwrap();
    }
    assert_counts(&restored, 0, 0);
    let root = temp.path().join("ledger");
    let mut background = LedgerBackgroundWriterV2::new(&root);
    for material in materials.lock().unwrap().iter() {
        background.write(material).unwrap();
    }
    background.finish().unwrap();
    let mut cold = LedgerWriterV1::open_durable(&root).unwrap();
    assert_eq!(cold.projection_v2(), restored.projection_v2());
    for record in &records {
        cold.apply_noe_memory_v1(record).unwrap();
    }
    assert_counts(&cold, 0, 0);
}
#[test]
fn initial_taker_is_not_deducted_again_and_late_acceptance_keeps_maker_depletion() {
    use SourceNeutralTradeEconomicDispositionV1::*;
    let temp = tempfile::tempdir().unwrap();
    let mut source = Source::new(&temp.path().join("noe"), "mixed-open-count");
    source.accept(Kind::Economic {
        evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
            opening_evidence_identity: "opening".into(),
            available_pusd: number("100"),
        },
    });
    let order = order_identity(&source, "sim-414");
    source.accept(counted_trade(
        &source,
        &order,
        "sim-258",
        "2",
        TradeLiquidityRoleV2::MakerInferred,
        PendingUnapplied,
        false,
    ));
    source.accept(accepted_quantity(&order, "4", "6"));
    source.accept(counted_trade(
        &source,
        &order,
        "sim-259",
        "4",
        TradeLiquidityRoleV2::Taker,
        FinalSuccess,
        false,
    ));
    source.accept(counted_trade(
        &source,
        &order,
        "sim-258",
        "2",
        TradeLiquidityRoleV2::MakerInferred,
        FinalFailureUnapplied,
        false,
    ));
    source.accept(counted_trade(
        &source,
        &order,
        "sim-260",
        "4",
        TradeLiquidityRoleV2::MakerInferred,
        PendingApplied,
        false,
    ));
    source.accept(counted_trade(
        &source,
        &order,
        "sim-260",
        "4",
        TradeLiquidityRoleV2::MakerInferred,
        FinalFailureCompensated,
        false,
    ));
    source.accept(Kind::OrderLifecycle {
        result: SourceNeutralOrderLifecycleResultV1 {
            order_identity: order.clone(),
            condition_id: ConditionId("condition".into()),
            token_id: TokenId("up".into()),
            event: SourceNeutralOrderLifecycleEventV1::Accepted {
                state: SourceNeutralOrderStateV1::Open,
            },
        },
    });
    source.stable();
    let records = source.source.take_closed(500, 700);
    assert_eq!(records.len(), 7);
    let mut writer =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    writer
        .apply_noe_memory_v1(&source.source.opening_record().unwrap())
        .unwrap();
    for (record, (open, pending)) in
        records
            .iter()
            .zip([(0, 1), (1, 1), (1, 1), (1, 0), (0, 1), (0, 0), (0, 0)])
    {
        writer.apply_noe_memory_v1(record).unwrap();
        assert_counts(&writer, open, pending);
    }
}

#[test]
fn canceled_remainder_and_late_match_keep_open_and_finality_counts_separate() {
    use SourceNeutralTradeEconomicDispositionV1::*;
    let temp = tempfile::tempdir().unwrap();
    let mut source = Source::new(&temp.path().join("noe"), "cancel-open-count");
    source.accept(Kind::Economic {
        evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
            opening_evidence_identity: "opening".into(),
            available_pusd: number("100"),
        },
    });
    let order = order_identity(&source, "sim-415");
    source.accept(accepted_quantity(&order, "0", "5"));
    source.accept(counted_trade(
        &source,
        &order,
        "sim-261",
        "2",
        TradeLiquidityRoleV2::MakerInferred,
        PendingUnapplied,
        true,
    ));
    for outcome in [
        SourceNeutralCancellationOutcomeV1::EvidenceUnknownNoEffect,
        SourceNeutralCancellationOutcomeV1::NotCanceledNoEffect,
        SourceNeutralCancellationOutcomeV1::Canceled,
    ] {
        source.accept(Kind::Control {
            result: Control::Cancellation {
                order_identity: order.clone(),
                outcome,
            },
        });
    }
    // 成交先于撤单发生，成交反馈随后到达。
    source.accept(counted_trade(
        &source,
        &order,
        "sim-262",
        "1",
        TradeLiquidityRoleV2::MakerInferred,
        PendingUnapplied,
        true,
    ));
    source.accept(accepted_quantity(&order, "0", "5"));
    source.accept(counted_trade(
        &source,
        &order,
        "sim-261",
        "2",
        TradeLiquidityRoleV2::MakerInferred,
        FinalFailureUnapplied,
        true,
    ));
    source.accept(counted_trade(
        &source,
        &order,
        "sim-262",
        "1",
        TradeLiquidityRoleV2::MakerInferred,
        FinalSuccess,
        true,
    ));
    source.stable();
    let records = source.source.take_closed(500, 700);
    assert_eq!(records.len(), 9);
    let mut writer =
        LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
    writer
        .apply_noe_memory_v1(&source.source.opening_record().unwrap())
        .unwrap();
    for (record, (open, pending)) in records.iter().zip([
        (1, 0),
        (1, 1),
        (1, 1),
        (1, 1),
        (0, 1),
        (0, 2),
        (0, 2),
        (0, 1),
        (0, 0),
    ]) {
        writer.apply_noe_memory_v1(record).unwrap();
        assert_counts(&writer, open, pending);
    }
}
