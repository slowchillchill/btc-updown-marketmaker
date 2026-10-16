//! 有限时钟工程材料：原 typed 核心/writer，不是宿主/策略研究。
#[allow(dead_code)]
#[path = "noe_evidence.rs"]
mod generator;
use contracts::{
    ConditionId, ExactDecimal, ExecutionReadValueV1, TokenId, TradeLiquidityRoleV2, economics::*,
    execution_normalization::*,
};
use execution_normalization::*;

fn dec(s: &str) -> ExactDecimal {
    ExactDecimal(s.into())
}
fn main() {
    let root = std::path::PathBuf::from(std::env::args().nth(1).expect("新时钟工程目录"));
    std::fs::create_dir(&root).unwrap();
    let seed = generator::sample::<SourceNeutralTradeLifecycleResultV2, SourceNeutralControlResultV3>(
        &root.join("seed"),
        "v4",
        SourceNeutralControlResultV3::Submission {
            outcome: SourceNeutralSubmissionOutcomeV2::RejectedNoEffect,
        },
        SourceNeutralControlResultV3::Submission {
            outcome: SourceNeutralSubmissionOutcomeV2::EvidenceUnknownNoEffect,
        },
    );
    let context = read_noe_result_v4(&seed[0]).unwrap().context;
    let mut core = NormalizationCore::<
        SourceNeutralTradeLifecycleResultV2,
        SourceNeutralControlResultV3,
    >::new(context.clone());
    let mut writer = StagedNormalizationWriter::<
        SourceNeutralTradeLifecycleResultV2,
        SourceNeutralControlResultV3,
    >::create_batched(&root.join("clocks"), context.clone())
    .unwrap();
    let condition = ConditionId(format!("0x{}", "3".repeat(64)));
    let order = StableLogicalIdentityV1("clock-order".into());
    let trade = SourceNeutralTradeLifecycleResultV2 {
        trade_identity: StableLogicalIdentityV1("clock-trade".into()),
        order_identity: order.clone(),
        condition_id: condition.clone(),
        token_id: TokenId("clock-up".into()),
        state: SourceNeutralTradeStateV1::Matched,
        economic_disposition: SourceNeutralTradeEconomicDispositionV1::PendingUnapplied,
        fill_sequence: 1,
        matched_at_utc_ms: -3,
        liquidity_role: TradeLiquidityRoleV2::MakerInferred,
        source_price_pusd_per_share: dec("1"),
        base_quantity_shares: dec("1"),
        quote_amount_pusd: dec("1"),
        immediate_taker_fee_pusd: dec("0"),
        economic_application_account_transition_seq: None,
    };
    let delta = SourceNeutralAccountBalanceDeltaV1 {
        cash_pusd_delta: dec("0"),
        available_pusd_delta: dec("-1"),
        principal_encumbrance_pusd_delta: dec("1"),
        immediate_taker_fee_encumbrance_pusd_delta: dec("0"),
        outcome_balance_deltas: vec![],
        maker_rebate_receivable_pusd_delta: dec("0"),
        taker_rebate_receivable_pusd_delta: dec("0"),
        maker_rebate_locked_remainder_pusd_delta: dec("0"),
        taker_rebate_locked_remainder_pusd_delta: dec("0"),
    };
    let kinds: Vec<
        SourceNeutralEvidenceKind<
            SourceNeutralTradeLifecycleResultV2,
            SourceNeutralControlResultV3,
        >,
    > = vec![
        SourceNeutralEvidenceKind::Control {
            result: SourceNeutralControlResultV3::Submission {
                outcome: SourceNeutralSubmissionOutcomeV2::Accepted {
                    order: SourceNeutralOrderSnapshotV1 {
                        order_identity: order.clone(),
                        condition_id: condition.clone(),
                        token_id: TokenId("clock-up".into()),
                        state: SourceNeutralOrderStateV1::Open,
                    },
                    measurement: SourceNeutralSubmissionMeasurementV1::Base {
                        matched_base_quantity_shares: dec("0"),
                        open_base_quantity_shares: dec("1"),
                    },
                    effective_processing_at_utc_ms: 9007199254740993,
                },
            },
        },
        SourceNeutralEvidenceKind::TradeLifecycle {
            result: trade.clone(),
        },
        SourceNeutralEvidenceKind::TradeLifecycleV3 {
            result: SourceNeutralTradeLifecycleResultV3 {
                result: trade,
                maker_fee_equivalent_pusd: ExecutionReadValueV1::Known(dec("0")),
            },
        },
        SourceNeutralEvidenceKind::Economic {
            evidence: SourceNeutralEconomicEvidenceV1::AccountTransition {
                account_transition_seq: 1,
                economic_effective_at_utc_ms: 1788840000000,
                condition_id: Some(condition.clone()),
                transition: SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
                    order_identity: order.clone(),
                },
                balance_delta: Box::new(delta.clone()),
            },
        },
        SourceNeutralEvidenceKind::EconomicV2 {
            evidence: SourceNeutralEconomicEvidenceV2 {
                economic_key: SourceNeutralEconomicKeyV1::AccountTransition {
                    account_transition_seq: 2,
                },
                disposition: SourceNeutralEconomicDispositionV2::AccountTransition {
                    economic_effective_at_utc_ms: 1788840000000,
                    condition_id: Some(condition.clone()),
                    transition: SourceNeutralAccountTransitionKindV2::Existing(
                        SourceNeutralAccountTransitionKindV1::OrderAcceptedEncumbranceApplied {
                            order_identity: order,
                        },
                    ),
                    balance_delta: Box::new(SourceNeutralAccountBalanceDeltaV2 {
                        existing: delta,
                        settlement_fee_liability_pusd_delta: dec("0"),
                        actual_taker_fee_liability_pusd_delta: dec("0"),
                    }),
                },
            },
        },
        SourceNeutralEvidenceKind::SettlementLifecycle {
            result: SourceNeutralSettlementLifecycleResultV1::AutoRedeemApplied {
                condition_id: condition.clone(),
                resolution_identity: StableLogicalIdentityV1("clock-resolution".into()),
                resolution_source_event_at_utc_ms: Some(1788840000000),
                resolution_public_consumed_at_utc_ms: None,
            },
        },
        SourceNeutralEvidenceKind::SettlementLifecycle {
            result: SourceNeutralSettlementLifecycleResultV1::AutoRedeemApplied {
                condition_id: condition.clone(),
                resolution_identity: StableLogicalIdentityV1("clock-gamma-resolution".into()),
                resolution_source_event_at_utc_ms: None,
                resolution_public_consumed_at_utc_ms: Some(1788840000000),
            },
        },
    ];
    let batch = core
        .normalize_and_accept(NormalizationBatchInput {
            source_watermark: SimV2SourceWatermarkV1 {
                run_id: context.source.account_instance_key.run_id.clone(),
                projection_batch_sequence: 9007199254740994,
            },
            evidence: kinds
                .into_iter()
                .enumerate()
                .map(|(i, kind)| SourceNeutralEvidence {
                    source_record: match i {
                        1 | 2 => SimV2SourceRecordReferenceV1::TradeLifecycle {
                            private_feedback_sequence: i as u64 + 1,
                            platform_event_at_utc_ms: 1788840000000,
                            feedback_received_at_utc_ms: 1788840000000,
                        },
                        3 | 4 => SimV2SourceRecordReferenceV1::OrderLifecycle {
                            private_feedback_sequence: i as u64 + 1,
                            platform_event_at_utc_ms: 1788840000000,
                            feedback_received_at_utc_ms: 1788840000000,
                        },
                        5 | 6 => SimV2SourceRecordReferenceV1::SettlementLifecycle {
                            private_feedback_sequence: i as u64 + 1,
                            platform_event_at_utc_ms: 1788840000000,
                            feedback_received_at_utc_ms: 1788840000000,
                        },
                        _ => SimV2SourceRecordReferenceV1::DirectResponse {
                            private_feedback_sequence: 1,
                            platform_responded_at_utc_ms: 1788840000000,
                            response_received_at_utc_ms: 1788840000000,
                            association: SimV2DirectResponseAssociationV1::SubmissionItem {
                                global_request_id: "clock-0".into(),
                                batch_item_ordinal: 0,
                            },
                        },
                    },
                    source_ordinal: 0,
                    logical_key: SourceNeutralLogicalKeyV1::AuditEvidence {
                        evidence_identity: format!("clock-{i}"),
                    },
                    kind,
                })
                .collect(),
        })
        .unwrap();
    writer.stage(&batch).unwrap();
    let mut refs = vec![];
    writer
        .stabilize(
            || 1788840000002,
            |published| refs.extend(published.result_references()),
        )
        .unwrap();
    let mut decoded = vec![];
    let mut sources = vec![];
    for (i, reference) in refs.into_iter().enumerate() {
        let path = root.join(format!("clock-{i}.json"));
        std::fs::write(&path, serde_json::to_vec(&reference).unwrap()).unwrap();
        sources.push(visualization::dto::NoeSourceConfig {
            label: match i {
                5 => "WS 判定源时间工程样本".into(),
                6 => "Gamma 公开消费时间工程样本（模拟依据）".into(),
                _ => format!("时钟工程样本 {i}"),
            },
            version: "v4".into(),
            reference_file: path.clone(),
        });
        let mut reader = visualization::sources::noe::NoeResults::new(vec![
            visualization::dto::NoeSourceConfig {
                label: "时钟工程引用".into(),
                version: "v4".into(),
                reference_file: path,
            },
        ]);
        decoded
            .push(serde_json::from_slice::<serde_json::Value>(&reader.load(0).unwrap()).unwrap());
    }
    std::fs::write(
        root.join("decoded.json"),
        serde_json::to_vec_pretty(&decoded).unwrap(),
    )
    .unwrap();
    std::fs::write(
        root.join("sources.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "runs": [{"label":"NOE 时钟工程样本 · 非策略结果", "noe_results": sources}]
        }))
        .unwrap(),
    )
    .unwrap();
}
