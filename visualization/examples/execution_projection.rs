//! 仅展示验证：原 Ledger typed 入口产生非零投影，外层不是宿主研究报告。
use contracts::execution_normalization::*;
use contracts::{ConditionId, ExactDecimal, TokenId};
use execution_normalization::*;
use ledger::*;
#[allow(dead_code)]
#[path = "noe_evidence.rs"]
pub mod noe;
use std::path::Path;
fn dec(s: &str) -> ExactDecimal {
    ExactDecimal(s.into())
}
pub fn projection(root: &Path) -> LedgerProjectionV2 {
    let refs = noe::sample::<SourceNeutralTradeLifecycleResultV1, SourceNeutralControlResultV1>(
        &root.join("opening"),
        "v1",
        SourceNeutralControlResultV1::Submission {
            outcome: SourceNeutralSubmissionOutcomeV1::RejectedNoEffect,
        },
        SourceNeutralControlResultV1::Submission {
            outcome: SourceNeutralSubmissionOutcomeV1::EvidenceUnknownNoEffect,
        },
    );
    let opening = refs
        .iter()
        .find(|r| {
            matches!(
                read_noe_result_v1(r).unwrap().record,
                NormalizationRecordV1::EconomicFact { .. }
            )
        })
        .unwrap();
    let mut ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: dec("1000"),
    });
    ledger.apply_noe_result(opening).unwrap();
    let first = ConditionId(format!("0x{}", "1".repeat(64)));
    let second = ConditionId(format!("0x{}", "2".repeat(64)));
    for (name, market, principal, fee, send) in [
        ("before", &first, "1.1", "0", false),
        ("unknown", &first, "2.2", "0.02", true),
        ("confirmed", &first, "3.3", "0.03", true),
        ("projection-only", &second, "4.4", "0", false),
    ] {
        let req = LedgerRequestIdentityV1(name.into());
        ledger
            .try_reserve_intent(&req, market, &dec(principal), &dec(fee))
            .unwrap();
        if send {
            ledger
                .record_submission_started(
                    &LedgerLocalEventIdentityV1(format!("sent-{name}")),
                    &req,
                    &LedgerSubmissionItemIdentityV1 {
                        global_request_id: name.into(),
                        batch_item_ordinal: 0,
                    },
                )
                .unwrap();
        }
    }
    let context = read_noe_result_v1(opening).unwrap().context;
    let mut core = NormalizationCoreV1::new(context.clone());
    let mut writer = StagedNormalizationWriter::<
        SourceNeutralTradeLifecycleResultV1,
        SourceNeutralControlResultV1,
    >::create(&root.join("feedback"), context.clone())
    .unwrap();
    let batch = core
        .normalize_and_accept(NormalizationBatchInputV1 {
            source_watermark: SimV2SourceWatermarkV1 {
                run_id: context.source.account_instance_key.run_id.clone(),
                projection_batch_sequence: 9007199254740994,
            },
            evidence: vec![
                SourceNeutralEvidenceV1 {
                    source_record: SimV2SourceRecordReferenceV1::DirectResponse {
                        private_feedback_sequence: 9007199254740997,
                        platform_responded_at_utc_ms: 1788840000003,
                        response_received_at_utc_ms: 1788840000004,
                        association: SimV2DirectResponseAssociationV1::SubmissionItem {
                            global_request_id: "confirmed".into(),
                            batch_item_ordinal: 0,
                        },
                    },
                    source_ordinal: 0,
                    logical_key: SourceNeutralLogicalKeyV1::SubmissionItem {
                        global_request_id: "confirmed".into(),
                        batch_item_ordinal: 0,
                    },
                    kind: SourceNeutralEvidenceKindV1::Control {
                        result: SourceNeutralControlResultV1::Submission {
                            outcome: SourceNeutralSubmissionOutcomeV1::Accepted {
                                order: SourceNeutralOrderSnapshotV1 {
                                    order_identity: StableLogicalIdentityV1(
                                        "engineering-open-order".into(),
                                    ),
                                    condition_id: first.clone(),
                                    token_id: TokenId("engineering-up".into()),
                                    state: SourceNeutralOrderStateV1::Open,
                                },
                            },
                        },
                    },
                },
                SourceNeutralEvidenceV1 {
                    source_record: SimV2SourceRecordReferenceV1::TradeLifecycle {
                        private_feedback_sequence: 9007199254740998,
                        platform_event_at_utc_ms: 1788840000005,
                        feedback_received_at_utc_ms: 1788840000006,
                    },
                    source_ordinal: 0,
                    logical_key: SourceNeutralLogicalKeyV1::TradeLifecycle {
                        trade_identity: "engineering-pending-trade".into(),
                        occurrence: 9007199254740998,
                    },
                    kind: SourceNeutralEvidenceKindV1::TradeLifecycle {
                        result: SourceNeutralTradeLifecycleResultV1 {
                            trade_identity: StableLogicalIdentityV1(
                                "engineering-pending-trade".into(),
                            ),
                            order_identity: StableLogicalIdentityV1(
                                "engineering-open-order".into(),
                            ),
                            condition_id: first,
                            token_id: TokenId("engineering-up".into()),
                            state: SourceNeutralTradeStateV1::Matched,
                            economic_disposition:
                                SourceNeutralTradeEconomicDispositionV1::PendingUnapplied,
                        },
                    },
                },
            ],
        })
        .unwrap();
    writer.stage(&batch).unwrap();
    writer
        .stabilize(
            || 1788840000007,
            |p| {
                for r in p.result_references() {
                    ledger.apply_noe_result(&r).unwrap();
                }
            },
        )
        .unwrap();
    ledger.projection_v2().unwrap()
}
fn main() {
    let root = std::path::PathBuf::from(std::env::args().nth(1).expect("新展示验证目录"));
    std::fs::create_dir(&root).unwrap();
    eprintln!("Ledger 原入口：开账、预留、发送、平台确认与待定成交；非研究运行");
    let p = projection(&root);
    // 仅为已存在 JSON 展示入口的边界夹具；不声称这个包装是 backtest 生产报告。
    let report = root.join("display-boundary.json");
    std::fs::write(&report,serde_json::to_vec_pretty(&serde_json::json!({"engineering_display_boundary":"原 Ledger 投影；外层非宿主报告/策略研究","ledger_consumed_memory":p})).unwrap()).unwrap();
    std::fs::write(root.join("sources.json"),serde_json::to_vec_pretty(&serde_json::json!({"runs":[{"label":"Ledger 原投影展示边界 · 外层非宿主报告/策略研究","report":report,"report_kind":"engineering_ledger_projection"}]})).unwrap()).unwrap();
}
