//! 原强类型核心和发布 writer 返回引用；仅生成新工程目录，不是策略研究。
use contracts::execution_normalization::*;
use contracts::{AccountInstanceKeyV1, ExactDecimal, PlatformSourceV1, SimCallerRoleV1};
use execution_normalization::*;
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt::Debug, path::Path};

pub fn sample<T: NoeTradePayload<C>, C: Clone + Debug + Eq + Serialize + DeserializeOwned>(
    root: &Path,
    version: &str,
    control: C,
    conflicting: C,
) -> Vec<NoeResultReferenceV1> {
    let run_id = format!("engineering-noe-{version}");
    let context = sim_v2_context(
        SimV2ContextSeedV1 {
            run_id: run_id.clone(),
            account_instance_key: AccountInstanceKeyV1 {
                run_id: run_id.clone(),
                caller_role: SimCallerRoleV1::ModelTrain,
                caller_instance_id: "engineering-reader".into(),
                account_id: "engineering-account".into(),
                platform_source: PlatformSourceV1::Simulated,
            },
            run_opened: DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Location(
                    "artifact://engineering/run-opened".into(),
                ),
            },
            account_opening: DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Location(
                    "artifact://engineering/account-opening".into(),
                ),
            },
            account_opening_evidence_identity: "engineering-opening".into(),
            implementation_identity: "visualization-engineering-generator".into(),
        },
        T::CONTRACT_VERSION,
    )
    .unwrap();
    let mut writer = StagedNormalizationWriter::<T, C>::create(root, context.clone()).unwrap();
    let mut core = NormalizationCore::<T, C>::new(context);
    let source = |seq| SimV2SourceRecordReferenceV1::DirectResponse {
        private_feedback_sequence: seq,
        platform_responded_at_utc_ms: 1788840000000,
        response_received_at_utc_ms: 1788840000001,
        association: SimV2DirectResponseAssociationV1::SubmissionItem {
            global_request_id: "engineering-request".into(),
            batch_item_ordinal: 1,
        },
    };
    let evidence = |seq, result| SourceNeutralEvidence {
        source_record: source(seq),
        source_ordinal: 0,
        logical_key: SourceNeutralLogicalKeyV1::SubmissionItem {
            global_request_id: "engineering-request".into(),
            batch_item_ordinal: 1,
        },
        kind: SourceNeutralEvidenceKind::Control { result },
    };
    let opening = if version == "v4" {
        SourceNeutralEvidenceKind::EconomicV2 {
            evidence: SourceNeutralEconomicEvidenceV2 {
                economic_key: SourceNeutralEconomicKeyV1::AccountOpening {
                    opening_evidence_identity: "engineering-opening".into(),
                },
                disposition:
                    contracts::economics::SourceNeutralEconomicDispositionV2::AccountOpening {
                        available_pusd: ExactDecimal("9007199254740993.01".into()),
                    },
            },
        }
    } else {
        SourceNeutralEvidenceKind::Economic {
            evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
                opening_evidence_identity: "engineering-opening".into(),
                available_pusd: ExactDecimal("9007199254740993.01".into()),
            },
        }
    };
    let batch = core
        .normalize_and_accept(NormalizationBatchInput {
            source_watermark: SimV2SourceWatermarkV1 {
                run_id,
                projection_batch_sequence: 9007199254740993,
            },
            evidence: vec![
                evidence(9007199254740993, control.clone()),
                evidence(9007199254740994, control),
                evidence(9007199254740995, conflicting),
                SourceNeutralEvidence {
                    source_record: SimV2SourceRecordReferenceV1::AccountOpening,
                    source_ordinal: 0,
                    logical_key: SourceNeutralLogicalKeyV1::EconomicEvidence {
                        evidence_identity: "engineering-opening".into(),
                    },
                    kind: opening.clone(),
                },
                SourceNeutralEvidence {
                    source_record: SimV2SourceRecordReferenceV1::AccountOpening,
                    source_ordinal: 1,
                    logical_key: SourceNeutralLogicalKeyV1::EconomicEvidence {
                        evidence_identity: "engineering-opening-alias".into(),
                    },
                    kind: opening,
                },
                SourceNeutralEvidence {
                    source_record: source(9007199254740996),
                    source_ordinal: 0,
                    logical_key: SourceNeutralLogicalKeyV1::AuditEvidence {
                        evidence_identity: "engineering-pending".into(),
                    },
                    kind: SourceNeutralEvidenceKind::Pending {
                        scope: PendingScopeV1::EvidenceGap {
                            scope_identity: "missing-engineering-feedback".into(),
                        },
                    },
                },
            ],
        })
        .unwrap();
    writer.stage(&batch).unwrap();
    let mut references = Vec::new();
    writer
        .stabilize(
            || 1788840000002,
            |published| references.extend(published.result_references()),
        )
        .unwrap();
    references
}
fn main() {
    let root = std::path::PathBuf::from(std::env::args().nth(1).expect("指定新工程目录"));
    std::fs::create_dir(&root).expect("只创建新工程目录");
    let mut entries = Vec::new();
    macro_rules! generate {
        ($v:literal,$trade:ident,$control:ident,$outcome:ident)=>{{
            eprintln!("工程 NOE {}：核心规范化、暂存、稳定发布并保存原返回引用",$v);
            let refs=sample::<$trade,$control>(&root.join($v),$v,$control::Submission {outcome:$outcome::RejectedNoEffect},$control::Submission {outcome:$outcome::EvidenceUnknownNoEffect});
            for (i,reference) in refs.into_iter().enumerate() {
                let path=root.join(format!("{}-reference-{i}.json",$v));
                std::fs::write(&path,serde_json::to_vec_pretty(&reference).unwrap()).unwrap();
                entries.push(serde_json::json!({"label":format!("{} 工程单引用 {i} · 非策略研究",$v),"version":$v,"reference_file":path}));
            }
        }}
    }
    generate!(
        "v4",
        SourceNeutralTradeLifecycleResultV2,
        SourceNeutralControlResultV3,
        SourceNeutralSubmissionOutcomeV2
    );
    generate!(
        "v1",
        SourceNeutralTradeLifecycleResultV1,
        SourceNeutralControlResultV1,
        SourceNeutralSubmissionOutcomeV1
    );
    generate!(
        "v2",
        SourceNeutralTradeLifecycleResultV2,
        SourceNeutralControlResultV1,
        SourceNeutralSubmissionOutcomeV1
    );
    generate!(
        "v3",
        SourceNeutralTradeLifecycleResultV2,
        SourceNeutralControlResultV2,
        SourceNeutralSubmissionOutcomeV2
    );
    std::fs::write(root.join("sources.json"),serde_json::to_vec_pretty(&serde_json::json!({"runs":[{"label":"NOE 工程引用 · 非策略研究","noe_results":entries}]})).unwrap()).unwrap();
    eprintln!("工程引用已保存；不证明任何报告 Ledger 消费。未运行研究/恢复。 ");
}
