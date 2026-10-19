//! SDK/公共成本/原Ledger表示边界；仅使用显式本轮合成输出，不调用模型或发送订单。
use ::execution_normalization::{
    DurableContextStoreV1, NormalizationBatchInputV1, SimV2ContextSeedV1,
    SourceNeutralEconomicEvidenceV1, SourceNeutralEvidenceKindV1, SourceNeutralEvidenceV1,
    SourceNeutralLogicalKeyV1,
};
use contracts::execution_normalization::{
    DurableSourceReferenceV1, ImmutableSourceReferenceV1, SimV2SourceRecordReferenceV1,
    SimV2SourceWatermarkV1,
};
use contracts::*;
use ledger::*;
use model_train::{ConsumerRunConfig, domain::*};
use num_bigint::BigUint;
use rust_decimal::Decimal;
use serde_json::json;
use sim_system::platform_rules::*;
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 3 {
        return Err("用法: model_precision_probe <本轮合成消费者配置.json> <新结果.json>".into());
    }
    let config: ConsumerRunConfig = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
        snapshot_path: config.rules.snapshot_path,
        expected_file_sha256: config.rules.expected_file_sha256,
        replay_causal_start_utc_ms: i64::MAX,
    })?;
    let n = coefficient_max();
    let q = shares(&n)?;
    let price = Decimal::new(9999, 4);
    let sdk = polymarket_client_sdk_v2::clob::order_builder::encode_limit_buy_order(
        price,
        q,
        Decimal::new(1, 4),
    )?;
    let cost = costs(
        &rules,
        &n,
        &[LimitLeg {
            price_index: 9999,
            tick: ExactDecimal("0.0001".into()),
            post_only: true,
        }],
    )?
    .remove(0);
    let principal = scaled_string(&cost.principal_micro, 6);
    let temp = tempfile::tempdir()?;
    let opening_id = "precision-opening".to_owned();
    let run_id = "pure-rust-precision-boundary".to_owned();
    let mut noe = DurableContextStoreV1::open_or_create_sim_v2(
        temp.path(),
        SimV2ContextSeedV1 {
            run_id: run_id.clone(),
            account_instance_key: AccountInstanceKeyV1 {
                run_id: run_id.clone(),
                caller_role: SimCallerRoleV1::ModelTrain,
                caller_instance_id: "precision-probe".into(),
                account_id: "synthetic-precision-only".into(),
                platform_source: PlatformSourceV1::Simulated,
            },
            run_opened: DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Sha256(Sha256Hex("1".repeat(64))),
            },
            account_opening: DurableSourceReferenceV1 {
                source: ImmutableSourceReferenceV1::Sha256(Sha256Hex("2".repeat(64))),
            },
            account_opening_evidence_identity: opening_id.clone(),
            implementation_identity: "pure-rust-noe-opening-boundary".into(),
        },
    )?;
    noe.commit_batch(
        NormalizationBatchInputV1 {
            source_watermark: SimV2SourceWatermarkV1 {
                run_id,
                projection_batch_sequence: 1,
            },
            evidence: vec![SourceNeutralEvidenceV1 {
                source_record: SimV2SourceRecordReferenceV1::AccountOpening,
                source_ordinal: 0,
                logical_key: SourceNeutralLogicalKeyV1::EconomicEvidence {
                    evidence_identity: opening_id.clone(),
                },
                kind: SourceNeutralEvidenceKindV1::Economic {
                    evidence: SourceNeutralEconomicEvidenceV1::AccountOpening {
                        opening_evidence_identity: opening_id,
                        available_pusd: ExactDecimal(Decimal::MAX.to_string()),
                    },
                },
            }],
        },
        1001,
    )?;
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    ledger.apply_noe_result(
        &noe.commits()
            .last()
            .ok_or("NOE没有提交")?
            .result_reference(0)
            .ok_or("NOE结果引用缺失")?,
    )?;
    let before = ledger.projection_v2().ok_or("合成账本未开户")?;
    let outcome = ledger.try_reserve_action_v2(&[ReserveIntentV1 {
        request_identity: LedgerRequestIdentityV1("precision-probe-only".into()),
        condition_id: ConditionId(
            "0x1111111111111111111111111111111111111111111111111111111111111111".into(),
        ),
        requested_principal_pusd: ExactDecimal(principal.clone()),
        immediate_fee_reserve_pusd: ExactDecimal("0".into()),
    }]);
    let error = format!("{outcome:?}");
    assert!(matches!(
        outcome,
        Err(LedgerErrorV1::DecimalParse {
            field: "action.principal",
            ..
        })
    ));
    assert_eq!(ledger.projection_v2().unwrap(), before, "原失败不改变账本");
    let result = json!({"model_forwards":0,"centishares":n.to_string(),"shares":q.to_string(),"price":price.to_string(),"sdk_helper":"encode_limit_buy_order 成功，沿原SIM同一入口；未构建或签署LIVE订单","sdk_maker_amount":sdk.maker_amount.to_string(),"sdk_taker_amount":sdk.taker_amount.to_string(),"public_principal_micro":cost.principal_micro.to_string(),"public_principal_pusd":principal,"affordable_with_decimal_max":cost.principal_micro<cash_micro(Decimal::MAX),"ledger_result":error,"ledger_unchanged":true,"route":"ModelConsumer保留ExactDecimal原值，未采样后舍入；SharedExecution.execute_batch沿Ledger原错误返回，非模型改NoTrade","quantity_count":quantity_count().to_string(),"micro_upper_width":BigUint::from(u128::MAX).to_string()});
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&args[2])?;
    serde_json::to_writer_pretty(file, &result)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
