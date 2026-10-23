//! 真实 SIM 产生的 NOE 前驱缺口，不构造矛盾的成交/经济状态。
use super::*;
use ::execution_normalization::NormalizationRecord;
use contracts::*;
use rust_decimal::Decimal;
use sim_system::public_consumption::*;
use std::sync::Arc;
#[path = "../common/fixture_clock.rs"]
mod fixture_clock;
#[path = "../common/fixture_execution.rs"]
mod fixture_execution;
#[path = "../common/fixture_execution_v3.rs"]
mod fixture_execution_v3;
#[path = "../common/fixture_input.rs"]
mod fixture_input;
#[path = "../common/fixture_policy.rs"]
mod fixture_policy;
#[path = "../common/fixture_recording.rs"]
mod fixture_recording;
#[path = "../common/fixture_reference.rs"]
mod fixture_reference;
#[path = "../common/fixture_terminal.rs"]
mod fixture_terminal;
#[path = "../common/fixture_trading.rs"]
mod fixture_trading;

#[tokio::test(flavor = "current_thread")]
async fn real_closed_trade_with_missing_economic_predecessor_stays_in_frozen_obligations() {
    use contracts::execution_normalization::SourceNeutralTradeEconomicDispositionV1 as D;
    use fixture_input::{CONDITION_ID, UP_TOKEN_ID};
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("dpt");
    let (core, times) = fixture_trading::recorded_trading(&root, "4", false);
    let (execution, observation) = fixture_execution_v3::execution(
        &temp.path().join("sim.jsonl"),
        &core,
        fixture_policy::SUCCESS_POLICY,
    );
    let ledger = LedgerWriterV1::new(AccountAdmissionPolicyV1 {
        per_market_all_in_commitment_limit_pusd: ExactDecimal("1000".into()),
    });
    let mut shared = SharedExecutionV1::from_opening(ledger, observation.clone())
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "obligation-gap".into());
    for _ in 0..4 {
        driver.next_group().unwrap().unwrap();
    }
    let intent = Arc::new(ModelIntentV1::SubmitSingleBuy {
        condition_id: ConditionId(CONDITION_ID.into()),
        order: BuyLegIntentV1 {
            token_id: TokenId(UP_TOKEN_ID.into()),
            order: BuyOrderIntentV1::FakMarket {
                spend_pusd: Decimal::from(6),
                worst_price_pusd_per_share: Decimal::new(6, 1),
            },
        },
    });
    let origin = shared.registry.freeze("gap-limit".into(), intent.clone());
    let receipt = intent_bridge::IntentBridge::bind_sim_group(&mut driver.decision().unwrap())
        .execute(
            RequestContextV2 {
                global_request_id: "gap-limit".into(),
                caller_correlation_id: None,
            },
            &intent,
        )
        .await
        .unwrap();
    shared.registry.record_sim_receipt(&origin, &receipt);
    let mut records = Vec::new();
    let mut cutoff = None;
    while driver.next_group().unwrap().is_some() {
        cutoff = Some(
            driver.decision().unwrap().frames()[0]
                .decision_cutoff
                .clone(),
        );
        records.extend(observation.take_closed_records());
    }
    records.extend(observation.take_closed_records());
    let mut withheld = None;
    // 缺少第一条真实经济转换；后继原记录不改号，Registry 仍消费实到反馈。
    for record in records {
        shared.registry.observe(&record);
        if withheld.is_none()
            && matches!(
                record.record(),
                NormalizationRecord::EconomicFact { .. }
                    | NormalizationRecord::EconomicFactV2 { .. }
            )
        {
            withheld = Some(record);
        } else {
            shared.pending.push(record);
        }
    }
    assert!(withheld.is_some());
    assert!(!shared.apply_closed().unwrap());
    assert!(shared.pending.iter().any(|r| matches!(
        <sim_system::execution::MeasuredOrderEvidence as SharedOrderEvidence>::apply(
            &mut shared.ledger,
            r
        )
        .unwrap(),
        LedgerMemoryApplyOutcomeV1::Applied(
            LedgerApplyOutcomeV1::MissingPlatformPredecessor { .. }
        )
    )));
    let frames = driver.frames_at_observation();
    let before = shared
        .freeze_action_before(cutoff.clone().unwrap(), &frames)
        .unwrap();
    assert_eq!(
        before.orders().count(),
        0,
        "Registry 已将该终局单移出活动人口"
    );
    assert_eq!(before.obligation_orders().count(), 1);
    let trade = &before.obligation_trades()[0];
    assert_eq!(trade.fact().economic_disposition, D::FinalSuccess);
    assert_eq!(trade.fact().base_quantity_shares, ExactDecimal("4".into()));
    assert!(!trade.success_applied());
    // 实际链中生命周期可先应用；缺经济前驱不意味着生命周期 owner 必然缺记录。
    assert!(trade.ledger.is_some());
    let borrowed = shared
        .borrow_action_before(cutoff.clone().unwrap(), &frames)
        .unwrap();
    let converted = borrowed.to_owned_read();
    assert_eq!(converted.obligation_orders().count(), 1);
    assert_eq!(converted.obligation_trades()[0].fact(), trade.fact());
    assert!(!converted.obligation_trades()[0].success_applied());
    drop(borrowed);
    assert!(matches!(
        shared
            .ledger
            .apply_noe_memory_v1(&withheld.unwrap())
            .unwrap(),
        LedgerMemoryApplyOutcomeV1::Applied(LedgerApplyOutcomeV1::NewlyApplied { .. })
    ));
    assert!(shared.apply_closed().unwrap());
    let after = shared
        .freeze_action_before(cutoff.unwrap(), &frames)
        .unwrap();
    assert!(after.obligation_trades().is_empty());
    let market = after
        .market_economics(&ConditionId(CONDITION_ID.into()))
        .unwrap();
    assert_eq!(
        market.orphan_shares.iter().sum::<Decimal>(),
        Decimal::from(4)
    );
    assert!(
        !before.obligation_trades()[0].success_applied(),
        "旧冻结不能被后继经济应用改写"
    );
    assert!(!converted.obligation_trades()[0].success_applied());
    driver.stop_and_drain().unwrap();
}
