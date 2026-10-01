mod common;
use common::*;
use contracts::{economics::*, *};
use ledger::*;
use num_bigint::BigUint;
use rust_decimal::Decimal;
use shared_run::*;
use sim_system::{execution::*, platform_rules::*, public_consumption::*, *};
use std::{path::Path, sync::Arc};

fn reserve(id: &str, principal: &str) -> ReserveIntentV1 {
    ReserveIntentV1 {
        request_identity: LedgerRequestIdentityV1(id.into()),
        condition_id: ConditionId(CONDITION_ID.into()),
        requested_principal_pusd: ExactDecimal(principal.into()),
        immediate_fee_reserve_pusd: ExactDecimal("0".into()),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn market_estimate_does_not_cap_arrival_fee_and_original_cash_recovers_once() {
    // 正常成功、钱包不足、先应用后失败补偿；FOK/FAK使用同一原本金与最差价。
    for (fok, funded, fails) in [
        (true, true, false),
        (false, true, false),
        (true, false, false),
        (false, true, true),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("dpt");
        let (core, times) = recorded_market_fee_change(&root);
        let mut policy: serde_json::Value = serde_json::from_slice(SUCCESS_POLICY).unwrap();
        if fails {
            policy["failure_overrides"] = serde_json::json!([{
                "selector": {"selector_domain": "REQUEST", "account_instance_key": {
                    "run_id": "shared-combination", "caller_role": SimCallerRoleV1::Paper,
                    "caller_instance_id": "shared-caller", "account_id": "shared-account",
                    "platform_source": PlatformSourceV1::Simulated
                }, "global_request_id": "market"},
                "path": {"steps": [{"state": "MATCHED", "offset_ms": 0},
                    {"state": "MINED", "offset_ms": 10}, {"state": "FAILED", "offset_ms": 20000}],
                    "economic_application_ordinal": 1}
            }]);
        }
        let policy = serde_json::to_vec(&policy).unwrap();
        let (mut execution, observation) = execution_with_seed_and_cash(
            &temp.path().join("sim.jsonl"),
            &core,
            &policy,
            None,
            "e".repeat(64),
            "92",
        );
        execution
            .configure_resolution_redeem_v1(ResolutionRedeemScenarioV1 {
                steps: vec![ResolutionRedeemScenarioStepV1 {
                    offset_ms: 0,
                    event: ResolutionRedeemScenarioEventV1::Terminal(SettlementTerminalV1::Success),
                }],
                costs: vec![],
            })
            .unwrap();
        // 同一历史冻结快照；没有读取变价后的盘口来反算决定时估费。
        let rules = load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
            snapshot_path: Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json"),
            expected_file_sha256: Sha256Hex(
                "d63c0e7b09edc4b2c4c3f0295f806b25f6f5fd03663cdf60ff062117e53e4e54".into(),
            ),
            replay_causal_start_utc_ms: i64::MAX,
        })
        .unwrap();
        let estimate = rules
            .market_buy_fee_estimate_v1(
                &BigUint::from(10_000_u32),
                90,
                &ExactDecimal("0.01".into()),
            )
            .unwrap();
        let bounds = rules.sim_market_buy_bounds_v1(Decimal::from(90)).unwrap();
        assert_eq!(bounds.maximum_fee_1e5_units, BigUint::from(2_520_000_u32));
        // 100*.07*.9*.1=.63；到达后90/.1=900，900*.07*.1*.9=5.67。
        assert_eq!(estimate.fee_1e5_units, BigUint::from(63_000_u32));
        let context_reference = observation
            .opening_record()
            .unwrap()
            .context()
            .context_reference
            .clone();
        let mut ledger =
            LedgerWriterV1::new_with_admission_v2(AccountAdmissionPolicyV2::DualCashNoMarketLimit);
        let ledger_root = temp.path().join("ledger");
        execution
            .bind_ledger_recording_v2(&mut ledger, &ledger_root)
            .unwrap();
        ledger
            .apply_noe_memory_v1(&observation.opening_record().unwrap())
            .unwrap();
        assert!(matches!(
            ledger
                .try_reserve_action_v2(&[reserve("principal-only", "90")])
                .unwrap()[0],
            LedgerReservationOutcomeV1::Reserved { .. }
        ));
        ledger
            .record_submission_started(
                &LedgerLocalEventIdentityV1("send".into()),
                &LedgerRequestIdentityV1("principal-only".into()),
                &LedgerSubmissionItemIdentityV1 {
                    global_request_id: "market".into(),
                    batch_item_ordinal: 0,
                },
            )
            .unwrap();
        let mut shared = SharedExecutionV1::from_opening(ledger, observation)
            .unwrap()
            .unwrap();
        let source = RecordedPublicGroupSourceV1::open(&root, 1, 1 << 20, times).unwrap();
        let mut driver = FinitePublicConsumptionV1::new(execution, source, "market-fee".into());
        for _ in 0..4 {
            driver.next_group().unwrap().unwrap();
        }
        if funded {
            driver
                .decision()
                .unwrap()
                .apply_external_cash_fact_v1(ExternalCashFactV1 {
                    identity: "excluded-deposit".into(),
                    signed_delta_pusd: ExactDecimal("108".into()),
                    attribution: ExternalCashAttributionV1::Deposit,
                    correction_of: None,
                })
                .unwrap();
        }
        let before = shared.action_before(&mut driver).unwrap().unwrap();
        let order = if fok {
            BuyOrderIntentV1::FokMarket {
                spend_pusd: Decimal::from(90),
                worst_price_pusd_per_share: Decimal::new(9, 1),
            }
        } else {
            BuyOrderIntentV1::FakMarket {
                spend_pusd: Decimal::from(90),
                worst_price_pusd_per_share: Decimal::new(9, 1),
            }
        };
        let intent = Arc::new(ModelIntentV1::SubmitSingleBuy {
            condition_id: ConditionId(CONDITION_ID.into()),
            order: BuyLegIntentV1 {
                token_id: TokenId(UP_TOKEN_ID.into()),
                order,
            },
        });
        let context = RequestContextV2 {
            global_request_id: "market".into(),
            caller_correlation_id: None,
        };
        let receipt = shared
            .execute(
                &mut driver.decision().unwrap(),
                context.clone(),
                Some(intent.clone()),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(receipt.platform_arrived_at_utc_ms() > 22_002);
        // 原请求重放必须保持同一登记结果，不再下第二张订单。
        assert_eq!(
            shared
                .execute(
                    &mut driver.decision().unwrap(),
                    context,
                    Some(intent.clone())
                )
                .await
                .unwrap()
                .unwrap(),
            receipt
        );
        let frozen_before = before.economics().clone();
        assert!(
            frozen_before
                .markets
                .iter()
                .all(|m| m.executed[1].actual_fee_pusd.0 == "0")
        );
        let mut saw_negative_maker_cash = false;
        while driver.next_group().unwrap().is_some() {
            shared.apply_closed().unwrap();
            if let Some(read) = shared.ledger().economic_read() {
                if read.cash().maker_available_pusd.0 == "-3.67" {
                    saw_negative_maker_cash = true;
                }
            }
        }
        assert_eq!(saw_negative_maker_cash, funded);
        assert!(shared.apply_closed().unwrap());
        assert_eq!(
            before.economics(),
            &frozen_before,
            "旧模型读取不倒填实际成交"
        );
        let read = shared.ledger().economic_read().unwrap();
        for trade in read.trades() {
            let fee: Decimal = trade.actual_net_fee_pusd.0.parse().unwrap();
            assert!(
                fee * Decimal::from(100_000)
                    <= bounds
                        .maximum_fee_1e5_units
                        .to_string()
                        .parse::<Decimal>()
                        .unwrap()
            );
            if trade.success_applied {
                assert!(trade.economic_applied);
                assert!(matches!(
                    trade.unpaired_contribution_shares,
                    ExecutionReadValueV1::Known(_)
                ));
            }
        }
        let market = read
            .market_totals()
            .find(|m| m.condition_id.0 == CONDITION_ID);
        if funded {
            let market = market.unwrap();
            assert_eq!(market.executed[1].shares.0, "900");
            assert_eq!(market.executed[1].principal_pusd.0, "90");
            assert_eq!(
                market.executed[1].actual_fee_pusd.0,
                if fails { "0" } else { "5.67" }
            );
            assert_eq!(
                read.cash().actual_available_pusd.0,
                if fails { "200" } else { "104.33" }
            );
            assert_eq!(
                read.cash().maker_available_pusd.0,
                if fails { "92" } else { "-3.67" }
            );
        } else {
            assert!(market.is_none() || market.unwrap().executed[1].shares.0 == "0");
            assert_eq!(read.cash().actual_available_pusd.0, "92");
            assert_eq!(read.cash().maker_available_pusd.0, "92");
        }
        assert_eq!(read.cash().actual_reserved_pusd.0, "0");
        assert_eq!(read.cash().maker_reserved_pusd.0, "0");
        let expected_cash = read.cash().clone();
        let nav = AccountEconomicsReadV1::from_ledger(&read);
        assert_eq!(
            nav.other_account_residual_pusd,
            ExactRationalV1 {
                numerator: if funded { "108" } else { "0" }.into(),
                denominator: "1".into()
            }
        );
        let ratio = |n: &str, d: &str| ExactRationalV1 {
            numerator: n.into(),
            denominator: d.into(),
        };
        assert_eq!(
            nav.worst_case_maker_nav_pusd,
            if funded && !fails {
                ratio("-367", "100")
            } else {
                ratio("92", "1")
            }
        );
        assert_eq!(
            nav.directional_residual_pusd,
            if funded && !fails {
                ratio("900", "1")
            } else {
                ratio("0", "1")
            }
        );
        assert_eq!(
            nav.conservative_nav_pusd,
            if funded && !fails {
                ratio("100433", "100")
            } else if funded {
                ratio("200", "1")
            } else {
                ratio("92", "1")
            }
        );
        drop(read);
        if funded && !fails {
            assert_eq!(
                shared
                    .reserve_action_v2(&[reserve("next-principal", "1")])
                    .unwrap(),
                vec![LedgerReservationOutcomeV1::InsufficientMakerAvailableFunds]
            );
        }
        if funded && fails {
            assert_eq!(
                shared
                    .reserve_action_v2(&[reserve("excluded-cash-not-new-principal", "93")])
                    .unwrap(),
                vec![LedgerReservationOutcomeV1::InsufficientMakerAvailableFunds]
            );
        }
        // 真实后继订单仍保留原意图，未按估费缩量、改价或自动补腿。
        for order in shared.registry().orders() {
            assert_eq!(
                order.origin.as_ref().unwrap().intent.intent(),
                intent.as_ref()
            );
        }
        // 未请求赎回，保留真实应收，仅稳定记录材料，不声明业务终止。
        driver.stop_and_drain().unwrap();
        let noe = ::execution_normalization::DurableContextStoreV3::open(
            temp.path().join("noe"),
            &context_reference,
        )
        .unwrap();
        let rejected = noe.commits().iter().flat_map(|commit| &commit.records).filter(|record| matches!(record,
            ::execution_normalization::NormalizationRecord::Evidence {
                evidence: ::execution_normalization::SourceNeutralEvidenceKind::Control {
                    result: contracts::execution_normalization::SourceNeutralControlResult::Submission {
                        outcome: contracts::execution_normalization::SourceNeutralSubmissionOutcomeV2::RejectedNoEffect,
                    },
                }, ..
            })).count();
        assert_eq!(
            rejected,
            usize::from(!funded),
            "实际拒绝反馈，不能由无成交余额推断拒单"
        );
        for _ in 0..2 {
            let restored = LedgerWriterV1::open_durable(&ledger_root).unwrap();
            assert_eq!(restored.economic_read().unwrap().cash(), &expected_cash);
            assert_eq!(
                AccountEconomicsReadV1::from_ledger(&restored.economic_read().unwrap()),
                nav
            );
        }
    }
}
