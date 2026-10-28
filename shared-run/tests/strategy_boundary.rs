#[path = "common/fixture_clock.rs"]
mod fixture_clock;
#[path = "common/fixture_execution.rs"]
mod fixture_execution;
#[path = "common/fixture_execution_v4.rs"]
mod fixture_execution_v4;
#[path = "common/fixture_input.rs"]
mod fixture_input;
#[path = "common/fixture_market_identity.rs"]
mod fixture_market_identity;
#[path = "common/fixture_nav.rs"]
mod fixture_nav;
#[path = "common/fixture_pair.rs"]
mod fixture_pair;
#[path = "common/fixture_policy.rs"]
mod fixture_policy;
#[path = "common/fixture_recording.rs"]
mod fixture_recording;
#[path = "common/fixture_terminal.rs"]
mod fixture_terminal;
mod common {
    pub use super::fixture_execution_v4::*;
    pub use super::fixture_input::*;
    pub use super::fixture_nav::*;
    pub use super::fixture_pair::*;
    pub use super::fixture_policy::*;
}

use common::*;
use contracts::{economics::EconomicRational, *};
use ledger::*;
use rust_decimal::Decimal;
use shared_run::*;
use sim_system::{execution::*, public_consumption::*};
use std::{path::Path, sync::Arc};

type Driver = FinitePublicConsumptionV1<RecordedPublicGroupSourceV1<MemoryRecordedTimes>>;
fn setup(root: &Path) -> (SharedExecutionV2, Driver) {
    let (shared, driver, _) = setup_with_observation(root);
    (shared, driver)
}
fn setup_with_observation(root: &Path) -> (SharedExecutionV2, Driver, MemoryNoeObservationV4) {
    let dpt = root.join("dpt");
    let core = recorded_memory_complete(&dpt);
    let (mut execution, observation, _) = execution_with_rules_v4(
        &root.join("sim.jsonl"),
        &core,
        SUCCESS_POLICY,
        None,
        "e".repeat(64),
        "100",
    );
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &root.join("ledger"))
        .unwrap();
    let shared = SharedExecutionV2::from_opening(ledger, observation.clone())
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&dpt, 1, 1 << 20, MemoryRecordedTimes).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "strategy-test".into());
    for _ in 0..2 {
        driver.next_group().unwrap().unwrap();
    }
    (shared, driver, observation)
}
fn buy(id: &str, principal: u32) -> ReservedIntentV1 {
    ReservedIntentV1 {
        context: RequestContextV2 {
            global_request_id: id.into(),
            caller_correlation_id: None,
        },
        intent: Arc::new(ModelIntentV1::SubmitSingleBuy {
            condition_id: ConditionId(CONDITION_ID.into()),
            order: BuyLegIntentV1 {
                token_id: TokenId(UP_TOKEN_ID.into()),
                order: BuyOrderIntentV1::GtcLimit {
                    limit_price_pusd_per_share: Decimal::new(2, 1),
                    size_shares: Decimal::from(principal * 5),
                    post_only: true,
                },
            },
        }),
        reserves: vec![IntentReserveV1 {
            batch_item_ordinal: 0,
            reserve: ReserveIntentV1 {
                request_identity: LedgerRequestIdentityV1(format!("{id}-reserve")),
                condition_id: ConditionId(CONDITION_ID.into()),
                requested_principal_pusd: ExactDecimal(principal.to_string()),
                immediate_fee_reserve_pusd: ExactDecimal("0".into()),
            },
        }],
    }
}
#[tokio::test(flavor = "current_thread")]
async fn single_reentry_after_no_request_effect_sends_the_first_frozen_intent() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver) = setup(temp.path());
    let first = buy("first-frozen-before-registration", 1);
    let mut invalid_context = first.context.clone();
    invalid_context.caller_correlation_id = Some(" invalid ".into());
    let error = shared
        .execute(
            &mut driver.decision().unwrap(),
            invalid_context,
            Some(first.intent.clone()),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error,
        SimRunActorErrorV2::InvalidBusinessRequest("caller_correlation_id 为空或含首尾空白")
    );
    assert!(
        shared
            .registry()
            .frozen_intent(&first.context.global_request_id)
            .is_some()
    );
    assert!(
        !shared
            .registry()
            .all_commitments()
            .any(|r| r.origin.intent.global_request_id() == first.context.global_request_id)
    );
    let mut changed = buy(&first.context.global_request_id, 2);
    changed.intent = Arc::new(ModelIntentV1::SubmitSingleBuy {
        condition_id: ConditionId(CONDITION_ID.into()),
        order: BuyLegIntentV1 {
            token_id: TokenId(UP_TOKEN_ID.into()),
            order: BuyOrderIntentV1::GtcLimit {
                limit_price_pusd_per_share: Decimal::new(4, 1),
                size_shares: Decimal::from(5),
                post_only: true,
            },
        },
    });
    shared
        .execute_reserved(
            &mut driver.decision().unwrap(),
            first.context.clone(),
            Some(changed.intent),
            &[first.reserves[0].reserve.clone()],
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        shared
            .registry()
            .frozen_intent(&first.context.global_request_id)
            .unwrap()
            .intent(),
        first.intent.as_ref()
    );
    while driver
        .next_group_at()
        .unwrap()
        .is_some_and(|at| at <= 26_000)
    {
        driver.next_group().unwrap().unwrap();
        shared.apply_closed().unwrap();
    }
    let cash = shared.ledger().economic_read().unwrap();
    assert_eq!(cash.cash().actual_reserved_pusd.0, "1");
    assert_eq!(cash.cash().actual_available_pusd.0, "99");
    assert_eq!(
        shared
            .registry()
            .orders()
            .filter(|o| o.accepted.is_some())
            .count(),
        1
    );
    driver.stop_and_drain().unwrap();
}
/// 只变公开报价、不变Ledger前缀；借用和完整拥有式都必须重估原10份孤腿。
#[tokio::test(flavor = "current_thread")]
async fn borrowed_input_revalues_same_ledger_and_keeps_owned_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let dpt = temp.path().join("dpt");
    let (core, times) = recorded_nav_marks_with_book_quantity(&dpt, "10");
    let (mut execution, observation, _) = execution_with_rules_v4(
        &temp.path().join("sim.jsonl"),
        &core,
        SUCCESS_POLICY,
        None,
        "e".repeat(64),
        "100",
    );
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &temp.path().join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&dpt, 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "borrowed-nav".into());
    driver.enable_finite_recording_wait().unwrap();
    for _ in 0..8 {
        driver.next_group().unwrap().unwrap();
    }
    shared
        .execute(
            &mut driver.decision().unwrap(),
            RequestContextV2 {
                global_request_id: "nav-ten".into(),
                caller_correlation_id: None,
            },
            Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                condition_id: ConditionId(CONDITION_ID.into()),
                order: BuyLegIntentV1 {
                    token_id: TokenId(UP_TOKEN_ID.into()),
                    order: BuyOrderIntentV1::GtcLimit {
                        limit_price_pusd_per_share: Decimal::new(6, 1),
                        size_shares: Decimal::from(10),
                        post_only: false,
                    },
                },
            })),
        )
        .await
        .unwrap();
    let mut first = None;
    let mut seen = Vec::new();
    while driver.next_group().unwrap().is_some() {
        let owned = shared
            .strategy_observation_input(&mut driver)
            .unwrap()
            .unwrap();
        let at = owned
            .action_before
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        let expected = match at {
            36_000 | 37_000 => ("12229", "125"), // 100 - 6 - .168 + 10*.4
            38_000 | 42_000 => ("12354", "125"), // 100 - 6 - .168 + 10*.5
            40_000 => ("11729", "125"),          // fresh空bid是已知零标记
            _ => continue,
        };
        let borrowed = shared
            .strategy_borrowed_observation_input(&mut driver, true)
            .unwrap()
            .unwrap();
        assert_eq!(
            borrowed.action_before.economics(),
            owned.action_before.economics()
        );
        assert_eq!(borrowed.trade_economics, owned.trade_economics);
        assert_eq!(
            borrowed.action_before.cutoff(),
            owned.action_before.cutoff()
        );
        assert!(
            borrowed
                .markets
                .iter()
                .all(|frame| &frame.decision_cutoff == borrowed.action_before.cutoff())
        );
        assert_eq!(
            borrowed.action_before.economics().conservative_nav_pusd,
            ExecutionReadValueV1::Known(ExactRationalV1 {
                numerator: expected.0.into(),
                denominator: expected.1.into()
            })
        );
        assert_eq!(
            borrowed.action_before.profit_and_equity().1,
            ExecutionReadValueV1::Known(
                EconomicRational::try_from(&ExactRationalV1 {
                    numerator: expected.0.into(),
                    denominator: expected.1.into()
                })
                .unwrap()
            )
        );
        if let Some(prior) = &first {
            let prior: &StrategyInputV1 = prior;
            assert_eq!(
                borrowed.action_before.ledger(),
                prior.action_before.ledger(),
                "只有公开报价推进，账户事实不变"
            );
            assert_eq!(
                prior.action_before.economics().conservative_nav_pusd,
                ExecutionReadValueV1::Known(ExactRationalV1 {
                    numerator: "12229".into(),
                    denominator: "125".into()
                })
            );
        }
        drop(borrowed);
        if first.is_none() {
            first = Some(owned);
        }
        seen.push(at);
        if at == 42_000 {
            break; // 报价前缀验证到此结束；原材料未声明市场最终终态。
        }
    }
    assert_eq!(seen, [36_000, 37_000, 38_000, 40_000, 42_000]);
    driver.stop_and_drain().unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn batch_insufficiency_sends_nothing_and_never_releases_prior_unknown() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver) = setup(temp.path());
    let old = buy("old", 1);
    let old_receipt = shared
        .execute_batch_reserved(&mut driver.decision().unwrap(), &[old.clone()])
        .await
        .unwrap();
    let error = shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[old.clone(), buy("large", 100)],
        )
        .await
        .unwrap_err();
    assert!(
        error.is_local_funds_rejection(),
        "真实整批资金不足是正常停批"
    );
    assert!(matches!(error.error, SharedActionErrorV1::Reservation(_)));
    assert!(error.sent.is_empty());
    assert!(error.release_errors.is_empty());
    let requests: Vec<_> = shared
        .ledger()
        .funds_read_v1()
        .requests()
        .filter(|r| r.request_identity.0 == "old-reserve")
        .map(|r| r.reservation_state)
        .collect();
    assert_eq!(requests, [ReservationStatusV1::SubmittedUnknown]);
    let replay = shared
        .execute_batch_reserved(&mut driver.decision().unwrap(), &[old.clone()])
        .await
        .unwrap();
    assert_eq!(replay[0].receipt, old_receipt[0].receipt);
    assert!(
        shared.registry().frozen_intent("large").is_none(),
        "不足批次零Bridge请求"
    );
    let mut converted = false;
    while driver.next_group().unwrap().is_some() {
        shared.strategy_input(&mut driver).unwrap();
        if shared.ledger().funds_read_v1().requests().any(|r| {
            r.request_identity.0 == "old-reserve"
                && r.reservation_state == ReservationStatusV1::Converted
        }) {
            converted = true;
            break;
        }
    }
    assert!(converted, "原NOE将真实已发送请求转换为平台义务");
    let before = shared.ledger().projection_v2().unwrap().ledger_entry_seq;
    let replay = shared
        .execute_batch_reserved(&mut driver.decision().unwrap(), &[old])
        .await
        .unwrap();
    assert_eq!(replay[0].receipt, old_receipt[0].receipt);
    assert_eq!(
        shared.ledger().projection_v2().unwrap().ledger_entry_seq,
        before
    );
}
#[tokio::test(flavor = "current_thread")]
async fn pre_registration_error_releases_current_and_unsent_tail() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver) = setup(temp.path());
    let first = buy("first", 1);
    let mut bad = buy("second", 2);
    // 真实请求边界错误，不伪造内部Ledger或订单状态。
    bad.context.caller_correlation_id = Some(" invalid ".into());
    let last = buy("last", 3);
    let error = shared
        .execute_batch_reserved(&mut driver.decision().unwrap(), &[first, bad, last])
        .await
        .unwrap_err();
    assert!(
        !error.is_local_funds_rejection(),
        "已发前缀或真实错误不能吞为资金拒绝"
    );
    assert_eq!(error.sent.len(), 1);
    assert_eq!(error.sent[0].context.global_request_id, "first");
    assert_eq!(error.attempted_intent_index, Some(1));
    assert_eq!(
        error.failed_request_effect,
        Some(RequestEffectV1::NoRequestEffect)
    );
    assert!(matches!(
        error.error,
        SharedActionErrorV1::Sim(SimRunActorErrorV2::InvalidBusinessRequest(
            "caller_correlation_id 为空或含首尾空白"
        ))
    ));
    assert!(error.release_errors.is_empty());
    let requests: std::collections::BTreeMap<_, _> = shared
        .ledger()
        .funds_read_v1()
        .requests()
        .map(|r| (r.request_identity.0.clone(), r.reservation_state))
        .collect();
    assert_eq!(
        requests["first-reserve"],
        ReservationStatusV1::SubmittedUnknown
    );
    assert_eq!(requests["second-reserve"], ReservationStatusV1::Released);
    assert_eq!(requests["last-reserve"], ReservationStatusV1::Released);
    assert!(shared.registry().frozen_intent("last").is_none());
}
#[tokio::test(flavor = "current_thread")]
async fn bridge_construction_error_releases_current_and_unsent_tail() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver) = setup(temp.path());
    let first = buy("first", 1);
    let mut bad = buy("gtd-overflow", 2);
    bad.intent = Arc::new(ModelIntentV1::SubmitSingleBuy {
        condition_id: ConditionId(CONDITION_ID.into()),
        order: BuyLegIntentV1 {
            token_id: TokenId(UP_TOKEN_ID.into()),
            order: BuyOrderIntentV1::GtdLimit {
                limit_price_pusd_per_share: Decimal::new(2, 1),
                size_shares: Decimal::from(10),
                post_only: true,
                expires_at_utc_sec: i64::MAX,
            },
        },
    });
    let error = shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[first, bad, buy("last", 3)],
        )
        .await
        .unwrap_err();
    assert!(
        !error.is_local_funds_rejection(),
        "已发前缀或真实错误不能吞为资金拒绝"
    );
    assert_eq!(error.sent.len(), 1);
    assert_eq!(error.sent[0].context.global_request_id, "first");
    assert_eq!(error.attempted_intent_index, Some(1));
    assert_eq!(
        error.failed_request_effect,
        Some(RequestEffectV1::NoRequestEffect)
    );
    assert!(matches!(
        error.error,
        SharedActionErrorV1::Sim(SimRunActorErrorV2::RequestTimeOverflow)
    ));
    assert!(error.release_errors.is_empty());
    let requests: std::collections::BTreeMap<_, _> = shared
        .ledger()
        .funds_read_v1()
        .requests()
        .map(|r| (r.request_identity.0.clone(), r.reservation_state))
        .collect();
    assert_eq!(
        requests["first-reserve"],
        ReservationStatusV1::SubmittedUnknown
    );
    assert_eq!(
        requests["gtd-overflow-reserve"],
        ReservationStatusV1::Released
    );
    assert_eq!(requests["last-reserve"], ReservationStatusV1::Released);
    assert!(shared.registry().frozen_intent("last").is_none());
}
#[tokio::test(flavor = "current_thread")]
async fn released_unsent_cache_is_not_current_batch_funding() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver) = setup(temp.path());
    let mut a = buy("a", 60);
    a.context.caller_correlation_id = Some(" invalid ".into());
    let failed = shared
        .execute_batch_reserved(&mut driver.decision().unwrap(), &[a.clone()])
        .await
        .unwrap_err();
    assert_eq!(
        failed.failed_request_effect,
        Some(RequestEffectV1::NoRequestEffect)
    );
    assert!(failed.sent.is_empty());
    assert_eq!(
        shared
            .ledger()
            .funds_read_v1()
            .requests()
            .find(|r| r.request_identity.0 == "a-reserve")
            .unwrap()
            .reservation_state,
        ReservationStatusV1::Released
    );
    assert_eq!(
        shared
            .ledger()
            .economic_read()
            .unwrap()
            .cash()
            .actual_available_pusd,
        ExactDecimal("100".into())
    );
    a.context.caller_correlation_id = None;
    // 无覆盖项位于后面，证明必须在本批任何发送前发现，而不是先发新B。
    let error = shared
        .execute_batch_reserved(&mut driver.decision().unwrap(), &[buy("b", 60), a])
        .await
        .expect_err("历史Reserved不是现存预留，120本金不能在100现金上发送");
    assert!(error.sent.is_empty());
    assert_eq!(error.attempted_intent_index, None);
    assert!(error.release_errors.is_empty());
    assert!(shared.registry().frozen_intent("b").is_none());
    assert_eq!(
        shared
            .ledger()
            .funds_read_v1()
            .requests()
            .find(|r| r.request_identity.0 == "a-reserve")
            .unwrap()
            .reservation_state,
        ReservationStatusV1::Released
    );
    assert!(
        !shared
            .ledger()
            .funds_read_v1()
            .requests()
            .any(|r| r.request_identity.0 == "b-reserve")
    );
}
#[tokio::test(flavor = "current_thread")]
async fn duplicate_global_id_cannot_substitute_old_intent_or_release_old_obligations() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver) = setup(temp.path());
    let first = buy("same", 1);
    let mut collision = buy("same", 2);
    // 第二项不同原意图、但复用第一项预留身份：后项尚未发送不能释放第一项。
    collision.reserves[0].reserve.request_identity =
        first.reserves[0].reserve.request_identity.clone();
    let error = shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[first.clone(), collision.clone(), buy("tail", 3)],
        )
        .await
        .unwrap_err();
    assert!(
        !error.is_local_funds_rejection(),
        "已发前缀或真实错误不能吞为资金拒绝"
    );
    assert_eq!(error.sent.len(), 1);
    assert!(matches!(
        error.error,
        SharedActionErrorV1::Sim(SimRunActorErrorV2::InvalidBusinessRequest(_))
    ));
    assert_eq!(
        shared.registry().frozen_intent("same").unwrap().intent(),
        first.intent.as_ref()
    );
    assert_eq!(
        shared
            .ledger()
            .funds_read_v1()
            .requests()
            .find(|r| r.request_identity.0 == "same-reserve")
            .unwrap()
            .reservation_state,
        ReservationStatusV1::SubmittedUnknown
    );
    // 跨批不同意图同ID，同样不能报告原回执为新意图已发。
    let error = shared
        .execute_batch_reserved(&mut driver.decision().unwrap(), &[collision])
        .await
        .unwrap_err();
    assert!(error.sent.is_empty());
    assert!(error.release_errors.is_empty());
    // 新global ID同样不能借用已经发送的旧Ledger请求占用。
    let mut alias = buy("different", 2);
    alias.reserves[0].reserve.request_identity = first.reserves[0].reserve.request_identity.clone();
    let error = shared
        .execute_batch_reserved(&mut driver.decision().unwrap(), &[alias])
        .await
        .unwrap_err();
    assert!(error.sent.is_empty());
    assert!(error.release_errors.is_empty());
    assert!(shared.registry().frozen_intent("different").is_none());
}
struct Controlled {
    output: Option<Result<StrategyOutputV1, &'static str>>,
}
impl StrategyV1 for Controlled {
    type Error = &'static str;
    type Diagnostics = ();
    fn decide(&mut self, _: &StrategyInputV1, _: i64) -> Result<StrategyOutputV1, Self::Error> {
        self.output.take().unwrap()
    }
}
#[tokio::test(flavor = "current_thread")]
async fn frozen_strategy_input_explicit_time_empty_error_exit_and_original_amount() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver) = setup(temp.path());
    let input = shared.strategy_input(&mut driver).unwrap().unwrap();
    let old_market = input.markets[0].observation.clone();
    let old_economics = input.action_before.economics().clone();
    driver.next_group().unwrap().unwrap();
    shared.strategy_input(&mut driver).unwrap().unwrap();
    while driver
        .decision()
        .unwrap()
        .cutoff()
        .decision_time()
        .logical_time_utc_ms()
        < 26_000
    {
        driver.next_group().unwrap().unwrap();
        shared.strategy_input(&mut driver).unwrap();
    }
    assert_eq!(input.markets[0].observation, old_market);
    assert_eq!(input.action_before.economics(), &old_economics);
    let mut controlled = Controlled {
        output: Some(Err("calculation-error")),
    };
    assert!(matches!(
        input.calculate(&mut controlled, 22_000),
        Err("calculation-error")
    ));
    controlled.output = Some(Ok(StrategyOutputV1::Continue {
        intents: vec![],
        next_evaluation_at_utc_ms: Some(25_000),
        diagnostics: (),
    }));
    let calculation = input
        .calculate(&mut controlled, 22_000)
        .unwrap()
        .complete(23_000);
    let result = shared
        .dispatch_strategy(
            &mut driver.decision().unwrap(),
            &mut controlled,
            calculation,
        )
        .await
        .unwrap();
    assert!(matches!(result,StrategyDispatchV1::Continue {receipts,..} if receipts.is_empty()));
    assert_eq!(shared.registry().all_commitments().count(), 0);
    // 健康资格分派的空动作同样零请求；当前观察截止不改写原冻结决定或发送时序。
    let ledger_seq = shared.ledger().applied_entry_sequence();
    let original_cutoff = input.action_before.cutoff().clone();
    let current_cutoff = driver.observation_cutoff().unwrap();
    assert!(
        current_cutoff.decision_time().logical_time_utc_ms()
            > original_cutoff.decision_time().logical_time_utc_ms()
    );
    controlled.output = Some(Ok(StrategyOutputV1::Continue {
        intents: vec![],
        next_evaluation_at_utc_ms: Some(25_000),
        diagnostics: (),
    }));
    let calculation = input
        .calculate(&mut controlled, 22_000)
        .unwrap()
        .complete(23_000);
    let mut decision = driver.decision().unwrap();
    let sent_at = decision.sent_at_utc_ms();
    let qualified = shared
        .dispatch_strategy_qualified(&mut decision, &mut controlled, calculation)
        .await
        .unwrap();
    let QualifiedStrategyDispatchV1::Dispatched {
        dispatch:
            StrategyDispatchV1::Continue {
                calculation,
                sent_at_utc_ms,
                receipts,
            },
        qualification_calls,
    } = qualified
    else {
        panic!("空动作应正常完成")
    };
    assert_eq!(qualification_calls, 0);
    assert!(receipts.is_empty());
    assert_eq!(sent_at_utc_ms, sent_at);
    assert_eq!(calculation.calculation.input_cutoff, original_cutoff);
    assert_eq!(shared.ledger().applied_entry_sequence(), ledger_seq);
    assert_eq!(shared.registry().all_commitments().count(), 0);
    controlled.output = Some(Ok(StrategyOutputV1::Exit { diagnostics: () }));
    let calculation = input
        .calculate(&mut controlled, 22_000)
        .unwrap()
        .complete(23_000);
    assert!(matches!(
        shared
            .dispatch_strategy(
                &mut driver.decision().unwrap(),
                &mut controlled,
                calculation
            )
            .await
            .unwrap(),
        StrategyDispatchV1::Exit { .. }
    ));
    assert_eq!(input.action_before.cutoff().private_feedback_sequence(), 0);
    let mut item = buy("amount", 1);
    item.intent = Arc::new(ModelIntentV1::SubmitSingleBuy {
        condition_id: ConditionId(CONDITION_ID.into()),
        order: BuyLegIntentV1 {
            token_id: TokenId(UP_TOKEN_ID.into()),
            order: BuyOrderIntentV1::FakMarket {
                spend_pusd: Decimal::new(1_000_001, 6),
                worst_price_pusd_per_share: Decimal::new(6, 1),
            },
        },
    });
    item.reserves[0].reserve.requested_principal_pusd = ExactDecimal("1.000001".into());
    shared
        .execute_batch_reserved(&mut driver.decision().unwrap(), &[item.clone()])
        .await
        .unwrap();
    assert_eq!(
        shared.registry().frozen_intent("amount").unwrap().intent(),
        item.intent.as_ref(),
        "非q=n/100金额保持原值"
    );
    let mut saw_new_trade = false;
    while driver.next_group().unwrap().is_some() {
        let fresh = shared.strategy_input(&mut driver).unwrap().unwrap();
        if fresh
            .trade_economics
            .iter()
            .any(|t| t.economic_applied && t.success_applied)
        {
            let current = shared.strategy_current_read(&driver).unwrap().unwrap();
            assert_eq!(current.action_before.cutoff(), fresh.action_before.cutoff());
            assert_eq!(
                current.trade_economics, fresh.trade_economics,
                "资格与决定共用同截止原交易费用/贡献映射"
            );
            assert!(
                current
                    .trade_economics
                    .iter()
                    .any(|t| t.actual_net_fee_pusd.0 != "0")
            );
            drop(current);
            let mixed = shared
                .strategy_input_for_market_frame(
                    &mut driver,
                    FrozenStrategyMarketFrameV1 {
                        timing: MarketFrameTimingV1 {
                            market_cutoff: original_cutoff.clone(),
                            earliest_start_at_utc_ms: 22_000,
                        },
                        markets: input.markets.clone(),
                    },
                    true,
                )
                .unwrap()
                .unwrap();
            assert_eq!(mixed.action_before.cutoff(), fresh.action_before.cutoff());
            assert_eq!(
                mixed.action_before.economics(),
                fresh.action_before.economics(),
                "估值来自当前合法行情，而非传入旧帧"
            );
            assert_eq!(
                mixed.trade_economics, fresh.trade_economics,
                "旧市场帧不冻结后续合法反馈"
            );
            assert_eq!(
                mixed.market_frame_timing.as_ref().unwrap().market_cutoff,
                original_cutoff
            );
            assert_eq!(
                mixed.markets[0].observation, old_market,
                "拥有式混合读取不得重取当前市场帧"
            );
            assert_eq!(mixed.markets[0].statistics, input.markets[0].statistics);
            assert!(Arc::ptr_eq(
                &mixed.markets[0].rule_context,
                &input.markets[0].rule_context
            ));
            saw_new_trade = true;
            break;
        }
    }
    assert!(saw_new_trade, "真实后续成交进入新策略冻结明细");
    assert!(input.trade_economics.is_empty(), "旧明细不倒填后续事实");
    assert_eq!(input.action_before.economics(), &old_economics);
}

struct QualificationProbe {
    intents: Vec<ReservedIntentV1>,
    invalidate_at: Option<usize>,
    checked: Vec<usize>,
    invalidated: Option<(usize, usize, Vec<SourceStreamScopeV1>)>,
}
impl StrategyV1 for QualificationProbe {
    type Error = &'static str;
    type Diagnostics = &'static str;
    fn uses_source_health(&self) -> bool {
        true
    }
    fn decide(
        &mut self,
        _: &StrategyInputV1,
        _: i64,
    ) -> Result<StrategyOutputV1<Self::Diagnostics>, Self::Error> {
        Ok(StrategyOutputV1::Continue {
            intents: self.intents.clone(),
            next_evaluation_at_utc_ms: None,
            diagnostics: "original-frozen-diagnostics",
        })
    }
    fn qualify_intent(
        &mut self,
        _: &StrategyCurrentReadV1<'_>,
        calculation: &StrategyCalculationV1<Self::Diagnostics>,
        index: usize,
        _: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        let StrategyOutputV1::Continue { diagnostics, .. } = &calculation.output else {
            panic!()
        };
        assert_eq!(*diagnostics, "original-frozen-diagnostics");
        self.checked.push(index);
        Ok(if self.invalidate_at == Some(index) {
            IntentQualificationV1::Invalidated(IntentInvalidationReasonV1 {
                reason_code: "synthetic-own-dependency-expired".into(),
                scopes: vec![SourceStreamScopeV1 {
                    source: SourceStreamKindV1::ChainlinkSpot,
                    condition_id: None,
                    asset_or_symbol: "BTC/USD".into(),
                }],
            })
        } else {
            IntentQualificationV1::Qualified
        })
    }
    fn invalidated(
        &mut self,
        _: &StrategyCalculationV1<Self::Diagnostics>,
        result: &InvalidatedBatchV1,
    ) {
        self.invalidated = Some((
            result.sent.len(),
            result.invalidated_intent_index,
            result.reason.scopes.clone(),
        ));
    }
}
#[tokio::test(flavor = "current_thread")]
async fn qualification_stops_suffix_preserves_sent_unknown_and_releases_only_unsent() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver) = setup(temp.path());
    let cancel = ReservedIntentV1 {
        context: RequestContextV2 {
            global_request_id: "old-batch-cancel".into(),
            caller_correlation_id: None,
        },
        intent: Arc::new(ModelIntentV1::CancelMarketOrders {
            condition_id: ConditionId(CONDITION_ID.into()),
        }),
        reserves: vec![],
    };
    let mut strategy = QualificationProbe {
        intents: vec![buy("prefix", 1), buy("invalid", 2), cancel],
        invalidate_at: Some(1),
        checked: vec![],
        invalidated: None,
    };
    let input = shared.strategy_input(&mut driver).unwrap().unwrap();
    let at = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let calculation = input.calculate(&mut strategy, at).unwrap().complete(at);
    let result = shared
        .dispatch_strategy_qualified(&mut driver.decision().unwrap(), &mut strategy, calculation)
        .await
        .unwrap();
    let QualifiedStrategyDispatchV1::Invalidated {
        calculation,
        result,
        ..
    } = result
    else {
        panic!("失效必须独立表达")
    };
    assert_eq!(strategy.checked, [0, 1]);
    assert_eq!(result.sent.len(), 1);
    assert_eq!(result.invalidated_intent_index, 1);
    assert!(result.release_errors.is_empty());
    assert_eq!(result.observed_cutoff, input.action_before.cutoff().clone());
    assert!(shared.registry().frozen_intent("invalid").is_none());
    assert!(
        shared
            .registry()
            .frozen_intent("old-batch-cancel")
            .is_none()
    );
    let requests: std::collections::BTreeMap<_, _> = shared
        .ledger()
        .funds_read_v1()
        .requests()
        .map(|r| (r.request_identity.0.clone(), r.reservation_state))
        .collect();
    assert_eq!(
        requests["prefix-reserve"],
        ReservationStatusV1::SubmittedUnknown
    );
    assert_eq!(requests["invalid-reserve"], ReservationStatusV1::Released);
    let StrategyOutputV1::Continue {
        intents,
        diagnostics,
        ..
    } = calculation.calculation.output
    else {
        panic!()
    };
    assert_eq!(intents.len(), 3);
    assert_eq!(diagnostics, "original-frozen-diagnostics");
    // 已尝试未知的原请求复读不再被健康资格当作尚未发送，也不释放原承诺。
    let mut retry = QualificationProbe {
        intents: vec![buy("prefix", 1)],
        invalidate_at: Some(0),
        checked: vec![],
        invalidated: None,
    };
    let calculation = input.calculate(&mut retry, at).unwrap().complete(at);
    let result = shared
        .dispatch_strategy_qualified(&mut driver.decision().unwrap(), &mut retry, calculation)
        .await
        .unwrap();
    assert!(matches!(
        result,
        QualifiedStrategyDispatchV1::Dispatched { .. }
    ));
    assert!(retry.checked.is_empty());
    assert_eq!(
        shared
            .ledger()
            .funds_read_v1()
            .requests()
            .find(|r| r.request_identity.0 == "prefix-reserve")
            .unwrap()
            .reservation_state,
        ReservationStatusV1::SubmittedUnknown
    );
}

#[test]
fn local_funds_classification_keeps_mixed_fault_unknown_and_release_failure_fatal() {
    let mut error = BatchExecutionErrorV1 {
        error: SharedActionErrorV1::Reservation(vec![
            LedgerReservationOutcomeV1::Reserved {
                ledger_entry_seq: 1,
            },
            LedgerReservationOutcomeV1::InsufficientGlobalAvailableFunds,
        ]),
        sent: vec![],
        attempted_intent_index: None,
        failed_request_effect: None,
        release_errors: vec![],
    };
    assert!(error.is_local_funds_rejection());
    error
        .release_errors
        .push(LedgerErrorV1::ReservationRequestNotFound {
            request_identity: LedgerRequestIdentityV1("real-release-failure".into()),
        });
    assert!(!error.is_local_funds_rejection());
    error.release_errors.clear();
    error.failed_request_effect = Some(RequestEffectV1::MayHaveRequestEffect);
    assert!(!error.is_local_funds_rejection());
    error.failed_request_effect = None;
    error.attempted_intent_index = Some(0);
    assert!(!error.is_local_funds_rejection());
    error.attempted_intent_index = None;
    if let SharedActionErrorV1::Reservation(outcomes) = &mut error.error {
        outcomes.push(LedgerReservationOutcomeV1::PerMarketLimitExceeded);
    }
    assert!(!error.is_local_funds_rejection());
    error.error = SharedActionErrorV1::Ledger(LedgerErrorV1::AccountNotOpened);
    assert!(!error.is_local_funds_rejection());
}


/// 真实NOE领取和资金变化后重新选取责任；闭市不裁掉旧持仓/预留。
#[tokio::test(flavor = "current_thread")]
async fn selected_observations_refresh_feedback_funds_and_ended_market_responsibility() {
    let temporary;
    let root = if let Some(parent) = std::env::var_os("BACKTEST_COST_TEST_OUTPUT_ROOT") {
        let root = std::path::PathBuf::from(parent).join("selected-feedback");
        std::fs::create_dir_all(&root).unwrap();
        root
    } else {
        temporary = tempfile::tempdir().unwrap();
        temporary.path().to_owned()
    };
    let (core, times) = recorded_nav_marks_with_book_quantity(&root.join("dpt"), "10");
    let (mut execution, observation, _) = execution_with_rules_v4(
        &root.join("sim.jsonl"),
        &core,
        SUCCESS_POLICY,
        None,
        "e".repeat(64),
        "100",
    );
    let mut ledger =
        LedgerWriterV1::new_with_admission_v3(AccountAdmissionPolicyV3::ActualCashNoMarketLimit);
    execution
        .bind_ledger_recording_v2(&mut ledger, &root.join("ledger"))
        .unwrap();
    let mut shared = SharedExecutionV2::from_opening(ledger, observation)
        .unwrap()
        .unwrap();
    let source = RecordedPublicGroupSourceV1::open(&root.join("dpt"), 1, 1 << 20, times).unwrap();
    let mut driver = FinitePublicConsumptionV1::new(execution, source, "selected-feedback".into());
    driver.enable_finite_recording_wait().unwrap();
    for _ in 0..8 {
        driver.next_group().unwrap().unwrap();
    }
    let old = ConditionId(CONDITION_ID.into());
    let other = ConditionId(fixture_market_identity::OTHER_CONDITION_ID.into());
    let first = shared
        .strategy_selected_observation_input(
            &mut driver,
            false,
            &[other.clone(), old.clone(), old.clone()],
        )
        .unwrap()
        .unwrap();
    let initial_sequence = first.action_before.cutoff().private_feedback_sequence();
    assert_eq!(first.markets.len(), 2, "重复历史ID与当前市场仅合并一次");
    assert!(
        first.markets.windows(2).all(
            |p| p[0].observation.market.condition_id.0 < p[1].observation.market.condition_id.0
        )
    );
    drop(first);
    shared
        .reserve_action_v2(&[ReserveIntentV1 {
            request_identity: LedgerRequestIdentityV1("selected-local-pending".into()),
            condition_id: old.clone(),
            requested_principal_pusd: ExactDecimal("3".into()),
            immediate_fee_reserve_pusd: ExactDecimal("0".into()),
        }])
        .unwrap();
    let receipt = shared
        .execute(
            &mut driver.decision().unwrap(),
            RequestContextV2 {
                global_request_id: "selected-ten".into(),
                caller_correlation_id: None,
            },
            Some(Arc::new(ModelIntentV1::SubmitSingleBuy {
                condition_id: old.clone(),
                order: BuyLegIntentV1 {
                    token_id: TokenId(UP_TOKEN_ID.into()),
                    order: BuyOrderIntentV1::GtcLimit {
                        limit_price_pusd_per_share: Decimal::new(6, 1),
                        size_shares: Decimal::from(10),
                        post_only: false,
                    },
                },
            })),
        )
        .await
        .unwrap()
        .unwrap();
    std::fs::write(
        root.join("request-receipt.json"),
        serde_json::to_vec_pretty(&receipt).unwrap(),
    )
    .unwrap();
    let mut rows = Vec::new();
    let mut saw_ended = false;
    while driver.next_group().unwrap().is_some() {
        let selected = shared
            .strategy_selected_observation_input(&mut driver, false, &[])
            .unwrap()
            .unwrap();
        let cutoff = selected.action_before.cutoff().clone();
        let economics = selected.action_before.economics().clone();
        let selected_ids: Vec<_> = selected
            .markets
            .iter()
            .map(|f| f.observation.market.condition_id.clone())
            .collect();
        drop(selected);
        let full = shared
            .strategy_observation_input_with_trade_details(&mut driver, false)
            .unwrap()
            .unwrap();
        assert_eq!(cutoff, *full.action_before.cutoff());
        assert_eq!(economics, *full.action_before.economics());
        let at = cutoff.decision_time().logical_time_utc_ms();
        rows.push(serde_json::json!({"at":at,"private_feedback_sequence":cutoff.private_feedback_sequence(),"selected":selected_ids,"cash":full.action_before.cash_available()}));
        if at == 66_000 {
            assert!(
                cutoff.private_feedback_sequence() > initial_sequence,
                "必须实际领取新反馈"
            );
            assert!(
                shared.registry().orders().any(|o| o.accepted.is_some()),
                "本地登记不代平台接纳"
            );
            let read = shared.ledger().necessary_read_v2().unwrap();
            assert!(
                read.marked_markets().any(|id| id == old.0),
                "真实持仓参与选择"
            );
            assert!(
                read.funds_markets().any(|id| id == old.0),
                "当前真实预留参与选择"
            );
            assert_eq!(read.cash().1, Decimal::from(3));
            let public_only: Vec<_> = driver
                .selected_frames_at_observation(std::iter::empty())
                .iter()
                .map(|f| f.observation.market.condition_id.clone())
                .collect();
            assert_eq!(
                public_only,
                [other.clone()],
                "旧市场已经结束，不再是默认当前市场"
            );
            assert_eq!(selected_ids.len(), 2);
            assert!(selected_ids.contains(&old));
            // 同前缀重读不改变反馈位置、资金或责任；不缓存过去选择集合。
            let again = shared
                .strategy_selected_observation_input(&mut driver, false, &[])
                .unwrap()
                .unwrap();
            assert_eq!(again.action_before.cutoff(), &cutoff);
            assert_eq!(
                again
                    .markets
                    .iter()
                    .map(|f| f.observation.market.condition_id.clone())
                    .collect::<Vec<_>>(),
                selected_ids
            );
            saw_ended = true;
            break;
        }
    }
    std::fs::write(
        root.join("selection.json"),
        serde_json::to_vec_pretty(&rows).unwrap(),
    )
    .unwrap();
    assert!(saw_ended);
    driver.stop_and_drain().unwrap();
}

struct HostBoundaryProbe {
    qualified: bool,
    observed_old: Option<(ReservationStatusV1, u64)>,
}
impl StrategyV1 for HostBoundaryProbe {
    type Error = &'static str;
    type Diagnostics = ();
    fn uses_source_health(&self) -> bool {
        self.qualified
    }
    fn decide(&mut self, _: &StrategyInputV1, _: i64) -> Result<StrategyOutputV1, Self::Error> {
        Ok(StrategyOutputV1::Continue {
            intents: vec![buy("new-host-managed", 2)],
            next_evaluation_at_utc_ms: None,
            diagnostics: (),
        })
    }
    fn qualify_intent(
        &mut self,
        current: &StrategyCurrentReadV1<'_>,
        _: &StrategyCalculationV1,
        index: usize,
        _: i64,
    ) -> Result<IntentQualificationV1, Self::Error> {
        assert_eq!(index, 0);
        self.observed_old = current
            .action_before
            .request_funds()
            .find(|r| r.request_identity.0 == "old-host-managed-reserve")
            .map(|r| (r.reservation_state, r.last_ledger_entry_seq));
        Ok(IntentQualificationV1::Qualified)
    }
}
#[tokio::test(flavor = "current_thread")]
async fn host_managed_dispatch_keeps_closed_old_feedback_until_original_account_read() {
    for qualified in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (mut shared, mut driver, observation) = setup_with_observation(temp.path());
        shared
            .execute_batch_reserved(
                &mut driver.decision().unwrap(),
                &[buy("old-host-managed", 1)],
            )
            .await
            .unwrap();
        let input = shared.strategy_input(&mut driver).unwrap().unwrap();
        let at = input
            .action_before
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        let mut strategy = HostBoundaryProbe {
            qualified,
            observed_old: None,
        };
        let calculation = input.calculate(&mut strategy, at).unwrap().complete(at);
        while driver
            .next_group_at()
            .unwrap()
            .is_some_and(|at| at <= 26_000)
        {
            driver.next_group().unwrap().unwrap();
        }
        let ready = observation
            .next_pending_feedback_received_at_utc_ms()
            .expect("原请求反馈已实际进入消费源");
        let now = driver
            .observation_cutoff()
            .unwrap()
            .decision_time()
            .logical_time_utc_ms();
        assert!(ready <= now, "旧反馈已到当前合法领取时刻");
        let delivered_before = observation.delivered_through();
        let before = shared
            .ledger()
            .funds_read_v1()
            .request(&LedgerRequestIdentityV1("old-host-managed-reserve".into()))
            .map(|r| (r.reservation_state, r.last_ledger_entry_seq))
            .unwrap();
        assert_eq!(before.0, ReservationStatusV1::SubmittedUnknown);
        if qualified {
            shared
                .dispatch_strategy_qualified(
                    &mut driver.decision().unwrap(),
                    &mut strategy,
                    calculation,
                )
                .await
                .unwrap();
            assert_eq!(strategy.observed_old, Some(before));
        } else {
            shared
                .dispatch_strategy(&mut driver.decision().unwrap(), &mut strategy, calculation)
                .await
                .unwrap();
        }
        assert_eq!(
            observation.delivered_through(),
            delivered_before,
            "发送前不提前领取已经可达的旧NOE"
        );
        let old = shared
            .ledger()
            .funds_read_v1()
            .request(&LedgerRequestIdentityV1("old-host-managed-reserve".into()))
            .map(|r| (r.reservation_state, r.last_ledger_entry_seq))
            .unwrap();
        assert_eq!(old, before);
        let new = shared
            .ledger()
            .funds_read_v1()
            .request(&LedgerRequestIdentityV1("new-host-managed-reserve".into()))
            .map(|r| {
                (
                    r.reservation_state,
                    r.last_ledger_entry_seq,
                    r.principal_pusd.0,
                )
            })
            .unwrap();
        assert_eq!(new.0, ReservationStatusV1::SubmittedUnknown);
        assert_eq!(new.2, "2");
        assert!(new.1 > before.1);
        shared.apply_closed().unwrap();
        assert!(observation.delivered_through() > delivered_before);
        let old = shared
            .ledger()
            .funds_read_v1()
            .request(&LedgerRequestIdentityV1("old-host-managed-reserve".into()))
            .map(|r| (r.reservation_state, r.last_ledger_entry_seq))
            .unwrap();
        assert_eq!(old.0, ReservationStatusV1::Converted);
        assert!(
            old.1 > new.1,
            "旧反馈在原宿主读取时才应用，Ledger事务次序保持"
        );
        driver.stop_and_drain().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn finite_actual_and_host_managed_keep_distinct_feedback_and_ledger_order() {
    for (qualified, actual) in [(false, false), (true, false), (false, true), (true, true)] {
        let temp = tempfile::tempdir().unwrap();
        let (mut shared, mut driver, observation) = setup_with_observation(temp.path());
        shared
            .execute_batch_reserved(
                &mut driver.decision().unwrap(),
                &[buy("old-host-managed", 1)],
            )
            .await
            .unwrap();
        let input = shared.strategy_input(&mut driver).unwrap().unwrap();
        let at = input
            .action_before
            .cutoff()
            .decision_time()
            .logical_time_utc_ms();
        let mut strategy = HostBoundaryProbe {
            qualified,
            observed_old: None,
        };
        let frozen_cutoff = input.action_before.cutoff().clone();
        let calculation = input.calculate(&mut strategy, at).unwrap().complete(at);
        while driver
            .next_group_at()
            .unwrap()
            .is_some_and(|at| at <= 26_000)
        {
            driver.next_group().unwrap().unwrap();
        }
        let ready = observation
            .next_pending_feedback_received_at_utc_ms()
            .expect("原请求反馈已实际进入消费源");
        let now = driver
            .observation_cutoff()
            .unwrap()
            .decision_time()
            .logical_time_utc_ms();
        assert!(ready <= now, "旧反馈已到当前合法领取时刻");
        let delivered_before = observation.delivered_through();
        let before = shared
            .ledger()
            .funds_read_v1()
            .request(&LedgerRequestIdentityV1("old-host-managed-reserve".into()))
            .map(|r| (r.reservation_state, r.last_ledger_entry_seq))
            .unwrap();
        assert_eq!(before.0, ReservationStatusV1::SubmittedUnknown);
        let mut decision = if actual {
            driver.send_decision_at(frozen_cutoff, now).unwrap()
        } else {
            driver
                .send_decision_at_host_managed(frozen_cutoff, now)
                .unwrap()
        };
        assert_eq!(decision.uses_actual_send_boundary(), actual);
        if qualified {
            shared
                .dispatch_strategy_qualified(&mut decision, &mut strategy, calculation)
                .await
                .unwrap();
            if actual {
                let observed = strategy.observed_old.expect("实际资格已读取旧请求");
                assert_eq!(observed.0, ReservationStatusV1::Converted);
                assert!(observed.1 > before.1);
            } else {
                assert_eq!(strategy.observed_old, Some(before));
            }
        } else {
            shared
                .dispatch_strategy(&mut decision, &mut strategy, calculation)
                .await
                .unwrap();
        }
        drop(decision);
        if actual {
            assert!(observation.delivered_through() > delivered_before);
        } else {
            assert_eq!(observation.delivered_through(), delivered_before);
        }
        let old = shared
            .ledger()
            .funds_read_v1()
            .request(&LedgerRequestIdentityV1("old-host-managed-reserve".into()))
            .map(|r| (r.reservation_state, r.last_ledger_entry_seq))
            .unwrap();
        if actual {
            assert_eq!(old.0, ReservationStatusV1::Converted);
            assert!(old.1 > before.1);
        } else {
            assert_eq!(old, before);
        }
        let old_before_host_read = old;
        let new = shared
            .ledger()
            .funds_read_v1()
            .request(&LedgerRequestIdentityV1("new-host-managed-reserve".into()))
            .map(|r| {
                (
                    r.reservation_state,
                    r.last_ledger_entry_seq,
                    r.principal_pusd.0,
                )
            })
            .unwrap();
        assert_eq!(new.0, ReservationStatusV1::SubmittedUnknown);
        assert_eq!(new.2, "2");
        assert!(new.1 > before.1);
        shared.apply_closed().unwrap();
        assert!(observation.delivered_through() > delivered_before);
        let old = shared
            .ledger()
            .funds_read_v1()
            .request(&LedgerRequestIdentityV1("old-host-managed-reserve".into()))
            .map(|r| (r.reservation_state, r.last_ledger_entry_seq))
            .unwrap();
        assert_eq!(old.0, ReservationStatusV1::Converted);
        if actual {
            assert_eq!(old, old_before_host_read);
            assert!(old.1 < new.1, "默认finite先领取旧事实，再登记新提交");
        } else {
            assert!(
                old.1 > new.1,
                "显式HostManaged先登记新提交，再由宿主领取旧事实"
            );
        }
        driver.stop_and_drain().unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn finite_actual_bridge_error_precedes_feedback_and_invalidating_qualification() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver, observation) = setup_with_observation(temp.path());
    shared
        .execute_batch_reserved(
            &mut driver.decision().unwrap(),
            &[buy("old-host-managed", 1)],
        )
        .await
        .unwrap();
    let mut bad = buy("finite-gtd-overflow", 2);
    bad.intent = Arc::new(ModelIntentV1::SubmitSingleBuy {
        condition_id: ConditionId(CONDITION_ID.into()),
        order: BuyLegIntentV1 {
            token_id: TokenId(UP_TOKEN_ID.into()),
            order: BuyOrderIntentV1::GtdLimit {
                limit_price_pusd_per_share: Decimal::new(2, 1),
                size_shares: Decimal::from(10),
                post_only: true,
                expires_at_utc_sec: i64::MAX,
            },
        },
    });
    let mut strategy = QualificationProbe {
        intents: vec![bad],
        invalidate_at: Some(0),
        checked: vec![],
        invalidated: None,
    };
    let input = shared.strategy_input(&mut driver).unwrap().unwrap();
    let cutoff = input.action_before.cutoff().clone();
    let at = cutoff.decision_time().logical_time_utc_ms();
    let calculation = input.calculate(&mut strategy, at).unwrap().complete(at);
    while driver
        .next_group_at()
        .unwrap()
        .is_some_and(|at| at <= 26_000)
    {
        driver.next_group().unwrap().unwrap();
    }
    let now = driver
        .observation_cutoff()
        .unwrap()
        .decision_time()
        .logical_time_utc_ms();
    assert!(
        observation
            .next_pending_feedback_received_at_utc_ms()
            .unwrap()
            <= now
    );
    let delivered = observation.delivered_through();
    let result = shared
        .dispatch_strategy_qualified(
            &mut driver.send_decision_at(cutoff, now).unwrap(),
            &mut strategy,
            calculation,
        )
        .await;
    let failure = match result {
        Err(failure) => failure,
        Ok(_) => panic!("原Bridge错误必须返回"),
    };
    let QualifiedBatchErrorV1::Execution(error) = failure.error else {
        panic!("Bridge error must remain the original execution error")
    };
    assert!(matches!(
        error.error,
        SharedActionErrorV1::Sim(SimRunActorErrorV2::RequestTimeOverflow)
    ));
    assert_eq!(error.attempted_intent_index, Some(0));
    assert_eq!(
        error.failed_request_effect,
        Some(RequestEffectV1::NoRequestEffect)
    );
    assert!(error.sent.is_empty() && error.release_errors.is_empty());
    assert!(strategy.checked.is_empty(), "资格不能掩盖原Bridge错误");
    assert_eq!(
        observation.delivered_through(),
        delivered,
        "Bridge失败时尚未提前领取旧反馈"
    );
    assert_eq!(
        shared
            .ledger()
            .funds_read_v1()
            .request(&LedgerRequestIdentityV1(
                "finite-gtd-overflow-reserve".into()
            ))
            .unwrap()
            .reservation_state,
        ReservationStatusV1::Released
    );
    driver.stop_and_drain().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn finite_actual_qualification_preserves_sent_prefix_and_releases_only_unsent() {
    let temp = tempfile::tempdir().unwrap();
    let (mut shared, mut driver) = setup(temp.path());
    let cancel = ReservedIntentV1 {
        context: RequestContextV2 {
            global_request_id: "old-batch-cancel".into(),
            caller_correlation_id: None,
        },
        intent: Arc::new(ModelIntentV1::CancelMarketOrders {
            condition_id: ConditionId(CONDITION_ID.into()),
        }),
        reserves: vec![],
    };
    let mut strategy = QualificationProbe {
        intents: vec![buy("prefix", 1), buy("invalid", 2), cancel],
        invalidate_at: Some(1),
        checked: vec![],
        invalidated: None,
    };
    let input = shared.strategy_input(&mut driver).unwrap().unwrap();
    let at = input
        .action_before
        .cutoff()
        .decision_time()
        .logical_time_utc_ms();
    let calculation = input.calculate(&mut strategy, at).unwrap().complete(at);
    let result = shared
        .dispatch_strategy_qualified(
            &mut driver
                .send_decision_at(input.action_before.cutoff().clone(), at)
                .unwrap(),
            &mut strategy,
            calculation,
        )
        .await
        .unwrap();
    let QualifiedStrategyDispatchV1::Invalidated {
        calculation,
        result,
        ..
    } = result
    else {
        panic!("失效必须独立表达")
    };
    assert_eq!(strategy.checked, [0, 1]);
    assert_eq!(result.sent.len(), 1);
    assert_eq!(result.invalidated_intent_index, 1);
    assert!(result.release_errors.is_empty());
    assert_eq!(
        result.observed_cutoff,
        driver.observation_cutoff().unwrap().clone()
    );
    assert!(shared.registry().frozen_intent("invalid").is_none());
    assert!(
        shared
            .registry()
            .frozen_intent("old-batch-cancel")
            .is_none()
    );
    let requests: std::collections::BTreeMap<_, _> = shared
        .ledger()
        .funds_read_v1()
        .requests()
        .map(|r| (r.request_identity.0.clone(), r.reservation_state))
        .collect();
    assert_eq!(
        requests["prefix-reserve"],
        ReservationStatusV1::SubmittedUnknown
    );
    assert_eq!(requests["invalid-reserve"], ReservationStatusV1::Released);
    let StrategyOutputV1::Continue {
        intents,
        diagnostics,
        ..
    } = calculation.calculation.output
    else {
        panic!()
    };
    assert_eq!(intents.len(), 3);
    assert_eq!(diagnostics, "original-frozen-diagnostics");
    // 已尝试未知的原请求复读不再被健康资格当作尚未发送，也不释放原承诺。
    let mut retry = QualificationProbe {
        intents: vec![buy("prefix", 1)],
        invalidate_at: Some(0),
        checked: vec![],
        invalidated: None,
    };
    let calculation = input.calculate(&mut retry, at).unwrap().complete(at);
    let result = shared
        .dispatch_strategy_qualified(&mut driver.decision().unwrap(), &mut retry, calculation)
        .await
        .unwrap();
    assert!(matches!(
        result,
        QualifiedStrategyDispatchV1::Dispatched { .. }
    ));
    assert!(retry.checked.is_empty());
    assert_eq!(
        shared
            .ledger()
            .funds_read_v1()
            .requests()
            .find(|r| r.request_identity.0 == "prefix-reserve")
            .unwrap()
            .reservation_state,
        ReservationStatusV1::SubmittedUnknown
    );
}
