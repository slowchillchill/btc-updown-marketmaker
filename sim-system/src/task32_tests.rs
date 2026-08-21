use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use contracts::{
    AccountInstanceKeyV1, PlatformAccountDeltaPredecessorV1, PlatformAccountDeltaReasonV1,
    PlatformSourceV1, SimCallerRoleV1,
};
use rust_decimal::Decimal;

use crate::execution::{
    InitialAccountStateV1, PostOrderDirectResponseV1, PostOrderItemV1, PostOrderRequestV1,
    RequestRegistrationDispositionV1, SimBuyOrderKindV1, SimBuyOrderV1, SimExecutionProcessV1,
    SimExecutionSessionV1, SimulatedTradeKnownStatusV1, SubmissionDeliveryDispositionV1,
    SubmissionDriveDispositionV1, SubmissionItemOutcomeV1, SubmissionProcessingErrorV1,
    SubmissionResponseAcceptanceV1, SubmissionResponseBridgeV1, TradeEconomicDispositionV1,
    TradeLifecycleFeedbackV1, TradesDirectResponseV1, TradesFilterV1, TradesQueryResultV1,
    TradesRequestV1,
};
use crate::finality::{
    TradeFinalityPathV1, TradeFinalityPolicyV1, TradeFinalitySelectorDomainV1,
    TradeFinalitySelectorV1, TradeFinalityStateV1, policy_for_tests,
};
use crate::latency_profile::load_embedded_proxy_latency_profile;
use crate::market::{MarketBucketApplyDispositionV1, MarketEventErrorV1};
use crate::matching::{MatchingStateErrorV1, TradeEconomicDispositionStateV1};
use crate::platform_rules::tests::successful_snapshot_for_sim_tests;
use crate::session::{RequestContextV1, SessionError};
use crate::task21_tests::{hidden_bucket, initial_snapshot_event, scenario, session_spec};
use crate::task22_tests::{
    asset as long_asset, context_for_arrival, load_initial_market as load_long_initial_market,
    market as long_market, scenario as long_scenario, session_spec as long_session_spec,
};
use crate::task23_tests::fak_request;

const RUN_ID: &str = "component-test-run";

#[derive(Clone, Default)]
struct Task32Captures {
    post_order: Arc<Mutex<Vec<Arc<PostOrderDirectResponseV1>>>>,
    trades: Arc<Mutex<Vec<Arc<TradesDirectResponseV1>>>>,
    trade_lifecycle: Arc<Mutex<Vec<Arc<TradeLifecycleFeedbackV1>>>>,
}

impl Task32Captures {
    fn bridge(&self) -> SubmissionResponseBridgeV1 {
        let post_order = Arc::clone(&self.post_order);
        let trades = Arc::clone(&self.trades);
        let trade_lifecycle = Arc::clone(&self.trade_lifecycle);
        SubmissionResponseBridgeV1::new(
            move |response| {
                post_order
                    .lock()
                    .expect("任务 3.2 单笔响应捕获锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                trades
                    .lock()
                    .expect("任务 3.2 成交查询响应捕获锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |feedback| {
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
            move |feedback| {
                trade_lifecycle
                    .lock()
                    .expect("任务 3.2 成交生命周期反馈捕获锁")
                    .push(Arc::clone(&feedback));
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
            |feedback| {
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
        )
    }
}

fn trade_full_once_bridge(
    attempts: Arc<Mutex<Vec<Arc<TradeLifecycleFeedbackV1>>>>,
) -> SubmissionResponseBridgeV1 {
    SubmissionResponseBridgeV1::new(
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        |feedback| {
            SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
        },
        move |feedback| {
            let mut attempts = attempts.lock().expect("成交反馈 Full 重试捕获锁");
            attempts.push(Arc::clone(&feedback));
            if attempts.len() == 1 {
                SubmissionResponseAcceptanceV1::Full
            } else {
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            }
        },
        |feedback| {
            SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
        },
    )
}

fn decimal(value: &str) -> Decimal {
    value.parse().expect("任务 3.2 测试十进制")
}

fn account_key(session_id: &str) -> AccountInstanceKeyV1 {
    AccountInstanceKeyV1 {
        run_id: RUN_ID.to_owned(),
        caller_role: SimCallerRoleV1::ModelTrain,
        caller_instance_id: format!("{session_id}-caller"),
        account_id: format!("{session_id}-account"),
        platform_source: PlatformSourceV1::Simulated,
    }
}

fn success_policy(
    policy_id: &str,
    mined_offset_ms: i64,
    confirmed_offset_ms: i64,
) -> Arc<TradeFinalityPolicyV1> {
    policy_for_tests(
        policy_id,
        &[
            (TradeFinalityStateV1::Matched, 0),
            (TradeFinalityStateV1::Mined, mined_offset_ms),
            (TradeFinalityStateV1::Confirmed, confirmed_offset_ms),
        ],
        1,
        Vec::new(),
    )
}

fn request_failure_policy(
    policy_id: &str,
    session_id: &str,
    request_id: &str,
    failed_offset_ms: i64,
    economic_application_ordinal: Option<u32>,
) -> Arc<TradeFinalityPolicyV1> {
    policy_for_tests(
        policy_id,
        &[
            (TradeFinalityStateV1::Matched, 0),
            (TradeFinalityStateV1::Mined, 100_000),
            (TradeFinalityStateV1::Confirmed, 200_000),
        ],
        1,
        vec![(
            TradeFinalitySelectorV1::Request {
                account_instance_key: account_key(session_id),
                global_request_id: request_id.to_owned(),
            },
            vec![
                (TradeFinalityStateV1::Matched, 0),
                (TradeFinalityStateV1::Failed, failed_offset_ms),
            ],
            economic_application_ordinal,
        )],
    )
}

async fn session_with_policy(
    session_id: &str,
    available_pusd: Decimal,
    capacity: usize,
    policy: Arc<TradeFinalityPolicyV1>,
    captures: &Task32Captures,
    load_market: bool,
) -> SimExecutionSessionV1 {
    session_with_policy_and_bridge(
        session_id,
        available_pusd,
        capacity,
        policy,
        captures.bridge(),
        load_market,
    )
    .await
}

async fn session_with_policy_and_bridge(
    session_id: &str,
    available_pusd: Decimal,
    capacity: usize,
    policy: Arc<TradeFinalityPolicyV1>,
    bridge: SubmissionResponseBridgeV1,
    load_market: bool,
) -> SimExecutionSessionV1 {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
    let scenario = scenario();
    let mut spec = session_spec(&scenario, SimCallerRoleV1::ModelTrain, session_id);
    spec.initial_account_state =
        InitialAccountStateV1::try_new(available_pusd).expect("任务 3.2 显式初始资金");
    spec.response_outbox_capacity = NonZeroUsize::new(capacity).expect("正反馈槽容量");
    let mut process = SimExecutionProcessV1::try_new(
        RUN_ID.to_owned(),
        policy,
        rules,
        profile,
        Arc::clone(&scenario),
    )
    .expect("任务 3.2 运行根");
    let mut session = process.create_active_session_for_tests(spec, bridge);
    if load_market {
        let initial = [
            initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10"),
            initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "12"),
        ];
        session
            .apply_closed_market_event_bucket(hidden_bucket(1_100, 2, &initial))
            .expect("任务 3.2 初始市场桶");
    }
    session
}

async fn long_session_with_policy(
    session_id: &str,
    available_pusd: Decimal,
    capacity: usize,
    policy: Arc<TradeFinalityPolicyV1>,
    captures: &Task32Captures,
) -> SimExecutionSessionV1 {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
    let scenario = long_scenario();
    let mut spec = long_session_spec(&scenario, SimCallerRoleV1::ModelTrain, session_id);
    spec.initial_account_state =
        InitialAccountStateV1::try_new(available_pusd).expect("任务 3.2 长窗口显式初始资金");
    spec.response_outbox_capacity = NonZeroUsize::new(capacity).expect("正反馈槽容量");
    let mut process = SimExecutionProcessV1::try_new(
        RUN_ID.to_owned(),
        policy,
        rules,
        profile,
        Arc::clone(&scenario),
    )
    .expect("任务 3.2 长窗口运行根");
    let mut session = process.create_active_session_for_tests(spec, captures.bridge());
    load_long_initial_market(&mut session, "0.00001", "0.6", "20");
    session
}

fn close_platform_at(
    session: &mut SimExecutionSessionV1,
    at_utc_ms: i64,
) -> Result<MarketBucketApplyDispositionV1, MarketEventErrorV1> {
    let position = session.platform_event_index_position();
    session.apply_closed_market_event_bucket(hidden_bucket(at_utc_ms, position, &[]))
}

struct ExecutedFill {
    request: PostOrderRequestV1,
    effective_at_utc_ms: i64,
    platform_order_id: String,
    trade_id: String,
}

fn execute_fak_and_deliver_initial(
    session: &mut SimExecutionSessionV1,
    captures: &Task32Captures,
    request_id: &str,
) -> ExecutedFill {
    let request = fak_request(session, request_id, 1, 1_200, "3", "0.6");
    let registration = session
        .post_order(request.clone())
        .expect("任务 3.2 FAK 登记");
    let effective_at_utc_ms = registration.item.effective_processing_at_utc_ms;
    assert_eq!(
        close_platform_at(session, effective_at_utc_ms),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(
        session.drive_next_execution_action_through(effective_at_utc_ms),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    let response_received_at_utc_ms = effective_at_utc_ms
        + i64::try_from(registration.receipt.transit.inbound_delay_ms).expect("响应时延适合 i64");
    assert_eq!(
        session
            .drive_private_delivery_after_platform_through_for_tests(response_received_at_utc_ms),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let responses = captures.post_order.lock().expect("单笔响应捕获锁");
    let response = responses.last().expect("单笔提交直接响应");
    let SubmissionItemOutcomeV1::Accepted(accepted) = &response.item.outcome else {
        panic!("任务 3.2 FAK 必须接受");
    };
    assert_eq!(accepted.fills.len(), 1);
    ExecutedFill {
        request,
        effective_at_utc_ms,
        platform_order_id: accepted.simulated_platform_order_id.clone(),
        trade_id: accepted.fills[0].simulated_trade_id.clone(),
    }
}

fn trade_feedbacks(
    captures: &Task32Captures,
    trade_id: &str,
) -> Vec<Arc<TradeLifecycleFeedbackV1>> {
    let mut feedbacks = captures
        .trade_lifecycle
        .lock()
        .expect("成交生命周期反馈捕获锁")
        .iter()
        .filter(|feedback| feedback.simulated_trade_id == trade_id)
        .cloned()
        .collect::<Vec<_>>();
    feedbacks.sort_by_key(|feedback| feedback.metadata.platform_event_at_utc_ms);
    feedbacks
}

fn assert_path_terminal(path: &TradeFinalityPathV1, expected: TradeFinalityStateV1) {
    assert_eq!(path.steps().last().expect("密封路径非空").state(), expected);
}

fn fak_request_for_effective_at(
    session: &SimExecutionSessionV1,
    profile: &crate::latency_profile::EmpiricalDelayProfile,
    request_id: &str,
    caller_sequence: u64,
    effective_at_utc_ms: i64,
    spend_pusd: &str,
) -> PostOrderRequestV1 {
    PostOrderRequestV1 {
        context: context_for_arrival(
            session,
            profile,
            request_id,
            caller_sequence,
            effective_at_utc_ms - 250,
        ),
        idempotency_key: format!("idem-{request_id}"),
        item: PostOrderItemV1 {
            item_id: format!("item-{request_id}"),
            order: SimBuyOrderV1 {
                condition_id: long_market().condition_id,
                token_id: long_asset("1", 0, "Up").token_id,
                kind: SimBuyOrderKindV1::FakMarket {
                    spend_pusd: decimal(spend_pusd),
                    worst_price_pusd_per_share: decimal("0.6"),
                },
            },
        },
    }
}

fn accepted_fill_identity(response: &PostOrderDirectResponseV1) -> (String, String) {
    let SubmissionItemOutcomeV1::Accepted(accepted) = &response.item.outcome else {
        panic!("任务 3.2 同刻夹具订单必须接受");
    };
    assert_eq!(accepted.fills.len(), 1);
    (
        accepted.simulated_platform_order_id.clone(),
        accepted.fills[0].simulated_trade_id.clone(),
    )
}

#[test]
fn task32_request_batch_and_fill_selectors_are_fully_account_scoped() {
    let key = account_key("selector-a");
    let other_key = account_key("selector-b");
    let cases = [
        (
            TradeFinalitySelectorV1::Request {
                account_instance_key: key.clone(),
                global_request_id: "request-a".to_owned(),
            },
            TradeFinalitySelectorDomainV1::Request,
        ),
        (
            TradeFinalitySelectorV1::BatchItem {
                account_instance_key: key.clone(),
                item_idempotency_identity: "batch-item-a".to_owned(),
            },
            TradeFinalitySelectorDomainV1::BatchItem,
        ),
        (
            TradeFinalitySelectorV1::Fill {
                account_instance_key: key.clone(),
                fill_economic_delta_identity: "fill-a".to_owned(),
            },
            TradeFinalitySelectorDomainV1::Fill,
        ),
    ];

    for (ordinal, (selector, expected_domain)) in cases.into_iter().enumerate() {
        let policy = policy_for_tests(
            &format!("task32-selector-{ordinal}"),
            &[
                (TradeFinalityStateV1::Matched, 0),
                (TradeFinalityStateV1::Mined, 10),
                (TradeFinalityStateV1::Confirmed, 20),
            ],
            1,
            vec![(
                selector,
                vec![
                    (TradeFinalityStateV1::Matched, 0),
                    (TradeFinalityStateV1::Failed, 10),
                ],
                None,
            )],
        );
        assert_eq!(policy.override_selector_domain(), Some(expected_domain));
        assert_path_terminal(
            policy.select_path_for_fill(&key, "request-a", "batch-item-a", "fill-a"),
            TradeFinalityStateV1::Failed,
        );
        assert_path_terminal(
            policy.select_path_for_fill(&other_key, "request-a", "batch-item-a", "fill-a"),
            TradeFinalityStateV1::Confirmed,
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task32_normal_success_exposes_actual_states_and_atomic_query_revisions() {
    let captures = Task32Captures::default();
    let mut session = session_with_policy(
        "task32-success",
        Decimal::TEN,
        4,
        success_policy("task32-success-policy", 100_000, 200_000),
        &captures,
        true,
    )
    .await;
    let revision_before = session.query_projection_revision();
    let fill = execute_fak_and_deliver_initial(&mut session, &captures, "task32-success-request");
    assert_eq!(session.query_projection_revision(), revision_before + 1);
    let internal = session
        .simulated_fill(&fill.platform_order_id)
        .expect("普通成功成交");
    assert_eq!(
        internal.finality_state(),
        Some(TradeFinalityStateV1::Matched)
    );
    assert_eq!(
        internal.economic_disposition(),
        TradeEconomicDispositionStateV1::PendingUnapplied
    );

    assert_eq!(
        close_platform_at(&mut session, fill.effective_at_utc_ms + 100_000),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(session.query_projection_revision(), revision_before + 2);
    let internal = session
        .simulated_fill(&fill.platform_order_id)
        .expect("已挖矿成交");
    assert_eq!(internal.finality_state(), Some(TradeFinalityStateV1::Mined));
    assert_eq!(
        internal.economic_disposition(),
        TradeEconomicDispositionStateV1::PendingApplied
    );

    assert_eq!(
        close_platform_at(&mut session, fill.effective_at_utc_ms + 200_000),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(session.query_projection_revision(), revision_before + 3);
    let internal = session
        .simulated_fill(&fill.platform_order_id)
        .expect("最终成功成交");
    assert_eq!(
        internal.finality_state(),
        Some(TradeFinalityStateV1::Confirmed)
    );
    assert_eq!(
        internal.economic_disposition(),
        TradeEconomicDispositionStateV1::FinalSuccess
    );

    let query_context = RequestContextV1 {
        execution_contract_version: session.session_spec().execution_contract_version.clone(),
        session_id: session.session_spec().session_id.clone(),
        account_id: session.session_spec().account_id.clone(),
        caller_role: session.session_spec().caller_role,
        caller_instance_id: session.session_spec().caller_instance_id.clone(),
        global_request_id: "task32-success-trades".to_owned(),
        caller_correlation_id: None,
        caller_sequence: 2,
        request_sent_at_utc_ms: fill.effective_at_utc_ms + 200_001,
        model_visible_cursor: session.model_visible_cursor(),
    };
    let query = session
        .trades(TradesRequestV1 {
            context: query_context,
            filter: TradesFilterV1::default(),
            cursor: None,
        })
        .expect("成交查询登记");
    let query_at = query.receipt.transit.platform_arrived_at_utc_ms;
    assert_eq!(
        close_platform_at(&mut session, query_at),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(
        session.drive_next_execution_action_through(query_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    let query_received_at =
        query_at + i64::try_from(query.receipt.transit.inbound_delay_ms).expect("查询时延适合 i64");
    assert_eq!(
        session.drive_private_delivery_after_platform_through_for_tests(query_received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let responses = captures.trades.lock().expect("成交查询响应捕获锁");
    let TradesQueryResultV1::Page(page) = &responses.last().expect("成交查询响应").result
    else {
        panic!("成交查询必须返回页面");
    };
    assert_eq!(page.query_projection_revision, revision_before + 3);
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].simulated_trade_id, fill.trade_id);
    assert_eq!(
        page.items[0].status.known,
        Some(SimulatedTradeKnownStatusV1::Confirmed)
    );
    assert_eq!(
        page.items[0].economic_disposition,
        TradeEconomicDispositionV1::FinalSuccess
    );

    let feedbacks = trade_feedbacks(&captures, &fill.trade_id);
    assert_eq!(feedbacks.len(), 3);
    assert_eq!(
        feedbacks[0].economic_disposition,
        TradeEconomicDispositionV1::PendingUnapplied
    );
    assert_eq!(feedbacks[0].account_delta, None);
    let applied = feedbacks[1]
        .account_delta
        .as_ref()
        .expect("MINED 必须承载唯一经济应用");
    assert_eq!(
        applied.reason,
        PlatformAccountDeltaReasonV1::FillFinalityEconomicApplied
    );
    assert_eq!(feedbacks[2].account_delta, None);
}

#[tokio::test(flavor = "current_thread")]
async fn task32_unapplied_failure_releases_principal_and_fee_once() {
    let session_id = "task32-unapplied-failure";
    let request_id = "task32-unapplied-failure-request";
    let captures = Task32Captures::default();
    let mut session = session_with_policy(
        session_id,
        Decimal::TEN,
        4,
        request_failure_policy(
            "task32-unapplied-failure-policy",
            session_id,
            request_id,
            100_000,
            None,
        ),
        &captures,
        true,
    )
    .await;
    let opening_summary = session.account_summary_sha256().clone();
    let opening_outcome = session.outcome_balances_sha256().clone();
    let fill = execute_fak_and_deliver_initial(&mut session, &captures, request_id);
    let principal_before_failure = session.principal_encumbered_pusd();
    let fee_before_failure = session.immediate_taker_fee_encumbered_pusd();
    assert!(principal_before_failure > Decimal::ZERO);
    assert!(fee_before_failure > Decimal::ZERO);
    session
        .drive_private_delivery_after_platform_through_for_tests(fill.effective_at_utc_ms + 40_000)
        .expect("终态前交付早期反馈");

    assert_eq!(
        close_platform_at(&mut session, fill.effective_at_utc_ms + 100_000),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(session.available_pusd(), Decimal::TEN);
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);
    assert_eq!(session.immediate_taker_fee_encumbered_pusd(), Decimal::ZERO);
    assert_eq!(session.account_summary_sha256(), &opening_summary);
    assert_eq!(session.outcome_balances_sha256(), &opening_outcome);
    let terminal_seq = session.next_account_transition_seq();
    assert_eq!(
        close_platform_at(&mut session, fill.effective_at_utc_ms + 100_000),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(session.next_account_transition_seq(), terminal_seq);

    session
        .drive_private_delivery_after_platform_through_for_tests(fill.effective_at_utc_ms + 140_000)
        .expect("未应用失败反馈交付");
    let feedbacks = trade_feedbacks(&captures, &fill.trade_id);
    assert_eq!(feedbacks.len(), 2);
    assert_eq!(
        feedbacks[1].economic_disposition,
        TradeEconomicDispositionV1::FinalFailureUnapplied
    );
    assert_eq!(feedbacks[1].economic_application_delta, None);
    let released = feedbacks[1]
        .account_delta
        .as_ref()
        .expect("未应用失败必须释放占用");
    assert_eq!(
        released.reason,
        PlatformAccountDeltaReasonV1::FillFinalityUnappliedFailureReleased
    );
    assert_eq!(
        decimal(&released.balance_delta.cash_pusd_delta.0),
        Decimal::ZERO
    );
    assert_eq!(
        decimal(&released.balance_delta.available_pusd_delta.0),
        principal_before_failure + fee_before_failure
    );
    assert_eq!(
        decimal(&released.balance_delta.principal_encumbrance_pusd_delta.0),
        -principal_before_failure
    );
    assert_eq!(
        decimal(
            &released
                .balance_delta
                .immediate_taker_fee_encumbrance_pusd_delta
                .0
        ),
        -fee_before_failure
    );
    assert!(released.balance_delta.outcome_balance_deltas.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn task32_applied_failure_compensates_atomically_and_references_original_delta() {
    let session_id = "task32-applied-failure";
    let request_id = "task32-applied-failure-request";
    let captures = Task32Captures::default();
    let mut session = session_with_policy(
        session_id,
        Decimal::TEN,
        4,
        request_failure_policy(
            "task32-applied-failure-policy",
            session_id,
            request_id,
            100_000,
            Some(0),
        ),
        &captures,
        true,
    )
    .await;
    let opening_summary = session.account_summary_sha256().clone();
    let opening_outcome = session.outcome_balances_sha256().clone();
    let fill = execute_fak_and_deliver_initial(&mut session, &captures, request_id);
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);
    assert_eq!(session.immediate_taker_fee_encumbered_pusd(), Decimal::ZERO);
    assert_ne!(session.outcome_balances_sha256(), &opening_outcome);
    session
        .drive_private_delivery_after_platform_through_for_tests(fill.effective_at_utc_ms + 40_000)
        .expect("终态前交付早期反馈");

    assert_eq!(
        close_platform_at(&mut session, fill.effective_at_utc_ms + 100_000),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(session.available_pusd(), Decimal::TEN);
    assert_eq!(session.account_summary_sha256(), &opening_summary);
    assert_eq!(session.outcome_balances_sha256(), &opening_outcome);
    let terminal_seq = session.next_account_transition_seq();

    session
        .drive_private_delivery_after_platform_through_for_tests(fill.effective_at_utc_ms + 140_000)
        .expect("补偿反馈交付");
    let feedbacks = trade_feedbacks(&captures, &fill.trade_id);
    assert_eq!(feedbacks.len(), 2);
    assert_eq!(
        feedbacks[0].economic_disposition,
        TradeEconomicDispositionV1::PendingApplied
    );
    let original = feedbacks[0]
        .account_delta
        .as_ref()
        .expect("首态必须原子应用经济变化");
    assert_eq!(
        original.reason,
        PlatformAccountDeltaReasonV1::FillFinalityEconomicApplied
    );
    assert_eq!(
        feedbacks[1].economic_disposition,
        TradeEconomicDispositionV1::FinalFailureCompensated
    );
    assert_eq!(
        feedbacks[1]
            .economic_application_delta
            .as_ref()
            .expect("补偿反馈必须引用原经济变化")
            .delta_id,
        original.delta_id
    );
    let compensated = feedbacks[1]
        .account_delta
        .as_ref()
        .expect("FAILED 与补偿必须同一反馈");
    assert_eq!(
        compensated.reason,
        PlatformAccountDeltaReasonV1::FillFinalityCompensationApplied
    );
    assert!(decimal(&compensated.balance_delta.cash_pusd_delta.0) > Decimal::ZERO);
    assert!(decimal(&compensated.balance_delta.available_pusd_delta.0) > Decimal::ZERO);
    assert_eq!(
        decimal(&compensated.balance_delta.principal_encumbrance_pusd_delta.0),
        Decimal::ZERO
    );
    assert_eq!(compensated.balance_delta.outcome_balance_deltas.len(), 1);
    assert!(
        decimal(
            &compensated.balance_delta.outcome_balance_deltas[0]
                .balance_shares_delta
                .0
        ) < Decimal::ZERO
    );

    assert_eq!(
        close_platform_at(&mut session, fill.effective_at_utc_ms + 100_000),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(session.next_account_transition_seq(), terminal_seq);
}

#[tokio::test(flavor = "current_thread")]
async fn task32_capacity_one_uses_latest_actual_transition_time_and_full_is_atomic() {
    let captures = Task32Captures::default();
    let mut session = session_with_policy(
        "task32-capacity",
        Decimal::TEN,
        1,
        success_policy("task32-capacity-policy", 6, 60_000),
        &captures,
        true,
    )
    .await;
    let request = fak_request(&session, "task32-capacity-request", 1, 1_200, "3", "0.6");
    let registration = session.post_order(request).expect("容量反例登记");
    let effective = registration.item.effective_processing_at_utc_ms;
    assert_eq!(
        close_platform_at(&mut session, effective),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(
        session.drive_next_execution_action_through(effective),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    let summary_before_full = session.account_summary_sha256().clone();
    let revision_before_full = session.query_projection_revision();
    let seq_before_full = session.next_account_transition_seq();
    let position_before_full = session.platform_event_index_position();
    let logical_platform_before_full = session.logical_platform_driven_through_for_tests();

    assert!(matches!(
        close_platform_at(&mut session, effective + 60_000),
        Err(MarketEventErrorV1::Session(
            SessionError::ResponseOutboxFull
        ))
    ));
    assert_eq!(session.account_summary_sha256(), &summary_before_full);
    assert_eq!(session.query_projection_revision(), revision_before_full);
    assert_eq!(session.next_account_transition_seq(), seq_before_full);
    assert_eq!(
        session.platform_event_index_position(),
        position_before_full
    );
    assert_eq!(
        session.logical_platform_driven_through_for_tests(),
        logical_platform_before_full
    );

    assert_eq!(
        session.drive_private_delivery_after_platform_through_for_tests(effective + 40_000),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let platform_order_id = {
        let responses = captures.post_order.lock().expect("容量反例响应锁");
        let SubmissionItemOutcomeV1::Accepted(accepted) = &responses[0].item.outcome else {
            panic!("容量反例订单必须接受");
        };
        accepted.simulated_platform_order_id.clone()
    };
    assert_eq!(session.simulated_fill_count(&platform_order_id), 1);
    assert_eq!(
        close_platform_at(&mut session, effective + 60_000),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(
        session.query_projection_revision(),
        revision_before_full + 2
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task32_physical_request_retry_reuses_one_fill_and_one_feedback_path() {
    let captures = Task32Captures::default();
    let mut session = session_with_policy(
        "task32-request-retry",
        Decimal::TEN,
        4,
        success_policy("task32-request-retry-policy", 100_000, 200_000),
        &captures,
        true,
    )
    .await;
    let fill =
        execute_fak_and_deliver_initial(&mut session, &captures, "task32-request-retry-request");
    session
        .drive_private_delivery_after_platform_through_for_tests(fill.effective_at_utc_ms + 40_000)
        .expect("重投前交付早期反馈");
    let summary_before_retry = session.account_summary_sha256().clone();
    let revision_before_retry = session.query_projection_revision();
    let seq_before_retry = session.next_account_transition_seq();
    let retry = session
        .post_order(fill.request.clone())
        .expect("同一物理请求重投");
    assert_eq!(
        retry.receipt.disposition,
        RequestRegistrationDispositionV1::PhysicalRetry
    );
    assert_eq!(session.simulated_order_count(), 1);
    assert_eq!(session.simulated_fill_count(&fill.platform_order_id), 1);
    assert_eq!(session.account_summary_sha256(), &summary_before_retry);
    assert_eq!(session.query_projection_revision(), revision_before_retry);
    assert_eq!(session.next_account_transition_seq(), seq_before_retry);

    assert_eq!(
        close_platform_at(&mut session, fill.effective_at_utc_ms + 100_000),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    session
        .drive_private_delivery_after_platform_through_for_tests(fill.effective_at_utc_ms + 140_000)
        .expect("重投后的唯一经济应用反馈交付");
    assert_eq!(
        close_platform_at(&mut session, fill.effective_at_utc_ms + 200_000),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    session
        .drive_private_delivery_after_platform_through_for_tests(fill.effective_at_utc_ms + 240_000)
        .expect("重投后的唯一终态反馈交付");
    let feedbacks = trade_feedbacks(&captures, &fill.trade_id);
    assert_eq!(feedbacks.len(), 3);
    assert_eq!(
        feedbacks
            .iter()
            .map(|feedback| feedback.metadata.delivery_id.as_str())
            .collect::<BTreeSet<_>>()
            .len(),
        3
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task32_trade_feedback_full_retries_the_same_sealed_queue_head() {
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let mut session = session_with_policy_and_bridge(
        "task32-trade-full",
        Decimal::TEN,
        4,
        success_policy("task32-trade-full-policy", 100_000, 200_000),
        trade_full_once_bridge(Arc::clone(&attempts)),
        true,
    )
    .await;
    let request = fak_request(&session, "task32-trade-full-request", 1, 1_200, "3", "0.6");
    let registration = session.post_order(request).expect("成交反馈 Full 反例登记");
    let effective_at = registration.item.effective_processing_at_utc_ms;
    assert_eq!(
        close_platform_at(&mut session, effective_at),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(
        session.drive_next_execution_action_through(effective_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );

    assert_eq!(
        session.drive_private_delivery_after_platform_through_for_tests(effective_at + 40_000),
        Ok(SubmissionDeliveryDispositionV1::Full)
    );
    let first = attempts
        .lock()
        .expect("首次 Full 成交反馈锁")
        .first()
        .cloned()
        .expect("首次 Full 必须保留队头");
    assert_eq!(
        session.model_visible_cursor().private_feedback_sequence + 1,
        first.metadata.private_feedback_sequence
    );
    assert_eq!(session.simulated_order_count(), 1);

    assert_eq!(
        session.drive_private_delivery_after_platform_through_for_tests(effective_at + 40_000),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let retry_attempts = attempts.lock().expect("成交反馈重试捕获锁");
    assert_eq!(retry_attempts.len(), 2);
    let retried = &retry_attempts[1];
    assert!(Arc::ptr_eq(&first, retried));
    assert_eq!(first.as_ref(), retried.as_ref());
    assert_eq!(first.metadata, retried.metadata);
    assert_eq!(
        first.metadata.payload_sha256,
        retried.metadata.payload_sha256
    );
    assert_eq!(
        first.metadata.delivery_confirmation(),
        retried.metadata.delivery_confirmation()
    );
    drop(retry_attempts);
    assert!(
        session.model_visible_cursor().private_feedback_sequence
            >= first.metadata.private_feedback_sequence
    );
    assert_eq!(
        session.drive_private_delivery_after_platform_through_for_tests(effective_at + 40_000),
        Ok(SubmissionDeliveryDispositionV1::NoVisibleResponse)
    );
    assert_eq!(attempts.lock().expect("成交反馈最终捕获锁").len(), 2);
}

#[derive(Debug, PartialEq, Eq)]
struct SameTimeFinalityOutcome {
    next_account_transition_seq: u64,
    account_summary_sha256: contracts::Sha256Hex,
    new_buy_accept_seq: u64,
    finality_delta_order: Vec<(String, u64)>,
    private_account_seq_order: Vec<(u64, u64)>,
}

fn deliver_without_backpressure(session: &mut SimExecutionSessionV1, through_utc_ms: i64) {
    match session
        .drive_private_delivery_after_platform_through_for_tests(through_utc_ms)
        .expect("任务 3.2 私有反馈交付")
    {
        SubmissionDeliveryDispositionV1::Delivered
        | SubmissionDeliveryDispositionV1::NoVisibleResponse => {}
        disposition => panic!("测试桥不得施加背压：{disposition:?}"),
    }
}

async fn run_same_time_finality_case(segmented_drive: bool) -> SameTimeFinalityOutcome {
    let session_id = "task32-same-time";
    let failed_request_id = "task32-same-time-failed";
    let normal_request_id = "task32-same-time-normal";
    let new_request_id = "task32-same-time-new";
    let target_at = 260_000;
    let policy = policy_for_tests(
        "task32-same-time-policy",
        &[
            (TradeFinalityStateV1::Matched, 0),
            (TradeFinalityStateV1::Mined, 60_000),
            (TradeFinalityStateV1::Confirmed, 90_000),
        ],
        1,
        vec![(
            TradeFinalitySelectorV1::Request {
                account_instance_key: account_key(session_id),
                global_request_id: failed_request_id.to_owned(),
            },
            vec![
                (TradeFinalityStateV1::Matched, 0),
                (TradeFinalityStateV1::Failed, 30_000),
            ],
            Some(0),
        )],
    );
    let captures = Task32Captures::default();
    let profile = load_embedded_proxy_latency_profile().expect("同刻测试代理时延配置档");
    let mut session =
        long_session_with_policy(session_id, decimal("9.5"), 8, policy, &captures).await;

    let normal_request =
        fak_request_for_effective_at(&session, &profile, normal_request_id, 1, 200_000, "3");
    let normal = session.post_order(normal_request).expect("普通成交登记");
    assert_eq!(normal.item.effective_processing_at_utc_ms, 200_000);
    let failed_request =
        fak_request_for_effective_at(&session, &profile, failed_request_id, 2, 230_000, "3");
    let failed = session
        .post_order(failed_request.clone())
        .expect("已应用失败成交登记");
    assert_eq!(failed.item.effective_processing_at_utc_ms, 230_000);
    let new_request =
        fak_request_for_effective_at(&session, &profile, new_request_id, 3, target_at, "6");
    let new_registration = session.post_order(new_request).expect("同刻新 BUY 登记");
    assert_eq!(
        new_registration.item.effective_processing_at_utc_ms,
        target_at
    );

    assert_eq!(
        close_platform_at(&mut session, 200_000),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(
        session.drive_next_execution_action_through(200_000),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    if segmented_drive {
        deliver_without_backpressure(&mut session, 210_000);
    }
    deliver_without_backpressure(&mut session, 229_999);

    assert_eq!(
        close_platform_at(&mut session, 230_000),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(
        session.drive_next_execution_action_through(230_000),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    if segmented_drive {
        deliver_without_backpressure(&mut session, 240_000);
    }
    deliver_without_backpressure(&mut session, 259_999);

    assert!(
        session.available_pusd() < decimal("6"),
        "同刻前可用资金应不足 6 pUSD，实际 {}，响应 {:?}",
        session.available_pusd(),
        captures.post_order.lock().expect("同刻诊断响应锁")
    );
    assert_eq!(
        close_platform_at(&mut session, target_at),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert!(session.available_pusd() > decimal("6"));
    assert_eq!(
        session.drive_next_execution_action_through(target_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    if segmented_drive {
        deliver_without_backpressure(&mut session, target_at + 10_000);
    }
    deliver_without_backpressure(&mut session, target_at + 29_999);

    let responses = captures.post_order.lock().expect("同刻响应捕获锁");
    assert_eq!(responses.len(), 3);
    let (_, normal_trade_id) = accepted_fill_identity(&responses[0]);
    let (_, failed_trade_id) = accepted_fill_identity(&responses[1]);
    assert!(
        normal_trade_id > failed_trade_id,
        "先创建的普通成交 ID 必须大于后创建的失败成交 ID，才能反驳插入序实现"
    );
    let SubmissionItemOutcomeV1::Accepted(new_accepted) = &responses[2].item.outcome else {
        panic!("补偿后同刻新 BUY 必须通过资金门");
    };
    let new_accept_seq = new_accepted
        .account_deltas
        .first()
        .expect("新 BUY 接受变化")
        .account_transition_seq;
    drop(responses);

    let failed_feedback = trade_feedbacks(&captures, &failed_trade_id)
        .into_iter()
        .find(|feedback| {
            feedback.metadata.platform_event_at_utc_ms == target_at
                && feedback.status.known == Some(SimulatedTradeKnownStatusV1::Failed)
        })
        .expect("同刻失败补偿反馈");
    let normal_feedback = trade_feedbacks(&captures, &normal_trade_id)
        .into_iter()
        .find(|feedback| {
            feedback.metadata.platform_event_at_utc_ms == target_at
                && feedback.status.known == Some(SimulatedTradeKnownStatusV1::Mined)
        })
        .expect("同刻普通经济应用反馈");
    let failed_delta = failed_feedback
        .account_delta
        .as_ref()
        .expect("同刻失败补偿账户变化");
    let normal_delta = normal_feedback
        .account_delta
        .as_ref()
        .expect("同刻普通经济应用账户变化");
    assert_eq!(
        failed_delta.reason,
        PlatformAccountDeltaReasonV1::FillFinalityCompensationApplied
    );
    assert_eq!(
        normal_delta.reason,
        PlatformAccountDeltaReasonV1::FillFinalityEconomicApplied
    );
    let actual = [
        (
            failed_trade_id.as_str(),
            failed_delta.account_transition_seq,
        ),
        (
            normal_trade_id.as_str(),
            normal_delta.account_transition_seq,
        ),
    ];
    let mut expected = actual;
    expected.sort_by_key(|(trade_id, _)| *trade_id);
    assert!(expected[0].1 < expected[1].1);
    assert_eq!(actual[0].1 < actual[1].1, failed_trade_id < normal_trade_id);
    assert!(new_accept_seq > failed_delta.account_transition_seq);
    assert!(new_accept_seq > normal_delta.account_transition_seq);
    let target_account_feedbacks = captures
        .trade_lifecycle
        .lock()
        .expect("同刻成交反馈锁")
        .iter()
        .filter(|feedback| feedback.metadata.platform_event_at_utc_ms == target_at)
        .filter(|feedback| feedback.account_delta.is_some())
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(target_account_feedbacks.len(), 2);
    let later = &target_account_feedbacks[0];
    let predecessor = &target_account_feedbacks[1];
    let later_delta = later.account_delta.as_ref().expect("后继账户变化");
    let predecessor_delta = predecessor.account_delta.as_ref().expect("前驱账户变化");
    assert_eq!(
        later_delta.account_transition_seq,
        predecessor_delta.account_transition_seq + 1
    );
    assert!(
        later.metadata.feedback_received_at_utc_ms
            < predecessor.metadata.feedback_received_at_utc_ms
    );
    assert!(
        later.metadata.private_feedback_sequence < predecessor.metadata.private_feedback_sequence
    );
    assert!(matches!(
        &later_delta.predecessor,
        PlatformAccountDeltaPredecessorV1::AccountDelta {
            delta_id,
            delta_sha256,
            account_transition_seq,
        } if delta_id == &predecessor_delta.delta_id
            && delta_sha256 == &predecessor_delta.delta_sha256
            && *account_transition_seq == predecessor_delta.account_transition_seq
    ));
    let private_account_seq_order = target_account_feedbacks
        .iter()
        .map(|feedback| {
            (
                feedback.metadata.private_feedback_sequence,
                feedback
                    .account_delta
                    .as_ref()
                    .expect("同刻账户变化")
                    .account_transition_seq,
            )
        })
        .collect::<Vec<_>>();
    let seq_before_retry = session.next_account_transition_seq();
    let retry = session
        .post_order(failed_request)
        .expect("同刻失败请求物理重投");
    assert_eq!(
        retry.receipt.disposition,
        RequestRegistrationDispositionV1::PhysicalRetry
    );
    assert_eq!(session.next_account_transition_seq(), seq_before_retry);
    assert_eq!(
        captures
            .trade_lifecycle
            .lock()
            .expect("重投后成交反馈锁")
            .iter()
            .filter(|feedback| {
                feedback.metadata.platform_event_at_utc_ms == target_at
                    && feedback.account_delta.is_some()
            })
            .count(),
        2
    );

    SameTimeFinalityOutcome {
        next_account_transition_seq: session.next_account_transition_seq(),
        account_summary_sha256: session.account_summary_sha256().clone(),
        new_buy_accept_seq: new_accept_seq,
        finality_delta_order: expected
            .into_iter()
            .map(|(trade_id, sequence)| (trade_id.to_owned(), sequence))
            .collect(),
        private_account_seq_order,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task32_same_time_finality_deltas_precede_new_buy_and_follow_fill_key_order() {
    let direct = run_same_time_finality_case(false).await;
    let segmented_clean_replay = run_same_time_finality_case(true).await;
    assert_eq!(segmented_clean_replay, direct);
}

#[tokio::test(flavor = "current_thread")]
async fn task32_absolute_finality_time_overflow_precedes_zero_step_and_all_state() {
    let captures = Task32Captures::default();
    let policy = policy_for_tests(
        "task32-time-overflow-policy",
        &[
            (TradeFinalityStateV1::Matched, 0),
            (TradeFinalityStateV1::Mined, i64::MAX - 1),
            (TradeFinalityStateV1::Confirmed, i64::MAX),
        ],
        1,
        Vec::new(),
    );
    let mut session = session_with_policy(
        "task32-time-overflow",
        Decimal::TEN,
        4,
        policy,
        &captures,
        true,
    )
    .await;
    let request = fak_request(
        &session,
        "task32-time-overflow-request",
        1,
        1_200,
        "3",
        "0.6",
    );
    let registration = session.post_order(request).expect("溢出反例登记");
    let effective = registration.item.effective_processing_at_utc_ms;
    assert_eq!(
        close_platform_at(&mut session, effective),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let summary_before = session.account_summary_sha256().clone();
    let outcome_before = session.outcome_balances_sha256().clone();
    let revision_before = session.query_projection_revision();
    let seq_before = session.next_account_transition_seq();

    assert!(matches!(
        session.drive_next_execution_action_through(effective),
        Err(SubmissionProcessingErrorV1::Matching(
            MatchingStateErrorV1::Finality(_)
        ))
    ));
    assert_eq!(session.simulated_order_count(), 0);
    assert_eq!(session.account_summary_sha256(), &summary_before);
    assert_eq!(session.outcome_balances_sha256(), &outcome_before);
    assert_eq!(session.query_projection_revision(), revision_before);
    assert_eq!(session.next_account_transition_seq(), seq_before);
    assert!(captures.post_order.lock().expect("溢出响应锁").is_empty());
    assert!(
        captures
            .trade_lifecycle
            .lock()
            .expect("溢出反馈锁")
            .is_empty()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task32_old_bucket_retry_survives_later_platform_watermark() {
    let captures = Task32Captures::default();
    let mut session = session_with_policy(
        "task32-old-bucket-retry",
        Decimal::TEN,
        4,
        success_policy("task32-old-bucket-policy", 100_000, 200_000),
        &captures,
        false,
    )
    .await;
    let initial_events = [
        initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10"),
        initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "12"),
    ];
    let original = hidden_bucket(1_100, 2, &initial_events);
    assert_eq!(
        session.apply_closed_market_event_bucket(original.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    session.prepare_private_delivery_through_for_tests(1_150);
    let summary_before_retry = session.account_summary_sha256().clone();
    let revision_before_retry = session.query_projection_revision();
    let seq_before_retry = session.next_account_transition_seq();
    let position_before_retry = session.platform_event_index_position();

    assert_eq!(
        session.apply_closed_market_event_bucket(original),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(
        session.platform_event_index_position(),
        position_before_retry
    );
    assert_eq!(session.account_summary_sha256(), &summary_before_retry);
    assert_eq!(session.query_projection_revision(), revision_before_retry);
    assert_eq!(session.next_account_transition_seq(), seq_before_retry);
}
