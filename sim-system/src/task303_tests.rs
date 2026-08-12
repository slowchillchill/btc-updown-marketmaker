use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use contracts::{PlatformAccountDeltaPredecessorV1, PlatformAccountDeltaReasonV1};
use rust_decimal::Decimal;

use crate::account::{InitialAccountStateV1, PlatformAccountStateV1};
use crate::execution::{
    AccountDeltaReferenceV1, CancelEffectDispositionV1, CancelNotCanceledReasonV1,
    CancelOrderDirectResponseV1, CancelOrderItemV1, CancelOrderRequestV1, CancelTargetOutcomeV1,
    OrderLifecycleEventKindV1, OrderLifecycleFeedbackV1, PlatformScenarioV1,
    PostOrderDirectResponseV1, PostOrderItemV1, PostOrderRequestV1, SimBuyOrderKindV1,
    SimBuyOrderV1, SimCallerRoleV1, SimExecutionSessionV1, SimulatedOrderKnownStatusV1,
    SubmissionDeliveryDispositionV1, SubmissionDriveDispositionV1, SubmissionEffectDispositionV1,
    SubmissionItemOutcomeV1, SubmissionResponseAcceptanceV1, SubmissionResponseBridgeV1,
    accepting_submission_response_bridge, execution_process_for_tests,
};
use crate::latency_profile::{EmpiricalDelayProfile, load_embedded_proxy_latency_profile};
use crate::market::MarketBucketApplyDispositionV1;
use crate::platform_rules::{SealedProcessRuleSnapshot, tests::successful_snapshot_for_sim_tests};
use crate::session::{RequestContextV1, RequestRegistrationDispositionV1};
use crate::task21_tests::{
    asset, hidden_bucket as short_hidden_bucket, initial_snapshot_event, market, scenario,
    session_spec,
};
use crate::task22_tests::{
    asset as long_asset, context_for_arrival as long_context_for_arrival,
    hidden_bucket as long_hidden_bucket, load_initial_market as load_long_initial_market,
    market as long_market, scenario as long_scenario, session_spec as long_session_spec,
};
use crate::task23_tests::{ask_transition, close_platform_at, fak_request, ready_session};

#[tokio::test(flavor = "current_thread")]
async fn task303_account_chain_starts_at_opening_anchor_and_fak_release_is_a_second_delta() {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
    let scenario = scenario();
    let mut spec = session_spec(
        &scenario,
        SimCallerRoleV1::ModelTrain,
        "task303-account-chain",
    );
    spec.initial_account_state =
        InitialAccountStateV1::try_new(Decimal::new(2, 0)).expect("显式初始资金");
    let mut process = execution_process_for_tests(rules, profile, Arc::clone(&scenario));
    let prepared = process
        .create_session(spec, accepting_submission_response_bridge())
        .expect("准备态会话");
    let anchor = prepared.control.opening_anchor;
    let mut account =
        PlatformAccountStateV1::try_from_opening_anchor(&anchor).expect("唯一平台开户锚点");
    let cloned_account = account.clone();
    assert!(account.shares_outcome_balance_storage_with(&cloned_account));
    drop(cloned_account);
    let opening_outcome_balances_sha256 = account.outcome_balances_sha256().clone();

    let accepted = account
        .apply_order_accepted(
            "accept-effect-a",
            "condition-a",
            1_000,
            Decimal::new(1, 0),
            Decimal::new(1, 1),
        )
        .expect("订单接受占用");
    assert_eq!(accepted.account_transition_seq, 1);
    assert!(matches!(
        &accepted.predecessor,
        PlatformAccountDeltaPredecessorV1::OpeningAnchor {
            anchor_id,
            anchor_sha256,
        } if anchor_id.as_str() == anchor.anchor_id.as_str()
            && anchor_sha256.0.as_str() == anchor.anchor_sha256.0.as_str()
    ));
    assert_eq!(
        accepted.reason,
        PlatformAccountDeltaReasonV1::OrderAcceptedEncumbranceApplied
    );

    let released = account
        .apply_fak_unspent_release("fak-release-a", "condition-a", 1_000, Decimal::new(4, 1))
        .expect("FAK 未花预算释放")
        .expect("正释放必须形成变化");
    assert_eq!(released.account_transition_seq, 2);
    assert!(matches!(
        &released.predecessor,
        PlatformAccountDeltaPredecessorV1::AccountDelta {
            delta_id,
            delta_sha256,
            account_transition_seq: 1,
        } if delta_id.as_str() == accepted.delta_id.as_str()
            && delta_sha256.0.as_str() == accepted.delta_sha256.0.as_str()
    ));
    assert_eq!(
        released.reason,
        PlatformAccountDeltaReasonV1::FakUnspentEncumbranceReleased
    );

    let snapshot = account.snapshot();
    assert_eq!(snapshot.cash_pusd.0, "2");
    assert_eq!(snapshot.available_pusd.0, "1.3");
    assert_eq!(snapshot.principal_encumbrance_pusd.0, "0.6");
    assert_eq!(snapshot.immediate_taker_fee_encumbrance_pusd.0, "0.1");
    assert_eq!(account.next_account_transition_seq(), 3);
    assert_eq!(
        account.outcome_balances_sha256(),
        &opening_outcome_balances_sha256,
        "纯 pUSD 占用与释放必须复用结果代币组件摘要"
    );

    let retried = account
        .apply_fak_unspent_release("fak-release-a", "condition-a", 1_000, Decimal::new(4, 1))
        .expect("相同原因物理重试")
        .expect("原正释放变化仍可取得");
    assert_eq!(retried, released);
    assert_eq!(account.next_account_transition_seq(), 3);

    let second_market = account
        .apply_order_accepted(
            "accept-effect-b",
            "condition-b",
            1_001,
            Decimal::new(13, 1),
            Decimal::ZERO,
        )
        .expect("第二个重叠市场消费同一资金池");
    assert_eq!(second_market.account_transition_seq, 3);
    assert_eq!(
        second_market.condition_id.as_ref().map(|id| id.0.as_str()),
        Some("condition-b")
    );
    assert!(matches!(
        &second_market.predecessor,
        PlatformAccountDeltaPredecessorV1::AccountDelta {
            delta_id,
            delta_sha256,
            account_transition_seq: 2,
        } if delta_id == &released.delta_id && delta_sha256 == &released.delta_sha256
    ));
    assert_eq!(account.available_pusd(), Decimal::ZERO);
    let exhausted_summary = account.account_summary_sha256().clone();
    assert!(
        account
            .apply_order_accepted(
                "accept-after-exhaustion",
                "condition-a",
                1_002,
                Decimal::new(1, 6),
                Decimal::ZERO,
            )
            .is_err()
    );
    assert_eq!(account.next_account_transition_seq(), 4);
    assert_eq!(account.account_summary_sha256(), &exhausted_summary);
}

#[tokio::test(flavor = "current_thread")]
async fn task303_account_rejects_unrepresentable_initial_cash_and_marketless_order_delta() {
    assert!(InitialAccountStateV1::try_new(Decimal::MAX).is_err());

    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
    let scenario = scenario();
    let spec = session_spec(
        &scenario,
        SimCallerRoleV1::ModelTrain,
        "task303-account-boundary",
    );
    let mut process = execution_process_for_tests(rules, profile, Arc::clone(&scenario));
    let prepared = process
        .create_session(spec, accepting_submission_response_bridge())
        .expect("准备态会话");
    let mut account =
        PlatformAccountStateV1::try_from_opening_anchor(&prepared.control.opening_anchor)
            .expect("唯一平台开户锚点");
    let summary_before = account.account_summary_sha256().clone();

    assert!(
        account
            .apply_order_accepted("marketless-order", "", 1_000, Decimal::ONE, Decimal::ZERO,)
            .is_err()
    );
    assert_eq!(account.next_account_transition_seq(), 1);
    assert_eq!(account.account_summary_sha256(), &summary_before);
}

#[tokio::test(flavor = "current_thread")]
async fn task303_capacity_one_delivers_fak_account_chain_and_direct_references_without_deadlock() {
    let lifecycle = Arc::new(Mutex::new(Vec::new()));
    let direct = Arc::new(Mutex::new(Vec::new()));
    let lifecycle_capture = Arc::clone(&lifecycle);
    let direct_capture = Arc::clone(&direct);
    let bridge = SubmissionResponseBridgeV1::new(
        move |response| {
            direct_capture
                .lock()
                .expect("直接响应捕获锁")
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
        |response| {
            SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
        },
        move |feedback: Arc<OrderLifecycleFeedbackV1>| {
            lifecycle_capture
                .lock()
                .expect("订单生命周期反馈捕获锁")
                .push(Arc::clone(&feedback));
            SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
        },
        |feedback| {
            SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
        },
        |feedback| {
            SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
        },
    );
    let mut session = ready_session("task303-capacity-one", Decimal::TEN, 1, bridge).await;
    let remove_initial_ask = ask_transition(1_101, 1_201, 3, "0.6", "5", "0");
    session
        .apply_closed_market_event_bucket(crate::task21_tests::hidden_bucket(
            1_101,
            3,
            &[remove_initial_ask],
        ))
        .expect("移除初始卖盘");
    let two_pusd_ask = ask_transition(1_102, 1_202, 4, "0.4", "0", "5");
    session
        .apply_closed_market_event_bucket(crate::task21_tests::hidden_bucket(
            1_102,
            4,
            &[two_pusd_ask],
        ))
        .expect("加入精确 2 pUSD 卖盘");
    let registration = session
        .post_order(fak_request(
            &session,
            "task303-partial-fak",
            1,
            1_200,
            "3",
            "0.6",
        ))
        .expect("容量为 1 时仍可原子预留请求与生命周期批次");
    let effective = registration.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, effective, 4);
    session
        .drive_next_execution_action_through(effective)
        .expect("FAK 逻辑动作");

    let cutoff = effective + 10_000;
    assert_eq!(
        session
            .drive_private_delivery_after_platform_through_for_tests(cutoff)
            .expect("按私有队头交替推进平台和交付"),
        SubmissionDeliveryDispositionV1::Delivered
    );

    let lifecycle = lifecycle.lock().expect("订单生命周期反馈捕获锁");
    assert_eq!(lifecycle.len(), 2);
    assert_eq!(lifecycle[0].account_delta.account_transition_seq, 1);
    assert_eq!(lifecycle[1].account_delta.account_transition_seq, 2);
    let direct = direct.lock().expect("直接响应捕获锁");
    let crate::execution::SubmissionItemOutcomeV1::Accepted(accepted) = &direct[0].item.outcome
    else {
        panic!("FAK 必须接受");
    };
    assert_eq!(accepted.account_deltas.len(), 2);
    assert_eq!(
        accepted.account_deltas[0].delta_id,
        lifecycle[0].account_delta.delta_id
    );
    assert_eq!(
        accepted.account_deltas[1].delta_id,
        lifecycle[1].account_delta.delta_id
    );
}

#[derive(Clone, Default)]
struct Task303DeliveryCaptures {
    post_order: Arc<Mutex<Vec<Arc<PostOrderDirectResponseV1>>>>,
    cancel_order: Arc<Mutex<Vec<Arc<CancelOrderDirectResponseV1>>>>,
    lifecycle: Arc<Mutex<Vec<Arc<OrderLifecycleFeedbackV1>>>>,
}

impl Task303DeliveryCaptures {
    fn bridge(&self) -> SubmissionResponseBridgeV1 {
        let post_order = Arc::clone(&self.post_order);
        let cancel_order = Arc::clone(&self.cancel_order);
        let lifecycle = Arc::clone(&self.lifecycle);
        SubmissionResponseBridgeV1::new(
            move |response| {
                post_order
                    .lock()
                    .expect("提交响应捕获锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                cancel_order
                    .lock()
                    .expect("撤单响应捕获锁")
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
            move |feedback| {
                lifecycle
                    .lock()
                    .expect("订单生命周期反馈捕获锁")
                    .push(Arc::clone(&feedback));
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
            |feedback| {
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
            |feedback| {
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
        )
    }
}

fn task303_context(
    session: &SimExecutionSessionV1,
    request_id: &str,
    caller_sequence: u64,
    request_sent_at_utc_ms: i64,
) -> RequestContextV1 {
    let spec = session.session_spec();
    RequestContextV1 {
        execution_contract_version: spec.execution_contract_version.clone(),
        session_id: spec.session_id.clone(),
        account_id: spec.account_id.clone(),
        caller_role: spec.caller_role,
        caller_instance_id: spec.caller_instance_id.clone(),
        global_request_id: request_id.to_owned(),
        caller_correlation_id: Some(format!("correlation-{request_id}")),
        caller_sequence,
        request_sent_at_utc_ms,
        model_visible_cursor: session.model_visible_cursor(),
    }
}

fn task303_gtc_request(
    session: &SimExecutionSessionV1,
    request_id: &str,
    caller_sequence: u64,
    request_sent_at_utc_ms: i64,
) -> PostOrderRequestV1 {
    PostOrderRequestV1 {
        context: task303_context(session, request_id, caller_sequence, request_sent_at_utc_ms),
        idempotency_key: format!("idem-{request_id}"),
        item: PostOrderItemV1 {
            item_id: format!("item-{request_id}"),
            order: SimBuyOrderV1 {
                condition_id: market().condition_id,
                token_id: asset("1", 0, "Up").token_id,
                kind: SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: Decimal::new(4, 1),
                    size_shares: Decimal::new(5, 0),
                    post_only: true,
                },
            },
        },
    }
}

fn drain_private_delivery(session: &mut SimExecutionSessionV1, cutoff: i64) {
    match session
        .drive_private_delivery_after_platform_through_for_tests(cutoff)
        .expect("按私有队头交替推进平台和交付")
    {
        SubmissionDeliveryDispositionV1::Delivered
        | SubmissionDeliveryDispositionV1::NoVisibleResponse => {}
        disposition => panic!("测试桥不应拒绝私有交付：{disposition:?}"),
    }
}

struct CompletedPositiveCancel {
    session: SimExecutionSessionV1,
    captures: Task303DeliveryCaptures,
    post_request: PostOrderRequestV1,
    cancel_request: CancelOrderRequestV1,
    cancel_at: i64,
    delivery_cutoff: i64,
}

async fn completed_positive_cancel(
    session_id: &str,
    initial_cash: Decimal,
) -> CompletedPositiveCancel {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
    let scenario = scenario();
    completed_positive_cancel_with_foundation(session_id, initial_cash, rules, profile, scenario)
}

fn completed_positive_cancel_with_foundation(
    session_id: &str,
    initial_cash: Decimal,
    rules: Arc<SealedProcessRuleSnapshot>,
    profile: Arc<EmpiricalDelayProfile>,
    scenario: Arc<PlatformScenarioV1>,
) -> CompletedPositiveCancel {
    let captures = Task303DeliveryCaptures::default();
    let mut spec = session_spec(&scenario, SimCallerRoleV1::ModelTrain, session_id);
    spec.initial_account_state =
        InitialAccountStateV1::try_new(initial_cash).expect("显式初始资金");
    spec.response_outbox_capacity = NonZeroUsize::new(4).expect("正反馈容量");
    let mut session = execution_process_for_tests(rules, profile, scenario)
        .create_active_session_for_tests(spec, captures.bridge());
    let initial = [
        initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10"),
        initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "12"),
    ];
    session
        .apply_closed_market_event_bucket(short_hidden_bucket(1_100, 2, &initial))
        .expect("初始市场桶");
    let post_request = task303_gtc_request(&session, "open-for-cancel", 1, 1_300);
    let post = session
        .post_order(post_request.clone())
        .expect("开放订单登记");
    let post_at = post.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, post_at, 2);
    assert_eq!(
        session.drive_next_execution_action_through(post_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    let post_delivery_cutoff = post_at + 10_000;
    drain_private_delivery(&mut session, post_delivery_cutoff);
    let platform_order_id = {
        let responses = captures.post_order.lock().expect("提交响应捕获锁");
        assert_eq!(responses.len(), 1);
        let SubmissionItemOutcomeV1::Accepted(accepted) = &responses[0].item.outcome else {
            panic!("开放订单必须接受");
        };
        accepted.simulated_platform_order_id.clone()
    };

    let cancel_request = CancelOrderRequestV1 {
        context: task303_context(
            &session,
            "positive-release-cancel",
            2,
            post_delivery_cutoff + 1,
        ),
        idempotency_key: "positive-release-cancel-idem".to_owned(),
        item: CancelOrderItemV1 {
            item_id: "positive-release-cancel-item".to_owned(),
            platform_order_id,
        },
    };
    let cancel = session
        .cancel_order(cancel_request.clone())
        .expect("正资金释放撤单登记");
    let cancel_at = cancel.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, cancel_at, 2);
    assert_eq!(
        session.drive_next_execution_action_through(cancel_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    let delivery_cutoff = cancel_at + 10_000;
    drain_private_delivery(&mut session, delivery_cutoff);
    CompletedPositiveCancel {
        session,
        captures,
        post_request,
        cancel_request,
        cancel_at,
        delivery_cutoff,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task303_cancel_response_and_feedback_share_one_delta_and_physical_retry_is_read_only() {
    let mut case = completed_positive_cancel("task303-positive-cancel", Decimal::TEN).await;
    assert_eq!(case.session.available_pusd(), Decimal::TEN);
    assert_eq!(case.session.principal_encumbered_pusd(), Decimal::ZERO);

    let lifecycle = case
        .captures
        .lifecycle
        .lock()
        .expect("订单生命周期反馈捕获锁")
        .clone();
    assert_eq!(lifecycle.len(), 2);
    assert_eq!(
        lifecycle[0].event_kind,
        OrderLifecycleEventKindV1::OrderAccepted
    );
    let canceled_feedback = &lifecycle[1];
    assert_eq!(
        canceled_feedback.event_kind,
        OrderLifecycleEventKindV1::CallerCanceled
    );
    assert_eq!(
        canceled_feedback.order_status.known,
        Some(SimulatedOrderKnownStatusV1::Canceled)
    );
    assert_eq!(
        canceled_feedback.account_delta.reason,
        PlatformAccountDeltaReasonV1::CallerCancellationEncumbranceReleased
    );
    assert_eq!(
        canceled_feedback.account_delta.platform_effective_at_utc_ms,
        case.cancel_at
    );
    assert_eq!(
        canceled_feedback.account_delta.condition_id,
        Some(market().condition_id)
    );
    assert_eq!(
        canceled_feedback.metadata.account_instance_key,
        canceled_feedback.account_delta.account_instance_key
    );
    assert_eq!(
        canceled_feedback.platform_order_id,
        case.cancel_request.item.platform_order_id
    );
    assert_eq!(canceled_feedback.token_id, asset("1", 0, "Up").token_id);
    assert!(matches!(
        &canceled_feedback.account_delta.predecessor,
        PlatformAccountDeltaPredecessorV1::AccountDelta {
            delta_id,
            delta_sha256,
            account_transition_seq: 1,
        } if delta_id == &lifecycle[0].account_delta.delta_id
            && delta_sha256 == &lifecycle[0].account_delta.delta_sha256
    ));

    let cancel_responses = case
        .captures
        .cancel_order
        .lock()
        .expect("撤单响应捕获锁")
        .clone();
    assert_eq!(cancel_responses.len(), 1);
    assert_eq!(
        cancel_responses[0].target.cancel_effect_disposition,
        CancelEffectDispositionV1::NewEffect
    );
    let CancelTargetOutcomeV1::Canceled(measurement) = &cancel_responses[0].target.outcome else {
        panic!("开放余量撤单必须形成正资金释放");
    };
    assert_eq!(measurement.released_quote_amount_pusd, Decimal::new(2, 0));
    assert_eq!(
        measurement.account_delta.as_ref(),
        Some(&AccountDeltaReferenceV1::from(
            &canceled_feedback.account_delta
        ))
    );

    let retry = case
        .session
        .cancel_order(case.cancel_request.clone())
        .expect("完全相同撤单物理重投");
    assert_eq!(
        retry.receipt.disposition,
        RequestRegistrationDispositionV1::PhysicalRetry
    );
    assert_eq!(
        case.session
            .drive_next_execution_action_through(case.delivery_cutoff)
            .expect("物理重投不得重建动作"),
        SubmissionDriveDispositionV1::NoAction
    );
    assert_eq!(
        case.session
            .drive_private_delivery_after_platform_through_for_tests(case.delivery_cutoff + 1)
            .expect("物理重投不得重建反馈"),
        SubmissionDeliveryDispositionV1::NoVisibleResponse
    );
    assert_eq!(case.session.available_pusd(), Decimal::TEN);
    assert_eq!(case.session.principal_encumbered_pusd(), Decimal::ZERO);
    assert_eq!(
        case.captures
            .lifecycle
            .lock()
            .expect("订单生命周期反馈捕获锁")
            .len(),
        2
    );
    assert_eq!(
        case.captures
            .cancel_order
            .lock()
            .expect("撤单响应捕获锁")
            .len(),
        1
    );

    let mut logical_retry = case.post_request.clone();
    logical_retry.context = task303_context(
        &case.session,
        "open-for-cancel-logical-retry",
        3,
        case.delivery_cutoff + 2,
    );
    let logical_retry = case
        .session
        .post_order(logical_retry)
        .expect("后继账户变化之后的原提交逻辑重试");
    let retry_at = logical_retry.item.effective_processing_at_utc_ms;
    close_platform_at(&mut case.session, retry_at, 2);
    assert_eq!(
        case.session.drive_next_execution_action_through(retry_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    let retry_delivery_cutoff = retry_at
        + i64::try_from(logical_retry.receipt.transit.inbound_delay_ms)
            .expect("测试入站延迟可表示为 i64");
    drain_private_delivery(&mut case.session, retry_delivery_cutoff);

    let post_responses = case.captures.post_order.lock().expect("提交响应捕获锁");
    assert_eq!(post_responses.len(), 2);
    assert_eq!(
        post_responses[1].item.submission_effect_disposition,
        SubmissionEffectDispositionV1::ExistingEffect
    );
    assert_eq!(
        post_responses[1].item.outcome,
        post_responses[0].item.outcome
    );
    assert_eq!(case.session.available_pusd(), Decimal::TEN);
    assert_eq!(case.session.principal_encumbered_pusd(), Decimal::ZERO);
    assert_eq!(
        case.captures
            .lifecycle
            .lock()
            .expect("订单生命周期反馈捕获锁")
            .len(),
        2,
        "A→B 后逻辑重试 A 不得追加账户变化或生命周期反馈"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task303_gtd_bucket_and_same_time_action_form_one_release_before_cancel() {
    let captures = Task303DeliveryCaptures::default();
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
    let scenario = long_scenario();
    let mut spec = long_session_spec(
        &scenario,
        SimCallerRoleV1::ModelTrain,
        "task303-gtd-collision",
    );
    spec.response_outbox_capacity = NonZeroUsize::new(4).expect("正反馈容量");
    let mut session = execution_process_for_tests(rules, Arc::clone(&profile), scenario)
        .create_active_session_for_tests(spec, captures.bridge());
    load_long_initial_market(&mut session, "0.00001", "0.6", "5");

    let post = session
        .post_order(PostOrderRequestV1 {
            context: long_context_for_arrival(&session, &profile, "gtd-source", 1, 150_000),
            idempotency_key: "gtd-source-idem".to_owned(),
            item: PostOrderItemV1 {
                item_id: "gtd-source-item".to_owned(),
                order: SimBuyOrderV1 {
                    condition_id: long_market().condition_id,
                    token_id: long_asset("1", 0, "Up").token_id,
                    kind: SimBuyOrderKindV1::GtdLimit {
                        limit_price_pusd_per_share: Decimal::new(1, 5),
                        size_shares: Decimal::new(5, 0),
                        post_only: true,
                        expires_at_utc_ms: 391_999,
                    },
                },
            },
        })
        .expect("GTD 订单登记");
    let post_at = post.item.effective_processing_at_utc_ms;
    assert_eq!(post_at, 150_000);
    let position = session.platform_event_index_position();
    session
        .apply_closed_market_event_bucket(long_hidden_bucket(post_at, position, position, &[]))
        .expect("关闭 GTD 提交时点");
    assert_eq!(
        session.drive_next_execution_action_through(post_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    drain_private_delivery(&mut session, 390_998);
    let platform_order_id = {
        let responses = captures.post_order.lock().expect("提交响应捕获锁");
        let SubmissionItemOutcomeV1::Accepted(accepted) = &responses[0].item.outcome else {
            panic!("GTD 订单必须接受");
        };
        accepted.simulated_platform_order_id.clone()
    };
    assert_eq!(
        captures
            .lifecycle
            .lock()
            .expect("订单生命周期反馈捕获锁")
            .len(),
        1
    );

    let cancel = session
        .cancel_order(CancelOrderRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "cancel-at-gtd-deadline",
                2,
                391_000,
            ),
            idempotency_key: "cancel-at-gtd-deadline-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "cancel-at-gtd-deadline-item".to_owned(),
                platform_order_id: platform_order_id.clone(),
            },
        })
        .expect("同刻撤单登记");
    let cancel_at = cancel.item.effective_processing_at_utc_ms;
    assert_eq!(cancel_at, 391_000);
    let position = session.platform_event_index_position();
    let expiry_bucket = long_hidden_bucket(cancel_at, position, position, &[]);
    assert_eq!(
        session
            .apply_closed_market_event_bucket(expiry_bucket.clone())
            .expect("市场桶推进 GTD 到期"),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(session.available_pusd(), Decimal::new(1_000, 0));
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);

    assert_eq!(
        session.drive_next_execution_action_through(cancel_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    assert_eq!(
        session
            .apply_closed_market_event_bucket(expiry_bucket)
            .expect("同一市场桶物理重投"),
        MarketBucketApplyDispositionV1::PhysicalRetry
    );
    assert_eq!(
        session.drive_next_execution_action_through(cancel_at),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    drain_private_delivery(&mut session, 450_000);

    let lifecycle = captures
        .lifecycle
        .lock()
        .expect("订单生命周期反馈捕获锁")
        .clone();
    assert_eq!(lifecycle.len(), 2);
    let expired = lifecycle
        .iter()
        .filter(|feedback| feedback.event_kind == OrderLifecycleEventKindV1::GtdExpired)
        .collect::<Vec<_>>();
    assert_eq!(expired.len(), 1);
    let expired = expired[0];
    assert_eq!(expired.platform_order_id, platform_order_id);
    assert_eq!(
        expired.order_status.known,
        Some(SimulatedOrderKnownStatusV1::Expired)
    );
    assert_eq!(expired.account_delta.account_transition_seq, 2);
    assert_eq!(
        expired.account_delta.reason,
        PlatformAccountDeltaReasonV1::GtdExpirationEncumbranceReleased
    );
    assert_eq!(
        expired.account_delta.platform_effective_at_utc_ms,
        cancel_at
    );
    assert!(matches!(
        &expired.account_delta.predecessor,
        PlatformAccountDeltaPredecessorV1::AccountDelta {
            delta_id,
            delta_sha256,
            account_transition_seq: 1,
        } if delta_id == &lifecycle[0].account_delta.delta_id
            && delta_sha256 == &lifecycle[0].account_delta.delta_sha256
    ));
    assert!(
        lifecycle
            .iter()
            .all(|feedback| { feedback.event_kind != OrderLifecycleEventKindV1::CallerCanceled })
    );
    let responses = captures.cancel_order.lock().expect("撤单响应捕获锁");
    assert_eq!(responses.len(), 1);
    assert_eq!(
        responses[0].target.outcome,
        CancelTargetOutcomeV1::NotCanceledNoEffect(
            CancelNotCanceledReasonV1::NoCancellableRemainder,
        )
    );
    assert_eq!(session.available_pusd(), Decimal::new(1_000, 0));
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);
}

#[tokio::test(flavor = "current_thread")]
async fn task303_cancel_payload_sha_changes_when_its_only_target_change_is_delta_reference() {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
    let scenario = scenario();
    let left = completed_positive_cancel_with_foundation(
        "task303-hash-isolation",
        Decimal::TEN,
        Arc::clone(&rules),
        Arc::clone(&profile),
        Arc::clone(&scenario),
    );
    let right = completed_positive_cancel_with_foundation(
        "task303-hash-isolation",
        Decimal::new(20, 0),
        rules,
        profile,
        scenario,
    );
    let left = left
        .captures
        .cancel_order
        .lock()
        .expect("左侧撤单响应捕获锁")[0]
        .as_ref()
        .clone();
    let right = right
        .captures
        .cancel_order
        .lock()
        .expect("右侧撤单响应捕获锁")[0]
        .as_ref()
        .clone();
    let CancelTargetOutcomeV1::Canceled(left_measurement) = &left.target.outcome else {
        panic!("左侧必须正释放");
    };
    let CancelTargetOutcomeV1::Canceled(right_measurement) = &right.target.outcome else {
        panic!("右侧必须正释放");
    };
    let left_delta = left_measurement
        .account_delta
        .as_ref()
        .expect("左侧账户变化引用");
    let right_delta = right_measurement
        .account_delta
        .as_ref()
        .expect("右侧账户变化引用");
    assert_ne!(left_delta, right_delta);
    assert_ne!(left.metadata.payload_sha256, right.metadata.payload_sha256);

    let mut normalized_right = right.clone();
    normalized_right.metadata.payload_sha256 = left.metadata.payload_sha256.clone();
    normalized_right.metadata.private_feedback_sequence = left.metadata.private_feedback_sequence;
    let CancelTargetOutcomeV1::Canceled(measurement) = &mut normalized_right.target.outcome else {
        panic!("规范化右侧必须正释放");
    };
    measurement.account_delta = Some(left_delta.clone());
    assert_eq!(left, normalized_right);
}
