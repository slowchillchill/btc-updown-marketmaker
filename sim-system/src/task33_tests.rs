use std::sync::{Arc, Mutex};

use contracts::{PlatformAccountDeltaPredecessorV1, PlatformAccountDeltaReasonV1, SimCallerRoleV1};
use rust_decimal::Decimal;

use crate::execution::{
    CancelNotCanceledReasonV1, CancelOrderDirectResponseV1, CancelOrderItemV1,
    CancelOrderRequestV1, CancelTargetOutcomeV1, OrderDirectResponseV1, OrderLifecycleEventKindV1,
    OrderLifecycleFeedbackV1, OrderQueryResultV1, OrderRequestV1, PostOrderDirectResponseV1,
    PostOrderItemV1, PostOrderRequestV1, RequestContextV1, SimBuyOrderKindV1, SimBuyOrderV1,
    SimExecutionSessionV1, SimulatedOrderKnownStatusV1, SubmissionDeliveryDispositionV1,
    SubmissionDriveDispositionV1, SubmissionItemOutcomeV1, SubmissionMeasurementV1,
    SubmissionPlatformOrderStatusV1, SubmissionResponseAcceptanceV1, SubmissionResponseBridgeV1,
    SubmissionTerminalReasonV1,
};
use crate::market::MarketBucketApplyDispositionV1;
use crate::task21_tests::{asset, hidden_bucket, market};
use crate::task23_tests::{ask_transition, bid_transition, close_platform_at, ready_session};

#[derive(Clone, Default)]
struct Task33Captures {
    post_order: Arc<Mutex<Vec<Arc<PostOrderDirectResponseV1>>>>,
    cancel_order: Arc<Mutex<Vec<Arc<CancelOrderDirectResponseV1>>>>,
    order: Arc<Mutex<Vec<Arc<OrderDirectResponseV1>>>>,
    order_lifecycle_attempts: Arc<Mutex<Vec<Arc<OrderLifecycleFeedbackV1>>>>,
}

impl Task33Captures {
    fn bridge(&self, dust_full_once: bool) -> SubmissionResponseBridgeV1 {
        let post_order = Arc::clone(&self.post_order);
        let cancel_order = Arc::clone(&self.cancel_order);
        let order = Arc::clone(&self.order);
        let order_lifecycle_attempts = Arc::clone(&self.order_lifecycle_attempts);
        SubmissionResponseBridgeV1::new(
            move |response| {
                post_order
                    .lock()
                    .expect("任务 3.3 单笔提交响应捕获锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                cancel_order
                    .lock()
                    .expect("任务 3.3 单笔撤单响应捕获锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                order
                    .lock()
                    .expect("任务 3.3 单笔订单查询响应捕获锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |feedback| {
                let first_dust_attempt = {
                    let mut attempts = order_lifecycle_attempts
                        .lock()
                        .expect("任务 3.3 订单生命周期反馈捕获锁");
                    attempts.push(Arc::clone(&feedback));
                    feedback.event_kind == OrderLifecycleEventKindV1::VenueDustTerminal
                        && attempts
                            .iter()
                            .filter(|attempt| {
                                attempt.event_kind == OrderLifecycleEventKindV1::VenueDustTerminal
                            })
                            .count()
                            == 1
                };
                if dust_full_once && first_dust_attempt {
                    SubmissionResponseAcceptanceV1::Full
                } else {
                    SubmissionResponseAcceptanceV1::Accepted(
                        feedback.metadata.delivery_confirmation(),
                    )
                }
            },
            |feedback| {
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
            |feedback| {
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
        )
    }

    fn post_order_response(&self, request_id: &str) -> Arc<PostOrderDirectResponseV1> {
        self.post_order
            .lock()
            .expect("任务 3.3 单笔提交响应捕获锁")
            .iter()
            .find(|response| response.metadata.global_request_id == request_id)
            .cloned()
            .expect("任务 3.3 指定单笔提交响应")
    }

    fn platform_order_id(&self, request_id: &str) -> String {
        let response = self.post_order_response(request_id);
        let SubmissionItemOutcomeV1::Accepted(accepted) = &response.item.outcome else {
            panic!("任务 3.3 GTC 必须被接受");
        };
        accepted.simulated_platform_order_id.clone()
    }

    fn dust_attempts(&self) -> Vec<Arc<OrderLifecycleFeedbackV1>> {
        self.order_lifecycle_attempts
            .lock()
            .expect("任务 3.3 订单生命周期反馈捕获锁")
            .iter()
            .filter(|feedback| feedback.event_kind == OrderLifecycleEventKindV1::VenueDustTerminal)
            .cloned()
            .collect()
    }
}

fn decimal(value: &str) -> Decimal {
    value.parse().expect("任务 3.3 测试十进制")
}

fn request_context(
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
        caller_role: SimCallerRoleV1::ModelTrain,
        caller_instance_id: spec.caller_instance_id.clone(),
        global_request_id: request_id.to_owned(),
        caller_correlation_id: Some(format!("correlation-{request_id}")),
        caller_sequence,
        request_sent_at_utc_ms,
        model_visible_cursor: session.model_visible_cursor(),
    }
}

fn gtc_request(
    session: &SimExecutionSessionV1,
    request_id: &str,
    caller_sequence: u64,
    request_sent_at_utc_ms: i64,
    price: &str,
    size: &str,
    post_only: bool,
) -> PostOrderRequestV1 {
    PostOrderRequestV1 {
        context: request_context(session, request_id, caller_sequence, request_sent_at_utc_ms),
        idempotency_key: format!("idem-{request_id}"),
        item: PostOrderItemV1 {
            item_id: format!("item-{request_id}"),
            order: SimBuyOrderV1 {
                condition_id: market().condition_id,
                token_id: asset("1", 0, "Up").token_id,
                kind: SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: decimal(price),
                    size_shares: decimal(size),
                    post_only,
                },
            },
        },
    }
}

fn cancel_request(
    session: &SimExecutionSessionV1,
    request_id: &str,
    caller_sequence: u64,
    request_sent_at_utc_ms: i64,
    platform_order_id: &str,
) -> CancelOrderRequestV1 {
    CancelOrderRequestV1 {
        context: request_context(session, request_id, caller_sequence, request_sent_at_utc_ms),
        idempotency_key: format!("idem-{request_id}"),
        item: CancelOrderItemV1 {
            item_id: format!("item-{request_id}"),
            platform_order_id: platform_order_id.to_owned(),
        },
    }
}

fn drive_gtc(session: &mut SimExecutionSessionV1, request: PostOrderRequestV1) -> i64 {
    let registration = session.post_order(request).expect("任务 3.3 GTC 登记");
    let effective_at_utc_ms = registration.item.effective_processing_at_utc_ms;
    let position = session.platform_event_index_position();
    close_platform_at(session, effective_at_utc_ms, position);
    assert_eq!(
        session.drive_next_execution_action_through(effective_at_utc_ms),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    effective_at_utc_ms
}

fn drain_private(session: &mut SimExecutionSessionV1, cutoff_utc_ms: i64) {
    assert_eq!(
        session
            .drive_private_delivery_after_platform_through_for_tests(cutoff_utc_ms)
            .expect("任务 3.3 私有交付"),
        SubmissionDeliveryDispositionV1::Delivered
    );
}

fn assert_zero_nonprincipal_movement(feedback: &OrderLifecycleFeedbackV1) {
    let movement = &feedback.account_delta.balance_delta;
    assert_eq!(decimal(&movement.cash_pusd_delta.0), Decimal::ZERO);
    assert_eq!(
        decimal(&movement.immediate_taker_fee_encumbrance_pusd_delta.0),
        Decimal::ZERO
    );
    assert!(movement.outcome_balance_deltas.is_empty());
    assert_eq!(
        decimal(&movement.maker_rebate_receivable_pusd_delta.0),
        Decimal::ZERO
    );
    assert_eq!(
        decimal(&movement.taker_rebate_receivable_pusd_delta.0),
        Decimal::ZERO
    );
    assert_eq!(
        decimal(&movement.maker_rebate_locked_remainder_pusd_delta.0),
        Decimal::ZERO
    );
    assert_eq!(
        decimal(&movement.taker_rebate_locked_remainder_pusd_delta.0),
        Decimal::ZERO
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task33_partial_taker_with_sole_fifo_remainder_dusts_in_the_order_action() {
    let captures = Task33Captures::default();
    let mut session = ready_session(
        "task33-immediate-dust",
        decimal("10"),
        8,
        captures.bridge(false),
    )
    .await;
    let remove_external_bid = bid_transition(1_101, 1_201, 3, "0.4", "10", "0");
    let better_ask = ask_transition(1_101, 1_201, 4, "0.3", "0", "4.999");
    assert_eq!(
        session.apply_closed_market_event_bucket(hidden_bucket(
            1_101,
            4,
            &[remove_external_bid, better_ask],
        )),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );

    let revision_before = session.query_projection_revision();
    let request = gtc_request(
        &session,
        "task33-immediate-dust",
        1,
        1_200,
        "0.4",
        "5",
        false,
    );
    let effective_at_utc_ms = drive_gtc(&mut session, request);
    assert_eq!(session.simulated_order_count(), 1);
    assert_eq!(session.principal_encumbered_pusd(), decimal("1.4997"));
    assert_eq!(session.next_account_transition_seq(), 3);
    assert_eq!(session.query_projection_revision(), revision_before + 1);

    drain_private(&mut session, effective_at_utc_ms + 10_000);
    let response = captures.post_order_response("task33-immediate-dust");
    let SubmissionItemOutcomeV1::Accepted(accepted) = &response.item.outcome else {
        panic!("任务 3.3 立即 dust GTC 必须被接受");
    };
    assert_eq!(
        accepted.platform_order_status,
        SubmissionPlatformOrderStatusV1::VenueDustTerminal
    );
    assert_eq!(
        accepted.terminal_reason,
        Some(SubmissionTerminalReasonV1::VenueDustTerminal)
    );
    let SubmissionMeasurementV1::Base(measurement) = &accepted.measurement else {
        panic!("任务 3.3 立即 dust GTC 必须使用 base 计量");
    };
    assert_eq!(measurement.open_base_quantity_shares, Decimal::ZERO);
    assert_eq!(
        measurement.venue_dust_closed_base_quantity_shares,
        decimal("0.001")
    );
    assert_eq!(measurement.open_encumbered_quote_amount_pusd, Decimal::ZERO);
    assert_eq!(
        measurement.venue_dust_released_quote_amount_pusd,
        decimal("0.0004")
    );
    let platform_order_id = captures.platform_order_id("task33-immediate-dust");
    let order = session
        .simulated_order(&platform_order_id)
        .expect("任务 3.3 立即 dust 订单");
    assert_eq!(order.accepted_base_units, Some(5_000_000));
    assert_eq!(order.matched_base_units, 4_999_000);
    assert_eq!(order.pending_maker_depletion_base_units, 0);
    assert_eq!(order.open_base_units, 0);
    assert_eq!(order.venue_dust_closed_base_units, 1_000);
    assert_eq!(order.accepted_quote_units, 2_000_000);
    assert_eq!(order.matched_quote_units, 1_499_700);
    assert_eq!(order.released_quote_units, 499_900);
    assert_eq!(order.open_quote_units, 0);
    assert_eq!(order.venue_dust_released_quote_units, 400);
    assert_eq!(session.simulated_fill_count(&platform_order_id), 1);

    let dust_attempts = captures.dust_attempts();
    assert_eq!(dust_attempts.len(), 1);
    let dust = &dust_attempts[0];
    assert_eq!(dust.platform_order_id, platform_order_id);
    assert_eq!(
        dust.event_kind,
        OrderLifecycleEventKindV1::VenueDustTerminal
    );
    assert_eq!(
        dust.order_status.known,
        Some(SimulatedOrderKnownStatusV1::VenueDustTerminal)
    );
    assert_eq!(
        dust.venue_dust_closed_base_quantity_shares,
        decimal("0.001")
    );
    assert_eq!(
        dust.venue_dust_released_quote_amount_pusd,
        decimal("0.0004")
    );
    assert_eq!(
        dust.account_delta.reason,
        PlatformAccountDeltaReasonV1::VenueDustEncumbranceReleased
    );
    assert_eq!(dust.account_delta.account_transition_seq, 2);
    assert_eq!(
        dust.account_delta.platform_effective_at_utc_ms,
        effective_at_utc_ms
    );
    assert_eq!(
        decimal(&dust.account_delta.balance_delta.available_pusd_delta.0),
        decimal("0.0004")
    );
    assert_eq!(
        decimal(
            &dust
                .account_delta
                .balance_delta
                .principal_encumbrance_pusd_delta
                .0,
        ),
        decimal("-0.0004")
    );
    let accepted = captures
        .order_lifecycle_attempts
        .lock()
        .expect("任务 3.3 订单生命周期反馈捕获锁")
        .iter()
        .find(|feedback| feedback.event_kind == OrderLifecycleEventKindV1::OrderAccepted)
        .cloned()
        .expect("任务 3.3 订单接受反馈");
    assert!(matches!(
        &dust.account_delta.predecessor,
        PlatformAccountDeltaPredecessorV1::AccountDelta {
            delta_id,
            delta_sha256,
            account_transition_seq: 1,
        } if delta_id == &accepted.account_delta.delta_id
            && delta_sha256 == &accepted.account_delta.delta_sha256
    ));
    assert_zero_nonprincipal_movement(dust);

    let query = session
        .order(OrderRequestV1 {
            context: request_context(
                &session,
                "task33-immediate-order-query",
                2,
                effective_at_utc_ms + 10_001,
            ),
            platform_order_id: platform_order_id.clone(),
        })
        .expect("任务 3.3 dust 订单查询登记");
    let query_at = query.receipt.transit.platform_arrived_at_utc_ms;
    let position = session.platform_event_index_position();
    close_platform_at(&mut session, query_at, position);
    assert_eq!(
        session.drive_next_execution_action_through(query_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    drain_private(&mut session, query_at + 10_000);
    let query_responses = captures
        .order
        .lock()
        .expect("任务 3.3 单笔订单查询响应捕获锁");
    assert_eq!(query_responses.len(), 1);
    let OrderQueryResultV1::Found(projection) = &query_responses[0].result else {
        panic!("任务 3.3 dust 订单查询必须命中");
    };
    assert_eq!(
        projection.status.known,
        Some(SimulatedOrderKnownStatusV1::VenueDustTerminal)
    );
    assert_eq!(projection.open_base_quantity_shares, Decimal::ZERO);
    assert_eq!(
        projection.venue_dust_closed_base_quantity_shares,
        decimal("0.001")
    );
    assert_eq!(projection.open_encumbered_quote_amount_pusd, Decimal::ZERO);
    assert_eq!(
        projection.venue_dust_released_quote_amount_pusd,
        decimal("0.0004")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task33_real_fifo_transition_dusts_atomically_and_retries_without_duplication() {
    let captures = Task33Captures::default();
    let mut session =
        ready_session("task33-dpt-dust", decimal("10"), 8, captures.bridge(true)).await;
    let better_ask = ask_transition(1_101, 1_201, 3, "0.3", "0", "4.999");
    assert_eq!(
        session.apply_closed_market_event_bucket(hidden_bucket(1_101, 3, &[better_ask])),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let request = gtc_request(&session, "task33-dpt-dust", 1, 1_200, "0.4", "5", false);
    let effective_at_utc_ms = drive_gtc(&mut session, request);
    assert_eq!(session.principal_encumbered_pusd(), decimal("1.5001"));
    assert_eq!(session.next_account_transition_seq(), 2);
    let available_before = session.available_pusd();
    let fee_before = session.immediate_taker_fee_encumbered_pusd();
    let outcomes_before = session.outcome_balances_sha256().clone();
    let revision_before = session.query_projection_revision();

    let exhausts_external_front = bid_transition(
        effective_at_utc_ms + 1,
        effective_at_utc_ms + 101,
        4,
        "0.4",
        "10",
        "0",
    );
    let dust_bucket = hidden_bucket(effective_at_utc_ms + 1, 4, &[exhausts_external_front]);
    assert_eq!(
        session.apply_closed_market_event_bucket(dust_bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(session.principal_encumbered_pusd(), decimal("1.4997"));
    assert_eq!(
        session.available_pusd(),
        available_before + decimal("0.0004")
    );
    assert_eq!(session.immediate_taker_fee_encumbered_pusd(), fee_before);
    assert_eq!(session.outcome_balances_sha256(), &outcomes_before);
    assert_eq!(session.next_account_transition_seq(), 3);
    assert_eq!(session.query_projection_revision(), revision_before + 1);

    let account_after = session.account_summary_sha256().clone();
    let next_seq_after = session.next_account_transition_seq();
    let revision_after = session.query_projection_revision();
    let logical_watermark_after = session.logical_platform_driven_through_for_tests();
    assert_eq!(
        session.apply_closed_market_event_bucket(dust_bucket),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(session.account_summary_sha256(), &account_after);
    assert_eq!(session.next_account_transition_seq(), next_seq_after);
    assert_eq!(session.query_projection_revision(), revision_after);
    assert_eq!(
        session.logical_platform_driven_through_for_tests(),
        logical_watermark_after
    );

    let delivery_cutoff = effective_at_utc_ms + 10_000;
    assert_eq!(
        session
            .drive_private_delivery_after_platform_through_for_tests(delivery_cutoff)
            .expect("任务 3.3 dust 首次交付"),
        SubmissionDeliveryDispositionV1::Full
    );
    assert_eq!(
        session
            .drive_private_delivery_after_platform_through_for_tests(delivery_cutoff)
            .expect("任务 3.3 dust 重试交付"),
        SubmissionDeliveryDispositionV1::Delivered
    );

    let platform_order_id = captures.platform_order_id("task33-dpt-dust");
    let response = captures.post_order_response("task33-dpt-dust");
    let SubmissionItemOutcomeV1::Accepted(accepted) = &response.item.outcome else {
        panic!("任务 3.3 DPT dust GTC 必须被接受");
    };
    assert_eq!(
        accepted.platform_order_status,
        SubmissionPlatformOrderStatusV1::Live,
        "后续 DPT 不得改写已经密封的初始直接响应"
    );
    assert_eq!(accepted.terminal_reason, None);
    let SubmissionMeasurementV1::Base(measurement) = &accepted.measurement else {
        panic!("任务 3.3 DPT dust GTC 必须使用 base 计量");
    };
    assert_eq!(measurement.open_base_quantity_shares, decimal("0.001"));
    assert_eq!(
        measurement.venue_dust_closed_base_quantity_shares,
        Decimal::ZERO
    );
    assert_eq!(
        measurement.open_encumbered_quote_amount_pusd,
        decimal("0.0004")
    );
    assert_eq!(
        measurement.venue_dust_released_quote_amount_pusd,
        Decimal::ZERO
    );
    let order = session
        .simulated_order(&platform_order_id)
        .expect("任务 3.3 DPT dust 订单");
    assert_eq!(order.matched_base_units, 4_999_000);
    assert_eq!(order.pending_maker_depletion_base_units, 0);
    assert_eq!(order.open_base_units, 0);
    assert_eq!(order.venue_dust_closed_base_units, 1_000);
    assert_eq!(order.open_quote_units, 0);
    assert_eq!(order.venue_dust_released_quote_units, 400);
    assert_eq!(session.simulated_fill_count(&platform_order_id), 1);

    let dust_attempts = captures.dust_attempts();
    assert_eq!(dust_attempts.len(), 2);
    assert!(Arc::ptr_eq(&dust_attempts[0], &dust_attempts[1]));
    assert_eq!(dust_attempts[0], dust_attempts[1]);
    assert_eq!(
        dust_attempts[0].metadata.delivery_confirmation(),
        dust_attempts[1].metadata.delivery_confirmation()
    );
    assert_eq!(
        dust_attempts[0].account_delta.reason,
        PlatformAccountDeltaReasonV1::VenueDustEncumbranceReleased
    );
    assert_eq!(
        dust_attempts[0].venue_dust_closed_base_quantity_shares,
        decimal("0.001")
    );
    assert_eq!(
        dust_attempts[0].venue_dust_released_quote_amount_pusd,
        decimal("0.0004")
    );
    assert_eq!(
        decimal(
            &dust_attempts[0]
                .account_delta
                .balance_delta
                .available_pusd_delta
                .0,
        ),
        decimal("0.0004")
    );
    assert_eq!(
        decimal(
            &dust_attempts[0]
                .account_delta
                .balance_delta
                .principal_encumbrance_pusd_delta
                .0,
        ),
        decimal("-0.0004")
    );
    assert_zero_nonprincipal_movement(&dust_attempts[0]);

    let account_before_cancel = session.account_summary_sha256().clone();
    let next_seq_before_cancel = session.next_account_transition_seq();
    let request = cancel_request(
        &session,
        "task33-late-cancel",
        2,
        delivery_cutoff + 1,
        &platform_order_id,
    );
    let cancel = session
        .cancel_order(request)
        .expect("任务 3.3 迟到撤单登记");
    let cancel_at = cancel.item.effective_processing_at_utc_ms;
    let position = session.platform_event_index_position();
    close_platform_at(&mut session, cancel_at, position);
    assert_eq!(
        session.drive_next_execution_action_through(cancel_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    drain_private(&mut session, cancel_at + 10_000);
    assert_eq!(session.account_summary_sha256(), &account_before_cancel);
    assert_eq!(
        session.next_account_transition_seq(),
        next_seq_before_cancel
    );
    let cancel_responses = captures
        .cancel_order
        .lock()
        .expect("任务 3.3 单笔撤单响应捕获锁");
    assert_eq!(cancel_responses.len(), 1);
    assert_eq!(
        cancel_responses[0].target.outcome,
        CancelTargetOutcomeV1::NotCanceledNoEffect(
            CancelNotCanceledReasonV1::NoCancellableRemainder,
        )
    );
    drop(cancel_responses);
    let order = session
        .simulated_order(&platform_order_id)
        .expect("任务 3.3 迟到撤单后的 dust 订单");
    assert_eq!(order.venue_dust_closed_base_units, 1_000);
    assert_eq!(order.venue_dust_released_quote_units, 400);
    assert_eq!(order.canceled_base_units, 0);
    assert_eq!(captures.dust_attempts().len(), 2);
    assert!(
        captures
            .order_lifecycle_attempts
            .lock()
            .expect("任务 3.3 订单生命周期反馈捕获锁")
            .iter()
            .all(|feedback| feedback.event_kind != OrderLifecycleEventKindV1::CallerCanceled)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task33_external_small_quantity_never_creates_account_dust() {
    let external_captures = Task33Captures::default();
    let mut external_session = ready_session(
        "task33-external-small",
        decimal("10"),
        4,
        external_captures.bridge(false),
    )
    .await;
    let account_before = external_session.account_summary_sha256().clone();
    let external_small = bid_transition(1_101, 1_201, 3, "0.5", "0", "0.006");
    assert_eq!(
        external_session.apply_closed_market_event_bucket(hidden_bucket(
            1_101,
            3,
            &[external_small]
        )),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let external_disappears = bid_transition(1_102, 1_202, 4, "0.5", "0.006", "0");
    assert_eq!(
        external_session.apply_closed_market_event_bucket(hidden_bucket(
            1_102,
            4,
            &[external_disappears]
        )),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(external_session.simulated_order_count(), 0);
    assert_eq!(external_session.next_account_transition_seq(), 1);
    assert_eq!(external_session.account_summary_sha256(), &account_before);
    assert!(external_captures.dust_attempts().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn task33_exact_upper_boundary_and_other_account_segment_block_dust() {
    let boundary_captures = Task33Captures::default();
    let mut boundary_session = ready_session(
        "task33-boundary",
        decimal("10"),
        4,
        boundary_captures.bridge(false),
    )
    .await;
    let remove_external_bid = bid_transition(1_101, 1_201, 3, "0.4", "10", "0");
    let better_ask = ask_transition(1_101, 1_201, 4, "0.3", "0", "4.99");
    assert_eq!(
        boundary_session.apply_closed_market_event_bucket(hidden_bucket(
            1_101,
            4,
            &[remove_external_bid, better_ask],
        )),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let request = gtc_request(
        &boundary_session,
        "task33-boundary",
        1,
        1_200,
        "0.4",
        "5",
        false,
    );
    let boundary_effective = drive_gtc(&mut boundary_session, request);
    assert_eq!(boundary_session.next_account_transition_seq(), 2);
    assert_eq!(
        boundary_session.principal_encumbered_pusd(),
        decimal("1.501")
    );
    drain_private(&mut boundary_session, boundary_effective + 10_000);
    let boundary_order_id = boundary_captures.platform_order_id("task33-boundary");
    let boundary_order = boundary_session
        .simulated_order(&boundary_order_id)
        .expect("任务 3.3 严格上界订单");
    assert_eq!(boundary_order.matched_base_units, 4_990_000);
    assert_eq!(boundary_order.open_base_units, 10_000);
    assert_eq!(boundary_order.venue_dust_closed_base_units, 0);
    assert_eq!(boundary_order.venue_dust_released_quote_units, 0);
    assert!(boundary_captures.dust_attempts().is_empty());

    let ownership_captures = Task33Captures::default();
    let mut ownership_session = ready_session(
        "task33-other-account-segment",
        decimal("10"),
        8,
        ownership_captures.bridge(false),
    )
    .await;
    let remove_external_bid = bid_transition(1_101, 1_201, 3, "0.4", "10", "0");
    assert_eq!(
        ownership_session.apply_closed_market_event_bucket(hidden_bucket(
            1_101,
            3,
            &[remove_external_bid]
        )),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let request = gtc_request(
        &ownership_session,
        "task33-guard-order",
        1,
        1_200,
        "0.4",
        "5",
        true,
    );
    let guard_registration = ownership_session
        .post_order(request)
        .expect("任务 3.3 guard GTC 登记");
    let guard_effective = guard_registration.item.effective_processing_at_utc_ms;
    let position = ownership_session.platform_event_index_position();
    close_platform_at(&mut ownership_session, guard_effective, position);
    assert_eq!(
        ownership_session.drive_next_execution_action_through(guard_effective),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    let guard_response_at = guard_effective
        + i64::try_from(guard_registration.receipt.transit.inbound_delay_ms)
            .expect("任务 3.3 guard 响应时延适合 i64");
    drain_private(&mut ownership_session, guard_response_at);
    let guard_order_id = ownership_captures.platform_order_id("task33-guard-order");
    let better_ask = ask_transition(
        guard_response_at + 1,
        guard_response_at + 101,
        4,
        "0.3",
        "0",
        "4.999",
    );
    assert_eq!(
        ownership_session.apply_closed_market_event_bucket(hidden_bucket(
            guard_response_at + 1,
            4,
            &[better_ask],
        )),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let request = gtc_request(
        &ownership_session,
        "task33-owned-target",
        2,
        guard_response_at + 2,
        "0.4",
        "5",
        false,
    );
    let target_effective = drive_gtc(&mut ownership_session, request);
    assert_eq!(ownership_session.next_account_transition_seq(), 3);
    assert_eq!(
        ownership_session.account_overlay_quantity(
            &market().condition_id.0,
            "1",
            contracts::BookSide::Bid,
            decimal("0.4"),
        ),
        decimal("5.001")
    );

    let cancel = cancel_request(
        &ownership_session,
        "task33-remove-guard",
        3,
        target_effective + 1,
        &guard_order_id,
    );
    let cancel_registration = ownership_session
        .cancel_order(cancel)
        .expect("任务 3.3 guard 撤单登记");
    let cancel_at = cancel_registration.item.effective_processing_at_utc_ms;
    let position = ownership_session.platform_event_index_position();
    close_platform_at(&mut ownership_session, cancel_at, position);
    assert_eq!(
        ownership_session.drive_next_execution_action_through(cancel_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    assert_eq!(
        ownership_session.account_overlay_quantity(
            &market().condition_id.0,
            "1",
            contracts::BookSide::Bid,
            decimal("0.4"),
        ),
        decimal("0.001"),
        "非 Level 的 guard 撤单只留下 dust 候选，不得自行触发 dust"
    );
    let account_before_echo = ownership_session.account_summary_sha256().clone();
    let next_seq_before_echo = ownership_session.next_account_transition_seq();
    let revision_before_echo = ownership_session.query_projection_revision();
    let fee_before_echo = ownership_session.immediate_taker_fee_encumbered_pusd();
    let outcomes_before_echo = ownership_session.outcome_balances_sha256().clone();
    let pure_debt_echo = ask_transition(cancel_at + 1, cancel_at + 101, 5, "0.3", "4.999", "0");
    assert_eq!(
        ownership_session.apply_closed_market_event_bucket(hidden_bucket(
            cancel_at + 1,
            5,
            &[pure_debt_echo],
        )),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(
        ownership_session.account_summary_sha256(),
        &account_before_echo
    );
    assert_eq!(
        ownership_session.next_account_transition_seq(),
        next_seq_before_echo
    );
    assert_eq!(
        ownership_session.query_projection_revision(),
        revision_before_echo
    );
    assert_eq!(
        ownership_session.immediate_taker_fee_encumbered_pusd(),
        fee_before_echo
    );
    assert_eq!(
        ownership_session.outcome_balances_sha256(),
        &outcomes_before_echo
    );
    assert_eq!(
        ownership_session.account_overlay_quantity(
            &market().condition_id.0,
            "1",
            contracts::BookSide::Bid,
            decimal("0.4"),
        ),
        decimal("0.001")
    );

    drain_private(&mut ownership_session, cancel_at + 10_000);
    let target_order_id = ownership_captures.platform_order_id("task33-owned-target");
    let guard_order = ownership_session
        .simulated_order(&guard_order_id)
        .expect("任务 3.3 前方其他账户订单段");
    let target_order = ownership_session
        .simulated_order(&target_order_id)
        .expect("任务 3.3 纯 debt 回声后的候选");
    assert_eq!(guard_order.open_base_units, 0);
    assert_eq!(guard_order.canceled_base_units, 5_000_000);
    assert_eq!(target_order.matched_base_units, 4_999_000);
    assert_eq!(target_order.open_base_units, 1_000);
    assert_eq!(target_order.venue_dust_closed_base_units, 0);
    assert_eq!(target_order.venue_dust_released_quote_units, 0);
    assert_eq!(ownership_session.simulated_fill_count(&target_order_id), 1);
    assert!(ownership_captures.dust_attempts().is_empty());
}
