use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use contracts::{
    ExactDecimal, ExecutionMarketEventPayloadV3, ExecutionMarketEventV3,
    MarketResolutionTerminalV3, PayoutEntryV1,
};
use data_pipeline_core::{derive_execution_market_event_v3_id, encode_execution_market_event_v3};
use rust_decimal::Decimal;

use crate::execution::{
    CancelAllOrdersDirectResponseV1, CancelOrderDirectResponseV1, CancelOrderItemV1,
    CancelOrderRequestV1, CancelOrdersDirectResponseV1, CancelTargetOutcomeV1,
    OrderDirectResponseV1, OrderLifecycleFeedbackV1, OrderQueryResultV1, OrderRequestV1,
    OrdersDirectResponseV1, OrdersFilterV1, OrdersQueryResultV1, OrdersRequestV1,
    PostOrderDirectResponseV1, PostOrderItemV1, PostOrderRequestV1, PostOrdersDirectResponseV1,
    PostOrdersRequestV1, ResponseDeliveryConfirmationV1, SettlementLifecycleFeedbackV1,
    SimBuyOrderKindV1, SimBuyOrderV1, SimCallerRoleV1, SimExecutionProcessV1,
    SimExecutionSessionV1, SimulatedOrderKnownStatusV1, SimulatedTradeKnownStatusV1,
    SubmissionDeliveryDispositionV1, SubmissionDriveDispositionV1, SubmissionEffectDispositionV1,
    SubmissionItemOutcomeV1, SubmissionProcessingErrorV1, SubmissionResponseAcceptanceV1,
    SubmissionResponseBridgeV1, TradeLifecycleFeedbackV1, TradesDirectResponseV1, TradesFilterV1,
    TradesQueryResultV1, TradesRequestV1, execution_process_for_tests,
};
use crate::finality::{TradeFinalityStateV1, policy_for_tests};
use crate::latency_profile::{EmpiricalDelayProfile, load_embedded_proxy_latency_profile};
use crate::market::{
    ClosedMarketEventBucketV1, MarketBucketApplyDispositionV1, MarketEventErrorV1,
    MarketEventPathV1, MarketLifecyclePhaseV1, MillisecondBucketClosureV1,
};
use crate::platform_rules::tests::successful_snapshot_for_sim_tests;
use crate::session::{InputPathSealOutcomeV1, RequestRegistrationDispositionV1, SessionError};
use crate::task21_tests::{
    asset, fresh_quality, market as short_market, orderbook_evidence, scenario as short_scenario,
    sealed_event, session_spec as short_session_spec,
};
use crate::task22_tests::{
    asset as long_asset, context_for_arrival, load_initial_market as load_long_initial_market,
    market as long_market, scenario as long_scenario, session_spec as long_session_spec,
};
use crate::task23_tests::{bid_transition, session_with_one_pending_maker_depletion_with_bridge};

const LONG_MARKET_END_AT_UTC_MS: i64 = 500_000;
const RESOLUTION_AT_UTC_MS: i64 = 510_000;
const PENDING_RESOLUTION_AT_UTC_MS: i64 = 550_000;

#[derive(Clone, Default)]
struct Task41Captures {
    post_order: Arc<Mutex<Vec<Arc<PostOrderDirectResponseV1>>>>,
    post_orders: Arc<Mutex<Vec<Arc<PostOrdersDirectResponseV1>>>>,
    cancel_order: Arc<Mutex<Vec<Arc<CancelOrderDirectResponseV1>>>>,
    cancel_orders: Arc<Mutex<Vec<Arc<CancelOrdersDirectResponseV1>>>>,
    cancel_all_orders: Arc<Mutex<Vec<Arc<CancelAllOrdersDirectResponseV1>>>>,
    order: Arc<Mutex<Vec<Arc<OrderDirectResponseV1>>>>,
    orders: Arc<Mutex<Vec<Arc<OrdersDirectResponseV1>>>>,
    trades: Arc<Mutex<Vec<Arc<TradesDirectResponseV1>>>>,
    order_lifecycle: Arc<Mutex<Vec<Arc<OrderLifecycleFeedbackV1>>>>,
    trade_lifecycle: Arc<Mutex<Vec<Arc<TradeLifecycleFeedbackV1>>>>,
    settlement_lifecycle: Arc<Mutex<Vec<Arc<SettlementLifecycleFeedbackV1>>>>,
}

impl Task41Captures {
    fn bridge(&self) -> SubmissionResponseBridgeV1 {
        let post_order = Arc::clone(&self.post_order);
        let post_orders = Arc::clone(&self.post_orders);
        let cancel_order = Arc::clone(&self.cancel_order);
        let cancel_orders = Arc::clone(&self.cancel_orders);
        let cancel_all_orders = Arc::clone(&self.cancel_all_orders);
        let order = Arc::clone(&self.order);
        let orders = Arc::clone(&self.orders);
        let trades = Arc::clone(&self.trades);
        let order_lifecycle = Arc::clone(&self.order_lifecycle);
        let trade_lifecycle = Arc::clone(&self.trade_lifecycle);
        let settlement_lifecycle = Arc::clone(&self.settlement_lifecycle);
        SubmissionResponseBridgeV1::new(
            move |response| {
                post_order
                    .lock()
                    .expect("任务 4.1 单笔提交响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                post_orders
                    .lock()
                    .expect("任务 4.1 批量提交响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                cancel_order
                    .lock()
                    .expect("任务 4.1 单笔撤单响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                cancel_orders
                    .lock()
                    .expect("任务 4.1 批量撤单响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                cancel_all_orders
                    .lock()
                    .expect("任务 4.1 全撤响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                order
                    .lock()
                    .expect("任务 4.1 单订单查询响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                orders
                    .lock()
                    .expect("任务 4.1 订单列表响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                trades
                    .lock()
                    .expect("任务 4.1 成交列表响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |feedback| {
                order_lifecycle
                    .lock()
                    .expect("任务 4.1 订单生命周期反馈锁")
                    .push(Arc::clone(&feedback));
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
            move |feedback| {
                trade_lifecycle
                    .lock()
                    .expect("任务 4.1 成交生命周期反馈锁")
                    .push(Arc::clone(&feedback));
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
            move |feedback| {
                settlement_lifecycle
                    .lock()
                    .expect("任务 4.1 结算生命周期反馈锁")
                    .push(Arc::clone(&feedback));
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
        )
    }

    fn delivery_counts(&self) -> [usize; 11] {
        [
            self.post_order.lock().expect("单笔提交响应锁").len(),
            self.post_orders.lock().expect("批量提交响应锁").len(),
            self.cancel_order.lock().expect("单笔撤单响应锁").len(),
            self.cancel_orders.lock().expect("批量撤单响应锁").len(),
            self.cancel_all_orders.lock().expect("全撤响应锁").len(),
            self.order.lock().expect("单订单查询响应锁").len(),
            self.orders.lock().expect("订单列表响应锁").len(),
            self.trades.lock().expect("成交列表响应锁").len(),
            self.order_lifecycle
                .lock()
                .expect("订单生命周期反馈锁")
                .len(),
            self.trade_lifecycle
                .lock()
                .expect("成交生命周期反馈锁")
                .len(),
            self.settlement_lifecycle
                .lock()
                .expect("结算生命周期反馈锁")
                .len(),
        ]
    }

    fn direct_confirmations(&self) -> Vec<ResponseDeliveryConfirmationV1> {
        let mut confirmations = Vec::new();
        confirmations.extend(
            self.post_order
                .lock()
                .expect("单笔提交响应锁")
                .iter()
                .map(|response| response.metadata.delivery_confirmation()),
        );
        confirmations.extend(
            self.post_orders
                .lock()
                .expect("批量提交响应锁")
                .iter()
                .map(|response| response.metadata.delivery_confirmation()),
        );
        confirmations.extend(
            self.cancel_order
                .lock()
                .expect("单笔撤单响应锁")
                .iter()
                .map(|response| response.metadata.delivery_confirmation()),
        );
        confirmations.extend(
            self.cancel_orders
                .lock()
                .expect("批量撤单响应锁")
                .iter()
                .map(|response| response.metadata.delivery_confirmation()),
        );
        confirmations.extend(
            self.cancel_all_orders
                .lock()
                .expect("全撤响应锁")
                .iter()
                .map(|response| response.metadata.delivery_confirmation()),
        );
        confirmations.extend(
            self.order
                .lock()
                .expect("单订单查询响应锁")
                .iter()
                .map(|response| response.metadata.delivery_confirmation()),
        );
        confirmations.extend(
            self.orders
                .lock()
                .expect("订单列表响应锁")
                .iter()
                .map(|response| response.metadata.delivery_confirmation()),
        );
        confirmations.extend(
            self.trades
                .lock()
                .expect("成交列表响应锁")
                .iter()
                .map(|response| response.metadata.delivery_confirmation()),
        );
        sort_confirmations(confirmations)
    }

    fn lifecycle_confirmations(&self) -> Vec<ResponseDeliveryConfirmationV1> {
        let mut confirmations = Vec::new();
        confirmations.extend(
            self.order_lifecycle
                .lock()
                .expect("订单生命周期反馈锁")
                .iter()
                .map(|feedback| feedback.metadata.delivery_confirmation()),
        );
        confirmations.extend(
            self.trade_lifecycle
                .lock()
                .expect("成交生命周期反馈锁")
                .iter()
                .map(|feedback| feedback.metadata.delivery_confirmation()),
        );
        confirmations.extend(
            self.settlement_lifecycle
                .lock()
                .expect("结算生命周期反馈锁")
                .iter()
                .map(|feedback| feedback.metadata.delivery_confirmation()),
        );
        sort_confirmations(confirmations)
    }
}

fn sort_confirmations(
    mut confirmations: Vec<ResponseDeliveryConfirmationV1>,
) -> Vec<ResponseDeliveryConfirmationV1> {
    confirmations.sort_by(|left, right| {
        (left.private_feedback_sequence, left.delivery_id.as_str())
            .cmp(&(right.private_feedback_sequence, right.delivery_id.as_str()))
    });
    confirmations
}

fn decimal(value: &str) -> Decimal {
    value.parse().expect("任务 4.1 测试十进制")
}

fn outcome_balance(snapshot: &contracts::PlatformAccountSnapshotV1, token_id: &str) -> Decimal {
    snapshot
        .outcome_balances
        .iter()
        .find(|balance| balance.token_id.0 == token_id)
        .map(|balance| decimal(&balance.balance_shares.0))
        .unwrap_or(Decimal::ZERO)
}

fn hidden_bucket(
    session: &SimExecutionSessionV1,
    at_utc_ms: i64,
    closure: MillisecondBucketClosureV1,
    events: &[ExecutionMarketEventV3],
) -> ClosedMarketEventBucketV1 {
    let start = session.platform_event_index_position();
    let next = start + u64::try_from(events.len()).expect("任务 4.1 事件数适合 u64");
    ClosedMarketEventBucketV1::hidden(
        at_utc_ms,
        start,
        next,
        closure,
        events
            .iter()
            .map(|event| encode_execution_market_event_v3(event).expect("任务 4.1 规范事件编码"))
            .collect::<Vec<_>>(),
    )
}

fn apply_empty_at(
    session: &mut SimExecutionSessionV1,
    at_utc_ms: i64,
) -> MarketBucketApplyDispositionV1 {
    session
        .apply_closed_market_event_bucket(hidden_bucket(
            session,
            at_utc_ms,
            MillisecondBucketClosureV1::NextEventAt(at_utc_ms + 1),
            &[],
        ))
        .expect("任务 4.1 空平台桶")
}

fn response_received_at(
    receipt: &crate::execution::RequestRegistrationReceiptV1,
    platform_responded_at_utc_ms: i64,
) -> i64 {
    platform_responded_at_utc_ms
        + i64::try_from(receipt.transit.inbound_delay_ms).expect("任务 4.1 入站时延适合 i64")
}

fn drive_one(
    session: &mut SimExecutionSessionV1,
    effective_at_utc_ms: i64,
    receipt: &crate::execution::RequestRegistrationReceiptV1,
) {
    assert_eq!(
        session.drive_next_execution_action_through(effective_at_utc_ms),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    drain_accepting(session, response_received_at(receipt, effective_at_utc_ms));
}

fn drain_accepting(session: &mut SimExecutionSessionV1, through_utc_ms: i64) {
    for _ in 0..256 {
        match session
            .drive_private_delivery_after_platform_through_for_tests(through_utc_ms)
            .expect("任务 4.1 私有事实交付")
        {
            SubmissionDeliveryDispositionV1::Delivered => continue,
            SubmissionDeliveryDispositionV1::NoVisibleResponse => return,
            disposition => panic!("任务 4.1 接受桥不应返回 {disposition:?}"),
        }
    }
    panic!("任务 4.1 私有事实未在有界次数内排空");
}

fn post_order_request(
    session: &SimExecutionSessionV1,
    profile: &EmpiricalDelayProfile,
    request_id: &str,
    caller_sequence: u64,
    desired_arrival_at_utc_ms: i64,
    order: SimBuyOrderV1,
) -> PostOrderRequestV1 {
    PostOrderRequestV1 {
        context: context_for_arrival(
            session,
            profile,
            request_id,
            caller_sequence,
            desired_arrival_at_utc_ms,
        ),
        idempotency_key: format!("idem-{request_id}"),
        item: PostOrderItemV1 {
            item_id: format!("item-{request_id}"),
            order,
        },
    }
}

fn fak_request(
    session: &SimExecutionSessionV1,
    profile: &EmpiricalDelayProfile,
    request_id: &str,
    caller_sequence: u64,
    desired_arrival_at_utc_ms: i64,
) -> PostOrderRequestV1 {
    PostOrderRequestV1 {
        context: context_for_arrival(
            session,
            profile,
            request_id,
            caller_sequence,
            desired_arrival_at_utc_ms,
        ),
        idempotency_key: format!("idem-{request_id}"),
        item: PostOrderItemV1 {
            item_id: format!("item-{request_id}"),
            order: SimBuyOrderV1 {
                condition_id: long_market().condition_id,
                token_id: long_asset("1", 0, "Up").token_id,
                kind: SimBuyOrderKindV1::FakMarket {
                    spend_pusd: decimal("3"),
                    worst_price_pusd_per_share: decimal("0.6"),
                },
            },
        },
    }
}

fn post_one(
    session: &mut SimExecutionSessionV1,
    captures: &Task41Captures,
    request: PostOrderRequestV1,
) -> Arc<PostOrderDirectResponseV1> {
    let request_id = request.context.global_request_id.clone();
    let before = captures
        .post_order
        .lock()
        .expect("任务 4.1 单笔提交响应锁")
        .len();
    let registration = session.post_order(request).expect("任务 4.1 单笔提交登记");
    let effective_at = registration.item.effective_processing_at_utc_ms;
    assert_eq!(
        apply_empty_at(session, effective_at),
        MarketBucketApplyDispositionV1::Applied
    );
    drive_one(session, effective_at, &registration.receipt);
    let responses = captures.post_order.lock().expect("任务 4.1 单笔提交响应锁");
    let response = responses.get(before).expect("任务 4.1 单笔提交直接响应");
    assert_eq!(response.metadata.global_request_id, request_id);
    Arc::clone(response)
}

fn resolution_event(
    market: &contracts::MarketIdentityV2,
    source_at_utc_ms: i64,
    received_at_utc_ms: i64,
    sequence: u64,
) -> ExecutionMarketEventV3 {
    let mut event = sealed_event(
        ExecutionMarketEventPayloadV3::MarketResolutionTerminal(
            MarketResolutionTerminalV3::Resolved {
                winning_asset_id: "1".to_owned(),
                winning_outcome: "Up".to_owned(),
                payout_vector: vec![
                    PayoutEntryV1 {
                        asset: asset("1", 0, "Up"),
                        payout_pusd_per_share: ExactDecimal("1".to_owned()),
                    },
                    PayoutEntryV1 {
                        asset: asset("2", 1, "Down"),
                        payout_pusd_per_share: ExactDecimal("0".to_owned()),
                    },
                ],
                mapping_rule_version: "task41-resolution-v1".to_owned(),
            },
        ),
        orderbook_evidence(source_at_utc_ms, received_at_utc_ms, sequence),
        fresh_quality(),
    );
    event.market = market.clone();
    event.event_id = derive_execution_market_event_v3_id(&event).expect("任务 4.1 判定事件身份");
    event
}

async fn long_session(
    session_id: &str,
    captures: &Task41Captures,
    finality_policy: Arc<crate::finality::TradeFinalityPolicyV1>,
) -> (SimExecutionSessionV1, Arc<EmpiricalDelayProfile>) {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("任务 4.1 代理时延配置档");
    let scenario = long_scenario();
    let mut spec = long_session_spec(&scenario, SimCallerRoleV1::ModelTrain, session_id);
    spec.response_outbox_capacity = NonZeroUsize::new(64).expect("任务 4.1 正响应槽容量");
    let mut process = SimExecutionProcessV1::try_new(
        "component-test-run".to_owned(),
        finality_policy,
        rules,
        Arc::clone(&profile),
        scenario,
    )
    .expect("任务 4.1 运行根");
    let mut session = process.create_active_session_for_tests(spec, captures.bridge());
    load_long_initial_market(&mut session, "0.01", "0.6", "1000");
    (session, profile)
}

async fn unloaded_short_session(
    session_id: &str,
    bridge: SubmissionResponseBridgeV1,
) -> SimExecutionSessionV1 {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("任务 4.1 代理时延配置档");
    let scenario = short_scenario();
    execution_process_for_tests(rules, profile, Arc::clone(&scenario))
        .create_active_session_for_tests(
            short_session_spec(&scenario, SimCallerRoleV1::ModelTrain, session_id),
            bridge,
        )
}

#[tokio::test(flavor = "current_thread")]
async fn task411_end_at_releases_both_external_matching_layers_but_keeps_open_order_operable() {
    let captures = Task41Captures::default();
    let post_capture = Arc::clone(&captures.post_order);
    let (mut session, platform_order_id, _, _, _, _) =
        session_with_one_pending_maker_depletion_with_bridge(
            "task411-end-release",
            post_capture,
            captures.bridge(),
            100,
        )
        .await;
    let profile = load_embedded_proxy_latency_profile().expect("任务 4.1 代理时延配置档");
    let condition_id = short_market().condition_id.0;
    let end_at = short_market().window.end_at_utc_ms;
    let debt_source = crate::task23_tests::fak_request(
        &session,
        "task411-queue-debt-source",
        2,
        1_400,
        "3",
        "0.6",
    );
    let debt_response = post_one(&mut session, &captures, debt_source);
    assert!(matches!(
        &debt_response.item.outcome,
        SubmissionItemOutcomeV1::Accepted(_)
    ));
    drain_accepting(&mut session, end_at - 1);

    let before = session.task41_diagnostics_for_tests(&condition_id);
    assert!(before.market_external_book_level_count > 0);
    assert!(before.execution_queue_level_count > 0);
    assert!(before.execution_queue_segment_count > 0);
    assert!(before.execution_queue_debt_base_units > 0);
    let order_before = session
        .simulated_order(&platform_order_id)
        .expect("任务 4.1 盘前开放订单")
        .clone();
    let account_before = session.account_snapshot_for_tests();
    let available_before = session.available_pusd();
    let principal_before = session.principal_encumbered_pusd();
    let revision_before = session.query_projection_revision();

    let end_at_l2 = bid_transition(end_at, end_at + 100, 6, "0.00001", "0", "0.1");
    assert_eq!(
        session.apply_closed_market_event_bucket(hidden_bucket(
            &session,
            end_at,
            MillisecondBucketClosureV1::NextEventAt(end_at + 1),
            &[end_at_l2],
        )),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let after_end = session.task41_diagnostics_for_tests(&condition_id);
    assert_eq!(after_end.market_external_book_level_count, 0);
    assert_eq!(after_end.execution_queue_level_count, 0);
    assert_eq!(after_end.execution_queue_segment_count, 0);
    assert_eq!(after_end.execution_queue_debt_base_units, 0);
    assert_eq!(after_end.input_seal, None);
    assert_eq!(
        session
            .platform_market_state(&condition_id)
            .expect("任务 4.1 盘后市场")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::PostEndPendingResolution
    );
    assert_eq!(
        session.simulated_order(&platform_order_id),
        Some(&order_before)
    );
    assert_eq!(session.account_snapshot_for_tests(), account_before);
    assert_eq!(session.available_pusd(), available_before);
    assert_eq!(session.principal_encumbered_pusd(), principal_before);
    assert_eq!(session.query_projection_revision(), revision_before);

    let post_end_l2 = bid_transition(end_at + 1, end_at + 101, 7, "0.00001", "0", "0.2");
    assert_eq!(
        session.apply_closed_market_event_bucket(hidden_bucket(
            &session,
            end_at + 1,
            MillisecondBucketClosureV1::NextEventAt(end_at + 2),
            &[post_end_l2],
        )),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let after_post_end_l2 = session.task41_diagnostics_for_tests(&condition_id);
    assert_eq!(after_post_end_l2.market_external_book_level_count, 0);
    assert_eq!(after_post_end_l2.execution_queue_level_count, 0);
    assert_eq!(after_post_end_l2.execution_queue_segment_count, 0);
    assert_eq!(after_post_end_l2.execution_queue_debt_base_units, 0);
    assert_eq!(
        session.simulated_order(&platform_order_id),
        Some(&order_before)
    );
    assert_eq!(session.account_snapshot_for_tests(), account_before);
    assert_eq!(session.query_projection_revision(), revision_before);

    let query = session
        .order(OrderRequestV1 {
            context: context_for_arrival(
                &session,
                &profile,
                "task411-open-order-query",
                3,
                end_at + 10,
            ),
            platform_order_id: platform_order_id.clone(),
        })
        .expect("任务 4.1 盘后开放订单查询登记");
    let query_at = query.receipt.transit.platform_arrived_at_utc_ms;
    assert_eq!(
        apply_empty_at(&mut session, query_at),
        MarketBucketApplyDispositionV1::Applied
    );
    drive_one(&mut session, query_at, &query.receipt);
    let query_response = captures
        .order
        .lock()
        .expect("任务 4.1 单订单查询响应锁")
        .last()
        .cloned()
        .expect("任务 4.1 盘后单订单查询响应");
    let OrderQueryResultV1::Found(found) = &query_response.result else {
        panic!("任务 4.1 盘后开放订单必须仍可查");
    };
    assert_eq!(found.platform_order_id, platform_order_id);
    assert_eq!(found.status.known, Some(SimulatedOrderKnownStatusV1::Live));
    assert_eq!(found.open_encumbered_quote_amount_pusd, principal_before);

    let cancel_sent_at = response_received_at(&query.receipt, query_at) + 1;
    let provisional_cancel_context = context_for_arrival(
        &session,
        &profile,
        "task411-open-order-cancel",
        4,
        cancel_sent_at,
    );
    let cancel_outbound_delay = cancel_sent_at
        .checked_sub(provisional_cancel_context.request_sent_at_utc_ms)
        .expect("任务 4.1 撤单出站时延");
    let cancel_arrival_at = cancel_sent_at
        .checked_add(cancel_outbound_delay)
        .expect("任务 4.1 撤单到达时点");
    let cancel = session
        .cancel_order(CancelOrderRequestV1 {
            context: context_for_arrival(
                &session,
                &profile,
                "task411-open-order-cancel",
                4,
                cancel_arrival_at,
            ),
            idempotency_key: "task411-open-order-cancel-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "task411-open-order-cancel-item".to_owned(),
                platform_order_id: platform_order_id.clone(),
            },
        })
        .expect("任务 4.1 盘后开放订单撤单登记");
    let cancel_at = cancel.item.effective_processing_at_utc_ms;
    assert_eq!(
        apply_empty_at(&mut session, cancel_at),
        MarketBucketApplyDispositionV1::Applied
    );
    drive_one(&mut session, cancel_at, &cancel.receipt);
    let cancel_response = captures
        .cancel_order
        .lock()
        .expect("任务 4.1 单笔撤单响应锁")
        .last()
        .cloned()
        .expect("任务 4.1 盘后撤单响应");
    assert!(matches!(
        &cancel_response.target.outcome,
        CancelTargetOutcomeV1::Canceled(_)
    ));
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);
    assert_eq!(
        session.available_pusd(),
        available_before + principal_before
    );
    assert_eq!(
        session
            .simulated_order(&platform_order_id)
            .expect("任务 4.1 撤单后订单")
            .open_quote_units,
        0
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task412_resolution_compacts_equivalent_queries_and_retention_prunes_expired_sources() {
    let captures = Task41Captures::default();
    let (mut session, profile) = long_session(
        "task412-compact-retention",
        &captures,
        crate::finality::successful_policy_for_tests(),
    )
    .await;
    let condition_id = long_market().condition_id.0;

    let fak_request = fak_request(&session, &profile, "task412-fak", 1, 200_000);
    let fak = post_one(&mut session, &captures, fak_request);
    let SubmissionItemOutcomeV1::Accepted(fak_accepted) = &fak.item.outcome else {
        panic!("任务 4.1 成交来源 FAK 必须接受");
    };
    let trade_id = fak_accepted
        .fills
        .first()
        .expect("任务 4.1 FAK 必须形成成交")
        .simulated_trade_id
        .clone();

    let original_gtc_request = post_order_request(
        &session,
        &profile,
        "task412-original-gtc",
        2,
        210_000,
        SimBuyOrderV1 {
            condition_id: long_market().condition_id,
            token_id: long_asset("1", 0, "Up").token_id,
            kind: SimBuyOrderKindV1::GtcLimit {
                limit_price_pusd_per_share: decimal("0.3"),
                size_shares: decimal("5"),
                post_only: true,
            },
        },
    );
    let original_gtc = post_one(&mut session, &captures, original_gtc_request.clone());
    let SubmissionItemOutcomeV1::Accepted(original_gtc_accepted) = &original_gtc.item.outcome
    else {
        panic!("任务 4.1 原始 GTC 必须接受");
    };
    let exact_order_id = original_gtc_accepted.simulated_platform_order_id.clone();

    for batch in 0..7_u64 {
        let item_count = if batch == 6 { 10 } else { 15 };
        let request_id = format!("task412-page-source-{batch}");
        let before = captures
            .post_orders
            .lock()
            .expect("任务 4.1 批量提交响应锁")
            .len();
        let items = (0..item_count)
            .map(|ordinal| PostOrderItemV1 {
                item_id: format!("{request_id}-item-{ordinal:02}"),
                order: SimBuyOrderV1 {
                    condition_id: long_market().condition_id,
                    token_id: long_asset("1", 0, "Up").token_id,
                    kind: SimBuyOrderKindV1::GtcLimit {
                        limit_price_pusd_per_share: decimal("0.3"),
                        size_shares: decimal("5"),
                        post_only: true,
                    },
                },
            })
            .collect::<Vec<_>>();
        let registration = session
            .post_orders(PostOrdersRequestV1 {
                context: context_for_arrival(
                    &session,
                    &profile,
                    &request_id,
                    3 + batch,
                    220_000 + i64::try_from(batch).expect("任务 4.1 批次适合 i64") * 10_000,
                ),
                idempotency_key: format!("{request_id}-idem"),
                items,
            })
            .expect("任务 4.1 分页来源批量订单登记");
        let effective_at = registration.items[0].effective_processing_at_utc_ms;
        assert_eq!(
            apply_empty_at(&mut session, effective_at),
            MarketBucketApplyDispositionV1::Applied
        );
        for ordinal in 0..item_count {
            assert_eq!(
                session.drive_next_execution_action_through(effective_at),
                Ok(SubmissionDriveDispositionV1::ItemProcessed {
                    request_completed: ordinal + 1 == item_count,
                })
            );
        }
        drain_accepting(
            &mut session,
            response_received_at(&registration.receipt, effective_at),
        );
        let responses = captures
            .post_orders
            .lock()
            .expect("任务 4.1 批量提交响应锁");
        let response = responses.get(before).expect("任务 4.1 批量提交直接响应");
        assert_eq!(response.items.len(), item_count);
        assert!(
            response
                .items
                .iter()
                .all(|item| matches!(&item.outcome, SubmissionItemOutcomeV1::Accepted(_)))
        );
    }
    assert_eq!(session.simulated_order_count(), 102);

    let root_query = session
        .orders(OrdersRequestV1 {
            context: context_for_arrival(&session, &profile, "task412-root-query", 10, 300_000),
            filter: OrdersFilterV1::default(),
            cursor: None,
        })
        .expect("任务 4.1 分页根查询登记");
    let root_query_at = root_query.receipt.transit.platform_arrived_at_utc_ms;
    assert_eq!(
        apply_empty_at(&mut session, root_query_at),
        MarketBucketApplyDispositionV1::Applied
    );
    drive_one(&mut session, root_query_at, &root_query.receipt);
    let root_page = {
        let responses = captures.orders.lock().expect("任务 4.1 订单列表响应锁");
        let OrdersQueryResultV1::Page(page) =
            &responses.last().expect("任务 4.1 分页根响应").result
        else {
            panic!("任务 4.1 分页根必须成功");
        };
        assert_eq!(page.items.len(), 100);
        page.clone()
    };
    let retained_cursor = root_page.next_cursor.clone().expect("任务 4.1 分页根游标");
    let root_before = session.task41_diagnostics_for_tests(&condition_id);
    assert_eq!(root_before.query_root_body_count, 1);
    assert_eq!(root_before.issued_cursor_count, 1);

    assert_eq!(
        apply_empty_at(&mut session, LONG_MARKET_END_AT_UTC_MS),
        MarketBucketApplyDispositionV1::Applied
    );
    let before_resolution = session.account_snapshot_for_tests();
    assert!(session.principal_encumbered_pusd() > Decimal::ZERO);
    assert!(outcome_balance(&before_resolution, "1") > Decimal::ZERO);
    let resolution = resolution_event(
        &long_market(),
        RESOLUTION_AT_UTC_MS,
        RESOLUTION_AT_UTC_MS + 100,
        3,
    );
    let resolution_bucket = hidden_bucket(
        &session,
        RESOLUTION_AT_UTC_MS,
        MillisecondBucketClosureV1::NextEventAt(RESOLUTION_AT_UTC_MS + 1),
        std::slice::from_ref(&resolution),
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(resolution_bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let terminal_account = session.account_snapshot_for_tests();
    let terminal_seq = session.next_account_transition_seq();
    let terminal_revision = session.query_projection_revision();
    let terminal = session.task41_diagnostics_for_tests(&condition_id);
    assert_eq!(
        session
            .platform_market_state(&condition_id)
            .expect("任务 4.1 判定终态市场")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::ResolutionTerminal
    );
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);
    assert_eq!(outcome_balance(&terminal_account, "1"), Decimal::ZERO);
    assert_eq!(terminal.hot_orders, terminal.compact_orders);
    assert_eq!(terminal.hot_trades, terminal.compact_trades);
    assert_eq!(terminal.hot_orders.len(), 102);
    assert_eq!(terminal.hot_trades.len(), 1);
    assert!(!terminal.confirmed_direct_response_confirmations.is_empty());
    assert!(
        !terminal
            .confirmed_lifecycle_feedback_confirmations
            .is_empty()
    );
    assert_eq!(
        sort_confirmations(terminal.confirmed_direct_response_confirmations.clone()),
        captures.direct_confirmations()
    );
    assert_eq!(
        sort_confirmations(terminal.confirmed_lifecycle_feedback_confirmations.clone()),
        captures.lifecycle_confirmations()
    );
    assert!(terminal.hot_orders.iter().any(|order| {
        order.platform_order_id == exact_order_id
            && order.status.known == Some(SimulatedOrderKnownStatusV1::CanceledMarketResolved)
    }));
    assert_eq!(
        session.apply_closed_market_event_bucket(resolution_bucket),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(session.account_snapshot_for_tests(), terminal_account);
    assert_eq!(session.next_account_transition_seq(), terminal_seq);
    assert_eq!(session.query_projection_revision(), terminal_revision);
    assert_eq!(
        session.task41_diagnostics_for_tests(&condition_id),
        terminal
    );

    let account_before_idempotent_retry = session.account_snapshot_for_tests();
    let mut idempotent_retry = original_gtc_request;
    idempotent_retry.context = context_for_arrival(
        &session,
        &profile,
        "task412-original-gtc-retry",
        11,
        520_000,
    );
    let retry_response = post_one(&mut session, &captures, idempotent_retry);
    assert_eq!(
        retry_response.item.submission_effect_disposition,
        SubmissionEffectDispositionV1::ExistingEffect
    );
    assert_eq!(
        retry_response.item.submission_effect_identity,
        original_gtc.item.submission_effect_identity
    );
    assert_eq!(retry_response.item.outcome, original_gtc.item.outcome);
    assert_eq!(
        session.account_snapshot_for_tests(),
        account_before_idempotent_retry
    );
    assert_eq!(session.query_projection_revision(), terminal_revision);
    assert_eq!(session.simulated_order_count(), 102);

    let continuation = session
        .orders(OrdersRequestV1 {
            context: context_for_arrival(
                &session,
                &profile,
                "task412-expired-root-continuation",
                12,
                530_000,
            ),
            filter: OrdersFilterV1::default(),
            cursor: Some(retained_cursor),
        })
        .expect("任务 4.1 旧根续页登记");
    let new_floor = session.next_query_root_sequence();
    session
        .advance_query_root_retention_floor(new_floor)
        .expect("任务 4.1 推进查询根逻辑水位");
    let after_floor = session.task41_diagnostics_for_tests(&condition_id);
    assert_eq!(after_floor.earliest_retained_root_sequence, new_floor);
    assert_eq!(after_floor.query_root_body_count, 0);
    assert_eq!(after_floor.issued_cursor_count, 0);
    let continuation_at = continuation.receipt.transit.platform_arrived_at_utc_ms;
    assert_eq!(
        apply_empty_at(&mut session, continuation_at),
        MarketBucketApplyDispositionV1::Applied
    );
    drive_one(&mut session, continuation_at, &continuation.receipt);
    let responses = captures.orders.lock().expect("任务 4.1 订单列表响应锁");
    let OrdersQueryResultV1::OutOfRetention(out_of_retention) =
        &responses.last().expect("任务 4.1 旧根续页响应").result
    else {
        panic!("任务 4.1 超过逻辑水位的旧根必须过期");
    };
    assert_eq!(out_of_retention.earliest_retained_root_sequence, new_floor);
    drop(responses);

    let filtered_trade = session
        .trades(TradesRequestV1 {
            context: context_for_arrival(
                &session,
                &profile,
                "task412-filtered-trade-from-compact",
                13,
                535_000,
            ),
            filter: TradesFilterV1 {
                condition_id: Some(long_market().condition_id),
                token_id: Some(long_asset("1", 0, "Up").token_id),
                after_utc_ms: Some(0),
                before_utc_ms: Some(RESOLUTION_AT_UTC_MS),
                ..TradesFilterV1::default()
            },
            cursor: None,
        })
        .expect("任务 4.1 紧凑成交过滤查询登记");
    let filtered_trade_at = filtered_trade.receipt.transit.platform_arrived_at_utc_ms;
    assert_eq!(
        apply_empty_at(&mut session, filtered_trade_at),
        MarketBucketApplyDispositionV1::Applied
    );
    drive_one(&mut session, filtered_trade_at, &filtered_trade.receipt);
    let filtered_trade_response = captures
        .trades
        .lock()
        .expect("任务 4.1 成交列表响应锁")
        .last()
        .cloned()
        .expect("任务 4.1 紧凑成交过滤响应");
    let TradesQueryResultV1::Page(filtered_trade_page) = &filtered_trade_response.result else {
        panic!("任务 4.1 紧凑成交过滤必须返回正常页");
    };
    assert_eq!(filtered_trade_page.items, terminal.compact_trades);
    let after_compact_filter = session.task41_diagnostics_for_tests(&condition_id);
    assert!(!after_compact_filter.hot_orders.is_empty());
    assert!(!after_compact_filter.hot_trades.is_empty());

    session
        .advance_order_history_retention_floor(RESOLUTION_AT_UTC_MS)
        .expect("任务 4.1 推进订单历史水位");
    session
        .advance_trade_history_retention_floor(RESOLUTION_AT_UTC_MS)
        .expect("任务 4.1 推进成交历史水位");
    let after_history_floors = session.task41_diagnostics_for_tests(&condition_id);
    assert!(
        after_history_floors
            .confirmed_direct_response_confirmations
            .len()
            >= terminal.confirmed_direct_response_confirmations.len()
    );
    assert!(
        after_history_floors
            .confirmed_lifecycle_feedback_confirmations
            .len()
            >= terminal.confirmed_lifecycle_feedback_confirmations.len()
    );
    assert_eq!(
        sort_confirmations(
            after_history_floors
                .confirmed_direct_response_confirmations
                .clone(),
        ),
        captures.direct_confirmations()
    );
    assert_eq!(
        sort_confirmations(
            after_history_floors
                .confirmed_lifecycle_feedback_confirmations
                .clone(),
        ),
        captures.lifecycle_confirmations()
    );

    let known_order = session
        .order(OrderRequestV1 {
            context: context_for_arrival(
                &session,
                &profile,
                "task412-known-order-after-floor",
                14,
                540_000,
            ),
            platform_order_id: exact_order_id.clone(),
        })
        .expect("任务 4.1 已知订单查询登记");
    let known_order_at = known_order.receipt.transit.platform_arrived_at_utc_ms;
    assert_eq!(
        apply_empty_at(&mut session, known_order_at),
        MarketBucketApplyDispositionV1::Applied
    );
    drive_one(&mut session, known_order_at, &known_order.receipt);
    let known_order_result = captures
        .order
        .lock()
        .expect("任务 4.1 单订单查询响应锁")
        .last()
        .cloned()
        .expect("任务 4.1 已知订单查询响应");
    let OrderQueryResultV1::Found(found_order) = &known_order_result.result else {
        panic!("任务 4.1 已知精确订单不得因水位误报过期");
    };
    assert_eq!(
        found_order.as_ref(),
        terminal
            .compact_orders
            .iter()
            .find(|order| order.platform_order_id == exact_order_id)
            .expect("任务 4.1 紧凑订单")
    );

    let missing_order = session
        .order(OrderRequestV1 {
            context: context_for_arrival(
                &session,
                &profile,
                "task412-missing-order-after-floor",
                15,
                550_000,
            ),
            platform_order_id: "e".repeat(64),
        })
        .expect("任务 4.1 缺失订单查询登记");
    let missing_order_at = missing_order.receipt.transit.platform_arrived_at_utc_ms;
    assert_eq!(
        apply_empty_at(&mut session, missing_order_at),
        MarketBucketApplyDispositionV1::Applied
    );
    drive_one(&mut session, missing_order_at, &missing_order.receipt);
    assert!(matches!(
        &captures
            .order
            .lock()
            .expect("任务 4.1 单订单查询响应锁")
            .last()
            .expect("任务 4.1 缺失订单查询响应")
            .result,
        OrderQueryResultV1::OutOfRetention(_)
    ));

    let active_orders = session
        .orders(OrdersRequestV1 {
            context: context_for_arrival(
                &session,
                &profile,
                "task412-terminal-active-orders",
                16,
                560_000,
            ),
            filter: OrdersFilterV1::default(),
            cursor: None,
        })
        .expect("任务 4.1 终态活动订单查询登记");
    let active_orders_at = active_orders.receipt.transit.platform_arrived_at_utc_ms;
    assert_eq!(
        apply_empty_at(&mut session, active_orders_at),
        MarketBucketApplyDispositionV1::Applied
    );
    drive_one(&mut session, active_orders_at, &active_orders.receipt);
    let active_responses = captures.orders.lock().expect("任务 4.1 订单列表响应锁");
    let OrdersQueryResultV1::Page(active_page) = &active_responses
        .last()
        .expect("任务 4.1 终态订单列表响应")
        .result
    else {
        panic!("任务 4.1 判定终态活动订单列表必须是正常页");
    };
    assert!(active_page.items.is_empty());
    drop(active_responses);

    let known_trade = session
        .trades(TradesRequestV1 {
            context: context_for_arrival(
                &session,
                &profile,
                "task412-known-trade-after-floor",
                17,
                570_000,
            ),
            filter: TradesFilterV1 {
                trade_id: Some(trade_id.clone()),
                ..TradesFilterV1::default()
            },
            cursor: None,
        })
        .expect("任务 4.1 已知成交查询登记");
    let known_trade_at = known_trade.receipt.transit.platform_arrived_at_utc_ms;
    assert_eq!(
        apply_empty_at(&mut session, known_trade_at),
        MarketBucketApplyDispositionV1::Applied
    );
    drive_one(&mut session, known_trade_at, &known_trade.receipt);
    let known_trade_response = captures
        .trades
        .lock()
        .expect("任务 4.1 成交列表响应锁")
        .last()
        .cloned()
        .expect("任务 4.1 已知成交查询响应");
    assert!(matches!(
        &known_trade_response.result,
        TradesQueryResultV1::OutOfRetention(_)
    ));

    let after_compact_queries = session.task41_diagnostics_for_tests(&condition_id);
    assert!(!after_compact_queries.hot_orders.is_empty());
    assert!(!after_compact_queries.hot_trades.is_empty());

    let missing_trade = session
        .trades(TradesRequestV1 {
            context: context_for_arrival(
                &session,
                &profile,
                "task412-missing-trade-after-floor",
                18,
                590_000,
            ),
            filter: TradesFilterV1 {
                trade_id: Some("f".repeat(64)),
                ..TradesFilterV1::default()
            },
            cursor: None,
        })
        .expect("任务 4.1 缺失成交查询登记");
    let missing_trade_at = missing_trade.receipt.transit.platform_arrived_at_utc_ms;
    assert_eq!(
        apply_empty_at(&mut session, missing_trade_at),
        MarketBucketApplyDispositionV1::Applied
    );
    drive_one(&mut session, missing_trade_at, &missing_trade.receipt);
    assert!(matches!(
        &captures
            .trades
            .lock()
            .expect("任务 4.1 成交列表响应锁")
            .last()
            .expect("任务 4.1 缺失成交查询响应")
            .result,
        TradesQueryResultV1::OutOfRetention(_)
    ));
    let after_all_queries = session.task41_diagnostics_for_tests(&condition_id);
    assert_eq!(
        sort_confirmations(
            after_all_queries
                .confirmed_direct_response_confirmations
                .clone(),
        ),
        captures.direct_confirmations()
    );
    assert_eq!(
        sort_confirmations(
            after_all_queries
                .confirmed_lifecycle_feedback_confirmations
                .clone(),
        ),
        captures.lifecycle_confirmations()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task411_first_end_of_input_seals_only_internal_failure_and_preserves_reachable_predecessor()
 {
    let incomplete_captures = Task41Captures::default();
    let mut incomplete =
        unloaded_short_session("task411-incomplete-eof", incomplete_captures.bridge()).await;
    let incomplete_condition_id = short_market().condition_id.0;
    let incomplete_eof = hidden_bucket(
        &incomplete,
        1_100,
        MillisecondBucketClosureV1::EndOfInput,
        &[],
    );
    assert!(matches!(
        incomplete.apply_closed_market_event_bucket(incomplete_eof),
        Err(MarketEventErrorV1::IncompleteInitialLoad(condition_ids))
            if condition_ids == vec![incomplete_condition_id.clone()]
    ));
    assert_eq!(
        incomplete
            .task41_diagnostics_for_tests(&incomplete_condition_id)
            .input_seal,
        None
    );
    assert_eq!(incomplete.platform_event_index_position(), 0);

    let captures = Task41Captures::default();
    let (mut session, profile) = long_session(
        "task411-missing-terminal-eof",
        &captures,
        crate::finality::successful_policy_for_tests(),
    )
    .await;
    let condition_id = long_market().condition_id.0;
    let end_at = LONG_MARKET_END_AT_UTC_MS;
    let eof_at = end_at + 1_000;
    let open_request = post_order_request(
        &session,
        &profile,
        "task411-eof-open-order",
        1,
        200_000,
        SimBuyOrderV1 {
            condition_id: long_market().condition_id,
            token_id: long_asset("1", 0, "Up").token_id,
            kind: SimBuyOrderKindV1::GtdLimit {
                limit_price_pusd_per_share: decimal("0.4"),
                size_shares: decimal("5"),
                post_only: true,
                expires_at_utc_ms: eof_at + 999,
            },
        },
    );
    let open_registration = session
        .post_order(open_request.clone())
        .expect("任务 4.1 EOF 前 GTD 登记");
    assert_eq!(
        open_registration.receipt.disposition,
        RequestRegistrationDispositionV1::NewLogicalRequest
    );
    let open_effective_at = open_registration.item.effective_processing_at_utc_ms;
    assert_eq!(
        apply_empty_at(&mut session, open_effective_at),
        MarketBucketApplyDispositionV1::Applied
    );
    drive_one(&mut session, open_effective_at, &open_registration.receipt);
    let open_response = captures
        .post_order
        .lock()
        .expect("任务 4.1 EOF 前 GTD 响应锁")
        .last()
        .cloned()
        .expect("任务 4.1 EOF 前 GTD 响应");
    let SubmissionItemOutcomeV1::Accepted(open_accepted) = &open_response.item.outcome else {
        panic!("任务 4.1 EOF 前 GTD 必须接受");
    };
    let open_order_id = open_accepted.simulated_platform_order_id.clone();
    drain_accepting(&mut session, end_at - 1);
    assert_eq!(session.next_order_deadline_at_utc_ms(), Some(eof_at));
    let open_order_before_eof = session
        .simulated_order(&open_order_id)
        .expect("任务 4.1 EOF 前开放 GTD")
        .clone();
    assert!(open_order_before_eof.open_base_units > 0);
    assert_eq!(open_order_before_eof.expired_base_units, 0);
    let pending_query = session
        .order(OrderRequestV1 {
            context: context_for_arrival(
                &session,
                &profile,
                "task411-pending-query-before-failed-eof",
                2,
                eof_at + 100_000,
            ),
            platform_order_id: open_order_id.clone(),
        })
        .expect("任务 4.1 EOF 前登记待执行查询");
    let before_eof = session.task41_diagnostics_for_tests(&condition_id);
    assert!(before_eof.market_external_book_level_count > 0);
    assert!(before_eof.execution_queue_level_count > 0);
    assert!(before_eof.execution_queue_segment_count > 0);
    let account_before_eof = session.account_snapshot_for_tests();
    let seq_before_eof = session.next_account_transition_seq();
    let revision_before_eof = session.query_projection_revision();
    let delivery_counts_before_eof = captures.delivery_counts();
    let eof_bucket = hidden_bucket(
        &session,
        eof_at,
        MillisecondBucketClosureV1::EndOfInput,
        &[],
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(eof_bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let failed = session.task41_diagnostics_for_tests(&condition_id);
    assert_eq!(
        failed.input_seal,
        Some((
            MarketEventPathV1::HiddenPlatform,
            InputPathSealOutcomeV1::FailedMissingQualifiedMarketTerminal {
                condition_ids: vec![condition_id.clone()],
            },
        ))
    );
    assert_eq!(session.platform_event_index_position(), 2);
    assert_eq!(failed.market_external_book_level_count, 0);
    assert_eq!(failed.execution_queue_level_count, 0);
    assert_eq!(failed.execution_queue_segment_count, 0);
    assert_eq!(failed.execution_queue_debt_base_units, 0);
    assert_eq!(
        session
            .platform_market_state(&condition_id)
            .expect("任务 4.1 EOF 失败封存市场")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::PostEndPendingResolution
    );
    assert_eq!(session.account_snapshot_for_tests(), account_before_eof);
    assert_eq!(session.next_account_transition_seq(), seq_before_eof);
    assert_eq!(session.query_projection_revision(), revision_before_eof);
    assert_eq!(session.next_order_deadline_at_utc_ms(), Some(eof_at));
    assert_eq!(
        session.simulated_order(&open_order_id),
        Some(&open_order_before_eof)
    );
    assert_eq!(failed.hot_orders, before_eof.hot_orders);
    assert_eq!(failed.compact_orders, before_eof.compact_orders);
    assert_eq!(failed.hot_trades, before_eof.hot_trades);
    assert_eq!(failed.compact_trades, before_eof.compact_trades);
    assert_eq!(
        failed.query_root_body_count,
        before_eof.query_root_body_count
    );
    assert_eq!(failed.issued_cursor_count, before_eof.issued_cursor_count);
    assert_eq!(
        failed.pending_direct_response_count,
        before_eof.pending_direct_response_count
    );
    assert_eq!(
        failed.pending_lifecycle_feedback_count,
        before_eof.pending_lifecycle_feedback_count
    );
    assert_eq!(captures.delivery_counts(), delivery_counts_before_eof);

    assert_eq!(
        session.apply_closed_market_event_bucket(eof_bucket),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(session.task41_diagnostics_for_tests(&condition_id), failed);
    assert_eq!(captures.delivery_counts(), delivery_counts_before_eof);

    let open_retry = session
        .post_order(open_request)
        .expect("任务 4.1 失败封存后原 GTD 物理重投");
    assert_eq!(
        open_retry.receipt.disposition,
        RequestRegistrationDispositionV1::PhysicalRetry
    );
    assert_eq!(
        open_retry.receipt.transit,
        open_registration.receipt.transit
    );
    assert_eq!(
        open_retry.receipt.scheduled_action_count,
        open_registration.receipt.scheduled_action_count
    );
    assert_eq!(open_retry.item, open_registration.item);
    assert_eq!(session.task41_diagnostics_for_tests(&condition_id), failed);
    assert_eq!(captures.delivery_counts(), delivery_counts_before_eof);

    assert_eq!(
        session.drive_next_execution_action_through(
            pending_query.receipt.transit.platform_arrived_at_utc_ms,
        ),
        Err(SubmissionProcessingErrorV1::Session(
            SessionError::InputFailedSealed,
        ))
    );
    assert_eq!(session.task41_diagnostics_for_tests(&condition_id), failed);
    assert_eq!(captures.delivery_counts(), delivery_counts_before_eof);

    assert_eq!(
        session.order(OrderRequestV1 {
            context: context_for_arrival(
                &session,
                &profile,
                "task411-query-after-failed-eof",
                3,
                eof_at + 200_000,
            ),
            platform_order_id: open_order_id,
        }),
        Err(SessionError::InputFailedSealed)
    );
    assert_eq!(session.task41_diagnostics_for_tests(&condition_id), failed);
    assert_eq!(captures.delivery_counts(), delivery_counts_before_eof);

    let pending_captures = Task41Captures::default();
    let pending_policy = policy_for_tests(
        "task411-pending-resolution-policy",
        &[
            (TradeFinalityStateV1::Matched, 0),
            (TradeFinalityStateV1::Mined, 1),
            (TradeFinalityStateV1::Confirmed, 400_000),
        ],
        1,
        Vec::new(),
    );
    let (mut pending, pending_profile) = long_session(
        "task411-qualified-resolution-waits",
        &pending_captures,
        pending_policy,
    )
    .await;
    let pending_fak = fak_request(
        &pending,
        &pending_profile,
        "task411-pending-fill",
        1,
        200_000,
    );
    let pending_fill_response = post_one(&mut pending, &pending_captures, pending_fak);
    let SubmissionItemOutcomeV1::Accepted(pending_fill) = &pending_fill_response.item.outcome
    else {
        panic!("任务 4.1 等待前驱夹具必须实际产生成交");
    };
    let pending_trade_id = pending_fill
        .fills
        .first()
        .expect("任务 4.1 等待前驱夹具成交")
        .simulated_trade_id
        .clone();
    let pending_finality_at = pending_fill_response
        .item
        .effective_processing_at_utc_ms
        .checked_add(400_000)
        .expect("任务 4.1 等待前驱最终性时点");
    assert_eq!(
        apply_empty_at(&mut pending, LONG_MARKET_END_AT_UTC_MS),
        MarketBucketApplyDispositionV1::Applied
    );
    let pending_resolution = resolution_event(
        &long_market(),
        PENDING_RESOLUTION_AT_UTC_MS,
        PENDING_RESOLUTION_AT_UTC_MS + 100,
        3,
    );
    assert_eq!(
        pending.apply_closed_market_event_bucket(hidden_bucket(
            &pending,
            PENDING_RESOLUTION_AT_UTC_MS,
            MillisecondBucketClosureV1::NextEventAt(PENDING_RESOLUTION_AT_UTC_MS + 1),
            &[pending_resolution],
        )),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let pending_condition_id = long_market().condition_id.0;
    let before_pending_eof = pending.task41_diagnostics_for_tests(&pending_condition_id);
    let pending_account = pending.account_snapshot_for_tests();
    assert_eq!(
        pending
            .platform_market_state(&pending_condition_id)
            .expect("任务 4.1 等待前驱市场")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::PostEndPendingResolution
    );
    assert!(before_pending_eof.compact_orders.is_empty());
    assert!(before_pending_eof.compact_trades.is_empty());
    let pending_trade_before_eof = before_pending_eof
        .hot_trades
        .iter()
        .find(|trade| trade.simulated_trade_id == pending_trade_id)
        .expect("任务 4.1 EOF 前等待前驱成交");
    assert_eq!(
        pending_trade_before_eof.status.known,
        Some(SimulatedTradeKnownStatusV1::Mined)
    );
    let exhausted = hidden_bucket(
        &pending,
        PENDING_RESOLUTION_AT_UTC_MS + 1,
        MillisecondBucketClosureV1::EndOfInput,
        &[],
    );
    assert_eq!(
        pending.apply_closed_market_event_bucket(exhausted.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let exhausted_diagnostics = pending.task41_diagnostics_for_tests(&pending_condition_id);
    assert_eq!(
        exhausted_diagnostics.input_seal,
        Some((
            MarketEventPathV1::HiddenPlatform,
            InputPathSealOutcomeV1::Exhausted,
        ))
    );
    assert_eq!(pending.platform_event_index_position(), 3);
    assert_eq!(pending.account_snapshot_for_tests(), pending_account);
    assert!(exhausted_diagnostics.compact_orders.is_empty());
    assert!(exhausted_diagnostics.compact_trades.is_empty());
    assert_eq!(
        pending.apply_closed_market_event_bucket(exhausted),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(
        pending.task41_diagnostics_for_tests(&pending_condition_id),
        exhausted_diagnostics
    );

    assert_eq!(
        pending.drive_next_execution_action_through(pending_finality_at),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    let after_predecessor = pending.task41_diagnostics_for_tests(&pending_condition_id);
    let terminal_trade = after_predecessor
        .hot_trades
        .iter()
        .find(|trade| trade.simulated_trade_id == pending_trade_id)
        .expect("任务 4.1 前驱推进后成交");
    assert_eq!(
        terminal_trade.status.known,
        Some(SimulatedTradeKnownStatusV1::Confirmed),
        "任务 4.1 内部最终性前驱必须先实际完成"
    );
    assert_eq!(
        pending
            .platform_market_state(&pending_condition_id)
            .expect("任务 4.1 前驱完成后市场")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::ResolutionTerminal
    );
    assert_eq!(
        after_predecessor.input_seal,
        exhausted_diagnostics.input_seal
    );
    assert_eq!(
        after_predecessor.hot_orders,
        after_predecessor.compact_orders
    );
    assert_eq!(
        after_predecessor.hot_trades,
        after_predecessor.compact_trades
    );
    assert_eq!(
        outcome_balance(&pending.account_snapshot_for_tests(), "1"),
        Decimal::ZERO
    );
}
