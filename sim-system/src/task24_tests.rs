use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use contracts::BookSide;
use rust_decimal::Decimal;

use crate::execution::{
    CancelAllOrdersDirectResponseV1, CancelAllOrdersRequestV1, CancelEffectDispositionV1,
    CancelNotCanceledReasonV1, CancelOrderDirectResponseV1, CancelOrderItemV1,
    CancelOrderRequestV1, CancelOrdersDirectResponseV1, CancelOrdersRequestV1,
    CancelTargetOutcomeV1, OrderDirectResponseV1, OrderQueryResultV1, OrderRequestV1,
    OrdersDirectResponseV1, OrdersFilterV1, OrdersQueryResultV1, OrdersRequestV1,
    PostOrderDirectResponseV1, PostOrderItemV1, PostOrderRequestV1, PostOrdersDirectResponseV1,
    PostOrdersRequestV1, RequestRegistrationReceiptV1, SimBuyOrderKindV1, SimBuyOrderV1,
    SimCallerRoleV1, SimExecutionContractErrorV1, SimExecutionProcessV1, SimExecutionSessionV1,
    SimulatedOrderKnownStatusV1, SubmissionDeliveryDispositionV1, SubmissionDriveDispositionV1,
    SubmissionItemOutcomeV1, SubmissionResponseAcceptanceV1, SubmissionResponseBridgeV1,
    TradeLiquidityRoleV1, TradesDirectResponseV1, TradesFilterV1, TradesQueryResultV1,
    TradesRequestV1, accepting_submission_response_bridge, orders_projection_canonical_byte_len,
};
use crate::latency_profile::{EmpiricalDelayProfile, load_embedded_proxy_latency_profile};
use crate::platform_rules::tests::successful_snapshot_for_sim_tests;
use crate::session::{RequestContextV1, RequestRegistrationDispositionV1};
use crate::task21_tests::{
    asset, hidden_bucket, initial_snapshot_event, market, scenario, session_spec,
};
use crate::task22_tests::{
    asset as long_asset, context_for_arrival as long_context_for_arrival,
    hidden_bucket as long_hidden_bucket, load_initial_market as load_long_initial_market,
    market as long_market, scenario as long_scenario, session_spec as long_session_spec,
};
use crate::task23_tests::{
    bid_transition, long_ask_transition, session_with_one_pending_maker_depletion,
    session_with_one_pending_maker_depletion_with_bridge,
};

#[derive(Clone, Default)]
struct Task24Captures {
    post_order: Arc<Mutex<Vec<Arc<PostOrderDirectResponseV1>>>>,
    post_orders: Arc<Mutex<Vec<Arc<PostOrdersDirectResponseV1>>>>,
    cancel_order: Arc<Mutex<Vec<Arc<CancelOrderDirectResponseV1>>>>,
    cancel_orders: Arc<Mutex<Vec<Arc<CancelOrdersDirectResponseV1>>>>,
    cancel_all_orders: Arc<Mutex<Vec<Arc<CancelAllOrdersDirectResponseV1>>>>,
    order: Arc<Mutex<Vec<Arc<OrderDirectResponseV1>>>>,
    orders: Arc<Mutex<Vec<Arc<OrdersDirectResponseV1>>>>,
    trades: Arc<Mutex<Vec<Arc<TradesDirectResponseV1>>>>,
}

impl Task24Captures {
    fn bridge(&self) -> SubmissionResponseBridgeV1 {
        let post_order = Arc::clone(&self.post_order);
        let post_orders = Arc::clone(&self.post_orders);
        let cancel_order = Arc::clone(&self.cancel_order);
        let cancel_orders = Arc::clone(&self.cancel_orders);
        let cancel_all_orders = Arc::clone(&self.cancel_all_orders);
        let order = Arc::clone(&self.order);
        let orders = Arc::clone(&self.orders);
        let trades = Arc::clone(&self.trades);
        SubmissionResponseBridgeV1::new(
            move |response| {
                post_order
                    .lock()
                    .expect("单笔提交响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                post_orders
                    .lock()
                    .expect("批量提交响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                cancel_order
                    .lock()
                    .expect("单撤响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                cancel_orders
                    .lock()
                    .expect("批撤响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                cancel_all_orders
                    .lock()
                    .expect("全撤响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                order
                    .lock()
                    .expect("单订单查询响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                orders
                    .lock()
                    .expect("订单列表响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                trades
                    .lock()
                    .expect("成交列表响应锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
        )
    }
}

fn decimal(value: &str) -> Decimal {
    value.parse().expect("任务 2.4 测试十进制")
}

async fn ready_session_with_bridge(
    id: &str,
    capacity: usize,
    bridge: SubmissionResponseBridgeV1,
) -> SimExecutionSessionV1 {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
    let scenario = scenario();
    let mut spec = session_spec(&scenario, SimCallerRoleV1::ModelTrain, id);
    spec.response_outbox_capacity = NonZeroUsize::new(capacity).expect("正响应槽容量");
    let mut session = SimExecutionProcessV1::new(rules, profile, scenario)
        .create_session(spec, bridge)
        .expect("任务 2.4 会话");
    let initial = [
        initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10"),
        initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "12"),
    ];
    session
        .apply_closed_market_event_bucket(hidden_bucket(1_100, 2, &initial))
        .expect("初始市场桶");
    session
}

async fn ready_session(id: &str) -> SimExecutionSessionV1 {
    ready_session_with_bridge(id, 2, accepting_submission_response_bridge()).await
}

async fn ready_long_session_with_bridge(
    id: &str,
    capacity: usize,
    bridge: SubmissionResponseBridgeV1,
) -> (SimExecutionSessionV1, Arc<EmpiricalDelayProfile>) {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
    let scenario = long_scenario();
    let mut spec = long_session_spec(&scenario, SimCallerRoleV1::ModelTrain, id);
    spec.response_outbox_capacity = NonZeroUsize::new(capacity).expect("正响应槽容量");
    let mut session = SimExecutionProcessV1::new(rules, Arc::clone(&profile), scenario)
        .create_session(spec, bridge)
        .expect("任务 2.4 长窗口会话");
    load_long_initial_market(&mut session, "0.00001", "0.6", "5");
    (session, profile)
}

async fn ready_long_session_pair_with_bridges(
    first_id: &str,
    first_bridge: SubmissionResponseBridgeV1,
    second_id: &str,
    second_bridge: SubmissionResponseBridgeV1,
) -> (
    SimExecutionSessionV1,
    SimExecutionSessionV1,
    Arc<EmpiricalDelayProfile>,
) {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
    let scenario = long_scenario();
    let mut process =
        SimExecutionProcessV1::new(rules, Arc::clone(&profile), Arc::clone(&scenario));
    let mut first_spec = long_session_spec(&scenario, SimCallerRoleV1::ModelTrain, first_id);
    first_spec.response_outbox_capacity = NonZeroUsize::new(2).expect("正响应槽容量");
    let mut second_spec = long_session_spec(&scenario, SimCallerRoleV1::ModelTrain, second_id);
    second_spec.response_outbox_capacity = NonZeroUsize::new(2).expect("正响应槽容量");
    let mut first = process
        .create_session(first_spec, first_bridge)
        .expect("同进程第一会话");
    let mut second = process
        .create_session(second_spec, second_bridge)
        .expect("同进程第二会话");
    load_long_initial_market(&mut first, "0.00001", "0.6", "5");
    load_long_initial_market(&mut second, "0.00001", "0.6", "5");
    (first, second, profile)
}

fn context(
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

fn gtc_request(
    session: &SimExecutionSessionV1,
    request_id: &str,
    caller_sequence: u64,
    request_sent_at_utc_ms: i64,
) -> PostOrderRequestV1 {
    PostOrderRequestV1 {
        context: context(session, request_id, caller_sequence, request_sent_at_utc_ms),
        idempotency_key: format!("idem-{request_id}"),
        item: PostOrderItemV1 {
            item_id: format!("item-{request_id}"),
            order: SimBuyOrderV1 {
                condition_id: market().condition_id,
                token_id: asset("1", 0, "Up").token_id,
                kind: SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: decimal("0.4"),
                    size_shares: decimal("5"),
                    post_only: true,
                },
            },
        },
    }
}

fn long_gtc_item(item_id: &str) -> PostOrderItemV1 {
    PostOrderItemV1 {
        item_id: item_id.to_owned(),
        order: SimBuyOrderV1 {
            condition_id: long_market().condition_id,
            token_id: long_asset("1", 0, "Up").token_id,
            kind: SimBuyOrderKindV1::GtcLimit {
                limit_price_pusd_per_share: decimal("0.4"),
                size_shares: decimal("5"),
                post_only: true,
            },
        },
    }
}

fn close_platform_at(session: &mut SimExecutionSessionV1, at: i64) {
    let position = session.platform_event_index_position();
    session
        .apply_closed_market_event_bucket(hidden_bucket(at, position, &[]))
        .expect("关闭到逻辑动作时点");
}

fn response_received_at(receipt: &RequestRegistrationReceiptV1, responded_at: i64) -> i64 {
    responded_at + i64::try_from(receipt.transit.inbound_delay_ms).expect("测试响应时延适合 i64")
}

fn drive_and_deliver(
    session: &mut SimExecutionSessionV1,
    effective_at: i64,
    receipt: &RequestRegistrationReceiptV1,
) {
    assert_eq!(
        session.drive_next_execution_action_through(effective_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    assert_eq!(
        session.drive_direct_response_delivery_through(response_received_at(receipt, effective_at)),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
}

fn post_long_batch(
    session: &mut SimExecutionSessionV1,
    profile: &EmpiricalDelayProfile,
    captures: &Task24Captures,
    request_id: &str,
    caller_sequence: u64,
    desired_arrival_at_utc_ms: i64,
    item_count: usize,
) -> (Vec<String>, i64) {
    let response_index = captures.post_orders.lock().expect("批量提交响应锁").len();
    let items = (0..item_count)
        .map(|ordinal| long_gtc_item(&format!("{request_id}-item-{ordinal:03}")))
        .collect::<Vec<_>>();
    let registration = session
        .post_orders(PostOrdersRequestV1 {
            context: long_context_for_arrival(
                session,
                profile,
                request_id,
                caller_sequence,
                desired_arrival_at_utc_ms,
            ),
            idempotency_key: format!("{request_id}-idem"),
            items,
        })
        .expect("长窗口批量提交登记");
    let effective_at = registration.items[0].effective_processing_at_utc_ms;
    assert!(
        registration
            .items
            .iter()
            .all(|item| item.effective_processing_at_utc_ms == effective_at)
    );
    close_platform_at(session, effective_at);
    for ordinal in 0..item_count {
        assert_eq!(
            session.drive_next_execution_action_through(effective_at),
            Ok(SubmissionDriveDispositionV1::ItemProcessed {
                request_completed: ordinal + 1 == item_count,
            })
        );
    }
    let received_at = response_received_at(&registration.receipt, effective_at);
    assert_eq!(
        session.drive_direct_response_delivery_through(received_at),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    let responses = captures.post_orders.lock().expect("批量提交响应锁");
    let response = responses.get(response_index).expect("批量提交直接响应");
    let order_ids = response
        .items
        .iter()
        .map(|item| {
            let SubmissionItemOutcomeV1::Accepted(accepted) = &item.outcome else {
                panic!("查询夹具订单必须被接受");
            };
            accepted.simulated_platform_order_id.clone()
        })
        .collect();
    (order_ids, received_at)
}

fn run_trades_query(
    session: &mut SimExecutionSessionV1,
    profile: &EmpiricalDelayProfile,
    captures: &Task24Captures,
    request_id: &str,
    caller_sequence: u64,
    desired_arrival_at_utc_ms: i64,
    filter: TradesFilterV1,
) -> TradesQueryResultV1 {
    let response_index = captures.trades.lock().expect("成交列表响应锁").len();
    let registration = session
        .trades(TradesRequestV1 {
            context: long_context_for_arrival(
                session,
                profile,
                request_id,
                caller_sequence,
                desired_arrival_at_utc_ms,
            ),
            filter,
            cursor: None,
        })
        .expect("成交列表登记");
    let effective_at = registration.receipt.transit.platform_arrived_at_utc_ms;
    close_platform_at(session, effective_at);
    drive_and_deliver(session, effective_at, &registration.receipt);
    captures
        .trades
        .lock()
        .expect("成交列表响应锁")
        .get(response_index)
        .expect("成交列表直接响应")
        .result
        .clone()
}

fn assert_model_visible_query_hides_hidden_provenance(
    value: &impl std::fmt::Debug,
    forbidden_values: &[&str],
) {
    let debug = format!("{value:?}");
    for forbidden in forbidden_values {
        assert!(
            !debug.contains(forbidden),
            "模型可见查询泄漏隐藏来源值 {forbidden}: {debug}"
        );
    }
    for forbidden_field in [
        "event_count:",
        "first_source_event_id:",
        "last_source_event_id:",
        "pending_maker_depletion_evidence:",
        "terminalized_pending_maker_depletion_evidence:",
        "source_identity:",
        "maker_depletion_evidence:",
    ] {
        assert!(
            !debug.contains(forbidden_field),
            "模型可见查询仍暴露隐藏来源字段 {forbidden_field}: {debug}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn pending_maker_queries_hide_hidden_provenance_before_public_visibility() {
    let captures = Task24Captures::default();
    let (mut session, platform_order_id, event_id, _, effective_at) =
        session_with_one_pending_maker_depletion_with_bridge(
            "task24-hidden-pending-query",
            Arc::clone(&captures.post_order),
            captures.bridge(),
            100_000,
        )
        .await;
    let internal_evidence = session
        .simulated_order(&platform_order_id)
        .and_then(|order| order.pending_maker_depletion_evidence.as_ref())
        .expect("内部待累计来源证据必须保留")
        .clone();
    assert_eq!(internal_evidence.event_count, 1);
    assert_eq!(internal_evidence.first_source_event_id, event_id);
    assert_eq!(
        session
            .model_visible_cursor()
            .public_visibility
            .closed_through_received_at_utc_ms(),
        None
    );

    let order_registration = session
        .order(OrderRequestV1 {
            context: context(&session, "hidden-pending-order", 2, effective_at + 1_000),
            platform_order_id: platform_order_id.clone(),
        })
        .expect("待累计订单单笔查询登记");
    let order_at = order_registration
        .receipt
        .transit
        .platform_arrived_at_utc_ms;
    assert!(order_at < effective_at + 100_001);
    close_platform_at(&mut session, order_at);
    drive_and_deliver(&mut session, order_at, &order_registration.receipt);

    let orders_registration = session
        .orders(OrdersRequestV1 {
            context: context(&session, "hidden-pending-orders", 3, effective_at + 2_000),
            filter: OrdersFilterV1::default(),
            cursor: None,
        })
        .expect("待累计订单列表查询登记");
    let orders_at = orders_registration
        .receipt
        .transit
        .platform_arrived_at_utc_ms;
    assert!(orders_at < effective_at + 100_001);
    close_platform_at(&mut session, orders_at);
    drive_and_deliver(&mut session, orders_at, &orders_registration.receipt);

    let order_responses = captures.order.lock().expect("单订单查询响应锁");
    let OrderQueryResultV1::Found(order) = &order_responses[0].result else {
        panic!("待累计订单必须仍可查询");
    };
    assert_eq!(order.platform_order_id, platform_order_id);
    assert_eq!(
        order.pending_maker_depletion_base_quantity_shares,
        decimal("0.000001")
    );
    assert_eq!(order.open_base_quantity_shares, decimal("4.999999"));
    assert_model_visible_query_hides_hidden_provenance(
        &order_responses[0].result,
        &[&event_id, &internal_evidence.sha256.0],
    );

    let orders_responses = captures.orders.lock().expect("订单列表响应锁");
    let OrdersQueryResultV1::Page(page) = &orders_responses[0].result else {
        panic!("待累计订单列表必须成功");
    };
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].platform_order_id, platform_order_id);
    assert_eq!(
        page.items[0].pending_maker_depletion_base_quantity_shares,
        decimal("0.000001")
    );
    assert_eq!(page.items[0].open_base_quantity_shares, decimal("4.999999"));
    assert_model_visible_query_hides_hidden_provenance(
        &orders_responses[0].result,
        &[&event_id, &internal_evidence.sha256.0],
    );
    assert_eq!(
        session
            .model_visible_cursor()
            .public_visibility
            .closed_through_received_at_utc_ms(),
        None
    );
}

#[tokio::test(flavor = "current_thread")]
async fn maker_trade_query_hides_hidden_depletion_provenance_before_public_visibility() {
    let captures = Task24Captures::default();
    let (mut session, platform_order_id, first_event_id, _, effective_at) =
        session_with_one_pending_maker_depletion_with_bridge(
            "task24-hidden-maker-trade",
            Arc::clone(&captures.post_order),
            captures.bridge(),
            100_000,
        )
        .await;
    let addition = bid_transition(
        effective_at + 3,
        effective_at + 100_003,
        6,
        "0.00001",
        "0",
        "0.099999",
    );
    session
        .apply_closed_market_event_bucket(hidden_bucket(effective_at + 3, 6, &[addition]))
        .expect("追加外部买方段");
    let depletion = bid_transition(
        effective_at + 4,
        effective_at + 100_004,
        7,
        "0.00001",
        "0.099999",
        "0",
    );
    let second_event_id = depletion.event_id.0.clone();
    session
        .apply_closed_market_event_bucket(hidden_bucket(effective_at + 4, 7, &[depletion]))
        .expect("形成 maker 推定成交");
    let internal_fill = session
        .simulated_fill(&platform_order_id)
        .expect("内部 maker 推定成交")
        .clone();
    let internal_evidence = internal_fill
        .maker_depletion_evidence
        .as_ref()
        .expect("内部 maker 成交来源证据")
        .clone();
    assert_eq!(internal_evidence.event_count, 2);
    assert_eq!(internal_evidence.first_source_event_id, first_event_id);
    assert_eq!(internal_evidence.last_source_event_id, second_event_id);
    assert_eq!(
        session
            .model_visible_cursor()
            .public_visibility
            .closed_through_received_at_utc_ms(),
        None
    );

    let registration = session
        .trades(TradesRequestV1 {
            context: context(&session, "hidden-maker-trades", 2, effective_at + 1_000),
            filter: TradesFilterV1::default(),
            cursor: None,
        })
        .expect("maker 成交查询登记");
    let query_at = registration.receipt.transit.platform_arrived_at_utc_ms;
    assert!(query_at < effective_at + 100_001);
    close_platform_at(&mut session, query_at);
    drive_and_deliver(&mut session, query_at, &registration.receipt);

    let responses = captures.trades.lock().expect("成交列表响应锁");
    let TradesQueryResultV1::Page(page) = &responses[0].result else {
        panic!("maker 成交查询必须成功");
    };
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].simulated_trade_id, internal_fill.trade_id);
    assert_eq!(page.items[0].platform_order_id, platform_order_id);
    assert_eq!(
        page.items[0].liquidity_role,
        TradeLiquidityRoleV1::MakerInferred
    );
    assert_eq!(page.items[0].base_quantity_shares, decimal("0.1"));
    assert_eq!(page.items[0].quote_amount_pusd, decimal("0.000001"));
    assert_model_visible_query_hides_hidden_provenance(
        &responses[0].result,
        &[
            &first_event_id,
            &second_event_id,
            &internal_fill.source_identity,
            &internal_evidence.sha256.0,
        ],
    );
    assert_eq!(
        session
            .model_visible_cursor()
            .public_visibility
            .closed_through_received_at_utc_ms(),
        None
    );
}

#[tokio::test(flavor = "current_thread")]
async fn taker_trade_query_hides_hidden_segment_identity_before_public_visibility() {
    let captures = Task24Captures::default();
    let (mut session, profile) =
        ready_long_session_with_bridge("task24-hidden-taker-trade", 2, captures.bridge()).await;
    let hidden_ask = long_ask_transition(120_000, 200_000, 3, "0.5", "0", "5");
    let start = session.platform_event_index_position();
    session
        .apply_closed_market_event_bucket(long_hidden_bucket(
            120_000,
            start,
            start + 1,
            &[hidden_ask],
        ))
        .expect("加入尚未公开可见的 taker 流动性");
    assert_eq!(
        session
            .model_visible_cursor()
            .public_visibility
            .closed_through_received_at_utc_ms(),
        None
    );
    let post = session
        .post_order(PostOrderRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "hidden-taker-source",
                1,
                130_000,
            ),
            idempotency_key: "hidden-taker-source-idem".to_owned(),
            item: PostOrderItemV1 {
                item_id: "hidden-taker-source-item".to_owned(),
                order: SimBuyOrderV1 {
                    condition_id: long_market().condition_id,
                    token_id: long_asset("1", 0, "Up").token_id,
                    kind: SimBuyOrderKindV1::FakMarket {
                        spend_pusd: decimal("2.5"),
                        worst_price_pusd_per_share: decimal("0.5"),
                    },
                },
            },
        })
        .expect("taker 来源订单登记");
    let post_at = post.item.effective_processing_at_utc_ms;
    assert!(post_at < 200_000);
    close_platform_at(&mut session, post_at);
    drive_and_deliver(&mut session, post_at, &post.receipt);
    let platform_order_id = {
        let responses = captures.post_order.lock().expect("提交响应锁");
        let SubmissionItemOutcomeV1::Accepted(accepted) = &responses[0].item.outcome else {
            panic!("taker 来源订单必须接受");
        };
        accepted.simulated_platform_order_id.clone()
    };
    let internal_fill = session
        .simulated_fill(&platform_order_id)
        .expect("内部 taker 成交")
        .clone();
    let result = run_trades_query(
        &mut session,
        &profile,
        &captures,
        "hidden-taker-trades",
        2,
        140_000,
        TradesFilterV1::default(),
    );
    let TradesQueryResultV1::Page(page) = &result else {
        panic!("taker 成交查询必须成功");
    };
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].simulated_trade_id, internal_fill.trade_id);
    assert_eq!(page.items[0].platform_order_id, platform_order_id);
    assert_eq!(page.items[0].liquidity_role, TradeLiquidityRoleV1::Taker);
    assert_eq!(page.items[0].source_price_pusd_per_share, decimal("0.5"));
    assert_eq!(page.items[0].base_quantity_shares, decimal("5"));
    assert_eq!(page.items[0].quote_amount_pusd, decimal("2.5"));
    assert_model_visible_query_hides_hidden_provenance(&result, &[&internal_fill.source_identity]);
    assert_eq!(
        session
            .model_visible_cursor()
            .public_visibility
            .closed_through_received_at_utc_ms(),
        None
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_action_uses_the_shared_action_driver_instead_of_the_submission_only_path() {
    let mut session = ready_session("task24-shared-driver").await;
    let registration = session
        .cancel_order(CancelOrderRequestV1 {
            context: context(&session, "cancel-missing", 1, 1_300),
            idempotency_key: "cancel-missing-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "cancel-missing-item".to_owned(),
                platform_order_id: "a".repeat(64),
            },
        })
        .expect("撤单登记");
    let effective_at = registration.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, effective_at);

    assert_eq!(
        session.drive_next_execution_action_through(effective_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn query_and_submission_share_one_bounded_response_slot() {
    let captures = Task24Captures::default();
    let mut session = ready_session_with_bridge("task24-shared-outbox", 1, captures.bridge()).await;
    let query = session
        .order(OrderRequestV1 {
            context: context(&session, "shared-slot-query", 1, 1_300),
            platform_order_id: "a".repeat(64),
        })
        .expect("查询应占用唯一响应槽");
    let post_request = gtc_request(&session, "shared-slot-post", 2, 1_301);

    assert_eq!(
        session.post_order(post_request.clone()),
        Err(SimExecutionContractErrorV1::ResponseOutboxFull)
    );

    let query_at = query.receipt.transit.platform_arrived_at_utc_ms;
    close_platform_at(&mut session, query_at);
    drive_and_deliver(&mut session, query_at, &query.receipt);
    assert!(matches!(
        captures.order.lock().expect("查询响应锁")[0].result,
        OrderQueryResultV1::NotFound
    ));

    session
        .post_order(post_request)
        .expect("确认释放统一响应槽后原提交上下文仍可登记");
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_and_order_query_use_the_same_private_delivery_chain_and_typed_handlers() {
    let captures = Task24Captures::default();
    let mut session =
        ready_session_with_bridge("task24-typed-delivery", 2, captures.bridge()).await;
    let missing_id = "b".repeat(64);
    let cancel = session
        .cancel_order(CancelOrderRequestV1 {
            context: context(&session, "typed-cancel", 1, 1_300),
            idempotency_key: "typed-cancel-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "typed-cancel-item".to_owned(),
                platform_order_id: missing_id.clone(),
            },
        })
        .expect("单撤登记");
    let order = session
        .order(OrderRequestV1 {
            context: context(&session, "typed-order", 2, 1_301),
            platform_order_id: missing_id.clone(),
        })
        .expect("单订单查询登记");
    let cancel_at = cancel.item.effective_processing_at_utc_ms;
    let order_at = order.receipt.transit.platform_arrived_at_utc_ms;
    let first_at = cancel_at.min(order_at);
    let second_at = cancel_at.max(order_at);
    close_platform_at(&mut session, first_at);
    assert!(matches!(
        session.drive_next_execution_action_through(first_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed { .. })
    ));
    if second_at > first_at {
        close_platform_at(&mut session, second_at);
    }
    assert!(matches!(
        session.drive_next_execution_action_through(second_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed { .. })
    ));
    let delivery_cutoff = response_received_at(&cancel.receipt, cancel_at)
        .max(response_received_at(&order.receipt, order_at));
    assert_eq!(
        session.drive_direct_response_delivery_through(delivery_cutoff),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );
    assert_eq!(
        session.drive_direct_response_delivery_through(delivery_cutoff),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );

    let cancel_responses = captures.cancel_order.lock().expect("单撤响应锁");
    assert_eq!(cancel_responses.len(), 1);
    assert_eq!(cancel_responses[0].target.platform_order_id, missing_id);
    assert_eq!(
        cancel_responses[0].target.outcome,
        CancelTargetOutcomeV1::NotCanceledNoEffect(CancelNotCanceledReasonV1::NotFoundOrNotOwned,)
    );
    let order_responses = captures.order.lock().expect("单订单查询响应锁");
    assert_eq!(order_responses.len(), 1);
    assert_eq!(order_responses[0].result, OrderQueryResultV1::NotFound);
    assert!(captures.post_order.lock().expect("提交响应锁").is_empty());
    assert!(captures.orders.lock().expect("列表响应锁").is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn post_end_cancel_releases_open_principal_once_and_idempotent_reuse_is_read_only() {
    let captures = Task24Captures::default();
    let mut session =
        ready_session_with_bridge("task24-post-end-cancel", 4, captures.bridge()).await;
    let post = session
        .post_order(gtc_request(&session, "active-order", 1, 1_300))
        .expect("开放订单登记");
    let post_at = post.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, post_at);
    drive_and_deliver(&mut session, post_at, &post.receipt);
    let platform_order_id = {
        let responses = captures.post_order.lock().expect("提交响应锁");
        let SubmissionItemOutcomeV1::Accepted(accepted) = &responses[0].item.outcome else {
            panic!("开放订单必须被接受");
        };
        accepted.simulated_platform_order_id.clone()
    };
    assert_eq!(session.query_projection_revision(), 1);
    assert_eq!(session.available_pusd(), decimal("998"));
    assert_eq!(session.principal_encumbered_pusd(), decimal("2"));

    let order_query = session
        .order(OrderRequestV1 {
            context: context(&session, "order-after-end", 2, 2_000),
            platform_order_id: platform_order_id.clone(),
        })
        .expect("盘后单订单查询登记");
    let order_query_at = order_query.receipt.transit.platform_arrived_at_utc_ms;
    assert!(order_query_at >= market().window.end_at_utc_ms);
    close_platform_at(&mut session, order_query_at);
    drive_and_deliver(&mut session, order_query_at, &order_query.receipt);
    {
        let responses = captures.order.lock().expect("单订单查询响应锁");
        let OrderQueryResultV1::Found(found) = &responses[0].result else {
            panic!("盘后未判定开放订单必须仍可单笔查询");
        };
        assert_eq!(found.platform_order_id, platform_order_id);
        assert_eq!(found.status.known, Some(SimulatedOrderKnownStatusV1::Live));
    }

    let orders_sent_at = response_received_at(&order_query.receipt, order_query_at) + 1;
    let orders_query = session
        .orders(OrdersRequestV1 {
            context: context(&session, "orders-after-end", 3, orders_sent_at),
            filter: OrdersFilterV1::default(),
            cursor: None,
        })
        .expect("盘后活动订单列表登记");
    let orders_query_at = orders_query.receipt.transit.platform_arrived_at_utc_ms;
    close_platform_at(&mut session, orders_query_at);
    drive_and_deliver(&mut session, orders_query_at, &orders_query.receipt);
    {
        let responses = captures.orders.lock().expect("订单列表响应锁");
        let OrdersQueryResultV1::Page(page) = &responses[0].result else {
            panic!("盘后未判定开放订单必须仍可列表查询");
        };
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].platform_order_id, platform_order_id);
    }

    let cancel_sent_at = response_received_at(&orders_query.receipt, orders_query_at) + 1;
    let cancel_request = CancelOrderRequestV1 {
        context: context(&session, "cancel-after-end", 4, cancel_sent_at),
        idempotency_key: "cancel-active-idem".to_owned(),
        item: CancelOrderItemV1 {
            item_id: "cancel-active-item".to_owned(),
            platform_order_id: platform_order_id.clone(),
        },
    };
    let cancel = session
        .cancel_order(cancel_request.clone())
        .expect("盘后撤单登记");
    let cancel_at = cancel.item.effective_processing_at_utc_ms;
    assert!(cancel_at >= market().window.end_at_utc_ms);
    close_platform_at(&mut session, cancel_at);
    drive_and_deliver(&mut session, cancel_at, &cancel.receipt);
    assert_eq!(session.query_projection_revision(), 2);
    assert_eq!(session.available_pusd(), decimal("1000"));
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);
    assert_eq!(
        session.account_overlay_quantity(
            &market().condition_id.0,
            "1",
            BookSide::Bid,
            decimal("0.4"),
        ),
        Decimal::ZERO
    );
    let canceled = session
        .simulated_order(&platform_order_id)
        .expect("撤单后订单保留");
    assert_eq!(canceled.canceled_base_units, 5_000_000);
    assert_eq!(canceled.canceled_released_quote_units, 2_000_000);
    assert_eq!(canceled.open_base_units, 0);
    assert_eq!(canceled.open_quote_units, 0);

    let retry_sent_at = response_received_at(&cancel.receipt, cancel_at) + 1;
    let retry_request = CancelOrderRequestV1 {
        context: context(&session, "cancel-after-end-reuse", 5, retry_sent_at),
        ..cancel_request
    };
    let retry = session
        .cancel_order(retry_request)
        .expect("跨请求撤单幂等复用登记");
    let retry_at = retry.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, retry_at);
    drive_and_deliver(&mut session, retry_at, &retry.receipt);
    assert_eq!(session.query_projection_revision(), 2);
    assert_eq!(session.available_pusd(), decimal("1000"));
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);
    let responses = captures.cancel_order.lock().expect("单撤响应锁");
    assert_eq!(responses.len(), 2);
    assert!(matches!(
        responses[0].target.outcome,
        CancelTargetOutcomeV1::Canceled(_)
    ));
    assert_eq!(
        responses[1].target.cancel_effect_disposition,
        CancelEffectDispositionV1::ExistingEffect
    );
    assert_eq!(
        responses[0].target.cancel_effect_identity,
        responses[1].target.cancel_effect_identity
    );
    assert_eq!(
        responses[0].target.cancel_effect_formed_at_utc_ms,
        responses[1].target.cancel_effect_formed_at_utc_ms
    );
    assert_eq!(responses[0].target.outcome, responses[1].target.outcome);
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_moves_pending_and_open_base_but_releases_only_open_quote() {
    let (mut session, platform_order_id, _, _, effective) =
        session_with_one_pending_maker_depletion("task24-pending-cancel").await;
    let before = session
        .simulated_order(&platform_order_id)
        .expect("待累计订单");
    assert_eq!(before.pending_maker_depletion_base_units, 1);
    assert_eq!(before.open_base_units, 4_999_999);
    assert_eq!(before.open_quote_units, 50);
    assert!(before.pending_maker_depletion_evidence.is_some());

    let cancel = session
        .cancel_order(CancelOrderRequestV1 {
            context: context(&session, "cancel-pending", 2, effective + 10_000),
            idempotency_key: "cancel-pending-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "cancel-pending-item".to_owned(),
                platform_order_id: platform_order_id.clone(),
            },
        })
        .expect("待累计订单撤单登记");
    let cancel_at = cancel.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, cancel_at);
    drive_and_deliver(&mut session, cancel_at, &cancel.receipt);

    let canceled = session
        .simulated_order(&platform_order_id)
        .expect("待累计撤单终态");
    assert_eq!(canceled.canceled_base_units, 5_000_000);
    assert_eq!(canceled.canceled_released_quote_units, 50);
    assert_eq!(canceled.pending_maker_depletion_base_units, 0);
    assert_eq!(canceled.open_base_units, 0);
    assert_eq!(canceled.open_quote_units, 0);
    assert!(canceled.pending_maker_depletion_evidence.is_none());
    assert!(
        canceled
            .terminalized_pending_maker_depletion_evidence
            .is_some()
    );
    assert_eq!(session.available_pusd(), decimal("10"));
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);
    assert_eq!(session.immediate_taker_fee_encumbered_pusd(), Decimal::ZERO);
    assert_eq!(
        session.account_overlay_quantity(
            &market().condition_id.0,
            "1",
            BookSide::Bid,
            decimal("0.00001"),
        ),
        Decimal::ZERO
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_preserves_partial_fill_and_taker_fee_while_releasing_only_open_principal() {
    let captures = Task24Captures::default();
    let (mut session, profile) =
        ready_long_session_with_bridge("task24-partial-fill-cancel", 2, captures.bridge()).await;
    let post = session
        .post_order(PostOrderRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "partial-fill-source",
                1,
                130_000,
            ),
            idempotency_key: "partial-fill-source-idem".to_owned(),
            item: PostOrderItemV1 {
                item_id: "partial-fill-source-item".to_owned(),
                order: SimBuyOrderV1 {
                    condition_id: long_market().condition_id,
                    token_id: long_asset("1", 0, "Up").token_id,
                    kind: SimBuyOrderKindV1::GtcLimit {
                        limit_price_pusd_per_share: decimal("0.6"),
                        size_shares: decimal("10"),
                        post_only: false,
                    },
                },
            },
        })
        .expect("部分成交来源订单登记");
    let post_at = post.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, post_at);
    drive_and_deliver(&mut session, post_at, &post.receipt);
    let platform_order_id = {
        let responses = captures.post_order.lock().expect("提交响应锁");
        let SubmissionItemOutcomeV1::Accepted(accepted) = &responses[0].item.outcome else {
            panic!("部分成交来源订单必须接受");
        };
        accepted.simulated_platform_order_id.clone()
    };
    let before = session
        .simulated_order(&platform_order_id)
        .expect("部分成交订单状态")
        .clone();
    let fill_before = session
        .simulated_fill(&platform_order_id)
        .expect("部分成交明细")
        .clone();
    assert_eq!(before.matched_base_units, 5_000_000);
    assert_eq!(before.open_base_units, 5_000_000);
    assert_eq!(before.matched_quote_units, 3_000_000);
    assert_eq!(before.open_quote_units, 3_000_000);
    assert!(before.immediate_taker_fee_encumbered_units > 0);
    let available_before = session.available_pusd();
    let principal_before = session.principal_encumbered_pusd();

    let cancel = session
        .cancel_order(CancelOrderRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "partial-fill-cancel",
                2,
                140_000,
            ),
            idempotency_key: "partial-fill-cancel-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "partial-fill-cancel-item".to_owned(),
                platform_order_id: platform_order_id.clone(),
            },
        })
        .expect("部分成交余量撤单登记");
    let cancel_at = cancel.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, cancel_at);
    drive_and_deliver(&mut session, cancel_at, &cancel.receipt);

    let after = session
        .simulated_order(&platform_order_id)
        .expect("撤单后订单状态");
    assert_eq!(after.matched_base_units, before.matched_base_units);
    assert_eq!(after.matched_quote_units, before.matched_quote_units);
    assert_eq!(
        after.immediate_taker_fee_encumbered_units,
        before.immediate_taker_fee_encumbered_units
    );
    assert_eq!(after.canceled_base_units, 5_000_000);
    assert_eq!(after.canceled_released_quote_units, 3_000_000);
    assert_eq!(after.open_base_units, 0);
    assert_eq!(after.open_quote_units, 0);
    assert_eq!(session.simulated_fill_count(&platform_order_id), 1);
    assert_eq!(
        session
            .simulated_fill(&platform_order_id)
            .expect("撤单后成交仍存在"),
        &fill_before
    );
    assert_eq!(session.available_pusd(), available_before + decimal("3"));
    assert_eq!(
        session.principal_encumbered_pusd(),
        principal_before - decimal("3")
    );
    let responses = captures.cancel_order.lock().expect("撤单响应锁");
    let CancelTargetOutcomeV1::Canceled(measurement) = &responses[0].target.outcome else {
        panic!("部分成交余量必须成功撤销");
    };
    assert_eq!(measurement.canceled_base_quantity_shares, decimal("5"));
    assert_eq!(measurement.released_quote_amount_pusd, decimal("3"));
}

#[tokio::test(flavor = "current_thread")]
async fn cross_session_cancel_does_not_reveal_or_mutate_the_owning_session() {
    let owner_captures = Task24Captures::default();
    let foreign_captures = Task24Captures::default();
    let (mut owner, mut foreign, profile) = ready_long_session_pair_with_bridges(
        "task24-cancel-owner",
        owner_captures.bridge(),
        "task24-cancel-foreign",
        foreign_captures.bridge(),
    )
    .await;
    let (order_ids, _) = post_long_batch(
        &mut owner,
        &profile,
        &owner_captures,
        "cross-session-owner-order",
        1,
        130_000,
        1,
    );
    let platform_order_id = order_ids[0].clone();
    let owner_order_before = owner
        .simulated_order(&platform_order_id)
        .expect("归属会话订单")
        .clone();
    let owner_available_before = owner.available_pusd();
    let owner_principal_before = owner.principal_encumbered_pusd();
    let owner_revision_before = owner.query_projection_revision();

    let cancel = foreign
        .cancel_order(CancelOrderRequestV1 {
            context: long_context_for_arrival(
                &foreign,
                &profile,
                "cross-session-cancel",
                1,
                140_000,
            ),
            idempotency_key: "cross-session-cancel-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "cross-session-cancel-item".to_owned(),
                platform_order_id: platform_order_id.clone(),
            },
        })
        .expect("跨会话撤单登记");
    let cancel_at = cancel.item.effective_processing_at_utc_ms;
    close_platform_at(&mut foreign, cancel_at);
    drive_and_deliver(&mut foreign, cancel_at, &cancel.receipt);

    assert_eq!(
        foreign_captures.cancel_order.lock().expect("跨会话响应锁")[0]
            .target
            .outcome,
        CancelTargetOutcomeV1::NotCanceledNoEffect(CancelNotCanceledReasonV1::NotFoundOrNotOwned,)
    );
    assert_eq!(
        owner
            .simulated_order(&platform_order_id)
            .expect("归属会话订单仍存在"),
        &owner_order_before
    );
    assert_eq!(owner.available_pusd(), owner_available_before);
    assert_eq!(owner.principal_encumbered_pusd(), owner_principal_before);
    assert_eq!(owner.query_projection_revision(), owner_revision_before);
    assert_eq!(foreign.query_projection_revision(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_orders_deduplicates_targets_and_commits_mixed_results_once() {
    let captures = Task24Captures::default();
    let (mut session, profile) =
        ready_long_session_with_bridge("task24-cancel-batch", 2, captures.bridge()).await;
    let (order_ids, _) = post_long_batch(
        &mut session,
        &profile,
        &captures,
        "cancel-batch-orders",
        1,
        130_000,
        2,
    );
    let missing_id = "f".repeat(64);
    assert_eq!(session.query_projection_revision(), 2);
    assert_eq!(session.available_pusd(), decimal("996"));

    let registration = session
        .cancel_orders(CancelOrdersRequestV1 {
            context: long_context_for_arrival(&session, &profile, "cancel-batch", 2, 140_000),
            idempotency_key: "cancel-batch-idem".to_owned(),
            items: vec![
                CancelOrderItemV1 {
                    item_id: "cancel-a-first".to_owned(),
                    platform_order_id: order_ids[0].clone(),
                },
                CancelOrderItemV1 {
                    item_id: "cancel-a-duplicate".to_owned(),
                    platform_order_id: order_ids[0].clone(),
                },
                CancelOrderItemV1 {
                    item_id: "cancel-missing".to_owned(),
                    platform_order_id: missing_id.clone(),
                },
                CancelOrderItemV1 {
                    item_id: "cancel-b".to_owned(),
                    platform_order_id: order_ids[1].clone(),
                },
            ],
        })
        .expect("混合批撤登记");
    assert_eq!(registration.receipt.scheduled_action_count, 3);
    assert_eq!(
        registration
            .items
            .iter()
            .map(|item| item.scheduled_action_ordinal)
            .collect::<Vec<_>>(),
        vec![0, 0, 1, 2]
    );
    let effective_at = registration.items[0].effective_processing_at_utc_ms;
    close_platform_at(&mut session, effective_at);
    assert_eq!(
        session.drive_next_execution_action_through(effective_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    assert_eq!(
        session.drive_next_execution_action_through(effective_at),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    assert_eq!(session.query_projection_revision(), 3);
    assert_eq!(session.available_pusd(), decimal("1000"));
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);
    assert_eq!(
        session.drive_direct_response_delivery_through(response_received_at(
            &registration.receipt,
            effective_at,
        )),
        Ok(SubmissionDeliveryDispositionV1::Delivered)
    );

    let responses = captures.cancel_orders.lock().expect("批撤响应锁");
    assert_eq!(responses.len(), 1);
    let targets = &responses[0].targets;
    assert_eq!(targets.len(), 3);
    assert_eq!(targets[0].platform_order_id, order_ids[0]);
    assert_eq!(targets[0].target_ordinal, 0);
    assert_eq!(
        targets[0]
            .input_item_refs
            .iter()
            .map(|item| (item.item_id.as_str(), item.input_item_ordinal))
            .collect::<Vec<_>>(),
        vec![("cancel-a-first", 0), ("cancel-a-duplicate", 1)]
    );
    assert!(matches!(
        targets[0].outcome,
        CancelTargetOutcomeV1::Canceled(_)
    ));
    assert_eq!(targets[1].platform_order_id, missing_id);
    assert_eq!(
        targets[1].outcome,
        CancelTargetOutcomeV1::NotCanceledNoEffect(CancelNotCanceledReasonV1::NotFoundOrNotOwned,)
    );
    assert_eq!(targets[2].platform_order_id, order_ids[1]);
    assert!(matches!(
        targets[2].outcome,
        CancelTargetOutcomeV1::Canceled(_)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn fatal_revision_exhaustion_rolls_back_cancel_and_closes_only_new_ingress() {
    let captures = Task24Captures::default();
    let (mut session, profile) =
        ready_long_session_with_bridge("task24-fatal-cancel", 2, captures.bridge()).await;
    let (order_ids, _) = post_long_batch(
        &mut session,
        &profile,
        &captures,
        "fatal-cancel-source",
        1,
        130_000,
        2,
    );
    let cancel_request = CancelOrdersRequestV1 {
        context: long_context_for_arrival(&session, &profile, "fatal-cancel", 2, 140_000),
        idempotency_key: "fatal-cancel-idem".to_owned(),
        items: order_ids
            .iter()
            .enumerate()
            .map(|(ordinal, platform_order_id)| CancelOrderItemV1 {
                item_id: format!("fatal-cancel-item-{ordinal}"),
                platform_order_id: platform_order_id.clone(),
            })
            .collect(),
    };
    let registration = session
        .cancel_orders(cancel_request.clone())
        .expect("致命边界批撤登记");
    let effective_at = registration.items[0].effective_processing_at_utc_ms;
    let orders_before = order_ids
        .iter()
        .map(|order_id| {
            session
                .simulated_order(order_id)
                .expect("致命错误前订单")
                .clone()
        })
        .collect::<Vec<_>>();
    let available_before = session.available_pusd();
    let principal_before = session.principal_encumbered_pusd();
    close_platform_at(&mut session, effective_at);
    session.exhaust_query_projection_revision_for_test();
    assert_eq!(
        session.drive_next_execution_action_through(effective_at),
        Err(crate::execution::SubmissionProcessingErrorV1::Session(
            SimExecutionContractErrorV1::PlatformRevisionRegression,
        ))
    );
    for (order_id, before) in order_ids.iter().zip(&orders_before) {
        assert_eq!(
            session
                .simulated_order(order_id)
                .expect("致命错误后订单仍存在"),
            before
        );
    }
    assert_eq!(session.available_pusd(), available_before);
    assert_eq!(session.principal_encumbered_pusd(), principal_before);
    assert!(
        captures
            .cancel_orders
            .lock()
            .expect("批撤响应锁")
            .is_empty()
    );
    let retry = session
        .cancel_orders(cancel_request)
        .expect("已有请求物理重投仍可登记");
    assert_eq!(
        retry.receipt.disposition,
        RequestRegistrationDispositionV1::PhysicalRetry
    );
    assert_eq!(retry.receipt.transit, registration.receipt.transit);
    assert_eq!(retry.receipt.scheduled_action_count, 2);
    assert_eq!(retry.items, registration.items);
    assert_eq!(
        session.cancel_order(CancelOrderRequestV1 {
            context: long_context_for_arrival(&session, &profile, "fatal-cancel-new", 3, 150_000,),
            idempotency_key: "fatal-cancel-new-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "fatal-cancel-new-item".to_owned(),
                platform_order_id: "d".repeat(64),
            },
        }),
        Err(SimExecutionContractErrorV1::ResponseBridgeClosed)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn gtd_expiration_at_the_same_millisecond_precedes_cancel_without_double_release() {
    let captures = Task24Captures::default();
    let (mut session, profile) =
        ready_long_session_with_bridge("task24-gtd-cancel", 2, captures.bridge()).await;
    let post = session
        .post_order(PostOrderRequestV1 {
            context: long_context_for_arrival(&session, &profile, "gtd-before-cancel", 1, 150_000),
            idempotency_key: "gtd-before-cancel-idem".to_owned(),
            item: PostOrderItemV1 {
                item_id: "gtd-before-cancel-item".to_owned(),
                order: SimBuyOrderV1 {
                    condition_id: long_market().condition_id,
                    token_id: long_asset("1", 0, "Up").token_id,
                    kind: SimBuyOrderKindV1::GtdLimit {
                        limit_price_pusd_per_share: decimal("0.4"),
                        size_shares: decimal("5"),
                        post_only: true,
                        expires_at_utc_ms: 391_999,
                    },
                },
            },
        })
        .expect("GTD 订单登记");
    let post_at = post.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, post_at);
    drive_and_deliver(&mut session, post_at, &post.receipt);
    let platform_order_id = {
        let responses = captures.post_order.lock().expect("提交响应锁");
        let SubmissionItemOutcomeV1::Accepted(accepted) = &responses[0].item.outcome else {
            panic!("GTD 订单必须接受");
        };
        accepted.simulated_platform_order_id.clone()
    };
    assert_eq!(session.available_pusd(), decimal("998"));

    let cancel = session
        .cancel_order(CancelOrderRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "cancel-at-gtd-expiry",
                2,
                391_000,
            ),
            idempotency_key: "cancel-at-gtd-expiry-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "cancel-at-gtd-expiry-item".to_owned(),
                platform_order_id: platform_order_id.clone(),
            },
        })
        .expect("GTD 到期同刻撤单登记");
    let cancel_at = cancel.item.effective_processing_at_utc_ms;
    assert_eq!(cancel_at, 391_000);
    close_platform_at(&mut session, cancel_at);
    drive_and_deliver(&mut session, cancel_at, &cancel.receipt);

    assert_eq!(session.query_projection_revision(), 2);
    assert_eq!(session.available_pusd(), decimal("1000"));
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);
    let order = session
        .simulated_order(&platform_order_id)
        .expect("到期订单保留");
    assert_eq!(order.expired_base_units, 5_000_000);
    assert_eq!(order.expired_released_quote_units, 2_000_000);
    assert_eq!(order.canceled_base_units, 0);
    assert_eq!(order.open_base_units, 0);
    let responses = captures.cancel_order.lock().expect("单撤响应锁");
    assert_eq!(
        responses[0].target.outcome,
        CancelTargetOutcomeV1::NotCanceledNoEffect(
            CancelNotCanceledReasonV1::NoCancellableRemainder,
        )
    );
}

#[tokio::test(flavor = "current_thread")]
async fn order_query_observes_live_and_terminal_rows_and_distinguishes_retention_loss() {
    let captures = Task24Captures::default();
    let (mut session, profile) =
        ready_long_session_with_bridge("task24-order-query", 2, captures.bridge()).await;
    let (order_ids, _) = post_long_batch(
        &mut session,
        &profile,
        &captures,
        "order-query-source",
        1,
        130_000,
        1,
    );
    let platform_order_id = order_ids[0].clone();

    let live_query = session
        .order(OrderRequestV1 {
            context: long_context_for_arrival(&session, &profile, "order-query-live", 2, 140_000),
            platform_order_id: platform_order_id.clone(),
        })
        .expect("活动订单查询登记");
    let live_query_at = live_query.receipt.transit.platform_arrived_at_utc_ms;
    close_platform_at(&mut session, live_query_at);
    drive_and_deliver(&mut session, live_query_at, &live_query.receipt);
    let cancel = session
        .cancel_order(CancelOrderRequestV1 {
            context: long_context_for_arrival(&session, &profile, "order-query-cancel", 3, 150_000),
            idempotency_key: "order-query-cancel-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "order-query-cancel-item".to_owned(),
                platform_order_id: platform_order_id.clone(),
            },
        })
        .expect("查询夹具撤单登记");
    let cancel_at = cancel.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, cancel_at);
    drive_and_deliver(&mut session, cancel_at, &cancel.receipt);
    let terminal_query = session
        .order(OrderRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "order-query-terminal",
                4,
                160_000,
            ),
            platform_order_id: platform_order_id.clone(),
        })
        .expect("终态订单查询登记");
    let terminal_query_at = terminal_query.receipt.transit.platform_arrived_at_utc_ms;
    close_platform_at(&mut session, terminal_query_at);
    drive_and_deliver(&mut session, terminal_query_at, &terminal_query.receipt);
    session
        .advance_order_history_retention_floor(150_000)
        .expect("推进订单历史水位");
    let retained_query = session
        .order(OrderRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "order-query-retention",
                5,
                170_000,
            ),
            platform_order_id: "e".repeat(64),
        })
        .expect("订单保留范围查询登记");
    let retained_query_at = retained_query.receipt.transit.platform_arrived_at_utc_ms;
    close_platform_at(&mut session, retained_query_at);
    drive_and_deliver(&mut session, retained_query_at, &retained_query.receipt);

    let responses = captures.order.lock().expect("单订单查询响应锁");
    assert_eq!(responses.len(), 3);
    let OrderQueryResultV1::Found(live) = &responses[0].result else {
        panic!("活动订单必须可查");
    };
    assert_eq!(live.platform_order_id, platform_order_id);
    assert_eq!(live.status.known, Some(SimulatedOrderKnownStatusV1::Live));
    let OrderQueryResultV1::Found(canceled) = &responses[1].result else {
        panic!("撤单终态必须可查");
    };
    assert_eq!(
        canceled.status.known,
        Some(SimulatedOrderKnownStatusV1::Canceled)
    );
    assert_eq!(canceled.canceled_base_quantity_shares, decimal("5"));
    let OrderQueryResultV1::OutOfRetention(out_of_retention) = &responses[2].result else {
        panic!("缺失订单且历史不完整必须返回范围结果");
    };
    assert_eq!(out_of_retention.retained_from_utc_ms, Some(150_000));
    assert_eq!(session.query_projection_revision(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_all_freezes_the_sorted_target_set_and_empty_cancel_all_is_legal() {
    let captures = Task24Captures::default();
    let (mut session, profile) =
        ready_long_session_with_bridge("task24-cancel-all", 2, captures.bridge()).await;
    let (first_ids, _) = post_long_batch(
        &mut session,
        &profile,
        &captures,
        "cancel-all-initial",
        1,
        130_000,
        2,
    );
    let first_request = CancelAllOrdersRequestV1 {
        context: long_context_for_arrival(&session, &profile, "cancel-all-first", 2, 140_000),
        idempotency_key: "cancel-all-frozen-idem".to_owned(),
    };
    let first = session
        .cancel_all_orders(first_request.clone())
        .expect("首次全撤登记");
    let first_at = first.receipt.transit.platform_arrived_at_utc_ms;
    close_platform_at(&mut session, first_at);
    drive_and_deliver(&mut session, first_at, &first.receipt);
    assert_eq!(session.query_projection_revision(), 3);

    let (later_ids, _) = post_long_batch(
        &mut session,
        &profile,
        &captures,
        "cancel-all-later",
        3,
        150_000,
        1,
    );
    assert_eq!(session.query_projection_revision(), 4);
    assert_eq!(session.available_pusd(), decimal("998"));

    let reused = session
        .cancel_all_orders(CancelAllOrdersRequestV1 {
            context: long_context_for_arrival(&session, &profile, "cancel-all-reused", 4, 160_000),
            ..first_request
        })
        .expect("全撤幂等复用登记");
    let reused_at = reused.receipt.transit.platform_arrived_at_utc_ms;
    close_platform_at(&mut session, reused_at);
    drive_and_deliver(&mut session, reused_at, &reused.receipt);
    assert_eq!(session.query_projection_revision(), 4);
    assert_eq!(session.available_pusd(), decimal("998"));
    assert!(
        session
            .simulated_order(&later_ids[0])
            .is_some_and(|order| order.open_base_units > 0)
    );

    let later_cancel = session
        .cancel_all_orders(CancelAllOrdersRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "cancel-all-later-target",
                5,
                170_000,
            ),
            idempotency_key: "cancel-all-later-target-idem".to_owned(),
        })
        .expect("撤销后来订单");
    let later_cancel_at = later_cancel.receipt.transit.platform_arrived_at_utc_ms;
    close_platform_at(&mut session, later_cancel_at);
    drive_and_deliver(&mut session, later_cancel_at, &later_cancel.receipt);
    let revision_after_last_effect = session.query_projection_revision();
    let empty = session
        .cancel_all_orders(CancelAllOrdersRequestV1 {
            context: long_context_for_arrival(&session, &profile, "cancel-all-empty", 6, 180_000),
            idempotency_key: "cancel-all-empty-idem".to_owned(),
        })
        .expect("空全撤登记");
    let empty_at = empty.receipt.transit.platform_arrived_at_utc_ms;
    close_platform_at(&mut session, empty_at);
    drive_and_deliver(&mut session, empty_at, &empty.receipt);
    assert_eq!(
        session.query_projection_revision(),
        revision_after_last_effect
    );

    let responses = captures.cancel_all_orders.lock().expect("全撤响应锁");
    assert_eq!(responses.len(), 4);
    let mut expected_first_ids = first_ids;
    expected_first_ids.sort();
    assert_eq!(
        responses[0]
            .targets
            .iter()
            .map(|target| target.platform_order_id.clone())
            .collect::<Vec<_>>(),
        expected_first_ids
    );
    assert_eq!(responses[0].targets.len(), responses[1].targets.len());
    for (first_target, reused_target) in responses[0].targets.iter().zip(&responses[1].targets) {
        assert_eq!(
            first_target.platform_order_id,
            reused_target.platform_order_id
        );
        assert_eq!(
            reused_target.cancel_effect_disposition,
            CancelEffectDispositionV1::ExistingEffect
        );
        assert_eq!(
            first_target.cancel_effect_identity,
            reused_target.cancel_effect_identity
        );
        assert_eq!(first_target.outcome, reused_target.outcome);
    }
    assert_eq!(responses[2].targets.len(), 1);
    assert_eq!(responses[2].targets[0].platform_order_id, later_ids[0]);
    assert!(responses[3].targets.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn orders_pages_freeze_the_root_and_reject_cursor_misuse_or_pruned_roots() {
    let captures = Task24Captures::default();
    let (mut session, profile) =
        ready_long_session_with_bridge("task24-orders-pages", 2, captures.bridge()).await;
    let mut all_order_ids = Vec::new();
    for batch in 0..7_u64 {
        let item_count = if batch == 6 { 11 } else { 15 };
        let (order_ids, _) = post_long_batch(
            &mut session,
            &profile,
            &captures,
            &format!("page-orders-{batch}"),
            batch + 1,
            130_000 + i64::try_from(batch).expect("测试批次适合 i64") * 10_000,
            item_count,
        );
        all_order_ids.extend(order_ids);
    }
    assert_eq!(all_order_ids.len(), 101);
    assert_eq!(session.query_projection_revision(), 101);

    let first_query = session
        .orders(OrdersRequestV1 {
            context: long_context_for_arrival(&session, &profile, "orders-page-1", 8, 210_000),
            filter: OrdersFilterV1::default(),
            cursor: None,
        })
        .expect("订单列表第一页登记");
    let first_query_at = first_query.receipt.transit.platform_arrived_at_utc_ms;
    close_platform_at(&mut session, first_query_at);
    drive_and_deliver(&mut session, first_query_at, &first_query.receipt);
    assert_eq!(session.query_projection_revision(), 101);
    let first_page = {
        let responses = captures.orders.lock().expect("订单列表响应锁");
        let OrdersQueryResultV1::Page(page) = &responses[0].result else {
            panic!("第一页必须成功");
        };
        page.clone()
    };
    assert_eq!(first_page.items.len(), 100);
    assert!(first_page.next_cursor.is_some());
    assert!(first_page.items.windows(2).all(|rows| {
        (
            rows[0].created_at_utc_ms,
            rows[0].platform_order_id.as_str(),
        ) <= (
            rows[1].created_at_utc_ms,
            rows[1].platform_order_id.as_str(),
        )
    }));
    assert!(
        first_page
            .items
            .windows(2)
            .any(|rows| rows[0].created_at_utc_ms == rows[1].created_at_utc_ms)
    );
    let representative_page_bytes = orders_projection_canonical_byte_len(&first_page.items);
    eprintln!(
        "SIM_QUERY_PAGE_SIZE_V1=100 representative canonical projection bytes={representative_page_bytes}"
    );
    assert!((10_000..1_048_576).contains(&representative_page_bytes));
    let page_ids = first_page
        .items
        .iter()
        .map(|order| order.platform_order_id.clone())
        .collect::<BTreeSet<_>>();
    let second_page_order_id = all_order_ids
        .iter()
        .find(|order_id| !page_ids.contains(*order_id))
        .expect("恰有一个第二页订单")
        .clone();
    let first_cursor = first_page.next_cursor.clone().expect("第一页游标");

    let cancel = session
        .cancel_order(CancelOrderRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "cancel-page-2-order",
                9,
                220_000,
            ),
            idempotency_key: "cancel-page-2-order-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "cancel-page-2-order-item".to_owned(),
                platform_order_id: second_page_order_id.clone(),
            },
        })
        .expect("撤销第二页订单");
    let cancel_at = cancel.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, cancel_at);
    drive_and_deliver(&mut session, cancel_at, &cancel.receipt);
    assert_eq!(session.query_projection_revision(), 102);

    assert!(matches!(
        session.trades(TradesRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "orders-cursor-as-trades",
                10,
                230_000,
            ),
            filter: TradesFilterV1::default(),
            cursor: Some(first_cursor.clone()),
        }),
        Err(SimExecutionContractErrorV1::InvalidRequest(_))
    ));
    assert!(matches!(
        session.orders(OrdersRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "orders-cursor-wrong-filter",
                10,
                230_000,
            ),
            filter: OrdersFilterV1 {
                condition_id: None,
                token_id: Some(long_asset("2", 1, "Down").token_id),
            },
            cursor: Some(first_page.next_cursor.clone().expect("第一页游标")),
        }),
        Err(SimExecutionContractErrorV1::InvalidRequest(_))
    ));
    let continuation = session
        .orders(OrdersRequestV1 {
            context: long_context_for_arrival(&session, &profile, "orders-page-2", 10, 230_000),
            filter: OrdersFilterV1::default(),
            cursor: Some(first_cursor),
        })
        .expect("旧根第二页登记");
    let continuation_at = continuation.receipt.transit.platform_arrived_at_utc_ms;
    close_platform_at(&mut session, continuation_at);
    drive_and_deliver(&mut session, continuation_at, &continuation.receipt);
    let second_page = {
        let responses = captures.orders.lock().expect("订单列表响应锁");
        let OrdersQueryResultV1::Page(page) = &responses[1].result else {
            panic!("旧根第二页必须成功");
        };
        page.clone()
    };
    assert_eq!(second_page.root_identity, first_page.root_identity);
    assert_eq!(second_page.query_projection_revision, 101);
    assert_eq!(second_page.items.len(), 1);
    assert_eq!(second_page.items[0].platform_order_id, second_page_order_id);
    assert!(second_page.next_cursor.is_none());
    let mut paged_ids = first_page
        .items
        .iter()
        .map(|order| order.platform_order_id.clone())
        .collect::<Vec<_>>();
    paged_ids.extend(
        second_page
            .items
            .iter()
            .map(|order| order.platform_order_id.clone()),
    );
    assert_eq!(paged_ids.len(), 101);
    assert_eq!(
        paged_ids.iter().cloned().collect::<BTreeSet<_>>().len(),
        101
    );
    assert_eq!(
        paged_ids.iter().cloned().collect::<BTreeSet<_>>(),
        all_order_ids.iter().cloned().collect::<BTreeSet<_>>()
    );
    let first_last = first_page.items.last().expect("第一页末项");
    let second_first = second_page.items.first().expect("第二页首项");
    assert!(
        (
            first_last.created_at_utc_ms,
            first_last.platform_order_id.as_str()
        ) < (
            second_first.created_at_utc_ms,
            second_first.platform_order_id.as_str(),
        )
    );
    assert_eq!(session.query_projection_revision(), 102);

    let current_query = session
        .orders(OrdersRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "orders-current-root",
                11,
                240_000,
            ),
            filter: OrdersFilterV1::default(),
            cursor: None,
        })
        .expect("当前订单根登记");
    let current_query_at = current_query.receipt.transit.platform_arrived_at_utc_ms;
    close_platform_at(&mut session, current_query_at);
    drive_and_deliver(&mut session, current_query_at, &current_query.receipt);
    {
        let responses = captures.orders.lock().expect("订单列表响应锁");
        let OrdersQueryResultV1::Page(page) = &responses[2].result else {
            panic!("当前根必须成功");
        };
        assert_eq!(page.items.len(), 100);
        assert!(page.next_cursor.is_none());
        assert!(
            page.items
                .iter()
                .all(|order| order.platform_order_id != second_page_order_id)
        );
        assert_eq!(page.query_projection_revision, 102);
    }

    let filtered_query = session
        .orders(OrdersRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "orders-filtered-empty",
                12,
                245_000,
            ),
            filter: OrdersFilterV1 {
                condition_id: Some(long_market().condition_id),
                token_id: Some(long_asset("2", 1, "Down").token_id),
            },
            cursor: None,
        })
        .expect("订单过滤空页登记");
    let filtered_query_at = filtered_query.receipt.transit.platform_arrived_at_utc_ms;
    close_platform_at(&mut session, filtered_query_at);
    drive_and_deliver(&mut session, filtered_query_at, &filtered_query.receipt);
    {
        let responses = captures.orders.lock().expect("订单列表响应锁");
        let OrdersQueryResultV1::Page(page) = &responses[3].result else {
            panic!("合法无匹配过滤必须成功");
        };
        assert!(page.items.is_empty());
        assert!(page.next_cursor.is_none());
    }

    post_long_batch(
        &mut session,
        &profile,
        &captures,
        "orders-after-current-root",
        13,
        250_000,
        1,
    );
    assert_eq!(session.query_projection_revision(), 103);
    let retained_root = session
        .orders(OrdersRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "orders-retained-root",
                14,
                260_000,
            ),
            filter: OrdersFilterV1::default(),
            cursor: None,
        })
        .expect("待裁剪根登记");
    let retained_root_at = retained_root.receipt.transit.platform_arrived_at_utc_ms;
    close_platform_at(&mut session, retained_root_at);
    drive_and_deliver(&mut session, retained_root_at, &retained_root.receipt);
    let retained_cursor = {
        let responses = captures.orders.lock().expect("订单列表响应锁");
        let OrdersQueryResultV1::Page(page) = &responses[4].result else {
            panic!("待裁剪根必须成功");
        };
        page.next_cursor.clone().expect("待裁剪根游标")
    };
    let retained_continuation = session
        .orders(OrdersRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "orders-pruned-continuation",
                15,
                270_000,
            ),
            filter: OrdersFilterV1::default(),
            cursor: Some(retained_cursor),
        })
        .expect("裁剪前续页登记");
    let new_floor = session.next_query_root_sequence();
    session
        .advance_query_root_retention_floor(new_floor)
        .expect("推进根保留水位");
    let retained_continuation_at = retained_continuation
        .receipt
        .transit
        .platform_arrived_at_utc_ms;
    close_platform_at(&mut session, retained_continuation_at);
    drive_and_deliver(
        &mut session,
        retained_continuation_at,
        &retained_continuation.receipt,
    );
    let responses = captures.orders.lock().expect("订单列表响应锁");
    let OrdersQueryResultV1::OutOfRetention(out_of_retention) = &responses[5].result else {
        panic!("已登记后裁剪必须返回范围结果");
    };
    assert_eq!(out_of_retention.earliest_retained_root_sequence, new_floor);
    assert!(
        out_of_retention
            .requested_root_sequence
            .is_some_and(|sequence| sequence < new_floor)
    );
    assert_eq!(session.query_projection_revision(), 103);
}

#[tokio::test(flavor = "current_thread")]
async fn trades_use_strict_discrete_millisecond_bounds_and_independent_retention() {
    let captures = Task24Captures::default();
    let (mut session, profile) =
        ready_long_session_with_bridge("task24-trades", 2, captures.bridge()).await;
    let second_ask = long_ask_transition(120_000, 120_100, 3, "0.5", "0", "5");
    let start = session.platform_event_index_position();
    session
        .apply_closed_market_event_bucket(long_hidden_bucket(
            120_000,
            start,
            start + 1,
            &[second_ask],
        ))
        .expect("加入第二个可执行卖价");
    let post = session
        .post_order(PostOrderRequestV1 {
            context: long_context_for_arrival(&session, &profile, "trade-source-order", 1, 130_000),
            idempotency_key: "trade-source-order-idem".to_owned(),
            item: PostOrderItemV1 {
                item_id: "trade-source-order-item".to_owned(),
                order: SimBuyOrderV1 {
                    condition_id: long_market().condition_id,
                    token_id: long_asset("1", 0, "Up").token_id,
                    kind: SimBuyOrderKindV1::FakMarket {
                        spend_pusd: decimal("5.5"),
                        worst_price_pusd_per_share: decimal("0.6"),
                    },
                },
            },
        })
        .expect("成交来源订单登记");
    let post_at = post.item.effective_processing_at_utc_ms;
    close_platform_at(&mut session, post_at);
    drive_and_deliver(&mut session, post_at, &post.receipt);
    let platform_order_id = {
        let responses = captures.post_order.lock().expect("提交响应锁");
        let SubmissionItemOutcomeV1::Accepted(accepted) = &responses[0].item.outcome else {
            panic!("成交来源订单必须接受");
        };
        accepted.simulated_platform_order_id.clone()
    };
    assert_eq!(session.simulated_fill_count(&platform_order_id), 2);
    let matched_at = post_at;
    assert_eq!(session.query_projection_revision(), 1);
    session
        .advance_trade_history_retention_floor(matched_at)
        .expect("推进成交历史水位");

    let within_retention = run_trades_query(
        &mut session,
        &profile,
        &captures,
        "trades-within-retention",
        2,
        140_000,
        TradesFilterV1 {
            after_utc_ms: Some(matched_at - 1),
            before_utc_ms: Some(matched_at + 1),
            ..TradesFilterV1::default()
        },
    );
    let TradesQueryResultV1::Page(within_page) = within_retention else {
        panic!("保留范围内查询必须成功");
    };
    assert_eq!(within_page.items.len(), 2);
    assert!(within_page.items.iter().all(|fill| {
        fill.platform_order_id == platform_order_id && fill.matched_at_utc_ms == matched_at
    }));
    assert!(
        within_page
            .items
            .windows(2)
            .all(|fills| fills[0].simulated_trade_id < fills[1].simulated_trade_id)
    );

    let exact_trade = run_trades_query(
        &mut session,
        &profile,
        &captures,
        "trades-exact-id",
        3,
        145_000,
        TradesFilterV1 {
            trade_id: Some(within_page.items[0].simulated_trade_id.clone()),
            condition_id: Some(long_market().condition_id),
            token_id: Some(long_asset("1", 0, "Up").token_id),
            after_utc_ms: None,
            before_utc_ms: None,
        },
    );
    let TradesQueryResultV1::Page(exact_page) = exact_trade else {
        panic!("精确成交过滤必须成功");
    };
    assert_eq!(exact_page.items.len(), 1);
    assert_eq!(
        exact_page.items[0].simulated_trade_id,
        within_page.items[0].simulated_trade_id
    );
    let wrong_trade = run_trades_query(
        &mut session,
        &profile,
        &captures,
        "trades-wrong-token",
        4,
        147_000,
        TradesFilterV1 {
            trade_id: Some(within_page.items[0].simulated_trade_id.clone()),
            condition_id: Some(long_market().condition_id),
            token_id: Some(long_asset("2", 1, "Down").token_id),
            after_utc_ms: Some(matched_at - 1),
            before_utc_ms: Some(matched_at + 1),
        },
    );
    let TradesQueryResultV1::Page(wrong_page) = wrong_trade else {
        panic!("无匹配成交过滤必须成功");
    };
    assert!(wrong_page.items.is_empty());

    let missing_exact_trade = run_trades_query(
        &mut session,
        &profile,
        &captures,
        "trades-missing-exact-id",
        5,
        149_000,
        TradesFilterV1 {
            trade_id: Some("f".repeat(64)),
            condition_id: None,
            token_id: None,
            after_utc_ms: None,
            before_utc_ms: None,
        },
    );
    assert!(matches!(
        missing_exact_trade,
        TradesQueryResultV1::OutOfRetention(_)
    ));

    for (ordinal, filter) in [
        TradesFilterV1 {
            after_utc_ms: Some(matched_at - 1),
            before_utc_ms: Some(matched_at),
            ..TradesFilterV1::default()
        },
        TradesFilterV1 {
            after_utc_ms: Some(matched_at),
            before_utc_ms: Some(matched_at + 1),
            ..TradesFilterV1::default()
        },
        TradesFilterV1 {
            after_utc_ms: Some(matched_at),
            before_utc_ms: Some(matched_at),
            ..TradesFilterV1::default()
        },
    ]
    .into_iter()
    .enumerate()
    {
        let result = run_trades_query(
            &mut session,
            &profile,
            &captures,
            &format!("trades-empty-{ordinal}"),
            6 + u64::try_from(ordinal).expect("测试序号适合 u64"),
            150_000 + i64::try_from(ordinal).expect("测试序号适合 i64") * 10_000,
            filter,
        );
        let TradesQueryResultV1::Page(page) = result else {
            panic!("离散毫秒空区间必须成功");
        };
        assert!(page.items.is_empty());
        assert!(page.next_cursor.is_none());
    }

    let out_of_retention = run_trades_query(
        &mut session,
        &profile,
        &captures,
        "trades-out-of-retention",
        9,
        180_000,
        TradesFilterV1 {
            after_utc_ms: Some(matched_at - 2),
            before_utc_ms: Some(matched_at + 1),
            ..TradesFilterV1::default()
        },
    );
    let TradesQueryResultV1::OutOfRetention(out_of_retention) = out_of_retention else {
        panic!("跨成交保留水位必须返回范围结果");
    };
    assert_eq!(out_of_retention.retained_from_utc_ms, Some(matched_at));

    assert!(matches!(
        session.trades(TradesRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "trades-negative-time",
                10,
                190_000,
            ),
            filter: TradesFilterV1 {
                after_utc_ms: Some(-1),
                ..TradesFilterV1::default()
            },
            cursor: None,
        }),
        Err(SimExecutionContractErrorV1::InvalidRequest(_))
    ));
    assert!(matches!(
        session.trades(TradesRequestV1 {
            context: long_context_for_arrival(
                &session,
                &profile,
                "trades-reversed-time",
                10,
                190_000,
            ),
            filter: TradesFilterV1 {
                after_utc_ms: Some(matched_at + 1),
                before_utc_ms: Some(matched_at),
                ..TradesFilterV1::default()
            },
            cursor: None,
        }),
        Err(SimExecutionContractErrorV1::InvalidRequest(_))
    ));
    assert_eq!(session.query_projection_revision(), 1);
}
