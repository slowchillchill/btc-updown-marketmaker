use std::sync::Arc;

use contracts::{
    ClobMarketId, ConditionId, ExecutionMarketEventPayloadV3, MarketEnrollmentIdentityV1,
    MarketIdentityV2, MarketWindowV2, Sha256Hex, TokenId,
};
use data_pipeline_core::{derive_execution_market_event_v3_id, encode_execution_market_event_v3};

use crate::execution::{
    CancelAllOrdersRequestV1, MarketEnrollmentRequestV1, OrderRequestV1, OrdersFilterV1,
    OrdersRequestV1, PlatformMarketV1, PostOrderItemV1, PostOrderRequestV1, SimBuyOrderKindV1,
    SimBuyOrderV1, SimCallerRoleV1, SimExecutionProcessV1, SimExecutionSessionV1, TradesFilterV1,
    TradesRequestV1, accepting_submission_response_bridge, execution_process_for_tests,
};
use crate::latency_profile::{EmpiricalDelayProfile, load_embedded_proxy_latency_profile};
use crate::market::{
    ClosedMarketEventBucketV1, MarketBucketApplyDispositionV1, MarketEventErrorV1,
    MillisecondBucketClosureV1,
};
use crate::platform_rules::tests::successful_snapshot_for_sim_tests;
use crate::session::{
    ActivateSessionRequestV1, FailSessionInitializationRequestV1, RequestContextV1,
};
use crate::task21_tests::{asset, initial_snapshot_event, scenario, session_spec};

fn candidate_market(
    condition_digit: char,
    clob_market_id: &str,
    first_token_id: &str,
    second_token_id: &str,
) -> PlatformMarketV1 {
    PlatformMarketV1::try_new(
        MarketIdentityV2 {
            condition_id: ConditionId(format!("0x{}", condition_digit.to_string().repeat(64))),
            clob_market_id: ClobMarketId(clob_market_id.to_owned()),
            window: MarketWindowV2 {
                start_at_utc_ms: 100_000,
                end_at_utc_ms: 500_000,
            },
        },
        [
            asset(first_token_id, 0, "Up"),
            asset(second_token_id, 1, "Down"),
        ],
    )
    .expect("候选市场必须符合 PlatformMarketV1")
}

fn enrollment_request(id: &str, market: PlatformMarketV1) -> MarketEnrollmentRequestV1 {
    MarketEnrollmentRequestV1 {
        enrollment_request_id: id.to_owned(),
        market,
    }
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
        caller_role: spec.caller_role,
        caller_instance_id: spec.caller_instance_id.clone(),
        global_request_id: request_id.to_owned(),
        caller_correlation_id: Some(format!("correlation-{request_id}")),
        caller_sequence,
        request_sent_at_utc_ms,
        model_visible_cursor: session.model_visible_cursor(),
    }
}

fn candidate_post_request(
    session: &SimExecutionSessionV1,
    request_id: &str,
    caller_sequence: u64,
    request_sent_at_utc_ms: i64,
    condition_id: ConditionId,
    token_id: TokenId,
) -> PostOrderRequestV1 {
    PostOrderRequestV1 {
        context: request_context(session, request_id, caller_sequence, request_sent_at_utc_ms),
        idempotency_key: format!("idem-{request_id}"),
        item: PostOrderItemV1 {
            item_id: format!("item-{request_id}"),
            order: SimBuyOrderV1 {
                condition_id,
                token_id,
                kind: SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: "0.5".parse().expect("测试价格"),
                    size_shares: "10".parse().expect("测试数量"),
                    post_only: false,
                },
            },
        },
    }
}

async fn active_session(
    role: SimCallerRoleV1,
    id: &str,
) -> (
    SimExecutionProcessV1,
    SimExecutionSessionV1,
    Arc<EmpiricalDelayProfile>,
) {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
    let scenario = scenario();
    let spec = session_spec(&scenario, role, id);
    let mut process = execution_process_for_tests(rules, Arc::clone(&profile), scenario);
    let session =
        process.create_active_session_for_tests(spec, accepting_submission_response_bridge());
    (process, session, profile)
}

fn candidate_snapshots(market: &PlatformMarketV1) -> Vec<contracts::ExecutionMarketEventV3> {
    market
        .assets()
        .iter()
        .enumerate()
        .map(|(index, candidate_asset)| {
            let mut event = initial_snapshot_event(
                &candidate_asset.token_id.0,
                candidate_asset.outcome.index,
                &candidate_asset.outcome.label,
                110_000,
                120_000,
                u64::try_from(index + 1).expect("测试序号"),
                "10",
            );
            event.market = market.market().clone();
            if let ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) = &mut event.payload {
                snapshot.asset = candidate_asset.clone();
                snapshot.book_digest_sha256 =
                    Sha256Hex(if index == 0 { '7' } else { '8' }.to_string().repeat(64));
            } else {
                panic!("测试夹具必须是初始盘口快照");
            }
            event.event_id =
                derive_execution_market_event_v3_id(&event).expect("重算候选市场事件身份");
            event
        })
        .collect()
}

fn public_bucket(
    at: i64,
    start: u64,
    next: u64,
    events: &[contracts::ExecutionMarketEventV3],
) -> ClosedMarketEventBucketV1 {
    ClosedMarketEventBucketV1::public(
        at,
        start,
        next,
        MillisecondBucketClosureV1::NextEventAt(at + 1),
        events
            .iter()
            .map(|event| encode_execution_market_event_v3(event).expect("规范事件编码"))
            .collect::<Vec<_>>(),
    )
}

fn hidden_empty_bucket(at: i64, position: u64) -> ClosedMarketEventBucketV1 {
    ClosedMarketEventBucketV1::hidden(
        at,
        position,
        position,
        MillisecondBucketClosureV1::NextEventAt(at + 1),
        Vec::<Vec<u8>>::new(),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn task31_enrollment_is_active_only_idempotent_and_rejects_all_identity_reuse() {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
    let initial_scenario = scenario();
    let prepared_spec = session_spec(
        &initial_scenario,
        SimCallerRoleV1::Paper,
        "task31-active-gate",
    );
    let mut process = execution_process_for_tests(rules, profile, Arc::clone(&initial_scenario));
    let mut prepared = process
        .create_session(
            prepared_spec.clone(),
            accepting_submission_response_bridge(),
        )
        .expect("准备态会话");
    let candidate = candidate_market('2', "124", "3", "4");
    assert!(
        process
            .enroll_market(
                &mut prepared.session,
                enrollment_request("prepared-enrollment", candidate.clone()),
            )
            .is_err(),
        "准备态不能登记市场"
    );
    process
        .activate_session(ActivateSessionRequestV1 {
            session_id: prepared_spec.session_id.clone(),
            account_instance_key: prepared.control.account_instance_key.clone(),
            opening_anchor_id: prepared.control.opening_anchor.anchor_id.clone(),
            opening_anchor_sha256: prepared.control.opening_anchor.anchor_sha256.clone(),
            activation_id: "task31-activation".to_owned(),
        })
        .expect("激活会话");

    let available_before = prepared.session.available_pusd();
    let private_before = prepared
        .session
        .model_visible_cursor()
        .private_feedback_sequence;
    let first_request = enrollment_request("enrollment-1", candidate.clone());
    let first = process
        .enroll_market(&mut prepared.session, first_request.clone())
        .expect("首次登记");
    assert_eq!(first.enrollment_chain_length, 1);
    assert_eq!(first.previous_enrollment, None);
    assert_eq!(first.market, candidate.market().clone());
    assert_eq!(first.assets, candidate.assets().clone());
    assert_eq!(
        process
            .enroll_market(&mut prepared.session, first_request)
            .expect("相同强类型请求幂等重试"),
        first
    );
    assert_eq!(prepared.session.available_pusd(), available_before);
    assert_eq!(
        prepared
            .session
            .model_visible_cursor()
            .private_feedback_sequence,
        private_before,
        "同步控制结果不得进入私有反馈序列"
    );

    assert!(
        process
            .enroll_market(
                &mut prepared.session,
                enrollment_request("enrollment-1", candidate_market('3', "125", "5", "6"),),
            )
            .is_err(),
        "同一请求身份不得绑定不同 PlatformMarketV1"
    );
    for (id, duplicate) in [
        (
            "duplicate-condition",
            candidate_market('2', "126", "7", "8"),
        ),
        ("duplicate-clob", candidate_market('4', "124", "9", "10")),
        ("duplicate-token", candidate_market('5', "127", "4", "11")),
    ] {
        assert!(
            process
                .enroll_market(&mut prepared.session, enrollment_request(id, duplicate))
                .is_err(),
            "全目录身份复用必须失败：{id}"
        );
        assert_eq!(
            process
                .enroll_market(
                    &mut prepared.session,
                    enrollment_request("enrollment-1", candidate.clone()),
                )
                .expect("失败不得污染首次登记记录"),
            first
        );
    }
    for rejected_digit in ['3', '4', '5'] {
        let condition_id = format!("0x{}", rejected_digit.to_string().repeat(64));
        assert!(
            prepared
                .session
                .public_market_state(&condition_id)
                .is_none()
        );
    }
    let second = process
        .enroll_market(
            &mut prepared.session,
            enrollment_request("enrollment-2", candidate_market('8', "130", "16", "17")),
        )
        .expect("身份复用失败后仍可登记全新市场");
    assert_eq!(second.enrollment_chain_length, 2);
    assert_eq!(
        second.previous_enrollment,
        Some(MarketEnrollmentIdentityV1 {
            enrollment_id: first.enrollment_id.clone(),
            enrollment_sha256: first.enrollment_sha256.clone(),
        })
    );

    let failed_spec = session_spec(
        &initial_scenario,
        SimCallerRoleV1::Paper,
        "task31-failed-closed-gate",
    );
    let mut failed = process
        .create_session(failed_spec.clone(), accepting_submission_response_bridge())
        .expect("待失败关闭会话");
    process
        .fail_session_initialization(FailSessionInitializationRequestV1 {
            session_id: failed_spec.session_id,
            account_instance_key: failed.control.account_instance_key.clone(),
            opening_anchor_id: failed.control.opening_anchor.anchor_id.clone(),
            opening_anchor_sha256: failed.control.opening_anchor.anchor_sha256.clone(),
            failure_id: "task31-ledger-init-failed".to_owned(),
        })
        .expect("初始化明确失败关闭");
    assert!(
        process
            .enroll_market(
                &mut failed.session,
                enrollment_request(
                    "failed-closed-enrollment",
                    candidate_market('6', "128", "12", "13"),
                ),
            )
            .is_err(),
        "失败关闭会话不能登记市场"
    );

    let (root_a, _session_a, _) =
        active_session(SimCallerRoleV1::Paper, "task31-cross-root-owner").await;
    let (_root_b, mut session_b, _) =
        active_session(SimCallerRoleV1::Paper, "task31-cross-root-owner").await;
    let cross_root_candidate = candidate_market('7', "129", "14", "15");
    assert!(
        root_a
            .enroll_market(
                &mut session_b,
                enrollment_request("cross-root-enrollment", cross_root_candidate.clone()),
            )
            .is_err(),
        "同形运行根不能操作另一运行根持有的会话句柄"
    );
    assert!(
        session_b
            .public_market_state(&cross_root_candidate.market().condition_id.0)
            .is_none(),
        "归属失败必须保持外来会话目录不变"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task31_paper_appends_after_existing_use_without_account_reset_or_cross_session_leak() {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
    let initial_scenario = scenario();
    let mut process = execution_process_for_tests(rules, profile, Arc::clone(&initial_scenario));
    let mut first = process.create_active_session_for_tests(
        session_spec(
            &initial_scenario,
            SimCallerRoleV1::Paper,
            "task31-paper-first",
        ),
        accepting_submission_response_bridge(),
    );
    let second = process.create_active_session_for_tests(
        session_spec(
            &initial_scenario,
            SimCallerRoleV1::Paper,
            "task31-paper-second",
        ),
        accepting_submission_response_bridge(),
    );
    let existing_events = [
        initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10"),
        initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "10"),
    ];
    first
        .apply_closed_market_event_bucket(public_bucket(1_200, 0, 2, &existing_events))
        .expect("paper 既有市场先成功运行");
    let initial_available = first.available_pusd();
    let existing_market = &initial_scenario.market_set().markets()[0];
    let registration = first
        .post_order(candidate_post_request(
            &first,
            "existing-market-buy",
            1,
            1_300,
            existing_market.market().condition_id.clone(),
            existing_market.assets()[0].token_id.clone(),
        ))
        .expect("既有市场合法 BUY 请求登记");
    let effective_at = registration.item.effective_processing_at_utc_ms;
    first
        .apply_closed_market_event_bucket(public_bucket(effective_at, 2, 2, &[]))
        .expect("闭合到既有市场 BUY 的有效处理时点");
    first
        .drive_next_execution_action_through(effective_at)
        .expect("既有市场 BUY 完成平台处理");
    let available_after_existing_buy = first.available_pusd();
    assert!(available_after_existing_buy < initial_available);
    assert_eq!(first.simulated_order_count(), 1);
    let account_summary_before_enrollment = first.account_summary_sha256().clone();
    let outcome_balances_before_enrollment = first.outcome_balances_sha256().clone();
    let next_account_transition_seq_before_enrollment = first.next_account_transition_seq();

    let candidate = candidate_market('2', "124", "3", "4");
    let private_before = first.model_visible_cursor().private_feedback_sequence;
    let enrollment = process
        .enroll_market(
            &mut first,
            enrollment_request("paper-append", candidate.clone()),
        )
        .expect("paper 追加未使用市场");

    assert_eq!(enrollment.enrollment_chain_length, 1);
    assert!(
        first
            .public_market_state(&candidate.market().condition_id.0)
            .is_some()
    );
    assert!(
        second
            .public_market_state(&candidate.market().condition_id.0)
            .is_none(),
        "同一进程内另一会话的目录不得改变"
    );
    assert_eq!(
        first.available_pusd(),
        available_after_existing_buy,
        "追加市场必须保留既有市场已经形成的资金占用"
    );
    assert_eq!(
        first.model_visible_cursor().private_feedback_sequence,
        private_before
    );
    assert_eq!(
        first.simulated_order_count(),
        1,
        "追加市场不得删除既有市场订单"
    );
    assert_eq!(
        first.account_summary_sha256(),
        &account_summary_before_enrollment,
        "追加市场不得重写账户摘要"
    );
    assert_eq!(
        first.outcome_balances_sha256(),
        &outcome_balances_before_enrollment,
        "追加市场不得预写零结果代币余额"
    );
    assert_eq!(
        first.next_account_transition_seq(),
        next_account_transition_seq_before_enrollment,
        "追加市场不得消费账户变化序号"
    );
    assert!(
        first.cloned_session_shares_market_enrollment_storage(),
        "高频工作副本必须共享低频市场登记状态"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task31_training_and_offline_freeze_only_after_the_first_successful_business_input() {
    let candidate = candidate_market('2', "124", "3", "4");

    let (training_process, mut training, _) =
        active_session(SimCallerRoleV1::ModelTrain, "task31-training-freeze").await;
    let prefrozen_request = enrollment_request("training-prefrozen", candidate.clone());
    let prefrozen = training_process
        .enroll_market(&mut training, prefrozen_request.clone())
        .expect("training 在首个业务输入前可完成预登记");
    training
        .orders(OrdersRequestV1 {
            context: request_context(&training, "training-first-request", 1, 1_000),
            filter: OrdersFilterV1::default(),
            cursor: None,
        })
        .expect("首个业务请求成功登记");
    let frozen_enrollment_storage = training.market_enrollment_storage_identity();
    training
        .orders(OrdersRequestV1 {
            context: request_context(&training, "training-second-request", 2, 1_001),
            filter: OrdersFilterV1::default(),
            cursor: None,
        })
        .expect("冻结后的普通业务请求成功登记");
    assert_eq!(
        training.market_enrollment_storage_identity(),
        frozen_enrollment_storage,
        "training 冻结后的高频请求不得复制低频市场登记状态"
    );
    assert_eq!(
        training_process
            .enroll_market(&mut training, prefrozen_request)
            .expect("冻结后同一登记身份与同一市场仍须幂等返回"),
        prefrozen,
        "幂等重试必须先于冻结门判断"
    );
    assert!(
        training_process
            .enroll_market(
                &mut training,
                enrollment_request("training-too-late", candidate_market('3', "125", "5", "6"),),
            )
            .is_err(),
        "training 首个成功请求后必须冻结全部登记"
    );
    assert!(
        training
            .platform_market_state(&format!("0x{}", "3".repeat(64)))
            .is_none()
    );

    let (offline_process, mut offline, _) =
        active_session(SimCallerRoleV1::OfflineEvaluation, "task31-offline-freeze").await;
    assert_eq!(
        offline
            .apply_closed_market_event_bucket(hidden_empty_bucket(1_100, 0))
            .expect("空闭合桶也是成功业务事件输入"),
        MarketBucketApplyDispositionV1::Applied
    );
    assert!(
        offline_process
            .enroll_market(
                &mut offline,
                enrollment_request("offline-too-late", candidate.clone()),
            )
            .is_err(),
        "offline 首个成功事件输入后必须冻结全部登记"
    );
    assert!(
        offline
            .platform_market_state(&candidate.market().condition_id.0)
            .is_none()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task31_paper_candidate_fences_are_identity_scoped_and_marketless_requests_do_not_fence() {
    let candidate = candidate_market('2', "124", "3", "4");

    let (post_process, mut post_session, _) =
        active_session(SimCallerRoleV1::Paper, "task31-post-fence").await;
    post_session
        .post_order(candidate_post_request(
            &post_session,
            "candidate-post",
            1,
            1_000,
            candidate.market().condition_id.clone(),
            candidate.assets()[0].token_id.clone(),
        ))
        .expect("未知候选市场请求可成功登记并形成使用围栏");
    assert!(
        post_process
            .enroll_market(
                &mut post_session,
                enrollment_request("after-candidate-post", candidate.clone()),
            )
            .is_err()
    );

    let (condition_process, mut condition_session, _) =
        active_session(SimCallerRoleV1::Paper, "task31-condition-fence").await;
    condition_session
        .orders(OrdersRequestV1 {
            context: request_context(&condition_session, "candidate-condition-filter", 1, 1_000),
            filter: OrdersFilterV1 {
                condition_id: Some(candidate.market().condition_id.clone()),
                token_id: None,
            },
            cursor: None,
        })
        .expect("condition 过滤请求成功登记");
    assert!(
        condition_process
            .enroll_market(
                &mut condition_session,
                enrollment_request("after-condition-filter", candidate.clone()),
            )
            .is_err()
    );

    let (token_process, mut token_session, _) =
        active_session(SimCallerRoleV1::Paper, "task31-token-fence").await;
    token_session
        .trades(TradesRequestV1 {
            context: request_context(&token_session, "candidate-token-filter", 1, 1_000),
            filter: TradesFilterV1 {
                token_id: Some(candidate.assets()[1].token_id.clone()),
                ..TradesFilterV1::default()
            },
            cursor: None,
        })
        .expect("token-only 过滤请求成功登记");
    assert!(
        token_process
            .enroll_market(
                &mut token_session,
                enrollment_request("after-token-filter", candidate.clone()),
            )
            .is_err()
    );

    let (free_process, mut free_session, _) =
        active_session(SimCallerRoleV1::Paper, "task31-no-fence").await;
    free_session
        .cancel_all_orders(CancelAllOrdersRequestV1 {
            context: request_context(&free_session, "cancel-all", 1, 1_000),
            idempotency_key: "idem-cancel-all".to_owned(),
        })
        .expect("全撤不建立候选市场围栏");
    free_session
        .orders(OrdersRequestV1 {
            context: request_context(&free_session, "orders-unfiltered", 2, 1_001),
            filter: OrdersFilterV1::default(),
            cursor: None,
        })
        .expect("无市场过滤查询不建立围栏");
    free_process
        .enroll_market(
            &mut free_session,
            enrollment_request("after-marketless-requests", candidate),
        )
        .expect("无候选身份请求不得阻止后续登记");

    let (order_process, mut order_session, _) =
        active_session(SimCallerRoleV1::Paper, "task31-order-id-no-fence").await;
    order_session
        .order(OrderRequestV1 {
            context: request_context(&order_session, "order-by-id", 1, 1_000),
            platform_order_id: "a".repeat(64),
        })
        .expect("只按平台订单 ID 查询不建立围栏");
    order_session
        .trades(TradesRequestV1 {
            context: request_context(&order_session, "trades-unfiltered", 2, 1_001),
            filter: TradesFilterV1::default(),
            cursor: None,
        })
        .expect("无市场过滤查询不建立围栏");
    order_process
        .enroll_market(
            &mut order_session,
            enrollment_request(
                "after-order-id-requests",
                candidate_market('2', "124", "3", "4"),
            ),
        )
        .expect("只按订单 ID 与无过滤查询不得阻止后续登记");
}

#[tokio::test(flavor = "current_thread")]
async fn task31_unknown_event_leaves_no_tombstone_then_new_market_drives_events_and_buy() {
    let (process, mut session, _) =
        active_session(SimCallerRoleV1::Paper, "task31-event-retry").await;
    let candidate = candidate_market('2', "124", "3", "4");
    let events = candidate_snapshots(&candidate);
    let bucket = public_bucket(120_000, 0, 2, &events);
    let available_before = session.available_pusd();
    let cursor_before = session.public_event_index_position();

    assert!(matches!(
        session.apply_closed_market_event_bucket(bucket.clone()),
        Err(MarketEventErrorV1::UnknownMarket(condition_id))
            if condition_id == candidate.market().condition_id.0
    ));
    assert_eq!(session.public_event_index_position(), cursor_before);
    assert_eq!(session.available_pusd(), available_before);

    process
        .enroll_market(
            &mut session,
            enrollment_request("after-unknown-event", candidate.clone()),
        )
        .expect("失败事件不得留下永久使用墓碑");
    assert_eq!(
        session
            .apply_closed_market_event_bucket(bucket)
            .expect("登记后原事件桶可以重试"),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(session.public_event_index_position(), 2);
    assert!(
        session
            .platform_market_state(&candidate.market().condition_id.0)
            .is_some()
    );

    let registration = session
        .post_order(candidate_post_request(
            &session,
            "new-market-buy",
            1,
            130_000,
            candidate.market().condition_id.clone(),
            candidate.assets()[0].token_id.clone(),
        ))
        .expect("新增市场 BUY 请求可登记");
    let effective_at = registration.item.effective_processing_at_utc_ms;
    session
        .apply_closed_market_event_bucket(public_bucket(effective_at, 2, 2, &[]))
        .expect("闭合到新增市场 BUY 的有效处理时点");
    session
        .drive_next_execution_action_through(effective_at)
        .expect("新增市场 BUY 必须从会话局部目录完成处理");
    assert_eq!(session.simulated_order_count(), 1);
    assert!(session.available_pusd() < available_before);
}

async fn replay_enrollment_chain(
    role: SimCallerRoleV1,
    id: &str,
) -> (
    Vec<contracts::MarketEnrollmentV1>,
    Sha256Hex,
    u64,
    rust_decimal::Decimal,
) {
    let (process, mut session, _) = active_session(role, id).await;
    if role == SimCallerRoleV1::Paper {
        session
            .orders(OrdersRequestV1 {
                context: request_context(&session, "paper-before-append", 1, 1_000),
                filter: OrdersFilterV1::default(),
                cursor: None,
            })
            .expect("paper 已有业务后仍可追加未使用市场");
    }
    let first = process
        .enroll_market(
            &mut session,
            enrollment_request(
                "replay-enrollment-1",
                candidate_market('2', "124", "3", "4"),
            ),
        )
        .expect("重放第一项登记");
    let second = process
        .enroll_market(
            &mut session,
            enrollment_request(
                "replay-enrollment-2",
                candidate_market('3', "125", "5", "6"),
            ),
        )
        .expect("重放第二项登记");
    (
        vec![first, second.clone()],
        second.enrollment_chain_sha256,
        session.model_visible_cursor().private_feedback_sequence,
        session.available_pusd(),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn task31_training_prefreeze_and_paper_append_are_deterministic_in_clean_sessions() {
    for role in [SimCallerRoleV1::ModelTrain, SimCallerRoleV1::Paper] {
        let first = replay_enrollment_chain(role, "task31-clean-replay").await;
        let second = replay_enrollment_chain(role, "task31-clean-replay").await;
        assert_eq!(first, second, "相同角色的干净重放必须逐字段相同");
        assert_eq!(first.0[1].enrollment_chain_length, 2);
        assert_eq!(
            first.0[1].previous_enrollment,
            Some(MarketEnrollmentIdentityV1 {
                enrollment_id: first.0[0].enrollment_id.clone(),
                enrollment_sha256: first.0[0].enrollment_sha256.clone(),
            })
        );
        assert_eq!(first.2, 0, "登记不得分配私有反馈序号");
        assert_eq!(first.3, "1000".parse().expect("初始资金"));
    }
}
