use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use contracts::{
    AccountInstanceKeyV1, ExactDecimal, ExecutionMarketEventPayloadV3, ExecutionMarketEventV3,
    PlatformAccountDeltaPredecessorV1, PlatformAccountDeltaReasonV1, PlatformAccountDeltaV1,
    PlatformSourceV1, SimCallerRoleV1,
};
use data_pipeline_core::derive_execution_market_event_v3_id;
use rust_decimal::Decimal;

use crate::execution::{
    InitialAccountStateV1, MakerRebateAccrualStatusV1, OrderLifecycleEventKindV1,
    OrderLifecycleFeedbackV1, PlatformMarketV1, PlatformScenarioV1, PostOrderDirectResponseV1,
    PostOrderItemV1, PostOrderRequestV1, SettlementLifecycleEventV1, SettlementLifecycleFeedbackV1,
    SimBuyOrderKindV1, SimBuyOrderV1, SimExecutionProcessV1, SimExecutionSessionV1,
    SimulatedTradeKnownStatusV1, SubmissionDeliveryDispositionV1, SubmissionDriveDispositionV1,
    SubmissionItemOutcomeV1, SubmissionProcessingErrorV1, SubmissionResponseAcceptanceV1,
    SubmissionResponseBridgeV1, TradeEconomicDispositionV1, TradeLifecycleFeedbackV1,
    TradeLiquidityRoleV1,
};
use crate::finality::{
    TradeFinalityPolicyV1, TradeFinalitySelectorV1, TradeFinalityStateV1, policy_for_tests,
};
use crate::latency_profile::{EmpiricalDelayProfile, load_embedded_proxy_latency_profile};
use crate::market::MarketBucketApplyDispositionV1;
use crate::platform_rules::{
    SealedProcessRuleSnapshot, tests::historical_snapshot_for_sim_tests_with_fee_schedule,
};
use crate::session::SessionError;
use crate::task21_tests::{
    asset, hidden_bucket, initial_snapshot_event, market as base_market, session_spec,
};
use crate::task22_tests::context_for_arrival;
use crate::task23_tests::bid_transition;

const RUN_ID: &str = "component-test-run";
const UTC_DAY_MS: i64 = 86_400_000;
const DAY_ZERO: i64 = 1_785_715_200_000;
const TAKER_MATURITY: i64 = DAY_ZERO + UTC_DAY_MS + 600_000;
const MAKER_MATURITY: i64 = DAY_ZERO + UTC_DAY_MS + 2_700_000;

#[derive(Clone, Default)]
struct Task34Captures {
    post_order: Arc<Mutex<Vec<Arc<PostOrderDirectResponseV1>>>>,
    order_lifecycle: Arc<Mutex<Vec<Arc<OrderLifecycleFeedbackV1>>>>,
    trade_lifecycle: Arc<Mutex<Vec<Arc<TradeLifecycleFeedbackV1>>>>,
    settlement_attempts: Arc<Mutex<Vec<Arc<SettlementLifecycleFeedbackV1>>>>,
}

impl Task34Captures {
    fn bridge(&self, settlement_full_once: bool) -> SubmissionResponseBridgeV1 {
        let post_order = Arc::clone(&self.post_order);
        let order_lifecycle = Arc::clone(&self.order_lifecycle);
        let trade_lifecycle = Arc::clone(&self.trade_lifecycle);
        let settlement_attempts = Arc::clone(&self.settlement_attempts);
        SubmissionResponseBridgeV1::new(
            move |response| {
                post_order
                    .lock()
                    .expect("任务 3.4 单笔提交响应捕获锁")
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
            move |feedback| {
                order_lifecycle
                    .lock()
                    .expect("任务 3.4 订单生命周期反馈捕获锁")
                    .push(Arc::clone(&feedback));
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
            move |feedback| {
                trade_lifecycle
                    .lock()
                    .expect("任务 3.4 成交生命周期反馈捕获锁")
                    .push(Arc::clone(&feedback));
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
            move |feedback| {
                let first_attempt = {
                    let mut attempts = settlement_attempts
                        .lock()
                        .expect("任务 3.4 结算生命周期反馈捕获锁");
                    attempts.push(Arc::clone(&feedback));
                    attempts.len() == 1
                };
                if settlement_full_once && first_attempt {
                    SubmissionResponseAcceptanceV1::Full
                } else {
                    SubmissionResponseAcceptanceV1::Accepted(
                        feedback.metadata.delivery_confirmation(),
                    )
                }
            },
        )
    }

    fn settlement_attempts(&self) -> Vec<Arc<SettlementLifecycleFeedbackV1>> {
        self.settlement_attempts
            .lock()
            .expect("任务 3.4 结算生命周期反馈捕获锁")
            .clone()
    }

    fn unique_settlements(&self) -> Vec<Arc<SettlementLifecycleFeedbackV1>> {
        let attempts = self.settlement_attempts();
        let mut identities = BTreeSet::new();
        attempts
            .into_iter()
            .filter(|feedback| identities.insert(feedback.settlement_identity.clone()))
            .collect()
    }

    fn trade_feedbacks(&self) -> Vec<Arc<TradeLifecycleFeedbackV1>> {
        self.trade_lifecycle
            .lock()
            .expect("任务 3.4 成交生命周期反馈捕获锁")
            .clone()
    }

    fn order_feedbacks(&self) -> Vec<Arc<OrderLifecycleFeedbackV1>> {
        self.order_lifecycle
            .lock()
            .expect("任务 3.4 订单生命周期反馈捕获锁")
            .clone()
    }

    fn post_order_responses(&self) -> Vec<Arc<PostOrderDirectResponseV1>> {
        self.post_order
            .lock()
            .expect("任务 3.4 单笔提交响应捕获锁")
            .clone()
    }
}

fn decimal(value: &str) -> Decimal {
    value.parse().expect("任务 3.4 测试十进制")
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

fn success_policy(policy_id: &str, confirmed_offset_ms: i64) -> Arc<TradeFinalityPolicyV1> {
    policy_for_tests(
        policy_id,
        &[
            (TradeFinalityStateV1::Matched, 0),
            (TradeFinalityStateV1::Mined, 1),
            (TradeFinalityStateV1::Confirmed, confirmed_offset_ms),
        ],
        1,
        Vec::new(),
    )
}

fn two_failure_policy(
    session_id: &str,
    maker_request_id: &str,
    taker_request_id: &str,
) -> Arc<TradeFinalityPolicyV1> {
    policy_for_tests(
        "task34-failed-fills-policy",
        &[
            (TradeFinalityStateV1::Matched, 0),
            (TradeFinalityStateV1::Mined, 1),
            (TradeFinalityStateV1::Confirmed, 2),
        ],
        1,
        vec![
            (
                TradeFinalitySelectorV1::Request {
                    account_instance_key: account_key(session_id),
                    global_request_id: maker_request_id.to_owned(),
                },
                vec![
                    (TradeFinalityStateV1::Matched, 0),
                    (TradeFinalityStateV1::Failed, 400_000),
                ],
                Some(0),
            ),
            (
                TradeFinalitySelectorV1::Request {
                    account_instance_key: account_key(session_id),
                    global_request_id: taker_request_id.to_owned(),
                },
                vec![
                    (TradeFinalityStateV1::Matched, 0),
                    (TradeFinalityStateV1::Failed, 400_000),
                ],
                None,
            ),
        ],
    )
}

fn market_between(start_at_utc_ms: i64, end_at_utc_ms: i64) -> PlatformMarketV1 {
    market_for_slot(0, start_at_utc_ms, end_at_utc_ms)
}

fn market_for_slot(slot: u8, start_at_utc_ms: i64, end_at_utc_ms: i64) -> PlatformMarketV1 {
    let mut identity = base_market();
    let condition_digit = char::from_digit(u32::from(slot) + 1, 16).expect("任务 3.4 市场槽合法");
    identity.condition_id.0 = format!("0x{}", condition_digit.to_string().repeat(64));
    identity.clob_market_id.0 = (123_u16 + u16::from(slot)).to_string();
    identity.window.start_at_utc_ms = start_at_utc_ms;
    identity.window.end_at_utc_ms = end_at_utc_ms;
    let first_token = u16::from(slot) * 2 + 1;
    PlatformMarketV1::try_new(
        identity,
        [
            asset(&(first_token + 1).to_string(), 1, "Down"),
            asset(&first_token.to_string(), 0, "Up"),
        ],
    )
    .expect("任务 3.4 五分钟市场")
}

fn scenario_for(market: &PlatformMarketV1) -> Arc<PlatformScenarioV1> {
    Arc::new(PlatformScenarioV1::try_new(vec![market.clone()]).expect("任务 3.4 单市场场景"))
}

fn scenario_for_markets(markets: &[PlatformMarketV1]) -> Arc<PlatformScenarioV1> {
    Arc::new(PlatformScenarioV1::try_new(markets.to_vec()).expect("任务 3.4 多市场场景"))
}

fn up_asset(market: &PlatformMarketV1) -> &contracts::AssetIdentityV1 {
    market
        .assets()
        .iter()
        .find(|candidate| candidate.outcome.index == 0)
        .expect("任务 3.4 市场必须有 Up 资产")
}

fn rebind_event(
    mut event: ExecutionMarketEventV3,
    market: &PlatformMarketV1,
) -> ExecutionMarketEventV3 {
    event.market = market.market().clone();
    if let ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) = &mut event.payload
    {
        transition.market = market.market().clone();
        transition.asset = up_asset(market).clone();
    }
    event.event_id = derive_execution_market_event_v3_id(&event).expect("任务 3.4 事件身份");
    event
}

#[allow(clippy::too_many_arguments)]
fn initial_event(
    market: &PlatformMarketV1,
    token_id: &str,
    outcome_index: u32,
    label: &str,
    source_at_utc_ms: i64,
    sequence: u64,
    bid_price: &str,
    bid_quantity: &str,
    ask_price: &str,
    ask_quantity: &str,
) -> ExecutionMarketEventV3 {
    let mut event = initial_snapshot_event(
        token_id,
        outcome_index,
        label,
        source_at_utc_ms,
        source_at_utc_ms + 100,
        sequence,
        bid_quantity,
    );
    let ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) = &mut event.payload else {
        panic!("任务 3.4 初始夹具必须是 L2 快照");
    };
    snapshot.initial_tick_size_pusd_per_share = ExactDecimal("0.01".to_owned());
    snapshot.bids[0].price_pusd_per_share = ExactDecimal(bid_price.to_owned());
    snapshot.bids[0].quantity_shares = ExactDecimal(bid_quantity.to_owned());
    snapshot.asks[0].price_pusd_per_share = ExactDecimal(ask_price.to_owned());
    snapshot.asks[0].quantity_shares = ExactDecimal(ask_quantity.to_owned());
    rebind_event(event, market)
}

fn load_initial_market(
    session: &mut SimExecutionSessionV1,
    market: &PlatformMarketV1,
    source_at_utc_ms: i64,
    bid_price: &str,
    bid_quantity: &str,
    ask_price: &str,
    ask_quantity: &str,
) {
    let up = up_asset(market);
    let down = market
        .assets()
        .iter()
        .find(|candidate| candidate.outcome.index == 1)
        .expect("任务 3.4 市场必须有 Down 资产");
    let start_position = session.platform_event_index_position();
    let first_sequence = start_position
        .checked_add(1)
        .expect("任务 3.4 初始市场序号不溢出");
    let second_sequence = start_position
        .checked_add(2)
        .expect("任务 3.4 初始市场序号不溢出");
    let events = [
        initial_event(
            market,
            &up.token_id.0,
            up.outcome.index,
            &up.outcome.label,
            source_at_utc_ms,
            first_sequence,
            bid_price,
            bid_quantity,
            ask_price,
            ask_quantity,
        ),
        initial_event(
            market,
            &down.token_id.0,
            down.outcome.index,
            &down.outcome.label,
            source_at_utc_ms,
            second_sequence,
            "0.4",
            "1",
            "0.6",
            "5",
        ),
    ];
    let next_position = start_position
        .checked_add(2)
        .expect("任务 3.4 初始市场位置不溢出");
    assert_eq!(
        session.apply_closed_market_event_bucket(hidden_bucket(
            source_at_utc_ms,
            next_position,
            &events,
        )),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
}

fn new_session(
    session_id: &str,
    available_pusd: Decimal,
    rules: Arc<SealedProcessRuleSnapshot>,
    policy: Arc<TradeFinalityPolicyV1>,
    market: &PlatformMarketV1,
    captures: &Task34Captures,
    settlement_full_once: bool,
) -> (SimExecutionSessionV1, Arc<EmpiricalDelayProfile>) {
    let profile = load_embedded_proxy_latency_profile().expect("任务 3.4 代理时延配置档");
    let scenario = scenario_for(market);
    let mut spec = session_spec(&scenario, SimCallerRoleV1::ModelTrain, session_id);
    spec.initial_account_state =
        InitialAccountStateV1::try_new(available_pusd).expect("任务 3.4 显式初始资金");
    spec.response_outbox_capacity = NonZeroUsize::new(64).expect("任务 3.4 正反馈槽容量");
    let mut process = SimExecutionProcessV1::try_new(
        RUN_ID.to_owned(),
        policy,
        rules,
        Arc::clone(&profile),
        scenario,
    )
    .expect("任务 3.4 运行根");
    (
        process.create_active_session_for_tests(spec, captures.bridge(settlement_full_once)),
        profile,
    )
}

fn new_multi_market_session(
    session_id: &str,
    available_pusd: Decimal,
    rules: Arc<SealedProcessRuleSnapshot>,
    policy: Arc<TradeFinalityPolicyV1>,
    markets: &[PlatformMarketV1],
    captures: &Task34Captures,
) -> (SimExecutionSessionV1, Arc<EmpiricalDelayProfile>) {
    let profile = load_embedded_proxy_latency_profile().expect("任务 3.4 代理时延配置档");
    let scenario = scenario_for_markets(markets);
    let mut spec = session_spec(&scenario, SimCallerRoleV1::ModelTrain, session_id);
    spec.initial_account_state =
        InitialAccountStateV1::try_new(available_pusd).expect("任务 3.4 显式初始资金");
    spec.response_outbox_capacity = NonZeroUsize::new(64).expect("任务 3.4 正反馈槽容量");
    let mut process = SimExecutionProcessV1::try_new(
        RUN_ID.to_owned(),
        policy,
        rules,
        Arc::clone(&profile),
        scenario,
    )
    .expect("任务 3.4 多市场运行根");
    (
        process.create_active_session_for_tests(spec, captures.bridge(false)),
        profile,
    )
}

#[allow(clippy::too_many_arguments)]
fn gtc_request_for_effective_at(
    session: &SimExecutionSessionV1,
    profile: &EmpiricalDelayProfile,
    market: &PlatformMarketV1,
    request_id: &str,
    caller_sequence: u64,
    effective_at_utc_ms: i64,
    price: &str,
    size: &str,
) -> PostOrderRequestV1 {
    PostOrderRequestV1 {
        context: context_for_arrival(
            session,
            profile,
            request_id,
            caller_sequence,
            effective_at_utc_ms,
        ),
        idempotency_key: format!("idem-{request_id}"),
        item: PostOrderItemV1 {
            item_id: format!("item-{request_id}"),
            order: SimBuyOrderV1 {
                condition_id: market.market().condition_id.clone(),
                token_id: up_asset(market).token_id.clone(),
                kind: SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: decimal(price),
                    size_shares: decimal(size),
                    post_only: true,
                },
            },
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn gtd_request_for_effective_at(
    session: &SimExecutionSessionV1,
    profile: &EmpiricalDelayProfile,
    market: &PlatformMarketV1,
    request_id: &str,
    caller_sequence: u64,
    effective_at_utc_ms: i64,
    expires_at_utc_ms: i64,
    price: &str,
    size: &str,
) -> PostOrderRequestV1 {
    PostOrderRequestV1 {
        context: context_for_arrival(
            session,
            profile,
            request_id,
            caller_sequence,
            effective_at_utc_ms,
        ),
        idempotency_key: format!("idem-{request_id}"),
        item: PostOrderItemV1 {
            item_id: format!("item-{request_id}"),
            order: SimBuyOrderV1 {
                condition_id: market.market().condition_id.clone(),
                token_id: up_asset(market).token_id.clone(),
                kind: SimBuyOrderKindV1::GtdLimit {
                    limit_price_pusd_per_share: decimal(price),
                    size_shares: decimal(size),
                    post_only: true,
                    expires_at_utc_ms,
                },
            },
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn fak_request_for_effective_at(
    session: &SimExecutionSessionV1,
    profile: &EmpiricalDelayProfile,
    market: &PlatformMarketV1,
    request_id: &str,
    caller_sequence: u64,
    effective_at_utc_ms: i64,
    spend: &str,
    worst_price: &str,
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
                condition_id: market.market().condition_id.clone(),
                token_id: up_asset(market).token_id.clone(),
                kind: SimBuyOrderKindV1::FakMarket {
                    spend_pusd: decimal(spend),
                    worst_price_pusd_per_share: decimal(worst_price),
                },
            },
        },
    }
}

fn apply_empty_at(
    session: &mut SimExecutionSessionV1,
    at_utc_ms: i64,
) -> MarketBucketApplyDispositionV1 {
    let position = session.platform_event_index_position();
    session
        .apply_closed_market_event_bucket(hidden_bucket(at_utc_ms, position, &[]))
        .expect("任务 3.4 空平台桶")
}

fn drive_post_order(
    session: &mut SimExecutionSessionV1,
    request: PostOrderRequestV1,
    expected_effective_at_utc_ms: i64,
) {
    let registration = session.post_order(request).expect("任务 3.4 订单登记");
    assert_eq!(
        registration.item.effective_processing_at_utc_ms,
        expected_effective_at_utc_ms
    );
    assert_eq!(
        apply_empty_at(session, expected_effective_at_utc_ms),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(
        session.drive_next_execution_action_through(expected_effective_at_utc_ms),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
}

fn apply_bid_transition(
    session: &mut SimExecutionSessionV1,
    market: &PlatformMarketV1,
    at_utc_ms: i64,
    sequence: u64,
    before: &str,
    after: &str,
) {
    let event = rebind_event(
        bid_transition(at_utc_ms, at_utc_ms + 100, sequence, "0.4", before, after),
        market,
    );
    let next_position = session.platform_event_index_position() + 1;
    assert_eq!(
        session
            .apply_closed_market_event_bucket(hidden_bucket(at_utc_ms, next_position, &[event],)),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
}

fn drain_accepting(session: &mut SimExecutionSessionV1, through_utc_ms: i64) {
    match session
        .drive_private_delivery_after_platform_through_for_tests(through_utc_ms)
        .expect("任务 3.4 私有反馈交付")
    {
        SubmissionDeliveryDispositionV1::Delivered
        | SubmissionDeliveryDispositionV1::NoVisibleResponse => {}
        disposition => panic!("任务 3.4 接受桥不得施加背压：{disposition:?}"),
    }
}

struct ActualSegmentsRun {
    session: SimExecutionSessionV1,
    captures: Task34Captures,
}

async fn run_actual_segments(
    session_id: &str,
    fee_rate: &str,
    fee_exponent: u32,
    settlement_full_once: bool,
) -> ActualSegmentsRun {
    let market = market_between(DAY_ZERO + 600_000, DAY_ZERO + 900_000);
    let captures = Task34Captures::default();
    let rules = historical_snapshot_for_sim_tests_with_fee_schedule(fee_rate, fee_exponent).await;
    let (mut session, profile) = new_session(
        session_id,
        decimal("1000"),
        rules,
        success_policy(&format!("{session_id}-policy"), 2),
        &market,
        &captures,
        settlement_full_once,
    );
    load_initial_market(
        &mut session,
        &market,
        DAY_ZERO + 610_000,
        "0.4",
        "1",
        "0.6",
        "1000",
    );

    let maker_request = gtc_request_for_effective_at(
        &session,
        &profile,
        &market,
        &format!("{session_id}-maker"),
        1,
        DAY_ZERO + 620_000,
        "0.4",
        "200",
    );
    drive_post_order(&mut session, maker_request, DAY_ZERO + 620_000);
    apply_bid_transition(&mut session, &market, DAY_ZERO + 621_000, 3, "1", "201");
    apply_bid_transition(&mut session, &market, DAY_ZERO + 622_000, 4, "201", "0");
    assert_eq!(
        apply_empty_at(&mut session, DAY_ZERO + 622_001),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(
        apply_empty_at(&mut session, DAY_ZERO + 622_002),
        MarketBucketApplyDispositionV1::Applied
    );

    let taker_request = fak_request_for_effective_at(
        &session,
        &profile,
        &market,
        &format!("{session_id}-taker"),
        2,
        DAY_ZERO + 630_000,
        "240",
        "0.6",
    );
    drive_post_order(&mut session, taker_request, DAY_ZERO + 630_000);
    assert_eq!(
        apply_empty_at(&mut session, DAY_ZERO + 630_001),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(
        apply_empty_at(&mut session, DAY_ZERO + 630_002),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(
        apply_empty_at(&mut session, DAY_ZERO + 900_000),
        MarketBucketApplyDispositionV1::Applied
    );

    if settlement_full_once {
        assert_eq!(
            session
                .drive_private_delivery_after_platform_through_for_tests(DAY_ZERO + 950_000)
                .expect("任务 3.4 首次结算反馈 Full"),
            SubmissionDeliveryDispositionV1::Full
        );
    }
    drain_accepting(&mut session, DAY_ZERO + 950_000);
    ActualSegmentsRun { session, captures }
}

fn assert_actual_segment_economics(
    captures: &Task34Captures,
    maker_fee: &str,
    maker_rebate: &str,
    taker_fee: &str,
    taker_rebate: &str,
) {
    let settlements = captures.unique_settlements();
    let maker = settlements
        .iter()
        .find(|feedback| {
            matches!(
                feedback.event,
                SettlementLifecycleEventV1::MakerRebateAccrualFinalized { .. }
            )
        })
        .expect("任务 3.4 maker 应计终态");
    let SettlementLifecycleEventV1::MakerRebateAccrualFinalized {
        status,
        contributions,
        accrual_pusd,
        account_delta,
        ..
    } = &maker.event
    else {
        unreachable!();
    };
    assert_eq!(*status, MakerRebateAccrualStatusV1::Calculated);
    assert_eq!(contributions.len(), 1);
    assert_eq!(contributions[0].base_quantity_shares, decimal("200"));
    assert_eq!(contributions[0].price_pusd_per_share, decimal("0.4"));
    assert_eq!(
        contributions[0].maker_fee_equivalent_pusd,
        decimal(maker_fee)
    );
    assert_eq!(contributions[0].rebate_rate, decimal("0.2"));
    assert_eq!(
        contributions[0].rebate_contribution_pusd,
        decimal(maker_rebate)
    );
    assert_eq!(*accrual_pusd, decimal(maker_rebate));
    assert_eq!(
        account_delta
            .as_ref()
            .expect("正 maker 应计账户变化")
            .reason,
        PlatformAccountDeltaReasonV1::MakerRebateAccrualFinalized
    );

    let taker = settlements
        .iter()
        .find(|feedback| {
            matches!(
                feedback.event,
                SettlementLifecycleEventV1::TakerRebateAccrualApplied { .. }
            )
        })
        .expect("任务 3.4 taker 应计事实");
    let SettlementLifecycleEventV1::TakerRebateAccrualApplied {
        actual_taker_fee_pusd,
        rebate_rate,
        accrual_pusd,
        account_delta,
        ..
    } = &taker.event
    else {
        unreachable!();
    };
    assert_eq!(*actual_taker_fee_pusd, decimal(taker_fee));
    assert_eq!(*rebate_rate, decimal("0.08"));
    assert_eq!(*accrual_pusd, decimal(taker_rebate));
    assert_eq!(
        account_delta.reason,
        PlatformAccountDeltaReasonV1::TakerRebateAccrualApplied
    );

    let terminal = captures
        .trade_feedbacks()
        .into_iter()
        .filter(|feedback| feedback.status.known == Some(SimulatedTradeKnownStatusV1::Confirmed))
        .collect::<Vec<_>>();
    assert_eq!(terminal.len(), 2);
    let maker_trade = terminal
        .iter()
        .find(|feedback| feedback.liquidity_role == TradeLiquidityRoleV1::MakerInferred)
        .expect("任务 3.4 实际 maker 成交");
    assert_eq!(maker_trade.base_quantity_shares, decimal("200"));
    assert_eq!(maker_trade.immediate_taker_fee_pusd, Decimal::ZERO);
    let taker_trade = terminal
        .iter()
        .find(|feedback| feedback.liquidity_role == TradeLiquidityRoleV1::Taker)
        .expect("任务 3.4 实际 taker 成交");
    assert_eq!(
        taker_trade.base_quantity_shares,
        decimal("400.000001"),
        "真实 SDK 市价买单编码会为 240/0.6 生成向上一个微份额的 base"
    );
    assert_eq!(taker_trade.immediate_taker_fee_pusd, decimal(taker_fee));
}

#[tokio::test(flavor = "current_thread")]
async fn task341_actual_segments_use_each_process_fee_schedule() {
    let first = run_actual_segments("task341-fee-a", "0.07", 1, true).await;
    assert!(first.session.cloned_execution_state_shares_rebate_storage());
    let first_attempts = first.captures.settlement_attempts();
    assert!(first_attempts.len() >= 2);
    assert!(Arc::ptr_eq(&first_attempts[0], &first_attempts[1]));
    assert_eq!(
        first_attempts[0].metadata.delivery_confirmation(),
        first_attempts[1].metadata.delivery_confirmation()
    );
    assert_actual_segment_economics(&first.captures, "3.36", "0.672", "6.72", "0.5376");

    let second = run_actual_segments("task341-fee-b", "0.08", 2, false).await;
    assert_actual_segment_economics(&second.captures, "0.9216", "0.18432", "1.8432", "0.147456");
}

#[tokio::test(flavor = "current_thread")]
async fn task341_failed_fills_compensate_and_finalize_zero_once() {
    let session_id = "task341-failed";
    let maker_request_id = "task341-failed-maker";
    let taker_request_id = "task341-failed-taker";
    let market = market_between(DAY_ZERO + 600_000, DAY_ZERO + 900_000);
    let captures = Task34Captures::default();
    let rules = historical_snapshot_for_sim_tests_with_fee_schedule("0.07", 1).await;
    let (mut session, profile) = new_session(
        session_id,
        decimal("1000"),
        rules,
        two_failure_policy(session_id, maker_request_id, taker_request_id),
        &market,
        &captures,
        false,
    );
    load_initial_market(
        &mut session,
        &market,
        DAY_ZERO + 610_000,
        "0.4",
        "1",
        "0.6",
        "1000",
    );
    let maker_request = gtc_request_for_effective_at(
        &session,
        &profile,
        &market,
        maker_request_id,
        1,
        DAY_ZERO + 620_000,
        "0.4",
        "200",
    );
    drive_post_order(&mut session, maker_request, DAY_ZERO + 620_000);
    apply_bid_transition(&mut session, &market, DAY_ZERO + 621_000, 3, "1", "201");
    apply_bid_transition(&mut session, &market, DAY_ZERO + 622_000, 4, "201", "0");
    let taker_request = fak_request_for_effective_at(
        &session,
        &profile,
        &market,
        taker_request_id,
        2,
        DAY_ZERO + 630_000,
        "240",
        "0.6",
    );
    drive_post_order(&mut session, taker_request, DAY_ZERO + 630_000);
    assert_eq!(
        apply_empty_at(&mut session, DAY_ZERO + 900_000),
        MarketBucketApplyDispositionV1::Applied
    );

    let maker_failed_at = DAY_ZERO + 1_022_000;
    let maker_failure_bucket = hidden_bucket(
        maker_failed_at,
        session.platform_event_index_position(),
        &[],
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(maker_failure_bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let summary_after_failure = session.account_summary_sha256().clone();
    let seq_after_failure = session.next_account_transition_seq();
    assert_eq!(
        session.apply_closed_market_event_bucket(maker_failure_bucket),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(session.account_summary_sha256(), &summary_after_failure);
    assert_eq!(session.next_account_transition_seq(), seq_after_failure);
    assert_eq!(
        apply_empty_at(&mut session, DAY_ZERO + 1_030_000),
        MarketBucketApplyDispositionV1::Applied
    );
    drain_accepting(&mut session, DAY_ZERO + 1_080_000);

    assert_eq!(session.available_pusd(), decimal("1000"));
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);
    assert_eq!(session.immediate_taker_fee_encumbered_pusd(), Decimal::ZERO);
    let settlements = captures.unique_settlements();
    let maker_results = settlements
        .iter()
        .filter_map(|feedback| match &feedback.event {
            SettlementLifecycleEventV1::MakerRebateAccrualFinalized {
                status,
                contributions,
                accrual_pusd,
                account_delta,
                ..
            } => Some((*status, contributions, *accrual_pusd, account_delta)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(maker_results.len(), 1);
    assert_eq!(
        maker_results[0].0,
        MakerRebateAccrualStatusV1::ZeroNoEligibleMakerFill
    );
    assert!(maker_results[0].1.is_empty());
    assert_eq!(maker_results[0].2, Decimal::ZERO);
    assert!(maker_results[0].3.is_none());
    assert!(!settlements.iter().any(|feedback| matches!(
        feedback.event,
        SettlementLifecycleEventV1::TakerRebateAccrualApplied { .. }
    )));
    assert!(!settlements.iter().any(|feedback| matches!(
        feedback.event,
        SettlementLifecycleEventV1::MakerRebateAccrualFinalized {
            status: MakerRebateAccrualStatusV1::UnknownIncomplete,
            ..
        }
    )));

    let failed = captures
        .trade_feedbacks()
        .into_iter()
        .filter(|feedback| feedback.status.known == Some(SimulatedTradeKnownStatusV1::Failed))
        .collect::<Vec<_>>();
    assert_eq!(failed.len(), 2);
    let maker_failed = failed
        .iter()
        .find(|feedback| feedback.liquidity_role == TradeLiquidityRoleV1::MakerInferred)
        .expect("任务 3.4 maker 失败反馈");
    assert_eq!(
        maker_failed.economic_disposition,
        TradeEconomicDispositionV1::FinalFailureCompensated
    );
    assert_eq!(
        maker_failed
            .account_delta
            .as_ref()
            .expect("任务 3.4 maker 补偿变化")
            .reason,
        PlatformAccountDeltaReasonV1::FillFinalityCompensationApplied
    );
    let taker_failed = failed
        .iter()
        .find(|feedback| feedback.liquidity_role == TradeLiquidityRoleV1::Taker)
        .expect("任务 3.4 taker 失败反馈");
    assert_eq!(
        taker_failed.economic_disposition,
        TradeEconomicDispositionV1::FinalFailureUnapplied
    );
    assert_eq!(
        taker_failed
            .account_delta
            .as_ref()
            .expect("任务 3.4 taker 未应用释放变化")
            .reason,
        PlatformAccountDeltaReasonV1::FillFinalityUnappliedFailureReleased
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task342_rebate_kinds_do_not_combine_and_use_independent_utc_clocks() {
    let mut run = run_actual_segments("task342-kind-clocks", "0.07", 1, false).await;
    assert_eq!(
        apply_empty_at(&mut run.session, TAKER_MATURITY - 1),
        MarketBucketApplyDispositionV1::Applied
    );
    assert!(!run.captures.unique_settlements().iter().any(|feedback| {
        matches!(
            feedback.event,
            SettlementLifecycleEventV1::MakerRebateDayFinalized { .. }
                | SettlementLifecycleEventV1::TakerRebateDayFinalized { .. }
                | SettlementLifecycleEventV1::MakerRebatePaid { .. }
                | SettlementLifecycleEventV1::TakerRebatePaid { .. }
        )
    }));
    assert_eq!(
        apply_empty_at(&mut run.session, TAKER_MATURITY),
        MarketBucketApplyDispositionV1::Applied
    );
    drain_accepting(&mut run.session, TAKER_MATURITY + 50_000);
    assert_eq!(
        apply_empty_at(&mut run.session, MAKER_MATURITY - 1),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(
        run.captures
            .unique_settlements()
            .iter()
            .filter(|feedback| matches!(
                feedback.event,
                SettlementLifecycleEventV1::MakerRebateDayFinalized { .. }
            ))
            .count(),
        0
    );
    assert_eq!(
        apply_empty_at(&mut run.session, MAKER_MATURITY),
        MarketBucketApplyDispositionV1::Applied
    );
    drain_accepting(&mut run.session, MAKER_MATURITY + 50_000);

    let settlements = run.captures.unique_settlements();
    let taker_day = settlements
        .iter()
        .find(|feedback| {
            matches!(
                feedback.event,
                SettlementLifecycleEventV1::TakerRebateDayFinalized { .. }
            )
        })
        .expect("任务 3.4 taker 00:10 日结");
    assert_eq!(taker_day.metadata.platform_event_at_utc_ms, TAKER_MATURITY);
    let SettlementLifecycleEventV1::TakerRebateDayFinalized {
        accrual_pusd,
        payable_pusd,
        locked_remainder_pusd,
        account_delta,
        ..
    } = &taker_day.event
    else {
        unreachable!();
    };
    assert_eq!(*accrual_pusd, decimal("0.5376"));
    assert_eq!(*payable_pusd, Decimal::ZERO);
    assert_eq!(*locked_remainder_pusd, Decimal::ZERO);
    assert!(account_delta.is_none());

    let maker_day = settlements
        .iter()
        .find(|feedback| {
            matches!(
                feedback.event,
                SettlementLifecycleEventV1::MakerRebateDayFinalized { .. }
            )
        })
        .expect("任务 3.4 maker 00:45 日结");
    assert_eq!(maker_day.metadata.platform_event_at_utc_ms, MAKER_MATURITY);
    let SettlementLifecycleEventV1::MakerRebateDayFinalized {
        accrual_pusd,
        payable_pusd,
        locked_remainder_pusd,
        account_delta,
        ..
    } = &maker_day.event
    else {
        unreachable!();
    };
    assert_eq!(*accrual_pusd, decimal("0.672"));
    assert_eq!(*payable_pusd, Decimal::ZERO);
    assert_eq!(*locked_remainder_pusd, Decimal::ZERO);
    assert!(account_delta.is_none());
    assert!(decimal("0.5376") + decimal("0.672") > Decimal::ONE);
    assert!(!settlements.iter().any(|feedback| matches!(
        feedback.event,
        SettlementLifecycleEventV1::MakerRebatePaid { .. }
            | SettlementLifecycleEventV1::TakerRebatePaid { .. }
    )));
}

#[tokio::test(flavor = "current_thread")]
async fn task342_matured_exact_threshold_waits_for_finality_and_pays_at_zero_available() {
    let market = market_between(DAY_ZERO + 600_000, DAY_ZERO + 900_000);
    let captures = Task34Captures::default();
    let rules = historical_snapshot_for_sim_tests_with_fee_schedule("0.07", 1).await;
    let matched_at = DAY_ZERO + 700_000;
    let confirmed_at = TAKER_MATURITY + 1_000;
    let (mut session, profile) = new_session(
        "task342-delayed-finality",
        decimal("369.642857"),
        rules,
        success_policy("task342-delayed-finality-policy", confirmed_at - matched_at),
        &market,
        &captures,
        false,
    );
    load_initial_market(
        &mut session,
        &market,
        DAY_ZERO + 610_000,
        "0.4",
        "1",
        "0.5",
        "1000",
    );
    let request = fak_request_for_effective_at(
        &session,
        &profile,
        &market,
        "task342-delayed-finality-taker",
        1,
        matched_at,
        "357.142857",
        "0.5",
    );
    drive_post_order(&mut session, request, matched_at);
    assert_eq!(
        apply_empty_at(&mut session, matched_at + 1),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(session.available_pusd(), Decimal::ZERO);
    assert_eq!(session.principal_encumbered_pusd(), Decimal::ZERO);
    assert_eq!(session.immediate_taker_fee_encumbered_pusd(), Decimal::ZERO);
    assert_eq!(
        apply_empty_at(&mut session, DAY_ZERO + 900_000),
        MarketBucketApplyDispositionV1::Applied
    );
    drain_accepting(&mut session, DAY_ZERO + 950_000);

    assert_eq!(
        apply_empty_at(&mut session, TAKER_MATURITY - 1),
        MarketBucketApplyDispositionV1::Applied
    );
    let maturity_bucket =
        hidden_bucket(TAKER_MATURITY, session.platform_event_index_position(), &[]);
    assert_eq!(
        session.apply_closed_market_event_bucket(maturity_bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(maturity_bucket),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert!(
        !captures
            .unique_settlements()
            .iter()
            .any(|feedback| matches!(
                feedback.event,
                SettlementLifecycleEventV1::TakerRebateAccrualApplied { .. }
                    | SettlementLifecycleEventV1::TakerRebateDayFinalized { .. }
                    | SettlementLifecycleEventV1::TakerRebatePaid { .. }
            ))
    );

    let confirmed_bucket =
        hidden_bucket(confirmed_at, session.platform_event_index_position(), &[]);
    assert_eq!(
        session.apply_closed_market_event_bucket(confirmed_bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let seq_after_confirmed = session.next_account_transition_seq();
    assert_eq!(
        session.apply_closed_market_event_bucket(confirmed_bucket),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(session.next_account_transition_seq(), seq_after_confirmed);
    drain_accepting(&mut session, confirmed_at + 50_000);

    let taker_settlements = captures
        .unique_settlements()
        .into_iter()
        .filter(|feedback| {
            matches!(
                feedback.event,
                SettlementLifecycleEventV1::TakerRebateAccrualApplied { .. }
                    | SettlementLifecycleEventV1::TakerRebateDayFinalized { .. }
                    | SettlementLifecycleEventV1::TakerRebatePaid { .. }
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(taker_settlements.len(), 3);
    assert!(
        taker_settlements
            .iter()
            .all(|feedback| feedback.metadata.platform_event_at_utc_ms == confirmed_at)
    );
    let accrual = taker_settlements
        .iter()
        .find(|feedback| {
            matches!(
                feedback.event,
                SettlementLifecycleEventV1::TakerRebateAccrualApplied { .. }
            )
        })
        .expect("成熟义务必须包含 taker 应计");
    let SettlementLifecycleEventV1::TakerRebateAccrualApplied {
        actual_taker_fee_pusd,
        accrual_pusd,
        account_delta,
        ..
    } = &accrual.event
    else {
        unreachable!();
    };
    assert_eq!(*actual_taker_fee_pusd, decimal("12.5"));
    assert_eq!(*accrual_pusd, Decimal::ONE);
    assert_eq!(
        account_delta.reason,
        PlatformAccountDeltaReasonV1::TakerRebateAccrualApplied
    );
    let day = taker_settlements
        .iter()
        .find(|feedback| {
            matches!(
                feedback.event,
                SettlementLifecycleEventV1::TakerRebateDayFinalized { .. }
            )
        })
        .expect("成熟义务必须包含 taker 日结");
    let SettlementLifecycleEventV1::TakerRebateDayFinalized {
        accrual_pusd,
        payable_pusd,
        locked_remainder_pusd,
        account_delta,
        ..
    } = &day.event
    else {
        unreachable!();
    };
    assert_eq!(*accrual_pusd, Decimal::ONE);
    assert_eq!(*payable_pusd, Decimal::ONE);
    assert_eq!(*locked_remainder_pusd, Decimal::ZERO);
    assert!(account_delta.is_none());
    let paid = taker_settlements
        .iter()
        .find(|feedback| {
            matches!(
                feedback.event,
                SettlementLifecycleEventV1::TakerRebatePaid { .. }
            )
        })
        .expect("成熟义务必须包含 taker 付款");
    let SettlementLifecycleEventV1::TakerRebatePaid {
        paid_pusd,
        account_delta,
        ..
    } = &paid.event
    else {
        unreachable!();
    };
    assert_eq!(*paid_pusd, Decimal::ONE);
    assert_eq!(
        account_delta.reason,
        PlatformAccountDeltaReasonV1::TakerRebatePaid
    );
    assert_eq!(
        decimal(&account_delta.balance_delta.available_pusd_delta.0),
        Decimal::ONE
    );
    assert_eq!(session.available_pusd(), Decimal::ONE);
}

#[derive(Debug, PartialEq, Eq)]
struct CrossDayOutcome {
    events: Vec<SettlementLifecycleEventV1>,
    available_pusd: Decimal,
    next_account_transition_seq: u64,
}

async fn run_cross_day_case(session_id: &str) -> CrossDayOutcome {
    let midnight = DAY_ZERO + UTC_DAY_MS;
    let market = market_between(midnight - 240_000, midnight + 60_000);
    let captures = Task34Captures::default();
    let rules = historical_snapshot_for_sim_tests_with_fee_schedule("0.07", 1).await;
    let (mut session, profile) = new_session(
        session_id,
        decimal("1000"),
        rules,
        success_policy(&format!("{session_id}-policy"), 2),
        &market,
        &captures,
        false,
    );
    load_initial_market(
        &mut session,
        &market,
        midnight - 230_000,
        "0.4",
        "1",
        "0.5",
        "2000",
    );
    for (ordinal, effective_at, spend) in [
        (1, midnight - 10_000, "357.143"),
        (2, midnight + 10_000, "357.1425"),
    ] {
        let request = fak_request_for_effective_at(
            &session,
            &profile,
            &market,
            &format!("{session_id}-day-{ordinal}"),
            ordinal,
            effective_at,
            spend,
            "0.5",
        );
        drive_post_order(&mut session, request, effective_at);
        assert_eq!(
            apply_empty_at(&mut session, effective_at + 1),
            MarketBucketApplyDispositionV1::Applied
        );
        assert_eq!(
            apply_empty_at(&mut session, effective_at + 2),
            MarketBucketApplyDispositionV1::Applied
        );
    }
    assert_eq!(
        apply_empty_at(&mut session, midnight + 60_000),
        MarketBucketApplyDispositionV1::Applied
    );
    drain_accepting(&mut session, midnight + 110_000);

    assert_eq!(
        apply_empty_at(&mut session, TAKER_MATURITY - 1),
        MarketBucketApplyDispositionV1::Applied
    );
    assert!(
        !captures
            .unique_settlements()
            .iter()
            .any(|feedback| matches!(
                feedback.event,
                SettlementLifecycleEventV1::TakerRebateDayFinalized { .. }
                    | SettlementLifecycleEventV1::TakerRebatePaid { .. }
            ))
    );
    let first_maturity_bucket =
        hidden_bucket(TAKER_MATURITY, session.platform_event_index_position(), &[]);
    assert_eq!(
        session.apply_closed_market_event_bucket(first_maturity_bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let seq_after_first_maturity = session.next_account_transition_seq();
    assert_eq!(
        session.apply_closed_market_event_bucket(first_maturity_bucket),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(
        session.next_account_transition_seq(),
        seq_after_first_maturity
    );
    drain_accepting(&mut session, TAKER_MATURITY + 50_000);

    let second_maturity = TAKER_MATURITY + UTC_DAY_MS;
    assert_eq!(
        apply_empty_at(&mut session, second_maturity - 1),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(
        captures
            .unique_settlements()
            .iter()
            .filter(|feedback| matches!(
                feedback.event,
                SettlementLifecycleEventV1::TakerRebateDayFinalized { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        apply_empty_at(&mut session, second_maturity),
        MarketBucketApplyDispositionV1::Applied
    );
    drain_accepting(&mut session, second_maturity + 50_000);

    let settlements = captures.unique_settlements();
    let taker_accruals = settlements
        .iter()
        .filter_map(|feedback| match &feedback.event {
            SettlementLifecycleEventV1::TakerRebateAccrualApplied {
                actual_taker_fee_pusd,
                accrual_pusd,
                ..
            } => Some((*actual_taker_fee_pusd, *accrual_pusd)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        taker_accruals,
        vec![
            (decimal("12.50001"), decimal("1.0000008")),
            (decimal("12.49999"), decimal("0.9999992")),
        ]
    );
    let taker_days = settlements
        .iter()
        .filter_map(|feedback| match &feedback.event {
            SettlementLifecycleEventV1::TakerRebateDayFinalized {
                attribution_day_start_utc_ms,
                accrual_pusd,
                payable_pusd,
                locked_remainder_pusd,
                ..
            } => Some((
                *attribution_day_start_utc_ms,
                *accrual_pusd,
                *payable_pusd,
                *locked_remainder_pusd,
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        taker_days,
        vec![
            (
                DAY_ZERO,
                decimal("1.0000008"),
                Decimal::ONE,
                decimal("0.0000008"),
            ),
            (
                DAY_ZERO + UTC_DAY_MS,
                decimal("0.9999992"),
                Decimal::ZERO,
                Decimal::ZERO,
            ),
        ]
    );
    let payments = settlements
        .iter()
        .filter_map(|feedback| match &feedback.event {
            SettlementLifecycleEventV1::TakerRebatePaid {
                attribution_day_start_utc_ms,
                paid_pusd,
                ..
            } => Some((*attribution_day_start_utc_ms, *paid_pusd)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(payments, vec![(DAY_ZERO, Decimal::ONE)]);

    CrossDayOutcome {
        events: settlements
            .into_iter()
            .map(|feedback| feedback.event.clone())
            .collect(),
        available_pusd: session.available_pusd(),
        next_account_transition_seq: session.next_account_transition_seq(),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task342_cross_day_floor_remainder_truncation_and_clean_replay_are_stable() {
    let first = run_cross_day_case("task342-cross-day").await;
    let replay = run_cross_day_case("task342-cross-day").await;
    assert_eq!(first, replay);
}

#[derive(Debug, PartialEq, Eq)]
struct FiveWayCollisionOutcome {
    target_delta_order: Vec<(PlatformAccountDeltaReasonV1, u64)>,
    account_summary_sha256: contracts::Sha256Hex,
    next_account_transition_seq: u64,
    gtd_expired_base_units: u128,
    gtd_matched_base_units: u128,
    dpt_maker_trade_id: String,
    new_buy_platform_order_id: String,
    settlement_emission_order: Vec<u64>,
}

fn accepted_response(
    captures: &Task34Captures,
    request_id: &str,
) -> Arc<PostOrderDirectResponseV1> {
    captures
        .post_order_responses()
        .into_iter()
        .find(|response| response.metadata.global_request_id == request_id)
        .unwrap_or_else(|| panic!("任务 3 阶段完成门缺少 {request_id} 直接响应"))
}

fn accepted_order_id(captures: &Task34Captures, request_id: &str) -> String {
    let response = accepted_response(captures, request_id);
    let SubmissionItemOutcomeV1::Accepted(accepted) = &response.item.outcome else {
        panic!(
            "任务 3 阶段完成门订单 {request_id} 必须接受，实际为 {:?}",
            response.item.outcome
        )
    };
    accepted.simulated_platform_order_id.clone()
}

fn accepted_trade_id(captures: &Task34Captures, request_id: &str) -> String {
    let response = accepted_response(captures, request_id);
    let SubmissionItemOutcomeV1::Accepted(accepted) = &response.item.outcome else {
        panic!(
            "任务 3 阶段完成门订单 {request_id} 必须接受，实际为 {:?}",
            response.item.outcome
        )
    };
    assert_eq!(accepted.fills.len(), 1, "前置 taker 订单必须恰有一笔成交");
    accepted.fills[0].simulated_trade_id.clone()
}

fn assert_direct_predecessor(
    predecessor: &PlatformAccountDeltaV1,
    successor: &PlatformAccountDeltaV1,
) {
    assert_eq!(
        successor.account_transition_seq,
        predecessor.account_transition_seq + 1,
        "五类碰撞中的账户变化必须连续"
    );
    assert!(matches!(
        &successor.predecessor,
        PlatformAccountDeltaPredecessorV1::AccountDelta {
            delta_id,
            delta_sha256,
            account_transition_seq,
        } if delta_id == &predecessor.delta_id
            && delta_sha256 == &predecessor.delta_sha256
            && *account_transition_seq == predecessor.account_transition_seq
    ));
}

async fn run_five_way_collision_case(segmented_delivery: bool) -> FiveWayCollisionOutcome {
    const SESSION_ID: &str = "task3-five-way-collision";
    const PREVIOUS_TAKER_REQUEST: &str = "task3-five-way-previous-taker";
    const DPT_MAKER_REQUEST: &str = "task3-five-way-dpt-maker";
    const GTD_REQUEST: &str = "task3-five-way-gtd";
    const NEW_BUY_REQUEST: &str = "task3-five-way-new-buy";
    const INGRESS_PROBE_REQUEST: &str = "task3-five-way-ingress-probe";

    let target_at = TAKER_MATURITY;
    let previous_fill_at = DAY_ZERO + 700_000;
    let previous_market = market_for_slot(0, DAY_ZERO + 600_000, DAY_ZERO + 900_000);
    let collision_market = market_for_slot(1, target_at - 270_000, target_at + 30_000);
    assert_eq!(
        previous_market.market().window.end_at_utc_ms
            - previous_market.market().window.start_at_utc_ms,
        300_000
    );
    assert_eq!(
        collision_market.market().window.end_at_utc_ms
            - collision_market.market().window.start_at_utc_ms,
        300_000
    );

    let policy = policy_for_tests(
        "task3-five-way-collision-policy",
        &[
            (TradeFinalityStateV1::Matched, 0),
            (TradeFinalityStateV1::Mined, 1),
            (
                TradeFinalityStateV1::Confirmed,
                target_at - previous_fill_at,
            ),
        ],
        2,
        vec![(
            TradeFinalitySelectorV1::Request {
                account_instance_key: account_key(SESSION_ID),
                global_request_id: DPT_MAKER_REQUEST.to_owned(),
            },
            vec![
                (TradeFinalityStateV1::Matched, 0),
                (TradeFinalityStateV1::Failed, 100_000),
            ],
            Some(0),
        )],
    );
    let captures = Task34Captures::default();
    let rules = historical_snapshot_for_sim_tests_with_fee_schedule("0.07", 1).await;
    let (mut session, profile) = new_multi_market_session(
        SESSION_ID,
        decimal("1000"),
        rules,
        policy,
        &[previous_market.clone(), collision_market.clone()],
        &captures,
    );

    load_initial_market(
        &mut session,
        &previous_market,
        DAY_ZERO + 610_000,
        "0.4",
        "1",
        "0.5",
        "1000",
    );
    let previous_taker_request = fak_request_for_effective_at(
        &session,
        &profile,
        &previous_market,
        PREVIOUS_TAKER_REQUEST,
        1,
        previous_fill_at,
        "357.142857",
        "0.5",
    );
    drive_post_order(&mut session, previous_taker_request, previous_fill_at);
    if segmented_delivery {
        drain_accepting(&mut session, previous_fill_at + 29_999);
    }

    load_initial_market(
        &mut session,
        &collision_market,
        target_at - 270_000,
        "0.4",
        "1",
        "0.6",
        "1000",
    );
    let dpt_maker_request = gtc_request_for_effective_at(
        &session,
        &profile,
        &collision_market,
        DPT_MAKER_REQUEST,
        2,
        target_at - 250_000,
        "0.4",
        "5",
    );
    drive_post_order(&mut session, dpt_maker_request, target_at - 250_000);
    let gtd_request = gtd_request_for_effective_at(
        &session,
        &profile,
        &collision_market,
        GTD_REQUEST,
        3,
        target_at - 240_000,
        target_at + 999,
        "0.4",
        "5",
    );
    drive_post_order(&mut session, gtd_request, target_at - 240_000);
    if segmented_delivery {
        drain_accepting(&mut session, target_at - 200_000);
    }

    let add_sequence = session.platform_event_index_position() + 1;
    apply_bid_transition(
        &mut session,
        &collision_market,
        target_at - 1,
        add_sequence,
        "1",
        "6",
    );
    let new_buy = gtc_request_for_effective_at(
        &session,
        &profile,
        &collision_market,
        NEW_BUY_REQUEST,
        4,
        target_at,
        "0.3",
        "5",
    );
    let new_buy_registration = session.post_order(new_buy).expect("同刻新 BUY 登记");
    assert_eq!(
        new_buy_registration.item.effective_processing_at_utc_ms,
        target_at
    );

    let snapshot_before_unclosed_drive = session.account_snapshot_for_tests();
    let summary_before_unclosed_drive = session.account_summary_sha256().clone();
    let seq_before_unclosed_drive = session.next_account_transition_seq();
    let order_count_before_unclosed_drive = session.simulated_order_count();
    assert_eq!(
        session.drive_next_execution_action_through(target_at),
        Err(SubmissionProcessingErrorV1::Session(
            SessionError::PlatformPrefixNotClosed
        ))
    );
    assert_eq!(
        session.account_snapshot_for_tests(),
        snapshot_before_unclosed_drive
    );
    assert_eq!(
        session.account_summary_sha256(),
        &summary_before_unclosed_drive
    );
    assert_eq!(
        session.next_account_transition_seq(),
        seq_before_unclosed_drive
    );
    assert_eq!(
        session.simulated_order_count(),
        order_count_before_unclosed_drive
    );

    let ingress_probe = gtc_request_for_effective_at(
        &session,
        &profile,
        &collision_market,
        INGRESS_PROBE_REQUEST,
        5,
        target_at + 100_000,
        "0.3",
        "5",
    );
    session
        .post_order(ingress_probe)
        .expect("非致命前缀错误后必须仍接受新登记");

    let target_sequence = session.platform_event_index_position() + 1;
    let target_event = rebind_event(
        bid_transition(target_at, target_at + 100, target_sequence, "0.4", "6", "0"),
        &collision_market,
    );
    let target_events = vec![target_event];
    assert_eq!(target_events.len(), 1, "完成门必须使用非空 DPT 桶");
    let target_bucket = hidden_bucket(target_at, target_sequence, &target_events);
    assert_eq!(
        session.apply_closed_market_event_bucket(target_bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );

    assert_eq!(
        session.drive_next_execution_action_through(target_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );

    let seq_after_target = session.next_account_transition_seq();
    let summary_after_target = session.account_summary_sha256().clone();
    assert_eq!(
        session.apply_closed_market_event_bucket(target_bucket),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(
        session.drive_next_execution_action_through(target_at),
        Ok(SubmissionDriveDispositionV1::NoAction)
    );
    assert_eq!(session.next_account_transition_seq(), seq_after_target);
    assert_eq!(session.account_summary_sha256(), &summary_after_target);
    drain_accepting(&mut session, target_at + 29_999);

    let previous_trade_id = accepted_trade_id(&captures, PREVIOUS_TAKER_REQUEST);
    let dpt_maker_order_id = accepted_order_id(&captures, DPT_MAKER_REQUEST);
    let gtd_order_id = accepted_order_id(&captures, GTD_REQUEST);
    let new_buy_order_id = accepted_order_id(&captures, NEW_BUY_REQUEST);

    let gtd_order = session
        .simulated_order(&gtd_order_id)
        .expect("同刻 GTD 订单保留查询证据");
    assert_eq!(gtd_order.expired_at_utc_ms, Some(target_at));
    assert_eq!(gtd_order.expired_base_units, 5_000_000);
    assert_eq!(gtd_order.matched_base_units, 0, "GTD 不得被同刻 DPT 成交");
    assert_eq!(gtd_order.open_base_units, 0);

    assert_eq!(session.simulated_fill_count(&dpt_maker_order_id), 1);
    let dpt_maker_fill = session
        .simulated_fill(&dpt_maker_order_id)
        .expect("非空 DPT 桶必须形成 maker 成交");
    assert_eq!(dpt_maker_fill.matched_at_utc_ms, target_at);
    assert_eq!(
        dpt_maker_fill.condition_id,
        collision_market.market().condition_id.0
    );
    let dpt_maker_trade_id = dpt_maker_fill.trade_id.clone();
    assert_ne!(dpt_maker_trade_id, previous_trade_id);

    let gtd_delta = captures
        .order_feedbacks()
        .into_iter()
        .find(|feedback| {
            feedback.platform_order_id == gtd_order_id
                && feedback.event_kind == OrderLifecycleEventKindV1::GtdExpired
                && feedback.metadata.platform_event_at_utc_ms == target_at
        })
        .expect("同刻 GTD 到期反馈")
        .account_delta
        .clone();
    let previous_finality_delta = captures
        .trade_feedbacks()
        .into_iter()
        .find(|feedback| {
            feedback.simulated_trade_id == previous_trade_id
                && feedback.status.known == Some(SimulatedTradeKnownStatusV1::Confirmed)
                && feedback.metadata.platform_event_at_utc_ms == target_at
        })
        .expect("同刻先前排定最终性反馈")
        .account_delta
        .clone()
        .expect("先前最终性必须在 Confirmed 应用经济变化");
    let dpt_maker_feedback = captures
        .trade_feedbacks()
        .into_iter()
        .find(|feedback| {
            feedback.simulated_trade_id == dpt_maker_trade_id
                && feedback.metadata.platform_event_at_utc_ms == target_at
        })
        .expect("同刻 DPT maker 成交反馈");
    assert_eq!(
        dpt_maker_feedback.liquidity_role,
        TradeLiquidityRoleV1::MakerInferred
    );
    let dpt_maker_delta = dpt_maker_feedback
        .account_delta
        .clone()
        .expect("DPT maker 成交必须在 Matched 应用经济变化");
    let new_buy_delta = captures
        .order_feedbacks()
        .into_iter()
        .find(|feedback| {
            feedback.platform_order_id == new_buy_order_id
                && feedback.event_kind == OrderLifecycleEventKindV1::OrderAccepted
                && feedback.metadata.platform_event_at_utc_ms == target_at
        })
        .expect("同刻新 BUY 接受反馈")
        .account_delta
        .clone();

    let target_settlements = captures
        .unique_settlements()
        .into_iter()
        .filter(|feedback| feedback.metadata.platform_event_at_utc_ms == target_at)
        .collect::<Vec<_>>();
    let taker_accrual = target_settlements
        .iter()
        .find(|feedback| {
            matches!(
                feedback.event,
                SettlementLifecycleEventV1::TakerRebateAccrualApplied { .. }
            )
        })
        .expect("同刻 taker 返佣应计");
    let SettlementLifecycleEventV1::TakerRebateAccrualApplied {
        accrual_pusd,
        account_delta: taker_accrual_delta,
        ..
    } = &taker_accrual.event
    else {
        unreachable!()
    };
    assert_eq!(*accrual_pusd, Decimal::ONE);
    let taker_day = target_settlements
        .iter()
        .find(|feedback| {
            matches!(
                feedback.event,
                SettlementLifecycleEventV1::TakerRebateDayFinalized { .. }
            )
        })
        .expect("同刻 taker 日结");
    let SettlementLifecycleEventV1::TakerRebateDayFinalized {
        payable_pusd,
        locked_remainder_pusd,
        account_delta: taker_day_delta,
        ..
    } = &taker_day.event
    else {
        unreachable!()
    };
    assert_eq!(*payable_pusd, Decimal::ONE);
    assert_eq!(*locked_remainder_pusd, Decimal::ZERO);
    assert!(taker_day_delta.is_none(), "零余数日结不得生成账户变化");
    let taker_payment = target_settlements
        .iter()
        .find(|feedback| {
            matches!(
                feedback.event,
                SettlementLifecycleEventV1::TakerRebatePaid { .. }
            )
        })
        .expect("同刻 taker 付款");
    let SettlementLifecycleEventV1::TakerRebatePaid {
        paid_pusd,
        account_delta: taker_payment_delta,
        ..
    } = &taker_payment.event
    else {
        unreachable!()
    };
    assert_eq!(*paid_pusd, Decimal::ONE);
    let settlement_emission_order = vec![
        taker_accrual.metadata.private_emission_sequence,
        taker_day.metadata.private_emission_sequence,
        taker_payment.metadata.private_emission_sequence,
    ];
    assert!(
        settlement_emission_order
            .windows(2)
            .all(|pair| pair[0] < pair[1])
    );

    let target_deltas = [
        gtd_delta,
        previous_finality_delta,
        taker_accrual_delta.clone(),
        taker_payment_delta.clone(),
        dpt_maker_delta,
        new_buy_delta,
    ];
    assert_eq!(
        target_deltas
            .iter()
            .map(|delta| delta.reason)
            .collect::<Vec<_>>(),
        vec![
            PlatformAccountDeltaReasonV1::GtdExpirationEncumbranceReleased,
            PlatformAccountDeltaReasonV1::FillFinalityEconomicApplied,
            PlatformAccountDeltaReasonV1::TakerRebateAccrualApplied,
            PlatformAccountDeltaReasonV1::TakerRebatePaid,
            PlatformAccountDeltaReasonV1::FillFinalityEconomicApplied,
            PlatformAccountDeltaReasonV1::OrderAcceptedEncumbranceApplied,
        ]
    );
    assert!(
        target_deltas
            .iter()
            .all(|delta| delta.platform_effective_at_utc_ms == target_at)
    );
    for pair in target_deltas.windows(2) {
        assert_direct_predecessor(&pair[0], &pair[1]);
    }

    FiveWayCollisionOutcome {
        target_delta_order: target_deltas
            .iter()
            .map(|delta| (delta.reason, delta.account_transition_seq))
            .collect(),
        account_summary_sha256: session.account_summary_sha256().clone(),
        next_account_transition_seq: session.next_account_transition_seq(),
        gtd_expired_base_units: gtd_order.expired_base_units,
        gtd_matched_base_units: gtd_order.matched_base_units,
        dpt_maker_trade_id,
        new_buy_platform_order_id: new_buy_order_id,
        settlement_emission_order,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn stage3_five_way_same_time_collision_uses_one_stable_platform_order() {
    let direct = run_five_way_collision_case(false).await;
    let segmented_clean_replay = run_five_way_collision_case(true).await;
    assert_eq!(segmented_clean_replay, direct);
}
