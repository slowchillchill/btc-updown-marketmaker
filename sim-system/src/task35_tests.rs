use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use contracts::{
    AccountInstanceKeyV1, CanonicalSourceKind, EventQualityV2, ExactDecimal,
    ExecutionMarketEventPayloadV3, ExecutionMarketEventV3, MarketDataQuality,
    MarketResolutionTerminalV3, PayoutEntryV1, PlatformAccountDeltaReasonV1,
    PlatformAccountSnapshotV1, PlatformOutcomeBalanceV1, PlatformSourceV1, QualityBoundaryV2,
    QualityScopeV1, ResyncQuality, Sha256Hex, SimCallerRoleV1, SourceEvidenceRefV2,
    SourceNativeIdentityV1,
};
use data_pipeline_core::{derive_execution_market_event_v3_id, encode_execution_market_event_v3};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};

use crate::execution::{
    CancelNotCanceledReasonV1, CancelOrderDirectResponseV1, CancelOrderItemV1,
    CancelOrderRequestV1, InitialAccountStateV1, OrderDirectResponseV1, OrderLifecycleEventKindV1,
    OrderLifecycleFeedbackV1, OrderQueryResultV1, OrderRequestV1, PlatformMarketV1,
    PlatformScenarioV1, PostOrderDirectResponseV1, PostOrderItemV1, PostOrderRequestV1,
    SettlementLifecycleEventV1, SettlementLifecycleFeedbackV1, SimBuyOrderKindV1, SimBuyOrderV1,
    SimExecutionProcessV1, SimExecutionSessionV1, SimulatedOrderKnownStatusV1,
    SimulatedTradeKnownStatusV1, SubmissionDeliveryDispositionV1, SubmissionDriveDispositionV1,
    SubmissionItemOutcomeV1, SubmissionRejectedReasonV1, SubmissionResponseAcceptanceV1,
    SubmissionResponseBridgeV1, TradeEconomicDispositionV1, TradeLifecycleFeedbackV1,
};
use crate::finality::{
    TradeFinalityPolicyV1, TradeFinalitySelectorV1, TradeFinalityStateV1, policy_for_tests,
};
use crate::latency_profile::{EmpiricalDelayProfile, load_embedded_proxy_latency_profile};
use crate::market::{
    ClosedMarketEventBucketV1, MarketBucketApplyDispositionV1, MarketEventErrorV1,
    MarketLifecyclePhaseV1, MillisecondBucketClosureV1,
};
use crate::platform_rules::tests::successful_snapshot_for_sim_tests;
use crate::task21_tests::{
    asset, fresh_quality, initial_snapshot_event, market as base_market, orderbook_evidence,
    sealed_event, session_spec,
};
use crate::task22_tests::context_for_arrival;
use crate::task23_tests::{bid_transition, session_with_one_pending_maker_depletion_with_bridge};

const RUN_ID: &str = "component-test-run";
const MARKET_START: i64 = 100_000;
const MARKET_END: i64 = 500_000;
const FILL_AT: i64 = 200_000;
const EARLY_RESOLUTION_AT: i64 = 550_000;
const FINALITY_AT: i64 = 600_000;

#[derive(Clone, Debug)]
enum LifecycleAttemptV1 {
    Order(Arc<OrderLifecycleFeedbackV1>),
    Trade(Arc<TradeLifecycleFeedbackV1>),
    Settlement(Arc<SettlementLifecycleFeedbackV1>),
}

impl LifecycleAttemptV1 {
    fn delivery_id(&self) -> &str {
        match self {
            Self::Order(value) => &value.metadata.delivery_id,
            Self::Trade(value) => &value.metadata.delivery_id,
            Self::Settlement(value) => &value.metadata.delivery_id,
        }
    }

    fn received_at_utc_ms(&self) -> i64 {
        match self {
            Self::Order(value) => value.metadata.feedback_received_at_utc_ms,
            Self::Trade(value) => value.metadata.feedback_received_at_utc_ms,
            Self::Settlement(value) => value.metadata.feedback_received_at_utc_ms,
        }
    }

    fn platform_event_at_utc_ms(&self) -> i64 {
        match self {
            Self::Order(value) => value.metadata.platform_event_at_utc_ms,
            Self::Trade(value) => value.metadata.platform_event_at_utc_ms,
            Self::Settlement(value) => value.metadata.platform_event_at_utc_ms,
        }
    }

    fn private_emission_sequence(&self) -> u64 {
        match self {
            Self::Order(value) => value.metadata.private_emission_sequence,
            Self::Trade(value) => value.metadata.private_emission_sequence,
            Self::Settlement(value) => value.metadata.private_emission_sequence,
        }
    }

    fn private_feedback_sequence(&self) -> u64 {
        match self {
            Self::Order(value) => value.metadata.private_feedback_sequence,
            Self::Trade(value) => value.metadata.private_feedback_sequence,
            Self::Settlement(value) => value.metadata.private_feedback_sequence,
        }
    }

    fn same_arc(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Order(left), Self::Order(right)) => Arc::ptr_eq(left, right),
            (Self::Trade(left), Self::Trade(right)) => Arc::ptr_eq(left, right),
            (Self::Settlement(left), Self::Settlement(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }
}

#[derive(Default)]
struct LifecycleCaptureStateV1 {
    attempts: Vec<LifecycleAttemptV1>,
    full_once_armed: bool,
}

#[derive(Clone, Default)]
struct Task35Captures {
    post_orders: Arc<Mutex<Vec<Arc<PostOrderDirectResponseV1>>>>,
    cancel_orders: Arc<Mutex<Vec<Arc<CancelOrderDirectResponseV1>>>>,
    order_queries: Arc<Mutex<Vec<Arc<OrderDirectResponseV1>>>>,
    lifecycle: Arc<Mutex<LifecycleCaptureStateV1>>,
}

impl Task35Captures {
    fn bridge(&self) -> SubmissionResponseBridgeV1 {
        let post_orders = Arc::clone(&self.post_orders);
        let cancel_orders = Arc::clone(&self.cancel_orders);
        let order_queries = Arc::clone(&self.order_queries);
        let order_lifecycle = Arc::clone(&self.lifecycle);
        let trade_lifecycle = Arc::clone(&self.lifecycle);
        let settlement_lifecycle = Arc::clone(&self.lifecycle);
        SubmissionResponseBridgeV1::new(
            move |response| {
                post_orders
                    .lock()
                    .expect("任务 3.5 单笔提交响应捕获锁")
                    .push(Arc::clone(&response));
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            move |response| {
                cancel_orders
                    .lock()
                    .expect("任务 3.5 单笔撤单响应捕获锁")
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
                order_queries
                    .lock()
                    .expect("任务 3.5 单笔订单查询响应捕获锁")
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
                capture_lifecycle(
                    &order_lifecycle,
                    LifecycleAttemptV1::Order(Arc::clone(&feedback)),
                    feedback.metadata.delivery_confirmation(),
                )
            },
            move |feedback| {
                capture_lifecycle(
                    &trade_lifecycle,
                    LifecycleAttemptV1::Trade(Arc::clone(&feedback)),
                    feedback.metadata.delivery_confirmation(),
                )
            },
            move |feedback| {
                capture_lifecycle(
                    &settlement_lifecycle,
                    LifecycleAttemptV1::Settlement(Arc::clone(&feedback)),
                    feedback.metadata.delivery_confirmation(),
                )
            },
        )
    }

    fn arm_full_once(&self) {
        self.lifecycle
            .lock()
            .expect("任务 3.5 生命周期捕获锁")
            .full_once_armed = true;
    }

    fn clear_lifecycle_attempts(&self) {
        self.lifecycle
            .lock()
            .expect("任务 3.5 生命周期捕获锁")
            .attempts
            .clear();
    }

    fn lifecycle_attempts(&self) -> Vec<LifecycleAttemptV1> {
        self.lifecycle
            .lock()
            .expect("任务 3.5 生命周期捕获锁")
            .attempts
            .clone()
    }

    fn unique_lifecycle_attempts(&self) -> Vec<LifecycleAttemptV1> {
        let mut seen = BTreeSet::new();
        self.lifecycle_attempts()
            .into_iter()
            .filter(|attempt| seen.insert(attempt.delivery_id().to_owned()))
            .collect()
    }

    fn unique_order_feedbacks(&self) -> Vec<Arc<OrderLifecycleFeedbackV1>> {
        self.unique_lifecycle_attempts()
            .into_iter()
            .filter_map(|attempt| match attempt {
                LifecycleAttemptV1::Order(value) => Some(value),
                _ => None,
            })
            .collect()
    }

    fn unique_trade_feedbacks(&self) -> Vec<Arc<TradeLifecycleFeedbackV1>> {
        self.unique_lifecycle_attempts()
            .into_iter()
            .filter_map(|attempt| match attempt {
                LifecycleAttemptV1::Trade(value) => Some(value),
                _ => None,
            })
            .collect()
    }

    fn unique_settlement_feedbacks(&self) -> Vec<Arc<SettlementLifecycleFeedbackV1>> {
        self.unique_lifecycle_attempts()
            .into_iter()
            .filter_map(|attempt| match attempt {
                LifecycleAttemptV1::Settlement(value) => Some(value),
                _ => None,
            })
            .collect()
    }
}

fn capture_lifecycle(
    capture: &Arc<Mutex<LifecycleCaptureStateV1>>,
    attempt: LifecycleAttemptV1,
    confirmation: crate::execution::ResponseDeliveryConfirmationV1,
) -> SubmissionResponseAcceptanceV1 {
    let mut capture = capture.lock().expect("任务 3.5 生命周期捕获锁");
    capture.attempts.push(attempt);
    if capture.full_once_armed {
        capture.full_once_armed = false;
        SubmissionResponseAcceptanceV1::Full
    } else {
        SubmissionResponseAcceptanceV1::Accepted(confirmation)
    }
}

fn decimal(value: &str) -> Decimal {
    value.parse().expect("任务 3.5 测试十进制")
}

fn exact_decimal(value: &ExactDecimal) -> Decimal {
    decimal(&value.0)
}

fn outcome_balance(snapshot: &PlatformAccountSnapshotV1, token_id: &str) -> Decimal {
    snapshot
        .outcome_balances
        .iter()
        .find(|balance| balance.token_id.0 == token_id)
        .map(|balance| exact_decimal(&balance.balance_shares))
        .unwrap_or(Decimal::ZERO)
}

fn account_key(session_id: &str, role: SimCallerRoleV1) -> AccountInstanceKeyV1 {
    AccountInstanceKeyV1 {
        run_id: RUN_ID.to_owned(),
        caller_role: role,
        caller_instance_id: format!("{session_id}-caller"),
        account_id: format!("{session_id}-account"),
        platform_source: PlatformSourceV1::Simulated,
    }
}

fn market_between(start_at_utc_ms: i64, end_at_utc_ms: i64) -> PlatformMarketV1 {
    let mut identity = base_market();
    identity.window.start_at_utc_ms = start_at_utc_ms;
    identity.window.end_at_utc_ms = end_at_utc_ms;
    PlatformMarketV1::try_new(identity, [asset("2", 1, "Down"), asset("1", 0, "Up")])
        .expect("任务 3.5 单市场")
}

fn scenario_for(market: &PlatformMarketV1) -> Arc<PlatformScenarioV1> {
    Arc::new(PlatformScenarioV1::try_new(vec![market.clone()]).expect("任务 3.5 单市场场景"))
}

fn success_policy(policy_id: &str, confirmed_offset_ms: i64) -> Arc<TradeFinalityPolicyV1> {
    policy_for_tests(
        policy_id,
        &[
            (TradeFinalityStateV1::Matched, 0),
            (TradeFinalityStateV1::Mined, 1),
            (TradeFinalityStateV1::Confirmed, confirmed_offset_ms),
        ],
        0,
        Vec::new(),
    )
}

fn failure_policy(
    session_id: &str,
    request_id: &str,
    failed_offset_ms: i64,
) -> Arc<TradeFinalityPolicyV1> {
    policy_for_tests(
        &format!("{session_id}-failure-policy"),
        &[
            (TradeFinalityStateV1::Matched, 0),
            (TradeFinalityStateV1::Mined, 1),
            (TradeFinalityStateV1::Confirmed, failed_offset_ms),
        ],
        0,
        vec![(
            TradeFinalitySelectorV1::Request {
                account_instance_key: account_key(session_id, SimCallerRoleV1::ModelTrain),
                global_request_id: request_id.to_owned(),
            },
            vec![
                (TradeFinalityStateV1::Matched, 0),
                (TradeFinalityStateV1::Failed, failed_offset_ms),
            ],
            Some(0),
        )],
    )
}

async fn new_session(
    session_id: &str,
    role: SimCallerRoleV1,
    available_pusd: Decimal,
    capacity: usize,
    policy: Arc<TradeFinalityPolicyV1>,
    market: &PlatformMarketV1,
    captures: &Task35Captures,
) -> (SimExecutionSessionV1, Arc<EmpiricalDelayProfile>) {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("任务 3.5 代理时延配置档");
    let scenario = scenario_for(market);
    let mut spec = session_spec(&scenario, role, session_id);
    spec.initial_account_state =
        InitialAccountStateV1::try_new(available_pusd).expect("任务 3.5 显式初始资金");
    spec.response_outbox_capacity = NonZeroUsize::new(capacity).expect("任务 3.5 正反馈槽容量");
    let mut process = SimExecutionProcessV1::try_new(
        RUN_ID.to_owned(),
        policy,
        rules,
        Arc::clone(&profile),
        scenario,
    )
    .expect("任务 3.5 运行根");
    (
        process.create_active_session_for_tests(spec, captures.bridge()),
        profile,
    )
}

fn rebind_event(
    mut event: ExecutionMarketEventV3,
    market: &PlatformMarketV1,
) -> ExecutionMarketEventV3 {
    event.market = market.market().clone();
    if let ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition) = &mut event.payload
    {
        transition.market = market.market().clone();
    }
    event.event_id = derive_execution_market_event_v3_id(&event).expect("任务 3.5 事件身份");
    event
}

#[allow(clippy::too_many_arguments)]
fn initial_event(
    market: &PlatformMarketV1,
    token_id: &str,
    outcome_index: u32,
    label: &str,
    source_at_utc_ms: i64,
    received_at_utc_ms: i64,
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
        received_at_utc_ms,
        sequence,
        bid_quantity,
    );
    let ExecutionMarketEventPayloadV3::InitialL2Snapshot(snapshot) = &mut event.payload else {
        panic!("任务 3.5 初始夹具必须是 L2 快照");
    };
    snapshot.initial_tick_size_pusd_per_share = ExactDecimal("0.01".to_owned());
    snapshot.bids[0].price_pusd_per_share = ExactDecimal(bid_price.to_owned());
    snapshot.bids[0].quantity_shares = ExactDecimal(bid_quantity.to_owned());
    snapshot.asks[0].price_pusd_per_share = ExactDecimal(ask_price.to_owned());
    snapshot.asks[0].quantity_shares = ExactDecimal(ask_quantity.to_owned());
    rebind_event(event, market)
}

fn encoded(event: &ExecutionMarketEventV3) -> Vec<u8> {
    encode_execution_market_event_v3(event).expect("任务 3.5 规范事件编码")
}

fn bucket(
    role: SimCallerRoleV1,
    at_utc_ms: i64,
    start_position: u64,
    next_position: u64,
    events: &[ExecutionMarketEventV3],
) -> ClosedMarketEventBucketV1 {
    let encoded = events.iter().map(encoded).collect::<Vec<_>>();
    match role {
        SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
            ClosedMarketEventBucketV1::hidden(
                at_utc_ms,
                start_position,
                next_position,
                MillisecondBucketClosureV1::NextEventAt(at_utc_ms + 1),
                encoded,
            )
        }
        SimCallerRoleV1::Paper => ClosedMarketEventBucketV1::public(
            at_utc_ms,
            start_position,
            next_position,
            MillisecondBucketClosureV1::NextEventAt(at_utc_ms + 1),
            encoded,
        ),
    }
}

fn role_position(session: &SimExecutionSessionV1) -> u64 {
    match session.session_spec().caller_role {
        SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
            session.platform_event_index_position()
        }
        SimCallerRoleV1::Paper => session.public_event_index_position(),
    }
}

fn apply_events(
    session: &mut SimExecutionSessionV1,
    at_utc_ms: i64,
    events: &[ExecutionMarketEventV3],
) -> MarketBucketApplyDispositionV1 {
    let start = role_position(session);
    let next = start + u64::try_from(events.len()).expect("任务 3.5 事件数适配 u64");
    session
        .apply_closed_market_event_bucket(bucket(
            session.session_spec().caller_role,
            at_utc_ms,
            start,
            next,
            events,
        ))
        .expect("任务 3.5 角色授权平台桶")
}

fn apply_empty_at(
    session: &mut SimExecutionSessionV1,
    at_utc_ms: i64,
) -> MarketBucketApplyDispositionV1 {
    apply_events(session, at_utc_ms, &[])
}

fn load_initial_market(
    session: &mut SimExecutionSessionV1,
    market: &PlatformMarketV1,
    ask_quantity: &str,
) {
    let events = [
        initial_event(
            market,
            "1",
            0,
            "Up",
            110_000,
            120_000,
            1,
            "0.4",
            "1",
            "0.4",
            ask_quantity,
        ),
        initial_event(
            market,
            "2",
            1,
            "Down",
            110_000,
            120_000,
            2,
            "0.4",
            "1",
            "0.4",
            ask_quantity,
        ),
    ];
    let at = match session.session_spec().caller_role {
        SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => 110_000,
        SimCallerRoleV1::Paper => 120_000,
    };
    assert_eq!(
        apply_events(session, at, &events),
        MarketBucketApplyDispositionV1::Applied
    );
}

fn resolution_event(
    market: &PlatformMarketV1,
    source_at_utc_ms: i64,
    received_at_utc_ms: i64,
    sequence: u64,
) -> ExecutionMarketEventV3 {
    rebind_event(
        sealed_event(
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
                    mapping_rule_version: "task35-resolution-v1".to_owned(),
                },
            ),
            orderbook_evidence(source_at_utc_ms, received_at_utc_ms, sequence),
            fresh_quality(),
        ),
        market,
    )
}

fn receive_only_resolution_event(
    market: &PlatformMarketV1,
    received_at_utc_ms: i64,
) -> ExecutionMarketEventV3 {
    let fixture_sha = |label: &str| {
        Sha256Hex(format!(
            "{:x}",
            Sha256::digest(format!("task35:{label}:{received_at_utc_ms}").as_bytes())
        ))
    };
    let evidence = SourceEvidenceRefV2 {
        source_kind: CanonicalSourceKind::PolymarketMarketMetadata,
        source_native_identity: Some(SourceNativeIdentityV1::SemanticEvidenceSha256 {
            sha256: fixture_sha("semantic-evidence"),
        }),
        source_event_at_utc_ms: None,
        received_at_utc_ms,
        collector_admission: None,
        input_manifest_sha256: fixture_sha("manifest"),
        input_file_sha256: fixture_sha("file"),
    };
    rebind_event(
        sealed_event(
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
                    mapping_rule_version: "task35-resolution-v1".to_owned(),
                },
            ),
            evidence,
            fresh_quality(),
        ),
        market,
    )
}

fn resolution_conflict_event(
    market: &PlatformMarketV1,
    source_at_utc_ms: i64,
    received_at_utc_ms: i64,
    sequence: u64,
) -> ExecutionMarketEventV3 {
    rebind_event(
        sealed_event(
            ExecutionMarketEventPayloadV3::QualityBoundary(QualityBoundaryV2 {
                scope: QualityScopeV1::Market,
                previous_quality: MarketDataQuality::Fresh,
                current_quality: MarketDataQuality::Invalid,
                reason_code: "POLYMARKET_TERMINAL_DUPLICATE_CONFLICT".to_owned(),
            }),
            orderbook_evidence(source_at_utc_ms, received_at_utc_ms, sequence),
            EventQualityV2 {
                availability: MarketDataQuality::Invalid,
                resync: ResyncQuality::NotRequired,
            },
        ),
        market,
    )
}

#[allow(clippy::too_many_arguments)]
fn gtc_request(
    session: &SimExecutionSessionV1,
    profile: &EmpiricalDelayProfile,
    market: &PlatformMarketV1,
    request_id: &str,
    caller_sequence: u64,
    effective_at_utc_ms: i64,
    token_id: &str,
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
                token_id: asset(
                    token_id,
                    u32::from(token_id == "2"),
                    if token_id == "1" { "Up" } else { "Down" },
                )
                .token_id,
                kind: SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: decimal(price),
                    size_shares: decimal(size),
                    post_only: false,
                },
            },
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn fak_request(
    session: &SimExecutionSessionV1,
    profile: &EmpiricalDelayProfile,
    market: &PlatformMarketV1,
    request_id: &str,
    caller_sequence: u64,
    effective_at_utc_ms: i64,
    token_id: &str,
    spend: &str,
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
                token_id: asset(
                    token_id,
                    u32::from(token_id == "2"),
                    if token_id == "1" { "Up" } else { "Down" },
                )
                .token_id,
                kind: SimBuyOrderKindV1::FakMarket {
                    spend_pusd: decimal(spend),
                    worst_price_pusd_per_share: decimal("0.6"),
                },
            },
        },
    }
}

fn drive_post_order(
    session: &mut SimExecutionSessionV1,
    request: PostOrderRequestV1,
    expected_effective_at_utc_ms: i64,
) {
    let registration = session.post_order(request).expect("任务 3.5 订单登记");
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

fn drain_accepting(session: &mut SimExecutionSessionV1, through_utc_ms: i64) {
    for _ in 0..32 {
        match session
            .drive_private_delivery_after_platform_through_for_tests(through_utc_ms)
            .expect("任务 3.5 私有事实交付")
        {
            SubmissionDeliveryDispositionV1::Delivered => continue,
            SubmissionDeliveryDispositionV1::NoVisibleResponse => return,
            disposition => panic!("任务 3.5 接受桥不应返回 {disposition:?}"),
        }
    }
    panic!("任务 3.5 私有事实未在有界次数内排空");
}

fn accepted_order_and_trade(captures: &Task35Captures, request_id: &str) -> (String, String) {
    let responses = captures
        .post_orders
        .lock()
        .expect("任务 3.5 提交响应捕获锁");
    let response = responses
        .iter()
        .find(|response| response.metadata.global_request_id == request_id)
        .expect("任务 3.5 指定提交响应");
    let SubmissionItemOutcomeV1::Accepted(accepted) = &response.item.outcome else {
        panic!("任务 3.5 订单必须接受");
    };
    (
        accepted.simulated_platform_order_id.clone(),
        accepted
            .fills
            .first()
            .expect("任务 3.5 订单必须形成一笔成交")
            .simulated_trade_id
            .clone(),
    )
}

fn auto_redeem_feedback(captures: &Task35Captures) -> Arc<SettlementLifecycleFeedbackV1> {
    captures
        .unique_settlement_feedbacks()
        .into_iter()
        .find(|feedback| {
            matches!(
                feedback.event,
                SettlementLifecycleEventV1::AutoRedeemApplied { .. }
            )
        })
        .expect("任务 3.5 唯一自动赎回反馈")
}

fn market_resolution_order_feedback(captures: &Task35Captures) -> Arc<OrderLifecycleFeedbackV1> {
    captures
        .unique_order_feedbacks()
        .into_iter()
        .find(|feedback| feedback.event_kind == OrderLifecycleEventKindV1::MarketResolvedCanceled)
        .expect("任务 3.5 判定关闭订单反馈")
}

fn assert_redeemed_balance(
    balances: &[PlatformOutcomeBalanceV1],
    token_id: &str,
    expected: Decimal,
) {
    let actual = balances
        .iter()
        .find(|balance| balance.token_id.0 == token_id)
        .map(|balance| exact_decimal(&balance.balance_shares))
        .unwrap_or(Decimal::ZERO);
    assert_eq!(actual, expected);
}

#[tokio::test(flavor = "current_thread")]
async fn task351_resolution_closes_pending_and_open_once_before_same_time_cancel() {
    let captures = Task35Captures::default();
    let post_order_capture = Arc::clone(&captures.post_orders);
    let (mut session, platform_order_id, _, _, _, _) =
        session_with_one_pending_maker_depletion_with_bridge(
            "task351-resolution-close",
            post_order_capture,
            captures.bridge(),
            100,
        )
        .await;
    let profile = load_embedded_proxy_latency_profile().expect("任务 3.5 代理时延配置档");
    let market =
        PlatformMarketV1::try_new(base_market(), [asset("2", 1, "Down"), asset("1", 0, "Up")])
            .expect("任务 3.5 基准市场");
    let end_at = market.market().window.end_at_utc_ms;

    drain_accepting(&mut session, end_at - 1);
    captures.clear_lifecycle_attempts();
    let pending_before = session
        .simulated_order(&platform_order_id)
        .expect("任务 3.5 判定前活动订单")
        .clone();
    assert_eq!(pending_before.pending_maker_depletion_base_units, 1);
    assert_eq!(pending_before.open_base_units, 4_999_999);
    assert_eq!(pending_before.open_quote_units, 50);
    let account_before_end = session.account_snapshot_for_tests();
    let next_seq_before_end = session.next_account_transition_seq();

    assert_eq!(
        apply_empty_at(&mut session, end_at),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(
        session.simulated_order(&platform_order_id),
        Some(&pending_before)
    );
    assert_eq!(session.account_snapshot_for_tests(), account_before_end);

    let post_end_l2 = rebind_event(
        bid_transition(end_at + 1, end_at + 101, 7, "0.00001", "0", "0.1"),
        &market,
    );
    assert_eq!(
        apply_events(&mut session, end_at + 1, &[post_end_l2]),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(
        session.simulated_order(&platform_order_id),
        Some(&pending_before)
    );
    assert_eq!(session.next_account_transition_seq(), next_seq_before_end);

    let resolution_at = end_at + 2;
    let cancel_registration = session
        .cancel_order(CancelOrderRequestV1 {
            context: context_for_arrival(
                &session,
                &profile,
                "task351-same-time-cancel",
                2,
                resolution_at,
            ),
            idempotency_key: "task351-same-time-cancel-idem".to_owned(),
            item: CancelOrderItemV1 {
                item_id: "task351-same-time-cancel-item".to_owned(),
                platform_order_id: platform_order_id.clone(),
            },
        })
        .expect("任务 3.5 同刻撤单登记");
    assert_eq!(
        cancel_registration.item.effective_processing_at_utc_ms,
        resolution_at
    );

    let resolution = resolution_event(&market, resolution_at, resolution_at + 100, 8);
    let resolution_event_id = resolution.event_id.0.clone();
    let start = role_position(&session);
    let resolution_bucket = bucket(
        SimCallerRoleV1::ModelTrain,
        resolution_at,
        start,
        start + 1,
        std::slice::from_ref(&resolution),
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(resolution_bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert!(captures.lifecycle_attempts().is_empty());
    assert_eq!(
        session.apply_closed_market_event_bucket(resolution_bucket),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(
        session.drive_next_execution_action_through(resolution_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    assert_eq!(
        session.next_account_transition_seq(),
        next_seq_before_end + 1,
        "零持仓赎回不能伪造账户变化"
    );
    assert_eq!(
        session
            .platform_market_state(&market.market().condition_id.0)
            .expect("任务 3.5 市场状态")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::ResolutionTerminal
    );

    assert_eq!(
        session
            .drive_private_delivery_after_platform_through_for_tests(resolution_at)
            .expect("任务 3.5 判定时点前私有可见性"),
        SubmissionDeliveryDispositionV1::NoVisibleResponse
    );
    drain_accepting(&mut session, resolution_at + 40_000);

    let resolution_feedback = market_resolution_order_feedback(&captures);
    assert_eq!(
        resolution_feedback.order_status.raw,
        "ORDER_STATUS_CANCELED_MARKET_RESOLVED"
    );
    assert_eq!(
        resolution_feedback.order_status.known,
        Some(SimulatedOrderKnownStatusV1::CanceledMarketResolved)
    );
    assert_eq!(
        resolution_feedback.market_resolved_canceled_base_quantity_shares,
        decimal("5")
    );
    assert_eq!(
        resolution_feedback.market_resolved_released_quote_amount_pusd,
        decimal("0.00005")
    );
    assert_eq!(
        resolution_feedback.account_delta.reason,
        PlatformAccountDeltaReasonV1::MarketResolutionEncumbranceReleased
    );
    assert_eq!(
        exact_decimal(
            &resolution_feedback
                .account_delta
                .balance_delta
                .available_pusd_delta
        ),
        decimal("0.00005")
    );
    assert_eq!(
        exact_decimal(
            &resolution_feedback
                .account_delta
                .balance_delta
                .principal_encumbrance_pusd_delta
        ),
        decimal("-0.00005")
    );
    assert_eq!(
        exact_decimal(
            &resolution_feedback
                .account_delta
                .balance_delta
                .cash_pusd_delta
        ),
        Decimal::ZERO
    );
    assert!(
        resolution_feedback
            .account_delta
            .balance_delta
            .outcome_balance_deltas
            .is_empty()
    );

    let auto = auto_redeem_feedback(&captures);
    let SettlementLifecycleEventV1::AutoRedeemApplied {
        resolution_event_id: actual_resolution_event_id,
        resolution_source_event_at_utc_ms,
        redeemed_outcome_balances,
        redeemed_pusd,
        account_delta,
        ..
    } = &auto.event
    else {
        unreachable!();
    };
    assert_eq!(actual_resolution_event_id, &resolution_event_id);
    assert_eq!(*resolution_source_event_at_utc_ms, resolution_at);
    assert_redeemed_balance(redeemed_outcome_balances, "1", Decimal::ZERO);
    assert_redeemed_balance(redeemed_outcome_balances, "2", Decimal::ZERO);
    assert_eq!(*redeemed_pusd, Decimal::ZERO);
    assert!(account_delta.is_none());

    let cancel_responses = captures
        .cancel_orders
        .lock()
        .expect("任务 3.5 同刻撤单响应捕获锁");
    assert_eq!(cancel_responses.len(), 1);
    assert!(matches!(
        cancel_responses[0].target.outcome,
        crate::execution::CancelTargetOutcomeV1::NotCanceledNoEffect(
            CancelNotCanceledReasonV1::NoCancellableRemainder
        )
    ));
    drop(cancel_responses);

    let query_at = resolution_at + 50_000;
    let query = session
        .order(OrderRequestV1 {
            context: context_for_arrival(
                &session,
                &profile,
                "task351-terminal-order-query",
                3,
                query_at,
            ),
            platform_order_id: platform_order_id.clone(),
        })
        .expect("任务 3.5 判定终态订单查询登记");
    assert_eq!(query.receipt.transit.platform_arrived_at_utc_ms, query_at);
    assert_eq!(
        apply_empty_at(&mut session, query_at),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(
        session.drive_next_execution_action_through(query_at),
        Ok(SubmissionDriveDispositionV1::ItemProcessed {
            request_completed: true,
        })
    );
    drain_accepting(&mut session, query_at + 40_000);
    let query_responses = captures
        .order_queries
        .lock()
        .expect("任务 3.5 订单查询捕获锁");
    let OrderQueryResultV1::Found(order) = &query_responses[0].result else {
        panic!("任务 3.5 判定终态订单必须可查");
    };
    assert_eq!(
        order.status.known,
        Some(SimulatedOrderKnownStatusV1::CanceledMarketResolved)
    );
    assert_eq!(
        order.market_resolved_canceled_base_quantity_shares,
        decimal("5")
    );
    assert_eq!(
        order.market_resolved_released_quote_amount_pusd,
        decimal("0.00005")
    );
    assert_eq!(
        order.pending_maker_depletion_base_quantity_shares,
        Decimal::ZERO
    );
    assert_eq!(order.open_base_quantity_shares, Decimal::ZERO);
    assert_eq!(order.open_encumbered_quote_amount_pusd, Decimal::ZERO);
}

async fn assert_resolution_waits_for_terminal_fill(session_id: &str, failed: bool) {
    let market = market_between(MARKET_START, MARKET_END);
    let captures = Task35Captures::default();
    let request_id = format!("{session_id}-fill");
    let policy = if failed {
        failure_policy(session_id, &request_id, FINALITY_AT - FILL_AT)
    } else {
        success_policy(
            &format!("{session_id}-success-policy"),
            FINALITY_AT - FILL_AT,
        )
    };
    let (mut session, profile) = new_session(
        session_id,
        SimCallerRoleV1::ModelTrain,
        decimal("20"),
        64,
        policy,
        &market,
        &captures,
    )
    .await;
    load_initial_market(&mut session, &market, "100");
    let request = fak_request(
        &session,
        &profile,
        &market,
        &request_id,
        1,
        FILL_AT,
        "1",
        "3",
    );
    drive_post_order(&mut session, request, FILL_AT);
    drain_accepting(&mut session, FILL_AT + 50_000);
    let (platform_order_id, trade_id) = accepted_order_and_trade(&captures, &request_id);
    let before_resolution = session.account_snapshot_for_tests();
    let winning_balance = outcome_balance(&before_resolution, "1");
    assert!(winning_balance > Decimal::ZERO);

    let resolution = resolution_event(&market, EARLY_RESOLUTION_AT, EARLY_RESOLUTION_AT + 100, 3);
    let resolution_event_id = resolution.event_id.0.clone();
    let position = role_position(&session);
    let resolution_bucket = bucket(
        SimCallerRoleV1::ModelTrain,
        EARLY_RESOLUTION_AT,
        position,
        position + 1,
        std::slice::from_ref(&resolution),
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(resolution_bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(resolution_bucket),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(
        session.account_snapshot_for_tests(),
        before_resolution,
        "判定不能越过合法已排定最终性而提前兑付"
    );
    assert_eq!(
        session
            .platform_market_state(&market.market().condition_id.0)
            .expect("任务 3.5 屏障期市场")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::PostEndPendingResolution
    );
    assert_eq!(
        session
            .simulated_fill(&platform_order_id)
            .map(|fill| fill.trade_id.as_str()),
        Some(trade_id.as_str())
    );
    drain_accepting(&mut session, FINALITY_AT - 1);
    assert!(
        !captures
            .unique_settlement_feedbacks()
            .iter()
            .any(|feedback| matches!(
                feedback.event,
                SettlementLifecycleEventV1::AutoRedeemApplied { .. }
            ))
    );

    let position = role_position(&session);
    let terminal_bucket = bucket(
        SimCallerRoleV1::ModelTrain,
        FINALITY_AT,
        position,
        position,
        &[],
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(terminal_bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let terminal_snapshot = session.account_snapshot_for_tests();
    assert_eq!(outcome_balance(&terminal_snapshot, "1"), Decimal::ZERO);
    assert_eq!(
        session.apply_closed_market_event_bucket(terminal_bucket),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(session.account_snapshot_for_tests(), terminal_snapshot);
    assert_eq!(
        session
            .platform_market_state(&market.market().condition_id.0)
            .expect("任务 3.5 赎回后市场")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::ResolutionTerminal
    );
    assert_eq!(
        session
            .drive_private_delivery_after_platform_through_for_tests(FINALITY_AT)
            .expect("任务 3.5 最终性时点反馈不可提前可见"),
        SubmissionDeliveryDispositionV1::NoVisibleResponse
    );
    drain_accepting(&mut session, FINALITY_AT + 50_000);

    let terminal_status = if failed {
        SimulatedTradeKnownStatusV1::Failed
    } else {
        SimulatedTradeKnownStatusV1::Confirmed
    };
    let terminal_trade = captures
        .unique_trade_feedbacks()
        .into_iter()
        .find(|feedback| {
            feedback.simulated_trade_id == trade_id
                && feedback.status.known == Some(terminal_status)
        })
        .expect("任务 3.5 成交最终性反馈");
    assert_eq!(
        terminal_trade.metadata.platform_event_at_utc_ms,
        FINALITY_AT
    );
    let auto = auto_redeem_feedback(&captures);
    assert_eq!(auto.metadata.platform_event_at_utc_ms, FINALITY_AT);
    assert!(
        terminal_trade.metadata.private_emission_sequence < auto.metadata.private_emission_sequence,
        "同刻必须先形成最终性及适用补偿，再形成赎回"
    );
    let SettlementLifecycleEventV1::AutoRedeemApplied {
        resolution_event_id: actual_resolution_event_id,
        resolution_source_event_at_utc_ms,
        redeemed_outcome_balances,
        redeemed_pusd,
        account_delta,
        ..
    } = &auto.event
    else {
        unreachable!();
    };
    assert_eq!(actual_resolution_event_id, &resolution_event_id);
    assert_eq!(*resolution_source_event_at_utc_ms, EARLY_RESOLUTION_AT);
    if failed {
        assert_eq!(
            terminal_trade.economic_disposition,
            TradeEconomicDispositionV1::FinalFailureCompensated
        );
        assert_eq!(
            terminal_trade
                .account_delta
                .as_ref()
                .expect("任务 3.5 已应用成交失败必须原子补偿")
                .reason,
            PlatformAccountDeltaReasonV1::FillFinalityCompensationApplied
        );
        assert_redeemed_balance(redeemed_outcome_balances, "1", Decimal::ZERO);
        assert_redeemed_balance(redeemed_outcome_balances, "2", Decimal::ZERO);
        assert_eq!(*redeemed_pusd, Decimal::ZERO);
        assert!(account_delta.is_none());
    } else {
        assert_eq!(
            terminal_trade.economic_disposition,
            TradeEconomicDispositionV1::FinalSuccess
        );
        assert_redeemed_balance(redeemed_outcome_balances, "1", winning_balance);
        assert_redeemed_balance(redeemed_outcome_balances, "2", Decimal::ZERO);
        assert_eq!(*redeemed_pusd, winning_balance);
        assert_eq!(
            account_delta
                .as_ref()
                .expect("任务 3.5 获胜余额赎回账户变化")
                .reason,
            PlatformAccountDeltaReasonV1::AutoRedeemApplied
        );
    }
}

async fn assert_real_resolution_conflict_blocks_redeem() {
    let session_id = "task352-resolution-conflict";
    let request_id = "task352-resolution-conflict-fill";
    let market = market_between(MARKET_START, MARKET_END);
    let captures = Task35Captures::default();
    let (mut session, profile) = new_session(
        session_id,
        SimCallerRoleV1::ModelTrain,
        decimal("20"),
        64,
        success_policy("task352-resolution-conflict-policy", FINALITY_AT - FILL_AT),
        &market,
        &captures,
    )
    .await;
    load_initial_market(&mut session, &market, "100");
    let request = fak_request(
        &session, &profile, &market, request_id, 1, FILL_AT, "1", "3",
    );
    drive_post_order(&mut session, request, FILL_AT);
    drain_accepting(&mut session, FILL_AT + 50_000);
    captures.clear_lifecycle_attempts();

    let resolution = resolution_event(&market, EARLY_RESOLUTION_AT, EARLY_RESOLUTION_AT + 100, 3);
    assert_eq!(
        apply_events(
            &mut session,
            EARLY_RESOLUTION_AT,
            std::slice::from_ref(&resolution),
        ),
        MarketBucketApplyDispositionV1::Applied
    );
    let conflict_at = EARLY_RESOLUTION_AT + 10_000;
    let conflict = resolution_conflict_event(&market, conflict_at, conflict_at + 100, 4);
    assert_eq!(
        apply_events(&mut session, conflict_at, std::slice::from_ref(&conflict),),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(
        session
            .platform_market_state(&market.market().condition_id.0)
            .expect("任务 3.5 冲突判定市场")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::PostEndPendingResolution
    );
    let before_terminal = session.account_snapshot_for_tests();
    let seq_before_terminal = session.next_account_transition_seq();
    let position = role_position(&session);
    let terminal_bucket = bucket(
        SimCallerRoleV1::ModelTrain,
        FINALITY_AT,
        position,
        position,
        &[],
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(terminal_bucket),
        Err(MarketEventErrorV1::ResolutionConflict(
            market.market().condition_id.0.clone()
        ))
    );
    assert_eq!(session.account_snapshot_for_tests(), before_terminal);
    assert_eq!(
        session.next_account_transition_seq(),
        seq_before_terminal,
        "冲突失败必须回滚同刻确认、返佣和赎回"
    );
    assert_eq!(
        session
            .simulated_fill(&accepted_order_and_trade(&captures, request_id).0,)
            .expect("任务 3.5 冲突前待处理成交")
            .finality_state(),
        Some(TradeFinalityStateV1::Mined)
    );
    assert!(
        !captures
            .unique_settlement_feedbacks()
            .iter()
            .any(|feedback| matches!(
                feedback.event,
                SettlementLifecycleEventV1::AutoRedeemApplied { .. }
            ))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task352_resolution_barrier_waits_for_confirmed_or_failed_then_redeems_once() {
    assert_resolution_waits_for_terminal_fill("task352-confirmed", false).await;
    assert_resolution_waits_for_terminal_fill("task352-failed", true).await;
    assert_real_resolution_conflict_blocks_redeem().await;
}

async fn assert_paper_negative_clock_skew_waits_for_market_end() {
    let market = market_between(MARKET_START, MARKET_END);
    let captures = Task35Captures::default();
    let (mut paper, profile) = new_session(
        "task352-paper-negative-clock-skew",
        SimCallerRoleV1::Paper,
        decimal("20"),
        64,
        success_policy("task352-paper-negative-clock-skew-policy", 2),
        &market,
        &captures,
    )
    .await;
    load_initial_market(&mut paper, &market, "100");
    let open_request_id = "task352-paper-negative-clock-skew-open";
    let open_at = MARKET_START + 100_000;
    let open_request = gtc_request(
        &paper,
        &profile,
        &market,
        open_request_id,
        1,
        open_at,
        "1",
        "0.3",
        "5",
    );
    drive_post_order(&mut paper, open_request, open_at);
    drain_accepting(&mut paper, open_at + 50_000);
    let platform_order_id = {
        let post_orders = captures
            .post_orders
            .lock()
            .expect("任务 3.5 模拟盘负时钟差开放订单响应锁");
        let response = post_orders
            .iter()
            .find(|response| response.metadata.global_request_id == open_request_id)
            .expect("任务 3.5 判定前开放订单响应");
        let SubmissionItemOutcomeV1::Accepted(accepted) = &response.item.outcome else {
            panic!("任务 3.5 判定前 GTC 必须接受");
        };
        assert!(accepted.fills.is_empty());
        accepted.simulated_platform_order_id.clone()
    };
    let open_order = paper
        .simulated_order(&platform_order_id)
        .expect("任务 3.5 判定前开放订单")
        .clone();
    assert_eq!(open_order.open_base_units, 5_000_000);
    assert_eq!(open_order.open_quote_units, 1_500_000);
    assert_eq!(paper.simulated_fill_count(&platform_order_id), 0);
    let account_before_receive = paper.account_snapshot_for_tests();
    let seq_before_receive = paper.next_account_transition_seq();
    captures.clear_lifecycle_attempts();

    let received_at = MARKET_END - 200_000;
    let source_at = MARKET_END + 10_000;
    assert!(received_at < MARKET_END && MARKET_END <= source_at);
    let resolution = resolution_event(&market, source_at, received_at, 3);
    let resolution_event_id = resolution.event_id.0.clone();
    assert_eq!(
        apply_events(&mut paper, received_at, std::slice::from_ref(&resolution),),
        MarketBucketApplyDispositionV1::Applied
    );
    assert!(paper.cloned_execution_state_shares_market_resolution_storage());
    assert_eq!(
        paper
            .platform_market_state(&market.market().condition_id.0)
            .expect("任务 3.5 模拟盘负时钟差判定状态")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::PostEndPendingResolution
    );
    let account_after_receive = paper.account_snapshot_for_tests();
    let seq_after_receive = paper.next_account_transition_seq();
    assert_ne!(account_after_receive, account_before_receive);
    assert_eq!(seq_after_receive, seq_before_receive + 1);
    assert_eq!(
        exact_decimal(&account_after_receive.available_pusd),
        decimal("20")
    );
    assert_eq!(
        exact_decimal(&account_after_receive.principal_encumbrance_pusd),
        Decimal::ZERO
    );
    assert_eq!(outcome_balance(&account_after_receive, "1"), Decimal::ZERO);
    assert_eq!(outcome_balance(&account_after_receive, "2"), Decimal::ZERO);
    let resolved_order = paper
        .simulated_order(&platform_order_id)
        .expect("任务 3.5 负时钟差判定关闭订单")
        .clone();
    assert_eq!(resolved_order.matched_base_units, 0);
    assert_eq!(resolved_order.open_base_units, 0);
    assert_eq!(resolved_order.open_quote_units, 0);
    assert_eq!(
        resolved_order.market_resolved_canceled_base_units,
        5_000_000
    );
    assert_eq!(
        resolved_order.market_resolved_released_quote_units,
        1_500_000
    );
    assert!(
        !captures
            .unique_settlement_feedbacks()
            .iter()
            .any(|feedback| matches!(
                feedback.event,
                SettlementLifecycleEventV1::AutoRedeemApplied { .. }
            ))
    );

    let addition = rebind_event(
        bid_transition(received_at + 1, received_at + 101, 4, "0.3", "0", "5"),
        &market,
    );
    assert_eq!(
        apply_events(&mut paper, received_at + 101, &[addition]),
        MarketBucketApplyDispositionV1::Applied
    );
    let depletion = rebind_event(
        bid_transition(received_at + 2, received_at + 102, 5, "0.3", "5", "0"),
        &market,
    );
    let position = role_position(&paper);
    let depletion_bucket = bucket(
        SimCallerRoleV1::Paper,
        received_at + 102,
        position,
        position + 1,
        std::slice::from_ref(&depletion),
    );
    assert_eq!(
        paper.apply_closed_market_event_bucket(depletion_bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(
        paper.apply_closed_market_event_bucket(depletion_bucket),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(
        paper.simulated_order(&platform_order_id),
        Some(&resolved_order)
    );
    assert_eq!(paper.simulated_fill_count(&platform_order_id), 0);
    assert_eq!(paper.account_snapshot_for_tests(), account_after_receive);
    assert_eq!(paper.next_account_transition_seq(), seq_after_receive);

    let rejected_request_id = "task352-paper-negative-clock-skew-buy";
    let rejected_at = received_at + 10_000;
    let rejected_request = gtc_request(
        &paper,
        &profile,
        &market,
        rejected_request_id,
        2,
        rejected_at,
        "1",
        "0.3",
        "5",
    );
    drive_post_order(&mut paper, rejected_request, rejected_at);
    drain_accepting(&mut paper, rejected_at + 90_000);
    let post_orders = captures
        .post_orders
        .lock()
        .expect("任务 3.5 模拟盘负时钟差提交响应锁");
    let rejected = post_orders
        .iter()
        .find(|response| response.metadata.global_request_id == rejected_request_id)
        .expect("任务 3.5 判定已接纳后新 BUY 响应");
    assert_eq!(
        rejected.item.outcome,
        SubmissionItemOutcomeV1::RejectedNoEffect(SubmissionRejectedReasonV1::EndAtReached)
    );
    drop(post_orders);
    assert_eq!(paper.simulated_order_count(), 1);
    let resolution_feedback = market_resolution_order_feedback(&captures);
    assert_eq!(
        resolution_feedback.order_status.known,
        Some(SimulatedOrderKnownStatusV1::CanceledMarketResolved)
    );
    assert_eq!(
        resolution_feedback.market_resolved_canceled_base_quantity_shares,
        decimal("5")
    );
    assert_eq!(
        resolution_feedback.market_resolved_released_quote_amount_pusd,
        decimal("1.5")
    );
    assert_eq!(
        resolution_feedback.account_delta.reason,
        PlatformAccountDeltaReasonV1::MarketResolutionEncumbranceReleased
    );
    assert_eq!(paper.account_snapshot_for_tests(), account_after_receive);
    assert_eq!(paper.next_account_transition_seq(), seq_after_receive);

    captures.clear_lifecycle_attempts();
    let position = role_position(&paper);
    let end_bucket = bucket(SimCallerRoleV1::Paper, MARKET_END, position, position, &[]);
    assert_eq!(
        paper.apply_closed_market_event_bucket(end_bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(paper.account_snapshot_for_tests(), account_after_receive);
    assert_eq!(paper.next_account_transition_seq(), seq_after_receive);
    assert_eq!(
        paper.apply_closed_market_event_bucket(end_bucket),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(
        paper
            .platform_market_state(&market.market().condition_id.0)
            .expect("任务 3.5 模拟盘负时钟差结算终态")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::ResolutionTerminal
    );
    drain_accepting(&mut paper, MARKET_END + 50_000);
    let autos = captures
        .unique_settlement_feedbacks()
        .into_iter()
        .filter(|feedback| {
            matches!(
                feedback.event,
                SettlementLifecycleEventV1::AutoRedeemApplied { .. }
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(autos.len(), 1);
    assert_eq!(autos[0].metadata.platform_event_at_utc_ms, MARKET_END);
    let SettlementLifecycleEventV1::AutoRedeemApplied {
        resolution_event_id: actual_resolution_event_id,
        resolution_source_event_at_utc_ms,
        redeemed_outcome_balances,
        redeemed_pusd,
        account_delta,
        ..
    } = &autos[0].event
    else {
        unreachable!();
    };
    assert_eq!(actual_resolution_event_id, &resolution_event_id);
    assert_eq!(*resolution_source_event_at_utc_ms, source_at);
    assert_redeemed_balance(redeemed_outcome_balances, "1", Decimal::ZERO);
    assert_redeemed_balance(redeemed_outcome_balances, "2", Decimal::ZERO);
    assert_eq!(*redeemed_pusd, Decimal::ZERO);
    assert!(account_delta.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn task352_train_and_paper_use_source_and_receive_time_without_receive_only_redeem() {
    let market = market_between(MARKET_START, MARKET_END);
    let train_captures = Task35Captures::default();
    let paper_captures = Task35Captures::default();
    let (mut train, _) = new_session(
        "task352-dual-time-train",
        SimCallerRoleV1::ModelTrain,
        decimal("20"),
        64,
        success_policy("task352-dual-time-train-policy", 2),
        &market,
        &train_captures,
    )
    .await;
    let (mut paper, _) = new_session(
        "task352-dual-time-paper",
        SimCallerRoleV1::Paper,
        decimal("20"),
        64,
        success_policy("task352-dual-time-paper-policy", 2),
        &market,
        &paper_captures,
    )
    .await;
    load_initial_market(&mut train, &market, "100");
    load_initial_market(&mut paper, &market, "100");
    assert_eq!(
        apply_empty_at(&mut train, MARKET_END),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(
        apply_empty_at(&mut paper, MARKET_END),
        MarketBucketApplyDispositionV1::Applied
    );
    train_captures.clear_lifecycle_attempts();
    paper_captures.clear_lifecycle_attempts();
    let train_seq = train.next_account_transition_seq();
    let paper_seq = paper.next_account_transition_seq();

    let receive_only_at = MARKET_END + 50;
    let receive_only = receive_only_resolution_event(&market, receive_only_at);
    assert_eq!(
        apply_events(
            &mut paper,
            receive_only_at,
            std::slice::from_ref(&receive_only)
        ),
        MarketBucketApplyDispositionV1::Applied
    );
    let paper_state = paper
        .platform_market_state(&market.market().condition_id.0)
        .expect("任务 3.5 模拟盘仅接收时点判定状态");
    assert_eq!(
        paper_state.receive_only_resolution_event_id(),
        Some(&receive_only.event_id)
    );
    assert_eq!(
        paper_state.lifecycle_phase(),
        MarketLifecyclePhaseV1::PostEndPendingResolution
    );
    assert_eq!(paper.next_account_transition_seq(), paper_seq);

    let source_at = MARKET_END + 1;
    let received_at = MARKET_END + 101;
    let resolution = resolution_event(&market, source_at, received_at, 3);
    let resolution_event_id = resolution.event_id.0.clone();
    let expected_resolution = match &resolution.payload {
        ExecutionMarketEventPayloadV3::MarketResolutionTerminal(value) => value.clone(),
        _ => unreachable!(),
    };
    assert_eq!(
        apply_events(&mut train, source_at, std::slice::from_ref(&resolution)),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(train.next_account_transition_seq(), train_seq);
    assert_eq!(
        train
            .platform_market_state(&market.market().condition_id.0)
            .expect("任务 3.5 训练判定状态")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::ResolutionTerminal
    );

    assert_eq!(
        apply_empty_at(&mut paper, received_at - 1),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(
        paper
            .platform_market_state(&market.market().condition_id.0)
            .expect("任务 3.5 模拟盘判定可见前状态")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::PostEndPendingResolution
    );
    assert_eq!(paper.next_account_transition_seq(), paper_seq);
    assert!(
        !paper_captures
            .unique_settlement_feedbacks()
            .iter()
            .any(|feedback| matches!(
                feedback.event,
                SettlementLifecycleEventV1::AutoRedeemApplied { .. }
            ))
    );
    assert_eq!(
        apply_events(&mut paper, received_at, std::slice::from_ref(&resolution)),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(paper.next_account_transition_seq(), paper_seq);
    assert_eq!(
        paper
            .platform_market_state(&market.market().condition_id.0)
            .expect("任务 3.5 模拟盘判定可见后状态")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::ResolutionTerminal
    );

    drain_accepting(&mut train, source_at + 50_000);
    drain_accepting(&mut paper, received_at + 50_000);
    for (auto, platform_at) in [
        (auto_redeem_feedback(&train_captures), source_at),
        (auto_redeem_feedback(&paper_captures), received_at),
    ] {
        assert_eq!(auto.metadata.platform_event_at_utc_ms, platform_at);
        let SettlementLifecycleEventV1::AutoRedeemApplied {
            resolution_event_id: actual_resolution_event_id,
            resolution_source_event_at_utc_ms,
            resolution: actual_resolution,
            redeemed_outcome_balances,
            redeemed_pusd,
            account_delta,
            ..
        } = &auto.event
        else {
            unreachable!();
        };
        assert_eq!(actual_resolution_event_id, &resolution_event_id);
        assert_eq!(*resolution_source_event_at_utc_ms, source_at);
        assert_eq!(actual_resolution, &expected_resolution);
        assert_redeemed_balance(redeemed_outcome_balances, "1", Decimal::ZERO);
        assert_redeemed_balance(redeemed_outcome_balances, "2", Decimal::ZERO);
        assert_eq!(*redeemed_pusd, Decimal::ZERO);
        assert!(account_delta.is_none());
    }

    assert_paper_negative_clock_skew_waits_for_market_end().await;
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TimedResolutionEconomicOutcomeV1 {
    before_resolution: PlatformAccountSnapshotV1,
    after_resolution: PlatformAccountSnapshotV1,
    next_account_transition_seq: u64,
    redeemed_outcome_balances: Vec<PlatformOutcomeBalanceV1>,
    redeemed_pusd: Decimal,
}

async fn run_timed_resolution_case(
    session_id: &str,
    token_id: &str,
    timer_probe_at_utc_ms: Option<i64>,
    resolution_at_utc_ms: i64,
) -> TimedResolutionEconomicOutcomeV1 {
    let market = market_between(MARKET_START, MARKET_END);
    let captures = Task35Captures::default();
    let (mut session, profile) = new_session(
        session_id,
        SimCallerRoleV1::ModelTrain,
        decimal("3.126"),
        64,
        success_policy(&format!("{session_id}-policy"), 2),
        &market,
        &captures,
    )
    .await;
    load_initial_market(&mut session, &market, "1000");
    let request = fak_request(
        &session,
        &profile,
        &market,
        &format!("{session_id}-fill"),
        1,
        FILL_AT,
        token_id,
        "3",
    );
    drive_post_order(&mut session, request, FILL_AT);
    assert_eq!(
        apply_empty_at(&mut session, FILL_AT + 2),
        MarketBucketApplyDispositionV1::Applied
    );
    assert_eq!(session.available_pusd(), Decimal::ZERO);
    assert_eq!(
        apply_empty_at(&mut session, MARKET_END),
        MarketBucketApplyDispositionV1::Applied
    );
    drain_accepting(&mut session, MARKET_END + 40_000);
    captures.clear_lifecycle_attempts();
    let before_resolution = session.account_snapshot_for_tests();
    let seq_before_resolution = session.next_account_transition_seq();
    assert_eq!(
        exact_decimal(&before_resolution.available_pusd),
        Decimal::ZERO
    );
    assert!(outcome_balance(&before_resolution, token_id) > Decimal::ZERO);

    if let Some(timer_probe_at_utc_ms) = timer_probe_at_utc_ms {
        assert!(timer_probe_at_utc_ms < resolution_at_utc_ms);
        assert_eq!(
            apply_empty_at(&mut session, timer_probe_at_utc_ms),
            MarketBucketApplyDispositionV1::Applied
        );
        assert_eq!(session.account_snapshot_for_tests(), before_resolution);
        assert_eq!(session.next_account_transition_seq(), seq_before_resolution);
        assert_eq!(
            session
                .platform_market_state(&market.market().condition_id.0)
                .expect("任务 3.5 固定时长探针市场")
                .lifecycle_phase(),
            MarketLifecyclePhaseV1::PostEndPendingResolution
        );
        assert!(
            !captures
                .unique_settlement_feedbacks()
                .iter()
                .any(|feedback| matches!(
                    feedback.event,
                    SettlementLifecycleEventV1::AutoRedeemApplied { .. }
                ))
        );
    }

    let resolution = resolution_event(&market, resolution_at_utc_ms, resolution_at_utc_ms + 100, 3);
    let position = role_position(&session);
    let resolution_bucket = bucket(
        SimCallerRoleV1::ModelTrain,
        resolution_at_utc_ms,
        position,
        position + 1,
        std::slice::from_ref(&resolution),
    );
    assert_eq!(
        session.apply_closed_market_event_bucket(resolution_bucket.clone()),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    let after_resolution = session.account_snapshot_for_tests();
    let next_account_transition_seq = session.next_account_transition_seq();
    assert_eq!(
        session.apply_closed_market_event_bucket(resolution_bucket),
        Ok(MarketBucketApplyDispositionV1::PhysicalRetry)
    );
    assert_eq!(session.account_snapshot_for_tests(), after_resolution);
    assert_eq!(
        session.next_account_transition_seq(),
        next_account_transition_seq
    );
    assert_eq!(outcome_balance(&after_resolution, token_id), Decimal::ZERO);
    assert_eq!(
        session
            .platform_market_state(&market.market().condition_id.0)
            .expect("任务 3.5 真实判定后市场")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::ResolutionTerminal
    );
    assert_eq!(
        session
            .drive_private_delivery_after_platform_through_for_tests(resolution_at_utc_ms)
            .expect("任务 3.5 真实判定反馈延迟"),
        SubmissionDeliveryDispositionV1::NoVisibleResponse
    );
    drain_accepting(&mut session, resolution_at_utc_ms + 50_000);
    let auto = auto_redeem_feedback(&captures);
    assert_eq!(auto.metadata.platform_event_at_utc_ms, resolution_at_utc_ms);
    let SettlementLifecycleEventV1::AutoRedeemApplied {
        redeemed_outcome_balances,
        redeemed_pusd,
        account_delta,
        ..
    } = &auto.event
    else {
        unreachable!();
    };
    assert_eq!(
        account_delta
            .as_ref()
            .expect("任务 3.5 正结果代币清零必须形成账户变化")
            .reason,
        PlatformAccountDeltaReasonV1::AutoRedeemApplied
    );
    if token_id == "1" {
        assert_eq!(
            *redeemed_pusd,
            outcome_balance(&before_resolution, token_id)
        );
    } else {
        assert_eq!(*redeemed_pusd, Decimal::ZERO);
    }

    TimedResolutionEconomicOutcomeV1 {
        before_resolution,
        after_resolution,
        next_account_transition_seq,
        redeemed_outcome_balances: redeemed_outcome_balances.clone(),
        redeemed_pusd: *redeemed_pusd,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task352_only_real_resolution_redeems_at_normal_or_later_time_with_zero_available() {
    let normal_at = MARKET_END + 180_000;
    let later_at = MARKET_END + 240_000;
    let normal_winner =
        run_timed_resolution_case("task352-timed-winner", "1", None, normal_at).await;
    let late_winner =
        run_timed_resolution_case("task352-late-winner", "1", Some(normal_at), later_at).await;
    assert_eq!(
        normal_winner.before_resolution,
        late_winner.before_resolution
    );
    assert_eq!(normal_winner.after_resolution, late_winner.after_resolution);
    assert_eq!(
        normal_winner.next_account_transition_seq,
        late_winner.next_account_transition_seq
    );
    assert_eq!(
        normal_winner.redeemed_outcome_balances,
        late_winner.redeemed_outcome_balances
    );
    assert_eq!(normal_winner.redeemed_pusd, late_winner.redeemed_pusd);

    let replay =
        run_timed_resolution_case("task352-late-winner", "1", Some(normal_at), later_at).await;
    assert_eq!(late_winner, replay);

    let loser =
        run_timed_resolution_case("task352-late-loser", "2", Some(normal_at), later_at).await;
    assert_redeemed_balance(
        &loser.redeemed_outcome_balances,
        "2",
        outcome_balance(&loser.before_resolution, "2"),
    );
    assert_eq!(loser.redeemed_pusd, Decimal::ZERO);
    assert_eq!(outcome_balance(&loser.after_resolution, "2"), Decimal::ZERO);
}

#[tokio::test(flavor = "current_thread")]
async fn task353_three_lifecycle_types_share_capacity_and_retry_the_same_arc() {
    let market = market_between(MARKET_START, MARKET_END);
    let captures = Task35Captures::default();
    let policy = policy_for_tests(
        "task353-shared-lifecycle-policy",
        &[
            (TradeFinalityStateV1::Matched, 0),
            (TradeFinalityStateV1::Mined, 1),
            (TradeFinalityStateV1::Confirmed, FINALITY_AT - FILL_AT),
        ],
        2,
        Vec::new(),
    );
    let (mut session, profile) = new_session(
        "task353-shared-lifecycle",
        SimCallerRoleV1::ModelTrain,
        decimal("20"),
        1,
        policy,
        &market,
        &captures,
    )
    .await;
    load_initial_market(&mut session, &market, "5");
    let request_id = "task353-partial-gtc";
    let request = gtc_request(
        &session, &profile, &market, request_id, 1, FILL_AT, "1", "0.4", "10",
    );
    drive_post_order(&mut session, request, FILL_AT);
    drain_accepting(&mut session, FILL_AT + 50_000);
    let (platform_order_id, trade_id) = accepted_order_and_trade(&captures, request_id);
    let order = session
        .simulated_order(&platform_order_id)
        .expect("任务 3.5 部分成交 GTC");
    assert_eq!(order.matched_base_units, 5_000_000);
    assert_eq!(order.open_base_units, 5_000_000);
    assert_eq!(
        apply_empty_at(&mut session, MARKET_END),
        MarketBucketApplyDispositionV1::Applied
    );
    drain_accepting(&mut session, MARKET_END + 40_000);
    captures.clear_lifecycle_attempts();
    assert!(captures.lifecycle_attempts().is_empty());
    captures.arm_full_once();

    let resolution = resolution_event(&market, FINALITY_AT, FINALITY_AT + 100, 3);
    let position = role_position(&session);
    assert_eq!(
        session.apply_closed_market_event_bucket(bucket(
            SimCallerRoleV1::ModelTrain,
            FINALITY_AT,
            position,
            position + 1,
            std::slice::from_ref(&resolution),
        )),
        Ok(MarketBucketApplyDispositionV1::Applied)
    );
    assert_eq!(
        session
            .platform_market_state(&market.market().condition_id.0)
            .expect("任务 3.5 共享队列判定市场")
            .lifecycle_phase(),
        MarketLifecyclePhaseV1::ResolutionTerminal
    );
    assert_eq!(
        session
            .simulated_fill(&platform_order_id)
            .expect("任务 3.5 同刻确认成交")
            .finality_state(),
        Some(TradeFinalityStateV1::Confirmed)
    );
    assert_eq!(
        session
            .simulated_order(&platform_order_id)
            .expect("任务 3.5 同刻判定关闭订单")
            .market_resolved_canceled_base_units,
        5_000_000
    );

    assert_eq!(
        session
            .drive_private_delivery_after_platform_through_for_tests(FINALITY_AT + 50_000)
            .expect("任务 3.5 首次统一生命周期交付"),
        SubmissionDeliveryDispositionV1::Full
    );
    let first_attempt = captures
        .lifecycle_attempts()
        .into_iter()
        .next()
        .expect("任务 3.5 首次 Full 必须捕获对象");
    assert_eq!(
        session
            .drive_private_delivery_after_platform_through_for_tests(FINALITY_AT + 50_000)
            .expect("任务 3.5 统一生命周期重投与后续交付"),
        SubmissionDeliveryDispositionV1::Delivered
    );
    let attempts = captures.lifecycle_attempts();
    assert!(attempts.len() >= 2);
    assert!(first_attempt.same_arc(&attempts[1]));
    assert_eq!(first_attempt.delivery_id(), attempts[1].delivery_id());

    let at_finality = captures
        .unique_lifecycle_attempts()
        .into_iter()
        .filter(|attempt| attempt.platform_event_at_utc_ms() == FINALITY_AT)
        .collect::<Vec<_>>();
    let delivery_keys = at_finality
        .iter()
        .map(|attempt| {
            (
                attempt.received_at_utc_ms(),
                attempt.private_emission_sequence(),
            )
        })
        .collect::<Vec<_>>();
    let mut sorted_keys = delivery_keys.clone();
    sorted_keys.sort();
    assert_eq!(delivery_keys, sorted_keys);
    assert!(
        at_finality
            .windows(2)
            .all(|pair| pair[1].private_feedback_sequence()
                == pair[0].private_feedback_sequence() + 1)
    );

    let confirmed = at_finality
        .iter()
        .filter_map(|attempt| match attempt {
            LifecycleAttemptV1::Trade(feedback)
                if feedback.simulated_trade_id == trade_id
                    && feedback.status.known == Some(SimulatedTradeKnownStatusV1::Confirmed) =>
            {
                Some(feedback)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(confirmed.len(), 1);
    assert_eq!(
        confirmed[0]
            .account_delta
            .as_ref()
            .expect("任务 3.5 成交经济应用变化")
            .reason,
        PlatformAccountDeltaReasonV1::FillFinalityEconomicApplied
    );

    let resolved_orders = at_finality
        .iter()
        .filter_map(|attempt| match attempt {
            LifecycleAttemptV1::Order(feedback)
                if feedback.platform_order_id == platform_order_id
                    && feedback.event_kind == OrderLifecycleEventKindV1::MarketResolvedCanceled =>
            {
                Some(feedback)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(resolved_orders.len(), 1);
    assert_eq!(
        resolved_orders[0].account_delta.reason,
        PlatformAccountDeltaReasonV1::MarketResolutionEncumbranceReleased
    );
    assert_eq!(
        resolved_orders[0].order_status.raw,
        "ORDER_STATUS_CANCELED_MARKET_RESOLVED"
    );

    let auto_redeems = at_finality
        .iter()
        .filter_map(|attempt| match attempt {
            LifecycleAttemptV1::Settlement(feedback) => match &feedback.event {
                SettlementLifecycleEventV1::AutoRedeemApplied {
                    account_delta: Some(account_delta),
                    ..
                } => Some(account_delta),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(auto_redeems.len(), 1);
    assert_eq!(
        auto_redeems[0].reason,
        PlatformAccountDeltaReasonV1::AutoRedeemApplied
    );

    assert!(at_finality.iter().all(|attempt| match attempt {
        LifecycleAttemptV1::Order(feedback) => !matches!(
            feedback.account_delta.reason,
            PlatformAccountDeltaReasonV1::FillFinalityEconomicApplied
                | PlatformAccountDeltaReasonV1::AutoRedeemApplied
        ),
        LifecycleAttemptV1::Trade(feedback) =>
            feedback.account_delta.as_ref().is_none_or(|delta| {
                delta.reason != PlatformAccountDeltaReasonV1::MarketResolutionEncumbranceReleased
                    && delta.reason != PlatformAccountDeltaReasonV1::AutoRedeemApplied
            }),
        LifecycleAttemptV1::Settlement(_) => true,
    }));
}
