use std::sync::Arc;

use sim_system::execution::{
    AccountOpeningAnchorV1, ActivateSessionRequestV1, CancelAllOrdersRegistrationV1,
    CancelAllOrdersRequestV1, CancelOrderRegistrationV1, CancelOrderRequestV1,
    CancelOrdersRegistrationV1, CancelOrdersRequestV1, FailSessionInitializationRequestV1,
    MakerRebateAccrualStatusV1, MakerRebateFillContributionV1, MarketEnrollmentRequestV1,
    MarketEnrollmentV1, NewPreparedSessionV2, OrderProjectionV1, OrderRegistrationV1,
    OrderRequestV1, OrdersRegistrationV1, OrdersRequestV1, PlatformScenarioV1,
    PostOrderRegistrationV1, PostOrderRequestV1, PostOrdersRegistrationV1, PostOrdersRequestV1,
    SessionCreationControlResultV2, SessionInitializationControlResultV1,
    SessionInitializationSnapshotV1, SessionSpecV2, SettlementLifecycleEventV1,
    SettlementLifecycleFeedbackV1, SimExecutionContractErrorV1, SimExecutionProcessV1,
    SimExecutionSessionV1, SubmissionResponseAcceptanceV1, SubmissionResponseBridgeV1,
    TradeProjectionV1, TradesRegistrationV1, TradesRequestV1,
};
use sim_system::finality::TradeFinalityPolicyV1;
use sim_system::latency_profile::EmpiricalDelayProfile;
use sim_system::platform_rules::SealedProcessRuleSnapshot;

#[allow(dead_code)]
fn assert_process_factory(
    rules: Arc<SealedProcessRuleSnapshot>,
    latency_profile: Arc<EmpiricalDelayProfile>,
    scenario: Arc<PlatformScenarioV1>,
    finality_policy: Arc<TradeFinalityPolicyV1>,
    session_spec: SessionSpecV2,
) -> Result<NewPreparedSessionV2, SimExecutionContractErrorV1> {
    SimExecutionProcessV1::try_new(
        "api-test-run".to_owned(),
        finality_policy,
        rules,
        latency_profile,
        scenario,
    )
    .expect("typed process root")
    .create_session(
        session_spec,
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
            |response| {
                SubmissionResponseAcceptanceV1::Accepted(response.metadata.delivery_confirmation())
            },
            |feedback| {
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
            |feedback| {
                SubmissionResponseAcceptanceV1::Accepted(feedback.metadata.delivery_confirmation())
            },
        ),
    )
}

#[allow(dead_code)]
fn assert_initialization_control_signatures(
    process: &mut SimExecutionProcessV1,
    session_spec: &SessionSpecV2,
    opening_anchor: &AccountOpeningAnchorV1,
    activation: ActivateSessionRequestV1,
    failure: FailSessionInitializationRequestV1,
) {
    let _: Result<SessionCreationControlResultV2, SimExecutionContractErrorV1> =
        process.retry_create_session(session_spec);
    let _: Result<SessionInitializationControlResultV1, SimExecutionContractErrorV1> =
        process.activate_session(activation);
    let _: Result<SessionInitializationControlResultV1, SimExecutionContractErrorV1> =
        process.fail_session_initialization(failure);
    let _: Result<SessionInitializationSnapshotV1, SimExecutionContractErrorV1> =
        process.session_initialization(opening_anchor);
}

#[allow(dead_code)]
fn assert_market_enrollment_control_signature(
    process: &SimExecutionProcessV1,
    session: &mut SimExecutionSessionV1,
    request: MarketEnrollmentRequestV1,
) {
    let _: Result<MarketEnrollmentV1, SimExecutionContractErrorV1> =
        process.enroll_market(session, request);
}

#[allow(clippy::too_many_arguments, dead_code)]
fn assert_exact_eight_method_signatures(
    session: &mut SimExecutionSessionV1,
    post_order: PostOrderRequestV1,
    post_orders: PostOrdersRequestV1,
    cancel_order: CancelOrderRequestV1,
    cancel_orders: CancelOrdersRequestV1,
    cancel_all_orders: CancelAllOrdersRequestV1,
    order: OrderRequestV1,
    orders: OrdersRequestV1,
    trades: TradesRequestV1,
) {
    let _: Result<PostOrderRegistrationV1, SimExecutionContractErrorV1> =
        session.post_order(post_order);
    let _: Result<PostOrdersRegistrationV1, SimExecutionContractErrorV1> =
        session.post_orders(post_orders);
    let _: Result<CancelOrderRegistrationV1, SimExecutionContractErrorV1> =
        session.cancel_order(cancel_order);
    let _: Result<CancelOrdersRegistrationV1, SimExecutionContractErrorV1> =
        session.cancel_orders(cancel_orders);
    let _: Result<CancelAllOrdersRegistrationV1, SimExecutionContractErrorV1> =
        session.cancel_all_orders(cancel_all_orders);
    let _: Result<OrderRegistrationV1, SimExecutionContractErrorV1> = session.order(order);
    let _: Result<OrdersRegistrationV1, SimExecutionContractErrorV1> = session.orders(orders);
    let _: Result<TradesRegistrationV1, SimExecutionContractErrorV1> = session.trades(trades);
}

#[allow(dead_code)]
fn assert_model_visible_query_projection_shapes(
    order: OrderProjectionV1,
    trade: TradeProjectionV1,
) {
    let OrderProjectionV1 {
        platform_order_id: _,
        global_request_id: _,
        item_id: _,
        condition_id: _,
        token_id: _,
        created_at_utc_ms: _,
        last_updated_at_utc_ms: _,
        kind: _,
        expires_at_utc_ms: _,
        source_price_pusd_per_share: _,
        status: _,
        accepted_base_quantity_shares: _,
        matched_base_quantity_shares: _,
        pending_maker_depletion_base_quantity_shares: _,
        open_base_quantity_shares: _,
        venue_dust_closed_base_quantity_shares: _,
        market_resolved_canceled_base_quantity_shares: _,
        canceled_base_quantity_shares: _,
        expired_base_quantity_shares: _,
        accepted_quote_amount_pusd: _,
        matched_quote_amount_pusd: _,
        open_encumbered_quote_amount_pusd: _,
        venue_dust_released_quote_amount_pusd: _,
        market_resolved_released_quote_amount_pusd: _,
        released_price_improvement_quote_amount_pusd: _,
        canceled_released_quote_amount_pusd: _,
        expired_released_quote_amount_pusd: _,
        immediate_taker_fee_encumbered_pusd: _,
    } = order;
    let TradeProjectionV1 {
        simulated_trade_id: _,
        platform_order_id: _,
        condition_id: _,
        token_id: _,
        matched_at_utc_ms: _,
        status: _,
        liquidity_role: _,
        source_price_pusd_per_share: _,
        base_quantity_shares: _,
        quote_amount_pusd: _,
        immediate_taker_fee_pusd: _,
        economic_disposition: _,
    } = trade;
}

#[allow(dead_code)]
fn assert_settlement_lifecycle_feedback_shape(
    feedback: SettlementLifecycleFeedbackV1,
    contribution: MakerRebateFillContributionV1,
    status: MakerRebateAccrualStatusV1,
) {
    let SettlementLifecycleFeedbackV1 {
        metadata: _,
        settlement_identity: _,
        event,
    } = feedback;
    let MakerRebateFillContributionV1 {
        simulated_trade_id: _,
        matched_at_utc_ms: _,
        attribution_day_start_utc_ms: _,
        base_quantity_shares: _,
        price_pusd_per_share: _,
        maker_fee_equivalent_pusd: _,
        rebate_rate: _,
        rebate_contribution_pusd: _,
    } = contribution;
    match status {
        MakerRebateAccrualStatusV1::Calculated
        | MakerRebateAccrualStatusV1::ZeroNoEligibleMakerFill
        | MakerRebateAccrualStatusV1::UnknownIncomplete => {}
    }
    match event {
        SettlementLifecycleEventV1::MakerRebateAccrualFinalized {
            condition_id: _,
            status: _,
            contributions: _,
            accrual_pusd: _,
            account_delta: _,
        }
        | SettlementLifecycleEventV1::TakerRebateAccrualApplied {
            simulated_trade_id: _,
            condition_id: _,
            matched_at_utc_ms: _,
            attribution_day_start_utc_ms: _,
            actual_taker_fee_pusd: _,
            rebate_rate: _,
            accrual_pusd: _,
            account_delta: _,
        }
        | SettlementLifecycleEventV1::MakerRebateDayFinalized {
            attribution_day_start_utc_ms: _,
            accrual_pusd: _,
            payable_pusd: _,
            locked_remainder_pusd: _,
            account_delta: _,
        }
        | SettlementLifecycleEventV1::TakerRebateDayFinalized {
            attribution_day_start_utc_ms: _,
            accrual_pusd: _,
            payable_pusd: _,
            locked_remainder_pusd: _,
            account_delta: _,
        }
        | SettlementLifecycleEventV1::MakerRebatePaid {
            attribution_day_start_utc_ms: _,
            paid_pusd: _,
            account_delta: _,
        }
        | SettlementLifecycleEventV1::TakerRebatePaid {
            attribution_day_start_utc_ms: _,
            paid_pusd: _,
            account_delta: _,
        } => {}
        SettlementLifecycleEventV1::AutoRedeemApplied {
            condition_id: _,
            resolution_event_id: _,
            resolution_source_event_at_utc_ms: _,
            resolution: _,
            redeemed_outcome_balances: _,
            redeemed_pusd: _,
            account_delta: _,
        } => {}
    }
}

#[test]
fn public_surface_type_checks_without_constructing_platform_state() {
    // 本测试的主要证据来自本文件成功编译；运行断言防止零测试误报。
    assert_eq!(8, 8);
}
