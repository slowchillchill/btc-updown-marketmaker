use std::sync::Arc;

use contracts::{ConditionId, ExactDecimal, Sha256Hex, TokenId};

use crate::execution::{
    CancelAllOrdersRequestV1, CancelOrderItemV1, CancelOrderRequestV1, CancelOrdersRequestV1,
    Decimal, InitialAccountStateV1, OrderRequestV1, OrdersFilterV1, OrdersRequestV1,
    PostOrderItemV1, PostOrderRequestV1, PostOrdersRequestV1, RequestRegistrationDispositionV1,
    SimBuyOrderKindV1, SimBuyOrderV1, SimExecutionSessionV1, TradesFilterV1, TradesRequestV1,
    accepting_submission_response_bridge, execution_process_for_tests,
};
use crate::finality::{failure_policy_for_tests, successful_policy_for_tests};
use crate::latency_profile::load_embedded_proxy_latency_profile;
use crate::platform_rules::tests::successful_snapshot_for_sim_tests;
use crate::session::{
    ActivateSessionRequestV1, FailSessionInitializationRequestV1, RequestContextV1,
    SessionCreationDispositionV2, SessionError, SessionInitializationControlDispositionV1,
    SessionInitializationStateV1, SimCallerRoleV1, SimProcessRoot, SimProcessRootErrorV1,
};
use crate::task21_tests::{scenario, session_spec};

fn spec_for_run(
    scenario: &crate::execution::PlatformScenarioV1,
    role: SimCallerRoleV1,
    session_id: &str,
    run_id: &str,
) -> crate::session::SessionSpecV2 {
    let mut spec = session_spec(scenario, role, session_id);
    spec.run_id = run_id.to_owned();
    spec
}

fn activation_request(
    session_id: &str,
    control: &crate::session::SessionCreationControlResultV2,
    activation_id: &str,
) -> ActivateSessionRequestV1 {
    ActivateSessionRequestV1 {
        session_id: session_id.to_owned(),
        account_instance_key: control.account_instance_key.clone(),
        opening_anchor_id: control.opening_anchor.anchor_id.clone(),
        opening_anchor_sha256: control.opening_anchor.anchor_sha256.clone(),
        activation_id: activation_id.to_owned(),
    }
}

fn failure_request(
    session_id: &str,
    control: &crate::session::SessionCreationControlResultV2,
    failure_id: &str,
) -> FailSessionInitializationRequestV1 {
    FailSessionInitializationRequestV1 {
        session_id: session_id.to_owned(),
        account_instance_key: control.account_instance_key.clone(),
        opening_anchor_id: control.opening_anchor.anchor_id.clone(),
        opening_anchor_sha256: control.opening_anchor.anchor_sha256.clone(),
        failure_id: failure_id.to_owned(),
    }
}

fn request_context(
    session: &SimExecutionSessionV1,
    global_request_id: &str,
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
        global_request_id: global_request_id.to_owned(),
        caller_correlation_id: None,
        caller_sequence,
        request_sent_at_utc_ms,
        model_visible_cursor: session.model_visible_cursor(),
    }
}

fn post_order_request(session: &SimExecutionSessionV1, request_id: &str) -> PostOrderRequestV1 {
    PostOrderRequestV1 {
        context: request_context(session, request_id, 1, 1_000),
        idempotency_key: format!("{request_id}-idempotency"),
        item: post_order_item(&format!("{request_id}-item")),
    }
}

fn post_order_item(item_id: &str) -> PostOrderItemV1 {
    PostOrderItemV1 {
        item_id: item_id.to_owned(),
        order: SimBuyOrderV1 {
            condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
            token_id: TokenId("1".to_owned()),
            kind: SimBuyOrderKindV1::GtcLimit {
                limit_price_pusd_per_share: Decimal::new(50, 2),
                size_shares: Decimal::new(1, 0),
                post_only: false,
            },
        },
    }
}

fn order_request(session: &SimExecutionSessionV1, request_id: &str) -> OrderRequestV1 {
    OrderRequestV1 {
        context: request_context(session, request_id, 1, 1_000),
        platform_order_id: "a".repeat(64),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn task30_run_root_rejects_invalid_identity_before_any_session_state() {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("proxy profile");
    let scenario = scenario();

    for invalid_run_id in ["", " run-a", "run-a "] {
        assert!(matches!(
            SimProcessRoot::try_new(
                invalid_run_id.to_owned(),
                successful_policy_for_tests(),
                Arc::clone(&rules),
                Arc::clone(&profile),
                Arc::clone(&scenario),
            ),
            Err(SimProcessRootErrorV1::InvalidRunId)
        ));
    }

    assert!(matches!(
        SimProcessRoot::try_new(
            "run-a".to_owned(),
            failure_policy_for_tests("another-run"),
            rules,
            profile,
            scenario,
        ),
        Err(SimProcessRootErrorV1::FinalitySelectorRunMismatch { override_index: 0 })
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn task30_one_root_shares_one_sealed_policy_and_distinct_roots_are_isolated() {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("proxy profile");
    let scenario = scenario();
    let policy = successful_policy_for_tests();
    let mut first_root = SimProcessRoot::try_new(
        "run-a".to_owned(),
        Arc::clone(&policy),
        Arc::clone(&rules),
        Arc::clone(&profile),
        Arc::clone(&scenario),
    )
    .expect("first root");

    let first = first_root
        .create_session(spec_for_run(
            &scenario,
            SimCallerRoleV1::ModelTrain,
            "session-a",
            "run-a",
        ))
        .expect("first session");
    let second = first_root
        .create_session(spec_for_run(
            &scenario,
            SimCallerRoleV1::OfflineEvaluation,
            "session-b",
            "run-a",
        ))
        .expect("second session");
    assert_eq!(first.session.run_id(), "run-a");
    assert_eq!(second.session.run_id(), "run-a");
    assert!(Arc::ptr_eq(first.session.finality_policy(), &policy));
    assert!(Arc::ptr_eq(second.session.finality_policy(), &policy));
    assert_eq!(first_root.registered_session_count(), 2);

    let second_policy = successful_policy_for_tests();
    let second_root = SimProcessRoot::try_new(
        "run-b".to_owned(),
        Arc::clone(&second_policy),
        rules,
        profile,
        scenario,
    )
    .expect("isolated root");
    assert_eq!(second_root.run_id(), "run-b");
    assert_eq!(second_root.registered_session_count(), 0);
    assert!(!Arc::ptr_eq(first_root.finality_policy(), &second_policy));
    assert_eq!(
        first_root.finality_policy().content_sha256(),
        second_root.finality_policy().content_sha256()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task30_first_creation_has_the_only_handle_and_bridge_free_retry_reuses_anchor() {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("proxy profile");
    let scenario = scenario();
    let spec = session_spec(&scenario, SimCallerRoleV1::ModelTrain, "opening-retry");
    let mut process = execution_process_for_tests(rules, profile, scenario);

    let prepared = process
        .create_session(spec.clone(), accepting_submission_response_bridge())
        .expect("first creation returns the sole prepared handle");
    assert_eq!(
        prepared.control.disposition,
        SessionCreationDispositionV2::NewPrepared
    );
    assert_eq!(
        prepared.control.initialization.state,
        SessionInitializationStateV1::PreparedLedgerInit
    );
    assert!(matches!(
        process.create_session(spec.clone(), accepting_submission_response_bridge()),
        Err(SessionError::SessionAlreadyCreatedRequiresRetry(session_id))
            if session_id == spec.session_id
    ));
    let mut conflicting_spec = spec.clone();
    conflicting_spec.response_bridge_binding_id = "conflicting-bridge".to_owned();
    assert!(matches!(
        process.create_session(
            conflicting_spec,
            accepting_submission_response_bridge()
        ),
        Err(SessionError::SessionCreationIdentityConflict(session_id))
            if session_id == spec.session_id
    ));

    let retry = process
        .retry_create_session(&spec)
        .expect("bridge-free retry returns the existing control result");
    assert_eq!(
        retry.disposition,
        SessionCreationDispositionV2::ExistingPrepared
    );
    assert_eq!(
        retry.account_instance_key,
        prepared.control.account_instance_key
    );
    assert_eq!(retry.opening_anchor, prepared.control.opening_anchor);
    assert_eq!(
        retry.session_spec_sha256,
        prepared.control.session_spec_sha256
    );
    assert_eq!(
        prepared
            .session
            .model_visible_cursor()
            .private_feedback_sequence,
        0
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task30_same_run_account_instance_cannot_issue_a_second_opening_anchor() {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("proxy profile");
    let scenario = scenario();
    let original = session_spec(
        &scenario,
        SimCallerRoleV1::OfflineEvaluation,
        "capital-fence-a",
    );
    let mut duplicate = original.clone();
    duplicate.session_id = "capital-fence-b".to_owned();
    duplicate.creation_request_id = "capital-fence-b-creation".to_owned();
    duplicate.response_bridge_binding_id = "capital-fence-b-bridge".to_owned();
    let mut process = execution_process_for_tests(rules, profile, scenario);

    let first = process
        .create_session(original.clone(), accepting_submission_response_bridge())
        .expect("first account opening");
    assert!(matches!(
        process.create_session(duplicate, accepting_submission_response_bridge()),
        Err(SessionError::AccountInstanceAlreadyReserved)
    ));
    let retry = process
        .retry_create_session(&original)
        .expect("rejected duplicate did not disturb the first opening");
    assert_eq!(retry.opening_anchor, first.control.opening_anchor);
}

#[tokio::test(flavor = "current_thread")]
async fn task30_opening_identity_separates_session_spec_from_economic_anchor() {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("proxy profile");
    let scenario = scenario();
    let base_spec = session_spec(&scenario, SimCallerRoleV1::ModelTrain, "opening-identity");
    let create_in_isolated_root = |run_id: &str, spec: crate::session::SessionSpecV2| {
        let mut root = SimProcessRoot::try_new(
            run_id.to_owned(),
            successful_policy_for_tests(),
            Arc::clone(&rules),
            Arc::clone(&profile),
            Arc::clone(&scenario),
        )
        .expect("isolated root");
        root.create_session(spec).expect("isolated opening").control
    };

    let base = create_in_isolated_root("component-test-run", base_spec.clone());
    let mut scaled_spec = base_spec.clone();
    scaled_spec.initial_account_state =
        InitialAccountStateV1::try_new(Decimal::new(1_000_000_000, 6))
            .expect("equivalent scaled amount");
    let scaled = create_in_isolated_root("component-test-run", scaled_spec);
    assert_eq!(scaled.session_spec_sha256, base.session_spec_sha256);
    assert_eq!(scaled.opening_anchor, base.opening_anchor);

    let mut bridge_spec = base_spec.clone();
    bridge_spec.response_bridge_binding_id = "opening-identity-other-bridge".to_owned();
    let bridge = create_in_isolated_root("component-test-run", bridge_spec);
    assert_ne!(bridge.session_spec_sha256, base.session_spec_sha256);
    assert_eq!(bridge.opening_anchor, base.opening_anchor);

    let mut seed_spec = base_spec.clone();
    seed_spec.session_seed = Sha256Hex("9".repeat(64));
    let seed = create_in_isolated_root("component-test-run", seed_spec);
    assert_ne!(seed.session_spec_sha256, base.session_spec_sha256);
    assert_eq!(seed.opening_anchor, base.opening_anchor);

    let mut funded_spec = base_spec.clone();
    funded_spec.initial_account_state =
        InitialAccountStateV1::try_new(Decimal::new(1_001, 0)).expect("different amount");
    let funded = create_in_isolated_root("component-test-run", funded_spec);
    assert_ne!(funded.session_spec_sha256, base.session_spec_sha256);
    assert_ne!(
        funded.opening_anchor.anchor_id,
        base.opening_anchor.anchor_id
    );
    assert_ne!(
        funded.opening_anchor.anchor_sha256,
        base.opening_anchor.anchor_sha256
    );

    let mut other_run_spec = base_spec.clone();
    other_run_spec.run_id = "other-run".to_owned();
    let other_run = create_in_isolated_root("other-run", other_run_spec);
    assert_ne!(other_run.account_instance_key, base.account_instance_key);
    assert_ne!(
        other_run.opening_anchor.anchor_id,
        base.opening_anchor.anchor_id
    );
    assert_ne!(
        other_run.opening_anchor.anchor_sha256,
        base.opening_anchor.anchor_sha256
    );

    let account = &base.opening_anchor.initial_platform_account;
    assert_eq!(account.cash_pusd, ExactDecimal("1000".to_owned()));
    assert_eq!(account.available_pusd, account.cash_pusd);
    for zero_bucket in [
        &account.principal_encumbrance_pusd,
        &account.immediate_taker_fee_encumbrance_pusd,
        &account.maker_rebate_receivable_pusd,
        &account.taker_rebate_receivable_pusd,
        &account.maker_rebate_locked_remainder_pusd,
        &account.taker_rebate_locked_remainder_pusd,
    ] {
        assert_eq!(zero_bucket, &ExactDecimal("0".to_owned()));
    }
    assert_eq!(
        account
            .outcome_balances
            .iter()
            .map(|balance| balance.token_id.0.as_str())
            .collect::<Vec<_>>(),
        vec!["1", "2"]
    );
    assert!(
        account
            .outcome_balances
            .iter()
            .all(|balance| balance.balance_shares == ExactDecimal("0".to_owned()))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task30_prepared_methods_have_no_effect_and_existing_handle_observes_activation() {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("proxy profile");
    let scenario = scenario();
    let mut spec = session_spec(&scenario, SimCallerRoleV1::ModelTrain, "activation-gate");
    spec.response_outbox_capacity =
        std::num::NonZeroUsize::new(1).expect("positive response capacity");
    let mut process = execution_process_for_tests(rules, profile, scenario);
    let mut prepared = process
        .create_session(spec.clone(), accepting_submission_response_bridge())
        .expect("prepared session");
    let post_order = post_order_request(&prepared.session, "prepared-post-order");
    let post_orders = PostOrdersRequestV1 {
        context: request_context(&prepared.session, "prepared-post-orders", 1, 1_000),
        idempotency_key: "prepared-post-orders-idempotency".to_owned(),
        items: vec![post_order_item("prepared-post-orders-item")],
    };
    let cancel_order = CancelOrderRequestV1 {
        context: request_context(&prepared.session, "prepared-cancel-order", 1, 1_000),
        idempotency_key: "prepared-cancel-order-idempotency".to_owned(),
        item: CancelOrderItemV1 {
            item_id: "prepared-cancel-order-item".to_owned(),
            platform_order_id: "b".repeat(64),
        },
    };
    let cancel_orders = CancelOrdersRequestV1 {
        context: request_context(&prepared.session, "prepared-cancel-orders", 1, 1_000),
        idempotency_key: "prepared-cancel-orders-idempotency".to_owned(),
        items: vec![CancelOrderItemV1 {
            item_id: "prepared-cancel-orders-item".to_owned(),
            platform_order_id: "c".repeat(64),
        }],
    };
    let cancel_all_orders = CancelAllOrdersRequestV1 {
        context: request_context(&prepared.session, "prepared-cancel-all", 1, 1_000),
        idempotency_key: "prepared-cancel-all-idempotency".to_owned(),
    };
    let order = order_request(&prepared.session, "prepared-order");
    let orders = OrdersRequestV1 {
        context: request_context(&prepared.session, "prepared-orders", 1, 1_000),
        filter: OrdersFilterV1::default(),
        cursor: None,
    };
    let trades = TradesRequestV1 {
        context: request_context(&prepared.session, "prepared-trades", 1, 1_000),
        filter: TradesFilterV1::default(),
        cursor: None,
    };
    let cursor_before = prepared.session.model_visible_cursor();

    assert!(matches!(
        prepared.session.post_order(post_order.clone()),
        Err(SessionError::SessionNotActive(
            SessionInitializationStateV1::PreparedLedgerInit
        ))
    ));
    assert!(matches!(
        prepared.session.post_orders(post_orders),
        Err(SessionError::SessionNotActive(
            SessionInitializationStateV1::PreparedLedgerInit
        ))
    ));
    assert!(matches!(
        prepared.session.cancel_order(cancel_order),
        Err(SessionError::SessionNotActive(
            SessionInitializationStateV1::PreparedLedgerInit
        ))
    ));
    assert!(matches!(
        prepared.session.cancel_orders(cancel_orders),
        Err(SessionError::SessionNotActive(
            SessionInitializationStateV1::PreparedLedgerInit
        ))
    ));
    assert!(matches!(
        prepared.session.cancel_all_orders(cancel_all_orders),
        Err(SessionError::SessionNotActive(
            SessionInitializationStateV1::PreparedLedgerInit
        ))
    ));
    assert!(matches!(
        prepared.session.order(order),
        Err(SessionError::SessionNotActive(
            SessionInitializationStateV1::PreparedLedgerInit
        ))
    ));
    assert!(matches!(
        prepared.session.orders(orders),
        Err(SessionError::SessionNotActive(
            SessionInitializationStateV1::PreparedLedgerInit
        ))
    ));
    assert!(matches!(
        prepared.session.trades(trades),
        Err(SessionError::SessionNotActive(
            SessionInitializationStateV1::PreparedLedgerInit
        ))
    ));
    assert_eq!(prepared.session.model_visible_cursor(), cursor_before);

    let activation = activation_request(&spec.session_id, &prepared.control, "activation-1");
    let mut wrong_anchor_activation = activation.clone();
    wrong_anchor_activation.opening_anchor_sha256 = Sha256Hex("8".repeat(64));
    assert!(matches!(
        process.activate_session(wrong_anchor_activation),
        Err(SessionError::InitializationControlIdentityConflict)
    ));
    assert_eq!(
        process
            .session_initialization(&prepared.control.opening_anchor)
            .expect("错误锚点不得改变准备态")
            .state,
        SessionInitializationStateV1::PreparedLedgerInit
    );
    let activated = process
        .activate_session(activation.clone())
        .expect("activate prepared account");
    assert_eq!(
        activated.disposition,
        SessionInitializationControlDispositionV1::Activated
    );
    assert_eq!(
        activated.snapshot.state,
        SessionInitializationStateV1::Active
    );
    assert_eq!(
        prepared
            .session
            .post_order(post_order)
            .expect("existing handle sees ACTIVE")
            .receipt
            .disposition,
        RequestRegistrationDispositionV1::NewLogicalRequest
    );

    assert_eq!(
        process
            .activate_session(activation.clone())
            .expect("same activation is idempotent")
            .disposition,
        SessionInitializationControlDispositionV1::ExistingActive
    );
    let mut conflicting_activation = activation;
    conflicting_activation.activation_id = "activation-conflict".to_owned();
    assert!(matches!(
        process.activate_session(conflicting_activation),
        Err(SessionError::ActivationIdentityConflict)
    ));
    assert_eq!(
        process
            .session_initialization(&prepared.control.opening_anchor)
            .expect("active snapshot")
            .activation_id
            .as_deref(),
        Some("activation-1")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn task30_initialization_failure_is_idempotent_permanent_and_non_tradable() {
    let rules = successful_snapshot_for_sim_tests().await;
    let profile = load_embedded_proxy_latency_profile().expect("proxy profile");
    let scenario = scenario();
    let spec = session_spec(&scenario, SimCallerRoleV1::Paper, "failed-opening");
    let mut process = execution_process_for_tests(rules, profile, scenario);
    let mut prepared = process
        .create_session(spec.clone(), accepting_submission_response_bridge())
        .expect("prepared session");
    let failure = failure_request(&spec.session_id, &prepared.control, "ledger-failure-1");

    assert_eq!(
        process
            .fail_session_initialization(failure.clone())
            .expect("first explicit failure")
            .disposition,
        SessionInitializationControlDispositionV1::FailedClosed
    );
    assert_eq!(
        process
            .fail_session_initialization(failure.clone())
            .expect("same failure is idempotent")
            .disposition,
        SessionInitializationControlDispositionV1::ExistingFailedClosed
    );
    let mut conflicting_failure = failure;
    conflicting_failure.failure_id = "ledger-failure-conflict".to_owned();
    assert!(matches!(
        process.fail_session_initialization(conflicting_failure),
        Err(SessionError::FailureIdentityConflict)
    ));
    assert!(matches!(
        process.activate_session(activation_request(
            &spec.session_id,
            &prepared.control,
            "late-activation"
        )),
        Err(SessionError::InitializationFailedClosed)
    ));
    let failed_write = post_order_request(&prepared.session, "failed-write");
    assert!(matches!(
        prepared.session.post_order(failed_write),
        Err(SessionError::SessionNotActive(
            SessionInitializationStateV1::InitializationFailedClosed
        ))
    ));
    let snapshot = process
        .session_initialization(&prepared.control.opening_anchor)
        .expect("failed-closed snapshot");
    assert_eq!(
        snapshot.state,
        SessionInitializationStateV1::InitializationFailedClosed
    );
    assert_eq!(snapshot.failure_id.as_deref(), Some("ledger-failure-1"));
}
