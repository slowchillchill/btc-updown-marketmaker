use data_collect::health::{
    CircuitState, ErrorClass, FreshnessStatus, NetworkFailure, ReconnectGate, RetryPolicy,
    SourceHealthRegistry, plan_retry_trace,
};

#[test]
fn reconnect_trace_records_bounded_backoff_jitter_and_validation_requirements() {
    let policy = RetryPolicy {
        max_attempts: 3,
        initial_backoff_ms: 100,
        max_backoff_ms: 250,
        max_elapsed_ms: 1_000,
        multiplier: 3,
        jitter_bps: 1_000,
        jitter_seed: 7,
    };

    let trace = plan_retry_trace("polymarket_market", NetworkFailure::RateLimit, policy);

    assert_eq!(trace.len(), 3);
    assert_eq!(trace[0].attempt, 1);
    assert_eq!(trace[0].failure, NetworkFailure::RateLimit);
    assert_eq!(trace[0].error_class, ErrorClass::RateLimited);
    assert_eq!(trace[0].max_backoff_ms, 250);
    assert!(trace.iter().all(|attempt| attempt.sleep_ms <= 250));
    assert!(
        trace
            .iter()
            .all(|attempt| attempt.cumulative_elapsed_ms <= 1_000)
    );
    assert!(
        trace
            .iter()
            .all(|attempt| attempt.subscription_recheck_required)
    );
    assert!(
        trace
            .iter()
            .all(|attempt| attempt.snapshot_recheck_required)
    );
    assert!(
        trace
            .iter()
            .all(|attempt| attempt.business_freshness_recheck_required)
    );
    assert!(
        trace
            .iter()
            .all(|attempt| attempt.coverage_recheck_required)
    );
    assert_eq!(trace.last().unwrap().final_state, CircuitState::Exhausted);
    assert!(trace.last().unwrap().retry_exhausted);

    let other_source = plan_retry_trace("rtds_chainlink", NetworkFailure::RateLimit, policy);
    assert_ne!(trace[0].sleep_ms, other_source[0].sleep_ms);
}

#[test]
fn reconnect_state_is_independent_per_source() {
    let mut registry = SourceHealthRegistry::default();

    registry.record_failure("polymarket_market", NetworkFailure::ConnectionFailed);
    registry.record_failure("binance_depth_update", NetworkFailure::SnapshotMissing);
    registry.open_circuit("polymarket_market");
    registry.mark_half_open("polymarket_market");
    registry.mark_recovered("binance_depth_update");
    registry.mark_exhausted("polymarket_market");

    assert_eq!(
        registry.state("polymarket_market"),
        Some(CircuitState::Exhausted)
    );
    assert_eq!(
        registry.failure("polymarket_market"),
        Some(NetworkFailure::ConnectionFailed)
    );
    assert_eq!(
        registry.state("binance_depth_update"),
        Some(CircuitState::Recovered)
    );
    assert_eq!(
        registry.failure("binance_depth_update"),
        Some(NetworkFailure::SnapshotMissing)
    );
}

#[test]
fn reconnect_recovery_requires_subscription_snapshot_business_and_coverage() {
    let mut gate = ReconnectGate::new("binance_depth_update", 1_000);

    gate.record_connected(0);
    gate.record_subscription_ack(100);
    gate.record_snapshot(200);
    gate.record_business_message(300);
    assert_eq!(gate.status(350), FreshnessStatus::Reconnecting);
    gate.record_source_coverage(350);
    assert_eq!(gate.status(400), FreshnessStatus::Healthy);

    gate.record_connected(1_000);
    gate.record_subscription_ack(1_100);
    gate.record_business_message(1_200);
    assert_eq!(gate.status(1_300), FreshnessStatus::Reconnecting);

    gate.record_snapshot(1_400);
    assert_eq!(gate.status(1_500), FreshnessStatus::Reconnecting);

    gate.record_source_coverage(1_450);
    assert_eq!(gate.status(1_500), FreshnessStatus::Healthy);
}

#[test]
fn reconnect_recovery_does_not_reuse_business_message_from_previous_connection() {
    let mut gate = ReconnectGate::new("polymarket_market", 1_000);

    gate.record_connected(0);
    gate.record_subscription_ack(100);
    gate.record_snapshot(200);
    gate.record_business_message(300);
    gate.record_source_coverage(350);
    assert_eq!(gate.status(400), FreshnessStatus::Healthy);

    gate.record_connected(500);
    gate.record_subscription_ack(550);
    gate.record_snapshot(600);
    gate.record_source_coverage(650);
    assert_eq!(gate.status(700), FreshnessStatus::Reconnecting);

    gate.record_business_message(750);
    assert_eq!(gate.status(800), FreshnessStatus::Healthy);
}
