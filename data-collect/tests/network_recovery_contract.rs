use data_collect::health::{
    ArtifactSignal, CircuitState, FreshnessStatus, NetworkFailure, ReconnectGate, RetryPolicy,
    SourceHealthRegistry, SourceHealthTracker, plan_retry_trace,
};

#[test]
fn network_failures_map_to_health_gap_or_reconnect_trace() {
    let required_cases = [
        NetworkFailure::ConnectionFailed,
        NetworkFailure::Disconnected,
        NetworkFailure::ServerShutdown,
        NetworkFailure::SubscriptionError,
        NetworkFailure::HttpError,
        NetworkFailure::WsProtocolError,
        NetworkFailure::EmptyPayload,
        NetworkFailure::EmptyArray,
        NetworkFailure::EmptySnapshot,
        NetworkFailure::NoBusinessMessages,
        NetworkFailure::PongButDataSilent,
        NetworkFailure::RateLimit,
        NetworkFailure::ConnectionLimit,
        NetworkFailure::SubscriptionLimit,
        NetworkFailure::SnapshotMissing,
        NetworkFailure::SnapshotStale,
        NetworkFailure::UpdateIdGap,
        NetworkFailure::SourceStale,
        NetworkFailure::SourceGap,
        NetworkFailure::CoverageDrop,
    ];
    assert_eq!(
        NetworkFailure::required_phase0_cases().len(),
        required_cases.len()
    );

    for failure in required_cases {
        let signal = failure.artifact_signal();
        assert!(
            matches!(
                signal,
                ArtifactSignal::SourceHealth
                    | ArtifactSignal::SourceGap
                    | ArtifactSignal::ReconnectTrace
            ),
            "{failure:?} must be recorded as health/gap/reconnect trace"
        );
    }
}

#[test]
fn retry_uses_bounded_backoff_jitter_and_records_trace() {
    let policy = RetryPolicy {
        max_attempts: 4,
        initial_backoff_ms: 100,
        max_backoff_ms: 500,
        max_elapsed_ms: 1_000,
        multiplier: 2,
        jitter_bps: 1_000,
        jitter_seed: 42,
    };

    let trace = plan_retry_trace(
        "polymarket_market",
        NetworkFailure::ConnectionFailed,
        policy,
    );

    assert_eq!(trace.len(), 4);
    assert_eq!(trace[0].attempt, 1);
    assert!(trace.iter().all(|item| item.max_elapsed_ms == 1_000));
    assert!(trace.iter().all(|item| item.jitter_seed == 42));
    assert!(trace.iter().all(|item| item.cumulative_elapsed_ms <= 1_000));
    assert!(trace.iter().all(|item| item.sleep_ms <= 500));
    assert!(trace.iter().all(|item| item.jitter_bps == 1_000));
    assert!(trace.last().unwrap().retry_exhausted);

    let other_source_trace = plan_retry_trace(
        "binance_depth_update",
        NetworkFailure::ConnectionFailed,
        policy,
    );
    assert_ne!(trace[0].sleep_ms, other_source_trace[0].sleep_ms);
}

#[test]
fn source_circuit_breakers_are_independent() {
    let mut registry = SourceHealthRegistry::default();

    registry.record_failure("polymarket_market", NetworkFailure::RateLimit);
    registry.record_failure("binance_depth_update", NetworkFailure::EmptyPayload);
    registry.open_circuit("polymarket_market");
    registry.mark_half_open("polymarket_market");

    assert_eq!(
        registry.state("polymarket_market"),
        Some(CircuitState::HalfOpen)
    );
    assert_eq!(
        registry.state("binance_depth_update"),
        Some(CircuitState::Degraded)
    );

    registry.mark_recovered("binance_depth_update");
    registry.mark_exhausted("polymarket_market");
    assert_eq!(
        registry.state("polymarket_market"),
        Some(CircuitState::Exhausted)
    );
    assert_eq!(
        registry.state("binance_depth_update"),
        Some(CircuitState::Recovered)
    );
}

#[test]
fn pong_without_business_data_marks_stale_not_healthy() {
    let mut tracker = SourceHealthTracker::new("polymarket_market", 1_000);
    tracker.record_connected(0);
    tracker.record_pong(500);

    assert_eq!(
        tracker.evaluate_freshness(900),
        FreshnessStatus::Reconnecting
    );
    let status = tracker.evaluate_freshness(1_501);

    assert_eq!(status, FreshnessStatus::Stale);
    assert_eq!(
        tracker.last_stale_reason(),
        Some("pong_without_business_data")
    );
}

#[test]
fn reconnect_does_not_reuse_pong_from_previous_connection() {
    let mut tracker = SourceHealthTracker::new("polymarket_market", 1_000);
    tracker.record_connected(0);
    tracker.record_pong(900);

    assert_eq!(
        tracker.evaluate_freshness(950),
        FreshnessStatus::Reconnecting
    );

    tracker.record_connected(1_000);
    assert_eq!(tracker.evaluate_freshness(2_001), FreshnessStatus::Stale);
    assert_eq!(tracker.last_stale_reason(), Some("no_business_data"));
}

#[test]
fn reconnect_requires_subscription_snapshot_and_fresh_business_data() {
    let mut gate = ReconnectGate::new("binance_depth_update", 1_000);

    gate.record_connected(0);
    assert_eq!(gate.status(100), FreshnessStatus::Reconnecting);

    gate.record_subscription_ack(100);
    assert_eq!(gate.status(200), FreshnessStatus::Reconnecting);

    gate.record_snapshot(300);
    assert_eq!(gate.status(400), FreshnessStatus::Reconnecting);

    gate.record_business_message(500);
    assert_eq!(gate.status(600), FreshnessStatus::Reconnecting);

    gate.record_source_coverage(550);
    assert_eq!(gate.status(600), FreshnessStatus::Healthy);

    assert_eq!(gate.status(1_700), FreshnessStatus::Stale);
}
