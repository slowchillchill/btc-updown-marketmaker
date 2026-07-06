use data_collect::health::{
    ArtifactSignal, CircuitState, CoverageVerdict, FreshnessStatus, NetworkFailure,
    RuntimeFailureClass, SourceHealthEvent,
};

#[test]
fn demand_network_and_resource_failures_map_to_health_gap_or_reconnect_trace() {
    for failure in NetworkFailure::required_demand_cases() {
        let signal = failure.artifact_signal();
        assert!(
            matches!(
                signal,
                ArtifactSignal::SourceHealth
                    | ArtifactSignal::SourceGap
                    | ArtifactSignal::ReconnectTrace
            ),
            "{failure:?} must be observable"
        );
    }
}

#[test]
fn reconnect_trace_event_records_attempt_sleep_error_and_final_state() {
    let event = SourceHealthEvent::from_failure(
        "polymarket_market",
        NetworkFailure::ConnectionFailed,
        Some(3),
        Some(800),
        CircuitState::HalfOpen,
    );

    assert_eq!(event.source_kind, "polymarket_market");
    assert_eq!(event.failure, NetworkFailure::ConnectionFailed);
    assert_eq!(event.attempt, Some(3));
    assert_eq!(event.sleep_ms, Some(800));
    assert_eq!(event.final_state, CircuitState::HalfOpen);
    assert_eq!(event.signal, ArtifactSignal::ReconnectTrace);
}

#[test]
fn source_gap_and_stale_are_degraded_not_process_fatal() {
    let stale = SourceHealthEvent::from_failure(
        "polymarket_market",
        NetworkFailure::PongButDataSilent,
        None,
        None,
        CircuitState::Open,
    );
    assert_eq!(stale.freshness_verdict, FreshnessStatus::Stale);
    assert_eq!(stale.coverage_verdict, CoverageVerdict::Stale);
    assert_eq!(
        stale.runtime_failure_class,
        RuntimeFailureClass::SourceDegraded
    );
    assert!(!stale.process_fatal);

    let gap = SourceHealthEvent::from_failure(
        "binance_depth_update",
        NetworkFailure::UpdateIdGap,
        None,
        None,
        CircuitState::Open,
    );
    assert_eq!(gap.coverage_verdict, CoverageVerdict::Gap);
    assert_eq!(
        gap.runtime_failure_class,
        RuntimeFailureClass::SourceDegraded
    );
    assert!(!gap.process_fatal);
}

#[test]
fn backpressure_and_resource_watermark_are_recorded_without_default_process_exit() {
    let backpressure = SourceHealthEvent::from_failure(
        "writer",
        NetworkFailure::WriterQueueBlocked,
        None,
        None,
        CircuitState::Open,
    );

    assert_eq!(
        backpressure.runtime_failure_class,
        RuntimeFailureClass::BackpressureDegraded
    );
    assert_eq!(backpressure.signal, ArtifactSignal::SourceGap);
    assert!(!backpressure.process_fatal);
}
