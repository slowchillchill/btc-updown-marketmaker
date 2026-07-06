use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NetworkFailure {
    ConnectionFailed,
    Disconnected,
    ServerShutdown,
    SubscriptionError,
    HttpError,
    WsProtocolError,
    EmptyPayload,
    EmptyArray,
    EmptySnapshot,
    NoBusinessMessages,
    PongButDataSilent,
    RateLimit,
    ConnectionLimit,
    SubscriptionLimit,
    SnapshotMissing,
    SnapshotStale,
    UpdateIdGap,
    SourceStale,
    SourceGap,
    CoverageDrop,
    WriterQueueBlocked,
    BackpressureTriggered,
    DiskWatermarkExceeded,
    MemoryWatermarkExceeded,
    CpuWatermarkExceeded,
}

impl NetworkFailure {
    pub fn required_phase0_cases() -> Vec<Self> {
        vec![
            Self::ConnectionFailed,
            Self::Disconnected,
            Self::ServerShutdown,
            Self::SubscriptionError,
            Self::HttpError,
            Self::WsProtocolError,
            Self::EmptyPayload,
            Self::EmptyArray,
            Self::EmptySnapshot,
            Self::NoBusinessMessages,
            Self::PongButDataSilent,
            Self::RateLimit,
            Self::ConnectionLimit,
            Self::SubscriptionLimit,
            Self::SnapshotMissing,
            Self::SnapshotStale,
            Self::UpdateIdGap,
            Self::SourceStale,
            Self::SourceGap,
            Self::CoverageDrop,
        ]
    }

    pub fn required_demand_cases() -> Vec<Self> {
        let mut cases = Self::required_phase0_cases();
        cases.extend([
            Self::WriterQueueBlocked,
            Self::BackpressureTriggered,
            Self::DiskWatermarkExceeded,
            Self::MemoryWatermarkExceeded,
            Self::CpuWatermarkExceeded,
        ]);
        cases
    }

    pub fn artifact_signal(self) -> ArtifactSignal {
        match self {
            Self::ConnectionFailed
            | Self::Disconnected
            | Self::ServerShutdown
            | Self::SubscriptionError
            | Self::HttpError
            | Self::WsProtocolError
            | Self::RateLimit
            | Self::ConnectionLimit
            | Self::SubscriptionLimit => ArtifactSignal::ReconnectTrace,
            Self::EmptyPayload
            | Self::EmptyArray
            | Self::EmptySnapshot
            | Self::SnapshotMissing
            | Self::SnapshotStale
            | Self::UpdateIdGap
            | Self::SourceGap
            | Self::CoverageDrop
            | Self::WriterQueueBlocked
            | Self::BackpressureTriggered
            | Self::DiskWatermarkExceeded
            | Self::MemoryWatermarkExceeded
            | Self::CpuWatermarkExceeded => ArtifactSignal::SourceGap,
            Self::NoBusinessMessages | Self::PongButDataSilent | Self::SourceStale => {
                ArtifactSignal::SourceHealth
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactSignal {
    SourceHealth,
    SourceGap,
    ReconnectTrace,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub max_elapsed_ms: u64,
    pub multiplier: u64,
    pub jitter_bps: u32,
    pub jitter_seed: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconnectAttempt {
    pub source_kind: String,
    pub failure: NetworkFailure,
    pub attempt: u32,
    pub sleep_ms: u64,
    pub max_elapsed_ms: u64,
    pub cumulative_elapsed_ms: u64,
    pub jitter_bps: u32,
    pub jitter_seed: u64,
    pub retry_exhausted: bool,
}

pub fn plan_retry_trace(
    source_kind: impl Into<String>,
    failure: NetworkFailure,
    policy: RetryPolicy,
) -> Vec<ReconnectAttempt> {
    let source_kind = source_kind.into();
    let attempts = policy.max_attempts.max(1);
    let multiplier = policy.multiplier.max(1);
    let max_elapsed_ms = policy.max_elapsed_ms.max(1);
    let mut trace = Vec::with_capacity(attempts as usize);
    let mut base_sleep = policy.initial_backoff_ms.max(1);
    let mut elapsed_ms = 0_u64;

    for attempt in 1..=attempts {
        if elapsed_ms >= max_elapsed_ms {
            break;
        }
        let jittered = jittered_sleep_ms(
            &source_kind,
            attempt,
            base_sleep,
            policy.jitter_bps,
            policy.jitter_seed,
        );
        let remaining_ms = max_elapsed_ms - elapsed_ms;
        let sleep_ms = jittered.min(policy.max_backoff_ms.max(1)).min(remaining_ms);
        elapsed_ms = elapsed_ms.saturating_add(sleep_ms);
        let retry_exhausted = attempt == attempts || elapsed_ms >= max_elapsed_ms;

        trace.push(ReconnectAttempt {
            source_kind: source_kind.clone(),
            failure,
            attempt,
            sleep_ms,
            max_elapsed_ms,
            cumulative_elapsed_ms: elapsed_ms,
            jitter_bps: policy.jitter_bps,
            jitter_seed: policy.jitter_seed,
            retry_exhausted,
        });
        if retry_exhausted {
            break;
        }

        base_sleep = base_sleep.saturating_mul(multiplier);
    }

    trace
}

fn jittered_sleep_ms(
    source_kind: &str,
    attempt: u32,
    base_sleep_ms: u64,
    jitter_bps: u32,
    jitter_seed: u64,
) -> u64 {
    if jitter_bps == 0 {
        return base_sleep_ms;
    }

    let spread = base_sleep_ms.saturating_mul(jitter_bps as u64) / 10_000;
    if spread == 0 {
        return base_sleep_ms;
    }

    let bucket_count = spread.saturating_mul(2).saturating_add(1);
    let jitter_bucket = stable_jitter_hash(source_kind, attempt, jitter_seed) % bucket_count;
    base_sleep_ms
        .saturating_sub(spread)
        .saturating_add(jitter_bucket)
        .max(1)
}

fn stable_jitter_hash(source_kind: &str, attempt: u32, jitter_seed: u64) -> u64 {
    let mut hash = 14_695_981_039_346_656_037_u64 ^ jitter_seed;
    for byte in source_kind
        .bytes()
        .chain(attempt.to_le_bytes())
        .chain(jitter_seed.to_le_bytes())
    {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    hash
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircuitState {
    Degraded,
    Open,
    HalfOpen,
    Recovered,
    Exhausted,
}

#[derive(Default)]
pub struct SourceHealthRegistry {
    states: HashMap<String, CircuitState>,
    failures: HashMap<String, NetworkFailure>,
}

impl SourceHealthRegistry {
    pub fn record_failure(&mut self, source_kind: impl Into<String>, failure: NetworkFailure) {
        let source_kind = source_kind.into();
        self.failures.insert(source_kind.clone(), failure);
        self.states.insert(source_kind, CircuitState::Degraded);
    }

    pub fn open_circuit(&mut self, source_kind: impl Into<String>) {
        self.states.insert(source_kind.into(), CircuitState::Open);
    }

    pub fn mark_half_open(&mut self, source_kind: impl Into<String>) {
        self.states
            .insert(source_kind.into(), CircuitState::HalfOpen);
    }

    pub fn mark_recovered(&mut self, source_kind: impl Into<String>) {
        self.states
            .insert(source_kind.into(), CircuitState::Recovered);
    }

    pub fn mark_exhausted(&mut self, source_kind: impl Into<String>) {
        self.states
            .insert(source_kind.into(), CircuitState::Exhausted);
    }

    pub fn state(&self, source_kind: &str) -> Option<CircuitState> {
        self.states.get(source_kind).copied()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FreshnessStatus {
    Reconnecting,
    Healthy,
    Stale,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoverageVerdict {
    Complete,
    Incomplete,
    Gap,
    Stale,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeFailureClass {
    SourceDegraded,
    BackpressureDegraded,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceHealthEvent {
    pub source_kind: String,
    pub failure: NetworkFailure,
    pub signal: ArtifactSignal,
    pub attempt: Option<u32>,
    pub sleep_ms: Option<u64>,
    pub final_state: CircuitState,
    pub freshness_verdict: FreshnessStatus,
    pub coverage_verdict: CoverageVerdict,
    pub runtime_failure_class: RuntimeFailureClass,
    pub process_fatal: bool,
}

impl SourceHealthEvent {
    pub fn from_failure(
        source_kind: impl Into<String>,
        failure: NetworkFailure,
        attempt: Option<u32>,
        sleep_ms: Option<u64>,
        final_state: CircuitState,
    ) -> Self {
        let signal = failure.artifact_signal();
        let freshness_verdict = match failure {
            NetworkFailure::NoBusinessMessages
            | NetworkFailure::PongButDataSilent
            | NetworkFailure::SourceStale => FreshnessStatus::Stale,
            NetworkFailure::ConnectionFailed
            | NetworkFailure::Disconnected
            | NetworkFailure::ServerShutdown
            | NetworkFailure::SubscriptionError
            | NetworkFailure::HttpError
            | NetworkFailure::WsProtocolError
            | NetworkFailure::RateLimit
            | NetworkFailure::ConnectionLimit
            | NetworkFailure::SubscriptionLimit => FreshnessStatus::Reconnecting,
            _ => FreshnessStatus::Healthy,
        };
        let coverage_verdict = match failure {
            NetworkFailure::NoBusinessMessages
            | NetworkFailure::PongButDataSilent
            | NetworkFailure::SourceStale => CoverageVerdict::Stale,
            NetworkFailure::ConnectionFailed
            | NetworkFailure::Disconnected
            | NetworkFailure::ServerShutdown
            | NetworkFailure::SubscriptionError
            | NetworkFailure::HttpError
            | NetworkFailure::WsProtocolError
            | NetworkFailure::RateLimit
            | NetworkFailure::ConnectionLimit
            | NetworkFailure::SubscriptionLimit => CoverageVerdict::Incomplete,
            _ => CoverageVerdict::Gap,
        };
        let runtime_failure_class = match failure {
            NetworkFailure::WriterQueueBlocked
            | NetworkFailure::BackpressureTriggered
            | NetworkFailure::DiskWatermarkExceeded
            | NetworkFailure::MemoryWatermarkExceeded
            | NetworkFailure::CpuWatermarkExceeded => RuntimeFailureClass::BackpressureDegraded,
            _ => RuntimeFailureClass::SourceDegraded,
        };

        Self {
            source_kind: source_kind.into(),
            failure,
            signal,
            attempt,
            sleep_ms,
            final_state,
            freshness_verdict,
            coverage_verdict,
            runtime_failure_class,
            process_fatal: false,
        }
    }
}

pub struct SourceHealthTracker {
    source_kind: String,
    freshness_timeout_ms: i64,
    connected_at_ms: Option<i64>,
    last_pong_at_ms: Option<i64>,
    last_business_at_ms: Option<i64>,
    last_stale_reason: Option<&'static str>,
}

impl SourceHealthTracker {
    pub fn new(source_kind: impl Into<String>, freshness_timeout_ms: i64) -> Self {
        Self {
            source_kind: source_kind.into(),
            freshness_timeout_ms,
            connected_at_ms: None,
            last_pong_at_ms: None,
            last_business_at_ms: None,
            last_stale_reason: None,
        }
    }

    pub fn record_connected(&mut self, at_ms: i64) {
        self.connected_at_ms = Some(at_ms);
        self.last_stale_reason = None;
    }

    pub fn record_pong(&mut self, at_ms: i64) {
        self.last_pong_at_ms = Some(at_ms);
    }

    pub fn record_business_message(&mut self, at_ms: i64) {
        self.last_business_at_ms = Some(at_ms);
        self.last_stale_reason = None;
    }

    pub fn evaluate_freshness(&mut self, now_ms: i64) -> FreshnessStatus {
        if self.connected_at_ms.is_none() {
            return FreshnessStatus::Reconnecting;
        }

        if let Some(last_business_at_ms) = self.last_business_at_ms {
            if now_ms - last_business_at_ms <= self.freshness_timeout_ms {
                return FreshnessStatus::Healthy;
            }
            self.last_stale_reason = Some("business_data_stale");
            return FreshnessStatus::Stale;
        }

        if let Some(last_pong_at_ms) = self.last_pong_at_ms {
            if now_ms - last_pong_at_ms <= self.freshness_timeout_ms {
                return FreshnessStatus::Reconnecting;
            }
            self.last_stale_reason = Some("pong_without_business_data");
            return FreshnessStatus::Stale;
        }

        if let Some(connected_at_ms) = self.connected_at_ms
            && now_ms - connected_at_ms <= self.freshness_timeout_ms
        {
            return FreshnessStatus::Reconnecting;
        }

        self.last_stale_reason = Some("no_business_data");
        FreshnessStatus::Stale
    }

    pub fn last_stale_reason(&self) -> Option<&'static str> {
        self.last_stale_reason
    }

    pub fn source_kind(&self) -> &str {
        &self.source_kind
    }
}

pub struct ReconnectGate {
    tracker: SourceHealthTracker,
    subscription_ack_at_ms: Option<i64>,
    snapshot_at_ms: Option<i64>,
}

impl ReconnectGate {
    pub fn new(source_kind: impl Into<String>, freshness_timeout_ms: i64) -> Self {
        Self {
            tracker: SourceHealthTracker::new(source_kind, freshness_timeout_ms),
            subscription_ack_at_ms: None,
            snapshot_at_ms: None,
        }
    }

    pub fn record_connected(&mut self, at_ms: i64) {
        self.tracker.record_connected(at_ms);
        self.subscription_ack_at_ms = None;
        self.snapshot_at_ms = None;
    }

    pub fn record_subscription_ack(&mut self, at_ms: i64) {
        self.subscription_ack_at_ms = Some(at_ms);
    }

    pub fn record_snapshot(&mut self, at_ms: i64) {
        self.snapshot_at_ms = Some(at_ms);
    }

    pub fn record_business_message(&mut self, at_ms: i64) {
        self.tracker.record_business_message(at_ms);
    }

    pub fn status(&mut self, now_ms: i64) -> FreshnessStatus {
        if self.subscription_ack_at_ms.is_none()
            || self.snapshot_at_ms.is_none()
            || self.tracker.last_business_at_ms.is_none()
        {
            return FreshnessStatus::Reconnecting;
        }
        self.tracker.evaluate_freshness(now_ms)
    }
}
