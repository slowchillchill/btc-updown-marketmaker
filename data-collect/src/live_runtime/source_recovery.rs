use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SourceRecoveryWaitLevel {
    InnerAttempt,
    OuterCycle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SourceRecoveryWait {
    pub(super) level: SourceRecoveryWaitLevel,
    pub(super) delay: Duration,
    pub(super) attempt: u32,
}

pub(super) struct SourceRecoverySupervisor {
    pub(super) source_kind: &'static str,
    pub(super) inner_attempts: u32,
    pub(super) outer_cycles: u32,
    pub(super) cycle_started: Instant,
    pub(super) jitter_seed: u64,
    #[cfg(test)]
    pub(super) wait_override: Option<Duration>,
}

impl SourceRecoverySupervisor {
    pub(super) fn new(source_kind: &'static str, jitter_seed: u64) -> Self {
        Self {
            source_kind,
            inner_attempts: 0,
            outer_cycles: 0,
            cycle_started: Instant::now(),
            jitter_seed,
            #[cfg(test)]
            wait_override: None,
        }
    }

    #[cfg(test)]
    pub(super) fn set_wait_override(&mut self, wait_override: Option<Duration>) {
        self.wait_override = wait_override;
    }

    fn apply_wait_override(&self, wait: SourceRecoveryWait) -> SourceRecoveryWait {
        #[cfg(test)]
        {
            let mut wait = wait;
            if let Some(delay) = self.wait_override {
                wait.delay = delay;
            }
            wait
        }
        #[cfg(not(test))]
        wait
    }

    pub(super) fn record_business_recovery(&mut self) {
        self.inner_attempts = 0;
        self.outer_cycles = 0;
        self.cycle_started = Instant::now();
    }

    pub(super) fn next_wait(&mut self) -> SourceRecoveryWait {
        self.inner_attempts = self.inner_attempts.saturating_add(1);
        let cycle_exhausted = self.inner_attempts > SOURCE_RECOVERY_CYCLE_MAX_ATTEMPTS
            || self.cycle_started.elapsed() >= SOURCE_RECOVERY_CYCLE_MAX_ELAPSED;
        if cycle_exhausted {
            self.outer_cycles = self.outer_cycles.saturating_add(1);
            self.inner_attempts = 0;
            self.cycle_started = Instant::now();
            let attempt = self.outer_cycles.min(32);
            return self.apply_wait_override(SourceRecoveryWait {
                level: SourceRecoveryWaitLevel::OuterCycle,
                delay: retry_delay(
                    self.source_kind,
                    attempt,
                    SOURCE_SUPERVISOR_BACKOFF_BASE_MS,
                    SOURCE_SUPERVISOR_BACKOFF_MAX_MS,
                    self.jitter_seed,
                ),
                attempt,
            });
        }
        self.apply_wait_override(SourceRecoveryWait {
            level: SourceRecoveryWaitLevel::InnerAttempt,
            delay: retry_delay(
                self.source_kind,
                self.inner_attempts,
                BINANCE_WS_RECONNECT_BASE_MS,
                BINANCE_WS_RECONNECT_MAX_MS,
                self.jitter_seed,
            ),
            attempt: self.inner_attempts,
        })
    }
}

pub(super) fn retry_delay(
    source_kind: &str,
    attempt: u32,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
    jitter_seed: u64,
) -> Duration {
    let attempt = attempt.max(1);
    let base_sleep_ms = initial_backoff_ms
        .max(1)
        .saturating_mul(2_u64.saturating_pow(attempt.saturating_sub(1)));
    let spread = base_sleep_ms.saturating_mul(SOURCE_RECOVERY_JITTER_BPS as u64) / 10_000;
    let jittered_sleep_ms = if spread == 0 {
        base_sleep_ms
    } else {
        let bucket_count = spread.saturating_mul(2).saturating_add(1);
        let mut hash = 14_695_981_039_346_656_037_u64 ^ jitter_seed;
        for byte in source_kind
            .bytes()
            .chain(attempt.to_le_bytes())
            .chain(jitter_seed.to_le_bytes())
        {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(1_099_511_628_211);
        }
        base_sleep_ms
            .saturating_sub(spread)
            .saturating_add(hash % bucket_count)
            .max(1)
    };
    Duration::from_millis(jittered_sleep_ms.min(max_backoff_ms.max(1)))
}

pub(super) async fn wait_for_source_recovery(
    recovery: &mut SourceRecoverySupervisor,
    sink: &SourceEventSink,
    deadline: Option<Instant>,
) -> bool {
    let wait = recovery.next_wait();
    let delay = deadline_remaining(deadline)
        .map(|remaining| remaining.min(wait.delay))
        .unwrap_or(wait.delay);
    if delay.is_zero() {
        return false;
    }
    sink.source_queue_backpressure
        .record_reconnect(recovery.source_kind, now_ms());
    sink.source_queue_backpressure
        .set_recovery_active(recovery.source_kind, true);
    sleep(delay).await;
    !deadline_reached(deadline)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_recovery_cycles_are_finite_jittered_capped_and_not_tight() {
        let mut recovery = SourceRecoverySupervisor::new("test_source", 7);
        for attempt in 1..=SOURCE_RECOVERY_CYCLE_MAX_ATTEMPTS {
            let wait = recovery.next_wait();
            assert_eq!(wait.level, SourceRecoveryWaitLevel::InnerAttempt);
            assert_eq!(wait.attempt, attempt);
            assert!(!wait.delay.is_zero());
            assert!(wait.delay <= Duration::from_millis(BINANCE_WS_RECONNECT_MAX_MS));
        }
        let outer = recovery.next_wait();
        assert_eq!(outer.level, SourceRecoveryWaitLevel::OuterCycle);
        assert_eq!(outer.attempt, 1);
        assert!(!outer.delay.is_zero());
        assert!(outer.delay <= Duration::from_millis(SOURCE_SUPERVISOR_BACKOFF_MAX_MS));
        assert_eq!(recovery.inner_attempts, 0);

        recovery.record_business_recovery();
        assert_eq!(recovery.inner_attempts, 0);
        assert_eq!(recovery.outer_cycles, 0);
        assert_eq!(
            recovery.next_wait().level,
            SourceRecoveryWaitLevel::InnerAttempt
        );

        recovery.cycle_started = Instant::now() - SOURCE_RECOVERY_CYCLE_MAX_ELAPSED;
        assert_eq!(
            recovery.next_wait().level,
            SourceRecoveryWaitLevel::OuterCycle,
            "max elapsed must end a cycle even before max attempts"
        );

        assert_ne!(
            retry_delay("test_source", 3, 500, 5_000, 7),
            retry_delay("test_source", 3, 500, 5_000, 8),
            "different stable jitter seeds should produce different retry delays"
        );
    }

    #[tokio::test]
    async fn source_recovery_outer_cycle_is_nonterminal_and_gap_neutral() {
        let (tx, _rx) = mpsc::channel(1);
        let gaps = Arc::new(Mutex::new(SourceGapStore::default()));
        let stats = Arc::new(SourceQueueBackpressureStats::default());
        let sink = SourceEventSink {
            tx,
            gaps: Arc::clone(&gaps),
            source_queue_backpressure: stats,
            inflight_budget: InflightBudget::new(),
            enqueue_lock: Arc::new(AsyncMutex::new(())),
        };
        let mut recovery = SourceRecoverySupervisor::new("test_source", 7);
        recovery.inner_attempts = SOURCE_RECOVERY_CYCLE_MAX_ATTEMPTS;

        let _ = wait_for_source_recovery(
            &mut recovery,
            &sink,
            Some(Instant::now() + Duration::from_millis(2)),
        )
        .await;

        assert_eq!(recovery.outer_cycles, 1);
        assert!(gaps.lock().unwrap().is_empty());
    }
}
