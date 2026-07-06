use tokio::sync::mpsc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueConfigError {
    message: String,
}

impl QueueConfigError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for QueueConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for QueueConfigError {}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueSample {
    pub rows: u64,
    pub bytes: u64,
    pub oldest_event_age_ms: u64,
    pub writer_lag_ms: u64,
}

impl QueueSample {
    fn exceeds_or_equals(self, threshold: Self) -> bool {
        self.rows >= threshold.rows
            || self.bytes >= threshold.bytes
            || self.oldest_event_age_ms >= threshold.oldest_event_age_ms
            || self.writer_lag_ms >= threshold.writer_lag_ms
    }

    fn max(self, other: Self) -> Self {
        Self {
            rows: self.rows.max(other.rows),
            bytes: self.bytes.max(other.bytes),
            oldest_event_age_ms: self.oldest_event_age_ms.max(other.oldest_event_age_ms),
            writer_lag_ms: self.writer_lag_ms.max(other.writer_lag_ms),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackpressureConfig {
    pub soft: QueueSample,
    pub hard: QueueSample,
    pub consecutive_hard_samples_to_pause: u32,
}

impl BackpressureConfig {
    pub fn new(
        soft: QueueSample,
        hard: QueueSample,
        consecutive_hard_samples_to_pause: u32,
    ) -> Self {
        Self {
            soft,
            hard,
            consecutive_hard_samples_to_pause: consecutive_hard_samples_to_pause.max(1),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackpressureSeverity {
    Healthy,
    SoftDegraded,
    HardDegraded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeAction {
    Continue,
    PauseIntake,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceGap {
    pub source_kind: String,
    pub reason: String,
    pub watermark: QueueSample,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackpressureVerdict {
    pub severity: BackpressureSeverity,
    pub action: RuntimeAction,
    pub watermark: QueueSample,
    pub process_fatal: bool,
    pub reason: Option<String>,
}

pub struct BackpressureMonitor {
    source_kind: String,
    config: BackpressureConfig,
    high_watermark: QueueSample,
    consecutive_hard_samples: u32,
    source_gaps: Vec<SourceGap>,
}

impl BackpressureMonitor {
    pub fn new(source_kind: impl Into<String>, config: BackpressureConfig) -> Self {
        Self {
            source_kind: source_kind.into(),
            config,
            high_watermark: QueueSample::default(),
            consecutive_hard_samples: 0,
            source_gaps: Vec::new(),
        }
    }

    pub fn observe(&mut self, sample: QueueSample) -> BackpressureVerdict {
        self.high_watermark = self.high_watermark.max(sample);

        if sample.exceeds_or_equals(self.config.hard) {
            self.consecutive_hard_samples = self.consecutive_hard_samples.saturating_add(1);
            let action =
                if self.consecutive_hard_samples >= self.config.consecutive_hard_samples_to_pause {
                    RuntimeAction::PauseIntake
                } else {
                    RuntimeAction::Continue
                };

            return BackpressureVerdict {
                severity: BackpressureSeverity::HardDegraded,
                action,
                watermark: sample,
                process_fatal: false,
                reason: Some("hard_watermark".to_owned()),
            };
        }

        self.consecutive_hard_samples = 0;

        if sample.exceeds_or_equals(self.config.soft) {
            return BackpressureVerdict {
                severity: BackpressureSeverity::SoftDegraded,
                action: RuntimeAction::Continue,
                watermark: sample,
                process_fatal: false,
                reason: Some("soft_watermark".to_owned()),
            };
        }

        BackpressureVerdict {
            severity: BackpressureSeverity::Healthy,
            action: RuntimeAction::Continue,
            watermark: sample,
            process_fatal: false,
            reason: None,
        }
    }

    pub fn record_queue_full(&mut self, sample: QueueSample) -> BackpressureVerdict {
        self.source_gaps.push(SourceGap {
            source_kind: self.source_kind.clone(),
            reason: "bounded_queue_full".to_owned(),
            watermark: sample,
        });
        self.observe(sample)
    }

    pub fn high_watermark(&self) -> QueueSample {
        self.high_watermark
    }

    pub fn source_gaps(&self) -> &[SourceGap] {
        &self.source_gaps
    }
}

pub fn bounded_event_queue<T>(
    capacity: usize,
) -> Result<(mpsc::Sender<T>, mpsc::Receiver<T>), QueueConfigError> {
    if capacity == 0 {
        return Err(QueueConfigError::new(
            "bounded event queue capacity must be positive",
        ));
    }
    Ok(mpsc::channel(capacity))
}
