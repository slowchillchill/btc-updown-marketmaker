use tokio::sync::mpsc;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueConfigError {
    message: String,
}

impl QueueConfigError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackpressureSeverity {
    Healthy,
    SoftDegraded,
    HardDegraded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeAction {
    Continue,
    PauseIntake,
    ResumeIntake,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackpressureReceipt {
    pub source_kind: String,
    #[serde(default)]
    pub source_kinds: Vec<String>,
    pub high_watermark: QueueSample,
    pub soft_observation_count: u64,
    pub hard_observation_count: u64,
    pub queue_full_observation_count: u64,
    pub consecutive_hard_samples: u32,
    pub longest_contiguous_hard_samples: u32,
    pub pause_intake_count: u64,
    pub recovery_count: u64,
    pub bounded_wait_success_count: u64,
    pub bounded_wait_success_ms_high: u64,
    pub send_timeout_count: u64,
    pub drop_count: u64,
    pub process_fatal: bool,
}

impl BackpressureReceipt {
    pub fn empty(source_kind: impl Into<String>) -> Self {
        let source_kind = source_kind.into();
        Self {
            source_kinds: vec![source_kind.clone()],
            source_kind,
            high_watermark: QueueSample::default(),
            soft_observation_count: 0,
            hard_observation_count: 0,
            queue_full_observation_count: 0,
            consecutive_hard_samples: 0,
            longest_contiguous_hard_samples: 0,
            pause_intake_count: 0,
            recovery_count: 0,
            bounded_wait_success_count: 0,
            bounded_wait_success_ms_high: 0,
            send_timeout_count: 0,
            drop_count: 0,
            process_fatal: false,
        }
    }

    pub fn is_valid_for_complete_manifest(&self) -> bool {
        !self.source_kind.trim().is_empty()
            && !self
                .source_kinds
                .iter()
                .any(|source| source.trim().is_empty())
            && !self.process_fatal
            && self.pause_intake_count == 0
            && self.send_timeout_count == 0
            && self.drop_count == 0
    }

    pub fn covers_source_kind(&self, source_kind: &str) -> bool {
        let source_kind = source_kind.trim();
        !source_kind.is_empty()
            && (self.source_kind == source_kind
                || self
                    .source_kinds
                    .iter()
                    .any(|covered| covered.trim() == source_kind))
    }
}

pub struct BackpressureMonitor {
    source_kind: String,
    config: BackpressureConfig,
    high_watermark: QueueSample,
    consecutive_hard_samples: u32,
    longest_contiguous_hard_samples: u32,
    soft_observation_count: u64,
    hard_observation_count: u64,
    queue_full_observation_count: u64,
    pause_intake_count: u64,
    recovery_count: u64,
    bounded_wait_success_count: u64,
    bounded_wait_success_ms_high: u64,
    send_timeout_count: u64,
    drop_count: u64,
    intake_paused: bool,
    source_gaps: Vec<SourceGap>,
}

impl BackpressureMonitor {
    pub fn new(source_kind: impl Into<String>, config: BackpressureConfig) -> Self {
        Self {
            source_kind: source_kind.into(),
            config,
            high_watermark: QueueSample::default(),
            consecutive_hard_samples: 0,
            longest_contiguous_hard_samples: 0,
            soft_observation_count: 0,
            hard_observation_count: 0,
            queue_full_observation_count: 0,
            pause_intake_count: 0,
            recovery_count: 0,
            bounded_wait_success_count: 0,
            bounded_wait_success_ms_high: 0,
            send_timeout_count: 0,
            drop_count: 0,
            intake_paused: false,
            source_gaps: Vec::new(),
        }
    }

    pub fn observe(&mut self, sample: QueueSample) -> BackpressureVerdict {
        self.high_watermark = self.high_watermark.max(sample);

        if sample.exceeds_or_equals(self.config.hard) {
            self.consecutive_hard_samples = self.consecutive_hard_samples.saturating_add(1);
            self.hard_observation_count = self.hard_observation_count.saturating_add(1);
            self.longest_contiguous_hard_samples = self
                .longest_contiguous_hard_samples
                .max(self.consecutive_hard_samples);
            let action =
                if self.consecutive_hard_samples >= self.config.consecutive_hard_samples_to_pause {
                    if !self.intake_paused {
                        self.pause_intake_count = self.pause_intake_count.saturating_add(1);
                    }
                    self.intake_paused = true;
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
            self.soft_observation_count = self.soft_observation_count.saturating_add(1);
            return BackpressureVerdict {
                severity: BackpressureSeverity::SoftDegraded,
                action: RuntimeAction::Continue,
                watermark: sample,
                process_fatal: false,
                reason: Some("soft_watermark".to_owned()),
            };
        }

        let action = if self.intake_paused {
            self.intake_paused = false;
            self.recovery_count = self.recovery_count.saturating_add(1);
            RuntimeAction::ResumeIntake
        } else {
            RuntimeAction::Continue
        };

        BackpressureVerdict {
            severity: BackpressureSeverity::Healthy,
            action,
            watermark: sample,
            process_fatal: false,
            reason: None,
        }
    }

    pub fn record_bounded_wait_success(
        &mut self,
        sample: QueueSample,
        wait_ms: u64,
    ) -> BackpressureVerdict {
        self.bounded_wait_success_count = self.bounded_wait_success_count.saturating_add(1);
        self.bounded_wait_success_ms_high = self.bounded_wait_success_ms_high.max(wait_ms);
        self.observe(sample)
    }

    pub fn record_queue_full(&mut self, sample: QueueSample) -> BackpressureVerdict {
        self.queue_full_observation_count = self.queue_full_observation_count.saturating_add(1);
        self.observe(sample)
    }

    pub fn record_send_timeout(&mut self, sample: QueueSample) -> BackpressureVerdict {
        self.send_timeout_count = self.send_timeout_count.saturating_add(1);
        self.source_gaps.push(SourceGap {
            source_kind: self.source_kind.clone(),
            reason: "bounded_send_timeout".to_owned(),
            watermark: sample,
        });
        self.observe(sample)
    }

    pub fn record_drop(&mut self, sample: QueueSample) -> BackpressureVerdict {
        self.drop_count = self.drop_count.saturating_add(1);
        self.source_gaps.push(SourceGap {
            source_kind: self.source_kind.clone(),
            reason: "dropped_event".to_owned(),
            watermark: sample,
        });
        self.observe(sample)
    }

    pub fn high_watermark(&self) -> QueueSample {
        self.high_watermark
    }

    pub fn receipt(&self) -> BackpressureReceipt {
        BackpressureReceipt {
            source_kind: self.source_kind.clone(),
            source_kinds: vec![self.source_kind.clone()],
            high_watermark: self.high_watermark,
            soft_observation_count: self.soft_observation_count,
            hard_observation_count: self.hard_observation_count,
            queue_full_observation_count: self.queue_full_observation_count,
            consecutive_hard_samples: self.consecutive_hard_samples,
            longest_contiguous_hard_samples: self.longest_contiguous_hard_samples,
            pause_intake_count: self.pause_intake_count,
            recovery_count: self.recovery_count,
            bounded_wait_success_count: self.bounded_wait_success_count,
            bounded_wait_success_ms_high: self.bounded_wait_success_ms_high,
            send_timeout_count: self.send_timeout_count,
            drop_count: self.drop_count,
            process_fatal: false,
        }
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
