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
