use std::path::{Path, PathBuf};

use crate::publish::{Result, training_utc_day_partition};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventAvailability {
    CollectorRecvTime(i64),
    ConsumerRuntimeReceiveTime(i64),
}

impl EventAvailability {
    pub const fn value_ms(self) -> i64 {
        match self {
            Self::CollectorRecvTime(value) | Self::ConsumerRuntimeReceiveTime(value) => value,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrainingEventAvailability {
    pub partition_path: PathBuf,
    pub utc_day: String,
    pub event_available_time_ms: EventAvailability,
    pub source_timestamp_ms: Option<i64>,
}

pub struct TrainingAvailabilityPolicy;

impl TrainingAvailabilityPolicy {
    pub fn event_availability(
        dataset_root: &Path,
        source_kind: &str,
        recv_time_utc_ms: i64,
        source_timestamp_ms: Option<i64>,
    ) -> Result<TrainingEventAvailability> {
        let partition = training_utc_day_partition(dataset_root, source_kind, recv_time_utc_ms)?;
        Ok(TrainingEventAvailability {
            partition_path: partition.partition_path,
            utc_day: partition.utc_day,
            event_available_time_ms: EventAvailability::CollectorRecvTime(recv_time_utc_ms),
            source_timestamp_ms,
        })
    }

    pub fn visible_at_causal_cutoff(
        event_available_time: EventAvailability,
        causal_cutoff_ms: i64,
    ) -> bool {
        event_available_time.value_ms() <= causal_cutoff_ms
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaperLiveAvailabilityPolicy {
    environment: String,
}

impl PaperLiveAvailabilityPolicy {
    pub fn new(environment: impl Into<String>) -> Self {
        Self {
            environment: environment.into(),
        }
    }

    pub fn event_available_time_semantics(&self) -> String {
        format!("{}_runtime_receive_time", self.environment)
    }

    pub const fn requires_consumer_runtime_receive_timestamp(&self) -> bool {
        true
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentNaming {
    pub partition_path: PathBuf,
    pub running_path: PathBuf,
    pub final_path: PathBuf,
    pub manifest_path: PathBuf,
    pub stem: String,
}

impl SegmentNaming {
    pub fn new(
        dataset_root: &Path,
        source_kind: &str,
        segment_start_recv_time_utc_ms: i64,
        segment_seq: u64,
        run_id: &str,
    ) -> Result<Self> {
        let partition =
            training_utc_day_partition(dataset_root, source_kind, segment_start_recv_time_utc_ms)?;
        let stem = format!(
            "segment_start_recv_ms={segment_start_recv_time_utc_ms}--segment_seq={segment_seq}--{}",
            sanitize_run_id(run_id)
        );
        let running_path = partition
            .partition_path
            .join(format!("{stem}-running.parquet"));
        let final_path = partition.partition_path.join(format!("{stem}.parquet"));
        let manifest_path = partition
            .partition_path
            .join(format!("{stem}.manifest.json"));

        Ok(Self {
            partition_path: partition.partition_path,
            running_path,
            final_path,
            manifest_path,
            stem,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentPolicy {
    pub max_rows: u64,
    pub max_bytes: u64,
    pub max_segment_duration_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentState {
    pub rows: u64,
    pub bytes: u64,
    pub segment_start_recv_time_utc_ms: i64,
    pub latest_recv_time_utc_ms: i64,
    pub utc_day: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RolloverReason {
    MaxRows,
    MaxBytes,
    MaxSegmentDuration,
    UtcDayBoundary,
    Shutdown,
    ShutdownTimeout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentRolloverDecision {
    pub should_roll: bool,
    pub reason: Option<RolloverReason>,
    pub process_fatal: bool,
    pub open_next_segment: bool,
}

impl SegmentRolloverDecision {
    pub const fn continue_current() -> Self {
        Self {
            should_roll: false,
            reason: None,
            process_fatal: false,
            open_next_segment: false,
        }
    }

    pub const fn soft_roll(reason: RolloverReason) -> Self {
        Self {
            should_roll: true,
            reason: Some(reason),
            process_fatal: false,
            open_next_segment: true,
        }
    }

    pub const fn shutdown_finalize_timeout() -> Self {
        Self {
            should_roll: true,
            reason: Some(RolloverReason::ShutdownTimeout),
            process_fatal: false,
            open_next_segment: false,
        }
    }

    pub const fn write_incomplete_manifest(&self) -> bool {
        matches!(self.reason, Some(RolloverReason::ShutdownTimeout))
    }
}

impl SegmentPolicy {
    pub fn evaluate(&self, state: &SegmentState, current_utc_day: &str) -> SegmentRolloverDecision {
        if state.utc_day != current_utc_day {
            return SegmentRolloverDecision::soft_roll(RolloverReason::UtcDayBoundary);
        }
        if self.max_rows > 0 && state.rows >= self.max_rows {
            return SegmentRolloverDecision::soft_roll(RolloverReason::MaxRows);
        }
        if self.max_bytes > 0 && state.bytes >= self.max_bytes {
            return SegmentRolloverDecision::soft_roll(RolloverReason::MaxBytes);
        }
        if self.max_segment_duration_ms > 0
            && state
                .latest_recv_time_utc_ms
                .saturating_sub(state.segment_start_recv_time_utc_ms)
                >= self.max_segment_duration_ms
        {
            return SegmentRolloverDecision::soft_roll(RolloverReason::MaxSegmentDuration);
        }
        SegmentRolloverDecision::continue_current()
    }
}

fn sanitize_run_id(run_id: &str) -> String {
    let sanitized: String = run_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '-'
            }
        })
        .collect();
    let sanitized = sanitized.trim_matches('-');
    if sanitized.is_empty() {
        "run".to_owned()
    } else {
        sanitized.to_owned()
    }
}
