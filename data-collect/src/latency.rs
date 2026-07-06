use serde::Serialize;

use crate::RawEvent;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimedRawEvent {
    pub event: RawEvent,
    pub source_recv_monotonic_ms: u64,
}

impl TimedRawEvent {
    pub fn new(event: RawEvent, source_recv_monotonic_ms: u64) -> Self {
        Self {
            event,
            source_recv_monotonic_ms,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequencedRawEvent {
    pub event_seq: u64,
    pub event: RawEvent,
    pub source_recv_monotonic_ms: u64,
    pub collector_dequeue_time_utc_ms: i64,
    pub collector_dequeue_monotonic_ms: u64,
}

impl SequencedRawEvent {
    pub fn hot_path_receipt(
        &self,
        hot_send_time_utc_ms: i64,
        hot_consumer_recv_time_utc_ms: i64,
    ) -> HotPathReceipt {
        HotPathReceipt {
            event_seq: self.event_seq,
            source_kind: self.event.source_kind.clone(),
            recv_time_utc_ms: self.event.recv_time_utc_ms,
            collector_dequeue_time_utc_ms: self.collector_dequeue_time_utc_ms,
            hot_send_time_utc_ms,
            hot_consumer_recv_time_utc_ms,
            source_recv_to_hot_delivery_ms: elapsed_utc_ms(
                self.event.recv_time_utc_ms,
                hot_consumer_recv_time_utc_ms,
            ),
            source_recv_to_collector_dequeue_ms: self
                .collector_dequeue_monotonic_ms
                .saturating_sub(self.source_recv_monotonic_ms),
            hot_queue_wait_ms: elapsed_utc_ms(hot_send_time_utc_ms, hot_consumer_recv_time_utc_ms),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LatencyFanout {
    next_event_seq: u64,
}

impl LatencyFanout {
    pub fn new(first_event_seq: u64) -> Self {
        Self {
            next_event_seq: first_event_seq,
        }
    }

    pub fn assign_sequence(
        &mut self,
        timed: TimedRawEvent,
        collector_dequeue_time_utc_ms: i64,
        collector_dequeue_monotonic_ms: u64,
    ) -> SequencedRawEvent {
        let event_seq = self.next_event_seq;
        self.next_event_seq = self.next_event_seq.saturating_add(1);
        SequencedRawEvent {
            event_seq,
            event: timed.event,
            source_recv_monotonic_ms: timed.source_recv_monotonic_ms,
            collector_dequeue_time_utc_ms,
            collector_dequeue_monotonic_ms,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HotPathReceipt {
    pub event_seq: u64,
    pub source_kind: String,
    pub recv_time_utc_ms: i64,
    pub collector_dequeue_time_utc_ms: i64,
    pub hot_send_time_utc_ms: i64,
    pub hot_consumer_recv_time_utc_ms: i64,
    pub source_recv_to_hot_delivery_ms: u64,
    pub source_recv_to_collector_dequeue_ms: u64,
    pub hot_queue_wait_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FlushReason {
    Rows,
    Time,
    Shutdown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlushPolicy {
    pub flush_rows: usize,
    pub max_flush_interval_ms: Option<u64>,
}

impl FlushPolicy {
    pub fn reason(
        &self,
        batch_len: usize,
        batch_oldest_monotonic_ms: Option<u64>,
        now_monotonic_ms: u64,
    ) -> Option<FlushReason> {
        if batch_len == 0 {
            return None;
        }
        if self.flush_rows > 0 && batch_len >= self.flush_rows {
            return Some(FlushReason::Rows);
        }
        let max_flush_interval_ms = self.max_flush_interval_ms?;
        let oldest_ms = batch_oldest_monotonic_ms?;
        if now_monotonic_ms.saturating_sub(oldest_ms) >= max_flush_interval_ms {
            return Some(FlushReason::Time);
        }
        None
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurableWriteBatch {
    batch_seq: u64,
    events: Vec<SequencedRawEvent>,
    flush_reason: FlushReason,
    batch_send_time_utc_ms: i64,
    writer_dequeue_time_utc_ms: i64,
    write_start_time_utc_ms: i64,
    write_done_time_utc_ms: i64,
    write_events_us: u64,
}

impl DurableWriteBatch {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch_seq: u64,
        events: Vec<SequencedRawEvent>,
        flush_reason: FlushReason,
        batch_send_time_utc_ms: i64,
        writer_dequeue_time_utc_ms: i64,
        write_start_time_utc_ms: i64,
        write_done_time_utc_ms: i64,
        write_events_us: u64,
    ) -> Self {
        Self {
            batch_seq,
            events,
            flush_reason,
            batch_send_time_utc_ms,
            writer_dequeue_time_utc_ms,
            write_start_time_utc_ms,
            write_done_time_utc_ms,
            write_events_us,
        }
    }

    pub fn receipt(&self) -> WriteBatchReceipt {
        let first_event_seq = self
            .events
            .first()
            .map(|event| event.event_seq)
            .unwrap_or(0);
        let last_event_seq = self.events.last().map(|event| event.event_seq).unwrap_or(0);
        let batch_first_recv_time_utc_ms = self
            .events
            .iter()
            .map(|event| event.event.recv_time_utc_ms)
            .min();
        let batch_last_recv_time_utc_ms = self
            .events
            .iter()
            .map(|event| event.event.recv_time_utc_ms)
            .max();

        WriteBatchReceipt {
            batch_seq: self.batch_seq,
            event_seqs: self.events.iter().map(|event| event.event_seq).collect(),
            first_event_seq,
            last_event_seq,
            rows: self.events.len() as u64,
            bytes_estimate: self.events.iter().map(estimated_event_bytes).sum(),
            batch_first_recv_time_utc_ms,
            batch_last_recv_time_utc_ms,
            batch_send_time_utc_ms: self.batch_send_time_utc_ms,
            writer_dequeue_time_utc_ms: self.writer_dequeue_time_utc_ms,
            write_start_time_utc_ms: self.write_start_time_utc_ms,
            write_done_time_utc_ms: self.write_done_time_utc_ms,
            write_events_us: self.write_events_us,
            writer_channel_wait_ms: elapsed_utc_ms(
                self.batch_send_time_utc_ms,
                self.writer_dequeue_time_utc_ms,
            ),
            batch_wait_ms: batch_first_recv_time_utc_ms
                .map(|started| elapsed_utc_ms(started, self.batch_send_time_utc_ms))
                .unwrap_or(0),
            flush_reason: self.flush_reason,
            source_recv_to_write_done_ms: self
                .events
                .iter()
                .map(|event| {
                    elapsed_utc_ms(event.event.recv_time_utc_ms, self.write_done_time_utc_ms)
                })
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct WriteBatchReceipt {
    pub batch_seq: u64,
    pub event_seqs: Vec<u64>,
    pub first_event_seq: u64,
    pub last_event_seq: u64,
    pub rows: u64,
    pub bytes_estimate: u64,
    pub batch_first_recv_time_utc_ms: Option<i64>,
    pub batch_last_recv_time_utc_ms: Option<i64>,
    pub batch_send_time_utc_ms: i64,
    pub writer_dequeue_time_utc_ms: i64,
    pub write_start_time_utc_ms: i64,
    pub write_done_time_utc_ms: i64,
    pub write_events_us: u64,
    pub writer_channel_wait_ms: u64,
    pub batch_wait_ms: u64,
    pub flush_reason: FlushReason,
    pub source_recv_to_write_done_ms: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueDurationTracker {
    queue_name: String,
    last_time_ms: Option<u64>,
    last_rows: u64,
    rows_high: u64,
    bytes_high: u64,
    non_empty_ms: u64,
    over_10_rows_ms: u64,
    over_100_rows_ms: u64,
    over_1000_rows_ms: u64,
    current_over_10_rows_ms: u64,
    current_over_100_rows_ms: u64,
    current_over_1000_rows_ms: u64,
    longest_contiguous_over_10_rows_ms: u64,
    longest_contiguous_over_100_rows_ms: u64,
    longest_contiguous_over_1000_rows_ms: u64,
    backpressure_wait_count: u64,
    backpressure_timeout_count: u64,
    dropped_rows: u64,
}

impl QueueDurationTracker {
    pub fn new(queue_name: impl Into<String>) -> Self {
        Self {
            queue_name: queue_name.into(),
            last_time_ms: None,
            last_rows: 0,
            rows_high: 0,
            bytes_high: 0,
            non_empty_ms: 0,
            over_10_rows_ms: 0,
            over_100_rows_ms: 0,
            over_1000_rows_ms: 0,
            current_over_10_rows_ms: 0,
            current_over_100_rows_ms: 0,
            current_over_1000_rows_ms: 0,
            longest_contiguous_over_10_rows_ms: 0,
            longest_contiguous_over_100_rows_ms: 0,
            longest_contiguous_over_1000_rows_ms: 0,
            backpressure_wait_count: 0,
            backpressure_timeout_count: 0,
            dropped_rows: 0,
        }
    }

    pub fn observe(&mut self, now_ms: u64, rows: u64, bytes: u64) {
        if let Some(last_time_ms) = self.last_time_ms {
            let elapsed_ms = now_ms.saturating_sub(last_time_ms);
            self.accumulate(elapsed_ms);
        }
        self.rows_high = self.rows_high.max(rows);
        self.bytes_high = self.bytes_high.max(bytes);
        self.last_time_ms = Some(now_ms);
        self.last_rows = rows;
    }

    pub fn record_backpressure_wait(&mut self) {
        self.backpressure_wait_count = self.backpressure_wait_count.saturating_add(1);
    }

    pub fn record_backpressure_timeout(&mut self) {
        self.backpressure_timeout_count = self.backpressure_timeout_count.saturating_add(1);
    }

    pub fn record_drop(&mut self, rows: u64) {
        self.dropped_rows = self.dropped_rows.saturating_add(rows);
    }

    pub fn finish(mut self, now_ms: u64) -> QueueDurationSummary {
        if let Some(last_time_ms) = self.last_time_ms {
            let elapsed_ms = now_ms.saturating_sub(last_time_ms);
            self.accumulate(elapsed_ms);
        }
        QueueDurationSummary {
            queue_name: self.queue_name,
            rows_high: self.rows_high,
            bytes_high: self.bytes_high,
            non_empty_ms: self.non_empty_ms,
            over_10_rows_ms: self.over_10_rows_ms,
            over_100_rows_ms: self.over_100_rows_ms,
            over_1000_rows_ms: self.over_1000_rows_ms,
            longest_contiguous_over_10_rows_ms: self.longest_contiguous_over_10_rows_ms,
            longest_contiguous_over_100_rows_ms: self.longest_contiguous_over_100_rows_ms,
            longest_contiguous_over_1000_rows_ms: self.longest_contiguous_over_1000_rows_ms,
            backpressure_wait_count: self.backpressure_wait_count,
            backpressure_timeout_count: self.backpressure_timeout_count,
            dropped_rows: self.dropped_rows,
        }
    }

    pub fn snapshot(&self, now_ms: u64) -> QueueDurationSummary {
        self.clone().finish(now_ms)
    }

    fn accumulate(&mut self, elapsed_ms: u64) {
        if self.last_rows > 0 {
            self.non_empty_ms = self.non_empty_ms.saturating_add(elapsed_ms);
        }
        self.accumulate_threshold(elapsed_ms, 10);
        self.accumulate_threshold(elapsed_ms, 100);
        self.accumulate_threshold(elapsed_ms, 1000);
    }

    fn accumulate_threshold(&mut self, elapsed_ms: u64, threshold: u64) {
        let over_threshold = self.last_rows > threshold;
        match threshold {
            10 if over_threshold => {
                self.over_10_rows_ms = self.over_10_rows_ms.saturating_add(elapsed_ms);
                self.current_over_10_rows_ms =
                    self.current_over_10_rows_ms.saturating_add(elapsed_ms);
                self.longest_contiguous_over_10_rows_ms = self
                    .longest_contiguous_over_10_rows_ms
                    .max(self.current_over_10_rows_ms);
            }
            10 => self.current_over_10_rows_ms = 0,
            100 if over_threshold => {
                self.over_100_rows_ms = self.over_100_rows_ms.saturating_add(elapsed_ms);
                self.current_over_100_rows_ms =
                    self.current_over_100_rows_ms.saturating_add(elapsed_ms);
                self.longest_contiguous_over_100_rows_ms = self
                    .longest_contiguous_over_100_rows_ms
                    .max(self.current_over_100_rows_ms);
            }
            100 => self.current_over_100_rows_ms = 0,
            1000 if over_threshold => {
                self.over_1000_rows_ms = self.over_1000_rows_ms.saturating_add(elapsed_ms);
                self.current_over_1000_rows_ms =
                    self.current_over_1000_rows_ms.saturating_add(elapsed_ms);
                self.longest_contiguous_over_1000_rows_ms = self
                    .longest_contiguous_over_1000_rows_ms
                    .max(self.current_over_1000_rows_ms);
            }
            1000 => self.current_over_1000_rows_ms = 0,
            _ => {}
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct QueueDurationSummary {
    pub queue_name: String,
    pub rows_high: u64,
    pub bytes_high: u64,
    pub non_empty_ms: u64,
    pub over_10_rows_ms: u64,
    pub over_100_rows_ms: u64,
    pub over_1000_rows_ms: u64,
    pub longest_contiguous_over_10_rows_ms: u64,
    pub longest_contiguous_over_100_rows_ms: u64,
    pub longest_contiguous_over_1000_rows_ms: u64,
    pub backpressure_wait_count: u64,
    pub backpressure_timeout_count: u64,
    pub dropped_rows: u64,
}

impl QueueDurationSummary {
    pub fn sustained_congestion_observed(
        &self,
        consecutive_samples: u64,
        sample_interval_ms: u64,
    ) -> bool {
        self.longest_contiguous_over_10_rows_ms
            >= consecutive_samples.saturating_mul(sample_interval_ms)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LatencyStats {
    pub count: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
}

impl LatencyStats {
    pub fn from_values(values: &[u64]) -> Self {
        if values.is_empty() {
            return Self {
                count: 0,
                p50: 0,
                p95: 0,
                p99: 0,
                max: 0,
            };
        }
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        Self {
            count: sorted.len() as u64,
            p50: percentile(&sorted, 50),
            p95: percentile(&sorted, 95),
            p99: percentile(&sorted, 99),
            max: *sorted.last().expect("non-empty sorted latency values"),
        }
    }
}

fn estimated_event_bytes(event: &SequencedRawEvent) -> u64 {
    (event.event.source_kind.len()
        + event.event.source_event_type.len()
        + event.event.source_identifier.len()
        + event.event.source_ts.as_ref().map_or(0, String::len)
        + event.event.raw_json.len()
        + event.event.collector_host_id.len()
        + event.event.collector_region.len()
        + event.event.parquet_raw_source_kind.len()) as u64
}

fn elapsed_utc_ms(start_ms: i64, end_ms: i64) -> u64 {
    end_ms.saturating_sub(start_ms).max(0) as u64
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = ((sorted.len() - 1) * percentile).div_ceil(100);
    sorted[index]
}
