use serde::Serialize;

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
    pub min: u64,
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
                min: 0,
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
            min: *sorted.first().expect("non-empty sorted latency values"),
            p50: percentile(&sorted, 50),
            p95: percentile(&sorted, 95),
            p99: percentile(&sorted, 99),
            max: *sorted.last().expect("non-empty sorted latency values"),
        }
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = ((sorted.len() - 1) * percentile).div_ceil(100);
    sorted[index]
}
