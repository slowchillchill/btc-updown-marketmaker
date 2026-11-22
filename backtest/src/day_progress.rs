//! 日作业低频端点诊断。模拟水位只在完整事件处理后发布。
use crate::{ProcessCountersV1, process_clock_ticks_per_second_v1, process_measurement_v1};
use serde_json::{Value, json};
use std::{collections::VecDeque, time::Instant};

pub(crate) struct DayProgress {
    started: Instant,
    counters: Option<ProcessCountersV1>,
    hz: Option<f64>,
    // 最多保留最近60个输出端点，不随模拟输入增长。
    history: VecDeque<(f64, i64)>,
    active: Vec<bool>,
}
impl DayProgress {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            counters: ProcessCountersV1::read().ok(),
            hz: process_clock_ticks_per_second_v1(),
            history: VecDeque::new(),
            active: Vec::new(),
        }
    }
    pub fn snapshot(&mut self, watermarks: &[Option<i64>], active: &[bool], end: i64) -> Value {
        let elapsed = self.started.elapsed().as_secs_f64();
        let after = ProcessCountersV1::read().ok();
        let measurement =
            process_measurement_v1(self.counters.as_ref(), after.as_ref(), self.hz, elapsed);
        self.with_measurement(watermarks, active, end, elapsed, measurement)
    }
    fn with_measurement(
        &mut self,
        watermarks: &[Option<i64>],
        active: &[bool],
        end: i64,
        elapsed: f64,
        measurement: Value,
    ) -> Value {
        if self.active != active {
            self.history.clear();
            self.active = active.to_vec();
        }
        // 活动账户尚未报告时保留未知；终态仍显示，但不参与剩余工作估计。
        let slowest = watermarks
            .iter()
            .zip(active)
            .filter(|(_, active)| **active)
            .map(|(at, _)| *at)
            .collect::<Option<Vec<_>>>()
            .and_then(|v| v.into_iter().min());
        let recent = self.history.back().and_then(|p| rate(*p, elapsed, slowest));
        let longer = self
            .history
            .front()
            .and_then(|p| rate(*p, elapsed, slowest));
        let eta = slowest.zip(longer).and_then(|(at, speed)| {
            (speed > 0.0)
                .then_some((end.saturating_sub(at).max(0) as f64 / 60_000.0) / speed * 3600.0)
        });
        let rate_wall_window_seconds = self.history.front().map(|p| elapsed - p.0);
        if let Some(at) = slowest {
            self.history.push_back((elapsed, at));
            if self.history.len() > 60 {
                self.history.pop_front();
            }
        }
        json!({"completed_event_at_utc_ms":watermarks,"active_accounts":active,
            "slowest_completed_event_at_utc_ms":slowest,
            "recent_sim_minutes_per_wall_hour":recent,
            "long_sim_minutes_per_wall_hour":longer,
            "rate_wall_window_seconds":rate_wall_window_seconds,
            "eta_seconds_at_long_window_rate":eta,
            "eta_status":if eta.is_some(){"conditional_on_long_window_rate"}else{"no_reliable_eta"},
            "simulation_end_exclusive_utc_ms":end,
            "process_since_monitor_start":measurement})
    }
}
fn rate(previous: (f64, i64), elapsed: f64, at: Option<i64>) -> Option<f64> {
    let delta = elapsed - previous.0;
    let simulated = at?.checked_sub(previous.1)?;
    (delta > 0.0 && simulated >= 0).then_some(simulated as f64 / 60_000.0 / delta * 3600.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn completed_account_keeps_watermark_but_does_not_pin_remaining_rate() {
        let mut progress = DayProgress::new();
        progress.with_measurement(
            &[Some(100), Some(200)],
            &[true, true],
            1000,
            1.0,
            Value::Null,
        );
        let changed = progress.with_measurement(
            &[Some(100), Some(300)],
            &[false, true],
            1000,
            2.0,
            Value::Null,
        );
        assert_eq!(changed["completed_event_at_utc_ms"][0], 100);
        assert_eq!(changed["slowest_completed_event_at_utc_ms"], 300);
        assert!(changed["eta_seconds_at_long_window_rate"].is_null());
        let next = progress.with_measurement(
            &[Some(100), Some(400)],
            &[false, true],
            1000,
            3.0,
            Value::Null,
        );
        assert_eq!(next["slowest_completed_event_at_utc_ms"], 400);
        assert!(next["eta_seconds_at_long_window_rate"].as_f64().unwrap() > 0.0);
    }
    #[test]
    fn progress_uses_slowest_complete_event_and_keeps_missing_account_unknown() {
        let mut progress = DayProgress::new();
        let first =
            progress.with_measurement(&[Some(0), None], &[true, true], 180_000, 1.0, Value::Null);
        assert!(first["slowest_completed_event_at_utc_ms"].is_null());
        let first = progress.with_measurement(
            &[Some(60_000), Some(0)],
            &[true, true],
            180_000,
            2.0,
            Value::Null,
        );
        assert!(first["eta_seconds_at_long_window_rate"].is_null());
        let next = progress.with_measurement(
            &[Some(180_000), Some(60_000)],
            &[true, true],
            180_000,
            62.0,
            Value::Null,
        );
        assert_eq!(next["slowest_completed_event_at_utc_ms"], 60_000);
        assert_eq!(next["recent_sim_minutes_per_wall_hour"], 60.0);
        assert_eq!(next["eta_seconds_at_long_window_rate"], 120.0);
        for i in 0..100 {
            progress.with_measurement(
                &[Some(60_000 + i)],
                &[true],
                180_000,
                63.0 + i as f64,
                Value::Null,
            );
        }
        assert_eq!(progress.history.len(), 60);
    }
}
