//! 显式工程窗口采样，在完整模拟毫秒处同步账户后读取一次进程端点。
use crate::{ProcessCountersV1, day_state::DayHubV1, process_clock_ticks_per_second_v1};
use sim_system::public_consumption::TrustedGroupDecisionCutoffV1;
use std::{collections::VecDeque, time::Instant};
pub(crate) struct DayCpuMeasurement {
    pub boundaries: VecDeque<i64>,
    arrived: Vec<bool>,
    cutoffs: Vec<Option<TrustedGroupDecisionCutoffV1>>,
    work: Vec<serde_json::Value>,
    started: Instant,
    hz: Option<f64>,
}
impl DayCpuMeasurement {
    pub fn new(boundaries: &[i64], candidates: usize) -> Self {
        Self {
            boundaries: boundaries
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
            arrived: vec![false; candidates],
            cutoffs: vec![None; candidates],
            work: vec![serde_json::Value::Null; candidates],
            started: Instant::now(),
            hz: process_clock_ticks_per_second_v1(),
        }
    }
    pub fn blocks_supply(&self, candidates: &[crate::day_state::CandidateProgressV1]) -> bool {
        self.arrived.iter().any(|arrived| *arrived)
            && candidates.iter().enumerate().any(|(i, candidate)| {
                candidate.active && !self.arrived[i] && candidate.waiting.is_some()
            })
    }
    /// 同步测量不能妨碍原有供料和退出。保留未对齐事实，关闭本次剩余窗口。
    pub fn abandon_waiting(&mut self, reason: &str) -> bool {
        if self.boundaries.is_empty() || !self.arrived.iter().any(|arrived| *arrived) {
            return false;
        }
        eprintln!(
            "day_cpu_boundary_unaligned={}",
            serde_json::json!({
            "reason":reason,"remaining_boundaries_utc_ms":self.boundaries,
            "arrived_accounts":self.arrived,"account_cutoffs":self.cutoffs,
            "aligned":false})
        );
        self.boundaries.clear();
        true
    }
}
pub(crate) fn sample_boundary(
    hub: &DayHubV1,
    index: usize,
    at: i64,
    cutoff: Option<TrustedGroupDecisionCutoffV1>,
) -> bool {
    let mut buffer = hub.0.0.lock().unwrap();
    let Some(measurement) = buffer.cpu_measurement.as_mut() else {
        return false;
    };
    let Some(boundary) = measurement.boundaries.front().copied() else {
        return false;
    };
    measurement.arrived[index] = true;
    measurement.cutoffs[index] = cutoff;
    #[cfg(feature = "stage-timing")]
    {
        measurement.work[index] = serde_json::json!({"history":shared_run::account_history_visits_v1(),"decision_recording":crate::stage_timing::recording_work()});
    }
    hub.0.1.control.notify_all();
    loop {
        let Some(measurement) = buffer.cpu_measurement.as_ref() else {
            break;
        };
        if measurement.boundaries.front() != Some(&boundary) {
            break;
        }
        if buffer.failure.is_some() {
            return false;
        }
        if hub
            .0
            .1
            .budget_stop_requested
            .load(std::sync::atomic::Ordering::Acquire)
        {
            buffer
                .cpu_measurement
                .as_mut()
                .unwrap()
                .abandon_waiting("budget_stop_requested");
            hub.0.1.control.notify_all();
            return false;
        }
        if buffer
            .candidates
            .iter()
            .enumerate()
            .all(|(i, c)| !c.active || measurement.arrived[i])
        {
            let active = buffer
                .candidates
                .iter()
                .map(|c| c.active)
                .collect::<Vec<_>>();
            let producer_public_end = buffer.public_base + buffer.public.len();
            let measurement = buffer.cpu_measurement.as_mut().unwrap();
            eprintln!(
                "day_cpu_boundary={}",
                serde_json::json!({"requested_at_utc_ms":boundary,
                "last_arrival_event_at_utc_ms":at,"account_cutoffs":measurement.cutoffs,
                "account_work":measurement.work,"active_accounts":active,"all_configured_accounts_present":active.iter().all(|v|*v),
                "producer_public_group_end":producer_public_end,
                "producer_applied_inputs":hub.0.1.producer_applied_inputs.load(std::sync::atomic::Ordering::Relaxed),
                "process_counters":ProcessCountersV1::read().ok(),"clock_ticks_per_second":measurement.hz,
                "measurement_elapsed_seconds":measurement.started.elapsed().as_secs_f64(),
                "mode":"synchronized_complete_event"})
            );
            measurement.boundaries.pop_front();
            measurement.arrived.fill(false);
            measurement.cutoffs.fill(None);
            hub.0.1.control.notify_all();
            break;
        }
        buffer = hub.0.1.control.wait(buffer).unwrap();
    }
    buffer
        .cpu_measurement
        .as_ref()
        .is_some_and(|m| !m.boundaries.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::day_state::{CandidateCursorV1, CandidateProgressV1};
    fn candidate() -> CandidateProgressV1 {
        CandidateProgressV1 {
            cursor: CandidateCursorV1 {
                public: 0,
                hidden: 0,
                times: crate::timing::DeliveryTimes {
                    delays: crate::DelayScheduleV1::Fixed(0),
                    last: None,
                },
            },
            active: true,
            waiting: None,
            source_health: None,
            source_progress: None,
            requirements: None,
            runtime_started: true,
        }
    }
    #[test]
    fn source_wait_and_backpressure_release_account_already_in_barrier() {
        let temp = tempfile::tempdir().unwrap();
        let (config, _) = crate::test_common::setup(temp.path());
        let prepared = crate::prepare_historical_run_v1(&config.preparation).unwrap();
        let groups = sim_system::public_consumption::RecordedPublicGroupSourceV1::open(
            &prepared.public_root,
            1,
            1 << 20,
            crate::timing::DeliveryTimes {
                delays: crate::DelayScheduleV1::Fixed(0),
                last: None,
            },
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let hub = DayHubV1::new(&[]);
        {
            let mut buffer = hub.0.0.lock().unwrap();
            buffer.candidates = vec![candidate(), candidate()];
            buffer.public.extend(groups.into_iter().map(Into::into));
            assert!(buffer.public.len() > 1, "已超过1组软上限");
            buffer.candidates[1].cursor.public = buffer.public.len();
            buffer.candidates[1].waiting = Some(crate::day_state::CandidateWaitSourceV1::Public);
            buffer.cpu_measurement = Some(DayCpuMeasurement::new(&[1, 2], 2));
        }
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| sample_boundary(&hub, 0, 1, None));
            let mut buffer = hub.0.0.lock().unwrap();
            while !buffer.cpu_measurement.as_ref().unwrap().arrived[0] {
                buffer = hub.0.1.control.wait(buffer).unwrap();
            }
            // 已到户占用软上限，未到户的真实来源等待要求生产者继续供料。
            assert!(
                buffer
                    .cpu_measurement
                    .as_ref()
                    .unwrap()
                    .blocks_supply(&buffer.candidates)
            );
            assert!(
                buffer
                    .cpu_measurement
                    .as_mut()
                    .unwrap()
                    .abandon_waiting("producer_backpressure")
            );
            hub.0.1.control.notify_all();
            drop(buffer);
            assert!(!worker.join().unwrap());
        });
    }
    #[test]
    fn budget_stop_releases_account_already_waiting_at_window() {
        let hub = DayHubV1::new(&[]);
        {
            let mut buffer = hub.0.0.lock().unwrap();
            buffer.candidates = vec![candidate(), candidate()];
            buffer.cpu_measurement = Some(DayCpuMeasurement::new(&[1, 2], 2));
        }
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| sample_boundary(&hub, 0, 1, None));
            let mut buffer = hub.0.0.lock().unwrap();
            while !buffer.cpu_measurement.as_ref().unwrap().arrived[0] {
                buffer = hub.0.1.control.wait(buffer).unwrap();
            }
            hub.0
                .1
                .budget_stop_requested
                .store(true, std::sync::atomic::Ordering::Release);
            hub.0.1.control.notify_all();
            drop(buffer);
            assert!(!worker.join().unwrap());
        });
        assert!(
            hub.0
                .0
                .lock()
                .unwrap()
                .cpu_measurement
                .as_ref()
                .unwrap()
                .boundaries
                .is_empty()
        );
    }
}
