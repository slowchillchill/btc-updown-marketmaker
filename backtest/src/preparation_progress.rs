//! 本次准备的可选标量进度；处理中计数不等于稳定发布。
use contracts::AppliedIngressV1;
use std::{
    io::Write,
    time::{Duration, Instant},
};

/// 仅供准备调用的协作式检查点；0关闭，输出故障关闭日志而不改变业务结果。
pub struct HistoricalPreparationProgressV1 {
    began: Instant,
    stage_began: Instant,
    last_output: Instant,
    interval: Duration,
    output: Option<Box<dyn Write + Send>>,
    stage: &'static str,
    applied_inputs: u64,
    groups: u64,
    observations: u64,
}
impl HistoricalPreparationProgressV1 {
    pub fn new(interval: Duration, output: impl Write + Send + 'static) -> Self {
        let now = Instant::now();
        Self {
            began: now,
            stage_began: now,
            last_output: now,
            interval,
            output: (!interval.is_zero()).then(|| Box::new(output) as Box<dyn Write + Send>),
            stage: "not_started",
            applied_inputs: 0,
            groups: 0,
            observations: 0,
        }
    }
    pub(crate) fn applied(&mut self, applied: &AppliedIngressV1) {
        if self.output.is_none() {
            return;
        }
        self.applied_inputs = applied.applied_prefix.next_input_position;
        self.groups += applied.closed_groups.len() as u64;
        self.observations += applied
            .closed_groups
            .iter()
            .map(|group| group.observations.len() as u64)
            .sum::<u64>();
        self.emit(false, None);
    }
    pub(crate) fn stage(&mut self, stage: &'static str) {
        if self.output.is_none() {
            return;
        }
        let previous = serde_json::json!({"stage":self.stage,"elapsed_seconds":self.stage_began.elapsed().as_secs_f64()});
        self.stage = stage;
        self.stage_began = Instant::now();
        self.emit(true, Some(previous));
    }
    fn emit(&mut self, force: bool, previous_stage: Option<serde_json::Value>) {
        let now = Instant::now();
        if !force && now.duration_since(self.last_output) < self.interval {
            return;
        }
        self.last_output = now;
        let snapshot = serde_json::json!({"schema":"historical_preparation_progress_v1","stage":self.stage,
            "elapsed_seconds":now.duration_since(self.began).as_secs_f64(),"previous_stage":previous_stage,
            "work":{"applied_inputs":self.applied_inputs,"generated_groups":self.groups,"generated_observations":self.observations},
            "published":self.stage=="complete","known_total_inputs":null,"applied_input_fraction":null,"eta_seconds":null,
            "phase_coverage":"admission/sort and hidden internals not independently instrumented"});
        let output = self.output.as_mut().unwrap();
        if serde_json::to_writer(&mut **output, &snapshot)
            .map_err(std::io::Error::other)
            .and_then(|_| output.write_all(b"\n"))
            .is_err()
        {
            self.output = None;
        }
    }
}
