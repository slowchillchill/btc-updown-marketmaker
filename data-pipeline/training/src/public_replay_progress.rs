//! 单次有限重放的可选标量进度；输出故障不参与业务结果。
use serde::Serialize;
use std::{
    io::Write,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Stage {
    MainReading,
    DependencyValidation,
    CandidateValidation,
    CandidateLocation,
    DependencyLocation,
    CoreApplication,
    Eof,
    Error,
}
#[derive(Default, Serialize)]
pub(super) struct Work {
    pub main_records_read: u64,
    pub dependency_records_validated: u64,
    pub candidate_records_validated: u64,
    pub candidate_location_rows_visited: u64,
    pub dependency_location_rows_visited: u64,
    pub candidate_target_records_parsed: u64,
    pub dependency_target_records_parsed: u64,
    pub applied_inputs: u64,
    pub generated_groups: u64,
    pub current_input_position: Option<u64>,
    pub end_input_applied: bool,
}
pub(super) struct ReplayProgress {
    pub work: Work,
    stage: Stage,
    known_total_inputs: Option<u64>,
    began: Instant,
    last_output: Instant,
    interval: Duration,
    seen_stages: u16,
    failed_stage: Option<Stage>,
    output: Option<Box<dyn Write + Send>>,
}
impl ReplayProgress {
    pub fn new(
        interval: Duration,
        output: impl Write + Send + 'static,
        total: Option<u64>,
    ) -> Self {
        let now = Instant::now();
        Self {
            work: Work::default(),
            stage: Stage::MainReading,
            known_total_inputs: total,
            began: now,
            last_output: now,
            interval,
            seen_stages: 0,
            failed_stage: None,
            output: Some(Box::new(output)),
        }
    }
    pub fn checkpoint(&mut self, stage: Stage, force: bool) {
        if self.output.is_none() {
            return;
        }
        self.stage = stage;
        let bit = 1 << stage as u16;
        let first = self.seen_stages & bit == 0;
        self.seen_stages |= bit;
        let now = Instant::now();
        if !force && !first && now.duration_since(self.last_output) < self.interval {
            return;
        }
        self.last_output = now;
        let snapshot = serde_json::json!({"schema":"finite_public_replay_progress_v1",
            "stage":self.stage,"failed_stage":self.failed_stage,"elapsed_seconds":now.duration_since(self.began).as_secs_f64(),
            "work":self.work,"known_total_inputs":self.known_total_inputs,
            "applied_input_fraction":self.known_total_inputs.filter(|n|*n>0).map(|n|self.work.applied_inputs as f64/n as f64),
            "validation_total":null,"location_total":null,"eta_seconds":null});
        let output = self.output.as_mut().unwrap();
        let written = serde_json::to_writer(&mut **output, &snapshot)
            .map_err(std::io::Error::other)
            .and_then(|_| output.write_all(b"\n"));
        if written.is_err() {
            self.output = None;
        }
    }
    pub fn error(&mut self) {
        self.failed_stage = Some(self.stage);
        self.checkpoint(Stage::Error, true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    #[derive(Clone, Default)]
    struct Output(Arc<Mutex<Vec<u8>>>);
    impl Write for Output {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn long_interval_formats_once_for_repeated_work_then_reports_completion() {
        let output = Output::default();
        let mut progress = ReplayProgress::new(Duration::from_secs(3600), output.clone(), None);
        for _ in 0..1000 {
            progress.work.main_records_read += 1;
            progress.checkpoint(Stage::MainReading, false);
        }
        progress.checkpoint(Stage::Eof, true);
        let bytes = output.0.lock().unwrap();
        let lines: Vec<serde_json::Value> = std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["work"]["main_records_read"], 1);
        assert_eq!(lines[1]["work"]["main_records_read"], 1000);
        assert_eq!(lines[1]["stage"], "eof");
    }
}
