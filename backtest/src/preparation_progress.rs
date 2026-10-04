//! 单次准备的可选阶段计量/标量进度；不持有逐记录历史。
use crate::{
    MaterialError, ProcessCountersV1, process_clock_ticks_per_second_v1, process_measurement_v1,
};
use contracts::AppliedIngressV1;
use data_pipeline_training::{
    SortMetrics,
    historical_preparation_progress::{
        HistoricalPreparationNoticeV1 as Notice, HistoricalPreparationStageV1 as Stage,
        HistoricalRawPassV1 as Pass,
    },
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    io::Write,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Default, Serialize)]
struct RawWork {
    files_completed: u64,
    decoded_batches: u64,
    physical_rows: u64,
    selected_rows: u64,
    initialization_files_completed: u64,
    initialization_decoded_batches: u64,
    initialization_physical_rows: u64,
    initialization_selected_rows: u64,
}
#[derive(Default, Serialize)]
struct Work {
    admission_files_completed: u64,
    admission_file_bytes: u64,
    admission_footer_rows: u64,
    known_daily_raw_files_per_pass: Option<u64>,
    known_daily_raw_rows_per_pass: Option<u64>,
    known_initialization_raw_rows_per_pass: Option<u64>,
    known_total_public_inputs: Option<u64>,
    initialization_selection: RawWork,
    epoch_pass: RawWork,
    second_supply: RawWork,
    source_sort_metrics: [Option<SortMetrics>; 6],
    applied_inputs: u64,
    generated_groups: u64,
    generated_observations: u64,
    hidden_records_read: u64,
    hidden_messages_supplied: u64,
    hidden_references_indexed: u64,
    hidden_messages_reduced: u64,
    hidden_steps_written: u64,
    hidden_reports_written: u64,
    hidden_steps_indexed: u64,
    hidden_message_sort: Option<SortMetrics>,
    hidden_index_sort: Option<SortMetrics>,
}
impl Work {
    fn raw(&mut self, pass: Pass) -> &mut RawWork {
        match pass {
            Pass::InitializationSelection => &mut self.initialization_selection,
            Pass::Epoch => &mut self.epoch_pass,
            Pass::SecondSupply => &mut self.second_supply,
        }
    }
}
struct Results {
    phases: [Option<Value>; 11],
    status: &'static str,
    failed_stage: Option<Stage>,
    work: Value,
    counter_sample_failures: u64,
}
impl Default for Results {
    fn default() -> Self {
        Self {
            phases: Default::default(),
            status: "disabled",
            failed_stage: None,
            work: Value::Null,
            counter_sample_failures: 0,
        }
    }
}
/// 仅共享固定11阶段诊断结果，不是公共/隐藏业务状态副本。
#[derive(Clone, Default)]
pub struct HistoricalPreparationMeasurementsV1(Arc<Mutex<Results>>);
impl HistoricalPreparationMeasurementsV1 {
    pub fn snapshot(&self, combined: &Value) -> Value {
        let results = self.0.lock().unwrap();
        if results.status == "disabled" {
            return json!({"schema":"historical_preparation_phase_measurements_v1","status":"disabled",
                "phases":null,"work":null,"residual":null,"counter_sample_failures":0});
        }
        let wall = results
            .phases
            .iter()
            .flatten()
            .map(|p| p["measurement"]["wall_seconds"].as_f64().unwrap())
            .sum::<f64>();
        let mut value = json!({"schema":"historical_preparation_phase_measurements_v1","status":results.status,
            "failed_stage":results.failed_stage,"phases":results.phases,"work":results.work,
            "counter_sample_failures":results.counter_sample_failures,
            "accounted_phase_wall_seconds":wall,
            "residual":{"wall_seconds":combined["wall_seconds"].as_f64().map(|v|v-wall),"user_seconds":null,"system_seconds":null,"proc_io":null},
            "measurement_note":"consecutive call intervals; parent total is not added; process I/O includes diagnostics; rchar is not device read; endpoint RSS is not peak; cooperative batch/file/loop checkpoints, no blocking-call heartbeat"});
        if !results
            .phases
            .iter()
            .flatten()
            .all(|p| p["measurement"]["available"] == true)
        {
            return value;
        }
        for key in ["user_seconds", "system_seconds"] {
            value["residual"][key] = json!(combined[key].as_f64().map(|v| {
                v - results
                    .phases
                    .iter()
                    .flatten()
                    .map(|p| p["measurement"][key].as_f64().unwrap())
                    .sum::<f64>()
            }));
        }
        if let Some(io) = combined["proc_io"].as_object() {
            let mut residual = serde_json::Map::new();
            for (key, v) in io {
                let sum = results
                    .phases
                    .iter()
                    .flatten()
                    .map(|p| p["measurement"]["proc_io"][key].as_u64().unwrap_or(0) as i128)
                    .sum::<i128>();
                residual.insert(key.clone(), json!(v.as_u64().map(|v| v as i128 - sum)));
            }
            value["residual"]["proc_io"] = Value::Object(residual);
        }
        value
    }
}
struct Current {
    stage: Stage,
    began: Instant,
    before: Option<ProcessCountersV1>,
}
/// 0关闭进度及内部阶段采样；输出故障只关闭日志，计量不可用不影响业务。
pub struct HistoricalPreparationProgressV1 {
    began: Instant,
    last_output: Instant,
    interval: Duration,
    output: Option<Box<dyn Write + Send>>,
    current: Option<Current>,
    work: Work,
    results: HistoricalPreparationMeasurementsV1,
    hz: Option<f64>,
    reader: fn() -> Result<ProcessCountersV1, MaterialError>,
    sample_failures: u64,
}
impl HistoricalPreparationProgressV1 {
    pub fn new(interval: Duration, output: impl Write + Send + 'static) -> Self {
        let now = Instant::now();
        let enabled = !interval.is_zero();
        Self {
            began: now,
            last_output: now,
            interval,
            output: enabled.then(|| Box::new(output) as Box<dyn Write + Send>),
            current: None,
            work: Work::default(),
            results: Default::default(),
            hz: enabled.then(process_clock_ticks_per_second_v1).flatten(),
            reader: ProcessCountersV1::read,
            sample_failures: 0,
        }
    }
    pub fn measurements(&self) -> HistoricalPreparationMeasurementsV1 {
        self.results.clone()
    }
    pub(crate) fn enabled(&self) -> bool {
        !self.interval.is_zero()
    }
    fn sample(&mut self) -> Option<ProcessCountersV1> {
        match (self.reader)() {
            Ok(v) => Some(v),
            Err(_) => {
                self.sample_failures += 1;
                None
            }
        }
    }
    pub(crate) fn stage(&mut self, stage: Stage) {
        if !self.enabled() {
            return;
        }
        if self.current.as_ref().is_some_and(|p| p.stage == stage) {
            return;
        }
        // 完成准入和初始化选择之后，原footer/已扫描选中文件提供可信物理总量。
        if stage == Stage::EpochPass {
            self.work.known_daily_raw_files_per_pass = Some(self.work.admission_files_completed);
            self.work.known_daily_raw_rows_per_pass = Some(self.work.admission_footer_rows);
            self.work.known_initialization_raw_rows_per_pass = Some(
                self.work
                    .initialization_selection
                    .initialization_physical_rows,
            );
        }
        if stage == Stage::PublicApplicationStatisticsRecording {
            // 六路原sort metrics给出实际Ingress总量，再加原有限End；不是全流程比例。
            self.work.known_total_public_inputs = self
                .work
                .source_sort_metrics
                .iter()
                .try_fold(1_u64, |n, m| Some(n + m.as_ref()?.input_rows));
        }
        let now = Instant::now();
        let after = self.sample();
        self.close_current(now, after.as_ref(), true);
        self.current = Some(Current {
            stage,
            began: now,
            before: after.clone(),
        });
        self.results.0.lock().unwrap().status = "running";
        self.emit(true, None, Some(&after));
    }
    fn close_current(&mut self, now: Instant, after: Option<&ProcessCountersV1>, completed: bool) {
        if let Some(current) = self.current.take() {
            let measurement = process_measurement_v1(
                current.before.as_ref(),
                after,
                self.hz,
                now.duration_since(current.began).as_secs_f64(),
            );
            let mut results = self.results.0.lock().unwrap();
            results.phases[current.stage.index()] = Some(
                json!({"stage":current.stage,"completed":completed,"measurement":measurement}),
            );
            results.work = json!(self.work);
            results.counter_sample_failures = self.sample_failures;
        }
    }
    pub(crate) fn finish(&mut self, success: bool) {
        if !self.enabled() {
            return;
        }
        let failed = self.current.as_ref().map(|p| p.stage).filter(|_| !success);
        let now = Instant::now();
        let after = self.sample();
        self.close_current(now, after.as_ref(), success);
        {
            let mut results = self.results.0.lock().unwrap();
            results.status = if success { "complete" } else { "error" };
            results.failed_stage = failed;
        }
        self.emit(
            true,
            Some(if success { "complete" } else { "error" }),
            Some(&after),
        );
    }
    pub(crate) fn applied(&mut self, applied: &AppliedIngressV1) {
        if !self.enabled() {
            return;
        }
        self.work.applied_inputs = applied.applied_prefix.next_input_position;
        self.work.generated_groups += applied.closed_groups.len() as u64;
        self.work.generated_observations += applied
            .closed_groups
            .iter()
            .map(|g| g.observations.len() as u64)
            .sum::<u64>();
        self.emit(false, None, None);
    }
    pub(crate) fn notice(&mut self, notice: Notice) {
        if !self.enabled() {
            return;
        }
        let checkpoint = match notice {
            Notice::Stage(stage) => {
                self.stage(stage);
                return;
            }
            Notice::AdmissionFile { bytes, rows } => {
                self.work.admission_files_completed += 1;
                self.work.admission_file_bytes += bytes;
                self.work.admission_footer_rows += rows;
                true
            }
            Notice::RawBatch {
                pass,
                initialization,
                rows,
            } => {
                let w = self.work.raw(pass);
                if initialization {
                    w.initialization_decoded_batches += 1;
                    w.initialization_physical_rows += rows;
                } else {
                    w.decoded_batches += 1;
                    w.physical_rows += rows;
                }
                true
            }
            Notice::RawFileComplete {
                pass,
                initialization,
            } => {
                let w = self.work.raw(pass);
                if initialization {
                    w.initialization_files_completed += 1;
                } else {
                    w.files_completed += 1;
                }
                true
            }
            Notice::SelectedRow {
                pass,
                initialization,
            } => {
                let w = self.work.raw(pass);
                if initialization {
                    w.initialization_selected_rows += 1;
                } else {
                    w.selected_rows += 1;
                }
                false
            }
            Notice::SourceSortFinished { index, metrics } => {
                self.work.source_sort_metrics[index] = Some(metrics);
                true
            }
            Notice::HiddenScanRow => {
                self.work.hidden_records_read += 1;
                false
            }
            Notice::HiddenMessageSupplied => {
                self.work.hidden_messages_supplied += 1;
                false
            }
            Notice::HiddenReferenceIndexed => {
                self.work.hidden_references_indexed += 1;
                false
            }
            Notice::HiddenMessageReduced => {
                self.work.hidden_messages_reduced += 1;
                false
            }
            Notice::HiddenStepWritten => {
                self.work.hidden_steps_written += 1;
                false
            }
            Notice::HiddenReportWritten => {
                self.work.hidden_reports_written += 1;
                false
            }
            Notice::HiddenStepIndexed => {
                self.work.hidden_steps_indexed += 1;
                false
            }
            Notice::HiddenMessagesSorted(metrics) => {
                self.work.hidden_message_sort = Some(metrics);
                true
            }
            Notice::HiddenIndexSorted(metrics) => {
                self.work.hidden_index_sort = Some(metrics);
                true
            }
            Notice::Checkpoint => true,
        };
        if checkpoint {
            self.emit(false, None, None);
        }
    }
    fn emit(
        &mut self,
        force: bool,
        terminal: Option<&'static str>,
        boundary_sample: Option<&Option<ProcessCountersV1>>,
    ) {
        if self.output.is_none() {
            return;
        }
        let now = Instant::now();
        if !force && now.duration_since(self.last_output) < self.interval {
            return;
        }
        self.last_output = now;
        // 检查点只有实际输出时采样。阶段边界的区间采样单独归属，不逐行采进程。
        let sample = boundary_sample.cloned().unwrap_or_else(|| self.sample());
        let snapshot = json!({"schema":"historical_preparation_progress_v1","stage":terminal.map(Value::from).unwrap_or_else(||self.current.as_ref().map(|p|json!(p.stage)).unwrap_or(Value::Null)),
            "elapsed_seconds":now.duration_since(self.began).as_secs_f64(),"work":self.work,"process_endpoint":sample,
            "published":terminal==Some("complete"),"known_total_inputs":self.work.known_total_public_inputs,
            "applied_input_fraction":self.work.known_total_public_inputs.map(|n|self.work.applied_inputs as f64/n as f64),"eta_seconds":null,
            "phase_coverage":"11 consecutive call intervals; internal lazy decode/serialize/push combined; no blocking-call heartbeat"});
        let output = self.output.as_mut().unwrap();
        if serde_json::to_writer(&mut **output, &snapshot)
            .map_err(std::io::Error::other)
            .and_then(|_| output.write_all(b"\n"))
            .is_err()
        {
            self.output = None;
        }
    }
    #[cfg(test)]
    pub(crate) fn set_reader(&mut self, reader: fn() -> Result<ProcessCountersV1, MaterialError>) {
        self.reader = reader;
    }
}

#[cfg(test)]
#[path = "preparation_progress_tests.rs"]
mod tests;
