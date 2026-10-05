//! 一次有限来源事实测量；输出逐来源标量增量，不选择健康阈值。
use contracts::*;
use data_pipeline_core::public_input::DptIngressV1;
use data_pipeline_training::{finite_source_progress::*, historical_preparation_progress::*};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[derive(Deserialize)]
struct Config {
    preparation: FiniteSourceProgressConfigV1,
    /// 0关闭周期输出；阶段切换始终输出。本例不估计未知总输入。
    progress_interval_seconds: u64,
}
#[derive(Clone, Serialize)]
struct Confirmation {
    input_position: u64,
    source_message_id: Sha256Hex,
    received_at_utc_ms: i64,
    source_event_at_utc_ms: Option<i64>,
    visible_frontier_utc_ms: i64,
}
#[derive(Serialize)]
struct SourceSummary {
    scope: SourceStreamScopeV1,
    changes: u64,
    valid_identity_changes: u64,
    first: Option<Confirmation>,
    last: Option<Confirmation>,
    #[serde(skip)]
    previous_valid_id: Option<Sha256Hex>,
}
fn proc_metrics() -> Value {
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    let stat = fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .map(|(_, s)| s.split_whitespace().collect())
        .unwrap_or_default();
    let memory = |name: &str| {
        status
            .lines()
            .find(|l| l.starts_with(name))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<u64>().ok())
    };
    let io: BTreeMap<_, _> = fs::read_to_string("/proc/self/io")
        .unwrap_or_default()
        .lines()
        .filter_map(|l| l.split_once(':'))
        .filter_map(|(k, v)| v.trim().parse::<u64>().ok().map(|v| (k.to_owned(), v)))
        .collect();
    json!({"user_cpu_ticks":fields.get(11).and_then(|v|v.parse::<u64>().ok()),"system_cpu_ticks":fields.get(12).and_then(|v|v.parse::<u64>().ok()),"rss_kib":memory("VmRSS:"),"peak_rss_kib":memory("VmHWM:"),"io":io})
}
fn logical_bytes(path: &Path) -> std::io::Result<u64> {
    let mut bytes = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let m = entry.metadata()?;
        bytes += if m.is_dir() {
            logical_bytes(&entry.path())?
        } else {
            m.len()
        };
    }
    Ok(bytes)
}
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args_os().skip(1);
    let config: Config = serde_json::from_slice(&fs::read(args.next().ok_or("CONFIG missing")?)?)?;
    let output = PathBuf::from(args.next().ok_or("OUTPUT missing")?);
    fs::create_dir(&output)?;
    let mut journal = BufWriter::new(File::create(output.join("source-changes.jsonl"))?);
    let mut bindings = BufWriter::new(File::create(output.join("market-bindings.jsonl"))?);
    let start = Instant::now();
    let interval = Duration::from_secs(config.progress_interval_seconds);
    let mut last_print = Instant::now();
    let mut phase = "initialization".to_owned();
    let mut stages = vec![json!({"stage":phase,"elapsed_seconds":0,"process":proc_metrics()})];
    let mut rows = BTreeMap::<String, u64>::new();
    let mut files = BTreeMap::<String, u64>::new();
    let mut selected = BTreeMap::<String, u64>::new();
    let mut sort_metrics = Vec::new();
    let total_main_files = config.preparation.files.len();
    let total_main_rows: u64 = config.preparation.files.iter().map(|f| f.rows).sum();
    let mut notice = |n| {
        let mut stage_changed = false;
        match n {
            HistoricalPreparationNoticeV1::Stage(s) => {
                phase = format!("{s:?}");
                stage_changed = true;
                stages.push(json!({"stage":phase,"elapsed_seconds":start.elapsed().as_secs_f64(),"process":proc_metrics(),"temporary_logical_bytes":logical_bytes(&config.preparation.work_root).ok()}));
            }
            HistoricalPreparationNoticeV1::RawBatch {
                pass,
                initialization,
                rows: n,
            } => {
                *rows
                    .entry(format!("{pass:?}/initialization={initialization}"))
                    .or_default() += n
            }
            HistoricalPreparationNoticeV1::RawFileComplete {
                pass,
                initialization,
            } => {
                *files
                    .entry(format!("{pass:?}/initialization={initialization}"))
                    .or_default() += 1
            }
            HistoricalPreparationNoticeV1::SelectedRow {
                pass,
                initialization,
            } => {
                *selected
                    .entry(format!("{pass:?}/initialization={initialization}"))
                    .or_default() += 1
            }
            HistoricalPreparationNoticeV1::SourceSortFinished { index, metrics } => {
                sort_metrics.push(json!({"index":index,"metrics":metrics}))
            }
            _ => {}
        }
        if stage_changed || (!interval.is_zero() && last_print.elapsed() >= interval) {
            eprintln!(
                "stage={phase} elapsed_s={:.1} raw_rows={rows:?} complete_files={files:?} selected_rows={selected:?} total_main_files_per_pass={total_main_files} total_main_rows_per_pass={total_main_rows} total_application_inputs=unknown",
                start.elapsed().as_secs_f64()
            );
            last_print = Instant::now();
        }
    };
    let notifier = HistoricalPreparationNotifierV1::new(&mut notice);
    let mut sources = BTreeMap::<SourceStreamScopeV1, usize>::new();
    let mut summaries = Vec::<SourceSummary>::new();
    let mut inputs = 0u64;
    let mut groups = 0u64;
    let mut changes = 0u64;
    let mut initialization_inputs = Vec::new();
    let mut initialization_source_message_ids = Vec::new();
    let mut now = i64::MIN;
    let mut normal_end = false;
    let mut last_application_print = Instant::now();
    let core = prepare_finite_source_progress_v1(
        &config.preparation,
        Some(&notifier),
        |input, applied, binding, _core| {
            inputs += 1;
            normal_end = matches!(&input.ingress, DptIngressV1::EndOfInput);
            if let Some(binding) = binding {
                initialization_inputs.push(input.input_position);
                initialization_source_message_ids.push(binding.source_message_id.clone());
            }
            for group in &applied.closed_groups {
                groups += 1;
                now = now.max(group.visible_at_utc_ms);
                for b in &group.market_bindings {
                    serde_json::to_writer(&mut bindings, b)?;
                    writeln!(bindings)?;
                }
                for p in &group.source_progress_changes {
                    changes += 1;
                    let next_id = sources.len();
                    let id = *sources.entry(p.scope.clone()).or_insert_with(|| {
                        summaries.push(SourceSummary {
                            scope: p.scope.clone(),
                            changes: 0,
                            valid_identity_changes: 0,
                            first: None,
                            last: None,
                            previous_valid_id: None,
                        });
                        next_id
                    });
                    let summary = &mut summaries[id];
                    summary.changes += 1;
                    let last_valid = p.last_valid_business_message.as_ref();
                    if last_valid.map(|v| &v.source_message_id)
                        != summary.previous_valid_id.as_ref()
                    {
                        if let Some(e) = last_valid {
                            summary.valid_identity_changes += 1;
                            let confirmation = Confirmation {
                                input_position: input.input_position,
                                source_message_id: e.source_message_id.clone(),
                                received_at_utc_ms: e.received_at_utc_ms,
                                source_event_at_utc_ms: e.source_event_at_utc_ms,
                                visible_frontier_utc_ms: now,
                            };
                            if summary.first.is_none() {
                                summary.first = Some(confirmation.clone());
                            }
                            summary.last = Some(confirmation);
                        }
                        summary.previous_valid_id = last_valid.map(|v| v.source_message_id.clone());
                    }
                    // 身份与未知值保留；只省去本研究不消费的证据正文/盘口。
                    let message = |e: &SourceEvidenceRefV3| {
                        json!([
                            e.source_message_id,
                            e.received_at_utc_ms,
                            e.source_event_at_utc_ms
                        ])
                    };
                    serde_json::to_writer(
                        &mut journal,
                        &json!([
                            id,
                            input.input_position,
                            group.group_position,
                            group.visible_at_utc_ms,
                            now,
                            last_valid.map(message),
                            message(&p.last_observed_message),
                            p.connection_generation,
                            p.synchronization,
                            p.quality
                        ]),
                    )?;
                    writeln!(journal)?;
                }
            }
            if !interval.is_zero() && last_application_print.elapsed() >= interval {
                eprintln!(
                    "stage=source_progress_application inputs={inputs} closed_groups={groups} changes={changes} elapsed_s={:.1} total_inputs=unknown",
                    start.elapsed().as_secs_f64()
                );
                last_application_print = Instant::now();
            }
            Ok(())
        },
    )?;
    journal.flush()?;
    bindings.flush()?;
    stages.push(json!({"stage":"finished","elapsed_seconds":start.elapsed().as_secs_f64(),"process":proc_metrics(),"temporary_logical_bytes":logical_bytes(&config.preparation.work_root)?}));
    let result = json!({"schema":"finite-source-progress-measurement-v1","elapsed_seconds":start.elapsed().as_secs_f64(),"inputs":inputs,"closed_groups":groups,"source_changes":changes,"normal_end":normal_end,"applied_prefix":core.applied_prefix(),"initialization_input_positions":initialization_inputs,"initialization_source_message_ids":initialization_source_message_ids,"core_object_counts":core.object_counts(),"sources":summaries,"stage_boundaries":stages,"raw_rows":rows,"raw_files":files,"selected_rows":selected,"sort_metrics":sort_metrics,"output_logical_bytes_before_summary":logical_bytes(&output)?});
    serde_json::to_writer_pretty(File::create(output.join("summary.json"))?, &result)?;
    eprintln!(
        "finished inputs={inputs} closed_groups={groups} changes={changes} elapsed_s={:.1}",
        start.elapsed().as_secs_f64()
    );
    Ok(())
}
