//! 显式历史材料的无模型读取入口；工具完成不表示窗口业务适用。
use crate::day_stream::DayIngressV1;
use crate::historical_initialization::{
    MetadataInitializationConfigV1, MetadataInitializationSelectionV1, existing,
    resolve_initialization_files,
};
use crate::public_input::open_finite_public_input_v1;
use crate::public_recording::RecordingResult;
use data_pipeline_core::public_input::DptIngressV1;
use data_pipeline_core::public_reference::source_evidence_v3_from_typed_raw_v2;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// 调用者声明的工具资源预算，不是材料业务合法性的定义。
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionLimits {
    pub batch_rows: usize,
    pub batch_bytes: usize,
    pub input_file_bytes: u64,
    pub input_rows: u64,
    pub output_bytes: u64,
    pub source_epochs: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionConfig {
    pub daily_manifests: Vec<PathBuf>,
    pub initialization: MetadataInitializationConfigV1,
    pub active_end_utc_ms: i64,
    pub limits: AdmissionLimits,
}

struct Report {
    file: File,
    bytes: u64,
    maximum: u64,
}
impl Report {
    fn row(&mut self, value: Value) -> RecordingResult<()> {
        // serde直接写入受限writer，不先物化可能超过报告预算的大字符串。
        serde_json::to_writer(&mut *self, &value)?;
        self.write_all(b"\n")?;
        Ok(())
    }
}
impl Write for Report {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if data.len() as u64 > self.maximum.saturating_sub(self.bytes) {
            return Err(std::io::Error::other("报告超过调用者output_bytes预算"));
        }
        let written = self.file.write(data)?;
        self.bytes += written as u64;
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

fn read_limited(path: &Path, max: u64) -> RecordingResult<Vec<u8>> {
    let maximum = max.checked_add(1).ok_or("读取预算溢出")?;
    let mut bytes = Vec::new();
    File::open(path)?.take(maximum).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max {
        return Err("JSON超过声明读取预算".into());
    }
    Ok(bytes)
}

fn add(total: &mut u64, value: u64, maximum: u64, what: &str) -> RecordingResult<()> {
    *total = total.checked_add(value).ok_or("资源计数溢出")?;
    if *total > maximum {
        return Err(format!("{what}超过调用者工具预算").into());
    }
    Ok(())
}

/// 同时显示原始清单字段与通过的正式准入，二者不是连续性结论。
fn manifest_facts(path: &Path, bytes: &[u8]) -> RecordingResult<Value> {
    let value: Value = serde_json::from_slice(bytes)?;
    Ok(json!({"kind":"manifest_declaration","path":path,
        "state":value.get("state"),"closure_reason":value.get("closure_reason"),
        "collection_day_coverage":value.get("collection_day_coverage"),
        "producer_barriers":value.get("producer_barriers"),
        "writer_finalize_acks":value.get("writer_finalize_acks"),
        "known_gaps":value.get("gaps"),
        "file_declarations":value.get("files"),
        "business_continuity":"Unknown"}))
}

fn metadata_fact(
    row: &data_pipeline_core::public_input::TypedRawRowV2,
    start: i64,
) -> RecordingResult<Option<Value>> {
    let Some(metadata) = existing(row) else {
        return Ok(None);
    };
    let source = source_evidence_v3_from_typed_raw_v2(row)?;
    Ok(Some(json!({"condition_id":metadata.condition_id,
        "row_kind":metadata.metadata_row_kind,"representation":"Existing",
        "source_message_id":source.source_message_id,
        "received_at_utc_ms":source.received_at_utc_ms,
        "collector_admission_time_utc_ms":metadata.collector_admission_time_utc_ms,
        "collector_admission_sequence":metadata.collector_admission_sequence,
        "market_start_utc":metadata.market_start_utc,"market_end_utc":metadata.market_end_utc,
        "before_start":source.received_at_utc_ms<start})))
}

#[derive(Default)]
struct WindowStats {
    rows: u64,
    first: Option<i64>,
    last: Option<i64>,
    previous: Option<i64>,
    largest_forward_interval_ms: i64,
    receive_regressions: u64,
    epochs: BTreeMap<String, (u64, u64)>,
    nonconsecutive_admission_steps: u64,
}
impl WindowStats {
    fn observe(&mut self, source: &contracts::SourceEvidenceRefV3) {
        let at = source.received_at_utc_ms;
        self.rows += 1;
        self.first = Some(self.first.map_or(at, |x| x.min(at)));
        self.last = Some(self.last.map_or(at, |x| x.max(at)));
        if let Some(previous) = self.previous {
            self.largest_forward_interval_ms = self
                .largest_forward_interval_ms
                .max(at.saturating_sub(previous));
            self.receive_regressions += u64::from(at < previous);
        }
        self.previous = Some(at);
        if let Some(admission) = &source.collector_admission {
            let sequence = admission.collector_admission_sequence;
            self.epochs
                .entry(admission.source_sequence_epoch_id.clone())
                .and_modify(|span| {
                    if span.1.checked_add(1) != Some(sequence) {
                        self.nonconsecutive_admission_steps += 1;
                    }
                    span.1 = sequence;
                })
                .or_insert((sequence, sequence));
        }
    }
    fn value(self, source: String) -> Value {
        json!({"kind":"window_source","source":source,"records":self.rows,
            "first_received_utc_ms":self.first,"last_received_utc_ms":self.last,
            "largest_forward_interval_ms":self.largest_forward_interval_ms,
            "receive_regressions":self.receive_regressions,"observed_epoch_sequence_spans":self.epochs,
            "observed_nonconsecutive_admission_steps":self.nonconsecutive_admission_steps,
            "interval_or_sequence_step_is_missing_business_data":"Unknown",
            "restart_cause":"Unknown","business_continuity":"Unknown"})
    }
}

fn execute(
    mode: &str,
    config: &AdmissionConfig,
    report: &mut Report,
    config_bytes: u64,
) -> RecordingResult<()> {
    let started = Instant::now();
    let limit = &config.limits;
    let start = config.initialization.received_start_utc_ms;
    let end = config.initialization.received_end_utc_ms;
    if !(start < end && start <= config.active_end_utc_ms && config.active_end_utc_ms <= end) {
        return Err("要求 S < H 且 S <= E <= H".into());
    }
    if limit.batch_rows == 0
        || limit.batch_bytes == 0
        || limit.input_rows == 0
        || limit.input_file_bytes == 0
        || limit.output_bytes == 0
        || limit.source_epochs == 0
    {
        return Err("工具资源预算必须为正".into());
    }
    if config.daily_manifests.is_empty() || config.initialization.files.is_empty() {
        return Err("必须显式声明原日清单和初始化文件".into());
    }
    if mode == "inspect" && !config.initialization.markets.is_empty() {
        return Err("inspect不消费选择；markets应为空，冻结选择后使用verify".into());
    }
    if mode == "verify" && config.initialization.markets.is_empty() {
        return Err("verify需要调用者显式冻结markets选择".into());
    }
    report.row(
        json!({"kind":"start","version":"dpt-historical-admission-v1","mode":mode,
        "S":start,"E":config.active_end_utc_ms,"H":end,"purpose":config.initialization.purpose,
        "business_continuity":"Unknown","input_config_bytes":config_bytes,"limits":limit}),
    )?;
    let mut manifest_bytes = 0u64;
    let mut declared_bytes = 0u64;
    let mut declared_rows = 0u64;
    let mut manifest_identities = BTreeMap::new();
    // 原日准入将验证全部列出文件。预算在调用该入口前按全部文件检查。
    for path in &config.daily_manifests {
        let bytes = read_limited(path, crate::DAILY_MANIFEST_MAX_BYTES as u64)?;
        manifest_bytes += bytes.len() as u64;
        let digest = format!("{:x}", Sha256::digest(&bytes));
        if manifest_identities
            .insert(path.clone(), digest.clone())
            .is_some_and(|old| old != digest)
        {
            return Err("同一清单在预算读取期间发生变化".into());
        }
        let value: Value = serde_json::from_slice(&bytes)?;
        for item in value
            .get("files")
            .and_then(Value::as_array)
            .ok_or("原清单缺files")?
        {
            let size = item
                .get("bytes")
                .and_then(Value::as_u64)
                .ok_or("文件bytes不是u64")?;
            let rows = item
                .get("rows")
                .and_then(Value::as_u64)
                .ok_or("文件rows不是u64")?;
            let relative = item
                .get("path")
                .and_then(Value::as_str)
                .ok_or("文件path不是字符串")?;
            let metadata =
                std::fs::metadata(path.parent().unwrap_or(Path::new(".")).join(relative))?;
            if !metadata.is_file() || metadata.len() != size {
                return Err("准入前文件类型或大小不符".into());
            }
            add(
                &mut declared_bytes,
                size,
                limit.input_file_bytes,
                "声明文件bytes",
            )?;
            add(&mut declared_rows, rows, limit.input_rows, "声明文件rows")?;
        }
        report.row(manifest_facts(path, &bytes)?)?;
    }
    let initialization_files = resolve_initialization_files(&config.initialization.files)?;
    let mut initialization_bytes = 0;
    let mut initialization_rows = 0;
    for file in &initialization_files {
        let metadata = std::fs::metadata(&file.file.path)?;
        if !metadata.is_file() || metadata.len() != file.file.bytes {
            return Err("初始化文件类型或大小不符".into());
        }
        add(
            &mut declared_bytes,
            file.file.bytes,
            limit.input_file_bytes,
            "声明文件bytes（含初始化遍历）",
        )?;
        add(
            &mut declared_rows,
            file.file.rows,
            limit.input_rows,
            "声明文件rows（含初始化遍历）",
        )?;
        initialization_bytes += file.file.bytes;
        initialization_rows += file.file.rows;
        report.row(json!({"kind":"initialization_file","facts":file}))?;
    }
    let mut admitted_files = 0u64;
    let mut admission_notice = |notice| {
        if let crate::historical_preparation_progress::HistoricalPreparationNoticeV1::AdmissionFile {..}=notice {
            admitted_files+=1;
            if admitted_files==1 || admitted_files%32==0 { eprintln!("日文件准入：已完成{admitted_files}个文件"); }
        }
    };
    let notifier = crate::historical_preparation_progress::HistoricalPreparationNotifierV1::new(
        &mut admission_notice,
    );
    let frozen = crate::freeze_public_source_input_set_with_progress_v1(
        &config.daily_manifests,
        Some(&notifier),
    )?;
    for day in frozen.admitted_daily_inputs() {
        if manifest_identities
            .get(day.manifest_path())
            .map(String::as_str)
            != Some(day.manifest_sha256())
        {
            return Err("正式日准入清单与预算声明读取身份不同".into());
        }
    }
    eprintln!("日文件准入完成；开始初始化{mode}");
    report.row(
        json!({"kind":"daily_admission","file_identity_footer_admitted":true,
        "input_set_identity_sha256":frozen.input_set_identity_sha256(),
        "input_manifest_set_sha256":frozen.input_manifest_set_sha256(),
        "all_listed_daily_files_admitted":true,"business_continuity":"Unknown"}),
    )?;
    let mut decoded_initialization = 0u64;
    let mut selected = 0u64;
    if mode == "inspect" {
        for file in &initialization_files {
            for batch in
                open_finite_public_input_v1(&file.file, limit.batch_rows, limit.batch_bytes)?
            {
                for row in batch? {
                    decoded_initialization += 1;
                    if decoded_initialization > initialization_rows {
                        return Err("初始化实际行数超出冻结声明".into());
                    }
                    if let Some(fact) = metadata_fact(&row, start)? {
                        report.row(
                            json!({"kind":"metadata_candidate","file":file.relative_path,
                            "manifest_sha256":file.manifest_sha256,"fact":fact}),
                        )?;
                    }
                }
            }
        }
    } else {
        // 同一解析事实进入原选择内核；不是新的选择实现。
        let mut observe = |notice| {
            if let crate::historical_preparation_progress::HistoricalPreparationNoticeV1::RawBatch { rows,.. } = notice {
                decoded_initialization += rows;
            }
        };
        let notifier = crate::historical_preparation_progress::HistoricalPreparationNotifierV1::new(
            &mut observe,
        );
        let selection = MetadataInitializationSelectionV1::open_from_files(
            &config.initialization,
            initialization_files,
            limit.batch_rows,
            limit.batch_bytes,
            Some(&notifier),
        )?;
        for entry in selection.read(limit.batch_rows, limit.batch_bytes) {
            let (row, binding) = entry?;
            selected += 1;
            report.row(json!({"kind":"selected_initialization","binding":binding,
                "fact":metadata_fact(&row,start)?}))?;
        }
        report.row(json!({"kind":"selection_verified","source_identity_sha256":selection.source_identity(
            frozen.input_set_identity_sha256(),&config.initialization)?,"selected_rows":selected,
            "excluded_input_rows":initialization_rows.saturating_sub(selected),
            "exclusion_basis":"原选择器的Existing/condition/Snapshot起点/pre-S条件；非单一排除原因"}))?;
    }
    let files = frozen
        .admitted_daily_inputs()
        .iter()
        .flat_map(|day| day.files())
        .collect::<Vec<_>>();
    let scanned = AtomicU64::new(0);
    let control = |count: u64| -> RecordingResult<()> {
        let total = scanned
            .fetch_add(count, Ordering::Relaxed)
            .checked_add(count)
            .ok_or("扫描计数溢出")?;
        if total > limit.input_rows.saturating_sub(initialization_rows) {
            return Err("窗口顺序扫描超过input_rows".into());
        }
        Ok(())
    };
    eprintln!("初始化阶段完成；开始原窗口窄列与载荷读取");
    let mut ingress = DayIngressV1::open_with_control(
        &files,
        "historical-admission-inspection".into(),
        limit.batch_rows,
        limit.batch_bytes,
        start..end,
        Vec::new(),
        limit.source_epochs,
        Some(&control),
    )?;
    let mut summaries = BTreeMap::<String, WindowStats>::new();
    let mut window_rows = 0u64;
    while let Some(input) = ingress.next_input()? {
        if let DptIngressV1::Message {
            source_evidence, ..
        } = input.ingress
        {
            window_rows += 1;
            if window_rows > limit.input_rows {
                return Err("窗口返回行超过input_rows".into());
            }
            let source = serde_json::to_value(&source_evidence.source_kind)?;
            let source = source.as_str().ok_or("来源枚举没有字符串表示")?.to_owned();
            summaries
                .entry(source)
                .or_default()
                .observe(&source_evidence);
        }
    }
    for (source, stats) in summaries {
        report.row(stats.value(source))?;
    }
    report.row(json!({"kind":"complete","complete":true,"mode":mode,
        "selected_rows":selected,"initialization_rows_decoded":decoded_initialization,"window_rows":window_rows,
        "window_narrow_rows_scanned":scanned.load(Ordering::Relaxed),
        "logical_work":{"config_bytes":config_bytes,"manifest_bytes_read_by_report":manifest_bytes,
            "declared_file_bytes_including_initialization_pass":declared_bytes,
            "initialization_file_bytes":initialization_bytes,
            "daily_admission":"全部声明文件内容哈希与footer校验；随后窗口窄列扫描及窗口载荷解码",
            "initialization":"单独身份校验与正式解码；与日文件重叠时仍单独计入声明遍历",
            "exact_kernel_read_bytes":"Unknown","physical_io":"Unknown"},
        "elapsed_seconds":started.elapsed().as_secs_f64(),"report_bytes_before_complete":report.bytes,
        "business_continuity":"Unknown","training_readiness":"Unknown"}))?;
    report.flush()?;
    report.file.sync_all()?;
    Ok(())
}

/// 仅命令行显式调用；不运行模型、规则查询或回放归约。
pub fn run_cli(args: impl IntoIterator<Item = String>) -> RecordingResult<()> {
    let args = args.into_iter().collect::<Vec<_>>();
    if args.len() != 5
        || !matches!(args[0].as_str(), "inspect" | "verify")
        || args[3] != "--config-bytes"
    {
        return Err("用法: dpt-historical-admission inspect|verify <config.json> <新report.jsonl> --config-bytes <上限>".into());
    }
    let cap: u64 = args[4].parse()?;
    let bytes = read_limited(Path::new(&args[1]), cap)?;
    let config: AdmissionConfig = serde_json::from_slice(&bytes)?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args[2])?;
    let mut report = Report {
        file,
        bytes: 0,
        maximum: config.limits.output_bytes,
    };
    let result = execute(&args[0], &config, &mut report, bytes.len() as u64);
    if let Err(error) = &result {
        // 字节超界时也不放宽预算；未完成文件留作失败事实。
        let _ = report.row(json!({"kind":"error","complete":false,"error":error.to_string()}));
        let _ = report.flush();
    }
    result
}
