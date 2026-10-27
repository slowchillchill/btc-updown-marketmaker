//! #181有限首差诊断；没有SIM、学习事务或可写checkpoint入口。
use crate::{
    ConsumerConfig, ConsumerRunConfig, learning::compose_trajectory_frame, worker::WorkerConfig,
};
use backtest::*;
use contracts::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, ChildStdin, Command, Stdio},
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticFile {
    pub path: PathBuf,
    pub sha256: String,
}
impl DiagnosticFile {
    fn read(&self) -> Result<Vec<u8>, MaterialError> {
        let bytes = fs::read(&self.path)?;
        if format!("{:x}", Sha256::digest(&bytes)) != self.sha256 {
            return Err(format!("原产物身份不符: {}", self.path.display()).into());
        }
        Ok(bytes)
    }
}
/// 旧反例对照保留；修后重验明确进入生产学习再生路径，不重标旧事务。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticInputPair {
    #[default]
    LegacyOffOn,
    CurrentConsumerReplay,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticConfig {
    #[serde(default)]
    pub input_pair: DiagnosticInputPair,
    pub completed_collection: DiagnosticFile,
    pub trajectory: DiagnosticFile,
    pub worker: WorkerConfig,
    pub initial_model_sha256: String,
    pub day_index: usize,
    pub candidate_index: usize,
    pub through_call: usize,
    pub max_public_streams: usize,
    pub max_forwards: usize,
    pub max_wall_seconds: u64,
    pub progress_interval_ms: u64,
    pub stop_file: PathBuf,
    pub output_directory: PathBuf,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticMode {
    Identities,
    Inputs,
    Model,
}
struct Budget<'a> {
    config: &'a DiagnosticConfig,
    started: Instant,
}
impl Budget<'_> {
    fn stopped(&self) -> bool {
        self.config.stop_file.exists()
            || self.started.elapsed() >= Duration::from_secs(self.config.max_wall_seconds)
    }
    fn check(&self) -> Result<(), MaterialError> {
        if self.stopped() {
            Err("诊断已停止，保留未完成状态；不重试".into())
        } else {
            Ok(())
        }
    }
}
/// 按结构找到首差，保留真实两值；不使用数值容差或摘要代替值。
pub fn first_difference(left: &Value, right: &Value, path: &str) -> Option<Value> {
    if left == right {
        return None;
    }
    match (left, right) {
        (Value::Object(a), Value::Object(b)) => {
            for k in a.keys().chain(b.keys()) {
                if a.get(k) != b.get(k) {
                    if a.get(k).is_none() || b.get(k).is_none() {
                        return Some(
                            json!({"path":format!("{path}/{k}"),"left":a.get(k),"right":b.get(k),"left_present":a.contains_key(k),"right_present":b.contains_key(k)}),
                        );
                    }
                    return first_difference(&a[k], &b[k], &format!("{path}/{k}"));
                }
            }
        }
        (Value::Array(a), Value::Array(b)) if a.len() == b.len() => {
            for (i, (a, b)) in a.iter().zip(b).enumerate() {
                if let Some(d) = first_difference(a, b, &format!("{path}/{i}")) {
                    return Some(d);
                }
            }
        }
        _ => {}
    }
    Some(json!({"path":path,"left":left,"right":right}))
}
fn save(path: &std::path::Path, value: &Value) -> Result<(), MaterialError> {
    let temporary = path.with_extension("incomplete.json");
    fs::write(&temporary, serde_json::to_vec_pretty(value)?)?;
    fs::rename(temporary, path)?;
    Ok(())
}
fn prefix(bytes: &[u8], count: usize) -> Result<Vec<Value>, MaterialError> {
    let mut rows = Vec::new();
    for line in bytes.split(|b| *b == b'\n').filter(|v| !v.is_empty()) {
        let row: Value = serde_json::from_slice(line)?;
        if row.get("call").is_none() {
            continue;
        }
        if row["call"].as_u64() != Some(rows.len() as u64 + 1) {
            return Err("诊断必须从合法call1顺序开始".into());
        }
        if row["options"]
            .as_array()
            .and_then(|v| v.get(row["selected_option"].as_u64()? as usize))
            .map(|v| &v["kind"])
            != Some(&json!("NoTrade"))
            || row["rpc"].as_array().is_none_or(|v| !v.is_empty())
        {
            return Err("本段仅支持冻结NoTrade且无条件RPC；其他动作未知".into());
        }
        rows.push(row);
        if rows.len() == count {
            break;
        }
    }
    if rows.len() != count {
        return Err("原轨迹缺少声明前缀".into());
    }
    Ok(rows)
}
fn rebuild(
    config: &DiagnosticConfig,
    consumer: &ConsumerRunConfig,
    rows: &[Value],
    rules: Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
    learning_side: bool,
    budget: &Budget<'_>,
    report: &mut Value,
) -> Result<Vec<Value>, MaterialError> {
    budget.check()?;
    let production_replay =
        learning_side && config.input_pair == DiagnosticInputPair::CurrentConsumerReplay;
    let source_progress = if config.input_pair == DiagnosticInputPair::LegacyOffOn {
        learning_side
    } else {
        crate::public_input::SOURCE_PROGRESS
    };
    let day = consumer.days.get(config.day_index).ok_or("日索引不存在")?;
    let candidate = day
        .candidates
        .get(config.candidate_index)
        .ok_or("账户索引不存在")?;
    let binding = consumer
        .rules
        .frozen()
        .ok_or("诊断只接受原冻结RECOVERY规则")?;
    let queries = rows
        .iter()
        .map(|r| {
            r["market_frame"]["market_cutoff"]["decision_time"]["logical_time_utc_ms"]
                .as_i64()
                .ok_or("原市场截止缺失")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let public = ContinuousPublicExportConfigV1 {
        rules: binding.public_rules(),
        preparation: day.preparation.clone(),
        markets: PublicObservationMarketsV1::All,
        statistics: candidate
            .public_statistics
            .clone()
            .ok_or("缺公共统计配置")?,
        public_delivery_delay_ms: candidate.timing.public_delivery_delay_ms.clone(),
        observe_after_groups: false,
        query_times_utc_ms: queries.clone(),
        export_new_trades: false,
    };
    let mut output = vec![None; rows.len()];
    let prefix_complete = std::sync::atomic::AtomicBool::new(false);
    let stream_index = report["public_streams"]
        .as_array()
        .ok_or("公共流计数缺失")?
        .len();
    if stream_index >= config.max_public_streams {
        return Err("公共流额度耗尽".into());
    }
    report["public_streams"]
        .as_array_mut()
        .unwrap()
        .push(json!({"source_progress":source_progress,"observation":crate::public_input::OBSERVATION,"production_learning_replay":production_replay,"status":"started"}));
    save(&config.output_directory.join("progress.json"), report)?;
    let progress_at = std::sync::Mutex::new(Instant::now());
    let mut deliver = |at,
                       group: &PublicGroupReferenceV1,
                       frames: &[shared_run::FrozenMarketReadV1]| {
        budget.check()?;
        for (i, row) in rows.iter().enumerate().filter(|(i, _)| queries[*i] == at) {
            let expected = &row["market_frame"]["market_cutoff"]["receipt"]["group"];
            let actual = serde_json::to_value(group)?;
            let (frame, projection) =
                compose_trajectory_frame(row, frames, &config.worker.encoding, true)?;
            output[i] = Some(
                json!({"call":row["call"],"expected_group":expected,"actual_group":actual,"group_matches":expected==&actual,"projection":projection,"frame":frame}),
            );
        }
        if output.iter().all(Option::is_some) {
            prefix_complete.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    };
    let notify = |p: &ContinuousPublicExportProgressV1| {
        let mut previous = progress_at.lock().unwrap();
        if previous.elapsed() >= Duration::from_millis(config.progress_interval_ms) {
            eprintln!(
                "诊断公共流 source_progress={source_progress} 输入={} 组={} 总耗时={:.3}s",
                p.processed_inputs,
                p.applied_groups,
                budget.started.elapsed().as_secs_f64()
            );
            let _ = save(
                &config.output_directory.join("public-progress.json"),
                &json!({"stream":stream_index,"source_progress":source_progress,"progress":p,"elapsed_seconds":budget.started.elapsed().as_secs_f64()}),
            );
            *previous = Instant::now();
        }
    };
    let stop = || budget.stopped() || prefix_complete.load(std::sync::atomic::Ordering::Relaxed);
    let state = if production_replay {
        let mut day = day.clone();
        day.candidates = vec![candidate.clone()];
        let account: crate::ConsumerConfig = serde_json::from_value(
            candidate.strategy.effective_parameters["model_consumer"].clone(),
        )?;
        let queries = [(account.account_key, queries.iter().copied().collect())]
            .into_iter()
            .collect();
        let last = std::sync::Mutex::new(ContinuousPublicExportProgressV1::default());
        crate::learning::visit_day_frames_controlled(
            &binding.public_rules(),
            Some(rules),
            &day,
            &queries,
            |_, at, group, frames| deliver(at, group, frames),
            stop,
            |p| {
                *last.lock().unwrap() = p.clone();
                notify(p);
            },
        )?;
        last.into_inner().unwrap()
    } else {
        visit_continuous_public_frames_with_requirements_v1(
            &public,
            rules,
            crate::public_input::OBSERVATION,
            source_progress,
            |at, g, f| deliver(at, g, &f),
            notify,
            stop,
        )?
    };
    let complete = prefix_complete.load(std::sync::atomic::Ordering::Relaxed);
    report["public_streams"][stream_index] = json!({"source_progress":source_progress,"observation":crate::public_input::OBSERVATION,"production_learning_replay":production_replay,"state":state,"planned_prefix_complete":complete});
    if budget.stopped() || (state.stopped && !complete) {
        return Err("公共再生停止，未完成必要查询；不运行模型".into());
    }
    output
        .into_iter()
        .map(|v| v.ok_or_else(|| "公共再生缺原查询帧".into()))
        .collect()
}
struct Protocol {
    child: Child,
    input: ChildStdin,
    responses: mpsc::Receiver<Result<(Value, usize), String>>,
    sent: usize,
    received: usize,
}
impl Drop for Protocol {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Protocol {
    fn start(c: &WorkerConfig) -> Result<Self, MaterialError> {
        let mut child = Command::new(&c.python)
            .arg(&c.script)
            .arg("serve")
            .arg(&c.model)
            .arg(&c.device)
            .arg(c.torch_threads.to_string())
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let input = child.stdin.take().ok_or("诊断stdin缺失")?;
        let stdout = child.stdout.take().ok_or("诊断stdout缺失")?;
        let (tx, responses) = mpsc::channel();
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                let mut line = String::new();
                let result = match reader.read_line(&mut line) {
                    Ok(0) => Err("诊断进程EOF".to_owned()),
                    Ok(_) => serde_json::from_str(&line)
                        .map(|v| (v, line.len()))
                        .map_err(|e| e.to_string()),
                    Err(e) => Err(e.to_string()),
                };
                let stop = result.is_err();
                if tx.send(result).is_err() || stop {
                    break;
                }
            }
        });
        Ok(Self {
            child,
            input,
            responses,
            sent: 0,
            received: 0,
        })
    }
    fn request(&mut self, v: &Value, budget: &Budget<'_>) -> Result<Value, MaterialError> {
        budget.check()?;
        let mut bytes = serde_json::to_vec(v)?;
        bytes.push(b'\n');
        self.input.write_all(&bytes)?;
        self.input.flush()?;
        self.sent += bytes.len();
        loop {
            budget.check()?;
            match self.responses.recv_timeout(Duration::from_millis(100)) {
                Ok(r) => {
                    let (v, n) = r.map_err(|e| format!("诊断协议: {e}"))?;
                    self.received += n;
                    if v.get("error").is_some() {
                        return Err(format!("诊断失败，不重试: {v}").into());
                    }
                    return Ok(v);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
}
/// 私有轨迹和相同token组件在成对分支间共享，协议只在当前调用边界序列化。
#[derive(Clone)]
struct InputFrame {
    private: Arc<Value>,
    global: Arc<Value>,
    markets: Vec<Arc<Value>>,
}
impl InputFrame {
    fn payload(&self) -> Value {
        let mut v = (*self.private).clone();
        v["global"] = (*self.global).clone();
        v["markets"] = json!(self.markets);
        v
    }
}
fn pair_inputs(
    row: &Value,
    a: &Value,
    b: &Value,
) -> Result<(InputFrame, InputFrame, usize, usize), MaterialError> {
    let private = Arc::new(row.clone());
    let global = Arc::new(a["global"].clone());
    let mut bytes = serde_json::to_vec(&*private)?.len() + serde_json::to_vec(&*global)?.len();
    let mut shared = 1;
    let global_b = if a["global"] == b["global"] {
        shared += 1;
        global.clone()
    } else {
        bytes += serde_json::to_vec(&b["global"])?.len();
        Arc::new(b["global"].clone())
    };
    let left = a["markets"].as_array().ok_or("缺模型token市场")?;
    let right = b["markets"].as_array().ok_or("缺模型token市场")?;
    if left.len() != right.len() {
        return Err("市场顺序长度不同，不建立模型分支".into());
    }
    let mut markets = Vec::new();
    let mut markets_b = Vec::new();
    for (a, b) in left.iter().zip(right) {
        let a = Arc::new(a.clone());
        bytes += serde_json::to_vec(&*a)?.len();
        let b = if *a == *b {
            shared += 1;
            a.clone()
        } else {
            bytes += serde_json::to_vec(b)?.len();
            Arc::new(b.clone())
        };
        markets.push(a);
        markets_b.push(b);
    }
    Ok((
        InputFrame {
            private: private.clone(),
            global,
            markets,
        },
        InputFrame {
            private,
            global: global_b,
            markets: markets_b,
        },
        bytes,
        shared,
    ))
}
fn model_branches(
    config: &DiagnosticConfig,
    left: &[InputFrame],
    right: &[InputFrame],
    inputs_differ: bool,
    budget: &Budget<'_>,
    report: &mut Value,
) -> Result<(), MaterialError> {
    let mut protocol = Protocol::start(&config.worker)?;
    let ready=protocol.request(&json!({"phase":"hello","model_sha256":config.initial_model_sha256,"encoding":config.worker.encoding}),budget)?;
    if ready["ready"] != true {
        return Err("诊断worker未就绪".into());
    }
    report["worker_identity"] = ready.clone();
    if let Some(first) = left.first() {
        let expected = &first.private["behavior"];
        if expected.is_object() {
            for key in [
                "model_sha256",
                "version",
                "torch",
                "device",
                "config",
                "numerical_runtime",
            ] {
                if expected[key] != ready["identity"][key] {
                    return Err(format!("诊断模型运行身份不符: {key}").into());
                }
            }
        }
    }
    let mut original_results = BTreeMap::<usize, Value>::new();
    for (branch, frames) in [
        ("inference", left),
        ("teacher", right),
        ("common_teacher", left),
    ] {
        if branch == "common_teacher" && !inputs_differ {
            continue;
        }
        for (i, input) in frames.iter().enumerate() {
            let frame = input.payload();
            budget.check()?;
            let n = report["forwards_started"].as_u64().unwrap_or(0);
            if n >= config.max_forwards as u64 {
                return Err("诊断前向额度耗尽；不重试".into());
            }
            report["forwards_started"] = json!(n + 1);
            report["in_flight"] = json!({"branch":branch,"call":i+1});
            save(&config.output_directory.join("progress.json"), report)?;
            let response=protocol.request(&json!({"phase":"diagnostic_step","branch":branch,"frame":frame,"seed":frame["sampling_seed"]}),budget)?;
            if response["branch"] != branch || response["call"] != frame["call"] {
                return Err("诊断响应身份不符".into());
            }
            report["forwards_completed"] =
                json!(report["forwards_completed"].as_u64().unwrap_or(0) + 1);
            report["in_flight"] = Value::Null;
            if branch == "inference" {
                original_results.insert(i, response.clone());
            } else if report["first_model_differences"][branch].is_null()
                && let Some(d) =
                    first_difference(&original_results[&i]["trace"], &response["trace"], "trace")
            {
                let difference = json!({"branch":branch,"call":i+1,"difference":d});
                report["first_model_differences"][branch] = difference.clone();
                if report["first_model_difference"].is_null() {
                    report["first_model_difference"] = difference;
                }
            }
            if i + 1 >= 24 {
                report["model_evidence"]
                    .as_array_mut()
                    .unwrap()
                    .push(response);
            }
            eprintln!(
                "诊断模型 {branch} call={} 前向已发起={} 总耗时={:.3}s",
                i + 1,
                n + 1,
                budget.started.elapsed().as_secs_f64()
            );
        }
    }
    report["model_metrics"] = protocol.request(&json!({"phase":"metrics"}), budget)?;
    report["ipc"] = json!({"request_bytes":protocol.sent,"response_bytes":protocol.received});
    Ok(())
}
pub fn run_diagnostic(
    config: &DiagnosticConfig,
    mode: DiagnosticMode,
) -> Result<Value, MaterialError> {
    if config.through_call == 0
        || config.through_call > 26
        || config.max_public_streams != 2
        || config.max_forwards > 78
        || config.max_wall_seconds == 0
        || config.progress_interval_ms == 0
    {
        return Err("诊断要求call1至最多26、两流、最多78前向及明确监督上限".into());
    }
    fs::create_dir(&config.output_directory)?;
    let budget = Budget {
        config,
        started: Instant::now(),
    };
    let mut report = json!({"version":"b-prefix-diagnostic-v1","status":"unfinished","config":config,"forwards_started":0,"forwards_completed":0,"backwards":0,"optimizer_steps":0,"commits":0,"public_streams":[],"first_input_difference":null,"first_projection_difference":null,"first_token_difference":null,"first_group_difference":null,"first_original_group_mismatch":null,"first_model_difference":null,"input_evidence":[],"model_evidence":[]});
    save(&config.output_directory.join("progress.json"), &report)?;
    let before = ProcessCountersV1::read().ok();
    let result = (|| -> Result<(), MaterialError> {
        budget.check()?;
        let collection: Value = serde_json::from_slice(&config.completed_collection.read()?)?;
        let consumer: ConsumerRunConfig = serde_json::from_value(collection["consumer"].clone())?;
        if consumer.rules.frozen().is_none() {
            return Err("诊断只读原冻结规则，不允许查询或历史替代".into());
        }
        let rules = crate::open_consumer_rules(&consumer)?.rules;
        let bytes = config.trajectory.read()?;
        let rows = prefix(&bytes, config.through_call)?;
        let day = consumer.days.get(config.day_index).ok_or("日索引不存在")?;
        let candidate = day
            .candidates
            .get(config.candidate_index)
            .ok_or("账户索引不存在")?;
        let account: ConsumerConfig = serde_json::from_value(
            candidate.strategy.effective_parameters["model_consumer"].clone(),
        )?;
        let original_path = account
            .learning
            .as_ref()
            .ok_or("原完成采集缺轨迹身份")?
            .trajectory_path
            .clone();
        if original_path != config.trajectory.path
            || rows.iter().any(|r| {
                r["account"] != account.account_key
                    || r["behavior"]["model_sha256"] != config.initial_model_sha256
                    || r["rules_identity"]
                        != consumer.rules.frozen().unwrap().expected_file_sha256.0
            })
        {
            return Err("原轨迹/账户/模型/规则身份不符".into());
        }
        if serde_json::to_value(&config.worker.encoding)?
            != serde_json::to_value(&consumer.worker.encoding)?
        {
            return Err("诊断编码参数与原消费者不同".into());
        }
        let model = DiagnosticFile {
            path: config.worker.model.clone(),
            sha256: config.initial_model_sha256.clone(),
        };
        let _ = model.read()?;
        report["identities"] = json!({"binding":consumer.rules.frozen(),"behavior":rows[0]["behavior"],"account":account,"original_binding":collection["binding"]});
        if mode == DiagnosticMode::Identities {
            return Ok(());
        }
        let left = rebuild(
            config,
            &consumer,
            &rows,
            rules.clone(),
            false,
            &budget,
            &mut report,
        )?;
        let right = rebuild(config, &consumer, &rows, rules, true, &budget, &mut report)?;
        let (left_label, right_label) = match config.input_pair {
            DiagnosticInputPair::LegacyOffOn => ("source_progress_off", "source_progress_on"),
            DiagnosticInputPair::CurrentConsumerReplay => {
                ("current_collection_off", "production_learning_replay_off")
            }
        };
        let mut shared = Vec::new();
        let mut alternate = Vec::new();
        let mut input_bytes = 0usize;
        let mut shared_components = 0;
        let mut valid = true;
        for (i, (a, b)) in left.into_iter().zip(right).enumerate() {
            valid &= a["group_matches"] == true && b["group_matches"] == true;
            if report["first_original_group_mismatch"].is_null()
                && (a["group_matches"] != true || b["group_matches"] != true)
            {
                let mut mismatch = json!({"call":i+1,"original":a["expected_group"]});
                mismatch[left_label] = a["actual_group"].clone();
                mismatch[right_label] = b["actual_group"].clone();
                report["first_original_group_mismatch"] = mismatch;
            }
            if report["first_input_difference"].is_null()
                && let Some(d) = first_difference(&a, &b, "input")
            {
                report["first_input_difference"] = json!({"call":i+1,"difference":d});
            }
            for (field, a, b) in [
                (
                    "first_group_difference",
                    a["actual_group"].clone(),
                    b["actual_group"].clone(),
                ),
                (
                    "first_projection_difference",
                    a["projection"].clone(),
                    b["projection"].clone(),
                ),
                (
                    "first_token_difference",
                    json!({"global":a["frame"]["global"],"markets":a["frame"]["markets"]}),
                    json!({"global":b["frame"]["global"],"markets":b["frame"]["markets"]}),
                ),
            ] {
                if report[field].is_null()
                    && let Some(d) = first_difference(&a, &b, field)
                {
                    report[field] = json!({"call":i+1,"difference":d});
                }
            }
            if i + 1 >= 24 {
                let mut evidence = json!({"call":i+1});
                evidence[left_label] = a.clone();
                evidence[right_label] = b.clone();
                report["input_evidence"]
                    .as_array_mut()
                    .unwrap()
                    .push(evidence);
            }
            let mut row = rows[i].clone();
            row["sampling_seed"] = json!(account.sampling_seed);
            let (x, y, bytes, components) = pair_inputs(&row, &a["frame"], &b["frame"])?;
            input_bytes += bytes;
            shared_components += components;
            shared.push(x);
            alternate.push(y);
        }
        report["model_public_input"] = json!({"contract":crate::public_input::contract(),"mode":config.input_pair,"legacy_reference":"原v1轨迹只读反例，不是v2学习事务恢复"});
        report["input_memory"] = json!({"unique_serialized_bytes":input_bytes,"shared_components":shared_components,"scope":"必要Value结构驻留；字节为唯一payload的JSON大小，不等于allocator RSS"});
        if !valid {
            report["status"] = json!("input_identity_mismatch_model_unknown");
            return Ok(());
        }
        if config.input_pair == DiagnosticInputPair::CurrentConsumerReplay
            && (!report["first_projection_difference"].is_null()
                || !report["first_token_difference"].is_null())
        {
            report["status"] = json!("production_input_mismatch_model_unknown");
            return Ok(());
        }
        if mode == DiagnosticMode::Model {
            let differs = config.input_pair == DiagnosticInputPair::LegacyOffOn
                && shared
                    .iter()
                    .zip(&alternate)
                    .any(|(a, b)| a.global != b.global || a.markets != b.markets);
            model_branches(config, &shared, &alternate, differs, &budget, &mut report)?;
        }
        Ok(())
    })();
    report["elapsed_seconds"] = json!(budget.started.elapsed().as_secs_f64());
    report["process_counters"] = json!({"before":before,"after":ProcessCountersV1::read().ok(),"scope":"Rust父进程；Python RSS由worker snapshot提供，非共时进程组峰"});
    if let Err(e) = result {
        report["error"] = json!(e.to_string());
        save(&config.output_directory.join("report.json"), &report)?;
        return Err(e);
    }
    if report["status"] == "unfinished" {
        report["status"] = json!("complete");
    }
    save(&config.output_directory.join("report.json"), &report)?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn first_difference_keeps_order_and_values_without_tolerance() {
        let a = json!({"markets":["a","b"],"v":1});
        let b = json!({"markets":["b","a"],"v":1});
        let d = first_difference(&a, &b, "").unwrap();
        assert_eq!(d["path"], "/markets/0");
        assert_eq!(d["left"], "a");
        assert_eq!(d["right"], "b");
        assert!(first_difference(&a, &a, "").is_none());
        assert_eq!(
            first_difference(&json!({"x":null}), &json!({}), "").unwrap()["right_present"],
            false
        );
        let frame = json!({"global":[[1,0,0,0]],"markets":[[[2,0,0,0]],[[3,0,0,0]]]});
        let mut other = frame.clone();
        other["markets"][1] = json!([[9, 0, 0, 0]]);
        let (left, right, _, shared) = pair_inputs(&json!({"call":1}), &frame, &other).unwrap();
        assert_eq!(shared, 3);
        assert!(Arc::ptr_eq(&left.private, &right.private));
        assert!(Arc::ptr_eq(&left.global, &right.global));
        assert!(Arc::ptr_eq(&left.markets[0], &right.markets[0]));
        assert!(!Arc::ptr_eq(&left.markets[1], &right.markets[1]));
    }
    #[test]
    fn original_prefix_rejects_corruption_and_unsupported_action() {
        let row = json!({"call":1,"options":[{"kind":"NoTrade"},{"kind":"Pair"}],"selected_option":0,"rpc":[]});
        assert_eq!(
            prefix(serde_json::to_string(&row).unwrap().as_bytes(), 1)
                .unwrap()
                .len(),
            1
        );
        let mut invalid = row;
        invalid["call"] = json!(2);
        assert!(prefix(serde_json::to_string(&invalid).unwrap().as_bytes(), 1).is_err());
        invalid["call"] = json!(1);
        invalid["selected_option"] = json!(1);
        assert!(prefix(serde_json::to_string(&invalid).unwrap().as_bytes(), 1).is_err());
        assert!(prefix(b"{broken", 1).is_err());
    }
    fn config(temp: &std::path::Path) -> DiagnosticConfig {
        let missing = DiagnosticFile {
            path: temp.join("missing"),
            sha256: "0".repeat(64),
        };
        DiagnosticConfig {
            input_pair: DiagnosticInputPair::LegacyOffOn,
            completed_collection: missing.clone(),
            trajectory: missing,
            worker: WorkerConfig {
                performance: None,
                python: "python3".into(),
                script: std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/diagnostic-worker.py"),
                model: temp.join("unused-model"),
                device: "cpu".into(),
                torch_threads: 1,
                max_batch: 1,
                encoding: crate::encoding::EncodingConfig {
                    field_buckets: 4096,
                    category_buckets: 4096,
                },
            },
            initial_model_sha256: "0".repeat(64),
            day_index: 0,
            candidate_index: 0,
            through_call: 3,
            max_public_streams: 2,
            max_forwards: 9,
            max_wall_seconds: 5,
            progress_interval_ms: 100,
            stop_file: temp.join("stop"),
            output_directory: temp.join("output"),
        }
    }
    #[test]
    fn current_profile_reuses_actual_production_learning_replay_and_labels_both_off() {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("input");
        fs::create_dir(&input).unwrap();
        let (consumer, rules, mut rows, _) =
            crate::learning::public_input_tests::collect(&input, false);
        rows.truncate(26);
        let binding = crate::FrozenRuleBindingV1::from_sealed(&rules, rules.access_mode());
        let consumer = crate::rules::bind_consumer(&consumer, &binding);
        let mut c = config(temp.path());
        c.input_pair = DiagnosticInputPair::CurrentConsumerReplay;
        c.through_call = 26;
        c.max_forwards = 52;
        fs::create_dir(&c.output_directory).unwrap();
        let budget = Budget {
            config: &c,
            started: Instant::now(),
        };
        let mut report = json!({"public_streams":[],"forwards_started":0});
        let left = rebuild(
            &c,
            &consumer,
            &rows,
            rules.clone(),
            false,
            &budget,
            &mut report,
        )
        .unwrap();
        let right = rebuild(&c, &consumer, &rows, rules, true, &budget, &mut report).unwrap();
        assert_eq!(left, right);
        assert_eq!(report["public_streams"][0]["source_progress"], false);
        assert_eq!(report["public_streams"][1]["source_progress"], false);
        assert_eq!(
            report["public_streams"][0]["production_learning_replay"],
            false
        );
        assert_eq!(
            report["public_streams"][1]["production_learning_replay"],
            true
        );
        assert_eq!(report["forwards_started"], 0);
    }
    #[test]
    fn no_torch_protocol_three_independent_branches_and_limit_preserve_in_flight() {
        let temp = tempfile::tempdir().unwrap();
        let mut c = config(temp.path());
        fs::create_dir(&c.output_directory).unwrap();
        let budget = Budget {
            config: &c,
            started: Instant::now(),
        };
        let frames=(1..=3).map(|i|InputFrame{private:Arc::new(json!({"call":i,"sampling_seed":7,"options":[{"kind":"NoTrade"},{"kind":"Pair"}],"selected_option":0,"rpc":[]})),global:Arc::new(json!([])),markets:vec![]}).collect::<Vec<_>>();
        let mut report = json!({"forwards_started":0,"forwards_completed":0,"model_evidence":[],"first_model_difference":null});
        model_branches(&c, &frames, &frames, true, &budget, &mut report).unwrap();
        assert_eq!(report["forwards_started"], 9);
        assert_eq!(
            report["model_metrics"]["calls"],
            json!({"inference":3,"teacher":3,"common_teacher":3})
        );
        let mut no_control = json!({"forwards_started":0,"forwards_completed":0,"model_evidence":[],"first_model_difference":null});
        model_branches(&c, &frames, &frames, false, &budget, &mut no_control).unwrap();
        assert_eq!(no_control["forwards_started"], 6);
        assert_eq!(no_control["model_metrics"]["calls"]["common_teacher"], 0);
        c.max_forwards = 2;
        let budget = Budget {
            config: &c,
            started: Instant::now(),
        };
        let mut r = json!({"forwards_started":0,"forwards_completed":0,"model_evidence":[],"first_model_difference":null});
        assert!(model_branches(&c, &frames, &frames, true, &budget, &mut r).is_err());
        assert_eq!(r["forwards_started"], 2);
        assert_eq!(r["forwards_completed"], 2);
    }
    #[test]
    fn protocol_failure_and_stop_do_not_retry_or_claim_completion() {
        let temp = tempfile::tempdir().unwrap();
        let c = config(temp.path());
        fs::create_dir(&c.output_directory).unwrap();
        let budget = Budget {
            config: &c,
            started: Instant::now(),
        };
        let frames = vec![InputFrame {
            private: Arc::new(json!({"call":1,"sampling_seed":0,"inject_error":true})),
            global: Arc::new(json!([])),
            markets: vec![],
        }];
        let mut r = json!({"forwards_started":0,"forwards_completed":0});
        assert!(model_branches(&c, &frames, &frames, false, &budget, &mut r).is_err());
        assert_eq!(r["forwards_started"], 1);
        assert_eq!(r["forwards_completed"], 0);
        assert_eq!(r["in_flight"]["call"], 1);
        let persisted: Value =
            serde_json::from_slice(&fs::read(c.output_directory.join("progress.json")).unwrap())
                .unwrap();
        assert_eq!(persisted["forwards_started"], 1);
        assert_eq!(persisted["forwards_completed"], 0);
        fs::write(&c.stop_file, b"stop").unwrap();
        assert!(budget.check().is_err());
    }
    #[test]
    fn two_core_streams_assemble_frozen_inputs_without_account_or_model_and_mismatch_stops_models()
    {
        let root = tempfile::tempdir().unwrap();
        let input = root.path().join("raw");
        fs::create_dir(&input).unwrap();
        let export = crate::learning_test_fixture::build(&input, 4);
        let template = root.path().join("template");
        fs::create_dir(&template).unwrap();
        let (run, _) = crate::learning_test_fixture::common::setup(&template);
        let mut candidate = HistoricalPreparedRunConfigV1::from(&run);
        candidate.public_statistics = Some(export.statistics.clone());
        candidate.timing.public_delivery_delay_ms = export.public_delivery_delay_ms.clone();
        let trajectory = root.path().join("trajectory.jsonl");
        candidate.strategy.effective_parameters["model_consumer"] = json!({"account_key":"fixture","sampling_seed":7,"request_namespace":"fixture","progress_every":0,"learning":{"group_id":"fixture","active_end_utc_ms":1,"evidence_end_utc_ms":2,"trajectory_path":trajectory,"collection_fault_after_call":null}});
        let mut c = config(root.path());
        c.through_call = 1;
        c.worker.model = root.path().join("model-identity-only");
        fs::write(&c.worker.model, b"not-a-model").unwrap();
        c.initial_model_sha256 = format!("{:x}", Sha256::digest(b"not-a-model"));
        let mut consumer = ConsumerRunConfig {
            worker: c.worker.clone(),
            rules: export.rules.clone().into(),
            days: vec![HistoricalDayConfigV1 {
                preparation: export.preparation.clone(),
                candidates: vec![candidate],
                progress_interval_seconds: 0,
                producer_lead_groups: Some(2),
            }],
            parallel_dates: 1,
        };
        let opened = crate::open_consumer_rules(&consumer).unwrap();
        consumer = crate::rules::bind_consumer(&consumer, &opened.binding);
        let query = crate::learning_test_fixture::common::BASE + 2500;
        let mut cfg = export;
        cfg.query_times_utc_ms = vec![query];
        let mut row = Value::Null;
        visit_continuous_public_frames_with_requirements_v1(&cfg,opened.rules,PublicObservationRequirementsV1::FULL,false,|_,group,frames|{row=json!({"version":"learning-trajectory-v1","account":"fixture","call":1,"processing_start_utc_ms":query+10,"behavior":{"model_sha256":c.initial_model_sha256},"rules_identity":consumer.rules.frozen().unwrap().expected_file_sha256,"market_ids":frames.iter().map(|f|f.observation.market.condition_id.clone()).collect::<Vec<_>>(),"market_private":frames.iter().map(|_|json!({"private":7})).collect::<Vec<_>>(),"global_private":{"time":query},"options":[{"kind":"NoTrade"}],"selected_option":0,"rpc":[],"market_frame":{"market_cutoff":{"decision_time":{"logical_time_utc_ms":query},"receipt":{"group":group}}}});Ok(())},|_|{},||false).unwrap();
        let row_bytes = serde_json::to_vec(&row).unwrap();
        fs::write(&trajectory, &row_bytes).unwrap();
        c.trajectory = DiagnosticFile {
            path: trajectory.clone(),
            sha256: format!("{:x}", Sha256::digest(&row_bytes)),
        };
        let collection = root.path().join("complete.json");
        let data =
            serde_json::to_vec(&json!({"consumer":consumer,"binding":{"identity":"fixture"}}))
                .unwrap();
        fs::write(&collection, &data).unwrap();
        c.completed_collection = DiagnosticFile {
            path: collection,
            sha256: format!("{:x}", Sha256::digest(&data)),
        };
        let result = run_diagnostic(&c, DiagnosticMode::Inputs).unwrap();
        assert_eq!(result["forwards_started"], 0);
        assert_eq!(result["public_streams"].as_array().unwrap().len(), 2);
        assert!(
            result["input_memory"]["shared_components"]
                .as_u64()
                .unwrap()
                >= 1
        );
        let mut bad = row;
        bad["market_frame"]["market_cutoff"]["receipt"]["group"]["group_id"] =
            json!("wrong-original-group");
        let bytes = serde_json::to_vec(&bad).unwrap();
        fs::write(&trajectory, &bytes).unwrap();
        c.trajectory.sha256 = format!("{:x}", Sha256::digest(&bytes));
        c.output_directory = root.path().join("mismatch");
        let result = run_diagnostic(&c, DiagnosticMode::Model).unwrap();
        assert_eq!(result["status"], "input_identity_mismatch_model_unknown");
        assert_eq!(result["forwards_started"], 0);
        assert_eq!(result["first_original_group_mismatch"]["call"], 1);
        assert!(result.get("worker_identity").is_none());
    }
}
