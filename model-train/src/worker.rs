//! 常驻模型进程；token不经JSON数值数组，停止信号独立于长epoch请求。
use crate::encoding::Token;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, value::RawValue};
use std::{
    io::{BufRead, BufReader, Write},
    os::{
        fd::AsRawFd,
        unix::{net::UnixStream, process::CommandExt},
    },
    path::PathBuf,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
    time::Instant,
};
pub const TRANSPORT_VERSION: &str = "b-frame-v1";
fn host_bytes() -> u64 {
    24 << 30
}
fn gpu_bytes() -> u64 {
    16 << 30
}
fn reserve_bytes() -> u64 {
    12 << 30
}
fn block_bytes() -> u64 {
    256 << 20
}
fn interop_threads() -> usize {
    1
}
fn rust_threads() -> usize {
    16
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PerformanceConfig {
    pub mode: String,
    pub native_library: PathBuf,
    #[serde(default = "rust_threads")]
    pub rust_threads: usize,
    #[serde(default = "host_bytes")]
    pub host_cache_bytes: u64,
    #[serde(default = "gpu_bytes")]
    pub gpu_cache_bytes: u64,
    #[serde(default = "reserve_bytes")]
    pub runtime_reserve_bytes: u64,
    #[serde(default = "block_bytes")]
    pub streaming_block_bytes: u64,
    #[serde(default = "block_bytes")]
    pub queue_bytes: u64,
    #[serde(default = "interop_threads")]
    pub torch_interop_threads: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
    pub python: PathBuf,
    pub script: PathBuf,
    pub model: PathBuf,
    pub device: String,
    pub torch_threads: usize,
    pub max_batch: usize,
    pub encoding: crate::encoding::EncodingConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub performance: Option<PerformanceConfig>,
}
/// 在公共准备/编码并行开始前调用；同一进程复用同一个全局池。
pub(crate) fn configure_runtime(config: &WorkerConfig) -> Result<(), String> {
    if let Some(performance) = &config.performance {
        static CONFIGURED: std::sync::OnceLock<Result<usize, String>> = std::sync::OnceLock::new();
        let result = CONFIGURED.get_or_init(|| {
            match rayon::ThreadPoolBuilder::new()
                .num_threads(performance.rust_threads)
                .build_global()
            {
                Ok(()) => Ok(rayon::current_num_threads()),
                Err(error) => {
                    let actual = rayon::current_num_threads();
                    if actual == performance.rust_threads {
                        Ok(actual)
                    } else {
                        Err(format!(
                            "公共Rust池已初始化为{actual}，不能采用声明的{}: {error}",
                            performance.rust_threads
                        ))
                    }
                }
            }
        });
        match result {
            Ok(actual) if *actual == performance.rust_threads => {}
            Ok(actual) => return Err(format!("本进程公共Rust池已配置为{actual}")),
            Err(error) => return Err(error.clone()),
        }
    }
    Ok(())
}

enum Payload {
    Json(Value),
    Raw(Box<RawValue>),
    Frame {
        metadata: Value,
        global: Vec<Token>,
        markets: Vec<Vec<Token>>,
    },
}
impl Payload {
    fn retained_bytes(&self) -> usize {
        match self {
            Self::Json(v) => serde_json::to_vec(v).map_or(0, |b| b.len()),
            Self::Raw(v) => v.get().len(),
            Self::Frame {
                metadata,
                global,
                markets,
            } => {
                serde_json::to_vec(metadata).map_or(0, |b| b.len())
                    + (global.len() + markets.iter().map(Vec::len).sum::<usize>())
                        * std::mem::size_of::<Token>()
            }
        }
    }
    fn legacy(&self) -> Value {
        match self {
            Self::Json(v) => v.clone(),
            Self::Raw(_) => unreachable!("raw请求使用独立typed envelope，不转Value"),
            Self::Frame {
                metadata,
                global,
                markets,
            } => {
                let mut v = metadata.clone();
                v["global"] = json!(global);
                v["markets"] = json!(markets);
                v
            }
        }
    }
}
fn raw_response_error(raw: &RawValue) -> Result<Option<Value>, String> {
    let fields: std::collections::BTreeMap<String, &RawValue> =
        serde_json::from_str(raw.get()).map_err(|e| e.to_string())?;
    // Presence, including error:null, has the same meaning as Value::get.
    fields
        .get("error")
        .map(|error| serde_json::from_str(error.get()).map_err(|e| e.to_string()))
        .transpose()
}
enum ReplyValue {
    Json(Value),
    Raw(Box<RawValue>),
}
#[derive(Deserialize)]
struct RawBatchResponse {
    batch: Vec<Box<RawValue>>,
}
#[derive(Serialize)]
struct RawBatchRequest<'a> {
    batch: Vec<&'a RawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_layouts: Option<Vec<Value>>,
}
enum BatchResponse {
    Json(Value),
    Raw(RawBatchResponse),
}
impl BatchResponse {
    fn items(self, expected: usize) -> Result<Vec<ReplyValue>, String> {
        match self {
            Self::Json(value) => match value.get("batch").and_then(Value::as_array) {
                Some(values) if values.len() == expected => {
                    Ok(values.iter().cloned().map(ReplyValue::Json).collect())
                }
                _ => Err(format!("模型批响应不匹配: {value}")),
            },
            Self::Raw(value) if value.batch.len() == expected => {
                Ok(value.batch.into_iter().map(ReplyValue::Raw).collect())
            }
            Self::Raw(_) => Err("raw模型批响应数量不匹配".into()),
        }
    }
}
struct Request {
    payload: Payload,
    reply: mpsc::Sender<Result<ReplyValue, String>>,
    _permit: Arc<BytePermit>,
}
struct ByteState {
    current: usize,
    peak: usize,
    waited_seconds: f64,
}
struct ByteBudget {
    limit: usize,
    state: Mutex<ByteState>,
    changed: Condvar,
}
struct BytePermit {
    budget: Arc<ByteBudget>,
    bytes: usize,
}
impl ByteBudget {
    fn acquire(self: &Arc<Self>, bytes: usize) -> BytePermit {
        let started = Instant::now();
        let mut state = self.state.lock().unwrap();
        // 单个合法大帧在队列清空后独占，不把额度变成新的输入拒绝条件。
        while state.current != 0 && state.current.saturating_add(bytes) > self.limit {
            state = self.changed.wait(state).unwrap();
        }
        state.waited_seconds += started.elapsed().as_secs_f64();
        state.current = state.current.saturating_add(bytes);
        state.peak = state.peak.max(state.current);
        BytePermit {
            budget: self.clone(),
            bytes,
        }
    }
}
impl Drop for BytePermit {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock().unwrap();
        state.current -= self.bytes;
        self.budget.changed.notify_all();
    }
}
struct WorkerState {
    sender: Mutex<Option<mpsc::SyncSender<Vec<Request>>>>,
    thread: Mutex<Option<JoinHandle<Result<(), String>>>>,
    cancel: Mutex<UnixStream>,
    budget: Arc<ByteBudget>,
}
impl WorkerState {
    fn finish(&self) -> Result<(), String> {
        self.sender.lock().map_err(|e| e.to_string())?.take();
        if let Some(thread) = self.thread.lock().map_err(|e| e.to_string())?.take() {
            thread
                .join()
                .map_err(|_| "模型通信线程panic".to_owned())??;
        }
        Ok(())
    }
}
impl Drop for WorkerState {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            eprintln!("模型关闭失败: {error}");
        }
    }
}
#[derive(Clone)]
pub struct ModelWorker {
    state: Arc<WorkerState>,
    pub config: Arc<WorkerConfig>,
    io: Arc<IoCounters>,
    compact: bool,
}
#[derive(Default)]
struct IoCounters {
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    exchanges: AtomicU64,
}
fn line_bytes(value: &Value) -> Result<Vec<u8>, serde_json::Error> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}
fn token_columns(sequence: &[Token], out: &mut Vec<u8>) {
    for t in sequence {
        out.extend_from_slice(&t.0.to_le_bytes());
    }
    for t in sequence {
        out.extend_from_slice(&t.1.to_le_bytes());
    }
    for t in sequence {
        out.extend_from_slice(&t.2.to_bits().to_le_bytes());
    }
    for t in sequence {
        out.push(t.3);
    }
}
fn compact_bytes(values: &[&Payload]) -> Result<Vec<u8>, String> {
    let mut batch = Vec::new();
    let mut layouts = Vec::new();
    let mut payload = Vec::new();
    for value in values {
        match value {
            Payload::Raw(_) => return Err("raw控制使用专用typed envelope".into()),
            Payload::Json(v) => {
                batch.push(v.clone());
                layouts.push(Value::Null);
            }
            Payload::Frame {
                metadata,
                global,
                markets,
            } => {
                batch.push(metadata.clone());
                layouts.push(json!({"global":global.len(),"markets":markets.iter().map(Vec::len).collect::<Vec<_>>()}));
                token_columns(global, &mut payload);
                for sequence in markets {
                    token_columns(sequence, &mut payload);
                }
            }
        }
    }
    let metadata = serde_json::to_vec(&json!({"batch":batch,"token_layouts":layouts}))
        .map_err(|e| e.to_string())?;
    let metadata_len =
        u32::try_from(metadata.len()).map_err(|_| "紧凑控制元数据长度超出协议u32".to_owned())?;
    let mut bytes = Vec::with_capacity(16 + metadata.len() + payload.len());
    bytes.extend_from_slice(b"BMF1");
    bytes.extend_from_slice(&metadata_len.to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&metadata);
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}
struct Process {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
    io: Arc<IoCounters>,
    finished: bool,
}
impl Process {
    fn exchange_bytes(&mut self, bytes: &[u8]) -> Result<Value, String> {
        serde_json::from_str(&self.exchange_line(bytes)?).map_err(|e| format!("模型JSON边界: {e}"))
    }
    fn exchange_line(&mut self, bytes: &[u8]) -> Result<String, String> {
        self.input
            .as_mut()
            .ok_or("模型输入已关闭")?
            .write_all(bytes)
            .and_then(|_| self.input.as_mut().unwrap().flush())
            .map_err(|e| e.to_string())?;
        self.io
            .request_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let mut line = String::new();
        if self
            .output
            .read_line(&mut line)
            .map_err(|e| e.to_string())?
            == 0
        {
            return Err(format!("模型进程EOF: {:?}", self.child.try_wait()));
        }
        self.io
            .response_bytes
            .fetch_add(line.len() as u64, Ordering::Relaxed);
        self.io.exchanges.fetch_add(1, Ordering::Relaxed);
        Ok(line)
    }
    fn finish(&mut self) -> Result<(), String> {
        self.input.take();
        let status = self.child.wait().map_err(|e| e.to_string())?;
        self.finished = true;
        if status.success() {
            Ok(())
        } else {
            Err(format!("模型子进程非正常退出: {status}"))
        }
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
impl ModelWorker {
    pub fn start(config: WorkerConfig) -> Result<Self, String> {
        Self::start_with_environment(config, &[])
    }
    /// 工程观测包装使用同一生产启动/传输/关闭；仅增加已绑定的子进程环境。
    pub fn start_with_environment(
        config: WorkerConfig,
        environment: &[(std::ffi::OsString, std::ffi::OsString)],
    ) -> Result<Self, String> {
        Self::start_with_environment_and_fds(config, environment, &[])
    }
    /// 仅继承调用方已持有的观测FD；取消仍使用独立生产通道。
    pub fn start_with_environment_and_fds(
        config: WorkerConfig,
        environment: &[(std::ffi::OsString, std::ffi::OsString)],
        inherited: &[std::os::fd::RawFd],
    ) -> Result<Self, String> {
        if config.max_batch == 0
            || config.torch_threads == 0
            || config.encoding.field_buckets == 0
            || config.encoding.category_buckets == 0
        {
            return Err("模型批/线程/编码桶必须为正".into());
        }
        if config
            .performance
            .as_ref()
            .is_some_and(|p| !matches!(p.mode.as_str(), "storage_v1" | "device_v1"))
        {
            return Err("未知模型性能执行版本".into());
        }
        let started = Instant::now();
        let (cancel_reader, cancel_writer) = UnixStream::pair().map_err(|e| e.to_string())?;
        let cancel_fd = cancel_reader.as_raw_fd();
        let mut command = Command::new(&config.python);
        command
            .arg(&config.script)
            .arg("serve")
            .arg(&config.model)
            .arg(&config.device)
            .arg(config.torch_threads.to_string())
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .env("B_MODEL_CANCEL_FD", cancel_fd.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if let Some(performance) = &config.performance {
            command
                .env(
                    "B_MODEL_INTEROP_THREADS",
                    performance.torch_interop_threads.to_string(),
                )
                .env("OMP_NUM_THREADS", config.torch_threads.to_string())
                .env("MKL_NUM_THREADS", config.torch_threads.to_string());
        }
        command.envs(environment.iter().map(|(key, value)| (key, value)));
        // 取消读端独立继承，不经过请求队列。
        let mut inherited = inherited.to_vec();
        inherited.push(cancel_fd);
        unsafe {
            command.pre_exec(move || {
                for fd in &inherited {
                    if libc::fcntl(*fd, libc::F_SETFD, 0) == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                Ok(())
            });
        }
        let mut child = command.spawn().map_err(|e| e.to_string())?;
        drop(cancel_reader);
        let io = Arc::new(IoCounters::default());
        let mut process = Process {
            input: child.stdin.take(),
            output: BufReader::new(child.stdout.take().unwrap()),
            child,
            io: io.clone(),
            finished: false,
        };
        let mut hello = json!({"encoding":config.encoding,"encoding_version":crate::encoding::ENCODING_VERSION});
        if let Some(p) = &config.performance {
            hello["performance"] = json!(p);
            hello["transport"] = TRANSPORT_VERSION.into();
        }
        let hello = process
            .exchange_bytes(&line_bytes(&json!({"hello":hello})).map_err(|e| e.to_string())?)?;
        if hello.get("ready") != Some(&Value::Bool(true)) {
            return Err(format!("模型加载失败: {hello}"));
        }
        let compact = config.performance.is_some();
        if compact && hello["transport"] != TRANSPORT_VERSION {
            return Err("模型未接受已声明紧凑协议".into());
        }
        eprintln!(
            "模型加载完成 {:.3}s: {}",
            started.elapsed().as_secs_f64(),
            hello
        );
        let budget = Arc::new(ByteBudget {
            limit: config
                .performance
                .as_ref()
                .map_or(usize::MAX, |p| p.queue_bytes as usize),
            state: Mutex::new(ByteState {
                current: 0,
                peak: 0,
                waited_seconds: 0.0,
            }),
            changed: Condvar::new(),
        });
        let (sender, receiver) = mpsc::sync_channel::<Vec<Request>>(config.max_batch);
        let config = Arc::new(config);
        let max_batch = config.max_batch;
        let thread = thread::spawn(move || {
            let mut queued = std::collections::VecDeque::new();
            loop {
                if queued.is_empty() {
                    match receiver.recv() {
                        Ok(first) => queued.extend(first),
                        Err(_) => break,
                    }
                }
                while queued.len() < max_batch {
                    match receiver.try_recv() {
                        Ok(more) => queued.extend(more),
                        Err(_) => break,
                    }
                }
                // Raw controls are kept in their own batch. Ordinary JSON/frame
                // replies retain the original parser and compact transport path.
                let raw = matches!(queued.front().unwrap().payload, Payload::Raw(_));
                let n = queued
                    .iter()
                    .take_while(|request| matches!(request.payload, Payload::Raw(_)) == raw)
                    .count()
                    .min(max_batch);
                let pending = queued.drain(..n).collect::<Vec<_>>();
                let response = if raw {
                    let batch = pending
                        .iter()
                        .map(|request| match &request.payload {
                            Payload::Raw(value) => value.as_ref(),
                            _ => unreachable!("同类raw批次"),
                        })
                        .collect();
                    let envelope = RawBatchRequest {
                        batch,
                        token_layouts: compact.then(|| vec![Value::Null; pending.len()]),
                    };
                    serde_json::to_vec(&envelope)
                        .map_err(|e| e.to_string())
                        .and_then(|metadata| {
                            let bytes = if compact {
                                let length = u32::try_from(metadata.len())
                                    .map_err(|_| "raw控制元数据超过协议u32")?;
                                let mut bytes = Vec::with_capacity(16 + metadata.len());
                                bytes.extend_from_slice(b"BMF1");
                                bytes.extend_from_slice(&length.to_le_bytes());
                                bytes.extend_from_slice(&0u64.to_le_bytes());
                                bytes.extend_from_slice(&metadata);
                                bytes
                            } else {
                                let mut bytes = metadata;
                                bytes.push(b'\n');
                                bytes
                            };
                            process.exchange_line(&bytes)
                        })
                        .and_then(|line| {
                            serde_json::from_str::<RawBatchResponse>(&line)
                                .map(BatchResponse::Raw)
                                .map_err(|e| format!("模型raw JSON边界: {e}"))
                        })
                } else {
                    let values = pending.iter().map(|r| &r.payload).collect::<Vec<_>>();
                    let bytes = if compact {
                        compact_bytes(&values)
                    } else {
                        line_bytes(
                            &json!({"batch":values.iter().map(|v|v.legacy()).collect::<Vec<_>>()}),
                        )
                        .map_err(|e| e.to_string())
                    };
                    bytes
                        .and_then(|bytes| process.exchange_bytes(&bytes))
                        .map(BatchResponse::Json)
                };
                match response {
                    Ok(response) => match response.items(pending.len()) {
                        Ok(values) => {
                            for (request, value) in pending.into_iter().zip(values) {
                                let _ = request.reply.send(Ok(value));
                            }
                        }
                        Err(error) => {
                            for request in pending {
                                let _ = request.reply.send(Err(error.clone()));
                            }
                        }
                    },
                    Err(error) => {
                        for request in pending {
                            let _ = request.reply.send(Err(error.clone()));
                        }
                        return Err(error);
                    }
                }
            }
            process.finish()
        });
        Ok(Self {
            state: Arc::new(WorkerState {
                sender: Mutex::new(Some(sender)),
                thread: Mutex::new(Some(thread)),
                cancel: Mutex::new(cancel_writer),
                budget,
            }),
            config,
            io,
            compact,
        })
    }
    pub fn compact(&self) -> bool {
        self.compact
    }
    pub fn cancel(&self) -> Result<(), String> {
        self.state
            .cancel
            .lock()
            .map_err(|e| e.to_string())?
            .write_all(b"!")
            .map_err(|e| e.to_string())
    }
    pub fn shutdown(&self) -> Result<(), String> {
        self.state.finish()
    }
    pub fn io_metrics(&self) -> Value {
        let queue = self.state.budget.state.lock().unwrap();
        json!({"request_bytes_including_envelope_and_newline":self.io.request_bytes.load(Ordering::Relaxed),"response_bytes_including_envelope_and_newline":self.io.response_bytes.load(Ordering::Relaxed),"exchanges_including_load_and_metrics":self.io.exchanges.load(Ordering::Relaxed),"transport":if self.compact {TRANSPORT_VERSION}else{"json-line-v1"},"retained_queue_bytes":queue.current,"retained_queue_peak_bytes":queue.peak,"producer_wait_seconds":queue.waited_seconds})
    }
    pub fn request(&self, value: Value) -> Result<Value, String> {
        Ok(self.request_payloads(vec![Payload::Json(value)])?.remove(0))
    }
    /// 受控停止验证保留原错误响应；其余错误仍按生产策略返回失败。
    pub fn request_controlled_stop(&self, value: Value) -> Result<Value, String> {
        Ok(self
            .request_payloads_inner(vec![Payload::Json(value)], true)?
            .remove(0))
    }
    pub fn request_batch(&self, values: Vec<Value>) -> Result<Vec<Value>, String> {
        self.request_payloads(values.into_iter().map(Payload::Json).collect())
    }
    pub fn request_frame(
        &self,
        metadata: Value,
        global: Vec<Token>,
        markets: Vec<Vec<Token>>,
    ) -> Result<Value, String> {
        Ok(self
            .request_payloads(vec![Payload::Frame {
                metadata,
                global,
                markets,
            }])?
            .remove(0))
    }
    fn request_payloads(&self, values: Vec<Payload>) -> Result<Vec<Value>, String> {
        self.request_payloads_inner(values, false)
    }
    fn request_payloads_inner(
        &self,
        values: Vec<Payload>,
        allow_stopped: bool,
    ) -> Result<Vec<Value>, String> {
        self.exchange_payloads_inner(values, allow_stopped)?
            .into_iter()
            .map(|reply| match reply {
                ReplyValue::Json(value) => Ok(value),
                ReplyValue::Raw(_) => Err("普通请求收到raw响应".into()),
            })
            .collect()
    }
    /// Exact JSON controls for the frozen-label driver; no Value/f64 roundtrip.
    pub(crate) fn request_raw(&self, value: Box<RawValue>) -> Result<Box<RawValue>, String> {
        match self
            .exchange_payloads_inner(vec![Payload::Raw(value)], false)?
            .remove(0)
        {
            ReplyValue::Raw(value) => Ok(value),
            ReplyValue::Json(_) => Err("raw请求收到普通响应".into()),
        }
    }
    fn exchange_payloads_inner(
        &self,
        values: Vec<Payload>,
        allow_stopped: bool,
    ) -> Result<Vec<ReplyValue>, String> {
        let mut receivers = Vec::new();
        let mut values = values.into_iter().peekable();
        while values.peek().is_some() {
            let mut payloads = Vec::new();
            let mut retained = 0usize;
            while let Some(value) = values.peek() {
                let bytes = value.retained_bytes();
                if !payloads.is_empty()
                    && (payloads.len() == self.config.max_batch
                        || retained.saturating_add(bytes) > self.state.budget.limit)
                {
                    break;
                }
                retained = retained.saturating_add(bytes);
                payloads.push(values.next().unwrap());
            }
            // 一个已就绪批次只申请一次，避免生产者持有未提交许可而互等。
            let permit = Arc::new(self.state.budget.acquire(retained));
            let mut requests = Vec::new();
            for payload in payloads {
                let (reply, receive) = mpsc::channel();
                requests.push(Request {
                    payload,
                    reply,
                    _permit: permit.clone(),
                });
                receivers.push(receive);
            }
            self.state
                .sender
                .lock()
                .map_err(|e| e.to_string())?
                .as_ref()
                .ok_or("模型worker已关闭")?
                .send(requests)
                .map_err(|e| e.to_string())?;
        }
        receivers
            .into_iter()
            .map(|r| {
                let value = r.recv().map_err(|e| e.to_string())??;
                match &value {
                    ReplyValue::Json(value) => {
                        if let Some(error) = value.get("error") {
                            if !(allow_stopped && value["stopped_before_commit"] == true) {
                                return Err(format!("模型失败（不转换成NoTrade）: {error}"));
                            }
                        }
                    }
                    ReplyValue::Raw(raw) => {
                        if let Some(error) = raw_response_error(raw)? {
                            return Err(format!("模型失败（不转换成NoTrade）: {error}"));
                        }
                    }
                }
                Ok(value)
            })
            .collect()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn raw_error_field_presence_matches_original_value_gate() {
        for text in [
            r#"{"error":null}"#,
            r#"{"error":"original"}"#,
            r#"{"label":{"critic_target":-1.1864766031503677}}"#,
        ] {
            let raw = RawValue::from_string(text.to_owned()).unwrap();
            let old: Value = serde_json::from_str(text).unwrap();
            assert_eq!(raw_response_error(&raw).unwrap(), old.get("error").cloned());
        }
    }
    #[test]
    fn one_buffer_has_original_json_line_bytes() {
        let value = json!({"batch":[{"name":"中文","tokens":[[1,0,0.1,0],[2,3,0.0,3]]}]});
        let mut original = Vec::new();
        serde_json::to_writer(&mut original, &value).unwrap();
        original.push(b'\n');
        assert_eq!(line_bytes(&value).unwrap(), original);
    }
    #[test]
    fn compact_columns_preserve_unsigned_ids_float_bits_and_order() {
        let sequence = vec![
            Token(u32::MAX, 0x80000001, -0.0, 3),
            Token(1, 2, f32::from_bits(0x3eaaaaab), 0),
        ];
        let mut bytes = Vec::new();
        token_columns(&sequence, &mut bytes);
        assert_eq!(bytes.len(), 26);
        assert_eq!(
            &bytes[..8],
            &[u32::MAX.to_le_bytes(), 1u32.to_le_bytes()].concat()
        );
        assert_eq!(
            &bytes[16..24],
            &[
                (-0.0f32).to_bits().to_le_bytes(),
                0x3eaaaaabu32.to_le_bytes()
            ]
            .concat()
        );
        assert_eq!(&bytes[24..], &[3, 0]);
    }
}
