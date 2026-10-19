//! 一个常驻Python进程；只合批已就绪的独立账户请求，没有SIM时钟或隐式批等待。
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::Instant,
};
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
}
struct Request {
    value: Value,
    reply: mpsc::Sender<Result<Value, String>>,
}
#[derive(Clone)]
pub struct ModelWorker {
    sender: mpsc::Sender<Vec<Request>>,
    pub config: Arc<WorkerConfig>,
    io: Arc<IoCounters>,
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
struct Process {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    io: Arc<IoCounters>,
}
impl Process {
    fn exchange(&mut self, value: &Value) -> Result<Value, String> {
        let bytes = line_bytes(value).map_err(|e| e.to_string())?;
        self.input
            .write_all(&bytes)
            .and_then(|_| self.input.flush())
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
        serde_json::from_str(&line).map_err(|e| format!("模型JSON边界: {e}"))
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl ModelWorker {
    pub fn start(config: WorkerConfig) -> Result<Self, String> {
        if config.max_batch == 0
            || config.torch_threads == 0
            || config.encoding.field_buckets == 0
            || config.encoding.category_buckets == 0
        {
            return Err("模型批/线程/编码桶必须为正".into());
        }
        let started = Instant::now();
        let mut child = Command::new(&config.python)
            .arg(&config.script)
            .arg("serve")
            .arg(&config.model)
            .arg(&config.device)
            .arg(config.torch_threads.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| e.to_string())?;
        let io = Arc::new(IoCounters::default());
        let mut process = Process {
            input: child.stdin.take().unwrap(),
            output: BufReader::new(child.stdout.take().unwrap()),
            child,
            io: io.clone(),
        };
        let hello=process.exchange(&json!({"hello":{"encoding":config.encoding,"encoding_version":crate::encoding::ENCODING_VERSION}}))?;
        if hello.get("ready") != Some(&Value::Bool(true)) {
            return Err(format!("模型加载失败: {hello}"));
        }
        eprintln!(
            "模型加载完成 {:.3}s: {}",
            started.elapsed().as_secs_f64(),
            hello
        );
        let (sender, receiver) = mpsc::channel::<Vec<Request>>();
        let config = Arc::new(config);
        let max_batch = config.max_batch;
        thread::spawn(move || {
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
                let n = queued.len().min(max_batch);
                let pending = queued.drain(..n).collect::<Vec<_>>();
                let request = json!({"batch":pending.iter().map(|r|&r.value).collect::<Vec<_>>()});
                match process.exchange(&request) {
                    Ok(response) => {
                        if let Some(values) = response
                            .get("batch")
                            .and_then(Value::as_array)
                            .filter(|v| v.len() == pending.len())
                        {
                            for (r, v) in pending.into_iter().zip(values) {
                                let _ = r.reply.send(Ok(v.clone()));
                            }
                        } else {
                            for r in pending {
                                let _ = r.reply.send(Err(format!("模型批响应不匹配: {response}")));
                            }
                        }
                    }
                    Err(e) => {
                        for r in pending {
                            let _ = r.reply.send(Err(e.clone()));
                        }
                        break;
                    }
                }
            }
        });
        Ok(Self { sender, config, io })
    }
    pub fn io_metrics(&self) -> Value {
        json!({"request_bytes_including_envelope_and_newline":self.io.request_bytes.load(Ordering::Relaxed),"response_bytes_including_envelope_and_newline":self.io.response_bytes.load(Ordering::Relaxed),"exchanges_including_load_and_metrics":self.io.exchanges.load(Ordering::Relaxed)})
    }
    pub fn request(&self, value: Value) -> Result<Value, String> {
        Ok(self.request_batch(vec![value])?.remove(0))
    }
    /// 调用方已就绪的独立账户请求；按配置上限分批，不推进同账户未来输入。
    pub fn request_batch(&self, values: Vec<Value>) -> Result<Vec<Value>, String> {
        let mut receivers = Vec::new();
        for chunk in values.chunks(self.config.max_batch) {
            let mut requests = Vec::new();
            for value in chunk {
                let (reply, receive) = mpsc::channel();
                requests.push(Request {
                    value: value.clone(),
                    reply,
                });
                receivers.push(receive);
            }
            self.sender.send(requests).map_err(|e| e.to_string())?;
        }
        receivers
            .into_iter()
            .map(|r| {
                let value = r.recv().map_err(|e| e.to_string())??;
                if let Some(error) = value.get("error") {
                    return Err(format!("模型失败（不转换成NoTrade）: {error}"));
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
    fn one_buffer_has_original_json_line_bytes() {
        let value = json!({"batch":[{"name":"中文","tokens":[[1,0,0.1,0],[2,3,0.0,3]]}]});
        let mut original = Vec::new();
        serde_json::to_writer(&mut original, &value).unwrap();
        original.push(b'\n');
        assert_eq!(line_bytes(&value).unwrap(), original);
    }
}
