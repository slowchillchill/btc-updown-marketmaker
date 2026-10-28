//! #377 已冻结12槽的真实跨进程驱动；不另建模型或学习算法。
use crate::{
    encoding::Token,
    worker::{ModelWorker, WorkerConfig},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, value::RawValue};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{self, OpenOptions},
    io::{Read, Write},
    os::{fd::AsRawFd, unix::net::UnixStream},
    path::PathBuf,
    time::Instant,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BoundFile {
    pub path: PathBuf,
    pub sha256: String,
    pub max_bytes: usize,
}
impl BoundFile {
    fn read(&self) -> Result<Vec<u8>, String> {
        if self.max_bytes == 0
            || fs::metadata(&self.path).map_err(|e| e.to_string())?.len() > self.max_bytes as u64
        {
            return Err("短槽材料超过冻结界限".into());
        }
        let mut bytes = Vec::new();
        fs::File::open(&self.path)
            .map_err(|e| e.to_string())?
            .take(self.max_bytes as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| e.to_string())?;
        if bytes.len() > self.max_bytes || format!("{:x}", Sha256::digest(&bytes)) != self.sha256 {
            return Err("短槽材料长度/SHA与冻结绑定不同".into());
        }
        Ok(bytes)
    }
    fn json(&self) -> Result<Value, String> {
        serde_json::from_slice(&self.read()?).map_err(|e| e.to_string())
    }
    pub(crate) fn lines(&self) -> Result<Vec<Value>, String> {
        let bytes = self.read()?;
        bytes
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).map_err(|e| e.to_string()))
            .collect()
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SlotConfig {
    pub slot: u8,
    pub manifest: BoundFile,
    pub consumer_worker: WorkerConfig,
    pub learner_worker: WorkerConfig,
    pub consumer_samples: BoundFile,
    pub learner_samples: BoundFile,
    pub fixtures: BoundFile,
    /// 正式learn_open原config/group/account_weights，checkpoint目录仅本槽副本。
    pub learning_open: Value,
    pub output_directory: PathBuf,
}
fn checked(value: Value) -> Result<Value, String> {
    if value.get("error").is_some() {
        Err(format!("短槽生产请求失败: {value}"))
    } else {
        Ok(value)
    }
}
fn request(worker: &ModelWorker, value: Value) -> Result<Value, String> {
    checked(worker.request(value)?)
}
fn typed(worker: &ModelWorker, mut frame: Value) -> Result<Value, String> {
    let global: Vec<Token> = serde_json::from_value(
        frame
            .as_object_mut()
            .ok_or("frame不是对象")?
            .remove("global")
            .ok_or("frame缺global")?,
    )
    .map_err(|e| e.to_string())?;
    let markets: Vec<Vec<Token>> = serde_json::from_value(
        frame
            .as_object_mut()
            .unwrap()
            .remove("markets")
            .ok_or("frame缺markets")?,
    )
    .map_err(|e| e.to_string())?;
    checked(worker.request_frame(frame, global, markets)?)
}
fn register(
    worker: &ModelWorker,
    phase: &str,
    samples: &[Value],
    fixtures: &[Value],
) -> Result<(), String> {
    let sample_keys=samples.iter().enumerate().map(|(index,s)|json!({"name":format!("{phase}/{index:03}"),"account":s["frame"]["account"],"call":s["frame"]["call"],"ordinal":index})).collect::<Vec<_>>();
    request(
        worker,
        json!({"phase":"learn_validation_register","samples":sample_keys,"fixtures":fixtures}),
    )?;
    Ok(())
}
fn collect(worker: &ModelWorker, samples: &[Value]) -> Result<(), String> {
    for sample in samples {
        let frame = &sample["frame"];
        let (begin, rpc) = if worker.compact() {
            let mut input = frame.clone();
            input["phase"] = "frame".into();
            let result = typed(worker, input)?;
            let trace=result["rpc"].as_array().ok_or("整帧缺rpc")?.iter().map(|item|json!({"request":item["request"],"response":{"post_only":item["response"]["post_only"],"index":item["response"]["index"],"rank":item["response"]["rank"]}})).collect::<Vec<_>>();
            (result["begin"].clone(), json!(trace))
        } else {
            let mut begin = frame.clone();
            begin["phase"] = "begin".into();
            let response = request(worker, begin)?;
            if response["option"] != frame["selected_option"] {
                return Err("原begin采样选项不同，不发送冻结条件域".into());
            }
            let mut trace = Vec::new();
            for item in frame["rpc"].as_array().ok_or("冻结frame缺rpc")? {
                let response = request(worker, item["request"].clone())?;
                trace.push(json!({"request":item["request"],"response":{"post_only":response["post_only"],"index":response["index"],"rank":response["rank"]}}));
            }
            (response, json!(trace))
        };
        if begin["option"] != frame["selected_option"] || rpc != frame["rpc"] {
            return Err("实际采集动作/条件域不同于原冻结历史，不继续短槽".into());
        }
    }
    Ok(())
}
fn environment(config: &SlotConfig, phase: &str) -> Vec<(OsString, OsString)> {
    [
        (
            "B_PERFORMANCE_MANIFEST",
            config.manifest.path.as_os_str().to_owned(),
        ),
        ("B_PERFORMANCE_SLOT", config.slot.to_string().into()),
        ("B_PERFORMANCE_PHASE", phase.into()),
        (
            "B_PERFORMANCE_OUTPUT",
            config.output_directory.as_os_str().to_owned(),
        ),
    ]
    .into_iter()
    .map(|(key, value)| (key.into(), value))
    .collect()
}
fn finish(worker: &ModelWorker) -> Result<Value, String> {
    request(worker, json!({"phase":"learn_validation_finish"}))
}
fn with_worker<T>(
    config: WorkerConfig,
    env: &[(OsString, OsString)],
    body: impl FnOnce(&ModelWorker) -> Result<T, String>,
) -> Result<T, String> {
    let worker = ModelWorker::start_with_environment(config, env)?;
    let result = body(&worker);
    let close = worker.shutdown();
    match (result, close) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (result, Err(error)) => Err(format!(
            "请求={}; worker关闭={error}",
            result.err().unwrap_or_else(|| "成功".into())
        )),
    }
}
#[cfg(test)]
fn assembled_learning_old_value(
    samples: &[Value],
    fixtures: &[Value],
    f4_labels: &Value,
) -> Result<(Value, Vec<Value>, Vec<Value>), String> {
    let mut labels = BTreeMap::<String, Vec<Value>>::new();
    let mut frames = Vec::new();
    let mut schedule = Vec::new();
    for sample in samples {
        let frame = sample["frame"].clone();
        let key = frame["account"].as_str().ok_or("学习账户缺失")?.to_owned();
        let account = labels.entry(key.clone()).or_default();
        schedule.push(json!({"account":key,"index":account.len()}));
        account.push(sample.get("label").ok_or("L65缺完整原标签")?.clone());
        frames.push(frame);
    }
    for case in ["F-N", "F-P", "F-R", "F-S"] {
        let fixture = fixtures
            .iter()
            .find(|f| f["case"] == case)
            .ok_or("F4缺冻结case")?;
        let frame = fixture["frame"].clone();
        let key = frame["account"].as_str().ok_or("F4缺账户")?.to_owned();
        if labels.contains_key(&key) {
            return Err("F4工程账户与原学习账户重叠".into());
        }
        labels.insert(
            key.clone(),
            vec![f4_labels.get(case).ok_or("缺冻结F4标签")?.clone()],
        );
        schedule.push(json!({"account":key,"index":0}));
        frames.push(frame);
    }
    Ok((json!(labels), schedule, frames))
}
// L65 retains its existing Value path. Only worker-created F4 labels are
// transported verbatim so Python binary64 values are never re-parsed in Rust.
#[derive(Serialize)]
#[serde(untagged)]
enum LearningLabel {
    Original(Value),
    Frozen(Box<RawValue>),
}
type LearningLabels = BTreeMap<String, Vec<LearningLabel>>;
#[derive(Deserialize)]
struct InitialF4Labels {
    labels: BTreeMap<String, Box<RawValue>>,
    labels_binding: Value,
}
#[derive(Serialize)]
struct PrepareLabels<'a> {
    phase: &'a str,
    labels: &'a LearningLabels,
    schedule: &'a [Value],
    #[serde(skip_serializing_if = "Option::is_none")]
    labels_binding: Option<&'a Value>,
}
#[derive(Serialize)]
struct LearningStep<'a> {
    phase: &'a str,
    frame: &'a Value,
    label: &'a LearningLabel,
}
fn request_typed<T: Serialize>(worker: &ModelWorker, value: &T) -> Result<Box<RawValue>, String> {
    worker.request_raw(serde_json::value::to_raw_value(value).map_err(|e| e.to_string())?)
}
fn assembled_learning(
    samples: &[Value],
    fixtures: &[Value],
    f4_labels: &BTreeMap<String, Box<RawValue>>,
) -> Result<(LearningLabels, Vec<Value>, Vec<Value>), String> {
    let mut labels = LearningLabels::new();
    let mut frames = Vec::new();
    let mut schedule = Vec::new();
    for sample in samples {
        let frame = sample["frame"].clone();
        let key = frame["account"].as_str().ok_or("学习账户缺失")?.to_owned();
        let account = labels.entry(key.clone()).or_default();
        schedule.push(json!({"account":key,"index":account.len()}));
        account.push(LearningLabel::Original(
            sample.get("label").ok_or("L65缺完整原标签")?.clone(),
        ));
        frames.push(frame);
    }
    for case in ["F-N", "F-P", "F-R", "F-S"] {
        let fixture = fixtures
            .iter()
            .find(|f| f["case"] == case)
            .ok_or("F4缺冻结case")?;
        let frame = fixture["frame"].clone();
        let key = frame["account"].as_str().ok_or("F4缺账户")?.to_owned();
        if labels.contains_key(&key) {
            return Err("F4工程账户与原学习账户重叠".into());
        }
        labels.insert(
            key.clone(),
            vec![LearningLabel::Frozen(
                f4_labels.get(case).ok_or("缺冻结F4标签")?.clone(),
            )],
        );
        schedule.push(json!({"account":key,"index":0}));
        frames.push(frame);
    }
    Ok((labels, schedule, frames))
}
fn prepare(
    worker: &ModelWorker,
    labels: &LearningLabels,
    schedule: &[Value],
    binding: &Value,
) -> Result<(), String> {
    let mut binding = binding.clone();
    if binding.get("max_bytes").is_none() {
        binding["max_bytes"] = binding["bytes"].clone();
    }
    request_typed(
        worker,
        &PrepareLabels {
            phase: "learn_validation_prepare",
            labels,
            schedule,
            labels_binding: Some(&binding),
        },
    )?;
    if worker.compact() {
        request_typed(
            worker,
            &PrepareLabels {
                phase: "learn_prepare",
                labels,
                schedule,
                labels_binding: None,
            },
        )?;
    }
    Ok(())
}
fn epochs(
    worker: &ModelWorker,
    frames: &[Value],
    labels: &LearningLabels,
    schedule: &[Value],
    stop: bool,
) -> Result<Vec<Value>, String> {
    let mut results = Vec::new();
    for _ in 0..2 {
        if worker.compact() {
            let command = json!({"phase":"learn_run_epoch"});
            let result = if stop {
                worker.request_controlled_stop(command)?
            } else {
                worker.request(command)?
            };
            if stop {
                if result["stopped_before_commit"] != true {
                    return Err("受控停止没有经过生产取消结果".into());
                }
                results.push(result);
                break;
            }
            results.push(checked(result)?);
        } else {
            request(worker, json!({"phase":"learn_epoch"}))?;
            for (frame, item) in frames.iter().zip(schedule) {
                let key = item["account"].as_str().ok_or("schedule账户缺失")?;
                let index = item["index"].as_u64().ok_or("schedule索引缺失")? as usize;
                request_typed(
                    worker,
                    &LearningStep {
                        phase: "learn_step",
                        frame,
                        label: &labels[key][index],
                    },
                )?;
            }
            results.push(request(worker, json!({"phase":"learn_end_epoch"}))?);
        }
    }
    Ok(results)
}
fn validate_samples(samples: &[Value]) -> Result<(), String> {
    if samples.len() != 65 {
        return Err("固定短样本必须65行".into());
    }
    for (index, sample) in samples.iter().enumerate() {
        if sample["ordinal"].as_u64() != Some(index as u64)
            || sample["expected_call"].as_u64() != Some(index as u64 + 1)
            || sample["frame"]["call"].as_u64() != Some(index as u64 + 1)
        {
            return Err("短样本ordinal/call不符".into());
        }
    }
    Ok(())
}
/// 仅由根编排器冻结槽位后调用；函数自身不选择槽位或重试。
pub fn run_slot(config: &SlotConfig) -> Result<Value, String> {
    let started = Instant::now();
    let manifest = config.manifest.json()?;
    if !(1..=12).contains(&config.slot) || manifest["slot"] != config.slot {
        return Err("短槽身份不符".into());
    }
    let consumer = config.consumer_samples.lines()?;
    let learner = config.learner_samples.lines()?;
    let fixtures = config.fixtures.lines()?;
    validate_samples(&consumer)?;
    validate_samples(&learner)?;
    fs::create_dir(&config.output_directory).map_err(|e| e.to_string())?;
    let outcome = (|| {
        let c = with_worker(
            config.consumer_worker.clone(),
            &environment(config, "C65"),
            |worker| {
                register(worker, "C65", &consumer, &[])?;
                collect(worker, &consumer)?;
                finish(worker)
            },
        )?;
        let cold = config.slot == 12;
        let stop = matches!(config.slot, 9 | 10);
        let (mut ready_read, ready_write) = UnixStream::pair().map_err(|e| e.to_string())?;
        let mut env = environment(config, "L65");
        if stop {
            env.push((
                "B_PERFORMANCE_READY_FD".into(),
                ready_write.as_raw_fd().to_string().into(),
            ));
        }
        let inherited = if stop {
            vec![ready_write.as_raw_fd()]
        } else {
            vec![]
        };
        let worker = ModelWorker::start_with_environment_and_fds(
            config.learner_worker.clone(),
            &env,
            &inherited,
        )?;
        drop(ready_write);
        let stop_thread = if stop {
            let control = worker.clone();
            Some(std::thread::spawn(move || {
                let mut marker = [0u8; 1];
                ready_read
                    .read_exact(&mut marker)
                    .map_err(|e| e.to_string())?;
                if marker != [b'R'] {
                    return Err("停止安全点marker不同".into());
                }
                control.cancel()
            }))
        } else {
            None
        };
        let learning: Result<Value, String> = (|| {
            register(&worker, "L65", &learner, &fixtures)?;
            request(&worker, config.learning_open.clone())?;
            collect(&worker, &learner)?;
            let initial_raw =
                request_typed(&worker, &json!({"phase":"learn_validation_initial_f4"}))?;
            let initial: InitialF4Labels =
                serde_json::from_str(initial_raw.get()).map_err(|e| e.to_string())?;
            // Preserve the old complete diagnostic output only. Learning never
            // consumes this Value; the raw labels above remain authoritative.
            let initial_summary: Value =
                serde_json::from_str(initial_raw.get()).map_err(|e| e.to_string())?;
            let (labels, schedule, frames) =
                assembled_learning(&learner, &fixtures, &initial.labels)?;
            if worker.compact() {
                for fixture in &fixtures {
                    let mut frame = fixture["frame"].clone();
                    frame["phase"] = "learn_cache_frame".into();
                    typed(&worker, frame)?;
                }
            }
            if cold {
                let snapshot = request(&worker, json!({"phase":"metrics"}))?;
                let sampling = snapshot
                    .get("collection_sampling_state")
                    .ok_or("生产采集终态缺精确CPU RNG封存")?
                    .clone();
                let hot = finish(&worker)?;
                worker.shutdown()?;
                let cold_result = with_worker(
                    config.learner_worker.clone(),
                    &environment(config, "L65-cold"),
                    |new_worker| {
                        register(new_worker, "L65", &learner, &fixtures)?;
                        request(new_worker, config.learning_open.clone())?;
                        let account = learner[0]["frame"]["account"].as_str().ok_or("L65缺账户")?;
                        request(
                            new_worker,
                            json!({"phase":"learn_restore_collection_sampling_state","state":sampling,"expected_calls":{account:65}}),
                        )?;
                        for frame in &frames {
                            let mut frame = frame.clone();
                            frame["phase"] = "learn_cache_frame".into();
                            typed(new_worker, frame)?;
                        }
                        prepare(new_worker, &labels, &schedule, &initial.labels_binding)?;
                        let epochs = epochs(new_worker, &frames, &labels, &schedule, false)?;
                        Ok(
                            json!({"epochs":epochs,"finish":finish(new_worker)?,"transport":new_worker.io_metrics()}),
                        )
                    },
                )?;
                Ok(json!({"initial_f4":initial_summary,"hot_finish":hot,"cold":cold_result}))
            } else {
                prepare(&worker, &labels, &schedule, &initial.labels_binding)?;
                let epochs = epochs(&worker, &frames, &labels, &schedule, stop)?;
                Ok(
                    json!({"initial_f4":initial_summary,"epochs":epochs,"finish":finish(&worker)?,"transport":worker.io_metrics()}),
                )
            }
        })();
        let close = worker.shutdown();
        let control = stop_thread
            .map(|thread| {
                thread
                    .join()
                    .map_err(|_| "停止控制线程panic".to_string())
                    .and_then(|result| result)
            })
            .unwrap_or(Ok(()));
        if let Err(error) = close {
            return Err(format!(
                "学习结果={}; shutdown={error}",
                learning.err().unwrap_or_default()
            ));
        }
        control?;
        Ok(json!({"consumer":c,"learner":learning?,"all_workers_waited":true}))
    })();
    let result = json!({"slot":config.slot,"elapsed_seconds":started.elapsed().as_secs_f64(),"result":outcome.as_ref().ok(),"error":outcome.as_ref().err()});
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(config.output_directory.join("driver-result.json"))
        .map_err(|e| e.to_string())?;
    serde_json::to_writer_pretty(&mut output, &result).map_err(|e| e.to_string())?;
    output.flush().map_err(|e| e.to_string())?;
    output.sync_all().map_err(|e| e.to_string())?;
    outcome.map(|_| result)
}

#[cfg(test)]
mod value_protocol_tests {
    use super::*;
    use crate::encoding::EncodingConfig;

    fn fake_worker(root: &std::path::Path, model: PathBuf) -> ModelWorker {
        fake_worker_mode(root, model, None)
    }
    fn fake_worker_mode(root: &std::path::Path, model: PathBuf, mode: Option<&str>) -> ModelWorker {
        let script = root.join("stdlib-label-worker.py");
        let code = r#"import hashlib, json, os, struct, sys
from pathlib import Path
sys.path.insert(0, os.environ['B_VALUE_PROTOCOL_MODULE_ROOT'])
from model_protocol import read_packet
source = Path(sys.argv[2])
source_bytes = source.read_bytes()
labels = json.loads(source_bytes)['labels']
binding = {'path':str(source),'sha256':hashlib.sha256(source_bytes).hexdigest(),'bytes':len(source_bytes),'max_bytes':len(source_bytes)}
binary=False
initial_calls=0
step_calls=0
prepare_calls=0
prepare_differences=[]
cases = ('F-N','F-P','F-R','F-S')
def bits(x): return struct.pack('>d',x).hex()
stream=sys.stdin.buffer
while True:
    if binary:
        request=read_packet(stream)
        if request is None: break
    else:
        line=stream.readline()
        if not line: break
        request=json.loads(line)
    if 'hello' in request:
        binary=bool(request['hello'].get('performance'))
        response = {'ready':True}
        if binary: response['transport']='b-frame-v1' 
    else:
        out=[]
        for item in request['batch']:
            if item['phase']=='learn_validation_initial_f4':
                initial_calls+=1
                out.append({'labels':labels,'labels_binding':binding})
            elif item['phase'] in ('learn_validation_prepare','learn_prepare'):
                prepare_calls+=1
                differences=[]
                for case in cases:
                    actual=item['labels']['account-'+case][0]
                    for field, expected in labels[case].items():
                        received=actual[field]
                        if received != expected:
                            differences.append({'case':case,'field':field,
                              'expected_repr':repr(expected),'received_repr':repr(received),
                              'expected_bits':bits(expected),'received_bits':bits(received),
                              'ulp_distance':abs(int(bits(expected),16)-int(bits(received),16))})
                prepare_differences.extend(differences)
                out.append({'exact':not differences,'differences':differences})
            elif item['phase']=='learn_step':
                account=item['frame']['account']
                expected=labels[account.removeprefix('account-')]
                if item['label']!=expected: out.append({'error':'step label changed'})
                else: step_calls+=1;out.append({'exact':True})
            elif item['phase'] in ('learn_epoch','learn_end_epoch','learn_run_epoch'):
                out.append({'epoch':1})
            elif item['phase']=='metrics':
                out.append({'initial_f4_calls':initial_calls,'step_calls':step_calls,'prepare_calls':prepare_calls,'prepare_differences':prepare_differences})
            else: out.append({'error':'unexpected phase'})
        response={'batch':out}
    print(json.dumps(response,allow_nan=False),flush=True)
"#;
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&script)
            .unwrap();
        f.write_all(code.as_bytes()).unwrap();
        drop(f);
        let performance = mode.map(|mode| crate::worker::PerformanceConfig {
            mode: mode.into(),
            native_library: "unused".into(),
            rust_threads: 16,
            host_cache_bytes: 1024,
            gpu_cache_bytes: 0,
            runtime_reserve_bytes: 0,
            streaming_block_bytes: 4,
            queue_bytes: 8,
            torch_interop_threads: 1,
        });
        ModelWorker::start_with_environment(
            WorkerConfig {
                python: "python3".into(),
                script,
                model,
                device: "cpu".into(),
                torch_threads: 1,
                max_batch: 2,
                encoding: EncodingConfig {
                    field_buckets: 4096,
                    category_buckets: 4096,
                },
                performance,
            },
            &[(
                "B_VALUE_PROTOCOL_MODULE_ROOT".into(),
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("python")
                    .into_os_string(),
            )],
        )
        .unwrap()
    }

    fn fixtures() -> Vec<Value> {
        ["F-N", "F-P", "F-R", "F-S"]
            .into_iter()
            .map(|case| json!({"case":case,"frame":{"account":format!("account-{case}")}}))
            .collect()
    }
    #[test]
    #[ignore = "explicit historical small-label binding; stdlib worker, no model"]
    fn reproduce_old_value_label_roundtrip() {
        let root = PathBuf::from(std::env::var("B_VALUE_PROTOCOL_EVIDENCE").unwrap());
        let path = PathBuf::from(std::env::var("B_VALUE_PROTOCOL_LABELS").unwrap());
        let bytes = fs::read(&path).unwrap();
        assert_eq!(bytes.len(), 1749);
        assert_eq!(
            format!("{:x}", Sha256::digest(&bytes)),
            "3ccdeb43a9fac1abbebdf8d6cc9bb3ea9b5ac30a1ff580b14a3f785ef4fff5c7"
        );
        let worker = fake_worker(&root, path);
        let initial = request(&worker, json!({"phase":"learn_validation_initial_f4"})).unwrap();
        let (labels, schedule, _) =
            assembled_learning_old_value(&[], &fixtures(), &initial["labels"]).unwrap();
        let result = request(
            &worker,
            json!({"phase":"learn_validation_prepare","labels":labels,"schedule":schedule}),
        )
        .unwrap();
        worker.shutdown().unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(root.join("old-value-reproduction.json"))
            .unwrap();
        serde_json::to_writer_pretty(&mut file, &result).unwrap();
        file.write_all(b"\n").unwrap();
        assert_eq!(result["exact"], false, "不能复现则停止，不按嫌疑修复");
        assert!(!result["differences"].as_array().unwrap().is_empty());
    }
    #[test]
    #[ignore = "explicit historical small-label binding; stdlib worker, no model"]
    fn exact_raw_labels_cover_prepare_steps_binary_and_cold_worker() {
        let root = PathBuf::from(std::env::var("B_VALUE_PROTOCOL_EVIDENCE").unwrap());
        let source = PathBuf::from(std::env::var("B_VALUE_PROTOCOL_LABELS").unwrap());
        let bytes = fs::read(&source).unwrap();
        assert_eq!(bytes.len(), 1749);
        assert_eq!(
            format!("{:x}", Sha256::digest(&bytes)),
            "3ccdeb43a9fac1abbebdf8d6cc9bb3ea9b5ac30a1ff580b14a3f785ef4fff5c7"
        );
        let mut reports = Vec::new();
        for (name, mode) in [
            ("R0", None),
            ("A", Some("storage_v1")),
            ("B", Some("device_v1")),
        ] {
            let dir = root.join(format!("raw-{name}"));
            fs::create_dir(&dir).unwrap();
            let worker = fake_worker_mode(&dir, source.clone(), mode);
            let initial_raw =
                request_typed(&worker, &json!({"phase":"learn_validation_initial_f4"})).unwrap();
            let initial: InitialF4Labels = serde_json::from_str(initial_raw.get()).unwrap();
            assert!(initial.labels["F-R"].get().contains("-1.1864766031503677"));
            let (labels, schedule, frames) =
                assembled_learning(&[], &fixtures(), &initial.labels).unwrap();
            let validation = request_typed(
                &worker,
                &PrepareLabels {
                    phase: "learn_validation_prepare",
                    labels: &labels,
                    schedule: &schedule,
                    labels_binding: Some(&initial.labels_binding),
                },
            )
            .unwrap();
            let checked: Value = serde_json::from_str(validation.get()).unwrap();
            assert_eq!(checked["exact"], true);
            assert_eq!(checked["differences"], json!([]));
            if worker.compact() {
                let production = request_typed(
                    &worker,
                    &PrepareLabels {
                        phase: "learn_prepare",
                        labels: &labels,
                        schedule: &schedule,
                        labels_binding: None,
                    },
                )
                .unwrap();
                assert_eq!(
                    serde_json::from_str::<Value>(production.get()).unwrap()["exact"],
                    true
                );
            }
            epochs(&worker, &frames, &labels, &schedule, false).unwrap();
            let hot = request(&worker, json!({"phase":"metrics"})).unwrap();
            assert_eq!(hot["initial_f4_calls"], 1);
            assert_eq!(
                hot["prepare_differences"],
                json!([]),
                "hot实际prepare比较不得有差异"
            );
            assert_eq!(hot["step_calls"], if mode.is_none() { 8 } else { 0 });
            assert_eq!(hot["prepare_calls"], if mode.is_none() { 1 } else { 2 });
            assert_eq!(worker.io_metrics()["retained_queue_bytes"], 0);
            worker.shutdown().unwrap();
            // New real process; reuse the same frozen raw labels, no initial teacher.
            let cold_dir = root.join(format!("cold-{name}"));
            fs::create_dir(&cold_dir).unwrap();
            let cold = fake_worker_mode(&cold_dir, source.clone(), mode);
            prepare(&cold, &labels, &schedule, &initial.labels_binding).unwrap();
            epochs(&cold, &frames, &labels, &schedule, false).unwrap();
            let status = request(&cold, json!({"phase":"metrics"})).unwrap();
            assert_eq!(status["initial_f4_calls"], 0);
            assert_eq!(
                status["prepare_differences"],
                json!([]),
                "cold实际两种prepare累计比较不得有差异"
            );
            assert_eq!(status["step_calls"], if mode.is_none() { 8 } else { 0 });
            assert_eq!(status["prepare_calls"], if mode.is_none() { 1 } else { 2 });
            assert_eq!(cold.io_metrics()["retained_queue_bytes"], 0);
            cold.shutdown().unwrap();
            reports.push(json!({"variant":name,"hot":hot,"cold":status,"strict_equal":true,"worker_waited":true}));
        }
        // L65's original Value path is kept, not relabeled or reparsed as a new contract.
        let original = json!({"advantage":-0.25,"critic_target":0.5,"special":{"kept":"full"}});
        let sample = json!({"frame":{"account":"original-L65"},"label":original});
        #[derive(Deserialize)]
        struct Saved {
            labels: BTreeMap<String, Box<RawValue>>,
        }
        let saved: Saved = serde_json::from_slice(&bytes).unwrap();
        let (mixed, _, _) = assembled_learning(&[sample], &fixtures(), &saved.labels).unwrap();
        let LearningLabel::Original(value) = &mixed["original-L65"][0] else {
            panic!("L65 must retain original Value")
        };
        assert_eq!(value, &original);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(root.join("raw-label-validation.json"))
            .unwrap();
        serde_json::to_writer_pretty(
            &mut file,
            &json!({"variants":reports,"models":0,"torch_imports":0,"L65_value_unchanged":true}),
        )
        .unwrap();
        file.write_all(b"\n").unwrap();
    }
}
