//! 诊断所属方：独立供料和真实逐输入协议；最后派生组及 Actor 完成后才确认。
use contracts::*;
use data_collect::{
    backpressure::bounded_event_queue,
    handoff::{read_handoff_v2, write_handoff_v2},
};
use data_pipeline_core::public_input::*;
use data_pipeline_training::{public_online::*, public_recording::*};
use serde::{Deserialize, Serialize};
use sim_system::{execution::SimRunActorErrorV2, public_consumption::*};
use std::{
    cell::RefCell,
    collections::VecDeque,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};
use tokio::net::UnixStream;
#[derive(Clone, Deserialize, Serialize)]
pub struct Window {
    pub first_input: u64,
    pub last_input: u64,
    pub first_group: u64,
    pub last_group: u64,
    pub first_at: i64,
    pub end_at: i64,
    pub natural_end: bool,
}
#[derive(Default, Serialize)]
struct Stats {
    supplied: u64,
    enqueued: u64,
    dequeued: u64,
    confirmed: u64,
    applied: u64,
    last_group_position: Option<u64>,
    pending_input: Option<u64>,
    pending_groups: usize,
    peak_queue: usize,
    preparation_peak_queue: usize,
    initial_queue_depth: u64,
    model_ready: bool,
    model_ready_ns: u128,
    window_anchor_ns: u128,
    window_started: bool,
    window_supply_complete: bool,
    complete: bool,
    failure: Option<String>,
    evidence_ns: u128,
    dpt: serde_json::Value,
}
struct Shared {
    stats: Mutex<Stats>,
    condition: Condvar,
    began: Instant,
    anchor: Mutex<Option<Instant>>,
    window: Window,
    path: PathBuf,
}
impl Shared {
    fn ns(&self) -> u128 {
        self.began.elapsed().as_nanos()
    }
    fn elapsed(&self) -> Option<Duration> {
        self.anchor.lock().unwrap().map(|at| at.elapsed())
    }
    fn line(&self, writer: &mut BufWriter<File>, value: impl Serialize) {
        let began = Instant::now();
        serde_json::to_writer(&mut *writer, &value).unwrap();
        writeln!(writer).unwrap();
        self.stats.lock().unwrap().evidence_ns += began.elapsed().as_nanos();
    }
    fn snapshot(&self) {
        let value = self.stats.lock().unwrap();
        let temporary = self.path.with_extension("tmp");
        let mut snapshot = serde_json::to_value(&*value).unwrap();
        snapshot["observed_ns"] = serde_json::json!(self.ns());
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for key in ["VmRSS:", "VmHWM:"] {
                if let Some(line) = status.lines().find(|l| l.starts_with(key)) {
                    snapshot[key.trim_end_matches(':')] = serde_json::json!(
                        line.split_whitespace()
                            .nth(1)
                            .unwrap()
                            .parse::<u64>()
                            .unwrap()
                    );
                }
            }
        }
        if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
            let fields = stat[stat.rfind(')').unwrap() + 2..]
                .split_whitespace()
                .collect::<Vec<_>>();
            snapshot["process_cpu_ticks"] = serde_json::json!(
                fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap()
            );
        }
        serde_json::to_writer(File::create(&temporary).unwrap(), &snapshot).unwrap();
        std::fs::rename(temporary, &self.path).unwrap();
    }
    fn fail(&self, error: &str) {
        self.stats.lock().unwrap().failure = Some(error.into());
        self.snapshot();
    }
}
struct Envelope {
    input: RecordedIngressV1,
    at: i64,
    queued: u128,
}
pub struct State {
    expected: VecDeque<serde_json::Value>,
    rt: tokio::runtime::Runtime,
    session: Option<PublicOnlineSessionV1<'static, UnixStream>>,
    pending: Option<PublicOnlinePendingV1<'static, UnixStream>>,
    groups: VecDeque<Arc<PreparedPublicGroupV1>>,
    shared: Arc<Shared>,
    input: u64,
    at: i64,
    last_at: i64,
    done: bool,
    log: BufWriter<File>,
    pub resources: serde_json::Value,
}
pub struct Source {
    pub control: Rc<RefCell<State>>,
}
fn error(value: impl std::fmt::Display) -> SimRunActorErrorV2 {
    SimRunActorErrorV2::PublicConsumptionSource(value.to_string())
}
impl Source {
    pub fn start(
        recording: &Path,
        output: &Path,
        header: PublicInputStreamHeaderV1,
        rows: usize,
        bytes: usize,
        recording_config: data_pipeline_training::gate_a::GateAOutputConfig,
        interval: u64,
    ) -> RecordingResult<Self> {
        let window: Window =
            serde_json::from_slice(&std::fs::read(std::env::var("FRAME_WINDOW")?)?)?;
        let root = output.parent().unwrap();
        let shared = Arc::new(Shared {
            stats: Mutex::new(Stats::default()),
            condition: Condvar::new(),
            began: Instant::now(),
            anchor: Mutex::new(None),
            window: window.clone(),
            path: root.join("online-progress.json"),
        });
        let (tx, mut rx) = bounded_event_queue::<Envelope>(25000)?;
        let (server, client) = std::os::unix::net::UnixStream::pair()?;
        server.set_nonblocking(true)?;
        client.set_nonblocking(true)?;
        let expected = if let Some(path) = std::env::var_os("FRAME_VERIFY") {
            std::fs::read_to_string(path)?
                .lines()
                .map(serde_json::from_str)
                .collect::<Result<VecDeque<_>, _>>()?
        } else {
            VecDeque::new()
        };
        let supply = shared.clone();
        let recording = recording.to_owned();
        let sender_header = header.clone();
        std::thread::Builder::new().name("source-feed".into()).spawn(move || {
            let result=(||->RecordingResult<()> {
                let rt=tokio::runtime::Builder::new_current_thread().enable_all().build()?;
                let mut reader=PublicRecordingReaderV1::recover(&recording,rows,bytes)?;
                let mut log=BufWriter::new(File::create(root_path(&supply,"supply.jsonl"))?);
                let mut last_at=i64::MIN;
                while let Some(row)=reader.next_row()? {
                    let PublicStoredRowV1::Input(input)=row else {continue;};
                    if input.input_position>supply.window.last_input {break;}
                    let at=match &input.ingress {DptIngressV1::Message{source_evidence,..}=>source_evidence.received_at_utc_ms,DptIngressV1::EndOfInput=>last_at,DptIngressV1::Aborted{..}=>return Err("原录制Aborted".into())};
                    last_at=last_at.max(at);
                    let measured=input.input_position>=supply.window.first_input;
                    if input.input_position==supply.window.first_input {
                        let mut state=supply.stats.lock().unwrap();
                        while !state.model_ready || state.confirmed<supply.window.first_input {state=supply.condition.wait(state).unwrap();}
                        *supply.anchor.lock().unwrap()=Some(Instant::now());state.initial_queue_depth=state.enqueued-state.confirmed;state.preparation_peak_queue=state.peak_queue;state.peak_queue=0;state.window_started=true;state.window_anchor_ns=supply.ns();
                    }
                    let planned=if measured {Duration::from_millis((last_at-supply.window.first_at).max(0) as u64)} else {Duration::ZERO};
                    if measured && std::env::var_os("FRAME_PROTOCOL").is_none() {
                        let anchor=supply.anchor.lock().unwrap().unwrap();
                        rt.block_on(async {tokio::time::sleep_until(tokio::time::Instant::from_std(anchor+planned)).await});
                    }
                    let seq=input.input_position+1;
                    let supplied=supply.ns();supply.stats.lock().unwrap().supplied=seq;
                    let queued=supply.ns();
                    rt.block_on(tx.send(Envelope {input,at:last_at,queued})).map_err(|_|"供料队列接收者停止")?;
                    let enqueued=supply.ns();let depth=25000-tx.capacity();
                    {let mut s=supply.stats.lock().unwrap();s.enqueued=seq;s.peak_queue=s.peak_queue.max(depth);}
                    if measured {supply.line(&mut log,serde_json::json!({"seq":seq,"at":last_at,"planned_ns":planned.as_nanos(),"supplied_ns":supplied,"enqueue_start_ns":queued,"enqueued_ns":enqueued,"queue_depth":depth}));}
                }
                supply.stats.lock().unwrap().window_supply_complete=true;
                log.flush()?;supply.snapshot();Ok(())
            })();if let Err(e)=result {supply.fail(&e.to_string());}
        })?;
        let handoff = shared.clone();
        std::thread::Builder::new().name("source-handoff".into()).spawn(move || {
            let rt=tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            let result:RecordingResult<()>=rt.block_on(async {
                let mut stream=UnixStream::from_std(server)?;
                let mut log=BufWriter::new(File::create(root_path(&handoff,"handoff.jsonl"))?);
                write_handoff_v2(&mut stream,&DataCollectHandoffV2::Session(DataCollectHandoffSessionV2 {frame_schema_version:DATA_COLLECT_HANDOFF_SCHEMA_V2.into(),environment:"diagnostic-recorded-typed-raw".into(),run_id:sender_header.input_stream_id,git_commit:std::env::var("FRAME_REVISION")?,binary_sha256:std::env::var("FRAME_BINARY_SHA")?,config_digest:std::env::var("FRAME_CONFIG_SHA")?,collector_host_id:"local-diagnostic".into(),collector_region:"local".into()})).await?;
                assert_eq!(read_handoff_v2(&mut stream).await?,DataCollectHandoffV2::Received {transport_sequence:0});
                while let Some(item)=rx.recv().await {
                    let seq=item.input.input_position+1;
                    let dequeued=handoff.ns();handoff.stats.lock().unwrap().dequeued=seq;
                    match item.input.ingress {
                        DptIngressV1::Message{row,..}=> {
                            write_handoff_v2(&mut stream,&DataCollectHandoffV2::Message {transport_sequence:seq,row}).await?;
                            let sent=handoff.ns();
                            let answer=read_handoff_v2(&mut stream).await?;
                            if answer!= (DataCollectHandoffV2::Received {transport_sequence:seq}) {return Err("确认不匹配".into());}
                            let ack=handoff.ns();
                            {let mut s=handoff.stats.lock().unwrap();s.confirmed=seq;handoff.condition.notify_all();}
                            if seq>handoff.window.first_input {handoff.line(&mut log,serde_json::json!({"seq":seq,"at":item.at,"queued_ns":item.queued,"dequeued_ns":dequeued,"sent_ns":sent,"ack_ns":ack,"remaining_queue":rx.len()}));}
                        }
                        DptIngressV1::EndOfInput=> {write_handoff_v2(&mut stream,&DataCollectHandoffV2::End {last_confirmed_sequence:seq-1}).await?;}
                        DptIngressV1::Aborted{..}=>return Err("来源Aborted".into()),
                    }
                }
                log.flush()?;Ok(())
            });if let Err(e)=result {handoff.fail(&e.to_string());}
        })?;
        let watcher = shared.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(1));
                watcher.snapshot();
                let s = watcher.stats.lock().unwrap();
                if s.complete || s.failure.is_some() {
                    break;
                }
            }
        });
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let stream = {
            let _guard = rt.enter();
            UnixStream::from_std(client)?
        };
        let progress = Box::leak(Box::new(PublicOnlineProgressV1::default()));
        let config = PublicOnlineConfigV1 {
            header,
            recording: recording_config,
            recording_in_flight_inputs: 128.try_into()?,
            stabilize_every_inputs: interval.try_into()?,
            read_page_rows: rows.try_into()?,
            max_read_batch_bytes: bytes.try_into()?,
        };
        let session = rt.block_on(PublicOnlineSessionV1::start(
            stream, output, config, progress,
        ))?;
        Ok(Source {
            control: Rc::new(RefCell::new(State {
                expected,
                rt,
                session: Some(session),
                pending: None,
                groups: VecDeque::new(),
                shared: shared.clone(),
                input: 0,
                at: i64::MIN,
                last_at: i64::MIN,
                done: false,
                log: BufWriter::new(File::create(root_path(&shared, "dpt-inputs.jsonl"))?),
                resources: serde_json::Value::Null,
            })),
        })
    }
}
fn root_path(shared: &Shared, name: &str) -> PathBuf {
    shared.path.parent().unwrap().join(name)
}
impl State {
    pub fn ready(&self) {
        let mut s = self.shared.stats.lock().unwrap();
        s.model_ready = true;
        s.model_ready_ns = self.ns();
        self.shared.condition.notify_all();
    }
    pub fn elapsed(&self) -> Option<Duration> {
        self.shared.elapsed()
    }
    pub fn acknowledge_if_empty(&mut self) -> RecordingResult<()> {
        if !self.groups.is_empty() {
            return Ok(());
        }
        let Some(pending) = self.pending.take() else {
            return Ok(());
        };
        let before = self.shared.ns();
        let measured = self.input >= self.shared.window.first_input;
        match self.rt.block_on(pending.complete())? {
            PublicOnlineCompletedInputV1::Continue(session) => self.session = Some(session),
            PublicOnlineCompletedInputV1::End(completion) => {
                self.resources = serde_json::to_value(completion)?;
                self.done = true;
            }
        }
        let end = self.shared.ns();
        self.shared.stats.lock().unwrap().pending_input = None;
        if measured {
            self.shared.line(&mut self.log,serde_json::json!({"input":self.input,"complete_begin_ns":before,"complete_end_ns":end,"groups_remaining":0}));
        }
        if !self.shared.window.natural_end && self.input == self.shared.window.last_input {
            let stopped = self
                .rt
                .block_on(self.session.take().unwrap().stop_and_drain())?;
            self.resources = serde_json::to_value(stopped)?;
            self.done = true;
        }
        if self.done {
            if !self.expected.is_empty() {
                return Err("对照组未全部消费".into());
            }
            self.log.flush()?;
            let mut stats = self.shared.stats.lock().unwrap();
            stats.complete = true;
            stats.dpt = self.resources.clone();
            drop(stats);
            self.shared.snapshot();
        }
        Ok(())
    }
    pub fn delivery_lag_ns(&self, logical: i64) -> i128 {
        self.elapsed().unwrap_or_default().as_nanos() as i128
            - (logical - self.shared.window.first_at) as i128 * 1_000_000
    }
    pub fn ns(&self) -> u128 {
        self.shared.ns()
    }
    pub fn snapshot(&self) -> serde_json::Value {
        let mut v = serde_json::to_value(&*self.shared.stats.lock().unwrap()).unwrap();
        v["window"] = serde_json::to_value(&self.shared.window).unwrap();
        v
    }
}
impl Iterator for Source {
    type Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut state = self.control.borrow_mut();
        loop {
            if let Some(prepared) = state.groups.pop_front() {
                if prepared.reference.group_position >= state.shared.window.first_group
                    && !state.expected.is_empty()
                {
                    let original = state.expected.pop_front().unwrap();
                    assert_eq!(
                        serde_json::to_value(&prepared.reference).unwrap(),
                        original["reference"]
                    );
                    assert_eq!(
                        serde_json::to_value(&*prepared.group).unwrap(),
                        original["group"]
                    );
                }
                {
                    let mut stats = state.shared.stats.lock().unwrap();
                    stats.pending_groups = state.groups.len();
                    stats.last_group_position = Some(prepared.reference.group_position);
                }
                let at = state.last_at.max(state.at);
                state.last_at = at;
                return Some(Ok(PublicGroupDeliveryV1 {
                    prepared,
                    delivered_at_utc_ms: at,
                }));
            }
            if let Err(e) = state.prepare_next() {
                return Some(Err(e));
            }
            if state.done {
                return None;
            }
        }
    }
}
impl State {
    /// 在整段 next_group/retry 之前准备来源，不 pop 组，也不确认正在交给消费者的组。
    pub fn prepare_next(&mut self) -> Result<bool, SimRunActorErrorV2> {
        while self.groups.is_empty() && !self.done {
            // 有派生组时只能由模型所属方完成；零组输入可在这里完成。
            if self.pending.is_some() {
                return Err(error("最后派生组尚未由模型确认"));
            }
            let session = self.session.take().unwrap();
            let remaining = self.elapsed().map_or(Duration::from_secs(1200), |elapsed| {
                Duration::from_secs(1200).saturating_sub(elapsed)
            });
            let begin = self.ns();
            let next = self.rt.block_on(async {
                tokio::time::timeout(remaining, session.next_applied(std::future::pending())).await
            });
            let next = match next {
                Ok(Ok(value)) => value,
                Ok(Err(e)) => {
                    self.shared.fail(&e.to_string());
                    return Err(error(e));
                }
                Err(e) => {
                    self.shared.fail(&e.to_string());
                    return Err(error(e));
                }
            };
            let PublicOnlineNextV1::Applied(mut pending) = next else {
                return Err(error("非预期Stopped"));
            };
            self.input = pending.input().input_position;
            if let DptIngressV1::Message {
                source_evidence, ..
            } = &pending.input().ingress
            {
                self.at = source_evidence.received_at_utc_ms;
            }
            self.resources = pending.bench_resources();
            let groups = PreparedPublicGroupV1::from_online(&mut pending);
            let applied = self.ns();
            {
                let mut s = self.shared.stats.lock().unwrap();
                s.applied = self.input + 1;
                s.pending_input = Some(self.input);
                s.pending_groups = groups.len();
                s.dpt = self.resources.clone();
            }
            if self.input >= self.shared.window.first_input {
                let value = serde_json::json!({"input":self.input,"next_begin_ns":begin,"applied_ns":applied,"derived_groups":groups.iter().map(|g|g.reference.group_position).collect::<Vec<_>>(),"recording":self.resources});
                let shared = self.shared.clone();
                shared.line(&mut self.log, value);
            }
            self.groups = groups.into();
            self.pending = Some(pending);
            if self.groups.is_empty()
                && let Err(e) = self.acknowledge_if_empty()
            {
                self.shared.fail(&e.to_string());
                return Err(error(e));
            }
        }
        Ok(self.done)
    }
}
