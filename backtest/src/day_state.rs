//! 日入口专用的消费前沿与完整恢复点协调。
use crate::{run::*, *};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use shared_run::*;
use sim_system::{
    execution::*, finality::TradeFinalityPolicyV2, platform_rules::SealedProcessRuleSnapshotV3,
    public_consumption::*, scenario::*,
};
use std::{
    collections::VecDeque,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CandidateCursorV1 {
    pub public: usize,
    pub hidden: usize,
    pub times: crate::timing::DeliveryTimes,
}
#[derive(Clone)]
pub(crate) enum ProductionV1 {
    Running,
    Ended,
    Failed(String),
}
pub(crate) struct CandidateProgressV1 {
    pub cursor: CandidateCursorV1,
    pub active: bool,
    pub waiting: bool,
    pub ready: Option<u64>,
    pub complete: Option<serde_json::Value>,
    pub source_health: Option<bool>,
    pub runtime_started: bool,
}
pub(crate) struct DayBufferV1 {
    pub public_base: usize,
    pub public: VecDeque<PublicGroupDeliveryV1>,
    pub hidden_base: usize,
    pub hidden: VecDeque<AcceptedHiddenStepBucketV1>,
    pub candidates: Vec<CandidateProgressV1>,
    pub production: ProductionV1,
    pub requested: Option<u64>,
    pub generation: u64,
    pub failure: Option<String>,
}
#[derive(Clone)]
pub(crate) struct DayHubV1(pub Arc<(Mutex<DayBufferV1>, Condvar)>);
impl DayHubV1 {
    pub fn new(candidates: &[HistoricalPreparedRunConfigV1]) -> Self {
        Self(Arc::new((
            Mutex::new(DayBufferV1 {
                public_base: 0,
                public: Default::default(),
                hidden_base: 0,
                hidden: Default::default(),
                candidates: candidates
                    .iter()
                    .map(|c| CandidateProgressV1 {
                        cursor: CandidateCursorV1 {
                            public: 0,
                            hidden: 0,
                            times: crate::timing::DeliveryTimes {
                                delays: c.timing.public_delivery_delay_ms.clone(),
                                last: None,
                            },
                        },
                        active: true,
                        waiting: false,
                        ready: None,
                        complete: None,
                        source_health: None,
                        runtime_started: false,
                    })
                    .collect(),
                production: ProductionV1::Running,
                requested: None,
                generation: 0,
                failure: None,
            }),
            Condvar::new(),
        )))
    }
    pub fn compact(buffer: &mut DayBufferV1) {
        let public = buffer
            .candidates
            .iter()
            .filter(|c| c.active)
            .map(|c| c.cursor.public)
            .min()
            .unwrap_or(buffer.public_base + buffer.public.len());
        let hidden = buffer
            .candidates
            .iter()
            .filter(|c| c.active)
            .map(|c| c.cursor.hidden)
            .min()
            .unwrap_or(buffer.hidden_base + buffer.hidden.len());
        while buffer.public_base < public {
            buffer.public.pop_front();
            buffer.public_base += 1;
        }
        while buffer.hidden_base < hidden {
            buffer.hidden.pop_front();
            buffer.hidden_base += 1;
        }
    }
    pub fn finish(
        &self,
        index: usize,
        result: &Result<HistoricalDayCandidateResultV1, MaterialError>,
    ) {
        let (mut buffer, cv) = (self.0.0.lock().unwrap(), &self.0.1);
        buffer.candidates[index].active = false;
        if let Ok(result) = result {
            buffer.candidates[index].complete = Some(result.report.clone());
        } else if let Err(error) = result {
            buffer.failure = Some(error.to_string());
        }
        Self::compact(&mut buffer);
        cv.notify_all();
    }
}
pub(crate) struct DayPublicSourceV1 {
    pub hub: DayHubV1,
    pub index: usize,
    pub restored: VecDeque<PublicGroupDeliveryV1>,
    failed: bool,
}
impl DayPublicSourceV1 {
    pub fn new(hub: DayHubV1, index: usize, restored: VecDeque<PublicGroupDeliveryV1>) -> Self {
        Self {
            hub,
            index,
            restored,
            failed: false,
        }
    }
}
impl Iterator for DayPublicSourceV1 {
    type Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(group) = self.restored.pop_front() {
            return Some(Ok(group));
        }
        if self.failed {
            return None;
        }
        let mut buffer = self.hub.0.0.lock().unwrap();
        loop {
            let position = buffer.candidates[self.index].cursor.public;
            if let Some(mut group) = buffer.public.get(position - buffer.public_base).cloned() {
                let cursor = &mut buffer.candidates[self.index].cursor;
                cursor.public += 1;
                let time = cursor.times.execution_time_for_visible_group(
                    &group.prepared.reference,
                    group.delivered_at_utc_ms,
                );
                buffer.candidates[self.index].waiting = false;
                DayHubV1::compact(&mut buffer);
                self.hub.0.1.notify_all();
                return Some(time.map(|at| {
                    group.delivered_at_utc_ms = at;
                    group
                }));
            }
            if let Some(error) = buffer.failure.clone() {
                self.failed = true;
                return Some(Err(SimRunActorErrorV2::PublicConsumptionSource(error)));
            }
            match &buffer.production {
                ProductionV1::Ended => return None,
                ProductionV1::Failed(error) => {
                    self.failed = true;
                    return Some(Err(SimRunActorErrorV2::PublicConsumptionSource(
                        error.clone(),
                    )));
                }
                ProductionV1::Running => {}
            }
            buffer.candidates[self.index].waiting = true;
            self.hub.0.1.notify_all();
            buffer = self.hub.0.1.wait(buffer).unwrap();
        }
    }
}
pub(crate) struct DayHiddenSourceV1 {
    pub hub: DayHubV1,
    pub index: usize,
    failed: bool,
}
impl DayHiddenSourceV1 {
    pub fn new(hub: DayHubV1, index: usize) -> Self {
        Self {
            hub,
            index,
            failed: false,
        }
    }
}
impl Iterator for DayHiddenSourceV1 {
    type Item = Result<AcceptedHiddenStepBucketV1, MaterialError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let mut buffer = self.hub.0.0.lock().unwrap();
        loop {
            let position = buffer.candidates[self.index].cursor.hidden;
            if let Some(bucket) = buffer.hidden.get(position - buffer.hidden_base).cloned() {
                buffer.candidates[self.index].cursor.hidden += 1;
                buffer.candidates[self.index].waiting = false;
                DayHubV1::compact(&mut buffer);
                self.hub.0.1.notify_all();
                return Some(Ok(bucket));
            }
            if let Some(error) = buffer.failure.clone() {
                self.failed = true;
                return Some(Err(error.into()));
            }
            match &buffer.production {
                ProductionV1::Ended => return None,
                ProductionV1::Failed(error) => {
                    self.failed = true;
                    return Some(Err(error.clone().into()));
                }
                ProductionV1::Running => {}
            }
            buffer.candidates[self.index].waiting = true;
            self.hub.0.1.notify_all();
            buffer = self.hub.0.1.wait(buffer).unwrap();
        }
    }
}
#[derive(Serialize, Deserialize)]
pub(crate) struct CandidateCheckpointV1<D> {
    driver: Vec<u8>,
    ledger: Vec<u8>,
    shared: Vec<u8>,
    strategy: Vec<u8>,
    state: RuntimeLoopCheckpointV1<D>,
    pub prefetched: Option<checkpoint::Delivery>,
    next_hidden: Option<HiddenBucketCheckpointV1>,
}
pub(crate) struct DayCandidateCheckpointV1<D> {
    pub hub: DayHubV1,
    pub index: usize,
    pub root: PathBuf,
    pub resumed: Option<CandidateCheckpointV1<D>>,
    pub hidden: Option<AcceptedHiddenStepBucketV1>,
    pub every_events: Option<u64>,
    events: u64,
}
impl<D> DayCandidateCheckpointV1<D> {
    pub fn new(
        hub: DayHubV1,
        index: usize,
        root: PathBuf,
        resumed: Option<CandidateCheckpointV1<D>>,
        every_events: Option<u64>,
    ) -> Self {
        Self {
            hub,
            index,
            root,
            resumed,
            hidden: None,
            every_events,
            events: 0,
        }
    }
}
impl<S> RuntimeCheckpointV1<S, DayPublicSourceV1> for DayCandidateCheckpointV1<S::Diagnostics>
where
    S: CheckpointStrategyV1,
    S::Diagnostics: Serialize + serde::de::DeserializeOwned,
{
    fn started(&mut self) {
        let mut buffer = self.hub.0.0.lock().unwrap();
        buffer.candidates[self.index].runtime_started = true;
        self.hub.0.1.notify_all();
    }
    fn restore(
        &mut self,
        config: &HistoricalPreparedRunConfigV1,
        rules: Arc<SealedProcessRuleSnapshotV3>,
        strategy: &mut S,
    ) -> Result<Option<RestoredRuntimeV1<S::Diagnostics>>, MaterialError> {
        let Some(saved) = self.resumed.take() else {
            return Ok(None);
        };
        let finality = Arc::new(TradeFinalityPolicyV2::decode_and_seal(&std::fs::read(
            &config.finality_policy_path,
        )?)?);
        let scenario = Arc::new(SealedPlatformScenarioV1::decode_and_seal(
            &serde_json::to_vec(&SimulationScenarioArtifactV1 {
                schema_version: PLATFORM_SCENARIO_SCHEMA_V1.into(),
                markets: vec![],
            })?,
        )?);
        let latency = sim_system::latency_profile::load_embedded_proxy_latency_profile()?;
        let (mut state, observation) = PublicConsumptionStateV1::restore_checkpoint(
            saved.driver.as_slice(),
            rules,
            finality,
            scenario,
            latency,
            config.max_group_bytes,
            config.finite_in_flight_groups,
        )?;
        let mut ledger = ledger::LedgerWriterV1::restore_checkpoint(saved.ledger.as_slice())?;
        state.reconnect_ledger_recording(&mut ledger, &config.ledger_root)?;
        let shared = SharedExecutionV2::restore_checkpoint(
            ledger,
            observation.clone(),
            saved.shared.as_slice(),
        )?;
        strategy.restore_strategy_checkpoint(&mut saved.strategy.as_slice())?;
        self.hidden = saved.next_hidden.map(HiddenBucketCheckpointV1::restore);
        Ok(Some((state, shared, observation, saved.state)))
    }
    fn restored_hidden(&mut self) -> Option<Result<AcceptedHiddenStepBucketV1, MaterialError>> {
        self.hidden.take().map(Ok)
    }
    fn save(
        &mut self,
        driver: &mut FinitePublicConsumptionV1<DayPublicSourceV1>,
        shared: &SharedExecutionV2,
        strategy: &S,
        state: RuntimeLoopCheckpointRefV1<'_, S::Diagnostics>,
        next_hidden: Option<&AcceptedHiddenStepBucketV1>,
    ) -> Result<(), MaterialError> {
        self.events += 1;
        let generation = {
            let mut buffer = self.hub.0.0.lock().unwrap();
            if buffer.requested.is_none()
                && self.every_events.is_some_and(|n| n > 0 && self.events >= n)
            {
                buffer.requested = Some(buffer.generation);
                self.hub.0.1.notify_all();
            }
            buffer
                .requested
                .filter(|g| buffer.candidates[self.index].ready != Some(*g))
        };
        let Some(generation) = generation else {
            return Ok(());
        };
        let mut execution = Vec::new();
        driver.write_checkpoint(&mut execution)?;
        let mut ledger = Vec::new();
        shared.ledger().write_checkpoint(&mut ledger)?;
        let mut registry = Vec::new();
        shared.write_checkpoint(&mut registry)?;
        let mut strategy_state = Vec::new();
        strategy.write_strategy_checkpoint(&mut strategy_state)?;
        #[derive(Serialize)]
        struct Saved<'a, D> {
            driver: Vec<u8>,
            ledger: Vec<u8>,
            shared: Vec<u8>,
            strategy: Vec<u8>,
            state: RuntimeLoopCheckpointRefV1<'a, D>,
            prefetched: Option<checkpoint::Delivery>,
            next_hidden: Option<HiddenBucketCheckpointV1>,
        }
        let saved = Saved {
            driver: execution,
            ledger,
            shared: registry,
            strategy: strategy_state,
            state,
            prefetched: driver
                .prefetched_group()?
                .as_ref()
                .map(checkpoint::Delivery::capture),
            next_hidden: next_hidden.map(HiddenBucketCheckpointV1::capture),
        };
        let path = self
            .root
            .join(format!("checkpoint-{generation:020}"))
            .join(format!("candidate-{:06}.cbor", self.index));
        write_cbor(&path, &saved)?;
        let mut buffer = self.hub.0.0.lock().unwrap();
        buffer.candidates[self.index].ready = Some(generation);
        self.hub.0.1.notify_all();
        while buffer.requested == Some(generation) && buffer.failure.is_none() {
            buffer = self.hub.0.1.wait(buffer).unwrap();
        }
        if let Some(error) = &buffer.failure {
            return Err(error.clone().into());
        }
        self.events = 0;
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
pub(crate) struct DayCheckpointV1 {
    pub format: String,
    pub identity: String,
    pub generation: u64,
    pub applied: u64,
    pub core: Vec<u8>,
    pub producer: Vec<u8>,
    pub catalog: HistoricalRuntimeCatalogV1,
    pub public_base: usize,
    pub public: Vec<checkpoint::Delivery>,
    pub hidden_base: usize,
    pub hidden: Vec<HiddenBucketCheckpointV1>,
    pub cursors: Vec<CandidateCursorV1>,
    pub completed: Vec<Option<serde_json::Value>>,
}
#[derive(Serialize, Deserialize)]
pub(crate) struct CheckpointFileV1 {
    pub path: String,
    pub sha256: String,
}
#[derive(Serialize, Deserialize)]
pub(crate) struct CheckpointPublicationV1 {
    pub format: String,
    pub identity: String,
    pub generation: u64,
    pub files: Vec<CheckpointFileV1>,
}
pub(crate) const DAY_CHECKPOINT_FORMAT: &str = "original-parquet-day-checkpoint-v1";
pub(crate) fn write_cbor(path: &Path, value: &impl Serialize) -> Result<(), MaterialError> {
    std::fs::create_dir_all(path.parent().unwrap())?;
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)?;
    ciborium::ser::into_writer(value, &mut file)?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}
pub(crate) fn hash_file(path: &Path) -> Result<String, MaterialError> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        digest.update(&buffer[..n]);
    }
    Ok(format!("{:x}", digest.finalize()))
}
pub(crate) fn read_publication(
    root: &Path,
    identity: &str,
) -> Result<Option<(CheckpointPublicationV1, DayCheckpointV1)>, MaterialError> {
    let pointer = root.join("checkpoint.json");
    if !pointer.exists() {
        return Ok(None);
    };
    let published: CheckpointPublicationV1 =
        serde_json::from_reader(std::fs::File::open(&pointer)?)?;
    if published.format != DAY_CHECKPOINT_FORMAT || published.identity != identity {
        return Err("日检查点与输入、配置或版本不符".into());
    }
    let directory = root.join(format!("checkpoint-{:020}", published.generation));
    for file in &published.files {
        let path = directory.join(&file.path);
        if path.parent() != Some(directory.as_path()) || hash_file(&path)? != file.sha256 {
            return Err("完整日检查点文件身份不符".into());
        }
    }
    let saved: DayCheckpointV1 =
        ciborium::de::from_reader(std::fs::File::open(directory.join("day.cbor"))?)?;
    if saved.format != DAY_CHECKPOINT_FORMAT
        || saved.identity != identity
        || saved.generation != published.generation
    {
        return Err("日检查点发布引用不符".into());
    }
    Ok(Some((published, saved)))
}
pub(crate) fn publish(
    root: &Path,
    identity: &str,
    generation: u64,
    candidates: usize,
    completed: &[Option<serde_json::Value>],
) -> Result<(), MaterialError> {
    let previous = if root.join("checkpoint.json").exists() {
        Some(serde_json::from_reader::<_, CheckpointPublicationV1>(
            std::fs::File::open(root.join("checkpoint.json"))?,
        )?)
    } else {
        None
    };
    let directory = root.join(format!("checkpoint-{generation:020}"));
    let mut files = vec![CheckpointFileV1 {
        path: "day.cbor".into(),
        sha256: hash_file(&directory.join("day.cbor"))?,
    }];
    for (index, complete) in completed.iter().enumerate().take(candidates) {
        if complete.is_none() {
            let path = format!("candidate-{index:06}.cbor");
            files.push(CheckpointFileV1 {
                sha256: hash_file(&directory.join(&path))?,
                path,
            });
        }
    }
    std::fs::File::open(&directory)?.sync_all()?;
    let pointer = root.join(".checkpoint.json.tmp");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&pointer)?;
    serde_json::to_writer(
        &mut file,
        &CheckpointPublicationV1 {
            format: DAY_CHECKPOINT_FORMAT.into(),
            identity: identity.into(),
            generation,
            files,
        },
    )?;
    file.flush()?;
    file.sync_all()?;
    std::fs::rename(pointer, root.join("checkpoint.json"))?;
    std::fs::File::open(root)?.sync_all()?;
    // 新引用耐久之后，上一完整点已失去消费者。仅回收其清单列明的准确文件。
    if let Some(previous) = previous {
        if previous.format == DAY_CHECKPOINT_FORMAT
            && previous.identity == identity
            && previous.generation != generation
        {
            let old = root.join(format!("checkpoint-{:020}", previous.generation));
            for file in previous.files {
                let path = old.join(file.path);
                if path.parent() == Some(old.as_path()) && path.is_file() {
                    std::fs::remove_file(path)?;
                }
            }
            if old.is_dir() {
                std::fs::File::open(&old)?.sync_all()?;
                if old.read_dir()?.next().is_none() {
                    std::fs::remove_dir(old)?;
                }
            }
            std::fs::File::open(root)?.sync_all()?;
        }
    }
    Ok(())
}
