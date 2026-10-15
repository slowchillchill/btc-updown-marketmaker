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
    cell::RefCell,
    collections::VecDeque,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    rc::Rc,
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
    pub ready: Option<(u64, String)>,
    pub complete: Option<serde_json::Value>,
    pub source_health: Option<bool>,
    pub requirements: Option<contracts::PublicObservationRequirementsV1>,
    pub runtime_started: bool,
}
#[derive(Clone)]
pub(crate) struct DayFailureSignalV1 {
    pub source: crate::day::DayFailureSourceV1,
    pub message: String,
}
pub(crate) struct DayBufferV1 {
    pub public_base: usize,
    pub public: VecDeque<PublicGroupDeliveryV1>,
    pub hidden_base: usize,
    pub hidden: VecDeque<AcceptedHiddenStepBucketV1>,
    pub candidates: Vec<CandidateProgressV1>,
    pub production: ProductionV1,
    pub requested: Option<u64>,
    pub research_summary: bool,
    pub budget_stop_requested: bool,
    pub budget_stopped: bool,
    pub generation: u64,
    pub failure: Option<DayFailureSignalV1>,
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
                        requirements: None,
                        runtime_started: false,
                    })
                    .collect(),
                production: ProductionV1::Running,
                requested: None,
                research_summary: false,
                budget_stop_requested: false,
                budget_stopped: false,
                generation: 0,
                failure: None,
            }),
            Condvar::new(),
        )))
    }
    /// 移除原安全消费前缀；调用者在释放 Hub 锁后析构临时引用。
    pub fn compact(
        buffer: &mut DayBufferV1,
    ) -> (Vec<PublicGroupDeliveryV1>, Vec<AcceptedHiddenStepBucketV1>) {
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
        let retired_public = buffer.public.drain(..public - buffer.public_base).collect();
        let retired_hidden = buffer.hidden.drain(..hidden - buffer.hidden_base).collect();
        buffer.public_base = public;
        buffer.hidden_base = hidden;
        (retired_public, retired_hidden)
    }
    pub fn finish(
        &self,
        index: usize,
        result: &Result<HistoricalDayCandidateResultV1, MaterialError>,
        run_id: &str,
    ) {
        let (mut buffer, cv) = (self.0.0.lock().unwrap(), &self.0.1);
        buffer.candidates[index].active = false;
        if let Ok(result) = result {
            if result.outcome == crate::day::CandidateOutcomeV1::Complete
                && result.report["end_reason"] != "BudgetStop"
            {
                buffer.candidates[index].complete = Some(result.report.clone());
            }
        } else if let Err(error) = result {
            if buffer.failure.is_none() {
                buffer.failure = Some(DayFailureSignalV1 {
                    source: crate::day::DayFailureSourceV1::Candidate {
                        index,
                        run_id: run_id.into(),
                    },
                    message: error.to_string(),
                });
            }
        }
        let retired = Self::compact(&mut buffer);
        cv.notify_all();
        drop(buffer);
        drop(retired);
    }
}
// 仅由一个候选线程持有；领取不推进共享回收位置。
const DAY_DELIVERY_BATCH: usize = 64;
pub(crate) struct DayLocalDeliveryCacheV1 {
    public: VecDeque<PublicGroupDeliveryV1>,
    hidden: VecDeque<AcceptedHiddenStepBucketV1>,
    public_end: usize,
    hidden_end: usize,
    times: crate::timing::DeliveryTimes,
}
impl DayLocalDeliveryCacheV1 {
    pub fn shared(hub: &DayHubV1, index: usize) -> Rc<RefCell<Self>> {
        let buffer = hub.0.0.lock().unwrap();
        let cursor = &buffer.candidates[index].cursor;
        Rc::new(RefCell::new(Self {
            public: VecDeque::new(),
            hidden: VecDeque::new(),
            public_end: cursor.public,
            hidden_end: cursor.hidden,
            times: cursor.times.clone(),
        }))
    }
    fn publish(&self, buffer: &mut DayBufferV1, index: usize) {
        let cursor = &mut buffer.candidates[index].cursor;
        cursor.public = self.public_end - self.public.len();
        cursor.hidden = self.hidden_end - self.hidden.len();
        cursor.times.last = self.times.last;
    }
}
pub(crate) struct DayPublicSourceV1 {
    pub hub: DayHubV1,
    pub index: usize,
    pub restored: VecDeque<PublicGroupDeliveryV1>,
    delivery: Rc<RefCell<DayLocalDeliveryCacheV1>>,
    failed: bool,
    #[cfg(feature = "stage-timing")]
    stages: crate::stage_timing::StageTimings,
}
impl DayPublicSourceV1 {
    pub fn new(
        hub: DayHubV1,
        index: usize,
        restored: VecDeque<PublicGroupDeliveryV1>,
        delivery: Rc<RefCell<DayLocalDeliveryCacheV1>>,
    ) -> Self {
        Self {
            hub,
            index,
            restored,
            delivery,
            failed: false,
            #[cfg(feature = "stage-timing")]
            stages: crate::stage_timing::StageTimings::new(format!(
                "candidate_public_transport:{index}"
            )),
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
        loop {
            let group = self.delivery.borrow_mut().public.pop_front();
            if let Some(mut group) = group {
                let time = self
                    .delivery
                    .borrow_mut()
                    .times
                    .execution_time_for_visible_group(
                        &group.prepared.reference,
                        group.delivered_at_utc_ms,
                    );
                return Some(time.map(|at| {
                    group.delivered_at_utc_ms = at;
                    group
                }));
            }
            #[cfg(feature = "stage-timing")]
            let lock_started = std::time::Instant::now();
            let mut buffer = self.hub.0.0.lock().unwrap();
            #[cfg(feature = "stage-timing")]
            self.stages.record(
                crate::stage_timing::Stage::CandidateRefillLockWait,
                lock_started.elapsed(),
            );
            #[cfg(feature = "stage-timing")]
            let reclaim_started = std::time::Instant::now();
            #[cfg(feature = "stage-timing")]
            let bases = (buffer.public_base, buffer.hidden_base);
            self.delivery.borrow().publish(&mut buffer, self.index);
            let retired = measure_stage!(
                self.stages,
                CandidateReclaimDetach,
                DayHubV1::compact(&mut buffer)
            );
            self.hub.0.1.notify_all();
            #[cfg(feature = "stage-timing")]
            {
                self.stages.record(
                    crate::stage_timing::Stage::CandidateCursorPublishAndReclaim,
                    reclaim_started.elapsed(),
                );
                self.stages.add_delivery_work(
                    0,
                    buffer.public_base - bases.0,
                    buffer.hidden_base - bases.1,
                );
            }
            // 发布仍在原锁内完成；先解锁并释放本批回收引用，再锁定当前可领取材料。
            drop(buffer);
            measure_stage!(self.stages, CandidateReclaimedDrop, drop(retired));
            #[cfg(feature = "stage-timing")]
            let lock_started = std::time::Instant::now();
            let mut buffer = self.hub.0.0.lock().unwrap();
            #[cfg(feature = "stage-timing")]
            self.stages.record(
                crate::stage_timing::Stage::CandidateRefillLockWait,
                lock_started.elapsed(),
            );
            #[cfg(feature = "stage-timing")]
            let mut emit_transport = false;
            loop {
                #[cfg(feature = "stage-timing")]
                let claim_started = std::time::Instant::now();
                let mut delivery = self.delivery.borrow_mut();
                let position = delivery.public_end;
                delivery.public.extend(
                    buffer
                        .public
                        .iter()
                        .skip(position - buffer.public_base)
                        .take(DAY_DELIVERY_BATCH)
                        .cloned(),
                );
                delivery.public_end += delivery.public.len();
                #[cfg(feature = "stage-timing")]
                {
                    self.stages.record(
                        crate::stage_timing::Stage::CandidateBatchClaim,
                        claim_started.elapsed(),
                    );
                    emit_transport |= self.stages.add_delivery_work(delivery.public.len(), 0, 0);
                }
                if !delivery.public.is_empty() {
                    buffer.candidates[self.index].waiting = false;
                    break;
                }
                drop(delivery);
                if let Some(error) = buffer.failure.clone() {
                    self.failed = true;
                    return Some(Err(SimRunActorErrorV2::PublicConsumptionSource(
                        error.message,
                    )));
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
                if !buffer.candidates[self.index].waiting {
                    buffer.candidates[self.index].waiting = true;
                    self.hub.0.1.notify_all();
                }
                #[cfg(test)]
                let observed_wait = tests::empty_wait_started();
                #[cfg(feature = "stage-timing")]
                let wait_started = std::time::Instant::now();
                buffer = self.hub.0.1.wait(buffer).unwrap();
                #[cfg(feature = "stage-timing")]
                self.stages.record(
                    crate::stage_timing::Stage::CandidateDataWait,
                    wait_started.elapsed(),
                );
                #[cfg(test)]
                tests::empty_wait_returned(observed_wait);
            }
            drop(buffer);
            #[cfg(feature = "stage-timing")]
            if emit_transport {
                self.stages.emit_transport();
            }
        }
    }
}
pub(crate) struct DayHiddenSourceV1 {
    pub hub: DayHubV1,
    pub index: usize,
    delivery: Rc<RefCell<DayLocalDeliveryCacheV1>>,
    failed: bool,
    #[cfg(feature = "stage-timing")]
    stages: crate::stage_timing::StageTimings,
}
impl DayHiddenSourceV1 {
    pub fn new(
        hub: DayHubV1,
        index: usize,
        delivery: Rc<RefCell<DayLocalDeliveryCacheV1>>,
    ) -> Self {
        Self {
            hub,
            index,
            delivery,
            failed: false,
            #[cfg(feature = "stage-timing")]
            stages: crate::stage_timing::StageTimings::new(format!(
                "candidate_hidden_transport:{index}"
            )),
        }
    }
}
impl Iterator for DayHiddenSourceV1 {
    type Item = Result<AcceptedHiddenStepBucketV1, MaterialError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            let bucket = self.delivery.borrow_mut().hidden.pop_front();
            if let Some(bucket) = bucket {
                return Some(Ok(bucket));
            }
            #[cfg(feature = "stage-timing")]
            let lock_started = std::time::Instant::now();
            let mut buffer = self.hub.0.0.lock().unwrap();
            #[cfg(feature = "stage-timing")]
            self.stages.record(
                crate::stage_timing::Stage::CandidateRefillLockWait,
                lock_started.elapsed(),
            );
            #[cfg(feature = "stage-timing")]
            let reclaim_started = std::time::Instant::now();
            #[cfg(feature = "stage-timing")]
            let bases = (buffer.public_base, buffer.hidden_base);
            self.delivery.borrow().publish(&mut buffer, self.index);
            let retired = measure_stage!(
                self.stages,
                CandidateReclaimDetach,
                DayHubV1::compact(&mut buffer)
            );
            self.hub.0.1.notify_all();
            #[cfg(feature = "stage-timing")]
            {
                self.stages.record(
                    crate::stage_timing::Stage::CandidateCursorPublishAndReclaim,
                    reclaim_started.elapsed(),
                );
                self.stages.add_delivery_work(
                    0,
                    buffer.public_base - bases.0,
                    buffer.hidden_base - bases.1,
                );
            }
            // 发布仍在原锁内完成；先解锁并释放本批回收引用，再锁定当前可领取材料。
            drop(buffer);
            measure_stage!(self.stages, CandidateReclaimedDrop, drop(retired));
            #[cfg(feature = "stage-timing")]
            let lock_started = std::time::Instant::now();
            let mut buffer = self.hub.0.0.lock().unwrap();
            #[cfg(feature = "stage-timing")]
            self.stages.record(
                crate::stage_timing::Stage::CandidateRefillLockWait,
                lock_started.elapsed(),
            );
            #[cfg(feature = "stage-timing")]
            let mut emit_transport = false;
            loop {
                #[cfg(feature = "stage-timing")]
                let claim_started = std::time::Instant::now();
                let mut delivery = self.delivery.borrow_mut();
                let position = delivery.hidden_end;
                delivery.hidden.extend(
                    buffer
                        .hidden
                        .iter()
                        .skip(position - buffer.hidden_base)
                        .take(DAY_DELIVERY_BATCH)
                        .cloned(),
                );
                delivery.hidden_end += delivery.hidden.len();
                #[cfg(feature = "stage-timing")]
                {
                    self.stages.record(
                        crate::stage_timing::Stage::CandidateBatchClaim,
                        claim_started.elapsed(),
                    );
                    emit_transport |= self.stages.add_delivery_work(delivery.hidden.len(), 0, 0);
                }
                if !delivery.hidden.is_empty() {
                    buffer.candidates[self.index].waiting = false;
                    break;
                }
                drop(delivery);
                if let Some(error) = buffer.failure.clone() {
                    self.failed = true;
                    return Some(Err(error.message.into()));
                }
                match &buffer.production {
                    ProductionV1::Ended => return None,
                    ProductionV1::Failed(error) => {
                        self.failed = true;
                        return Some(Err(error.clone().into()));
                    }
                    ProductionV1::Running => {}
                }
                if !buffer.candidates[self.index].waiting {
                    buffer.candidates[self.index].waiting = true;
                    self.hub.0.1.notify_all();
                }
                #[cfg(test)]
                let observed_wait = tests::empty_wait_started();
                #[cfg(feature = "stage-timing")]
                let wait_started = std::time::Instant::now();
                buffer = self.hub.0.1.wait(buffer).unwrap();
                #[cfg(feature = "stage-timing")]
                self.stages.record(
                    crate::stage_timing::Stage::CandidateDataWait,
                    wait_started.elapsed(),
                );
                #[cfg(test)]
                tests::empty_wait_returned(observed_wait);
            }
            drop(buffer);
            #[cfg(feature = "stage-timing")]
            if emit_transport {
                self.stages.emit_transport();
            }
        }
    }
}
#[derive(Serialize, Deserialize)]
pub(crate) struct CandidateCheckpointV1<D> {
    #[serde(with = "checkpoint_bytes")]
    driver: Vec<u8>,
    #[serde(with = "checkpoint_bytes")]
    ledger: Vec<u8>,
    #[serde(with = "checkpoint_bytes")]
    shared: Vec<u8>,
    #[serde(with = "checkpoint_bytes")]
    strategy: Vec<u8>,
    state: RuntimeLoopCheckpointV1<D>,
    pub prefetched: Option<checkpoint::Delivery>,
    next_hidden: Option<HiddenBucketCheckpointV1>,
}
pub(crate) struct DayCandidateCheckpointV1<D> {
    delivery: Rc<RefCell<DayLocalDeliveryCacheV1>>,
    pub hub: DayHubV1,
    pub index: usize,
    pub root: PathBuf,
    pub resumed: Option<CandidateCheckpointV1<D>>,
    pub hidden: Option<AcceptedHiddenStepBucketV1>,
    pub every_events: Option<u64>,
    events: u64,
    #[cfg(feature = "stage-timing")]
    stages: crate::stage_timing::StageTimings,
}
impl<D> DayCandidateCheckpointV1<D> {
    pub fn new(
        hub: DayHubV1,
        index: usize,
        root: PathBuf,
        resumed: Option<CandidateCheckpointV1<D>>,
        every_events: Option<u64>,
        delivery: Rc<RefCell<DayLocalDeliveryCacheV1>>,
    ) -> Self {
        Self {
            hub,
            index,
            root,
            resumed,
            hidden: None,
            every_events,
            events: 0,
            delivery,
            #[cfg(feature = "stage-timing")]
            stages: crate::stage_timing::StageTimings::new(format!("candidate_checkpoint:{index}")),
        }
    }
}
impl<S> RuntimeCheckpointV1<S, DayPublicSourceV1> for DayCandidateCheckpointV1<S::Diagnostics>
where
    S: CheckpointStrategyV1,
    S::Diagnostics: Serialize + serde::de::DeserializeOwned,
{
    fn research_enabled(&self) -> bool {
        self.hub.0.0.lock().unwrap().research_summary
    }
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
    ) -> Result<RuntimeCheckpointControlV1, MaterialError> {
        self.events += 1;
        #[cfg(feature = "stage-timing")]
        let sample = self.stages.next_diagnostic_sample();
        let generation = measure_sampled_stage!(
            self.stages,
            sample,
            CandidateControlAndCursorPublishSampled,
            {
                let mut buffer = measure_sampled_stage!(
                    self.stages,
                    sample,
                    CandidateControlLockWaitSampled,
                    self.hub.0.0.lock().unwrap()
                );
                self.delivery.borrow().publish(&mut buffer, self.index);
                if buffer.requested.is_none()
                    && self.every_events.is_some_and(|n| n > 0 && self.events >= n)
                {
                    buffer.requested = Some(buffer.generation);
                    self.hub.0.1.notify_all();
                }
                let generation = buffer.requested.filter(|g| {
                    buffer.candidates[self.index]
                        .ready
                        .as_ref()
                        .map(|(g, _)| *g)
                        != Some(*g)
                });
                (generation, buffer.budget_stopped)
            }
        );
        let (Some(generation), _) = generation else {
            return Ok(if generation.1 {
                RuntimeCheckpointControlV1::SavedThenStop
            } else {
                RuntimeCheckpointControlV1::Continue
            });
        };
        #[cfg(feature = "stage-timing")]
        let encode_started = std::time::Instant::now();
        #[cfg(feature = "stage-timing")]
        let timings = &mut self.stages;
        let mut execution = Vec::new();
        measure_stage!(
            timings,
            EncodeSimDriver,
            driver.write_checkpoint(&mut execution)
        )?;
        let mut ledger = Vec::new();
        measure_stage!(
            timings,
            EncodeLedger,
            shared.ledger().write_checkpoint(&mut ledger)
        )?;
        let mut registry = Vec::new();
        measure_stage!(
            timings,
            EncodeSharedExecution,
            shared.write_checkpoint(&mut registry)
        )?;
        let mut strategy_state = Vec::new();
        measure_stage!(
            timings,
            EncodeStrategy,
            strategy.write_strategy_checkpoint(&mut strategy_state)
        )?;
        #[cfg(feature = "stage-timing")]
        eprintln!(
            "checkpoint_subsystem_bytes={}",
            serde_json::json!({"candidate_index":self.index,"generation":generation,"driver":execution.len(),"ledger":ledger.len(),"shared_execution":registry.len(),"strategy":strategy_state.len()})
        );
        #[derive(Serialize)]
        struct Saved<'a, D> {
            #[serde(with = "checkpoint_bytes")]
            driver: Vec<u8>,
            #[serde(with = "checkpoint_bytes")]
            ledger: Vec<u8>,
            #[serde(with = "checkpoint_bytes")]
            shared: Vec<u8>,
            #[serde(with = "checkpoint_bytes")]
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
        #[cfg(feature = "stage-timing")]
        self.stages.record(
            crate::stage_timing::Stage::EncodeSubsystemsAndCapture,
            encode_started.elapsed(),
        );
        #[cfg(feature = "stage-timing")]
        let write_started = std::time::Instant::now();
        let sha256 = write_cbor(&path, &saved)?;
        #[cfg(feature = "stage-timing")]
        self.stages.record(
            crate::stage_timing::Stage::EncodeCandidateWriteAndSync,
            write_started.elapsed(),
        );
        #[cfg(feature = "stage-timing")]
        let wait_started = std::time::Instant::now();
        let mut buffer = self.hub.0.0.lock().unwrap();
        buffer.candidates[self.index].ready = Some((generation, sha256));
        self.hub.0.1.notify_all();
        while buffer.requested == Some(generation) && buffer.failure.is_none() {
            buffer = self.hub.0.1.wait(buffer).unwrap();
        }
        if let Some(error) = &buffer.failure {
            return Err(error.message.clone().into());
        }
        #[cfg(feature = "stage-timing")]
        self.stages.record(
            crate::stage_timing::Stage::WaitSharedPublication,
            wait_started.elapsed(),
        );
        self.events = 0;
        #[cfg(feature = "stage-timing")]
        self.stages.emit_checkpoint(generation);
        Ok(if buffer.budget_stopped {
            RuntimeCheckpointControlV1::SavedThenStop
        } else {
            RuntimeCheckpointControlV1::Continue
        })
    }
}
#[derive(Serialize, Deserialize)]
pub(crate) struct DayCheckpointV1<C = HistoricalRuntimeCatalogV1> {
    pub format: String,
    pub identity: String,
    pub generation: u64,
    pub applied: u64,
    pub state: DayCheckpointStateV2<C>,
    pub completed: Vec<Option<serde_json::Value>>,
}
/// 只有所有候选完成才省略运行状态；部分完成仍保留各个消费前沿。
#[derive(Serialize, Deserialize)]
pub(crate) enum DayCheckpointStateV2<C = HistoricalRuntimeCatalogV1> {
    Running {
        #[serde(with = "checkpoint_bytes")]
        core: Vec<u8>,
        #[serde(with = "checkpoint_bytes")]
        producer: Vec<u8>,
        catalog: C,
        public_base: usize,
        public: checkpoint::DeliveryBatch,
        hidden_base: usize,
        hidden: Vec<HiddenBucketCheckpointV1>,
        cursors: Vec<CandidateCursorV1>,
    },
    Complete,
}
/// 内部 CBOR 模块字节，不改变所属模块的正式编码。
mod checkpoint_bytes {
    use serde::{Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(value)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        struct Bytes;
        impl<'de> serde::de::Visitor<'de> for Bytes {
            type Value = Vec<u8>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("模块检查点字节")
            }
            fn visit_bytes<E: serde::de::Error>(self, value: &[u8]) -> Result<Self::Value, E> {
                Ok(value.to_vec())
            }
            fn visit_byte_buf<E: serde::de::Error>(self, value: Vec<u8>) -> Result<Self::Value, E> {
                Ok(value)
            }
        }
        deserializer.deserialize_byte_buf(Bytes)
    }
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
pub(crate) const DAY_CHECKPOINT_FORMAT: &str = "original-parquet-day-checkpoint-v5";
struct DigestWriter<W> {
    inner: W,
    digest: Sha256,
}
impl<W: Write> Write for DigestWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(bytes)?;
        self.digest.update(&bytes[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
pub(crate) fn write_cbor(path: &Path, value: &impl Serialize) -> Result<String, MaterialError> {
    std::fs::create_dir_all(path.parent().unwrap())?;
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)?;
    let mut writer = BufWriter::new(DigestWriter {
        inner: file,
        digest: Sha256::new(),
    });
    ciborium::ser::into_writer(value, &mut writer)?;
    writer.flush()?;
    writer.get_ref().inner.sync_all()?;
    Ok(format!("{:x}", writer.get_ref().digest.clone().finalize()))
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
/// 摘要只累计实际读入字节。解码后继续读至EOF，再允许恢复任何写入owner。
struct DigestReader<R> {
    inner: R,
    digest: Sha256,
}
impl<R: Read> Read for DigestReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(bytes)?;
        self.digest.update(&bytes[..n]);
        Ok(n)
    }
}
fn read_verified_cbor<T: serde::de::DeserializeOwned>(
    path: &Path,
    expected: &str,
) -> Result<T, MaterialError> {
    let mut reader = BufReader::new(DigestReader {
        inner: std::fs::File::open(path)?,
        digest: Sha256::new(),
    });
    let value = ciborium::de::from_reader(&mut reader)?;
    std::io::copy(&mut reader, &mut std::io::sink())?;
    if format!("{:x}", reader.get_ref().digest.clone().finalize()) != expected {
        return Err("完整日检查点文件身份不符".into());
    }
    Ok(value)
}
pub(crate) fn read_publication(
    root: &Path,
    identity: &str,
) -> Result<
    Option<(
        CheckpointPublicationV1,
        DayCheckpointV1,
        std::collections::BTreeMap<String, Vec<u8>>,
    )>,
    MaterialError,
> {
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
    let mut saved = None;
    let mut candidates = std::collections::BTreeMap::new();
    for file in &published.files {
        let path = directory.join(&file.path);
        if path.parent() != Some(directory.as_path()) {
            return Err("完整日检查点文件身份不符".into());
        }
        if file.path == "day.cbor" {
            saved = Some(read_verified_cbor::<DayCheckpointV1>(&path, &file.sha256)?);
        } else if file.path.starts_with("candidate-") && file.path.ends_with(".cbor") {
            let mut reader = BufReader::new(DigestReader {
                inner: std::fs::File::open(&path)?,
                digest: Sha256::new(),
            });
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes)?;
            if format!("{:x}", reader.get_ref().digest.clone().finalize()) != file.sha256 {
                return Err("完整日检查点文件身份不符".into());
            }
            candidates.insert(file.path.clone(), bytes);
        } else if hash_file(&path)? != file.sha256 {
            return Err("完整日检查点文件身份不符".into());
        }
    }
    let saved = saved.ok_or("完整日检查点缺少day.cbor")?;
    if saved.format != DAY_CHECKPOINT_FORMAT
        || saved.identity != identity
        || saved.generation != published.generation
    {
        return Err("日检查点发布引用不符".into());
    }
    Ok(Some((published, saved, candidates)))
}
pub(crate) fn publish(
    root: &Path,
    identity: &str,
    generation: u64,
    files: Vec<CheckpointFileV1>,
) -> Result<(), MaterialError> {
    let previous = if root.join("checkpoint.json").exists() {
        Some(serde_json::from_reader::<_, CheckpointPublicationV1>(
            std::fs::File::open(root.join("checkpoint.json"))?,
        )?)
    } else {
        None
    };
    let directory = root.join(format!("checkpoint-{generation:020}"));
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::RefCell,
        time::{Duration, Instant},
    };

    #[derive(Default, Serialize)]
    struct EmptyWaitObservation {
        wait_calls: u64,
        returned_waits: u64,
        wait_wall_including_lock_reacquire_ns: u128,
    }
    thread_local! {
        static EMPTY_WAITS: RefCell<Option<EmptyWaitObservation>> = const { RefCell::new(None) };
    }
    pub(super) fn empty_wait_started() -> Option<Instant> {
        EMPTY_WAITS.with(|value| {
            let mut value = value.borrow_mut();
            value.as_mut().map(|v| {
                v.wait_calls += 1;
                Instant::now()
            })
        })
    }
    pub(super) fn empty_wait_returned(started: Option<Instant>) {
        if let Some(started) = started {
            EMPTY_WAITS.with(|value| {
                let mut value = value.borrow_mut();
                let v = value.as_mut().unwrap();
                v.returned_waits += 1;
                v.wait_wall_including_lock_reacquire_ns += started.elapsed().as_nanos();
            });
        }
    }
    #[test]
    fn local_batch_publishes_only_returned_prefix_and_delays_tail_error() {
        let temp = tempfile::tempdir().unwrap();
        let (config, _) = crate::test_common::setup(temp.path());
        let prepared = crate::prepare_historical_run_v1(&config.preparation).unwrap();
        let groups = RecordedPublicGroupSourceV1::open(
            &prepared.public_root,
            1,
            1 << 20,
            crate::timing::DeliveryTimes {
                delays: crate::timing::DelayScheduleV1::Fixed(0),
                last: None,
            },
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        assert!(groups.len() > 2);
        let mut candidate = HistoricalPreparedRunConfigV1::from(&config);
        candidate.timing.public_delivery_delay_ms =
            crate::timing::DelayScheduleV1::Sequence(vec![3, 4]);
        let hub = DayHubV1::new(&[candidate.clone(), candidate]);
        {
            let mut buffer = hub.0.0.lock().unwrap();
            buffer.public.extend(groups.iter().cloned());
            buffer.production = ProductionV1::Ended;
        }
        let delivery = DayLocalDeliveryCacheV1::shared(&hub, 0);
        let mut source = DayPublicSourceV1::new(hub.clone(), 0, VecDeque::new(), delivery.clone());
        let first = source.next().unwrap().unwrap();
        assert_eq!(first.delivered_at_utc_ms, groups[0].delivered_at_utc_ms + 3);
        assert_eq!(
            delivery.borrow().public.len(),
            groups.len().min(DAY_DELIVERY_BATCH) - 1
        );
        {
            let mut buffer = hub.0.0.lock().unwrap();
            assert_eq!(buffer.candidates[0].cursor.public, 0, "领取不提前发布");
            delivery.borrow().publish(&mut buffer, 0);
            assert_eq!(buffer.candidates[0].cursor.public, 1);
            assert_eq!(
                buffer.candidates[0].cursor.times.last,
                Some(first.delivered_at_utc_ms)
            );
            DayHubV1::compact(&mut buffer);
            assert_eq!(buffer.public_base, 0, "慢候选仍持有全部前缀");
        }
        // 原共享游标足以续接；局部未返回尾项无须持久化。
        drop(source);
        drop(delivery);
        let delivery = DayLocalDeliveryCacheV1::shared(&hub, 0);
        let mut source = DayPublicSourceV1::new(hub.clone(), 0, VecDeque::new(), delivery);
        let second = source.next().unwrap().unwrap();
        assert_eq!(second.prepared.reference, groups[1].prepared.reference);
        assert_eq!(
            second.delivered_at_utc_ms,
            (groups[1].delivered_at_utc_ms + 4).max(first.delivered_at_utc_ms)
        );
        assert!(
            source.next().unwrap().is_err(),
            "序列耗尽只在第三项真正交付时报错"
        );
    }
    #[test]
    #[ignore = "#185 有限共享队列回收诊断，手动运行并保留原始计时"]
    fn bounded_history_handoff_observation() {
        let temp = tempfile::tempdir().unwrap();
        let (config, _) = crate::test_common::setup(temp.path());
        let prepared = crate::prepare_historical_run_v1(&config.preparation).unwrap();
        let delivered = RecordedPublicGroupSourceV1::open(
            &prepared.public_root,
            1,
            1 << 20,
            crate::timing::DeliveryTimes {
                delays: crate::timing::DelayScheduleV1::Fixed(0),
                last: None,
            },
        )
        .unwrap()
        .last()
        .unwrap()
        .unwrap();
        let (base, facts) = delivered
            .prepared
            .group
            .observations
            .shared_parts()
            .unwrap();
        let template = base.values().next().unwrap();
        for history in [128, 8192] {
            let mut markets = contracts::MarketObservationSnapshotsV1::new();
            for index in 0..history {
                let mut market = template.as_ref().clone();
                market.market.condition_id = contracts::ConditionId(format!("0x{index:064x}"));
                markets.insert(market.market.condition_id.0.clone(), Arc::new(market));
            }
            let key = markets.keys().next().unwrap().clone();
            let hub = DayHubV1::new(&[HistoricalPreparedRunConfigV1::from(&config)]);
            for index in 0..4096 {
                let mut changed = markets[&key].as_ref().clone();
                changed.active.value = Some(index % 2 == 0);
                markets.insert(key.clone(), Arc::new(changed));
                let mut group = delivered.prepared.group.as_ref().clone();
                group.observations = contracts::MarketObservationSetV1::shared(
                    markets.clone(),
                    facts.as_ref().clone(),
                );
                // 只运行真实运输/回收入口，不将这份重复引用诊断送入SIM业务应用。
                hub.0
                    .0
                    .lock()
                    .unwrap()
                    .public
                    .push_back(PublicGroupDeliveryV1 {
                        prepared: Arc::new(PreparedPublicGroupV1 {
                            reference: delivered.prepared.reference.clone(),
                            group: Arc::new(group),
                            pm_inputs: vec![],
                            pm_contexts: vec![],
                        }),
                        delivered_at_utc_ms: delivered.delivered_at_utc_ms,
                    });
            }
            hub.0.0.lock().unwrap().production = ProductionV1::Ended;
            let delivery = DayLocalDeliveryCacheV1::shared(&hub, 0);
            let mut source = DayPublicSourceV1::new(hub.clone(), 0, VecDeque::new(), delivery);
            let started = Instant::now();
            let mut returned = 0;
            for group in source.by_ref() {
                drop(group.unwrap());
                returned += 1;
            }
            assert_eq!(returned, 4096);
            let buffer = hub.0.0.lock().unwrap();
            assert!(buffer.public.is_empty());
            eprintln!(
                "handoff_history_diagnostic={}",
                serde_json::json!({"history_markets":history,
                "groups":returned,"transport_wall_ns":started.elapsed().as_nanos(),"retained_public":buffer.public.len()})
            );
            drop(buffer);
            drop(source);
        }
    }
    #[test]
    #[ignore = "#185 explicit bounded empty-Hub wait observation, not a throughput gate"]
    fn two_empty_day_consumers_observe_existing_wakeups() {
        let hub = DayHubV1::new(&[]);
        {
            let mut buffer = hub.0.0.lock().unwrap();
            for _ in 0..2 {
                buffer.candidates.push(CandidateProgressV1 {
                    cursor: CandidateCursorV1 {
                        public: 0,
                        hidden: 0,
                        times: crate::timing::DeliveryTimes {
                            delays: crate::timing::DelayScheduleV1::Fixed(0),
                            last: None,
                        },
                    },
                    active: true,
                    waiting: false,
                    ready: None,
                    complete: None,
                    source_health: None,
                    requirements: None,
                    runtime_started: false,
                });
            }
        }
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let started = Instant::now();
        let observations = std::thread::scope(|scope| {
            let workers = (0..2)
                .map(|index| {
                    let hub = hub.clone();
                    let barrier = barrier.clone();
                    scope.spawn(move || {
                        EMPTY_WAITS.with(|value| {
                            *value.borrow_mut() = Some(EmptyWaitObservation::default())
                        });
                        barrier.wait();
                        // 两条原 next/Condvar 路径共享空Hub；不制造输入或业务超时。
                        if index == 0 {
                            assert!(
                                DayPublicSourceV1::new(
                                    hub.clone(),
                                    index,
                                    VecDeque::new(),
                                    DayLocalDeliveryCacheV1::shared(&hub, index)
                                )
                                .next()
                                .is_none()
                            );
                        } else {
                            assert!(
                                DayHiddenSourceV1::new(
                                    hub.clone(),
                                    index,
                                    DayLocalDeliveryCacheV1::shared(&hub, index)
                                )
                                .next()
                                .is_none()
                            );
                        }
                        EMPTY_WAITS.with(|value| value.borrow_mut().take().unwrap())
                    })
                })
                .collect::<Vec<_>>();
            barrier.wait();
            std::thread::sleep(Duration::from_millis(200));
            {
                let mut buffer = hub.0.0.lock().unwrap();
                buffer.production = ProductionV1::Ended;
                hub.0.1.notify_all();
            }
            workers
                .into_iter()
                .map(|w| w.join().unwrap())
                .collect::<Vec<_>>()
        });
        for v in &observations {
            assert_eq!(v.wait_calls, v.returned_waits);
        }
        let output = serde_json::json!({
            "format":"issue185-empty-hub-waits-v1", "empty_observation_requested_ms":200,
            "wall_ns":started.elapsed().as_nanos(), "sources":["public","hidden"],
            "observations":observations, "termination":"original ProductionV1::Ended + notify_all",
            "limits":"test-only counters; wait wall includes lock reacquire, not CPU; no minimum wake count gate or extrapolated savings"
        });
        println!("empty_hub_observation: {output}");
        if let Ok(path) = std::env::var("ISSUE185_WAIT_OUTPUT") {
            std::fs::write(path, serde_json::to_vec_pretty(&output).unwrap()).unwrap();
        }
    }
    #[test]
    fn checkpoint_read_digest_includes_the_complete_decode_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("value.cbor");
        let digest = write_cbor(&path, &42u64).unwrap();
        assert_eq!(read_verified_cbor::<u64>(&path, &digest).unwrap(), 42);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"remaining-content")
            .unwrap();
        assert!(read_verified_cbor::<u64>(&path, &digest).is_err());
        // 原格式允许解码后剩余字节；它们必须同样参与整个文件摘要。
        assert_eq!(
            read_verified_cbor::<u64>(&path, &hash_file(&path).unwrap()).unwrap(),
            42
        );
    }
    #[test]
    fn checkpoint_digest_counts_partial_writes_and_propagates_failures() {
        struct Partial {
            bytes: Vec<u8>,
            failed: bool,
        }
        impl Write for Partial {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if self.failed {
                    return Err(std::io::Error::other("原写入失败"));
                }
                let n = bytes.len().min(3);
                self.bytes.extend_from_slice(&bytes[..n]);
                Ok(n)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Err(std::io::Error::other("原刷新失败"))
            }
        }
        let mut writer = DigestWriter {
            inner: Partial {
                bytes: vec![],
                failed: false,
            },
            digest: Sha256::new(),
        };
        writer.write_all(b"checkpoint").unwrap();
        let expected = Sha256::digest(&writer.inner.bytes);
        assert_eq!(writer.digest.clone().finalize(), expected);
        writer.inner.failed = true;
        assert!(writer.write(b"not committed").is_err());
        assert_eq!(writer.digest.clone().finalize(), expected);
        assert!(writer.flush().is_err());
        let tmp = tempfile::tempdir().unwrap();
        #[derive(Serialize, Deserialize)]
        struct Saved {
            #[serde(with = "checkpoint_bytes")]
            bytes: Vec<u8>,
        }
        let path = tmp.path().join("state.cbor");
        let digest = write_cbor(
            &path,
            &Saved {
                bytes: vec![0, 24, 255],
            },
        )
        .unwrap();
        assert_eq!(digest, hash_file(&path).unwrap());
        let value: ciborium::Value =
            ciborium::from_reader(std::fs::File::open(&path).unwrap()).unwrap();
        assert!(
            matches!(&value.as_map().unwrap()[0].1,ciborium::Value::Bytes(b) if b == &[0,24,255])
        );
        let saved: Saved = ciborium::from_reader(std::fs::File::open(&path).unwrap()).unwrap();
        assert_eq!(saved.bytes, vec![0, 24, 255]);
        assert!(write_cbor(&path, &saved).is_err(), "原create_new边界保持");
    }
}

#[cfg(test)]
mod version_boundary_tests {
    use super::*;
    #[test]
    fn old_day_materials_are_not_loaded_as_the_new_position_format() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("checkpoint.json"),
            br#"{"format":"original-parquet-day-checkpoint-v4","identity":"same-input","generation":0,"files":[]}"#).unwrap();
        let error = read_publication(temp.path(), "same-input").err().unwrap();
        assert_eq!(error.to_string(), "日检查点与输入、配置或版本不符");
    }
}
