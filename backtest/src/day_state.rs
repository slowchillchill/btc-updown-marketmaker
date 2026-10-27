//! 日入口的消费前沿、候选启动和预算退出协调。
use crate::{run::*, *};
use sim_system::execution::SimRunActorErrorV2;
use sim_system::public_consumption::*;
use std::{
    cell::RefCell,
    collections::VecDeque,
    rc::Rc,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Clone)]
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
    pub source_health: Option<bool>,
    pub source_progress: Option<bool>,
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
    pub public: VecDeque<DayPublicDeliveryV1>,
    pub retained_payload_bytes: u64,
    pub peak_payload_bytes: u64,
    pub public_payload_sizes: VecDeque<u64>,
    pub hidden_payload_sizes: VecDeque<u64>,
    pub producer_wait_seconds: f64,
    pub hidden_base: usize,
    pub hidden: VecDeque<AcceptedHiddenStepBucketV1>,
    pub candidates: Vec<CandidateProgressV1>,
    pub production: ProductionV1,
    pub research_summary: bool,
    pub failure: Option<DayFailureSignalV1>,
}
#[derive(Clone)]
pub(crate) struct DayHubV1(pub Arc<(Mutex<DayBufferV1>, DaySignalsV1)>);
pub(crate) struct DaySignalsV1 {
    pub public: Condvar,
    pub hidden: Condvar,
    pub control: Condvar,
    pub budget_stop_requested: AtomicBool,
}
impl DaySignalsV1 {
    pub fn notify_all(&self) {
        self.public.notify_all();
        self.hidden.notify_all();
        self.control.notify_all();
    }
}
impl DayHubV1 {
    pub fn new(candidates: &[HistoricalPreparedRunConfigV1]) -> Self {
        Self(Arc::new((
            Mutex::new(DayBufferV1 {
                public_base: 0,
                public: Default::default(),
                retained_payload_bytes: 0,
                peak_payload_bytes: 0,
                public_payload_sizes: Default::default(),
                hidden_payload_sizes: Default::default(),
                producer_wait_seconds: 0.0,
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
                        source_health: None,
                        source_progress: None,
                        requirements: None,
                        runtime_started: false,
                    })
                    .collect(),
                production: ProductionV1::Running,
                research_summary: false,
                failure: None,
            }),
            DaySignalsV1 {
                public: Condvar::new(),
                hidden: Condvar::new(),
                control: Condvar::new(),
                budget_stop_requested: AtomicBool::new(false),
            },
        )))
    }
    /// 移除原安全消费前缀；调用者在释放 Hub 锁后析构临时引用。
    pub fn compact(
        buffer: &mut DayBufferV1,
    ) -> (Vec<DayPublicDeliveryV1>, Vec<AcceptedHiddenStepBucketV1>) {
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
        let public_retired = public - buffer.public_base;
        let hidden_retired = hidden - buffer.hidden_base;
        let released_public: u64 = buffer
            .public_payload_sizes
            .drain(..public_retired.min(buffer.public_payload_sizes.len()))
            .sum();
        let released_hidden: u64 = buffer
            .hidden_payload_sizes
            .drain(..hidden_retired.min(buffer.hidden_payload_sizes.len()))
            .sum();
        buffer.retained_payload_bytes = buffer
            .retained_payload_bytes
            .saturating_sub(released_public.saturating_add(released_hidden));
        let retired_public = buffer.public.drain(..public_retired).collect();
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
        if let Err(error) = result {
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
#[derive(Clone)]
pub(crate) struct DayPublicDeliveryV1 {
    pub delivery: PublicGroupDeliveryV1,
    pub initialization: Option<(bool, bool)>,
    pub research_public: Option<Arc<HistoricalPublicEvidenceV1>>,
}
impl From<PublicGroupDeliveryV1> for DayPublicDeliveryV1 {
    fn from(delivery: PublicGroupDeliveryV1) -> Self {
        Self {
            delivery,
            initialization: None,
            research_public: None,
        }
    }
}
pub(crate) struct DayLocalDeliveryCacheV1 {
    public: VecDeque<DayPublicDeliveryV1>,
    hidden: VecDeque<AcceptedHiddenStepBucketV1>,
    initialization: Option<(u64, bool, bool)>,
    research_public: Option<(u64, Arc<HistoricalPublicEvidenceV1>)>,
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
            initialization: None,
            research_public: None,
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
    delivery: Rc<RefCell<DayLocalDeliveryCacheV1>>,
    failed: bool,
    #[cfg(feature = "stage-timing")]
    stages: crate::stage_timing::StageTimings,
}
impl DayPublicSourceV1 {
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
            stages: crate::stage_timing::StageTimings::new_transport(format!(
                "candidate_public_transport:{index}"
            )),
        }
    }
}
impl Iterator for DayPublicSourceV1 {
    type Item = Result<PublicGroupDeliveryV1, SimRunActorErrorV2>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            let group = self.delivery.borrow_mut().public.pop_front();
            if let Some(item) = group {
                let mut group = item.delivery;
                self.delivery.borrow_mut().research_public = item
                    .research_public
                    .map(|v| (group.prepared.group.group_position, v));
                self.delivery.borrow_mut().initialization =
                    item.initialization.map(|(initial, dependency)| {
                        (group.prepared.group.group_position, initial, dependency)
                    });
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
            let lock_started = self
                .stages
                .start(crate::stage_timing::Stage::CandidateRefillLockWait);
            let mut buffer = self.hub.0.0.lock().unwrap();
            #[cfg(feature = "stage-timing")]
            self.stages.finish(
                crate::stage_timing::Stage::CandidateRefillLockWait,
                lock_started,
            );
            #[cfg(feature = "stage-timing")]
            let reclaim_started = self
                .stages
                .start(crate::stage_timing::Stage::CandidateCursorPublishAndReclaim);
            #[cfg(feature = "stage-timing")]
            let bases = (buffer.public_base, buffer.hidden_base);
            self.delivery.borrow().publish(&mut buffer, self.index);
            let retired = measure_stage!(
                self.stages,
                CandidateReclaimDetach,
                DayHubV1::compact(&mut buffer)
            );
            self.hub.0.1.control.notify_all();
            #[cfg(feature = "stage-timing")]
            {
                self.stages.finish(
                    crate::stage_timing::Stage::CandidateCursorPublishAndReclaim,
                    reclaim_started,
                );
                self.stages.add_delivery_work(
                    0,
                    buffer.public_base - bases.0,
                    buffer.hidden_base - bases.1,
                );
            }
            // 发布仍在原锁内完成；先解锁并释放本批回收引用，再锁定当前可领取材料。
            if !retired.0.is_empty() || !retired.1.is_empty() {
                drop(buffer);
                measure_stage!(self.stages, CandidateReclaimedDrop, drop(retired));
                #[cfg(feature = "stage-timing")]
                let lock_started = self
                    .stages
                    .start(crate::stage_timing::Stage::CandidateRefillLockWait);
                buffer = self.hub.0.0.lock().unwrap();
                #[cfg(feature = "stage-timing")]
                self.stages.finish(
                    crate::stage_timing::Stage::CandidateRefillLockWait,
                    lock_started,
                );
            }
            #[cfg(feature = "stage-timing")]
            let mut emit_transport = false;
            loop {
                #[cfg(feature = "stage-timing")]
                let claim_started = self
                    .stages
                    .start(crate::stage_timing::Stage::CandidateBatchClaim);
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
                    self.stages.finish(
                        crate::stage_timing::Stage::CandidateBatchClaim,
                        claim_started,
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
                    self.hub.0.1.control.notify_all();
                }
                #[cfg(test)]
                let observed_wait = tests::empty_wait_started();
                #[cfg(feature = "stage-timing")]
                let wait_started = self
                    .stages
                    .start(crate::stage_timing::Stage::CandidateDataWait);
                buffer = self.hub.0.1.public.wait(buffer).unwrap();
                #[cfg(feature = "stage-timing")]
                self.stages
                    .finish(crate::stage_timing::Stage::CandidateDataWait, wait_started);
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
            stages: crate::stage_timing::StageTimings::new_transport(format!(
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
            let lock_started = self
                .stages
                .start(crate::stage_timing::Stage::CandidateRefillLockWait);
            let mut buffer = self.hub.0.0.lock().unwrap();
            #[cfg(feature = "stage-timing")]
            self.stages.finish(
                crate::stage_timing::Stage::CandidateRefillLockWait,
                lock_started,
            );
            #[cfg(feature = "stage-timing")]
            let reclaim_started = self
                .stages
                .start(crate::stage_timing::Stage::CandidateCursorPublishAndReclaim);
            #[cfg(feature = "stage-timing")]
            let bases = (buffer.public_base, buffer.hidden_base);
            self.delivery.borrow().publish(&mut buffer, self.index);
            let retired = measure_stage!(
                self.stages,
                CandidateReclaimDetach,
                DayHubV1::compact(&mut buffer)
            );
            self.hub.0.1.control.notify_all();
            #[cfg(feature = "stage-timing")]
            {
                self.stages.finish(
                    crate::stage_timing::Stage::CandidateCursorPublishAndReclaim,
                    reclaim_started,
                );
                self.stages.add_delivery_work(
                    0,
                    buffer.public_base - bases.0,
                    buffer.hidden_base - bases.1,
                );
            }
            // 发布仍在原锁内完成；先解锁并释放本批回收引用，再锁定当前可领取材料。
            if !retired.0.is_empty() || !retired.1.is_empty() {
                drop(buffer);
                measure_stage!(self.stages, CandidateReclaimedDrop, drop(retired));
                #[cfg(feature = "stage-timing")]
                let lock_started = self
                    .stages
                    .start(crate::stage_timing::Stage::CandidateRefillLockWait);
                buffer = self.hub.0.0.lock().unwrap();
                #[cfg(feature = "stage-timing")]
                self.stages.finish(
                    crate::stage_timing::Stage::CandidateRefillLockWait,
                    lock_started,
                );
            }
            #[cfg(feature = "stage-timing")]
            let mut emit_transport = false;
            loop {
                #[cfg(feature = "stage-timing")]
                let claim_started = self
                    .stages
                    .start(crate::stage_timing::Stage::CandidateBatchClaim);
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
                    self.stages.finish(
                        crate::stage_timing::Stage::CandidateBatchClaim,
                        claim_started,
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
                    self.hub.0.1.control.notify_all();
                }
                #[cfg(test)]
                let observed_wait = tests::empty_wait_started();
                #[cfg(feature = "stage-timing")]
                let wait_started = self
                    .stages
                    .start(crate::stage_timing::Stage::CandidateDataWait);
                buffer = self.hub.0.1.hidden.wait(buffer).unwrap();
                #[cfg(feature = "stage-timing")]
                self.stages
                    .finish(crate::stage_timing::Stage::CandidateDataWait, wait_started);
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
pub(crate) struct DayCandidateControlV1 {
    delivery: Rc<RefCell<DayLocalDeliveryCacheV1>>,
    hub: DayHubV1,
    index: usize,
}
impl DayCandidateControlV1 {
    pub fn new(
        hub: DayHubV1,
        index: usize,
        delivery: Rc<RefCell<DayLocalDeliveryCacheV1>>,
    ) -> Self {
        Self {
            delivery,
            hub,
            index,
        }
    }
}
impl RuntimeControlV1 for DayCandidateControlV1 {
    fn public_evidence(&self, position: u64) -> Option<Arc<HistoricalPublicEvidenceV1>> {
        self.delivery
            .borrow()
            .research_public
            .as_ref()
            .filter(|(p, _)| *p == position)
            .map(|(_, v)| v.clone())
    }
    fn initialization_classification(&self, position: u64) -> Option<(bool, bool)> {
        self.delivery
            .borrow()
            .initialization
            .filter(|(p, ..)| *p == position)
            .map(|(_, initial, dependency)| (initial, dependency))
    }
    fn research_enabled(&self) -> bool {
        self.hub.0.0.lock().unwrap().research_summary
    }
    fn started(&mut self) {
        let mut buffer = self.hub.0.0.lock().unwrap();
        buffer.candidates[self.index].runtime_started = true;
        self.hub.0.1.control.notify_all();
    }
    fn stop_requested(&mut self) -> Result<bool, MaterialError> {
        Ok(self.hub.0.1.budget_stop_requested.load(Ordering::Acquire))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::RefCell,
        time::{Duration, Instant},
    };

    #[derive(Default, serde::Serialize)]
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
    fn budget_poll_uses_atomic_flag_without_locking_or_publishing_a_prefetched_prefix() {
        let temp = tempfile::tempdir().unwrap();
        let (config, _) = crate::test_common::setup(temp.path());
        let hub = DayHubV1::new(&[HistoricalPreparedRunConfigV1::from(&config)]);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let worker_hub = hub.clone();
            scope.spawn(move || {
                let delivery = DayLocalDeliveryCacheV1::shared(&worker_hub, 0);
                let mut control = DayCandidateControlV1::new(worker_hub, 0, delivery);
                ready_tx.send(()).unwrap();
                go_rx.recv().unwrap();
                result_tx.send(control.stop_requested().unwrap()).unwrap();
            });
            ready_rx.recv().unwrap();
            let buffer = hub.0.0.lock().unwrap();
            hub.0.1.budget_stop_requested.store(true, Ordering::Release);
            go_tx.send(()).unwrap();
            assert!(
                result_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("停止检查不能等待Hub锁")
            );
            assert_eq!(buffer.candidates[0].cursor.public, 0);
        });
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
            buffer.public.extend(groups.iter().cloned().map(Into::into));
            buffer.production = ProductionV1::Ended;
        }
        let delivery = DayLocalDeliveryCacheV1::shared(&hub, 0);
        let mut source = DayPublicSourceV1::new(hub.clone(), 0, delivery.clone());
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
        let mut source = DayPublicSourceV1::new(hub.clone(), 0, delivery);
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
                hub.0.0.lock().unwrap().public.push_back(
                    PublicGroupDeliveryV1 {
                        prepared: Arc::new(PreparedPublicGroupV1 {
                            statistics: Default::default(),
                            reference: delivered.prepared.reference.clone(),
                            group: Arc::new(group),
                            pm_inputs: vec![],
                            pm_contexts: vec![],
                        }),
                        delivered_at_utc_ms: delivered.delivered_at_utc_ms,
                    }
                    .into(),
                );
            }
            hub.0.0.lock().unwrap().production = ProductionV1::Ended;
            let delivery = DayLocalDeliveryCacheV1::shared(&hub, 0);
            let mut source = DayPublicSourceV1::new(hub.clone(), 0, delivery);
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
                    source_health: None,
                    source_progress: None,
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
}
