//! #185 有限测量开关：原研究整点及运行末输出累计值；不参与业务判断。
//! 新增 *_sampled 每64次机会计一次，count 是样本数，不将 total 外推成全量。
//! producer 目录更新只记录实际写入（全量）；其他三个子阶段采样。四者互斥，嵌套在既有 delivery_assembly_and_publish_including_lock。
//! 候选分类锁等待与分类互斥；控制锁等待嵌套于控制检查父阶段。
//! 运输的锁等待、发布/移除、锁外析构、领取、Condvar等待互斥；移除嵌套在发布/移除。
//! 运输阶段又嵌套在候选既有来源等待父阶段，不能与父阶段相加或跨候选相加当墙钟。
#[cfg(feature = "stage-timing")]
#[derive(Default, serde::Serialize)]
struct Sample {
    count: u64,
    total_ns: u128,
    max_ns: u128,
}
#[cfg(feature = "stage-timing")]
macro_rules! define_stages {
    ($($stage:ident => $name:literal),* $(,)?) => {
        #[derive(Clone, Copy)]
        #[repr(usize)]
        pub(crate) enum Stage { $($stage),* }
        const STAGE_NAMES: [&str; [$(stringify!($stage)),*].len()] = [$($name),*];
    };
}
#[cfg(feature = "stage-timing")]
define_stages! {
    ApplyClosedIncludingNoeClaimAndLedger => "apply_closed_including_noe_claim_and_ledger",
    CalculationIncludingCutoffCopyAndDecide => "calculation_including_cutoff_copy_and_decide",
    CheckpointCaptureSubsystems => "checkpoint_capture_subsystems",
    CheckpointCaptureWriteAndPublish => "checkpoint_capture_write_and_publish",
    CheckpointEncodeDayWriteAndSync => "checkpoint_encode_day_write_and_sync",
    CurrentPublicObservationAssembly => "current_public_observation_assembly",
    DayEconomicSampling => "day_economic_sampling",
    DeliveryAssemblyAndPublishIncludingLock => "delivery_assembly_and_publish_including_lock",
    DispatchIncludingReserveBridgeSim => "dispatch_including_reserve_bridge_sim",
    DispatchIncludingReserveFullQualificationBridgeSim => "dispatch_including_reserve_full_qualification_bridge_sim",
    EncodeCandidateWriteAndSync => "encode_candidate_write_and_sync",
    EncodeLedger => "encode_ledger",
    EncodeSharedExecution => "encode_shared_execution",
    EncodeSimDriver => "encode_sim_driver",
    EncodeStrategy => "encode_strategy",
    EncodeSubsystemsAndCapture => "encode_subsystems_and_capture",
    HiddenSourceWaitAndAcquisition => "hidden_source_wait_and_acquisition",
    NextAppliedIncludingDecodeCoreHidden => "next_applied_including_decode_core_hidden",
    NextHealthEvaluationIncludingDependencyLookup => "next_health_evaluation_including_dependency_lookup",
    ProducerBackpressureWait => "producer_backpressure_wait",
    ProducerWaitStartupCheckpointOrCompletion => "producer_wait_startup_checkpoint_or_completion",
    PublicQualification => "public_qualification",
    RecordingStabilizationWait => "recording_stabilization_wait",
    RecordingStopAndDrain => "recording_stop_and_drain",
    RevalidationIncludingCurrentReadAndFullQualification => "revalidation_including_current_read_and_full_qualification",
    RuntimeOpenAndAccountAssembly => "runtime_open_and_account_assembly",
    SimAdvanceBeforeIncludingNoe => "sim_advance_before_including_noe",
    SimHiddenBucketIncludingNoe => "sim_hidden_bucket_including_noe",
    SimNextGroupAtIncludingSourceWait => "sim_next_group_at_including_source_wait",
    SimNextGroupIncludingNoe => "sim_next_group_including_noe",
    StrategyInputAssembly => "strategy_input_assembly",
    WaitSharedPublication => "wait_shared_publication",
    InitializationCatalogLockWaitSampled => "initialization_catalog_lock_wait_sampled",
    InitializationClassificationSampled => "initialization_classification_sampled",
    CandidateControlLockWaitSampled => "candidate_control_lock_wait_sampled",
    CandidateControlAndCursorPublishSampled => "candidate_control_and_cursor_publish_including_lock_sampled",
    ProducerCatalogUpdate => "producer_catalog_update_including_lock",
    ProducerDeliveryConstruction => "producer_public_delivery_construction_sampled",
    ProducerHubLockWait => "producer_hub_lock_wait_sampled",
    ProducerHubPublish => "producer_hub_publish_including_hidden_construction_sampled",
    CandidateRefillLockWait => "candidate_refill_lock_wait",
    CandidateCursorPublishAndReclaim => "candidate_cursor_publish_and_detach",
    CandidateReclaimDetach => "candidate_reclaim_detach",
    CandidateReclaimedDrop => "candidate_reclaimed_drop_outside_lock",
    CandidateBatchClaim => "candidate_batch_claim",
    CandidateDataWait => "candidate_data_wait",
}
#[cfg(feature = "stage-timing")]
pub(crate) struct StageTimings {
    label: String,
    started: std::time::Instant,
    samples: [Sample; STAGE_NAMES.len()],
    diagnostic_calls: u64,
    initialization_market_visits: u64,
    initialization_source_id_visits: u64,
    claimed: u64,
    reclaimed_public: u64,
    reclaimed_hidden: u64,
}
#[cfg(feature = "stage-timing")]
impl StageTimings {
    pub(crate) fn new(label: String) -> Self {
        Self {
            label,
            started: std::time::Instant::now(),
            samples: std::array::from_fn(|_| Sample::default()),
            diagnostic_calls: 0,
            initialization_market_visits: 0,
            initialization_source_id_visits: 0,
            claimed: 0,
            reclaimed_public: 0,
            reclaimed_hidden: 0,
        }
    }
    pub(crate) fn next_diagnostic_sample(&mut self) -> bool {
        let sample = self.diagnostic_calls.is_multiple_of(64);
        self.diagnostic_calls += 1;
        sample
    }
    pub(crate) fn add_initialization_work(&mut self, markets: u64, sources: u64) {
        self.initialization_market_visits += markets;
        self.initialization_source_id_visits += sources;
    }
    pub(crate) fn add_delivery_work(
        &mut self,
        claimed: usize,
        public: usize,
        hidden: usize,
    ) -> bool {
        let prior_claimed = self.claimed;
        self.claimed += claimed as u64;
        self.reclaimed_public += public as u64;
        self.reclaimed_hidden += hidden as u64;
        prior_claimed / 1_048_576 != self.claimed / 1_048_576
    }
    pub(crate) fn emit_transport(&self) {
        eprintln!(
            "stage_timing_transport={}",
            serde_json::json!({"scope":self.label,
            "scope_wall_ns":self.started.elapsed().as_nanos(),"stages":self.samples(),"diagnostics":self.diagnostics()})
        );
    }
    fn diagnostics(&self) -> serde_json::Value {
        serde_json::json!({"sampled_stage_stride":64,"diagnostic_calls":self.diagnostic_calls,
            "initialization_market_visits":self.initialization_market_visits,
            "initialization_source_id_visits":self.initialization_source_id_visits,
            "claimed_items":self.claimed,"reclaimed_public":self.reclaimed_public,
            "reclaimed_hidden":self.reclaimed_hidden})
    }
    pub(crate) fn record(&mut self, stage: Stage, elapsed: std::time::Duration) {
        let sample = &mut self.samples[stage as usize];
        sample.count += 1;
        let ns = elapsed.as_nanos();
        sample.total_ns += ns;
        sample.max_ns = sample.max_ns.max(ns);
    }
    fn samples(&self) -> std::collections::BTreeMap<&'static str, &Sample> {
        STAGE_NAMES
            .iter()
            .copied()
            .zip(&self.samples)
            .filter(|(_, sample)| sample.count > 0)
            .collect()
    }
    pub(crate) fn emit_checkpoint(&self, generation: u64) {
        eprintln!(
            "stage_timing_checkpoint={}",
            serde_json::json!({"scope":self.label,"generation":generation,
                "scope_wall_ns":self.started.elapsed().as_nanos(),"stages":self.samples(),"diagnostics":self.diagnostics()})
        );
    }
    pub(crate) fn emit_hour(&self, boundary_utc_ms: i64) {
        eprintln!(
            "stage_timing_hour={}",
            serde_json::json!({"scope":self.label,"boundary_utc_ms":boundary_utc_ms,
                "scope_wall_ns":self.started.elapsed().as_nanos(),"stages":self.samples(),"diagnostics":self.diagnostics()})
        );
    }
}
#[cfg(feature = "stage-timing")]
impl Drop for StageTimings {
    fn drop(&mut self) {
        eprintln!(
            "stage_timing={}",
            serde_json::json!({"scope":self.label,"scope_wall_ns":self.started.elapsed().as_nanos(),"stages":self.samples(),"diagnostics":self.diagnostics()})
        );
    }
}
macro_rules! measure_stage {
    ($timings:expr, $stage:ident, $expression:expr) => {{
        #[cfg(feature = "stage-timing")]
        let started = std::time::Instant::now();
        let result = $expression;
        #[cfg(feature = "stage-timing")]
        $timings.record(crate::stage_timing::Stage::$stage, started.elapsed());
        result
    }};
}
/// 只对新增高频诊断采样；原 measure_stage 的统计区间与计数不变。
macro_rules! measure_sampled_stage {
    ($timings:expr, $sample:expr, $stage:ident, $expression:expr) => {{
        #[cfg(feature = "stage-timing")]
        let started = $sample.then(std::time::Instant::now);
        let result = $expression;
        #[cfg(feature = "stage-timing")]
        if let Some(started) = started {
            $timings.record(crate::stage_timing::Stage::$stage, started.elapsed());
        }
        result
    }};
}

#[cfg(all(test, feature = "stage-timing"))]
mod tests {
    use super::*;

    #[test]
    fn sampled_diagnostics_keep_expression_count_and_actual_sample_coverage() {
        let mut stages = StageTimings::new("sampling-test".into());
        let mut expressions = 0;
        let started = std::time::Instant::now();
        for _ in 0..65_536 {
            let sample = stages.next_diagnostic_sample();
            measure_sampled_stage!(stages, sample, InitializationClassificationSampled, {
                expressions += 1;
                std::hint::black_box(expressions);
            });
        }
        let wall = started.elapsed().as_nanos();
        assert_eq!(expressions, 65_536);
        assert_eq!(
            stages.samples[Stage::InitializationClassificationSampled as usize].count,
            1024
        );
        assert_eq!(stages.diagnostic_calls, 65_536);
        eprintln!(
            "timing_sampling_diagnostic={}",
            serde_json::json!({"opportunities":65_536,
            "sample_count":1024,"wall_ns":wall,"includes_loop_and_expression":true})
        );
    }
    #[test]
    fn hourly_output_keeps_cumulative_counts_and_maximum() {
        let mut timings = StageTimings::new("candidate cumulative-test".into());
        timings.record(
            crate::stage_timing::Stage::CalculationIncludingCutoffCopyAndDecide,
            std::time::Duration::from_nanos(3),
        );
        timings.emit_hour(3_600_000);
        timings.record(
            crate::stage_timing::Stage::CalculationIncludingCutoffCopyAndDecide,
            std::time::Duration::from_nanos(7),
        );
        timings.emit_hour(7_200_000);
        let sample = &timings.samples[Stage::CalculationIncludingCutoffCopyAndDecide as usize];
        assert_eq!(sample.count, 2);
        assert_eq!(sample.total_ns, 10);
        assert_eq!(sample.max_ns, 7);
    }
}
