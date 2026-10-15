//! #185 有限测量开关：原研究整点及运行末输出累计值；不参与业务判断。
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
}
#[cfg(feature = "stage-timing")]
pub(crate) struct StageTimings {
    label: String,
    started: std::time::Instant,
    samples: [Sample; STAGE_NAMES.len()],
}
#[cfg(feature = "stage-timing")]
impl StageTimings {
    pub(crate) fn new(label: String) -> Self {
        Self {
            label,
            started: std::time::Instant::now(),
            samples: std::array::from_fn(|_| Sample::default()),
        }
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
                "scope_wall_ns":self.started.elapsed().as_nanos(),"stages":self.samples()})
        );
    }
    pub(crate) fn emit_hour(&self, boundary_utc_ms: i64) {
        eprintln!(
            "stage_timing_hour={}",
            serde_json::json!({"scope":self.label,"boundary_utc_ms":boundary_utc_ms,
                "scope_wall_ns":self.started.elapsed().as_nanos(),"stages":self.samples()})
        );
    }
}
#[cfg(feature = "stage-timing")]
impl Drop for StageTimings {
    fn drop(&mut self) {
        eprintln!(
            "stage_timing={}",
            serde_json::json!({"scope":self.label,"scope_wall_ns":self.started.elapsed().as_nanos(),"stages":self.samples()})
        );
    }
}
macro_rules! measure_stage {
    ($timings:ident, $stage:ident, $expression:expr) => {{
        #[cfg(feature = "stage-timing")]
        let started = std::time::Instant::now();
        let result = $expression;
        #[cfg(feature = "stage-timing")]
        $timings.record(crate::stage_timing::Stage::$stage, started.elapsed());
        result
    }};
}

#[cfg(all(test, feature = "stage-timing"))]
mod tests {
    use super::*;

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
