use serde::Serialize;

const RUNNER_IDENTITY: &str = "sim-task-4-5-1-golden-v1";
#[cfg(any(feature = "measurement", test))]
const TARGETED_RUNNER_IDENTITY: &str = "sim-task-4-5-2-retention-24h-v1";
#[cfg(feature = "measurement")]
const COMPONENT_RUNNER_IDENTITY: &str = "sim-stage-5-component-conformance-v1";
#[cfg(any(feature = "gate-a-measurement", all(test, feature = "measurement")))]
const GATE_A_RUNNER_IDENTITY: &str = "sim-stage-6-gate-a-real-vertical-v1";
#[cfg(feature = "measurement")]
const COMPONENT_WORKLOAD_IDENTITY: &str = "offline-evaluation-two-market-single-root-v1";
#[cfg(feature = "gate-a-measurement")]
const GATE_A_WORKLOAD_IDENTITY: &str = "gate-a-real-vertical-two-path-single-root-v1";
const WORKLOAD_IDENTITY: &str = "public-sim-execution-two-market-256x8-v1";
#[cfg(any(feature = "measurement", test))]
const TARGETED_RETENTION_WINDOW_MS: i64 = 86_400_000;
const LOOPS_PER_ROOT: usize = 256;
const REQUESTS_PER_ROOT: usize = LOOPS_PER_ROOT * 8;
const GIB: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SafetyLimits {
    wall_ns: u64,
    trial_ns: u64,
    rss_bytes: u64,
    output_bytes: u64,
    free_disk_bytes: u64,
}
impl SafetyLimits {
    const fn fixed() -> Self {
        Self {
            wall_ns: 2 * 60 * 60 * 1_000_000_000,
            trial_ns: 15 * 60 * 1_000_000_000,
            rss_bytes: 8 * GIB,
            output_bytes: 2 * GIB,
            free_disk_bytes: 20 * GIB,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SafetyUsage {
    wall_ns: u64,
    trial_ns: u64,
    rss_bytes: u64,
    output_bytes: u64,
    free_disk_bytes: u64,
}
impl SafetyUsage {
    #[cfg(test)]
    const fn safe() -> Self {
        Self {
            wall_ns: 0,
            trial_ns: 0,
            rss_bytes: 0,
            output_bytes: 0,
            free_disk_bytes: u64::MAX,
        }
    }
}
fn safety_violation(l: SafetyLimits, u: SafetyUsage) -> Option<&'static str> {
    if u.wall_ns > l.wall_ns {
        Some("wall_clock")
    } else if u.trial_ns > l.trial_ns {
        Some("trial_clock")
    } else if u.rss_bytes > l.rss_bytes {
        Some("rss")
    } else if u.output_bytes > l.output_bytes {
        Some("output")
    } else if u.free_disk_bytes < l.free_disk_bytes {
        Some("free_disk")
    } else {
        None
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PairKey {
    workload: String,
    durability: String,
    shape: String,
    trial: usize,
}
impl PairKey {
    fn new(w: &str, d: &str, s: &str, t: usize) -> Self {
        Self {
            workload: w.into(),
            durability: d.into(),
            shape: s.into(),
            trial: t,
        }
    }
}
fn validate_feature_pair(bb: &str, bk: &[PairKey], cb: &str, ck: &[PairKey]) -> Result<(), String> {
    if bb != "measurement_off" || cb != "measurement_on" {
        return Err("feature 开销要求 baseline=measurement_off 且 current=measurement_on".into());
    }
    let b = bk
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let c = ck
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    if b.len() != bk.len() || c.len() != ck.len() {
        return Err("feature 配对键重复".into());
    }
    if b != c {
        return Err("feature 开销缺少相同 workload/durability/shape/trial 配对".into());
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
struct DistributionSummary {
    count: usize,
    min: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

fn nearest_rank(sorted_values: &[u64], percentile: f64) -> u64 {
    assert!(!sorted_values.is_empty(), "nearest-rank 需要非空样本");
    assert!((0.0..=1.0).contains(&percentile), "百分位必须位于 [0, 1]");
    let rank = (percentile * sorted_values.len() as f64).ceil() as usize;
    sorted_values[rank.saturating_sub(1).min(sorted_values.len() - 1)]
}

fn distribution_summary(mut values: Vec<u64>) -> DistributionSummary {
    assert!(!values.is_empty(), "分布摘要需要非空样本");
    values.sort_unstable();
    DistributionSummary {
        count: values.len(),
        min: values[0],
        p50: nearest_rank(&values, 0.50),
        p95: nearest_rank(&values, 0.95),
        p99: nearest_rank(&values, 0.99),
        max: values[values.len() - 1],
    }
}

mod enabled {
    use std::collections::BTreeMap;
    #[cfg(feature = "gate-a-measurement")]
    use std::collections::HashSet;
    use std::fs::{self, File, OpenOptions};
    use std::io::{BufWriter, Read, Write};
    use std::ops::Range;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;
    #[cfg(feature = "measurement")]
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use contracts::{
        AssetIdentityV1, BestBidAskV2, CanonicalEventId, CanonicalSourceKind, ClobMarketId,
        CollectorAdmissionCoordinateV1, ConditionId, EventLineageV2, EventQualityV2, EventTimeV2,
        ExactDecimal, ExecutionMarketEventPayloadV3, ExecutionMarketEventV3, InitialAccountStateV2,
        InitialL2SnapshotV2, MarketDataQuality, MarketIdentityV2, MarketResolutionTerminalV3,
        MarketWindowV2, OutcomeIdentityV1, PayoutEntryV1, PriceLevelV1, QueryCursorV2,
        ResyncQuality, Sha256Hex, SimCallerRoleV1, SourceEvidenceRefV2, TokenId,
    };
    #[cfg(feature = "measurement")]
    use contracts::{
        OrderQueryResultV2, OrdersQueryResultV2, QueryOutOfRetentionV2, SubmissionItemOutcomeV2,
        TradesQueryResultV2,
    };
    use data_pipeline_core::{
        decode_execution_market_event_v3, derive_execution_market_event_v3_id,
        encode_execution_market_event_v3,
    };
    #[cfg(feature = "gate-a-measurement")]
    use data_pipeline_training::{
        ClosedExecutionMarketIndexBucketV1, GateASimulationReplayAdapter, PublishedGateAFinal,
        ReplayIndexClosureV1, open_published_gate_a_final,
    };
    #[cfg(feature = "measurement")]
    use rustix::fs::{RenameFlags, renameat_with};
    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};
    #[cfg(feature = "measurement")]
    use sim_system::execution::BusinessDirectResponseV2;
    use sim_system::execution::{
        AcceptedClosedMarketBucketV2, BusinessProjectionBatchV2, BusinessProjectionPortV2,
        CancelAllOrdersRequestV2, CancelOrderBatchItemV2, CancelOrderItemV1, CancelOrderRequestV2,
        CancelOrdersRequestV2, DurableMarketBucketReferenceV2, OrderRequestV2, OrdersFilterV1,
        OrdersRequestV2, PostOrderBatchItemV2, PostOrderItemV1, PostOrderRequestV2,
        PostOrdersRequestV2, PublicVisibilityCursorV1, RequestContextV2, RetentionAdvanceV2,
        RunDurabilityRoleV2, RunOpenedV2, SessionSpecV3, SimBuyOrderKindV1, SimBuyOrderV1,
        SimExecution, SimRunActorErrorV2, TradesFilterV1, TradesRequestV2, TrustedDecisionCutoffV2,
    };
    use sim_system::execution::{MarketEventPathV1, MillisecondBucketClosureV1};
    use sim_system::finality::TradeFinalityPolicyV2;
    #[cfg(feature = "gate-a-measurement")]
    use sim_system::latency_profile::DelaySampleDomainV1;
    use sim_system::latency_profile::{EmpiricalDelayProfile, load_embedded_proxy_latency_profile};
    use sim_system::platform_rules::{
        CurrentProcessSnapshotRequestV1, ImplementationIdentityV1, ReferenceMarketSpecV1,
        ReferenceOutcomeSpecV1, SealedProcessRuleSnapshotV3,
        query_and_seal_current_process_snapshot_v3,
    };
    #[cfg(feature = "measurement")]
    use sim_system::platform_rules::{
        HistoricalProcessSnapshotRequestV1, load_historical_process_snapshot_v3,
    };
    use sim_system::scenario::{
        PLATFORM_SCENARIO_SCHEMA_V1, PlatformMarketV1, PlatformScenarioV1,
        SealedPlatformScenarioV1, SimulationScenarioArtifactV1,
    };

    #[cfg(feature = "measurement")]
    use super::COMPONENT_RUNNER_IDENTITY;
    #[cfg(feature = "measurement")]
    use super::COMPONENT_WORKLOAD_IDENTITY;
    #[cfg(any(feature = "gate-a-measurement", all(test, feature = "measurement")))]
    use super::GATE_A_RUNNER_IDENTITY;
    #[cfg(feature = "gate-a-measurement")]
    use super::GATE_A_WORKLOAD_IDENTITY;
    use super::{
        DistributionSummary, LOOPS_PER_ROOT, PairKey, REQUESTS_PER_ROOT, RUNNER_IDENTITY,
        SafetyLimits, SafetyUsage, WORKLOAD_IDENTITY, distribution_summary, safety_violation,
        validate_feature_pair,
    };
    #[cfg(any(feature = "measurement", test))]
    use super::{TARGETED_RETENTION_WINDOW_MS, TARGETED_RUNNER_IDENTITY};

    #[cfg(feature = "gate-b-flow")]
    mod gate_b {
        include!("sim_measurement/gate_b.rs");
    }

    const CONDITION: &str = "0xd4346bcf419856fa3261d3c0705597d5c909ead020b9f1a0de468092704fc65f";
    const TOKEN_UP: &str =
        "45253098770861975037434792771412818336165758089047645530470012231811628219325";
    const TOKEN_DOWN: &str =
        "13103176462561566402126211408478811357460751704260150146723274072578196298541";
    const SLUG: &str = "btc-updown-5m-1785715500";
    const MAX_GROUP_BYTES: usize = 8 * 1024 * 1024;
    const TRIALS: usize = 3;
    const WARMUP_TRIALS: usize = 1;
    const MARKET_ONE_START: i64 = 1_785_715_500_000;
    const MARKET_TWO_START: i64 = 1_785_715_650_000;
    const MARKET_END: i64 = 1_785_715_900_000;
    const REQUEST_CUTOFF_BASE: i64 = 1_785_715_700_000;
    const BODY_ONE_SOURCE: i64 = 1_785_715_750_000;
    const BODY_TWO_SOURCE: i64 = 1_785_715_760_000;
    #[cfg(feature = "gate-a-measurement")]
    const GATE_A_MARKET_END: i64 = 1_785_715_800_000;
    #[cfg(feature = "gate-a-measurement")]
    const GATE_A_REPRESENTATIVE_TRADE_EVENT_ID: &str =
        "0ba3871e7c0b1b6356f2e70f679ff250a4fe1a7c343289711ea05cd06c69f830";
    #[cfg(feature = "gate-a-measurement")]
    const GATE_A_REQUEST_ID: &str = "stage-6-gate-a-representative-post";

    #[cfg(feature = "measurement")]
    pub(super) const COMPONENT_CAUSAL_START_UTC_MS: i64 = 1_787_400_000_000;
    #[cfg(feature = "measurement")]
    pub(super) const COMPONENT_TIME_SHIFT_MS: i64 =
        COMPONENT_CAUSAL_START_UTC_MS - (MARKET_ONE_START + 10);
    #[cfg(feature = "measurement")]
    pub(super) const COMPONENT_REQUEST_CUTOFF_BASE: i64 =
        REQUEST_CUTOFF_BASE + COMPONENT_TIME_SHIFT_MS;

    const RESOLUTION_ONE_SOURCE: i64 = MARKET_END + 10;
    const RESOLUTION_TWO_SOURCE: i64 = MARKET_END + 20;
    const FINAL_EOF_SOURCE: i64 = RESOLUTION_TWO_SOURCE + 86_500_000;
    const PUBLIC_DELAY_MS: i64 = 10;
    #[cfg(feature = "measurement")]
    const TARGETED_FINAL_CUTOFF_UTC_MS: i64 = FINAL_EOF_SOURCE + PUBLIC_DELAY_MS;

    #[cfg(feature = "measurement")]
    const BUILD: &str = "measurement_on";
    #[cfg(not(feature = "measurement"))]
    const BUILD: &str = "measurement_off";

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) enum RunMode {
        GoldenMatrix,
        TargetedRetention24h,
        ComponentConformance,
        GateARealVertical,
        #[cfg(feature = "gate-b-flow")]
        GateBFlow,
    }

    #[cfg(all(test, feature = "measurement"))]
    pub(super) const fn runner_identity(mode: RunMode) -> &'static str {
        match mode {
            RunMode::GoldenMatrix => RUNNER_IDENTITY,
            RunMode::TargetedRetention24h => TARGETED_RUNNER_IDENTITY,
            RunMode::ComponentConformance => COMPONENT_RUNNER_IDENTITY,
            RunMode::GateARealVertical => GATE_A_RUNNER_IDENTITY,
            #[cfg(feature = "gate-b-flow")]
            RunMode::GateBFlow => gate_b::GATE_B_RUNNER_IDENTITY,
        }
    }

    #[cfg(feature = "measurement")]
    #[derive(Debug)]
    struct ComponentEvidenceArgs {
        rule_snapshot: PathBuf,
        rule_snapshot_sha256: Sha256Hex,
        stage4_formal_audit: PathBuf,
        stage4_formal_audit_sha256: Sha256Hex,
        retention_manifest: PathBuf,
        retention_manifest_sha256: Sha256Hex,
        test_evidence: PathBuf,
        test_evidence_sha256: Sha256Hex,
    }

    #[cfg(feature = "gate-a-measurement")]
    #[derive(Debug)]
    struct GateARealVerticalArgs {
        gate_a_root: PathBuf,
        stage5_component_manifest: PathBuf,
        stage5_component_manifest_sha256: Sha256Hex,
        stage5_component_receipt_id: Sha256Hex,
    }

    #[derive(Debug)]
    pub(super) struct Args {
        output_dir: PathBuf,
        identity: ImplementationIdentityV1,
        baseline_summary: Option<PathBuf>,
        #[cfg(feature = "measurement")]
        component: Option<ComponentEvidenceArgs>,
        #[cfg(feature = "gate-a-measurement")]
        gate_a: Option<GateARealVerticalArgs>,
        #[cfg(feature = "gate-b-flow")]
        gate_b: Option<gate_b::GateBArgs>,
        pub(super) mode: RunMode,
        argv: Vec<String>,
    }

    #[cfg(all(test, feature = "gate-b-flow"))]
    impl Args {
        pub(super) fn gate_b_configuration_for_test(
            &self,
        ) -> (&ReferenceMarketSpecV1, &str, &str, &str) {
            let gate = self.gate_b.as_ref().expect("测试参数已选择 Gate B");
            (
                &gate.reference_market,
                &gate.run_id,
                &gate.initial_available_pusd.0,
                &gate.per_market_all_in_commitment_limit_pusd.0,
            )
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum Durability {
        CleanRun,
        RecoverablePaper,
    }

    impl Durability {
        fn label(self) -> &'static str {
            match self {
                Self::CleanRun => "clean_run",
                Self::RecoverablePaper => "recoverable_paper",
            }
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum Shape {
        SingleRoot,
        FourRoots,
    }

    impl Shape {
        fn roots(self) -> usize {
            match self {
                Self::SingleRoot => 1,
                Self::FourRoots => 4,
            }
        }

        fn label(self) -> &'static str {
            match self {
                Self::SingleRoot => "single_root",
                Self::FourRoots => "four_roots",
            }
        }
    }

    #[derive(Clone, Copy, Debug, Serialize)]
    struct ProcSnapshot {
        cpu_ticks: u64,
        rss_bytes: u64,
        rss_high_water_bytes: u64,
        io_read_bytes: u64,
        io_write_bytes: u64,
    }

    #[derive(Debug, Serialize)]
    struct Sample {
        schema_version: &'static str,
        runner_identity: &'static str,
        build: &'static str,
        durability: Durability,
        shape: Shape,
        trial: usize,
        root: usize,
        run: String,
        workload: &'static str,
        operation: &'static str,
        phase: &'static str,
        warmup: bool,
        requests: usize,
        e2e_elapsed_ns: u64,
        trial_elapsed_ns: u64,
        trial_requests: usize,
        trial_rss_peak_bytes: u64,
        stage_elapsed_ns: Value,
        measurement_observations: Value,
        measurement_resources: Value,
        resources: RootResources,
        proc_before: ProcSnapshot,
        proc_after: ProcSnapshot,
    }

    #[derive(Debug, Serialize)]
    struct RootResources {
        log_bytes: u64,
        durable_bucket_count: usize,
        durable_event_count: usize,
        cold_candidate_accepted_inputs: Option<u64>,
        paper_recovered: bool,
        recovery_elapsed_ns: u64,
        retention_before: Value,
        retention_after: Value,
        resource_high_water: Value,
    }

    #[derive(Debug)]
    struct RootResult {
        run_id: String,
        elapsed_ns: u64,
        observations: Value,
        resources: Value,
        root_resources: RootResources,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct IdentityBundle {
        rule_snapshot_sha256: String,
        synchronized_rule_values: Value,
        reference_market_condition_id: String,
        reference_market_clob_market_id: String,
        reference_market_slug: String,
        scenario_sha256: String,
        finality_policy_sha256: String,
        latency_profile_sha256: String,
        workload_identity: String,
        loops_per_root: usize,
        requests_per_root: usize,
        market_count: usize,
    }

    impl IdentityBundle {
        fn same_business_identity(&self, other: &Self) -> bool {
            self.synchronized_rule_values == other.synchronized_rule_values
                && self.reference_market_condition_id == other.reference_market_condition_id
                && self.reference_market_clob_market_id == other.reference_market_clob_market_id
                && self.reference_market_slug == other.reference_market_slug
                && self.scenario_sha256 == other.scenario_sha256
                && self.finality_policy_sha256 == other.finality_policy_sha256
                && self.latency_profile_sha256 == other.latency_profile_sha256
                && self.workload_identity == other.workload_identity
                && self.loops_per_root == other.loops_per_root
                && self.requests_per_root == other.requests_per_root
                && self.market_count == other.market_count
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct CellSummary {
        durability: Durability,
        shape: Shape,
        operation: String,
        trial_count: usize,
        requests_per_trial: usize,
        elapsed_ns: DistributionSummary,
        throughput_requests_per_second: DistributionSummary,
        public_request_latency_ns: Option<DistributionSummary>,
        stage_latency_ns: BTreeMap<String, DistributionSummary>,
        cpu_ticks: DistributionSummary,
        rss_high_water_bytes: DistributionSummary,
        io_read_bytes: DistributionSummary,
        io_write_bytes: DistributionSummary,
        log_bytes: DistributionSummary,
        recovery_elapsed_ns: DistributionSummary,
        retention_before: BTreeMap<String, DistributionSummary>,
        retention_after: BTreeMap<String, DistributionSummary>,
        resource_high_water: BTreeMap<String, DistributionSummary>,
        slowest_trial_ref: String,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TrialSummary {
        workload: String,
        durability: Durability,
        shape: Shape,
        trial: usize,
        requests: usize,
        elapsed_ns: u64,
        throughput_requests_per_second: u64,
        public_request_latency_ns: Option<DistributionSummary>,
        stage_latency_ns: BTreeMap<String, DistributionSummary>,
        cpu_ticks: u64,
        rss_peak_bytes: u64,
        io_read_bytes: u64,
        io_write_bytes: u64,
        log_bytes: u64,
        recovery_elapsed_ns: u64,
        retention_before: Value,
        retention_after: Value,
        resource_high_water: Value,
    }

    #[derive(Debug, Serialize)]
    struct BaselineComparison {
        baseline_path: String,
        baseline_build: String,
        pairing: &'static str,
        trials: Vec<BaselineTrialComparison>,
    }

    #[derive(Debug, Serialize)]
    struct BaselineTrialComparison {
        workload: String,
        durability: Durability,
        shape: Shape,
        trial: usize,
        current_elapsed_ns: u64,
        baseline_elapsed_ns: u64,
        absolute_delta_ns: i128,
        ratio: f64,
    }

    #[derive(Debug, Serialize)]
    struct Summary<'a> {
        schema_version: &'static str,
        runner_identity: &'static str,
        build: &'static str,
        identities: &'a IdentityBundle,
        measured_trials_per_cell: usize,
        warmup_trials_per_cell: usize,
        trials: &'a [TrialSummary],
        cells: &'a [CellSummary],
        baseline_comparison: Option<BaselineComparison>,
        candidate_status: &'static str,
    }

    #[cfg(any(feature = "measurement", test))]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
    pub(super) struct MinMedianMax {
        pub(super) min: u64,
        pub(super) median: u64,
        pub(super) max: u64,
    }

    #[cfg(any(feature = "measurement", test))]
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) struct TargetedSummaryInput {
        warmup: bool,
        retention_elapsed_ns: u64,
        released_business_records: u64,
        released_business_bytes: u64,
        semantics_passed: bool,
    }

    #[cfg(any(feature = "measurement", test))]
    impl TargetedSummaryInput {
        pub(super) const fn new(
            warmup: bool,
            retention_elapsed_ns: u64,
            released_business_records: u64,
            released_business_bytes: u64,
            semantics_passed: bool,
        ) -> Self {
            Self {
                warmup,
                retention_elapsed_ns,
                released_business_records,
                released_business_bytes,
                semantics_passed,
            }
        }
    }

    #[cfg(any(feature = "measurement", test))]
    #[derive(Debug, PartialEq, Eq, Serialize)]
    pub(super) struct TargetedSummary {
        schema_version: &'static str,
        runner_identity: &'static str,
        exploratory_input_only: bool,
        window_ms: i64,
        pub(super) measured_trial_count: usize,
        pub(super) warmup_trial_count: usize,
        pub(super) retention_elapsed_ns: MinMedianMax,
        pub(super) released_business_records: MinMedianMax,
        pub(super) released_business_bytes: MinMedianMax,
        pub(super) semantic_pass_count: usize,
    }

    #[cfg(any(feature = "measurement", test))]
    fn min_median_max(mut values: Vec<u64>) -> Result<MinMedianMax, String> {
        if values.is_empty() {
            return Err("targeted 汇总需要非空正式 trial".to_owned());
        }
        values.sort_unstable();
        Ok(MinMedianMax {
            min: values[0],
            median: values[values.len() / 2],
            max: values[values.len() - 1],
        })
    }

    #[cfg(any(feature = "measurement", test))]
    pub(super) fn summarize_targeted(
        samples: &[TargetedSummaryInput],
    ) -> Result<TargetedSummary, String> {
        let measured = samples
            .iter()
            .filter(|sample| !sample.warmup)
            .collect::<Vec<_>>();
        if measured.len() != TRIALS || samples.iter().filter(|sample| sample.warmup).count() != 1 {
            return Err("targeted 必须恰有 1 次 warmup 和 3 次正式 trial".to_owned());
        }
        Ok(TargetedSummary {
            schema_version: "sim_targeted_retention_summary_v1",
            runner_identity: TARGETED_RUNNER_IDENTITY,
            exploratory_input_only: true,
            window_ms: TARGETED_RETENTION_WINDOW_MS,
            measured_trial_count: measured.len(),
            warmup_trial_count: 1,
            retention_elapsed_ns: min_median_max(
                measured
                    .iter()
                    .map(|sample| sample.retention_elapsed_ns)
                    .collect(),
            )?,
            released_business_records: min_median_max(
                measured
                    .iter()
                    .map(|sample| sample.released_business_records)
                    .collect(),
            )?,
            released_business_bytes: min_median_max(
                measured
                    .iter()
                    .map(|sample| sample.released_business_bytes)
                    .collect(),
            )?,
            semantic_pass_count: measured
                .iter()
                .filter(|sample| sample.semantics_passed)
                .count(),
        })
    }

    #[cfg(feature = "measurement")]
    #[derive(Clone, Debug)]
    struct CapturedResponse {
        method: &'static str,
        body: Value,
    }

    #[cfg(feature = "measurement")]
    #[derive(Default)]
    struct CapturingProjectionState {
        durable_through: u64,
        responses: BTreeMap<String, CapturedResponse>,
        semantic_batches: Vec<Value>,
    }

    #[cfg(feature = "measurement")]
    struct CapturingProjectionPort {
        state: Arc<Mutex<CapturingProjectionState>>,
    }

    #[derive(Clone, Debug)]
    struct TargetedQueryPlan {
        old_root_sequence: u64,
        retained_from_utc_ms: i64,
        trade_id: String,
    }

    #[cfg(feature = "measurement")]
    impl BusinessProjectionPortV2 for CapturingProjectionPort {
        fn try_accept(&mut self, batch: Arc<BusinessProjectionBatchV2>) -> bool {
            let mut state = self.state.lock().expect("targeted 投影状态锁未损坏");
            state.durable_through = batch.projection_batch_sequence;
            state
                .semantic_batches
                .push(semantic_projection_batch(&batch));
            for response in batch.direct_responses() {
                let (global_request_id, method, body) = match response {
                    BusinessDirectResponseV2::PostOrder(response) => (
                        &response.metadata.global_request_id,
                        "post_order",
                        serde_json::to_value(response),
                    ),
                    BusinessDirectResponseV2::Order(response) => (
                        &response.metadata.global_request_id,
                        "order",
                        serde_json::to_value(response),
                    ),
                    BusinessDirectResponseV2::Orders(response) => (
                        &response.metadata.global_request_id,
                        "orders",
                        serde_json::to_value(response),
                    ),
                    BusinessDirectResponseV2::Trades(response) => (
                        &response.metadata.global_request_id,
                        "trades",
                        serde_json::to_value(response),
                    ),
                    _ => continue,
                };
                if let Ok(body) = body {
                    state
                        .responses
                        .insert(global_request_id.clone(), CapturedResponse { method, body });
                }
            }
            true
        }

        fn durable_through(&self, _run_id: &str) -> u64 {
            self.state
                .lock()
                .expect("targeted 投影状态锁未损坏")
                .durable_through
        }
    }

    #[cfg(feature = "measurement")]
    fn semantic_projection_batch(batch: &BusinessProjectionBatchV2) -> Value {
        let direct_responses = batch
            .direct_responses()
            .map(|response| match response {
                BusinessDirectResponseV2::PostOrder(value) => {
                    json!({"method":"post_order","value":value})
                }
                BusinessDirectResponseV2::PostOrders(value) => {
                    json!({"method":"post_orders","value":value})
                }
                BusinessDirectResponseV2::CancelOrder(value) => {
                    json!({"method":"cancel_order","value":value})
                }
                BusinessDirectResponseV2::CancelOrders(value) => {
                    json!({"method":"cancel_orders","value":value})
                }
                BusinessDirectResponseV2::CancelMarketOrders(value) => {
                    json!({"method":"cancel_market_orders","value":value})
                }
                BusinessDirectResponseV2::CancelAllOrders(value) => {
                    json!({"method":"cancel_all_orders","value":value})
                }
                BusinessDirectResponseV2::Order(value) => {
                    json!({"method":"order","value":value})
                }
                BusinessDirectResponseV2::Orders(value) => {
                    json!({"method":"orders","value":value})
                }
                BusinessDirectResponseV2::Trades(value) => {
                    json!({"method":"trades","value":value})
                }
            })
            .collect::<Vec<_>>();
        json!({
            "projection_batch_sequence": batch.projection_batch_sequence,
            "account_opening": batch.account_opening,
            "market_enrollment": batch.market_enrollment,
            "direct_responses": direct_responses,
            "order_lifecycle": batch.order_lifecycle,
            "trade_lifecycle": batch.trade_lifecycle,
            "settlement_lifecycle": batch.settlement_lifecycle,
        })
    }

    #[derive(Default)]
    struct AcceptingProjectionPort {
        durable_through: u64,
    }

    impl BusinessProjectionPortV2 for AcceptingProjectionPort {
        fn try_accept(&mut self, batch: Arc<BusinessProjectionBatchV2>) -> bool {
            self.durable_through = batch.projection_batch_sequence;
            true
        }

        fn durable_through(&self, _run_id: &str) -> u64 {
            self.durable_through
        }
    }

    pub fn main() -> Result<(), String> {
        let run_started = Instant::now();
        let args = parse_args()?;
        match args.mode {
            RunMode::GoldenMatrix => run_golden_matrix(args, run_started),
            RunMode::TargetedRetention24h => run_targeted_retention(args, run_started),
            RunMode::ComponentConformance => run_component_conformance(args),
            RunMode::GateARealVertical => run_gate_a_real_vertical(args, run_started),
            #[cfg(feature = "gate-b-flow")]
            RunMode::GateBFlow => gate_b::run(args, run_started),
        }
    }

    #[cfg(feature = "measurement")]
    #[derive(Clone, Debug, PartialEq, Eq, Serialize)]
    pub(super) struct ComponentAttemptEvidence {
        pub(super) accepted_input_count: u64,
        pub(super) projection_batches: Vec<Value>,
        pub(super) log_sha256: String,
        pub(super) log_bytes: u64,
        pub(super) cold_candidate_run_id: String,
    }

    #[cfg(feature = "gate-a-measurement")]
    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
    struct GateAPathStats {
        buckets: u64,
        index_rows_scanned: u64,
        execution_market_events: u64,
        first_index_at_utc_ms: Option<i64>,
        last_index_at_utc_ms: Option<i64>,
        first_source_event_at_utc_ms: Option<i64>,
        last_source_event_at_utc_ms: Option<i64>,
        first_received_at_utc_ms: Option<i64>,
        last_received_at_utc_ms: Option<i64>,
        max_execution_market_events_per_ms: u64,
        initial_l2_snapshots: u64,
        tick_size_changes: u64,
        public_trades: u64,
        book_resync_delta_inferences: u64,
        market_resolutions: u64,
        end_of_input_seen: bool,
        final_next_logical_position: u64,
    }

    #[cfg(feature = "gate-a-measurement")]
    impl GateAPathStats {
        fn observe(&mut self, bucket: &ClosedExecutionMarketIndexBucketV1) {
            self.buckets = self.buckets.saturating_add(1);
            self.index_rows_scanned = self.index_rows_scanned.saturating_add(
                bucket
                    .next_logical_position()
                    .saturating_sub(bucket.start_logical_position()),
            );
            let event_count = u64::try_from(bucket.events().len()).unwrap_or(u64::MAX);
            self.execution_market_events = self.execution_market_events.saturating_add(event_count);
            self.max_execution_market_events_per_ms =
                self.max_execution_market_events_per_ms.max(event_count);
            self.first_index_at_utc_ms
                .get_or_insert(bucket.index_at_utc_ms());
            self.last_index_at_utc_ms = Some(bucket.index_at_utc_ms());
            self.final_next_logical_position = bucket.next_logical_position();
            self.end_of_input_seen |= bucket.closure() == ReplayIndexClosureV1::EndOfInput;
            for event in bucket.events().iter() {
                if let Some(at) = event.time.source_event_at_utc_ms {
                    self.first_source_event_at_utc_ms.get_or_insert(at);
                    self.last_source_event_at_utc_ms = Some(at);
                }
                self.first_received_at_utc_ms
                    .get_or_insert(event.time.received_at_utc_ms);
                self.last_received_at_utc_ms = Some(event.time.received_at_utc_ms);
                match &event.payload {
                    ExecutionMarketEventPayloadV3::InitialL2Snapshot(_) => {
                        self.initial_l2_snapshots += 1;
                    }
                    ExecutionMarketEventPayloadV3::TickSizeChange(_) => {
                        self.tick_size_changes += 1;
                    }
                    ExecutionMarketEventPayloadV3::PublicTrade(_) => {
                        self.public_trades += 1;
                    }
                    ExecutionMarketEventPayloadV3::LevelExecutionTransition(transition)
                        if transition.basis
                            == contracts::LevelExecutionBasis::BookResyncDeltaInference =>
                    {
                        self.book_resync_delta_inferences += 1;
                    }
                    ExecutionMarketEventPayloadV3::MarketResolutionTerminal(_) => {
                        self.market_resolutions += 1;
                    }
                    _ => {}
                }
            }
        }

        fn logical_span_ms(&self) -> u64 {
            match (self.first_index_at_utc_ms, self.last_index_at_utc_ms) {
                (Some(first), Some(last)) => {
                    u64::try_from(last.saturating_sub(first)).unwrap_or_default()
                }
                _ => 0,
            }
        }

        fn average_events_per_second(&self) -> u64 {
            self.execution_market_events
                .saturating_mul(1_000)
                .checked_div(self.logical_span_ms().max(1))
                .unwrap_or_default()
        }
    }

    #[cfg(feature = "gate-a-measurement")]
    #[derive(Default)]
    struct GateALatencySamples {
        by_phase: BTreeMap<String, Vec<u64>>,
        count: u64,
    }

    #[cfg(feature = "gate-a-measurement")]
    impl GateALatencySamples {
        fn record(
            &mut self,
            writer: &mut BufWriter<File>,
            input_ordinal: u64,
            execution: &SimExecution,
        ) -> Result<(), String> {
            for observation in execution.drain_measurement_observations() {
                let encoded = serde_json::to_value(&observation)
                    .map_err(|error| format!("编码 Gate A 测量观察失败：{error}"))?;
                let phase = encoded
                    .get("phase")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "Gate A 测量观察缺少 phase".to_owned())?;
                self.by_phase
                    .entry(phase.to_owned())
                    .or_default()
                    .push(observation.elapsed_ns);
                self.count = self.count.saturating_add(1);
                write_json_line(
                    writer,
                    &json!({
                        "schema_version":"gate_a_real_vertical_measurement_sample_v1",
                        "sample_kind":"sim_measurement_observation",
                        "input_ordinal":input_ordinal,
                        "observation":encoded
                    }),
                )?;
            }
            Ok(())
        }

        fn summaries(&self) -> BTreeMap<String, DistributionSummary> {
            self.by_phase
                .iter()
                .filter(|(_, values)| !values.is_empty())
                .map(|(phase, values)| (phase.clone(), distribution_summary(values.clone())))
                .collect()
        }
    }

    #[cfg(feature = "gate-a-measurement")]
    fn write_json_line(writer: &mut BufWriter<File>, value: &Value) -> Result<(), String> {
        serde_json::to_writer(&mut *writer, value)
            .map_err(|error| format!("编码 Gate A JSONL 样本失败：{error}"))?;
        writer
            .write_all(b"\n")
            .map_err(|error| format!("写 Gate A JSONL 样本失败：{error}"))
    }

    #[cfg(feature = "gate-a-measurement")]
    fn record_stage_sample(
        writer: &mut BufWriter<File>,
        phase: &str,
        elapsed_ns: u64,
    ) -> Result<(), String> {
        write_json_line(
            writer,
            &json!({
                "schema_version":"gate_a_real_vertical_measurement_sample_v1",
                "sample_kind":"stage_timing",
                "phase":phase,
                "elapsed_ns":elapsed_ns
            }),
        )
    }

    #[cfg(feature = "gate-a-measurement")]
    fn record_resource_checkpoint(
        writer: &mut BufWriter<File>,
        label: &str,
        input_ordinal: u64,
        snapshot: &sim_system::execution::MeasurementResourceSnapshot,
        high_water: &mut Value,
    ) -> Result<(), String> {
        let value = serde_json::to_value(snapshot)
            .map_err(|error| format!("编码 Gate A 业务资源检查点失败：{error}"))?;
        merge_numeric_max(high_water, &value);
        write_json_line(
            writer,
            &json!({
                "schema_version":"gate_a_real_vertical_measurement_sample_v1",
                "sample_kind":"business_resource_checkpoint",
                "label":label,
                "input_ordinal":input_ordinal,
                "resources":value
            }),
        )
    }

    #[cfg(feature = "gate-a-measurement")]
    fn proc_max(left: ProcSnapshot, right: ProcSnapshot) -> ProcSnapshot {
        ProcSnapshot {
            cpu_ticks: left.cpu_ticks.max(right.cpu_ticks),
            rss_bytes: left.rss_bytes.max(right.rss_bytes),
            rss_high_water_bytes: left.rss_high_water_bytes.max(right.rss_high_water_bytes),
            io_read_bytes: left.io_read_bytes.max(right.io_read_bytes),
            io_write_bytes: left.io_write_bytes.max(right.io_write_bytes),
        }
    }

    #[cfg(feature = "gate-a-measurement")]
    pub(super) fn observe_unique_gate_a_event_ids<'a>(
        unique_event_ids: &mut HashSet<String>,
        event_ids: impl IntoIterator<Item = &'a str>,
    ) {
        unique_event_ids.extend(event_ids.into_iter().map(str::to_owned));
    }

    #[cfg(feature = "gate-a-measurement")]
    fn real_gate_a_scenario() -> Result<Arc<SealedPlatformScenarioV1>, String> {
        let scenario = PlatformScenarioV1::try_new(vec![market(
            CONDITION,
            "3278040",
            TOKEN_UP,
            TOKEN_DOWN,
            MARKET_ONE_START,
            GATE_A_MARKET_END,
        )?])
        .map_err(|error| format!("构造 Gate A 代表市场场景失败：{error}"))?;
        let artifact = SimulationScenarioArtifactV1 {
            schema_version: PLATFORM_SCENARIO_SCHEMA_V1.to_owned(),
            scenario_sha256: scenario.scenario_sha256().clone(),
            market_set_sha256: scenario.market_set_sha256().clone(),
            markets: scenario.market_set().markets().to_vec(),
        };
        let bytes = serde_json::to_vec(&artifact)
            .map_err(|error| format!("编码 Gate A 代表市场场景失败：{error}"))?;
        SealedPlatformScenarioV1::decode_and_seal(&bytes)
            .map(Arc::new)
            .map_err(|error| format!("密封 Gate A 代表市场场景失败：{error}"))
    }

    #[cfg(feature = "gate-a-measurement")]
    fn gate_a_bucket(
        path: MarketEventPathV1,
        bucket: &ClosedExecutionMarketIndexBucketV1,
    ) -> AcceptedClosedMarketBucketV2 {
        AcceptedClosedMarketBucketV2 {
            reference: DurableMarketBucketReferenceV2 {
                path,
                bucket_at_utc_ms: bucket.index_at_utc_ms(),
                start_index_position: bucket.start_logical_position(),
                next_index_position: bucket.next_logical_position(),
                closure: match bucket.closure() {
                    ReplayIndexClosureV1::NextGlobalEventAt(at) => {
                        MillisecondBucketClosureV1::NextEventAt(at)
                    }
                    ReplayIndexClosureV1::EndOfInput => MillisecondBucketClosureV1::EndOfInput,
                },
            },
            events: Arc::clone(bucket.events()),
        }
    }

    #[cfg(feature = "gate-a-measurement")]
    fn stage5_component_binding(args: &GateARealVerticalArgs) -> Result<Value, String> {
        let actual_sha = sha256_file(&args.stage5_component_manifest)?;
        if actual_sha != args.stage5_component_manifest_sha256.0 {
            return Err(format!(
                "阶段 5 component manifest SHA-256 不匹配：expected={}, actual={actual_sha}",
                args.stage5_component_manifest_sha256.0
            ));
        }
        let value: Value = serde_json::from_slice(
            &fs::read(&args.stage5_component_manifest)
                .map_err(|error| format!("读取阶段 5 component manifest 失败：{error}"))?,
        )
        .map_err(|error| format!("解析阶段 5 component manifest 失败：{error}"))?;
        if value.get("schema_version").and_then(Value::as_str)
            != Some("simulation_run_manifest_v2_envelope")
            || value.get("component_receipt_id").and_then(Value::as_str)
                != Some(args.stage5_component_receipt_id.0.as_str())
            || value.pointer("/content/component_scope/COMPONENT_GOLDEN")
                != Some(&Value::Bool(true))
        {
            return Err("阶段 5 component manifest 身份或 COMPONENT_GOLDEN 范围不匹配".to_owned());
        }
        Ok(json!({
            "path":args.stage5_component_manifest,
            "manifest_file_sha256":actual_sha,
            "component_receipt_id":args.stage5_component_receipt_id,
            "implementation_identity":value.pointer("/content/implementation_identity"),
            "component_scope":value.pointer("/content/component_scope")
        }))
    }

    #[cfg(feature = "gate-a-measurement")]
    fn gate_a_inventory(final_gate_a: &PublishedGateAFinal) -> Result<Vec<Value>, String> {
        let candidate = final_gate_a.artifacts();
        let mut inventory = Vec::new();
        for artifact in [
            &candidate.canonical_event_store,
            &candidate.event_time_index,
            &candidate.visibility_index,
            &candidate.market_observations,
            &candidate.reduction_reports,
        ] {
            let data_path = artifact
                .artifact_path()
                .strip_prefix(final_gate_a.root())
                .map_err(|error| format!("计算 Gate A 数据相对路径失败：{error}"))?;
            inventory.push(json!({
                "path":data_path,
                "bytes":artifact.manifest().file.bytes,
                "sha256":artifact.manifest().file.sha256,
                "rows":artifact.manifest().file.rows,
                "logical_sha256":artifact.manifest().file.logical_sha256
            }));
            let manifest_path = artifact
                .manifest_path()
                .strip_prefix(final_gate_a.root())
                .map_err(|error| format!("计算 Gate A manifest 相对路径失败：{error}"))?;
            inventory.push(json!({
                "path":manifest_path,
                "bytes":fs::metadata(artifact.manifest_path())
                    .map_err(|error| format!("读取 Gate A manifest metadata 失败：{error}"))?
                    .len(),
                "sha256":artifact.manifest_sha256()
            }));
        }
        inventory.sort_by(|left, right| left["path"].as_str().cmp(&right["path"].as_str()));
        Ok(inventory)
    }

    #[cfg(feature = "gate-a-measurement")]
    pub(super) fn joint_manifest_envelope(content: Value) -> Result<Value, String> {
        let content_bytes = serde_json::to_vec(&content)
            .map_err(|error| format!("编码联合 manifest content 失败：{error}"))?;
        let receipt_id = format!("{:x}", Sha256::digest(content_bytes));
        Ok(json!({
            "schema_version":"joint_real_vertical_manifest_v1_envelope",
            "joint_receipt_id":receipt_id,
            "content":content
        }))
    }

    fn run_gate_a_real_vertical(args: Args, run_started: Instant) -> Result<(), String> {
        #[cfg(not(feature = "gate-a-measurement"))]
        {
            let _ = (args, run_started);
            return Err("--gate-a-real-vertical 只允许 gate-a-measurement feature 构建".to_owned());
        }
        #[cfg(feature = "gate-a-measurement")]
        {
            let gate = args
                .gate_a
                .as_ref()
                .expect("parser 已绑定 Gate A real vertical 参数");
            if args.output_dir.exists() {
                return Err(format!("联合最终目标已存在：{}", args.output_dir.display()));
            }
            let parent = args
                .output_dir
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            fs::create_dir_all(parent).map_err(|error| format!("创建联合父目录失败：{error}"))?;
            let final_name = args
                .output_dir
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| "联合输出目标必须以 UTF-8 文件名结束".to_owned())?;
            let candidate = parent.join(format!(".{final_name}.candidate"));
            if candidate.exists() {
                return Err(format!("联合 candidate 已存在：{}", candidate.display()));
            }
            let current_head = command_output("git", &["rev-parse", "HEAD"]);
            if current_head != args.identity.git_commit {
                return Err(format!(
                    "实现提交身份不匹配：arg={}, HEAD={current_head}",
                    args.identity.git_commit
                ));
            }
            let current_lock_sha = sha256_file(Path::new("Cargo.lock"))?;
            if current_lock_sha != args.identity.cargo_lock_sha256.0 {
                return Err(format!(
                    "Cargo.lock 身份不匹配：arg={}, actual={current_lock_sha}",
                    args.identity.cargo_lock_sha256.0
                ));
            }
            let stage5_binding = stage5_component_binding(gate)?;
            fs::create_dir(&candidate)
                .map_err(|error| format!("创建联合 candidate 失败：{error}"))?;

            let samples_path = candidate.join("measurement-samples.jsonl");
            let mut samples_writer = BufWriter::new(
                File::create(&samples_path)
                    .map_err(|error| format!("创建 Gate A 测量样本失败：{error}"))?,
            );
            let proc_before = proc_snapshot()?;
            let mut proc_high_water = proc_before;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("创建 Gate A CURRENT_QUERY runtime 失败：{error}"))?;

            let rule_sync_started = Instant::now();
            let rule_snapshot = runtime
                .block_on(query_and_seal_current_process_snapshot_v3(
                    CurrentProcessSnapshotRequestV1 {
                        reference_market: reference_market(),
                        target_path: candidate.join("platform-rule-snapshot-v3.json"),
                        implementation_identity: args.identity.clone(),
                    },
                ))
                .map_err(|error| format!("Gate A runner 唯一 CURRENT_QUERY 失败：{error}"))?;
            let rule_sync_elapsed_ns = elapsed_ns(rule_sync_started);
            record_stage_sample(
                &mut samples_writer,
                "current_query_rule_sync",
                rule_sync_elapsed_ns,
            )?;
            proc_high_water = proc_max(proc_high_water, proc_snapshot()?);

            let gate_a_open_started = Instant::now();
            let final_gate_a = open_published_gate_a_final(&gate.gate_a_root)
                .map_err(|error| format!("只读打开 Gate A 产物失败：{error}"))?;
            let gate_a_open_elapsed_ns = elapsed_ns(gate_a_open_started);
            record_stage_sample(
                &mut samples_writer,
                "open_gate_a_final",
                gate_a_open_elapsed_ns,
            )?;
            proc_high_water = proc_max(proc_high_water, proc_snapshot()?);

            let output = data_pipeline_training::gate_a_formal_output_config();
            let mut adapter = GateASimulationReplayAdapter::open(
                &final_gate_a,
                output.sort.batch_rows,
                output.sort.max_run_bytes,
                output.sort.batch_rows,
            )
            .map_err(|error| format!("打开 Gate A 双路适配器失败：{error}"))?;
            let scenario = real_gate_a_scenario()?;
            let finality = successful_finality()?;
            let latency = load_embedded_proxy_latency_profile()
                .map_err(|error| format!("加载 Gate A runner 时延档失败：{error}"))?;
            let log_path = candidate.join("sim-event-log-v2.working.jsonl");
            let opened = RunOpenedV2 {
                projection_version: Default::default(),
                run_id: GATE_A_RUNNER_IDENTITY.to_owned(),
                durability_role: RunDurabilityRoleV2::CleanRun,
                platform_rule_snapshot_file_sha256: rule_snapshot.file_sha256().clone(),
                trade_finality_policy_file_sha256: finality.policy_id().clone(),
                platform_scenario_file_sha256: scenario.file_sha256().clone(),
                proxy_latency_profile_file_sha256: latency.file_sha256().clone(),
                implementation_identity: args.identity.clone(),
                market_event_source_identity_sha256: Some(
                    adapter.canonical_event_store_manifest_sha256().clone(),
                ),
            };
            let mut execution = SimExecution::create_new(
                &log_path,
                opened,
                Arc::clone(&rule_snapshot),
                Arc::clone(&finality),
                Arc::clone(&scenario),
                Arc::clone(&latency),
                Box::<AcceptingProjectionPort>::default(),
                MAX_GROUP_BYTES,
            )
            .map_err(|error| format!("创建 Gate A 真实驱动根失败：{error}"))?;
            execution
                .create_session(SessionSpecV3 {
                    run_id: GATE_A_RUNNER_IDENTITY.to_owned(),
                    session_id: "stage-6-gate-a-real-vertical-session".to_owned(),
                    account_id: "stage-6-gate-a-real-vertical-account".to_owned(),
                    caller_role: SimCallerRoleV1::OfflineEvaluation,
                    caller_instance_id: "stage-6-gate-a-real-vertical-caller".to_owned(),
                    creation_request_id: "stage-6-gate-a-real-vertical-create".to_owned(),
                    response_bridge_binding_id: "stage-6-gate-a-real-vertical-bridge".to_owned(),
                    initial_account_state: InitialAccountStateV2 {
                        available_pusd: ExactDecimal("1000".to_owned()),
                    },
                    session_seed: Sha256Hex("8".repeat(64)),
                })
                .map_err(|error| format!("创建 Gate A 真实驱动会话失败：{error}"))?;
            execution
                .activate_session("stage-6-gate-a-real-vertical-activate".to_owned())
                .map_err(|error| format!("激活 Gate A 真实驱动会话失败：{error}"))?;

            let mut business_high_water = Value::Null;
            let session_opened_resources = execution.measurement_resource_snapshot();
            record_resource_checkpoint(
                &mut samples_writer,
                "session_opened",
                0,
                &session_opened_resources,
                &mut business_high_water,
            )?;
            let log_bytes_after_session_open = fs::metadata(&log_path)
                .map_err(|error| format!("读取会话开启日志大小失败：{error}"))?
                .len();
            let mut latency_samples = GateALatencySamples::default();
            latency_samples.record(&mut samples_writer, 0, &execution)?;
            let drive_started = Instant::now();
            let mut platform = adapter
                .next_platform_bucket()
                .map_err(|error| format!("读取 Gate A 平台首桶失败：{error}"))?;
            let mut visibility = adapter
                .next_visibility_bucket()
                .map_err(|error| format!("读取 Gate A 可见性首桶失败：{error}"))?;
            let mut platform_stats = GateAPathStats::default();
            let mut visibility_stats = GateAPathStats::default();
            let mut drive_ordinal = 0_u64;
            let mut prefix_advances = 0_u64;
            let mut request_count = 0_u64;
            let mut unique_event_ids = HashSet::new();
            while platform.is_some() || visibility.is_some() {
                let next_bucket_at = match (&platform, &visibility) {
                    (Some(left), Some(right)) => {
                        left.index_at_utc_ms().min(right.index_at_utc_ms())
                    }
                    (Some(bucket), None) | (None, Some(bucket)) => bucket.index_at_utc_ms(),
                    (None, None) => unreachable!(),
                };
                if let Some(required) = execution.next_required_logical_drive()
                    && required.required_at_utc_ms < next_bucket_at
                {
                    let next_index_position = match required.path {
                        MarketEventPathV1::HiddenPlatform => platform.as_ref().map_or_else(
                            || adapter.platform_next_logical_position(),
                            ClosedExecutionMarketIndexBucketV1::start_logical_position,
                        ),
                        MarketEventPathV1::PublicVisibility => visibility.as_ref().map_or_else(
                            || adapter.visibility_next_logical_position(),
                            ClosedExecutionMarketIndexBucketV1::start_logical_position,
                        ),
                    };
                    execution
                        .drive_platform_prefix(sim_system::execution::PlatformPrefixAdvanceV2 {
                            path: required.path,
                            closed_through_utc_ms: required.required_at_utc_ms,
                            next_index_position,
                        })
                        .map_err(|error| {
                            format!(
                                "Gate A 必要空前缀推进失败：ordinal={drive_ordinal}, required={required:?}, error={error}"
                            )
                        })?;
                    drive_ordinal = drive_ordinal.saturating_add(1);
                    prefix_advances = prefix_advances.saturating_add(1);
                    latency_samples.record(&mut samples_writer, drive_ordinal, &execution)?;
                    continue;
                }

                let path = match (&platform, &visibility) {
                    (Some(left), Some(right))
                        if left.index_at_utc_ms() <= right.index_at_utc_ms() =>
                    {
                        MarketEventPathV1::HiddenPlatform
                    }
                    (Some(_), None) => MarketEventPathV1::HiddenPlatform,
                    _ => MarketEventPathV1::PublicVisibility,
                };
                let bucket = match path {
                    MarketEventPathV1::HiddenPlatform => platform.take().expect("平台桶存在"),
                    MarketEventPathV1::PublicVisibility => visibility.take().expect("可见性桶存在"),
                };
                observe_unique_gate_a_event_ids(
                    &mut unique_event_ids,
                    bucket
                        .events()
                        .iter()
                        .map(|event| event.event_id.0.as_str()),
                );
                match path {
                    MarketEventPathV1::HiddenPlatform => platform_stats.observe(&bucket),
                    MarketEventPathV1::PublicVisibility => visibility_stats.observe(&bucket),
                }
                if path == MarketEventPathV1::HiddenPlatform
                    && bucket
                        .events()
                        .iter()
                        .any(|event| event.event_id.0 == GATE_A_REPRESENTATIVE_TRADE_EVENT_ID)
                {
                    if request_count != 0 {
                        return Err("Gate A 代表订单触发事件重复".to_owned());
                    }
                    let outbound_delay = i64::try_from(latency.sample_delay_ms(
                        "8".repeat(64).as_bytes(),
                        GATE_A_REQUEST_ID,
                        DelaySampleDomainV1::RequestOutbound,
                    ))
                    .map_err(|_| "Gate A 代表订单出站时延不可表示".to_owned())?;
                    execution
                        .post_order(
                            TrustedDecisionCutoffV2 {
                                logical_time_utc_ms: bucket.index_at_utc_ms() - outbound_delay,
                                public_visibility: PublicVisibilityCursorV1::default(),
                                private_feedback_sequence: 0,
                            },
                            PostOrderRequestV2 {
                                context: RequestContextV2 {
                                    global_request_id: GATE_A_REQUEST_ID.to_owned(),
                                    caller_correlation_id: Some(
                                        "stage-6-gate-a-real-vertical".to_owned(),
                                    ),
                                },
                                item: PostOrderItemV1 {
                                    item_id: "stage-6-gate-a-real-vertical-item".to_owned(),
                                    order: SimBuyOrderV1 {
                                        condition_id: ConditionId(CONDITION.to_owned()),
                                        token_id: TokenId(TOKEN_UP.to_owned()),
                                        kind: SimBuyOrderKindV1::GtcLimit {
                                            limit_price_pusd_per_share: "0.01"
                                                .parse()
                                                .expect("固定 Gate A 代表价格"),
                                            size_shares: rule_snapshot
                                                .synchronized_rules()
                                                .minimum_order_size_shares
                                                .value(),
                                            post_only: false,
                                        },
                                    },
                                },
                            },
                        )
                        .map_err(|error| format!("登记 Gate A 代表订单失败：{error}"))?;
                    request_count = 1;
                    drive_ordinal = drive_ordinal.saturating_add(1);
                    latency_samples.record(&mut samples_writer, drive_ordinal, &execution)?;
                }
                execution
                    .drive_closed_market_bucket(gate_a_bucket(path, &bucket))
                    .map_err(|error| {
                        let event_ids = bucket
                            .events()
                            .iter()
                            .map(|event| event.event_id.0.as_str())
                            .collect::<Vec<_>>();
                        format!(
                            "Gate A 真实驱动首分歧：ordinal={}, path={path:?}, time={}, event_ids={event_ids:?}, error={error}",
                            drive_ordinal + 1,
                            bucket.index_at_utc_ms()
                        )
                    })?;
                drive_ordinal = drive_ordinal.saturating_add(1);
                latency_samples.record(&mut samples_writer, drive_ordinal, &execution)?;
                match path {
                    MarketEventPathV1::HiddenPlatform => {
                        platform = adapter
                            .next_platform_bucket()
                            .map_err(|error| format!("读取 Gate A 平台下一桶失败：{error}"))?;
                    }
                    MarketEventPathV1::PublicVisibility => {
                        visibility = adapter
                            .next_visibility_bucket()
                            .map_err(|error| format!("读取 Gate A 可见性下一桶失败：{error}"))?;
                    }
                }
                let total_buckets = platform_stats
                    .buckets
                    .saturating_add(visibility_stats.buckets);
                if total_buckets.is_multiple_of(4_096) {
                    let resources = execution.measurement_resource_snapshot();
                    record_resource_checkpoint(
                        &mut samples_writer,
                        "periodic",
                        drive_ordinal,
                        &resources,
                        &mut business_high_water,
                    )?;
                    proc_high_water = proc_max(proc_high_water, proc_snapshot()?);
                }
            }
            let real_drive_elapsed_ns = elapsed_ns(drive_started);
            record_stage_sample(
                &mut samples_writer,
                "real_dual_path_drive",
                real_drive_elapsed_ns,
            )?;
            if !platform_stats.end_of_input_seen || !visibility_stats.end_of_input_seen {
                return Err("Gate A 双路未同时到达 EndOfInput".to_owned());
            }
            if let Some(required) = execution.next_required_logical_drive() {
                return Err(format!("Gate A 双路结束后仍有必要逻辑推进：{required:?}"));
            }
            if request_count != 1 {
                return Err(format!("Gate A 代表请求数必须为 1，actual={request_count}"));
            }
            let unique_event_count = u64::try_from(unique_event_ids.len())
                .expect("Gate A event_id 集合基数必须可表示为 u64");
            let final_resources = execution.measurement_resource_snapshot();
            record_resource_checkpoint(
                &mut samples_writer,
                "end_of_input",
                drive_ordinal,
                &final_resources,
                &mut business_high_water,
            )?;
            proc_high_water = proc_max(proc_high_water, proc_snapshot()?);
            let log_bytes = fs::metadata(&log_path)
                .map_err(|error| format!("读取正式工作日志大小失败：{error}"))?
                .len();
            let log_sha256 = sha256_file(&log_path)?;
            let log_growth_bytes = log_bytes.saturating_sub(log_bytes_after_session_open);
            drop(execution);
            fs::remove_file(&log_path)
                .map_err(|error| format!("删除 bundle 外正式工作日志失败：{error}"))?;

            let end_to_end_elapsed_ns = elapsed_ns(run_started);
            let unique_events_per_second_drive = unique_event_count
                .saturating_mul(1_000_000_000)
                .checked_div(real_drive_elapsed_ns.max(1))
                .unwrap_or_default();
            let unique_events_per_second_end_to_end = unique_event_count
                .saturating_mul(1_000_000_000)
                .checked_div(end_to_end_elapsed_ns.max(1))
                .unwrap_or_default();
            let gate_a_input_inventory = gate_a_inventory(&final_gate_a)?;
            let binary_path = std::env::current_exe()
                .map_err(|error| format!("定位 Gate A runner 二进制失败：{error}"))?;
            let binary_sha256 = sha256_file(&binary_path)?;
            let report_path = candidate.join("representative-market-report.json");
            let report = json!({
                "schema_version":"representative_market_report_v1",
                "runner_identity":GATE_A_RUNNER_IDENTITY,
                "workload_identity":GATE_A_WORKLOAD_IDENTITY,
                "representative_market":{
                    "condition_id":CONDITION,
                    "slug":SLUG,
                    "clob_market_id":"3278040",
                    "market_start_at_utc_ms":MARKET_ONE_START,
                    "market_end_at_utc_ms":GATE_A_MARKET_END,
                    "outcomes":[{"token_id":TOKEN_UP,"outcome":"Up"},{"token_id":TOKEN_DOWN,"outcome":"Down"}]
                },
                "input_counts":{
                    "unique_execution_market_events":unique_event_count,
                    "canonical_store_rows_including_reference":final_gate_a.artifacts().canonical_event_store.manifest().file.rows,
                    "platform_path":platform_stats,
                    "visibility_path":visibility_stats,
                    "raw_rows_are_not_reported_as_sim_events":true,
                    "dual_path_sum_is_not_reported_as_unique_events":true
                },
                "throughput":{
                    "real_drive_elapsed_ns":real_drive_elapsed_ns,
                    "unique_events_per_second_real_drive":unique_events_per_second_drive,
                    "end_to_end_through_input_completion_elapsed_ns":end_to_end_elapsed_ns,
                    "unique_events_per_second_end_to_end":unique_events_per_second_end_to_end,
                    "platform_average_events_per_logical_second":platform_stats.average_events_per_second(),
                    "visibility_average_events_per_logical_second":visibility_stats.average_events_per_second(),
                    "pr57_elapsed_seconds_not_reused_as_throughput":3057.73
                },
                "request_load":{
                    "representative_requests":request_count,
                    "request_id":GATE_A_REQUEST_ID,
                    "source_event_trigger":GATE_A_REPRESENTATIVE_TRADE_EVENT_ID
                },
                "latency_ns_nearest_rank":latency_samples.summaries(),
                "logical_terminal":{
                    "platform_end_of_input":platform_stats.end_of_input_seen,
                    "visibility_end_of_input":visibility_stats.end_of_input_seen,
                    "platform_next_logical_position":adapter.platform_next_logical_position(),
                    "visibility_next_logical_position":adapter.visibility_next_logical_position(),
                    "next_required_logical_drive":Value::Null,
                    "max_page_rows":adapter.max_page_rows()
                },
                "pr57_inherited_scale_facts":{
                    "closed_buckets":156783,
                    "public_trades":1918,
                    "book_resync_delta_inferences":362,
                    "resync_boundaries":362,
                    "not_a_new_throughput_measurement":true
                }
            });
            let report_seal_started = Instant::now();
            write_json(report_path.clone(), &report)?;
            File::open(&report_path)
                .and_then(|file| file.sync_all())
                .map_err(|error| format!("同步代表市场报告失败：{error}"))?;
            let report_seal_elapsed_ns = elapsed_ns(report_seal_started);
            record_stage_sample(
                &mut samples_writer,
                "seal_representative_market_report",
                report_seal_elapsed_ns,
            )?;
            samples_writer
                .flush()
                .map_err(|error| format!("刷新 Gate A 测量样本失败：{error}"))?;
            File::open(&samples_path)
                .and_then(|file| file.sync_all())
                .map_err(|error| format!("同步 Gate A 测量样本失败：{error}"))?;
            drop(samples_writer);
            let samples_sha256 = sha256_file(&samples_path)?;
            let report_sha256 = sha256_file(&report_path)?;
            let proc_after = proc_snapshot()?;
            proc_high_water = proc_max(proc_high_water, proc_after);
            let resource_path = candidate.join("resource-baseline.json");
            write_json(
                resource_path.clone(),
                &json!({
                    "schema_version":"joint_real_vertical_resource_baseline_v1",
                    "hardware":{
                        "uname":command_output("uname", &["-a"]),
                        "cpu_model":cpu_model()?,
                        "logical_cores":std::thread::available_parallelism()
                            .map(|count| count.get())
                            .map_err(|error| format!("读取逻辑核心数失败：{error}"))?
                    },
                    "build":{
                        "profile":if cfg!(debug_assertions) { "debug" } else { "release" },
                        "git_commit":args.identity.git_commit,
                        "source_identity_sha256":args.identity.source_identity_sha256,
                        "cargo_lock_sha256":args.identity.cargo_lock_sha256,
                        "binary_path":binary_path,
                        "binary_sha256":binary_sha256
                    },
                    "proc_before":proc_before,
                    "proc_after":proc_after,
                    "proc_high_water":proc_high_water,
                    "proc_delta":{
                        "cpu_ticks":proc_after.cpu_ticks.saturating_sub(proc_before.cpu_ticks),
                        "io_read_bytes":proc_after.io_read_bytes.saturating_sub(proc_before.io_read_bytes),
                        "io_write_bytes":proc_after.io_write_bytes.saturating_sub(proc_before.io_write_bytes)
                    },
                    "business_resource_high_water":business_high_water,
                    "business_resource_final":final_resources,
                    "input_sequence_final":final_resources.next_input_sequence,
                    "working_log":{
                        "included_in_bundle":false,
                        "deleted_after_hashing":true,
                        "bytes_after_session_open":log_bytes_after_session_open,
                        "final_bytes":log_bytes,
                        "growth_bytes":log_growth_bytes,
                        "sha256":log_sha256
                    },
                    "phase_elapsed_ns":{
                        "current_query_rule_sync":rule_sync_elapsed_ns,
                        "open_gate_a_final":gate_a_open_elapsed_ns,
                        "real_dual_path_drive":real_drive_elapsed_ns,
                        "seal_representative_market_report":report_seal_elapsed_ns,
                        "end_to_end_through_input_completion":end_to_end_elapsed_ns
                    },
                    "measurement_observation_count":latency_samples.count,
                    "measurement_samples_sha256":samples_sha256,
                    "representative_market_report_sha256":report_sha256,
                    "no_new_resource_threshold_or_acceptance_gate":true
                }),
            )?;
            let resource_sha256 = sha256_file(&resource_path)?;
            let local_inventory = artifact_inventory(&candidate)?;
            let content = json!({
                "schema_version":"joint_real_vertical_manifest_v1",
                "scope":{
                    "REAL_DPT":true,
                    "COMPONENT_GOLDEN":true,
                    "REAL_PAPER_RECOVERY":"NOT_CLAIMED",
                    "TRAINING_LOOP":"NOT_CLAIMED",
                    "FINAL_TRAINING_RETENTION_POLICY":"NOT_CLAIMED",
                    "PRODUCTION_SLA":"NOT_CLAIMED"
                },
                "implementation_identity":{
                    "git_commit":args.identity.git_commit,
                    "source_identity_sha256":args.identity.source_identity_sha256,
                    "cargo_lock_sha256":args.identity.cargo_lock_sha256,
                    "binary_sha256":binary_sha256,
                    "adapter_identity":"data_pipeline_training::GateASimulationReplayAdapter",
                    "runner_identity":GATE_A_RUNNER_IDENTITY
                },
                "gate_a":{
                    "root":final_gate_a.root(),
                    "input_set_identity_sha256":final_gate_a.artifacts().canonical_event_store.manifest().input_set_identity_sha256,
                    "input_manifest_set_sha256":final_gate_a.artifacts().canonical_event_store.manifest().input_manifest_set_sha256,
                    "artifact_manifests":{
                        "canonical_event_store":final_gate_a.artifacts().canonical_event_store.manifest_sha256(),
                        "event_time_index":final_gate_a.artifacts().event_time_index.manifest_sha256(),
                        "visibility_index":final_gate_a.artifacts().visibility_index.manifest_sha256(),
                        "market_observations":final_gate_a.artifacts().market_observations.manifest_sha256(),
                        "reduction_reports":final_gate_a.artifacts().reduction_reports.manifest_sha256()
                    },
                    "inventory_reference_not_copy":gate_a_input_inventory
                },
                "stage5_component":stage5_binding,
                "current_query_rule_snapshot":{
                    "path":"platform-rule-snapshot-v3.json",
                    "file_sha256":rule_snapshot.file_sha256(),
                    "access_mode":rule_snapshot.access_mode(),
                    "query_count":1,
                    "queried_before_gate_a_open_and_run_root":true,
                    "shared_by_all_runs_in_process":true
                },
                "real_drive":{
                    "workload_identity":GATE_A_WORKLOAD_IDENTITY,
                    "unique_execution_market_events":unique_event_count,
                    "platform_path":platform_stats,
                    "visibility_path":visibility_stats,
                    "prefix_advances":prefix_advances,
                    "request_count":request_count,
                    "max_page_rows":adapter.max_page_rows(),
                    "platform_final_next_logical_position":adapter.platform_next_logical_position(),
                    "visibility_final_next_logical_position":adapter.visibility_next_logical_position(),
                    "platform_index_file_bytes_scanned":final_gate_a.artifacts().event_time_index.manifest().file.bytes,
                    "visibility_index_file_bytes_scanned":final_gate_a.artifacts().visibility_index.manifest().file.bytes,
                    "logical_terminal":true
                },
                "artifacts":{
                    "measurement_samples":{"path":"measurement-samples.jsonl","sha256":samples_sha256},
                    "representative_market_report":{"path":"representative-market-report.json","sha256":report_sha256},
                    "resource_baseline":{"path":"resource-baseline.json","sha256":resource_sha256},
                    "platform_rule_snapshot":{"path":"platform-rule-snapshot-v3.json","sha256":rule_snapshot.file_sha256()}
                },
                "local_inventory":local_inventory,
                "local_inventory_excludes_manifest_itself":true,
                "no_simulation_run_manifest_v2_generated":true,
                "dpt_remained_offline_and_sim_used_only_public_readers":true
            });
            let envelope = joint_manifest_envelope(content)?;
            let receipt_id = envelope["joint_receipt_id"]
                .as_str()
                .expect("联合 receipt ID 必须为字符串")
                .to_owned();
            write_json(
                candidate.join("joint-real-vertical-manifest-v1.json"),
                &envelope,
            )?;
            sync_tree(&candidate)?;
            publish_candidate(&candidate, &args.output_dir)?;
            eprintln!(
                "Gate A joint real vertical PASS: receipt_id={receipt_id},output={},unique_events={unique_event_count},platform_buckets={},visibility_buckets={},drive_events_per_second={unique_events_per_second_drive},end_to_end_events_per_second={unique_events_per_second_end_to_end}",
                args.output_dir.display(),
                platform_stats.buckets,
                visibility_stats.buckets,
            );
            Ok(())
        }
    }

    fn run_component_conformance(args: Args) -> Result<(), String> {
        #[cfg(not(feature = "measurement"))]
        {
            let _ = args;
            return Err("--component-conformance 只允许 measurement feature 构建".to_owned());
        }
        #[cfg(feature = "measurement")]
        {
            let component = args
                .component
                .as_ref()
                .expect("parser 已绑定 component 参数");
            if args.output_dir.exists() {
                return Err(format!("组件最终目标已存在：{}", args.output_dir.display()));
            }
            let parent = args
                .output_dir
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            fs::create_dir_all(parent).map_err(|error| format!("创建组件父目录失败：{error}"))?;
            let final_name = args
                .output_dir
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| "组件输出目标必须以 UTF-8 文件名结束".to_owned())?;
            let candidate = parent.join(format!(".{final_name}.candidate"));
            if candidate.exists() {
                return Err(format!("组件 candidate 已存在：{}", candidate.display()));
            }
            fs::create_dir(&candidate)
                .map_err(|error| format!("创建组件 candidate 失败：{error}"))?;

            let snapshot =
                load_historical_process_snapshot_v3(HistoricalProcessSnapshotRequestV1 {
                    snapshot_path: component.rule_snapshot.clone(),
                    expected_file_sha256: component.rule_snapshot_sha256.clone(),
                    replay_causal_start_utc_ms: COMPONENT_CAUSAL_START_UTC_MS,
                })
                .map_err(|error| format!("离线加载 V3 历史规则失败：{error}"))?;
            let scenario = component_fixed_scenario()?;
            let finality = successful_finality()?;
            let latency = load_embedded_proxy_latency_profile()
                .map_err(|error| format!("加载嵌入时延档失败：{error}"))?;

            fs::create_dir_all(candidate.join("inputs"))
                .map_err(|error| format!("创建组件输入目录失败：{error}"))?;
            copy_verified(
                &component.rule_snapshot,
                &component.rule_snapshot_sha256.0,
                &candidate.join("inputs/platform-rule-snapshot-v3.json"),
            )?;
            fs::write(
                candidate.join("inputs/platform-scenario-v1.json"),
                component_fixed_scenario_bytes()?,
            )
            .map_err(|error| format!("写组件场景输入失败：{error}"))?;
            fs::write(
                candidate.join("inputs/trade-finality-policy-v2.json"),
                successful_finality_bytes()?,
            )
            .map_err(|error| format!("写组件最终性输入失败：{error}"))?;
            let latency_source = include_bytes!(
                "../../config/latency-profiles/98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596/profile.json"
            );
            fs::write(
                candidate.join("inputs/proxy-latency-profile-v1.json"),
                latency_source,
            )
            .map_err(|error| format!("写组件时延输入失败：{error}"))?;

            let template_buckets = golden_buckets_with_shift(&scenario, COMPONENT_TIME_SHIFT_MS)?;
            let golden_path = candidate.join("inputs/component-golden-events-v3.jsonl");
            let buckets = write_and_read_component_events(&golden_path, &template_buckets)?;
            let golden_sha256 = sha256_file(&golden_path)?;

            let attempts_root = candidate.join("validation-attempts");
            fs::create_dir_all(&attempts_root)
                .map_err(|error| format!("创建组件验证目录失败：{error}"))?;
            let baseline = run_component_attempt(
                "canonical",
                &attempts_root.join("canonical"),
                Arc::clone(&snapshot),
                Arc::clone(&scenario),
                Arc::clone(&finality),
                Arc::clone(&latency),
                args.identity.clone(),
                &golden_sha256,
                &buckets,
                false,
            )?;
            let rerun = run_component_attempt(
                "clean-rerun",
                &attempts_root.join("clean-rerun"),
                Arc::clone(&snapshot),
                Arc::clone(&scenario),
                Arc::clone(&finality),
                Arc::clone(&latency),
                args.identity.clone(),
                &golden_sha256,
                &buckets,
                false,
            )?;
            let perturbed = run_component_attempt(
                "schedule-perturbed",
                &attempts_root.join("schedule-perturbed"),
                Arc::clone(&snapshot),
                Arc::clone(&scenario),
                Arc::clone(&finality),
                Arc::clone(&latency),
                args.identity.clone(),
                &golden_sha256,
                &buckets,
                true,
            )?;
            let comparison = component_comparison(&baseline, &rerun, &perturbed);
            write_json(candidate.join("component-comparison.json"), &comparison)?;
            if comparison["status"] != "PASS" {
                write_json(candidate.join("failure.json"), &comparison)?;
                sync_tree(&candidate)?;
                publish_candidate(&candidate, &failed_output_path(&args.output_dir))?;
                return Err("组件语义比较失败；已原子发布 .failed 证据".to_owned());
            }

            fs::create_dir_all(candidate.join("evidence"))
                .map_err(|error| format!("创建复用证据目录失败：{error}"))?;
            copy_verified(
                &component.stage4_formal_audit,
                &component.stage4_formal_audit_sha256.0,
                &candidate.join("evidence/stage4-formal-gate-audit.json"),
            )?;
            copy_verified(
                &component.retention_manifest,
                &component.retention_manifest_sha256.0,
                &candidate.join("evidence/stage4-retention-manifest.json"),
            )?;
            copy_verified(
                &component.test_evidence,
                &component.test_evidence_sha256.0,
                &candidate.join("evidence/stage5-test-evidence.json"),
            )?;
            write_json(
                candidate.join("component-resource-summary.json"),
                &json!({
                    "schema_version":"component_resource_summary_v1",
                    "canonical_log_bytes":baseline.log_bytes,
                    "stage4_formal_audit_sha256":component.stage4_formal_audit_sha256,
                    "stage4_retention_manifest_sha256":component.retention_manifest_sha256,
                }),
            )?;

            let binary_sha256 = sha256_file(
                &std::env::current_exe().map_err(|error| format!("定位当前二进制失败：{error}"))?,
            )?;
            let inventory = component_artifact_inventory(&candidate)?;
            let content = json!({
                "schema_version":"simulation_run_manifest_v2",
                "component_scope":{
                    "COMPONENT_GOLDEN":true,
                    "REAL_DPT":"NOT_CLAIMED",
                    "REAL_PAPER_RECOVERY":"NOT_CLAIMED",
                    "TRAINING_LOOP":"NOT_CLAIMED",
                    "FINAL_TRAINING_RETENTION_POLICY":"NOT_CLAIMED",
                    "PRODUCTION_SLA":"NOT_CLAIMED"
                },
                "canonical_run":{
                    "run_id":COMPONENT_RUNNER_IDENTITY,
                    "caller_role":"OFFLINE_EVALUATION",
                    "durability":"CLEAN_RUN",
                    "shape":"single_root",
                    "account_id":"component-account",
                    "session_id":"component-session",
                    "trajectory_count":1,
                    "session_seed":"8".repeat(64),
                    "accepted_input_count":baseline.accepted_input_count,
                    "log":"validation-attempts/canonical/sim-event-log-v2.jsonl",
                    "projection":"validation-attempts/canonical/component-projection.json"
                },
                "implementation_identity":{
                    "git_commit":args.identity.git_commit,
                    "source_identity_sha256":args.identity.source_identity_sha256,
                    "cargo_lock_sha256":args.identity.cargo_lock_sha256,
                    "binary_sha256":binary_sha256
                },
                "input_identity":{
                    "rule_snapshot_sha256":component.rule_snapshot_sha256,
                    "scenario_sha256":scenario.file_sha256(),
                    "finality_policy_sha256":finality.policy_id(),
                    "latency_profile_sha256":latency.file_sha256(),
                    "golden_events_file_sha256":golden_sha256,
                    "workload_identity":COMPONENT_WORKLOAD_IDENTITY,
                    "market_count":2
                },
                "reused_evidence":{
                    "stage4_formal_audit_sha256":component.stage4_formal_audit_sha256,
                    "stage4_retention_manifest_sha256":component.retention_manifest_sha256,
                    "stage5_test_evidence_sha256":component.test_evidence_sha256
                },
                "validation_evidence":{
                    "clean_rerun":"component-comparison.json",
                    "schedule_perturbed":"component-comparison.json",
                    "attempts_are_not_formal_run_roots":true
                },
                "artifact_inventory":inventory
            });
            let content_bytes = serde_json::to_vec(&content)
                .map_err(|error| format!("编码组件 manifest content 失败：{error}"))?;
            let receipt_id = format!("{:x}", Sha256::digest(&content_bytes));
            write_json(
                candidate.join("simulation-run-manifest-v2.json"),
                &json!({
                    "schema_version":"simulation_run_manifest_v2_envelope",
                    "component_receipt_id":receipt_id,
                    "content":content
                }),
            )?;
            sync_tree(&candidate)?;
            publish_candidate(&candidate, &args.output_dir)
        }
    }

    #[cfg(feature = "measurement")]
    #[allow(clippy::too_many_arguments)]
    fn run_component_attempt(
        attempt: &str,
        root_dir: &Path,
        snapshot: Arc<SealedProcessRuleSnapshotV3>,
        scenario: Arc<SealedPlatformScenarioV1>,
        finality: Arc<TradeFinalityPolicyV2>,
        latency: Arc<EmpiricalDelayProfile>,
        identity: ImplementationIdentityV1,
        golden_sha256: &str,
        buckets: &[(
            DurableMarketBucketReferenceV2,
            Arc<[ExecutionMarketEventV3]>,
        )],
        perturb: bool,
    ) -> Result<ComponentAttemptEvidence, String> {
        fs::create_dir_all(root_dir).map_err(|error| format!("创建组件运行根失败：{error}"))?;
        let log_path = root_dir.join("sim-event-log-v2.jsonl");
        let capture = Arc::new(Mutex::new(CapturingProjectionState::default()));
        let mut execution = SimExecution::create_new(
            &log_path,
            RunOpenedV2 {
                projection_version: Default::default(),
                run_id: COMPONENT_RUNNER_IDENTITY.to_owned(),
                durability_role: RunDurabilityRoleV2::CleanRun,
                platform_rule_snapshot_file_sha256: snapshot.file_sha256().clone(),
                trade_finality_policy_file_sha256: finality.policy_id().clone(),
                platform_scenario_file_sha256: scenario.file_sha256().clone(),
                proxy_latency_profile_file_sha256: latency.file_sha256().clone(),
                implementation_identity: identity,
                market_event_source_identity_sha256: Some(Sha256Hex(golden_sha256.to_owned())),
            },
            Arc::clone(&snapshot),
            Arc::clone(&finality),
            Arc::clone(&scenario),
            Arc::clone(&latency),
            Box::new(CapturingProjectionPort {
                state: Arc::clone(&capture),
            }),
            MAX_GROUP_BYTES,
        )
        .map_err(|error| format!("创建组件单根失败：{error}"))?;
        execution
            .create_session(SessionSpecV3 {
                run_id: COMPONENT_RUNNER_IDENTITY.to_owned(),
                session_id: "component-session".to_owned(),
                account_id: "component-account".to_owned(),
                caller_role: SimCallerRoleV1::OfflineEvaluation,
                caller_instance_id: "component-caller".to_owned(),
                creation_request_id: "component-create".to_owned(),
                response_bridge_binding_id: "component-bridge".to_owned(),
                initial_account_state: InitialAccountStateV2 {
                    available_pusd: ExactDecimal("2000".to_owned()),
                },
                session_seed: Sha256Hex("8".repeat(64)),
            })
            .map_err(|error| format!("创建组件会话失败：{error}"))?;
        execution
            .activate_session("component-activate".to_owned())
            .map_err(|error| format!("激活组件会话失败：{error}"))?;
        let mut ordered = buckets.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|(reference, _)| {
            (
                reference.bucket_at_utc_ms,
                match reference.path {
                    MarketEventPathV1::HiddenPlatform => 0_u8,
                    MarketEventPathV1::PublicVisibility => 1_u8,
                },
            )
        });
        let mut next_bucket = 0;
        while next_bucket < ordered.len()
            && ordered[next_bucket].0.bucket_at_utc_ms
                <= MARKET_TWO_START + 10 + PUBLIC_DELAY_MS + COMPONENT_TIME_SHIFT_MS
        {
            drive_real_bucket(&mut execution, ordered[next_bucket])?;
            next_bucket += 1;
        }
        if perturb {
            std::thread::yield_now();
        }
        let cancelled = AtomicBool::new(false);
        let mut resources = measurement_resources(&execution)?;
        let mut decision = COMPONENT_REQUEST_CUTOFF_BASE;
        drive_fixed_request_range(
            &mut execution,
            &scenario,
            0,
            false,
            0..2,
            None,
            &ordered,
            &mut next_bucket,
            &cancelled,
            &mut resources,
            &mut decision,
            COMPONENT_TIME_SHIFT_MS,
        )?;
        if perturb {
            std::thread::yield_now();
        }
        while next_bucket < ordered.len() {
            drive_required_before_next_bucket(&mut execution, &ordered, &mut next_bucket)?;
            drive_real_bucket(&mut execution, ordered[next_bucket])?;
            next_bucket += 1;
            if perturb {
                std::thread::yield_now();
            }
        }
        let candidate = execution
            .complete_clean_run(bucket_reader(buckets))
            .map_err(|error| format!("组件 CleanRun 封口/冷恢复失败：{error}"))?;
        let projection_batches = capture
            .lock()
            .map_err(|_| "组件投影状态锁损坏".to_owned())?
            .semantic_batches
            .clone();
        write_json(
            root_dir.join("component-projection.json"),
            &json!({
                "schema_version":"component_projection_evidence_v1",
                "attempt":attempt,
                "validation_evidence_only":attempt != "canonical",
                "batches":projection_batches
            }),
        )?;
        Ok(ComponentAttemptEvidence {
            accepted_input_count: candidate.accepted_input_count(),
            projection_batches,
            log_sha256: sha256_file(&log_path)?,
            log_bytes: fs::metadata(&log_path)
                .map_err(|error| format!("读取组件日志大小失败：{error}"))?
                .len(),
            cold_candidate_run_id: candidate.run_id().to_owned(),
        })
    }

    #[cfg(feature = "measurement")]
    pub(super) fn write_and_read_component_events(
        path: &Path,
        buckets: &[(
            DurableMarketBucketReferenceV2,
            Arc<[ExecutionMarketEventV3]>,
        )],
    ) -> Result<
        Vec<(
            DurableMarketBucketReferenceV2,
            Arc<[ExecutionMarketEventV3]>,
        )>,
        String,
    > {
        let mut writer = BufWriter::new(
            File::create(path).map_err(|error| format!("创建组件黄金事件文件失败：{error}"))?,
        );
        for (_, events) in buckets {
            for event in events.iter() {
                let encoded = encode_execution_market_event_v3(event)
                    .map_err(|error| format!("编码组件黄金事件失败：{error}"))?;
                writer
                    .write_all(&encoded)
                    .and_then(|()| writer.write_all(b"\n"))
                    .map_err(|error| format!("写组件黄金事件失败：{error}"))?;
            }
        }
        writer
            .flush()
            .map_err(|error| format!("刷新组件黄金事件失败：{error}"))?;
        let bytes = fs::read(path).map_err(|error| format!("回读组件黄金事件失败：{error}"))?;
        let decoded = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| {
                decode_execution_market_event_v3(line)
                    .map_err(|error| format!("窄入口解码组件黄金事件失败：{error}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut cursor = 0;
        let restored = buckets
            .iter()
            .map(|(reference, events)| {
                let end = cursor + events.len();
                let restored = decoded
                    .get(cursor..end)
                    .ok_or_else(|| "组件黄金事件文件条目数不足".to_owned())?
                    .to_vec();
                cursor = end;
                Ok((reference.clone(), Arc::from(restored.into_boxed_slice())))
            })
            .collect::<Result<Vec<_>, String>>()?;
        if cursor != decoded.len() {
            return Err("组件黄金事件文件含未消费的额外条目".to_owned());
        }
        Ok(restored)
    }

    #[cfg(feature = "measurement")]
    pub(super) fn component_comparison(
        canonical: &ComponentAttemptEvidence,
        rerun: &ComponentAttemptEvidence,
        perturbed: &ComponentAttemptEvidence,
    ) -> Value {
        for (run, candidate) in [("clean-rerun", rerun), ("schedule-perturbed", perturbed)] {
            let divergence = if canonical.accepted_input_count != candidate.accepted_input_count {
                Some((
                    "accepted_inputs",
                    "$/accepted_input_count".to_owned(),
                    None,
                    json!(canonical.accepted_input_count),
                    json!(candidate.accepted_input_count),
                ))
            } else if canonical.projection_batches != candidate.projection_batches {
                let input_sequence = first_changed_batch(
                    &canonical.projection_batches,
                    &candidate.projection_batches,
                );
                let path = first_json_divergence(
                    &Value::Array(canonical.projection_batches.clone()),
                    &Value::Array(candidate.projection_batches.clone()),
                    "$/projection_batches".to_owned(),
                )
                .unwrap_or_else(|| "$/projection_batches".to_owned());
                Some((
                    "semantic_projection",
                    path,
                    input_sequence,
                    narrow_projection_summary(canonical, input_sequence),
                    narrow_projection_summary(candidate, input_sequence),
                ))
            } else if canonical.cold_candidate_run_id != candidate.cold_candidate_run_id {
                Some((
                    "cold_recovery",
                    "$/cold_candidate_run_id".to_owned(),
                    None,
                    json!(canonical.cold_candidate_run_id),
                    json!(candidate.cold_candidate_run_id),
                ))
            } else if canonical.log_sha256 != candidate.log_sha256
                || canonical.log_bytes != candidate.log_bytes
            {
                Some((
                    "canonical_log",
                    if canonical.log_sha256 != candidate.log_sha256 {
                        "$/log_sha256".to_owned()
                    } else {
                        "$/log_bytes".to_owned()
                    },
                    None,
                    json!({"sha256":canonical.log_sha256,"bytes":canonical.log_bytes}),
                    json!({"sha256":candidate.log_sha256,"bytes":candidate.log_bytes}),
                ))
            } else {
                None
            };
            if let Some((group, path, input_sequence, left, right)) = divergence {
                return json!({
                    "schema_version":"component_comparison_v1",
                    "status":"FAIL",
                    "first_divergence":{
                        "run":run,
                        "path":path,
                        "input_sequence":input_sequence,
                        "group":group,
                        "item":"first unequal narrow evidence",
                        "left":left,
                        "right":right
                    }
                });
            }
        }
        json!({
            "schema_version":"component_comparison_v1",
            "status":"PASS",
            "first_divergence":Value::Null,
            "canonical_equals_clean_rerun":true,
            "canonical_equals_schedule_perturbed":true,
            "validation_attempts_are_not_formal_runs":true
        })
    }

    #[cfg(feature = "measurement")]
    fn first_changed_batch(left: &[Value], right: &[Value]) -> Option<usize> {
        left.iter()
            .zip(right)
            .position(|(left, right)| left != right)
            .or_else(|| (left.len() != right.len()).then_some(left.len().min(right.len())))
    }

    #[cfg(feature = "measurement")]
    fn first_json_divergence(left: &Value, right: &Value, path: String) -> Option<String> {
        match (left, right) {
            (Value::Array(left), Value::Array(right)) => {
                for index in 0..left.len().max(right.len()) {
                    let next = format!("{path}/{index}");
                    match (left.get(index), right.get(index)) {
                        (Some(left), Some(right)) => {
                            if let Some(found) = first_json_divergence(left, right, next) {
                                return Some(found);
                            }
                        }
                        _ => return Some(next),
                    }
                }
                None
            }
            (Value::Object(left), Value::Object(right)) => {
                let keys = left
                    .keys()
                    .chain(right.keys())
                    .collect::<std::collections::BTreeSet<_>>();
                for key in keys {
                    let next = format!("{path}/{key}");
                    match (left.get(key), right.get(key)) {
                        (Some(left), Some(right)) => {
                            if let Some(found) = first_json_divergence(left, right, next) {
                                return Some(found);
                            }
                        }
                        _ => return Some(next),
                    }
                }
                None
            }
            _ => (left != right).then_some(path),
        }
    }

    #[cfg(feature = "measurement")]
    fn narrow_projection_summary(
        evidence: &ComponentAttemptEvidence,
        input_sequence: Option<usize>,
    ) -> Value {
        input_sequence
            .and_then(|index| evidence.projection_batches.get(index))
            .cloned()
            .unwrap_or(Value::Null)
    }

    #[cfg(feature = "measurement")]
    fn copy_verified(source: &Path, expected_sha256: &str, target: &Path) -> Result<(), String> {
        let bytes = fs::read(source).map_err(|error| format!("读取待复制证据文件失败：{error}"))?;
        let actual = format!("{:x}", Sha256::digest(&bytes));
        if actual != expected_sha256 {
            return Err(format!(
                "证据文件 SHA-256 不匹配：{} expected={expected_sha256} actual={actual}",
                source.display()
            ));
        }
        fs::write(target, bytes).map_err(|error| format!("写入已核验证据文件失败：{error}"))
    }

    #[cfg(feature = "measurement")]
    fn component_artifact_inventory(root: &Path) -> Result<Vec<Value>, String> {
        let mut paths = Vec::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in
                fs::read_dir(&directory).map_err(|error| format!("遍历组件产物失败：{error}"))?
            {
                let entry = entry.map_err(|error| format!("读取组件产物项失败：{error}"))?;
                if entry
                    .file_type()
                    .map_err(|error| format!("读取组件产物类型失败：{error}"))?
                    .is_dir()
                {
                    pending.push(entry.path());
                } else if entry.file_name()
                    != std::ffi::OsStr::new("simulation-run-manifest-v2.json")
                {
                    paths.push(entry.path());
                }
            }
        }
        paths.sort();
        paths
            .into_iter()
            .map(|path| {
                let relative_path = path
                    .strip_prefix(root)
                    .map_err(|error| format!("计算组件相对路径失败：{error}"))?;
                let bytes = fs::read(&path)
                    .map_err(|error| format!("读取组件产物字节快照失败：{error}"))?;
                Ok(component_inventory_member(relative_path, &bytes))
            })
            .collect()
    }

    #[cfg(feature = "measurement")]
    pub(super) fn component_inventory_member(relative_path: &Path, bytes: &[u8]) -> Value {
        json!({
            "path":relative_path,
            "bytes":bytes.len(),
            "sha256":format!("{:x}", Sha256::digest(bytes))
        })
    }

    #[cfg(feature = "measurement")]
    pub(super) fn sync_tree(root: &Path) -> Result<(), String> {
        let mut directories = vec![root.to_path_buf()];
        let mut index = 0;
        while index < directories.len() {
            let directory = directories[index].clone();
            for entry in fs::read_dir(&directory)
                .map_err(|error| format!("遍历待同步组件目录失败：{error}"))?
            {
                let entry = entry.map_err(|error| format!("读取待同步组件项失败：{error}"))?;
                if entry
                    .file_type()
                    .map_err(|error| format!("读取待同步组件项类型失败：{error}"))?
                    .is_dir()
                {
                    directories.push(entry.path());
                } else {
                    File::open(entry.path())
                        .and_then(|file| file.sync_all())
                        .map_err(|error| format!("同步组件文件失败：{error}"))?;
                }
            }
            index += 1;
        }
        for directory in directories.into_iter().rev() {
            File::open(directory)
                .and_then(|file| file.sync_all())
                .map_err(|error| format!("同步组件目录失败：{error}"))?;
        }
        Ok(())
    }

    #[cfg(feature = "measurement")]
    pub(super) fn publish_candidate(candidate: &Path, target: &Path) -> Result<(), String> {
        if target.exists() {
            return Err(format!("组件发布目标已存在：{}", target.display()));
        }
        let candidate_parent = candidate.parent().unwrap_or(Path::new("."));
        if target.parent().unwrap_or(Path::new(".")) != candidate_parent {
            return Err("组件 candidate 与发布目标必须同级".to_owned());
        }
        let parent = File::open(candidate_parent)
            .map_err(|error| format!("打开组件发布父目录失败：{error}"))?;
        let candidate_name = candidate
            .file_name()
            .ok_or_else(|| "组件 candidate 缺少文件名".to_owned())?;
        let target_name = target
            .file_name()
            .ok_or_else(|| "组件目标缺少文件名".to_owned())?;
        renameat_with(
            &parent,
            candidate_name,
            &parent,
            target_name,
            RenameFlags::NOREPLACE,
        )
        .map_err(|error| format!("NOREPLACE 原子发布组件 bundle 失败：{error}"))?;
        parent
            .sync_all()
            .map_err(|error| format!("同步组件发布父目录失败：{error}"))
    }

    #[cfg(feature = "measurement")]
    pub(super) fn failed_output_path(output: &Path) -> PathBuf {
        let mut path = output.as_os_str().to_os_string();
        path.push(".failed");
        PathBuf::from(path)
    }

    fn run_golden_matrix(args: Args, run_started: Instant) -> Result<(), String> {
        fs::create_dir_all(&args.output_dir)
            .map_err(|error| format!("创建输出目录失败：{error}"))?;
        check_safety(run_started, run_started, &args.output_dir)?;
        let start_unix_ms = unix_ms()?;
        let start_utc = utc_now()?;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("创建 Tokio 运行时失败：{error}"))?;
        let snapshot = runtime
            .block_on(query_and_seal_current_process_snapshot_v3(
                CurrentProcessSnapshotRequestV1 {
                    reference_market: reference_market(),
                    target_path: args.output_dir.join("platform-rule-snapshot-v3.json"),
                    implementation_identity: args.identity.clone(),
                },
            ))
            .map_err(|error| format!("查询并封存进程规则快照失败：{error}"))?;
        let scenario = fixed_scenario()?;
        let finality = successful_finality()?;
        let latency = load_embedded_proxy_latency_profile()
            .map_err(|error| format!("加载嵌入时延档失败：{error}"))?;
        let identities = identities(&snapshot, &scenario, &finality, &latency);

        let samples_path = args.output_dir.join("samples.jsonl");
        if samples_path.exists() {
            fs::remove_file(&samples_path)
                .map_err(|error| format!("清理旧 samples.jsonl 失败：{error}"))?;
        }
        let mut samples = Vec::new();
        for durability in [Durability::CleanRun, Durability::RecoverablePaper] {
            for shape in [Shape::SingleRoot, Shape::FourRoots] {
                for trial in 0..(WARMUP_TRIALS + TRIALS) {
                    let trial_started = Instant::now();
                    let trial_proc_before = proc_snapshot()?;
                    let cancelled = Arc::new(AtomicBool::new(false));
                    let mut handles = Vec::with_capacity(shape.roots());
                    for root in 0..shape.roots() {
                        let root_dir = args.output_dir.join(format!(
                            "root-{}-{}-{trial}-{root}",
                            durability.label(),
                            shape.label()
                        ));
                        let snapshot = Arc::clone(&snapshot);
                        let scenario = Arc::clone(&scenario);
                        let finality = Arc::clone(&finality);
                        let latency = Arc::clone(&latency);
                        let identity = args.identity.clone();
                        let cancelled = Arc::clone(&cancelled);
                        handles.push(std::thread::spawn(move || {
                            run_root(
                                root_dir, durability, root, trial, snapshot, scenario, finality,
                                latency, identity, cancelled,
                            )
                        }));
                    }
                    let mut trial_rss_peak_bytes = trial_proc_before.rss_bytes;
                    let mut safety_error = None;
                    while handles.iter().any(|handle| !handle.is_finished()) {
                        let proc = proc_snapshot()?;
                        trial_rss_peak_bytes = trial_rss_peak_bytes.max(proc.rss_bytes);
                        if let Err(error) =
                            check_safety(run_started, trial_started, &args.output_dir)
                        {
                            cancelled.store(true, Ordering::Relaxed);
                            safety_error = Some(error);
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    let mut results = Vec::with_capacity(shape.roots());
                    for handle in handles {
                        results.push(
                            handle
                                .join()
                                .map_err(|_| "测量根线程异常退出".to_owned())??,
                        );
                    }
                    let trial_elapsed_ns = elapsed_ns(trial_started);
                    let trial_proc_after = proc_snapshot()?;
                    trial_rss_peak_bytes = trial_rss_peak_bytes.max(trial_proc_after.rss_bytes);
                    if let Some(error) = safety_error {
                        return Err(error);
                    }
                    check_safety(run_started, trial_started, &args.output_dir)?;
                    for (root, result) in results.into_iter().enumerate() {
                        let sample = Sample {
                            schema_version: "sim_measurement_sample_v2",
                            runner_identity: RUNNER_IDENTITY,
                            build: BUILD,
                            durability,
                            shape,
                            trial,
                            root,
                            run: result.run_id,
                            workload: WORKLOAD_IDENTITY,
                            operation: "golden_workload_e2e",
                            phase: "public_end_to_end",
                            warmup: trial < WARMUP_TRIALS,
                            requests: REQUESTS_PER_ROOT,
                            e2e_elapsed_ns: result.elapsed_ns,
                            trial_elapsed_ns,
                            trial_requests: REQUESTS_PER_ROOT * shape.roots(),
                            trial_rss_peak_bytes,
                            stage_elapsed_ns: stage_elapsed(&result.observations),
                            measurement_observations: result.observations,
                            measurement_resources: result.resources,
                            resources: result.root_resources,
                            proc_before: trial_proc_before,
                            proc_after: trial_proc_after,
                        };
                        append_json_line(&samples_path, &sample)?;
                        samples.push(sample);
                    }
                }
            }
        }

        let trials = summarize_trials(&samples);
        let cells = summarize(&samples);
        let baseline_comparison = compare_baseline(&args, &identities, &trials)?;
        let summary = Summary {
            schema_version: "sim_measurement_summary_v2",
            runner_identity: RUNNER_IDENTITY,
            build: BUILD,
            identities: &identities,
            measured_trials_per_cell: TRIALS,
            warmup_trials_per_cell: WARMUP_TRIALS,
            trials: &trials,
            cells: &cells,
            baseline_comparison,
            candidate_status: "exploratory_candidate_only_not_a_formal_gate",
        };
        write_json(args.output_dir.join("summary.json"), &summary)?;
        write_candidate_table(&args.output_dir, &cells)?;

        let end_unix_ms = unix_ms()?;
        let end_utc = utc_now()?;
        write_environment(
            &args,
            &identities,
            start_unix_ms,
            &start_utc,
            end_unix_ms,
            &end_utc,
        )?;
        Ok(())
    }

    fn run_targeted_retention(args: Args, run_started: Instant) -> Result<(), String> {
        #[cfg(not(feature = "measurement"))]
        {
            let _ = (args, run_started);
            return Err(
                "--targeted-retention-24h 只允许 measurement feature 构建；未装配探针".to_owned(),
            );
        }

        #[cfg(feature = "measurement")]
        {
            if args.output_dir.exists()
                && fs::read_dir(&args.output_dir)
                    .map_err(|error| format!("读取 targeted 输出目录失败：{error}"))?
                    .next()
                    .is_some()
            {
                return Err("targeted 输出目录必须为空，拒绝混入陈旧产物".to_owned());
            }
            fs::create_dir_all(&args.output_dir)
                .map_err(|error| format!("创建 targeted 输出目录失败：{error}"))?;
            check_safety(run_started, run_started, &args.output_dir)?;
            let started_at_unix_ms = unix_ms()?;
            let started_at_utc = utc_now()?;
            let evidence_root = std::env::temp_dir().join(format!(
                "sim-targeted-retention-{}-{started_at_unix_ms}",
                std::process::id()
            ));
            fs::create_dir(&evidence_root)
                .map_err(|error| format!("创建 targeted 临时证据根失败：{error}"))?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("创建 Tokio 运行时失败：{error}"))?;
            let snapshot = runtime
                .block_on(query_and_seal_current_process_snapshot_v3(
                    CurrentProcessSnapshotRequestV1 {
                        reference_market: reference_market(),
                        target_path: evidence_root.join("platform-rule-snapshot-v3.json"),
                        implementation_identity: args.identity.clone(),
                    },
                ))
                .map_err(|error| format!("查询并封存进程规则快照失败：{error}"))?;
            let scenario = fixed_scenario()?;
            let finality = successful_finality()?;
            let latency = load_embedded_proxy_latency_profile()
                .map_err(|error| format!("加载嵌入时延档失败：{error}"))?;
            let identities = identities(&snapshot, &scenario, &finality, &latency);
            let current_exe =
                std::env::current_exe().map_err(|error| format!("定位当前二进制失败：{error}"))?;
            let binary_sha256 = sha256_file(&current_exe)?;
            let samples_path = args.output_dir.join("retention-samples.jsonl");
            if samples_path.exists() {
                fs::remove_file(&samples_path)
                    .map_err(|error| format!("清理旧 retention-samples.jsonl 失败：{error}"))?;
            }

            let mut summary_inputs = Vec::with_capacity(WARMUP_TRIALS + TRIALS);
            for trial in 0..(WARMUP_TRIALS + TRIALS) {
                let trial_started = Instant::now();
                check_safety(run_started, trial_started, &args.output_dir)?;
                let warmup = trial < WARMUP_TRIALS;
                let root_dir = evidence_root.join(format!("retention-root-{trial}"));
                let trial_snapshot = Arc::clone(&snapshot);
                let trial_scenario = Arc::clone(&scenario);
                let trial_finality = Arc::clone(&finality);
                let trial_latency = Arc::clone(&latency);
                let trial_identity = args.identity.clone();
                let trial_identities = identities.clone();
                let trial_binary = current_exe.clone();
                let trial_binary_sha256 = binary_sha256.clone();
                let cancelled = Arc::new(AtomicBool::new(false));
                let worker_cancelled = Arc::clone(&cancelled);
                let handle = std::thread::spawn(move || {
                    run_targeted_root(
                        root_dir,
                        trial,
                        warmup,
                        trial_snapshot,
                        trial_scenario,
                        trial_finality,
                        trial_latency,
                        trial_identity,
                        &trial_identities,
                        &trial_binary,
                        &trial_binary_sha256,
                        run_started,
                        trial_started,
                        worker_cancelled,
                    )
                });
                let mut safety_error = None;
                while !handle.is_finished() {
                    if let Err(error) = check_safety(run_started, trial_started, &args.output_dir) {
                        cancelled.store(true, Ordering::Relaxed);
                        safety_error = Some(error);
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                let sample = handle
                    .join()
                    .map_err(|_| "targeted trial 线程异常退出".to_owned())??;
                if let Some(error) = safety_error {
                    return Err(error);
                }
                check_safety(run_started, trial_started, &args.output_dir)?;
                summary_inputs.push(TargetedSummaryInput::new(
                    warmup,
                    sample["retention_elapsed_ns"]
                        .as_u64()
                        .ok_or_else(|| "targeted sample 缺少 retention_elapsed_ns".to_owned())?,
                    sample["released_resources"]["business_records"]
                        .as_u64()
                        .ok_or_else(|| "targeted sample 缺少释放 records".to_owned())?,
                    sample["released_resources"]["business_bytes"]
                        .as_u64()
                        .ok_or_else(|| "targeted sample 缺少释放 bytes".to_owned())?,
                    sample["semantics_passed"].as_bool() == Some(true),
                ));
                append_json_line(&samples_path, &sample)?;
            }

            write_json(
                args.output_dir.join("retention-summary.json"),
                &summarize_targeted(&summary_inputs)?,
            )?;
            let completed_at_unix_ms = unix_ms()?;
            let completed_at_utc = utc_now()?;
            write_json(
                args.output_dir.join("retention-environment.json"),
                &json!({
                    "schema_version": "sim_targeted_retention_environment_v1",
                    "runner_identity": TARGETED_RUNNER_IDENTITY,
                    "build": BUILD,
                    "exploratory_input_only": true,
                    "argv": args.argv,
                    "started_at_unix_ms": started_at_unix_ms,
                    "started_at_utc": started_at_utc,
                    "completed_at_unix_ms": completed_at_unix_ms,
                    "completed_at_utc": completed_at_utc,
                    "uname": command_output("uname", &["-a"]),
                    "cpu_model": cpu_model()?,
                    "logical_cores": std::thread::available_parallelism()
                        .map(|count| count.get())
                        .map_err(|error| format!("读取逻辑核心数失败：{error}"))?,
                    "git_identity": args.identity,
                    "git_head": command_output("git", &["rev-parse", "HEAD"]),
                    "binary": {"path": current_exe, "sha256": binary_sha256},
                    "identities": identities,
                    "fixed_mode": {
                        "window_ms": TARGETED_RETENTION_WINDOW_MS,
                        "durability": "recoverable_paper",
                        "shape": "single_root",
                        "loops_per_root": LOOPS_PER_ROOT,
                        "methods_per_loop": 8,
                        "requests_per_root": REQUESTS_PER_ROOT,
                        "warmup_trials": WARMUP_TRIALS,
                        "measured_trials": TRIALS,
                        "targeted_first_post_order": {
                            "kind": "fok_market",
                            "spend_pusd": "3",
                            "worst_price_pusd_per_share": "0.6"
                        }
                    }
                }),
            )?;
            fs::remove_dir_all(&evidence_root)
                .map_err(|error| format!("清理 targeted 临时证据根失败：{error}"))?;
            let artifacts = artifact_inventory(&args.output_dir)?;
            write_json(
                args.output_dir.join("retention-manifest.json"),
                &targeted_manifest_value(
                    &args.argv,
                    command_output("git", &["rev-parse", "HEAD"]),
                    binary_sha256,
                    args.identity.source_identity_sha256.0,
                    args.identity.cargo_lock_sha256.0,
                    identities.rule_snapshot_sha256,
                    identities.workload_identity,
                    artifacts,
                ),
            )
        }
    }

    #[cfg(feature = "measurement")]
    #[allow(clippy::too_many_arguments)]
    fn run_targeted_root(
        root_dir: PathBuf,
        trial: usize,
        warmup: bool,
        snapshot: Arc<SealedProcessRuleSnapshotV3>,
        scenario: Arc<SealedPlatformScenarioV1>,
        finality: Arc<TradeFinalityPolicyV2>,
        latency: Arc<EmpiricalDelayProfile>,
        identity: ImplementationIdentityV1,
        identities: &IdentityBundle,
        binary_path: &Path,
        binary_sha256: &str,
        run_started: Instant,
        trial_started: Instant,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Value, String> {
        fs::create_dir_all(&root_dir).map_err(|error| format!("创建 targeted 根失败：{error}"))?;
        let log_path = root_dir.join("sim-event-log-v2.jsonl");
        if log_path.exists() {
            fs::remove_file(&log_path)
                .map_err(|error| format!("清理旧 targeted 日志失败：{error}"))?;
        }
        let run_id = format!("{TARGETED_RUNNER_IDENTITY}-trial-{trial}");
        let capture = Arc::new(Mutex::new(CapturingProjectionState::default()));
        let mut execution = SimExecution::create_new(
            &log_path,
            RunOpenedV2 {
                projection_version: Default::default(),
                run_id: run_id.clone(),
                durability_role: RunDurabilityRoleV2::RecoverablePaper,
                platform_rule_snapshot_file_sha256: snapshot.file_sha256().clone(),
                trade_finality_policy_file_sha256: finality.policy_id().clone(),
                platform_scenario_file_sha256: scenario.file_sha256().clone(),
                proxy_latency_profile_file_sha256: latency.file_sha256().clone(),
                implementation_identity: identity.clone(),
                market_event_source_identity_sha256: Some(Sha256Hex("7".repeat(64))),
            },
            Arc::clone(&snapshot),
            Arc::clone(&finality),
            Arc::clone(&scenario),
            Arc::clone(&latency),
            Box::new(CapturingProjectionPort {
                state: Arc::clone(&capture),
            }),
            MAX_GROUP_BYTES,
        )
        .map_err(|error| format!("创建 targeted Paper 根失败：{error}"))?;
        let spec = SessionSpecV3 {
            run_id: run_id.clone(),
            session_id: format!("retention-session-{trial}"),
            account_id: "shared-account".to_owned(),
            caller_role: SimCallerRoleV1::Paper,
            caller_instance_id: "targeted-retention-runner".to_owned(),
            creation_request_id: format!("retention-create-{trial}"),
            response_bridge_binding_id: "targeted-retention-bridge".to_owned(),
            initial_account_state: InitialAccountStateV2 {
                available_pusd: ExactDecimal("2000".to_owned()),
            },
            session_seed: Sha256Hex("8".repeat(64)),
        };
        execution
            .create_session(spec.clone())
            .map_err(|error| format!("创建 targeted 会话失败：{error}"))?;
        execution
            .create_session(spec)
            .map_err(|error| format!("重试 targeted 会话失败：{error}"))?;
        execution
            .activate_session(format!("retention-activate-{trial}"))
            .map_err(|error| format!("激活 targeted 会话失败：{error}"))?;

        let buckets = golden_buckets(&scenario)?;
        let public_buckets = buckets
            .iter()
            .filter(|(reference, _)| reference.path == MarketEventPathV1::PublicVisibility)
            .collect::<Vec<_>>();
        let mut next_bucket = 0;
        while next_bucket < 2 {
            drive_real_bucket(&mut execution, public_buckets[next_bucket])?;
            next_bucket += 1;
        }
        let mut resource_high_water = measurement_resources(&execution)?;
        let mut next_decision_at_utc_ms = REQUEST_CUTOFF_BASE;
        drive_fixed_request_range(
            &mut execution,
            &scenario,
            0,
            true,
            0..253,
            None,
            &public_buckets,
            &mut next_bucket,
            &cancelled,
            &mut resource_high_water,
            &mut next_decision_at_utc_ms,
            0,
        )?;
        check_safety(run_started, trial_started, &root_dir)?;

        let first_post: contracts::PostOrderDirectResponseV2 =
            captured_response(&capture, "r0-i0-post-order", "post_order")?;
        let SubmissionItemOutcomeV2::Accepted(accepted) = &first_post.item.outcome else {
            return Err("targeted 首个 FOK 未形成 accepted terminal order".to_owned());
        };
        if accepted.fills.len() != 1 || accepted.platform_order_id != "sim-1" {
            return Err("targeted 首个 FOK 必须恰好形成 sim-1 和一笔成交".to_owned());
        }
        let trade_id = accepted.fills[0].trade_id.clone();
        let old_orders: contracts::OrdersDirectResponseV2 =
            captured_response(&capture, "r0-i0-orders", "orders")?;
        let old_trades: contracts::TradesDirectResponseV2 =
            captured_response(&capture, "r0-i0-trades", "trades")?;
        let (
            OrdersQueryResultV2::Page(old_orders_page),
            TradesQueryResultV2::Page(old_trades_page),
        ) = (old_orders.result, old_trades.result)
        else {
            return Err("targeted iteration 0 的 orders/trades 未物化 Page 根".to_owned());
        };
        if old_orders_page.root_sequence != old_trades_page.root_sequence {
            return Err("targeted iteration 0 的 orders/trades 未物化同一旧根".to_owned());
        }
        let old_root_sequence = old_orders_page.root_sequence;
        let current_before_retention: contracts::OrdersDirectResponseV2 =
            captured_response(&capture, "r0-i252-orders", "orders")?;
        let OrdersQueryResultV2::Page(current_before_retention_page) =
            current_before_retention.result
        else {
            return Err("targeted retention 前当前 orders 未返回 Page".to_owned());
        };
        let retained_from_utc_ms = TARGETED_FINAL_CUTOFF_UTC_MS - TARGETED_RETENTION_WINDOW_MS;
        let earliest_retained_root_sequence = old_root_sequence
            .checked_add(1)
            .ok_or_else(|| "旧 query root sequence 溢出".to_owned())?;
        if current_before_retention_page.root_sequence < earliest_retained_root_sequence {
            return Err("公开 orders Page 不能证明当前 revision 已越过旧 root".to_owned());
        }
        let retention_input = RetentionAdvanceV2 {
            earliest_retained_root_sequence,
            order_history_retained_from_utc_ms: Some(retained_from_utc_ms),
            trade_history_retained_from_utc_ms: Some(retained_from_utc_ms),
        };
        let retention_before = measurement_resources(&execution)?;
        let log_bytes_before = fs::metadata(&log_path)
            .map_err(|error| format!("读取 retention 前日志大小失败：{error}"))?
            .len();
        let retention_started = Instant::now();
        execution
            .advance_retention(retention_input)
            .map_err(|error| format!("推进 targeted retention 失败：{error}"))?;
        let retention_elapsed_ns = elapsed_ns(retention_started);
        let retention_after = measurement_resources(&execution)?;
        let log_bytes_after = fs::metadata(&log_path)
            .map_err(|error| format!("读取 retention 后日志大小失败：{error}"))?
            .len();
        let released_business_records = numeric_at(&retention_before, &["business", "records"])
            .saturating_sub(numeric_at(&retention_after, &["business", "records"]));
        let released_business_bytes = numeric_at(&retention_before, &["business", "bytes"])
            .saturating_sub(numeric_at(&retention_after, &["business", "bytes"]));
        if released_business_records == 0 || released_business_bytes == 0 {
            return Err("targeted retention 未释放旧 query/history 业务资源".to_owned());
        }
        let log_sequence_before = numeric_at(&retention_before, &["next_input_sequence"]);
        let log_sequence_after = numeric_at(&retention_after, &["next_input_sequence"]);
        if log_sequence_after != log_sequence_before.saturating_add(1) {
            return Err("targeted retention 必须只新增一项 RetentionAdvance 耐久输入".to_owned());
        }
        let query_plan = TargetedQueryPlan {
            old_root_sequence,
            retained_from_utc_ms,
            trade_id: trade_id.clone(),
        };
        drive_fixed_request_range(
            &mut execution,
            &scenario,
            0,
            true,
            253..LOOPS_PER_ROOT,
            Some(&query_plan),
            &public_buckets,
            &mut next_bucket,
            &cancelled,
            &mut resource_high_water,
            &mut next_decision_at_utc_ms,
            0,
        )?;
        while next_bucket < public_buckets.len() {
            drive_required_before_next_bucket(&mut execution, &public_buckets, &mut next_bucket)?;
            drive_real_bucket(&mut execution, public_buckets[next_bucket])?;
            next_bucket += 1;
        }
        let live_semantics = read_retention_semantics(
            &capture,
            old_root_sequence,
            earliest_retained_root_sequence,
            retained_from_utc_ms,
        )?;
        let resources_before_drop = measurement_resources(&execution)?;
        drop(execution);
        let recovered_capture = Arc::new(Mutex::new(CapturingProjectionState::default()));
        let recovered = SimExecution::recover_paper(
            &log_path,
            snapshot,
            finality,
            scenario,
            Arc::clone(&latency),
            Box::new(CapturingProjectionPort {
                state: Arc::clone(&recovered_capture),
            }),
            MAX_GROUP_BYTES,
            bucket_reader(&buckets),
        )
        .map_err(|error| format!("targeted Paper 冷恢复失败：{error}"))?;
        let recovered_resources = measurement_resources(&recovered)?;
        if recovered_resources != resources_before_drop {
            return Err("targeted 冷恢复后的资源/日志序号未复现 retention 后状态".to_owned());
        }
        let recovery_semantics = read_retention_semantics(
            &recovered_capture,
            old_root_sequence,
            earliest_retained_root_sequence,
            retained_from_utc_ms,
        )?;
        if recovery_semantics != live_semantics {
            return Err("targeted 冷恢复语义未与 live 结果精确一致".to_owned());
        }
        let recovered_final_resources = recovered_resources.clone();
        drop(recovered);
        check_safety(run_started, trial_started, &root_dir)?;
        let log_bytes_final = fs::metadata(&log_path)
            .map_err(|error| format!("读取 targeted 最终日志大小失败：{error}"))?
            .len();
        let log_sha256 = sha256_file(&log_path)?;

        Ok(json!({
            "schema_version": "sim_targeted_retention_sample_v1",
            "runner_identity": TARGETED_RUNNER_IDENTITY,
            "workload_identity": WORKLOAD_IDENTITY,
            "build": BUILD,
            "durability": "recoverable_paper",
            "shape": "single_root",
            "trial": trial,
            "warmup": warmup,
            "requests_in_fixed_workload": REQUESTS_PER_ROOT,
            "window_ms": TARGETED_RETENTION_WINDOW_MS,
            "final_logical_cutoff_utc_ms": TARGETED_FINAL_CUTOFF_UTC_MS,
            "retention_input": retention_input,
            "retention_elapsed_ns": retention_elapsed_ns,
            "resources_before": retention_before,
            "resources_after": retention_after,
            "released_resources": {
                "business_records": released_business_records,
                "business_bytes": released_business_bytes
            },
            "log": {
                "path": log_path,
                "bytes_before_retention": log_bytes_before,
                "bytes_after_retention": log_bytes_after,
                "bytes_added_by_retention": log_bytes_after.saturating_sub(log_bytes_before),
                "next_input_sequence_before_retention": log_sequence_before,
                "next_input_sequence_after_retention": log_sequence_after,
                "retention_input_sequence_delta": log_sequence_after.saturating_sub(log_sequence_before),
                "bytes_final": log_bytes_final,
                "sha256": log_sha256
            },
            "old_public_root": {
                "orders_root_sequence": old_root_sequence,
                "trades_root_sequence": old_root_sequence,
                "earliest_retained_root_sequence": earliest_retained_root_sequence
            },
            "accepted_terminal_order": {
                "platform_order_id": accepted.platform_order_id,
                "trade_id": trade_id,
                "terminal_reason": accepted.terminal_reason
            },
            "live_semantics": live_semantics,
            "recovery": {
                "resource_state_reproduced_before_new_queries": true,
                "resources_before_new_queries": recovered_resources,
                "resources_after_semantic_queries": recovered_final_resources,
                "semantics": recovery_semantics
            },
            "semantics_passed": true,
            "identity": {
                "implementation": identity,
                "rule_snapshot_sha256": identities.rule_snapshot_sha256,
                "scenario_sha256": identities.scenario_sha256,
                "finality_policy_sha256": identities.finality_policy_sha256,
                "latency_profile_sha256": identities.latency_profile_sha256,
                "market_event_source_identity_sha256": "7".repeat(64),
                "binary_path": binary_path,
                "binary_sha256": binary_sha256
            }
        }))
    }

    #[cfg(feature = "measurement")]
    fn captured_response<T: serde::de::DeserializeOwned>(
        state: &Arc<Mutex<CapturingProjectionState>>,
        request_id: &str,
        expected_method: &str,
    ) -> Result<T, String> {
        let response = state
            .lock()
            .map_err(|_| "targeted 投影状态锁损坏".to_owned())?
            .responses
            .get(request_id)
            .cloned()
            .ok_or_else(|| format!("targeted 缺少公开方法响应：{request_id}"))?;
        if response.method != expected_method {
            return Err(format!(
                "targeted 方法响应类型不一致：{request_id}: {}",
                response.method
            ));
        }
        serde_json::from_value(response.body)
            .map_err(|error| format!("解码 targeted 公开响应失败：{request_id}: {error}"))
    }

    #[cfg(feature = "measurement")]
    fn read_retention_semantics(
        capture: &Arc<Mutex<CapturingProjectionState>>,
        old_root_sequence: u64,
        earliest_retained_root_sequence: u64,
        retained_from_utc_ms: i64,
    ) -> Result<Value, String> {
        let old_orders: contracts::OrdersDirectResponseV2 =
            captured_response(capture, "r0-i253-orders", "orders")?;
        if old_orders.result
            != OrdersQueryResultV2::OutOfRetention(QueryOutOfRetentionV2::RootSnapshot {
                requested_root_sequence: old_root_sequence,
                earliest_retained_root_sequence,
            })
        {
            return Err("旧 orders root 未返回 OutOfRetention".to_owned());
        }
        let old_trades: contracts::TradesDirectResponseV2 =
            captured_response(capture, "r0-i253-trades", "trades")?;
        if old_trades.result
            != TradesQueryResultV2::OutOfRetention(QueryOutOfRetentionV2::RootSnapshot {
                requested_root_sequence: old_root_sequence,
                earliest_retained_root_sequence,
            })
        {
            return Err("旧 trades root 未返回 OutOfRetention".to_owned());
        }
        let order: contracts::OrderDirectResponseV2 =
            captured_response(capture, "r0-i253-order", "order")?;
        if order.result
            != OrderQueryResultV2::OutOfRetention(QueryOutOfRetentionV2::OrderHistory {
                retained_from_utc_ms,
            })
        {
            return Err("旧终态 order history 未返回 OutOfRetention".to_owned());
        }
        let trade: contracts::TradesDirectResponseV2 =
            captured_response(capture, "r0-i254-trades", "trades")?;
        if trade.result
            != TradesQueryResultV2::OutOfRetention(QueryOutOfRetentionV2::TradeHistory {
                retained_from_utc_ms,
            })
        {
            return Err("旧终态 trade history 未返回 OutOfRetention".to_owned());
        }
        let old_orders_result = serde_json::to_value(&old_orders.result)
            .map_err(|error| format!("编码旧 orders 结果失败：{error}"))?;
        let old_trades_result = serde_json::to_value(&old_trades.result)
            .map_err(|error| format!("编码旧 trades 结果失败：{error}"))?;
        let terminal_order_result = serde_json::to_value(&order.result)
            .map_err(|error| format!("编码终态 order 结果失败：{error}"))?;
        let terminal_trade_result = serde_json::to_value(&trade.result)
            .map_err(|error| format!("编码终态 trade 结果失败：{error}"))?;
        let current_orders: contracts::OrdersDirectResponseV2 =
            captured_response(capture, "r0-i254-orders", "orders")?;
        let current_orders_result = serde_json::to_value(&current_orders.result)
            .map_err(|error| format!("编码当前 orders 结果失败：{error}"))?;
        let OrdersQueryResultV2::Page(current_orders_page) = current_orders.result else {
            return Err("当前 orders 范围未返回 Page".to_owned());
        };
        if current_orders_page.root_sequence < earliest_retained_root_sequence {
            return Err("当前 orders root 低于保留水位".to_owned());
        }
        let current_trades: contracts::TradesDirectResponseV2 =
            captured_response(capture, "r0-i255-trades", "trades")?;
        let current_trades_result = serde_json::to_value(&current_trades.result)
            .map_err(|error| format!("编码当前 trades 结果失败：{error}"))?;
        let TradesQueryResultV2::Page(current_trades_page) = current_trades.result else {
            return Err("当前 trades 范围未返回 Page".to_owned());
        };
        if current_trades_page.root_sequence < earliest_retained_root_sequence {
            return Err("当前 trades root 低于保留水位".to_owned());
        }
        Ok(json!({
            "old_orders_root_out_of_retention": true,
            "old_trades_root_out_of_retention": true,
            "terminal_order_history_out_of_retention": true,
            "terminal_trade_history_out_of_retention": true,
            "current_orders_query_succeeded": true,
            "current_trades_query_succeeded": true,
            "current_orders_root_sequence": current_orders_page.root_sequence,
            "current_trades_root_sequence": current_trades_page.root_sequence,
            "earliest_retained_root_sequence": earliest_retained_root_sequence,
            "order_history_retained_from_utc_ms": retained_from_utc_ms,
            "trade_history_retained_from_utc_ms": retained_from_utc_ms,
            "business_results": {
                "old_orders": old_orders_result,
                "old_trades": old_trades_result,
                "terminal_order": terminal_order_result,
                "terminal_trade": terminal_trade_result,
                "current_orders": current_orders_result,
                "current_trades": current_trades_result
            }
        }))
    }

    #[cfg(feature = "measurement")]
    fn numeric_at(value: &Value, path: &[&str]) -> u64 {
        path.iter()
            .try_fold(value, |current, key| current.get(*key))
            .and_then(Value::as_u64)
            .unwrap_or_default()
    }

    #[cfg(feature = "measurement")]
    pub(super) fn targeted_manifest_value(
        argv: &[String],
        git_head: String,
        binary_sha256: String,
        source_identity_sha256: String,
        cargo_lock_sha256: String,
        rule_snapshot_sha256: String,
        workload_identity: String,
        artifacts: Vec<Value>,
    ) -> Value {
        json!({
            "schema_version": "sim_targeted_retention_manifest_v1",
            "runner_identity": TARGETED_RUNNER_IDENTITY,
            "exploratory_input_only": true,
            "command": argv,
            "exit_code": 0,
            "git_head": git_head,
            "binary_sha256": binary_sha256,
            "source_identity_sha256": source_identity_sha256,
            "cargo_lock_sha256": cargo_lock_sha256,
            "rule_snapshot_sha256": rule_snapshot_sha256,
            "workload_identity": workload_identity,
            "no_failures": true,
            "artifacts": artifacts,
            "inventory_excludes_manifest_itself": true
        })
    }

    #[cfg(feature = "measurement")]
    pub(super) fn artifact_inventory(output_dir: &Path) -> Result<Vec<Value>, String> {
        let mut paths = Vec::new();
        let mut pending = vec![output_dir.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(&directory)
                .map_err(|error| format!("遍历 targeted 产物失败：{error}"))?
            {
                let entry = entry.map_err(|error| format!("读取 targeted 产物项失败：{error}"))?;
                let metadata = entry
                    .metadata()
                    .map_err(|error| format!("读取 targeted 产物 metadata 失败：{error}"))?;
                if metadata.is_dir() {
                    pending.push(entry.path());
                } else if metadata.is_file()
                    && entry.file_name() != std::ffi::OsStr::new("retention-manifest.json")
                {
                    paths.push(entry.path());
                }
            }
        }
        paths.sort();
        paths
            .into_iter()
            .map(|path| {
                let bytes = fs::metadata(&path)
                    .map_err(|error| format!("读取产物大小失败：{error}"))?
                    .len();
                let relative = path
                    .strip_prefix(output_dir)
                    .map_err(|error| format!("计算产物相对路径失败：{error}"))?;
                Ok(json!({
                    "path": relative,
                    "bytes": bytes,
                    "sha256": sha256_file(&path)?
                }))
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn run_root(
        root_dir: PathBuf,
        durability: Durability,
        root: usize,
        trial: usize,
        snapshot: Arc<SealedProcessRuleSnapshotV3>,
        scenario: Arc<SealedPlatformScenarioV1>,
        finality: Arc<TradeFinalityPolicyV2>,
        latency: Arc<EmpiricalDelayProfile>,
        identity: ImplementationIdentityV1,
        cancelled: Arc<AtomicBool>,
    ) -> Result<RootResult, String> {
        fs::create_dir_all(&root_dir).map_err(|error| format!("创建运行根失败：{error}"))?;
        let log_path = root_dir.join("sim-event-log-v2.jsonl");
        if log_path.exists() {
            fs::remove_file(&log_path).map_err(|error| format!("清理旧运行日志失败：{error}"))?;
        }
        let run_id = format!("{RUNNER_IDENTITY}-{}-{trial}-{root}", durability.label());
        let started = Instant::now();
        let role = match durability {
            Durability::CleanRun => RunDurabilityRoleV2::CleanRun,
            Durability::RecoverablePaper => RunDurabilityRoleV2::RecoverablePaper,
        };
        let caller_role = SimCallerRoleV1::Paper;
        let opened = RunOpenedV2 {
            projection_version: Default::default(),
            run_id: run_id.clone(),
            durability_role: role,
            platform_rule_snapshot_file_sha256: snapshot.file_sha256().clone(),
            trade_finality_policy_file_sha256: finality.policy_id().clone(),
            platform_scenario_file_sha256: scenario.file_sha256().clone(),
            proxy_latency_profile_file_sha256: latency.file_sha256().clone(),
            implementation_identity: identity,
            market_event_source_identity_sha256: Some(Sha256Hex("7".repeat(64))),
        };
        let mut execution = SimExecution::create_new(
            &log_path,
            opened,
            Arc::clone(&snapshot),
            Arc::clone(&finality),
            Arc::clone(&scenario),
            Arc::clone(&latency),
            Box::<AcceptingProjectionPort>::default(),
            MAX_GROUP_BYTES,
        )
        .map_err(|error| format!("创建固定测量根失败：{error}"))?;
        let spec = SessionSpecV3 {
            run_id: run_id.clone(),
            session_id: format!("session-{trial}-{root}"),
            account_id: "shared-account".to_owned(),
            caller_role,
            caller_instance_id: format!("caller-{root}"),
            creation_request_id: format!("create-{trial}-{root}"),
            response_bridge_binding_id: format!("bridge-{root}"),
            initial_account_state: InitialAccountStateV2 {
                available_pusd: ExactDecimal("2000".to_owned()),
            },
            session_seed: Sha256Hex("8".repeat(64)),
        };
        execution
            .create_session(spec.clone())
            .map_err(|error| format!("创建会话失败：{error}"))?;
        execution
            .create_session(spec)
            .map_err(|error| format!("重试创建会话失败：{error}"))?;
        execution
            .activate_session(format!("activate-{root}"))
            .map_err(|error| format!("激活会话失败：{error}"))?;

        let buckets = golden_buckets(&scenario)?;
        let active_paths: &[MarketEventPathV1] = &[MarketEventPathV1::PublicVisibility];
        let public_buckets = buckets
            .iter()
            .filter(|(reference, _)| reference.path == MarketEventPathV1::PublicVisibility)
            .collect::<Vec<_>>();
        let mut next_bucket = 0_usize;
        while next_bucket < 2 {
            drive_real_bucket(&mut execution, public_buckets[next_bucket])?;
            next_bucket += 1;
        }
        let mut resource_high_water = measurement_resources(&execution)?;

        drive_fixed_requests(
            &mut execution,
            &scenario,
            root,
            false,
            &public_buckets,
            &mut next_bucket,
            &cancelled,
            &mut resource_high_water,
        )?;
        while next_bucket < public_buckets.len() {
            drive_required_before_next_bucket(&mut execution, &public_buckets, &mut next_bucket)?;
            drive_real_bucket(&mut execution, public_buckets[next_bucket])?;
            next_bucket += 1;
            merge_numeric_max(
                &mut resource_high_water,
                &measurement_resources(&execution)?,
            );
        }
        let retention_before = measurement_resources(&execution)?;
        merge_numeric_max(&mut resource_high_water, &retention_before);
        execution
            .advance_retention(RetentionAdvanceV2 {
                earliest_retained_root_sequence: 0,
                order_history_retained_from_utc_ms: None,
                trade_history_retained_from_utc_ms: None,
            })
            .map_err(|error| format!("推进保留水位失败：{error}"))?;
        let retention_after = measurement_resources(&execution)?;

        let mut observations = drain_measurements(&execution)?;
        let mut resources = retention_after.clone();
        let mut cold_candidate_accepted_inputs = None;
        let mut paper_recovered = false;
        let recovery_started = Instant::now();
        match durability {
            Durability::CleanRun => {
                let candidate = execution
                    .complete_clean_run(bucket_reader(&buckets))
                    .map_err(|error| format!("CleanRun 冷完成失败：{error}"))?;
                #[cfg(feature = "measurement")]
                append_measurements(
                    &mut observations,
                    serde_json::to_value(candidate.measurement_observations())
                        .map_err(|error| format!("编码 CleanRun 封口/恢复观察失败：{error}"))?,
                )?;
                cold_candidate_accepted_inputs = Some(candidate.accepted_input_count());
                if candidate.run_id() != run_id || candidate.log_path() != log_path {
                    return Err("CleanRun 冷候选身份不一致".to_owned());
                }
            }
            Durability::RecoverablePaper => {
                drop(execution);
                let recovered = SimExecution::recover_paper(
                    &log_path,
                    snapshot,
                    finality,
                    scenario,
                    latency,
                    Box::<AcceptingProjectionPort>::default(),
                    MAX_GROUP_BYTES,
                    bucket_reader(&buckets),
                )
                .map_err(|error| format!("Paper 恢复失败：{error}"))?;
                append_measurements(&mut observations, drain_measurements(&recovered)?)?;
                resources = measurement_resources(&recovered)?;
                paper_recovered = true;
                drop(recovered);
            }
        }
        let recovery_elapsed_ns = elapsed_ns(recovery_started);
        let elapsed_ns = elapsed_ns(started);
        let log_bytes = fs::metadata(&log_path)
            .map_err(|error| format!("读取日志 metadata 失败：{error}"))?
            .len();
        Ok(RootResult {
            run_id,
            elapsed_ns,
            observations,
            resources,
            root_resources: RootResources {
                log_bytes,
                durable_bucket_count: buckets
                    .iter()
                    .filter(|(reference, _)| active_paths.contains(&reference.path))
                    .count(),
                durable_event_count: buckets
                    .iter()
                    .filter(|(reference, _)| active_paths.contains(&reference.path))
                    .map(|(_, events)| events.len())
                    .sum(),
                cold_candidate_accepted_inputs,
                paper_recovered,
                recovery_elapsed_ns,
                retention_before,
                retention_after,
                resource_high_water,
            },
        })
    }

    fn bucket_reader<'a>(
        buckets: &'a [(
            DurableMarketBucketReferenceV2,
            Arc<[ExecutionMarketEventV3]>,
        )],
    ) -> impl FnMut(
        &DurableMarketBucketReferenceV2,
    ) -> Result<Arc<[ExecutionMarketEventV3]>, SimRunActorErrorV2>
    + 'a {
        move |reference| {
            buckets
                .iter()
                .find(|(candidate, _)| candidate == reference)
                .map(|(_, events)| Arc::clone(events))
                .ok_or(SimRunActorErrorV2::CompletionNotReady(
                    "固定 runner 找不到耐久市场桶正文",
                ))
        }
    }

    pub(super) fn golden_buckets(
        scenario: &SealedPlatformScenarioV1,
    ) -> Result<
        Vec<(
            DurableMarketBucketReferenceV2,
            Arc<[ExecutionMarketEventV3]>,
        )>,
        String,
    > {
        golden_buckets_with_shift(scenario, 0)
    }

    pub(super) fn golden_buckets_with_shift(
        scenario: &SealedPlatformScenarioV1,
        time_shift_ms: i64,
    ) -> Result<
        Vec<(
            DurableMarketBucketReferenceV2,
            Arc<[ExecutionMarketEventV3]>,
        )>,
        String,
    > {
        let markets = scenario.scenario().market_set().markets();
        let source_times = [
            MARKET_ONE_START + 10 + time_shift_ms,
            MARKET_TWO_START + 10 + time_shift_ms,
        ];
        let mut hidden_events = Vec::new();
        let mut public_events = Vec::new();
        for (index, market) in markets.iter().enumerate() {
            hidden_events.push(Arc::from(
                initial_events(
                    market,
                    source_times[index],
                    source_times[index] + PUBLIC_DELAY_MS,
                    1 + (index as u64 * 2),
                )?
                .into_boxed_slice(),
            ));
            public_events.push(Arc::clone(hidden_events.last().expect("刚插入初始事件")));
        }
        let body_events = [
            Arc::from(
                vec![body_event(
                    &markets[0],
                    BODY_ONE_SOURCE + time_shift_ms,
                    BODY_ONE_SOURCE + PUBLIC_DELAY_MS + time_shift_ms,
                    5,
                )?]
                .into_boxed_slice(),
            ),
            Arc::from(
                vec![body_event(
                    &markets[1],
                    BODY_TWO_SOURCE + time_shift_ms,
                    BODY_TWO_SOURCE + PUBLIC_DELAY_MS + time_shift_ms,
                    6,
                )?]
                .into_boxed_slice(),
            ),
        ];
        let resolution_events = [
            Arc::from(
                vec![resolution_event(
                    &markets[0],
                    RESOLUTION_ONE_SOURCE + time_shift_ms,
                    RESOLUTION_ONE_SOURCE + PUBLIC_DELAY_MS + time_shift_ms,
                    7,
                )?]
                .into_boxed_slice(),
            ),
            Arc::from(
                vec![resolution_event(
                    &markets[1],
                    RESOLUTION_TWO_SOURCE + time_shift_ms,
                    RESOLUTION_TWO_SOURCE + PUBLIC_DELAY_MS + time_shift_ms,
                    8,
                )?]
                .into_boxed_slice(),
            ),
        ];
        let final_events: Arc<[ExecutionMarketEventV3]> = Arc::from(
            vec![body_event(
                &markets[0],
                FINAL_EOF_SOURCE + time_shift_ms,
                FINAL_EOF_SOURCE + PUBLIC_DELAY_MS + time_shift_ms,
                9,
            )?]
            .into_boxed_slice(),
        );
        let mut buckets = Vec::new();
        for path in [
            MarketEventPathV1::HiddenPlatform,
            MarketEventPathV1::PublicVisibility,
        ] {
            let times = match path {
                MarketEventPathV1::HiddenPlatform => [
                    source_times[0],
                    source_times[1],
                    BODY_ONE_SOURCE + time_shift_ms,
                    BODY_TWO_SOURCE + time_shift_ms,
                    RESOLUTION_ONE_SOURCE + time_shift_ms,
                    RESOLUTION_TWO_SOURCE + time_shift_ms,
                    FINAL_EOF_SOURCE + time_shift_ms,
                ],
                MarketEventPathV1::PublicVisibility => [
                    source_times[0] + PUBLIC_DELAY_MS,
                    source_times[1] + PUBLIC_DELAY_MS,
                    BODY_ONE_SOURCE + PUBLIC_DELAY_MS + time_shift_ms,
                    BODY_TWO_SOURCE + PUBLIC_DELAY_MS + time_shift_ms,
                    RESOLUTION_ONE_SOURCE + PUBLIC_DELAY_MS + time_shift_ms,
                    RESOLUTION_TWO_SOURCE + PUBLIC_DELAY_MS + time_shift_ms,
                    FINAL_EOF_SOURCE + PUBLIC_DELAY_MS + time_shift_ms,
                ],
            };
            let events = [
                Arc::clone(&public_events[0]),
                Arc::clone(&public_events[1]),
                Arc::clone(&body_events[0]),
                Arc::clone(&body_events[1]),
                Arc::clone(&resolution_events[0]),
                Arc::clone(&resolution_events[1]),
                Arc::clone(&final_events),
            ];
            let positions = [(0, 2), (2, 4), (4, 5), (5, 6), (6, 7), (7, 8), (8, 9)];
            for index in 0..7 {
                buckets.push((
                    DurableMarketBucketReferenceV2 {
                        path,
                        bucket_at_utc_ms: times[index],
                        start_index_position: positions[index].0,
                        next_index_position: positions[index].1,
                        closure: if index == 6 {
                            MillisecondBucketClosureV1::EndOfInput
                        } else {
                            MillisecondBucketClosureV1::NextEventAt(times[index + 1])
                        },
                    },
                    Arc::clone(&events[index]),
                ));
            }
        }
        Ok(buckets)
    }

    fn initial_events(
        market: &PlatformMarketV1,
        source_at: i64,
        received_at: i64,
        first_sequence: u64,
    ) -> Result<Vec<ExecutionMarketEventV3>, String> {
        market
            .assets()
            .iter()
            .enumerate()
            .map(|(index, asset)| {
                canonical_event(
                    market,
                    ExecutionMarketEventPayloadV3::InitialL2Snapshot(InitialL2SnapshotV2 {
                        asset: asset.clone(),
                        initial_tick_size_pusd_per_share: ExactDecimal("0.01".to_owned()),
                        source_sequence_epoch_id: "measurement-epoch-v1".to_owned(),
                        source_cursor: first_sequence + index as u64,
                        book_digest_sha256: Sha256Hex(if index == 0 {
                            "e".repeat(64)
                        } else {
                            "f".repeat(64)
                        }),
                        bids: vec![PriceLevelV1 {
                            price_pusd_per_share: ExactDecimal("0.4".to_owned()),
                            quantity_shares: ExactDecimal("20".to_owned()),
                        }],
                        asks: vec![PriceLevelV1 {
                            price_pusd_per_share: ExactDecimal("0.6".to_owned()),
                            quantity_shares: ExactDecimal("20".to_owned()),
                        }],
                    }),
                    source_at,
                    received_at,
                    first_sequence + index as u64,
                )
            })
            .collect()
    }

    fn body_event(
        market: &PlatformMarketV1,
        source_at: i64,
        received_at: i64,
        sequence: u64,
    ) -> Result<ExecutionMarketEventV3, String> {
        canonical_event(
            market,
            ExecutionMarketEventPayloadV3::BestBidAsk(BestBidAskV2 {
                asset: market.assets()[0].clone(),
                best_bid_pusd_per_share: Some(ExactDecimal("0.4".to_owned())),
                best_ask_pusd_per_share: Some(ExactDecimal("0.6".to_owned())),
            }),
            source_at,
            received_at,
            sequence,
        )
    }

    fn resolution_event(
        market: &PlatformMarketV1,
        source_at: i64,
        received_at: i64,
        sequence: u64,
    ) -> Result<ExecutionMarketEventV3, String> {
        let winner = &market.assets()[0];
        canonical_event(
            market,
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(
                MarketResolutionTerminalV3::Resolved {
                    winning_asset_id: winner.token_id.0.clone(),
                    winning_outcome: winner.outcome.label.clone(),
                    payout_vector: market
                        .assets()
                        .iter()
                        .enumerate()
                        .map(|(index, asset)| PayoutEntryV1 {
                            asset: asset.clone(),
                            payout_pusd_per_share: ExactDecimal(if index == 0 {
                                "1".to_owned()
                            } else {
                                "0".to_owned()
                            }),
                        })
                        .collect(),
                    mapping_rule_version: "measurement-resolution-v1".to_owned(),
                },
            ),
            source_at,
            received_at,
            sequence,
        )
    }

    fn canonical_event(
        market: &PlatformMarketV1,
        payload: ExecutionMarketEventPayloadV3,
        source_at: i64,
        received_at: i64,
        sequence: u64,
    ) -> Result<ExecutionMarketEventV3, String> {
        let evidence = SourceEvidenceRefV2 {
            source_kind: CanonicalSourceKind::PolymarketOrderbook,
            source_native_identity: None,
            source_event_at_utc_ms: Some(source_at),
            received_at_utc_ms: received_at,
            collector_admission: Some(CollectorAdmissionCoordinateV1 {
                source_sequence_epoch_id: "measurement-epoch-v1".to_owned(),
                collector_admission_sequence: sequence,
                admitted_at_utc_ms: received_at,
            }),
            input_manifest_sha256: Sha256Hex("a".repeat(64)),
            input_file_sha256: Sha256Hex("b".repeat(64)),
        };
        let mut event = ExecutionMarketEventV3 {
            schema_version: contracts::EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
            event_id: CanonicalEventId("0".repeat(64)),
            market: market.market().clone(),
            time: EventTimeV2 {
                source_event_at_utc_ms: Some(source_at),
                received_at_utc_ms: received_at,
            },
            lineage: EventLineageV2 {
                canonical_rule_version: "measurement-event-v1".to_owned(),
                source_evidence: vec![evidence],
            },
            quality: EventQualityV2 {
                availability: MarketDataQuality::Fresh,
                resync: ResyncQuality::NotRequired,
            },
            payload,
        };
        event.event_id = derive_execution_market_event_v3_id(&event)
            .map_err(|error| format!("派生固定事件身份失败：{error}"))?;
        let encoded = encode_execution_market_event_v3(&event)
            .map_err(|error| format!("编码固定强类型事件失败：{error}"))?;
        decode_execution_market_event_v3(&encoded)
            .map_err(|error| format!("解码固定强类型事件失败：{error}"))
    }

    #[allow(clippy::too_many_arguments)]
    fn drive_fixed_requests(
        execution: &mut SimExecution,
        scenario: &SealedPlatformScenarioV1,
        root: usize,
        targeted_retention: bool,
        buckets: &[&(
            DurableMarketBucketReferenceV2,
            Arc<[ExecutionMarketEventV3]>,
        )],
        next_bucket: &mut usize,
        cancelled: &AtomicBool,
        resource_high_water: &mut Value,
    ) -> Result<(), String> {
        let mut next_decision_at_utc_ms = REQUEST_CUTOFF_BASE;
        drive_fixed_request_range(
            execution,
            scenario,
            root,
            targeted_retention,
            0..LOOPS_PER_ROOT,
            None,
            buckets,
            next_bucket,
            cancelled,
            resource_high_water,
            &mut next_decision_at_utc_ms,
            0,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn drive_fixed_request_range(
        execution: &mut SimExecution,
        scenario: &SealedPlatformScenarioV1,
        root: usize,
        targeted_retention: bool,
        iterations: Range<usize>,
        targeted_queries: Option<&TargetedQueryPlan>,
        buckets: &[&(
            DurableMarketBucketReferenceV2,
            Arc<[ExecutionMarketEventV3]>,
        )],
        next_bucket: &mut usize,
        cancelled: &AtomicBool,
        resource_high_water: &mut Value,
        decision_cursor: &mut i64,
        time_shift_ms: i64,
    ) -> Result<(), String> {
        let markets = scenario.scenario().market_set().markets();
        let public_visibility: PublicVisibilityCursorV1 = serde_json::from_value(json!({
            "closed_through_received_at_utc_ms": MARKET_TWO_START + 10 + PUBLIC_DELAY_MS + time_shift_ms,
            "next_visibility_index_position": 4
        }))
        .map_err(|error| format!("构造公开可见游标失败：{error}"))?;
        let mut next_decision_at_utc_ms = *decision_cursor;
        for iteration in iterations {
            if cancelled.load(Ordering::Relaxed) {
                return Err("安全停止边界触发，固定请求循环已取消".to_owned());
            }
            let market = &markets[iteration % markets.len()];
            let condition_id = market.market().condition_id.clone();
            let token_id = market.assets()[0].token_id.clone();
            let prefix = format!("r{root}-i{iteration}");
            let context = |method: &str| RequestContextV2 {
                global_request_id: format!("{prefix}-{method}"),
                caller_correlation_id: Some(prefix.clone()),
            };
            let cutoff = |logical_time_utc_ms| TrustedDecisionCutoffV2 {
                logical_time_utc_ms,
                public_visibility: public_visibility.clone(),
                private_feedback_sequence: 0,
            };
            let post = |id: &str| PostOrderItemV1 {
                item_id: id.to_owned(),
                order: SimBuyOrderV1 {
                    condition_id: condition_id.clone(),
                    token_id: token_id.clone(),
                    kind: SimBuyOrderKindV1::GtcLimit {
                        limit_price_pusd_per_share: "0.7".parse().expect("固定价格"),
                        size_shares: "5".parse().expect("固定数量"),
                        post_only: true,
                    },
                },
            };
            let first_post = || PostOrderItemV1 {
                item_id: format!("{prefix}-post-item"),
                order: SimBuyOrderV1 {
                    condition_id: condition_id.clone(),
                    token_id: token_id.clone(),
                    kind: if targeted_retention && iteration == 0 {
                        SimBuyOrderKindV1::FokMarket {
                            spend_pusd: "3".parse().expect("固定 targeted FOK 预算"),
                            worst_price_pusd_per_share: "0.6"
                                .parse()
                                .expect("固定 targeted FOK 最差价"),
                        }
                    } else {
                        SimBuyOrderKindV1::GtcLimit {
                            limit_price_pusd_per_share: "0.7".parse().expect("固定价格"),
                            size_shares: "5".parse().expect("固定数量"),
                            post_only: true,
                        }
                    },
                },
            };
            let receipt = execution
                .post_order(
                    cutoff(next_decision_at_utc_ms),
                    PostOrderRequestV2 {
                        context: context("post-order"),
                        item: first_post(),
                    },
                )
                .map_err(request_error)?;
            let request_required_at_utc_ms = receipt.platform_arrived_at_utc_ms();
            next_decision_at_utc_ms = receipt.platform_arrived_at_utc_ms().saturating_add(1);
            drive_until_current_request_is_complete(
                execution,
                buckets,
                next_bucket,
                request_required_at_utc_ms,
            )?;
            let receipt = execution
                .post_orders(
                    cutoff(next_decision_at_utc_ms),
                    PostOrdersRequestV2 {
                        context: context("post-orders"),
                        items: vec![PostOrderBatchItemV2 {
                            batch_item_ordinal: 0,
                            item: post(&format!("{prefix}-posts-item")),
                        }],
                    },
                )
                .map_err(request_error)?;
            let request_required_at_utc_ms = receipt.platform_arrived_at_utc_ms();
            next_decision_at_utc_ms = receipt.platform_arrived_at_utc_ms().saturating_add(1);
            drive_until_current_request_is_complete(
                execution,
                buckets,
                next_bucket,
                request_required_at_utc_ms,
            )?;
            let receipt = execution
                .cancel_order(
                    cutoff(next_decision_at_utc_ms),
                    CancelOrderRequestV2 {
                        context: context("cancel-order"),
                        item: CancelOrderItemV1 {
                            item_id: format!("{prefix}-cancel-item"),
                            platform_order_id: "sim-1".to_owned(),
                        },
                    },
                )
                .map_err(request_error)?;
            let request_required_at_utc_ms = receipt.platform_arrived_at_utc_ms();
            next_decision_at_utc_ms = receipt.platform_arrived_at_utc_ms().saturating_add(1);
            drive_until_current_request_is_complete(
                execution,
                buckets,
                next_bucket,
                request_required_at_utc_ms,
            )?;
            let receipt = execution
                .cancel_orders(
                    cutoff(next_decision_at_utc_ms),
                    CancelOrdersRequestV2 {
                        context: context("cancel-orders"),
                        items: vec![CancelOrderBatchItemV2 {
                            batch_item_ordinal: 0,
                            item: CancelOrderItemV1 {
                                item_id: format!("{prefix}-cancels-item"),
                                platform_order_id: "sim-2".to_owned(),
                            },
                        }],
                    },
                )
                .map_err(request_error)?;
            let request_required_at_utc_ms = receipt.platform_arrived_at_utc_ms();
            next_decision_at_utc_ms = receipt.platform_arrived_at_utc_ms().saturating_add(1);
            drive_until_current_request_is_complete(
                execution,
                buckets,
                next_bucket,
                request_required_at_utc_ms,
            )?;
            let receipt = execution
                .cancel_all_orders(
                    cutoff(next_decision_at_utc_ms),
                    CancelAllOrdersRequestV2 {
                        context: context("cancel-all-orders"),
                    },
                )
                .map_err(request_error)?;
            let request_required_at_utc_ms = receipt.platform_arrived_at_utc_ms();
            next_decision_at_utc_ms = receipt.platform_arrived_at_utc_ms().saturating_add(1);
            drive_until_current_request_is_complete(
                execution,
                buckets,
                next_bucket,
                request_required_at_utc_ms,
            )?;
            let receipt = execution
                .order(
                    cutoff(next_decision_at_utc_ms),
                    OrderRequestV2 {
                        context: context("order"),
                        platform_order_id: "sim-1".to_owned(),
                    },
                )
                .map_err(request_error)?;
            let request_required_at_utc_ms = receipt.platform_arrived_at_utc_ms();
            next_decision_at_utc_ms = receipt.platform_arrived_at_utc_ms().saturating_add(1);
            drive_until_current_request_is_complete(
                execution,
                buckets,
                next_bucket,
                request_required_at_utc_ms,
            )?;
            let (orders_filter, orders_cursor) = match (targeted_queries, iteration) {
                (Some(plan), 253) => (
                    OrdersFilterV1::default(),
                    Some(QueryCursorV2 {
                        root_sequence: plan.old_root_sequence,
                        next_offset: 0,
                    }),
                ),
                _ => (
                    OrdersFilterV1 {
                        condition_id: Some(condition_id.clone()),
                        token_id: Some(token_id.clone()),
                    },
                    None,
                ),
            };
            let receipt = execution
                .orders(
                    cutoff(next_decision_at_utc_ms),
                    OrdersRequestV2 {
                        context: context("orders"),
                        filter: orders_filter,
                        cursor: orders_cursor,
                    },
                )
                .map_err(request_error)?;
            let request_required_at_utc_ms = receipt.platform_arrived_at_utc_ms();
            next_decision_at_utc_ms = receipt.platform_arrived_at_utc_ms().saturating_add(1);
            drive_until_current_request_is_complete(
                execution,
                buckets,
                next_bucket,
                request_required_at_utc_ms,
            )?;
            let (trades_filter, trades_cursor) = match (targeted_queries, iteration) {
                (Some(plan), 253) => (
                    TradesFilterV1::default(),
                    Some(QueryCursorV2 {
                        root_sequence: plan.old_root_sequence,
                        next_offset: 0,
                    }),
                ),
                (Some(plan), 254) => (
                    TradesFilterV1 {
                        trade_id: Some(plan.trade_id.clone()),
                        ..TradesFilterV1::default()
                    },
                    None,
                ),
                (Some(plan), 255) => (
                    TradesFilterV1 {
                        after_utc_ms: Some(plan.retained_from_utc_ms - 1),
                        before_utc_ms: Some(plan.retained_from_utc_ms + 1),
                        ..TradesFilterV1::default()
                    },
                    None,
                ),
                _ => (
                    TradesFilterV1 {
                        condition_id: Some(condition_id),
                        token_id: Some(token_id),
                        ..Default::default()
                    },
                    None,
                ),
            };
            let receipt = execution
                .trades(
                    cutoff(next_decision_at_utc_ms),
                    TradesRequestV2 {
                        context: context("trades"),
                        filter: trades_filter,
                        cursor: trades_cursor,
                    },
                )
                .map_err(request_error)?;
            let request_required_at_utc_ms = receipt.platform_arrived_at_utc_ms();
            next_decision_at_utc_ms = receipt.platform_arrived_at_utc_ms().saturating_add(1);
            drive_until_current_request_is_complete(
                execution,
                buckets,
                next_bucket,
                request_required_at_utc_ms,
            )?;
            merge_numeric_max(resource_high_water, &measurement_resources(execution)?);
        }
        *decision_cursor = next_decision_at_utc_ms;
        Ok(())
    }

    fn drive_until_current_request_is_complete(
        execution: &mut SimExecution,
        buckets: &[&(
            DurableMarketBucketReferenceV2,
            Arc<[ExecutionMarketEventV3]>,
        )],
        next_bucket: &mut usize,
        request_required_at_utc_ms: i64,
    ) -> Result<(), String> {
        while execution
            .next_required_logical_drive()
            .is_some_and(|required| required.required_at_utc_ms <= request_required_at_utc_ms)
        {
            if *next_bucket >= buckets.len() {
                return Err("固定请求在 EOF 前仍有必要逻辑工作".to_owned());
            }
            let required = execution
                .next_required_logical_drive()
                .expect("循环已经确认下一必要推进存在");
            let next_reference = &buckets[*next_bucket].0;
            if next_reference.bucket_at_utc_ms <= required.required_at_utc_ms {
                drive_real_bucket(execution, buckets[*next_bucket])?;
                *next_bucket += 1;
            } else {
                execution
                    .drive_platform_prefix(sim_system::execution::PlatformPrefixAdvanceV2 {
                        path: required.path,
                        closed_through_utc_ms: required.required_at_utc_ms,
                        next_index_position: next_reference.start_index_position,
                    })
                    .map_err(|error| {
                        format!(
                            "驱动请求必要空前缀失败：required={required:?}, next_bucket={next_reference:?}: {error}"
                        )
                    })?;
            }
        }
        Ok(())
    }

    fn drive_required_before_next_bucket(
        execution: &mut SimExecution,
        buckets: &[&(
            DurableMarketBucketReferenceV2,
            Arc<[ExecutionMarketEventV3]>,
        )],
        next_bucket: &mut usize,
    ) -> Result<(), String> {
        let next_reference = &buckets[*next_bucket].0;
        while execution
            .next_required_logical_drive()
            .is_some_and(|required| required.required_at_utc_ms < next_reference.bucket_at_utc_ms)
        {
            let required = execution
                .next_required_logical_drive()
                .expect("循环已经确认下一必要推进存在");
            execution
                .drive_platform_prefix(sim_system::execution::PlatformPrefixAdvanceV2 {
                    path: required.path,
                    closed_through_utc_ms: required.required_at_utc_ms,
                    next_index_position: next_reference.start_index_position,
                })
                .map_err(|error| {
                    format!(
                        "驱动桶前必要空前缀失败：required={required:?}, next_bucket={next_reference:?}: {error}"
                    )
                })?;
        }
        Ok(())
    }

    fn drive_real_bucket(
        execution: &mut SimExecution,
        bucket: &(
            DurableMarketBucketReferenceV2,
            Arc<[ExecutionMarketEventV3]>,
        ),
    ) -> Result<(), String> {
        execution
            .drive_closed_market_bucket(AcceptedClosedMarketBucketV2 {
                reference: bucket.0.clone(),
                events: Arc::clone(&bucket.1),
            })
            .map_err(|error| format!("驱动固定真实市场桶失败：{error}"))
    }

    fn request_error(error: SimRunActorErrorV2) -> String {
        format!("固定公开请求失败：{error}")
    }

    #[cfg(feature = "measurement")]
    fn drain_measurements(execution: &SimExecution) -> Result<Value, String> {
        serde_json::to_value(execution.drain_measurement_observations())
            .map_err(|error| format!("编码测量观察失败：{error}"))
    }

    #[cfg(not(feature = "measurement"))]
    fn drain_measurements(_execution: &SimExecution) -> Result<Value, String> {
        Ok(Value::Null)
    }

    #[cfg(feature = "measurement")]
    fn measurement_resources(execution: &SimExecution) -> Result<Value, String> {
        serde_json::to_value(execution.measurement_resource_snapshot())
            .map_err(|error| format!("编码测量资源失败：{error}"))
    }

    #[cfg(not(feature = "measurement"))]
    fn measurement_resources(_execution: &SimExecution) -> Result<Value, String> {
        Ok(Value::Null)
    }

    fn append_measurements(target: &mut Value, additional: Value) -> Result<(), String> {
        match (target, additional) {
            (Value::Null, Value::Null) => Ok(()),
            (Value::Array(target), Value::Array(mut additional)) => {
                target.append(&mut additional);
                Ok(())
            }
            _ => Err("测量观察的 feature 形状不一致".to_owned()),
        }
    }

    fn merge_numeric_max(target: &mut Value, candidate: &Value) {
        match (target, candidate) {
            (slot @ Value::Null, candidate) => *slot = candidate.clone(),
            (Value::Object(target), Value::Object(candidate)) => {
                for (key, value) in candidate {
                    merge_numeric_max(target.entry(key.clone()).or_insert(Value::Null), value);
                }
            }
            (Value::Number(target), Value::Number(candidate)) => {
                if candidate.as_u64().unwrap_or_default() > target.as_u64().unwrap_or_default() {
                    *target = candidate.clone();
                }
            }
            _ => {}
        }
    }

    fn stage_elapsed(observations: &Value) -> Value {
        let Some(observations) = observations.as_array() else {
            return Value::Null;
        };
        let mut stages = BTreeMap::<String, u64>::new();
        for observation in observations {
            if let (Some(phase), Some(elapsed)) = (
                observation.get("phase").and_then(Value::as_str),
                observation.get("elapsed_ns").and_then(Value::as_u64),
            ) {
                *stages.entry(phase.to_owned()).or_default() = stages
                    .get(phase)
                    .copied()
                    .unwrap_or_default()
                    .saturating_add(elapsed);
            }
        }
        serde_json::to_value(stages).expect("BTreeMap 测量阶段可序列化")
    }

    #[cfg(all(test, not(feature = "measurement")))]
    pub(super) fn internal_measurement_fields_for_test() -> (Value, Value) {
        (Value::Null, Value::Null)
    }

    fn summarize_trials(samples: &[Sample]) -> Vec<TrialSummary> {
        let mut trials = Vec::new();
        for durability in [Durability::CleanRun, Durability::RecoverablePaper] {
            for shape in [Shape::SingleRoot, Shape::FourRoots] {
                for trial in WARMUP_TRIALS..(WARMUP_TRIALS + TRIALS) {
                    let selected = samples
                        .iter()
                        .filter(|sample| {
                            sample.durability == durability
                                && sample.shape == shape
                                && sample.trial == trial
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(selected.len(), shape.roots(), "trial 的隔离根数量必须完整");
                    let elapsed_ns = selected[0].trial_elapsed_ns;
                    let requests = selected[0].trial_requests;
                    assert!(selected.iter().all(|sample| {
                        sample.trial_elapsed_ns == elapsed_ns && sample.trial_requests == requests
                    }));
                    let mut phase_values = BTreeMap::<String, Vec<u64>>::new();
                    for sample in &selected {
                        if let Some(observations) = sample.measurement_observations.as_array() {
                            for observation in observations {
                                if let (Some(phase), Some(elapsed)) = (
                                    observation.get("phase").and_then(Value::as_str),
                                    observation.get("elapsed_ns").and_then(Value::as_u64),
                                ) {
                                    phase_values
                                        .entry(phase.to_owned())
                                        .or_default()
                                        .push(elapsed);
                                }
                            }
                        }
                    }
                    let public_request_latency_ns = phase_values
                        .get("public_end_to_end")
                        .cloned()
                        .map(distribution_summary);
                    trials.push(TrialSummary {
                        workload: WORKLOAD_IDENTITY.to_owned(),
                        durability,
                        shape,
                        trial,
                        requests,
                        elapsed_ns,
                        throughput_requests_per_second: u64::try_from(
                            (requests as u128)
                                .saturating_mul(1_000_000_000)
                                .checked_div(u128::from(elapsed_ns.max(1)))
                                .unwrap_or_default(),
                        )
                        .unwrap_or(u64::MAX),
                        public_request_latency_ns,
                        stage_latency_ns: phase_values
                            .into_iter()
                            .map(|(phase, values)| (phase, distribution_summary(values)))
                            .collect(),
                        cpu_ticks: selected[0]
                            .proc_after
                            .cpu_ticks
                            .saturating_sub(selected[0].proc_before.cpu_ticks),
                        rss_peak_bytes: selected[0].trial_rss_peak_bytes,
                        io_read_bytes: selected[0]
                            .proc_after
                            .io_read_bytes
                            .saturating_sub(selected[0].proc_before.io_read_bytes),
                        io_write_bytes: selected[0]
                            .proc_after
                            .io_write_bytes
                            .saturating_sub(selected[0].proc_before.io_write_bytes),
                        log_bytes: selected
                            .iter()
                            .map(|sample| sample.resources.log_bytes)
                            .sum(),
                        recovery_elapsed_ns: selected
                            .iter()
                            .map(|sample| sample.resources.recovery_elapsed_ns)
                            .max()
                            .expect("trial 至少一个根"),
                        retention_before: sum_numeric_values(
                            selected
                                .iter()
                                .map(|sample| &sample.resources.retention_before),
                        ),
                        retention_after: sum_numeric_values(
                            selected
                                .iter()
                                .map(|sample| &sample.resources.retention_after),
                        ),
                        resource_high_water: sum_numeric_values(
                            selected
                                .iter()
                                .map(|sample| &sample.resources.resource_high_water),
                        ),
                    });
                }
            }
        }
        trials
    }

    fn numeric_leaves(value: &Value) -> BTreeMap<String, u64> {
        fn visit(prefix: &str, value: &Value, output: &mut BTreeMap<String, u64>) {
            match value {
                Value::Object(object) => {
                    for (key, value) in object {
                        let path = if prefix.is_empty() {
                            key.clone()
                        } else {
                            format!("{prefix}.{key}")
                        };
                        visit(&path, value, output);
                    }
                }
                Value::Number(number) => {
                    if let Some(value) = number.as_u64() {
                        output.insert(prefix.to_owned(), value);
                    }
                }
                _ => {}
            }
        }
        let mut output = BTreeMap::new();
        visit("", value, &mut output);
        output
    }

    fn sum_numeric_values<'a>(values: impl Iterator<Item = &'a Value>) -> Value {
        let mut sums = BTreeMap::<String, u64>::new();
        for value in values {
            for (key, value) in numeric_leaves(value) {
                *sums.entry(key).or_default() = sums
                    .get(&key)
                    .copied()
                    .unwrap_or_default()
                    .saturating_add(value);
            }
        }
        serde_json::to_value(sums).expect("trial 资源和可序列化")
    }

    fn summarize_numeric_values<'a>(
        values: impl Iterator<Item = &'a Value>,
    ) -> BTreeMap<String, DistributionSummary> {
        let mut grouped = BTreeMap::<String, Vec<u64>>::new();
        for value in values {
            for (key, value) in numeric_leaves(value) {
                grouped.entry(key).or_default().push(value);
            }
        }
        grouped
            .into_iter()
            .map(|(key, values)| (key, distribution_summary(values)))
            .collect()
    }

    fn summarize(samples: &[Sample]) -> Vec<CellSummary> {
        let trials = summarize_trials(samples);
        let mut cells = Vec::new();
        for durability in [Durability::CleanRun, Durability::RecoverablePaper] {
            for shape in [Shape::SingleRoot, Shape::FourRoots] {
                let selected_trials = trials
                    .iter()
                    .filter(|trial| trial.durability == durability && trial.shape == shape)
                    .collect::<Vec<_>>();
                let slowest = selected_trials
                    .iter()
                    .max_by_key(|trial| trial.elapsed_ns)
                    .expect("每个 cell 必须含三个整体 trial");
                let mut phase_values = BTreeMap::<String, Vec<u64>>::new();
                for trial in &selected_trials {
                    for (phase, values) in &trial.stage_latency_ns {
                        phase_values
                            .entry(phase.clone())
                            .or_default()
                            .push(values.p99);
                    }
                }
                let public_request_latency_ns = selected_trials
                    .iter()
                    .filter_map(|trial| trial.public_request_latency_ns.map(|values| values.p99))
                    .collect::<Vec<_>>();
                let public_request_latency_ns = (!public_request_latency_ns.is_empty())
                    .then(|| distribution_summary(public_request_latency_ns));
                cells.push(CellSummary {
                    durability,
                    shape,
                    operation: "golden_workload_e2e".to_owned(),
                    trial_count: selected_trials.len(),
                    requests_per_trial: selected_trials[0].requests,
                    elapsed_ns: distribution_summary(
                        selected_trials
                            .iter()
                            .map(|trial| trial.elapsed_ns)
                            .collect(),
                    ),
                    throughput_requests_per_second: distribution_summary(
                        selected_trials
                            .iter()
                            .map(|trial| trial.throughput_requests_per_second)
                            .collect(),
                    ),
                    public_request_latency_ns,
                    stage_latency_ns: phase_values
                        .into_iter()
                        .map(|(phase, values)| (phase, distribution_summary(values)))
                        .collect(),
                    cpu_ticks: distribution_summary(
                        selected_trials
                            .iter()
                            .map(|trial| trial.cpu_ticks)
                            .collect(),
                    ),
                    rss_high_water_bytes: distribution_summary(
                        selected_trials
                            .iter()
                            .map(|trial| trial.rss_peak_bytes)
                            .collect(),
                    ),
                    io_read_bytes: distribution_summary(
                        selected_trials
                            .iter()
                            .map(|trial| trial.io_read_bytes)
                            .collect(),
                    ),
                    io_write_bytes: distribution_summary(
                        selected_trials
                            .iter()
                            .map(|trial| trial.io_write_bytes)
                            .collect(),
                    ),
                    log_bytes: distribution_summary(
                        selected_trials
                            .iter()
                            .map(|trial| trial.log_bytes)
                            .collect(),
                    ),
                    recovery_elapsed_ns: distribution_summary(
                        selected_trials
                            .iter()
                            .map(|trial| trial.recovery_elapsed_ns)
                            .collect(),
                    ),
                    retention_before: summarize_numeric_values(
                        selected_trials.iter().map(|trial| &trial.retention_before),
                    ),
                    retention_after: summarize_numeric_values(
                        selected_trials.iter().map(|trial| &trial.retention_after),
                    ),
                    resource_high_water: summarize_numeric_values(
                        selected_trials
                            .iter()
                            .map(|trial| &trial.resource_high_water),
                    ),
                    slowest_trial_ref: format!(
                        "{}:{}:{}:{}",
                        BUILD,
                        slowest.durability.label(),
                        slowest.shape.label(),
                        slowest.trial
                    ),
                });
            }
        }
        cells
    }

    fn compare_baseline(
        args: &Args,
        identities: &IdentityBundle,
        trials: &[TrialSummary],
    ) -> Result<Option<BaselineComparison>, String> {
        let Some(path) = &args.baseline_summary else {
            return Ok(None);
        };
        let bytes =
            fs::read(path).map_err(|error| format!("读取 baseline summary 失败：{error}"))?;
        let baseline: Value = serde_json::from_slice(&bytes)
            .map_err(|error| format!("解码 baseline summary 失败：{error}"))?;
        if baseline.get("runner_identity").and_then(Value::as_str) != Some(RUNNER_IDENTITY) {
            return Err("baseline runner identity 不一致，停止比较".to_owned());
        }
        let baseline_identities: IdentityBundle = serde_json::from_value(
            baseline
                .get("identities")
                .cloned()
                .ok_or_else(|| "baseline 缺少规则/业务 identities，停止比较".to_owned())?,
        )
        .map_err(|error| format!("baseline identities 非法：{error}"))?;
        if !baseline_identities.same_business_identity(identities) {
            return Err("baseline 规则或业务身份不一致，停止比较".to_owned());
        }
        let baseline_build = baseline
            .get("build")
            .and_then(Value::as_str)
            .ok_or_else(|| "baseline 缺少 build".to_owned())?;
        if baseline_build != "measurement_off" || BUILD != "measurement_on" {
            return Err(
                "feature 开销要求 baseline=measurement_off 且 current=measurement_on".to_owned(),
            );
        }
        let baseline_trials: Vec<TrialSummary> = serde_json::from_value(
            baseline
                .get("trials")
                .cloned()
                .ok_or_else(|| "baseline 缺少 trials".to_owned())?,
        )
        .map_err(|error| format!("baseline trials 非法：{error}"))?;
        let key = |trial: &TrialSummary| {
            PairKey::new(
                &trial.workload,
                trial.durability.label(),
                trial.shape.label(),
                trial.trial,
            )
        };
        let baseline_keys = baseline_trials.iter().map(key).collect::<Vec<_>>();
        let current_keys = trials.iter().map(key).collect::<Vec<_>>();
        validate_feature_pair(baseline_build, &baseline_keys, BUILD, &current_keys)?;
        let mut comparisons = Vec::new();
        for trial in trials {
            let previous = baseline_trials
                .iter()
                .find(|previous| key(previous) == key(trial))
                .expect("配对检查后 baseline trial 必须存在");
            comparisons.push(BaselineTrialComparison {
                workload: trial.workload.clone(),
                durability: trial.durability,
                shape: trial.shape,
                trial: trial.trial,
                current_elapsed_ns: trial.elapsed_ns,
                baseline_elapsed_ns: previous.elapsed_ns,
                absolute_delta_ns: i128::from(trial.elapsed_ns) - i128::from(previous.elapsed_ns),
                ratio: trial.elapsed_ns as f64 / previous.elapsed_ns.max(1) as f64,
            });
        }
        Ok(Some(BaselineComparison {
            baseline_path: path.display().to_string(),
            baseline_build: baseline_build.to_owned(),
            pairing: "same_workload_durability_shape_trial",
            trials: comparisons,
        }))
    }

    fn write_candidate_table(output_dir: &Path, cells: &[CellSummary]) -> Result<(), String> {
        let mut table = BufWriter::new(
            File::create(output_dir.join("candidate-table.tsv"))
                .map_err(|error| format!("创建 candidate-table.tsv 失败：{error}"))?,
        );
        writeln!(table, "metric\tscope\tunit\traw_min\traw_median\traw_max\tslowest_sample_ref\tproposed_4_5_2_value\trounding_headroom_rationale")
            .map_err(|error| format!("写候选表头失败：{error}"))?;
        for cell in cells {
            let mut write = |metric: &str,
                             unit: &str,
                             values: DistributionSummary,
                             throughput: bool|
             -> Result<(), String> {
                let proposed = if throughput {
                    values.min.saturating_mul(4) / 5
                } else {
                    values.p99.saturating_mul(5).saturating_add(3) / 4
                };
                writeln!(table, "{}\tdurability={};shape={};operation={}\t{}\t{}\t{}\t{}\t{}\t{}\texploratory candidate with 25 percent {} headroom; not a formal gate",
                    metric, cell.durability.label(), cell.shape.label(), cell.operation,
                    unit, values.min, values.p50, values.max, cell.slowest_trial_ref, proposed,
                    if throughput { "downward" } else { "upward" })
                    .map_err(|error| format!("写候选表失败：{error}"))
            };
            write("e2e_elapsed", "ns", cell.elapsed_ns, false)?;
            write(
                "throughput",
                "requests_per_second",
                cell.throughput_requests_per_second,
                true,
            )?;
            if let Some(values) = cell.public_request_latency_ns {
                write("public_request_latency", "ns", values, false)?;
            }
            for (metric, unit, values) in [
                ("cpu_ticks", "ticks", cell.cpu_ticks),
                ("rss_high_water", "bytes", cell.rss_high_water_bytes),
                ("io_read", "bytes", cell.io_read_bytes),
                ("io_write", "bytes", cell.io_write_bytes),
                ("log", "bytes", cell.log_bytes),
                ("recovery_elapsed", "ns", cell.recovery_elapsed_ns),
            ] {
                write(metric, unit, values, false)?;
            }
            for (prefix, unit, values) in [
                ("phase", "ns", &cell.stage_latency_ns),
                ("retention_before", "quantity", &cell.retention_before),
                ("retention_after", "quantity", &cell.retention_after),
                ("resource_high_water", "quantity", &cell.resource_high_water),
            ] {
                for (name, values) in values {
                    write(&format!("{prefix}.{name}"), unit, *values, false)?;
                }
            }
        }
        table
            .flush()
            .map_err(|error| format!("刷新候选表失败：{error}"))
    }

    fn identities(
        snapshot: &SealedProcessRuleSnapshotV3,
        scenario: &SealedPlatformScenarioV1,
        finality: &TradeFinalityPolicyV2,
        latency: &EmpiricalDelayProfile,
    ) -> IdentityBundle {
        IdentityBundle {
            rule_snapshot_sha256: snapshot.file_sha256().0.clone(),
            synchronized_rule_values: serde_json::to_value(snapshot.synchronized_rules())
                .expect("公开同步规则值可序列化"),
            reference_market_condition_id: CONDITION.to_owned(),
            reference_market_clob_market_id: "3278040".to_owned(),
            reference_market_slug: SLUG.to_owned(),
            scenario_sha256: scenario.file_sha256().0.clone(),
            finality_policy_sha256: finality.policy_id().0.clone(),
            latency_profile_sha256: latency.file_sha256().0.clone(),
            workload_identity: WORKLOAD_IDENTITY.to_owned(),
            loops_per_root: LOOPS_PER_ROOT,
            requests_per_root: REQUESTS_PER_ROOT,
            market_count: 2,
        }
    }

    fn write_environment(
        args: &Args,
        identities: &IdentityBundle,
        start_unix_ms: u128,
        start_utc: &str,
        end_unix_ms: u128,
        end_utc: &str,
    ) -> Result<(), String> {
        let current_exe =
            std::env::current_exe().map_err(|error| format!("定位当前二进制失败：{error}"))?;
        let environment = json!({
            "schema_version": "sim_measurement_environment_v2",
            "runner_identity": RUNNER_IDENTITY,
            "build": BUILD,
            "argv": args.argv,
            "started_at_unix_ms": start_unix_ms,
            "started_at_utc": start_utc,
            "completed_at_unix_ms": end_unix_ms,
            "completed_at_utc": end_utc,
            "uname": command_output("uname", &["-a"]),
            "cpu_model": cpu_model()?,
            "logical_cores": std::thread::available_parallelism().map(|count| count.get())
                .map_err(|error| format!("读取逻辑核心数失败：{error}"))?,
            "git_identity": args.identity,
            "git_head": command_output("git", &["rev-parse", "HEAD"]),
            "binary": {
                "path": current_exe,
                "sha256": sha256_file(&current_exe)?,
            },
            "identities": identities,
            "scenario": {
                "market_count": 2,
                "market_end_at_utc_ms": MARKET_END,
                "request_cutoff_base_utc_ms": REQUEST_CUTOFF_BASE,

                "resolution_source_times_utc_ms": [RESOLUTION_ONE_SOURCE, RESOLUTION_TWO_SOURCE],
                "execution_paths": ["public_visibility"],
                "caller_role": "paper",
            },
            "workload_constants": {
                "loops_per_root": LOOPS_PER_ROOT,
                "methods_per_loop": 8,
                "requests_per_root": REQUESTS_PER_ROOT,
                "shapes": [{"name":"single_root","roots":1},{"name":"four_roots","roots":4}],
                "durabilities": ["clean_run", "recoverable_paper"],
                "max_group_bytes": MAX_GROUP_BYTES,
            },
            "safety_stop_limits_not_formal_gates": {
                "wall_ns": SafetyLimits::fixed().wall_ns,
                "trial_ns": SafetyLimits::fixed().trial_ns,
                "rss_bytes": SafetyLimits::fixed().rss_bytes,
                "output_bytes": SafetyLimits::fixed().output_bytes,
                "minimum_free_disk_bytes": SafetyLimits::fixed().free_disk_bytes,
            },
            "warmup": {
                "recorded": true,
                "trials_per_cell": WARMUP_TRIALS,
                "excluded_from_summary": true,
                "measured_trials_per_cell": TRIALS,
            },
            "proc_at_completion": proc_snapshot()?,
        });
        write_json(args.output_dir.join("environment.json"), &environment)
    }

    fn reference_market() -> ReferenceMarketSpecV1 {
        ReferenceMarketSpecV1 {
            condition_id: ConditionId(CONDITION.to_owned()),
            clob_market_id: ClobMarketId("3278040".to_owned()),
            slug: SLUG.to_owned(),
            outcomes: vec![
                ReferenceOutcomeSpecV1 {
                    token_id: TokenId(TOKEN_UP.to_owned()),
                    outcome: "Up".to_owned(),
                },
                ReferenceOutcomeSpecV1 {
                    token_id: TokenId(TOKEN_DOWN.to_owned()),
                    outcome: "Down".to_owned(),
                },
            ],
        }
    }

    pub(super) fn fixed_scenario() -> Result<Arc<SealedPlatformScenarioV1>, String> {
        let bytes = fixed_scenario_bytes()?;
        SealedPlatformScenarioV1::decode_and_seal(&bytes)
            .map(Arc::new)
            .map_err(|error| format!("密封固定场景失败：{error}"))
    }

    #[cfg(feature = "measurement")]
    pub(super) fn component_fixed_scenario() -> Result<Arc<SealedPlatformScenarioV1>, String> {
        let bytes = component_fixed_scenario_bytes()?;
        SealedPlatformScenarioV1::decode_and_seal(&bytes)
            .map(Arc::new)
            .map_err(|error| format!("密封固定 component 场景失败：{error}"))
    }

    fn fixed_scenario_bytes() -> Result<Vec<u8>, String> {
        fixed_scenario_bytes_with_shift(0)
    }

    #[cfg(feature = "measurement")]
    fn component_fixed_scenario_bytes() -> Result<Vec<u8>, String> {
        fixed_scenario_bytes_with_shift(COMPONENT_TIME_SHIFT_MS)
    }

    fn fixed_scenario_bytes_with_shift(time_shift_ms: i64) -> Result<Vec<u8>, String> {
        let first = market(
            CONDITION,
            "3278040",
            TOKEN_UP,
            TOKEN_DOWN,
            MARKET_ONE_START + time_shift_ms,
            MARKET_END + time_shift_ms,
        )?;
        let second = market(
            &format!("0x{}", "e".repeat(64)),
            "3278041",
            "10000000000000000000000000000000000000000000000000000000000000001",
            "10000000000000000000000000000000000000000000000000000000000000002",
            MARKET_TWO_START + time_shift_ms,
            MARKET_END + time_shift_ms,
        )?;
        let scenario = PlatformScenarioV1::try_new(vec![first, second])
            .map_err(|error| format!("构造固定双市场场景失败：{error}"))?;
        let artifact = SimulationScenarioArtifactV1 {
            schema_version: PLATFORM_SCENARIO_SCHEMA_V1.to_owned(),
            scenario_sha256: scenario.scenario_sha256().clone(),
            market_set_sha256: scenario.market_set_sha256().clone(),
            markets: scenario.market_set().markets().to_vec(),
        };
        serde_json::to_vec(&artifact).map_err(|error| format!("编码固定场景失败：{error}"))
    }

    fn market(
        condition: &str,
        clob: &str,
        up: &str,
        down: &str,
        start: i64,
        end: i64,
    ) -> Result<PlatformMarketV1, String> {
        PlatformMarketV1::try_new(
            MarketIdentityV2 {
                condition_id: ConditionId(condition.to_owned()),
                clob_market_id: ClobMarketId(clob.to_owned()),
                window: MarketWindowV2 {
                    start_at_utc_ms: start,
                    end_at_utc_ms: end,
                },
            },
            [
                AssetIdentityV1 {
                    token_id: TokenId(up.to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 0,
                        label: "Up".to_owned(),
                    },
                },
                AssetIdentityV1 {
                    token_id: TokenId(down.to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 1,
                        label: "Down".to_owned(),
                    },
                },
            ],
        )
        .map_err(|error| format!("构造固定市场失败：{error}"))
    }

    fn successful_finality() -> Result<Arc<TradeFinalityPolicyV2>, String> {
        let bytes = successful_finality_bytes()?;
        TradeFinalityPolicyV2::decode_and_seal(&bytes)
            .map(Arc::new)
            .map_err(|error| format!("密封成功最终性策略失败：{error}"))
    }

    fn successful_finality_bytes() -> Result<Vec<u8>, String> {
        serde_json::to_vec(&json!({
            "schema_version": "trade_finality_policy_v2",
            "normal_path": { "steps": [
                {"state":"MATCHED","offset_ms":0},
                {"state":"MINED","offset_ms":10},
                {"state":"CONFIRMED","offset_ms":20}
            ], "economic_application_ordinal": 1 },
            "failure_overrides": []
        }))
        .map_err(|error| format!("编码成功最终性策略失败：{error}"))
    }

    fn proc_snapshot() -> Result<ProcSnapshot, String> {
        let stat = fs::read_to_string("/proc/self/stat")
            .map_err(|error| format!("读取 /proc/self/stat 失败：{error}"))?;
        let (_, fields) = stat
            .rsplit_once(") ")
            .ok_or_else(|| "解析 /proc/self/stat 失败".to_owned())?;
        let fields = fields.split_whitespace().collect::<Vec<_>>();
        let parse_stat = |index: usize| -> Result<u64, String> {
            fields
                .get(index)
                .ok_or_else(|| format!("/proc/self/stat 缺少字段 {index}"))?
                .parse()
                .map_err(|error| format!("解析 /proc/self/stat 字段 {index} 失败：{error}"))
        };
        let status = fs::read_to_string("/proc/self/status")
            .map_err(|error| format!("读取 /proc/self/status 失败：{error}"))?;
        let status_bytes = |name: &str| -> Result<u64, String> {
            status
                .lines()
                .find_map(|line| line.strip_prefix(name))
                .and_then(|value| value.split_whitespace().next())
                .ok_or_else(|| format!("/proc/self/status 缺少 {name}"))?
                .parse::<u64>()
                .map(|kib| kib.saturating_mul(1024))
                .map_err(|error| format!("解析 /proc/self/status {name} 失败：{error}"))
        };
        let io = fs::read_to_string("/proc/self/io")
            .map_err(|error| format!("读取 /proc/self/io 失败：{error}"))?;
        let io_value = |name: &str| -> Result<u64, String> {
            io.lines()
                .find_map(|line| line.strip_prefix(name))
                .ok_or_else(|| format!("/proc/self/io 缺少 {name}"))?
                .trim()
                .parse()
                .map_err(|error| format!("解析 /proc/self/io {name} 失败：{error}"))
        };
        Ok(ProcSnapshot {
            cpu_ticks: parse_stat(11)?.saturating_add(parse_stat(12)?),
            rss_bytes: status_bytes("VmRSS:")?,
            rss_high_water_bytes: status_bytes("VmHWM:")?,
            io_read_bytes: io_value("read_bytes:")?,
            io_write_bytes: io_value("write_bytes:")?,
        })
    }

    fn directory_bytes(path: &Path) -> Result<u64, String> {
        let mut total = 0_u64;
        let mut pending = vec![path.to_path_buf()];
        while let Some(path) = pending.pop() {
            for entry in fs::read_dir(&path)
                .map_err(|error| format!("遍历输出目录 {} 失败：{error}", path.display()))?
            {
                let entry = entry.map_err(|error| format!("读取输出目录项失败：{error}"))?;
                let metadata = entry
                    .metadata()
                    .map_err(|error| format!("读取输出目录项 metadata 失败：{error}"))?;
                if metadata.is_dir() {
                    pending.push(entry.path());
                } else if metadata.is_file() {
                    total = total.saturating_add(metadata.len());
                }
            }
        }
        Ok(total)
    }

    fn free_disk_bytes(path: &Path) -> Result<u64, String> {
        let output = Command::new("df")
            .arg("-Pk")
            .arg(path)
            .output()
            .map_err(|error| format!("查询可用磁盘失败：{error}"))?;
        if !output.status.success() {
            return Err("查询可用磁盘失败：df 非零退出".to_owned());
        }
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .skip(1)
            .filter_map(|line| line.split_whitespace().nth(3)?.parse::<u64>().ok())
            .next()
            .map(|kib| kib.saturating_mul(1024))
            .ok_or_else(|| "解析 df 可用磁盘失败".to_owned())
    }

    fn check_safety(
        run_started: Instant,
        trial_started: Instant,
        output_dir: &Path,
    ) -> Result<(), String> {
        let proc = proc_snapshot()?;
        let usage = SafetyUsage {
            wall_ns: elapsed_ns(run_started),
            trial_ns: elapsed_ns(trial_started),
            rss_bytes: proc.rss_bytes.max(proc.rss_high_water_bytes),
            output_bytes: directory_bytes(output_dir)?,
            free_disk_bytes: free_disk_bytes(output_dir)?,
        };
        if let Some(boundary) = safety_violation(SafetyLimits::fixed(), usage) {
            return Err(format!("安全停止边界触发：{boundary}; usage={usage:?}"));
        }
        Ok(())
    }

    fn cpu_model() -> Result<String, String> {
        let cpuinfo = fs::read_to_string("/proc/cpuinfo")
            .map_err(|error| format!("读取 /proc/cpuinfo 失败：{error}"))?;
        Ok(cpuinfo
            .lines()
            .find_map(|line| line.strip_prefix("model name\t: "))
            .unwrap_or("unknown")
            .to_owned())
    }

    fn command_output(program: &str, arguments: &[&str]) -> String {
        Command::new(program)
            .args(arguments)
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
            .unwrap_or_else(|| "unavailable".to_owned())
    }

    fn utc_now() -> Result<String, String> {
        let value = command_output("date", &["-u", "+%Y-%m-%dT%H:%M:%S.%3NZ"]);
        if value == "unavailable" {
            Err("读取 UTC 时间失败".to_owned())
        } else {
            Ok(value)
        }
    }

    fn unix_ms() -> Result<u128, String> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .map_err(|error| format!("读取 Unix 时间失败：{error}"))
    }

    fn elapsed_ns(started: Instant) -> u64 {
        u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    pub(super) fn sha256_file(path: &Path) -> Result<String, String> {
        let mut file =
            File::open(path).map_err(|error| format!("打开二进制计算 SHA-256 失败：{error}"))?;
        let mut digest = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let count = file
                .read(&mut buffer)
                .map_err(|error| format!("读取二进制计算 SHA-256 失败：{error}"))?;
            if count == 0 {
                break;
            }
            digest.update(&buffer[..count]);
        }
        Ok(format!("{:x}", digest.finalize()))
    }

    fn append_json_line(path: &Path, value: &impl Serialize) -> Result<(), String> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| format!("打开 samples.jsonl 失败：{error}"))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, value)
            .map_err(|error| format!("编码样本失败：{error}"))?;
        writer
            .write_all(b"\n")
            .and_then(|_| writer.flush())
            .map_err(|error| format!("写 samples.jsonl 失败：{error}"))
    }

    fn write_json(path: PathBuf, value: &impl Serialize) -> Result<(), String> {
        let file = File::create(&path)
            .map_err(|error| format!("创建 {} 失败：{error}", path.display()))?;
        serde_json::to_writer_pretty(BufWriter::new(file), value)
            .map_err(|error| format!("写 {} 失败：{error}", path.display()))
    }

    fn parse_args() -> Result<Args, String> {
        parse_args_from(std::env::args().collect())
    }

    pub(super) fn parse_args_from(argv: Vec<String>) -> Result<Args, String> {
        let mut output_dir = None;
        let mut git_commit = None;
        let mut source = None;
        let mut lock = None;
        let mut baseline = None;
        let mut rule_snapshot = None;
        let mut rule_snapshot_sha256 = None;
        let mut stage4_formal_audit = None;
        let mut stage4_formal_audit_sha256 = None;
        let mut retention_manifest = None;
        let mut retention_manifest_sha256 = None;
        let mut test_evidence = None;
        let mut test_evidence_sha256 = None;
        let mut gate_a_root = None;
        let mut stage5_component_manifest = None;
        let mut stage5_component_manifest_sha256 = None;
        let mut stage5_component_receipt_id = None;
        #[cfg(feature = "gate-b-flow")]
        let mut reference_condition_id = None;
        #[cfg(feature = "gate-b-flow")]
        let mut reference_clob_market_id = None;
        #[cfg(feature = "gate-b-flow")]
        let mut reference_market_slug = None;
        #[cfg(feature = "gate-b-flow")]
        let mut reference_up_token_id = None;
        #[cfg(feature = "gate-b-flow")]
        let mut reference_up_outcome = None;
        #[cfg(feature = "gate-b-flow")]
        let mut reference_down_token_id = None;
        #[cfg(feature = "gate-b-flow")]
        let mut reference_down_outcome = None;
        #[cfg(feature = "gate-b-flow")]
        let mut gate_b_run_id = None;
        #[cfg(feature = "gate-b-flow")]
        let mut initial_available_pusd = None;
        #[cfg(feature = "gate-b-flow")]
        let mut per_market_all_in_commitment_limit_pusd = None;
        let mut mode = RunMode::GoldenMatrix;
        let mut index = 1;
        while index < argv.len() {
            let flag = &argv[index];
            if flag == "--targeted-retention-24h" {
                if mode != RunMode::GoldenMatrix {
                    return Err("固定运行 mode 不得重复或组合".to_owned());
                }
                mode = RunMode::TargetedRetention24h;
                index += 1;
                continue;
            }
            if flag == "--component-conformance" {
                if mode != RunMode::GoldenMatrix {
                    return Err("固定运行 mode 不得重复或组合".to_owned());
                }
                mode = RunMode::ComponentConformance;
                index += 1;
                continue;
            }
            if flag == "--gate-a-real-vertical" {
                if mode != RunMode::GoldenMatrix {
                    return Err("固定运行 mode 不得重复或组合".to_owned());
                }
                mode = RunMode::GateARealVertical;
                index += 1;
                continue;
            }
            #[cfg(feature = "gate-b-flow")]
            if flag == "--gate-b-flow" {
                if mode != RunMode::GoldenMatrix {
                    return Err("固定运行 mode 不得重复或组合".to_owned());
                }
                mode = RunMode::GateBFlow;
                index += 1;
                continue;
            }
            let value = argv
                .get(index + 1)
                .ok_or_else(|| format!("参数 {flag} 缺少值"))?;
            match flag.as_str() {
                "--output-dir" => output_dir = Some(PathBuf::from(value)),
                "--git-commit" => git_commit = Some(value.clone()),
                "--source-identity-sha256" => source = Some(value.clone()),
                "--cargo-lock-sha256" => lock = Some(value.clone()),
                "--baseline-summary" => baseline = Some(PathBuf::from(value)),
                "--rule-snapshot" => rule_snapshot = Some(PathBuf::from(value)),
                "--rule-snapshot-sha256" => rule_snapshot_sha256 = Some(value.clone()),
                "--stage4-formal-audit" => stage4_formal_audit = Some(PathBuf::from(value)),
                "--stage4-formal-audit-sha256" => {
                    stage4_formal_audit_sha256 = Some(value.clone());
                }
                "--retention-manifest" => retention_manifest = Some(PathBuf::from(value)),
                "--retention-manifest-sha256" => {
                    retention_manifest_sha256 = Some(value.clone());
                }
                "--test-evidence" => test_evidence = Some(PathBuf::from(value)),
                "--test-evidence-sha256" => test_evidence_sha256 = Some(value.clone()),
                "--gate-a-root" => {
                    if gate_a_root.replace(PathBuf::from(value)).is_some() {
                        return Err("--gate-a-root 不得重复".to_owned());
                    }
                }
                "--stage5-component-manifest" => {
                    stage5_component_manifest = Some(PathBuf::from(value));
                }
                "--stage5-component-manifest-sha256" => {
                    stage5_component_manifest_sha256 = Some(value.clone());
                }
                "--stage5-component-receipt-id" => {
                    stage5_component_receipt_id = Some(value.clone());
                }
                #[cfg(feature = "gate-b-flow")]
                "--reference-condition-id" => reference_condition_id = Some(value.clone()),
                #[cfg(feature = "gate-b-flow")]
                "--reference-clob-market-id" => reference_clob_market_id = Some(value.clone()),
                #[cfg(feature = "gate-b-flow")]
                "--reference-market-slug" => reference_market_slug = Some(value.clone()),
                #[cfg(feature = "gate-b-flow")]
                "--reference-up-token-id" => reference_up_token_id = Some(value.clone()),
                #[cfg(feature = "gate-b-flow")]
                "--reference-up-outcome" => reference_up_outcome = Some(value.clone()),
                #[cfg(feature = "gate-b-flow")]
                "--reference-down-token-id" => reference_down_token_id = Some(value.clone()),
                #[cfg(feature = "gate-b-flow")]
                "--reference-down-outcome" => reference_down_outcome = Some(value.clone()),
                #[cfg(feature = "gate-b-flow")]
                "--run-id" => gate_b_run_id = Some(value.clone()),
                #[cfg(feature = "gate-b-flow")]
                "--initial-available-pusd" => initial_available_pusd = Some(value.clone()),
                #[cfg(feature = "gate-b-flow")]
                "--per-market-all-in-commitment-limit-pusd" => {
                    per_market_all_in_commitment_limit_pusd = Some(value.clone());
                }
                _ => return Err(format!("未知参数：{flag}")),
            }
            index += 2;
        }
        let git_commit = git_commit.ok_or_else(|| "缺少 --git-commit".to_owned())?;
        let source = source.ok_or_else(|| "缺少 --source-identity-sha256".to_owned())?;
        let lock = lock.ok_or_else(|| "缺少 --cargo-lock-sha256".to_owned())?;
        if git_commit.len() != 40 || source.len() != 64 || lock.len() != 64 {
            return Err("git commit 必须为 40 位，SHA-256 必须为 64 位".to_owned());
        }
        if mode != RunMode::GoldenMatrix && baseline.is_some() {
            return Err("固定 mode 不接受 --baseline-summary".to_owned());
        }
        if mode != RunMode::GoldenMatrix && !cfg!(feature = "measurement") {
            return Err("固定 mode 只允许 measurement feature 构建；未装配探针".to_owned());
        }
        if mode == RunMode::GateARealVertical && !cfg!(feature = "gate-a-measurement") {
            return Err("--gate-a-real-vertical 只允许 gate-a-measurement feature 构建".to_owned());
        }
        #[cfg(not(feature = "measurement"))]
        if rule_snapshot.is_some()
            || rule_snapshot_sha256.is_some()
            || stage4_formal_audit.is_some()
            || stage4_formal_audit_sha256.is_some()
            || retention_manifest.is_some()
            || retention_manifest_sha256.is_some()
            || test_evidence.is_some()
            || test_evidence_sha256.is_some()
        {
            return Err("component evidence 参数只允许 --component-conformance".to_owned());
        }
        #[cfg(feature = "measurement")]
        let component = if mode == RunMode::ComponentConformance {
            let values = [
                rule_snapshot_sha256.as_deref(),
                stage4_formal_audit_sha256.as_deref(),
                retention_manifest_sha256.as_deref(),
                test_evidence_sha256.as_deref(),
            ];
            if values
                .iter()
                .any(|value| value.is_none_or(|value| value.len() != 64))
            {
                return Err("component evidence SHA-256 必须完整且为 64 位".to_owned());
            }
            Some(ComponentEvidenceArgs {
                rule_snapshot: rule_snapshot.ok_or_else(|| "缺少 --rule-snapshot".to_owned())?,
                rule_snapshot_sha256: Sha256Hex(rule_snapshot_sha256.expect("已检查")),
                stage4_formal_audit: stage4_formal_audit
                    .ok_or_else(|| "缺少 --stage4-formal-audit".to_owned())?,
                stage4_formal_audit_sha256: Sha256Hex(stage4_formal_audit_sha256.expect("已检查")),
                retention_manifest: retention_manifest
                    .ok_or_else(|| "缺少 --retention-manifest".to_owned())?,
                retention_manifest_sha256: Sha256Hex(retention_manifest_sha256.expect("已检查")),
                test_evidence: test_evidence.ok_or_else(|| "缺少 --test-evidence".to_owned())?,
                test_evidence_sha256: Sha256Hex(test_evidence_sha256.expect("已检查")),
            })
        } else {
            if rule_snapshot.is_some()
                || rule_snapshot_sha256.is_some()
                || stage4_formal_audit.is_some()
                || stage4_formal_audit_sha256.is_some()
                || retention_manifest.is_some()
                || retention_manifest_sha256.is_some()
                || test_evidence.is_some()
                || test_evidence_sha256.is_some()
            {
                return Err("component evidence 参数只允许 --component-conformance".to_owned());
            }
            None
        };
        #[cfg(not(feature = "gate-a-measurement"))]
        if gate_a_root.is_some()
            || stage5_component_manifest.is_some()
            || stage5_component_manifest_sha256.is_some()
            || stage5_component_receipt_id.is_some()
        {
            return Err("Gate A evidence 参数只允许 --gate-a-real-vertical".to_owned());
        }
        #[cfg(feature = "gate-a-measurement")]
        let gate_a = if mode == RunMode::GateARealVertical {
            let manifest_sha = stage5_component_manifest_sha256
                .clone()
                .ok_or_else(|| "缺少 --stage5-component-manifest-sha256".to_owned())?;
            let receipt_id = stage5_component_receipt_id
                .clone()
                .ok_or_else(|| "缺少 --stage5-component-receipt-id".to_owned())?;
            if manifest_sha.len() != 64 || receipt_id.len() != 64 {
                return Err("阶段 5 manifest SHA-256 与 receipt ID 必须为 64 位".to_owned());
            }
            Some(GateARealVerticalArgs {
                gate_a_root: gate_a_root
                    .clone()
                    .ok_or_else(|| "缺少 --gate-a-root".to_owned())?,
                stage5_component_manifest: stage5_component_manifest
                    .clone()
                    .ok_or_else(|| "缺少 --stage5-component-manifest".to_owned())?,
                stage5_component_manifest_sha256: Sha256Hex(manifest_sha),
                stage5_component_receipt_id: Sha256Hex(receipt_id),
            })
        } else {
            #[cfg(feature = "gate-b-flow")]
            let used_by_gate_b = mode == RunMode::GateBFlow;
            #[cfg(not(feature = "gate-b-flow"))]
            let used_by_gate_b = false;
            if stage5_component_manifest.is_some()
                || stage5_component_manifest_sha256.is_some()
                || stage5_component_receipt_id.is_some()
                || (!used_by_gate_b && gate_a_root.is_some())
            {
                return Err("Gate A evidence 参数只允许 --gate-a-real-vertical".to_owned());
            }
            None
        };
        #[cfg(feature = "gate-b-flow")]
        let gate_b = if mode == RunMode::GateBFlow {
            Some(gate_b::GateBArgs {
                gate_a_root: gate_a_root.ok_or_else(|| "缺少 --gate-a-root".to_owned())?,
                reference_market: ReferenceMarketSpecV1 {
                    condition_id: ConditionId(
                        reference_condition_id
                            .ok_or_else(|| "缺少 --reference-condition-id".to_owned())?,
                    ),
                    clob_market_id: ClobMarketId(
                        reference_clob_market_id
                            .ok_or_else(|| "缺少 --reference-clob-market-id".to_owned())?,
                    ),
                    slug: reference_market_slug
                        .ok_or_else(|| "缺少 --reference-market-slug".to_owned())?,
                    outcomes: vec![
                        ReferenceOutcomeSpecV1 {
                            token_id: TokenId(
                                reference_up_token_id
                                    .ok_or_else(|| "缺少 --reference-up-token-id".to_owned())?,
                            ),
                            outcome: reference_up_outcome
                                .ok_or_else(|| "缺少 --reference-up-outcome".to_owned())?,
                        },
                        ReferenceOutcomeSpecV1 {
                            token_id: TokenId(
                                reference_down_token_id
                                    .ok_or_else(|| "缺少 --reference-down-token-id".to_owned())?,
                            ),
                            outcome: reference_down_outcome
                                .ok_or_else(|| "缺少 --reference-down-outcome".to_owned())?,
                        },
                    ],
                },
                run_id: gate_b_run_id.ok_or_else(|| "缺少 --run-id".to_owned())?,
                initial_available_pusd: ExactDecimal(
                    initial_available_pusd
                        .ok_or_else(|| "缺少 --initial-available-pusd".to_owned())?,
                ),
                per_market_all_in_commitment_limit_pusd: ExactDecimal(
                    per_market_all_in_commitment_limit_pusd.ok_or_else(|| {
                        "缺少 --per-market-all-in-commitment-limit-pusd".to_owned()
                    })?,
                ),
            })
        } else {
            if reference_condition_id.is_some()
                || reference_clob_market_id.is_some()
                || reference_market_slug.is_some()
                || reference_up_token_id.is_some()
                || reference_up_outcome.is_some()
                || reference_down_token_id.is_some()
                || reference_down_outcome.is_some()
                || gate_b_run_id.is_some()
                || initial_available_pusd.is_some()
                || per_market_all_in_commitment_limit_pusd.is_some()
            {
                return Err("Gate B 参数只允许 --gate-b-flow".to_owned());
            }
            None
        };
        Ok(Args {
            output_dir: output_dir.ok_or_else(|| "缺少 --output-dir".to_owned())?,
            identity: ImplementationIdentityV1 {
                git_commit,
                source_identity_sha256: Sha256Hex(source),
                cargo_lock_sha256: Sha256Hex(lock),
            },
            baseline_summary: baseline,
            #[cfg(feature = "measurement")]
            component,
            #[cfg(feature = "gate-a-measurement")]
            gate_a,
            #[cfg(feature = "gate-b-flow")]
            gate_b,
            mode,
            argv,
        })
    }
}

fn main() {
    if let Err(error) = enabled::main() {
        eprintln!("sim_measurement 失败：{error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "measurement")]
    use std::path::Path;

    #[cfg(feature = "measurement")]
    use sha2::Digest;

    use super::*;

    #[cfg(feature = "measurement")]
    #[test]
    fn targeted_parser_fixes_identity_and_default_mode_is_unchanged() {
        let common = [
            "sim_measurement",
            "--output-dir",
            "/tmp/out",
            "--git-commit",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--source-identity-sha256",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "--cargo-lock-sha256",
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        ];
        let default = enabled::parse_args_from(common.map(str::to_owned).to_vec())
            .expect("既有默认参数必须继续解析");
        assert_eq!(default.mode, enabled::RunMode::GoldenMatrix);
        assert_eq!(enabled::runner_identity(default.mode), RUNNER_IDENTITY);

        let mut targeted = common.map(str::to_owned).to_vec();
        targeted.push("--targeted-retention-24h".to_owned());
        let targeted = enabled::parse_args_from(targeted).expect("固定 targeted flag 必须解析");
        assert_eq!(targeted.mode, enabled::RunMode::TargetedRetention24h);
        assert_eq!(
            enabled::runner_identity(targeted.mode),
            "sim-task-4-5-2-retention-24h-v1"
        );
    }

    #[cfg(feature = "measurement")]
    #[test]
    fn component_parser_selects_the_only_fixed_component_mode() {
        let args = vec![
            "sim_measurement",
            "--output-dir",
            "/tmp/component-final",
            "--git-commit",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--source-identity-sha256",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "--cargo-lock-sha256",
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "--component-conformance",
            "--rule-snapshot",
            "/tmp/rules-v3.json",
            "--rule-snapshot-sha256",
            "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            "--stage4-formal-audit",
            "/tmp/stage4-audit.json",
            "--stage4-formal-audit-sha256",
            "22ae1c4a36803c2367fec81472e3ab5b7ecf6c65b78159866f41ed92aed1e5b7",
            "--retention-manifest",
            "/tmp/retention.json",
            "--retention-manifest-sha256",
            "d3d5b7d615dfcc6b76e393f384739c72fee52fea71707f01d7a42006b58bef16",
            "--test-evidence",
            "/tmp/tests.json",
            "--test-evidence-sha256",
            "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        let parsed = enabled::parse_args_from(args).expect("固定 component 参数必须解析");
        assert_eq!(parsed.mode, enabled::RunMode::ComponentConformance);
        assert_eq!(
            enabled::runner_identity(parsed.mode),
            "sim-stage-5-component-conformance-v1"
        );
    }

    #[cfg(feature = "gate-a-measurement")]
    #[test]
    fn gate_a_parser_selects_the_narrow_real_vertical_mode() {
        let args = vec![
            "sim_measurement",
            "--output-dir",
            "/tmp/joint-bundle",
            "--git-commit",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--source-identity-sha256",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "--cargo-lock-sha256",
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "--gate-a-real-vertical",
            "--gate-a-root",
            "/tmp/gate-a-final",
            "--stage5-component-manifest",
            "/tmp/simulation-run-manifest-v2.json",
            "--stage5-component-manifest-sha256",
            "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            "--stage5-component-receipt-id",
            "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        let parsed = enabled::parse_args_from(args).expect("固定 Gate A 参数必须解析");
        assert_eq!(parsed.mode, enabled::RunMode::GateARealVertical);
        assert_eq!(
            enabled::runner_identity(parsed.mode),
            "sim-stage-6-gate-a-real-vertical-v1"
        );
    }

    #[cfg(feature = "gate-a-measurement")]
    #[test]
    fn gate_a_unique_count_is_the_union_with_visibility_only_events() {
        let mut unique_event_ids = std::collections::HashSet::new();
        let platform = ["shared-a", "shared-b"];
        let visibility = ["shared-a", "shared-b", "visibility-only"];
        enabled::observe_unique_gate_a_event_ids(&mut unique_event_ids, platform);
        enabled::observe_unique_gate_a_event_ids(&mut unique_event_ids, visibility);

        assert_ne!(platform.len(), visibility.len());
        assert_eq!(unique_event_ids.len(), 3);
    }

    #[cfg(feature = "gate-a-measurement")]
    #[test]
    fn joint_manifest_content_identity_and_noreplace_publish_are_atomic() {
        let directory = tempfile::tempdir().expect("联合 manifest 临时目录");
        let candidate = directory.path().join(".joint-bundle.candidate");
        let target = directory.path().join("joint-bundle");
        std::fs::create_dir(&candidate).expect("创建联合 candidate");
        for name in [
            "platform-rule-snapshot-v3.json",
            "measurement-samples.jsonl",
            "representative-market-report.json",
            "resource-baseline.json",
        ] {
            std::fs::write(candidate.join(name), format!("{name}\n")).expect("写联合代表成员");
        }
        let inventory =
            enabled::artifact_inventory(&candidate).expect("联合 inventory 必须来自四个真实成员");
        assert_eq!(inventory.len(), 4);
        let content = serde_json::json!({
            "schema_version":"joint_real_vertical_manifest_v1",
            "local_inventory":inventory,
            "local_inventory_excludes_manifest_itself":true
        });
        let content_bytes = serde_json::to_vec(&content).expect("编码联合 content");
        let expected_receipt = format!("{:x}", sha2::Sha256::digest(content_bytes));
        let envelope = enabled::joint_manifest_envelope(content).expect("构造联合 envelope");
        assert_eq!(envelope["joint_receipt_id"], expected_receipt);
        std::fs::write(
            candidate.join("joint-real-vertical-manifest-v1.json"),
            serde_json::to_vec_pretty(&envelope).expect("编码联合 envelope"),
        )
        .expect("写联合 manifest");
        enabled::sync_tree(&candidate).expect("同步联合 candidate");
        enabled::publish_candidate(&candidate, &target).expect("原子发布联合 candidate");
        let mut names = std::fs::read_dir(&target)
            .expect("读取联合 bundle")
            .map(|entry| {
                entry
                    .expect("读取联合成员")
                    .file_name()
                    .into_string()
                    .expect("联合成员名 UTF-8")
            })
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(
            names,
            vec![
                "joint-real-vertical-manifest-v1.json",
                "measurement-samples.jsonl",
                "platform-rule-snapshot-v3.json",
                "representative-market-report.json",
                "resource-baseline.json",
            ]
        );
        let second = directory.path().join(".joint-bundle.second");
        std::fs::create_dir(&second).expect("创建第二联合 candidate");
        assert!(enabled::publish_candidate(&second, &target).is_err());
        assert!(second.exists());
    }

    #[cfg(not(feature = "measurement"))]
    #[test]
    fn feature_off_rejects_component_flag_at_cli_boundary() {
        let args = vec![
            "sim_measurement",
            "--output-dir",
            "/tmp/out",
            "--git-commit",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--source-identity-sha256",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "--cargo-lock-sha256",
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "--component-conformance",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        let error = enabled::parse_args_from(args).expect_err("feature-off 必须拒绝 component");
        assert!(error.contains("未装配探针"));
    }

    #[cfg(feature = "measurement")]
    #[test]
    fn component_comparison_is_equal_and_reports_the_first_changed_batch() {
        let baseline = enabled::ComponentAttemptEvidence {
            accepted_input_count: 9,
            projection_batches: vec![serde_json::json!({"value":1})],
            log_sha256: "a".repeat(64),
            log_bytes: 100,
            cold_candidate_run_id: COMPONENT_RUNNER_IDENTITY.to_owned(),
        };
        let pass = enabled::component_comparison(&baseline, &baseline, &baseline);
        assert_eq!(pass["status"], "PASS");
        assert!(pass["first_divergence"].is_null());
        let mut changed = baseline.clone();
        changed.projection_batches[0] = serde_json::json!({"value":2});
        let fail = enabled::component_comparison(&baseline, &changed, &baseline);
        assert_eq!(fail["status"], "FAIL");
        assert_eq!(fail["first_divergence"]["run"], "clean-rerun");
        assert_eq!(fail["first_divergence"]["group"], "semantic_projection");
        assert_eq!(fail["first_divergence"]["input_sequence"], 0);
        assert!(
            fail["first_divergence"]["path"]
                .as_str()
                .expect("divergence path")
                .contains("projection_batches/0/value")
        );

        let mut changed_count = baseline.clone();
        changed_count.accepted_input_count += 1;
        let fail = enabled::component_comparison(&baseline, &changed_count, &baseline);
        assert_eq!(fail["first_divergence"]["group"], "accepted_inputs");
        assert!(fail["first_divergence"]["input_sequence"].is_null());
    }

    #[cfg(feature = "measurement")]
    #[test]
    fn component_publish_is_noreplace_and_candidate_never_masquerades_as_final() {
        let directory = tempfile::tempdir().expect("component publish tempdir");
        let candidate = directory.path().join(".bundle.candidate");
        let final_path = directory.path().join("bundle");
        std::fs::create_dir(&candidate).expect("create component candidate");
        std::fs::write(candidate.join("proof.json"), b"{}\n").expect("write component proof");
        enabled::sync_tree(&candidate).expect("sync component candidate");
        assert!(!final_path.exists());
        enabled::publish_candidate(&candidate, &final_path).expect("publish component candidate");
        assert!(final_path.join("proof.json").is_file());
        assert!(!candidate.exists());
        let second = directory.path().join(".bundle.candidate");
        std::fs::create_dir(&second).expect("create second candidate");
        assert!(enabled::publish_candidate(&second, &final_path).is_err());
        assert!(second.exists());
        assert_eq!(
            enabled::failed_output_path(&directory.path().join("bundle.v2")),
            directory.path().join("bundle.v2.failed")
        );
    }

    #[cfg(feature = "measurement")]
    #[test]
    fn component_golden_file_round_trips_through_the_v3_decoder_and_has_real_sha() {
        let directory = tempfile::tempdir().expect("component golden tempdir");
        let scenario = enabled::fixed_scenario().expect("fixed component scenario");
        let buckets = enabled::golden_buckets(&scenario).expect("fixed component buckets");
        let path = directory.path().join("component-golden-events-v3.jsonl");
        let restored = enabled::write_and_read_component_events(&path, &buckets)
            .expect("round-trip component golden events");
        assert_eq!(restored, buckets);
        let sha = enabled::sha256_file(&path).expect("component golden file SHA");
        assert_ne!(sha, "7".repeat(64));
        assert_eq!(sha.len(), 64);
    }

    #[cfg(feature = "measurement")]
    #[test]
    fn component_timeline_keeps_rules_events_requests_and_scenario_causally_aligned() {
        let default_scenario = enabled::fixed_scenario().expect("fixed default scenario");
        let component_scenario =
            enabled::component_fixed_scenario().expect("fixed component scenario");
        let default_buckets =
            enabled::golden_buckets(&default_scenario).expect("fixed default buckets");
        let component_buckets = enabled::golden_buckets_with_shift(
            &component_scenario,
            enabled::COMPONENT_TIME_SHIFT_MS,
        )
        .expect("fixed component buckets");

        let default_market_start = default_scenario.scenario().market_set().markets()[0]
            .market()
            .window
            .start_at_utc_ms;
        let component_market_start = component_scenario.scenario().market_set().markets()[0]
            .market()
            .window
            .start_at_utc_ms;
        assert_eq!(
            component_market_start - default_market_start,
            enabled::COMPONENT_TIME_SHIFT_MS
        );

        let earliest_component_source = component_buckets
            .iter()
            .flat_map(|(_, events)| events.iter())
            .filter_map(|event| event.time.source_event_at_utc_ms)
            .min()
            .expect("component source time");
        let earliest_component_received = component_buckets
            .iter()
            .flat_map(|(_, events)| events.iter())
            .map(|event| event.time.received_at_utc_ms)
            .min()
            .expect("component received time");
        let earliest_default_source = default_buckets
            .iter()
            .flat_map(|(_, events)| events.iter())
            .filter_map(|event| event.time.source_event_at_utc_ms)
            .min()
            .expect("default source time");

        assert_eq!(
            enabled::COMPONENT_CAUSAL_START_UTC_MS,
            earliest_component_source
        );
        assert!(earliest_component_received >= enabled::COMPONENT_CAUSAL_START_UTC_MS);
        assert_eq!(
            enabled::COMPONENT_REQUEST_CUTOFF_BASE - earliest_component_source,
            199_990
        );
        assert_eq!(
            earliest_component_source - earliest_default_source,
            enabled::COMPONENT_TIME_SHIFT_MS
        );
        assert!(earliest_component_source > 1_787_398_441_633);
    }

    #[cfg(feature = "measurement")]
    #[test]
    fn component_inventory_member_derives_length_and_sha_from_one_byte_snapshot() {
        let bytes = b"same component inventory byte snapshot";
        let member = enabled::component_inventory_member(Path::new("proof.bin"), bytes);
        assert_eq!(member["path"], "proof.bin");
        assert_eq!(member["bytes"], bytes.len());
        assert_eq!(
            member["sha256"],
            format!("{:x}", sha2::Sha256::digest(bytes))
        );
    }

    #[cfg(not(feature = "measurement"))]
    #[test]
    fn feature_off_rejects_targeted_flag_at_cli_boundary() {
        let args = vec![
            "sim_measurement",
            "--output-dir",
            "/tmp/out",
            "--git-commit",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--source-identity-sha256",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "--cargo-lock-sha256",
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "--targeted-retention-24h",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        let error = enabled::parse_args_from(args).expect_err("feature-off 必须拒绝 targeted");
        assert!(error.contains("未装配探针"));
    }

    #[test]
    fn targeted_summary_uses_only_three_measured_trials() {
        let samples = [
            enabled::TargetedSummaryInput::new(true, 999, 999, 999, true),
            enabled::TargetedSummaryInput::new(false, 30, 3, 300, true),
            enabled::TargetedSummaryInput::new(false, 10, 1, 100, true),
            enabled::TargetedSummaryInput::new(false, 20, 2, 200, true),
        ];
        let summary = enabled::summarize_targeted(&samples).expect("固定 targeted 汇总必须成功");
        assert_eq!(summary.measured_trial_count, 3);
        assert_eq!(summary.warmup_trial_count, 1);
        assert_eq!(summary.retention_elapsed_ns.min, 10);
        assert_eq!(summary.retention_elapsed_ns.median, 20);
        assert_eq!(summary.retention_elapsed_ns.max, 30);
        assert_eq!(summary.released_business_records.median, 2);
        assert_eq!(summary.released_business_bytes.median, 200);
        assert_eq!(summary.semantic_pass_count, 3);
    }

    #[cfg(feature = "measurement")]
    #[test]
    fn targeted_output_inventory_has_four_schemas_and_hashes_only_the_other_three_files() {
        let directory = tempfile::tempdir().expect("创建 targeted 产物代表目录");
        let samples_path = directory.path().join("retention-samples.jsonl");
        let environment_path = directory.path().join("retention-environment.json");
        let summary_path = directory.path().join("retention-summary.json");
        std::fs::write(
            &samples_path,
            [true, false, false, false]
                .into_iter()
                .map(|warmup| {
                    serde_json::to_string(&serde_json::json!({
                        "schema_version": "sim_targeted_retention_sample_v1",
                        "warmup": warmup
                    }))
                    .expect("编码 targeted sample 代表")
                })
                .collect::<Vec<_>>()
                .join("\n")
                + "\n",
        )
        .expect("写入 targeted sample 代表");
        std::fs::write(
            &environment_path,
            br#"{"schema_version":"sim_targeted_retention_environment_v1"}"#,
        )
        .expect("写入 targeted environment 代表");
        std::fs::write(
            &summary_path,
            br#"{"schema_version":"sim_targeted_retention_summary_v1","measured_trial_count":3,"warmup_trial_count":1}"#,
        )
        .expect("写入 targeted summary 代表");

        let artifacts = enabled::artifact_inventory(directory.path())
            .expect("读取 targeted 三项 manifest inventory");
        let manifest = enabled::targeted_manifest_value(
            &[
                "sim_measurement".to_owned(),
                "--targeted-retention-24h".to_owned(),
            ],
            "a".repeat(40),
            "b".repeat(64),
            "c".repeat(64),
            "d".repeat(64),
            "e".repeat(64),
            "public-sim-execution-two-market-256x8-v1".to_owned(),
            artifacts,
        );
        std::fs::write(
            directory.path().join("retention-manifest.json"),
            serde_json::to_vec(&manifest).expect("编码 targeted manifest 代表"),
        )
        .expect("写入 targeted manifest 代表");

        let mut names = std::fs::read_dir(directory.path())
            .expect("读取 targeted 产物目录")
            .map(|entry| {
                entry
                    .expect("读取 targeted 产物项")
                    .file_name()
                    .into_string()
                    .expect("targeted 产物名必须为 UTF-8")
            })
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(
            names,
            vec![
                "retention-environment.json",
                "retention-manifest.json",
                "retention-samples.jsonl",
                "retention-summary.json",
            ]
        );
        assert_eq!(
            manifest["schema_version"],
            "sim_targeted_retention_manifest_v1"
        );
        assert_eq!(manifest["binary_sha256"], "b".repeat(64));
        assert_eq!(manifest["source_identity_sha256"], "c".repeat(64));
        assert_eq!(manifest["cargo_lock_sha256"], "d".repeat(64));
        assert_eq!(manifest["rule_snapshot_sha256"], "e".repeat(64));
        assert_eq!(
            manifest["workload_identity"],
            "public-sim-execution-two-market-256x8-v1"
        );
        assert_eq!(manifest["inventory_excludes_manifest_itself"], true);
        let inventory = manifest["artifacts"]
            .as_array()
            .expect("manifest artifacts 必须为数组");
        assert_eq!(inventory.len(), 3);
        for entry in inventory {
            let path = entry["path"].as_str().expect("inventory path");
            assert_ne!(path, "retention-manifest.json");
            assert_eq!(
                entry["bytes"].as_u64().expect("inventory bytes"),
                std::fs::metadata(directory.path().join(path))
                    .expect("读取 inventory 文件")
                    .len()
            );
            assert_eq!(
                entry["sha256"].as_str().expect("inventory sha"),
                enabled::sha256_file(&directory.path().join(path)).expect("复核 inventory sha")
            );
        }
        let sample_lines = std::fs::read_to_string(samples_path)
            .expect("读取 targeted sample 代表")
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("解析 sample 行"))
            .collect::<Vec<_>>();
        assert_eq!(sample_lines.len(), 4);
        assert!(
            sample_lines
                .iter()
                .all(|sample| { sample["schema_version"] == "sim_targeted_retention_sample_v1" })
        );
        assert_eq!(
            sample_lines
                .iter()
                .filter(|sample| sample["warmup"] == true)
                .count(),
            1
        );
        let environment: serde_json::Value = serde_json::from_slice(
            &std::fs::read(environment_path).expect("读取 targeted environment 代表"),
        )
        .expect("解析 targeted environment 代表");
        assert_eq!(
            environment["schema_version"],
            "sim_targeted_retention_environment_v1"
        );
        let summary: serde_json::Value = serde_json::from_slice(
            &std::fs::read(summary_path).expect("读取 targeted summary 代表"),
        )
        .expect("解析 targeted summary 代表");
        assert_eq!(
            summary["schema_version"],
            "sim_targeted_retention_summary_v1"
        );
        assert_eq!(summary["measured_trial_count"], 3);
        assert_eq!(summary["warmup_trial_count"], 1);
    }

    #[test]
    fn nearest_rank_uses_ceil_p_times_n_minus_one() {
        let values = vec![1, 2, 3, 4, 5];
        assert_eq!(nearest_rank(&values, 0.50), 3);
        assert_eq!(nearest_rank(&values, 0.95), 5);
        assert_eq!(nearest_rank(&values, 0.99), 5);
    }

    #[test]
    fn distribution_summary_keeps_count_min_percentiles_and_max() {
        let summary = distribution_summary(vec![9, 1, 5, 3]);
        assert_eq!(summary.count, 4);
        assert_eq!(summary.min, 1);
        assert_eq!(summary.p50, 3);
        assert_eq!(summary.p95, 9);
        assert_eq!(summary.p99, 9);
        assert_eq!(summary.max, 9);
    }

    #[test]
    fn feature_pair_requires_off_on_and_identical_trial_keys() {
        let keys = vec![PairKey::new("workload", "clean_run", "four_roots", 1)];
        assert!(validate_feature_pair("measurement_off", &keys, "measurement_on", &keys).is_ok());
        assert!(validate_feature_pair("measurement_on", &keys, "measurement_on", &keys).is_err());
        assert!(validate_feature_pair("measurement_off", &keys, "measurement_off", &keys).is_err());
        assert!(validate_feature_pair("measurement_off", &keys, "measurement_on", &[]).is_err());
    }

    #[test]
    fn safety_budget_reports_every_stop_boundary() {
        let limits = SafetyLimits::fixed();
        for (expected, usage) in [
            (
                "wall_clock",
                SafetyUsage {
                    wall_ns: limits.wall_ns + 1,
                    ..SafetyUsage::safe()
                },
            ),
            (
                "trial_clock",
                SafetyUsage {
                    trial_ns: limits.trial_ns + 1,
                    ..SafetyUsage::safe()
                },
            ),
            (
                "rss",
                SafetyUsage {
                    rss_bytes: limits.rss_bytes + 1,
                    ..SafetyUsage::safe()
                },
            ),
            (
                "output",
                SafetyUsage {
                    output_bytes: limits.output_bytes + 1,
                    ..SafetyUsage::safe()
                },
            ),
            (
                "free_disk",
                SafetyUsage {
                    free_disk_bytes: limits.free_disk_bytes - 1,
                    ..SafetyUsage::safe()
                },
            ),
        ] {
            assert_eq!(safety_violation(limits, usage), Some(expected));
        }
        assert_eq!(safety_violation(limits, SafetyUsage::safe()), None);
    }

    #[cfg(feature = "gate-b-flow")]
    #[test]
    fn gate_b_parser_requires_one_explicit_root_and_complete_reference_market() {
        let arguments: Vec<String> = vec![
            "sim_measurement",
            "--gate-b-flow",
            "--output-dir",
            "/tmp/gate-b-output",
            "--git-commit",
            "0123456789012345678901234567890123456789",
            "--source-identity-sha256",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--cargo-lock-sha256",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "--gate-a-root",
            "/tmp/gate-a",
            "--reference-condition-id",
            "0x07760d950eee192fc99eecdf9adc18e7dedf6764ec9ecbbc4e5489ba7ae45819",
            "--reference-clob-market-id",
            "3278040",
            "--reference-market-slug",
            "btc-updown-5m-parser-test",
            "--reference-up-token-id",
            "111",
            "--reference-up-outcome",
            "Up",
            "--reference-down-token-id",
            "222",
            "--reference-down-outcome",
            "Down",
            "--run-id",
            "gate-b-user-run",
            "--initial-available-pusd",
            "2000",
            "--per-market-all-in-commitment-limit-pusd",
            "1000",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        let parsed = enabled::parse_args_from(arguments.clone()).unwrap();
        assert_eq!(parsed.mode, enabled::RunMode::GateBFlow);
        assert_eq!(
            enabled::runner_identity(parsed.mode),
            "sim-gate-b-explicit-flow-v1"
        );
        let (reference, run_id, initial_available, per_market_limit) =
            parsed.gate_b_configuration_for_test();
        assert_eq!(
            reference.condition_id.0,
            "0x07760d950eee192fc99eecdf9adc18e7dedf6764ec9ecbbc4e5489ba7ae45819"
        );
        assert_eq!(reference.clob_market_id.0, "3278040");
        assert_eq!(reference.slug, "btc-updown-5m-parser-test");
        assert_eq!(reference.outcomes[0].token_id.0, "111");
        assert_eq!(reference.outcomes[0].outcome, "Up");
        assert_eq!(reference.outcomes[1].token_id.0, "222");
        assert_eq!(reference.outcomes[1].outcome, "Down");
        assert_eq!(
            (run_id, initial_available, per_market_limit),
            ("gate-b-user-run", "2000", "1000")
        );

        for required in [
            "--reference-condition-id",
            "--reference-clob-market-id",
            "--reference-market-slug",
            "--reference-up-token-id",
            "--reference-up-outcome",
            "--reference-down-token-id",
            "--reference-down-outcome",
            "--run-id",
            "--initial-available-pusd",
            "--per-market-all-in-commitment-limit-pusd",
        ] {
            let mut missing = arguments.clone();
            let index = missing
                .iter()
                .position(|argument| argument == required)
                .expect("固定 Gate B 参数存在");
            missing.drain(index..=index + 1);
            assert_eq!(
                enabled::parse_args_from(missing).unwrap_err(),
                format!("缺少 {required}")
            );
        }

        let missing_root = vec![
            "sim_measurement",
            "--gate-b-flow",
            "--output-dir",
            "/tmp/gate-b-missing-root",
            "--git-commit",
            "0123456789012345678901234567890123456789",
            "--source-identity-sha256",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--cargo-lock-sha256",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "--reference-condition-id",
            "condition",
            "--reference-clob-market-id",
            "market",
            "--reference-market-slug",
            "slug",
            "--reference-up-token-id",
            "up-token",
            "--reference-up-outcome",
            "Up",
            "--reference-down-token-id",
            "down-token",
            "--reference-down-outcome",
            "Down",
            "--run-id",
            "missing-root-run",
            "--initial-available-pusd",
            "2000",
            "--per-market-all-in-commitment-limit-pusd",
            "1000",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        assert_eq!(
            enabled::parse_args_from(missing_root).unwrap_err(),
            "缺少 --gate-a-root"
        );

        let mut duplicate_root = vec![
            "sim_measurement",
            "--gate-b-flow",
            "--output-dir",
            "/tmp/gate-b-duplicate-root",
            "--git-commit",
            "0123456789012345678901234567890123456789",
            "--source-identity-sha256",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--cargo-lock-sha256",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "--reference-condition-id",
            "condition",
            "--reference-clob-market-id",
            "market",
            "--reference-market-slug",
            "slug",
            "--reference-up-token-id",
            "up-token",
            "--reference-up-outcome",
            "Up",
            "--reference-down-token-id",
            "down-token",
            "--reference-down-outcome",
            "Down",
            "--run-id",
            "duplicate-root-run",
            "--initial-available-pusd",
            "2000",
            "--per-market-all-in-commitment-limit-pusd",
            "1000",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
        duplicate_root.extend([
            "--gate-a-root".to_owned(),
            "/tmp/gate-a-one".to_owned(),
            "--gate-a-root".to_owned(),
            "/tmp/gate-a-two".to_owned(),
        ]);
        assert_eq!(
            enabled::parse_args_from(duplicate_root).unwrap_err(),
            "--gate-a-root 不得重复"
        );

        for obsolete in [
            "--historical-rule-snapshot",
            "--historical-rule-snapshot-sha256",
        ] {
            let mut rejected = arguments.clone();
            rejected.extend([obsolete.to_owned(), "obsolete".to_owned()]);
            assert_eq!(
                enabled::parse_args_from(rejected).unwrap_err(),
                format!("未知参数：{obsolete}")
            );
        }
    }

    #[cfg(not(feature = "measurement"))]
    #[test]
    fn feature_off_serializes_internal_measurement_fields_as_null() {
        let (observations, resources) = enabled::internal_measurement_fields_for_test();
        assert!(observations.is_null());
        assert!(resources.is_null());
    }
}
