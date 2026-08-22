use serde::Serialize;

const RUNNER_IDENTITY: &str = "sim-task-4-5-1-golden-v1";
const WORKLOAD_IDENTITY: &str = "public-sim-execution-two-market-256x8-v1";
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
    use std::fs::{self, File, OpenOptions};
    use std::io::{BufWriter, Read, Write};
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use contracts::{
        AssetIdentityV1, BestBidAskV2, CanonicalEventId, CanonicalSourceKind, ClobMarketId,
        CollectorAdmissionCoordinateV1, ConditionId, EventLineageV2, EventQualityV2, EventTimeV2,
        ExactDecimal, ExecutionMarketEventPayloadV3, ExecutionMarketEventV3, InitialAccountStateV2,
        InitialL2SnapshotV2, MarketDataQuality, MarketIdentityV2, MarketResolutionTerminalV3,
        MarketWindowV2, OutcomeIdentityV1, PayoutEntryV1, PriceLevelV1, ResyncQuality, Sha256Hex,
        SimCallerRoleV1, SourceEvidenceRefV2, TokenId,
    };
    use data_pipeline_core::{
        decode_execution_market_event_v3, derive_execution_market_event_v3_id,
        encode_execution_market_event_v3,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};
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
    use sim_system::latency_profile::{EmpiricalDelayProfile, load_embedded_proxy_latency_profile};
    use sim_system::platform_rules::{
        CurrentProcessSnapshotRequestV1, ImplementationIdentityV1, ReferenceMarketSpecV1,
        ReferenceOutcomeSpecV1, SealedProcessRuleSnapshotV3,
        query_and_seal_current_process_snapshot_v3,
    };
    use sim_system::scenario::{
        PLATFORM_SCENARIO_SCHEMA_V1, PlatformMarketV1, PlatformScenarioV1,
        SealedPlatformScenarioV1, SimulationScenarioArtifactV1,
    };

    use super::{
        DistributionSummary, LOOPS_PER_ROOT, PairKey, REQUESTS_PER_ROOT, RUNNER_IDENTITY,
        SafetyLimits, SafetyUsage, WORKLOAD_IDENTITY, distribution_summary, safety_violation,
        validate_feature_pair,
    };

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

    const RESOLUTION_ONE_SOURCE: i64 = MARKET_END + 10;
    const RESOLUTION_TWO_SOURCE: i64 = MARKET_END + 20;
    const FINAL_EOF_SOURCE: i64 = RESOLUTION_TWO_SOURCE + 86_500_000;
    const PUBLIC_DELAY_MS: i64 = 10;

    #[cfg(feature = "measurement")]
    const BUILD: &str = "measurement_on";
    #[cfg(not(feature = "measurement"))]
    const BUILD: &str = "measurement_off";

    #[derive(Debug)]
    struct Args {
        output_dir: PathBuf,
        identity: ImplementationIdentityV1,
        baseline_summary: Option<PathBuf>,
        argv: Vec<String>,
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
            run_id: run_id.clone(),
            durability_role: role,
            platform_rule_snapshot_file_sha256: snapshot.file_sha256().clone(),
            trade_finality_policy_file_sha256: finality.policy_id().clone(),
            platform_scenario_file_sha256: scenario.file_sha256().clone(),
            proxy_latency_profile_file_sha256: latency.file_sha256().clone(),
            implementation_identity: identity,
            market_event_source_identity_sha256: Sha256Hex("7".repeat(64)),
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
            .retry_create_session(spec)
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

    fn golden_buckets(
        scenario: &SealedPlatformScenarioV1,
    ) -> Result<
        Vec<(
            DurableMarketBucketReferenceV2,
            Arc<[ExecutionMarketEventV3]>,
        )>,
        String,
    > {
        let markets = scenario.scenario().market_set().markets();
        let source_times = [MARKET_ONE_START + 10, MARKET_TWO_START + 10];
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
                    BODY_ONE_SOURCE,
                    BODY_ONE_SOURCE + PUBLIC_DELAY_MS,
                    5,
                )?]
                .into_boxed_slice(),
            ),
            Arc::from(
                vec![body_event(
                    &markets[1],
                    BODY_TWO_SOURCE,
                    BODY_TWO_SOURCE + PUBLIC_DELAY_MS,
                    6,
                )?]
                .into_boxed_slice(),
            ),
        ];
        let resolution_events = [
            Arc::from(
                vec![resolution_event(
                    &markets[0],
                    RESOLUTION_ONE_SOURCE,
                    RESOLUTION_ONE_SOURCE + PUBLIC_DELAY_MS,
                    7,
                )?]
                .into_boxed_slice(),
            ),
            Arc::from(
                vec![resolution_event(
                    &markets[1],
                    RESOLUTION_TWO_SOURCE,
                    RESOLUTION_TWO_SOURCE + PUBLIC_DELAY_MS,
                    8,
                )?]
                .into_boxed_slice(),
            ),
        ];
        let final_events: Arc<[ExecutionMarketEventV3]> = Arc::from(
            vec![body_event(
                &markets[0],
                FINAL_EOF_SOURCE,
                FINAL_EOF_SOURCE + PUBLIC_DELAY_MS,
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
                    BODY_ONE_SOURCE,
                    BODY_TWO_SOURCE,
                    RESOLUTION_ONE_SOURCE,
                    RESOLUTION_TWO_SOURCE,
                    FINAL_EOF_SOURCE,
                ],
                MarketEventPathV1::PublicVisibility => [
                    source_times[0] + PUBLIC_DELAY_MS,
                    source_times[1] + PUBLIC_DELAY_MS,
                    BODY_ONE_SOURCE + PUBLIC_DELAY_MS,
                    BODY_TWO_SOURCE + PUBLIC_DELAY_MS,
                    RESOLUTION_ONE_SOURCE + PUBLIC_DELAY_MS,
                    RESOLUTION_TWO_SOURCE + PUBLIC_DELAY_MS,
                    FINAL_EOF_SOURCE + PUBLIC_DELAY_MS,
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

    fn drive_fixed_requests(
        execution: &mut SimExecution,
        scenario: &SealedPlatformScenarioV1,
        root: usize,
        buckets: &[&(
            DurableMarketBucketReferenceV2,
            Arc<[ExecutionMarketEventV3]>,
        )],
        next_bucket: &mut usize,
        cancelled: &AtomicBool,
        resource_high_water: &mut Value,
    ) -> Result<(), String> {
        let markets = scenario.scenario().market_set().markets();
        let public_visibility: PublicVisibilityCursorV1 = serde_json::from_value(json!({
            "closed_through_received_at_utc_ms": MARKET_TWO_START + 10 + PUBLIC_DELAY_MS,
            "next_visibility_index_position": 4
        }))
        .map_err(|error| format!("构造公开可见游标失败：{error}"))?;
        let mut next_decision_at_utc_ms = REQUEST_CUTOFF_BASE;
        for iteration in 0..LOOPS_PER_ROOT {
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
            let receipt = execution
                .post_order(
                    cutoff(next_decision_at_utc_ms),
                    PostOrderRequestV2 {
                        context: context("post-order"),
                        item: post(&format!("{prefix}-post-item")),
                    },
                )
                .map_err(request_error)?;
            next_decision_at_utc_ms = receipt.platform_arrived_at_utc_ms().saturating_add(1);
            drive_until_current_request_is_complete(execution, buckets, next_bucket)?;
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
            next_decision_at_utc_ms = receipt.platform_arrived_at_utc_ms().saturating_add(1);
            drive_until_current_request_is_complete(execution, buckets, next_bucket)?;
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
            next_decision_at_utc_ms = receipt.platform_arrived_at_utc_ms().saturating_add(1);
            drive_until_current_request_is_complete(execution, buckets, next_bucket)?;
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
            next_decision_at_utc_ms = receipt.platform_arrived_at_utc_ms().saturating_add(1);
            drive_until_current_request_is_complete(execution, buckets, next_bucket)?;
            let receipt = execution
                .cancel_all_orders(
                    cutoff(next_decision_at_utc_ms),
                    CancelAllOrdersRequestV2 {
                        context: context("cancel-all-orders"),
                    },
                )
                .map_err(request_error)?;
            next_decision_at_utc_ms = receipt.platform_arrived_at_utc_ms().saturating_add(1);
            drive_until_current_request_is_complete(execution, buckets, next_bucket)?;
            let receipt = execution
                .order(
                    cutoff(next_decision_at_utc_ms),
                    OrderRequestV2 {
                        context: context("order"),
                        platform_order_id: "sim-1".to_owned(),
                    },
                )
                .map_err(request_error)?;
            next_decision_at_utc_ms = receipt.platform_arrived_at_utc_ms().saturating_add(1);
            drive_until_current_request_is_complete(execution, buckets, next_bucket)?;
            let receipt = execution
                .orders(
                    cutoff(next_decision_at_utc_ms),
                    OrdersRequestV2 {
                        context: context("orders"),
                        filter: OrdersFilterV1 {
                            condition_id: Some(condition_id.clone()),
                            token_id: Some(token_id.clone()),
                        },
                        cursor: None,
                    },
                )
                .map_err(request_error)?;
            next_decision_at_utc_ms = receipt.platform_arrived_at_utc_ms().saturating_add(1);
            drive_until_current_request_is_complete(execution, buckets, next_bucket)?;
            let receipt = execution
                .trades(
                    cutoff(next_decision_at_utc_ms),
                    TradesRequestV2 {
                        context: context("trades"),
                        filter: TradesFilterV1 {
                            condition_id: Some(condition_id),
                            token_id: Some(token_id),
                            ..Default::default()
                        },
                        cursor: None,
                    },
                )
                .map_err(request_error)?;
            next_decision_at_utc_ms = receipt.platform_arrived_at_utc_ms().saturating_add(1);
            drive_until_current_request_is_complete(execution, buckets, next_bucket)?;
            merge_numeric_max(resource_high_water, &measurement_resources(execution)?);
        }
        Ok(())
    }

    fn drive_until_current_request_is_complete(
        execution: &mut SimExecution,
        buckets: &[&(
            DurableMarketBucketReferenceV2,
            Arc<[ExecutionMarketEventV3]>,
        )],
        next_bucket: &mut usize,
    ) -> Result<(), String> {
        while execution
            .next_required_logical_drive()
            .is_some_and(|required| required.mutation_at_utc_ms.is_some())
        {
            if *next_bucket >= buckets.len() {
                return Err("固定请求在 EOF 前仍有 pending mutation".to_owned());
            }
            let required = execution
                .next_required_logical_drive()
                .expect("循环已经确认下一必要推进存在");
            let next_reference = &buckets[*next_bucket].0;
            if next_reference.bucket_at_utc_ms <= required.earliest_at_utc_ms() {
                drive_real_bucket(execution, buckets[*next_bucket])?;
                *next_bucket += 1;
            } else {
                execution
                    .drive_platform_prefix(sim_system::execution::PlatformPrefixAdvanceV2 {
                        path: required.path,
                        closed_through_utc_ms: required.earliest_at_utc_ms(),
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
            .is_some_and(|required| required.earliest_at_utc_ms() < next_reference.bucket_at_utc_ms)
        {
            let required = execution
                .next_required_logical_drive()
                .expect("循环已经确认下一必要推进存在");
            execution
                .drive_platform_prefix(sim_system::execution::PlatformPrefixAdvanceV2 {
                    path: required.path,
                    closed_through_utc_ms: required.earliest_at_utc_ms(),
                    next_index_position: next_reference.start_index_position,
                })
                .map_err(|error| format!("驱动桶前必要空前缀失败：{error}"))?;
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

    fn fixed_scenario() -> Result<Arc<SealedPlatformScenarioV1>, String> {
        let first = market(
            CONDITION,
            "3278040",
            TOKEN_UP,
            TOKEN_DOWN,
            MARKET_ONE_START,
            MARKET_END,
        )?;
        let second = market(
            &format!("0x{}", "e".repeat(64)),
            "3278041",
            "10000000000000000000000000000000000000000000000000000000000000001",
            "10000000000000000000000000000000000000000000000000000000000000002",
            MARKET_TWO_START,
            MARKET_END,
        )?;
        let scenario = PlatformScenarioV1::try_new(vec![first, second])
            .map_err(|error| format!("构造固定双市场场景失败：{error}"))?;
        let artifact = SimulationScenarioArtifactV1 {
            schema_version: PLATFORM_SCENARIO_SCHEMA_V1.to_owned(),
            scenario_sha256: scenario.scenario_sha256().clone(),
            market_set_sha256: scenario.market_set_sha256().clone(),
            markets: scenario.market_set().markets().to_vec(),
        };
        let bytes =
            serde_json::to_vec(&artifact).map_err(|error| format!("编码固定场景失败：{error}"))?;
        SealedPlatformScenarioV1::decode_and_seal(&bytes)
            .map(Arc::new)
            .map_err(|error| format!("密封固定场景失败：{error}"))
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
        let bytes = serde_json::to_vec(&json!({
            "schema_version": "trade_finality_policy_v2",
            "normal_path": { "steps": [
                {"state":"MATCHED","offset_ms":0},
                {"state":"MINED","offset_ms":10},
                {"state":"CONFIRMED","offset_ms":20}
            ], "economic_application_ordinal": 1 },
            "failure_overrides": []
        }))
        .map_err(|error| format!("编码成功最终性策略失败：{error}"))?;
        TradeFinalityPolicyV2::decode_and_seal(&bytes)
            .map(Arc::new)
            .map_err(|error| format!("密封成功最终性策略失败：{error}"))
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

    fn sha256_file(path: &Path) -> Result<String, String> {
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
        let argv = std::env::args().collect::<Vec<_>>();
        let mut values = argv.iter().skip(1);
        let mut output_dir = None;
        let mut git_commit = None;
        let mut source = None;
        let mut lock = None;
        let mut baseline = None;
        while let Some(flag) = values.next() {
            let value = values.next().ok_or_else(|| format!("参数 {flag} 缺少值"))?;
            match flag.as_str() {
                "--output-dir" => output_dir = Some(PathBuf::from(value)),
                "--git-commit" => git_commit = Some(value.clone()),
                "--source-identity-sha256" => source = Some(value.clone()),
                "--cargo-lock-sha256" => lock = Some(value.clone()),
                "--baseline-summary" => baseline = Some(PathBuf::from(value)),
                _ => return Err(format!("未知参数：{flag}")),
            }
        }
        let git_commit = git_commit.ok_or_else(|| "缺少 --git-commit".to_owned())?;
        let source = source.ok_or_else(|| "缺少 --source-identity-sha256".to_owned())?;
        let lock = lock.ok_or_else(|| "缺少 --cargo-lock-sha256".to_owned())?;
        if git_commit.len() != 40 || source.len() != 64 || lock.len() != 64 {
            return Err("git commit 必须为 40 位，SHA-256 必须为 64 位".to_owned());
        }
        Ok(Args {
            output_dir: output_dir.ok_or_else(|| "缺少 --output-dir".to_owned())?,
            identity: ImplementationIdentityV1 {
                git_commit,
                source_identity_sha256: Sha256Hex(source),
                cargo_lock_sha256: Sha256Hex(lock),
            },
            baseline_summary: baseline,
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
    use super::*;

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

    #[cfg(not(feature = "measurement"))]
    #[test]
    fn feature_off_serializes_internal_measurement_fields_as_null() {
        let (observations, resources) = enabled::internal_measurement_fields_for_test();
        assert!(observations.is_null());
        assert!(resources.is_null());
    }
}
