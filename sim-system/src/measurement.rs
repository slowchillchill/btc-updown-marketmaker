use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use contracts::{AccountInstanceKeyV1, PlatformSourceV1, Sha256Hex, SimCallerRoleV1};
use serde::{Deserialize, Serialize};

use crate::resource::{
    COMPONENT_GOLDEN_RESOURCE_POLICY_ID_V1, ResourceLimitV1, ResourceQuantityV1,
    SIMULATION_RESOURCE_POLICY_SCHEMA_V1, SimulationDurableResourcePolicyV1,
    SimulationDurableResourceUsageV1, SimulationFailureEvidenceReserveV1,
    SimulationResourceHighWaterV1, SimulationResourcePolicyV1, SimulationResourceUsageV1,
    SimulationRunResourcePolicyV1, SimulationRunResourceUsageV1, SimulationSessionResourcePolicyV1,
    SimulationSessionResourceUsageV1, canonical_json_sha256_v1,
};

pub const COMPONENT_GOLDEN_LOAD_SCHEMA_V1: &str = "component_golden_load_v1";
pub const COMPONENT_GOLDEN_LOAD_ID_V1: &str = "COMPONENT_GOLDEN_V1";
pub const COMPONENT_GOLDEN_MEASUREMENT_REPORT_SCHEMA_V1: &str =
    "component_golden_measurement_report_v1";
pub const COMPONENT_GOLDEN_RUN_MEASUREMENT_SCHEMA_V1: &str = "component_golden_run_measurement_v1";
pub const TRAJECTORY_LOGICAL_SUMMARY_SCHEMA_V1: &str =
    "component_golden_trajectory_logical_summary_v1";

const COMPONENT_GOLDEN_LOAD_HASH_DOMAIN_V1: &str = "sim-system/component-golden-load/v1";
const TRAJECTORY_LOGICAL_SUMMARY_HASH_DOMAIN_V1: &str =
    "sim-system/component-golden-trajectory-logical-summary/v1";
const COMPONENT_GOLDEN_REPORT_HASH_DOMAIN_V1: &str =
    "sim-system/component-golden-measurement-report/v1";
const COMPONENT_GOLDEN_RUN_MEASUREMENT_HASH_DOMAIN_V1: &str =
    "sim-system/component-golden-run-measurement/v1";
const FROZEN_RESOURCE_BUDGETS_HASH_DOMAIN_V1: &str = "sim-system/frozen-resource-budgets/v1";
const MEASUREMENT_CACHE_PRECONDITION_HASH_DOMAIN_V1: &str =
    "sim-system/measurement-cache-precondition/v1";
const FOUR_KIB: u64 = 4 * 1024;
const SIXTY_FOUR_MIB: u64 = 64 * 1024 * 1024;

pub const MEASUREMENT_CACHE_PRECONDITION_SCHEMA_V1: &str = "measurement_cache_precondition_v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ComponentGoldenTrajectoryV1 {
    A,
    B,
}

pub const COMPONENT_GOLDEN_TRAJECTORIES_V1: [ComponentGoldenTrajectoryV1; 2] = [
    ComponentGoldenTrajectoryV1::A,
    ComponentGoldenTrajectoryV1::B,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ComponentGoldenRoleV1 {
    ModelTrain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ComponentGoldenMeasurementModeV1 {
    Serial,
    Parallel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ComponentGoldenTrialIdV1 {
    S1,
    P1,
    P2,
    S2,
    S3,
    P3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FrozenBudgetVerificationTrialIdV1 {
    Serial,
    Parallel,
}

pub const FROZEN_BUDGET_VERIFICATION_TRIALS_V1: [FrozenBudgetVerificationTrialIdV1; 2] = [
    FrozenBudgetVerificationTrialIdV1::Serial,
    FrozenBudgetVerificationTrialIdV1::Parallel,
];

impl FrozenBudgetVerificationTrialIdV1 {
    pub const fn mode(self) -> ComponentGoldenMeasurementModeV1 {
        match self {
            Self::Serial => ComponentGoldenMeasurementModeV1::Serial,
            Self::Parallel => ComponentGoldenMeasurementModeV1::Parallel,
        }
    }

    pub const fn worker_count(self) -> u16 {
        match self {
            Self::Serial => 1,
            Self::Parallel => 2,
        }
    }
}

pub const COMPONENT_GOLDEN_TRIAL_ORDER_V1: [ComponentGoldenTrialIdV1; 6] = [
    ComponentGoldenTrialIdV1::S1,
    ComponentGoldenTrialIdV1::P1,
    ComponentGoldenTrialIdV1::P2,
    ComponentGoldenTrialIdV1::S2,
    ComponentGoldenTrialIdV1::S3,
    ComponentGoldenTrialIdV1::P3,
];

impl ComponentGoldenTrialIdV1 {
    pub const fn mode(self) -> ComponentGoldenMeasurementModeV1 {
        match self {
            Self::S1 | Self::S2 | Self::S3 => ComponentGoldenMeasurementModeV1::Serial,
            Self::P1 | Self::P2 | Self::P3 => ComponentGoldenMeasurementModeV1::Parallel,
        }
    }

    pub const fn worker_count(self) -> u16 {
        match self.mode() {
            ComponentGoldenMeasurementModeV1::Serial => 1,
            ComponentGoldenMeasurementModeV1::Parallel => 2,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentGoldenIdentityIsolationV1 {
    pub trajectory_identity_is_distinct: bool,
    pub seed_is_distinct: bool,
    pub bridge_identity_is_distinct: bool,
    pub account_identity_is_distinct: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentGoldenEventLoadV1 {
    pub markets_per_trajectory: u16,
    pub market_duration_seconds: u16,
    pub markets_overlap: bool,
    pub unique_normative_events_per_market: u16,
    pub unique_event_ids: u16,
    pub hidden_and_public_paths_per_trajectory: u16,
    pub total_path_deliveries: u16,
    pub max_same_logical_time_burst: u16,
    pub canonical_reference_events: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentGoldenCallLoadV1 {
    pub top_level_calls: u16,
    pub post_orders_batch_sizes: [u16; 7],
    pub post_order_calls: u16,
    pub cancel_order_calls: u16,
    pub cancel_orders_calls: u16,
    pub cancel_orders_input_items: u16,
    pub cancel_orders_unique_targets: u16,
    pub cancel_all_orders_calls: u16,
    pub order_calls: u16,
    pub orders_calls: u16,
    pub trades_calls: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentGoldenLifecycleLoadV1 {
    pub first_page_items: u16,
    pub continuation_page_items: u16,
    pub feedback_is_bounded_then_drained: bool,
    pub feedback_drains_in_private_sequence: bool,
    pub strong_downstream_watermark_advances: bool,
    pub first_market_release_and_redemption_precede_second_market_success: bool,
    pub logical_retention_cycles_per_trajectory: u16,
    pub uses_wall_clock_sleep: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentGoldenScheduleV1 {
    pub concurrency_levels: [u16; 2],
    pub serial_trajectory_order: [ComponentGoldenTrajectoryV1; 2],
    pub parallel_long_lived_workers: u16,
    pub parallel_uses_stage_barriers: bool,
    pub trial_order: [ComponentGoldenTrialIdV1; 6],
    pub discarded_warmup_trials: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentGoldenLoadContractV1 {
    pub schema_version: String,
    pub load_id: String,
    pub role: ComponentGoldenRoleV1,
    pub trajectory_count: u16,
    pub identity_isolation: ComponentGoldenIdentityIsolationV1,
    pub events: ComponentGoldenEventLoadV1,
    pub calls_per_trajectory: ComponentGoldenCallLoadV1,
    pub lifecycle: ComponentGoldenLifecycleLoadV1,
    pub schedule: ComponentGoldenScheduleV1,
}

impl ComponentGoldenLoadContractV1 {
    pub fn component_golden_v1() -> Self {
        Self {
            schema_version: COMPONENT_GOLDEN_LOAD_SCHEMA_V1.to_owned(),
            load_id: COMPONENT_GOLDEN_LOAD_ID_V1.to_owned(),
            role: ComponentGoldenRoleV1::ModelTrain,
            trajectory_count: 2,
            identity_isolation: ComponentGoldenIdentityIsolationV1 {
                trajectory_identity_is_distinct: true,
                seed_is_distinct: true,
                bridge_identity_is_distinct: true,
                account_identity_is_distinct: true,
            },
            events: ComponentGoldenEventLoadV1 {
                markets_per_trajectory: 2,
                market_duration_seconds: 5 * 60,
                markets_overlap: true,
                unique_normative_events_per_market: 5,
                unique_event_ids: 10,
                hidden_and_public_paths_per_trajectory: 2,
                total_path_deliveries: 40,
                max_same_logical_time_burst: 2,
                canonical_reference_events: 0,
            },
            calls_per_trajectory: ComponentGoldenCallLoadV1 {
                top_level_calls: 19,
                post_orders_batch_sizes: [15, 15, 15, 15, 15, 15, 12],
                post_order_calls: 3,
                cancel_order_calls: 1,
                cancel_orders_calls: 1,
                cancel_orders_input_items: 4,
                cancel_orders_unique_targets: 3,
                cancel_all_orders_calls: 1,
                order_calls: 2,
                orders_calls: 2,
                trades_calls: 2,
            },
            lifecycle: ComponentGoldenLifecycleLoadV1 {
                first_page_items: 100,
                continuation_page_items: 2,
                feedback_is_bounded_then_drained: true,
                feedback_drains_in_private_sequence: true,
                strong_downstream_watermark_advances: true,
                first_market_release_and_redemption_precede_second_market_success: true,
                logical_retention_cycles_per_trajectory: 2,
                uses_wall_clock_sleep: false,
            },
            schedule: ComponentGoldenScheduleV1 {
                concurrency_levels: [1, 2],
                serial_trajectory_order: COMPONENT_GOLDEN_TRAJECTORIES_V1,
                parallel_long_lived_workers: 2,
                parallel_uses_stage_barriers: true,
                trial_order: COMPONENT_GOLDEN_TRIAL_ORDER_V1,
                discarded_warmup_trials: 0,
            },
        }
    }

    pub fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        if self != &Self::component_golden_v1() {
            return Err(SimulationMeasurementErrorV1::ContractMismatch(
                "COMPONENT_GOLDEN_V1 负载与预登记刻度不一致".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn content_sha256(&self) -> Result<Sha256Hex, SimulationMeasurementErrorV1> {
        self.validate()?;
        measurement_hash(COMPONENT_GOLDEN_LOAD_HASH_DOMAIN_V1, self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MeasurementLatencyKindV1 {
    EventApplication,
    RequestResponse,
    QueryResponse,
    CheckpointOrSeal,
    RetentionRolling,
    TrialEndToEnd,
}

pub const MEASUREMENT_LATENCY_KINDS_V1: [MeasurementLatencyKindV1; 6] = [
    MeasurementLatencyKindV1::EventApplication,
    MeasurementLatencyKindV1::RequestResponse,
    MeasurementLatencyKindV1::QueryResponse,
    MeasurementLatencyKindV1::CheckpointOrSeal,
    MeasurementLatencyKindV1::RetentionRolling,
    MeasurementLatencyKindV1::TrialEndToEnd,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NearestRankLatencyNsV1 {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
}

pub fn nearest_rank_latency_ns_v1(
    samples_ns: &[u64],
) -> Result<NearestRankLatencyNsV1, SimulationMeasurementErrorV1> {
    if samples_ns.is_empty() {
        return Err(SimulationMeasurementErrorV1::EmptyLatencySamples);
    }
    let mut ordered = samples_ns.to_vec();
    ordered.sort_unstable();
    Ok(NearestRankLatencyNsV1 {
        p50: nearest_rank_value(&ordered, 50)?,
        p95: nearest_rank_value(&ordered, 95)?,
        p99: nearest_rank_value(&ordered, 99)?,
        max: *ordered
            .last()
            .ok_or(SimulationMeasurementErrorV1::EmptyLatencySamples)?,
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawLatencySamplesNsV1 {
    pub samples_ns: Vec<u64>,
    pub nearest_rank: NearestRankLatencyNsV1,
}

impl RawLatencySamplesNsV1 {
    pub fn from_samples(samples_ns: Vec<u64>) -> Result<Self, SimulationMeasurementErrorV1> {
        let nearest_rank = nearest_rank_latency_ns_v1(&samples_ns)?;
        Ok(Self {
            samples_ns,
            nearest_rank,
        })
    }

    pub fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        let expected = nearest_rank_latency_ns_v1(&self.samples_ns)?;
        if self.nearest_rank != expected {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "延迟最近秩统计不能由原始纳秒样本重算".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcfsIoSnapshotV1 {
    pub rchar_bytes: u64,
    pub wchar_bytes: u64,
    pub read_syscalls: u64,
    pub write_syscalls: u64,
    pub storage_read_bytes: u64,
    pub storage_write_bytes: u64,
    pub cancelled_write_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcfsProcessResourceSnapshotV1 {
    pub pid: u32,
    pub process_start_time_ticks: u64,
    pub vm_rss_bytes: u64,
    pub vm_hwm_bytes: u64,
    pub vm_swap_bytes: u64,
    pub thread_count: u32,
    pub open_file_descriptor_count: u64,
    pub file_descriptor_soft_limit: u64,
    pub file_descriptor_hard_limit: u64,
    pub user_cpu_ticks: u64,
    pub system_cpu_ticks: u64,
    pub io: ProcfsIoSnapshotV1,
}

impl ProcfsProcessResourceSnapshotV1 {
    fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        if self.pid == 0
            || self.process_start_time_ticks == 0
            || self.thread_count == 0
            || self.vm_hwm_bytes < self.vm_rss_bytes
            || self.file_descriptor_soft_limit == 0
            || self.file_descriptor_hard_limit == 0
            || self.open_file_descriptor_count > self.file_descriptor_soft_limit
            || self.file_descriptor_soft_limit > self.file_descriptor_hard_limit
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "/proc 进程资源快照非法".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "limit",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum CgroupMemoryLimitV1 {
    Unbounded,
    Bytes { bytes: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "limit",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum CgroupPidsLimitV1 {
    Unbounded,
    Count { count: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CgroupMemoryEventsV1 {
    pub low: u64,
    pub high: u64,
    pub max: u64,
    pub oom: u64,
    pub oom_kill: u64,
    pub oom_group_kill: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CgroupV2ResourceSnapshotV1 {
    pub memory_current_bytes: u64,
    pub memory_peak_bytes: Option<u64>,
    pub memory_limit: CgroupMemoryLimitV1,
    pub memory_swap_current_bytes: u64,
    pub memory_swap_peak_bytes: Option<u64>,
    pub memory_swap_limit: CgroupMemoryLimitV1,
    pub memory_events: CgroupMemoryEventsV1,
    pub cpu_usage_usec: u64,
    pub pids_current: u64,
    pub pids_limit: CgroupPidsLimitV1,
}

impl CgroupV2ResourceSnapshotV1 {
    fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        let Some(memory_peak_bytes) = self.memory_peak_bytes else {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "正式测量必须记录 cgroup memory.peak".to_owned(),
            ));
        };
        if memory_peak_bytes < self.memory_current_bytes {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "cgroup memory.peak 小于 memory.current".to_owned(),
            ));
        }
        let CgroupMemoryLimitV1::Bytes {
            bytes: memory_max_bytes,
        } = self.memory_limit
        else {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "正式测量要求有限 cgroup memory.max".to_owned(),
            ));
        };
        if memory_max_bytes == 0
            || self.memory_current_bytes > memory_max_bytes
            || memory_peak_bytes > memory_max_bytes
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "cgroup memory.current 超出有限 memory.max".to_owned(),
            ));
        }
        let Some(memory_swap_peak_bytes) = self.memory_swap_peak_bytes else {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "正式测量必须记录 cgroup memory.swap.peak".to_owned(),
            ));
        };
        if memory_swap_peak_bytes < self.memory_swap_current_bytes {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "cgroup memory.swap.peak 小于 memory.swap.current".to_owned(),
            ));
        }
        let CgroupMemoryLimitV1::Bytes {
            bytes: memory_swap_max_bytes,
        } = self.memory_swap_limit
        else {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "正式测量要求有限 cgroup memory.swap.max".to_owned(),
            ));
        };
        if self.memory_swap_current_bytes > memory_swap_max_bytes
            || memory_swap_peak_bytes > memory_swap_max_bytes
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "cgroup memory.swap.current 超出有限 memory.swap.max".to_owned(),
            ));
        }
        let CgroupPidsLimitV1::Count {
            count: pids_max_count,
        } = self.pids_limit
        else {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "正式测量要求有限 cgroup pids.max".to_owned(),
            ));
        };
        if pids_max_count == 0 || self.pids_current > pids_max_count {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "cgroup pids.current 超出有限 pids.max".to_owned(),
            ));
        }
        if self.memory_events.low != 0
            || self.memory_events.high != 0
            || self.memory_events.max != 0
            || self.memory_events.oom != 0
            || self.memory_events.oom_kill != 0
            || self.memory_events.oom_group_kill != 0
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "正式测量不得出现 cgroup memory.events 资源事件".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InternalResourceSnapshotV1 {
    pub trajectory_sessions:
        BTreeMap<ComponentGoldenTrajectoryV1, SimulationSessionResourceUsageV1>,
    pub shared_run: SimulationRunResourceUsageV1,
    pub durable: SimulationDurableResourceUsageV1,
    pub high_water: SimulationResourceHighWaterV1,
}

impl InternalResourceSnapshotV1 {
    fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        for session in self.trajectory_sessions.values() {
            let usage = SimulationResourceUsageV1 {
                session: session.clone(),
                run: self.shared_run.clone(),
                durable: self.durable.clone(),
            };
            if !self.high_water.covers(&usage) {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "内部资源高水位未覆盖当前资源快照".to_owned(),
                ));
            }
        }
        let shared_usage = SimulationResourceUsageV1 {
            session: SimulationSessionResourceUsageV1::default(),
            run: self.shared_run.clone(),
            durable: self.durable.clone(),
        };
        if !self.high_water.covers(&shared_usage) {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "内部资源高水位未覆盖共享运行或耐久资源".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasuredResourceSnapshotV1 {
    pub sample_index: u64,
    pub elapsed_ns: u64,
    pub procfs: ProcfsProcessResourceSnapshotV1,
    pub cgroup_v2: CgroupV2ResourceSnapshotV1,
    pub internal: InternalResourceSnapshotV1,
}

impl MeasuredResourceSnapshotV1 {
    fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        self.procfs.validate()?;
        self.cgroup_v2.validate()?;
        self.internal.validate()
    }

    fn validate_follows(&self, previous: &Self) -> Result<(), SimulationMeasurementErrorV1> {
        let proc_counters_are_monotonic = self.procfs.vm_hwm_bytes >= previous.procfs.vm_hwm_bytes
            && self.procfs.user_cpu_ticks >= previous.procfs.user_cpu_ticks
            && self.procfs.system_cpu_ticks >= previous.procfs.system_cpu_ticks
            && self.procfs.io.rchar_bytes >= previous.procfs.io.rchar_bytes
            && self.procfs.io.wchar_bytes >= previous.procfs.io.wchar_bytes
            && self.procfs.io.read_syscalls >= previous.procfs.io.read_syscalls
            && self.procfs.io.write_syscalls >= previous.procfs.io.write_syscalls
            && self.procfs.io.storage_read_bytes >= previous.procfs.io.storage_read_bytes
            && self.procfs.io.storage_write_bytes >= previous.procfs.io.storage_write_bytes
            && self.procfs.io.cancelled_write_bytes >= previous.procfs.io.cancelled_write_bytes;
        let cgroup_events_are_monotonic = self.cgroup_v2.memory_events.low
            >= previous.cgroup_v2.memory_events.low
            && self.cgroup_v2.memory_events.high >= previous.cgroup_v2.memory_events.high
            && self.cgroup_v2.memory_events.max >= previous.cgroup_v2.memory_events.max
            && self.cgroup_v2.memory_events.oom >= previous.cgroup_v2.memory_events.oom
            && self.cgroup_v2.memory_events.oom_kill >= previous.cgroup_v2.memory_events.oom_kill
            && self.cgroup_v2.memory_events.oom_group_kill
                >= previous.cgroup_v2.memory_events.oom_group_kill;
        let optional_peaks_are_monotonic = match (
            self.cgroup_v2.memory_peak_bytes,
            previous.cgroup_v2.memory_peak_bytes,
        ) {
            (Some(current), Some(before)) => current >= before,
            (None, None) | (Some(_), None) => true,
            (None, Some(_)) => false,
        } && match (
            self.cgroup_v2.memory_swap_peak_bytes,
            previous.cgroup_v2.memory_swap_peak_bytes,
        ) {
            (Some(current), Some(before)) => current >= before,
            (None, None) | (Some(_), None) => true,
            (None, Some(_)) => false,
        };
        if self.procfs.pid != previous.procfs.pid
            || self.procfs.process_start_time_ticks != previous.procfs.process_start_time_ticks
            || self.procfs.file_descriptor_soft_limit != previous.procfs.file_descriptor_soft_limit
            || self.procfs.file_descriptor_hard_limit != previous.procfs.file_descriptor_hard_limit
            || self.cgroup_v2.memory_limit != previous.cgroup_v2.memory_limit
            || self.cgroup_v2.memory_swap_limit != previous.cgroup_v2.memory_swap_limit
            || self.cgroup_v2.pids_limit != previous.cgroup_v2.pids_limit
            || self.cgroup_v2.cpu_usage_usec < previous.cgroup_v2.cpu_usage_usec
            || !proc_counters_are_monotonic
            || !cgroup_events_are_monotonic
            || !optional_peaks_are_monotonic
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "同一试次的 /proc 或 cgroup 累计计数回退或身份改变".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementRuntimeHardGuardsV1 {
    pub cgroup_memory_max_bytes: u64,
    pub cgroup_memory_swap_max_bytes: u64,
    pub cgroup_pids_max_count: u64,
    pub file_descriptor_soft_limit: u64,
    pub file_descriptor_hard_limit: u64,
}

impl MeasurementRuntimeHardGuardsV1 {
    fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        if self.cgroup_memory_max_bytes == 0
            || self.cgroup_pids_max_count == 0
            || self.file_descriptor_soft_limit == 0
            || self.file_descriptor_hard_limit == 0
            || self.file_descriptor_soft_limit > self.file_descriptor_hard_limit
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "正式测量运行时硬护栏必须有限且合法".to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_snapshot(
        &self,
        snapshot: &MeasuredResourceSnapshotV1,
    ) -> Result<(), SimulationMeasurementErrorV1> {
        let memory_max_bytes = match snapshot.cgroup_v2.memory_limit {
            CgroupMemoryLimitV1::Bytes { bytes } => bytes,
            CgroupMemoryLimitV1::Unbounded => {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "正式测量不得使用无界 cgroup memory.max".to_owned(),
                ));
            }
        };
        let memory_swap_max_bytes = match snapshot.cgroup_v2.memory_swap_limit {
            CgroupMemoryLimitV1::Bytes { bytes } => bytes,
            CgroupMemoryLimitV1::Unbounded => {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "正式测量不得使用无界 cgroup memory.swap.max".to_owned(),
                ));
            }
        };
        let pids_max_count = match snapshot.cgroup_v2.pids_limit {
            CgroupPidsLimitV1::Count { count } => count,
            CgroupPidsLimitV1::Unbounded => {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "正式测量不得使用无界 cgroup pids.max".to_owned(),
                ));
            }
        };
        if memory_max_bytes != self.cgroup_memory_max_bytes
            || memory_swap_max_bytes != self.cgroup_memory_swap_max_bytes
            || pids_max_count != self.cgroup_pids_max_count
            || snapshot.procfs.file_descriptor_soft_limit != self.file_descriptor_soft_limit
            || snapshot.procfs.file_descriptor_hard_limit != self.file_descriptor_hard_limit
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "试次运行时硬护栏与报告冻结值不一致".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TrajectoryLogicalComponentV1 {
    Account,
    Markets,
    Orders,
    Trades,
    RequestResults,
    Feedback,
    Finality,
    Retention,
}

pub const TRAJECTORY_LOGICAL_COMPONENTS_V1: [TrajectoryLogicalComponentV1; 8] = [
    TrajectoryLogicalComponentV1::Account,
    TrajectoryLogicalComponentV1::Markets,
    TrajectoryLogicalComponentV1::Orders,
    TrajectoryLogicalComponentV1::Trades,
    TrajectoryLogicalComponentV1::RequestResults,
    TrajectoryLogicalComponentV1::Feedback,
    TrajectoryLogicalComponentV1::Finality,
    TrajectoryLogicalComponentV1::Retention,
];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// 只包含单条账户轨迹的逻辑身份与逻辑终态；共享证据提交序号、耐久文件路径和文件 SHA-256
/// 没有字段入口，因此不能参与串行/并行确定性摘要。
pub struct TrajectoryLogicalSummaryInputV1 {
    pub schema_version: String,
    pub workload_contract_sha256: Sha256Hex,
    pub trajectory: ComponentGoldenTrajectoryV1,
    pub logical_component_sha256: BTreeMap<TrajectoryLogicalComponentV1, Sha256Hex>,
}

impl TrajectoryLogicalSummaryInputV1 {
    pub fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        if self.schema_version != TRAJECTORY_LOGICAL_SUMMARY_SCHEMA_V1 {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "逐轨迹逻辑摘要 schema_version 非法".to_owned(),
            ));
        }
        validate_sha256(&self.workload_contract_sha256, "workload_contract_sha256")?;
        let expected = TRAJECTORY_LOGICAL_COMPONENTS_V1
            .into_iter()
            .collect::<Vec<_>>();
        let actual = self
            .logical_component_sha256
            .keys()
            .copied()
            .collect::<Vec<_>>();
        if actual != expected {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "逐轨迹逻辑摘要成员不完整或含额外成员".to_owned(),
            ));
        }
        for (component, sha256) in &self.logical_component_sha256 {
            validate_sha256(sha256, component.field_name())?;
        }
        Ok(())
    }

    pub fn content_sha256(&self) -> Result<Sha256Hex, SimulationMeasurementErrorV1> {
        self.validate()?;
        measurement_hash(TRAJECTORY_LOGICAL_SUMMARY_HASH_DOMAIN_V1, self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrajectoryRuntimeBindingV1 {
    pub session_id: String,
    pub session_seed_sha256: Sha256Hex,
    pub response_bridge_binding_id: String,
    pub account_instance_key: AccountInstanceKeyV1,
}

impl TrajectoryRuntimeBindingV1 {
    fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        if self.session_id.trim().is_empty()
            || self.response_bridge_binding_id.trim().is_empty()
            || self.account_instance_key.run_id.trim().is_empty()
            || self
                .account_instance_key
                .caller_instance_id
                .trim()
                .is_empty()
            || self.account_instance_key.account_id.trim().is_empty()
            || self.account_instance_key.caller_role != SimCallerRoleV1::ModelTrain
            || self.account_instance_key.platform_source != PlatformSourceV1::Simulated
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "逐轨迹运行身份绑定非法".to_owned(),
            ));
        }
        validate_sha256(&self.session_seed_sha256, "session_seed_sha256")
    }
}

impl TrajectoryLogicalComponentV1 {
    const fn field_name(self) -> &'static str {
        match self {
            Self::Account => "logical_component_sha256.ACCOUNT",
            Self::Markets => "logical_component_sha256.MARKETS",
            Self::Orders => "logical_component_sha256.ORDERS",
            Self::Trades => "logical_component_sha256.TRADES",
            Self::RequestResults => "logical_component_sha256.REQUEST_RESULTS",
            Self::Feedback => "logical_component_sha256.FEEDBACK",
            Self::Finality => "logical_component_sha256.FINALITY",
            Self::Retention => "logical_component_sha256.RETENTION",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThroughputBreakdownV1 {
    pub request_batch_items: u64,
    pub unique_fills: u64,
    pub affected_orders: u64,
    pub order_feedback: u64,
    pub trade_feedback: u64,
    pub settlement_feedback: u64,
    pub query_calls: u64,
    pub query_pages: u64,
    pub query_return_rows: u64,
}

impl ThroughputBreakdownV1 {
    fn checked_add(self, other: Self) -> Result<Self, SimulationMeasurementErrorV1> {
        let add = |left: u64, right: u64| {
            left.checked_add(right)
                .ok_or(SimulationMeasurementErrorV1::ArithmeticOverflow(
                    "throughput_breakdown",
                ))
        };
        Ok(Self {
            request_batch_items: add(self.request_batch_items, other.request_batch_items)?,
            unique_fills: add(self.unique_fills, other.unique_fills)?,
            affected_orders: add(self.affected_orders, other.affected_orders)?,
            order_feedback: add(self.order_feedback, other.order_feedback)?,
            trade_feedback: add(self.trade_feedback, other.trade_feedback)?,
            settlement_feedback: add(self.settlement_feedback, other.settlement_feedback)?,
            query_calls: add(self.query_calls, other.query_calls)?,
            query_pages: add(self.query_pages, other.query_pages)?,
            query_return_rows: add(self.query_return_rows, other.query_return_rows)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrajectoryThroughputV1 {
    pub completed_path_deliveries: u16,
    pub completed_top_level_calls: u16,
    pub breakdown: ThroughputBreakdownV1,
    pub elapsed_ns: u64,
    pub completed_work_items_per_second_milli: u64,
}

impl TrajectoryThroughputV1 {
    fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        if self.completed_path_deliveries != 20
            || self.completed_top_level_calls != 19
            || self.breakdown.request_batch_items != 106
            || self.breakdown.unique_fills == 0
            || self.breakdown.affected_orders == 0
            || self.breakdown.order_feedback == 0
            || self.breakdown.trade_feedback == 0
            || self.breakdown.settlement_feedback == 0
            || self.breakdown.query_calls != 6
            || self.breakdown.query_pages < 2
            || self.breakdown.query_return_rows < 102
            || self.elapsed_ns == 0
            || self.completed_work_items_per_second_milli
                != throughput_per_second_milli(
                    u64::from(self.completed_path_deliveries)
                        + u64::from(self.completed_top_level_calls),
                    self.elapsed_ns,
                )?
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "逐轨迹吞吐计数或派生速率不符合 COMPONENT_GOLDEN_V1".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrialThroughputV1 {
    pub completed_path_deliveries: u16,
    pub completed_top_level_calls: u16,
    pub breakdown: ThroughputBreakdownV1,
    pub elapsed_ns: u64,
    pub completed_work_items_per_second_milli: u64,
}

impl TrialThroughputV1 {
    fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        let completed =
            u64::from(self.completed_path_deliveries) + u64::from(self.completed_top_level_calls);
        if self.completed_path_deliveries > 40 || self.completed_top_level_calls > 38 {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "试次总吞吐计数超过 COMPONENT_GOLDEN_V1".to_owned(),
            ));
        }
        let expected_rate = if self.elapsed_ns == 0 {
            if completed != 0 {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "非空试次吞吐必须记录 elapsed_ns".to_owned(),
                ));
            }
            0
        } else {
            throughput_per_second_milli(completed, self.elapsed_ns)?
        };
        if self.completed_work_items_per_second_milli != expected_rate {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "试次总吞吐派生速率不能由原始计数与 elapsed_ns 重算".to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_complete(
        &self,
        trajectories: &BTreeMap<ComponentGoldenTrajectoryV1, TrajectoryTrialResultV1>,
    ) -> Result<(), SimulationMeasurementErrorV1> {
        self.validate()?;
        if self.completed_path_deliveries != 40 || self.completed_top_level_calls != 38 {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "成功试次必须完成 40 次路径投递与 38 个顶层调用".to_owned(),
            ));
        }
        let expected_breakdown = trajectories
            .values()
            .try_fold(ThroughputBreakdownV1::default(), |total, trajectory| {
                total.checked_add(trajectory.throughput.breakdown)
            })?;
        if self.breakdown != expected_breakdown {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "试次总吞吐分栏不能由两条轨迹原始计数重算".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogicalRollingCycleIdV1 {
    FirstMarket,
    SecondMarket,
}

pub const LOGICAL_ROLLING_CYCLE_IDS_V1: [LogicalRollingCycleIdV1; 2] = [
    LogicalRollingCycleIdV1::FirstMarket,
    LogicalRollingCycleIdV1::SecondMarket,
];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalRollingCycleV1 {
    pub cycle_id: LogicalRollingCycleIdV1,
    pub hot_snapshot_index: u64,
    pub compact_snapshot_index: u64,
    pub expired_snapshot_index: u64,
    pub logical_completion_ns: u64,
    pub hot_state_high_water: ResourceQuantityV1,
    pub compact_projection_high_water: ResourceQuantityV1,
    pub expired_deleted: ResourceQuantityV1,
}

impl LogicalRollingCycleV1 {
    fn validate(
        &self,
        cycle_id: LogicalRollingCycleIdV1,
    ) -> Result<(), SimulationMeasurementErrorV1> {
        if self.cycle_id != cycle_id
            || self.hot_snapshot_index >= self.compact_snapshot_index
            || self.compact_snapshot_index >= self.expired_snapshot_index
            || self.logical_completion_ns == 0
            || self.hot_state_high_water.records == 0
            || self.hot_state_high_water.bytes == 0
            || self.compact_projection_high_water.records == 0
            || self.compact_projection_high_water.bytes == 0
            || self.expired_deleted.records == 0
            || self.expired_deleted.bytes == 0
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "逻辑滚动周期必须按热态、紧凑投影、过期删除顺序闭合".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrajectoryTrialResultV1 {
    pub runtime_binding: TrajectoryRuntimeBindingV1,
    pub logical_summary_input: TrajectoryLogicalSummaryInputV1,
    pub logical_summary_sha256: Sha256Hex,
    pub throughput: TrajectoryThroughputV1,
    pub latency: BTreeMap<MeasurementLatencyKindV1, RawLatencySamplesNsV1>,
    pub rolling_cycles: BTreeMap<LogicalRollingCycleIdV1, LogicalRollingCycleV1>,
}

impl TrajectoryTrialResultV1 {
    fn validate(
        &self,
        trajectory: ComponentGoldenTrajectoryV1,
    ) -> Result<(), SimulationMeasurementErrorV1> {
        self.runtime_binding.validate()?;
        if self.logical_summary_input.trajectory != trajectory {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "逐轨迹结果键与摘要轨迹不一致".to_owned(),
            ));
        }
        let expected_sha256 = self.logical_summary_input.content_sha256()?;
        if self.logical_summary_sha256 != expected_sha256 {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "逐轨迹逻辑摘要 SHA-256 不能由摘要输入重算".to_owned(),
            ));
        }
        self.throughput.validate()?;
        let expected_latency = MEASUREMENT_LATENCY_KINDS_V1.into_iter().collect::<Vec<_>>();
        let actual_latency = self.latency.keys().copied().collect::<Vec<_>>();
        if actual_latency != expected_latency {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "逐轨迹延迟样本必须覆盖六个预登记类别".to_owned(),
            ));
        }
        for samples in self.latency.values() {
            samples.validate()?;
        }
        let expected_cycles = LOGICAL_ROLLING_CYCLE_IDS_V1.into_iter().collect::<Vec<_>>();
        let actual_cycles = self.rolling_cycles.keys().copied().collect::<Vec<_>>();
        if actual_cycles != expected_cycles {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "逐轨迹必须保留恰好两个完整逻辑滚动周期".to_owned(),
            ));
        }
        let mut previous_completion = 0;
        for (cycle_id, cycle) in &self.rolling_cycles {
            cycle.validate(*cycle_id)?;
            if cycle.logical_completion_ns < previous_completion {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "第二个逻辑滚动周期不得先于第一个完成".to_owned(),
                ));
            }
            previous_completion = cycle.logical_completion_ns;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MeasurementFailureStageV1 {
    Setup,
    Workload,
    Measurement,
    Persistence,
    Validation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "status",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum MeasurementTrialOutcomeV1 {
    Succeeded,
    Failed {
        stage: MeasurementFailureStageV1,
        reason: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "kind",
    content = "id",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum MeasurementRunIdentityV1 {
    ComponentTrial(ComponentGoldenTrialIdV1),
    FrozenBudgetVerification(FrozenBudgetVerificationTrialIdV1),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentGoldenRunMeasurementV1 {
    pub schema_version: String,
    pub run_id: String,
    pub measurement_run_identity: MeasurementRunIdentityV1,
    pub workload_contract_sha256: Sha256Hex,
    pub environment: MeasurementEnvironmentIdentityV1,
    pub runtime_hard_guards: MeasurementRuntimeHardGuardsV1,
    pub throughput: TrialThroughputV1,
    /// `CheckpointOrSeal` 在本封口前正文中只记录封口前检查点；最终发布由外层回执绑定。
    pub latency: BTreeMap<MeasurementLatencyKindV1, RawLatencySamplesNsV1>,
    pub resource_snapshots: Vec<MeasuredResourceSnapshotV1>,
    pub transaction_resource_increments: Vec<ResourceTransactionIncrementV1>,
    pub trajectories: BTreeMap<ComponentGoldenTrajectoryV1, TrajectoryTrialResultV1>,
}

impl ComponentGoldenRunMeasurementV1 {
    pub fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        if self.schema_version != COMPONENT_GOLDEN_RUN_MEASUREMENT_SCHEMA_V1
            || self.run_id.trim().is_empty()
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "封口前测量正文的版本或 run_id 非法".to_owned(),
            ));
        }
        validate_sha256(
            &self.workload_contract_sha256,
            "preseal.workload_contract_sha256",
        )?;
        self.environment.validate()?;
        self.runtime_hard_guards.validate()?;
        self.throughput.validate_complete(&self.trajectories)?;
        let expected_latency = MEASUREMENT_LATENCY_KINDS_V1.into_iter().collect::<Vec<_>>();
        let actual_latency = self.latency.keys().copied().collect::<Vec<_>>();
        if actual_latency != expected_latency {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "封口前测量正文必须覆盖六个预登记延迟类别".to_owned(),
            ));
        }
        for samples in self.latency.values() {
            samples.validate()?;
        }
        if self.resource_snapshots.is_empty() || self.transaction_resource_increments.is_empty() {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "封口前测量正文缺少资源快照或逐事务资源增量".to_owned(),
            ));
        }
        validate_resource_snapshot_sequence(&self.resource_snapshots)?;
        for snapshot in &self.resource_snapshots {
            self.runtime_hard_guards.validate_snapshot(snapshot)?;
        }
        for (expected_index, increment) in self.transaction_resource_increments.iter().enumerate() {
            increment.validate(expected_index as u64)?;
        }
        let expected_trajectories = COMPONENT_GOLDEN_TRAJECTORIES_V1
            .into_iter()
            .collect::<Vec<_>>();
        let actual_trajectories = self.trajectories.keys().copied().collect::<Vec<_>>();
        if actual_trajectories != expected_trajectories {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "封口前测量正文必须包含 A、B 两条轨迹".to_owned(),
            ));
        }
        for (trajectory, result) in &self.trajectories {
            result.validate(*trajectory)?;
            validate_rolling_snapshot_references(result, self.resource_snapshots.len())?;
            if result.runtime_binding.account_instance_key.run_id != self.run_id
                || result.logical_summary_input.workload_contract_sha256
                    != self.workload_contract_sha256
            {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "封口前逐轨迹运行身份或摘要未绑定同一运行与负载合同".to_owned(),
                ));
            }
        }
        validate_trajectory_identity_isolation(&self.trajectories)
    }

    pub fn content_sha256(&self) -> Result<Sha256Hex, SimulationMeasurementErrorV1> {
        self.validate()?;
        measurement_hash(COMPONENT_GOLDEN_RUN_MEASUREMENT_HASH_DOMAIN_V1, self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationRunArtifactReceiptBindingV1 {
    pub run_id: String,
    pub measurement_run_identity: MeasurementRunIdentityV1,
    pub manifest_sha256: Sha256Hex,
    pub through_commit_sequence: u64,
    pub evidence_head_sha256: Sha256Hex,
}

impl SimulationRunArtifactReceiptBindingV1 {
    fn validate_for(
        &self,
        expected_identity: MeasurementRunIdentityV1,
    ) -> Result<(), SimulationMeasurementErrorV1> {
        if self.run_id.trim().is_empty() || self.measurement_run_identity != expected_identity {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "运行产物回执未绑定预期测量运行身份".to_owned(),
            ));
        }
        validate_sha256(&self.manifest_sha256, "manifest_sha256")?;
        validate_sha256(&self.evidence_head_sha256, "evidence_head_sha256")?;
        if self.through_commit_sequence == 0 {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "运行产物回执 through_commit_sequence 必须大于零".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedRunResourceEvidenceV1 {
    pub audit_file_sha256: Sha256Hex,
    pub run_artifact_receipt: SimulationRunArtifactReceiptBindingV1,
    pub resource_policy_id: String,
    pub resource_policy_sha256: Sha256Hex,
    pub high_water: SimulationResourceHighWaterV1,
    pub durable_transaction_resource_increments: Vec<ResourceTransactionIncrementV1>,
    pub artifact_publication_increment: BTreeMap<ResourceBudgetPathV1, ResourceQuantityV1>,
    pub postseal_resource_snapshot: MeasuredResourceSnapshotV1,
    pub seal_latency_ns: u64,
}

impl SealedRunResourceEvidenceV1 {
    fn validate_shape(&self) -> Result<(), SimulationMeasurementErrorV1> {
        validate_sha256(&self.audit_file_sha256, "sealed.audit_file_sha256")?;
        if self.resource_policy_id.trim().is_empty() {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "封口后资源证据缺少资源策略身份".to_owned(),
            ));
        }
        validate_sha256(
            &self.resource_policy_sha256,
            "sealed.resource_policy_sha256",
        )?;
        if self.durable_transaction_resource_increments.is_empty() {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "封口后资源证据缺少完整逐事务增量".to_owned(),
            ));
        }
        if self.seal_latency_ns == 0 {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "正式封口延迟必须大于零".to_owned(),
            ));
        }
        self.postseal_resource_snapshot.validate()?;
        let high_water = usage_by_budget_path(&self.high_water.observed);
        validate_budget_paths(&high_water, "封口后资源高水位")?;
        let artifacts = high_water[&ResourceBudgetPathV1::DurableArtifacts];
        let runtime_root = high_water[&ResourceBudgetPathV1::DurableRuntimeRoot];
        if artifacts.records == 0
            || artifacts.bytes == 0
            || runtime_root.records < artifacts.records
            || runtime_root.bytes < artifacts.bytes
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "封口后资源高水位必须包含非空产物，且运行根必须覆盖产物".to_owned(),
            ));
        }
        validate_budget_paths(&self.artifact_publication_increment, "产物发布资源增量")?;
        for path in RESOURCE_BUDGET_PATHS_V1 {
            let increment = self.artifact_publication_increment[&path];
            match path {
                ResourceBudgetPathV1::DurableArtifacts
                | ResourceBudgetPathV1::DurableRuntimeRoot
                    if increment.records == 0 || increment.bytes == 0 =>
                {
                    return Err(SimulationMeasurementErrorV1::ReportMismatch(
                        "产物发布必须对 artifacts 与 runtime_root 记录非零增量".to_owned(),
                    ));
                }
                ResourceBudgetPathV1::DurableArtifacts
                | ResourceBudgetPathV1::DurableRuntimeRoot => {}
                _ if increment != ResourceQuantityV1::default() => {
                    return Err(SimulationMeasurementErrorV1::ReportMismatch(
                        "产物发布增量只能改变 artifacts 与 runtime_root".to_owned(),
                    ));
                }
                _ => {}
            }
            let peak = high_water[&path];
            if increment.records > peak.records || increment.bytes > peak.bytes {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "产物发布资源增量超过对应封口后高水位".to_owned(),
                ));
            }
        }
        if self.postseal_resource_snapshot.internal.high_water != self.high_water {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "封口后资源快照未绑定审计提取的最终内部高水位".to_owned(),
            ));
        }
        for (expected_index, increment) in self
            .durable_transaction_resource_increments
            .iter()
            .enumerate()
        {
            increment.validate(expected_index as u64)?;
            for (path, quantity) in &increment.increments {
                let peak = high_water[path];
                if quantity.records > peak.records || quantity.bytes > peak.bytes {
                    return Err(SimulationMeasurementErrorV1::ReportMismatch(
                        "封口后逐事务资源增量超过对应资源高水位".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_for(
        &self,
        expected_identity: MeasurementRunIdentityV1,
        receipt: &SimulationRunArtifactReceiptBindingV1,
        preseal: &ComponentGoldenRunMeasurementV1,
    ) -> Result<(), SimulationMeasurementErrorV1> {
        self.validate_shape()?;
        self.run_artifact_receipt.validate_for(expected_identity)?;
        if &self.run_artifact_receipt != receipt {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "封口后资源证据与运行产物回执的 manifest、提交序号或证据头不一致".to_owned(),
            ));
        }
        let last_preseal = preseal.resource_snapshots.last().ok_or_else(|| {
            SimulationMeasurementErrorV1::ReportMismatch(
                "封口后资源证据缺少可衔接的封口前快照".to_owned(),
            )
        })?;
        if self.postseal_resource_snapshot.sample_index != last_preseal.sample_index + 1 {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "封口后资源快照序号未连续衔接封口前快照".to_owned(),
            ));
        }
        self.postseal_resource_snapshot
            .validate_follows(last_preseal)?;
        preseal
            .runtime_hard_guards
            .validate_snapshot(&self.postseal_resource_snapshot)?;
        let preseal_count = preseal.transaction_resource_increments.len();
        if self.durable_transaction_resource_increments.len() <= preseal_count
            || self.durable_transaction_resource_increments[..preseal_count]
                != preseal.transaction_resource_increments
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "封口后完整逐事务资源增量必须原样扩展封口前事务前缀".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentGoldenTrialReportV1 {
    pub trial_id: ComponentGoldenTrialIdV1,
    pub mode: ComponentGoldenMeasurementModeV1,
    pub worker_count: u16,
    pub discarded_warmup_samples: u64,
    pub preseal_measurement: Option<ComponentGoldenRunMeasurementV1>,
    pub run_artifact_receipt: Option<SimulationRunArtifactReceiptBindingV1>,
    pub sealed_resource_evidence: Option<SealedRunResourceEvidenceV1>,
    pub outcome: MeasurementTrialOutcomeV1,
    pub throughput: TrialThroughputV1,
    pub latency: BTreeMap<MeasurementLatencyKindV1, RawLatencySamplesNsV1>,
    pub resource_snapshots: Vec<MeasuredResourceSnapshotV1>,
    pub transaction_resource_increments: Vec<ResourceTransactionIncrementV1>,
    pub trajectories: BTreeMap<ComponentGoldenTrajectoryV1, TrajectoryTrialResultV1>,
}

impl ComponentGoldenTrialReportV1 {
    fn validate_integrity(&self) -> Result<(), SimulationMeasurementErrorV1> {
        if self.mode != self.trial_id.mode()
            || self.worker_count != self.trial_id.worker_count()
            || self.discarded_warmup_samples != 0
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "试次模式、工作线程数或预热样本处理不符合预登记调度".to_owned(),
            ));
        }
        let expected_identity = MeasurementRunIdentityV1::ComponentTrial(self.trial_id);
        match (
            &self.outcome,
            &self.preseal_measurement,
            &self.run_artifact_receipt,
            &self.sealed_resource_evidence,
        ) {
            (MeasurementTrialOutcomeV1::Succeeded, Some(preseal), Some(receipt), Some(sealed)) => {
                preseal.validate()?;
                if preseal.measurement_run_identity != expected_identity
                    || preseal.run_id != receipt.run_id
                    || preseal.throughput != self.throughput
                    || preseal.resource_snapshots != self.resource_snapshots
                    || preseal.transaction_resource_increments
                        != self.transaction_resource_increments
                    || preseal.trajectories != self.trajectories
                {
                    return Err(SimulationMeasurementErrorV1::ReportMismatch(
                        "试次正文与封口前测量正文不一致".to_owned(),
                    ));
                }
                receipt.validate_for(expected_identity)?;
                sealed.validate_for(expected_identity, receipt, preseal)?;
                validate_outer_latency_binding(preseal, Some(sealed), &self.latency)?;
            }
            (MeasurementTrialOutcomeV1::Succeeded, _, _, _) => {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "成功试次必须保留封口前测量、运行产物回执与封口后资源证据".to_owned(),
                ));
            }
            (MeasurementTrialOutcomeV1::Failed { reason, .. }, preseal, receipt, sealed) => {
                if reason.trim().is_empty() {
                    return Err(SimulationMeasurementErrorV1::ReportMismatch(
                        "失败试次必须保留非空失败原因".to_owned(),
                    ));
                }
                if let Some(preseal) = preseal {
                    preseal.validate()?;
                    if preseal.measurement_run_identity != expected_identity
                        || preseal.throughput != self.throughput
                        || preseal.resource_snapshots != self.resource_snapshots
                        || preseal.transaction_resource_increments
                            != self.transaction_resource_increments
                        || preseal.trajectories != self.trajectories
                        || receipt
                            .as_ref()
                            .is_some_and(|receipt| receipt.run_id != preseal.run_id)
                    {
                        return Err(SimulationMeasurementErrorV1::ReportMismatch(
                            "失败试次正文与已有封口前测量正文不一致".to_owned(),
                        ));
                    }
                    validate_outer_latency_binding(preseal, sealed.as_ref(), &self.latency)?;
                }
                if let Some(receipt) = receipt {
                    receipt.validate_for(expected_identity)?;
                }
                match (sealed, preseal, receipt) {
                    (Some(sealed), Some(preseal), Some(receipt)) => {
                        sealed.validate_for(expected_identity, receipt, preseal)?;
                    }
                    (Some(_), _, _) => {
                        return Err(SimulationMeasurementErrorV1::ReportMismatch(
                            "失败试次保留封口后资源证据时也必须保留其封口前正文与回执".to_owned(),
                        ));
                    }
                    (None, _, _) => {}
                }
            }
        }
        self.throughput.validate()?;
        for samples in self.latency.values() {
            samples.validate()?;
        }
        for (expected_index, increment) in self.transaction_resource_increments.iter().enumerate() {
            increment.validate(expected_index as u64)?;
        }
        validate_resource_snapshot_sequence(&self.resource_snapshots)?;
        for (trajectory, result) in &self.trajectories {
            result.validate(*trajectory)?;
            validate_rolling_snapshot_references(result, self.resource_snapshots.len())?;
        }
        Ok(())
    }

    fn validate_succeeded(&self) -> Result<(), SimulationMeasurementErrorV1> {
        if !matches!(self.outcome, MeasurementTrialOutcomeV1::Succeeded) {
            return Err(SimulationMeasurementErrorV1::IncompleteTrial(self.trial_id));
        }
        if self.resource_snapshots.is_empty() || self.transaction_resource_increments.is_empty() {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "成功试次缺少延迟原始样本或资源快照".to_owned(),
            ));
        }
        if self.sealed_resource_evidence.is_none() {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "成功试次缺少封口后资源证据".to_owned(),
            ));
        }
        self.throughput.validate_complete(&self.trajectories)?;
        let expected_latency = MEASUREMENT_LATENCY_KINDS_V1.into_iter().collect::<Vec<_>>();
        let actual_latency = self.latency.keys().copied().collect::<Vec<_>>();
        if actual_latency != expected_latency {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "成功试次延迟样本必须覆盖六个预登记类别".to_owned(),
            ));
        }
        let expected = COMPONENT_GOLDEN_TRAJECTORIES_V1
            .into_iter()
            .collect::<Vec<_>>();
        let actual = self.trajectories.keys().copied().collect::<Vec<_>>();
        if actual != expected {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "成功试次必须包含 A、B 两条轨迹".to_owned(),
            ));
        }
        validate_trajectory_identity_isolation(&self.trajectories)
    }
}

fn validate_trajectory_identity_isolation(
    trajectories: &BTreeMap<ComponentGoldenTrajectoryV1, TrajectoryTrialResultV1>,
) -> Result<(), SimulationMeasurementErrorV1> {
    let Some(a) = trajectories
        .get(&ComponentGoldenTrajectoryV1::A)
        .map(|result| &result.runtime_binding)
    else {
        return Err(SimulationMeasurementErrorV1::ReportMismatch(
            "测量正文缺少 A 轨迹".to_owned(),
        ));
    };
    let Some(b) = trajectories
        .get(&ComponentGoldenTrajectoryV1::B)
        .map(|result| &result.runtime_binding)
    else {
        return Err(SimulationMeasurementErrorV1::ReportMismatch(
            "测量正文缺少 B 轨迹".to_owned(),
        ));
    };
    if a.session_id == b.session_id
        || a.session_seed_sha256 == b.session_seed_sha256
        || a.response_bridge_binding_id == b.response_bridge_binding_id
        || a.account_instance_key == b.account_instance_key
    {
        return Err(SimulationMeasurementErrorV1::ReportMismatch(
            "A、B 轨迹的身份、种子、桥和账户必须分别不同".to_owned(),
        ));
    }
    Ok(())
}

fn validate_outer_latency_binding(
    preseal: &ComponentGoldenRunMeasurementV1,
    sealed: Option<&SealedRunResourceEvidenceV1>,
    outer: &BTreeMap<MeasurementLatencyKindV1, RawLatencySamplesNsV1>,
) -> Result<(), SimulationMeasurementErrorV1> {
    let mut expected = preseal.latency.clone();
    if let Some(sealed) = sealed {
        let checkpoint_or_seal = expected
            .get_mut(&MeasurementLatencyKindV1::CheckpointOrSeal)
            .ok_or_else(|| {
                SimulationMeasurementErrorV1::ReportMismatch(
                    "封口前延迟缺少 CheckpointOrSeal 类别".to_owned(),
                )
            })?;
        checkpoint_or_seal.samples_ns.push(sealed.seal_latency_ns);
        checkpoint_or_seal.nearest_rank =
            nearest_rank_latency_ns_v1(&checkpoint_or_seal.samples_ns)?;
    }
    if outer != &expected {
        return Err(SimulationMeasurementErrorV1::ReportMismatch(
            "外层延迟必须保持封口前样本，并只向 CheckpointOrSeal 追加正式封口样本".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementHostIdentityV1 {
    pub machine_identity_sha256: Sha256Hex,
    pub boot_identity_sha256: Sha256Hex,
    pub os_release_sha256: Sha256Hex,
    pub kernel_release: String,
    pub architecture: String,
    pub online_cpu_count: u32,
    pub page_size_bytes: u64,
    pub physical_memory_bytes: u64,
    pub swap_total_bytes: u64,
}

impl MeasurementHostIdentityV1 {
    fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        validate_sha256(&self.machine_identity_sha256, "machine_identity_sha256")?;
        validate_sha256(&self.boot_identity_sha256, "boot_identity_sha256")?;
        validate_sha256(&self.os_release_sha256, "os_release_sha256")?;
        if self.kernel_release.trim().is_empty()
            || self.architecture.trim().is_empty()
            || self.online_cpu_count == 0
            || self.page_size_bytes == 0
            || self.physical_memory_bytes == 0
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "主机身份字段非法".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementBuildIdentityV1 {
    pub release_binary_sha256: Sha256Hex,
    pub source_tree_sha256: Sha256Hex,
    pub cargo_lock_sha256: Sha256Hex,
    pub source_commit: String,
    pub rustc_version: String,
    pub target_triple: String,
    pub cargo_profile: String,
}

impl MeasurementBuildIdentityV1 {
    fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        validate_sha256(&self.release_binary_sha256, "release_binary_sha256")?;
        validate_sha256(&self.source_tree_sha256, "source_tree_sha256")?;
        validate_sha256(&self.cargo_lock_sha256, "cargo_lock_sha256")?;
        if !is_lower_hex(&self.source_commit, 40)
            || self.rustc_version.trim().is_empty()
            || self.target_triple.trim().is_empty()
            || self.cargo_profile != "release"
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "发布构建身份字段非法".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementFilesystemIdentityV1 {
    pub measurement_root_sha256: Sha256Hex,
    pub mount_identity_sha256: Sha256Hex,
    pub filesystem_type: String,
    pub block_size_bytes: u64,
    pub total_bytes: u64,
    pub available_bytes_before: u64,
}

impl MeasurementFilesystemIdentityV1 {
    fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        validate_sha256(&self.measurement_root_sha256, "measurement_root_sha256")?;
        validate_sha256(&self.mount_identity_sha256, "mount_identity_sha256")?;
        if self.filesystem_type.trim().is_empty()
            || self.block_size_bytes == 0
            || self.total_bytes == 0
            || self.available_bytes_before > self.total_bytes
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "测量文件系统身份字段非法".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementCachePreconditionV1 {
    pub schema_version: String,
    pub release_unit_test_binary_precompiled_outside_measurement_scope: bool,
    pub cargo_locked: bool,
    pub cargo_offline: bool,
    pub discarded_warmup_trials: u16,
}

impl MeasurementCachePreconditionV1 {
    pub fn formal_v1() -> Self {
        Self {
            schema_version: MEASUREMENT_CACHE_PRECONDITION_SCHEMA_V1.to_owned(),
            release_unit_test_binary_precompiled_outside_measurement_scope: true,
            cargo_locked: true,
            cargo_offline: true,
            discarded_warmup_trials: 0,
        }
    }

    pub fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        if self != &Self::formal_v1() {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "正式测量缓存前提必须证明发布测试二进制已在计量 scope 外预编译，并固定 locked、offline 与零预热丢弃"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    pub fn content_sha256(&self) -> Result<Sha256Hex, SimulationMeasurementErrorV1> {
        self.validate()?;
        measurement_hash(MEASUREMENT_CACHE_PRECONDITION_HASH_DOMAIN_V1, self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementEnvironmentIdentityV1 {
    pub host: MeasurementHostIdentityV1,
    pub build: MeasurementBuildIdentityV1,
    pub filesystem: MeasurementFilesystemIdentityV1,
    pub cgroup_path_sha256: Sha256Hex,
    pub input_stream_sha256: Sha256Hex,
    pub runner_sha256: Sha256Hex,
    pub cache_precondition: MeasurementCachePreconditionV1,
    pub cache_precondition_sha256: Sha256Hex,
}

impl MeasurementEnvironmentIdentityV1 {
    fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        self.host.validate()?;
        self.build.validate()?;
        self.filesystem.validate()?;
        validate_sha256(&self.cgroup_path_sha256, "cgroup_path_sha256")?;
        validate_sha256(&self.input_stream_sha256, "input_stream_sha256")?;
        validate_sha256(&self.runner_sha256, "runner_sha256")?;
        self.cache_precondition.validate()?;
        validate_sha256(&self.cache_precondition_sha256, "cache_precondition_sha256")?;
        if self.cache_precondition_sha256 != self.cache_precondition.content_sha256()? {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "缓存前提 SHA-256 不能由固定正文重算".to_owned(),
            ));
        }
        Ok(())
    }

    fn has_same_fixed_context_as(&self, other: &Self) -> bool {
        self.build == other.build && self.has_same_cross_build_context_as(other)
    }

    fn has_same_cross_build_context_as(&self, other: &Self) -> bool {
        self.host == other.host
            && self.filesystem.measurement_root_sha256 == other.filesystem.measurement_root_sha256
            && self.filesystem.mount_identity_sha256 == other.filesystem.mount_identity_sha256
            && self.filesystem.filesystem_type == other.filesystem.filesystem_type
            && self.filesystem.block_size_bytes == other.filesystem.block_size_bytes
            && self.filesystem.total_bytes == other.filesystem.total_bytes
            && self.input_stream_sha256 == other.input_stream_sha256
            && self.runner_sha256 == other.runner_sha256
            && self.cache_precondition == other.cache_precondition
            && self.cache_precondition_sha256 == other.cache_precondition_sha256
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResourceBudgetPathV1 {
    SessionInMemoryState,
    SessionHotMarkets,
    SessionInputQueue,
    SessionRequestQueue,
    SessionFeedbackQueue,
    SessionActiveOrders,
    SessionInternalEvents,
    SessionQueryCache,
    SessionIndexes,
    RunInMemoryState,
    RunSessions,
    RunHotMarkets,
    RunInputQueue,
    RunRequestQueue,
    RunFeedbackQueue,
    RunActiveOrders,
    RunInternalEvents,
    RunQueryCache,
    RunIndexes,
    RunCapitalFence,
    DurableEvidence,
    DurableCheckpoints,
    DurableArtifacts,
    DurableRuntimeRoot,
}

pub const RESOURCE_BUDGET_PATHS_V1: [ResourceBudgetPathV1; 24] = [
    ResourceBudgetPathV1::SessionInMemoryState,
    ResourceBudgetPathV1::SessionHotMarkets,
    ResourceBudgetPathV1::SessionInputQueue,
    ResourceBudgetPathV1::SessionRequestQueue,
    ResourceBudgetPathV1::SessionFeedbackQueue,
    ResourceBudgetPathV1::SessionActiveOrders,
    ResourceBudgetPathV1::SessionInternalEvents,
    ResourceBudgetPathV1::SessionQueryCache,
    ResourceBudgetPathV1::SessionIndexes,
    ResourceBudgetPathV1::RunInMemoryState,
    ResourceBudgetPathV1::RunSessions,
    ResourceBudgetPathV1::RunHotMarkets,
    ResourceBudgetPathV1::RunInputQueue,
    ResourceBudgetPathV1::RunRequestQueue,
    ResourceBudgetPathV1::RunFeedbackQueue,
    ResourceBudgetPathV1::RunActiveOrders,
    ResourceBudgetPathV1::RunInternalEvents,
    ResourceBudgetPathV1::RunQueryCache,
    ResourceBudgetPathV1::RunIndexes,
    ResourceBudgetPathV1::RunCapitalFence,
    ResourceBudgetPathV1::DurableEvidence,
    ResourceBudgetPathV1::DurableCheckpoints,
    ResourceBudgetPathV1::DurableArtifacts,
    ResourceBudgetPathV1::DurableRuntimeRoot,
];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceTransactionIncrementV1 {
    pub transaction_index: u64,
    pub increments: BTreeMap<ResourceBudgetPathV1, ResourceQuantityV1>,
}

impl ResourceTransactionIncrementV1 {
    fn validate(&self, expected_index: u64) -> Result<(), SimulationMeasurementErrorV1> {
        if self.transaction_index != expected_index {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "逐事务资源增量序号不连续".to_owned(),
            ));
        }
        validate_budget_paths(&self.increments, "逐事务资源增量")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceBudgetObservationV1 {
    pub peak: ResourceQuantityV1,
    pub max_single_transaction_increment: ResourceQuantityV1,
}

pub fn derive_resource_budget_observations_v1(
    trials: &BTreeMap<ComponentGoldenTrialIdV1, ComponentGoldenTrialReportV1>,
) -> Result<BTreeMap<ResourceBudgetPathV1, ResourceBudgetObservationV1>, SimulationMeasurementErrorV1>
{
    let mut observations = RESOURCE_BUDGET_PATHS_V1
        .into_iter()
        .map(|path| {
            (
                path,
                ResourceBudgetObservationV1 {
                    peak: ResourceQuantityV1::default(),
                    max_single_transaction_increment: ResourceQuantityV1::default(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut sealed_high_water_count = 0_u64;
    let mut increment_count = 0_u64;
    for trial in trials.values() {
        let Some(sealed) = &trial.sealed_resource_evidence else {
            continue;
        };
        sealed.validate_shape()?;
        sealed_high_water_count = sealed_high_water_count.checked_add(1).ok_or(
            SimulationMeasurementErrorV1::ArithmeticOverflow("sealed_high_water_count"),
        )?;
        let raw = usage_by_budget_path(&sealed.high_water.observed);
        for (path, quantity) in raw {
            let observation = observations.get_mut(&path).ok_or_else(|| {
                SimulationMeasurementErrorV1::BudgetMismatch(
                    "封口后高水位含未知预算路径".to_owned(),
                )
            })?;
            observe_quantity_max(&mut observation.peak, quantity);
        }
        for increment in &sealed.durable_transaction_resource_increments {
            increment_count = increment_count.checked_add(1).ok_or(
                SimulationMeasurementErrorV1::ArithmeticOverflow("transaction_increment_count"),
            )?;
            validate_budget_paths(&increment.increments, "逐事务资源增量")?;
            for (path, quantity) in &increment.increments {
                let observation = observations.get_mut(path).ok_or_else(|| {
                    SimulationMeasurementErrorV1::BudgetMismatch(
                        "逐事务增量含未知预算路径".to_owned(),
                    )
                })?;
                observe_quantity_max(&mut observation.max_single_transaction_increment, *quantity);
            }
        }
        increment_count = increment_count.checked_add(1).ok_or(
            SimulationMeasurementErrorV1::ArithmeticOverflow("artifact_increment_count"),
        )?;
        for (path, quantity) in &sealed.artifact_publication_increment {
            let observation = observations.get_mut(path).ok_or_else(|| {
                SimulationMeasurementErrorV1::BudgetMismatch(
                    "产物发布增量含未知预算路径".to_owned(),
                )
            })?;
            observe_quantity_max(&mut observation.max_single_transaction_increment, *quantity);
        }
    }
    if sealed_high_water_count == 0 || increment_count == 0 {
        return Err(SimulationMeasurementErrorV1::BudgetMismatch(
            "不能从空封口后高水位或空完整逐事务增量推导 Pmax/Amax".to_owned(),
        ));
    }
    let artifacts = observations
        .get(&ResourceBudgetPathV1::DurableArtifacts)
        .ok_or_else(|| {
            SimulationMeasurementErrorV1::BudgetMismatch("资源预算观测缺少耐久产物路径".to_owned())
        })?;
    if artifacts.peak.records == 0
        || artifacts.peak.bytes == 0
        || artifacts.max_single_transaction_increment.records == 0
        || artifacts.max_single_transaction_increment.bytes == 0
    {
        return Err(SimulationMeasurementErrorV1::BudgetMismatch(
            "封口后耐久产物的 Pmax 与 Amax 必须均为非零实测值".to_owned(),
        ));
    }
    Ok(observations)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenResourceBudgetsV1 {
    pub limits: BTreeMap<ResourceBudgetPathV1, ResourceLimitV1>,
    pub process_memory_max_bytes: u64,
    pub failure_reserve: SimulationFailureEvidenceReserveV1,
}

impl FrozenResourceBudgetsV1 {
    fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        validate_budget_paths(&self.limits, "冻结资源预算")?;
        if self.process_memory_max_bytes == 0
            || !self.process_memory_max_bytes.is_multiple_of(SIXTY_FOUR_MIB)
            || self
                .limits
                .values()
                .any(|limit| !limit.max_bytes.is_multiple_of(FOUR_KIB))
        {
            return Err(SimulationMeasurementErrorV1::BudgetMismatch(
                "冻结字节上限必须按 4 KiB、进程内存上限必须按 64 MiB 对齐".to_owned(),
            ));
        }
        for (name, reserve, path) in [
            (
                "evidence",
                self.failure_reserve.evidence,
                ResourceBudgetPathV1::DurableEvidence,
            ),
            (
                "checkpoints",
                self.failure_reserve.checkpoints,
                ResourceBudgetPathV1::DurableCheckpoints,
            ),
            (
                "runtime_root",
                self.failure_reserve.runtime_root,
                ResourceBudgetPathV1::DurableRuntimeRoot,
            ),
        ] {
            let limit = self.limit(path)?;
            if reserve.records == 0 || reserve.bytes == 0 {
                return Err(SimulationMeasurementErrorV1::BudgetMismatch(format!(
                    "失败证据保留 {name} 的实测记录和字节增量必须均大于零"
                )));
            }
            if reserve.records > limit.max_records || reserve.bytes > limit.max_bytes {
                return Err(SimulationMeasurementErrorV1::BudgetMismatch(format!(
                    "失败证据保留 {name} 的实测增量超过对应硬上限"
                )));
            }
        }
        Ok(())
    }

    fn limit(
        &self,
        path: ResourceBudgetPathV1,
    ) -> Result<ResourceLimitV1, SimulationMeasurementErrorV1> {
        self.limits.get(&path).copied().ok_or_else(|| {
            SimulationMeasurementErrorV1::BudgetMismatch("冻结资源预算缺少必需路径".to_owned())
        })
    }

    pub fn content_sha256(&self) -> Result<Sha256Hex, SimulationMeasurementErrorV1> {
        self.validate()?;
        measurement_hash(FROZEN_RESOURCE_BUDGETS_HASH_DOMAIN_V1, self)
    }

    pub fn to_resource_policy_v1(
        &self,
    ) -> Result<SimulationResourcePolicyV1, SimulationMeasurementErrorV1> {
        self.validate()?;
        Ok(SimulationResourcePolicyV1 {
            schema_version: SIMULATION_RESOURCE_POLICY_SCHEMA_V1.to_owned(),
            policy_id: COMPONENT_GOLDEN_RESOURCE_POLICY_ID_V1.to_owned(),
            session: SimulationSessionResourcePolicyV1 {
                in_memory_state: self.limit(ResourceBudgetPathV1::SessionInMemoryState)?,
                hot_markets: self.limit(ResourceBudgetPathV1::SessionHotMarkets)?,
                input_queue: self.limit(ResourceBudgetPathV1::SessionInputQueue)?,
                request_queue: self.limit(ResourceBudgetPathV1::SessionRequestQueue)?,
                feedback_queue: self.limit(ResourceBudgetPathV1::SessionFeedbackQueue)?,
                active_orders: self.limit(ResourceBudgetPathV1::SessionActiveOrders)?,
                internal_events: self.limit(ResourceBudgetPathV1::SessionInternalEvents)?,
                query_cache: self.limit(ResourceBudgetPathV1::SessionQueryCache)?,
                indexes: self.limit(ResourceBudgetPathV1::SessionIndexes)?,
            },
            run: SimulationRunResourcePolicyV1 {
                in_memory_state: self.limit(ResourceBudgetPathV1::RunInMemoryState)?,
                sessions: self.limit(ResourceBudgetPathV1::RunSessions)?,
                hot_markets: self.limit(ResourceBudgetPathV1::RunHotMarkets)?,
                input_queue: self.limit(ResourceBudgetPathV1::RunInputQueue)?,
                request_queue: self.limit(ResourceBudgetPathV1::RunRequestQueue)?,
                feedback_queue: self.limit(ResourceBudgetPathV1::RunFeedbackQueue)?,
                active_orders: self.limit(ResourceBudgetPathV1::RunActiveOrders)?,
                internal_events: self.limit(ResourceBudgetPathV1::RunInternalEvents)?,
                query_cache: self.limit(ResourceBudgetPathV1::RunQueryCache)?,
                indexes: self.limit(ResourceBudgetPathV1::RunIndexes)?,
                capital_fence: self.limit(ResourceBudgetPathV1::RunCapitalFence)?,
            },
            durable: SimulationDurableResourcePolicyV1 {
                evidence: self.limit(ResourceBudgetPathV1::DurableEvidence)?,
                checkpoints: self.limit(ResourceBudgetPathV1::DurableCheckpoints)?,
                artifacts: self.limit(ResourceBudgetPathV1::DurableArtifacts)?,
                runtime_root: self.limit(ResourceBudgetPathV1::DurableRuntimeRoot)?,
                failure_reserve: self.failure_reserve,
            },
        })
    }
}

pub fn freeze_resource_budgets_v1(
    observations: &BTreeMap<ResourceBudgetPathV1, ResourceBudgetObservationV1>,
    process_memory_peak_bytes: u64,
    failure_reserve: SimulationFailureEvidenceReserveV1,
) -> Result<FrozenResourceBudgetsV1, SimulationMeasurementErrorV1> {
    validate_budget_paths(observations, "资源预算观测")?;
    if process_memory_peak_bytes == 0 {
        return Err(SimulationMeasurementErrorV1::BudgetMismatch(
            "进程内存峰值必须大于零".to_owned(),
        ));
    }

    let mut limits = BTreeMap::new();
    for (path, observation) in observations {
        let max_records = padded_peak(
            observation.peak.records,
            observation.max_single_transaction_increment.records,
        )?;
        let raw_bytes = padded_peak(
            observation.peak.bytes,
            observation.max_single_transaction_increment.bytes,
        )?;
        let max_bytes = round_up(raw_bytes, FOUR_KIB)?;
        limits.insert(*path, ResourceLimitV1::new(max_records, max_bytes));
    }
    let frozen = FrozenResourceBudgetsV1 {
        limits,
        process_memory_max_bytes: round_up(process_memory_peak_bytes, SIXTY_FOUR_MIB)?,
        failure_reserve,
    };
    frozen.validate()?;
    Ok(frozen)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenBudgetVerificationTrialV1 {
    pub verification_id: FrozenBudgetVerificationTrialIdV1,
    pub mode: ComponentGoldenMeasurementModeV1,
    pub worker_count: u16,
    pub discarded_warmup_samples: u64,
    pub workload_contract_sha256: Sha256Hex,
    pub frozen_budgets_sha256: Sha256Hex,
    pub preseal_measurement: Option<ComponentGoldenRunMeasurementV1>,
    pub run_artifact_receipt: Option<SimulationRunArtifactReceiptBindingV1>,
    pub sealed_resource_evidence: Option<SealedRunResourceEvidenceV1>,
    pub outcome: MeasurementTrialOutcomeV1,
    pub throughput: TrialThroughputV1,
    pub latency: BTreeMap<MeasurementLatencyKindV1, RawLatencySamplesNsV1>,
    pub resource_snapshots: Vec<MeasuredResourceSnapshotV1>,
    pub trajectories: BTreeMap<ComponentGoldenTrajectoryV1, TrajectoryTrialResultV1>,
}

impl FrozenBudgetVerificationTrialV1 {
    fn validate_integrity(
        &self,
        workload_contract_sha256: &Sha256Hex,
        frozen_budgets_sha256: &Sha256Hex,
    ) -> Result<(), SimulationMeasurementErrorV1> {
        if self.mode != self.verification_id.mode()
            || self.worker_count != self.verification_id.worker_count()
            || self.discarded_warmup_samples != 0
            || &self.workload_contract_sha256 != workload_contract_sha256
            || &self.frozen_budgets_sha256 != frozen_budgets_sha256
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "冻结后验证试次的模式、负载或预算绑定不一致".to_owned(),
            ));
        }
        validate_sha256(
            &self.workload_contract_sha256,
            "verification.workload_contract_sha256",
        )?;
        validate_sha256(
            &self.frozen_budgets_sha256,
            "verification.frozen_budgets_sha256",
        )?;
        let expected_identity =
            MeasurementRunIdentityV1::FrozenBudgetVerification(self.verification_id);
        match (
            &self.outcome,
            &self.preseal_measurement,
            &self.run_artifact_receipt,
            &self.sealed_resource_evidence,
        ) {
            (MeasurementTrialOutcomeV1::Succeeded, Some(preseal), Some(receipt), Some(sealed)) => {
                preseal.validate()?;
                if preseal.measurement_run_identity != expected_identity
                    || preseal.run_id != receipt.run_id
                    || preseal.workload_contract_sha256 != self.workload_contract_sha256
                    || preseal.throughput != self.throughput
                    || preseal.resource_snapshots != self.resource_snapshots
                    || preseal.trajectories != self.trajectories
                {
                    return Err(SimulationMeasurementErrorV1::ReportMismatch(
                        "冻结后验证正文与封口前测量正文不一致".to_owned(),
                    ));
                }
                receipt.validate_for(expected_identity)?;
                sealed.validate_for(expected_identity, receipt, preseal)?;
                validate_outer_latency_binding(preseal, Some(sealed), &self.latency)?;
            }
            (MeasurementTrialOutcomeV1::Succeeded, _, _, _) => {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "冻结后成功验证试次必须保留封口前测量、运行产物回执与封口后资源证据".to_owned(),
                ));
            }
            (MeasurementTrialOutcomeV1::Failed { reason, .. }, preseal, receipt, sealed) => {
                if reason.trim().is_empty() {
                    return Err(SimulationMeasurementErrorV1::ReportMismatch(
                        "冻结后失败验证试次必须保留非空失败原因".to_owned(),
                    ));
                }
                if let Some(preseal) = preseal {
                    preseal.validate()?;
                    if preseal.measurement_run_identity != expected_identity
                        || preseal.workload_contract_sha256 != self.workload_contract_sha256
                        || preseal.throughput != self.throughput
                        || preseal.resource_snapshots != self.resource_snapshots
                        || preseal.trajectories != self.trajectories
                        || receipt
                            .as_ref()
                            .is_some_and(|receipt| receipt.run_id != preseal.run_id)
                    {
                        return Err(SimulationMeasurementErrorV1::ReportMismatch(
                            "冻结后失败验证正文与已有封口前测量正文不一致".to_owned(),
                        ));
                    }
                    validate_outer_latency_binding(preseal, sealed.as_ref(), &self.latency)?;
                }
                if let Some(receipt) = receipt {
                    receipt.validate_for(expected_identity)?;
                }
                match (sealed, preseal, receipt) {
                    (Some(sealed), Some(preseal), Some(receipt)) => {
                        sealed.validate_for(expected_identity, receipt, preseal)?;
                    }
                    (Some(_), _, _) => {
                        return Err(SimulationMeasurementErrorV1::ReportMismatch(
                            "冻结后失败试次保留封口后资源证据时也必须保留其封口前正文与回执"
                                .to_owned(),
                        ));
                    }
                    (None, _, _) => {}
                }
            }
        }
        self.throughput.validate()?;
        for samples in self.latency.values() {
            samples.validate()?;
        }
        validate_resource_snapshot_sequence(&self.resource_snapshots)?;
        for (trajectory, result) in &self.trajectories {
            result.validate(*trajectory)?;
            validate_rolling_snapshot_references(result, self.resource_snapshots.len())?;
            if result.logical_summary_input.workload_contract_sha256
                != self.workload_contract_sha256
            {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "冻结后逐轨迹摘要未绑定同一负载合同".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn validate_complete(
        &self,
        workload_contract_sha256: &Sha256Hex,
        frozen_budgets_sha256: &Sha256Hex,
        frozen_budgets: &FrozenResourceBudgetsV1,
        stable_summaries: &BTreeMap<ComponentGoldenTrajectoryV1, Sha256Hex>,
        stable_seeds: &BTreeMap<ComponentGoldenTrajectoryV1, Sha256Hex>,
    ) -> Result<(), SimulationMeasurementErrorV1> {
        self.validate_integrity(workload_contract_sha256, frozen_budgets_sha256)?;
        if !matches!(self.outcome, MeasurementTrialOutcomeV1::Succeeded) {
            return Err(SimulationMeasurementErrorV1::IncompleteVerificationTrial(
                self.verification_id,
            ));
        }
        self.throughput.validate_complete(&self.trajectories)?;
        let expected_latency = MEASUREMENT_LATENCY_KINDS_V1.into_iter().collect::<Vec<_>>();
        let actual_latency = self.latency.keys().copied().collect::<Vec<_>>();
        if actual_latency != expected_latency || self.resource_snapshots.is_empty() {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "冻结后成功验证试次缺少完整延迟或资源样本".to_owned(),
            ));
        }
        let expected_trajectories = COMPONENT_GOLDEN_TRAJECTORIES_V1
            .into_iter()
            .collect::<Vec<_>>();
        let actual_trajectories = self.trajectories.keys().copied().collect::<Vec<_>>();
        if actual_trajectories != expected_trajectories {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "冻结后成功验证试次必须包含 A、B 两条轨迹".to_owned(),
            ));
        }
        for (trajectory, result) in &self.trajectories {
            if stable_summaries.get(trajectory) != Some(&result.logical_summary_sha256) {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "冻结后串行或并行验证的逐轨迹逻辑摘要发生变化".to_owned(),
                ));
            }
            if stable_seeds.get(trajectory) != Some(&result.runtime_binding.session_seed_sha256) {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "冻结后串行或并行验证的逐轨迹会话种子发生变化".to_owned(),
                ));
            }
        }
        let sealed = self.sealed_resource_evidence.as_ref().ok_or_else(|| {
            SimulationMeasurementErrorV1::ReportMismatch(
                "冻结后成功验证试次缺少封口后资源证据".to_owned(),
            )
        })?;
        validate_high_water_within_frozen_budgets(&sealed.high_water, frozen_budgets)?;
        for snapshot in &self.resource_snapshots {
            validate_process_snapshot_within_frozen_budgets(snapshot, frozen_budgets)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentGoldenMeasurementReportV1 {
    pub schema_version: String,
    pub workload_contract: ComponentGoldenLoadContractV1,
    pub workload_contract_sha256: Sha256Hex,
    pub environment: MeasurementEnvironmentIdentityV1,
    pub runtime_hard_guards: MeasurementRuntimeHardGuardsV1,
    pub trials: BTreeMap<ComponentGoldenTrialIdV1, ComponentGoldenTrialReportV1>,
    pub budget_observations: BTreeMap<ResourceBudgetPathV1, ResourceBudgetObservationV1>,
    pub process_resident_peak_bytes: u64,
    pub process_swap_peak_bytes: u64,
    pub process_memory_peak_bytes: u64,
    pub frozen_budgets: FrozenResourceBudgetsV1,
    pub frozen_budget_verification_trials:
        BTreeMap<FrozenBudgetVerificationTrialIdV1, FrozenBudgetVerificationTrialV1>,
}

impl ComponentGoldenMeasurementReportV1 {
    pub fn validate_integrity(&self) -> Result<(), SimulationMeasurementErrorV1> {
        if self.schema_version != COMPONENT_GOLDEN_MEASUREMENT_REPORT_SCHEMA_V1 {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "测量报告 schema_version 非法".to_owned(),
            ));
        }
        self.workload_contract.validate()?;
        let workload_sha256 = self.workload_contract.content_sha256()?;
        if self.workload_contract_sha256 != workload_sha256 {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "测量报告负载合同 SHA-256 不能由正文重算".to_owned(),
            ));
        }
        self.environment.validate()?;
        self.runtime_hard_guards.validate()?;
        let expected_trials = COMPONENT_GOLDEN_TRIAL_ORDER_V1
            .into_iter()
            .collect::<Vec<_>>();
        let actual_trials = self.trials.keys().copied().collect::<Vec<_>>();
        if actual_trials != expected_trials {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "测量报告必须保留 S1,P1,P2,S2,S3,P3 六个试次".to_owned(),
            ));
        }

        let mut observed_process_resident_peak = 0_u64;
        let mut observed_process_swap_peak = 0_u64;
        let mut observed_process_memory_peak = 0_u64;
        let mut initial_policy_binding: Option<(&str, &Sha256Hex)> = None;
        for (trial_id, trial) in &self.trials {
            if trial.trial_id != *trial_id {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "试次映射键与正文 trial_id 不一致".to_owned(),
                ));
            }
            trial.validate_integrity()?;
            if let Some(preseal) = &trial.preseal_measurement
                && (preseal.workload_contract_sha256 != self.workload_contract_sha256
                    || !preseal
                        .environment
                        .has_same_fixed_context_as(&self.environment)
                    || preseal.runtime_hard_guards != self.runtime_hard_guards)
            {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "试次封口前测量正文未绑定报告的负载、固定环境上下文或初测运行硬门".to_owned(),
                ));
            }
            for result in trial.trajectories.values() {
                if result.logical_summary_input.workload_contract_sha256
                    != self.workload_contract_sha256
                {
                    return Err(SimulationMeasurementErrorV1::ReportMismatch(
                        "逐轨迹逻辑摘要未绑定报告的 COMPONENT_GOLDEN_V1 合同".to_owned(),
                    ));
                }
            }
            for snapshot in &trial.resource_snapshots {
                self.runtime_hard_guards.validate_snapshot(snapshot)?;
                observed_process_resident_peak =
                    observed_process_resident_peak.max(snapshot.procfs.vm_hwm_bytes);
                observed_process_swap_peak =
                    observed_process_swap_peak.max(snapshot.procfs.vm_swap_bytes);
                observed_process_memory_peak = observed_process_memory_peak
                    .max(snapshot_process_or_cgroup_memory_peak_bytes(snapshot)?);
            }
            if let Some(sealed) = &trial.sealed_resource_evidence {
                let current = (
                    sealed.resource_policy_id.as_str(),
                    &sealed.resource_policy_sha256,
                );
                if initial_policy_binding.is_some_and(|expected| expected != current) {
                    return Err(SimulationMeasurementErrorV1::ReportMismatch(
                        "六个初始试次必须绑定同一预冻结资源策略".to_owned(),
                    ));
                }
                initial_policy_binding.get_or_insert(current);
                let snapshot = &sealed.postseal_resource_snapshot;
                observed_process_resident_peak =
                    observed_process_resident_peak.max(snapshot.procfs.vm_hwm_bytes);
                observed_process_swap_peak =
                    observed_process_swap_peak.max(snapshot.procfs.vm_swap_bytes);
                observed_process_memory_peak = observed_process_memory_peak
                    .max(snapshot_process_or_cgroup_memory_peak_bytes(snapshot)?);
            }
        }
        if self.process_resident_peak_bytes != observed_process_resident_peak
            || self.process_swap_peak_bytes != observed_process_swap_peak
            || self.process_memory_peak_bytes != observed_process_memory_peak
        {
            return Err(SimulationMeasurementErrorV1::BudgetMismatch(
                "进程常驻、交换和合计峰值不能由六试次封口前后 /proc 与 cgroup 样本重算".to_owned(),
            ));
        }
        let expected_observations = derive_resource_budget_observations_v1(&self.trials)?;
        if self.budget_observations != expected_observations {
            return Err(SimulationMeasurementErrorV1::BudgetMismatch(
                "报告的 Pmax/Amax 不能由六试次内部高水位与逐事务增量样本重算".to_owned(),
            ));
        }
        let expected_budgets = freeze_resource_budgets_v1(
            &expected_observations,
            observed_process_memory_peak,
            self.frozen_budgets.failure_reserve,
        )?;
        if self.frozen_budgets != expected_budgets {
            return Err(SimulationMeasurementErrorV1::BudgetMismatch(
                "冻结预算不能由 Pmax 与 Amax 重算".to_owned(),
            ));
        }
        let frozen_budgets_sha256 = self.frozen_budgets.content_sha256()?;
        let frozen_resource_policy_sha256 = self
            .frozen_budgets
            .to_resource_policy_v1()?
            .content_sha256()
            .map_err(|error| {
                SimulationMeasurementErrorV1::BudgetMismatch(format!(
                    "冻结资源策略 SHA-256 计算失败：{error}"
                ))
            })?;
        let expected_verifications = FROZEN_BUDGET_VERIFICATION_TRIALS_V1
            .into_iter()
            .collect::<Vec<_>>();
        let actual_verifications = self
            .frozen_budget_verification_trials
            .keys()
            .copied()
            .collect::<Vec<_>>();
        if actual_verifications != expected_verifications {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "报告必须保留冻结后的串行与并行验证试次".to_owned(),
            ));
        }
        let expected_verification_guards = MeasurementRuntimeHardGuardsV1 {
            cgroup_memory_max_bytes: self.frozen_budgets.process_memory_max_bytes,
            cgroup_memory_swap_max_bytes: 0,
            cgroup_pids_max_count: self.runtime_hard_guards.cgroup_pids_max_count,
            file_descriptor_soft_limit: self.runtime_hard_guards.file_descriptor_soft_limit,
            file_descriptor_hard_limit: self.runtime_hard_guards.file_descriptor_hard_limit,
        };
        let mut verification_environment: Option<&MeasurementEnvironmentIdentityV1> = None;
        let mut verification_policy_binding: Option<(&str, &Sha256Hex)> = None;
        for (verification_id, verification) in &self.frozen_budget_verification_trials {
            if verification.verification_id != *verification_id {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "冻结后验证试次映射键与正文身份不一致".to_owned(),
                ));
            }
            verification
                .validate_integrity(&self.workload_contract_sha256, &frozen_budgets_sha256)?;
            if let Some(preseal) = &verification.preseal_measurement {
                if !preseal
                    .environment
                    .has_same_cross_build_context_as(&self.environment)
                    || preseal.runtime_hard_guards != expected_verification_guards
                {
                    return Err(SimulationMeasurementErrorV1::ReportMismatch(
                        "冻结后验证必须绑定报告固定环境上下文、冻结内存硬门、零交换和初测 pids/fd 硬门"
                        .to_owned(),
                    ));
                }
                if verification_environment.is_some_and(|expected| {
                    !preseal.environment.has_same_fixed_context_as(expected)
                }) {
                    return Err(SimulationMeasurementErrorV1::ReportMismatch(
                        "两次冻结后验证必须绑定同一最终发布构建与固定上下文".to_owned(),
                    ));
                }
                verification_environment.get_or_insert(&preseal.environment);
            }
            if let Some(sealed) = &verification.sealed_resource_evidence {
                if sealed.resource_policy_id != COMPONENT_GOLDEN_RESOURCE_POLICY_ID_V1
                    || sealed.resource_policy_sha256 != frozen_resource_policy_sha256
                {
                    return Err(SimulationMeasurementErrorV1::ReportMismatch(
                        "冻结后验证审计资源策略必须与冻结预算映射出的生产策略精确一致".to_owned(),
                    ));
                }
                let current = (
                    sealed.resource_policy_id.as_str(),
                    &sealed.resource_policy_sha256,
                );
                if verification_policy_binding.is_some_and(|expected| expected != current) {
                    return Err(SimulationMeasurementErrorV1::ReportMismatch(
                        "两次冻结后验证必须绑定同一冻结资源策略".to_owned(),
                    ));
                }
                verification_policy_binding.get_or_insert(current);
                validate_process_snapshot_within_frozen_budgets(
                    &sealed.postseal_resource_snapshot,
                    &self.frozen_budgets,
                )?;
            }
        }
        Ok(())
    }

    pub fn validate_complete(&self) -> Result<(), SimulationMeasurementErrorV1> {
        self.validate_integrity()?;
        let mut stable_summaries: BTreeMap<ComponentGoldenTrajectoryV1, Sha256Hex> =
            BTreeMap::new();
        let mut stable_seeds: BTreeMap<ComponentGoldenTrajectoryV1, Sha256Hex> = BTreeMap::new();
        for trial in self.trials.values() {
            trial.validate_succeeded()?;
            for (trajectory, result) in &trial.trajectories {
                match stable_summaries.get(trajectory) {
                    Some(expected) if expected != &result.logical_summary_sha256 => {
                        return Err(SimulationMeasurementErrorV1::ReportMismatch(
                            "同一轨迹在串行与并行试次的逻辑摘要不一致".to_owned(),
                        ));
                    }
                    Some(_) => {}
                    None => {
                        stable_summaries.insert(*trajectory, result.logical_summary_sha256.clone());
                    }
                }
                match stable_seeds.get(trajectory) {
                    Some(expected) if expected != &result.runtime_binding.session_seed_sha256 => {
                        return Err(SimulationMeasurementErrorV1::ReportMismatch(
                            "同一轨迹在六个正式试次的会话种子不一致".to_owned(),
                        ));
                    }
                    Some(_) => {}
                    None => {
                        stable_seeds.insert(
                            *trajectory,
                            result.runtime_binding.session_seed_sha256.clone(),
                        );
                    }
                }
            }
        }
        let frozen_budgets_sha256 = self.frozen_budgets.content_sha256()?;
        for verification in self.frozen_budget_verification_trials.values() {
            verification.validate_complete(
                &self.workload_contract_sha256,
                &frozen_budgets_sha256,
                &self.frozen_budgets,
                &stable_summaries,
                &stable_seeds,
            )?;
        }
        let mut run_ids = BTreeSet::new();
        let mut artifact_receipts = BTreeSet::new();
        let mut preseal_measurements = BTreeSet::new();
        let mut cgroup_paths = BTreeSet::new();
        let mut process_identities = BTreeSet::new();
        let mut audit_files = BTreeSet::new();
        for measurement in self
            .trials
            .values()
            .filter_map(|trial| trial.preseal_measurement.as_ref())
            .chain(
                self.frozen_budget_verification_trials
                    .values()
                    .filter_map(|trial| trial.preseal_measurement.as_ref()),
            )
        {
            if !preseal_measurements.insert(measurement.content_sha256()?) {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "六个初始试次与两个冻结后验证必须绑定八个不同封口前测量正文".to_owned(),
                ));
            }
            let process = &measurement.resource_snapshots[0].procfs;
            if !cgroup_paths.insert(measurement.environment.cgroup_path_sha256.clone())
                || !process_identities.insert((
                    measurement.environment.host.boot_identity_sha256.clone(),
                    process.pid,
                    process.process_start_time_ticks,
                ))
            {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "六个初始试次与两个冻结后验证必须使用八个不同 cgroup 路径与进程 PID".to_owned(),
                ));
            }
        }
        for receipt in self
            .trials
            .values()
            .filter_map(|trial| trial.run_artifact_receipt.as_ref())
            .chain(
                self.frozen_budget_verification_trials
                    .values()
                    .filter_map(|trial| trial.run_artifact_receipt.as_ref()),
            )
        {
            if !run_ids.insert(receipt.run_id.clone())
                || !artifact_receipts.insert((
                    receipt.manifest_sha256.clone(),
                    receipt.through_commit_sequence,
                    receipt.evidence_head_sha256.clone(),
                ))
            {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "六个初始试次与两个冻结后验证必须绑定八个不同运行及产物回执".to_owned(),
                ));
            }
        }
        for sealed in self
            .trials
            .values()
            .filter_map(|trial| trial.sealed_resource_evidence.as_ref())
            .chain(
                self.frozen_budget_verification_trials
                    .values()
                    .filter_map(|trial| trial.sealed_resource_evidence.as_ref()),
            )
        {
            if !audit_files.insert(sealed.audit_file_sha256.clone()) {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "六个初始试次与两个冻结后验证必须绑定八个不同审计文件".to_owned(),
                ));
            }
        }
        if run_ids.len() != 8
            || artifact_receipts.len() != 8
            || preseal_measurements.len() != 8
            || cgroup_paths.len() != 8
            || process_identities.len() != 8
            || audit_files.len() != 8
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "完整测量报告必须绑定八个不同的封口前测量、成功运行、进程、cgroup、审计文件及产物回执"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    pub fn content_sha256(&self) -> Result<Sha256Hex, SimulationMeasurementErrorV1> {
        self.validate_integrity()?;
        measurement_hash(COMPONENT_GOLDEN_REPORT_HASH_DOMAIN_V1, self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimulationMeasurementErrorV1 {
    EmptyLatencySamples,
    ArithmeticOverflow(&'static str),
    ContractMismatch(String),
    ReportMismatch(String),
    BudgetMismatch(String),
    InvalidSha256 { field: &'static str },
    IncompleteTrial(ComponentGoldenTrialIdV1),
    IncompleteVerificationTrial(FrozenBudgetVerificationTrialIdV1),
    Serialization(String),
}

impl fmt::Display for SimulationMeasurementErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyLatencySamples => write!(formatter, "延迟样本不能为空"),
            Self::ArithmeticOverflow(field) => write!(formatter, "测量预算算术溢出：{field}"),
            Self::ContractMismatch(message) => write!(formatter, "负载合同不一致：{message}"),
            Self::ReportMismatch(message) => write!(formatter, "测量报告不一致：{message}"),
            Self::BudgetMismatch(message) => write!(formatter, "资源预算不一致：{message}"),
            Self::InvalidSha256 { field } => write!(formatter, "SHA-256 字段非法：{field}"),
            Self::IncompleteTrial(trial_id) => {
                write!(formatter, "正式测量试次未成功：{trial_id:?}")
            }
            Self::IncompleteVerificationTrial(trial_id) => {
                write!(formatter, "冻结后验证试次未成功：{trial_id:?}")
            }
            Self::Serialization(message) => write!(formatter, "测量合同序列化失败：{message}"),
        }
    }
}

impl std::error::Error for SimulationMeasurementErrorV1 {}

fn throughput_per_second_milli(
    completed_work_items: u64,
    elapsed_ns: u64,
) -> Result<u64, SimulationMeasurementErrorV1> {
    if elapsed_ns == 0 {
        return Err(SimulationMeasurementErrorV1::ReportMismatch(
            "吞吐量 elapsed_ns 必须大于零".to_owned(),
        ));
    }
    let numerator = u128::from(completed_work_items) * 1_000_000_000_000_u128;
    u64::try_from(numerator / u128::from(elapsed_ns)).map_err(|_| {
        SimulationMeasurementErrorV1::ArithmeticOverflow("completed_work_items_per_second_milli")
    })
}

fn validate_budget_paths<T>(
    values: &BTreeMap<ResourceBudgetPathV1, T>,
    subject: &str,
) -> Result<(), SimulationMeasurementErrorV1> {
    let expected = RESOURCE_BUDGET_PATHS_V1.into_iter().collect::<Vec<_>>();
    let actual = values.keys().copied().collect::<Vec<_>>();
    if actual != expected {
        return Err(SimulationMeasurementErrorV1::BudgetMismatch(format!(
            "{subject}必须恰好覆盖 24 个预登记路径"
        )));
    }
    Ok(())
}

fn observe_quantity_max(target: &mut ResourceQuantityV1, candidate: ResourceQuantityV1) {
    target.records = target.records.max(candidate.records);
    target.bytes = target.bytes.max(candidate.bytes);
}

fn validate_resource_snapshot_sequence(
    snapshots: &[MeasuredResourceSnapshotV1],
) -> Result<(), SimulationMeasurementErrorV1> {
    let mut previous_snapshot: Option<&MeasuredResourceSnapshotV1> = None;
    for (expected_index, snapshot) in snapshots.iter().enumerate() {
        snapshot.validate()?;
        if snapshot.sample_index != expected_index as u64
            || previous_snapshot.is_some_and(|previous| snapshot.elapsed_ns < previous.elapsed_ns)
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "资源快照序号不连续或 elapsed_ns 发生回退".to_owned(),
            ));
        }
        if let Some(previous) = previous_snapshot {
            snapshot.validate_follows(previous)?;
        }
        previous_snapshot = Some(snapshot);
    }
    Ok(())
}

fn validate_rolling_snapshot_references(
    trajectory: &TrajectoryTrialResultV1,
    snapshot_count: usize,
) -> Result<(), SimulationMeasurementErrorV1> {
    let last_index = snapshot_count.checked_sub(1).ok_or_else(|| {
        SimulationMeasurementErrorV1::ReportMismatch("逻辑滚动周期缺少资源快照".to_owned())
    })? as u64;
    if trajectory
        .rolling_cycles
        .values()
        .any(|cycle| cycle.expired_snapshot_index > last_index)
    {
        return Err(SimulationMeasurementErrorV1::ReportMismatch(
            "逻辑滚动周期引用了不存在的资源快照".to_owned(),
        ));
    }
    Ok(())
}

fn validate_high_water_within_frozen_budgets(
    high_water: &SimulationResourceHighWaterV1,
    frozen_budgets: &FrozenResourceBudgetsV1,
) -> Result<(), SimulationMeasurementErrorV1> {
    let usage = usage_by_budget_path(&high_water.observed);
    for (path, quantity) in usage {
        let limit = frozen_budgets.limits.get(&path).ok_or_else(|| {
            SimulationMeasurementErrorV1::BudgetMismatch("冻结资源预算缺少验证路径".to_owned())
        })?;
        if quantity.records > limit.max_records || quantity.bytes > limit.max_bytes {
            return Err(SimulationMeasurementErrorV1::BudgetMismatch(format!(
                "冻结后验证试次越过资源预算：{path:?}"
            )));
        }
    }
    Ok(())
}

fn validate_process_snapshot_within_frozen_budgets(
    snapshot: &MeasuredResourceSnapshotV1,
    frozen_budgets: &FrozenResourceBudgetsV1,
) -> Result<(), SimulationMeasurementErrorV1> {
    let process_memory_bytes = snapshot_process_or_cgroup_memory_peak_bytes(snapshot)?;
    if process_memory_bytes > frozen_budgets.process_memory_max_bytes {
        return Err(SimulationMeasurementErrorV1::BudgetMismatch(
            "冻结后验证试次越过进程或 cgroup 内存上限".to_owned(),
        ));
    }
    Ok(())
}

fn snapshot_process_or_cgroup_memory_peak_bytes(
    snapshot: &MeasuredResourceSnapshotV1,
) -> Result<u64, SimulationMeasurementErrorV1> {
    let proc_memory_bytes = snapshot
        .procfs
        .vm_hwm_bytes
        .checked_add(snapshot.procfs.vm_swap_bytes)
        .ok_or(SimulationMeasurementErrorV1::ArithmeticOverflow(
            "process_memory_peak_bytes",
        ))?;
    let cgroup_memory_bytes = snapshot
        .cgroup_v2
        .memory_peak_bytes
        .ok_or_else(|| {
            SimulationMeasurementErrorV1::ReportMismatch(
                "正式测量缺少 cgroup memory.peak".to_owned(),
            )
        })?
        .checked_add(snapshot.cgroup_v2.memory_swap_peak_bytes.ok_or_else(|| {
            SimulationMeasurementErrorV1::ReportMismatch(
                "正式测量缺少 cgroup memory.swap.peak".to_owned(),
            )
        })?)
        .ok_or(SimulationMeasurementErrorV1::ArithmeticOverflow(
            "cgroup_memory_peak_bytes",
        ))?;
    Ok(proc_memory_bytes.max(cgroup_memory_bytes))
}

fn usage_by_budget_path(
    usage: &SimulationResourceUsageV1,
) -> BTreeMap<ResourceBudgetPathV1, ResourceQuantityV1> {
    BTreeMap::from([
        (
            ResourceBudgetPathV1::SessionInMemoryState,
            usage.session.in_memory_state,
        ),
        (
            ResourceBudgetPathV1::SessionHotMarkets,
            usage.session.hot_markets,
        ),
        (
            ResourceBudgetPathV1::SessionInputQueue,
            usage.session.input_queue,
        ),
        (
            ResourceBudgetPathV1::SessionRequestQueue,
            usage.session.request_queue,
        ),
        (
            ResourceBudgetPathV1::SessionFeedbackQueue,
            usage.session.feedback_queue,
        ),
        (
            ResourceBudgetPathV1::SessionActiveOrders,
            usage.session.active_orders,
        ),
        (
            ResourceBudgetPathV1::SessionInternalEvents,
            usage.session.internal_events,
        ),
        (
            ResourceBudgetPathV1::SessionQueryCache,
            usage.session.query_cache,
        ),
        (ResourceBudgetPathV1::SessionIndexes, usage.session.indexes),
        (
            ResourceBudgetPathV1::RunInMemoryState,
            usage.run.in_memory_state,
        ),
        (ResourceBudgetPathV1::RunSessions, usage.run.sessions),
        (ResourceBudgetPathV1::RunHotMarkets, usage.run.hot_markets),
        (ResourceBudgetPathV1::RunInputQueue, usage.run.input_queue),
        (
            ResourceBudgetPathV1::RunRequestQueue,
            usage.run.request_queue,
        ),
        (
            ResourceBudgetPathV1::RunFeedbackQueue,
            usage.run.feedback_queue,
        ),
        (
            ResourceBudgetPathV1::RunActiveOrders,
            usage.run.active_orders,
        ),
        (
            ResourceBudgetPathV1::RunInternalEvents,
            usage.run.internal_events,
        ),
        (ResourceBudgetPathV1::RunQueryCache, usage.run.query_cache),
        (ResourceBudgetPathV1::RunIndexes, usage.run.indexes),
        (
            ResourceBudgetPathV1::RunCapitalFence,
            usage.run.capital_fence,
        ),
        (
            ResourceBudgetPathV1::DurableEvidence,
            usage.durable.evidence,
        ),
        (
            ResourceBudgetPathV1::DurableCheckpoints,
            usage.durable.checkpoints,
        ),
        (
            ResourceBudgetPathV1::DurableArtifacts,
            usage.durable.artifacts,
        ),
        (
            ResourceBudgetPathV1::DurableRuntimeRoot,
            usage.durable.runtime_root,
        ),
    ])
}

fn nearest_rank_value(
    ordered: &[u64],
    percentile: u64,
) -> Result<u64, SimulationMeasurementErrorV1> {
    let len = u64::try_from(ordered.len())
        .map_err(|_| SimulationMeasurementErrorV1::ArithmeticOverflow("sample_count"))?;
    let product =
        len.checked_mul(percentile)
            .ok_or(SimulationMeasurementErrorV1::ArithmeticOverflow(
                "nearest_rank",
            ))?;
    let rank = product
        .checked_add(99)
        .ok_or(SimulationMeasurementErrorV1::ArithmeticOverflow(
            "nearest_rank",
        ))?
        / 100;
    let index = usize::try_from(rank - 1)
        .map_err(|_| SimulationMeasurementErrorV1::ArithmeticOverflow("nearest_rank"))?;
    ordered
        .get(index)
        .copied()
        .ok_or(SimulationMeasurementErrorV1::EmptyLatencySamples)
}

fn padded_peak(peak: u64, increment: u64) -> Result<u64, SimulationMeasurementErrorV1> {
    let padded_by_ratio = (u128::from(peak) * 5).div_ceil(4);
    let padded_by_transaction = u128::from(peak) + u128::from(increment);
    u64::try_from(padded_by_ratio.max(padded_by_transaction)).map_err(|_| {
        SimulationMeasurementErrorV1::ArithmeticOverflow("ceil(max(1.25*Pmax,Pmax+Amax))")
    })
}

fn round_up(value: u64, quantum: u64) -> Result<u64, SimulationMeasurementErrorV1> {
    if value == 0 {
        return Ok(0);
    }
    value
        .checked_add(quantum - 1)
        .map(|sum| (sum / quantum) * quantum)
        .ok_or(SimulationMeasurementErrorV1::ArithmeticOverflow("round_up"))
}

fn measurement_hash<T: Serialize>(
    domain: &str,
    value: &T,
) -> Result<Sha256Hex, SimulationMeasurementErrorV1> {
    canonical_json_sha256_v1(domain, value)
        .map_err(|error| SimulationMeasurementErrorV1::Serialization(error.to_string()))
}

fn validate_sha256(
    value: &Sha256Hex,
    field: &'static str,
) -> Result<(), SimulationMeasurementErrorV1> {
    if !is_lower_hex(&value.0, 64) {
        return Err(SimulationMeasurementErrorV1::InvalidSha256 { field });
    }
    Ok(())
}

fn is_lower_hex(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sha(byte: char) -> Sha256Hex {
        Sha256Hex(byte.to_string().repeat(64))
    }

    fn measured_failure_reserve() -> SimulationFailureEvidenceReserveV1 {
        SimulationFailureEvidenceReserveV1 {
            evidence: ResourceQuantityV1::new(1, 1),
            checkpoints: ResourceQuantityV1::new(1, 1),
            runtime_root: ResourceQuantityV1::new(1, 1),
        }
    }

    fn component_trial_receipt(
        trial_id: ComponentGoldenTrialIdV1,
    ) -> Option<SimulationRunArtifactReceiptBindingV1> {
        let (manifest_digit, evidence_digit, through_commit_sequence) = match trial_id {
            ComponentGoldenTrialIdV1::S1 => ('1', '7', 1),
            ComponentGoldenTrialIdV1::P1 => ('2', '8', 2),
            ComponentGoldenTrialIdV1::P2 => ('3', '9', 3),
            ComponentGoldenTrialIdV1::S2 => ('4', 'a', 4),
            ComponentGoldenTrialIdV1::S3 => ('5', 'b', 5),
            ComponentGoldenTrialIdV1::P3 => ('6', 'c', 6),
        };
        Some(SimulationRunArtifactReceiptBindingV1 {
            run_id: component_trial_run_id(trial_id),
            measurement_run_identity: MeasurementRunIdentityV1::ComponentTrial(trial_id),
            manifest_sha256: sha(manifest_digit),
            through_commit_sequence,
            evidence_head_sha256: sha(evidence_digit),
        })
    }

    fn component_trial_run_id(trial_id: ComponentGoldenTrialIdV1) -> String {
        match trial_id {
            ComponentGoldenTrialIdV1::S1 => "component-golden-s1",
            ComponentGoldenTrialIdV1::P1 => "component-golden-p1",
            ComponentGoldenTrialIdV1::P2 => "component-golden-p2",
            ComponentGoldenTrialIdV1::S2 => "component-golden-s2",
            ComponentGoldenTrialIdV1::S3 => "component-golden-s3",
            ComponentGoldenTrialIdV1::P3 => "component-golden-p3",
        }
        .to_owned()
    }

    fn verification_trial_receipt(
        verification_id: FrozenBudgetVerificationTrialIdV1,
    ) -> Option<SimulationRunArtifactReceiptBindingV1> {
        let (manifest_digit, evidence_digit, through_commit_sequence) = match verification_id {
            FrozenBudgetVerificationTrialIdV1::Serial => ('d', 'e', 7),
            FrozenBudgetVerificationTrialIdV1::Parallel => ('e', 'f', 8),
        };
        Some(SimulationRunArtifactReceiptBindingV1 {
            run_id: verification_trial_run_id(verification_id),
            measurement_run_identity: MeasurementRunIdentityV1::FrozenBudgetVerification(
                verification_id,
            ),
            manifest_sha256: sha(manifest_digit),
            through_commit_sequence,
            evidence_head_sha256: sha(evidence_digit),
        })
    }

    fn verification_trial_run_id(verification_id: FrozenBudgetVerificationTrialIdV1) -> String {
        match verification_id {
            FrozenBudgetVerificationTrialIdV1::Serial => "component-golden-verify-serial",
            FrozenBudgetVerificationTrialIdV1::Parallel => "component-golden-verify-parallel",
        }
        .to_owned()
    }

    fn measurement_environment() -> MeasurementEnvironmentIdentityV1 {
        let cache_precondition = MeasurementCachePreconditionV1::formal_v1();
        let cache_precondition_sha256 = cache_precondition.content_sha256().unwrap();
        MeasurementEnvironmentIdentityV1 {
            host: MeasurementHostIdentityV1 {
                machine_identity_sha256: sha('1'),
                boot_identity_sha256: sha('2'),
                os_release_sha256: sha('3'),
                kernel_release: "test-kernel".to_owned(),
                architecture: "x86_64".to_owned(),
                online_cpu_count: 2,
                page_size_bytes: 4_096,
                physical_memory_bytes: SIXTY_FOUR_MIB,
                swap_total_bytes: 0,
            },
            build: MeasurementBuildIdentityV1 {
                release_binary_sha256: sha('a'),
                source_tree_sha256: sha('4'),
                cargo_lock_sha256: sha('5'),
                source_commit: "a".repeat(40),
                rustc_version: "rustc test".to_owned(),
                target_triple: "x86_64-unknown-linux-gnu".to_owned(),
                cargo_profile: "release".to_owned(),
            },
            filesystem: MeasurementFilesystemIdentityV1 {
                measurement_root_sha256: sha('6'),
                mount_identity_sha256: sha('7'),
                filesystem_type: "ext4".to_owned(),
                block_size_bytes: 4_096,
                total_bytes: SIXTY_FOUR_MIB,
                available_bytes_before: SIXTY_FOUR_MIB,
            },
            cgroup_path_sha256: sha('8'),
            input_stream_sha256: sha('b'),
            runner_sha256: sha('c'),
            cache_precondition,
            cache_precondition_sha256,
        }
    }

    fn measurement_environment_for_ordinal(ordinal: usize) -> MeasurementEnvironmentIdentityV1 {
        let mut environment = measurement_environment();
        environment.cgroup_path_sha256 = sha(char::from_digit(ordinal as u32, 16).unwrap());
        environment.filesystem.available_bytes_before -= ordinal as u64;
        environment
    }

    fn measurement_runtime_hard_guards() -> MeasurementRuntimeHardGuardsV1 {
        MeasurementRuntimeHardGuardsV1 {
            cgroup_memory_max_bytes: 2 * SIXTY_FOUR_MIB,
            cgroup_memory_swap_max_bytes: 0,
            cgroup_pids_max_count: 16,
            file_descriptor_soft_limit: 1_024,
            file_descriptor_hard_limit: 1_024,
        }
    }

    fn sealed_high_water() -> SimulationResourceHighWaterV1 {
        let quantity = ResourceQuantityV1::new(4, 4);
        SimulationResourceHighWaterV1 {
            observed: SimulationResourceUsageV1 {
                session: SimulationSessionResourceUsageV1 {
                    in_memory_state: quantity,
                    hot_markets: quantity,
                    input_queue: quantity,
                    request_queue: quantity,
                    feedback_queue: quantity,
                    active_orders: quantity,
                    internal_events: quantity,
                    query_cache: quantity,
                    indexes: quantity,
                },
                run: SimulationRunResourceUsageV1 {
                    in_memory_state: quantity,
                    sessions: quantity,
                    hot_markets: quantity,
                    input_queue: quantity,
                    request_queue: quantity,
                    feedback_queue: quantity,
                    active_orders: quantity,
                    internal_events: quantity,
                    query_cache: quantity,
                    indexes: quantity,
                    capital_fence: quantity,
                },
                durable: SimulationDurableResourceUsageV1 {
                    evidence: quantity,
                    checkpoints: quantity,
                    artifacts: quantity,
                    runtime_root: ResourceQuantityV1::new(8, 8),
                },
            },
        }
    }

    fn sealed_resource_evidence(
        receipt: &SimulationRunArtifactReceiptBindingV1,
        preseal: &ComponentGoldenRunMeasurementV1,
        ordinal: usize,
    ) -> SealedRunResourceEvidenceV1 {
        let mut durable_transaction_resource_increments =
            preseal.transaction_resource_increments.clone();
        durable_transaction_resource_increments.push(ResourceTransactionIncrementV1 {
            transaction_index: durable_transaction_resource_increments.len() as u64,
            increments: RESOURCE_BUDGET_PATHS_V1
                .into_iter()
                .map(|path| {
                    let quantity = match path {
                        ResourceBudgetPathV1::DurableEvidence
                        | ResourceBudgetPathV1::DurableRuntimeRoot => ResourceQuantityV1::new(1, 1),
                        _ => ResourceQuantityV1::default(),
                    };
                    (path, quantity)
                })
                .collect(),
        });
        let artifact_publication_increment = RESOURCE_BUDGET_PATHS_V1
            .into_iter()
            .map(|path| {
                let quantity = match path {
                    ResourceBudgetPathV1::DurableArtifacts
                    | ResourceBudgetPathV1::DurableRuntimeRoot => ResourceQuantityV1::new(1, 1),
                    _ => ResourceQuantityV1::default(),
                };
                (path, quantity)
            })
            .collect();
        let mut postseal_resource_snapshot = preseal.resource_snapshots.last().unwrap().clone();
        postseal_resource_snapshot.sample_index += 1;
        postseal_resource_snapshot.elapsed_ns += 1;
        postseal_resource_snapshot.procfs.user_cpu_ticks += 1;
        postseal_resource_snapshot.procfs.system_cpu_ticks += 1;
        postseal_resource_snapshot.procfs.io.rchar_bytes += 1;
        postseal_resource_snapshot.procfs.io.wchar_bytes += 1;
        postseal_resource_snapshot.procfs.io.read_syscalls += 1;
        postseal_resource_snapshot.procfs.io.write_syscalls += 1;
        postseal_resource_snapshot.cgroup_v2.cpu_usage_usec += 1;
        postseal_resource_snapshot.internal.high_water = sealed_high_water();
        SealedRunResourceEvidenceV1 {
            audit_file_sha256: sha(char::from_digit(ordinal as u32, 16).unwrap()),
            run_artifact_receipt: receipt.clone(),
            resource_policy_id: COMPONENT_GOLDEN_RESOURCE_POLICY_ID_V1.to_owned(),
            resource_policy_sha256: sha('f'),
            high_water: sealed_high_water(),
            durable_transaction_resource_increments,
            artifact_publication_increment,
            postseal_resource_snapshot,
            seal_latency_ns: 4,
        }
    }

    fn logical_summary(
        trajectory: ComponentGoldenTrajectoryV1,
        workload_sha256: &Sha256Hex,
    ) -> TrajectoryLogicalSummaryInputV1 {
        let logical_component_sha256 = TRAJECTORY_LOGICAL_COMPONENTS_V1
            .into_iter()
            .enumerate()
            .map(|(index, component)| {
                let digit = char::from_digit((index + 1) as u32, 16).unwrap();
                (component, sha(digit))
            })
            .collect();
        TrajectoryLogicalSummaryInputV1 {
            schema_version: TRAJECTORY_LOGICAL_SUMMARY_SCHEMA_V1.to_owned(),
            workload_contract_sha256: workload_sha256.clone(),
            trajectory,
            logical_component_sha256,
        }
    }

    fn runtime_binding(
        run_id: &str,
        trajectory: ComponentGoldenTrajectoryV1,
    ) -> TrajectoryRuntimeBindingV1 {
        let suffix = match trajectory {
            ComponentGoldenTrajectoryV1::A => "a",
            ComponentGoldenTrajectoryV1::B => "b",
        };
        TrajectoryRuntimeBindingV1 {
            session_id: format!("{run_id}-session-{suffix}"),
            session_seed_sha256: match trajectory {
                ComponentGoldenTrajectoryV1::A => sha('a'),
                ComponentGoldenTrajectoryV1::B => sha('b'),
            },
            response_bridge_binding_id: format!("{run_id}-bridge-{suffix}"),
            account_instance_key: AccountInstanceKeyV1 {
                run_id: run_id.to_owned(),
                caller_role: SimCallerRoleV1::ModelTrain,
                caller_instance_id: format!("{run_id}-caller-{suffix}"),
                account_id: format!("{run_id}-account-{suffix}"),
                platform_source: PlatformSourceV1::Simulated,
            },
        }
    }

    fn complete_report() -> ComponentGoldenMeasurementReportV1 {
        let workload_contract = ComponentGoldenLoadContractV1::component_golden_v1();
        let workload_contract_sha256 = workload_contract.content_sha256().unwrap();
        let environment = measurement_environment();
        let runtime_hard_guards = measurement_runtime_hard_guards();
        let trials = COMPONENT_GOLDEN_TRIAL_ORDER_V1
            .into_iter()
            .enumerate()
            .map(|(trial_ordinal, trial_id)| {
                let trial_environment = measurement_environment_for_ordinal(trial_ordinal);
                let trajectories =
                    COMPONENT_GOLDEN_TRAJECTORIES_V1
                        .into_iter()
                        .map(|trajectory| {
                            let logical_summary_input =
                                logical_summary(trajectory, &workload_contract_sha256);
                            let logical_summary_sha256 =
                                logical_summary_input.content_sha256().unwrap();
                            (
                                trajectory,
                                TrajectoryTrialResultV1 {
                                    runtime_binding: runtime_binding(
                                        &component_trial_run_id(trial_id),
                                        trajectory,
                                    ),
                                    logical_summary_input,
                                    logical_summary_sha256,
                                    throughput: TrajectoryThroughputV1 {
                                        completed_path_deliveries: 20,
                                        completed_top_level_calls: 19,
                                        breakdown: ThroughputBreakdownV1 {
                                            request_batch_items: 106,
                                            unique_fills: 1,
                                            affected_orders: 1,
                                            order_feedback: 1,
                                            trade_feedback: 1,
                                            settlement_feedback: 1,
                                            query_calls: 6,
                                            query_pages: 2,
                                            query_return_rows: 102,
                                        },
                                        elapsed_ns: 1_000,
                                        completed_work_items_per_second_milli:
                                            throughput_per_second_milli(39, 1_000).unwrap(),
                                    },
                                    latency: MEASUREMENT_LATENCY_KINDS_V1
                                        .into_iter()
                                        .map(|kind| {
                                            (
                                                kind,
                                                RawLatencySamplesNsV1::from_samples(vec![1, 2, 3])
                                                    .unwrap(),
                                            )
                                        })
                                        .collect(),
                                    rolling_cycles: BTreeMap::from([
                                        (
                                            LogicalRollingCycleIdV1::FirstMarket,
                                            LogicalRollingCycleV1 {
                                                cycle_id: LogicalRollingCycleIdV1::FirstMarket,
                                                hot_snapshot_index: 0,
                                                compact_snapshot_index: 1,
                                                expired_snapshot_index: 2,
                                                logical_completion_ns: 1,
                                                hot_state_high_water: ResourceQuantityV1::new(1, 1),
                                                compact_projection_high_water:
                                                    ResourceQuantityV1::new(1, 1),
                                                expired_deleted: ResourceQuantityV1::new(1, 1),
                                            },
                                        ),
                                        (
                                            LogicalRollingCycleIdV1::SecondMarket,
                                            LogicalRollingCycleV1 {
                                                cycle_id: LogicalRollingCycleIdV1::SecondMarket,
                                                hot_snapshot_index: 0,
                                                compact_snapshot_index: 1,
                                                expired_snapshot_index: 2,
                                                logical_completion_ns: 2,
                                                hot_state_high_water: ResourceQuantityV1::new(1, 1),
                                                compact_projection_high_water:
                                                    ResourceQuantityV1::new(1, 1),
                                                expired_deleted: ResourceQuantityV1::new(1, 1),
                                            },
                                        ),
                                    ]),
                                },
                            )
                        })
                        .collect::<BTreeMap<_, _>>();
                let session_usage = SimulationSessionResourceUsageV1::default();
                let internal = InternalResourceSnapshotV1 {
                    trajectory_sessions: COMPONENT_GOLDEN_TRAJECTORIES_V1
                        .into_iter()
                        .map(|trajectory| (trajectory, session_usage.clone()))
                        .collect(),
                    shared_run: SimulationRunResourceUsageV1::default(),
                    durable: SimulationDurableResourceUsageV1::default(),
                    high_water: SimulationResourceHighWaterV1::default(),
                };
                let mut trial = ComponentGoldenTrialReportV1 {
                    trial_id,
                    mode: trial_id.mode(),
                    worker_count: trial_id.worker_count(),
                    discarded_warmup_samples: 0,
                    preseal_measurement: None,
                    run_artifact_receipt: component_trial_receipt(trial_id),
                    sealed_resource_evidence: None,
                    outcome: MeasurementTrialOutcomeV1::Succeeded,
                    throughput: TrialThroughputV1 {
                        completed_path_deliveries: 40,
                        completed_top_level_calls: 38,
                        breakdown: ThroughputBreakdownV1 {
                            request_batch_items: 212,
                            unique_fills: 2,
                            affected_orders: 2,
                            order_feedback: 2,
                            trade_feedback: 2,
                            settlement_feedback: 2,
                            query_calls: 12,
                            query_pages: 4,
                            query_return_rows: 204,
                        },
                        elapsed_ns: 1_000,
                        completed_work_items_per_second_milli: throughput_per_second_milli(
                            78, 1_000,
                        )
                        .unwrap(),
                    },
                    latency: MEASUREMENT_LATENCY_KINDS_V1
                        .into_iter()
                        .map(|kind| {
                            (
                                kind,
                                RawLatencySamplesNsV1::from_samples(vec![1, 2, 3]).unwrap(),
                            )
                        })
                        .collect(),
                    resource_snapshots: {
                        let first = MeasuredResourceSnapshotV1 {
                            sample_index: 0,
                            elapsed_ns: 1,
                            procfs: ProcfsProcessResourceSnapshotV1 {
                                pid: 100 + trial_ordinal as u32,
                                process_start_time_ticks: 1_000 + trial_ordinal as u64,
                                vm_rss_bytes: 1,
                                vm_hwm_bytes: SIXTY_FOUR_MIB,
                                vm_swap_bytes: 0,
                                thread_count: u32::from(trial_id.worker_count()),
                                open_file_descriptor_count: 3,
                                file_descriptor_soft_limit: 1_024,
                                file_descriptor_hard_limit: 1_024,
                                user_cpu_ticks: 1,
                                system_cpu_ticks: 1,
                                io: ProcfsIoSnapshotV1 {
                                    rchar_bytes: 1,
                                    wchar_bytes: 1,
                                    read_syscalls: 1,
                                    write_syscalls: 1,
                                    storage_read_bytes: 0,
                                    storage_write_bytes: 0,
                                    cancelled_write_bytes: 0,
                                },
                            },
                            cgroup_v2: CgroupV2ResourceSnapshotV1 {
                                memory_current_bytes: 1,
                                memory_peak_bytes: Some(SIXTY_FOUR_MIB),
                                memory_limit: CgroupMemoryLimitV1::Bytes {
                                    bytes: 2 * SIXTY_FOUR_MIB,
                                },
                                memory_swap_current_bytes: 0,
                                memory_swap_peak_bytes: Some(0),
                                memory_swap_limit: CgroupMemoryLimitV1::Bytes { bytes: 0 },
                                memory_events: CgroupMemoryEventsV1 {
                                    low: 0,
                                    high: 0,
                                    max: 0,
                                    oom: 0,
                                    oom_kill: 0,
                                    oom_group_kill: 0,
                                },
                                cpu_usage_usec: 1,
                                pids_current: u64::from(trial_id.worker_count()),
                                pids_limit: CgroupPidsLimitV1::Count { count: 16 },
                            },
                            internal,
                        };
                        let mut compact = first.clone();
                        compact.sample_index = 1;
                        compact.elapsed_ns = 2;
                        compact.procfs.user_cpu_ticks = 2;
                        compact.procfs.system_cpu_ticks = 2;
                        compact.procfs.io.rchar_bytes = 2;
                        compact.procfs.io.wchar_bytes = 2;
                        compact.procfs.io.read_syscalls = 2;
                        compact.procfs.io.write_syscalls = 2;
                        compact.cgroup_v2.cpu_usage_usec = 2;
                        let mut expired = compact.clone();
                        expired.sample_index = 2;
                        expired.elapsed_ns = 3;
                        expired.procfs.user_cpu_ticks = 3;
                        expired.procfs.system_cpu_ticks = 3;
                        expired.procfs.io.rchar_bytes = 3;
                        expired.procfs.io.wchar_bytes = 3;
                        expired.procfs.io.read_syscalls = 3;
                        expired.procfs.io.write_syscalls = 3;
                        expired.cgroup_v2.cpu_usage_usec = 3;
                        vec![first, compact, expired]
                    },
                    transaction_resource_increments: vec![ResourceTransactionIncrementV1 {
                        transaction_index: 0,
                        increments: RESOURCE_BUDGET_PATHS_V1
                            .into_iter()
                            .map(|path| {
                                let quantity = if path == ResourceBudgetPathV1::DurableArtifacts {
                                    ResourceQuantityV1::default()
                                } else {
                                    ResourceQuantityV1::new(3, 1)
                                };
                                (path, quantity)
                            })
                            .collect(),
                    }],
                    trajectories,
                };
                trial.preseal_measurement = Some(ComponentGoldenRunMeasurementV1 {
                    schema_version: COMPONENT_GOLDEN_RUN_MEASUREMENT_SCHEMA_V1.to_owned(),
                    run_id: trial.run_artifact_receipt.as_ref().unwrap().run_id.clone(),
                    measurement_run_identity: MeasurementRunIdentityV1::ComponentTrial(trial_id),
                    workload_contract_sha256: workload_contract_sha256.clone(),
                    environment: trial_environment,
                    runtime_hard_guards,
                    throughput: trial.throughput.clone(),
                    latency: trial.latency.clone(),
                    resource_snapshots: trial.resource_snapshots.clone(),
                    transaction_resource_increments: trial.transaction_resource_increments.clone(),
                    trajectories: trial.trajectories.clone(),
                });
                trial.sealed_resource_evidence = Some(sealed_resource_evidence(
                    trial.run_artifact_receipt.as_ref().unwrap(),
                    trial.preseal_measurement.as_ref().unwrap(),
                    trial_ordinal,
                ));
                let seal_latency = trial
                    .sealed_resource_evidence
                    .as_ref()
                    .unwrap()
                    .seal_latency_ns;
                let checkpoint_or_seal = trial
                    .latency
                    .get_mut(&MeasurementLatencyKindV1::CheckpointOrSeal)
                    .unwrap();
                checkpoint_or_seal.samples_ns.push(seal_latency);
                checkpoint_or_seal.nearest_rank =
                    nearest_rank_latency_ns_v1(&checkpoint_or_seal.samples_ns).unwrap();
                (trial_id, trial)
            })
            .collect::<BTreeMap<_, _>>();
        let budget_observations = derive_resource_budget_observations_v1(&trials).unwrap();
        let frozen_budgets = freeze_resource_budgets_v1(
            &budget_observations,
            SIXTY_FOUR_MIB,
            measured_failure_reserve(),
        )
        .unwrap();
        let frozen_budgets_sha256 = frozen_budgets.content_sha256().unwrap();
        let frozen_resource_policy_sha256 = frozen_budgets
            .to_resource_policy_v1()
            .unwrap()
            .content_sha256()
            .unwrap();
        let frozen_budget_verification_trials = FROZEN_BUDGET_VERIFICATION_TRIALS_V1
            .into_iter()
            .enumerate()
            .map(|(verification_ordinal, verification_id)| {
                let source_trial_id = match verification_id {
                    FrozenBudgetVerificationTrialIdV1::Serial => ComponentGoldenTrialIdV1::S1,
                    FrozenBudgetVerificationTrialIdV1::Parallel => ComponentGoldenTrialIdV1::P1,
                };
                let source = &trials[&source_trial_id];
                let mut verification_trajectories = source.trajectories.clone();
                for (trajectory, result) in &mut verification_trajectories {
                    result.runtime_binding =
                        runtime_binding(&verification_trial_run_id(verification_id), *trajectory);
                }
                let mut verification_snapshots = source.resource_snapshots.clone();
                for snapshot in &mut verification_snapshots {
                    snapshot.procfs.pid = 106 + verification_ordinal as u32;
                    snapshot.procfs.process_start_time_ticks = 1_006 + verification_ordinal as u64;
                    snapshot.cgroup_v2.memory_limit = CgroupMemoryLimitV1::Bytes {
                        bytes: frozen_budgets.process_memory_max_bytes,
                    };
                }
                let verification_runtime_hard_guards = MeasurementRuntimeHardGuardsV1 {
                    cgroup_memory_max_bytes: frozen_budgets.process_memory_max_bytes,
                    cgroup_memory_swap_max_bytes: 0,
                    cgroup_pids_max_count: runtime_hard_guards.cgroup_pids_max_count,
                    file_descriptor_soft_limit: runtime_hard_guards.file_descriptor_soft_limit,
                    file_descriptor_hard_limit: runtime_hard_guards.file_descriptor_hard_limit,
                };
                let mut verification = FrozenBudgetVerificationTrialV1 {
                    verification_id,
                    mode: verification_id.mode(),
                    worker_count: verification_id.worker_count(),
                    discarded_warmup_samples: 0,
                    workload_contract_sha256: workload_contract_sha256.clone(),
                    frozen_budgets_sha256: frozen_budgets_sha256.clone(),
                    preseal_measurement: None,
                    run_artifact_receipt: verification_trial_receipt(verification_id),
                    sealed_resource_evidence: None,
                    outcome: MeasurementTrialOutcomeV1::Succeeded,
                    throughput: source.throughput.clone(),
                    latency: source.latency.clone(),
                    resource_snapshots: verification_snapshots,
                    trajectories: verification_trajectories,
                };
                verification.preseal_measurement = Some(ComponentGoldenRunMeasurementV1 {
                    schema_version: COMPONENT_GOLDEN_RUN_MEASUREMENT_SCHEMA_V1.to_owned(),
                    run_id: verification
                        .run_artifact_receipt
                        .as_ref()
                        .unwrap()
                        .run_id
                        .clone(),
                    measurement_run_identity: MeasurementRunIdentityV1::FrozenBudgetVerification(
                        verification_id,
                    ),
                    workload_contract_sha256: workload_contract_sha256.clone(),
                    environment: measurement_environment_for_ordinal(6 + verification_ordinal),
                    runtime_hard_guards: verification_runtime_hard_guards,
                    throughput: verification.throughput.clone(),
                    latency: verification.latency.clone(),
                    resource_snapshots: verification.resource_snapshots.clone(),
                    transaction_resource_increments: source.transaction_resource_increments.clone(),
                    trajectories: verification.trajectories.clone(),
                });
                verification.sealed_resource_evidence = Some(sealed_resource_evidence(
                    verification.run_artifact_receipt.as_ref().unwrap(),
                    verification.preseal_measurement.as_ref().unwrap(),
                    6 + verification_ordinal,
                ));
                verification
                    .sealed_resource_evidence
                    .as_mut()
                    .unwrap()
                    .resource_policy_sha256 = frozen_resource_policy_sha256.clone();
                let seal_latency = verification
                    .sealed_resource_evidence
                    .as_ref()
                    .unwrap()
                    .seal_latency_ns;
                let checkpoint_or_seal = verification
                    .latency
                    .get_mut(&MeasurementLatencyKindV1::CheckpointOrSeal)
                    .unwrap();
                checkpoint_or_seal.samples_ns.push(seal_latency);
                checkpoint_or_seal.nearest_rank =
                    nearest_rank_latency_ns_v1(&checkpoint_or_seal.samples_ns).unwrap();
                (verification_id, verification)
            })
            .collect();
        ComponentGoldenMeasurementReportV1 {
            schema_version: COMPONENT_GOLDEN_MEASUREMENT_REPORT_SCHEMA_V1.to_owned(),
            workload_contract,
            workload_contract_sha256,
            environment,
            runtime_hard_guards,
            trials,
            budget_observations,
            process_resident_peak_bytes: SIXTY_FOUR_MIB,
            process_swap_peak_bytes: 0,
            process_memory_peak_bytes: SIXTY_FOUR_MIB,
            frozen_budgets,
            frozen_budget_verification_trials,
        }
    }

    #[test]
    fn component_golden_contract_freezes_the_approved_scale() {
        let contract = ComponentGoldenLoadContractV1::component_golden_v1();
        contract.validate().unwrap();
        assert_eq!(contract.trajectory_count, 2);
        assert_eq!(contract.events.unique_event_ids, 10);
        assert_eq!(contract.events.total_path_deliveries, 40);
        assert_eq!(contract.calls_per_trajectory.top_level_calls, 19);
        assert_eq!(
            contract.calls_per_trajectory.post_orders_batch_sizes,
            [15, 15, 15, 15, 15, 15, 12]
        );
        assert_eq!(contract.lifecycle.first_page_items, 100);
        assert_eq!(contract.lifecycle.continuation_page_items, 2);
        assert_eq!(
            contract.schedule.trial_order,
            COMPONENT_GOLDEN_TRIAL_ORDER_V1
        );
        assert_eq!(
            contract.content_sha256().unwrap(),
            contract.content_sha256().unwrap()
        );
    }

    #[test]
    fn component_golden_contract_rejects_unknown_json_fields() {
        let contract = ComponentGoldenLoadContractV1::component_golden_v1();
        let mut value = serde_json::to_value(contract).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("unexpected".to_owned(), serde_json::json!(true));
        assert!(serde_json::from_value::<ComponentGoldenLoadContractV1>(value).is_err());
    }

    #[test]
    fn nearest_rank_uses_ceil_p_times_n_minus_one_and_retains_raw_samples() {
        let raw = RawLatencySamplesNsV1::from_samples(vec![100, 1, 4, 3, 2]).unwrap();
        assert_eq!(
            raw.nearest_rank,
            NearestRankLatencyNsV1 {
                p50: 3,
                p95: 100,
                p99: 100,
                max: 100,
            }
        );
        assert_eq!(raw.samples_ns, vec![100, 1, 4, 3, 2]);
        raw.validate().unwrap();
    }

    #[test]
    fn budget_formula_uses_peak_ratio_transaction_increment_and_rounding() {
        let observations = RESOURCE_BUDGET_PATHS_V1
            .into_iter()
            .map(|path| {
                (
                    path,
                    ResourceBudgetObservationV1 {
                        peak: ResourceQuantityV1::new(10, 4_097),
                        max_single_transaction_increment: ResourceQuantityV1::new(3, 1),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let frozen =
            freeze_resource_budgets_v1(&observations, 1, measured_failure_reserve()).unwrap();
        let representative = frozen
            .limits
            .get(&ResourceBudgetPathV1::SessionInMemoryState)
            .unwrap();
        assert_eq!(representative.max_records, 13);
        assert_eq!(representative.max_bytes, 8 * 1024);
        assert_eq!(frozen.process_memory_max_bytes, SIXTY_FOUR_MIB);
        assert_eq!(frozen.failure_reserve, measured_failure_reserve());

        let transaction_dominates = padded_peak(100, 50).unwrap();
        assert_eq!(transaction_dominates, 150);
    }

    #[test]
    fn frozen_budgets_map_all_twenty_four_distinct_limits_into_the_runtime_policy() {
        let limits = RESOURCE_BUDGET_PATHS_V1
            .into_iter()
            .enumerate()
            .map(|(index, path)| {
                let sentinel = (index + 1) as u64;
                (path, ResourceLimitV1::new(sentinel, sentinel * FOUR_KIB))
            })
            .collect::<BTreeMap<_, _>>();
        let frozen = FrozenResourceBudgetsV1 {
            limits,
            process_memory_max_bytes: SIXTY_FOUR_MIB,
            failure_reserve: measured_failure_reserve(),
        };

        let policy = frozen.to_resource_policy_v1().unwrap();
        assert_eq!(policy.schema_version, SIMULATION_RESOURCE_POLICY_SCHEMA_V1);
        assert_eq!(policy.policy_id, COMPONENT_GOLDEN_RESOURCE_POLICY_ID_V1);
        assert_eq!(policy.durable.failure_reserve, measured_failure_reserve());
        let mapped = [
            (
                ResourceBudgetPathV1::SessionInMemoryState,
                policy.session.in_memory_state,
            ),
            (
                ResourceBudgetPathV1::SessionHotMarkets,
                policy.session.hot_markets,
            ),
            (
                ResourceBudgetPathV1::SessionInputQueue,
                policy.session.input_queue,
            ),
            (
                ResourceBudgetPathV1::SessionRequestQueue,
                policy.session.request_queue,
            ),
            (
                ResourceBudgetPathV1::SessionFeedbackQueue,
                policy.session.feedback_queue,
            ),
            (
                ResourceBudgetPathV1::SessionActiveOrders,
                policy.session.active_orders,
            ),
            (
                ResourceBudgetPathV1::SessionInternalEvents,
                policy.session.internal_events,
            ),
            (
                ResourceBudgetPathV1::SessionQueryCache,
                policy.session.query_cache,
            ),
            (ResourceBudgetPathV1::SessionIndexes, policy.session.indexes),
            (
                ResourceBudgetPathV1::RunInMemoryState,
                policy.run.in_memory_state,
            ),
            (ResourceBudgetPathV1::RunSessions, policy.run.sessions),
            (ResourceBudgetPathV1::RunHotMarkets, policy.run.hot_markets),
            (ResourceBudgetPathV1::RunInputQueue, policy.run.input_queue),
            (
                ResourceBudgetPathV1::RunRequestQueue,
                policy.run.request_queue,
            ),
            (
                ResourceBudgetPathV1::RunFeedbackQueue,
                policy.run.feedback_queue,
            ),
            (
                ResourceBudgetPathV1::RunActiveOrders,
                policy.run.active_orders,
            ),
            (
                ResourceBudgetPathV1::RunInternalEvents,
                policy.run.internal_events,
            ),
            (ResourceBudgetPathV1::RunQueryCache, policy.run.query_cache),
            (ResourceBudgetPathV1::RunIndexes, policy.run.indexes),
            (
                ResourceBudgetPathV1::RunCapitalFence,
                policy.run.capital_fence,
            ),
            (
                ResourceBudgetPathV1::DurableEvidence,
                policy.durable.evidence,
            ),
            (
                ResourceBudgetPathV1::DurableCheckpoints,
                policy.durable.checkpoints,
            ),
            (
                ResourceBudgetPathV1::DurableArtifacts,
                policy.durable.artifacts,
            ),
            (
                ResourceBudgetPathV1::DurableRuntimeRoot,
                policy.durable.runtime_root,
            ),
        ];
        assert_eq!(mapped.len(), RESOURCE_BUDGET_PATHS_V1.len());
        assert_eq!(
            mapped.iter().map(|(path, _)| *path).collect::<Vec<_>>(),
            RESOURCE_BUDGET_PATHS_V1
        );
        assert_eq!(
            mapped
                .iter()
                .map(|(_, limit)| limit.max_records)
                .collect::<BTreeSet<_>>()
                .len(),
            RESOURCE_BUDGET_PATHS_V1.len()
        );
        for (path, actual) in mapped {
            assert_eq!(actual, frozen.limits[&path]);
        }
    }

    #[test]
    fn frozen_budgets_reject_zero_or_over_limit_failure_reserve() {
        let observations = RESOURCE_BUDGET_PATHS_V1
            .into_iter()
            .map(|path| {
                (
                    path,
                    ResourceBudgetObservationV1 {
                        peak: ResourceQuantityV1::new(2, FOUR_KIB),
                        max_single_transaction_increment: ResourceQuantityV1::new(1, 1),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut zero = measured_failure_reserve();
        zero.evidence.records = 0;
        assert!(freeze_resource_budgets_v1(&observations, 1, zero).is_err());

        let mut over_limit = measured_failure_reserve();
        over_limit.runtime_root.bytes = 2 * FOUR_KIB + 1;
        assert!(freeze_resource_budgets_v1(&observations, 1, over_limit).is_err());
    }

    #[test]
    fn trajectory_summary_hash_is_content_addressed_and_complete() {
        let workload_sha256 = ComponentGoldenLoadContractV1::component_golden_v1()
            .content_sha256()
            .unwrap();
        let input = logical_summary(ComponentGoldenTrajectoryV1::A, &workload_sha256);
        input.validate().unwrap();
        let first = input.content_sha256().unwrap();
        let second = input.content_sha256().unwrap();
        assert_eq!(first, second);

        let mut changed = input;
        changed
            .logical_component_sha256
            .insert(TrajectoryLogicalComponentV1::Orders, sha('0'));
        assert_ne!(first, changed.content_sha256().unwrap());
    }

    #[test]
    fn preseal_measurement_is_complete_content_addressed_and_receipt_free() {
        let report = complete_report();
        let trial = &report.trials[&ComponentGoldenTrialIdV1::S1];
        let receipt = trial.run_artifact_receipt.as_ref().unwrap();
        let measurement = ComponentGoldenRunMeasurementV1 {
            schema_version: COMPONENT_GOLDEN_RUN_MEASUREMENT_SCHEMA_V1.to_owned(),
            run_id: receipt.run_id.clone(),
            measurement_run_identity: MeasurementRunIdentityV1::ComponentTrial(
                ComponentGoldenTrialIdV1::S1,
            ),
            workload_contract_sha256: report.workload_contract_sha256.clone(),
            environment: report.environment.clone(),
            runtime_hard_guards: report.runtime_hard_guards,
            throughput: trial.throughput.clone(),
            latency: trial.latency.clone(),
            resource_snapshots: trial.resource_snapshots.clone(),
            transaction_resource_increments: trial.transaction_resource_increments.clone(),
            trajectories: trial.trajectories.clone(),
        };

        measurement.validate().unwrap();
        assert_eq!(
            measurement.content_sha256().unwrap(),
            measurement.content_sha256().unwrap()
        );
        let value = serde_json::to_value(&measurement).unwrap();
        assert!(value.get("run_artifact_receipt").is_none());
        assert!(value.get("outcome").is_none());

        let mut missing_latency = measurement;
        missing_latency
            .latency
            .remove(&MeasurementLatencyKindV1::CheckpointOrSeal);
        assert!(missing_latency.validate().is_err());
    }

    #[test]
    fn trial_ids_freeze_mode_and_worker_count() {
        let actual = COMPONENT_GOLDEN_TRIAL_ORDER_V1
            .into_iter()
            .map(|trial| (trial, trial.mode(), trial.worker_count()))
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                (
                    ComponentGoldenTrialIdV1::S1,
                    ComponentGoldenMeasurementModeV1::Serial,
                    1,
                ),
                (
                    ComponentGoldenTrialIdV1::P1,
                    ComponentGoldenMeasurementModeV1::Parallel,
                    2,
                ),
                (
                    ComponentGoldenTrialIdV1::P2,
                    ComponentGoldenMeasurementModeV1::Parallel,
                    2,
                ),
                (
                    ComponentGoldenTrialIdV1::S2,
                    ComponentGoldenMeasurementModeV1::Serial,
                    1,
                ),
                (
                    ComponentGoldenTrialIdV1::S3,
                    ComponentGoldenMeasurementModeV1::Serial,
                    1,
                ),
                (
                    ComponentGoldenTrialIdV1::P3,
                    ComponentGoldenMeasurementModeV1::Parallel,
                    2,
                ),
            ]
        );
    }

    #[test]
    fn six_trial_report_recomputes_all_embedded_evidence() {
        let report = complete_report();
        report.validate_integrity().unwrap();
        report.validate_complete().unwrap();
        assert_eq!(
            report.content_sha256().unwrap(),
            report.content_sha256().unwrap()
        );

        let mut divergent = report;
        let trial = divergent
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::P1)
            .unwrap();
        {
            let result = trial
                .trajectories
                .get_mut(&ComponentGoldenTrajectoryV1::A)
                .unwrap();
            result
                .logical_summary_input
                .logical_component_sha256
                .insert(TrajectoryLogicalComponentV1::Orders, sha('0'));
            result.logical_summary_sha256 = result.logical_summary_input.content_sha256().unwrap();
        }
        trial.preseal_measurement.as_mut().unwrap().trajectories = trial.trajectories.clone();
        divergent.validate_integrity().unwrap();
        assert!(divergent.validate_complete().is_err());
    }

    #[test]
    fn complete_report_requires_fixed_trajectory_seeds_across_all_eight_runs() {
        let mut changed_initial_seed = complete_report();
        let trial = changed_initial_seed
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::P1)
            .unwrap();
        trial
            .trajectories
            .get_mut(&ComponentGoldenTrajectoryV1::A)
            .unwrap()
            .runtime_binding
            .session_seed_sha256 = sha('0');
        trial.preseal_measurement.as_mut().unwrap().trajectories = trial.trajectories.clone();
        changed_initial_seed.validate_integrity().unwrap();
        assert!(changed_initial_seed.validate_complete().is_err());

        let mut changed_verification_seed = complete_report();
        let verification = changed_verification_seed
            .frozen_budget_verification_trials
            .get_mut(&FrozenBudgetVerificationTrialIdV1::Parallel)
            .unwrap();
        verification
            .trajectories
            .get_mut(&ComponentGoldenTrajectoryV1::A)
            .unwrap()
            .runtime_binding
            .session_seed_sha256 = sha('0');
        verification
            .preseal_measurement
            .as_mut()
            .unwrap()
            .trajectories = verification.trajectories.clone();
        changed_verification_seed.validate_integrity().unwrap();
        assert!(changed_verification_seed.validate_complete().is_err());
    }

    #[test]
    fn complete_report_rejects_relabelled_or_reused_artifact_receipts() {
        let mut relabelled = complete_report();
        let s1_receipt = relabelled.trials[&ComponentGoldenTrialIdV1::S1]
            .run_artifact_receipt
            .clone();
        relabelled
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::P1)
            .unwrap()
            .run_artifact_receipt = s1_receipt;
        assert!(relabelled.validate_integrity().is_err());
        assert!(relabelled.validate_complete().is_err());

        let mut reused = complete_report();
        let s1_receipt = reused.trials[&ComponentGoldenTrialIdV1::S1]
            .run_artifact_receipt
            .as_ref()
            .unwrap()
            .clone();
        let p1_receipt = reused
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::P1)
            .unwrap()
            .run_artifact_receipt
            .as_mut()
            .unwrap();
        p1_receipt.manifest_sha256 = s1_receipt.manifest_sha256;
        p1_receipt.through_commit_sequence = s1_receipt.through_commit_sequence;
        p1_receipt.evidence_head_sha256 = s1_receipt.evidence_head_sha256;
        let changed_p1_receipt = p1_receipt.clone();
        let reused_sealed_receipt = reused
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::P1)
            .unwrap()
            .sealed_resource_evidence
            .as_mut()
            .unwrap();
        reused_sealed_receipt.run_artifact_receipt = changed_p1_receipt;
        reused.validate_integrity().unwrap();
        assert!(reused.validate_complete().is_err());

        let mut swapped_measurements = complete_report();
        let s1 = swapped_measurements
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S1)
            .unwrap()
            .preseal_measurement
            .take();
        let p1 = swapped_measurements
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::P1)
            .unwrap()
            .preseal_measurement
            .take();
        swapped_measurements
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S1)
            .unwrap()
            .preseal_measurement = p1;
        swapped_measurements
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::P1)
            .unwrap()
            .preseal_measurement = s1;
        assert!(swapped_measurements.validate_integrity().is_err());

        let mut changed_outer_latency = complete_report();
        changed_outer_latency
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S1)
            .unwrap()
            .latency
            .insert(
                MeasurementLatencyKindV1::EventApplication,
                RawLatencySamplesNsV1::from_samples(vec![4, 5, 6]).unwrap(),
            );
        assert!(changed_outer_latency.validate_integrity().is_err());
    }

    #[test]
    fn report_rejects_self_reported_peak_or_transaction_increment_tampering() {
        let mut peak_tampered = complete_report();
        peak_tampered
            .budget_observations
            .get_mut(&ResourceBudgetPathV1::RunSessions)
            .unwrap()
            .peak
            .records = 1;
        assert!(peak_tampered.validate_integrity().is_err());

        let mut increment_tampered = complete_report();
        increment_tampered
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S1)
            .unwrap()
            .transaction_resource_increments[0]
            .increments
            .get_mut(&ResourceBudgetPathV1::RunSessions)
            .unwrap()
            .records = 4;
        assert!(increment_tampered.validate_integrity().is_err());
    }

    #[test]
    fn completed_report_rejects_self_reported_throughput_totals() {
        let mut report = complete_report();
        let trial = report
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S2)
            .unwrap();
        trial.throughput.breakdown.unique_fills += 1;
        trial.preseal_measurement.as_mut().unwrap().throughput = trial.throughput.clone();
        assert!(report.validate_integrity().is_err());
    }

    #[test]
    fn report_rejects_proc_or_cgroup_counter_regression() {
        let mut report = complete_report();
        let first = report
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S1)
            .unwrap()
            .resource_snapshots[0]
            .clone();
        let mut second = first;
        second.sample_index = 1;
        second.elapsed_ns = 2;
        second.procfs.io.rchar_bytes = 0;
        report
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S1)
            .unwrap()
            .resource_snapshots
            .push(second);
        assert!(report.validate_integrity().is_err());
    }

    #[test]
    fn report_requires_manifest_receipts_for_initial_and_verification_trials() {
        let mut initial = complete_report();
        initial
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S1)
            .unwrap()
            .run_artifact_receipt
            .as_mut()
            .unwrap()
            .through_commit_sequence = 0;
        assert!(initial.validate_integrity().is_err());

        let mut verification = complete_report();
        verification
            .frozen_budget_verification_trials
            .get_mut(&FrozenBudgetVerificationTrialIdV1::Parallel)
            .unwrap()
            .run_artifact_receipt
            .as_mut()
            .unwrap()
            .manifest_sha256 = Sha256Hex("not-a-sha".to_owned());
        assert!(verification.validate_integrity().is_err());

        let mut missing = complete_report();
        missing
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S1)
            .unwrap()
            .run_artifact_receipt = None;
        assert!(missing.validate_integrity().is_err());
    }

    #[test]
    fn final_serial_and_parallel_verifications_are_mandatory_and_budgeted() {
        let mut missing = complete_report();
        missing
            .frozen_budget_verification_trials
            .remove(&FrozenBudgetVerificationTrialIdV1::Parallel);
        assert!(missing.validate_integrity().is_err());

        let mut failed = complete_report();
        failed
            .frozen_budget_verification_trials
            .get_mut(&FrozenBudgetVerificationTrialIdV1::Serial)
            .unwrap()
            .outcome = MeasurementTrialOutcomeV1::Failed {
            stage: MeasurementFailureStageV1::Validation,
            reason: "冻结后串行验证失败".to_owned(),
        };
        failed.validate_integrity().unwrap();
        assert!(failed.validate_complete().is_err());

        let mut exceeded = complete_report();
        let exceeded_sessions = ResourceQuantityV1::new(
            exceeded.frozen_budgets.limits[&ResourceBudgetPathV1::RunSessions].max_records + 1,
            exceeded.frozen_budgets.limits[&ResourceBudgetPathV1::RunSessions].max_bytes + 1,
        );
        let verification = exceeded
            .frozen_budget_verification_trials
            .get_mut(&FrozenBudgetVerificationTrialIdV1::Serial)
            .unwrap();
        let sealed = verification.sealed_resource_evidence.as_mut().unwrap();
        sealed.high_water.observed.run.sessions = exceeded_sessions;
        sealed
            .postseal_resource_snapshot
            .internal
            .high_water
            .observed
            .run
            .sessions = exceeded_sessions;
        exceeded.validate_integrity().unwrap();
        assert!(exceeded.validate_complete().is_err());

        let mut divergent = complete_report();
        let verification = divergent
            .frozen_budget_verification_trials
            .get_mut(&FrozenBudgetVerificationTrialIdV1::Parallel)
            .unwrap();
        {
            let trajectory = verification
                .trajectories
                .get_mut(&ComponentGoldenTrajectoryV1::A)
                .unwrap();
            trajectory
                .logical_summary_input
                .logical_component_sha256
                .insert(TrajectoryLogicalComponentV1::Orders, sha('0'));
            trajectory.logical_summary_sha256 =
                trajectory.logical_summary_input.content_sha256().unwrap();
        }
        verification
            .preseal_measurement
            .as_mut()
            .unwrap()
            .trajectories = verification.trajectories.clone();
        divergent.validate_integrity().unwrap();
        assert!(divergent.validate_complete().is_err());
    }

    #[test]
    fn formal_report_rejects_unbounded_or_triggered_runtime_guards() {
        let mut unbounded = complete_report();
        unbounded
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S1)
            .unwrap()
            .resource_snapshots[0]
            .cgroup_v2
            .memory_limit = CgroupMemoryLimitV1::Unbounded;
        assert!(unbounded.validate_integrity().is_err());

        let mut event_triggered = complete_report();
        event_triggered
            .frozen_budget_verification_trials
            .get_mut(&FrozenBudgetVerificationTrialIdV1::Serial)
            .unwrap()
            .resource_snapshots[0]
            .cgroup_v2
            .memory_events
            .max = 1;
        assert!(event_triggered.validate_integrity().is_err());

        let mut fd_exceeded = complete_report();
        fd_exceeded
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::P1)
            .unwrap()
            .resource_snapshots[0]
            .procfs
            .open_file_descriptor_count = 1_025;
        assert!(fd_exceeded.validate_integrity().is_err());

        let mut self_reported_guard = complete_report();
        self_reported_guard
            .runtime_hard_guards
            .cgroup_memory_max_bytes += 1;
        assert!(self_reported_guard.validate_integrity().is_err());
    }

    #[test]
    fn build_identity_requires_a_full_lowercase_git_commit() {
        let mut short = complete_report();
        short.environment.build.source_commit = "abc123".to_owned();
        assert!(short.validate_integrity().is_err());

        let mut uppercase = complete_report();
        uppercase.environment.build.source_commit = "A".repeat(40);
        assert!(uppercase.validate_integrity().is_err());
    }

    #[test]
    fn failed_trial_is_retained_but_cannot_pass_completion() {
        let mut report = complete_report();
        let failed = report
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::P2)
            .unwrap();
        failed.outcome = MeasurementTrialOutcomeV1::Failed {
            stage: MeasurementFailureStageV1::Measurement,
            reason: "代表性失败".to_owned(),
        };
        failed.preseal_measurement = None;
        failed.run_artifact_receipt = None;
        failed.sealed_resource_evidence = None;
        report.validate_integrity().unwrap();
        assert!(matches!(
            report.validate_complete(),
            Err(SimulationMeasurementErrorV1::IncompleteTrial(
                ComponentGoldenTrialIdV1::P2
            ))
        ));

        let mut setup_failure = complete_report()
            .trials
            .remove(&ComponentGoldenTrialIdV1::P2)
            .unwrap();
        setup_failure.outcome = MeasurementTrialOutcomeV1::Failed {
            stage: MeasurementFailureStageV1::Setup,
            reason: "构建运行根前失败".to_owned(),
        };
        setup_failure.preseal_measurement = None;
        setup_failure.run_artifact_receipt = None;
        setup_failure.sealed_resource_evidence = None;
        setup_failure.throughput = TrialThroughputV1 {
            completed_path_deliveries: 0,
            completed_top_level_calls: 0,
            breakdown: ThroughputBreakdownV1::default(),
            elapsed_ns: 0,
            completed_work_items_per_second_milli: 0,
        };
        setup_failure.latency.clear();
        setup_failure.resource_snapshots.clear();
        setup_failure.transaction_resource_increments.clear();
        setup_failure.trajectories.clear();
        setup_failure.validate_integrity().unwrap();

        let complete = complete_report();
        let mut verification_setup_failure = complete.frozen_budget_verification_trials
            [&FrozenBudgetVerificationTrialIdV1::Serial]
            .clone();
        verification_setup_failure.outcome = MeasurementTrialOutcomeV1::Failed {
            stage: MeasurementFailureStageV1::Setup,
            reason: "冻结后验证运行根创建前失败".to_owned(),
        };
        verification_setup_failure.preseal_measurement = None;
        verification_setup_failure.run_artifact_receipt = None;
        verification_setup_failure.sealed_resource_evidence = None;
        verification_setup_failure.throughput = setup_failure.throughput.clone();
        verification_setup_failure.latency.clear();
        verification_setup_failure.resource_snapshots.clear();
        verification_setup_failure.trajectories.clear();
        verification_setup_failure
            .validate_integrity(
                &complete.workload_contract_sha256,
                &complete.frozen_budgets.content_sha256().unwrap(),
            )
            .unwrap();

        let mut after_seal = complete_report();
        after_seal
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::P2)
            .unwrap()
            .outcome = MeasurementTrialOutcomeV1::Failed {
            stage: MeasurementFailureStageV1::Validation,
            reason: "封口后验证失败".to_owned(),
        };
        after_seal.validate_integrity().unwrap();
        assert!(after_seal.validate_complete().is_err());

        let mut wrong_identity = complete_report();
        let s1_receipt = wrong_identity.trials[&ComponentGoldenTrialIdV1::S1]
            .run_artifact_receipt
            .clone();
        let failed = wrong_identity
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::P2)
            .unwrap();
        failed.outcome = MeasurementTrialOutcomeV1::Failed {
            stage: MeasurementFailureStageV1::Persistence,
            reason: "密封后仍保留回执".to_owned(),
        };
        failed.run_artifact_receipt = s1_receipt;
        assert!(wrong_identity.validate_integrity().is_err());
    }

    #[test]
    fn multi_process_report_accepts_fixed_context_but_rejects_reused_cgroup_or_process_identity() {
        let report = complete_report();
        report.validate_complete().unwrap();

        let s1_environment = report.trials[&ComponentGoldenTrialIdV1::S1]
            .preseal_measurement
            .as_ref()
            .unwrap()
            .environment
            .clone();
        let mut reused_cgroup = report.clone();
        reused_cgroup
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::P1)
            .unwrap()
            .preseal_measurement
            .as_mut()
            .unwrap()
            .environment
            .cgroup_path_sha256 = s1_environment.cgroup_path_sha256;
        reused_cgroup.validate_integrity().unwrap();
        assert!(reused_cgroup.validate_complete().is_err());

        let s1_process = report.trials[&ComponentGoldenTrialIdV1::S1].resource_snapshots[0]
            .procfs
            .clone();
        let mut reused_process = report;
        let p1 = reused_process
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::P1)
            .unwrap();
        for snapshot in &mut p1.resource_snapshots {
            snapshot.procfs.pid = s1_process.pid;
            snapshot.procfs.process_start_time_ticks = s1_process.process_start_time_ticks;
        }
        p1.preseal_measurement.as_mut().unwrap().resource_snapshots = p1.resource_snapshots.clone();
        let postseal = &mut p1
            .sealed_resource_evidence
            .as_mut()
            .unwrap()
            .postseal_resource_snapshot;
        postseal.procfs.pid = s1_process.pid;
        postseal.procfs.process_start_time_ticks = s1_process.process_start_time_ticks;
        reused_process.validate_integrity().unwrap();
        assert!(reused_process.validate_complete().is_err());
    }

    #[test]
    fn verification_uses_final_build_and_exact_frozen_memory_guards() {
        let mut report = complete_report();
        for verification in report.frozen_budget_verification_trials.values_mut() {
            let environment = &mut verification
                .preseal_measurement
                .as_mut()
                .unwrap()
                .environment;
            environment.build.release_binary_sha256 = sha('9');
            environment.build.source_tree_sha256 = sha('8');
            environment.build.source_commit = "b".repeat(40);
        }
        report.validate_complete().unwrap();

        let mut wrong_guard = report;
        let verification = wrong_guard
            .frozen_budget_verification_trials
            .get_mut(&FrozenBudgetVerificationTrialIdV1::Serial)
            .unwrap();
        verification
            .preseal_measurement
            .as_mut()
            .unwrap()
            .runtime_hard_guards
            .cgroup_memory_max_bytes += SIXTY_FOUR_MIB;
        assert!(wrong_guard.validate_integrity().is_err());
    }

    #[test]
    fn sealed_resource_evidence_supplies_nonzero_artifact_peak_and_increment() {
        let report = complete_report();
        let trial = &report.trials[&ComponentGoldenTrialIdV1::S1];
        assert_eq!(
            usage_by_budget_path(
                &trial
                    .resource_snapshots
                    .last()
                    .unwrap()
                    .internal
                    .high_water
                    .observed
            )[&ResourceBudgetPathV1::DurableArtifacts],
            ResourceQuantityV1::default()
        );
        let artifact_observation =
            report.budget_observations[&ResourceBudgetPathV1::DurableArtifacts];
        assert!(artifact_observation.peak.records > 0);
        assert!(artifact_observation.peak.bytes > 0);
        assert!(
            artifact_observation
                .max_single_transaction_increment
                .records
                > 0
        );
        assert!(artifact_observation.max_single_transaction_increment.bytes > 0);
        report.validate_complete().unwrap();
    }

    #[test]
    fn sealed_evidence_rejects_high_water_receipt_audit_and_publication_tampering() {
        let mut no_artifact = complete_report();
        no_artifact
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S1)
            .unwrap()
            .sealed_resource_evidence
            .as_mut()
            .unwrap()
            .high_water
            .observed
            .durable
            .artifacts = ResourceQuantityV1::default();
        assert!(no_artifact.validate_integrity().is_err());

        let mut root_does_not_cover = complete_report();
        let sealed = root_does_not_cover
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S1)
            .unwrap()
            .sealed_resource_evidence
            .as_mut()
            .unwrap();
        sealed.high_water.observed.durable.runtime_root = ResourceQuantityV1::new(1, 1);
        assert!(root_does_not_cover.validate_integrity().is_err());

        let mut receipt = complete_report();
        receipt
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S1)
            .unwrap()
            .sealed_resource_evidence
            .as_mut()
            .unwrap()
            .run_artifact_receipt
            .manifest_sha256 = sha('0');
        assert!(receipt.validate_integrity().is_err());

        let mut audit = complete_report();
        let reused_audit_sha256 = audit.trials[&ComponentGoldenTrialIdV1::S1]
            .sealed_resource_evidence
            .as_ref()
            .unwrap()
            .audit_file_sha256
            .clone();
        audit
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::P1)
            .unwrap()
            .sealed_resource_evidence
            .as_mut()
            .unwrap()
            .audit_file_sha256 = reused_audit_sha256;
        audit.validate_integrity().unwrap();
        assert!(audit.validate_complete().is_err());

        let mut false_publication = complete_report();
        false_publication
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S1)
            .unwrap()
            .sealed_resource_evidence
            .as_mut()
            .unwrap()
            .artifact_publication_increment
            .insert(
                ResourceBudgetPathV1::DurableEvidence,
                ResourceQuantityV1::new(1, 1),
            );
        assert!(false_publication.validate_integrity().is_err());
    }

    #[test]
    fn sealed_snapshot_and_latency_are_mandatory_and_bound_to_outer_measurement() {
        let mut missing_latency = complete_report();
        missing_latency
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S1)
            .unwrap()
            .sealed_resource_evidence
            .as_mut()
            .unwrap()
            .seal_latency_ns = 0;
        assert!(missing_latency.validate_integrity().is_err());

        let mut altered_outer = complete_report();
        altered_outer
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S1)
            .unwrap()
            .latency
            .get_mut(&MeasurementLatencyKindV1::CheckpointOrSeal)
            .unwrap()
            .samples_ns
            .pop();
        assert!(altered_outer.validate_integrity().is_err());

        let mut stale_postseal = complete_report();
        stale_postseal
            .trials
            .get_mut(&ComponentGoldenTrialIdV1::S1)
            .unwrap()
            .sealed_resource_evidence
            .as_mut()
            .unwrap()
            .postseal_resource_snapshot
            .sample_index += 1;
        assert!(stale_postseal.validate_integrity().is_err());
    }

    #[test]
    fn cgroup_peak_participates_in_frozen_process_memory_budget() {
        let observations = RESOURCE_BUDGET_PATHS_V1
            .into_iter()
            .map(|path| {
                (
                    path,
                    ResourceBudgetObservationV1 {
                        peak: ResourceQuantityV1::new(1, 1),
                        max_single_transaction_increment: ResourceQuantityV1::new(1, 1),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let frozen = freeze_resource_budgets_v1(
            &observations,
            129 * 1024 * 1024,
            measured_failure_reserve(),
        )
        .unwrap();
        assert_eq!(frozen.process_memory_max_bytes, 192 * 1024 * 1024);

        let mut snapshot =
            complete_report().trials[&ComponentGoldenTrialIdV1::S1].resource_snapshots[0].clone();
        snapshot.cgroup_v2.memory_limit = CgroupMemoryLimitV1::Bytes {
            bytes: 256 * 1024 * 1024,
        };
        snapshot.cgroup_v2.memory_peak_bytes = Some(129 * 1024 * 1024);
        validate_process_snapshot_within_frozen_budgets(&snapshot, &frozen).unwrap();

        let mut too_low = frozen;
        too_low.process_memory_max_bytes = 128 * 1024 * 1024;
        assert!(validate_process_snapshot_within_frozen_budgets(&snapshot, &too_low).is_err());
    }

    #[test]
    fn cache_precondition_is_fixed_content_not_a_self_reported_digest() {
        let mut report = complete_report();
        report.environment.cache_precondition_sha256 = sha('0');
        assert!(report.validate_integrity().is_err());

        let mut report = complete_report();
        report
            .environment
            .cache_precondition
            .release_unit_test_binary_precompiled_outside_measurement_scope = false;
        assert!(report.validate_integrity().is_err());
    }
}
