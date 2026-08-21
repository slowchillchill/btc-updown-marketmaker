use std::collections::BTreeMap;
use std::fmt;

use crate::resource::{
    ResourceQuantityV1, SimulationDurableResourceUsageV1, SimulationResourceHighWaterV1,
    SimulationResourceUsageV1, SimulationRunResourceUsageV1, SimulationSessionResourceUsageV1,
    canonical_json_sha256_v1,
};
use contracts::{AccountInstanceKeyV1, PlatformSourceV1, Sha256Hex, SimCallerRoleV1};
use serde::{Deserialize, Serialize};

pub const COMPONENT_GOLDEN_LOAD_SCHEMA_V1: &str = "component_golden_load_v1";
pub const COMPONENT_GOLDEN_LOAD_ID_V1: &str = "COMPONENT_GOLDEN_V1";
pub const COMPONENT_GOLDEN_RUN_MEASUREMENT_SCHEMA_V1: &str = "component_golden_run_measurement_v1";
pub const TRAJECTORY_LOGICAL_SUMMARY_SCHEMA_V1: &str =
    "component_golden_trajectory_logical_summary_v1";

const COMPONENT_GOLDEN_LOAD_HASH_DOMAIN_V1: &str = "sim-system/component-golden-load/v1";
const TRAJECTORY_LOGICAL_SUMMARY_HASH_DOMAIN_V1: &str =
    "sim-system/component-golden-trajectory-logical-summary/v1";
const COMPONENT_GOLDEN_RUN_MEASUREMENT_HASH_DOMAIN_V1: &str =
    "sim-system/component-golden-run-measurement/v1";
const MEASUREMENT_CACHE_PRECONDITION_HASH_DOMAIN_V1: &str =
    "sim-system/measurement-cache-precondition/v1";

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentGoldenIdentityIsolationV1 {
    pub trajectory_identity_is_distinct: bool,
    pub seed_is_distinct: bool,
    pub bridge_identity_is_distinct: bool,
    pub account_identity_is_distinct: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentGoldenEventKindCountsV1 {
    pub initial_l2_snapshot: u64,
    pub level_execution_transition: u64,
    pub market_resolution_terminal: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentGoldenEventLoadV1 {
    pub markets_per_trajectory: u16,
    pub market_duration_seconds: u16,
    pub markets_overlap: bool,
    pub unique_normative_events_per_market: u16,
    pub unique_event_ids: u16,
    pub unique_event_kinds: ComponentGoldenEventKindCountsV1,
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
                unique_event_kinds: ComponentGoldenEventKindCountsV1 {
                    initial_l2_snapshot: 4,
                    level_execution_transition: 4,
                    market_resolution_terminal: 2,
                },
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
pub struct ComponentGoldenQueryMethodBreakdownV1 {
    pub calls: u64,
    pub pages: u64,
    pub return_rows: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentGoldenQueryMethodsV1 {
    pub order: ComponentGoldenQueryMethodBreakdownV1,
    pub orders: ComponentGoldenQueryMethodBreakdownV1,
    pub trades: ComponentGoldenQueryMethodBreakdownV1,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentGoldenQuerySourceBreakdownV1 {
    pub hot_state: ComponentGoldenQueryMethodsV1,
    pub retained_projection: ComponentGoldenQueryMethodsV1,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThroughputBreakdownV1 {
    pub event_kind_path_deliveries: ComponentGoldenEventKindCountsV1,
    pub request_batch_items: u64,
    pub unique_fills: u64,
    pub affected_orders: u64,
    pub order_feedback: u64,
    pub trade_feedback: u64,
    pub settlement_feedback: u64,
    pub query_calls: u64,
    pub query_pages: u64,
    pub query_return_rows: u64,
    pub query_sources: ComponentGoldenQuerySourceBreakdownV1,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThroughputRatesPerSecondMilliV1 {
    pub event_path_deliveries: u64,
    pub request_calls: u64,
    pub request_batch_items: u64,
    pub unique_fills: u64,
    pub query_calls: u64,
    pub query_pages: u64,
    pub query_return_rows: u64,
}

impl ThroughputRatesPerSecondMilliV1 {
    pub fn from_counts(
        completed_path_deliveries: u16,
        completed_top_level_calls: u16,
        breakdown: ThroughputBreakdownV1,
        elapsed_ns: u64,
    ) -> Result<Self, SimulationMeasurementErrorV1> {
        Ok(Self {
            event_path_deliveries: throughput_per_second_milli(
                u64::from(completed_path_deliveries),
                elapsed_ns,
            )?,
            request_calls: throughput_per_second_milli(
                u64::from(completed_top_level_calls),
                elapsed_ns,
            )?,
            request_batch_items: throughput_per_second_milli(
                breakdown.request_batch_items,
                elapsed_ns,
            )?,
            unique_fills: throughput_per_second_milli(breakdown.unique_fills, elapsed_ns)?,
            query_calls: throughput_per_second_milli(breakdown.query_calls, elapsed_ns)?,
            query_pages: throughput_per_second_milli(breakdown.query_pages, elapsed_ns)?,
            query_return_rows: throughput_per_second_milli(
                breakdown.query_return_rows,
                elapsed_ns,
            )?,
        })
    }
}

impl ThroughputBreakdownV1 {
    pub(crate) fn checked_add(self, other: Self) -> Result<Self, SimulationMeasurementErrorV1> {
        let add = |left: u64, right: u64| {
            left.checked_add(right)
                .ok_or(SimulationMeasurementErrorV1::ArithmeticOverflow(
                    "throughput_breakdown",
                ))
        };
        let add_method = |left: ComponentGoldenQueryMethodBreakdownV1,
                          right: ComponentGoldenQueryMethodBreakdownV1| {
            Ok::<_, SimulationMeasurementErrorV1>(ComponentGoldenQueryMethodBreakdownV1 {
                calls: add(left.calls, right.calls)?,
                pages: add(left.pages, right.pages)?,
                return_rows: add(left.return_rows, right.return_rows)?,
            })
        };
        let add_methods = |left: ComponentGoldenQueryMethodsV1,
                           right: ComponentGoldenQueryMethodsV1| {
            Ok::<_, SimulationMeasurementErrorV1>(ComponentGoldenQueryMethodsV1 {
                order: add_method(left.order, right.order)?,
                orders: add_method(left.orders, right.orders)?,
                trades: add_method(left.trades, right.trades)?,
            })
        };
        Ok(Self {
            event_kind_path_deliveries: ComponentGoldenEventKindCountsV1 {
                initial_l2_snapshot: add(
                    self.event_kind_path_deliveries.initial_l2_snapshot,
                    other.event_kind_path_deliveries.initial_l2_snapshot,
                )?,
                level_execution_transition: add(
                    self.event_kind_path_deliveries.level_execution_transition,
                    other.event_kind_path_deliveries.level_execution_transition,
                )?,
                market_resolution_terminal: add(
                    self.event_kind_path_deliveries.market_resolution_terminal,
                    other.event_kind_path_deliveries.market_resolution_terminal,
                )?,
            },
            request_batch_items: add(self.request_batch_items, other.request_batch_items)?,
            unique_fills: add(self.unique_fills, other.unique_fills)?,
            affected_orders: add(self.affected_orders, other.affected_orders)?,
            order_feedback: add(self.order_feedback, other.order_feedback)?,
            trade_feedback: add(self.trade_feedback, other.trade_feedback)?,
            settlement_feedback: add(self.settlement_feedback, other.settlement_feedback)?,
            query_calls: add(self.query_calls, other.query_calls)?,
            query_pages: add(self.query_pages, other.query_pages)?,
            query_return_rows: add(self.query_return_rows, other.query_return_rows)?,
            query_sources: ComponentGoldenQuerySourceBreakdownV1 {
                hot_state: add_methods(
                    self.query_sources.hot_state,
                    other.query_sources.hot_state,
                )?,
                retained_projection: add_methods(
                    self.query_sources.retained_projection,
                    other.query_sources.retained_projection,
                )?,
            },
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
    pub rates_per_second_milli: ThroughputRatesPerSecondMilliV1,
}

impl TrajectoryThroughputV1 {
    fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        if self.completed_path_deliveries != 20
            || self.completed_top_level_calls != 19
            || self.breakdown.event_kind_path_deliveries
                != (ComponentGoldenEventKindCountsV1 {
                    initial_l2_snapshot: 8,
                    level_execution_transition: 8,
                    market_resolution_terminal: 4,
                })
            || self.breakdown.request_batch_items != 106
            || self.breakdown.unique_fills == 0
            || self.breakdown.affected_orders == 0
            || self.breakdown.order_feedback == 0
            || self.breakdown.trade_feedback == 0
            || self.breakdown.settlement_feedback == 0
            || self.breakdown.query_calls != 6
            || self.breakdown.query_pages != 4
            || self.breakdown.query_return_rows != 213
            || self.breakdown.query_sources
                != (ComponentGoldenQuerySourceBreakdownV1 {
                    hot_state: ComponentGoldenQueryMethodsV1 {
                        order: ComponentGoldenQueryMethodBreakdownV1 {
                            calls: 2,
                            pages: 0,
                            return_rows: 2,
                        },
                        orders: ComponentGoldenQueryMethodBreakdownV1 {
                            calls: 2,
                            pages: 2,
                            return_rows: 102,
                        },
                        trades: ComponentGoldenQueryMethodBreakdownV1 {
                            calls: 2,
                            pages: 2,
                            return_rows: 109,
                        },
                    },
                    retained_projection: ComponentGoldenQueryMethodsV1 {
                        order: ComponentGoldenQueryMethodBreakdownV1 {
                            calls: 0,
                            pages: 0,
                            return_rows: 0,
                        },
                        orders: ComponentGoldenQueryMethodBreakdownV1 {
                            calls: 0,
                            pages: 0,
                            return_rows: 0,
                        },
                        trades: ComponentGoldenQueryMethodBreakdownV1 {
                            calls: 0,
                            pages: 0,
                            return_rows: 0,
                        },
                    },
                })
            || self.elapsed_ns == 0
            || self.rates_per_second_milli
                != ThroughputRatesPerSecondMilliV1::from_counts(
                    self.completed_path_deliveries,
                    self.completed_top_level_calls,
                    self.breakdown,
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
    pub rates_per_second_milli: ThroughputRatesPerSecondMilliV1,
}

impl TrialThroughputV1 {
    fn validate(&self) -> Result<(), SimulationMeasurementErrorV1> {
        if self.completed_path_deliveries > 40 || self.completed_top_level_calls > 38 {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "试次总吞吐计数超过 COMPONENT_GOLDEN_V1".to_owned(),
            ));
        }
        let expected_rates = if self.elapsed_ns == 0 {
            if self.completed_path_deliveries != 0
                || self.completed_top_level_calls != 0
                || self.breakdown != ThroughputBreakdownV1::default()
            {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "非空试次吞吐必须记录 elapsed_ns".to_owned(),
                ));
            }
            ThroughputRatesPerSecondMilliV1::default()
        } else {
            ThroughputRatesPerSecondMilliV1::from_counts(
                self.completed_path_deliveries,
                self.completed_top_level_calls,
                self.breakdown,
                self.elapsed_ns,
            )?
        };
        if self.rates_per_second_milli != expected_rates {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "试次分栏吞吐速率不能由各自原始计数与 elapsed_ns 重算".to_owned(),
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
        if self.latency[&MeasurementLatencyKindV1::RequestResponse]
            .samples_ns
            .len()
            != 13
            || self.latency[&MeasurementLatencyKindV1::QueryResponse]
                .samples_ns
                .len()
                != 6
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "逐轨迹请求响应与查询响应延迟必须分别覆盖 13 与 6 个固定调用".to_owned(),
            ));
        }
        let trajectory_end_to_end = &self.latency[&MeasurementLatencyKindV1::TrialEndToEnd];
        if trajectory_end_to_end.samples_ns != [self.throughput.elapsed_ns] {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "逐轨迹端到端延迟必须精确等于该轨迹自身的墙钟时长".to_owned(),
            ));
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
            if cycle.logical_completion_ns > self.throughput.elapsed_ns {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "逻辑滚动周期完成时间不得晚于所属轨迹墙钟结束".to_owned(),
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
        self.validate_latency_and_elapsed_bindings()?;
        validate_trajectory_identity_isolation(&self.trajectories)
    }

    fn validate_latency_and_elapsed_bindings(&self) -> Result<(), SimulationMeasurementErrorV1> {
        for kind in MEASUREMENT_LATENCY_KINDS_V1 {
            let expected_samples = if kind == MeasurementLatencyKindV1::TrialEndToEnd {
                vec![self.throughput.elapsed_ns]
            } else {
                self.trajectories
                    .values()
                    .flat_map(|trajectory| trajectory.latency[&kind].samples_ns.iter().copied())
                    .collect::<Vec<_>>()
            };
            let expected = RawLatencySamplesNsV1::from_samples(expected_samples)?;
            if self.latency[&kind] != expected {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "封口前试次延迟必须从 A、B 轨迹原始样本与整试次墙钟精确重算".to_owned(),
                ));
            }
        }

        let mut trajectory_elapsed_sum = 0_u64;
        for result in self.trajectories.values() {
            if result.throughput.elapsed_ns > self.throughput.elapsed_ns {
                return Err(SimulationMeasurementErrorV1::ReportMismatch(
                    "逐轨迹墙钟不得大于整试次墙钟".to_owned(),
                ));
            }
            trajectory_elapsed_sum = trajectory_elapsed_sum
                .checked_add(result.throughput.elapsed_ns)
                .ok_or(SimulationMeasurementErrorV1::ArithmeticOverflow(
                    "trajectory_elapsed_sum",
                ))?;
        }
        let mode = match self.measurement_run_identity {
            MeasurementRunIdentityV1::ComponentTrial(trial_id) => trial_id.mode(),
            MeasurementRunIdentityV1::FrozenBudgetVerification(verification_id) => {
                verification_id.mode()
            }
        };
        if mode == ComponentGoldenMeasurementModeV1::Serial
            && trajectory_elapsed_sum > self.throughput.elapsed_ns
        {
            return Err(SimulationMeasurementErrorV1::ReportMismatch(
                "串行 A、B 轨迹墙钟之和不得大于整试次墙钟".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn content_sha256(&self) -> Result<Sha256Hex, SimulationMeasurementErrorV1> {
        self.validate()?;
        measurement_hash(COMPONENT_GOLDEN_RUN_MEASUREMENT_HASH_DOMAIN_V1, self)
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
        SimulationMeasurementErrorV1::ArithmeticOverflow("throughput_per_second_milli")
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
