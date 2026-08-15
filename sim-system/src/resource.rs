use std::fmt;

use contracts::Sha256Hex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SIMULATION_RESOURCE_POLICY_SCHEMA_V1: &str = "simulation_resource_policy_v1";
pub const COMPONENT_GOLDEN_RESOURCE_POLICY_ID_V1: &str = "component_golden_resource_policy_v1";
pub const SIMULATION_RESOURCE_POLICY_HASH_DOMAIN_V1: &str =
    "sim-system/simulation-resource-policy/v1";

// These deliberately generous values are pre-freeze placeholders. Task 4.4's
// owner must replace them with the measured COMPONENT_GOLDEN limits before the
// task is accepted; they are not production capacity claims.
const PREFREEZE_SESSION_MAX_RECORDS_V1: u64 = 1_000_000;
const PREFREEZE_SESSION_MAX_BYTES_V1: u64 = 1 << 30;
const PREFREEZE_RUN_MAX_RECORDS_V1: u64 = 4_000_000;
const PREFREEZE_RUN_MAX_BYTES_V1: u64 = 8 << 30;
const PREFREEZE_DURABLE_MAX_RECORDS_V1: u64 = 4_000_000;
const PREFREEZE_DURABLE_MAX_BYTES_V1: u64 = 16 << 30;
const PREFREEZE_DURABLE_ROOT_MAX_RECORDS_V1: u64 = 12_000_000;
const PREFREEZE_DURABLE_ROOT_MAX_BYTES_V1: u64 = 32 << 30;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceQuantityV1 {
    pub records: u64,
    pub bytes: u64,
}

impl ResourceQuantityV1 {
    pub const fn new(records: u64, bytes: u64) -> Self {
        Self { records, bytes }
    }

    fn observe_max(&mut self, candidate: Self) {
        self.records = self.records.max(candidate.records);
        self.bytes = self.bytes.max(candidate.bytes);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLimitV1 {
    pub max_records: u64,
    pub max_bytes: u64,
}

impl ResourceLimitV1 {
    pub const fn new(max_records: u64, max_bytes: u64) -> Self {
        Self {
            max_records,
            max_bytes,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationSessionResourcePolicyV1 {
    pub in_memory_state: ResourceLimitV1,
    pub hot_markets: ResourceLimitV1,
    pub input_queue: ResourceLimitV1,
    pub request_queue: ResourceLimitV1,
    pub feedback_queue: ResourceLimitV1,
    pub active_orders: ResourceLimitV1,
    pub internal_events: ResourceLimitV1,
    pub query_cache: ResourceLimitV1,
    pub indexes: ResourceLimitV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationRunResourcePolicyV1 {
    pub in_memory_state: ResourceLimitV1,
    pub sessions: ResourceLimitV1,
    pub hot_markets: ResourceLimitV1,
    pub input_queue: ResourceLimitV1,
    pub request_queue: ResourceLimitV1,
    pub feedback_queue: ResourceLimitV1,
    pub active_orders: ResourceLimitV1,
    pub internal_events: ResourceLimitV1,
    pub query_cache: ResourceLimitV1,
    pub indexes: ResourceLimitV1,
    pub capital_fence: ResourceLimitV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationDurableResourcePolicyV1 {
    pub evidence: ResourceLimitV1,
    pub checkpoints: ResourceLimitV1,
    pub artifacts: ResourceLimitV1,
    pub runtime_root: ResourceLimitV1,
    pub failure_reserve: SimulationFailureEvidenceReserveV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationFailureEvidenceReserveV1 {
    pub run_checkpoint_growth: ResourceQuantityV1,
    pub evidence: ResourceQuantityV1,
    pub checkpoints: ResourceQuantityV1,
    pub runtime_root: ResourceQuantityV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationResourcePolicyV1 {
    pub schema_version: String,
    pub policy_id: String,
    pub session: SimulationSessionResourcePolicyV1,
    pub run: SimulationRunResourcePolicyV1,
    pub durable: SimulationDurableResourcePolicyV1,
}

impl SimulationResourcePolicyV1 {
    pub fn component_golden_v1() -> Self {
        let session_limit = ResourceLimitV1::new(
            PREFREEZE_SESSION_MAX_RECORDS_V1,
            PREFREEZE_SESSION_MAX_BYTES_V1,
        );
        let run_limit =
            ResourceLimitV1::new(PREFREEZE_RUN_MAX_RECORDS_V1, PREFREEZE_RUN_MAX_BYTES_V1);
        let durable_limit = ResourceLimitV1::new(
            PREFREEZE_DURABLE_MAX_RECORDS_V1,
            PREFREEZE_DURABLE_MAX_BYTES_V1,
        );

        Self {
            schema_version: SIMULATION_RESOURCE_POLICY_SCHEMA_V1.to_owned(),
            policy_id: COMPONENT_GOLDEN_RESOURCE_POLICY_ID_V1.to_owned(),
            session: SimulationSessionResourcePolicyV1 {
                in_memory_state: session_limit,
                hot_markets: session_limit,
                input_queue: session_limit,
                request_queue: session_limit,
                feedback_queue: session_limit,
                active_orders: session_limit,
                internal_events: session_limit,
                query_cache: session_limit,
                indexes: session_limit,
            },
            run: SimulationRunResourcePolicyV1 {
                in_memory_state: run_limit,
                sessions: ResourceLimitV1::new(1_024, PREFREEZE_RUN_MAX_BYTES_V1),
                hot_markets: run_limit,
                input_queue: run_limit,
                request_queue: run_limit,
                feedback_queue: run_limit,
                active_orders: run_limit,
                internal_events: run_limit,
                query_cache: run_limit,
                indexes: run_limit,
                capital_fence: ResourceLimitV1::new(1_000_000, 1 << 29),
            },
            durable: SimulationDurableResourcePolicyV1 {
                evidence: durable_limit,
                checkpoints: durable_limit,
                artifacts: durable_limit,
                runtime_root: ResourceLimitV1::new(
                    PREFREEZE_DURABLE_ROOT_MAX_RECORDS_V1,
                    PREFREEZE_DURABLE_ROOT_MAX_BYTES_V1,
                ),
                failure_reserve: SimulationFailureEvidenceReserveV1 {
                    run_checkpoint_growth: ResourceQuantityV1::new(1, 1 << 20),
                    evidence: ResourceQuantityV1::new(1, 1 << 20),
                    checkpoints: ResourceQuantityV1::new(1, 1 << 20),
                    runtime_root: ResourceQuantityV1::new(3, 2 << 20),
                },
            },
        }
    }

    pub fn content_sha256(&self) -> Result<Sha256Hex, SimulationResourceErrorV1> {
        canonical_json_sha256_v1(SIMULATION_RESOURCE_POLICY_HASH_DOMAIN_V1, self)
    }

    pub fn validate_candidate(
        &self,
        candidate: &SimulationResourceUsageV1,
    ) -> Result<(), SimulationResourceErrorV1> {
        self.validate_identity()?;
        let mut admission = self.clone();
        admission.run.in_memory_state.max_records = admission
            .run
            .in_memory_state
            .max_records
            .checked_sub(self.durable.failure_reserve.run_checkpoint_growth.records)
            .ok_or_else(|| {
                SimulationResourceErrorV1::Serialization(
                    "运行检查点失败预留记录数量超过硬上限".to_owned(),
                )
            })?;
        for limit in [
            &mut admission.run.in_memory_state,
            &mut admission.run.sessions,
        ] {
            limit.max_bytes = limit
                .max_bytes
                .checked_sub(self.durable.failure_reserve.run_checkpoint_growth.bytes)
                .ok_or_else(|| {
                    SimulationResourceErrorV1::Serialization(
                        "运行检查点失败预留字节超过硬上限".to_owned(),
                    )
                })?;
        }
        for (limit, reserve) in [
            (
                &mut admission.durable.evidence,
                self.durable.failure_reserve.evidence,
            ),
            (
                &mut admission.durable.checkpoints,
                self.durable.failure_reserve.checkpoints,
            ),
            (
                &mut admission.durable.runtime_root,
                self.durable.failure_reserve.runtime_root,
            ),
        ] {
            limit.max_records =
                limit
                    .max_records
                    .checked_sub(reserve.records)
                    .ok_or_else(|| {
                        SimulationResourceErrorV1::Serialization(
                            "失败证据保留记录数量超过硬上限".to_owned(),
                        )
                    })?;
            limit.max_bytes = limit.max_bytes.checked_sub(reserve.bytes).ok_or_else(|| {
                SimulationResourceErrorV1::Serialization("失败证据保留字节超过硬上限".to_owned())
            })?;
        }
        admission.validate_hard_candidate(candidate)
    }

    fn validate_hard_candidate(
        &self,
        candidate: &SimulationResourceUsageV1,
    ) -> Result<(), SimulationResourceErrorV1> {
        self.validate_identity()?;

        // The order is contractual: local admission is checked before shared
        // run/durable admission, and check_limit checks records before bytes.
        let checks = [
            (
                ResourceLimitScopeV1::Session,
                ResourceLimitKindV1::InMemoryState,
                candidate.session.in_memory_state,
                self.session.in_memory_state,
            ),
            (
                ResourceLimitScopeV1::Session,
                ResourceLimitKindV1::HotMarkets,
                candidate.session.hot_markets,
                self.session.hot_markets,
            ),
            (
                ResourceLimitScopeV1::Session,
                ResourceLimitKindV1::InputQueue,
                candidate.session.input_queue,
                self.session.input_queue,
            ),
            (
                ResourceLimitScopeV1::Session,
                ResourceLimitKindV1::RequestQueue,
                candidate.session.request_queue,
                self.session.request_queue,
            ),
            (
                ResourceLimitScopeV1::Session,
                ResourceLimitKindV1::FeedbackQueue,
                candidate.session.feedback_queue,
                self.session.feedback_queue,
            ),
            (
                ResourceLimitScopeV1::Session,
                ResourceLimitKindV1::ActiveOrders,
                candidate.session.active_orders,
                self.session.active_orders,
            ),
            (
                ResourceLimitScopeV1::Session,
                ResourceLimitKindV1::InternalEvents,
                candidate.session.internal_events,
                self.session.internal_events,
            ),
            (
                ResourceLimitScopeV1::Session,
                ResourceLimitKindV1::QueryCache,
                candidate.session.query_cache,
                self.session.query_cache,
            ),
            (
                ResourceLimitScopeV1::Session,
                ResourceLimitKindV1::Indexes,
                candidate.session.indexes,
                self.session.indexes,
            ),
            (
                ResourceLimitScopeV1::Run,
                ResourceLimitKindV1::InMemoryState,
                candidate.run.in_memory_state,
                self.run.in_memory_state,
            ),
            (
                ResourceLimitScopeV1::Run,
                ResourceLimitKindV1::Sessions,
                candidate.run.sessions,
                self.run.sessions,
            ),
            (
                ResourceLimitScopeV1::Run,
                ResourceLimitKindV1::HotMarkets,
                candidate.run.hot_markets,
                self.run.hot_markets,
            ),
            (
                ResourceLimitScopeV1::Run,
                ResourceLimitKindV1::InputQueue,
                candidate.run.input_queue,
                self.run.input_queue,
            ),
            (
                ResourceLimitScopeV1::Run,
                ResourceLimitKindV1::RequestQueue,
                candidate.run.request_queue,
                self.run.request_queue,
            ),
            (
                ResourceLimitScopeV1::Run,
                ResourceLimitKindV1::FeedbackQueue,
                candidate.run.feedback_queue,
                self.run.feedback_queue,
            ),
            (
                ResourceLimitScopeV1::Run,
                ResourceLimitKindV1::ActiveOrders,
                candidate.run.active_orders,
                self.run.active_orders,
            ),
            (
                ResourceLimitScopeV1::Run,
                ResourceLimitKindV1::InternalEvents,
                candidate.run.internal_events,
                self.run.internal_events,
            ),
            (
                ResourceLimitScopeV1::Run,
                ResourceLimitKindV1::QueryCache,
                candidate.run.query_cache,
                self.run.query_cache,
            ),
            (
                ResourceLimitScopeV1::Run,
                ResourceLimitKindV1::Indexes,
                candidate.run.indexes,
                self.run.indexes,
            ),
            (
                ResourceLimitScopeV1::Run,
                ResourceLimitKindV1::CapitalFence,
                candidate.run.capital_fence,
                self.run.capital_fence,
            ),
            (
                ResourceLimitScopeV1::Durable,
                ResourceLimitKindV1::Evidence,
                candidate.durable.evidence,
                self.durable.evidence,
            ),
            (
                ResourceLimitScopeV1::Durable,
                ResourceLimitKindV1::Checkpoints,
                candidate.durable.checkpoints,
                self.durable.checkpoints,
            ),
            (
                ResourceLimitScopeV1::Durable,
                ResourceLimitKindV1::Artifacts,
                candidate.durable.artifacts,
                self.durable.artifacts,
            ),
            (
                ResourceLimitScopeV1::Durable,
                ResourceLimitKindV1::RuntimeRoot,
                candidate.durable.runtime_root,
                self.durable.runtime_root,
            ),
        ];
        for (scope, kind, quantity, limit) in checks {
            check_limit(&self.policy_id, scope, kind, quantity, limit)?;
        }

        Ok(())
    }

    pub fn validate_failure_candidate(
        &self,
        candidate: &SimulationResourceUsageV1,
    ) -> Result<(), SimulationResourceErrorV1> {
        self.validate_hard_candidate(candidate)
    }

    pub fn validate_limit_fact(
        &self,
        fact: &ResourceLimitReachedV1,
    ) -> Result<(), SimulationResourceErrorV1> {
        let mut candidate = SimulationResourceUsageV1::default();
        let target = match (fact.scope, fact.kind) {
            (ResourceLimitScopeV1::Session, ResourceLimitKindV1::InMemoryState) => {
                &mut candidate.session.in_memory_state
            }
            (ResourceLimitScopeV1::Session, ResourceLimitKindV1::HotMarkets) => {
                &mut candidate.session.hot_markets
            }
            (ResourceLimitScopeV1::Session, ResourceLimitKindV1::InputQueue) => {
                &mut candidate.session.input_queue
            }
            (ResourceLimitScopeV1::Session, ResourceLimitKindV1::RequestQueue) => {
                &mut candidate.session.request_queue
            }
            (ResourceLimitScopeV1::Session, ResourceLimitKindV1::FeedbackQueue) => {
                &mut candidate.session.feedback_queue
            }
            (ResourceLimitScopeV1::Session, ResourceLimitKindV1::ActiveOrders) => {
                &mut candidate.session.active_orders
            }
            (ResourceLimitScopeV1::Session, ResourceLimitKindV1::InternalEvents) => {
                &mut candidate.session.internal_events
            }
            (ResourceLimitScopeV1::Session, ResourceLimitKindV1::QueryCache) => {
                &mut candidate.session.query_cache
            }
            (ResourceLimitScopeV1::Session, ResourceLimitKindV1::Indexes) => {
                &mut candidate.session.indexes
            }
            (ResourceLimitScopeV1::Run, ResourceLimitKindV1::InMemoryState) => {
                &mut candidate.run.in_memory_state
            }
            (ResourceLimitScopeV1::Run, ResourceLimitKindV1::Sessions) => {
                &mut candidate.run.sessions
            }
            (ResourceLimitScopeV1::Run, ResourceLimitKindV1::HotMarkets) => {
                &mut candidate.run.hot_markets
            }
            (ResourceLimitScopeV1::Run, ResourceLimitKindV1::InputQueue) => {
                &mut candidate.run.input_queue
            }
            (ResourceLimitScopeV1::Run, ResourceLimitKindV1::RequestQueue) => {
                &mut candidate.run.request_queue
            }
            (ResourceLimitScopeV1::Run, ResourceLimitKindV1::FeedbackQueue) => {
                &mut candidate.run.feedback_queue
            }
            (ResourceLimitScopeV1::Run, ResourceLimitKindV1::ActiveOrders) => {
                &mut candidate.run.active_orders
            }
            (ResourceLimitScopeV1::Run, ResourceLimitKindV1::InternalEvents) => {
                &mut candidate.run.internal_events
            }
            (ResourceLimitScopeV1::Run, ResourceLimitKindV1::QueryCache) => {
                &mut candidate.run.query_cache
            }
            (ResourceLimitScopeV1::Run, ResourceLimitKindV1::Indexes) => &mut candidate.run.indexes,
            (ResourceLimitScopeV1::Run, ResourceLimitKindV1::CapitalFence) => {
                &mut candidate.run.capital_fence
            }
            (ResourceLimitScopeV1::Durable, ResourceLimitKindV1::Evidence) => {
                &mut candidate.durable.evidence
            }
            (ResourceLimitScopeV1::Durable, ResourceLimitKindV1::Checkpoints) => {
                &mut candidate.durable.checkpoints
            }
            (ResourceLimitScopeV1::Durable, ResourceLimitKindV1::Artifacts) => {
                &mut candidate.durable.artifacts
            }
            (ResourceLimitScopeV1::Durable, ResourceLimitKindV1::RuntimeRoot) => {
                &mut candidate.durable.runtime_root
            }
            _ => {
                return Err(SimulationResourceErrorV1::Serialization(
                    "资源超限事实的作用域与资源种类不相容".to_owned(),
                ));
            }
        };
        *target = fact.candidate;
        match self.validate_candidate(&candidate) {
            Err(SimulationResourceErrorV1::LimitReached(expected)) if &expected == fact => Ok(()),
            _ => Err(SimulationResourceErrorV1::Serialization(
                "资源超限事实不是资源策略可推导的首个超限结果".to_owned(),
            )),
        }
    }

    fn validate_identity(&self) -> Result<(), SimulationResourceErrorV1> {
        if self.schema_version != SIMULATION_RESOURCE_POLICY_SCHEMA_V1 {
            return Err(SimulationResourceErrorV1::InvalidPolicyIdentity {
                field: "schema_version",
                expected: SIMULATION_RESOURCE_POLICY_SCHEMA_V1,
                actual: self.schema_version.clone(),
            });
        }
        if self.policy_id != COMPONENT_GOLDEN_RESOURCE_POLICY_ID_V1 {
            return Err(SimulationResourceErrorV1::InvalidPolicyIdentity {
                field: "policy_id",
                expected: COMPONENT_GOLDEN_RESOURCE_POLICY_ID_V1,
                actual: self.policy_id.clone(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationSessionResourceUsageV1 {
    pub in_memory_state: ResourceQuantityV1,
    pub hot_markets: ResourceQuantityV1,
    pub input_queue: ResourceQuantityV1,
    pub request_queue: ResourceQuantityV1,
    pub feedback_queue: ResourceQuantityV1,
    pub active_orders: ResourceQuantityV1,
    pub internal_events: ResourceQuantityV1,
    pub query_cache: ResourceQuantityV1,
    pub indexes: ResourceQuantityV1,
}

impl SimulationSessionResourceUsageV1 {
    fn observe_max(&mut self, candidate: &Self) {
        self.in_memory_state.observe_max(candidate.in_memory_state);
        self.hot_markets.observe_max(candidate.hot_markets);
        self.input_queue.observe_max(candidate.input_queue);
        self.request_queue.observe_max(candidate.request_queue);
        self.feedback_queue.observe_max(candidate.feedback_queue);
        self.active_orders.observe_max(candidate.active_orders);
        self.internal_events.observe_max(candidate.internal_events);
        self.query_cache.observe_max(candidate.query_cache);
        self.indexes.observe_max(candidate.indexes);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationRunResourceUsageV1 {
    pub in_memory_state: ResourceQuantityV1,
    pub sessions: ResourceQuantityV1,
    pub hot_markets: ResourceQuantityV1,
    pub input_queue: ResourceQuantityV1,
    pub request_queue: ResourceQuantityV1,
    pub feedback_queue: ResourceQuantityV1,
    pub active_orders: ResourceQuantityV1,
    pub internal_events: ResourceQuantityV1,
    pub query_cache: ResourceQuantityV1,
    pub indexes: ResourceQuantityV1,
    pub capital_fence: ResourceQuantityV1,
}

impl SimulationRunResourceUsageV1 {
    fn observe_max(&mut self, candidate: &Self) {
        self.in_memory_state.observe_max(candidate.in_memory_state);
        self.sessions.observe_max(candidate.sessions);
        self.hot_markets.observe_max(candidate.hot_markets);
        self.input_queue.observe_max(candidate.input_queue);
        self.request_queue.observe_max(candidate.request_queue);
        self.feedback_queue.observe_max(candidate.feedback_queue);
        self.active_orders.observe_max(candidate.active_orders);
        self.internal_events.observe_max(candidate.internal_events);
        self.query_cache.observe_max(candidate.query_cache);
        self.indexes.observe_max(candidate.indexes);
        self.capital_fence.observe_max(candidate.capital_fence);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationDurableResourceUsageV1 {
    pub evidence: ResourceQuantityV1,
    pub checkpoints: ResourceQuantityV1,
    pub artifacts: ResourceQuantityV1,
    pub runtime_root: ResourceQuantityV1,
}

impl SimulationDurableResourceUsageV1 {
    fn observe_max(&mut self, candidate: &Self) {
        self.evidence.observe_max(candidate.evidence);
        self.checkpoints.observe_max(candidate.checkpoints);
        self.artifacts.observe_max(candidate.artifacts);
        self.runtime_root.observe_max(candidate.runtime_root);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationResourceUsageV1 {
    pub session: SimulationSessionResourceUsageV1,
    pub run: SimulationRunResourceUsageV1,
    pub durable: SimulationDurableResourceUsageV1,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationResourceHighWaterV1 {
    pub observed: SimulationResourceUsageV1,
}

impl SimulationResourceHighWaterV1 {
    pub fn observe(&mut self, candidate: &SimulationResourceUsageV1) {
        self.observed.session.observe_max(&candidate.session);
        self.observed.run.observe_max(&candidate.run);
        self.observed.durable.observe_max(&candidate.durable);
    }

    pub fn covers(&self, candidate: &SimulationResourceUsageV1) -> bool {
        let mut observed = self.clone();
        observed.observe(candidate);
        observed == *self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResourceLimitScopeV1 {
    Session,
    Run,
    Durable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResourceLimitKindV1 {
    InMemoryState,
    Sessions,
    HotMarkets,
    InputQueue,
    RequestQueue,
    FeedbackQueue,
    ActiveOrders,
    InternalEvents,
    QueryCache,
    Indexes,
    CapitalFence,
    Evidence,
    Checkpoints,
    Artifacts,
    RuntimeRoot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResourceLimitDimensionV1 {
    Records,
    Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLimitReachedV1 {
    pub policy_id: String,
    pub scope: ResourceLimitScopeV1,
    pub kind: ResourceLimitKindV1,
    pub exceeded_dimension: ResourceLimitDimensionV1,
    pub candidate: ResourceQuantityV1,
    pub limit: ResourceLimitV1,
}

impl ResourceLimitReachedV1 {
    pub fn same_admission_barrier(&self, other: &Self) -> bool {
        self.policy_id == other.policy_id
            && self.scope == other.scope
            && self.kind == other.kind
            && self.exceeded_dimension == other.exceeded_dimension
            && self.limit == other.limit
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "state",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum ResourceAdmissionStateV1 {
    #[default]
    Open,
    SessionIngressClosed {
        limit: ResourceLimitReachedV1,
    },
    RunIngressClosed {
        limit: ResourceLimitReachedV1,
    },
}

impl ResourceAdmissionStateV1 {
    pub fn apply_limit(&mut self, limit: ResourceLimitReachedV1) {
        *self = match limit.scope {
            ResourceLimitScopeV1::Session => Self::SessionIngressClosed { limit },
            ResourceLimitScopeV1::Run | ResourceLimitScopeV1::Durable => {
                Self::RunIngressClosed { limit }
            }
        };
    }

    pub fn session_ingress_is_open(&self) -> bool {
        matches!(self, Self::Open)
    }

    pub fn run_ingress_is_open(&self) -> bool {
        matches!(self, Self::Open | Self::SessionIngressClosed { .. })
    }

    pub fn blocking_limit(&self) -> Option<&ResourceLimitReachedV1> {
        match self {
            Self::Open => None,
            Self::SessionIngressClosed { limit } | Self::RunIngressClosed { limit } => Some(limit),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimulationResourceErrorV1 {
    InvalidPolicyIdentity {
        field: &'static str,
        expected: &'static str,
        actual: String,
    },
    Serialization(String),
    LimitReached(ResourceLimitReachedV1),
}

impl SimulationResourceErrorV1 {
    pub fn limit_reached(&self) -> Option<&ResourceLimitReachedV1> {
        match self {
            Self::LimitReached(limit) => Some(limit),
            Self::InvalidPolicyIdentity { .. } | Self::Serialization(_) => None,
        }
    }
}

impl fmt::Display for SimulationResourceErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicyIdentity {
                field,
                expected,
                actual,
            } => write!(
                formatter,
                "资源策略身份非法：{field} 应为 {expected}，实际为 {actual}"
            ),
            Self::Serialization(message) => write!(formatter, "资源合同序列化失败：{message}"),
            Self::LimitReached(limit) => write!(
                formatter,
                "资源上限已达到：scope={:?}, kind={:?}, dimension={:?}, candidate={:?}, limit={:?}",
                limit.scope, limit.kind, limit.exceeded_dimension, limit.candidate, limit.limit
            ),
        }
    }
}

impl std::error::Error for SimulationResourceErrorV1 {}

pub fn canonical_json_sha256_v1<T: Serialize>(
    domain: &str,
    value: &T,
) -> Result<Sha256Hex, SimulationResourceErrorV1> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| SimulationResourceErrorV1::Serialization(error.to_string()))?;
    let mut hash = Sha256::new();
    hash.update((domain.len() as u64).to_be_bytes());
    hash.update(domain.as_bytes());
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    Ok(Sha256Hex(format!("{:x}", hash.finalize())))
}

fn check_limit(
    policy_id: &str,
    scope: ResourceLimitScopeV1,
    kind: ResourceLimitKindV1,
    candidate: ResourceQuantityV1,
    limit: ResourceLimitV1,
) -> Result<(), SimulationResourceErrorV1> {
    let exceeded_dimension = if candidate.records > limit.max_records {
        Some(ResourceLimitDimensionV1::Records)
    } else if candidate.bytes > limit.max_bytes {
        Some(ResourceLimitDimensionV1::Bytes)
    } else {
        None
    };

    match exceeded_dimension {
        Some(exceeded_dimension) => Err(SimulationResourceErrorV1::LimitReached(
            ResourceLimitReachedV1 {
                policy_id: policy_id.to_owned(),
                scope,
                kind,
                exceeded_dimension,
                candidate,
                limit,
            },
        )),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_equal_to_limit_is_admitted() {
        let policy = SimulationResourcePolicyV1::component_golden_v1();
        let mut candidate = SimulationResourceUsageV1::default();
        candidate.session.active_orders = ResourceQuantityV1::new(
            policy.session.active_orders.max_records,
            policy.session.active_orders.max_bytes,
        );

        assert_eq!(policy.validate_candidate(&candidate), Ok(()));
    }

    #[test]
    fn candidate_one_record_above_limit_returns_typed_fact() {
        let policy = SimulationResourcePolicyV1::component_golden_v1();
        let mut candidate = SimulationResourceUsageV1::default();
        candidate.session.active_orders = ResourceQuantityV1::new(
            policy.session.active_orders.max_records + 1,
            policy.session.active_orders.max_bytes,
        );

        let error = policy.validate_candidate(&candidate).unwrap_err();
        let limit = error.limit_reached().expect("超限必须返回强类型事实");
        assert_eq!(limit.scope, ResourceLimitScopeV1::Session);
        assert_eq!(limit.kind, ResourceLimitKindV1::ActiveOrders);
        assert_eq!(limit.exceeded_dimension, ResourceLimitDimensionV1::Records);
        assert_eq!(limit.candidate, candidate.session.active_orders);
        assert_eq!(limit.limit, policy.session.active_orders);
    }

    #[test]
    fn session_limit_closes_only_the_affected_session_ingress() {
        let policy = SimulationResourcePolicyV1::component_golden_v1();
        let mut candidate = SimulationResourceUsageV1::default();
        candidate.session.request_queue.records = policy.session.request_queue.max_records + 1;
        let limit = policy
            .validate_candidate(&candidate)
            .unwrap_err()
            .limit_reached()
            .expect("应返回会话超限事实")
            .clone();

        let mut state = ResourceAdmissionStateV1::default();
        state.apply_limit(limit);

        assert!(!state.session_ingress_is_open());
        assert!(state.run_ingress_is_open());
        assert!(matches!(
            state,
            ResourceAdmissionStateV1::SessionIngressClosed { .. }
        ));
    }

    #[test]
    fn run_limit_closes_run_and_session_ingress() {
        let policy = SimulationResourcePolicyV1::component_golden_v1();
        let mut candidate = SimulationResourceUsageV1::default();
        candidate.run.internal_events.bytes = policy.run.internal_events.max_bytes + 1;
        let limit = policy
            .validate_candidate(&candidate)
            .unwrap_err()
            .limit_reached()
            .expect("应返回运行超限事实")
            .clone();

        let mut state = ResourceAdmissionStateV1::default();
        state.apply_limit(limit);

        assert!(!state.session_ingress_is_open());
        assert!(!state.run_ingress_is_open());
        assert!(matches!(
            state,
            ResourceAdmissionStateV1::RunIngressClosed { .. }
        ));
    }

    #[test]
    fn high_water_never_decreases() {
        let mut high_water = SimulationResourceHighWaterV1::default();
        let mut first = SimulationResourceUsageV1::default();
        first.session.hot_markets = ResourceQuantityV1::new(7, 70);
        first.run.query_cache = ResourceQuantityV1::new(11, 110);
        first.durable.runtime_root = ResourceQuantityV1::new(13, 130);
        high_water.observe(&first);

        let mut second = SimulationResourceUsageV1::default();
        second.session.hot_markets = ResourceQuantityV1::new(6, 80);
        second.run.query_cache = ResourceQuantityV1::new(12, 100);
        second.durable.runtime_root = ResourceQuantityV1::new(10, 90);
        high_water.observe(&second);

        assert_eq!(
            high_water.observed.session.hot_markets,
            ResourceQuantityV1::new(7, 80)
        );
        assert_eq!(
            high_water.observed.run.query_cache,
            ResourceQuantityV1::new(12, 110)
        );
        assert_eq!(
            high_water.observed.durable.runtime_root,
            ResourceQuantityV1::new(13, 130)
        );
    }

    #[test]
    fn policy_hash_is_stable_and_content_sensitive() {
        let policy = SimulationResourcePolicyV1::component_golden_v1();
        let first = policy.content_sha256().unwrap();
        let second = policy.content_sha256().unwrap();
        assert_eq!(first, second);

        let mut changed = policy;
        changed.session.active_orders.max_records += 1;
        assert_ne!(first, changed.content_sha256().unwrap());
    }
}
