use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

use contracts::{AccountInstanceKeyV1, Sha256Hex};
use serde::{Deserialize, Serialize};

use crate::measurement::ComponentGoldenRunMeasurementV1;
use crate::platform_rules::ImplementationIdentityV1;
use crate::resource::{ResourceLimitReachedV1, SimulationResourceHighWaterV1};
use crate::scenario::PlatformMarketV1;

pub const SIMULATION_RUN_INTENT_BINDING_SCHEMA_V1: &str = "simulation_run_intent_binding_v1";
pub const SIMULATION_RUN_MANIFEST_SCHEMA_V1: &str = "simulation_run_manifest_v1";
pub const SIMULATION_RUN_AUDIT_SCHEMA_V1: &str = "simulation_run_audit_v1";

pub(crate) const SIMULATION_RUN_MANIFEST_FILE_NAME_V1: &str = "simulation-run-manifest.json";
pub(crate) const SIMULATION_RUN_AUDIT_FILE_NAME_V1: &str = "simulation-run-audit.json";
pub(crate) const PLATFORM_SCENARIO_FILE_NAME_V1: &str = "platform-scenario.json";
pub(crate) const TRADE_FINALITY_POLICY_FILE_NAME_V1: &str = "trade-finality-policy.json";
pub(crate) const PLATFORM_RULE_SNAPSHOT_FILE_NAME_V1: &str = "platform-rule-snapshot.json";
pub(crate) const PROXY_LATENCY_PROFILE_FILE_NAME_V1: &str = "proxy-latency-profile.json";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SimulationWorkloadKindV1 {
    ComponentGolden,
    RealVertical,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SimulationDeterministicScriptContractV1 {
    ComponentGoldenV1,
    RealVerticalV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationRunIntentBindingV1 {
    pub schema_version: String,
    pub implementation_identity: ImplementationIdentityV1,
    pub workload_kind: SimulationWorkloadKindV1,
    pub deterministic_script_contract: SimulationDeterministicScriptContractV1,
}

impl SimulationRunIntentBindingV1 {
    pub fn try_new(
        implementation_identity: ImplementationIdentityV1,
        workload_kind: SimulationWorkloadKindV1,
        deterministic_script_contract: SimulationDeterministicScriptContractV1,
    ) -> Result<Self, SimulationRunArtifactErrorV1> {
        let candidate = Self {
            schema_version: SIMULATION_RUN_INTENT_BINDING_SCHEMA_V1.to_owned(),
            implementation_identity,
            workload_kind,
            deterministic_script_contract,
        };
        candidate.validate()?;
        Ok(candidate)
    }

    pub(crate) fn validate(&self) -> Result<(), SimulationRunArtifactErrorV1> {
        if self.schema_version != SIMULATION_RUN_INTENT_BINDING_SCHEMA_V1 {
            return Err(SimulationRunArtifactErrorV1::InvalidIntentBinding(
                "运行意图绑定 schema 不受支持",
            ));
        }
        if self.implementation_identity.git_commit.len() != 40
            || !self
                .implementation_identity
                .git_commit
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || !is_sha256_hex(&self.implementation_identity.source_identity_sha256)
            || !is_sha256_hex(&self.implementation_identity.cargo_lock_sha256)
        {
            return Err(SimulationRunArtifactErrorV1::InvalidIntentBinding(
                "运行意图绑定包含非法实现身份",
            ));
        }
        if !matches!(
            (self.workload_kind, self.deterministic_script_contract),
            (
                SimulationWorkloadKindV1::ComponentGolden,
                SimulationDeterministicScriptContractV1::ComponentGoldenV1
            ) | (
                SimulationWorkloadKindV1::RealVertical,
                SimulationDeterministicScriptContractV1::RealVerticalV1
            )
        ) {
            return Err(SimulationRunArtifactErrorV1::InvalidIntentBinding(
                "命名负载与确定性脚本合同不匹配",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum DurableRunLifecycleV1 {
    Open,
    ArtifactInputsSealed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationArtifactMemberV1 {
    pub schema_version: String,
    pub relative_path: String,
    pub content_identity: Sha256Hex,
    pub file_sha256: Sha256Hex,
    pub byte_length: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationEvidenceReferenceV1 {
    pub sequence: u64,
    pub file_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationEvidencePrefixV1 {
    pub run_root_relative_path: String,
    pub through_commit_sequence: u64,
    pub evidence_count: u64,
    pub evidence_bytes: u64,
    pub evidence_head_sha256: Sha256Hex,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SimulationInputPathV1 {
    HiddenPlatform,
    PublicVisibility,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SimulationInputClosureV1 {
    NextEventAt { next_event_at_utc_ms: i64 },
    EndOfInput,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationInputRangeV1 {
    pub path: SimulationInputPathV1,
    pub next_index_position: u64,
    pub last_bucket: Option<SimulationLastInputBucketV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationLastInputBucketV1 {
    pub bucket_at_utc_ms: i64,
    pub start_index_position: u64,
    pub next_index_position: u64,
    pub bucket_sha256: Sha256Hex,
    pub closure: SimulationInputClosureV1,
    pub evidence: SimulationEvidenceReferenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationAccountAuditV1 {
    pub account_instance_key: AccountInstanceKeyV1,
    pub opening_anchor_id: String,
    pub opening_anchor_sha256: Sha256Hex,
    pub final_account_transition_seq: u64,
    pub final_account_summary_sha256: Sha256Hex,
    pub account_delta_count: u64,
    pub account_delta_head: Option<SimulationAccountDeltaHeadV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationAccountDeltaHeadV1 {
    pub delta_id: String,
    pub delta_sha256: Sha256Hex,
    pub evidence: SimulationEvidenceReferenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationFeedbackAuditV1 {
    pub next_private_feedback_sequence: u64,
    pub process_confirmed_through: u64,
    pub downstream_durable_through: u64,
    pub downstream_durable_confirmation: Option<SimulationDeliveryConfirmationAuditV1>,
    pub deliveries: BTreeMap<u64, SimulationPrivateDeliveryAuditV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationDeliveryConfirmationAuditV1 {
    pub private_feedback_sequence: u64,
    pub delivery_id: String,
    pub payload_sha256: Sha256Hex,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SimulationPrivateDeliveryKindV1 {
    Direct,
    Lifecycle,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationPrivateDeliveryAuditV1 {
    pub kind: SimulationPrivateDeliveryKindV1,
    pub confirmation: SimulationDeliveryConfirmationAuditV1,
    pub prepared_evidence: SimulationEvidenceReferenceV1,
    pub confirmed_evidence: SimulationEvidenceReferenceV1,
    pub downstream_durable_evidence: SimulationEvidenceReferenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationLogicalActionAuditV1 {
    pub action_identity: String,
    pub batch_item_ordinal: u16,
    pub evidence: SimulationEvidenceReferenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationRequestClosureV1 {
    pub global_request_id: String,
    pub registration_evidence: SimulationEvidenceReferenceV1,
    pub logical_actions: Vec<SimulationLogicalActionAuditV1>,
    pub direct_delivery_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "state",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum SimulationSessionLifecycleV1 {
    ActiveCompleted {
        activation_id: String,
        activation_evidence: SimulationEvidenceReferenceV1,
        completion_kind: SimulationSessionCompletionKindV1,
        completion_evidence: SimulationEvidenceReferenceV1,
    },
    InitializationFailedClosed {
        failure_id: String,
        failure_evidence: SimulationEvidenceReferenceV1,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SimulationSessionCompletionKindV1 {
    PaperRetentionExpired,
    CleanRunCompleted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationEnrollmentChainHeadV1 {
    pub enrollment_id: String,
    pub enrollment_sha256: Sha256Hex,
    pub chain_sha256: Sha256Hex,
    pub evidence: SimulationEvidenceReferenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationSessionAuditV1 {
    pub session_id: String,
    pub session_created_evidence: SimulationEvidenceReferenceV1,
    pub account: SimulationAccountAuditV1,
    pub creation_request_id: String,
    pub response_bridge_binding_id: String,
    pub session_seed_sha256: Sha256Hex,
    pub enrollment_count: u64,
    pub enrollment_head: Option<SimulationEnrollmentChainHeadV1>,
    pub requests: BTreeMap<String, SimulationRequestClosureV1>,
    pub input_ranges: Vec<SimulationInputRangeV1>,
    pub feedback: SimulationFeedbackAuditV1,
    pub query_projection_revision: u64,
    pub query_root_sequence_floor: u64,
    pub order_history_retained_from_utc_ms: Option<i64>,
    pub trade_history_retained_from_utc_ms: Option<i64>,
    pub evicted_market_evidence: BTreeMap<String, SimulationEvidenceReferenceV1>,
    pub lifecycle: SimulationSessionLifecycleV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SimulationResourceObservationScopeV1 {
    DurableLogicalObservationsOnly,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationResourceObservationV1 {
    pub measurement_scope: SimulationResourceObservationScopeV1,
    pub resource_policy_id: String,
    pub resource_policy_sha256: Sha256Hex,
    pub resource_high_water: SimulationResourceHighWaterV1,
    pub evidence_file_count: u64,
    pub evidence_bytes: u64,
    pub session_count: u64,
    pub request_count: u64,
    pub logical_action_result_count: u64,
    pub account_delta_count: u64,
    pub private_output_count: u64,
    pub market_bucket_count: u64,
    pub maximum_account_transition_seq: u64,
    pub maximum_private_feedback_sequence: u64,
    pub maximum_input_index_position: u64,
    pub maximum_query_projection_revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationInvariantReportV1 {
    pub evidence_predecessor_chain_verified: bool,
    pub durable_projection_verified: bool,
    pub account_delta_chains_verified: bool,
    pub private_output_closure_verified: bool,
    pub completion_facts_and_durable_closure_verified: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationRunAuditV1 {
    pub schema_version: String,
    pub run_id: String,
    pub through_commit_sequence: u64,
    pub sessions: Vec<SimulationSessionAuditV1>,
    pub capital_issuance_fence_count: u64,
    pub capital_issuance_fence_sha256: Sha256Hex,
    pub component_golden_measurement: Option<ComponentGoldenRunMeasurementV1>,
    pub component_golden_measurement_sha256: Option<Sha256Hex>,
    pub resource_observation: SimulationResourceObservationV1,
    pub invariants: SimulationInvariantReportV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationRunManifestV1 {
    pub schema_version: String,
    pub run_id: String,
    pub execution_contract_version: String,
    pub market_event_schema_version: String,
    pub order_lifecycle_feedback_schema_version: String,
    pub trade_lifecycle_feedback_schema_version: String,
    pub settlement_lifecycle_feedback_schema_version: String,
    pub run_intent_binding: SimulationRunIntentBindingV1,
    pub run_intent_content_sha256: Sha256Hex,
    pub component_golden_measurement_sha256: Option<Sha256Hex>,
    pub evidence_prefix: SimulationEvidencePrefixV1,
    pub market_event_source_identity_sha256: Sha256Hex,
    pub rule_snapshot_file_sha256: Sha256Hex,
    pub scenario_sha256: Sha256Hex,
    pub scenario_market_set_sha256: Sha256Hex,
    pub finality_policy_id: String,
    pub finality_policy_content_sha256: Sha256Hex,
    pub latency_profile_id: String,
    pub latency_profile_version: u32,
    pub latency_profile_file_sha256: Sha256Hex,
    pub latency_sampler_version: String,
    pub latency_sampler_domains: Vec<String>,
    pub artifacts: Vec<SimulationArtifactMemberV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationScenarioArtifactV1 {
    pub schema_version: String,
    pub scenario_sha256: Sha256Hex,
    pub market_set_sha256: Sha256Hex,
    pub markets: Vec<PlatformMarketV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimulationRunArtifactReceiptV1 {
    pub manifest_sha256: Sha256Hex,
    pub artifact_directory: PathBuf,
    pub through_commit_sequence: u64,
    pub evidence_head_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimulationRunArtifactErrorV1 {
    InvalidIntentBinding(&'static str),
    RunNotPersistent,
    RunNotReady(&'static str),
    RunAlreadySealed,
    ResourceLimitReached(ResourceLimitReachedV1),
    Durability(String),
    DurabilityRecoveryRequired(String),
    Integrity(String),
}

impl fmt::Display for SimulationRunArtifactErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIntentBinding(message) => write!(formatter, "运行意图绑定非法：{message}"),
            Self::RunNotPersistent => write!(formatter, "运行没有启用本地耐久根"),
            Self::RunNotReady(message) => write!(formatter, "运行尚未满足产物封存条件：{message}"),
            Self::RunAlreadySealed => write!(formatter, "运行产物输入已经封口"),
            Self::ResourceLimitReached(limit) => write!(
                formatter,
                "运行产物资源硬上限已触发：{:?}/{:?}",
                limit.scope, limit.kind
            ),
            Self::Durability(message) => write!(formatter, "运行产物耐久操作失败：{message}"),
            Self::DurabilityRecoveryRequired(message) => {
                write!(formatter, "运行产物耐久结果需要恢复裁决：{message}")
            }
            Self::Integrity(message) => write!(formatter, "运行产物完整性失败：{message}"),
        }
    }
}

impl std::error::Error for SimulationRunArtifactErrorV1 {}

fn is_sha256_hex(value: &Sha256Hex) -> bool {
    value.0.len() == 64
        && value
            .0
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
