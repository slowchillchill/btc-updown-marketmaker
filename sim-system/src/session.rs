use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

pub use contracts::SimCallerRoleV1;
use contracts::{
    ACCOUNT_OPENING_ANCHOR_SCHEMA_V1, AccountInstanceKeyV1, AccountOpeningAnchorV1,
    EXECUTION_MARKET_EVENT_SCHEMA_V3, ExactDecimal, MARKET_ENROLLMENT_SCHEMA_V1,
    MarketEnrollmentIdentityV1, MarketEnrollmentV1, MarketResolutionTerminalV3,
    PlatformAccountSnapshotV1, PlatformOutcomeBalanceV1, PlatformSourceV1, Sha256Hex, TokenId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::account::{InitialAccountStateV1, PlatformAccountStateV1};
use crate::finality::TradeFinalityPolicyV1;
use crate::latency_profile::{DelaySampleDomainV1, EmpiricalDelayProfile};
use crate::market::{
    AccountBookOverlayStoreV1, ClosedMarketEventBucketV1, DurableMarketBucketReferenceV1,
    MarketEventErrorV1, MarketEventPathV1, MarketLifecyclePhaseV1, MarketStateStoreV1,
    MillisecondBucketClosureV1, PreparedMarketResolutionTerminalDeltaV2,
    PreparedMarketStoreBucketDeltaV2, PreparedMarketStoreBucketStepV2,
    PreparedMarketStoreClosureUpgradeV2,
};
use crate::market_directory::{SessionMarketDirectoryErrorV1, SessionMarketDirectoryV1};
use crate::matching::{
    AuthorizedMarketEventEffectsV1, CancelOrderStateDispositionV1,
    CommittableExecutionDriveDeltaV2, ExecutionStateV1, MatchingStateErrorV1,
    PreparedExecutionDriveDeltaV2, TerminalExecutionMarketEvictionV1,
};
use crate::platform_rules::SealedProcessRuleSnapshot;
use crate::scenario::{PlatformMarketV1, PlatformScenarioV1};

pub const SIM_EXECUTION_CONTRACT_V1: &str = "sim_execution_contract_v1";
pub const SESSION_SPEC_SCHEMA_V2: &str = "session_spec_v2";
pub(crate) const STABLE_ID_MAX_BYTES_V1: usize = 256;
pub(crate) const SESSION_ID_MAX_BYTES_V1: usize = STABLE_ID_MAX_BYTES_V1;

pub(crate) fn session_id_is_valid_v1(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= SESSION_ID_MAX_BYTES_V1
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

const SIM_PROCESS_ROOT_CHECKPOINT_SCHEMA_V1: &str = "sim_process_root_checkpoint_v1";
const SIM_SESSION_CHECKPOINT_SCHEMA_V1: &str = "sim_session_checkpoint_v1";

const SESSION_SPEC_HASH_DOMAIN_V2: &str = "SIM_SESSION_SPEC_SHA256_V2";
const ACCOUNT_OPENING_ANCHOR_ID_DOMAIN_V1: &str = "SIM_ACCOUNT_OPENING_ANCHOR_ID_V1";
const ACCOUNT_OPENING_ANCHOR_HASH_DOMAIN_V1: &str = "SIM_ACCOUNT_OPENING_ANCHOR_SHA256_V1";
const MARKET_ENROLLMENT_ID_DOMAIN_V1: &str = "SIM_MARKET_ENROLLMENT_ID_V1";
const MARKET_ENROLLMENT_HASH_DOMAIN_V1: &str = "SIM_MARKET_ENROLLMENT_SHA256_V1";
const MARKET_ENROLLMENT_CHAIN_HASH_DOMAIN_V1: &str = "SIM_MARKET_ENROLLMENT_CHAIN_SHA256_V1";
const REQUEST_PENDING_ACTION_COUNTER_BYTES_V1: u64 = 8;

#[cfg(test)]
std::thread_local! {
    static PENDING_ACTION_ENTRY_VISIT_COUNT_FOR_TESTS: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

#[cfg(test)]
fn record_pending_action_entry_visit_for_tests() {
    PENDING_ACTION_ENTRY_VISIT_COUNT_FOR_TESTS.with(|count| count.set(count.get() + 1));
}

#[cfg(not(test))]
#[inline(always)]
fn record_pending_action_entry_visit_for_tests() {}

#[cfg(test)]
fn reset_pending_action_entry_visits_for_tests() {
    PENDING_ACTION_ENTRY_VISIT_COUNT_FOR_TESTS.with(|count| count.set(0));
}

#[cfg(test)]
fn pending_action_entry_visits_for_tests() -> usize {
    PENDING_ACTION_ENTRY_VISIT_COUNT_FOR_TESTS.with(std::cell::Cell::get)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSpecV2 {
    pub run_id: String,
    pub session_id: String,
    pub account_id: String,
    pub caller_role: SimCallerRoleV1,
    pub caller_instance_id: String,
    pub creation_request_id: String,
    pub response_bridge_binding_id: String,
    pub initial_account_state: InitialAccountStateV1,
    pub response_outbox_capacity: NonZeroUsize,
    pub market_set_sha256: Sha256Hex,
    pub scenario_sha256: Sha256Hex,
    pub session_seed: Sha256Hex,
    pub execution_contract_version: String,
    pub market_event_schema_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketEnrollmentRequestV1 {
    pub enrollment_request_id: String,
    pub market: PlatformMarketV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SessionInitializationStateV1 {
    PreparedLedgerInit,
    Active,
    InitializationFailedClosed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionInitializationSnapshotV1 {
    pub state: SessionInitializationStateV1,
    pub activation_id: Option<String>,
    pub failure_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionCreationDispositionV2 {
    NewPrepared,
    ExistingPrepared,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionCreationControlResultV2 {
    pub disposition: SessionCreationDispositionV2,
    pub account_instance_key: AccountInstanceKeyV1,
    pub opening_anchor: AccountOpeningAnchorV1,
    pub session_spec_sha256: Sha256Hex,
    pub initialization: SessionInitializationSnapshotV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivateSessionRequestV1 {
    pub session_id: String,
    pub account_instance_key: AccountInstanceKeyV1,
    pub opening_anchor_id: String,
    pub opening_anchor_sha256: Sha256Hex,
    pub activation_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailSessionInitializationRequestV1 {
    pub session_id: String,
    pub account_instance_key: AccountInstanceKeyV1,
    pub opening_anchor_id: String,
    pub opening_anchor_sha256: Sha256Hex,
    pub failure_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionInitializationControlDispositionV1 {
    Activated,
    ExistingActive,
    FailedClosed,
    ExistingFailedClosed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum InputPathSealOutcomeV1 {
    Exhausted,
    FailedMissingQualifiedMarketTerminal { condition_ids: Vec<String> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionInitializationControlResultV1 {
    pub disposition: SessionInitializationControlDispositionV1,
    pub snapshot: SessionInitializationSnapshotV1,
}

type SharedSessionInitializationV1 = Arc<Mutex<SessionInitializationSnapshotV1>>;

#[derive(Clone)]
struct SessionCreationRecordV2 {
    spec: SessionSpecV2,
    spec_sha256: Sha256Hex,
    account_instance_key: AccountInstanceKeyV1,
    opening_anchor: AccountOpeningAnchorV1,
    initialization: SharedSessionInitializationV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CapitalIssuanceFenceV1 {
    session_id: String,
    account_instance_key: AccountInstanceKeyV1,
    opening_anchor: AccountOpeningAnchorV1,
    creation_request_id: String,
    spec_sha256: Sha256Hex,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketEnrollmentStateV1 {
    directory: SessionMarketDirectoryV1,
    chain: Vec<MarketEnrollmentV1>,
    records_by_request_id: BTreeMap<String, usize>,
    business_input_started: bool,
    paper_candidate_condition_ids: BTreeSet<String>,
    paper_candidate_token_ids: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionCreationCheckpointV2 {
    spec: SessionSpecV2,
    spec_sha256: Sha256Hex,
    account_instance_key: AccountInstanceKeyV1,
    opening_anchor: AccountOpeningAnchorV1,
    initialization: SessionInitializationSnapshotV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SimProcessRootCheckpointV1 {
    schema_version: String,
    run_id: String,
    rule_snapshot_file_sha256: Sha256Hex,
    finality_policy_id: String,
    finality_policy_content_sha256: Sha256Hex,
    latency_profile_id: String,
    latency_profile_version: u32,
    latency_profile_file_sha256: Sha256Hex,
    scenario_market_set_sha256: Sha256Hex,
    scenario_sha256: Sha256Hex,
    capital_issuance_fences: Vec<CapitalIssuanceFenceV1>,
    session_creations: Vec<SessionCreationCheckpointV2>,
}

impl SimProcessRootCheckpointV1 {
    pub(crate) fn run_id(&self) -> &str {
        &self.run_id
    }

    pub(crate) fn resource_capital_fence_count(&self) -> usize {
        self.capital_issuance_fences.len()
    }

    pub(crate) fn resource_session_creation_count(&self) -> usize {
        self.session_creations.len()
    }

    pub(crate) fn capital_issuance_fence_audit(&self) -> Result<(u64, Sha256Hex), SessionError> {
        let count = u64::try_from(self.capital_issuance_fences.len()).map_err(|_| {
            SessionError::SessionIdentitySerialization("资本签发围栏数量超出 u64".to_owned())
        })?;
        let sha256 = hash_json(
            "SIM_CAPITAL_ISSUANCE_FENCE_AUDIT_V1",
            &self.capital_issuance_fences,
        )?;
        Ok((count, sha256))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SimSessionCheckpointV1 {
    schema_version: String,
    session_id: String,
    hidden_platform_markets: Option<MarketStateStoreV1>,
    public_markets: MarketStateStoreV1,
    execution_state: ExecutionStateV1,
    market_enrollment: MarketEnrollmentStateV1,
    state: SessionStateCheckpointV1,
}

impl SimSessionCheckpointV1 {
    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn recovered_query_revisions(&self) -> BTreeMap<String, u64> {
        self.state
            .queries
            .iter()
            .map(|(request_id, record)| (request_id.clone(), record.platform_state_revision))
            .collect()
    }

    pub(crate) fn platform_state_revision(&self) -> u64 {
        self.state.platform_state_revision
    }

    pub(crate) fn logical_platform_driven_through(&self) -> Option<i64> {
        self.state.closed_action_through_utc_ms
    }

    pub(crate) fn private_fact_schedule_closed_through(&self) -> Option<i64> {
        self.state.private_fact_schedule_closed_through_utc_ms
    }

    pub(crate) fn resource_core_counts(&self) -> SimSessionResourceCountsV1 {
        let public_hot_markets = self.public_markets.resource_hot_market_count();
        let hidden_hot_markets = self
            .hidden_platform_markets
            .as_ref()
            .map_or(0, MarketStateStoreV1::resource_hot_market_count);
        let market_indexes = self.public_markets.resource_index_count().saturating_add(
            self.hidden_platform_markets
                .as_ref()
                .map_or(0, MarketStateStoreV1::resource_index_count),
        );
        let external_price_levels = self
            .public_markets
            .resource_external_price_level_count()
            .saturating_add(
                self.hidden_platform_markets
                    .as_ref()
                    .map_or(0, MarketStateStoreV1::resource_external_price_level_count),
            );
        SimSessionResourceCountsV1 {
            hot_markets: public_hot_markets.saturating_add(hidden_hot_markets),
            pending_requests: self.state.requests.len(),
            pending_actions: self.state.pending_actions.len(),
            pending_private_facts: self.state.pending_private_facts.len(),
            active_orders: self.execution_state.resource_active_order_count(),
            internal_events: self
                .state
                .pending_actions
                .len()
                .saturating_add(self.state.pending_private_facts.len())
                .saturating_add(self.execution_state.resource_internal_event_count()),
            indexes: market_indexes
                .saturating_add(external_price_levels)
                .saturating_add(self.execution_state.resource_index_count())
                .saturating_add(self.state.requests.len())
                .saturating_add(self.state.mutations.len())
                .saturating_add(self.state.queries.len())
                .saturating_add(self.state.lifecycle_feedback.len())
                .saturating_add(self.market_enrollment.chain.len())
                .saturating_add(self.market_enrollment.records_by_request_id.len()),
            hot_order_and_fill_records: self.execution_state.resource_order_and_fill_count(),
        }
    }

    pub(crate) fn resource_component_bytes(
        &self,
    ) -> Result<SimSessionResourceBytesV1, SessionError> {
        let hot_markets =
            encoded_resource_len(&(&self.public_markets, &self.hidden_platform_markets))?;
        let pending_schedule = encoded_resource_len(&(
            &self.state.pending_actions,
            &self.state.pending_private_facts,
        ))?;
        let session_indexes = encoded_resource_len(&(
            &self.state.requests,
            &self.state.mutations,
            &self.state.queries,
            &self.state.lifecycle_feedback,
            &self.market_enrollment,
        ))?;
        let market_indexes = self
            .public_markets
            .resource_index_encoded_bytes()
            .and_then(|public| {
                self.hidden_platform_markets
                    .as_ref()
                    .map_or(Ok(0), MarketStateStoreV1::resource_index_encoded_bytes)
                    .and_then(|hidden| {
                        public
                            .checked_add(hidden)
                            .ok_or_else(|| "市场索引资源字节溢出".to_owned())
                    })
            })
            .map_err(SessionError::ExecutionState)?;
        let execution_indexes = self
            .execution_state
            .resource_index_encoded_bytes()
            .map_err(SessionError::ExecutionState)?;
        let runtime_request_counters = self.resource_runtime_request_counter_bytes()?;
        Ok(SimSessionResourceBytesV1 {
            hot_markets,
            active_orders: self
                .execution_state
                .resource_active_order_encoded_bytes()
                .map_err(SessionError::ExecutionState)?,
            internal_events: pending_schedule
                .checked_add(
                    self.execution_state
                        .resource_internal_event_encoded_bytes()
                        .map_err(SessionError::ExecutionState)?,
                )
                .ok_or_else(|| SessionError::ExecutionState("内部事件资源字节溢出".to_owned()))?,
            indexes: session_indexes
                .checked_add(runtime_request_counters)
                .and_then(|value| value.checked_add(market_indexes))
                .and_then(|value| value.checked_add(execution_indexes))
                .ok_or_else(|| SessionError::ExecutionState("索引资源字节溢出".to_owned()))?,
        })
    }

    pub(crate) fn resource_runtime_request_counter_bytes(&self) -> Result<u64, SessionError> {
        u64::try_from(self.state.requests.len())
            .ok()
            .and_then(|count| count.checked_mul(REQUEST_PENDING_ACTION_COUNTER_BYTES_V1))
            .ok_or_else(|| SessionError::ExecutionState("请求待处理动作计数字节溢出".to_owned()))
    }
}

fn encoded_resource_len<T: Serialize>(value: &T) -> Result<u64, SessionError> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        SessionError::ExecutionState(format!("会话资源计量序列化失败：{error}"))
    })?;
    u64::try_from(bytes.len())
        .map_err(|_| SessionError::ExecutionState("会话资源字节超出 u64".to_owned()))
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SimSessionResourceCountsV1 {
    pub(crate) hot_markets: usize,
    pub(crate) pending_requests: usize,
    pub(crate) pending_actions: usize,
    pub(crate) pending_private_facts: usize,
    pub(crate) active_orders: usize,
    pub(crate) internal_events: usize,
    pub(crate) indexes: usize,
    pub(crate) hot_order_and_fill_records: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SimSessionResourceBytesV1 {
    pub(crate) hot_markets: u64,
    pub(crate) active_orders: u64,
    pub(crate) internal_events: u64,
    pub(crate) indexes: u64,
}

impl MarketEnrollmentStateV1 {
    fn new(directory: SessionMarketDirectoryV1) -> Self {
        Self {
            directory,
            chain: Vec::new(),
            records_by_request_id: BTreeMap::new(),
            business_input_started: false,
            paper_candidate_condition_ids: BTreeSet::new(),
            paper_candidate_token_ids: BTreeSet::new(),
        }
    }
}

pub(crate) struct PreparedSimSessionV2 {
    pub session: SimSession,
    pub control: SessionCreationControlResultV2,
}

#[derive(Clone)]
pub struct SimProcessRoot {
    run_id: Arc<str>,
    finality_policy: Arc<TradeFinalityPolicyV1>,
    rule_snapshot: Arc<SealedProcessRuleSnapshot>,
    latency_profile: Arc<EmpiricalDelayProfile>,
    scenario: Arc<PlatformScenarioV1>,
    initial_market_directory: SessionMarketDirectoryV1,
    session_creations: BTreeMap<String, SessionCreationRecordV2>,
    account_issuance_fence: BTreeMap<AccountInstanceKeyV1, CapitalIssuanceFenceV1>,
    creation_requests: BTreeMap<String, String>,
}

impl SimProcessRoot {
    pub(crate) fn detached_clone(&self) -> Result<Self, SessionError> {
        let session_creations = self
            .session_creations
            .iter()
            .map(|(session_id, record)| {
                Ok((
                    session_id.clone(),
                    SessionCreationRecordV2 {
                        spec: record.spec.clone(),
                        spec_sha256: record.spec_sha256.clone(),
                        account_instance_key: record.account_instance_key.clone(),
                        opening_anchor: record.opening_anchor.clone(),
                        initialization: Arc::new(Mutex::new(initialization_snapshot(
                            &record.initialization,
                        )?)),
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>, SessionError>>()?;
        Ok(Self {
            run_id: Arc::clone(&self.run_id),
            finality_policy: Arc::clone(&self.finality_policy),
            rule_snapshot: Arc::clone(&self.rule_snapshot),
            latency_profile: Arc::clone(&self.latency_profile),
            scenario: Arc::clone(&self.scenario),
            initial_market_directory: self.initial_market_directory.clone(),
            session_creations,
            account_issuance_fence: self.account_issuance_fence.clone(),
            creation_requests: self.creation_requests.clone(),
        })
    }

    pub fn try_new(
        run_id: String,
        finality_policy: Arc<TradeFinalityPolicyV1>,
        rule_snapshot: Arc<SealedProcessRuleSnapshot>,
        latency_profile: Arc<EmpiricalDelayProfile>,
        scenario: Arc<PlatformScenarioV1>,
    ) -> Result<Self, SimProcessRootErrorV1> {
        if !is_stable_text(&run_id) {
            return Err(SimProcessRootErrorV1::InvalidRunId);
        }
        for (override_index, member) in finality_policy.failure_overrides().iter().enumerate() {
            if member.selector().account_instance_key().run_id != run_id {
                return Err(SimProcessRootErrorV1::FinalitySelectorRunMismatch { override_index });
            }
        }
        let initial_market_directory = SessionMarketDirectoryV1::try_from_scenario(&scenario)
            .map_err(|error| {
                SimProcessRootErrorV1::InvalidInitialMarketDirectory(error.to_string())
            })?;
        Ok(Self {
            run_id: Arc::from(run_id),
            finality_policy,
            rule_snapshot,
            latency_profile,
            scenario,
            initial_market_directory,
            session_creations: BTreeMap::new(),
            account_issuance_fence: BTreeMap::new(),
            creation_requests: BTreeMap::new(),
        })
    }

    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    #[must_use]
    pub fn finality_policy(&self) -> &Arc<TradeFinalityPolicyV1> {
        &self.finality_policy
    }

    pub(crate) fn rule_snapshot(&self) -> &Arc<SealedProcessRuleSnapshot> {
        &self.rule_snapshot
    }

    pub(crate) fn latency_profile(&self) -> &Arc<EmpiricalDelayProfile> {
        &self.latency_profile
    }

    pub(crate) fn scenario(&self) -> &Arc<PlatformScenarioV1> {
        &self.scenario
    }

    pub fn create_session(
        &mut self,
        spec: SessionSpecV2,
    ) -> Result<PreparedSimSessionV2, SessionError> {
        if self.is_session_retired(&spec.session_id) {
            return Err(SessionError::SessionRetentionExpired(spec.session_id));
        }
        let (account_instance_key, spec_sha256, opening_anchor) =
            self.validated_creation_material(&spec)?;
        if let Some(existing) = self.session_creations.get(&spec.session_id) {
            if existing.spec == spec
                && existing.spec_sha256 == spec_sha256
                && existing.account_instance_key == account_instance_key
                && existing.opening_anchor == opening_anchor
            {
                return Err(SessionError::SessionAlreadyCreatedRequiresRetry(
                    spec.session_id,
                ));
            }
            return Err(SessionError::SessionCreationIdentityConflict(
                spec.session_id,
            ));
        }
        if self
            .account_issuance_fence
            .contains_key(&account_instance_key)
        {
            return Err(SessionError::AccountInstanceAlreadyReserved);
        }
        if self
            .creation_requests
            .contains_key(&spec.creation_request_id)
        {
            return Err(SessionError::CreationRequestConflict(
                spec.creation_request_id,
            ));
        }
        let market_directory = self.initial_market_directory.clone();
        let hidden_platform_markets = match spec.caller_role {
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
                Some(MarketStateStoreV1::new(&market_directory))
            }
            SimCallerRoleV1::Paper => None,
        };
        let public_markets = MarketStateStoreV1::new(&market_directory);
        let execution_state = ExecutionStateV1::new(
            &market_directory,
            &opening_anchor,
            self.rule_snapshot.as_ref(),
        )
        .map_err(SessionError::from)?;
        let initialization = Arc::new(Mutex::new(SessionInitializationSnapshotV1 {
            state: SessionInitializationStateV1::PreparedLedgerInit,
            activation_id: None,
            failure_id: None,
        }));
        let control = SessionCreationControlResultV2 {
            disposition: SessionCreationDispositionV2::NewPrepared,
            account_instance_key: account_instance_key.clone(),
            opening_anchor: opening_anchor.clone(),
            session_spec_sha256: spec_sha256.clone(),
            initialization: initialization_snapshot(&initialization)?,
        };
        let session = SimSession {
            finality_policy: Arc::clone(&self.finality_policy),
            rule_snapshot: Arc::clone(&self.rule_snapshot),
            latency_profile: Arc::clone(&self.latency_profile),
            scenario: Arc::clone(&self.scenario),
            hidden_platform_markets,
            public_markets,
            execution_state,
            terminal_projection_pending: BTreeSet::new(),
            market_enrollment: Arc::new(MarketEnrollmentStateV1::new(market_directory)),
            initialization: Arc::clone(&initialization),
            state: SessionState::new(spec.clone()),
        };
        self.session_creations.insert(
            spec.session_id.clone(),
            SessionCreationRecordV2 {
                spec: spec.clone(),
                spec_sha256: spec_sha256.clone(),
                account_instance_key: account_instance_key.clone(),
                opening_anchor: opening_anchor.clone(),
                initialization,
            },
        );
        self.account_issuance_fence.insert(
            account_instance_key.clone(),
            CapitalIssuanceFenceV1 {
                session_id: spec.session_id.clone(),
                account_instance_key,
                opening_anchor,
                creation_request_id: spec.creation_request_id.clone(),
                spec_sha256,
            },
        );
        self.creation_requests
            .insert(spec.creation_request_id, spec.session_id);
        Ok(PreparedSimSessionV2 { session, control })
    }

    pub fn retry_create_session(
        &self,
        spec: &SessionSpecV2,
    ) -> Result<SessionCreationControlResultV2, SessionError> {
        if self.is_session_retired(&spec.session_id) {
            return Err(SessionError::SessionRetentionExpired(
                spec.session_id.clone(),
            ));
        }
        let (account_instance_key, spec_sha256, opening_anchor) =
            self.validated_creation_material(spec)?;
        let record = self
            .session_creations
            .get(&spec.session_id)
            .ok_or_else(|| SessionError::SessionNotFound(spec.session_id.clone()))?;
        if record.spec != *spec
            || record.spec_sha256 != spec_sha256
            || record.account_instance_key != account_instance_key
            || record.opening_anchor != opening_anchor
            || self.creation_requests.get(&spec.creation_request_id) != Some(&spec.session_id)
        {
            return Err(SessionError::SessionCreationIdentityConflict(
                spec.session_id.clone(),
            ));
        }
        let fence = self
            .account_issuance_fence
            .get(&account_instance_key)
            .ok_or_else(|| {
                SessionError::SessionCreationIdentityConflict(spec.session_id.clone())
            })?;
        if fence.session_id != spec.session_id
            || fence.account_instance_key != account_instance_key
            || fence.opening_anchor != opening_anchor
            || fence.creation_request_id != spec.creation_request_id
            || fence.spec_sha256 != spec_sha256
        {
            return Err(SessionError::SessionCreationIdentityConflict(
                spec.session_id.clone(),
            ));
        }
        let initialization = initialization_snapshot(&record.initialization)?;
        if initialization.state != SessionInitializationStateV1::PreparedLedgerInit {
            return Err(SessionError::SessionCreationRetryNotPrepared);
        }
        Ok(SessionCreationControlResultV2 {
            disposition: SessionCreationDispositionV2::ExistingPrepared,
            account_instance_key,
            opening_anchor,
            session_spec_sha256: spec_sha256,
            initialization,
        })
    }

    pub fn activate_session(
        &self,
        request: ActivateSessionRequestV1,
    ) -> Result<SessionInitializationControlResultV1, SessionError> {
        validate_activation_request(&request)?;
        let record = self.validated_control_record(
            &request.session_id,
            &request.account_instance_key,
            &request.opening_anchor_id,
            &request.opening_anchor_sha256,
        )?;
        let mut initialization = record
            .initialization
            .lock()
            .map_err(|_| SessionError::InitializationStateUnavailable)?;
        let disposition = match initialization.state {
            SessionInitializationStateV1::PreparedLedgerInit => {
                initialization.state = SessionInitializationStateV1::Active;
                initialization.activation_id = Some(request.activation_id);
                SessionInitializationControlDispositionV1::Activated
            }
            SessionInitializationStateV1::Active
                if initialization.activation_id.as_deref()
                    == Some(request.activation_id.as_str()) =>
            {
                SessionInitializationControlDispositionV1::ExistingActive
            }
            SessionInitializationStateV1::Active => {
                return Err(SessionError::ActivationIdentityConflict);
            }
            SessionInitializationStateV1::InitializationFailedClosed => {
                return Err(SessionError::InitializationFailedClosed);
            }
        };
        Ok(SessionInitializationControlResultV1 {
            disposition,
            snapshot: initialization.clone(),
        })
    }

    pub fn fail_session_initialization(
        &self,
        request: FailSessionInitializationRequestV1,
    ) -> Result<SessionInitializationControlResultV1, SessionError> {
        validate_failure_request(&request)?;
        let record = self.validated_control_record(
            &request.session_id,
            &request.account_instance_key,
            &request.opening_anchor_id,
            &request.opening_anchor_sha256,
        )?;
        let mut initialization = record
            .initialization
            .lock()
            .map_err(|_| SessionError::InitializationStateUnavailable)?;
        let disposition = match initialization.state {
            SessionInitializationStateV1::PreparedLedgerInit => {
                initialization.state = SessionInitializationStateV1::InitializationFailedClosed;
                initialization.failure_id = Some(request.failure_id);
                SessionInitializationControlDispositionV1::FailedClosed
            }
            SessionInitializationStateV1::InitializationFailedClosed
                if initialization.failure_id.as_deref() == Some(request.failure_id.as_str()) =>
            {
                SessionInitializationControlDispositionV1::ExistingFailedClosed
            }
            SessionInitializationStateV1::InitializationFailedClosed => {
                return Err(SessionError::FailureIdentityConflict);
            }
            SessionInitializationStateV1::Active => {
                return Err(SessionError::InitializationAlreadyActive);
            }
        };
        Ok(SessionInitializationControlResultV1 {
            disposition,
            snapshot: initialization.clone(),
        })
    }

    pub fn session_initialization(
        &self,
        opening_anchor: &AccountOpeningAnchorV1,
    ) -> Result<SessionInitializationSnapshotV1, SessionError> {
        validate_initialization_identity(
            &opening_anchor.session_id,
            &opening_anchor.account_instance_key,
            &opening_anchor.anchor_id,
            &opening_anchor.anchor_sha256,
        )?;
        let record = self.validated_control_record(
            &opening_anchor.session_id,
            &opening_anchor.account_instance_key,
            &opening_anchor.anchor_id,
            &opening_anchor.anchor_sha256,
        )?;
        if &record.opening_anchor != opening_anchor {
            return Err(SessionError::InitializationControlIdentityConflict);
        }
        initialization_snapshot(&record.initialization)
    }

    fn validated_creation_material(
        &self,
        spec: &SessionSpecV2,
    ) -> Result<(AccountInstanceKeyV1, Sha256Hex, AccountOpeningAnchorV1), SessionError> {
        validate_session_spec(spec)?;
        if spec.run_id != self.run_id.as_ref() {
            return Err(SessionError::RunIdMismatch);
        }
        if spec.market_set_sha256 != *self.scenario.market_set_sha256() {
            return Err(SessionError::MarketSetIdentityMismatch);
        }
        if spec.scenario_sha256 != *self.scenario.scenario_sha256() {
            return Err(SessionError::ScenarioIdentityMismatch);
        }
        let account_instance_key = account_instance_key(spec);
        let spec_sha256 = session_spec_sha256(spec)?;
        let opening_anchor =
            account_opening_anchor(spec, account_instance_key.clone(), &self.scenario)?;
        Ok((account_instance_key, spec_sha256, opening_anchor))
    }

    fn validated_control_record(
        &self,
        session_id: &str,
        account_instance_key: &AccountInstanceKeyV1,
        opening_anchor_id: &str,
        opening_anchor_sha256: &Sha256Hex,
    ) -> Result<&SessionCreationRecordV2, SessionError> {
        let record = self
            .session_creations
            .get(session_id)
            .ok_or_else(|| SessionError::SessionNotFound(session_id.to_owned()))?;
        if &record.account_instance_key != account_instance_key
            || record.opening_anchor.anchor_id != opening_anchor_id
            || &record.opening_anchor.anchor_sha256 != opening_anchor_sha256
        {
            return Err(SessionError::InitializationControlIdentityConflict);
        }
        let fence = self
            .account_issuance_fence
            .get(account_instance_key)
            .ok_or(SessionError::InitializationControlIdentityConflict)?;
        if fence.session_id != record.spec.session_id
            || fence.account_instance_key != record.account_instance_key
            || fence.opening_anchor != record.opening_anchor
            || fence.creation_request_id != record.spec.creation_request_id
            || fence.spec_sha256 != record.spec_sha256
        {
            return Err(SessionError::InitializationControlIdentityConflict);
        }
        Ok(record)
    }

    pub(crate) fn enroll_market(
        &self,
        session: &mut SimSession,
        request: MarketEnrollmentRequestV1,
    ) -> Result<MarketEnrollmentV1, SessionError> {
        let session_id = session.session_spec().session_id.clone();
        let record = self
            .session_creations
            .get(&session_id)
            .ok_or_else(|| SessionError::SessionNotFound(session_id.clone()))?;
        if record.spec != *session.session_spec()
            || !Arc::ptr_eq(&record.initialization, &session.initialization)
        {
            return Err(SessionError::MarketEnrollmentSessionOwnershipConflict);
        }
        session.enroll_market(
            request,
            &record.account_instance_key,
            &record.opening_anchor,
        )
    }

    pub(crate) fn restore_session_from_checkpoint(
        &self,
        checkpoint: SimSessionCheckpointV1,
    ) -> Result<SimSession, SessionCheckpointErrorV1> {
        self.restore_session_from_checkpoint_with_evicted_markets(checkpoint, &BTreeMap::new())
    }

    pub(crate) fn restore_session_from_checkpoint_with_evicted_markets(
        &self,
        checkpoint: SimSessionCheckpointV1,
        evicted_markets: &BTreeMap<String, TerminalExecutionMarketEvictionV1>,
    ) -> Result<SimSession, SessionCheckpointErrorV1> {
        if checkpoint.schema_version != SIM_SESSION_CHECKPOINT_SCHEMA_V1 {
            return Err(SessionCheckpointErrorV1::UnsupportedSchema(
                checkpoint.schema_version,
            ));
        }
        let record = self
            .session_creations
            .get(&checkpoint.session_id)
            .ok_or_else(|| {
                SessionCheckpointErrorV1::SessionNotRegistered(checkpoint.session_id.clone())
            })?;
        validate_market_enrollment_checkpoint(
            &self.initial_market_directory,
            &checkpoint.market_enrollment,
            &record.account_instance_key,
            &record.opening_anchor,
        )?;
        match (
            record.spec.caller_role,
            checkpoint.hidden_platform_markets.is_some(),
        ) {
            (SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation, true)
            | (SimCallerRoleV1::Paper, false) => {}
            _ => {
                return Err(SessionCheckpointErrorV1::InvalidSessionState(
                    "隐藏市场状态与会话角色不匹配".to_owned(),
                ));
            }
        }
        let expected_evicted_condition_ids = evicted_markets
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if checkpoint
            .public_markets
            .evicted_condition_ids()
            .collect::<BTreeSet<_>>()
            != expected_evicted_condition_ids
            || checkpoint
                .hidden_platform_markets
                .as_ref()
                .is_some_and(|markets| {
                    markets.evicted_condition_ids().collect::<BTreeSet<_>>()
                        != expected_evicted_condition_ids
                })
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "检查点公开或隐藏驱逐市场集合与仅追加驱逐事实不一致".to_owned(),
            ));
        }
        checkpoint
            .public_markets
            .validate_recovered(&checkpoint.market_enrollment.directory)
            .map_err(|error| SessionCheckpointErrorV1::InvalidSessionState(error.to_string()))?;
        if let Some(hidden) = &checkpoint.hidden_platform_markets {
            hidden
                .validate_recovered(&checkpoint.market_enrollment.directory)
                .map_err(|error| {
                    SessionCheckpointErrorV1::InvalidSessionState(error.to_string())
                })?;
        }
        checkpoint
            .execution_state
            .validate_recovered(
                &checkpoint.market_enrollment.directory,
                &record.opening_anchor,
                self.rule_snapshot.as_ref(),
                self.finality_policy.as_ref(),
                checkpoint.state.closed_action_through_utc_ms,
                evicted_markets,
            )
            .map_err(|error| {
                SessionCheckpointErrorV1::InvalidExecutionState(format!("{error:?}"))
            })?;
        let authorized_markets = match record.spec.caller_role {
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => checkpoint
                .hidden_platform_markets
                .as_ref()
                .expect("已核对训练或离线会话必须拥有隐藏市场状态"),
            SimCallerRoleV1::Paper => &checkpoint.public_markets,
        };
        checkpoint
            .execution_state
            .validate_recovered_market_resolutions(
                &authorized_markets.qualified_resolution_recovery_states(),
                &authorized_markets.lifecycle_phase_recovery_states(),
            )
            .map_err(|error| {
                SessionCheckpointErrorV1::InvalidExecutionState(format!("{error:?}"))
            })?;

        Ok(SimSession {
            finality_policy: Arc::clone(&self.finality_policy),
            rule_snapshot: Arc::clone(&self.rule_snapshot),
            latency_profile: Arc::clone(&self.latency_profile),
            scenario: Arc::clone(&self.scenario),
            hidden_platform_markets: checkpoint.hidden_platform_markets,
            public_markets: checkpoint.public_markets,
            execution_state: checkpoint.execution_state,
            terminal_projection_pending: BTreeSet::new(),
            market_enrollment: Arc::new(checkpoint.market_enrollment),
            initialization: Arc::clone(&record.initialization),
            state: checkpoint.state.restore(record.spec.clone())?,
        })
    }

    pub fn registered_session_count(&self) -> usize {
        self.session_creations.len()
    }

    pub(crate) fn capital_issuance_fence_count(&self) -> usize {
        self.account_issuance_fence.len()
    }

    pub(crate) fn is_session_retired(&self, session_id: &str) -> bool {
        !self.session_creations.contains_key(session_id)
            && self.capital_fence_for_session(session_id).is_some()
    }

    pub(crate) fn retire_session(&mut self, session_id: &str) -> Result<(), SessionError> {
        if self.session_creations.remove(session_id).is_some()
            || self.is_session_retired(session_id)
        {
            Ok(())
        } else {
            Err(SessionError::SessionNotFound(session_id.to_owned()))
        }
    }

    pub(crate) fn opening_anchor_for_session(
        &self,
        session_id: &str,
    ) -> Option<&AccountOpeningAnchorV1> {
        self.session_creations
            .get(session_id)
            .map(|record| &record.opening_anchor)
    }

    pub(crate) fn validate_durable_session_creation(
        &self,
        spec: &SessionSpecV2,
        opening_anchor: &AccountOpeningAnchorV1,
        initialization: &SessionInitializationSnapshotV1,
    ) -> bool {
        self.session_creations
            .get(&spec.session_id)
            .is_some_and(|record| {
                record.spec == *spec
                    && record.opening_anchor == *opening_anchor
                    && initialization.state == SessionInitializationStateV1::PreparedLedgerInit
                    && initialization.activation_id.is_none()
                    && initialization.failure_id.is_none()
            })
    }

    pub(crate) fn validate_durable_capital_fence(
        &self,
        spec: &SessionSpecV2,
        opening_anchor: &AccountOpeningAnchorV1,
    ) -> bool {
        let Ok((account_instance_key, spec_sha256, expected_opening_anchor)) =
            self.validated_creation_material(spec)
        else {
            return false;
        };
        if expected_opening_anchor != *opening_anchor {
            return false;
        }
        self.account_issuance_fence
            .get(&account_instance_key)
            .is_some_and(|fence| {
                fence
                    == &CapitalIssuanceFenceV1 {
                        session_id: spec.session_id.clone(),
                        account_instance_key,
                        opening_anchor: opening_anchor.clone(),
                        creation_request_id: spec.creation_request_id.clone(),
                        spec_sha256,
                    }
            })
    }

    fn capital_fence_for_session(&self, session_id: &str) -> Option<&CapitalIssuanceFenceV1> {
        self.account_issuance_fence
            .values()
            .find(|fence| fence.session_id == session_id)
    }

    fn validate_checkpoint_capital_fence(
        &self,
        fence: &CapitalIssuanceFenceV1,
    ) -> Result<(), SessionCheckpointErrorV1> {
        let identity_conflict =
            || SessionCheckpointErrorV1::CreationIdentityConflict(fence.session_id.clone());
        let anchor = &fence.opening_anchor;
        if !is_stable_text(&fence.session_id)
            || !is_stable_text(&fence.creation_request_id)
            || !is_sha256_hex(&fence.spec_sha256.0)
            || fence.account_instance_key.run_id != self.run_id.as_ref()
            || !is_stable_text(&fence.account_instance_key.caller_instance_id)
            || !is_stable_text(&fence.account_instance_key.account_id)
            || fence.account_instance_key.platform_source != PlatformSourceV1::Simulated
            || anchor.schema_version != ACCOUNT_OPENING_ANCHOR_SCHEMA_V1
            || anchor.account_instance_key != fence.account_instance_key
            || anchor.session_id != fence.session_id
            || anchor.initial_scenario_sha256 != *self.scenario.scenario_sha256()
            || anchor.initial_market_set_sha256 != *self.scenario.market_set_sha256()
            || !is_sha256_hex(&anchor.anchor_id)
            || !is_sha256_hex(&anchor.anchor_sha256.0)
            || !is_sha256_hex(&anchor.initial_account_state_sha256.0)
        {
            return Err(identity_conflict());
        }
        PlatformAccountStateV1::try_from_opening_anchor(anchor).map_err(|_| identity_conflict())?;
        let expected_anchor_id = hash_json(
            ACCOUNT_OPENING_ANCHOR_ID_DOMAIN_V1,
            &AccountOpeningAnchorIdentityMaterialV1 {
                schema_version: ACCOUNT_OPENING_ANCHOR_SCHEMA_V1,
                account_instance_key: &anchor.account_instance_key,
                session_id: &anchor.session_id,
                initial_scenario_sha256: &anchor.initial_scenario_sha256,
                initial_market_set_sha256: &anchor.initial_market_set_sha256,
                initial_account_state_sha256: &anchor.initial_account_state_sha256,
                initial_platform_account: &anchor.initial_platform_account,
            },
        )
        .map_err(|_| identity_conflict())?
        .0;
        if anchor.anchor_id != expected_anchor_id {
            return Err(identity_conflict());
        }
        let expected_anchor_sha256 = hash_json(
            ACCOUNT_OPENING_ANCHOR_HASH_DOMAIN_V1,
            &AccountOpeningAnchorHashMaterialV1 {
                schema_version: ACCOUNT_OPENING_ANCHOR_SCHEMA_V1,
                anchor_id: &anchor.anchor_id,
                account_instance_key: &anchor.account_instance_key,
                session_id: &anchor.session_id,
                initial_scenario_sha256: &anchor.initial_scenario_sha256,
                initial_market_set_sha256: &anchor.initial_market_set_sha256,
                initial_account_state_sha256: &anchor.initial_account_state_sha256,
                initial_platform_account: &anchor.initial_platform_account,
            },
        )
        .map_err(|_| identity_conflict())?;
        if anchor.anchor_sha256 != expected_anchor_sha256 {
            return Err(identity_conflict());
        }
        Ok(())
    }

    pub(crate) fn current_initialization_for_session(
        &self,
        session_id: &str,
    ) -> Result<SessionInitializationSnapshotV1, SessionError> {
        let record = self
            .session_creations
            .get(session_id)
            .ok_or_else(|| SessionError::SessionNotFound(session_id.to_owned()))?;
        initialization_snapshot(&record.initialization)
    }

    pub(crate) fn checkpoint(
        &self,
    ) -> Result<SimProcessRootCheckpointV1, SessionCheckpointErrorV1> {
        let capital_issuance_fences = self.account_issuance_fence.values().cloned().collect();
        let session_creations = self
            .session_creations
            .values()
            .map(|record| {
                Ok(SessionCreationCheckpointV2 {
                    spec: record.spec.clone(),
                    spec_sha256: record.spec_sha256.clone(),
                    account_instance_key: record.account_instance_key.clone(),
                    opening_anchor: record.opening_anchor.clone(),
                    initialization: initialization_snapshot(&record.initialization)?,
                })
            })
            .collect::<Result<Vec<_>, SessionError>>()?;
        Ok(SimProcessRootCheckpointV1 {
            schema_version: SIM_PROCESS_ROOT_CHECKPOINT_SCHEMA_V1.to_owned(),
            run_id: self.run_id.to_string(),
            rule_snapshot_file_sha256: self.rule_snapshot.file_sha256().clone(),
            finality_policy_id: self.finality_policy.policy_id().to_owned(),
            finality_policy_content_sha256: self.finality_policy.content_sha256().clone(),
            latency_profile_id: self.latency_profile.profile_id().to_owned(),
            latency_profile_version: self.latency_profile.profile_version(),
            latency_profile_file_sha256: self.latency_profile.file_sha256().clone(),
            scenario_market_set_sha256: self.scenario.market_set_sha256().clone(),
            scenario_sha256: self.scenario.scenario_sha256().clone(),
            capital_issuance_fences,
            session_creations,
        })
    }

    pub(crate) fn restore_from_checkpoint(
        checkpoint: SimProcessRootCheckpointV1,
        finality_policy: Arc<TradeFinalityPolicyV1>,
        rule_snapshot: Arc<SealedProcessRuleSnapshot>,
        latency_profile: Arc<EmpiricalDelayProfile>,
        scenario: Arc<PlatformScenarioV1>,
    ) -> Result<Self, SessionCheckpointErrorV1> {
        if checkpoint.schema_version != SIM_PROCESS_ROOT_CHECKPOINT_SCHEMA_V1 {
            return Err(SessionCheckpointErrorV1::UnsupportedSchema(
                checkpoint.schema_version,
            ));
        }
        validate_dependency_identity(
            "平台规则快照",
            &checkpoint.rule_snapshot_file_sha256,
            rule_snapshot.file_sha256(),
        )?;
        if checkpoint.finality_policy_id != finality_policy.policy_id()
            || checkpoint.finality_policy_content_sha256 != *finality_policy.content_sha256()
        {
            return Err(SessionCheckpointErrorV1::DependencyIdentityConflict(
                "成交最终性策略",
            ));
        }
        if checkpoint.latency_profile_id != latency_profile.profile_id()
            || checkpoint.latency_profile_version != latency_profile.profile_version()
            || checkpoint.latency_profile_file_sha256 != *latency_profile.file_sha256()
        {
            return Err(SessionCheckpointErrorV1::DependencyIdentityConflict(
                "代理时延配置档",
            ));
        }
        if checkpoint.scenario_market_set_sha256 != *scenario.market_set_sha256()
            || checkpoint.scenario_sha256 != *scenario.scenario_sha256()
        {
            return Err(SessionCheckpointErrorV1::DependencyIdentityConflict(
                "平台场景",
            ));
        }

        let mut root = Self::try_new(
            checkpoint.run_id,
            finality_policy,
            rule_snapshot,
            latency_profile,
            scenario,
        )?;
        for fence in checkpoint.capital_issuance_fences {
            root.validate_checkpoint_capital_fence(&fence)?;
            if root
                .account_issuance_fence
                .contains_key(&fence.account_instance_key)
                || root.capital_fence_for_session(&fence.session_id).is_some()
                || root
                    .creation_requests
                    .contains_key(&fence.creation_request_id)
            {
                return Err(SessionCheckpointErrorV1::CreationIdentityConflict(
                    fence.session_id,
                ));
            }
            root.creation_requests
                .insert(fence.creation_request_id.clone(), fence.session_id.clone());
            root.account_issuance_fence
                .insert(fence.account_instance_key.clone(), fence);
        }
        for creation in checkpoint.session_creations {
            validate_checkpoint_initial_account_state(&creation.spec)?;
            validate_initialization_snapshot(&creation.initialization)?;
            let (account_instance_key, spec_sha256, opening_anchor) =
                root.validated_creation_material(&creation.spec)?;
            if creation.account_instance_key != account_instance_key
                || creation.spec_sha256 != spec_sha256
                || creation.opening_anchor != opening_anchor
            {
                return Err(SessionCheckpointErrorV1::CreationIdentityConflict(
                    creation.spec.session_id,
                ));
            }
            let expected_fence = CapitalIssuanceFenceV1 {
                session_id: creation.spec.session_id.clone(),
                account_instance_key: account_instance_key.clone(),
                opening_anchor: opening_anchor.clone(),
                creation_request_id: creation.spec.creation_request_id.clone(),
                spec_sha256: spec_sha256.clone(),
            };
            if root
                .session_creations
                .contains_key(&creation.spec.session_id)
                || root.account_issuance_fence.get(&account_instance_key) != Some(&expected_fence)
                || root
                    .creation_requests
                    .get(&creation.spec.creation_request_id)
                    != Some(&creation.spec.session_id)
            {
                return Err(SessionCheckpointErrorV1::CreationIdentityConflict(
                    creation.spec.session_id,
                ));
            }

            let session_id = creation.spec.session_id.clone();
            let initialization = Arc::new(Mutex::new(creation.initialization));
            root.session_creations.insert(
                session_id,
                SessionCreationRecordV2 {
                    spec: creation.spec,
                    spec_sha256: spec_sha256.clone(),
                    account_instance_key: account_instance_key.clone(),
                    opening_anchor: opening_anchor.clone(),
                    initialization,
                },
            );
        }
        Ok(root)
    }
}

#[derive(Clone)]
pub struct SimSession {
    finality_policy: Arc<TradeFinalityPolicyV1>,
    rule_snapshot: Arc<SealedProcessRuleSnapshot>,
    latency_profile: Arc<EmpiricalDelayProfile>,
    scenario: Arc<PlatformScenarioV1>,
    hidden_platform_markets: Option<MarketStateStoreV1>,
    public_markets: MarketStateStoreV1,
    execution_state: ExecutionStateV1,
    terminal_projection_pending: BTreeSet<String>,
    market_enrollment: Arc<MarketEnrollmentStateV1>,
    initialization: SharedSessionInitializationV1,
    state: SessionState,
}

impl SimSession {
    pub fn run_id(&self) -> &str {
        &self.state.spec.run_id
    }

    pub fn finality_policy(&self) -> &Arc<TradeFinalityPolicyV1> {
        &self.finality_policy
    }

    pub fn rule_snapshot(&self) -> &Arc<SealedProcessRuleSnapshot> {
        &self.rule_snapshot
    }

    pub fn latency_profile(&self) -> &Arc<EmpiricalDelayProfile> {
        &self.latency_profile
    }

    pub fn scenario(&self) -> &Arc<PlatformScenarioV1> {
        &self.scenario
    }

    pub fn session_spec(&self) -> &SessionSpecV2 {
        &self.state.spec
    }

    pub(crate) fn checkpoint(&self) -> SimSessionCheckpointV1 {
        SimSessionCheckpointV1 {
            schema_version: SIM_SESSION_CHECKPOINT_SCHEMA_V1.to_owned(),
            session_id: self.state.spec.session_id.clone(),
            hidden_platform_markets: self.hidden_platform_markets.clone(),
            public_markets: self.public_markets.clone(),
            execution_state: self.execution_state.detached_clone(),
            market_enrollment: self.market_enrollment.as_ref().clone(),
            state: SessionStateCheckpointV1::from(&self.state),
        }
    }

    pub(crate) fn initialization_snapshot(
        &self,
    ) -> Result<SessionInitializationSnapshotV1, SessionError> {
        initialization_snapshot(&self.initialization)
    }

    pub(crate) fn require_active(&self) -> Result<(), SessionError> {
        let snapshot = self.initialization_snapshot()?;
        if snapshot.state == SessionInitializationStateV1::Active {
            Ok(())
        } else {
            Err(SessionError::SessionNotActive(snapshot.state))
        }
    }

    pub(crate) fn market_directory(&self) -> &SessionMarketDirectoryV1 {
        &self.market_enrollment.directory
    }

    pub(crate) fn durable_market_enrollment_chain(&self) -> &[MarketEnrollmentV1] {
        &self.market_enrollment.chain
    }

    pub(crate) fn resolution_terminal_condition_ids(&self) -> BTreeSet<String> {
        let markets = match self.session_spec().caller_role {
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => self
                .hidden_platform_markets
                .as_ref()
                .expect("训练或离线会话必须拥有隐藏市场状态"),
            SimCallerRoleV1::Paper => &self.public_markets,
        };
        markets
            .lifecycle_phase_recovery_states()
            .into_iter()
            .filter_map(|(condition_id, phase)| {
                matches!(
                    phase,
                    MarketLifecyclePhaseV1::ResolutionTerminal | MarketLifecyclePhaseV1::Evicted
                )
                .then_some(condition_id)
            })
            .collect()
    }

    pub(crate) fn durable_bucket_state_matches(
        &self,
        path: MarketEventPathV1,
        reference: Option<&DurableMarketBucketReferenceV1>,
    ) -> bool {
        let store = match path {
            MarketEventPathV1::HiddenPlatform => match &self.hidden_platform_markets {
                Some(store) => store,
                None => return false,
            },
            MarketEventPathV1::PublicVisibility => &self.public_markets,
        };
        let Some(reference) = reference else {
            let cursor_is_default = match path {
                MarketEventPathV1::HiddenPlatform => {
                    self.platform_prefix_cursor()
                        .closed_through_source_event_at_utc_ms()
                        .is_none()
                        && self
                            .platform_prefix_cursor()
                            .next_event_time_index_position()
                            == 0
                }
                MarketEventPathV1::PublicVisibility => {
                    self.public_visibility_cursor()
                        .closed_through_received_at_utc_ms()
                        .is_none()
                        && self
                            .public_visibility_cursor()
                            .next_visibility_index_position()
                            == 0
                }
            };
            return !store.has_last_applied_bucket()
                && cursor_is_default
                && self.input_path_seal(path).is_none();
        };
        if reference.path != path {
            return false;
        }
        let seal_matches = match reference.closure {
            MillisecondBucketClosureV1::EndOfInput => {
                self.input_path_seal(path) == Some(&store.expected_input_seal_outcome())
            }
            MillisecondBucketClosureV1::NextEventAt(_) => self.input_path_seal(path).is_none(),
        };
        let cursor_matches = match path {
            MarketEventPathV1::HiddenPlatform => {
                self.platform_prefix_cursor()
                    .next_event_time_index_position()
                    == reference.next_index_position
                    && self
                        .platform_prefix_cursor()
                        .closed_through_source_event_at_utc_ms()
                        == Some(reference.bucket_at_utc_ms)
            }
            MarketEventPathV1::PublicVisibility => {
                self.public_visibility_cursor()
                    .next_visibility_index_position()
                    == reference.next_index_position
                    && self
                        .public_visibility_cursor()
                        .closed_through_received_at_utc_ms()
                        == Some(reference.bucket_at_utc_ms)
            }
        };
        seal_matches
            && cursor_matches
            && store.last_applied_bucket_matches_durable_reference(reference)
    }

    #[cfg(test)]
    pub(crate) fn shares_market_enrollment_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.market_enrollment, &other.market_enrollment)
    }

    #[cfg(test)]
    pub(crate) fn market_enrollment_storage_identity(&self) -> usize {
        Arc::as_ptr(&self.market_enrollment) as usize
    }

    #[cfg(test)]
    pub(crate) fn market_enrollment_storage_strong_count(&self) -> usize {
        Arc::strong_count(&self.market_enrollment)
    }

    pub(crate) fn enroll_market(
        &mut self,
        request: MarketEnrollmentRequestV1,
        account_instance_key: &AccountInstanceKeyV1,
        opening_anchor: &AccountOpeningAnchorV1,
    ) -> Result<MarketEnrollmentV1, SessionError> {
        self.require_active()?;
        validate_market_enrollment_request(&request)?;
        if let Some(existing_index) = self
            .market_enrollment
            .records_by_request_id
            .get(&request.enrollment_request_id)
        {
            let existing = &self.market_enrollment.chain[*existing_index];
            if &existing.market == request.market.market()
                && &existing.assets == request.market.assets()
            {
                return Ok(existing.clone());
            }
            return Err(SessionError::MarketEnrollmentRequestConflict(
                request.enrollment_request_id,
            ));
        }
        if self.state.has_any_input_seal() {
            return Err(SessionError::MarketEnrollmentFrozenAfterInputExhausted);
        }
        if matches!(
            self.session_spec().caller_role,
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation
        ) && self.market_enrollment.business_input_started
        {
            return Err(SessionError::MarketEnrollmentFrozenAfterBusinessInput);
        }

        let condition_id = &request.market.market().condition_id.0;
        if self
            .market_enrollment
            .paper_candidate_condition_ids
            .contains(condition_id)
        {
            return Err(SessionError::MarketEnrollmentCandidateAlreadyUsed(
                condition_id.clone(),
            ));
        }
        if let Some(asset) = request.market.assets().iter().find(|asset| {
            self.market_enrollment
                .paper_candidate_token_ids
                .contains(&asset.token_id.0)
        }) {
            return Err(SessionError::MarketEnrollmentCandidateAlreadyUsed(
                asset.token_id.0.clone(),
            ));
        }

        let enrollment = build_market_enrollment(
            &request,
            account_instance_key,
            opening_anchor,
            self.market_enrollment.chain.last(),
        )?;
        let mut working = self.clone();
        let working_market_enrollment = Arc::make_mut(&mut working.market_enrollment);
        working_market_enrollment
            .directory
            .try_insert(request.market.clone())
            .map_err(SessionError::from)?;
        working
            .public_markets
            .insert_market(&request.market)
            .map_err(|error| SessionError::MarketEnrollmentState(error.to_string()))?;
        if let Some(hidden) = working.hidden_platform_markets.as_mut() {
            hidden
                .insert_market(&request.market)
                .map_err(|error| SessionError::MarketEnrollmentState(error.to_string()))?;
        }
        working
            .execution_state
            .insert_market(&request.market)
            .map_err(SessionError::from)?;
        let enrollment_index = working_market_enrollment.chain.len();
        working_market_enrollment.chain.push(enrollment.clone());
        working_market_enrollment
            .records_by_request_id
            .insert(request.enrollment_request_id, enrollment_index);
        *self = working;
        Ok(enrollment)
    }

    pub(crate) fn record_successful_business_request(
        &mut self,
        condition_ids: BTreeSet<String>,
        token_ids: BTreeSet<String>,
    ) {
        let prepared = self.prepare_successful_business_market_use(condition_ids, token_ids);
        self.commit_prevalidated_successful_business_market_use(prepared);
    }

    pub(crate) fn prepare_successful_business_market_use(
        &self,
        condition_ids: BTreeSet<String>,
        token_ids: BTreeSet<String>,
    ) -> PreparedBusinessMarketUseV1 {
        match self.session_spec().caller_role {
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
                PreparedBusinessMarketUseV1 {
                    mark_business_input_started: !self.market_enrollment.business_input_started,
                    paper_candidate_condition_ids: BTreeSet::new(),
                    paper_candidate_token_ids: BTreeSet::new(),
                }
            }
            SimCallerRoleV1::Paper => {
                let paper_candidate_condition_ids = condition_ids
                    .into_iter()
                    .filter(|condition_id| {
                        !self.market_enrollment.directory.contains(condition_id)
                            && !self
                                .market_enrollment
                                .paper_candidate_condition_ids
                                .contains(condition_id)
                    })
                    .collect::<BTreeSet<_>>();
                let paper_candidate_token_ids = token_ids
                    .into_iter()
                    .filter(|token_id| {
                        !self.market_enrollment.directory.contains_token(token_id)
                            && !self
                                .market_enrollment
                                .paper_candidate_token_ids
                                .contains(token_id)
                    })
                    .collect::<BTreeSet<_>>();
                PreparedBusinessMarketUseV1 {
                    mark_business_input_started: false,
                    paper_candidate_condition_ids,
                    paper_candidate_token_ids,
                }
            }
        }
    }

    pub(crate) fn commit_prevalidated_successful_business_market_use(
        &mut self,
        prepared: PreparedBusinessMarketUseV1,
    ) {
        if !prepared.mark_business_input_started
            && prepared.paper_candidate_condition_ids.is_empty()
            && prepared.paper_candidate_token_ids.is_empty()
        {
            return;
        }
        let market_enrollment = Arc::make_mut(&mut self.market_enrollment);
        if prepared.mark_business_input_started {
            market_enrollment.business_input_started = true;
        }
        market_enrollment
            .paper_candidate_condition_ids
            .extend(prepared.paper_candidate_condition_ids);
        market_enrollment
            .paper_candidate_token_ids
            .extend(prepared.paper_candidate_token_ids);
    }

    pub(crate) fn record_successful_market_input(&mut self) {
        if matches!(
            self.session_spec().caller_role,
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation
        ) && !self.market_enrollment.business_input_started
        {
            Arc::make_mut(&mut self.market_enrollment).business_input_started = true;
        }
    }

    pub(crate) fn input_path_seal(
        &self,
        path: MarketEventPathV1,
    ) -> Option<&InputPathSealOutcomeV1> {
        self.state.input_path_seal(path)
    }

    pub(crate) fn is_quiescent_for_retention(&self) -> bool {
        self.state.pending_actions.is_empty()
            && self.state.expected_direct_response_request_ids.is_empty()
            && self.state.pending_private_facts.is_empty()
            && self.state.in_flight_action_identity.is_none()
    }

    pub(crate) fn commit_input_path_seal(
        &mut self,
        path: MarketEventPathV1,
        outcome: InputPathSealOutcomeV1,
    ) {
        self.state.commit_input_path_seal(path, outcome);
    }

    pub(crate) fn failed_input_seal(&self) -> Option<(MarketEventPathV1, &InputPathSealOutcomeV1)> {
        self.state.failed_input_seal()
    }

    pub(crate) fn require_input_not_failed(&self) -> Result<(), SessionError> {
        if self.failed_input_seal().is_some() {
            Err(SessionError::InputFailedSealed)
        } else {
            Ok(())
        }
    }

    pub fn platform_prefix_cursor(&self) -> &PlatformPrefixCursorV1 {
        &self.state.platform_prefix
    }

    pub fn public_visibility_cursor(&self) -> &PublicVisibilityCursorV1 {
        &self.state.public_visibility
    }

    pub fn private_feedback_sequence(&self) -> u64 {
        self.state.private_feedback_sequence
    }

    pub fn model_visible_cursor(&self) -> ModelVisibleCursorV1 {
        self.state.model_visible_cursor()
    }

    #[cfg(test)]
    pub(crate) fn close_platform_prefix_through(
        &mut self,
        source_event_at_utc_ms: i64,
        next_event_time_index_position: u64,
    ) -> Result<(), SessionError> {
        if self.hidden_platform_markets.is_none() {
            return Err(SessionError::HiddenPlatformReplayForbidden);
        }
        self.validate_platform_prefix_closure(
            source_event_at_utc_ms,
            next_event_time_index_position,
        )?;
        self.commit_platform_prefix_closure(source_event_at_utc_ms, next_event_time_index_position);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn advance_public_visibility_through(
        &mut self,
        received_at_utc_ms: i64,
        next_visibility_index_position: u64,
    ) -> Result<(), SessionError> {
        self.validate_public_visibility_closure(
            received_at_utc_ms,
            next_visibility_index_position,
        )?;
        self.commit_public_visibility_closure(received_at_utc_ms, next_visibility_index_position);
        Ok(())
    }

    pub(crate) fn hidden_platform_markets(&self) -> Option<&MarketStateStoreV1> {
        self.hidden_platform_markets.as_ref()
    }

    pub(crate) fn public_markets(&self) -> &MarketStateStoreV1 {
        &self.public_markets
    }

    pub(crate) fn hidden_platform_markets_mut(&mut self) -> Option<&mut MarketStateStoreV1> {
        self.hidden_platform_markets.as_mut()
    }

    pub(crate) fn public_markets_mut(&mut self) -> &mut MarketStateStoreV1 {
        &mut self.public_markets
    }

    pub(crate) fn account_overlay(&self) -> &AccountBookOverlayStoreV1 {
        self.execution_state.account_overlay()
    }

    #[cfg(test)]
    pub(crate) fn account_overlay_mut(&mut self) -> &mut AccountBookOverlayStoreV1 {
        self.execution_state.account_overlay_mut()
    }

    pub(crate) fn execution_state(&self) -> &ExecutionStateV1 {
        &self.execution_state
    }

    pub(crate) fn execution_state_mut(&mut self) -> &mut ExecutionStateV1 {
        &mut self.execution_state
    }

    pub(crate) fn replace_execution_state(&mut self, state: ExecutionStateV1) {
        self.execution_state = state;
    }

    pub(crate) fn record_terminal_projection_pending(&mut self, condition_id: &str) {
        self.terminal_projection_pending
            .insert(condition_id.to_owned());
    }

    pub(crate) fn pending_terminal_projection_condition_ids(&self) -> Vec<String> {
        self.terminal_projection_pending.iter().cloned().collect()
    }

    pub(crate) fn commit_terminal_projections_prepared(&mut self, condition_ids: &[String]) {
        for condition_id in condition_ids {
            self.terminal_projection_pending.remove(condition_id);
        }
    }

    pub(crate) fn validate_platform_prefix_closure(
        &self,
        source_event_at_utc_ms: i64,
        next_event_time_index_position: u64,
    ) -> Result<(), SessionError> {
        self.state.validate_platform_prefix_closure(
            source_event_at_utc_ms,
            next_event_time_index_position,
        )
    }

    pub(crate) fn commit_platform_prefix_closure(
        &mut self,
        source_event_at_utc_ms: i64,
        next_event_time_index_position: u64,
    ) {
        self.state
            .commit_platform_prefix_closure(source_event_at_utc_ms, next_event_time_index_position);
    }

    pub(crate) fn validate_public_visibility_closure(
        &self,
        received_at_utc_ms: i64,
        next_visibility_index_position: u64,
    ) -> Result<(), SessionError> {
        self.state
            .validate_public_visibility_closure(received_at_utc_ms, next_visibility_index_position)
    }

    pub(crate) fn commit_public_visibility_closure(
        &mut self,
        received_at_utc_ms: i64,
        next_visibility_index_position: u64,
    ) {
        self.state
            .commit_public_visibility_closure(received_at_utc_ms, next_visibility_index_position);
    }

    pub(crate) fn close_private_fact_schedule_through(
        &mut self,
        received_at_utc_ms: i64,
    ) -> Result<(), SessionError> {
        self.state
            .close_private_fact_schedule_through(received_at_utc_ms)
    }

    pub(crate) fn private_fact_schedule_closed_through(&self) -> Option<i64> {
        self.state.private_fact_schedule_closed_through_utc_ms
    }

    pub(crate) fn logical_platform_driven_through(&self) -> Option<i64> {
        self.state.closed_action_through_utc_ms
    }

    pub(crate) fn commit_logical_platform_driven_through(&mut self, logical_time_utc_ms: i64) {
        self.state.closed_action_through_utc_ms = Some(
            self.state
                .closed_action_through_utc_ms
                .map_or(logical_time_utc_ms, |current| {
                    current.max(logical_time_utc_ms)
                }),
        );
    }

    pub(crate) fn next_private_fact_received_at_through(
        &self,
        received_at_utc_ms: i64,
    ) -> Option<i64> {
        self.state
            .next_private_fact_received_at_through(received_at_utc_ms)
    }

    pub(crate) fn expect_direct_response(
        &mut self,
        global_request_id: &str,
    ) -> Result<(), SessionError> {
        self.state.expect_direct_response(global_request_id)
    }

    pub(crate) fn next_private_emission_sequence(&self) -> Result<u64, SessionError> {
        self.state.next_private_emission_sequence()
    }

    pub(crate) fn prepare_next_private_fact_through(
        &self,
        received_at_utc_ms: i64,
    ) -> Result<Option<VisiblePrivateFactV1>, SessionError> {
        self.state
            .prepare_next_private_fact_through(received_at_utc_ms)
    }

    pub(crate) fn prepare_private_fact_slice_v2(
        &self,
        received_at_utc_ms: i64,
    ) -> Result<PreparedPrivateFactSliceV2, SessionError> {
        self.state.prepare_private_fact_slice_v2(received_at_utc_ms)
    }

    pub(crate) fn commit_prevalidated_private_fact_slice_v2(
        &mut self,
        prepared: PreparedPrivateFactSliceV2,
    ) -> Option<VisiblePrivateFactV1> {
        self.state
            .commit_prevalidated_private_fact_slice_v2(prepared)
    }

    pub(crate) fn confirm_prepared_private_fact(
        &mut self,
        candidate: &VisiblePrivateFactV1,
    ) -> Result<(), SessionError> {
        self.state.confirm_prepared_private_fact(candidate)
    }

    pub(crate) fn validate_prepared_private_fact(
        &self,
        candidate: &VisiblePrivateFactV1,
    ) -> Result<(), SessionError> {
        self.state.validate_prepared_private_fact(candidate)
    }

    pub(crate) fn commit_prevalidated_private_fact(&mut self, candidate: &VisiblePrivateFactV1) {
        self.state.commit_prevalidated_private_fact(candidate);
    }

    pub(crate) fn register_and_schedule_request(
        &mut self,
        context: RequestContextV1,
        canonical_request_sha256: Sha256Hex,
        actions: Vec<ParsedLogicalActionV1>,
    ) -> Result<RegisteredLogicalScheduleV1, SessionError> {
        let prepared = self.prepare_request_schedule(context, canonical_request_sha256, actions)?;
        Ok(self.commit_prevalidated_request_schedule(prepared))
    }

    pub(crate) fn prepare_request_schedule(
        &self,
        context: RequestContextV1,
        canonical_request_sha256: Sha256Hex,
        actions: Vec<ParsedLogicalActionV1>,
    ) -> Result<PreparedRequestScheduleV1, SessionError> {
        let actions = actions
            .into_iter()
            .map(|action| LogicalActionPlanV1 {
                action_identity: action.action_identity,
                batch_item_ordinal: action.batch_item_ordinal,
                processing_delay_ms: self.processing_delay_ms(action.method_timing),
            })
            .collect();
        self.state.prepare_request_schedule(
            &self.latency_profile,
            context,
            canonical_request_sha256,
            actions,
        )
    }

    pub(crate) fn prepare_registered_request_retry_schedule(
        &self,
        context: &RequestContextV1,
        actions: Vec<ParsedLogicalActionV1>,
    ) -> Result<PreparedRequestScheduleV1, SessionError> {
        let actions = actions
            .into_iter()
            .map(|action| LogicalActionPlanV1 {
                action_identity: action.action_identity,
                batch_item_ordinal: action.batch_item_ordinal,
                processing_delay_ms: self.processing_delay_ms(action.method_timing),
            })
            .collect();
        self.state
            .prepare_registered_request_retry_schedule(context, actions)
    }

    pub(crate) fn commit_prevalidated_request_schedule(
        &mut self,
        prepared: PreparedRequestScheduleV1,
    ) -> RegisteredLogicalScheduleV1 {
        self.state.commit_prevalidated_request_schedule(prepared)
    }

    pub(crate) fn recovered_request_ids(&self) -> BTreeSet<String> {
        self.state.requests.keys().cloned().collect()
    }

    pub(crate) fn recovered_pending_private_facts(
        &self,
    ) -> Vec<(PrivateVisibilityEvidenceV1, i64, u64)> {
        self.state
            .pending_private_facts
            .iter()
            .map(|(key, evidence)| {
                (
                    evidence.clone(),
                    key.received_at_utc_ms,
                    key.private_emission_sequence,
                )
            })
            .collect()
    }

    pub(crate) fn validate_recovered_private_history(
        &self,
        confirmed: &[(i64, u64)],
    ) -> Result<(), SessionCheckpointErrorV1> {
        if confirmed
            .iter()
            .map(|(received_at, _)| *received_at)
            .ne(self
                .state
                .private_fact_received_at_by_sequence
                .iter()
                .copied())
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "已确认私有事实接收时点与会话可见性前缀不一致".to_owned(),
            ));
        }
        if confirmed.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "已确认私有事实没有保持统一队头顺序".to_owned(),
            ));
        }
        let pending = self.recovered_pending_private_facts();
        if confirmed.last().is_some_and(|last| {
            pending
                .first()
                .is_some_and(|(_, received_at, emission)| *last >= (*received_at, *emission))
        }) {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "已确认与待接收私有事实没有保持统一队头顺序".to_owned(),
            ));
        }
        let mut emissions = confirmed
            .iter()
            .map(|(_, emission_sequence)| *emission_sequence)
            .collect::<BTreeSet<_>>();
        if emissions.len() != confirmed.len() {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "已确认私有事实包含重复发射序号".to_owned(),
            ));
        }
        for (_, _, emission_sequence) in pending {
            if !emissions.insert(emission_sequence) {
                return Err(SessionCheckpointErrorV1::InvalidSessionState(
                    "已确认与待接收私有事实发射序号交叠".to_owned(),
                ));
            }
        }
        if emissions != self.state.used_private_emission_sequences {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "私有事实正文未精确覆盖已分配发射序号前缀".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn recovered_partially_processed_request_ids(&self) -> BTreeSet<String> {
        self.state
            .requests
            .keys()
            .filter(|request_id| {
                let mut has_released = false;
                let mut has_pending = false;
                for (action_identity, (_, action_request_id)) in &self.state.action_keys {
                    if action_request_id != *request_id {
                        continue;
                    }
                    if self.state.released_action_ids.contains(action_identity) {
                        has_released = true;
                    } else {
                        has_pending = true;
                    }
                }
                has_released && has_pending
            })
            .cloned()
            .collect()
    }

    pub(crate) fn recovered_unformed_direct_response_request_ids(&self) -> BTreeSet<String> {
        self.state
            .requests
            .iter()
            .filter_map(|(request_id, record)| {
                record
                    .response_transit
                    .is_none()
                    .then_some(request_id.clone())
            })
            .collect()
    }

    pub(crate) fn recovered_query_revisions(&self) -> BTreeMap<String, u64> {
        self.state
            .queries
            .iter()
            .map(|(request_id, record)| (request_id.clone(), record.platform_state_revision))
            .collect()
    }

    pub(crate) fn query_revision_for_request(&self, request_id: &str) -> Option<u64> {
        self.state
            .queries
            .get(request_id)
            .map(|record| record.platform_state_revision)
    }

    pub(crate) fn validate_recovered_action_results(
        &self,
        action_identities: &BTreeSet<String>,
    ) -> Result<(), SessionCheckpointErrorV1> {
        if self.state.released_action_ids != *action_identities {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "已释放动作集合与耐久动作结果不一致".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_recovered_mutation_records(
        &self,
        expected: &BTreeMap<String, (Sha256Hex, String)>,
    ) -> Result<(), SessionCheckpointErrorV1> {
        if self.state.mutations.len() != expected.len()
            || self.state.mutations.iter().any(|(identity, record)| {
                expected.get(identity)
                    != Some(&(
                        record.payload_sha256.clone(),
                        record.effect_identity.clone(),
                    ))
            })
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "变更幂等记录与已释放动作结果不一致".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_recovered_query_records(
        &self,
        expected: &BTreeMap<String, (Sha256Hex, u64)>,
    ) -> Result<(), SessionCheckpointErrorV1> {
        if self.state.queries.len() != expected.len()
            || self.state.queries.iter().any(|(request_id, record)| {
                expected.get(request_id)
                    != Some(&(record.query_sha256.clone(), record.platform_state_revision))
            })
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "查询幂等记录与已释放动作结果不一致".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_recovered_paper_market_candidates(
        &self,
        expected_condition_ids: &BTreeSet<String>,
        expected_token_ids: &BTreeSet<String>,
    ) -> Result<(), SessionCheckpointErrorV1> {
        if self.session_spec().caller_role != SimCallerRoleV1::Paper {
            return Ok(());
        }
        if self.market_enrollment.business_input_started
            || self.market_enrollment.paper_candidate_condition_ids != *expected_condition_ids
            || self.market_enrollment.paper_candidate_token_ids != *expected_token_ids
        {
            return Err(SessionCheckpointErrorV1::InvalidMarketEnrollment(
                "模拟盘市场候选集合与已登记请求正文不一致".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_recovered_request_binding(
        &self,
        global_request_id: &str,
        context: &RequestContextV1,
        canonical_request_sha256: &Sha256Hex,
    ) -> Result<(), SessionCheckpointErrorV1> {
        let record = self.state.requests.get(global_request_id).ok_or_else(|| {
            SessionCheckpointErrorV1::InvalidSessionState(format!(
                "执行层待处理请求不属于会话调度记录：{global_request_id}"
            ))
        })?;
        if context.global_request_id != global_request_id
            || record.context != *context
            || record.canonical_request_sha256 != *canonical_request_sha256
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                "执行层待处理请求与会话调度身份不符：{global_request_id}"
            )));
        }
        Ok(())
    }

    pub(crate) fn validate_recovered_request_schedule(
        &self,
        global_request_id: &str,
        transit: &RequestTransitV1,
        actions: &[QueuedLogicalActionV1],
    ) -> Result<(), SessionCheckpointErrorV1> {
        let record = self.state.requests.get(global_request_id).ok_or_else(|| {
            SessionCheckpointErrorV1::InvalidSessionState(format!(
                "耐久请求证据不属于会话调度记录：{global_request_id}"
            ))
        })?;
        if &record.transit != transit || actions.is_empty() {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                "耐久请求传输或动作集合与会话调度记录不一致：{global_request_id}"
            )));
        }
        let mut expected_actions = self
            .state
            .action_keys
            .iter()
            .filter(|(_, (_, request_id))| request_id == global_request_id)
            .map(|(identity, (key, request_id))| QueuedLogicalActionV1 {
                action_identity: identity.clone(),
                global_request_id: request_id.clone(),
                key: *key,
            })
            .collect::<Vec<_>>();
        expected_actions.sort_by_key(|action| action.key.batch_item_ordinal);
        if expected_actions != actions {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                "耐久请求动作身份与会话调度记录不一致：{global_request_id}"
            )));
        }
        Ok(())
    }

    pub(crate) fn complete_direct_response(
        &mut self,
        global_request_id: &str,
        platform_responded_at_utc_ms: i64,
        private_emission_sequence: u64,
    ) -> Result<RequestTransitV1, SessionError> {
        self.state.complete_direct_response(
            global_request_id,
            platform_responded_at_utc_ms,
            private_emission_sequence,
        )
    }

    pub(crate) fn bind_lifecycle_feedback_timing(
        &mut self,
        feedback_id: String,
        platform_event_at_utc_ms: i64,
        private_emission_sequence: u64,
    ) -> Result<LifecycleFeedbackTimingV1, SessionError> {
        self.state.bind_lifecycle_feedback_timing(
            &self.latency_profile,
            feedback_id,
            platform_event_at_utc_ms,
            private_emission_sequence,
        )
    }

    fn processing_delay_ms(&self, method_timing: ParsedMethodTimingV1) -> u64 {
        match method_timing {
            ParsedMethodTimingV1::Ordinary => 0,
            ParsedMethodTimingV1::FokFakMarketBuy
                if self
                    .rule_snapshot
                    .synchronized_rules()
                    .taker_order_delay_enabled =>
            {
                self.rule_snapshot
                    .snapshot()
                    .documented_platform_rules
                    .taker_delay
                    .value
                    .delay_ms
            }
            ParsedMethodTimingV1::FokFakMarketBuy => 0,
        }
    }

    pub(crate) fn complete_logical_item(
        &mut self,
        action_identity: &str,
    ) -> Result<bool, SessionError> {
        self.state.complete_logical_item(action_identity)
    }

    pub(crate) fn first_pending_action_through(
        &self,
        effective_processing_at_utc_ms: i64,
    ) -> Option<QueuedLogicalActionV1> {
        self.state
            .first_pending_action_through(effective_processing_at_utc_ms)
    }

    pub(crate) fn prepare_execution_action_slice_v2(
        &mut self,
        effective_processing_at_utc_ms: i64,
        complete_same_request_at_same_time: bool,
    ) -> Result<PreparedExecutionActionSliceV2<'_>, SessionError> {
        let SimSession {
            latency_profile,
            state,
            ..
        } = self;
        let latency_profile = Arc::as_ref(latency_profile);
        state.prepare_execution_action_slice_v2(
            latency_profile,
            effective_processing_at_utc_ms,
            complete_same_request_at_same_time,
        )
    }

    pub(crate) fn prepare_scheduled_session_drive_v2(
        &mut self,
        requested_cutoff_utc_ms: i64,
        complete_same_request_at_same_time: bool,
    ) -> Result<PreparedScheduledSessionDriveV2<'_>, MarketEventErrorV1> {
        let SimSession {
            latency_profile,
            hidden_platform_markets,
            public_markets,
            execution_state,
            terminal_projection_pending,
            state,
            ..
        } = self;
        let action = state.prepare_execution_action_slice_v2(
            Arc::as_ref(latency_profile),
            requested_cutoff_utc_ms,
            complete_same_request_at_same_time,
        )?;
        let actual_cutoff_utc_ms = action.logical_platform_driven_through_utc_ms();
        let first_due_at_utc_ms = [
            execution_state.next_gtd_deadline_at_utc_ms(),
            execution_state.next_scheduled_finality_at_utc_ms(),
            execution_state.next_rebate_clock_at_utc_ms(),
        ]
        .into_iter()
        .flatten()
        .filter(|at_utc_ms| *at_utc_ms <= actual_cutoff_utc_ms)
        .min();
        let execution = execution_state.prepare_execution_drive_delta_v2(actual_cutoff_utc_ms)?;

        let mut redeemed_resolutions = BTreeMap::<String, MarketResolutionTerminalV3>::new();
        for effect in execution
            .scheduled_finality_effects()
            .iter()
            .flat_map(|lifecycle| lifecycle.settlement_feedbacks.iter())
            .chain(execution.rebate_clock_effects().iter())
        {
            let Some((condition_id, resolution)) = effect.action.redeemed_resolution() else {
                continue;
            };
            if let Some(existing) = redeemed_resolutions.get(condition_id) {
                if existing != resolution {
                    return Err(MarketEventErrorV1::ResolutionConflict(
                        condition_id.to_owned(),
                    ));
                }
            } else {
                redeemed_resolutions.insert(condition_id.to_owned(), resolution.clone());
            }
        }

        let hidden_platform_markets = hidden_platform_markets.as_mut();
        let mut hidden_terminal_deltas = Vec::new();
        let mut public_terminal_deltas = Vec::new();
        let mut newly_redeemed_condition_ids = BTreeSet::new();
        for (condition_id, resolution) in redeemed_resolutions {
            let (hidden_delta, public_delta) = prepare_redeemed_resolution_terminal_paths_v2(
                hidden_platform_markets.as_deref(),
                public_markets,
                &condition_id,
                &resolution,
            )?;
            if let Some(delta) = hidden_delta {
                hidden_terminal_deltas.push(delta);
            }
            if let Some(delta) = public_delta {
                public_terminal_deltas.push(delta);
            }
            newly_redeemed_condition_ids.insert(condition_id);
        }

        let mut terminal_projection_condition_ids = terminal_projection_pending.clone();
        terminal_projection_condition_ids.extend(newly_redeemed_condition_ids.iter().cloned());
        Ok(PreparedScheduledSessionDriveV2 {
            action,
            execution,
            hidden_platform_markets,
            public_markets,
            hidden_terminal_deltas,
            public_terminal_deltas,
            terminal_projection_pending,
            newly_redeemed_condition_ids,
            terminal_projection_condition_ids,
            actual_cutoff_utc_ms,
            first_due_at_utc_ms,
        })
    }

    pub(crate) fn prepare_executing_closed_bucket_session_drive_v2(
        &mut self,
        bucket: &ClosedMarketEventBucketV1,
    ) -> Result<PreparedClosedBucketSessionDriveAttemptV2<'_>, MarketEventErrorV1> {
        let reference = bucket.durable_reference();
        let caller_role = self.session_spec().caller_role;
        if !bucket.drives_execution_for(caller_role) {
            if reference.path == MarketEventPathV1::HiddenPlatform
                && caller_role == SimCallerRoleV1::Paper
            {
                return Err(SessionError::HiddenPlatformReplayForbidden.into());
            }
            return Err(SessionError::ExecutionState(
                "执行闭桶稀疏准备只能处理当前角色的执行路径".to_owned(),
            )
            .into());
        }
        if self.failed_input_seal().is_some() || self.input_path_seal(reference.path).is_some() {
            return Err(MarketEventErrorV1::InputPathAlreadySealed);
        }
        if self.last_applied_bucket_coordinates_match(bucket) {
            return Err(MarketEventErrorV1::BucketIdentityConflict);
        }
        let expected_start = match reference.path {
            MarketEventPathV1::HiddenPlatform => self
                .platform_prefix_cursor()
                .next_event_time_index_position(),
            MarketEventPathV1::PublicVisibility => self
                .public_visibility_cursor()
                .next_visibility_index_position(),
        };
        if reference.start_index_position != expected_start {
            return Err(MarketEventErrorV1::BucketStartPositionConflict {
                expected: expected_start,
                actual: reference.start_index_position,
            });
        }

        let lifecycle_account_rule = self
            .rule_snapshot
            .snapshot()
            .project_account_rules
            .lifecycle_account_behavior
            .value
            .clone();
        let SimSession {
            finality_policy,
            latency_profile,
            hidden_platform_markets,
            public_markets,
            execution_state,
            terminal_projection_pending,
            market_enrollment,
            state,
            ..
        } = self;
        let action = state.prepare_closed_bucket_actionless_slice_v2(
            Arc::as_ref(latency_profile),
            reference.path,
            reference.bucket_at_utc_ms,
            reference.next_index_position,
            None,
        )?;
        let first_due_at_utc_ms = [
            execution_state.next_gtd_deadline_at_utc_ms(),
            execution_state.next_scheduled_finality_at_utc_ms(),
            execution_state.next_rebate_clock_at_utc_ms(),
        ]
        .into_iter()
        .flatten()
        .filter(|at_utc_ms| *at_utc_ms <= reference.bucket_at_utc_ms)
        .min();
        let mut execution =
            execution_state.prepare_execution_drive_delta_v2(reference.bucket_at_utc_ms)?;
        let directory = &market_enrollment.directory;
        let hidden_platform_markets = hidden_platform_markets.as_mut();
        let selected_store = match reference.path {
            MarketEventPathV1::HiddenPlatform => hidden_platform_markets
                .as_deref()
                .expect("隐藏执行闭桶必须持有隐藏市场状态"),
            MarketEventPathV1::PublicVisibility => &*public_markets,
        };
        let mut selected_bucket = selected_store.begin_bucket_delta_v2(bucket)?;
        let mut hidden_terminal_deltas = Vec::new();
        let mut public_terminal_deltas = Vec::new();
        let mut newly_redeemed_condition_ids = BTreeSet::new();

        let mut scheduled_redeemed_resolutions = BTreeMap::new();
        for effect in execution
            .scheduled_finality_effects()
            .iter()
            .flat_map(|lifecycle| lifecycle.settlement_feedbacks.iter())
            .chain(execution.rebate_clock_effects().iter())
        {
            let Some((condition_id, resolution)) = effect.action.redeemed_resolution() else {
                continue;
            };
            if let Some(existing) = scheduled_redeemed_resolutions.get(condition_id) {
                if existing != resolution {
                    return Err(MarketEventErrorV1::ResolutionConflict(
                        condition_id.to_owned(),
                    ));
                }
                continue;
            }
            scheduled_redeemed_resolutions.insert(condition_id.to_owned(), resolution.clone());
        }
        for (condition_id, resolution) in scheduled_redeemed_resolutions {
            let (next, _) = selected_bucket.stage_redeemed_resolution_terminal(
                selected_store,
                &condition_id,
                &resolution,
            )?;
            selected_bucket = next;
            let (hidden_delta, public_delta) = prepare_other_redeemed_resolution_terminal_path_v2(
                reference.path,
                hidden_platform_markets.as_deref(),
                public_markets,
                &condition_id,
                &resolution,
            )?;
            if let Some(delta) = hidden_delta {
                hidden_terminal_deltas.push(delta);
            }
            if let Some(delta) = public_delta {
                public_terminal_deltas.push(delta);
            }
            newly_redeemed_condition_ids.insert(condition_id);
        }

        execution = execution
            .prepare_authorized_market_bucket_start_v2(directory, reference.bucket_at_utc_ms)?;
        let mut dpt_effects = AuthorizedMarketEventEffectsV1::default();
        let selected_bucket = loop {
            match selected_bucket.advance(selected_store)? {
                PreparedMarketStoreBucketStepV2::Authorized(staged_event) => {
                    let condition_id = staged_event.event().market.condition_id.0.clone();
                    let (next_execution, event_effects) = execution
                        .prepare_authorized_market_event_v2(
                            directory,
                            reference.bucket_at_utc_ms,
                            staged_event.event(),
                            Arc::as_ref(finality_policy),
                            &lifecycle_account_rule,
                            staged_event.qualified_resolution(),
                        )?;
                    execution = next_execution;
                    let mut event_redeemed_resolutions = BTreeMap::new();
                    for effect in event_effects
                        .trade_finality_effects
                        .iter()
                        .flat_map(|lifecycle| lifecycle.settlement_feedbacks.iter())
                        .chain(event_effects.settlement_feedbacks.iter())
                    {
                        let Some((redeemed_condition_id, resolution)) =
                            effect.action.redeemed_resolution()
                        else {
                            continue;
                        };
                        if let Some(existing) =
                            event_redeemed_resolutions.get(redeemed_condition_id)
                        {
                            if existing != resolution {
                                return Err(MarketEventErrorV1::ResolutionConflict(
                                    redeemed_condition_id.to_owned(),
                                ));
                            }
                        } else {
                            event_redeemed_resolutions
                                .insert(redeemed_condition_id.to_owned(), resolution.clone());
                        }
                    }
                    dpt_effects.append(event_effects);
                    selected_bucket = staged_event.complete();
                    for (redeemed_condition_id, resolution) in event_redeemed_resolutions {
                        debug_assert_eq!(redeemed_condition_id, condition_id);
                        let candidate_resolution = execution
                            .candidate_redeemed_market_resolution(&redeemed_condition_id)
                            .ok_or_else(|| {
                                MarketEventErrorV1::ResolutionConflict(
                                    redeemed_condition_id.clone(),
                                )
                            })?;
                        if candidate_resolution != &resolution {
                            return Err(MarketEventErrorV1::ResolutionConflict(
                                redeemed_condition_id,
                            ));
                        }
                        let (next, _) = selected_bucket.stage_redeemed_resolution_terminal(
                            selected_store,
                            &condition_id,
                            &resolution,
                        )?;
                        selected_bucket = next;
                        if newly_redeemed_condition_ids.insert(condition_id.clone()) {
                            let (hidden_delta, public_delta) =
                                prepare_other_redeemed_resolution_terminal_path_v2(
                                    reference.path,
                                    hidden_platform_markets.as_deref(),
                                    public_markets,
                                    &condition_id,
                                    &resolution,
                                )?;
                            if let Some(delta) = hidden_delta {
                                hidden_terminal_deltas.push(delta);
                            }
                            if let Some(delta) = public_delta {
                                public_terminal_deltas.push(delta);
                            }
                        }
                    }
                }
                PreparedMarketStoreBucketStepV2::EvictedNoExecution(next) => {
                    selected_bucket = next;
                }
                PreparedMarketStoreBucketStepV2::Complete(prepared) => break prepared,
            }
        };

        let mut action = action;
        if reference.closure == MillisecondBucketClosureV1::EndOfInput {
            selected_bucket.validate_initial_load_complete(selected_store)?;
            let outcome = selected_bucket.input_seal_outcome(selected_store);
            if matches!(
                outcome,
                InputPathSealOutcomeV1::FailedMissingQualifiedMarketTerminal { .. }
            ) {
                return Ok(PreparedClosedBucketSessionDriveAttemptV2::FailedInputSeal(
                    outcome,
                ));
            }
            action.stage_closed_bucket_input_seal(outcome);
        }
        let mut terminal_projection_condition_ids = terminal_projection_pending.clone();
        terminal_projection_condition_ids.extend(newly_redeemed_condition_ids.iter().cloned());
        let mark_business_input_started = matches!(
            caller_role,
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation
        ) && !market_enrollment.business_input_started;
        Ok(PreparedClosedBucketSessionDriveAttemptV2::Applied(
            PreparedClosedBucketSessionDriveV2 {
                action,
                execution,
                hidden_platform_markets,
                public_markets,
                selected_path: reference.path,
                selected_bucket,
                hidden_terminal_deltas,
                public_terminal_deltas,
                terminal_projection_pending,
                market_enrollment,
                mark_business_input_started,
                newly_redeemed_condition_ids,
                terminal_projection_condition_ids,
                dpt_effects,
                actual_cutoff_utc_ms: reference.bucket_at_utc_ms,
                first_due_at_utc_ms,
            },
        ))
    }

    pub(crate) fn prepare_market_only_closed_bucket_session_drive_v2(
        &mut self,
        bucket: &ClosedMarketEventBucketV1,
        failed_input_seal: Option<InputPathSealOutcomeV1>,
    ) -> Result<PreparedMarketOnlyClosedBucketSessionDriveV2<'_>, MarketEventErrorV1> {
        let reference = bucket.durable_reference();
        let caller_role = self.session_spec().caller_role;
        let drives_execution = bucket.drives_execution_for(caller_role);
        if drives_execution != failed_input_seal.is_some() {
            return Err(SessionError::ExecutionState(
                "市场专用闭桶准备的执行角色与失败封口模式不一致".to_owned(),
            )
            .into());
        }
        if reference.path == MarketEventPathV1::HiddenPlatform
            && caller_role == SimCallerRoleV1::Paper
        {
            return Err(SessionError::HiddenPlatformReplayForbidden.into());
        }
        if self.failed_input_seal().is_some() || self.input_path_seal(reference.path).is_some() {
            return Err(MarketEventErrorV1::InputPathAlreadySealed);
        }
        if self.last_applied_bucket_coordinates_match(bucket) {
            return Err(MarketEventErrorV1::BucketIdentityConflict);
        }

        let SimSession {
            hidden_platform_markets,
            public_markets,
            execution_state,
            terminal_projection_pending,
            market_enrollment,
            state,
            ..
        } = self;
        let directory = &market_enrollment.directory;
        let mut execution =
            PreparedExecutionDriveDeltaV2::prepare_without_scheduled_progress_v2(execution_state);
        if failed_input_seal.is_some() {
            execution = execution
                .prepare_authorized_market_bucket_start_v2(directory, reference.bucket_at_utc_ms)?;
        }
        let hidden_platform_markets = hidden_platform_markets.as_mut();
        let selected_store = match reference.path {
            MarketEventPathV1::HiddenPlatform => hidden_platform_markets
                .as_deref()
                .expect("隐藏市场专用闭桶必须持有隐藏市场状态"),
            MarketEventPathV1::PublicVisibility => &*public_markets,
        };
        let mut selected_bucket = selected_store.begin_bucket_delta_v2(bucket)?;
        let mut hidden_terminal_deltas = Vec::new();
        let mut public_terminal_deltas = Vec::new();
        let mut newly_terminal_condition_ids = BTreeSet::new();
        let selected_bucket = loop {
            match selected_bucket.advance(selected_store)? {
                PreparedMarketStoreBucketStepV2::Authorized(staged_event) => {
                    let condition_id = staged_event.event().market.condition_id.0.clone();
                    let qualified_resolution = staged_event.qualified_resolution();
                    selected_bucket = staged_event.complete();
                    if failed_input_seal.is_none()
                        && qualified_resolution
                        && let Some(resolution) = execution
                            .candidate_redeemed_market_resolution(&condition_id)
                            .cloned()
                    {
                        let (next, changed) = selected_bucket.stage_redeemed_resolution_terminal(
                            selected_store,
                            &condition_id,
                            &resolution,
                        )?;
                        selected_bucket = next;
                        if changed && newly_terminal_condition_ids.insert(condition_id.clone()) {
                            let (hidden_delta, public_delta) =
                                prepare_other_redeemed_resolution_terminal_path_v2(
                                    reference.path,
                                    hidden_platform_markets.as_deref(),
                                    public_markets,
                                    &condition_id,
                                    &resolution,
                                )?;
                            if let Some(delta) = hidden_delta {
                                hidden_terminal_deltas.push(delta);
                            }
                            if let Some(delta) = public_delta {
                                public_terminal_deltas.push(delta);
                            }
                        }
                    }
                }
                PreparedMarketStoreBucketStepV2::EvictedNoExecution(next) => {
                    selected_bucket = next;
                }
                PreparedMarketStoreBucketStepV2::Complete(prepared) => break prepared,
            }
        };

        let input_seal_after = if reference.closure == MillisecondBucketClosureV1::EndOfInput {
            selected_bucket.validate_initial_load_complete(selected_store)?;
            let outcome = selected_bucket.input_seal_outcome(selected_store);
            if failed_input_seal
                .as_ref()
                .is_some_and(|expected| expected != &outcome)
            {
                return Err(SessionError::ExecutionState(
                    "执行闭桶失败封口与市场专用重备结果不一致".to_owned(),
                )
                .into());
            }
            Some(outcome)
        } else {
            if failed_input_seal.is_some() {
                return Err(SessionError::ExecutionState(
                    "非输入终止闭桶不得准备失败封口事务".to_owned(),
                )
                .into());
            }
            None
        };
        let metadata = state.prepare_closed_bucket_metadata_delta_v2(
            reference.path,
            reference.bucket_at_utc_ms,
            reference.next_index_position,
            input_seal_after,
        )?;
        let mut terminal_projection_condition_ids = terminal_projection_pending.clone();
        terminal_projection_condition_ids.extend(newly_terminal_condition_ids.iter().cloned());
        let mark_business_input_started = matches!(
            caller_role,
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation
        ) && !market_enrollment.business_input_started;
        Ok(PreparedMarketOnlyClosedBucketSessionDriveV2 {
            metadata,
            execution,
            hidden_platform_markets,
            public_markets,
            selected_path: reference.path,
            selected_bucket,
            hidden_terminal_deltas,
            public_terminal_deltas,
            terminal_projection_pending,
            market_enrollment,
            mark_business_input_started,
            newly_terminal_condition_ids,
            terminal_projection_condition_ids,
        })
    }

    pub(crate) fn prepare_closed_bucket_closure_upgrade_session_v2(
        &mut self,
        bucket: &ClosedMarketEventBucketV1,
    ) -> Result<PreparedClosedBucketClosureUpgradeSessionV2<'_>, MarketEventErrorV1> {
        let reference = bucket.durable_reference();
        let caller_role = self.session_spec().caller_role;
        if reference.path == MarketEventPathV1::HiddenPlatform
            && caller_role == SimCallerRoleV1::Paper
        {
            return Err(SessionError::HiddenPlatformReplayForbidden.into());
        }
        if self.last_applied_bucket_identity_matches(bucket)
            || !self.last_applied_bucket_coordinates_match(bucket)
        {
            return Err(MarketEventErrorV1::BucketIdentityConflict);
        }
        let drives_execution = bucket.drives_execution_for(caller_role);
        let SimSession {
            hidden_platform_markets,
            public_markets,
            execution_state,
            terminal_projection_pending,
            state,
            ..
        } = self;
        let execution =
            PreparedExecutionDriveDeltaV2::prepare_without_scheduled_progress_v2(execution_state);
        let hidden_platform_markets = hidden_platform_markets.as_mut();
        let selected_store = match reference.path {
            MarketEventPathV1::HiddenPlatform => hidden_platform_markets
                .as_deref()
                .expect("隐藏闭桶封口升级必须持有隐藏市场状态"),
            MarketEventPathV1::PublicVisibility => &*public_markets,
        };
        let (selected_upgrade, outcome) = selected_store.prepare_closure_upgrade_v2(bucket)?;
        let metadata = state.prepare_closed_bucket_seal_upgrade_delta_v2(reference.path, outcome);
        let terminal_projection_condition_ids = if drives_execution {
            BTreeSet::new()
        } else {
            terminal_projection_pending.clone()
        };
        Ok(PreparedClosedBucketClosureUpgradeSessionV2 {
            metadata,
            execution,
            hidden_platform_markets,
            public_markets,
            selected_path: reference.path,
            selected_upgrade,
            terminal_projection_pending,
            terminal_projection_condition_ids,
        })
    }

    pub(crate) fn take_next_action_through(
        &mut self,
        effective_processing_at_utc_ms: i64,
    ) -> Result<Option<QueuedLogicalActionV1>, SessionError> {
        self.state
            .take_next_action_through(effective_processing_at_utc_ms)
    }

    pub(crate) fn validate_in_flight_action(
        &self,
        action: &QueuedLogicalActionV1,
    ) -> Result<(), SessionError> {
        self.state.validate_in_flight_action(action)
    }

    pub(crate) fn advance_platform_state_revision(
        &mut self,
        next_revision: u64,
    ) -> Result<(), SessionError> {
        self.state.advance_platform_state_revision(next_revision)
    }

    pub(crate) fn platform_state_revision(&self) -> u64 {
        self.state.platform_state_revision
    }

    pub(crate) fn advance_platform_state_revision_once(&mut self) -> Result<u64, SessionError> {
        let next = self.prepare_next_platform_state_revision()?;
        self.state.advance_platform_state_revision(next)?;
        Ok(next)
    }

    pub(crate) fn prepare_next_platform_state_revision(&self) -> Result<u64, SessionError> {
        self.state
            .platform_state_revision
            .checked_add(1)
            .ok_or(SessionError::PlatformRevisionRegression)
    }

    pub(crate) fn commit_prepared_platform_state_revision(&mut self, next_revision: u64) {
        debug_assert_eq!(
            self.state.platform_state_revision.checked_add(1),
            Some(next_revision)
        );
        self.state.platform_state_revision = next_revision;
    }

    pub(crate) fn resolve_mutation_idempotency(
        &mut self,
        global_request_id: &str,
        action_identity: &str,
        idempotency_key: String,
        payload_sha256: Sha256Hex,
        effect_identity: String,
    ) -> Result<MutationResolutionV1, SessionError> {
        self.state.resolve_mutation_idempotency(
            global_request_id,
            action_identity,
            idempotency_key,
            payload_sha256,
            effect_identity,
        )
    }

    pub(crate) fn resolve_query_revision(
        &mut self,
        global_request_id: &str,
        action_identity: &str,
        query_sha256: Sha256Hex,
    ) -> Result<QueryRevisionV1, SessionError> {
        self.state
            .resolve_query_revision(global_request_id, action_identity, query_sha256, None)
    }

    pub(crate) fn resolve_query_revision_at(
        &mut self,
        global_request_id: &str,
        action_identity: &str,
        query_sha256: Sha256Hex,
        frozen_revision: u64,
    ) -> Result<QueryRevisionV1, SessionError> {
        self.state.resolve_query_revision(
            global_request_id,
            action_identity,
            query_sha256,
            Some(frozen_revision),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimProcessRootErrorV1 {
    InvalidRunId,
    FinalitySelectorRunMismatch { override_index: usize },
    InvalidInitialMarketDirectory(String),
}

impl fmt::Display for SimProcessRootErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRunId => write!(
                formatter,
                "运行根 run_id 不能为空、超过 256 字节或带首尾空白"
            ),
            Self::FinalitySelectorRunMismatch { override_index } => write!(
                formatter,
                "第 {override_index} 个成交最终性覆盖不属于本运行根"
            ),
            Self::InvalidInitialMarketDirectory(message) => {
                write!(formatter, "初始会话市场目录非法：{message}")
            }
        }
    }
}

impl std::error::Error for SimProcessRootErrorV1 {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SessionCheckpointErrorV1 {
    UnsupportedSchema(String),
    DependencyIdentityConflict(&'static str),
    CreationIdentityConflict(String),
    SessionNotRegistered(String),
    InvalidInitialization(&'static str),
    InvalidInitialAccountState,
    InvalidMarketEnrollment(String),
    InvalidSessionState(String),
    InvalidExecutionState(String),
    ProcessRoot(SimProcessRootErrorV1),
    Session(SessionError),
}

impl fmt::Display for SessionCheckpointErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchema(schema) => {
                write!(formatter, "不支持的会话检查点版本：{schema}")
            }
            Self::DependencyIdentityConflict(dependency) => {
                write!(formatter, "恢复所载入的{dependency}身份与检查点不匹配")
            }
            Self::CreationIdentityConflict(session_id) => {
                write!(formatter, "恢复的会话创建身份不连续：{session_id}")
            }
            Self::SessionNotRegistered(session_id) => {
                write!(formatter, "恢复的会话不属于当前运行根：{session_id}")
            }
            Self::InvalidInitialization(message) => {
                write!(formatter, "恢复的会话初始化状态非法：{message}")
            }
            Self::InvalidInitialAccountState => {
                write!(formatter, "恢复的初始账户状态与其既有身份不匹配")
            }
            Self::InvalidMarketEnrollment(message) => {
                write!(formatter, "恢复的市场登记链非法：{message}")
            }
            Self::InvalidSessionState(message) => {
                write!(formatter, "恢复的会话状态非法：{message}")
            }
            Self::InvalidExecutionState(message) => {
                write!(formatter, "恢复的执行状态非法：{message}")
            }
            Self::ProcessRoot(error) => write!(formatter, "恢复运行根失败：{error}"),
            Self::Session(error) => write!(formatter, "恢复会话失败：{error}"),
        }
    }
}

impl std::error::Error for SessionCheckpointErrorV1 {}

impl From<SimProcessRootErrorV1> for SessionCheckpointErrorV1 {
    fn from(value: SimProcessRootErrorV1) -> Self {
        Self::ProcessRoot(value)
    }
}

impl From<SessionError> for SessionCheckpointErrorV1 {
    fn from(value: SessionError) -> Self {
        Self::Session(value)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformPrefixCursorV1 {
    closed_through_source_event_at_utc_ms: Option<i64>,
    next_event_time_index_position: u64,
}

impl PlatformPrefixCursorV1 {
    pub fn closed_through_source_event_at_utc_ms(&self) -> Option<i64> {
        self.closed_through_source_event_at_utc_ms
    }

    pub fn next_event_time_index_position(&self) -> u64 {
        self.next_event_time_index_position
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicVisibilityCursorV1 {
    closed_through_received_at_utc_ms: Option<i64>,
    next_visibility_index_position: u64,
}

impl PublicVisibilityCursorV1 {
    pub fn closed_through_received_at_utc_ms(&self) -> Option<i64> {
        self.closed_through_received_at_utc_ms
    }

    pub fn next_visibility_index_position(&self) -> u64 {
        self.next_visibility_index_position
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelVisibleCursorV1 {
    pub session_id: String,
    pub public_visibility: PublicVisibilityCursorV1,
    pub private_feedback_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestContextV1 {
    pub execution_contract_version: String,
    pub session_id: String,
    pub account_id: String,
    pub caller_role: SimCallerRoleV1,
    pub caller_instance_id: String,
    pub global_request_id: String,
    pub caller_correlation_id: Option<String>,
    pub caller_sequence: u64,
    pub request_sent_at_utc_ms: i64,
    pub model_visible_cursor: ModelVisibleCursorV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestTransitV1 {
    pub request_sent_at_utc_ms: i64,
    pub outbound_delay_ms: u64,
    pub platform_arrived_at_utc_ms: i64,
    pub platform_responded_at_utc_ms: Option<i64>,
    pub private_emission_sequence: Option<u64>,
    pub inbound_delay_ms: u64,
    pub response_received_at_utc_ms: Option<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestRegistrationDispositionV1 {
    NewLogicalRequest,
    PhysicalRetry,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredRequestV1 {
    pub disposition: RequestRegistrationDispositionV1,
    pub transit: RequestTransitV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleFeedbackTimingV1 {
    pub feedback_id: String,
    pub platform_event_at_utc_ms: i64,
    pub private_emission_sequence: u64,
    pub inbound_delay_ms: u64,
    pub feedback_received_at_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PrivateVisibilityEvidenceV1 {
    DirectResponse { global_request_id: String },
    LifecycleFeedback { feedback_id: String },
}

impl PrivateVisibilityEvidenceV1 {
    fn identity(&self) -> String {
        match self {
            Self::DirectResponse { global_request_id } => {
                format!("DIRECT_RESPONSE:{global_request_id}")
            }
            Self::LifecycleFeedback { feedback_id } => {
                format!("LIFECYCLE_FEEDBACK:{feedback_id}")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VisiblePrivateFactV1 {
    pub evidence: PrivateVisibilityEvidenceV1,
    pub received_at_utc_ms: i64,
    pub private_emission_sequence: u64,
    pub private_feedback_sequence: u64,
}

pub(crate) struct PreparedPrivateFactSliceV2 {
    session_id: String,
    schedule_closed_through_before_utc_ms: Option<i64>,
    close_through_utc_ms: i64,
    schedule_advanced: bool,
    candidate: Option<VisiblePrivateFactV1>,
}

impl PreparedPrivateFactSliceV2 {
    pub(crate) fn close_through_utc_ms(&self) -> i64 {
        self.close_through_utc_ms
    }

    pub(crate) fn schedule_advanced(&self) -> bool {
        self.schedule_advanced
    }

    pub(crate) fn candidate(&self) -> Option<&VisiblePrivateFactV1> {
        self.candidate.as_ref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrivateFactOrderKey {
    received_at_utc_ms: i64,
    private_emission_sequence: u64,
    identity: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalActionKeyV1 {
    pub effective_processing_at_utc_ms: i64,
    pub caller_sequence: u64,
    pub batch_item_ordinal: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ParsedMethodTimingV1 {
    Ordinary,
    FokFakMarketBuy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParsedLogicalActionV1 {
    pub action_identity: String,
    pub batch_item_ordinal: u16,
    pub method_timing: ParsedMethodTimingV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LogicalActionPlanV1 {
    action_identity: String,
    batch_item_ordinal: u16,
    processing_delay_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueuedLogicalActionV1 {
    pub action_identity: String,
    pub global_request_id: String,
    pub key: LogicalActionKeyV1,
}

struct PreparedPrivateFactRegistrationV2 {
    key: PrivateFactOrderKey,
    evidence: PrivateVisibilityEvidenceV1,
}

struct PreparedDirectResponseCompletionV2 {
    global_request_id: String,
    response_transit: RequestTransitV1,
}

struct PreparedClosedBucketStateV2 {
    path: MarketEventPathV1,
    bucket_at_utc_ms: i64,
    next_index_position: u64,
    input_seal_after: Option<InputPathSealOutcomeV1>,
}

pub(crate) struct PreparedExecutionActionSliceV2<'a> {
    state: &'a mut SessionState,
    latency_profile: &'a EmpiricalDelayProfile,
    closed_action_through_before_utc_ms: Option<i64>,
    closed_action_through_after_utc_ms: i64,
    actions: Vec<QueuedLogicalActionV1>,
    next_action_index: usize,
    completed_request_id: Option<String>,
    prepared_request_pending_action_count: Option<(String, u64)>,
    platform_state_revision_before: u64,
    platform_state_revision_after: u64,
    prepared_mutations: Vec<(String, MutationRecord)>,
    prepared_queries: Vec<(String, QueryRecord)>,
    prepared_lifecycle_feedback: Vec<(String, LifecycleFeedbackTimingV1)>,
    prepared_private_facts: Vec<PreparedPrivateFactRegistrationV2>,
    prepared_used_private_emission_sequences: Vec<u64>,
    prepared_direct_response: Option<PreparedDirectResponseCompletionV2>,
    prepared_closed_bucket: Option<PreparedClosedBucketStateV2>,
    preparation_failed: bool,
}

pub(crate) struct CommittableExecutionActionSliceV2<'a> {
    prepared: PreparedExecutionActionSliceV2<'a>,
}

struct PreparedClosedBucketMetadataDeltaV2<'a> {
    state: &'a mut SessionState,
    path: MarketEventPathV1,
    cursor_after: Option<(i64, u64)>,
    input_seal_after: Option<InputPathSealOutcomeV1>,
}

struct CommittableClosedBucketMetadataDeltaV2<'a> {
    prepared: PreparedClosedBucketMetadataDeltaV2<'a>,
}

pub(crate) struct PreparedScheduledSessionDriveV2<'a> {
    action: PreparedExecutionActionSliceV2<'a>,
    execution: PreparedExecutionDriveDeltaV2<'a>,
    hidden_platform_markets: Option<&'a mut MarketStateStoreV1>,
    public_markets: &'a mut MarketStateStoreV1,
    hidden_terminal_deltas: Vec<PreparedMarketResolutionTerminalDeltaV2>,
    public_terminal_deltas: Vec<PreparedMarketResolutionTerminalDeltaV2>,
    terminal_projection_pending: &'a mut BTreeSet<String>,
    newly_redeemed_condition_ids: BTreeSet<String>,
    terminal_projection_condition_ids: BTreeSet<String>,
    actual_cutoff_utc_ms: i64,
    first_due_at_utc_ms: Option<i64>,
}

pub(crate) struct CommittableScheduledSessionDriveV2<'a> {
    action: CommittableExecutionActionSliceV2<'a>,
    execution: CommittableExecutionDriveDeltaV2<'a>,
    hidden_platform_markets: Option<&'a mut MarketStateStoreV1>,
    public_markets: &'a mut MarketStateStoreV1,
    hidden_terminal_deltas: Vec<PreparedMarketResolutionTerminalDeltaV2>,
    public_terminal_deltas: Vec<PreparedMarketResolutionTerminalDeltaV2>,
    terminal_projection_pending: &'a mut BTreeSet<String>,
    newly_redeemed_condition_ids: BTreeSet<String>,
    terminal_projection_condition_ids: BTreeSet<String>,
}

pub(crate) enum PreparedClosedBucketSessionDriveAttemptV2<'a> {
    Applied(PreparedClosedBucketSessionDriveV2<'a>),
    FailedInputSeal(InputPathSealOutcomeV1),
}

pub(crate) struct PreparedClosedBucketSessionDriveV2<'a> {
    action: PreparedExecutionActionSliceV2<'a>,
    execution: PreparedExecutionDriveDeltaV2<'a>,
    hidden_platform_markets: Option<&'a mut MarketStateStoreV1>,
    public_markets: &'a mut MarketStateStoreV1,
    selected_path: MarketEventPathV1,
    selected_bucket: PreparedMarketStoreBucketDeltaV2,
    hidden_terminal_deltas: Vec<PreparedMarketResolutionTerminalDeltaV2>,
    public_terminal_deltas: Vec<PreparedMarketResolutionTerminalDeltaV2>,
    terminal_projection_pending: &'a mut BTreeSet<String>,
    market_enrollment: &'a mut Arc<MarketEnrollmentStateV1>,
    mark_business_input_started: bool,
    newly_redeemed_condition_ids: BTreeSet<String>,
    terminal_projection_condition_ids: BTreeSet<String>,
    dpt_effects: AuthorizedMarketEventEffectsV1,
    actual_cutoff_utc_ms: i64,
    first_due_at_utc_ms: Option<i64>,
}

pub(crate) struct CommittableClosedBucketSessionDriveV2<'a> {
    action: CommittableExecutionActionSliceV2<'a>,
    execution: CommittableExecutionDriveDeltaV2<'a>,
    hidden_platform_markets: Option<&'a mut MarketStateStoreV1>,
    public_markets: &'a mut MarketStateStoreV1,
    selected_path: MarketEventPathV1,
    selected_bucket: PreparedMarketStoreBucketDeltaV2,
    hidden_terminal_deltas: Vec<PreparedMarketResolutionTerminalDeltaV2>,
    public_terminal_deltas: Vec<PreparedMarketResolutionTerminalDeltaV2>,
    terminal_projection_pending: &'a mut BTreeSet<String>,
    market_enrollment: &'a mut Arc<MarketEnrollmentStateV1>,
    mark_business_input_started: bool,
    newly_redeemed_condition_ids: BTreeSet<String>,
    terminal_projection_condition_ids: BTreeSet<String>,
}

pub(crate) struct PreparedMarketOnlyClosedBucketSessionDriveV2<'a> {
    metadata: PreparedClosedBucketMetadataDeltaV2<'a>,
    execution: PreparedExecutionDriveDeltaV2<'a>,
    hidden_platform_markets: Option<&'a mut MarketStateStoreV1>,
    public_markets: &'a mut MarketStateStoreV1,
    selected_path: MarketEventPathV1,
    selected_bucket: PreparedMarketStoreBucketDeltaV2,
    hidden_terminal_deltas: Vec<PreparedMarketResolutionTerminalDeltaV2>,
    public_terminal_deltas: Vec<PreparedMarketResolutionTerminalDeltaV2>,
    terminal_projection_pending: &'a mut BTreeSet<String>,
    market_enrollment: &'a mut Arc<MarketEnrollmentStateV1>,
    mark_business_input_started: bool,
    newly_terminal_condition_ids: BTreeSet<String>,
    terminal_projection_condition_ids: BTreeSet<String>,
}

pub(crate) struct CommittableMarketOnlyClosedBucketSessionDriveV2<'a> {
    metadata: CommittableClosedBucketMetadataDeltaV2<'a>,
    execution: CommittableExecutionDriveDeltaV2<'a>,
    hidden_platform_markets: Option<&'a mut MarketStateStoreV1>,
    public_markets: &'a mut MarketStateStoreV1,
    selected_path: MarketEventPathV1,
    selected_bucket: PreparedMarketStoreBucketDeltaV2,
    hidden_terminal_deltas: Vec<PreparedMarketResolutionTerminalDeltaV2>,
    public_terminal_deltas: Vec<PreparedMarketResolutionTerminalDeltaV2>,
    terminal_projection_pending: &'a mut BTreeSet<String>,
    market_enrollment: &'a mut Arc<MarketEnrollmentStateV1>,
    mark_business_input_started: bool,
    newly_terminal_condition_ids: BTreeSet<String>,
    terminal_projection_condition_ids: BTreeSet<String>,
}

pub(crate) struct PreparedClosedBucketClosureUpgradeSessionV2<'a> {
    metadata: PreparedClosedBucketMetadataDeltaV2<'a>,
    execution: PreparedExecutionDriveDeltaV2<'a>,
    hidden_platform_markets: Option<&'a mut MarketStateStoreV1>,
    public_markets: &'a mut MarketStateStoreV1,
    selected_path: MarketEventPathV1,
    selected_upgrade: PreparedMarketStoreClosureUpgradeV2,
    terminal_projection_pending: &'a mut BTreeSet<String>,
    terminal_projection_condition_ids: BTreeSet<String>,
}

pub(crate) struct CommittableClosedBucketClosureUpgradeSessionV2<'a> {
    metadata: CommittableClosedBucketMetadataDeltaV2<'a>,
    execution: CommittableExecutionDriveDeltaV2<'a>,
    hidden_platform_markets: Option<&'a mut MarketStateStoreV1>,
    public_markets: &'a mut MarketStateStoreV1,
    selected_path: MarketEventPathV1,
    selected_upgrade: PreparedMarketStoreClosureUpgradeV2,
    terminal_projection_pending: &'a mut BTreeSet<String>,
    terminal_projection_condition_ids: BTreeSet<String>,
}

fn prepare_redeemed_resolution_terminal_paths_v2(
    hidden_platform_markets: Option<&MarketStateStoreV1>,
    public_markets: &MarketStateStoreV1,
    condition_id: &str,
    resolution: &MarketResolutionTerminalV3,
) -> Result<
    (
        Option<PreparedMarketResolutionTerminalDeltaV2>,
        Option<PreparedMarketResolutionTerminalDeltaV2>,
    ),
    MarketEventErrorV1,
> {
    let hidden_delta = hidden_platform_markets
        .map(|store| store.prepare_resolution_terminal_v2(condition_id, resolution))
        .transpose()?
        .flatten();
    let public_delta = public_markets.prepare_resolution_terminal_v2(condition_id, resolution)?;
    if hidden_delta.is_none() && public_delta.is_none() {
        return Err(MarketEventErrorV1::ResolutionConflict(
            condition_id.to_owned(),
        ));
    }
    Ok((hidden_delta, public_delta))
}

fn prepare_other_redeemed_resolution_terminal_path_v2(
    selected_path: MarketEventPathV1,
    hidden_platform_markets: Option<&MarketStateStoreV1>,
    public_markets: &MarketStateStoreV1,
    condition_id: &str,
    resolution: &MarketResolutionTerminalV3,
) -> Result<
    (
        Option<PreparedMarketResolutionTerminalDeltaV2>,
        Option<PreparedMarketResolutionTerminalDeltaV2>,
    ),
    MarketEventErrorV1,
> {
    match selected_path {
        MarketEventPathV1::HiddenPlatform => Ok((
            None,
            public_markets.prepare_resolution_terminal_v2(condition_id, resolution)?,
        )),
        MarketEventPathV1::PublicVisibility => Ok((
            hidden_platform_markets
                .map(|store| store.prepare_resolution_terminal_v2(condition_id, resolution))
                .transpose()?
                .flatten(),
            None,
        )),
    }
}

impl PreparedScheduledSessionDriveV2<'_> {
    pub(crate) fn current_action(&self) -> Option<QueuedLogicalActionV1> {
        self.action.current_action()
    }

    pub(crate) fn request_completed(&self) -> bool {
        self.action.request_completed()
    }

    pub(crate) fn finish_current_action(
        &mut self,
        action_identity: &str,
    ) -> Result<(), SessionError> {
        self.action.finish_current_action(action_identity)
    }

    pub(crate) fn actual_cutoff_utc_ms(&self) -> i64 {
        self.actual_cutoff_utc_ms
    }

    pub(crate) fn first_due_at_utc_ms(&self) -> Option<i64> {
        self.first_due_at_utc_ms
    }

    pub(crate) fn execution_candidate(&self) -> &PreparedExecutionDriveDeltaV2<'_> {
        &self.execution
    }

    pub(crate) fn terminal_projection_condition_ids(&self) -> &BTreeSet<String> {
        &self.terminal_projection_condition_ids
    }

    pub(crate) fn advance_platform_state_revision_once(&mut self) -> Result<u64, SessionError> {
        self.action.advance_platform_state_revision_once()
    }

    pub(crate) fn prepare_query_revision(
        &mut self,
        global_request_id: &str,
        action_identity: &str,
        query_sha256: Sha256Hex,
    ) -> Result<QueryRevisionV1, SessionError> {
        self.action
            .prepare_query_revision(global_request_id, action_identity, query_sha256)
    }

    pub(crate) fn prepare_query_revision_at(
        &mut self,
        global_request_id: &str,
        action_identity: &str,
        query_sha256: Sha256Hex,
        frozen_revision: u64,
    ) -> Result<QueryRevisionV1, SessionError> {
        self.action.prepare_query_revision_at(
            global_request_id,
            action_identity,
            query_sha256,
            frozen_revision,
        )
    }

    pub(crate) fn prepare_mutation_resolution(
        &mut self,
        global_request_id: &str,
        action_identity: &str,
        idempotency_key: String,
        payload_sha256: Sha256Hex,
        effect_identity: String,
    ) -> Result<MutationResolutionV1, SessionError> {
        self.action.prepare_mutation_resolution(
            global_request_id,
            action_identity,
            idempotency_key,
            payload_sha256,
            effect_identity,
        )
    }

    pub(crate) fn query_revision_for_request(&self, request_id: &str) -> Option<u64> {
        self.action.query_revision_for_request(request_id)
    }

    pub(crate) fn next_private_emission_sequence(&mut self) -> Result<u64, SessionError> {
        self.action.next_private_emission_sequence()
    }

    pub(crate) fn prepare_complete_direct_response(
        &mut self,
        global_request_id: &str,
        platform_responded_at_utc_ms: i64,
        private_emission_sequence: u64,
    ) -> Result<RequestTransitV1, SessionError> {
        self.action.prepare_complete_direct_response(
            global_request_id,
            platform_responded_at_utc_ms,
            private_emission_sequence,
        )
    }

    pub(crate) fn prepare_next_lifecycle_feedback_timing(
        &mut self,
        feedback_id: String,
        platform_event_at_utc_ms: i64,
    ) -> Result<LifecycleFeedbackTimingV1, SessionError> {
        let private_emission_sequence = self.action.next_private_emission_sequence()?;
        self.action.prepare_lifecycle_feedback_timing(
            feedback_id,
            platform_event_at_utc_ms,
            private_emission_sequence,
        )
    }
}

impl<'a> PreparedScheduledSessionDriveV2<'a> {
    pub(crate) fn map_execution_candidate<T, E>(
        self,
        prepare: impl FnOnce(
            PreparedExecutionDriveDeltaV2<'a>,
        ) -> Result<(PreparedExecutionDriveDeltaV2<'a>, T), E>,
    ) -> Result<(Self, T), E> {
        let Self {
            action,
            execution,
            hidden_platform_markets,
            public_markets,
            hidden_terminal_deltas,
            public_terminal_deltas,
            terminal_projection_pending,
            newly_redeemed_condition_ids,
            terminal_projection_condition_ids,
            actual_cutoff_utc_ms,
            first_due_at_utc_ms,
        } = self;
        let (execution, value) = prepare(execution)?;
        Ok((
            Self {
                action,
                execution,
                hidden_platform_markets,
                public_markets,
                hidden_terminal_deltas,
                public_terminal_deltas,
                terminal_projection_pending,
                newly_redeemed_condition_ids,
                terminal_projection_condition_ids,
                actual_cutoff_utc_ms,
                first_due_at_utc_ms,
            },
            value,
        ))
    }

    pub(crate) fn prepare_caller_cancel_remainder(
        self,
        platform_order_id: &str,
        canceled_at_utc_ms: i64,
        caller_cancel_effect_identity: &Sha256Hex,
    ) -> Result<(Self, CancelOrderStateDispositionV1), MatchingStateErrorV1> {
        let Self {
            action,
            execution,
            hidden_platform_markets,
            public_markets,
            hidden_terminal_deltas,
            public_terminal_deltas,
            terminal_projection_pending,
            newly_redeemed_condition_ids,
            terminal_projection_condition_ids,
            actual_cutoff_utc_ms,
            first_due_at_utc_ms,
        } = self;
        let (execution, disposition) = execution.prepare_caller_cancel_remainder(
            platform_order_id,
            canceled_at_utc_ms,
            caller_cancel_effect_identity,
        )?;
        Ok((
            Self {
                action,
                execution,
                hidden_platform_markets,
                public_markets,
                hidden_terminal_deltas,
                public_terminal_deltas,
                terminal_projection_pending,
                newly_redeemed_condition_ids,
                terminal_projection_condition_ids,
                actual_cutoff_utc_ms,
                first_due_at_utc_ms,
            },
            disposition,
        ))
    }

    pub(crate) fn into_committable(
        self,
        prepared_condition_ids: BTreeSet<String>,
    ) -> Result<CommittableScheduledSessionDriveV2<'a>, MarketEventErrorV1> {
        if prepared_condition_ids != self.terminal_projection_condition_ids {
            return Err(SessionError::QueryIdentityConflict(
                "scheduled-terminal-projection-condition-set".to_owned(),
            )
            .into());
        }
        let Self {
            action,
            execution,
            hidden_platform_markets,
            public_markets,
            hidden_terminal_deltas,
            public_terminal_deltas,
            terminal_projection_pending,
            newly_redeemed_condition_ids,
            terminal_projection_condition_ids,
            ..
        } = self;
        let action = action.into_committable()?;
        let execution = execution.into_committable()?;
        Ok(CommittableScheduledSessionDriveV2 {
            action,
            execution,
            hidden_platform_markets,
            public_markets,
            hidden_terminal_deltas,
            public_terminal_deltas,
            terminal_projection_pending,
            newly_redeemed_condition_ids,
            terminal_projection_condition_ids,
        })
    }
}

impl CommittableScheduledSessionDriveV2<'_> {
    pub(crate) fn commit(self) {
        let Self {
            action,
            execution,
            mut hidden_platform_markets,
            public_markets,
            hidden_terminal_deltas,
            public_terminal_deltas,
            terminal_projection_pending,
            newly_redeemed_condition_ids,
            terminal_projection_condition_ids,
        } = self;
        execution.commit();
        if let Some(store) = hidden_platform_markets.as_deref_mut() {
            for delta in hidden_terminal_deltas {
                delta.commit(store);
            }
        } else {
            debug_assert!(hidden_terminal_deltas.is_empty());
        }
        for delta in public_terminal_deltas {
            delta.commit(public_markets);
        }
        terminal_projection_pending.extend(newly_redeemed_condition_ids);
        for condition_id in terminal_projection_condition_ids {
            terminal_projection_pending.remove(&condition_id);
        }
        action.commit();
    }
}

impl PreparedClosedBucketSessionDriveV2<'_> {
    pub(crate) fn execution_candidate(&self) -> &PreparedExecutionDriveDeltaV2<'_> {
        &self.execution
    }

    pub(crate) fn terminal_projection_condition_ids(&self) -> &BTreeSet<String> {
        &self.terminal_projection_condition_ids
    }

    pub(crate) fn dpt_effects(&self) -> &AuthorizedMarketEventEffectsV1 {
        &self.dpt_effects
    }

    pub(crate) fn actual_cutoff_utc_ms(&self) -> i64 {
        self.actual_cutoff_utc_ms
    }

    pub(crate) fn first_due_at_utc_ms(&self) -> Option<i64> {
        self.first_due_at_utc_ms
    }

    pub(crate) fn advance_platform_state_revision_once(&mut self) -> Result<u64, SessionError> {
        self.action.advance_platform_state_revision_once()
    }

    pub(crate) fn prepare_next_lifecycle_feedback_timing(
        &mut self,
        feedback_id: String,
        platform_event_at_utc_ms: i64,
    ) -> Result<LifecycleFeedbackTimingV1, SessionError> {
        let private_emission_sequence = self.action.next_private_emission_sequence()?;
        self.action.prepare_lifecycle_feedback_timing(
            feedback_id,
            platform_event_at_utc_ms,
            private_emission_sequence,
        )
    }
}

impl<'a> PreparedClosedBucketSessionDriveV2<'a> {
    pub(crate) fn into_committable(
        self,
        prepared_condition_ids: BTreeSet<String>,
    ) -> Result<CommittableClosedBucketSessionDriveV2<'a>, MarketEventErrorV1> {
        if prepared_condition_ids != self.terminal_projection_condition_ids {
            return Err(SessionError::QueryIdentityConflict(
                "closed-bucket-terminal-projection-condition-set".to_owned(),
            )
            .into());
        }
        let Self {
            action,
            execution,
            hidden_platform_markets,
            public_markets,
            selected_path,
            selected_bucket,
            hidden_terminal_deltas,
            public_terminal_deltas,
            terminal_projection_pending,
            market_enrollment,
            mark_business_input_started,
            newly_redeemed_condition_ids,
            terminal_projection_condition_ids,
            dpt_effects: _,
            actual_cutoff_utc_ms: _,
            first_due_at_utc_ms: _,
        } = self;
        let action = action.into_committable()?;
        let execution = execution.into_committable()?;
        Ok(CommittableClosedBucketSessionDriveV2 {
            action,
            execution,
            hidden_platform_markets,
            public_markets,
            selected_path,
            selected_bucket,
            hidden_terminal_deltas,
            public_terminal_deltas,
            terminal_projection_pending,
            market_enrollment,
            mark_business_input_started,
            newly_redeemed_condition_ids,
            terminal_projection_condition_ids,
        })
    }
}

impl CommittableClosedBucketSessionDriveV2<'_> {
    pub(crate) fn commit(self) {
        let Self {
            action,
            execution,
            mut hidden_platform_markets,
            public_markets,
            selected_path,
            selected_bucket,
            hidden_terminal_deltas,
            public_terminal_deltas,
            terminal_projection_pending,
            market_enrollment,
            mark_business_input_started,
            newly_redeemed_condition_ids,
            terminal_projection_condition_ids,
        } = self;
        execution.commit();
        match selected_path {
            MarketEventPathV1::HiddenPlatform => selected_bucket.commit_prevalidated(
                hidden_platform_markets
                    .as_deref_mut()
                    .expect("隐藏执行闭桶必须持有隐藏市场状态"),
            ),
            MarketEventPathV1::PublicVisibility => {
                selected_bucket.commit_prevalidated(public_markets)
            }
        }
        if let Some(store) = hidden_platform_markets.as_deref_mut() {
            for delta in hidden_terminal_deltas {
                delta.commit(store);
            }
        } else {
            debug_assert!(hidden_terminal_deltas.is_empty());
        }
        for delta in public_terminal_deltas {
            delta.commit(public_markets);
        }
        terminal_projection_pending.extend(newly_redeemed_condition_ids);
        for condition_id in terminal_projection_condition_ids {
            terminal_projection_pending.remove(&condition_id);
        }
        if mark_business_input_started {
            Arc::make_mut(market_enrollment).business_input_started = true;
        }
        action.commit();
    }
}

impl PreparedMarketOnlyClosedBucketSessionDriveV2<'_> {
    pub(crate) fn execution_candidate(&self) -> &PreparedExecutionDriveDeltaV2<'_> {
        &self.execution
    }

    pub(crate) fn terminal_projection_condition_ids(&self) -> &BTreeSet<String> {
        &self.terminal_projection_condition_ids
    }
}

impl<'a> PreparedMarketOnlyClosedBucketSessionDriveV2<'a> {
    pub(crate) fn into_committable(
        self,
        prepared_condition_ids: BTreeSet<String>,
    ) -> Result<CommittableMarketOnlyClosedBucketSessionDriveV2<'a>, MarketEventErrorV1> {
        if prepared_condition_ids != self.terminal_projection_condition_ids {
            return Err(SessionError::QueryIdentityConflict(
                "market-only-closed-bucket-terminal-projection-condition-set".to_owned(),
            )
            .into());
        }
        let Self {
            metadata,
            execution,
            hidden_platform_markets,
            public_markets,
            selected_path,
            selected_bucket,
            hidden_terminal_deltas,
            public_terminal_deltas,
            terminal_projection_pending,
            market_enrollment,
            mark_business_input_started,
            newly_terminal_condition_ids,
            terminal_projection_condition_ids,
        } = self;
        Ok(CommittableMarketOnlyClosedBucketSessionDriveV2 {
            metadata: metadata.into_committable(),
            execution: execution.into_committable()?,
            hidden_platform_markets,
            public_markets,
            selected_path,
            selected_bucket,
            hidden_terminal_deltas,
            public_terminal_deltas,
            terminal_projection_pending,
            market_enrollment,
            mark_business_input_started,
            newly_terminal_condition_ids,
            terminal_projection_condition_ids,
        })
    }
}

impl CommittableMarketOnlyClosedBucketSessionDriveV2<'_> {
    pub(crate) fn commit(self) {
        let Self {
            metadata,
            execution,
            mut hidden_platform_markets,
            public_markets,
            selected_path,
            selected_bucket,
            hidden_terminal_deltas,
            public_terminal_deltas,
            terminal_projection_pending,
            market_enrollment,
            mark_business_input_started,
            newly_terminal_condition_ids,
            terminal_projection_condition_ids,
        } = self;
        execution.commit();
        match selected_path {
            MarketEventPathV1::HiddenPlatform => selected_bucket.commit_prevalidated(
                hidden_platform_markets
                    .as_deref_mut()
                    .expect("隐藏市场专用闭桶必须持有隐藏市场状态"),
            ),
            MarketEventPathV1::PublicVisibility => {
                selected_bucket.commit_prevalidated(public_markets)
            }
        }
        if let Some(store) = hidden_platform_markets.as_deref_mut() {
            for delta in hidden_terminal_deltas {
                delta.commit(store);
            }
        } else {
            debug_assert!(hidden_terminal_deltas.is_empty());
        }
        for delta in public_terminal_deltas {
            delta.commit(public_markets);
        }
        terminal_projection_pending.extend(newly_terminal_condition_ids);
        for condition_id in terminal_projection_condition_ids {
            terminal_projection_pending.remove(&condition_id);
        }
        if mark_business_input_started {
            Arc::make_mut(market_enrollment).business_input_started = true;
        }
        metadata.commit();
    }
}

impl PreparedClosedBucketClosureUpgradeSessionV2<'_> {
    pub(crate) fn execution_candidate(&self) -> &PreparedExecutionDriveDeltaV2<'_> {
        &self.execution
    }

    pub(crate) fn terminal_projection_condition_ids(&self) -> &BTreeSet<String> {
        &self.terminal_projection_condition_ids
    }
}

impl<'a> PreparedClosedBucketClosureUpgradeSessionV2<'a> {
    pub(crate) fn into_committable(
        self,
        prepared_condition_ids: BTreeSet<String>,
    ) -> Result<CommittableClosedBucketClosureUpgradeSessionV2<'a>, MarketEventErrorV1> {
        if prepared_condition_ids != self.terminal_projection_condition_ids {
            return Err(SessionError::QueryIdentityConflict(
                "closed-bucket-upgrade-terminal-projection-condition-set".to_owned(),
            )
            .into());
        }
        let Self {
            metadata,
            execution,
            hidden_platform_markets,
            public_markets,
            selected_path,
            selected_upgrade,
            terminal_projection_pending,
            terminal_projection_condition_ids,
        } = self;
        Ok(CommittableClosedBucketClosureUpgradeSessionV2 {
            metadata: metadata.into_committable(),
            execution: execution.into_committable()?,
            hidden_platform_markets,
            public_markets,
            selected_path,
            selected_upgrade,
            terminal_projection_pending,
            terminal_projection_condition_ids,
        })
    }
}

impl CommittableClosedBucketClosureUpgradeSessionV2<'_> {
    pub(crate) fn commit(self) {
        let Self {
            metadata,
            execution,
            mut hidden_platform_markets,
            public_markets,
            selected_path,
            selected_upgrade,
            terminal_projection_pending,
            terminal_projection_condition_ids,
        } = self;
        execution.commit();
        match selected_path {
            MarketEventPathV1::HiddenPlatform => selected_upgrade.commit_prevalidated(
                hidden_platform_markets
                    .as_deref_mut()
                    .expect("隐藏闭桶封口升级必须持有隐藏市场状态"),
            ),
            MarketEventPathV1::PublicVisibility => {
                selected_upgrade.commit_prevalidated(public_markets)
            }
        }
        for condition_id in terminal_projection_condition_ids {
            terminal_projection_pending.remove(&condition_id);
        }
        metadata.commit();
    }
}

impl<'a> PreparedClosedBucketMetadataDeltaV2<'a> {
    fn into_committable(self) -> CommittableClosedBucketMetadataDeltaV2<'a> {
        CommittableClosedBucketMetadataDeltaV2 { prepared: self }
    }
}

impl CommittableClosedBucketMetadataDeltaV2<'_> {
    fn commit(self) {
        let PreparedClosedBucketMetadataDeltaV2 {
            state,
            path,
            cursor_after,
            input_seal_after,
        } = self.prepared;
        if let Some((bucket_at_utc_ms, next_index_position)) = cursor_after {
            match path {
                MarketEventPathV1::HiddenPlatform => {
                    state.commit_platform_prefix_closure(bucket_at_utc_ms, next_index_position)
                }
                MarketEventPathV1::PublicVisibility => {
                    state.commit_public_visibility_closure(bucket_at_utc_ms, next_index_position)
                }
            }
        }
        if let Some(outcome) = input_seal_after {
            state.commit_input_path_seal(path, outcome);
        }
    }
}

impl CommittableExecutionActionSliceV2<'_> {
    pub(crate) fn commit(self) {
        self.prepared.commit_prevalidated();
    }
}

impl<'a> PreparedExecutionActionSliceV2<'a> {
    fn preparation_failed_error() -> SessionError {
        SessionError::ExecutionState("执行动作准备已经失败，必须丢弃当前准备结果".to_owned())
    }

    fn require_preparation_open(&self) -> Result<(), SessionError> {
        if self.preparation_failed {
            Err(Self::preparation_failed_error())
        } else {
            Ok(())
        }
    }

    fn record_preparation_failure(&mut self, error: SessionError) -> SessionError {
        self.preparation_failed = true;
        error
    }

    pub(crate) fn current_action(&self) -> Option<QueuedLogicalActionV1> {
        self.actions.get(self.next_action_index).cloned()
    }

    pub(crate) fn request_completed(&self) -> bool {
        self.completed_request_id.is_some() && self.next_action_index == self.actions.len()
    }

    pub(crate) fn logical_platform_driven_through_utc_ms(&self) -> i64 {
        self.closed_action_through_after_utc_ms
    }

    pub(crate) fn has_state_change(&self) -> bool {
        !self.actions.is_empty()
            || self.closed_action_through_before_utc_ms
                != Some(self.closed_action_through_after_utc_ms)
            || self.platform_state_revision_before != self.platform_state_revision_after
            || !self.prepared_mutations.is_empty()
            || !self.prepared_queries.is_empty()
            || !self.prepared_lifecycle_feedback.is_empty()
            || !self.prepared_private_facts.is_empty()
            || self.prepared_direct_response.is_some()
            || self.prepared_closed_bucket.is_some()
    }

    fn stage_closed_bucket_input_seal(&mut self, outcome: InputPathSealOutcomeV1) {
        let prepared_bucket = self
            .prepared_closed_bucket
            .as_mut()
            .expect("只有闭桶无动作切片可以准备输入封口");
        debug_assert!(prepared_bucket.input_seal_after.is_none());
        prepared_bucket.input_seal_after = Some(outcome);
    }

    pub(crate) fn platform_state_revision(&self) -> u64 {
        self.platform_state_revision_after
    }

    pub(crate) fn advance_platform_state_revision_once(&mut self) -> Result<u64, SessionError> {
        self.require_preparation_open()?;
        let Some(next) = self.platform_state_revision_after.checked_add(1) else {
            return Err(self.record_preparation_failure(SessionError::PlatformRevisionRegression));
        };
        self.platform_state_revision_after = next;
        Ok(next)
    }

    pub(crate) fn finish_current_action(
        &mut self,
        action_identity: &str,
    ) -> Result<(), SessionError> {
        self.require_preparation_open()?;
        let Some(current) = self.actions.get(self.next_action_index) else {
            return Err(
                self.record_preparation_failure(SessionError::ActionNotLogicallyReleased(
                    action_identity.to_owned(),
                )),
            );
        };
        if current.action_identity != action_identity {
            return Err(
                self.record_preparation_failure(SessionError::ActionNotLogicallyReleased(
                    action_identity.to_owned(),
                )),
            );
        }
        self.next_action_index += 1;
        Ok(())
    }

    pub(crate) fn into_committable(
        self,
    ) -> Result<CommittableExecutionActionSliceV2<'a>, SessionError> {
        if self.preparation_failed {
            return Err(Self::preparation_failed_error());
        }
        if let Some(unfinished) = self.actions.get(self.next_action_index) {
            return Err(SessionError::ActionNotLogicallyReleased(
                unfinished.action_identity.clone(),
            ));
        }
        Ok(CommittableExecutionActionSliceV2 { prepared: self })
    }

    pub(crate) fn prepare_mutation_resolution(
        &mut self,
        global_request_id: &str,
        action_identity: &str,
        idempotency_key: String,
        payload_sha256: Sha256Hex,
        effect_identity: String,
    ) -> Result<MutationResolutionV1, SessionError> {
        self.require_preparation_open()?;
        let result = self.prepare_mutation_resolution_inner(
            global_request_id,
            action_identity,
            idempotency_key,
            payload_sha256,
            effect_identity,
        );
        if result.is_err() {
            self.preparation_failed = true;
        }
        result
    }

    fn prepare_mutation_resolution_inner(
        &mut self,
        global_request_id: &str,
        action_identity: &str,
        idempotency_key: String,
        payload_sha256: Sha256Hex,
        effect_identity: String,
    ) -> Result<MutationResolutionV1, SessionError> {
        self.validate_current_action(global_request_id, action_identity)?;
        if idempotency_key.is_empty()
            || effect_identity.is_empty()
            || !is_sha256_hex(&payload_sha256.0)
        {
            return Err(SessionError::InvalidRequest("变更幂等绑定非法"));
        }
        let existing = self
            .prepared_mutations
            .iter()
            .find(|(identity, _)| identity == &idempotency_key)
            .map(|(_, record)| record)
            .or_else(|| self.state.mutations.get(&idempotency_key));
        if let Some(existing) = existing {
            if existing.payload_sha256 != payload_sha256 {
                return Err(SessionError::MutationIdentityConflict(idempotency_key));
            }
            return Ok(MutationResolutionV1 {
                disposition: MutationResolutionDispositionV1::ExistingEffect,
                effect_identity: existing.effect_identity.clone(),
            });
        }
        self.prepared_mutations.push((
            idempotency_key,
            MutationRecord {
                payload_sha256,
                effect_identity: effect_identity.clone(),
            },
        ));
        Ok(MutationResolutionV1 {
            disposition: MutationResolutionDispositionV1::NewEffect,
            effect_identity,
        })
    }

    pub(crate) fn prepare_query_revision(
        &mut self,
        global_request_id: &str,
        action_identity: &str,
        query_sha256: Sha256Hex,
    ) -> Result<QueryRevisionV1, SessionError> {
        self.prepare_query_revision_checked(global_request_id, action_identity, query_sha256, None)
    }

    pub(crate) fn prepare_query_revision_at(
        &mut self,
        global_request_id: &str,
        action_identity: &str,
        query_sha256: Sha256Hex,
        frozen_revision: u64,
    ) -> Result<QueryRevisionV1, SessionError> {
        self.prepare_query_revision_checked(
            global_request_id,
            action_identity,
            query_sha256,
            Some(frozen_revision),
        )
    }

    fn prepare_query_revision_checked(
        &mut self,
        global_request_id: &str,
        action_identity: &str,
        query_sha256: Sha256Hex,
        frozen_revision: Option<u64>,
    ) -> Result<QueryRevisionV1, SessionError> {
        self.require_preparation_open()?;
        let result = self.prepare_query_revision_unchecked(
            global_request_id,
            action_identity,
            query_sha256,
            frozen_revision,
        );
        if result.is_err() {
            self.preparation_failed = true;
        }
        result
    }

    pub(crate) fn query_revision_for_request(&self, request_id: &str) -> Option<u64> {
        self.prepared_queries
            .iter()
            .find(|(prepared_request_id, _)| prepared_request_id == request_id)
            .map(|(_, record)| record.platform_state_revision)
            .or_else(|| {
                self.state
                    .queries
                    .get(request_id)
                    .map(|record| record.platform_state_revision)
            })
    }

    pub(crate) fn next_private_emission_sequence(&mut self) -> Result<u64, SessionError> {
        self.require_preparation_open()?;
        let result = self.next_private_emission_sequence_unchecked();
        if result.is_err() {
            self.preparation_failed = true;
        }
        result
    }

    fn next_private_emission_sequence_unchecked(&self) -> Result<u64, SessionError> {
        self.prepared_used_private_emission_sequences
            .iter()
            .copied()
            .chain(self.state.used_private_emission_sequences.last().copied())
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(SessionError::CursorRegression("私有事实发射序号溢出"))
    }

    pub(crate) fn prepare_lifecycle_feedback_timing(
        &mut self,
        feedback_id: String,
        platform_event_at_utc_ms: i64,
        private_emission_sequence: u64,
    ) -> Result<LifecycleFeedbackTimingV1, SessionError> {
        self.require_preparation_open()?;
        let result = self.prepare_lifecycle_feedback_timing_inner(
            feedback_id,
            platform_event_at_utc_ms,
            private_emission_sequence,
        );
        if result.is_err() {
            self.preparation_failed = true;
        }
        result
    }

    fn prepare_lifecycle_feedback_timing_inner(
        &mut self,
        feedback_id: String,
        platform_event_at_utc_ms: i64,
        private_emission_sequence: u64,
    ) -> Result<LifecycleFeedbackTimingV1, SessionError> {
        if feedback_id.is_empty() {
            return Err(SessionError::InvalidRequest("反馈身份为空"));
        }
        let existing = self
            .prepared_lifecycle_feedback
            .iter()
            .find(|(identity, _)| identity == &feedback_id)
            .map(|(_, timing)| timing)
            .or_else(|| self.state.lifecycle_feedback.get(&feedback_id));
        if let Some(existing) = existing {
            if existing.platform_event_at_utc_ms == platform_event_at_utc_ms
                && existing.private_emission_sequence == private_emission_sequence
            {
                return Ok(existing.clone());
            }
            return Err(SessionError::FeedbackIdentityConflict(feedback_id));
        }
        let inbound_delay_ms = self.latency_profile.sample_delay_ms(
            self.state.spec.session_seed.0.as_bytes(),
            &feedback_id,
            DelaySampleDomainV1::LifecycleFeedbackInbound,
        );
        let feedback_received_at_utc_ms =
            checked_add_delay(platform_event_at_utc_ms, inbound_delay_ms)?;
        let timing = LifecycleFeedbackTimingV1 {
            feedback_id: feedback_id.clone(),
            platform_event_at_utc_ms,
            private_emission_sequence,
            inbound_delay_ms,
            feedback_received_at_utc_ms,
        };
        self.prepare_private_fact_registration(
            PrivateVisibilityEvidenceV1::LifecycleFeedback {
                feedback_id: feedback_id.clone(),
            },
            feedback_received_at_utc_ms,
            private_emission_sequence,
        )?;
        self.prepared_lifecycle_feedback
            .push((feedback_id, timing.clone()));
        Ok(timing)
    }

    pub(crate) fn prepare_complete_direct_response(
        &mut self,
        global_request_id: &str,
        platform_responded_at_utc_ms: i64,
        private_emission_sequence: u64,
    ) -> Result<RequestTransitV1, SessionError> {
        self.require_preparation_open()?;
        let result = self.prepare_complete_direct_response_inner(
            global_request_id,
            platform_responded_at_utc_ms,
            private_emission_sequence,
        );
        if result.is_err() {
            self.preparation_failed = true;
        }
        result
    }

    fn prepare_complete_direct_response_inner(
        &mut self,
        global_request_id: &str,
        platform_responded_at_utc_ms: i64,
        private_emission_sequence: u64,
    ) -> Result<RequestTransitV1, SessionError> {
        let prepared_request_completed = self.completed_request_id.as_deref()
            == Some(global_request_id)
            && self.next_action_index == self.actions.len();
        if !self.state.released_request_ids.contains(global_request_id)
            && !prepared_request_completed
        {
            return Err(SessionError::RequestNotLogicallyReleased(
                global_request_id.to_owned(),
            ));
        }
        let record = self
            .state
            .requests
            .get(global_request_id)
            .ok_or_else(|| SessionError::RequestNotFound(global_request_id.to_owned()))?;
        if let Some(existing) = &record.response_transit {
            if existing.platform_responded_at_utc_ms == Some(platform_responded_at_utc_ms)
                && existing.private_emission_sequence == Some(private_emission_sequence)
            {
                return Ok(existing.clone());
            }
            return Err(SessionError::RequestIdentityConflict(
                global_request_id.to_owned(),
            ));
        }
        if let Some(existing) = &self.prepared_direct_response {
            if existing.global_request_id == global_request_id
                && existing.response_transit.platform_responded_at_utc_ms
                    == Some(platform_responded_at_utc_ms)
                && existing.response_transit.private_emission_sequence
                    == Some(private_emission_sequence)
            {
                return Ok(existing.response_transit.clone());
            }
            return Err(SessionError::RequestIdentityConflict(
                global_request_id.to_owned(),
            ));
        }
        let registration_transit = record.transit.clone();
        if platform_responded_at_utc_ms < registration_transit.platform_arrived_at_utc_ms {
            return Err(SessionError::ResponseBeforeArrival);
        }
        let last_action_at = if prepared_request_completed {
            self.actions
                .last()
                .filter(|action| action.global_request_id == global_request_id)
                .map(|action| action.key.effective_processing_at_utc_ms)
        } else {
            self.state
                .action_keys
                .values()
                .filter(|(_, request_id)| request_id == global_request_id)
                .map(|(key, _)| key.effective_processing_at_utc_ms)
                .max()
        }
        .ok_or_else(|| SessionError::RequestNotLogicallyReleased(global_request_id.to_owned()))?;
        if platform_responded_at_utc_ms < last_action_at {
            return Err(SessionError::ResponseBeforeLogicalCompletion);
        }
        let response_received_at_utc_ms = checked_add_delay(
            platform_responded_at_utc_ms,
            registration_transit.inbound_delay_ms,
        )?;
        self.prepare_private_fact_registration(
            PrivateVisibilityEvidenceV1::DirectResponse {
                global_request_id: global_request_id.to_owned(),
            },
            response_received_at_utc_ms,
            private_emission_sequence,
        )?;
        let mut response_transit = registration_transit;
        response_transit.platform_responded_at_utc_ms = Some(platform_responded_at_utc_ms);
        response_transit.private_emission_sequence = Some(private_emission_sequence);
        response_transit.response_received_at_utc_ms = Some(response_received_at_utc_ms);
        self.prepared_direct_response = Some(PreparedDirectResponseCompletionV2 {
            global_request_id: global_request_id.to_owned(),
            response_transit: response_transit.clone(),
        });
        Ok(response_transit)
    }

    fn commit_prevalidated(self) {
        let PreparedExecutionActionSliceV2 {
            state,
            latency_profile: _,
            closed_action_through_before_utc_ms: _,
            closed_action_through_after_utc_ms,
            actions,
            next_action_index,
            completed_request_id,
            prepared_request_pending_action_count,
            platform_state_revision_before,
            platform_state_revision_after,
            prepared_mutations,
            prepared_queries,
            prepared_lifecycle_feedback,
            prepared_private_facts,
            prepared_used_private_emission_sequences,
            prepared_direct_response,
            prepared_closed_bucket,
            preparation_failed,
        } = self;
        debug_assert!(!preparation_failed);
        debug_assert_eq!(next_action_index, actions.len());
        for action in actions {
            state.pending_actions.remove(&action.key);
            state.released_action_ids.insert(action.action_identity);
        }
        if let Some((global_request_id, remaining_pending_action_count)) =
            prepared_request_pending_action_count
        {
            let request = state
                .requests
                .get_mut(&global_request_id)
                .expect("已核对执行动作所属请求存在");
            request.remaining_pending_action_count = remaining_pending_action_count;
        }
        if let Some(global_request_id) = completed_request_id {
            state.released_request_ids.insert(global_request_id);
        }
        state.closed_action_through_utc_ms = Some(closed_action_through_after_utc_ms);
        if let Some(prepared_bucket) = prepared_closed_bucket {
            match prepared_bucket.path {
                MarketEventPathV1::HiddenPlatform => state.commit_platform_prefix_closure(
                    prepared_bucket.bucket_at_utc_ms,
                    prepared_bucket.next_index_position,
                ),
                MarketEventPathV1::PublicVisibility => state.commit_public_visibility_closure(
                    prepared_bucket.bucket_at_utc_ms,
                    prepared_bucket.next_index_position,
                ),
            }
            if let Some(outcome) = prepared_bucket.input_seal_after {
                state.commit_input_path_seal(prepared_bucket.path, outcome);
            }
        }
        debug_assert_eq!(
            state.platform_state_revision,
            platform_state_revision_before
        );
        state.platform_state_revision = platform_state_revision_after;
        for (identity, record) in prepared_mutations {
            debug_assert!(state.mutations.insert(identity, record).is_none());
        }
        for (request_id, record) in prepared_queries {
            debug_assert!(state.queries.insert(request_id, record).is_none());
        }
        for (feedback_id, timing) in prepared_lifecycle_feedback {
            debug_assert!(
                state
                    .lifecycle_feedback
                    .insert(feedback_id, timing)
                    .is_none()
            );
        }
        for registration in prepared_private_facts {
            let identity = registration.key.identity.clone();
            debug_assert!(
                state
                    .private_fact_keys
                    .insert(identity, registration.key.clone())
                    .is_none()
            );
            debug_assert!(
                state
                    .pending_private_facts
                    .insert(registration.key, registration.evidence)
                    .is_none()
            );
        }
        for sequence in prepared_used_private_emission_sequences {
            debug_assert!(state.used_private_emission_sequences.insert(sequence));
        }
        if let Some(prepared_response) = prepared_direct_response {
            let request = state
                .requests
                .get_mut(&prepared_response.global_request_id)
                .expect("已准备的直接响应必须仍属于当前会话");
            debug_assert!(request.response_transit.is_none());
            request.response_transit = Some(prepared_response.response_transit);
            let removed = state
                .expected_direct_response_request_ids
                .remove(&prepared_response.global_request_id);
            debug_assert!(removed);
        }
    }

    fn prepare_query_revision_unchecked(
        &mut self,
        global_request_id: &str,
        action_identity: &str,
        query_sha256: Sha256Hex,
        frozen_revision: Option<u64>,
    ) -> Result<QueryRevisionV1, SessionError> {
        self.validate_current_action(global_request_id, action_identity)?;
        if !is_sha256_hex(&query_sha256.0) {
            return Err(SessionError::InvalidRequest("查询摘要非法"));
        }
        let existing = self
            .prepared_queries
            .iter()
            .find(|(request_id, _)| request_id == global_request_id)
            .map(|(_, record)| record)
            .or_else(|| self.state.queries.get(global_request_id));
        if let Some(existing) = existing {
            let revision_conflicts = frozen_revision
                .is_some_and(|revision| revision != existing.platform_state_revision);
            if existing.query_sha256 != query_sha256 || revision_conflicts {
                return Err(SessionError::QueryIdentityConflict(
                    global_request_id.to_owned(),
                ));
            }
            return Ok(QueryRevisionV1 {
                platform_state_revision: existing.platform_state_revision,
                reused: true,
            });
        }
        let selected_revision = match frozen_revision {
            Some(revision) if revision <= self.platform_state_revision_after => revision,
            Some(_) => {
                return Err(SessionError::QueryIdentityConflict(
                    global_request_id.to_owned(),
                ));
            }
            None => self.platform_state_revision_after,
        };
        self.prepared_queries.push((
            global_request_id.to_owned(),
            QueryRecord {
                query_sha256,
                platform_state_revision: selected_revision,
            },
        ));
        Ok(QueryRevisionV1 {
            platform_state_revision: selected_revision,
            reused: false,
        })
    }

    fn validate_current_action(
        &self,
        global_request_id: &str,
        action_identity: &str,
    ) -> Result<(), SessionError> {
        if !self.state.requests.contains_key(global_request_id) {
            return Err(SessionError::RequestNotFound(global_request_id.to_owned()));
        }
        let current = self
            .actions
            .get(self.next_action_index)
            .ok_or_else(|| SessionError::ActionNotLogicallyReleased(action_identity.to_owned()))?;
        if current.action_identity != action_identity
            || current.global_request_id != global_request_id
        {
            return Err(SessionError::ActionNotLogicallyReleased(
                action_identity.to_owned(),
            ));
        }
        let action_request_id = self
            .state
            .action_keys
            .get(action_identity)
            .map(|(_, request_id)| request_id)
            .ok_or_else(|| SessionError::ActionNotLogicallyReleased(action_identity.to_owned()))?;
        if action_request_id != global_request_id {
            return Err(SessionError::ActionNotLogicallyReleased(
                action_identity.to_owned(),
            ));
        }
        Ok(())
    }

    fn prepare_private_fact_registration(
        &mut self,
        evidence: PrivateVisibilityEvidenceV1,
        received_at_utc_ms: i64,
        private_emission_sequence: u64,
    ) -> Result<(), SessionError> {
        if self
            .state
            .private_fact_schedule_closed_through_utc_ms
            .is_some_and(|closed| received_at_utc_ms <= closed)
        {
            return Err(SessionError::PrivateFactScheduleClosed);
        }
        let identity = evidence.identity();
        let key = PrivateFactOrderKey {
            received_at_utc_ms,
            private_emission_sequence,
            identity: identity.clone(),
        };
        if let Some(existing) = self
            .prepared_private_facts
            .iter()
            .find(|registration| registration.key.identity == identity)
            .map(|registration| &registration.key)
            .or_else(|| self.state.private_fact_keys.get(&identity))
        {
            if existing == &key {
                return Ok(());
            }
            return Err(SessionError::PrivateFactIdentityConflict(identity));
        }
        if self.next_private_emission_sequence()? != private_emission_sequence {
            return Err(SessionError::PrivateEmissionSequenceConflict(
                private_emission_sequence,
            ));
        }
        if self
            .prepared_used_private_emission_sequences
            .contains(&private_emission_sequence)
            || self
                .state
                .used_private_emission_sequences
                .contains(&private_emission_sequence)
        {
            return Err(SessionError::PrivateEmissionSequenceConflict(
                private_emission_sequence,
            ));
        }
        self.prepared_used_private_emission_sequences
            .push(private_emission_sequence);
        self.prepared_private_facts
            .push(PreparedPrivateFactRegistrationV2 { key, evidence });
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RegisteredLogicalScheduleV1 {
    pub request: RegisteredRequestV1,
    pub actions: Vec<QueuedLogicalActionV1>,
}

pub(crate) struct PreparedRequestScheduleV1 {
    request_registration: PreparedRequestRegistrationV1,
    schedule: RegisteredLogicalScheduleV1,
}

impl PreparedRequestScheduleV1 {
    pub(crate) fn schedule(&self) -> &RegisteredLogicalScheduleV1 {
        &self.schedule
    }
}

pub(crate) struct PreparedBusinessMarketUseV1 {
    mark_business_input_started: bool,
    paper_candidate_condition_ids: BTreeSet<String>,
    paper_candidate_token_ids: BTreeSet<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MutationResolutionDispositionV1 {
    NewEffect,
    ExistingEffect,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MutationResolutionV1 {
    pub disposition: MutationResolutionDispositionV1,
    pub effect_identity: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryRevisionV1 {
    pub platform_state_revision: u64,
    pub reused: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionError {
    InvalidSessionSpec(&'static str),
    SessionIdentitySerialization(String),
    RunIdMismatch,
    SessionAlreadyCreatedRequiresRetry(String),
    SessionRetentionExpired(String),
    AccountInstanceAlreadyReserved,
    CreationRequestConflict(String),
    SessionNotFound(String),
    SessionCreationIdentityConflict(String),
    SessionCreationRetryNotPrepared,
    InvalidInitializationControl(&'static str),
    InitializationStateUnavailable,
    InitializationControlIdentityConflict,
    ActivationIdentityConflict,
    FailureIdentityConflict,
    InitializationFailedClosed,
    InitializationAlreadyActive,
    SessionNotActive(SessionInitializationStateV1),
    InvalidMarketEnrollment(&'static str),
    MarketEnrollmentSessionOwnershipConflict,
    MarketEnrollmentRequestConflict(String),
    MarketEnrollmentFrozenAfterBusinessInput,
    MarketEnrollmentFrozenAfterInputExhausted,
    MarketEnrollmentCandidateAlreadyUsed(String),
    MarketEnrollmentDirectory(String),
    MarketEnrollmentState(String),
    MarketEnrollmentSequenceOverflow,
    MarketSetIdentityMismatch,
    ScenarioIdentityMismatch,
    CursorRegression(&'static str),
    HiddenPlatformReplayForbidden,
    FutureModelVisibleCursor,
    RequestBeforeVisibleFact,
    RequestBehindPlatformPrefix,
    InvalidRequest(&'static str),
    NonMonotonicCallerSequence,
    NonMonotonicRequestSentAt,
    RequestIdentityConflict(String),
    RequestNotFound(String),
    RequestNotLogicallyReleased(String),
    ActionNotLogicallyReleased(String),
    TimeOverflow,
    ResponseBeforeArrival,
    ResponseBeforeLogicalCompletion,
    FeedbackIdentityConflict(String),
    PrivateFactScheduleClosed,
    PrivateFactScheduleNotClosed,
    PrivateFactScheduleCrossesExpectedResponse(String),
    PrivateFactScheduleCrossesPendingAction(String),
    PrivateFactScheduleCrossesPendingPlatformWork(String),
    PrivateFactIdentityConflict(String),
    PrivateEmissionSequenceConflict(u64),
    PrivateFactConfirmationConflict,
    PlatformPrefixNotClosed,
    PlatformPrefixCrossesUnsealedRequest,
    PlatformPrefixCrossesPendingAction,
    ActionWatermarkClosed,
    ActionScheduleEmpty,
    ActionScheduleSealed(String),
    ActionScheduleNotSealed(String),
    ActionProcessingInProgress(String),
    ActionIdentityConflict(String),
    MutationIdentityConflict(String),
    QueryIdentityConflict(String),
    PlatformRevisionRegression,
    ExecutionState(String),
    ResponseOutboxFull,
    ResponseBridgeClosed,
    ResponseDeliveryIdentityConflict,
    InputFailedSealed,
    RetentionBarrierNotSatisfied(&'static str),

    RunArtifactInputsSealed,
    Durability(String),
    DurabilityRecoveryRequired(String),
}

impl fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSessionSpec(message) => write!(formatter, "会话规格非法：{message}"),
            Self::SessionIdentitySerialization(message) => {
                write!(formatter, "会话身份规范序列化失败：{message}")
            }
            Self::RunIdMismatch => write!(formatter, "会话 run_id 与运行根不匹配"),
            Self::SessionAlreadyCreatedRequiresRetry(session_id) => {
                write!(
                    formatter,
                    "会话已经创建，必须使用无桥创建重试：{session_id}"
                )
            }
            Self::SessionRetentionExpired(session_id) => {
                write!(formatter, "会话热状态保留期已经结束：{session_id}")
            }
            Self::AccountInstanceAlreadyReserved => {
                write!(formatter, "账户实例键已经保留，不能再次签发初始资金")
            }
            Self::CreationRequestConflict(request_id) => {
                write!(formatter, "开户请求身份已经绑定另一会话：{request_id}")
            }
            Self::SessionNotFound(session_id) => write!(formatter, "会话不存在：{session_id}"),
            Self::SessionCreationIdentityConflict(session_id) => {
                write!(
                    formatter,
                    "会话创建重试与原开户身份或载荷冲突：{session_id}"
                )
            }
            Self::SessionCreationRetryNotPrepared => {
                write!(formatter, "只有准备态会话可以返回 EXISTING_PREPARED")
            }
            Self::InvalidInitializationControl(message) => {
                write!(formatter, "会话初始化控制请求非法：{message}")
            }
            Self::InitializationStateUnavailable => {
                write!(formatter, "会话初始化状态不可用")
            }
            Self::InitializationControlIdentityConflict => {
                write!(formatter, "会话初始化控制身份与开户锚点不匹配")
            }
            Self::ActivationIdentityConflict => {
                write!(formatter, "激活身份与已经生效的激活结果冲突")
            }
            Self::FailureIdentityConflict => {
                write!(formatter, "初始化失败身份或证据与原结果冲突")
            }
            Self::InitializationFailedClosed => {
                write!(formatter, "会话初始化已经失败关闭，不能再激活")
            }
            Self::InitializationAlreadyActive => {
                write!(formatter, "会话已经激活，不能再标记初始化失败")
            }
            Self::SessionNotActive(state) => {
                write!(formatter, "会话尚未开放业务入口，当前初始化状态：{state:?}")
            }
            Self::InvalidMarketEnrollment(message) => {
                write!(formatter, "市场登记控制请求非法：{message}")
            }
            Self::MarketEnrollmentSessionOwnershipConflict => {
                write!(formatter, "市场登记会话句柄不属于当前运行根")
            }
            Self::MarketEnrollmentRequestConflict(request_id) => {
                write!(formatter, "市场登记请求身份绑定不同市场：{request_id}")
            }
            Self::MarketEnrollmentFrozenAfterBusinessInput => {
                write!(
                    formatter,
                    "训练或离线评估会话已经接纳业务输入，市场目录已冻结"
                )
            }
            Self::MarketEnrollmentFrozenAfterInputExhausted => {
                write!(formatter, "输入路径已经耗尽，不能再追加市场")
            }
            Self::MarketEnrollmentCandidateAlreadyUsed(identity) => {
                write!(formatter, "候选市场身份已经被业务请求使用：{identity}")
            }
            Self::MarketEnrollmentDirectory(message) => {
                write!(formatter, "市场登记目录冲突：{message}")
            }
            Self::MarketEnrollmentState(message) => {
                write!(formatter, "市场登记状态扩展失败：{message}")
            }
            Self::MarketEnrollmentSequenceOverflow => {
                write!(formatter, "市场登记链长度溢出")
            }
            Self::MarketSetIdentityMismatch => write!(formatter, "会话市场集摘要与进程场景不匹配"),
            Self::ScenarioIdentityMismatch => write!(formatter, "会话场景摘要与进程场景不匹配"),
            Self::CursorRegression(message) => write!(formatter, "游标倒退或不闭合：{message}"),
            Self::HiddenPlatformReplayForbidden => {
                write!(formatter, "模拟盘会话不能推进隐藏平台事件时间前缀")
            }
            Self::FutureModelVisibleCursor => write!(formatter, "请求引用未来模型可见游标"),
            Self::RequestBeforeVisibleFact => write!(formatter, "请求发送时点早于其模型可见事实"),
            Self::RequestBehindPlatformPrefix => {
                write!(formatter, "请求到达时点已经被平台状态前缀关闭")
            }
            Self::InvalidRequest(message) => write!(formatter, "请求上下文非法：{message}"),
            Self::NonMonotonicCallerSequence => write!(formatter, "调用方序列没有严格递增"),
            Self::NonMonotonicRequestSentAt => write!(formatter, "请求发送时点发生倒退"),
            Self::RequestIdentityConflict(id) => write!(formatter, "请求身份载荷冲突：{id}"),
            Self::RequestNotFound(id) => write!(formatter, "请求身份不存在：{id}"),
            Self::RequestNotLogicallyReleased(id) => {
                write!(formatter, "请求尚未按逻辑顺序释放：{id}")
            }
            Self::ActionNotLogicallyReleased(id) => {
                write!(formatter, "批量项尚未按逻辑顺序释放：{id}")
            }
            Self::TimeOverflow => write!(formatter, "逻辑时间加法溢出"),
            Self::ResponseBeforeArrival => write!(formatter, "平台响应时点早于请求到达时点"),
            Self::ResponseBeforeLogicalCompletion => {
                write!(formatter, "平台响应时点早于请求最后一个逻辑动作")
            }
            Self::FeedbackIdentityConflict(id) => write!(formatter, "反馈身份冲突：{id}"),
            Self::PrivateFactScheduleClosed => {
                write!(formatter, "私有事实接收时点已被输入水位关闭")
            }
            Self::PrivateFactScheduleNotClosed => {
                write!(formatter, "私有事实计划尚未闭合到模型时点")
            }
            Self::PrivateFactScheduleCrossesExpectedResponse(id) => {
                write!(formatter, "私有事实计划水位跨过尚未形成的直接响应：{id}")
            }
            Self::PrivateFactScheduleCrossesPendingAction(id) => {
                write!(formatter, "私有事实计划水位跨过尚未处理的逻辑动作：{id}")
            }
            Self::PrivateFactScheduleCrossesPendingPlatformWork(id) => {
                write!(
                    formatter,
                    "私有事实计划水位跨过尚未处理的平台逻辑工作：{id}"
                )
            }
            Self::PrivateFactIdentityConflict(id) => write!(formatter, "私有事实身份冲突：{id}"),
            Self::PrivateEmissionSequenceConflict(sequence) => {
                write!(formatter, "私有事实发射序号重复：{sequence}")
            }
            Self::PrivateFactConfirmationConflict => {
                write!(formatter, "私有事实确认与当前连续队头不一致")
            }
            Self::PlatformPrefixNotClosed => write!(formatter, "平台事件前缀尚未闭合到动作时点"),
            Self::PlatformPrefixCrossesUnsealedRequest => {
                write!(formatter, "平台状态前缀不能闭合到尚未密封的请求时点")
            }
            Self::PlatformPrefixCrossesPendingAction => {
                write!(formatter, "平台状态前缀不能跨过更早的待处理动作")
            }
            Self::ActionWatermarkClosed => write!(formatter, "动作时点已经被逻辑水位关闭"),
            Self::ActionScheduleEmpty => write!(formatter, "请求没有可密封的逻辑动作"),
            Self::ActionScheduleSealed(id) => write!(formatter, "请求动作集合已经密封：{id}"),
            Self::ActionScheduleNotSealed(id) => {
                write!(formatter, "请求动作集合尚未密封：{id}")
            }
            Self::ActionProcessingInProgress(id) => {
                write!(formatter, "前一逻辑动作尚未完成：{id}")
            }
            Self::ActionIdentityConflict(id) => write!(formatter, "逻辑动作身份冲突：{id}"),
            Self::MutationIdentityConflict(id) => write!(formatter, "变更幂等身份冲突：{id}"),
            Self::QueryIdentityConflict(id) => write!(formatter, "查询身份冲突：{id}"),
            Self::PlatformRevisionRegression => write!(formatter, "平台状态修订号倒退"),
            Self::ExecutionState(message) => write!(formatter, "会话执行状态非法：{message}"),
            Self::ResponseOutboxFull => write!(formatter, "会话提交响应发件箱已满"),
            Self::ResponseBridgeClosed => write!(formatter, "会话响应桥接器已关闭新逻辑入口"),
            Self::ResponseDeliveryIdentityConflict => {
                write!(formatter, "响应交付确认身份与连续队头冲突")
            }
            Self::InputFailedSealed => write!(formatter, "模拟运行已经因输入终态缺失而失败封存"),
            Self::RetentionBarrierNotSatisfied(message) => {
                write!(formatter, "保留期删除屏障尚未满足：{message}")
            }

            Self::RunArtifactInputsSealed => {
                write!(formatter, "运行产物输入已经封口，不能继续提交业务变化")
            }
            Self::Durability(message) => write!(formatter, "会话本地耐久提交失败：{message}"),
            Self::DurabilityRecoveryRequired(message) => {
                write!(
                    formatter,
                    "会话本地耐久结果不确定，必须重启恢复后裁决：{message}"
                )
            }
        }
    }
}

impl std::error::Error for SessionError {}

impl From<MatchingStateErrorV1> for SessionError {
    fn from(value: MatchingStateErrorV1) -> Self {
        Self::ExecutionState(format!("{value:?}"))
    }
}

impl From<SessionMarketDirectoryErrorV1> for SessionError {
    fn from(value: SessionMarketDirectoryErrorV1) -> Self {
        Self::MarketEnrollmentDirectory(value.to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestRecord {
    context: RequestContextV1,
    canonical_request_sha256: Sha256Hex,
    transit: RequestTransitV1,
    response_transit: Option<RequestTransitV1>,
    #[serde(skip)]
    remaining_pending_action_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationRecord {
    payload_sha256: Sha256Hex,
    effect_identity: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryRecord {
    query_sha256: Sha256Hex,
    platform_state_revision: u64,
}

enum PreparedRequestRegistrationV1 {
    New(Box<RequestRecord>),
    PhysicalRetry(RequestTransitV1),
}

impl PreparedRequestRegistrationV1 {
    fn registered_request(&self) -> RegisteredRequestV1 {
        match self {
            Self::New(record) => RegisteredRequestV1 {
                disposition: RequestRegistrationDispositionV1::NewLogicalRequest,
                transit: record.transit.clone(),
            },
            Self::PhysicalRetry(transit) => RegisteredRequestV1 {
                disposition: RequestRegistrationDispositionV1::PhysicalRetry,
                transit: transit.clone(),
            },
        }
    }
}

#[derive(Clone)]
struct SessionState {
    spec: SessionSpecV2,
    platform_prefix: PlatformPrefixCursorV1,
    public_visibility: PublicVisibilityCursorV1,
    hidden_input_seal: Option<InputPathSealOutcomeV1>,
    public_input_seal: Option<InputPathSealOutcomeV1>,
    private_feedback_sequence: u64,
    private_fact_received_at_by_sequence: Vec<i64>,
    last_caller_sequence: Option<u64>,
    last_request_sent_at_utc_ms: Option<i64>,
    requests: BTreeMap<String, RequestRecord>,
    lifecycle_feedback: BTreeMap<String, LifecycleFeedbackTimingV1>,
    private_fact_keys: BTreeMap<String, PrivateFactOrderKey>,
    pending_private_facts: BTreeMap<PrivateFactOrderKey, PrivateVisibilityEvidenceV1>,
    expected_direct_response_request_ids: BTreeSet<String>,
    used_private_emission_sequences: BTreeSet<u64>,
    private_fact_schedule_closed_through_utc_ms: Option<i64>,
    private_visibility_closed_through_utc_ms: Option<i64>,
    pending_actions: BTreeMap<LogicalActionKeyV1, QueuedLogicalActionV1>,
    action_keys: BTreeMap<String, (LogicalActionKeyV1, String)>,
    sealed_request_ids: BTreeSet<String>,
    released_action_ids: BTreeSet<String>,
    in_flight_action_identity: Option<String>,
    released_request_ids: BTreeSet<String>,
    closed_action_through_utc_ms: Option<i64>,
    mutations: BTreeMap<String, MutationRecord>,
    queries: BTreeMap<String, QueryRecord>,
    platform_state_revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionStateCheckpointV1 {
    platform_prefix: PlatformPrefixCursorV1,
    public_visibility: PublicVisibilityCursorV1,
    hidden_input_seal: Option<InputPathSealOutcomeV1>,
    public_input_seal: Option<InputPathSealOutcomeV1>,
    private_feedback_sequence: u64,
    private_fact_received_at_by_sequence: Vec<i64>,
    last_caller_sequence: Option<u64>,
    last_request_sent_at_utc_ms: Option<i64>,
    requests: BTreeMap<String, RequestRecord>,
    lifecycle_feedback: BTreeMap<String, LifecycleFeedbackTimingV1>,
    private_fact_keys: BTreeMap<String, PrivateFactOrderKey>,
    pending_private_facts: Vec<(PrivateFactOrderKey, PrivateVisibilityEvidenceV1)>,
    expected_direct_response_request_ids: BTreeSet<String>,
    used_private_emission_sequences: BTreeSet<u64>,
    private_fact_schedule_closed_through_utc_ms: Option<i64>,
    private_visibility_closed_through_utc_ms: Option<i64>,
    pending_actions: Vec<(LogicalActionKeyV1, QueuedLogicalActionV1)>,
    action_keys: BTreeMap<String, (LogicalActionKeyV1, String)>,
    sealed_request_ids: BTreeSet<String>,
    released_action_ids: BTreeSet<String>,
    released_request_ids: BTreeSet<String>,
    closed_action_through_utc_ms: Option<i64>,
    mutations: BTreeMap<String, MutationRecord>,
    queries: BTreeMap<String, QueryRecord>,
    platform_state_revision: u64,
}

impl From<&SessionState> for SessionStateCheckpointV1 {
    fn from(state: &SessionState) -> Self {
        Self {
            platform_prefix: state.platform_prefix.clone(),
            public_visibility: state.public_visibility.clone(),
            hidden_input_seal: state.hidden_input_seal.clone(),
            public_input_seal: state.public_input_seal.clone(),
            private_feedback_sequence: state.private_feedback_sequence,
            private_fact_received_at_by_sequence: state
                .private_fact_received_at_by_sequence
                .clone(),
            last_caller_sequence: state.last_caller_sequence,
            last_request_sent_at_utc_ms: state.last_request_sent_at_utc_ms,
            requests: state
                .requests
                .iter()
                .map(|(request_id, record)| {
                    let mut record = record.clone();
                    record.remaining_pending_action_count = 0;
                    (request_id.clone(), record)
                })
                .collect(),
            lifecycle_feedback: state.lifecycle_feedback.clone(),
            private_fact_keys: state.private_fact_keys.clone(),
            pending_private_facts: state
                .pending_private_facts
                .iter()
                .map(|(key, evidence)| (key.clone(), evidence.clone()))
                .collect(),
            expected_direct_response_request_ids: state
                .expected_direct_response_request_ids
                .clone(),
            used_private_emission_sequences: state.used_private_emission_sequences.clone(),
            private_fact_schedule_closed_through_utc_ms: state
                .private_fact_schedule_closed_through_utc_ms,
            private_visibility_closed_through_utc_ms: state
                .private_visibility_closed_through_utc_ms,
            pending_actions: state
                .pending_actions
                .iter()
                .map(|(key, action)| (*key, action.clone()))
                .collect(),
            action_keys: state.action_keys.clone(),
            sealed_request_ids: state.sealed_request_ids.clone(),
            released_action_ids: state.released_action_ids.clone(),
            released_request_ids: state.released_request_ids.clone(),
            closed_action_through_utc_ms: state.closed_action_through_utc_ms,
            mutations: state.mutations.clone(),
            queries: state.queries.clone(),
            platform_state_revision: state.platform_state_revision,
        }
    }
}

impl SessionStateCheckpointV1 {
    fn restore(mut self, spec: SessionSpecV2) -> Result<SessionState, SessionCheckpointErrorV1> {
        if spec.caller_role == SimCallerRoleV1::Paper
            && (self.hidden_input_seal.is_some()
                || self
                    .platform_prefix
                    .closed_through_source_event_at_utc_ms()
                    .is_some()
                || self.platform_prefix.next_event_time_index_position() != 0)
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "模拟盘检查点不得包含隐藏输入游标或封存结果".to_owned(),
            ));
        }
        let confirmed_private_fact_count = usize::try_from(self.private_feedback_sequence)
            .map_err(|_| {
                SessionCheckpointErrorV1::InvalidSessionState(
                    "私有反馈水位无法映射为接收时点数量".to_owned(),
                )
            })?;
        if self.private_fact_received_at_by_sequence.len() != confirmed_private_fact_count {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "私有事实接收时点不是进程内确认水位的连续前缀".to_owned(),
            ));
        }
        let used_private_emission_count = u64::try_from(self.used_private_emission_sequences.len())
            .map_err(|_| {
                SessionCheckpointErrorV1::InvalidSessionState(
                    "私有事实发射序号集合超出 u64 边界".to_owned(),
                )
            })?;
        if self
            .used_private_emission_sequences
            .iter()
            .copied()
            .ne(1..=used_private_emission_count)
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "私有事实发射序号不是从 1 开始的连续前缀".to_owned(),
            ));
        }
        let pending_private_fact_count = self.pending_private_facts.len();
        let pending_private_facts = self
            .pending_private_facts
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        if pending_private_facts.len() != pending_private_fact_count {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "待接收私有事实排序键重复".to_owned(),
            ));
        }
        if self.private_fact_keys.len() != pending_private_facts.len()
            || pending_private_facts.iter().any(|(key, evidence)| {
                key.identity != evidence.identity()
                    || self.private_fact_keys.get(&key.identity) != Some(key)
            })
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "私有事实身份索引与统一待接收队列不一致".to_owned(),
            ));
        }
        let pending_lifecycle_timings = pending_private_facts
            .iter()
            .filter_map(|(key, evidence)| match evidence {
                PrivateVisibilityEvidenceV1::LifecycleFeedback { feedback_id } => {
                    Some((feedback_id, key))
                }
                PrivateVisibilityEvidenceV1::DirectResponse { .. } => None,
            })
            .collect::<BTreeMap<_, _>>();
        if self.lifecycle_feedback.len() != pending_lifecycle_timings.len()
            || self.lifecycle_feedback.iter().any(|(feedback_id, timing)| {
                timing.feedback_id != *feedback_id
                    || pending_lifecycle_timings
                        .get(feedback_id)
                        .is_none_or(|key| {
                            key.received_at_utc_ms != timing.feedback_received_at_utc_ms
                                || key.private_emission_sequence != timing.private_emission_sequence
                        })
            })
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "生命周期反馈时序与统一待接收队列不一致".to_owned(),
            ));
        }
        for (key, evidence) in &pending_private_facts {
            if let PrivateVisibilityEvidenceV1::DirectResponse { global_request_id } = evidence {
                let Some(transit) = self
                    .requests
                    .get(global_request_id)
                    .and_then(|record| record.response_transit.as_ref())
                else {
                    return Err(SessionCheckpointErrorV1::InvalidSessionState(
                        "待接收直接响应缺少已形成的传输事实".to_owned(),
                    ));
                };
                if transit.response_received_at_utc_ms != Some(key.received_at_utc_ms)
                    || transit.private_emission_sequence != Some(key.private_emission_sequence)
                {
                    return Err(SessionCheckpointErrorV1::InvalidSessionState(
                        "待接收直接响应与传输时序不一致".to_owned(),
                    ));
                }
            }
        }
        let total_emission_count = self
            .private_feedback_sequence
            .checked_add(u64::try_from(pending_private_facts.len()).map_err(|_| {
                SessionCheckpointErrorV1::InvalidSessionState(
                    "待接收私有事实数量超出 u64 边界".to_owned(),
                )
            })?)
            .ok_or_else(|| {
                SessionCheckpointErrorV1::InvalidSessionState("私有事实总序号溢出".to_owned())
            })?;
        if used_private_emission_count != total_emission_count {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "已确认与待接收私有事实未覆盖完整发射序号前缀".to_owned(),
            ));
        }
        let max_confirmed_received_at = self
            .private_fact_received_at_by_sequence
            .iter()
            .copied()
            .max();
        if max_confirmed_received_at.is_some_and(|received_at| {
            self.private_visibility_closed_through_utc_ms
                .is_none_or(|closed| closed < received_at)
        }) || self
            .private_visibility_closed_through_utc_ms
            .is_some_and(|visible| {
                self.private_fact_schedule_closed_through_utc_ms
                    .is_none_or(|scheduled| visible > scheduled)
            })
            || self
                .private_fact_schedule_closed_through_utc_ms
                .is_some_and(|scheduled| {
                    self.closed_action_through_utc_ms
                        .is_none_or(|closed| scheduled > closed)
                        || self
                            .expected_direct_response_request_ids
                            .iter()
                            .any(|request_id| {
                                let Some(record) = self.requests.get(request_id) else {
                                    return true;
                                };
                                self.action_keys
                                    .values()
                                    .filter(|(_, action_request_id)| {
                                        action_request_id == request_id
                                    })
                                    .map(|(key, _)| key.effective_processing_at_utc_ms)
                                    .max()
                                    .and_then(|responded_at| {
                                        checked_add_delay(
                                            responded_at,
                                            record.transit.inbound_delay_ms,
                                        )
                                        .ok()
                                    })
                                    .is_some_and(|expected_at| expected_at <= scheduled)
                            })
                })
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "私有事实计划、可见性与逻辑动作水位不一致".to_owned(),
            ));
        }
        let pending_action_count = self.pending_actions.len();
        let pending_actions = self.pending_actions.into_iter().collect::<BTreeMap<_, _>>();
        if pending_actions.len() != pending_action_count {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "待处理逻辑动作排序键重复".to_owned(),
            ));
        }
        if pending_actions.iter().any(|(key, action)| {
            action.key != *key
                || self.action_keys.get(&action.action_identity)
                    != Some(&(*key, action.global_request_id.clone()))
        }) {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "待处理逻辑动作正文与身份索引不一致".to_owned(),
            ));
        }
        for record in self.requests.values_mut() {
            record.remaining_pending_action_count = 0;
        }
        for action in pending_actions.values() {
            let record = self
                .requests
                .get_mut(&action.global_request_id)
                .ok_or_else(|| {
                    SessionCheckpointErrorV1::InvalidSessionState(format!(
                        "待处理逻辑动作引用未登记请求：{}",
                        action.global_request_id
                    ))
                })?;
            record.remaining_pending_action_count = record
                .remaining_pending_action_count
                .checked_add(1)
                .ok_or_else(|| {
                    SessionCheckpointErrorV1::InvalidSessionState(
                        "请求待处理逻辑动作计数溢出".to_owned(),
                    )
                })?;
        }
        let pending_action_identities = pending_actions
            .values()
            .map(|action| action.action_identity.as_str())
            .collect::<BTreeSet<_>>();
        let earliest_pending_private_fact_at = pending_private_facts
            .first_key_value()
            .map(|(key, _)| key.received_at_utc_ms);
        let earliest_pending_action_at = pending_actions
            .first_key_value()
            .map(|(key, _)| key.effective_processing_at_utc_ms);
        if earliest_pending_private_fact_at.is_some_and(|pending_at| {
            self.private_visibility_closed_through_utc_ms
                .is_some_and(|visible| visible > pending_at)
        }) || earliest_pending_action_at.is_some_and(|pending_at| {
            self.private_fact_schedule_closed_through_utc_ms
                .is_some_and(|scheduled| scheduled >= pending_at)
        }) {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "私有事实水位越过待交付事实或待处理动作".to_owned(),
            ));
        }
        if self
            .action_keys
            .iter()
            .any(|(action_identity, (key, request_id))| {
                self.requests
                    .get(request_id)
                    .is_none_or(|request| key.caller_sequence != request.context.caller_sequence)
                    || (pending_action_identities.contains(action_identity.as_str())
                        == self.released_action_ids.contains(action_identity))
            })
            || self
                .released_action_ids
                .iter()
                .any(|identity| !self.action_keys.contains_key(identity))
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "逻辑动作索引、待处理集合与已释放集合不一致".to_owned(),
            ));
        }
        let request_ids = self.requests.keys().cloned().collect::<BTreeSet<_>>();
        if self.sealed_request_ids != request_ids {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "恢复请求集合没有全部完成动作计划密封".to_owned(),
            ));
        }
        let expected_direct_response_request_ids = self
            .requests
            .iter()
            .filter_map(|(request_id, record)| {
                record
                    .response_transit
                    .is_none()
                    .then_some(request_id.clone())
            })
            .collect::<BTreeSet<_>>();
        if self.expected_direct_response_request_ids != expected_direct_response_request_ids {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "待形成直接响应集合与请求传输状态不一致".to_owned(),
            ));
        }
        let mut expected_released_request_ids = BTreeSet::new();
        for (request_id, record) in &self.requests {
            let mut actions = self
                .action_keys
                .iter()
                .filter_map(|(identity, (key, action_request_id))| {
                    (action_request_id == request_id).then_some((identity, key))
                })
                .collect::<Vec<_>>();
            actions.sort_by_key(|(_, key)| key.batch_item_ordinal);
            if actions.is_empty()
                || actions
                    .iter()
                    .enumerate()
                    .any(|(expected, (_, key))| usize::from(key.batch_item_ordinal) != expected)
            {
                return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                    "恢复请求动作序号不是从零开始的连续集合：{request_id}"
                )));
            }
            let completed = actions
                .iter()
                .all(|(identity, _)| self.released_action_ids.contains(identity.as_str()));
            if completed {
                expected_released_request_ids.insert(request_id.clone());
            }
            if record.response_transit.is_some() != completed {
                return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                    "请求完成状态与直接响应传输状态不一致：{request_id}"
                )));
            }
            if record.transit.request_sent_at_utc_ms != record.context.request_sent_at_utc_ms
                || checked_add_delay(
                    record.transit.request_sent_at_utc_ms,
                    record.transit.outbound_delay_ms,
                )
                .ok()
                    != Some(record.transit.platform_arrived_at_utc_ms)
            {
                return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                    "请求登记传输时点与请求上下文不一致：{request_id}"
                )));
            }
            if let Some(response) = &record.response_transit {
                let (Some(responded_at), Some(emission_sequence), Some(received_at)) = (
                    response.platform_responded_at_utc_ms,
                    response.private_emission_sequence,
                    response.response_received_at_utc_ms,
                ) else {
                    return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                        "已形成直接响应缺少完整传输时点：{request_id}"
                    )));
                };
                let last_action_at = actions
                    .iter()
                    .map(|(_, key)| key.effective_processing_at_utc_ms)
                    .max()
                    .expect("已核对请求至少有一个动作");
                if response.request_sent_at_utc_ms != record.transit.request_sent_at_utc_ms
                    || response.outbound_delay_ms != record.transit.outbound_delay_ms
                    || response.platform_arrived_at_utc_ms
                        != record.transit.platform_arrived_at_utc_ms
                    || response.inbound_delay_ms != record.transit.inbound_delay_ms
                    || responded_at < last_action_at
                    || emission_sequence == 0
                    || checked_add_delay(responded_at, response.inbound_delay_ms).ok()
                        != Some(received_at)
                {
                    return Err(SessionCheckpointErrorV1::InvalidSessionState(format!(
                        "已形成直接响应传输链与请求动作不一致：{request_id}"
                    )));
                }
            }
        }
        if self.released_request_ids != expected_released_request_ids {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "已释放请求集合与动作完成状态不一致".to_owned(),
            ));
        }
        let expected_last_caller_sequence = self
            .requests
            .values()
            .map(|record| record.context.caller_sequence)
            .max();
        let expected_last_request_sent_at = self
            .requests
            .values()
            .map(|record| record.context.request_sent_at_utc_ms)
            .max();
        if self.last_caller_sequence != expected_last_caller_sequence
            || self.last_request_sent_at_utc_ms != expected_last_request_sent_at
        {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "请求单调水位与已登记请求集合不一致".to_owned(),
            ));
        }
        let max_released_at = self
            .released_action_ids
            .iter()
            .filter_map(|identity| self.action_keys.get(identity))
            .map(|(key, _)| key.effective_processing_at_utc_ms)
            .max();
        let next_pending_at = pending_actions
            .first_key_value()
            .map(|(key, _)| key.effective_processing_at_utc_ms);
        if max_released_at.is_some_and(|released_at| {
            self.closed_action_through_utc_ms
                .is_none_or(|closed| closed < released_at)
        }) || next_pending_at.is_some_and(|pending_at| {
            self.closed_action_through_utc_ms
                .is_some_and(|closed| closed > pending_at)
        }) {
            return Err(SessionCheckpointErrorV1::InvalidSessionState(
                "逻辑动作闭合水位与已释放或待处理动作不一致".to_owned(),
            ));
        }
        Ok(SessionState {
            spec,
            platform_prefix: self.platform_prefix,
            public_visibility: self.public_visibility,
            hidden_input_seal: self.hidden_input_seal,
            public_input_seal: self.public_input_seal,
            private_feedback_sequence: self.private_feedback_sequence,
            private_fact_received_at_by_sequence: self.private_fact_received_at_by_sequence,
            last_caller_sequence: self.last_caller_sequence,
            last_request_sent_at_utc_ms: self.last_request_sent_at_utc_ms,
            requests: self.requests,
            lifecycle_feedback: self.lifecycle_feedback,
            private_fact_keys: self.private_fact_keys,
            pending_private_facts,
            expected_direct_response_request_ids: self.expected_direct_response_request_ids,
            used_private_emission_sequences: self.used_private_emission_sequences,
            private_fact_schedule_closed_through_utc_ms: self
                .private_fact_schedule_closed_through_utc_ms,
            private_visibility_closed_through_utc_ms: self.private_visibility_closed_through_utc_ms,
            pending_actions,
            action_keys: self.action_keys,
            sealed_request_ids: self.sealed_request_ids,
            released_action_ids: self.released_action_ids,
            in_flight_action_identity: None,
            released_request_ids: self.released_request_ids,
            closed_action_through_utc_ms: self.closed_action_through_utc_ms,
            mutations: self.mutations,
            queries: self.queries,
            platform_state_revision: self.platform_state_revision,
        })
    }
}

impl SessionState {
    #[cfg(test)]
    fn pending_action_count_for_request_for_tests(&self, global_request_id: &str) -> usize {
        self.requests
            .get(global_request_id)
            .map(|record| {
                usize::try_from(record.remaining_pending_action_count)
                    .expect("测试请求待处理动作计数适合 usize")
            })
            .unwrap_or(0)
    }

    fn new(spec: SessionSpecV2) -> Self {
        Self {
            spec,
            platform_prefix: PlatformPrefixCursorV1::default(),
            public_visibility: PublicVisibilityCursorV1::default(),
            hidden_input_seal: None,
            public_input_seal: None,
            private_feedback_sequence: 0,
            private_fact_received_at_by_sequence: Vec::new(),
            last_caller_sequence: None,
            last_request_sent_at_utc_ms: None,
            requests: BTreeMap::new(),
            lifecycle_feedback: BTreeMap::new(),
            private_fact_keys: BTreeMap::new(),
            pending_private_facts: BTreeMap::new(),
            expected_direct_response_request_ids: BTreeSet::new(),
            used_private_emission_sequences: BTreeSet::new(),
            private_fact_schedule_closed_through_utc_ms: None,
            private_visibility_closed_through_utc_ms: None,
            pending_actions: BTreeMap::new(),
            action_keys: BTreeMap::new(),
            sealed_request_ids: BTreeSet::new(),
            released_action_ids: BTreeSet::new(),
            in_flight_action_identity: None,
            released_request_ids: BTreeSet::new(),
            closed_action_through_utc_ms: None,
            mutations: BTreeMap::new(),
            queries: BTreeMap::new(),
            platform_state_revision: 0,
        }
    }

    fn model_visible_cursor(&self) -> ModelVisibleCursorV1 {
        ModelVisibleCursorV1 {
            session_id: self.spec.session_id.clone(),
            public_visibility: self.public_visibility.clone(),
            private_feedback_sequence: self.private_feedback_sequence,
        }
    }

    fn input_path_seal(&self, path: MarketEventPathV1) -> Option<&InputPathSealOutcomeV1> {
        match path {
            MarketEventPathV1::HiddenPlatform => self.hidden_input_seal.as_ref(),
            MarketEventPathV1::PublicVisibility => self.public_input_seal.as_ref(),
        }
    }

    fn commit_input_path_seal(&mut self, path: MarketEventPathV1, outcome: InputPathSealOutcomeV1) {
        let slot = match path {
            MarketEventPathV1::HiddenPlatform => &mut self.hidden_input_seal,
            MarketEventPathV1::PublicVisibility => &mut self.public_input_seal,
        };
        debug_assert!(slot.is_none());
        *slot = Some(outcome);
    }

    fn failed_input_seal(&self) -> Option<(MarketEventPathV1, &InputPathSealOutcomeV1)> {
        [
            (MarketEventPathV1::HiddenPlatform, &self.hidden_input_seal),
            (MarketEventPathV1::PublicVisibility, &self.public_input_seal),
        ]
        .into_iter()
        .find_map(|(path, seal)| {
            seal.as_ref()
                .filter(|outcome| {
                    matches!(
                        outcome,
                        InputPathSealOutcomeV1::FailedMissingQualifiedMarketTerminal { .. }
                    )
                })
                .map(|outcome| (path, outcome))
        })
    }

    fn has_any_input_seal(&self) -> bool {
        self.hidden_input_seal.is_some() || self.public_input_seal.is_some()
    }

    fn close_platform_prefix_through(
        &mut self,
        source_event_at_utc_ms: i64,
        next_event_time_index_position: u64,
    ) -> Result<(), SessionError> {
        self.validate_platform_prefix_closure(
            source_event_at_utc_ms,
            next_event_time_index_position,
        )?;
        self.commit_platform_prefix_closure(source_event_at_utc_ms, next_event_time_index_position);
        Ok(())
    }

    fn validate_platform_prefix_closure(
        &self,
        source_event_at_utc_ms: i64,
        next_event_time_index_position: u64,
    ) -> Result<(), SessionError> {
        if self.spec.caller_role == SimCallerRoleV1::Paper {
            return Err(SessionError::HiddenPlatformReplayForbidden);
        }
        if self
            .earliest_unsealed_request_at()
            .is_some_and(|request_at| source_event_at_utc_ms >= request_at)
        {
            return Err(SessionError::PlatformPrefixCrossesUnsealedRequest);
        }
        if self
            .earliest_scheduled_action_at()
            .is_some_and(|pending_at| source_event_at_utc_ms > pending_at)
        {
            return Err(SessionError::PlatformPrefixCrossesPendingAction);
        }
        if self.platform_prefix.closed_through_source_event_at_utc_ms
            == Some(source_event_at_utc_ms)
            && self.platform_prefix.next_event_time_index_position == next_event_time_index_position
        {
            return Ok(());
        }
        if self
            .platform_prefix
            .closed_through_source_event_at_utc_ms
            .is_some_and(|current| source_event_at_utc_ms <= current)
            || next_event_time_index_position < self.platform_prefix.next_event_time_index_position
        {
            return Err(SessionError::CursorRegression("平台事件时间前缀"));
        }
        Ok(())
    }

    fn commit_platform_prefix_closure(
        &mut self,
        source_event_at_utc_ms: i64,
        next_event_time_index_position: u64,
    ) {
        self.platform_prefix.closed_through_source_event_at_utc_ms = Some(source_event_at_utc_ms);
        self.platform_prefix.next_event_time_index_position = next_event_time_index_position;
    }

    fn advance_public_visibility_through(
        &mut self,
        received_at_utc_ms: i64,
        next_visibility_index_position: u64,
    ) -> Result<(), SessionError> {
        self.validate_public_visibility_closure(
            received_at_utc_ms,
            next_visibility_index_position,
        )?;
        self.commit_public_visibility_closure(received_at_utc_ms, next_visibility_index_position);
        Ok(())
    }

    fn validate_public_visibility_closure(
        &self,
        received_at_utc_ms: i64,
        next_visibility_index_position: u64,
    ) -> Result<(), SessionError> {
        if self.spec.caller_role == SimCallerRoleV1::Paper
            && self
                .earliest_unsealed_request_at()
                .is_some_and(|request_at| received_at_utc_ms >= request_at)
        {
            return Err(SessionError::PlatformPrefixCrossesUnsealedRequest);
        }
        if self.spec.caller_role == SimCallerRoleV1::Paper
            && self
                .earliest_scheduled_action_at()
                .is_some_and(|pending_at| received_at_utc_ms > pending_at)
        {
            return Err(SessionError::PlatformPrefixCrossesPendingAction);
        }
        if self.public_visibility.closed_through_received_at_utc_ms == Some(received_at_utc_ms)
            && self.public_visibility.next_visibility_index_position
                == next_visibility_index_position
        {
            return Ok(());
        }
        if self
            .public_visibility
            .closed_through_received_at_utc_ms
            .is_some_and(|current| received_at_utc_ms <= current)
            || next_visibility_index_position
                < self.public_visibility.next_visibility_index_position
        {
            return Err(SessionError::CursorRegression("公开可见前缀"));
        }
        Ok(())
    }

    fn commit_public_visibility_closure(
        &mut self,
        received_at_utc_ms: i64,
        next_visibility_index_position: u64,
    ) {
        self.public_visibility.closed_through_received_at_utc_ms = Some(received_at_utc_ms);
        self.public_visibility.next_visibility_index_position = next_visibility_index_position;
    }

    fn close_private_fact_schedule_through(
        &mut self,
        received_at_utc_ms: i64,
    ) -> Result<(), SessionError> {
        self.validate_private_fact_schedule_closure(received_at_utc_ms)?;
        self.private_fact_schedule_closed_through_utc_ms = Some(received_at_utc_ms);
        Ok(())
    }

    fn validate_private_fact_schedule_closure(
        &self,
        received_at_utc_ms: i64,
    ) -> Result<(), SessionError> {
        if self.private_fact_schedule_closed_through_utc_ms == Some(received_at_utc_ms) {
            return Ok(());
        }
        if self
            .private_fact_schedule_closed_through_utc_ms
            .is_some_and(|closed| received_at_utc_ms <= closed)
        {
            return Err(SessionError::CursorRegression("私有事实计划水位"));
        }
        if let Some(request_id) =
            self.expected_direct_response_request_ids
                .iter()
                .find(|request_id| {
                    self.requests
                        .get(*request_id)
                        .is_some_and(|record| record.response_transit.is_none())
                        && self
                            .expected_direct_response_received_at(request_id)
                            .is_ok_and(|expected_at| expected_at <= received_at_utc_ms)
                })
        {
            return Err(SessionError::PrivateFactScheduleCrossesExpectedResponse(
                request_id.clone(),
            ));
        }
        if let Some((key, action)) = self.pending_actions.first_key_value()
            && key.effective_processing_at_utc_ms <= received_at_utc_ms
        {
            return Err(SessionError::PrivateFactScheduleCrossesPendingAction(
                action.global_request_id.clone(),
            ));
        }
        if self
            .closed_action_through_utc_ms
            .is_none_or(|closed| closed < received_at_utc_ms)
        {
            return Err(SessionError::PrivateFactScheduleCrossesPendingPlatformWork(
                format!("LOGICAL_PLATFORM:{received_at_utc_ms}"),
            ));
        }
        Ok(())
    }

    fn next_private_fact_received_at_through(&self, received_at_utc_ms: i64) -> Option<i64> {
        self.pending_private_facts
            .first_key_value()
            .map(|(key, _)| key.received_at_utc_ms)
            .filter(|next| *next <= received_at_utc_ms)
    }

    fn expect_direct_response(&mut self, global_request_id: &str) -> Result<(), SessionError> {
        if self
            .expected_direct_response_request_ids
            .contains(global_request_id)
        {
            return Ok(());
        }
        let expected_at = self.expected_direct_response_received_at(global_request_id)?;
        if self
            .private_fact_schedule_closed_through_utc_ms
            .is_some_and(|closed| expected_at <= closed)
        {
            return Err(SessionError::PrivateFactScheduleCrossesExpectedResponse(
                global_request_id.to_owned(),
            ));
        }
        self.expected_direct_response_request_ids
            .insert(global_request_id.to_owned());
        Ok(())
    }

    fn expected_direct_response_received_at(
        &self,
        global_request_id: &str,
    ) -> Result<i64, SessionError> {
        let record = self
            .requests
            .get(global_request_id)
            .ok_or_else(|| SessionError::RequestNotFound(global_request_id.to_owned()))?;
        let responded_at = self
            .action_keys
            .values()
            .filter(|(_, request_id)| request_id == global_request_id)
            .map(|(key, _)| key.effective_processing_at_utc_ms)
            .max()
            .ok_or(SessionError::ActionScheduleEmpty)?;
        checked_add_delay(responded_at, record.transit.inbound_delay_ms)
    }

    fn next_private_emission_sequence(&self) -> Result<u64, SessionError> {
        self.used_private_emission_sequences
            .last()
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(SessionError::CursorRegression("私有事实发射序号溢出"))
    }

    fn prepare_next_private_fact_through(
        &self,
        received_at_utc_ms: i64,
    ) -> Result<Option<VisiblePrivateFactV1>, SessionError> {
        self.prepare_next_private_fact_through_with_schedule(
            received_at_utc_ms,
            self.private_fact_schedule_closed_through_utc_ms,
        )
    }

    fn prepare_next_private_fact_through_with_schedule(
        &self,
        received_at_utc_ms: i64,
        schedule_closed_through_utc_ms: Option<i64>,
    ) -> Result<Option<VisiblePrivateFactV1>, SessionError> {
        if schedule_closed_through_utc_ms.is_none_or(|closed| closed < received_at_utc_ms) {
            return Err(SessionError::PrivateFactScheduleNotClosed);
        }
        if self
            .private_visibility_closed_through_utc_ms
            .is_some_and(|closed| received_at_utc_ms < closed)
        {
            return Err(SessionError::CursorRegression("私有事实可见水位"));
        }
        let Some((key, evidence)) = self.pending_private_facts.first_key_value() else {
            return Ok(None);
        };
        if key.received_at_utc_ms > received_at_utc_ms {
            return Ok(None);
        }
        let private_feedback_sequence = self
            .private_feedback_sequence
            .checked_add(1)
            .ok_or(SessionError::CursorRegression("私有反馈序号溢出"))?;
        Ok(Some(VisiblePrivateFactV1 {
            evidence: evidence.clone(),
            received_at_utc_ms: key.received_at_utc_ms,
            private_emission_sequence: key.private_emission_sequence,
            private_feedback_sequence,
        }))
    }

    fn prepare_private_fact_slice_v2(
        &self,
        received_at_utc_ms: i64,
    ) -> Result<PreparedPrivateFactSliceV2, SessionError> {
        let schedule_closed_through_before_utc_ms =
            self.private_fact_schedule_closed_through_utc_ms;
        if schedule_closed_through_before_utc_ms.is_some_and(|closed| received_at_utc_ms < closed) {
            self.validate_private_fact_schedule_closure(received_at_utc_ms)?;
        }
        let next_private_at = self.next_private_fact_received_at_through(received_at_utc_ms);
        let close_through_utc_ms = match (schedule_closed_through_before_utc_ms, next_private_at) {
            (Some(closed), Some(next)) if next <= closed => closed,
            (Some(_), Some(next)) => next,
            (Some(closed), None) if closed >= received_at_utc_ms => closed,
            (_, Some(next)) => next,
            _ => received_at_utc_ms,
        };
        let schedule_advanced = schedule_closed_through_before_utc_ms != Some(close_through_utc_ms);
        if schedule_advanced {
            self.validate_private_fact_schedule_closure(close_through_utc_ms)?;
        }
        let candidate = self.prepare_next_private_fact_through_with_schedule(
            close_through_utc_ms,
            Some(close_through_utc_ms),
        )?;
        if let Some(candidate) = &candidate {
            self.validate_prepared_private_fact_with_schedule(
                candidate,
                Some(close_through_utc_ms),
            )?;
        }
        Ok(PreparedPrivateFactSliceV2 {
            session_id: self.spec.session_id.clone(),
            schedule_closed_through_before_utc_ms,
            close_through_utc_ms,
            schedule_advanced,
            candidate,
        })
    }

    fn confirm_prepared_private_fact(
        &mut self,
        candidate: &VisiblePrivateFactV1,
    ) -> Result<(), SessionError> {
        self.validate_prepared_private_fact(candidate)?;
        self.commit_prevalidated_private_fact(candidate);
        Ok(())
    }

    fn validate_prepared_private_fact(
        &self,
        candidate: &VisiblePrivateFactV1,
    ) -> Result<(), SessionError> {
        self.validate_prepared_private_fact_with_schedule(
            candidate,
            self.private_fact_schedule_closed_through_utc_ms,
        )
    }

    fn validate_prepared_private_fact_with_schedule(
        &self,
        candidate: &VisiblePrivateFactV1,
        schedule_closed_through_utc_ms: Option<i64>,
    ) -> Result<(), SessionError> {
        let expected = self
            .prepare_next_private_fact_through_with_schedule(
                candidate.received_at_utc_ms,
                schedule_closed_through_utc_ms,
            )?
            .ok_or(SessionError::PrivateFactConfirmationConflict)?;
        if &expected != candidate {
            return Err(SessionError::PrivateFactConfirmationConflict);
        }
        let identity = candidate.evidence.identity();
        let key = self
            .private_fact_keys
            .get(&identity)
            .ok_or(SessionError::PrivateFactConfirmationConflict)?;
        if self.pending_private_facts.get(key) != Some(&candidate.evidence) {
            return Err(SessionError::PrivateFactConfirmationConflict);
        }
        if let PrivateVisibilityEvidenceV1::LifecycleFeedback { feedback_id } = &candidate.evidence
        {
            let timing = self
                .lifecycle_feedback
                .get(feedback_id)
                .ok_or(SessionError::PrivateFactConfirmationConflict)?;
            if timing.feedback_received_at_utc_ms != candidate.received_at_utc_ms
                || timing.private_emission_sequence != candidate.private_emission_sequence
            {
                return Err(SessionError::PrivateFactConfirmationConflict);
            }
        }
        Ok(())
    }

    fn commit_prevalidated_private_fact_slice_v2(
        &mut self,
        prepared: PreparedPrivateFactSliceV2,
    ) -> Option<VisiblePrivateFactV1> {
        let PreparedPrivateFactSliceV2 {
            session_id,
            schedule_closed_through_before_utc_ms,
            close_through_utc_ms,
            schedule_advanced,
            candidate,
        } = prepared;
        debug_assert_eq!(self.spec.session_id, session_id);
        debug_assert_eq!(
            self.private_fact_schedule_closed_through_utc_ms,
            schedule_closed_through_before_utc_ms
        );
        if schedule_advanced {
            debug_assert!(
                self.validate_private_fact_schedule_closure(close_through_utc_ms)
                    .is_ok()
            );
            self.private_fact_schedule_closed_through_utc_ms = Some(close_through_utc_ms);
        } else {
            debug_assert_eq!(
                self.private_fact_schedule_closed_through_utc_ms,
                Some(close_through_utc_ms)
            );
        }
        if let Some(candidate) = candidate {
            self.commit_prevalidated_private_fact(&candidate);
            Some(candidate)
        } else {
            None
        }
    }

    fn commit_prevalidated_private_fact(&mut self, candidate: &VisiblePrivateFactV1) {
        debug_assert!(self.validate_prepared_private_fact(candidate).is_ok());
        let identity = candidate.evidence.identity();
        let key = self
            .private_fact_keys
            .remove(&identity)
            .expect("已验证私有事实必须保留身份键");
        self.pending_private_facts
            .remove(&key)
            .expect("已验证私有事实必须仍是统一队头");
        if let PrivateVisibilityEvidenceV1::LifecycleFeedback { feedback_id } = &candidate.evidence
        {
            self.lifecycle_feedback
                .remove(feedback_id)
                .expect("已验证生命周期反馈必须仍保留时序绑定");
        }
        self.private_feedback_sequence = candidate.private_feedback_sequence;
        self.private_fact_received_at_by_sequence
            .push(candidate.received_at_utc_ms);
        self.private_visibility_closed_through_utc_ms = Some(
            self.private_visibility_closed_through_utc_ms
                .map_or(candidate.received_at_utc_ms, |closed| {
                    closed.max(candidate.received_at_utc_ms)
                }),
        );
    }

    fn take_private_facts_through(
        &mut self,
        received_at_utc_ms: i64,
    ) -> Result<Vec<VisiblePrivateFactV1>, SessionError> {
        let mut visible = Vec::new();
        while let Some(candidate) = self.prepare_next_private_fact_through(received_at_utc_ms)? {
            self.confirm_prepared_private_fact(&candidate)?;
            visible.push(candidate);
        }
        self.private_visibility_closed_through_utc_ms = Some(received_at_utc_ms);
        Ok(visible)
    }

    fn register_request(
        &mut self,
        profile: &EmpiricalDelayProfile,
        context: RequestContextV1,
        canonical_request_sha256: Sha256Hex,
    ) -> Result<RegisteredRequestV1, SessionError> {
        let prepared =
            self.prepare_request_registration(profile, context, canonical_request_sha256)?;
        Ok(self.commit_prepared_request(prepared))
    }

    fn prepare_request_registration(
        &self,
        profile: &EmpiricalDelayProfile,
        context: RequestContextV1,
        canonical_request_sha256: Sha256Hex,
    ) -> Result<PreparedRequestRegistrationV1, SessionError> {
        validate_request_context(&self.spec, &context)?;
        if !is_sha256_hex(&canonical_request_sha256.0) {
            return Err(SessionError::InvalidRequest("规范请求摘要非法"));
        }
        if let Some(existing) = self.requests.get(&context.global_request_id) {
            if existing.context != context
                || existing.canonical_request_sha256 != canonical_request_sha256
            {
                return Err(SessionError::RequestIdentityConflict(
                    context.global_request_id,
                ));
            }
            return Ok(PreparedRequestRegistrationV1::PhysicalRetry(
                existing.transit.clone(),
            ));
        }
        validate_model_visible_cursor(self, &context.model_visible_cursor)?;
        validate_request_visible_time(self, &context)?;
        if self
            .last_caller_sequence
            .is_some_and(|last| context.caller_sequence <= last)
        {
            return Err(SessionError::NonMonotonicCallerSequence);
        }
        if self
            .last_request_sent_at_utc_ms
            .is_some_and(|last| context.request_sent_at_utc_ms < last)
        {
            return Err(SessionError::NonMonotonicRequestSentAt);
        }

        let outbound_delay_ms = profile.sample_delay_ms(
            self.spec.session_seed.0.as_bytes(),
            &context.global_request_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        let inbound_delay_ms = profile.sample_delay_ms(
            self.spec.session_seed.0.as_bytes(),
            &context.global_request_id,
            DelaySampleDomainV1::DirectResponseInbound,
        );
        let platform_arrived_at_utc_ms =
            checked_add_delay(context.request_sent_at_utc_ms, outbound_delay_ms)?;
        if self
            .closed_action_through_utc_ms
            .is_some_and(|closed| platform_arrived_at_utc_ms <= closed)
        {
            return Err(SessionError::ActionWatermarkClosed);
        }
        if self
            .applicable_platform_prefix_closed_through()
            .is_some_and(|closed| platform_arrived_at_utc_ms <= closed)
        {
            return Err(SessionError::RequestBehindPlatformPrefix);
        }
        let transit = RequestTransitV1 {
            request_sent_at_utc_ms: context.request_sent_at_utc_ms,
            outbound_delay_ms,
            platform_arrived_at_utc_ms,
            platform_responded_at_utc_ms: None,
            private_emission_sequence: None,
            inbound_delay_ms,
            response_received_at_utc_ms: None,
        };
        Ok(PreparedRequestRegistrationV1::New(Box::new(
            RequestRecord {
                context,
                canonical_request_sha256,
                transit,
                response_transit: None,
                remaining_pending_action_count: 0,
            },
        )))
    }

    fn commit_prepared_request(
        &mut self,
        prepared: PreparedRequestRegistrationV1,
    ) -> RegisteredRequestV1 {
        match prepared {
            PreparedRequestRegistrationV1::New(record) => {
                let record = *record;
                let registered = RegisteredRequestV1 {
                    disposition: RequestRegistrationDispositionV1::NewLogicalRequest,
                    transit: record.transit.clone(),
                };
                self.last_caller_sequence = Some(record.context.caller_sequence);
                self.last_request_sent_at_utc_ms = Some(record.context.request_sent_at_utc_ms);
                self.requests
                    .insert(record.context.global_request_id.clone(), record);
                registered
            }
            PreparedRequestRegistrationV1::PhysicalRetry(transit) => RegisteredRequestV1 {
                disposition: RequestRegistrationDispositionV1::PhysicalRetry,
                transit,
            },
        }
    }

    fn register_and_schedule_request(
        &mut self,
        profile: &EmpiricalDelayProfile,
        context: RequestContextV1,
        canonical_request_sha256: Sha256Hex,
        actions: Vec<LogicalActionPlanV1>,
    ) -> Result<RegisteredLogicalScheduleV1, SessionError> {
        let prepared =
            self.prepare_request_schedule(profile, context, canonical_request_sha256, actions)?;
        Ok(self.commit_prevalidated_request_schedule(prepared))
    }

    fn prepare_request_schedule(
        &self,
        profile: &EmpiricalDelayProfile,
        context: RequestContextV1,
        canonical_request_sha256: Sha256Hex,
        actions: Vec<LogicalActionPlanV1>,
    ) -> Result<PreparedRequestScheduleV1, SessionError> {
        let global_request_id = context.global_request_id.clone();
        let caller_sequence = context.caller_sequence;
        let prepared_request =
            self.prepare_request_registration(profile, context, canonical_request_sha256)?;
        self.prepare_request_schedule_from_registration(
            global_request_id,
            caller_sequence,
            prepared_request,
            actions,
        )
    }

    fn prepare_registered_request_retry_schedule(
        &self,
        context: &RequestContextV1,
        actions: Vec<LogicalActionPlanV1>,
    ) -> Result<PreparedRequestScheduleV1, SessionError> {
        validate_request_context(&self.spec, context)?;
        let existing = self
            .requests
            .get(&context.global_request_id)
            .ok_or_else(|| SessionError::RequestNotFound(context.global_request_id.clone()))?;
        if existing.context != *context {
            return Err(SessionError::RequestIdentityConflict(
                context.global_request_id.clone(),
            ));
        }
        self.prepare_request_schedule_from_registration(
            context.global_request_id.clone(),
            context.caller_sequence,
            PreparedRequestRegistrationV1::PhysicalRetry(existing.transit.clone()),
            actions,
        )
    }

    fn prepare_request_schedule_from_registration(
        &self,
        global_request_id: String,
        caller_sequence: u64,
        mut prepared_request: PreparedRequestRegistrationV1,
        actions: Vec<LogicalActionPlanV1>,
    ) -> Result<PreparedRequestScheduleV1, SessionError> {
        let registered_request = prepared_request.registered_request();
        let prepared_actions = self.prepare_logical_schedule(
            &global_request_id,
            caller_sequence,
            &registered_request,
            &actions,
        )?;

        if let PreparedRequestRegistrationV1::New(record) = &mut prepared_request {
            self.validate_direct_response_expectation(
                &global_request_id,
                &registered_request.transit,
                &prepared_actions,
            )?;
            record.remaining_pending_action_count = u64::try_from(prepared_actions.len())
                .expect("连续 u16 批量项序号保证待处理动作数量适合 u64");
        }

        Ok(PreparedRequestScheduleV1 {
            request_registration: prepared_request,
            schedule: RegisteredLogicalScheduleV1 {
                request: registered_request,
                actions: prepared_actions,
            },
        })
    }

    fn validate_direct_response_expectation(
        &self,
        global_request_id: &str,
        transit: &RequestTransitV1,
        actions: &[QueuedLogicalActionV1],
    ) -> Result<(), SessionError> {
        let responded_at_utc_ms = actions
            .iter()
            .map(|action| action.key.effective_processing_at_utc_ms)
            .max()
            .ok_or(SessionError::ActionScheduleEmpty)?;
        let expected_at_utc_ms = checked_add_delay(responded_at_utc_ms, transit.inbound_delay_ms)?;
        if self
            .private_fact_schedule_closed_through_utc_ms
            .is_some_and(|closed| expected_at_utc_ms <= closed)
        {
            return Err(SessionError::PrivateFactScheduleCrossesExpectedResponse(
                global_request_id.to_owned(),
            ));
        }
        Ok(())
    }

    fn commit_prevalidated_request_schedule(
        &mut self,
        prepared: PreparedRequestScheduleV1,
    ) -> RegisteredLogicalScheduleV1 {
        let PreparedRequestScheduleV1 {
            request_registration,
            schedule,
        } = prepared;
        let is_new = matches!(request_registration, PreparedRequestRegistrationV1::New(_));
        let committed_request = self.commit_prepared_request(request_registration);
        debug_assert_eq!(committed_request, schedule.request);
        let global_request_id = schedule
            .actions
            .first()
            .expect("prevalidated request schedule is non-empty")
            .global_request_id
            .clone();
        if is_new {
            for action in &schedule.actions {
                self.action_keys.insert(
                    action.action_identity.clone(),
                    (action.key, action.global_request_id.clone()),
                );
                self.pending_actions.insert(action.key, action.clone());
            }
            self.sealed_request_ids.insert(global_request_id.clone());
            self.expected_direct_response_request_ids
                .insert(global_request_id);
        }
        schedule
    }

    fn prepare_logical_schedule(
        &self,
        global_request_id: &str,
        caller_sequence: u64,
        registered_request: &RegisteredRequestV1,
        actions: &[LogicalActionPlanV1],
    ) -> Result<Vec<QueuedLogicalActionV1>, SessionError> {
        if actions.is_empty() {
            return Err(SessionError::ActionScheduleEmpty);
        }
        if actions.iter().enumerate().any(|(expected, action)| {
            usize::from(action.batch_item_ordinal) != expected || action.action_identity.is_empty()
        }) {
            return Err(SessionError::InvalidRequest(
                "批量项序号必须从零开始且连续，动作身份不得为空",
            ));
        }
        let mut identities = BTreeSet::new();
        if actions
            .iter()
            .any(|action| !identities.insert(action.action_identity.as_str()))
        {
            return Err(SessionError::InvalidRequest("逻辑动作身份重复"));
        }

        let physical_retry =
            registered_request.disposition == RequestRegistrationDispositionV1::PhysicalRetry;
        let mut prepared = Vec::with_capacity(actions.len());
        for action in actions {
            let key = LogicalActionKeyV1 {
                effective_processing_at_utc_ms: checked_add_delay(
                    registered_request.transit.platform_arrived_at_utc_ms,
                    action.processing_delay_ms,
                )?,
                caller_sequence,
                batch_item_ordinal: action.batch_item_ordinal,
            };
            if let Some((existing_key, existing_request_id)) =
                self.action_keys.get(&action.action_identity)
            {
                if physical_retry
                    && *existing_key == key
                    && existing_request_id == global_request_id
                {
                    prepared.push(QueuedLogicalActionV1 {
                        action_identity: action.action_identity.clone(),
                        global_request_id: global_request_id.to_owned(),
                        key,
                    });
                    continue;
                }
                return Err(SessionError::ActionIdentityConflict(
                    action.action_identity.clone(),
                ));
            }
            if physical_retry {
                return Err(SessionError::ActionIdentityConflict(
                    action.action_identity.clone(),
                ));
            }
            if self
                .applicable_platform_prefix_closed_through()
                .is_some_and(|closed| key.effective_processing_at_utc_ms < closed)
            {
                return Err(SessionError::RequestBehindPlatformPrefix);
            }
            if self
                .closed_action_through_utc_ms
                .is_some_and(|closed| key.effective_processing_at_utc_ms <= closed)
            {
                return Err(SessionError::ActionWatermarkClosed);
            }
            if self.pending_actions.contains_key(&key) {
                return Err(SessionError::ActionIdentityConflict(
                    action.action_identity.clone(),
                ));
            }
            prepared.push(QueuedLogicalActionV1 {
                action_identity: action.action_identity.clone(),
                global_request_id: global_request_id.to_owned(),
                key,
            });
        }

        if physical_retry && !self.sealed_request_ids.contains(global_request_id) {
            return Err(SessionError::ActionScheduleNotSealed(
                global_request_id.to_owned(),
            ));
        }
        if !physical_retry && self.sealed_request_ids.contains(global_request_id) {
            return Err(SessionError::ActionScheduleSealed(
                global_request_id.to_owned(),
            ));
        }
        Ok(prepared)
    }

    fn complete_direct_response(
        &mut self,
        global_request_id: &str,
        platform_responded_at_utc_ms: i64,
        private_emission_sequence: u64,
    ) -> Result<RequestTransitV1, SessionError> {
        if !self.released_request_ids.contains(global_request_id) {
            return Err(SessionError::RequestNotLogicallyReleased(
                global_request_id.to_owned(),
            ));
        }
        let record = self
            .requests
            .get(global_request_id)
            .ok_or_else(|| SessionError::RequestNotFound(global_request_id.to_owned()))?;
        if let Some(existing) = &record.response_transit {
            if existing.platform_responded_at_utc_ms == Some(platform_responded_at_utc_ms)
                && existing.private_emission_sequence == Some(private_emission_sequence)
            {
                return Ok(existing.clone());
            }
            return Err(SessionError::RequestIdentityConflict(
                global_request_id.to_owned(),
            ));
        }
        let registration_transit = record.transit.clone();
        if platform_responded_at_utc_ms < registration_transit.platform_arrived_at_utc_ms {
            return Err(SessionError::ResponseBeforeArrival);
        }
        let last_action_at = self
            .action_keys
            .values()
            .filter(|(_, request_id)| request_id == global_request_id)
            .map(|(key, _)| key.effective_processing_at_utc_ms)
            .max()
            .ok_or_else(|| {
                SessionError::RequestNotLogicallyReleased(global_request_id.to_owned())
            })?;
        if platform_responded_at_utc_ms < last_action_at {
            return Err(SessionError::ResponseBeforeLogicalCompletion);
        }
        let response_received_at_utc_ms = checked_add_delay(
            platform_responded_at_utc_ms,
            registration_transit.inbound_delay_ms,
        )?;
        let evidence = PrivateVisibilityEvidenceV1::DirectResponse {
            global_request_id: global_request_id.to_owned(),
        };
        self.register_private_fact(
            evidence,
            response_received_at_utc_ms,
            private_emission_sequence,
        )?;
        let mut response_transit = registration_transit;
        response_transit.platform_responded_at_utc_ms = Some(platform_responded_at_utc_ms);
        response_transit.private_emission_sequence = Some(private_emission_sequence);
        response_transit.response_received_at_utc_ms = Some(response_received_at_utc_ms);
        self.requests
            .get_mut(global_request_id)
            .ok_or_else(|| SessionError::RequestNotFound(global_request_id.to_owned()))?
            .response_transit = Some(response_transit.clone());
        let removed = self
            .expected_direct_response_request_ids
            .remove(global_request_id);
        debug_assert!(removed);
        Ok(response_transit)
    }

    fn bind_lifecycle_feedback_timing(
        &mut self,
        profile: &EmpiricalDelayProfile,
        feedback_id: String,
        platform_event_at_utc_ms: i64,
        private_emission_sequence: u64,
    ) -> Result<LifecycleFeedbackTimingV1, SessionError> {
        if feedback_id.is_empty() {
            return Err(SessionError::InvalidRequest("反馈身份为空"));
        }
        if let Some(existing) = self.lifecycle_feedback.get(&feedback_id) {
            if existing.platform_event_at_utc_ms == platform_event_at_utc_ms
                && existing.private_emission_sequence == private_emission_sequence
            {
                return Ok(existing.clone());
            }
            return Err(SessionError::FeedbackIdentityConflict(feedback_id));
        }
        let inbound_delay_ms = profile.sample_delay_ms(
            self.spec.session_seed.0.as_bytes(),
            &feedback_id,
            DelaySampleDomainV1::LifecycleFeedbackInbound,
        );
        let feedback_received_at_utc_ms =
            checked_add_delay(platform_event_at_utc_ms, inbound_delay_ms)?;
        let timing = LifecycleFeedbackTimingV1 {
            feedback_id: feedback_id.clone(),
            platform_event_at_utc_ms,
            private_emission_sequence,
            inbound_delay_ms,
            feedback_received_at_utc_ms,
        };
        self.register_private_fact(
            PrivateVisibilityEvidenceV1::LifecycleFeedback {
                feedback_id: feedback_id.clone(),
            },
            feedback_received_at_utc_ms,
            private_emission_sequence,
        )?;
        self.lifecycle_feedback.insert(feedback_id, timing.clone());
        Ok(timing)
    }

    fn register_private_fact(
        &mut self,
        evidence: PrivateVisibilityEvidenceV1,
        received_at_utc_ms: i64,
        private_emission_sequence: u64,
    ) -> Result<(), SessionError> {
        if self
            .private_fact_schedule_closed_through_utc_ms
            .is_some_and(|closed| received_at_utc_ms <= closed)
        {
            return Err(SessionError::PrivateFactScheduleClosed);
        }
        let identity = evidence.identity();
        let key = PrivateFactOrderKey {
            received_at_utc_ms,
            private_emission_sequence,
            identity: identity.clone(),
        };
        if let Some(existing) = self.private_fact_keys.get(&identity) {
            if existing == &key {
                return Ok(());
            }
            return Err(SessionError::PrivateFactIdentityConflict(identity));
        }
        if !self
            .used_private_emission_sequences
            .insert(private_emission_sequence)
        {
            return Err(SessionError::PrivateEmissionSequenceConflict(
                private_emission_sequence,
            ));
        }
        self.private_fact_keys.insert(identity, key.clone());
        self.pending_private_facts.insert(key, evidence);
        Ok(())
    }

    fn enqueue_logical_action(
        &mut self,
        action_identity: String,
        global_request_id: String,
        key: LogicalActionKeyV1,
    ) -> Result<bool, SessionError> {
        if action_identity.is_empty() {
            return Err(SessionError::InvalidRequest("逻辑动作身份为空"));
        }
        let request = self
            .requests
            .get(&global_request_id)
            .ok_or_else(|| SessionError::RequestNotFound(global_request_id.clone()))?;
        if key.caller_sequence != request.context.caller_sequence
            || key.effective_processing_at_utc_ms < request.transit.platform_arrived_at_utc_ms
        {
            return Err(SessionError::InvalidRequest("逻辑动作键与请求绑定不一致"));
        }
        if self
            .applicable_platform_prefix_closed_through()
            .is_some_and(|closed| key.effective_processing_at_utc_ms < closed)
        {
            return Err(SessionError::RequestBehindPlatformPrefix);
        }
        if self
            .closed_action_through_utc_ms
            .is_some_and(|closed| key.effective_processing_at_utc_ms <= closed)
        {
            return Err(SessionError::ActionWatermarkClosed);
        }
        if let Some((existing_key, existing_request_id)) = self.action_keys.get(&action_identity) {
            if *existing_key == key && existing_request_id == &global_request_id {
                return Ok(false);
            }
            return Err(SessionError::ActionIdentityConflict(action_identity));
        }
        if self.sealed_request_ids.contains(&global_request_id) {
            return Err(SessionError::ActionScheduleSealed(global_request_id));
        }
        if self.pending_actions.contains_key(&key) {
            return Err(SessionError::ActionIdentityConflict(action_identity));
        }
        let remaining_pending_action_count = request
            .remaining_pending_action_count
            .checked_add(1)
            .expect("受 u16 批量项序号约束的待处理动作计数不会溢出");
        self.requests
            .get_mut(&global_request_id)
            .expect("已核对逻辑动作所属请求存在")
            .remaining_pending_action_count = remaining_pending_action_count;
        self.action_keys
            .insert(action_identity.clone(), (key, global_request_id.clone()));
        self.pending_actions.insert(
            key,
            QueuedLogicalActionV1 {
                action_identity,
                global_request_id,
                key,
            },
        );
        Ok(true)
    }

    fn seal_logical_actions(&mut self, global_request_id: &str) -> Result<(), SessionError> {
        if !self.requests.contains_key(global_request_id) {
            return Err(SessionError::RequestNotFound(global_request_id.to_owned()));
        }
        if self.sealed_request_ids.contains(global_request_id) {
            return Ok(());
        }
        let mut ordinals = self
            .pending_actions
            .values()
            .filter(|action| action.global_request_id == global_request_id)
            .map(|action| action.key.batch_item_ordinal)
            .collect::<Vec<_>>();
        if ordinals.is_empty() {
            return Err(SessionError::ActionScheduleEmpty);
        }
        ordinals.sort_unstable();
        if ordinals
            .iter()
            .copied()
            .enumerate()
            .any(|(expected, actual)| usize::from(actual) != expected)
        {
            return Err(SessionError::InvalidRequest("批量项序号必须从零开始且连续"));
        }
        self.sealed_request_ids.insert(global_request_id.to_owned());
        Ok(())
    }

    fn prepare_closed_bucket_actionless_slice_v2<'a>(
        &'a mut self,
        latency_profile: &'a EmpiricalDelayProfile,
        path: MarketEventPathV1,
        bucket_at_utc_ms: i64,
        next_index_position: u64,
        input_seal_after: Option<InputPathSealOutcomeV1>,
    ) -> Result<PreparedExecutionActionSliceV2<'a>, SessionError> {
        if let Some(identity) = &self.in_flight_action_identity {
            return Err(SessionError::ActionProcessingInProgress(identity.clone()));
        }
        match path {
            MarketEventPathV1::HiddenPlatform => {
                self.validate_platform_prefix_closure(bucket_at_utc_ms, next_index_position)?;
            }
            MarketEventPathV1::PublicVisibility => {
                self.validate_public_visibility_closure(bucket_at_utc_ms, next_index_position)?;
            }
        }
        if self
            .closed_action_through_utc_ms
            .is_some_and(|closed| bucket_at_utc_ms < closed)
        {
            return Err(SessionError::ActionWatermarkClosed);
        }
        let closed_action_through_before_utc_ms = self.closed_action_through_utc_ms;
        let platform_state_revision = self.platform_state_revision;
        Ok(PreparedExecutionActionSliceV2 {
            state: self,
            latency_profile,
            closed_action_through_before_utc_ms,
            closed_action_through_after_utc_ms: bucket_at_utc_ms,
            actions: Vec::new(),
            next_action_index: 0,
            completed_request_id: None,
            prepared_request_pending_action_count: None,
            platform_state_revision_before: platform_state_revision,
            platform_state_revision_after: platform_state_revision,
            prepared_mutations: Vec::new(),
            prepared_queries: Vec::new(),
            prepared_lifecycle_feedback: Vec::new(),
            prepared_private_facts: Vec::new(),
            prepared_used_private_emission_sequences: Vec::new(),
            prepared_direct_response: None,
            prepared_closed_bucket: Some(PreparedClosedBucketStateV2 {
                path,
                bucket_at_utc_ms,
                next_index_position,
                input_seal_after,
            }),
            preparation_failed: false,
        })
    }

    fn prepare_closed_bucket_metadata_delta_v2(
        &mut self,
        path: MarketEventPathV1,
        bucket_at_utc_ms: i64,
        next_index_position: u64,
        input_seal_after: Option<InputPathSealOutcomeV1>,
    ) -> Result<PreparedClosedBucketMetadataDeltaV2<'_>, SessionError> {
        match path {
            MarketEventPathV1::HiddenPlatform => {
                self.validate_platform_prefix_closure(bucket_at_utc_ms, next_index_position)?;
            }
            MarketEventPathV1::PublicVisibility => {
                self.validate_public_visibility_closure(bucket_at_utc_ms, next_index_position)?;
            }
        }
        Ok(PreparedClosedBucketMetadataDeltaV2 {
            state: self,
            path,
            cursor_after: Some((bucket_at_utc_ms, next_index_position)),
            input_seal_after,
        })
    }

    fn prepare_closed_bucket_seal_upgrade_delta_v2(
        &mut self,
        path: MarketEventPathV1,
        input_seal_after: InputPathSealOutcomeV1,
    ) -> PreparedClosedBucketMetadataDeltaV2<'_> {
        let input_seal_after = self
            .input_path_seal(path)
            .is_none()
            .then_some(input_seal_after);
        PreparedClosedBucketMetadataDeltaV2 {
            state: self,
            path,
            cursor_after: None,
            input_seal_after,
        }
    }

    fn prepare_execution_action_slice_v2<'a>(
        &'a mut self,
        latency_profile: &'a EmpiricalDelayProfile,
        effective_processing_at_utc_ms: i64,
        complete_same_request_at_same_time: bool,
    ) -> Result<PreparedExecutionActionSliceV2<'a>, SessionError> {
        if let Some(identity) = &self.in_flight_action_identity {
            return Err(SessionError::ActionProcessingInProgress(identity.clone()));
        }
        let applicable_prefix_closed = match self.spec.caller_role {
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
                self.hidden_input_seal.is_some()
                    || self
                        .platform_prefix
                        .closed_through_source_event_at_utc_ms
                        .is_some_and(|closed| closed >= effective_processing_at_utc_ms)
            }
            SimCallerRoleV1::Paper => {
                self.public_input_seal.is_some()
                    || self
                        .public_visibility
                        .closed_through_received_at_utc_ms
                        .is_some_and(|closed| closed >= effective_processing_at_utc_ms)
            }
        };
        if !applicable_prefix_closed {
            return Err(SessionError::PlatformPrefixNotClosed);
        }
        if self
            .closed_action_through_utc_ms
            .is_some_and(|closed| effective_processing_at_utc_ms < closed)
        {
            return Err(SessionError::ActionWatermarkClosed);
        }

        let closed_action_through_before_utc_ms = self.closed_action_through_utc_ms;
        let platform_state_revision = self.platform_state_revision;
        let Some(first_action) = self
            .pending_actions
            .first_key_value()
            .map(|(_, action)| action.clone())
        else {
            return Ok(PreparedExecutionActionSliceV2 {
                state: self,
                latency_profile,
                closed_action_through_before_utc_ms,
                closed_action_through_after_utc_ms: effective_processing_at_utc_ms,
                actions: Vec::new(),
                next_action_index: 0,
                completed_request_id: None,
                prepared_request_pending_action_count: None,
                platform_state_revision_before: platform_state_revision,
                platform_state_revision_after: platform_state_revision,
                prepared_mutations: Vec::new(),
                prepared_queries: Vec::new(),
                prepared_lifecycle_feedback: Vec::new(),
                prepared_private_facts: Vec::new(),
                prepared_used_private_emission_sequences: Vec::new(),
                prepared_direct_response: None,
                prepared_closed_bucket: None,
                preparation_failed: false,
            });
        };
        if first_action.key.effective_processing_at_utc_ms > effective_processing_at_utc_ms {
            return Ok(PreparedExecutionActionSliceV2 {
                state: self,
                latency_profile,
                closed_action_through_before_utc_ms,
                closed_action_through_after_utc_ms: effective_processing_at_utc_ms,
                actions: Vec::new(),
                next_action_index: 0,
                completed_request_id: None,
                prepared_request_pending_action_count: None,
                platform_state_revision_before: platform_state_revision,
                platform_state_revision_after: platform_state_revision,
                prepared_mutations: Vec::new(),
                prepared_queries: Vec::new(),
                prepared_lifecycle_feedback: Vec::new(),
                prepared_private_facts: Vec::new(),
                prepared_used_private_emission_sequences: Vec::new(),
                prepared_direct_response: None,
                prepared_closed_bucket: None,
                preparation_failed: false,
            });
        }

        let actions = if complete_same_request_at_same_time {
            let Some(request) = self.requests.get(&first_action.global_request_id) else {
                return Err(SessionError::ActionIdentityConflict(
                    first_action.action_identity,
                ));
            };
            let request_pending_action_count =
                usize::try_from(request.remaining_pending_action_count)
                    .expect("运行时请求待处理动作计数来自当前内存映射");
            let actions = self
                .pending_actions
                .values()
                .take(request_pending_action_count)
                .inspect(|_| record_pending_action_entry_visit_for_tests())
                .cloned()
                .collect::<Vec<_>>();
            if actions.is_empty()
                || actions.len() != request_pending_action_count
                || actions.iter().enumerate().any(|(offset, action)| {
                    let Ok(offset) = u16::try_from(offset) else {
                        return true;
                    };
                    action.global_request_id != first_action.global_request_id
                        || action.key.effective_processing_at_utc_ms
                            != first_action.key.effective_processing_at_utc_ms
                        || action.key.caller_sequence != first_action.key.caller_sequence
                        || first_action.key.batch_item_ordinal.checked_add(offset)
                            != Some(action.key.batch_item_ordinal)
                })
            {
                return Err(SessionError::InvalidRequest(
                    "同请求全批动作必须同刻、同调用方序列且序号连续",
                ));
            }
            actions
        } else {
            vec![first_action]
        };

        for action in &actions {
            if !self.sealed_request_ids.contains(&action.global_request_id) {
                return Err(SessionError::ActionScheduleNotSealed(
                    action.global_request_id.clone(),
                ));
            }
            if !self.requests.contains_key(&action.global_request_id)
                || self.pending_actions.get(&action.key) != Some(action)
                || self.released_action_ids.contains(&action.action_identity)
                || self
                    .action_keys
                    .get(&action.action_identity)
                    .is_none_or(|(key, request_id)| {
                        key != &action.key || request_id != &action.global_request_id
                    })
            {
                return Err(SessionError::ActionIdentityConflict(
                    action.action_identity.clone(),
                ));
            }
        }

        let global_request_id = actions
            .first()
            .expect("due execution action slice is non-empty")
            .global_request_id
            .clone();
        let request_pending_action_count = self
            .requests
            .get(&global_request_id)
            .expect("已核对执行动作所属请求存在")
            .remaining_pending_action_count;
        let prepared_action_count =
            u64::try_from(actions.len()).expect("当前执行动作切片数量适合 u64");
        let remaining_pending_action_count = request_pending_action_count
            .checked_sub(prepared_action_count)
            .expect("当前执行动作切片不得超过请求待处理动作计数");
        let completed_request_id =
            (remaining_pending_action_count == 0).then_some(global_request_id.clone());
        if completed_request_id
            .as_ref()
            .is_some_and(|request_id| self.released_request_ids.contains(request_id))
        {
            return Err(SessionError::RequestIdentityConflict(
                completed_request_id.expect("checked completed request identity"),
            ));
        }

        Ok(PreparedExecutionActionSliceV2 {
            state: self,
            latency_profile,
            closed_action_through_before_utc_ms,
            closed_action_through_after_utc_ms: actions[0].key.effective_processing_at_utc_ms,
            actions,
            next_action_index: 0,
            completed_request_id,
            prepared_request_pending_action_count: Some((
                global_request_id,
                remaining_pending_action_count,
            )),
            platform_state_revision_before: platform_state_revision,
            platform_state_revision_after: platform_state_revision,
            prepared_mutations: Vec::new(),
            prepared_queries: Vec::new(),
            prepared_lifecycle_feedback: Vec::new(),
            prepared_private_facts: Vec::new(),
            prepared_used_private_emission_sequences: Vec::new(),
            prepared_direct_response: None,
            prepared_closed_bucket: None,
            preparation_failed: false,
        })
    }

    fn complete_logical_item(&mut self, action_identity: &str) -> Result<bool, SessionError> {
        if self.in_flight_action_identity.as_deref() != Some(action_identity) {
            return Err(SessionError::ActionNotLogicallyReleased(
                action_identity.to_owned(),
            ));
        }
        let (_, global_request_id) = self
            .action_keys
            .get(action_identity)
            .cloned()
            .ok_or_else(|| SessionError::ActionNotLogicallyReleased(action_identity.to_owned()))?;
        self.in_flight_action_identity = None;
        let request_completed = self
            .requests
            .get(&global_request_id)
            .expect("已释放逻辑动作所属请求必须存在")
            .remaining_pending_action_count
            == 0;
        if request_completed {
            self.released_request_ids.insert(global_request_id);
        }
        Ok(request_completed)
    }

    fn first_pending_action_through(
        &self,
        effective_processing_at_utc_ms: i64,
    ) -> Option<QueuedLogicalActionV1> {
        self.pending_actions
            .first_key_value()
            .filter(|(key, _)| key.effective_processing_at_utc_ms <= effective_processing_at_utc_ms)
            .map(|(_, action)| action.clone())
    }

    fn take_next_action_through(
        &mut self,
        effective_processing_at_utc_ms: i64,
    ) -> Result<Option<QueuedLogicalActionV1>, SessionError> {
        if let Some(identity) = &self.in_flight_action_identity {
            return Err(SessionError::ActionProcessingInProgress(identity.clone()));
        }
        let applicable_prefix_closed = match self.spec.caller_role {
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
                self.hidden_input_seal.is_some()
                    || self
                        .platform_prefix
                        .closed_through_source_event_at_utc_ms
                        .is_some_and(|closed| closed >= effective_processing_at_utc_ms)
            }
            SimCallerRoleV1::Paper => {
                self.public_input_seal.is_some()
                    || self
                        .public_visibility
                        .closed_through_received_at_utc_ms
                        .is_some_and(|closed| closed >= effective_processing_at_utc_ms)
            }
        };
        if !applicable_prefix_closed {
            return Err(SessionError::PlatformPrefixNotClosed);
        }
        if self
            .closed_action_through_utc_ms
            .is_some_and(|closed| effective_processing_at_utc_ms < closed)
        {
            return Err(SessionError::ActionWatermarkClosed);
        }
        let Some((key, action)) = self
            .pending_actions
            .first_key_value()
            .map(|(key, action)| (*key, action.clone()))
        else {
            self.closed_action_through_utc_ms = Some(effective_processing_at_utc_ms);
            return Ok(None);
        };
        if key.effective_processing_at_utc_ms > effective_processing_at_utc_ms {
            self.closed_action_through_utc_ms = Some(effective_processing_at_utc_ms);
            return Ok(None);
        }
        if !self.sealed_request_ids.contains(&action.global_request_id) {
            return Err(SessionError::ActionScheduleNotSealed(
                action.global_request_id,
            ));
        }
        self.pending_actions
            .remove(&key)
            .expect("selected pending action key must still exist");
        let request = self
            .requests
            .get_mut(&action.global_request_id)
            .expect("已密封逻辑动作所属请求必须存在");
        request.remaining_pending_action_count = request
            .remaining_pending_action_count
            .checked_sub(1)
            .expect("待释放逻辑动作必须计入请求待处理动作计数");
        self.released_action_ids
            .insert(action.action_identity.clone());
        self.in_flight_action_identity = Some(action.action_identity.clone());
        self.closed_action_through_utc_ms = Some(key.effective_processing_at_utc_ms);
        Ok(Some(action))
    }

    fn validate_in_flight_action(
        &self,
        action: &QueuedLogicalActionV1,
    ) -> Result<(), SessionError> {
        if self.in_flight_action_identity.as_deref() != Some(&action.action_identity) {
            return Err(SessionError::ActionNotLogicallyReleased(
                action.action_identity.clone(),
            ));
        }
        let Some((registered_key, registered_request_id)) =
            self.action_keys.get(&action.action_identity)
        else {
            return Err(SessionError::ActionNotLogicallyReleased(
                action.action_identity.clone(),
            ));
        };
        if registered_key != &action.key || registered_request_id != &action.global_request_id {
            return Err(SessionError::ActionIdentityConflict(
                action.action_identity.clone(),
            ));
        }
        Ok(())
    }

    fn advance_platform_state_revision(&mut self, next_revision: u64) -> Result<(), SessionError> {
        if next_revision == self.platform_state_revision {
            return Ok(());
        }
        if next_revision <= self.platform_state_revision {
            return Err(SessionError::PlatformRevisionRegression);
        }
        self.platform_state_revision = next_revision;
        Ok(())
    }

    fn earliest_unsealed_request_at(&self) -> Option<i64> {
        self.requests
            .iter()
            .filter(|(request_id, _)| !self.sealed_request_ids.contains(*request_id))
            .map(|(_, record)| record.transit.platform_arrived_at_utc_ms)
            .min()
    }

    fn earliest_scheduled_action_at(&self) -> Option<i64> {
        let pending_action_at = self
            .pending_actions
            .first_key_value()
            .map(|(key, _)| key.effective_processing_at_utc_ms);
        let in_flight_action_at = self
            .in_flight_action_identity
            .as_ref()
            .and_then(|identity| self.action_keys.get(identity))
            .map(|(key, _)| key.effective_processing_at_utc_ms);
        pending_action_at
            .into_iter()
            .chain(in_flight_action_at)
            .min()
    }

    fn applicable_platform_prefix_closed_through(&self) -> Option<i64> {
        match self.spec.caller_role {
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
                self.platform_prefix.closed_through_source_event_at_utc_ms
            }
            SimCallerRoleV1::Paper => self.public_visibility.closed_through_received_at_utc_ms,
        }
    }

    fn resolve_mutation_idempotency(
        &mut self,
        global_request_id: &str,
        action_identity: &str,
        idempotency_key: String,
        payload_sha256: Sha256Hex,
        effect_identity: String,
    ) -> Result<MutationResolutionV1, SessionError> {
        if !self.requests.contains_key(global_request_id) {
            return Err(SessionError::RequestNotFound(global_request_id.to_owned()));
        }
        let action_request_id = self
            .action_keys
            .get(action_identity)
            .map(|(_, request_id)| request_id)
            .ok_or_else(|| SessionError::ActionNotLogicallyReleased(action_identity.to_owned()))?;
        if action_request_id != global_request_id
            || !self.released_action_ids.contains(action_identity)
        {
            return Err(SessionError::ActionNotLogicallyReleased(
                action_identity.to_owned(),
            ));
        }
        if idempotency_key.is_empty()
            || effect_identity.is_empty()
            || !is_sha256_hex(&payload_sha256.0)
        {
            return Err(SessionError::InvalidRequest("变更幂等绑定非法"));
        }
        if let Some(existing) = self.mutations.get(&idempotency_key) {
            if existing.payload_sha256 != payload_sha256 {
                return Err(SessionError::MutationIdentityConflict(idempotency_key));
            }
            return Ok(MutationResolutionV1 {
                disposition: MutationResolutionDispositionV1::ExistingEffect,
                effect_identity: existing.effect_identity.clone(),
            });
        }
        if self.in_flight_action_identity.as_deref() != Some(action_identity) {
            return Err(SessionError::ActionNotLogicallyReleased(
                action_identity.to_owned(),
            ));
        }
        self.mutations.insert(
            idempotency_key,
            MutationRecord {
                payload_sha256,
                effect_identity: effect_identity.clone(),
            },
        );
        Ok(MutationResolutionV1 {
            disposition: MutationResolutionDispositionV1::NewEffect,
            effect_identity,
        })
    }

    fn resolve_query_revision(
        &mut self,
        global_request_id: &str,
        action_identity: &str,
        query_sha256: Sha256Hex,
        frozen_revision: Option<u64>,
    ) -> Result<QueryRevisionV1, SessionError> {
        if !self.requests.contains_key(global_request_id) {
            return Err(SessionError::RequestNotFound(global_request_id.to_owned()));
        }
        let action_request_id = self
            .action_keys
            .get(action_identity)
            .map(|(_, request_id)| request_id)
            .ok_or_else(|| SessionError::ActionNotLogicallyReleased(action_identity.to_owned()))?;
        if action_request_id != global_request_id
            || !self.released_action_ids.contains(action_identity)
        {
            return Err(SessionError::ActionNotLogicallyReleased(
                action_identity.to_owned(),
            ));
        }
        if !is_sha256_hex(&query_sha256.0) {
            return Err(SessionError::InvalidRequest("查询摘要非法"));
        }
        if let Some(existing) = self.queries.get(global_request_id) {
            let revision_conflicts = frozen_revision
                .is_some_and(|revision| revision != existing.platform_state_revision);
            if existing.query_sha256 != query_sha256 || revision_conflicts {
                return Err(SessionError::QueryIdentityConflict(
                    global_request_id.to_owned(),
                ));
            }
            return Ok(QueryRevisionV1 {
                platform_state_revision: existing.platform_state_revision,
                reused: true,
            });
        }
        let selected_revision = match frozen_revision {
            Some(revision) if revision <= self.platform_state_revision => revision,
            Some(_) => {
                return Err(SessionError::QueryIdentityConflict(
                    global_request_id.to_owned(),
                ));
            }
            None => self.platform_state_revision,
        };
        if self.in_flight_action_identity.as_deref() != Some(action_identity) {
            return Err(SessionError::ActionNotLogicallyReleased(
                action_identity.to_owned(),
            ));
        }
        self.queries.insert(
            global_request_id.to_owned(),
            QueryRecord {
                query_sha256,
                platform_state_revision: selected_revision,
            },
        );
        Ok(QueryRevisionV1 {
            platform_state_revision: selected_revision,
            reused: false,
        })
    }
}

#[derive(Serialize)]
struct SessionSpecIdentityMaterialV2<'a> {
    schema_version: &'static str,
    run_id: &'a str,
    session_id: &'a str,
    account_id: &'a str,
    caller_role: SimCallerRoleV1,
    caller_instance_id: &'a str,
    creation_request_id: &'a str,
    response_bridge_binding_id: &'a str,
    initial_account_state_sha256: &'a Sha256Hex,
    response_outbox_capacity: u64,
    market_set_sha256: &'a Sha256Hex,
    scenario_sha256: &'a Sha256Hex,
    session_seed: &'a Sha256Hex,
    execution_contract_version: &'a str,
    market_event_schema_version: &'a str,
}

#[derive(Serialize)]
struct AccountOpeningAnchorIdentityMaterialV1<'a> {
    schema_version: &'static str,
    account_instance_key: &'a AccountInstanceKeyV1,
    session_id: &'a str,
    initial_scenario_sha256: &'a Sha256Hex,
    initial_market_set_sha256: &'a Sha256Hex,
    initial_account_state_sha256: &'a Sha256Hex,
    initial_platform_account: &'a PlatformAccountSnapshotV1,
}

#[derive(Serialize)]
struct AccountOpeningAnchorHashMaterialV1<'a> {
    schema_version: &'static str,
    anchor_id: &'a str,
    account_instance_key: &'a AccountInstanceKeyV1,
    session_id: &'a str,
    initial_scenario_sha256: &'a Sha256Hex,
    initial_market_set_sha256: &'a Sha256Hex,
    initial_account_state_sha256: &'a Sha256Hex,
    initial_platform_account: &'a PlatformAccountSnapshotV1,
}

#[derive(Serialize)]
struct MarketEnrollmentIdMaterialV1<'a> {
    schema_version: &'static str,
    enrollment_request_id: &'a str,
    account_instance_key: &'a AccountInstanceKeyV1,
    opening_anchor_id: &'a str,
    opening_anchor_sha256: &'a Sha256Hex,
    previous_enrollment: Option<&'a MarketEnrollmentIdentityV1>,
    market: &'a contracts::MarketIdentityV2,
    assets: &'a [contracts::AssetIdentityV1; 2],
    enrollment_chain_length: u64,
}

#[derive(Serialize)]
struct MarketEnrollmentHashMaterialV1<'a> {
    schema_version: &'static str,
    enrollment_id: &'a str,
    enrollment_request_id: &'a str,
    account_instance_key: &'a AccountInstanceKeyV1,
    opening_anchor_id: &'a str,
    opening_anchor_sha256: &'a Sha256Hex,
    previous_enrollment: Option<&'a MarketEnrollmentIdentityV1>,
    market: &'a contracts::MarketIdentityV2,
    assets: &'a [contracts::AssetIdentityV1; 2],
    enrollment_chain_length: u64,
}

#[derive(Serialize)]
struct MarketEnrollmentChainHashMaterialV1<'a> {
    schema_version: &'static str,
    account_instance_key: &'a AccountInstanceKeyV1,
    opening_anchor_id: &'a str,
    opening_anchor_sha256: &'a Sha256Hex,
    previous_chain_sha256: Option<&'a Sha256Hex>,
    new_chain_head: &'a MarketEnrollmentIdentityV1,
    enrollment_chain_length: u64,
}

fn build_market_enrollment(
    request: &MarketEnrollmentRequestV1,
    account_instance_key: &AccountInstanceKeyV1,
    opening_anchor: &AccountOpeningAnchorV1,
    previous: Option<&MarketEnrollmentV1>,
) -> Result<MarketEnrollmentV1, SessionError> {
    let enrollment_chain_length = match previous {
        Some(previous) => previous
            .enrollment_chain_length
            .checked_add(1)
            .ok_or(SessionError::MarketEnrollmentSequenceOverflow)?,
        None => 1,
    };
    let previous_enrollment = previous.map(|previous| MarketEnrollmentIdentityV1 {
        enrollment_id: previous.enrollment_id.clone(),
        enrollment_sha256: previous.enrollment_sha256.clone(),
    });
    let id_material = MarketEnrollmentIdMaterialV1 {
        schema_version: MARKET_ENROLLMENT_SCHEMA_V1,
        enrollment_request_id: &request.enrollment_request_id,
        account_instance_key,
        opening_anchor_id: &opening_anchor.anchor_id,
        opening_anchor_sha256: &opening_anchor.anchor_sha256,
        previous_enrollment: previous_enrollment.as_ref(),
        market: request.market.market(),
        assets: request.market.assets(),
        enrollment_chain_length,
    };
    let enrollment_id = hash_json(MARKET_ENROLLMENT_ID_DOMAIN_V1, &id_material)?.0;
    let hash_material = MarketEnrollmentHashMaterialV1 {
        schema_version: MARKET_ENROLLMENT_SCHEMA_V1,
        enrollment_id: &enrollment_id,
        enrollment_request_id: &request.enrollment_request_id,
        account_instance_key,
        opening_anchor_id: &opening_anchor.anchor_id,
        opening_anchor_sha256: &opening_anchor.anchor_sha256,
        previous_enrollment: previous_enrollment.as_ref(),
        market: request.market.market(),
        assets: request.market.assets(),
        enrollment_chain_length,
    };
    let enrollment_sha256 = hash_json(MARKET_ENROLLMENT_HASH_DOMAIN_V1, &hash_material)?;
    let new_chain_head = MarketEnrollmentIdentityV1 {
        enrollment_id: enrollment_id.clone(),
        enrollment_sha256: enrollment_sha256.clone(),
    };
    let chain_material = MarketEnrollmentChainHashMaterialV1 {
        schema_version: MARKET_ENROLLMENT_SCHEMA_V1,
        account_instance_key,
        opening_anchor_id: &opening_anchor.anchor_id,
        opening_anchor_sha256: &opening_anchor.anchor_sha256,
        previous_chain_sha256: previous.map(|previous| &previous.enrollment_chain_sha256),
        new_chain_head: &new_chain_head,
        enrollment_chain_length,
    };
    let enrollment_chain_sha256 =
        hash_json(MARKET_ENROLLMENT_CHAIN_HASH_DOMAIN_V1, &chain_material)?;
    Ok(MarketEnrollmentV1 {
        schema_version: MARKET_ENROLLMENT_SCHEMA_V1.to_owned(),
        enrollment_id,
        enrollment_sha256,
        enrollment_request_id: request.enrollment_request_id.clone(),
        account_instance_key: account_instance_key.clone(),
        opening_anchor_id: opening_anchor.anchor_id.clone(),
        opening_anchor_sha256: opening_anchor.anchor_sha256.clone(),
        previous_enrollment,
        market: request.market.market().clone(),
        assets: request.market.assets().clone(),
        enrollment_chain_length,
        enrollment_chain_sha256,
    })
}

pub(crate) fn validate_durable_market_enrollment_chain(
    chain: &[MarketEnrollmentV1],
    account_instance_key: &AccountInstanceKeyV1,
    opening_anchor: &AccountOpeningAnchorV1,
) -> Result<(), SessionCheckpointErrorV1> {
    let mut previous = None;
    for enrollment in chain {
        if !is_stable_text(&enrollment.enrollment_request_id) {
            return Err(SessionCheckpointErrorV1::InvalidMarketEnrollment(
                "登记请求身份为空或含首尾空白".to_owned(),
            ));
        }
        let market =
            PlatformMarketV1::try_new(enrollment.market.clone(), enrollment.assets.clone())
                .map_err(|error| {
                    SessionCheckpointErrorV1::InvalidMarketEnrollment(error.to_string())
                })?;
        let request = MarketEnrollmentRequestV1 {
            enrollment_request_id: enrollment.enrollment_request_id.clone(),
            market,
        };
        let reconstructed =
            build_market_enrollment(&request, account_instance_key, opening_anchor, previous)
                .map_err(SessionCheckpointErrorV1::from)?;
        if reconstructed != *enrollment {
            return Err(SessionCheckpointErrorV1::InvalidMarketEnrollment(
                "登记身份、前驱或链摘要不连续".to_owned(),
            ));
        }
        previous = Some(enrollment);
    }
    Ok(())
}

fn validate_dependency_identity(
    dependency: &'static str,
    checkpoint: &Sha256Hex,
    loaded: &Sha256Hex,
) -> Result<(), SessionCheckpointErrorV1> {
    if checkpoint == loaded {
        Ok(())
    } else {
        Err(SessionCheckpointErrorV1::DependencyIdentityConflict(
            dependency,
        ))
    }
}

fn validate_checkpoint_initial_account_state(
    spec: &SessionSpecV2,
) -> Result<(), SessionCheckpointErrorV1> {
    let reconstructed = InitialAccountStateV1::try_new(spec.initial_account_state.available_pusd())
        .map_err(|_| SessionCheckpointErrorV1::InvalidInitialAccountState)?;
    if reconstructed == spec.initial_account_state {
        Ok(())
    } else {
        Err(SessionCheckpointErrorV1::InvalidInitialAccountState)
    }
}

fn validate_initialization_snapshot(
    snapshot: &SessionInitializationSnapshotV1,
) -> Result<(), SessionCheckpointErrorV1> {
    let valid = match snapshot.state {
        SessionInitializationStateV1::PreparedLedgerInit => {
            snapshot.activation_id.is_none() && snapshot.failure_id.is_none()
        }
        SessionInitializationStateV1::Active => {
            snapshot
                .activation_id
                .as_deref()
                .is_some_and(is_stable_text)
                && snapshot.failure_id.is_none()
        }
        SessionInitializationStateV1::InitializationFailedClosed => {
            snapshot.activation_id.is_none()
                && snapshot.failure_id.as_deref().is_some_and(is_stable_text)
        }
    };
    if valid {
        Ok(())
    } else {
        Err(SessionCheckpointErrorV1::InvalidInitialization(
            "状态与激活或失败身份不一致",
        ))
    }
}

fn validate_market_enrollment_checkpoint(
    initial_directory: &SessionMarketDirectoryV1,
    checkpoint: &MarketEnrollmentStateV1,
    account_instance_key: &AccountInstanceKeyV1,
    opening_anchor: &AccountOpeningAnchorV1,
) -> Result<(), SessionCheckpointErrorV1> {
    let mut reconstructed_directory = initial_directory.clone();
    let mut reconstructed_records = BTreeMap::new();
    let mut previous = None;
    for (index, enrollment) in checkpoint.chain.iter().enumerate() {
        if !is_stable_text(&enrollment.enrollment_request_id) {
            return Err(SessionCheckpointErrorV1::InvalidMarketEnrollment(
                "登记请求身份为空或含首尾空白".to_owned(),
            ));
        }
        let market =
            PlatformMarketV1::try_new(enrollment.market.clone(), enrollment.assets.clone())
                .map_err(|error| {
                    SessionCheckpointErrorV1::InvalidMarketEnrollment(error.to_string())
                })?;
        let request = MarketEnrollmentRequestV1 {
            enrollment_request_id: enrollment.enrollment_request_id.clone(),
            market: market.clone(),
        };
        let reconstructed =
            build_market_enrollment(&request, account_instance_key, opening_anchor, previous)?;
        if reconstructed != *enrollment {
            return Err(SessionCheckpointErrorV1::InvalidMarketEnrollment(
                "登记身份、前驱或链摘要不连续".to_owned(),
            ));
        }
        if reconstructed_records
            .insert(enrollment.enrollment_request_id.clone(), index)
            .is_some()
        {
            return Err(SessionCheckpointErrorV1::InvalidMarketEnrollment(
                "登记请求身份重复".to_owned(),
            ));
        }
        reconstructed_directory
            .try_insert(market)
            .map_err(|error| {
                SessionCheckpointErrorV1::InvalidMarketEnrollment(error.to_string())
            })?;
        previous = Some(enrollment);
    }
    if checkpoint.directory != reconstructed_directory
        || checkpoint.records_by_request_id != reconstructed_records
    {
        return Err(SessionCheckpointErrorV1::InvalidMarketEnrollment(
            "目录或请求索引与登记链不一致".to_owned(),
        ));
    }
    if checkpoint
        .paper_candidate_condition_ids
        .iter()
        .any(|condition_id| checkpoint.directory.contains(condition_id))
        || checkpoint
            .paper_candidate_token_ids
            .iter()
            .any(|token_id| checkpoint.directory.contains_token(token_id))
    {
        return Err(SessionCheckpointErrorV1::InvalidMarketEnrollment(
            "待登记候选已经存在于会话目录".to_owned(),
        ));
    }
    Ok(())
}

fn initialization_snapshot(
    initialization: &SharedSessionInitializationV1,
) -> Result<SessionInitializationSnapshotV1, SessionError> {
    initialization
        .lock()
        .map(|snapshot| snapshot.clone())
        .map_err(|_| SessionError::InitializationStateUnavailable)
}

pub(crate) fn validate_activation_request(
    request: &ActivateSessionRequestV1,
) -> Result<(), SessionError> {
    validate_initialization_identity(
        &request.session_id,
        &request.account_instance_key,
        &request.opening_anchor_id,
        &request.opening_anchor_sha256,
    )?;
    if !is_stable_text(&request.activation_id) {
        return Err(SessionError::InvalidInitializationControl(
            "activation_id 为空、超过 256 字节或含首尾空白",
        ));
    }
    Ok(())
}

pub(crate) fn validate_failure_request(
    request: &FailSessionInitializationRequestV1,
) -> Result<(), SessionError> {
    validate_initialization_identity(
        &request.session_id,
        &request.account_instance_key,
        &request.opening_anchor_id,
        &request.opening_anchor_sha256,
    )?;
    if !is_stable_text(&request.failure_id) {
        return Err(SessionError::InvalidInitializationControl(
            "failure_id 为空、超过 256 字节或含首尾空白",
        ));
    }
    Ok(())
}

pub(crate) fn validate_market_enrollment_request(
    request: &MarketEnrollmentRequestV1,
) -> Result<(), SessionError> {
    if !is_stable_text(&request.enrollment_request_id) {
        return Err(SessionError::InvalidMarketEnrollment(
            "enrollment_request_id 为空、超过 256 字节或含首尾空白",
        ));
    }
    request
        .market
        .validate_for_admission()
        .map_err(|_| SessionError::InvalidMarketEnrollment("市场身份或结果身份非法"))
}

fn validate_initialization_identity(
    session_id: &str,
    account_instance_key: &AccountInstanceKeyV1,
    opening_anchor_id: &str,
    opening_anchor_sha256: &Sha256Hex,
) -> Result<(), SessionError> {
    if !is_stable_text(session_id)
        || !is_stable_text(&account_instance_key.run_id)
        || !is_stable_text(&account_instance_key.caller_instance_id)
        || !is_stable_text(&account_instance_key.account_id)
        || !is_stable_text(opening_anchor_id)
    {
        return Err(SessionError::InvalidInitializationControl(
            "开户身份字段为空、超过 256 字节或含首尾空白",
        ));
    }
    if !is_sha256_hex(&opening_anchor_sha256.0) {
        return Err(SessionError::InvalidInitializationControl(
            "opening_anchor_sha256 非法",
        ));
    }
    Ok(())
}

fn account_instance_key(spec: &SessionSpecV2) -> AccountInstanceKeyV1 {
    AccountInstanceKeyV1 {
        run_id: spec.run_id.clone(),
        caller_role: spec.caller_role,
        caller_instance_id: spec.caller_instance_id.clone(),
        account_id: spec.account_id.clone(),
        platform_source: PlatformSourceV1::Simulated,
    }
}

fn session_spec_sha256(spec: &SessionSpecV2) -> Result<Sha256Hex, SessionError> {
    let response_outbox_capacity = u64::try_from(spec.response_outbox_capacity.get())
        .map_err(|_| SessionError::InvalidSessionSpec("响应发件箱容量超出 u64"))?;
    let material = SessionSpecIdentityMaterialV2 {
        schema_version: SESSION_SPEC_SCHEMA_V2,
        run_id: &spec.run_id,
        session_id: &spec.session_id,
        account_id: &spec.account_id,
        caller_role: spec.caller_role,
        caller_instance_id: &spec.caller_instance_id,
        creation_request_id: &spec.creation_request_id,
        response_bridge_binding_id: &spec.response_bridge_binding_id,
        initial_account_state_sha256: spec.initial_account_state.sha256(),
        response_outbox_capacity,
        market_set_sha256: &spec.market_set_sha256,
        scenario_sha256: &spec.scenario_sha256,
        session_seed: &spec.session_seed,
        execution_contract_version: &spec.execution_contract_version,
        market_event_schema_version: &spec.market_event_schema_version,
    };
    hash_json(SESSION_SPEC_HASH_DOMAIN_V2, &material)
}

fn account_opening_anchor(
    spec: &SessionSpecV2,
    account_instance_key: AccountInstanceKeyV1,
    scenario: &PlatformScenarioV1,
) -> Result<AccountOpeningAnchorV1, SessionError> {
    let initial_platform_account = initial_platform_account(spec, scenario);
    let identity_material = AccountOpeningAnchorIdentityMaterialV1 {
        schema_version: ACCOUNT_OPENING_ANCHOR_SCHEMA_V1,
        account_instance_key: &account_instance_key,
        session_id: &spec.session_id,
        initial_scenario_sha256: &spec.scenario_sha256,
        initial_market_set_sha256: &spec.market_set_sha256,
        initial_account_state_sha256: spec.initial_account_state.sha256(),
        initial_platform_account: &initial_platform_account,
    };
    let anchor_id = hash_json(ACCOUNT_OPENING_ANCHOR_ID_DOMAIN_V1, &identity_material)?.0;
    let hash_material = AccountOpeningAnchorHashMaterialV1 {
        schema_version: ACCOUNT_OPENING_ANCHOR_SCHEMA_V1,
        anchor_id: &anchor_id,
        account_instance_key: &account_instance_key,
        session_id: &spec.session_id,
        initial_scenario_sha256: &spec.scenario_sha256,
        initial_market_set_sha256: &spec.market_set_sha256,
        initial_account_state_sha256: spec.initial_account_state.sha256(),
        initial_platform_account: &initial_platform_account,
    };
    let anchor_sha256 = hash_json(ACCOUNT_OPENING_ANCHOR_HASH_DOMAIN_V1, &hash_material)?;
    Ok(AccountOpeningAnchorV1 {
        schema_version: ACCOUNT_OPENING_ANCHOR_SCHEMA_V1.to_owned(),
        anchor_id,
        anchor_sha256,
        account_instance_key,
        session_id: spec.session_id.clone(),
        initial_scenario_sha256: spec.scenario_sha256.clone(),
        initial_market_set_sha256: spec.market_set_sha256.clone(),
        initial_account_state_sha256: spec.initial_account_state.sha256().clone(),
        initial_platform_account,
    })
}

fn initial_platform_account(
    spec: &SessionSpecV2,
    scenario: &PlatformScenarioV1,
) -> PlatformAccountSnapshotV1 {
    let mut outcome_tokens = BTreeMap::<String, TokenId>::new();
    for market in scenario.market_set().markets() {
        for asset in market.assets() {
            outcome_tokens
                .entry(asset.token_id.0.clone())
                .or_insert_with(|| asset.token_id.clone());
        }
    }
    let zero = exact_decimal(spec.initial_account_state.initial_encumbered_pusd());
    let available = exact_decimal(spec.initial_account_state.available_pusd());
    let mut outcome_tokens: Vec<_> = outcome_tokens.into_values().collect();
    outcome_tokens.sort_by(|left, right| compare_token_ids(&left.0, &right.0));
    PlatformAccountSnapshotV1 {
        cash_pusd: available.clone(),
        available_pusd: available,
        principal_encumbrance_pusd: zero.clone(),
        immediate_taker_fee_encumbrance_pusd: zero.clone(),
        outcome_balances: outcome_tokens
            .into_iter()
            .map(|token_id| PlatformOutcomeBalanceV1 {
                token_id,
                balance_shares: exact_decimal(
                    spec.initial_account_state.initial_outcome_balance_shares(),
                ),
            })
            .collect(),
        maker_rebate_receivable_pusd: exact_decimal(
            spec.initial_account_state.initial_rebate_receivable_pusd(),
        ),
        taker_rebate_receivable_pusd: exact_decimal(
            spec.initial_account_state.initial_rebate_receivable_pusd(),
        ),
        maker_rebate_locked_remainder_pusd: zero.clone(),
        taker_rebate_locked_remainder_pusd: zero,
    }
}

fn exact_decimal(value: rust_decimal::Decimal) -> ExactDecimal {
    ExactDecimal(value.normalize().to_string())
}

fn compare_token_ids(left: &str, right: &str) -> Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

fn hash_json<T: Serialize>(domain: &str, value: &T) -> Result<Sha256Hex, SessionError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| SessionError::SessionIdentitySerialization(error.to_string()))?;
    let mut hash = Sha256::new();
    hash.update((domain.len() as u64).to_be_bytes());
    hash.update(domain.as_bytes());
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    Ok(Sha256Hex(format!("{:x}", hash.finalize())))
}

pub(crate) fn validate_session_spec(spec: &SessionSpecV2) -> Result<(), SessionError> {
    if spec.session_id.len() > SESSION_ID_MAX_BYTES_V1 {
        return Err(SessionError::InvalidSessionSpec(
            "session_id 超过 256 字节上限",
        ));
    }
    if [
        spec.run_id.as_str(),
        spec.session_id.as_str(),
        spec.account_id.as_str(),
        spec.caller_instance_id.as_str(),
        spec.creation_request_id.as_str(),
        spec.response_bridge_binding_id.as_str(),
    ]
    .into_iter()
    .any(|value| value.len() > STABLE_ID_MAX_BYTES_V1)
    {
        return Err(SessionError::InvalidSessionSpec(
            "稳定身份字段超过 256 字节上限",
        ));
    }
    if spec.run_id.is_empty()
        || spec.session_id.is_empty()
        || spec.account_id.is_empty()
        || spec.caller_instance_id.is_empty()
        || spec.creation_request_id.is_empty()
        || spec.response_bridge_binding_id.is_empty()
        || !is_stable_text(&spec.run_id)
        || !is_stable_text(&spec.session_id)
        || !is_stable_text(&spec.account_id)
        || !is_stable_text(&spec.caller_instance_id)
        || !is_stable_text(&spec.creation_request_id)
        || !is_stable_text(&spec.response_bridge_binding_id)
    {
        return Err(SessionError::InvalidSessionSpec("身份字段为空"));
    }
    if !session_id_is_valid_v1(&spec.session_id) {
        return Err(SessionError::InvalidSessionSpec(
            "session_id 只能包含 ASCII 字母、数字、连字符或下划线",
        ));
    }
    if spec.execution_contract_version != SIM_EXECUTION_CONTRACT_V1
        || spec.market_event_schema_version != EXECUTION_MARKET_EVENT_SCHEMA_V3
    {
        return Err(SessionError::InvalidSessionSpec("合同版本不匹配"));
    }
    if !is_sha256_hex(&spec.market_set_sha256.0)
        || !is_sha256_hex(&spec.scenario_sha256.0)
        || !is_sha256_hex(&spec.session_seed.0)
    {
        return Err(SessionError::InvalidSessionSpec("内容身份或种子摘要非法"));
    }
    Ok(())
}

fn is_stable_text(value: &str) -> bool {
    !value.is_empty() && value.len() <= STABLE_ID_MAX_BYTES_V1 && value.trim() == value
}

fn validate_request_context(
    spec: &SessionSpecV2,
    context: &RequestContextV1,
) -> Result<(), SessionError> {
    if context.execution_contract_version != SIM_EXECUTION_CONTRACT_V1
        || context.execution_contract_version != spec.execution_contract_version
        || context.session_id != spec.session_id
        || context.account_id != spec.account_id
        || context.caller_role != spec.caller_role
        || context.caller_instance_id != spec.caller_instance_id
        || !is_stable_text(&context.global_request_id)
        || context
            .caller_correlation_id
            .as_deref()
            .is_some_and(|identity| !is_stable_text(identity))
    {
        return Err(SessionError::InvalidRequest("合同或调用方身份不匹配"));
    }
    Ok(())
}

fn validate_model_visible_cursor(
    state: &SessionState,
    cursor: &ModelVisibleCursorV1,
) -> Result<(), SessionError> {
    if cursor.session_id != state.spec.session_id
        || cursor.private_feedback_sequence > state.private_feedback_sequence
        || cursor.public_visibility.next_visibility_index_position
            > state.public_visibility.next_visibility_index_position
        || match (
            cursor.public_visibility.closed_through_received_at_utc_ms,
            state.public_visibility.closed_through_received_at_utc_ms,
        ) {
            (Some(request), Some(current)) => request > current,
            (Some(_), None) => true,
            (None, _) => false,
        }
    {
        return Err(SessionError::FutureModelVisibleCursor);
    }
    Ok(())
}

fn validate_request_visible_time(
    state: &SessionState,
    context: &RequestContextV1,
) -> Result<(), SessionError> {
    let public_visible_at = context
        .model_visible_cursor
        .public_visibility
        .closed_through_received_at_utc_ms;
    let private_visible_at = match context.model_visible_cursor.private_feedback_sequence {
        0 => None,
        sequence => state
            .private_fact_received_at_by_sequence
            .get((sequence - 1) as usize)
            .copied(),
    };
    if public_visible_at
        .into_iter()
        .chain(private_visible_at)
        .any(|visible_at| visible_at > context.request_sent_at_utc_ms)
    {
        return Err(SessionError::RequestBeforeVisibleFact);
    }
    Ok(())
}

fn checked_add_delay(base_utc_ms: i64, delay_ms: u64) -> Result<i64, SessionError> {
    let delay = i64::try_from(delay_ms).map_err(|_| SessionError::TimeOverflow)?;
    base_utc_ms
        .checked_add(delay)
        .ok_or(SessionError::TimeOverflow)
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::finality::successful_policy_for_tests;
    use crate::latency_profile::load_embedded_proxy_latency_profile;
    use crate::market::{ClosedMarketEventBucketV1, MarketBucketApplyDispositionV1};
    use crate::platform_rules::tests::successful_snapshot_for_sim_tests;
    use crate::scenario::scenario_for_sim_tests;
    use crate::task21_tests::{
        asset, fresh_quality, hidden_bucket, initial_snapshot_event, orderbook_evidence,
        sealed_event,
    };
    use contracts::{
        AssetIdentityV1, ClobMarketId, ConditionId, ExecutionMarketEventPayloadV3,
        ExecutionMarketEventV3, MarketIdentityV2, MarketWindowV2, OutcomeIdentityV1, PayoutEntryV1,
    };
    use data_pipeline_core::encode_execution_market_event_v3;

    fn sha(byte: &str) -> Sha256Hex {
        Sha256Hex(byte.repeat(64))
    }

    fn spec() -> SessionSpecV2 {
        SessionSpecV2 {
            run_id: "component-test-run".to_owned(),
            session_id: "session-a".to_owned(),
            account_id: "account-a".to_owned(),
            caller_role: SimCallerRoleV1::ModelTrain,
            caller_instance_id: "caller-a".to_owned(),
            creation_request_id: "create-session-a".to_owned(),
            response_bridge_binding_id: "bridge-session-a".to_owned(),
            initial_account_state: InitialAccountStateV1::try_new(rust_decimal::Decimal::new(
                1_000, 0,
            ))
            .expect("initial account"),
            response_outbox_capacity: NonZeroUsize::new(2).expect("positive test capacity"),
            market_set_sha256: sha("a"),
            scenario_sha256: sha("b"),
            session_seed: sha("c"),
            execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
            market_event_schema_version: EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
        }
    }

    fn checkpoint_spec(scenario: &PlatformScenarioV1, suffix: &str) -> SessionSpecV2 {
        let mut spec = spec();
        spec.session_id = format!("session-{suffix}");
        spec.account_id = format!("account-{suffix}");
        spec.caller_instance_id = format!("caller-{suffix}");
        spec.creation_request_id = format!("create-session-{suffix}");
        spec.response_bridge_binding_id = format!("bridge-session-{suffix}");
        spec.market_set_sha256 = scenario.market_set_sha256().clone();
        spec.scenario_sha256 = scenario.scenario_sha256().clone();
        spec.session_seed =
            hash_json("SIM_SESSION_CHECKPOINT_TEST_SEED", &suffix).expect("测试会话种子身份");
        spec
    }

    async fn private_fact_slice_test_session(suffix: &str) -> SimSession {
        let finality_policy = successful_policy_for_tests();
        let rule_snapshot = successful_snapshot_for_sim_tests().await;
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let scenario = scenario_for_sim_tests();
        let mut root = SimProcessRoot::try_new(
            "component-test-run".to_owned(),
            finality_policy,
            rule_snapshot,
            latency_profile,
            Arc::clone(&scenario),
        )
        .expect("运行根");
        root.create_session(checkpoint_spec(&scenario, suffix))
            .expect("私有事实切片测试会话")
            .session
    }

    fn public_market_bucket_for_session_test(
        at_utc_ms: i64,
        next_position: u64,
        events: &[ExecutionMarketEventV3],
    ) -> ClosedMarketEventBucketV1 {
        let event_count = u64::try_from(events.len()).expect("测试事件数量");
        ClosedMarketEventBucketV1::public(
            at_utc_ms,
            next_position
                .checked_sub(event_count)
                .expect("测试桶索引覆盖全部事件"),
            next_position,
            MillisecondBucketClosureV1::NextEventAt(at_utc_ms + 1),
            events
                .iter()
                .map(|event| encode_execution_market_event_v3(event).expect("规范市场事件编码"))
                .collect::<Vec<_>>(),
        )
    }

    fn resolution_event_for_session_test(
        winning_outcome_index: u32,
        source_at_utc_ms: i64,
        received_at_utc_ms: i64,
        sequence: u64,
    ) -> (ExecutionMarketEventV3, MarketResolutionTerminalV3) {
        let (winning_asset_id, winning_outcome) = match winning_outcome_index {
            0 => ("1", "Up"),
            1 => ("2", "Down"),
            _ => panic!("测试结果索引必须属于二元市场"),
        };
        let resolution = MarketResolutionTerminalV3::Resolved {
            winning_asset_id: winning_asset_id.to_owned(),
            winning_outcome: winning_outcome.to_owned(),
            payout_vector: vec![
                PayoutEntryV1 {
                    asset: asset("1", 0, "Up"),
                    payout_pusd_per_share: ExactDecimal(
                        (winning_outcome_index == 0)
                            .then_some("1")
                            .unwrap_or("0")
                            .to_owned(),
                    ),
                },
                PayoutEntryV1 {
                    asset: asset("2", 1, "Down"),
                    payout_pusd_per_share: ExactDecimal(
                        (winning_outcome_index == 1)
                            .then_some("1")
                            .unwrap_or("0")
                            .to_owned(),
                    ),
                },
            ],
            mapping_rule_version: "scheduled-session-drive-resolution-v1".to_owned(),
        };
        let event = sealed_event(
            ExecutionMarketEventPayloadV3::MarketResolutionTerminal(resolution.clone()),
            orderbook_evidence(source_at_utc_ms, received_at_utc_ms, sequence),
            fresh_quality(),
        );
        (event, resolution)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn process_root_run_id_matches_session_stable_identity_byte_limit() {
        let finality_policy = successful_policy_for_tests();
        let rule_snapshot = successful_snapshot_for_sim_tests().await;
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let scenario = scenario_for_sim_tests();

        let at_limit = "a".repeat(STABLE_ID_MAX_BYTES_V1);
        assert_eq!(at_limit.len(), 256);
        SimProcessRoot::try_new(
            at_limit,
            Arc::clone(&finality_policy),
            Arc::clone(&rule_snapshot),
            Arc::clone(&latency_profile),
            Arc::clone(&scenario),
        )
        .expect("恰好达到 256 个 UTF-8 字节的 run_id 应被接受");

        let over_limit = "a".repeat(STABLE_ID_MAX_BYTES_V1 + 1);
        assert_eq!(over_limit.len(), 257);
        let error = SimProcessRoot::try_new(
            over_limit,
            finality_policy,
            rule_snapshot,
            latency_profile,
            scenario,
        )
        .err()
        .expect("257 个 UTF-8 字节的 run_id 应被拒绝");
        assert_eq!(error, SimProcessRootErrorV1::InvalidRunId);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn session_id_is_bounded_safe_ascii_at_creation_boundary() {
        let finality_policy = successful_policy_for_tests();
        let rule_snapshot = successful_snapshot_for_sim_tests().await;
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let scenario = scenario_for_sim_tests();
        let mut root = SimProcessRoot::try_new(
            "component-test-run".to_owned(),
            Arc::clone(&finality_policy),
            Arc::clone(&rule_snapshot),
            Arc::clone(&latency_profile),
            Arc::clone(&scenario),
        )
        .expect("运行根");

        let at_limit_session_id = "a".repeat(SESSION_ID_MAX_BYTES_V1);
        assert_eq!(SESSION_ID_MAX_BYTES_V1, 256);
        assert_eq!(at_limit_session_id.len(), SESSION_ID_MAX_BYTES_V1);
        let mut at_limit = checkpoint_spec(&scenario, "session-id-limit");
        at_limit.session_id = at_limit_session_id.clone();
        root.create_session(at_limit)
            .expect("恰好达到 ASCII 字节上限的 session_id 应被接受");

        let mut over_limit = checkpoint_spec(&scenario, "session-id-over-limit");
        over_limit.session_id = format!("{at_limit_session_id}b");
        assert_eq!(over_limit.session_id.len(), SESSION_ID_MAX_BYTES_V1 + 1);
        let error = root
            .create_session(over_limit)
            .err()
            .expect("超出 ASCII 字节上限的 session_id 应被拒绝");
        assert_eq!(
            error,
            SessionError::InvalidSessionSpec("session_id 超过 256 字节上限")
        );

        let mut escaped_control = checkpoint_spec(&scenario, "session-id-control");
        escaped_control.session_id = format!("{}\0", "a".repeat(SESSION_ID_MAX_BYTES_V1 - 1));
        assert_eq!(escaped_control.session_id.len(), SESSION_ID_MAX_BYTES_V1);
        let error = root
            .create_session(escaped_control)
            .err()
            .expect("会触发 JSON 转义膨胀的 session_id 控制字符应被拒绝");
        assert_eq!(
            error,
            SessionError::InvalidSessionSpec(
                "session_id 只能包含 ASCII 字母、数字、连字符或下划线"
            )
        );
    }

    #[test]
    fn initialization_and_enrollment_validators_bound_request_identity_before_copy() {
        let account_instance_key = AccountInstanceKeyV1 {
            run_id: "component-test-run".to_owned(),
            caller_role: SimCallerRoleV1::ModelTrain,
            caller_instance_id: "caller-a".to_owned(),
            account_id: "account-a".to_owned(),
            platform_source: PlatformSourceV1::Simulated,
        };
        let at_limit = "a".repeat(STABLE_ID_MAX_BYTES_V1);
        let over_limit = "a".repeat(STABLE_ID_MAX_BYTES_V1 + 1);

        let mut activation = ActivateSessionRequestV1 {
            session_id: "session-a".to_owned(),
            account_instance_key: account_instance_key.clone(),
            opening_anchor_id: "opening-anchor-a".to_owned(),
            opening_anchor_sha256: sha("a"),
            activation_id: at_limit.clone(),
        };
        validate_activation_request(&activation)
            .expect("activation_id 恰好达到 256 字节时应被接受");
        activation.activation_id = over_limit.clone();
        assert_eq!(
            validate_activation_request(&activation),
            Err(SessionError::InvalidInitializationControl(
                "activation_id 为空、超过 256 字节或含首尾空白"
            ))
        );

        let mut failure = FailSessionInitializationRequestV1 {
            session_id: "session-a".to_owned(),
            account_instance_key,
            opening_anchor_id: "opening-anchor-a".to_owned(),
            opening_anchor_sha256: sha("a"),
            failure_id: at_limit.clone(),
        };
        validate_failure_request(&failure).expect("failure_id 恰好达到 256 字节时应被接受");
        failure.failure_id = over_limit.clone();
        assert_eq!(
            validate_failure_request(&failure),
            Err(SessionError::InvalidInitializationControl(
                "failure_id 为空、超过 256 字节或含首尾空白"
            ))
        );

        let market = appended_market();
        let mut enrollment = MarketEnrollmentRequestV1 {
            enrollment_request_id: at_limit,
            market,
        };
        validate_market_enrollment_request(&enrollment)
            .expect("enrollment_request_id 恰好达到 256 字节时应被接受");
        enrollment.enrollment_request_id = over_limit;
        assert_eq!(
            validate_market_enrollment_request(&enrollment),
            Err(SessionError::InvalidMarketEnrollment(
                "enrollment_request_id 为空、超过 256 字节或含首尾空白"
            ))
        );
    }

    #[test]
    fn enrollment_validator_rejects_unchecked_oversized_market_text() {
        let request = MarketEnrollmentRequestV1 {
            enrollment_request_id: "enroll-appended-market".to_owned(),
            market: appended_market(),
        };
        let mut encoded = serde_json::to_value(request).expect("编码市场登记请求");
        encoded["market"]["assets"][0]["outcome"]["label"] =
            serde_json::Value::String("a".repeat(STABLE_ID_MAX_BYTES_V1 + 1));
        let unchecked: MarketEnrollmentRequestV1 =
            serde_json::from_value(encoded).expect("反序列化未经过构造器的市场登记请求");
        assert_eq!(
            validate_market_enrollment_request(&unchecked),
            Err(SessionError::InvalidMarketEnrollment(
                "市场身份或结果身份非法"
            ))
        );
    }

    fn appended_market() -> PlatformMarketV1 {
        PlatformMarketV1::try_new(
            MarketIdentityV2 {
                condition_id: ConditionId(format!("0x{}", "2".repeat(64))),
                clob_market_id: ClobMarketId("456".to_owned()),
                window: MarketWindowV2 {
                    start_at_utc_ms: 2_000,
                    end_at_utc_ms: 3_000,
                },
            },
            [
                AssetIdentityV1 {
                    token_id: TokenId("3".to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 0,
                        label: "Up".to_owned(),
                    },
                },
                AssetIdentityV1 {
                    token_id: TokenId("4".to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 1,
                        label: "Down".to_owned(),
                    },
                },
            ],
        )
        .expect("追加市场夹具")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn process_and_session_checkpoints_round_trip_and_rebind_once() {
        let finality_policy = successful_policy_for_tests();
        let rule_snapshot = successful_snapshot_for_sim_tests().await;
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let scenario = scenario_for_sim_tests();
        let mut root = SimProcessRoot::try_new(
            "component-test-run".to_owned(),
            Arc::clone(&finality_policy),
            Arc::clone(&rule_snapshot),
            Arc::clone(&latency_profile),
            Arc::clone(&scenario),
        )
        .expect("运行根");
        let first_spec = checkpoint_spec(&scenario, "first");
        let first = root
            .create_session(first_spec.clone())
            .expect("首个准备态会话");
        root.activate_session(ActivateSessionRequestV1 {
            session_id: first_spec.session_id.clone(),
            account_instance_key: first.control.account_instance_key.clone(),
            opening_anchor_id: first.control.opening_anchor.anchor_id.clone(),
            opening_anchor_sha256: first.control.opening_anchor.anchor_sha256.clone(),
            activation_id: "activate-first".to_owned(),
        })
        .expect("激活首个会话");
        let mut first_session = first.session;
        root.enroll_market(
            &mut first_session,
            MarketEnrollmentRequestV1 {
                enrollment_request_id: "enroll-appended-market".to_owned(),
                market: appended_market(),
            },
        )
        .expect("追加市场");
        first_session
            .register_and_schedule_request(
                RequestContextV1 {
                    execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
                    session_id: first_spec.session_id.clone(),
                    account_id: first_spec.account_id.clone(),
                    caller_role: first_spec.caller_role,
                    caller_instance_id: first_spec.caller_instance_id.clone(),
                    global_request_id: "pending-request".to_owned(),
                    caller_correlation_id: None,
                    caller_sequence: 1,
                    request_sent_at_utc_ms: 500,
                    model_visible_cursor: first_session.model_visible_cursor(),
                },
                hash_json("SIM_SESSION_CHECKPOINT_TEST_REQUEST", &"pending-request")
                    .expect("测试请求身份"),
                vec![ParsedLogicalActionV1 {
                    action_identity: "pending-request/item-0".to_owned(),
                    batch_item_ordinal: 0,
                    method_timing: ParsedMethodTimingV1::Ordinary,
                }],
            )
            .expect("保留待处理逻辑动作");
        first_session
            .expect_direct_response("pending-request")
            .expect("保留待形成直接响应");
        first_session
            .bind_lifecycle_feedback_timing("pending-feedback".to_owned(), 700, 1)
            .expect("保留待接收私有事实");

        let second_spec = checkpoint_spec(&scenario, "second");
        let second = root
            .create_session(second_spec.clone())
            .expect("第二个准备态会话");
        root.fail_session_initialization(FailSessionInitializationRequestV1 {
            session_id: second_spec.session_id.clone(),
            account_instance_key: second.control.account_instance_key.clone(),
            opening_anchor_id: second.control.opening_anchor.anchor_id.clone(),
            opening_anchor_sha256: second.control.opening_anchor.anchor_sha256.clone(),
            failure_id: "ledger-init-failed".to_owned(),
        })
        .expect("关闭第二个会话初始化");
        let prepared_spec = checkpoint_spec(&scenario, "prepared");
        let prepared = root
            .create_session(prepared_spec)
            .expect("保留第三个准备态会话");
        let detached_root = root.detached_clone().expect("分离运行根副本");
        detached_root
            .activate_session(ActivateSessionRequestV1 {
                session_id: prepared.control.opening_anchor.session_id.clone(),
                account_instance_key: prepared.control.account_instance_key.clone(),
                opening_anchor_id: prepared.control.opening_anchor.anchor_id.clone(),
                opening_anchor_sha256: prepared.control.opening_anchor.anchor_sha256.clone(),
                activation_id: "detached-preview-activation".to_owned(),
            })
            .expect("只激活分离副本");
        assert_eq!(
            root.session_initialization(&prepared.control.opening_anchor)
                .expect("原运行根仍为准备态")
                .state,
            SessionInitializationStateV1::PreparedLedgerInit
        );

        let root_checkpoint: SimProcessRootCheckpointV1 = serde_json::from_slice(
            &serde_json::to_vec(&root.checkpoint().expect("运行根检查点"))
                .expect("编码运行根检查点"),
        )
        .expect("解码运行根检查点");
        let session_checkpoint: SimSessionCheckpointV1 = serde_json::from_slice(
            &serde_json::to_vec(&first_session.checkpoint()).expect("编码会话检查点"),
        )
        .expect("解码会话检查点");
        let expected_session_checkpoint = session_checkpoint.clone();
        let mut restored_root = SimProcessRoot::restore_from_checkpoint(
            root_checkpoint,
            Arc::clone(&finality_policy),
            Arc::clone(&rule_snapshot),
            Arc::clone(&latency_profile),
            Arc::clone(&scenario),
        )
        .expect("恢复运行根");
        let mut restored_session = restored_root
            .restore_session_from_checkpoint(session_checkpoint)
            .expect("恢复会话");

        assert_eq!(restored_root.registered_session_count(), 3);
        assert_eq!(
            restored_session
                .initialization_snapshot()
                .expect("初始化状态"),
            SessionInitializationSnapshotV1 {
                state: SessionInitializationStateV1::Active,
                activation_id: Some("activate-first".to_owned()),
                failure_id: None,
            }
        );
        assert_eq!(
            restored_root
                .session_initialization(&second.control.opening_anchor)
                .expect("失败关闭状态"),
            SessionInitializationSnapshotV1 {
                state: SessionInitializationStateV1::InitializationFailedClosed,
                activation_id: None,
                failure_id: Some("ledger-init-failed".to_owned()),
            }
        );
        assert_eq!(
            restored_root
                .session_initialization(&prepared.control.opening_anchor)
                .expect("准备态状态"),
            SessionInitializationSnapshotV1 {
                state: SessionInitializationStateV1::PreparedLedgerInit,
                activation_id: None,
                failure_id: None,
            }
        );
        assert_eq!(restored_session.market_directory().markets().len(), 2);
        assert!(Arc::ptr_eq(
            restored_session.finality_policy(),
            &finality_policy
        ));
        assert!(Arc::ptr_eq(
            restored_session.rule_snapshot(),
            &rule_snapshot
        ));
        assert!(Arc::ptr_eq(
            restored_session.latency_profile(),
            &latency_profile
        ));
        assert!(Arc::ptr_eq(restored_session.scenario(), &scenario));
        assert_eq!(restored_session.checkpoint(), expected_session_checkpoint);

        let ownership_result = restored_root.enroll_market(
            &mut restored_session,
            MarketEnrollmentRequestV1 {
                enrollment_request_id: "enroll-appended-market".to_owned(),
                market: appended_market(),
            },
        );
        assert!(ownership_result.is_ok(), "恢复会话必须共享运行根初始化 Arc");
        assert_eq!(
            restored_root.retry_create_session(&first_spec),
            Err(SessionError::SessionCreationRetryNotPrepared)
        );
        assert_eq!(
            restored_root.retry_create_session(&second_spec),
            Err(SessionError::SessionCreationRetryNotPrepared)
        );
        let mut duplicate_capital_spec = first_spec;
        duplicate_capital_spec.session_id = "session-third".to_owned();
        duplicate_capital_spec.creation_request_id = "create-session-third".to_owned();
        duplicate_capital_spec.response_bridge_binding_id = "bridge-session-third".to_owned();
        assert!(matches!(
            restored_root.create_session(duplicate_capital_spec),
            Err(SessionError::AccountInstanceAlreadyReserved)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retired_session_checkpoint_retains_capital_fence_only() {
        let finality_policy = successful_policy_for_tests();
        let rule_snapshot = successful_snapshot_for_sim_tests().await;
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let scenario = scenario_for_sim_tests();
        let mut root = SimProcessRoot::try_new(
            "component-test-run".to_owned(),
            Arc::clone(&finality_policy),
            Arc::clone(&rule_snapshot),
            Arc::clone(&latency_profile),
            Arc::clone(&scenario),
        )
        .expect("运行根");
        let retired_spec = checkpoint_spec(&scenario, "retired");
        let prepared = root
            .create_session(retired_spec.clone())
            .expect("创建待退休会话");
        let opening_anchor = prepared.control.opening_anchor.clone();
        root.activate_session(ActivateSessionRequestV1 {
            session_id: retired_spec.session_id.clone(),
            account_instance_key: prepared.control.account_instance_key.clone(),
            opening_anchor_id: opening_anchor.anchor_id.clone(),
            opening_anchor_sha256: opening_anchor.anchor_sha256.clone(),
            activation_id: "activate-retired".to_owned(),
        })
        .expect("激活待退休会话");
        assert_eq!(root.registered_session_count(), 1);
        assert_eq!(root.capital_issuance_fence_count(), 1);
        assert!(root.validate_durable_capital_fence(&retired_spec, &opening_anchor));

        root.retire_session(&retired_spec.session_id)
            .expect("退休会话热状态");
        assert_eq!(root.registered_session_count(), 0);
        assert_eq!(root.capital_issuance_fence_count(), 1);
        assert!(root.is_session_retired(&retired_spec.session_id));
        assert_eq!(
            root.retry_create_session(&retired_spec),
            Err(SessionError::SessionRetentionExpired(
                retired_spec.session_id.clone()
            ))
        );

        let checkpoint: SimProcessRootCheckpointV1 = serde_json::from_slice(
            &serde_json::to_vec(&root.checkpoint().expect("运行根检查点"))
                .expect("编码运行根检查点"),
        )
        .expect("解码运行根检查点");
        assert!(checkpoint.session_creations.is_empty());
        assert_eq!(checkpoint.capital_issuance_fences.len(), 1);
        let mut restored = SimProcessRoot::restore_from_checkpoint(
            checkpoint,
            finality_policy,
            rule_snapshot,
            latency_profile,
            scenario,
        )
        .expect("恢复退休会话资本围栏");
        assert_eq!(restored.registered_session_count(), 0);
        assert_eq!(restored.capital_issuance_fence_count(), 1);
        assert!(restored.is_session_retired(&retired_spec.session_id));
        assert!(restored.validate_durable_capital_fence(&retired_spec, &opening_anchor));
        assert!(matches!(
            restored.create_session(retired_spec.clone()),
            Err(SessionError::SessionRetentionExpired(session_id))
                if session_id == retired_spec.session_id
        ));

        let mut duplicate_capital_spec = retired_spec;
        duplicate_capital_spec.session_id = "session-retired-replacement".to_owned();
        duplicate_capital_spec.creation_request_id =
            "create-session-retired-replacement".to_owned();
        duplicate_capital_spec.response_bridge_binding_id =
            "bridge-session-retired-replacement".to_owned();
        assert!(matches!(
            restored.create_session(duplicate_capital_spec),
            Err(SessionError::AccountInstanceAlreadyReserved)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn process_checkpoint_rejects_rebound_dependency_identity_conflict() {
        let finality_policy = successful_policy_for_tests();
        let rule_snapshot = successful_snapshot_for_sim_tests().await;
        let latency_profile = load_embedded_proxy_latency_profile().expect("代理时延配置档");
        let scenario = scenario_for_sim_tests();
        let root = SimProcessRoot::try_new(
            "component-test-run".to_owned(),
            Arc::clone(&finality_policy),
            Arc::clone(&rule_snapshot),
            Arc::clone(&latency_profile),
            Arc::clone(&scenario),
        )
        .expect("运行根");
        let checkpoint = root.checkpoint().expect("运行根检查点");
        let conflicting_scenario = Arc::new(
            PlatformScenarioV1::try_new(vec![appended_market()]).expect("身份不同的合法场景"),
        );

        assert!(matches!(
            SimProcessRoot::restore_from_checkpoint(
                checkpoint,
                finality_policy,
                rule_snapshot,
                latency_profile,
                conflicting_scenario,
            ),
            Err(SessionCheckpointErrorV1::DependencyIdentityConflict(
                "平台场景"
            ))
        ));
    }

    fn request(state: &SessionState, id: &str, sequence: u64, sent_at: i64) -> RequestContextV1 {
        RequestContextV1 {
            execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
            session_id: state.spec.session_id.clone(),
            account_id: state.spec.account_id.clone(),
            caller_role: state.spec.caller_role,
            caller_instance_id: state.spec.caller_instance_id.clone(),
            global_request_id: id.to_owned(),
            caller_correlation_id: Some("audit-only".to_owned()),
            caller_sequence: sequence,
            request_sent_at_utc_ms: sent_at,
            model_visible_cursor: state.model_visible_cursor(),
        }
    }

    fn drive_idle_logical_platform_through(state: &mut SessionState, cutoff: i64) {
        match state.spec.caller_role {
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
                if state
                    .platform_prefix
                    .closed_through_source_event_at_utc_ms
                    .is_none_or(|closed| closed < cutoff)
                {
                    state
                        .close_platform_prefix_through(
                            cutoff,
                            state.platform_prefix.next_event_time_index_position,
                        )
                        .expect("close model-train platform input through private cutoff");
                }
            }
            SimCallerRoleV1::Paper => {
                if state
                    .public_visibility
                    .closed_through_received_at_utc_ms
                    .is_none_or(|closed| closed < cutoff)
                {
                    state
                        .advance_public_visibility_through(
                            cutoff,
                            state.public_visibility.next_visibility_index_position,
                        )
                        .expect("close paper platform input through private cutoff");
                }
            }
        }
        assert_eq!(
            state
                .take_next_action_through(cutoff)
                .expect("drive logical platform work through private cutoff"),
            None
        );
    }

    #[test]
    fn same_request_retry_reuses_transit_and_never_requeues() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let context = request(&state, "request-1", 1, 1_000);
        let first = state
            .register_request(&profile, context.clone(), sha("d"))
            .expect("new request");
        let retry = state
            .register_request(&profile, context, sha("d"))
            .expect("physical retry");
        assert_eq!(
            first.disposition,
            RequestRegistrationDispositionV1::NewLogicalRequest
        );
        assert_eq!(
            retry.disposition,
            RequestRegistrationDispositionV1::PhysicalRetry
        );
        assert_eq!(first.transit, retry.transit);

        let key = LogicalActionKeyV1 {
            effective_processing_at_utc_ms: first.transit.platform_arrived_at_utc_ms,
            caller_sequence: 1,
            batch_item_ordinal: 0,
        };
        assert!(
            state
                .enqueue_logical_action("request-1/item-0".to_owned(), "request-1".to_owned(), key,)
                .expect("first queue")
        );
        assert!(
            !state
                .enqueue_logical_action("request-1/item-0".to_owned(), "request-1".to_owned(), key,)
                .expect("retry does not queue")
        );
        state
            .seal_logical_actions("request-1")
            .expect("seal request actions");
        state
            .expect_direct_response("request-1")
            .expect("track the still-unformed direct response");
        state
            .close_platform_prefix_through(key.effective_processing_at_utc_ms, 1)
            .expect("close platform prefix");
        let action = state
            .take_next_action_through(key.effective_processing_at_utc_ms)
            .expect("release request")
            .expect("one request action");
        assert_eq!(action.action_identity, "request-1/item-0");
        assert!(
            state
                .complete_logical_item("request-1/item-0")
                .expect("complete request action")
        );
        let completed = state
            .complete_direct_response("request-1", first.transit.platform_arrived_at_utc_ms + 5, 1)
            .expect("complete direct response");
        assert!(
            !state
                .expected_direct_response_request_ids
                .contains("request-1")
        );
        let received_at = completed
            .response_received_at_utc_ms
            .expect("completed response received time");
        assert_eq!(
            state.take_private_facts_through(received_at),
            Err(SessionError::PrivateFactScheduleNotClosed)
        );
        drive_idle_logical_platform_through(&mut state, received_at - 1);
        state
            .close_private_fact_schedule_through(received_at - 1)
            .expect("close before response");
        assert!(
            state
                .take_private_facts_through(received_at - 1)
                .expect("nothing visible before response")
                .is_empty()
        );
        drive_idle_logical_platform_through(&mut state, received_at);
        state
            .close_private_fact_schedule_through(received_at)
            .expect("close through response");
        let visible = state
            .take_private_facts_through(received_at)
            .expect("response becomes visible at received time");
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].private_feedback_sequence, 1);
        assert!(matches!(
            state.register_request(&profile, request(&state, "request-1", 1, 1_000), sha("e")),
            Err(SessionError::RequestIdentityConflict(_))
        ));
    }

    #[test]
    fn request_schedule_prepare_is_read_only_and_retry_uses_stored_request() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let context = request(&state, "prepared-request", 1, 1_000);
        let actions = (0..2_u16)
            .map(|ordinal| LogicalActionPlanV1 {
                action_identity: format!("prepared-request/item-{ordinal}"),
                batch_item_ordinal: ordinal,
                processing_delay_ms: 0,
            })
            .collect::<Vec<_>>();

        let prepared = state
            .prepare_request_schedule(&profile, context.clone(), sha("d"), actions.clone())
            .expect("prevalidate new request schedule");
        assert_eq!(
            prepared.schedule().request.disposition,
            RequestRegistrationDispositionV1::NewLogicalRequest
        );
        assert!(state.requests.is_empty());
        assert!(state.pending_actions.is_empty());
        assert!(state.expected_direct_response_request_ids.is_empty());

        let first = state.commit_prevalidated_request_schedule(prepared);
        assert_eq!(first.actions.len(), 2);
        assert!(state.requests.contains_key("prepared-request"));
        assert_eq!(state.pending_actions.len(), 2);
        assert_eq!(
            state.pending_action_count_for_request_for_tests("prepared-request"),
            2
        );
        assert!(
            state
                .expected_direct_response_request_ids
                .contains("prepared-request")
        );

        let retry = state
            .prepare_registered_request_retry_schedule(&context, actions)
            .expect("rebuild retry schedule from stored request and transit");
        assert_eq!(
            retry.schedule().request.disposition,
            RequestRegistrationDispositionV1::PhysicalRetry
        );
        assert_eq!(retry.schedule().request.transit, first.request.transit);
        assert_eq!(retry.schedule().actions, first.actions);

        let retry = state.commit_prevalidated_request_schedule(retry);
        assert_eq!(retry.actions, first.actions);
        assert_eq!(state.requests.len(), 1);
        assert_eq!(state.pending_actions.len(), 2);
        assert_eq!(
            state.pending_action_count_for_request_for_tests("prepared-request"),
            2
        );
        assert_eq!(state.expected_direct_response_request_ids.len(), 1);
    }

    #[test]
    fn request_schedule_prepare_checks_direct_response_time_before_mutation() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let state = SessionState::new(spec());
        let request_id = "response-time-overflow";
        let outbound_delay_ms = profile.sample_delay_ms(
            state.spec.session_seed.0.as_bytes(),
            request_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        let inbound_delay_ms = profile.sample_delay_ms(
            state.spec.session_seed.0.as_bytes(),
            request_id,
            DelaySampleDomainV1::DirectResponseInbound,
        );
        assert!(inbound_delay_ms > 0);
        let sent_at_utc_ms =
            i64::MAX - i64::try_from(outbound_delay_ms).expect("test outbound delay fits i64");

        let error = state
            .prepare_request_schedule(
                &profile,
                request(&state, request_id, 1, sent_at_utc_ms),
                sha("d"),
                vec![LogicalActionPlanV1 {
                    action_identity: format!("{request_id}/item-0"),
                    batch_item_ordinal: 0,
                    processing_delay_ms: 0,
                }],
            )
            .err()
            .expect("direct response time overflow must fail during prepare");
        assert_eq!(error, SessionError::TimeOverflow);
        assert!(state.requests.is_empty());
        assert!(state.pending_actions.is_empty());
        assert!(state.expected_direct_response_request_ids.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_pending_action_counter_bytes_are_counted_without_a_new_index_record() {
        let mut session = private_fact_slice_test_session("pending-action-resource").await;
        let profile = Arc::clone(&session.latency_profile);
        let before = session.checkpoint();
        let before_core = before.resource_core_counts();
        let before_components = before
            .resource_component_bytes()
            .expect("measure baseline session components");
        let before_wire_indexes = encoded_resource_len(&(
            &before.state.requests,
            &before.state.mutations,
            &before.state.queries,
            &before.state.lifecycle_feedback,
            &before.market_enrollment,
        ))
        .expect("measure baseline wire indexes");
        assert_eq!(before.resource_runtime_request_counter_bytes(), Ok(0));

        let scheduled = session
            .state
            .register_and_schedule_request(
                profile.as_ref(),
                request(&session.state, "resource-counted-request", 1, 1_000),
                sha("d"),
                (0..2_u16)
                    .map(|ordinal| LogicalActionPlanV1 {
                        action_identity: format!("resource-counted-request/item-{ordinal}"),
                        batch_item_ordinal: ordinal,
                        processing_delay_ms: 0,
                    })
                    .collect(),
            )
            .expect("register resource-counted request");
        let after = session.checkpoint();
        let after_core = after.resource_core_counts();
        let after_components = after
            .resource_component_bytes()
            .expect("measure session components with request counter");
        let after_wire_indexes = encoded_resource_len(&(
            &after.state.requests,
            &after.state.mutations,
            &after.state.queries,
            &after.state.lifecycle_feedback,
            &after.market_enrollment,
        ))
        .expect("measure wire indexes with request");
        assert_eq!(after_core.indexes, before_core.indexes + 1);
        assert_eq!(
            after.resource_runtime_request_counter_bytes(),
            Ok(REQUEST_PENDING_ACTION_COUNTER_BYTES_V1)
        );
        assert_eq!(
            after_components
                .indexes
                .checked_sub(before_components.indexes)
                .expect("index bytes grow after request registration"),
            after_wire_indexes
                .checked_sub(before_wire_indexes)
                .expect("wire index bytes grow after request registration")
                + REQUEST_PENDING_ACTION_COUNTER_BYTES_V1
        );

        let effective = scheduled.actions[0].key.effective_processing_at_utc_ms;
        session
            .state
            .close_platform_prefix_through(effective, 1)
            .expect("close resource-counted request prefix");
        for expected_ordinal in 0..2_u16 {
            let action = session
                .state
                .take_next_action_through(effective)
                .expect("take resource-counted action")
                .expect("resource-counted action");
            assert_eq!(action.key.batch_item_ordinal, expected_ordinal);
            session
                .state
                .complete_logical_item(&action.action_identity)
                .expect("complete resource-counted action");
        }
        assert_eq!(
            session
                .state
                .pending_action_count_for_request_for_tests("resource-counted-request"),
            0
        );
        assert_eq!(
            session
                .checkpoint()
                .resource_runtime_request_counter_bytes(),
            Ok(REQUEST_PENDING_ACTION_COUNTER_BYTES_V1),
            "completed request retains its embedded runtime counter storage"
        );
    }

    #[test]
    fn complete_platform_prefix_precedes_stably_sorted_actions() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let first = state
            .register_request(&profile, request(&state, "request-1", 1, 900), sha("d"))
            .expect("request 1");
        let second = state
            .register_request(&profile, request(&state, "request-2", 2, 900), sha("e"))
            .expect("request 2");
        let effective = first
            .transit
            .platform_arrived_at_utc_ms
            .max(second.transit.platform_arrived_at_utc_ms);
        state
            .enqueue_logical_action(
                "action-2".to_owned(),
                "request-2".to_owned(),
                LogicalActionKeyV1 {
                    effective_processing_at_utc_ms: effective,
                    caller_sequence: 2,
                    batch_item_ordinal: 0,
                },
            )
            .expect("later action inserted first");
        state
            .enqueue_logical_action(
                "action-1".to_owned(),
                "request-1".to_owned(),
                LogicalActionKeyV1 {
                    effective_processing_at_utc_ms: effective,
                    caller_sequence: 1,
                    batch_item_ordinal: 0,
                },
            )
            .expect("earlier action inserted second");
        state
            .seal_logical_actions("request-1")
            .expect("seal request 1");
        state
            .seal_logical_actions("request-2")
            .expect("seal request 2");
        assert_eq!(
            state.take_next_action_through(effective),
            Err(SessionError::PlatformPrefixNotClosed)
        );
        state
            .close_platform_prefix_through(effective, 77)
            .expect("complete DPT prefix");
        let first_action = state
            .take_next_action_through(effective)
            .expect("release first action")
            .expect("first action");
        assert_eq!(
            state.take_next_action_through(effective),
            Err(SessionError::ActionProcessingInProgress(
                "action-1".to_owned()
            ))
        );
        assert!(
            state
                .complete_logical_item("action-1")
                .expect("complete first action request")
        );
        let second_action = state
            .take_next_action_through(effective)
            .expect("release second action")
            .expect("second action");
        assert!(
            state
                .complete_logical_item("action-2")
                .expect("complete second action request")
        );
        assert_eq!(
            [first_action.action_identity, second_action.action_identity],
            ["action-1", "action-2"]
        );
        assert_eq!(
            state.enqueue_logical_action(
                "late-action".to_owned(),
                "request-1".to_owned(),
                LogicalActionKeyV1 {
                    effective_processing_at_utc_ms: effective,
                    caller_sequence: 1,
                    batch_item_ordinal: 1,
                },
            ),
            Err(SessionError::ActionWatermarkClosed)
        );
    }

    #[test]
    fn prepared_closed_bucket_actionless_slice_allows_same_time_action_and_rejects_earlier_action()
    {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let registered = state
            .register_request(&profile, request(&state, "same-time", 1, 1_000), sha("c"))
            .expect("登记同刻动作请求");
        let bucket_at_utc_ms = registered.transit.platform_arrived_at_utc_ms;
        state
            .enqueue_logical_action(
                "same-time/item-0".to_owned(),
                "same-time".to_owned(),
                LogicalActionKeyV1 {
                    effective_processing_at_utc_ms: bucket_at_utc_ms,
                    caller_sequence: 1,
                    batch_item_ordinal: 0,
                },
            )
            .expect("登记同刻动作");
        state
            .seal_logical_actions("same-time")
            .expect("封口同刻动作请求");
        state
            .expect_direct_response("same-time")
            .expect("登记直接响应");
        let before = SessionStateCheckpointV1::from(&state);

        let prepared = state
            .prepare_closed_bucket_actionless_slice_v2(
                &profile,
                MarketEventPathV1::HiddenPlatform,
                bucket_at_utc_ms,
                1,
                None,
            )
            .expect("闭桶必须允许同刻动作留待后续事务");
        assert!(prepared.current_action().is_none());
        assert_eq!(
            prepared.logical_platform_driven_through_utc_ms(),
            bucket_at_utc_ms
        );
        drop(prepared);
        assert_eq!(SessionStateCheckpointV1::from(&state), before);

        state
            .prepare_closed_bucket_actionless_slice_v2(
                &profile,
                MarketEventPathV1::HiddenPlatform,
                bucket_at_utc_ms,
                1,
                None,
            )
            .expect("重新准备同刻闭桶")
            .into_committable()
            .expect("闭桶状态切片必须可提交")
            .commit();
        assert_eq!(
            state
                .platform_prefix
                .closed_through_source_event_at_utc_ms(),
            Some(bucket_at_utc_ms)
        );
        assert_eq!(state.closed_action_through_utc_ms, Some(bucket_at_utc_ms));
        assert_eq!(state.pending_actions.len(), 1);
        assert_eq!(
            state
                .prepare_execution_action_slice_v2(&profile, bucket_at_utc_ms, false)
                .expect("同刻动作必须在闭桶提交后可达")
                .current_action()
                .map(|action| action.action_identity),
            Some("same-time/item-0".to_owned())
        );

        let mut earlier = SessionState::new(spec());
        let registered = earlier
            .register_request(&profile, request(&earlier, "earlier", 1, 1_000), sha("d"))
            .expect("登记更早动作请求");
        let action_at_utc_ms = registered.transit.platform_arrived_at_utc_ms;
        earlier
            .enqueue_logical_action(
                "earlier/item-0".to_owned(),
                "earlier".to_owned(),
                LogicalActionKeyV1 {
                    effective_processing_at_utc_ms: action_at_utc_ms,
                    caller_sequence: 1,
                    batch_item_ordinal: 0,
                },
            )
            .expect("登记更早动作");
        earlier
            .seal_logical_actions("earlier")
            .expect("封口更早动作请求");
        earlier
            .expect_direct_response("earlier")
            .expect("登记更早请求直接响应");
        let before = SessionStateCheckpointV1::from(&earlier);
        assert!(matches!(
            earlier.prepare_closed_bucket_actionless_slice_v2(
                &profile,
                MarketEventPathV1::HiddenPlatform,
                action_at_utc_ms + 1,
                1,
                None,
            ),
            Err(SessionError::PlatformPrefixCrossesPendingAction)
        ));
        assert_eq!(SessionStateCheckpointV1::from(&earlier), before);
    }

    #[test]
    fn execution_action_slice_full_session_commit_matches_v1() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let registered = state
            .register_request(
                &profile,
                request(&state, "prepared-action", 1, 1_000),
                sha("d"),
            )
            .expect("register prepared action request");
        let effective = registered.transit.platform_arrived_at_utc_ms;
        state
            .enqueue_logical_action(
                "prepared-action/item-0".to_owned(),
                "prepared-action".to_owned(),
                LogicalActionKeyV1 {
                    effective_processing_at_utc_ms: effective,
                    caller_sequence: 1,
                    batch_item_ordinal: 0,
                },
            )
            .expect("queue prepared action");
        state
            .seal_logical_actions("prepared-action")
            .expect("seal prepared action request");
        state
            .expect_direct_response("prepared-action")
            .expect("expect prepared direct response");
        state
            .close_platform_prefix_through(effective, 1)
            .expect("close prepared action prefix");

        let before = SessionStateCheckpointV1::from(&state);
        let mut existing = state.clone();
        let prepared = state
            .prepare_execution_action_slice_v2(&profile, effective, false)
            .expect("prepare one execution action");
        assert_eq!(
            prepared
                .current_action()
                .map(|action| action.action_identity),
            Some("prepared-action/item-0".to_owned())
        );
        assert!(!prepared.request_completed());
        assert!(prepared.has_state_change());
        assert_eq!(prepared.logical_platform_driven_through_utc_ms(), effective);
        let prepared_action = prepared.current_action().expect("owned prepared action");
        drop(prepared);
        assert_eq!(SessionStateCheckpointV1::from(&state), before);
        assert_eq!(state.in_flight_action_identity, None);

        let action = existing
            .take_next_action_through(effective)
            .expect("take existing action")
            .expect("existing action");
        assert_eq!(action, prepared_action);
        let existing_revision = existing
            .platform_state_revision
            .checked_add(1)
            .expect("existing platform revision");
        existing
            .advance_platform_state_revision(existing_revision)
            .expect("advance existing platform revision");
        let existing_mutation = existing
            .resolve_mutation_idempotency(
                "prepared-action",
                &action.action_identity,
                "prepared-mutation".to_owned(),
                sha("e"),
                "prepared-effect".to_owned(),
            )
            .expect("resolve existing mutation");
        let existing_query = existing
            .resolve_query_revision("prepared-action", &action.action_identity, sha("f"), None)
            .expect("resolve existing query");
        let existing_lifecycle_sequence = existing
            .next_private_emission_sequence()
            .expect("existing lifecycle sequence");
        let existing_lifecycle = existing
            .bind_lifecycle_feedback_timing(
                &profile,
                "prepared-lifecycle".to_owned(),
                effective + 1,
                existing_lifecycle_sequence,
            )
            .expect("bind existing lifecycle timing");
        assert!(
            existing
                .complete_logical_item(&action.action_identity)
                .expect("complete existing action")
        );
        let existing_response_sequence = existing
            .next_private_emission_sequence()
            .expect("existing direct response sequence");
        let existing_response = existing
            .complete_direct_response("prepared-action", effective + 5, existing_response_sequence)
            .expect("complete existing direct response");

        let mut prepared = state
            .prepare_execution_action_slice_v2(&profile, effective, false)
            .expect("reprepare one execution action");
        let action = prepared
            .current_action()
            .expect("prepared action for full commit");
        let prepared_revision = prepared
            .advance_platform_state_revision_once()
            .expect("advance prepared platform revision");
        assert_eq!(prepared_revision, existing_revision);
        let prepared_mutation = prepared
            .prepare_mutation_resolution(
                "prepared-action",
                &action.action_identity,
                "prepared-mutation".to_owned(),
                sha("e"),
                "prepared-effect".to_owned(),
            )
            .expect("prepare mutation resolution");
        let prepared_query = prepared
            .prepare_query_revision("prepared-action", &action.action_identity, sha("f"))
            .expect("prepare query revision");
        assert_eq!(
            prepared.query_revision_for_request("prepared-action"),
            Some(prepared_revision)
        );
        let prepared_lifecycle_sequence = prepared
            .next_private_emission_sequence()
            .expect("prepared lifecycle sequence");
        let prepared_lifecycle = prepared
            .prepare_lifecycle_feedback_timing(
                "prepared-lifecycle".to_owned(),
                effective + 1,
                prepared_lifecycle_sequence,
            )
            .expect("prepare lifecycle timing");
        prepared
            .finish_current_action(&action.action_identity)
            .expect("finish prepared action before direct response");
        assert!(prepared.request_completed());
        let prepared_response_sequence = prepared
            .next_private_emission_sequence()
            .expect("prepared direct response sequence");
        assert!(prepared_lifecycle_sequence < prepared_response_sequence);
        let prepared_response = prepared
            .prepare_complete_direct_response(
                "prepared-action",
                effective + 5,
                prepared_response_sequence,
            )
            .expect("prepare direct response");
        assert_eq!(prepared_mutation, existing_mutation);
        assert_eq!(prepared_query, existing_query);
        assert_eq!(prepared_lifecycle, existing_lifecycle);
        assert_eq!(prepared_response, existing_response);
        prepared
            .into_committable()
            .expect("all prepared actions are finished")
            .commit();
        assert_eq!(
            SessionStateCheckpointV1::from(&state),
            SessionStateCheckpointV1::from(&existing)
        );
        assert_eq!(state.in_flight_action_identity, None);
    }

    #[test]
    fn execution_action_slice_bounds_same_request_work_and_rebuilds_runtime_counts() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let target = state
            .register_request(
                &profile,
                request(&state, "bounded-target", 1, 1_000),
                sha("d"),
            )
            .expect("register bounded target");
        let target_effective = target.transit.platform_arrived_at_utc_ms;
        for ordinal in 0..3_u16 {
            state
                .enqueue_logical_action(
                    format!("bounded-target/item-{ordinal}"),
                    "bounded-target".to_owned(),
                    LogicalActionKeyV1 {
                        effective_processing_at_utc_ms: target_effective,
                        caller_sequence: 1,
                        batch_item_ordinal: ordinal,
                    },
                )
                .expect("queue bounded target item");
        }
        state
            .seal_logical_actions("bounded-target")
            .expect("seal bounded target");
        state
            .expect_direct_response("bounded-target")
            .expect("track bounded target direct response");

        let unrelated_id = "unrelated";
        let unrelated = state
            .register_request(&profile, request(&state, unrelated_id, 2, 2_000), sha("e"))
            .expect("register unrelated request");
        let unrelated_effective = unrelated
            .transit
            .platform_arrived_at_utc_ms
            .max(target_effective + 1);
        for ordinal in 0..128_u16 {
            state
                .enqueue_logical_action(
                    format!("{unrelated_id}/item-{ordinal}"),
                    unrelated_id.to_owned(),
                    LogicalActionKeyV1 {
                        effective_processing_at_utc_ms: unrelated_effective,
                        caller_sequence: 2,
                        batch_item_ordinal: ordinal,
                    },
                )
                .expect("queue unrelated action");
        }
        state
            .seal_logical_actions(unrelated_id)
            .expect("seal unrelated request");
        state
            .expect_direct_response(unrelated_id)
            .expect("track unrelated direct response");
        state
            .close_platform_prefix_through(target_effective, 1)
            .expect("close bounded target prefix");
        assert_eq!(
            state.pending_action_count_for_request_for_tests("bounded-target"),
            3
        );
        assert_eq!(
            state.pending_action_count_for_request_for_tests(unrelated_id),
            128
        );

        let before = SessionStateCheckpointV1::from(&state);
        let encoded = serde_json::to_vec(&before).expect("encode V1 session state checkpoint");
        assert!(!String::from_utf8_lossy(&encoded).contains("remaining_pending_action_count"));
        let decoded: SessionStateCheckpointV1 =
            serde_json::from_slice(&encoded).expect("decode unchanged V1 session state checkpoint");
        assert_eq!(decoded, before);
        let mut legacy = decoded
            .restore(spec())
            .expect("restore runtime pending-action counters once");
        assert_eq!(
            legacy.pending_action_count_for_request_for_tests("bounded-target"),
            3
        );
        assert_eq!(
            legacy.pending_action_count_for_request_for_tests(unrelated_id),
            128
        );
        reset_pending_action_entry_visits_for_tests();
        for expected_ordinal in 0..3_u16 {
            let action = legacy
                .take_next_action_through(target_effective)
                .expect("take bounded legacy action")
                .expect("bounded legacy action");
            assert_eq!(action.key.batch_item_ordinal, expected_ordinal);
            assert_eq!(
                legacy.pending_action_count_for_request_for_tests("bounded-target"),
                usize::from(2 - expected_ordinal)
            );
            let completed = legacy
                .complete_logical_item(&action.action_identity)
                .expect("complete bounded legacy action");
            assert_eq!(completed, expected_ordinal == 2);
        }
        assert_eq!(pending_action_entry_visits_for_tests(), 0);
        assert_eq!(
            legacy.pending_action_count_for_request_for_tests("bounded-target"),
            0
        );
        assert_eq!(
            legacy.pending_action_count_for_request_for_tests(unrelated_id),
            128
        );

        reset_pending_action_entry_visits_for_tests();
        let mut dropped = state
            .prepare_execution_action_slice_v2(&profile, target_effective, true)
            .expect("prepare bounded target batch");
        while let Some(action) = dropped.current_action() {
            dropped
                .finish_current_action(&action.action_identity)
                .expect("finish dropped bounded target item");
        }
        assert!(dropped.request_completed());
        drop(dropped);
        assert!((3..=6).contains(&pending_action_entry_visits_for_tests()));
        assert_eq!(
            state.pending_action_count_for_request_for_tests("bounded-target"),
            3
        );
        assert_eq!(SessionStateCheckpointV1::from(&state), before);

        reset_pending_action_entry_visits_for_tests();
        let mut prepared = state
            .prepare_execution_action_slice_v2(&profile, target_effective, true)
            .expect("reprepare bounded target batch");
        while let Some(action) = prepared.current_action() {
            prepared
                .finish_current_action(&action.action_identity)
                .expect("finish committed bounded target item");
        }
        prepared
            .into_committable()
            .expect("bounded target batch is committable")
            .commit();
        assert!((3..=6).contains(&pending_action_entry_visits_for_tests()));
        assert_eq!(
            state.pending_action_count_for_request_for_tests("bounded-target"),
            0
        );
        assert_eq!(
            state.pending_action_count_for_request_for_tests(unrelated_id),
            128
        );
        assert_eq!(
            SessionStateCheckpointV1::from(&state),
            SessionStateCheckpointV1::from(&legacy)
        );
    }

    #[test]
    fn execution_action_slice_cancel_batch_reads_its_prepared_mutation() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let registered = state
            .register_request(
                &profile,
                request(&state, "prepared-cancel-mutation", 1, 1_000),
                sha("d"),
            )
            .expect("register cancel mutation batch");
        let effective = registered.transit.platform_arrived_at_utc_ms;
        for ordinal in 0..2_u16 {
            state
                .enqueue_logical_action(
                    format!("prepared-cancel-mutation/item-{ordinal}"),
                    "prepared-cancel-mutation".to_owned(),
                    LogicalActionKeyV1 {
                        effective_processing_at_utc_ms: effective,
                        caller_sequence: 1,
                        batch_item_ordinal: ordinal,
                    },
                )
                .expect("queue cancel mutation item");
        }
        state
            .seal_logical_actions("prepared-cancel-mutation")
            .expect("seal cancel mutation batch");
        state
            .close_platform_prefix_through(effective, 1)
            .expect("close cancel mutation prefix");

        let mut prepared = state
            .prepare_execution_action_slice_v2(&profile, effective, true)
            .expect("prepare cancel mutation batch");
        let first = prepared
            .current_action()
            .expect("first cancel mutation action");
        let first_resolution = prepared
            .prepare_mutation_resolution(
                "prepared-cancel-mutation",
                &first.action_identity,
                "shared-cancel-mutation".to_owned(),
                sha("e"),
                "first-effect".to_owned(),
            )
            .expect("first mutation is new");
        prepared
            .finish_current_action(&first.action_identity)
            .expect("finish first cancel mutation action");
        let second = prepared
            .current_action()
            .expect("second cancel mutation action");
        let second_resolution = prepared
            .prepare_mutation_resolution(
                "prepared-cancel-mutation",
                &second.action_identity,
                "shared-cancel-mutation".to_owned(),
                sha("e"),
                "ignored-second-effect".to_owned(),
            )
            .expect("second mutation reuses prepared first mutation");
        prepared
            .finish_current_action(&second.action_identity)
            .expect("finish second cancel mutation action");
        assert_eq!(
            first_resolution.disposition,
            MutationResolutionDispositionV1::NewEffect
        );
        assert_eq!(
            second_resolution.disposition,
            MutationResolutionDispositionV1::ExistingEffect
        );
        assert_eq!(second_resolution.effect_identity, "first-effect");
        prepared
            .into_committable()
            .expect("ordered cancel mutation batch is committable")
            .commit();

        assert_eq!(state.mutations.len(), 1);
        assert_eq!(
            state
                .mutations
                .get("shared-cancel-mutation")
                .map(|record| record.effect_identity.as_str()),
            Some("first-effect")
        );
        assert!(
            state
                .released_request_ids
                .contains("prepared-cancel-mutation")
        );
        assert_eq!(state.in_flight_action_identity, None);
    }

    #[test]
    fn execution_action_slice_rejects_reverse_cancel_item_before_state_change() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let registered = state
            .register_request(
                &profile,
                request(&state, "prepared-reverse-cancel", 1, 1_000),
                sha("d"),
            )
            .expect("register reverse cancel batch");
        let effective = registered.transit.platform_arrived_at_utc_ms;
        for ordinal in 0..2_u16 {
            state
                .enqueue_logical_action(
                    format!("prepared-reverse-cancel/item-{ordinal}"),
                    "prepared-reverse-cancel".to_owned(),
                    LogicalActionKeyV1 {
                        effective_processing_at_utc_ms: effective,
                        caller_sequence: 1,
                        batch_item_ordinal: ordinal,
                    },
                )
                .expect("queue reverse cancel item");
        }
        state
            .seal_logical_actions("prepared-reverse-cancel")
            .expect("seal reverse cancel batch");
        state
            .close_platform_prefix_through(effective, 1)
            .expect("close reverse cancel prefix");
        let before = SessionStateCheckpointV1::from(&state);

        let mut prepared = state
            .prepare_execution_action_slice_v2(&profile, effective, true)
            .expect("prepare reverse cancel batch");
        let second_identity = "prepared-reverse-cancel/item-1";
        assert_eq!(
            prepared.prepare_mutation_resolution(
                "prepared-reverse-cancel",
                second_identity,
                "reverse-cancel-mutation".to_owned(),
                sha("e"),
                "reverse-effect".to_owned(),
            ),
            Err(SessionError::ActionNotLogicallyReleased(
                second_identity.to_owned()
            ))
        );
        let preparation_failed =
            SessionError::ExecutionState("执行动作准备已经失败，必须丢弃当前准备结果".to_owned());
        assert_eq!(
            prepared.finish_current_action(second_identity),
            Err(preparation_failed.clone())
        );
        let first = prepared
            .current_action()
            .expect("first cancel action remains current");
        assert_eq!(
            prepared.prepare_mutation_resolution(
                "prepared-reverse-cancel",
                &first.action_identity,
                "correct-after-reverse".to_owned(),
                sha("e"),
                "correct-effect".to_owned(),
            ),
            Err(preparation_failed.clone())
        );
        assert_eq!(
            prepared.finish_current_action(&first.action_identity),
            Err(preparation_failed.clone())
        );
        assert_eq!(prepared.into_committable().err(), Some(preparation_failed));
        assert_eq!(SessionStateCheckpointV1::from(&state), before);

        let mut reverse_finish = state
            .prepare_execution_action_slice_v2(&profile, effective, true)
            .expect("reprepare cancel batch for reverse finish");
        assert_eq!(
            reverse_finish.finish_current_action(second_identity),
            Err(SessionError::ActionNotLogicallyReleased(
                second_identity.to_owned()
            ))
        );
        let first = reverse_finish
            .current_action()
            .expect("reverse finish keeps first action current");
        let preparation_failed =
            SessionError::ExecutionState("执行动作准备已经失败，必须丢弃当前准备结果".to_owned());
        assert_eq!(
            reverse_finish.finish_current_action(&first.action_identity),
            Err(preparation_failed.clone())
        );
        assert_eq!(
            reverse_finish.into_committable().err(),
            Some(preparation_failed)
        );
        assert_eq!(SessionStateCheckpointV1::from(&state), before);

        let mut missing = state
            .prepare_execution_action_slice_v2(&profile, effective, true)
            .expect("reprepare cancel batch for missing-item check");
        let first = missing.current_action().expect("first missing-item action");
        missing
            .finish_current_action(&first.action_identity)
            .expect("finish only the first action without prior error");
        assert_eq!(
            missing.into_committable().err(),
            Some(SessionError::ActionNotLogicallyReleased(
                second_identity.to_owned()
            ))
        );
        assert_eq!(SessionStateCheckpointV1::from(&state), before);
        assert_eq!(state.in_flight_action_identity, None);
    }

    #[test]
    fn execution_action_slice_drop_or_later_prepare_error_keeps_state_unchanged() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let registered = state
            .register_request(
                &profile,
                request(&state, "prepared-rollback", 1, 1_000),
                sha("d"),
            )
            .expect("register prepared rollback request");
        let effective = registered.transit.platform_arrived_at_utc_ms;
        state
            .enqueue_logical_action(
                "prepared-rollback/item-0".to_owned(),
                "prepared-rollback".to_owned(),
                LogicalActionKeyV1 {
                    effective_processing_at_utc_ms: effective,
                    caller_sequence: 1,
                    batch_item_ordinal: 0,
                },
            )
            .expect("queue prepared rollback action");
        state
            .seal_logical_actions("prepared-rollback")
            .expect("seal prepared rollback request");
        state
            .close_platform_prefix_through(effective, 1)
            .expect("close prepared rollback prefix");
        let before = SessionStateCheckpointV1::from(&state);

        let mut dropped = state
            .prepare_execution_action_slice_v2(&profile, effective, false)
            .expect("prepare dropped overlay");
        let action = dropped.current_action().expect("dropped overlay action");
        dropped
            .prepare_mutation_resolution(
                "prepared-rollback",
                &action.action_identity,
                "dropped-mutation".to_owned(),
                sha("e"),
                "dropped-effect".to_owned(),
            )
            .expect("prepare mutation before drop");
        drop(dropped);
        assert_eq!(SessionStateCheckpointV1::from(&state), before);

        let mut failed = state
            .prepare_execution_action_slice_v2(&profile, effective, false)
            .expect("prepare failing overlay");
        let action = failed.current_action().expect("failing overlay action");
        failed
            .prepare_mutation_resolution(
                "prepared-rollback",
                &action.action_identity,
                "failed-mutation".to_owned(),
                sha("e"),
                "failed-effect".to_owned(),
            )
            .expect("prepare mutation before later error");
        let revision = failed
            .advance_platform_state_revision_once()
            .expect("prepare virtual platform revision");
        assert_eq!(failed.platform_state_revision(), revision);
        assert_eq!(
            failed.prepare_query_revision_at(
                "prepared-rollback",
                &action.action_identity,
                sha("f"),
                revision + 1,
            ),
            Err(SessionError::QueryIdentityConflict(
                "prepared-rollback".to_owned()
            ))
        );
        let preparation_failed =
            SessionError::ExecutionState("执行动作准备已经失败，必须丢弃当前准备结果".to_owned());
        assert_eq!(
            failed.finish_current_action(&action.action_identity),
            Err(preparation_failed.clone())
        );
        assert_eq!(failed.into_committable().err(), Some(preparation_failed));
        assert_eq!(SessionStateCheckpointV1::from(&state), before);
        assert_eq!(state.in_flight_action_identity, None);
    }

    #[test]
    fn execution_action_slice_private_emission_sequence_is_contiguous_and_drop_is_read_only() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let cutoff = 10_000;
        let mut state = SessionState::new(spec());
        state
            .close_platform_prefix_through(cutoff, 1)
            .expect("close empty prefix for private reservations");
        let before = SessionStateCheckpointV1::from(&state);
        let preparation_failed =
            SessionError::ExecutionState("执行动作准备已经失败，必须丢弃当前准备结果".to_owned());

        for invalid in [0, 2, u64::MAX] {
            let mut prepared = state
                .prepare_execution_action_slice_v2(&profile, cutoff, false)
                .expect("prepare invalid private reservation overlay");
            assert_eq!(
                prepared.prepare_lifecycle_feedback_timing(
                    format!("invalid-sequence-{invalid}"),
                    1_000,
                    invalid,
                ),
                Err(SessionError::PrivateEmissionSequenceConflict(invalid))
            );
            assert_eq!(
                prepared.next_private_emission_sequence(),
                Err(preparation_failed.clone())
            );
            assert_eq!(
                prepared.into_committable().err(),
                Some(preparation_failed.clone())
            );
            assert_eq!(SessionStateCheckpointV1::from(&state), before);
        }
        let mut prepared = state
            .prepare_execution_action_slice_v2(&profile, cutoff, false)
            .expect("prepare repeated private reservation overlay");
        let first = prepared
            .prepare_lifecycle_feedback_timing("repeated-feedback".to_owned(), 1_000, 1)
            .expect("prepare the next continuous emission sequence");
        assert_eq!(
            prepared.prepare_lifecycle_feedback_timing("repeated-feedback".to_owned(), 1_000, 1,),
            Ok(first)
        );
        assert_eq!(
            prepared.prepare_lifecycle_feedback_timing("repeated-feedback".to_owned(), 1_000, 2,),
            Err(SessionError::FeedbackIdentityConflict(
                "repeated-feedback".to_owned()
            ))
        );
        assert_eq!(prepared.into_committable().err(), Some(preparation_failed));
        assert_eq!(SessionStateCheckpointV1::from(&state), before);
        assert_eq!(state.in_flight_action_identity, None);
    }

    #[test]
    fn execution_action_slice_no_action_prepares_and_commits_only_the_watermark() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let cutoff = 10_000;
        let mut state = SessionState::new(spec());
        state
            .close_platform_prefix_through(cutoff, 1)
            .expect("close empty execution prefix");
        let before = SessionStateCheckpointV1::from(&state);
        let prepared = state
            .prepare_execution_action_slice_v2(&profile, cutoff, false)
            .expect("prepare empty action slice");
        assert!(prepared.current_action().is_none());
        assert!(!prepared.request_completed());
        assert!(prepared.has_state_change());
        assert_eq!(prepared.logical_platform_driven_through_utc_ms(), cutoff);
        drop(prepared);
        assert_eq!(SessionStateCheckpointV1::from(&state), before);

        state
            .prepare_execution_action_slice_v2(&profile, cutoff, false)
            .expect("reprepare empty action slice")
            .into_committable()
            .expect("empty action slice is immediately committable")
            .commit();
        assert_eq!(state.closed_action_through_utc_ms, Some(cutoff));
        let same_cutoff = state
            .prepare_execution_action_slice_v2(&profile, cutoff, false)
            .expect("prepare same empty cutoff");
        assert!(!same_cutoff.has_state_change());
        same_cutoff
            .into_committable()
            .expect("same empty cutoff is immediately committable")
            .commit();
        assert_eq!(state.closed_action_through_utc_ms, Some(cutoff));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scheduled_session_drive_resolution_paths_distinguish_absent_from_conflicting_evidence()
    {
        let mut session = private_fact_slice_test_session("scheduled-drive-resolution-paths").await;
        let condition_id = session
            .scenario()
            .market_set()
            .markets()
            .first()
            .expect("session drive 判定路径测试市场")
            .market()
            .condition_id
            .0
            .clone();
        let initial = [
            initial_snapshot_event("1", 0, "Up", 1_100, 1_200, 1, "10"),
            initial_snapshot_event("2", 1, "Down", 1_100, 1_200, 2, "10"),
        ];
        assert_eq!(
            session.apply_closed_market_event_bucket(hidden_bucket(1_100, 2, &initial)),
            Ok(MarketBucketApplyDispositionV1::Applied)
        );
        assert_eq!(
            session.apply_closed_market_event_bucket(public_market_bucket_for_session_test(
                1_200, 2, &initial,
            )),
            Ok(MarketBucketApplyDispositionV1::Applied)
        );

        let (hidden_resolution_event, resolution) =
            resolution_event_for_session_test(0, 2_001, 2_100, 3);
        assert_eq!(
            session.apply_closed_market_event_bucket(hidden_bucket(
                2_001,
                3,
                &[hidden_resolution_event],
            )),
            Ok(MarketBucketApplyDispositionV1::Applied)
        );
        let before_absent_path_prepare = session.checkpoint();
        let pending_before_absent_path_prepare =
            session.pending_terminal_projection_condition_ids();
        let (hidden_delta, public_delta) = prepare_redeemed_resolution_terminal_paths_v2(
            session.hidden_platform_markets(),
            session.public_markets(),
            &condition_id,
            &resolution,
        )
        .expect("授权 hidden 精确、public 尚无判定时至少一路适用");
        assert!(hidden_delta.is_some());
        assert!(public_delta.is_none());
        drop((hidden_delta, public_delta));
        assert_eq!(session.checkpoint(), before_absent_path_prepare);
        assert_eq!(
            session.pending_terminal_projection_condition_ids(),
            pending_before_absent_path_prepare
        );

        let (public_conflict_event, _) = resolution_event_for_session_test(1, 2_002, 2_101, 3);
        assert_eq!(
            session.apply_closed_market_event_bucket(public_market_bucket_for_session_test(
                2_101,
                3,
                &[public_conflict_event],
            )),
            Ok(MarketBucketApplyDispositionV1::Applied)
        );
        let before_conflict = session.checkpoint();
        let pending_before_conflict = session.pending_terminal_projection_condition_ids();
        let conflict = match prepare_redeemed_resolution_terminal_paths_v2(
            session.hidden_platform_markets(),
            session.public_markets(),
            &condition_id,
            &resolution,
        ) {
            Err(error) => error,
            Ok(_) => panic!("任一路已有相反合格判定正文时必须严格失败"),
        };
        assert!(matches!(
            conflict,
            MarketEventErrorV1::ResolutionConflict(actual) if actual == condition_id
        ));
        assert_eq!(session.checkpoint(), before_conflict);
        assert_eq!(
            session.pending_terminal_projection_condition_ids(),
            pending_before_conflict
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scheduled_session_drive_splits_fields_uses_actual_cutoff_and_clears_exact_pending_union()
     {
        let cutoff = 1_500;
        let mut idle = private_fact_slice_test_session("scheduled-drive-idle").await;
        idle.close_platform_prefix_through(cutoff, 1)
            .expect("闭合无动作 session drive 平台前缀");
        let idle_before = idle.checkpoint();
        let idle_pending_before = idle.pending_terminal_projection_condition_ids();
        {
            let prepared = idle
                .prepare_scheduled_session_drive_v2(cutoff, false)
                .expect("准备无动作 session drive");
            assert!(prepared.current_action().is_none());
            assert_eq!(prepared.actual_cutoff_utc_ms(), cutoff);
            assert_eq!(prepared.first_due_at_utc_ms(), None);
            assert!(prepared.execution_candidate().has_state_change());
            assert!(prepared.terminal_projection_condition_ids().is_empty());
        }
        assert_eq!(idle.checkpoint(), idle_before);
        assert_eq!(
            idle.pending_terminal_projection_condition_ids(),
            idle_pending_before
        );

        let condition_id = idle
            .scenario()
            .market_set()
            .markets()
            .first()
            .expect("session drive 测试市场")
            .market()
            .condition_id
            .0
            .clone();
        idle.record_terminal_projection_pending(&condition_id);
        let pending_before = idle.checkpoint();
        let pending_ids_before = idle.pending_terminal_projection_condition_ids();
        let wrong_set_error = match idle
            .prepare_scheduled_session_drive_v2(cutoff, false)
            .expect("准备已有终态投影 pending union")
            .into_committable(BTreeSet::new())
        {
            Err(error) => error,
            Ok(_) => panic!("查询准备集合缺项必须在提交前拒绝"),
        };
        assert!(matches!(
            wrong_set_error,
            MarketEventErrorV1::Session(SessionError::QueryIdentityConflict(identity))
                if identity == "scheduled-terminal-projection-condition-set"
        ));
        assert_eq!(idle.checkpoint(), pending_before);
        assert_eq!(
            idle.pending_terminal_projection_condition_ids(),
            pending_ids_before
        );

        let prepared_ids = BTreeSet::from([condition_id]);
        idle.prepare_scheduled_session_drive_v2(cutoff, false)
            .expect("重新准备精确终态投影 pending union")
            .into_committable(prepared_ids)
            .expect("精确准备集合必须可提交")
            .commit();
        assert!(idle.pending_terminal_projection_condition_ids().is_empty());
        assert_eq!(idle.logical_platform_driven_through(), Some(cutoff));

        let mut with_action = private_fact_slice_test_session("scheduled-drive-action").await;
        let context = request(&with_action.state, "scheduled-drive-action", 1, 1_000);
        let schedule = with_action
            .register_and_schedule_request(
                context,
                sha("d"),
                vec![ParsedLogicalActionV1 {
                    action_identity: "scheduled-drive-action/item-0".to_owned(),
                    batch_item_ordinal: 0,
                    method_timing: ParsedMethodTimingV1::Ordinary,
                }],
            )
            .expect("正式登记 scheduled session drive 动作");
        let action_at = schedule.actions[0].key.effective_processing_at_utc_ms;
        let requested_cutoff = action_at;
        with_action
            .close_platform_prefix_through(requested_cutoff, 1)
            .expect("闭合有动作 session drive 平台前缀");
        let expected_first_due = [
            with_action.execution_state().next_gtd_deadline_at_utc_ms(),
            with_action
                .execution_state()
                .next_scheduled_finality_at_utc_ms(),
            with_action.execution_state().next_rebate_clock_at_utc_ms(),
        ]
        .into_iter()
        .flatten()
        .filter(|at| *at <= action_at)
        .min();
        let action_before = with_action.checkpoint();
        let action_pending_before = with_action.pending_terminal_projection_condition_ids();
        {
            let prepared = with_action
                .prepare_scheduled_session_drive_v2(requested_cutoff, false)
                .expect("准备有动作 session drive");
            assert_eq!(
                prepared
                    .current_action()
                    .map(|action| action.action_identity),
                Some("scheduled-drive-action/item-0".to_owned())
            );
            assert_eq!(prepared.actual_cutoff_utc_ms(), action_at);
            assert_eq!(prepared.first_due_at_utc_ms(), expected_first_due);
        }
        assert_eq!(with_action.checkpoint(), action_before);
        assert_eq!(
            with_action.pending_terminal_projection_condition_ids(),
            action_pending_before
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scheduled_session_drive_order_action_forwarding_is_read_only_and_sticky() {
        let mut source = private_fact_slice_test_session("scheduled-drive-order-action").await;
        let request_id = "scheduled-drive-order-action";
        let action_identity = "scheduled-drive-order-action/item-0";
        let context = request(&source.state, request_id, 1, 1_000);
        let schedule = source
            .register_and_schedule_request(
                context,
                sha("d"),
                vec![ParsedLogicalActionV1 {
                    action_identity: action_identity.to_owned(),
                    batch_item_ordinal: 0,
                    method_timing: ParsedMethodTimingV1::Ordinary,
                }],
            )
            .expect("正式登记单项 Order 查询动作");
        let action_at = schedule.actions[0].key.effective_processing_at_utc_ms;
        source
            .close_platform_prefix_through(action_at, 1)
            .expect("闭合 Order 查询动作的平台前缀");
        let source_checkpoint = source.checkpoint();
        let source_pending = source.pending_terminal_projection_condition_ids();

        let mut successful = source.clone();
        let successful_before = successful.checkpoint();
        let successful_pending_before = successful.pending_terminal_projection_condition_ids();
        let mut prepared = successful
            .prepare_scheduled_session_drive_v2(action_at, false)
            .expect("准备含单项 Order 查询动作的 scheduled candidate");
        let action = prepared.current_action().expect("存在单项 Order 查询动作");
        assert_eq!(action.action_identity, action_identity);
        assert!(prepared.execution_candidate().has_state_change());
        let scheduled_revision = prepared
            .advance_platform_state_revision_once()
            .expect("scheduled candidate 先推进平台修订号");
        let frozen_revision = scheduled_revision
            .checked_sub(1)
            .expect("scheduled candidate 必须真实推进平台修订号");
        let query = prepared
            .prepare_query_revision_at(request_id, action_identity, sha("e"), frozen_revision)
            .expect("在 scheduled candidate 后绑定既有查询根冻结修订号");
        assert_eq!(query.platform_state_revision, frozen_revision);
        assert_eq!(
            prepared.query_revision_for_request(request_id),
            Some(frozen_revision)
        );
        prepared
            .finish_current_action(action_identity)
            .expect("按序完成单项 Order 查询动作");
        assert!(prepared.request_completed());
        let private_emission_sequence = prepared
            .next_private_emission_sequence()
            .expect("分配直接响应私有发射序号");
        let response_transit = prepared
            .prepare_complete_direct_response(request_id, action_at, private_emission_sequence)
            .expect("完成单项 Order 查询直接响应传输");
        assert_eq!(
            response_transit.platform_responded_at_utc_ms,
            Some(action_at)
        );
        assert_eq!(
            response_transit.private_emission_sequence,
            Some(private_emission_sequence)
        );
        assert!(response_transit.response_received_at_utc_ms.is_some());
        let prepared_condition_ids = prepared.terminal_projection_condition_ids().clone();
        let committable = prepared
            .into_committable(prepared_condition_ids)
            .expect("完整 Order 查询准备可进入只含 void commit 的形态");
        drop(committable);
        assert_eq!(successful.checkpoint(), successful_before);
        assert_eq!(
            successful.pending_terminal_projection_condition_ids(),
            successful_pending_before
        );

        let preparation_failed =
            SessionError::ExecutionState("执行动作准备已经失败，必须丢弃当前准备结果".to_owned());
        let mut failed = source.clone();
        let failed_before = failed.checkpoint();
        let failed_pending_before = failed.pending_terminal_projection_condition_ids();
        let mut prepared = failed
            .prepare_scheduled_session_drive_v2(action_at, false)
            .expect("重新准备 sticky 失败路径");
        let prepared_condition_ids = prepared.terminal_projection_condition_ids().clone();
        assert!(matches!(
            prepared.prepare_query_revision_at(
                request_id,
                "wrong-order-action",
                sha("e"),
                scheduled_revision,
            ),
            Err(SessionError::ActionNotLogicallyReleased(identity))
                if identity == "wrong-order-action"
        ));
        assert_eq!(prepared.query_revision_for_request(request_id), None);
        assert!(!prepared.request_completed());
        assert_eq!(
            prepared.finish_current_action(action_identity),
            Err(preparation_failed.clone())
        );
        assert_eq!(
            prepared.next_private_emission_sequence(),
            Err(preparation_failed.clone())
        );
        assert_eq!(
            prepared.prepare_complete_direct_response(request_id, action_at, 1),
            Err(preparation_failed.clone())
        );
        assert!(matches!(
            prepared.into_committable(prepared_condition_ids),
            Err(MarketEventErrorV1::Session(error)) if error == preparation_failed
        ));
        assert_eq!(failed.checkpoint(), failed_before);
        assert_eq!(
            failed.pending_terminal_projection_condition_ids(),
            failed_pending_before
        );
        assert_eq!(source.checkpoint(), source_checkpoint);
        assert_eq!(
            source.pending_terminal_projection_condition_ids(),
            source_pending
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scheduled_session_drive_caller_cancel_forwarding_consumes_and_preserves_candidate() {
        let mut session = private_fact_slice_test_session("scheduled-drive-caller-cancel").await;
        let request_id = "scheduled-drive-caller-cancel";
        let action_identity = "scheduled-drive-caller-cancel/item-0";
        let platform_order_id = "scheduled-drive-caller-cancel-order";
        let context = request(&session.state, request_id, 1, 1_000);
        let schedule = session
            .register_and_schedule_request(
                context,
                sha("d"),
                vec![ParsedLogicalActionV1 {
                    action_identity: action_identity.to_owned(),
                    batch_item_ordinal: 0,
                    method_timing: ParsedMethodTimingV1::Ordinary,
                }],
            )
            .expect("正式登记单项 caller-cancel 动作");
        let action_at = schedule.actions[0].key.effective_processing_at_utc_ms;
        let condition_id = session.scenario().market_set().markets()[0]
            .market()
            .condition_id
            .0
            .clone();
        let token_id = session.scenario().market_set().markets()[0].assets()[0]
            .token_id
            .0
            .clone();
        session
            .execution_state_mut()
            .commit_new_order(
                crate::matching::SimOrderRecordV1 {
                    platform_order_id: platform_order_id.to_owned(),
                    global_request_id: "scheduled-drive-caller-cancel-source".to_owned(),
                    item_id: "scheduled-drive-caller-cancel-source-item".to_owned(),
                    item_idempotency_identity: "scheduled-drive-caller-cancel-source-idempotency"
                        .to_owned(),
                    condition_id,
                    token_id,
                    created_at_utc_ms: action_at - 1,
                    last_updated_at_utc_ms: action_at - 1,
                    kind: crate::matching::SimOrderKindStateV1::Gtd {
                        expires_at_utc_ms: action_at + 10_000,
                    },
                    source_price_pusd_per_share: rust_decimal::Decimal::new(4, 1),
                    accepted_base_units: Some(1_000_000),
                    matched_base_units: 0,
                    pending_maker_depletion_base_units: 0,
                    pending_maker_depletion_evidence: None,
                    open_base_units: 1_000_000,
                    expired_base_units: 0,
                    expired_released_quote_units: 0,
                    expired_at_utc_ms: None,
                    gtd_expiration_identity: None,
                    venue_dust_closed_base_units: 0,
                    venue_dust_released_quote_units: 0,
                    venue_dust_at_utc_ms: None,
                    venue_dust_identity: None,
                    market_resolved_canceled_base_units: 0,
                    market_resolved_released_quote_units: 0,
                    market_resolved_at_utc_ms: None,
                    market_resolution_effect_identity: None,
                    canceled_base_units: 0,
                    canceled_released_quote_units: 0,
                    canceled_at_utc_ms: None,
                    caller_cancel_effect_identity: None,
                    terminalized_pending_maker_depletion_evidence: None,
                    accepted_quote_units: 400_000,
                    matched_quote_units: 0,
                    open_quote_units: 400_000,
                    released_quote_units: 0,
                    sdk_taker_amount_audit_base_units: None,
                    immediate_taker_fee_encumbered_units: 0,
                    terminal_reason: None,
                },
                Vec::new(),
            )
            .expect("建立可区分真实转发的活动 GTD 订单");
        session
            .close_platform_prefix_through(action_at, 1)
            .expect("闭合 caller-cancel 动作的平台前缀");
        let before = session.checkpoint();
        let pending_before = session.pending_terminal_projection_condition_ids();

        let mut prepared = session
            .prepare_scheduled_session_drive_v2(action_at, false)
            .expect("准备含 caller-cancel 动作的 scheduled candidate");
        let resolution = prepared
            .prepare_mutation_resolution(
                request_id,
                action_identity,
                "cancel-idempotency".to_owned(),
                sha("e"),
                "cancel-effect".to_owned(),
            )
            .expect("首次撤单幂等身份形成新 effect");
        assert_eq!(
            resolution.disposition,
            MutationResolutionDispositionV1::NewEffect
        );
        assert_eq!(resolution.effect_identity, "cancel-effect");
        let (prepared, disposition) = prepared
            .prepare_caller_cancel_remainder(platform_order_id, action_at, &sha("e"))
            .expect("活动 caller-cancel 必须转发到底层候选");
        let crate::matching::CancelOrderStateDispositionV1::Canceled(effect) = disposition else {
            panic!("活动 caller-cancel 不得被包装层降级为无效果")
        };
        assert_eq!(effect.open_base_units_before, 1_000_000);
        let candidate = prepared
            .execution_candidate()
            .candidate_order(platform_order_id)
            .expect("撤单候选保留目标订单 after-image");
        assert_eq!(candidate.open_base_units, 0);
        assert_eq!(candidate.canceled_base_units, 1_000_000);
        assert_eq!(candidate.caller_cancel_effect_identity, Some(sha("e")));
        let mut prepared = prepared;
        assert_eq!(
            prepared.prepare_mutation_resolution(
                request_id,
                action_identity,
                "cancel-idempotency".to_owned(),
                sha("f"),
                "conflicting-effect".to_owned(),
            ),
            Err(SessionError::MutationIdentityConflict(
                "cancel-idempotency".to_owned()
            ))
        );
        let preparation_failed =
            SessionError::ExecutionState("执行动作准备已经失败，必须丢弃当前准备结果".to_owned());
        assert_eq!(
            prepared.finish_current_action(action_identity),
            Err(preparation_failed.clone())
        );
        let prepared_condition_ids = prepared.terminal_projection_condition_ids().clone();
        assert!(matches!(
            prepared.into_committable(prepared_condition_ids),
            Err(MarketEventErrorV1::Session(error)) if error == preparation_failed
        ));
        assert_eq!(session.checkpoint(), before);
        assert_eq!(
            session.pending_terminal_projection_condition_ids(),
            pending_before
        );

        let mut prepared = session
            .prepare_scheduled_session_drive_v2(action_at, false)
            .expect("重新准备 caller-cancel 成功共同提交路径");
        prepared
            .prepare_mutation_resolution(
                request_id,
                action_identity,
                "cancel-idempotency".to_owned(),
                sha("e"),
                "cancel-effect".to_owned(),
            )
            .expect("重新准备撤单 mutation");
        let (mut prepared, disposition) = prepared
            .prepare_caller_cancel_remainder(platform_order_id, action_at, &sha("e"))
            .expect("重新准备活动 caller-cancel");
        assert!(matches!(
            disposition,
            crate::matching::CancelOrderStateDispositionV1::Canceled(_)
        ));
        prepared
            .finish_current_action(action_identity)
            .expect("完成 caller-cancel 动作");
        assert!(prepared.request_completed());
        let prepared_condition_ids = prepared.terminal_projection_condition_ids().clone();
        prepared
            .into_committable(prepared_condition_ids)
            .expect("mutation 与 caller-cancel 候选必须共同可提交")
            .commit();

        let committed = session
            .execution_state()
            .order(platform_order_id)
            .expect("提交后保留撤单终态订单");
        assert_eq!(committed.open_base_units, 0);
        assert_eq!(committed.canceled_base_units, 1_000_000);
        assert_eq!(committed.caller_cancel_effect_identity, Some(sha("e")));
        assert_eq!(
            session.execution_state().next_gtd_deadline_at_utc_ms(),
            None
        );
        assert_eq!(
            session.account_overlay().quantity(
                &committed.condition_id,
                &committed.token_id,
                contracts::BookSide::Bid,
                committed.source_price_pusd_per_share,
            ),
            rust_decimal::Decimal::ZERO
        );
        assert_eq!(
            session
                .state
                .mutations
                .get("cancel-idempotency")
                .map(|record| record.effect_identity.as_str()),
            Some("cancel-effect")
        );
    }

    #[test]
    fn execution_action_slice_future_action_advances_watermark_and_preserves_the_action() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let registered = state
            .register_request(
                &profile,
                request(&state, "prepared-future-action", 1, 1_000),
                sha("d"),
            )
            .expect("register future action request");
        let cutoff = registered.transit.platform_arrived_at_utc_ms + 50;
        let future_at = cutoff + 50;
        let future_key = LogicalActionKeyV1 {
            effective_processing_at_utc_ms: future_at,
            caller_sequence: 1,
            batch_item_ordinal: 0,
        };
        state
            .enqueue_logical_action(
                "prepared-future-action/item-0".to_owned(),
                "prepared-future-action".to_owned(),
                future_key,
            )
            .expect("queue future action");
        state
            .seal_logical_actions("prepared-future-action")
            .expect("seal future action request");
        state
            .close_platform_prefix_through(cutoff, 1)
            .expect("close prefix before future action");

        let before = SessionStateCheckpointV1::from(&state);
        let mut existing = state.clone();
        let prepared = state
            .prepare_execution_action_slice_v2(&profile, cutoff, false)
            .expect("prepare future action cutoff");
        assert!(prepared.current_action().is_none());
        assert!(prepared.has_state_change());
        assert_eq!(prepared.logical_platform_driven_through_utc_ms(), cutoff);
        drop(prepared);
        assert_eq!(SessionStateCheckpointV1::from(&state), before);
        assert_eq!(
            state
                .pending_actions
                .get(&future_key)
                .map(|action| action.action_identity.as_str()),
            Some("prepared-future-action/item-0")
        );

        assert_eq!(
            existing
                .take_next_action_through(cutoff)
                .expect("old path advances before future action"),
            None
        );
        state
            .prepare_execution_action_slice_v2(&profile, cutoff, false)
            .expect("reprepare future action cutoff")
            .into_committable()
            .expect("future action cutoff is immediately committable")
            .commit();
        assert_eq!(
            SessionStateCheckpointV1::from(&state),
            SessionStateCheckpointV1::from(&existing)
        );
        assert!(state.pending_actions.contains_key(&future_key));
    }

    #[test]
    fn execution_action_slice_single_mode_leaves_the_rest_of_one_request_pending() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let registered = state
            .register_request(
                &profile,
                request(&state, "prepared-multi-action", 1, 1_000),
                sha("d"),
            )
            .expect("register multi-action request");
        let effective = registered.transit.platform_arrived_at_utc_ms;
        for ordinal in 0..2_u16 {
            state
                .enqueue_logical_action(
                    format!("prepared-multi-action/item-{ordinal}"),
                    "prepared-multi-action".to_owned(),
                    LogicalActionKeyV1 {
                        effective_processing_at_utc_ms: effective,
                        caller_sequence: 1,
                        batch_item_ordinal: ordinal,
                    },
                )
                .expect("queue multi-action item");
        }
        state
            .seal_logical_actions("prepared-multi-action")
            .expect("seal multi-action request");
        state
            .close_platform_prefix_through(effective, 1)
            .expect("close multi-action prefix");

        let before = SessionStateCheckpointV1::from(&state);
        let mut existing = state.clone();
        let prepared = state
            .prepare_execution_action_slice_v2(&profile, effective, false)
            .expect("prepare only the first action");
        assert_eq!(
            prepared
                .current_action()
                .map(|action| action.key.batch_item_ordinal),
            Some(0)
        );
        assert!(!prepared.request_completed());
        drop(prepared);
        assert_eq!(SessionStateCheckpointV1::from(&state), before);

        let existing_action = existing
            .take_next_action_through(effective)
            .expect("take old first action")
            .expect("old first action");
        assert!(
            !existing
                .complete_logical_item(&existing_action.action_identity)
                .expect("old first action leaves request pending")
        );
        let mut prepared = state
            .prepare_execution_action_slice_v2(&profile, effective, false)
            .expect("reprepare only the first action");
        let action = prepared.current_action().expect("first prepared action");
        prepared
            .finish_current_action(&action.action_identity)
            .expect("finish first prepared action");
        prepared
            .into_committable()
            .expect("single selected action is committable")
            .commit();
        assert_eq!(
            SessionStateCheckpointV1::from(&state),
            SessionStateCheckpointV1::from(&existing)
        );
        assert_eq!(state.pending_actions.len(), 1);
        assert!(!state.released_request_ids.contains("prepared-multi-action"));
    }

    #[test]
    fn execution_action_slice_prefix_watermark_and_cross_time_failures_are_read_only() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let registered = state
            .register_request(
                &profile,
                request(&state, "prepared-cross-time", 1, 1_000),
                sha("d"),
            )
            .expect("register cross-time request");
        let first_at = registered.transit.platform_arrived_at_utc_ms;
        let second_at = first_at + 1;
        for (ordinal, effective_processing_at_utc_ms) in [(0_u16, first_at), (1, second_at)] {
            state
                .enqueue_logical_action(
                    format!("prepared-cross-time/item-{ordinal}"),
                    "prepared-cross-time".to_owned(),
                    LogicalActionKeyV1 {
                        effective_processing_at_utc_ms,
                        caller_sequence: 1,
                        batch_item_ordinal: ordinal,
                    },
                )
                .expect("queue cross-time action");
        }
        state
            .seal_logical_actions("prepared-cross-time")
            .expect("seal cross-time request");
        state
            .expect_direct_response("prepared-cross-time")
            .expect("track cross-time direct response");

        let before_prefix = SessionStateCheckpointV1::from(&state);
        assert!(matches!(
            state.prepare_execution_action_slice_v2(&profile, first_at, false),
            Err(SessionError::PlatformPrefixNotClosed)
        ));
        assert_eq!(SessionStateCheckpointV1::from(&state), before_prefix);
        assert_eq!(state.in_flight_action_identity, None);

        state
            .close_platform_prefix_through(first_at, 1)
            .expect("close cross-time prefix");
        let before_batch = SessionStateCheckpointV1::from(&state);
        assert!(matches!(
            state.prepare_execution_action_slice_v2(&profile, first_at, true),
            Err(SessionError::InvalidRequest(_))
        ));
        assert_eq!(SessionStateCheckpointV1::from(&state), before_batch);
        assert_eq!(state.in_flight_action_identity, None);

        let single = state
            .prepare_execution_action_slice_v2(&profile, first_at, false)
            .expect("prepare first cross-time item");
        let mut single = single;
        let action = single.current_action().expect("first cross-time item");
        single
            .finish_current_action(&action.action_identity)
            .expect("finish first cross-time item");
        single
            .into_committable()
            .expect("finished cross-time item is committable")
            .commit();
        let before_regression = SessionStateCheckpointV1::from(&state);
        assert!(matches!(
            state.prepare_execution_action_slice_v2(&profile, first_at - 1, false),
            Err(SessionError::ActionWatermarkClosed)
        ));
        assert_eq!(SessionStateCheckpointV1::from(&state), before_regression);
        assert_eq!(state.in_flight_action_identity, None);

        state
            .close_platform_prefix_through(second_at, 2)
            .expect("close through final cross-time action");
        let before_response = SessionStateCheckpointV1::from(&state);
        let mut premature = state
            .prepare_execution_action_slice_v2(&profile, second_at, false)
            .expect("prepare final cross-time action for premature response");
        let final_action = premature.current_action().expect("final cross-time action");
        premature
            .finish_current_action(&final_action.action_identity)
            .expect("finish final cross-time action before response validation");
        assert_eq!(
            premature.prepare_complete_direct_response("prepared-cross-time", second_at - 1, 1),
            Err(SessionError::ResponseBeforeLogicalCompletion)
        );
        drop(premature);
        assert_eq!(SessionStateCheckpointV1::from(&state), before_response);

        let mut completed = state
            .prepare_execution_action_slice_v2(&profile, second_at, false)
            .expect("reprepare final cross-time action");
        let final_action = completed.current_action().expect("final cross-time action");
        completed
            .finish_current_action(&final_action.action_identity)
            .expect("finish final cross-time action");
        completed
            .prepare_complete_direct_response("prepared-cross-time", second_at, 1)
            .expect("response at final action time succeeds");
        drop(completed);
        assert_eq!(SessionStateCheckpointV1::from(&state), before_response);
    }

    #[test]
    fn no_action_closes_watermark_without_crossing_future_action() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let cutoff = 10_000;
        let mut empty = SessionState::new(spec());
        assert_eq!(
            empty.take_next_action_through(cutoff),
            Err(SessionError::PlatformPrefixNotClosed)
        );
        assert_eq!(empty.closed_action_through_utc_ms, None);
        empty
            .close_platform_prefix_through(cutoff, 1)
            .expect("close empty action prefix");
        assert_eq!(
            empty
                .take_next_action_through(cutoff)
                .expect("empty schedule is closed through cutoff"),
            None
        );
        assert_eq!(empty.closed_action_through_utc_ms, Some(cutoff));
        assert_eq!(
            empty.take_next_action_through(cutoff - 1),
            Err(SessionError::ActionWatermarkClosed)
        );
        assert_eq!(empty.closed_action_through_utc_ms, Some(cutoff));

        let mut state = SessionState::new(spec());
        let first_sent_at = 1_000;
        let first_outbound_delay = profile.sample_delay_ms(
            state.spec.session_seed.0.as_bytes(),
            "request-1",
            DelaySampleDomainV1::RequestOutbound,
        );
        let late_outbound_delay = profile.sample_delay_ms(
            state.spec.session_seed.0.as_bytes(),
            "late-request",
            DelaySampleDomainV1::RequestOutbound,
        );
        let first_arrived_at =
            checked_add_delay(first_sent_at, first_outbound_delay).expect("first request arrival");
        let late_outbound_delay =
            i64::try_from(late_outbound_delay).expect("test outbound delay fits i64");
        let cutoff = first_arrived_at + late_outbound_delay + 1;
        let future_processing_delay =
            u64::try_from(cutoff - first_arrived_at + 1).expect("future processing delay");
        let first = state
            .register_and_schedule_request(
                &profile,
                request(&state, "request-1", 1, first_sent_at),
                sha("d"),
                vec![LogicalActionPlanV1 {
                    action_identity: "future-action".to_owned(),
                    batch_item_ordinal: 0,
                    processing_delay_ms: future_processing_delay,
                }],
            )
            .expect("register one future action");
        assert_eq!(
            first.actions[0].key.effective_processing_at_utc_ms,
            cutoff + 1
        );
        state
            .close_platform_prefix_through(cutoff, 1)
            .expect("close before future action");
        assert_eq!(
            state
                .take_next_action_through(cutoff)
                .expect("no action through cutoff"),
            None
        );
        assert_eq!(state.closed_action_through_utc_ms, Some(cutoff));

        let requests_before = state.requests.keys().cloned().collect::<Vec<_>>();
        let action_keys_before = state.action_keys.clone();
        let pending_actions_before = state.pending_actions.clone();
        let sealed_requests_before = state.sealed_request_ids.clone();
        let last_caller_sequence_before = state.last_caller_sequence;
        let last_request_sent_at_before = state.last_request_sent_at_utc_ms;
        let late_sent_at = cutoff - late_outbound_delay;
        assert_eq!(
            state.register_and_schedule_request(
                &profile,
                request(&state, "late-request", 2, late_sent_at),
                sha("e"),
                vec![LogicalActionPlanV1 {
                    action_identity: "late-request-action".to_owned(),
                    batch_item_ordinal: 0,
                    processing_delay_ms: 0,
                }],
            ),
            Err(SessionError::ActionWatermarkClosed)
        );
        assert_eq!(
            state.requests.keys().cloned().collect::<Vec<_>>(),
            requests_before
        );
        assert_eq!(state.action_keys, action_keys_before);
        assert_eq!(state.pending_actions, pending_actions_before);
        assert_eq!(state.sealed_request_ids, sealed_requests_before);
        assert_eq!(state.last_caller_sequence, last_caller_sequence_before);
        assert_eq!(
            state.last_request_sent_at_utc_ms,
            last_request_sent_at_before
        );
        assert_eq!(state.closed_action_through_utc_ms, Some(cutoff));

        assert_eq!(
            state.enqueue_logical_action(
                "late-same-time-action".to_owned(),
                "request-1".to_owned(),
                LogicalActionKeyV1 {
                    effective_processing_at_utc_ms: cutoff,
                    caller_sequence: 1,
                    batch_item_ordinal: 1,
                },
            ),
            Err(SessionError::ActionWatermarkClosed)
        );
        assert_eq!(state.action_keys, action_keys_before);
        assert_eq!(state.pending_actions, pending_actions_before);
        assert_eq!(state.closed_action_through_utc_ms, Some(cutoff));

        let future = state
            .register_and_schedule_request(
                &profile,
                request(&state, "future-request", 2, cutoff + 100),
                sha("f"),
                vec![LogicalActionPlanV1 {
                    action_identity: "future-request-action".to_owned(),
                    batch_item_ordinal: 0,
                    processing_delay_ms: 0,
                }],
            )
            .expect("future request remains registrable");
        assert!(future.actions[0].key.effective_processing_at_utc_ms > cutoff);
        assert_eq!(state.closed_action_through_utc_ms, Some(cutoff));
    }

    #[test]
    fn private_schedule_cannot_cross_an_unprocessed_action() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let scheduled = state
            .register_and_schedule_request(
                &profile,
                request(&state, "pending-private-action", 1, 1_000),
                sha("a"),
                vec![LogicalActionPlanV1 {
                    action_identity: "pending-private-action/item-0".to_owned(),
                    batch_item_ordinal: 0,
                    processing_delay_ms: 0,
                }],
            )
            .expect("register pending action");
        let effective = scheduled.actions[0].key.effective_processing_at_utc_ms;

        assert_eq!(
            state.close_private_fact_schedule_through(effective),
            Err(SessionError::PrivateFactScheduleCrossesPendingAction(
                "pending-private-action".to_owned()
            ))
        );
        assert_eq!(state.private_fact_schedule_closed_through_utc_ms, None);
    }

    #[test]
    fn private_schedule_requires_the_logical_platform_driver_through_the_same_cutoff() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let cutoff = 10_000;
        let mut state = SessionState::new(spec());
        state
            .close_platform_prefix_through(cutoff, 1)
            .expect("close platform input through cutoff");

        assert_eq!(
            state.close_private_fact_schedule_through(cutoff),
            Err(SessionError::PrivateFactScheduleCrossesPendingPlatformWork(
                format!("LOGICAL_PLATFORM:{cutoff}")
            ))
        );
        assert_eq!(state.private_fact_schedule_closed_through_utc_ms, None);

        assert_eq!(
            state
                .take_next_action_through(cutoff)
                .expect("explicit no-action drive closes logical platform work"),
            None
        );
        state
            .close_private_fact_schedule_through(cutoff)
            .expect("private schedule may close only after logical platform work");
        assert_eq!(
            state.private_fact_schedule_closed_through_utc_ms,
            Some(cutoff)
        );

        let late_outbound_delay = profile.sample_delay_ms(
            state.spec.session_seed.0.as_bytes(),
            "late-after-platform-drive",
            DelaySampleDomainV1::RequestOutbound,
        );
        let late_outbound_delay =
            i64::try_from(late_outbound_delay).expect("test outbound delay fits i64");
        assert_eq!(
            state.register_and_schedule_request(
                &profile,
                request(
                    &state,
                    "late-after-platform-drive",
                    1,
                    cutoff - late_outbound_delay,
                ),
                sha("f"),
                vec![LogicalActionPlanV1 {
                    action_identity: "late-after-platform-drive/item-0".to_owned(),
                    batch_item_ordinal: 0,
                    processing_delay_ms: 0,
                }],
            ),
            Err(SessionError::ActionWatermarkClosed)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn private_fact_slice_prepare_is_read_only() {
        let mut session = private_fact_slice_test_session("private-slice-read-only").await;
        let timing = session
            .bind_lifecycle_feedback_timing("private-slice-feedback".to_owned(), 10_000, 1)
            .expect("登记私有事实");
        let cutoff = timing
            .feedback_received_at_utc_ms
            .checked_add(100)
            .expect("测试截止点");
        session.commit_logical_platform_driven_through(cutoff);
        session
            .close_private_fact_schedule_through(timing.feedback_received_at_utc_ms - 1)
            .expect("建立既有计划水位");
        let before = session.checkpoint();

        let prepared = session
            .prepare_private_fact_slice_v2(cutoff)
            .expect("借用式准备私有事实切片");

        assert_eq!(
            prepared.close_through_utc_ms(),
            timing.feedback_received_at_utc_ms
        );
        assert!(prepared.schedule_advanced());
        assert!(matches!(
            prepared.candidate().map(|candidate| &candidate.evidence),
            Some(PrivateVisibilityEvidenceV1::LifecycleFeedback { feedback_id })
                if feedback_id == "private-slice-feedback"
        ));
        assert_eq!(session.checkpoint(), before);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn private_fact_slice_commit_matches_existing_online_sequence() {
        let mut source = private_fact_slice_test_session("private-slice-equivalence").await;
        let timing = source
            .bind_lifecycle_feedback_timing("private-slice-equivalent".to_owned(), 20_000, 1)
            .expect("登记私有事实");
        let cutoff = timing
            .feedback_received_at_utc_ms
            .checked_add(100)
            .expect("测试截止点");
        source.commit_logical_platform_driven_through(cutoff);
        source
            .close_private_fact_schedule_through(timing.feedback_received_at_utc_ms - 1)
            .expect("建立既有计划水位");
        let mut existing_online = source.clone();
        let mut sliced = source;

        existing_online
            .close_private_fact_schedule_through(timing.feedback_received_at_utc_ms)
            .expect("旧在线顺序提交计划水位");
        let existing_candidate = existing_online
            .prepare_next_private_fact_through(timing.feedback_received_at_utc_ms)
            .expect("旧在线顺序准备候选")
            .expect("存在候选");
        existing_online
            .validate_prepared_private_fact(&existing_candidate)
            .expect("旧在线顺序验证候选");
        existing_online.commit_prevalidated_private_fact(&existing_candidate);

        let prepared = sliced
            .prepare_private_fact_slice_v2(cutoff)
            .expect("准备切片");
        let committed = sliced
            .commit_prevalidated_private_fact_slice_v2(prepared)
            .expect("切片提交应消费候选");

        assert_eq!(committed, existing_candidate);
        assert_eq!(sliced.checkpoint(), existing_online.checkpoint());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn private_fact_slice_same_cutoff_has_no_change() {
        let mut session = private_fact_slice_test_session("private-slice-same-cutoff").await;
        let cutoff = 30_000;
        session.commit_logical_platform_driven_through(cutoff);
        session
            .close_private_fact_schedule_through(cutoff)
            .expect("建立计划水位");
        let before = session.checkpoint();

        let prepared = session
            .prepare_private_fact_slice_v2(cutoff)
            .expect("相同截止点准备");
        assert_eq!(prepared.close_through_utc_ms(), cutoff);
        assert!(!prepared.schedule_advanced());
        assert!(prepared.candidate().is_none());
        assert!(
            session
                .commit_prevalidated_private_fact_slice_v2(prepared)
                .is_none()
        );
        assert_eq!(session.checkpoint(), before);
    }

    #[test]
    fn public_and_private_visibility_watermarks_are_independent() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let timing = state
            .bind_lifecycle_feedback_timing(&profile, "visible-feedback".to_owned(), 1_000, 1)
            .expect("feedback timing");
        drive_idle_logical_platform_through(&mut state, timing.feedback_received_at_utc_ms);
        state
            .close_private_fact_schedule_through(timing.feedback_received_at_utc_ms)
            .expect("close feedback plan");
        state
            .take_private_facts_through(timing.feedback_received_at_utc_ms)
            .expect("private sequence");
        let private_only = state.model_visible_cursor();
        assert_eq!(private_only.private_feedback_sequence, 1);
        assert_eq!(
            private_only
                .public_visibility
                .closed_through_received_at_utc_ms(),
            None
        );
        state
            .advance_public_visibility_through(2_000, 40)
            .expect("public prefix");
        let both = state.model_visible_cursor();
        assert_eq!(both.private_feedback_sequence, 1);
        assert_eq!(
            both.public_visibility.closed_through_received_at_utc_ms(),
            Some(2_000)
        );
        let mut future = request(&state, "future-cursor", 1, 2_000);
        future.model_visible_cursor.private_feedback_sequence = 2;
        assert_eq!(
            state.register_request(&profile, future, sha("d")),
            Err(SessionError::FutureModelVisibleCursor)
        );
    }

    #[test]
    fn paper_session_cannot_advance_hidden_replay_and_uses_received_prefix() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut paper_spec = spec();
        paper_spec.caller_role = SimCallerRoleV1::Paper;
        let mut state = SessionState::new(paper_spec);
        let registered = state
            .register_request(
                &profile,
                request(&state, "paper-request", 1, 1_000),
                sha("d"),
            )
            .expect("paper request");
        let effective = registered.transit.platform_arrived_at_utc_ms;
        state
            .enqueue_logical_action(
                "paper-action".to_owned(),
                "paper-request".to_owned(),
                LogicalActionKeyV1 {
                    effective_processing_at_utc_ms: effective,
                    caller_sequence: 1,
                    batch_item_ordinal: 0,
                },
            )
            .expect("paper action");
        state
            .seal_logical_actions("paper-request")
            .expect("seal paper request");
        assert_eq!(
            state.close_platform_prefix_through(effective, 10),
            Err(SessionError::HiddenPlatformReplayForbidden)
        );
        assert_eq!(
            state.take_next_action_through(effective),
            Err(SessionError::PlatformPrefixNotClosed)
        );
        state
            .advance_public_visibility_through(effective, 10)
            .expect("paper received-time prefix");
        assert_eq!(
            state
                .take_next_action_through(effective)
                .expect("paper action")
                .expect("one paper action")
                .action_identity,
            "paper-action"
        );
        assert!(
            state
                .complete_logical_item("paper-action")
                .expect("complete paper action")
        );
    }

    #[test]
    fn lifecycle_feedback_uses_its_own_stable_inbound_sample() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let first = state
            .bind_lifecycle_feedback_timing(&profile, "feedback-1".to_owned(), 4_000, 2)
            .expect("feedback timing");
        let retry = state
            .bind_lifecycle_feedback_timing(&profile, "feedback-1".to_owned(), 4_000, 2)
            .expect("feedback retry");
        let second = state
            .bind_lifecycle_feedback_timing(&profile, "feedback-2".to_owned(), 3_990, 1)
            .expect("second feedback timing");
        assert_eq!(first, retry);
        assert_eq!(
            first.feedback_received_at_utc_ms,
            first.platform_event_at_utc_ms + first.inbound_delay_ms as i64
        );
        let closed_through = first
            .feedback_received_at_utc_ms
            .max(second.feedback_received_at_utc_ms);
        drive_idle_logical_platform_through(&mut state, closed_through);
        state
            .close_private_fact_schedule_through(closed_through)
            .expect("close private schedule");
        let visible = state
            .take_private_facts_through(closed_through)
            .expect("release private facts");
        let mut expected = [
            (
                first.feedback_received_at_utc_ms,
                first.private_emission_sequence,
                "feedback-1",
            ),
            (
                second.feedback_received_at_utc_ms,
                second.private_emission_sequence,
                "feedback-2",
            ),
        ];
        expected.sort();
        assert_eq!(
            visible
                .iter()
                .map(|fact| match &fact.evidence {
                    PrivateVisibilityEvidenceV1::LifecycleFeedback { feedback_id } => {
                        feedback_id.as_str()
                    }
                    PrivateVisibilityEvidenceV1::DirectResponse { .. } => "unexpected-response",
                })
                .collect::<Vec<_>>(),
            expected.map(|(_, _, feedback_id)| feedback_id)
        );
        assert_eq!(
            visible
                .iter()
                .map(|fact| fact.private_feedback_sequence)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(
            state.bind_lifecycle_feedback_timing(&profile, "feedback-1".to_owned(), 4_001, 2,),
            Err(SessionError::PrivateEmissionSequenceConflict(2))
        );
        assert_eq!(
            state.bind_lifecycle_feedback_timing(
                &profile,
                "late-registered".to_owned(),
                i64::MIN + 100,
                3,
            ),
            Err(SessionError::PrivateFactScheduleClosed)
        );
    }

    #[test]
    fn request_causality_and_late_registration_fail_before_state_change() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut wrong_identity = SessionState::new(spec());
        let mut cross_session = request(&wrong_identity, "cross-session", 1, 1_000);
        cross_session.session_id = "session-b".to_owned();
        assert_eq!(
            wrong_identity.register_request(&profile, cross_session, sha("d")),
            Err(SessionError::InvalidRequest("合同或调用方身份不匹配"))
        );
        assert!(wrong_identity.requests.is_empty());
        assert_eq!(wrong_identity.last_caller_sequence, None);

        let mut state = SessionState::new(spec());
        let timing = state
            .bind_lifecycle_feedback_timing(&profile, "feedback-1".to_owned(), 2_000, 1)
            .expect("feedback timing");
        drive_idle_logical_platform_through(&mut state, timing.feedback_received_at_utc_ms);
        state
            .close_private_fact_schedule_through(timing.feedback_received_at_utc_ms)
            .expect("close private facts");
        state
            .take_private_facts_through(timing.feedback_received_at_utc_ms)
            .expect("make feedback visible");
        let before_visible = request(
            &state,
            "before-visible",
            1,
            timing.feedback_received_at_utc_ms - 1,
        );
        assert_eq!(
            state.register_request(&profile, before_visible, sha("d")),
            Err(SessionError::RequestBeforeVisibleFact)
        );
        assert!(state.requests.is_empty());
        assert_eq!(state.last_caller_sequence, None);

        let mut advanced = SessionState::new(spec());
        advanced
            .close_platform_prefix_through(100_000, 1)
            .expect("advance platform with no pending input");
        assert_eq!(
            advanced
                .register_request(&profile, request(&advanced, "late-request", 1, 0), sha("d"),),
            Err(SessionError::RequestBehindPlatformPrefix)
        );
        assert!(advanced.requests.is_empty());
        assert_eq!(advanced.last_caller_sequence, None);

        let mut exact = SessionState::new(spec());
        let exact_context = request(&exact, "request-1", 1, 1_000);
        let exact_arrival = checked_add_delay(
            exact_context.request_sent_at_utc_ms,
            profile.sample_delay_ms(
                exact.spec.session_seed.0.as_bytes(),
                &exact_context.global_request_id,
                DelaySampleDomainV1::RequestOutbound,
            ),
        )
        .expect("exact arrival");
        exact
            .close_platform_prefix_through(exact_arrival, 1)
            .expect("close exact arrival before request registration");
        assert_eq!(
            exact.register_request(&profile, exact_context, sha("d")),
            Err(SessionError::RequestBehindPlatformPrefix)
        );
        assert!(exact.requests.is_empty());

        let mut monotonic = SessionState::new(spec());
        monotonic
            .register_request(
                &profile,
                request(&monotonic, "request-1", 1, 1_000),
                sha("d"),
            )
            .expect("first logical request");
        assert_eq!(
            monotonic.register_request(
                &profile,
                request(&monotonic, "request-2", 2, 999),
                sha("e"),
            ),
            Err(SessionError::NonMonotonicRequestSentAt)
        );
        assert_eq!(monotonic.requests.len(), 1);
        assert_eq!(monotonic.last_caller_sequence, Some(1));
        assert_eq!(monotonic.last_request_sent_at_utc_ms, Some(1_000));
    }

    #[test]
    fn same_time_prefix_waits_for_every_registered_request_schedule() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let first = state
            .register_request(&profile, request(&state, "request-2", 1, 1_000), sha("d"))
            .expect("first request");
        let effective = first.transit.platform_arrived_at_utc_ms;
        let second_sent_at = effective
            - i64::try_from(profile.sample_delay_ms(
                state.spec.session_seed.0.as_bytes(),
                "request-1",
                DelaySampleDomainV1::RequestOutbound,
            ))
            .expect("delay fits i64");
        let second = state
            .register_request(
                &profile,
                request(&state, "request-1", 2, second_sent_at),
                sha("e"),
            )
            .expect("second request");
        assert_eq!(second.transit.platform_arrived_at_utc_ms, effective);

        state
            .enqueue_logical_action(
                "action-1".to_owned(),
                "request-2".to_owned(),
                LogicalActionKeyV1 {
                    effective_processing_at_utc_ms: effective,
                    caller_sequence: 1,
                    batch_item_ordinal: 0,
                },
            )
            .expect("first action");
        state
            .seal_logical_actions("request-2")
            .expect("seal first request");
        assert_eq!(
            state.close_platform_prefix_through(effective, 1),
            Err(SessionError::PlatformPrefixCrossesUnsealedRequest)
        );
        assert_eq!(
            state
                .platform_prefix
                .closed_through_source_event_at_utc_ms(),
            None
        );

        state
            .enqueue_logical_action(
                "action-2".to_owned(),
                "request-1".to_owned(),
                LogicalActionKeyV1 {
                    effective_processing_at_utc_ms: effective,
                    caller_sequence: 2,
                    batch_item_ordinal: 0,
                },
            )
            .expect("second action");
        state
            .seal_logical_actions("request-1")
            .expect("seal second request");
        state
            .close_platform_prefix_through(effective, 1)
            .expect("all same-time request schedules are sealed");
        for expected in ["action-1", "action-2"] {
            let action = state
                .take_next_action_through(effective)
                .expect("release action")
                .expect("pending action");
            assert_eq!(action.action_identity, expected);
            state
                .complete_logical_item(expected)
                .expect("complete action");
        }
    }

    #[test]
    fn prefix_and_batch_barriers_preserve_each_item_processing_point() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let registered = state
            .register_request(&profile, request(&state, "batch", 1, 1_000), sha("d"))
            .expect("batch request");
        let first_at = registered.transit.platform_arrived_at_utc_ms;
        let second_at = first_at + 250;
        state
            .enqueue_logical_action(
                "batch/limit-0".to_owned(),
                "batch".to_owned(),
                LogicalActionKeyV1 {
                    effective_processing_at_utc_ms: first_at,
                    caller_sequence: 1,
                    batch_item_ordinal: 0,
                },
            )
            .expect("first batch item");
        state
            .enqueue_logical_action(
                "batch/market-1".to_owned(),
                "batch".to_owned(),
                LogicalActionKeyV1 {
                    effective_processing_at_utc_ms: second_at,
                    caller_sequence: 1,
                    batch_item_ordinal: 1,
                },
            )
            .expect("second batch item");
        assert_eq!(
            state.close_platform_prefix_through(first_at + 1, 1),
            Err(SessionError::PlatformPrefixCrossesUnsealedRequest)
        );
        assert_eq!(
            state
                .platform_prefix
                .closed_through_source_event_at_utc_ms(),
            None
        );
        assert_eq!(
            state.close_platform_prefix_through(first_at, 1),
            Err(SessionError::PlatformPrefixCrossesUnsealedRequest)
        );
        state
            .seal_logical_actions("batch")
            .expect("seal complete batch schedule");
        state
            .expect_direct_response("batch")
            .expect("track the still-unformed batch response");
        state
            .close_platform_prefix_through(first_at, 1)
            .expect("close through first item");
        let first = state
            .take_next_action_through(first_at)
            .expect("release first batch item")
            .expect("first batch item");
        assert_eq!(first.action_identity, "batch/limit-0");
        state
            .resolve_mutation_idempotency(
                "batch",
                "batch/limit-0",
                "item-0-key".to_owned(),
                sha("e"),
                "effect-0".to_owned(),
            )
            .expect("released item may resolve its effect");
        assert!(
            !state
                .complete_logical_item("batch/limit-0")
                .expect("first item does not complete batch")
        );
        assert_eq!(
            state.complete_direct_response("batch", second_at + 1, 1),
            Err(SessionError::RequestNotLogicallyReleased(
                "batch".to_owned()
            ))
        );
        assert_eq!(
            state.resolve_mutation_idempotency(
                "batch",
                "batch/market-1",
                "item-1-key".to_owned(),
                sha("f"),
                "effect-1".to_owned(),
            ),
            Err(SessionError::ActionNotLogicallyReleased(
                "batch/market-1".to_owned()
            ))
        );
        assert_eq!(
            state.close_platform_prefix_through(second_at + 1, 2),
            Err(SessionError::PlatformPrefixCrossesPendingAction)
        );
        state
            .close_platform_prefix_through(second_at, 2)
            .expect("close through second item");
        let second = state
            .take_next_action_through(second_at)
            .expect("release second batch item")
            .expect("second batch item");
        assert_eq!(second.action_identity, "batch/market-1");
        state
            .resolve_mutation_idempotency(
                "batch",
                "batch/market-1",
                "item-1-key".to_owned(),
                sha("f"),
                "effect-1".to_owned(),
            )
            .expect("second item effect");
        assert!(
            state
                .complete_logical_item("batch/market-1")
                .expect("second item completes batch")
        );
        assert_eq!(
            state.complete_direct_response("batch", second_at - 1, 1),
            Err(SessionError::ResponseBeforeLogicalCompletion)
        );
        state
            .complete_direct_response("batch", second_at + 1, 1)
            .expect("response only after all batch items");
    }

    #[test]
    fn mutation_and_query_identity_resolve_only_at_logical_processing() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        let first = state
            .register_request(&profile, request(&state, "query-1", 1, 1_000), sha("d"))
            .expect("query 1");
        let second = state
            .register_request(&profile, request(&state, "query-2", 2, 1_000), sha("e"))
            .expect("query 2");
        assert_eq!(
            state.resolve_query_revision("query-1", "query-action-1", sha("f"), None),
            Err(SessionError::ActionNotLogicallyReleased(
                "query-action-1".to_owned()
            ))
        );
        let effective = first
            .transit
            .platform_arrived_at_utc_ms
            .max(second.transit.platform_arrived_at_utc_ms);
        state
            .enqueue_logical_action(
                "query-action-1".to_owned(),
                "query-1".to_owned(),
                LogicalActionKeyV1 {
                    effective_processing_at_utc_ms: effective,
                    caller_sequence: 1,
                    batch_item_ordinal: 0,
                },
            )
            .expect("queue query 1");
        state
            .enqueue_logical_action(
                "query-action-2".to_owned(),
                "query-2".to_owned(),
                LogicalActionKeyV1 {
                    effective_processing_at_utc_ms: effective,
                    caller_sequence: 2,
                    batch_item_ordinal: 0,
                },
            )
            .expect("queue query 2");
        state.seal_logical_actions("query-1").expect("seal query 1");
        state.seal_logical_actions("query-2").expect("seal query 2");
        state
            .close_platform_prefix_through(effective, 10)
            .expect("close platform prefix");
        let first_action = state
            .take_next_action_through(effective)
            .expect("release first query")
            .expect("first query action");
        assert_eq!(first_action.action_identity, "query-action-1");
        state
            .advance_platform_state_revision(7)
            .expect("revision 7");
        let first = state
            .resolve_query_revision("query-1", "query-action-1", sha("f"), None)
            .expect("query cutoff");
        let created = state
            .resolve_mutation_idempotency(
                "query-1",
                "query-action-1",
                "mutation-key".to_owned(),
                sha("2"),
                "effect-1".to_owned(),
            )
            .expect("first mutation effect");
        assert!(
            state
                .complete_logical_item("query-action-1")
                .expect("complete first query")
        );
        let second_action = state
            .take_next_action_through(effective)
            .expect("release second query")
            .expect("second query action");
        assert_eq!(second_action.action_identity, "query-action-2");
        state
            .advance_platform_state_revision(8)
            .expect("revision 8");
        let second = state
            .resolve_query_revision("query-2", "query-action-2", sha("1"), None)
            .expect("new query cutoff");
        let existing = state
            .resolve_mutation_idempotency(
                "query-2",
                "query-action-2",
                "mutation-key".to_owned(),
                sha("2"),
                "effect-2".to_owned(),
            )
            .expect("same mutation effect");
        assert!(
            state
                .complete_logical_item("query-action-2")
                .expect("complete second query")
        );
        let retry = state
            .resolve_query_revision("query-1", "query-action-1", sha("f"), None)
            .expect("query retry");
        assert_eq!(first.platform_state_revision, 7);
        assert_eq!(retry.platform_state_revision, 7);
        assert!(retry.reused);
        assert_eq!(second.platform_state_revision, 8);
        assert_eq!(
            state.resolve_query_revision("query-1", "query-action-1", sha("0"), None),
            Err(SessionError::QueryIdentityConflict("query-1".to_owned()))
        );
        assert_eq!(
            created.disposition,
            MutationResolutionDispositionV1::NewEffect
        );
        assert_eq!(existing.effect_identity, "effect-1");
        assert_eq!(
            state.resolve_mutation_idempotency(
                "query-2",
                "query-action-2",
                "mutation-key".to_owned(),
                sha("3"),
                "effect-3".to_owned(),
            ),
            Err(SessionError::MutationIdentityConflict(
                "mutation-key".to_owned()
            ))
        );
    }

    #[test]
    fn proxy_delay_overflow_fails_before_request_is_registered() {
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut state = SessionState::new(spec());
        assert_eq!(
            state.register_request(&profile, request(&state, "overflow", 1, i64::MAX), sha("d"),),
            Err(SessionError::TimeOverflow)
        );
        assert!(state.requests.is_empty());
    }

    #[test]
    fn sampler_version_is_bound_by_session_module() {
        assert_eq!(
            crate::latency_profile::PROXY_LATENCY_SAMPLER_VERSION_V1,
            "sha256_multiply_high_empirical_rank_v1"
        );
    }
}
