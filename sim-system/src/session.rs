use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

pub use contracts::SimCallerRoleV1;
use contracts::{
    ACCOUNT_OPENING_ANCHOR_SCHEMA_V1, AccountInstanceKeyV1, AccountOpeningAnchorV1,
    EXECUTION_MARKET_EVENT_SCHEMA_V3, ExactDecimal, MARKET_ENROLLMENT_SCHEMA_V1,
    MarketEnrollmentIdentityV1, MarketEnrollmentV1, PlatformAccountSnapshotV1,
    PlatformOutcomeBalanceV1, PlatformSourceV1, Sha256Hex, TokenId,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::account::InitialAccountStateV1;
use crate::finality::TradeFinalityPolicyV1;
use crate::latency_profile::{DelaySampleDomainV1, EmpiricalDelayProfile};
use crate::market::{AccountBookOverlayStoreV1, MarketStateStoreV1};
use crate::market_directory::{SessionMarketDirectoryErrorV1, SessionMarketDirectoryV1};
use crate::matching::{ExecutionStateV1, MatchingStateErrorV1};
use crate::platform_rules::SealedProcessRuleSnapshot;
use crate::scenario::{PlatformMarketV1, PlatformScenarioV1};

pub const SIM_EXECUTION_CONTRACT_V1: &str = "sim_execution_contract_v1";
pub const SESSION_SPEC_SCHEMA_V2: &str = "session_spec_v2";

const SESSION_SPEC_HASH_DOMAIN_V2: &str = "SIM_SESSION_SPEC_SHA256_V2";
const ACCOUNT_OPENING_ANCHOR_ID_DOMAIN_V1: &str = "SIM_ACCOUNT_OPENING_ANCHOR_ID_V1";
const ACCOUNT_OPENING_ANCHOR_HASH_DOMAIN_V1: &str = "SIM_ACCOUNT_OPENING_ANCHOR_SHA256_V1";
const MARKET_ENROLLMENT_ID_DOMAIN_V1: &str = "SIM_MARKET_ENROLLMENT_ID_V1";
const MARKET_ENROLLMENT_HASH_DOMAIN_V1: &str = "SIM_MARKET_ENROLLMENT_SHA256_V1";
const MARKET_ENROLLMENT_CHAIN_HASH_DOMAIN_V1: &str = "SIM_MARKET_ENROLLMENT_CHAIN_SHA256_V1";

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketEnrollmentRequestV1 {
    pub enrollment_request_id: String,
    pub market: PlatformMarketV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionInitializationStateV1 {
    PreparedLedgerInit,
    Active,
    InitializationFailedClosed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivateSessionRequestV1 {
    pub session_id: String,
    pub account_instance_key: AccountInstanceKeyV1,
    pub opening_anchor_id: String,
    pub opening_anchor_sha256: Sha256Hex,
    pub activation_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone)]
struct CapitalIssuanceFenceV1 {
    opening_anchor: AccountOpeningAnchorV1,
    creation_request_id: String,
    spec_sha256: Sha256Hex,
}

#[derive(Clone)]
struct MarketEnrollmentStateV1 {
    directory: SessionMarketDirectoryV1,
    chain: Vec<MarketEnrollmentV1>,
    records_by_request_id: BTreeMap<String, usize>,
    business_input_started: bool,
    paper_candidate_condition_ids: BTreeSet<String>,
    paper_candidate_token_ids: BTreeSet<String>,
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
    pub fn try_new(
        run_id: String,
        finality_policy: Arc<TradeFinalityPolicyV1>,
        rule_snapshot: Arc<SealedProcessRuleSnapshot>,
        latency_profile: Arc<EmpiricalDelayProfile>,
        scenario: Arc<PlatformScenarioV1>,
    ) -> Result<Self, SimProcessRootErrorV1> {
        if run_id.is_empty() || run_id.trim() != run_id {
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

    pub fn create_session(
        &mut self,
        spec: SessionSpecV2,
    ) -> Result<PreparedSimSessionV2, SessionError> {
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
            account_instance_key,
            CapitalIssuanceFenceV1 {
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
        if fence.opening_anchor != opening_anchor
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
        if fence.opening_anchor != record.opening_anchor
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

    pub fn registered_session_count(&self) -> usize {
        self.session_creations.len()
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

    #[cfg(test)]
    pub(crate) fn shares_market_enrollment_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.market_enrollment, &other.market_enrollment)
    }

    #[cfg(test)]
    pub(crate) fn market_enrollment_storage_identity(&self) -> usize {
        Arc::as_ptr(&self.market_enrollment) as usize
    }

    pub(crate) fn enroll_market(
        &mut self,
        request: MarketEnrollmentRequestV1,
        account_instance_key: &AccountInstanceKeyV1,
        opening_anchor: &AccountOpeningAnchorV1,
    ) -> Result<MarketEnrollmentV1, SessionError> {
        self.require_active()?;
        if !is_stable_text(&request.enrollment_request_id) {
            return Err(SessionError::InvalidMarketEnrollment(
                "enrollment_request_id 为空或含首尾空白",
            ));
        }
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
        match self.session_spec().caller_role {
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => {
                if !self.market_enrollment.business_input_started {
                    Arc::make_mut(&mut self.market_enrollment).business_input_started = true;
                }
            }
            SimCallerRoleV1::Paper => {
                let new_condition_ids = condition_ids
                    .into_iter()
                    .filter(|condition_id| {
                        !self.market_enrollment.directory.contains(condition_id)
                            && !self
                                .market_enrollment
                                .paper_candidate_condition_ids
                                .contains(condition_id)
                    })
                    .collect::<BTreeSet<_>>();
                let new_token_ids = token_ids
                    .into_iter()
                    .filter(|token_id| {
                        !self.market_enrollment.directory.contains_token(token_id)
                            && !self
                                .market_enrollment
                                .paper_candidate_token_ids
                                .contains(token_id)
                    })
                    .collect::<BTreeSet<_>>();
                if !new_condition_ids.is_empty() || !new_token_ids.is_empty() {
                    let market_enrollment = Arc::make_mut(&mut self.market_enrollment);
                    market_enrollment
                        .paper_candidate_condition_ids
                        .extend(new_condition_ids);
                    market_enrollment
                        .paper_candidate_token_ids
                        .extend(new_token_ids);
                }
            }
        }
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

    #[cfg(test)]
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
        let actions = actions
            .into_iter()
            .map(|action| LogicalActionPlanV1 {
                action_identity: action.action_identity,
                batch_item_ordinal: action.batch_item_ordinal,
                processing_delay_ms: self.processing_delay_ms(action.method_timing),
            })
            .collect();
        self.state.register_and_schedule_request(
            &self.latency_profile,
            context,
            canonical_request_sha256,
            actions,
        )
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
            Self::InvalidRunId => write!(formatter, "运行根 run_id 不能为空或带首尾空白"),
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelVisibleCursorV1 {
    pub session_id: String,
    pub public_visibility: PublicVisibilityCursorV1,
    pub private_feedback_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleFeedbackTimingV1 {
    pub feedback_id: String,
    pub platform_event_at_utc_ms: i64,
    pub private_emission_sequence: u64,
    pub inbound_delay_ms: u64,
    pub feedback_received_at_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct PrivateFactOrderKey {
    received_at_utc_ms: i64,
    private_emission_sequence: u64,
    identity: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueuedLogicalActionV1 {
    pub action_identity: String,
    pub global_request_id: String,
    pub key: LogicalActionKeyV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RegisteredLogicalScheduleV1 {
    pub request: RegisteredRequestV1,
    pub actions: Vec<QueuedLogicalActionV1>,
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

#[derive(Clone)]
struct RequestRecord {
    context: RequestContextV1,
    canonical_request_sha256: Sha256Hex,
    transit: RequestTransitV1,
    response_transit: Option<RequestTransitV1>,
}

#[derive(Clone)]
struct MutationRecord {
    payload_sha256: Sha256Hex,
    effect_identity: String,
}

#[derive(Clone)]
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

impl SessionState {
    fn new(spec: SessionSpecV2) -> Self {
        Self {
            spec,
            platform_prefix: PlatformPrefixCursorV1::default(),
            public_visibility: PublicVisibilityCursorV1::default(),
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
        self.private_fact_schedule_closed_through_utc_ms = Some(received_at_utc_ms);
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
        if self
            .private_fact_schedule_closed_through_utc_ms
            .is_none_or(|closed| closed < received_at_utc_ms)
        {
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
        let expected = self
            .prepare_next_private_fact_through(candidate.received_at_utc_ms)?
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
        let global_request_id = context.global_request_id.clone();
        let caller_sequence = context.caller_sequence;
        let prepared_request =
            self.prepare_request_registration(profile, context, canonical_request_sha256)?;
        let registered_request = prepared_request.registered_request();
        let prepared_actions = self.prepare_logical_schedule(
            &global_request_id,
            caller_sequence,
            &registered_request,
            &actions,
        )?;

        let is_new = matches!(prepared_request, PreparedRequestRegistrationV1::New(_));
        let committed_request = self.commit_prepared_request(prepared_request);
        if is_new {
            for action in &prepared_actions {
                self.action_keys.insert(
                    action.action_identity.clone(),
                    (action.key, action.global_request_id.clone()),
                );
                self.pending_actions.insert(action.key, action.clone());
            }
            self.sealed_request_ids.insert(global_request_id);
        }

        Ok(RegisteredLogicalScheduleV1 {
            request: committed_request,
            actions: prepared_actions,
        })
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
        let request_completed = !self
            .pending_actions
            .values()
            .any(|action| action.global_request_id == global_request_id);
        if request_completed {
            self.released_request_ids.insert(global_request_id);
        }
        Ok(request_completed)
    }

    fn take_next_action_through(
        &mut self,
        effective_processing_at_utc_ms: i64,
    ) -> Result<Option<QueuedLogicalActionV1>, SessionError> {
        if let Some(identity) = &self.in_flight_action_identity {
            return Err(SessionError::ActionProcessingInProgress(identity.clone()));
        }
        let applicable_prefix_closed = match self.spec.caller_role {
            SimCallerRoleV1::ModelTrain | SimCallerRoleV1::OfflineEvaluation => self
                .platform_prefix
                .closed_through_source_event_at_utc_ms
                .is_some_and(|closed| closed >= effective_processing_at_utc_ms),
            SimCallerRoleV1::Paper => self
                .public_visibility
                .closed_through_received_at_utc_ms
                .is_some_and(|closed| closed >= effective_processing_at_utc_ms),
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

fn initialization_snapshot(
    initialization: &SharedSessionInitializationV1,
) -> Result<SessionInitializationSnapshotV1, SessionError> {
    initialization
        .lock()
        .map(|snapshot| snapshot.clone())
        .map_err(|_| SessionError::InitializationStateUnavailable)
}

fn validate_activation_request(request: &ActivateSessionRequestV1) -> Result<(), SessionError> {
    validate_initialization_identity(
        &request.session_id,
        &request.account_instance_key,
        &request.opening_anchor_id,
        &request.opening_anchor_sha256,
    )?;
    if !is_stable_text(&request.activation_id) {
        return Err(SessionError::InvalidInitializationControl(
            "activation_id 为空或含首尾空白",
        ));
    }
    Ok(())
}

fn validate_failure_request(
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
            "failure_id 为空或含首尾空白",
        ));
    }
    Ok(())
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
            "开户身份字段为空或含首尾空白",
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

fn validate_session_spec(spec: &SessionSpecV2) -> Result<(), SessionError> {
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
    !value.is_empty() && value.trim() == value
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
        || context.global_request_id.is_empty()
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
    use crate::latency_profile::load_embedded_proxy_latency_profile;

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
