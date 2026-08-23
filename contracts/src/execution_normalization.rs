use serde::{Deserialize, Serialize};

use crate::{
    AccountInstanceKeyV1, ConditionId, ExactDecimal, PlatformAccountBalanceDeltaV1,
    PlatformAccountDeltaReasonV2, Sha256Hex,
};

pub const EXECUTION_NORMALIZATION_CONTRACT_V1: &str = "execution_normalization_v1";

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExecutionNormalizationContextReferenceV1(pub String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SourceNeutralAccountIdentityV1 {
    pub source_category: String,
    pub environment: String,
    pub run_scope: String,
    pub account_scope: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ImmutableSourceReferenceV1 {
    Identity(String),
    Sha256(Sha256Hex),
    Location(String),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DurableSourceReferenceV1 {
    pub source: ImmutableSourceReferenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimV2ContextSourceV1 {
    pub account_instance_key: AccountInstanceKeyV1,
    pub run_opened: DurableSourceReferenceV1,
    pub account_opening_evidence_identity: String,
    pub account_opening: DurableSourceReferenceV1,
    pub available_pusd: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionNormalizationContextV1 {
    pub context_reference: ExecutionNormalizationContextReferenceV1,
    pub account_identity: SourceNeutralAccountIdentityV1,
    pub source: SimV2ContextSourceV1,
    pub contract_version: String,
    pub implementation_identity: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SimV2SourceWatermarkV1 {
    pub run_id: String,
    pub projection_batch_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SimV2DirectResponseAssociationV1 {
    Request {
        global_request_id: String,
    },
    SubmissionItem {
        global_request_id: String,
        batch_item_ordinal: u16,
    },
    CancellationTarget {
        global_request_id: String,
        target_ordinal: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SimV2SourceRecordReferenceV1 {
    AccountOpening,
    MarketEnrollment {
        enrollment_sequence: u64,
    },
    DirectResponse {
        private_feedback_sequence: u64,
        platform_responded_at_utc_ms: i64,
        response_received_at_utc_ms: i64,
        association: SimV2DirectResponseAssociationV1,
    },
    OrderLifecycle {
        private_feedback_sequence: u64,
        platform_event_at_utc_ms: i64,
        feedback_received_at_utc_ms: i64,
    },
    TradeLifecycle {
        private_feedback_sequence: u64,
        platform_event_at_utc_ms: i64,
        feedback_received_at_utc_ms: i64,
    },
    SettlementLifecycle {
        private_feedback_sequence: u64,
        platform_event_at_utc_ms: i64,
        feedback_received_at_utc_ms: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SimV2SourceReferenceV1 {
    pub context_reference: ExecutionNormalizationContextReferenceV1,
    pub watermark: SimV2SourceWatermarkV1,
    pub record: SimV2SourceRecordReferenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StableLogicalIdentityV1(pub String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SimV2EconomicIdentityV1 {
    pub account_identity: SourceNeutralAccountIdentityV1,
    pub account_transition_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NoeEconomicFactReferenceV1(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralEconomicFactV1 {
    pub fact_reference: NoeEconomicFactReferenceV1,
    pub logical_identity: StableLogicalIdentityV1,
    pub economic_identity: SimV2EconomicIdentityV1,
    pub source_reference: SimV2SourceReferenceV1,
    pub platform_effective_at_utc_ms: i64,
    pub reason: PlatformAccountDeltaReasonV2,
    pub condition_id: Option<ConditionId>,
    pub balance_delta: PlatformAccountBalanceDeltaV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerEconomicFactReferenceV1 {
    pub context_reference: ExecutionNormalizationContextReferenceV1,
    pub economic_identity: SimV2EconomicIdentityV1,
    pub fact_reference: NoeEconomicFactReferenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoeSourceCommitBoundaryV1 {
    pub context_reference: ExecutionNormalizationContextReferenceV1,
    pub source_watermark: SimV2SourceWatermarkV1,
    pub durable_available_at_utc_ms: i64,
}
