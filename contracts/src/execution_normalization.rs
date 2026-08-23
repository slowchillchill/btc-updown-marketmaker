use serde::{Deserialize, Serialize};

use crate::{
    AccountInstanceKeyV1, CancelInputItemRefV2, ConditionId, ExactDecimal, Sha256Hex, TokenId,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
        input_item_refs: Vec<CancelInputItemRefV2>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimV2SourceReferenceV1 {
    pub context_reference: ExecutionNormalizationContextReferenceV1,
    pub watermark: SimV2SourceWatermarkV1,
    pub record: SimV2SourceRecordReferenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StableLogicalIdentityV1(pub String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NoeEconomicFactReferenceV1(pub String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SourceNeutralEconomicIdentityV1 {
    AccountOpening {
        account_identity: SourceNeutralAccountIdentityV1,
        opening_evidence_identity: String,
    },
    AccountTransition {
        account_identity: SourceNeutralAccountIdentityV1,
        account_transition_seq: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralOutcomeBalanceDeltaV1 {
    pub token_id: TokenId,
    pub balance_shares_delta: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralAccountBalanceDeltaV1 {
    pub cash_pusd_delta: ExactDecimal,
    pub available_pusd_delta: ExactDecimal,
    pub principal_encumbrance_pusd_delta: ExactDecimal,
    pub immediate_taker_fee_encumbrance_pusd_delta: ExactDecimal,
    pub outcome_balance_deltas: Vec<SourceNeutralOutcomeBalanceDeltaV1>,
    pub maker_rebate_receivable_pusd_delta: ExactDecimal,
    pub taker_rebate_receivable_pusd_delta: ExactDecimal,
    pub maker_rebate_locked_remainder_pusd_delta: ExactDecimal,
    pub taker_rebate_locked_remainder_pusd_delta: ExactDecimal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralAccountTransitionKindV1 {
    OrderAcceptedEncumbranceApplied {
        order_identity: StableLogicalIdentityV1,
    },
    FakUnspentEncumbranceReleased {
        order_identity: StableLogicalIdentityV1,
    },
    CallerCancellationEncumbranceReleased {
        order_identity: StableLogicalIdentityV1,
    },
    GtdExpirationEncumbranceReleased {
        order_identity: StableLogicalIdentityV1,
        encoded_expires_at_utc_ms: i64,
    },
    VenueDustEncumbranceReleased {
        order_identity: StableLogicalIdentityV1,
    },
    MarketResolutionEncumbranceReleased {
        resolution_identity: StableLogicalIdentityV1,
        order_identity: StableLogicalIdentityV1,
    },
    FillFinalityEconomicApplied {
        trade_identity: StableLogicalIdentityV1,
        path_ordinal: u32,
    },
    FillFinalityUnappliedFailureReleased {
        trade_identity: StableLogicalIdentityV1,
        path_ordinal: u32,
    },
    FillFinalityCompensationApplied {
        trade_identity: StableLogicalIdentityV1,
        path_ordinal: u32,
    },
    MakerRebateAccrualFinalized,
    TakerRebateAccrualApplied {
        trade_identity: StableLogicalIdentityV1,
    },
    MakerRebateDayFinalized {
        attribution_day_utc_ms: i64,
    },
    TakerRebateDayFinalized {
        attribution_day_utc_ms: i64,
    },
    MakerRebatePaid {
        attribution_day_utc_ms: i64,
    },
    TakerRebatePaid {
        attribution_day_utc_ms: i64,
    },
    AutoRedeemApplied {
        resolution_identity: StableLogicalIdentityV1,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralEconomicDispositionV1 {
    AccountOpening {
        available_pusd: ExactDecimal,
    },
    AccountTransition {
        economic_effective_at_utc_ms: i64,
        condition_id: Option<ConditionId>,
        transition: SourceNeutralAccountTransitionKindV1,
        balance_delta: Box<SourceNeutralAccountBalanceDeltaV1>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralEconomicFactV1 {
    pub fact_reference: NoeEconomicFactReferenceV1,
    pub logical_identity: StableLogicalIdentityV1,
    pub economic_identity: SourceNeutralEconomicIdentityV1,
    pub source_reference: SimV2SourceReferenceV1,
    pub disposition: SourceNeutralEconomicDispositionV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerEconomicFactReferenceV1 {
    pub context_reference: ExecutionNormalizationContextReferenceV1,
    pub economic_identity: SourceNeutralEconomicIdentityV1,
    pub fact_reference: NoeEconomicFactReferenceV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoeSourceCommitBoundaryV1 {
    pub context_reference: ExecutionNormalizationContextReferenceV1,
    pub source_watermark: SimV2SourceWatermarkV1,
    pub durable_available_at_utc_ms: i64,
}
