use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use contracts::execution_normalization::{
    ExecutionNormalizationContextReferenceV1, ExecutionNormalizationContextV1,
    NoeEconomicFactReferenceV1, SimV2DirectResponseAssociationV1, SimV2SourceRecordReferenceV1,
    SimV2SourceReferenceV1, SimV2SourceWatermarkV1, SourceNeutralAccountBalanceDeltaV1,
    SourceNeutralAccountTransitionKindV1, SourceNeutralEconomicDispositionV1,
    SourceNeutralEconomicFactV1, SourceNeutralEconomicIdentityV1, StableLogicalIdentityV1,
};
use contracts::{CancelInputItemRefV2, ConditionId, ExactDecimal};
use serde::{Deserialize, Serialize};

pub const NORMALIZATION_CORE_V1: &str = "normalization_core_v1";

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SourceSequenceV1 {
    AccountOpening,
    MarketEnrollment { enrollment_sequence: u64 },
    PrivateFeedback { private_feedback_sequence: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SourceOrderV1 {
    pub sequence: SourceSequenceV1,
    pub ordinal: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SourceNeutralLogicalKeyV1 {
    AccountOpening {
        opening_evidence_identity: String,
    },
    SubmissionItem {
        global_request_id: String,
        batch_item_ordinal: u16,
    },
    CancellationTarget {
        global_request_id: String,
        target_ordinal: u64,
    },
    OrderLifecycle {
        order_identity: String,
        occurrence: u64,
    },
    TradeLifecycle {
        trade_identity: String,
        occurrence: u64,
    },
    SettlementLifecycle {
        settlement_identity: String,
        occurrence: u64,
    },
    EconomicEvidence {
        evidence_identity: String,
    },
    AuditEvidence {
        evidence_identity: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SourceNeutralEconomicKeyV1 {
    AccountOpening { opening_evidence_identity: String },
    AccountTransition { account_transition_seq: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralEconomicEvidenceV1 {
    AccountOpening {
        opening_evidence_identity: String,
        available_pusd: ExactDecimal,
    },
    AccountTransition {
        account_transition_seq: u64,
        economic_effective_at_utc_ms: i64,
        condition_id: Option<ConditionId>,
        transition: SourceNeutralAccountTransitionKindV1,
        balance_delta: Box<SourceNeutralAccountBalanceDeltaV1>,
    },
}

impl SourceNeutralEconomicEvidenceV1 {
    fn key(&self) -> SourceNeutralEconomicKeyV1 {
        match self {
            Self::AccountOpening {
                opening_evidence_identity,
                ..
            } => SourceNeutralEconomicKeyV1::AccountOpening {
                opening_evidence_identity: opening_evidence_identity.clone(),
            },
            Self::AccountTransition {
                account_transition_seq,
                ..
            } => SourceNeutralEconomicKeyV1::AccountTransition {
                account_transition_seq: *account_transition_seq,
            },
        }
    }

    fn disposition(&self) -> SourceNeutralEconomicDispositionV1 {
        match self {
            Self::AccountOpening { available_pusd, .. } => {
                SourceNeutralEconomicDispositionV1::AccountOpening {
                    available_pusd: available_pusd.clone(),
                }
            }
            Self::AccountTransition {
                economic_effective_at_utc_ms,
                condition_id,
                transition,
                balance_delta,
                ..
            } => SourceNeutralEconomicDispositionV1::AccountTransition {
                economic_effective_at_utc_ms: *economic_effective_at_utc_ms,
                condition_id: condition_id.clone(),
                transition: transition.clone(),
                balance_delta: balance_delta.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmissionDispositionV1 {
    Accepted,
    RejectedNoEffect,
    EvidenceUnknownNoEffect,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderLifecycleDispositionV1 {
    Open,
    PartiallyFilled {
        cumulative_filled_shares: ExactDecimal,
    },
    Filled,
    Cancelled,
    Expired,
    VenueDustClosed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeLifecycleDispositionV1 {
    Observed,
    Finalized,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettlementLifecycleDispositionV1 {
    Resolved,
    Redeemed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PendingScopeV1 {
    EvidenceGap {
        scope_identity: String,
    },
    EconomicKey {
        economic_key: SourceNeutralEconomicKeyV1,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NormalizationPendingScopeV1 {
    EvidenceGap {
        scope_identity: String,
    },
    EconomicIdentity {
        economic_identity: SourceNeutralEconomicIdentityV1,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceNeutralEvidenceKindV1 {
    Submission {
        disposition: SubmissionDispositionV1,
    },
    OrderLifecycle {
        disposition: OrderLifecycleDispositionV1,
    },
    TradeLifecycle {
        disposition: TradeLifecycleDispositionV1,
    },
    SettlementLifecycle {
        disposition: SettlementLifecycleDispositionV1,
    },
    Economic {
        evidence: SourceNeutralEconomicEvidenceV1,
    },
    LateEconomicCoverage {
        economic_key: SourceNeutralEconomicKeyV1,
    },
    Pending {
        scope: PendingScopeV1,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralEvidenceV1 {
    pub source_reference: SimV2SourceReferenceV1,
    pub source_ordinal: u32,
    pub logical_key: SourceNeutralLogicalKeyV1,
    pub kind: SourceNeutralEvidenceKindV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizationBatchInputV1 {
    pub source_watermark: SimV2SourceWatermarkV1,
    pub evidence: Vec<SourceNeutralEvidenceV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NormalizationConflictScopeV1 {
    LogicalIdentity {
        logical_identity: StableLogicalIdentityV1,
    },
    EconomicIdentity {
        economic_identity: SourceNeutralEconomicIdentityV1,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EconomicAliasDecisionV1 {
    EconomicEvidence,
    LateEconomicCoverage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NormalizationRecordV1 {
    Evidence {
        logical_identity: StableLogicalIdentityV1,
        source_order: SourceOrderV1,
        source_reference: SimV2SourceReferenceV1,
        evidence: SourceNeutralEvidenceKindV1,
    },
    EconomicFact {
        source_order: SourceOrderV1,
        fact: SourceNeutralEconomicFactV1,
    },
    Duplicate {
        logical_identity: StableLogicalIdentityV1,
        source_order: SourceOrderV1,
        source_reference: SimV2SourceReferenceV1,
        original_source_reference: SimV2SourceReferenceV1,
    },
    EconomicAlias {
        logical_identity: StableLogicalIdentityV1,
        source_order: SourceOrderV1,
        source_reference: SimV2SourceReferenceV1,
        decision: EconomicAliasDecisionV1,
        economic_identity: SourceNeutralEconomicIdentityV1,
        primary_logical_identity: StableLogicalIdentityV1,
        fact_reference: NoeEconomicFactReferenceV1,
    },
    Pending {
        logical_identity: StableLogicalIdentityV1,
        source_order: SourceOrderV1,
        source_reference: SimV2SourceReferenceV1,
        evidence: SourceNeutralEvidenceKindV1,
        scope: NormalizationPendingScopeV1,
    },
    Conflict {
        logical_identity: StableLogicalIdentityV1,
        source_order: SourceOrderV1,
        source_reference: SimV2SourceReferenceV1,
        scope: NormalizationConflictScopeV1,
        conflicting_evidence: SourceNeutralEvidenceKindV1,
    },
}

impl NormalizationRecordV1 {
    pub fn logical_identity(&self) -> &StableLogicalIdentityV1 {
        match self {
            Self::Evidence {
                logical_identity, ..
            }
            | Self::Duplicate {
                logical_identity, ..
            }
            | Self::EconomicAlias {
                logical_identity, ..
            }
            | Self::Pending {
                logical_identity, ..
            }
            | Self::Conflict {
                logical_identity, ..
            } => logical_identity,
            Self::EconomicFact { fact, .. } => &fact.logical_identity,
        }
    }

    pub fn source_order(&self) -> &SourceOrderV1 {
        match self {
            Self::Evidence { source_order, .. }
            | Self::EconomicFact { source_order, .. }
            | Self::Duplicate { source_order, .. }
            | Self::EconomicAlias { source_order, .. }
            | Self::Pending { source_order, .. }
            | Self::Conflict { source_order, .. } => source_order,
        }
    }

    pub fn source_reference(&self) -> &SimV2SourceReferenceV1 {
        match self {
            Self::Evidence {
                source_reference, ..
            }
            | Self::Duplicate {
                source_reference, ..
            }
            | Self::EconomicAlias {
                source_reference, ..
            }
            | Self::Pending {
                source_reference, ..
            }
            | Self::Conflict {
                source_reference, ..
            } => source_reference,
            Self::EconomicFact { fact, .. } => &fact.source_reference,
        }
    }

    pub fn economic_fact(&self) -> Option<&SourceNeutralEconomicFactV1> {
        match self {
            Self::EconomicFact { fact, .. } => Some(fact),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizationBatchV1 {
    pub source_watermark: SimV2SourceWatermarkV1,
    pub records: Vec<NormalizationRecordV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CoreErrorV1 {
    ContextReferenceMismatch {
        expected: ExecutionNormalizationContextReferenceV1,
        found: ExecutionNormalizationContextReferenceV1,
    },
    SourceWatermarkMismatch {
        expected: SimV2SourceWatermarkV1,
        found: SimV2SourceWatermarkV1,
    },
    IdentityEncoding {
        message: String,
    },
}

impl fmt::Display for CoreErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ContextReferenceMismatch { expected, found } => write!(
                formatter,
                "上下文引用不匹配：期望 {}，实际 {}",
                expected.0, found.0
            ),
            Self::SourceWatermarkMismatch { expected, found } => write!(
                formatter,
                "来源水位不匹配：期望 {}:{}，实际 {}:{}",
                expected.run_id,
                expected.projection_batch_sequence,
                found.run_id,
                found.projection_batch_sequence
            ),
            Self::IdentityEncoding { message } => write!(formatter, "身份编码失败：{message}"),
        }
    }
}

impl Error for CoreErrorV1 {}

#[derive(Debug, Default)]
pub(crate) struct CoreStateV1 {
    logical: BTreeMap<StableLogicalIdentityV1, StoredLogicalV1>,
    economic: BTreeMap<SourceNeutralEconomicIdentityV1, SourceNeutralEconomicFactV1>,
}

#[derive(Debug, Default)]
pub(crate) struct CoreStateDeltaV1 {
    logical: BTreeMap<StableLogicalIdentityV1, StoredLogicalV1>,
    economic: BTreeMap<SourceNeutralEconomicIdentityV1, SourceNeutralEconomicFactV1>,
}

#[derive(Clone, Debug)]
struct StoredLogicalV1 {
    evidence: StoredEvidenceV1,
    cancellation_input_item_refs: Option<Vec<CancelInputItemRefV2>>,
    source_reference: SimV2SourceReferenceV1,
}

#[derive(Clone, Debug)]
enum StoredEvidenceV1 {
    Evidence(SourceNeutralEvidenceKindV1),
    EconomicAlias {
        economic_identity: SourceNeutralEconomicIdentityV1,
    },
    Economic {
        economic_identity: SourceNeutralEconomicIdentityV1,
        disposition: SourceNeutralEconomicDispositionV1,
    },
}

#[derive(Clone)]
struct OrderedEvidenceV1 {
    source_reference: SimV2SourceReferenceV1,
    source_order: SourceOrderV1,
    kind: SourceNeutralEvidenceKindV1,
}

pub struct PreparedNormalizationBatchV1 {
    batch: NormalizationBatchV1,
    state_delta: CoreStateDeltaV1,
}

impl PreparedNormalizationBatchV1 {
    pub fn batch(&self) -> &NormalizationBatchV1 {
        &self.batch
    }

    pub(crate) fn into_parts(self) -> (NormalizationBatchV1, CoreStateDeltaV1) {
        (self.batch, self.state_delta)
    }
}

#[derive(Debug)]
pub struct NormalizationCoreV1 {
    context: ExecutionNormalizationContextV1,
    state: CoreStateV1,
}

impl NormalizationCoreV1 {
    pub fn new(context: ExecutionNormalizationContextV1) -> Self {
        Self {
            context,
            state: CoreStateV1::default(),
        }
    }

    pub(crate) fn restore<'a>(
        context: ExecutionNormalizationContextV1,
        records: impl IntoIterator<Item = &'a NormalizationRecordV1>,
    ) -> Self {
        let mut core = Self::new(context);
        for record in records {
            match record {
                NormalizationRecordV1::Evidence {
                    logical_identity,
                    source_reference,
                    evidence,
                    ..
                }
                | NormalizationRecordV1::Pending {
                    logical_identity,
                    source_reference,
                    evidence,
                    ..
                } => {
                    core.state.logical.insert(
                        logical_identity.clone(),
                        StoredLogicalV1 {
                            evidence: StoredEvidenceV1::Evidence(evidence.clone()),
                            cancellation_input_item_refs: Self::cancellation_input_item_refs(
                                source_reference,
                            )
                            .map(<[CancelInputItemRefV2]>::to_vec),
                            source_reference: source_reference.clone(),
                        },
                    );
                }
                NormalizationRecordV1::EconomicFact { fact, .. } => {
                    core.state.logical.insert(
                        fact.logical_identity.clone(),
                        StoredLogicalV1 {
                            evidence: StoredEvidenceV1::Economic {
                                economic_identity: fact.economic_identity.clone(),
                                disposition: fact.disposition.clone(),
                            },
                            cancellation_input_item_refs: Self::cancellation_input_item_refs(
                                &fact.source_reference,
                            )
                            .map(<[CancelInputItemRefV2]>::to_vec),
                            source_reference: fact.source_reference.clone(),
                        },
                    );
                    core.state
                        .economic
                        .insert(fact.economic_identity.clone(), fact.clone());
                }
                NormalizationRecordV1::EconomicAlias {
                    logical_identity,
                    source_reference,
                    decision,
                    economic_identity,
                    ..
                } => {
                    let evidence = match decision {
                        EconomicAliasDecisionV1::EconomicEvidence => {
                            StoredEvidenceV1::EconomicAlias {
                                economic_identity: economic_identity.clone(),
                            }
                        }
                        EconomicAliasDecisionV1::LateEconomicCoverage => {
                            StoredEvidenceV1::Evidence(
                                SourceNeutralEvidenceKindV1::LateEconomicCoverage {
                                    economic_key: Self::economic_key(economic_identity),
                                },
                            )
                        }
                    };
                    core.state.logical.insert(
                        logical_identity.clone(),
                        StoredLogicalV1 {
                            evidence,
                            cancellation_input_item_refs: Self::cancellation_input_item_refs(
                                source_reference,
                            )
                            .map(<[CancelInputItemRefV2]>::to_vec),
                            source_reference: source_reference.clone(),
                        },
                    );
                }
                NormalizationRecordV1::Conflict {
                    logical_identity,
                    source_reference,
                    scope: NormalizationConflictScopeV1::EconomicIdentity { .. },
                    conflicting_evidence,
                    ..
                } => {
                    let evidence = match conflicting_evidence {
                        SourceNeutralEvidenceKindV1::Economic { evidence } => {
                            StoredEvidenceV1::Economic {
                                economic_identity: core.economic_identity(&evidence.key()),
                                disposition: evidence.disposition(),
                            }
                        }
                        evidence => StoredEvidenceV1::Evidence(evidence.clone()),
                    };
                    core.state.logical.insert(
                        logical_identity.clone(),
                        StoredLogicalV1 {
                            evidence,
                            cancellation_input_item_refs: Self::cancellation_input_item_refs(
                                source_reference,
                            )
                            .map(<[CancelInputItemRefV2]>::to_vec),
                            source_reference: source_reference.clone(),
                        },
                    );
                }
                NormalizationRecordV1::Duplicate { .. }
                | NormalizationRecordV1::Conflict {
                    scope: NormalizationConflictScopeV1::LogicalIdentity { .. },
                    ..
                } => {}
            }
        }
        core
    }

    pub fn context(&self) -> &ExecutionNormalizationContextV1 {
        &self.context
    }

    pub fn prepare_batch(
        &self,
        input: NormalizationBatchInputV1,
    ) -> Result<PreparedNormalizationBatchV1, CoreErrorV1> {
        let mut ordered = Vec::with_capacity(input.evidence.len());
        for evidence in input.evidence {
            self.check_evidence_boundary(&input.source_watermark, &evidence)?;
            let logical_identity = self.logical_identity(&evidence.logical_key)?;
            let source_order = Self::source_order(&evidence);
            ordered.push((
                source_order.clone(),
                logical_identity,
                OrderedEvidenceV1 {
                    source_reference: evidence.source_reference,
                    source_order,
                    kind: evidence.kind,
                },
            ));
        }
        ordered.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

        let mut state_delta = CoreStateDeltaV1::default();
        let mut records = Vec::with_capacity(ordered.len());
        for (_, logical_identity, evidence) in ordered {
            records.push(self.normalize_one(&mut state_delta, logical_identity, evidence)?);
        }
        Ok(PreparedNormalizationBatchV1 {
            batch: NormalizationBatchV1 {
                source_watermark: input.source_watermark,
                records,
            },
            state_delta,
        })
    }

    pub(crate) fn accept_delta(&mut self, state_delta: CoreStateDeltaV1) {
        self.state.logical.extend(state_delta.logical);
        self.state.economic.extend(state_delta.economic);
    }

    fn check_evidence_boundary(
        &self,
        watermark: &SimV2SourceWatermarkV1,
        evidence: &SourceNeutralEvidenceV1,
    ) -> Result<(), CoreErrorV1> {
        if evidence.source_reference.context_reference != self.context.context_reference {
            return Err(CoreErrorV1::ContextReferenceMismatch {
                expected: self.context.context_reference.clone(),
                found: evidence.source_reference.context_reference.clone(),
            });
        }
        if evidence.source_reference.watermark != *watermark {
            return Err(CoreErrorV1::SourceWatermarkMismatch {
                expected: watermark.clone(),
                found: evidence.source_reference.watermark.clone(),
            });
        }
        Ok(())
    }

    fn source_order(evidence: &SourceNeutralEvidenceV1) -> SourceOrderV1 {
        let sequence = match &evidence.source_reference.record {
            SimV2SourceRecordReferenceV1::AccountOpening => SourceSequenceV1::AccountOpening,
            SimV2SourceRecordReferenceV1::MarketEnrollment {
                enrollment_sequence,
            } => SourceSequenceV1::MarketEnrollment {
                enrollment_sequence: *enrollment_sequence,
            },
            SimV2SourceRecordReferenceV1::DirectResponse {
                private_feedback_sequence,
                ..
            }
            | SimV2SourceRecordReferenceV1::OrderLifecycle {
                private_feedback_sequence,
                ..
            }
            | SimV2SourceRecordReferenceV1::TradeLifecycle {
                private_feedback_sequence,
                ..
            }
            | SimV2SourceRecordReferenceV1::SettlementLifecycle {
                private_feedback_sequence,
                ..
            } => SourceSequenceV1::PrivateFeedback {
                private_feedback_sequence: *private_feedback_sequence,
            },
        };
        SourceOrderV1 {
            sequence,
            ordinal: evidence.source_ordinal,
        }
    }

    fn logical_identity(
        &self,
        key: &SourceNeutralLogicalKeyV1,
    ) -> Result<StableLogicalIdentityV1, CoreErrorV1> {
        serde_json::to_string(&(NORMALIZATION_CORE_V1, &self.context.account_identity, key))
            .map(|encoded| StableLogicalIdentityV1(format!("noe-logical-v1:{encoded}")))
            .map_err(|error| CoreErrorV1::IdentityEncoding {
                message: error.to_string(),
            })
    }

    fn fact_reference(
        &self,
        identity: &SourceNeutralEconomicIdentityV1,
    ) -> Result<NoeEconomicFactReferenceV1, CoreErrorV1> {
        serde_json::to_string(&(NORMALIZATION_CORE_V1, identity))
            .map(|encoded| NoeEconomicFactReferenceV1(format!("noe-economic-fact-v1:{encoded}")))
            .map_err(|error| CoreErrorV1::IdentityEncoding {
                message: error.to_string(),
            })
    }

    fn normalize_one(
        &self,
        state_delta: &mut CoreStateDeltaV1,
        logical_identity: StableLogicalIdentityV1,
        evidence: OrderedEvidenceV1,
    ) -> Result<NormalizationRecordV1, CoreErrorV1> {
        let cancellation_input_item_refs =
            Self::cancellation_input_item_refs(&evidence.source_reference)
                .map(<[CancelInputItemRefV2]>::to_vec);
        if let Some(existing) = state_delta
            .logical
            .get(&logical_identity)
            .or_else(|| self.state.logical.get(&logical_identity))
        {
            if self.stored_evidence_matches(state_delta, &existing.evidence, &evidence.kind)
                && existing.cancellation_input_item_refs.as_deref()
                    == cancellation_input_item_refs.as_deref()
            {
                if let SourceNeutralEvidenceKindV1::LateEconomicCoverage { economic_key } =
                    &evidence.kind
                {
                    let economic_identity = self.economic_identity(economic_key);
                    if let Some(fact) = state_delta
                        .economic
                        .get(&economic_identity)
                        .or_else(|| self.state.economic.get(&economic_identity))
                    {
                        let decision = EconomicAliasDecisionV1::LateEconomicCoverage;
                        return Ok(self.economic_alias(logical_identity, evidence, fact, decision));
                    }
                }
                return Ok(NormalizationRecordV1::Duplicate {
                    logical_identity,
                    source_order: evidence.source_order,
                    source_reference: evidence.source_reference,
                    original_source_reference: existing.source_reference.clone(),
                });
            }
            return Ok(NormalizationRecordV1::Conflict {
                scope: NormalizationConflictScopeV1::LogicalIdentity {
                    logical_identity: logical_identity.clone(),
                },
                logical_identity,
                source_order: evidence.source_order,
                source_reference: evidence.source_reference,
                conflicting_evidence: evidence.kind,
            });
        }

        match &evidence.kind {
            SourceNeutralEvidenceKindV1::Economic {
                evidence: economic_evidence,
            } => {
                let economic_identity = self.economic_identity(&economic_evidence.key());
                let disposition = economic_evidence.disposition();
                if let Some(existing) = state_delta
                    .economic
                    .get(&economic_identity)
                    .or_else(|| self.state.economic.get(&economic_identity))
                {
                    if existing.disposition == disposition {
                        state_delta.logical.insert(
                            logical_identity.clone(),
                            StoredLogicalV1 {
                                evidence: StoredEvidenceV1::EconomicAlias {
                                    economic_identity: economic_identity.clone(),
                                },
                                cancellation_input_item_refs,
                                source_reference: evidence.source_reference.clone(),
                            },
                        );
                        return Ok(self.economic_alias(
                            logical_identity,
                            evidence,
                            existing,
                            EconomicAliasDecisionV1::EconomicEvidence,
                        ));
                    }
                    state_delta.logical.insert(
                        logical_identity.clone(),
                        StoredLogicalV1 {
                            evidence: StoredEvidenceV1::Economic {
                                economic_identity: economic_identity.clone(),
                                disposition: disposition.clone(),
                            },
                            cancellation_input_item_refs,
                            source_reference: evidence.source_reference.clone(),
                        },
                    );
                    return Ok(NormalizationRecordV1::Conflict {
                        scope: NormalizationConflictScopeV1::EconomicIdentity { economic_identity },
                        logical_identity,
                        source_order: evidence.source_order,
                        source_reference: evidence.source_reference,
                        conflicting_evidence: evidence.kind,
                    });
                }

                let fact = SourceNeutralEconomicFactV1 {
                    fact_reference: self.fact_reference(&economic_identity)?,
                    logical_identity: logical_identity.clone(),
                    economic_identity: economic_identity.clone(),
                    source_reference: evidence.source_reference.clone(),
                    disposition,
                };
                state_delta.logical.insert(
                    logical_identity,
                    StoredLogicalV1 {
                        evidence: StoredEvidenceV1::Economic {
                            economic_identity,
                            disposition: fact.disposition.clone(),
                        },
                        cancellation_input_item_refs,
                        source_reference: evidence.source_reference.clone(),
                    },
                );
                state_delta
                    .economic
                    .insert(fact.economic_identity.clone(), fact.clone());
                Ok(NormalizationRecordV1::EconomicFact {
                    source_order: evidence.source_order,
                    fact,
                })
            }
            SourceNeutralEvidenceKindV1::LateEconomicCoverage { economic_key } => {
                let economic_identity = self.economic_identity(economic_key);
                let record = if let Some(fact) = state_delta
                    .economic
                    .get(&economic_identity)
                    .or_else(|| self.state.economic.get(&economic_identity))
                {
                    self.economic_alias(
                        logical_identity.clone(),
                        evidence.clone(),
                        fact,
                        EconomicAliasDecisionV1::LateEconomicCoverage,
                    )
                } else {
                    NormalizationRecordV1::Pending {
                        logical_identity: logical_identity.clone(),
                        source_order: evidence.source_order.clone(),
                        source_reference: evidence.source_reference.clone(),
                        evidence: evidence.kind.clone(),
                        scope: NormalizationPendingScopeV1::EconomicIdentity { economic_identity },
                    }
                };
                state_delta.logical.insert(
                    logical_identity,
                    StoredLogicalV1 {
                        evidence: StoredEvidenceV1::Evidence(evidence.kind),
                        cancellation_input_item_refs,
                        source_reference: evidence.source_reference,
                    },
                );
                Ok(record)
            }
            SourceNeutralEvidenceKindV1::Pending { scope } => {
                let normalized_scope = match scope {
                    PendingScopeV1::EvidenceGap { scope_identity } => {
                        NormalizationPendingScopeV1::EvidenceGap {
                            scope_identity: scope_identity.clone(),
                        }
                    }
                    PendingScopeV1::EconomicKey { economic_key } => {
                        NormalizationPendingScopeV1::EconomicIdentity {
                            economic_identity: self.economic_identity(economic_key),
                        }
                    }
                };
                let record = NormalizationRecordV1::Pending {
                    logical_identity: logical_identity.clone(),
                    source_order: evidence.source_order.clone(),
                    source_reference: evidence.source_reference.clone(),
                    evidence: evidence.kind.clone(),
                    scope: normalized_scope,
                };
                state_delta.logical.insert(
                    logical_identity,
                    StoredLogicalV1 {
                        evidence: StoredEvidenceV1::Evidence(evidence.kind),
                        cancellation_input_item_refs,
                        source_reference: evidence.source_reference,
                    },
                );
                Ok(record)
            }
            SourceNeutralEvidenceKindV1::Submission { .. }
            | SourceNeutralEvidenceKindV1::OrderLifecycle { .. }
            | SourceNeutralEvidenceKindV1::TradeLifecycle { .. }
            | SourceNeutralEvidenceKindV1::SettlementLifecycle { .. } => {
                let record = NormalizationRecordV1::Evidence {
                    logical_identity: logical_identity.clone(),
                    source_order: evidence.source_order.clone(),
                    source_reference: evidence.source_reference.clone(),
                    evidence: evidence.kind.clone(),
                };
                state_delta.logical.insert(
                    logical_identity,
                    StoredLogicalV1 {
                        evidence: StoredEvidenceV1::Evidence(evidence.kind),
                        cancellation_input_item_refs,
                        source_reference: evidence.source_reference,
                    },
                );
                Ok(record)
            }
        }
    }

    fn economic_alias(
        &self,
        logical_identity: StableLogicalIdentityV1,
        evidence: OrderedEvidenceV1,
        fact: &SourceNeutralEconomicFactV1,
        decision: EconomicAliasDecisionV1,
    ) -> NormalizationRecordV1 {
        NormalizationRecordV1::EconomicAlias {
            logical_identity,
            source_order: evidence.source_order,
            source_reference: evidence.source_reference,
            decision,
            economic_identity: fact.economic_identity.clone(),
            primary_logical_identity: fact.logical_identity.clone(),
            fact_reference: fact.fact_reference.clone(),
        }
    }

    fn economic_identity(
        &self,
        key: &SourceNeutralEconomicKeyV1,
    ) -> SourceNeutralEconomicIdentityV1 {
        match key {
            SourceNeutralEconomicKeyV1::AccountOpening {
                opening_evidence_identity,
            } => SourceNeutralEconomicIdentityV1::AccountOpening {
                account_identity: self.context.account_identity.clone(),
                opening_evidence_identity: opening_evidence_identity.clone(),
            },
            SourceNeutralEconomicKeyV1::AccountTransition {
                account_transition_seq,
            } => SourceNeutralEconomicIdentityV1::AccountTransition {
                account_identity: self.context.account_identity.clone(),
                account_transition_seq: *account_transition_seq,
            },
        }
    }

    fn economic_key(identity: &SourceNeutralEconomicIdentityV1) -> SourceNeutralEconomicKeyV1 {
        match identity {
            SourceNeutralEconomicIdentityV1::AccountOpening {
                opening_evidence_identity,
                ..
            } => SourceNeutralEconomicKeyV1::AccountOpening {
                opening_evidence_identity: opening_evidence_identity.clone(),
            },
            SourceNeutralEconomicIdentityV1::AccountTransition {
                account_transition_seq,
                ..
            } => SourceNeutralEconomicKeyV1::AccountTransition {
                account_transition_seq: *account_transition_seq,
            },
        }
    }

    fn stored_evidence_matches(
        &self,
        state_delta: &CoreStateDeltaV1,
        stored: &StoredEvidenceV1,
        incoming: &SourceNeutralEvidenceKindV1,
    ) -> bool {
        match (stored, incoming) {
            (StoredEvidenceV1::Evidence(stored), incoming) => stored == incoming,
            (
                StoredEvidenceV1::EconomicAlias { economic_identity },
                SourceNeutralEvidenceKindV1::Economic { evidence },
            ) => {
                economic_identity == &self.economic_identity(&evidence.key())
                    && state_delta
                        .economic
                        .get(economic_identity)
                        .or_else(|| self.state.economic.get(economic_identity))
                        .is_some_and(|fact| fact.disposition == evidence.disposition())
            }
            (
                StoredEvidenceV1::Economic {
                    economic_identity,
                    disposition,
                },
                SourceNeutralEvidenceKindV1::Economic { evidence },
            ) => {
                economic_identity == &self.economic_identity(&evidence.key())
                    && disposition == &evidence.disposition()
            }
            (StoredEvidenceV1::EconomicAlias { .. } | StoredEvidenceV1::Economic { .. }, _) => {
                false
            }
        }
    }

    fn cancellation_input_item_refs(
        source_reference: &SimV2SourceReferenceV1,
    ) -> Option<&[CancelInputItemRefV2]> {
        match &source_reference.record {
            SimV2SourceRecordReferenceV1::DirectResponse {
                association:
                    SimV2DirectResponseAssociationV1::CancellationTarget {
                        input_item_refs, ..
                    },
                ..
            } => Some(input_item_refs),
            _ => None,
        }
    }
}
