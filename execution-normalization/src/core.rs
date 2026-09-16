use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use contracts::execution_normalization::{
    ExecutionNormalizationContextReferenceV1, ExecutionNormalizationContextV1,
    NoeEconomicFactReferenceV1, SimV2DirectResponseAssociationV1, SimV2SourceRecordReferenceV1,
    SimV2SourceReferenceV1, SimV2SourceWatermarkV1, SourceNeutralAccountBalanceDeltaV1,
    SourceNeutralAccountTransitionKindV1, SourceNeutralControlResultV1,
    SourceNeutralEconomicDispositionV1, SourceNeutralEconomicFactV1,
    SourceNeutralEconomicIdentityV1, SourceNeutralMarketEnrollmentResultV1,
    SourceNeutralOrderLifecycleResultV1, SourceNeutralSettlementLifecycleResultV1,
    SourceNeutralTradeLifecycleResultV1, StableLogicalIdentityV1,
};
use contracts::{CancelInputItemRefV2, ConditionId, ExactDecimal};
use serde::{Deserialize, Serialize};

use crate::identity::NormalizationIdentity;
use contracts::execution_normalization::SourceNeutralAccountIdentityV1;

#[cfg(test)]
thread_local! { pub(crate) static NORMALIZATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

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
    DirectResponseRequest {
        global_request_id: String,
    },
    MarketEnrollment {
        enrollment_request_id: String,
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
        private_feedback_sequence: u64,
    },
    EconomicEvidence {
        evidence_identity: String,
    },
    AccountTransition {
        account_transition_seq: u64,
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
pub enum SourceNeutralEconomicEvidenceV1<I = StableLogicalIdentityV1> {
    AccountOpening {
        opening_evidence_identity: String,
        available_pusd: ExactDecimal,
    },
    AccountTransition {
        account_transition_seq: u64,
        economic_effective_at_utc_ms: i64,
        condition_id: Option<ConditionId>,
        transition: SourceNeutralAccountTransitionKindV1<I>,
        balance_delta: Box<SourceNeutralAccountBalanceDeltaV1>,
    },
}

impl<I: Clone> SourceNeutralEconomicEvidenceV1<I> {
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

    fn disposition(&self) -> SourceNeutralEconomicDispositionV1<I> {
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SourceNeutralEntityKeyV1 {
    Order { source_order_id: String },
    Trade { source_trade_id: String },
    Resolution { source_resolution_id: String },
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
pub enum SourceNeutralEvidenceKind<T, C = SourceNeutralControlResultV1, I = StableLogicalIdentityV1>
{
    MarketEnrollment {
        result: SourceNeutralMarketEnrollmentResultV1,
    },
    Control {
        result: C,
    },
    OrderLifecycle {
        result: SourceNeutralOrderLifecycleResultV1<I>,
    },
    TradeLifecycle {
        result: T,
    },
    SettlementLifecycle {
        result: SourceNeutralSettlementLifecycleResultV1<I>,
    },
    Economic {
        evidence: SourceNeutralEconomicEvidenceV1<I>,
    },
    LateEconomicCoverage {
        economic_key: SourceNeutralEconomicKeyV1,
    },
    Pending {
        scope: PendingScopeV1,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceNeutralEvidence<T, C = SourceNeutralControlResultV1, I = StableLogicalIdentityV1> {
    pub source_reference: SimV2SourceReferenceV1,
    pub source_ordinal: u32,
    pub logical_key: SourceNeutralLogicalKeyV1,
    pub kind: SourceNeutralEvidenceKind<T, C, I>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizationBatchInput<T, C = SourceNeutralControlResultV1, I = StableLogicalIdentityV1>
{
    pub source_watermark: SimV2SourceWatermarkV1,
    pub evidence: Vec<SourceNeutralEvidence<T, C, I>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NormalizationConflictScopeV1<I = StableLogicalIdentityV1> {
    LogicalIdentity {
        logical_identity: I,
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
pub enum NormalizationRecord<
    T,
    C = SourceNeutralControlResultV1,
    I = StableLogicalIdentityV1,
    F = NoeEconomicFactReferenceV1,
> {
    Evidence {
        logical_identity: I,
        source_order: SourceOrderV1,
        source_reference: SimV2SourceReferenceV1,
        evidence: SourceNeutralEvidenceKind<T, C, I>,
    },
    EconomicFact {
        source_order: SourceOrderV1,
        fact: SourceNeutralEconomicFactV1<I, F>,
    },
    Duplicate {
        logical_identity: I,
        source_order: SourceOrderV1,
        source_reference: SimV2SourceReferenceV1,
        original_source_reference: SimV2SourceReferenceV1,
    },
    EconomicAlias {
        logical_identity: I,
        source_order: SourceOrderV1,
        source_reference: SimV2SourceReferenceV1,
        decision: EconomicAliasDecisionV1,
        economic_identity: SourceNeutralEconomicIdentityV1,
        primary_logical_identity: I,
        fact_reference: F,
    },
    Pending {
        logical_identity: I,
        source_order: SourceOrderV1,
        source_reference: SimV2SourceReferenceV1,
        evidence: SourceNeutralEvidenceKind<T, C, I>,
        scope: NormalizationPendingScopeV1,
    },
    Conflict {
        logical_identity: I,
        source_order: SourceOrderV1,
        source_reference: SimV2SourceReferenceV1,
        scope: NormalizationConflictScopeV1<I>,
        conflicting_evidence: SourceNeutralEvidenceKind<T, C, I>,
    },
}

impl<T: Clone + PartialEq, C: Clone + PartialEq, I, F> NormalizationRecord<T, C, I, F> {
    pub fn logical_identity(&self) -> &I {
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

    pub fn economic_fact(&self) -> Option<&SourceNeutralEconomicFactV1<I, F>> {
        match self {
            Self::EconomicFact { fact, .. } => Some(fact),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizationBatch<
    T,
    C = SourceNeutralControlResultV1,
    I = StableLogicalIdentityV1,
    F = NoeEconomicFactReferenceV1,
> {
    pub source_watermark: SimV2SourceWatermarkV1,
    pub records: Vec<NormalizationRecord<T, C, I, F>>,
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

/// 必要幂等与经济身份索引的当前条目数；按需读取，不估算堆字节。
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct NormalizationIndexCountsV1 {
    pub logical: usize,
    pub economic: usize,
}

#[derive(Debug)]
pub(crate) struct CoreState<T, C, I, F> {
    logical: BTreeMap<I, StoredLogical<T, C, I>>,
    economic: BTreeMap<SourceNeutralEconomicIdentityV1, SourceNeutralEconomicFactV1<I, F>>,
}

#[derive(Debug)]
pub(crate) struct CoreStateDelta<T, C, I, F> {
    logical: BTreeMap<I, StoredLogical<T, C, I>>,
    economic: BTreeMap<SourceNeutralEconomicIdentityV1, SourceNeutralEconomicFactV1<I, F>>,
}

#[derive(Clone, Debug)]
struct StoredLogical<T, C, I> {
    evidence: StoredEvidence<T, C, I>,
    cancellation_input_item_refs: Option<Vec<CancelInputItemRefV2>>,
    source_reference: SimV2SourceReferenceV1,
}

#[derive(Clone, Debug)]
enum StoredEvidence<T, C, I> {
    Evidence(SourceNeutralEvidenceKind<T, C, I>),
    EconomicAlias {
        economic_identity: SourceNeutralEconomicIdentityV1,
    },
    Economic {
        economic_identity: SourceNeutralEconomicIdentityV1,
        disposition: SourceNeutralEconomicDispositionV1<I>,
    },
}

#[derive(Clone)]
struct OrderedEvidence<T, C, I> {
    source_reference: SimV2SourceReferenceV1,
    source_order: SourceOrderV1,
    kind: SourceNeutralEvidenceKind<T, C, I>,
}

pub struct PreparedNormalizationBatch<
    T,
    C = SourceNeutralControlResultV1,
    I = StableLogicalIdentityV1,
    F = NoeEconomicFactReferenceV1,
> {
    batch: NormalizationBatch<T, C, I, F>,
    state_delta: CoreStateDelta<T, C, I, F>,
}

impl<T: Clone + PartialEq, C: Clone + PartialEq, I, F> PreparedNormalizationBatch<T, C, I, F> {
    pub fn batch(&self) -> &NormalizationBatch<T, C, I, F> {
        &self.batch
    }

    pub(crate) fn into_parts(self) -> (NormalizationBatch<T, C, I, F>, CoreStateDelta<T, C, I, F>) {
        (self.batch, self.state_delta)
    }
}

#[derive(Debug)]
pub struct NormalizationCore<
    T,
    C = SourceNeutralControlResultV1,
    I = StableLogicalIdentityV1,
    F = NoeEconomicFactReferenceV1,
> {
    context: ExecutionNormalizationContextV1,
    account: std::sync::Arc<SourceNeutralAccountIdentityV1>,
    state: CoreState<T, C, I, F>,
}

impl<T: Clone + PartialEq, C: Clone + PartialEq, I: NormalizationIdentity<F>, F: Clone + PartialEq>
    NormalizationCore<T, C, I, F>
{
    pub fn new(context: ExecutionNormalizationContextV1) -> Self {
        Self {
            account: std::sync::Arc::new(context.account_identity.clone()),
            context,
            state: CoreState::default(),
        }
    }

    pub fn retained_index_counts(&self) -> NormalizationIndexCountsV1 {
        NormalizationIndexCountsV1 {
            logical: self.state.logical.len(),
            economic: self.state.economic.len(),
        }
    }

    pub fn account_identity(&self) -> &std::sync::Arc<SourceNeutralAccountIdentityV1> {
        &self.account
    }

    pub(crate) fn restore<'a>(
        context: ExecutionNormalizationContextV1,
        records: impl IntoIterator<Item = &'a NormalizationRecord<T, C, I, F>>,
    ) -> Self
    where
        T: 'a,
        C: 'a,
        I: 'a,
        F: 'a,
    {
        let mut core = Self::new(context);
        for record in records {
            match record {
                NormalizationRecord::Evidence {
                    logical_identity,
                    source_reference,
                    evidence,
                    ..
                }
                | NormalizationRecord::Pending {
                    logical_identity,
                    source_reference,
                    evidence,
                    ..
                } => {
                    core.state.logical.insert(
                        logical_identity.clone(),
                        StoredLogical {
                            evidence: StoredEvidence::Evidence(evidence.clone()),
                            cancellation_input_item_refs: Self::cancellation_input_item_refs(
                                source_reference,
                            )
                            .map(<[CancelInputItemRefV2]>::to_vec),
                            source_reference: source_reference.clone(),
                        },
                    );
                }
                NormalizationRecord::EconomicFact { fact, .. } => {
                    core.state.logical.insert(
                        fact.logical_identity.clone(),
                        StoredLogical {
                            evidence: StoredEvidence::Economic {
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
                NormalizationRecord::EconomicAlias {
                    logical_identity,
                    source_reference,
                    decision,
                    economic_identity,
                    ..
                } => {
                    let evidence = match decision {
                        EconomicAliasDecisionV1::EconomicEvidence => {
                            StoredEvidence::EconomicAlias {
                                economic_identity: economic_identity.clone(),
                            }
                        }
                        EconomicAliasDecisionV1::LateEconomicCoverage => StoredEvidence::Evidence(
                            SourceNeutralEvidenceKind::LateEconomicCoverage {
                                economic_key: Self::economic_key(economic_identity),
                            },
                        ),
                    };
                    core.state.logical.insert(
                        logical_identity.clone(),
                        StoredLogical {
                            evidence,
                            cancellation_input_item_refs: Self::cancellation_input_item_refs(
                                source_reference,
                            )
                            .map(<[CancelInputItemRefV2]>::to_vec),
                            source_reference: source_reference.clone(),
                        },
                    );
                }
                NormalizationRecord::Conflict {
                    logical_identity,
                    source_reference,
                    scope: NormalizationConflictScopeV1::EconomicIdentity { .. },
                    conflicting_evidence,
                    ..
                } => {
                    let evidence = match conflicting_evidence {
                        SourceNeutralEvidenceKind::Economic { evidence } => {
                            StoredEvidence::Economic {
                                economic_identity: core.economic_identity(&evidence.key()),
                                disposition: evidence.disposition(),
                            }
                        }
                        evidence => StoredEvidence::Evidence(evidence.clone()),
                    };
                    core.state.logical.insert(
                        logical_identity.clone(),
                        StoredLogical {
                            evidence,
                            cancellation_input_item_refs: Self::cancellation_input_item_refs(
                                source_reference,
                            )
                            .map(<[CancelInputItemRefV2]>::to_vec),
                            source_reference: source_reference.clone(),
                        },
                    );
                }
                NormalizationRecord::Duplicate { .. }
                | NormalizationRecord::Conflict {
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

    pub(crate) fn entity_identity(&self, key: &SourceNeutralEntityKeyV1) -> Result<I, CoreErrorV1> {
        I::entity(&self.account, key)
    }

    pub fn prepare_batch(
        &self,
        input: NormalizationBatchInput<T, C, I>,
    ) -> Result<PreparedNormalizationBatch<T, C, I, F>, CoreErrorV1> {
        #[cfg(test)]
        NORMALIZATIONS.with(|count| count.set(count.get() + 1));
        let mut ordered = Vec::with_capacity(input.evidence.len());
        for evidence in input.evidence {
            self.check_evidence_boundary(&input.source_watermark, &evidence)?;
            let logical_identity = self.logical_identity(&evidence.logical_key)?;
            let source_order = Self::source_order(&evidence);
            ordered.push((
                source_order.clone(),
                logical_identity,
                OrderedEvidence {
                    source_reference: evidence.source_reference,
                    source_order,
                    kind: evidence.kind,
                },
            ));
        }
        ordered.sort_by(|left, right| left.0.cmp(&right.0));
        // 原 SourceOrder 同序时沿用旧字符串比较；真实映射器逐项唯一 ordinal 不进入此分支。
        let mut start = 0;
        while start < ordered.len() {
            let mut end = start + 1;
            while end < ordered.len() && ordered[end].0 == ordered[start].0 {
                end += 1;
            }
            if end - start > 1 {
                let mut tied = ordered
                    .drain(start..end)
                    .map(|entry| Ok((entry.1.legacy_sort_key()?, entry)))
                    .collect::<Result<Vec<_>, CoreErrorV1>>()?;
                tied.sort_by(|left, right| left.0.cmp(&right.0));
                ordered.splice(start..start, tied.into_iter().map(|(_, entry)| entry));
            }
            start = end;
        }

        let mut state_delta = CoreStateDelta::default();
        let mut records = Vec::with_capacity(ordered.len());
        for (_, logical_identity, evidence) in ordered {
            records.push(self.normalize_one(&mut state_delta, logical_identity, evidence)?);
        }
        Ok(PreparedNormalizationBatch {
            batch: NormalizationBatch {
                source_watermark: input.source_watermark,
                records,
            },
            state_delta,
        })
    }

    /// 新内存运行只归一化一次；后台接收返回的不可变结果，不重放核心。
    pub fn normalize_and_accept(
        &mut self,
        input: NormalizationBatchInput<T, C, I>,
    ) -> Result<NormalizationBatch<T, C, I, F>, CoreErrorV1> {
        let (batch, delta) = self.prepare_batch(input)?.into_parts();
        self.accept_delta(delta);
        Ok(batch)
    }

    pub(crate) fn accept_delta(&mut self, state_delta: CoreStateDelta<T, C, I, F>) {
        self.state.logical.extend(state_delta.logical);
        self.state.economic.extend(state_delta.economic);
    }

    fn check_evidence_boundary(
        &self,
        watermark: &SimV2SourceWatermarkV1,
        evidence: &SourceNeutralEvidence<T, C, I>,
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

    fn source_order(evidence: &SourceNeutralEvidence<T, C, I>) -> SourceOrderV1 {
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

    fn logical_identity(&self, key: &SourceNeutralLogicalKeyV1) -> Result<I, CoreErrorV1> {
        I::logical(&self.account, key)
    }

    fn fact_reference(&self, identity: &SourceNeutralEconomicIdentityV1) -> Result<F, CoreErrorV1> {
        I::fact_reference(identity)
    }

    fn normalize_one(
        &self,
        state_delta: &mut CoreStateDelta<T, C, I, F>,
        logical_identity: I,
        evidence: OrderedEvidence<T, C, I>,
    ) -> Result<NormalizationRecord<T, C, I, F>, CoreErrorV1> {
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
                if let SourceNeutralEvidenceKind::LateEconomicCoverage { economic_key } =
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
                return Ok(NormalizationRecord::Duplicate {
                    logical_identity,
                    source_order: evidence.source_order,
                    source_reference: evidence.source_reference,
                    original_source_reference: existing.source_reference.clone(),
                });
            }
            return Ok(NormalizationRecord::Conflict {
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
            SourceNeutralEvidenceKind::Economic {
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
                            StoredLogical {
                                evidence: StoredEvidence::EconomicAlias {
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
                        StoredLogical {
                            evidence: StoredEvidence::Economic {
                                economic_identity: economic_identity.clone(),
                                disposition: disposition.clone(),
                            },
                            cancellation_input_item_refs,
                            source_reference: evidence.source_reference.clone(),
                        },
                    );
                    return Ok(NormalizationRecord::Conflict {
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
                    StoredLogical {
                        evidence: StoredEvidence::Economic {
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
                Ok(NormalizationRecord::EconomicFact {
                    source_order: evidence.source_order,
                    fact,
                })
            }
            SourceNeutralEvidenceKind::LateEconomicCoverage { economic_key } => {
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
                    NormalizationRecord::Pending {
                        logical_identity: logical_identity.clone(),
                        source_order: evidence.source_order.clone(),
                        source_reference: evidence.source_reference.clone(),
                        evidence: evidence.kind.clone(),
                        scope: NormalizationPendingScopeV1::EconomicIdentity { economic_identity },
                    }
                };
                state_delta.logical.insert(
                    logical_identity,
                    StoredLogical {
                        evidence: StoredEvidence::Evidence(evidence.kind),
                        cancellation_input_item_refs,
                        source_reference: evidence.source_reference,
                    },
                );
                Ok(record)
            }
            SourceNeutralEvidenceKind::Pending { scope } => {
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
                let record = NormalizationRecord::Pending {
                    logical_identity: logical_identity.clone(),
                    source_order: evidence.source_order.clone(),
                    source_reference: evidence.source_reference.clone(),
                    evidence: evidence.kind.clone(),
                    scope: normalized_scope,
                };
                state_delta.logical.insert(
                    logical_identity,
                    StoredLogical {
                        evidence: StoredEvidence::Evidence(evidence.kind),
                        cancellation_input_item_refs,
                        source_reference: evidence.source_reference,
                    },
                );
                Ok(record)
            }
            SourceNeutralEvidenceKind::MarketEnrollment { .. }
            | SourceNeutralEvidenceKind::Control { .. }
            | SourceNeutralEvidenceKind::OrderLifecycle { .. }
            | SourceNeutralEvidenceKind::TradeLifecycle { .. }
            | SourceNeutralEvidenceKind::SettlementLifecycle { .. } => {
                let record = NormalizationRecord::Evidence {
                    logical_identity: logical_identity.clone(),
                    source_order: evidence.source_order.clone(),
                    source_reference: evidence.source_reference.clone(),
                    evidence: evidence.kind.clone(),
                };
                state_delta.logical.insert(
                    logical_identity,
                    StoredLogical {
                        evidence: StoredEvidence::Evidence(evidence.kind),
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
        logical_identity: I,
        evidence: OrderedEvidence<T, C, I>,
        fact: &SourceNeutralEconomicFactV1<I, F>,
        decision: EconomicAliasDecisionV1,
    ) -> NormalizationRecord<T, C, I, F> {
        NormalizationRecord::EconomicAlias {
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
        state_delta: &CoreStateDelta<T, C, I, F>,
        stored: &StoredEvidence<T, C, I>,
        incoming: &SourceNeutralEvidenceKind<T, C, I>,
    ) -> bool {
        match (stored, incoming) {
            (StoredEvidence::Evidence(stored), incoming) => stored == incoming,
            (
                StoredEvidence::EconomicAlias { economic_identity },
                SourceNeutralEvidenceKind::Economic { evidence },
            ) => {
                economic_identity == &self.economic_identity(&evidence.key())
                    && state_delta
                        .economic
                        .get(economic_identity)
                        .or_else(|| self.state.economic.get(economic_identity))
                        .is_some_and(|fact| fact.disposition == evidence.disposition())
            }
            (
                StoredEvidence::Economic {
                    economic_identity,
                    disposition,
                },
                SourceNeutralEvidenceKind::Economic { evidence },
            ) => {
                economic_identity == &self.economic_identity(&evidence.key())
                    && disposition == &evidence.disposition()
            }
            (StoredEvidence::EconomicAlias { .. } | StoredEvidence::Economic { .. }, _) => false,
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

impl<T, C, I, F> Default for CoreState<T, C, I, F> {
    fn default() -> Self {
        Self {
            logical: BTreeMap::new(),
            economic: BTreeMap::new(),
        }
    }
}

impl<T, C, I, F> Default for CoreStateDelta<T, C, I, F> {
    fn default() -> Self {
        Self {
            logical: BTreeMap::new(),
            economic: BTreeMap::new(),
        }
    }
}

// 各载荷版本复用同一规范化算法，每个运行只拥有一个核心状态。
pub type SourceNeutralEvidenceKindV1 =
    SourceNeutralEvidenceKind<SourceNeutralTradeLifecycleResultV1>;
pub type SourceNeutralEvidenceKindV2 = SourceNeutralEvidenceKind<
    contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
>;
pub type SourceNeutralEvidenceV1 = SourceNeutralEvidence<SourceNeutralTradeLifecycleResultV1>;
pub type SourceNeutralEvidenceV2 =
    SourceNeutralEvidence<contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2>;
pub type NormalizationBatchInputV1 = NormalizationBatchInput<SourceNeutralTradeLifecycleResultV1>;
pub type NormalizationBatchInputV2 = NormalizationBatchInput<
    contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
>;
pub type NormalizationRecordV1 = NormalizationRecord<SourceNeutralTradeLifecycleResultV1>;
pub type NormalizationRecordV2 =
    NormalizationRecord<contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2>;
pub type NormalizationBatchV1 = NormalizationBatch<SourceNeutralTradeLifecycleResultV1>;
pub type NormalizationBatchV2 =
    NormalizationBatch<contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2>;
pub type PreparedNormalizationBatchV1 =
    PreparedNormalizationBatch<SourceNeutralTradeLifecycleResultV1>;
pub type PreparedNormalizationBatchV2 = PreparedNormalizationBatch<
    contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
>;
pub type NormalizationCoreV1<T = SourceNeutralTradeLifecycleResultV1> = NormalizationCore<T>;
pub type NormalizationCoreV2 =
    NormalizationCore<contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2>;

impl NormalizationCoreV2 {
    pub fn prepare_batch_v2(
        &self,
        input: NormalizationBatchInputV2,
    ) -> Result<PreparedNormalizationBatchV2, CoreErrorV1> {
        self.prepare_batch(input)
    }
}

pub type SourceNeutralEvidenceKindV3 = SourceNeutralEvidenceKind<
    contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
    contracts::execution_normalization::SourceNeutralControlResultV2,
>;
pub type SourceNeutralEvidenceV3 = SourceNeutralEvidence<
    contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
    contracts::execution_normalization::SourceNeutralControlResultV2,
>;
pub type NormalizationBatchInputV3 = NormalizationBatchInput<
    contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
    contracts::execution_normalization::SourceNeutralControlResultV2,
>;
pub type NormalizationRecordV3 = NormalizationRecord<
    contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
    contracts::execution_normalization::SourceNeutralControlResultV2,
>;
pub type NormalizationBatchV3 = NormalizationBatch<
    contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
    contracts::execution_normalization::SourceNeutralControlResultV2,
>;
pub type PreparedNormalizationBatchV3 = PreparedNormalizationBatch<
    contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
    contracts::execution_normalization::SourceNeutralControlResultV2,
>;
pub type NormalizationCoreV3 = NormalizationCore<
    contracts::execution_normalization::SourceNeutralTradeLifecycleResultV2,
    contracts::execution_normalization::SourceNeutralControlResultV2,
>;
impl NormalizationCoreV3 {
    pub fn prepare_batch_v3(
        &self,
        input: NormalizationBatchInputV3,
    ) -> Result<PreparedNormalizationBatchV3, CoreErrorV1> {
        self.prepare_batch(input)
    }
}
