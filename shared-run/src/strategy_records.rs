//! 策略只读业务投影：单条已交付事实，不携带 NOE 批次、来源坐标或持久路径。
//!
//! 宿主继续保存原 Registry 和 NOE 引用。这里仅借用对应业务载荷，既不复制
//! 整批记录，也不通过格式化、序列化或比较重新暴露被隔离的宿主对象。
use crate::{
    CancelObjectRead, CancellationRequestsReadV1, CancellationScopeV1, ExecutionOrderRead,
    FrozenIntentRefV1, FrozenTradeEconomicsV1, IntentCommitmentReadV1, ObservedTradeApplicationV1,
    SubmittedLegReadV1,
};
use contracts::{
    AccountInstanceKeyV1, ConditionId, ExactDecimal, ExactRationalV1, ExecutionReadValueV1,
    ModelIntentV1, TokenId, economics::SourceNeutralEconomicDispositionV2,
    execution_normalization::*,
};
use execution_normalization::{
    EconomicAliasDecisionV1, MemoryEconomicFactReferenceV1, MemoryIdentityKeyV1,
    MemoryLogicalIdentityV1 as Identity, NoeConsumedRecordRef, NoeConsumptionVisibilityV1,
    NoeMemoryRecord, NormalizationConflictScopeV1, NormalizationPendingScopeV1,
    NormalizationRecord, SourceNeutralEntityKeyV1, SourceNeutralEvidenceKind,
};
use serde::Serialize;

/// 已交付账户与本地动作形成的账本业务快照；来源前缀继续仅供宿主审计。
/// ledger_entry_seq 是此账户已应用账本的版本，不是 SIM actor 输入序号。
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StrategyLedgerReadV1<'a> {
    pub account_instance_key: &'a AccountInstanceKeyV1,
    pub ledger_entry_seq: u64,
    pub available_through_utc_ms: Option<i64>,
    pub platform_confirmed_projection: &'a ledger::LedgerPlatformConfirmedProjectionV1,
    pub local_pending_reservation_projection: &'a ledger::LedgerLocalPendingReservationProjectionV1,
    pub market_projections: &'a [ledger::LedgerMarketProjectionV1],
}
impl<'a> From<&'a ledger::LedgerProjectionV2> for StrategyLedgerReadV1<'a> {
    fn from(value: &'a ledger::LedgerProjectionV2) -> Self {
        Self {
            account_instance_key: &value.account_instance_key,
            ledger_entry_seq: value.ledger_entry_seq,
            available_through_utc_ms: value.available_through_utc_ms,
            platform_confirmed_projection: &value.platform_confirmed_projection,
            local_pending_reservation_projection: &value.local_pending_reservation_projection,
            market_projections: &value.market_projections,
        }
    }
}

type Evidence<R> = SourceNeutralEvidenceKind<
    SourceNeutralTradeLifecycleResultV2<Identity>,
    SourceNeutralControlResult<
        SourceNeutralSubmissionOutcomeV2<Identity>,
        SourceNeutralOrderSnapshotV2<Identity>,
        Identity,
        R,
    >,
    Identity,
>;

/// 原单条 NOE 的业务内容。明确省略 source_order/source_reference；经济事实也逐项投影。
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum StrategyNoeRecordV1<'a, R> {
    Evidence {
        logical_identity: &'a Identity,
        evidence: &'a Evidence<R>,
    },
    EconomicFact {
        fact_reference: &'a MemoryEconomicFactReferenceV1,
        logical_identity: &'a Identity,
        economic_identity: &'a SourceNeutralEconomicIdentityV1,
        disposition: &'a SourceNeutralEconomicDispositionV1<Identity>,
    },
    EconomicFactV2 {
        fact_reference: &'a MemoryEconomicFactReferenceV1,
        logical_identity: &'a Identity,
        economic_identity: &'a SourceNeutralEconomicIdentityV1,
        disposition: &'a SourceNeutralEconomicDispositionV2<Identity>,
    },
    Duplicate {
        logical_identity: &'a Identity,
    },
    EconomicAlias {
        logical_identity: &'a Identity,
        decision: &'a EconomicAliasDecisionV1,
        economic_identity: &'a SourceNeutralEconomicIdentityV1,
        primary_logical_identity: &'a Identity,
        fact_reference: &'a MemoryEconomicFactReferenceV1,
    },
    Pending {
        logical_identity: &'a Identity,
        evidence: &'a Evidence<R>,
        scope: &'a NormalizationPendingScopeV1,
    },
    Conflict {
        logical_identity: &'a Identity,
        scope: &'a NormalizationConflictScopeV1<Identity>,
        conflicting_evidence: &'a Evidence<R>,
    },
}

impl<'a, R> From<&'a NoeMemoryRecord<R>> for StrategyNoeRecordV1<'a, R> {
    fn from(record: &'a NoeMemoryRecord<R>) -> Self {
        match record {
            NormalizationRecord::Evidence {
                logical_identity,
                evidence,
                ..
            } => Self::Evidence {
                logical_identity,
                evidence,
            },
            NormalizationRecord::EconomicFact { fact, .. } => Self::EconomicFact {
                fact_reference: &fact.fact_reference,
                logical_identity: &fact.logical_identity,
                economic_identity: &fact.economic_identity,
                disposition: &fact.disposition,
            },
            NormalizationRecord::EconomicFactV2 { fact, .. } => Self::EconomicFactV2 {
                fact_reference: &fact.fact_reference,
                logical_identity: &fact.logical_identity,
                economic_identity: &fact.economic_identity,
                disposition: &fact.disposition,
            },
            NormalizationRecord::Duplicate {
                logical_identity, ..
            } => Self::Duplicate { logical_identity },
            NormalizationRecord::EconomicAlias {
                logical_identity,
                decision,
                economic_identity,
                primary_logical_identity,
                fact_reference,
                ..
            } => Self::EconomicAlias {
                logical_identity,
                decision,
                economic_identity,
                primary_logical_identity,
                fact_reference,
            },
            NormalizationRecord::Pending {
                logical_identity,
                evidence,
                scope,
                ..
            } => Self::Pending {
                logical_identity,
                evidence,
                scope,
            },
            NormalizationRecord::Conflict {
                logical_identity,
                scope,
                conflicting_evidence,
                ..
            } => Self::Conflict {
                logical_identity,
                scope,
                conflicting_evidence,
            },
        }
    }
}

/// 此类型物理上只保存单条业务视图和已建立的交付可见性，没有原批引用。
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StrategyNoeRecordReadV1<'a, R> {
    record: StrategyNoeRecordV1<'a, R>,
    visibility: &'a NoeConsumptionVisibilityV1,
}
impl<'a, R: Clone + std::fmt::Debug> From<&'a NoeConsumedRecordRef<R>>
    for StrategyNoeRecordReadV1<'a, R>
{
    fn from(record: &'a NoeConsumedRecordRef<R>) -> Self {
        Self {
            record: record.record().into(),
            visibility: record.visibility(),
        }
    }
}
impl<'a, R: Clone> StrategyNoeRecordReadV1<'a, R> {
    /// 返回只含借用字段的小型投影值，不延长或复制宿主批次。
    pub fn record(&self) -> StrategyNoeRecordV1<'a, R> {
        self.record.clone()
    }
    pub fn visibility(&self) -> &'a NoeConsumptionVisibilityV1 {
        self.visibility
    }
}

/// 原冻结意图的业务读取；不提供 ExecutionNormalizationContext 或持久来源。
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StrategyFrozenIntentReadV1<'a> {
    global_request_id: &'a str,
    intent: &'a ModelIntentV1,
}
impl<'a> From<&'a FrozenIntentRefV1> for StrategyFrozenIntentReadV1<'a> {
    fn from(value: &'a FrozenIntentRefV1) -> Self {
        Self {
            global_request_id: value.global_request_id(),
            intent: value.intent(),
        }
    }
}
impl<'a> StrategyFrozenIntentReadV1<'a> {
    pub fn global_request_id(&self) -> &'a str {
        self.global_request_id
    }
    pub fn intent(&self) -> &'a ModelIntentV1 {
        self.intent
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StrategySubmittedLegReadV1<'a> {
    pub intent: StrategyFrozenIntentReadV1<'a>,
    pub batch_item_ordinal: u16,
}
impl<'a> From<SubmittedLegReadV1<'a>> for StrategySubmittedLegReadV1<'a> {
    fn from(value: SubmittedLegReadV1<'a>) -> Self {
        Self {
            intent: value.intent.into(),
            batch_item_ordinal: value.batch_item_ordinal,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StrategyExecutionOrderReadV1<'a, R> {
    pub order_identity: &'a Identity,
    pub condition_id: Option<&'a ConditionId>,
    pub token_id: Option<&'a TokenId>,
    pub base_quantity_applicability: Option<bool>,
    pub latest_cancel_feedback: Option<StrategyNoeRecordReadV1<'a, R>>,
    pub origin: Option<StrategySubmittedLegReadV1<'a>>,
    pub accepted: Option<StrategyNoeRecordReadV1<'a, R>>,
    pub formed: Option<StrategyNoeRecordReadV1<'a, R>>,
    pub latest_lifecycle: Option<StrategyNoeRecordReadV1<'a, R>>,
    pub latest_direct_feedback: Option<StrategyNoeRecordReadV1<'a, R>>,
    pub matched_shares: ExecutionReadValueV1<ExactDecimal>,
    pub active_unfilled_shares: ExecutionReadValueV1<ExactDecimal>,
    pub cancellable: ExecutionReadValueV1<bool>,
}
impl<'a, R: Clone + std::fmt::Debug> From<ExecutionOrderRead<'a, R>>
    for StrategyExecutionOrderReadV1<'a, R>
{
    fn from(value: ExecutionOrderRead<'a, R>) -> Self {
        Self {
            order_identity: value.order_identity,
            condition_id: value.condition_id,
            token_id: value.token_id,
            base_quantity_applicability: value.base_quantity_applicability,
            latest_cancel_feedback: value.latest_cancel_feedback.map(Into::into),
            origin: value.origin.map(Into::into),
            accepted: value.accepted.map(Into::into),
            formed: value.formed.map(Into::into),
            latest_lifecycle: value.latest_lifecycle.map(Into::into),
            latest_direct_feedback: value.latest_direct_feedback.map(Into::into),
            matched_shares: value.matched_shares,
            active_unfilled_shares: value.active_unfilled_shares,
            cancellable: value.cancellable,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StrategyIntentCommitmentReadV1<'a> {
    pub origin: StrategySubmittedLegReadV1<'a>,
    pub order_identity: Option<&'a Identity>,
    pub remaining_target_shares: ExecutionReadValueV1<ExactDecimal>,
}
impl<'a> From<IntentCommitmentReadV1<'a>> for StrategyIntentCommitmentReadV1<'a> {
    fn from(value: IntentCommitmentReadV1<'a>) -> Self {
        Self {
            origin: value.origin.into(),
            order_identity: value.order_identity,
            remaining_target_shares: value.remaining_target_shares,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum StrategyCancelObjectReadV1<'a, R> {
    Single {
        order: StrategyExecutionOrderReadV1<'a, R>,
    },
    Paired {
        origin: StrategyFrozenIntentReadV1<'a>,
        orders: [StrategyExecutionOrderReadV1<'a, R>; 2],
    },
    Market {
        condition_id: &'a ConditionId,
        orders: Vec<StrategyExecutionOrderReadV1<'a, R>>,
    },
}
impl<'a, R: Clone + std::fmt::Debug> From<CancelObjectRead<'a, R>>
    for StrategyCancelObjectReadV1<'a, R>
{
    fn from(value: CancelObjectRead<'a, R>) -> Self {
        match value {
            CancelObjectRead::Single { order } => Self::Single {
                order: order.into(),
            },
            CancelObjectRead::Paired { origin, orders } => Self::Paired {
                origin: origin.into(),
                orders: orders.map(Into::into),
            },
            CancelObjectRead::Market {
                condition_id,
                orders,
            } => Self::Market {
                condition_id,
                orders: orders.into_iter().map(Into::into).collect(),
            },
        }
    }
}
impl<'a, R> StrategyCancelObjectReadV1<'a, R> {
    pub fn request_scope(&self) -> CancellationScopeV1<'a> {
        match self {
            Self::Single { order } => {
                CancellationScopeV1::Orders(vec![platform_order_id(order.order_identity)])
            }
            Self::Paired { orders, .. } => CancellationScopeV1::Orders(
                orders
                    .iter()
                    .map(|order| platform_order_id(order.order_identity))
                    .collect(),
            ),
            Self::Market { condition_id, .. } => CancellationScopeV1::Market(condition_id),
        }
    }
}
fn platform_order_id(identity: &Identity) -> &str {
    match &identity.key {
        MemoryIdentityKeyV1::Entity(SourceNeutralEntityKeyV1::Order { source_order_id }) => {
            source_order_id
        }
        _ => unreachable!("订单读取来自原订单身份"),
    }
}

/// 账本已计算的单笔成交数值；引用原业务字段，不另做经济归约。
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StrategyTradeEconomicsReadV1<'a> {
    pub trade_identity: &'a Identity,
    pub fact: &'a SourceNeutralTradeLifecycleResultV2<Identity>,
    pub economic_applied: bool,
    pub success_applied: bool,
    pub actual_net_fee_pusd: &'a ExactDecimal,
    pub unpaired_contribution_shares: &'a ExecutionReadValueV1<ExactRationalV1>,
}
impl<'a> From<&'a FrozenTradeEconomicsV1> for StrategyTradeEconomicsReadV1<'a> {
    fn from(value: &'a FrozenTradeEconomicsV1) -> Self {
        Self {
            trade_identity: &value.trade_identity,
            fact: &value.fact,
            economic_applied: value.economic_applied,
            success_applied: value.success_applied,
            actual_net_fee_pusd: &value.actual_net_fee_pusd,
            unpaired_contribution_shares: &value.unpaired_contribution_shares,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StrategyObservedTradeApplicationV1<'a, R> {
    pub observed: Option<StrategyNoeRecordReadV1<'a, R>>,
    pub ledger: Option<StrategyTradeEconomicsReadV1<'a>>,
    fact: &'a SourceNeutralTradeLifecycleResultV2<Identity>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StrategyCancellationRequestsReadV1<'a> {
    pub pending_count: ExecutionReadValueV1<u64>,
    pub response_counts: [ExecutionReadValueV1<u64>; 3],
    pub latest_response:
        ExecutionReadValueV1<Option<StrategyNoeRecordReadV1<'a, CancellationRequestCompletedV1>>>,
}
impl<'a> From<CancellationRequestsReadV1<'a>> for StrategyCancellationRequestsReadV1<'a> {
    fn from(value: CancellationRequestsReadV1<'a>) -> Self {
        Self {
            pending_count: value.pending_count,
            response_counts: value.response_counts,
            latest_response: match value.latest_response {
                ExecutionReadValueV1::Known(record) => {
                    ExecutionReadValueV1::Known(record.map(Into::into))
                }
                ExecutionReadValueV1::Unavailable => ExecutionReadValueV1::Unavailable,
                ExecutionReadValueV1::NotApplicable => ExecutionReadValueV1::NotApplicable,
            },
        }
    }
}
impl<'a, R: Clone + std::fmt::Debug + Eq> From<&'a ObservedTradeApplicationV1<R>>
    for StrategyObservedTradeApplicationV1<'a, R>
{
    fn from(value: &'a ObservedTradeApplicationV1<R>) -> Self {
        Self {
            observed: value.observed.as_ref().map(Into::into),
            ledger: value.ledger.as_ref().map(Into::into),
            fact: value.fact(),
        }
    }
}
impl<'a, R> StrategyObservedTradeApplicationV1<'a, R> {
    pub fn fact(&self) -> &'a SourceNeutralTradeLifecycleResultV2<Identity> {
        self.fact
    }
    pub fn success_applied(&self) -> bool {
        self.ledger
            .as_ref()
            .is_some_and(|trade| trade.success_applied)
    }
}
