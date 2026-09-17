//! 同一 Ledger 核心的新来源入口和唯一后台材料。
use super::*;
use crate::projection::LedgerProjectionV2;
use contracts::execution_normalization::*;
use execution_normalization::{NormalizationRecord, SourceNeutralEvidenceKind};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LedgerMemoryApplyOutcomeV1 {
    Applied(LedgerApplyOutcomeV1),
    NotApplicable,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LedgerRecordMaterialKindV2 {
    Genesis,
    Transaction,
    ReservationDecision,
}
#[derive(Clone, Debug)]
pub struct LedgerRecordMaterialV2 {
    pub(super) payload: Arc<DurablePayloadV1>,
}
impl LedgerRecordMaterialV2 {
    pub(super) fn new(payload: DurablePayloadV1) -> Self {
        Self {
            payload: Arc::new(payload),
        }
    }
    pub fn waiting_for_noe_stability(&self) -> bool {
        matches!(self.payload.as_ref(), DurablePayloadV1::Transaction(e) if matches!(&e.transaction.source, LedgerTransactionSourceV1::Memory(r) if r.durable_reference().is_none()))
    }
    pub fn kind(&self) -> LedgerRecordMaterialKindV2 {
        match self.payload.as_ref() {
            DurablePayloadV1::Genesis { .. } => LedgerRecordMaterialKindV2::Genesis,
            DurablePayloadV1::Transaction(_) => LedgerRecordMaterialKindV2::Transaction,
            DurablePayloadV1::ReservationDecision { .. } => {
                LedgerRecordMaterialKindV2::ReservationDecision
            }
        }
    }
    pub fn ledger_entry_seq(&self) -> Option<u64> {
        match self.payload.as_ref() {
            DurablePayloadV1::Transaction(e) => Some(e.ledger_entry_seq()),
            _ => None,
        }
    }
}
impl LedgerWriterV1 {
    /// 新内存实例初始化接线；不同时绑定旧同步耐久路径。
    pub fn bind_record_sink_v2(&mut self, mut sink: Box<dyn FnMut(LedgerRecordMaterialV2)>) {
        sink(LedgerRecordMaterialV2::new(DurablePayloadV1::Genesis {
            policy: self.policy.clone(),
        }));
        self.record_sink = Some(sink);
    }
    pub fn projection_v2(&self) -> Option<LedgerProjectionV2> {
        let p = self.projection()?;
        Some(LedgerProjectionV2 {
            account_instance_key: p.account_instance_key,
            ledger_entry_seq: p.ledger_entry_seq,
            platform_source_prefix: p.platform_source_prefix,
            ledger_local_prefix: p.ledger_local_prefix,
            available_through_utc_ms: self.account.as_ref()?.available_through_utc_ms,
            platform_confirmed_projection: p.platform_confirmed_projection,
            local_pending_reservation_projection: p.local_pending_reservation_projection,
            market_projections: p.market_projections,
        })
    }
    /// 耐久数据不能成为业务应用能力。
    /// ```compile_fail
    /// fn apply(writer: &mut ledger::LedgerWriterV1,
    ///          view: &execution_normalization::NoeDurableRecordViewV1) {
    ///     writer.apply_noe_memory_v1(view);
    /// }
    /// ```
    /// ```compile_fail
    /// fn apply(writer: &mut ledger::LedgerWriterV1,
    ///          data: &execution_normalization::NoeConsumedRecordReferenceV1) {
    ///     writer.apply_noe_memory_v1(data);
    /// }
    /// ```
    pub fn apply_noe_memory_v1(
        &mut self,
        record: &NoeConsumedRecordRefV1,
    ) -> Result<LedgerMemoryApplyOutcomeV1, LedgerErrorV1> {
        if let Some(seq) = self.applied_memory.get(&record.position()) {
            return Ok(LedgerMemoryApplyOutcomeV1::Applied(
                LedgerApplyOutcomeV1::AlreadyApplied {
                    ledger_entry_seq: *seq,
                },
            ));
        }
        let source = LedgerTransactionSourceV1::Memory(record.clone());
        let at = match record.visibility() {
            NoeConsumptionVisibilityV1::Initialization => None,
            NoeConsumptionVisibilityV1::Feedback {
                available_at_utc_ms,
                ..
            } => Some(*available_at_utc_ms),
        };
        let at = at.max(
            self.account
                .as_ref()
                .and_then(|a| a.available_through_utc_ms),
        );
        let account = &record.context().source.account_instance_key;
        let out = match record.record() {
            NormalizationRecord::EconomicFact { fact, .. } => match &fact.disposition {
                SourceNeutralEconomicDispositionV1::AccountOpening { available_pusd } => self
                    .apply_opening(
                        &source,
                        account,
                        &record.context().source.account_opening_evidence_identity,
                        at,
                        available_pusd,
                    )?,
                SourceNeutralEconomicDispositionV1::AccountTransition {
                    condition_id,
                    transition,
                    balance_delta,
                    ..
                } => {
                    let SourceNeutralEconomicIdentityV1::AccountTransition {
                        account_transition_seq,
                        ..
                    } = fact.economic_identity
                    else {
                        unreachable!("NOE规范化经济身份")
                    };
                    self.apply_transition(
                        &source,
                        account,
                        account_transition_seq,
                        at,
                        condition_id.as_ref(),
                        transition,
                        balance_delta,
                        &fact.source_reference.record,
                    )?
                }
            },
            NormalizationRecord::Evidence {
                evidence,
                source_reference,
                ..
            } => {
                let Some(evidence) = ledger_evidence(evidence) else {
                    return Ok(LedgerMemoryApplyOutcomeV1::NotApplicable);
                };
                self.apply_lifecycle_evidence(
                    &source,
                    account,
                    at,
                    &source_reference.record,
                    &evidence,
                )?
            }
            NormalizationRecord::Pending {
                source_reference, ..
            } if self.account.is_some() => {
                self.apply_pending_result(&source, account, at, &source_reference.record)?
            }
            _ => return Ok(LedgerMemoryApplyOutcomeV1::NotApplicable),
        };
        Ok(LedgerMemoryApplyOutcomeV1::Applied(out))
    }
}
type MemoryIdentity = execution_normalization::MemoryLogicalIdentityV1;
type MemoryControl = SourceNeutralControlResult<
    SourceNeutralSubmissionOutcomeV2<MemoryIdentity>,
    SourceNeutralOrderSnapshotV2<MemoryIdentity>,
    MemoryIdentity,
>;
fn old_order(
    order: &SourceNeutralOrderSnapshotV2<MemoryIdentity>,
) -> SourceNeutralOrderSnapshotV1<MemoryIdentity> {
    SourceNeutralOrderSnapshotV1 {
        order_identity: order.order_identity.clone(),
        condition_id: order.condition_id.clone(),
        token_id: order.token_id.clone(),
        state: order.state.clone(),
    }
}
fn ledger_control(result: &MemoryControl) -> LedgerControl<MemoryIdentity> {
    match result {
        SourceNeutralControlResult::Submission { outcome } => LedgerControl::Submission {
            outcome: match outcome {
                SourceNeutralSubmissionOutcomeV2::Accepted { order, .. } => {
                    SourceNeutralSubmissionOutcomeV1::Accepted {
                        order: order.clone(),
                    }
                }
                SourceNeutralSubmissionOutcomeV2::RejectedNoEffect => {
                    SourceNeutralSubmissionOutcomeV1::RejectedNoEffect
                }
                SourceNeutralSubmissionOutcomeV2::EvidenceUnknownNoEffect => {
                    SourceNeutralSubmissionOutcomeV1::EvidenceUnknownNoEffect
                }
            },
        },
        SourceNeutralControlResult::Cancellation {
            order_identity,
            outcome,
        } => LedgerControl::Cancellation {
            order_identity: order_identity.clone(),
            outcome: outcome.clone(),
        },
        SourceNeutralControlResult::OrderQuery { result } => LedgerControl::OrderQuery {
            result: match result {
                SourceNeutralOrderQueryResult::Found { order } => {
                    SourceNeutralOrderQueryResult::Found {
                        order: old_order(order),
                    }
                }
                SourceNeutralOrderQueryResult::NotFound => SourceNeutralOrderQueryResult::NotFound,
                SourceNeutralOrderQueryResult::OutOfRetention(x) => {
                    SourceNeutralOrderQueryResult::OutOfRetention(x.clone())
                }
            },
        },
        SourceNeutralControlResult::OrdersQuery { result } => LedgerControl::OrdersQuery {
            result: match result {
                SourceNeutralOrdersQueryResult::Page(p) => {
                    SourceNeutralOrdersQueryResult::Page(SourceNeutralOrdersPage {
                        root_sequence: p.root_sequence,
                        items: p.items.iter().map(old_order).collect(),
                        next_cursor: p.next_cursor.clone(),
                    })
                }
                SourceNeutralOrdersQueryResult::InvalidCursor => {
                    SourceNeutralOrdersQueryResult::InvalidCursor
                }
                SourceNeutralOrdersQueryResult::OutOfRetention(x) => {
                    SourceNeutralOrdersQueryResult::OutOfRetention(x.clone())
                }
            },
        },
        SourceNeutralControlResult::TradesQuery { result } => LedgerControl::TradesQuery {
            result: result.clone(),
        },
    }
}
fn ledger_evidence(
    e: &SourceNeutralEvidenceKind<
        SourceNeutralTradeLifecycleResultV2<MemoryIdentity>,
        MemoryControl,
        MemoryIdentity,
    >,
) -> Option<LedgerEvidence<MemoryIdentity>> {
    Some(match e {
        SourceNeutralEvidenceKind::Control { result } => LedgerEvidence::Control {
            result: ledger_control(result),
        },
        SourceNeutralEvidenceKind::OrderLifecycle { result } => LedgerEvidence::OrderLifecycle {
            result: result.clone(),
        },
        SourceNeutralEvidenceKind::TradeLifecycle { result: r } => LedgerEvidence::TradeLifecycle {
            result: SourceNeutralTradeLifecycleResultV1 {
                trade_identity: r.trade_identity.clone(),
                order_identity: r.order_identity.clone(),
                condition_id: r.condition_id.clone(),
                token_id: r.token_id.clone(),
                state: r.state.clone(),
                economic_disposition: r.economic_disposition.clone(),
            },
        },
        SourceNeutralEvidenceKind::SettlementLifecycle { result } => {
            LedgerEvidence::SettlementLifecycle {
                result: result.clone(),
            }
        }
        _ => return None,
    })
}

/// 由运行已有后台worker持有；复用唯一交易段和原durable_record_seq。
pub struct LedgerBackgroundWriterV2 {
    root: std::path::PathBuf,
    writer: Option<DurableLedgerV1>,
}
impl LedgerBackgroundWriterV2 {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_owned(),
            writer: None,
        }
    }
    /// false 表示原NOE批尚未真实稳定，原材料必须保留且后续不能越过。
    pub fn write(
        &mut self,
        material: &LedgerRecordMaterialV2,
    ) -> Result<bool, LedgerDurableErrorV1> {
        match material.payload.as_ref() {
            DurablePayloadV1::Genesis { policy } => {
                self.writer = Some(DurableLedgerV1::create_v2(&self.root, policy)?);
            }
            DurablePayloadV1::Transaction(effect) => {
                let mut effect = (**effect).clone();
                if let LedgerTransactionSourceV1::Memory(record) = &effect.transaction.source {
                    let Some(reference) = record.durable_reference() else {
                        return Ok(false);
                    };
                    effect.transaction.source = LedgerTransactionSourceV1::NoeConsumed(reference);
                }
                self.writer
                    .as_mut()
                    .expect("绑定先交接Genesis")
                    .append_v2(&DurablePayloadV1::Transaction(Box::new(effect)))?;
            }
            payload => self
                .writer
                .as_mut()
                .expect("绑定先交接Genesis")
                .append_v2(payload)?,
        }
        Ok(true)
    }
    pub fn finish(&mut self) -> Result<(), LedgerDurableErrorV1> {
        if let Some(writer) = &mut self.writer {
            writer.seal_active_segment()?;
        }
        Ok(())
    }
}
