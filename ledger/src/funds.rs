//! 由原提交分录派生的订单归属；不另记资金事实。
use super::*;
use contracts::ExecutionReadValueV1;
use contracts::execution_normalization::{
    SourceNeutralAccountTransitionKindV1 as Transition, SourceNeutralEconomicDispositionV1,
};
use execution_normalization::{
    MemoryLogicalIdentityV1, NormalizationRecord, SourceNeutralEvidenceKind,
};

type Identity = MemoryLogicalIdentityV1;
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(super) struct OrderFundsStateV1 {
    condition: Option<ConditionId>,
    principal: Decimal,
    fee: Decimal,
    has_origin: bool,
    last_seq: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct UnassignedFundsV1 {
    condition: Option<ConditionId>,
    principal: Decimal,
    fee: Decimal,
    seq: u64,
}
#[derive(Default, Serialize, Deserialize)]
pub(super) struct FundsStateV1 {
    orders: BTreeMap<Identity, OrderFundsStateV1>,
    trades: BTreeMap<Identity, Identity>,
    unassigned: BTreeMap<Identity, Vec<UnassignedFundsV1>>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(super) struct FundsPatchV1 {
    orders: Vec<(Identity, OrderFundsStateV1)>,
    trade: Option<(Identity, Identity)>,
    unassigned: Option<(Identity, Vec<UnassignedFundsV1>)>,
}
impl FundsStateV1 {
    pub(super) fn prepare(
        &self,
        effect: &LedgerCommittedEffectV1,
    ) -> Result<Option<FundsPatchV1>, LedgerErrorV1> {
        match &effect.transaction.source {
            LedgerTransactionSourceV1::Memory(source) => self.prepare_record(effect, source),
            LedgerTransactionSourceV1::MemoryV2(source) => self.prepare_record(effect, source),
            _ => Ok(None),
        }
    }
    fn prepare_record<R: Clone + std::fmt::Debug + Eq>(
        &self,
        effect: &LedgerCommittedEffectV1,
        source: &execution_normalization::NoeConsumedRecordRef<R>,
    ) -> Result<Option<FundsPatchV1>, LedgerErrorV1> {
        let seq = effect.ledger_entry_seq();
        let mut patch = FundsPatchV1::default();
        let mut row = None;
        let mut origin = false;
        let mut condition = None;
        match source.record() {
            NormalizationRecord::EconomicFact { fact,.. } => if let SourceNeutralEconomicDispositionV1::AccountTransition {transition,condition_id,..}=&fact.disposition {
                condition=condition_id.clone();
                match transition {
                    Transition::OrderAcceptedEncumbranceApplied {order_identity} => {row=Some(order_identity.clone());origin=true;}
                    Transition::FakUnspentEncumbranceReleased {order_identity} | Transition::CallerCancellationEncumbranceReleased {order_identity} | Transition::GtdExpirationEncumbranceReleased {order_identity,..} | Transition::VenueDustEncumbranceReleased {order_identity} | Transition::MarketResolutionEncumbranceReleased {order_identity,..} => row=Some(order_identity.clone()),
                    Transition::FillFinalityEconomicApplied {trade_identity,..} | Transition::FillFinalityUnappliedFailureReleased {trade_identity,..} | Transition::FillFinalityCompensationApplied {trade_identity,..} => {
                        row=self.trades.get(trade_identity).cloned();
                        if row.is_none() {
                            let (principal,fee)=entry_amounts(&effect.transaction.entries)?;
                            let mut pending=self.unassigned.get(trade_identity).cloned().unwrap_or_default();
                            pending.push(UnassignedFundsV1 {condition:condition.clone(),principal,fee,seq});
                            patch.unassigned=Some((trade_identity.clone(),pending));
                        }
                    }
                    _=>{}
                }
            },
            NormalizationRecord::Evidence {evidence,..} => match evidence {
                SourceNeutralEvidenceKind::TradeLifecycle {result} => {
                    patch.trade=Some((result.trade_identity.clone(),result.order_identity.clone()));
                    row=Some(result.order_identity.clone()); condition=Some(result.condition_id.clone());
                    patch.unassigned=Some((result.trade_identity.clone(),Vec::new()));
                }
                SourceNeutralEvidenceKind::TradeLifecycleV3 {result} => {
                    let result = &result.result;
                    patch.trade=Some((result.trade_identity.clone(),result.order_identity.clone()));
                    row=Some(result.order_identity.clone()); condition=Some(result.condition_id.clone());
                    patch.unassigned=Some((result.trade_identity.clone(),Vec::new()));
                }
                SourceNeutralEvidenceKind::OrderLifecycle {result} => {row=Some(result.order_identity.clone());condition=Some(result.condition_id.clone());}
                SourceNeutralEvidenceKind::Control {result: contracts::execution_normalization::SourceNeutralControlResult::Submission {outcome: contracts::execution_normalization::SourceNeutralSubmissionOutcomeV2::Accepted {order,..}}} => {row=Some(order.order_identity.clone());condition=Some(order.condition_id.clone());}
                _=>{}
            },
            _=>{}
        }
        if let Some(id) = row {
            let mut state = self.orders.get(&id).cloned().unwrap_or_default();
            state.condition = condition.or(state.condition);
            state.has_origin |= origin;
            let (principal, fee) = entry_amounts(&effect.transaction.entries)?;
            add_amounts(&mut state, principal, fee, seq)?;
            if let Some((trade, _)) = &patch.trade
                && let Some(pending) = self.unassigned.get(trade)
            {
                for delta in pending {
                    add_amounts(&mut state, delta.principal, delta.fee, delta.seq)?;
                }
            }
            if origin {
                state.last_seq = Some(seq);
            }
            patch.orders.push((id, state));
        }
        Ok(Some(patch))
    }
    pub(super) fn commit(&mut self, patch: FundsPatchV1) {
        self.orders.extend(patch.orders);
        if let Some((trade, order)) = patch.trade {
            self.trades.insert(trade, order);
        }
        if let Some((trade, pending)) = patch.unassigned {
            if pending.is_empty() {
                self.unassigned.remove(&trade);
            } else {
                self.unassigned.insert(trade, pending);
            }
        }
    }
}
fn entry_amounts(entries: &[LedgerEntryV1]) -> Result<(Decimal, Decimal), LedgerErrorV1> {
    let mut principal = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    for (account, _, amount) in entries {
        match account {
            LedgerAccountV1::PrincipalEncumbrance(_) => {
                principal = checked_add(principal, *amount, "order principal")?
            }
            LedgerAccountV1::ImmediateTakerFeeEncumbrance(_) => {
                fee = checked_add(fee, *amount, "order fee")?
            }
            _ => {}
        }
    }
    Ok((principal, fee))
}
fn add_amounts(
    state: &mut OrderFundsStateV1,
    principal: Decimal,
    fee: Decimal,
    seq: u64,
) -> Result<(), LedgerErrorV1> {
    state.principal = checked_add(state.principal, principal, "order principal")?;
    state.fee = checked_add(state.fee, fee, "order fee")?;
    if !principal.is_zero() || !fee.is_zero() {
        state.last_seq = Some(state.last_seq.unwrap_or_default().max(seq));
    }
    Ok(())
}
#[derive(Debug)]
pub struct RequestFundsReadV1<'a> {
    pub request_identity: &'a LedgerRequestIdentityV1,
    pub condition_id: &'a ConditionId,
    pub reservation_state: ReservationStatusV1,
    pub principal_pusd: ExactDecimal,
    pub immediate_fee_pusd: ExactDecimal,
    pub last_ledger_entry_seq: u64,
}
#[derive(Debug)]
pub struct OrderFundsReadV1<'a> {
    pub order_identity: &'a Identity,
    pub principal_encumbrance_pusd: ExecutionReadValueV1<ExactDecimal>,
    pub immediate_fee_encumbrance_pusd: ExecutionReadValueV1<ExactDecimal>,
    pub last_ledger_entry_seq: Option<u64>,
}
pub struct LedgerFundsReadV1<'a> {
    writer: &'a LedgerWriterV1,
}
impl LedgerWriterV1 {
    pub fn funds_read_v1(&self) -> LedgerFundsReadV1<'_> {
        LedgerFundsReadV1 { writer: self }
    }
}
impl LedgerFundsReadV1<'_> {
    pub fn requests(&self) -> impl Iterator<Item = RequestFundsReadV1<'_>> {
        self.writer
            .reservations
            .requests
            .keys()
            .filter_map(|id| self.request(id))
    }
    pub fn orders(&self) -> impl Iterator<Item = OrderFundsReadV1<'_>> {
        self.writer
            .funds
            .orders
            .keys()
            .filter_map(|id| self.order(id))
    }
    pub fn projection(&self) -> Option<crate::LedgerProjectionV2> {
        self.writer.projection_v2()
    }
    pub fn request(&self, id: &LedgerRequestIdentityV1) -> Option<RequestFundsReadV1<'_>> {
        let (id, cached) = self.writer.reservations.requests.get_key_value(id)?;
        let r = cached.reservation.as_ref()?;
        let released = matches!(
            r.status,
            ReservationStatusV1::Converted | ReservationStatusV1::Released
        );
        Some(RequestFundsReadV1 {
            request_identity: id,
            condition_id: &r.condition_id,
            reservation_state: r.status,
            principal_pusd: exact(if released {
                Decimal::ZERO
            } else {
                r.principal_pusd
            }),
            immediate_fee_pusd: exact(if released {
                Decimal::ZERO
            } else {
                r.immediate_fee_pusd
            }),
            last_ledger_entry_seq: r.last_applied_ledger_entry_seq,
        })
    }
    pub fn submission_request(
        &self,
        item: &LedgerSubmissionItemIdentityV1,
    ) -> Option<&LedgerRequestIdentityV1> {
        self.writer.lifecycle.submission_requests.get(item)
    }
    pub fn order(&self, id: &Identity) -> Option<OrderFundsReadV1<'_>> {
        let (id, state) = self.writer.funds.orders.get_key_value(id)?;
        let unknown = !state.has_origin
            || self
                .writer
                .funds
                .unassigned
                .values()
                .flatten()
                .any(|delta| delta.condition.is_none() || delta.condition == state.condition);
        let amount = |v| {
            if unknown {
                ExecutionReadValueV1::Unavailable
            } else {
                ExecutionReadValueV1::Known(exact(v))
            }
        };
        Some(OrderFundsReadV1 {
            order_identity: id,
            principal_encumbrance_pusd: amount(state.principal),
            immediate_fee_encumbrance_pusd: amount(state.fee),
            last_ledger_entry_seq: state.last_seq,
        })
    }
}
