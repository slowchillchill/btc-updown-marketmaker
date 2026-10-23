use super::*;
use contracts::execution_normalization::SourceNeutralTradeEconomicDispositionV1 as D;

/// 每个原腿的互斥未履约部分。成功应用的份额只留在 H；失败量不进入 T。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ObligationDiagnosticV1 {
    pub request_id: Option<String>,
    pub ordinal: Option<u16>,
    pub order_identity: Option<::execution_normalization::MemoryLogicalIdentityV1>,
    pub trade_identity: Option<::execution_normalization::MemoryLogicalIdentityV1>,
    pub condition_id: ConditionId,
    pub token_id: Option<TokenId>,
    pub original_price: Option<ExactDecimal>,
    pub quantity: ExecutionReadValueV1<ExactDecimal>,
    pub cost: ExecutionReadValueV1<ExactRationalV1>,
    pub success_applied: bool,
    pub economic_applied: Option<bool>,
    pub trade_disposition: Option<D>,
    pub observed_noe_position: Option<::execution_normalization::NoeRecordPositionV1>,
    pub economic_application_account_transition_seq: Option<u64>,
}
#[derive(Clone)]
pub(super) struct Obligation {
    pub diagnostic: ObligationDiagnosticV1,
    pub quantity: Decimal,
    pub cost: R,
    pub pending_match: bool,
    pub unknown: bool,
}
#[derive(Clone)]
pub(super) struct InventoryState {
    pub h: [Decimal; 2],
    pub t: [Decimal; 2],
    pub principal: R,
    pub fee: R,
    pub exposure: [R; 2],
    pub investment: R,
    pub funds: R,
    pub obligations: Vec<Obligation>,
    pub unknown: bool,
}
impl InventoryState {
    pub fn market_exposure(&self) -> R {
        self.exposure[0].clone().max(self.exposure[1].clone())
    }
    pub fn active(&self) -> bool {
        self.h.iter().chain(&self.t).any(|q| *q > Decimal::ZERO)
            || !self.obligations.is_empty()
            || self.unknown
    }
    pub fn coverage(&self, token: &TokenId, price: Decimal) -> Decimal {
        self.obligations
            .iter()
            .filter(|o| {
                o.diagnostic.token_id.as_ref() == Some(token)
                    && o.diagnostic
                        .original_price
                        .as_ref()
                        .is_some_and(|p| decimal(p).ok() == Some(price))
            })
            .map(|o| o.quantity)
            .sum()
    }
    pub fn old_quantity(&self, token: &TokenId) -> Decimal {
        self.obligations
            .iter()
            .filter(|o| o.diagnostic.token_id.as_ref() == Some(token))
            .map(|o| o.quantity)
            .sum()
    }
}

pub(super) fn inventory(
    read: &ActionBeforeReadV2<
        impl std::borrow::Borrow<SharedExecutionRegistryV2>,
        impl ActionAccountReadV2,
    >,
    condition: &ConditionId,
    tokens: &[TokenId; 2],
) -> Result<InventoryState, StrategyErrorV1> {
    let market = read.market_economics(condition);
    let mut state = InventoryState {
        h: [Decimal::ZERO; 2],
        t: [Decimal::ZERO; 2],
        principal: R::zero(),
        fee: R::zero(),
        exposure: [R::zero(), R::zero()],
        investment: R::zero(),
        funds: R::zero(),
        obligations: vec![],
        unknown: false,
    };
    if let Some(m) = &market {
        state.investment = m.successful_investment.clone();
        if m.unresolved {
            if let Some(assets) = &m.assets {
                for side in 0..2 {
                    if let Some(i) = assets.iter().position(|a| a.token_id == tokens[side]) {
                        state.h[side] = m.orphan_shares[i];
                    } else {
                        state.unknown = true;
                    }
                }
            } else if m.orphan_shares.iter().any(|q| *q > Decimal::ZERO) {
                state.unknown = true;
            }
            state.principal = m.orphan_principal.clone();
            if let Some(fee) = known(&m.orphan_fee) {
                state.fee = fee.clone();
            } else {
                state.unknown |= state.h.iter().any(|q| *q > Decimal::ZERO);
            }
        }
    }
    let orders: BTreeMap<_, _> = read
        .obligation_orders()
        .filter(|o| o.condition_id == Some(condition))
        .map(|o| (o.order_identity.clone(), o))
        .collect();
    let mut origins = BTreeMap::new();
    for order in orders.values() {
        if let Some(origin) = &order.origin {
            origins.insert(
                (
                    origin.intent.global_request_id().to_owned(),
                    origin.batch_item_ordinal,
                ),
                (origin.clone(), Some(order.order_identity)),
            );
        } else {
            state.unknown = true;
            state.obligations.push(Obligation {
                diagnostic: ObligationDiagnosticV1 {
                    request_id: None,
                    ordinal: None,
                    order_identity: Some(order.order_identity.clone()),
                    trade_identity: None,
                    condition_id: condition.clone(),
                    token_id: order.token_id.cloned(),
                    original_price: None,
                    quantity: order.active_unfilled_shares.clone(),
                    cost: ExecutionReadValueV1::Unavailable,
                    success_applied: false,
                    economic_applied: None,
                    trade_disposition: None,
                    observed_noe_position: None,
                    economic_application_account_transition_seq: None,
                },
                quantity: Decimal::ZERO,
                cost: R::zero(),
                unknown: true,
                pending_match: false,
            });
        }
    }
    for commitment in read.commitments() {
        if limit_leg(
            commitment.origin.intent.intent(),
            commitment.origin.batch_item_ordinal,
        )
        .is_some_and(|(id, _)| id == condition)
        {
            origins
                .entry((
                    commitment.origin.intent.global_request_id().to_owned(),
                    commitment.origin.batch_item_ordinal,
                ))
                .or_insert((commitment.origin, commitment.order_identity));
        }
    }
    for ((request, ordinal), (origin, order_id)) in origins {
        let Some((_, leg)) = limit_leg(origin.intent.intent(), ordinal) else {
            state.unknown = true;
            continue;
        };
        let order = order_id.and_then(|id| orders.get(id));
        let (price, target, post_only) = match leg.order {
            BuyOrderIntentV1::GtcLimit {
                limit_price_pusd_per_share,
                size_shares,
                post_only,
            }
            | BuyOrderIntentV1::GtdLimit {
                limit_price_pusd_per_share,
                size_shares,
                post_only,
                ..
            } => (
                Some(limit_price_pusd_per_share),
                Some(size_shares),
                post_only,
            ),
            _ => (None, None, false),
        };
        let remaining = if let Some(order) = order {
            match &order.active_unfilled_shares {
                ExecutionReadValueV1::Known(q) => Some(decimal(q)?),
                ExecutionReadValueV1::NotApplicable => Some(Decimal::ZERO),
                _ => None,
            }
        } else {
            target
        };
        let unknown = remaining.is_none() || price.is_none();
        let q = remaining.unwrap_or(Decimal::ZERO);
        if q > Decimal::ZERO || unknown {
            let principal = price.map_or_else(R::zero, |p| r(p).mul(&r(q)));
            let fee = if post_only {
                Some(R::zero())
            } else {
                order_id
                    .and_then(|id| read.order_funds().find(|f| f.order_identity == id))
                    .and_then(|f| known(&f.immediate_fee_encumbrance_pusd).cloned())
                    .map(|f| decimal(&f).map(r))
                    .transpose()?
            };
            let unknown = unknown || fee.is_none();
            let cost = principal.add(&fee.unwrap_or_default());
            state.obligations.push(Obligation {
                diagnostic: ObligationDiagnosticV1 {
                    request_id: Some(request),
                    ordinal: Some(ordinal),
                    order_identity: order_id.cloned(),
                    trade_identity: None,
                    condition_id: condition.clone(),
                    token_id: Some(leg.token_id.clone()),
                    original_price: price.map(exact),
                    quantity: remaining
                        .map(exact)
                        .map(ExecutionReadValueV1::Known)
                        .unwrap_or(ExecutionReadValueV1::Unavailable),
                    cost: if unknown {
                        ExecutionReadValueV1::Unavailable
                    } else {
                        ExecutionReadValueV1::Known(cost.public())
                    },
                    success_applied: false,
                    economic_applied: None,
                    trade_disposition: None,
                    observed_noe_position: None,
                    economic_application_account_transition_seq: None,
                },
                quantity: q,
                cost,
                unknown,
                pending_match: false,
            });
        }
    }
    for trade in read
        .obligation_trades()
        .iter()
        .filter(|t| t.fact().condition_id == *condition)
    {
        let fact = trade.fact();
        if trade.success_applied() {
            continue;
        }
        let failed = matches!(
            fact.economic_disposition,
            D::FinalFailureUnapplied | D::FinalFailureCompensated
        );
        let applied = trade.ledger.as_ref().is_some_and(|t| t.economic_applied);
        if failed && !applied {
            continue;
        }
        let order = orders.get(&fact.order_identity);
        let origin = order.and_then(|o| o.origin.as_ref());
        let original_price = origin
            .and_then(|o| limit_leg(o.intent.intent(), o.batch_item_ordinal))
            .and_then(|(_, l)| match l.order {
                BuyOrderIntentV1::GtcLimit {
                    limit_price_pusd_per_share,
                    ..
                }
                | BuyOrderIntentV1::GtdLimit {
                    limit_price_pusd_per_share,
                    ..
                } => Some(exact(limit_price_pusd_per_share)),
                _ => None,
            });
        let fee = if applied {
            &trade
                .ledger
                .as_ref()
                .expect("原应用状态")
                .actual_net_fee_pusd
        } else {
            &fact.immediate_taker_fee_pusd
        };
        let cost = r(decimal(&fact.quote_amount_pusd)?).add(&r(decimal(fee)?));
        let q = if failed {
            Decimal::ZERO
        } else {
            decimal(&fact.base_quantity_shares)?
        };
        state.obligations.push(Obligation {
            diagnostic: ObligationDiagnosticV1 {
                request_id: origin.map(|o| o.intent.global_request_id().to_owned()),
                ordinal: origin.map(|o| o.batch_item_ordinal),
                order_identity: Some(fact.order_identity.clone()),
                trade_identity: Some(fact.trade_identity.clone()),
                condition_id: condition.clone(),
                token_id: Some(fact.token_id.clone()),
                original_price,
                quantity: ExecutionReadValueV1::Known(exact(q)),
                cost: ExecutionReadValueV1::Known(cost.public()),
                success_applied: false,
                economic_applied: Some(applied),
                trade_disposition: Some(fact.economic_disposition.clone()),
                observed_noe_position: trade.observed.as_ref().map(|r| r.position()),
                economic_application_account_transition_seq: fact
                    .economic_application_account_transition_seq,
            },
            quantity: q,
            cost,
            unknown: origin.is_none(),
            pending_match: !failed,
        });
    }
    let pending_cost = state
        .obligations
        .iter()
        .fold(R::zero(), |sum, o| sum.add(&o.cost));
    let funds = read.market_funds(condition);
    state.funds = funds.total();
    let additional = funds.total().sub(&pending_cost).max(R::zero());
    state.investment = state
        .investment
        .add(&pending_cost.clone().max(funds.total()));
    state.unknown |= state.obligations.iter().any(|o| o.unknown)
        || (state.obligations.is_empty() && funds.has_obligation())
        || (additional.is_positive()
            && (funds.local_principal > Decimal::ZERO || funds.local_fee > Decimal::ZERO));
    for side in 0..2 {
        state.exposure[side] = if state.h[side] > Decimal::ZERO {
            state.principal.add(&state.fee)
        } else {
            R::zero()
        };
        state.exposure[side] = state.exposure[side].add(&additional);
        for obligation in &state.obligations {
            if obligation.diagnostic.token_id.as_ref() == Some(&tokens[side]) {
                state.exposure[side] = state.exposure[side].add(&obligation.cost);
                if obligation.pending_match {
                    state.t[side] += obligation.quantity;
                }
            }
        }
    }
    Ok(state)
}
