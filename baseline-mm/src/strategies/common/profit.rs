use super::*;
use ledger::*;
use shared_run::*;
use std::collections::BTreeMap;
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfitAllocationV2 {
    pub global_request_id: String,
    pub reserve_identity: LedgerRequestIdentityV1,
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub estimated_principal_pusd: ExactRationalV1,
    pub estimated_fee_pusd: ExactRationalV1,
    /// 只冻结决策场景，不能当作已成交数量。
    pub estimated_shares: ExactRationalV1,
    /// 原截止可配对孤腿场景；不能由后续别单池变化重估释放。
    pub original_orphan_shares: ExactRationalV1,
    pub original_orphan_principal_per_share: ExactRationalV1,
    #[serde(with = "sim_system::public_consumption::checkpoint::cutoff")]
    pub estimate_cutoff: sim_system::public_consumption::TrustedGroupDecisionCutoffV1,
    /// 首次分配的利润资源，不是当前预计损失或风险上界。
    pub allocated_profit_pusd: ExactRationalV1,
}

pub(crate) struct RemainingRepairEstimate {
    pub(crate) shares: R,
    pub(crate) principal: R,
    pub(crate) fee: R,
    pub(crate) loss: R,
    pub(crate) unknown: bool,
}

pub(crate) fn limit_leg(
    i: &ModelIntentV1,
    ordinal: u16,
) -> Option<(&ConditionId, &BuyLegIntentV1)> {
    match i {
        ModelIntentV1::SubmitSingleBuy {
            condition_id,
            order,
        } if ordinal == 0 => Some((condition_id, order)),
        ModelIntentV1::SubmitPairedBuy {
            condition_id,
            orders,
        } => orders.get(usize::from(ordinal)).map(|l| (condition_id, l)),
        _ => None,
    }
}

pub(crate) fn remaining_estimate(
    read: &ActionBeforeReadV2<
        impl std::borrow::Borrow<SharedExecutionRegistryV2>,
        impl ActionAccountReadV2,
    >,
    a: &ProfitAllocationV2,
) -> Result<Option<RemainingRepairEstimate>, StrategyErrorV1> {
    remaining_estimate_policy(read, a, false)
}

/// D-0378只由持续库存使用；旧V5保留原冻结策略。
pub(crate) fn remaining_inventory_estimate(
    read: &ActionBeforeReadV2<
        impl std::borrow::Borrow<SharedExecutionRegistryV2>,
        impl ActionAccountReadV2,
    >,
    a: &ProfitAllocationV2,
) -> Result<Option<RemainingRepairEstimate>, StrategyErrorV1> {
    remaining_estimate_policy(read, a, true)
}
fn remaining_estimate_policy(
    read: &ActionBeforeReadV2<
        impl std::borrow::Borrow<SharedExecutionRegistryV2>,
        impl ActionAccountReadV2,
    >,
    a: &ProfitAllocationV2,
    inventory_policy: bool,
) -> Result<Option<RemainingRepairEstimate>, StrategyErrorV1> {
    use contracts::execution_normalization::SourceNeutralTradeEconomicDispositionV1 as D;
    let original_q = rational(&a.estimated_shares)?;
    let original_p = rational(&a.estimated_principal_pusd)?;
    let original_f = rational(&a.estimated_fee_pusd)?;
    let held_q = rational(&a.original_orphan_shares)?;
    let held_unit = rational(&a.original_orphan_principal_per_share)?;
    let loss = |q: &R, principal: &R, fees: &R, success: &R| {
        let paired = q.clone().min(held_q.sub(success).max(R::zero()));
        let paired_principal = if q.is_positive() {
            principal.mul(&paired).divided_by(q)
        } else {
            R::zero()
        };
        held_unit
            .mul(&paired)
            .add(&paired_principal)
            .sub(&paired)
            .add(fees)
            .max(R::zero())
    };
    let ExecutionReadValueV1::Known(mut trades) =
        read.request_trade_economics(&a.global_request_id)
    else {
        return Ok(Some(RemainingRepairEstimate {
            shares: original_q.clone(),
            principal: original_p.clone(),
            fee: original_f.clone(),
            loss: if inventory_policy {
                rational(&a.allocated_profit_pusd)?
            } else {
                loss(&original_q, &original_p, &original_f, &R::zero())
            },
            unknown: true,
        }));
    };
    if inventory_policy {
        trades.retain(|t| t.fact.condition_id == a.condition_id && t.fact.token_id == a.token_id);
    }
    let active_local = read.request_funds().any(|f| {
        f.request_identity == &a.reserve_identity
            && !matches!(
                f.reservation_state,
                ReservationStatusV1::Converted | ReservationStatusV1::Released
            )
    });
    let commitments: Vec<_> = read
        .commitments()
        .filter(|c| {
            c.origin.intent.global_request_id() == a.global_request_id
                && (!inventory_policy
                    || limit_leg(c.origin.intent.intent(), c.origin.batch_item_ordinal)
                        .is_some_and(|(condition, leg)| {
                            *condition == a.condition_id && leg.token_id == a.token_id
                        }))
        })
        .collect();
    let mut success = R::zero();
    let mut pending_q = R::zero();
    let mut pending_p = R::zero();
    let mut pending_fee = R::zero();
    let mut pairing_q = R::zero();
    let mut pairing_p = R::zero();
    let mut unapplied_fee = R::zero();
    for t in &trades {
        let q = r(decimal(&t.fact.base_quantity_shares)?);
        let p = r(decimal(&t.fact.quote_amount_pusd)?);
        if t.success_applied {
            success = success.add(&q);
            continue;
        }
        let failed = matches!(
            t.fact.economic_disposition,
            D::FinalFailureUnapplied | D::FinalFailureCompensated
        );
        if failed && !t.economic_applied {
            continue;
        }
        let fee = r(decimal(if t.economic_applied {
            &t.actual_net_fee_pusd
        } else {
            &t.fact.immediate_taker_fee_pusd
        })?);
        pending_q = pending_q.add(&q);
        pending_p = pending_p.add(&p);
        pending_fee = pending_fee.add(&fee);
        if !failed {
            pairing_q = pairing_q.add(&q);
            pairing_p = pairing_p.add(&p);
            if !t.economic_applied {
                unapplied_fee = unapplied_fee.add(&fee);
            }
        }
    }
    let mut unused_ratio = R::zero();
    let mut unknown = false;
    if active_local {
        let matched = trades.iter().try_fold(R::zero(), |sum, t| {
            Ok::<_, StrategyErrorV1>(sum.add(&r(decimal(&t.fact.quote_amount_pusd)?)))
        })?;
        unused_ratio = original_p
            .sub(&matched)
            .max(R::zero())
            .divided_by(&original_p);
    } else {
        for c in commitments {
            let amount = limit_leg(c.origin.intent.intent(), c.origin.batch_item_ordinal)
                .is_some_and(|(_, l)| {
                    matches!(
                        l.order,
                        BuyOrderIntentV1::FakMarket { .. } | BuyOrderIntentV1::FokMarket { .. }
                    )
                });
            let related: Vec<_> = trades
                .iter()
                .filter(|t| Some(t.fact.order_identity.clone()).as_ref() == c.order_identity)
                .collect();
            let nonterminal = |t: &&TradeEconomicReadV1<'_>| {
                !matches!(
                    t.fact.economic_disposition,
                    D::FinalSuccess | D::FinalFailureUnapplied | D::FinalFailureCompensated
                )
            };
            let ratio =
                if amount {
                    let matched_unapplied = related
                        .iter()
                        .copied()
                        .filter(nonterminal)
                        .filter(|t| !t.economic_applied)
                        .try_fold(R::zero(), |s, t| {
                            Ok::<_, StrategyErrorV1>(s.add(&r(decimal(&t.fact.quote_amount_pusd)?)))
                        })?;
                    match c
                        .order_identity
                        .and_then(|id| read.order_funds().find(|f| f.order_identity == id))
                        .and_then(|f| known(&f.principal_encumbrance_pusd).cloned())
                    {
                        Some(p) => r(decimal(&p)?)
                            .sub(&matched_unapplied)
                            .max(R::zero())
                            .divided_by(&original_p),
                        None => {
                            unknown = true;
                            r(Decimal::ONE)
                        }
                    }
                } else {
                    let pending = related.iter().copied().filter(nonterminal).try_fold(
                        R::zero(),
                        |s, t| {
                            Ok::<_, StrategyErrorV1>(
                                s.add(&r(decimal(&t.fact.base_quantity_shares)?)),
                            )
                        },
                    )?;
                    match known(&c.remaining_target_shares) {
                        Some(q) => r(decimal(q)?)
                            .sub(&pending)
                            .max(R::zero())
                            .divided_by(&original_q),
                        None => {
                            unknown = true;
                            r(Decimal::ONE)
                        }
                    }
                };
            unused_ratio = unused_ratio.add(&ratio);
        }
    }
    let unused_ratio = unused_ratio.min(r(Decimal::ONE)).max(R::zero());
    let unused_q = original_q.mul(&unused_ratio);
    let unused_p = original_p.mul(&unused_ratio);
    let unused_f = original_f.mul(&unused_ratio);
    let shares = unused_q.add(&pending_q);
    let principal = unused_p.add(&pending_p);
    let fee = unused_f.add(&pending_fee);
    if !active_local && shares.is_zero() && principal.is_zero() && fee.is_zero() {
        return Ok(None);
    }
    let unresolved_q = unused_q.add(&pairing_q);
    let unresolved_p = unused_p.add(&pairing_p);
    let unresolved_f = unused_f.add(&unapplied_fee);
    let mut remaining_loss = loss(&unresolved_q, &unresolved_p, &unresolved_f, &success);
    if inventory_policy {
        // 当前H/P已吸收所属成功应用部分；这里仅使用未决部分，不重扣已实现损失。
        let actual_need = read.market_economics(&a.condition_id).and_then(|market| {
            let assets = market.assets.as_ref()?;
            let side = assets
                .iter()
                .position(|asset| asset.token_id == a.token_id)?;
            let held = r(market.orphan_shares[1 - side]);
            if held.is_zero() || !market.unresolved {
                return Some(R::zero());
            }
            // 已应用历史费用已由原refresh_pi计入P；已知性仍是原成本依赖。
            known(&market.orphan_fee)?;
            let paired = unresolved_q.clone().min(held.clone());
            let new_principal = if unresolved_q.is_positive() {
                unresolved_p.mul(&paired).divided_by(&unresolved_q)
            } else {
                R::zero()
            };
            Some(
                market
                    .orphan_principal
                    .mul(&paired)
                    .divided_by(&held)
                    .add(&new_principal)
                    .sub(&paired)
                    .add(&unresolved_f)
                    .max(R::zero()),
            )
        });
        if let Some(actual_need) = actual_need {
            // 未封顶实际需要与原冻结场景分别估计；外层才施加首次分配上限。
            remaining_loss = remaining_loss.max(actual_need);
        } else {
            unknown = true;
        }
        if unknown {
            remaining_loss = remaining_loss.max(rational(&a.allocated_profit_pusd)?);
        }
    }
    Ok(Some(RemainingRepairEstimate {
        shares,
        principal,
        fee,
        unknown,
        loss: remaining_loss,
    }))
}

pub(crate) fn remaining_profit_occupancy(
    read: &ActionBeforeReadV2<
        impl std::borrow::Borrow<SharedExecutionRegistryV2>,
        impl ActionAccountReadV2,
    >,
    allocations: &mut BTreeMap<String, ProfitAllocationV2>,
) -> Result<R, StrategyErrorV1> {
    remaining_profit_occupancy_policy(read, allocations, false)
}
pub(crate) fn remaining_inventory_profit_occupancy(
    read: &ActionBeforeReadV2<
        impl std::borrow::Borrow<SharedExecutionRegistryV2>,
        impl ActionAccountReadV2,
    >,
    allocations: &mut BTreeMap<String, ProfitAllocationV2>,
) -> Result<R, StrategyErrorV1> {
    remaining_profit_occupancy_policy(read, allocations, true)
}
fn remaining_profit_occupancy_policy(
    read: &ActionBeforeReadV2<
        impl std::borrow::Borrow<SharedExecutionRegistryV2>,
        impl ActionAccountReadV2,
    >,
    allocations: &mut BTreeMap<String, ProfitAllocationV2>,
    inventory_policy: bool,
) -> Result<R, StrategyErrorV1> {
    let mut total = R::zero();
    let mut closed = Vec::new();
    for (id, allocation) in allocations.iter() {
        if let Some(remaining) = remaining_estimate_policy(read, allocation, inventory_policy)? {
            total = total.add(&rational(&allocation.allocated_profit_pusd)?.min(remaining.loss));
        } else {
            closed.push(id.clone());
        }
    }
    for id in closed {
        allocations.remove(&id);
    }
    Ok(total)
}
