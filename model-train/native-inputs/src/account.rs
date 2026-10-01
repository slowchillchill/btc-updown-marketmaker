//! §3 的完整415槽，所有范围先在精确域归约，最后一次投影。
use crate::{InputError, exact::*, root::MarketBinding, values::*};
use contracts::ExecutionReadValueV1::{Known, NotApplicable, Unavailable};
use contracts::{economics::EconomicRational as R, *};
use ledger::{
    EconomicSettlementStateV1 as S, LedgerMarketProjectionV1 as L, MarketEconomicTotalsReadV1 as M,
};
use shared_run::ActionBeforeReadV2;
use std::collections::BTreeMap;
fn status(s: S) -> usize {
    match s {
        S::Unresolved => 0,
        S::ResolvedNoCurrentPayout => 1,
        S::ReceivableNotRequested => 2,
        S::RequestPending => 3,
        S::Redeemed => 4,
        S::RequestFailedNoEffect => 5,
    }
}
fn esum(
    read: &shared_run::AccountEconomicsReadV2,
    scope: &[&ConditionId],
    f: impl Fn(&M) -> Value,
) -> Value {
    sum(scope.iter().map(|id| {
        read.markets
            .iter()
            .find(|m| &m.condition_id == *id)
            .map(&f)
            .unwrap_or_else(|| Known(R::zero()))
    }))
}
fn lsum(read: &ActionBeforeReadV2, scope: &[&ConditionId], f: impl Fn(&L) -> Value) -> Value {
    sum(scope.iter().map(|id| {
        read.ledger()
            .market_projections
            .iter()
            .find(|m| &m.condition_id == *id)
            .map(&f)
            .unwrap_or_else(|| Known(R::zero()))
    }))
}
fn econ_leg(m: &M, side: usize) -> Option<usize> {
    m.assets
        .as_ref()?
        .iter()
        .position(|a| a.outcome.index == side as u32)
}
pub(crate) fn encode(
    read: &ActionBeforeReadV2,
    current: Option<&ConditionId>,
    old: &[ConditionId],
    markets: &BTreeMap<String, MarketBinding>,
    now: i64,
    projection: &InputProjection,
) -> Result<[f32; 433], InputError> {
    let mut out = [0.; 433];
    let scopes = [
        current.into_iter().collect::<Vec<_>>(),
        old.iter().collect::<Vec<_>>(),
    ];
    for (group, scope) in scopes.iter().enumerate() {
        let available = group != 0 || current.is_some();
        let wrap = |v| if available { v } else { NotApplicable };
        let fields: [fn(&L) -> Value; 9] = [
            |m| d(&m.local_before_send_principal_pusd),
            |m| d(&m.local_before_send_immediate_fee_pusd),
            |m| Known(integer(m.local_before_send_count)),
            |m| d(&m.local_after_send_unknown_principal_pusd),
            |m| d(&m.local_after_send_unknown_immediate_fee_pusd),
            |m| Known(integer(m.local_after_send_unknown_count)),
            |m| d(&m.platform_confirmation_pending_economic_principal_pusd),
            |m| d(&m.platform_confirmation_pending_economic_immediate_fee_pusd),
            |m| Known(integer(m.platform_confirmation_pending_economic_count)),
        ];
        for (i, f) in fields.iter().enumerate() {
            slot(
                projection,
                &mut out,
                group * 27 + i * 3,
                wrap(lsum(read, scope, f)),
            );
        }
        for side_index in 0..2 {
            for cost in [false, true] {
                let amount = lsum(read, scope, |m| {
                    let Some(binding) = markets.get(&m.condition_id.0) else {
                        return Unavailable;
                    };
                    let Some(asset) = binding
                        .assets
                        .iter()
                        .find(|a| a.outcome.index == side_index as u32)
                    else {
                        return Unavailable;
                    };
                    m.outcome_balances
                        .iter()
                        .find(|b| b.token_id == asset.token_id)
                        .map(|b| {
                            d(if cost {
                                &b.position_cost_basis_pusd
                            } else {
                                &b.balance_shares
                            })
                        })
                        .unwrap_or_else(|| Known(R::zero()))
                });
                slot(
                    projection,
                    &mut out,
                    54 + group * 12 + usize::from(cost) * 6 + side_index * 3,
                    wrap(amount),
                );
            }
            let commitment = sum(read.commitments().filter_map(|c| {
                let (condition, leg) = leg(&c.origin).ok()?;
                if !scope.contains(&condition) {
                    return None;
                }
                match markets
                    .get(&condition.0)
                    .and_then(|m| side(&m.assets, &leg.token_id))
                {
                    Some(s) if s == side_index => {
                        Some(crate::values::read(&c.remaining_target_shares))
                    }
                    Some(_) => None,
                    None => Some(Unavailable),
                }
            }));
            slot(
                projection,
                &mut out,
                98 + group * 6 + side_index * 3,
                wrap(commitment.clone()),
            );
            for cost in [false, true] {
                let value = esum(read.economics(), scope, |m| {
                    econ_leg(m, side_index)
                        .map(|s| {
                            if cost {
                                q(&m.success_principal_cost_pusd[s])
                            } else {
                                d(&m.success_shares[s])
                            }
                        })
                        .unwrap_or(Unavailable)
                });
                slot(
                    projection,
                    &mut out,
                    122 + usize::from(cost) * 12 + group * 6 + side_index * 3,
                    wrap(value),
                );
            }
            slot(
                projection,
                &mut out,
                149 + group * 9 + side_index * 3,
                wrap(esum(read.economics(), scope, |m| {
                    econ_leg(m, side_index)
                        .map(|s| d(&m.orphan_shares[s]))
                        .unwrap_or(Unavailable)
                })),
            );
            if group == 0 {
                let gap = if read.economics().markets.iter().any(|m| {
                    scope.contains(&&m.condition_id) && m.settlement_state != S::Unresolved
                }) {
                    NotApplicable
                } else {
                    esum(read.economics(), scope, |m| {
                        econ_leg(m, side_index)
                            .map(|s| d(&m.repair_gap_shares[s]))
                            .unwrap_or(Unavailable)
                    })
                };
                let cap = match (&gap, commitment) {
                    (Known(g), Known(c)) => Known(g.sub(&c).max(R::zero())),
                    (NotApplicable, _) => NotApplicable,
                    _ => Unavailable,
                };
                slot(projection, &mut out, 164 + side_index * 3, wrap(gap));
                slot(projection, &mut out, 170 + side_index * 3, wrap(cap));
            }
        }
        slot(
            projection,
            &mut out,
            81 + group * 6,
            wrap(lsum(read, scope, |m| Known(integer(m.open_order_count)))),
        );
        slot(
            projection,
            &mut out,
            84 + group * 6,
            wrap(lsum(read, scope, |m| Known(integer(m.pending_fill_count)))),
        );
        slot(
            projection,
            &mut out,
            146 + group * 9,
            wrap(paired_shares(read.economics(), scope)),
        );
        for role_index in 0..2 {
            let base = 176 + group * 18 + role_index * 9;
            slot(
                projection,
                &mut out,
                base,
                wrap(esum(read.economics(), scope, |m| {
                    d(&m.executed[role_index].shares)
                })),
            );
            slot(
                projection,
                &mut out,
                base + 3,
                wrap(esum(read.economics(), scope, |m| {
                    d(&m.executed[role_index].principal_pusd)
                })),
            );
            slot(
                projection,
                &mut out,
                base + 6,
                wrap(esum(read.economics(), scope, |m| {
                    d(&m.executed[role_index].actual_fee_pusd)
                })),
            );
            encode_rebates(
                &mut out,
                212 + group * 36 + role_index * 18,
                read.economics(),
                scope,
                role_index,
                available,
                projection,
            );
        }
        let values: [fn(&M) -> Value; 5] = [
            |m| d(&m.guaranteed_position_value_pusd),
            |m| d(&m.paired_resolution_receivable_pusd),
            |m| d(&m.winner_orphan_receivable_pusd),
            |m| d(&m.unpaid_taker_fee_pusd),
            |m| d(&m.unpaid_settlement_fee_pusd),
        ];
        for (i, f) in values.iter().enumerate() {
            slot(
                projection,
                &mut out,
                284 + group * 15 + i * 3,
                wrap(esum(read.economics(), scope, f)),
            );
        }
        let values: [fn(&M) -> Value; 4] = [
            |m| q(&m.confirmed_net_pnl_pusd),
            |m| q(&m.confirmed_net_loss_pusd),
            |m| q(&m.maximum_confirmed_net_loss_pusd),
            |m| q(&m.unresolved_orphan_principal_cost_pusd),
        ];
        for (i, f) in values.iter().enumerate() {
            slot(
                projection,
                &mut out,
                332 + group * 12 + i * 3,
                wrap(esum(read.economics(), scope, f)),
            );
        }
        // action_snapshot 已保留完整未闭合人口，直接消费，不能再次按可撤资格过滤。
        let mut orders = Vec::new();
        let mut unknown_scope = false;
        for order in read.orders() {
            let Some(condition) = order.condition_id else {
                unknown_scope = true;
                continue;
            };
            if scope.contains(&condition) {
                orders.push(order)
            }
        }
        let ages = order_ages(&orders, now);
        slot(
            projection,
            &mut out,
            382 + group * 12,
            wrap(ages[0].clone()),
        );
        slot(
            projection,
            &mut out,
            385 + group * 12,
            wrap(ages[1].clone()),
        );
        let active = sum(orders.iter().map(|o| match &o.active_unfilled_shares {
            NotApplicable => Known(R::zero()),
            v => crate::values::read(v),
        }));
        slot(projection, &mut out, 388 + group * 12, wrap(active));
        let cancellable = sum(orders.iter().map(|o| match o.cancellable {
            Known(v) => Known(integer(u8::from(v))),
            _ => Unavailable,
        }));
        slot(projection, &mut out, 391 + group * 12, wrap(cancellable));
        slot(
            projection,
            &mut out,
            406 + group * 3,
            wrap(Known(integer(orders.len()))),
        );
        if unknown_scope && !scope.is_empty() {
            for offset in [382, 385, 388, 391] {
                slot(projection, &mut out, offset + group * 12, Unavailable);
            }
            slot(projection, &mut out, 406 + group * 3, Unavailable);
        }
    }
    let mut balances = [R::zero(), R::zero()];
    if let Some(c) = current {
        let binding = markets.get(&c.0).ok_or(InputError::MissingAssociation)?;
        if let Some(m) = read
            .ledger()
            .market_projections
            .iter()
            .find(|m| &m.condition_id == c)
        {
            for balance in &m.outcome_balances {
                let s = side(&binding.assets, &balance.token_id)
                    .ok_or(InputError::MissingAssociation)?;
                balances[s] = balances[s].add(&decimal(&balance.balance_shares));
            }
        }
        slot(
            projection,
            &mut out,
            78,
            Known(balances[0].sub(&balances[1])),
        );
        slot(
            projection,
            &mut out,
            95,
            lsum(read, &[c], |m| d(&m.current_all_in_commitment_pusd)),
        );
        let state = read
            .economics()
            .markets
            .iter()
            .find(|m| &m.condition_id == c)
            .map(|m| m.settlement_state)
            .unwrap_or(S::Unresolved);
        out[356 + status(state)] = 1.;
        out[362] = 1.;
        out[363] = 1.;
    }
    let cash = &read.economics().cash;
    for (i, v) in [
        &cash.actual_available_pusd,
        &cash.actual_reserved_pusd,
        &cash.maker_available_pusd,
        &cash.maker_reserved_pusd,
    ]
    .iter()
    .enumerate()
    {
        slot(projection, &mut out, 110 + i * 3, d(v));
    }
    let a = read.economics();
    for (i, v) in [
        qr(&a.conservative_nav_pusd),
        q(&a.worst_case_maker_nav_pusd),
        qr(&a.directional_residual_pusd),
        q(&a.other_account_residual_pusd),
        q(&a.other_receivable_pusd),
        q(&a.other_liability_pusd),
    ]
    .iter()
    .enumerate()
    {
        slot(projection, &mut out, 314 + i * 3, v.clone());
    }
    for i in 0..6 {
        slot(
            projection,
            &mut out,
            364 + i * 3,
            Known(integer(
                old.iter()
                    .filter(|id| {
                        status(
                            a.markets
                                .iter()
                                .find(|m| &m.condition_id == *id)
                                .map(|m| m.settlement_state)
                                .unwrap_or(S::Unresolved),
                        ) == i
                    })
                    .count(),
            )),
        );
    }
    slot(
        projection,
        &mut out,
        412,
        Known(integer(read.pending_noe_record_count())),
    );
    request_block(
        &mut out,
        415,
        read.cancellation_requests(None),
        now,
        projection,
    );
    Ok(out)
}

pub(crate) fn encode_rebates(
    out: &mut [f32],
    base: usize,
    read: &shared_run::AccountEconomicsReadV2,
    scope: &[&ConditionId],
    role: usize,
    applicable: bool,
    projection: &InputProjection,
) {
    for index in 0..6 {
        let value = esum(read, scope, |m| {
            let b = &m.rebates[role];
            match index {
                0 => match &b.estimated_pusd {
                    Known(v) => q(v),
                    Unavailable => Unavailable,
                    NotApplicable => NotApplicable,
                },
                1 => q(&b.accrued_unmatured_pusd),
                2 => q(&b.below_threshold_pusd),
                3 => q(&b.confirmed_unpaid_pusd),
                4 => q(&b.paid_pusd),
                _ => q(&b.locked_remainder_pusd),
            }
        });
        slot(
            projection,
            out,
            base + index * 3,
            if applicable { value } else { NotApplicable },
        );
    }
}

pub(crate) fn order_ages(orders: &[shared_run::ExecutionOrderReadV2<'_>], now: i64) -> [Value; 2] {
    let ages: Option<Vec<_>> = orders
        .iter()
        .map(|o| {
            times(o.formed).map(|(source, _)| ratio(i128::from(now) - i128::from(source), 1000))
        })
        .collect();
    if orders.is_empty() {
        [NotApplicable, NotApplicable]
    } else if let Some(ages) = ages {
        [
            Known(ages.iter().max().unwrap().clone()),
            Known(ages.iter().min().unwrap().clone()),
        ]
    } else {
        [Unavailable, Unavailable]
    }
}

pub(crate) fn paired_shares(
    read: &shared_run::AccountEconomicsReadV2,
    scope: &[&ConditionId],
) -> Value {
    esum(read, scope, |m| d(&m.paired_shares))
}
