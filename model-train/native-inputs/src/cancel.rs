//! 原完整撤单对象顺序与原 ordinal，不重建对象资格或金额。
use crate::{InputError, exact::*, root::MarketBinding, values::*};
use contracts::ExecutionReadValueV1::{Known, NotApplicable, Unavailable};
use contracts::{economics::EconomicRational as R, *};
use shared_run::{ActionBeforeReadV1, CancelObjectReadV1, ExecutionOrderReadV1};
use std::collections::BTreeMap;
fn template(
    out: &mut [f32],
    base: usize,
    order: &ExecutionOrderReadV1<'_>,
    read: &ActionBeforeReadV1,
    markets: &BTreeMap<String, MarketBinding>,
    now: i64,
) -> Result<(), InputError> {
    let origin = order
        .origin
        .as_ref()
        .ok_or(InputError::MissingAssociation)?;
    let (condition, leg) = leg(origin)?;
    let mapping = markets
        .get(&condition.0)
        .ok_or(InputError::MissingAssociation)?;
    let s = side(&mapping.assets, &leg.token_id).ok_or(InputError::MissingAssociation)?;
    out[base + s] = 1.;
    out[base + 2] = 1.;
    out[base + 3] = 1.;
    let (variant, post, price, target, spend, expiry) = match &leg.order {
        BuyOrderIntentV1::GtcLimit {
            limit_price_pusd_per_share,
            size_shares,
            post_only,
        } => (
            0,
            Some(*post_only),
            R::from_decimal(*limit_price_pusd_per_share),
            R::from_decimal(*size_shares),
            NotApplicable,
            None,
        ),
        BuyOrderIntentV1::GtdLimit {
            limit_price_pusd_per_share,
            size_shares,
            post_only,
            expires_at_utc_sec,
        } => (
            1,
            Some(*post_only),
            R::from_decimal(*limit_price_pusd_per_share),
            R::from_decimal(*size_shares),
            NotApplicable,
            Some(*expires_at_utc_sec),
        ),
        BuyOrderIntentV1::FokMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => (
            2,
            None,
            R::from_decimal(*worst_price_pusd_per_share),
            R::from_decimal(*spend_pusd).divided_by(&R::from_decimal(*worst_price_pusd_per_share)),
            Known(R::from_decimal(*spend_pusd)),
            None,
        ),
        BuyOrderIntentV1::FakMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => (
            3,
            None,
            R::from_decimal(*worst_price_pusd_per_share),
            R::from_decimal(*spend_pusd).divided_by(&R::from_decimal(*worst_price_pusd_per_share)),
            Known(R::from_decimal(*spend_pusd)),
            None,
        ),
    };
    out[base + 4 + variant] = 1.;
    out[base + 8] = 1.;
    out[base + 9] = 1.;
    if let Some(post) = post {
        out[base + 10 + usize::from(post)] = 1.;
        out[base + 12] = 1.;
        out[base + 13] = 1.;
    }
    linear_slot(out, base + 14, &price.mul(&integer(2)).sub(&integer(1)));
    slot(out, base + 17, Known(target));
    slot(out, base + 20, spend);
    slot(out, base + 23, crate::values::read(&order.matched_shares));
    slot(
        out,
        base + 26,
        crate::values::read(&order.active_unfilled_shares),
    );
    let funds = read
        .order_funds()
        .find(|f| f.order_identity == order.order_identity);
    slot(
        out,
        base + 29,
        funds
            .as_ref()
            .map(|f| crate::values::read(&f.principal_encumbrance_pusd))
            .unwrap_or(Unavailable),
    );
    slot(
        out,
        base + 32,
        funds
            .as_ref()
            .map(|f| crate::values::read(&f.immediate_fee_encumbrance_pusd))
            .unwrap_or(Unavailable),
    );
    for (index, record) in [
        (base + 35, order.formed),
        (base + 41, order.latest_direct_feedback),
    ] {
        out[index + 5] = 1.;
        if let Some((source, received)) = times(record) {
            out[index] = signed_log_f32(&ratio(i128::from(now) - i128::from(source), 1000));
            out[index + 1] = signed_log_f32(&ratio(i128::from(now) - i128::from(received), 1000));
            out[index + 2] =
                signed_log_f32(&ratio(i128::from(received) - i128::from(source), 1000));
            out[index + 3] = 1.;
            out[index + 4] = 1.;
        }
    }
    slot(
        out,
        base + 47,
        expiry
            .map(|expiration| {
                Known(ratio(
                    (i128::from(expiration) - 60) * 1000 - i128::from(now),
                    1000,
                ))
            })
            .unwrap_or(NotApplicable),
    );
    Ok(())
}
pub(crate) fn encode(
    read: &ActionBeforeReadV1,
    current: Option<&ConditionId>,
    old: &[ConditionId],
    markets: &BTreeMap<String, MarketBinding>,
    now: i64,
) -> Result<Vec<Vec<f32>>, InputError> {
    let objects = match read.cancel_objects() {
        Known(v) => v,
        _ => return Err(InputError::MissingAssociation),
    };
    let mut output = Vec::with_capacity(objects.len());
    for object in objects {
        let mut out = [0.; 129];
        let condition = match &object {
            CancelObjectReadV1::Single { order } => {
                leg(order
                    .origin
                    .as_ref()
                    .ok_or(InputError::MissingAssociation)?)?
                .0
            }
            CancelObjectReadV1::Paired { orders, .. } => {
                leg(orders[0]
                    .origin
                    .as_ref()
                    .ok_or(InputError::MissingAssociation)?)?
                .0
            }
            CancelObjectReadV1::Market { condition_id, .. } => condition_id,
        };
        if Some(condition) == current {
            out[3] = 1.;
        } else if old.contains(condition) {
            out[4] = 1.;
        } else {
            return Err(InputError::MissingAssociation);
        }
        match object {
            CancelObjectReadV1::Single { order } => {
                out[0] = 1.;
                template(&mut out, 5, &order, read, markets, now)?;
            }
            CancelObjectReadV1::Paired { orders, .. } => {
                out[1] = 1.;
                for order in &orders {
                    let ordinal = order
                        .origin
                        .as_ref()
                        .ok_or(InputError::MissingAssociation)?
                        .batch_item_ordinal;
                    template(
                        &mut out,
                        5 + usize::from(ordinal) * 50,
                        order,
                        read,
                        markets,
                        now,
                    )?;
                }
            }
            CancelObjectReadV1::Market {
                condition_id,
                orders,
            } => {
                out[2] = 1.;
                let mapping = markets
                    .get(&condition_id.0)
                    .ok_or(InputError::MissingAssociation)?;
                for s in 0..2 {
                    let mut members = Vec::new();
                    for order in &orders {
                        let (_, leg) = leg(order
                            .origin
                            .as_ref()
                            .ok_or(InputError::MissingAssociation)?)?;
                        if side(&mapping.assets, &leg.token_id)
                            .ok_or(InputError::MissingAssociation)?
                            == s
                        {
                            members.push(order)
                        }
                    }
                    slot(&mut out, 105 + s * 3, Known(integer(members.len())));
                    slot(
                        &mut out,
                        111 + s * 3,
                        sum(members
                            .iter()
                            .map(|o| crate::values::read(&o.active_unfilled_shares))),
                    );
                    for fee in [false, true] {
                        let total = sum(members.iter().map(|o| {
                            read.order_funds()
                                .find(|f| f.order_identity == o.order_identity)
                                .map(|f| {
                                    crate::values::read(if fee {
                                        &f.immediate_fee_encumbrance_pusd
                                    } else {
                                        &f.principal_encumbrance_pusd
                                    })
                                })
                                .unwrap_or(Unavailable)
                        }));
                        slot(&mut out, 117 + usize::from(fee) * 6 + s * 3, total);
                    }
                }
            }
        }
        output.push(out.to_vec());
    }
    Ok(output)
}
