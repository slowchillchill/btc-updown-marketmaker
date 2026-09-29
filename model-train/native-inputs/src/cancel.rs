//! 原完整撤单对象顺序与原 ordinal，不重建对象资格或金额。
use crate::{InputError, exact::*, root::MarketBinding, values::*};
use contracts::ExecutionReadValueV1::{Known, NotApplicable, Unavailable};
use contracts::{economics::EconomicRational as R, *};
use shared_run::{ActionBeforeReadV2, CancelObjectReadV2, ExecutionOrderReadV2};
use std::collections::BTreeMap;
fn template(
    out: &mut [f32],
    base: usize,
    order: &ExecutionOrderReadV2<'_>,
    read: &ActionBeforeReadV2,
    markets: &BTreeMap<String, MarketBinding>,
    now: i64,
    projection: &InputProjection,
) -> Result<(), InputError> {
    if let (Some(condition), Some(token)) = (order.condition_id, order.token_id)
        && let Some(s) = markets
            .get(&condition.0)
            .and_then(|m| side(&m.assets, token))
    {
        out[base + s] = 1.;
        out[base + 2] = 1.;
    }
    out[base + 3] = 1.;
    let original = order.origin.as_ref().map(leg).transpose()?;
    if let Some((_, leg)) = original {
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
                R::from_decimal(*spend_pusd)
                    .divided_by(&R::from_decimal(*worst_price_pusd_per_share)),
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
                R::from_decimal(*spend_pusd)
                    .divided_by(&R::from_decimal(*worst_price_pusd_per_share)),
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
        slot(projection, out, base + 17, Known(target));
        slot(projection, out, base + 20, spend);
        slot(
            projection,
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
        for offset in [50, 51, 53] {
            out[base + offset] = 1.;
        }
    } else {
        out[base + 9] = 1.;
        slot(projection, out, base + 14, Unavailable);
        slot(projection, out, base + 17, Unavailable);
        if let Some(base_quantity) = order.base_quantity_applicability {
            out[base + 50] = 1.;
            out[base + 51] = 1.;
            if base_quantity {
                out[base + 13] = 1.;
            } else {
                slot(projection, out, base + 20, Unavailable);
                out[base + 53] = 1.;
            }
        }
        // 未知 GTD 子型仍不可判断到期适用性。
    }
    out[base + 52] = f32::from(order.base_quantity_applicability.is_some());
    slot(
        projection,
        out,
        base + 23,
        crate::values::read(&order.matched_shares),
    );
    slot(
        projection,
        out,
        base + 26,
        crate::values::read(&order.active_unfilled_shares),
    );
    if order.base_quantity_applicability.is_none() {
        out[base + 28] = 0.;
    }
    let funds = read
        .order_funds()
        .find(|f| f.order_identity == order.order_identity);
    slot(
        projection,
        out,
        base + 29,
        funds
            .as_ref()
            .map(|f| crate::values::read(&f.principal_encumbrance_pusd))
            .unwrap_or(Unavailable),
    );
    slot(
        projection,
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
            out[index] = projection.project(i128::from(now) - i128::from(source));
            out[index + 1] = projection.project(i128::from(now) - i128::from(received));
            out[index + 2] = projection.project(i128::from(received) - i128::from(source));
            out[index + 3] = 1.;
            out[index + 4] = 1.;
        }
    }
    match order.cancellable {
        Known(value) => out[base + 54..base + 57].copy_from_slice(&[f32::from(value), 1., 1.]),
        Unavailable => out[base + 56] = 1.,
        NotApplicable => {}
    }
    out[base + 61] = 1.;
    if let Some(record) = order.latest_cancel_feedback {
        use ::execution_normalization::{NormalizationRecord, SourceNeutralEvidenceKind};
        use contracts::execution_normalization::{
            SourceNeutralCancellationOutcomeV1::*, SourceNeutralControlResult,
        };
        if let NormalizationRecord::Evidence {
            evidence:
                SourceNeutralEvidenceKind::Control {
                    result: SourceNeutralControlResult::Cancellation { outcome, .. },
                },
            ..
        } = record.record()
        {
            let index = match outcome {
                Canceled => 0,
                NotCanceledNoEffect => 1,
                EvidenceUnknownNoEffect => 2,
            };
            out[base + 57 + index] = 1.;
            out[base + 60] = 1.;
        }
    }
    time_slot(
        out,
        base + 62,
        Known(order.latest_cancel_feedback),
        now,
        projection,
    );
    Ok(())
}
pub(crate) fn encode(
    read: &ActionBeforeReadV2,
    objects: &[CancelObjectReadV2<'_>],
    current: Option<&ConditionId>,
    old: &[ConditionId],
    markets: &BTreeMap<String, MarketBinding>,
    now: i64,
    projection: &InputProjection,
) -> Result<Vec<Vec<f32>>, InputError> {
    let mut output = Vec::with_capacity(objects.len());
    for object in objects {
        let mut out = [0.; 184];
        let condition = match &object {
            CancelObjectReadV2::Single { order } => order.condition_id,
            CancelObjectReadV2::Paired { orders, .. } => orders[0].condition_id,
            CancelObjectReadV2::Market { condition_id, .. } => Some(*condition_id),
        };
        if condition.is_some() && condition == current {
            out[3] = 1.;
            out[5] = 1.;
        } else if condition.is_some_and(|c| old.contains(c)) {
            out[4] = 1.;
            out[5] = 1.;
        }
        request_block(
            &mut out,
            166,
            read.cancellation_requests(Some(&object.request_scope())),
            now,
            projection,
        );
        match object {
            CancelObjectReadV2::Single { order } => {
                out[0] = 1.;
                template(&mut out, 6, &order, read, markets, now, projection)?;
            }
            CancelObjectReadV2::Paired { orders, .. } => {
                out[1] = 1.;
                for order in orders {
                    let ordinal = order
                        .origin
                        .as_ref()
                        .ok_or(InputError::MissingAssociation)?
                        .batch_item_ordinal;
                    template(
                        &mut out,
                        6 + usize::from(ordinal) * 68,
                        order,
                        read,
                        markets,
                        now,
                        projection,
                    )?;
                }
            }
            CancelObjectReadV2::Market {
                condition_id,
                orders,
            } => {
                out[2] = 1.;
                let mapping = markets
                    .get(&condition_id.0)
                    .ok_or(InputError::MissingAssociation)?;
                for s in 0..2 {
                    let mut members = Vec::new();
                    let mut unknown_side = false;
                    for order in orders {
                        match order
                            .token_id
                            .and_then(|token| side(&mapping.assets, token))
                        {
                            Some(leg) if leg == s => members.push(order),
                            Some(_) => {}
                            None => unknown_side = true,
                        }
                    }
                    if unknown_side {
                        for offset in [142, 148, 154, 160] {
                            slot(projection, &mut out, offset + s * 3, Unavailable);
                        }
                        continue;
                    }
                    slot(
                        projection,
                        &mut out,
                        142 + s * 3,
                        Known(integer(members.len())),
                    );
                    slot(
                        projection,
                        &mut out,
                        148 + s * 3,
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
                        slot(
                            projection,
                            &mut out,
                            154 + usize::from(fee) * 6 + s * 3,
                            total,
                        );
                    }
                }
            }
        }
        output.push(out.to_vec());
    }
    Ok(output)
}
