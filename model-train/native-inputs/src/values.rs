use crate::exact::*;
use contracts::ExecutionReadValueV1::{Known, NotApplicable, Unavailable};
use contracts::{economics::EconomicRational as R, *};
use shared_run::SubmittedLegReadV1;
pub(crate) type Value = ExecutionReadValueV1<R>;
pub(crate) fn d(v: &ExactDecimal) -> Value {
    Known(decimal(v))
}
pub(crate) fn q(v: &ExactRationalV1) -> Value {
    Known(rational(v))
}
pub(crate) fn read(v: &ExecutionReadValueV1<ExactDecimal>) -> Value {
    match v {
        Known(v) => d(v),
        Unavailable => Unavailable,
        NotApplicable => NotApplicable,
    }
}
pub(crate) fn sum(values: impl IntoIterator<Item = Value>) -> Value {
    let mut sum = R::zero();
    for v in values {
        match v {
            Known(v) => sum = sum.add(&v),
            _ => return Unavailable,
        }
    }
    Known(sum)
}
pub(crate) fn slot(out: &mut [f32], at: usize, value: Value) {
    match value {
        Known(v) => out[at..at + 3].copy_from_slice(&[signed_log_f32(&v), 1., 1.]),
        Unavailable => out[at + 2] = 1.,
        NotApplicable => {}
    }
}
pub(crate) fn linear_slot(out: &mut [f32], at: usize, v: &R) {
    out[at..at + 3].copy_from_slice(&[round_f32(v), 1., 1.])
}
pub(crate) fn leg<'a>(
    s: &'a SubmittedLegReadV1<'a>,
) -> Result<(&'a ConditionId, &'a BuyLegIntentV1), crate::InputError> {
    match s.intent.intent() {
        ModelIntentV1::SubmitSingleBuy {
            condition_id,
            order,
        } => Ok((condition_id, order)),
        ModelIntentV1::SubmitPairedBuy {
            condition_id,
            orders,
        } => Ok((condition_id, &orders[s.batch_item_ordinal as usize])),
        _ => Err(crate::InputError::MissingAssociation),
    }
}
pub(crate) fn side(assets: &[AssetIdentityV1], token: &TokenId) -> Option<usize> {
    assets
        .iter()
        .find(|a| &a.token_id == token)
        .and_then(|a| match a.outcome.index {
            0 => Some(0),
            1 => Some(1),
            _ => None,
        })
}
pub(crate) fn times(
    record: Option<&::execution_normalization::NoeConsumedRecordRefV1>,
) -> Option<(i64, i64)> {
    use contracts::execution_normalization::SimV2SourceRecordReferenceV1::*;
    match &record?.record().source_reference().record {
        DirectResponse {
            platform_responded_at_utc_ms,
            response_received_at_utc_ms,
            ..
        } => Some((*platform_responded_at_utc_ms, *response_received_at_utc_ms)),
        OrderLifecycle {
            platform_event_at_utc_ms,
            feedback_received_at_utc_ms,
            ..
        }
        | TradeLifecycle {
            platform_event_at_utc_ms,
            feedback_received_at_utc_ms,
            ..
        }
        | SettlementLifecycle {
            platform_event_at_utc_ms,
            feedback_received_at_utc_ms,
            ..
        } => Some((*platform_event_at_utc_ms, *feedback_received_at_utc_ms)),
        _ => None,
    }
}

pub(crate) fn order_condition<'a>(
    order: &'a shared_run::ExecutionOrderReadV1<'a>,
) -> Result<&'a ConditionId, crate::InputError> {
    if let Some(origin) = &order.origin {
        return leg(origin).map(|(condition, _)| condition);
    }
    use ::execution_normalization::{NormalizationRecord, SourceNeutralEvidenceKind};
    if let Some(record) = order.formed
        && let NormalizationRecord::Evidence {
            evidence: SourceNeutralEvidenceKind::OrderLifecycle { result },
            ..
        } = record.record()
    {
        return Ok(&result.condition_id);
    }
    Err(crate::InputError::MissingAssociation)
}
