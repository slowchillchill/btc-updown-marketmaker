#![cfg(feature = "stage-timing")]
use contracts::ExecutionReadValueV1;
use serde_json::{Value, json};
use shared_run::{CancelObjectRead, ExecutionOrderRead, SharedExecutionRegistry};
fn order<R>(order: &ExecutionOrderRead<'_, R>) -> Value {
    json!({"identity":order.order_identity,"condition":order.condition_id,"token":order.token_id,
        "base_applicability":order.base_quantity_applicability,"matched":order.matched_shares,
        "active":order.active_unfilled_shares,"cancellable":order.cancellable,
        "origin":order.origin.as_ref().map(|o|(o.intent.global_request_id(),o.batch_item_ordinal))})
}
fn object<R>(object: &CancelObjectRead<'_, R>) -> Value {
    match object {
        CancelObjectRead::Single { order: value } => json!({"single":order(value)}),
        CancelObjectRead::Paired { origin, orders } => {
            json!({"pair":origin.global_request_id(),"orders":orders.iter().map(order).collect::<Vec<_>>()})
        }
        CancelObjectRead::Market {
            condition_id,
            orders,
        } => json!({"market":condition_id,"orders":orders.iter().map(order).collect::<Vec<_>>()}),
    }
}
pub fn assert_cancel_objects_reference<R: Clone + std::fmt::Debug + Eq>(
    registry: &SharedExecutionRegistry<R>,
) {
    let actual = registry.cancel_objects();
    let original = registry.cancel_objects_full_history_for_measurement();
    match (actual, original) {
        (ExecutionReadValueV1::Known(actual), ExecutionReadValueV1::Known(original)) => assert_eq!(
            actual.iter().map(object).collect::<Vec<_>>(),
            original.iter().map(object).collect::<Vec<_>>()
        ),
        (actual, original) => assert_eq!(
            std::mem::discriminant(&actual),
            std::mem::discriminant(&original)
        ),
    }
}
