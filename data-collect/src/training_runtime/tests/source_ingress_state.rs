use super::*;

#[test]
fn source_queue_recoverable_wait_is_not_a_source_gap() {
    assert!(!is_training_runtime_source_gap_reason(
        "bounded_queue_backpressure_wait:binance_depth_update"
    ));
}
